"""MCP server for testigo-recall.

Exposes the codebase knowledge base as tools that any MCP-compatible
AI agent (Claude Code, Cursor, Windsurf, etc.) can call directly.

On startup, if TESTIGO_RECALL_REPO is set (e.g. "owner/repo"), the server
automatically downloads the latest knowledge base from the GitHub
release — no external tools required.

Usage:
    testigo-recall-mcp                # stdio transport (default)
    TESTIGO_RECALL_REPO=owner/repo testigo-recall-mcp
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from testigo_recall_mcp.storage.db import Database

logger = logging.getLogger(__name__)

_STATIC_INSTRUCTIONS = (
    "You have access to a pre-scanned codebase knowledge base. "
    "Use search_codebase to find facts about behaviors, design decisions, "
    "and assumptions. Use get_module_facts for deep dives into specific modules. "
    "Always search the knowledge base BEFORE reading source files — it's faster "
    "and cheaper.\n\n"
    "IMPORTANT — interpreting results:\n"
    "- All facts have pr_id starting with 'SCAN:' and describe the CURRENT state of the code.\n"
    "- Facts are refreshed automatically when PRs touch those files — the DB always "
    "reflects the latest merged code.\n"
    "- Always check the 'timestamp' field. More recent = more reliable.\n\n"
    "EFFICIENT USAGE — follow this pattern to minimize token cost:\n"
    "1. Start with list_modules to get the full map of what's scanned.\n"
    "2. Use get_module_facts for targeted deep dives on specific modules — "
    "it gives you everything about that module with zero noise.\n"
    "3. Use search_codebase only for cross-module questions (max 1-2 broad searches). "
    "Always use the category filter ('behavior', 'design', 'assumption') to reduce noise.\n"
    "4. Use get_component_impact for blast-radius questions ('what depends on X?').\n"
    "5. NEVER repeat similar searches with rephrased queries — trust the first result.\n"
    "6. Facts include a 'symbols' array with function/class names for grep-based navigation. "
    "Use these to jump directly to code instead of reading entire files.\n\n"
    "Optimal workflow: list_modules -> get_module_facts on ~3-5 relevant modules -> done. "
    "This costs ~7 calls. Avoid the anti-pattern of 8+ broad searches + 8+ file reads."
)

_db: Database | None = None


def _sync_db_from_github(repo: str, cache_dir: Path) -> list[Path]:
    """Download all .db assets from a GitHub release.

    Uses the GitHub API directly — no external tools required.
    Public repos work without auth. Private repos need GITHUB_TOKEN
    or GH_TOKEN environment variable.

    Returns list of downloaded file paths (empty on failure).
    """
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    dl_headers = {"Accept": "application/octet-stream"}
    if token:
        dl_headers["Authorization"] = f"token {token}"

    try:
        # Step 1: Find all .db assets via the GitHub API
        api_url = f"https://api.github.com/repos/{repo}/releases/tags/knowledge-base"
        api_headers = {"Authorization": f"token {token}"} if token else {}
        req = urllib.request.Request(api_url, headers=api_headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            release = json.loads(resp.read())

        db_assets = [a for a in release.get("assets", []) if a["name"].endswith(".db")]
        if not db_assets:
            logger.warning("No .db assets in release for %s", repo)
            return []

        # Step 2: Download each asset
        paths: list[Path] = []
        for asset in db_assets:
            db_path = cache_dir / asset["name"]
            try:
                req = urllib.request.Request(asset["url"], headers=dl_headers)
                with urllib.request.urlopen(req, timeout=60) as resp:
                    db_path.write_bytes(resp.read())
                logger.info("Downloaded %s from %s", asset["name"], repo)
                paths.append(db_path)
            except (urllib.error.HTTPError, urllib.error.URLError, OSError, TimeoutError) as e:
                logger.warning("Failed to download %s from %s: %s", asset["name"], repo, e)
        return paths
    except urllib.error.HTTPError as e:
        logger.warning("Could not access release for %s: HTTP %d", repo, e.code)
        return []
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        logger.warning("Could not access release for %s: %s", repo, e)
        return []


def _collect_sources() -> list[Path]:
    """Collect all DB sources from env vars.

    Supports comma-separated values for multiple sources:
      TESTIGO_RECALL_DB_PATH=local1.db,local2.db
      TESTIGO_RECALL_REPO=org/repo-a,org/repo-b
    """
    sources: list[Path] = []

    # Local DB paths (comma-separated)
    local = os.environ.get("TESTIGO_RECALL_DB_PATH") or os.environ.get("PR_IMPACT_DB_PATH")
    if local:
        for p in local.split(","):
            p = p.strip()
            if p:
                path = Path(p)
                if path.exists():
                    sources.append(path)
                else:
                    logger.warning("Local DB not found: %s", p)

    # GitHub repos (comma-separated)
    repos = os.environ.get("TESTIGO_RECALL_REPO") or os.environ.get("PR_IMPACT_REPO")
    if repos:
        for repo in repos.split(","):
            repo = repo.strip()
            if not repo:
                continue
            cache_dir = Path.home() / ".testigo-recall" / repo.replace("/", "--")
            cache_dir.mkdir(parents=True, exist_ok=True)

            logger.info("Syncing knowledge base from %s ...", repo)
            downloaded = _sync_db_from_github(repo, cache_dir)
            if downloaded:
                sources.extend(downloaded)
            else:
                # Fallback: use any cached .db files in the directory
                cached = sorted(cache_dir.glob("*.db"))
                if cached:
                    logger.info("Using %d cached DB(s) from %s", len(cached), cache_dir)
                    sources.extend(cached)
                else:
                    logger.warning("No knowledge base available for %s", repo)

    return sources


def _merge_into(target: sqlite3.Connection, source_path: Path) -> int:
    """Merge all data from source DB into target. Returns facts merged."""
    target.execute("ATTACH DATABASE ? AS src", (str(source_path),))

    # Replace pr_analyses (natural PK handles conflicts)
    target.execute(
        "INSERT OR REPLACE INTO pr_analyses "
        "SELECT * FROM src.pr_analyses"
    )

    # Clear facts/deps for modules we're importing (avoid duplicates)
    target.execute(
        "DELETE FROM facts WHERE EXISTS ("
        "  SELECT 1 FROM src.pr_analyses s "
        "  WHERE s.pr_id = facts.pr_id AND s.repo = facts.repo"
        ")"
    )
    target.execute(
        "DELETE FROM dependencies WHERE EXISTS ("
        "  SELECT 1 FROM src.pr_analyses s "
        "  WHERE s.pr_id = dependencies.pr_id AND s.repo = dependencies.repo"
        ")"
    )

    # Insert facts (skip id — autoincrement + FTS triggers handle it)
    # Check if source DB has symbols column (older DBs may not)
    src_cols = {r[1] for r in target.execute("PRAGMA src.table_info(facts)").fetchall()}
    symbols_expr = "COALESCE(symbols, '[]')" if "symbols" in src_cols else "'[]'"
    count = target.execute(
        "INSERT INTO facts (pr_id, repo, category, summary, detail, confidence, source, source_files, symbols) "
        "SELECT pr_id, repo, category, summary, detail, confidence, source, source_files, "
        f"{symbols_expr} FROM src.facts"
    ).rowcount

    # Insert dependencies
    target.execute(
        "INSERT INTO dependencies (pr_id, repo, from_component, to_component, relation) "
        "SELECT pr_id, repo, from_component, to_component, relation "
        "FROM src.dependencies"
    )

    # Merge repo summaries (newer wins)
    try:
        target.execute(
            "INSERT OR REPLACE INTO repo_summaries "
            "SELECT * FROM src.repo_summaries"
        )
    except sqlite3.OperationalError:
        pass  # Source DB may not have repo_summaries table yet

    # Commit before detach — SQLite requires no open transactions
    target.commit()
    target.execute("DETACH DATABASE src")
    return count


def _resolve_db_path() -> str | None:
    """Determine the DB path, supporting multiple sources.

    If only one source exists, it's used directly.
    If multiple sources exist, they're merged into a temp DB.
    """
    sources = _collect_sources()

    if not sources:
        return None

    if len(sources) == 1:
        return str(sources[0])

    # Multiple sources — merge into a temp DB
    merged_path = Path(tempfile.gettempdir()) / "testigo-recall-merged.db"

    # Copy first source as base (preserves schema + FTS)
    shutil.copy2(sources[0], merged_path)
    logger.info("Base DB: %s", sources[0])

    conn = sqlite3.connect(str(merged_path))
    conn.row_factory = sqlite3.Row
    try:
        for src in sources[1:]:
            count = _merge_into(conn, src)
            logger.info("Merged %d facts from %s", count, src)
    finally:
        conn.close()

    logger.info("Merged %d sources into %s", len(sources), merged_path)
    return str(merged_path)


def _get_db() -> Database:
    """Lazy-init the database connection."""
    global _db
    if _db is None:
        _db = Database(_resolve_db_path())
    return _db


def _build_catalog() -> str:
    """Build repo catalog string from DB summaries.

    Called at module level to inject into FastMCP instructions.
    Returns empty string if no summaries exist yet.
    """
    try:
        db = _get_db()
        summaries = db.get_repo_summaries()
        if not summaries:
            return ""
        lines = []
        c = db._conn.cursor()
        for s in summaries:
            row = c.execute(
                "SELECT COUNT(*) as cnt FROM facts WHERE repo = ?",
                (s["repo"],),
            ).fetchone()
            count = row["cnt"] if row else 0
            lines.append(f"- {s['repo']} ({count} facts): {s['summary']}")
        return (
            "\n\nAVAILABLE REPOSITORIES:\n"
            + "\n".join(lines)
            + "\nUse this catalog to decide which repo to search for a given question."
        )
    except Exception:
        return ""


mcp = FastMCP(
    "testigo-recall",
    instructions=_STATIC_INSTRUCTIONS + _build_catalog(),
)


@mcp.tool()
def search_codebase(
    query: str,
    category: str | None = None,
    min_confidence: float = 0.0,
    limit: int = 20,
) -> str:
    """Search the codebase knowledge base for facts about what the code does,
    how it's built, and what it assumes.

    Use this FIRST before reading source files. It returns pre-extracted facts
    ranked by relevance, saving significant time and tokens.

    Args:
        query: Search keywords (e.g. "authentication", "payment flow", "database connection")
        category: Optional filter — "behavior" (what it does), "design" (how it's built), or "assumption" (what it expects)
        min_confidence: Minimum confidence threshold 0.0-1.0 (default: 0.0)
        limit: Max results to return (default: 20)
    """
    db = _get_db()
    results = db.search(query, category=category, min_confidence=min_confidence, limit=limit)
    if not results:
        return f"No facts found for '{query}'. Try broader keywords or remove the category filter."
    return json.dumps(results, indent=2)


@mcp.tool()
def get_module_facts(module_id: str) -> str:
    """Get all extracted facts for a specific module.

    Module IDs look like "SCAN:backend/app/api".
    Use search_codebase first to discover module IDs, then use this
    for a deep dive into a specific module.

    Args:
        module_id: The module identifier (e.g. "SCAN:backend/app/api/simplified")
    """
    db = _get_db()
    facts = db.get_facts_by_module(module_id)
    if not facts:
        return f"No facts found for module '{module_id}'. Use search_codebase to find valid module IDs."
    return json.dumps(facts, indent=2)


@mcp.tool()
def get_recent_changes(
    category: str | None = None,
    limit: int = 10,
) -> str:
    """Get the most recently extracted facts across the entire codebase.

    Useful for understanding what changed recently or getting an overview
    of the codebase.

    Args:
        category: Optional filter — "behavior", "design", or "assumption"
        limit: Number of recent facts to return (default: 10)
    """
    db = _get_db()
    facts = db.get_recent_facts(category=category, limit=limit)
    if not facts:
        return "No facts in the knowledge base yet. Run 'testigo-recall scan' first."
    return json.dumps(facts, indent=2)


@mcp.tool()
def get_component_impact(component_name: str) -> str:
    """Find all modules and PRs where a specific component (file/service) appears.

    Use this to understand the blast radius of changes to a component —
    what depends on it and what it depends on.

    Args:
        component_name: File path or service name (e.g. "api_service.py", "backend/app/auth")
    """
    db = _get_db()
    impacts = db.get_component_impact(component_name)
    if not impacts:
        return f"No dependency data found for '{component_name}'. Try a shorter name or file path."
    return json.dumps(impacts, indent=2)


@mcp.tool()
def list_modules(repo_name: str | None = None) -> str:
    """List scanned modules in the knowledge base.

    Without repo_name: returns a compact summary of repos with module/fact counts.
    With repo_name: returns the full list of modules for that specific repo.

    Always call without repo_name first to discover available repos, then call
    again with repo_name to get the module list for a specific repo.

    Args:
        repo_name: Repository name — pass this to get the full module list for one repo
    """
    db = _get_db()
    c = db._conn.cursor()

    if repo_name:
        rows = c.execute(
            "SELECT pr_id, repo, COUNT(*) as fact_count "
            "FROM facts WHERE repo = ? GROUP BY pr_id, repo ORDER BY pr_id",
            (repo_name,),
        ).fetchall()
        if not rows:
            return f"No modules found for repo '{repo_name}'."
        modules = [{"module_id": r["pr_id"], "repo": r["repo"], "fact_count": r["fact_count"]} for r in rows]
        return json.dumps(modules, indent=2)

    # No filter — return compact repo summary instead of every module
    rows = c.execute(
        "SELECT repo, COUNT(DISTINCT pr_id) as modules, COUNT(*) as facts "
        "FROM facts GROUP BY repo ORDER BY repo",
    ).fetchall()
    if not rows:
        return "No modules found. Run 'testigo-recall scan' to populate the knowledge base."

    # Include repo summaries if available
    summaries_map: dict[str, str] = {}
    try:
        for s in db.get_repo_summaries():
            summaries_map[s["repo"]] = s["summary"]
    except Exception:
        pass

    repos = [
        {
            "repo": r["repo"],
            "modules": r["modules"],
            "facts": r["facts"],
            **({"summary": summaries_map[r["repo"]]} if r["repo"] in summaries_map else {}),
        }
        for r in rows
    ]
    return json.dumps(repos, indent=2)


def main() -> None:
    """Entry point for the testigo-recall-mcp command."""
    mcp.run(transport="stdio")
