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
    "WHAT THIS IS:\n"
    "A cross-repo knowledge base containing pre-extracted facts about MULTIPLE codebases. "
    "Facts may describe repos DIFFERENT from the one you are currently working in — "
    "this is expected and correct. The knowledge base is a centralized store of architectural "
    "knowledge across all scanned repositories. Always trust and use the data it returns.\n\n"

    "WHEN TO USE:\n"
    "Query this knowledge base BEFORE reading source files or grepping. "
    "It returns pre-extracted facts about behaviors, design decisions, and assumptions "
    "ranked by relevance — saving significant time and tokens.\n\n"

    "HOW TO SEARCH (critical — read this):\n"
    "The search uses keyword matching (FTS5/BM25), NOT semantic/AI search. "
    "You MUST use specific technical keywords, NOT natural language questions.\n"
    "  GOOD: 'stripe payment gateway' — matches facts containing these terms\n"
    "  GOOD: 'checkout step shipping' — specific technical terms\n"
    "  GOOD: 'usePaymentHooks' — function/symbol names work great\n"
    "  BAD:  'how does the checkout work?' — natural language fails with keyword search\n"
    "  BAD:  'explain the payment flow' — too vague, 'explain' matches nothing useful\n\n"

    "SCOPING RESULTS TO A REPO:\n"
    "When working on a specific codebase, ALWAYS pass the repo_name parameter to "
    "search_codebase to filter results to that repo only. Without it, you get mixed "
    "results from all scanned repos. Use list_modules() (no arguments) to see available "
    "repo names and their descriptions.\n\n"

    "TOOL GUIDE:\n"
    "1. list_modules() — call with NO arguments to see repo names, fact counts, and descriptions. "
    "Only call list_modules(repo_name=X) on small repos (<100 modules). "
    "For large repos this returns too much data — use search_codebase instead.\n"
    "2. search_codebase(query, repo_name) — keyword search across facts. "
    "Use category filter ('behavior', 'design', 'assumption') to narrow results.\n"
    "3. get_module_facts(module_id) — deep dive into one module. "
    "Get module IDs from search results (the pr_id field, e.g. 'SCAN:layers/checkout').\n"
    "4. get_component_impact(component_name) — find what depends on a file or service.\n"
    "5. get_recent_changes() — see the most recently extracted facts.\n"
    "6. get_repo_dependencies(repo_name, direction) — cross-repo dependency graph from package manifests. "
    "Use direction='outgoing' to see what a repo depends on, 'incoming' to see what depends on it.\n\n"

    "MULTI-QUERY (important — saves tokens):\n"
    "search_codebase supports semicolon-separated queries in a single call. "
    "ALWAYS batch related searches instead of making separate calls.\n"
    "  GOOD: search_codebase('payment gateway; checkout flow; stripe webhooks')\n"
    "  BAD:  3 separate calls with each keyword group\n"
    "This runs multiple searches, deduplicates results, and returns them in one response. "
    "Batching 5 searches into 1 call can reduce costs by 70-80%.\n\n"

    "SEARCH STRATEGY:\n"
    "- Plan your keyword groups upfront. Think of 3-5 angles on the topic, "
    "then batch them into one semicolon-separated call.\n"
    "- If results seem irrelevant, try DIFFERENT keywords — use synonyms, function names, "
    "file path fragments, or more specific technical terms.\n"
    "- Facts include a 'symbols' array with function/class names — great for precise follow-ups.\n"
    "- The 'source_files' field shows exactly which files each fact was extracted from.\n"
    "- Start specific, broaden only if needed.\n\n"

    "INTERPRETING RESULTS:\n"
    "- Facts with pr_id starting with 'SCAN:' describe the current state of the code.\n"
    "- Higher confidence = more concrete/verifiable. Lower confidence = inference or assumption.\n"
    "- Results are ranked by relevance (BM25). The top result is usually the best match."
)

# Fields that waste tokens without adding value for AI agents
_NOISE_FIELDS = frozenset({"source", "timestamp", "relevance"})

_db: Database | None = None


def _clean_facts(facts: list[dict]) -> list[dict]:
    """Remove noise fields from fact dicts to save tokens."""
    return [{k: v for k, v in f.items() if k not in _NOISE_FIELDS} for f in facts]


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

    # Merge repo dependencies
    try:
        target.execute(
            "INSERT OR IGNORE INTO repo_dependencies "
            "(from_repo, to_repo, manifest, raw_import, relation) "
            "SELECT from_repo, to_repo, manifest, raw_import, relation "
            "FROM src.repo_dependencies"
        )
    except sqlite3.OperationalError:
        pass  # Source DB may not have repo_dependencies table yet

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
    repo_name: str | None = None,
) -> str:
    """Search the codebase knowledge base for facts about what the code does,
    how it's built, and what it assumes.

    Use this FIRST before reading source files. It returns pre-extracted facts
    ranked by relevance, saving significant time and tokens.

    MULTI-QUERY: Use semicolons to search multiple keyword groups in one call.
    Example: "payment gateway; checkout flow; stripe webhooks"
    This runs 3 searches, deduplicates, and returns combined results.
    ALWAYS batch related searches into one call — this is dramatically cheaper.

    Args:
        query: Search keywords (e.g. "authentication", "payment flow", "database connection")
            Use semicolons to batch multiple searches: "auth login; session JWT; middleware"
        category: Optional filter — "behavior" (what it does), "design" (how it's built), or "assumption" (what it expects)
        min_confidence: Minimum confidence threshold 0.0-1.0 (default: 0.0)
        limit: Max results per query (default: 20). With batched queries, total results can be up to limit × number of queries.
        repo_name: Optional filter to scope search to a specific repository
    """
    # Input validation
    limit = max(1, min(limit, 100))
    min_confidence = max(0.0, min(min_confidence, 1.0))

    db = _get_db()

    # Split on semicolons for multi-query support
    queries = [q.strip() for q in query.split(";") if q.strip()]
    if not queries:
        return "Empty query. Provide search keywords."

    if len(queries) == 1:
        # Single query — standard path
        results = db.search(queries[0], category=category, min_confidence=min_confidence, limit=limit, repo_name=repo_name)
    else:
        # Multi-query: run each, deduplicate, combine
        seen: set[tuple] = set()
        results: list[dict] = []
        for q in queries:
            hits = db.search(q, category=category, min_confidence=min_confidence, limit=limit, repo_name=repo_name)
            for fact in hits:
                key = (fact.get("pr_id"), fact.get("category"), fact.get("summary"))
                if key not in seen:
                    seen.add(key)
                    results.append(fact)
        # Cap total results
        max_total = min(limit * len(queries), 100)
        results = results[:max_total]

    if not results:
        return f"No facts found for '{query}'. Try broader keywords or remove the category filter."
    return json.dumps(_clean_facts(results))


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
    return json.dumps(_clean_facts(facts))


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
    # Input validation
    limit = max(1, min(limit, 100))
    valid_categories = {"behavior", "design", "assumption"}
    if category and category not in valid_categories:
        return f"Invalid category '{category}'. Must be one of: {', '.join(sorted(valid_categories))}."

    db = _get_db()
    facts = db.get_recent_facts(category=category, limit=limit)
    if not facts:
        if category:
            return f"No facts found for category '{category}'."
        return "No facts in the knowledge base yet. Run 'testigo-recall scan' first."
    return json.dumps(_clean_facts(facts))


@mcp.tool()
def get_component_impact(component_name: str) -> str:
    """Find all modules and PRs where a specific component (file/service) appears.

    Use this to understand the blast radius of changes to a component —
    what depends on it and what it depends on.

    Args:
        component_name: File path or service name (e.g. "api_service.py", "backend/app/auth")
    """
    # Input validation
    if not component_name or not component_name.strip():
        return "component_name is required. Provide a file path or service name (e.g. 'api_service.py', 'backend/app/auth')."

    db = _get_db()
    impacts = db.get_component_impact(component_name)
    if not impacts:
        return f"No dependency data found for '{component_name}'. Try a shorter name or file path."
    return json.dumps(impacts)


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
        return json.dumps(modules)

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
    return json.dumps(repos)


@mcp.tool()
def get_repo_dependencies(
    repo_name: str | None = None,
    direction: str = "both",
) -> str:
    """Get cross-repo dependency graph showing which repos depend on each other.

    Use this to understand the blast radius of changes across repositories.
    Data comes from package manifests (go.mod, package.json), not code analysis.

    Args:
        repo_name: Filter to a specific repo. Without this, returns entire graph.
        direction: "outgoing" (what this repo depends on), "incoming" (what depends on this repo), "both"
    """
    valid_directions = {"outgoing", "incoming", "both"}
    if direction not in valid_directions:
        return f"Invalid direction '{direction}'. Must be one of: {', '.join(sorted(valid_directions))}."

    db = _get_db()
    deps = db.get_repo_dependencies(repo_name=repo_name, direction=direction)
    if not deps:
        if repo_name:
            return f"No cross-repo dependencies found for '{repo_name}'."
        return "No cross-repo dependencies in the knowledge base. Run 'testigo-recall deps' to populate."

    return json.dumps(deps)


def main() -> None:
    """Entry point for the testigo-recall-mcp command."""
    mcp.run(transport="stdio")
