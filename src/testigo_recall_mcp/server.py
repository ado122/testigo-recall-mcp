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
import urllib.error
import urllib.request
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from testigo_recall_mcp.storage.db import Database

logger = logging.getLogger(__name__)

mcp = FastMCP(
    "testigo-recall",
    instructions=(
        "You have access to a pre-scanned codebase knowledge base. "
        "Use search_codebase to find facts about behaviors, design decisions, "
        "and assumptions. Use get_module_facts for deep dives into specific modules. "
        "Always search the knowledge base BEFORE reading source files — it's faster "
        "and cheaper.\n\n"
        "IMPORTANT — interpreting results:\n"
        "- Facts with source='scan' (pr_id starts with 'SCAN:') describe the CURRENT "
        "state of a module. These are refreshed automatically when PRs touch those files.\n"
        "- Facts with source='ai' (pr_id starts with 'PR-') describe what a specific PR CHANGED.\n"
        "- For 'how does X work now?' → use the most recent scan facts (check timestamp).\n"
        "- For 'what changed?' or 'why?' → use PR facts.\n"
        "- Always check the 'timestamp' field. More recent = more reliable."
    ),
)

_db: Database | None = None


def _sync_db_from_github(repo: str, db_path: Path) -> bool:
    """Download the latest knowledge base from a GitHub release.

    Uses the GitHub API directly — no external tools required.
    Public repos work without auth. Private repos need GITHUB_TOKEN
    or GH_TOKEN environment variable.

    Returns True if download succeeded.
    """
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    headers = {"Accept": "application/octet-stream"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        # Step 1: Find the asset URL via the GitHub API
        api_url = f"https://api.github.com/repos/{repo}/releases/tags/knowledge-base"
        api_headers = {"Authorization": f"token {token}"} if token else {}
        req = urllib.request.Request(api_url, headers=api_headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            release = json.loads(resp.read())

        asset = next((a for a in release.get("assets", []) if a["name"] == "impact-history.db"), None)
        if not asset:
            logger.warning("No impact-history.db asset in release for %s", repo)
            return False

        # Step 2: Download the asset binary
        req = urllib.request.Request(asset["url"], headers=headers)
        with urllib.request.urlopen(req, timeout=60) as resp:
            db_path.write_bytes(resp.read())
        return True
    except urllib.error.HTTPError as e:
        logger.warning("Could not download DB from %s: HTTP %d", repo, e.code)
        return False
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        logger.warning("Could not download DB from %s: %s", repo, e)
        return False


def _resolve_db_path() -> str | None:
    """Determine the DB path, auto-downloading from GitHub if configured."""
    # Explicit path takes priority — no auto-download
    explicit = os.environ.get("TESTIGO_RECALL_DB_PATH") or os.environ.get("PR_IMPACT_DB_PATH")
    if explicit:
        return explicit

    # If a GitHub repo is configured, auto-download
    repo = os.environ.get("TESTIGO_RECALL_REPO") or os.environ.get("PR_IMPACT_REPO")
    if repo:
        db_path = Path.home() / ".testigo-recall" / repo.replace("/", "--") / "knowledge-base.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Syncing knowledge base from %s ...", repo)
        if _sync_db_from_github(repo, db_path):
            logger.info("Knowledge base synced to %s", db_path)
        elif db_path.exists():
            logger.info("Using cached knowledge base at %s", db_path)
        else:
            logger.warning("No knowledge base available for %s", repo)

        return str(db_path)

    return None


def _get_db() -> Database:
    """Lazy-init the database connection."""
    global _db
    if _db is None:
        _db = Database(_resolve_db_path())
    return _db


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

    Module IDs look like "SCAN:backend/app/api" or "PR-123".
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
    """List all scanned modules in the knowledge base.

    Returns module names so you can use get_module_facts to dive deeper.

    Args:
        repo_name: Optional repository name filter
    """
    db = _get_db()
    c = db._conn.cursor()
    if repo_name:
        rows = c.execute(
            "SELECT pr_id, repo, COUNT(*) as fact_count "
            "FROM facts WHERE repo = ? GROUP BY pr_id, repo ORDER BY pr_id",
            (repo_name,),
        ).fetchall()
    else:
        rows = c.execute(
            "SELECT pr_id, repo, COUNT(*) as fact_count "
            "FROM facts GROUP BY pr_id, repo ORDER BY pr_id",
        ).fetchall()

    if not rows:
        return "No modules found. Run 'testigo-recall scan' to populate the knowledge base."

    modules = [{"module_id": r["pr_id"], "repo": r["repo"], "fact_count": r["fact_count"]} for r in rows]
    return json.dumps(modules, indent=2)


def main() -> None:
    """Entry point for the testigo-recall-mcp command."""
    mcp.run(transport="stdio")
