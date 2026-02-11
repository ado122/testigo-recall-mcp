from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from testigo_recall_mcp.models import (
    ChangeUnit,
    Dependency,
    Fact,
    PRAnalysis,
)

_DEFAULT_DB_PATH = Path(__file__).resolve().parents[3] / "knowledge-base.db"

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS pr_analyses (
    pr_id TEXT NOT NULL,
    repo TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    files TEXT NOT NULL,
    kinds TEXT NOT NULL,
    PRIMARY KEY (pr_id, repo)
);

CREATE TABLE IF NOT EXISTS dependencies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pr_id TEXT NOT NULL,
    repo TEXT NOT NULL,
    from_component TEXT NOT NULL,
    to_component TEXT NOT NULL,
    relation TEXT NOT NULL,
    FOREIGN KEY (pr_id, repo) REFERENCES pr_analyses(pr_id, repo)
);

CREATE TABLE IF NOT EXISTS facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pr_id TEXT NOT NULL,
    repo TEXT NOT NULL,
    category TEXT NOT NULL,
    summary TEXT NOT NULL,
    detail TEXT NOT NULL,
    confidence REAL NOT NULL,
    source TEXT NOT NULL DEFAULT 'ai',
    source_files TEXT NOT NULL DEFAULT '[]',
    symbols TEXT NOT NULL DEFAULT '[]',
    FOREIGN KEY (pr_id, repo) REFERENCES pr_analyses(pr_id, repo)
);

CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts USING fts5(
    summary,
    detail,
    content=facts,
    content_rowid=id,
    tokenize='porter unicode61'
);

-- Triggers to keep FTS in sync with facts table
CREATE TRIGGER IF NOT EXISTS facts_ai AFTER INSERT ON facts BEGIN
    INSERT INTO facts_fts(rowid, summary, detail)
    VALUES (new.id, new.summary, new.detail);
END;

CREATE TRIGGER IF NOT EXISTS facts_ad AFTER DELETE ON facts BEGIN
    INSERT INTO facts_fts(facts_fts, rowid, summary, detail)
    VALUES ('delete', old.id, old.summary, old.detail);
END;
"""


class Database:
    def __init__(self, db_path: str | Path | None = None):
        self._path = Path(db_path) if db_path else _DEFAULT_DB_PATH
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path))
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        """Add columns that may be missing in older databases."""
        c = self._conn.cursor()
        cols = {r[1] for r in c.execute("PRAGMA table_info(facts)").fetchall()}
        if "symbols" not in cols:
            c.execute("ALTER TABLE facts ADD COLUMN symbols TEXT NOT NULL DEFAULT '[]'")
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def save_analysis(self, analysis: PRAnalysis) -> None:
        """Save or replace a PR analysis."""
        c = self._conn.cursor()
        for table in ("facts", "dependencies", "pr_analyses"):
            c.execute(
                f"DELETE FROM {table} WHERE pr_id = ? AND repo = ?",
                (analysis.pr_id, analysis.repo),
            )

        c.execute(
            "INSERT INTO pr_analyses (pr_id, repo, timestamp, files, kinds) VALUES (?, ?, ?, ?, ?)",
            (
                analysis.pr_id,
                analysis.repo,
                analysis.timestamp.isoformat(),
                json.dumps(analysis.change.files),
                json.dumps(analysis.change.kind),
            ),
        )

        for dep in analysis.dependencies:
            c.execute(
                "INSERT INTO dependencies (pr_id, repo, from_component, to_component, relation) VALUES (?, ?, ?, ?, ?)",
                (analysis.pr_id, analysis.repo, dep.from_component, dep.to_component, dep.relation),
            )

        for fact in analysis.facts:
            c.execute(
                "INSERT INTO facts (pr_id, repo, category, summary, detail, confidence, source, source_files, symbols) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    analysis.pr_id, analysis.repo,
                    fact.category, fact.summary, fact.detail,
                    fact.confidence, fact.source,
                    json.dumps(fact.files),
                    json.dumps(fact.symbols),
                ),
            )

        self._conn.commit()

    def get_analysis(self, pr_id: str, repo: str | None = None) -> PRAnalysis | None:
        """Get a full PR analysis by ID."""
        c = self._conn.cursor()
        if repo:
            c.execute("SELECT * FROM pr_analyses WHERE pr_id = ? AND repo = ?", (pr_id, repo))
        else:
            c.execute("SELECT * FROM pr_analyses WHERE pr_id = ?", (pr_id,))
        row = c.fetchone()
        if not row:
            return None

        pr_id_val = row["pr_id"]
        repo_val = row["repo"]

        deps = [
            Dependency(from_component=r["from_component"], to_component=r["to_component"], relation=r["relation"])
            for r in c.execute(
                "SELECT * FROM dependencies WHERE pr_id = ? AND repo = ?", (pr_id_val, repo_val)
            )
        ]
        facts = [
            Fact(
                category=r["category"],
                summary=r["summary"],
                detail=r["detail"],
                confidence=r["confidence"],
                source=r["source"],
                files=json.loads(r["source_files"]) if r["source_files"] else [],
                symbols=json.loads(r["symbols"]) if r["symbols"] else [],
            )
            for r in c.execute(
                "SELECT * FROM facts WHERE pr_id = ? AND repo = ?", (pr_id_val, repo_val)
            )
        ]

        return PRAnalysis(
            pr_id=pr_id_val,
            repo=repo_val,
            timestamp=datetime.fromisoformat(row["timestamp"]),
            change=ChangeUnit(id=pr_id_val, files=json.loads(row["files"]), kind=json.loads(row["kinds"])),
            dependencies=deps,
            facts=facts,
        )

    def get_component_impact(self, component: str) -> list[dict]:
        """Find all PRs where a component appears in dependencies."""
        # Strip common extensions so "scanner.py" matches "scanner"
        stem = component.rsplit(".", 1)[0] if "." in component else component
        # Also match partial paths: "scanner" matches "testigo_recall.scanner"
        pattern = f"%{stem}%"
        c = self._conn.cursor()
        rows = c.execute(
            "SELECT DISTINCT pr_id, repo, from_component, to_component, relation "
            "FROM dependencies WHERE from_component LIKE ? OR to_component LIKE ?",
            (pattern, pattern),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_recent_facts(self, category: str | None = None, limit: int = 20) -> list[dict]:
        """Get recent facts, optionally filtered by category."""
        c = self._conn.cursor()
        if category:
            rows = c.execute(
                "SELECT f.pr_id, f.repo, f.category, f.summary, f.detail, "
                "f.confidence, f.source, f.source_files, f.symbols, p.timestamp "
                "FROM facts f "
                "JOIN pr_analyses p ON f.pr_id = p.pr_id AND f.repo = p.repo "
                "WHERE f.category = ? "
                "ORDER BY p.timestamp DESC LIMIT ?",
                (category, limit),
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT f.pr_id, f.repo, f.category, f.summary, f.detail, "
                "f.confidence, f.source, f.source_files, f.symbols, p.timestamp "
                "FROM facts f "
                "JOIN pr_analyses p ON f.pr_id = p.pr_id AND f.repo = p.repo "
                "ORDER BY p.timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._format_fact_row(r) for r in rows]

    def get_facts_by_module(self, pr_id: str) -> list[dict]:
        """Get all facts for a specific module/PR."""
        c = self._conn.cursor()
        rows = c.execute(
            "SELECT f.pr_id, f.repo, f.category, f.summary, f.detail, "
            "f.confidence, f.source, f.source_files, f.symbols "
            "FROM facts f WHERE f.pr_id = ? "
            "ORDER BY f.category, f.confidence DESC",
            (pr_id,),
        ).fetchall()
        return [self._format_fact_row(r) for r in rows]

    def search(
        self,
        term: str,
        category: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 50,
    ) -> list[dict]:
        """Full-text search across all facts with BM25 ranking."""
        words = term.split()
        if not words:
            return []
        fts_query = " OR ".join(f'"{w}"' for w in words)

        try:
            return self._fts_search(fts_query, category, min_confidence, limit)
        except sqlite3.OperationalError:
            return self._like_search(term, category, min_confidence, limit)

    def _fts_search(
        self, fts_query: str, category: str | None, min_confidence: float, limit: int,
    ) -> list[dict]:
        """Search using FTS5 with BM25 relevance ranking."""
        c = self._conn.cursor()
        query = (
            "SELECT f.pr_id, f.repo, f.category, f.summary, f.detail, "
            "f.confidence, f.source, f.source_files, f.symbols, "
            "pa.timestamp, "
            "bm25(facts_fts, 10.0, 5.0) AS relevance "
            "FROM facts_fts fts "
            "JOIN facts f ON f.id = fts.rowid "
            "JOIN pr_analyses pa ON pa.pr_id = f.pr_id AND pa.repo = f.repo "
            "WHERE facts_fts MATCH ?"
        )
        params: list = [fts_query]

        if category:
            query += " AND f.category = ?"
            params.append(category)
        if min_confidence > 0:
            query += " AND f.confidence >= ?"
            params.append(min_confidence)

        query += " ORDER BY relevance, CASE WHEN f.source = 'scan' THEN 0 ELSE 1 END, pa.timestamp DESC LIMIT ?"
        params.append(limit)

        rows = c.execute(query, params).fetchall()
        return [self._format_fact_row(r) for r in rows]

    def _like_search(
        self, term: str, category: str | None, min_confidence: float, limit: int,
    ) -> list[dict]:
        """Fallback search using LIKE patterns."""
        c = self._conn.cursor()
        pattern = f"%{term}%"
        query = (
            "SELECT f.pr_id, f.repo, f.category, f.summary, f.detail, "
            "f.confidence, f.source, f.source_files, f.symbols, pa.timestamp "
            "FROM facts f "
            "JOIN pr_analyses pa ON pa.pr_id = f.pr_id AND pa.repo = f.repo "
            "WHERE (f.summary LIKE ? OR f.detail LIKE ?)"
        )
        params: list = [pattern, pattern]

        if category:
            query += " AND f.category = ?"
            params.append(category)
        if min_confidence > 0:
            query += " AND f.confidence >= ?"
            params.append(min_confidence)

        query += " ORDER BY f.confidence DESC LIMIT ?"
        params.append(limit)

        rows = c.execute(query, params).fetchall()
        return [self._format_fact_row(r) for r in rows]

    @staticmethod
    def _format_fact_row(row: sqlite3.Row) -> dict:
        """Format a DB row into a self-describing dict for AI consumption."""
        d = dict(row)
        if "source_files" in d:
            d["source_files"] = json.loads(d["source_files"]) if d["source_files"] else []
        if "symbols" in d:
            d["symbols"] = json.loads(d["symbols"]) if d["symbols"] else []
        return d
