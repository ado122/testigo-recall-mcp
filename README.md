# testigo-recall-mcp

MCP server that exposes a pre-scanned codebase knowledge base to AI agents. Instead of reading source files directly, agents query pre-extracted facts about code behavior, design decisions, and assumptions — saving time and tokens.

Works with Claude Code, Cursor, Windsurf, and any MCP-compatible client.

## Installation

```bash
pip install testigo-recall-mcp
```

## Configuration

### Claude Code

Add to your Claude Code MCP settings (`~/.claude/mcp.json` or project-level):

```json
{
  "mcpServers": {
    "testigo-recall": {
      "command": "testigo-recall-mcp",
      "env": {
        "TESTIGO_RECALL_REPO": "owner/repo"
      }
    }
  }
}
```

### Cursor / Windsurf

Add to your MCP configuration:

```json
{
  "mcpServers": {
    "testigo-recall": {
      "command": "testigo-recall-mcp",
      "env": {
        "TESTIGO_RECALL_REPO": "owner/repo"
      }
    }
  }
}
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `TESTIGO_RECALL_REPO` | GitHub repo (e.g. `owner/repo`). Auto-downloads the knowledge base from the `knowledge-base` release tag. |
| `TESTIGO_RECALL_DB_PATH` | Explicit path to a local SQLite knowledge base file. Takes priority over repo sync. |
| `GITHUB_TOKEN` | GitHub token for private repos. Public repos work without auth. |

## Tools

The server exposes 5 tools to AI agents:

### `search_codebase`
Full-text search across the knowledge base. Returns facts ranked by relevance.

- `query` — search keywords (e.g. "authentication", "payment flow")
- `category` — optional filter: `behavior`, `design`, or `assumption`
- `min_confidence` — confidence threshold 0.0-1.0
- `limit` — max results (default: 20)

### `get_module_facts`
Deep dive into a specific module. Use `search_codebase` first to discover module IDs.

- `module_id` — e.g. `SCAN:backend/app/api` or `PR-123`

### `get_recent_changes`
Most recently extracted facts across the codebase.

- `category` — optional filter
- `limit` — number of results (default: 10)

### `get_component_impact`
Blast radius analysis — shows what depends on a component and what it depends on.

- `component_name` — file path or service name (e.g. `api_service.py`)

### `list_modules`
Lists all scanned modules in the knowledge base.

- `repo_name` — optional repository filter

## How It Works

The knowledge base contains pre-extracted facts organized by category:

- **behavior** — what the code does (triggers, outcomes)
- **design** — how it's built (decisions, patterns, trade-offs)
- **assumption** — what it expects (invariants, prerequisites)

Facts come from two sources:
- **SCAN facts** (`SCAN:module/path`) — current state of a module, refreshed automatically
- **PR facts** (`PR-123`) — what a specific PR changed, preserved as history

The server uses SQLite with FTS5 full-text search for fast, relevance-ranked queries.
