# testigo-recall-mcp

MCP server that exposes a pre-scanned codebase knowledge base to AI agents. Instead of reading source files directly, agents query pre-extracted facts about code behavior, design decisions, and assumptions — saving time and tokens.

Works with Claude Code, Cursor, Windsurf, and any MCP-compatible client.

## Installation

```bash
pip install testigo-recall-mcp
```

## Configuration

### GitHub Releases (default)

Auto-downloads knowledge base `.db` files from a GitHub release:

```json
{
  "mcpServers": {
    "testigo-recall": {
      "command": "testigo-recall-mcp",
      "env": {
        "TESTIGO_RECALL_REPO": "owner/repo",
        "GITHUB_TOKEN": "ghp_..."
      }
    }
  }
}
```

### Azure Blob Storage

Downloads `.db` files from an Azure Blob Storage container. Auth uses your existing `az login` session — no secrets needed:

```json
{
  "mcpServers": {
    "testigo-recall": {
      "command": "testigo-recall-mcp",
      "env": {
        "TESTIGO_RECALL_AZURE_URL": "https://account.blob.core.windows.net/container"
      }
    }
  }
}
```

Auth priority: SAS token > `az login` bearer token > public container.

### Local files

Point directly at local `.db` files (useful for development/demos):

```json
{
  "mcpServers": {
    "testigo-recall": {
      "command": "testigo-recall-mcp",
      "env": {
        "TESTIGO_RECALL_DB_PATH": "/path/to/knowledge.db"
      }
    }
  }
}
```

All backends can be combined — DBs are merged at startup.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `TESTIGO_RECALL_REPO` | GitHub repo(s), comma-separated (e.g. `owner/repo`). Auto-downloads from `knowledge-base` release tag. |
| `TESTIGO_RECALL_AZURE_URL` | Azure Blob Storage container URL(s), comma-separated. |
| `TESTIGO_RECALL_AZURE_SAS` | Optional SAS token for Azure (read+list). Not needed if `az login` is active. |
| `TESTIGO_RECALL_DB_PATH` | Local `.db` file path(s), comma-separated. |
| `GITHUB_TOKEN` | GitHub token for private repos. Public repos work without auth. |

## Tools

The server exposes 6 tools to AI agents:

### `search_codebase`
Full-text search across the knowledge base. Returns facts ranked by relevance. Supports semicolon-separated multi-query batching (e.g. `"auth login; session JWT; middleware"`).

- `query` — search keywords
- `category` — optional filter: `behavior`, `design`, or `assumption`
- `min_confidence` — confidence threshold 0.0-1.0
- `limit` — max results (default: 20)
- `repo_name` — optional filter to scope to one repository

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
Lists all scanned modules in the knowledge base. Call without arguments for a compact repo summary.

- `repo_name` — optional repository filter

### `get_repo_dependencies`
Cross-repo dependency graph from package manifests (go.mod, package.json).

- `repo_name` — optional filter
- `direction` — `outgoing`, `incoming`, or `both`

## How It Works

The knowledge base contains pre-extracted facts organized by category:

- **behavior** — what the code does (triggers, outcomes)
- **design** — how it's built (decisions, patterns, trade-offs)
- **assumption** — what it expects (invariants, prerequisites)

Facts come from two sources:
- **SCAN facts** (`SCAN:module/path`) — current state of a module, refreshed automatically
- **PR facts** (`PR-123`) — what a specific PR changed, preserved as history

The server uses SQLite with FTS5 full-text search for fast, relevance-ranked queries.

## Multi-repo support

All backends support multiple repositories in a single MCP server instance. Each repo gets its own `.db` file, stored in a shared location (GitHub release, Azure container, or local directory). The server downloads all `.db` files and merges them into a unified knowledge base at startup.

```
knowledge-base/
  ├── frontend-app.db
  ├── backend-api.db
  ├── shared-lib.db
  └── infrastructure.db
```

Use `list_modules()` to see all available repos, and `repo_name` parameter on `search_codebase` to scope queries.
