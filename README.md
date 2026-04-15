# disk-inventory

Fast, AI-friendly disk usage analysis. Crawls your filesystem into a SQLite index, then answers questions about where your space is going — via CLI, MCP (for Claude/AI assistants), or JSON.

Think ncdu meets Disk Inventory X, but queryable by an AI.

## Quick Start

```bash
# Build
cargo build --release

# Index your home directory (first run takes 1-2 minutes)
disk-inventory daemon run --once

# Where is my space going?
disk-inventory usage

# Largest files
disk-inventory top --files

# What can I clean up?
disk-inventory waste

# Use with Claude Code (MCP)
# Add to your .mcp.json — see MCP Setup below
```

## What It Does

**Crawls** your filesystem with parallel directory walking (jwalk), collecting file metadata into a SQLite database with WAL mode for concurrent access.

**Indexes** everything: sizes (actual disk allocation, not logical), timestamps, extensions, directory hierarchy. Pre-computes recursive directory sizes so any query is instant.

**Answers questions** through three interfaces:
- **CLI** with human-readable tables, JSON, or CSV output
- **MCP server** so Claude or other AI assistants can analyze your disk
- **JSON** for scripting and piping

## CLI Commands

```
disk-inventory usage [path]              # Directory size breakdown
disk-inventory top [path]                # Largest files and directories
disk-inventory waste [path]              # Reclaimable space (node_modules, caches, build artifacts)
disk-inventory search <pattern> [path]   # Find files by name
disk-inventory types [path]              # Breakdown by file extension
disk-inventory trends [path]             # What's growing over time
disk-inventory duplicates [path]         # Find duplicate files

disk-inventory daemon run                # Start long-running daemon
disk-inventory daemon run --once         # Single crawl and exit
disk-inventory daemon status             # Show scan progress
disk-inventory daemon status --watch     # Live-updating status
disk-inventory daemon status --wait      # Block until scan completes
disk-inventory daemon install            # Install as OS service (launchd/systemd)
disk-inventory daemon uninstall          # Remove OS service
disk-inventory daemon log                # Show daemon logs
disk-inventory daemon rescan [path]      # Trigger rescan via IPC

disk-inventory mcp                       # Start MCP server (stdio)
```

All query commands support `--format json`, `--format csv`, or `--format table` (default).

## MCP Setup

### Claude Code

From the CLI:

```bash
claude mcp add disk-inventory /path/to/disk-inventory -- mcp
```

Or inside Claude Code, run `/mcp` → "Add new MCP server" → stdio → command: `/path/to/disk-inventory` → args: `mcp`.

### Cursor

Add to your Cursor MCP config (Settings → MCP Servers → Add):

```json
{
  "mcpServers": {
    "disk-inventory": {
      "command": "/path/to/disk-inventory",
      "args": ["mcp"]
    }
  }
}
```

Or add directly to `~/.cursor/mcp.json`.

### opencode

Add to your `opencode.json`:

```json
{
  "mcp": {
    "disk-inventory": {
      "type": "stdio",
      "command": "/path/to/disk-inventory",
      "args": ["mcp"]
    }
  }
}
```

### Other MCP Clients

Any client supporting MCP stdio transport can use disk-inventory. The server binary is:

```
command: /path/to/disk-inventory
args:    mcp
transport: stdio
```

The server reads from stdin, writes JSON-RPC to stdout, logs to stderr.

### MCP Tools

| Tool | Description |
|------|-------------|
| `disk_overview` | High-level usage summary with directory breakdown |
| `find_large_items` | Largest files/directories with filtering by type, extension, age |
| `find_waste` | Reclaimable space: node_modules, build artifacts, caches, logs (17 categories) |
| `find_duplicates` | Duplicate files by content hash (tiered: size grouping, partial hash, full hash) |
| `disk_usage_by_type` | Breakdown by file extension |
| `disk_trends` | Historical growth analysis (what grew most this week/month) |
| `search_files` | Flexible file search by name pattern, size, and date |
| `scan_status` | Index freshness, active scan progress, trigger rescan |

## Waste Detection

Built-in detection for 17 categories of reclaimable space, each with a safety rating:

| Category | Safety | Examples |
|----------|--------|----------|
| `node_modules` | Safe | npm/yarn/pnpm dependency trees |
| `build_artifacts` | Safe | Rust `target/`, Python `__pycache__`, Go build cache |
| `package_caches` | Safe | npm, pip, Homebrew download caches |
| `xcode` | Safe/Review | DerivedData, old simulator runtimes |
| `docker` | Review | Images, containers, build cache |
| `log_files` | Review | `*.log` files |
| `virtual_envs` | Review | Python `.venv`/`venv` directories |
| `git_data` | Caution | Large `.git` directories |
| `trash` | Safe | `~/.Trash` contents |
| `system_caches` | Review | `~/Library/Caches` |

Custom rules can be added in `~/.disk-inventory/config.toml`:

```toml
[[waste_rules]]
name = "Unity Library"
pattern = "**/Library/Bee"
category = "build_artifacts"
safety = "safe"
cleanup = "Unity rebuilds on next open"
```

## Architecture

Two-process design:

```
disk-inventory daemon run     writes to -->  ~/.disk-inventory/index.db (SQLite WAL)
disk-inventory usage          reads from ->  ~/.disk-inventory/index.db
disk-inventory mcp            reads from ->  ~/.disk-inventory/index.db
```

The **daemon** crawls the filesystem and maintains the index. It can run as a one-shot (`--once`), a long-lived process with periodic rescans, or an OS service.

The **CLI** and **MCP server** are read-only query layers over the shared database. SQLite WAL mode allows concurrent reads while the daemon writes.

### Key Design Decisions

- **Actual disk size** (`blocks * 512`) instead of logical file size — correctly handles APFS clones, sparse files, and compression
- **Pre-computed `dir_sizes` table** — O(1) lookup for any directory's recursive size
- **FTS5** for instant filename search
- **Incremental scanning** — mtime-based directory skipping for fast rescans
- **Cross-filesystem guard** — won't wander into network mounts or external volumes (configurable)
- **Debounced filesystem watcher** — FSEvents on macOS, inotify on Linux via the `notify` crate

## Configuration

`~/.disk-inventory/config.toml` (all fields optional, shown with defaults):

```toml
[daemon]
scan_interval_secs = 21600        # 6 hours between rescans
snapshot_interval_secs = 86400    # 1 day between history snapshots
watch_paths = ["~"]               # Paths to index

[scanner]
exclude_patterns = [
    ".Spotlight-V100", ".fseventsd", ".DocumentRevisions-V100",
    ".Trashes", ".vol", ".DS_Store", "Thumbs.db",
    ".disk-inventory", ".TimeMachine",
]
max_depth = 128
follow_symlinks = false
cross_filesystems = false

[database]
path = "~/.disk-inventory/index.db"
history_retention_days = 90

[waste]
disabled_categories = []          # e.g., ["old_downloads"] to suppress
```

## Building

Requires Rust 1.70+.

```bash
cargo build --release
# Binary at target/release/disk-inventory
```

### Platform Support

| Platform | Status |
|----------|--------|
| macOS (Apple Silicon) | Primary target, fully tested |
| macOS (Intel) | Supported |
| Linux (x86_64) | Supported |
| Linux (aarch64) | Supported |

## How It Works

### Crawl Pipeline

1. **Walk** — parallel directory traversal with jwalk
2. **Insert** — batch write file metadata to SQLite (10K rows/transaction)
3. **Mark deletions** — soft-delete entries not seen in this scan
4. **Compute dir_sizes** — bottom-up aggregation (deepest directories first)
5. **Extension stats** — materialized breakdown by file type
6. **Size history** — record directory sizes for trend analysis
7. **Compact history** — roll up old entries (daily -> weekly -> monthly)

### Incremental Scanning

After the initial full crawl, rescans use mtime-based directory skipping: only directories whose modification time changed since the last scan are re-examined. Unchanged subtrees are skipped entirely.

### Duplicate Detection

Three-tier approach minimizing I/O:

1. **Size grouping** (free) — files with unique sizes can't be duplicates, eliminating ~95% of files
2. **Partial hash** — first 4KB with xxhash64 for remaining candidates
3. **Full hash** — complete file xxhash64 only for files that match on size and partial hash

## License

MIT
