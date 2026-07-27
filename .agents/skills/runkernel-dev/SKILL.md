---
name: runkernel-dev
description: Architecture guide and developer manual for developing, testing, and maintaining the runkernel task engine, CLI, and internal protocol.
---

# `runkernel-dev`: Developing & Maintaining Runkernel

This guide is for developers and AI agents extending, debugging, or contributing to the `runkernel` codebase.

---

## 1. System Vision & Architecture

`runkernel` is a lightweight, local-first Rust workflow engine. It replaces rigid serialization formats (like YAML) with compiled, type-safe Rust control flow.

### Product Invariants & Non-Goals

- **Rust is Source of Truth**: Workflows are executable Rust code. `runkernel.toml` only locates workflow binaries; it NEVER defines tasks, dependencies, or conditionals.
- **Local Engine Boundary**: `runkernel` v0.1 is local-only. Remote workers, Kubernetes backends, distributed queues, database state, web UIs, and YAML pipeline parsers are explicitly out of scope for v0.1.
- **Strict Protocol Separation**: The user CLI (`runkernel`) communicates with workflow binaries via the internal `__runkernel` hidden CLI protocol.

---

## 2. Workspace Structure & Crate Responsibilities

```
/Users/adrift/projects/rote/
├── Cargo.toml                    # Workspace manifest
├── crates/
│   ├── runkernel/                # Core library crate
│   │   └── src/
│   │       ├── lib.rs            # Public exports
│   │       ├── pipeline.rs       # DAG scheduler, topological sort, cycle detection, execution loop
│   │       ├── task.rs           # Task builder pattern, Shell enum, CacheMode, closure definitions
│   │       ├── context.rs        # Runtime state, inter-task outputs, env helpers, forwarded args
│   │       └── cache.rs          # SHA-256 caching engine, glob expansion, cache cleaning
│   ├── runkernel-cli/            # User CLI application
│   │   └── src/
│   │       ├── main.rs           # Entry point
│   │       └── lib.rs            # Manifest discovery, workflow selection, cargo delegation, formatting
│   └── runkernel-cli-support/    # Workflow support crate
│       └── src/
│           └── lib.rs            # RunkernelApp, __runkernel protocol handler, JSON serialization
├── examples/                     # Workflow integration examples
│   ├── basic/                    # Minimal pipeline example
│   ├── cache/                    # Input/env var caching demo
│   ├── ops/                      # Infrastructure deployment workflow
│   ├── outputs/                  # Inter-task output passing demo
│   ├── parallel/                 # Concurrent task execution demo
│   └── rollback/                 # Failure recovery and unwinding demo
└── docs/                         # Design specs, publishing, roadmap
    ├── design.md
    ├── publishing.md
    └── roadmap.md
```

---

## 3. Core Engine Mechanics (`crates/runkernel`)

### A. DAG Validation & Cycle Detection (`pipeline.rs`)

Before any task executes, `Pipeline` validates the task graph:

1. **Missing Dependencies**: Every dependency listed in `task.dependencies` must exist in `pipeline.tasks`.
2. **Cycle Detection**: Topological sorting uses Depth-First Search (DFS) with node state tracking (`Unvisited`, `Visiting`, `Visited`). If a cycle is encountered (e.g. `A -> B -> A`), the exact cycle path is formatted and returned as an error (`anyhow::bail!`).

```rust
// Validation entry point in pipeline.rs
pub fn validate(&self) -> anyhow::Result<()>
```

### B. Concurrent Execution Loop (`pipeline.rs`)

Tasks run concurrently as soon as all their dependencies complete:

- Tasks are tracked in-memory using `tokio::spawn`.
- Ready tasks are scheduled concurrently using `futures::stream::FuturesUnordered`.
- `FailurePolicy` dictates behavior when a task fails:
  - `FailFast`: Immediately cancels running tasks and stops scheduling pending tasks.
  - `FinishRunning` (default): Lets active tasks finish, cancels pending unstarted tasks.
  - `ContinueIndependent`: Skips tasks dependent on the failed task, but continues executing independent branches.

### C. Selective Task Execution (`run_task`)

When running a specific target task (`pipeline.run_task("deploy")` or `runkernel run deploy`):
- `runkernel` computes the **dependency closure** (all recursive dependencies of `"deploy"`).
- Unrelated tasks outside the dependency closure are omitted from scheduling entirely.

### D. Inter-Task Output Store (`context.rs`)

- Outputs are stored in thread-safe shared state (`Arc<Mutex<HashMap<String, HashMap<String, Value>>>>`).
- Keys are namespaced by producer task name and output key.
- Downstream tasks can read outputs via `ctx.output_from::<T>(producer_task, key)`.
- On a **cache hit**, `CacheManager` restores cached outputs into the shared state without re-executing the task body.

---

## 4. Caching Engine Mechanics (`cache.rs`)

### Cache Identity Computation

The SHA-256 cache hash for a task is computed from:
1. Pipeline name
2. Task name
3. Declared dependencies (sorted)
4. Shell command (or `None` for native closures)
5. Explicit cache key (if provided)
6. Declared environment variable names and their current values
7. Matched file paths (sorted) and file contents for all declared glob patterns in `inputs`

### Collision-Safe Storage Layout

- Cache root: `.runkernel/cache/{pipeline_hash}/`
- Filename format: `{sanitized_task_name}-{hash16}.json`
- Example: `build_wasm-a31f95c02a19e0dd.json`
- Sanitization replaces unsafe path characters (`/`, `\`, `:`) with `_`.
- The 16-character hex hash suffix prevents collisions between tasks named `foo/bar` and `foo_bar`.

### Native Function Caching

Closure bodies cannot be hashed reflectively in Rust. Therefore, native function tasks (`exec_fn`) require an explicit `.cache_key(...)`, `.inputs(...)`, or `.env_vars(...)` to be marked cacheable. Task explanations call this out explicitly when inspected.

---

## 5. The Internal `__runkernel` Protocol (`runkernel-cli-support`)

Workflow binaries built with `runkernel-cli-support::RunkernelApp` handle hidden protocol invocations issued by `runkernel-cli`:

### Protocol Subcommands

| Subcommand | Output Format | Purpose |
| :--- | :--- | :--- |
| `__runkernel metadata` | `--format json` | Protocol version, workflow name, capabilities |
| `__runkernel list` | `--format json` | List of tasks, descriptions, dependencies, cacheability |
| `__runkernel graph` | `--format json` | Nodes and dependency edges (includes isolated nodes) |
| `__runkernel explain <task>` | `--format json` | Comprehensive task inspection breakdown |
| `__runkernel run [task] [-- args...]` | Text / Standard | Execute workflow or task, forwarding args after `--` |

### Protocol Invariants

- Protocol version: `PROTOCOL_VERSION = 1`
- Forwarded arguments: Passed after `--` on `runkernel run` (e.g. `runkernel run deploy -- --target prod`) and exposed to tasks via `Context::args()`.
- Process exit code: Preserved from workflow binary execution.

---

## 6. CLI Application Architecture (`crates/runkernel-cli`)

1. **Manifest Discovery**: Walks parent directories searching for `runkernel.toml`.
2. **Workflow Selection**: Resolves workflow by `--workflow` flag, `default` section, or single configured entry.
3. **Delegation**: Spawns `cargo run` with appropriate `--package`, `--bin`, or `--manifest-path` flags, passing `__runkernel ...` internally.
4. **Formatting**: Renders `list` as clean tables/lists, `graph` as DOT/Mermaid, and `explain` as formatted task cards.

---

## 7. Testing & Quality Standards

### Running Tests

All changes must pass unit and integration tests:

```bash
cargo test
```

> [!NOTE]
> If running in a sandboxed command runner where `/Users/.../.rustup` is outside workspace boundaries, invoke `run_command` with `BypassSandbox: true`.

### Code Quality Gates

Before submitting PRs or commits, ensure formatting and clippy lints pass cleanly:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

### Adding New Engine Features

1. **New Task Capabilities**: Add builder methods to `Task` in `crates/runkernel/src/task.rs`.
2. **New Cache Rules**: Modify hash computation in `crates/runkernel/src/cache.rs` and add unit tests in `cache::tests`.
3. **New Graph Renderers**: Add output formats in `crates/runkernel-cli/src/lib.rs` and ensure isolated tasks are rendered.
