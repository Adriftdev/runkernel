# Agent Directives for Runkernel Development

> **The Ethos:** Tooling should not force you into a proprietary configuration language (like YAML) when the host language is already expressive, typed, and testable. Runkernel cures "YAML inflation" by returning control flow and dependency execution back to **compiled Rust**.

This document governs how AI agents should interact with, debug, and extend the Runkernel core engine across its workspace crates.

## 1. Skill Routing

Before executing complex tasks or architecture changes, you must ingest the relevant skill file:

- **Engine Development (`runkernel-dev`)**: See `.agents/skills/runkernel-dev/SKILL.md` for instructions on extending the DAG scheduler, cache manager, DFS sorting, and internal protocol.
- **Library Usage (`runkernel-usage`)**: See `.agents/skills/runkernel-usage/SKILL.md` for building workflows and using the engine as a consumer.

## 2. Technology Stack & Workspace

- **Language:** Rust (Strictly enforced)
- **Async Runtime:** Tokio
- **Workspace Layout:**
  - `crates/runkernel`: Core DAG scheduler, caching engine, and task state.
  - `crates/runkernel-cli`: User-facing CLI and manifest discovery.
  - `crates/runkernel-cli-support`: Support library handling the `__runkernel` internal IPC protocol.

## 3. Execution & Testing Commands

Do not guess command flags. Use these exact commands to validate the workspace:

- **Format check:** `cargo fmt --all -- --check`
- **Linting:** `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- **Testing:** `cargo test --workspace` _(Note: If sandboxed, invoke with `BypassSandbox: true`)_
- **Run Workflow Examples:** `cargo run -p ops` (Run twice to verify `[CACHE]` hits)
- **Run CLI manually:** `cargo run -p runkernel-cli -- list`

## 4. Architectural Boundaries

### 🔴 Never Do

- **Never introduce distributed/remote execution:** Runkernel v0.1 is strictly local-first. Do not add remote worker logic, distributed queues, or Kubernetes dependencies to the core engine.
- **Never parse YAML pipelines:** `runkernel.toml` is strictly for manifest discovery and CLI defaults, never for defining tasks or dependencies. Rust is the absolute source of truth.
- **Never break the `__runkernel` protocol:** The CLI and support crates communicate via JSON over stdout. Never write raw text to stdout in the support crate that breaks the expected JSON schema (`PROTOCOL_VERSION = 1`).
- **Never block the Tokio executor:** The DAG scheduler (`pipeline.rs`) relies on asynchronous concurrency. Do not use blocking I/O or `std::thread::sleep` inside the execution loops.

### 🟡 Ask First

- **Before modifying DFS Cycle Detection:** The topological sorting algorithm in `pipeline.rs` is critical for validation. Propose algorithmic changes before modifying the `Unvisited/Visiting/Visited` state machine.
- **Before changing Cache Identity rules:** Modifying how `cache.rs` computes the SHA-256 hash (involving globs, env vars, and shell commands) risks breaking cache determinism across the entire ecosystem.
- **Before altering shared state:** Inter-task output passing relies on a thread-safe `Arc<Mutex<HashMap<...>>>`. Propose locking strategy changes before implementation to avoid deadlocks during parallel task execution.

### 🟢 Always Do

- **Maintain collision-safe cache logic:** When modifying cache storage, ensure paths remain sanitized and retain the 16-character hex hash suffix (`{sanitized_task_name}-{hash16}.json`) to prevent naming collisions.
- **Propagate exact failure state:** Ensure that changes to task execution properly honor the `FailurePolicy` (`FailFast`, `FinishRunning`, `ContinueIndependent`) and correctly trigger the configured `RollbackPolicy`.
- **Update Protocol Serializers:** If you add a new capability or attribute to `Task`, you must update the JSON serialization in `runkernel-cli-support` so the CLI can correctly render it in `runkernel explain` and `runkernel graph`.
