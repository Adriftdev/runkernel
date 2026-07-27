# Agent Directives for Runkernel Development

This repository contains **Runkernel**, a local-first, code-native Rust task graph engine. This document governs how AI agents should interact with, debug, and extend the engine's core source code across its workspace crates.

## Technology Stack & Workspace

- **Language:** Rust (Strictly enforced)
- **Async Runtime:** Tokio (`futures::stream::FuturesUnordered`)
- **Core Crates:**
  - `crates/runkernel`: Core DAG scheduler, caching engine, and task state.
  - `crates/runkernel-cli`: User-facing CLI and manifest discovery.
  - `crates/runkernel-cli-support`: The `__runkernel` internal IPC protocol handler.
- **Core Paradigms:** Type-safe DAG execution, deterministic SHA-256 caching, and strict separation between the CLI runner and the compiled workflow binaries.

## Concrete Execution Commands

Do not guess command flags. Use these exact commands to validate the workspace:

- **Format check:** `cargo fmt --all -- --check`
- **Linting:** `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- **Testing:** `cargo test --workspace`
  _(Note: If running inside a sandboxed command runner, you must invoke tests with `BypassSandbox: true` to access standard toolchains)._

## Architectural Boundaries

### 🔴 Never Do

- **Never introduce distributed/remote execution:** Runkernel v0.1 is explicitly bounded as a local-first engine. Do not add Kubernetes backends, distributed queues, or remote worker logic to the core engine.
- **Never parse YAML pipelines:** Rust is the absolute source of truth. `runkernel.toml` is strictly for manifest discovery and CLI defaults, not for defining tasks or dependencies.
- **Never break the `__runkernel` protocol:** The `runkernel-cli` and `runkernel-cli-support` crates communicate via a hidden IPC protocol over stdout. Never write raw text to stdout in the support crate that breaks the expected JSON schema (`PROTOCOL_VERSION = 1`).
- **Never block the Tokio executor:** The DAG scheduler (`pipeline.rs`) relies on asynchronous concurrency. Do not use blocking I/O or `std::thread::sleep` inside the execution loops.

### 🟡 Ask First

- **Before modifying DFS Cycle Detection:** The topological sorting algorithm in `pipeline.rs` is critical for DAG validation. Propose algorithmic changes before modifying the `Unvisited/Visiting/Visited` state machine.
- **Before changing Cache Identity rules:** Modifying how `cache.rs` computes the SHA-256 hash (involving globs, env vars, and shell commands) risks breaking cache determinism across the entire ecosystem.
- **Before altering shared state:** Inter-task output passing relies on a thread-safe `Arc<Mutex<HashMap<...>>>`. Propose locking strategy changes before implementation to avoid deadlocks during parallel task execution.

### 🟢 Always Do

- **Maintain collision-safe cache logic:** When modifying cache storage, ensure paths remain sanitized and retain the 16-character hex hash suffix (`{sanitized_task_name}-{hash16}.json`) to prevent naming collisions.
- **Propagate exact failure state:** Ensure that changes to task execution properly honor the `FailurePolicy` (`FailFast`, `FinishRunning`, `ContinueIndependent`) and correctly trigger the configured `RollbackPolicy`.
- **Update Protocol Serializers:** If you add a new capability or attribute to `Task`, you must update the JSON serialization in `runkernel-cli-support` so the CLI can correctly render it in `runkernel explain` and `runkernel graph`.
