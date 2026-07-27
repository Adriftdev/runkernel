# Runkernel: Pure-Rust Task Graph Engine for Build, Ops & Deployment

> **The Ethos:** Tooling should not force you into a proprietary configuration language (like YAML) when the host language is already expressive, typed, and testable.

This document serves as an architectural guide, developer manual, and design reference for AI agents and developers working on the `runkernel` codebase.

---

## 1. System Philosophy & Ethos

Modern infrastructure engineering suffers from "YAML inflation." We took serialization formats and forced them to handle loops, conditions, string interpolation, and dependency execution graphs. As a result:

- We lose IDE autocompletion, hover documentation, and jump-to-definition.
- We lose type-safety for parameters and environment variables.
- We cannot easily write unit tests for deployment pipelines.

`runkernel` returns control flow, dependency management, and execution orchestration back to **compiled Rust**. It acts as a lightweight, library-first orchestrator typically compiled inside an `ops/` or workflow binary in a repository.

---

## 2. Core Architecture & Layout

The project is structured as a Cargo workspace:

```
/Users/adrift/projects/rote/
├── Cargo.toml                      # Workspace configuration
├── crates/
│   ├── runkernel/                  # Core library crate (Pipeline, Task, Context, CacheManager)
│   │   └── src/
│   │       ├── lib.rs              # Re-exports core types
│   │       ├── context.rs          # Runtime state, inter-task outputs, env helpers
│   │       ├── task.rs             # Task fluent builder, Shell enum, CacheMode
│   │       ├── cache.rs            # Deterministic SHA-256 caching & file globbing
│   │       └── pipeline.rs         # DAG execution loops, topological DFS sorting, rollback
│   ├── runkernel-cli/              # User-facing CLI tool
│   └── runkernel-cli-support/      # Support library for workflow binaries (__runkernel protocol)
├── examples/                       # Executable example workflows (basic, cache, ops, outputs, parallel, rollback)
├── .agents/skills/                 # AI Agent Skill definitions
│   ├── runkernel-usage/SKILL.md    # Skill for consuming runkernel as a library
│   └── runkernel-dev/SKILL.md      # Skill for developing and contributing to runkernel
└── runkernel.toml                  # Workflow configuration manifest
```

---

## 3. Agent Skills

Agent skills are available in the repository to guide AI agents:

- **Library Usage (`runkernel-usage`)**: See [.agents/skills/runkernel-usage/SKILL.md](.agents/skills/runkernel-usage/SKILL.md) for building workflows, defining shell and native async tasks, handling inputs/env vars, outputs, rollback, and CLI integration.
- **Engine Development (`runkernel-dev`)**: See [.agents/skills/runkernel-dev/SKILL.md](.agents/skills/runkernel-dev/SKILL.md) for extending the DAG scheduler, cache manager, CLI protocol, topological sort, and running test suites.

---

## 4. Run & Test Instructions

### Running Tests

To run all unit and integration tests across the workspace:

```bash
cargo test
```

### Running Example Workflows

To run the sample infrastructure deployment binary:

```bash
cargo run -p ops
```

To run via `runkernel-cli`:

```bash
cargo run -p runkernel-cli -- list
cargo run -p runkernel-cli -- run
```

### Testing Cache Hits

Run `cargo run -p ops` twice in succession. The second time, cached tasks will be skipped with `[CACHE]` status.
