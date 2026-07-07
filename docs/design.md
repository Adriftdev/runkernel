# runkernel Design

## Product Boundary

`runkernel` is a local, Rust-native workflow kernel for build, ops, and deployment task graphs. Workflow logic is Rust code: `runkernel.toml` only locates a workflow binary, and does not define tasks, dependencies, conditionals, or a second task DSL.

The user-facing CLI should stay workflow-shaped:

- `runkernel list`
- `runkernel graph`
- `runkernel explain <task>`
- `runkernel run [task]`

Raw Cargo-shaped invocation is intentionally hidden. The CLI may delegate to `cargo run -- ... __runkernel ...` internally, but users should not need to pass package, bin, or manifest options for ordinary workflow use.

## Crate Responsibilities

`crates/runkernel` is the core library. It owns `Pipeline`, `Task`, `Context`, DAG validation and scheduling, shell/native task execution, deterministic local caching, typed task outputs, rollback policies, lifecycle events, graph inspection, task explanation, and structured results.

`crates/runkernel-cli` is the user-facing command. It discovers `runkernel.toml`, selects a workflow, resolves the configured Cargo target, validates protocol metadata for inspection commands, formats `list`/`graph`/`explain`, forwards run arguments, and preserves delegated process exit status.

`crates/runkernel-cli-support` is linked into workflow binaries. It exposes `RunkernelApp`, implements the internal `__runkernel` protocol, returns JSON metadata/list/graph/explain responses, and turns forwarded run arguments into `RunOptions`.

## Execution Model

`Pipeline` stores named tasks and validates the graph before execution. Validation rejects missing dependencies and circular dependencies. Ready tasks are scheduled deterministically by task name, and independent branches run concurrently.

`Pipeline::run()` executes the full runnable graph. `Pipeline::run_task(name)` trims execution to the selected task and its dependency closure, leaving unrelated tasks untouched.

`Pipeline::run()` returns `anyhow::Result<PipelineResult>`. Graph and setup failures return `Err`. Task failures are captured in `PipelineResult` with per-task status, duration, error, cache state, and rollback state.

Failure policy controls scheduling after a task fails:

- `FailFast`: stop scheduling, cancel active work where possible, and mark active tasks cancelled.
- `FinishRunning`: stop scheduling new work and let active tasks finish.
- `ContinueIndependent`: skip tasks that depend on failed tasks, but continue unrelated branches.

The default is `FinishRunning`.

## Context And Outputs

`Context` provides the current task name, pipeline name, workspace root, forwarded run arguments, environment helpers, and typed task outputs.

Arguments after `--` in `runkernel run [task] -- ...` are forwarded through the internal protocol and exposed through `Context::args()` for native tasks and rollback handlers.

Outputs are held in an in-memory shared store for the current run. A task can only read outputs from tasks that have completed. Successful cached tasks restore cached outputs before dependents are scheduled, so downstream tasks can consume cached producer outputs.

## Cache Model

Cache entries live under `.runkernel/cache/{pipeline_hash}/` with filenames shaped as `{sanitized_task_name}-{task_hash16}.json`. The hash suffix prevents collisions between task names like `foo/bar` and `foo_bar`.

Cache identity includes:

- pipeline name
- task name
- declared dependencies
- shell command and shell override
- explicit cache key
- declared environment variable values
- declared input patterns
- matched input file paths and file contents

Missing glob matches are deterministic and do not fail. Invalid glob patterns return an error. Native function task closure bodies cannot be hashed, so explicit cache keys, declared inputs, and declared environment variables are the intended invalidation controls. Cache explanations mention when a native function task depends only on declared identity because the closure body is not hashed.

## Rollback Model

Rollback is task-local code controlled by a pipeline rollback policy:

- `Disabled`: no rollback handlers run.
- `FailedTaskOnly`: run only the failed task handler. This is the default and backs `.on_failure(...)`.
- `CompletedTasksReverseOrder`: after failure, roll back completed tasks with handlers in reverse completion order.

Rollback failures are recorded on task results and do not hide the original task failure.

## CLI And Protocol Model

The CLI discovers `runkernel.toml` by walking upward from the current directory unless `--config` is supplied. Workflow selection uses `--workflow`, a single configured workflow, or a workflow named `default`; otherwise the CLI asks the user to choose.

The manifest describes workflow binary locations, not task definitions:

```toml
[workflow.default]
package = "ops"
bin = "ops"
manifest_path = "examples/ops/Cargo.toml"
working_dir = "."
default_task = "deploy-edge"
description = "Example ops workflow"
```

Workflow binaries use `runkernel-cli-support::RunkernelApp` to expose:

- `__runkernel metadata --format json`
- `__runkernel list --format json`
- `__runkernel graph --format json`
- `__runkernel explain <task> --format json`
- `__runkernel run [task] [-- args...]`

Before `list`, `graph`, and `explain`, the CLI validates metadata protocol version `1` and the required capability. `run` preserves normal workflow output and delegated exit status.

## Graph And Inspection

Graph APIs return all nodes and all dependency edges. Text, DOT, Mermaid, and JSON graph renderers must include isolated tasks. DOT output escapes quotes and backslashes. Mermaid output uses stable generated IDs for task names that are unsafe Mermaid identifiers while preserving labels.

`TaskExplanation` exposes the task name, description, dependencies, dependents, action kind, cache mode, declared inputs, declared environment variables, selected shell, and rollback presence.

## Release And Extension Points

The v0.1 release is local-only. CI must run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test`.

Future layers can add richer reporting, task grouping, shell completions, doctor checks, cache inspection, or experimental execution backends. Those extensions should preserve the v0.1 boundary: Rust code defines workflows, `runkernel.toml` locates workflows, and `runkernel` runs workflows.

Non-goals for v0.1:

- YAML or TOML task definitions
- remote workers
- distributed scheduling
- database-backed job state
- web UI
- Kubernetes backend
- plugin marketplace
- dynamic library workflow loading
