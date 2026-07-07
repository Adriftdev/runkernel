# runkernel Design

## Execution Model

runkernel stores tasks in a named pipeline and validates the dependency graph before execution. Validation rejects missing dependencies and circular dependencies. Ready tasks are scheduled deterministically by task name, and independent branches run concurrently.

`Pipeline::run()` returns `anyhow::Result<PipelineResult>`. Graph and setup failures return `Err`. Task failures are captured in `PipelineResult` with per-task status, duration, error, cache state, and rollback state.

Failure policy controls scheduling after a task fails:

- `FailFast`: stop scheduling, cancel active work where possible, and mark active tasks cancelled.
- `FinishRunning`: stop scheduling new work and let active tasks finish.
- `ContinueIndependent`: skip tasks that depend on failed tasks, but continue unrelated branches.

The default is `FinishRunning`.

## Cache Model

Tasks can use input-based caching, explicit cache keys, or disabled caching. Cache entries live under `.runkernel/cache/{pipeline_hash}/{task_name}.json`.

Cache identity includes:

- pipeline name
- task name
- declared dependencies
- shell command and shell override
- explicit cache key
- declared environment variable values
- declared input patterns
- matched input file paths and file contents

Missing glob matches are deterministic and do not fail. Invalid glob patterns return an error. Native function tasks cannot be hashed by code body, so explicit cache keys are the intended invalidation control for those tasks.

## Failure Model

A task failure does not discard execution results. The pipeline result reports failed, skipped, cached, completed, cancelled, and rolled-back tasks. `summary.success` is false when any task failed, was skipped because of failure, was cancelled, or had a rollback failure.

## Rollback Model

Rollback is task-local code controlled by a pipeline rollback policy:

- `Disabled`: no rollback handlers run.
- `FailedTaskOnly`: run only the failed task handler. This is the default and backs `.on_failure(...)`.
- `CompletedTasksReverseOrder`: after failure, roll back completed tasks with handlers in reverse completion order.

Rollback failures are recorded on task results and do not hide the original task failure.

## Context Model

`Context` provides the current task name, pipeline name, workspace root, environment helpers, and typed task outputs.

Outputs are held in an in-memory shared store for the current run. Successful cached tasks restore cached outputs before dependents are scheduled, so downstream tasks can consume cached producer outputs.

## CLI Model

The CLI should remain a thin wrapper over the library:

- `runkernel run [task]`
- `runkernel list`
- `runkernel graph`
- `runkernel explain <task>`
- `runkernel workflows`
- `runkernel cache clean`
- `runkernel cache status`

`runkernel.toml` describes workflow binary locations, not task definitions. The CLI discovers the manifest, selects a workflow, and delegates to Cargo with protocol arguments after Cargo's `--`.

Workflow binaries use `runkernel-cli-support::RunkernelApp` to expose `__runkernel metadata/list/graph/explain/run`. Machine-readable protocol commands return JSON, while `run` preserves normal workflow output and exit status.
