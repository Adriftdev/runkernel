# runkernel

runkernel is a code-native Rust task graph engine for build, ops, and deployment workflows.

Instead of encoding control flow in YAML, runkernel lets a project define typed, testable workflow logic in Rust. It is currently a local, library-first engine: it runs named tasks, resolves dependencies as a DAG, executes independent branches concurrently, skips deterministic cache hits, reports structured results, emits lifecycle events, passes typed outputs between tasks, and supports rollback policies for failure handling.

## Basic Example

```rust
use runkernel::{Pipeline, Task};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut pipeline = Pipeline::new("build");
    pipeline.add(Task::new("format").exec("cargo fmt --check"));
    pipeline.add(Task::new("test").depends_on(&["format"]).exec("cargo test"));

    let result = pipeline.run().await?;
    if !result.summary.success {
        anyhow::bail!("pipeline failed: {:?}", result.summary);
    }

    Ok(())
}
```

`Pipeline::run()` returns `anyhow::Result<PipelineResult>`. Invalid graphs, invalid glob patterns, and setup errors return `Err`. Task execution failures are represented in the returned `PipelineResult` so callers can inspect failed, skipped, cached, cancelled, and rolled-back tasks directly.

## DAG Dependencies

```rust
pipeline.add(Task::new("lint").exec("cargo clippy --all-targets"));
pipeline.add(Task::new("unit-test").exec("cargo test"));
pipeline.add(
    Task::new("package")
        .depends_on(&["lint", "unit-test"])
        .exec("cargo build"),
);
```

`lint` and `unit-test` can run concurrently. `package` starts only after both complete successfully or are restored from cache.

## Native Rust Tasks

```rust
use runkernel::{Context, Task};

pipeline.add(Task::new("build-wasm").exec_fn(|ctx: Context| async move {
    let target = ctx.env("TARGET_ENV")?;
    println!("building for {target}");
    Ok(())
}));
```

Native tasks can use normal Rust libraries, typed environment parsing, and task outputs.

## Caching

```rust
pipeline.add(
    Task::new("build")
        .exec("cargo build")
        .inputs(&["src/**/*.rs", "Cargo.toml", "Cargo.lock"])
        .env_vars(&["TARGET_ENV"])
        .cache_key("build-v1"),
);
```

Cache entries are scoped by pipeline namespace and task name under `.runkernel/cache/{pipeline_hash}/`. Cache identity includes the pipeline name, task name, declared dependencies, shell command, explicit cache key, declared environment values, matched file paths, and file contents.

Native function tasks should use `.cache_key(...)` when they opt into caching because runkernel cannot hash closure logic.

## Outputs

```rust
pipeline.add(Task::new("build").exec_fn(|ctx| async move {
    ctx.set_output("artifact", "dist/app.wasm")?;
    Ok(())
}));

pipeline.add(
    Task::new("deploy")
        .depends_on(&["build"])
        .exec_fn(|ctx| async move {
            let artifact: String = ctx.output_from("build", "artifact")?;
            println!("deploying {artifact}");
            Ok(())
        }),
);
```

Outputs are stored as JSON values and deserialized through serde. A task can only read outputs from tasks that have completed.

## Rollback

```rust
use runkernel::{Pipeline, RollbackPolicy, Task};

let mut pipeline = Pipeline::new("release")
    .rollback_policy(RollbackPolicy::CompletedTasksReverseOrder);

pipeline.add(
    Task::new("provision")
        .exec_fn(|_| async move { Ok(()) })
        .rollback(|_| async move {
            println!("undo provision");
            Ok(())
        }),
);
```

`RollbackPolicy::FailedTaskOnly` is the default for compatibility with `.on_failure(...)`. `CompletedTasksReverseOrder` rolls back completed tasks with rollback handlers after a pipeline failure.

## Events

```rust
use runkernel::PipelineEvent;

let pipeline = Pipeline::new("observed").with_callback(|event| {
    if let PipelineEvent::TaskCompleted { name, duration } = event {
        println!("{name} completed in {duration:?}");
    }
});
```

Events include queued, started, cached, completed, failed, skipped, cancelled, rollback, and pipeline-finished transitions. They are intended for CLIs and UIs without stdout scraping.

## Graph and Explain

```rust
let dot = pipeline.to_dot()?;
let explanation = pipeline.explain_task("build")?;
```

`to_dot()` exports the validated DAG as Graphviz DOT. `explain_task()` returns an inspectable summary of a task's dependencies, dependents, action kind, cache configuration, declared inputs, environment variables, shell, and rollback handler presence.

## CLI

The workspace includes a CLI binary named `runkernel`:

```bash
cargo run -p runkernel-cli -- list
cargo run -p runkernel-cli -- graph
cargo run -p runkernel-cli -- explain deploy-edge
cargo run -p runkernel-cli -- run deploy-edge
cargo run -p runkernel-cli -- cache status
cargo run -p runkernel-cli -- cache clean
```

`runkernel.toml` does not define tasks. It tells the CLI where your Rust workflow lives:

```toml
[workflow.default]
package = "ops"
bin = "ops"
manifest_path = "examples/ops/Cargo.toml"
working_dir = "."
default_task = "deploy-edge"
description = "Example ops workflow"
```

The CLI discovers `runkernel.toml` by walking up from the current directory. It delegates to the configured Cargo binary with the internal `__runkernel` protocol for list, graph, explain, and selected task execution. Workflow binaries use `runkernel-cli-support::RunkernelApp` to expose that protocol.

## Current Limitations

- runkernel is a local library, not a distributed workflow system.
- Shell execution defaults to `sh -c`; Windows shells are modeled but not broadly tested.
- Cache storage is local JSON under `.runkernel/cache`.
- Function task caching requires explicit user cache keys.
- The CLI runs Rust workflow binaries discovered through `runkernel.toml`; the manifest does not define tasks.
- There is no remote worker, Kubernetes, web UI, plugin system, or YAML pipeline format.

## Roadmap

- Stabilize the public API and docs.
- Continue hardening deterministic caching.
- Expand structured result and event coverage.
- Improve shell stdout/stderr configuration.
- Improve workflow protocol output formats and shell stdout/stderr controls.
