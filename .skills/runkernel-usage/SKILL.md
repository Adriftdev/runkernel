---
name: runkernel-usage
description: Guide for consuming and operating runkernel as a Rust library to build code-native build, ops, and deployment task graphs.
---

# `runkernel-usage`: Using Runkernel as a Rust Library

`runkernel` is a code-native Rust task graph engine for build, ops, and deployment workflows. Instead of encoding pipeline logic in YAML, `runkernel` lets you define typed, testable, concurrent DAG workflows directly in Rust.

---

## 1. Cargo Dependencies

Add `runkernel` (and optionally `runkernel-cli-support` if building a CLI-supported workflow) to your `Cargo.toml`:

```toml
[dependencies]
runkernel = "0.1"
runkernel-cli-support = "0.1" # Optional: for CLI integration (__runkernel protocol)
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
anyhow = "1.0"
```

---

## 2. Core Library Usage Pattern

### Defining Pipelines & Tasks

A pipeline is a named container for tasks. Tasks declare dependencies, shell or native Rust execution logic, input files/env vars for caching, outputs, and rollback actions.

```rust
use runkernel::{Pipeline, Task, Shell, FailurePolicy, RollbackPolicy};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut pipeline = Pipeline::new("deployment");

    // 1. Basic Shell Task
    pipeline.add(
        Task::new("format")
            .description("Check code formatting")
            .exec("cargo fmt --check")
    );

    // 2. Task with Dependencies
    pipeline.add(
        Task::new("test")
            .description("Run unit test suite")
            .depends_on(&["format"])
            .exec("cargo test")
    );

    // 3. Task with Custom Shell
    pipeline.add(
        Task::new("lint")
            .description("Run clippy lints")
            .depends_on(&["format"])
            .exec_with(Shell::Bash, "cargo clippy --all-targets -- -D warnings")
    );

    // Run the full pipeline graph
    let result = pipeline.run().await?;
    if !result.summary.success {
        anyhow::bail!("Pipeline execution failed");
    }

    Ok(())
}
```

---

## 3. Task Types & Actions

### A. Shell Executable Tasks

Shell tasks execute commands via a subshell.

- Default shell: `sh -c` (`Shell::Sh`).
- Available shells: `Shell::Sh`, `Shell::Bash`, `Shell::Zsh`, `Shell::PowerShell`, `Shell::Cmd`, `Shell::Custom { program, args }`.

```rust
Task::new("build")
    .exec("cargo build --release")
    .exec_with(Shell::Bash, "scripts/deploy.sh")
```

### B. Native Async Rust Tasks (`exec_fn`)

Native tasks run async Rust closures with full access to the task `Context`.

```rust
use runkernel::{Context, Task};

pipeline.add(
    Task::new("prepare-config")
        .exec_fn(|ctx: Context| async move {
            let target_env = ctx.env("TARGET_ENV").unwrap_or_else(|_| "staging".to_string());
            println!("Preparing configuration for env: {target_env}");
            println!("Task args passed from CLI: {:?}", ctx.args());
            Ok(())
        })
);
```

---

## 4. Execution Context & Environment (`Context`)

Inside `exec_fn` closures and rollback handlers, `Context` provides:

- **Workspace Path**: `ctx.workspace_root()` -> `&Path`
- **Current Metadata**: `ctx.task_name()`, `ctx.pipeline_name()`
- **Forwarded CLI Arguments**: `ctx.args()` -> `&[String]`
- **Environment Variables**:
  - `ctx.env("VAR_NAME")` -> `anyhow::Result<String>`
  - `ctx.require_env::<T>()` -> Deserializes environment variables into typed structs (via `envy`).

```rust
#[derive(serde::Deserialize)]
struct DeployConfig {
    target_host: String,
    port: u16,
}

pipeline.add(Task::new("deploy").exec_fn(|ctx| async move {
    let cfg: DeployConfig = ctx.require_env()?;
    println!("Connecting to {}:{}", cfg.target_host, cfg.port);
    Ok(())
}));
```

---

## 5. Passing Typed Outputs Between Tasks

Tasks can output JSON-serializable values that downstream tasks consume once dependencies complete.

```rust
use runkernel::Task;

// Producer Task
pipeline.add(Task::new("build").exec_fn(|ctx| async move {
    ctx.set_output("artifact_path", "dist/app.wasm")?;
    ctx.set_output("version", "1.2.3")?;
    Ok(())
}));

// Consumer Task (must depend on producer)
pipeline.add(
    Task::new("deploy")
        .depends_on(&["build"])
        .exec_fn(|ctx| async move {
            let path: String = ctx.output_from("build", "artifact_path")?;
            let version: String = ctx.output_from("build", "version")?;
            println!("Deploying version {version} from {path}");
            Ok(())
        })
);
```

> [!IMPORTANT]
> A task can ONLY read outputs from tasks it directly or indirectly depends on that have finished executing. Reading outputs prematurely returns an error. When a producer task is skipped due to a cache hit, cached outputs are automatically restored so downstream tasks still receive them.

---

## 6. Deterministic Caching Engine

`runkernel` skips redundant task runs if declared inputs, environment variables, dependencies, and shell commands haven't changed.

```rust
Task::new("build")
    .exec("cargo build --release")
    .inputs(&["src/**/*.rs", "Cargo.toml", "Cargo.lock"])
    .env_vars(&["TARGET_ENV", "RUSTFLAGS"])
    .cache_key("build-v1") // Explicit cache key
```

### Caching Rules & Identity

1. **Shell Tasks**: Caching is enabled by default (`CacheMode::Inputs`). Hashing includes:
   - Pipeline name & Task name
   - Declared dependencies
   - Shell command string
   - Values of declared `env_vars`
   - Matched file paths & contents of declared `inputs` globs
2. **Native Rust Tasks (`exec_fn`)**: Closures cannot be hashed automatically. You MUST provide `.cache_key(...)`, `.inputs(...)`, or `.env_vars(...)` to make native tasks cacheable.
3. **Disabling Cache**: Call `.cache_disabled()` or set `CacheMode::Disabled`.
4. **Cache Location**: Cached results live under `.runkernel/cache/{pipeline_hash}/{sanitized_task_name}-{hash16}.json`.

---

## 7. Failure Policies & Rollback Handlers

### Failure Policies

Control how remaining tasks in the DAG are handled when a task fails:

```rust
use runkernel::FailurePolicy;

// Options:
// - FailurePolicy::FinishRunning (default): Stop scheduling new work, let running tasks finish.
// - FailurePolicy::FailFast: Cancel active tasks immediately and stop scheduling.
// - FailurePolicy::ContinueIndependent: Continue independent DAG branches, skip dependent branches.
let pipeline = Pipeline::new("my-pipeline")
    .failure_policy(FailurePolicy::FailFast);
```

### Rollback Handlers & Policies

Define cleanup logic when tasks or pipelines fail.

```rust
use runkernel::{Pipeline, RollbackPolicy, Task};

let mut pipeline = Pipeline::new("release")
    .rollback_policy(RollbackPolicy::CompletedTasksReverseOrder);

pipeline.add(
    Task::new("provision-server")
        .exec_fn(|_| async move {
            println!("Provisioning cloud instance...");
            Ok(())
        })
        .rollback(|ctx| async move {
            println!("Rollback: Destroying provisioned cloud instance...");
            Ok(())
        })
);

pipeline.add(
    Task::new("deploy-app")
        .depends_on(&["provision-server"])
        .exec_fn(|_| async move {
            anyhow::bail!("Deployment failed!")
        })
);
```

- `RollbackPolicy::FailedTaskOnly` (default): Only run rollback for the failed task.
- `RollbackPolicy::CompletedTasksReverseOrder`: Unwind completed tasks with rollback handlers in reverse order of completion after failure.
- `RollbackPolicy::Disabled`: Ignore all rollback handlers.
- `.on_failure(...)`: Convenience wrapper for simple non-failing cleanup logic (`Fn(Context) -> Future<Output = ()>`).

---

## 8. Pipeline Lifecycle Events

Subscribe to realtime events during DAG execution:

```rust
use runkernel::PipelineEvent;

let pipeline = Pipeline::new("observed").with_callback(|event| match event {
    PipelineEvent::TaskQueued { name } => println!("[QUEUED] {name}"),
    PipelineEvent::TaskStarted { name } => println!("[START] {name}"),
    PipelineEvent::TaskCompleted { name, duration } => println!("[DONE] {name} in {duration:?}"),
    PipelineEvent::TaskFailed { name, error } => println!("[FAIL] {name}: {error}"),
    PipelineEvent::TaskCached { name } => println!("[CACHE] {name}"),
    PipelineEvent::TaskSkipped { name, reason } => println!("[SKIP] {name}: {reason}"),
    _ => {}
});
```

---

## 9. Graph Export & Inspection

Inspect and visualize the task DAG before or after execution:

```rust
let dot_string = pipeline.to_dot()?;       // Export Graphviz DOT format
let mermaid_str = pipeline.to_mermaid()?;   // Export Mermaid diagram format
let explanation = pipeline.explain_task("deploy")?; // Inspect dependencies, inputs, shell, cache mode
```

---

## 10. CLI Integration (`runkernel-cli` & `runkernel.toml`)

To allow `runkernel-cli` to discover and run your Rust workflow:

### A. Define `runkernel.toml` in your project root or workspace

```toml
[workflow.default]
package = "ops"
bin = "ops"
manifest_path = "examples/ops/Cargo.toml"
working_dir = "."
default_task = "deploy"
description = "Production ops workflow"
```

### B. Wrap your workflow binary using `RunkernelApp`

```rust
use runkernel::{Pipeline, Task};
use runkernel_cli_support::RunkernelApp;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut pipeline = Pipeline::new("ops");
    pipeline.add(Task::new("deploy").exec("echo deploying"));

    RunkernelApp::new(pipeline).run_from_args().await
}
```

This enables CLI commands like:
- `runkernel list`
- `runkernel graph`
- `runkernel explain deploy`
- `runkernel run deploy -- --target prod`
