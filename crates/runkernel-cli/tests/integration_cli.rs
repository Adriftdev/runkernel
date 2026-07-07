use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

fn runkernel_bin() -> &'static str {
    env!("CARGO_BIN_EXE_runkernel")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .to_path_buf()
}

fn temp_project(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "runkernel-it-{name}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn run_in(root: &Path, args: &[&str]) -> Output {
    let mut command = Command::new(runkernel_bin());
    command
        .current_dir(root)
        .args(args)
        .env("CARGO_TERM_COLOR", "never")
        .env(
            "CARGO_TARGET_DIR",
            workspace_root().join("target").join("integration-fixtures"),
        );
    command.output().unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn write_manifest(root: &Path, package: &str) {
    fs::write(
        root.join("runkernel.toml"),
        format!(
            r#"[workflow.default]
package = "{package}"
bin = "{package}"
manifest_path = "Cargo.toml"
working_dir = "."
default_task = "build"
description = "Fixture workflow"
"#
        ),
    )
    .unwrap();
}

fn write_cargo_toml(root: &Path, package: &str) {
    let workspace = workspace_root();
    fs::write(
        root.join("Cargo.toml"),
        format!(
            r#"[package]
name = "{package}"
version = "0.1.0"
edition = "2021"

[dependencies]
anyhow = "1.0"
tokio = {{ version = "1", features = ["macros", "rt-multi-thread"] }}
runkernel = {{ path = "{}" }}
runkernel-cli-support = {{ path = "{}" }}
"#,
            workspace.join("crates/runkernel").display(),
            workspace.join("crates/runkernel-cli-support").display()
        ),
    )
    .unwrap();
    fs::copy(workspace.join("Cargo.lock"), root.join("Cargo.lock")).unwrap();
}

fn write_normal_workflow(root: &Path, package: &str) {
    write_manifest(root, package);
    write_cargo_toml(root, package);
    fs::create_dir_all(root.join("src")).unwrap();
    fs::write(
        root.join("src/main.rs"),
        r#"use runkernel::{Pipeline, Task};
use runkernel_cli_support::RunkernelApp;

fn pipeline() -> Pipeline {
    let mut pipeline = Pipeline::new("fixture");
    pipeline.add(Task::new("prepare").description("Prepare").exec_fn(|_| async move { Ok(()) }));
    pipeline.add(
        Task::new("build")
            .description("Build")
            .depends_on(&["prepare"])
            .exec_fn(|ctx| async move {
                println!("BUILD_ARGS={}", ctx.args().join(","));
                Ok(())
            }),
    );
    pipeline.add(Task::new("solo").description("Isolated").exec_fn(|_| async move { Ok(()) }));
    pipeline.add(Task::new("fail").description("Fail").exec_fn(|_| async move {
        anyhow::bail!("fixture failure")
    }));
    pipeline
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    RunkernelApp::new(pipeline()).run_from_args().await
}
"#,
    )
    .unwrap();
}

fn write_non_protocol_workflow(root: &Path, package: &str) {
    write_manifest(root, package);
    write_cargo_toml(root, package);
    fs::create_dir_all(root.join("src")).unwrap();
    fs::write(
        root.join("src/main.rs"),
        r#"fn main() {
    println!("not protocol json");
}
"#,
    )
    .unwrap();
}

fn write_unsupported_protocol_workflow(root: &Path, package: &str) {
    write_manifest(root, package);
    write_cargo_toml(root, package);
    fs::create_dir_all(root.join("src")).unwrap();
    fs::write(
        root.join("src/main.rs"),
        r##"fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["__runkernel", "metadata", "--format", "json"] {
        println!(r#"{{"protocol_version":99,"workflow_name":"bad","description":null,"runkernel_version":"0.1.0","supports":{{"list":true,"graph":true,"explain":true,"run_task":true,"run_all":true}}}}"#);
    } else {
        println!("[]");
    }
}
"##,
    )
    .unwrap();
}

fn write_build_failure_workflow(root: &Path, package: &str) {
    write_manifest(root, package);
    write_cargo_toml(root, package);
    fs::create_dir_all(root.join("src")).unwrap();
    fs::write(
        root.join("src/main.rs"),
        "fn main() { this does not compile }\n",
    )
    .unwrap();
}

#[test]
fn test_cli_success_paths_against_fixture_workflow() {
    let root = temp_project("success");
    write_normal_workflow(&root, "fixture_success");

    let workflows = run_in(&root, &["workflows"]);
    assert!(workflows.status.success(), "{}", stderr(&workflows));
    assert!(stdout(&workflows).contains("Fixture workflow"));

    let list = run_in(&root, &["list"]);
    assert!(list.status.success(), "{}", stderr(&list));
    assert!(stdout(&list).contains("build  Build"));

    let graph = run_in(&root, &["graph"]);
    assert!(graph.status.success(), "{}", stderr(&graph));
    let graph_stdout = stdout(&graph);
    assert!(graph_stdout.contains("Tasks:\n  build\n  fail\n  prepare\n  solo"));
    assert!(graph_stdout.contains("prepare -> build"));

    let graph_json = run_in(&root, &["graph", "--format", "json"]);
    assert!(graph_json.status.success(), "{}", stderr(&graph_json));
    assert!(stdout(&graph_json).contains("\"id\": \"solo\""));

    let graph_dot = run_in(&root, &["graph", "--format", "dot"]);
    assert!(graph_dot.status.success(), "{}", stderr(&graph_dot));
    assert!(stdout(&graph_dot).contains("\"solo\";"));

    let graph_mermaid = run_in(&root, &["graph", "--format", "mermaid"]);
    assert!(graph_mermaid.status.success(), "{}", stderr(&graph_mermaid));
    assert!(stdout(&graph_mermaid).contains("solo[\"solo\"]"));

    let explain = run_in(&root, &["explain", "build"]);
    assert!(explain.status.success(), "{}", stderr(&explain));
    assert!(stdout(&explain).contains("Task: build"));

    let run = run_in(&root, &["run", "build"]);
    assert!(run.status.success(), "{}", stderr(&run));
    assert!(stdout(&run).contains("BUILD_ARGS="));

    let run_with_args = run_in(&root, &["run", "build", "--", "--target", "test"]);
    assert!(run_with_args.status.success(), "{}", stderr(&run_with_args));
    assert!(stdout(&run_with_args).contains("BUILD_ARGS=--target,test"));

    let cache_status = run_in(&root, &["cache", "status"]);
    assert!(cache_status.status.success(), "{}", stderr(&cache_status));
    assert!(stdout(&cache_status).contains("Cache root:"));
}

#[test]
fn test_cli_manifest_and_selection_failures() {
    let missing = temp_project("missing-manifest");
    let missing_output = run_in(&missing, &["list"]);
    assert!(!missing_output.status.success());
    assert!(stderr(&missing_output).contains("No runkernel.toml found"));

    let invalid = temp_project("invalid-manifest");
    fs::write(invalid.join("runkernel.toml"), "not = [valid").unwrap();
    let invalid_output = run_in(&invalid, &["list"]);
    assert!(!invalid_output.status.success());
    assert!(stderr(&invalid_output).contains("Failed to parse"));

    let unknown = temp_project("unknown-workflow");
    write_normal_workflow(&unknown, "fixture_unknown_workflow");
    let unknown_output = run_in(&unknown, &["--workflow", "missing", "list"]);
    assert!(!unknown_output.status.success());
    assert!(stderr(&unknown_output).contains("Unknown workflow 'missing'"));
}

#[test]
fn test_cli_protocol_and_execution_failures() {
    let unknown_task = temp_project("unknown-task");
    write_normal_workflow(&unknown_task, "fixture_unknown_task");
    let unknown_task_output = run_in(&unknown_task, &["run", "missing"]);
    assert!(!unknown_task_output.status.success());
    assert!(stderr(&unknown_task_output).contains("Unknown task 'missing'"));

    let task_failure = temp_project("task-failure");
    write_normal_workflow(&task_failure, "fixture_task_failure");
    let task_failure_output = run_in(&task_failure, &["run", "fail"]);
    assert!(!task_failure_output.status.success());
    assert!(stderr(&task_failure_output).contains("pipeline failed"));

    let non_protocol = temp_project("non-protocol");
    write_non_protocol_workflow(&non_protocol, "fixture_non_protocol");
    let non_protocol_output = run_in(&non_protocol, &["list"]);
    assert!(!non_protocol_output.status.success());
    assert!(stderr(&non_protocol_output).contains("does not appear to support"));

    let unsupported = temp_project("unsupported-protocol");
    write_unsupported_protocol_workflow(&unsupported, "fixture_unsupported_protocol");
    let unsupported_output = run_in(&unsupported, &["list"]);
    assert!(!unsupported_output.status.success());
    assert!(stderr(&unsupported_output).contains("Expected 1, got 99"));

    let build_failure = temp_project("build-failure");
    write_build_failure_workflow(&build_failure, "fixture_build_failure");
    let build_failure_output = run_in(&build_failure, &["list"]);
    assert!(!build_failure_output.status.success());
    assert!(stderr(&build_failure_output).contains("Failed to run workflow"));
}
