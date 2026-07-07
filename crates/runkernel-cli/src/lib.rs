use clap::{Parser, Subcommand, ValueEnum};
use runkernel::{CacheManager, GraphEdge, PipelineGraph};
use runkernel_cli_support::{graph_to_text, ExplainResponse, ListResponse};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, ExitStatus};

const CONFIG_FILE: &str = "runkernel.toml";

#[derive(Debug, Parser)]
#[command(name = "runkernel")]
#[command(about = "Code-native Rust workflow runner")]
pub struct Cli {
    #[arg(short, long, global = true)]
    pub config: Option<PathBuf>,
    #[arg(short, long, global = true)]
    pub workflow: Option<String>,
    #[arg(long, global = true)]
    pub verbose: bool,
    #[arg(long, global = true)]
    pub release: bool,
    #[arg(long, global = true)]
    pub features: Option<String>,
    #[arg(long, global = true)]
    pub all_features: bool,
    #[arg(long, global = true)]
    pub no_default_features: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Run {
        task: Option<String>,
        #[arg(last = true)]
        args: Vec<String>,
    },
    List,
    Graph {
        #[arg(long, value_enum, default_value_t = GraphFormat::Text)]
        format: GraphFormat,
    },
    Explain {
        task: String,
    },
    Init,
    Workflows,
    Cache {
        #[command(subcommand)]
        command: CacheCommand,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum GraphFormat {
    Text,
    Json,
    Dot,
    Mermaid,
}

#[derive(Debug, Subcommand)]
pub enum CacheCommand {
    Clean,
    Status,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RunkernelManifest {
    pub workflow: BTreeMap<String, WorkflowConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WorkflowConfig {
    pub package: Option<String>,
    pub bin: Option<String>,
    pub manifest_path: Option<PathBuf>,
    pub working_dir: Option<PathBuf>,
    pub default_task: Option<String>,
    pub description: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ResolvedWorkflow {
    pub name: String,
    pub package: Option<String>,
    pub bin: Option<String>,
    pub manifest_path: PathBuf,
    pub working_dir: PathBuf,
    pub default_task: Option<String>,
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CliOutcome {
    pub output: String,
    pub exit_code: i32,
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}

pub fn run_cli(cli: Cli) -> anyhow::Result<CliOutcome> {
    match &cli.command {
        Command::Init => init_config(),
        Command::Workflows => {
            let (manifest, config_path) = load_manifest(cli.config.as_deref())?;
            let config_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
            validate_manifest(&manifest, config_dir)?;
            Ok(CliOutcome::success(format_workflows(&manifest)))
        }
        Command::Cache { command } => run_cache_command(command),
        Command::Run { task, args } => {
            let workflow = selected_workflow(&cli)?;
            let task = task.clone().or(workflow.default_task.clone());
            let mut protocol_args = vec!["__runkernel".to_string(), "run".to_string()];
            if let Some(task) = task {
                protocol_args.push(task);
            }
            if !args.is_empty() {
                protocol_args.push("--".to_string());
                protocol_args.extend(args.clone());
            }
            let status = run_protocol_status(&workflow, &protocol_args, &cli)?;
            Ok(CliOutcome {
                output: String::new(),
                exit_code: exit_code(status),
            })
        }
        Command::List => {
            let workflow = selected_workflow(&cli)?;
            let response: ListResponse = run_protocol_json(
                &workflow,
                &["__runkernel", "list", "--format", "json"],
                &cli,
            )?;
            Ok(CliOutcome::success(format_list(&workflow.name, &response)))
        }
        Command::Graph { format } => {
            let workflow = selected_workflow(&cli)?;
            let graph: PipelineGraph = run_protocol_json(
                &workflow,
                &["__runkernel", "graph", "--format", "json"],
                &cli,
            )?;
            Ok(CliOutcome::success(format_graph(
                &workflow.name,
                &graph,
                *format,
            )?))
        }
        Command::Explain { task } => {
            let workflow = selected_workflow(&cli)?;
            let response: ExplainResponse = run_protocol_json(
                &workflow,
                &["__runkernel", "explain", task, "--format", "json"],
                &cli,
            )?;
            Ok(CliOutcome::success(format_explain(&response)))
        }
    }
}

impl CliOutcome {
    fn success(output: impl Into<String>) -> Self {
        Self {
            output: output.into(),
            exit_code: 0,
        }
    }
}

fn init_config() -> anyhow::Result<CliOutcome> {
    let path = PathBuf::from(CONFIG_FILE);
    if path.exists() {
        anyhow::bail!("{} already exists; refusing to overwrite it.", CONFIG_FILE);
    }
    let content = r#"[workflow.default]
package = "ops"
bin = "ops"
manifest_path = "examples/ops/Cargo.toml"
working_dir = "."
default_task = "deploy-edge"
description = "Example ops workflow"
"#;
    std::fs::write(&path, content)?;
    Ok(CliOutcome::success(format!("Created {}", path.display())))
}

fn run_cache_command(command: &CacheCommand) -> anyhow::Result<CliOutcome> {
    let manager = CacheManager::new();
    match command {
        CacheCommand::Clean => {
            let result = manager.clean_all()?;
            Ok(CliOutcome::success(format!(
                "{} cache path: {}",
                if result.removed {
                    "Removed"
                } else {
                    "No cache found at"
                },
                result.path.display()
            )))
        }
        CacheCommand::Status => Ok(CliOutcome::success(format!(
            "Cache root: {}\nExists: {}",
            manager.cache_root().display(),
            manager.cache_root().exists()
        ))),
    }
}

fn selected_workflow(cli: &Cli) -> anyhow::Result<ResolvedWorkflow> {
    let (manifest, config_path) = load_manifest(cli.config.as_deref())?;
    let config_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
    validate_manifest(&manifest, config_dir)?;
    let selected = select_workflow(&manifest, cli.workflow.as_deref())?;
    resolve_workflow(config_dir, selected.0, selected.1)
}

fn load_manifest(config: Option<&Path>) -> anyhow::Result<(RunkernelManifest, PathBuf)> {
    let path = match config {
        Some(path) => path.to_path_buf(),
        None => discover_manifest(std::env::current_dir()?)?,
    };
    let content = std::fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("Failed to read {}: {}", path.display(), e))?;
    let manifest = toml::from_str::<RunkernelManifest>(&content)
        .map_err(|e| anyhow::anyhow!("Failed to parse {}: {}", path.display(), e))?;
    Ok((manifest, path))
}

fn discover_manifest(start: PathBuf) -> anyhow::Result<PathBuf> {
    for dir in start.ancestors() {
        let candidate = dir.join(CONFIG_FILE);
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    anyhow::bail!(
        "No runkernel.toml found.\n\nSearched from:\n  {}\n\nCreate one with:\n  runkernel init",
        start.display()
    )
}

fn validate_manifest(manifest: &RunkernelManifest, config_dir: &Path) -> anyhow::Result<()> {
    if manifest.workflow.is_empty() {
        anyhow::bail!("runkernel.toml must define at least one [workflow.<name>] entry");
    }
    for (name, workflow) in &manifest.workflow {
        if name.trim().is_empty() {
            anyhow::bail!("Workflow names must be non-empty");
        }
        if matches!(&workflow.package, Some(value) if value.trim().is_empty()) {
            anyhow::bail!("Workflow '{}' has an empty package field", name);
        }
        if matches!(&workflow.bin, Some(value) if value.trim().is_empty()) {
            anyhow::bail!("Workflow '{}' has an empty bin field", name);
        }
        if let Some(path) = &workflow.manifest_path {
            let path = config_dir.join(path);
            if !path.exists() {
                anyhow::bail!(
                    "Workflow '{}' references manifest_path '{}', but that file does not exist.",
                    name,
                    workflow.manifest_path.as_ref().unwrap().display()
                );
            }
        }
        if let Some(path) = &workflow.working_dir {
            let path = config_dir.join(path);
            if !path.exists() {
                anyhow::bail!(
                    "Workflow '{}' references working_dir '{}', but that directory does not exist.",
                    name,
                    workflow.working_dir.as_ref().unwrap().display()
                );
            }
        }
    }
    Ok(())
}

fn select_workflow<'a>(
    manifest: &'a RunkernelManifest,
    requested: Option<&str>,
) -> anyhow::Result<(&'a str, &'a WorkflowConfig)> {
    if let Some(name) = requested {
        return manifest
            .workflow
            .get_key_value(name)
            .map(|(name, workflow)| (name.as_str(), workflow))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Unknown workflow '{}'.\n\nAvailable workflows:\n  {}",
                    name,
                    available_workflows(manifest)
                )
            });
    }
    if manifest.workflow.len() == 1 {
        let (name, workflow) = manifest.workflow.iter().next().unwrap();
        return Ok((name, workflow));
    }
    if let Some(workflow) = manifest.workflow.get("default") {
        return Ok(("default", workflow));
    }
    anyhow::bail!(
        "No workflow selected.\n\nThis project defines multiple workflows:\n  {}\n\nUse:\n  runkernel run --workflow <name> <task>",
        available_workflows(manifest)
    )
}

fn available_workflows(manifest: &RunkernelManifest) -> String {
    manifest
        .workflow
        .keys()
        .cloned()
        .collect::<Vec<_>>()
        .join("\n  ")
}

fn resolve_workflow(
    config_dir: &Path,
    name: &str,
    workflow: &WorkflowConfig,
) -> anyhow::Result<ResolvedWorkflow> {
    let manifest_path = workflow
        .manifest_path
        .clone()
        .unwrap_or_else(|| PathBuf::from("Cargo.toml"));
    let manifest_path = config_dir.join(manifest_path);
    let working_dir = workflow
        .working_dir
        .clone()
        .map(|path| config_dir.join(path))
        .unwrap_or_else(|| config_dir.to_path_buf());
    Ok(ResolvedWorkflow {
        name: name.to_string(),
        package: workflow.package.clone(),
        bin: workflow.bin.clone(),
        manifest_path,
        working_dir,
        default_task: workflow.default_task.clone(),
        description: workflow.description.clone(),
    })
}

fn build_cargo_command(
    workflow: &ResolvedWorkflow,
    protocol_args: &[String],
    cli: &Cli,
) -> ProcessCommand {
    let mut command = ProcessCommand::new("cargo");
    command.current_dir(&workflow.working_dir);
    command.arg("run");
    if !cli.verbose {
        command.arg("--quiet");
    }
    if cli.release {
        command.arg("--release");
    }
    if let Some(features) = &cli.features {
        command.arg("--features").arg(features);
    }
    if cli.all_features {
        command.arg("--all-features");
    }
    if cli.no_default_features {
        command.arg("--no-default-features");
    }
    command.arg("--manifest-path").arg(&workflow.manifest_path);
    if let Some(package) = &workflow.package {
        command.arg("--package").arg(package);
    }
    if let Some(bin) = &workflow.bin {
        command.arg("--bin").arg(bin);
    }
    command.arg("--");
    command.args(protocol_args);
    command
}

#[cfg(test)]
fn command_args(command: &ProcessCommand) -> Vec<String> {
    command
        .get_args()
        .map(|arg| arg.to_string_lossy().to_string())
        .collect()
}

fn run_protocol_status(
    workflow: &ResolvedWorkflow,
    protocol_args: &[String],
    cli: &Cli,
) -> anyhow::Result<ExitStatus> {
    let mut command = build_cargo_command(workflow, protocol_args, cli);
    command.status().map_err(|e| {
        anyhow::anyhow!(
            "Failed to run workflow '{}' through Cargo: {}",
            workflow.name,
            e
        )
    })
}

fn run_protocol_json<T>(
    workflow: &ResolvedWorkflow,
    protocol_args: &[&str],
    cli: &Cli,
) -> anyhow::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let protocol_args: Vec<_> = protocol_args.iter().map(|arg| arg.to_string()).collect();
    let mut command = build_cargo_command(workflow, &protocol_args, cli);
    let output = command.output().map_err(|e| {
        anyhow::anyhow!(
            "Failed to run workflow '{}' through Cargo: {}",
            workflow.name,
            e
        )
    })?;
    if !output.status.success() {
        anyhow::bail!(
            "Failed to run workflow '{}' through Cargo.\n{}",
            workflow.name,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    serde_json::from_slice(&output.stdout).map_err(|e| {
        anyhow::anyhow!(
            "Workflow '{}' returned invalid protocol JSON: {}\n{}",
            workflow.name,
            e,
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn exit_code(status: ExitStatus) -> i32 {
    status.code().unwrap_or(1)
}

fn format_workflows(manifest: &RunkernelManifest) -> String {
    let mut output = String::from("Workflows:");
    for (name, workflow) in &manifest.workflow {
        output.push_str("\n  ");
        output.push_str(name);
        if let Some(description) = &workflow.description {
            output.push_str("  ");
            output.push_str(description);
        }
    }
    output
}

fn format_list(workflow: &str, response: &ListResponse) -> String {
    let mut output = format!("Workflow: {workflow}\n\nTasks:");
    for task in &response.tasks {
        output.push_str("\n  ");
        output.push_str(&task.name);
        if let Some(description) = &task.description {
            output.push_str("  ");
            output.push_str(description);
        }
    }
    output
}

fn format_graph(
    workflow: &str,
    graph: &PipelineGraph,
    format: GraphFormat,
) -> anyhow::Result<String> {
    match format {
        GraphFormat::Text => Ok(graph_to_text(graph)),
        GraphFormat::Json => Ok(serde_json::to_string_pretty(graph)?),
        GraphFormat::Dot => graph_to_dot(workflow, graph),
        GraphFormat::Mermaid => Ok(graph_to_mermaid(graph)),
    }
}

fn graph_to_dot(workflow: &str, graph: &PipelineGraph) -> anyhow::Result<String> {
    let mut output = format!("digraph \"{}\" {{\n  rankdir=LR;\n", workflow);
    for node in &graph.nodes {
        output.push_str("  \"");
        output.push_str(&node.id);
        output.push_str("\";\n");
    }
    for GraphEdge { from, to } in &graph.edges {
        output.push_str("  \"");
        output.push_str(from);
        output.push_str("\" -> \"");
        output.push_str(to);
        output.push_str("\";\n");
    }
    output.push_str("}\n");
    Ok(output)
}

fn graph_to_mermaid(graph: &PipelineGraph) -> String {
    let mut output = String::from("graph TD");
    for edge in &graph.edges {
        output.push_str("\n  ");
        output.push_str(&edge.from);
        output.push_str(" --> ");
        output.push_str(&edge.to);
    }
    output
}

fn format_explain(response: &ExplainResponse) -> String {
    let task = &response.task;
    format!(
        "Task: {}\n\nDescription:\n  {}\n\nDependencies:\n  {}\n\nDependents:\n  {}\n\nCache:\n  {}\n\nRollback:\n  {}\n\nInputs:\n  {}\n\nEnvironment:\n  {}",
        task.name,
        task.description.as_deref().unwrap_or("none"),
        join_or_none(&task.dependencies),
        join_or_none(&task.dependents),
        if task.cacheable { "enabled" } else { "disabled" },
        if task.has_rollback { "enabled" } else { "disabled" },
        join_or_none(&task.inputs),
        join_or_none(&task.env_vars),
    )
}

fn join_or_none(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_string()
    } else {
        values.join("\n  ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use runkernel::{GraphNode, TaskExplanation};
    use runkernel_cli_support::{ExplainResponse, TaskListItem};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "runkernel-cli-{name}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_manifest(root: &Path, content: &str) -> PathBuf {
        let path = root.join(CONFIG_FILE);
        std::fs::write(&path, content).unwrap();
        path
    }

    fn manifest_content() -> &'static str {
        r#"[workflow.default]
package = "ops"
bin = "ops"
manifest_path = "Cargo.toml"
working_dir = "."
default_task = "deploy"
description = "Ops workflow"
"#
    }

    #[test]
    fn test_parse_run_command_with_forwarded_args() {
        let cli = Cli::try_parse_from([
            "runkernel",
            "run",
            "deploy",
            "--release",
            "--",
            "--target",
            "prod",
        ])
        .unwrap();
        assert!(cli.release);
        let Command::Run { task, args } = cli.command else {
            panic!("expected run command");
        };
        assert_eq!(task.as_deref(), Some("deploy"));
        assert_eq!(args, vec!["--target", "prod"]);
    }

    #[test]
    fn test_discover_manifest_from_nested_directory() {
        let root = temp_dir("discover");
        std::fs::write(
            root.join("Cargo.toml"),
            "[package]\nname='x'\nversion='0.1.0'\nedition='2021'\n",
        )
        .unwrap();
        let manifest = write_manifest(&root, manifest_content());
        let nested = root.join("a").join("b");
        std::fs::create_dir_all(&nested).unwrap();

        assert_eq!(discover_manifest(nested).unwrap(), manifest);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn test_select_default_workflow() {
        let manifest = toml::from_str::<RunkernelManifest>(
            r#"[workflow.ops]
package = "ops"
[workflow.default]
package = "default"
"#,
        )
        .unwrap();
        let (name, workflow) = select_workflow(&manifest, None).unwrap();
        assert_eq!(name, "default");
        assert_eq!(workflow.package.as_deref(), Some("default"));
    }

    #[test]
    fn test_unknown_workflow_lists_available() {
        let manifest = toml::from_str::<RunkernelManifest>(
            r#"[workflow.ops]
package = "ops"
"#,
        )
        .unwrap();
        let err = select_workflow(&manifest, Some("release")).unwrap_err();
        assert!(err.to_string().contains("Unknown workflow 'release'"));
        assert!(err.to_string().contains("ops"));
    }

    #[test]
    fn test_build_cargo_command_ordering() {
        let root = PathBuf::from("/tmp/project");
        let workflow = ResolvedWorkflow {
            name: "ops".to_string(),
            package: Some("ops".to_string()),
            bin: Some("ops".to_string()),
            manifest_path: root.join("examples/ops/Cargo.toml"),
            working_dir: root.clone(),
            default_task: Some("deploy".to_string()),
            description: None,
        };
        let cli = Cli {
            config: None,
            workflow: Some("ops".to_string()),
            verbose: false,
            release: true,
            features: Some("cloud,edge".to_string()),
            all_features: false,
            no_default_features: true,
            command: Command::Run {
                task: Some("deploy".to_string()),
                args: Vec::new(),
            },
        };
        let args = command_args(&build_cargo_command(
            &workflow,
            &[
                "__runkernel".to_string(),
                "run".to_string(),
                "deploy".to_string(),
                "--".to_string(),
                "--target".to_string(),
                "prod".to_string(),
            ],
            &cli,
        ));
        assert_eq!(
            args,
            vec![
                "run",
                "--quiet",
                "--release",
                "--features",
                "cloud,edge",
                "--no-default-features",
                "--manifest-path",
                "/tmp/project/examples/ops/Cargo.toml",
                "--package",
                "ops",
                "--bin",
                "ops",
                "--",
                "__runkernel",
                "run",
                "deploy",
                "--",
                "--target",
                "prod"
            ]
        );
    }

    #[test]
    fn test_format_list() {
        let output = format_list(
            "ops",
            &ListResponse {
                tasks: vec![TaskListItem {
                    name: "deploy".to_string(),
                    description: Some("Deploy".to_string()),
                    dependencies: vec!["build".to_string()],
                    cacheable: false,
                }],
            },
        );
        assert!(output.contains("Workflow: ops"));
        assert!(output.contains("deploy  Deploy"));
    }

    #[test]
    fn test_format_graph_variants() {
        let graph = PipelineGraph {
            nodes: vec![
                GraphNode {
                    id: "lint".to_string(),
                    label: "lint".to_string(),
                },
                GraphNode {
                    id: "test".to_string(),
                    label: "test".to_string(),
                },
            ],
            edges: vec![GraphEdge {
                from: "lint".to_string(),
                to: "test".to_string(),
            }],
        };
        assert_eq!(
            format_graph("ops", &graph, GraphFormat::Text).unwrap(),
            "lint -> test"
        );
        assert!(format_graph("ops", &graph, GraphFormat::Dot)
            .unwrap()
            .contains("\"lint\" -> \"test\""));
        assert!(format_graph("ops", &graph, GraphFormat::Mermaid)
            .unwrap()
            .contains("lint --> test"));
    }

    #[test]
    fn test_format_explain() {
        let output = format_explain(&ExplainResponse {
            task: TaskExplanation {
                name: "deploy".to_string(),
                description: Some("Deploy app".to_string()),
                dependencies: vec!["build".to_string()],
                dependents: Vec::new(),
                cacheable: false,
                action: "shell".to_string(),
                cache_mode: "disabled".to_string(),
                inputs: Vec::new(),
                env_vars: vec!["TARGET".to_string()],
                shell: Some("Sh".to_string()),
                has_rollback: true,
            },
        });
        assert!(output.contains("Task: deploy"));
        assert!(output.contains("Deploy app"));
        assert!(output.contains("TARGET"));
    }

    #[test]
    fn test_init_refuses_overwrite() {
        let root = temp_dir("init");
        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(&root).unwrap();
        write_manifest(&root, manifest_content());
        let err = init_config().unwrap_err();
        std::env::set_current_dir(original).unwrap();
        assert!(err.to_string().contains("already exists"));
        std::fs::remove_dir_all(root).ok();
    }
}
