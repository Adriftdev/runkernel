use runkernel::{Pipeline, PipelineGraph, TaskExplanation};
use serde::{Deserialize, Serialize};

const PROTOCOL_COMMAND: &str = "__runkernel";
const PROTOCOL_VERSION: u32 = 1;

pub struct RunkernelApp {
    pipeline: Pipeline,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataResponse {
    pub protocol_version: u32,
    pub workflow_name: String,
    pub description: Option<String>,
    pub runkernel_version: String,
    pub supports: ProtocolSupport,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProtocolSupport {
    pub list: bool,
    pub graph: bool,
    pub explain: bool,
    pub run_task: bool,
    pub run_all: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListResponse {
    pub tasks: Vec<TaskListItem>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskListItem {
    pub name: String,
    pub description: Option<String>,
    pub dependencies: Vec<String>,
    pub cacheable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExplainResponse {
    pub task: TaskExplanation,
}

impl RunkernelApp {
    pub fn new(pipeline: Pipeline) -> Self {
        Self { pipeline }
    }

    pub async fn run_from_args(self) -> anyhow::Result<()> {
        self.run_from(std::env::args().skip(1)).await
    }

    pub async fn run_from<I, S>(self, args: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let args: Vec<String> = args.into_iter().map(Into::into).collect();
        if args.first().map(String::as_str) != Some(PROTOCOL_COMMAND) {
            return finish_result(self.pipeline.run().await?);
        }

        match args.get(1).map(String::as_str) {
            Some("metadata") => {
                require_json_format(&args[2..])?;
                emit_json(&metadata(&self.pipeline))
            }
            Some("list") => {
                require_json_format(&args[2..])?;
                emit_json(&list(&self.pipeline))
            }
            Some("graph") => {
                require_json_format(&args[2..])?;
                emit_json(&self.pipeline.graph()?)
            }
            Some("explain") => {
                let task = args
                    .get(2)
                    .ok_or_else(|| anyhow::anyhow!("Missing task for __runkernel explain"))?;
                require_json_format(&args[3..])?;
                emit_json(&ExplainResponse {
                    task: self.pipeline.explain(task)?,
                })
            }
            Some("run") => {
                let task = args.get(2).filter(|value| value.as_str() != "--").cloned();
                let result = match task {
                    Some(task) => self.pipeline.run_task(&task).await?,
                    None => self.pipeline.run().await?,
                };
                finish_result(result)
            }
            Some(other) => anyhow::bail!("Unknown __runkernel command '{}'", other),
            None => anyhow::bail!("Missing __runkernel command"),
        }
    }
}

fn metadata(pipeline: &Pipeline) -> MetadataResponse {
    MetadataResponse {
        protocol_version: PROTOCOL_VERSION,
        workflow_name: pipeline.name().to_string(),
        description: None,
        runkernel_version: env!("CARGO_PKG_VERSION").to_string(),
        supports: ProtocolSupport {
            list: true,
            graph: true,
            explain: true,
            run_task: true,
            run_all: true,
        },
    }
}

fn list(pipeline: &Pipeline) -> ListResponse {
    let mut tasks: Vec<_> = pipeline
        .tasks()
        .map(|task| TaskListItem {
            name: task.name.clone(),
            description: task.description.clone(),
            dependencies: task.dependencies.clone(),
            cacheable: task.cacheable(),
        })
        .collect();
    tasks.sort_by(|a, b| a.name.cmp(&b.name));
    ListResponse { tasks }
}

fn require_json_format(args: &[String]) -> anyhow::Result<()> {
    if args.is_empty() {
        return Ok(());
    }
    if args == ["--format".to_string(), "json".to_string()] {
        return Ok(());
    }
    anyhow::bail!("Only '--format json' is supported for __runkernel protocol commands")
}

fn emit_json<T>(value: &T) -> anyhow::Result<()>
where
    T: Serialize,
{
    serde_json::to_writer(std::io::stdout(), value)?;
    println!();
    Ok(())
}

fn finish_result(result: runkernel::PipelineResult) -> anyhow::Result<()> {
    if result.summary.success {
        Ok(())
    } else {
        anyhow::bail!("pipeline failed: {:?}", result.summary)
    }
}

pub fn graph_to_text(graph: &PipelineGraph) -> String {
    graph
        .edges
        .iter()
        .map(|edge| format!("{} -> {}", edge.from, edge.to))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use runkernel::Task;

    fn pipeline() -> Pipeline {
        let mut pipeline = Pipeline::new("support-test");
        pipeline.add(Task::new("lint").description("Run lint").exec("true"));
        pipeline.add(
            Task::new("test")
                .description("Run tests")
                .depends_on(&["lint"])
                .exec("true"),
        );
        pipeline
    }

    #[test]
    fn test_metadata_shape() {
        let metadata = metadata(&pipeline());
        assert_eq!(metadata.protocol_version, 1);
        assert_eq!(metadata.workflow_name, "support-test");
        assert!(metadata.supports.list);
    }

    #[test]
    fn test_list_shape() {
        let list = list(&pipeline());
        assert_eq!(list.tasks.len(), 2);
        assert_eq!(list.tasks[0].name, "lint");
        assert_eq!(list.tasks[0].description.as_deref(), Some("Run lint"));
    }

    #[test]
    fn test_graph_to_text() {
        let graph = pipeline().graph().unwrap();
        assert_eq!(graph_to_text(&graph), "lint -> test");
    }
}
