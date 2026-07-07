use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub type OutputStore = Arc<RwLock<HashMap<String, HashMap<String, Value>>>>;
pub type CompletedStore = Arc<RwLock<HashSet<String>>>;

#[derive(Clone, Debug)]
pub struct Context {
    pub task_name: String,
    pub pipeline_name: String,
    pub workspace_root: PathBuf,
    outputs: OutputStore,
    completed: CompletedStore,
}

impl Context {
    /// Creates a new Context for a task.
    pub fn new(task_name: impl Into<String>) -> Self {
        Context {
            task_name: task_name.into(),
            pipeline_name: String::new(),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            outputs: Arc::new(RwLock::new(HashMap::new())),
            completed: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub(crate) fn with_shared(
        task_name: impl Into<String>,
        pipeline_name: impl Into<String>,
        workspace_root: PathBuf,
        outputs: OutputStore,
        completed: CompletedStore,
    ) -> Self {
        Context {
            task_name: task_name.into(),
            pipeline_name: pipeline_name.into(),
            workspace_root,
            outputs,
            completed,
        }
    }

    /// Gets the task name.
    pub fn name(&self) -> &str {
        &self.task_name
    }

    /// Gets the pipeline name.
    pub fn pipeline_name(&self) -> &str {
        &self.pipeline_name
    }

    /// Gets the workspace root used for this pipeline run.
    pub fn workspace_root(&self) -> &std::path::Path {
        &self.workspace_root
    }

    /// Fetches an environment variable by name. Returns an error if not found.
    pub fn env(&self, key: &str) -> anyhow::Result<String> {
        std::env::var(key).map_err(|_| anyhow::anyhow!("Environment variable '{}' is not set", key))
    }

    /// Deserializes the process environment variables into a type-safe structure.
    pub fn require_env<T>(&self) -> anyhow::Result<T>
    where
        T: DeserializeOwned,
    {
        envy::from_env::<T>()
            .map_err(|e| anyhow::anyhow!("Failed to parse environment configuration: {}", e))
    }

    /// Stores a typed output value for the current task.
    pub fn set_output<T>(&self, key: &str, value: T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let value = serde_json::to_value(value)?;
        let mut outputs = self
            .outputs
            .write()
            .map_err(|_| anyhow::anyhow!("Output store lock is poisoned"))?;

        outputs
            .entry(self.task_name.clone())
            .or_default()
            .insert(key.to_string(), value);
        Ok(())
    }

    /// Reads a typed output value from a completed task.
    pub fn output_from<T>(&self, task: &str, key: &str) -> anyhow::Result<T>
    where
        T: DeserializeOwned,
    {
        let completed = self
            .completed
            .read()
            .map_err(|_| anyhow::anyhow!("Completion store lock is poisoned"))?;
        if !completed.contains(task) {
            anyhow::bail!(
                "Output from task '{}' is not available because the task has not completed",
                task
            );
        }
        drop(completed);

        let outputs = self
            .outputs
            .read()
            .map_err(|_| anyhow::anyhow!("Output store lock is poisoned"))?;
        let task_outputs = outputs
            .get(task)
            .ok_or_else(|| anyhow::anyhow!("Task '{}' has no outputs", task))?;
        let value = task_outputs
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("Task '{}' has no output named '{}'", task, key))?;
        serde_json::from_value(value.clone())
            .map_err(|e| anyhow::anyhow!("Failed to deserialize output '{}.{}': {}", task, key, e))
    }
}
