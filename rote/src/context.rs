use serde::de::DeserializeOwned;

#[derive(Clone, Debug)]
pub struct Context {
    pub task_name: String,
}

impl Context {
    /// Creates a new Context for a task.
    pub fn new(task_name: impl Into<String>) -> Self {
        Context {
            task_name: task_name.into(),
        }
    }

    /// Gets the task name.
    pub fn name(&self) -> &str {
        &self.task_name
    }

    /// Fetches an environment variable by name. Returns an error if not found.
    pub fn env(&self, key: &str) -> anyhow::Result<String> {
        std::env::var(key).map_err(|_| {
            anyhow::anyhow!("Environment variable '{}' is not set", key)
        })
    }

    /// Deserializes the process environment variables into a type-safe structure.
    pub fn require_env<T>(&self) -> anyhow::Result<T>
    where
        T: DeserializeOwned,
    {
        envy::from_env::<T>().map_err(|e| {
            anyhow::anyhow!("Failed to parse environment configuration: {}", e)
        })
    }
}
