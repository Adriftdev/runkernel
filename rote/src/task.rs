use std::future::Future;
use std::pin::Pin;
use crate::context::Context;

pub type TaskFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
pub type FailureFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

pub type TaskFn = Box<dyn Fn(Context) -> TaskFuture + Send + Sync>;
pub type FailureFn = Box<dyn Fn(Context) -> FailureFuture + Send + Sync>;

pub enum TaskAction {
    Shell(String),
    Fn(TaskFn),
}

pub struct Task {
    pub name: String,
    pub dependencies: Vec<String>,
    pub action: Option<TaskAction>,
    pub failure_handler: Option<FailureFn>,
    pub inputs: Vec<String>,
    pub env_vars: Vec<String>,
}

impl Task {
    /// Creates a new Task with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Task {
            name: name.into(),
            dependencies: Vec::new(),
            action: None,
            failure_handler: None,
            inputs: Vec::new(),
            env_vars: Vec::new(),
        }
    }

    /// Adds task dependencies.
    pub fn depends_on(mut self, deps: &[&str]) -> Self {
        self.dependencies.extend(deps.iter().map(|&s| s.to_string()));
        self
    }

    /// Sets the task action to execute a shell command.
    pub fn exec(mut self, cmd: impl Into<String>) -> Self {
        self.action = Some(TaskAction::Shell(cmd.into()));
        self
    }

    /// Sets the task action to run a native async Rust closure.
    pub fn exec_fn<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.action = Some(TaskAction::Fn(Box::new(move |ctx| Box::pin(f(ctx)))));
        self
    }

    /// Sets a guaranteed rollback handler to run if this task fails.
    pub fn on_failure<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.failure_handler = Some(Box::new(move |ctx| Box::pin(f(ctx))));
        self
    }

    /// Declares file inputs (supports glob patterns) for deterministic caching.
    pub fn inputs(mut self, inputs: &[&str]) -> Self {
        self.inputs.extend(inputs.iter().map(|&s| s.to_string()));
        self
    }

    /// Declares environment variables that this task depends on for deterministic caching.
    pub fn env_vars(mut self, env_vars: &[&str]) -> Self {
        self.env_vars.extend(env_vars.iter().map(|&s| s.to_string()));
        self
    }
}
