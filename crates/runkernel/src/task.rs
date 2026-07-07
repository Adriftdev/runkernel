use crate::context::Context;
use std::future::Future;
use std::pin::Pin;

pub type TaskFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
pub type RollbackFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

pub type TaskFn = Box<dyn Fn(Context) -> TaskFuture + Send + Sync>;
pub type RollbackFn = Box<dyn Fn(Context) -> RollbackFuture + Send + Sync>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Shell {
    #[default]
    Sh,
    Bash,
    Zsh,
    PowerShell,
    Cmd,
    Custom {
        program: String,
        args: Vec<String>,
    },
}

impl Shell {
    pub(crate) fn command_parts(&self, command: &str) -> (String, Vec<String>) {
        match self {
            Shell::Sh => (
                "sh".to_string(),
                vec!["-c".to_string(), command.to_string()],
            ),
            Shell::Bash => (
                "bash".to_string(),
                vec!["-c".to_string(), command.to_string()],
            ),
            Shell::Zsh => (
                "zsh".to_string(),
                vec!["-c".to_string(), command.to_string()],
            ),
            Shell::PowerShell => (
                "pwsh".to_string(),
                vec!["-Command".to_string(), command.to_string()],
            ),
            Shell::Cmd => (
                "cmd".to_string(),
                vec!["/C".to_string(), command.to_string()],
            ),
            Shell::Custom { program, args } => {
                let mut all_args = args.clone();
                all_args.push(command.to_string());
                (program.clone(), all_args)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheMode {
    Disabled,
    Inputs,
    Explicit { key: String },
}

pub enum TaskAction {
    Shell {
        command: String,
        shell: Option<Shell>,
    },
    Fn(TaskFn),
}

pub struct Task {
    pub name: String,
    pub description: Option<String>,
    pub dependencies: Vec<String>,
    pub action: Option<TaskAction>,
    pub rollback_handler: Option<RollbackFn>,
    pub inputs: Vec<String>,
    pub env_vars: Vec<String>,
    pub cache_mode: CacheMode,
}

impl Task {
    /// Creates a new Task with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Task {
            name: name.into(),
            description: None,
            dependencies: Vec::new(),
            action: None,
            rollback_handler: None,
            inputs: Vec::new(),
            env_vars: Vec::new(),
            cache_mode: CacheMode::Inputs,
        }
    }

    /// Adds a human-readable task description for CLI and protocol inspection.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Adds task dependencies.
    pub fn depends_on(mut self, deps: &[&str]) -> Self {
        self.dependencies
            .extend(deps.iter().map(|&s| s.to_string()));
        self.dependencies.sort();
        self.dependencies.dedup();
        self
    }

    /// Sets the task action to execute a shell command.
    pub fn exec(mut self, cmd: impl Into<String>) -> Self {
        self.action = Some(TaskAction::Shell {
            command: cmd.into(),
            shell: None,
        });
        self
    }

    /// Sets the task action to execute a shell command with a task-specific shell.
    pub fn exec_with(mut self, shell: Shell, cmd: impl Into<String>) -> Self {
        self.action = Some(TaskAction::Shell {
            command: cmd.into(),
            shell: Some(shell),
        });
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
        self.rollback_handler = Some(Box::new(move |ctx| {
            let fut = f(ctx);
            Box::pin(async move {
                fut.await;
                Ok(())
            })
        }));
        self
    }

    /// Sets a rollback handler for this task.
    pub fn rollback<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.rollback_handler = Some(Box::new(move |ctx| Box::pin(f(ctx))));
        self
    }

    /// Declares file inputs (supports glob patterns) for deterministic caching.
    pub fn inputs(mut self, inputs: &[&str]) -> Self {
        self.inputs.extend(inputs.iter().map(|&s| s.to_string()));
        self
    }

    /// Declares environment variables that this task depends on for deterministic caching.
    pub fn env_vars(mut self, env_vars: &[&str]) -> Self {
        self.env_vars
            .extend(env_vars.iter().map(|&s| s.to_string()));
        self.env_vars.sort();
        self.env_vars.dedup();
        self
    }

    /// Uses an explicit cache key for this task. This is especially important for native Rust tasks.
    pub fn cache_key(mut self, key: impl Into<String>) -> Self {
        self.cache_mode = CacheMode::Explicit { key: key.into() };
        self
    }

    /// Disables cache checks and cache writes for this task.
    pub fn cache_disabled(mut self) -> Self {
        self.cache_mode = CacheMode::Disabled;
        self
    }

    /// Returns whether this task is configured for caching.
    pub fn cacheable(&self) -> bool {
        self.is_cacheable()
    }

    /// Returns whether this task has a rollback handler.
    pub fn has_rollback(&self) -> bool {
        self.rollback_handler.is_some()
    }
}
