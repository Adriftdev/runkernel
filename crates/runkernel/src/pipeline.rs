use crate::cache::{CacheEligibility, CacheLookup, CacheManager};
use crate::context::{CompletedStore, Context, OutputStore};
use crate::task::{CacheMode, RollbackFn, Shell, Task, TaskAction};
use futures::future::{AbortHandle, Abortable, BoxFuture};
use futures::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailurePolicy {
    FailFast,
    #[default]
    FinishRunning,
    ContinueIndependent,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RollbackPolicy {
    Disabled,
    #[default]
    FailedTaskOnly,
    CompletedTasksReverseOrder,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Running,
    Cached,
    Completed,
    Failed,
    Skipped,
    Cancelled,
    RolledBack,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RollbackStatus {
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskResult {
    pub name: String,
    pub status: TaskStatus,
    pub duration: Option<Duration>,
    pub error: Option<String>,
    pub cache_hit: bool,
    pub cache_reason: Option<String>,
    pub rollback_status: Option<RollbackStatus>,
    pub rollback_error: Option<String>,
}

impl TaskResult {
    fn pending(name: impl Into<String>) -> Self {
        TaskResult {
            name: name.into(),
            status: TaskStatus::Pending,
            duration: None,
            error: None,
            cache_hit: false,
            cache_reason: None,
            rollback_status: None,
            rollback_error: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineSummary {
    pub name: String,
    pub success: bool,
    pub completed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub cached: usize,
    pub cancelled: usize,
    pub rolled_back: usize,
    pub rollback_failed: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineResult {
    pub name: String,
    pub duration: Duration,
    pub tasks: Vec<TaskResult>,
    pub summary: PipelineSummary,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphNode {
    pub id: String,
    pub label: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEdge {
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineGraph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskExplanation {
    pub name: String,
    pub description: Option<String>,
    pub dependencies: Vec<String>,
    pub dependents: Vec<String>,
    pub cacheable: bool,
    pub action: String,
    pub cache_mode: String,
    pub inputs: Vec<String>,
    pub env_vars: Vec<String>,
    pub shell: Option<String>,
    pub has_rollback: bool,
}

#[derive(Clone, Debug)]
pub enum PipelineEvent {
    PipelineStarted { name: String, task_count: usize },
    TaskQueued { name: String },
    TaskStarted { name: String },
    TaskCached { name: String, reason: String },
    TaskCompleted { name: String, duration: Duration },
    TaskFailed { name: String, error: String },
    TaskSkipped { name: String, reason: String },
    TaskCancelled { name: String },
    TaskRollbackStarted { name: String },
    TaskRollbackCompleted { name: String },
    TaskRollbackFailed { name: String, error: String },
    PipelineFinished { result: PipelineSummary },
}

pub type EventCallback = Arc<dyn Fn(PipelineEvent) + Send + Sync>;

pub struct Pipeline {
    pub name: String,
    pub tasks: HashMap<String, Task>,
    pub event_callback: Option<EventCallback>,
    pub failure_policy: FailurePolicy,
    pub rollback_policy: RollbackPolicy,
    pub shell: Shell,
}

#[derive(Clone)]
struct RunShared {
    pipeline_name: String,
    workspace_root: PathBuf,
    outputs: OutputStore,
    completed: CompletedStore,
    default_shell: Shell,
}

impl Pipeline {
    /// Creates a new Pipeline.
    pub fn new(name: impl Into<String>) -> Self {
        Pipeline {
            name: name.into(),
            tasks: HashMap::new(),
            event_callback: None,
            failure_policy: FailurePolicy::default(),
            rollback_policy: RollbackPolicy::default(),
            shell: Shell::default(),
        }
    }

    /// Sets the pipeline failure policy.
    pub fn failure_policy(mut self, policy: FailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    /// Sets the pipeline rollback policy.
    pub fn rollback_policy(mut self, policy: RollbackPolicy) -> Self {
        self.rollback_policy = policy;
        self
    }

    /// Sets the default shell for shell tasks.
    pub fn shell(mut self, shell: Shell) -> Self {
        self.shell = shell;
        self
    }

    /// Sets the event callback for the pipeline.
    pub fn with_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(PipelineEvent) + Send + Sync + 'static,
    {
        self.event_callback = Some(Arc::new(callback));
        self
    }

    /// Sets the event callback on a mutable reference.
    pub fn on_event<F>(&mut self, callback: F)
    where
        F: Fn(PipelineEvent) + Send + Sync + 'static,
    {
        self.event_callback = Some(Arc::new(callback));
    }

    /// Adds a Task to the pipeline.
    pub fn add(&mut self, task: Task) {
        self.tasks.insert(task.name.clone(), task);
    }

    /// Returns the pipeline name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns all tasks in this pipeline.
    pub fn tasks(&self) -> impl Iterator<Item = &Task> {
        self.tasks.values()
    }

    /// Returns one task by name.
    pub fn task(&self, name: &str) -> Option<&Task> {
        self.tasks.get(name)
    }

    /// Returns this pipeline's graph as nodes and dependency edges.
    pub fn graph(&self) -> anyhow::Result<PipelineGraph> {
        find_execution_order(&self.tasks)?;

        let mut task_names: Vec<_> = self.tasks.keys().cloned().collect();
        task_names.sort();
        let nodes = task_names
            .iter()
            .map(|name| GraphNode {
                id: name.clone(),
                label: name.clone(),
            })
            .collect();

        let mut edges = Vec::new();
        for task_name in &task_names {
            let mut dependencies = self.tasks[task_name].dependencies.clone();
            dependencies.sort();
            for dependency in dependencies {
                edges.push(GraphEdge {
                    from: dependency,
                    to: task_name.clone(),
                });
            }
        }

        Ok(PipelineGraph { nodes, edges })
    }

    /// Returns Graphviz DOT for the validated pipeline DAG.
    pub fn to_dot(&self) -> anyhow::Result<String> {
        let graph = self.graph()?;

        let mut output = String::new();
        output.push_str("digraph \"");
        output.push_str(&dot_escape(&self.name));
        output.push_str("\" {\n");
        output.push_str("  rankdir=LR;\n");

        for node in &graph.nodes {
            output.push_str("  \"");
            output.push_str(&dot_escape(&node.id));
            output.push_str("\";\n");
        }

        for edge in &graph.edges {
            output.push_str("  \"");
            output.push_str(&dot_escape(&edge.from));
            output.push_str("\" -> \"");
            output.push_str(&dot_escape(&edge.to));
            output.push_str("\";\n");
        }

        output.push_str("}\n");
        Ok(output)
    }

    /// Returns an inspectable explanation for a task in this pipeline.
    pub fn explain(&self, name: &str) -> anyhow::Result<TaskExplanation> {
        self.explain_task(name)
    }

    /// Returns an inspectable explanation for a task in this pipeline.
    pub fn explain_task(&self, name: &str) -> anyhow::Result<TaskExplanation> {
        find_execution_order(&self.tasks)?;
        let task = self
            .tasks
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("Task '{}' does not exist in the pipeline", name))?;

        let mut dependents: Vec<_> = self
            .tasks
            .iter()
            .filter_map(|(candidate, candidate_task)| {
                candidate_task
                    .dependencies
                    .iter()
                    .any(|dependency| dependency == name)
                    .then_some(candidate.clone())
            })
            .collect();
        dependents.sort();

        let (action, shell) = match &task.action {
            Some(TaskAction::Shell { shell, .. }) => (
                "shell".to_string(),
                Some(format!("{:?}", shell.as_ref().unwrap_or(&self.shell))),
            ),
            Some(TaskAction::Fn(_)) => ("rust-fn".to_string(), None),
            None => ("none".to_string(), None),
        };

        let cache_mode = match &task.cache_mode {
            CacheMode::Disabled => "disabled".to_string(),
            CacheMode::Inputs => "inputs".to_string(),
            CacheMode::Explicit { key } => format!("explicit:{key}"),
        };

        Ok(TaskExplanation {
            name: task.name.clone(),
            description: task.description.clone(),
            dependencies: task.dependencies.clone(),
            dependents,
            cacheable: task.cacheable(),
            action,
            cache_mode,
            inputs: task.inputs.clone(),
            env_vars: task.env_vars.clone(),
            shell,
            has_rollback: task.rollback_handler.is_some(),
        })
    }

    /// Runs the pipeline and returns structured execution results.
    pub async fn run(self) -> anyhow::Result<PipelineResult> {
        let event_cb = self.event_callback.clone();
        find_execution_order(&self.tasks)?;

        let start_time = Instant::now();
        let tasks = Arc::new(self.tasks);
        let mut dep_counts = HashMap::new();
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
        let mut results = HashMap::new();

        for (name, task) in tasks.iter() {
            dep_counts.insert(name.clone(), task.dependencies.len());
            results.insert(name.clone(), TaskResult::pending(name));
            for dep in &task.dependencies {
                dependents
                    .entry(dep.clone())
                    .or_default()
                    .push(name.clone());
            }
        }
        for deps in dependents.values_mut() {
            deps.sort();
        }

        emit(
            &event_cb,
            PipelineEvent::PipelineStarted {
                name: self.name.clone(),
                task_count: tasks.len(),
            },
        );

        let shared = RunShared {
            pipeline_name: self.name.clone(),
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            outputs: Arc::new(RwLock::new(HashMap::new())),
            completed: Arc::new(RwLock::new(HashSet::new())),
            default_shell: self.shell.clone(),
        };

        let mut active = FuturesUnordered::new();
        let mut active_aborts: HashMap<String, AbortHandle> = HashMap::new();
        let mut completed_tasks = HashSet::new();
        let mut failed_tasks = HashSet::new();
        let mut executed_completion_order = Vec::new();
        let mut pipeline_failed = false;

        let mut initial_ready: Vec<_> = dep_counts
            .iter()
            .filter_map(|(name, count)| (*count == 0).then_some(name.clone()))
            .collect();
        initial_ready.sort();
        for name in initial_ready {
            schedule_task(
                name,
                &tasks,
                &shared,
                &event_cb,
                &mut active,
                &mut active_aborts,
                &mut results,
            );
        }

        while let Some(outcome) = active.next().await {
            let (name, execution) = match outcome {
                Ok(outcome) => outcome,
                Err(_) => {
                    continue;
                }
            };
            active_aborts.remove(&name);

            match execution.status {
                TaskStatus::Completed | TaskStatus::Cached => {
                    completed_tasks.insert(name.clone());
                    shared
                        .completed
                        .write()
                        .map_err(|_| anyhow::anyhow!("Completion store lock is poisoned"))?
                        .insert(name.clone());
                    if execution.status == TaskStatus::Completed {
                        executed_completion_order.push(name.clone());
                    }
                    results.insert(name.clone(), execution);

                    let can_schedule = !pipeline_failed
                        || self.failure_policy == FailurePolicy::ContinueIndependent;
                    if can_schedule {
                        if let Some(next_tasks) = dependents.get(&name) {
                            for dependent in next_tasks {
                                if let Some(count) = dep_counts.get_mut(dependent) {
                                    *count -= 1;
                                    if *count == 0 {
                                        schedule_task(
                                            dependent.clone(),
                                            &tasks,
                                            &shared,
                                            &event_cb,
                                            &mut active,
                                            &mut active_aborts,
                                            &mut results,
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                TaskStatus::Failed => {
                    pipeline_failed = true;
                    failed_tasks.insert(name.clone());
                    emit(
                        &event_cb,
                        PipelineEvent::TaskFailed {
                            name: name.clone(),
                            error: execution
                                .error
                                .clone()
                                .unwrap_or_else(|| "task failed".to_string()),
                        },
                    );
                    results.insert(name.clone(), execution);

                    if self.rollback_policy == RollbackPolicy::FailedTaskOnly {
                        rollback_task(&name, &tasks, &shared, &event_cb, &mut results, false).await;
                    }

                    if self.failure_policy == FailurePolicy::FailFast {
                        let mut cancelled: Vec<_> = active_aborts.keys().cloned().collect();
                        cancelled.sort();
                        for task_name in cancelled {
                            if let Some(handle) = active_aborts.remove(&task_name) {
                                handle.abort();
                            }
                            mark_cancelled(&task_name, &event_cb, &mut results);
                        }
                        break;
                    }
                }
                _ => {
                    results.insert(name.clone(), execution);
                }
            }
        }

        if pipeline_failed && self.rollback_policy == RollbackPolicy::CompletedTasksReverseOrder {
            for task_name in executed_completion_order.iter().rev() {
                rollback_task(task_name, &tasks, &shared, &event_cb, &mut results, true).await;
            }
        }

        if pipeline_failed {
            let reason = match self.failure_policy {
                FailurePolicy::ContinueIndependent => "dependency failed",
                FailurePolicy::FinishRunning => "pipeline stopped scheduling after failure",
                FailurePolicy::FailFast => "pipeline failed fast",
            };
            mark_pending_skipped(reason, &event_cb, &mut results);
        }

        let duration = start_time.elapsed();
        let mut task_results: Vec<_> = results.into_values().collect();
        task_results.sort_by(|a, b| a.name.cmp(&b.name));
        let summary = summarize(&self.name, &task_results);
        emit(
            &event_cb,
            PipelineEvent::PipelineFinished {
                result: summary.clone(),
            },
        );

        Ok(PipelineResult {
            name: self.name,
            duration,
            tasks: task_results,
            summary,
        })
    }

    /// Runs one task and its dependency closure.
    pub async fn run_task(mut self, task: &str) -> anyhow::Result<PipelineResult> {
        if !self.tasks.contains_key(task) {
            let mut available: Vec<_> = self.tasks.keys().cloned().collect();
            available.sort();
            anyhow::bail!(
                "Unknown task '{}'.\n\nAvailable tasks:\n  {}",
                task,
                available.join("\n  ")
            );
        }

        let mut needed = HashSet::new();
        collect_dependencies(task, &self.tasks, &mut needed)?;
        self.tasks.retain(|name, _| needed.contains(name));
        self.run().await
    }
}

fn schedule_task(
    name: String,
    tasks: &Arc<HashMap<String, Task>>,
    shared: &RunShared,
    event_cb: &Option<EventCallback>,
    active: &mut FuturesUnordered<Abortable<BoxFuture<'static, (String, TaskResult)>>>,
    active_aborts: &mut HashMap<String, AbortHandle>,
    results: &mut HashMap<String, TaskResult>,
) {
    emit(event_cb, PipelineEvent::TaskQueued { name: name.clone() });
    if let Some(result) = results.get_mut(&name) {
        result.status = TaskStatus::Running;
    }

    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    active_aborts.insert(name.clone(), abort_handle);
    let tasks = Arc::clone(tasks);
    let shared = shared.clone();
    let event_cb = event_cb.clone();
    let task_name = name.clone();
    active.push(Abortable::new(
        Box::pin(async move {
            let result = execute_task(task_name.clone(), tasks, shared, event_cb).await;
            (task_name, result)
        }),
        abort_registration,
    ));
}

async fn execute_task(
    name: String,
    tasks: Arc<HashMap<String, Task>>,
    shared: RunShared,
    callback: Option<EventCallback>,
) -> TaskResult {
    let mut result = TaskResult::pending(name.clone());
    let task = match tasks.get(&name) {
        Some(task) => task,
        None => {
            result.status = TaskStatus::Failed;
            result.error = Some(format!("Task '{}' not found in workspace", name));
            return result;
        }
    };

    let cache_manager = CacheManager::for_pipeline(&shared.pipeline_name);
    let cache_hash = match cache_manager.compute_hash(&shared.pipeline_name, task) {
        Ok(CacheEligibility::Enabled { hash, reason }) => {
            match cache_manager.lookup(&name, &hash) {
                CacheLookup::Hit { reason, outputs } => {
                    if let Ok(mut output_store) = shared.outputs.write() {
                        output_store.insert(name.clone(), outputs);
                    }
                    emit(
                        &callback,
                        PipelineEvent::TaskCached {
                            name: name.clone(),
                            reason: reason.clone(),
                        },
                    );
                    result.status = TaskStatus::Cached;
                    result.duration = Some(Duration::ZERO);
                    result.cache_hit = true;
                    result.cache_reason = Some(reason);
                    return result;
                }
                CacheLookup::Miss { reason } => {
                    result.cache_reason = Some(reason);
                }
            }
            Some((hash, reason))
        }
        Ok(CacheEligibility::Disabled(reason)) => {
            result.cache_reason = Some(reason);
            None
        }
        Err(e) => {
            result.status = TaskStatus::Failed;
            result.error = Some(format!("Failed to compute cache hash: {}", e));
            return result;
        }
    };

    emit(&callback, PipelineEvent::TaskStarted { name: name.clone() });
    let start_time = Instant::now();

    let action_result = match &task.action {
        Some(TaskAction::Shell { command, shell }) => {
            let shell = shell.as_ref().unwrap_or(&shared.default_shell);
            let (program, args) = shell.command_parts(command);
            let mut child = match tokio::process::Command::new(program)
                .args(args)
                .stdout(std::process::Stdio::inherit())
                .stderr(std::process::Stdio::inherit())
                .kill_on_drop(true)
                .spawn()
            {
                Ok(child) => child,
                Err(e) => {
                    result.status = TaskStatus::Failed;
                    result.duration = Some(start_time.elapsed());
                    result.error = Some(format!("Failed to spawn shell command: {}", e));
                    return result;
                }
            };

            match child.wait().await {
                Ok(status) if status.success() => Ok(()),
                Ok(status) => Err(anyhow::anyhow!(
                    "Command exited with status code: {}",
                    status
                )),
                Err(e) => Err(anyhow::anyhow!("Failed to await shell command: {}", e)),
            }
        }
        Some(TaskAction::Fn(f)) => {
            let ctx = Context::with_shared(
                name.clone(),
                shared.pipeline_name.clone(),
                shared.workspace_root.clone(),
                Arc::clone(&shared.outputs),
                Arc::clone(&shared.completed),
            );
            f(ctx).await
        }
        None => Ok(()),
    };

    result.duration = Some(start_time.elapsed());
    if let Err(e) = action_result {
        result.status = TaskStatus::Failed;
        result.error = Some(e.to_string());
        return result;
    }

    let outputs = shared
        .outputs
        .read()
        .ok()
        .and_then(|store| store.get(&name).cloned())
        .unwrap_or_default();

    if let Some((hash, reason)) = cache_hash {
        if let Err(e) = cache_manager.save_cache(&name, &hash, outputs) {
            result.cache_reason = Some(format!("{}; failed to save cache: {}", reason, e));
        } else {
            result.cache_reason = Some(reason);
        }
    }

    result.status = TaskStatus::Completed;
    emit(
        &callback,
        PipelineEvent::TaskCompleted {
            name: name.clone(),
            duration: result.duration.unwrap_or_default(),
        },
    );
    result
}

async fn rollback_task(
    name: &str,
    tasks: &HashMap<String, Task>,
    shared: &RunShared,
    event_cb: &Option<EventCallback>,
    results: &mut HashMap<String, TaskResult>,
    mark_rolled_back: bool,
) {
    let Some(task) = tasks.get(name) else {
        return;
    };
    let Some(handler) = task.rollback_handler.as_ref() else {
        return;
    };

    emit(
        event_cb,
        PipelineEvent::TaskRollbackStarted {
            name: name.to_string(),
        },
    );
    let ctx = Context::with_shared(
        name.to_string(),
        shared.pipeline_name.clone(),
        shared.workspace_root.clone(),
        Arc::clone(&shared.outputs),
        Arc::clone(&shared.completed),
    );
    match run_rollback(handler, ctx).await {
        Ok(()) => {
            if let Some(result) = results.get_mut(name) {
                result.rollback_status = Some(RollbackStatus::Completed);
                if mark_rolled_back {
                    result.status = TaskStatus::RolledBack;
                }
            }
            emit(
                event_cb,
                PipelineEvent::TaskRollbackCompleted {
                    name: name.to_string(),
                },
            );
        }
        Err(e) => {
            let error = e.to_string();
            if let Some(result) = results.get_mut(name) {
                result.rollback_status = Some(RollbackStatus::Failed);
                result.rollback_error = Some(error.clone());
            }
            emit(
                event_cb,
                PipelineEvent::TaskRollbackFailed {
                    name: name.to_string(),
                    error,
                },
            );
        }
    }
}

async fn run_rollback(handler: &RollbackFn, ctx: Context) -> anyhow::Result<()> {
    handler(ctx).await
}

fn mark_cancelled(
    name: &str,
    event_cb: &Option<EventCallback>,
    results: &mut HashMap<String, TaskResult>,
) {
    if let Some(result) = results.get_mut(name) {
        result.status = TaskStatus::Cancelled;
    }
    emit(
        event_cb,
        PipelineEvent::TaskCancelled {
            name: name.to_string(),
        },
    );
}

fn mark_pending_skipped(
    reason: &str,
    event_cb: &Option<EventCallback>,
    results: &mut HashMap<String, TaskResult>,
) {
    let mut pending: Vec<_> = results
        .iter()
        .filter_map(|(name, result)| (result.status == TaskStatus::Pending).then_some(name.clone()))
        .collect();
    pending.sort();
    for name in pending {
        if let Some(result) = results.get_mut(&name) {
            result.status = TaskStatus::Skipped;
            result.error = Some(reason.to_string());
        }
        emit(
            event_cb,
            PipelineEvent::TaskSkipped {
                name,
                reason: reason.to_string(),
            },
        );
    }
}

fn summarize(name: &str, tasks: &[TaskResult]) -> PipelineSummary {
    let mut summary = PipelineSummary {
        name: name.to_string(),
        success: true,
        completed: 0,
        failed: 0,
        skipped: 0,
        cached: 0,
        cancelled: 0,
        rolled_back: 0,
        rollback_failed: 0,
    };

    for task in tasks {
        match task.status {
            TaskStatus::Completed => summary.completed += 1,
            TaskStatus::Failed => summary.failed += 1,
            TaskStatus::Skipped => summary.skipped += 1,
            TaskStatus::Cached => summary.cached += 1,
            TaskStatus::Cancelled => summary.cancelled += 1,
            TaskStatus::RolledBack => summary.rolled_back += 1,
            TaskStatus::Pending | TaskStatus::Running => {}
        }
        if task.rollback_status == Some(RollbackStatus::Failed) {
            summary.rollback_failed += 1;
        }
    }
    summary.success = summary.failed == 0
        && summary.skipped == 0
        && summary.cancelled == 0
        && summary.rollback_failed == 0;
    summary
}

fn emit(callback: &Option<EventCallback>, event: PipelineEvent) {
    if let Some(cb) = callback {
        cb(event);
    }
}

fn dot_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn collect_dependencies(
    task: &str,
    tasks: &HashMap<String, Task>,
    needed: &mut HashSet<String>,
) -> anyhow::Result<()> {
    if !needed.insert(task.to_string()) {
        return Ok(());
    }
    let task_def = tasks
        .get(task)
        .ok_or_else(|| anyhow::anyhow!("Task '{}' does not exist in the pipeline", task))?;
    for dependency in &task_def.dependencies {
        collect_dependencies(dependency, tasks, needed)?;
    }
    Ok(())
}

/// Helper function to perform topological sorting and cycle detection.
fn find_execution_order(tasks: &HashMap<String, Task>) -> anyhow::Result<Vec<String>> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum NodeState {
        Visiting,
        Visited,
    }

    let mut states = HashMap::new();
    let mut order = Vec::new();

    for (name, task) in tasks {
        for dep in &task.dependencies {
            if !tasks.contains_key(dep) {
                anyhow::bail!(
                    "Task '{}' depends on '{}', which does not exist in the pipeline",
                    name,
                    dep
                );
            }
        }
    }

    fn dfs(
        node: &str,
        tasks: &HashMap<String, Task>,
        states: &mut HashMap<String, NodeState>,
        order: &mut Vec<String>,
        path: &mut Vec<String>,
    ) -> anyhow::Result<()> {
        path.push(node.to_string());
        states.insert(node.to_string(), NodeState::Visiting);

        if let Some(task) = tasks.get(node) {
            for dep in &task.dependencies {
                match states.get(dep) {
                    Some(NodeState::Visiting) => {
                        let cycle_start = path.iter().position(|x| x == dep).unwrap();
                        let cycle_path = path[cycle_start..].join(" -> ");
                        anyhow::bail!("Circular dependency detected: {} -> {}", cycle_path, dep);
                    }
                    Some(NodeState::Visited) => {}
                    None => {
                        dfs(dep, tasks, states, order, path)?;
                    }
                }
            }
        }

        states.insert(node.to_string(), NodeState::Visited);
        path.pop();
        order.push(node.to_string());
        Ok(())
    }

    let mut path = Vec::new();
    let mut names: Vec<_> = tasks.keys().cloned().collect();
    names.sort();
    for name in names {
        if !states.contains_key(&name) {
            dfs(&name, tasks, &mut states, &mut order, &mut path)?;
        }
    }

    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{CacheEligibility, CacheManager};
    use crate::task::Task;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    fn status(result: &PipelineResult, name: &str) -> TaskStatus {
        result
            .tasks
            .iter()
            .find(|task| task.name == name)
            .unwrap()
            .status
            .clone()
    }

    #[tokio::test]
    async fn test_dag_success() {
        let mut p = Pipeline::new("test");
        p.add(Task::new("t1").exec("echo 't1'"));
        p.add(Task::new("t2").depends_on(&["t1"]).exec("echo 't2'"));
        let res = p.run().await.unwrap();
        assert!(res.summary.success);
        assert_eq!(status(&res, "t1"), TaskStatus::Completed);
        assert_eq!(status(&res, "t2"), TaskStatus::Completed);
    }

    #[tokio::test]
    async fn test_cycle_detection() {
        let mut p = Pipeline::new("test_cycle");
        p.add(Task::new("t1").depends_on(&["t2"]).exec("echo 't1'"));
        p.add(Task::new("t2").depends_on(&["t1"]).exec("echo 't2'"));
        let res = p.run().await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Circular dependency detected"));
    }

    #[tokio::test]
    async fn test_missing_dependency() {
        let mut p = Pipeline::new("test_missing");
        p.add(
            Task::new("t1")
                .depends_on(&["nonexistent"])
                .exec("echo 't1'"),
        );
        let res = p.run().await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("does not exist in the pipeline"));
    }

    #[tokio::test]
    async fn test_rollback_on_failure() {
        static ROLLED_BACK: AtomicBool = AtomicBool::new(false);

        let mut p = Pipeline::new("test_failure");
        p.add(
            Task::new("fail-task")
                .exec_fn(|_| async move { anyhow::bail!("forced failure") })
                .on_failure(|_| async move {
                    ROLLED_BACK.store(true, Ordering::SeqCst);
                }),
        );

        let res = p.run().await.unwrap();
        assert!(!res.summary.success);
        assert_eq!(status(&res, "fail-task"), TaskStatus::Failed);
        assert!(ROLLED_BACK.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_output_passing() {
        let mut p = Pipeline::new("outputs");
        p.add(Task::new("build").exec_fn(|ctx| async move {
            ctx.set_output("artifact", "dist/app.wasm")?;
            Ok(())
        }));
        p.add(
            Task::new("deploy")
                .depends_on(&["build"])
                .exec_fn(|ctx| async move {
                    let artifact: String = ctx.output_from("build", "artifact")?;
                    assert_eq!(artifact, "dist/app.wasm");
                    Ok(())
                }),
        );

        let res = p.run().await.unwrap();
        assert!(res.summary.success);
    }

    #[tokio::test]
    async fn test_premature_output_read_fails() {
        let mut p = Pipeline::new("outputs-missing");
        p.add(Task::new("read").exec_fn(|ctx| async move {
            let _: String = ctx.output_from("build", "artifact")?;
            Ok(())
        }));

        let res = p.run().await.unwrap();
        assert!(!res.summary.success);
        assert_eq!(status(&res, "read"), TaskStatus::Failed);
    }

    #[tokio::test]
    async fn test_continue_independent_policy() {
        let mut p = Pipeline::new("continue").failure_policy(FailurePolicy::ContinueIndependent);
        p.add(Task::new("fail").exec_fn(|_| async move { anyhow::bail!("nope") }));
        p.add(
            Task::new("blocked")
                .depends_on(&["fail"])
                .exec("echo blocked"),
        );
        p.add(Task::new("independent").exec("echo independent"));

        let res = p.run().await.unwrap();
        assert_eq!(status(&res, "fail"), TaskStatus::Failed);
        assert_eq!(status(&res, "blocked"), TaskStatus::Skipped);
        assert_eq!(status(&res, "independent"), TaskStatus::Completed);
    }

    #[tokio::test]
    async fn test_run_task_executes_only_dependency_closure() {
        static UNRELATED_RAN: AtomicBool = AtomicBool::new(false);

        let mut p = Pipeline::new("selected-task");
        p.add(Task::new("build").exec_fn(|_| async move { Ok(()) }));
        p.add(
            Task::new("deploy")
                .depends_on(&["build"])
                .exec_fn(|_| async move { Ok(()) }),
        );
        p.add(Task::new("unrelated").exec_fn(|_| async move {
            UNRELATED_RAN.store(true, Ordering::SeqCst);
            Ok(())
        }));

        let res = p.run_task("deploy").await.unwrap();
        assert_eq!(res.tasks.len(), 2);
        assert_eq!(status(&res, "build"), TaskStatus::Completed);
        assert_eq!(status(&res, "deploy"), TaskStatus::Completed);
        assert!(!UNRELATED_RAN.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_finish_running_policy_skips_new_tasks() {
        let mut p = Pipeline::new("finish").failure_policy(FailurePolicy::FinishRunning);
        p.add(Task::new("fail").exec_fn(|_| async move { anyhow::bail!("nope") }));
        p.add(
            Task::new("blocked")
                .depends_on(&["fail"])
                .exec("echo blocked"),
        );

        let res = p.run().await.unwrap();
        assert_eq!(status(&res, "fail"), TaskStatus::Failed);
        assert_eq!(status(&res, "blocked"), TaskStatus::Skipped);
    }

    #[tokio::test]
    async fn test_reverse_rollback_marks_completed_task() {
        static ROLLED_BACK: AtomicBool = AtomicBool::new(false);

        let mut p = Pipeline::new("rollback-reverse")
            .rollback_policy(RollbackPolicy::CompletedTasksReverseOrder);
        p.add(
            Task::new("done")
                .exec_fn(|_| async move { Ok(()) })
                .rollback(|_| async move {
                    ROLLED_BACK.store(true, Ordering::SeqCst);
                    Ok(())
                }),
        );
        p.add(
            Task::new("fail")
                .depends_on(&["done"])
                .exec_fn(|_| async move { anyhow::bail!("nope") }),
        );

        let res = p.run().await.unwrap();
        assert!(ROLLED_BACK.load(Ordering::SeqCst));
        assert_eq!(status(&res, "done"), TaskStatus::RolledBack);
    }

    #[tokio::test]
    async fn test_rollback_failure_is_recorded() {
        let mut p = Pipeline::new("rollback-failure")
            .rollback_policy(RollbackPolicy::CompletedTasksReverseOrder);
        p.add(
            Task::new("done")
                .exec_fn(|_| async move { Ok(()) })
                .rollback(|_| async move { anyhow::bail!("rollback failed") }),
        );
        p.add(
            Task::new("fail")
                .depends_on(&["done"])
                .exec_fn(|_| async move { anyhow::bail!("nope") }),
        );

        let res = p.run().await.unwrap();
        let done = res.tasks.iter().find(|task| task.name == "done").unwrap();
        assert_eq!(done.rollback_status, Some(RollbackStatus::Failed));
        assert_eq!(res.summary.rollback_failed, 1);
    }

    #[tokio::test]
    async fn test_fail_fast_cancels_active_task() {
        let mut p = Pipeline::new("fail-fast").failure_policy(FailurePolicy::FailFast);
        p.add(Task::new("fail").exec_fn(|_| async move { anyhow::bail!("nope") }));
        p.add(Task::new("slow").exec_fn(|_| async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        }));

        let res = p.run().await.unwrap();
        assert_eq!(status(&res, "fail"), TaskStatus::Failed);
        assert_eq!(status(&res, "slow"), TaskStatus::Cancelled);
    }

    #[tokio::test]
    async fn test_cache_hit_restores_outputs() {
        static RUNS: AtomicUsize = AtomicUsize::new(0);
        let pipeline_name = format!(
            "cache-outputs-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        for _ in 0..2 {
            let mut p = Pipeline::new(pipeline_name.clone());
            p.add(
                Task::new("build")
                    .exec_fn(|ctx| async move {
                        RUNS.fetch_add(1, Ordering::SeqCst);
                        ctx.set_output("artifact", "dist/app.wasm")?;
                        Ok(())
                    })
                    .cache_key("build-output-v1"),
            );
            p.add(
                Task::new("deploy")
                    .depends_on(&["build"])
                    .exec_fn(|ctx| async move {
                        let artifact: String = ctx.output_from("build", "artifact")?;
                        assert_eq!(artifact, "dist/app.wasm");
                        Ok(())
                    }),
            );
            let res = p.run().await.unwrap();
            assert!(res.summary.success);
        }

        let mut p = Pipeline::new(pipeline_name);
        p.add(
            Task::new("build")
                .exec_fn(|ctx| async move {
                    RUNS.fetch_add(1, Ordering::SeqCst);
                    ctx.set_output("artifact", "dist/app.wasm")?;
                    Ok(())
                })
                .cache_key("build-output-v1"),
        );
        p.add(
            Task::new("deploy")
                .depends_on(&["build"])
                .exec_fn(|ctx| async move {
                    let _: String = ctx.output_from("build", "artifact")?;
                    Ok(())
                }),
        );
        let res = p.run().await.unwrap();
        assert_eq!(status(&res, "build"), TaskStatus::Cached);
        assert_eq!(RUNS.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_env_var_changes_invalidate_hash() {
        let key = "RUNKERNEL_TEST_CACHE_ENV_VAR";
        let manager = CacheManager::new();
        let task = Task::new("env").env_vars(&[key]);

        std::env::set_var(key, "one");
        let first = match manager.compute_hash("pipeline", &task).unwrap() {
            CacheEligibility::Enabled { hash, .. } => hash,
            CacheEligibility::Disabled(_) => panic!("task should be cacheable"),
        };

        std::env::set_var(key, "two");
        let second = match manager.compute_hash("pipeline", &task).unwrap() {
            CacheEligibility::Enabled { hash, .. } => hash,
            CacheEligibility::Disabled(_) => panic!("task should be cacheable"),
        };

        assert_ne!(first, second);
    }

    #[test]
    fn test_invalid_glob_returns_error() {
        let manager = CacheManager::new();
        let err = manager
            .compute_hash("pipeline", &Task::new("bad-glob").inputs(&["["]))
            .unwrap_err();
        assert!(err.to_string().contains("Invalid glob pattern"));
    }

    #[tokio::test]
    async fn test_events_match_result_transitions() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&events);
        let mut p = Pipeline::new("events").with_callback(move |event| {
            captured.lock().unwrap().push(event);
        });
        p.add(Task::new("task").exec_fn(|_| async move { Ok(()) }));

        let res = p.run().await.unwrap();
        assert_eq!(status(&res, "task"), TaskStatus::Completed);

        let events = events.lock().unwrap();
        assert!(events
            .iter()
            .any(|event| matches!(event, PipelineEvent::TaskQueued { name } if name == "task")));
        assert!(events
            .iter()
            .any(|event| matches!(event, PipelineEvent::TaskStarted { name } if name == "task")));
        assert!(events.iter().any(|event| {
            matches!(event, PipelineEvent::TaskCompleted { name, .. } if name == "task")
        }));
        assert!(events.iter().any(
            |event| matches!(event, PipelineEvent::PipelineFinished { result } if result.success)
        ));
    }

    #[test]
    fn test_to_dot_exports_validated_graph() {
        let mut p = Pipeline::new("graph");
        p.add(Task::new("build").exec("cargo build"));
        p.add(Task::new("test").depends_on(&["build"]).exec("cargo test"));

        let dot = p.to_dot().unwrap();
        assert!(dot.contains("digraph \"graph\""));
        assert!(dot.contains("\"build\" -> \"test\""));
    }

    #[test]
    fn test_to_dot_rejects_invalid_graph() {
        let mut p = Pipeline::new("bad-graph");
        p.add(
            Task::new("test")
                .depends_on(&["missing"])
                .exec("cargo test"),
        );

        let err = p.to_dot().unwrap_err();
        assert!(err.to_string().contains("does not exist in the pipeline"));
    }

    #[test]
    fn test_explain_task_reports_public_shape() {
        let mut p = Pipeline::new("explain").shell(Shell::Bash);
        p.add(
            Task::new("build")
                .exec("cargo build")
                .inputs(&["src/**/*.rs"])
                .env_vars(&["PROFILE"])
                .cache_key("build-v1")
                .rollback(|_| async move { Ok(()) }),
        );
        p.add(Task::new("test").depends_on(&["build"]).exec("cargo test"));

        let explanation = p.explain_task("build").unwrap();
        assert_eq!(explanation.name, "build");
        assert_eq!(explanation.dependents, vec!["test"]);
        assert_eq!(explanation.action, "shell");
        assert_eq!(explanation.cache_mode, "explicit:build-v1");
        assert_eq!(explanation.shell, Some("Bash".to_string()));
        assert!(explanation.has_rollback);
    }

    #[test]
    fn test_shell_command_parts() {
        let (program, args) = Shell::Bash.command_parts("cargo test");
        assert_eq!(program, "bash");
        assert_eq!(args, vec!["-c", "cargo test"]);
    }
}
