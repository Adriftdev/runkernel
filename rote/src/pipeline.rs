use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use futures::stream::{FuturesUnordered, StreamExt};
use crate::cache::CacheManager;
use crate::context::Context;
use crate::task::{Task, TaskAction};

pub enum PipelineEvent {
    PipelineStarted { name: String, task_count: usize },
    TaskStarted { name: String },
    TaskCached { name: String },
    TaskCompleted { name: String, duration: std::time::Duration },
    TaskFailed { name: String, error: String },
    TaskRollback { name: String },
    PipelineFinished { name: String, duration: std::time::Duration, cached: usize, completed: usize, failed: usize },
}

pub type EventCallback = Arc<dyn Fn(PipelineEvent) + Send + Sync>;

pub struct Pipeline {
    pub name: String,
    pub tasks: HashMap<String, Task>,
    pub event_callback: Option<EventCallback>,
}

impl Pipeline {
    /// Creates a new Pipeline.
    pub fn new(name: impl Into<String>) -> Self {
        Pipeline {
            name: name.into(),
            tasks: HashMap::new(),
            event_callback: None,
        }
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

    /// Runs the pipeline. Resolves the DAG, checks for cycles, handles parallel execution,
    /// skips tasks if cached, and executes rollback handlers on failure.
    pub async fn run(self) -> anyhow::Result<()> {
        let event_cb = self.event_callback.clone();

        // 1. Validate DAG and detect cycles
        let _ordered = find_execution_order(&self.tasks)?;

        // 2. Setup dependency coordination structures
        let tasks = Arc::new(self.tasks);
        let mut dep_counts = HashMap::new();
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

        for (name, task) in tasks.iter() {
            dep_counts.insert(name.clone(), task.dependencies.len());
            for dep in &task.dependencies {
                dependents.entry(dep.clone()).or_default().push(name.clone());
            }
        }

        let mut active_futures = FuturesUnordered::new();
        let mut completed_tasks = HashSet::new();
        let mut pipeline_failed = false;
        let mut cached_count = 0;
        let mut completed_count = 0;
        let mut failed_count = 0;

        let start_time = std::time::Instant::now();

        if let Some(ref cb) = event_cb {
            cb(PipelineEvent::PipelineStarted {
                name: self.name.clone(),
                task_count: tasks.len(),
            });
        } else {
            println!("=== Starting Pipeline: {} ===", self.name);
        }

        // Start tasks with zero initial dependencies
        for (name, count) in &dep_counts {
            if *count == 0 {
                let task_name = name.clone();
                let tasks_clone = Arc::clone(&tasks);
                let cb_clone = event_cb.clone();
                active_futures.push(execute_task_wrapper(task_name, tasks_clone, cb_clone));
            }
        }

        // Exec coordination loop
        while let Some(result) = active_futures.next().await {
            match result {
                Ok((name, cached)) => {
                    if cached {
                        cached_count += 1;
                        if event_cb.is_none() {
                            println!("Task '{}' completed successfully (cached).", name);
                        }
                    } else {
                        completed_count += 1;
                        if event_cb.is_none() {
                            println!("Task '{}' completed successfully.", name);
                        }
                    }
                    completed_tasks.insert(name.clone());

                    // If the pipeline has already failed, do not start new tasks
                    if !pipeline_failed {
                        if let Some(deps) = dependents.get(&name) {
                            for dep in deps {
                                if let Some(count) = dep_counts.get_mut(dep) {
                                    *count -= 1;
                                    if *count == 0 {
                                        let task_name = dep.clone();
                                        let tasks_clone = Arc::clone(&tasks);
                                        let cb_clone = event_cb.clone();
                                        active_futures.push(execute_task_wrapper(task_name, tasks_clone, cb_clone));
                                    }
                                }
                            }
                        }
                    }
                }
                Err((name, err)) => {
                    failed_count += 1;
                    if let Some(ref cb) = event_cb {
                        cb(PipelineEvent::TaskFailed {
                            name: name.clone(),
                            error: err.to_string(),
                        });
                    } else {
                        eprintln!("Task '{}' failed: {:?}", name, err);
                    }
                    pipeline_failed = true;

                    // Trigger the failure/rollback handler for this task if defined
                    if let Some(task) = tasks.get(&name) {
                        if let Some(ref failure_handler) = task.failure_handler {
                            if let Some(ref cb) = event_cb {
                                cb(PipelineEvent::TaskRollback { name: name.clone() });
                            } else {
                                println!("Running rollback handler for task '{}'...", name);
                            }
                            let ctx = Context::new(name.clone());
                            failure_handler(ctx).await;
                        }
                    }
                }
            }
        }

        let duration = start_time.elapsed();

        if let Some(ref cb) = event_cb {
            cb(PipelineEvent::PipelineFinished {
                name: self.name.clone(),
                duration,
                cached: cached_count,
                completed: completed_count,
                failed: failed_count,
            });
        } else {
            if pipeline_failed {
                println!("=== Pipeline Failed: {} ===", self.name);
            } else {
                println!("=== Pipeline Completed Successfully: {} ===", self.name);
            }
        }

        if pipeline_failed {
            anyhow::bail!("Pipeline execution failed due to errors in tasks.");
        }

        Ok(())
    }
}

/// Helper task execution wrapper to run caching checks and execute tasks on background threads.
async fn execute_task_wrapper(
    name: String,
    tasks: Arc<HashMap<String, Task>>,
    callback: Option<EventCallback>,
) -> Result<(String, bool), (String, anyhow::Error)> {
    let task = tasks.get(&name).ok_or_else(|| {
        (name.clone(), anyhow::anyhow!("Task '{}' not found in workspace", name))
    })?;

    let cache_manager = CacheManager::new();

    // Check cache if qualifies
    let cache_hash = if task.is_cacheable() {
        match cache_manager.compute_hash(task) {
            Ok(hash) => {
                if cache_manager.is_cached(&name, &hash) {
                    if let Some(ref cb) = callback {
                        cb(PipelineEvent::TaskCached { name: name.clone() });
                    } else {
                        println!("Task '{}' is CACHED (skipping execution)", name);
                    }
                    return Ok((name, true));
                }
                Some(hash)
            }
            Err(e) => {
                return Err((name, anyhow::anyhow!("Failed to compute cache hash: {}", e)));
            }
        }
    } else {
        None
    };

    if let Some(ref cb) = callback {
        cb(PipelineEvent::TaskStarted { name: name.clone() });
    } else {
        println!("Running task '{}'...", name);
    }

    let start_time = std::time::Instant::now();

    // Run action
    match &task.action {
        Some(TaskAction::Shell(cmd)) => {
            let mut child = tokio::process::Command::new("sh")
                .arg("-c")
                .arg(cmd)
                .stdout(std::process::Stdio::inherit())
                .stderr(std::process::Stdio::inherit())
                .spawn()
                .map_err(|e| (name.clone(), anyhow::anyhow!("Failed to spawn shell command: {}", e)))?;

            let status = child.wait().await
                .map_err(|e| (name.clone(), anyhow::anyhow!("Failed to await shell command: {}", e)))?;

            if !status.success() {
                return Err((name.clone(), anyhow::anyhow!("Command exited with status code: {}", status)));
            }
        }
        Some(TaskAction::Fn(f)) => {
            let ctx = Context::new(name.clone());
            let fut = f(ctx);
            if let Err(e) = fut.await {
                return Err((name.clone(), e));
            }
        }
        None => {}
    }

    // Save cache
    if let Some(hash) = cache_hash {
        if let Err(e) = cache_manager.save_cache(&name, &hash) {
            eprintln!("Warning: failed to save cache for task '{}': {}", name, e);
        }
    }

    let duration = start_time.elapsed();
    if let Some(ref cb) = callback {
        cb(PipelineEvent::TaskCompleted {
            name: name.clone(),
            duration,
        });
    }

    Ok((name, false))
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

    // Verify all dependencies actually exist
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

    // Inner DFS cycle-checking & ordering recursive function
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
    for name in tasks.keys() {
        if !states.contains_key(name) {
            dfs(name, tasks, &mut states, &mut order, &mut path)?;
        }
    }

    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Task;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn test_dag_success() {
        let mut p = Pipeline::new("test");
        p.add(Task::new("t1").exec("echo 't1'"));
        p.add(Task::new("t2").depends_on(&["t1"]).exec("echo 't2'"));
        let res = p.run().await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_cycle_detection() {
        let mut p = Pipeline::new("test_cycle");
        p.add(Task::new("t1").depends_on(&["t2"]).exec("echo 't1'"));
        p.add(Task::new("t2").depends_on(&["t1"]).exec("echo 't2'"));
        let res = p.run().await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("Circular dependency detected"));
    }

    #[tokio::test]
    async fn test_missing_dependency() {
        let mut p = Pipeline::new("test_missing");
        p.add(Task::new("t1").depends_on(&["nonexistent"]).exec("echo 't1'"));
        let res = p.run().await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("does not exist in the pipeline"));
    }

    #[tokio::test]
    async fn test_rollback_on_failure() {
        static ROLLED_BACK: AtomicBool = AtomicBool::new(false);

        let mut p = Pipeline::new("test_failure");
        p.add(Task::new("fail-task")
            .exec_fn(|_| async move {
                anyhow::bail!("forced failure")
            })
            .on_failure(|_| async move {
                ROLLED_BACK.store(true, Ordering::SeqCst);
            })
        );

        let res = p.run().await;
        assert!(res.is_err());
        assert!(ROLLED_BACK.load(Ordering::SeqCst));
    }
}
