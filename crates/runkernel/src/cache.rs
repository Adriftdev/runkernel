use crate::task::{CacheMode, Task, TaskAction};
use glob::glob;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheEntry {
    pub task_name: String,
    pub hash: String,
    pub timestamp: u64,
    #[serde(default)]
    pub outputs: HashMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheEligibility {
    Disabled(String),
    Enabled { hash: String, reason: String },
}

#[derive(Clone, Debug)]
pub enum CacheLookup {
    Hit {
        reason: String,
        outputs: HashMap<String, Value>,
    },
    Miss {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheCleanResult {
    pub path: PathBuf,
    pub removed: bool,
}

pub struct CacheManager {
    cache_root: PathBuf,
    cache_dir: PathBuf,
}

impl Default for CacheManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheManager {
    /// Creates a CacheManager pointing to `.runkernel/cache/default`.
    pub fn new() -> Self {
        Self::with_cache_root(PathBuf::from(".runkernel").join("cache"))
    }

    /// Creates a CacheManager with a custom cache root.
    pub fn with_cache_root(cache_root: impl Into<PathBuf>) -> Self {
        let cache_root = cache_root.into();
        CacheManager {
            cache_dir: cache_root.join("default"),
            cache_root,
        }
    }

    /// Creates a CacheManager scoped to a pipeline namespace.
    pub fn for_pipeline(pipeline_name: &str) -> Self {
        let cache_root = PathBuf::from(".runkernel").join("cache");
        CacheManager {
            cache_dir: cache_root.join(pipeline_hash(pipeline_name)),
            cache_root,
        }
    }

    /// Returns the root directory for all runkernel cache entries.
    pub fn cache_root(&self) -> PathBuf {
        self.cache_root.clone()
    }

    /// Returns the cache namespace directory for a pipeline.
    pub fn pipeline_cache_dir(&self, pipeline_name: &str) -> PathBuf {
        self.cache_root.join(pipeline_hash(pipeline_name))
    }

    /// Removes the full cache root if it exists.
    pub fn clean_all(&self) -> anyhow::Result<CacheCleanResult> {
        clean_dir(self.cache_root.clone())
    }

    /// Removes one pipeline cache namespace if it exists.
    pub fn clean_pipeline(&self, pipeline_name: &str) -> anyhow::Result<CacheCleanResult> {
        clean_dir(self.pipeline_cache_dir(pipeline_name))
    }

    /// Computes cache eligibility and current task hash.
    pub fn compute_hash(
        &self,
        pipeline_name: &str,
        task: &Task,
    ) -> anyhow::Result<CacheEligibility> {
        let is_native_function = matches!(&task.action, Some(TaskAction::Fn(_)));
        match &task.cache_mode {
            CacheMode::Disabled => {
                return Ok(CacheEligibility::Disabled(
                    "cache disabled for task".to_string(),
                ));
            }
            CacheMode::Inputs if task.inputs.is_empty() && task.env_vars.is_empty() => {
                if is_native_function {
                    return Ok(CacheEligibility::Disabled(
                        "native function task cache disabled: no declared inputs, environment variables, or explicit cache key; closure body is not hashed"
                            .to_string(),
                    ));
                }
                return Ok(CacheEligibility::Disabled(
                    "no declared file inputs, environment variables, or explicit cache key"
                        .to_string(),
                ));
            }
            CacheMode::Inputs | CacheMode::Explicit { .. } => {}
        }

        let mut hasher = Sha256::new();
        hasher.update(b"pipeline:");
        hasher.update(pipeline_name.as_bytes());
        hasher.update(b"\ntask:");
        hasher.update(task.name.as_bytes());

        let mut dependencies = task.dependencies.clone();
        dependencies.sort();
        hasher.update(b"\ndependencies:");
        for dep in dependencies {
            hasher.update(dep.as_bytes());
            hasher.update(b";");
        }

        hasher.update(b"\naction:");
        match &task.action {
            Some(TaskAction::Shell { command, shell }) => {
                hasher.update(b"shell:");
                hasher.update(format!("{:?}", shell).as_bytes());
                hasher.update(b":");
                hasher.update(command.as_bytes());
            }
            Some(TaskAction::Fn(_)) => {
                hasher.update(b"fn:");
            }
            None => {
                hasher.update(b"none:");
            }
        }

        if let CacheMode::Explicit { key } = &task.cache_mode {
            hasher.update(b"\nexplicit-key:");
            hasher.update(key.as_bytes());
        }

        let mut sorted_envs = task.env_vars.clone();
        sorted_envs.sort();
        hasher.update(b"\nenv:");
        for key in sorted_envs {
            let val = std::env::var(&key).unwrap_or_default();
            hasher.update(key.as_bytes());
            hasher.update(b"=");
            hasher.update(val.as_bytes());
            hasher.update(b";");
        }

        hasher.update(b"\ninputs:");
        for file_path in self.resolve_inputs(task, &mut hasher)? {
            let mut file = match File::open(&file_path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    hasher.update(file_path.to_string_lossy().as_bytes());
                    hasher.update(b":missing\n");
                    continue;
                }
                Err(e) => {
                    anyhow::bail!(
                        "Failed to open input file '{}' for hashing: {}",
                        file_path.display(),
                        e
                    );
                }
            };

            hasher.update(file_path.to_string_lossy().as_bytes());
            hasher.update(b":");
            let mut buffer = [0; 8192];
            loop {
                let count = file.read(&mut buffer).map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to read input file '{}' for hashing: {}",
                        file_path.display(),
                        e
                    )
                })?;
                if count == 0 {
                    break;
                }
                hasher.update(&buffer[..count]);
            }
            hasher.update(b"\n");
        }

        Ok(CacheEligibility::Enabled {
            hash: hex::encode(hasher.finalize()),
            reason: match (&task.cache_mode, is_native_function) {
                (CacheMode::Explicit { .. }, true) => {
                    "explicit cache key for native function task; closure body is not hashed"
                        .to_string()
                }
                (CacheMode::Inputs, true) => {
                    "native function task cache uses declared inputs/env only; closure body is not hashed"
                        .to_string()
                }
                (CacheMode::Explicit { .. }, false) => {
                    "explicit cache key and declared inputs".to_string()
                }
                (CacheMode::Inputs, false) => "declared inputs or environment variables".to_string(),
                (CacheMode::Disabled, _) => unreachable!(),
            },
        })
    }

    /// Checks whether the task has a valid cache entry.
    pub fn lookup(&self, task_name: &str, current_hash: &str) -> CacheLookup {
        let cache_file = self.cache_file(task_name);
        if !cache_file.exists() {
            return CacheLookup::Miss {
                reason: "no cache entry found".to_string(),
            };
        }

        match std::fs::read_to_string(&cache_file)
            .ok()
            .and_then(|content| serde_json::from_str::<CacheEntry>(&content).ok())
        {
            Some(entry) if entry.hash == current_hash => CacheLookup::Hit {
                reason: "cache entry hash matches current task identity".to_string(),
                outputs: entry.outputs,
            },
            Some(_) => CacheLookup::Miss {
                reason: "cache entry hash does not match current task identity".to_string(),
            },
            None => CacheLookup::Miss {
                reason: "cache entry could not be parsed".to_string(),
            },
        }
    }

    /// Saves a cache entry for a successful task execution.
    pub fn save_cache(
        &self,
        task_name: &str,
        current_hash: &str,
        outputs: HashMap<String, Value>,
    ) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.cache_dir)?;
        let cache_file = self.cache_file(task_name);

        let seconds = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = CacheEntry {
            task_name: task_name.to_string(),
            hash: current_hash.to_string(),
            timestamp: seconds,
            outputs,
        };

        let content = serde_json::to_string_pretty(&entry)?;
        std::fs::write(cache_file, content)?;
        Ok(())
    }

    fn cache_file(&self, task_name: &str) -> PathBuf {
        self.cache_dir
            .join(format!("{}.json", task_file_name(task_name)))
    }

    fn resolve_inputs(&self, task: &Task, hasher: &mut Sha256) -> anyhow::Result<Vec<PathBuf>> {
        let mut files_to_hash = Vec::new();
        let mut patterns = task.inputs.clone();
        patterns.sort();

        for pattern in &patterns {
            hasher.update(b"pattern:");
            hasher.update(pattern.as_bytes());
            hasher.update(b"\n");

            let mut matched = false;
            match glob(pattern) {
                Ok(paths) => {
                    for path in paths {
                        let path = path.map_err(|e| {
                            anyhow::anyhow!("Failed to resolve glob pattern '{}': {}", pattern, e)
                        })?;
                        if path.is_file() {
                            matched = true;
                            if !should_ignore_path(&path) {
                                files_to_hash.push(path);
                            }
                        }
                    }
                }
                Err(e) => {
                    anyhow::bail!("Invalid glob pattern '{}': {}", pattern, e);
                }
            }

            if !matched {
                hasher.update(b"pattern-matched:no-files\n");
            }
        }

        files_to_hash.sort();
        files_to_hash.dedup();
        Ok(files_to_hash)
    }
}

fn clean_dir(path: PathBuf) -> anyhow::Result<CacheCleanResult> {
    if path.exists() {
        std::fs::remove_dir_all(&path)?;
        Ok(CacheCleanResult {
            path,
            removed: true,
        })
    } else {
        Ok(CacheCleanResult {
            path,
            removed: false,
        })
    }
}

fn pipeline_hash(pipeline_name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(pipeline_name.as_bytes());
    hex::encode(hasher.finalize())[..16].to_string()
}

fn task_file_name(task_name: &str) -> String {
    let mut sanitized = String::new();
    for ch in task_name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    if sanitized.is_empty() {
        sanitized.push_str("task");
    }
    format!("{}-{}", sanitized, task_name_hash(task_name))
}

fn task_name_hash(task_name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(task_name.as_bytes());
    hex::encode(hasher.finalize())[..16].to_string()
}

fn should_ignore_path(path: &Path) -> bool {
    for component in path.components() {
        if let std::path::Component::Normal(name) = component {
            if let Some("target" | "node_modules" | ".git" | ".runkernel" | ".cargo" | ".next") =
                name.to_str()
            {
                return true;
            }
        }
    }
    false
}

impl Task {
    /// Determines if this task qualifies for caching.
    pub fn is_cacheable(&self) -> bool {
        !matches!(self.cache_mode, CacheMode::Disabled)
            && (!self.inputs.is_empty()
                || !self.env_vars.is_empty()
                || matches!(self.cache_mode, CacheMode::Explicit { .. }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn temp_cache_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "runkernel-{name}-{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ))
    }

    #[test]
    fn test_should_ignore_path() {
        assert!(should_ignore_path(Path::new(
            "services/drone/drone-ops/target/.rustc_info.json"
        )));
        assert!(should_ignore_path(Path::new("target/debug/deps/foo")));
        assert!(should_ignore_path(Path::new(
            "node_modules/lodash/index.js"
        )));
        assert!(should_ignore_path(Path::new(".git/config")));
        assert!(should_ignore_path(Path::new(".runkernel/cache/foo.json")));
        assert!(should_ignore_path(Path::new(".cargo/config.toml")));
        assert!(should_ignore_path(Path::new(
            ".next/server/pages/index.html"
        )));

        assert!(!should_ignore_path(Path::new(
            "services/drone/drone-ops/src/main.rs"
        )));
        assert!(!should_ignore_path(Path::new("src/lib.rs")));
        assert!(!should_ignore_path(Path::new("target_file.rs")));
        assert!(!should_ignore_path(Path::new("git_manager.rs")));
    }

    #[test]
    fn test_compute_hash_ignores_missing_file_gracefully() {
        let manager = CacheManager::new();
        let task = Task::new("test-task").inputs(&["this_file_does_not_exist_xyz.txt"]);

        let hash1 = match manager.compute_hash("pipeline", &task).unwrap() {
            CacheEligibility::Enabled { hash, .. } => hash,
            CacheEligibility::Disabled(_) => panic!("task should be cacheable"),
        };
        let hash2 = match manager.compute_hash("pipeline", &task).unwrap() {
            CacheEligibility::Enabled { hash, .. } => hash,
            CacheEligibility::Disabled(_) => panic!("task should be cacheable"),
        };
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_pipeline_hash_namespaces_same_task() {
        let first = CacheManager::for_pipeline("one").cache_file("build");
        let second = CacheManager::for_pipeline("two").cache_file("build");
        assert_ne!(first, second);
    }

    #[test]
    fn test_task_file_name_adds_hash_suffix_to_avoid_collisions() {
        let slash = task_file_name("foo/bar");
        let underscore = task_file_name("foo_bar");

        assert_ne!(slash, underscore);
        assert!(slash.starts_with("foo_bar-"));
        assert!(underscore.starts_with("foo_bar-"));
        assert!(!slash.ends_with(".json"));
    }

    #[test]
    fn test_shell_command_changes_hash() {
        let manager = CacheManager::new();
        let first = match manager
            .compute_hash(
                "pipeline",
                &Task::new("build").exec("echo one").cache_key("v1"),
            )
            .unwrap()
        {
            CacheEligibility::Enabled { hash, .. } => hash,
            CacheEligibility::Disabled(_) => panic!("task should be cacheable"),
        };
        let second = match manager
            .compute_hash(
                "pipeline",
                &Task::new("build").exec("echo two").cache_key("v1"),
            )
            .unwrap()
        {
            CacheEligibility::Enabled { hash, .. } => hash,
            CacheEligibility::Disabled(_) => panic!("task should be cacheable"),
        };
        assert_ne!(first, second);
    }

    #[test]
    fn test_native_function_without_identity_is_not_cacheable_and_explains_why() {
        let manager = CacheManager::new();
        let task = Task::new("native").exec_fn(|_| async move { Ok(()) });

        let reason = match manager.compute_hash("pipeline", &task).unwrap() {
            CacheEligibility::Disabled(reason) => reason,
            CacheEligibility::Enabled { .. } => panic!("task should not be cacheable"),
        };

        assert!(reason.contains("native function task cache disabled"));
        assert!(reason.contains("closure body is not hashed"));
    }

    #[test]
    fn test_native_function_with_inputs_explains_closure_is_not_hashed() {
        let manager = CacheManager::new();
        let task = Task::new("native")
            .exec_fn(|_| async move { Ok(()) })
            .env_vars(&["RUNKERNEL_NATIVE_CACHE_REASON"]);

        let reason = match manager.compute_hash("pipeline", &task).unwrap() {
            CacheEligibility::Enabled { reason, .. } => reason,
            CacheEligibility::Disabled(_) => panic!("task should be cacheable"),
        };

        assert!(reason.contains("declared inputs/env only"));
        assert!(reason.contains("closure body is not hashed"));
    }

    #[test]
    fn test_native_function_with_explicit_key_explains_closure_is_not_hashed() {
        let manager = CacheManager::new();
        let task = Task::new("native")
            .exec_fn(|_| async move { Ok(()) })
            .cache_key("native-v1");

        let reason = match manager.compute_hash("pipeline", &task).unwrap() {
            CacheEligibility::Enabled { reason, .. } => reason,
            CacheEligibility::Disabled(_) => panic!("task should be cacheable"),
        };

        assert!(reason.contains("explicit cache key for native function task"));
        assert!(reason.contains("closure body is not hashed"));
    }

    #[test]
    fn test_input_file_changes_hash() {
        let dir = std::env::temp_dir().join(format!(
            "runkernel-cache-test-{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("input.txt");
        std::fs::write(&file, "one").unwrap();

        let manager = CacheManager::new();
        let pattern = file.to_string_lossy().to_string();
        let task = Task::new("file").inputs(&[&pattern]);
        let first = match manager.compute_hash("pipeline", &task).unwrap() {
            CacheEligibility::Enabled { hash, .. } => hash,
            CacheEligibility::Disabled(_) => panic!("task should be cacheable"),
        };

        std::fs::write(&file, "two").unwrap();
        let second = match manager.compute_hash("pipeline", &task).unwrap() {
            CacheEligibility::Enabled { hash, .. } => hash,
            CacheEligibility::Disabled(_) => panic!("task should be cacheable"),
        };

        assert_ne!(first, second);
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn test_clean_all_removes_cache_root() {
        let root = temp_cache_root("clean-all");
        std::fs::create_dir_all(root.join("namespace")).unwrap();
        std::fs::write(root.join("namespace").join("task.json"), "{}").unwrap();

        let manager = CacheManager::with_cache_root(&root);
        let result = manager.clean_all().unwrap();

        assert_eq!(result.path, root);
        assert!(result.removed);
        assert!(!result.path.exists());
    }

    #[test]
    fn test_clean_pipeline_removes_only_pipeline_namespace() {
        let root = temp_cache_root("clean-pipeline");
        let manager = CacheManager::with_cache_root(&root);
        let first = manager.pipeline_cache_dir("first");
        let second = manager.pipeline_cache_dir("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();

        let result = manager.clean_pipeline("first").unwrap();

        assert_eq!(result.path, first);
        assert!(result.removed);
        assert!(!result.path.exists());
        assert!(second.exists());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn test_clean_missing_cache_succeeds() {
        let root = temp_cache_root("clean-missing");
        let manager = CacheManager::with_cache_root(&root);

        let result = manager.clean_all().unwrap();

        assert_eq!(result.path, root);
        assert!(!result.removed);
    }
}
