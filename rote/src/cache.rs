use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::SystemTime;
use sha2::{Sha256, Digest};
use glob::glob;
use serde::{Serialize, Deserialize};
use crate::task::{Task, TaskAction};

#[derive(Serialize, Deserialize, Debug)]
pub struct CacheEntry {
    pub task_name: String,
    pub hash: String,
    pub timestamp: u64,
}

pub struct CacheManager {
    cache_dir: PathBuf,
}

impl Default for CacheManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheManager {
    /// Creates a new CacheManager pointing to `.rote/cache`.
    pub fn new() -> Self {
        CacheManager {
            cache_dir: PathBuf::from(".rote").join("cache"),
        }
    }

    /// Checks if a task has a valid cache entry matching the current hash.
    pub fn is_cached(&self, task_name: &str, current_hash: &str) -> bool {
        let cache_file = self.cache_dir.join(format!("{}.json", task_name));
        if !cache_file.exists() {
            return false;
        }

        if let Ok(content) = std::fs::read_to_string(&cache_file) {
            if let Ok(entry) = serde_json::from_str::<CacheEntry>(&content) {
                return entry.hash == current_hash;
            }
        }
        false
    }

    /// Saves a cache entry for a successful task execution.
    pub fn save_cache(&self, task_name: &str, current_hash: &str) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.cache_dir)?;
        let cache_file = self.cache_dir.join(format!("{}.json", task_name));

        let seconds = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = CacheEntry {
            task_name: task_name.to_string(),
            hash: current_hash.to_string(),
            timestamp: seconds,
        };

        let content = serde_json::to_string_pretty(&entry)?;
        std::fs::write(cache_file, content)?;
        Ok(())
    }

    /// Computes the hash for the task's execution config, env variables, and file inputs.
    pub fn compute_hash(&self, task: &Task) -> anyhow::Result<String> {
        let mut hasher = Sha256::new();

        // 1. Hash execution action
        match &task.action {
            Some(TaskAction::Shell(cmd)) => {
                hasher.update(b"shell:");
                hasher.update(cmd.as_bytes());
            }
            Some(TaskAction::Fn(_)) => {
                hasher.update(b"fn:");
            }
            None => {
                hasher.update(b"none:");
            }
        }

        // 2. Hash env vars (sorted for determinism)
        let mut sorted_envs = task.env_vars.clone();
        sorted_envs.sort();
        for key in sorted_envs {
            let val = std::env::var(&key).unwrap_or_default();
            hasher.update(key.as_bytes());
            hasher.update(b"=");
            hasher.update(val.as_bytes());
            hasher.update(b";");
        }

        // 3. Resolve and hash glob file inputs
        let mut files_to_hash = Vec::new();
        for pattern in &task.inputs {
            match glob(pattern) {
                Ok(paths) => {
                    for path in paths.flatten() {
                        if path.is_file() {
                            if should_ignore_path(&path) {
                                continue;
                            }
                            files_to_hash.push(path);
                        }
                    }
                }
                Err(e) => {
                    anyhow::bail!("Invalid glob pattern '{}': {}", pattern, e);
                }
            }
        }

        // Sort paths for deterministic hashing order
        files_to_hash.sort();

        for file_path in files_to_hash {
            let mut file = match File::open(&file_path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    continue;
                }
                Err(e) => {
                    anyhow::bail!("Failed to open input file '{}' for hashing: {}", file_path.display(), e);
                }
            };

            hasher.update(file_path.to_string_lossy().as_bytes());
            hasher.update(b":");
            let mut buffer = [0; 8192];
            loop {
                let count = file.read(&mut buffer).map_err(|e| {
                    anyhow::anyhow!("Failed to read input file '{}' for hashing: {}", file_path.display(), e)
                })?;
                if count == 0 {
                    break;
                }
                hasher.update(&buffer[..count]);
            }
            hasher.update(b"\n");
        }

        let result = hasher.finalize();
        Ok(hex::encode(result))
    }
}

fn should_ignore_path(path: &std::path::Path) -> bool {
    for component in path.components() {
        if let std::path::Component::Normal(name) = component {
            if let Some(name_str) = name.to_str() {
                match name_str {
                    "target" | "node_modules" | ".git" | ".rote" | ".cargo" | ".next" => return true,
                    _ => {}
                }
            }
        }
    }
    false
}

impl Task {
    /// Determines if this task qualifies for caching (must declare inputs or env variables).
    pub fn is_cacheable(&self) -> bool {
        !self.inputs.is_empty() || !self.env_vars.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_should_ignore_path() {
        assert!(should_ignore_path(Path::new("services/drone/drone-ops/target/.rustc_info.json")));
        assert!(should_ignore_path(Path::new("target/debug/deps/foo")));
        assert!(should_ignore_path(Path::new("node_modules/lodash/index.js")));
        assert!(should_ignore_path(Path::new(".git/config")));
        assert!(should_ignore_path(Path::new(".rote/cache/foo.json")));
        assert!(should_ignore_path(Path::new(".cargo/config.toml")));
        assert!(should_ignore_path(Path::new(".next/server/pages/index.html")));

        assert!(!should_ignore_path(Path::new("services/drone/drone-ops/src/main.rs")));
        assert!(!should_ignore_path(Path::new("src/lib.rs")));
        assert!(!should_ignore_path(Path::new("target_file.rs")));
        assert!(!should_ignore_path(Path::new("git_manager.rs")));
    }

    #[test]
    fn test_compute_hash_ignores_missing_file_gracefully() {
        let manager = CacheManager::new();
        // A task with a non-existent file
        let task = Task::new("test-task")
            .inputs(&["this_file_does_not_exist_xyz.txt"]);

        // Since the glob pattern won't find the file, it shouldn't fail
        let hash1 = manager.compute_hash(&task).unwrap();

        // If we have a pattern that matches nothing, it is deterministic
        let hash2 = manager.compute_hash(&task).unwrap();
        assert_eq!(hash1, hash2);
    }
}
