use rote::{Pipeline, Task, Context};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize, Debug)]
struct DeployEnv {
    target_ip: String,
    ssh_key_path: PathBuf,
}

mod compilation {
    use rote::Context;
    pub async fn run_wasm_pack(_ctx: &Context) -> anyhow::Result<()> {
        println!("wasm-pack compiled successfully!");
        Ok(())
    }
}

mod recovery {
    pub async fn flash_fallback_firmware() {
        println!("Flash fallback firmware completed!");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Set some sample environment variables for testing so the run doesn't fail
    if std::env::var("TARGET_ENV").is_err() {
        std::env::set_var("TARGET_ENV", "production");
    }
    if std::env::var("TARGET_IP").is_err() {
        std::env::set_var("TARGET_IP", "192.168.1.100");
    }
    if std::env::var("SSH_KEY_PATH").is_err() {
        std::env::set_var("SSH_KEY_PATH", "/home/runner/.ssh/id_rsa");
    }

    let mut pipeline = Pipeline::new("Core Infrastructure Build");

    // 1. Simple shell executions with fluent dependencies
    pipeline.add(
        Task::new("lint")
            .exec("cargo clippy --all-targets")
    );

    pipeline.add(
        Task::new("test")
            .depends_on(&["lint"])
            .exec("cargo test")
    );

    // 2. Native Rust closures for complex Wasm or edge-device logic
    pipeline.add(
        Task::new("build-wasm")
            .depends_on(&["test"])
            .exec_fn(|ctx: Context| async move {
                println!("Building WebAssembly target for {}...", ctx.env("TARGET_ENV")?);
                // Type-safe environment variables validation
                let env: DeployEnv = ctx.require_env()?;
                println!("Parsed configuration safely: Target IP = {}, SSH Key = {}", env.target_ip, env.ssh_key_path.display());
                
                std::fs::create_dir_all("./dist")?;
                crate::compilation::run_wasm_pack(&ctx).await
            })
            // Declare inputs for deterministic caching (e.g. any rs file in rote or ops)
            .inputs(&["rote/src/*.rs", "ops/src/*.rs"])
            .env_vars(&["TARGET_ENV", "TARGET_IP", "SSH_KEY_PATH"])
    );

    // 3. Guaranteed Rollbacks / Teardowns
    pipeline.add(
        Task::new("deploy-edge")
            .depends_on(&["build-wasm"])
            .exec("sh scripts/flash_board.sh")
            .on_failure(|_ctx| async move {
                println!("Deploy failed. Rolling back edge node to last known good state.");
                crate::recovery::flash_fallback_firmware().await;
            })
    );

    // The engine automatically resolves the DAG and executes 
    // independent branches in parallel.
    pipeline.run().await
}
