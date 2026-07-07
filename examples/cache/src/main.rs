use runkernel::{Pipeline, Task};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut pipeline = Pipeline::new("cache-example");
    pipeline.add(
        Task::new("build")
            .exec("cargo build")
            .inputs(&["crates/runkernel/src/**/*.rs", "Cargo.toml", "Cargo.lock"])
            .env_vars(&["PROFILE"])
            .cache_key("build-v1"),
    );

    let result = pipeline.run().await?;
    println!("{:#?}", result.summary);
    Ok(())
}
