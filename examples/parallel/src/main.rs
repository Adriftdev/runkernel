use runkernel::{Pipeline, Task};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut pipeline = Pipeline::new("parallel-example");
    pipeline.add(Task::new("lint").exec("cargo clippy --all-targets"));
    pipeline.add(Task::new("unit-test").exec("cargo test"));
    pipeline.add(
        Task::new("package")
            .depends_on(&["lint", "unit-test"])
            .exec("cargo build"),
    );

    let result = pipeline.run().await?;
    println!("{:#?}", result.summary);
    Ok(())
}
