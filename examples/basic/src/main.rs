use runkernel::{Pipeline, Task};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut pipeline = Pipeline::new("basic");
    pipeline.add(Task::new("format").exec("cargo fmt --check"));
    pipeline.add(Task::new("test").depends_on(&["format"]).exec("cargo test"));

    let result = pipeline.run().await?;
    println!("{:#?}", result.summary);
    Ok(())
}
