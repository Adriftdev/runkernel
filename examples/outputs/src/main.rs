use runkernel::{Context, Pipeline, Task};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut pipeline = Pipeline::new("outputs-example");
    pipeline.add(Task::new("build").exec_fn(|ctx: Context| async move {
        ctx.set_output("artifact", "dist/app.wasm")?;
        Ok(())
    }));
    pipeline.add(
        Task::new("deploy")
            .depends_on(&["build"])
            .exec_fn(|ctx: Context| async move {
                let artifact: String = ctx.output_from("build", "artifact")?;
                println!("Deploying {artifact}");
                Ok(())
            }),
    );

    let result = pipeline.run().await?;
    println!("{:#?}", result.summary);
    Ok(())
}
