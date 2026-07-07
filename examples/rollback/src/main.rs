use runkernel::{Pipeline, RollbackPolicy, Task};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut pipeline = Pipeline::new("rollback-example")
        .rollback_policy(RollbackPolicy::CompletedTasksReverseOrder);
    pipeline.add(
        Task::new("provision")
            .exec_fn(|_| async move {
                println!("Provisioned resource");
                Ok(())
            })
            .rollback(|_| async move {
                println!("Removed provisioned resource");
                Ok(())
            }),
    );
    pipeline.add(
        Task::new("deploy")
            .depends_on(&["provision"])
            .exec_fn(|_| async move { anyhow::bail!("deployment failed") }),
    );

    let result = pipeline.run().await?;
    println!("{:#?}", result.summary);
    Ok(())
}
