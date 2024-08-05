use std::path::PathBuf;

use runtimes::{python::PythonRunner, WorkflowRunner};

mod backends;
mod protocol;
mod runtimes;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut runner = PythonRunner { workdir: None };
    runner.init().await?;

    let file = PathBuf::from("../demos/workflow.py");

    let wf_result = runner.run_workflow(&file, "example").await?;

    println!("Workflow result {wf_result}");

    Ok(())
}
