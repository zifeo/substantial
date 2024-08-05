use std::path::Path;

use serde_json::Value;

pub mod python;

pub trait WorkflowRunner {
    async fn run_workflow(
        &mut self,
        workflow_file: &Path,
        name: &'static str,
    ) -> anyhow::Result<Value>;
    async fn init(&mut self) -> anyhow::Result<()>;
    async fn deinit(&mut self) -> anyhow::Result<()>;
}
