use std::path::{Path, PathBuf};

use anyhow::{Context, Ok};
use pyo3::prelude::*;
use serde_json::Value;
use serde_pyobject::from_pyobject;

pub mod definitions;

macro_rules! debug_sys {
    ($py: expr) => {
        let sys = $py.import_bound("sys")?;
        let version: String = sys.getattr("version")?.extract()?;
        let path: Vec<String> = sys.getattr("path")?.extract()?;
        println!("Python {version}\n{path:?}\n");
    };
}

pub struct PythonRunner {
    pub workdir: Option<PathBuf>,
}

impl super::WorkflowRunner for PythonRunner {
    async fn run_workflow(
        &mut self,
        workflow_file: &Path,
        name: &'static str,
    ) -> anyhow::Result<Value> {
        std::env::set_var(
            "PYTHONPATH",
            workflow_file
                .canonicalize()
                .with_context(|| format!("Resolving path {}", workflow_file.display()))?
                .parent()
                .with_context(|| format!("Get parent of {}", workflow_file.display()))?,
        );

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            debug_sys!(py);
            // let substantial = PyModule::import_bound(py, "substantial")?;

            let wf_entry_point = workflow_file.file_stem().unwrap().to_str().unwrap();
            let wf_mod = py.import_bound(wf_entry_point)?;

            let context = definitions::Context;
            let output = wf_mod.call_method(name, (context,), None)?;
            from_pyobject(output).map_err(|e| e.into())
        })
    }

    async fn init(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn deinit(&mut self) -> anyhow::Result<()> {
        std::env::remove_var("PYTHONPATH");
        Ok(())
    }
}
