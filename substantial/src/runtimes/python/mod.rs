use std::{
    borrow::Borrow,
    path::{Path, PathBuf},
};

use anyhow::{Context, Ok};
use definitions::*;
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

        pyo3::append_to_inittab!(substantial); // requires auto-initialize off
        pyo3::prepare_freethreaded_python();

        Python::with_gil(|py| {
            debug_sys!(py);

            // setup loop
            let asyncio = py.import_bound("asyncio")?;
            let policy = asyncio.getattr("get_event_loop_policy")?.call0()?;
            let ev_loop = policy.call_method0("new_event_loop")?;
            asyncio.call_method1("set_event_loop", (ev_loop.borrow(),))?;

            // setup module
            let wf_entry_point = workflow_file.file_stem().unwrap().to_str().unwrap();
            let wf_mod = py.import_bound(wf_entry_point)?;

            // call workflow
            let context = definitions::Context;
            let coro = wf_mod.call_method1(name, (context,))?;
            let output = ev_loop.call_method1("run_until_complete", (coro,))?;

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
