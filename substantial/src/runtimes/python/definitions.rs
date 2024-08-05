use pyo3::prelude::*;

#[pyfunction]
fn double(x: usize) -> usize {
    x * 2
}

#[pymodule(name = "substantial")]
fn substantial(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(double, m)?)?;
    m.add("Context", Context)
}

// Define the Context struct
#[pyclass]
pub struct Context;

#[pymethods]
impl Context {
    #[new]
    fn new() -> Self {
        Context
    }

    fn save(&self, value: i32) -> PyResult<i32> {
        Ok(value + 5)
    }
}
