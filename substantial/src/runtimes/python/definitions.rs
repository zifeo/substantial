use pyo3::prelude::*;

// submodules issues
// https://github.com/PyO3/pyo3/discussions/3591
// https://github.com/PyO3/pyo3/issues/759

// better async
// https://github.com/PyO3/pyo3/issues/1632

#[pymodule]
pub fn substantial(m: Bound<PyModule>) -> PyResult<()> {
    m.add_class::<Context>()?;
    Ok(())
}

#[pyclass]
pub struct Context;

#[pymethods]
impl Context {
    fn save(&self, lambda: Bound<PyAny>) -> PyResult<i32> /*PyResult<&'_ Bound<'_, PyAny>>*/ {
        // save(lambda: coro(..) | value)
        if !lambda.is_callable() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "{:?} is not callable",
                lambda
            )));
        }
        // FIXME: wrong lifetime, make global? => who should own it?
        // let coro_or_val = lambda.call0()?.as_ref();
        // Ok(coro_or_val)
        // ALT: write Context in python but each method will call a rust function
        Ok(1234)
    }
}
