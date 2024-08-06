use pyo3::{
    prelude::*,
    types::{PyDict, PyString},
};

// submodules issues
// https://github.com/PyO3/pyo3/discussions/3591
// https://github.com/PyO3/pyo3/issues/759

// better async
// https://github.com/PyO3/pyo3/issues/1632

// decorators
// https://github.com/PyO3/pyo3/discussions/3537

#[pymodule]
pub fn substantial(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Context>()?;
    Ok(())
}

#[pyclass]
pub struct Context;

#[pymethods]
impl Context {
    fn save(&self, lambda: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        // save(lambda: coro(..) | value)
        if !lambda.is_callable() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "{:?} is not callable",
                lambda
            )));
        }

        let py = lambda.py();
        let coro_or_val = lambda.call0()?.to_object(py);

        // let inspect = py.import_bound("inspect")?;
        // let out: bool = inspect
        //     .call_method1("iscoroutinefunction", (coro_or_val.clone_ref(py),))?
        //     .extract()?;
        // println!("out {out:?}");

        Ok(coro_or_val)
    }

    fn receive(&self, py: Python, event_name: String) -> PyResult<Py<PyAny>> {
        let foo = PyString::new_bound(py, &event_name);
        Ok(foo.to_object(py))
    }

    fn func(&self, py: Python, fn_name: String, args: Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        let foo = PyString::new_bound(args.py(), &format!("{fn_name} {args:?}"));
        Ok(foo.to_object(py))
    }
}
