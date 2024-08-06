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
pub fn substantial(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Context>()?;
    Ok(())
}

#[pyclass]
pub struct Context;

#[pymethods]
impl Context {
    fn save(&self, py: Python, lambda: &PyAny) -> PyResult<Py<PyAny>> {
        // save(lambda: coro(..) | value)
        if !lambda.is_callable() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "{:?} is not callable",
                lambda
            )));
        }

        // TODO: better way to increase ref count
        // Object is owned by py, which is (impossible?) to do on 0.22.0
        let coro_or_val = lambda.call0()?.to_object(py);

        // let inspect = py.import("inspect")?;
        // let out: bool = inspect
        //     .call_method1("iscoroutinefunction", (coro_or_val.as_ref(py),))?
        //     .extract()?;
        // println!("out {out:?}");

        Ok(coro_or_val)
    }

    fn receive(&self, py: Python, event_name: String) -> PyResult<Py<PyAny>> {
        let foo = PyString::new(py, &event_name);
        Ok(foo.to_object(py))
    }

    fn func(&self, py: Python, fn_name: String, args: &PyDict) -> PyResult<Py<PyAny>> {
        let foo = PyString::new(py, &format!("{fn_name} {args:?}"));
        Ok(foo.to_object(py))
    }
}
