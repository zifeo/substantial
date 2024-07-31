use serde_json::Value;

use crate::{downcast, host::Host, promise::Promise, wit::utils::PromisedResult};

impl crate::wit::utils::Guest for crate::Substantial {
    fn concat_then_uppercase(items: Vec<String>) -> PromisedResult {
        // poc for how chaining computations work
        // notice how rust (guest) has no idea what Host::concat evaluates to
        Host::concat(items)
            .map(|r: String| r.to_uppercase())
            .map(|r| format!("{r}A"))
            .map(|r| format!("{r}B"))
            .into()
    }

    // should never be used explicitly
    fn evaluate_guest(promised: PromisedResult, first_output: String) -> Result<String, String> {
        let output =
            serde_json::from_str::<serde_json::Value>(&first_output).map_err(|e| e.to_string())?;

        match output {
            Value::Bool(value) => {
                let ret = Promise::resolve(promised.ref_guest, Box::new(value))?;
                serde_json::to_string(&downcast!(ret, bool))
            }
            Value::Number(value) => {
                let ret = Promise::resolve(promised.ref_guest, Box::new(value))?;
                serde_json::to_string(&downcast!(ret, f64))
            }
            Value::String(value) => {
                let ret = Promise::resolve(promised.ref_guest, Box::new(value))?;
                serde_json::to_string(&downcast!(ret, String))
            }
            Value::Array(_) | Value::Null | Value::Object(_) => {
                return Err(format!("TODO: array, null and object not supported yet"))
            }
        }
        .map_err(|e| e.to_string())
    }
}
