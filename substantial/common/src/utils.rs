use std::{any::Any, borrow::Borrow, cell::RefCell};

use crate::{
    host::Host,
    promise::{Promise, PENDING},
    wit::utils::PromisedResult,
    Substantial,
};

impl crate::wit::utils::Guest for crate::Substantial {
    fn concat_then_uppercase(items: Vec<String>) -> PromisedResult {
        // poc for how chaining computations work
        Host::concat(items)
            .map(|r| format!("{r}A"))
            .map(|r| format!("{r}B"))
            .map(|r: String| r.to_uppercase())
            .into()
    }

    // should never be used explicitly
    fn evaluate_guest(promised: PromisedResult, first_output: String) -> String {
        // Convert String into Box<dyn Any>

        // TODO: tag type as it must be known at compile time
        // there are only 4 cases since it comes from the host
        // Promise::resolve<f32 | string | bool | null>
        Promise::resolve::<String>(promised.ref_guest, Box::new(first_output));
        todo!()
    }
}
