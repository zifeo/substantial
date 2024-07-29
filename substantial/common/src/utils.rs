use std::{cell::RefCell, collections::HashMap};

use crate::wit::metatype::substantial::host::eprint;

thread_local! {
    pub static RESULTS: RefCell<HashMap<String, Result<String, String>>> = RefCell::new(HashMap::new());
}

impl crate::wit::utils::Guest for crate::Substantial {
    fn host_result(id: String, json_result: String) {
        eprint("host error");
        RESULTS.with_borrow_mut(|r| r.insert(id, Ok(json_result)));
    }

    fn host_error(id: String, json_error: String) {
        eprint("host success");
        RESULTS.with_borrow_mut(|r| r.insert(id, Err(json_error)));
    }
}
