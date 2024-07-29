use crate::wit::metatype::substantial::host::{self, eprint};
use std::{thread::sleep, time::Duration};

use crate::utils::RESULTS;

fn resolve_result(id: &str) -> Result<String, String> {
    loop {
        if let Some(value) = RESULTS.with_borrow(|r| r.get(id).cloned()) {
            return value;
        }
        //sleep(Duration::from_secs(1)); // RuntimeError: unreachable
    }
}

pub struct Host;

impl Host {
    pub fn print(message: String) {
        host::print(&message)
    }

    pub fn eprint(message: String) {
        host::eprint(&message)
    }

    pub fn concat(items: Vec<String>) -> String {
        host::print("calling..");
        host::indirect_call("concat", &items);
        host::print("hosted..");

        match resolve_result("concat") {
            Ok(r) => host::print(&r),
            Err(e) => host::eprint(&e),
        }
        "".to_owned()
    }
}
