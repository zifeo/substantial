use crate::{
    promise::Promise,
    wit::metatype::substantial::host::{self},
};

pub struct Host;

impl Host {
    pub fn print(message: String) {
        host::print(&message)
    }

    pub fn eprint(message: String) {
        host::eprint(&message)
    }

    pub fn concat(items: Vec<String>) -> Promise {
        host::concat(&items).into()
    }
}
