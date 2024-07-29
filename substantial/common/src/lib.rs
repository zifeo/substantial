mod backends;
mod host;
mod utils;

pub mod wit {
    wit_bindgen::generate!({ world: "substantial" });
    use crate::Substantial;
    pub use exports::metatype::substantial::{backend, utils};
    export!(Substantial);
}

pub struct Substantial;
