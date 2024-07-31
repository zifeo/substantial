use crate::{host::Host, wit::backend::Records};

pub struct Fs;

impl super::Backend for Fs {
    fn read_events(run_id: String) -> Option<crate::wit::backend::Records> {
        Some(Records {
            repr: "".to_string(),
        })
    }
}
