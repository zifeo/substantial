use crate::{host::Host, wit::backend::Records};

pub struct Fs;

impl super::Backend for Fs {
    fn read_events(run_id: String) -> Option<crate::wit::backend::Records> {
        // concat is async in the host world
        Host::concat(vec!["Hello".to_owned(), "World".to_owned()]);

        // todo!("fs read events")
        Some(Records {
            repr: "".to_string(),
        })
    }
}
