use fs::Fs;
use redis::Redis;

use crate::wit::backend::{BakendType, Datetime, Event, Metadata, Records};

mod fs;
mod redis;

pub trait Backend {
    // TODO: should directly use the protoc stuff
    fn read_events(run_id: String) -> Option<Records>;

    // fn write_events(backend: BakendType, run_id: String, content: Records);
    // fn read_all_metadata(backend: BakendType, run_id: String) -> Vec<Metadata>;
    // fn append_metadata(backend: BakendType, run_id: String, schedule: Datetime, content: String);
    // fn add_schedule(
    //     backend: BakendType,
    //     queue: String,
    //     run_id: String,
    //     schedule: Datetime,
    //     content: Option<Event>,
    // );
    // fn read_schedule(
    //     backend: BakendType,
    //     queue: String,
    //     run_id: String,
    //     schedule: Datetime,
    // ) -> Option<Event>;
    // fn close_schedule(backend: BakendType, queue: String, run_id: String, schedule: Datetime);
}

impl crate::wit::backend::Guest for crate::Substantial {
    // TODO: do protoc deserialize here, the actual implementation should directly use protobuff structs
    fn read_events(backend: BakendType, run_id: String) -> Option<Records> {
        match backend {
            BakendType::Fs => Fs::read_events(run_id),
            BakendType::Redis => Redis::read_events(run_id),
        }
    }

    fn write_events(backend: BakendType, run_id: String, content: Records) {
        todo!()
    }

    fn read_all_metadata(backend: BakendType, run_id: String) -> Vec<Metadata> {
        todo!()
    }

    fn append_metadata(backend: BakendType, run_id: String, schedule: Datetime, content: String) {
        todo!()
    }

    fn add_schedule(
        backend: BakendType,
        queue: String,
        run_id: String,
        schedule: Datetime,
        content: Option<Event>,
    ) {
        todo!()
    }

    fn read_schedule(
        backend: BakendType,
        queue: String,
        run_id: String,
        schedule: Datetime,
    ) -> Option<Event> {
        todo!()
    }

    fn close_schedule(backend: BakendType, queue: String, run_id: String, schedule: Datetime) {
        todo!()
    }
}
