use chrono::{DateTime, Utc};

use crate::protocol::{
    events::{Event, Records},
    metadata::Metadata,
};

// TODO: document
pub trait Backend {
    async fn read_events(run_id: String) -> Option<Records>;

    async fn write_events(run_id: String, content: Records);

    async fn read_all_metadata(run_id: String) -> Vec<Metadata>;

    async fn append_metadata(run_id: String, schedule: DateTime<Utc>, content: String);

    async fn add_schedule(
        queue: String,
        run_id: String,
        schedule: DateTime<Utc>,
        content: Option<Event>,
    );

    async fn read_schedule(queue: String, run_id: String, schedule: DateTime<Utc>)
        -> Option<Event>;

    async fn close_schedule(queue: String, run_id: String, schedule: DateTime<Utc>);
}
