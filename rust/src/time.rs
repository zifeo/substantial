use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::proto::layout::v1::Time;

pub const fn seconds(n: u64) -> Duration {
    Duration::from_secs(n)
}

pub const fn minutes(n: u64) -> Duration {
    Duration::from_secs(n * 60)
}

pub const fn hours(n: u64) -> Duration {
    Duration::from_secs(n * 3600)
}

pub const fn days(n: u64) -> Duration {
    Duration::from_secs(n * 86_400)
}

pub(crate) fn now() -> Time {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before 1970")
        .as_millis();
    Time { ms: ms as i64 }
}

pub(crate) fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).expect("duration fits in u64 milliseconds")
}

/// How far `at` lies ahead of `now`, zero when it has passed.
pub(crate) fn until(at: &Time, now: &Time) -> Duration {
    Duration::from_millis(u64::try_from(at.ms - now.ms).unwrap_or(0))
}
