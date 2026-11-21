use std::time::Duration;

pub const fn duration_seconds(n: u64) -> Duration {
    Duration::from_secs(n)
}

pub const fn duration_minutes(n: u64) -> Duration {
    Duration::from_secs(n * 60)
}

pub const fn duration_hours(n: u64) -> Duration {
    Duration::from_secs(n * 3600)
}

pub const fn duration_days(n: u64) -> Duration {
    Duration::from_secs(n * 86_400)
}

/// Whole milliseconds, as the host counts every duration.
pub(crate) fn whole_millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).expect("a duration fits in u64 milliseconds")
}
