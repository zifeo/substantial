use std::time::Duration;

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

/// Whole milliseconds, as the host counts every duration. One past u64 is past the longest a call
/// carries, so it saturates to a value the host refuses with `InvalidArgument`.
pub(crate) fn whole_millis(d: Duration) -> u64 {
    // § one .:b60bm4szk@h158s
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}
