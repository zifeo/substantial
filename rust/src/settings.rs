//! Worker settings (I-18): a preset names every setting, the only defaults there are.

use std::time::Duration;

use crate::error::{Error, Result};
use crate::time::{days, minutes, seconds};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Preset {
    Script,
    Service,
}

impl std::str::FromStr for Preset {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "script" => Ok(Preset::Script),
            "service" => Ok(Preset::Service),
            _ => Err(Error::ConfigError(format!("unknown preset {s:?}"))),
        }
    }
}

/// Adapter retries of transient failures: `attempts` in all, backoff doubling from `initial` to `max`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreRetry {
    pub attempts: u32,
    pub initial: Duration,
    pub max: Duration,
}

impl StoreRetry {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.attempts == 0 || self.initial.is_zero() || self.max < self.initial {
            return Err(Error::ConfigError(format!("invalid store_retry {self:?}")));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ContinueAsNewAfter {
    pub(crate) segments: u64,
    pub(crate) bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Settings {
    pub(crate) concurrency: u32,
    pub(crate) commit_every: Duration,
    pub(crate) shutdown_grace: Duration,
    pub(crate) claim_ttl: Duration,
    pub(crate) claim_skew: Duration,
    pub(crate) max_idle: Duration,
    pub(crate) wait: Duration,
    pub(crate) due_page: u32,
    pub(crate) due_pages: u32,
    pub(crate) snapshot_every: u64,
    pub(crate) continue_as_new_after: ContinueAsNewAfter,
    pub(crate) dedup_window: Duration,
    pub(crate) step_timeout: Duration,
}

/// The settings a worker takes; the others keep their preset value, set only by the tests.
pub(crate) const PUBLIC: [&str; 6] = [
    "concurrency",
    "max_idle",
    "shutdown_grace",
    "claim_ttl",
    "continue_as_new_after",
    "step_timeout",
];

/// The retries of every store (I-26).
pub const STORE_RETRY: StoreRetry = StoreRetry {
    attempts: 5,
    initial: Duration::from_millis(100),
    max: Duration::from_secs(5),
};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

impl Settings {
    /// Every setting of `preset`; `waiting_store` is whether the store's listings can wait.
    pub(crate) fn preset(preset: Preset, waiting_store: bool) -> Self {
        let service = preset == Preset::Service;
        Self {
            concurrency: if service { 16 } else { 1 },
            commit_every: Duration::ZERO,
            shutdown_grace: seconds(if service { 25 } else { 5 }),
            claim_ttl: seconds(30),
            claim_skew: seconds(5),
            max_idle: match (service, waiting_store) {
                (false, _) => seconds(1),
                (true, false) => seconds(30),
                (true, true) => minutes(5),
            },
            wait: seconds(20),
            due_page: 100,
            due_pages: 10,
            snapshot_every: 64,
            continue_as_new_after: ContinueAsNewAfter {
                segments: 1000,
                bytes: 8 * MIB,
            },
            dedup_window: days(7),
            step_timeout: minutes(5),
        }
    }

    /// Overrides one setting by name; an unknown name or a value that does not parse is refused.
    ///
    /// Durations take a unit (`100ms`, `5s`, `10min`, `1h`, `7d`), sizes too (`64KiB`, `8MiB`);
    /// `continue_as_new_after` is `segments,size`, or `0` to compress at every boundary.
    pub(crate) fn set(&mut self, name: &str, value: &str) -> Result<()> {
        let mut next = self.clone();
        next.apply(name, value)?;
        next.validate()?;
        *self = next;
        Ok(())
    }

    fn apply(&mut self, name: &str, value: &str) -> Result<()> {
        let invalid = |e: Error| match e {
            Error::ConfigError(message) => Error::ConfigError(format!("setting {name}: {message}")),
            other => other,
        };
        match name {
            "concurrency" => self.concurrency = count(value).map_err(invalid)?,
            "commit_every" => self.commit_every = duration(value).map_err(invalid)?,
            "shutdown_grace" => self.shutdown_grace = duration(value).map_err(invalid)?,
            "claim_ttl" => self.claim_ttl = duration(value).map_err(invalid)?,
            "claim_skew" => self.claim_skew = duration(value).map_err(invalid)?,
            "max_idle" => self.max_idle = duration(value).map_err(invalid)?,
            "wait" => self.wait = duration(value).map_err(invalid)?,
            "due_page" => self.due_page = count(value).map_err(invalid)?,
            "due_pages" => self.due_pages = count(value).map_err(invalid)?,
            "snapshot_every" => self.snapshot_every = count(value).map_err(invalid)?,
            "continue_as_new_after" => {
                self.continue_as_new_after = match value.split_once(',') {
                    None if value.trim() == "0" => ContinueAsNewAfter {
                        segments: 0,
                        bytes: 0,
                    },
                    Some((segments, bytes)) => ContinueAsNewAfter {
                        segments: count(segments).map_err(invalid)?,
                        bytes: size(bytes).map_err(invalid)?,
                    },
                    None => return Err(invalid(parse_error(value, "segments,size"))),
                }
            }
            "dedup_window" => self.dedup_window = duration(value).map_err(invalid)?,
            "step_timeout" => self.step_timeout = duration(value).map_err(invalid)?,
            _ => return Err(unknown(&[name])),
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let invalid = |what: &str| Err(Error::ConfigError(format!("{what} must be positive")));
        if self.concurrency == 0 {
            return invalid("concurrency");
        }
        if self.due_page == 0 || self.due_pages == 0 {
            return invalid("due_page and due_pages");
        }
        if self.due_page > 1000 {
            return Err(Error::ConfigError(
                "due_page must be at most 1000, an S3 page".into(),
            ));
        }
        if self.claim_ttl.is_zero() || self.max_idle.is_zero() {
            return invalid("claim_ttl and max_idle");
        }
        if self.step_timeout.is_zero() {
            return invalid("step_timeout");
        }
        if self.snapshot_every == 0 {
            return invalid("snapshot_every");
        }
        if self.wait.subsec_nanos() != 0 || !(1..=20).contains(&self.wait.as_secs()) {
            return Err(Error::ConfigError(
                "wait must be whole seconds from 1 s to 20 s".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn unknown(names: &[&str]) -> Error {
    Error::ConfigError(format!("unknown settings {names:?}"))
}

fn parse_error(value: &str, expected: &str) -> Error {
    Error::ConfigError(format!("{value:?} is not {expected}"))
}

fn count<T: std::str::FromStr>(value: &str) -> Result<T> {
    value
        .trim()
        .parse()
        .map_err(|_| parse_error(value, "a whole number"))
}

fn split_unit(value: &str) -> (&str, &str) {
    let value = value.trim();
    let at = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    (&value[..at], value[at..].trim())
}

fn duration(value: &str) -> Result<Duration> {
    let (n, unit) = split_unit(value);
    let n: u64 = n.parse().map_err(|_| parse_error(value, "a duration"))?;
    let ms = match unit {
        "ms" => 1,
        "s" => 1000,
        "min" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => {
            return Err(parse_error(
                value,
                "a duration with a unit (ms, s, min, h, d)",
            ));
        }
    };
    Ok(Duration::from_millis(n * ms))
}

fn size(value: &str) -> Result<u64> {
    let (n, unit) = split_unit(value);
    let n: u64 = n.parse().map_err(|_| parse_error(value, "a size"))?;
    let factor = match unit {
        "B" => 1,
        "KiB" => KIB,
        "MiB" => MIB,
        "GiB" => 1024 * MIB,
        _ => return Err(parse_error(value, "a size with a unit (B, KiB, MiB, GiB)")),
    };
    Ok(n * factor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presets_differ_where_the_table_says() {
        let script = Settings::preset(Preset::Script, false);
        let service = Settings::preset(Preset::Service, false);
        assert_eq!((script.concurrency, service.concurrency), (1, 16));
        assert_eq!(script.max_idle, seconds(1));
        assert_eq!(service.max_idle, seconds(30));
        assert_eq!(Settings::preset(Preset::Service, true).max_idle, minutes(5));
        assert_eq!(script.shutdown_grace, seconds(5));
        assert_eq!(service.shutdown_grace, seconds(25));
        assert_eq!(
            (script.step_timeout, service.step_timeout),
            (minutes(5), minutes(5))
        );
    }

    #[test]
    fn set_overrides_and_refuses() {
        let mut s = Settings::preset(Preset::Script, false);
        s.set("concurrency", "4").unwrap();
        s.set("commit_every", "250ms").unwrap();
        s.set("continue_as_new_after", "0").unwrap();
        s.set("continue_as_new_after", "10,1KiB").unwrap();
        assert_eq!(s.concurrency, 4);
        assert_eq!(s.commit_every, Duration::from_millis(250));
        assert_eq!(s.continue_as_new_after.bytes, 1024);
        assert_eq!(
            s.set("concurency", "4").unwrap_err().to_string(),
            r#"config error: unknown settings ["concurency"]"#
        );
        assert!(matches!(
            s.set("commit_every", "250"),
            Err(Error::ConfigError(_))
        ));
        assert!(matches!(
            s.set("concurrency", "0"),
            Err(Error::ConfigError(_))
        ));
        s.set("step_timeout", "30s").unwrap();
        assert_eq!(s.step_timeout, seconds(30));
        assert!(matches!(
            s.set("step_timeout", "0s"),
            Err(Error::ConfigError(_))
        ));
        assert!(matches!(
            "daemon".parse::<Preset>(),
            Err(Error::ConfigError(_))
        ));
    }
}
