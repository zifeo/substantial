//! Schedules: [`Cron`], the fire times of a cron expression in a time zone, and
//! [`Context::schedule`], the loop that fires them, configured by the `schedule` message
//! `Substantial::schedule` sends.
//!
//! The cron arithmetic is the library's own, the same in every host: a local time a clock change
//! skips fires at the end of the gap, and a local time it repeats fires at its first occurrence.

use std::future::Future;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{
    DateTime, Datelike, NaiveDate, NaiveDateTime, Offset, TimeDelta, TimeZone, Timelike, Utc,
};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::context::Context;
use crate::error::{Error, Result};

/// The message that carries a schedule's config.
pub(crate) const MESSAGE: &str = "schedule";
/// The inline step `Context::schedule` records its fires in.
pub(crate) const RESERVED_STEP: &str = "schedule.next";
/// How many fired instants the published `schedule` value keeps.
// § ey8zhz0kq.d@vmfe3
const RECENT: usize = 10;
// § end ey8zhz0kq.d

const MINUTE: i64 = 60_000;
const DAY: i64 = 86_400_000;
/// A cron expression none of whose fires comes within this many years never fires.
const YEARS: i32 = 8;

const MONTHS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];
const DAYS: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

struct Field {
    name: &'static str,
    lo: u64,
    hi: u64,
    /// The names of its values, the first being `first`.
    names: &'static [&'static str],
    first: u64,
}

const FIELDS: [Field; 5] = [
    Field {
        name: "minute",
        lo: 0,
        hi: 59,
        names: &[],
        first: 0,
    },
    Field {
        name: "hour",
        lo: 0,
        hi: 23,
        names: &[],
        first: 0,
    },
    Field {
        name: "day of month",
        lo: 1,
        hi: 31,
        names: &[],
        first: 0,
    },
    Field {
        name: "month",
        lo: 1,
        hi: 12,
        names: &MONTHS,
        first: 1,
    },
    Field {
        name: "day of week",
        lo: 0,
        hi: 7,
        names: &DAYS,
        first: 0,
    },
];

fn is_number(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit())
}

/// A number of digits; one past `u64` is out of every field's range anyway.
fn number(text: &str) -> u64 {
    text.parse().unwrap_or(u64::MAX)
}

fn value(expr: &str, field: &Field, text: &str) -> Result<u64> {
    if is_number(text) {
        return Ok(number(text));
    }
    let lower = text.to_lowercase();
    match field.names.iter().position(|n| *n == lower) {
        Some(i) => Ok(field.first + i as u64),
        None => Err(Error::InvalidArgument(format!(
            "cron {expr:?}: {text:?} is not a {}",
            field.name
        ))),
    }
}

/// The values of one field, as a bit set.
fn field(expr: &str, text: &str, field: &Field) -> Result<u64> {
    let mut values = 0u64;
    for item in text.split(',') {
        let (span, step) = match item.split_once('/') {
            Some((span, step_text)) => {
                if !is_number(step_text) || number(step_text) == 0 {
                    return Err(Error::InvalidArgument(format!(
                        "cron {expr:?}: the step {step_text:?} is not positive"
                    )));
                }
                (span, Some(number(step_text)))
            }
            None => (item, None),
        };
        let (a, b) = if span == "*" {
            (field.lo, field.hi)
        } else if let Some((first, last)) = span.split_once('-') {
            (value(expr, field, first)?, value(expr, field, last)?)
        } else {
            let a = value(expr, field, span)?;
            (a, if step.is_some() { field.hi } else { a })
        };
        if !(field.lo <= a && a <= b && b <= field.hi) {
            return Err(Error::InvalidArgument(format!(
                "cron {expr:?}: {item:?} is not a {} range of {}-{}",
                field.name, field.lo, field.hi
            )));
        }
        let step = usize::try_from(step.unwrap_or(1)).unwrap_or(usize::MAX);
        for v in (a..=b).step_by(step) {
            values |= 1 << v;
        }
    }
    Ok(values)
}

fn zone(tz: &str) -> Result<Tz> {
    tz.parse().map_err(|_| {
        Error::InvalidArgument(format!("{tz:?} is not a time zone of the IANA database"))
    })
}

/// A cron expression of five fields (minute, hour, day of month, month, day of week) and the IANA
/// time zone it reads its times in: `Cron::new("0 3 * * *", "Europe/Zurich")?`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cron {
    expr: String,
    tz: String,
}

impl Cron {
    /// `InvalidArgument` for an expression that does not parse or never fires, or a zone outside
    /// the IANA database.
    pub fn new(expr: &str, tz: &str) -> Result<Self> {
        compile(expr, tz)?;
        Ok(Self {
            expr: expr.to_owned(),
            tz: tz.to_owned(),
        })
    }

    pub fn expr(&self) -> &str {
        &self.expr
    }

    pub fn tz(&self) -> &str {
        &self.tz
    }
}

struct Compiled {
    minutes: u64,
    hours: u64,
    days: u64,
    months: u64,
    weekdays: u64,
    /// Vixie cron: when either day field starts with `*`, a day matches both, otherwise either.
    either: bool,
    zone: Tz,
}

fn has(set: u64, v: u32) -> bool {
    set & (1 << v) != 0
}

impl Compiled {
    fn day(&self, w: &NaiveDateTime) -> bool {
        // § d08sxcdv8.a@s5hfx
        let dom = has(self.days, w.day());
        let dow = has(self.weekdays, w.weekday().num_days_from_sunday());
        if self.either { dom || dow } else { dom && dow }
    }
}

fn compile(expr: &str, tz: &str) -> Result<Compiled> {
    let fields: Vec<&str> = expr.split_whitespace().collect();
    if fields.len() != 5 {
        return Err(Error::InvalidArgument(format!(
            "cron {expr:?} has {} fields, not 5: minute hour day-of-month month day-of-week",
            fields.len()
        )));
    }
    let mut sets = [0u64; 5];
    for (set, (text, spec)) in sets.iter_mut().zip(fields.iter().zip(&FIELDS)) {
        *set = field(expr, text, spec)?;
    }
    // 7 is Sunday too.
    let weekdays = (sets[4] & 0x7f) | (sets[4] >> 7 & 1);
    // § d08sxcdv8.a@4abpf
    let either = !fields[2].starts_with('*') && !fields[4].starts_with('*');
    // § end d08sxcdv8.a
    let c = Compiled {
        minutes: sets[0],
        hours: sets[1],
        days: sets[2],
        months: sets[3],
        weekdays,
        either,
        zone: zone(tz)?,
    };
    let start = NaiveDate::from_ymd_opt(2000, 1, 1).expect("a date");
    next_wall(&c, start.and_time(Default::default()), expr)?;
    Ok(c)
}

fn midnight(d: NaiveDate) -> NaiveDateTime {
    d.and_time(Default::default())
}

/// The first local time at or after `w`, on the minute, that matches.
fn next_wall(c: &Compiled, mut w: NaiveDateTime, expr: &str) -> Result<NaiveDateTime> {
    let limit = w.year() + YEARS;
    while w.year() <= limit {
        if !has(c.months, w.month()) {
            let (y, m) = (w.year() + (w.month() / 12) as i32, w.month() % 12 + 1);
            w = midnight(NaiveDate::from_ymd_opt(y, m, 1).expect("a first of the month"));
        } else if !c.day(&w) {
            w = midnight(w.date()) + TimeDelta::days(1);
        } else if !has(c.hours, w.hour()) {
            let hour = w.date().and_hms_opt(w.hour(), 0, 0).expect("an hour");
            w = hour + TimeDelta::hours(1);
        } else if !has(c.minutes, w.minute()) {
            let minute = w.with_second(0).and_then(|w| w.with_nanosecond(0));
            w = minute.expect("a minute") + TimeDelta::minutes(1);
        } else {
            return Ok(w);
        }
    }
    Err(Error::InvalidArgument(format!("cron {expr:?} never fires")))
}

fn instant(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).expect("a time in chrono's range")
}

/// The offset of `zone` at the instant `t`, in ms.
fn offset(zone: Tz, t: i64) -> i64 {
    let at = instant(t).naive_utc();
    i64::from(zone.offset_from_utc_datetime(&at).fix().local_minus_utc()) * 1000
}

fn wall_ms(w: NaiveDateTime) -> i64 {
    w.and_utc().timestamp_millis()
}

/// The instant of the local time `w` (ms, read as UTC): the earlier of two in a repeated hour,
/// the end of the gap for a time a clock change skips.
fn resolve(zone: Tz, w: i64) -> i64 {
    // § d08sxcdv8@p0gmq
    let (before, after) = (offset(zone, w - DAY), offset(zone, w + DAY));
    let offsets = if before == after {
        vec![before]
    } else {
        vec![before, after]
    };
    let valid = offsets.into_iter().filter(|o| offset(zone, w - o) == *o);
    if let Some(t) = valid.map(|o| w - o).min() {
        return t;
    }
    let (mut lo, mut hi) = (w - before.max(after), w - before.min(after));
    let first = offset(zone, lo);
    while hi - lo > 1 {
        let mid = (lo + hi).div_euclid(2);
        if offset(zone, mid) == first {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    hi
}

/// The first fire time of `cron` strictly after `after`, both in ms since the epoch.
pub fn next_fire(cron: &Cron, after: i64) -> Result<i64> {
    let c = compile(&cron.expr, &cron.tz)?;
    next(&c, &cron.expr, after)
}

fn next(c: &Compiled, expr: &str, after: i64) -> Result<i64> {
    // § d08sxcdv8@4r3zf
    // No local time before the earliest reading of `after` can resolve after it.
    let least = [-DAY, 0, DAY]
        .into_iter()
        .map(|d| offset(c.zone, after + d))
        .min()
        .expect("three offsets");
    let mut w = instant((after + least).div_euclid(MINUTE) * MINUTE).naive_utc();
    loop {
        w = next_wall(c, w, expr)?;
        let t = resolve(c.zone, wall_ms(w));
        if t > after {
            return Ok(t);
        }
        w += TimeDelta::minutes(1);
    }
}

/// The form of instants in a schedule's values: `2026-11-15T03:00:00.000Z`.
pub fn iso(ms: i64) -> String {
    instant(ms).format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

pub fn parse(text: &str) -> Result<i64> {
    DateTime::parse_from_rfc3339(text)
        .map(|t| t.timestamp_millis())
        .map_err(|e| Error::Protocol(format!("a schedule instant {text:?}: {e}")))
}

/// The fires a plan skipped: how many, the first and the last.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Skipped {
    pub count: u64,
    pub oldest: Option<String>,
    pub newest: Option<String>,
}

/// The fires due by a time, those skipped before them, and the next fire after it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    pub due: Vec<String>,
    pub skipped: Skipped,
    pub next: String,
}

/// The fires due by `now` after `after`, at most the `backfill` most recent, those before them
/// skipped, and the next fire after `now`.
pub fn plan(expr: &str, tz: &str, after: &str, now: &str, backfill: u64) -> Result<Plan> {
    // § vnykycs34.d@t9qnn
    let c = compile(expr, tz)?;
    let (mut t, end) = (parse(after)?, parse(now)?);
    let mut due: std::collections::VecDeque<i64> = Default::default();
    let (mut count, mut oldest, mut newest) = (0, None, None);
    loop {
        t = next(&c, expr, t)?;
        if t > end {
            break;
        }
        due.push_back(t);
        if due.len() as u64 > backfill {
            let skipped = due.pop_front();
            newest = skipped;
            oldest = oldest.or(skipped);
            count += 1;
        }
    }
    Ok(Plan {
        due: due.into_iter().map(iso).collect(),
        skipped: Skipped {
            count,
            oldest: oldest.map(iso),
            newest: newest.map(iso),
        },
        next: iso(t),
    })
}

/// The value of a `schedule` message.
pub(crate) fn config(revision: u64, cron: Option<&Cron>, backfill: u64) -> Value {
    json!({
        "revision": revision,
        "cron": cron.map(Cron::expr),
        "tz": cron.map(Cron::tz),
        "backfill": backfill,
    })
}

/// The id of a config's `schedule` message: its revision and a digest of its canonical JSON, so
/// that a resend of it is delivered once and another config of the revision reaches the run.
pub(crate) fn message_id(wanted: &Value) -> Result<String> {
    let json = serde_jcs::to_vec(wanted)?;
    let digest = Sha256::digest(&json);
    Ok(format!(
        "schedule-{}-{}",
        wanted["revision"],
        hex::encode(&digest[..8])
    ))
}

fn flat(
    revision: u64,
    cron: &Option<String>,
    tz: &Option<String>,
    backfill: Option<u64>,
) -> String {
    json!([revision, cron, tz, backfill]).to_string()
}

pub(crate) fn check_config(revision: u64, backfill: u64) -> Result<()> {
    if revision < 1 {
        return Err(Error::InvalidArgument(format!(
            "a schedule's revision is a positive integer, not {revision}"
        )));
    }
    if backfill < 1 {
        return Err(Error::InvalidArgument(format!(
            "backfill is a positive integer, not {backfill}"
        )));
    }
    Ok(())
}

/// The loop's state, the same JSON in every host.
#[derive(Serialize, Deserialize)]
struct State {
    revision: u64,
    cron: Option<String>,
    tz: Option<String>,
    backfill: Option<u64>,
    after: Option<String>,
    next: Option<String>,
    pending: Vec<String>,
    last: Option<String>,
    recent: Vec<String>,
    skipped: Skipped,
}

fn initial() -> State {
    State {
        revision: 0,
        cron: None,
        tz: None,
        backfill: None,
        after: None,
        next: None,
        pending: Vec::new(),
        last: None,
        recent: Vec::new(),
        skipped: Skipped::default(),
    }
}

fn published(st: &State) -> Value {
    // § ey8zhz0kq.d@pph2d
    json!({
        "revision": st.revision,
        "cron": st.cron,
        "tz": st.tz,
        "backfill": st.backfill,
        "last": st.last,
        "recent": st.recent,
        "skipped": st.skipped,
        "next": st.pending.first().or(st.next.as_ref()),
    })
}

fn received(value: &Value) -> Result<(u64, Option<Cron>, u64)> {
    // § ey8zhz0kq.c@3jf60
    let refused = || {
        Error::InvalidArgument(format!(
            "a schedule message is a config of sub.schedule, not {value}"
        ))
    };
    let object = value.as_object().ok_or_else(refused)?;
    let keys = ["backfill", "cron", "revision", "tz"];
    if object.len() != keys.len() || !keys.iter().all(|k| object.contains_key(*k)) {
        return Err(refused());
    }
    let text = |v: &Value| {
        v.as_str().map(str::to_owned).ok_or_else(|| {
            Error::InvalidArgument(format!(
                "a cron takes an expression and a time zone, strings: {value}"
            ))
        })
    };
    let cron = match &object["cron"] {
        Value::Null => None,
        expr => Some(Cron::new(&text(expr)?, &text(&object["tz"])?)?),
    };
    if cron.is_none() && !object["tz"].is_null() {
        return Err(Error::InvalidArgument(format!(
            "a schedule message without cron has no tz: {value}"
        )));
    }
    let integer = |what: &str, v: &Value| {
        v.as_u64()
            .ok_or_else(|| Error::InvalidArgument(format!("{what} is a positive integer, not {v}")))
    };
    let revision = integer("a schedule's revision", &object["revision"])?;
    let backfill = integer("backfill", &object["backfill"])?;
    check_config(revision, backfill)?;
    Ok((revision, cron, backfill))
}

type Configs = Arc<Mutex<Vec<Value>>>;

fn lock(configs: &Configs) -> std::sync::MutexGuard<'_, Vec<Value>> {
    configs.lock().expect("never panics while locked")
}

impl Context {
    /// Calls `body` at each fire time of the schedule `Substantial::schedule` configures, the
    /// workflow's one loop, until it breaks; fires missed while no worker ran come oldest first,
    /// at most `backfill` of them. As in `iterate`, the error it returns ends the run (a
    /// continuation at a boundary past the threshold), so propagate it.
    pub async fn schedule<F, Fut>(&self, mut body: F) -> Result<()>
    where
        F: FnMut(Context, DateTime<Utc>) -> Fut,
        Fut: Future<Output = Result<ControlFlow<()>>>,
    {
        let configs = Configs::default();
        let pushed = configs.clone();
        self.handle(MESSAGE, move |v: Value| lock(&pushed).push(v))?;
        let mut st = self.iteration(initial())?;
        loop {
            while st.pending.is_empty() {
                if !lock(&configs).is_empty() {
                    self.apply(&mut st, &configs).await?;
                    continue;
                }
                let any = |configs: &Configs| {
                    let c = configs.clone();
                    move || !lock(&c).is_empty()
                };
                let Some(cron) = st.cron.clone() else {
                    // § vnykycs34.c@sddg6
                    self.wait_until(any(&configs)).await?;
                    continue;
                };
                let (Some(tz), Some(after), Some(backfill)) =
                    (st.tz.clone(), st.after.clone(), st.backfill)
                else {
                    return Err(Error::Protocol(
                        "a schedule state with a cron and without its tz, after or backfill".into(),
                    ));
                };
                // § vnykycs34.a@9177j
                let now = self.now().await?.timestamp_millis();
                let fires: Plan = self
                    .step(RESERVED_STEP, move || {
                        let (cron, tz, after) = (cron.clone(), tz.clone(), after.clone());
                        async move { plan(&cron, &tz, &after, &iso(now), backfill) }
                    })
                    .reserved()
                    .await?;
                // § end vnykycs34.a
                if let Some(last) = fires.due.last() {
                    st.after = Some(last.clone());
                    st.pending = fires.due.clone();
                }
                let skipped = fires.skipped;
                if skipped.count > 0 {
                    st.skipped = Skipped {
                        count: st.skipped.count + skipped.count,
                        oldest: st.skipped.oldest.take().or(skipped.oldest),
                        newest: skipped.newest,
                    };
                }
                let next = parse(&fires.next)?;
                let changed = st.next.as_ref() != Some(&fires.next);
                st.next = Some(fires.next);
                if st.pending.is_empty() {
                    // § ey8zhz0kq.d@hrkbh
                    if changed {
                        self.publish(MESSAGE, published(&st))?;
                    }
                    let wait = u64::try_from(next - now).map_err(|_| {
                        Error::Protocol(format!("a next fire at {next} ms, before now, {now} ms"))
                    })?;
                    let wait = Duration::from_millis(wait);
                    self.wait_until(any(&configs)).timeout(wait).await?;
                }
            }
            // § vnykycs34@g831j
            let at = st.pending.remove(0);
            st.last = Some(at.clone());
            // § ey8zhz0kq.d@8z2qf
            st.recent.push(at.clone());
            let over = st.recent.len().saturating_sub(RECENT);
            st.recent.drain(..over);
            self.publish(MESSAGE, published(&st))?;
            // § end ey8zhz0kq.d
            if body(self.clone(), instant(parse(&at)?)).await?.is_break() {
                return Ok(());
            }
            // Before the loop's boundary: only its state crosses a compression.
            if !lock(&configs).is_empty() {
                self.apply(&mut st, &configs).await?;
            }
            self.boundary(&st)?;
        }
    }

    /// Applies the config of the highest revision received, if higher than the state's.
    async fn apply(&self, st: &mut State, configs: &Configs) -> Result<()> {
        let values = std::mem::take(&mut *lock(configs));
        let all = values.iter().map(received).collect::<Result<Vec<_>>>()?;
        let mut highest: Option<&(u64, Option<Cron>, u64)> = None;
        // § ey8zhz0kq.e@1w66k
        for r in &all {
            if highest.is_none_or(|h| r.0 > h.0) {
                highest = Some(r);
            }
        }
        // § end ey8zhz0kq.e
        // § ey8zhz0kq.f@x25nt
        let mut known = std::collections::HashMap::new();
        if st.revision > 0 {
            known.insert(
                st.revision,
                flat(st.revision, &st.cron, &st.tz, st.backfill),
            );
        }
        for (r, cron, backfill) in &all {
            let text = flat(
                *r,
                &cron.as_ref().map(|c| c.expr.clone()),
                &cron.as_ref().map(|c| c.tz.clone()),
                Some(*backfill),
            );
            if let Some(seen) = known.get(r)
                && *seen != text
            {
                return Err(Error::ConfigError(format!(
                    "schedule revision {r} came with two configs: {seen} and {text}"
                )));
            }
            known.insert(*r, text);
        }
        // § end ey8zhz0kq.f
        let Some((revision, cron, backfill)) = highest.cloned() else {
            return Ok(());
        };
        // § ey8zhz0kq@n8sn0
        if revision <= st.revision {
            return Ok(());
        }
        // § end ey8zhz0kq
        let expr = cron.as_ref().map(|c| c.expr.clone());
        let tz = cron.as_ref().map(|c| c.tz.clone());
        st.revision = revision;
        st.backfill = Some(backfill);
        if (&expr, &tz) != (&st.cron, &st.tz) {
            // § vnykycs34.c@ktc77
            // Another cron starts from now: it fires nothing it would have fired before.
            (st.cron, st.tz, st.next) = (expr, tz, None);
            st.pending.clear();
            st.after = match cron {
                Some(_) => Some(iso(self.now().await?.timestamp_millis())),
                None => None,
            };
            if st.cron.is_some() {
                // The plan that follows publishes it, with its next fire.
                return Ok(());
            }
        }
        self.publish(MESSAGE, published(st))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_ids_are_the_same_in_every_host() {
        let hourly = Cron::new("0 * * * *", "UTC").unwrap();
        let id = |wanted| message_id(&wanted).unwrap();
        assert_eq!(
            id(config(1, Some(&hourly), 5)),
            "schedule-1-0a984120fb588a41"
        );
        assert_eq!(id(config(2, None, 1)), "schedule-2-1669c891556cdd13");
    }
}
