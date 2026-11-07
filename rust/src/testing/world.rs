//! What an environment and its clients share: the namespace, the virtual time, the workers, the
//! holds, the seed and what Chaos injected.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use chrono::NaiveDate;
use prost::Message;

use super::hold::HoldState;
use super::store::{Chaos, Faults};
use super::time::{State, Time};
use super::workers::{EnvWorker, Incarnation};
use super::{derive, unit};
use crate::error::{Error, Result};
use crate::plan::Ulids;
use crate::proto::layout::v1::{self as layout, Status};
use crate::store::MemoryStore;
use crate::time::millis;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().expect("never panics while locked")
}

/// The time of a due entry, `<yyyymmdd>t<hhmmssmmm>z~<run>~<tag>`.
pub(super) fn due_ms(entry: &str) -> i64 {
    let t = entry.split('~').next().unwrap_or_default();
    let n = |range: std::ops::Range<usize>| -> u32 {
        t.get(range)
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| panic!("a due entry {entry:?}"))
    };
    NaiveDate::from_ymd_opt(n(0..4) as i32, n(4..6), n(6..8))
        .and_then(|d| d.and_hms_milli_opt(n(9..11), n(11..13), n(13..15), n(15..18)))
        .unwrap_or_else(|| panic!("a due entry {entry:?}"))
        .and_utc()
        .timestamp_millis()
}

pub(crate) struct World {
    pub(super) ns: String,
    pub(super) memory: MemoryStore,
    pub(super) time: Arc<Time>,
    chaos: Option<Chaos>,
    pub(super) seed: u64,
    /// One core seed for every worker: a run's random then depends on the run alone.
    pub(super) core_seed: [u8; 32],
    pub(super) ulids: Ulids,
    faults: Mutex<Faults>,
    pub(super) writes: AtomicU64,
    pub(super) outage_until: AtomicI64,
    pub(super) partitions: u32,
    pub(super) workers: Mutex<Vec<Arc<EnvWorker>>>,
    pub(super) holds: Mutex<Vec<Arc<HoldState>>>,
    errors: Mutex<Vec<Error>>,
    /// The chains whose failure the test read through the environment.
    pub(super) checked: Mutex<HashSet<String>>,
    /// Each run that ended failed, its chain and its failure, from the write of its result.
    pub(super) failed: Mutex<BTreeMap<String, (String, String)>>,
    /// The earliest due entry of each workflow, as of a count of writes.
    due: Mutex<Option<(u64, HashMap<String, i64>)>>,
    /// Off only in the test that compares a scenario with and without skipping.
    pub(super) skipping: AtomicBool,
    pub(super) skipped: AtomicU64,
}

impl World {
    pub(super) fn new(ns: String, ms: i64, chaos: Option<Chaos>) -> Result<Self> {
        let seed = chaos.map_or(0, |c| c.seed);
        Ok(Self {
            memory: MemoryStore::open(&ns)?,
            ns,
            time: Arc::new(Time::new(ms)),
            chaos,
            seed,
            core_seed: derive(seed, &[&"core"]),
            ulids: Ulids::seeded(seed),
            faults: Mutex::default(),
            writes: AtomicU64::new(0),
            outage_until: AtomicI64::new(ms),
            partitions: 1,
            workers: Mutex::default(),
            holds: Mutex::default(),
            errors: Mutex::default(),
            checked: Mutex::default(),
            failed: Mutex::default(),
            due: Mutex::default(),
            skipping: AtomicBool::new(true),
            skipped: AtomicU64::new(0),
        })
    }

    /// Records a failed end as its result is written: retention may collect it before `check`.
    pub(super) fn written(&self, key: &str, bytes: &[u8]) {
        let Some(run) = key
            .strip_prefix("runs/")
            .and_then(|k| k.strip_suffix("/result"))
            .filter(|run| !run.contains('/'))
        else {
            return;
        };
        let result = layout::Result::decode(bytes).expect("the core writes a result it decodes");
        if result.status() != Status::Failed {
            return;
        }
        let start = self.object(&format!("runs/{run}/start"));
        let start = layout::Start::decode(start.expect("a run ends after its start"))
            .expect("the core writes a start it decodes");
        let error = result.error.unwrap_or_default();
        let failure = format!("{run} ({}: {})", error.r#type, error.message);
        lock(&self.failed).insert(run.to_owned(), (start.chain, failure));
    }

    pub(super) fn worker_id(&self, place: usize, replaced: u32) -> String {
        hex::encode(&derive(self.seed, &[&"worker", &place, &replaced])[..8])
    }

    /// The try number when Chaos faults this try of `kind` on `key` by `inc`.
    pub(super) fn draw(&self, kind: &str, key: &str, inc: &Incarnation) -> Option<u32> {
        let chaos = self.chaos?;
        let n = inc.tries(kind, key, 1);
        (unit(self.seed, &[&kind, &key, &inc.id, &n]) < chaos.rate).then_some(n)
    }

    /// How many of the next `scans` scans of `inc` Chaos lets through, one after the other:
    /// those whose every listing succeeds at its first try.
    fn succeeding(&self, inc: &Incarnation, scans: i64) -> i64 {
        let Some(chaos) = self.chaos else {
            return scans;
        };
        let keys = lock(&inc.scan).keys.clone();
        for i in 0..scans {
            let ahead = u32::try_from(i + 1).expect("fewer scans than tries can count");
            for key in &keys {
                let n = inc.tries("list", key, 0) + ahead;
                if unit(self.seed, &[&"list", key, &inc.id, &n]) < chaos.rate {
                    return i;
                }
            }
        }
        scans
    }

    pub(super) fn faulted(&self, count: impl FnOnce(&mut Faults)) {
        count(&mut lock(&self.faults));
    }

    pub(super) fn faults(&self) -> Faults {
        *lock(&self.faults)
    }

    /// The hold that takes the next attempt of `step`, if any.
    pub(super) fn hold_of(&self, step: &str) -> Option<Arc<HoldState>> {
        let holds = lock(&self.holds);
        let hold = holds
            .iter()
            .find(|h| h.step == step && *lock(&h.left) > 0)?;
        *lock(&hold.left) -= 1;
        Some(hold.clone())
    }

    pub(super) fn died(&self, error: &Error) {
        lock(&self.errors).push(error.again());
    }

    fn raise_died(&self) -> Result<()> {
        let mut errors = lock(&self.errors);
        match errors.is_empty() {
            true => Ok(()),
            false => Err(errors.remove(0)),
        }
    }

    pub(super) async fn settle(&self) -> Result<()> {
        self.time.settle().await?;
        self.raise_died()
    }

    pub(super) async fn advance(&self, ms: i64) -> Result<()> {
        self.time
            .advance(ms, |st, target| self.skip_scans(st, target))
            .await?;
        self.raise_died()
    }

    pub(super) fn keys(&self, prefix: &str) -> Vec<String> {
        self.memory.keys(prefix)
    }

    pub(super) fn object(&self, key: &str) -> Option<Bytes> {
        self.memory.object(key)
    }

    /// Kills every worker as a SIGKILL would.
    pub(super) fn crash_all(&self) {
        for worker in lock(&self.workers).iter() {
            if worker.is_alive() {
                worker.crash();
            }
        }
    }

    /// Drops the workers, which hold the world, and the namespace.
    pub(super) fn drop_all(&self) {
        lock(&self.workers).clear();
        lock(&self.holds).clear();
        MemoryStore::forget(&self.ns);
    }

    /// The earliest due entry of each workflow, as of the last write.
    fn earliest_due(&self) -> HashMap<String, i64> {
        let writes = self.writes.load(Ordering::SeqCst);
        let mut cached = lock(&self.due);
        if let Some((seen, due)) = &*cached
            && *seen == writes
        {
            return due.clone();
        }
        let mut due: HashMap<String, i64> = HashMap::new();
        for key in self.keys("queues/") {
            let workflow = key.split('/').nth(1).unwrap_or_default();
            let at = due_ms(key.rsplit('/').next().unwrap_or_default());
            let earliest = due.entry(workflow.to_owned()).or_insert(at);
            *earliest = (*earliest).min(at);
        }
        *cached = Some((writes, due.clone()));
        due
    }

    /// Moves past the scans that cannot find anything before `target` or the next deadline of
    /// anything else, keeping the instants of the scans left.
    ///
    /// A worker's scan is skipped when the worker is idle, its deadline is the one its last scan
    /// set (`max_idle` after it), nothing was written since that scan, and no due entry it lists
    /// is due by then: the listing would return the keys it returned, none of them due, and set
    /// the deadline that skipping sets.
    ///
    /// Under Chaos, a scan is skipped only when the draw of each of its listings succeeds, and the
    /// skip counts their tries as the scan would: a scan whose draw fails runs, and may back off,
    /// which moves the scans after it. Never in an outage, where every scan fails.
    fn skip_scans(&self, st: &mut State, target: i64) {
        if !self.skipping.load(Ordering::SeqCst) || st.ms < self.outage_until.load(Ordering::SeqCst)
        {
            return;
        }
        let due = self.earliest_due();
        let writes = self.writes.load(Ordering::SeqCst);
        let mut idle: Vec<(u64, i64, Option<i64>)> = Vec::new();
        let mut bound = target;
        for (id, t) in &st.tasks {
            let Some(at) = t.waiter.as_ref().and_then(|w| w.at) else {
                continue;
            };
            if t.owner.paused() {
                continue;
            }
            if let Some(found) = self.idle_scan(st, *id, at, &due, writes) {
                if let Some(first) = found.1 {
                    bound = bound.min(first);
                }
                idle.push((*id, found.0, found.1));
                continue;
            }
            bound = bound.min(at);
        }
        for (id, mi, first) in idle {
            let t = st.tasks.get_mut(&id).expect("an idle task is tracked");
            let w = t.waiter.as_mut().expect("an idle task is parked");
            let at = w.at.expect("an idle task has a deadline");
            let limit = first.map_or(bound, |f| bound.min(f - 1));
            if limit < at {
                continue;
            }
            let scans = self.succeeding(&t.owner, (limit - at) / mi + 1);
            if scans == 0 {
                continue;
            }
            let counted = u32::try_from(scans).expect("fewer scans than tries can count");
            for key in &lock(&t.owner.scan).keys {
                t.owner.tries("list", key, counted);
            }
            self.skipped
                .fetch_add(scans.unsigned_abs(), Ordering::SeqCst);
            let last = at + (scans - 1) * mi;
            lock(&t.owner.scan).at = Some(last);
            w.at = Some(first.map_or(last + mi, |f| (last + mi).min(f)));
        }
    }

    /// For the agent task `id` of an idle worker parked until `at` on the deadline its last scan
    /// set, its `max_idle` and the earliest due entry of the workflows it serves.
    fn idle_scan(
        &self,
        st: &State,
        id: u64,
        at: i64,
        due: &HashMap<String, i64>,
        writes: u64,
    ) -> Option<(i64, Option<i64>)> {
        let inc = &st.tasks[&id].owner;
        let (agent, events) = inc.agent.get()?;
        let worker = inc.worker()?;
        if *agent != id || !events.idle() {
            return None;
        }
        let mi = i64::try_from(millis(worker.settings.max_idle)).ok()?;
        let first = worker
            .served()
            .filter_map(|name| due.get(name))
            .min()
            .copied();
        let (scanned_at, seen) = {
            let scan = lock(&inc.scan);
            (scan.at, scan.seen)
        };
        let alone = (st.tasks.iter()).all(|(other, t)| *other == id || !Arc::ptr_eq(&t.owner, inc));
        let idle = scanned_at.is_some_and(|s| at == s + mi)
            && seen == writes
            && alone
            && first.is_none_or(|f| f > at);
        idle.then_some((mi, first))
    }
}
