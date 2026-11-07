//! The environment: a `Substantial` on a namespace in memory and a virtual clock, its clients,
//! and what a test asks of them.

use std::future::IntoFuture;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;

use super::attached::Attached;
use super::hold::{Hold, HoldState};
use super::store::{Chaos, Faults};
use super::world::World;
use crate::client::Substantial;
use crate::error::{Error, Result};
use crate::proto::boundary::v1::{self as b, plan_input::Plan};
use crate::time::millis;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().expect("never panics while locked")
}

fn ms(d: Duration) -> i64 {
    i64::try_from(millis(d)).expect("a duration fits in i64 ms")
}

/// A `Substantial` for tests: a namespace in memory, on a virtual clock, whose workers run in the
/// test's process and move only in `settle` and `advance`. `close` it, or drop it, which crashes
/// its workers without waiting for the step bodies still running.
pub struct Env {
    sub: Substantial,
    world: Arc<World>,
    closed: bool,
}

/// An opening of an environment, awaited to open it; `chaos` faults its workers.
#[must_use = "an environment does nothing until awaited"]
pub struct OpenEnv {
    now: DateTime<Utc>,
    chaos: Option<Chaos>,
}

impl OpenEnv {
    pub fn chaos(mut self, chaos: Chaos) -> Self {
        self.chaos = Some(chaos);
        self
    }
}

impl IntoFuture for OpenEnv {
    type Output = Result<Env>;
    type IntoFuture = BoxFuture<'static, Result<Env>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let ms = self.now.timestamp_millis();
            if ms < 0 {
                return Err(Error::InvalidArgument(format!(
                    "an environment starts after 1970, not at {}",
                    self.now.to_rfc3339()
                )));
            }
            if !self.now.timestamp_subsec_nanos().is_multiple_of(1_000_000) {
                return Err(Error::InvalidArgument(format!(
                    "an environment's clock counts whole milliseconds, not {}",
                    self.now.to_rfc3339()
                )));
            }
            if let Some(c) = self.chaos
                && !(0.0..=1.0).contains(&c.rate)
            {
                return Err(Error::InvalidArgument(format!(
                    "a Chaos rate is a number from 0 to 1, not {}",
                    c.rate
                )));
            }
            let ns = format!("testing-{}", hex::encode(rand::random::<[u8; 8]>()));
            let world = Arc::new(World::new(ns, ms, self.chaos)?);
            let plan = Plan::Create(b::CreatePlan {
                prefix: String::new(),
                partitions: world.partitions,
            });
            let sub = Attached::new(&world, true).open(plan).await?;
            Ok(Env {
                sub,
                world,
                closed: false,
            })
        })
    }
}

impl Deref for Env {
    type Target = Substantial;

    fn deref(&self) -> &Substantial {
        &self.sub
    }
}

impl Env {
    /// A fresh namespace in memory whose clock starts at `now`.
    pub fn open(now: DateTime<Utc>) -> OpenEnv {
        OpenEnv { now, chaos: None }
    }

    /// Another `Substantial` on the same namespace and clock, as another pod or the application
    /// under test would hold.
    pub async fn client(&self) -> Result<Substantial> {
        let plan = Plan::Open(b::OpenPlan {
            prefix: String::new(),
        });
        Attached::new(&self.world, false).open(plan).await
    }

    /// The virtual time.
    pub fn now(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(self.world.time.now()).expect("a time in chrono's range")
    }

    /// What Chaos injected so far.
    pub fn faults(&self) -> Faults {
        self.world.faults()
    }

    /// Lets every worker do all it can at the current time, until nothing can happen without the
    /// clock moving.
    pub async fn settle(&self) -> Result<()> {
        self.world.settle().await
    }

    /// Settles, then moves the clock forward by `d`, settling at each moment a worker has
    /// something due.
    pub async fn advance(&self, d: Duration) -> Result<()> {
        self.world.advance(ms(d)).await
    }

    /// Every store operation fails for the next `d` of virtual time.
    pub fn outage(&self, d: Duration) {
        let until = self.world.time.now() + ms(d);
        self.world.outage_until.store(until, Ordering::SeqCst);
    }

    /// Holds the next attempt of `step` (`"<step>.undo"` for its undo) once its body returned or
    /// raised; `times` holds more.
    pub fn hold(&self, step: &str) -> Hold {
        let hold = Arc::new(HoldState {
            step: step.to_owned(),
            left: Mutex::new(1),
            held: Mutex::default(),
        });
        lock(&self.world.holds).push(hold.clone());
        Hold(hold)
    }

    /// `Unchecked` naming every run that ended failed without the test reading its failure
    /// through the environment.
    pub async fn check(&self) -> Result<()> {
        let checked = lock(&self.world.checked).clone();
        let unread: Vec<String> = (lock(&self.world.failed).values())
            .filter(|(chain, _)| !checked.contains(chain))
            .map(|(_, failure)| failure.clone())
            .collect();
        if unread.is_empty() {
            return Ok(());
        }
        Err(Error::Unchecked(format!(
            "runs ended failed and the test never read it: {}",
            unread.join("; ")
        )))
    }

    /// Kills the workers as a SIGKILL would, waits up to 10 s for the step bodies still running,
    /// and drops the namespace.
    pub async fn close(mut self) {
        self.closed = true;
        self.world.crash_all();
        // A hung body is left running past the bound of `Stuck`, so that leaving never hangs.
        let until = std::time::Instant::now() + self.world.time.lock().step_limit;
        while self.world.time.bodies_running() {
            let left = until.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                break;
            }
            let _ = tokio::time::timeout(left, self.world.time.changed()).await;
        }
        self.world.drop_all();
    }

    #[doc(hidden)]
    pub fn __namespace(&self) -> &str {
        &self.world.ns
    }

    #[doc(hidden)]
    pub fn __keys(&self, prefix: &str) -> Vec<String> {
        self.world.keys(prefix)
    }

    /// Each worker's place among the test's, how many times Chaos replaced it, and its id.
    #[doc(hidden)]
    pub fn __workers(&self) -> Vec<(usize, u32, String)> {
        lock(&self.world.workers)
            .iter()
            .map(|w| w.place())
            .collect()
    }

    /// Skips no scan when false, to compare a scenario with and without skipping.
    #[doc(hidden)]
    pub fn __skipping(&self, on: bool) {
        self.world.skipping.store(on, Ordering::SeqCst);
    }

    /// How many scans were skipped.
    #[doc(hidden)]
    pub fn __skipped(&self) -> u64 {
        self.world.skipped.load(Ordering::SeqCst)
    }

    /// Every object of the namespace, its key and its bytes.
    #[doc(hidden)]
    pub fn __objects(&self) -> Vec<(String, Vec<u8>)> {
        let keys = self.world.keys("");
        keys.into_iter()
            .filter_map(|k| Some((k.clone(), self.world.object(&k)?.to_vec())))
            .collect()
    }

    #[doc(hidden)]
    pub fn __step_limit(&self, limit: Duration) {
        self.world.time.lock().step_limit = limit;
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        if !self.closed {
            self.world.crash_all();
            self.world.drop_all();
        }
    }
}
