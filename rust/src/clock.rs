//! The host's clock: the time every machine is given, and every wait on it (step timeouts,
//! timers, backoffs, shutdown grace). A worker takes one; `testing` gives it a virtual one.
//!
//! Everything a worker task waits on is a [`Ready`] its producer sets at once, so that a clock
//! can tell from state alone whether a task is waiting and on what.

use std::collections::VecDeque;
use std::future::Future;
#[cfg(test)]
use std::sync::atomic::AtomicI64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::task::{Poll, Waker};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::task::AbortHandle;

use crate::proto::boundary::v1::outcome::Outcome;
use crate::proto::layout::v1::Time;
use crate::time::until;

/// Something a clock waits on: ready once its producer set it, which wakes the waiters it holds.
pub(crate) trait Ready: Send + Sync {
    fn ready(&self) -> bool;
    fn register(&self, waker: &Waker);
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().expect("never panics while locked")
}

#[derive(Default)]
struct Wakers(Mutex<Vec<Waker>>);

impl Wakers {
    fn register(&self, waker: &Waker) {
        let mut wakers = lock(&self.0);
        if !wakers.iter().any(|w| w.will_wake(waker)) {
            wakers.push(waker.clone());
        }
    }

    fn wake(&self) {
        let wakers = std::mem::take(&mut *lock(&self.0));
        wakers.into_iter().for_each(Waker::wake);
    }
}

/// A queue a clock can wait on: ready while it holds an item.
pub(crate) struct Inbox<T> {
    items: Mutex<VecDeque<T>>,
    wakers: Wakers,
}

impl<T> Default for Inbox<T> {
    fn default() -> Self {
        Self {
            items: Mutex::default(),
            wakers: Wakers::default(),
        }
    }
}

impl<T> Inbox<T> {
    pub(crate) fn push(&self, item: T) {
        lock(&self.items).push_back(item);
        self.wakers.wake();
    }

    pub(crate) fn pop(&self) -> Option<T> {
        lock(&self.items).pop_front()
    }

    pub(crate) fn is_empty(&self) -> bool {
        lock(&self.items).is_empty()
    }
}

impl<T: Send> Ready for Inbox<T> {
    fn ready(&self) -> bool {
        !self.is_empty()
    }

    fn register(&self, waker: &Waker) {
        self.wakers.register(waker);
    }
}

/// Set once, never reset.
#[derive(Default)]
pub(crate) struct Flag {
    set: AtomicBool,
    wakers: Wakers,
}

impl Flag {
    pub(crate) fn set(&self) {
        self.set.store(true, Ordering::SeqCst);
        self.wakers.wake();
    }

    pub(crate) fn is_set(&self) -> bool {
        self.set.load(Ordering::SeqCst)
    }
}

impl Ready for Flag {
    fn ready(&self) -> bool {
        self.is_set()
    }

    fn register(&self, waker: &Waker) {
        self.wakers.register(waker);
    }
}

/// A task spawned on a clock: its value once it returned, and a flag set then.
pub(crate) struct Task<T> {
    done: Arc<Flag>,
    value: Arc<Mutex<Option<T>>>,
    abort: AbortHandle,
}

impl<T: Send + 'static> Task<T> {
    /// Runs `future` through `spawn`, which returns the handle aborting what it spawned, then
    /// `after` once its value is in.
    pub(crate) fn new(
        future: impl Future<Output = T> + Send + 'static,
        after: impl FnOnce(&T) + Send + 'static,
        spawn: impl FnOnce(BoxFuture<'static, ()>) -> AbortHandle,
    ) -> Self {
        let done = Arc::new(Flag::default());
        let value = Arc::new(Mutex::new(None));
        let (d, v) = (done.clone(), value.clone());
        let abort = spawn(Box::pin(async move {
            let out = future.await;
            let mut slot = lock(&v);
            let out = slot.insert(out);
            d.set();
            after(out);
        }));
        Self { done, value, abort }
    }

    pub(crate) fn done(&self) -> Arc<dyn Ready> {
        self.done.clone()
    }

    #[cfg(feature = "testing")]
    pub(crate) fn is_done(&self) -> bool {
        self.done.is_set()
    }

    /// Whether it returned or was aborted.
    pub(crate) fn is_finished(&self) -> bool {
        self.done.is_set() || self.abort.is_finished()
    }

    pub(crate) fn take(&self) -> Option<T> {
        lock(&self.value).take()
    }

    pub(crate) fn with<R>(&self, f: impl FnOnce(Option<&T>) -> R) -> R {
        f(lock(&self.value).as_ref())
    }

    pub(crate) fn abort(&self) {
        self.abort.abort();
    }

    /// Waits on the real clock until it returned.
    pub(crate) async fn returned(&self) {
        Clock::Real.wait(&[self.done()], None).await;
    }
}

/// Tasks aborted when dropped.
#[derive(Default)]
pub(crate) struct Tasks(Vec<Task<()>>);

impl Tasks {
    pub(crate) fn push(&mut self, task: Task<()>) {
        self.0.retain(|t| !t.is_finished());
        self.0.push(task);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.iter().all(Task::is_finished)
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        self.0.iter().for_each(Task::abort);
    }
}

#[derive(Clone)]
pub(crate) enum Clock {
    Real,
    /// The real clock reading the time from the test, for replays driven by hand.
    #[cfg(test)]
    Fixed(Arc<AtomicI64>),
    #[cfg(feature = "testing")]
    Virtual(Arc<crate::testing::Incarnation>),
}

impl Clock {
    pub(crate) fn now(&self) -> Time {
        match self {
            Clock::Real => crate::time::now(),
            #[cfg(test)]
            Clock::Fixed(ms) => Time {
                ms: ms.load(Ordering::SeqCst),
            },
            #[cfg(feature = "testing")]
            Clock::Virtual(i) => i.now(),
        }
    }

    /// Monotonic milliseconds, for durations a step of the wall clock must not change.
    pub(crate) fn elapsed(&self) -> i64 {
        match self {
            #[cfg(feature = "testing")]
            Clock::Virtual(i) => i.now().ms,
            _ => {
                static START: OnceLock<tokio::time::Instant> = OnceLock::new();
                let start = *START.get_or_init(tokio::time::Instant::now);
                i64::try_from(start.elapsed().as_millis()).expect("ms fit in i64")
            }
        }
    }

    /// Returns once one of `signals` is ready, or once `at` has come.
    pub(crate) async fn wait(&self, signals: &[Arc<dyn Ready>], at: Option<Time>) {
        #[cfg(feature = "testing")]
        if let Clock::Virtual(i) = self {
            return i.wait(signals, at.map(|t| t.ms)).await;
        }
        let mut sleep = at.map(|at| Box::pin(tokio::time::sleep(until(&at, &self.now()))));
        std::future::poll_fn(|cx| {
            for s in signals {
                s.register(cx.waker());
            }
            if signals.iter().any(|s| s.ready()) {
                return Poll::Ready(());
            }
            match &mut sleep {
                Some(sleep) => sleep.as_mut().poll(cx),
                None => Poll::Pending,
            }
        })
        .await
    }

    /// A task of the worker, named for what the virtual clock reports.
    pub(crate) fn spawn<T: Send + 'static>(
        &self,
        name: std::fmt::Arguments<'_>,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T> {
        self.spawn_then(name, future, |_| {})
    }

    /// As `spawn`, then `after` once its value is in, which `Task::with` then reads.
    pub(crate) fn spawn_then<T: Send + 'static>(
        &self,
        name: std::fmt::Arguments<'_>,
        future: impl Future<Output = T> + Send + 'static,
        after: impl FnOnce(&T) + Send + 'static,
    ) -> Task<T> {
        #[cfg(feature = "testing")]
        if let Clock::Virtual(i) = self {
            return Task::new(future, after, |f| i.spawn(name.to_string(), f));
        }
        let _ = name;
        Task::new(future, after, |f| tokio::spawn(f).abort_handle())
    }

    /// Counts a replay of `run`, which the virtual clock bounds at one instant.
    pub(crate) fn replayed(&self, run: &str) {
        #[cfg(feature = "testing")]
        if let Clock::Virtual(i) = self {
            i.replayed(run);
        }
        let _ = run;
    }

    /// The code was given the failure of `chain`, a child it started; the testing environment
    /// counts it read.
    pub(crate) fn failure_read(&self, chain: &str) {
        #[cfg(feature = "testing")]
        if let Clock::Virtual(i) = self {
            i.failure_read(chain);
        }
        let _ = chain;
    }

    /// One attempt of the step `name`; `None` when its timeout passed first.
    pub(crate) async fn attempt(
        &self,
        name: &str,
        key: &str,
        body: BoxFuture<'static, Outcome>,
        timeout: Option<Duration>,
    ) -> Option<Outcome> {
        #[cfg(feature = "testing")]
        if let Clock::Virtual(i) = self {
            return i.attempt(name, key, body, timeout).await;
        }
        let _ = (name, key);
        match timeout {
            Some(t) => tokio::time::timeout(t, body).await.ok(),
            None => Some(body.await),
        }
    }
}
