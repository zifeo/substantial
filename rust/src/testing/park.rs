//! A task of the environment parked on its signals and deadline, as `Time` sees it.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::time::Time;
use super::workers::Incarnation;
use crate::clock::Ready;

tokio::task_local! {
    /// The id of the environment's task running.
    pub(super) static TASK: u64;
}

/// A task parked until one of `signals` is ready or the clock reaches `at`, in ms.
pub(super) struct Waiter {
    signals: Vec<Arc<dyn Ready>>,
    pub(super) at: Option<i64>,
    pub(super) waker: Waker,
}

impl Waiter {
    pub(super) fn ready(&self, owner: &Incarnation, now: i64) -> bool {
        !owner.paused()
            && (self.at.is_some_and(|at| at <= now) || self.signals.iter().any(|s| s.ready()))
    }
}

pub(super) struct Tracked {
    pub(super) owner: Arc<Incarnation>,
    pub(super) name: String,
    pub(super) waiter: Option<Waiter>,
}

pub(super) struct Wait<'a> {
    pub(super) time: &'a Time,
    pub(super) id: u64,
    pub(super) parked: bool,
    pub(super) signals: Vec<Arc<dyn Ready>>,
    pub(super) at: Option<i64>,
}

impl Future for Wait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let mut st = this.time.lock();
        let (ms, open) = (st.ms, st.open);
        // A task forgotten by a crash stays here until its abort drops it.
        let Some(task) = st.tasks.get_mut(&this.id) else {
            return Poll::Pending;
        };
        if !this.parked {
            for s in &this.signals {
                s.register(cx.waker());
            }
            let waiter = Waiter {
                signals: std::mem::take(&mut this.signals),
                at: this.at,
                waker: cx.waker().clone(),
            };
            let ready = open && waiter.ready(&task.owner, ms);
            task.waiter = Some(waiter);
            this.parked = true;
            drop(st);
            this.time.notify();
            // A wait yields at least once, as on the real clock, so that a loop of waits already
            // due lets the other tasks run.
            if ready {
                cx.waker().wake_by_ref();
            }
            return Poll::Pending;
        }
        let owner = task.owner.clone();
        let waiter = task.waiter.as_mut().expect("a parked task has its waiter");
        if !waiter.waker.will_wake(cx.waker()) {
            waiter.waker = cx.waker().clone();
        }
        for s in &waiter.signals {
            s.register(cx.waker());
        }
        if open && waiter.ready(&owner, ms) {
            task.waiter = None;
            this.parked = false;
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

impl Drop for Wait<'_> {
    fn drop(&mut self) {
        if self.parked
            && let Some(task) = self.time.lock().tasks.get_mut(&self.id)
        {
            task.waiter = None;
        }
    }
}
