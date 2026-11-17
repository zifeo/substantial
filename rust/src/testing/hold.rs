//! The holds of step attempts: `Env::hold`.

use std::sync::{Arc, Mutex, MutexGuard};

use super::workers::Incarnation;
use crate::clock::Flag;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().expect("never panics while locked")
}

pub(super) struct Held {
    pub(super) inc: Arc<Incarnation>,
    pub(super) released: Arc<Flag>,
    pub(super) run: String,
}

pub(super) struct HoldState {
    pub(super) step: String,
    pub(super) left: Mutex<u32>,
    pub(super) held: Mutex<Vec<Arc<Held>>>,
}

/// Takes a held attempt off its hold when it ends, however it ends.
pub(super) struct Holding(pub(super) Arc<HoldState>, pub(super) Arc<Held>);

impl Drop for Holding {
    fn drop(&mut self) {
        lock(&self.0.held).retain(|h| !Arc::ptr_eq(h, &self.1));
    }
}

/// The next attempts of a step, held once their body returned or raised: `Env::hold`.
#[derive(Clone)]
pub struct Hold(pub(super) Arc<HoldState>);

impl std::fmt::Debug for Hold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let left = *lock(&self.0.left);
        write!(
            f,
            "<hold of {}: {} held, {left} to come>",
            self.0.step,
            self.held()
        )
    }
}

impl Hold {
    /// Holds the next `times` attempts instead of the next one.
    ///
    /// # Panics
    ///
    /// When `times` is 0.
    pub fn times(self, times: u32) -> Self {
        assert!(times > 0, "a hold takes at least one attempt");
        *lock(&self.0.left) = times;
        self
    }

    /// The number of attempts held now.
    pub fn held(&self) -> usize {
        lock(&self.0.held)
            .iter()
            .filter(|h| !h.inc.is_dead())
            .count()
    }

    /// Hands the outcomes of the attempts held to their workers.
    pub fn release(&self) {
        for h in std::mem::take(&mut *lock(&self.0.held)) {
            h.released.set();
        }
    }

    /// Crashes the workers that hold the attempts held.
    pub fn crash(&self) {
        for h in std::mem::take(&mut *lock(&self.0.held)) {
            if let Some(worker) = h.inc.worker()
                && !h.inc.is_dead()
                && Arc::ptr_eq(&worker.current().inc, &h.inc)
            {
                worker.crash();
            }
        }
    }
}
