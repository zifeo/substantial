//! A value shared by a workflow's handlers and predicates, which are `'static` closures.

use std::sync::{Arc, Mutex, MutexGuard};

/// A value that handlers update and predicates read: `Var::new(v)`, then
/// `c.handle(name, var.handler(|v, msg| ..))` and `c.wait_until(var.check(|v| ..))`.
#[derive(Default)]
pub struct Var<T>(Arc<Mutex<T>>);

impl<T> Clone for Var<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Send + 'static> Var<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(Mutex::new(value)))
    }

    fn lock(&self) -> MutexGuard<'_, T> {
        self.0
            .lock()
            .expect("a Var is poisoned by a panic while locked")
    }

    /// A handler for `c.handle` and `Handlers::on`: `f` gets the value and the message.
    pub fn handler<M>(
        &self,
        mut f: impl FnMut(&mut T, M) + Send + 'static,
    ) -> impl FnMut(M) + Send + 'static {
        let var = self.clone();
        move |m| f(&mut var.lock(), m)
    }

    /// A predicate for `c.wait_until`.
    pub fn check(
        &self,
        f: impl Fn(&T) -> bool + Send + 'static,
    ) -> impl Fn() -> bool + Send + 'static {
        let var = self.clone();
        move || f(&var.lock())
    }

    /// A copy of the value.
    pub fn get(&self) -> T
    where
        T: Clone,
    {
        self.lock().clone()
    }

    /// `f` of the value, for a read without a copy.
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        f(&self.lock())
    }
}
