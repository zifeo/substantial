//! The executor of I-24 for Rust: the host polls the workflow future of a replay itself.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Poll, Wake, Waker};

#[derive(Default)]
struct Flag(AtomicBool);

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Polls the workflow future of a replay until it is quiescent: a poll returns `Pending` with no
/// wake pending.
pub(crate) struct Executor {
    flag: Arc<Flag>,
    waker: Waker,
}

impl Executor {
    pub(crate) fn new() -> Self {
        let flag = Arc::new(Flag::default());
        Self {
            waker: Waker::from(flag.clone()),
            flag,
        }
    }

    pub(crate) fn run<T>(&self, future: Pin<&mut (dyn Future<Output = T> + Send)>) -> Poll<T> {
        let mut future = future;
        let mut cx = std::task::Context::from_waker(&self.waker);
        loop {
            self.flag.0.store(false, Ordering::SeqCst);
            if let Poll::Ready(v) = future.as_mut().poll(&mut cx) {
                return Poll::Ready(v);
            }
            if !self.flag.0.swap(false, Ordering::SeqCst) {
                return Poll::Pending;
            }
        }
    }
}
