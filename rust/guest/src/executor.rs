//! The guest's executor: it polls the workflow, then waits on every call still pending, as the
//! Rust host polls its own code to quiescence.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::__bindings::substantial::guest::context as wit;

/// A call made and not yet taken, with the waker of the future awaiting it.
struct Live {
    call: Rc<wit::Call>,
    waker: Waker,
    /// A `wait` answered it ended, and its future has not taken it yet.
    ended: bool,
}

thread_local! {
    /// Every call made and not taken, by the order it was made in. A combinator polls only the
    /// futures it was woken for, so a call stays here from its first poll until its future takes
    /// it or drops, not only while the last poll saw it.
    static LIVE: RefCell<BTreeMap<u64, Live>> = const { RefCell::new(BTreeMap::new()) };
    static NEXT: Cell<u64> = const { Cell::new(0) };
}

/// A call of a verb, made when first polled so that calls reach the host in the code's order.
pub(crate) struct Calling {
    make: Option<Box<dyn FnOnce() -> wit::Call>>,
    call: Option<(u64, Rc<wit::Call>)>,
}

impl Calling {
    pub(crate) fn new(make: impl FnOnce() -> wit::Call + 'static) -> Self {
        Self {
            make: Some(Box::new(make)),
            call: None,
        }
    }
}

impl Future for Calling {
    type Output = Result<String, wit::Raised>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // § .:em5h7z7cg.a@327as
        if let Some(make) = self.make.take() {
            let id = NEXT.with(|n| n.replace(n.get() + 1));
            self.call = Some((id, Rc::new(make())));
        }
        // § end .:em5h7z7cg.a
        let (id, call) = self.call.clone().expect("a call polled after it ended");
        match call.take() {
            Some(ended) => {
                self.call = None;
                LIVE.with(|l| l.borrow_mut().remove(&id));
                Poll::Ready(ended)
            }
            None => {
                let waker = cx.waker().clone();
                LIVE.with(|l| match l.borrow_mut().entry(id) {
                    std::collections::btree_map::Entry::Occupied(mut live) => {
                        live.get_mut().waker = waker;
                    }
                    std::collections::btree_map::Entry::Vacant(free) => {
                        free.insert(Live {
                            call,
                            waker,
                            ended: false,
                        });
                    }
                });
                Poll::Pending
            }
        }
    }
}

impl Drop for Calling {
    fn drop(&mut self) {
        if let Some((id, _)) = &self.call {
            // A thread local may be gone at the thread's end, when nothing waits any more.
            let _ = LIVE.try_with(|l| l.borrow_mut().remove(id));
        }
    }
}

/// Runs `future` to its end, blocking in the host's `wait` whenever it waits on calls.
///
/// # Panics
///
/// When `future` waits on something other than a call, which nothing would ever end.
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        // The calls a `wait` already answered ended are left out: their futures were woken and
        // will take them when polled, and waiting on them would answer at once.
        let pending: Vec<(u64, Rc<wit::Call>)> = LIVE.with(|l| {
            (l.borrow().iter())
                .filter(|(_, live)| !live.ended)
                .map(|(id, live)| (*id, live.call.clone()))
                .collect()
        });
        assert!(
            !pending.is_empty(),
            "the code awaits something other than a call of its context, which nothing would end"
        );
        let calls: Vec<&wit::Call> = pending.iter().map(|(_, call)| &**call).collect();
        // § .:em5h7z7cg@mawfx
        for i in wit::wait(&calls) {
            let id = pending[i as usize].0;
            // Woken once the registry is released: a waker may poll or drop a call re-entrantly.
            let waker = LIVE.with(|l| {
                let mut live = l.borrow_mut();
                let live = live.get_mut(&id)?;
                live.ended = true;
                Some(live.waker.clone())
            });
            if let Some(waker) = waker {
                waker.wake();
            }
        }
        // § end .:em5h7z7cg
    }
}
