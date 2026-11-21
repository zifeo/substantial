//! The guest's executor: it polls the workflow, then waits on every call the poll left pending,
//! as the Rust host polls its own code to quiescence.

use std::cell::RefCell;
use std::future::Future;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::__bindings::substantial::guest::context as wit;

thread_local! {
    /// The calls the last poll found pending, with the wakers of the futures awaiting them.
    static PENDING: RefCell<Vec<(Rc<wit::Call>, Waker)>> = const { RefCell::new(Vec::new()) };
}

/// A call of a verb, made when first polled so that calls reach the host in the code's order.
pub(crate) struct Calling {
    make: Option<Box<dyn FnOnce() -> wit::Call>>,
    call: Option<Rc<wit::Call>>,
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
        // § .:em5h7z7cg.a@pjrvw
        if let Some(make) = self.make.take() {
            self.call = Some(Rc::new(make()));
        }
        // § end .:em5h7z7cg.a
        let call = self.call.clone().expect("a call polled after it ended");
        match call.take() {
            Some(ended) => {
                self.call = None;
                Poll::Ready(ended)
            }
            None => {
                PENDING.with(|p| p.borrow_mut().push((call, cx.waker().clone())));
                Poll::Pending
            }
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
            PENDING.with(|p| p.borrow_mut().clear());
            return value;
        }
        let pending = PENDING.with(|p| std::mem::take(&mut *p.borrow_mut()));
        assert!(
            !pending.is_empty(),
            "the code awaits something other than a call of its context, which nothing would end"
        );
        let calls: Vec<&wit::Call> = pending.iter().map(|(call, _)| &**call).collect();
        // § .:em5h7z7cg@e4fb4
        for i in wit::wait(&calls) {
            pending[i as usize].1.wake_by_ref();
        }
        // § end .:em5h7z7cg
    }
}
