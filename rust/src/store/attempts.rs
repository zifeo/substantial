//! The requests an adapter sends for one store operation, its retries and read-backs included,
//! which the core counts for runs and the observer is told.

use std::cell::Cell;
use std::future::Future;

tokio::task_local! {
    static SENT: Cell<u32>;
}

/// Counts one request of the operation being measured; called by the adapters' shared retry
/// layer for each attempt it makes.
pub(crate) fn sent() {
    // § zemxe46k3.a@2k8hb
    let _ = SENT.try_with(|n| n.set(n.get() + 1));
}

/// Performs a store operation, with the requests it sent. An adapter outside the shared retry
/// layer, the memory store or a test's, sends each operation once. An operation measured inside
/// another, through a store wrapping a store, counts in both.
pub(crate) async fn measured<T>(op: impl Future<Output = T>) -> (T, u32) {
    // § zemxe46k3.a@3g27z
    let counted = async {
        let out = op.await;
        (out, SENT.with(Cell::get))
    };
    let (out, n) = SENT.scope(Cell::new(0), counted).await;
    let n = n.max(1);
    let _ = SENT.try_with(|outer| outer.set(outer.get() + n));
    (out, n)
}
