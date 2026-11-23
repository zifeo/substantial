//! Retries of transient failures and resolution of ambiguous creates, shared by every adapter.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;

use super::attempts::sent;
use super::{Cas, Create, Object, StoreFailure};
use crate::settings::StoreRetry;

/// How one attempt of an operation failed.
#[derive(Debug)]
pub(crate) enum Fault {
    /// Nothing happened; trying again may succeed.
    Transient(String),
    /// A create that may or may not have happened: a timeout, a 5xx, a lost connection.
    Ambiguous(String),
    /// Trying again cannot help: an invalid key, a refused permission.
    Permanent(String),
}

struct Backoff<S> {
    policy: StoreRetry,
    delay: Duration,
    attempt: u32,
    sleep: S,
}

impl<S, SF> Backoff<S>
where
    S: FnMut(Duration) -> SF,
    SF: Future<Output = ()>,
{
    fn new(policy: StoreRetry, sleep: S) -> Self {
        Self {
            policy,
            delay: policy.initial,
            attempt: 1,
            sleep,
        }
    }

    /// Waits before the next attempt, or fails once every attempt is spent.
    async fn next(&mut self, error: &str, sleep: bool) -> Result<(), StoreFailure> {
        // § zemxe46k3@j75d4
        if self.attempt >= self.policy.attempts {
            let unit = if self.attempt == 1 {
                "attempt"
            } else {
                "attempts"
            };
            return Err(StoreFailure::new(format!(
                "{error} (after {} {unit})",
                self.attempt
            )));
        }
        self.attempt += 1;
        if sleep {
            (self.sleep)(self.delay).await;
            self.delay = (self.delay * 2).min(self.policy.max);
        }
        Ok(())
    }
}

/// Retries `op` on transient faults, waiting each backoff through `sleep`: `tokio::time::sleep`
/// on the real clock.
pub(crate) async fn with_retry<T, F, Fut, S, SF>(
    policy: StoreRetry,
    mut op: F,
    sleep: S,
) -> Result<T, StoreFailure>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Fault>>,
    S: FnMut(Duration) -> SF,
    SF: Future<Output = ()>,
{
    // § zemxe46k3@hcjty
    let mut backoff = Backoff::new(policy, sleep);
    loop {
        sent();
        match op().await {
            Ok(value) => return Ok(value),
            Err(Fault::Permanent(e)) => return Err(StoreFailure::new(e)),
            Err(Fault::Transient(e) | Fault::Ambiguous(e)) => backoff.next(&e, true).await?,
        }
    }
}

/// A create that never assumes it won or lost: an ambiguous outcome is followed by a `get`,
/// the same bytes meaning `Created`, other bytes `Exists`, and a missing key creating again.
pub(crate) async fn create<C, CF, G, GF, S, SF>(
    policy: StoreRetry,
    bytes: &Bytes,
    mut create: C,
    mut get: G,
    sleep: S,
) -> Result<Create, StoreFailure>
where
    C: FnMut() -> CF,
    CF: Future<Output = Result<Create, Fault>>,
    G: FnMut() -> GF,
    GF: Future<Output = Result<Option<Object>, Fault>>,
    S: FnMut(Duration) -> SF,
    SF: Future<Output = ()>,
{
    // § pnvmmdmz4@jczdt
    let mut backoff = Backoff::new(policy, sleep);
    let mut ambiguous = false;
    // An earlier attempt left unanswered may have landed after the get, so a later `Exists` is
    // checked too.
    let mut was_ambiguous = false;
    loop {
        if ambiguous {
            sent();
            match get().await {
                Ok(Some(stored)) if stored.bytes == *bytes => {
                    return Ok(Create::Created { etag: stored.etag });
                }
                Ok(Some(_)) => return Ok(Create::Exists),
                Ok(None) => ambiguous = false,
                Err(Fault::Permanent(e)) => return Err(StoreFailure::new(e)),
                Err(Fault::Transient(e) | Fault::Ambiguous(e)) => {
                    backoff.next(&e, true).await?;
                    continue;
                }
            }
        }
        sent();
        match create().await {
            Ok(Create::Exists) if was_ambiguous => {
                // § pnvmmdmz4.a@vqgkq
                ambiguous = true;
                backoff
                    .next("exists after an ambiguous create", false)
                    .await?;
            }
            Ok(outcome) => return Ok(outcome),
            Err(Fault::Permanent(e)) => return Err(StoreFailure::new(e)),
            Err(Fault::Transient(e)) => backoff.next(&e, true).await?,
            Err(Fault::Ambiguous(e)) => {
                (ambiguous, was_ambiguous) = (true, true);
                backoff.next(&e, false).await?;
            }
        }
    }
}

/// A compare-and-set that never assumes it won or lost: an ambiguous outcome is followed by a
/// `get`, the same bytes meaning `Written`, the ETag named still current sending it again, and
/// anything else `Mismatch`.
pub(crate) async fn cas<C, CF, G, GF, S, SF>(
    policy: StoreRetry,
    bytes: &Bytes,
    etag: &str,
    mut cas: C,
    mut get: G,
    sleep: S,
) -> Result<Cas, StoreFailure>
where
    C: FnMut() -> CF,
    CF: Future<Output = Result<Cas, Fault>>,
    G: FnMut() -> GF,
    GF: Future<Output = Result<Option<Object>, Fault>>,
    S: FnMut(Duration) -> SF,
    SF: Future<Output = ()>,
{
    // § dqf0ythhp@fxgff
    let mut backoff = Backoff::new(policy, sleep);
    let mut ambiguous = false;
    let mut was_ambiguous = false;
    loop {
        if ambiguous {
            sent();
            match get().await {
                Ok(Some(stored)) if stored.bytes == *bytes => {
                    return Ok(Cas::Written { etag: stored.etag });
                }
                Ok(Some(stored)) if stored.etag == etag => ambiguous = false,
                Ok(_) => return Ok(Cas::Mismatch),
                Err(Fault::Permanent(e)) => return Err(StoreFailure::new(e)),
                Err(Fault::Transient(e) | Fault::Ambiguous(e)) => {
                    backoff.next(&e, true).await?;
                    continue;
                }
            }
        }
        sent();
        match cas().await {
            Ok(Cas::Mismatch) if was_ambiguous => {
                // § dqf0ythhp.a@phkxm
                ambiguous = true;
                backoff
                    .next("mismatch after an ambiguous compare-and-set", false)
                    .await?;
            }
            Ok(outcome) => return Ok(outcome),
            Err(Fault::Permanent(e)) => return Err(StoreFailure::new(e)),
            Err(Fault::Transient(e)) => backoff.next(&e, true).await?,
            Err(Fault::Ambiguous(e)) => {
                (ambiguous, was_ambiguous) = (true, true);
                backoff.next(&e, false).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;

    const POLICY: StoreRetry = StoreRetry {
        attempts: 3,
        initial: Duration::from_millis(100),
        max: Duration::from_secs(5),
    };

    fn script<T>(steps: Vec<Result<T, Fault>>) -> Mutex<VecDeque<Result<T, Fault>>> {
        Mutex::new(steps.into())
    }

    #[tokio::test(start_paused = true)]
    async fn transient_failures_are_retried_then_fail() {
        let steps = script(vec![Err(Fault::Transient("a".into())), Ok(7)]);
        let op = || async { steps.lock().unwrap().pop_front().unwrap() };
        assert_eq!(with_retry(POLICY, op, tokio::time::sleep).await, Ok(7));

        let steps = script(
            (0..3)
                .map(|_| Err::<u8, _>(Fault::Transient("down".into())))
                .collect(),
        );
        let op = || async { steps.lock().unwrap().pop_front().unwrap() };
        let err = with_retry(POLICY, op, tokio::time::sleep)
            .await
            .unwrap_err();
        assert!(err.message.contains("down (after 3 attempts)"), "{err}");

        let once = StoreRetry {
            attempts: 1,
            ..POLICY
        };
        let op = || async { Err::<u8, _>(Fault::Transient("down".into())) };
        let err = with_retry(once, op, tokio::time::sleep).await.unwrap_err();
        assert!(err.message.ends_with("down (after 1 attempt)"), "{err}");
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_failures_are_not_retried() {
        let steps = script(vec![
            Err::<u8, _>(Fault::Permanent("bad key".into())),
            Ok(1),
        ]);
        let op = || async { steps.lock().unwrap().pop_front().unwrap() };
        assert_eq!(
            with_retry(POLICY, op, tokio::time::sleep).await,
            Err(StoreFailure::new("bad key"))
        );
    }

    async fn resolve(
        creates: Vec<Result<Create, Fault>>,
        gets: Vec<Result<Option<Object>, Fault>>,
    ) -> Result<Create, StoreFailure> {
        let (creates, gets) = (script(creates), script(gets));
        let mine = Bytes::from_static(b"mine");
        create(
            POLICY,
            &mine,
            || async {
                creates
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("unexpected create")
            },
            || async { gets.lock().unwrap().pop_front().expect("unexpected get") },
            tokio::time::sleep,
        )
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn every_request_is_counted_the_read_back_too() {
        use crate::store::attempts::measured;
        let steps = script(vec![Err(Fault::Transient("a".into())), Ok(7)]);
        let op = || async { steps.lock().unwrap().pop_front().unwrap() };
        // § zemxe46k3.a@6j9ds
        let (out, sent) = measured(with_retry(POLICY, op, tokio::time::sleep)).await;
        assert_eq!((out, sent), (Ok(7), 2));
        let lost = vec![Err(Fault::Ambiguous("timeout".into()))];
        let same = vec![object(b"mine", "e1")];
        let (out, sent) = measured(resolve(lost, same)).await;
        assert_eq!((out, sent), (Ok(created("e1")), 2));
        let (_, sent) = measured(async {}).await;
        assert_eq!(sent, 1, "an adapter without retries sends one request");
    }

    async fn resolve_cas(
        writes: Vec<Result<Cas, Fault>>,
        gets: Vec<Result<Option<Object>, Fault>>,
    ) -> Result<Cas, StoreFailure> {
        let (writes, gets) = (script(writes), script(gets));
        let mine = Bytes::from_static(b"mine");
        cas(
            POLICY,
            &mine,
            "e0",
            || async { writes.lock().unwrap().pop_front().expect("unexpected cas") },
            || async { gets.lock().unwrap().pop_front().expect("unexpected get") },
            tokio::time::sleep,
        )
        .await
    }

    fn created(etag: &str) -> Create {
        Create::Created { etag: etag.into() }
    }

    fn object(bytes: &'static [u8], etag: &str) -> Result<Option<Object>, Fault> {
        Ok(Some(Object {
            bytes: Bytes::from_static(bytes),
            etag: etag.into(),
        }))
    }

    #[tokio::test(start_paused = true)]
    async fn ambiguous_compare_and_sets_are_resolved_with_a_get() {
        let lost = || Err(Fault::Ambiguous("timeout".into()));
        let written = |e: &str| Ok::<_, StoreFailure>(Cas::Written { etag: e.into() });
        let mine = object(b"mine", "e1");
        assert_eq!(resolve_cas(vec![lost()], vec![mine]).await, written("e1"));
        let theirs = object(b"theirs", "e2");
        assert_eq!(
            resolve_cas(vec![lost()], vec![theirs]).await,
            Ok(Cas::Mismatch)
        );
        assert_eq!(
            resolve_cas(vec![lost()], vec![Ok(None)]).await,
            Ok(Cas::Mismatch)
        );
        let unchanged = object(b"before", "e0");
        let again = vec![lost(), Ok(Cas::Written { etag: "e3".into() })];
        assert_eq!(resolve_cas(again, vec![unchanged]).await, written("e3"));
        // The first attempt landed after the read-back, so the second finds its own write.
        let landed_late = vec![lost(), Ok(Cas::Mismatch)];
        let gets = vec![object(b"before", "e0"), object(b"mine", "e4")];
        assert_eq!(resolve_cas(landed_late, gets).await, written("e4"));
        let flaky = vec![Err(Fault::Transient("get".into())), object(b"mine", "e5")];
        assert_eq!(resolve_cas(vec![lost()], flaky).await, written("e5"));
        let plain = vec![Ok(Cas::Mismatch)];
        assert_eq!(resolve_cas(plain, vec![]).await, Ok(Cas::Mismatch));
    }

    #[tokio::test(start_paused = true)]
    async fn ambiguous_creates_are_resolved_with_a_get() {
        let lost = || Err(Fault::Ambiguous("timeout".into()));
        let same = object(b"mine", "e1");
        let other = object(b"theirs", "e2");
        assert_eq!(resolve(vec![lost()], vec![same]).await, Ok(created("e1")));
        assert_eq!(resolve(vec![lost()], vec![other]).await, Ok(Create::Exists));
        let again = vec![lost(), Ok(created("e3"))];
        assert_eq!(resolve(again, vec![Ok(None)]).await, Ok(created("e3")));
        let landed_late = vec![lost(), Ok(Create::Exists)];
        let gets = vec![Ok(None), object(b"mine", "e4")];
        assert_eq!(resolve(landed_late, gets).await, Ok(created("e4")));
        let flaky_get = vec![Err(Fault::Transient("get".into())), object(b"mine", "e5")];
        assert_eq!(resolve(vec![lost()], flaky_get).await, Ok(created("e5")));
        assert!(
            resolve(
                vec![lost(), lost()],
                vec![Ok(None), Err(Fault::Transient("x".into()))]
            )
            .await
            .is_err()
        );
    }
}
