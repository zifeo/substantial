//! Retries of transient failures and resolution of ambiguous creates, shared by every adapter.

use std::future::Future;

use bytes::Bytes;

use super::{Create, StoreFailure};
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

struct Backoff {
    policy: StoreRetry,
    delay: std::time::Duration,
    attempt: u32,
}

impl Backoff {
    fn new(policy: StoreRetry) -> Self {
        Self {
            policy,
            delay: policy.initial,
            attempt: 1,
        }
    }

    /// Waits before the next attempt, or fails once every attempt is spent.
    async fn next(&mut self, error: &str, sleep: bool) -> Result<(), StoreFailure> {
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
            tokio::time::sleep(self.delay).await;
            self.delay = (self.delay * 2).min(self.policy.max);
        }
        Ok(())
    }
}

pub(crate) async fn with_retry<T, F, Fut>(policy: StoreRetry, mut op: F) -> Result<T, StoreFailure>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Fault>>,
{
    let mut backoff = Backoff::new(policy);
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(Fault::Permanent(e)) => return Err(StoreFailure::new(e)),
            Err(Fault::Transient(e) | Fault::Ambiguous(e)) => backoff.next(&e, true).await?,
        }
    }
}

/// A create that never assumes it won or lost: an ambiguous outcome is followed by a `get`,
/// the same bytes meaning `Created`, other bytes `Exists`, and a missing key creating again.
pub(crate) async fn create<C, CF, G, GF>(
    policy: StoreRetry,
    bytes: &Bytes,
    mut create: C,
    mut get: G,
) -> Result<Create, StoreFailure>
where
    C: FnMut() -> CF,
    CF: Future<Output = Result<Create, Fault>>,
    G: FnMut() -> GF,
    GF: Future<Output = Result<Option<Bytes>, Fault>>,
{
    let mut backoff = Backoff::new(policy);
    let mut ambiguous = false;
    // An earlier attempt left unanswered may have landed after the get, so a later `Exists` is
    // checked too.
    let mut was_ambiguous = false;
    loop {
        if ambiguous {
            match get().await {
                Ok(Some(stored)) if stored == *bytes => return Ok(Create::Created),
                Ok(Some(_)) => return Ok(Create::Exists),
                Ok(None) => ambiguous = false,
                Err(Fault::Permanent(e)) => return Err(StoreFailure::new(e)),
                Err(Fault::Transient(e) | Fault::Ambiguous(e)) => {
                    backoff.next(&e, true).await?;
                    continue;
                }
            }
        }
        match create().await {
            Ok(Create::Exists) if was_ambiguous => {
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

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
        assert_eq!(with_retry(POLICY, op).await, Ok(7));

        let steps = script(
            (0..3)
                .map(|_| Err::<u8, _>(Fault::Transient("down".into())))
                .collect(),
        );
        let op = || async { steps.lock().unwrap().pop_front().unwrap() };
        let err = with_retry(POLICY, op).await.unwrap_err();
        assert!(err.message.contains("down (after 3 attempts)"), "{err}");

        let once = StoreRetry {
            attempts: 1,
            ..POLICY
        };
        let op = || async { Err::<u8, _>(Fault::Transient("down".into())) };
        let err = with_retry(once, op).await.unwrap_err();
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
            with_retry(POLICY, op).await,
            Err(StoreFailure::new("bad key"))
        );
    }

    async fn resolve(
        creates: Vec<Result<Create, Fault>>,
        gets: Vec<Result<Option<Bytes>, Fault>>,
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
        )
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn ambiguous_creates_are_resolved_with_a_get() {
        let lost = || Err(Fault::Ambiguous("timeout".into()));
        let same = Ok(Some(Bytes::from_static(b"mine")));
        let other = Ok(Some(Bytes::from_static(b"theirs")));
        assert_eq!(resolve(vec![lost()], vec![same]).await, Ok(Create::Created));
        assert_eq!(resolve(vec![lost()], vec![other]).await, Ok(Create::Exists));
        let again = vec![lost(), Ok(Create::Created)];
        assert_eq!(resolve(again, vec![Ok(None)]).await, Ok(Create::Created));
        let landed_late = vec![lost(), Ok(Create::Exists)];
        let gets = vec![Ok(None), Ok(Some(Bytes::from_static(b"mine")))];
        assert_eq!(resolve(landed_late, gets).await, Ok(Create::Created));
        let flaky_get = vec![
            Err(Fault::Transient("get".into())),
            Ok(Some(Bytes::from_static(b"mine"))),
        ];
        assert_eq!(resolve(vec![lost()], flaky_get).await, Ok(Create::Created));
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
