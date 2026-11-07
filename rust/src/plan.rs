//! Client actions as plan machines (I-23): the host performs each store step until the outcome.

use crate::core::{Core, PlanMachine};
use crate::error::{Error, Result};
use crate::proto::boundary::v1::{self as b, ErrorKind, plan_step};
use crate::store::{self, Store};

/// Runs a plan to its outcome, the first step that is not a store operation.
pub(crate) async fn run(
    core: &Core,
    store: &dyn Store,
    plan: b::plan_input::Plan,
) -> Result<plan_step::Step> {
    let machine = PlanMachine::new(core, &b::PlanInput { plan: Some(plan) })?;
    loop {
        match machine.next()? {
            plan_step::Step::Store(op) => machine.feed(store::perform(store, op).await)?,
            outcome => return Ok(outcome),
        }
    }
}

/// An `INVALID` answer to a plan or call whose names and ids come from the caller: the caller's
/// argument, not a bug of the host.
pub(crate) fn caller_invalid(e: Error) -> Error {
    match e {
        Error::Core {
            kind: ErrorKind::Invalid,
            message,
        } => Error::InvalidArgument(message),
        other => other,
    }
}

pub(crate) fn unexpected(plan: &str, outcome: plan_step::Step) -> Error {
    Error::Protocol(format!("{plan} plan answered {outcome:?}"))
}

const CROCKFORD: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// ULIDs: 48 bits of milliseconds then 80 random bits, in lowercase Crockford base32, as run
/// and message ids are lowercase (I-17). Monotonic: one made in the same millisecond as the last,
/// or earlier when the clock went back, is the last plus one, so a client's messages are consumed
/// in the order sent.
pub(crate) struct Ulids {
    /// The time and random part of the last ULID.
    last: std::sync::Mutex<(u128, u128)>,
    bits: Bits,
}

enum Bits {
    Random,
    /// From a test environment's seed and the millisecond.
    #[cfg(feature = "testing")]
    Seeded(u64),
}

/// The ULIDs of the process, monotonic across its clients.
static ULIDS: Ulids = Ulids::new(Bits::Random);

impl Ulids {
    const fn new(bits: Bits) -> Self {
        Self {
            last: std::sync::Mutex::new((0, 0)),
            bits,
        }
    }

    #[cfg(feature = "testing")]
    pub(crate) const fn seeded(seed: u64) -> Self {
        Self::new(Bits::Seeded(seed))
    }

    pub(crate) fn next(&self, ms: i64) -> String {
        let ms = u128::try_from(ms).expect("a time after 1970") & ((1 << 48) - 1);
        let mut last = self.last.lock().expect("never poisoned");
        let ms = ms.max(last.0);
        let random = if last.0 == ms {
            let next = last.1 + 1;
            assert!(next < 1 << 80, "ULIDs of one millisecond exhausted");
            next
        } else {
            let bits = match self.bits {
                Bits::Random => u128::from_be_bytes(rand::random::<[u8; 16]>()),
                #[cfg(feature = "testing")]
                Bits::Seeded(seed) => crate::testing::ulid_bits(seed, ms),
            };
            bits & ((1 << 80) - 1)
        };
        *last = (ms, random);
        let value = (ms << 80) | random;
        (0..26)
            .rev()
            .map(|i| CROCKFORD[((value >> (i * 5)) & 31) as usize] as char)
            .collect()
    }
}

pub(crate) fn ulid(ms: i64) -> String {
    ULIDS.next(ms)
}

#[cfg(test)]
mod tests {
    use super::ulid;

    // One test, so that no other test of this binary makes ULIDs in between.
    #[test]
    fn ulids_sort_by_time_then_in_generation_order() {
        let now = crate::time::now().ms;
        let (a, b) = (ulid(now), ulid(now + 1_000));
        assert_eq!(a.len(), 26);
        assert!(a < b);
        assert!(
            a.bytes()
                .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase())
        );
        let (c, d) = (ulid(now + 2_000), ulid(now + 2_000));
        assert!(c < d);
        assert_eq!(c[..10], d[..10]);
        let e = ulid(now);
        assert!(d < e, "a clock gone back keeps the order");
        assert_eq!(d[..10], e[..10]);
        let ids: Vec<String> = (0..10_000).map(|_| ulid(crate::time::now().ms)).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len());
    }
}
