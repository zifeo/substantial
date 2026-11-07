//! Logs of workflow code (I-14): `tracing` events, muted while a replay walks the history.

use std::fmt::Display;
use std::sync::Arc;

use super::scheduler::Shared;

/// The logger of a run, `c.log`.
#[derive(Clone)]
pub struct Log {
    shared: Arc<Shared>,
}

macro_rules! level {
    ($name:ident, $level:ident) => {
        pub fn $name(&self, message: impl Display) {
            if self.shared.live() {
                tracing::$level!(run = self.shared.run(), "{message}");
            }
        }
    };
}

impl Log {
    pub(super) fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }

    level!(trace, trace);
    level!(debug, debug);
    level!(info, info);
    level!(warn, warn);
    level!(error, error);
}
