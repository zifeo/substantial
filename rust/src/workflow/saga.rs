//! Rollbacks and sagas (I-10): the core says which steps to undo, the host runs their undos.

use std::future::Future;

use super::context::Context;
use super::scheduler::{CallFuture, StepSpec};
use super::with_context;
use crate::error::{Error, Result};
use crate::proto::boundary::v1::{self as b, call::Call, called::Called};

fn undos(called: Called) -> Result<Vec<b::Undo>> {
    match called {
        Called::Undos(u) => Ok(u.undos),
        other => Err(Error::Protocol(format!("expected undos, got {other:?}"))),
    }
}

impl Context {
    /// Runs `f` in a saga: on an error, the undos of the steps called through its context, most
    /// recent first, then the error again. Those steps are then off the stack of a later rollback.
    pub async fn saga<T, F, Fut>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Context) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let saga = match self.shared.call(Call::SagaBegin(b::Empty {}))? {
            Called::Saga(id) => id,
            other => return Err(Error::Protocol(format!("saga_begin answered {other:?}"))),
        };
        let scoped = Context::in_saga(self.shared.clone(), Some(saga));
        // Declared steps find their context there, so that they join the saga too.
        let result = with_context(scoped.clone(), f(scoped)).await;
        // Continuing as new is no failure: nothing is undone.
        // § one ctp0q370t.a@f50bk, ctp0q370t.b@s43d9
        let failed = matches!(&result, Err(e) if !matches!(e, Error::ContinuedAsNew(_)));
        let ended = self.shared.call(Call::SagaEnd(b::call::SagaEnd {
            saga,
            failed,
            parent: self.saga,
        }))?;
        // § ctp0q370t@c4akr
        match (failed, ended) {
            (true, ended) => self.run_undos(undos(ended)?).await?,
            (false, Called::Done(_)) => {}
            (false, other) => return Err(Error::Protocol(format!("saga_end answered {other:?}"))),
        }
        result
        // § end ctp0q370t
    }

    /// Runs the undos of every step so far, most recent first; return the error it gives to end
    /// the run failed with `RolledBack`: `return Err(c.rollback(..).await)`.
    pub async fn rollback(&self, reason: &str) -> Error {
        // § zjw5cess8@0nckf
        let undone = async {
            let undos = undos(self.shared.call(Call::Rollback(b::Empty {}))?)?;
            self.run_undos(undos).await
        };
        match undone.await {
            // § one dxgjekysz.c@nksq6
            Ok(()) => Error::RolledBack(reason.to_owned()),
            Err(e) => e,
        }
    }

    async fn run_undos(&self, undos: Vec<b::Undo>) -> Result<()> {
        for u in undos {
            let (undo, retry, timeout, heartbeat) = self.shared.undo(u.call).ok_or_else(|| {
                Error::Protocol(format!("undo of call {} without an undo", u.call))
            })?;
            // § a5c1mfs8d.a@mwhw0
            let options = b::StepOptions {
                retry,
                timeout_ms: Some(timeout),
                occurrence: Some(u.occurrence),
                ..Default::default()
            };
            // § end a5c1mfs8d.a
            let call = Call::Step(b::call::Step {
                name: u.name.clone(),
                options: Some(options),
            });
            let spec = StepSpec {
                body: undo(u.value),
                heartbeat,
                undo: None,
            };
            match CallFuture::new(self.shared.clone(), call, Some(spec)).await {
                Ok(_) => {}
                Err(Error::StepFailed { error, .. }) => {
                    // § j5gf2r47g@wfazw
                    return Err(Error::UndoFailed {
                        step: u.name,
                        error,
                    });
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}
