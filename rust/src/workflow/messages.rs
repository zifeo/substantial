//! Handlers bound to a run's messages (I-9).

use std::panic::{AssertUnwindSafe, catch_unwind};

use serde::de::DeserializeOwned;

use super::context::Context;
use super::decode;
use super::scheduler::Handler;
use super::step::failure;
use crate::error::{Error, Result};
use crate::names::check_name;
use crate::proto::boundary::v1::{self as b, call::Call, called::Called};

fn handler<T, F>(mut f: F) -> Handler
where
    T: DeserializeOwned,
    F: FnMut(T) + Send + 'static,
{
    // § aeby9bgqd.a@24ben
    Box::new(move |m: &b::Message| {
        let failed = |why: String| {
            failure(
                "MessageDecode",
                format!("message {:?} {}: {why}", m.name, m.id),
                false,
            )
        };
        let value = decode(m.value.clone()).map_err(|e| failed(e.to_string()))?;
        catch_unwind(AssertUnwindSafe(|| f(value))).map_err(|panic| {
            let message = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("a panic without a message");
            failed(format!("the handler panicked: {message}"))
        })
    })
}

/// Handlers of several message names, bound together by [`Context::handle_all`].
#[derive(Default)]
pub struct Handlers(Vec<(String, Handler)>);

impl Handlers {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `f` as the handler of `name`; a message whose value does not decode as a `T`, or a
    /// panic of `f`, fails the run with `MessageDecode`.
    pub fn on<T, F>(mut self, name: &str, f: F) -> Self
    where
        T: DeserializeOwned,
        F: FnMut(T) + Send + 'static,
    {
        self.0.push((name.to_owned(), handler(f)));
        self
    }
}

impl Context {
    /// Binds `f` to the messages of `name`: every one consumed so far, in order, then each new one.
    /// A message whose value does not decode as a `T`, or a panic of `f`, fails the run with
    /// `MessageDecode`.
    pub fn handle<T, F>(&self, name: &str, f: F) -> Result<()>
    where
        T: DeserializeOwned,
        F: FnMut(T) + Send + 'static,
    {
        self.handle_all(Handlers::new().on(name, f))
    }

    /// Binds every handler of `handlers` at once: the messages of their names consumed so far, in
    /// the order they arrived across names, then each new one.
    pub fn handle_all(&self, handlers: Handlers) -> Result<()> {
        let Handlers(mut handlers) = handlers;
        let names: Vec<String> = handlers.iter().map(|(n, _)| n.clone()).collect();
        for name in &names {
            check_name("message name", name)?;
        }
        if let Some(name) = names
            .iter()
            .enumerate()
            .find_map(|(i, n)| names[..i].contains(n).then_some(n))
        {
            return Err(Error::InvalidArgument(format!(
                "{name:?} is handled twice in one handle_all"
            )));
        }
        let call = Call::Bind(b::call::Bind { names });
        let past = match self.shared.call(call)? {
            Called::Messages(m) => m.messages,
            other => return Err(Error::Protocol(format!("bind answered {other:?}"))),
        };
        // § aeby9bgqd@rcys9
        for m in &past {
            let (_, h) = handlers
                .iter_mut()
                .find(|(n, _)| *n == m.name)
                .ok_or_else(|| Error::Protocol(format!("bind answered message {:?}", m.name)))?;
            if let Err(f) = h(m) {
                self.shared.fail_code(f.clone());
                return Err(Error::Failed(f));
            }
        }
        for (name, h) in handlers {
            self.shared.add_handler(name, h);
        }
        // § end aeby9bgqd
        Ok(())
    }
}
