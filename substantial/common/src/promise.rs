use std::{
    any::Any,
    borrow::{Borrow, BorrowMut},
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
};

use crate::wit::{metatype::substantial::host, utils::PromisedResult};

pub struct Promise {
    pub ref_host: String,
    pub ref_guest: u32,
}

type DynF = Box<dyn Fn(Box<dyn Any>) -> Box<dyn Any> + Send + Sync>;

thread_local! {
    pub static PENDING: RefCell<HashMap<u32, Vec<DynF>>> = RefCell::new(HashMap::new());
}

impl From<host::Promise> for Promise {
    fn from(promise: host::Promise) -> Self {
        PENDING.with_borrow_mut(|m| {
            let ref_host = promise.ref_host;
            let ref_guest = m.len() as u32 + 1;
            m.insert(ref_guest, Vec::new());
            Promise {
                ref_host,
                ref_guest,
            }
        })
    }
}

impl From<Promise> for PromisedResult {
    fn from(promise: Promise) -> Self {
        PromisedResult {
            ref_host: promise.ref_host,
            ref_guest: promise.ref_guest,
        }
    }
}

#[macro_export]
macro_rules! downcast {
    ($value:expr, $type:ty) => {{
        match $value.downcast::<$type>() {
            Ok(v) => Ok(v),
            Err(_) => Err(format!("Cannot cast into {}", stringify!($type))),
        }
    }};
}

impl Promise {
    pub fn map<F, T>(self, f: F) -> Self
    where
        F: Fn(T) -> T + 'static + Send + Sync,
        T: 'static + Any, // always known at compile time
    {
        PENDING.with_borrow_mut(|m| {
            m.entry(self.ref_guest).and_modify(|v| {
                v.push(Box::new(move |input: Box<dyn Any>| -> Box<dyn Any> {
                    let input = downcast!(input, T).expect("Fatal: type does not match");
                    let output = f(*input);
                    Box::new(output)
                }));
            });
            self
        })
    }

    pub fn resolve(id: u32, input: Box<dyn Any>) -> Result<Box<dyn Any>, String> {
        PENDING.with_borrow_mut(|m| {
            let chain = m
                .get_mut(&id)
                .ok_or_else(|| format!("Fatal: pending promise id={id} not found"))?;
            let ret = chain.iter().fold(input, |prev, f| f(prev));
            Ok(ret)
        })
    }
}
