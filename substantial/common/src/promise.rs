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

impl Promise {
    pub fn map<F, T>(self, f: F) -> Self
    where
        F: Fn(T) -> T + 'static + Send + Sync,
        T: 'static + Any, // always known at compile time
    {
        PENDING.with_borrow_mut(|m| {
            m.entry(self.ref_guest).and_modify(|v| {
                v.push(Box::new(move |input: Box<dyn Any>| -> Box<dyn Any> {
                    let input = input.downcast::<T>().expect("Fatal: type does not match");
                    let output = f(*input);
                    Box::new(output)
                }));
            });
            self
        })
    }

    pub fn resolve<T>(id: u32, input: Box<dyn Any>) -> Box<dyn Any> {
        PENDING.with_borrow_mut(|m| {
            let chain = m.get_mut(&id).unwrap();
            chain.iter().fold(input, |prev, f| f(prev))
        })
    }
}
