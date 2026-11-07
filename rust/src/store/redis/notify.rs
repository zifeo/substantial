//! The notifications behind the waiting listings of the Redis store.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ::redis::{PushKind, Value};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use super::connect::{CONNECT_TIMEOUT, Failure};
use super::store::Shared;
use crate::store::retry::Fault;

/// A subscription silent this long gets a PING; one silent twice as long is lost.
const PING_EVERY: Duration = Duration::from_secs(10);

/// The store's one subscription to the channel of its namespace, where every create and delete
/// publishes its key, demultiplexed to the waiting listings whose prefix the key has.
///
/// A lost subscription wakes every listing waiting on it, so each lists again once subscribed
/// anew: no notification is missed between a listing's page and its wait.
pub(super) struct Notifications {
    channel: String,
    sharded: bool,
    waiters: Mutex<HashMap<u64, (String, Arc<Notify>)>>,
    next: AtomicU64,
    ready: watch::Sender<bool>,
    error: Mutex<String>,
    task: Mutex<Option<JoinHandle<()>>>,
}

/// A wait registered under a prefix, removed when dropped.
pub(super) struct Watch<'a> {
    notifications: &'a Notifications,
    id: u64,
    notify: Arc<Notify>,
}

impl Watch<'_> {
    /// Returns once a key under the prefix was created or deleted, or the subscription was lost,
    /// since the watch was made.
    pub(super) async fn woken(&self) {
        self.notify.notified().await
    }
}

impl Drop for Watch<'_> {
    fn drop(&mut self) {
        self.notifications
            .waiters
            .lock()
            .expect("never poisoned")
            .remove(&self.id);
    }
}

impl Notifications {
    pub(super) fn new(channel: String, sharded: bool) -> Self {
        Notifications {
            channel,
            sharded,
            waiters: Mutex::default(),
            next: AtomicU64::new(0),
            ready: watch::Sender::new(false),
            error: Mutex::new("not subscribed yet".into()),
            task: Mutex::default(),
        }
    }

    /// A wait woken once a key under `prefix` is created or deleted, or the subscription is lost,
    /// from now on.
    pub(super) async fn watch<'a>(
        &'a self,
        shared: &Arc<Shared>,
        prefix: &str,
    ) -> Result<Watch<'a>, Fault> {
        {
            let mut task = self.task.lock().expect("never poisoned");
            if task.is_none() {
                *task = Some(tokio::spawn(run(shared.clone())));
            }
        }
        // Registered before the subscription is known ready, so a loss in between wakes it.
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let notify = Arc::new(Notify::new());
        self.waiters
            .lock()
            .expect("never poisoned")
            .insert(id, (prefix.to_owned(), notify.clone()));
        let watch = Watch {
            notifications: self,
            id,
            notify,
        };
        let mut ready = self.ready.subscribe();
        if tokio::time::timeout(2 * CONNECT_TIMEOUT, ready.wait_for(|r| *r))
            .await
            .is_err()
        {
            let error = self.error.lock().expect("never poisoned").clone();
            return Err(Fault::Transient(format!(
                "no subscription to {}: {error}",
                self.channel
            )));
        }
        Ok(watch)
    }

    fn wake(&self, key: Option<&str>) {
        for (prefix, notify) in self.waiters.lock().expect("never poisoned").values() {
            if key.is_none_or(|k| k.starts_with(prefix.as_str())) {
                notify.notify_one();
            }
        }
    }

    pub(super) fn stop(&self) {
        if let Some(task) = self.task.lock().expect("never poisoned").take() {
            task.abort();
        }
    }

    /// Listens on one subscription until it is lost; whether it was subscribed, with why it was
    /// lost.
    async fn listen(&self, shared: &Shared) -> (bool, Failure) {
        let primary = match tokio::time::timeout(2 * CONNECT_TIMEOUT, shared.primary()).await {
            Ok(Ok(primary)) => primary,
            Ok(Err(e)) => return (false, e),
            Err(_) => {
                let why = format!("no primary within {} s", 2 * CONNECT_TIMEOUT.as_secs());
                return (false, Fault::Transient(why).into());
            }
        };
        let (subscribed, e) = self.subscribed(shared, &primary).await;
        shared.lost(&primary);
        (subscribed, e)
    }

    async fn subscribed(
        &self,
        shared: &Shared,
        primary: &super::connect::Primary,
    ) -> (bool, Failure) {
        let (push, mut pushed) = tokio::sync::mpsc::unbounded_channel();
        let connector = &shared.connector;
        let opened = tokio::time::timeout(
            CONNECT_TIMEOUT,
            connector.connection(
                &primary.host,
                primary.port,
                connector.location.db,
                true,
                Some(push),
            ),
        )
        .await;
        let mut conn = match opened {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => return (false, e),
            Err(_) => {
                return (
                    false,
                    Fault::Transient("no connection within 5 s".into()).into(),
                );
            }
        };
        let command = if self.sharded {
            "SSUBSCRIBE"
        } else {
            "SUBSCRIBE"
        };
        let subscribe = ::redis::cmd(command).arg(&self.channel).to_owned();
        let reply =
            tokio::time::timeout(CONNECT_TIMEOUT, subscribe.query_async::<Value>(&mut conn)).await;
        match reply {
            Ok(Ok(Value::Push { .. })) => {}
            Ok(Ok(other)) => {
                let why = format!("{command} {} answered {other:?}", self.channel);
                return (false, Fault::Transient(why).into());
            }
            Ok(Err(e)) => return (false, e.into()),
            Err(_) => {
                let why = format!("{command} {} answered nothing in 5 s", self.channel);
                return (false, Fault::Transient(why).into());
            }
        }
        self.ready.send_replace(true);
        loop {
            let message = match tokio::time::timeout(PING_EVERY, pushed.recv()).await {
                Ok(message) => message,
                Err(_) => {
                    let ping = ::redis::cmd("PING").to_owned();
                    match tokio::time::timeout(PING_EVERY, ping.query_async::<Value>(&mut conn))
                        .await
                    {
                        Ok(Ok(_)) => continue,
                        Ok(Err(e)) => return (true, e.into()),
                        Err(_) => {
                            let why = format!(
                                "the subscription answered no PING in {} s",
                                PING_EVERY.as_secs()
                            );
                            return (true, Fault::Transient(why).into());
                        }
                    }
                }
            };
            let Some(message) = message else {
                return (
                    true,
                    Fault::Transient("the connection closed".into()).into(),
                );
            };
            match message.kind {
                PushKind::Message | PushKind::SMessage => {
                    let key = message
                        .data
                        .get(1)
                        .and_then(|k| ::redis::from_redis_value_ref::<String>(k).ok());
                    match key {
                        Some(key) => self.wake(Some(&key)),
                        None => self.wake(None),
                    }
                }
                // Confirmations reach the push sender too.
                PushKind::Subscribe | PushKind::SSubscribe => {}
                PushKind::Disconnection => {
                    return (
                        true,
                        Fault::Transient("the connection closed".into()).into(),
                    );
                }
                // An unsubscribe the server sent, as when the slot moved to another node.
                other => {
                    let why = format!("the subscription ended with {other} {:?}", message.data);
                    return (true, Fault::Transient(why).into());
                }
            }
        }
    }
}

/// Subscribes, and subscribes again once lost, until the store is dropped.
async fn run(shared: Arc<Shared>) {
    let notifications = &shared.notifications;
    let mut delay = shared.retry.initial;
    loop {
        let (subscribed, e) = notifications.listen(&shared).await;
        let error = match e {
            Failure::Fault(Fault::Transient(m) | Fault::Ambiguous(m) | Fault::Permanent(m)) => m,
            Failure::Config(m) | Failure::Provider(m) => m,
            Failure::Redis(e) => e.to_string(),
        };
        tracing::debug!("subscription to {} lost: {error}", notifications.channel);
        *notifications.error.lock().expect("never poisoned") = error;
        notifications.ready.send_replace(false);
        notifications.wake(None);
        if subscribed {
            delay = shared.retry.initial;
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(shared.retry.max);
    }
}
