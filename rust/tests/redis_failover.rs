//! Failovers during a stream of creates: the primary holding the namespace is killed, a replica
//! takes over, creates are acknowledged again, and every create acknowledged before or during it
//! is there after, none acknowledged twice, on the containers of `dev/compose.yaml`, which a test
//! fails without; each test starts the killed container again and waits for it to rejoin as a
//! replica.

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{UP, failover_lock, redis_url, retry};
use substantial::__internal::{Create, RedisLocation, RedisStore, Store};
use substantial::Credentials;

const WRITERS_PER_KEY: usize = 2;
const KEYS_AT_ONCE: usize = 4;
/// Creates acknowledged before the kill, and again among those sent after the promotion: counts,
/// not durations, so that a loaded machine makes the test slower, never failing.
const ACKS: u32 = 50;
const WITHIN: Duration = Duration::from_secs(60);

/// The container `name`, which the test fails without.
fn container(name: &str) -> String {
    let found = std::process::Command::new("docker")
        .args(["inspect", name])
        .output();
    match found {
        Ok(out) if out.status.success() => name.to_owned(),
        Ok(_) => panic!("no container {name}: run `{UP}`"),
        Err(e) => panic!("no docker: {e}"),
    }
}

fn docker(args: &[&str]) {
    let status = std::process::Command::new("docker")
        .args(args)
        .stdout(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(status.success(), "docker {}", args.join(" "));
}

/// Starts a killed container again when dropped, however the test ends.
struct Restart(String);

impl Drop for Restart {
    fn drop(&mut self) {
        docker(&["start", &self.0]);
    }
}

async fn until(what: &str, mut check: impl AsyncFnMut() -> bool, within: Duration) {
    let deadline = Instant::now() + within;
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn connection(at: &str) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
    let client = redis::Client::open(format!("redis://{at}"))?;
    let config = redis::AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(1)))
        .set_response_timeout(Some(Duration::from_secs(1)));
    client
        .get_multiplexed_async_connection_with_config(&config)
        .await
}

/// `INFO replication` of the server at `at`.
async fn replication(at: &str) -> redis::RedisResult<redis::InfoDict> {
    let mut conn = connection(at).await?;
    let info: String = redis::cmd("INFO")
        .arg("replication")
        .query_async(&mut conn)
        .await?;
    Ok(redis::InfoDict::new(&info))
}

/// `role:master_link_status` of the server at `at`.
async fn role(at: &str) -> Option<String> {
    let info = replication(at).await.ok()?;
    let role: String = info.get("role")?;
    let link: String = info.get("master_link_status").unwrap_or_default();
    Some(format!("{role}:{link}"))
}

async fn rejoined(at: &str) -> bool {
    role(at).await.as_deref() == Some("slave:up")
}

type Answers = Mutex<HashMap<String, Vec<(Create, Bytes)>>>;

/// Creates keys, each by two writers at once with their own bytes, while the primary is killed,
/// until `ACKS` creates sent after the promotion are acknowledged, then checks each answer against
/// the store once the old primary is back. `rejoin_first` starts the killed container right after
/// the promotion, for a primary with one replica, which acknowledges no create alone (I-26).
async fn stream_through_failover(
    url: &str,
    prefix: &str,
    mut failed_over: impl AsyncFnMut(&str) -> bool,
    rejoin_first: bool,
) {
    let location = RedisLocation::parse(url).unwrap();
    let store = Arc::new(RedisStore::open(location.clone(), &Credentials::None, retry()).unwrap());
    Store::open(&*store).await.unwrap();
    let at = store.primary_address().unwrap();
    let port = at.rsplit_once(':').unwrap().1;
    let killed = container(&format!("{prefix}-{port}"));
    let answers = Arc::new(Answers::default());
    let failures = Arc::new(AtomicU32::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let promoted = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicU32::new(0));
    let acked_after = Arc::new(AtomicU32::new(0));
    let writers: Vec<_> = (0..KEYS_AT_ONCE)
        .flat_map(|slot| (0..WRITERS_PER_KEY).map(move |w| (slot, w)))
        .map(|(slot, w)| {
            let (store, answers) = (store.clone(), answers.clone());
            let (failures, stop, promoted) = (failures.clone(), stop.clone(), promoted.clone());
            let (acked, acked_after) = (acked.clone(), acked_after.clone());
            tokio::spawn(async move {
                let mut n = 0;
                while !stop.load(Ordering::SeqCst) {
                    let key = format!("k/{slot}-{n:06}");
                    n += 1;
                    let data = Bytes::from(format!("{key} by {w}"));
                    let after = promoted.load(Ordering::SeqCst);
                    match store.create(&key, data.clone(), None).await {
                        Ok(outcome) => {
                            if matches!(outcome, Create::Created { .. }) {
                                acked.fetch_add(1, Ordering::SeqCst);
                                if after {
                                    acked_after.fetch_add(1, Ordering::SeqCst);
                                }
                            }
                            answers
                                .lock()
                                .unwrap()
                                .entry(key)
                                .or_default()
                                .push((outcome, data));
                        }
                        Err(_) => {
                            failures.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            })
        })
        .collect();
    until(
        "creates acknowledged before the kill",
        async || acked.load(Ordering::SeqCst) >= ACKS,
        WITHIN,
    )
    .await;
    let restart = Restart(killed.clone());
    docker(&["kill", &killed]);
    let killed_at = Instant::now();
    until(
        "a replica promoted",
        async || failed_over(&at).await,
        WITHIN,
    )
    .await;
    let promoted_in = killed_at.elapsed();
    promoted.store(true, Ordering::SeqCst);
    if rejoin_first {
        docker(&["start", &killed]);
    }
    until(
        "creates acknowledged after the promotion",
        async || acked_after.load(Ordering::SeqCst) >= ACKS,
        WITHIN,
    )
    .await;
    let resumed_in = killed_at.elapsed() - promoted_in;
    stop.store(true, Ordering::SeqCst);
    for writer in writers {
        writer.await.unwrap();
    }
    drop(restart);
    drop(store);
    until(
        "the old primary back as a replica",
        async || rejoined(&at).await,
        Duration::from_secs(60),
    )
    .await;

    let check = RedisStore::open(location, &Credentials::None, retry()).unwrap();
    let mut acknowledged = 0;
    let answers = std::mem::take(&mut *answers.lock().unwrap());
    for (key, outcomes) in answers {
        let created: Vec<_> = outcomes
            .iter()
            .filter(|(outcome, _)| matches!(outcome, Create::Created { .. }))
            .map(|(_, data)| data.clone())
            .collect();
        assert!(created.len() <= 1, "{key} created twice: {outcomes:?}");
        let stored = check.get(&key).await.unwrap().map(|o| o.bytes);
        if let Some(created) = created.first() {
            acknowledged += 1;
            assert_eq!(
                stored.as_ref(),
                Some(created),
                "{key} acknowledged as {created:?}, holds {stored:?}"
            );
        } else if !outcomes.is_empty() {
            assert!(
                stored.is_some(),
                "{key} answered exists, then missing: {outcomes:?}"
            );
        }
    }
    println!(
        "{acknowledged} acknowledged, {} store failures, a replica promoted {:.1} s after the \
         kill, {ACKS} creates sent after it acknowledged {:.1} s later",
        failures.load(Ordering::SeqCst),
        promoted_in.as_secs_f64(),
        resumed_in.as_secs_f64()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sentinel_failover_keeps_every_acknowledged_create() {
    let _alone = failover_lock(true);
    let url = redis_url("redis-sentinel");
    let location = RedisLocation::parse(&url).unwrap();
    let (host, port) = location.nodes[0].clone();
    let master = location.master.clone().unwrap();
    let failed_over = async |old: &str| {
        let Ok(mut sentinel) = connection(&format!("{host}:{port}")).await else {
            return false;
        };
        let answer: redis::RedisResult<(String, u16)> = redis::cmd("SENTINEL")
            .arg("GET-MASTER-ADDR-BY-NAME")
            .arg(&master)
            .query_async(&mut sentinel)
            .await;
        answer.is_ok_and(|(h, p)| format!("{h}:{p}") != old)
    };
    stream_through_failover(&url, "substantial-sentinel", failed_over, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_failover_keeps_every_acknowledged_create() {
    let _alone = failover_lock(true);
    let url = redis_url("redis-cluster");
    let location = RedisLocation::parse(&url).unwrap();
    let store = RedisStore::open(location, &Credentials::None, retry()).unwrap();
    Store::open(&store).await.unwrap();
    let primary = store.primary_address().unwrap();
    drop(store);
    let info = replication(&primary).await.unwrap();
    let replica_line: String = info.get("slave0").unwrap();
    let field = |name: &str| {
        replica_line
            .split(',')
            .find_map(|f| f.strip_prefix(name))
            .unwrap()
            .to_owned()
    };
    let replica = format!("{}:{}", field("ip="), field("port="));
    let failed_over = async |_old: &str| {
        role(&replica)
            .await
            .is_some_and(|r| r.starts_with("master"))
    };
    stream_through_failover(&url, "substantial-cluster", failed_over, true).await;
}
