//! Waiting listings of the S3 adapter on `h3+http://`, against the fake h3 server of
//! `fake_h3`.

mod common;
mod fake_h3;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{Hidden, eventually, retry};
use fake_h3::{FakeH3, Listed, header, signed_headers};
use substantial::__internal::{self as internal, ListWait, Listing, Store};
use substantial::{Context, Preset, Substantial, Workflow};

async fn open(h3: &FakeH3) -> Arc<dyn Store> {
    let unique = hex::encode(rand::random::<[u8; 8]>());
    internal::open(&h3.url(&unique), &FakeH3::credentials(), retry()).unwrap()
}

fn wait(etag: Option<&str>, seconds: u32) -> Option<ListWait> {
    Some(ListWait {
        etag: etag.map(str::to_owned),
        seconds,
    })
}

fn last(h3: &FakeH3) -> Listed {
    h3.listed().pop().expect("a listing")
}

#[tokio::test]
async fn an_immediate_answer_carries_the_etag() {
    let h3 = FakeH3::start().await;
    let s = open(&h3).await;
    assert!(s.capabilities().wait);
    s.create("q/a", Bytes::from_static(b"a")).await.unwrap();
    let plain = s.list("q/", None, 10, None).await.unwrap();
    assert_eq!(plain.etag, None);
    assert_eq!(
        header(&last(&h3).headers, "x-substantial-wait-seconds"),
        None
    );

    let started = Instant::now();
    let first = s.list("q/", None, 10, wait(None, 0)).await.unwrap();
    assert_eq!(first.keys, ["q/a"]);
    let etag = first.etag.clone().expect("an etag");
    let headers = last(&h3).headers;
    assert_eq!(header(&headers, "x-substantial-wait-seconds"), Some("0"));
    assert_eq!(header(&headers, "x-substantial-wait-etag"), None);
    assert!(signed_headers(&headers).contains(&"x-substantial-wait-seconds".to_owned()));

    let again = s.list("q/", None, 10, wait(Some(&etag), 0)).await.unwrap();
    assert_eq!(again, first);
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_wait_is_answered_by_a_change() {
    let h3 = FakeH3::start().await;
    let s = open(&h3).await;
    s.create("q/a", Bytes::from_static(b"a")).await.unwrap();
    let etag = s.list("q/", None, 10, wait(None, 0)).await.unwrap().etag;
    let held = {
        let (s, etag) = (s.clone(), etag.clone());
        tokio::spawn(async move { s.list("q/", None, 10, wait(etag.as_deref(), 10)).await })
    };
    eventually("a held listing", async || h3.holds() == 1).await;
    let started = Instant::now();
    s.create("q/b", Bytes::from_static(b"b")).await.unwrap();
    let woken = held.await.unwrap().unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(woken.keys, ["q/a", "q/b"]);
    assert!(woken.etag.is_some() && woken.etag != etag);

    let listed = last(&h3);
    assert!(listed.woken);
    assert_eq!(
        header(&listed.headers, "x-substantial-wait-seconds"),
        Some("10")
    );
    assert_eq!(
        header(&listed.headers, "x-substantial-wait-etag"),
        etag.as_deref()
    );
    let signed = signed_headers(&listed.headers);
    for name in ["x-substantial-wait-etag", "x-substantial-wait-seconds"] {
        assert!(signed.contains(&name.to_owned()), "{name} in {signed:?}");
    }
}

#[tokio::test]
async fn a_wait_that_ends_unchanged_keeps_the_etag() {
    let h3 = FakeH3::start().await;
    let s = open(&h3).await;
    s.create("q/a", Bytes::from_static(b"a")).await.unwrap();
    s.create("other/x", Bytes::from_static(b"x")).await.unwrap();
    let before = s.list("q/", None, 10, wait(None, 0)).await.unwrap();
    let held = {
        let s = s.clone();
        let etag = before.etag.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            // Longer than the SDK's 5 s grace for stalled streams, which must not cut a wait.
            let listing = s.list("q/", None, 10, wait(etag.as_deref(), 6)).await;
            (listing, started.elapsed())
        })
    };
    eventually("a held listing", async || h3.holds() == 1).await;
    s.create("other/y", Bytes::from_static(b"y")).await.unwrap();
    let (after, elapsed): (Result<Listing, _>, _) = held.await.unwrap();
    assert_eq!(after.unwrap(), before);
    assert!(elapsed >= Duration::from_secs(6), "{elapsed:?}");
    assert!(!last(&h3).woken);
}

#[tokio::test]
async fn a_missing_etag_is_a_store_failure() {
    let h3 = FakeH3::start().await;
    let s = open(&h3).await;
    h3.omit_etag();
    let failure = s.list("q/", None, 10, wait(None, 0)).await.unwrap_err();
    assert!(
        failure.message.contains("x-substantial-listing-etag"),
        "{failure}"
    );
    assert_eq!(h3.listed().len(), 1, "a missing etag is not retried");
    assert!(s.list("q/", None, 10, None).await.is_ok());
}

#[tokio::test]
async fn waits_h3_cannot_answer_are_refused() {
    let h3 = FakeH3::start().await;
    let s = open(&h3).await;
    assert!(s.list("q/", None, 10, wait(None, 21)).await.is_err());
    assert!(s.list("q/", None, 1001, wait(None, 0)).await.is_err());
    assert!(h3.listed().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_from_another_client_comes_through_the_wait() {
    let h3 = FakeH3::start().await;
    let url = h3.url("e2e");
    let workflow = Workflow::without_input("answer", |_c| async { Ok(42) });
    let sub = Substantial::create(url.as_str())
        .partitions(1)
        .credentials(FakeH3::credentials())
        .await
        .unwrap();
    let worker = sub
        .worker(&workflow)
        .preset(Preset::Script)
        .set("max_idle", "5min")
        .hidden("wait", "20s")
        .start()
        .unwrap();
    eventually("a held listing", async || h3.holds() >= 1).await;

    let other = Substantial::open(url.as_str())
        .credentials(FakeH3::credentials())
        .await
        .unwrap();
    let started = Instant::now();
    let run = other.start(&workflow, ()).await.unwrap();
    let result = run.result().timeout(Duration::from_secs(10)).await;
    assert_eq!(result.unwrap(), 42);
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(h3.listed().iter().any(|l| l.woken));
    tokio::time::timeout(Duration::from_secs(30), worker.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[derive(serde::Serialize, serde::Deserialize)]
struct N {
    n: u32,
}

/// A client waits on each run's `result` key, through a continuation, instead of polling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_result_is_waited_on_its_key_through_a_continuation() {
    let h3 = FakeH3::start().await;
    let countdown = Workflow::new("countdown", |c: Context, input: N| async move {
        c.sleep(Duration::from_secs(1)).await?;
        if input.n > 0 {
            return Err(c.continue_as_new(N { n: input.n - 1 }).await);
        }
        Ok("liftoff".to_owned())
    });
    let sub = Substantial::create(h3.url("results").as_str())
        .partitions(1)
        .credentials(FakeH3::credentials())
        .await
        .unwrap();
    let worker = sub
        .worker(&countdown)
        .preset(Preset::Script)
        .start()
        .unwrap();
    let run = sub.start(&countdown, N { n: 1 }).await.unwrap();
    let value = run.result().timeout(Duration::from_secs(10)).await;
    assert_eq!(value.unwrap(), "liftoff");

    let waits: Vec<Listed> = h3
        .listed()
        .into_iter()
        .filter(|l| l.prefix.ends_with("/result"))
        .collect();
    let mut runs: Vec<&str> = waits.iter().map(|l| l.prefix.as_str()).collect();
    runs.dedup();
    assert_eq!(
        runs.len(),
        2,
        "the first run, then the one it continued into: {runs:?}"
    );
    assert!(
        waits.len() <= 4,
        "a listing for the etag, then one held, per run: {waits:?}"
    );
    assert!(
        waits
            .iter()
            .all(|l| header(&l.headers, "x-substantial-wait-seconds").is_some())
    );
    assert!(waits.iter().any(|l| l.woken), "{waits:?}");
    tokio::time::timeout(Duration::from_secs(30), worker.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_get_is_not_queued_behind_waiting_listings() {
    let h3 = FakeH3::start().await;
    let s = open(&h3).await;
    let first = s.list("p0/", None, 1, wait(None, 0)).await.unwrap();
    let etag = first.etag.expect("an etag");
    // As a worker serving 4 workflows on 16 partitions holds 64.
    let held: Vec<_> = (0..64)
        .map(|i| {
            let (s, etag) = (s.clone(), etag.clone());
            tokio::spawn(async move {
                s.list(&format!("p{i}/"), None, 1, wait(Some(&etag), 2))
                    .await
            })
        })
        .collect();
    eventually("64 held listings", async || h3.holds() >= 64).await;
    let started = Instant::now();
    assert_eq!(s.get("k").await.unwrap(), None);
    let took = started.elapsed();
    for h in held {
        h.await.unwrap().unwrap();
    }
    assert!(
        took < Duration::from_millis(500),
        "a get took {took:?} behind 64 held listings"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_ends_promptly_with_4_workflows_on_16_partitions() {
    let h3 = FakeH3::start().await;
    let flows: Vec<Workflow<u32>> = ["a", "b", "c", "d"]
        .into_iter()
        .map(|n| Workflow::without_input(&format!("pool-{n}"), |_c| async { Ok(1) }))
        .collect();
    let sub = Substantial::create(h3.url("pool").as_str())
        .partitions(16)
        .credentials(FakeH3::credentials())
        .await
        .unwrap();
    let worker = sub
        .worker(flows.iter().collect::<Vec<_>>())
        .preset(Preset::Script)
        .start()
        .unwrap();
    eventually("a held listing per workflow and partition", async || {
        h3.holds() >= 64
    })
    .await;
    let started = Instant::now();
    let run = sub.start(&flows[0], ()).await.unwrap();
    let value = run.result().timeout(Duration::from_secs(10)).await;
    assert_eq!(value.unwrap(), 1);
    let took = started.elapsed();
    assert!(took < Duration::from_secs(2), "a run took {took:?}");
    tokio::time::timeout(Duration::from_secs(30), worker.shutdown())
        .await
        .unwrap()
        .unwrap();
}
