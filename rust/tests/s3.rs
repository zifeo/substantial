//! What the S3 adapter does beyond the shared contract; all but URL parsing run only when
//! `SUBSTANTIAL_S3_URL` is set (see `dev/rustfs.compose.yaml`).

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use common::{retry, s3_url};
use futures::{StreamExt, TryStreamExt};
use substantial::__internal::{self as internal, Create, ListWait, S3Location};
use substantial::{Credentials, Error, Keys, Substantial};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn urls_need_every_part() {
    let ok = S3Location::parse("h3+http://127.0.0.1:9/b/ns/sub?region=r").unwrap();
    assert_eq!(
        ok,
        S3Location {
            endpoint: "http://127.0.0.1:9".into(),
            bucket: "b".into(),
            namespace: "ns/sub".into(),
            region: "r".into(),
            wait: true,
        }
    );
    let aws = "s3://s3.eu-central-1.amazonaws.com/b/ns?region=eu-central-1";
    let aws = S3Location::parse(aws).unwrap();
    assert_eq!(aws.endpoint, "https://s3.eu-central-1.amazonaws.com");
    assert!(!aws.wait);
    let ipv6 = S3Location::parse("s3+http://[::1]:9000/b/ns?region=r").unwrap();
    assert_eq!(ipv6.endpoint, "http://[::1]:9000");
    assert!(S3Location::parse("h3://h/b/ns?region=r").unwrap().wait);
    for url in [
        "s3://h/b/ns",
        "s3://h/b/ns?region=",
        "s3://h/b/ns?region=r&region=s",
        "s3://h/b/ns?region=us_east",
        "s3://h/b/ns?region=US-EAST-1",
        "s3://h/b/ns?region=-r",
        "s3://h/b/ns?region=r-",
        "s3://h/b/ns?region=us%20east",
        "s3://h/b/ns?region=r&endpoint=http://h",
        "s3://h/b/ns?region=r&wait=true",
        "s3://h/b?region=r",
        "s3://h/b/?region=r",
        "s3://h/?region=r",
        "s3://h?region=r",
        "s3://h//ns?region=r",
        "s3://h/b/ns/?region=r",
        "s3://h/b/a//c?region=r",
        "s3:///b/ns?region=r",
        "s3://k:s@h/b/ns?region=r",
        "s3://@h/b/ns?region=r",
        "s3://:@h/b/ns?region=r",
        "s3://h/b/ns?region=r#x",
        "s3+https://h/b/ns?region=r",
        "gs://h/b/ns?region=r",
    ] {
        assert!(
            matches!(S3Location::parse(url), Err(Error::ConfigError(_))),
            "{url}"
        );
    }
    let long = format!("s3://h/b/ns?region={}", "r".repeat(64));
    assert!(matches!(
        S3Location::parse(&long),
        Err(Error::ConfigError(_))
    ));
    let longest = format!("s3://h/b/ns?region={}", "r".repeat(63));
    assert_eq!(S3Location::parse(&longest).unwrap().region, "r".repeat(63));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_and_open_run_the_probe() {
    let Some(url) = s3_url("s3") else { return };
    let env = &Credentials::Env;
    assert!(matches!(
        Substantial::open(&url).credentials(Credentials::Env).await,
        Err(Error::LayoutError(_))
    ));
    assert!(matches!(
        Substantial::create(&url).partitions(2).await,
        Err(Error::ConfigError(_))
    ));
    let created = Substantial::create(&url)
        .partitions(2)
        .credentials(Credentials::Env)
        .await
        .unwrap();
    assert_eq!(created.partitions(), 2);
    let opened = Substantial::open(&url)
        .credentials(Credentials::Env)
        .await
        .unwrap();
    assert_eq!(opened.partitions(), 2);
    let s = internal::open(&url, env, retry()).unwrap();
    let probes = s.list("probe/", None, 10, None).await.unwrap().keys;
    assert_eq!(probes, ["probe/exclusive"], "one probe key, kept");
}

#[tokio::test]
async fn wrong_credentials_are_a_store_error() {
    let Some(url) = s3_url("s3") else { return };
    let wrong = Credentials::S3 {
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID").unwrap(),
        secret_access_key: "not-the-secret".into(),
        session_token: None,
    };
    match Substantial::create(&url)
        .partitions(1)
        .credentials(wrong)
        .await
    {
        Err(Error::StoreError(m)) => {
            assert!(m.contains("HTTP 403 SignatureDoesNotMatch: "), "{m}");
            assert!(!m.contains('<'), "the answer's XML is read: {m}");
        }
        Err(e) => panic!("{e}"),
        Ok(_) => panic!("created with a wrong secret"),
    }
}

#[tokio::test]
async fn a_missing_bucket_is_a_store_error_not_a_missing_key() {
    let Some(url) = s3_url("s3") else { return };
    let (endpoint, rest) = url.split_once("://").unwrap().1.split_once('/').unwrap();
    let (_, path) = rest.split_once('/').unwrap();
    let scheme = url.split_once("://").unwrap().0;
    let missing = format!("{scheme}://{endpoint}/substantial-no-such-bucket/{path}");
    let s = internal::open(&missing, &Credentials::Env, retry()).unwrap();
    assert!(s.get("runs/a/start").await.is_err());
    assert!(s.delete("runs/a/start").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listings_go_past_one_page() {
    let Some(url) = s3_url("s3") else { return };
    let s = internal::open(&url, &Credentials::Env, retry()).unwrap();
    let keys: Vec<String> = (0..1005).map(|i| format!("due/{i:05}")).collect();
    futures::stream::iter(&keys)
        .map(|k| s.create(k, Bytes::new()))
        .buffer_unordered(32)
        .try_for_each(|c| async move {
            assert_eq!(c, Create::Created);
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(s.list("due/", None, 2000, None).await.unwrap().keys, keys);
    let after = s.list("due/", Some("due/00002"), 1001, None).await.unwrap();
    assert_eq!(after.keys, keys[3..1004]);
    assert_eq!(s.list("due/", None, 0, None).await.unwrap().keys.len(), 0);
}

#[tokio::test]
async fn h3_on_a_plain_s3_server_fails_waiting_listings_for_want_of_an_etag() {
    let Some(url) = s3_url("h3") else { return };
    let s = internal::open(&url, &Credentials::Env, retry()).unwrap();
    assert!(s.capabilities().wait);
    s.create("q/a", Bytes::from_static(b"a")).await.unwrap();
    assert_eq!(s.list("q/", None, 10, None).await.unwrap().keys, ["q/a"]);
    let wait = Some(ListWait {
        etag: None,
        seconds: 1,
    });
    let failure = s.list("q/", None, 10, wait).await.unwrap_err();
    assert!(
        failure.message.contains("x-substantial-listing-etag"),
        "{failure}"
    );
}

/// The test keys from a provider that counts its calls; its first `failing` calls fail.
fn counted(expires_in: Option<TimeDelta>, failing: u32) -> (Credentials, Arc<AtomicU32>) {
    let calls = Arc::new(AtomicU32::new(0));
    let counter = calls.clone();
    let provider = Credentials::provider(move || {
        let call = counter.fetch_add(1, Ordering::SeqCst);
        std::future::ready(if call < failing {
            Err("no keys yet")
        } else {
            Ok(Keys {
                access_key_id: std::env::var("AWS_ACCESS_KEY_ID").unwrap(),
                secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").unwrap(),
                session_token: None,
                expires_at: expires_in.map(|d| DateTime::<Utc>::from(SystemTime::now()) + d),
            })
        })
    });
    (provider, calls)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_credentials_provider_is_called_again_near_expiry() {
    let Some(url) = s3_url("s3") else { return };
    let (provider, calls) = counted(Some(TimeDelta::hours(1)), 0);
    let sub = Substantial::create(&url)
        .partitions(1)
        .credentials(provider)
        .await
        .unwrap();
    assert_eq!(sub.partitions(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "called at open, then kept");

    let four_requests = async |s: &dyn internal::Store| {
        s.create("k", Bytes::from_static(b"v")).await.unwrap();
        s.get("k").await.unwrap();
        s.list("", None, 10, None).await.unwrap();
        s.delete("k").await.unwrap();
    };
    for (expires_in, expected) in [
        (Some(TimeDelta::minutes(1)), 4),
        (Some(TimeDelta::hours(1)), 1),
        (None, 1),
    ] {
        let url = s3_url("s3").unwrap();
        let (provider, calls) = counted(expires_in, 0);
        let s = internal::open(&url, &provider, retry()).unwrap();
        four_requests(&*s).await;
        assert_eq!(calls.load(Ordering::SeqCst), expected, "{expires_in:?}");
    }

    let (provider, calls) = counted(None, 1);
    let s = internal::open(&url, &provider, retry()).unwrap();
    let failure = s.get("k").await.unwrap_err();
    assert_eq!(failure.message, "credentials provider: no keys yet");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the request fails without retries"
    );
    assert_eq!(s.get("k").await.unwrap(), None);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the next request calls it again"
    );
}

#[tokio::test(start_paused = true)]
async fn a_provider_that_never_answers_fails_each_attempt_within_its_timeout() {
    let calls = Arc::new(AtomicU32::new(0));
    let counter = calls.clone();
    let provider = Credentials::provider(move || {
        counter.fetch_add(1, Ordering::SeqCst);
        std::future::pending::<Result<Keys, &str>>()
    });
    let s = internal::open("s3+http://127.0.0.1:1/b/ns?region=r", &provider, retry()).unwrap();
    let got = tokio::time::timeout(Duration::from_secs(3600), s.get("k")).await;
    let failure = got.expect("the attempts end").unwrap_err();
    assert_eq!(
        failure.message,
        "credentials provider: no keys within 60000 ms (after 5 attempts)"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        5,
        "asked again at each attempt"
    );
}

/// A server on 127.0.0.1 answering every request with `answer`, then closing the connection;
/// returns its URL and the method of every request.
async fn raw_server(answer: &'static [u8]) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let methods = Arc::new(Mutex::new(Vec::new()));
    let seen = methods.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut buf = [0u8; 4096];
            while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut buf).await.unwrap();
                head.extend_from_slice(&buf[..n]);
            }
            let method = String::from_utf8_lossy(&head)
                .split(' ')
                .next()
                .unwrap()
                .to_owned();
            seen.lock().unwrap().push(method);
            socket.write_all(answer).await.unwrap();
            socket.shutdown().await.unwrap();
        }
    });
    (
        format!("s3+http://127.0.0.1:{port}/b/ns?region=us-east-1"),
        methods,
    )
}

fn fake() -> Credentials {
    Credentials::S3 {
        access_key_id: "fake".into(),
        secret_access_key: "fake-secret".into(),
        session_token: None,
    }
}

#[tokio::test]
async fn a_body_cut_short_is_retried_as_transient() {
    let (url, methods) = raw_server(b"HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\nabc").await;
    let s = internal::open(&url, &fake(), retry()).unwrap();
    let failure = s.get("k").await.unwrap_err();
    assert!(
        failure.message.ends_with("(after 5 attempts)"),
        "{failure:?}"
    );
    assert_eq!(*methods.lock().unwrap(), ["GET"; 5]);
}

#[tokio::test]
async fn a_listing_without_is_truncated_is_complete() {
    let (url, methods) = raw_server(
        b"HTTP/1.1 200 OK\r\ncontent-length: 113\r\n\r\n<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
          <ListBucketResult><Contents><Key>ns/q/a</Key></Contents></ListBucketResult>",
    )
    .await;
    let s = internal::open(&url, &fake(), retry()).unwrap();
    let listing = s.list("q/", None, 10, None).await.unwrap();
    assert_eq!(listing.keys, ["q/a"]);
    assert_eq!(*methods.lock().unwrap(), ["GET"]);
}
