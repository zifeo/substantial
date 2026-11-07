//! A fake h3 server on 127.0.0.1: the S3 calls of the store contract on one in-memory bucket, its
//! listings held on the wait headers, and the headers of every listing recorded.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::HeaderMap;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use substantial::Credentials;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::Instant;

const BUCKET: &str = "h3";

/// A listing as the fake received it.
#[derive(Clone, Debug)]
pub struct Listed {
    pub prefix: String,
    pub headers: HeaderMap,
    /// Held on its wait etag, then answered with another page: a change ended the wait.
    pub woken: bool,
}

pub struct FakeH3 {
    addr: SocketAddr,
    state: Arc<State>,
}

struct State {
    objects: Mutex<BTreeMap<String, Bytes>>,
    changes: watch::Sender<u64>,
    listed: Mutex<Vec<Listed>>,
    holds: AtomicUsize,
    omit_etag: AtomicBool,
}

type Answer = Response<Full<Bytes>>;

impl FakeH3 {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(State {
            objects: Mutex::default(),
            changes: watch::Sender::new(0),
            listed: Mutex::default(),
            holds: AtomicUsize::new(0),
            omit_etag: AtomicBool::new(false),
        });
        let served = state.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let state = served.clone();
                let service = service_fn(move |request| handle(state.clone(), request));
                tokio::spawn(http1::Builder::new().serve_connection(TokioIo::new(stream), service));
            }
        });
        Self { addr, state }
    }

    pub fn url(&self, namespace: &str) -> String {
        format!(
            "h3+http://{}/{BUCKET}/{namespace}?region=us-east-1",
            self.addr
        )
    }

    pub fn credentials() -> Credentials {
        Credentials::S3 {
            access_key_id: "fake".into(),
            secret_access_key: "fake-secret".into(),
            session_token: None,
        }
    }

    pub fn listed(&self) -> Vec<Listed> {
        self.state.listed.lock().unwrap().clone()
    }

    /// How many listings were held so far.
    pub fn holds(&self) -> usize {
        self.state.holds.load(Ordering::SeqCst)
    }

    /// Answers waiting listings without their etag, as a server whose listings cannot wait.
    pub fn omit_etag(&self) {
        self.state.omit_etag.store(true, Ordering::SeqCst);
    }
}

/// The headers `SignedHeaders` of the request's SigV4 `Authorization` names.
pub fn signed_headers(headers: &HeaderMap) -> Vec<String> {
    let authorization = headers["authorization"].to_str().unwrap();
    let signed = authorization
        .split(',')
        .find_map(|part| part.trim().strip_prefix("SignedHeaders="))
        .unwrap_or_else(|| panic!("no SignedHeaders in {authorization:?}"));
    signed.split(';').map(str::to_owned).collect()
}

pub fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).map(|v| v.to_str().unwrap())
}

async fn handle(state: Arc<State>, request: Request<Incoming>) -> Result<Answer, Infallible> {
    let (parts, body) = request.into_parts();
    let Ok(body) = body.collect().await.map(|b| b.to_bytes()) else {
        return Ok(error(StatusCode::BAD_REQUEST, "IncompleteBody"));
    };
    let path = decode(parts.uri.path());
    let query: BTreeMap<String, String> =
        url::form_urlencoded::parse(parts.uri.query().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
    let Some(rest) = path.strip_prefix(&format!("/{BUCKET}")) else {
        return Ok(error(StatusCode::NOT_FOUND, "NoSuchBucket"));
    };
    let key = rest.strip_prefix('/').filter(|k| !k.is_empty());
    let listing = query.get("list-type").is_some_and(|t| t == "2");
    Ok(match (&parts.method, key) {
        (&Method::GET, None) if listing => state.list(&parts.headers, &query).await,
        (&Method::PUT, Some(key)) => state.put(&parts.headers, key, body),
        (&Method::GET, Some(key)) => match state.objects.lock().unwrap().get(key) {
            Some(bytes) => Response::new(Full::new(bytes.clone())),
            None => error(StatusCode::NOT_FOUND, "NoSuchKey"),
        },
        (&Method::DELETE, Some(key)) => {
            state.objects.lock().unwrap().remove(key);
            state.changes.send_modify(|n| *n += 1);
            reply(StatusCode::NO_CONTENT, "")
        }
        _ => error(StatusCode::BAD_REQUEST, "InvalidRequest"),
    })
}

impl State {
    fn put(&self, headers: &HeaderMap, key: &str, body: Bytes) -> Answer {
        let exclusive = header(headers, "if-none-match") == Some("*");
        {
            let mut objects = self.objects.lock().unwrap();
            if exclusive && objects.contains_key(key) {
                return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
            }
            objects.insert(key.to_owned(), body);
        }
        self.changes.send_modify(|n| *n += 1);
        reply(StatusCode::OK, "")
    }

    async fn list(&self, headers: &HeaderMap, query: &BTreeMap<String, String>) -> Answer {
        let prefix = query.get("prefix").cloned().unwrap_or_default();
        let after = query.get("start-after").cloned().unwrap_or_default();
        let max_keys: usize = query.get("max-keys").map_or(1000, |m| m.parse().unwrap());
        let seconds =
            header(headers, "x-substantial-wait-seconds").map(|s| s.parse::<u64>().unwrap());
        let wait_etag = header(headers, "x-substantial-wait-etag");
        let deadline = Instant::now() + Duration::from_secs(seconds.unwrap_or(0));
        let mut changes = self.changes.subscribe();
        let mut held = false;
        let (keys, truncated, etag) = loop {
            changes.borrow_and_update();
            let (keys, truncated) = self.page(&prefix, &after, max_keys);
            let mut hasher = DefaultHasher::new();
            (&keys, truncated).hash(&mut hasher);
            let etag = format!("{:016x}", hasher.finish());
            if seconds.is_none() || wait_etag != Some(etag.as_str()) || Instant::now() >= deadline {
                break (keys, truncated, etag);
            }
            if !held {
                held = true;
                self.holds.fetch_add(1, Ordering::SeqCst);
            }
            let _ = tokio::time::timeout_at(deadline, changes.changed()).await;
        };
        let woken = held && wait_etag != Some(etag.as_str());
        let listed = Listed {
            prefix: prefix.clone(),
            headers: headers.clone(),
            woken,
        };
        self.listed.lock().unwrap().push(listed);
        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>{BUCKET}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount>\
             <MaxKeys>{max_keys}</MaxKeys><IsTruncated>{truncated}</IsTruncated>",
            escape(&prefix),
            keys.len(),
        );
        for key in &keys {
            xml += &format!(
                "<Contents><Key>{}</Key><Size>0</Size></Contents>",
                escape(key)
            );
        }
        xml += "</ListBucketResult>";
        let mut answer = reply(StatusCode::OK, &xml);
        if seconds.is_some() && !self.omit_etag.load(Ordering::SeqCst) {
            let value = etag.parse().unwrap();
            answer
                .headers_mut()
                .insert("x-substantial-listing-etag", value);
        }
        answer
    }

    fn page(&self, prefix: &str, after: &str, max_keys: usize) -> (Vec<String>, bool) {
        let objects = self.objects.lock().unwrap();
        let mut keys: Vec<String> = objects
            .keys()
            .filter(|k| k.starts_with(prefix) && k.as_str() > after)
            .take(max_keys + 1)
            .cloned()
            .collect();
        let truncated = keys.len() > max_keys;
        keys.truncate(max_keys);
        (keys, truncated)
    }
}

fn reply(status: StatusCode, body: &str) -> Answer {
    let mut answer = Response::new(Full::new(Bytes::from(body.to_owned())));
    *answer.status_mut() = status;
    answer
}

fn error(status: StatusCode, code: &str) -> Answer {
    let xml =
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code></Error>");
    reply(status, &xml)
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Percent-decodes a path, as the SDK encodes keys.
fn decode(path: &str) -> String {
    let mut bytes = Vec::with_capacity(path.len());
    let mut rest = path.as_bytes();
    while let Some((&b, tail)) = rest.split_first() {
        match (b, tail) {
            (b'%', [h, l, tail @ ..]) => {
                let hex = std::str::from_utf8(&[*h, *l]).unwrap().to_owned();
                bytes.push(u8::from_str_radix(&hex, 16).unwrap());
                rest = tail;
            }
            _ => {
                bytes.push(b);
                rest = tail;
            }
        }
    }
    String::from_utf8(bytes).unwrap()
}
