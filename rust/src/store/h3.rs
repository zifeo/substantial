//! Waiting listings: a `ListObjectsV2` carrying the wait headers, answered with the
//! listing ETag of its page.

use std::sync::{Arc, Mutex};

use aws_sdk_s3::client::customize::CustomizableOperation;
use aws_sdk_s3::config::interceptors::BeforeDeserializationInterceptorContextRef;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::BoxError;
use aws_sdk_s3::operation::list_objects_v2::builders::ListObjectsV2FluentBuilder;
use aws_sdk_s3::operation::list_objects_v2::{ListObjectsV2Error, ListObjectsV2Output};

use super::retry::Fault;
use super::s3::PAGE;
use super::{ListWait, Store, StoreFailure};

const WAIT_SECONDS: &str = "x-substantial-wait-seconds";
const WAIT_ETAG: &str = "x-substantial-wait-etag";
const LISTING_ETAG: &str = "x-substantial-listing-etag";
/// The longest an h3 server holds a listing.
const MAX_WAIT_SECONDS: u32 = 20;

type List =
    CustomizableOperation<ListObjectsV2Output, ListObjectsV2Error, ListObjectsV2FluentBuilder>;

/// The listing ETag of the answer, read from its headers since the SDK's output drops them.
#[derive(Clone, Debug, Default)]
pub(super) struct Answer(Arc<Mutex<Option<String>>>);

impl Answer {
    pub(super) fn etag(&self) -> Result<String, Fault> {
        self.0
            .lock()
            .expect("never poisoned")
            .clone()
            .ok_or_else(|| {
                Fault::Permanent(format!(
                    "a waiting listing was answered without {LISTING_ETAG}: \
                 the h3:// URL names a server whose listings cannot wait"
                ))
            })
    }
}

impl Intercept for Answer {
    fn name(&self) -> &'static str {
        "ListingEtag"
    }

    fn read_before_deserialization(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let etag = context.response().headers().get(LISTING_ETAG);
        *self.0.lock().expect("never poisoned") = etag.map(str::to_owned);
        Ok(())
    }
}

/// Adds the wait headers to a listing. `mutate_request` runs before signing, so SigV4 covers them.
pub(super) fn wait(list: List, max_keys: u32, wait: &ListWait) -> Result<(List, Answer), Fault> {
    // The listing ETag covers one page, so a waiting listing is never continued.
    if max_keys > PAGE {
        return Err(Fault::Permanent(format!(
            "a waiting listing takes at most {PAGE} keys, not {max_keys}"
        )));
    }
    if wait.seconds > MAX_WAIT_SECONDS {
        return Err(Fault::Permanent(format!(
            "a waiting listing waits at most {MAX_WAIT_SECONDS} s, not {} s",
            wait.seconds
        )));
    }
    if let Some(etag) = &wait.etag
        && !etag.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(Fault::Permanent(format!(
            "the listing etag {etag:?} cannot be sent in {WAIT_ETAG}"
        )));
    }
    let (seconds, etag) = (wait.seconds.to_string(), wait.etag.clone());
    let answer = Answer::default();
    let list = list
        .mutate_request(move |request| {
            let headers = request.headers_mut();
            headers.insert(WAIT_SECONDS, seconds.clone());
            if let Some(etag) = &etag {
                headers.insert(WAIT_ETAG, etag.clone());
            }
        })
        .interceptor(answer.clone());
    Ok((list, answer))
}

/// Returns once the run's `result` exists, holding waiting listings that take its key as their
/// prefix: a result waited on is seen as soon as it is created, in one request per
/// `MAX_WAIT_SECONDS` meanwhile.
pub(crate) async fn result_created(store: &dyn Store, key: &str) -> Result<(), StoreFailure> {
    let mut wait = ListWait {
        etag: None,
        seconds: 0,
    };
    loop {
        let page = store.list(key, None, 1, Some(wait)).await?;
        if page.keys.iter().any(|k| k == key) {
            return Ok(());
        }
        let etag = page.etag.ok_or_else(|| {
            StoreFailure::new(format!("a waiting listing answered without {LISTING_ETAG}"))
        })?;
        wait = ListWait {
            etag: Some(etag),
            seconds: MAX_WAIT_SECONDS,
        };
    }
}
