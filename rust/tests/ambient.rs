//! The AWS environment and shared files are not read: the URL and the credentials alone define an
//! S3 store. Its own test binary, as the only test that sets environment variables.

#[allow(dead_code)]
mod fake_h3;

use bytes::Bytes;
use fake_h3::FakeH3;
use substantial::__internal::{self as internal, Create, STORE_RETRY};

#[tokio::test]
async fn the_aws_environment_and_files_are_not_read() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config");
    std::fs::write(
        &config,
        "[default]\ns3 =\n  use_accelerate_endpoint = true\n",
    )
    .unwrap();
    let ambient = [
        ("AWS_PROFILE", "absent"),
        ("AWS_CONFIG_FILE", config.to_str().unwrap()),
        ("AWS_USE_FIPS_ENDPOINT", "true"),
        ("AWS_USE_DUALSTACK_ENDPOINT", "true"),
    ];
    for (name, value) in ambient {
        // SAFETY: the only test of this binary, set before any other thread reads the environment.
        unsafe { std::env::set_var(name, value) };
    }
    let h3 = FakeH3::start().await;
    let s = internal::open(&h3.url("ambient"), &FakeH3::credentials(), STORE_RETRY).unwrap();
    let created = s.create("q/a", Bytes::from_static(b"a")).await.unwrap();
    assert_eq!(created, Create::Created);
    assert_eq!(s.get("q/a").await.unwrap(), Some(Bytes::from_static(b"a")));
}
