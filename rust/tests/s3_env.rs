//! `Credentials::Env` reaches the S3 store of the tests through the process's environment. Its own
//! test binary, as the only test that sets the AWS keys: the other S3 tests give them explicitly.

mod common;

use common::{s3_url, setting};
use substantial::{Credentials, Substantial};

#[tokio::test]
async fn env_credentials_read_the_aws_keys() {
    for name in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"] {
        let value = setting(name);
        // SAFETY: the only test of this binary, set before any other thread reads the environment.
        unsafe { std::env::set_var(name, value) };
    }
    let url = s3_url("s3");
    drop(
        Substantial::create(&url)
            .credentials(Credentials::Env)
            .await
            .unwrap(),
    );
    Substantial::open(&url)
        .credentials(Credentials::Env)
        .await
        .unwrap();
}
