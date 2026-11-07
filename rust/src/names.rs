//! The naming rules of I-17, checked by the host so that a refusal states the rule.

use crate::error::{Error, Result};

/// Run and message ids, workflow names and published keys, which go into store keys.
pub(crate) fn check_key(what: &str, value: &str) -> Result<()> {
    let allowed = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b);
    if (1..=100).contains(&value.len())
        && value.bytes().all(allowed)
        && value != "."
        && value != ".."
        && !value.starts_with("substantial.")
    {
        return Ok(());
    }
    Err(Error::InvalidArgument(format!(
        "{what} {value:?} must match [a-z0-9._-]{{1,100}}, . and .. excepted, and not start with \
         substantial."
    )))
}

/// Step and message names.
pub(crate) fn check_name(what: &str, value: &str) -> Result<()> {
    let allowed = |b: u8| b.is_ascii_alphanumeric() || b"._-".contains(&b);
    if (1..=128).contains(&value.len()) && value.bytes().all(allowed) {
        return Ok(());
    }
    Err(Error::InvalidArgument(format!(
        "{what} {value:?} must match [A-Za-z0-9._-]{{1,128}}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules() {
        assert!(check_key("run id", "backup-2026.10_31").is_ok());
        for bad in [
            "",
            ".",
            "..",
            "Bad",
            "a b",
            "substantial.x",
            &"a".repeat(101),
        ] {
            let e = check_key("run id", bad).unwrap_err().to_string();
            assert!(e.contains("must match [a-z0-9._-]{1,100}"), "{e}");
        }
        assert!(check_name("message name", "Approve.v2").is_ok());
        let e = check_name("message name", "a b").unwrap_err().to_string();
        assert_eq!(
            e,
            r#"invalid argument: message name "a b" must match [A-Za-z0-9._-]{1,128}"#
        );
    }
}
