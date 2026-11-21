//! The naming rules of I-17, checked by the host so that a refusal states the rule.

use crate::error::{Error, Result};

/// Run and message ids, workflow names and published keys, which go into store keys.
pub(crate) fn check_key(what: &str, value: &str) -> Result<()> {
    // § px8dv58y7.a@bpy15
    let allowed = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b);
    // § one px8dv58y7.b@g3cnz
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
    // § px8dv58y7.a@4bgat
    let allowed = |b: u8| b.is_ascii_alphanumeric() || b"._-".contains(&b);
    if (1..=128).contains(&value.len()) && value.bytes().all(allowed) {
        return Ok(());
    }
    Err(Error::InvalidArgument(format!(
        "{what} {value:?} must match [A-Za-z0-9._-]{{1,128}}"
    )))
}

/// The name of a message sent, by a client or a workflow.
pub(crate) fn check_sent_name(value: &str) -> Result<()> {
    check_name("message name", value)?;
    // § j69rwh8x9@80y9s
    if value.starts_with("substantial.") {
        return Err(Error::InvalidArgument(format!(
            "message name {value:?} starts with substantial., which is reserved"
        )));
    }
    // § end j69rwh8x9
    Ok(())
}

/// A step name of the code, at its first call: the macro checks a declared step's as it compiles.
pub(crate) fn check_step_name(value: &str) -> Result<()> {
    check_name("step name", value)?;
    // § px8dv58y7.c@0f0bk
    if value.starts_with("substantial.") {
        return Err(Error::InvalidArgument(format!(
            "step name {value:?} starts with substantial., which is reserved"
        )));
    }
    // § end px8dv58y7.c
    Ok(())
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
