//! Keys as file names: every component checked before it becomes a path.

use super::retry::Fault;

pub(super) const TMP: &str = ".tmp";

fn component_error(c: &str) -> Option<&'static str> {
    match c {
        "" => Some("empty"),
        "." | ".." => Some("relative"),
        _ if c.len() > 255 => Some("longer than 255 bytes"),
        _ if c.bytes().any(|b| b.is_ascii_uppercase()) => {
            Some("uppercase, which a file system may fold")
        }
        _ if c.contains('\0') => Some("holds a NUL byte"),
        _ => None,
    }
}

pub(super) fn check_key(key: &str) -> Result<(), Fault> {
    key.split('/').enumerate().try_for_each(|(i, c)| {
        let tmp = (i == 0 && c == TMP).then_some("reserved for temporary files");
        match component_error(c).or(tmp) {
            Some(why) => Err(Fault::Permanent(format!(
                "invalid key component {c:?}: {why}"
            ))),
            None => Ok(()),
        }
    })
}

/// Whether a file name could be a key component; others are not the store's.
pub(super) fn is_component(name: &str) -> bool {
    component_error(name).is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn components() {
        assert!(check_key("runs/01arz3ndektsv4rrffq69g5fav/start").is_ok());
        assert!(check_key(".tmp/x").is_err());
        assert!(check_key("a/.tmp").is_ok());
        assert!(!is_component(&"x".repeat(256)));
        assert!(is_component("a.b-c_d"));
        assert!(!is_component(".DS_Store") && check_key("runs/Run/start").is_err());
    }
}
