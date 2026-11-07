//! The naming rules of I-17, checked when the code compiles.

use syn::LitStr;

/// Workflow names go into store keys: `[a-z0-9._-]{1,100}`, `.` and `..` excepted.
pub fn key(lit: &LitStr) -> syn::Result<()> {
    let name = lit.value();
    let allowed = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b);
    if (1..=100).contains(&name.len())
        && name.bytes().all(allowed)
        && name != "."
        && name != ".."
        && !name.starts_with("substantial.")
    {
        return Ok(());
    }
    Err(syn::Error::new(
        lit.span(),
        format!("workflow name {name:?} must match [a-z0-9._-]{{1,100}}, . and .. excepted"),
    ))
}

/// Step and message names: `[A-Za-z0-9._-]{1,128}`, `substantial.` being reserved.
pub fn call(lit: &LitStr, what: &str) -> syn::Result<()> {
    let name = lit.value();
    let allowed = |b: u8| b.is_ascii_alphanumeric() || b"._-".contains(&b);
    if (1..=128).contains(&name.len())
        && name.bytes().all(allowed)
        && !name.starts_with("substantial.")
    {
        return Ok(());
    }
    Err(syn::Error::new(
        lit.span(),
        format!("{what} name {name:?} must match [A-Za-z0-9._-]{{1,128}}"),
    ))
}
