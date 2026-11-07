//! What the macros refuse, with the message the code then fails to compile with.

use proc_macro2::TokenStream;
use quote::quote;

use crate::{step, workflow};

fn refused(expanded: syn::Result<TokenStream>, message: &str) {
    match expanded {
        Ok(tokens) => panic!("expanded to {tokens}"),
        Err(e) => assert!(e.to_string().contains(message), "{e}"),
    }
}

#[test]
fn a_step_is_an_async_fn_of_values_or_shared_references() {
    refused(
        step::expand(
            quote!(),
            quote!(
                fn f() -> Result<(), E> {
                    Ok(())
                }
            ),
        ),
        "a step is an async fn",
    );
    refused(
        step::expand(
            quote!(),
            quote!(
                async fn f<T>(t: T) -> Result<T, E> {
                    Ok(t)
                }
            ),
        ),
        "a step has no generics",
    );
    refused(
        step::expand(
            quote!(),
            quote!(
                async fn f(v: &mut Vec<u8>) -> R {
                    x
                }
            ),
        ),
        "a step takes no &mut",
    );
    refused(
        step::expand(
            quote!(),
            quote!(
                async fn f(s: &'static str) -> R {
                    x
                }
            ),
        ),
        "a step's reference has no lifetime",
    );
    refused(
        step::expand(
            quote!(),
            quote!(
                async fn f() {}
            ),
        ),
        "a step returns a Result",
    );
    refused(
        step::expand(
            quote!(name = "a step"),
            quote!(
                async fn f() -> R {
                    x
                }
            ),
        ),
        "step name \"a step\"",
    );
    refused(
        step::expand(
            quote!(retries = 3),
            quote!(
                async fn f() -> R {
                    x
                }
            ),
        ),
        "expected `name`, `retry`, `timeout` or `undo`",
    );
}

#[test]
fn a_workflow_takes_a_context_then_its_input() {
    refused(
        workflow::expand(
            quote!(),
            quote!(
                async fn w() -> Result<()> {
                    Ok(())
                }
            ),
        ),
        "a workflow takes a Context",
    );
    refused(
        workflow::expand(
            quote!(),
            quote!(
                async fn w(c: Context, a: u32, b: u32) -> R {
                    x
                }
            ),
        ),
        "a workflow takes a Context",
    );
    refused(
        workflow::expand(
            quote!(),
            quote!(
                fn w(c: Context) -> Result<()> {
                    Ok(())
                }
            ),
        ),
        "a workflow is an async fn",
    );
    refused(
        workflow::expand(
            quote!(name = "Backup"),
            quote!(
                async fn w(c: Context) -> R {
                    x
                }
            ),
        ),
        "workflow name \"Backup\"",
    );
    let expanded = workflow::expand(
        quote!(name = "backup", version = 2),
        quote!(
            async fn backup_v2(c: Context) -> Result<()> {
                Ok(())
            }
        ),
    )
    .unwrap()
    .to_string();
    assert!(expanded.contains("struct backup_v2"), "{expanded}");
    assert!(expanded.contains("with_version (2)"), "{expanded}");
}

#[test]
fn a_workflow_input_is_a_struct_or_a_map() {
    for input in [
        quote!(i64),
        quote!(String),
        quote!(std::vec::Vec<u8>),
        quote!((u32, u32)),
        quote!(()),
        quote!([u8; 4]),
    ] {
        refused(
            workflow::expand(
                quote!(),
                quote!(
                    async fn w(c: Context, x: #input) -> R {
                        x
                    }
                ),
            ),
            "a workflow input is a struct or a map",
        );
    }
    for input in [
        quote!(Input),
        quote!(HashMap<String, u32>),
        quote!(serde_json::Value),
    ] {
        let expanded = workflow::expand(
            quote!(),
            quote!(
                async fn w(c: Context, x: #input) -> R {
                    x
                }
            ),
        );
        assert!(expanded.is_ok(), "{input}");
    }
}
