//! The attribute macros of Substantial, re-exported by the `substantial` crate: use them from
//! there. They expand to its explicit API (`Workflow::new`, `Context::step`).

mod names;
mod step;
mod wasm;
mod workflow;

use proc_macro::TokenStream;

/// Declares a workflow: `async fn name(c: Context[, input: I]) -> Result<O>` becomes a value
/// named like the function, passed where a workflow is (`sub.worker(name)`, `sub.start(name, ..)`,
/// `c.start(&name, ..)`). Options: `name = ".."`, the function's name unless given,
/// `version = n`, `step_timeout = duration`, `retention = duration` and `budget = b`, anything
/// `Into<Budget>` (`[("requests", 1000)]`).
#[proc_macro_attribute]
pub fn workflow(args: TokenStream, item: TokenStream) -> TokenStream {
    workflow::expand(args.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Declares a step: an async function whose value is recorded, called inside a workflow like the
/// function itself. Its parameters are values or shared references (`&str`, `&T`): a reference is
/// made owned once at the call with `ToOwned`, and each attempt borrows a clone of it. Options:
/// `name = ".."`, the function's name unless given, `retry = Retry {..}`, `timeout = duration`
/// and `undo = f`, where `f` takes the step's value then the step's parameters, of the same types.
#[proc_macro_attribute]
pub fn step(args: TokenStream, item: TokenStream) -> TokenStream {
    step::expand(args.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `export!(workflow, steps = [a, b])` makes a Wasm workflow of a source built for wasm32: natively,
/// it only checks that the workflow and its steps exist, so that the same source builds both ways.
#[proc_macro]
pub fn export(item: TokenStream) -> TokenStream {
    // § c7aw13gws.b@vbtc4
    wasm::checked(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `#[workflow]` of a Wasm workflow, which `substantial` re-exports for wasm32: use it from there.
#[doc(hidden)]
#[proc_macro_attribute]
pub fn wasm_workflow(args: TokenStream, item: TokenStream) -> TokenStream {
    // § c7aw13gws@ncede
    wasm::workflow(args.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `#[step]` of a Wasm workflow, which `substantial` re-exports for wasm32: use it from there.
#[doc(hidden)]
#[proc_macro_attribute]
pub fn wasm_step(args: TokenStream, item: TokenStream) -> TokenStream {
    // § c7aw13gws@dnszs
    wasm::step(args.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `export!` of a Wasm workflow, which `substantial` re-exports for wasm32: use it from there.
#[doc(hidden)]
#[proc_macro]
pub fn wasm_export(item: TokenStream) -> TokenStream {
    // § c7aw13gws.b@r4vqe
    wasm::export(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[cfg(test)]
mod tests;
