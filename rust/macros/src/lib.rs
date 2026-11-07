//! The attribute macros of Substantial, re-exported by the `substantial` crate: use them from
//! there. They expand to its explicit API (`Workflow::new`, `Context::step`).

mod names;
mod step;
mod workflow;

use proc_macro::TokenStream;

/// Declares a workflow: `async fn name(c: Context[, input: I]) -> Result<O>` becomes a value
/// named like the function, passed where a workflow is (`sub.worker(name)`, `sub.start(name, ..)`,
/// `c.start(&name, ..)`). Options: `name = ".."`, the function's name unless given, and
/// `version = n`.
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

#[cfg(test)]
mod tests;
