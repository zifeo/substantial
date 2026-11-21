//! The macros of a Wasm workflow, which `substantial` re-exports for wasm32: they expand to the
//! `__macro` module of `substantial-wasm-sdk`, which `substantial` re-exports there.

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, quote_spanned};
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Expr, FnArg, Ident, ItemFn, LitStr, Pat, Path, ReturnType, Token, Type};

use crate::names;

pub fn workflow(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    // § c7aw13gws.d@e495s
    if !args.is_empty() {
        return Err(syn::Error::new(
            args.span(),
            "a Wasm workflow takes its name, version and settings from the worker serving it",
        ));
    }
    // § end c7aw13gws.d
    let mut f: ItemFn = syn::parse2(item)?;
    let sig = &f.sig;
    if sig.asyncness.is_none() {
        return Err(syn::Error::new(
            sig.fn_token.span(),
            "a workflow is an async fn",
        ));
    }
    if !sig.generics.params.is_empty() || sig.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            sig.generics.span(),
            "a workflow has no generics",
        ));
    }
    if let ReturnType::Default = &sig.output {
        return Err(syn::Error::new(
            sig.span(),
            "a workflow returns a Result<_>",
        ));
    }
    let runner = match sig.inputs.len() {
        1 => quote!(run_without_input),
        2 => quote!(run_workflow),
        _ => {
            return Err(syn::Error::new(
                sig.inputs.span(),
                "a workflow takes a Context, then its input if it has one",
            ));
        }
    };
    let context = match &sig.inputs[0] {
        FnArg::Receiver(r) => return Err(syn::Error::new(r.span(), "a workflow takes no self")),
        FnArg::Typed(t) => match &*t.pat {
            Pat::Ident(p) => Some(p.ident.clone()),
            _ => None,
        },
    };
    let ident = sig.ident.clone();
    let (docs, attrs): (Vec<_>, Vec<_>) = f.attrs.drain(..).partition(|a| a.path().is_ident("doc"));
    let vis = f.vis.clone();
    let body = Ident::new("__substantial_workflow", Span::mixed_site());
    f.attrs = attrs;
    f.vis = syn::Visibility::Inherited;
    f.sig.ident = body.clone();
    if let Some(c) = context {
        let block = &f.block;
        f.block = syn::parse_quote_spanned!(block.span()=> { let _ = &#c; #block });
    }
    Ok(quote! {
        #(#docs)*
        #[allow(non_camel_case_types)]
        #[derive(Clone, Copy, Debug)]
        #vis struct #ident;

        impl ::substantial::__macro::Workflow for #ident {
            fn run(
                &self,
                input: ::std::string::String,
            ) -> ::core::result::Result<::std::string::String, ::substantial::__macro::End> {
                #f
                ::substantial::__macro::#runner(input, #body)
            }
        }
    })
}

#[derive(Default)]
struct StepOptions {
    name: Option<LitStr>,
    retry: Option<Expr>,
    timeout: Option<Expr>,
}

pub fn step(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let mut o = StepOptions::default();
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("retry") {
            o.retry = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("timeout") {
            o.timeout = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("name") {
            o.name = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("undo") {
            // § one c7aw13gws.d@bt9f5
            return Err(meta.error("a Wasm workflow's step has no undo yet"));
        } else {
            return Err(meta.error("expected `name`, `retry` or `timeout`"));
        }
        Ok(())
    });
    syn::parse::Parser::parse2(parser, args)?;
    let mut f: ItemFn = syn::parse2(item)?;
    let sig = &f.sig;
    if sig.asyncness.is_none() {
        return Err(syn::Error::new(
            sig.fn_token.span(),
            "a step is an async fn",
        ));
    }
    if !sig.generics.params.is_empty() || sig.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            sig.generics.span(),
            "a step has no generics",
        ));
    }
    let ret = match &sig.output {
        ReturnType::Type(_, ty) => ty.clone(),
        ReturnType::Default => {
            return Err(syn::Error::new(sig.span(), "a step returns a Result<_, _>"));
        }
    };
    let mut types = Vec::new();
    // The type each parameter decodes into in an attempt: the owned form of a reference.
    let mut owned = Vec::new();
    let mut borrowed = Vec::new();
    for input in &sig.inputs {
        match input {
            FnArg::Receiver(r) => return Err(syn::Error::new(r.span(), "a step takes no self")),
            FnArg::Typed(t) => match &*t.ty {
                Type::Reference(r) if r.mutability.is_some() => {
                    return Err(syn::Error::new(r.span(), "a step takes no &mut"));
                }
                Type::Reference(r) if r.lifetime.is_some() => {
                    return Err(syn::Error::new(
                        r.span(),
                        "a step's reference has no lifetime",
                    ));
                }
                Type::Reference(r) => {
                    let elem = &r.elem;
                    types.push(t.ty.clone());
                    owned.push(quote!(<#elem as ::std::borrow::ToOwned>::Owned));
                    borrowed.push(true);
                }
                ty => {
                    types.push(t.ty.clone());
                    owned.push(quote!(#ty));
                    borrowed.push(false);
                }
            },
        }
    }
    let ident = sig.ident.clone();
    let name = o
        .name
        .unwrap_or_else(|| LitStr::new(&ident.unraw().to_string(), ident.span()));
    names::call(&name, "step")?;
    names::not_reserved(&name)?;

    let args: Vec<Ident> = (0..types.len())
        .map(|i| format_ident!("__arg{i}"))
        .collect();
    let lent = args.iter().zip(&borrowed).map(|(a, b)| match b {
        true => quote!(&#a),
        false => quote!(#a),
    });
    let retry = o.retry.map(|r| quote!(.retry(#r)));
    let timeout = o.timeout.map(|t| quote!(.timeout(#t)));
    let (docs, attrs): (Vec<_>, Vec<_>) = f.attrs.drain(..).partition(|a| a.path().is_ident("doc"));
    let vis = f.vis.clone();
    let body = Ident::new("__substantial_step", Span::mixed_site());
    let entry = entry_name(&ident);
    f.attrs = attrs;
    f.vis = syn::Visibility::Inherited;
    f.sig.ident = body.clone();
    let output = quote_spanned!(ret.span()=> <#ret as ::substantial::__macro::Returns>::Value);
    Ok(quote! {
        #(#docs)*
        #vis fn #ident(#(#args: #types),*) -> impl ::core::future::Future<
            Output = ::substantial::Result<#output>,
        > + use<> {
            // § ztt9a58wy.a@8e0jn
            let __args = ::substantial::__macro::encode_arguments(&(#(&#args,)*));
            // § end ztt9a58wy.a
            async move {
                ::substantial::__macro::current_context()?
                    .__step::<#output>(#name, __args)
                    #retry
                    #timeout
                    .await
            }
        }

        #[doc(hidden)]
        #[allow(non_snake_case)]
        #vis fn #entry() -> ::substantial::__macro::StepEntry {
            #f
            ::substantial::__macro::StepEntry {
                name: #name,
                // § ztt9a58wy@neq09
                attempt: |__args| ::std::boxed::Box::pin(async move {
                    let (#(#args,)*): (#(#owned,)*) = ::substantial::__macro::decode_arguments(&__args)?;
                    ::substantial::__macro::step_outcome(#body(#(#lent),*).await)
                }),
                // § end ztt9a58wy
            }
        }
    })
}

fn entry_name(step: &Ident) -> Ident {
    format_ident!("__substantial_wasm_step_{}", step.unraw())
}

/// `export!(workflow, steps = [a, b])`.
struct Export {
    workflow: Path,
    steps: Vec<Path>,
}

impl Parse for Export {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let workflow: Path = input.parse()?;
        let mut steps = Vec::new();
        if input.parse::<Option<Token![,]>>()?.is_some() && !input.is_empty() {
            let key: Ident = input.parse()?;
            if key != "steps" {
                return Err(syn::Error::new(key.span(), "expected `steps = [..]`"));
            }
            input.parse::<Token![=]>()?;
            let list;
            syn::bracketed!(list in input);
            steps = Punctuated::<Path, Token![,]>::parse_terminated(&list)?
                .into_iter()
                .collect();
            input.parse::<Option<Token![,]>>()?;
        }
        Ok(Export { workflow, steps })
    }
}

pub fn checked(item: TokenStream) -> syn::Result<TokenStream> {
    let Export { workflow, steps } = syn::parse2(item)?;
    Ok(quote! {
        const _: () = {
            const fn workflow<W: ::substantial::AsWorkflow>(_: &W) {}
            workflow(&#workflow);
            #(let _ = #steps;)*
        };
    })
}

pub fn export(item: TokenStream) -> syn::Result<TokenStream> {
    let Export { workflow, steps } = syn::parse2(item)?;
    let entries = steps.iter().map(|s| {
        let mut path = s.clone();
        let last = path.segments.last_mut().expect("a path has a segment");
        last.ident = entry_name(&last.ident);
        quote!(#path())
    });
    Ok(quote! {
        const _: () = {
            struct __SubstantialModule;

            impl ::substantial::__bindings::exports::substantial::wasm::workflow::Guest
                for __SubstantialModule
            {
                fn run(
                    input: ::std::string::String,
                ) -> ::core::result::Result<::std::string::String, ::substantial::__macro::End> {
                    ::substantial::__macro::Workflow::run(&#workflow, input)
                }

                fn step(
                    name: ::std::string::String,
                    args: ::std::string::String,
                ) -> ::core::result::Result<::std::string::String, ::substantial::__macro::StepError> {
                    ::substantial::__macro::run_step(&[#(#entries),*], &name, args)
                }
            }

            ::substantial::__bindings::__export_module!(__SubstantialModule);
        };
    })
}
