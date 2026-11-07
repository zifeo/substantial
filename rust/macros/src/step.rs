use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, quote_spanned};
use syn::ext::IdentExt;
use syn::spanned::Spanned;
use syn::{Expr, FnArg, Ident, ItemFn, LitStr, ReturnType, Type};

use crate::names;

#[derive(Default)]
struct Options {
    name: Option<LitStr>,
    retry: Option<Expr>,
    timeout: Option<Expr>,
    undo: Option<Expr>,
}

fn options(args: TokenStream) -> syn::Result<Options> {
    let mut o = Options::default();
    let parser = syn::meta::parser(|meta| {
        let slot = if meta.path.is_ident("retry") {
            &mut o.retry
        } else if meta.path.is_ident("timeout") {
            &mut o.timeout
        } else if meta.path.is_ident("undo") {
            &mut o.undo
        } else if meta.path.is_ident("name") {
            o.name = Some(meta.value()?.parse()?);
            return Ok(());
        } else {
            return Err(meta.error("expected `name`, `retry`, `timeout` or `undo`"));
        };
        *slot = Some(meta.value()?.parse()?);
        Ok(())
    });
    syn::parse::Parser::parse2(parser, args)?;
    Ok(o)
}

pub fn expand(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let o = options(args)?;
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
    let mut borrowed = Vec::new();
    for input in &sig.inputs {
        match input {
            FnArg::Receiver(r) => return Err(syn::Error::new(r.span(), "a step takes no self")),
            FnArg::Typed(t) => {
                let owned = match &*t.ty {
                    Type::Reference(r) if r.mutability.is_some() => {
                        return Err(syn::Error::new(r.span(), "a step takes no &mut"));
                    }
                    Type::Reference(r) if r.lifetime.is_some() => {
                        return Err(syn::Error::new(
                            r.span(),
                            "a step's reference has no lifetime",
                        ));
                    }
                    Type::Reference(r) => Some(r.elem.clone()),
                    _ => None,
                };
                types.push(t.ty.clone());
                borrowed.push(owned);
            }
        }
    }
    let ident = sig.ident.clone();
    let name = o
        .name
        .unwrap_or_else(|| LitStr::new(&ident.unraw().to_string(), ident.span()));
    names::call(&name, "step")?;

    let args: Vec<Ident> = (0..types.len())
        .map(|i| format_ident!("__arg{i}"))
        .collect();
    // A reference is owned once at the call, so that the step future is 'static, then cloned for
    // each attempt and lent to the body.
    let owned = args.iter().zip(&borrowed).filter_map(|(a, b)| {
        b.as_ref().map(|elem| {
            quote!(let #a: <#elem as ::std::borrow::ToOwned>::Owned = ::std::borrow::ToOwned::to_owned(#a);)
        })
    });
    let passed = |names: &[Ident]| {
        let each = names.iter().zip(&borrowed).map(|(n, b)| match b {
            Some(_) => quote!(&#n),
            None => quote!(#n),
        });
        quote!(#(#each),*)
    };
    let calls = passed(&args);
    let retry = o.retry.map(|r| quote!(.retry(#r)));
    let timeout = o.timeout.map(|t| quote!(.timeout(#t)));
    // The undo gets the arguments too: its closure keeps copies of its own.
    let (undo_args, undo) = match o.undo {
        Some(undo) => {
            let copies: Vec<Ident> = (0..types.len())
                .map(|i| format_ident!("__undo{i}"))
                .collect();
            let undo_calls = passed(&copies);
            (
                quote!(#(let #copies = ::core::clone::Clone::clone(&#args);)*),
                quote!(.undo(move |__value| {
                    #(let #copies = ::core::clone::Clone::clone(&#copies);)*
                    async move { #undo(__value, #undo_calls).await }
                })),
            )
        }
        None => (quote!(), quote!()),
    };

    let (docs, attrs): (Vec<_>, Vec<_>) = f.attrs.drain(..).partition(|a| a.path().is_ident("doc"));
    let vis = f.vis.clone();
    let body = Ident::new("__substantial_step", Span::mixed_site());
    f.attrs = attrs;
    f.vis = syn::Visibility::Inherited;
    f.sig.ident = body.clone();
    let output = quote_spanned!(ret.span()=> <#ret as ::substantial::__macro::Returns>::Value);
    Ok(quote! {
        #(#docs)*
        #vis fn #ident(#(#args: #types),*) -> impl ::core::future::Future<
            Output = ::substantial::Result<#output>,
        > + ::core::marker::Send + use<> {
            #f
            #(#owned)*
            #undo_args
            async move {
                ::substantial::__macro::context()?
                    .step(#name, move || {
                        #(let #args = ::core::clone::Clone::clone(&#args);)*
                        async move { #body(#calls).await }
                    })
                    #retry
                    #timeout
                    #undo
                    .await
            }
        }
    })
}
