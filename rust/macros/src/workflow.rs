use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::ext::IdentExt;
use syn::spanned::Spanned;
use syn::{Expr, FnArg, Ident, ItemFn, LitStr, Pat, ReturnType, Type};

use crate::names;

pub fn expand(args: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let mut name: Option<LitStr> = None;
    let mut version: Option<Expr> = None;
    let mut step_timeout: Option<Expr> = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            name = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("version") {
            version = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("step_timeout") {
            step_timeout = Some(meta.value()?.parse()?);
        } else {
            return Err(
                meta.error("expected `name = \"..\"`, `version = ..` or `step_timeout = ..`")
            );
        }
        Ok(())
    });
    syn::parse::Parser::parse2(parser, args)?;
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
    let ret = match &sig.output {
        ReturnType::Type(_, ty) => ty.clone(),
        ReturnType::Default => {
            return Err(syn::Error::new(
                sig.span(),
                "a workflow returns a Result<_>",
            ));
        }
    };
    let constructor = match sig.inputs.len() {
        1 => quote!(without_input),
        2 => quote!(new),
        _ => {
            return Err(syn::Error::new(
                sig.inputs.span(),
                "a workflow takes a Context, then its input if it has one",
            ));
        }
    };
    if let Some(FnArg::Typed(input)) = sig.inputs.iter().nth(1) {
        object_input(&input.ty)?;
    }
    let context = match &sig.inputs[0] {
        FnArg::Receiver(r) => return Err(syn::Error::new(r.span(), "a workflow takes no self")),
        FnArg::Typed(t) => match &*t.pat {
            Pat::Ident(p) => Some(p.ident.clone()),
            _ => None,
        },
    };
    let ident = sig.ident.clone();
    let name = name.unwrap_or_else(|| LitStr::new(&ident.unraw().to_string(), ident.span()));
    names::key(&name)?;
    let version = version.map(|v| quote!(.with_version(#v)));
    let step_timeout = step_timeout.map(|t| quote!(.with_step_timeout(#t)));

    let (docs, attrs): (Vec<_>, Vec<_>) = f.attrs.drain(..).partition(|a| a.path().is_ident("doc"));
    let vis = f.vis.clone();
    let body = Ident::new("__substantial_workflow", Span::mixed_site());
    f.attrs = attrs;
    f.vis = syn::Visibility::Inherited;
    f.sig.ident = body.clone();
    // The context is in the signature of every workflow, used or not.
    if let Some(c) = context {
        let block = &f.block;
        f.block = syn::parse_quote_spanned!(block.span()=> { let _ = &#c; #block });
    }
    let output = quote_spanned!(ret.span()=> <#ret as ::substantial::__macro::Returns>::Value);
    Ok(quote! {
        #(#docs)*
        #[allow(non_camel_case_types)]
        #[derive(Clone, Copy, Debug)]
        #vis struct #ident;

        impl ::substantial::AsWorkflow for #ident {
            type Output = #output;

            fn workflow(&self) -> ::substantial::Workflow<Self::Output> {
                #f
                ::substantial::Workflow::#constructor(#name, #body)#version #step_timeout
            }
        }
    })
}

/// Types that never deserialize from a JSON object; any other is checked when a run starts.
const NOT_OBJECTS: &[&str] = &[
    "bool",
    "char",
    "str",
    "String",
    "i8",
    "i16",
    "i32",
    "i64",
    "i128",
    "isize",
    "u8",
    "u16",
    "u32",
    "u64",
    "u128",
    "usize",
    "f32",
    "f64",
    "Vec",
    "VecDeque",
    "LinkedList",
    "HashSet",
    "BTreeSet",
    "BinaryHeap",
];

/// A workflow's input is one JSON object (I-5): a scalar, a string, a sequence or a tuple is
/// refused when the code compiles.
fn object_input(ty: &Type) -> syn::Result<()> {
    let refused = match ty {
        Type::Paren(p) => return object_input(&p.elem),
        Type::Group(g) => return object_input(&g.elem),
        Type::Tuple(_) | Type::Array(_) | Type::Slice(_) | Type::Reference(_) => true,
        Type::Path(p) if p.qself.is_none() => p
            .path
            .segments
            .last()
            .is_some_and(|s| NOT_OBJECTS.contains(&s.ident.to_string().as_str())),
        _ => false,
    };
    if refused {
        return Err(syn::Error::new(
            ty.span(),
            "a workflow input is a struct or a map, as it is one JSON object",
        ));
    }
    Ok(())
}
