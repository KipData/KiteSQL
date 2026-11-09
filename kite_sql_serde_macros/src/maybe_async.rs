// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `#[maybe_async]` compiles one async source as the sync path (by default) or the async path (with
//! the crate's `async` feature), in place and under the same names. It emits both versions of
//! the item, each under its `cfg`, so only one ever exists in a build.
//!
//! - **sync**: `async` removed from signatures, `.await` removed from expressions (macro
//!   arguments included), `async |..|`
//!   closures lose `async`, `AsyncFn*` bounds become `Fn*`, and an `async { .. }` block becomes
//!   an immediately called closure `(|| { .. })()`, so `?` and `return` inside it keep leaving
//!   just the block. This is the code the sync backends run, identical to hand-written sync code.
//! - **async**: the code as written, except that `async fn` in trait definitions becomes
//!   `fn .. -> impl Future<Output = R> + Send`, so generic code can prove its futures are `Send`,
//!   and an `async fn` marked `#[test]` becomes a plain test running its body on
//!   `crate::maybe_async::block_on`, so one test source covers both paths.
//!
//! `#[sync_only]` / `#[async_only]` keep an impl item, trait item or nested item in one path
//! only. `#[keep_async_fn]` on a trait keeps its `async fn`s as written on the async path; use it
//! for traits with concrete implementors only, where callers see the concrete future type anyway.

use proc_macro2::{Group, TokenStream, TokenTree};
use quote::quote;
use syn::visit_mut::{self, VisitMut};
use syn::{Attribute, Expr, Ident, ImplItem, Item, ReturnType, Signature, TraitItem, Type};

#[derive(Clone, Copy, PartialEq)]
enum Variant {
    Sync,
    Async,
}

impl Variant {
    fn keep_attr(self) -> &'static str {
        match self {
            Variant::Sync => "sync_only",
            Variant::Async => "async_only",
        }
    }

    fn drop_attr(self) -> &'static str {
        match self {
            Variant::Sync => "async_only",
            Variant::Async => "sync_only",
        }
    }
}

struct Transform {
    variant: Variant,
}

impl Transform {
    /// Returns `false` if the item belongs to the other copy; strips the marker attributes.
    fn retain(&self, attrs: &mut Vec<Attribute>) -> bool {
        if attrs
            .iter()
            .any(|attr| attr.path().is_ident(self.variant.drop_attr()))
        {
            return false;
        }
        attrs.retain(|attr| !attr.path().is_ident(self.variant.keep_attr()));
        true
    }

    fn retain_items(&self, items: &mut Vec<Item>) {
        items.retain_mut(|item| match item_attrs(item) {
            Some(attrs) => self.retain(attrs),
            None => true,
        });
    }
}

impl VisitMut for Transform {
    fn visit_file_mut(&mut self, file: &mut syn::File) {
        self.retain_items(&mut file.items);
        visit_mut::visit_file_mut(self, file);
    }

    fn visit_item_mod_mut(&mut self, item: &mut syn::ItemMod) {
        if let Some((_, items)) = &mut item.content {
            self.retain_items(items);
        }
        visit_mut::visit_item_mod_mut(self, item);
    }

    fn visit_item_impl_mut(&mut self, item: &mut syn::ItemImpl) {
        item.items.retain_mut(|item| match item {
            ImplItem::Fn(f) => self.retain(&mut f.attrs),
            ImplItem::Const(c) => self.retain(&mut c.attrs),
            ImplItem::Type(t) => self.retain(&mut t.attrs),
            _ => true,
        });
        visit_mut::visit_item_impl_mut(self, item);
    }

    fn visit_item_trait_mut(&mut self, item: &mut syn::ItemTrait) {
        item.items.retain_mut(|item| match item {
            TraitItem::Fn(f) => self.retain(&mut f.attrs),
            TraitItem::Const(c) => self.retain(&mut c.attrs),
            TraitItem::Type(t) => self.retain(&mut t.attrs),
            _ => true,
        });
        let keep_async_fn = item
            .attrs
            .iter()
            .any(|attr| attr.path().is_ident("keep_async_fn"));
        item.attrs
            .retain(|attr| !attr.path().is_ident("keep_async_fn"));
        visit_mut::visit_item_trait_mut(self, item);
        if self.variant == Variant::Async && keep_async_fn {
            item.attrs
                .push(syn::parse_quote!(#[allow(async_fn_in_trait)]));
        } else if self.variant == Variant::Async {
            for item in &mut item.items {
                if let TraitItem::Fn(f) = item {
                    desugar_trait_async_fn(f);
                }
            }
        }
    }

    fn visit_item_fn_mut(&mut self, item: &mut syn::ItemFn) {
        visit_mut::visit_item_fn_mut(self, item);
        // An async test runs to completion on `crate::maybe_async::block_on`.
        if self.variant == Variant::Async
            && item.sig.asyncness.is_some()
            && item.attrs.iter().any(|attr| attr.path().is_ident("test"))
        {
            item.sig.asyncness = None;
            let block = &item.block;
            *item.block = syn::parse_quote!({ crate::maybe_async::block_on(async move #block) });
        }
    }

    fn visit_signature_mut(&mut self, sig: &mut Signature) {
        if self.variant == Variant::Sync {
            sig.asyncness = None;
        }
        visit_mut::visit_signature_mut(self, sig);
    }

    fn visit_expr_mut(&mut self, expr: &mut Expr) {
        if self.variant == Variant::Sync {
            while let Expr::Await(await_expr) = expr {
                *expr = (*await_expr.base).clone();
            }
            if let Expr::Closure(closure) = expr {
                closure.asyncness = None;
            }
            if let Expr::Async(block) = expr {
                let capture = block.capture;
                let body = block.block.clone();
                *expr = syn::parse_quote!((#capture || #body)());
            }
        }
        visit_mut::visit_expr_mut(self, expr);
    }

    fn visit_macro_mut(&mut self, mac: &mut syn::Macro) {
        // Macro arguments are opaque tokens, so `.await` and `async` are removed token by token.
        if self.variant == Variant::Sync {
            mac.tokens = strip_await(std::mem::take(&mut mac.tokens));
        }
        visit_mut::visit_macro_mut(self, mac);
    }

    fn visit_path_segment_mut(&mut self, segment: &mut syn::PathSegment) {
        if self.variant == Variant::Sync {
            let sync_name = match segment.ident.to_string().as_str() {
                "AsyncFnOnce" => Some("FnOnce"),
                "AsyncFnMut" => Some("FnMut"),
                "AsyncFn" => Some("Fn"),
                _ => None,
            };
            if let Some(name) = sync_name {
                segment.ident = Ident::new(name, segment.ident.span());
            }
        }
        visit_mut::visit_path_segment_mut(self, segment);
    }
}

/// Drops every `.await` from `tokens`, and `async` before `fn` and closures, inside groups too.
fn strip_await(tokens: TokenStream) -> TokenStream {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    let is_ident = |i: usize, name: &str| matches!(tokens.get(i), Some(TokenTree::Ident(ident)) if ident == name);
    let is_punct = |i: usize, c: char| matches!(tokens.get(i), Some(TokenTree::Punct(punct)) if punct.as_char() == c);
    let mut output = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        if is_punct(i, '.') && is_ident(i + 1, "await") {
            i += 2;
            continue;
        }
        if is_ident(i, "async")
            && (is_ident(i + 1, "fn")
                || is_punct(i + 1, '|')
                || (is_ident(i + 1, "move") && is_punct(i + 2, '|')))
        {
            i += 1;
            continue;
        }
        output.push(match &tokens[i] {
            TokenTree::Group(group) => {
                let mut stripped = Group::new(group.delimiter(), strip_await(group.stream()));
                stripped.set_span(group.span());
                TokenTree::Group(stripped)
            }
            token => token.clone(),
        });
        i += 1;
    }
    output.into_iter().collect()
}

/// `async fn f(..) -> R { body }` in a trait becomes
/// `fn f(..) -> impl Future<Output = R> + Send { async move { let r: R = { body }; r } }`.
/// Naming `R` on the binding lets `?` inside the body infer its error type.
fn desugar_trait_async_fn(f: &mut syn::TraitItemFn) {
    if f.sig.asyncness.take().is_none() {
        return;
    }
    let output: Type = match &f.sig.output {
        ReturnType::Default => syn::parse_quote!(()),
        ReturnType::Type(_, ty) => (**ty).clone(),
    };
    f.sig.output = syn::parse_quote! {
        -> impl ::core::future::Future<Output = #output> + ::core::marker::Send
    };
    if let Some(block) = &f.default {
        // A body that only leaves through `return` makes the trailing binding unreachable.
        f.attrs
            .push(syn::parse_quote!(#[allow(unreachable_code, clippy::let_and_return)]));
        f.default = Some(syn::parse_quote! {{
            async move {
                let __maybe_async_output: #output = #block;
                __maybe_async_output
            }
        }});
    }
}

fn item_attrs(item: &mut Item) -> Option<&mut Vec<Attribute>> {
    Some(match item {
        Item::Const(i) => &mut i.attrs,
        Item::Enum(i) => &mut i.attrs,
        Item::Fn(i) => &mut i.attrs,
        Item::Impl(i) => &mut i.attrs,
        Item::Macro(i) => &mut i.attrs,
        Item::Mod(i) => &mut i.attrs,
        Item::Static(i) => &mut i.attrs,
        Item::Struct(i) => &mut i.attrs,
        Item::Trait(i) => &mut i.attrs,
        Item::Type(i) => &mut i.attrs,
        Item::Union(i) => &mut i.attrs,
        Item::Use(i) => &mut i.attrs,
        _ => return None,
    })
}

pub(crate) fn handle(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new_spanned(attr, "`#[maybe_async]` takes no arguments")
            .to_compile_error();
    }
    let item: Item = match syn::parse2(item) {
        Ok(item) => item,
        Err(err) => return err.to_compile_error(),
    };
    let copy = |variant| {
        let mut file = syn::File {
            shebang: None,
            attrs: Vec::new(),
            items: vec![item.clone()],
        };
        Transform { variant }.visit_file_mut(&mut file);
        file.items
    };
    let sync = copy(Variant::Sync);
    let async_copy = copy(Variant::Async);
    quote! {
        #(#[cfg(not(feature = "async"))] #sync)*
        #(#[cfg(feature = "async")] #async_copy)*
    }
}
