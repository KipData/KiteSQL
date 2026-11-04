use proc_macro2::{Spacing, TokenStream, TokenTree};
use quote::{quote, ToTokens};
use syn::{parse_quote, Error, Generics, Ident, Lifetime, Type};

/// The single lifetime of a derived struct, which its fields may borrow from a transaction.
pub(crate) fn single_lifetime(
    generics: &Generics,
    struct_name: &Ident,
    derive_name: &str,
) -> Result<Option<Lifetime>, Error> {
    let mut lifetimes = generics.lifetimes();
    let lifetime = lifetimes.next().map(|param| param.lifetime.clone());
    if lifetimes.next().is_some() {
        return Err(Error::new_spanned(
            struct_name,
            format!("{derive_name} supports at most one lifetime parameter"),
        ));
    }
    Ok(lifetime)
}

/// Implements `FromQueryRow` and `FromTxnRow` with `decode`, which reads `schema` and `tuple`.
///
/// A struct with a lifetime borrows from the transaction when it decodes from a transaction's
/// result row, and owns copies of every value when it decodes from any result row.
pub(crate) fn row_impls(
    struct_name: &Ident,
    generics: &Generics,
    lifetime: Option<&Lifetime>,
    decode: TokenStream,
) -> TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    match lifetime {
        Some(lifetime) => quote! {
            impl #impl_generics ::kite_sql::orm::FromTxnRow<#lifetime> for #struct_name #ty_generics
            #where_clause
            {
                fn from_txn_row(
                    schema: &::kite_sql::types::tuple::SchemaView<'_, '_>,
                    tuple: &mut ::kite_sql::db::ResultRow<'_, #lifetime>,
                ) -> ::std::result::Result<Self, ::kite_sql::errors::DatabaseError> {
                    #decode
                }
            }

            impl #impl_generics ::kite_sql::orm::FromQueryRow for #struct_name #ty_generics
            #where_clause
            {
                fn from_query_row(
                    schema: &::kite_sql::types::tuple::SchemaView<'_, '_>,
                    tuple: &mut ::kite_sql::db::ResultRow<'_, '_>,
                ) -> ::std::result::Result<Self, ::kite_sql::errors::DatabaseError> {
                    <Self as ::kite_sql::orm::FromTxnRow<#lifetime>>::from_txn_row(
                        schema,
                        &mut ::kite_sql::db::ResultRow::materialized(tuple),
                    )
                }
            }
        },
        None => {
            let mut txn_generics = generics.clone();
            txn_generics.params.insert(0, parse_quote!('__kite_txn));
            let (txn_impl_generics, _, _) = txn_generics.split_for_impl();
            quote! {
                impl #impl_generics ::kite_sql::orm::FromQueryRow for #struct_name #ty_generics
                #where_clause
                {
                    fn from_query_row(
                        schema: &::kite_sql::types::tuple::SchemaView<'_, '_>,
                        tuple: &mut ::kite_sql::db::ResultRow<'_, '_>,
                    ) -> ::std::result::Result<Self, ::kite_sql::errors::DatabaseError> {
                        #decode
                    }
                }

                impl #txn_impl_generics ::kite_sql::orm::FromTxnRow<'__kite_txn>
                    for #struct_name #ty_generics
                #where_clause
                {
                    fn from_txn_row(
                        schema: &::kite_sql::types::tuple::SchemaView<'_, '_>,
                        tuple: &mut ::kite_sql::db::ResultRow<'_, '__kite_txn>,
                    ) -> ::std::result::Result<Self, ::kite_sql::errors::DatabaseError> {
                        <Self as ::kite_sql::orm::FromQueryRow>::from_query_row(schema, tuple)
                    }
                }
            }
        }
    }
}

/// `ty` with `lifetime` replaced by `'static`, for statics, which cannot name the struct's
/// lifetime. Lifetimes never affect what a column type maps to.
pub(crate) fn with_static_lifetime(ty: &Type, lifetime: Option<&Lifetime>) -> TokenStream {
    match lifetime {
        Some(lifetime) => replace_lifetime(ty.to_token_stream(), &lifetime.ident),
        None => ty.to_token_stream(),
    }
}

fn replace_lifetime(tokens: TokenStream, name: &Ident) -> TokenStream {
    let mut output = Vec::new();
    let mut tokens = tokens.into_iter().peekable();
    while let Some(token) = tokens.next() {
        match token {
            TokenTree::Punct(punct)
                if punct.as_char() == '\'' && punct.spacing() == Spacing::Joint =>
            {
                match tokens.peek() {
                    Some(TokenTree::Ident(ident)) if ident == name => {
                        tokens.next();
                        output.extend(quote!('static));
                    }
                    _ => output.push(TokenTree::Punct(punct)),
                }
            }
            TokenTree::Group(group) => {
                let mut replaced = proc_macro2::Group::new(
                    group.delimiter(),
                    replace_lifetime(group.stream(), name),
                );
                replaced.set_span(group.span());
                output.push(TokenTree::Group(replaced));
            }
            token => output.push(token),
        }
    }
    output.into_iter().collect()
}
