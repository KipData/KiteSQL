use darling::ast::Data;
use darling::{FromDeriveInput, FromField};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_quote, DeriveInput, Error, Generics, Ident, Type};

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(from_tuple), supports(struct_named))]
struct FromTupleOpts {
    ident: Ident,
    generics: Generics,
    data: Data<(), FromTupleFieldOpts>,
}

#[derive(Debug, FromField)]
#[darling(attributes(from_tuple))]
struct FromTupleFieldOpts {
    ident: Option<Ident>,
    ty: Type,
    rename: Option<String>,
    #[darling(default)]
    skip: bool,
}

pub(crate) fn handle(ast: DeriveInput) -> Result<TokenStream, Error> {
    let from_tuple_opts = FromTupleOpts::from_derive_input(&ast)?;
    let struct_name = &from_tuple_opts.ident;
    let mut generics = from_tuple_opts.generics.clone();

    let Data::Struct(data_struct) = from_tuple_opts.data else {
        return Err(Error::new_spanned(
            struct_name,
            "FromTuple only supports structs with named fields",
        ));
    };

    let mut assignments = Vec::new();

    for field in data_struct.fields {
        let field_name = field.ident.ok_or_else(|| {
            Error::new_spanned(struct_name, "FromTuple only supports named struct fields")
        })?;
        let field_ty = field.ty;

        if field.skip {
            continue;
        }

        let column_name = field.rename.unwrap_or_else(|| field_name.to_string());
        generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#field_ty : ::kite_sql::orm::FromDataValue));

        assignments.push(quote! {
            if let Some(value) = ::kite_sql::orm::try_get::<#field_ty>(&mut tuple, schema, #column_name) {
                struct_instance.#field_name = value;
            }
        });
    }

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::core::convert::From<(&::kite_sql::types::tuple::SchemaRef, ::kite_sql::types::tuple::Tuple)>
            for #struct_name #ty_generics
            #where_clause
        {
            fn from((schema, mut tuple): (&::kite_sql::types::tuple::SchemaRef, ::kite_sql::types::tuple::Tuple)) -> Self {
                let mut struct_instance = <Self as ::core::default::Default>::default();

                #(#assignments)*

                struct_instance
            }
        }
    })
}
