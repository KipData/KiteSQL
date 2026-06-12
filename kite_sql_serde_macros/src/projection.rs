use darling::ast::Data;
use darling::{FromDeriveInput, FromField};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{parse_quote, DeriveInput, Error, Generics, Ident, LitStr, Type};

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(projection), supports(struct_named))]
struct ProjectionOpts {
    ident: Ident,
    generics: Generics,
    data: Data<(), ProjectionFieldOpts>,
}

#[derive(Debug, FromField)]
#[darling(attributes(projection))]
struct ProjectionFieldOpts {
    ident: Option<Ident>,
    ty: Type,
    rename: Option<String>,
    from: Option<String>,
}

pub(crate) fn handle(ast: DeriveInput) -> Result<TokenStream, Error> {
    let projection_opts = ProjectionOpts::from_derive_input(&ast)?;
    let struct_name = &projection_opts.ident;
    let mut generics = projection_opts.generics.clone();

    let Data::Struct(data_struct) = projection_opts.data else {
        return Err(Error::new_spanned(
            struct_name,
            "Projection only supports structs with named fields",
        ));
    };

    let mut projection_exprs = Vec::new();
    let mut assignments = Vec::new();

    for field in data_struct.fields {
        let ProjectionFieldOpts {
            ident,
            ty: field_ty,
            rename,
            from,
        } = field;
        let field_name = ident.ok_or_else(|| {
            Error::new_spanned(struct_name, "Projection only supports named struct fields")
        })?;
        let field_name_string = field_name.to_string();
        let source_name = rename.clone().unwrap_or_else(|| field_name_string.clone());
        let source_name_lit = LitStr::new(&source_name, Span::call_site());
        let field_name_lit = LitStr::new(&field_name_string, Span::call_site());
        let relation_expr = if let Some(source_relation) = from {
            let relation_lit = LitStr::new(&source_relation, Span::call_site());
            quote!(#relation_lit)
        } else {
            quote!(relation)
        };

        generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#field_ty : ::kite_sql::orm::FromDataValue));

        projection_exprs.push(if rename.is_some() {
            quote! {
                {
                    let expr = scope.column_ref(#relation_expr, #source_name_lit)?;
                    scope.alias(expr, #field_name_lit)
                }
            }
        } else {
            quote! {
                scope.column_ref(#relation_expr, #source_name_lit)?
            }
        });
        assignments.push(quote! {
            if let Some(value) = ::kite_sql::orm::try_get::<#field_ty>(&mut tuple, schema, #field_name_lit) {
                struct_instance.#field_name = value;
            }
        });
    }

    let mut from_generics = generics.clone();
    from_generics.params.insert(0, parse_quote!('__kite_arena));
    from_generics.params.insert(0, parse_quote!('__kite_schema));
    let (from_impl_generics, _, from_where_clause) = from_generics.split_for_impl();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::kite_sql::orm::Projection for #struct_name #ty_generics
        #where_clause
        {
            fn bind_projection<'ctx, 'bind, 'parent, 'arena, T, A>(
                scope: &mut ::kite_sql::orm::ExprBindScope<'ctx, 'bind, 'parent, 'arena, T, A>,
                relation: &str,
            ) -> ::std::result::Result<::std::vec::Vec<::kite_sql::expression::ScalarExpression>, ::kite_sql::errors::DatabaseError>
            where
                T: ::kite_sql::storage::Transaction,
                A: AsRef<[(&'static str, ::kite_sql::types::value::DataValue)]>,
            {
                Ok(::std::vec![#(#projection_exprs),*])
            }
        }

        impl #from_impl_generics From<(&::kite_sql::types::tuple::SchemaView<'__kite_schema, '__kite_arena>, ::kite_sql::types::tuple::Tuple)> for #struct_name #ty_generics
        #from_where_clause
        {
            fn from((schema, mut tuple): (&::kite_sql::types::tuple::SchemaView<'__kite_schema, '__kite_arena>, ::kite_sql::types::tuple::Tuple)) -> Self {
                let mut struct_instance = <Self as ::std::default::Default>::default();
                #(#assignments)*
                struct_instance
            }
        }
    })
}
