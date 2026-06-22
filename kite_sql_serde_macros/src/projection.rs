use darling::ast::Data;
use darling::{FromDeriveInput, FromField};
use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
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
    let mut field_initializers = Vec::new();
    let mut field_index_declarations = Vec::new();
    let mut field_index_resolvers = Vec::new();

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
        let field_index_ident = format_ident!("__kite_projection_{field_name}_index");
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
        field_index_declarations.push(quote! {
            let mut #field_index_ident = None;
        });
        field_index_resolvers.push(quote! {
            if #field_index_ident.is_none() && __kite_orm_column_name == #field_name_lit {
                #field_index_ident = Some(__kite_orm_index);
                __kite_orm_found_fields += 1;
            }
        });
        field_initializers.push(quote! {
            #field_name: ::kite_sql::orm::take_value_at::<#field_ty>(
                tuple,
                #field_index_ident,
                #field_name_lit,
            )?
        });
    }

    let field_count = field_index_declarations.len();
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
                Ok(::std::vec![
                    #(::kite_sql::orm::IntoOrmScalarExpression::into_orm_scalar(#projection_exprs)),*
                ])
            }
        }

        impl #impl_generics ::kite_sql::orm::FromQueryRow for #struct_name #ty_generics
        #where_clause
        {
            fn from_query_row(
                schema: &::kite_sql::types::tuple::SchemaView<'_, '_>,
                tuple: &mut ::kite_sql::types::tuple::Tuple,
            ) -> ::std::result::Result<Self, ::kite_sql::errors::DatabaseError> {
                let mut __kite_orm_found_fields = 0usize;
                #(#field_index_declarations)*
                for (__kite_orm_index, __kite_orm_column) in schema.iter().enumerate() {
                    let __kite_orm_column_name = __kite_orm_column.name();
                    #(#field_index_resolvers)*
                    if __kite_orm_found_fields == #field_count {
                        break;
                    }
                }

                Ok(Self {
                    #(#field_initializers),*
                })
            }
        }
    })
}
