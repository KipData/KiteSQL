use darling::ast::Data;
use darling::{FromDeriveInput, FromField, FromMeta};
use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use std::collections::BTreeSet;
use syn::{parse_quote, DeriveInput, Error, Generics, Ident, LitStr, Type};

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(model), supports(struct_named))]
struct OrmOpts {
    ident: Ident,
    generics: Generics,
    table: Option<String>,
    #[darling(default, multiple, rename = "index")]
    indexes: Vec<ModelIndexOpts>,
    data: Data<(), OrmFieldOpts>,
}

#[derive(Debug, FromMeta)]
struct ModelIndexOpts {
    name: String,
    columns: String,
    #[darling(default)]
    unique: bool,
}

#[derive(Debug, FromField)]
#[darling(attributes(model))]
struct OrmFieldOpts {
    ident: Option<Ident>,
    ty: Type,
    rename: Option<String>,
    varchar: Option<u32>,
    #[darling(rename = "char")]
    char_len: Option<u32>,
    decimal_precision: Option<u8>,
    decimal_scale: Option<u8>,
    #[darling(rename = "default")]
    default_literal: Option<String>,
    #[darling(default)]
    skip: bool,
    #[darling(default)]
    primary_key: bool,
    #[darling(default)]
    unique: bool,
    #[darling(default)]
    index: bool,
}

pub(crate) fn handle(ast: DeriveInput) -> Result<TokenStream, Error> {
    let orm_opts = OrmOpts::from_derive_input(&ast)?;
    let struct_name = &orm_opts.ident;
    let table_name = orm_opts.table.ok_or_else(|| {
        Error::new_spanned(
            struct_name,
            "Model requires #[model(table = \"table_name\")] on the struct",
        )
    })?;
    let table_name_lit = LitStr::new(&table_name, Span::call_site());
    let mut generics = orm_opts.generics.clone();

    let Data::Struct(data_struct) = orm_opts.data else {
        return Err(Error::new_spanned(
            struct_name,
            "Model only supports structs with named fields",
        ));
    };

    let mut field_initializers = Vec::new();
    let mut field_index_declarations = Vec::new();
    let mut field_index_resolvers = Vec::new();
    let mut params = Vec::new();
    let mut orm_fields = Vec::new();
    let mut orm_columns = Vec::new();
    let mut field_getters = Vec::new();
    let mut column_names = Vec::new();
    let mut placeholder_names = Vec::new();
    let mut orm_indexes = Vec::new();
    let mut persisted_columns = Vec::new();
    let mut index_names = BTreeSet::new();
    index_names.insert("pk_index".to_string());
    let mut primary_key_type = None;
    let mut primary_key_value = None;
    let mut primary_key_column = None;
    let mut primary_key_placeholder = None;
    let mut primary_key_count = 0usize;

    for field in data_struct.fields {
        let field_name = field.ident.ok_or_else(|| {
            Error::new_spanned(struct_name, "Model only supports named struct fields")
        })?;
        let field_ty = field.ty;

        if field.skip {
            if field.primary_key {
                return Err(Error::new_spanned(
                    field_name,
                    "primary key field cannot be skipped",
                ));
            }
            if field.unique {
                return Err(Error::new_spanned(
                    field_name,
                    "unique field cannot be skipped",
                ));
            }
            if field.index {
                return Err(Error::new_spanned(
                    field_name,
                    "index field cannot be skipped",
                ));
            }
            if field.varchar.is_some() {
                return Err(Error::new_spanned(
                    field_name,
                    "varchar field cannot be skipped",
                ));
            }
            if field.char_len.is_some() {
                return Err(Error::new_spanned(
                    field_name,
                    "char field cannot be skipped",
                ));
            }
            if field.default_literal.is_some() {
                return Err(Error::new_spanned(
                    field_name,
                    "default field cannot be skipped",
                ));
            }
            if field.decimal_precision.is_some() || field.decimal_scale.is_some() {
                return Err(Error::new_spanned(
                    field_name,
                    "decimal field cannot be skipped",
                ));
            }
            generics
                .make_where_clause()
                .predicates
                .push(parse_quote!(#field_ty : ::core::default::Default));
            field_initializers.push(quote! {
                #field_name: ::core::default::Default::default()
            });
            continue;
        }

        let varchar_len = field.varchar;
        let char_len = field.char_len;
        let decimal_precision = field.decimal_precision;
        let decimal_scale = field.decimal_scale;
        if varchar_len.is_some() && char_len.is_some() {
            return Err(Error::new_spanned(
                field_name,
                "char and varchar cannot be used together",
            ));
        }
        if decimal_scale.is_some() && decimal_precision.is_none() {
            return Err(Error::new_spanned(
                field_name,
                "decimal_scale requires decimal_precision",
            ));
        }
        let default_literal = field
            .default_literal
            .map(|value| LitStr::new(&value, Span::call_site()));
        let field_name_string = field_name.to_string();
        let column_name = field.rename.unwrap_or_else(|| field_name_string.clone());
        let placeholder_name = format!(":{column_name}");
        let column_name_lit = LitStr::new(&column_name, Span::call_site());
        let placeholder_lit = LitStr::new(&placeholder_name, Span::call_site());
        let is_primary_key = field.primary_key;
        let is_unique = field.unique;
        let is_index = field.index;
        let column_index = orm_columns.len();
        let field_index_ident = format_ident!("__kite_orm_{field_name}_index");

        persisted_columns.push((field_name_string, column_name.clone()));
        column_names.push(column_name.clone());
        placeholder_names.push(placeholder_name.clone());

        if is_primary_key {
            primary_key_count += 1;
            primary_key_column = Some(column_name.clone());
            primary_key_placeholder = Some(placeholder_name.clone());
            primary_key_type = Some(quote! { #field_ty });
            primary_key_value = Some(quote! {
                &self.#field_name
            });
        }

        generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#field_ty : ::kite_sql::orm::FromDataValue));
        generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#field_ty : ::kite_sql::orm::ToDataValue));
        generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#field_ty : ::kite_sql::orm::ModelColumnType));

        if varchar_len.is_some() || char_len.is_some() {
            generics
                .make_where_clause()
                .predicates
                .push(parse_quote!(#field_ty : ::kite_sql::orm::StringType));
        }
        if decimal_precision.is_some() || decimal_scale.is_some() {
            generics
                .make_where_clause()
                .predicates
                .push(parse_quote!(#field_ty : ::kite_sql::orm::DecimalType));
        }

        let data_type = if let Some(varchar_len) = varchar_len {
            quote! { ::kite_sql::types::LogicalType::Varchar(Some(#varchar_len), ::kite_sql::types::CharLengthUnits::Characters) }
        } else if let Some(char_len) = char_len {
            quote! { ::kite_sql::types::LogicalType::Char(#char_len, ::kite_sql::types::CharLengthUnits::Characters) }
        } else if let Some(decimal_precision) = decimal_precision {
            if let Some(decimal_scale) = decimal_scale {
                quote! { ::kite_sql::types::LogicalType::Decimal(Some(#decimal_precision), Some(#decimal_scale)) }
            } else {
                quote! { ::kite_sql::types::LogicalType::Decimal(Some(#decimal_precision), None) }
            }
        } else {
            quote! { <#field_ty as ::kite_sql::orm::ModelColumnType>::logical_type() }
        };
        let default_tokens = if let Some(default_literal) = &default_literal {
            quote! { Some(::kite_sql::types::value::DataValue::from(#default_literal.to_string())) }
        } else {
            quote! { None::<::kite_sql::types::value::DataValue> }
        };

        field_index_declarations.push(quote! {
            let mut #field_index_ident = None;
        });
        field_index_resolvers.push(quote! {
            if #field_index_ident.is_none() && __kite_orm_column_name == #column_name_lit {
                #field_index_ident = Some(__kite_orm_index);
                __kite_orm_found_fields += 1;
            }
        });
        field_initializers.push(quote! {
            #field_name: ::kite_sql::orm::take_value_at::<#field_ty>(
                &mut tuple,
                #field_index_ident,
                #column_name_lit,
            )?
        });
        params.push(quote! {
            (#placeholder_lit, ::kite_sql::orm::ToDataValue::to_data_value(&self.#field_name))
        });
        orm_fields.push(quote! {
            ::kite_sql::orm::OrmField {
                column: #column_name_lit,
                column_index: #column_index,
                placeholder: #placeholder_lit,
                primary_key: #is_primary_key,
                unique: #is_unique,
            }
        });
        let getter_name = format_ident!("{}", field_name);
        field_getters.push(quote! {
            pub fn #getter_name() -> ::kite_sql::orm::Field<Self, #field_ty> {
                ::kite_sql::orm::Field::new(#table_name_lit, #column_name_lit)
            }
        });
        orm_columns.push(quote! {
            {
                let data_type = #data_type;
                let default = #default_tokens
                    .map(|value| {
                        ::kite_sql::expression::ScalarExpression::Constant(
                            value
                                .cast(&data_type)
                                .expect("failed to cast ORM default value to column type"),
                        )
                    });
                let desc = ::kite_sql::catalog::column::ColumnDesc::new(
                    data_type,
                    #is_primary_key.then_some(#column_index),
                    #is_unique,
                    default,
                )
                    .expect("failed to build ORM column descriptor");
                ::kite_sql::catalog::column::ColumnCatalog::new(
                    #column_name_lit.to_string(),
                    if #is_primary_key {
                        false
                    } else {
                        <#field_ty as ::kite_sql::orm::ModelColumnType>::nullable()
                    },
                    desc,
                )
            }
        });
        if is_unique {
            let unique_index_name_value = format!("uk_{column_name}_index");
            if !index_names.insert(unique_index_name_value.clone()) {
                return Err(Error::new_spanned(
                    struct_name,
                    format!("duplicate ORM index name: {unique_index_name_value}"),
                ));
            }
        }
        if is_index {
            let index_name = format!("{table_name}_{column_name}_index");
            let index_name_lit = LitStr::new(&index_name, Span::call_site());
            let column_name_for_index = column_name_lit.clone();
            if !index_names.insert(index_name.clone()) {
                return Err(Error::new_spanned(
                    struct_name,
                    format!("duplicate ORM index name: {index_name}"),
                ));
            }
            orm_indexes.push(quote! {
                (#index_name_lit, &[#column_name_for_index], false)
            });
        }
    }

    for index in orm_opts.indexes {
        if !index_names.insert(index.name.clone()) {
            return Err(Error::new_spanned(
                struct_name,
                format!("duplicate ORM index name: {}", index.name),
            ));
        }

        let mut resolved_columns = Vec::new();
        let mut seen_columns = BTreeSet::new();
        for raw_column in index.columns.split(',') {
            let raw_column = raw_column.trim();
            if raw_column.is_empty() {
                continue;
            }

            let column_name = persisted_columns
                .iter()
                .find_map(|(field_name, column_name)| {
                    if field_name == raw_column || column_name == raw_column {
                        Some(column_name.clone())
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    Error::new_spanned(
                        struct_name,
                        format!(
                            "unknown ORM index column `{raw_column}`; use a persisted field name or column name",
                        ),
                    )
                })?;

            if !seen_columns.insert(column_name.clone()) {
                return Err(Error::new_spanned(
                    struct_name,
                    format!(
                        "duplicate ORM index column `{}` in index `{}`",
                        column_name, index.name
                    ),
                ));
            }

            resolved_columns.push(column_name);
        }

        if resolved_columns.is_empty() {
            return Err(Error::new_spanned(
                struct_name,
                format!(
                    "ORM index `{}` must reference at least one column",
                    index.name
                ),
            ));
        }

        let index_name_lit = LitStr::new(&index.name, Span::call_site());
        let index_columns = resolved_columns
            .iter()
            .map(|column| LitStr::new(column, Span::call_site()))
            .collect::<Vec<_>>();
        let is_unique = index.unique;
        orm_indexes.push(quote! {
            (#index_name_lit, &[#(#index_columns),*], #is_unique)
        });
    }

    if primary_key_count != 1 {
        return Err(Error::new_spanned(
            struct_name,
            "Model requires exactly one #[model(primary_key)] field",
        ));
    }

    let primary_key_type = primary_key_type.expect("primary key checked above");
    let primary_key_value = primary_key_value.expect("primary key checked above");
    let _primary_key_column = primary_key_column.expect("primary key checked above");
    let _primary_key_placeholder = primary_key_placeholder.expect("primary key checked above");
    let field_count = field_index_declarations.len();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::kite_sql::orm::FromQueryRow
            for #struct_name #ty_generics
            #where_clause
        {
            fn from_query_row(
                schema: &::kite_sql::types::tuple::SchemaView<'_, '_>,
                mut tuple: ::kite_sql::types::tuple::Tuple,
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

        impl #impl_generics #struct_name #ty_generics
            #where_clause
        {
            #(#field_getters)*
        }

        impl #impl_generics ::kite_sql::orm::Model for #struct_name #ty_generics
            #where_clause
        {
            type PrimaryKey = #primary_key_type;

            fn table_name() -> &'static str {
                #table_name_lit
            }

            fn fields() -> &'static [::kite_sql::orm::OrmField] {
                &[
                    #(#orm_fields),*
                ]
            }

            fn columns() -> &'static [::kite_sql::catalog::column::ColumnCatalog] {
                static ORM_COLUMNS: ::std::sync::LazyLock<::std::vec::Vec<::kite_sql::catalog::column::ColumnCatalog>> = ::std::sync::LazyLock::new(|| {
                    vec![
                        #(#orm_columns),*
                    ]
                });
                ORM_COLUMNS.as_slice()
            }

            fn indexes() -> &'static [(&'static str, &'static [&'static str], bool)] {
                &[
                    #(#orm_indexes),*
                ]
            }

            fn params(&self) -> Vec<(&'static str, ::kite_sql::types::value::DataValue)> {
                vec![
                    #(#params),*
                ]
            }

            fn primary_key(&self) -> &Self::PrimaryKey {
                #primary_key_value
            }
        }
    })
}
