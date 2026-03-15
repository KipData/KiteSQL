use darling::ast::Data;
use darling::{FromDeriveInput, FromField};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{parse_quote, DeriveInput, Error, Generics, Ident, LitStr, Type};

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(model), supports(struct_named))]
struct OrmOpts {
    ident: Ident,
    generics: Generics,
    table: Option<String>,
    data: Data<(), OrmFieldOpts>,
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
    default_expr: Option<String>,
    #[darling(default)]
    skip: bool,
    #[darling(default)]
    primary_key: bool,
    #[darling(default)]
    unique: bool,
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

    let mut assignments = Vec::new();
    let mut params = Vec::new();
    let mut orm_fields = Vec::new();
    let mut column_names = Vec::new();
    let mut placeholder_names = Vec::new();
    let mut update_assignments = Vec::new();
    let mut ddl_columns = Vec::new();
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
            if field.default_expr.is_some() {
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
        let default_expr = field
            .default_expr
            .map(|value| LitStr::new(&value, Span::call_site()));
        let column_name = field.rename.unwrap_or_else(|| field_name.to_string());
        let placeholder_name = format!(":{column_name}");
        let column_name_lit = LitStr::new(&column_name, Span::call_site());
        let placeholder_lit = LitStr::new(&placeholder_name, Span::call_site());
        let is_primary_key = field.primary_key;
        let is_unique = field.unique;

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
        } else {
            update_assignments.push(format!("{column_name} = {placeholder_name}"));
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

        let ddl_type = if let Some(varchar_len) = varchar_len {
            quote! { ::std::format!("varchar({})", #varchar_len) }
        } else if let Some(char_len) = char_len {
            quote! { ::std::format!("char({})", #char_len) }
        } else if let Some(decimal_precision) = decimal_precision {
            if let Some(decimal_scale) = decimal_scale {
                quote! { ::std::format!("decimal({}, {})", #decimal_precision, #decimal_scale) }
            } else {
                quote! { ::std::format!("decimal({})", #decimal_precision) }
            }
        } else {
            quote! { <#field_ty as ::kite_sql::orm::ModelColumnType>::ddl_type() }
        };
        let default_clause = if let Some(default_expr) = default_expr {
            quote! {
                column_def.push_str(" default ");
                column_def.push_str(#default_expr);
            }
        } else {
            quote! {}
        };

        assignments.push(quote! {
            if let Some(value) = ::kite_sql::orm::try_get::<#field_ty>(&mut tuple, schema, #column_name_lit) {
                struct_instance.#field_name = value;
            }
        });
        params.push(quote! {
            (#placeholder_lit, ::kite_sql::orm::ToDataValue::to_data_value(&self.#field_name))
        });
        orm_fields.push(quote! {
            ::kite_sql::orm::OrmField {
                column: #column_name_lit,
                placeholder: #placeholder_lit,
                primary_key: #is_primary_key,
                unique: #is_unique,
            }
        });
        ddl_columns.push(quote! {
            {
                let ddl_type = #ddl_type;
                let mut column_def = ::std::format!(
                    "{} {}",
                    #column_name_lit,
                    ddl_type,
                );
                if #is_primary_key {
                    column_def.push_str(" primary key");
                } else {
                    if !<#field_ty as ::kite_sql::orm::ModelColumnType>::nullable() {
                        column_def.push_str(" not null");
                    }
                    if #is_unique {
                        column_def.push_str(" unique");
                    }
                }
                #default_clause
                column_def
            }
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
    let primary_key_column = primary_key_column.expect("primary key checked above");
    let primary_key_placeholder = primary_key_placeholder.expect("primary key checked above");
    let select_sql = LitStr::new(
        &format!("select {} from {}", column_names.join(", "), table_name,),
        Span::call_site(),
    );
    let insert_sql = LitStr::new(
        &format!(
            "insert into {} ({}) values ({})",
            table_name,
            column_names.join(", "),
            placeholder_names.join(", "),
        ),
        Span::call_site(),
    );
    let update_sql = LitStr::new(
        &format!(
            "update {} set {} where {} = {}",
            table_name,
            update_assignments.join(", "),
            primary_key_column,
            primary_key_placeholder,
        ),
        Span::call_site(),
    );
    let delete_sql = LitStr::new(
        &format!(
            "delete from {} where {} = {}",
            table_name, primary_key_column, primary_key_placeholder,
        ),
        Span::call_site(),
    );
    let find_sql = LitStr::new(
        &format!(
            "select {} from {} where {} = {}",
            column_names.join(", "),
            table_name,
            primary_key_column,
            primary_key_placeholder,
        ),
        Span::call_site(),
    );
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

            fn params(&self) -> Vec<(&'static str, ::kite_sql::types::value::DataValue)> {
                vec![
                    #(#params),*
                ]
            }

            fn primary_key(&self) -> &Self::PrimaryKey {
                #primary_key_value
            }

            fn select_statement() -> &'static ::kite_sql::db::Statement {
                static SELECT_STATEMENT: ::std::sync::LazyLock<::kite_sql::db::Statement> = ::std::sync::LazyLock::new(|| {
                    ::kite_sql::db::prepare(#select_sql).expect("failed to prepare ORM select statement")
                });
                &SELECT_STATEMENT
            }

            fn insert_statement() -> &'static ::kite_sql::db::Statement {
                static INSERT_STATEMENT: ::std::sync::LazyLock<::kite_sql::db::Statement> = ::std::sync::LazyLock::new(|| {
                    ::kite_sql::db::prepare(#insert_sql).expect("failed to prepare ORM insert statement")
                });
                &INSERT_STATEMENT
            }

            fn update_statement() -> &'static ::kite_sql::db::Statement {
                static UPDATE_STATEMENT: ::std::sync::LazyLock<::kite_sql::db::Statement> = ::std::sync::LazyLock::new(|| {
                    ::kite_sql::db::prepare(#update_sql).expect("failed to prepare ORM update statement")
                });
                &UPDATE_STATEMENT
            }

            fn delete_statement() -> &'static ::kite_sql::db::Statement {
                static DELETE_STATEMENT: ::std::sync::LazyLock<::kite_sql::db::Statement> = ::std::sync::LazyLock::new(|| {
                    ::kite_sql::db::prepare(#delete_sql).expect("failed to prepare ORM delete statement")
                });
                &DELETE_STATEMENT
            }

            fn find_statement() -> &'static ::kite_sql::db::Statement {
                static FIND_STATEMENT: ::std::sync::LazyLock<::kite_sql::db::Statement> = ::std::sync::LazyLock::new(|| {
                    ::kite_sql::db::prepare(#find_sql).expect("failed to prepare ORM find statement")
                });
                &FIND_STATEMENT
            }

            fn create_table_statement() -> &'static ::kite_sql::db::Statement {
                static CREATE_TABLE_STATEMENT: ::std::sync::LazyLock<::kite_sql::db::Statement> = ::std::sync::LazyLock::new(|| {
                    let sql = ::std::format!(
                        "create table {} ({})",
                        #table_name_lit,
                        vec![#(#ddl_columns),*].join(", ")
                    );
                    ::kite_sql::db::prepare(&sql).expect("failed to prepare ORM create table statement")
                });
                &CREATE_TABLE_STATEMENT
            }

            fn create_table_if_not_exists_statement() -> &'static ::kite_sql::db::Statement {
                static CREATE_TABLE_IF_NOT_EXISTS_STATEMENT: ::std::sync::LazyLock<::kite_sql::db::Statement> = ::std::sync::LazyLock::new(|| {
                    let sql = ::std::format!(
                        "create table if not exists {} ({})",
                        #table_name_lit,
                        vec![#(#ddl_columns),*].join(", ")
                    );
                    ::kite_sql::db::prepare(&sql).expect("failed to prepare ORM create table if not exists statement")
                });
                &CREATE_TABLE_IF_NOT_EXISTS_STATEMENT
            }

            fn drop_table_statement() -> &'static ::kite_sql::db::Statement {
                static DROP_TABLE_STATEMENT: ::std::sync::LazyLock<::kite_sql::db::Statement> = ::std::sync::LazyLock::new(|| {
                    let sql = ::std::format!("drop table {}", #table_name_lit);
                    ::kite_sql::db::prepare(&sql).expect("failed to prepare ORM drop table statement")
                });
                &DROP_TABLE_STATEMENT
            }

            fn drop_table_if_exists_statement() -> &'static ::kite_sql::db::Statement {
                static DROP_TABLE_IF_EXISTS_STATEMENT: ::std::sync::LazyLock<::kite_sql::db::Statement> = ::std::sync::LazyLock::new(|| {
                    let sql = ::std::format!("drop table if exists {}", #table_name_lit);
                    ::kite_sql::db::prepare(&sql).expect("failed to prepare ORM drop table if exists statement")
                });
                &DROP_TABLE_IF_EXISTS_STATEMENT
            }
        }
    })
}
