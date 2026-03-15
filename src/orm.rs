use crate::catalog::{ColumnRef, TableCatalog};
use crate::db::{
    DBTransaction, Database, DatabaseIter, OrmIter, ResultIter, Statement, TransactionIter,
};
use crate::errors::DatabaseError;
use crate::storage::{Storage, Transaction};
use crate::types::tuple::{SchemaRef, Tuple};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use sqlparser::ast::CharLengthUnits;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Static metadata about a single model field.
///
/// This type is primarily consumed by code generated from `#[derive(Model)]`.
pub struct OrmField {
    pub column: &'static str,
    pub placeholder: &'static str,
    pub primary_key: bool,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Static metadata about a single persisted model column.
///
/// This is primarily consumed by the built-in ORM migration helper.
pub struct OrmColumn {
    pub name: &'static str,
    pub ddl_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default_expr: Option<&'static str>,
}

impl OrmColumn {
    /// Renders the SQL column definition used by `CREATE TABLE` and migrations.
    pub fn definition_sql(&self) -> String {
        let mut column_def = ::std::format!("{} {}", self.name, self.ddl_type);

        if self.primary_key {
            column_def.push_str(" primary key");
        } else {
            if !self.nullable {
                column_def.push_str(" not null");
            }
            if self.unique {
                column_def.push_str(" unique");
            }
        }
        if let Some(default_expr) = self.default_expr {
            column_def.push_str(" default ");
            column_def.push_str(default_expr);
        }

        column_def
    }
}

/// Trait implemented by ORM models.
///
/// In normal usage you should derive this trait with `#[derive(Model)]` rather
/// than implementing it by hand. The derive macro generates tuple mapping,
/// cached CRUD/DDL statements and model metadata.
pub trait Model: Sized + for<'a> From<(&'a SchemaRef, Tuple)> {
    /// Rust type used as the model primary key.
    ///
    /// This associated type lets APIs such as
    /// [`Database::get`](crate::orm::Database::get) and
    /// [`Database::delete_by_id`](crate::orm::Database::delete_by_id)
    /// infer the key type directly from the model, so callers only need to
    /// write `database.get::<User>(&id)`.
    type PrimaryKey: ToDataValue;

    /// Returns the backing table name for the model.
    fn table_name() -> &'static str;

    /// Returns metadata for every persisted field on the model.
    fn fields() -> &'static [OrmField];

    /// Returns persisted column definitions for the model.
    ///
    /// `#[derive(Model)]` generates this automatically. Manual implementations
    /// can override it to opt into [`Database::migrate`](crate::orm::Database::migrate).
    fn columns() -> &'static [OrmColumn] {
        &[]
    }

    /// Converts the model into named query parameters.
    fn params(&self) -> Vec<(&'static str, DataValue)>;

    /// Returns a reference to the current primary-key value.
    fn primary_key(&self) -> &Self::PrimaryKey;

    /// Returns the cached `SELECT` statement used by [`Database::list`](crate::orm::Database::list).
    fn select_statement() -> &'static Statement;

    /// Returns the cached `INSERT` statement for the model.
    fn insert_statement() -> &'static Statement;

    /// Returns the cached `UPDATE` statement for the model.
    fn update_statement() -> &'static Statement;

    /// Returns the cached `DELETE` statement for the model.
    fn delete_statement() -> &'static Statement;

    /// Returns the cached `SELECT .. WHERE primary_key = ...` statement.
    fn find_statement() -> &'static Statement;

    /// Returns the cached `CREATE TABLE` statement for the model.
    fn create_table_statement() -> &'static Statement;

    /// Returns the cached `CREATE TABLE IF NOT EXISTS` statement for the model.
    fn create_table_if_not_exists_statement() -> &'static Statement;

    /// Returns cached `CREATE INDEX` statements declared by the model.
    ///
    /// `#[derive(Model)]` generates these from fields annotated with
    /// `#[model(index)]`. Manual implementations can override this to provide
    /// custom secondary indexes.
    fn create_index_statements() -> &'static [Statement] {
        &[]
    }

    /// Returns cached `CREATE INDEX IF NOT EXISTS` statements declared by the model.
    fn create_index_if_not_exists_statements() -> &'static [Statement] {
        &[]
    }

    /// Returns the cached `DROP TABLE` statement for the model.
    fn drop_table_statement() -> &'static Statement;

    /// Returns the cached `DROP TABLE IF EXISTS` statement for the model.
    fn drop_table_if_exists_statement() -> &'static Statement;

    /// Returns the cached `ANALYZE TABLE` statement for the model.
    fn analyze_statement() -> &'static Statement;

    fn primary_key_field() -> &'static OrmField {
        Self::fields()
            .iter()
            .find(|field| field.primary_key)
            .expect("ORM model must define exactly one primary key field")
    }
}

/// Conversion trait from [`DataValue`] into Rust values for ORM mapping.
///
/// This trait is mainly intended for framework internals and derive-generated
/// code.
pub trait FromDataValue: Sized {
    fn logical_type() -> Option<LogicalType>;

    fn from_data_value(value: DataValue) -> Option<Self>;
}

/// Conversion trait from Rust values into [`DataValue`] for ORM parameters.
///
/// This trait is mainly intended for framework internals and derive-generated
/// code.
pub trait ToDataValue {
    fn to_data_value(&self) -> DataValue;
}

/// Maps a Rust field type to the SQL column type used by ORM DDL helpers.
///
/// `#[derive(Model)]` relies on this trait to build `CREATE TABLE` statements.
/// Most built-in scalar types already implement it, and custom types can opt in
/// by implementing this trait together with [`FromDataValue`] and [`ToDataValue`].
pub trait ModelColumnType {
    fn ddl_type() -> String;

    fn nullable() -> bool {
        false
    }
}

/// Marker trait for string-like model fields that support `#[model(varchar = N)]`
/// and `#[model(char = N)]`.
pub trait StringType {}

/// Marker trait for decimal-like model fields that support precision/scale DDL attributes.
pub trait DecimalType {}

/// Extracts and converts a named field from a tuple using the given schema.
///
/// This helper is used by code generated from `#[derive(Model)]` and by the
/// lower-level `from_tuple!` macro.
pub fn try_get<T: FromDataValue>(
    tuple: &mut Tuple,
    schema: &SchemaRef,
    field_name: &str,
) -> Option<T> {
    let ty = T::logical_type()?;
    let (idx, _) = schema
        .iter()
        .enumerate()
        .find(|(_, col)| col.name() == field_name)?;

    let value = std::mem::replace(&mut tuple.values[idx], DataValue::Null)
        .cast(&ty)
        .ok()?;

    T::from_data_value(value)
}

macro_rules! impl_from_data_value_by_method {
    ($ty:ty, $method:ident) => {
        impl FromDataValue for $ty {
            fn logical_type() -> Option<LogicalType> {
                LogicalType::type_trans::<Self>()
            }

            fn from_data_value(value: DataValue) -> Option<Self> {
                value.$method()
            }
        }
    };
}

macro_rules! impl_to_data_value_by_clone {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl ToDataValue for $ty {
                fn to_data_value(&self) -> DataValue {
                    DataValue::from(self.clone())
                }
            }
        )+
    };
}

impl_from_data_value_by_method!(bool, bool);
impl_from_data_value_by_method!(i8, i8);
impl_from_data_value_by_method!(i16, i16);
impl_from_data_value_by_method!(i32, i32);
impl_from_data_value_by_method!(i64, i64);
impl_from_data_value_by_method!(u8, u8);
impl_from_data_value_by_method!(u16, u16);
impl_from_data_value_by_method!(u32, u32);
impl_from_data_value_by_method!(u64, u64);
impl_from_data_value_by_method!(f32, float);
impl_from_data_value_by_method!(f64, double);
impl_from_data_value_by_method!(NaiveDate, date);
impl_from_data_value_by_method!(NaiveDateTime, datetime);
impl_from_data_value_by_method!(NaiveTime, time);
impl_from_data_value_by_method!(Decimal, decimal);

impl_to_data_value_by_clone!(bool, i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, Decimal, String);

macro_rules! impl_model_column_type {
    ($sql:expr; $($ty:ty),+ $(,)?) => {
        $(
            impl ModelColumnType for $ty {
                fn ddl_type() -> String {
                    $sql.to_string()
                }
            }
        )+
    };
}

impl_model_column_type!("boolean"; bool);
impl_model_column_type!("tinyint"; i8);
impl_model_column_type!("smallint"; i16);
impl_model_column_type!("int"; i32);
impl_model_column_type!("bigint"; i64);
impl_model_column_type!("utinyint"; u8);
impl_model_column_type!("usmallint"; u16);
impl_model_column_type!("unsigned integer"; u32);
impl_model_column_type!("ubigint"; u64);
impl_model_column_type!("float"; f32);
impl_model_column_type!("double"; f64);
impl_model_column_type!("date"; NaiveDate);
impl_model_column_type!("datetime"; NaiveDateTime);
impl_model_column_type!("time"; NaiveTime);
impl_model_column_type!("decimal"; Decimal);
impl_model_column_type!("varchar"; String, Arc<str>);

impl StringType for String {}
impl StringType for Arc<str> {}
impl DecimalType for Decimal {}

impl FromDataValue for String {
    fn logical_type() -> Option<LogicalType> {
        LogicalType::type_trans::<Self>()
    }

    fn from_data_value(value: DataValue) -> Option<Self> {
        if let DataValue::Utf8 { value, .. } = value {
            Some(value)
        } else {
            None
        }
    }
}

impl FromDataValue for Arc<str> {
    fn logical_type() -> Option<LogicalType> {
        Some(LogicalType::Varchar(None, CharLengthUnits::Characters))
    }

    fn from_data_value(value: DataValue) -> Option<Self> {
        if let DataValue::Utf8 { value, .. } = value {
            Some(value.into())
        } else {
            None
        }
    }
}

impl ToDataValue for Arc<str> {
    fn to_data_value(&self) -> DataValue {
        DataValue::from(self.to_string())
    }
}

impl ToDataValue for str {
    fn to_data_value(&self) -> DataValue {
        DataValue::from(self.to_string())
    }
}

impl ToDataValue for &str {
    fn to_data_value(&self) -> DataValue {
        DataValue::from((*self).to_string())
    }
}

impl ToDataValue for NaiveDate {
    fn to_data_value(&self) -> DataValue {
        DataValue::from(self)
    }
}

impl ToDataValue for NaiveDateTime {
    fn to_data_value(&self) -> DataValue {
        DataValue::from(self)
    }
}

impl ToDataValue for NaiveTime {
    fn to_data_value(&self) -> DataValue {
        DataValue::from(self)
    }
}

impl<T: FromDataValue> FromDataValue for Option<T> {
    fn logical_type() -> Option<LogicalType> {
        T::logical_type()
    }

    fn from_data_value(value: DataValue) -> Option<Self> {
        if matches!(value, DataValue::Null) {
            Some(None)
        } else {
            T::from_data_value(value).map(Some)
        }
    }
}

impl<T: ToDataValue> ToDataValue for Option<T> {
    fn to_data_value(&self) -> DataValue {
        match self {
            Some(value) => value.to_data_value(),
            None => DataValue::Null,
        }
    }
}

impl<T: ModelColumnType> ModelColumnType for Option<T> {
    fn ddl_type() -> String {
        T::ddl_type()
    }

    fn nullable() -> bool {
        true
    }
}

impl<T: StringType> StringType for Option<T> {}
impl<T: DecimalType> DecimalType for Option<T> {}

fn normalize_sql_fragment(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn canonicalize_model_type(value: &str) -> String {
    let normalized = normalize_sql_fragment(value);

    match normalized.as_str() {
        "boolean" => "boolean".to_string(),
        "tinyint" => "tinyint".to_string(),
        "smallint" => "smallint".to_string(),
        "int" | "integer" => "integer".to_string(),
        "bigint" => "bigint".to_string(),
        "utinyint" => "utinyint".to_string(),
        "usmallint" => "usmallint".to_string(),
        "unsigned integer" | "uinteger" => "uinteger".to_string(),
        "ubigint" => "ubigint".to_string(),
        "float" => "float".to_string(),
        "double" => "double".to_string(),
        "date" => "date".to_string(),
        "datetime" => "datetime".to_string(),
        "time" => "time(some(0))".to_string(),
        "varchar" => "varchar(none, characters)".to_string(),
        "decimal" => "decimal(none, none)".to_string(),
        _ => {
            if let Some(inner) = normalized
                .strip_prefix("varchar(")
                .and_then(|value| value.strip_suffix(')'))
            {
                return ::std::format!("varchar(some({}), characters)", inner.trim());
            }
            if let Some(inner) = normalized
                .strip_prefix("char(")
                .and_then(|value| value.strip_suffix(')'))
            {
                return ::std::format!("char({}, characters)", inner.trim());
            }
            if let Some(inner) = normalized
                .strip_prefix("decimal(")
                .and_then(|value| value.strip_suffix(')'))
            {
                let parts = inner.split(',').map(str::trim).collect::<Vec<_>>();
                return match parts.as_slice() {
                    [precision] => ::std::format!("decimal(some({precision}), none)"),
                    [precision, scale] => {
                        ::std::format!("decimal(some({precision}), some({scale}))")
                    }
                    _ => normalized,
                };
            }
            normalized
        }
    }
}

fn model_column_default(model: &OrmColumn) -> Option<String> {
    model.default_expr.map(normalize_sql_fragment)
}

fn catalog_column_default(column: &ColumnRef) -> Option<String> {
    column
        .desc()
        .default
        .as_ref()
        .map(|expr| normalize_sql_fragment(&expr.to_string()))
}

fn model_column_type_matches_catalog(model: &OrmColumn, column: &ColumnRef) -> bool {
    canonicalize_model_type(&model.ddl_type)
        == normalize_sql_fragment(&column.datatype().to_string())
}

fn model_column_matches_catalog(model: &OrmColumn, column: &ColumnRef) -> bool {
    model.primary_key == column.desc().is_primary()
        && model.unique == column.desc().is_unique()
        && model.nullable == column.nullable()
        && model_column_type_matches_catalog(model, column)
        && model_column_default(model) == catalog_column_default(column)
}

fn model_column_rename_compatible(model: &OrmColumn, column: &ColumnRef) -> bool {
    model.primary_key == column.desc().is_primary()
        && model.unique == column.desc().is_unique()
        && model.nullable == column.nullable()
        && model_column_type_matches_catalog(model, column)
        && model_column_default(model) == catalog_column_default(column)
}

fn extract_optional_model<I, M>(mut iter: I) -> Result<Option<M>, DatabaseError>
where
    I: ResultIter,
    M: Model,
{
    let schema = iter.schema().clone();

    Ok(match iter.next() {
        Some(tuple) => Some(M::from((&schema, tuple?))),
        None => None,
    })
}

impl<S: Storage> Database<S> {
    fn table_catalog(&self, table_name: &str) -> Result<Option<TableCatalog>, DatabaseError> {
        let transaction = self.storage.transaction()?;
        transaction
            .table(self.state.table_cache(), table_name.into())
            .map(|table| table.cloned())
    }

    /// Creates the table described by a model.
    ///
    /// Any secondary indexes declared with `#[model(index)]` are created after
    /// the table itself.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kite_sql::db::DataBaseBuilder;
    /// use kite_sql::Model;
    ///
    /// #[derive(Default, Debug, PartialEq, Model)]
    /// #[model(table = "users")]
    /// struct User {
    ///     #[model(primary_key)]
    ///     id: i32,
    ///     name: String,
    ///     age: Option<i32>,
    /// }
    ///
    /// let database = DataBaseBuilder::path(".").build_in_memory().unwrap();
    /// database.create_table::<User>().unwrap();
    /// ```
    pub fn create_table<M: Model>(&self) -> Result<(), DatabaseError> {
        self.execute(
            M::create_table_statement(),
            Vec::<(&'static str, DataValue)>::new(),
        )?
        .done()?;

        for statement in M::create_index_statements() {
            self.execute(statement, Vec::<(&'static str, DataValue)>::new())?
                .done()?;
        }

        Ok(())
    }

    /// Creates the model table if it does not already exist.
    ///
    /// This is useful for examples, tests and bootstrap flows where rerunning
    /// schema initialization should stay idempotent. Secondary indexes declared
    /// with `#[model(index)]` are created with `IF NOT EXISTS` as well.
    pub fn create_table_if_not_exists<M: Model>(&self) -> Result<(), DatabaseError> {
        self.execute(
            M::create_table_if_not_exists_statement(),
            Vec::<(&'static str, DataValue)>::new(),
        )?
        .done()?;

        for statement in M::create_index_if_not_exists_statements() {
            self.execute(statement, Vec::<(&'static str, DataValue)>::new())?
                .done()?;
        }

        Ok(())
    }

    /// Migrates an existing table to match the current model definition.
    ///
    /// This helper creates the table when it does not exist, adds missing
    /// columns, drops columns that are no longer declared by the model, applies
    /// supported `ALTER TABLE .. CHANGE/ALTER COLUMN` operations for existing
    /// columns, and ensures declared secondary indexes exist.
    ///
    /// The migration can automatically handle safe column renames plus changes
    /// to type, nullability and default expressions for non-primary-key columns
    /// when the underlying DDL supports them. Primary-key changes and unique
    /// constraint changes still return an error so you can handle them manually.
    pub fn migrate<M: Model>(&self) -> Result<(), DatabaseError> {
        let columns = M::columns();
        if columns.is_empty() {
            return Err(DatabaseError::UnsupportedStmt(
                "ORM migration requires Model::columns(); #[derive(Model)] provides it automatically"
                    .to_string(),
            ));
        }

        let Some(table) = self.table_catalog(M::table_name())? else {
            return self.create_table::<M>();
        };

        let model_primary_key = columns
            .iter()
            .find(|column| column.primary_key)
            .ok_or(DatabaseError::PrimaryKeyNotFound)?;
        let table_primary_key = table
            .primary_keys()
            .first()
            .map(|(_, column)| column.clone())
            .ok_or(DatabaseError::PrimaryKeyNotFound)?;
        if table_primary_key.name() != model_primary_key.name
            || !model_column_matches_catalog(model_primary_key, &table_primary_key)
        {
            return Err(DatabaseError::InvalidValue(::std::format!(
                "ORM migration does not support changing the primary key for table `{}`",
                M::table_name(),
            )));
        }

        let current_columns = table
            .columns()
            .map(|column| (column.name().to_string(), column.clone()))
            .collect::<BTreeMap<_, _>>();
        let model_columns = columns
            .iter()
            .map(|column| (column.name, column))
            .collect::<BTreeMap<_, _>>();
        let mut handled_current = BTreeMap::new();
        let mut handled_model = BTreeMap::new();

        for column in columns {
            let Some(current_column) = current_columns.get(column.name) else {
                continue;
            };
            handled_current.insert(current_column.name().to_string(), ());
            handled_model.insert(column.name, ());

            if column.primary_key != current_column.desc().is_primary() {
                return Err(DatabaseError::InvalidValue(::std::format!(
                    "ORM migration does not support changing the primary key for table `{}`",
                    M::table_name(),
                )));
            }
            if column.unique != current_column.desc().is_unique() {
                return Err(DatabaseError::InvalidValue(::std::format!(
                    "ORM migration cannot automatically change unique constraint on column `{}` of table `{}`",
                    column.name,
                    M::table_name(),
                )));
            }
            if model_column_matches_catalog(column, current_column) {
                continue;
            }

            if !model_column_type_matches_catalog(column, current_column) {
                self.run(::std::format!(
                    "alter table {} alter column {} type {}",
                    M::table_name(),
                    column.name,
                    column.ddl_type,
                ))?
                .done()?;
            }

            if model_column_default(column) != catalog_column_default(current_column) {
                if let Some(default_expr) = column.default_expr {
                    self.run(::std::format!(
                        "alter table {} alter column {} set default {}",
                        M::table_name(),
                        column.name,
                        default_expr,
                    ))?
                    .done()?;
                } else {
                    self.run(::std::format!(
                        "alter table {} alter column {} drop default",
                        M::table_name(),
                        column.name,
                    ))?
                    .done()?;
                }
            }

            if column.nullable != current_column.nullable() {
                let op = if column.nullable {
                    "drop not null"
                } else {
                    "set not null"
                };
                self.run(::std::format!(
                    "alter table {} alter column {} {}",
                    M::table_name(),
                    column.name,
                    op,
                ))?
                .done()?;
            }
        }

        let mut rename_pairs = Vec::new();
        let unmatched_model_columns = columns
            .iter()
            .filter(|column| !handled_model.contains_key(column.name))
            .collect::<Vec<_>>();
        let unmatched_current_columns = table
            .columns()
            .filter(|column| !handled_current.contains_key(column.name()))
            .collect::<Vec<_>>();

        for model_column in &unmatched_model_columns {
            if model_column.primary_key {
                continue;
            }
            let candidates = unmatched_current_columns
                .iter()
                .copied()
                .filter(|column| !column.desc().is_primary())
                .filter(|column| model_column_rename_compatible(model_column, column))
                .collect::<Vec<_>>();
            if candidates.len() != 1 {
                continue;
            }
            let current_column = candidates[0];
            let reverse_candidates = unmatched_model_columns
                .iter()
                .filter(|other| !other.primary_key)
                .filter(|other| model_column_rename_compatible(other, current_column))
                .collect::<Vec<_>>();
            if reverse_candidates.len() != 1 {
                continue;
            }
            rename_pairs.push((current_column.name().to_string(), model_column.name));
            handled_current.insert(current_column.name().to_string(), ());
            handled_model.insert(model_column.name, ());
        }

        for (old_name, new_name) in rename_pairs {
            self.run(::std::format!(
                "alter table {} rename column {} to {}",
                M::table_name(),
                old_name,
                new_name,
            ))?
            .done()?;
        }

        for column in table.columns() {
            if handled_current.contains_key(column.name())
                || model_columns.contains_key(column.name())
            {
                continue;
            }
            if column.desc().is_primary() {
                return Err(DatabaseError::InvalidValue(::std::format!(
                    "ORM migration cannot drop the primary key column `{}` from table `{}`",
                    column.name(),
                    M::table_name(),
                )));
            }

            self.run(::std::format!(
                "alter table {} drop column {}",
                M::table_name(),
                column.name(),
            ))?
            .done()?;
        }

        for column in columns {
            if handled_model.contains_key(column.name) || current_columns.contains_key(column.name)
            {
                continue;
            }
            if column.primary_key {
                return Err(DatabaseError::InvalidValue(::std::format!(
                    "ORM migration cannot add a new primary key column `{}` to an existing table `{}`",
                    column.name,
                    M::table_name(),
                )));
            }

            self.run(::std::format!(
                "alter table {} add column {}",
                M::table_name(),
                column.definition_sql(),
            ))?
            .done()?;
        }

        for statement in M::create_index_if_not_exists_statements() {
            self.execute(statement, Vec::<(&'static str, DataValue)>::new())?
                .done()?;
        }

        Ok(())
    }

    /// Drops a non-primary-key model index by name.
    ///
    /// Primary-key indexes are managed by the table definition itself and
    /// cannot be dropped independently.
    pub fn drop_index<M: Model>(&self, index_name: &str) -> Result<(), DatabaseError> {
        let sql = ::std::format!("drop index {}.{}", M::table_name(), index_name);
        let statement = crate::db::prepare(&sql)?;

        self.execute(&statement, Vec::<(&'static str, DataValue)>::new())?
            .done()
    }

    /// Drops a non-primary-key model index by name if it exists.
    pub fn drop_index_if_exists<M: Model>(&self, index_name: &str) -> Result<(), DatabaseError> {
        let sql = ::std::format!("drop index if exists {}.{}", M::table_name(), index_name);
        let statement = crate::db::prepare(&sql)?;

        self.execute(&statement, Vec::<(&'static str, DataValue)>::new())?
            .done()
    }

    /// Drops the model table.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kite_sql::db::DataBaseBuilder;
    /// use kite_sql::Model;
    ///
    /// #[derive(Default, Debug, PartialEq, Model)]
    /// #[model(table = "users")]
    /// struct User {
    ///     #[model(primary_key)]
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// let database = DataBaseBuilder::path(".").build_in_memory().unwrap();
    /// database.create_table::<User>().unwrap();
    /// database.drop_table::<User>().unwrap();
    /// ```
    pub fn drop_table<M: Model>(&self) -> Result<(), DatabaseError> {
        self.execute(
            M::drop_table_statement(),
            Vec::<(&'static str, DataValue)>::new(),
        )?
        .done()
    }

    /// Drops the model table if it exists.
    ///
    /// This variant is convenient for cleanup code that should succeed even if
    /// the table was already removed.
    pub fn drop_table_if_exists<M: Model>(&self) -> Result<(), DatabaseError> {
        self.execute(
            M::drop_table_if_exists_statement(),
            Vec::<(&'static str, DataValue)>::new(),
        )?
        .done()
    }

    /// Refreshes optimizer statistics for the model table.
    ///
    /// This runs `ANALYZE TABLE` for the backing table so the optimizer can use
    /// up-to-date statistics.
    pub fn analyze<M: Model>(&self) -> Result<(), DatabaseError> {
        self.execute(
            M::analyze_statement(),
            Vec::<(&'static str, DataValue)>::new(),
        )?
        .done()
    }

    /// Inserts a model into its backing table.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kite_sql::db::DataBaseBuilder;
    /// use kite_sql::Model;
    ///
    /// #[derive(Default, Debug, PartialEq, Model)]
    /// #[model(table = "users")]
    /// struct User {
    ///     #[model(primary_key)]
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// let database = DataBaseBuilder::path(".").build_in_memory().unwrap();
    /// database.create_table::<User>().unwrap();
    /// database.insert(&User { id: 1, name: "Alice".to_string() }).unwrap();
    /// ```
    pub fn insert<M: Model>(&self, model: &M) -> Result<(), DatabaseError> {
        self.execute(M::insert_statement(), model.params())?.done()
    }

    /// Updates a model in its backing table using the primary key.
    pub fn update<M: Model>(&self, model: &M) -> Result<(), DatabaseError> {
        self.execute(M::update_statement(), model.params())?.done()
    }

    /// Deletes a model from its backing table by primary key.
    ///
    /// The primary-key type is inferred from `M`, so callers do not need a
    /// separate generic argument for the key type.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kite_sql::db::DataBaseBuilder;
    /// use kite_sql::Model;
    ///
    /// #[derive(Default, Debug, PartialEq, Model)]
    /// #[model(table = "users")]
    /// struct User {
    ///     #[model(primary_key)]
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// let database = DataBaseBuilder::path(".").build_in_memory().unwrap();
    /// database.run("create table users (id int primary key, name varchar)").unwrap().done().unwrap();
    /// database.insert(&User { id: 1, name: "Alice".to_string() }).unwrap();
    /// database.delete_by_id::<User>(&1).unwrap();
    /// assert!(database.get::<User>(&1).unwrap().is_none());
    /// ```
    pub fn delete_by_id<M: Model>(&self, key: &M::PrimaryKey) -> Result<(), DatabaseError> {
        let params = &[(M::primary_key_field().placeholder, key.to_data_value())];
        self.execute(M::delete_statement(), params)?.done()
    }

    /// Loads a single model by primary key.
    ///
    /// The key type is taken from `M::PrimaryKey`, so `database.get::<User>(&1)`
    /// works without an extra generic parameter.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kite_sql::db::DataBaseBuilder;
    /// use kite_sql::Model;
    ///
    /// #[derive(Default, Debug, PartialEq, Model)]
    /// #[model(table = "users")]
    /// struct User {
    ///     #[model(primary_key)]
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// let database = DataBaseBuilder::path(".").build_in_memory().unwrap();
    /// database.run("create table users (id int primary key, name varchar)").unwrap().done().unwrap();
    /// database.insert(&User { id: 1, name: "Alice".to_string() }).unwrap();
    /// let user = database.get::<User>(&1).unwrap().unwrap();
    /// assert_eq!(user.name, "Alice");
    /// ```
    pub fn get<M: Model>(&self, key: &M::PrimaryKey) -> Result<Option<M>, DatabaseError> {
        let params = &[(M::primary_key_field().placeholder, key.to_data_value())];
        extract_optional_model(self.execute(M::find_statement(), params)?)
    }

    /// Lists all rows from the model table as a typed iterator.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kite_sql::db::DataBaseBuilder;
    /// use kite_sql::Model;
    ///
    /// #[derive(Default, Debug, PartialEq, Model)]
    /// #[model(table = "users")]
    /// struct User {
    ///     #[model(primary_key)]
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// let database = DataBaseBuilder::path(".").build_in_memory().unwrap();
    /// database.run("create table users (id int primary key, name varchar)").unwrap().done().unwrap();
    /// database.insert(&User { id: 1, name: "Alice".to_string() }).unwrap();
    ///
    /// let users = database.list::<User>().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    /// assert_eq!(users.len(), 1);
    /// assert_eq!(users[0].name, "Alice");
    /// ```
    pub fn list<M: Model>(&self) -> Result<OrmIter<DatabaseIter<'_, S>, M>, DatabaseError> {
        Ok(self
            .execute(
                M::select_statement(),
                Vec::<(&'static str, DataValue)>::new(),
            )?
            .orm::<M>())
    }
}

impl<'a, S: Storage> DBTransaction<'a, S> {
    /// Refreshes optimizer statistics for the model table inside the current transaction.
    pub fn analyze<M: Model>(&mut self) -> Result<(), DatabaseError> {
        self.execute(
            M::analyze_statement(),
            Vec::<(&'static str, DataValue)>::new(),
        )?
        .done()
    }

    /// Inserts a model inside the current transaction.
    pub fn insert<M: Model>(&mut self, model: &M) -> Result<(), DatabaseError> {
        self.execute(M::insert_statement(), model.params())?.done()
    }

    /// Updates a model inside the current transaction.
    pub fn update<M: Model>(&mut self, model: &M) -> Result<(), DatabaseError> {
        self.execute(M::update_statement(), model.params())?.done()
    }

    /// Deletes a model by primary key inside the current transaction.
    pub fn delete_by_id<M: Model>(&mut self, key: &M::PrimaryKey) -> Result<(), DatabaseError> {
        let params = &[(M::primary_key_field().placeholder, key.to_data_value())];
        self.execute(M::delete_statement(), params)?.done()
    }

    /// Loads a single model by primary key inside the current transaction.
    pub fn get<M: Model>(&mut self, key: &M::PrimaryKey) -> Result<Option<M>, DatabaseError> {
        let params = &[(M::primary_key_field().placeholder, key.to_data_value())];
        extract_optional_model(self.execute(M::find_statement(), params)?)
    }

    /// Lists all rows for a model inside the current transaction.
    pub fn list<M: Model>(&mut self) -> Result<OrmIter<TransactionIter<'_>, M>, DatabaseError> {
        Ok(self
            .execute(
                M::select_statement(),
                Vec::<(&'static str, DataValue)>::new(),
            )?
            .orm::<M>())
    }
}
