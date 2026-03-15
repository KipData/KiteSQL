use crate::db::{
    DBTransaction, Database, DatabaseIter, OrmIter, ResultIter, Statement, TransactionIter,
};
use crate::errors::DatabaseError;
use crate::storage::Storage;
use crate::types::tuple::{SchemaRef, Tuple};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use sqlparser::ast::CharLengthUnits;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Static metadata about a single model field.
///
/// This type is primarily consumed by code generated from `#[derive(Model)]`.
pub struct OrmField {
    pub column: &'static str,
    pub placeholder: &'static str,
    pub primary_key: bool,
}

/// Trait implemented by ORM models.
///
/// In normal usage you should derive this trait with `#[derive(Model)]` rather
/// than implementing it by hand. The derive macro generates tuple mapping,
/// cached statements and CRUD metadata for the model.
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
    /// database.run("create table users (id int primary key, name varchar)").unwrap().done().unwrap();
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
