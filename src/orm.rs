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
pub struct OrmField {
    pub column: &'static str,
    pub placeholder: &'static str,
    pub primary_key: bool,
}

pub trait Model: Sized + for<'a> From<(&'a SchemaRef, Tuple)> {
    fn table_name() -> &'static str;

    fn fields() -> &'static [OrmField];

    fn params(&self) -> Vec<(&'static str, DataValue)>;

    fn primary_key_value(&self) -> DataValue;

    fn select_statement() -> &'static Statement;

    fn insert_statement() -> &'static Statement;

    fn update_statement() -> &'static Statement;

    fn delete_statement() -> &'static Statement;

    fn find_statement() -> &'static Statement;

    fn primary_key_field() -> &'static OrmField {
        Self::fields()
            .iter()
            .find(|field| field.primary_key)
            .expect("ORM model must define exactly one primary key field")
    }
}

pub trait FromDataValue: Sized {
    fn logical_type() -> Option<LogicalType>;

    fn from_data_value(value: DataValue) -> Option<Self>;
}

pub trait ToDataValue {
    fn to_data_value(&self) -> DataValue;
}

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
    pub fn insert<M: Model>(&self, model: &M) -> Result<(), DatabaseError> {
        self.execute(M::insert_statement(), model.params())?.done()
    }

    pub fn update<M: Model>(&self, model: &M) -> Result<(), DatabaseError> {
        self.execute(M::update_statement(), model.params())?.done()
    }

    pub fn delete<M: Model>(&self, model: &M) -> Result<(), DatabaseError> {
        let params = vec![(
            M::primary_key_field().placeholder,
            model.primary_key_value(),
        )];
        self.execute(M::delete_statement(), params)?.done()
    }

    pub fn delete_by_id<M: Model, K: ToDataValue>(&self, key: &K) -> Result<(), DatabaseError> {
        let params = vec![(M::primary_key_field().placeholder, key.to_data_value())];
        self.execute(M::delete_statement(), params)?.done()
    }

    pub fn get<M: Model, K: ToDataValue>(&self, key: &K) -> Result<Option<M>, DatabaseError> {
        let params = vec![(M::primary_key_field().placeholder, key.to_data_value())];
        extract_optional_model(self.execute(M::find_statement(), params)?)
    }

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
    pub fn insert<M: Model>(&mut self, model: &M) -> Result<(), DatabaseError> {
        self.execute(M::insert_statement(), model.params())?.done()
    }

    pub fn update<M: Model>(&mut self, model: &M) -> Result<(), DatabaseError> {
        self.execute(M::update_statement(), model.params())?.done()
    }

    pub fn delete<M: Model>(&mut self, model: &M) -> Result<(), DatabaseError> {
        let params = vec![(
            M::primary_key_field().placeholder,
            model.primary_key_value(),
        )];
        self.execute(M::delete_statement(), params)?.done()
    }

    pub fn delete_by_id<M: Model, K: ToDataValue>(&mut self, key: &K) -> Result<(), DatabaseError> {
        let params = vec![(M::primary_key_field().placeholder, key.to_data_value())];
        self.execute(M::delete_statement(), params)?.done()
    }

    pub fn get<M: Model, K: ToDataValue>(&mut self, key: &K) -> Result<Option<M>, DatabaseError> {
        let params = vec![(M::primary_key_field().placeholder, key.to_data_value())];
        extract_optional_model(self.execute(M::find_statement(), params)?)
    }

    pub fn list<M: Model>(&mut self) -> Result<OrmIter<TransactionIter<'_>, M>, DatabaseError> {
        Ok(self
            .execute(
                M::select_statement(),
                Vec::<(&'static str, DataValue)>::new(),
            )?
            .orm::<M>())
    }
}
