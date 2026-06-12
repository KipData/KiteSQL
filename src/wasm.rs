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

#![cfg(target_arch = "wasm32")]

use crate::db::{DataBaseBuilder, Database, DatabaseIter};
use crate::storage::memory::MemoryStorage;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::CharLengthUnits;
use js_sys::{Array, Object, Reflect};
use wasm_bindgen::prelude::*;

fn to_js_err(err: impl ToString) -> JsValue {
    js_sys::Error::new(&err.to_string()).into()
}

fn set_prop(object: &Object, key: &str, value: JsValue) -> Result<(), JsValue> {
    Reflect::set(object, &JsValue::from_str(key), &value)?;
    Ok(())
}

fn data_value_to_js(value: &DataValue) -> Result<JsValue, JsValue> {
    match value {
        DataValue::Null => Ok(JsValue::NULL),
        DataValue::Boolean(value) => Ok(JsValue::from_bool(*value)),
        DataValue::Float32(value) => Ok(JsValue::from_f64(value.0 as f64)),
        DataValue::Float64(value) => Ok(JsValue::from_f64(value.0)),
        DataValue::Int8(value) => Ok(JsValue::from_f64(*value as f64)),
        DataValue::Int16(value) => Ok(JsValue::from_f64(*value as f64)),
        DataValue::Int32(value) => Ok(JsValue::from_f64(*value as f64)),
        DataValue::Int64(value) => Ok(JsValue::from_f64(*value as f64)),
        DataValue::UInt8(value) => Ok(JsValue::from_f64(*value as f64)),
        DataValue::UInt16(value) => Ok(JsValue::from_f64(*value as f64)),
        DataValue::UInt32(value) => Ok(JsValue::from_f64(*value as f64)),
        DataValue::UInt64(value) => Ok(JsValue::from_f64(*value as f64)),
        DataValue::Utf8 { value, ty, unit } => {
            let object = Object::new();
            set_prop(&object, "value", JsValue::from_str(value))?;
            set_prop(&object, "type", utf8_type_to_js(ty)?)?;
            set_prop(&object, "unit", char_length_units_to_js(*unit))?;
            Ok(object.into())
        }
        DataValue::Date32(value) => Ok(JsValue::from_f64(*value as f64)),
        DataValue::Date64(value) => Ok(JsValue::from_f64(*value as f64)),
        DataValue::Time32(value, precision) => {
            let object = Object::new();
            set_prop(&object, "value", JsValue::from_f64(*value as f64))?;
            set_prop(&object, "precision", JsValue::from_f64(*precision as f64))?;
            Ok(object.into())
        }
        DataValue::Time64(value, precision, with_tz) => {
            let object = Object::new();
            set_prop(&object, "value", JsValue::from_f64(*value as f64))?;
            set_prop(&object, "precision", JsValue::from_f64(*precision as f64))?;
            set_prop(&object, "withTimezone", JsValue::from_bool(*with_tz))?;
            Ok(object.into())
        }
        #[cfg(feature = "decimal")]
        DataValue::Decimal(value) => Ok(JsValue::from_str(&value.to_string())),
        DataValue::Tuple(values, is_upper) => {
            let object = Object::new();
            set_prop(&object, "values", data_values_to_js(values)?)?;
            set_prop(&object, "isUpper", JsValue::from_bool(*is_upper))?;
            Ok(object.into())
        }
    }
}

fn utf8_type_to_js(ty: &Utf8Type) -> Result<JsValue, JsValue> {
    let object = Object::new();
    match ty {
        Utf8Type::Variable(len) => {
            set_prop(&object, "kind", JsValue::from_str("variable"))?;
            set_prop(
                &object,
                "len",
                len.map(|len| JsValue::from_f64(len as f64))
                    .unwrap_or(JsValue::NULL),
            )?;
        }
        Utf8Type::Fixed(len) => {
            set_prop(&object, "kind", JsValue::from_str("fixed"))?;
            set_prop(&object, "len", JsValue::from_f64(*len as f64))?;
        }
    }
    Ok(object.into())
}

fn char_length_units_to_js(unit: CharLengthUnits) -> JsValue {
    JsValue::from_str(match unit {
        CharLengthUnits::Characters => "characters",
        CharLengthUnits::Octets => "octets",
    })
}

fn data_values_to_js(values: &[DataValue]) -> Result<JsValue, JsValue> {
    let array = Array::new();
    for value in values {
        array.push(&data_value_to_js(value)?);
    }
    Ok(array.into())
}

fn tuple_to_wasm_row(tuple: &Tuple) -> Result<JsValue, JsValue> {
    let object = Object::new();
    set_prop(
        &object,
        "pk",
        tuple
            .pk
            .as_ref()
            .map(data_value_to_js)
            .transpose()?
            .unwrap_or(JsValue::NULL),
    )?;
    set_prop(&object, "values", data_values_to_js(&tuple.values)?)?;
    Ok(object.into())
}

#[wasm_bindgen]
pub struct WasmDatabase {
    inner: Database<MemoryStorage>,
}

#[wasm_bindgen]
pub struct WasmResultIter {
    inner: Option<DatabaseIter<'static, MemoryStorage>>,
}

impl Drop for WasmResultIter {
    fn drop(&mut self) {
        if let Some(iter) = self.inner.take() {
            let _ = iter.done();
        }
    }
}

#[wasm_bindgen]
impl WasmDatabase {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<WasmDatabase, JsValue> {
        let db = DataBaseBuilder::path(".")
            .build()
            .map_err(|e| to_js_err(format!("init database failed: {e}")))?;
        Ok(WasmDatabase { inner: db })
    }

    /// Stream results with a JS-friendly iterator that exposes `next()`.
    pub fn run(&self, sql: &str) -> Result<WasmResultIter, JsValue> {
        let iter = self.inner.run(sql).map_err(to_js_err)?;
        // DatabaseIter owns its internal state; lifetime is only in the type.
        let iter_static: DatabaseIter<'static, MemoryStorage> =
            unsafe { std::mem::transmute(iter) };
        Ok(WasmResultIter {
            inner: Some(iter_static),
        })
    }

    pub fn execute(&self, sql: &str) -> Result<(), JsValue> {
        let mut iter = self.inner.run(sql).map_err(to_js_err)?;
        while iter.next_borrowed_tuple().map_err(to_js_err)?.is_some() {}
        Ok(())
    }

    pub fn ddl(&mut self, sql: &str) -> Result<(), JsValue> {
        self.inner.ddl(sql).map_err(to_js_err)
    }

    pub fn analyze(&mut self, table_name: &str) -> Result<(), JsValue> {
        self.inner.analyze(table_name).map_err(to_js_err)
    }
}

#[wasm_bindgen]
impl WasmResultIter {
    /// Returns the next row as a JS object, or `undefined` when done.
    #[wasm_bindgen(js_name = next)]
    pub fn next(&mut self) -> Result<JsValue, JsValue> {
        let iter = self
            .inner
            .as_mut()
            .ok_or_else(|| to_js_err("iterator already consumed"))?;
        match iter.next_borrowed_tuple().map_err(to_js_err)? {
            Some(tuple) => tuple_to_wasm_row(tuple),
            None => Ok(JsValue::undefined()),
        }
    }

    /// Returns the output schema as an array of `{ name, datatype, nullable }`.
    #[wasm_bindgen(js_name = schema)]
    pub fn schema(&self) -> Result<JsValue, JsValue> {
        let iter = self
            .inner
            .as_ref()
            .ok_or_else(|| to_js_err("iterator already consumed"))?;
        iter.schema(|schema| {
            let columns = Array::new();
            for col in schema.iter() {
                let object = Object::new();
                set_prop(&object, "name", JsValue::from_str(col.name()))?;
                set_prop(
                    &object,
                    "datatype",
                    JsValue::from_str(&col.datatype().to_string()),
                )?;
                set_prop(&object, "nullable", JsValue::from_bool(col.nullable()))?;
                columns.push(&object);
            }
            Ok(columns.into())
        })
    }

    /// Collect all remaining rows into an array and finish the iterator.
    #[wasm_bindgen(js_name = rows)]
    pub fn rows(&mut self) -> Result<JsValue, JsValue> {
        let mut iter = self
            .inner
            .take()
            .ok_or_else(|| to_js_err("iterator already consumed"))?;
        let rows = Array::new();
        while let Some(tuple) = iter.next_borrowed_tuple().map_err(to_js_err)? {
            rows.push(&tuple_to_wasm_row(tuple)?);
        }
        iter.done().map_err(to_js_err)?;
        Ok(rows.into())
    }

    /// Finish iteration early and commit any work.
    pub fn finish(&mut self) -> Result<(), JsValue> {
        if let Some(iter) = self.inner.take() {
            iter.done().map_err(to_js_err)?;
        }
        Ok(())
    }
}
