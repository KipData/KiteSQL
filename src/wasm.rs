#![cfg(target_arch = "wasm32")]

use crate::db::{DataBaseBuilder, Database, DatabaseIter, ResultIter};
use crate::storage::memory::MemoryStorage;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use serde::Serialize;
use wasm_bindgen::prelude::*;

#[derive(Serialize)]
struct WasmRow {
    pk: Option<DataValue>,
    values: Vec<DataValue>,
}

fn to_js_err(err: impl ToString) -> JsValue {
    js_sys::Error::new(&err.to_string()).into()
}

fn tuple_to_wasm_row(tuple: Tuple) -> WasmRow {
    WasmRow {
        pk: tuple.pk,
        values: tuple.values,
    }
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
        let iter = self.inner.run(sql).map_err(to_js_err)?;
        for tuple in iter {
            tuple.map_err(to_js_err)?;
        }
        Ok(())
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
        match iter.next() {
            Some(Ok(tuple)) => serde_wasm_bindgen::to_value(&tuple_to_wasm_row(tuple))
                .map_err(|e| to_js_err(format!("serialize row: {e}"))),
            Some(Err(err)) => Err(to_js_err(err.to_string())),
            None => Ok(JsValue::undefined()),
        }
    }

    /// Collect all remaining rows into an array and finish the iterator.
    #[wasm_bindgen(js_name = rows)]
    pub fn rows(&mut self) -> Result<JsValue, JsValue> {
        let mut iter = self
            .inner
            .take()
            .ok_or_else(|| to_js_err("iterator already consumed"))?;
        let mut rows = Vec::new();
        for tuple in &mut iter {
            let tuple = tuple.map_err(to_js_err)?;
            rows.push(tuple_to_wasm_row(tuple));
        }
        iter.done().map_err(to_js_err)?;
        serde_wasm_bindgen::to_value(&rows).map_err(|e| to_js_err(format!("serialize rows: {e}")))
    }

    /// Finish iteration early and commit any work.
    pub fn finish(&mut self) -> Result<(), JsValue> {
        if let Some(iter) = self.inner.take() {
            iter.done().map_err(to_js_err)?;
        }
        Ok(())
    }
}
