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

#![cfg(all(not(target_arch = "wasm32"), feature = "python"))]

use crate::db::{DataBaseBuilder, Database, DatabaseIter};
use crate::errors::DatabaseError;
#[cfg(feature = "lmdb")]
use crate::storage::lmdb::LmdbStorage;
use crate::storage::memory::MemoryStorage;
#[cfg(feature = "rocksdb")]
use crate::storage::rocksdb::RocksStorage;
use crate::types::tuple::{SchemaRef, Tuple};
use crate::types::value::DataValue;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule};

fn to_py_err(err: impl ToString) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

#[allow(deprecated)]
fn data_value_to_py(py: Python<'_>, value: &DataValue) -> PyResult<PyObject> {
    let object = match value {
        DataValue::Null => py.None(),
        DataValue::Boolean(value) => value.into_py(py),
        DataValue::Float32(value) => value.0.into_py(py),
        DataValue::Float64(value) => value.0.into_py(py),
        DataValue::Int8(value) => value.into_py(py),
        DataValue::Int16(value) => value.into_py(py),
        DataValue::Int32(value) => value.into_py(py),
        DataValue::Int64(value) => value.into_py(py),
        DataValue::UInt8(value) => value.into_py(py),
        DataValue::UInt16(value) => value.into_py(py),
        DataValue::UInt32(value) => value.into_py(py),
        DataValue::UInt64(value) => value.into_py(py),
        DataValue::Utf8 { value, .. } => value.clone().into_py(py),
        DataValue::Date32(_)
        | DataValue::Date64(_)
        | DataValue::Time32(_, _)
        | DataValue::Time64(_, _, _)
        | DataValue::Decimal(_) => value.to_string().into_py(py),
        DataValue::Tuple(values, _is_upper) => {
            let py_values = values
                .iter()
                .map(|value| data_value_to_py(py, value))
                .collect::<PyResult<Vec<_>>>()?;
            PyList::new(py, py_values)?.into_any().unbind()
        }
    };

    Ok(object)
}

fn tuple_to_python_row(py: Python<'_>, tuple: &Tuple) -> PyResult<PyObject> {
    let row = PyDict::new(py);

    match tuple.pk.as_ref() {
        Some(pk) => row.set_item("pk", data_value_to_py(py, pk)?)?,
        None => row.set_item("pk", py.None())?,
    }

    let values = tuple
        .values
        .iter()
        .map(|value| data_value_to_py(py, value))
        .collect::<PyResult<Vec<_>>>()?;
    row.set_item("values", PyList::new(py, values)?)?;

    Ok(row.into_any().unbind())
}

fn schema_to_python(py: Python<'_>, schema: &SchemaRef) -> PyResult<Vec<PyObject>> {
    schema
        .iter()
        .map(|col| {
            let column = PyDict::new(py);
            column.set_item("name", col.name())?;
            column.set_item("datatype", col.datatype().to_string())?;
            column.set_item("nullable", col.nullable())?;
            Ok(column.into_any().unbind())
        })
        .collect()
}

enum PythonDatabaseInner {
    #[cfg(feature = "lmdb")]
    Lmdb(Database<LmdbStorage>),
    Memory(Database<MemoryStorage>),
    #[cfg(feature = "rocksdb")]
    Rocks(Database<RocksStorage>),
}

impl PythonDatabaseInner {
    fn run(&self, sql: &str) -> Result<PythonResultIterInner, DatabaseError> {
        match self {
            #[cfg(feature = "lmdb")]
            PythonDatabaseInner::Lmdb(db) => {
                let iter = db.run(sql)?;
                // DatabaseIter owns state internally; only the type carries the lifetime.
                let iter_static: DatabaseIter<'static, LmdbStorage> =
                    unsafe { std::mem::transmute(iter) };
                Ok(PythonResultIterInner::Lmdb(iter_static))
            }
            PythonDatabaseInner::Memory(db) => {
                let iter = db.run(sql)?;
                // DatabaseIter owns state internally; only the type carries the lifetime.
                let iter_static: DatabaseIter<'static, MemoryStorage> =
                    unsafe { std::mem::transmute(iter) };
                Ok(PythonResultIterInner::Memory(iter_static))
            }
            #[cfg(feature = "rocksdb")]
            PythonDatabaseInner::Rocks(db) => {
                let iter = db.run(sql)?;
                // DatabaseIter owns state internally; only the type carries the lifetime.
                let iter_static: DatabaseIter<'static, RocksStorage> =
                    unsafe { std::mem::transmute(iter) };
                Ok(PythonResultIterInner::Rocks(iter_static))
            }
        }
    }
}

enum PythonResultIterInner {
    #[cfg(feature = "lmdb")]
    Lmdb(DatabaseIter<'static, LmdbStorage>),
    Memory(DatabaseIter<'static, MemoryStorage>),
    #[cfg(feature = "rocksdb")]
    Rocks(DatabaseIter<'static, RocksStorage>),
}

impl PythonResultIterInner {
    fn next_tuple(&mut self) -> Result<Option<&Tuple>, DatabaseError> {
        match self {
            #[cfg(feature = "lmdb")]
            PythonResultIterInner::Lmdb(iter) => iter.next_borrowed_tuple(),
            PythonResultIterInner::Memory(iter) => iter.next_borrowed_tuple(),
            #[cfg(feature = "rocksdb")]
            PythonResultIterInner::Rocks(iter) => iter.next_borrowed_tuple(),
        }
    }

    fn schema(&self) -> &SchemaRef {
        match self {
            #[cfg(feature = "lmdb")]
            PythonResultIterInner::Lmdb(iter) => iter.schema(),
            PythonResultIterInner::Memory(iter) => iter.schema(),
            #[cfg(feature = "rocksdb")]
            PythonResultIterInner::Rocks(iter) => iter.schema(),
        }
    }

    fn done(self) -> Result<(), DatabaseError> {
        match self {
            #[cfg(feature = "lmdb")]
            PythonResultIterInner::Lmdb(iter) => iter.done(),
            PythonResultIterInner::Memory(iter) => iter.done(),
            #[cfg(feature = "rocksdb")]
            PythonResultIterInner::Rocks(iter) => iter.done(),
        }
    }
}

#[pyclass(name = "Database", unsendable)]
pub struct PythonDatabase {
    inner: PythonDatabaseInner,
}

#[pymethods]
impl PythonDatabase {
    #[new]
    #[pyo3(signature = (path, backend=None))]
    pub fn new(path: String, backend: Option<&str>) -> PyResult<Self> {
        let backend = backend.unwrap_or("rocksdb").to_ascii_lowercase();
        let inner = match backend.as_str() {
            #[cfg(feature = "rocksdb")]
            "rocksdb" => PythonDatabaseInner::Rocks(
                DataBaseBuilder::path(path)
                    .build_rocksdb()
                    .map_err(to_py_err)?,
            ),
            #[cfg(feature = "lmdb")]
            "lmdb" => PythonDatabaseInner::Lmdb(
                DataBaseBuilder::path(path)
                    .build_lmdb()
                    .map_err(to_py_err)?,
            ),
            other => {
                let mut expected = Vec::new();
                #[cfg(feature = "rocksdb")]
                expected.push("rocksdb");
                #[cfg(feature = "lmdb")]
                expected.push("lmdb");
                return Err(PyValueError::new_err(format!(
                    "unsupported backend '{other}', expected {}",
                    expected.join(" or ")
                )));
            }
        };

        Ok(PythonDatabase { inner })
    }

    #[staticmethod]
    pub fn in_memory() -> PyResult<Self> {
        let inner = PythonDatabaseInner::Memory(
            DataBaseBuilder::path(".")
                .build_in_memory()
                .map_err(to_py_err)?,
        );
        Ok(PythonDatabase { inner })
    }

    pub fn run(&self, sql: &str) -> PyResult<PythonResultIter> {
        let iter = self.inner.run(sql).map_err(to_py_err)?;
        Ok(PythonResultIter { inner: Some(iter) })
    }

    pub fn execute(&self, sql: &str) -> PyResult<()> {
        let mut iter = self.inner.run(sql).map_err(to_py_err)?;
        while iter.next_tuple().map_err(to_py_err)?.is_some() {}
        iter.done().map_err(to_py_err)?;

        Ok(())
    }
}

#[pyclass(name = "ResultIter", unsendable)]
pub struct PythonResultIter {
    inner: Option<PythonResultIterInner>,
}

impl PythonResultIter {
    fn inner_ref(&self) -> PyResult<&PythonResultIterInner> {
        self.inner
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("iterator already consumed"))
    }

    fn inner_mut(&mut self) -> PyResult<&mut PythonResultIterInner> {
        self.inner
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("iterator already consumed"))
    }
}

#[pymethods]
impl PythonResultIter {
    pub fn next(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let iter = self.inner_mut()?;

        match iter.next_tuple().map_err(to_py_err)? {
            Some(tuple) => tuple_to_python_row(py, tuple).map(Some),
            None => Ok(None),
        }
    }

    pub fn schema(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        let iter = self.inner_ref()?;
        schema_to_python(py, iter.schema())
    }

    pub fn rows(&mut self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        let mut iter = self
            .inner
            .take()
            .ok_or_else(|| PyValueError::new_err("iterator already consumed"))?;

        let mut rows = Vec::new();
        while let Some(tuple) = iter.next_tuple().map_err(to_py_err)? {
            rows.push(tuple_to_python_row(py, tuple)?);
        }
        iter.done().map_err(to_py_err)?;

        Ok(rows)
    }

    pub fn finish(&mut self) -> PyResult<()> {
        if let Some(iter) = self.inner.take() {
            iter.done().map_err(to_py_err)?;
        }
        Ok(())
    }

    fn __iter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        self.next(py)
    }
}

#[pymodule]
fn kite_sql(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PythonDatabase>()?;
    m.add_class::<PythonResultIter>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::kite_sql;
    use pyo3::exceptions::PyRuntimeError;
    use pyo3::ffi::c_str;
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyModule};
    use std::ffi::CStr;
    use tempfile::TempDir;

    fn register_module<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyModule>> {
        let module = PyModule::new(py, "kite_sql")?;
        kite_sql(py, &module)?;
        Ok(module)
    }

    fn run_script(
        py: Python<'_>,
        module: &Bound<'_, PyModule>,
        script: &'static CStr,
        backend: &str,
        db_path: &str,
    ) -> PyResult<()> {
        let locals = PyDict::new(py);
        locals.set_item("kite_sql", module)?;
        locals.set_item("backend", backend)?;
        locals.set_item("db_path", db_path)?;
        py.run(script, None, Some(&locals))
    }

    fn run_script_on_all_backends(
        py: Python<'_>,
        module: &Bound<'_, PyModule>,
        script: &'static CStr,
    ) -> PyResult<()> {
        run_script(py, module, script, "memory", "")?;

        #[cfg(feature = "rocksdb")]
        {
            let temp_dir = TempDir::new()
                .map_err(|e| PyRuntimeError::new_err(format!("create tempdir: {e}")))?;
            let path = temp_dir.path().to_string_lossy().to_string();

            run_script(py, module, script, "rocksdb", &path)?;
        }

        #[cfg(feature = "lmdb")]
        {
            let temp_dir = TempDir::new()
                .map_err(|e| PyRuntimeError::new_err(format!("create tempdir: {e}")))?;
            let path = temp_dir.path().to_string_lossy().to_string();
            run_script(py, module, script, "lmdb", &path)?;
        }

        Ok(())
    }

    #[test]
    fn test_python_hello_world_api() -> PyResult<()> {
        Python::with_gil(|py| {
            let module = register_module(py)?;
            run_script_on_all_backends(
                py,
                &module,
                c_str!(
                    r#"
db = kite_sql.Database.in_memory() if backend == "memory" else kite_sql.Database(db_path, backend)
db.execute("drop table if exists my_struct")
db.execute("create table my_struct (c1 int primary key, c2 int)")
db.execute("insert into my_struct values(0, 0), (1, 1)")

iter_obj = db.run("select * from my_struct")
schema = iter_obj.schema()
assert schema == [
    {"name": "c1", "datatype": "Integer", "nullable": False},
    {"name": "c2", "datatype": "Integer", "nullable": True},
]
rows = iter_obj.rows()
assert len(rows) == 2
assert rows[0]["values"] == [0, 0]
assert rows[1]["values"] == [1, 1]

db.execute("update my_struct set c2 = c2 + 10 where c1 = 1")
after = db.run("select c2 from my_struct where c1 = 1").rows()
assert after[0]["values"] == [11]

stream = db.run("select * from my_struct")
streamed = []
row = stream.next()
while row is not None:
    streamed.append(row["values"])
    row = stream.next()
stream.finish()
assert streamed == [[0, 0], [1, 11]]

db.execute("drop table my_struct")
"#
                ),
            )?;
            Ok(())
        })
    }

    #[test]
    fn test_python_index_usage_api() -> PyResult<()> {
        Python::with_gil(|py| {
            let module = register_module(py)?;
            run_script_on_all_backends(
                py,
                &module,
                c_str!(
                    r#"
db = kite_sql.Database.in_memory() if backend == "memory" else kite_sql.Database(db_path, backend)
db.execute("drop table if exists t1")
db.execute("create table t1(id int primary key, c1 int, c2 int)")

for i in range(2000):
    id_v = i * 3
    c1_v = id_v + 1
    c2_v = id_v + 2
    db.execute(f"insert into t1 values({id_v}, {c1_v}, {c2_v})")

db.execute("create unique index u_c1_index on t1 (c1)")
db.execute("create index c2_index on t1 (c2)")
db.execute("create index p_index on t1 (c1, c2)")
db.execute("analyze table t1")

def row_vals(row):
    ints = row["values"]
    pk = row["pk"] if row["pk"] is not None else ints[0]
    return [pk] + ints[1:]

first10 = db.run("select * from t1 limit 10").rows()
assert len(first10) == 10

pk_row = [row_vals(r) for r in db.run("select * from t1 where id = 0").rows()]
assert pk_row == [[0, 1, 2]]

range_pk = [row_vals(r) for r in db.run("select * from t1 where id >= 9 and id <= 15").rows()]
assert range_pk == [
    [9, 10, 11],
    [12, 13, 14],
    [15, 16, 17],
]

c1_eq = [row_vals(r) for r in db.run("select * from t1 where c1 = 7 and c2 = 8").rows()]
assert c1_eq == [[6, 7, 8]]

c2_range = [row_vals(r) for r in db.run("select * from t1 where c2 > 100 and c2 < 110").rows()]
assert c2_range == [
    [99, 100, 101],
    [102, 103, 104],
    [105, 106, 107],
]

db.execute("update t1 set c2 = 123456 where c1 = 7")
after_update = [row_vals(r) for r in db.run("select * from t1 where c2 = 123456").rows()]
assert after_update == [[6, 7, 123456]]

db.execute("delete from t1 where c1 = 7")
after_delete = db.run("select * from t1 where c2 = 123456").rows()
assert len(after_delete) == 0

db.execute("drop table t1")
"#
                ),
            )?;
            Ok(())
        })
    }

    #[test]
    fn test_python_rejects_unknown_backend() -> PyResult<()> {
        Python::with_gil(|py| {
            let module = register_module(py)?;
            let locals = PyDict::new(py);
            locals.set_item("kite_sql", module)?;
            py.run(
                c_str!(
                    r#"
try:
    kite_sql.Database("/tmp/kitesql-python-invalid", "unknown")
    raise AssertionError("expected constructor to reject unknown backend")
except ValueError as exc:
    assert "unsupported backend" in str(exc)
"#
                ),
                None,
                Some(&locals),
            )
        })
    }
}
