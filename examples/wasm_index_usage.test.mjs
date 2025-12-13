// Verify index usage in wasm build: create table, build indexes, analyze, and query with filters.
import assert from "node:assert/strict";
import { createRequire } from "module";
import fs from "node:fs";

// Minimal localStorage polyfill for wasm code paths (statistics persistence).
class LocalStorage {
  constructor() {
    this.map = new Map();
  }
  get length() {
    return this.map.size;
  }
  key(i) {
    return Array.from(this.map.keys())[i] ?? null;
  }
  getItem(k) {
    return this.map.has(k) ? this.map.get(k) : null;
  }
  setItem(k, v) {
    this.map.set(String(k), String(v));
  }
  removeItem(k) {
    this.map.delete(k);
  }
  clear() {
    this.map.clear();
  }
}
const windowShim = { localStorage: new LocalStorage() };
globalThis.window = windowShim;
globalThis.localStorage = windowShim.localStorage;
globalThis.self = windowShim;
global.window = windowShim;

const require = createRequire(import.meta.url);
const { WasmDatabase } = require("../pkg/kite_sql.js");

// Load the shared test dataset for richer coverage.
function loadTestData() {
  const csvPath = new URL("../tests/data/row_20000.csv", import.meta.url);
  return fs.readFileSync(csvPath, "utf8").trim();
}

async function main() {
  const db = new WasmDatabase();

  await db.execute("drop table if exists t1");
  await db.execute("create table t1(id int primary key, c1 int, c2 int)");

  // Insert data in bulk
  const data = loadTestData();
  for (const line of data.split("\n")) {
    const [id, c1, c2] = line.split("|").map((v) => parseInt(v, 10));
    await db.execute(`insert into t1 values(${id}, ${c1}, ${c2})`);
  }

  // Add indexes and analyze
  await db.execute("create unique index u_c1_index on t1 (c1)");
  await db.execute("create index c2_index on t1 (c2)");
  await db.execute("create index p_index on t1 (c1, c2)");
  await db.execute("analyze table t1");

  const rowVals = (row) => {
    const ints = row.values.map((v) => v.Int32 ?? v);
    const id = row.pk?.Int32 ?? ints[0];
    const rest = ints.slice(1);
    return [id, ...rest];
  };

  // Basic queries to confirm rows
  const first10 = db.run("select * from t1 limit 10").rows();
  assert.equal(first10.length, 10);

  // Point lookups on primary key
  const pkRow = db.run("select * from t1 where id = 0").rows().map(rowVals);
  assert.deepEqual(pkRow, [[0, 1, 2]]);

  // Range on primary key
  const rangePk = db.run("select * from t1 where id >= 9 and id <= 15").rows().map(rowVals);
  assert.deepEqual(rangePk, [
    [9, 10, 11],
    [12, 13, 14],
    [15, 16, 17],
  ]);

  // Query hitting c1 index
  const c1Eq = db.run("select * from t1 where c1 = 7 and c2 = 8").rows().map(rowVals);
  assert.deepEqual(c1Eq, [[6, 7, 8]]);

  // Range query on c2 index
  const c2Range = db.run("select * from t1 where c2 > 100 and c2 < 110").rows().map(rowVals);
  assert.deepEqual(c2Range, [
    [99, 100, 101],
    [102, 103, 104],
    [105, 106, 107],
  ]);

  // Update and re-query to ensure index maintenance
  await db.execute("update t1 set c2 = 123456 where c1 = 7");
  const afterUpdate = db.run("select * from t1 where c2 = 123456").rows().map(rowVals);
  assert.deepEqual(afterUpdate, [[6, 7, 123456]]);

  // Delete and ensure index reflects removal
  await db.execute("delete from t1 where c1 = 7");
  const afterDelete = db.run("select * from t1 where c2 = 123456").rows().map(rowVals);
  assert.equal(afterDelete.length, 0);

  await db.execute("drop table t1");
  console.log("wasm index usage test passed");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
