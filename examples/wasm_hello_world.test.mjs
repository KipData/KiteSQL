// Simple wasm smoke test; run `wasm-pack build --target nodejs` first to generate ./pkg
import assert from "node:assert/strict";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const { WasmDatabase } = require("../pkg/kite_sql.js");

async function main() {
  const db = new WasmDatabase();

  await db.execute("drop table if exists my_struct");
  await db.execute("create table my_struct (c1 int primary key, c2 int)");
  await db.execute("insert into my_struct values(0, 0), (1, 1)");

  const rows = db.run("select * from my_struct").rows();
  assert.equal(rows.length, 2, "should return two rows");

  const [first, second] = rows;
  const firstVals = first.values.map((v) => v.Int32 ?? v);
  const secondVals = second.values.map((v) => v.Int32 ?? v);
  assert.deepEqual(firstVals, [0, 0]);
  assert.deepEqual(secondVals, [1, 1]);

  await db.execute("update my_struct set c2 = c2 + 10 where c1 = 1");
  const after = db.run("select c2 from my_struct where c1 = 1").rows();
  assert.deepEqual(after[0].values.map((v) => v.Int32 ?? v), [11]);

  // Stream the rows using the iterator interface
  const stream = db.run("select * from my_struct");
  const streamed = [];
  let row = stream.next();
  while (row !== undefined) {
    streamed.push(row.values.map((v) => v.Int32 ?? v));
    row = stream.next();
  }
  stream.finish();
  assert.deepEqual(streamed, [
    [0, 0],
    [1, 11],
  ]);

  await db.execute("drop table my_struct");
  console.log("wasm hello_world test passed");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
