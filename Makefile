# Helper variables (override on invocation if needed).
CARGO ?= cargo
WASM_PACK ?= wasm-pack
SQLLOGIC_PATH ?= tests/slt/**/*.slt
PYO3_PYTHON ?= /usr/bin/python3.12
TPCC_MEASURE_TIME ?= 15
TPCC_NUM_WARE ?= 1
TPCC_PPROF_OUTPUT ?= /tmp/tpcc_lmdb.svg
TPCC_HEAPTRACK_MEASURE_TIME ?= 300
TPCC_HEAPTRACK_OUTPUT ?= /tmp/tpcc_lmdb_heaptrack
TPCC_SQLITE_PROFILE ?= balanced

.PHONY: test test-python test-wasm test-slt test-all wasm-build check tpcc tpcc-kitesql-rocksdb tpcc-kitesql-lmdb tpcc-lmdb-flamegraph tpcc-lmdb-heaptrack tpcc-sqlite tpcc-sqlite-practical tpcc-sqlite-balanced tpcc-dual cargo-check build wasm-examples native-examples fmt clippy

## Run default Rust tests in the current environment (non-WASM).
test:
	$(CARGO) test --all

## Run Python binding API tests implemented with pyo3.
test-python:
	PYO3_PYTHON=$(PYO3_PYTHON) $(CARGO) test --features python,decimal test_python_

## Perform a `cargo check` across the workspace.
cargo-check:
	$(CARGO) check

## Build the workspace artifacts (debug).
build:
	$(CARGO) build

## Build the WebAssembly package (artifact goes to ./pkg).
wasm-build:
	$(WASM_PACK) build --release --target nodejs -- --features wasm

## Execute wasm-bindgen tests under Node.js (wasm32 target).
test-wasm:
	$(WASM_PACK) test --node -- --features wasm --package kite_sql --lib

## Run the sqllogictest harness against the configured .slt suite.
test-slt:
	$(CARGO) run -p sqllogictest-test -- --path '$(SQLLOGIC_PATH)'

## Convenience target to run every suite in sequence.
test-all: test test-wasm test-slt test-python

## Run formatting (check mode) across the workspace.
fmt:
	$(CARGO) fmt --all -- --check

## Execute clippy across all targets/features with warnings elevated to errors.
clippy:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

## Run formatting (check mode) and clippy linting together.
check: fmt clippy

tpcc: tpcc-kitesql-lmdb

## Execute the TPCC workload on KiteSQL with RocksDB storage.
tpcc-kitesql-rocksdb:
	$(CARGO) run -p tpcc --release -- --backend kitesql-rocksdb

## Execute the TPCC workload on KiteSQL with LMDB storage.
tpcc-kitesql-lmdb:
	$(CARGO) run -p tpcc --release -- --backend kitesql-lmdb

## Execute TPCC on LMDB and emit a pprof flamegraph SVG.
tpcc-lmdb-flamegraph:
	CARGO_PROFILE_RELEASE_DEBUG=true $(CARGO) run -p tpcc --release --features pprof -- --backend kitesql-lmdb --measure-time $(TPCC_MEASURE_TIME) --num-ware $(TPCC_NUM_WARE) --pprof-output $(TPCC_PPROF_OUTPUT)

## Execute TPCC on LMDB under heaptrack and emit a heap profile.
tpcc-lmdb-heaptrack:
	@command -v heaptrack >/dev/null || { echo "heaptrack is not installed"; exit 1; }
	$(CARGO) build -p tpcc --release
	@mkdir -p $(dir $(TPCC_HEAPTRACK_OUTPUT))
	heaptrack -o $(TPCC_HEAPTRACK_OUTPUT) ./target/release/tpcc --backend kitesql-lmdb --measure-time $(TPCC_HEAPTRACK_MEASURE_TIME) --num-ware $(TPCC_NUM_WARE)
	@echo "heaptrack output:"
	@ls -1 $(TPCC_HEAPTRACK_OUTPUT)*
	@echo "open gui: heaptrack_gui $$(ls -1 $(TPCC_HEAPTRACK_OUTPUT)* | tail -n 1)"

## Execute the TPCC workload on SQLite with the practical profile.
tpcc-sqlite:
	$(CARGO) run -p tpcc --release -- --backend sqlite --sqlite-profile $(TPCC_SQLITE_PROFILE) --path kite_sql_tpcc.sqlite

## Execute the TPCC workload on SQLite with the practical profile.
tpcc-sqlite-practical:
	$(MAKE) tpcc-sqlite TPCC_SQLITE_PROFILE=practical

## Execute the TPCC workload on SQLite with the balanced profile.
tpcc-sqlite-balanced:
	$(MAKE) tpcc-sqlite TPCC_SQLITE_PROFILE=balanced

## Execute TPCC while mirroring every statement to an in-memory SQLite instance for validation.
tpcc-dual:
	$(CARGO) run -p tpcc --release -- --backend dual --measure-time 60

## Run JavaScript-based Wasm example scripts.
wasm-examples:
	node examples/wasm_hello_world.test.mjs
	node examples/wasm_index_usage.test.mjs

## Run the native (non-Wasm) example binaries.
native-examples:
	$(CARGO) run --example hello_world
	$(CARGO) run --example transaction
