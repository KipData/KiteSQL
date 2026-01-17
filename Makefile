# Helper variables (override on invocation if needed).
CARGO ?= cargo
WASM_PACK ?= wasm-pack
SQLLOGIC_PATH ?= tests/slt/**/*.slt

.PHONY: test test-wasm test-slt test-all wasm-build check tpcc tpcc-dual cargo-check build wasm-examples native-examples fmt clippy

## Run default Rust tests in the current environment (non-WASM).
test:
	$(CARGO) test --all

## Perform a `cargo check` across the workspace.
cargo-check:
	$(CARGO) check

## Build the workspace artifacts (debug).
build:
	$(CARGO) build

## Build the WebAssembly package (artifact goes to ./pkg).
wasm-build:
	$(WASM_PACK) build --release --target nodejs

## Execute wasm-bindgen tests under Node.js (wasm32 target).
test-wasm:
	$(WASM_PACK) test --node -- --package kite_sql --lib

## Run the sqllogictest harness against the configured .slt suite.
test-slt:
	$(CARGO) run -p sqllogictest-test -- --path '$(SQLLOGIC_PATH)'

## Convenience target to run every suite in sequence.
test-all: test test-wasm test-slt

## Run formatting (check mode) across the workspace.
fmt:
	$(CARGO) fmt --all -- --check

## Execute clippy across all targets/features with warnings elevated to errors.
clippy:
	$(CARGO) clippy --all-targets --all-features -- -D warnings

## Run formatting (check mode) and clippy linting together.
check: fmt clippy

## Execute the TPCC workload example as a standalone command.
tpcc:
	$(CARGO) run -p tpcc --release

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
