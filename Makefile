# Helper variables (override on invocation if needed).
CARGO ?= cargo
WASM_PACK ?= wasm-pack
SQLLOGIC_PATH ?= tests/slt/**/*.slt

.PHONY: test test-wasm test-slt test-all wasm-build check tpcc

## Run default Rust tests in the current environment (non-WASM).
test:
	$(CARGO) test

## Build the WebAssembly package (artifact goes to ./pkg).
wasm-build:
	$(WASM_PACK) build --release --target nodejs

## Execute wasm-bindgen tests under Node.js (wasm32 target).
test-wasm:
	$(WASM_PACK) test --node --release

## Run the sqllogictest harness against the configured .slt suite.
test-slt:
	$(CARGO) run -p sqllogictest-test -- --path '$(SQLLOGIC_PATH)'

## Convenience target to run every suite in sequence.
test-all: test test-wasm test-slt

## Run formatting (check mode) and clippy linting together.
check:
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --all-targets --all-features -- -D warnings

## Execute the TPCC workload example as a standalone command.
tpcc:
	$(CARGO) run -p tpcc --release
