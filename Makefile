# Helper variables (override on invocation if needed).
CARGO ?= cargo
GRCOV ?= grcov
WASM_PACK ?= wasm-pack
SQLLOGIC_PATH ?= tests/slt/**/*.slt
PYO3_PYTHON ?= /usr/bin/python3.12
TPCC_MEASURE_TIME ?= 15
TPCC_NUM_WARE ?= 1
TPCC_PPROF_OUTPUT ?= /tmp/tpcc_lmdb.svg
TPCC_HEAPTRACK_MEASURE_TIME ?= 300
TPCC_HEAPTRACK_OUTPUT ?= /tmp/tpcc_lmdb_heaptrack
TPCC_SQLITE_PROFILE ?= balanced
CODECOV_OUTPUT ?= lcov.info
COVERAGE_PROFILE_DIR ?= target/grcov/profraw
COVERAGE_HTML_DIR ?= target/grcov/html
COVERAGE_RUSTFLAGS ?= -Cinstrument-coverage
COVERAGE_TEST_FEATURES ?= copy,decimal,orm
# Pinned nightly for fuzzing; keep in sync with the `fuzz` job in .github/workflows/ci.yml.
FUZZ_TOOLCHAIN ?= nightly-2026-10-04
FUZZ_TARGET ?= sql_exec
FUZZ_TIME ?= 60
# Pass the host triple explicitly: prebuilt (musl) cargo-fuzz binaries otherwise default to musl.
FUZZ_TRIPLE ?= $(shell rustc -vV | sed -n 's/^host: //p')
FUZZ_TARGET_DIR ?= $(CURDIR)/fuzz/target
FUZZ_BIN = $(FUZZ_TARGET_DIR)/$(FUZZ_TRIPLE)/release/$(FUZZ_TARGET)
# Per-target libFuzzer args and the exit code that still counts as a pass.
# sql_exec: tests/slt as read-only seeds (libFuzzer only writes to the FIRST dir); fork mode
# so timeouts are skipped (TODO in fuzz/fuzz_targets/sql_exec.rs). Fork mode ignores OOMs by
# default, hence -ignore_ooms=0, and exits with the LAST child's code, so a trailing ignored
# timeout (70) is not a failure.
FUZZ_ARGS_sql_exec = tests/slt -dict=fuzz/sql.dict -max_len=32768 \
	-fork=1 -ignore_timeouts=1 -ignore_ooms=0 -timeout_exitcode=70
FUZZ_PASS_EXIT_sql_exec = 70
# sql_gen: bounded generated queries, so crashes, timeouts and OOMs all fail.
FUZZ_ARGS_sql_gen = -max_len=4096
FUZZ_PASS_EXIT_sql_gen = 0
COVERAGE_REPORT_ARGS ?= --llvm --ignore-not-existing --keep-only 'src/**' --ignore 'src/**/tests/**' --ignore 'tests/**' --ignore 'tpcc/**' --excl-start 'GRCOV_EXCL_START' --excl-stop 'GRCOV_EXCL_STOP'

.PHONY: test test-python test-wasm test-slt test-all codecov codecov-html wasm-build check tpcc tpcc-kitesql-rocksdb tpcc-kitesql-lmdb tpcc-lmdb-flamegraph tpcc-lmdb-heaptrack tpcc-sqlite tpcc-sqlite-practical tpcc-sqlite-balanced tpcc-dual cargo-check build wasm-examples native-examples fmt clippy fuzz

## Run default Rust tests in the current environment (non-WASM).
test:
	$(CARGO) test --all --features spill

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

## Generate an lcov coverage report for Codecov upload.
codecov:
	bash -c "set -euo pipefail; \
		command -v $(GRCOV) >/dev/null || { echo 'grcov is not installed; run: cargo install grcov'; exit 1; }; \
		rm -rf '$(COVERAGE_PROFILE_DIR)' '$(CODECOV_OUTPUT)'; \
		mkdir -p '$(COVERAGE_PROFILE_DIR)'; \
		CARGO_INCREMENTAL=0 RUSTFLAGS=\"$${RUSTFLAGS:-} $(COVERAGE_RUSTFLAGS)\" LLVM_PROFILE_FILE='$(COVERAGE_PROFILE_DIR)/kitesql-%p-%m.profraw' $(CARGO) test --all --features '$(COVERAGE_TEST_FEATURES)'; \
		CARGO_INCREMENTAL=0 RUSTFLAGS=\"$${RUSTFLAGS:-} $(COVERAGE_RUSTFLAGS)\" LLVM_PROFILE_FILE='$(COVERAGE_PROFILE_DIR)/kitesql-%p-%m.profraw' $(CARGO) run -p sqllogictest-test -- --path '$(SQLLOGIC_PATH)'; \
		$(GRCOV) . --binary-path \"$${CARGO_TARGET_DIR:-target}/debug\" -s . -t lcov $(COVERAGE_REPORT_ARGS) -o '$(CODECOV_OUTPUT)'"

## Generate a local HTML coverage report.
codecov-html:
	bash -c "set -euo pipefail; \
		command -v $(GRCOV) >/dev/null || { echo 'grcov is not installed; run: cargo install grcov'; exit 1; }; \
		rm -rf '$(COVERAGE_PROFILE_DIR)' '$(COVERAGE_HTML_DIR)'; \
		mkdir -p '$(COVERAGE_PROFILE_DIR)' '$(COVERAGE_HTML_DIR)'; \
		CARGO_INCREMENTAL=0 RUSTFLAGS=\"$${RUSTFLAGS:-} $(COVERAGE_RUSTFLAGS)\" LLVM_PROFILE_FILE='$(COVERAGE_PROFILE_DIR)/kitesql-%p-%m.profraw' $(CARGO) test --all --features '$(COVERAGE_TEST_FEATURES)'; \
		CARGO_INCREMENTAL=0 RUSTFLAGS=\"$${RUSTFLAGS:-} $(COVERAGE_RUSTFLAGS)\" LLVM_PROFILE_FILE='$(COVERAGE_PROFILE_DIR)/kitesql-%p-%m.profraw' $(CARGO) run -p sqllogictest-test -- --path '$(SQLLOGIC_PATH)'; \
		$(GRCOV) . --binary-path \"$${CARGO_TARGET_DIR:-target}/debug\" -s . -t html $(COVERAGE_REPORT_ARGS) -o '$(COVERAGE_HTML_DIR)'"
	@echo "Coverage report: $(COVERAGE_HTML_DIR)/index.html"

## Run fuzz target FUZZ_TARGET (sql_exec | sql_gen) for FUZZ_TIME seconds
## (needs a nightly toolchain and cargo-fuzz). The binary is run directly because
## `cargo fuzz run` turns every non-zero exit into a failure.
fuzz:
	@mkdir -p fuzz/corpus/$(FUZZ_TARGET) fuzz/artifacts/$(FUZZ_TARGET)
	$(CARGO) +$(FUZZ_TOOLCHAIN) fuzz build --target $(FUZZ_TRIPLE) --target-dir $(FUZZ_TARGET_DIR) $(FUZZ_TARGET)
	@status=0; $(FUZZ_BIN) fuzz/corpus/$(FUZZ_TARGET) $(FUZZ_ARGS_$(FUZZ_TARGET)) \
		-artifact_prefix=fuzz/artifacts/$(FUZZ_TARGET)/ \
		-timeout=10 -rss_limit_mb=4096 -max_total_time=$(FUZZ_TIME) || status=$$?; \
	rm -rf "$${TMPDIR:-/tmp}"/kitesql-fuzz-$(FUZZ_TARGET)-*; \
	if [ $$status -ne 0 ] && [ $$status -ne $(FUZZ_PASS_EXIT_$(FUZZ_TARGET)) ]; then \
		echo "fuzz: $(FUZZ_TARGET) failed (exit $$status), see fuzz/artifacts/$(FUZZ_TARGET)/"; exit $$status; \
	fi

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
