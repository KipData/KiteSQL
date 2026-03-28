#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

NUM_WARE="${TPCC_NUM_WARE:-1}"
MAX_DUPLICATE_RETRY="${TPCC_DUPLICATE_RETRY:-1}"
MAIN_MEASURE_TIME="${TPCC_MAIN_MEASURE_TIME:-}"
STAMP="${TPCC_RESULT_STAMP:-$(date +%Y-%m-%d_%H-%M-%S)}"
RESULT_DIR="${TPCC_RESULT_DIR:-$ROOT_DIR/tpcc/results/$STAMP}"
LOG_DIR="$RESULT_DIR/logs"
TMP_DIR="$ROOT_DIR/target/tpcc-run-data"
BINARY="$ROOT_DIR/target/release/tpcc"
SUMMARY_FILE="$RESULT_DIR/summary.md"

mkdir -p "$LOG_DIR" "$TMP_DIR"

if [[ ! -x "$BINARY" ]]; then
    echo "missing binary: $BINARY" >&2
    echo "build it first with: cargo build -p tpcc --release" >&2
    exit 1
fi

extract_tpmc() {
    local log_file="$1"
    awk '/<TpmC>/{getline; print $1; exit}' "$log_file"
}

extract_p90() {
    local log_file="$1"
    local label="$2"
    awk -v label="$label" '
        /<90th Percentile RT \(MaxRT\)>/ { in_block = 1; next }
        in_block && index($0, label) {
            gsub(/^[[:space:]]+/, "", $0)
            print $3
            exit
        }
    ' "$log_file"
}

should_retry_duplicate() {
    local log_file="$1"
    rg -q "UNIQUE constraint failed|duplicate key|primary key|Duplicate" "$log_file"
}

run_variant() {
    local name="$1"
    local measure_label="$2"
    local db_path="$3"
    shift 3
    local -a cmd=("$@")
    local log_file="$LOG_DIR/$name.log"
    local status="ok"
    local notes="-"
    local attempts=0
    local max_attempts=$((MAX_DUPLICATE_RETRY + 1))

    : > "$log_file"

    while (( attempts < max_attempts )); do
        attempts=$((attempts + 1))
        rm -rf "$db_path"

        {
            printf '## Attempt %s\n' "$attempts"
            printf '$'
            printf ' %q' "${cmd[@]}"
            printf '\n\n'
        } >> "$log_file"

        set +e
        "${cmd[@]}" >> "$log_file" 2>&1
        local cmd_status=$?
        set -e

        if [[ "$cmd_status" -eq 0 ]]; then
            break
        fi

        if (( attempts < max_attempts )) && should_retry_duplicate "$log_file"; then
            notes="retry after duplicate-key failure"
            printf '\n[runner] duplicate-key style failure detected, retrying %s from scratch\n\n' "$name" >> "$log_file"
            continue
        fi

        status="failed"
        notes="$(tail -n 5 "$log_file" | tr '\n' ' ' | sed 's/[[:space:]]\\+/ /g; s/^ //; s/ $//')"
        break
    done

    local tpmc="-"
    local new_order="-"
    local payment="-"
    local order_status="-"
    local delivery="-"
    local stock_level="-"

    if [[ "$status" == "ok" ]]; then
        tpmc="$(extract_tpmc "$log_file" || echo -)"
        new_order="$(extract_p90 "$log_file" "New-Order" || echo -)"
        payment="$(extract_p90 "$log_file" "Payment" || echo -)"
        order_status="$(extract_p90 "$log_file" "Order-Status" || echo -)"
        delivery="$(extract_p90 "$log_file" "Delivery" || echo -)"
        stock_level="$(extract_p90 "$log_file" "Stock-Level" || echo -)"
    fi

    printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | [%s](./logs/%s.log) |\n' \
        "$name" \
        "$status" \
        "$attempts" \
        "$measure_label" \
        "$tpmc" \
        "$new_order" \
        "$payment" \
        "$order_status" \
        "$delivery" \
        "$stock_level" \
        "$notes" \
        "$name" \
        "$name" \
        >> "$SUMMARY_FILE"

    rm -rf "$db_path"

    cat "$log_file"
}

cat > "$SUMMARY_FILE" <<EOF
# TPCC Run Summary

- Timestamp: $STAMP
- Warehouses: ${NUM_WARE}
- Duplicate-key retries per variant: ${MAX_DUPLICATE_RETRY}
- Main variants measure time: ${MAIN_MEASURE_TIME:-tpcc default (720s)}
- Binary: \`$BINARY\`

| Variant | Status | Attempts | Measure Time | TpmC | New-Order p90 | Payment p90 | Order-Status p90 | Delivery p90 | Stock-Level p90 | Notes | Raw Log |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
EOF

main_measure_args=()
main_measure_label="tpcc default (720s)"
if [[ -n "$MAIN_MEASURE_TIME" ]]; then
    main_measure_args=(--measure-time "$MAIN_MEASURE_TIME")
    main_measure_label="${MAIN_MEASURE_TIME}s"
fi

run_variant \
    "kitesql-lmdb" \
    "$main_measure_label" \
    "$TMP_DIR/kitesql-lmdb" \
    "$BINARY" --backend kitesql-lmdb "${main_measure_args[@]}" --num-ware "$NUM_WARE" --path "$TMP_DIR/kitesql-lmdb"

run_variant \
    "kitesql-rocksdb" \
    "$main_measure_label" \
    "$TMP_DIR/kitesql-rocksdb" \
    "$BINARY" --backend kitesql-rocksdb "${main_measure_args[@]}" --num-ware "$NUM_WARE" --path "$TMP_DIR/kitesql-rocksdb"

run_variant \
    "sqlite-balanced" \
    "$main_measure_label" \
    "$TMP_DIR/sqlite-balanced.sqlite" \
    "$BINARY" --backend sqlite --sqlite-profile balanced "${main_measure_args[@]}" --num-ware "$NUM_WARE" --path "$TMP_DIR/sqlite-balanced.sqlite"

run_variant \
    "sqlite-practical" \
    "$main_measure_label" \
    "$TMP_DIR/sqlite-practical.sqlite" \
    "$BINARY" --backend sqlite --sqlite-profile practical "${main_measure_args[@]}" --num-ware "$NUM_WARE" --path "$TMP_DIR/sqlite-practical.sqlite"

rm -rf "$TMP_DIR"

printf '\nGenerated summary: %s\n' "$SUMMARY_FILE"
