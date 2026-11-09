# TPCC on KiteSQL
Run `make tpcc` (or `cargo run -p tpcc --release`) to exercise the workload on KiteSQL with LMDB storage.

| Target | Command |
| --- | --- |
| `make tpcc` / `make tpcc-kitesql-lmdb` | `--backend kitesql-lmdb` |
| `make tpcc-kitesql-rocksdb` | `--backend kitesql-rocksdb` |
| `make tpcc-sqlite` | `--backend sqlite --path kite_sql_tpcc.sqlite` |
| `make tpcc-dual` | `--backend dual --measure-time 60` |
| `make tpcc-lmdb-flamegraph` | LMDB run with a pprof flamegraph (`TPCC_PPROF_OUTPUT`) |
| `make tpcc-lmdb-heaptrack` | LMDB run under heaptrack (`TPCC_HEAPTRACK_OUTPUT`) |

`tpcc-dual` executes the workload on KiteSQL while mirroring every statement to an in-memory SQLite database and asserts that both engines return identical tuples, which makes it the correctness check.

Common options:

| Option | Default | Meaning |
| --- | --- | --- |
| `--backend` | `kitesql-lmdb` | `kitesql-lmdb`, `kitesql-rocksdb`, `kitesql-optimistic-rocksdb`, `sqlite`, or `dual`; `kitesql-slatedb-s3` or `sqlite` in the [SlateDB build](#slatedb-on-s3) |
| `--path` | `kite_sql_tpcc` | Database path |
| `--threads` | `1` | Threads that run the terminals |
| `--terminals` | `--threads` | Concurrent terminals; terminal `i` has home warehouse `i % num_ware + 1`. More terminals than threads needs an async storage (SlateDB) |
| `--num-ware` | `1` | Number of warehouses |
| `--measure-time` | `720` | Measured seconds after loading |
| `--max-retry` | `5` | Retries per transaction; conflicts are counted apart from failures and retried after a random backoff |
| `--sync` | off | Wait for each commit to be durable: fsync for LMDB and RocksDB, `synchronous = FULL` instead of `OFF` for SQLite, the write to S3 for SlateDB |
| `--rocksdb-stats` | off | Print RocksDB statistics |

## SlateDB on S3
KiteSQL on SlateDB runs on the async path, which excludes the sync storages, so it is a separate build:

```shell
AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
cargo run -p tpcc --release --no-default-features --features slatedb-s3 -- \
    --s3-bucket <bucket> [--s3-endpoint http://127.0.0.1:9000 --s3-allow-http]
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--s3-bucket` | required | Bucket; credentials and region come from `AWS_*` or the default AWS chain |
| `--s3-endpoint` | AWS | Endpoint of an S3-compatible store |
| `--s3-allow-http` | off | Allow a plain `http://` endpoint |
| `--path` | `kite_sql_tpcc` | Key prefix; each run writes under a new `<path>/<millis>` |
| `--slatedb-memory-cache-mb` | `4096` | Memory of the block cache |
| `--slatedb-disk-cache` | `kite_sql_tpcc_cache` | Local disk tier of the block cache, cleared before each run |
| `--slatedb-disk-cache-mb` | `65536` | Its capacity; the files grow as they fill. `0` turns it off |

Blocks written by compactions also go into the cache. Without a cache that holds the data, every miss costs an S3 request in the transaction, and TpmC drops by orders of magnitude.

## Stable Performance Runner
Use the Python runner for local performance comparisons:

```shell
./scripts/run_tpcc_stable.py --build
```

It runs `kitesql-lmdb`, `kitesql-rocksdb`, and `sqlite` (select a subset with `--variants`), pins TPCC to one CPU, and waits before each variant until the machine has enough consecutive stable samples.

By default TPCC runs as a single worker, so its speed depends on the core it lands on. On hybrid CPUs (P-cores and E-cores) an unpinned run can differ by 30-60% depending on scheduler placement. The runner therefore pins every variant, including its background threads, to the strongest core by default: the one with the highest `cpuinfo_max_freq`, lowest id on ties, skipping the physical core of `cpu0`. On hybrid CPUs this is a P-core, and on CPUs with Turbo Boost Max 3.0 it is one of the favored P-cores. Use `--cpu <n>` to choose one or `--no-cpu-pin` to disable pinning. The chosen CPU is recorded in `summary.md`.

The runner waits until:

- CPU temperature is at or below `--cool-temp-c` (default `70.0`).
- CPU usage is at or below `--idle-cpu-percent` (default `20.0`).
- At least `--min-cooldown-sec` seconds have passed after the previous variant (default `300`).
- The state remains stable for `--stable-samples` samples (default `3`, sampled every `10s`).

Example shorter smoke run:

```shell
./scripts/run_tpcc_stable.py --measure-time 60 --cool-temp-c 60 --min-cooldown-sec 180
```

Before a formal local run, clear old TPCC data and Linux page cache so each matrix starts from a comparable state:

```shell
rm -rf target/tpcc-stable-run-data kite_sql_tpcc kite_sql_tpcc.sqlite
sync
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
```

The runner reads CPU temperatures from Linux `/sys/class/hwmon` and `/sys/class/thermal` when available. If no CPU temperature sensor is exposed, it still gates on CPU usage and the fixed cooldown window, and records the missing temperature source in the raw log.

Duplicate-key note:
The benchmark stores `history.h_date` as `timestamp(6)`, so high-throughput `Payment` transactions do not collide on second-level timestamp buckets. A duplicate-primary-key failure during TPCC should be treated as a run failure and investigated or rerun from a clean database.

- i9-13900HX
- 32.0 GB
- KIOXIA-EXCERIA PLUS G3 SSD

## 720s comparison
Local 720-second comparison on the machine above. Latencies are p90 in µs.

1 thread, 1 warehouse:

| Backend | TpmC | New-Order p90 (µs) | Payment p90 (µs) | Order-Status p90 (µs) | Delivery p90 (µs) | Stock-Level p90 (µs) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| KiteSQL LMDB | 254023 | 211 | 38 | 25 | 293 | 175 |
| KiteSQL RocksDB | 47488 | 372 | 85 | 415 | 15471 | 389 |
| KiteSQL SlateDB (S3) | 12016 | 677 | 240 | 399 | 74047 | 1638 |
| SQLite | 214001 | 251 | 37 | 32 | 338 | 227 |

4 threads, 4 warehouses:

| Backend | TpmC | New-Order p90 (µs) | Payment p90 (µs) | Order-Status p90 (µs) | Delivery p90 (µs) | Stock-Level p90 (µs) | Conflicts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| KiteSQL LMDB | 240584 | 930 | 780 | 29 | 890 | 207 | 0 |
| KiteSQL RocksDB | 212936 | 505 | 109 | 285 | 8991 | 400 | 529 |
| KiteSQL SlateDB (S3) | 19010 | 1749 | 1816 | 2481 | 66495 | 2523 | 51 |
| SQLite | 205896 | 254 | 41 | 45 | 336 | 380 | 0 |

- Run date: `2026-11-09`. Latency is measured in microseconds and includes commit and retries.
- The 1-thread rows are pinned to `cpu8`, the highest-frequency P-core on this machine (5.4 GHz), background threads included. The 4-thread rows are pinned to `cpu8,10,2,4`, four distinct physical P-cores. These runs used `taskset` directly, since the stable runner covers neither threads nor SlateDB.
- The 4-thread rows give each terminal its own home warehouse, so they scale data with threads.
- All rows use `--max-retry 5` and run without `--sync`, so commits are not fsynced and SlateDB does not wait for S3; SQLite uses WAL with `synchronous = OFF`.

The runner writes `summary.md` and per-variant logs to `tpcc/results/<timestamp>/`.

## Refer to
- https://github.com/AgilData/tpcc
