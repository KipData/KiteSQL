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
| `--backend` | `kitesql-lmdb` | `kitesql-lmdb`, `kitesql-rocksdb`, `kitesql-optimistic-rocksdb`, `sqlite`, or `dual` |
| `--path` | `kite_sql_tpcc` | Database path |
| `--threads` | `1` | Concurrent terminals, each on its own connection |
| `--num-ware` | `1` | Number of warehouses |
| `--measure-time` | `720` | Measured seconds after loading |
| `--max-retry` | `5` | Retries per failed transaction |
| `--sync` | off | Fsync on commit (SQLite: `synchronous = FULL` instead of `OFF`) |
| `--rocksdb-stats` | off | Print RocksDB statistics |

## Stable Performance Runner
Use the Python runner for local performance comparisons:

```shell
./scripts/run_tpcc_stable.py --build
```

It runs `kitesql-lmdb`, `kitesql-rocksdb`, and `sqlite` (select a subset with `--variants`), pins TPCC to one CPU, and waits before each variant until the machine has enough consecutive stable samples.

TPCC runs as a single worker, so its speed depends on the core it lands on. On hybrid CPUs (P-cores and E-cores) an unpinned run can differ by 30-60% depending on scheduler placement. The runner therefore pins every variant, including its background threads, to the strongest core by default: the one with the highest `cpuinfo_max_freq`, lowest id on ties, skipping the physical core of `cpu0`. On hybrid CPUs this is a P-core, and on CPUs with Turbo Boost Max 3.0 it is one of the favored P-cores. Use `--cpu <n>` to choose one or `--no-cpu-pin` to disable pinning. The chosen CPU is recorded in `summary.md`.

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
- Tips: TPC-C currently runs as a single worker.

## 720s comparison
Local 720-second comparison on the machine above:

| Backend | TpmC | New-Order p90 (µs) | Payment p90 (µs) | Order-Status p90 (µs) | Delivery p90 (µs) | Stock-Level p90 (µs) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| KiteSQL LMDB | 165956 | 303 | 75 | 43 | 436 | 318 |
| KiteSQL RocksDB | 50739 | 392 | 188 | 198 | 11743 | 812 |
| SQLite | 191484 | 274 | 41 | 33 | 343 | 268 |

- Run date: `2026-10-25`. Latency is measured in microseconds and includes commit.
- Every row is pinned to `cpu8`, the highest-frequency P-core on this machine (5.4 GHz).
- All rows use `--num-ware 1`, `--max-retry 5`, and TPCC's default 720-second measure time.
- All rows run without `--sync`, so commits are not fsynced; SQLite uses WAL with `synchronous = OFF`.

The runner writes `summary.md` and per-variant logs to `tpcc/results/<timestamp>/`.

## Refer to
- https://github.com/AgilData/tpcc
