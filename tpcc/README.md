# TPCC on KiteSQL
Run `make tpcc` (or `cargo run -p tpcc --release`) to exercise the workload on KiteSQL's native storage.

Run `make tpcc-dual` to execute the workload on KiteSQL while mirroring every statement to an in-memory SQLite database; the runner asserts that both engines return identical tuples, making it ideal for correctness validation. This target runs for 60 seconds (`--measure-time 60`). Use `cargo run -p tpcc --release -- --backend dual --measure-time <secs>` for a custom duration.

## Performance Matrix Script
Use `./scripts/run_tpcc_matrix.sh` to run the TPCC performance comparison in one shot.

- The script measures `kitesql-lmdb`, `kitesql-rocksdb`, `sqlite-balanced`, and `sqlite-practical`.
- Main variants follow TPCC's default duration unless `TPCC_MAIN_MEASURE_TIME` is overridden.
- If a run fails with a duplicate-key style error, the script clears that backend's database and retries that variant once.
- Outputs are written to `tpcc/results/<timestamp>/`, including `summary.md` and per-backend raw logs.

For more stable local numbers on machines that thermal-throttle under sustained TPCC load, use the Python runner:

```shell
./scripts/run_tpcc_stable.py --build
```

It runs the same four variants, but waits before each variant until the machine has enough consecutive stable samples:

- CPU temperature is at or below `--cool-temp-c` (default `65.0`).
- CPU usage is at or below `--idle-cpu-percent` (default `20.0`).
- At least `--min-cooldown-sec` seconds have passed after the previous variant (default `300`).
- The state remains stable for `--stable-samples` samples (default `3`, sampled every `10s`).

Example shorter smoke run:

```shell
./scripts/run_tpcc_stable.py --measure-time 60 --cool-temp-c 60 --min-cooldown-sec 180
```

Before a formal local run, clear old TPCC data and Linux page cache so each matrix starts from a comparable state:

```shell
rm -rf target/tpcc-stable-run-data target/tpcc-run-data kite_sql_tpcc kite_sql_tpcc.sqlite
sync
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
```

The runner reads CPU temperatures from Linux `/sys/class/hwmon` and `/sys/class/thermal` when available. If no CPU temperature sensor is exposed, it still gates on CPU usage and the fixed cooldown window, and records the missing temperature source in the raw log.

Duplicate-key note:
The benchmark stores `history.h_date` as `timestamp(6)`, so high-throughput `Payment` transactions do not collide on second-level timestamp buckets. A duplicate-primary-key failure during TPCC should be treated as a run failure and investigated or rerun from a clean database.

Example:
```shell
TPCC_DUPLICATE_RETRY=1 ./scripts/run_tpcc_matrix.sh
```

- i9-13900HX
- 32.0 GB
- KIOXIA-EXCERIA PLUS G3 SSD
- Tips: TPC-C currently runs as a single worker.

## 720s comparison
Local stable-run 720-second comparison on the machine above:

| Backend | TpmC | New-Order p90 | Payment p90 | Order-Status p90 | Delivery p90 | Stock-Level p90 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| KiteSQL LMDB | 71345 | 0.001s | 0.001s | 0.001s | 0.002s | 0.001s |
| KiteSQL RocksDB | 40563 | 0.001s | 0.001s | 0.002s | 0.009s | 0.001s |
| SQLite balanced | 67527 | 0.001s | 0.001s | 0.001s | 0.001s | 0.001s |
| SQLite practical | 64774 | 0.001s | 0.001s | 0.001s | 0.001s | 0.001s |

- All rows are from `./scripts/run_tpcc_stable.py --build` at `2026-06-23_00-50-48`.
- The stable-run gates were `temp<=65.0C`, `cpu<=20.0%`, `min_cooldown=300s`, `stable_samples=3`, and `sample_interval=10.0s`.
- All rows use `--num-ware 1`, `--max-retry 5`, and TPCC's default 720-second measure time.
- SQLite rows use the `balanced` and `practical` profiles respectively.

### KiteSQL LMDB
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  856143 |    0 |    8829 | 864972 |
| Payment      |  856120 |    0 |       0 | 856120 |
| Order-Status |   85612 |    0 |       0 | 85612 |
| Delivery     |   85612 |    0 |       0 | 85612 |
| Stock-Level  |   85612 |    0 |       0 | 85612 |
+--------------+---------+------+---------+-------+
<Constraint Check> (all must be [OK])
[transaction percentage]
   Payment:  43.5% (>=43.0%)  [OK]
   Order-Status:   4.3% (>=4.0%)  [OK]
   Delivery:   4.3% (>=4.0%)  [OK]
   Stock-Level:   4.3% (>=4.0%)  [OK]
[response time (at least 90% passed)]
   New-Order: 100.0%  [OK]
   Payment: 100.0%  [OK]
   Order-Status: 100.0%  [OK]
   Delivery: 100.0%  [OK]
   Stock-Level: 100.0%  [OK]


<RT Histogram>

1.New-Order

0.001, 856143

2.Payment

0.001, 856120

3.Order-Status

0.001,  83223
0.002,   2103
0.003,    286

4.Delivery

0.002,  85606
0.003,      6

5.Stock-Level

0.001,  85612

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.001)
     Payment : 0.001  (0.000)
Order-Status : 0.001  (0.002)
    Delivery : 0.002  (0.002)
 Stock-Level : 0.001  (0.000)
<TpmC>
71345 Tpmc
```

### KiteSQL RocksDB
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  486763 |    0 |    5029 | 491792 |
| Payment      |  486739 |    0 |       0 | 486739 |
| Order-Status |   48674 |    0 |       0 | 48674 |
| Delivery     |   48674 |    0 |       0 | 48674 |
| Stock-Level  |   48674 |    0 |       0 | 48674 |
+--------------+---------+------+---------+-------+
<Constraint Check> (all must be [OK])
[transaction percentage]
   Payment:  43.5% (>=43.0%)  [OK]
   Order-Status:   4.3% (>=4.0%)  [OK]
   Delivery:   4.3% (>=4.0%)  [OK]
   Stock-Level:   4.3% (>=4.0%)  [OK]
[response time (at least 90% passed)]
   New-Order: 100.0%  [OK]
   Payment: 100.0%  [OK]
   Order-Status: 100.0%  [OK]
   Delivery: 100.0%  [OK]
   Stock-Level: 100.0%  [OK]


<RT Histogram>

1.New-Order

0.001, 486709
0.002,     54

2.Payment

0.001, 486736
0.002,      2

3.Order-Status

0.001,  43202
0.002,   4215
0.003,    717
0.004,    395
0.005,    137
0.006,      7
0.007,      1

4.Delivery

0.002,   3572
0.003,   8048
0.004,   7083
0.005,   4455
0.006,   5238
0.007,   6637
0.008,   6138
0.009,   4702
0.010,   2626
0.011,    164
0.012,      4
0.013,      3
0.014,      3
0.022,      1

5.Stock-Level

0.001,  48674

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.001)
     Payment : 0.001  (0.007)
Order-Status : 0.002  (0.007)
    Delivery : 0.009  (0.022)
 Stock-Level : 0.001  (0.001)
<TpmC>
40563 Tpmc
```

### SQLite balanced
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  810343 |    0 |    8322 | 818665 |
| Payment      |  810320 |    0 |       0 | 810320 |
| Order-Status |   81032 |    0 |       0 | 81032 |
| Delivery     |   81032 |    0 |       0 | 81032 |
| Stock-Level  |   81032 |    0 |       0 | 81032 |
+--------------+---------+------+---------+-------+
<Constraint Check> (all must be [OK])
[transaction percentage]
   Payment:  43.5% (>=43.0%)  [OK]
   Order-Status:   4.3% (>=4.0%)  [OK]
   Delivery:   4.3% (>=4.0%)  [OK]
   Stock-Level:   4.3% (>=4.0%)  [OK]
[response time (at least 90% passed)]
   New-Order: 100.0%  [OK]
   Payment: 100.0%  [OK]
   Order-Status: 100.0%  [OK]
   Delivery: 100.0%  [OK]
   Stock-Level: 100.0%  [OK]


<RT Histogram>

1.New-Order

0.001, 810329
0.002,     14

2.Payment

0.001, 810320

3.Order-Status

0.001,  81032

4.Delivery

0.001,  81032

5.Stock-Level

0.001,  81032

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.001)
     Payment : 0.001  (0.000)
Order-Status : 0.001  (0.000)
    Delivery : 0.001  (0.001)
 Stock-Level : 0.001  (0.000)
<TpmC>
67527 Tpmc
```

### SQLite practical
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  777304 |    0 |    8128 | 785432 |
| Payment      |  777280 |    0 |       0 | 777280 |
| Order-Status |   77728 |    0 |       0 | 77728 |
| Delivery     |   77728 |    0 |       0 | 77728 |
| Stock-Level  |   77728 |    0 |       0 | 77728 |
+--------------+---------+------+---------+-------+
<Constraint Check> (all must be [OK])
[transaction percentage]
   Payment:  43.5% (>=43.0%)  [OK]
   Order-Status:   4.3% (>=4.0%)  [OK]
   Delivery:   4.3% (>=4.0%)  [OK]
   Stock-Level:   4.3% (>=4.0%)  [OK]
[response time (at least 90% passed)]
   New-Order: 100.0%  [OK]
   Payment: 100.0%  [OK]
   Order-Status: 100.0%  [OK]
   Delivery: 100.0%  [OK]
   Stock-Level: 100.0%  [OK]


<RT Histogram>

1.New-Order

0.001, 777302
0.002,      2

2.Payment

0.001, 777280

3.Order-Status

0.001,  77728

4.Delivery

0.001,  77728

5.Stock-Level

0.001,  77728

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.001)
     Payment : 0.001  (0.000)
Order-Status : 0.001  (0.000)
    Delivery : 0.001  (0.001)
 Stock-Level : 0.001  (0.000)
<TpmC>
64774 Tpmc
```

## Refer to
- https://github.com/AgilData/tpcc
