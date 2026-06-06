# TPCC on KiteSQL
Run `make tpcc` (or `cargo run -p tpcc --release`) to exercise the workload on KiteSQL's native storage.

Run `make tpcc-dual` to execute the workload on KiteSQL while mirroring every statement to an in-memory SQLite database; the runner asserts that both engines return identical tuples, making it ideal for correctness validation. This target runs for 60 seconds (`--measure-time 60`). Use `cargo run -p tpcc --release -- --backend dual --measure-time <secs>` for a custom duration.

## Performance Matrix Script
Use `./scripts/run_tpcc_matrix.sh` to run the TPCC performance comparison in one shot.

- The script measures `kitesql-lmdb`, `kitesql-rocksdb`, `sqlite-balanced`, and `sqlite-practical`.
- Main variants follow TPCC's default duration unless `TPCC_MAIN_MEASURE_TIME` is overridden.
- If a run fails with a duplicate-key style error, the script clears that backend's database and retries that variant once.
- Outputs are written to `tpcc/results/<timestamp>/`, including `summary.md` and per-backend raw logs.

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
Local 720-second comparison on the machine above:

| Backend | TpmC | New-Order p90 | Payment p90 | Order-Status p90 | Delivery p90 | Stock-Level p90 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| KiteSQL LMDB | 68394 | 0.001s | 0.001s | 0.001s | 0.002s | 0.001s |
| KiteSQL RocksDB | 30387 | 0.001s | 0.001s | 0.001s | 0.015s | 0.002s |
| SQLite balanced | 41690 | 0.001s | 0.001s | 0.001s | 0.001s | 0.001s |
| SQLite practical | 38861 | 0.001s | 0.001s | 0.001s | 0.001s | 0.001s |

- All rows are from fresh 720-second reruns with `--num-ware 1` and the default `--max-retry 5`.
- SQLite rows use the `balanced` and `practical` profiles respectively.

### SQLite practical
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  466339 |    0 |    4794 | 471133 |
| Payment      |  466316 |    0 |       0 | 466316 |
| Order-Status |   46632 |    0 |       0 | 46632 |
| Delivery     |   46631 |    0 |       0 | 46631 |
| Stock-Level  |   46632 |    0 |       0 | 46632 |
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

0.001, 465092
0.002,   1232
0.003,     15

2.Payment

0.001, 466316

3.Order-Status

0.001,  46632

4.Delivery

0.001,  46388
0.002,    240
0.003,      3

5.Stock-Level

0.001,  46632

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.002)
     Payment : 0.001  (0.001)
Order-Status : 0.001  (0.000)
    Delivery : 0.001  (0.002)
 Stock-Level : 0.001  (0.000)
<TpmC>
38861 Tpmc
```

### KiteSQL LMDB
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  820734 |    0 |    8539 | 829273 |
| Payment      |  820710 |    0 |       0 | 820710 |
| Order-Status |   82071 |    0 |       0 | 82071 |
| Delivery     |   82071 |    0 |       0 | 82071 |
| Stock-Level  |   82071 |    0 |       0 | 82071 |
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

0.001, 820617
0.002,    101
0.003,     15
0.005,      1

2.Payment

0.001, 820710

3.Order-Status

0.001,  79893
0.002,   2053
0.003,    125

4.Delivery

0.002,  82014
0.003,     41
0.004,     11
0.005,      2
0.006,      2
0.007,      1

5.Stock-Level

0.001,  82041
0.002,     27
0.003,      2
0.004,      1

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.005)
     Payment : 0.001  (0.001)
Order-Status : 0.001  (0.002)
    Delivery : 0.002  (0.006)
 Stock-Level : 0.001  (0.003)
<TpmC>
68394 Tpmc
```

### KiteSQL RocksDB
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  364643 |    0 |    3680 | 368323 |
| Payment      |  364620 |    0 |       0 | 364620 |
| Order-Status |   36462 |    0 |       0 | 36462 |
| Delivery     |   36462 |    0 |       0 | 36462 |
| Stock-Level  |   36462 |    0 |       0 | 36462 |
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

0.001, 364318
0.002,    317
0.003,      6

2.Payment

0.001, 364591
0.002,     25

3.Order-Status

0.001,  33075
0.002,   2697
0.003,    507
0.004,    139
0.005,     17

4.Delivery

0.002,    267
0.003,   2720
0.004,   3784
0.005,   2306
0.006,   2973
0.007,   3023
0.008,   2369
0.009,   2423
0.010,   2347
0.011,   2531
0.012,   2473
0.013,   2253
0.014,   2295
0.015,   2402
0.016,   1429
0.017,    740
0.018,    102
0.019,     16
0.020,      4
0.021,      2
0.022,      2
0.023,      1

5.Stock-Level

0.001,  20624
0.002,  12335
0.003,   3423
0.004,     74
0.005,      3
0.006,      1

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.008)
     Payment : 0.001  (0.009)
Order-Status : 0.001  (0.006)
    Delivery : 0.015  (0.022)
 Stock-Level : 0.002  (0.011)
<TpmC>
30387 Tpmc
```

### SQLite balanced
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  500279 |    0 |    5039 | 505318 |
| Payment      |  500257 |    0 |       0 | 500257 |
| Order-Status |   50025 |    0 |       0 | 50025 |
| Delivery     |   50026 |    0 |       0 | 50026 |
| Stock-Level  |   50026 |    0 |       0 | 50026 |
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

0.001, 497040
0.002,   3214
0.003,     20
0.004,      4
0.005,      1

2.Payment

0.001, 500250
0.002,      5
0.003,      2

3.Order-Status

0.001,  50025

4.Delivery

0.001,  49409
0.002,    612
0.003,      5

5.Stock-Level

0.001,  50026

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.004)
     Payment : 0.001  (0.002)
Order-Status : 0.001  (0.001)
    Delivery : 0.001  (0.002)
 Stock-Level : 0.001  (0.000)
<TpmC>
41690 Tpmc
```

## Refer to
- https://github.com/AgilData/tpcc
