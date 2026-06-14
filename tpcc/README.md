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
| KiteSQL LMDB | 61723 | 0.001s | 0.001s | 0.001s | 0.002s | 0.001s |
| KiteSQL RocksDB | 30446 | 0.001s | 0.001s | 0.001s | 0.016s | 0.002s |
| SQLite balanced | 42989 | 0.001s | 0.001s | 0.001s | 0.001s | 0.001s |
| SQLite practical | 42276 | 0.001s | 0.001s | 0.001s | 0.001s | 0.001s |

- All rows are from fresh 720-second reruns with `--num-ware 1` and the default `--max-retry 5`.
- SQLite rows use the `balanced` and `practical` profiles respectively.
- Raw logs for this run were generated under `tpcc/results/2026-06-14_720-full-current/`.

### KiteSQL LMDB
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  740676 |    0 |    7570 | 748246 |
| Payment      |  740652 |    0 |       0 | 740652 |
| Order-Status |   74066 |    0 |       0 | 74066 |
| Delivery     |   74065 |    0 |       0 | 74065 |
| Stock-Level  |   74066 |    0 |       0 | 74066 |
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

0.001, 740407
0.002,    269

2.Payment

0.001, 740652

3.Order-Status

0.001,  72021
0.002,   1961
0.003,     81
0.004,      3

4.Delivery

0.002,  73846
0.003,    179
0.004,     40

5.Stock-Level

0.001,  73870
0.002,    192
0.003,      4

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.002)
     Payment : 0.001  (0.001)
Order-Status : 0.001  (0.004)
    Delivery : 0.002  (0.003)
 Stock-Level : 0.001  (0.002)
<TpmC>
61723 Tpmc
```

### KiteSQL RocksDB
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  365348 |    0 |    3642 | 368990 |
| Payment      |  365329 |    0 |       0 | 365329 |
| Order-Status |   36532 |    0 |       0 | 36532 |
| Delivery     |   36533 |    0 |       0 | 36533 |
| Stock-Level  |   36532 |    0 |       0 | 36532 |
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

0.001, 365260
0.002,     85
0.003,      3

2.Payment

0.001, 365325
0.002,      1
0.003,      1

3.Order-Status

0.001,  33395
0.002,   2506
0.003,    491
0.004,    133
0.005,      7

4.Delivery

0.002,    670
0.003,   3081
0.004,   3292
0.005,   1845
0.006,   2227
0.007,   2495
0.008,   2465
0.009,   2434
0.010,   2546
0.011,   2394
0.012,   2231
0.013,   2333
0.014,   2207
0.015,   2564
0.016,   2219
0.017,   1165
0.018,    345
0.019,     12
0.020,      3
0.021,      1
0.022,      1
0.023,      1
0.024,      1
0.026,      1

5.Stock-Level

0.001,  19405
0.002,  13755
0.003,   3316
0.004,     53
0.005,      1
0.007,      1

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.003)
     Payment : 0.001  (0.007)
Order-Status : 0.001  (0.005)
    Delivery : 0.016  (0.025)
 Stock-Level : 0.002  (0.011)
<TpmC>
30446 Tpmc
```

### SQLite balanced
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  515898 |    0 |    5323 | 521221 |
| Payment      |  515873 |    0 |       0 | 515873 |
| Order-Status |   51587 |    0 |       0 | 51587 |
| Delivery     |   51587 |    0 |       0 | 51587 |
| Stock-Level  |   51588 |    0 |       0 | 51588 |
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

0.001, 514986
0.002,    886
0.003,     26

2.Payment

0.001, 515873

3.Order-Status

0.001,  51587

4.Delivery

0.001,  51420
0.002,    165
0.003,      2

5.Stock-Level

0.001,  51588

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.003)
     Payment : 0.001  (0.001)
Order-Status : 0.001  (0.001)
    Delivery : 0.001  (0.003)
 Stock-Level : 0.001  (0.000)
<TpmC>
42989 Tpmc
```

### SQLite practical
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  507311 |    0 |    5123 | 512434 |
| Payment      |  507288 |    0 |       0 | 507288 |
| Order-Status |   50728 |    0 |       0 | 50728 |
| Delivery     |   50729 |    0 |       0 | 50729 |
| Stock-Level  |   50729 |    0 |       0 | 50729 |
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

0.001, 506391
0.002,    898
0.003,     22

2.Payment

0.001, 507287
0.002,      1

3.Order-Status

0.001,  50728

4.Delivery

0.001,  50566
0.002,    160
0.003,      3

5.Stock-Level

0.001,  50729

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.002)
     Payment : 0.001  (0.001)
Order-Status : 0.001  (0.001)
    Delivery : 0.001  (0.003)
 Stock-Level : 0.001  (0.000)
<TpmC>
42276 Tpmc
```

## Refer to
- https://github.com/AgilData/tpcc
