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
| KiteSQL LMDB | 73638 | 0.001s | 0.001s | 0.001s | 0.002s | 0.001s |
| KiteSQL RocksDB | 39051 | 0.001s | 0.001s | 0.002s | 0.009s | 0.001s |
| SQLite balanced | 56788 | 0.001s | 0.001s | 0.001s | 0.001s | 0.001s |
| SQLite practical | 44049 | 0.001s | 0.001s | 0.001s | 0.001s | 0.001s |

- All rows are from fresh 720-second reruns with `--num-ware 1` and the default `--max-retry 5`.
- SQLite rows use the `balanced` and `practical` profiles respectively.

### KiteSQL LMDB
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  883651 |    0 |    8950 | 892601 |
| Payment      |  883630 |    0 |       0 | 883630 |
| Order-Status |   88363 |    0 |       0 | 88363 |
| Delivery     |   88363 |    0 |       0 | 88363 |
| Stock-Level  |   88363 |    0 |       0 | 88363 |
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

0.001, 883629
0.002,     19
0.003,      3

2.Payment

0.001, 883628
0.002,      2

3.Order-Status

0.001,  85670
0.002,   2219
0.003,    474

4.Delivery

0.002,  88351
0.003,      8
0.004,      2
0.005,      1
0.006,      1

5.Stock-Level

0.001,  88363

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.003)
     Payment : 0.001  (0.001)
Order-Status : 0.001  (0.003)
    Delivery : 0.002  (0.006)
 Stock-Level : 0.001  (0.001)
<TpmC>
73638 Tpmc
```

### KiteSQL RocksDB
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  468621 |    0 |    4749 | 473370 |
| Payment      |  468598 |    0 |       0 | 468598 |
| Order-Status |   46859 |    0 |       0 | 46859 |
| Delivery     |   46860 |    0 |       0 | 46860 |
| Stock-Level  |   46860 |    0 |       0 | 46860 |
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

0.001, 468582
0.002,     35
0.003,      1

2.Payment

0.001, 468590
0.002,      4

3.Order-Status

0.001,  41541
0.002,   4161
0.003,    702
0.004,    332
0.005,    116
0.006,      6

4.Delivery

0.002,   2614
0.003,   7226
0.004,   6196
0.005,   3654
0.006,   5167
0.007,   6457
0.008,   6343
0.009,   5644
0.010,   3185
0.011,    363
0.012,      7
0.013,      2
0.015,      2

5.Stock-Level

0.001,  46860

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.008)
     Payment : 0.001  (0.008)
Order-Status : 0.002  (0.010)
    Delivery : 0.009  (0.014)
 Stock-Level : 0.001  (0.000)
<TpmC>
39051 Tpmc
```

### SQLite balanced
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  681461 |    0 |    6997 | 688458 |
| Payment      |  681435 |    0 |       0 | 681435 |
| Order-Status |   68143 |    0 |       0 | 68143 |
| Delivery     |   68144 |    0 |       0 | 68144 |
| Stock-Level  |   68144 |    0 |       0 | 68144 |
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

0.001, 681310
0.002,    151

2.Payment

0.001, 681435

3.Order-Status

0.001,  68143

4.Delivery

0.001,  68121
0.002,     23

5.Stock-Level

0.001,  68144

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.001)
     Payment : 0.001  (0.001)
Order-Status : 0.001  (0.000)
    Delivery : 0.001  (0.001)
 Stock-Level : 0.001  (0.000)
<TpmC>
56788 Tpmc
```

### SQLite practical
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  528594 |    0 |    5563 | 534157 |
| Payment      |  528570 |    0 |       0 | 528570 |
| Order-Status |   52857 |    0 |       0 | 52857 |
| Delivery     |   52857 |    0 |       0 | 52857 |
| Stock-Level  |   52857 |    0 |       0 | 52857 |
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

0.001, 528448
0.002,    141
0.003,      5

2.Payment

0.001, 528567
0.002,      3

3.Order-Status

0.001,  52857

4.Delivery

0.001,  52831
0.002,     25
0.003,      1

5.Stock-Level

0.001,  52857

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.003)
     Payment : 0.001  (0.002)
Order-Status : 0.001  (0.000)
    Delivery : 0.001  (0.003)
 Stock-Level : 0.001  (0.000)
<TpmC>
44049 Tpmc
```

## Refer to
- https://github.com/AgilData/tpcc
