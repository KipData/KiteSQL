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
The occasional duplicate-primary-key failure during TPCC reruns is not treated here as a database correctness bug. In this benchmark setup, the `history` table uses `h_date` as part of its primary key; under very high `Payment` throughput, multiple transactions can hit the same timestamp bucket for the same logical history key and collide on the benchmark schema itself. That is why the matrix script simply reruns the affected backend from a fresh database when this specific condition appears.

Example:
```shell
TPCC_DUPLICATE_RETRY=1 ./scripts/run_tpcc_matrix.sh
```

- i9-13900HX
- 32.0 GB
- KIOXIA-EXCERIA PLUS G3 SSD
- Tips: Pass `--threads <n>` to run multiple worker threads (default: 8)

## 720s comparison
Local 720-second comparison on the machine above:

| Backend | TpmC | New-Order p90 | Payment p90 | Order-Status p90 | Delivery p90 | Stock-Level p90 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| KiteSQL LMDB | 53510 | 0.001s | 0.001s | 0.001s | 0.002s | 0.001s |
| KiteSQL RocksDB | 32248 | 0.001s | 0.001s | 0.002s | 0.011s | 0.003s |
| SQLite balanced | 36273 | 0.001s | 0.001s | 0.001s | 0.001s | 0.001s |
| SQLite practical | 35516 | 0.001s | 0.001s | 0.001s | 0.001s | 0.001s |

- `SQLite practical` keeps the previous stable README result because the latest rerun failed twice with `UNIQUE constraint failed: history...h_date`.
- The latest rerun summary is recorded in `tpcc/results/2026-03-28_19-51-45/summary.md`.

### SQLite practical
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  426196 |    0 |    4361 | 430557 |
| Payment      |  426170 |    0 |   28459 | 454629 |
| Order-Status |   42617 |    0 |     618 | 43235 |
| Delivery     |   42617 |    0 |       0 | 42617 |
| Stock-Level  |   42617 |    0 |       0 | 42617 |
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

0.001, 425333
0.002,    858
0.003,      3
0.004,      2

2.Payment

0.001, 426168
0.002,      1
0.003,      1

3.Order-Status

0.001,  42617

4.Delivery

0.001,  42284
0.002,    331
0.003,      2

5.Stock-Level

0.001,  42617

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.004)
     Payment : 0.001  (0.003)
Order-Status : 0.001  (0.001)
    Delivery : 0.001  (0.003)
 Stock-Level : 0.001  (0.001)
<TpmC>
35516 Tpmc
```

### KiteSQL LMDB
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  642115 |    0 |    6475 | 648590 |
| Payment      |  642093 |    0 |   60782 | 702875 |
| Order-Status |   64209 |    0 |     621 | 64830 |
| Delivery     |   64209 |    0 |       0 | 64209 |
| Stock-Level  |   64209 |    0 |       0 | 64209 |
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

0.001, 640181
0.002,   1933
0.003,      1

2.Payment

0.001, 642092
0.002,      1

3.Order-Status

0.001,  62712
0.002,   1482
0.003,     13
0.004,      2

4.Delivery

0.002,  63332
0.003,    785
0.004,     91
0.005,      1

5.Stock-Level

0.001,  62987
0.002,   1220
0.003,      2

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.002)
     Payment : 0.001  (0.001)
Order-Status : 0.001  (0.003)
    Delivery : 0.002  (0.004)
 Stock-Level : 0.001  (0.002)
<TpmC>
53510 Tpmc
```

### KiteSQL RocksDB
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  386982 |    0 |    3867 | 390849 |
| Payment      |  386959 |    0 |   23536 | 410495 |
| Order-Status |   38696 |    0 |     591 | 39287 |
| Delivery     |   38696 |    0 |       0 | 38696 |
| Stock-Level  |   38696 |    0 |       0 | 38696 |
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

0.001, 375252
0.002,  11525
0.003,    202
0.004,      2

2.Payment

0.001, 386703
0.002,    250
0.003,      5

3.Order-Status

0.001,  32982
0.002,   4124
0.003,   1025
0.004,    324
0.005,    176
0.006,     53
0.007,      8
0.009,      1

4.Delivery

0.002,      6
0.003,   8086
0.004,  10302
0.005,   8066
0.006,   1217
0.007,   1153
0.008,   1062
0.009,   1635
0.010,   1907
0.011,   2165
0.012,   1612
0.013,   1135
0.014,    226
0.015,     45
0.016,     23
0.017,     17
0.018,     13
0.019,     10
0.020,      8
0.021,      4
0.022,      3
0.023,      1

5.Stock-Level

0.001,  20753
0.002,  13780
0.003,   3570
0.004,    526
0.005,     39
0.006,     12
0.007,      9
0.008,      3
0.009,      2
0.010,      2

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.005)
     Payment : 0.001  (0.006)
Order-Status : 0.002  (0.012)
    Delivery : 0.011  (0.023)
 Stock-Level : 0.003  (0.010)
<TpmC>
32248 Tpmc
```

### SQLite balanced
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  435291 |    0 |    4433 | 439724 |
| Payment      |  435265 |    0 |   33227 | 468492 |
| Order-Status |   43527 |    0 |     566 | 44093 |
| Delivery     |   43527 |    0 |       0 | 43527 |
| Stock-Level  |   43527 |    0 |       0 | 43527 |
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

0.001, 435079
0.002,    210
0.003,      2

2.Payment

0.001, 435265

3.Order-Status

0.001,  43527

4.Delivery

0.001,  43442
0.002,     85

5.Stock-Level

0.001,  43527

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.003)
     Payment : 0.001  (0.001)
Order-Status : 0.001  (0.001)
    Delivery : 0.001  (0.002)
 Stock-Level : 0.001  (0.000)
<TpmC>
36273 Tpmc
```

## Refer to
- https://github.com/AgilData/tpcc
