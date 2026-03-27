# TPCC on KiteSQL
Run `make tpcc` (or `cargo run -p tpcc --release`) to exercise the workload on KiteSQL's native storage.

Run `make tpcc-dual` to execute the workload on KiteSQL while mirroring every statement to an in-memory SQLite database; the runner asserts that both engines return identical tuples, making it ideal for correctness validation. This target runs for 60 seconds (`--measure-time 60`). Use `cargo run -p tpcc --release -- --backend dual --measure-time <secs>` for a custom duration.

- i9-13900HX
- 32.0 GB
- KIOXIA-EXCERIA PLUS G3 SSD
- Tips: Pass `--threads <n>` to run multiple worker threads (default: 8)

## 720s comparison
Local 720-second comparison between SQLite practical and KiteSQL LMDB on the machine above:

| Backend | TpmC | New-Order p90 | Payment p90 | Order-Status p90 | Delivery p90 | Stock-Level p90 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SQLite practical | 35516 | 0.001s | 0.001s | 0.001s | 0.001s | 0.001s |
| KiteSQL LMDB | 29171 | 0.001s | 0.001s | 0.001s | 0.015s | 0.002s |

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
| New-Order    |  350056 |    0 |    3659 | 353715 |
| Payment      |  350032 |    0 |   19195 | 369227 |
| Order-Status |   35003 |    0 |     601 | 35604 |
| Delivery     |   35003 |    0 |       0 | 35003 |
| Stock-Level  |   35003 |    0 |       0 | 35003 |
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

0.001, 345360
0.002,   4681
0.003,     13
0.004,      2

2.Payment

0.001, 350028
0.002,      4

3.Order-Status

0.001,  34692
0.002,    311

4.Delivery

0.004,    177
0.005,   2737
0.006,   2956
0.007,   2668
0.008,   3165
0.009,   2548
0.010,   2632
0.011,   3188
0.012,   2946
0.013,   4605
0.014,   2900
0.015,   2535
0.016,   1423
0.017,    150
0.018,    105
0.019,     87
0.020,     59
0.021,     52
0.022,     39
0.023,     25
0.024,      5
0.025,      1

5.Stock-Level

0.001,  31058
0.002,   3938
0.003,      7

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.003)
     Payment : 0.001  (0.002)
Order-Status : 0.001  (0.002)
    Delivery : 0.015  (0.025)
 Stock-Level : 0.002  (0.002)
<TpmC>
29171 Tpmc
```

## Refer to
- https://github.com/AgilData/tpcc
