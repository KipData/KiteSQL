# TPCC on KiteSQL
Run `make tpcc` (or `cargo run -p tpcc --release`) to exercise the workload on KiteSQL's native storage.

Run `make tpcc-dual` to execute the workload on KiteSQL while mirroring every statement to an in-memory SQLite database; the runner asserts that both engines return identical tuples, making it ideal for correctness validation. This target runs for 60 seconds (`--measure-time 60`). Use `cargo run -p tpcc --release -- --backend dual --measure-time <secs>` for a custom duration.

- i9-13900HX
- 32.0 GB
- KIOXIA-EXCERIA PLUS G3 SSD
- Tips: Pass `--threads <n>` to run multiple worker threads (default: 8)
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  221183 |    0 |    2284 | 223467 |
| Payment      |  221160 |    0 |    7346 | 228506 |
| Order-Status |   22116 |    0 |     493 | 22609 |
| Delivery     |   22117 |    0 |       0 | 22117 |
| Stock-Level  |   22116 |    0 |       0 | 22116 |
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

0.001, 153654
0.002,  66775
0.003,     17
0.004,      2
0.006,      1

2.Payment

0.001, 220847
0.002,     11
0.003,      1

3.Order-Status

0.001,  20887
0.002,    946
0.003,    123

4.Delivery

0.009,    342
0.010,    802
0.011,    862
0.012,   1017
0.013,   1309
0.014,   1514
0.015,   1647
0.016,   1788
0.017,   1797
0.018,   1792
0.019,   1747
0.020,   1533
0.021,   1333
0.022,   1179
0.023,    719
0.024,    358
0.025,    153
0.026,     13
0.027,      3
0.029,      2
0.037,      1
0.038,      1
0.039,      1

5.Stock-Level

0.001,  10061
0.002,   7547
0.003,   1844
0.004,     25
0.005,      1

<90th Percentile RT (MaxRT)>
   New-Order : 0.002  (0.006)
     Payment : 0.001  (0.019)
Order-Status : 0.001  (0.003)
    Delivery : 0.022  (0.038)
 Stock-Level : 0.002  (0.005)
<TpmC>
18432 Tpmc
```

## Refer to
- https://github.com/AgilData/tpcc
