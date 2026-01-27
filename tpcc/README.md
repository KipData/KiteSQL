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
| New-Order    |  326708 |    0 |    3334 | 330042 |
| Payment      |  326683 |    0 |   16218 | 342901 |
| Order-Status |   32669 |    0 |     547 | 33216 |
| Delivery     |   32669 |    0 |       0 | 32669 |
| Stock-Level  |   32668 |    0 |       0 | 32668 |
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

0.001, 219955
0.002, 106100
0.003,     44
0.004,      2
0.005,      1

2.Payment

0.001, 326442
0.002,     66
0.003,      1

3.Order-Status

0.001,  28771
0.002,   2736
0.003,    454
0.004,    145
0.005,     22
0.006,      2

4.Delivery

0.003,     11
0.004,   6201
0.005,   6965
0.006,   6338
0.007,   5805
0.008,   2971
0.009,    355
0.010,    535
0.011,    690
0.012,    980
0.013,    273
0.014,    693
0.015,    132
0.016,     43
0.017,      2
0.019,      1
0.021,      3
0.022,      1
0.024,      1

5.Stock-Level

0.001,  15844
0.002,  13502
0.003,   2228
0.004,    163
0.005,      1

<90th Percentile RT (MaxRT)>
   New-Order : 0.002  (0.005)
     Payment : 0.001  (0.013)
Order-Status : 0.002  (0.006)
    Delivery : 0.010  (0.023)
 Stock-Level : 0.002  (0.017)
<TpmC>
27226 Tpmc
```

## Refer to
- https://github.com/AgilData/tpcc
