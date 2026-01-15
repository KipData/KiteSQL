# TPCC on KiteSQL
Run `make tpcc` (or `cargo run -p tpcc --release`) to exercise the workload on KiteSQL's native storage.

Run `make tpcc-dual` to execute the workload on KiteSQL while mirroring every statement to an in-memory SQLite database; the runner asserts that both engines return identical tuples, making it ideal for correctness validation. This target runs for 60 seconds (`--measure-time 60`). Use `cargo run -p tpcc --release -- --backend dual --measure-time <secs>` for a custom duration.

- i9-13900HX
- 32.0 GB
- KIOXIA-EXCERIA PLUS G3 SSD
- Tips: Pass `--threads <n>` to run multiple worker threads (default: 8)
```shell
|New-Order| sc: 445996  lt: 0  fl: 4649
|Payment| sc: 445972  lt: 0  fl: 0
|Order-Status| sc: 44597  lt: 0  fl: 622
|Delivery| sc: 44597  lt: 0  fl: 0
|Stock-Level| sc: 44597  lt: 0  fl: 0
in 720 sec.
<Constraint Check> (all must be [OK])
[transaction percentage]
   Payment: 43.0% (>=43.0%)  [Ok]
   Order-Status: 4.0% (>=4.0%)  [Ok]
   Delivery: 4.0% (>=4.0%)  [Ok]
   Stock-Level: 4.0% (>=4.0%)  [Ok]
[response time (at least 90%% passed)]
   New-Order: 100.0  [OK]
   Payment: 100.0  [OK]
   Order-Status: 100.0  [OK]
   Delivery: 100.0  [OK]
   Stock-Level: 100.0  [OK]
   New-Order Total: 445996
   Payment Total: 445972
   Order-Status Total: 44597
   Delivery Total: 44597
   Stock-Level Total: 44597


<RT Histogram>

1.New-Order

0.001, 282206
0.002, 163359
0.003,     95
0.004,      6

2.Payment

0.001, 444948
0.002,    260

3.Order-Status

0.001,  36721
0.002,   5254
0.003,   1110
0.004,    396
0.005,    189
0.006,     62
0.007,      6

4.Delivery

0.001,  43260

5.Stock-Level

0.001,  22651
0.002,  18136
0.003,   3223
0.004,    108
0.005,      5
0.006,      4
0.007,      1

<90th Percentile RT (MaxRT)>
   New-Order : 0.002  (0.012)
     Payment : 0.001  (0.002)
Order-Status : 0.002  (0.019)
    Delivery : 0.001  (0.001)
 Stock-Level : 0.002  (0.018)
<TpmC>
37166 Tpmc
```

## Refer to
- https://github.com/AgilData/tpcc
