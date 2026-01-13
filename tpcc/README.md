# TPCC on KiteSQL
run `cargo run -p tpcc --release` to run tpcc

- i9-13900HX
- 32.0 GB
- KIOXIA-EXCERIA PLUS G3 SSD
- Tips: TPCC currently only supports single thread
```shell
|New-Order| sc: 434819  lt: 0  fl: 4430
|Payment| sc: 434798  lt: 0  fl: 0
|Order-Status| sc: 43480  lt: 0  fl: 598
|Delivery| sc: 43480  lt: 0  fl: 0
|Stock-Level| sc: 43479  lt: 0  fl: 0
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
   New-Order Total: 434819
   Payment Total: 434798
   Order-Status Total: 43480
   Delivery Total: 43480
   Stock-Level Total: 43479


<RT Histogram>

1.New-Order

0.001, 259304
0.002, 174946
0.003,    102
0.004,      7
0.007,      2

2.Payment

0.001, 431876
0.002,    285
0.003,      2

3.Order-Status

0.001,  36715
0.002,   4995
0.003,   1124
0.004,    366
0.005,    201
0.006,     71
0.007,      4
0.008,      1

4.Delivery

0.001,  42956

5.Stock-Level

0.001,  23079
0.002,  16248
0.003,   3817
0.004,    110
0.005,      2
0.007,      1

<90th Percentile RT (MaxRT)>
   New-Order : 0.002  (0.013)
     Payment : 0.001  (0.017)
Order-Status : 0.002  (0.016)
    Delivery : 0.001  (0.001)
 Stock-Level : 0.002  (0.022)
<TpmC>
36235 Tpmc
```

## Explain
run `cargo test -p tpcc explain_tpcc -- --ignored` to explain tpcc statements

Tips: after TPCC loaded tables

## Refer to
- https://github.com/AgilData/tpcc