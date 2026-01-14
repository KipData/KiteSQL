# TPCC on KiteSQL
run `cargo run -p tpcc --release` to run tpcc

- i9-13900HX
- 32.0 GB
- KIOXIA-EXCERIA PLUS G3 SSD
- Tips: TPCC currently only supports single thread
```shell
|New-Order| sc: 133498  lt: 0  fl: 1360
|Payment| sc: 133473  lt: 0  fl: 0
|Order-Status| sc: 13348  lt: 0  fl: 450
|Delivery| sc: 13348  lt: 0  fl: 0
|Stock-Level| sc: 13347  lt: 0  fl: 0
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
   New-Order Total: 133498
   Payment Total: 133473
   Order-Status Total: 13348
   Delivery Total: 13348
   Stock-Level Total: 13347


<RT Histogram>

1.New-Order

0.001,  83231
0.002,  49784
0.003,     36
0.004,      4
0.005,      2

2.Payment

0.001, 133281
0.002,    184
0.003,      2

3.Order-Status

0.012,     31
0.013,    265
0.014,    332
0.015,    307
0.016,    296
0.017,    284
0.018,    303
0.019,    415
0.020,    386
0.021,    382
0.022,    252
0.023,    228
0.024,    264
0.025,    249
0.026,    268
0.027,    253
0.028,    246
0.029,    277
0.030,    253
0.031,    237
0.032,    289
0.033,    172
0.034,    192
0.035,    268
0.036,    266
0.037,    276
0.038,    243
0.039,    223
0.040,    216
0.041,    225
0.042,    248
0.043,    193
0.044,    174
0.045,    307
0.046,    305
0.047,    246
0.048,    213
0.049,    267
0.050,    197
0.051,    182
0.052,    207
0.053,     84
0.054,     42
0.055,     54
0.056,    102
0.057,    156
0.058,    165
0.059,    199
0.060,    195
0.061,    173
0.062,    141
0.063,    102
0.064,     56
0.065,     29
0.066,      8
0.067,      6
0.068,      2
0.069,      3
0.075,      1
0.078,      1

4.Delivery

0.001,  11129
0.002,      1

5.Stock-Level

0.001,   5697
0.002,   4669
0.003,    494
0.004,      8
0.005,      1

<90th Percentile RT (MaxRT)>
   New-Order : 0.002  (0.005)
     Payment : 0.001  (0.003)
Order-Status : 0.057  (0.088)
    Delivery : 0.001  (0.001)
 Stock-Level : 0.002  (0.006)
<TpmC>
11125 Tpmc
```

## Explain
run `cargo test -p tpcc explain_tpcc -- --ignored` to explain tpcc statements

Tips: after TPCC loaded tables

## Refer to
- https://github.com/AgilData/tpcc