# TPCC on KiteSQL
run `cargo run -p tpcc --release` to run tpcc

- i9-13900HX
- 32.0 GB
- KIOXIA-EXCERIA PLUS G3 SSD
- Tips: TPCC currently only supports single thread
```shell
|New-Order| sc: 93779  lt: 0  fl: 926
|Payment| sc: 93759  lt: 0  fl: 0
|Order-Status| sc: 9376  lt: 0  fl: 417
|Delivery| sc: 9375  lt: 0  fl: 0
|Stock-Level| sc: 9375  lt: 0  fl: 0
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
   New-Order Total: 93779
   Payment Total: 93759
   Order-Status Total: 9376
   Delivery Total: 9375
   Stock-Level Total: 9375


<RT Histogram>

1.New-Order

0.001,  20973
0.002,  71372
0.003,   1306
0.004,     15
0.005,      2

2.Payment

0.001,  90277
0.002,   3307
0.003,     11
0.004,      3

3.Order-Status

0.013,     24
0.014,    108
0.015,    189
0.016,    207
0.017,    201
0.018,    207
0.019,    221
0.020,    198
0.021,    187
0.022,    163
0.023,    175
0.024,    259
0.025,    298
0.026,    300
0.027,    239
0.028,    207
0.029,    165
0.030,    142
0.031,    135
0.032,    205
0.033,    303
0.034,    270
0.035,    272
0.036,    170
0.037,    138
0.038,    156
0.039,    175
0.040,    193
0.041,    207
0.042,    211
0.043,    264
0.044,    304
0.045,    265
0.046,    224
0.047,    172
0.048,    176
0.049,    172
0.050,    207
0.051,    238
0.052,    269
0.053,    219
0.054,    242
0.055,    156
0.056,    134
0.057,     94
0.058,     88
0.059,     75
0.060,     49
0.061,     13
0.062,      4
0.063,      1
0.064,      1
0.076,      1
0.080,      1
0.117,      1
0.127,      1
0.150,      1
0.158,      1
0.172,      1
0.176,      1

4.Delivery

0.011,    117
0.012,    483
0.013,    659
0.014,    704
0.015,    790
0.016,    911
0.017,    969
0.018,    974
0.019,    895
0.020,    927
0.021,    782
0.022,    542
0.023,    362
0.024,    209
0.025,     45
0.026,      1
0.027,      3

5.Stock-Level

0.001,   1815
0.002,   3454
0.003,   3646
0.004,    377
0.005,     28

<90th Percentile RT (MaxRT)>
   New-Order : 0.002  (0.004)
     Payment : 0.001  (0.025)
Order-Status : 0.053  (0.175)
    Delivery : 0.022  (0.027)
 Stock-Level : 0.003  (0.019)
<TpmC>
7815 tpmC
```

## Explain
run `cargo test -p tpcc explain_tpcc -- --ignored` to explain tpcc statements

Tips: after TPCC loaded tables

## Refer to
- https://github.com/AgilData/tpcc