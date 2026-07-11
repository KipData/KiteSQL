# TPCC on KiteSQL
Run `make tpcc` (or `cargo run -p tpcc --release`) to exercise the workload on KiteSQL's native storage.

Run `make tpcc-dual` to execute the workload on KiteSQL while mirroring every statement to an in-memory SQLite database; the runner asserts that both engines return identical tuples, making it ideal for correctness validation. This target runs for 60 seconds (`--measure-time 60`). Use `cargo run -p tpcc --release -- --backend dual --measure-time <secs>` for a custom duration.

## Stable Performance Runner
Use the Python runner for local performance comparisons:

```shell
./scripts/run_tpcc_stable.py --build
```

It runs the same four variants, but waits before each variant until the machine has enough consecutive stable samples:

- CPU temperature is at or below `--cool-temp-c` (default `65.0`).
- CPU usage is at or below `--idle-cpu-percent` (default `20.0`).
- At least `--min-cooldown-sec` seconds have passed after the previous variant (default `300`).
- The state remains stable for `--stable-samples` samples (default `3`, sampled every `10s`).

Example shorter smoke run:

```shell
./scripts/run_tpcc_stable.py --measure-time 60 --cool-temp-c 60 --min-cooldown-sec 180
```

Before a formal local run, clear old TPCC data and Linux page cache so each matrix starts from a comparable state:

```shell
rm -rf target/tpcc-stable-run-data kite_sql_tpcc kite_sql_tpcc.sqlite
sync
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
```

The runner reads CPU temperatures from Linux `/sys/class/hwmon` and `/sys/class/thermal` when available. If no CPU temperature sensor is exposed, it still gates on CPU usage and the fixed cooldown window, and records the missing temperature source in the raw log.

Duplicate-key note:
The benchmark stores `history.h_date` as `timestamp(6)`, so high-throughput `Payment` transactions do not collide on second-level timestamp buckets. A duplicate-primary-key failure during TPCC should be treated as a run failure and investigated or rerun from a clean database.

- i9-13900HX
- 32.0 GB
- KIOXIA-EXCERIA PLUS G3 SSD
- Tips: TPC-C currently runs as a single worker.

## 720s comparison
Local stable-run 720-second comparison on the machine above:

| Backend | TpmC | New-Order p90 | Payment p90 | Order-Status p90 | Delivery p90 | Stock-Level p90 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| KiteSQL LMDB | 82871 | 0.001s | 0.001s | 0.001s | 0.002s | 0.001s |
| KiteSQL RocksDB | 40960 | 0.001s | 0.001s | 0.001s | 0.011s | 0.001s |
| SQLite balanced | 51637 | 0.001s | 0.001s | 0.001s | 0.001s | 0.001s |
| SQLite practical | 61424 | 0.001s | 0.001s | 0.001s | 0.001s | 0.001s |

- The KiteSQL rows are from `2026-07-11_17-20-24`; the SQLite rows are from `2026-07-11_20-25-01`.
- The stable-run gates were `temp<=65.0C`, `cpu<=20.0%`, `min_cooldown=300s`, `stable_samples=3`, and `sample_interval=10.0s`.
- All rows use `--num-ware 1`, `--max-retry 5`, and TPCC's default 720-second measure time.
- SQLite rows use the `balanced` and `practical` profiles respectively.

### KiteSQL LMDB
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  994454 |    0 |   10137 | 1004591 |
| Payment      |  994430 |    0 |       0 | 994430 |
| Order-Status |   99443 |    0 |       0 | 99443 |
| Delivery     |   99443 |    0 |       0 | 99443 |
| Stock-Level  |   99443 |    0 |       0 | 99443 |
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

0.001, 994388
0.002,     64
0.003,      1
0.004,      1

2.Payment

0.001, 994428
0.002,      2

3.Order-Status

0.001,  96207
0.002,   2704
0.003,    532

4.Delivery

0.001,  89134
0.002,  10294
0.003,     12
0.004,      2
0.005,      1

5.Stock-Level

0.001,  99443

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.003)
     Payment : 0.001  (0.001)
Order-Status : 0.001  (0.003)
    Delivery : 0.002  (0.004)
 Stock-Level : 0.001  (0.001)
<TpmC>
82871 Tpmc
```

### KiteSQL RocksDB
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  491520 |    0 |    4973 | 496493 |
| Payment      |  491493 |    0 |       0 | 491493 |
| Order-Status |   49150 |    0 |       0 | 49150 |
| Delivery     |   49150 |    0 |       0 | 49150 |
| Stock-Level  |   49149 |    0 |       0 | 49149 |
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

0.001, 491385
0.002,    131
0.003,      4

2.Payment

0.001, 491475
0.002,     15
0.003,      1

3.Order-Status

0.001,  44281
0.002,   3740
0.003,    721
0.004,    320
0.005,     80

4.Delivery

0.002,   5008
0.003,   6120
0.004,   5573
0.005,   2405
0.006,   4233
0.007,   4778
0.008,   5329
0.009,   5870
0.010,   4851
0.011,   3188
0.012,   1585
0.013,    191
0.014,      4
0.015,      2
0.016,      5
0.017,      2
0.018,      4
0.019,      1
0.020,      1

5.Stock-Level

0.001,  49149

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.003)
     Payment : 0.001  (0.007)
Order-Status : 0.001  (0.008)
    Delivery : 0.011  (0.019)
 Stock-Level : 0.001  (0.001)
<TpmC>
40960 Tpmc
```

### SQLite balanced
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  619643 |    0 |    6299 | 625942 |
| Payment      |  619618 |    0 |       0 | 619618 |
| Order-Status |   61961 |    0 |       0 | 61961 |
| Delivery     |   61962 |    0 |       0 | 61962 |
| Stock-Level  |   61962 |    0 |       0 | 61962 |
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

0.001, 619561
0.002,     82

2.Payment

0.001, 619618

3.Order-Status

0.001,  61961

4.Delivery

0.001,  61956
0.002,      6

5.Stock-Level

0.001,  61962

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.001)
     Payment : 0.001  (0.001)
Order-Status : 0.001  (0.000)
    Delivery : 0.001  (0.001)
 Stock-Level : 0.001  (0.000)
<TpmC>
51637 Tpmc
```

### SQLite practical
```shell
Transaction Summary (elapsed 720.0s)
+--------------+---------+------+---------+-------+
| Transaction  | Success | Late | Failure | Total |
+--------------+---------+------+---------+-------+
| New-Order    |  737086 |    0 |    7349 | 744435 |
| Payment      |  737064 |    0 |       0 | 737064 |
| Order-Status |   73706 |    0 |       0 | 73706 |
| Delivery     |   73706 |    0 |       0 | 73706 |
| Stock-Level  |   73706 |    0 |       0 | 73706 |
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

0.001, 737069
0.002,     17

2.Payment

0.001, 737064

3.Order-Status

0.001,  73706

4.Delivery

0.001,  73706

5.Stock-Level

0.001,  73706

<90th Percentile RT (MaxRT)>
   New-Order : 0.001  (0.001)
     Payment : 0.001  (0.001)
Order-Status : 0.001  (0.000)
    Delivery : 0.001  (0.001)
 Stock-Level : 0.001  (0.000)
<TpmC>
61424 Tpmc
```

## Refer to
- https://github.com/AgilData/tpcc
