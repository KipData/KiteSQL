# RFC 0001: 由 GreedyJoinReorder 主导 Join 决策（顺序、实现与索引选择）

- 状态：Draft
- 日期：2026-10-20
- 涉及模块：`optimizer/rule/normalization/join_reorder.rs`（`GreedyJoinReorder`、`OrientNestedLoopJoin`）、
  `optimizer/rule/normalization/parameterized_index.rs`（`ParameterizeInnerJoin`）、
  `planner/operator`（`PhysicalOption`）、`optimizer/rule/implementation`（scan cost）

## 1. 背景

### 1.1 Join 决策分散在四处

| 阶段 | 规则 | 决定什么 | 依据 |
|---|---|---|---|
| implementation | `IndexScanImplementation` 等 | 每个 scan 的访问路径 | 静态 range 代价，不知道连接键 |
| after_batches 1 | `GreedyJoinReorder` | 连接顺序、hash 的 build 方向 | 只按 hash 估算 |
| after_batches 2 | `ParameterizeInnerJoin` | 是否改写为索引 probe、用哪个索引 | 能 probe 就改写，取 `index_infos` 中第一个可用的 PK/Unique |
| after_batches 5 | `OrientNestedLoopJoin` | Cross NLJ 的内外侧 | `estimated_work` |

Greedy 处于被动位置：它拿到的是 scan 已经选好的计划，只调整 join 的位置，而且按"全部是 hash"排序；
排好之后，`ParameterizeInnerJoin` 不经代价比较就把部分 join 改成 probe，`OrientNestedLoopJoin`
再独立调整 NLJ 方向。连接顺序是按一种实现的代价定的，最终执行的却可能是另一种实现。

### 1.2 后果

- 连接顺序没有考虑 probe：一个"小外侧 ⋈ 大唯一内侧"的 pair，按 hash 估算很贵，会被推迟；
  而按 probe 它本应最先合并。
- probe 是否更优从未被比较：大外侧 ⋈ 小唯一内侧时，probe 的 seek 次数远多于 hash 的全量读取，
  但仍会被改写成 probe。
- 索引选择与连接无关：probe 用的是第一个可用索引；Greedy 的 `index_statistics` 只读 implementation
  选中的那个索引，覆盖连接键的其它索引提供的唯一性和 NDV 被忽略。
- 代价单位不统一，且 probe 单价被低估、HashJoin / NLJ 漏算部分代价（见第 4 节），
  即使让 Greedy 比较，也比较不出正确结果。

## 2. 目标与非目标

目标：

- `GreedyJoinReorder` 成为 Greedy 区域内 join 的唯一决策者：在合并每一个 pair 时，同时决定
  连接顺序、join 实现（Hash / IndexProbe / NestLoop）和方向，以及区域内每个叶子 scan 使用的索引。
- 下游规则 `ParameterizeInnerJoin` 对 Greedy 已决策的 join 只负责执行和校验，不再自行判断；
  原有逻辑仅作为未经 Greedy 处理的 join 的兜底。`OrientNestedLoopJoin` 并入 Greedy 后删除。
- 提供支撑上述决策的统一代价模型。

非目标：

- 不改变 optimizer pipeline 的 batch 顺序（Greedy 依赖 implementation 产出的 `row_count`）。
- 不支持非唯一索引（Normal / Composite）的 inner join probe，见第 8 节。
- 不建模 I/O。

## 3. 设计：Greedy 的决策权

### 3.1 职责划分

| 规则 | 现在 | 改造后 |
|---|---|---|
| scan impl rule | 选访问路径 | 只提供静态最优的默认值；Greedy 区域内的叶子以 Greedy 的决策为准 |
| `GreedyJoinReorder` | 只排顺序，按 hash 估算 | 每个 pair 决定顺序 + 实现 + 方向 + probe 索引，写入 `join_hint` |
| `ParameterizeInnerJoin` | 能 probe 就改写 | 有 hint：校验后按 hint 执行；无 hint：原逻辑兜底 |
| `OrientNestedLoopJoin` | 调整所有 Cross NLJ 方向 | 删除，职责并入 Greedy（3.10） |

Greedy 区域内每个叶子的访问路径都由 Greedy 决定，scan impl rule 的结果只是 Greedy 的输入之一：

- 叶子作为 probe 内侧：访问路径是 Greedy 选中的 probe 索引（hint 中的 `index`），
  由 `ParameterizeInnerJoin` 替换掉 scan 原来的索引与 range。不存在"probe 更优但 scan 用了别的索引"的情况。
- 叶子作为 hash / NLJ 的输入：访问路径不使用连接键，候选就是 scan impl rule 用静态 range 比较过的那些，
  其最低代价方案也就是此时的最优方案；Greedy 以它的 `work` 作为该形态的输入代价，选择 Hash 即确认采用它。
  - 例：`a JOIN b ON a.id = b.a_id WHERE b.status = 1`，b 上有 `idx(a_id)`、`idx(status)`。hash 时 b 被独立读出，
  扫描时拿不到 a 的值，`idx(a_id)` 无法限定范围，可用的只有 `status = 1` 对应的 range 或全表扫描；
  只有 probe 才能把 `a.id` 的值传给 b 的 scan，在 `idx(a_id)` 上 seek。
- 不在 Greedy 区域内的 scan，以及未经 Greedy 处理的 join 下的 scan，仍由 scan impl rule 自己决定。

Greedy 在 implementation 之后运行（依赖其产出的 `row_count`），所以 scan 节点上先有一个静态最优的默认值，
Greedy 的决策在其上生效；对同一个叶子，Greedy 的决策优先级最高。

### 3.2 决策流程

1. 收集区域内的叶子和连接边（现有逻辑）。
2. 对每个叶子生成 probe 候选并缓存（3.3）。
3. 每轮对每个候选 pair 列出可行的实现形态，用代价模型给每种形态打分，取 pair 内最优形态（3.4）。
4. 在所有 pair 中按 `JoinCandidate` 的顺序选出本轮合并的 pair。
5. 按选中的形态构造 join：确定左右（probe 内侧、NLJ 内侧放右边，hash build 放左边），写入 `join_hint`（3.5）。
6. 合并结果作为新输入参与下一轮，其 `InputCost` 由所选形态的代价函数给出。

### 3.3 Source 与 probe 候选

Greedy 根据 source 的列信息与其上存在的索引，自己生成候选，不依赖 impl rule 保留候选：

```rust
struct ProbeCandidate {
    index: IndexMetaRef,
    key_columns: Vec<ColumnRef>, // 必须由连接键提供的索引列（已扣除常量等值列），非空
    lookup_work: usize,          // 单次 probe 代价，见 4.3
    rows_per_lookup: usize,      // 唯一索引 = 1，为非唯一索引预留
}
```

- 适用的叶子：`Filter*` + 没有 limit 的 `TableScan`。已合并的 join 结果不能作为 probe 内侧，
  所以 probe 只出现在"已合并结果 ⋈ 叶子"的形态，与 Greedy 左深扩展的方式吻合。
- 索引的每一列要么出现在某条连接边上（属于该叶子一侧），要么被叶子 filter 的常量等值绑定；
  两者都满足时按常量处理，减少对连接键的要求。
- 每个叶子最多 10 个候选，按 PK → Unique → `key_columns` 少的优先 → index id 排序后截断。
- 同一组覆盖连接键的索引也用于行数估算：任意被覆盖的 PK/Unique 即认为该侧唯一；
  NDV 通过 `loader.load(table, index_id)` 读取该索引的 `distinct_values_len`（复合键得到联合 NDV），
  再按 filter 后行数以 `min(N, rows)` 收紧。不再只看 implementation 选中的索引。

### 3.4 Pair 内的实现形态

对一个 pair (L, R)，可行形态如下：

| 形态 | 条件 |
|---|---|
| Hash(build = L) / Hash(build = R) | pair 有连接键 |
| Probe(inner = R) / Probe(inner = L) | 对应一侧是叶子，且某个候选的 `key_columns` 被该 pair 在这一侧的连接键覆盖 |
| NestLoop(outer = L) / NestLoop(outer = R) | pair 没有连接键（cross） |

每种形态按第 4 节的代价模型打分，pair 内取最低分。`JoinCandidate` 的比较顺序（`derive(Ord)`）：

```
cross_join → score → impl_rank → rows → left → right → swapped
```

- `cross_join` 按 pair 判断（该 pair 是否有连接键），不再用全局的 `connected`。
- `impl_rank`：probe < hash < NLJ，只在分数相同或无统计时起作用。

### 3.5 决策下发：`join_hint`

```rust
enum JoinHint {
    Hash,
    Probe { index: IndexMetaRef, lookup_work: usize },
    NestLoop,
}

// PhysicalOption 新增，参与 PartialEq / Hash（会影响后续改写结果）
join_hint: Option<JoinHint>,
```

Hint 写在 Greedy 构造 join 处（现在设置 `PhysicalOption(HashJoin/NestLoopJoin, ...)` 的位置）。
左右已经由 Greedy 定好，hint 只表达"按这个形态执行，不要再判断"。

### 3.6 下游规则的执行与兜底

- `ParameterizeInnerJoin`
  - `Probe`：用共用判定函数（3.7）再校验一次，通过则按指定索引改写，并把 `lookup_work` 写入
    MarkApply 的 `PhysicalOption`；不尝试交换左右；校验失败按 hash 处理。
  - `Hash` / `NestLoop`：不改写。
  - `None`：Greedy 没处理过的 join（ON 带 filter、`force_nested_loop`、`limit_pushed` 等），走原逻辑兜底。
- NestLoop 方向由 Greedy 决定，下游不再有调整方向的规则（3.10）。
- 被替换的静态 range 不会丢失过滤语义：Filter 仍在 scan 上方，`EliminateIndexFilter` 在 Parameterize 之后运行。
- explain 只展示选中的索引（现有行为），不展示 hint 与 `lookup_work`。

### 3.7 共用判定函数

从 `parameterize` 中拆出纯判定函数（不修改 plan），Greedy 生成候选与 `ParameterizeInnerJoin` 校验都调用它：

- 索引列都在 scan 的 `columns` 中；
- 连接值表达式类型与索引列类型完全一致（`value.return_type == column.datatype()`）；
- 至少用到一个连接键（`uses_join_key`），只靠常量凑满索引不算 probe。

Greedy 拥有决策权的前提是它的判断与执行一致；若两边各写一份，可能出现 Greedy 按 probe 定了顺序、
执行时却退回 hash，结果比不考虑 probe 更差。

### 3.8 索引覆盖：probe 与 scan 用同一口径

现状：覆盖信息（`covered_deserializers` / `cover_mapping`）只在 `PushPredicateIntoScan` 中计算，
且只针对能从 filter 拆出静态 range 的索引。只被连接键用到的索引覆盖信息始终为 `None`；
`parameterize` 改写为 probe 时只设置 `lookup = Probe`、`residual_predicate = None`，不重算覆盖。
执行器（`index_scan.rs`）会按覆盖信息决定是否回表，因此一个实际覆盖了 scan 所需列的二级 Unique 索引，
probe 时仍会回表，代价按 `seeks = 2` 计。结果是 probe 与 scan 选中的覆盖索引比较时被系统性高估。

修正：

1. 把覆盖计算从 `PushPredicateIntoScan` 拆成纯函数
   `index_covering(scan, index) -> Option<(deserializers, mapping)>`，只依赖 scan 的 `columns` 与索引列，与 range 无关。
2. Greedy 生成候选时用它决定 `seeks`（PK 或覆盖 Unique = 1，否则 = 2）；scan cost 中非覆盖 ×2 的判断使用同一函数。
3. `ParameterizeInnerJoin` 按 hint（以及兜底路径）改写为 probe 时，同时写入 `covered_deserializers` / `cover_mapping`，
   使执行器真正跳过回表。

这样 Hash 形态用 scan 已选路径（可能是覆盖索引）的 `work`，Probe 形态枚举 `scan.index_infos` 中全部候选并按真实覆盖计价，
"scan 选了覆盖索引，但 probe 更优"的情况在 pair 内比较时直接体现，由 Greedy 选出后覆盖 scan 的选择。

### 3.9 无统计信息时的回退

一个 pair 只要有一侧缺少统计信息（行数，或 probe 所需的 N），该 pair 不做代价比较，
按 `impl_rank` 启发式选择 probe > hash > NLJ，与现有 `ParameterizeInnerJoin` 的行为一致。
NestLoop 方向在缺少统计时沿用 `OrientNestedLoopJoin` 现有的回退：一侧行数为 0 / 1 时它作外侧，否则比较 `startup_work`。

### 3.10 融合 `OrientNestedLoopJoin`

`OrientNestedLoopJoin` 只匹配 `Cross` + `on = None` + `limit_pushed = false` + `NestLoopJoin`，
用 `left_work + left_rows × right_work` 与对侧比较决定方向，与 `JoinImpl::NestLoop` 的公式本质相同。
它处理三类 join，全部并入 Greedy 后删除该规则及 `Orient Nested Loop Join` batch：

| 来源 | 现在 | 融合后 |
|---|---|---|
| Greedy 区域内无连接键的 pair | Greedy 构造时 `swapped: connected && ...`，cross 永不交换，方向靠 Orient 修正 | Greedy 比较 `NestLoop(outer = L / R)`，写入 `NestLoop` hint |
| 根为 `Cross` 的区域（如 `FROM a, b`，或位于 outer join / 带 filter 的 inner join 之下） | Greedy 要求根为 `Inner`（`apply` 入口），不处理 | Greedy 触发条件放宽为 `can_expand(plan)`；`can_expand` 本就接受 Cross |
| `force_nested_loop` 的 cross（`FORCE_NEST_LOOP_JOIN`） | `can_expand` 排除，不进区域；Orient 不检查该字段，仍调整方向 | 仍不进区域、不重排；Greedy 对这种单个 join 只做方向选择，行为不变 |

`force_nested_loop` 的语义是限制实现，不放开重排，所以只接管方向。

时序代价：Orient 原本在 `Parameterize Mark Apply`、`Eliminate Index Filter`、`Materialize Mark Apply` 之后运行，
看到的是改写后的 `estimated_work`。并入 Greedy 后，NLJ 内侧子树若含子查询类 MarkApply，方向按其改写前的估算决定；
物化正好降低重跑内侧的代价。该偏差先接受（Greedy 中 cross 本就排在最后，此类情况少见），
与 `rescan_work` 一并作为未来工作。改写对这类 MarkApply `estimated_work` 的实际影响尚未核实。

## 4. 支撑：代价模型

Greedy 的决策只有在代价可比时才有意义，本节是决策权改造的前提。

### 4.1 现状

| 位置 | 单位 | 是否考虑行宽 |
|---|---|---|
| scan impl rule 的 `cost` | 行数（非覆盖索引 ×2，排序 hint 有折扣） | 否 |
| `calculate_work` / `calculate_startup_work` | CPU 单位：`CPU_ROW_WORK = 4`，`CPU_OPERATOR_WORK = 1` | 否 |
| `GreedyJoinReorder` | 值个数（行数 × 列数） | 是 |

- MarkApply 每次 lookup 只算 `startup_work(scan) + OP = 1`，比顺序扫一行（4）还便宜。
- HashJoin 走默认分支，漏算 probe 侧的全量读取。
- NLJ 只有 Cross 单独处理且没计逐对比较；非 Cross 的 NLJ 完全没计"每个外侧行重跑内侧"。

### 4.2 统一的行代价与 join 代价函数

```rust
// width 为列数；每行有固定开销，零列行不会被估成 0
fn rows_work(rows: usize, width: usize) -> usize {
    rows * (CPU_ROW_WORK + width * CPU_OPERATOR_WORK)
}

enum JoinImpl { Hash, NestLoop { full: bool }, IndexProbe { lookup_work: usize } }
struct InputCost { rows: usize, width: usize, work: usize }
impl JoinImpl {
    fn work(self, left: &InputCost, right: &InputCost, out_rows: usize, out_width: usize) -> usize;
}
```

```
Hash       = build.work                           // build 侧完整读一遍
           + probe.work                           // probe 侧完整读一遍
           + rows_work(build)                     // build 侧整行存入哈希表
           + probe.rows × OP                      // probe 侧每行查一次哈希表
           + rows_work(out)                       // 输出满足条件的行
IndexProbe = outer.work                           // 外侧完整读一遍
           + outer.rows × lookup_work             // 每个外侧行，去内侧索引 seek 一次；内侧不全量读
           + rows_work(out)                       // 输出满足条件的行
NestLoop   = outer.work                           // 外侧完整读一遍
           + outer.rows × (inner.work             // 每个外侧行，内侧从头重跑一遍
                           + inner.rows × OP)     // 每个外侧行，和内侧每一行比较一次
           + rows_work(out)                       // 输出满足条件的行
           (+ inner.work，仅 Full join：最后再扫一遍内侧输出未匹配行)
```

- 代价函数只接收统计量，不依赖 `&LogicalPlan`，因为 Greedy 评估的是尚未构造的 join。
  Greedy 与 `calculate_work` 共用它。
- `calculate_work`、`calculate_startup_work` 改用 `rows_work`；
  scan impl rule 不变（同一 scan 的候选列数相同，换单位不影响排序）。
- `rescan_work` 暂不引入，视为等于 `work`（例外见 6.2）。

### 4.3 单次 probe 代价 `lookup_work`

Hash 的 probe 是内存哈希查找（约 `OP`），前提是两侧全量读；MarkApply 的 probe 是存储层 seek，
每次都要重建内侧 executor，单价高，好处是内侧不全量读。

```
lookup_work = PROBE_RESET_WORK(= CPU_ROW_WORK)
            + seeks × ⌈log2(N)⌉ × OP          // seeks: PK/覆盖 Unique = 1，非覆盖 Unique = 2
            + rows_per_lookup × rows_work(1, inner_width)
```

- N 取该索引直方图的 `values_len`，缺失时取 PK 直方图；都没有视为无统计。内侧 Filter 只按行计。
- 示例：内侧 100 万行、宽度 2，扫一行 = 6，查一次 = 30；外侧约小于内侧 1/4 时 probe 更优。
- 存于 `PhysicalOption.lookup_work: Option<usize>`，由 Greedy 经 hint 传入，兜底路径由
  `ParameterizeInnerJoin` 用 loader 计算。semi / anti 等子查询类 MarkApply 也改用该公式。

### 4.4 Pair 分数：本步处理量

```
score(Hash / NestLoop) = work(join) − left.work − right.work
score(Probe)           = outer.rows × lookup_work + rows_work(out) − inner_leaf.work
```

- 分数只计这一步的中间处理量。全 hash 时每张叶子无论顺序都读一遍，计入只会让大表被推迟。
  现有 Greedy 公式（`product × (left_width + right_width) + min(left_size, right_size)`）已是这个思路。
- Probe 的 `− inner_leaf.work` 是对基准（每张叶子都完整读一遍）的修正，不是处理量；
  内侧大、外侧小时分数为负，Greedy 会优先合并。分数用 `i128`，代码中需注释该项含义。

## 5. Pipeline 与 hint 的有效性

`db.rs` 中 after_batches 的顺序：

1. `Greedy Join Reorder`（once_topdown）
2. `Parameterize Mark Apply`：`ParameterizeMarkApply` + `ParameterizeInnerJoin`（fix_point_topdown(1)）
3. `Eliminate Index Filter`
4. `Materialize Mark Apply`
5. `Orient Nested Loop Join`（本 RFC 删除）
6. ……

Greedy 与 Parameterize 之间没有其它 batch；同一 batch 的 `ParameterizeMarkApply` 只匹配 `MarkApply`，
其 `parameterize_right_subtree` 只处理 `Project` 和 `TableScan`，不修改 Join 节点，hint 不会被破坏。
改写出的 MarkApply 的 `estimated_work` 仍须使用 Greedy 传入的 `lookup_work`，使 `calculate_work`
（含 explain 中的代价）与 Greedy 的决策口径一致。

## 6. 已核实的执行器行为

### 6.1 MarkApply inner probe（`execution/dql/mark_apply.rs`）

- `predicates` 包含全部 `on` 等值条件（常量等值在 `parameterize` 返回前被 `truncate`）；`next_join_tuple`
  取出内侧行后用 `predicates_matched` 校验全部条件，连接键多于 `key_columns` 时多出的条件也会被校验。
- 原 join 的 `filter` 包成 `Filter` 放在 MarkApply 之上；被 probe 一侧原有的 Filter 保留。
- 每个外侧行先 reset 内侧，最多取 1 行，所以本 RFC 只支持 PK/Unique probe。

### 6.2 NestLoopJoin 与 `reset`

NLJ 不物化内侧：每取一个外侧行就 `reset_right_input()` 并把内侧扫到底；Full join 最后再扫一遍内侧。
`SeqScan` / `IndexScan` 的 `reset` 回到存储层重新定位。有内部状态的算子 `reset` 时清空状态并递归 reset 输入：

| 算子 | `reset` 行为 |
|---|---|
| HashJoin | `state = Build`，`bump.reset()`，reset 两侧 |
| Sort / ExternalSort | 清空 `rows`，reset 输入 |
| TopK | `output = None`，`arena.reset()`，reset 输入 |
| HashAgg / SimpleAgg | 清空输出状态，reset 输入 |
| Window | `state.clear()`，`reset_functions()`，reset 输入 |
| NestLoopJoin | 回到 `PullLeft`，reset 两侧 |
| MarkApply | 只 reset 左侧；`materialize` 且已有缓存时保留右侧缓存 |

因此 `rescan_work = work` 是准确的，唯一例外是开启 `materialize` 的 MarkApply。

### 6.3 排序 hint 不影响 Greedy 叶子

scan cost 中的排序折扣来自 `sort_elimination_hint` / `stream_aggregate_hint`，需确认 Greedy 把叶子当作
hash 输入或改写为 probe 时，不会丢失上层依赖的有序性、也不会用到打过折的 `work`：

- hint 在 implementation 阶段由 `annotate_hints_and_physical_options`（`heuristic/optimizer.rs`）自上而下下发，
  只穿过 `Filter` / `Project` / `Limit` / `TopK` / `Sort`；遇到 Join 等其它算子即换成空 hint。
  所以 Join 之下的 scan（即 Greedy 的叶子）`hint_sum = 0`，没有排序折扣。
- 消费有序性的 `EliminateRedundantSort` / `UseStreamAggregate` 在同一阶段的 `apply_annotated_post_rules` 中运行，
  早于 Greedy；其 `ensure_order` 只沿 `Childrens::Only` 下钻，不穿过 Join。Join 的 `PhysicalOption`
  本身是 `SortOption::None`。
- 叶子与 Join 之间若有 Sort / Aggregate，该子树不是 `Filter* + TableScan`，不会作为 probe 内侧，
  其有序性只在子树内部被消费。

结论：Greedy 改变叶子的访问路径不会破坏任何上层有序性依赖，Hash 形态使用的 `work` 也不含排序折扣。

## 7. 实施计划

代价模型是 Greedy 决策的前提，所以先统一代价，再移交决策权，分两步提交以便定位回归。

1. 统一代价（只改行为）：引入 `rows_work`、`JoinImpl` / `InputCost`，修正 HashJoin、NLJ、MarkApply 的估算，
   Greedy 分数改为本步处理量。现有连接顺序与 explain / slt 期望值会变化，需整体重新校准。
2. 移交决策权：拆出共用判定函数与 `index_covering`；Greedy 生成 probe 候选、按形态比较并写入 `join_hint`；
   `ParameterizeInnerJoin` 改为有 hint 时只执行；Greedy 触发条件放宽到 `can_expand`、接管 NestLoop 方向
   （含 `force_nested_loop` 的单个 cross），删除 `OrientNestedLoopJoin` 及其 batch。

测试（单测走 `make test`，slt 走 `make test-slt`）：

- `rows_work` 与各 `JoinImpl::work`：Full join、零列行、饱和运算。
- Greedy：大外侧 × 小唯一内侧选 hash；小外侧 × 大唯一内侧选 probe 且优先合并；临界点；
  多个可用唯一索引时选代价最低的；复合唯一索引 + 常量等值；无统计回退启发式。
- 下游规则：hint 为 `Probe` / `Hash` / `NestLoop` / `None` 四种路径；`Probe` 校验失败退回 hash。
- NestLoop 方向：Greedy 区域内的 cross pair、根为 Cross 的区域、`force_nested_loop` 的 cross 三类，
  方向均与原 `OrientNestedLoopJoin` 的现有用例结论一致（迁移其单测），`force_nested_loop` 下不发生重排。
- 覆盖：scan 选了覆盖索引、另一个覆盖的 Unique 可 probe 时，probe 按 `seeks = 1` 计价并被选中，
  改写后的 IndexScan 带 `covered_deserializers`，执行时不回表。
- 正确性：probe 后全部 `on` 条件、join filter、静态 filter 均保留（slt 覆盖）。

## 8. 未来工作

- 开启 `materialize` 的 MarkApply 的 `rescan_work`；以及 NLJ 内侧含子查询类 MarkApply 时，
  Greedy 按其参数化 / 物化前的估算决定方向的偏差（3.10）。
- 非唯一索引 probe：执行器需支持每个外侧行输出多行，且每行 reset 内侧开销较大，按具体 case 评估；
  届时 `rows_per_lookup` 由索引 NDV 估算，10 个候选上限才真正起作用。
- I/O 建模。
