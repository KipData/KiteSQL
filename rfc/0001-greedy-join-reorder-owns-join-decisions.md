# RFC 0001: GreedyJoinReorder Owns Join Decisions (Order, Implementation, and Index Choice)

- Status: Draft
- Date: 2026-10-20
- Modules: `optimizer/rule/normalization/join_reorder.rs` (`GreedyJoinReorder`, `OrientNestedLoopJoin`),
  `optimizer/rule/normalization/parameterized_index.rs` (`ParameterizeInnerJoin`),
  `planner/operator` (`PhysicalOption`), `optimizer/rule/implementation` (scan cost)

## 1. Background

### 1.1 Join decisions are spread across four places

| Stage | Rule | Decides | Based on |
|---|---|---|---|
| implementation | `IndexScanImplementation`, etc. | Access path of each scan | Static range cost; unaware of join keys |
| after_batches 1 | `GreedyJoinReorder` | Join order and hash build side | Hash estimates only |
| after_batches 2 | `ParameterizeInnerJoin` | Whether to rewrite to an index probe, and which index | Rewrites whenever a probe is possible, using the first usable PK/Unique in `index_infos` |
| after_batches 5 | `OrientNestedLoopJoin` | Outer/inner side of a Cross NLJ | `estimated_work` |

Greedy is passive: it receives a plan whose scans are already chosen, only moves joins around, and orders them
as if every join were a hash join. Afterwards, `ParameterizeInnerJoin` turns some joins into probes without any
cost comparison, and `OrientNestedLoopJoin` adjusts NLJ direction independently. The join order is chosen for
one implementation, while the one that finally executes may be another.

### 1.2 Consequences

- Join order ignores probes: a "small outer ⋈ large unique inner" pair looks expensive as a hash join and is
  postponed, although as a probe it should be merged first.
- Whether a probe is better is never compared: for a large outer ⋈ small unique inner, the probe performs far
  more seeks than a full hash read, yet it is still rewritten into a probe.
- Index choice is independent of the join: the probe uses the first usable index, and Greedy's
  `index_statistics` only reads the index chosen during implementation, ignoring the uniqueness and NDV that
  other indexes bound by the join keys would provide.
- Cost units are inconsistent, the per-probe cost is underestimated, and HashJoin / NLJ miss part of their cost
  (see Section 4), so even if Greedy compared the options it could not get the right answer.

## 2. Goals and Non-goals

Goals:

- `GreedyJoinReorder` becomes the only decision maker for joins inside a Greedy region: when merging each pair,
  it decides at once the join order, the join implementation (Hash / IndexProbe / NestLoop) and direction, and
  the index used by each leaf scan in the region.
- The downstream rule `ParameterizeInnerJoin` only executes and validates joins already decided by Greedy and no
  longer makes its own choice; its original logic remains only as a fallback for joins Greedy did not handle.
  `OrientNestedLoopJoin` is merged into Greedy and removed.
- Provide a unified cost model that supports these decisions.

Non-goals:

- Do not change the batch order of the optimizer pipeline (Greedy relies on the `row_count` produced by
  implementation).
- Do not support inner join probes on non-unique indexes (Normal / Composite); see Section 8.
- Do not model I/O.

## 3. Design: Greedy Owns the Decisions

### 3.1 Responsibilities

| Rule | Today | After |
|---|---|---|
| scan impl rule | Chooses the access path | Only provides a statically optimal default; leaves inside a Greedy region follow Greedy's decision |
| `GreedyJoinReorder` | Orders only, using hash estimates | Decides order + implementation + direction + probe index for each pair; rewrites probes directly and marks the rest `join_decided` |
| `ParameterizeInnerJoin` | Rewrites whenever it can probe | `join_decided`: no rewrite; otherwise the original logic is the fallback |
| `OrientNestedLoopJoin` | Orients every Cross NLJ | Removed; its responsibility moves into Greedy (3.10) |

The access path of every leaf inside a Greedy region is decided by Greedy; the scan impl rule's result is only
one of Greedy's inputs:

- Leaf as a probe inner side: the access path is the probe index chosen by Greedy, which replaces the scan's
  original index and range during the rewrite. There is no case where "the probe is better but the scan uses
  another index".
- Leaf as a hash / NLJ input: the access path does not use the join keys, so the candidates are exactly those
  the scan impl rule already compared with static ranges, and its cheapest plan is also the best one here.
  Greedy uses its `work` as the input cost of this shape; choosing Hash confirms it.
  - Example: `a JOIN b ON a.id = b.a_id WHERE b.status = 1`, where b has `idx(a_id)` and `idx(status)`. Under
    hash, b is read independently and its scan cannot see a's values, so `idx(a_id)` cannot restrict the range;
    only the range for `status = 1` or a full scan is usable. Only a probe can pass `a.id` to b's scan and seek
    on `idx(a_id)`.
- Scans outside a Greedy region, and scans under joins Greedy did not handle, are still decided by the scan impl
  rule.

Greedy runs after implementation (it relies on the `row_count` produced there), so each scan node already has a
statically optimal default and Greedy's decision is applied on top of it; for the same leaf, Greedy's decision has
the highest priority.

### 3.2 Decision flow

1. Collect the leaves and join edges in the region (existing logic).
2. For each pair, use the shared predicate function to find probe-able indexes on the leaf side (3.3).
3. The search is left-deep: start from the input with the fewest rows (ties broken by the lowest read cost).
   Each round only evaluates "current result ⋈ one remaining input", lists the feasible shapes, scores them,
   and keeps the best shape within the pair (3.4). Remaining inputs are always leaves before they are merged,
   so they keep the chance to be probed.
4. Among these pairs, pick the input to merge this round by `JoinCandidate` order; when the current result is
   estimated at no more than 1 row, cross joins are not deprioritized.
5. Build the join with the chosen shape: decide left and right (the probe inner side and NLJ inner side go to the
   right, the hash build side to the left); rewrite probes directly into MarkApply and mark the rest
   `join_decided` (3.5).
6. The merged result becomes a new input for the next round; its `InputCost` comes from the cost function of the
   chosen shape.

### 3.3 Sources and probe candidates

Greedy decides the usable probe indexes itself from the source's column information and the indexes on it,
without relying on the impl rule to keep candidates:

```rust
struct ProbeIndex {
    position: usize,                // position in scan.index_infos
    probe: Vec<ExprRef>,            // outer values and constants, in index column order
    lookup_work: usize,             // cost of one probe, see 4.3
}
```

- Eligible leaves: `Filter*` + a `TableScan` without a limit. A merged join result cannot be a probe inner side,
  so probes only appear in the "merged result ⋈ leaf" shape, which matches how Greedy extends left-deep.
- Every index column either appears on some join edge (on this leaf's side) or is bound to a constant by an
  equality in the leaf's filter; when both hold, it is treated as a constant to reduce the join-key requirement.
- Each pair calls the predicate function directly and takes the PK/Unique with the lowest `lookup_work` (ties
  broken by index order); nothing is cached or truncated, and the number of candidates is bounded by the indexes
  on the table.
- The output rows of a probe are tightened by uniqueness to `min(estimated rows, outer rows)`: the probed index is
  bound by the join keys, so its uniqueness is known.

### 3.4 Implementation shapes within a pair

For a pair (L, R), the feasible shapes are:

| Shape | Condition |
|---|---|
| Hash(build = L) / Hash(build = R) | The pair has join keys |
| Probe(inner = R) / Probe(inner = L) | That side is a leaf, and the predicate function finds an index using the pair's join keys |
| NestLoop(outer = L) / NestLoop(outer = R) | The pair has no join keys (cross) |

Each shape is scored with the cost model in Section 4, and the lowest score within the pair wins. `JoinCandidate`
compares in this order (`derive(Ord)`):

```
cross_join → score → rows → left → right → swapped → probe
```

- When scores tie within a pair, the probe wins; without statistics, the default row count is used (see 3.9).

### 3.5 Passing the decision down: `join_decided`

```rust
// New in PhysicalOption; like startup_work, it is an in-pass annotation that does not take part in
// PartialEq / Hash and is not serialized
join_decided: bool,
```

When Greedy chooses a probe, it directly calls the rewrite function shared with `ParameterizeInnerJoin` to build
the MarkApply, so the decision and the execution use the same code and need no further validation. Hash /
NestLoop joins are marked `join_decided`, meaning "execute in this shape and do not decide again".

### 3.6 Execution and fallback in downstream rules

- `ParameterizeInnerJoin`
  - `join_decided`: no rewrite.
  - Otherwise: for joins Greedy did not handle (ON with a filter, `limit_pushed`, etc.), the original logic is the
    fallback; it chooses the index with the same predicate function and computes `lookup_work` with the loader.
- NestLoop direction is decided by Greedy; no downstream rule adjusts direction any more (3.10).
- The replaced static range does not lose its filtering semantics: the Filter stays above the scan, and
  `EliminateIndexFilter` runs after Parameterize.
- explain only shows the chosen index (existing behavior), not `join_decided` or `lookup_work`.

### 3.7 Shared predicate function

A pure predicate function (which does not modify the plan) is extracted from `parameterize`; both Greedy's
candidate generation and `ParameterizeInnerJoin`'s validation call it:

- Every index column is in the scan's `columns`;
- The join value expression type exactly matches the index column type (`value.return_type == column.datatype()`);
- At least one join key is used (`uses_join_key`); filling the index with constants alone is not a probe.

Greedy can only own the decision if its judgment matches execution. If each side kept its own copy, Greedy could
order joins for a probe while execution falls back to hash, which is worse than ignoring probes altogether.

### 3.8 Index coverage: probes and scans use the same rule

Today: coverage information (`covered_deserializers` / `cover_mapping`) is only computed in
`PushPredicateIntoScan`, and only for indexes that a static range can be extracted from the filter. Coverage for an
index used only by join keys is always `None`; when `parameterize` rewrites to a probe it only sets
`lookup = Probe` and `residual_predicate = None`, without recomputing coverage. The executor (`index_scan.rs`)
decides whether to look up the table by coverage, so a secondary Unique index that actually covers the columns the
scan needs still looks up the table during a probe, and its cost is counted with `seeks = 2`. As a result, probes are
systematically overestimated when compared with a covering index chosen by the scan.

Fix:

1. Extract the coverage computation from `PushPredicateIntoScan` into a pure function
   `index_covering(scan, index) -> Option<(deserializers, mapping)>`, which only depends on the scan's `columns`
   and the index columns, not on the range.
2. Greedy uses it to decide `seeks` when generating candidates (PK or covering Unique = 1, otherwise = 2); the
   non-covering ×2 rule in scan cost uses the same function.
3. The probe rewrite (shared by Greedy and the fallback path) also writes `covered_deserializers` /
   `cover_mapping`, so the executor really skips the table lookup.

This way, the Hash shape uses the `work` of the path the scan already chose (possibly a covering index), and the
Probe shape enumerates every candidate in `scan.index_infos` and prices it by its real coverage. The case "the scan
chose a covering index, but a probe is better" shows up directly in the in-pair comparison, and Greedy's choice
overrides the scan's.

### 3.9 Default row count without statistics

An input without statistics is priced at `DEFAULT_TABLE_ROWS` (1000) rows, and the N needed by a probe also falls
back to this value when missing; cost comparison proceeds as usual, with no heuristic branch. A static range on a
PK/Unique equality has an exact row count even without statistics, and the scan impl rule chooses it and reports
that row count. A join merged from default row counts is still recorded as having unknown rows, so the made-up
estimate does not leak to later rules.

### 3.10 Merging `OrientNestedLoopJoin`

`OrientNestedLoopJoin` only matches `Cross` + `on = None` + `limit_pushed = false` + `NestLoopJoin`, and decides the
direction by comparing `left_work + left_rows × right_work` with the opposite side, which is essentially the same
formula as `JoinImpl::NestLoop`. It handles three kinds of joins; all of them move into Greedy, and the rule and its
`Orient Nested Loop Join` batch are removed:

| Source | Today | After |
|---|---|---|
| A pair without join keys inside a Greedy region | Greedy builds it with `swapped: connected && ...`, so cross joins are never swapped and Orient fixes the direction | Greedy compares `NestLoop(outer = L / R)` |
| A region rooted at `Cross` (e.g. `FROM a, b`, or under an outer join / an inner join with a filter) | Greedy requires an `Inner` root (`apply` entry) and skips it | Greedy's trigger is relaxed to `can_expand(plan)`; `can_expand` already accepts Cross |
| Joins under `FORCE_NEST_LOOP_JOIN` | Excluded by `can_expand`, so not in a region; Orient only orients the cross joins among them | They enter a region (only with joins carrying the same hint) and are reordered as usual, but each pair only compares NestLoop |

The hint means "use NLJ, not hash"; it does not promise execution in the written order.

Ordering cost: Orient used to run after `Parameterize Mark Apply`, `Eliminate Index Filter`, and
`Materialize Mark Apply`, so it saw the rewritten `estimated_work`. After moving into Greedy, if an NLJ inner subtree
contains a subquery-style MarkApply, the direction is decided by its pre-rewrite estimate, while materialization
actually lowers the cost of re-running the inner side. This deviation is accepted for now (cross joins are already
ordered last in Greedy, so such cases are rare) and is left as future work together with `rescan_work`. The actual
effect of the rewrite on the `estimated_work` of such MarkApply nodes has not been verified yet.

## 4. Foundation: Cost Model

Greedy's decisions only make sense when costs are comparable; this section is the prerequisite for moving the
decisions to Greedy.

### 4.1 Today

| Location | Unit | Accounts for row width |
|---|---|---|
| scan impl rule `cost` | Rows (non-covering index ×2, discount for sort hints) | No |
| `calculate_work` / `calculate_startup_work` | CPU units: `CPU_ROW_WORK = 4`, `CPU_OPERATOR_WORK = 1` | No |
| `GreedyJoinReorder` | Number of values (rows × columns) | Yes |

- Each MarkApply lookup only counts `startup_work(scan) + OP = 1`, cheaper than scanning one row sequentially (4).
- HashJoin takes the default branch and misses the full read of the probe side.
- Among NLJs only Cross is handled separately, and it does not count per-pair comparisons; non-Cross NLJs do not
  count "re-run the inner side for each outer row" at all.

### 4.2 Unified row cost and join cost functions

```rust
// width is the number of columns; each row has a fixed overhead, so zero-column rows are not estimated as 0
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
Hash       = build.work                           // read the build side once
           + probe.work                           // read the probe side once
           + rows_work(build) + build.rows × HASH_BUILD_WORK  // store whole build rows in the hash table; hashing, allocation, and freeing calibrated on LMDB
           + probe.rows × OP                      // one hash table lookup per probe row
           + rows_work(out)                       // emit matching rows
IndexProbe = outer.work                           // read the outer side once
           + outer.rows × lookup_work             // one inner index seek per outer row; the inner side is not fully read
           + rows_work(out)                       // emit matching rows
NestLoop   = outer.work                           // read the outer side once
           + outer.rows × (inner.work             // re-run the inner side for each outer row
                           + inner.rows × OP)     // compare each outer row with every inner row
           + rows_work(out)                       // emit matching rows
           (+ inner.work, Full join only: scan the inner side once more for unmatched rows)
```

- The cost functions only take statistics and do not depend on `&LogicalPlan`, because Greedy evaluates joins that
  have not been built yet. Greedy and `calculate_work` share them.
- `calculate_work` and `calculate_startup_work` switch to `rows_work`; the scan impl rule is unchanged (candidates of
  the same scan have the same number of columns, so changing the unit does not change their order).
- `rescan_work` is not introduced yet and is treated as equal to `work` (exception in 6.2).

### 4.3 Cost of one probe: `lookup_work`

A Hash probe is an in-memory hash lookup (about `OP`), but requires reading both sides fully; a MarkApply probe is a
storage-level seek that rebuilds the inner executor every time, so it costs more per probe, but the inner side is not
read fully.

```
lookup_work = PROBE_RESET_WORK(= CPU_ROW_WORK)
            + seeks × ⌈log2(N)⌉ × OP          // seeks: PK/covering Unique = 1, non-covering Unique = 2
            + rows_per_lookup × rows_work(1, inner_width)
```

- N is the `values_len` of the index histogram, or `DEFAULT_TABLE_ROWS` when absent. An inner Filter is only counted
  per row.
- Example: with 1M inner rows of width 2, scanning one row = 6 and one lookup = 30; the probe wins when the outer
  side is smaller than roughly 1/4 of the inner side.
- Stored in `PhysicalOption.lookup_work: Option<usize>`, computed by the predicate function with the loader and
  written during the probe rewrite (shared by Greedy and the fallback path). Subquery-style MarkApply such as semi /
  anti also switch to this formula.

### 4.4 Pair score: work done in this step

```
score(Hash / NestLoop) = work(join) − left.work − right.work
score(Probe)           = outer.rows × lookup_work + rows_work(out) − inner_leaf.work
```

- The score only counts the intermediate work of this step. With all-hash joins each leaf is read once regardless of
  order, so counting the reads would only postpone large tables. The existing Greedy formula
  (`product × (left_width + right_width) + min(left_size, right_size)`) already follows this idea.
- The `− inner_leaf.work` term of Probe is a correction against the baseline (every leaf is read once), not work
  done; when the inner side is large and the outer side small, the score is negative and Greedy merges it first.
  Scores use `i128`, and the code must comment on the meaning of this term.

## 5. Pipeline and the Validity of `join_decided`

Order of after_batches in `db.rs`:

1. `Greedy Join Reorder` (once_topdown)
2. `Parameterize Mark Apply`: `ParameterizeMarkApply` + `ParameterizeInnerJoin` (fix_point_topdown(1))
3. `Eliminate Index Filter`
4. `Materialize Mark Apply`
5. `Orient Nested Loop Join` (removed by this RFC)
6. ...

There is no other batch between Greedy and Parameterize. In the same batch, `ParameterizeMarkApply` only matches
`MarkApply`, and its `parameterize_right_subtree` only handles `Project` and `TableScan` without touching Join nodes,
so the mark is not destroyed. The `estimated_work` of the rewritten MarkApply must still use the `lookup_work`
written by Greedy, so that `calculate_work` (including costs shown in explain) is consistent with Greedy's decision.

## 6. Verified Executor Behavior

### 6.1 MarkApply inner probe (`execution/dql/mark_apply.rs`)

- `predicates` contains every `on` equality (constant equalities are `truncate`d before `parameterize` returns);
  after `next_join_tuple` fetches an inner row it checks all conditions with `predicates_matched`, so extra
  conditions are also checked when there are more join keys than `key_columns`.
- The original join's `filter` is wrapped into a `Filter` above the MarkApply; the existing Filter on the probed side
  is kept.
- The inner side is reset for each outer row and at most 1 row is taken, so this RFC only supports PK/Unique probes.

### 6.2 NestLoopJoin and `reset`

NLJ does not materialize its inner side: for each outer row it calls `reset_right_input()` and scans the inner side
to the end; a Full join scans the inner side once more at the end. `reset` on `SeqScan` / `IndexScan` repositions in
the storage layer. Stateful operators clear their state on `reset` and reset their inputs recursively:

| Operator | `reset` behavior |
|---|---|
| HashJoin | `state = Build`, `bump.reset()`, reset both sides |
| Sort / ExternalSort | Clear `rows`, reset input |
| TopK | `output = None`, `arena.reset()`, reset input |
| HashAgg / SimpleAgg | Clear output state, reset input |
| Window | `state.clear()`, `reset_functions()`, reset input |
| NestLoopJoin | Back to `PullLeft`, reset both sides |
| MarkApply | Only resets the left side; keeps the right-side cache when `materialize` is on and a cache exists |

So `rescan_work = work` is accurate; the only exception is MarkApply with `materialize` enabled.

### 6.3 Sort hints do not affect Greedy leaves

The sort discount in scan cost comes from `sort_elimination_hint` / `stream_aggregate_hint`. We need to confirm that
when Greedy treats a leaf as a hash input or rewrites it into a probe, it neither loses an ordering that an upper
operator relies on nor uses a discounted `work`:

- Hints are pushed top-down during implementation by `annotate_hints_and_physical_options`
  (`heuristic/optimizer.rs`) and only pass through `Filter` / `Project` / `Limit` / `TopK` / `Sort`; any other
  operator such as Join replaces them with an empty hint. So scans under a Join (i.e. Greedy leaves) have
  `hint_sum = 0` and no sort discount.
- `EliminateRedundantSort` / `UseStreamAggregate`, which consume ordering, run in `apply_annotated_post_rules` of the
  same stage, before Greedy; their `ensure_order` only descends through `Childrens::Only` and never crosses a Join.
  A Join's own `PhysicalOption` is `SortOption::None`.
- If a Sort / Aggregate sits between a leaf and the Join, that subtree is not `Filter* + TableScan`, so it cannot be a
  probe inner side, and its ordering is only consumed inside the subtree.

Conclusion: changing a leaf's access path in Greedy does not break any upper ordering dependency, and the `work` used
by the Hash shape contains no sort discount.

## 7. Implementation Plan

The cost model is the prerequisite for Greedy's decisions, so costs are unified first and decisions are handed over
second, in two commits to make regressions easy to locate.

1. Unify costs (behavior only): introduce `rows_work` and `JoinImpl` / `InputCost`, fix the HashJoin, NLJ, and
   MarkApply estimates, and change the Greedy score to the work done in this step. Existing join orders and explain /
   slt expectations will change and need to be recalibrated as a whole.
2. Hand over decisions: extract the shared predicate function and `index_covering`; Greedy compares shapes, rewrites
   probes directly, and marks the rest `join_decided`; `ParameterizeInnerJoin` skips `join_decided` joins; Greedy's
   trigger is relaxed to `can_expand` and it takes over NestLoop direction (`FORCE_NEST_LOOP_JOIN` regions only
   compare NestLoop); remove `OrientNestedLoopJoin` and its batch.

Tests (unit tests via `make test`, slt via `make test-slt`):

- `rows_work` and each `JoinImpl::work`: Full join, zero-column rows, saturating arithmetic.
- Greedy: large outer × small unique inner chooses hash; small outer × large unique inner chooses probe and merges
  it first; the break-even point; the cheapest of several usable unique indexes; a composite unique index plus
  constant equalities; default row counts without statistics.
- Downstream rules: `join_decided` joins are not rewritten; unmarked joins take the fallback.
- NestLoop direction: cross pairs inside a Greedy region, regions rooted at Cross, and `FORCE_NEST_LOOP_JOIN` regions
  all reach the same direction as the existing `OrientNestedLoopJoin` cases (migrate its unit tests), and only NLJ
  appears under `FORCE_NEST_LOOP_JOIN`.
- Coverage: when the scan chose a covering index and another covering Unique can be probed, the probe is priced with
  `seeks = 1` and chosen, the rewritten IndexScan carries `covered_deserializers`, and execution skips the table
  lookup.
- Correctness: after a probe, all `on` conditions, the join filter, and static filters are kept (covered by slt).

## 8. Future Work

- `rescan_work` for MarkApply with `materialize` enabled, and the deviation where Greedy decides NLJ direction from
  the pre-parameterization / pre-materialization estimate of a subquery-style MarkApply in the inner side (3.10).
- Probes on non-unique indexes: the executor must support emitting multiple rows per outer row, and resetting the
  inner side per row is expensive, so evaluate case by case; `rows_per_lookup` would then be estimated from the
  index NDV.
- Joint NDV and uniqueness bound for composite join keys: under the hash shape, estimate rows with any PK/Unique bound
  by the join keys, not only the index chosen during implementation (row count is a property of the data, not of
  which index the scan uses). Single-column keys already get NDV from column histograms, so the benefit is only for
  composite keys; do it when a concrete case appears.
- I/O modeling.
