# OPT-1 · Aggregate Pushdown Rule Parity — Design

Date: 2026-05-20
Status: Draft (pending implementation plan)
Roadmap item: TODO List → Optimizer 路线图 → OPT-1
Reference doc: `NovaRocks TODO/OPT-1-aggregate-pushdown-rule.md`
StarRocks reference: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/tree/pdagg/`
Prereq landed: OPT-5 (observability + disable knob + plan-golden) merged as PR #147 / commit `d839aa3f`

## 1. Goal

Add a `LogicalAggregate → LogicalJoin` pushdown rule to the NovaRocks RBO
pipeline so analytical queries with aggregations over joins compute partial
aggregates as close to the leaves as possible. Cost-gate the rewrite with
NDV bucketing so high-cardinality group keys do not get pushed past joins
where the partial aggregate buys nothing. Use OPT-5's plan-golden suite
and `disable_optimizer_rules` knob to verify behavior end-to-end.

Out of scope for this PR (see §9 Non-goals):

- AVG / STDDEV / HLL_UNION / BITMAP_UNION pushdown.
- Aggregate args that are expressions (`SUM(a+b)`).
- Set-operation (UNION ALL) pushdown.
- Grouping sets / rollup / cube.
- CBO Memo carrying push and no-push alternatives in parallel.

## 2. Reference Survey (StarRocks)

Confirmed by reading
`fe-core/src/main/java/com/starrocks/sql/optimizer/rule/tree/pdagg/`:

- **Two-phase architecture.** `PushDownAggregateCollector` walks
  top-down from `LogicalAggregateOperator`, accumulating an
  `AggregatePushDownContext` and selecting the deepest valid push target.
  `PushDownAggregateRewriter` then performs a single top-down rewrite at
  the chosen target.
- **White-list aggregate functions.** SUM / MIN / MAX / HLL_UNION /
  BITMAP_UNION / PERCENTILE_UNION. DISTINCT and `COUNT(*)` are skipped
  in the collector entry.
- **Group-key derivation at join.** Partial group-by =
  (original group keys living on this child) ∪ (join predicate columns
  on this child).
- **Function rewrite.** Same function name at partial and final stages
  (`SUM(x) → SUM(SUM_partial(x))`, `MIN(x) → MIN(MIN_partial(x))`); the
  framework relies on dynamic function resolution to handle partial
  return-type coercion.
- **Cost gate.** Per-column NDV bucketed into LOW / MEDIUM / HIGH;
  decision depends on number of buckets and combined NDV vs row count.
- **Outer join handling.** No explicit rejection; the safety follows
  from "all aggregation columns must come from one child" — on an
  outer join's amplifier side, that constraint can never be satisfied.

Decisions adopted from the survey (deltas vs StarRocks):

- **`COUNT(x)` IS in the white-list.** Mapped explicitly as
  `partial=COUNT, final=SUM` — this is the one function pair where the
  outer function name changes. StarRocks does this implicitly via
  dynamic function resolution; NovaRocks does it via an explicit
  three-tuple table.
- **HLL_UNION / BITMAP_UNION / PERCENTILE_UNION are excluded** from the
  v1 white-list. NovaRocks BITMAP / HLL types are not landed yet
  (tracked separately as PR-B2 / INT-3).
- **Aggregate args are restricted to `ColumnRef`** in v1. `SUM(a+b)`
  is filed as an OPT-1 follow-up.

## 3. Current Baseline (verified)

- `LogicalPlan::Aggregate(AggregateNode)` at `src/sql/planner/plan.rs:21`,
  with fields `input`, `group_by: Vec<TypedExpr>`,
  `aggregates: Vec<AggregateCall>`, `output_columns: Vec<OutputColumn>`.
- `AggregateCall { name: String, args: Vec<TypedExpr>, distinct: bool,
  result_type: DataType, order_by: Vec<SortItem> }` at
  `src/sql/planner/plan.rs:185-191`.
- `LogicalPlan::Join(JoinNode)` with `join_type: JoinKind`, `condition:
  Option<TypedExpr>` at `src/sql/planner/plan.rs:211-217`.
- `JoinKind` covers Inner / LeftOuter / RightOuter / FullOuter / Cross /
  LeftSemi / RightSemi / LeftAnti / RightAnti.
- Existing RBO rule sub-modules to follow as structural template:
  `src/sql/optimizer/rbo/rules/predicate_pushdown/` (multi-file rule
  set), `src/sql/optimizer/rbo/rules/join_reorder/` (rule factory takes
  `&table_stats`).
- `ColumnStatistic.distinct_values_count: f64` already populated by the
  stats subsystem (`src/sql/optimizer/statistics.rs:7-13`).
- RBO driver gate `options.is_enabled(rule.name())` already exists
  (`src/sql/optimizer/rbo/driver.rs:60`) — OPT-5 plumbed
  `disable_optimizer_rules`. New rule's `name()` plugs straight in.
- OPT-5 SQL suite `sql-tests/optimizer/` provides plan-golden conventions
  and the `@explain_contains` / `@normalize_explain_timing` directives.

## 4. Design

### 4.1 File layout

New subdirectory `src/sql/optimizer/rbo/rules/aggregate_pushdown/`:

- `mod.rs` — public entry point. Exports
  `pub(crate) fn aggregate_pushdown_rules(table_stats: &HashMap<String, TableStatistics>) -> Vec<Box<dyn RewriteRule>>`
  returning a single `AggregatePushdownRule` instance.
- `rule.rs` — `AggregatePushdownRule { table_stats: HashMap<String, TableStatistics> }`
  implementing `RewriteRule`. `name() = "AggregatePushdown"`. `matches` is
  `matches!(plan, LogicalPlan::Aggregate(_))`. `apply` delegates to
  `collector::collect` then `rewriter::rewrite`.
- `collector.rs` — Phase 1. Top-down DFS from an `AggregateNode`, returns
  `Option<PushPlan>`.
- `rewriter.rs` — Phase 2. Splices `partial` `AggregateNode` into the plan
  tree at the push target, rebuilds the parent path, rewrites the
  top-level aggregate's function calls.
- `cost.rs` — `should_push(plan: &PushPlan, stats: &HashMap<...>) -> bool`.
  NDV bucketing heuristic.
- `context.rs` — `AggregatePushDownContext { original_groupby, original_aggregates,
  derived_groupby, required_columns, ... }` plus
  `PushPlan { target_subtree_path, partial_groupby, partial_aggregates }`.
- `tests.rs` — unit tests covering the matrix in §4.6.

Wire-up: `src/sql/optimizer/rbo/rules/mod.rs` re-exports
`aggregate_pushdown::aggregate_pushdown_rules`. `src/sql/optimizer/mod.rs`
inserts a new `rewrite_to_fixed_point` pass between the second predicate
pushdown pass and column pruning (current lines ~73-83).

### 4.2 Safety filters and function table

White-list aggregate functions (constant table in `rewriter.rs`):

| Original | Partial fn | Final fn | Notes |
|---|---|---|---|
| `SUM(x)`  | `SUM(x)`   | `SUM`   | identity rewrite |
| `MIN(x)`  | `MIN(x)`   | `MIN`   | identity rewrite |
| `MAX(x)`  | `MAX(x)`   | `MAX`   | identity rewrite |
| `COUNT(x)`| `COUNT(x)` | `SUM`   | function name changes at final stage |

Anything outside the table → reject the **entire** aggregate
(not just the offending call), because partial schema is only valid if
every output column is splittable.

Explicit rejection list, with the stage that rejects:

| Pattern | Stage | Reason |
|---|---|---|
| `AggregateCall.distinct == true` for any call | Collector entry | DISTINCT is `SplitDistinctAgg`'s domain |
| `COUNT(*)` (`name == "count"` AND `args.is_empty()`) | Collector entry | partial COUNT(*) on either side × cross join row count |
| `AggregateNode.group_by.is_empty()` | Collector entry | partial collapses to a single row, indistinguishable from full count |
| any `AggregateCall.order_by` non-empty | Collector entry | order-sensitive aggregates (`array_agg ORDER BY ...`) cannot be re-ordered after partial |
| any call `name` not in white-list | Collector entry | YAGNI; future PRs extend |
| any aggregate arg is not a bare `ColumnRef` | Collector entry | first version supports column references only |
| any aggregate arg references a non-deterministic function (RAND / UUID / NOW / CURRENT_TIMESTAMP / CURRENT_DATE) | Collector entry | re-evaluation under partial yields different results |
| Outer join's amplifier side carries the aggregate | Collector at Join | LEFT OUTER cannot push to right; RIGHT OUTER cannot push to left |
| Cross join (`condition.is_none()`) | Collector at Join | no equi key to use as partial group |
| Non-equi join condition | Collector at Join | no column equality to derive partial group keys |
| Semi / Anti / Full Outer join | Collector at Join | not supported in v1 |
| Aggregate args / group keys span both join children | Collector at Join | cannot split into per-side partials |
| Project introduces a non-`ColumnRef` expression on a path column | Collector at Project | re-binding columns through computed expressions is not v1 |
| `AggregateNode.already_pushed == true` (rewriter output) | Collector entry | idempotency guard, see §4.5 |

Non-deterministic detection: a static list of function names
`{ "rand", "random", "uuid", "now", "current_timestamp",
"current_date" }` checked recursively against every `TypedExpr` in
`aggregate.args`. Lives in `collector.rs`.

### 4.3 Algorithm — Collector

Entry: `collect(plan: &LogicalPlan, ctx: &mut AggregatePushDownContext)
-> Option<PushPlan>`.

```text
collect(plan, ctx):
  match plan:
    Scan(_) | LocalParquet(_):
      // Reached a leaf; this is the push target.
      return Some(PushPlan {
        target: plan,
        partial_groupby: ctx.derived_groupby.clone(),
        partial_aggregates: ctx.original_aggregates.clone(),
      })

    Filter(child, predicate):
      // Predicate filters partial input; predicate columns must be
      // visible in the partial group-by to preserve semantics across
      // splice. We don't push the filter; we just record its columns.
      ctx.required_columns ∪= predicate.referenced_columns()
      collect(child, ctx)

    Project(child, items):
      // Translate ctx's column refs through the projection. If any
      // required column maps to a non-ColumnRef expression, reject.
      for item in items:
        if item.output is in ctx.required_columns:
          if !matches!(item.expr, TypedExpr::ColumnRef(_)):
            return None
      ctx.translate_refs(items)
      collect(child, ctx)

    Join(left, right, kind, condition):
      split_at_join(plan, ctx)

    Aggregate(_) | Limit(_) | Sort(_) | Union(_) | other:
      // Do not penetrate other blocking operators in v1.
      return None
```

```text
split_at_join(join, ctx):
  // Step 1: join shape filter.
  if join.kind not in {Inner, LeftOuter, RightOuter}: return None
  let Some(cond) = join.condition else return None  // cross join
  let equi_keys = extract_equi_keys(cond)
  if equi_keys.is_empty(): return None              // non-equi join

  // Step 2: column requirements.
  required = ctx.aggregate_arg_columns ∪ ctx.derived_groupby_columns
  left_cols = join.left.output_columns
  right_cols = join.right.output_columns

  // Step 3: side selection.
  side =
    if required ⊆ left_cols  { Left }
    else if required ⊆ right_cols { Right }
    else { return None }  // cross-side aggregate; cannot push as a whole

  // Step 4: outer-join amplifier rejection.
  match (join.kind, side):
    (RightOuter, Left)  => return None  // left is amplified by RIGHT OUTER
    (LeftOuter,  Right) => return None  // right is amplified by LEFT OUTER
    _ => ()

  // Step 5: partial group-by = derived groupby on side + side-bound join keys.
  let side_subtree = match side { Left => &join.left, Right => &join.right }
  let join_keys_on_side =
    equi_keys.filter(|c| c in side_subtree.output_columns)
  let mut child_ctx = ctx.clone()
  child_ctx.derived_groupby =
    ctx.derived_groupby.filter(|c| c in side_subtree.output_columns)
      .chain(join_keys_on_side)
      .dedup()

  // Step 6: recurse to find a deeper target on the chosen side.
  let deeper = collect(side_subtree, &mut child_ctx)
  deeper.or(Some(PushPlan {
    target: side_subtree,
    partial_groupby: child_ctx.derived_groupby,
    partial_aggregates: ctx.original_aggregates.clone(),
  }))
```

Step 5's "**deepest valid target**" semantics matches StarRocks. Other
fixed-point rules then continue rewriting the plan.

### 4.4 Cost gate

`cost.rs`:

```rust
const HIGH_REDUCTION_RATIO: f64 = 100.0;       // row_count / ndv ≥ 100 → LOW bucket
const MIN_PARTIAL_BENEFIT_RATIO: f64 = 0.5;    // partial NDV / row_count < 0.5 → push
const UNKNOWN_NDV_ROW_THRESHOLD: u64 = 10_000; // fallback when NDV unavailable

pub(crate) fn should_push(
    plan: &PushPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> bool { ... }
```

Algorithm:

1. Estimate `target_subtree` `Statistics` by calling
   `crate::sql::optimizer::rbo::rules::join_reorder::cardinality::estimate_statistics(plan, table_stats)`
   (verified `pub(crate)` at `src/sql/optimizer/rbo/rules/join_reorder/cardinality.rs:14`).
   Use `stats.output_row_count` and
   `stats.column_statistics[col].distinct_values_count`.
2. For each column in `plan.partial_groupby`, look up its
   `distinct_values_count` from the same `Statistics`.
3. **All NDVs known:** compute joint NDV ≈ `min(prod(per_col_ndv),
   row_count)`. Push iff
   `joint_ndv < row_count * MIN_PARTIAL_BENEFIT_RATIO`.
4. **Any NDV unknown / NaN / 0:** fall back to row-count threshold.
   Push iff `row_count > UNKNOWN_NDV_ROW_THRESHOLD`.

Constants are chosen conservatively and explicitly tagged `TODO(opt-3):
revisit once histogram-aware estimation lands`.

### 4.5 Algorithm — Rewriter

Input: original `AggregateNode` + `PushPlan` from the collector.

Output: a new `LogicalPlan` with:

```text
   AggregateNode {
     group_by: <original>,
     aggregates: <rewritten as FINAL-stage calls>,
     output_columns: <original>,
     already_pushed: true,
     input: <original path with partial node spliced at PushPlan.target>,
   }
```

Steps:

1. **Build partial AggregateNode.** For each original `AggregateCall`,
   construct a partial call:
   - `name` = white-list partial name (same as original except
     `COUNT(x)` stays `COUNT(x)` at partial stage).
   - `args` = original args (still valid column refs on push target).
   - `distinct = false`, `order_by = vec![]`.
   - `result_type` = partial result type via a small per-function
     helper table in `rewriter.rs` (only four entries — see "Partial
     result type table" below).
2. **Allocate synthetic output columns** for the partial aggregates with
   names `__nr_agg_pd_<i>` (where `<i>` is the index into
   `partial_aggregates`). `OutputColumn` is name-keyed only (see
   `src/sql/analysis/mod.rs:27-32`: `{ name, data_type, nullable }`) —
   no id allocator needed; uniqueness is guaranteed by the
   `__nr_agg_pd_` prefix being reserved.
3. **Splice partial node into the plan tree.** Walk the same path the
   collector took (recorded in `PushPlan.target_subtree_path`); at each
   node, clone-rebuild with the child swapped. At the final join,
   replace the chosen side with the new partial `AggregateNode` whose
   `input` is `PushPlan.target` and whose `output_columns` = partial
   group-by outputs ∪ partial aggregate outputs.
4. **Rewrite top-level aggregate function calls.** For each original
   `AggregateCall` at index `i`:
   - `name` = white-list **final** name (e.g. `COUNT → SUM`).
   - `args` = `vec![TypedExpr::ColumnRef(partial_output_cols[i])]`.
   - `result_type` = original `result_type` (top-level output type
     unchanged).
   - `order_by`, `distinct` carried as-is (note: distinct=true already
     rejected upstream, so this is always false here).
5. **Preserve top-level `output_columns` exactly** — parent operators'
   SchemaRef stays intact.
6. **Set `already_pushed = true`** on the returned top-level
   `AggregateNode`.

**Partial result type table** (rewriter-local helper, see §4.2 for the
function rewrite table this complements):

| Original call | Partial result type rule |
|---|---|
| `SUM(x: Int8/Int16/Int32/Int64)` | `Int64` |
| `SUM(x: UInt*)` | `Int64` |
| `SUM(x: Float32/Float64)` | `Float64` |
| `SUM(x: Decimal(p,s))` | `Decimal(min(p+10, 38), s)` |
| `MIN(x) / MAX(x)` | type of `x` |
| `COUNT(x)` | `Int64` |

These rules are identical to what the analyzer assigns at
`AggregateCall.result_type` (lifted from `expr.data_type` at
`src/sql/planner/mod.rs:1110`), so the partial type is just the
original call's `result_type` for SUM/MIN/MAX/COUNT — no separate
inference path needed. The rewriter copies `call.result_type` to the
partial. The **final** call for COUNT(x) becomes SUM(partial_count) and
its `result_type` stays `Int64` (matching the original COUNT's type).

### 4.6 Verification

#### 4.6.1 Unit tests

Positive cases (rule fires, output structurally as expected):

- `pushes_sum_under_inner_join_to_left`
- `pushes_sum_under_inner_join_to_right`
- `pushes_count_via_partial_count_final_sum`
- `pushes_through_left_outer_to_left_preserved`
- `pushes_through_right_outer_to_right_preserved`
- `pushes_through_filter_above_join`
- `pushes_to_deeper_target_through_multiple_inner_joins`
- `partial_groupby_includes_join_keys`
- `multi_aggregate_pushed_as_one_partial`

Negative cases (rule returns `None`, plan unchanged):

- `rejects_count_star`
- `rejects_empty_groupby`
- `rejects_distinct_aggregate`
- `rejects_avg_function`
- `rejects_order_sensitive_aggregate`
- `rejects_aggregate_expr_not_columnref`
- `rejects_aggregate_on_nondeterministic_function`
- `rejects_left_outer_when_agg_on_right`
- `rejects_right_outer_when_agg_on_left`
- `rejects_cross_join`
- `rejects_non_equi_join`
- `rejects_semi_anti_join`
- `rejects_aggregate_columns_across_join_sides`
- `idempotent_does_not_repush_already_pushed_plan`

Cost-gate unit tests (in `cost.rs`):

- `low_cardinality_pushes` — row=10000, ndv=10 → push.
- `high_cardinality_rejects` — row=10000, ndv=10000 → reject.
- `unknown_ndv_pushes_above_threshold` — ndv unset, row=20000 → push.
- `unknown_ndv_rejects_below_threshold` — ndv unset, row=500 → reject.
- `multi_column_joint_ndv_capped_by_row_count`.

#### 4.6.2 SQL plan-golden

New cases under `sql-tests/optimizer/sql/` + `result/`:

- `aggregate_pushdown_inner_join.sql` — positive baseline; recorded
  EXPLAIN VERBOSE shows two `AGGREGATE` nodes (partial + final).
- `aggregate_pushdown_count_star_rejected.sql` — negative; recorded
  output shows a single top-level `AGGREGATE`.
- `aggregate_pushdown_left_outer_preserved.sql` — positive; LEFT OUTER
  pushes to the preserved (left) side.
- `aggregate_pushdown_disabled.sql` — two EXPLAIN VERBOSE statements
  around `SET disable_optimizer_rules = 'AggregatePushdown'`; the two
  plans must differ. Restore the SET at the end of the file.

#### 4.6.3 Regression

Run `--suite analyze-statistics --mode verify` after the rule lands.
Per OPT-1's acceptance criteria, this suite should pass without engine
special paths. If any case is still slow, file follow-ups; do not block
the PR on suite latency.

Also re-run the OPT-5 regression set (`tpc-h`, optimizer, filter) for
no result drift.

## 5. Data Flow

```text
LogicalPlan (post predicate-pushdown, post join-reorder)
   |
   v
AggregatePushdownRule::apply(LogicalAggregate)
   |
   +-- collector::collect(input, ctx)
   |    |
   |    +-- walks Scan / Filter / Project / Join recursively
   |    +-- returns Option<PushPlan>
   |
   +-- if PushPlan, cost::should_push(&push_plan, &table_stats)
   |    |
   |    +-- NDV bucketing + row-count threshold
   |
   +-- if push allowed, rewriter::rewrite(orig_aggregate, push_plan)
        |
        +-- builds partial AggregateNode at PushPlan.target
        +-- splices through Project / Filter / Join clones
        +-- rewrites top-level call signatures (COUNT→SUM at final)
        +-- sets already_pushed=true on the result
   |
   v
Rewritten LogicalPlan (continues through fixed-point driver,
column pruning, CTE inline, Memo conversion, CBO search)
```

## 6. Failure Modes & Edge Cases

- **`AggregateNode.already_pushed` lost on clone in some other rule** —
  audit at PR review. All `..` struct update sites and explicit
  constructions in `column_pruning`, `predicate_pushdown`,
  `cte_rewrite`, and the planner. Default `false` is correct for
  fresh construction; clones preserve. Test:
  `idempotent_does_not_repush_already_pushed_plan`.

- **Partial result type widening misalignment.** Partial result type
  ≠ original arg type (SUM(INT)→BIGINT is the typical case). The
  rewriter relies on the analyzer's existing aggregate-result-type
  inference (verified location during implementation). If the
  analyzer's helper isn't reusable, the rewriter inlines the type
  table for the four white-listed functions.

- **Multi-column joint NDV explosion.** `min(prod(ndv), row_count)`
  caps the estimate but can still inflate decisions when columns are
  correlated. Acceptable for v1; OPT-3 histogram will improve.

- **Synthetic output column id collisions.** Use the project's id
  allocator (verify exact API during implementation). Test that two
  back-to-back rule firings on different aggregates produce
  non-overlapping ids.

- **Push under a join that is itself going to be removed by
  `EliminateUniqueAggregate`.** Both rules run in the same fixed-point
  driver — order is not guaranteed. Both produce semantically equivalent
  plans, but the rewrite shapes diverge. Acceptable; document in the
  rule's comment.

- **Plan-golden output ordering of synthetic columns.** Output column
  names follow `__nr_agg_pd_0, __nr_agg_pd_1, ...` by the index into the
  original `aggregates` list — deterministic per query.

## 7. Test Plan

1. **Rust unit tests** at `src/sql/optimizer/rbo/rules/aggregate_pushdown/`
   covering the matrices in §4.6.1.
2. **Cost-gate unit tests** in `cost.rs`.
3. **SQL plan-golden** in `sql-tests/optimizer/` (§4.6.2).
4. **Regression**: full library `cargo test --lib`, `cargo clippy --lib`
   (no new warnings on touched files), and SQL suites `tpc-h`,
   `analyze-statistics`, `optimizer`, `filter`.

## 8. Roll-out Sequencing (becomes the writing-plans input)

1. Add `already_pushed: bool` to `AggregateNode`; default false;
   audit all construction sites; minimal tests.
2. Scaffold `aggregate_pushdown/` subdir with `mod.rs`, `rule.rs`,
   `collector.rs`, `rewriter.rs`, `cost.rs`, `context.rs`; rule returns
   `None` for everything. Wire into pipeline. Confirm `cargo test`
   still green.
3. Implement `collector.rs` (safety filters + Scan/Filter/Project
   traversal, no join yet). Unit-test entry rejections.
4. Implement `split_at_join` and inner-join push to left/right. Unit
   tests for positive inner-join cases.
5. Add outer-join handling and rejection of cross-side / non-equi /
   semi/anti. Unit tests for negative cases.
6. Implement `rewriter.rs` with the white-list function table.
   Unit-test the partial/final wiring (rows-out schema preservation,
   COUNT→SUM at final).
7. Implement `cost.rs` NDV bucketing + threshold fallback. Cost-gate
   unit tests.
8. Add `idempotency` guard (`already_pushed` flag) and corresponding
   test.
9. Add SQL plan-golden cases under `sql-tests/optimizer/`. Run
   `--suite optimizer --mode record`, inspect, then verify.
10. Run regression: `cargo test --lib`, `--suite tpc-h --mode verify`,
    `--suite analyze-statistics --mode verify`, `--suite filter`.
11. Documentation: add a bullet under `CLAUDE.md` / `AGENTS.md` §9
    pointing at the new rule and the white-list constraints.

## 9. Non-goals (explicit)

- No AVG / STDDEV / VAR / HLL_UNION / BITMAP_UNION pushdown.
- No expression-arg pushdown (`SUM(a+b)`).
- No set-operation pushdown.
- No grouping sets / rollup / cube pushdown.
- No CBO Memo logical alternative for push vs no-push (RBO decides).
- No per-rule session variable beyond `disable_optimizer_rules`.
- No broadcast-join-specific cardinality threshold.
- No distinct aggregate pushdown.

## 10. Follow-up Tickets to File on Merge

- `OPT-1-followup-avg-stddev` — extend white-list to AVG / STDDEV / VAR
  (AVG splits as `SUM/COUNT` at partial, `SUM(partial_sum)/SUM(partial_count)` at final).
- `OPT-1-followup-arg-expressions` — push aggregates whose args are
  expressions, by projecting the expression below the partial.
- `OPT-1-followup-union-pushdown` — extend collector to walk through
  UNION ALL.
- `OPT-1-followup-cbo-memo-alternative` — register push and no-push
  forms as parallel CBO logical alternatives so cost picks.
- `OPT-1-followup-bitmap-hll-pushdown` — wire BITMAP_UNION /
  HLL_UNION white-list entries once the types land (PR-B2 / INT-3).
