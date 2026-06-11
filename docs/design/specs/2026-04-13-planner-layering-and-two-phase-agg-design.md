# Planner Layering Cleanup and Two-Phase Distributed Aggregation

Date: 2026-04-13
Status: Draft

## 1. Overview

Align NovaRocks's standalone planner toward the StarRocks FE reference architecture by cleaning up layering violations in `fragment_builder.rs` and then implementing two-phase (LOCAL + GLOBAL) distributed hash aggregation. The goal is to close the largest single gap observed in a TPC-DS 99-query EXPLAIN comparison between the two systems: standalone currently emits only `HASH AGGREGATE (SINGLE)` with >80% `GATHER EXCHANGE`, whereas FE uses two-phase aggregation with `HASH_PARTITIONED` shuffle for nearly every group-by query.

**Scope (in):**
- Move optimization decisions currently made in `fragment_builder.rs` into the cascades layer. Turn `fragment_builder.rs` into a pure `PhysicalPlanNode → TPlan` translator.
- Introduce expression-rewriting infrastructure for splitting aggregates into LOCAL and GLOBAL phases.
- Implement `SplitTwoPhaseAggRule` to produce Local+Global alternatives alongside Single, and let the cost-based search pick the winner.

**Scope (out):**
- Merging the legacy `src/sql/optimizer/` pass into cascades (deferred, tracked separately).
- Decomposing `src/standalone/engine.rs` (4538 lines, catch-all execution engine).
- Tuning BROADCAST vs SHUFFLE join distribution selection.
- MERGING-EXCHANGE (distributed merge sort).
- DISTINCT aggregate four-phase pattern (`DISTINCT_LOCAL` / `DISTINCT_GLOBAL`).
- Two-phase aggregation over `PhysicalRepeat` (grouping sets / ROLLUP / CUBE).

**Validation:**
- `cargo test` stays green at every phase boundary.
- TPC-DS 99 queries pass `sql-tests --suite tpc-ds --mode verify` after Phase 4 (results match FE).
- EXPLAIN snapshots are diffed at each phase boundary; unexpected plan changes block merge.

## 2. Motivation

A TPC-DS 99-query EXPLAIN comparison between StarRocks FE (port 9130, Iceberg catalog) and the NovaRocks standalone server (port 9030) identified the following:

| Dimension | FE | Standalone | Gap |
|---|---|---|---|
| Aggregation mode | 243 `update serialize` + 234 `merge finalize` (two-phase, 98/99 queries) | 202 `HASH AGGREGATE (SINGLE)` | P0 — largest single gap |
| Exchange type | Rich `HASH_PARTITIONED` mix (hundreds of partition patterns) | 806 `GATHER EXCHANGE` vs 179 `HASH EXCHANGE` | P0 — coupled to aggregation |
| Sort/Limit | 79 queries use TOP-N; 89 queries use MERGING-EXCHANGE | Separate SORT + LIMIT + GATHER EXCHANGE | P1 |
| Join distribution | 76% BROADCAST / 19% SHUFFLE / 5% CROSS | 85% BROADCAST / 11% SHUFFLE / 7% NEST LOOP | P1 |
| Join order | Cost-based reorder | Differs (see q3, q7, q17) | P2 |

Before adding two-phase aggregation, structural issues in the planner must be addressed. `src/sql/cascades/fragment_builder.rs` (2465 lines) currently makes optimization decisions that should belong to cascades rules — notably the `use_top_n` post-mutation (lines 967–991), join-condition side-swapping heuristics (lines 559–605), and window-function multi-group detection (lines 1004–1009). Adding two-phase aggregation on top of this layering would compound the mess. The user has explicitly prioritized structural cleanup (Option A) over a faster feature delivery.

## 3. Target Architecture

```
LogicalPlan (src/sql/plan/)
    ↓  [Analyzer + RBO rewriter]
Cascades Memo (src/sql/cascades/)
    ↓  [Transformation rules]            ← all optimization decisions live here
Cascades Memo (optimized)
    ↓  [Implementation rules]            ← Logical → Physical, algorithm + properties fixed
PhysicalPlanNode (complete, translatable)
    ↓  [Fragment Builder]                ← pure PhysicalPlanNode → TPlan translation
TPlan (execution)
```

### 3.1 Two Red Lines

**Red Line 1 — Fragment builder is a dumb translator.**
`fragment_builder.rs` does not make any optimization decision. Every `PhysicalPlanNode` it receives is already finalized: operator shape, column bindings, aggregate mode, and output nullability are all set by the cascades layer. Fragment builder mechanically maps each physical operator to the corresponding Thrift plan node. Specifically:
- No `use_top_n = true` post-mutation on sort nodes.
- No "try natural order, swap on failure" fallback for join equi-conditions.
- No post-hoc grouping of window expressions.
- No output-nullability widening for OUTER JOIN tuples.

**Red Line 2 — Cascades owns the final shape of physical operators.**
- A new `PhysicalTopN` operator exists and is produced by a cascades implementation rule matching `PhysicalLimit(PhysicalSort(x))`.
- `PhysicalHashAggregate` gains `Local` and `Global` modes in addition to `Single`, produced by `SplitTwoPhaseAggRule`.
- `JoinCommutativity` normalizes `eq_conditions` when swapping join children.
- Multi-group window decomposition happens in the `PhysicalWindow` implementation rule, which emits one `PhysicalWindow` per `(partition_by, order_by)` signature with any required `PhysicalSort` between them.
- OUTER-JOIN output-column nullability is derived as a physical property in cascades and stored on the `PhysicalPlanNode`.

### 3.2 Non-Goals for This Round

- Physical expression IR separate from `TypedExpr`: keep `TypedExpr` everywhere for now. The expression rewriter introduced in Phase 3 operates on `TypedExpr`/`AggregateCall` directly. Introducing a separate physical expression type is a larger refactor deferred to a future round.
- Physical type system separate from Arrow `DataType`: keep Arrow types throughout.
- `src/sql/optimizer/` legacy pass remains as today; it runs before cascades as an RBO rewriter.

## 4. Phased Implementation Path

Four phases. Each phase is an independent, reviewable change with its own regression baseline.

### 4.1 Phase 1 — TopN Pilot

Establish the "move optimization from fragment_builder to cascades" pattern with the smallest possible change.

**Changes:**

| File | Change |
|---|---|
| `src/sql/cascades/operator.rs` | Add `Operator::PhysicalTopN(PhysicalTopNOp)`. Fields: `sort_items: Vec<SortItem>`, `limit: Option<u64>`, `offset: Option<u64>`. |
| `src/sql/cascades/rules/implement.rs` | Add `SortLimitToTopN` rule. Matches `PhysicalLimit(PhysicalSort(x))`, produces `PhysicalTopN(x)` with merged limit/offset/sort_items. |
| `src/sql/cascades/search.rs` | `PhysicalTopN` output distribution = child distribution (preserved). `PhysicalTopN` output ordering = `Required(sort_items)`. Cost = `n * log2(min(n, limit))`. |
| `src/sql/cascades/fragment_builder.rs` | Remove the 25-line `use_top_n` post-mutation in `visit_limit`. Add `visit_physical_top_n` that emits a `SORT_NODE` with `use_top_n=true`, `limit`, `offset`. |
| `src/sql/explain.rs` | Format `PhysicalTopN` as `TOP-N (limit=N, offset=M)` followed by `sort_items:` line. |

**Regression baseline:** Current standalone EXPLAIN snapshots (already saved to `/tmp/novarocks-plan-compare/standalone/`).

**Expected plan changes (TPC-DS):** Every query currently showing `LIMIT [limit=N] → SORT BY [...]` should now show `TOP-N (limit=N) → [sort_items]`. Roughly 79 queries affected based on current FE TOP-N usage as a proxy.

**Not-expected changes:** Queries with SORT but no LIMIT stay unchanged. Queries with LIMIT but no SORT stay unchanged. Join/scan/aggregate shapes all unchanged.

**Validation:** `cargo test`; diff EXPLAIN snapshots; run a handful of LIMIT queries (q3, q7, q22) end-to-end and confirm results match.

**Phase 1 landed.** Date: 2026-04-13. HEAD at landing: 502e4cc7f7237b55825c031440e7ed908abef9e6. Observed:
- 65 of 99 TPC-DS queries switched from LIMIT+SORT to TOP-N (Phase-0 LIMIT+SORT count: 84; Phase-1 TOP-N count: 65; delta explained by queries where LIMIT is above a non-SORT node, e.g. subquery LIMIT, which correctly remain as LIMIT nodes).
- Non-Sort/Limit/TopN plan lines are identical between Phase-0 and Phase-1 across all 10 representative queries — the 8 apparent diffs are purely structural: TOP-N collapses the old `outer GATHER EXCHANGE -> LIMIT -> SORT BY -> inner GATHER EXCHANGE` sequence into `TOP-N -> GATHER EXCHANGE`, removing exactly one `GATHER EXCHANGE` level; join/scan/aggregate shapes are unchanged.
- TPC-DS verify suite: 85 of 99 queries pass (14 fail: q6, q7, q17, q18, q25, q26, q29, q45, q54, q61, q72, q80, q85, q95). These failures are pre-existing (present at Phase-0 baseline 5473b31); no new failures introduced by Phase 1.

### 4.2 Phase 2 — Remaining fragment_builder Cleanups

Apply the Phase 1 pattern to the other `fragment_builder.rs` layer violations. Each sub-item is an independent commit.

**2.1 Join condition normalization in `JoinCommutativity` rule.**
Today, when `JoinCommutativity` swaps `(L JOIN R) → (R JOIN L)`, it does not rewrite the `eq_conditions`. Fragment builder then tries to compile each `(expr_a, expr_b)` pair against `(left.scope, right.scope)`; if that fails, it retries against the swapped scopes. Move the normalization into `JoinCommutativity`: when swapping children, swap each `(expr_a, expr_b)` pair in `eq_conditions`. The `other_condition` is a predicate and semantically side-insensitive, so it does not need rewriting. Delete the natural-or-swap fallback in `fragment_builder.rs` (currently around lines 559–605). Also adjust `JoinKind` when swapping: `LeftOuter ↔ RightOuter`, `LeftSemi ↔ RightSemi`, `LeftAnti ↔ RightAnti`; `Inner`, `FullOuter`, and `Cross` are symmetric.

**2.2 Multi-group window decomposition in the `PhysicalWindow` implementation rule.**
Today, `fragment_builder.rs:1004–1009` groups window expressions by `(partition_by, order_by)` signature at emission time and emits multiple sort+analytic node pairs. Move this into the implementation rule: the rule receives a logical `Window` operator with N window expressions, partitions them by signature into K groups, and emits a chain of K `PhysicalWindow` nodes with `PhysicalSort` between them where the ordering signature changes. Fragment builder's `visit_physical_window` emits one `SORT_NODE` + `ANALYTIC_EVAL_NODE` per cascades-provided `PhysicalWindow`.

**2.3 OUTER JOIN nullability widening as a physical property.**
Today, `fragment_builder.rs:631–657` mutates the descriptor table during `visit_hash_join` to widen nullability for OUTER-JOIN tuples. Move this into `PhysicalHashJoin`'s property derivation: the physical plan node carries output column `nullable` flags derived from the join type (LEFT OUTER widens right; RIGHT OUTER widens left; FULL OUTER widens both). Fragment builder reads these flags from `PhysicalPlanNode.output_columns` and emits the descriptor table accordingly, never mutating it retroactively.

**Regression baseline for each sub-item:** The EXPLAIN snapshot after the preceding sub-item.

**Expected plan changes per sub-item:**
- 2.1: queries with JOIN commutativity trigger may show normalized `eq_conditions` ordering. The set of affected queries is baseline-captured empirically during implementation; the cleanup is not expected to add or remove joins.
- 2.2: queries with multiple window expressions with different `(partition_by, order_by)` signatures now show multiple `PhysicalWindow` + `PhysicalSort` nodes in the cascades-level EXPLAIN (what was implicit at fragment level becomes explicit at cascades level). The exact affected query list is captured during implementation.
- 2.3: no observable change in EXPLAIN text, but `output_columns` nullable flags move from fragment-time to cascades-time.

**Phase 2 landed.** Date: 2026-04-13. HEAD at landing: 3907588. Three sub-tasks delivered as five commits (3fbecc3 + 788a431 for 2.1, 55c08db + 1c95a5b for 2.2, 33604af + 3907588 for 2.3). Observed:
- 60 of 99 TPC-DS queries' EXPLAIN snapshots differ vs Phase 1. Differences classified as eq_conditions LHS/RHS ordering normalized by JoinToHashJoin (majority of queries), and SHUFFLE→BROADCAST distribution strategy changes with probe/build side reordering (cascades cost model sensitivity from Phase 2 rewrites). No changes to scan order, join shape, aggregate mode, or partition keys.
- TPC-DS end-to-end execution not re-run for Phase 2 — only EXPLAIN-level regression checked, since refactors should be plan-equivalent. Phase 1's 85/99 verify result remains the latest authoritative pass count.
- Task 2.1 deviation from spec text: normalization lives in `JoinToHashJoin` (implementation rule) rather than `JoinCommutativity` (transformation rule), because `LogicalJoin` has no `eq_conditions` field. Equivalent intent.
- Task 2.3 partial fallback: `fragment_builder`'s tuple-level nullability widening continues to be driven by `op.join_type`. `stats.rs::widen_for_join_kind` is the authoritative source for `output_columns.nullable`, consumed by downstream cascades code. A future per-slot nullability mechanism would let `fragment_builder` read from `output_columns`.

### 4.3 Phase 3 — Expression Rewriting Infrastructure

Build the aggregate rewriter that Phase 4 depends on. This phase adds new code but does not wire it into the optimizer main path, so EXPLAIN output is unchanged.

**New module:** `src/sql/cascades/rewrite/` (new directory)
- `aggregate_rewriter.rs` — the `AggregateRewriter` API.
- `agg_registry.rs` — per-function split-strategy registry.
- `intermediate_type.rs` — intermediate type derivation helpers.

**Core API:**

```rust
pub(crate) enum AggSplitStrategy {
    NonSplittable,
    /// Local and Global use the same function name; intermediate type may differ.
    /// Example: sum(i32) → Local sum(i32)→i64 → Global sum(i64)→i64.
    Simple { intermediate_type: DataType },
    /// Local and Global use different function names.
    /// Example: count → Local count(x)→i64 → Global sum(i64)→i64.
    Remap {
        local_fn: &'static str,
        global_fn: &'static str,
        intermediate_type: DataType,
    },
    /// Local produces multiple intermediate fields; Global combines them with an expression.
    /// Example: avg → Local (sum_x, count_x) → Global sum(sum_x) / sum(count_x).
    Composite {
        fields: Vec<CompositeField>,
        combine: CombineFn,
    },
}

pub(crate) struct AggregateRewriter;

impl AggregateRewriter {
    /// Look up the split strategy for a single aggregate call.
    pub(crate) fn strategy(call: &AggregateCall) -> AggSplitStrategy;

    /// Split a group of aggregate calls into (local_calls, global_calls).
    /// Returns None if any call is NonSplittable — caller falls back to Single mode.
    pub(crate) fn split(
        calls: &[AggregateCall],
        local_col_alloc: &mut ColumnAllocator,
    ) -> Option<SplitResult>;
}

pub(crate) struct SplitResult {
    pub local_calls: Vec<AggregateCall>,
    pub global_calls: Vec<AggregateCall>,
    /// Output expressions that combine composite aggregates (e.g., avg → sum/count).
    pub output_projection: Vec<TypedExpr>,
}
```

**Supported functions in this round:**
- Must support: `sum`, `count`, `count(*)`, `count(expr)`, `min`, `max`, `avg`.
- Also support: `bitmap_union`, `bitmap_union_count` (already marked as supported window kinds in project memory).
- Deferred to future rounds: `approx_count_distinct`, `percentile_*`, `hll_*`, `stddev`, `variance`, DISTINCT aggregates.

**Function registry layout:**
Each function gets one entry in `agg_registry.rs`, keyed by normalized function name. Unknown functions default to `NonSplittable` (fail-safe).

**Unit tests:** For each supported function, test that `split()` produces correct `(local, global)` calls and that the combined result matches single-phase evaluation semantically. Tests live in `src/sql/cascades/rewrite/tests/`.

**Regression baseline:** Phase 2.3's EXPLAIN snapshot. Expected plan changes: **none** (infrastructure-only, not wired to main path).

### 4.4 Phase 4 — Two-Phase Distributed Aggregation

Wire the rewriter into cascades as a new rule that generates Local+Global alternatives alongside Single.

**Changes:**

| File | Change |
|---|---|
| `src/sql/cascades/rules/implement.rs` | Extend `AggToHashAgg` (or add `SplitTwoPhaseAggRule`): when all aggregate calls are splittable and `group_by` is non-empty and input is not `PhysicalRepeat`, emit a second alternative `PhysicalHashAggregate(Local) → PhysicalHashAggregate(Global)`. Use `AggregateRewriter::split` to derive calls. |
| `src/sql/cascades/search.rs` | `PhysicalHashAggregate(Local)` output distribution = `Any` (preserves child). `PhysicalHashAggregate(Global)` requires input `HashPartitioned(group_keys)`. |
| `src/sql/cascades/cost.rs` | `HashAggregate(Local)` cost = `input_size × 0.5`. `HashAggregate(Global)` cost = `local_output_size × 0.3`. Local output size ≈ `input_row_count × reduction_factor × row_size`, where `reduction_factor = min(group_ndv / input_rows, 1.0)`. |
| `src/sql/cascades/fragment_builder.rs` | `visit_physical_hash_aggregate` handles `Local` → Thrift `update serialize` (streaming pre-agg); `Global` → Thrift `merge finalize`. Pure translation. |
| `src/sql/explain.rs` | Format as `HASH AGGREGATE (LOCAL, ...)` / `HASH AGGREGATE (GLOBAL, ...)`. |

**Split eligibility (all must hold):**
1. `group_by` is non-empty (scalar aggregates stay Single + Gather: final output is one row, two phases adds a useless shuffle).
2. Input is not `PhysicalRepeat` (grouping sets / ROLLUP / CUBE — deferred).
3. No aggregate call is DISTINCT (deferred).
4. Every aggregate call's function is registered as splittable.

When eligibility fails, only the Single alternative is produced; behavior is unchanged.

**Cost model intuition:**
```
Single total      = input_size
Two-phase total   ≈ input_size × 0.5 + local_output × 1.5 (shuffle) + local_output × 0.3
                  ≈ input_size × 0.5 + local_output × 1.8
```
When `local_output` << `input_size` (i.e., groups are few relative to rows), two-phase wins. When groups approach input rows (one group per row), `local_output ≈ input_size` and Single wins. No new tuning parameters are introduced; `group_ndv` uses the existing NDV heuristic (`sqrt(non_null_rows) * 10`) when stats are unknown.

**Regression baseline:** Phase 3's EXPLAIN snapshot.

**Expected plan changes:** Queries with `GROUP BY` and splittable aggregates switch from `HASH AGGREGATE (SINGLE)` to `HASH AGGREGATE (LOCAL)` + `HASH EXCHANGE` + `HASH AGGREGATE (GLOBAL)`. Scalar aggregates, grouping sets, DISTINCT, and queries with unsupported aggregates stay Single.

**Not-expected changes:** Scan/join/top-n/window shapes unchanged from Phase 3.

**Correctness validation (hard gate):**
- `cargo test` green.
- TPC-DS 99 queries pass `sql-tests --suite tpc-ds --mode verify` (results match FE).
- No panics, optimizer timeouts, or cancelled fragments.

**Observed metrics (recorded, not gated):**
- Count of queries switching from `SINGLE` to `LOCAL+GLOBAL`.
- `HASH EXCHANGE` vs `GATHER EXCHANGE` ratio change.
- Summary diff against the Phase 3 snapshot.

## 5. Aggregate Function Support Matrix

| Function | Strategy | Local | Global | Intermediate type |
|---|---|---|---|---|
| `sum(i32)` | Simple | `sum` | `sum` | `i64` |
| `sum(i64)` | Simple | `sum` | `sum` | `i64` |
| `sum(f64)` | Simple | `sum` | `sum` | `f64` |
| `sum(Decimal)` | Simple | `sum` | `sum` | widened `Decimal` |
| `count(*)` | Remap | `count(*)` | `sum` | `i64` |
| `count(expr)` | Remap | `count(expr)` | `sum` | `i64` |
| `min(T)` | Simple | `min` | `min` | `T` |
| `max(T)` | Simple | `max` | `max` | `T` |
| `avg(T)` | Composite | `(sum, count)` | `sum(sum) / sum(count)` | `(T_sum, i64)` |
| `bitmap_union(b)` | Simple | `bitmap_union` | `bitmap_union` | `bitmap` |
| `bitmap_union_count(b)` | Remap | `bitmap_union` | `bitmap_union_count` | `bitmap` |
| others | NonSplittable (default) | — | — | — |

Non-splittable examples (deferred): `approx_count_distinct`, `percentile_approx`, `hll_union`, `stddev`, `variance`, any `DISTINCT` aggregate.

## 6. Verification Strategy

Three layers of validation; every phase passes all applicable layers before merge.

**Layer 1 — Unit tests (`cargo test`).**
Required green at every phase boundary. New code (PhysicalTopN, AggregateRewriter, SplitTwoPhaseAggRule) ships with unit tests covering construction, cost, property derivation, and rewriter correctness.

**Layer 2 — EXPLAIN snapshot diff.**
Phase 0 baseline: snapshots saved during initial TPC-DS comparison (99 FE plans + 99 standalone plans) live in `/tmp/novarocks-plan-compare/`. Each phase regenerates standalone snapshots and diffs against the prior phase. Unexpected diff → investigation before merge. This is the primary guard for Phase 1 / Phase 2 where functional behavior is unchanged.

**Layer 3 — End-to-end execution (`sql-tests --suite tpc-ds --mode verify`).**
Runs after Phase 1, Phase 2.3, and Phase 4. TPC-DS results must match the FE reference. The four multi-statement query files (q14, q23, q24, q39) contain multiple SELECT statements in one file; the `mysql` CLI used for ad-hoc EXPLAIN snapshots only returns the first statement's plan. The `sql-test-runner` executes all statements in each file, so end-to-end verification covers every statement.

### 6.1 Per-Phase Regression Matrix

| Phase | Baseline | Expected plan changes | Unexpected-change response |
|---|---|---|---|
| Phase 1 (TopN) | Current standalone EXPLAIN | `SORT + LIMIT` → `TOP-N`; nothing else changes | Investigate and block merge |
| Phase 2.1 (join commutativity) | Phase 1 EXPLAIN | `eq_conditions` ordering in affected joins | Any scan reorder, any new join: investigate |
| Phase 2.2 (window grouping) | Phase 2.1 EXPLAIN | Multi-window queries gain explicit `PhysicalSort` between groups | Any change to non-window queries: investigate |
| Phase 2.3 (nullability widening) | Phase 2.2 EXPLAIN | No EXPLAIN-visible change; `output_columns.nullable` moves earlier in pipeline | Any text change: investigate |
| Phase 3 (rewriter infra) | Phase 2.3 EXPLAIN | None — infrastructure only | Any change: investigate |
| Phase 4 (two-phase agg) | Phase 3 EXPLAIN | Eligible group-by aggregates gain LOCAL+GLOBAL+shuffle; others unchanged | Non-aggregate node changes, or scalar/grouping-set/DISTINCT aggregates changing: investigate |

### 6.2 Known Test Blind Spots

- **Multi-statement query files (q14/q23/q24/q39):** the FE `mysql` client only EXPLAINs the first statement. Phase boundary EXPLAIN diffs cover only the first statement of these four. End-to-end execution via the test runner still covers every statement.
- **Data scale:** TPC-DS data is at roughly SF=1 (e.g., `customer=90000`, `store_returns=277502`). At this scale, BROADCAST may always beat SHUFFLE, so two-phase aggregation's performance advantage is not fully exercised. This round measures correctness only, not performance.

## 7. Risk Mitigation

**Per-phase isolation.** Each phase is a self-contained commit (or small commit chain). A regression in Phase N can be reverted without unwinding earlier phases.

**No feature flag.** Phased landing gives us the same flexibility as a flag with less code debt. If Phase 4 exposes a class of queries that regress functionally, the fallback is to tighten eligibility in `SplitTwoPhaseAggRule` (adding a `can_split` predicate) rather than toggling a flag at runtime.

**Cost-model escape hatch.** If Phase 4 cost estimates turn out to favor two-phase in cases where it regresses performance (unlikely given correctness-first goal, but possible), the fallback is to adjust the Local/Global cost constants in `src/sql/cascades/cost.rs`. These constants are centralized; no scattered tuning.

**Phase independence.**
- Phase 1 and Phase 2 sub-items have no inter-dependencies; they could be parallelized across commits if needed.
- Phase 3 is strictly a prerequisite for Phase 4.
- Phase 2.3 (nullability widening) is the only Phase 2 item that touches `PhysicalPlanNode.output_columns`; it must land before Phase 4 to avoid fragment builder inconsistencies when agg splits run over OUTER-JOIN inputs.

## 8. Deferred Items (Not In This Round)

- **Legacy `src/sql/optimizer/` merge into cascades.** Predicate pushdown, join reorder, column pruning currently run as a pre-pass before cascades. Merging into cascades rules removes the duplicate statistics derivation and unifies the pipeline.
- **`src/standalone/engine.rs` decomposition.** 4538-line catch-all mixing catalog, DDL, DML, Iceberg, Parquet I/O, literal evaluation, and query dispatch. Separate spec.
- **BROADCAST vs SHUFFLE join distribution tuning.** Current threshold is a fixed 500K rows; dynamic decision based on table stats is a separate improvement.
- **MERGING-EXCHANGE.** Distributed merge-sort for TOP-N across fragments. Builds on Phase 1's PhysicalTopN.
- **DISTINCT aggregate four-phase pattern.** Follows StarRocks's `DISTINCT_LOCAL` / `DISTINCT_GLOBAL` model; not needed for the non-DISTINCT TPC-DS queries in this round.
- **Two-phase aggregation over `PhysicalRepeat`.** Grouping sets / ROLLUP / CUBE; requires two-phase over expanded grouping keys.
- **Physical expression IR separate from `TypedExpr`.** Larger expression-system refactor.

## 9. Appendix: Reference Code Locations

| Concern | NovaRocks (current) | StarRocks FE (reference) |
|---|---|---|
| Aggregate split rule | `src/sql/cascades/rules/implement.rs:499–536` (single only, two-phase marked "deferred") | `rule/transformation/SplitTwoPhaseAggRule.java` |
| AggType enum | `src/sql/cascades/operator.rs` (`AggMode: Single/Local/Global`) | `operator/AggType.java` (LOCAL/GLOBAL/DISTINCT_LOCAL/DISTINCT_GLOBAL) |
| Physical TopN | not present; fragment_builder sets `use_top_n=true` on SORT_NODE | `operator/physical/PhysicalTopNOperator.java` |
| Distribution property | `src/sql/cascades/property.rs` (`DistributionSpec: Any/Gather/HashPartitioned`) | `base/DistributionSpec.java` (Any/Replicated/Gather/Hash/RoundRobin) |
| Property enforcement | `src/sql/cascades/search.rs:162–232, 505–542` | `task/EnforceAndCostTask.java` |
| Fragment translation | `src/sql/cascades/fragment_builder.rs` (2465 lines, mixed responsibilities) | `sql/plan/PlanFragmentBuilder.java` (~3000 lines, pure translation) |
| Cost model | `src/sql/cascades/cost.rs` | `cost/CostModel.java` |
| Join commutativity | `src/sql/cascades/rules/` (missing eq_conditions normalization) | `rule/join/JoinCommutativityRule.java` |
