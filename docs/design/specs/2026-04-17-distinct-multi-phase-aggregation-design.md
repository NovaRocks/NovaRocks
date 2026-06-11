# DISTINCT Aggregation — Multi-Phase (3/4-phase) Execution

**Date:** 2026-04-17
**Scope:** Cascades optimizer + fragment builder — aggregation with a single DISTINCT column.
  - **In-scope:** one DISTINCT column (any number of DISTINCT aggregate calls on that same column); mix with arbitrary non-DISTINCT aggregates; with or without GROUP BY.
  - **Out-of-scope:** multi-column DISTINCT args (`count(distinct a, b)`), multiple DISTINCT aggs on different columns (`count(distinct a), count(distinct b)`), `multi_distinct_count` / `fused_multi_distinct` low-NDV rewrite.
**Motivation:** All four TPC-DS queries with DISTINCT aggregation (q16, q28, q94, q95) currently compile to `HASH AGGREGATE (SINGLE)` because `AggToHashAgg` explicitly bails on any `AggregateCall.distinct`. This serializes dedup + aggregation to a single instance. StarRocks handles DISTINCT via a 3-phase (group-by) or 4-phase (scalar) pattern with `DISTINCT_LOCAL`/`DISTINCT_GLOBAL` modes. This spec ports that pattern 1:1.

---

## 1. Approach

**Mirror StarRocks 1:1.** A new cascades transformation rule `SplitDistinctAgg` emits a multi-phase alternative alongside the existing `PhysicalHashAggregate(Single)` (which `AggToHashAgg` always emits). Cost search picks. No backend changes — the four new `PhysicalHashAggregate` phases use `need_finalize` and `is_merge_agg` flags that the existing aggregate executor already supports.

**Reference files (StarRocks main, read-only):**
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/AggType.java` — enum `{ LOCAL, GLOBAL, DISTINCT_LOCAL, DISTINCT_GLOBAL }` with phase docstrings that are the canonical contract.
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/SplitAggregateRule.java` (+ `SplitMultiPhaseAggRule` variants) — phase-splitting logic.
- `fe/fe-core/src/main/java/com/starrocks/sql/plan/PlanFragmentBuilder.java::visitPhysicalHashAggregate` — fragment compilation for each phase.

---

## 2. Architecture

### 2.1 `AggMode` extension

```rust
// src/sql/optimizer/operator.rs
pub(crate) enum AggMode {
    Single,           // 1-phase (today's DISTINCT fallback)
    Local,            // 2-phase LOCAL (non-DISTINCT or DISTINCT first phase)
    Global,           // 2-phase GLOBAL (final merge)
    DistinctLocal,    // 4-phase third phase (scalar-only)
    DistinctGlobal,   // 3/4-phase shuffle-dedup phase
}
```

`Default` stays `Single`. Existing 2-phase rule (`AggToHashAgg`) continues to use `Local`/`Global` unchanged.

### 2.2 Plan shape — 3-phase (DISTINCT + GROUP BY)

Source query:
```sql
SELECT g, count(distinct x), sum(a) FROM t GROUP BY g;
```

Logical input to cascades:
```
LogicalAggregate(group_by=[g], aggs=[count(distinct x), sum(a)])
  <child>
```

`SplitDistinctAgg` alternative (single NewExpr emitted; cost search picks among this and `AggToHashAgg`'s `Single`):
```
PhysicalHashAggregate(GLOBAL, group_by=[g], aggs=[count(x), merge(sum(a))])
  PhysicalHashAggregate(DISTINCT_GLOBAL, group_by=[g, x], aggs=[merge(sum(a))])
    PhysicalHashAggregate(LOCAL, group_by=[g, x], aggs=[sum(a)])
      <child>
```

After distribution enforcement the cascades plan additionally contains `PhysicalDistribution(Hash([g, x]))` between LOCAL and DISTINCT_GLOBAL (or `Hash([g])` if we choose to shuffle only by g — see §2.5 on distribution choice), and `PhysicalDistribution(Hash([g]))` between DISTINCT_GLOBAL and GLOBAL is normally elided because DISTINCT_GLOBAL already outputs data partitioned by a superset of g.

### 2.3 Plan shape — 4-phase (DISTINCT, no GROUP BY)

Source query:
```sql
SELECT count(distinct x), sum(a) FROM t;
```

Logical input:
```
LogicalAggregate(group_by=[], aggs=[count(distinct x), sum(a)])
  <child>
```

`SplitDistinctAgg` alternative:
```
PhysicalHashAggregate(GLOBAL, scalar, aggs=[sum(partial_count), merge(sum(a))])
  PhysicalHashAggregate(DISTINCT_LOCAL, scalar, aggs=[count(x), merge(sum(a))])
    PhysicalHashAggregate(DISTINCT_GLOBAL, group_by=[x], aggs=[merge(sum(a))])
      PhysicalHashAggregate(LOCAL, group_by=[x], aggs=[sum(a)])
        <child>
```

Enforced distributions:
- LOCAL → DISTINCT_GLOBAL: `Hash([x])` exchange.
- DISTINCT_GLOBAL → DISTINCT_LOCAL: `Any` (each instance runs DISTINCT_LOCAL on its local DISTINCT_GLOBAL output — no exchange).
- DISTINCT_LOCAL → GLOBAL: `Gather` exchange (scalar final aggregate).

### 2.4 Phase semantics (output per phase)

For a DISTINCT query with `group_by=G` (possibly empty), DISTINCT column `x`, non-DISTINCT aggs `A = [f_i(col_i)]`:

| Mode | group_by | produces (per input row of that phase) | intermediate state |
|---|---|---|---|
| LOCAL | `G + {x}` | `(g…, x, update(f_i, col_i))` for each distinct `(g, x)` in the local partition | update-serialize |
| DISTINCT_GLOBAL | `G + {x}` | `(g…, x, merged_f_i)` for each GLOBAL distinct `(g, x)` after shuffle | merge-serialize |
| DISTINCT_LOCAL (4-phase only) | `∅` | `(count(x), merged_f_i)` per local instance — counts the distinct `x`'s this instance saw after DISTINCT_GLOBAL | serialize |
| GLOBAL (3-phase) | `G` | `(g…, count(x), finalize(merged_f_i))` — final output | finalize |
| GLOBAL (4-phase) | `∅` | `(sum(partial_count), finalize(merged_f_i))` | finalize |

**Correctness sketch (mixed DISTINCT + non-DISTINCT aggs):** In LOCAL we group by `(G, x)`, so each non-DISTINCT agg like `sum(a)` computes the per-`(g, x)` sum. In DISTINCT_GLOBAL we merge those per-`(g, x)` partial states, yielding a single globally-merged `(g, x, sum(a))` row per distinct `(g, x)` pair. When the next phase rolls up to `G` (3-phase) or scalar (4-phase), summing those per-`(g, x)` `sum(a)` values across all `x` for a given `g` reproduces `sum(a)` over all rows with that `g` — because each original row contributed once to exactly one `(g, x)` bucket in LOCAL.

### 2.5 Distribution choice for DISTINCT_GLOBAL input

Two options for shuffling before DISTINCT_GLOBAL in the 3-phase case (with group-by `g`):

- **Hash(g+x)**: co-locates both g and x on one instance. Subsequent GLOBAL (shuffled by g) can be elided to `Any`.
- **Hash(g)**: enough to co-locate dedup per `g`. Requires the DISTINCT_GLOBAL operator to still group by `(g, x)` but over fewer instances.

StarRocks uses **Hash(g+x)** (actually in code: shuffle by `g`, but since DISTINCT_GLOBAL groups by `(g,x)` the duplicate-elimination is still correct). The practical benefit of Hash(g+x) is load balancing when `g` has skewed cardinality; the downside is if `g` already has high NDV, Hash(g+x) over-shards.

**Decision: Hash(g+x)** — mirrors StarRocks's default and maximizes parallelism for low-g-NDV cases. If profiling shows the over-sharding concern, tune in a follow-up.

For 4-phase (no group-by): always Hash([x]) between LOCAL and DISTINCT_GLOBAL.

---

## 3. Changes by File

### 3.1 `src/sql/optimizer/operator.rs`

Extend the `AggMode` enum with `DistinctLocal` and `DistinctGlobal` variants. The enum is used only via `matches!` in the optimizer and via pattern-matching in fragment builder, so adding variants does not auto-propagate; callers must be extended explicitly (checked by `cargo build`).

### 3.2 `src/sql/optimizer/rules/split_distinct_agg.rs` (new)

New transformation rule matching `LogicalAggregate` where at least one `AggregateCall.distinct == true`. Guard:

```rust
fn matches(&self, op: &Operator) -> bool {
    matches!(op, Operator::LogicalAggregate(agg) if agg.aggregates.iter().any(|a| a.distinct))
}
```

In `apply`:
1. Partition `agg.aggregates` into `distinct_calls` (where `call.distinct == true`) and `non_distinct_calls`.
2. Extract the DISTINCT column from `distinct_calls[0].args`. Support only the case where `distinct_calls[0].args.len() == 1` and it's a simple column reference or expression — multi-column DISTINCT args (`count(distinct a, b)`) return empty (rule skips; cost will use `Single`).
3. Verify **all** `distinct_calls` share the same single-column DISTINCT arg (by structural equality of the `TypedExpr`). If not, skip.
4. Branch on `agg.group_by.is_empty()`:
   - Empty → build the 4-phase chain (LOCAL → DISTINCT_GLOBAL → DISTINCT_LOCAL → GLOBAL).
   - Non-empty → build the 3-phase chain (LOCAL → DISTINCT_GLOBAL → GLOBAL).
5. Each non-root phase becomes a new `MExpr` inserted into the memo via `memo.new_group`; the root phase is the returned `NewExpr`.

The rule always emits **exactly one** alternative (the multi-phase root). `AggToHashAgg` has already emitted the `Single` alternative for the same group, so cost search picks between `Single` and the multi-phase form naturally.

Rule signature detail: construction of each phase needs the original `aggregates` vector transformed — each phase gets a specific subset of aggregate calls with specific merge/non-merge semantics. Keep this synthesis inside the rule; the fragment builder is kept mode-driven only (it reads the `AggMode` and knows how to compile each phase's aggregate calls from the phase's input scope).

### 3.3 `src/sql/optimizer/rules/mod.rs`

Register `split_distinct_agg::SplitDistinctAgg` in `all_transformation_rules()` after `SplitTopN`.

### 3.4 `src/sql/codegen/fragment_builder.rs::visit_hash_aggregate`

Current code dispatches on `Single` / `Local` / `Global`. Extend the dispatch to five arms:

- `Single`: unchanged.
- `Local`: unchanged (update-serialize path).
- `DistinctGlobal`: near-copy of `Global`'s `is_merge_agg=true + need_finalize=false` compilation, but:
  - `group_by` includes the DISTINCT column as an extra key — fragment builder treats it exactly like any other grouping key (no special handling needed).
  - The aggregate calls list does **not** contain a function for the DISTINCT column (it's passing through as a grouping key). Only the non-DISTINCT aggs are merged.
- `DistinctLocal` (4-phase only): 
  - Scalar (no group-by).
  - For the DISTINCT column: emit a plain `count(x)` (not a merge; a regular count over the DISTINCT_GLOBAL output rows, where each row represents one globally-distinct `x`).
  - For non-DISTINCT aggs: merge (the intermediate state continues through).
  - `need_finalize=false` because GLOBAL downstream will finalize.
- `Global`: existing code when `mode == Global` — needs a minor update so that when the parent was constructed by `SplitDistinctAgg` (3-phase: `Global` over `DistinctGlobal`, 4-phase: `Global` over `DistinctLocal`), the aggregate expressions are compiled as merge functions over the child scope's intermediate columns. This is exactly what `compile_merge_aggregate_call` already does. The change is: identify which aggregate calls are "finalize a distinct count" vs "merge a non-distinct agg" — this is determined at rule-construction time by setting the appropriate `AggregateCall.name` (e.g., rewriting `count(distinct x)` to `count(x)` for DISTINCT_LOCAL and to `sum(partial_count)` for GLOBAL in 4-phase).

**Implementation note — aggregate call synthesis:** the rule `SplitDistinctAgg.apply()` is responsible for producing each phase's `AggregateCall` list such that the fragment builder's existing `AggMode`-driven compilation produces correct Thrift. The rule synthesizes new `AggregateCall` objects with the correct `name`, `args`, `distinct=false` (!), and `result_type` for each phase. Importantly, DISTINCT calls like `count(distinct x)` are **only** `distinct=true` at the LOGICAL level; every `PhysicalHashAggregate` operator carries `distinct=false` calls. The phase-specific names encode the phase semantic:
- LOCAL phase: original non-DISTINCT calls as-is; DISTINCT calls have `distinct=false` and treat `x` as a grouping key (no agg function emitted for x in LOCAL's aggregates list).
- DISTINCT_GLOBAL: merge each non-DISTINCT agg on the intermediate column; no function for x.
- DISTINCT_LOCAL: `count(x)` (regular, non-distinct), merged non-distinct aggs.
- GLOBAL (3-phase): `count(x)` (regular) — counts the DISTINCT_GLOBAL output rows per `g`; merged non-distinct aggs.
- GLOBAL (4-phase): `sum(partial_count)` on DISTINCT_LOCAL's count output; merged non-distinct aggs.

### 3.5 `src/sql/optimizer/search.rs`

Distribution contracts for the new modes:

| Mode | provides | requires from child |
|---|---|---|
| DistinctGlobal | `Hash(group_by)` (where group_by includes x in 3-phase, is `[x]` in 4-phase) | `Hash(group_by)` |
| DistinctLocal | `Any` | `Any` |

Concrete `output_properties` and `required_input_properties` arms added for each new AggMode. Existing `Local`/`Global`/`Single` arms unchanged.

### 3.6 Expression compilation (`src/sql/physical/expr_compiler.rs`)

Reuse existing helpers:
- `compile_aggregate_call_typed` — LOCAL update-serialize, and DISTINCT_LOCAL's `count(x)` (where `x` is a SlotRef to DISTINCT_GLOBAL's output).
- `compile_merge_aggregate_call` — DISTINCT_GLOBAL, DISTINCT_LOCAL, and GLOBAL phases' merge of non-DISTINCT aggs.

No new helper required for the "DistinctLocal's count(x)" case — it IS a regular non-distinct `count(x)` compiled via the existing path, where `x` is the DISTINCT_GLOBAL output slot. The rule synthesizes the `AggregateCall` for this count at plan construction.

---

## 4. Validation

### 4.1 Unit tests

New test module `src/sql/optimizer/rules/split_distinct_agg.rs::tests`:

- `fires_on_scalar_distinct` — constructs `LogicalAggregate([], [count(distinct x), sum(a)])`, asserts a 4-phase chain is emitted with correct modes and group-by sets.
- `fires_on_group_by_distinct` — `LogicalAggregate([g], [count(distinct x), sum(a)])`, asserts 3-phase chain.
- `skips_multi_column_distinct_arg` — `count(distinct a, b)`, asserts `apply()` returns empty.
- `skips_multi_distinct_different_cols` — `count(distinct a), count(distinct b)`, asserts `apply()` returns empty.
- `skips_when_no_distinct` — `count(a)` only, asserts `apply()` returns empty (rule's `matches` already filters, but defensive).
- `preserves_multiple_distinct_on_same_col` — `count(distinct x), sum(distinct x)`, asserts 4-phase (or 3-phase) chain that counts and sums x once each.

Additional fragment-builder integration tests can be skipped if TPC-DS coverage is sufficient.

### 4.2 TPC-DS suite verify

Run `sql-tests --suite tpc-ds --mode verify -j 1 --query-timeout 120`. Expect **99/99 pass**. The four DISTINCT queries (q16, q28, q94, q95) should continue to produce the same result values; their EXPLAIN plans will change from `HASH AGGREGATE (SINGLE)` to the 4-phase form.

### 4.3 EXPLAIN baseline diff

Baseline: `/tmp/novarocks-plan-compare/standalone-merge-topn/` (post-TopN spec).

After this spec lands, re-capture as `/tmp/novarocks-plan-compare/standalone-distinct/`, diff. Expected:
- q16, q28, q94, q95: plan shape changes (new HASH AGGREGATE (DISTINCT_GLOBAL) / (DISTINCT_LOCAL) lines; possibly more HASH EXCHANGEs).
- All other queries: byte-identical.

### 4.4 Hand-crafted group-by DISTINCT test

TPC-DS has no `GROUP BY g, count(distinct x)` query, so 3-phase path is not covered by suite verify. Add a targeted sql-tests case (e.g. `sql-tests/aggregate/distinct_group_by.sql`) with:

```sql
SELECT ss_store_sk, count(distinct ss_item_sk), sum(ss_quantity) 
FROM store_sales WHERE ss_sold_date_sk < 2451000 
GROUP BY ss_store_sk ORDER BY ss_store_sk LIMIT 20;
```

Verify result matches single-stage equivalent (disable the rule via `OptimizerOptions` if necessary, or use a comparison query on a known small table).

### 4.5 Performance spot-check

Compare q28 wall time before/after. q28 has 6 independent DISTINCT aggregates currently running as 6 SINGLE passes; parallelizing each to 4-phase is the biggest expected win.

Record before/after in the landing note. Optional — not a blocker.

---

## 5. Risks

**Risk 1 — Cost model picks SINGLE for all DISTINCT queries.** The existing cost model was calibrated without DISTINCT multi-phase as an option. If multi-phase is consistently priced higher than SINGLE (e.g., because multi-phase introduces more operators and exchanges), the rule's alternative is never chosen. Mitigation: after implementation, measure — if <75% of eligible queries switch to multi-phase, add a cost penalty for DISTINCT SINGLE (or a bonus for multi-phase) in a follow-up. The TopN spec noted the same risk; current adoption rate was 67/99 which is acceptable.

**Risk 2 — Enforcer inserts redundant exchanges.** DISTINCT_GLOBAL output is partitioned by `[g, x]`; for 3-phase the GLOBAL requires Hash(g). The enforcer may insert a Hash(g) re-shuffle if it doesn't recognize that Hash(g+x) is already sufficient. Mitigation: review the distribution-compatibility check in `search.rs` — if Hash([g, x]) doesn't satisfy Hash([g]) today, extend it (this may be useful beyond DISTINCT). If left as-is, the plan is correct but has an extra exchange; performance sub-optimal but not wrong.

**Risk 3 — Correctness bug in mixed DISTINCT + non-DISTINCT aggregation.** The correctness sketch in §2.4 relies on each original row contributing to exactly one `(g, x)` bucket in LOCAL. This is true iff `x` is deterministic per row, which holds for simple column refs and pure expressions. Mitigation: TPC-DS q16/q28/q94/q95 all use simple column refs as DISTINCT args; unit test covers a hand-crafted `sum(a), count(distinct x)` scalar query to verify.

**Risk 4 — `is_merge_agg` + `need_finalize` flags map incorrectly to one of the new modes.** The four new phases use different combinations. Mitigation: unit-test the fragment-builder output for each mode, asserting the resulting `TPlanNode.agg_node` has the expected `is_merge_agg` / `need_finalize` / `intermediate_tuple_id` values.

---

## 6. Non-Goals

- **Multi-column DISTINCT args** (`count(distinct a, b)`): StarRocks uses a tuple-grouping approach; defer.
- **Multiple DISTINCT on different columns** (`count(distinct a), count(distinct b)`): StarRocks uses `RewriteMultiDistinctRule` to rewrite into a UNION or `multi_distinct_count` path; defer.
- **`multi_distinct_count` / `fused_multi_distinct` low-NDV rewrite**: a different optimization path; defer.
- **q80's SINGLE-over-REPEAT case** (cost-tuning issue for grouping-sets, not DISTINCT): out of scope for this spec.
- **Skew-aware bucketization** of DISTINCT_GLOBAL: StarRocks has `GroupByCountDistinctDataSkewEliminateRule` for extreme skew; defer.

---

## 7. Implementation Phasing

Single phase; work is well-bounded (one new rule file, operator.rs enum extension, fragment-builder 5-branch dispatch, search.rs contracts, tests). The plan document (separate) will sequence it as:

1. `AggMode` enum extension + compile-driven audit of every `match AggMode` site.
2. `SplitDistinctAgg` rule + unit tests (TDD).
3. Distribution contracts for new modes in `search.rs`.
4. Fragment builder extension — DistinctGlobal branch (3-phase works end-to-end at this point).
5. Fragment builder extension — DistinctLocal branch (4-phase works end-to-end).
6. Register rule + validation (TPC-DS 99/99 + hand-crafted 3-phase case + EXPLAIN diff).

Total expected diff: ~600-800 LOC across 4-5 files. No backend changes.

---

## 8. Landing Note (2026-04-17)

Implementation completed on 2026-04-17. Key deviations from the plan above:

### 8.1 Rule registration

`SplitDistinctAgg` was registered in `all_implementation_rules()` (not `all_transformation_rules()`) in `src/sql/optimizer/rules/mod.rs`, matching the pattern for `AggToHashAgg`. This is correct: the rule emits `Physical*` alternatives and must fire during the implementation phase of Cascades search.

### 8.2 `distinct` flag on GLOBAL phase aggregate call

The spec (§3.4) says each `PhysicalHashAggregate` carries `distinct=false` calls at the physical level. In practice the implementation keeps `distinct=true` on the GLOBAL phase's first aggregate (e.g., `count(distinct x)`) so that `agg_call_display_name()` produces `"count(distinct x)"` — the key that the PROJECT node above GLOBAL resolves when compiling its output expressions. Using `distinct=false` in that slot would generate key `"count(x)"` and cause a scope-lookup miss during codegen.

This is an artefact of how `ExprScope` resolves aggregate output columns by display name: the scope key registered in `visit_hash_aggregate` is `agg_call_display_name(agg_call)` which includes the `distinct` qualifier when `call.distinct == true`. The PROJECT's `compile_typed_inner` for an `AggregateCall` expression does the same lookup. The rule's `apply_three_phase` and `apply_four_phase` therefore clone the original `first_distinct` aggregate call (with `distinct=true`) as the first element of the GLOBAL phase's `aggregates` vector, ensuring the key matches.

### 8.3 Fragment builder — unchanged

No changes were required to `fragment_builder.rs`. The existing `visit_hash_aggregate` implementation already handles `DistinctGlobal` and `DistinctLocal` modes correctly because those modes were implemented in earlier commits (commits `64d2df8` and `6fafad7`).

### 8.4 Validation results

- **TPC-DS 99/99 pass** confirmed after registering the rule. Pre-registration, q16/q28/q94/q95 failed with "column not found" due to the `distinct=false` bug described in §8.2; post-fix all 99 queries pass.
- **Aggregate suite**: `distinct_group_by_multi_phase` case added (`sql-tests/aggregate/sql/distinct_group_by_multi_phase.sql`), 1/1 pass.
- **EXPLAIN diff (q16, baseline = standalone-merge-topn):**
  - Before: `HASH AGGREGATE (SINGLE)` over the full join.
  - After: `HASH AGGREGATE (LOCAL) → HASH AGGREGATE (DISTINCT_GLOBAL) → HASH AGGREGATE (DISTINCT_LOCAL) → HASH AGGREGATE (GLOBAL)` — the 4-phase chain as specified.
  - All other 95 queries: plan unchanged (no DISTINCT aggregation, rule does not fire).
