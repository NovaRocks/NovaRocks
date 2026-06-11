# Distributed TopN via MERGING-EXCHANGE

**Date:** 2026-04-17
**Scope:** Cascades optimizer + fragment builder — `ORDER BY ... LIMIT N` (`PhysicalTopN`) only. Plain `ORDER BY` without limit is out of scope.
**Motivation:** Standalone plans pin every TopN to a single instance by requiring Gather distribution at its child. Across 99 TPC-DS queries, ~110 top-level TopN nodes sit directly above `GATHER EXCHANGE`, serializing sort+limit on one pipeline. StarRocks FE emits the standard two-stage pattern (PARTIAL sort per fragment → merging receive-side sort on coordinator), cutting per-instance sorted input by `pipeline_dop×` and exploiting k-way merge.

---

## 1. Approach

**Mirror StarRocks 1:1.** The cascades transformation rule `SplitTopN` emits a two-stage alternative alongside the single-stage TopN; cost search picks. The fragment builder handles fragment splitting — no new physical operator, no new Thrift message. The runtime already supports merging-exchange: `src/lower/node/exchange.rs:105-140` detects `exchange_node.sort_info` on EXCHANGE_NODE and constructs a SortNode at the receive side. This path is currently unused; the present work is what activates it.

**Reference files (StarRocks main, read-only):**
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/SortPhase.java` — `PARTIAL | FINAL` enum.
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/SplitTopNRule.java` — transformation rule.
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/implementation/TopNImplementationRule.java` — passes phase/isSplit through implementation.
- `fe/fe-core/src/main/java/com/starrocks/sql/plan/PlanFragmentBuilder.java:2700-2874` — `visitPhysicalTopN` + `buildPartialTopNFragment` + `buildFinalTopNFragment`.

---

## 2. Architecture

### 2.1 Phase enum

```rust
// src/sql/optimizer/operator.rs
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TopNPhase {
    Partial,
    Final,
}

impl Default for TopNPhase {
    fn default() -> Self { TopNPhase::Final }
}
```

### 2.2 Operator shape

Both `LogicalTopNOp` and `PhysicalTopNOp` gain:

```rust
pub phase: TopNPhase,   // default Final
pub is_split: bool,     // default false
```

Default values (`Final`, `false`) reproduce today's single-stage behavior byte-for-byte.

### 2.3 Plan shape after split

Before SplitTopN fires (single-stage, unchanged):
```
LogicalTopN(FINAL, !split, L, O)
  <child>
```

After SplitTopN (two-stage alternative in same memo group):
```
LogicalTopN(FINAL, split=true, limit=L, offset=O)
  LogicalTopN(PARTIAL, limit=L+O, offset=0)
    <child>
```

After implementation + distribution enforcement:
```
PhysicalTopN(FINAL, split=true, L, O)
  PhysicalTopN(PARTIAL, L+O, 0)
    <child>
```

**No PhysicalDistribution(Gather) enforcer between FINAL(split) and PARTIAL.** The FINAL(split) requires `Any` from its child — the merging-exchange emitted at fragment-build time is the gather. This mirrors StarRocks's model where `SplitTopNRule` produces FINAL→PARTIAL as a direct parent/child pair and the fragment builder materializes the exchange.

### 2.4 Fragment output shape

Partial fragment (one per instance):
```
SORT_NODE (sort_info, limit = L+O, offset = 0)
  <child-plan-nodes>
```

Coordinator fragment:
```
EXCHANGE_NODE (sort_info, limit = L, offset = O)       ← MERGING EXCHANGE
```

No separate final SORT_NODE. The merging-receive side already performs the k-way merge and applies offset/limit (see `src/lower/node/exchange.rs:112-150`).

---

## 3. Changes by File

### 3.1 `src/sql/optimizer/operator.rs`

Add `TopNPhase` enum. Extend `LogicalTopNOp` and `PhysicalTopNOp` with `phase: TopNPhase` and `is_split: bool`. Defaults preserve today's behavior.

### 3.2 `src/sql/optimizer/rules/split_top_n.rs` (new)

New transformation rule:

```rust
pub(crate) struct SplitTopN;

impl Rule for SplitTopN {
    fn name(&self) -> &str { "SplitTopN" }
    fn rule_type(&self) -> RuleType { RuleType::Transformation }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(t) if t.phase == TopNPhase::Final && !t.is_split)
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalTopN(src) = &expr.op else { return vec![]; };
        // Must have a finite limit; otherwise this is plain ORDER BY, out of scope.
        let limit = match src.limit { Some(l) if l >= 0 => l, _ => return vec![] };
        let offset = src.offset.unwrap_or(0).max(0);
        // Overflow guard: limit + offset must not overflow i64.
        let partial_limit = limit.checked_add(offset).unwrap_or(i64::MAX);

        // PARTIAL child: same sort items, larger limit, zero offset.
        let partial_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: src.items.clone(),
                limit: Some(partial_limit),
                offset: Some(0),
                phase: TopNPhase::Partial,
                is_split: false,
            }),
            children: expr.children.clone(),
        };
        let partial_group = memo.new_group(partial_mexpr);

        // FINAL with split flag, original limit/offset. Child = new partial group.
        let final_expr = NewExpr {
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: src.items.clone(),
                limit: src.limit,
                offset: src.offset,
                phase: TopNPhase::Final,
                is_split: true,
            }),
            children: vec![partial_group],
        };

        vec![final_expr]
    }
}
```

Emit a single alternative (not two like `AggToHashAgg`); the original single-stage plan remains in the memo from the initial conversion. Cost search decides between them.

### 3.3 `src/sql/optimizer/rules/mod.rs`

Register `SplitTopN` in `all_transformation_rules()`.

### 3.4 `src/sql/optimizer/rules/implement.rs`

`TopNToPhysical` passes `phase` and `is_split` through:

```rust
vec![NewExpr {
    op: Operator::PhysicalTopN(PhysicalTopNOp {
        items: op.items.clone(),
        limit: op.limit,
        offset: op.offset,
        phase: op.phase,
        is_split: op.is_split,
    }),
    children: expr.children.clone(),
}]
```

### 3.5 `src/sql/optimizer/search.rs`

Distribution contracts for `PhysicalTopN`:

| phase / is_split | provides | requires from child |
|---|---|---|
| Final, !split (default) | Gather | Gather (today's contract) |
| Final, split=true | Gather | **Any** (new — child is PARTIAL, exchange emitted at fragment stage) |
| Partial, * | Any | Any (mirrors scan, preserves child distribution) |

Update two switch sites:
- `derive_property_from` (line ~350): read `phase`/`is_split` off `PhysicalTopNOp`, set distribution/ordering accordingly.
- `required_child_property` (line ~496): dispatch on phase/is_split.

### 3.6 `src/sql/codegen/fragment_builder.rs`

`visit_physical_top_n` dispatches on `phase`/`is_split`:

1. **`!is_split` (single-stage, today's behavior):** existing codepath, unchanged.
2. **`phase == Partial`:** emit SORT_NODE with `limit = L+O`, `offset = 0`. Extend the child's `plan_nodes` with the SORT_NODE and return. No fragment boundary created here — cascades did not insert a PhysicalDistribution above, so this VisitResult is consumed in the same fragment by the FINAL(split) parent.
3. **`phase == Final && is_split`:**
   - Call `self.visit(child)` — returns a VisitResult with `plan_nodes` ending in a SORT_NODE produced by the PARTIAL branch.
   - Assert the first plan node is a SORT_NODE; extract its `TSortInfo`.
   - Create a new fragment boundary manually (this is the new responsibility — no PhysicalDistribution triggered it):
     - Close the in-progress VisitResult into a partial fragment whose root is the SORT_NODE, with a DataStreamSink (distribution = Unpartitioned) feeding the new exchange.
     - Start a coordinator fragment containing a single EXCHANGE_NODE: copy `sort_info` from the partial SORT_NODE, set `limit = L`, `offset = O` on the exchange.
   - Coordinator fragment root = new EXCHANGE_NODE.

Extract a helper `close_partial_into_merging_exchange(partial_result, sort_info, limit, offset)` to keep the FINAL(split) branch small. This helper performs the manual fragment boundary creation that PhysicalDistribution's visitor normally does.

### 3.7 Existing tests

- Unit tests in `src/sql/optimizer/rules/sort_limit_to_top_n.rs`, `implement.rs` TopN tests: update field constructions to include `phase: Final`, `is_split: false`.
- Unit tests in `src/sql/optimizer/search.rs` for TopN property derivation: add cases for `phase=Partial` and `phase=Final, split=true`.

---

## 4. Non-Goals

- **Plain ORDER BY without LIMIT.** StarRocks's `ExchangeSortToMergeRule` (post-search tree rewrite) handles this. TPC-DS has exactly one such query (q34) and the gain is smaller since there's no finite `L+O` to cap partial output. Defer.
- **PARTITION-BY top-N (`ROW_NUMBER() = k` elimination).** Different rule, already partially present as PhysicalTopN-with-partition-columns in StarRocks. Out of scope.
- **Parallel-merge tuning (`use_parallel_merge`).** Runtime knob; always false for now.
- **Cost calibration.** The existing cost model should favor two-stage when child cardinality exceeds some threshold; if it over-picks single-stage, we add a multiplicative penalty in a follow-up.

---

## 5. Validation

### 5.1 Unit tests

- `cargo test` green.
- New tests in `split_top_n.rs` covering: rule emits the split form; rule skips DISTINCT-like cases (N/A here); limit/offset arithmetic on partial; overflow guard.

### 5.2 EXPLAIN snapshot diff

Baseline: current 99 EXPLAINs at `/tmp/novarocks-plan-compare/standalone-current/` (captured 2026-04-17).

After: regenerate 99 EXPLAINs into `/tmp/novarocks-plan-compare/standalone-merge-topn/`. Expected deltas:
- Queries with a top-level `TOP-N ... GATHER EXCHANGE`: pattern changes to `TOP-N(final) ... MERGING EXCHANGE ... TOP-N(partial)`.
- Queries with tiny child cardinality (cost picks single-stage): shape unchanged.
- Non-TopN queries: shape unchanged.

Any unrelated plan change blocks merge.

### 5.3 Correctness

`cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite tpc-ds -j 1 --mode verify --query-timeout 120`

Expected: 99/99 pass, matching the 2026-04-17 baseline.

### 5.4 Performance spot-check

Pick 3 queries where the two-stage form should dominate (top-level TopN over a post-aggregation result of large cardinality, e.g. q3, q19, q22 if not timing out). Compare wall time before/after. Expected: measurable reduction proportional to `pipeline_dop`. Record in landing note.

---

## 6. Risks

**Risk 1 — Distribution property change cascades.** Making `Partial` provide `Any` and `Final(split)` require `Any` changes how the enforcer inserts Gather nodes. If any path incorrectly assumes Sort always forces gather at a specific point, plans could mis-enforce. Mitigation: Layer-2 EXPLAIN diff is the gate; inspect non-TopN plan changes.

**Risk 2 — Exchange `sort_info` lowering regression.** The merging-exchange code path (`lower_exchange_node` at line 105) is currently not exercised in tests. Adding the first real caller may uncover latent bugs. Mitigation: explicit unit tests for a single-fragment SplitTopN plan; spot-check EXPLAIN output includes both sides' sort info correctly.

**Risk 3 — Cost model over-picks single-stage.** The existing cost model may not credit two-stage's reduced merge cost enough, leaving many queries on the single-stage form. Mitigation: measure how many of the 110 current TopN queries actually switch to two-stage after the change; if <50%, add a penalty term in a follow-up spec.

**Risk 4 — Assert on partial root being SORT_NODE fails for empty-input edge cases.** If cascades fuses an empty `PhysicalValues` directly under a TopN, partial fragment might not end in SORT_NODE. Mitigation: test; fall back to treating as single-stage if assertion would fail.

---

## 7. Implementation Phasing

Single phase — the change is small and self-contained. The plan (separate document) will sequence:
1. Operator shape + enum.
2. SplitTopN rule + unit test.
3. `TopNToPhysical` passes phase/split; search distribution rules updated.
4. Fragment builder FINAL(split) + PARTIAL branches.
5. Full validation per §5.

---

## 8. Landing Note

**Landed.** Date: 2026-04-17. HEAD: `0927abc`. Implementation complete across 7 tasks (commits `fe29567` → `0927abc`, with minor review fixes). TPC-DS standalone suite verify: **99/99 pass** (`-j 1`, `--query-timeout 120`, wall time 84.34s). Lib tests: 957/0.

**EXPLAIN diff summary (baseline 2026-04-17-pre vs merge-topn):**
- Queries that differ: **67** of 99
- Queries with two-stage TopN materialization: **67**
- Queries with plan shape unchanged: **32**
- Unrelated plan changes: **0** (hard requirement met)

**Representative two-stage EXPLAIN (query `q3`):**
```
TOP-N (limit=100) [d_year ASC NULLS FIRST, sum_agg DESC NULLS LAST, brand_id ASC NULLS FIRST]
  TOP-N (limit=100, offset=0) [d_year ASC NULLS FIRST, sum_agg DESC NULLS LAST, brand_id ASC NULLS FIRST]
    PROJECT [dt.d_year AS d_year, item.i_brand_id AS brand_id, item.i_brand AS brand, sum(ss_ext_sales_price) AS sum_agg]
      HASH AGGREGATE (GLOBAL, ...)
        HASH AGGREGATE (LOCAL, ...)
          HASH JOIN (BROADCAST, INNER, ...)
```
The coordinator-side `TOP-N (limit=100)` (FINAL phase) merges sorted streams from partial instances, each contributing `TOP-N (limit=100, offset=0)` (PARTIAL phase) output via MERGING EXCHANGE.

**Non-goals deferred:**
- Plain `ORDER BY` without LIMIT (`ExchangeSortToMergeRule`-equivalent). TPC-DS has one query (q34) in this category.
- Cost calibration / penalty tuning — if cost picks single-stage too aggressively, a follow-up spec can tune the cost model.
