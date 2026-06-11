# G3 — Output / Required Property Deriver Visitor

**Date**: 2026-05-21
**Roadmap ref**: [SQL Layer Roadmap §2.2](2026-05-20-sql-layer-roadmap-design.md#22-g3--output_properties-visitor-p1) (G3)
**Status**: Spec — pending implementation plan
**Scope**: `src/sql/optimizer/` only. No changes to FE-compatible plan lowering, exchange/runtime, connectors, or codegen below the fragment layer.

---

## §0 Goal

Replace the single-argument `output_properties(op)` and one-shot
`required_input_properties(op, parent_required, n)` in `src/sql/optimizer/search.rs`
with a pair of explicit **visitors** that respect Invariant 3 from the SQL layer
roadmap:

1. **`DeriveOutput`** sees the children's actual chosen-winner outputs, so
   `Broadcast` / `Colocate` hash joins (and every passthrough operator) report
   their real downstream distribution instead of `Any`.
2. **`DeriveRequired`** is symmetrised to a trait, and is extended with
   **required-property pushdown** for operators where pushdown is provably
   safe (Broadcast left side, all passthrough operators).

The result is cached on the existing `Winner` entry per `(GroupId, PhysicalPropertySet)`,
unlocking PR-F1 Phase 2.1 (Window-over-Broadcast-Join avoids a redundant `HASH EXCHANGE`)
and similar cases throughout the optimizer.

---

## §1 The unavoidable asymmetry

In a top-down Cascades search the per-node algorithm is, in order:

```
1. derive_required(op, parent_required, n)   →  child_reqs[]
2. for each i: optimize_group(child_i, child_reqs[i])  →  child_winner[i]
3. derive_output(op, [child_winner[i].output, ...])    →  provided
4. if provided.satisfies(parent_required): done; else: enforcer
```

`derive_required` runs in step 1 — **before** any child has been optimised, so
it physically cannot observe child winner outputs. The roadmap §1 Invariant 3
lists both visitors symmetrically, but the practical contract is asymmetric:

| visitor       | signature                                                            | sees children outputs? |
|---------------|----------------------------------------------------------------------|------------------------|
| `DeriveOutput`   | `(&self, children_outputs: &[&PhysicalPropertySet]) -> PhysicalPropertySet` | **yes**                |
| `DeriveRequired` | `(&self, parent_required: &PhysicalPropertySet, num_children: usize) -> Vec<PhysicalPropertySet>` | **no**                 |

This asymmetry is documented in the trait doc-comments. It is not a defect; a
two-pass model that retroactively rederives `required` after seeing winners
would multiply search cost without delivering useful pushdown that the
single-pass pushdown in §3 doesn't already achieve.

---

## §2 Trait contracts and module layout

```rust
// src/sql/optimizer/derive/mod.rs

pub(crate) trait DeriveOutput {
    /// Children outputs are the *actual* outputs of the winners chosen for
    /// each child group at this point in the search. They are NOT the
    /// `required` properties the children were asked to produce — the winner
    /// may exceed the requirement (e.g. a SHUFFLE_JOIN child asked for `Any`
    /// will report `Hash([eq_keys])`).
    fn derive_output(
        &self,
        children_outputs: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet;
}

pub(crate) trait DeriveRequired {
    /// Top-down: produces required properties for each child slot purely
    /// from (op, parent_required, num_children). Children outputs are NOT
    /// available — see §1 of the design spec for why.
    fn derive_required(
        &self,
        parent_required: &PhysicalPropertySet,
        num_children: usize,
    ) -> Vec<PhysicalPropertySet>;
}

pub(crate) fn derive_output(
    op: &Operator,
    children_outputs: &[&PhysicalPropertySet],
) -> PhysicalPropertySet;

pub(crate) fn derive_required(
    op: &Operator,
    parent_required: &PhysicalPropertySet,
    num_children: usize,
) -> Vec<PhysicalPropertySet>;

pub(crate) fn needed_enforcers(
    required: &PhysicalPropertySet,
    provided: &PhysicalPropertySet,
) -> Vec<EnforcerKind>;  // moved from search.rs
```

**Files**:

```
src/sql/optimizer/derive/
  mod.rs               // traits, dispatcher fns, needed_enforcers, passthrough helpers
  scan.rs              // PhysicalScanOp, PhysicalValuesOp, PhysicalGenerateSeriesOp
  hash_join.rs         // PhysicalHashJoinOp (Shuffle / Broadcast / Colocate)
  nest_loop_join.rs    // PhysicalNestLoopJoinOp
  hash_aggregate.rs    // PhysicalHashAggregateOp (Single / Local / Global / Distinct*)
  sort.rs              // PhysicalSortOp (top-level + analytic precursor)
  top_n.rs             // PhysicalTopNOp (Partial / Final[split])
  window.rs            // PhysicalWindowOp
  passthrough.rs       // Filter / Project / Limit / SubqueryAlias / CTEProduce / Repeat / TableFunction
  set_op.rs            // PhysicalUnionOp / PhysicalIntersectOp / PhysicalExceptOp
  cte.rs               // PhysicalCTEAnchorOp / PhysicalCTEConsumeOp
  enforcer.rs          // PhysicalDistributionOp
```

Each file implements both traits on the corresponding physical-op struct.
`passthrough.rs` exposes three helpers; passthrough operators are split into
two flavours based on whether they constrain the child's distribution (see
§3.4 for the rationale):

```rust
pub(crate) fn passthrough_output(children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
    children.first().copied().cloned().unwrap_or_else(PhysicalPropertySet::any)
}

/// Distribution-blind passthrough (Filter, Project, SubqueryAlias,
/// CTEProduce, Repeat): child's required distribution is `Any`. Any
/// mismatch with the parent's required distribution becomes an enforcer
/// placed *above* the passthrough.
pub(crate) fn passthrough_required_distribution_blind(
    _parent: &PhysicalPropertySet,
) -> Vec<PhysicalPropertySet> {
    vec![PhysicalPropertySet::any()]
}

/// Full-passthrough (Limit, TableFunction): forwards parent's required.
/// Used when the operator's correctness depends on the child satisfying
/// the parent's distribution before the operator fires (e.g. global
/// `LIMIT` requires Gather).
pub(crate) fn passthrough_required_full(parent: &PhysicalPropertySet) -> Vec<PhysicalPropertySet> {
    vec![parent.clone()]
}
```

`search.rs` deletes its local `output_properties` and `required_input_properties`
fns; the only remaining derive-related logic in `search.rs` is the dispatch
calls to `derive::`.

---

## §3 Per-operator behaviour table

Bold rows are the operators whose behaviour changes vs. today.

| Op | `derive_output` | `derive_required` | Δ |
|---|---|---|---|
| `Scan` / `Values` / `GenerateSeries` / `CTEConsume` | `Any` | `[]` | = |
| **`Filter` / `Project` / `SubqueryAlias` / `CTEProduce` / `Repeat`** (distribution-blind passthrough — see §3.4) | **child[0]** | **`[Any]`** | **★ output follows child; required is distribution-blind** |
| **`Limit` / `TableFunction`** (full-passthrough — see §3.4) | **child[0]** | `[parent_required]` | **★ output now follows child** |
| `Window` | `Hash(partition_cols)` if all PARTITION BY entries are column-refs; else `Any` | `[Hash(partition_cols)]` if non-empty; else `[Gather]` | = |
| `HashJoin` (Shuffle) | `Hash(left_eq_keys)` | `[Hash(all_eq_cols), Hash(all_eq_cols)]` | = |
| **`HashJoin` (Broadcast, preserves-left)** | **`child[0].distribution`**, ordering `Any` | `[push_or_any, Gather]` — see §3.2 | **★** |
| **`HashJoin` (Broadcast, other join types)** | `Any` | `[Any, Gather]` | ★ (only output normalised) |
| **`HashJoin` (Colocate, preserves-left)** | **`child[0].distribution`**, ordering `Any` | `[Any, Any]` (no pushdown, see §3.3) | **★** |
| **`HashJoin` (Colocate, other join types)** | `Any` | `[Any, Any]` | ★ |
| `NestLoopJoin` | `Gather` | `[Gather, Gather]` | = |
| `HashAggregate` Single, group_by non-empty | `Hash(group_by)` | `[Any]` | = |
| `HashAggregate` Single, scalar | `Gather` | `[Gather]` | = |
| `HashAggregate` Local | `Hash(group_by)` or `Gather` | `[Any]` | = |
| `HashAggregate` Global | `Hash(group_by)` or `Gather` | `[Hash(group_by)]` or `[Gather]` | = |
| `HashAggregate` DistinctLocal | (today's logic) | `[Any]` | = |
| `HashAggregate` DistinctGlobal | (today's logic) | `[Hash(group_by)]` or `[Gather]` | = |
| `Sort` (top-level ORDER BY) | `Gather` + `Required(keys)` | `[Gather]` | = |
| `Sort` (analytic precursor — `analytic_partition_exprs` non-empty) | `Hash(partition_cols)` + `Required(keys)` | `[Hash(partition_cols)]` | = |
| `TopN` Partial | `Any` + `Required(keys)?` | `[Any]` | = |
| `TopN` Final (split) | `Gather` + `Required(keys)?` | `[Any]` | = |
| `TopN` Final (single-stage) | `Gather` + `Required(keys)?` | `[Gather]` | = |
| `Union` / `Intersect` / `Except` | `Any` | `[Any; n]` | = |
| `PhysicalDistribution` (enforcer) | `spec` | `[Any]` | = |
| `CTEAnchor` | `Any` | `[Any, Any]` | = |

### §3.1 `preserves_left` predicate

```rust
matches!(
    self.join_type,
    JoinKind::Inner
        | JoinKind::LeftOuter
        | JoinKind::LeftSemi
        | JoinKind::LeftAnti
        | JoinKind::Cross
)
```

For `RightOuter` and `FullOuter` — unmatched right rows inject `NULL` into
the left-side join-key columns. Those rows all hash to the NULL bucket and
concentrate on a single instance, breaking any `Hash(left_keys)` partition
contract the left child provided.

For `RightSemi` and `RightAnti` — the broadcast build side is replicated to
every instance, so emission semantics require runtime-side deduplication of
right rows; the resulting per-instance output is not partitioned by any
function of left's keys.

We therefore return `Any` for `RightOuter`, `FullOuter`, `RightSemi`, and
`RightAnti`.

### §3.2 Broadcast required pushdown

```rust
let preserves_left = /* §3.1 */;
let left_req = if preserves_left
    && matches!(parent_required.distribution, DistributionSpec::HashPartitioned(_))
{
    PhysicalPropertySet {
        distribution: parent_required.distribution.clone(),
        ordering: OrderingSpec::Any,  // hash join doesn't preserve ordering
    }
} else {
    PhysicalPropertySet::any()
};
vec![left_req, PhysicalPropertySet::gather()]
```

Effect: when a parent requires `Hash([c0])` and the broadcast-join's left
child is itself eligible to be hash-partitioned on `c0`, the requirement is
pushed down so the left child may produce `Hash([c0])` directly — avoiding an
enforcer above the join.

### §3.3 Why Colocate does NOT push down

Colocate's invariant is: both inputs share the same bucket function. If we
push `parent_required.distribution` onto the left side, we either (a) force
the right side to also change → degrades to shuffle, or (b) violate the
colocate invariant. Either outcome defeats the purpose of selecting Colocate
during search. Returning `[Any, Any]` lets each child contribute its natural
colocate distribution; the parent verifies via `derive_output` that the
combined output indeed satisfies `parent_required`.

Pushing through Colocate is meaningful only once `HashSource` (G4) can
distinguish "colocate-bucketed hash" from "shuffle-produced hash" — left for
G4.

### §3.4 Two flavours of passthrough

The seven single-child operators that "preserve" their input split into two
distribution flavours:

| Flavour | Operators | `derive_required` |
|---|---|---|
| **Distribution-blind** | `Filter` / `Project` / `SubqueryAlias` / `CTEProduce` / `Repeat` | `[Any]` |
| **Full-passthrough** | `Limit` / `TableFunction` | `[parent_required]` |

**Why distribution-blind for Filter/Project/etc**: these operators do not
themselves constrain the distribution of their input. If the parent above
requires `Gather` (e.g. a top-level result on a single instance), forwarding
`Gather` to the child means the entire subtree runs single-instance, with
the passthrough sitting **above** a `Gather` operator that has already
serialised everything. That defeats the parallelism the passthrough could
have provided — `Project` evaluating a column rewrite is perfectly happy to
run distributed, and so are `Filter` and `SubqueryAlias`.

Returning `[Any]` instead lets the child pick the cheapest distribution
(usually whatever the underlying scan or join naturally produces); the
mismatch with the parent's required is resolved by an enforcer placed
**above** the passthrough, yielding the StarRocks-canonical shape
`Gather → Filter/Project → ...` rather than `Filter/Project → Gather → ...`.
The passthrough's own `derive_output` still follows its child (Invariant 3),
so when the child genuinely already satisfies the parent's distribution
(e.g. via subset hash partitioning) no enforcer is needed at all.

**Why full-passthrough for Limit**: a global `LIMIT 10` is only correct when
executed on a single instance. If we returned `[Any]` for Limit, the
optimizer could choose to run Limit distributed (each instance retaining 10
rows), then Gather above Limit, yielding up to `10 × N` rows in the result —
incorrect. Forwarding the parent's `Gather` to Limit's child forces the
child to deliver gathered output, and Limit operates on the single-instance
data.

**Why full-passthrough for TableFunction**: `PhysicalTableFunctionOp` covers
a heterogeneous family of UDTF-like operators with operator-specific
semantics (positional dependence, side effects, ordering sensitivity). We
keep the safe full-passthrough behaviour here; refining individual table
functions to be distribution-blind is a follow-up.

---

## §4 `SearchContext::optimize_group` rewrite

The candidate-physical-expr loop in `src/sql/optimizer/search.rs` is restructured:

```rust
for expr_idx in 0..num_physical {
    let expr = &memo.groups[group_id].physical_exprs[expr_idx];

    // (broadcast row-count guard unchanged)

    // 1. derive required for each child slot (top-down, no child visibility)
    let child_reqs = derive::derive_required(&expr.op, required, expr.children.len());

    // 2. compute own cost
    let own_stats = derive_statistics(expr, memo, &self.table_stats);
    let child_stats_vec: Vec<_> = expr.children.iter()
        .map(|&cg| stats_for_group(&memo.groups[cg], memo, &self.table_stats))
        .collect();
    let child_stats_refs: Vec<&_> = child_stats_vec.iter().collect();
    let own_cost = compute_cost(&expr.op, &own_stats, &child_stats_refs);

    // 3. optimize each child, collect winner outputs
    let mut total = own_cost;
    let mut child_outputs: Vec<PhysicalPropertySet> =
        Vec::with_capacity(expr.children.len());
    let mut feasible = true;
    for (i, &cg) in expr.children.iter().enumerate() {
        let child_cost = self.optimize_group(memo, cg, &child_reqs[i])?;
        if child_cost.is_infinite() { feasible = false; break; }
        total += child_cost;
        let cw = self.winners
            .get(&(cg, child_reqs[i].clone()))
            .expect("child just optimized; winner must be in cache");
        child_outputs.push(cw.output.clone());
    }
    if !feasible { continue; }

    // 4. derive this node's actual output from children outputs
    let child_output_refs: Vec<&PhysicalPropertySet> =
        child_outputs.iter().collect();
    let provided = derive::derive_output(&expr.op, &child_output_refs);

    // 5. enforcer (if any) and final candidate cost
    let (actual_output, enforcer_info, candidate_cost) =
        if provided.satisfies(required) {
            (provided, None, total)
        } else {
            let enforcers = derive::needed_enforcers(required, &provided);
            if enforcers.is_empty() { continue; }
            let group_stats =
                stats_for_group(&memo.groups[group_id], memo, &self.table_stats);
            let enforcer_cost: f64 = enforcers.iter()
                .map(|e| estimate_enforcer_cost(e, &group_stats))
                .sum();
            let kind = enforcers.into_iter().next().unwrap();
            (
                required.clone(),
                Some(EnforcerInfo { kind, child_props: provided }),
                total + enforcer_cost,
            )
        };

    if candidate_cost < best_cost {
        best_cost = candidate_cost;
        best_index = expr_idx;
        best_enforcer = enforcer_info;
        best_output = actual_output;
    }
}
```

Key consequences:

- **Self-recursive enforcer trick is gone.** Today, when a candidate's
  natural output doesn't satisfy `required`, search re-optimises the *same
  group* for the natural `provided` and adds an enforcer on top, relying on
  the winner cache to break the recursion. After G3, the candidate's natural
  `provided` is derived directly from children winners we just computed —
  no re-optimisation of the same group is needed. The `in_progress` cycle
  guard is **retained as defence** but does not fire in normal flow.

- **`Winner` gains an `output` field.** Construction is in one place
  (this loop) and the field is required (no `Option`), so a missed branch
  is a compile error:

  ```rust
  pub(crate) struct Winner {
      pub(crate) group_id: GroupId,
      pub(crate) expr_index: usize,
      pub(crate) cost: Cost,
      pub(crate) enforcer: Option<EnforcerInfo>,
      pub(crate) output: PhysicalPropertySet,  // new
  }
  ```

- **Cache contract**: `winners: HashMap<(GroupId, PhysicalPropertySet), Winner>`
  is unchanged in key shape. This is the "cached on the Memo group" location
  the roadmap §2.2 referred to — keyed per `(group, required)` so that
  different requireds may select different winners with different outputs.
  A pure per-group cache would be incorrect.

- **`needed_enforcers` and `estimate_enforcer_cost`** move to `derive/mod.rs`
  so the property layer owns both "what output do I have" and "what enforcer
  bridges the gap". `search.rs` is left with the Cascades search algorithm
  proper and the cost-aggregation logic.

---

## §5 Testing strategy

### §5.1 Unit tests (per-op file)

Each `derive/<op>.rs` carries `#[cfg(test)] mod tests` with:

- **Output positive cases**: at least one case per non-trivial output rule.
- **Output negative cases**: e.g. broadcast right-outer returns `Any`.
- **Required positive cases**: pushdown sites verified.
- **Required negative cases**: e.g. colocate does NOT push down.

Concrete must-have cases:

| Test name (suffix `_test` omitted) | What it asserts |
|---|---|
| `hash_join_broadcast_inner_preserves_left_distribution` | left=Hash([c0]), inner → output=Hash([c0]) |
| `hash_join_broadcast_inner_preserves_left_ordering_is_any` | left=Hash+Required → output ordering=Any |
| `hash_join_broadcast_right_outer_returns_any` | left=Hash([c0]), RightOuter → output=Any |
| `hash_join_broadcast_right_semi_returns_any` | left=Hash([c0]), RightSemi → output=Any |
| `hash_join_broadcast_full_outer_returns_any` | left=Hash([c0]), FullOuter → output=Any |
| `hash_join_colocate_inner_preserves_left` | colocate variant of the inner preserves-left case |
| `hash_join_colocate_right_outer_returns_any` | colocate variant of right-outer normalisation |
| `hash_join_broadcast_required_pushes_down_hash` | parent=Hash([c0]), preserves-left → left_req=Hash([c0]) |
| `hash_join_broadcast_required_does_not_push_gather` | parent=Gather → left_req=Any (Gather not pushed) |
| `hash_join_broadcast_required_does_not_push_right_outer` | parent=Hash([c0]), RightOuter → left_req=Any |
| `hash_join_colocate_required_returns_any_any` | parent=Hash([c0]) → both child reqs=Any |
| `passthrough_filter_output_follows_child` | child=Hash([c0]) → output=Hash([c0]) |
| `passthrough_project_output_preserves_ordering` | child=Hash+Required → output preserves both |
| `filter_required_is_distribution_blind` | parent=Gather → child_req=Any (distribution-blind) |
| `project_required_is_distribution_blind` | parent=Hash([c1]) → child_req=Any |
| `limit_required_forwards_parent` | parent=Gather → child_req=Gather (full-passthrough) |
| `passthrough_no_children_falls_back_to_any` | children=[] → output=Any (defensive) |

All existing tests in `search.rs::tests` and `search.rs::top_n_property_tests`
migrate to the corresponding `derive/<op>.rs` files. No test semantics
change; only the call site changes from `output_properties(op)` to
`op.derive_output(&[])` or the dispatcher.

### §5.2 Integration tests (`search.rs::tests`)

A new module `search.rs::tests::cascaded_derivation` adds:

- `winner_records_output_for_scan`: `scan` under `Any` → `winner.output == Any`.
- `winner_records_output_for_shuffle_join`: SHUFFLE_JOIN with eq=(c0) → `winner.output == Hash([c0])`.
- `cascaded_output_through_broadcast_join`: memo `Window[part=c0] → BroadcastJoin(inner) → SHUFFLE_JOIN(c0)` — assert the Broadcast join's winner output is `Hash([c0,...])` and no enforcer is inserted above it.
- `cascaded_output_passthrough_chain`: memo `Agg[group=c0] → Filter → Project → SHUFFLE_JOIN(c0)` — assert Agg's child requirement is satisfied through the passthrough chain without enforcer.
- `cycle_guard_still_safe`: defensive — keep `in_progress` retained and assert
  no infinite loop on a constructed CTE-like reuse case.

### §5.3 sql-test golden regression

Run `--suite optimizer --mode verify` first to expose all plan-shape changes.
Expected forms of change:

1. **Fewer `EXCHANGE` nodes above Window / Aggregate over Broadcast/Colocate joins** — good; the headline of G3.
2. **Fewer `EXCHANGE` nodes above passthrough chains over Hash-partitioned inputs** — also good; a free win from §3 ★ rows on Filter/Project/etc.
3. **No EXPLAIN changes** for queries that don't hit any ★ branch.

Suites that need verification beyond `optimizer/`:

- `window_*` (PR-F1 surface)
- `subquery_alias`
- `tpc-ds`
- `tpc-h`
- `ssb`
- `join`
- `filter`
- `sort`

Each plan-shape diff must be **reviewed manually** and explained in the PR
description (one-line bullet per case). Diffs that add `EXCHANGE` or change
operator order are bugs and must block the PR.

New plan-golden cases (under `sql-tests/optimizer/`):

- `g3_broadcast_join_output_inherits_left_hash.sql` — Window over Broadcast Inner Join keyed on the join's left distribution; asserts only one EXCHANGE in the plan.
- `g3_passthrough_chain_output_inherits_child.sql` — Aggregate above Filter+Project above Shuffle Hash Join; asserts no EXCHANGE between Aggregate and Join.
- `g3_broadcast_right_outer_does_not_inherit.sql` — Window over Broadcast Right Outer Join keyed on what would have been left's distribution; asserts EXCHANGE *is* inserted (regression guard for the right/full-outer rule).
- `g3_colocate_required_no_pushdown.sql` — Colocate hash join under a parent with `Hash([c0])` requirement; asserts no spurious shuffle.

Each uses `-- @explain_contains` / negated `@explain_contains` for the EXCHANGE
assertions, so the cases are stable against unrelated formatting changes.

### §5.4 Cargo gates

```
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo build
cargo test
```

All four must pass. No `--no-verify`.

---

## §6 Invariant compliance audit

| Invariant | Verdict | Notes |
|---|---|---|
| 1 — `ColumnId` not strings | ✅ | derive consumes the existing `ColumnId` shape throughout; no string column refs added or removed. |
| 2 — Five-layer separation | ✅ | derive sits in the **Property** layer. It does not read Thrift, does not modify the plan tree, does not select algorithms. `fragment_builder` does not call into `derive`. |
| 3 — Explicit visitor sees children | ✅ output / ⚠ required | output is fully compliant; required is asymmetric by top-down necessity, documented in §1. |
| 4 — Rules as Pattern + pure apply | ✅ | derive methods are pure functions of `(self, inputs)`; no Memo mutation, no cross-group reads. |
| 5 — Single Logical tree | n/a | G3 touches only physical-layer operators. |

---

## §7 Risks and mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| Passthrough output upgrade triggers unexpected EXPLAIN changes across many suites | medium | §5.3 full sql-test verify + manual diff review; any non-obvious diff explained in PR description bullet. |
| Broadcast right/full outer accidentally inherits left output → wrong result data | **high** | §3.1 explicit `preserves_left` predicate; §5.1 `hash_join_broadcast_right_outer_returns_any` and `..._full_outer_returns_any` unit tests; §5.3 `g3_broadcast_right_outer_does_not_inherit.sql` plan-golden. |
| Colocate required pushdown breaks colocate invariant (degrades to shuffle or wrong execution) | high | §3.3 explicit `[Any, Any]` for colocate; §5.1 `hash_join_colocate_required_returns_any_any`; §5.3 `g3_colocate_required_no_pushdown.sql`. |
| `Winner.output` field missed on some construction path | medium | Field is non-`Option` and `Winner` is constructed in exactly one place — compiler enforces. |
| Removing the "optimize same group for `provided`" trick allows infinite recursion in some CTE/Memo-reuse scenario | low | `in_progress` cycle guard retained; §5.2 `cycle_guard_still_safe`. |
| External callers of `output_properties` / `required_input_properties` outside `search.rs` | low | grep confirmed (only `search.rs` and its tests). Migration is internal. |

---

## §8 Out of scope

- **G4** — `HashSource` tag on `HashPartitioned` distinguishing Agg / Join /
  Bucket / Enforce. Once G4 lands, Colocate required pushdown becomes
  meaningful and `satisfyContainAll` can be tightened.
- **G7** — `LogicalProperty` equivalence classes and unique-column sets.
- **G5** — Pattern-based rule framework.
- **JoinReorder** interaction with broadcast output: existing assumption
  ("left child of `PhysicalHashJoinOp` is the probe side, right is build")
  is preserved unchanged. Any future change to this convention is a separate
  PR.
- **Required pushdown beyond `Hash(...)`**: only `HashPartitioned` is pushed
  down through Broadcast left side. `Gather` is not pushed because Broadcast
  has no inherent gather-preserving property.
- **Caching `LogicalProperty` on the Memo group "computed once when a group
  is first explored"**: the roadmap mentions this; PhysicalProperty caching
  in `winners` already meets the practical need. Logical-property caching
  is left to a future PR.

---

## §9 Implementation roadmap (for writing-plans)

The implementation plan (next skill) will sequence work approximately as:

1. **Add `Winner.output`** (non-`Option`, constructed with `PhysicalPropertySet::any()` placeholder at the single construction site) — purely additive, no behaviour change. The real value comes in step 4.
2. **Create `src/sql/optimizer/derive/` skeleton** — traits, dispatcher fns, `passthrough.rs` helpers; everything else delegates to today's logic moved as-is. Wire `search.rs` to call the dispatchers. No behaviour change expected.
3. **Migrate per-op tests** from `search.rs::tests` to `derive/<op>.rs`.
4. **Rewrite `optimize_group` loop per §4** — `Winner.output` populated from children winners; remove self-recursive enforcer call.
5. **Implement Broadcast / Colocate output rules per §3** — the headline behavioural change.
6. **Implement Broadcast required pushdown per §3.2**.
7. **Verify against `optimizer/` suite**; review every plan-shape diff.
8. **Verify against `window_*`, `subquery_alias`, `tpc-ds`, `tpc-h`, `ssb`, `join`, `filter`, `sort`**.
9. **Add §5.3 new plan-golden cases**.

Each numbered step should land as a separate commit on the G3 PR branch so
the diff narrative matches the design narrative.
