# SQL Layer Architecture Roadmap & Invariants

**Date**: 2026-05-20  
**Source**: Obsidian `ARCH-sql-layer-vs-starrocks.md`  
**Scope**: NovaRocks standalone SQL optimizer — from parser output through Fragment codegen.  
**Out of scope**: FE-compatible plan lowering (`src/lower/**`), exchange/runtime, connectors.

---

## §0 Context

> NovaRocks 不是缺 rule，是缺 5 条架构契约和 1 个 canonical column id。
> 补完之后再补 rule 才是健康路径。

The current optimizer pipeline broadly mirrors StarRocks FE:

```
Parser → AST → Analyzer → Logical tree → Optimizer (RBO + Memo + CBO) → Physical plan → Fragment → Execution
```

But the per-layer abstractions are systematically thinner or duplicated. Seven concrete gaps
(`G1`–`G7`) have been identified against the StarRocks reference implementation. The gaps are
not independent features to bolt on — they reflect the absence of a shared foundation. Adding
features before the foundation is in place causes hacks to accumulate.

This document establishes:
1. **Five design invariants** — the principles every new piece of SQL-layer code should respect.
2. **A sequenced roadmap** — the order in which gaps should be closed, with enough context for each
   gap to be picked up as a standalone brainstorming/spec/implementation cycle.

---

## §1 Design Invariants

These are **design principles**, not PR-rejection rules. When a PR deviates from an invariant
due to legitimate practical constraints, the deviation should be noted in the PR description
with a rationale and a follow-up ticket.

---

### Invariant 1 — Columns are identified by global `ColumnId`, never by strings

Every column gets a `ColumnId(u32)` allocated by `ColumnRefFactory` at analysis time. All
downstream layers — distribution specs, equivalence classes, sort keys, output schemas — compare
columns by id. String names appear only in display contexts: `EXPLAIN` output, error messages,
and the final output schema exposed to the client.

A `SubqueryAlias`, `Project`, or `Window` does **not** create a new id for an existing column.
It only changes the display name.

**Current violation**: `src/sql/optimizer/property.rs:6`
```rust
pub(crate) struct ColumnRef {
    pub qualifier: Option<String>,
    pub column: String,
}
```
After `SUBQUERY ALIAS [l]`, `c0` becomes `l.c0`. A distribution spec `Hash([c0])` can no
longer satisfy `Hash([l.c0])`, breaking distribution reuse across aliases. This is the root
cause of the alias re-qualification issue in PR-F1 and blocks all equivalence-based
optimizations.

**Compliant shape**: `ColumnId(u32)` in `property.rs`; a single `ColumnRefFactory` in
`src/sql/analyzer/`; all `DistributionSpec`, `SortKey`, and `ColumnRefSet` hold `ColumnId`.

---

### Invariant 2 — Five-layer separation: Logical / Physical / Property / Cost / Fragment

| Layer | Responsibility | Must NOT do |
|---|---|---|
| Logical | Semantics-preserving rewrites (RBO + CBO transformation rules) | Choose physical algorithms, inspect Statistics, emit Thrift |
| Physical | Select algorithms + inject distribution enforcers | Change query semantics or output schema |
| Property | Derive `LogicalProperty` + decide `PhysicalProperty` satisfaction | Modify the plan tree |
| Cost | Estimate cost for `OptExpression` | Read Thrift, write plan |
| Fragment | `OptExpression` → `PlanFragment` (pure serialization) | Make semantic decisions or choose physical algorithms |

**Current violation**: `src/sql/codegen/fragment_builder.rs` decides the value of
`analytic_partition_exprs` for the `SORT` node while serializing to Thrift. This is a physical
algorithm decision happening inside the serialization layer.

**Compliant shape**: Physical implementation rules in `src/sql/optimizer/search.rs` (or a
dedicated physical rule) attach `analytic_partition_exprs` to the `PhysicalSortOp` during the
optimizer phase. `fragment_builder.rs` reads the already-decided value and serializes it.

---

### Invariant 3 — Property derivation is an explicit visitor that sees children

Both output-property derivation and required-property derivation must be able to observe the
properties of child operators. Property derivation results are cached on the `Memo` group; they
are computed once when a group is first explored, not recomputed on every search iteration.

**Current violation**: `src/sql/optimizer/search.rs:261`
```rust
fn output_properties(op: &Operator) -> PhysicalPropertySet { ... }
```
Single-argument: cannot see children. Broadcast/Colocate JOIN output is approximated as `Any`
(`search.rs:301-303`), losing distribution reuse for downstream operators.

**Compliant shape**:
```rust
trait OutputPropertyDeriver {
    fn derive(op: &Operator, children_outputs: &[&PhysicalPropertySet]) -> PhysicalPropertySet;
}
trait RequiredPropertyDeriver {
    fn derive(op: &Operator, parent_required: &PhysicalPropertySet) -> Vec<PhysicalPropertySet>;
}
```

---

### Invariant 4 — Rules are described with `Pattern`; `Rule::apply` is a pure function

A `Pattern` describes the subtree shape a rule matches: root operator kind plus expected child
patterns. The optimizer framework performs the structural match; `Rule::apply` receives the
already-matched subtree and returns a list of new expressions. Rules do not manually navigate
the Memo to inspect children.

**Current violation**: `src/sql/optimizer/rule.rs`
```rust
fn matches(&self, op: &Operator) -> bool { ... }
```
Rules can only inspect the root operator. Inspecting children requires manual `memo.groups[child_id]`
navigation inside `apply`, producing boilerplate that is repeated across rules.

**Compliant shape**:
```rust
struct Pattern { op_kind: OperatorKind, children: Vec<Pattern> }

trait Rule {
    fn pattern(&self) -> &Pattern;
    fn apply(&self, matched: &MatchedSubtree) -> Vec<NewExpr>;
}
```

---

### Invariant 5 — One Logical tree; planner and optimizer share the same `Operator` type

The planner outputs an `Operator` tree directly. RBO rules operate on `Operator`. CBO seeds the
Memo from the RBO output. There is no separate `LogicalPlan` enum that must be converted to
`Operator` or kept in sync.

Any code that copies a field from a `LogicalPlan` variant into an `Operator` variant (or vice
versa) is a violation.

**Current violation**:
- `src/sql/planner/plan.rs` — `LogicalPlan` enum used by RBO
- `src/sql/optimizer/operator.rs` — `Operator::Logical*Op` enum used by CBO

Adding any field (e.g., `analytic_partition_by` in PR-F1) requires updates in both enums plus
5+ propagation sites (`cte_rewrite.rs`, `rbo/driver.rs`, `column_pruning.rs`, …). Any missed
site is a silent bug.

**Compliant shape**: Delete `LogicalPlan`. Planner emits `Operator` directly. Conversion
function `convert::logical_plan_to_memo` is deleted.

---

## §2 Roadmap

### §2.0 Pre-work (Hygiene — no architecture changes required)

These items reduce ongoing damage while the deeper gaps are being fixed. Neither requires
touching the optimizer's core data structures.

**P0.5-a — Extract `analytic_partition_exprs` into an explicit `PerPartitionSort` concept**

`PhysicalSortOp` in `src/exec/operators/sort_node.rs` carries `analytic_partition_exprs` as a
side-channel field added in PR #150. The field communicates "this SORT is scoped per partition
of a Window operator" but the contract is implicit. Before G6 makes the Fragment layer a proper
IR, extract this into a named concept (a wrapper struct or a typed `SortMode` enum) so that the
intent is visible to anyone reading the operator definition.  
Entry: `src/exec/operators/sort_node.rs`, `src/sql/codegen/fragment_builder.rs`  
Effort: 0.5–1 day  
Validation: All PR-F1 sql-test cases remain green; window/sort golden files unchanged.

**P0.5-b — Document the `output_properties` limitation**

Add a doc comment to `output_properties` at `src/sql/optimizer/search.rs:261` stating explicitly
that it cannot observe child outputs and naming G3 as the planned fix. This prevents future PRs
from designing logic that assumes child visibility is available.  
Entry: `src/sql/optimizer/search.rs:261`  
Effort: 30 minutes  
Validation: `cargo doc` clean.

---

### §2.1 G1 — ColumnId (P0)

**Goal**: Replace string-based `ColumnRef` with a globally unique `ColumnId(u32)`. All
distribution specs, sort keys, equivalence sets, and output schemas reference columns by id.
Display names (for EXPLAIN, errors, output schema) are stored in a side table keyed by
`ColumnId`.

**Entry files**:
- `src/sql/analyzer/**` — id allocation (`ColumnRefFactory`)
- `src/sql/optimizer/property.rs` — `ColumnRef` → `ColumnId` in `DistributionSpec`, `SortKey`
- `src/sql/planner/plan.rs`, `src/sql/optimizer/operator.rs` — column references in all operators
- `src/sql/codegen/**` — display-name lookup at serialization boundary

**Dependencies**: None (this is the P0 starting point).

**Unlocks**: G7 (equivalence classes require stable column identity); Phase 2.1 Broadcast/
Colocate output derivation; PR-F1 alias re-qualification; predicate pushdown across aliases;
join equality transitivity.

**Effort**: 1–2 weeks (one-time large refactor; touches many files but changes are mechanical).

**Validation**:
- Full sql-test suite green, especially `subquery_alias`, `window_*`, and PR-F1 cases.
- `grep -r 'ColumnRef {' src/sql/optimizer/` returns zero results after migration.
- New unit test: re-aliased column shares the same `ColumnId` as the original.

---

### §2.2 G3 — `output_properties` visitor (P1)

**Goal**: Change the output-property derivation signature to accept child outputs, enabling
Broadcast and Colocate JOIN to report their actual output distribution (based on the left
child) instead of `Any`. Cache the result on the Memo group.

**Entry files**:
- `src/sql/optimizer/search.rs:261` — signature change
- `src/sql/optimizer/property.rs` — updated derivation logic for Join, Filter, Project, etc.

**Dependencies**: G1 strongly recommended (otherwise child outputs still carry string
`ColumnRef` and distribution comparison still fails for aliased columns).

**Unlocks**: PR-F1 Phase 2.1 (Window reuses JOIN's distribution without extra EXCHANGE);
multi-level Window sharing distribution; Colocate join detection.

**Effort**: 3–5 days.

**Validation**:
- Broadcast/Colocate join test cases in EXPLAIN show fewer `EXCHANGE` nodes.
- No regression in Memo search iteration count (cache hit sanity check).

---

### §2.3 G4 — `HashDistribution` source type (P1)

**Goal**: Add a `HashSource` tag to `HashPartitioned`:
`HashPartitioned(Vec<ColumnId>, HashSource)` where `HashSource ∈ {Agg, Join, Bucket, Enforce}`.
Update `satisfies` to branch on source type: `Agg` allows contain-all (superset satisfies);
`Join` requires exact-match.

**Entry files**:
- `src/sql/optimizer/property.rs` — `DistributionSpec`, `satisfies`

**Dependencies**: G1 (columns should be `ColumnId` before adding more semantics to distribution
specs). Can be done in parallel with G3.

**Unlocks**: Fixes the over-permissive `satisfyContainAll` from PR #153 (a JOIN distribution
can currently satisfy an AGG requirement that it shouldn't); clean SHUFFLE_AGG vs SHUFFLE_JOIN
distinction.

**Effort**: 3–5 days.

**Validation**:
- New negative test: a `Hash([a, b], Join)` distribution does NOT satisfy a `Hash([a], Agg)`
  requirement (exact-match check).
- Existing SHUFFLE_AGG and Window tests remain green.

---

### §2.4 G7 — `LogicalProperty` equivalence classes & unique columns (P1)

**Goal**: Extend `LogicalProperties` (currently `{ output_columns, row_count }`) with:
- `equivalence_classes: Vec<ColumnIdSet>` — populated from filter equalities and join equalities.
- `unique_columns: ColumnIdSet` — populated from DISTINCT, primary key constraints, and
  aggregation group-by keys.

**Entry files**:
- `src/sql/optimizer/memo.rs:98` — `LogicalProperties` struct
- `src/sql/optimizer/property.rs` — `ColumnIdSet` type
- Logical property derivation sites for Filter, Join, Aggregate, etc.

**Dependencies**: G1 (strict — equivalence classes require `ColumnId` for identity comparison).

**Unlocks**: Predicate pushdown across equivalences (free once classes exist); join equality
transitivity closure; distinct elimination; improved column pruning.

**Effort**: 1 week.

**Validation**:
- New `equivalence_propagation_*` sql-test golden files asserting that `WHERE a = b` causes
  predicate on `a` to be pushed as predicate on `b` as well.
- TPC-DS and TPC-H plan shapes do not regress.

---

### §2.5 G2 — Unify `LogicalPlan` and `Operator::Logical*Op` (P2)

**Goal**: Delete `src/sql/planner/plan.rs::LogicalPlan`. The planner produces an `Operator`
tree directly. RBO rules operate on `Operator`. The conversion function
`convert::logical_plan_to_memo` is deleted. "Field must be added in two places" is eliminated.

**Entry files**:
- `src/sql/planner/plan.rs` — deleted
- `src/sql/optimizer/operator.rs` — extended with any fields currently in `LogicalPlan` only
- `src/sql/optimizer/convert.rs` — deleted
- `src/sql/optimizer/rbo/**` — updated to operate on `Operator`
- `src/sql/planner/**` — planner emits `Operator` directly

**Dependencies**: G1 recommended (column representation unified first makes the merge cleaner).
G3 and G4 can land before or after — they don't touch `LogicalPlan`.

**Unlocks**: Eliminates 5+ file propagation cost for every new operator field; makes G5
(Pattern framework) straightforward to add since there is only one operator type tree.

**Effort**: 1–2 weeks.

**Validation**:
- All RBO-driven sql-test golden files unchanged.
- `grep -r 'LogicalPlan' src/` returns zero results after migration.

---

### §2.6 G5 — Pattern-based composable Rule framework (P3)

**Goal**: Implement `Pattern { op_kind: OperatorKind, children: Vec<Pattern> }`. Add
`Rule::pattern() -> &Pattern` and update the optimizer to perform structural matching before
calling `Rule::apply`. `apply` receives a `MatchedSubtree` with direct access to matched child
expressions — no more manual Memo navigation.

**Entry files**:
- `src/sql/optimizer/rule.rs`
- `src/sql/optimizer/rbo/driver.rs`
- `src/sql/optimizer/search.rs`

**Dependencies**: None strict; G2 (single Operator type) makes implementation cleaner.

**Unlocks**: Rule count can grow sustainably; eliminates `memo.groups[child_id]` boilerplate;
enables Pattern sharing across rules; transformation vs. implementation rule types become
explicit.

**Effort**: 1 week.

**Validation**:
- At least 3 existing rules migrated to Pattern API; their sql-test golden files unchanged.
- New filter-pushdown rule (if applicable) written using Pattern without manual Memo access.

---

### §2.7 G6 — Fragment intermediate representation (P3)

**Goal**: `fragment_builder.rs` builds an internal Rust `PlanFragment` struct (not Thrift
types). The final `to_thrift()` call is the only place Thrift types appear. EXPLAIN, plan dumps,
and regression tests operate on the native struct without Thrift deserialization.

**Entry files**:
- `src/sql/codegen/fragment_builder.rs`
- Interface between `src/exec/**` and Thrift structs
- New `src/sql/codegen/plan_fragment.rs` (native IR types)

**Dependencies**: Recommended after G1 (ColumnId used uniformly in the native IR from the
start), G2 (single Operator type), and G5 (Pattern framework). All three are advisory — G6
can be started independently if the team has bandwidth, though the native IR will need
retroactive ColumnId adoption if G1 is not yet done.

**Unlocks**: Fragment unit tests (native structs are easy to construct); readable plan dumps
without Thrift deserialization; IDL changes no longer propagate into codegen logic; EXPLAIN
can be built from the native IR directly.

**Effort**: 2–3 weeks.

**Validation**:
- All FE-compatible end-to-end sql-tests remain green.
- At least 3 fragment-level unit tests added (one per major operator type).
- `grep -rn 'TPlanNode {' src/sql/codegen/fragment_builder.rs` returns zero results.

---

### §2.8 Dependency graph

```
[Pre-work: P0.5-a, P0.5-b]
            │
            ▼
    ┌──────────────┐
    │  G1 ColumnId │  (P0)
    └──────┬───────┘
           │
   ┌───────┼──────────────┬──────────────┐
   ▼       ▼              ▼              ▼
┌────┐  ┌────┐          ┌────┐         ┌────┐
│ G7 │  │ G3 │          │ G4 │         │ G2 │  (weak dep on G1)
└────┘  └────┘          └────┘         └─┬──┘
 (P1)    (P1)            (P1)            │
                                         │ (P2)
                                         ▼
                                  ┌────┐   ┌────┐
                                  │ G5 │   │ G6 │  (P3, parallel)
                                  └────┘   └────┘
```

**Parallel opportunities**:
- G3, G4, G7 can all proceed in parallel once G1 lands.
- G5 and G6 are the final wave; G6 can be started independently of G5 if needed.

**Foundation milestone**: When G1 + G3 + G4 + G7 are all merged, the majority of
outstanding plan-quality work in PR-F1 and E1 becomes straightforward to implement
without architectural hacks.

---

## §3 How to use this document

Each time a new gap is picked up:

1. **Read §1.** Confirm the planned change aligns with all five invariants. Note any deviation
   in the PR description with a rationale.
2. **Read the corresponding §2.x entry.** Use it as the briefing input for a new
   brainstorming session to produce a detailed implementation spec for that gap.
3. **Open a new brainstorm session** for the chosen gap. The §2.x descriptor is self-contained
   enough to hand to a fresh agent cold.
4. **After each gap lands**, update the §2.x entry's validation status (or create a follow-up
   memory note) so the next engineer knows what has already been done.

The sub-spec entries in §2 are not themselves implementation plans. Each will expand into a
full spec (via brainstorming) and then an implementation plan (via writing-plans) before code
is written.
