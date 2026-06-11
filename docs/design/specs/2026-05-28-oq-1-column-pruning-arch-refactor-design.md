# OQ-1: Column Pruning Architecture Refactor — Design

Date: 2026-05-28
Tasks:
- OQ-1 in [Optimizer Plan Quality Roadmap](../../../../../../../Documents/Obsidian/NovaRocks%20TODO/NovaRocks%20Roadmap.md#optimizer-plan-quality-roadmap)
Predecessor: PR #200 (JN-* join-suite residuals)
Successors: OQ-2 (NULL filter inference), OQ-3 (cardinality), OQ-4 (SplitAgg), OQ-5 (runtime filter), OQ-6 (SubqueryAlias fold), OQ-7 (validation)
Scope: single PR-α (big-bang refactor + 5 gap fixes)

## 1. Goal

Replace the current single-rule, hand-walked top-down column-pruning implementation (`src/sql/optimizer/rewrite/rules/column_pruning.rs::PruneColumns`, 562 lines) with a per-operator-rule architecture aligned with StarRocks's `Prune*ColumnsRule` family.

After this PR-α:

- Each logical operator type has its own `Prune<Op>Columns` rule (18 rules total).
- A new Phase 1 "tagging pass" populates a new `required_output_columns: Option<HashSet<ColumnId>>` field on every operator before Phase 2 rules fire.
- Existing `LogicalRewriteRule` trait and bottom-up driver are unchanged; the new rules plug into the same framework as `PushDownPredicate*`, `JoinReorder`, `AggregatePushdown`, `LowCardinalityDictionaryRewrite`.
- All five gaps identified in the OQ-1 preflight audit are closed in the same PR (SubqueryAlias propagation, Project items pruning, CTE inline ordering, Union branch alignment, Aggregate/Window output_columns pruning).
- `ScanNode.required_columns: Option<Vec<String>>` remains as the codegen-facing contract; the new `PruneScanColumns` rule populates it from the ColumnId-based `required_output_columns`. **Codegen / fragment_builder unchanged.**

Non-goals (deferred to OQ-1.6+ follow-ups, see §10):

- Eliminating `SubqueryAlias` logical operator (StarRocks's approach).
- `PruneEmpty*` cardinality=0 propagation.
- Physical-plan column pruning (`PruneShuffleColumn`-style).
- Multi-consumer CTE column pruning beyond the union-of-needs design (initial PR only covers single-consumer in tests).

## 2. Background

### 2.1 Why this matters now

PR #200 closed the 9 JN-* join-suite residuals (50/59 → 57/60 pass). Running the suite `-j 1` (to avoid the GlobalDriverExecutor lock-contention false-positives in `-j 4`) revealed real plan-quality bottlenecks:

- `join_one_key` single case: **534s wall-clock**, 103 trivial joins on a 33-column wide table.
- `EXPLAIN ANALYZE` on the canonical slow query `join_one_key` q22 (CTE LEFT SEMI JOIN) shows:
  - Planning: 9 ms (optimizer fine)
  - Execution: 5.6 s (the problem)
  - **Right side reads 33 columns** when only `c_tinyint_null` is needed.

A side-by-side comparison with StarRocks FE on the same schema + data showed the column pruning gap directly: SR's plan reads 1 column on the build side; ours reads 33. Bandwidth, hash-table memory, and cache footprint are all 33× worse.

### 2.2 The five gaps

From the OQ-1 preflight audit:

1. **Gap 1: SubqueryAlias 屏蔽 needed 传播.** `column_pruning.rs:280-291` deliberately passes `None` to inner plan, citing "the inner plan has its own column namespace (aliases differ from base columns)". With ColumnId-based propagation, this concern dissolves.
2. **Gap 2: `ProjectNode.items` never pruned.** Current rule only uses `needed` to compute `child_needed` for the Scan; `items` is preserved verbatim.
3. **Gap 3: CTE inline runs after column pruning.** `cte_rewrite::inline_single_use_ctes` is called after `query_rewrite_pipeline` finishes, so pruning sees `CTEAnchor/CTEProduce/CTEConsume`, gives CTEProduce `None`, and the CTE-internal Scan reads all columns.
4. **Gap 4: Union/Intersect/Except branches independent.** Each branch gets `None`; no position-aligned pruning across branches.
5. **Gap 5: Aggregate / Window `output_columns` never pruned.** Even when the parent doesn't need all aggregate outputs, the entire `output_columns` list is preserved.

### 2.3 Architecture choice rationale

StarRocks splits column pruning into per-operator rules and uses a shared mutable `TaskContext.requiredColumns: ColumnRefSet` populated as rules fire. We chose **not** to copy that exact model (shared mutable state across rules has poor maintainability and weak observability) but to keep the per-operator rule split, plus a **two-phase walk** (tag-then-prune) where each operator carries its own `required_output_columns` as metadata.

Architecture-first ordering (Gap 6 before gap fixes) was chosen so each gap fix lands as a new per-operator rule in the new framework, not as a patch on the soon-to-be-deleted single rule.

## 3. Architecture Overview

### 3.1 Two-phase pipeline

```
query_rewrite_pipeline (existing):
  PredicatePushdownPreJoin
  JoinReorder
  PredicatePushdownPostJoin
  AggregatePushdown
  TagRequiredColumns           ← NEW Phase 1 (single-pass top-down walk)
  ColumnPruning                ← Phase 2 (18 per-operator rules, fixed-point loop)
  LowCardinalityDictionaryRewrite
```

`TagRequiredColumns` is a `RewriteStage` whose single "rule" is a wrapper around `required_columns::tag_required_columns(plan, None)` — a top-down recursive function. It is *not* iterated; the stage runs the function exactly once. Idempotency guarantee: calling it on a plan whose `required_output_columns` are already set is a no-op (writes the same values back). This lets the fixed-point loop terminate cleanly if it ever re-enters tagging.

`ColumnPruning` is a normal fixed-point `RewriteStage` with 18 transformation rules. The existing bottom-up driver handles convergence. Each rule reads the *local* `required_output_columns` field; they do not communicate through shared state.

### 3.2 File layout

```
src/sql/optimizer/rewrite/
├── required_columns.rs                     ← NEW: Phase 1 tagging pass
└── rules/
    └── column_pruning/                     ← NEW directory
        ├── mod.rs                          ← register all 18 rules + helpers
        ├── prune_scan.rs
        ├── prune_project.rs
        ├── prune_filter.rs                 ← no-op rule (preserved for symmetry)
        ├── prune_aggregate.rs
        ├── prune_join.rs                   ← no-op rule (preserved for symmetry)
        ├── prune_sort.rs                   ← no-op rule
        ├── prune_limit.rs                  ← no-op rule
        ├── prune_window.rs
        ├── prune_union.rs
        ├── prune_intersect.rs
        ├── prune_except.rs
        ├── prune_subquery_alias.rs
        ├── prune_cte_anchor.rs
        ├── prune_cte_consume.rs
        ├── prune_cte_produce.rs
        ├── prune_repeat.rs                 ← no-op rule
        ├── prune_decode.rs
        └── prune_table_function.rs         ← no-op rule
```

Deleted: `src/sql/optimizer/rewrite/rules/column_pruning.rs` (the 562-line single rule). The four existing tests in that file migrate into the corresponding new per-rule files.

### 3.3 Trait usage

No new trait. All 18 rules implement the existing `LogicalRewriteRule` (signature elided to its relevant subset; full trait in `src/sql/optimizer/rewrite/rule.rs`):

```rust
pub(crate) trait LogicalRewriteRule: Send + Sync {
    fn name(&self) -> &'static str;
    fn phase(&self) -> RewritePhase;                                // returns StructuralRewrite
    fn traversal(&self) -> RewriteTraversal { RewriteTraversal::BottomUp }
    fn matches(&self, plan: &LogicalPlan, ctx: &RewriteContext) -> bool;
    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String>;
}
```

Each Prune* rule's `matches` returns true for exactly one `LogicalPlan` variant. `apply` reads the node's `required_output_columns` and returns `RewriteResult::Changed(new_plan)` or `RewriteResult::Unchanged` (no-op). All Prune* rules use `traversal = BottomUp` (default) — Phase 1 already populated `required_output_columns` so per-rule order is irrelevant.

**Note on framework's `TopDown` option**: The framework already supports `TopDown` traversal (see `tree.rs::rewrite_top_down`). Phase 1's tagging walk could in principle be expressed as a single `TopDown`-traversal rule that internally walks the subtree. PR-α implementation chooses between (a) a standalone module function `tag_required_columns(plan)` called from a single-rule wrapper, or (b) embedding the full walk inside a `TopDown` Rule. The spec leaves this implementation detail open; either path produces the same observable behavior, and the `required_output_columns` field semantics are unchanged.

### 3.4 Why CTE inline can stay where it is

The current order — `query_rewrite_pipeline` (incl. ColumnPruning) → `cte_rewrite::inline_single_use_ctes` — would normally block Gap 3's fix. The new Phase 1 tagging pass handles `CTEAnchor` with a **two-walk pattern** (see §5.6): it tags the consumer subtree first, then walks it again to collect every `CTEConsume.required_output_columns` for the same `cte_id`, unions them, translates through `CTEProduce.output_columns` position-alignment, and tags the produce subtree. By the time `inline_single_use_ctes` later runs, the produce-side `Scan` is already pruned. No pipeline reordering needed.

## 4. Data Model

### 4.1 The field

Add to **every** internal LogicalPlan variant:

```rust
pub required_output_columns: Option<HashSet<ColumnId>>,
```

Semantics:

- `None` ⇒ "tagging hasn't visited this node; assume all outputs required". Defensive default — every Prune* rule treats this as no-op.
- `Some(set)` ⇒ "tagging visited; downstream needs exactly this ColumnId set".

Operators affected — the field is added to every internal node type and to leaves with explicit output schemas (Values), but is informational-only on leaves with no schema (GenerateSeries). 18 rule-relevant operator categories (Union/Intersect/Except share one category for position-aligned semantics):

| Operator | Has output_columns/items? | Phase 2 rule action |
|---|---|---|
| Scan | (special: `required_columns: Option<Vec<String>>`) | Translate ID set → name list |
| Project | `items: Vec<ProjectItem>` | Filter items by `output_column_id ∈ set`, auto_fill 1 if empty |
| Filter | none | No-op (Phase 1 already propagated predicate cols) |
| Aggregate | `output_columns`, `aggregates` | Filter both |
| Join | none | No-op (Phase 1 propagated condition cols) |
| Sort | none | No-op |
| Limit | none | No-op |
| Window | `output_columns`, `window_exprs` | Filter both |
| Union/Intersect/Except | `outputs` (implicit, per-position) | Position-aligned filter across all branches |
| SubqueryAlias | `output_columns` | Filter |
| CTEAnchor | none | No-op (Phase 1 did the work) |
| CTEProduce | `output_columns` | Filter |
| CTEConsume | `output_columns` (mapping to producer) | Filter |
| Repeat | none | No-op |
| Decode | `mappings`, `output_columns` | Filter mappings + outputs |
| TableFunction | none | No-op |
| Values | `columns` (leaf) | Optional: filter; OQ-1 leaves as-is |
| GenerateSeries | none (leaf) | No-op |

### 4.2 Why HashSet<ColumnId> not Vec<ColumnId>

- O(1) `contains` lookup, used inside every rule's filter loop.
- No ordering concern (ColumnIds are independent identifiers).
- Auto-deduplication.

### 4.3 Why ColumnId not String

- Eliminates the alias-name-mapping problem (Gap 1 root cause). The same column under `SELECT t1.k1 FROM t1` and `SELECT t2.k1 FROM (SELECT k1 FROM t1) t2` carries the **same** ColumnId because our `ColumnRef` expressions are id-stamped during analysis.
- Aligns with StarRocks's `ColumnRefSet` design (we'd need this eventually for OQ-2/4/5 anyway).
- The `ExprKind::ColumnRef { column_id: ColumnId, ... }` field already exists; we just start using it for pruning.

### 4.4 Codegen contract

`ScanNode.required_columns: Option<Vec<String>>` **stays**. The current codegen/fragment_builder reads it and emits the column-subset request to the storage layer. `PruneScanColumns` is the only rule that bridges the two representations:

```rust
// Inside PruneScanColumns::apply
let ids = node.required_output_columns.as_ref()?;
let names: Vec<String> = node.columns.iter()
    .filter(|c| ids.contains(&c.column_id))
    .map(|c| c.name.clone())
    .collect();
node.required_columns = Some(names);
```

This isolates the ID↔name bridge to one place. Codegen is unchanged.

## 5. Phase 1: Tagging Pass

Module: `src/sql/optimizer/rewrite/required_columns.rs`

Entry point:

```rust
pub(crate) fn tag_required_columns(
    plan: LogicalPlan,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlan;
```

Style: pure function, take-by-value / return-by-value, recursive. Mirrors current `prune_inner` shape but does *not* prune — only writes `required_output_columns`.

### 5.1 Scan

```rust
LogicalPlan::Scan(mut scan) => {
    let needed = parent_needed.unwrap_or_else(|| all_output_ids(&scan));
    scan.required_output_columns = Some(needed);
    LogicalPlan::Scan(scan)
}
```

### 5.2 Project

```rust
LogicalPlan::Project(mut node) => {
    node.required_output_columns = parent_needed.clone();
    let child_needed: HashSet<ColumnId> = node.items.iter()
        .filter(|item| match &parent_needed {
            None => true,
            Some(n) => n.contains(&item.output_column_id),
        })
        .flat_map(|item| collect_column_ids(&item.expr))
        .collect();
    node.input = Box::new(tag_required_columns(*node.input, Some(child_needed)));
    LogicalPlan::Project(node)
}
```

When `parent_needed = None`, child_needed is the union of all items' column refs (effectively None, but explicit). When `parent_needed = Some({a})`, only items producing `a` contribute to child_needed.

### 5.3 Filter

```rust
LogicalPlan::Filter(mut node) => {
    node.required_output_columns = parent_needed.clone();
    let mut child_needed = parent_needed.clone().unwrap_or_default();
    child_needed.extend(collect_column_ids(&node.predicate));
    node.input = Box::new(tag_required_columns(*node.input, Some(child_needed)));
    LogicalPlan::Filter(node)
}
```

Filter doesn't *project away* columns, so its output_columns == input_columns. The needed set passed down is `parent_needed ∪ predicate_cols`.

### 5.4 Aggregate

```rust
LogicalPlan::Aggregate(mut node) => {
    node.required_output_columns = parent_needed.clone();
    let mut child_needed = HashSet::new();
    for gb in &node.group_by {
        child_needed.extend(collect_column_ids(gb));
    }
    for (i, agg) in node.aggregates.iter().enumerate() {
        let out_id = node.output_columns[node.group_by.len() + i].column_id;
        let aggregate_needed = match &parent_needed {
            None => true,
            Some(n) => n.contains(&out_id),
        };
        if aggregate_needed {
            for arg in &agg.args {
                child_needed.extend(collect_column_ids(arg));
            }
            for item in &agg.order_by {
                child_needed.extend(collect_column_ids(&item.expr));
            }
        }
    }
    node.input = Box::new(tag_required_columns(*node.input, Some(child_needed)));
    LogicalPlan::Aggregate(node)
}
```

Phase 1 computes child_needed but does *not* yet drop unused aggregates from `node.aggregates` — that happens in Phase 2's `PruneAggregateColumns` rule (Gap 5).

### 5.5 Join

```rust
LogicalPlan::Join(mut node) => {
    node.required_output_columns = parent_needed.clone();
    let mut combined = parent_needed.unwrap_or_default();
    if let Some(cond) = &node.condition {
        combined.extend(collect_column_ids(cond));
    }
    let left_outputs = collect_output_ids(&node.left);
    let right_outputs = collect_output_ids(&node.right);
    let left_needed: HashSet<ColumnId> = combined.intersection(&left_outputs).cloned().collect();
    let right_needed: HashSet<ColumnId> = combined.intersection(&right_outputs).cloned().collect();
    node.left = Box::new(tag_required_columns(*node.left, Some(left_needed)));
    node.right = Box::new(tag_required_columns(*node.right, Some(right_needed)));
    LogicalPlan::Join(node)
}
```

`collect_output_ids` is a helper that recursively determines a plan subtree's output ColumnId set (using existing `OutputColumn` fields where present, derived from base columns at leaves).

### 5.6 CTEAnchor (two-walk pattern)

This is Gap 3's solution.

```rust
LogicalPlan::CTEAnchor(mut node) => {
    let cte_id = node.cte_id;

    // Walk 1: tag the consumer subtree with parent_needed.
    // Every CTEConsume with matching cte_id receives its own required_output_columns.
    let consumer = tag_required_columns(*node.consumer, parent_needed);

    // Walk 2: scan the now-tagged consumer subtree to collect the union of all
    // CTEConsume.required_output_columns for this cte_id.
    let consume_needs = collect_cte_consumer_needs(&consumer, cte_id);

    // Translate the consume-side ColumnIds to produce-side ColumnIds via
    // CTEConsumeNode.output_columns ↔ CTEProduceNode.output_columns position alignment.
    let produce_input_needed = translate_consume_to_produce_ids(
        &consume_needs,
        &node.produce,
    );

    let produce = tag_required_columns(*node.produce, Some(produce_input_needed));

    node.consumer = Box::new(consumer);
    node.produce = Box::new(produce);
    node.required_output_columns = parent_needed.clone();
    LogicalPlan::CTEAnchor(node)
}

LogicalPlan::CTEConsume(mut node) => {
    node.required_output_columns = parent_needed;
    LogicalPlan::CTEConsume(node)
}
```

`collect_cte_consumer_needs` is a simple recursive scan; it traverses the consumer subtree and for every `CTEConsume(c)` with matching `cte_id` unions `c.required_output_columns` into an accumulator. O(subtree size); acceptable cost given pipelines fit a few hundred nodes.

For initial scope, multi-consumer correctness is **designed in** (the union pattern is correct), but unit-test coverage in PR-α only includes single-consumer cases. A follow-up PR adds multi-consumer test cases.

### 5.7 Union (position-aligned)

This is Gap 4's solution.

```rust
LogicalPlan::Union(mut node) => {
    let outputs: Vec<ColumnId> = node.output_columns.iter().map(|c| c.column_id).collect();
    let needed_positions: HashSet<usize> = match &parent_needed {
        None => (0..outputs.len()).collect(),
        Some(n) => outputs.iter().enumerate()
            .filter_map(|(i, id)| n.contains(id).then_some(i))
            .collect(),
    };

    node.required_output_columns = parent_needed.clone();
    node.inputs = node.inputs.into_iter().map(|child| {
        let child_outputs = collect_output_ids_ordered(&child);
        let child_needed: HashSet<ColumnId> = needed_positions.iter()
            .map(|&i| child_outputs[i])
            .collect();
        tag_required_columns(child, Some(child_needed))
    }).collect();

    LogicalPlan::Union(node)
}
```

`Intersect` and `Except` follow the same pattern.

Note: `UnionNode` currently doesn't store explicit `output_columns`; its output schema is derived from `inputs[0]`. We need to add an `output_columns: Vec<OutputColumn>` field to `UnionNode` (and `Intersect`/`Except`) as part of this PR for position-aligned pruning. This is a small additive change.

### 5.8 SubqueryAlias (transparent ColumnId propagation)

This is Gap 1's solution. **Trivial** under ColumnId-based needed.

```rust
LogicalPlan::SubqueryAlias(mut node) => {
    node.required_output_columns = parent_needed.clone();
    node.input = Box::new(tag_required_columns(*node.input, parent_needed));
    LogicalPlan::SubqueryAlias(node)
}
```

No name mapping. The alias only renames the *qualifier*, not the ColumnId. Inner plan's `output_columns[i].column_id` == outer reference's column_id.

### 5.9 Sort / Limit / Window / Repeat / Decode / TableFunction

Sort, Limit pass needed plus their own key/predicate columns. Window/Repeat/Decode/TableFunction follow operator-specific rules; details in implementation but follow the same `tag` → recurse → return pattern. Specifications match StarRocks's `PruneSortColumns`, `PruneLimitColumns`, etc.

## 6. Phase 2: Per-Operator Rules

### 6.1 General rule shape

Every Prune* rule conforms to this template:

```rust
struct PruneXxxColumns;

impl LogicalRewriteRule for PruneXxxColumns {
    fn name(&self) -> &'static str { "PruneXxxColumns" }
    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Xxx(_))
    }
    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Xxx(mut node) = plan else { return None; };
        let needed = node.required_output_columns.as_ref()?;  // None ⇒ no-op
        let new_node = prune_xxx_columns(node, needed);
        if new_node_is_unchanged(&new_node) {
            None  // idempotent
        } else {
            Some(LogicalPlan::Xxx(new_node))
        }
    }
}
```

### 6.2 The 18 rules

| Rule | Operator | Action | Lines (est.) |
|---|---|---|---|
| `PruneScanColumns` | Scan | Translate ColumnId set → `required_columns: Vec<String>` for codegen | ~60 |
| `PruneProjectColumns` | Project | Filter `items` by output_column_id; auto_fill if empty | ~80 |
| `PruneFilterColumns` | Filter | **No-op** (Phase 1 propagated pred cols) | ~30 |
| `PruneAggregateColumns` | Aggregate | Filter `output_columns` and `aggregates` (Gap 5) | ~100 |
| `PruneJoinColumns` | Join | **No-op** (Phase 1 propagated cond cols) | ~30 |
| `PruneSortColumns` | Sort | **No-op** | ~30 |
| `PruneLimitColumns` | Limit | **No-op** | ~30 |
| `PruneWindowColumns` | Window | Filter `output_columns` and `window_exprs` (Gap 5) | ~80 |
| `PruneUnionColumns` | Union | Position-aligned filter `output_columns` + `child_outputs` (Gap 4) | ~80 |
| `PruneIntersectColumns` | Intersect | Same as Union | ~80 |
| `PruneExceptColumns` | Except | Same as Union | ~80 |
| `PruneSubqueryAliasColumns` | SubqueryAlias | Filter `output_columns` (Gap 1) | ~50 |
| `PruneCTEAnchorColumns` | CTEAnchor | **No-op** | ~30 |
| `PruneCTEConsumeColumns` | CTEConsume | Filter `output_columns` | ~60 |
| `PruneCTEProduceColumns` | CTEProduce | Filter `output_columns` | ~60 |
| `PruneRepeatColumns` | Repeat | **No-op** | ~30 |
| `PruneDecodeColumns` | Decode | Filter `mappings` and `output_columns` | ~70 |
| `PruneTableFunctionColumns` | TableFunction | **No-op** | ~30 |

No-op rules are kept for architectural symmetry — every operator has its own dedicated rule file. This documents the design, makes it obvious how to extend (e.g., if a future operator needs more pruning logic), and gives `disable_optimizer_rules` per-operator granularity.

### 6.3 Auto-fill safeguard

When a rule would prune an operator's outputs to empty (e.g., `Project` with all items filtered out, or `Union` with all positions filtered out), the rule inserts a single placeholder column:

```rust
fn auto_fill_one_item(factory: &ColumnRefFactory) -> ProjectItem {
    let col_id = factory.next();
    ProjectItem {
        output_column_id: col_id,
        output_name: format!("auto_fill_{}", col_id),
        expr: TypedExpr::int_literal(1),
    }
}
```

Matches StarRocks's `Utils.findSmallestColumnRef` / `ConstantOperator.createTinyInt((byte) 1)` pattern.

### 6.4 Idempotency

Each rule's `apply` returns `None` if applying produces no change. Equality check is done by comparing relevant fields (item count, output_columns content) — not by `format!("{:?}", ...)` which the old single-rule used (O(plan size)).

## 7. Pipeline & Trace Integration

### 7.1 Registry change

```rust
// rewrite/registry.rs::query_rewrite_pipeline
RewritePipeline::from_stages(vec![
    RewriteStage::new("PredicatePushdownPreJoin", ...),
    RewriteStage::new("JoinReorder", ...),
    RewriteStage::new("PredicatePushdownPostJoin", ...),
    RewriteStage::new("AggregatePushdown", ...),
    RewriteStage::new(
        "TagRequiredColumns",                              // ← NEW
        RewritePhase::StructuralRewrite,
        vec![Box::new(TagRequiredColumnsRule)],            // single rule wrapping the walk
    ),
    RewriteStage::new(
        "ColumnPruning",                                   // ← renamed/restructured
        RewritePhase::StructuralRewrite,
        rules::column_pruning::all_rules(),                // 18 per-operator rules
    ),
    RewriteStage::new("LowCardinalityDictionaryRewrite", ...),
])
```

`TagRequiredColumnsRule` is a wrapper rule that implements `LogicalRewriteRule` but matches only the root (via a "pattern" that matches anything, but `apply` is guarded to run exactly once via an internal flag in the operator's `required_output_columns` being `Some` ⇒ already tagged ⇒ no-op).

### 7.2 Disable / debug

Each rule appears in `is_known_rewrite_rule_name`. Users can disable any subset via `SET disable_optimizer_rules = 'PruneSubqueryAliasColumns,PruneProjectColumns'` for debugging plan regressions.

### 7.3 Trace events

The `RewriteTrace` framework emits `RuleMatched` / `RuleSkipped` / `RuleFailed` for each Prune* rule per group. `TagRequiredColumns` stage emits a single `PhaseStarted` / `PhaseEnded` pair. Inspecting `ctx.trace().events()` shows the full pruning trajectory.

### 7.4 EXPLAIN output

`src/sql/explain.rs` extended:

- **Normal mode**: no change.
- **Verbose / Costs mode**: each operator gets a trailing `req=[col_a, col_b, col_c]` showing the names of columns in `required_output_columns` (translated via the operator's own `output_columns` field where present).

Example after PR-α:

```
HASH JOIN (BROADCAST, LEFT SEMI) req=[]
  SCAN t1 (alias=t1) req=[k1, c_tinyint_null]                  <-- 2 cols
    TABLE: opt_probe.t1
    columns: k1, c_tinyint_null
  SUBQUERY ALIAS [t2] req=[c_tinyint_null]                     <-- 1 col!
    PROJECT [c_tinyint_null] req=[c_tinyint_null]              <-- 1 item!
      SCAN opt_probe.t1 req=[k1, c_tinyint_null]
        columns: k1, c_tinyint_null
        predicates: k1 < 100
```

## 8. Gap-to-Rule Mapping

Reference table linking each gap to its Phase 1 + Phase 2 implementation:

| Gap | Phase 1 (`required_columns.rs`) | Phase 2 (rule file) | Verification |
|---|---|---|---|
| Gap 1: SubqueryAlias 屏蔽 needed | `tag_subquery_alias` transparent ColumnId propagation (§5.8) | `PruneSubqueryAliasColumns` filters `output_columns` | golden plan `prune_subquery_alias_cte_left_semi.sql`: outer-only-c_tinyint_null query → SubqueryAlias 1 col |
| Gap 2: Project.items 不裁 | (Phase 1 unchanged from current logic) | `PruneProjectColumns` filters `items` by output_column_id | golden plan `prune_project_items_filter_only.sql`: 33-item project → 1 item |
| Gap 3: CTE inline after pruning | `tag_cte_anchor` two-walk pattern (§5.6) | `PruneCTEConsumeColumns` + `PruneCTEProduceColumns` filter their `output_columns` | golden plan `prune_cte_anchor_multi_consume.sql`: union of consumer needs propagated to producer |
| Gap 4: Union/Intersect/Except branches independent | `tag_union` position-aligned propagation (§5.7) | `PruneUnionColumns` / `PruneIntersectColumns` / `PruneExceptColumns` filter `output_columns` + `child_outputs` | golden plan `prune_union_branch_alignment.sql`: 3-branch UNION ALL → all branches drop same positions |
| Gap 5: Aggregate / Window output_columns 不裁 | `tag_aggregate` / `tag_window` filter outputs by parent_needed | `PruneAggregateColumns` / `PruneWindowColumns` filter `output_columns` + `aggregates` / `window_exprs` | golden plan `prune_aggregate_unused_agg.sql` + `prune_window_unused_output.sql` |

## 9. Testing Strategy

Three gates, all hard requirements for PR-α merge.

### 9.1 Gate 1: SQL suite -j 1 全绿

- `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite join,cte --mode verify -j 1`
- Expected: 57/60 join + 100% cte pass (matches current baseline; PR-α does not regress).
- `--suite tpc-ds --only q11,q14,q35,q69` (CTE / SubqueryAlias hot cases): 100% pass.
- `join` suite wall_time: **≥ 30% reduction** vs current (1996s → ≤ 1400s, conservative).

### 9.2 Gate 2: golden plan tests

New SQL files under `sql-tests/optimizer/`:

| File | Validates | Notes |
|---|---|---|
| `prune_subquery_alias_cte_left_semi.sql` | Gap 1 + Gap 3 | The canonical q22-shape; `-- @explain_contains` locks Scan to 2 cols + SubqueryAlias to 1 col |
| `prune_project_items_filter_only.sql` | Gap 2 | Wide-table SELECT a FROM (SELECT * FROM t) |
| `prune_cte_anchor_multi_consume.sql` | Gap 3 (multi-consumer) | Tests union-of-needs across 2 CTEConsume sites |
| `prune_union_branch_alignment.sql` | Gap 4 | 3-branch UNION ALL with outer projection of subset cols |
| `prune_intersect_branch_alignment.sql` | Gap 4 (Intersect) | 2-branch INTERSECT |
| `prune_aggregate_unused_agg.sql` | Gap 5 | SELECT only group_by from `... GROUP BY ... HAVING ... agg(...)` |
| `prune_window_unused_output.sql` | Gap 5 (Window) | Window with multiple OVER(), outer query uses only one |
| `prune_idempotent_fixed_point.sql` | Architecture | Pipeline applied twice produces identical plan |

Each uses `-- @explain_contains=<substring>` to assert plan shape facts.

### 9.3 Gate 3: per-rule unit tests

Each of the 18 rule files contains at minimum:

- 1 positive test (rule fires and modifies plan as expected)
- 1 no-op test (`required_output_columns = None` or already-converged plan ⇒ returns `None`)

`required_columns.rs::tag_*` per-operator branches each get at least 1 test covering the tagging behavior.

The 4 existing tests in `column_pruning.rs` (`root_scan_without_parent_keeps_all_columns`, `project_selecting_one_col_prunes_scan_required_columns`, `filter_predicate_columns_are_preserved_in_scan_required`, `aggregate_group_by_and_agg_args_propagate_to_scan`) migrate into `prune_project.rs`, `prune_filter.rs`, `prune_aggregate.rs`, `prune_scan.rs` respectively, ensuring no coverage loss.

### 9.4 Bonus (not hard gate, but to include in PR description)

- `EXPLAIN COSTS` diff for `join_one_key` q22 + `join_linear_chained` q31 + simple INNER count(*), showing before/after for plan structure.
- `cargo test --lib` full pass (~2800 lib unit tests, runs in CI).

## 10. Out of Scope

The following are explicitly **not** in PR-α; each gets its own future task:

1. **Eliminating `SubqueryAlias` logical operator (StarRocks parity)**: requires analyzer changes. Tracked as OQ-1.6.
2. **Advanced multi-consumer CTE scenarios**: PR-α's golden test `prune_cte_anchor_multi_consume.sql` exercises basic 2-consumer behavior to verify the union-of-needs logic. Out of scope are: 3+ consumers with disjoint/nested needed sets, nested-CTE consumers (CTE referencing CTE), and recursive CTE. Tracked as OQ-1.7.
3. **`PruneEmpty*` family** (cardinality-0 subtree elimination): different optimization concern. Tracked as OQ-X (new task).
4. **Physical-plan column pruning** (`PruneShuffleColumn`-style): runs after Cascades on physical plan. Tracked as OQ-1.8.
5. **UK/FK constraint propagation refinement**: existing `rules/ukfk.rs` stays; PR-α does not refactor it.
6. **OQ-2 through OQ-7** (NULL filter inference, cardinality, SplitAgg, runtime filter, etc.): independent roadmap tasks.

## 11. Success Criteria

PR-α merges only when **all** of the following hold:

1. Three test gates pass (§9).
2. `join_one_key` q22 EXPLAIN: right side `SubqueryAlias t2` shows `req=[c_tinyint_null]`; right-side Scan reads exactly 2 columns; left-side Scan reads exactly 2 columns.
3. `join` suite -j 1 wall_time ≥ 30% reduction from current 1996s baseline.
4. `cargo test --lib` 100% pass.
5. PR description includes before/after plan diffs for 3 standard probe queries.
6. The 4 pre-existing column-pruning unit tests have functional equivalents in the new per-rule files (zero coverage regression).

## 12. Open Questions

1. **`UnionNode.output_columns` field add-on**: PR-α adds an explicit `output_columns: Vec<OutputColumn>` to UnionNode/IntersectNode/ExceptNode for position-aligned pruning. Currently the implicit schema is derived from `inputs[0]`. Switching from implicit to explicit means existing analyzer code that constructs Union nodes must populate this field. Mechanical work; tracked inside PR-α.

2. **`TagRequiredColumnsRule` re-entry semantics**: if a downstream rule (in `LowCardinalityDictionaryRewrite` stage) modifies the plan structure, should we re-run tagging? Current design says no — tagging runs once per `query_rewrite_pipeline` invocation. If a later rule needs fresh needed sets, it must invalidate `required_output_columns` to `None` on affected nodes; subsequent Prune* rules will then no-op safely. PR-α audits all rules after ColumnPruning to confirm they either don't change plan structure or properly invalidate.

3. **`auto_fill_one_item` ColumnId minting**: requires access to `ColumnRefFactory`. PR-α threads this through the `RewriteContext` (already passed to rule.apply via the existing framework — need to confirm).

---

## Appendix: Worked Example (q22)

**Before PR-α** (current NovaRocks plan):

```
PROJECT [count(1), count(t1.k1), count(t1.c_tinyint_null)]
  HASH AGGREGATE (SINGLE)
    GATHER EXCHANGE
      HASH JOIN (BROADCAST, LEFT SEMI, eq: t1.c_tinyint_null = t2.c_tinyint_null)
        SCAN t1 (left)
             columns: k1, c_tinyint_null                          ← OK
        GATHER EXCHANGE
          SUBQUERY ALIAS [t2]
            PROJECT [33 items, identity rename]                   ← Gap 2: 33 → should be 1
              SCAN t1 (right)
                   columns: k1, c_bool, c_bool_null, ... (33)     ← Gap 1+3: 33 → should be 2
                   predicates: k1 < 100
```

**After PR-α** (target):

```
PROJECT [count(1), count(t1.k1), count(t1.c_tinyint_null)] req=[count, count_k1, count_tinyint]
  HASH AGGREGATE (SINGLE) req=[count, count_k1, count_tinyint]
    GATHER EXCHANGE
      HASH JOIN (BROADCAST, LEFT SEMI) req=[k1, c_tinyint_null]
        SCAN t1 (left) req=[k1, c_tinyint_null]
             columns: k1, c_tinyint_null
        GATHER EXCHANGE
          SUBQUERY ALIAS [t2] req=[c_tinyint_null]                ← Gap 1 ✓
            PROJECT [c_tinyint_null] req=[c_tinyint_null]         ← Gap 2 ✓: 33 → 1
              SCAN t1 (right) req=[k1, c_tinyint_null]            ← Gap 3 ✓: 33 → 2
                   columns: k1, c_tinyint_null
                   predicates: k1 < 100
```

Build-side scan: 33 cols → 2 cols. SubqueryAlias output: 33 cols → 1 col. Projected items: 33 → 1. Bandwidth, hash-table footprint, and exchange cost all drop proportionally.
