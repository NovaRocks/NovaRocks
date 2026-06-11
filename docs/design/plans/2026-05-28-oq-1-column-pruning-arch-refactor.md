# OQ-1 Column Pruning Architecture Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single 562-line `PruneColumns` rewrite rule with a per-operator rule architecture aligned with StarRocks, closing 5 column-pruning gaps that cause `SELECT *`-through-CTE/SubqueryAlias bandwidth amplification.

**Architecture:** Two-phase walk — Phase 1 `tag_required_columns` (top-down pass writing `required_output_columns: Option<HashSet<ColumnId>>` on every operator) followed by Phase 2 fixed-point loop of 18 per-operator `Prune*Columns` rules (bottom-up). ColumnId-based propagation makes SubqueryAlias transparent (Gap 1) and unblocks all five gaps without changing the rule trait or driver.

**Tech Stack:** Rust, existing `src/sql/optimizer/rewrite/` framework (`LogicalRewriteRule` trait, `RewriteContext`, `RewriteStage`, bottom-up/top-down `tree.rs` driver), `ColumnId` / `ColumnRefFactory` from `src/sql/column_id.rs`.

**Spec reference:** `docs/design/specs/2026-05-28-oq-1-column-pruning-arch-refactor-design.md`

---

## File Structure

### New files

| Path | Responsibility |
|---|---|
| `src/sql/optimizer/rewrite/required_columns.rs` | Phase 1: `tag_required_columns()` walker + per-operator branches + small helpers (`collect_column_ids`, `collect_output_ids`, `collect_output_ids_ordered`, `auto_fill_column_id`) |
| `src/sql/optimizer/rewrite/rules/column_pruning/mod.rs` | Re-exports + `column_pruning_rules()` registry function |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_scan.rs` | `PruneScanColumns` — translates ColumnId set → `required_columns: Vec<String>` |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_project.rs` | `PruneProjectColumns` — filters `items` (Gap 2) |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_filter.rs` | `PruneFilterColumns` — no-op rule, kept for symmetry |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_aggregate.rs` | `PruneAggregateColumns` — filters `output_columns` + `aggregates` (Gap 5) |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_join.rs` | `PruneJoinColumns` — no-op rule |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_sort.rs` | `PruneSortColumns` — no-op rule |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_limit.rs` | `PruneLimitColumns` — no-op rule |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_window.rs` | `PruneWindowColumns` — filters `output_columns` + `window_exprs` (Gap 5) |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_union.rs` | `PruneUnionColumns` — position-aligned filter (Gap 4) |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_intersect.rs` | `PruneIntersectColumns` — same pattern as Union |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_except.rs` | `PruneExceptColumns` — same pattern as Union |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_subquery_alias.rs` | `PruneSubqueryAliasColumns` — filter `output_columns` (Gap 1) |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_cte_anchor.rs` | `PruneCTEAnchorColumns` — no-op rule |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_cte_consume.rs` | `PruneCTEConsumeColumns` — filter `output_columns` |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_cte_produce.rs` | `PruneCTEProduceColumns` — filter `output_columns` |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_repeat.rs` | `PruneRepeatColumns` — no-op rule |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_decode.rs` | `PruneDecodeColumns` — filter `mappings` + `output_columns` |
| `src/sql/optimizer/rewrite/rules/column_pruning/prune_table_function.rs` | `PruneTableFunctionColumns` — no-op rule |
| `sql-tests/optimizer/prune_subquery_alias_cte_left_semi.sql` | Golden: Gap 1 + Gap 3 |
| `sql-tests/optimizer/prune_project_items_filter_only.sql` | Golden: Gap 2 |
| `sql-tests/optimizer/prune_cte_anchor_multi_consume.sql` | Golden: Gap 3 multi-consumer |
| `sql-tests/optimizer/prune_union_branch_alignment.sql` | Golden: Gap 4 |
| `sql-tests/optimizer/prune_intersect_branch_alignment.sql` | Golden: Gap 4 Intersect |
| `sql-tests/optimizer/prune_aggregate_unused_agg.sql` | Golden: Gap 5 |
| `sql-tests/optimizer/prune_window_unused_output.sql` | Golden: Gap 5 Window |
| `sql-tests/optimizer/prune_idempotent_fixed_point.sql` | Golden: convergence sanity |

### Files to modify

| Path | What changes |
|---|---|
| `src/sql/planner/plan.rs` | Add `required_output_columns: Option<HashSet<ColumnId>>` to 18 node structs; add `output_columns: Vec<OutputColumn>` to `UnionNode`/`IntersectNode`/`ExceptNode` |
| `src/sql/planner/mod.rs` | Populate `output_columns` when constructing Union/Intersect/Except nodes |
| `src/sql/optimizer/rewrite/context.rs` | Add `column_ref_factory: Option<Rc<RefCell<ColumnRefFactory>>>` slot for auto_fill ColumnId minting |
| `src/sql/optimizer/mod.rs` | Pass factory into `RewriteContext` before pipeline.rewrite |
| `src/sql/optimizer/rewrite/registry.rs` | Replace `"ColumnPruning"` stage with `"TagRequiredColumns"` + `"ColumnPruning"` |
| `src/sql/optimizer/rewrite/rules/mod.rs` | Re-export the new `column_pruning` module, update `column_pruning_rules()` |
| `src/sql/optimizer/rewrite/rules/utils.rs` | Add `collect_column_id_refs` helper (alongside existing `collect_column_refs`) |
| `src/sql/explain.rs` | Emit `req=[col_a, col_b]` per operator in Verbose / Costs mode |

### Files to delete

| Path | Why |
|---|---|
| `src/sql/optimizer/rewrite/rules/column_pruning.rs` | Old single 562-line rule; superseded by `column_pruning/` directory. The 4 unit tests migrate into per-rule files. |

---

## Tasks

The plan has 35 tasks organized in 5 phases. Phase A lays the foundation (struct fields + helpers). Phase B implements the tagging pass operator-by-operator. Phase C implements the 18 per-operator pruning rules. Phase D handles the cutover (delete old + EXPLAIN). Phase E adds golden tests and final verification.

Each task follows TDD: write failing test → run to confirm fail → implement → run to confirm pass → commit. For purely mechanical struct field additions (Tasks 1, 2), the "test" is `cargo build` succeeding after construction sites are updated.

---

## Phase A: Foundation

### Task 1: Thread `ColumnRefFactory` into `RewriteContext`

The auto-fill safeguard (when a Project/Union prunes to empty, we need to mint a new ColumnId for a placeholder constant column) requires factory access. We thread a shared factory through `RewriteContext`.

**Files:**
- Modify: `src/sql/optimizer/rewrite/context.rs:98+` (add field)
- Modify: `src/sql/optimizer/mod.rs:64-90` (pass factory in)

- [ ] **Step 1.1: Read current context.rs structure**

Read `src/sql/optimizer/rewrite/context.rs` to understand the existing struct shape and constructors.

- [ ] **Step 1.2: Add factory slot to RewriteContext**

In `src/sql/optimizer/rewrite/context.rs`, add to `RewriteContext`:

```rust
use std::cell::RefCell;
use std::rc::Rc;
use crate::sql::column_id::ColumnRefFactory;

pub(crate) struct RewriteContext {
    // ... existing fields ...
    column_ref_factory: Option<Rc<RefCell<ColumnRefFactory>>>,
}

impl RewriteContext {
    pub(crate) fn set_column_ref_factory(
        &mut self,
        factory: Rc<RefCell<ColumnRefFactory>>,
    ) {
        self.column_ref_factory = Some(factory);
    }

    pub(crate) fn column_ref_factory(&self) -> Option<&Rc<RefCell<ColumnRefFactory>>> {
        self.column_ref_factory.as_ref()
    }
}
```

Also add `column_ref_factory: None` to every constructor (`for_query`, `for_mv_refresh`, etc.).

- [ ] **Step 1.3: Write unit test for factory threading**

In the existing `#[cfg(test)] mod tests` of `context.rs`, add:

```rust
#[test]
fn column_ref_factory_can_be_set_and_read() {
    use std::cell::RefCell;
    use std::rc::Rc;
    use crate::sql::column_id::ColumnRefFactory;

    let mut ctx = RewriteContext::for_query(Vec::<String>::new());
    assert!(ctx.column_ref_factory().is_none());

    let factory = Rc::new(RefCell::new(ColumnRefFactory::default()));
    ctx.set_column_ref_factory(Rc::clone(&factory));

    assert!(ctx.column_ref_factory().is_some());
}
```

- [ ] **Step 1.4: Run test to verify it passes**

```bash
cargo test --lib sql::optimizer::rewrite::context::tests::column_ref_factory_can_be_set_and_read -- --nocapture
```

Expected: PASS.

- [ ] **Step 1.5: Pass factory from optimize() into context**

In `src/sql/optimizer/mod.rs` around line 64-78, change `optimize()`:

```rust
pub(crate) fn optimize(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
    factory: ColumnRefFactory,
    dictionary_provider: Option<...>,
) -> Result<PhysicalPlanNode, String> {
    let deadline = Instant::now() + OPTIMIZE_TIMEOUT;
    let factory = std::rc::Rc::new(std::cell::RefCell::new(factory));

    let session_settings = options::current_session_optimizer_settings();
    let options = options::OptimizerOptions::from_session(&session_settings);
    let mut rewrite_ctx =
        rewrite::context::RewriteContext::for_query(session_settings.disabled_rules.clone());
    rewrite_ctx.policy_mut().max_iterations = options.rewrite_max_iterations;
    rewrite_ctx.set_query_table_stats(table_stats.clone());
    rewrite_ctx.set_deadline(deadline);
    rewrite_ctx.set_column_ref_factory(Rc::clone(&factory));  // ← NEW
    // ... rest unchanged
```

After pipeline.rewrite(), extract the factory back into a plain `ColumnRefFactory` for the Memo step (use `Rc::try_unwrap(factory).expect(...).into_inner()`):

```rust
let factory = std::rc::Rc::try_unwrap(factory)
    .expect("factory should have no other references after rewrite")
    .into_inner();
let mut memo = Memo::new();
memo.factory = factory;
```

- [ ] **Step 1.6: Run full lib tests to verify nothing broke**

```bash
cargo build --lib
cargo test --lib sql::optimizer::rewrite::
```

Expected: All passing.

- [ ] **Step 1.7: Commit**

```bash
git add src/sql/optimizer/rewrite/context.rs src/sql/optimizer/mod.rs
git commit -m "feat(optimizer): thread ColumnRefFactory through RewriteContext for auto-fill column minting"
```

---

### Task 2: Add `output_columns: Vec<OutputColumn>` to UnionNode/IntersectNode/ExceptNode

Position-aligned column pruning (Gap 4) requires each set-op node to carry an explicit output schema. Currently these are implicit (derived from `inputs[0]`).

**Files:**
- Modify: `src/sql/planner/plan.rs:298-313` (add field to 3 structs)
- Modify: `src/sql/planner/mod.rs` (every site that constructs UnionNode/IntersectNode/ExceptNode)

- [ ] **Step 2.1: Inventory all construction sites**

```bash
grep -rnE "UnionNode \{|IntersectNode \{|ExceptNode \{|UnionNode\b|IntersectNode\b|ExceptNode\b" src/ tests/ | grep -v "\.md\|test"
```

Note each site for later updates.

- [ ] **Step 2.2: Add field to all 3 structs**

In `src/sql/planner/plan.rs`:

```rust
#[derive(Clone, Debug)]
pub(crate) struct UnionNode {
    pub inputs: Vec<LogicalPlan>,
    pub all: bool,
    pub output_columns: Vec<crate::sql::analysis::OutputColumn>,  // ← NEW
}

#[derive(Clone, Debug)]
pub(crate) struct IntersectNode {
    pub inputs: Vec<LogicalPlan>,
    pub output_columns: Vec<crate::sql::analysis::OutputColumn>,  // ← NEW
}

#[derive(Clone, Debug)]
pub(crate) struct ExceptNode {
    pub inputs: Vec<LogicalPlan>,
    pub output_columns: Vec<crate::sql::analysis::OutputColumn>,  // ← NEW
}
```

- [ ] **Step 2.3: Update all UnionNode construction sites**

**CRITICAL CORRECTNESS REQUIREMENT (corrected after investigation — do NOT use first-branch IDs).**

A set-op node's `output_columns` MUST carry the **fresh set-op output ColumnIds** that the analyzer allocated for the set-op result — i.e. the `ResolvedQuery.output_columns` of the set-op query itself. These are the IDs the parent scope registers and the IDs that any parent operator's expressions reference. They are **distinct** from both the left and right branch ColumnIds.

Why this matters: Gap-4 pruning (Task 11 `tag_union`, Task 20 `PruneUnionColumns`) maps `required_output_columns` (parent-referenced IDs) to branch positions via `UnionNode.output_columns[i].column_id`. If `output_columns` carried left-branch IDs instead, that ID-based lookup would match nothing and prune everything. (An earlier draft of this plan wrongly said "use `inputs[0]` schema" — that is the bug; the parent references fresh set-op IDs, verified by tracing `analyze_set_expr` → scope registration in `resolve_from.rs` → `plan_set_operation_scoped`.)

Implementation:
- The fresh IDs live in the enclosing `ResolvedQuery.output_columns`, which `plan_scoped_query` already has in scope (it destructures the query into body + output_columns + modifiers). Thread those `output_columns` down so the set-op node stamps them onto its `output_columns` field — OR, more simply, in `plan_scoped_query`, after the body is planned, if the resulting node is a `Union`/`Intersect`/`Except`, overwrite its `output_columns` with the query's fresh `output_columns`. Recursion then handles nested set-ops correctly: each `ResolvedQuery` (including nested-union branches) stamps ITS own fresh output IDs onto ITS set-op node.
- Branch-internal IDs are NOT lost — they remain on the branch child nodes and are recovered positionally at prune time via `collect_output_ids_ordered(child)` (Task 4 / Task 11). The set-op node declares the fresh IDs; each branch keeps its own.
- For passthrough sites (cte_rewrite, tree.rs, the soon-to-be-deleted column_pruning.rs): propagate the existing `node.output_columns` unchanged. For test-only sites: `vec![]` is fine.

**Regression guard (add as a test in Step 2.5):** for `SELECT x, y FROM (SELECT a, b FROM t1 UNION ALL SELECT c, d FROM t2) sub`, the planned `SubqueryAlias.output_columns` ColumnIds (fresh IDs) MUST equal the child `Union.output_columns` ColumnIds, position by position. Before the fix they differ; after the fix they match. This is the canonical proof the set-op node now carries parent-referenced IDs.

- [ ] **Step 2.4: Build to find missed sites**

```bash
cargo build --lib 2>&1 | grep -E "error\[" | head
```

Iterate: any error like "missing field `output_columns`" → add population. Expected: zero errors.

- [ ] **Step 2.5: Write unit test for output_columns persistence**

In `src/sql/planner/plan.rs` test module add:

```rust
#[test]
fn union_node_carries_explicit_output_columns() {
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    let cols = vec![OutputColumn {
        column_id: ColumnId::UNSET,
        name: "x".to_string(),
        data_type: DataType::Int32,
        nullable: false,
    }];
    let node = UnionNode {
        inputs: vec![],
        all: true,
        output_columns: cols.clone(),
    };
    assert_eq!(node.output_columns.len(), 1);
    assert_eq!(node.output_columns[0].name, "x");
}
```

- [ ] **Step 2.6: Run all lib tests**

```bash
cargo test --lib
```

Expected: All passing. No existing test should break since the field is purely additive and constructors all populate it.

- [ ] **Step 2.7: Commit**

```bash
git add src/sql/planner/plan.rs src/sql/planner/mod.rs
git commit -m "feat(planner): add explicit output_columns to UnionNode/IntersectNode/ExceptNode"
```

---

### Task 3: Add `required_output_columns: Option<HashSet<ColumnId>>` to all plan variants

Mechanical addition of the metadata field that Phase 1 will populate. Defaults to `None` everywhere.

**Files:**
- Modify: `src/sql/planner/plan.rs` (every internal node struct)
- Modify: every `LogicalPlan::Xxx(...) = ...` construction site (broad: planner + analyzer + tests + cte_rewrite + existing column_pruning)

- [ ] **Step 3.1: Add field to every node struct**

In `src/sql/planner/plan.rs`, add to the following 18 structs:

`ScanNode`, `FilterNode`, `ProjectNode`, `AggregateNode`, `JoinNode`, `SortNode`, `LimitNode`, `WindowNode`, `UnionNode`, `IntersectNode`, `ExceptNode`, `SubqueryAliasNode`, `CTEAnchorNode`, `CTEProduceNode`, `CTEConsumeNode`, `RepeatPlanNode`, `DecodeNode`, `TableFunctionNode`, `ValuesNode`, `GenerateSeriesNode`.

For each struct, add:

```rust
use std::collections::HashSet;
use crate::sql::column_id::ColumnId;

#[derive(Clone, Debug)]
pub(crate) struct ProjectNode {
    pub input: Box<LogicalPlan>,
    pub items: Vec<ProjectItem>,
    pub required_output_columns: Option<HashSet<ColumnId>>,  // ← NEW
}
```

Repeat for all 20 structs (18 internal + 2 leaves: Values, GenerateSeries).

- [ ] **Step 3.2: Add `Default::default()` to all construction sites**

Build:
```bash
cargo build --lib 2>&1 | grep "missing field" | head
```

For each error, add `required_output_columns: None` (or `Default::default()`) to the struct literal. Use a code search:

```bash
grep -rn "ProjectNode {" src/ tests/ | wc -l
```

There will be many sites (~50+). Process them mechanically.

For sites that use `..node` struct-update syntax (e.g., `ProjectNode { input: ..., items: ..., ..node }`), they automatically inherit the field — no change needed.

- [ ] **Step 3.3: Build verifies clean**

```bash
cargo build --lib 2>&1 | grep -E "error\[" | head
```

Expected: zero errors.

- [ ] **Step 3.4: Unit test field defaults None**

In `src/sql/planner/plan.rs` test module add:

```rust
#[test]
fn project_node_required_output_columns_defaults_none() {
    let node = ProjectNode {
        input: Box::new(LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
            required_output_columns: None,
        })),
        items: vec![],
        required_output_columns: None,
    };
    assert!(node.required_output_columns.is_none());
}
```

- [ ] **Step 3.5: Run all tests**

```bash
cargo test --lib
```

Expected: all pass.

- [ ] **Step 3.6: Commit**

```bash
git add src/sql/planner/plan.rs src/
git commit -m "feat(planner): add required_output_columns: Option<HashSet<ColumnId>> to all plan variants"
```

---

### Task 4: Helper functions in `rules/utils.rs`

Add the helpers Phase 1 needs.

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/utils.rs`

- [ ] **Step 4.1: Write failing tests**

In `src/sql/optimizer/rewrite/rules/utils.rs` test module:

```rust
#[cfg(test)]
mod column_id_helper_tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, TypedExpr, BinOp};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn col_ref_with_id(id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: None,
                column: format!("c{id}"),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    #[test]
    fn collect_column_id_refs_from_simple_column_ref() {
        let expr = col_ref_with_id(7);
        let ids = collect_column_id_refs(&expr);
        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&ColumnId::new_for_test(7)));
    }

    #[test]
    fn collect_column_id_refs_from_binary_op() {
        let expr = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref_with_id(3)),
                op: BinOp::Eq,
                right: Box::new(col_ref_with_id(5)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let ids = collect_column_id_refs(&expr);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&ColumnId::new_for_test(3)));
        assert!(ids.contains(&ColumnId::new_for_test(5)));
    }

    #[test]
    fn collect_column_id_refs_ignores_unset() {
        let expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: "x".to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        };
        let ids = collect_column_id_refs(&expr);
        assert!(ids.is_empty(), "UNSET should not be collected");
    }
}
```

(If `ColumnId::new_for_test` doesn't exist, add a small `#[cfg(test)]` constructor to `column_id.rs`.)

- [ ] **Step 4.2: Run test to confirm fail**

```bash
cargo test --lib sql::optimizer::rewrite::rules::utils::column_id_helper_tests -- --nocapture
```

Expected: compile error "collect_column_id_refs not found".

- [ ] **Step 4.3: Implement collect_column_id_refs**

In `src/sql/optimizer/rewrite/rules/utils.rs` add:

```rust
use std::collections::HashSet;
use crate::sql::column_id::ColumnId;

/// Recursively collect every `ColumnId` referenced by `expr`. Ignores
/// `ColumnId::UNSET` (which indicates an un-resolved reference and should
/// not constrain pruning).
pub(crate) fn collect_column_id_refs(expr: &TypedExpr) -> HashSet<ColumnId> {
    let mut acc = HashSet::new();
    walk_collect(&mut acc, expr);
    acc
}

fn walk_collect(acc: &mut HashSet<ColumnId>, expr: &TypedExpr) {
    use crate::sql::analysis::ExprKind::*;
    match &expr.kind {
        ColumnRef { column_id, .. } => {
            if *column_id != ColumnId::UNSET {
                acc.insert(*column_id);
            }
        }
        BinaryOp { left, right, .. } => {
            walk_collect(acc, left);
            walk_collect(acc, right);
        }
        UnaryOp { expr, .. } => walk_collect(acc, expr),
        FunctionCall { args, .. } | Coalesce { exprs: args, .. } => {
            for arg in args {
                walk_collect(acc, arg);
            }
        }
        // ... handle every other ExprKind variant
        Literal(_) => {}
        _ => { /* exhaustively enumerate or use catch-all + match-completeness build error */ }
    }
}
```

(Refer to the existing `collect_column_refs` in the same file as a template — it already handles every `ExprKind` variant by name; mirror that exhaustive match.)

- [ ] **Step 4.4: Run tests, see them pass**

```bash
cargo test --lib sql::optimizer::rewrite::rules::utils::column_id_helper_tests
```

Expected: 3 passing.

- [ ] **Step 4.5: Write failing test for `collect_output_ids`**

```rust
#[test]
fn collect_output_ids_from_scan_returns_all_columns() {
    let scan = LogicalPlan::Scan(ScanNode {
        database: "d".to_string(),
        table: three_col_table(),  // a,b,c with ColumnIds 1,2,3
        alias: None,
        columns: vec![
            OutputColumn { column_id: ColumnId::new_for_test(1), name: "a".to_string(), data_type: DataType::Int32, nullable: false },
            OutputColumn { column_id: ColumnId::new_for_test(2), name: "b".to_string(), data_type: DataType::Utf8, nullable: true },
            OutputColumn { column_id: ColumnId::new_for_test(3), name: "c".to_string(), data_type: DataType::Float64, nullable: true },
        ],
        predicates: vec![],
        required_columns: None,
        required_output_columns: None,
        dict_columns: vec![],
    });
    let ids = collect_output_ids(&scan);
    assert_eq!(ids.len(), 3);
    assert!(ids.contains(&ColumnId::new_for_test(1)));
    assert!(ids.contains(&ColumnId::new_for_test(2)));
    assert!(ids.contains(&ColumnId::new_for_test(3)));
}
```

- [ ] **Step 4.6: Run test to confirm fail**

Expected: `collect_output_ids not found`.

- [ ] **Step 4.7: Implement `collect_output_ids` and `collect_output_ids_ordered`**

```rust
/// Set of every ColumnId that appears in a plan subtree's output schema.
pub(crate) fn collect_output_ids(plan: &LogicalPlan) -> HashSet<ColumnId> {
    collect_output_ids_ordered(plan).into_iter().collect()
}

/// Same as `collect_output_ids` but preserves output order. Required by
/// position-aligned Union/Intersect/Except handlers.
pub(crate) fn collect_output_ids_ordered(plan: &LogicalPlan) -> Vec<ColumnId> {
    match plan {
        LogicalPlan::Scan(s) => s.columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::Project(p) => p.items.iter().map(|i| i.output_column_id).collect(),
        LogicalPlan::Aggregate(a) => a.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::Window(w) => w.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::SubqueryAlias(s) => s.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::CTEProduce(p) => p.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::CTEConsume(c) => c.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::Union(u) => u.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::Intersect(i) => i.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::Except(e) => e.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::Values(v) => v.columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::GenerateSeries(g) => vec![g.output_column_id],
        LogicalPlan::Decode(d) => d.output_columns.iter().map(|c| c.column_id).collect(),
        // Pass-throughs (output = input output)
        LogicalPlan::Filter(f) => collect_output_ids_ordered(&f.input),
        LogicalPlan::Sort(s) => collect_output_ids_ordered(&s.input),
        LogicalPlan::Limit(l) => collect_output_ids_ordered(&l.input),
        LogicalPlan::Join(j) => {
            let mut out = collect_output_ids_ordered(&j.left);
            out.extend(collect_output_ids_ordered(&j.right));
            out
        }
        LogicalPlan::TableFunction(t) => collect_output_ids_ordered(&t.input),
        LogicalPlan::Repeat(r) => collect_output_ids_ordered(&r.input),
        LogicalPlan::CTEAnchor(c) => collect_output_ids_ordered(&c.consumer),
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker should not appear in non-IMV pruning")
        }
    }
}
```

- [ ] **Step 4.8: Run tests**

```bash
cargo test --lib sql::optimizer::rewrite::rules::utils
```

Expected: all passing.

- [ ] **Step 4.9: Commit**

```bash
git add src/sql/optimizer/rewrite/rules/utils.rs src/sql/column_id.rs
git commit -m "feat(rewrite): collect_column_id_refs + collect_output_ids[_ordered] helpers"
```

---

### Task 4b: Add `output_column_id: ColumnId` to `ProjectItem`

**Why this task exists (discovered during Task 4 + an architectural investigation):** The whole design rests on a `HashSet<ColumnId>` "needed set". An investigation of how column references resolve across operator boundaries (cited in the spec; key facts: `column_id.rs:26-27` invariant, `resolve_from.rs:506-514` `add_column_with_id`, `analyzer/mod.rs:1418-1429`) established that the ColumnId space IS coherent across Project / SubqueryAlias / derived-table boundaries — a parent's reference to a projected column carries the SAME ColumnId the analyzer minted for that output column. The breaks are only at set-ops and CTEConsume (handled by position). BUT: that minted output ColumnId for a Project column is stored ONLY in the analyzer's separate `output_columns` list — `ProjectItem` itself (`src/sql/analysis/mod.rs`) carries only `expr` + `output_name`, NO output id. So `tag_project` / `PruneProjectColumns` / `collect_output_ids_ordered(Project)` cannot key a Project's output columns by ColumnId. This task adds that id to `ProjectItem`, making the spec's `item.output_column_id` references (§5.2, §6.2) real. Without it, Project pruning silently drops computed columns and Project output-id matching fails.

**Files:**
- Modify: `src/sql/analysis/mod.rs` (add field to `ProjectItem`; stamp it in `analyze_projection`)
- Modify: ~12 files / 38 `ProjectItem { ... }` construction sites (analyzer, planner, optimizer rewrite rules, mv_ddl, tests)
- Modify: `src/sql/optimizer/rewrite/rules/utils.rs` (fix `collect_output_ids_ordered` Project arm to use `output_column_id`)

- [ ] **Step 4b.1: Add the field**

```rust
// src/sql/analysis/mod.rs
pub(crate) struct ProjectItem {
    pub expr: TypedExpr,
    pub output_name: String,
    pub output_column_id: crate::sql::column_id::ColumnId,  // NEW — the ColumnId parents reference for this output column
}
```

- [ ] **Step 4b.2: Stamp the id at the analyzer site**

In `analyze_projection` (`src/sql/analysis/mod.rs` ~line 1383-1451), the code already computes `column_id` for each select item (passthrough → reuse `ColumnRef`'s id; computed → `alloc_column_id`) and pushes it to `output_columns`. Carry that SAME `column_id` onto the `ProjectItem` it builds in lock-step. There are two select-item arms (`UnnamedExpr` and `ExprWithAlias`) — both compute a `column_id`; thread it into the `ProjectItem { ... }` literal. Confirm `output_columns` and the projection `Vec<ProjectItem>` stay positionally 1:1.

- [ ] **Step 4b.3: Populate all other construction sites**

```bash
cargo build --lib 2>&1 | grep -E "missing field .output_column_id" | head -50
```

For each flagged site:
- **Reconstruction sites** (predicate_pushdown `push_through_project`, `ukfk`, `tree.rs`, the old `column_pruning.rs`, `aggregate_pushdown/collector`): these transform an EXISTING item's expr but keep its output identity — carry the existing `item.output_column_id` through (`output_column_id: item.output_column_id`, or `..item` spread).
- **New-item sites that wrap an existing column** (planner synthetic projections at `planner/mod.rs` sort/window sites, `subquery_rewrite`): the new item is typically a `ColumnRef` to an existing column — use that `ColumnRef`'s `column_id` as the `output_column_id`.
- **New-item sites that create a genuinely new computed/decoded column** (`low_cardinality_dict/rewriter`): mint a fresh id via the `ColumnRefFactory` now available through `ctx.column_ref_factory()` (threaded in Task 1). If a site has no ctx/factory access, prefer reusing the wrapped column's id; only mint when semantically a new column.
- **`mv_ddl.rs` + test sites**: use the wrapped expr's `ColumnRef` id where passthrough, or `ColumnId::UNSET` for test items that are never pruned. (UNSET is acceptable ONLY where the item's output id is never consumed by pruning — tests and DDL paths.)

Iterate `cargo build --lib` until zero errors.

- [ ] **Step 4b.4: Fix `collect_output_ids_ordered` Project arm**

In `src/sql/optimizer/rewrite/rules/utils.rs`, the `LogicalPlan::Project` arm currently extracts an id only when `item.expr` is a `ColumnRef` (dropping computed columns). Replace it with:
```rust
LogicalPlan::Project(p) => p.items.iter()
    .map(|item| item.output_column_id)
    .filter(|id| *id != ColumnId::UNSET)
    .collect(),
```

- [ ] **Step 4b.5: Tests**

- Add a test that a COMPUTED project item (`a + b AS c`) gets a non-UNSET `output_column_id` after analysis, and that `collect_output_ids_ordered` on a Project with one passthrough + one computed item returns BOTH ids.
- `cargo build --lib` clean; `cargo test --lib` — expect all passing (the existing ~3149 plus new). Some analyzer/planner tests may need `output_column_id` added to their `ProjectItem` literals — that's expected mechanical churn, not a behavior change.

- [ ] **Step 4b.6: Commit**

```bash
git add -A
git commit -m "feat(analyzer): add output_column_id to ProjectItem so Project outputs are ColumnId-addressable for pruning"
```

**Note for downstream tasks:** Task 6 (`tag_project`) and Task 16 (`PruneProjectColumns`) now key off `item.output_column_id` (as the spec already shows). The earlier Task 4 `collect_output_ids_ordered` Project arm is superseded by Step 4b.4.

---

## Phase B: Tagging Pass

### Task 5: `required_columns.rs` module scaffold + Scan/Values/GenerateSeries handlers

**Files:**
- Create: `src/sql/optimizer/rewrite/required_columns.rs`
- Modify: `src/sql/optimizer/rewrite/mod.rs` (add `pub(crate) mod required_columns;`)

- [ ] **Step 5.1: Write failing test for module entry point**

Create `src/sql/optimizer/rewrite/required_columns.rs`:

```rust
//! Phase 1 of column pruning: top-down tagging pass that writes
//! `required_output_columns: Option<HashSet<ColumnId>>` on every operator.
//!
//! Phase 2 (per-operator `Prune*Columns` rules in
//! `rewrite/rules/column_pruning/`) reads these tags to do the actual
//! pruning. See spec
//! `docs/design/specs/2026-05-28-oq-1-column-pruning-arch-refactor-design.md`
//! §5 for per-operator semantics.

use std::collections::HashSet;

use crate::sql::column_id::ColumnId;
use crate::sql::planner::plan::LogicalPlan;

/// Walk `plan` top-down and write `required_output_columns` on every
/// operator based on `parent_needed`.
///
/// `parent_needed = None` means the root has no caller restriction
/// (i.e. all outputs are required). Internally each operator type
/// computes its child's needed set.
pub(crate) fn tag_required_columns(
    plan: LogicalPlan,
    parent_needed: Option<HashSet<ColumnId>>,
) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(_) => tag_scan(plan, parent_needed),
        LogicalPlan::Values(_) => tag_values(plan, parent_needed),
        LogicalPlan::GenerateSeries(_) => tag_generate_series(plan, parent_needed),
        // ... others stubbed for now
        other => other,  // TEMPORARY no-op; filled by later tasks
    }
}

fn tag_scan(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Scan(mut scan) = plan else { unreachable!() };
    let needed = parent_needed.unwrap_or_else(|| {
        scan.columns.iter().map(|c| c.column_id).collect()
    });
    scan.required_output_columns = Some(needed);
    LogicalPlan::Scan(scan)
}

fn tag_values(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Values(mut node) = plan else { unreachable!() };
    let needed = parent_needed.unwrap_or_else(|| {
        node.columns.iter().map(|c| c.column_id).collect()
    });
    node.required_output_columns = Some(needed);
    LogicalPlan::Values(node)
}

fn tag_generate_series(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::GenerateSeries(mut node) = plan else { unreachable!() };
    let needed = parent_needed.unwrap_or_else(|| {
        std::iter::once(node.output_column_id).collect()
    });
    node.required_output_columns = Some(needed);
    LogicalPlan::GenerateSeries(node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::ScanSource;
    use arrow::datatypes::DataType;

    fn scan_with_3_cols() -> LogicalPlan {
        use crate::sql::catalog::{ColumnDef, TableDef};
        use crate::sql::planner::plan::ScanNode;

        let table = TableDef {
            name: "t".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks { db_id: 0, table_id: 0 },
        };
        LogicalPlan::Scan(ScanNode {
            database: "d".to_string(),
            table,
            alias: None,
            columns: vec![
                OutputColumn { column_id: ColumnId::new_for_test(1), name: "a".to_string(), data_type: DataType::Int32, nullable: false },
                OutputColumn { column_id: ColumnId::new_for_test(2), name: "b".to_string(), data_type: DataType::Int32, nullable: false },
                OutputColumn { column_id: ColumnId::new_for_test(3), name: "c".to_string(), data_type: DataType::Int32, nullable: false },
            ],
            predicates: vec![],
            required_columns: None,
            required_output_columns: None,
            dict_columns: vec![],
        })
    }

    #[test]
    fn tag_scan_with_none_keeps_all_cols() {
        let plan = scan_with_3_cols();
        let tagged = tag_required_columns(plan, None);
        let LogicalPlan::Scan(s) = tagged else { panic!() };
        let req = s.required_output_columns.unwrap();
        assert_eq!(req.len(), 3);
    }

    #[test]
    fn tag_scan_with_subset_keeps_only_those() {
        let plan = scan_with_3_cols();
        let subset: HashSet<ColumnId> = std::iter::once(ColumnId::new_for_test(2)).collect();
        let tagged = tag_required_columns(plan, Some(subset.clone()));
        let LogicalPlan::Scan(s) = tagged else { panic!() };
        assert_eq!(s.required_output_columns.unwrap(), subset);
    }
}
```

Add to `src/sql/optimizer/rewrite/mod.rs`:

```rust
pub(crate) mod required_columns;
```

- [ ] **Step 5.2: Run tests to verify they pass**

```bash
cargo test --lib sql::optimizer::rewrite::required_columns
```

Expected: 2 passing.

- [ ] **Step 5.3: Commit**

```bash
git add src/sql/optimizer/rewrite/required_columns.rs src/sql/optimizer/rewrite/mod.rs
git commit -m "feat(rewrite): tag_required_columns module + Scan/Values/GenerateSeries handlers"
```

---

### Task 6: Project handler

**Files:**
- Modify: `src/sql/optimizer/rewrite/required_columns.rs`

- [ ] **Step 6.1: Write failing test**

Add to `required_columns.rs` tests module:

```rust
#[test]
fn tag_project_propagates_needed_to_input_filtered_by_output_id() {
    use crate::sql::analysis::{ProjectItem, TypedExpr, ExprKind};
    use crate::sql::planner::plan::ProjectNode;

    // Plan: Project[a, b] <- Scan[a,b,c]
    // parent_needed = {b}
    // Expected:
    //   - Project.required_output_columns = Some({b})
    //   - Scan.required_output_columns = Some({b}) (only b is referenced by surviving project items)
    let scan = scan_with_3_cols();
    let project = LogicalPlan::Project(ProjectNode {
        input: Box::new(scan),
        items: vec![
            ProjectItem {
                output_column_id: ColumnId::new_for_test(101),  // alias for a
                output_name: "a".to_string(),
                expr: col_ref_with_id(1),
            },
            ProjectItem {
                output_column_id: ColumnId::new_for_test(102),  // alias for b
                output_name: "b".to_string(),
                expr: col_ref_with_id(2),
            },
        ],
        required_output_columns: None,
    });
    let needed: HashSet<ColumnId> = std::iter::once(ColumnId::new_for_test(102)).collect();
    let tagged = tag_required_columns(project, Some(needed.clone()));

    let LogicalPlan::Project(p) = tagged else { panic!() };
    assert_eq!(p.required_output_columns.unwrap(), needed);
    let LogicalPlan::Scan(s) = *p.input else { panic!() };
    let scan_req = s.required_output_columns.unwrap();
    assert!(scan_req.contains(&ColumnId::new_for_test(2)), "scan should keep b");
    assert!(!scan_req.contains(&ColumnId::new_for_test(1)), "scan should NOT keep a");
}
```

(Define `col_ref_with_id` test helper at module top if not yet present, matching Task 4.)

- [ ] **Step 6.2: Run test, confirm it fails**

```bash
cargo test --lib sql::optimizer::rewrite::required_columns::tests::tag_project_propagates_needed_to_input_filtered_by_output_id
```

Expected: FAIL — Project hits the catch-all `other => other` and returns unchanged plan.

- [ ] **Step 6.3: Implement tag_project**

Add to `required_columns.rs`:

```rust
use crate::sql::optimizer::rewrite::rules::utils::collect_column_id_refs;

fn tag_project(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Project(mut node) = plan else { unreachable!() };
    node.required_output_columns = parent_needed.clone();
    let child_needed: HashSet<ColumnId> = node.items.iter()
        .filter(|item| match &parent_needed {
            None => true,
            Some(n) => n.contains(&item.output_column_id),
        })
        .flat_map(|item| collect_column_id_refs(&item.expr))
        .collect();
    let new_input = tag_required_columns(*node.input, Some(child_needed));
    node.input = Box::new(new_input);
    LogicalPlan::Project(node)
}
```

Wire into `tag_required_columns` match:

```rust
LogicalPlan::Project(_) => tag_project(plan, parent_needed),
```

- [ ] **Step 6.4: Run test, see it pass**

```bash
cargo test --lib sql::optimizer::rewrite::required_columns::tests::tag_project_propagates_needed_to_input_filtered_by_output_id
```

Expected: PASS.

- [ ] **Step 6.5: Commit**

```bash
git add src/sql/optimizer/rewrite/required_columns.rs
git commit -m "feat(rewrite): tag_project — filter child needed by output_column_id"
```

---

### Task 7: Filter, Sort, Limit handlers

These three are trivial — they pass needed through plus their own key/predicate columns.

- [ ] **Step 7.1: Write failing tests (combined)**

Add to tests module:

```rust
#[test]
fn tag_filter_adds_predicate_cols_to_child_needed() {
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::planner::plan::FilterNode;

    // Filter(c > 0) <- Scan[a,b,c]
    // parent_needed = {a}
    // Expected: child_needed = {a, c}
    let scan = scan_with_3_cols();
    let pred = TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(col_ref_with_id(3)),
            op: BinOp::Gt,
            right: Box::new(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(0)),
                data_type: DataType::Int32,
                nullable: false,
            }),
        },
        data_type: DataType::Boolean,
        nullable: false,
    };
    let filter = LogicalPlan::Filter(FilterNode {
        input: Box::new(scan),
        predicate: pred,
        required_output_columns: None,
    });
    let needed: HashSet<ColumnId> = std::iter::once(ColumnId::new_for_test(1)).collect();
    let tagged = tag_required_columns(filter, Some(needed));
    let LogicalPlan::Filter(f) = tagged else { panic!() };
    let LogicalPlan::Scan(s) = *f.input else { panic!() };
    let scan_req = s.required_output_columns.unwrap();
    assert!(scan_req.contains(&ColumnId::new_for_test(1)));
    assert!(scan_req.contains(&ColumnId::new_for_test(3)));
}
```

Same shape for Sort (test that sort key column is added) and Limit (test that needed passes through unchanged).

- [ ] **Step 7.2: Confirm failures**

```bash
cargo test --lib sql::optimizer::rewrite::required_columns::tests::tag_filter
```

- [ ] **Step 7.3: Implement tag_filter / tag_sort / tag_limit**

```rust
fn tag_filter(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Filter(mut node) = plan else { unreachable!() };
    node.required_output_columns = parent_needed.clone();
    let mut child_needed = parent_needed.unwrap_or_default();
    child_needed.extend(collect_column_id_refs(&node.predicate));
    node.input = Box::new(tag_required_columns(*node.input, Some(child_needed)));
    LogicalPlan::Filter(node)
}

fn tag_sort(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Sort(mut node) = plan else { unreachable!() };
    node.required_output_columns = parent_needed.clone();
    let mut child_needed = parent_needed.unwrap_or_default();
    for item in &node.items {
        child_needed.extend(collect_column_id_refs(&item.expr));
    }
    for expr in &node.analytic_partition_by {
        child_needed.extend(collect_column_id_refs(expr));
    }
    node.input = Box::new(tag_required_columns(*node.input, Some(child_needed)));
    LogicalPlan::Sort(node)
}

fn tag_limit(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Limit(mut node) = plan else { unreachable!() };
    node.required_output_columns = parent_needed.clone();
    node.input = Box::new(tag_required_columns(*node.input, parent_needed));
    LogicalPlan::Limit(node)
}
```

Wire into match.

- [ ] **Step 7.4: Run tests**

```bash
cargo test --lib sql::optimizer::rewrite::required_columns
```

Expected: all passing.

- [ ] **Step 7.5: Commit**

```bash
git add src/sql/optimizer/rewrite/required_columns.rs
git commit -m "feat(rewrite): tag_filter / tag_sort / tag_limit handlers"
```

---

### Task 8: Aggregate handler

**Files:**
- Modify: `src/sql/optimizer/rewrite/required_columns.rs`

- [ ] **Step 8.1: Write failing test**

```rust
#[test]
fn tag_aggregate_only_requests_needed_aggregate_args() {
    use crate::sql::analysis::AggregateCall;
    use crate::sql::planner::plan::AggregateNode;
    // Plan: Aggregate[group_by=[a], sum(b), avg(c)] <- Scan[a,b,c]
    //   output_columns = [a (id=1), sum_b (id=201), avg_c (id=202)]
    //   parent_needed = {sum_b}  (don't need avg_c)
    // Expected: child_needed = {a, b} (NOT c)
    let scan = scan_with_3_cols();
    let agg = LogicalPlan::Aggregate(AggregateNode {
        input: Box::new(scan),
        group_by: vec![col_ref_with_id(1)],
        aggregates: vec![
            AggregateCall { name: "sum".into(), args: vec![col_ref_with_id(2)], distinct: false, result_type: DataType::Int64, order_by: vec![] },
            AggregateCall { name: "avg".into(), args: vec![col_ref_with_id(3)], distinct: false, result_type: DataType::Float64, order_by: vec![] },
        ],
        output_columns: vec![
            OutputColumn { column_id: ColumnId::new_for_test(1), name: "a".into(), data_type: DataType::Int32, nullable: false },
            OutputColumn { column_id: ColumnId::new_for_test(201), name: "sum_b".into(), data_type: DataType::Int64, nullable: true },
            OutputColumn { column_id: ColumnId::new_for_test(202), name: "avg_c".into(), data_type: DataType::Float64, nullable: true },
        ],
        already_pushed: false,
        required_output_columns: None,
    });
    let needed: HashSet<ColumnId> = std::iter::once(ColumnId::new_for_test(201)).collect();
    let tagged = tag_required_columns(agg, Some(needed));
    let LogicalPlan::Aggregate(a) = tagged else { panic!() };
    let LogicalPlan::Scan(s) = *a.input else { panic!() };
    let scan_req = s.required_output_columns.unwrap();
    assert!(scan_req.contains(&ColumnId::new_for_test(1)), "group_by a kept");
    assert!(scan_req.contains(&ColumnId::new_for_test(2)), "sum(b) arg kept");
    assert!(!scan_req.contains(&ColumnId::new_for_test(3)), "avg(c) not needed; c dropped");
}
```

- [ ] **Step 8.2: Confirm fail**

- [ ] **Step 8.3: Implement tag_aggregate**

```rust
fn tag_aggregate(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Aggregate(mut node) = plan else { unreachable!() };
    node.required_output_columns = parent_needed.clone();

    let mut child_needed = HashSet::new();
    for gb in &node.group_by {
        child_needed.extend(collect_column_id_refs(gb));
    }
    let group_by_len = node.group_by.len();
    for (i, agg) in node.aggregates.iter().enumerate() {
        let agg_output_id = node.output_columns[group_by_len + i].column_id;
        let is_needed = match &parent_needed {
            None => true,
            Some(n) => n.contains(&agg_output_id),
        };
        if is_needed {
            for arg in &agg.args {
                child_needed.extend(collect_column_id_refs(arg));
            }
            for item in &agg.order_by {
                child_needed.extend(collect_column_id_refs(&item.expr));
            }
        }
    }
    node.input = Box::new(tag_required_columns(*node.input, Some(child_needed)));
    LogicalPlan::Aggregate(node)
}
```

- [ ] **Step 8.4: Run test, see pass**

- [ ] **Step 8.5: Commit**

```bash
git add src/sql/optimizer/rewrite/required_columns.rs
git commit -m "feat(rewrite): tag_aggregate — drop aggregate input cols when output not needed"
```

---

### Task 9: Join handler

- [ ] **Step 9.1: Write failing test**

```rust
#[test]
fn tag_join_splits_needed_by_child_outputs_and_adds_condition_cols() {
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, TypedExpr};
    use crate::sql::planner::plan::JoinNode;
    // Plan: Join[INNER, on a=d] <- {Scan_l[a,b,c], Scan_r[d,e,f]}
    // parent_needed = {b, f}
    // Expected:
    //   left_needed = {a, b}  (b from parent + a from join cond)
    //   right_needed = {d, f}  (f from parent + d from join cond)
    let scan_l = scan_with_3_cols();  // ids 1,2,3 = a,b,c
    let scan_r = scan_with_ids(4, 5, 6);  // helper: ids 4,5,6 = d,e,f
    let cond = TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(col_ref_with_id(1)),  // a
            op: BinOp::Eq,
            right: Box::new(col_ref_with_id(4)),  // d
        },
        data_type: DataType::Boolean,
        nullable: false,
    };
    let join = LogicalPlan::Join(JoinNode {
        left: Box::new(scan_l),
        right: Box::new(scan_r),
        join_type: JoinKind::Inner,
        condition: Some(cond),
        required_output_columns: None,
    });
    let needed: HashSet<ColumnId> = [ColumnId::new_for_test(2), ColumnId::new_for_test(6)].into_iter().collect();
    let tagged = tag_required_columns(join, Some(needed));
    let LogicalPlan::Join(j) = tagged else { panic!() };
    let LogicalPlan::Scan(l) = *j.left else { panic!() };
    let LogicalPlan::Scan(r) = *j.right else { panic!() };
    let lreq = l.required_output_columns.unwrap();
    let rreq = r.required_output_columns.unwrap();
    assert!(lreq.contains(&ColumnId::new_for_test(1)) && lreq.contains(&ColumnId::new_for_test(2)));
    assert_eq!(lreq.len(), 2);
    assert!(rreq.contains(&ColumnId::new_for_test(4)) && rreq.contains(&ColumnId::new_for_test(6)));
    assert_eq!(rreq.len(), 2);
}
```

(Define `scan_with_ids` test helper that takes 3 column IDs.)

- [ ] **Step 9.2: Confirm fail**

- [ ] **Step 9.3: Implement tag_join**

```rust
fn tag_join(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    use crate::sql::optimizer::rewrite::rules::utils::collect_output_ids;
    let LogicalPlan::Join(mut node) = plan else { unreachable!() };
    node.required_output_columns = parent_needed.clone();

    let mut combined = parent_needed.unwrap_or_default();
    if let Some(cond) = &node.condition {
        combined.extend(collect_column_id_refs(cond));
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

- [ ] **Step 9.4: Run test, see pass**

- [ ] **Step 9.5: Commit**

```bash
git add src/sql/optimizer/rewrite/required_columns.rs
git commit -m "feat(rewrite): tag_join — split needed by child outputs, add condition cols"
```

---

### Task 10: SubqueryAlias handler (Gap 1)

- [ ] **Step 10.1: Write failing test**

```rust
#[test]
fn tag_subquery_alias_transparently_propagates_needed() {
    use crate::sql::planner::plan::SubqueryAliasNode;
    // Plan: SubqueryAlias[t] <- Scan[a,b,c]
    // parent_needed = {b}
    // Expected: scan.required_output_columns = {b}
    let scan = scan_with_3_cols();
    let alias = LogicalPlan::SubqueryAlias(SubqueryAliasNode {
        input: Box::new(scan),
        alias: "t".to_string(),
        output_columns: vec![
            OutputColumn { column_id: ColumnId::new_for_test(1), name: "a".into(), data_type: DataType::Int32, nullable: false },
            OutputColumn { column_id: ColumnId::new_for_test(2), name: "b".into(), data_type: DataType::Int32, nullable: false },
            OutputColumn { column_id: ColumnId::new_for_test(3), name: "c".into(), data_type: DataType::Int32, nullable: false },
        ],
        required_output_columns: None,
    });
    let needed: HashSet<ColumnId> = std::iter::once(ColumnId::new_for_test(2)).collect();
    let tagged = tag_required_columns(alias, Some(needed.clone()));
    let LogicalPlan::SubqueryAlias(s) = tagged else { panic!() };
    assert_eq!(s.required_output_columns.unwrap(), needed);
    let LogicalPlan::Scan(inner) = *s.input else { panic!() };
    let inner_req = inner.required_output_columns.unwrap();
    assert!(inner_req.contains(&ColumnId::new_for_test(2)));
    assert_eq!(inner_req.len(), 1, "only b propagated");
}
```

- [ ] **Step 10.2: Confirm fail**

- [ ] **Step 10.3: Implement tag_subquery_alias**

```rust
fn tag_subquery_alias(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::SubqueryAlias(mut node) = plan else { unreachable!() };
    node.required_output_columns = parent_needed.clone();
    node.input = Box::new(tag_required_columns(*node.input, parent_needed));
    LogicalPlan::SubqueryAlias(node)
}
```

- [ ] **Step 10.4: Run test, see pass**

- [ ] **Step 10.5: Commit**

```bash
git add src/sql/optimizer/rewrite/required_columns.rs
git commit -m "feat(rewrite): tag_subquery_alias — transparent ColumnId propagation (Gap 1)"
```

---

### Task 11: Union / Intersect / Except handlers (Gap 4)

- [ ] **Step 11.1: Write failing test for Union**

```rust
#[test]
fn tag_union_position_aligned_propagation() {
    use crate::sql::planner::plan::UnionNode;
    // Plan: Union[output: x@1001, y@1002, z@1003] <- {Scan_a[1,2,3], Scan_b[4,5,6]}
    // parent_needed = {y@1002}
    // Expected:
    //   - position 1 (y) is needed
    //   - Scan_a request {2}
    //   - Scan_b request {5}
    let scan_a = scan_with_3_cols();              // ids 1,2,3
    let scan_b = scan_with_ids(4, 5, 6);          // ids 4,5,6
    let union = LogicalPlan::Union(UnionNode {
        inputs: vec![scan_a, scan_b],
        all: true,
        output_columns: vec![
            OutputColumn { column_id: ColumnId::new_for_test(1001), name: "x".into(), data_type: DataType::Int32, nullable: false },
            OutputColumn { column_id: ColumnId::new_for_test(1002), name: "y".into(), data_type: DataType::Int32, nullable: false },
            OutputColumn { column_id: ColumnId::new_for_test(1003), name: "z".into(), data_type: DataType::Int32, nullable: false },
        ],
        required_output_columns: None,
    });
    let needed: HashSet<ColumnId> = std::iter::once(ColumnId::new_for_test(1002)).collect();
    let tagged = tag_required_columns(union, Some(needed));
    let LogicalPlan::Union(u) = tagged else { panic!() };
    let LogicalPlan::Scan(a) = &u.inputs[0] else { panic!() };
    let LogicalPlan::Scan(b) = &u.inputs[1] else { panic!() };
    let a_req = a.required_output_columns.as_ref().unwrap();
    let b_req = b.required_output_columns.as_ref().unwrap();
    assert!(a_req.contains(&ColumnId::new_for_test(2)) && a_req.len() == 1);
    assert!(b_req.contains(&ColumnId::new_for_test(5)) && b_req.len() == 1);
}
```

- [ ] **Step 11.2: Confirm fail**

- [ ] **Step 11.3: Implement tag_union/intersect/except**

```rust
fn tag_union(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    use crate::sql::optimizer::rewrite::rules::utils::collect_output_ids_ordered;
    let LogicalPlan::Union(mut node) = plan else { unreachable!() };

    let outputs: Vec<ColumnId> = node.output_columns.iter().map(|c| c.column_id).collect();
    let needed_positions: Vec<usize> = match &parent_needed {
        None => (0..outputs.len()).collect(),
        Some(n) => outputs.iter().enumerate()
            .filter_map(|(i, id)| n.contains(id).then_some(i))
            .collect(),
    };

    node.required_output_columns = parent_needed;
    node.inputs = node.inputs.into_iter().map(|child| {
        let child_outputs = collect_output_ids_ordered(&child);
        let child_needed: HashSet<ColumnId> = needed_positions.iter()
            .map(|&i| child_outputs[i])
            .collect();
        tag_required_columns(child, Some(child_needed))
    }).collect();
    LogicalPlan::Union(node)
}

fn tag_intersect(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    /* same pattern as tag_union — replace Union with Intersect */
    // Implementation mirrors tag_union exactly except for the LogicalPlan variant.
    let LogicalPlan::Intersect(mut node) = plan else { unreachable!() };
    use crate::sql::optimizer::rewrite::rules::utils::collect_output_ids_ordered;
    let outputs: Vec<ColumnId> = node.output_columns.iter().map(|c| c.column_id).collect();
    let needed_positions: Vec<usize> = match &parent_needed {
        None => (0..outputs.len()).collect(),
        Some(n) => outputs.iter().enumerate()
            .filter_map(|(i, id)| n.contains(id).then_some(i))
            .collect(),
    };
    node.required_output_columns = parent_needed;
    node.inputs = node.inputs.into_iter().map(|child| {
        let child_outputs = collect_output_ids_ordered(&child);
        let child_needed: HashSet<ColumnId> = needed_positions.iter().map(|&i| child_outputs[i]).collect();
        tag_required_columns(child, Some(child_needed))
    }).collect();
    LogicalPlan::Intersect(node)
}

fn tag_except(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Except(mut node) = plan else { unreachable!() };
    use crate::sql::optimizer::rewrite::rules::utils::collect_output_ids_ordered;
    let outputs: Vec<ColumnId> = node.output_columns.iter().map(|c| c.column_id).collect();
    let needed_positions: Vec<usize> = match &parent_needed {
        None => (0..outputs.len()).collect(),
        Some(n) => outputs.iter().enumerate()
            .filter_map(|(i, id)| n.contains(id).then_some(i))
            .collect(),
    };
    node.required_output_columns = parent_needed;
    node.inputs = node.inputs.into_iter().map(|child| {
        let child_outputs = collect_output_ids_ordered(&child);
        let child_needed: HashSet<ColumnId> = needed_positions.iter().map(|&i| child_outputs[i]).collect();
        tag_required_columns(child, Some(child_needed))
    }).collect();
    LogicalPlan::Except(node)
}
```

(Yes, this is repetitive — extract a `tag_set_op` helper that takes the variant pattern as a closure if we want DRY; here we duplicate for clarity and because variant unwrapping in Rust doesn't compose cleanly via generics.)

- [ ] **Step 11.4: Add equivalent Intersect/Except tests, run all**

```bash
cargo test --lib sql::optimizer::rewrite::required_columns::tests::tag_union
```

Expected: all set-op tests passing.

- [ ] **Step 11.5: Commit**

```bash
git add src/sql/optimizer/rewrite/required_columns.rs
git commit -m "feat(rewrite): tag_union/intersect/except — position-aligned propagation (Gap 4)"
```

---

### Task 12: CTEAnchor / CTEProduce / CTEConsume two-walk (Gap 3)

This is the most complex tagging logic. Two passes over the consumer subtree.

**Files:**
- Modify: `src/sql/optimizer/rewrite/required_columns.rs`

- [ ] **Step 12.1: Write failing test**

```rust
#[test]
fn tag_cte_anchor_collects_consumer_needs_and_translates_to_producer() {
    use crate::sql::analysis::cte::CteId;
    use crate::sql::planner::plan::{CTEAnchorNode, CTEConsumeNode, CTEProduceNode};

    // Setup:
    //   CTEProduce[c0=1, c1=2, c2=3] <- Scan[a@10,b@20,c@30]
    //   Two CTEConsumers:
    //     consume1 outputs (k0@101, k1@102, k2@103) — mapping by position to produce
    //     consume2 outputs (m0@201, m1@202, m2@203)
    //   Top of consumer subtree needs {k1@102, m2@203}
    //   Expected: produce input scan gets {b@20, c@30}
    let cte_id = CteId::new_for_test(7);
    let scan = LogicalPlan::Scan(/* a,b,c with IDs 10,20,30, full ScanNode */);
    let produce = LogicalPlan::CTEProduce(CTEProduceNode {
        cte_id,
        input: Box::new(scan),
        output_columns: vec![
            OutputColumn { column_id: ColumnId::new_for_test(1), name: "c0".into(), data_type: DataType::Int32, nullable: false },
            OutputColumn { column_id: ColumnId::new_for_test(2), name: "c1".into(), data_type: DataType::Int32, nullable: false },
            OutputColumn { column_id: ColumnId::new_for_test(3), name: "c2".into(), data_type: DataType::Int32, nullable: false },
        ],
        required_output_columns: None,
    });
    let consume1 = LogicalPlan::CTEConsume(CTEConsumeNode {
        cte_id,
        alias: "u1".into(),
        output_columns: vec![
            OutputColumn { column_id: ColumnId::new_for_test(101), name: "k0".into(), data_type: DataType::Int32, nullable: false },
            OutputColumn { column_id: ColumnId::new_for_test(102), name: "k1".into(), data_type: DataType::Int32, nullable: false },
            OutputColumn { column_id: ColumnId::new_for_test(103), name: "k2".into(), data_type: DataType::Int32, nullable: false },
        ],
        required_output_columns: None,
    });
    let consume2 = LogicalPlan::CTEConsume(CTEConsumeNode {
        cte_id,
        alias: "u2".into(),
        output_columns: vec![
            OutputColumn { column_id: ColumnId::new_for_test(201), name: "m0".into(), data_type: DataType::Int32, nullable: false },
            OutputColumn { column_id: ColumnId::new_for_test(202), name: "m1".into(), data_type: DataType::Int32, nullable: false },
            OutputColumn { column_id: ColumnId::new_for_test(203), name: "m2".into(), data_type: DataType::Int32, nullable: false },
        ],
        required_output_columns: None,
    });

    // Build a Join of consume1 and consume2 as consumer subtree.
    let consumer = LogicalPlan::Join(JoinNode {
        left: Box::new(consume1),
        right: Box::new(consume2),
        join_type: JoinKind::Inner,
        condition: None,
        required_output_columns: None,
    });
    let anchor = LogicalPlan::CTEAnchor(CTEAnchorNode {
        cte_id,
        produce: Box::new(produce),
        consumer: Box::new(consumer),
        required_output_columns: None,
    });

    // Outer needs k1 + m2.
    let needed: HashSet<ColumnId> = [ColumnId::new_for_test(102), ColumnId::new_for_test(203)].into_iter().collect();
    let tagged = tag_required_columns(anchor, Some(needed));

    // Verify producer's scan ended up with b@20 and c@30 (positions 1, 2 of producer).
    let LogicalPlan::CTEAnchor(a) = tagged else { panic!() };
    let LogicalPlan::CTEProduce(p) = *a.produce else { panic!() };
    let LogicalPlan::Scan(s) = *p.input else { panic!() };
    let req = s.required_output_columns.unwrap();
    assert!(req.contains(&ColumnId::new_for_test(20)), "b@20 from k1 position 1");
    assert!(req.contains(&ColumnId::new_for_test(30)), "c@30 from m2 position 2");
    assert!(!req.contains(&ColumnId::new_for_test(10)), "a@10 NOT needed");
}
```

(Fill in the Scan construction with proper ScanNode fields.)

- [ ] **Step 12.2: Confirm fail**

- [ ] **Step 12.3: Implement tag_cte_anchor + tag_cte_consume + helpers**

```rust
fn tag_cte_consume(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::CTEConsume(mut node) = plan else { unreachable!() };
    node.required_output_columns = parent_needed;
    LogicalPlan::CTEConsume(node)  // leaf — no recursion
}

fn tag_cte_anchor(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::CTEAnchor(mut node) = plan else { unreachable!() };
    let cte_id = node.cte_id;

    // Walk 1: tag the consumer subtree.
    let consumer = tag_required_columns(*node.consumer, parent_needed.clone());

    // Walk 2: collect every CTEConsume.required_output_columns for this cte_id.
    let mut consume_needs: HashSet<ColumnId> = HashSet::new();
    collect_cte_consumer_needs(&consumer, cte_id, &mut consume_needs);

    // Translate to producer-side ColumnIds via position alignment.
    let produce_input_needed = translate_consume_to_produce(&consume_needs, &node.produce);

    let produce = tag_required_columns(*node.produce, Some(produce_input_needed));

    node.consumer = Box::new(consumer);
    node.produce = Box::new(produce);
    node.required_output_columns = parent_needed;
    LogicalPlan::CTEAnchor(node)
}

fn tag_cte_produce(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::CTEProduce(mut node) = plan else { unreachable!() };
    node.required_output_columns = parent_needed.clone();
    // Translate parent_needed (which is in producer-output-id space) to
    // input-id space. Producer's input columns may differ from output_columns
    // (e.g., Project below).
    // For simplicity: pass parent_needed through; downstream Project's tag will
    // re-resolve via its own items.
    node.input = Box::new(tag_required_columns(*node.input, parent_needed));
    LogicalPlan::CTEProduce(node)
}

fn collect_cte_consumer_needs(
    plan: &LogicalPlan,
    cte_id: CteId,
    acc: &mut HashSet<ColumnId>,
) {
    match plan {
        LogicalPlan::CTEConsume(c) if c.cte_id == cte_id => {
            if let Some(req) = &c.required_output_columns {
                acc.extend(req.iter().cloned());
            }
        }
        LogicalPlan::Scan(_) | LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) => {}
        LogicalPlan::Filter(f) => collect_cte_consumer_needs(&f.input, cte_id, acc),
        LogicalPlan::Project(p) => collect_cte_consumer_needs(&p.input, cte_id, acc),
        LogicalPlan::Aggregate(a) => collect_cte_consumer_needs(&a.input, cte_id, acc),
        LogicalPlan::Join(j) => {
            collect_cte_consumer_needs(&j.left, cte_id, acc);
            collect_cte_consumer_needs(&j.right, cte_id, acc);
        }
        LogicalPlan::Sort(s) => collect_cte_consumer_needs(&s.input, cte_id, acc),
        LogicalPlan::Limit(l) => collect_cte_consumer_needs(&l.input, cte_id, acc),
        LogicalPlan::Window(w) => collect_cte_consumer_needs(&w.input, cte_id, acc),
        LogicalPlan::Union(u) => for i in &u.inputs { collect_cte_consumer_needs(i, cte_id, acc); }
        LogicalPlan::Intersect(i) => for x in &i.inputs { collect_cte_consumer_needs(x, cte_id, acc); }
        LogicalPlan::Except(e) => for x in &e.inputs { collect_cte_consumer_needs(x, cte_id, acc); }
        LogicalPlan::SubqueryAlias(s) => collect_cte_consumer_needs(&s.input, cte_id, acc),
        LogicalPlan::TableFunction(t) => collect_cte_consumer_needs(&t.input, cte_id, acc),
        LogicalPlan::Repeat(r) => collect_cte_consumer_needs(&r.input, cte_id, acc),
        LogicalPlan::Decode(d) => collect_cte_consumer_needs(&d.input, cte_id, acc),
        LogicalPlan::CTEAnchor(c) => {
            // Don't descend into nested CTEAnchors with the same cte_id (none),
            // but do descend into consumer for outer cte_id collection.
            collect_cte_consumer_needs(&c.consumer, cte_id, acc);
            collect_cte_consumer_needs(&c.produce, cte_id, acc);
        }
        LogicalPlan::CTEProduce(p) => collect_cte_consumer_needs(&p.input, cte_id, acc),
        LogicalPlan::CTEConsume(_) => { /* mismatching cte_id — skip */ }
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {}
    }
}

fn translate_consume_to_produce(
    consume_needs: &HashSet<ColumnId>,
    produce: &LogicalPlan,
) -> HashSet<ColumnId> {
    // CTEProduce.output_columns and CTEConsume.output_columns are aligned by position.
    // Walk the consumer subtree to find any CTEConsume.output_columns; use the first
    // one (all consumers share same shape) to build position mapping.
    // For each consume_needs id, find its position in any CTEConsume.output_columns,
    // then translate to produce.output_columns[pos].column_id.
    //
    // Simpler approach: each CTEConsume node already carries `output_columns`. As long
    // as we know the cte_id, we look up the producer (already provided as `produce` arg)
    // and walk consumer subtree elsewhere... but we don't have consumer subtree here.
    //
    // Strategy: receive `cte_id` and `consumer` subtree as args instead.
    unimplemented!("see refactored signature below")
}
```

Refactor: `translate_consume_to_produce` needs both the producer and the consumer subtree to find the position mapping. Pass both into `tag_cte_anchor`:

```rust
fn tag_cte_anchor(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::CTEAnchor(mut node) = plan else { unreachable!() };
    let cte_id = node.cte_id;
    let consumer = tag_required_columns(*node.consumer, parent_needed.clone());

    let mut consume_needs = HashSet::new();
    collect_cte_consumer_needs(&consumer, cte_id, &mut consume_needs);

    let producer_output_ids = match &*node.produce {
        LogicalPlan::CTEProduce(p) => p.output_columns.iter().map(|c| c.column_id).collect::<Vec<_>>(),
        _ => panic!("CTEAnchor.produce must be CTEProduce"),
    };
    let consume_position_map = find_consume_position_map(&consumer, cte_id);
    let produce_input_needed: HashSet<ColumnId> = consume_needs.iter()
        .filter_map(|cid| consume_position_map.get(cid).map(|&pos| producer_output_ids[pos]))
        .collect();

    let produce = tag_required_columns(*node.produce, Some(produce_input_needed));

    node.consumer = Box::new(consumer);
    node.produce = Box::new(produce);
    node.required_output_columns = parent_needed;
    LogicalPlan::CTEAnchor(node)
}

fn find_consume_position_map(plan: &LogicalPlan, cte_id: CteId) -> HashMap<ColumnId, usize> {
    let mut map = HashMap::new();
    walk_consume_position_map(plan, cte_id, &mut map);
    map
}

fn walk_consume_position_map(plan: &LogicalPlan, cte_id: CteId, map: &mut HashMap<ColumnId, usize>) {
    match plan {
        LogicalPlan::CTEConsume(c) if c.cte_id == cte_id => {
            for (i, col) in c.output_columns.iter().enumerate() {
                map.entry(col.column_id).or_insert(i);
            }
        }
        // ... recurse same shape as collect_cte_consumer_needs
        _ => { /* recurse */ }
    }
}
```

- [ ] **Step 12.4: Run tests**

```bash
cargo test --lib sql::optimizer::rewrite::required_columns::tests::tag_cte_anchor
```

Expected: PASS.

- [ ] **Step 12.5: Add second multi-consumer test**

```rust
#[test]
fn tag_cte_anchor_union_of_multi_consumer_needs() {
    // Two consumers each request a different subset; producer's input
    // needed should be the union.
    // ... (verify the union semantics)
}
```

- [ ] **Step 12.6: Run all CTE tests**

- [ ] **Step 12.7: Commit**

```bash
git add src/sql/optimizer/rewrite/required_columns.rs
git commit -m "feat(rewrite): tag_cte_anchor two-walk pattern (Gap 3) + tag_cte_produce/consume"
```

---

### Task 13: Window / Repeat / Decode / TableFunction handlers

These are similar in pattern: Window has its own output_columns; Repeat / TableFunction pass needed through (with additional cols); Decode swaps dict↔string IDs.

- [ ] **Step 13.1: Write failing tests for each (4 tests)**

(For each, follow the same shape: build minimal plan with the wrapping node + a Scan, set parent_needed, verify child Scan gets the right needed set.)

- [ ] **Step 13.2: Confirm all fail**

- [ ] **Step 13.3: Implement tag_window / tag_repeat / tag_decode / tag_table_function**

```rust
fn tag_window(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Window(mut node) = plan else { unreachable!() };
    node.required_output_columns = parent_needed.clone();
    let mut child_needed = parent_needed.clone().unwrap_or_default();
    // Window-needed cols: partition_by + order_by + arg cols of each WindowExpr
    // whose output is in parent_needed (or all if parent_needed is None).
    let input_col_count = node.output_columns.len() - node.window_exprs.len();
    for (i, wexpr) in node.window_exprs.iter().enumerate() {
        let out_id = node.output_columns[input_col_count + i].column_id;
        let is_needed = match &parent_needed {
            None => true,
            Some(n) => n.contains(&out_id),
        };
        if is_needed {
            for arg in &wexpr.args { child_needed.extend(collect_column_id_refs(arg)); }
            for expr in &wexpr.partition_by { child_needed.extend(collect_column_id_refs(expr)); }
            for item in &wexpr.order_by { child_needed.extend(collect_column_id_refs(&item.expr)); }
        }
    }
    // Pass-through input columns that are in parent_needed
    for col in node.output_columns.iter().take(input_col_count) {
        if parent_needed.as_ref().is_none_or(|n| n.contains(&col.column_id)) {
            child_needed.insert(col.column_id);
        }
    }
    node.input = Box::new(tag_required_columns(*node.input, Some(child_needed)));
    LogicalPlan::Window(node)
}

fn tag_repeat(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Repeat(mut node) = plan else { unreachable!() };
    node.required_output_columns = parent_needed.clone();
    // Repeat needs all rollup columns + parent_needed.
    let mut child_needed = parent_needed.unwrap_or_default();
    for col_ref in &node.repeat_column_ref_list {
        child_needed.insert(col_ref.column_id);
    }
    node.input = Box::new(tag_required_columns(*node.input, Some(child_needed)));
    LogicalPlan::Repeat(node)
}

fn tag_decode(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::Decode(mut node) = plan else { unreachable!() };
    node.required_output_columns = parent_needed.clone();
    // Decode: replace string_column with dict_column in needed.
    let mut child_needed = parent_needed.unwrap_or_default();
    for mapping in &node.mappings {
        if child_needed.remove(&mapping.string_column_id) {
            child_needed.insert(mapping.dict_column_id);
        }
    }
    node.input = Box::new(tag_required_columns(*node.input, Some(child_needed)));
    LogicalPlan::Decode(node)
}

fn tag_table_function(plan: LogicalPlan, parent_needed: Option<HashSet<ColumnId>>) -> LogicalPlan {
    let LogicalPlan::TableFunction(mut node) = plan else { unreachable!() };
    node.required_output_columns = parent_needed.clone();
    // TableFunction outputs additional columns + passes through input. For
    // safety: pass parent_needed through (the function's own input handler
    // will compute correctly).
    node.input = Box::new(tag_required_columns(*node.input, parent_needed));
    LogicalPlan::TableFunction(node)
}
```

(Check `DecodeMapping` and `RepeatColumnRef` actual field names — may need adjustment.)

- [ ] **Step 13.4: Wire all into match, run tests**

- [ ] **Step 13.5: Commit**

```bash
git add src/sql/optimizer/rewrite/required_columns.rs
git commit -m "feat(rewrite): tag_window/repeat/decode/table_function handlers"
```

---

### Task 14: Wrap as `TagRequiredColumns` rule + register stage

**Files:**
- Modify: `src/sql/optimizer/rewrite/required_columns.rs` (add Rule wrapper at bottom)
- Modify: `src/sql/optimizer/rewrite/registry.rs` (insert stage)

- [ ] **Step 14.1: Define TagRequiredColumns rule**

Append to `required_columns.rs`:

```rust
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};

pub(crate) struct TagRequiredColumns;

impl LogicalRewriteRule for TagRequiredColumns {
    fn name(&self) -> &'static str { "TagRequiredColumns" }
    fn phase(&self) -> RewritePhase { RewritePhase::StructuralRewrite }
    fn traversal(&self) -> RewriteTraversal { RewriteTraversal::TopDown }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        // Match only when the operator hasn't been tagged yet — once
        // required_output_columns is Some(_), this rule is a no-op so we
        // skip it via matches() to keep the trace clean.
        plan_has_no_required_output_columns(plan)
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        // The rule walks the entire subtree from this node and writes
        // required_output_columns everywhere. After this returns, all
        // descendants are tagged.
        let tagged = tag_required_columns(plan, None);
        Ok(RewriteResult::Changed(tagged))
    }
}

fn plan_has_no_required_output_columns(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(s) => s.required_output_columns.is_none(),
        LogicalPlan::Project(p) => p.required_output_columns.is_none(),
        // ... all 20 variants
        _ => true,
    }
}
```

- [ ] **Step 14.2: Register stage in registry.rs**

In `src/sql/optimizer/rewrite/registry.rs::query_rewrite_pipeline`, insert before `"ColumnPruning"`:

```rust
RewriteStage::new(
    "TagRequiredColumns",
    RewritePhase::StructuralRewrite,
    vec![Box::new(crate::sql::optimizer::rewrite::required_columns::TagRequiredColumns)],
),
RewriteStage::new(
    "ColumnPruning",
    RewritePhase::StructuralRewrite,
    rules::column_pruning_rules(),   // existing — Phase 2 (Task 24 replaces this)
),
```

- [ ] **Step 14.3: Write integration test that tags propagate through pipeline**

In a new file `src/sql/optimizer/rewrite/required_columns.rs::tests` (or a separate integration test):

```rust
#[test]
fn tag_required_columns_runs_via_pipeline() {
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use std::collections::HashMap;

    let plan = LogicalPlan::Project(ProjectNode {
        input: Box::new(scan_with_3_cols()),
        items: vec![ProjectItem {
            output_column_id: ColumnId::new_for_test(101),
            output_name: "a".into(),
            expr: col_ref_with_id(1),
        }],
        required_output_columns: None,
    });
    let table_stats = HashMap::new();
    let mut ctx = RewriteContext::for_query(Vec::<String>::new());
    let pipeline = query_rewrite_pipeline(&table_stats);
    let tagged = pipeline.rewrite(plan, &mut ctx).unwrap();

    // Verify the Project has required_output_columns set.
    let LogicalPlan::Project(p) = tagged else { panic!() };
    assert!(p.required_output_columns.is_some());
}
```

- [ ] **Step 14.4: Run integration test**

```bash
cargo test --lib sql::optimizer::rewrite::required_columns
```

Expected: PASS.

- [ ] **Step 14.5: Commit**

```bash
git add src/sql/optimizer/rewrite/required_columns.rs src/sql/optimizer/rewrite/registry.rs
git commit -m "feat(rewrite): register TagRequiredColumns stage in query_rewrite_pipeline"
```

---

## Phase C: Per-Operator Pruning Rules

### Task 15: PruneScanColumns

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/column_pruning/mod.rs`
- Create: `src/sql/optimizer/rewrite/rules/column_pruning/prune_scan.rs`
- Modify: `src/sql/optimizer/rewrite/rules/mod.rs` (point column_pruning_rules to new module)

- [ ] **Step 15.1: Create directory + mod.rs scaffold**

Create `src/sql/optimizer/rewrite/rules/column_pruning/mod.rs`:

```rust
//! Per-operator column pruning rules (Phase 2 of OQ-1).
//!
//! Each rule reads its node's `required_output_columns` (populated by
//! Phase 1 `TagRequiredColumns`) and filters the node's metadata
//! (`items`, `output_columns`, etc.) accordingly.

pub(crate) mod prune_scan;
// ... others added by subsequent tasks

use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) fn column_pruning_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![
        Box::new(prune_scan::PruneScanColumns),
        // ... 17 others appended by tasks 16-23
    ]
}
```

- [ ] **Step 15.2: Write failing test for PruneScanColumns**

Create `src/sql/optimizer/rewrite/rules/column_pruning/prune_scan.rs`:

```rust
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct PruneScanColumns;

impl LogicalRewriteRule for PruneScanColumns {
    fn name(&self) -> &'static str { "PruneScanColumns" }
    fn phase(&self) -> RewritePhase { RewritePhase::StructuralRewrite }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Scan(_))
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Scan(mut node) = plan else { unreachable!() };
        let Some(ids) = &node.required_output_columns else {
            return Ok(RewriteResult::Unchanged);
        };
        let names: Vec<String> = node.columns.iter()
            .filter(|c| ids.contains(&c.column_id))
            .map(|c| c.name.clone())
            .collect();
        // Always include predicate-referenced columns.
        let mut name_set: std::collections::HashSet<String> = names.into_iter().collect();
        for pred in &node.predicates {
            for col_name in crate::sql::optimizer::rewrite::rules::utils::collect_column_refs(pred) {
                name_set.insert(col_name.to_lowercase());
            }
        }
        let mut names: Vec<String> = name_set.into_iter().collect();
        if names.is_empty() && !node.columns.is_empty() {
            names.push(node.columns[0].name.clone());
        }
        if node.required_columns.as_ref() == Some(&names) {
            return Ok(RewriteResult::Unchanged);
        }
        node.required_columns = Some(names);
        Ok(RewriteResult::Changed(LogicalPlan::Scan(node)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Migrate the existing test from column_pruning.rs:
    // `root_scan_without_parent_keeps_all_columns` becomes:
    #[test]
    fn pruned_scan_without_required_output_columns_returns_unchanged() {
        // ... build scan with required_output_columns = None
        // ... apply rule, expect RewriteResult::Unchanged
    }

    #[test]
    fn pruned_scan_with_required_set_filters_to_names() {
        // ... build scan with required_output_columns = Some({col_id_of_b})
        // ... apply, expect required_columns == Some(["b"])
    }
}
```

- [ ] **Step 15.3: Confirm test fails (test imports compile, test logic fails)**

```bash
cargo test --lib sql::optimizer::rewrite::rules::column_pruning::prune_scan
```

Expected: PASS once logic implemented in Step 15.2 (TDD inverted here — write impl + test together for trivial rules).

- [ ] **Step 15.4: Wire mod.rs registration into rules/mod.rs**

In `src/sql/optimizer/rewrite/rules/mod.rs`, change:

```rust
pub(crate) fn column_pruning_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    let mut rules = column_pruning::column_pruning_rules();
    rules.push(Box::new(ukfk::PruneUkFkJoin));
    rules.push(Box::new(ukfk::EliminateUniqueAggregate));
    rules
}
```

(Keep `ukfk` rules in the same stage — they're related cleanup.)

**Important**: Task 15 only **adds** the new `column_pruning` submodule. The old `pub(crate) mod column_pruning;` declaration in `rules/mod.rs` stays untouched and continues to register `column_pruning::PruneColumns` rule. Both old and new rules coexist in `column_pruning_rules()` for Tasks 15-24 (one big-bang activation moment in Task 25). This way each per-rule task can be tested independently against the suite without flipping the whole architecture.

For Task 15, the temporary `column_pruning_rules()` body looks like:

```rust
pub(crate) fn column_pruning_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    let mut rules: Vec<Box<dyn LogicalRewriteRule>> = vec![
        // Old single rule — kept active during Tasks 15-24, removed in Task 25.
        Box::new(column_pruning::PruneColumns),
        // New per-operator rules — added incrementally Tasks 15-23.
    ];
    rules.extend(column_pruning_new::column_pruning_rules());
    rules.push(Box::new(ukfk::PruneUkFkJoin));
    rules.push(Box::new(ukfk::EliminateUniqueAggregate));
    rules
}
```

(Use `column_pruning_new` as a short transient alias for `column_pruning::` directory module to avoid name collision with the old `column_pruning::` file module — Rust's module system requires distinct names when both exist.)

- [ ] **Step 15.5: Run all rule unit tests**

```bash
cargo test --lib sql::optimizer::rewrite::rules::column_pruning::prune_scan
```

Expected: PASS.

- [ ] **Step 15.6: Commit**

```bash
git add src/sql/optimizer/rewrite/rules/column_pruning/ src/sql/optimizer/rewrite/rules/mod.rs
git commit -m "feat(rewrite): PruneScanColumns rule + column_pruning module scaffold"
```

---

### Task 16: PruneProjectColumns (Gap 2)

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/column_pruning/prune_project.rs`
- Modify: `src/sql/optimizer/rewrite/rules/column_pruning/mod.rs` (register)

- [ ] **Step 16.1: Write failing test**

```rust
#[test]
fn prune_project_filters_items_to_needed_outputs() {
    // Project[a, b, c] with required_output_columns = {b's id}
    // Expected: items reduced to [b]
}

#[test]
fn prune_project_auto_fills_when_all_items_dropped() {
    // Project[a, b] with required_output_columns = {} (empty)
    // Expected: items = [auto_fill_col := 1]
}

#[test]
fn prune_project_unchanged_when_required_output_columns_none() {
    // required_output_columns = None
    // Expected: RewriteResult::Unchanged
}
```

- [ ] **Step 16.2: Implement PruneProjectColumns**

```rust
use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{LogicalPlan, ProjectNode};
use arrow::datatypes::DataType;

pub(crate) struct PruneProjectColumns;

impl LogicalRewriteRule for PruneProjectColumns {
    fn name(&self) -> &'static str { "PruneProjectColumns" }
    fn phase(&self) -> RewritePhase { RewritePhase::StructuralRewrite }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Project(_))
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Project(mut node) = plan else { unreachable!() };
        let Some(needed) = &node.required_output_columns else {
            return Ok(RewriteResult::Unchanged);
        };
        let original_len = node.items.len();
        let mut new_items: Vec<ProjectItem> = node.items.into_iter()
            .filter(|item| needed.contains(&item.output_column_id))
            .collect();
        if new_items.is_empty() {
            new_items.push(auto_fill_item(ctx)?);
        }
        if new_items.len() == original_len {
            return Ok(RewriteResult::Unchanged);
        }
        node.items = new_items;
        Ok(RewriteResult::Changed(LogicalPlan::Project(node)))
    }
}

fn auto_fill_item(ctx: &mut RewriteContext) -> Result<ProjectItem, String> {
    let factory = ctx.column_ref_factory().ok_or("ColumnRefFactory required for auto_fill")?;
    let mut factory_mut = factory.borrow_mut();
    let new_id = factory_mut.next_id();  // verify method name
    Ok(ProjectItem {
        output_column_id: new_id,
        output_name: format!("auto_fill_{}", new_id.as_u32()),
        expr: TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int8,
            nullable: false,
        },
    })
}
```

(Verify `ColumnRefFactory::next_id` / `ColumnId::as_u32` exist; adjust to actual API.)

- [ ] **Step 16.3: Register in mod.rs**

Add `pub(crate) mod prune_project;` and `Box::new(prune_project::PruneProjectColumns)` in column_pruning_rules().

- [ ] **Step 16.4: Run tests, see pass**

- [ ] **Step 16.5: Commit**

```bash
git add src/sql/optimizer/rewrite/rules/column_pruning/
git commit -m "feat(rewrite): PruneProjectColumns — filter items by output_column_id (Gap 2)"
```

---

### Task 17: PruneSubqueryAliasColumns (Gap 1)

- [ ] **Step 17.1: Write failing test**

```rust
#[test]
fn prune_subquery_alias_filters_output_columns_by_needed() {
    // SubqueryAlias with output_columns=[a,b,c] and required_output_columns={a's id}
    // Expected: output_columns = [a]
}
```

- [ ] **Step 17.2: Implement** (mirror PruneProjectColumns, filtering `output_columns` field)

```rust
impl LogicalRewriteRule for PruneSubqueryAliasColumns {
    fn name(&self) -> &'static str { "PruneSubqueryAliasColumns" }
    fn phase(&self) -> RewritePhase { RewritePhase::StructuralRewrite }
    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::SubqueryAlias(_))
    }
    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::SubqueryAlias(mut node) = plan else { unreachable!() };
        let Some(needed) = &node.required_output_columns else {
            return Ok(RewriteResult::Unchanged);
        };
        let original_len = node.output_columns.len();
        let new_cols: Vec<_> = node.output_columns.into_iter()
            .filter(|c| needed.contains(&c.column_id))
            .collect();
        if new_cols.len() == original_len {
            return Ok(RewriteResult::Unchanged);
        }
        node.output_columns = new_cols;
        Ok(RewriteResult::Changed(LogicalPlan::SubqueryAlias(node)))
    }
}
```

- [ ] **Step 17.3-17.5: Test, register, commit**

```bash
git commit -m "feat(rewrite): PruneSubqueryAliasColumns — filter output_columns (Gap 1)"
```

---

### Task 18: PruneAggregateColumns (Gap 5)

- [ ] **Step 18.1: Write failing test**

```rust
#[test]
fn prune_aggregate_drops_unneeded_aggregate_outputs() {
    // Aggregate with output_columns=[a, sum(b), avg(c)], required_output_columns={a, sum_b}
    // Expected: output_columns=[a, sum_b], aggregates=[sum(b)]
}
```

- [ ] **Step 18.2: Implement**

```rust
fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
    let LogicalPlan::Aggregate(mut node) = plan else { unreachable!() };
    let Some(needed) = &node.required_output_columns else {
        return Ok(RewriteResult::Unchanged);
    };
    let group_by_len = node.group_by.len();
    let original_output_len = node.output_columns.len();
    // group_by cols always kept (semantic requirement)
    let mut new_outputs: Vec<_> = node.output_columns[..group_by_len].to_vec();
    let mut new_aggregates: Vec<_> = Vec::new();
    for (i, agg) in node.aggregates.into_iter().enumerate() {
        let agg_output_id = node.output_columns[group_by_len + i].column_id;
        if needed.contains(&agg_output_id) {
            new_outputs.push(node.output_columns[group_by_len + i].clone());
            new_aggregates.push(agg);
        }
    }
    if new_outputs.len() == original_output_len {
        return Ok(RewriteResult::Unchanged);
    }
    node.output_columns = new_outputs;
    node.aggregates = new_aggregates;
    Ok(RewriteResult::Changed(LogicalPlan::Aggregate(node)))
}
```

- [ ] **Step 18.3-18.5: Test, register, commit**

---

### Task 19: PruneWindowColumns (Gap 5)

Same pattern as PruneAggregateColumns but for `WindowNode.window_exprs` + `output_columns`.

- [ ] **Step 19.1: Write failing test**
- [ ] **Step 19.2: Implement (mirror Aggregate)**
- [ ] **Step 19.3-19.5: Test, register, commit**

---

### Task 20: PruneUnionColumns + PruneIntersectColumns + PruneExceptColumns (Gap 4)

Each rule prunes its `output_columns` AND each branch's projection of corresponding position.

- [ ] **Step 20.1: Write failing test for PruneUnionColumns**

```rust
#[test]
fn prune_union_drops_unneeded_output_positions_and_branch_cols() {
    // Union[output: x,y,z], required_output_columns={y's id}
    // Expected: output_columns=[y]; each branch's relevant child gets only y position
}
```

- [ ] **Step 20.2: Implement (shared helper)**

```rust
fn prune_set_op<F, B>(
    node_inputs: Vec<LogicalPlan>,
    output_columns: Vec<OutputColumn>,
    needed: &HashSet<ColumnId>,
    rebuild: F,
) -> Result<RewriteResult, String>
where F: FnOnce(Vec<LogicalPlan>, Vec<OutputColumn>) -> LogicalPlan,
{
    // Compute positions of needed outputs
    let needed_positions: Vec<usize> = output_columns.iter().enumerate()
        .filter_map(|(i, c)| needed.contains(&c.column_id).then_some(i))
        .collect();
    // ... filter outputs and branches accordingly
    // ... auto_fill if empty
    unimplemented!()
}
```

(Branch-side pruning here only updates Union/Intersect/Except metadata; Phase 2 rules on each branch's interior operators will trigger separately for branch-internal pruning.)

- [ ] **Step 20.3-20.5: Tests, register, commit**

---

### Task 21: PruneCTEAnchor + PruneCTEConsume + PruneCTEProduce

- [ ] **Step 21.1: Write failing tests for all 3**

- [ ] **Step 21.2: Implement (CTEAnchor is no-op; Consume + Produce filter output_columns)**

```rust
// PruneCTEAnchorColumns
fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
    // No-op: anchor only carries cte_id + produce/consumer wrappers
    Ok(RewriteResult::Unchanged)
}
```

```rust
// PruneCTEConsumeColumns
fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
    let LogicalPlan::CTEConsume(mut node) = plan else { unreachable!() };
    let Some(needed) = &node.required_output_columns else { return Ok(RewriteResult::Unchanged); };
    let original_len = node.output_columns.len();
    node.output_columns.retain(|c| needed.contains(&c.column_id));
    if node.output_columns.is_empty() {
        // Keep at least 1 column
        let first = /* re-derive smallest col_id from original */;
        node.output_columns.push(first);
    }
    if node.output_columns.len() == original_len {
        return Ok(RewriteResult::Unchanged);
    }
    Ok(RewriteResult::Changed(LogicalPlan::CTEConsume(node)))
}
```

(Same for CTEProduce.)

- [ ] **Step 21.3-21.5: Tests, register, commit**

---

### Task 22: PruneDecodeColumns

- [ ] **Step 22.1: Write failing test**

```rust
#[test]
fn prune_decode_drops_unneeded_mappings_and_outputs() {
    // Decode with mappings={(dict_x, string_x), (dict_y, string_y)}, output_columns=[x,y]
    // required_output_columns={x's id}
    // Expected: output_columns=[x], mappings=[(dict_x, string_x)]
}
```

- [ ] **Step 22.2: Implement**

```rust
fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
    let LogicalPlan::Decode(mut node) = plan else { unreachable!() };
    let Some(needed) = &node.required_output_columns else {
        return Ok(RewriteResult::Unchanged);
    };
    let orig_output_len = node.output_columns.len();
    node.output_columns.retain(|c| needed.contains(&c.column_id));
    let kept_string_ids: HashSet<ColumnId> = node.output_columns.iter()
        .filter_map(|c| /* find string-column-id for this output */)
        .collect();
    node.mappings.retain(|m| kept_string_ids.contains(&m.string_column_id));
    if node.output_columns.len() == orig_output_len {
        return Ok(RewriteResult::Unchanged);
    }
    Ok(RewriteResult::Changed(LogicalPlan::Decode(node)))
}
```

- [ ] **Step 22.3-22.5: Test, register, commit**

---

### Task 23: All no-op rules (Filter, Join, Sort, Limit, Repeat, TableFunction)

These 6 rules are nearly identical. Single task creates all of them.

- [ ] **Step 23.1: Create 6 files with no-op pattern**

For each rule, e.g. `prune_filter.rs`:

```rust
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct PruneFilterColumns;

impl LogicalRewriteRule for PruneFilterColumns {
    fn name(&self) -> &'static str { "PruneFilterColumns" }
    fn phase(&self) -> RewritePhase { RewritePhase::StructuralRewrite }
    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Filter(_))
    }
    fn apply(&self, _plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        // Phase 1 already propagated predicate cols to child; nothing to prune here.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // ... minimal no-op test
}
```

Repeat for Join, Sort, Limit, Repeat, TableFunction (each ~30 lines).

- [ ] **Step 23.2: Write tests for each (6 tests)**

For each: build the operator with `required_output_columns = Some(...)`, apply the rule, assert `Unchanged`.

- [ ] **Step 23.3: Register all 6 in mod.rs**

- [ ] **Step 23.4: Run all rule tests**

```bash
cargo test --lib sql::optimizer::rewrite::rules::column_pruning
```

Expected: all passing.

- [ ] **Step 23.5: Commit**

```bash
git add src/sql/optimizer/rewrite/rules/column_pruning/
git commit -m "feat(rewrite): 6 no-op pruning rules for symmetry (Filter/Join/Sort/Limit/Repeat/TableFunction)"
```

---

### Task 24: Register all 18 rules in column_pruning_rules()

- [ ] **Step 24.1: Update mod.rs registration**

In `src/sql/optimizer/rewrite/rules/column_pruning/mod.rs`:

```rust
pub(crate) fn column_pruning_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![
        Box::new(prune_scan::PruneScanColumns),
        Box::new(prune_project::PruneProjectColumns),
        Box::new(prune_filter::PruneFilterColumns),
        Box::new(prune_aggregate::PruneAggregateColumns),
        Box::new(prune_join::PruneJoinColumns),
        Box::new(prune_sort::PruneSortColumns),
        Box::new(prune_limit::PruneLimitColumns),
        Box::new(prune_window::PruneWindowColumns),
        Box::new(prune_union::PruneUnionColumns),
        Box::new(prune_intersect::PruneIntersectColumns),
        Box::new(prune_except::PruneExceptColumns),
        Box::new(prune_subquery_alias::PruneSubqueryAliasColumns),
        Box::new(prune_cte_anchor::PruneCTEAnchorColumns),
        Box::new(prune_cte_consume::PruneCTEConsumeColumns),
        Box::new(prune_cte_produce::PruneCTEProduceColumns),
        Box::new(prune_repeat::PruneRepeatColumns),
        Box::new(prune_decode::PruneDecodeColumns),
        Box::new(prune_table_function::PruneTableFunctionColumns),
    ]
}
```

In `src/sql/optimizer/rewrite/rules/mod.rs`:

```rust
pub(crate) fn column_pruning_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    let mut rules = column_pruning::column_pruning_rules();
    rules.push(Box::new(ukfk::PruneUkFkJoin));
    rules.push(Box::new(ukfk::EliminateUniqueAggregate));
    rules
}
```

- [ ] **Step 24.2: Update is_known_rewrite_rule_name + registry tests**

Update `registry.rs` test `query_pipeline_contains_migrated_query_rules`:

```rust
assert_eq!(names, vec![
    "AggregatePushdown",
    "EliminateUniqueAggregate",
    "JoinReorder",
    "LowCardinalityDictionaryRewrite",
    "PruneAggregateColumns",
    "PruneCTEAnchorColumns",
    "PruneCTEConsumeColumns",
    "PruneCTEProduceColumns",
    "PruneDecodeColumns",
    "PruneExceptColumns",
    "PruneFilterColumns",
    "PruneIntersectColumns",
    "PruneJoinColumns",
    "PruneLimitColumns",
    "PruneProjectColumns",
    "PruneRepeatColumns",
    "PruneScanColumns",
    "PruneSortColumns",
    "PruneSubqueryAliasColumns",
    "PruneTableFunctionColumns",
    "PruneUKFKJoin",
    "PruneUnionColumns",
    "PruneWindowColumns",
    "PushDownPredicateAggregate",
    "PushDownPredicateAggregate",
    "PushDownPredicateJoin",
    "PushDownPredicateJoin",
    "PushDownPredicateProject",
    "PushDownPredicateProject",
    "PushDownPredicateScan",
    "PushDownPredicateScan",
    "PushSemiAntiRightOnlyCondition",
    "PushSemiAntiRightOnlyCondition",
    "TagRequiredColumns",
]);
```

- [ ] **Step 24.3: Run all tests**

```bash
cargo test --lib sql::optimizer::rewrite
```

Expected: passing.

- [ ] **Step 24.4: Commit**

```bash
git add src/sql/optimizer/rewrite/
git commit -m "feat(rewrite): register all 18 PruneXxxColumns rules in column_pruning_rules()"
```

---

## Phase D: Migration

### Task 25: Delete old `column_pruning.rs`, migrate 4 tests

**Files:**
- Delete: `src/sql/optimizer/rewrite/rules/column_pruning.rs` (562 lines)
- Modify: `src/sql/optimizer/rewrite/rules/mod.rs` (remove `pub(crate) mod column_pruning;` and `column_pruning::PruneColumns` reference)

- [ ] **Step 25.1: Inventory the 4 tests in old file**

```bash
grep -E "fn test|#\[test\]" src/sql/optimizer/rewrite/rules/column_pruning.rs
```

The 4 tests are:
- `root_scan_without_parent_keeps_all_columns` → migrate to `prune_scan.rs`
- `project_selecting_one_col_prunes_scan_required_columns` → migrate to `prune_project.rs`
- `filter_predicate_columns_are_preserved_in_scan_required` → migrate to `prune_filter.rs` (or just keep its semantic in pipeline integration test)
- `aggregate_group_by_and_agg_args_propagate_to_scan` → migrate to `prune_aggregate.rs`

- [ ] **Step 25.2: For each, port test body**

Each test currently invokes `PruneColumns.apply(plan)`. Rewrite each to invoke the appropriate new rule via the pipeline (since per-operator rules now require tagging first):

```rust
#[test]
fn project_selecting_one_col_prunes_scan_required_columns() {
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use std::collections::HashMap;

    let table = three_col_table();
    let scan = LogicalPlan::Scan(scan_node(&table));
    let project = LogicalPlan::Project(ProjectNode {
        input: Box::new(scan),
        items: vec![ProjectItem { ... only `a` ... }],
        required_output_columns: None,
    });

    let table_stats = HashMap::new();
    let mut ctx = RewriteContext::for_query(Vec::<String>::new());
    let pipeline = query_rewrite_pipeline(&table_stats);
    let out = pipeline.rewrite(project, &mut ctx).unwrap();

    if let LogicalPlan::Project(p) = out {
        if let LogicalPlan::Scan(s) = *p.input {
            assert_eq!(s.required_columns, Some(vec!["a".to_string()]));
        }
    }
}
```

- [ ] **Step 25.3: Delete old file**

```bash
git rm src/sql/optimizer/rewrite/rules/column_pruning.rs
```

In `src/sql/optimizer/rewrite/rules/mod.rs`, remove:

```rust
// pub(crate) mod column_pruning;  ← DELETE
```

- [ ] **Step 25.4: Build + run tests**

```bash
cargo build --lib
cargo test --lib sql::optimizer::rewrite
```

Expected: builds clean, all passing.

- [ ] **Step 25.5: Commit**

```bash
git add -A
git commit -m "refactor(rewrite): delete column_pruning.rs single-rule, migrate 4 tests to per-rule files"
```

---

### Task 26: EXPLAIN integration — emit `req=[...]` in Verbose/Costs

**Files:**
- Modify: `src/sql/explain.rs`

- [ ] **Step 26.1: Read explain.rs structure**

```bash
grep -nE "fn format_logical|fn format_physical|ExplainMode|Verbose|Costs" src/sql/explain.rs | head
```

- [ ] **Step 26.2: Write failing test**

In `src/sql/explain.rs` test module:

```rust
#[test]
fn explain_verbose_emits_req_for_tagged_operator() {
    // Build a Project with required_output_columns = Some({col_id_1})
    // Run format_logical(..., ExplainMode::Verbose)
    // Assert output contains "req=[a]"
}
```

- [ ] **Step 26.3: Confirm fail**

- [ ] **Step 26.4: Implement append-req-suffix in formatters**

Locate the function that formats each operator's header line. Append:

```rust
fn format_req_suffix(node_req: Option<&HashSet<ColumnId>>, columns: &[OutputColumn], mode: ExplainMode) -> String {
    if !matches!(mode, ExplainMode::Verbose | ExplainMode::Costs) {
        return String::new();
    }
    let Some(ids) = node_req else { return String::new(); };
    let names: Vec<&str> = columns.iter()
        .filter(|c| ids.contains(&c.column_id))
        .map(|c| c.name.as_str())
        .collect();
    format!(" req=[{}]", names.join(", "))
}
```

Wire it into each operator's format line.

- [ ] **Step 26.5: Run tests, see pass**

- [ ] **Step 26.6: Commit**

```bash
git add src/sql/explain.rs
git commit -m "feat(explain): emit req=[col_a, col_b] per operator in Verbose/Costs mode"
```

---

## Phase E: Validation

### Task 27: Golden plan `prune_subquery_alias_cte_left_semi.sql`

**Files:**
- Create: `sql-tests/optimizer/prune_subquery_alias_cte_left_semi.sql`
- Create: `sql-tests/optimizer/result/prune_subquery_alias_cte_left_semi.result`

- [ ] **Step 27.1: Create SQL test file**

```sql
-- @tags=column_pruning,gap1,gap3
-- @explain_contains=SubqueryAlias t2 req=[c_tinyint_null]
-- @explain_contains=columns: k1, c_tinyint_null

DROP TABLE IF EXISTS ${case_db}.t1;
CREATE TABLE ${case_db}.t1 (
    k1 bigint NULL,
    c_int int NULL,
    c_tinyint_null tinyint NULL,
    c_varchar varchar(100) NULL,
    c_bigint bigint NULL,
    c_date date NULL
) ENGINE=OLAP DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 4 PROPERTIES("replication_num"="1");

INSERT INTO ${case_db}.t1 VALUES
    (1, 10, 1, 'a', 100, '2023-01-01'),
    (2, 20, 2, 'b', 200, '2023-01-02'),
    (3, NULL, NULL, 'c', NULL, '2023-01-03');

-- Lock plan shape: build side must be pruned to single column.
EXPLAIN VERBOSE WITH w1 AS (SELECT * FROM ${case_db}.t1 WHERE k1 < 100)
SELECT count(1)
FROM ${case_db}.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;
```

- [ ] **Step 27.2: Record initial result**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer \
  --only prune_subquery_alias_cte_left_semi \
  --mode record \
  --update-expected
```

- [ ] **Step 27.3: Verify it passes verify mode**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer \
  --only prune_subquery_alias_cte_left_semi \
  --mode verify
```

Expected: PASS.

- [ ] **Step 27.4: Commit**

```bash
git add sql-tests/optimizer/prune_subquery_alias_cte_left_semi.sql sql-tests/optimizer/result/prune_subquery_alias_cte_left_semi.result
git commit -m "test(optimizer): golden plan for SubqueryAlias + CTE column pruning (Gap 1 + Gap 3)"
```

---

### Task 28-34: Remaining 7 golden plan tests

Same pattern as Task 27 — for each:

- **Task 28**: `prune_project_items_filter_only.sql` — `EXPLAIN VERBOSE SELECT a FROM (SELECT * FROM t10col) sub;` lock that Project shows 1 item, Scan reads 1 col.
- **Task 29**: `prune_cte_anchor_multi_consume.sql` — 2 CTEConsumers needing different subsets; lock producer scan reads union.
- **Task 30**: `prune_union_branch_alignment.sql` — 3-branch UNION ALL; lock all branches drop same positions.
- **Task 31**: `prune_intersect_branch_alignment.sql` — same for INTERSECT.
- **Task 32**: `prune_aggregate_unused_agg.sql` — Agg with multiple aggregates, outer needs only one.
- **Task 33**: `prune_window_unused_output.sql` — Window with multiple OVER, outer needs one.
- **Task 34**: `prune_idempotent_fixed_point.sql` — same plan piped twice; plan unchanged second time.

For each task:
- [ ] Write SQL + assert annotations
- [ ] Record result mode
- [ ] Verify mode passes
- [ ] Commit per task

(Tasks 28-34 each follow Task 27's 4-step pattern exactly. See spec §9.2 for what each asserts.)

---

### Task 35: Final integration verification + suite gating

**Files:**
- None modified (verification only)

- [ ] **Step 35.1: Run join suite -j 1**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite join \
  --mode verify -j 1 --query-timeout 180
```

Expected: **57/60 pass, 3 fail** (same as pre-OQ-1 baseline: array_type / force_partition_hash / full_outer_with_using — these 3 are out-of-scope known failures, not regressions).

If wall_time is recorded in the runner summary (it is — see `slowest cases (top 5)` + `suite join wall_time=`), compare against the 1996s pre-OQ-1 baseline. **Hard gate: wall_time ≤ 1400s (-30%)**.

- [ ] **Step 35.2: Run cte suite -j 1**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite cte \
  --mode verify -j 1
```

Expected: 100% pass.

- [ ] **Step 35.3: Run TPC-DS subset (CTE-heavy queries)**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-ds --only q11,q14,q35,q69 \
  --mode verify -j 1
```

Expected: 100% pass.

- [ ] **Step 35.4: Run cargo test --lib**

```bash
cargo test --lib 2>&1 | tail -10
```

Expected: ~2800+ tests pass, 0 fail.

- [ ] **Step 35.5: Capture before/after plan diffs for PR description**

Run on the standalone server pre/post OQ-1:

```sql
USE opt_probe;
EXPLAIN VERBOSE
WITH w1 AS (SELECT * FROM opt_probe.t1 WHERE k1 < 100)
SELECT count(1), count(t1.k1), count(t1.c_tinyint_null)
FROM opt_probe.t1 t1 LEFT SEMI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;
```

Save before/after into `/tmp/nova_q22_before.txt` and `/tmp/nova_q22_after.txt`. Diff them for the PR description.

Repeat for:
- `join_linear_chained` q31 (3-leg UNION ALL CTE)
- Simple INNER count(*): `SELECT count(*) FROM opt_probe.t1 tt1 JOIN opt_probe.t1 tt2 ON tt1.c_int = tt2.c_int WHERE tt1.k1 < 50;`

- [ ] **Step 35.6: Open the PR**

```bash
git push origin claude/oq-1-column-pruning
gh pr create --title "OQ-1: per-operator column pruning architecture refactor" --body "$(cat <<'EOF'
## Summary
Replace single 562-line PruneColumns rule with per-operator pruning architecture
(18 Prune*Columns rules + TagRequiredColumns Phase 1 pass + ColumnId-based
required_output_columns). Closes 5 column-pruning gaps from OQ-1 preflight
audit.

## Test plan
- [ ] join suite -j 1 verify: 57/60 pass (same as baseline, 3 known unrelated fails)
- [ ] join suite -j 1 wall_time ≥ 30% reduction vs 1996s baseline
- [ ] cte suite -j 1 verify: 100% pass
- [ ] tpc-ds q11/q14/q35/q69 -j 1 verify: 100% pass
- [ ] cargo test --lib: all passing
- [ ] 8 new golden plan tests in sql-tests/optimizer/ all PASS

## Plan diffs

### join_one_key q22 (before)
[paste /tmp/nova_q22_before.txt]

### join_one_key q22 (after)
[paste /tmp/nova_q22_after.txt]

### join_linear_chained q31 (before/after)
[paste diffs]

### Inner count(*) (before/after)
[paste diffs]

Spec: docs/design/specs/2026-05-28-oq-1-column-pruning-arch-refactor-design.md
Plan: docs/design/plans/2026-05-28-oq-1-column-pruning-arch-refactor.md
EOF
)"
```

- [ ] **Step 35.7: Commit any remaining state + tag**

```bash
git status --short
# Should be clean
```

---

## Plan Self-Review

Before handoff, the implementor (or executing skill) should verify:

1. **All 35 tasks have explicit steps with code blocks.** No "implement similar to Task N" — code repeated where needed.
2. **Each Prune* rule has matching test fixtures.** Tasks 15-23.
3. **TagRequiredColumns is registered in pipeline.** Task 14.
4. **Old column_pruning.rs deleted.** Task 25.
5. **Migration tests preserve original 4 unit-test scenarios.** Task 25.
6. **8 golden plan tests cover all 5 gaps.** Tasks 27-34.
7. **Final verification runs join + cte + tpc-ds + cargo test.** Task 35.

Spec coverage check:
- Spec §5 (Phase 1 per-operator handlers) ↔ Tasks 5-13
- Spec §6 (Phase 2 per-operator rules, 18 of them) ↔ Tasks 15-23
- Spec §7 (pipeline + trace + EXPLAIN) ↔ Tasks 14, 26
- Spec §8 (Gap-to-Rule mapping) ↔ Tasks 10, 16, 12, 11, 18, 19, 20
- Spec §9 (3 test gates) ↔ Tasks 23 (unit), 27-34 (golden), 35 (suite)
- Spec §10 (out of scope) ↔ Not tasked, deferred per spec
- Spec §11 (success criteria) ↔ Task 35 step 35.5

All covered.
