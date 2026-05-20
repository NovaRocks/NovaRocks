# OPT-1 Aggregate Pushdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `LogicalAggregate → LogicalJoin` pushdown rule to the NovaRocks RBO pipeline. Push partial aggregates close to leaves when NDV bucketing says it reduces rows. Verify via OPT-5 plan-golden suite + `disable_optimizer_rules` knob.

**Architecture:** Two-phase Collector / Rewriter (StarRocks pattern). **v1 scope restriction:** `aggregate.input` must be a `Join` directly, with both sides being `Scan` (no `Filter` / `Project` intermediation, no nested joins). The collector picks one side based on required-column visibility; the rewriter wraps that side with a partial `AggregateNode`. Cost gate (NDV bucketing) decides commit. Top-level call rewrites: `COUNT → SUM` at final, others identity. `AggregateNode.already_pushed` flag makes the rule idempotent.

Multi-table joins, Filter/Project intermediation, and AVG/STDDEV are filed as OPT-1 follow-ups (see spec §10).

**Tech Stack:** Rust 2021, existing RBO driver (`src/sql/optimizer/rbo/`), existing stats subsystem (`src/sql/optimizer/rbo/rules/join_reorder/cardinality.rs`).

**Spec:** `docs/superpowers/specs/2026-05-20-opt-1-aggregate-pushdown-design.md`

---

## File Structure

**Created:**

- `src/sql/optimizer/rbo/rules/aggregate_pushdown/mod.rs` — module root, `aggregate_pushdown_rules(table_stats) -> Vec<Box<dyn RewriteRule>>`.
- `src/sql/optimizer/rbo/rules/aggregate_pushdown/rule.rs` — `AggregatePushdownRule` impl of `RewriteRule`.
- `src/sql/optimizer/rbo/rules/aggregate_pushdown/context.rs` — `AggregatePushDownContext` and `PushPlan` structs.
- `src/sql/optimizer/rbo/rules/aggregate_pushdown/collector.rs` — top-down collector, safety filters, `split_at_join`.
- `src/sql/optimizer/rbo/rules/aggregate_pushdown/rewriter.rs` — splice + function rewrite + partial result-type helper.
- `src/sql/optimizer/rbo/rules/aggregate_pushdown/cost.rs` — NDV bucketing + row-count threshold.
- `sql-tests/optimizer/sql/aggregate_pushdown_inner_join.sql` (+ matching `.result`)
- `sql-tests/optimizer/sql/aggregate_pushdown_count_star_rejected.sql` (+ matching `.result`)
- `sql-tests/optimizer/sql/aggregate_pushdown_left_outer_preserved.sql` (+ matching `.result`)
- `sql-tests/optimizer/sql/aggregate_pushdown_disabled.sql` (+ matching `.result`)

**Modified:**

- `src/sql/planner/plan.rs` — `AggregateNode` gains `already_pushed: bool`.
- `src/sql/planner/mod.rs:387, 611` — two construction sites set `already_pushed: false`.
- `src/sql/optimizer/cte_rewrite.rs:81, 198` — two clone-into-new-node sites carry the flag.
- `src/sql/optimizer/rbo/rules/column_pruning.rs:156, 485` — same.
- `src/sql/optimizer/rbo/rules/predicate_pushdown/push_to_aggregate.rs:82, 164` — same.
- `src/sql/optimizer/rbo/rules/join_reorder/cardinality.rs:701` (test) — same.
- `src/sql/optimizer/rbo/rules/join_reorder/cost.rs:193` (test) — same.
- `src/sql/optimizer/stats.rs:1566` (test) — same.
- `src/sql/optimizer/rbo/rules/mod.rs` — re-export, add to `all_rbo_rules`, bump test count.
- `src/sql/optimizer/mod.rs:78` — insert new fixed-point pass between second predicate-pushdown and column-pruning.
- `AGENTS.md` (symlinked from `CLAUDE.md`) §9 — new bullet pointing at the rule.

---

## Task 1: Add `already_pushed` field to `AggregateNode`

**Goal:** Idempotency flag exists, defaults to false, all construction sites compile.

**Files:**
- Modify: `src/sql/planner/plan.rs:177-182`
- Modify (10 construction sites): `src/sql/planner/mod.rs:387, 611`; `src/sql/optimizer/cte_rewrite.rs:81, 198`; `src/sql/optimizer/rbo/rules/column_pruning.rs:156, 485`; `src/sql/optimizer/rbo/rules/predicate_pushdown/push_to_aggregate.rs:82, 164`; `src/sql/optimizer/rbo/rules/join_reorder/cardinality.rs:701`; `src/sql/optimizer/rbo/rules/join_reorder/cost.rs:193`; `src/sql/optimizer/stats.rs:1566`.

- [ ] **Step 1: Add the field**

Edit `src/sql/planner/plan.rs:177-182`:

```rust
#[derive(Clone, Debug)]
pub(crate) struct AggregateNode {
    pub input: Box<LogicalPlan>,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
    /// Set to true by `AggregatePushdownRule`'s rewriter on the FINAL
    /// (top-level) aggregate after a partial aggregate has been spliced
    /// below. The collector treats `already_pushed = true` as a hard
    /// "skip" signal so the rule does not re-fire on its own output.
    /// Other rules (predicate pushdown, column pruning, cte rewrite,
    /// etc.) MUST preserve this flag when cloning `AggregateNode`.
    pub already_pushed: bool,
}
```

- [ ] **Step 2: Verify build fails until all construction sites pass the field**

```
cargo build --lib 2>&1 | grep -E "missing.*already_pushed" | head -20
```

Expected: ~10 errors, one per site listed in **Files** above.

- [ ] **Step 3: Add `already_pushed: false` to every production construction site**

For each of these lines, add `already_pushed: false,` as the last field
in the struct literal. Open the file, find the `AggregateNode { ... }`
literal, add the field. Files and lines (line numbers may shift after
the edit at the top of the file):

- `src/sql/planner/mod.rs:387, 611`
- `src/sql/optimizer/cte_rewrite.rs:81, 198`
- `src/sql/optimizer/rbo/rules/column_pruning.rs:156, 485`
- `src/sql/optimizer/rbo/rules/predicate_pushdown/push_to_aggregate.rs:82, 164`

For test-only sites, do the same:

- `src/sql/optimizer/rbo/rules/join_reorder/cardinality.rs:701`
- `src/sql/optimizer/rbo/rules/join_reorder/cost.rs:193`
- `src/sql/optimizer/stats.rs:1566`

(`src/sql/optimizer/rbo/driver.rs:122` uses the `..n` struct update
pattern and does NOT need to be modified — the flag carries through.)

- [ ] **Step 4: Verify clean build**

```
cargo build --lib 2>&1 | tail -3
```

Expected: `Finished ...` no errors.

- [ ] **Step 5: Verify existing tests still pass**

```
cargo test --lib -- sql::planner:: sql::optimizer:: 2>&1 | tail -10
```

Expected: PASS, 0 failures. No regressions.

- [ ] **Step 6: Add a smoke test that `already_pushed` defaults to false**

Append to `src/sql/planner/plan.rs` `#[cfg(test)] mod tests` (create one
if absent — check the bottom of the file):

```rust
#[cfg(test)]
mod plan_tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;

    #[test]
    fn aggregate_node_already_pushed_defaults_false_via_construction() {
        // Construction is explicit; this just asserts no constructor in
        // the crate accidentally sets the flag to true. Sample one
        // production construction site by reproducing its shape minimally.
        let node = AggregateNode {
            input: Box::new(LogicalPlan::Values(crate::sql::planner::plan::ValuesNode {
                rows: vec![],
                output_columns: vec![],
            })),
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(!node.already_pushed);
    }
}
```

(If `ValuesNode` is named differently, grep for the correct enum variant
construction in `src/sql/planner/plan.rs` and substitute.)

- [ ] **Step 7: Run all library tests once**

```
cargo test --lib 2>&1 | tail -3
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
cargo fmt
git add src/sql/planner/plan.rs src/sql/planner/mod.rs \
  src/sql/optimizer/cte_rewrite.rs \
  src/sql/optimizer/rbo/rules/column_pruning.rs \
  src/sql/optimizer/rbo/rules/predicate_pushdown/push_to_aggregate.rs \
  src/sql/optimizer/rbo/rules/join_reorder/cardinality.rs \
  src/sql/optimizer/rbo/rules/join_reorder/cost.rs \
  src/sql/optimizer/stats.rs
git commit -m "feat(plan): add AggregateNode::already_pushed flag

Prepares the LogicalPlan AggregateNode for OPT-1's aggregate-pushdown
rule. Default false everywhere; the rule's rewriter sets it to true on
the FINAL aggregate after splicing a partial below. Other rules clone
the flag transparently."
```

---

## Task 2: Scaffold `aggregate_pushdown` module

**Goal:** New module exists, rule registered, returns `None` for every
plan. Production behavior unchanged.

**Files:**
- Create: `src/sql/optimizer/rbo/rules/aggregate_pushdown/mod.rs`
- Create: `src/sql/optimizer/rbo/rules/aggregate_pushdown/rule.rs`
- Create: `src/sql/optimizer/rbo/rules/aggregate_pushdown/context.rs`
- Create: `src/sql/optimizer/rbo/rules/aggregate_pushdown/collector.rs`
- Create: `src/sql/optimizer/rbo/rules/aggregate_pushdown/rewriter.rs`
- Create: `src/sql/optimizer/rbo/rules/aggregate_pushdown/cost.rs`
- Modify: `src/sql/optimizer/rbo/rules/mod.rs:9-19, 51-59, 67-85`

- [ ] **Step 1: Create the module root**

Create `src/sql/optimizer/rbo/rules/aggregate_pushdown/mod.rs`:

```rust
//! Aggregate pushdown rule (OPT-1).
//!
//! Pushes `LogicalAggregate` past `LogicalJoin` toward leaves when cost-justified.
//! See docs/superpowers/specs/2026-05-20-opt-1-aggregate-pushdown-design.md.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::optimizer::rbo::rule::RewriteRule;
use crate::sql::optimizer::statistics::TableStatistics;

pub(crate) mod collector;
pub(crate) mod context;
pub(crate) mod cost;
pub(crate) mod rewriter;
pub(crate) mod rule;

pub(crate) use rule::AggregatePushdownRule;

#[allow(dead_code)]
pub(crate) fn aggregate_pushdown_rules(
    table_stats: &HashMap<String, TableStatistics>,
) -> Vec<Box<dyn RewriteRule>> {
    vec![Box::new(AggregatePushdownRule::new(Arc::new(
        table_stats.clone(),
    )))]
}
```

- [ ] **Step 2: Create the rule stub**

Create `src/sql/optimizer/rbo/rules/aggregate_pushdown/rule.rs`:

```rust
//! AggregatePushdownRule entry point.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::optimizer::rbo::rule::RewriteRule;
use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::planner::plan::LogicalPlan;

#[allow(dead_code)]
pub(crate) struct AggregatePushdownRule {
    table_stats: Arc<HashMap<String, TableStatistics>>,
}

impl AggregatePushdownRule {
    #[allow(dead_code)]
    pub(crate) fn new(table_stats: Arc<HashMap<String, TableStatistics>>) -> Self {
        Self { table_stats }
    }
}

impl RewriteRule for AggregatePushdownRule {
    fn name(&self) -> &'static str {
        "AggregatePushdown"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Aggregate(_))
    }

    fn apply(&self, _plan: LogicalPlan) -> Option<LogicalPlan> {
        // Stub: return None until collector + rewriter land.
        None
    }
}
```

- [ ] **Step 3: Create empty `context.rs`, `collector.rs`, `rewriter.rs`, `cost.rs`**

Each file is just a header comment + empty body. Example for
`context.rs`:

```rust
//! Aggregate pushdown collector/rewriter shared state.
//!
//! Implemented in later tasks (collector, rewriter).
```

Identical shape for the other three (substitute the noun).

- [ ] **Step 4: Wire the module into `rbo/rules/mod.rs`**

Edit `src/sql/optimizer/rbo/rules/mod.rs`:

After line 9 (`pub(crate) mod column_pruning;`), add:

```rust
pub(crate) mod aggregate_pushdown;
```

In `all_rbo_rules` (around line 51-58), add a line after
`all.extend(join_reorder_rules(table_stats));`:

```rust
    all.extend(aggregate_pushdown::aggregate_pushdown_rules(table_stats));
```

Update the test `registry_contains_expected_rules` (line 67-85):

```rust
    #[test]
    fn registry_contains_expected_rules() {
        let rules = all_rbo_rules(&HashMap::new());
        assert_eq!(rules.len(), 10);
        let mut names: Vec<&str> = rules.iter().map(|r| r.name()).collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "AggregatePushdown",
                "EliminateUniqueAggregate",
                "JoinReorder",
                "PruneColumns",
                "PruneUkFkJoin",
                "PushDownPredicateAggregate",
                "PushDownPredicateJoin",
                "PushDownPredicateProject",
                "PushDownPredicateScan",
                "PushSemiAntiRightOnlyCondition",
            ]
        );
    }
```

- [ ] **Step 5: Verify build clean**

```
cargo build --lib 2>&1 | tail -3
cargo test --lib -- sql::optimizer::rbo::rules:: 2>&1 | tail -10
```

Expected: build clean; `registry_contains_expected_rules` passes.

- [ ] **Step 6: Add a smoke test on the stub**

Append to `src/sql/optimizer/rbo/rules/aggregate_pushdown/rule.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::planner::plan::{AggregateNode, ScanNode};

    fn dummy_aggregate() -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(LogicalPlan::Scan(ScanNode {
                database: "db".into(),
                table: crate::sql::catalog::TableDef {
                    name: "t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: None,
                    storage: crate::sql::catalog::TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/t.parquet"),
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
                    name: "id".into(),
                    data_type: arrow::datatypes::DataType::Int32,
                    nullable: false,
                }],
                predicates: vec![],
                required_columns: None,
            })),
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            already_pushed: false,
        })
    }

    #[test]
    fn stub_returns_none() {
        let rule = AggregatePushdownRule::new(Arc::new(HashMap::new()));
        let plan = dummy_aggregate();
        assert!(rule.matches(&plan));
        assert!(rule.apply(plan).is_none());
    }
}
```

- [ ] **Step 7: Run + commit**

```bash
cargo build --lib
cargo test --lib -- sql::optimizer::rbo::rules:: 2>&1 | tail -5
cargo fmt
git add src/sql/optimizer/rbo/rules/aggregate_pushdown/ \
  src/sql/optimizer/rbo/rules/mod.rs
git commit -m "feat(rbo): scaffold AggregatePushdown rule module

Empty Collector/Rewriter/Cost stubs; rule registered in all_rbo_rules
returning None for every plan. Production behavior unchanged."
```

---

## Task 3: Pipeline wire-up

**Goal:** Rule actually runs at the documented position (between
second predicate-pushdown pass and column pruning). Since rule still
returns `None`, no observable behavior change.

**Files:**
- Modify: `src/sql/optimizer/mod.rs:78-84`

- [ ] **Step 1: Insert the new fixed-point pass**

Edit `src/sql/optimizer/mod.rs` after line 78 (after the second
`rewrite_to_fixed_point` for predicate pushdown), before line 79 (the
column_pruning pass):

```rust
    // OPT-1: aggregate pushdown. Runs after predicates settle and joins
    // are reordered (so the join shape is final), but before column
    // pruning (which needs to see the partial aggregate's required cols).
    let rewritten = rbo::driver::rewrite_to_fixed_point(
        rewritten,
        &rbo::rules::aggregate_pushdown::aggregate_pushdown_rules(table_stats),
        &options,
        deadline,
    )?;
```

- [ ] **Step 2: Build + run full library tests**

```
cargo build --lib
cargo test --lib 2>&1 | tail -5
```

Expected: PASS, 0 failures. (Rule still returns None — nothing changes.)

- [ ] **Step 3: Commit**

```bash
cargo fmt
git add src/sql/optimizer/mod.rs
git commit -m "feat(optimizer): wire AggregatePushdown into RBO pipeline

Runs as its own fixed-point pass between the second predicate-pushdown
pass and column pruning. Rule body still returns None; the wire-in is
a behavior-preserving change that the next commits build on."
```

---

## Task 4: Collector — entry rejection (safety filters)

**Goal:** Reject every aggregate that fails an entry-level safety
check. No traversal yet.

**Files:**
- Modify: `src/sql/optimizer/rbo/rules/aggregate_pushdown/context.rs`
- Modify: `src/sql/optimizer/rbo/rules/aggregate_pushdown/collector.rs`
- Modify: `src/sql/optimizer/rbo/rules/aggregate_pushdown/rule.rs`

- [ ] **Step 1: Define `AggregatePushDownContext` and `PushPlan`**

Replace `context.rs` content with:

```rust
//! Aggregate pushdown collector/rewriter shared state.

use crate::sql::analysis::TypedExpr;
use crate::sql::planner::plan::{AggregateCall, LogicalPlan};

/// Which side of the original join receives the partial aggregate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    Left,
    Right,
}

/// State accumulated by the collector before producing a PushPlan.
#[derive(Clone, Debug)]
pub(crate) struct AggregatePushDownContext {
    /// Original group-by expressions from the LogicalAggregate at the
    /// top of the descent. Unchanged across the walk.
    pub original_groupby: Vec<TypedExpr>,
    /// Original aggregate calls from the top LogicalAggregate.
    pub original_aggregates: Vec<AggregateCall>,
    /// Columns required by aggregate args + group-by.
    pub required_columns: Vec<String>,
}

/// Result of a successful collector descent.
#[derive(Clone, Debug)]
pub(crate) struct PushPlan {
    /// Which side of the original join the partial aggregate wraps.
    pub side: Side,
    /// The chosen side's subtree (a `LogicalPlan::Scan` in v1).
    pub target_subtree: LogicalPlan,
    /// Group-by columns for the partial aggregate.
    pub partial_groupby: Vec<TypedExpr>,
    /// Aggregate calls to use at the partial stage. For v1 these are
    /// the same shape as the original calls (function name unchanged
    /// for SUM/MIN/MAX/COUNT — see rewriter for the final-stage table).
    pub partial_aggregates: Vec<AggregateCall>,
}
```

- [ ] **Step 2: Write failing tests for entry rejections in collector.rs**

Replace `collector.rs` with:

```rust
//! Aggregate pushdown collector — phase 1 of the rule.

use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::planner::plan::{AggregateCall, AggregateNode, LogicalPlan};
use std::collections::HashMap;

use super::context::{AggregatePushDownContext, PushPlan};

/// Entry: examine the AggregateNode for entry-level rejections.
/// Returns Some(ctx) when the aggregate is a candidate to push;
/// returns None when an entry-level filter rejects it.
pub(crate) fn entry_safety_check(
    aggregate: &AggregateNode,
) -> Option<AggregatePushDownContext> {
    // Idempotency guard.
    if aggregate.already_pushed {
        return None;
    }
    // Empty group-by: partial collapses to a single row.
    if aggregate.group_by.is_empty() {
        return None;
    }
    // Per-call filters.
    for call in &aggregate.aggregates {
        // Distinct is SplitDistinctAgg's domain.
        if call.distinct {
            return None;
        }
        // Order-sensitive aggregate.
        if !call.order_by.is_empty() {
            return None;
        }
        // White-list check.
        let name = call.name.to_ascii_lowercase();
        if !matches!(name.as_str(), "sum" | "min" | "max" | "count") {
            return None;
        }
        // COUNT(*) has no args.
        if name == "count" && call.args.is_empty() {
            return None;
        }
        // Args must be bare ColumnRefs.
        for arg in &call.args {
            if !matches!(arg.kind, ExprKind::ColumnRef { .. }) {
                return None;
            }
            // Non-deterministic functions in args.
            if expr_uses_nondeterministic(arg) {
                return None;
            }
        }
    }

    Some(AggregatePushDownContext {
        original_groupby: aggregate.group_by.clone(),
        original_aggregates: aggregate.aggregates.clone(),
        required_columns: collect_required_columns(aggregate),
    })
}

fn collect_required_columns(aggregate: &AggregateNode) -> Vec<String> {
    let mut out = Vec::new();
    for gb in &aggregate.group_by {
        collect_column_refs_into(gb, &mut out);
    }
    for call in &aggregate.aggregates {
        for arg in &call.args {
            collect_column_refs_into(arg, &mut out);
        }
    }
    out.sort();
    out.dedup();
    out
}

fn collect_column_refs_into(expr: &TypedExpr, out: &mut Vec<String>) {
    if let ExprKind::ColumnRef { name, .. } = &expr.kind {
        out.push(name.clone());
    }
    // (Non-ColumnRef expressions in the top-level group_by are allowed
    // structurally; we just don't collect them here. They would have
    // been caught earlier if they appeared in aggregate args.)
}

const NONDETERMINISTIC_FUNCTIONS: &[&str] = &[
    "rand",
    "random",
    "uuid",
    "now",
    "current_timestamp",
    "current_date",
];

fn expr_uses_nondeterministic(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::FunctionCall { name, args, .. } => {
            if NONDETERMINISTIC_FUNCTIONS
                .iter()
                .any(|n| n.eq_ignore_ascii_case(name))
            {
                return true;
            }
            args.iter().any(expr_uses_nondeterministic)
        }
        ExprKind::BinaryOp { left, right, .. } => {
            expr_uses_nondeterministic(left) || expr_uses_nondeterministic(right)
        }
        ExprKind::UnaryOp { expr: inner, .. } => expr_uses_nondeterministic(inner),
        _ => false,
    }
}

/// Top-level collector entry. To be wired in subsequent tasks.
#[allow(dead_code)]
pub(crate) fn collect_push_plan(
    aggregate: &AggregateNode,
    _table_stats: &HashMap<String, TableStatistics>,
) -> Option<PushPlan> {
    let _ctx = entry_safety_check(aggregate)?;
    // Traversal added in Tasks 5 and 6.
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn};
    use crate::sql::planner::plan::{AggregateNode, LogicalPlan, ValuesNode};
    use arrow::datatypes::DataType;

    fn col_ref(name: &str, ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                name: name.into(),
                table_alias: None,
            },
            data_type: ty,
            nullable: true,
        }
    }

    fn make_agg(
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
        already_pushed: bool,
    ) -> AggregateNode {
        AggregateNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                output_columns: vec![],
            })),
            group_by,
            aggregates,
            output_columns: vec![],
            already_pushed,
        }
    }

    fn sum_call(col: &str) -> AggregateCall {
        AggregateCall {
            name: "sum".into(),
            args: vec![col_ref(col, DataType::Int64)],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
        }
    }

    #[test]
    fn rejects_empty_groupby() {
        let agg = make_agg(vec![], vec![sum_call("v")], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_distinct_aggregate() {
        let mut call = sum_call("v");
        call.distinct = true;
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![call], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_order_sensitive_aggregate() {
        let mut call = sum_call("v");
        call.order_by.push(crate::sql::analysis::SortItem {
            expr: col_ref("v", DataType::Int64),
            asc: true,
            nulls_first: false,
        });
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![call], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_count_star() {
        let count_star = AggregateCall {
            name: "count".into(),
            args: vec![],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
        };
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![count_star], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_avg_function() {
        let avg = AggregateCall {
            name: "avg".into(),
            args: vec![col_ref("v", DataType::Int64)],
            distinct: false,
            result_type: DataType::Float64,
            order_by: vec![],
        };
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![avg], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_aggregate_expr_not_columnref() {
        let mut call = sum_call("v");
        call.args[0] = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref("a", DataType::Int64)),
                op: crate::sql::analysis::BinOp::Add,
                right: Box::new(col_ref("b", DataType::Int64)),
            },
            data_type: DataType::Int64,
            nullable: true,
        };
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![call], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_nondeterministic_arg() {
        let mut call = sum_call("v");
        call.args[0] = TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "rand".into(),
                args: vec![],
                table_alias: None,
            },
            data_type: DataType::Float64,
            nullable: false,
        };
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![call], false);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn rejects_already_pushed_aggregate() {
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![sum_call("v")], true);
        assert!(entry_safety_check(&agg).is_none());
    }

    #[test]
    fn accepts_inner_join_candidate() {
        let agg = make_agg(vec![col_ref("k", DataType::Int64)], vec![sum_call("v")], false);
        let ctx = entry_safety_check(&agg).expect("should pass entry checks");
        assert_eq!(ctx.original_groupby.len(), 1);
        assert_eq!(ctx.original_aggregates.len(), 1);
        assert!(ctx.required_columns.contains(&"k".to_string()));
        assert!(ctx.required_columns.contains(&"v".to_string()));
    }
}
```

(`ExprKind` variant names and `BinOp::Add` may need adjustment — verify
against `src/sql/analysis/mod.rs` before submitting the test. If
`FunctionCall` has a different field set, fix the test literals to
match.)

- [ ] **Step 3: Run failing tests**

```
cargo test --lib -- sql::optimizer::rbo::rules::aggregate_pushdown::collector 2>&1 | tail -15
```

Expected: 9 PASS (the entry_safety_check function is fully written;
this Step actually validates it works).

- [ ] **Step 4: Verify clippy clean on new code**

```
cargo clippy --lib 2>&1 | grep -E "aggregate_pushdown/" | head -10
```

Expected: empty (no new warnings).

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add src/sql/optimizer/rbo/rules/aggregate_pushdown/context.rs \
        src/sql/optimizer/rbo/rules/aggregate_pushdown/collector.rs
git commit -m "feat(aggpd): collector entry safety filters

Validates the AggregateNode itself: empty group-by, distinct,
order-sensitive, non-whitelist function, COUNT(*), non-ColumnRef args,
non-deterministic arg, already_pushed. Returns Some(ctx) only for
candidate aggregates. Traversal (Scan/Filter/Project/Join) lands in
Tasks 5 and 6."
```

---

## Task 5: Collector — gate `aggregate.input` to direct Join

**Goal:** v1 only handles direct two-table joins. The collector
accepts only `aggregate.input == LogicalPlan::Join`. Anything else
(Scan / Filter / Project / Aggregate / Sort / Limit / nested Join /
etc.) is rejected with a clear test. Task 6 implements the
`split_at_join` logic.

**Files:**
- Modify: `src/sql/optimizer/rbo/rules/aggregate_pushdown/collector.rs`

- [ ] **Step 1: Replace `collect_push_plan` stub with the v1 gate**

```rust
pub(crate) fn collect_push_plan(
    aggregate: &AggregateNode,
    _table_stats: &HashMap<String, TableStatistics>,
) -> Option<PushPlan> {
    let ctx = entry_safety_check(aggregate)?;
    // v1: aggregate.input MUST be a Join directly. Filter/Project
    // intermediation and nested-Join targets are OPT-1 follow-ups.
    let join = match aggregate.input.as_ref() {
        LogicalPlan::Join(j) => j,
        _ => return None,
    };
    // split_at_join lands in Task 6.
    let _ = ctx;
    let _ = join;
    None
}
```

- [ ] **Step 2: Add rejection tests for non-Join inputs**

Append to the `tests` module in `collector.rs`:

```rust
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::planner::plan::{FilterNode, ProjectItem, ProjectNode, ScanNode};

    fn dummy_scan_with_cols(cols: &[(&str, DataType)]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                iceberg_table: None,
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: cols
                .iter()
                .map(|(n, ty)| OutputColumn {
                    name: (*n).into(),
                    data_type: ty.clone(),
                    nullable: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
        })
    }

    #[test]
    fn rejects_when_input_is_scan_directly() {
        // No Join means no work to do — would just wrap the scan with an
        // identity partial that buys nothing. v1 rejects.
        let scan = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let agg = AggregateNode {
            input: Box::new(scan),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }

    #[test]
    fn rejects_when_input_is_filter_above_join() {
        // Filter intermediation between Aggregate and Join is an OPT-1
        // follow-up. v1 rejects.
        let scan_a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let scan_b = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(crate::sql::planner::plan::JoinNode {
            left: Box::new(scan_a),
            right: Box::new(scan_b),
            join_type: crate::sql::analysis::JoinKind::Inner,
            condition: Some(col_ref("k", DataType::Boolean)),
        });
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(join),
            predicate: col_ref("k", DataType::Boolean),
        });
        let agg = AggregateNode {
            input: Box::new(filter),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }

    #[test]
    fn rejects_when_input_is_project_above_join() {
        let scan_a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let scan_b = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(crate::sql::planner::plan::JoinNode {
            left: Box::new(scan_a),
            right: Box::new(scan_b),
            join_type: crate::sql::analysis::JoinKind::Inner,
            condition: Some(col_ref("k", DataType::Boolean)),
        });
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(join),
            items: vec![ProjectItem {
                expr: col_ref("k", DataType::Int64),
                output_name: "k".into(),
            }],
            output_columns: vec![],
        });
        let agg = AggregateNode {
            input: Box::new(project),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }
```

(Inspect `FilterNode`, `ProjectNode`, `ProjectItem` actual field names
in `src/sql/planner/plan.rs` and adjust if needed. `JoinNode`/`JoinKind`
are also referenced — same advice.)

- [ ] **Step 3: Run + verify**

```
cargo test --lib -- sql::optimizer::rbo::rules::aggregate_pushdown::collector 2>&1 | tail -10
```

Expected: all rejection tests PASS (collect_push_plan returns None
for non-Join inputs). Total tests at this point: 12 (9 from Task 4
+ 3 new).

- [ ] **Step 4: Commit**

```bash
cargo fmt
git add src/sql/optimizer/rbo/rules/aggregate_pushdown/collector.rs
git commit -m "feat(aggpd): collector accepts only direct-Join input in v1

aggregate.input must be LogicalPlan::Join directly. Scan-only inputs
(no benefit), Filter/Project intermediation, and nested joins are all
rejected. Filter/Project + multi-table joins are OPT-1 follow-ups.
split_at_join logic for choosing which side to push lands in Task 6."
```

---

## Task 6: Collector — `split_at_join`

**Goal:** Push aggregates through Inner / LeftOuter / RightOuter joins
in the v1 scope (`aggregate.input` is the Join; both sides are Scans).
Enforce all rejection criteria in §4.2 of the spec.

**Files:**
- Modify: `src/sql/optimizer/rbo/rules/aggregate_pushdown/collector.rs`

(The `Side` enum and `PushPlan.side` field were already defined in
Task 4's `context.rs`.)

- [ ] **Step 1: Add `split_at_join` and finalize `collect_push_plan`**

Replace the body of `collect_push_plan` in `collector.rs` and add the
`split_at_join` helper + helpers it needs:

```rust
pub(crate) fn collect_push_plan(
    aggregate: &AggregateNode,
    _table_stats: &HashMap<String, TableStatistics>,
) -> Option<PushPlan> {
    let ctx = entry_safety_check(aggregate)?;
    let join = match aggregate.input.as_ref() {
        LogicalPlan::Join(j) => j,
        _ => return None,
    };
    split_at_join(join, ctx)
}

fn split_at_join(
    join: &crate::sql::planner::plan::JoinNode,
    ctx: AggregatePushDownContext,
) -> Option<PushPlan> {
    use crate::sql::analysis::JoinKind;

    // Step 1: join-shape filter.
    match join.join_type {
        JoinKind::Inner | JoinKind::LeftOuter | JoinKind::RightOuter => {}
        _ => return None,
    }
    let cond = join.condition.as_ref()?;
    let equi_keys = extract_equi_key_pairs(cond);
    if equi_keys.is_empty() {
        return None;
    }

    // Step 2: per-side column visibility.
    let left_cols = collect_output_column_names(&join.left);
    let right_cols = collect_output_column_names(&join.right);

    let side = if ctx
        .required_columns
        .iter()
        .all(|c| left_cols.contains(c))
    {
        Side::Left
    } else if ctx
        .required_columns
        .iter()
        .all(|c| right_cols.contains(c))
    {
        Side::Right
    } else {
        return None;
    };

    // Step 3: outer-join amplifier rejection.
    match (join.join_type, side) {
        (JoinKind::RightOuter, Side::Left) => return None,
        (JoinKind::LeftOuter, Side::Right) => return None,
        _ => {}
    }

    // Step 4: chosen-side subtree MUST be a Scan in v1 (no nested joins,
    // no intermediate Filter/Project on the side).
    let side_subtree = match side {
        Side::Left => &join.left,
        Side::Right => &join.right,
    };
    if !matches!(side_subtree.as_ref(), LogicalPlan::Scan(_)) {
        return None;
    }
    let side_cols = match side {
        Side::Left => &left_cols,
        Side::Right => &right_cols,
    };

    // Step 5: partial group-by = original group-by cols on this side
    //         + side-bound equi-keys.
    let mut partial_groupby: Vec<TypedExpr> = ctx
        .original_groupby
        .iter()
        .filter(|gb| match &gb.kind {
            ExprKind::ColumnRef { name, .. } => side_cols.contains(name),
            _ => false,
        })
        .cloned()
        .collect();
    for (left_key, right_key) in &equi_keys {
        let candidate = match side {
            Side::Left => left_key,
            Side::Right => right_key,
        };
        let already = partial_groupby.iter().any(|gb| match (&gb.kind, &candidate.kind) {
            (
                ExprKind::ColumnRef { name: a, .. },
                ExprKind::ColumnRef { name: b, .. },
            ) => a == b,
            _ => false,
        });
        if !already {
            partial_groupby.push(candidate.clone());
        }
    }

    Some(PushPlan {
        side,
        target_subtree: (**side_subtree).clone(),
        partial_groupby,
        partial_aggregates: ctx.original_aggregates,
    })
}

fn extract_equi_key_pairs(cond: &TypedExpr) -> Vec<(TypedExpr, TypedExpr)> {
    let mut out = Vec::new();
    walk_and_collect_equi(cond, &mut out);
    out
}

fn walk_and_collect_equi(expr: &TypedExpr, out: &mut Vec<(TypedExpr, TypedExpr)>) {
    use crate::sql::analysis::BinOp;
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            if matches!(left.kind, ExprKind::ColumnRef { .. })
                && matches!(right.kind, ExprKind::ColumnRef { .. })
            {
                out.push(((**left).clone(), (**right).clone()));
            }
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            walk_and_collect_equi(left, out);
            walk_and_collect_equi(right, out);
        }
        _ => {}
    }
}

fn collect_output_column_names(plan: &LogicalPlan) -> Vec<String> {
    use crate::sql::planner::plan::*;
    match plan {
        LogicalPlan::Scan(s) => s.columns.iter().map(|c| c.name.clone()).collect(),
        LogicalPlan::Filter(f) => collect_output_column_names(&f.input),
        LogicalPlan::Project(p) => p.items.iter().map(|i| i.output_name.clone()).collect(),
        LogicalPlan::Join(j) => {
            let mut l = collect_output_column_names(&j.left);
            l.extend(collect_output_column_names(&j.right));
            l
        }
        LogicalPlan::Aggregate(a) => a.output_columns.iter().map(|c| c.name.clone()).collect(),
        _ => Vec::new(),
    }
}
```

Update the existing import line at the top of `collector.rs` to
include `Side`:

```rust
// Before:
use super::context::{AggregatePushDownContext, PushPlan};
// After:
use super::context::{AggregatePushDownContext, PushPlan, Side};
```

(`JoinKind`, `BinOp::Eq`, `BinOp::And`, `JoinNode` field names — verify
against `src/sql/analysis/mod.rs` and `src/sql/planner/plan.rs`. Adjust
if any don't match.)

- [ ] **Step 2: Add positive join tests**

Append to the `tests` module:

```rust
    use crate::sql::analysis::{BinOp, JoinKind};
    use crate::sql::planner::plan::JoinNode;

    fn eq(a: &str, b: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(a, DataType::Int64)),
                op: BinOp::Eq,
                right: Box::new(col_ref(b, DataType::Int64)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    #[test]
    fn pushes_sum_under_inner_join_to_left() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::Inner,
            condition: Some(eq("k", "k")),
        });
        let agg = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        let plan = collect_push_plan(&agg, &HashMap::new()).expect("should push to left");
        assert_eq!(plan.side, super::super::context::Side::Left);
        assert!(matches!(plan.target_subtree, LogicalPlan::Scan(_)));
    }

    #[test]
    fn rejects_nested_join_on_target_side() {
        // v1 only handles direct-Scan sides. A nested join on the
        // chosen side must be rejected; multi-table is OPT-1 follow-up.
        let inner_join = LogicalPlan::Join(JoinNode {
            left: Box::new(dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)])),
            right: Box::new(dummy_scan_with_cols(&[("k", DataType::Int64)])),
            join_type: JoinKind::Inner,
            condition: Some(eq("k", "k")),
        });
        let outer_join = LogicalPlan::Join(JoinNode {
            left: Box::new(inner_join),
            right: Box::new(dummy_scan_with_cols(&[("k", DataType::Int64)])),
            join_type: JoinKind::Inner,
            condition: Some(eq("k", "k")),
        });
        let agg = AggregateNode {
            input: Box::new(outer_join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }

    #[test]
    fn rejects_outer_join_amplifier_side() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        // LEFT OUTER JOIN; aggregate on right (amplifier) — must reject.
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::LeftOuter,
            condition: Some(eq("k", "k")),
        });
        let agg = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }

    #[test]
    fn accepts_left_outer_when_agg_on_preserved_left() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::LeftOuter,
            condition: Some(eq("k", "k")),
        });
        let agg = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        let plan = collect_push_plan(&agg, &HashMap::new()).expect("push to preserved left");
        assert!(matches!(plan.target_subtree, LogicalPlan::Scan(_)));
    }

    #[test]
    fn rejects_cross_join() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("x", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::Cross,
            condition: None,
        });
        let agg = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }

    #[test]
    fn rejects_aggregate_columns_across_sides() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("k", DataType::Int64), ("w", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::Inner,
            condition: Some(eq("k", "k")),
        });
        // sum(v) is on left, but group-by k is on both (and we'll add a
        // cross-side aggregate sum(w)). Wait — required = {k, v, w}; k
        // exists on both sides, v on left only, w on right only. Required
        // is NOT subset of either side → reject.
        let agg = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v"), sum_call("w")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }

    #[test]
    fn rejects_semi_anti_join() {
        let a = dummy_scan_with_cols(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = dummy_scan_with_cols(&[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::LeftSemi,
            condition: Some(eq("k", "k")),
        });
        let agg = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![sum_call("v")],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(collect_push_plan(&agg, &HashMap::new()).is_none());
    }
```

- [ ] **Step 3: Run + verify**

```
cargo test --lib -- sql::optimizer::rbo::rules::aggregate_pushdown::collector 2>&1 | tail -15
```

Expected: all PASS (19 tests total).

- [ ] **Step 4: Commit**

```bash
cargo fmt
git add src/sql/optimizer/rbo/rules/aggregate_pushdown/collector.rs
git commit -m "feat(aggpd): collector handles inner/outer joins

split_at_join enforces join-shape filters (no cross/non-equi/semi/anti),
rejects outer-join amplifier side, derives partial group-by from
original group-by + side-bound equi keys, and recurses to find the
deepest valid push target. Aggregates whose required columns span both
sides are rejected as one-shot non-pushable."
```

---

## Task 7: Rewriter — splice + final-call rewrite

**Goal:** Given a `PushPlan` and the original `AggregateNode`, produce
a new `LogicalPlan` with the partial aggregate spliced in. The top
aggregate stays at its original position with its function calls
rewritten and `already_pushed = true`.

**Files:**
- Modify: `src/sql/optimizer/rbo/rules/aggregate_pushdown/rewriter.rs`
- Modify: `src/sql/optimizer/rbo/rules/aggregate_pushdown/rule.rs`

- [ ] **Step 1: Implement the rewriter**

Replace `rewriter.rs` with:

```rust
//! Aggregate pushdown rewriter — phase 2 of the rule.

use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
use crate::sql::planner::plan::{AggregateCall, AggregateNode, LogicalPlan, SortItem};

use super::context::PushPlan;

const PARTIAL_OUTPUT_PREFIX: &str = "__nr_agg_pd_";

/// Construct the final LogicalPlan: a top-level Aggregate (with
/// already_pushed=true) whose input is the original subtree with a
/// partial Aggregate spliced in at the push target.
pub(crate) fn rewrite(
    original: &AggregateNode,
    plan: PushPlan,
) -> LogicalPlan {
    // Capture the side before plan is consumed by the moves below.
    let plan_side = plan.side;

    // 1. Build partial AggregateCalls. For SUM/MIN/MAX function name is
    //    unchanged at the partial stage; for COUNT it stays COUNT at
    //    partial and becomes SUM at final.
    let partial_calls: Vec<AggregateCall> = plan
        .partial_aggregates
        .iter()
        .map(|c| AggregateCall {
            name: partial_fn_name(&c.name),
            args: c.args.clone(),
            distinct: false,
            result_type: c.result_type.clone(),
            order_by: vec![],
        })
        .collect();

    // 2. Synthetic output columns for each partial call.
    let partial_output_cols: Vec<OutputColumn> = partial_calls
        .iter()
        .enumerate()
        .map(|(i, call)| OutputColumn {
            name: format!("{}{}", PARTIAL_OUTPUT_PREFIX, i),
            data_type: call.result_type.clone(),
            nullable: true,
        })
        .collect();

    // 3. Partial group-by output columns (just project the group-by
    //    column refs through unchanged).
    let partial_groupby_outputs: Vec<OutputColumn> = plan
        .partial_groupby
        .iter()
        .filter_map(|gb| match &gb.kind {
            ExprKind::ColumnRef { name, .. } => Some(OutputColumn {
                name: name.clone(),
                data_type: gb.data_type.clone(),
                nullable: gb.nullable,
            }),
            _ => None,
        })
        .collect();

    let mut partial_outputs = partial_groupby_outputs.clone();
    partial_outputs.extend(partial_output_cols.clone());

    let partial_aggregate = AggregateNode {
        input: Box::new(plan.target_subtree),
        group_by: plan.partial_groupby,
        aggregates: partial_calls,
        output_columns: partial_outputs,
        already_pushed: false, // partial isn't itself a final
    };

    // 4. Splice partial into the chosen side of the join. v1 invariant
    //    (enforced by the collector): original.input is a Join, and
    //    PushPlan.side identifies which side gets wrapped.
    let new_input = {
        let mut join = match (*original.input).clone() {
            LogicalPlan::Join(j) => j,
            _ => unreachable!("collector guarantees original.input is a Join"),
        };
        let wrapped = Box::new(LogicalPlan::Aggregate(partial_aggregate));
        match plan_side {
            super::context::Side::Left => join.left = wrapped,
            super::context::Side::Right => join.right = wrapped,
        }
        LogicalPlan::Join(join)
    };

    // 5. Rewrite top-level aggregate calls to reference partial outputs.
    let final_aggs: Vec<AggregateCall> = original
        .aggregates
        .iter()
        .zip(partial_output_cols.iter())
        .map(|(orig, pc)| AggregateCall {
            name: final_fn_name(&orig.name),
            args: vec![TypedExpr {
                kind: ExprKind::ColumnRef {
                    name: pc.name.clone(),
                    table_alias: None,
                },
                data_type: pc.data_type.clone(),
                nullable: pc.nullable,
            }],
            distinct: false,
            result_type: orig.result_type.clone(),
            order_by: orig.order_by.clone(),
        })
        .collect();

    LogicalPlan::Aggregate(AggregateNode {
        input: Box::new(new_input),
        group_by: original.group_by.clone(),
        aggregates: final_aggs,
        output_columns: original.output_columns.clone(),
        already_pushed: true,
    })
}

fn partial_fn_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn final_fn_name(name: &str) -> String {
    match name.to_ascii_lowercase().as_str() {
        "count" => "sum".to_string(),
        other => other.to_string(),
    }
}

fn collect_output_column_names(plan: &LogicalPlan) -> Vec<String> {
    use crate::sql::planner::plan::*;
    match plan {
        LogicalPlan::Scan(s) => s.columns.iter().map(|c| c.name.clone()).collect(),
        LogicalPlan::Filter(f) => collect_output_column_names(&f.input),
        LogicalPlan::Project(p) => p.items.iter().map(|i| i.output_name.clone()).collect(),
        LogicalPlan::Join(j) => {
            let mut l = collect_output_column_names(&j.left);
            l.extend(collect_output_column_names(&j.right));
            l
        }
        LogicalPlan::Aggregate(a) => a.output_columns.iter().map(|c| c.name.clone()).collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::planner::plan::*;
    use arrow::datatypes::DataType;

    fn col_ref(name: &str, ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                name: name.into(),
                table_alias: None,
            },
            data_type: ty,
            nullable: true,
        }
    }

    fn scan(name: &str, cols: &[(&str, DataType)]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: name.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                iceberg_table: None,
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: cols
                .iter()
                .map(|(n, ty)| OutputColumn {
                    name: (*n).into(),
                    data_type: ty.clone(),
                    nullable: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
        })
    }

    #[test]
    fn rewrites_count_to_sum_at_final() {
        let a = scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan("b", &[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: crate::sql::analysis::JoinKind::Inner,
            condition: Some(TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(col_ref("k", DataType::Int64)),
                    op: crate::sql::analysis::BinOp::Eq,
                    right: Box::new(col_ref("k", DataType::Int64)),
                },
                data_type: DataType::Boolean,
                nullable: false,
            }),
        });
        let count_call = AggregateCall {
            name: "count".into(),
            args: vec![col_ref("v", DataType::Int64)],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
        };
        let original = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![count_call],
            output_columns: vec![OutputColumn {
                name: "k".into(),
                data_type: DataType::Int64,
                nullable: true,
            }],
            already_pushed: false,
        };
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]),
            partial_groupby: vec![col_ref("k", DataType::Int64)],
            partial_aggregates: original.aggregates.clone(),
        };
        let out = rewrite(&original, push);
        let LogicalPlan::Aggregate(top) = out else {
            panic!("top must be Aggregate")
        };
        assert!(top.already_pushed);
        assert_eq!(top.aggregates[0].name, "sum");
        // Partial call should still be "count".
        let LogicalPlan::Join(j) = *top.input else {
            panic!("input must be Join")
        };
        let LogicalPlan::Aggregate(partial) = *j.left else {
            panic!("partial on left")
        };
        assert!(!partial.already_pushed);
        assert_eq!(partial.aggregates[0].name, "count");
    }

    #[test]
    fn rewrites_sum_stays_sum() {
        let a = scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan("b", &[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: crate::sql::analysis::JoinKind::Inner,
            condition: Some(TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(col_ref("k", DataType::Int64)),
                    op: crate::sql::analysis::BinOp::Eq,
                    right: Box::new(col_ref("k", DataType::Int64)),
                },
                data_type: DataType::Boolean,
                nullable: false,
            }),
        });
        let original = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![AggregateCall {
                name: "sum".into(),
                args: vec![col_ref("v", DataType::Int64)],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![],
            already_pushed: false,
        };
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]),
            partial_groupby: vec![col_ref("k", DataType::Int64)],
            partial_aggregates: original.aggregates.clone(),
        };
        let out = rewrite(&original, push);
        let LogicalPlan::Aggregate(top) = out else {
            panic!()
        };
        assert_eq!(top.aggregates[0].name, "sum");
        assert!(matches!(top.aggregates[0].args[0].kind,
            ExprKind::ColumnRef { ref name, .. } if name.starts_with("__nr_agg_pd_")));
    }

    #[test]
    fn rewriter_output_preserves_top_output_columns() {
        let a = scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan("b", &[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a.clone()),
            right: Box::new(b),
            join_type: crate::sql::analysis::JoinKind::Inner,
            condition: Some(TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(col_ref("k", DataType::Int64)),
                    op: crate::sql::analysis::BinOp::Eq,
                    right: Box::new(col_ref("k", DataType::Int64)),
                },
                data_type: DataType::Boolean,
                nullable: false,
            }),
        });
        let original = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![AggregateCall {
                name: "sum".into(),
                args: vec![col_ref("v", DataType::Int64)],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![
                OutputColumn {
                    name: "k".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
                OutputColumn {
                    name: "total".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
            ],
            already_pushed: false,
        };
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: a,
            partial_groupby: original.group_by.clone(),
            partial_aggregates: original.aggregates.clone(),
        };
        let out = rewrite(&original, push);
        let LogicalPlan::Aggregate(top) = out else {
            panic!()
        };
        assert_eq!(top.output_columns.len(), 2);
        assert_eq!(top.output_columns[0].name, "k");
        assert_eq!(top.output_columns[1].name, "total");
    }
}
```

- [ ] **Step 2: Wire `rewrite` into the rule**

Edit `rule.rs` so `apply` calls collector + rewriter:

```rust
    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let agg = match &plan {
            LogicalPlan::Aggregate(a) => a,
            _ => return None,
        };
        let push = super::collector::collect_push_plan(agg, &self.table_stats)?;
        Some(super::rewriter::rewrite(agg, push))
    }
```

- [ ] **Step 3: Run unit tests**

```
cargo test --lib -- sql::optimizer::rbo::rules::aggregate_pushdown:: 2>&1 | tail -10
```

Expected: PASS (all collector + rewriter tests).

- [ ] **Step 4: Run all library tests for regression**

```
cargo test --lib 2>&1 | tail -3
```

Expected: PASS, 0 failures. If any pre-existing test breaks because a
plan now contains a pushed aggregate, inspect and decide:
- If the change is semantically correct, update the affected test
  golden/expectation.
- If it's a real bug, surface immediately.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add src/sql/optimizer/rbo/rules/aggregate_pushdown/
git commit -m "feat(aggpd): rewriter splices partial + rewrites final

For SUM/MIN/MAX the partial and final use the same function name; for
COUNT(x) the partial stays COUNT but the final becomes SUM. Synthetic
output columns are named __nr_agg_pd_<i>. The top-level Aggregate's
output_columns are preserved exactly so upstream operators are
unchanged. already_pushed=true is set on the top-level result for
idempotency."
```

---

## Task 8: Cost gate

**Goal:** NDV bucketing + row-count threshold. Reject pushes that won't
reduce rows.

**Files:**
- Modify: `src/sql/optimizer/rbo/rules/aggregate_pushdown/cost.rs`
- Modify: `src/sql/optimizer/rbo/rules/aggregate_pushdown/rule.rs`

- [ ] **Step 1: Implement cost.rs**

Replace `cost.rs` with:

```rust
//! Aggregate pushdown cost gate — NDV bucketing + row-count threshold.

use std::collections::HashMap;

use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::optimizer::rbo::rules::join_reorder::cardinality::estimate_statistics;
use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::planner::plan::LogicalPlan;

use super::context::PushPlan;

const MIN_PARTIAL_BENEFIT_RATIO: f64 = 0.5;
const UNKNOWN_NDV_ROW_THRESHOLD: f64 = 10_000.0;

/// True iff pushing the partial aggregate is expected to reduce rows.
pub(crate) fn should_push(
    plan: &PushPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> bool {
    let stats = estimate_statistics(&plan.target_subtree, table_stats);
    let row_count = stats.output_row_count;
    if row_count <= 1.0 {
        // Trivially small subtree; partial buys nothing.
        return false;
    }

    let ndvs: Vec<Option<f64>> = plan
        .partial_groupby
        .iter()
        .map(|gb| match &gb.kind {
            ExprKind::ColumnRef { name, .. } => stats
                .column_statistics
                .get(name)
                .map(|cs| cs.distinct_values_count)
                .filter(|n| n.is_finite() && *n > 0.0),
            _ => None,
        })
        .collect();

    if ndvs.iter().any(|n| n.is_none()) {
        // Fallback: push only if the target is "big enough".
        return row_count > UNKNOWN_NDV_ROW_THRESHOLD;
    }

    let joint_ndv: f64 = ndvs.iter().flatten().product::<f64>().min(row_count);
    joint_ndv < row_count * MIN_PARTIAL_BENEFIT_RATIO
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::optimizer::statistics::ColumnStatistic;
    use crate::sql::planner::plan::ScanNode;
    use arrow::datatypes::DataType;

    fn col_ref(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                name: name.into(),
                table_alias: None,
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn scan_with_stats(table: &str, row_count: u64, col: &str, ndv: f64)
        -> (LogicalPlan, HashMap<String, TableStatistics>)
    {
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: table.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                iceberg_table: None,
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                name: col.into(),
                data_type: DataType::Int64,
                nullable: false,
            }],
            predicates: vec![],
            required_columns: None,
        });
        let mut col_stats = HashMap::new();
        col_stats.insert(
            col.to_string(),
            ColumnStatistic {
                min: None,
                max: None,
                null_count: 0.0,
                distinct_values_count: ndv,
            },
        );
        let mut table_stats = HashMap::new();
        table_stats.insert(
            format!("db.{table}"),
            TableStatistics {
                row_count,
                column_statistics: col_stats,
            },
        );
        (scan, table_stats)
    }

    #[test]
    fn low_cardinality_pushes() {
        let (scan, stats) = scan_with_stats("t", 10_000, "k", 10.0);
        let plan = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan,
            partial_groupby: vec![col_ref("k")],
            partial_aggregates: vec![],
        };
        assert!(should_push(&plan, &stats));
    }

    #[test]
    fn high_cardinality_rejects() {
        let (scan, stats) = scan_with_stats("t", 10_000, "k", 10_000.0);
        let plan = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan,
            partial_groupby: vec![col_ref("k")],
            partial_aggregates: vec![],
        };
        assert!(!should_push(&plan, &stats));
    }

    #[test]
    fn unknown_ndv_pushes_above_threshold() {
        let (scan, stats) = scan_with_stats("t", 20_000, "k", f64::NAN);
        let plan = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan,
            partial_groupby: vec![col_ref("k")],
            partial_aggregates: vec![],
        };
        assert!(should_push(&plan, &stats));
    }

    #[test]
    fn unknown_ndv_rejects_below_threshold() {
        let (scan, stats) = scan_with_stats("t", 500, "k", f64::NAN);
        let plan = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan,
            partial_groupby: vec![col_ref("k")],
            partial_aggregates: vec![],
        };
        assert!(!should_push(&plan, &stats));
    }
}
```

(Verify `TableStatistics`, `ColumnStatistic` field names against
`src/sql/optimizer/statistics.rs` — adjust if `min`, `max`,
`null_count` are named differently.)

- [ ] **Step 2: Wire the cost gate into the rule**

Edit `rule.rs`:

```rust
    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let agg = match &plan {
            LogicalPlan::Aggregate(a) => a,
            _ => return None,
        };
        let push = super::collector::collect_push_plan(agg, &self.table_stats)?;
        if !super::cost::should_push(&push, &self.table_stats) {
            return None;
        }
        Some(super::rewriter::rewrite(agg, push))
    }
```

- [ ] **Step 3: Run unit tests**

```
cargo test --lib -- sql::optimizer::rbo::rules::aggregate_pushdown:: 2>&1 | tail -10
```

Expected: PASS (all collector + rewriter + cost tests).

- [ ] **Step 4: Run full library test sweep**

```
cargo test --lib 2>&1 | tail -3
```

Expected: PASS, 0 failures.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add src/sql/optimizer/rbo/rules/aggregate_pushdown/cost.rs \
        src/sql/optimizer/rbo/rules/aggregate_pushdown/rule.rs
git commit -m "feat(aggpd): cost gate via NDV bucketing

Pushes aggregate iff joint NDV across partial group-by columns is
expected to reduce rows by at least MIN_PARTIAL_BENEFIT_RATIO (0.5).
Falls back to row-count threshold (10000) when any column NDV is
unknown. Uses join_reorder::cardinality::estimate_statistics on the
target subtree for row_count + NDV lookup."
```

---

## Task 9: Idempotency guard end-to-end

**Goal:** When the rule fires once, the fixed-point driver re-applies
it on the rewriter's output. The collector must reject the
`already_pushed = true` top aggregate.

This is already wired in Task 4 (`entry_safety_check` returns None
when `already_pushed = true`) and Task 7 (rewriter sets the flag).
This task adds an explicit end-to-end test.

**Files:**
- Modify: `src/sql/optimizer/rbo/rules/aggregate_pushdown/rule.rs`

- [ ] **Step 1: Add an idempotency test**

Append to the `tests` module in `rule.rs`:

```rust
    #[test]
    fn idempotent_does_not_repush_already_pushed_plan() {
        use crate::sql::analysis::{BinOp, JoinKind};
        use crate::sql::catalog::{TableDef, TableStorage};
        use crate::sql::planner::plan::{JoinNode, ScanNode};

        fn col(name: &str) -> TypedExpr {
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    name: name.into(),
                    table_alias: None,
                },
                data_type: arrow::datatypes::DataType::Int64,
                nullable: true,
            }
        }

        fn scan(name: &str, cols: &[&str]) -> LogicalPlan {
            LogicalPlan::Scan(ScanNode {
                database: "db".into(),
                table: TableDef {
                    name: name.into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: None,
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/t.parquet"),
                    },
                },
                alias: None,
                columns: cols
                    .iter()
                    .map(|n| OutputColumn {
                        name: (*n).into(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                    })
                    .collect(),
                predicates: vec![],
                required_columns: None,
            })
        }

        // Build a plan with already_pushed = true. The rule must reject.
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(LogicalPlan::Join(JoinNode {
                left: Box::new(scan("a", &["k", "v"])),
                right: Box::new(scan("b", &["k"])),
                join_type: JoinKind::Inner,
                condition: Some(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col("k")),
                        op: BinOp::Eq,
                        right: Box::new(col("k")),
                    },
                    data_type: arrow::datatypes::DataType::Boolean,
                    nullable: false,
                }),
            })),
            group_by: vec![col("k")],
            aggregates: vec![AggregateCall {
                name: "sum".into(),
                args: vec![col("v")],
                distinct: false,
                result_type: arrow::datatypes::DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![],
            already_pushed: true, // <- key invariant
        });

        let rule = AggregatePushdownRule::new(Arc::new(HashMap::new()));
        assert!(rule.apply(plan).is_none(), "must not re-fire on already_pushed");
    }
```

(Import `AggregateCall`, `AggregateNode`, `LogicalPlan`, `TypedExpr`,
`ExprKind`, `OutputColumn` at the top of the test module as needed.)

- [ ] **Step 2: Run + commit**

```
cargo test --lib -- sql::optimizer::rbo::rules::aggregate_pushdown::rule 2>&1 | tail -5
cargo fmt
git add src/sql/optimizer/rbo/rules/aggregate_pushdown/rule.rs
git commit -m "test(aggpd): explicit idempotency test for already_pushed guard

Verifies the rule short-circuits at the entry safety check when given
an AggregateNode the rewriter already produced."
```

---

## Task 10: SQL plan-golden + regression + docs

**Goal:** Land four SQL cases under `sql-tests/optimizer/`, run a full
regression sweep, update `AGENTS.md` / `CLAUDE.md`.

**Files:**
- Create: `sql-tests/optimizer/sql/aggregate_pushdown_inner_join.sql`
- Create: `sql-tests/optimizer/sql/aggregate_pushdown_count_star_rejected.sql`
- Create: `sql-tests/optimizer/sql/aggregate_pushdown_left_outer_preserved.sql`
- Create: `sql-tests/optimizer/sql/aggregate_pushdown_disabled.sql`
- Create: matching `.result` files (recorded via `--mode record`)
- Modify: `AGENTS.md` §9

- [ ] **Step 1: Build debug binary + start server**

```bash
cargo build
source docker/iceberg-rest/runtime/current/env.sh
lsof -i :$NOVA_ENV_MYSQL_PORT -t | xargs -r kill 2>/dev/null
sleep 2

LOG=/tmp/novarocks-opt1.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then echo "died"; tail -20 "$LOG"; exit 1; fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timeout"; kill -9 "$SRV_PID"; exit 1; }
```

- [ ] **Step 2: Write the four SQL cases**

`sql-tests/optimizer/sql/aggregate_pushdown_inner_join.sql`:

```sql
-- @tags=optimizer,aggregate_pushdown
-- Test Objective:
-- 1. Verify the EXPLAIN VERBOSE plan shows a partial AGGREGATE under
--    the join and a final AGGREGATE on top.
-- 2. Future PRs touching aggregate pushdown must intentionally re-record.
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_a;
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_b;
CREATE TABLE ${case_db}.t_agg_pd_a (k INT, v INT);
CREATE TABLE ${case_db}.t_agg_pd_b (k INT);
INSERT INTO ${case_db}.t_agg_pd_a VALUES (1, 10), (2, 20), (1, 30);
INSERT INTO ${case_db}.t_agg_pd_b VALUES (1), (2);
EXPLAIN VERBOSE
SELECT a.k, SUM(a.v)
FROM ${case_db}.t_agg_pd_a a
INNER JOIN ${case_db}.t_agg_pd_b b ON a.k = b.k
GROUP BY a.k;
```

`sql-tests/optimizer/sql/aggregate_pushdown_count_star_rejected.sql`:

```sql
-- @tags=optimizer,aggregate_pushdown,negative
-- Test Objective:
-- COUNT(*) must NOT be pushed; the plan should contain a single
-- top-level AGGREGATE.
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_neg_a;
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_neg_b;
CREATE TABLE ${case_db}.t_agg_pd_neg_a (k INT);
CREATE TABLE ${case_db}.t_agg_pd_neg_b (k INT);
INSERT INTO ${case_db}.t_agg_pd_neg_a VALUES (1), (2);
INSERT INTO ${case_db}.t_agg_pd_neg_b VALUES (1);
EXPLAIN VERBOSE
SELECT COUNT(*)
FROM ${case_db}.t_agg_pd_neg_a a
INNER JOIN ${case_db}.t_agg_pd_neg_b b ON a.k = b.k;
```

`sql-tests/optimizer/sql/aggregate_pushdown_left_outer_preserved.sql`:

```sql
-- @tags=optimizer,aggregate_pushdown,outer
-- Test Objective:
-- LEFT OUTER JOIN with aggregate on the preserved (left) side: the
-- rule must still push to the left side.
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_lo_a;
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_lo_b;
CREATE TABLE ${case_db}.t_agg_pd_lo_a (k INT, v INT);
CREATE TABLE ${case_db}.t_agg_pd_lo_b (k INT);
INSERT INTO ${case_db}.t_agg_pd_lo_a VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.t_agg_pd_lo_b VALUES (1);
EXPLAIN VERBOSE
SELECT a.k, SUM(a.v)
FROM ${case_db}.t_agg_pd_lo_a a
LEFT OUTER JOIN ${case_db}.t_agg_pd_lo_b b ON a.k = b.k
GROUP BY a.k;
```

`sql-tests/optimizer/sql/aggregate_pushdown_disabled.sql`:

```sql
-- @tags=optimizer,aggregate_pushdown,session_rule_disable
-- Test Objective:
-- Verify SET disable_optimizer_rules = 'AggregatePushdown' suppresses
-- the rewrite. Two EXPLAIN VERBOSE outputs around the SET must differ.
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_dis_a;
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_dis_b;
CREATE TABLE ${case_db}.t_agg_pd_dis_a (k INT, v INT);
CREATE TABLE ${case_db}.t_agg_pd_dis_b (k INT);
INSERT INTO ${case_db}.t_agg_pd_dis_a VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.t_agg_pd_dis_b VALUES (1);

EXPLAIN VERBOSE
SELECT a.k, SUM(a.v)
FROM ${case_db}.t_agg_pd_dis_a a
INNER JOIN ${case_db}.t_agg_pd_dis_b b ON a.k = b.k
GROUP BY a.k;

SET disable_optimizer_rules = 'AggregatePushdown';

EXPLAIN VERBOSE
SELECT a.k, SUM(a.v)
FROM ${case_db}.t_agg_pd_dis_a a
INNER JOIN ${case_db}.t_agg_pd_dis_b b ON a.k = b.k
GROUP BY a.k;

SET disable_optimizer_rules = '';
```

- [ ] **Step 3: Record `.result` files**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only \
    aggregate_pushdown_inner_join,aggregate_pushdown_count_star_rejected,aggregate_pushdown_left_outer_preserved,aggregate_pushdown_disabled \
  --mode record
```

Manually inspect each generated `.result`:

- `aggregate_pushdown_inner_join.result`: must contain two AGGREGATE
  nodes (a partial under the join, a final on top).
- `aggregate_pushdown_count_star_rejected.result`: must contain only
  one AGGREGATE node (no partial).
- `aggregate_pushdown_left_outer_preserved.result`: must contain a
  partial AGGREGATE under the LEFT OUTER JOIN's left side.
- `aggregate_pushdown_disabled.result`: the two EXPLAIN outputs must
  differ — one has the partial, the other doesn't.

If any case looks wrong, debug **before** committing the goldens.

- [ ] **Step 4: Verify mode locks the goldens**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode verify
```

Expected: all optimizer suite cases PASS (existing 3 from OPT-5 + 4
new).

- [ ] **Step 5: Run the analyze-statistics suite**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite analyze-statistics --mode verify 2>&1 | tail -20
```

If it passes: great, OPT-1 acceptance criterion met.

If cases fail with **result drift** (wrong rows), that's a real bug —
surface it and pause.

If cases fail with **infrastructure errors** (missing tables, env
problems): note as pre-existing, don't block PR.

- [ ] **Step 6: Spot-check tpc-h for cross-suite drift**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-h --mode verify 2>&1 | tail -20
```

Same rule: result drift = real bug; init.sql failures = pre-existing.

- [ ] **Step 7: Stop the server**

```bash
kill -INT "$SRV_PID" 2>/dev/null
wait "$SRV_PID" 2>/dev/null || true
```

- [ ] **Step 8: Update CLAUDE.md / AGENTS.md**

Append a new bullet at the end of `AGENTS.md` §9:

```markdown
- **Aggregate pushdown rule (OPT-1)**: see
  `src/sql/optimizer/rbo/rules/aggregate_pushdown/`. Pushes
  `LogicalAggregate` past inner/outer joins toward leaves when NDV
  bucketing predicts a real row-count reduction. White-list functions
  are SUM/MIN/MAX/COUNT(col). Disable via
  `SET disable_optimizer_rules = 'AggregatePushdown'`. Plan-shape
  cases live under `sql-tests/optimizer/aggregate_pushdown_*.sql`. The
  idempotency guard is `AggregateNode::already_pushed` —
  other rules must preserve the flag when cloning.
```

- [ ] **Step 9: Commit suite + docs**

```bash
git add sql-tests/optimizer/sql/aggregate_pushdown_*.sql \
        sql-tests/optimizer/result/aggregate_pushdown_*.result \
        AGENTS.md
git commit -m "test: add aggregate-pushdown plan-golden cases + docs

Four cases under sql-tests/optimizer/:
- aggregate_pushdown_inner_join (positive)
- aggregate_pushdown_count_star_rejected (negative)
- aggregate_pushdown_left_outer_preserved (positive, outer)
- aggregate_pushdown_disabled (verifies SET disable_optimizer_rules)

AGENTS.md §9 gains a bullet pointing at the new rule, the
already_pushed flag, and the disable knob."
```

---

## Verification Checklist (run before PR)

- [ ] `cargo build --lib` clean.
- [ ] `cargo clippy --lib` — no new warnings on touched files.
- [ ] `cargo test --lib` passes (~2300+ tests after OPT-1 lands).
- [ ] `--suite optimizer --mode verify` passes (3 OPT-5 + 4 OPT-1 = 7).
- [ ] `--suite filter --only filter_basic_comparison --mode verify` still passes.
- [ ] `--suite analyze-statistics --mode verify` no NEW result drift.
- [ ] `--suite tpc-h --mode verify` no NEW result drift.
- [ ] Manual smoke: `EXPLAIN VERBOSE SELECT a.k, SUM(a.v) FROM ... INNER JOIN ... GROUP BY a.k;` shows two AGGREGATE nodes.
- [ ] Manual smoke: `SET disable_optimizer_rules = 'AggregatePushdown';` followed by the same query shows only one AGGREGATE node.
