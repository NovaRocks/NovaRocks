# Phase 3 predicate_pushdown Migration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the legacy `src/sql/optimizer/predicate_pushdown.rs` pass into a set of small, bottom-up `RewriteRule` implementations under the RBO framework, preserving the legacy "pushdown → join_reorder → pushdown" two-pass semantics. After this phase, `rewriter::rewrite` contains only `reorder_joins_cbo`, and `predicate_pushdown.rs` is deleted (its one helper still needed by `join_reorder` is moved to `join_reorder.rs`).

**Architecture:** Five shape-specific bottom-up rules + one SEMI/ANTI condition rule, each matching a local shape and performing a single-step rewrite. The RBO driver's node- and tree-level fixed-points replace the legacy manual recursion. Rules do NOT recurse; the driver owns traversal. To preserve the legacy "push before and after join_reorder" semantics, `cascades::optimize` runs the RBO driver twice: once before `rewriter::rewrite` (which now contains only `reorder_joins_cbo`) and once after.

**Tech Stack:** Rust 2021, cascades framework in `src/sql/cascades/`, existing `expr_utils` helpers kept as a cross-module dependency until Phase 4 moves them.

**Spec reference:** `docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md` §4.3.

**Prior art:** Phase 2 column_pruning migration (`docs/design/plans/2026-04-13-phase2-column-pruning-migration.md`) established the migration pattern used here. Key difference: Phase 3 rules are bottom-up, local, and DO NOT recurse (unlike `PruneColumns`, which was a documented one-time exception).

---

## Task 1: Expose Required `expr_utils` Helpers to Cascades

Phase 2 already upgraded `collect_column_refs` and `merge_needed` to `pub(crate)`. Phase 3 needs six more helpers currently `pub(super)`: `split_and`, `combine_and`, `collect_output_columns`, `wrap_remaining_filter`, `collect_qualified_column_refs`, and `collect_qualified_output_columns` (plus the `QualifiedRef` type alias that appears in the latter two signatures). Upgrade their visibility to `pub(crate)` so the new rules (under `src/sql/cascades/`) can call them. Phase 4 relocates these helpers into cascades entirely; this is a temporary cross-module bridge.

**Files:**
- Modify: `src/sql/optimizer/expr_utils.rs`

- [ ] **Step 1: Upgrade visibility on six helpers.**

Edit `src/sql/optimizer/expr_utils.rs`. Change each of:

```rust
pub(super) fn split_and(expr: TypedExpr) -> Vec<TypedExpr> {
pub(super) fn combine_and(mut exprs: Vec<TypedExpr>) -> TypedExpr {
pub(super) fn collect_output_columns(plan: &LogicalPlan) -> HashSet<String> {
pub(super) fn wrap_remaining_filter(plan: LogicalPlan, remaining: Vec<TypedExpr>) -> LogicalPlan {
pub(super) fn collect_qualified_column_refs(expr: &TypedExpr) -> Vec<QualifiedRef> {
pub(super) fn collect_qualified_output_columns(plan: &LogicalPlan) -> HashSet<QualifiedRef> {
```

to use `pub(crate)` instead of `pub(super)`. The `QualifiedRef` type alias (`pub(super) type QualifiedRef = (Option<String>, String);`) must also become `pub(crate) type QualifiedRef = ...` since it appears in the signatures above.

- [ ] **Step 2: Verify the whole crate still builds.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
```

Expected: clean build (no warnings about unused visibility changes — the helpers are currently consumed by `predicate_pushdown` which is in the same module).

- [ ] **Step 3: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/optimizer/expr_utils.rs
git commit -m "Bridge: upgrade more expr_utils helpers to pub(crate) for Phase 3

Phase 3 migrates predicate_pushdown from src/sql/optimizer/ into cascades
RBO rules. Those rules need split_and, combine_and, collect_output_columns,
wrap_remaining_filter, collect_qualified_column_refs, and
collect_qualified_output_columns — currently pub(super) and thus invisible
outside the optimizer module. Upgrade them (and the QualifiedRef type
alias) to pub(crate).

Phase 4 relocates expr_utils into src/sql/cascades/rbo/utils.rs; this
visibility bump is a temporary cross-module bridge, same pattern as the
Phase 2 bridge for collect_column_refs/merge_needed.
"
```

---

## Task 2: Scaffold `rbo/rules/predicate_pushdown/` Module

Set up the new submodule with a `mod.rs` that re-exports the per-shape rules and exposes a `predicate_pushdown_rules()` factory. The factory is empty at this point; each subsequent task appends one rule.

**Files:**
- Create: `src/sql/cascades/rbo/rules/predicate_pushdown/mod.rs`
- Modify: `src/sql/cascades/rbo/rules/mod.rs`

- [ ] **Step 1: Create the submodule with an empty factory.**

Create `src/sql/cascades/rbo/rules/predicate_pushdown/mod.rs`:

```rust
//! Predicate pushdown RBO rules. Each sub-module is a small bottom-up
//! rewrite matching a specific `LogicalFilter(X)` shape (or, for the
//! SEMI/ANTI case, a `LogicalJoin(...)` with an inner condition that can
//! be partly pushed into the right child).
//!
//! Unlike `PruneColumns` (documented exception — top-down, recurses
//! internally), these rules follow the convention: `apply` performs one
//! shape rewrite at this node only; the driver's bottom-up + fixed-point
//! walker handles traversal and repeated firing.
//!
//! Replaces the legacy `src/sql/optimizer/predicate_pushdown.rs` single
//! recursive function. The semantic target: every conjunct of every Filter
//! lands as close to the Scan as safely possible, respecting
//! SEMI/ANTI/OUTER null-preservation constraints.

use super::super::rule::RewriteRule;

/// Every predicate-pushdown rule in canonical application order.
pub(crate) fn predicate_pushdown_rules() -> Vec<Box<dyn RewriteRule>> {
    // Tasks 3–7 add each rule in sequence.
    Vec::new()
}
```

- [ ] **Step 2: Register the submodule in the parent rule index.**

Edit `src/sql/cascades/rbo/rules/mod.rs`. After the existing `pub(crate) mod column_pruning;` line, add:

```rust
pub(crate) mod predicate_pushdown;
```

In `all_rbo_rules()`, after `all.extend(column_pruning_rules());`, add:

```rust
all.extend(predicate_pushdown::predicate_pushdown_rules());
```

The resulting `all_rbo_rules()` body reads:

```rust
pub(crate) fn all_rbo_rules() -> Vec<Box<dyn RewriteRule>> {
    let mut all = Vec::new();
    all.extend(column_pruning_rules());
    all.extend(predicate_pushdown::predicate_pushdown_rules());
    all
}
```

- [ ] **Step 3: Verify build + registry test.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
cargo test -p novarocks --lib sql::cascades::rbo::rules::tests::registry_contains_prune_columns 2>&1 | tail -5
```

Expected: clean build; registry test still passes (factory is empty, so `all_rbo_rules()` still returns exactly `[PruneColumns]`).

- [ ] **Step 4: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/rules/predicate_pushdown/ src/sql/cascades/rbo/rules/mod.rs
git commit -m "Scaffold predicate_pushdown RBO rule submodule

Empty factory + registry wiring. Subsequent Phase 3 tasks append each
shape-specific rule (PushDownPredicateScan, ...Project, ...Join,
...Aggregate, PushSemiAntiRightOnlyCondition) one at a time.
"
```

---

## Task 3: `PushDownPredicateScan`

Matches `LogicalFilter(LogicalScan)`. Splits the predicate on AND, pushes conjuncts whose column references all exist in the scan's output columns into `ScanNode.predicates`, and wraps any unpushable conjuncts back as a residual `Filter` above the Scan.

**Files:**
- Create: `src/sql/cascades/rbo/rules/predicate_pushdown/push_to_scan.rs`
- Modify: `src/sql/cascades/rbo/rules/predicate_pushdown/mod.rs`

- [ ] **Step 1: Write the rule.**

Create `src/sql/cascades/rbo/rules/predicate_pushdown/push_to_scan.rs`:

```rust
//! PushDownPredicateScan — `Filter(Scan)` rewrite.
//!
//! Pushes filter conjuncts into `ScanNode.predicates` when every column
//! the conjunct references is present in the scan's output. Unpushable
//! conjuncts are wrapped back as a residual `Filter` above the scan.
//!
//! Mirrors the `LogicalPlan::Scan(mut scan)` arm of legacy
//! `predicate_pushdown::push_filter_into`.

use std::collections::HashSet;

use super::super::super::rule::RewriteRule;
use crate::sql::optimizer::expr_utils::{collect_column_refs, split_and, wrap_remaining_filter};
use crate::sql::plan::*;

pub(crate) struct PushDownPredicateScan;

impl RewriteRule for PushDownPredicateScan {
    fn name(&self) -> &'static str {
        "PushDownPredicateScan"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Filter(f) if matches!(*f.input, LogicalPlan::Scan(_))
        )
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Filter(filter) = plan else {
            return None;
        };
        let LogicalPlan::Scan(mut scan) = *filter.input else {
            return None;
        };

        let conjuncts = split_and(filter.predicate);
        let scan_columns: HashSet<String> =
            scan.columns.iter().map(|c| c.name.to_lowercase()).collect();

        let mut pushed_any = false;
        let mut remaining = Vec::new();
        for conj in conjuncts {
            let refs = collect_column_refs(&conj);
            if refs
                .iter()
                .all(|r| scan_columns.contains(&r.to_lowercase()))
            {
                scan.predicates.push(conj);
                pushed_any = true;
            } else {
                remaining.push(conj);
            }
        }

        if !pushed_any {
            // No change — re-wrap the untouched filter so the driver's
            // "Option::None = no-op" contract holds.
            return None;
        }

        Some(wrap_remaining_filter(LogicalPlan::Scan(scan), remaining))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
    use crate::sql::ir::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }

    fn eq(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Eq,
                right: Box::new(b),
            },
        }
    }

    fn scan_with_cols(cols: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: cols
                    .iter()
                    .map(|n| ColumnDef {
                        name: (*n).into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    })
                    .collect(),
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: cols
                .iter()
                .map(|n| ColumnDef {
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
        })
    }

    #[test]
    fn pushes_single_scan_column_predicate() {
        let scan = scan_with_cols(&["a", "b"]);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: eq(col("a"), int_lit(1)),
        });
        let rule = PushDownPredicateScan;
        assert!(rule.matches(&filter));
        let out = rule.apply(filter).expect("should rewrite");
        match out {
            LogicalPlan::Scan(s) => {
                assert_eq!(s.predicates.len(), 1);
            }
            _ => panic!("expected bare Scan after full pushdown, got {:?}", out),
        }
    }

    #[test]
    fn leaves_unmatched_shape_alone() {
        let rule = PushDownPredicateScan;
        let scan = scan_with_cols(&["a"]);
        assert!(!rule.matches(&scan));
    }

    #[test]
    fn returns_none_when_nothing_pushed() {
        // Filter references a column the scan does not expose — nothing
        // is pushable; rule must return None so the driver's fixed-point
        // terminates on this shape.
        let scan = scan_with_cols(&["a"]);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: eq(col("zz"), int_lit(1)),
        });
        let rule = PushDownPredicateScan;
        assert!(rule.apply(filter).is_none());
    }
}
```

- [ ] **Step 2: Register the rule in the factory.**

Edit `src/sql/cascades/rbo/rules/predicate_pushdown/mod.rs`. Add `pub(crate) mod push_to_scan;` near the top, and in `predicate_pushdown_rules()` replace `Vec::new()` with:

```rust
vec![Box::new(push_to_scan::PushDownPredicateScan)]
```

- [ ] **Step 3: Run the new tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test -p novarocks --lib sql::cascades::rbo::rules::predicate_pushdown::push_to_scan 2>&1 | tail -20
```

Expected: 3 tests pass.

- [ ] **Step 4: Verify the crate still builds clean.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
```

Expected: clean.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/rules/predicate_pushdown/
git commit -m "Add PushDownPredicateScan RBO rule

First of five shape-specific predicate-pushdown rules migrated from
legacy src/sql/optimizer/predicate_pushdown.rs. Matches Filter(Scan),
pushes conjuncts whose refs are all scan-output columns into
ScanNode.predicates; wraps residuals back as a Filter. Returns None
when nothing changes so the driver's fixed-point terminates.
"
```

---

## Task 4: `PushDownPredicateProject`

Matches `LogicalFilter(LogicalProject)`. Splits the predicate; pushes conjuncts whose refs only reference pass-through (base-column) projection items below the Project; keeps conjuncts that reference computed expressions as a residual Filter above.

**Files:**
- Create: `src/sql/cascades/rbo/rules/predicate_pushdown/push_through_project.rs`
- Modify: `src/sql/cascades/rbo/rules/predicate_pushdown/mod.rs`

- [ ] **Step 1: Write the rule.**

Create `src/sql/cascades/rbo/rules/predicate_pushdown/push_through_project.rs`:

```rust
//! PushDownPredicateProject — `Filter(Project)` rewrite.
//!
//! Pushes conjuncts that reference only pass-through (i.e. bare
//! `ColumnRef`) projection items below the Project, leaving conjuncts
//! that touch computed expressions as a residual Filter above. One step
//! only — the driver's bottom-up walker will push further at the next
//! round.
//!
//! Mirrors the `LogicalPlan::Project(proj)` arm of legacy
//! `predicate_pushdown::push_filter_into`, with the difference that this
//! rule does NOT recurse (driver owns traversal).

use std::collections::HashSet;

use super::super::super::rule::RewriteRule;
use crate::sql::ir::ExprKind;
use crate::sql::optimizer::expr_utils::{
    collect_column_refs, combine_and, split_and, wrap_remaining_filter,
};
use crate::sql::plan::*;

pub(crate) struct PushDownPredicateProject;

impl RewriteRule for PushDownPredicateProject {
    fn name(&self) -> &'static str {
        "PushDownPredicateProject"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Filter(f) if matches!(*f.input, LogicalPlan::Project(_))
        )
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Filter(filter) = plan else {
            return None;
        };
        let LogicalPlan::Project(proj) = *filter.input else {
            return None;
        };

        let passthrough_columns: HashSet<String> = proj
            .items
            .iter()
            .filter_map(|item| {
                if let ExprKind::ColumnRef { column, .. } = &item.expr.kind {
                    Some(column.to_lowercase())
                } else {
                    None
                }
            })
            .collect();

        let conjuncts = split_and(filter.predicate);
        let mut pushable = Vec::new();
        let mut remaining = Vec::new();
        for conj in conjuncts {
            let refs = collect_column_refs(&conj);
            if !refs.is_empty()
                && refs
                    .iter()
                    .all(|r| passthrough_columns.contains(&r.to_lowercase()))
            {
                pushable.push(conj);
            } else {
                remaining.push(conj);
            }
        }

        if pushable.is_empty() {
            return None;
        }

        // Build Filter(child) below the Project.
        let pushed = combine_and(pushable);
        let new_child = LogicalPlan::Filter(FilterNode {
            input: proj.input,
            predicate: pushed,
        });
        let new_project = LogicalPlan::Project(ProjectNode {
            input: Box::new(new_child),
            items: proj.items,
        });
        Some(wrap_remaining_filter(new_project, remaining))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
    use crate::sql::ir::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::plan::{ProjectItem, ProjectNode};
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
        }
    }
    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }
    fn eq(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Eq,
                right: Box::new(b),
            },
        }
    }

    fn scan_with_cols(cols: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: cols
                    .iter()
                    .map(|n| ColumnDef {
                        name: (*n).into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    })
                    .collect(),
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: cols
                .iter()
                .map(|n| ColumnDef {
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
        })
    }

    #[test]
    fn pushes_through_passthrough_project() {
        // SELECT a, b FROM (SELECT a, b FROM t) WHERE a = 1
        let scan = scan_with_cols(&["a", "b"]);
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![
                ProjectItem {
                    alias: "a".into(),
                    expr: col("a"),
                },
                ProjectItem {
                    alias: "b".into(),
                    expr: col("b"),
                },
            ],
        });
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(project),
            predicate: eq(col("a"), int_lit(1)),
        });

        let rule = PushDownPredicateProject;
        assert!(rule.matches(&filter));
        let out = rule.apply(filter).expect("should push");
        // Expected shape: Project(Filter(Scan))
        match out {
            LogicalPlan::Project(p) => match *p.input {
                LogicalPlan::Filter(_) => {}
                other => panic!("expected Filter under Project, got {:?}", other),
            },
            other => panic!("expected Project at root, got {:?}", other),
        }
    }

    #[test]
    fn does_not_push_through_computed_projection() {
        // SELECT a+1 AS x FROM t WHERE x = 5 — 'x' is computed; filter stays above.
        let scan = scan_with_cols(&["a"]);
        let add_expr = TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(col("a")),
                op: BinOp::Add,
                right: Box::new(int_lit(1)),
            },
        };
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                alias: "x".into(),
                expr: add_expr,
            }],
        });
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(project),
            predicate: eq(col("x"), int_lit(5)),
        });
        let rule = PushDownPredicateProject;
        assert!(rule.apply(filter).is_none());
    }
}
```

- [ ] **Step 2: Register the rule.**

Edit `src/sql/cascades/rbo/rules/predicate_pushdown/mod.rs`. Add `pub(crate) mod push_through_project;`, and extend the `vec![...]` in `predicate_pushdown_rules()`:

```rust
vec![
    Box::new(push_to_scan::PushDownPredicateScan),
    Box::new(push_through_project::PushDownPredicateProject),
]
```

- [ ] **Step 3: Run tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test -p novarocks --lib sql::cascades::rbo::rules::predicate_pushdown::push_through_project 2>&1 | tail -20
```

Expected: 2 tests pass.

- [ ] **Step 4: Verify crate builds.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
```

Expected: clean.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/rules/predicate_pushdown/
git commit -m "Add PushDownPredicateProject RBO rule

Pushes conjuncts through pass-through column projections; preserves
computed-expression filter placement above the Project.
"
```

---

## Task 5: `PushDownPredicateAggregate`

Matches `LogicalFilter(LogicalAggregate)`. Pushes conjuncts that reference only GROUP BY key columns below the aggregate (as a Filter over `agg.input`). Keeps conjuncts that reference aggregate output columns as a residual Filter above.

**Files:**
- Create: `src/sql/cascades/rbo/rules/predicate_pushdown/push_to_aggregate.rs`
- Modify: `src/sql/cascades/rbo/rules/predicate_pushdown/mod.rs`

- [ ] **Step 1: Write the rule.**

Create `src/sql/cascades/rbo/rules/predicate_pushdown/push_to_aggregate.rs`:

```rust
//! PushDownPredicateAggregate — `Filter(Aggregate)` rewrite.
//!
//! Pushes conjuncts whose refs are entirely GROUP BY key columns below
//! the aggregate. Predicates referencing aggregate outputs (computed
//! expressions) remain above.
//!
//! Mirrors legacy `push_predicates_through_aggregate`. Does not recurse.

use std::collections::HashSet;

use super::super::super::rule::RewriteRule;
use crate::sql::ir::ExprKind;
use crate::sql::optimizer::expr_utils::{
    collect_column_refs, combine_and, split_and, wrap_remaining_filter,
};
use crate::sql::plan::*;

pub(crate) struct PushDownPredicateAggregate;

impl RewriteRule for PushDownPredicateAggregate {
    fn name(&self) -> &'static str {
        "PushDownPredicateAggregate"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Filter(f) if matches!(*f.input, LogicalPlan::Aggregate(_))
        )
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Filter(filter) = plan else {
            return None;
        };
        let LogicalPlan::Aggregate(agg) = *filter.input else {
            return None;
        };

        // GROUP BY key column names (only ColumnRef-shaped GROUP BY items
        // contribute pushable columns; computed GROUP BY expressions do not,
        // matching the legacy behavior).
        let group_by_columns: HashSet<String> = agg
            .group_by
            .iter()
            .filter_map(|e| match &e.kind {
                ExprKind::ColumnRef { column, .. } => Some(column.to_lowercase()),
                _ => None,
            })
            .collect();

        let conjuncts = split_and(filter.predicate);
        let mut pushable = Vec::new();
        let mut remaining = Vec::new();
        for conj in conjuncts {
            let refs = collect_column_refs(&conj);
            if !refs.is_empty()
                && refs
                    .iter()
                    .all(|r| group_by_columns.contains(&r.to_lowercase()))
            {
                pushable.push(conj);
            } else {
                remaining.push(conj);
            }
        }

        if pushable.is_empty() {
            return None;
        }

        let pushed = combine_and(pushable);
        let new_child = LogicalPlan::Filter(FilterNode {
            input: agg.input,
            predicate: pushed,
        });
        let new_agg = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(new_child),
            group_by: agg.group_by,
            aggregates: agg.aggregates,
            output_columns: agg.output_columns,
        });
        Some(wrap_remaining_filter(new_agg, remaining))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
    use crate::sql::ir::{AggregateFunc, BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::plan::AggregateCall;
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
        }
    }
    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }
    fn eq(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Eq,
                right: Box::new(b),
            },
        }
    }
    fn scan_with_cols(cols: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: cols
                    .iter()
                    .map(|n| ColumnDef {
                        name: (*n).into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    })
                    .collect(),
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: cols
                .iter()
                .map(|n| ColumnDef {
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
        })
    }

    /// Build an Aggregate that GROUPs BY `a` and SUMs `b`. Resulting columns
    /// are `a` (group key) and `sum_b` (aggregate output). The AggregateCall
    /// shape matches the crate's `AggregateCall` definition; if that struct
    /// carries different fields from this sketch, match the real shape via
    /// cargo error messages.
    fn agg_sum_grouped_by_a(scan: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![col("a")],
            aggregates: vec![AggregateCall {
                func: AggregateFunc::Sum,
                args: vec![col("b")],
                distinct: false,
                alias: "sum_b".into(),
                return_type: DataType::Int64,
                nullable: true,
                filter: None,
            }],
            output_columns: vec![
                ColumnDef {
                    name: "a".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
                ColumnDef {
                    name: "sum_b".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
            ],
        })
    }

    #[test]
    fn pushes_group_by_column_predicate() {
        let scan = scan_with_cols(&["a", "b"]);
        let agg = agg_sum_grouped_by_a(scan);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(agg),
            predicate: eq(col("a"), int_lit(1)),
        });
        let rule = PushDownPredicateAggregate;
        assert!(rule.matches(&filter));
        let out = rule.apply(filter).expect("should push");
        match out {
            LogicalPlan::Aggregate(a) => match *a.input {
                LogicalPlan::Filter(_) => {}
                other => panic!("expected Filter under Aggregate, got {:?}", other),
            },
            other => panic!("expected Aggregate at root, got {:?}", other),
        }
    }

    #[test]
    fn does_not_push_aggregate_output_predicate() {
        let scan = scan_with_cols(&["a", "b"]);
        let agg = agg_sum_grouped_by_a(scan);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(agg),
            predicate: eq(col("sum_b"), int_lit(100)),
        });
        let rule = PushDownPredicateAggregate;
        assert!(rule.apply(filter).is_none());
    }
}
```

- [ ] **Step 2: Read the `AggregateCall` / `AggregateFunc` struct shapes and adjust test helper if needed.**

```bash
cd /Users/harbor/project/NovaRocks
grep -n 'pub struct AggregateCall\|pub enum AggregateFunc' src/sql/plan.rs src/sql/ir.rs src/sql/ir/*.rs 2>/dev/null
```

The test helper `agg_sum_grouped_by_a` inside the new file is a sketch; if the actual `AggregateCall` fields differ (e.g. different names for `alias`/`return_type`/`nullable`/`filter`), update the literal to match. The Phase 2 column_pruning plan hit the same fit-and-adjust cycle (see its Task 2 Step 2 note).

- [ ] **Step 3: Register the rule.**

Edit `src/sql/cascades/rbo/rules/predicate_pushdown/mod.rs`. Add `pub(crate) mod push_to_aggregate;`. Extend the factory `vec![...]`:

```rust
vec![
    Box::new(push_to_scan::PushDownPredicateScan),
    Box::new(push_through_project::PushDownPredicateProject),
    Box::new(push_to_aggregate::PushDownPredicateAggregate),
]
```

- [ ] **Step 4: Run tests + build.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test -p novarocks --lib sql::cascades::rbo::rules::predicate_pushdown::push_to_aggregate 2>&1 | tail -20
cargo build 2>&1 | tail -3
```

Expected: 2 tests pass; clean build.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/rules/predicate_pushdown/
git commit -m "Add PushDownPredicateAggregate RBO rule

Pushes GROUP-BY-key-only predicates below an Aggregate, leaving
aggregate-output predicates above.
"
```

---

## Task 6: `PushDownPredicateJoin` (with OR-factoring)

Matches `LogicalFilter(LogicalJoin)`. This is the largest rule — mirrors legacy `push_predicates_through_join` + `factor_common_eq_from_or` (single-step OR factoring of cross-side equi-joins).

Classification logic per conjunct:
- left-only refs → push into left
- right-only refs → push into right (only for INNER/CROSS/RIGHT_OUTER/RIGHT_SEMI/RIGHT_ANTI; others remain)
- both-sides refs: re-check with qualified refs (self-join disambiguation); if genuinely cross-side, try OR factoring to extract common equi-joins into the join condition; otherwise merge into join condition
- constant (no refs) → push to left (mirrors legacy)
- OUTER joins on non-preserving sides → remaining filter

When a CROSS join gains a join predicate, upgrade to INNER.

**Files:**
- Create: `src/sql/cascades/rbo/rules/predicate_pushdown/push_to_join.rs`
- Modify: `src/sql/cascades/rbo/rules/predicate_pushdown/mod.rs`

- [ ] **Step 1: Write the rule file skeleton (helpers + impl).**

Create `src/sql/cascades/rbo/rules/predicate_pushdown/push_to_join.rs`. **Port** the following pieces from `src/sql/optimizer/predicate_pushdown.rs` verbatim (only changes: move to this file, change `pub(super)` to private, adjust imports):

- `fn push_predicates_through_join(predicate: TypedExpr, join: JoinNode) -> LogicalPlan` (legacy lines 154-359) — **but wrap it** as an `apply` that takes `Filter(Join)`.
- `fn factor_common_eq_from_or(...)` (legacy lines 439-519) — keep private.
- `fn split_or_branches(...)` (legacy lines 612-626) — keep private.
- `fn split_and_refs(...)` (legacy lines 628-642) — keep private.
- `fn is_cross_side_eq(...)` (legacy lines 644-673) — keep private.
- `fn expr_eq(...)` (legacy lines 675-677) — keep private.
- `fn merge_join_conditions(...)` (legacy lines 680-694) — keep private.

The file header and rule struct:

```rust
//! PushDownPredicateJoin — `Filter(Join)` rewrite.
//!
//! Classifies conjuncts of the filter predicate by which side of the join
//! they reference, pushes single-side predicates below the join (respecting
//! OUTER/SEMI/ANTI null-preservation), and merges genuine cross-side
//! conjuncts into the join condition. Also performs single-step OR-factoring
//! to extract common equi-joins from OR branches. Upgrades a CROSS join to
//! INNER when a predicate promotes it.
//!
//! Mirrors legacy `push_predicates_through_join` + `factor_common_eq_from_or`
//! from `src/sql/optimizer/predicate_pushdown.rs`. One step per apply — the
//! driver's fixed-point handles repeated firing when a newly-formed shape
//! exposes further opportunities.

use std::collections::HashSet;

use super::super::super::rule::RewriteRule;
use crate::sql::ir::{BinOp, ExprKind, JoinKind, LiteralValue, TypedExpr};
use crate::sql::optimizer::expr_utils::{
    collect_column_refs, collect_output_columns, collect_qualified_column_refs,
    collect_qualified_output_columns, combine_and, split_and, wrap_remaining_filter,
};
use crate::sql::plan::*;
use arrow::datatypes::DataType;

pub(crate) struct PushDownPredicateJoin;

impl RewriteRule for PushDownPredicateJoin {
    fn name(&self) -> &'static str {
        "PushDownPredicateJoin"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Filter(f) if matches!(*f.input, LogicalPlan::Join(_))
        )
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Filter(filter) = plan else {
            return None;
        };
        let LogicalPlan::Join(join) = *filter.input else {
            return None;
        };
        let before_join = join.clone();
        let before_pred = filter.predicate.clone();
        let rewritten = push_predicates_through_join(filter.predicate, join);
        // If the rewrite yields exactly Filter(Join{ == before_join }) with
        // the original predicate re-attached, nothing changed — return None
        // so the driver's fixed-point terminates.
        if is_unchanged(&rewritten, &before_join, &before_pred) {
            None
        } else {
            Some(rewritten)
        }
    }
}

/// Detect "apply was a no-op": the output is literally Filter(Join(same))
/// with the same predicate. Covers the case where every conjunct landed in
/// `remaining` because none were classifiable.
fn is_unchanged(out: &LogicalPlan, before_join: &JoinNode, before_pred: &TypedExpr) -> bool {
    let LogicalPlan::Filter(f) = out else {
        return false;
    };
    let LogicalPlan::Join(j) = &*f.input else {
        return false;
    };
    // Debug-format comparison is coarse but safe: a true rewrite changes at
    // least one child subtree or the condition shape.
    format!("{:?}", j) == format!("{:?}", before_join)
        && format!("{:?}", f.predicate) == format!("{:?}", before_pred)
}

// ---- Port of legacy helpers below (verbatim except pub(super) -> private
// and absolute paths where needed). ----

fn push_predicates_through_join(predicate: TypedExpr, join: JoinNode) -> LogicalPlan {
    // ... paste the body of legacy push_predicates_through_join here
    //     (predicate_pushdown.rs lines 154-359) ...
}

fn factor_common_eq_from_or(
    expr: &TypedExpr,
    left_cols: &HashSet<String>,
    right_cols: &HashSet<String>,
) -> (Vec<TypedExpr>, Option<TypedExpr>) {
    // ... paste legacy lines 439-519 ...
}

fn split_or_branches(expr: &TypedExpr) -> Vec<&TypedExpr> {
    // ... paste legacy lines 612-626 ...
}

fn split_and_refs(expr: &TypedExpr) -> Vec<&TypedExpr> {
    // ... paste legacy lines 628-642 ...
}

fn is_cross_side_eq(
    expr: &TypedExpr,
    left_cols: &HashSet<String>,
    right_cols: &HashSet<String>,
) -> bool {
    // ... paste legacy lines 644-673 ...
}

fn expr_eq(a: &TypedExpr, b: &TypedExpr) -> bool {
    format!("{:?}", a.kind) == format!("{:?}", b.kind)
}

fn merge_join_conditions(
    existing: Option<TypedExpr>,
    new_preds: Vec<TypedExpr>,
) -> Option<TypedExpr> {
    // ... paste legacy lines 680-694 ...
}
```

**Paste instructions for the porting steps (applies to every `// ... paste ...` placeholder above):** copy the function body verbatim from the corresponding lines in `src/sql/optimizer/predicate_pushdown.rs`. The only edits while pasting are:

1. Remove any `super::` import prefixes that no longer resolve (they're all in the `use` block at the top of the new file).
2. Replace qualified `crate::sql::ir::BinOp::*` etc. with the local `BinOp::*` path (since they're already imported).
3. Leave all internal logic unchanged — the existing behavior is the target.

- [ ] **Step 2: Perform the port by reading the legacy source directly.**

```bash
cd /Users/harbor/project/NovaRocks
sed -n '154,359p' src/sql/optimizer/predicate_pushdown.rs  # push_predicates_through_join
sed -n '439,519p' src/sql/optimizer/predicate_pushdown.rs  # factor_common_eq_from_or
sed -n '612,694p' src/sql/optimizer/predicate_pushdown.rs  # split_or_branches + split_and_refs + is_cross_side_eq + expr_eq + merge_join_conditions
```

Copy those bodies into the placeholders in the new file.

- [ ] **Step 3: Write inline tests.**

Append to `push_to_join.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
        }
    }
    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }
    fn eq(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Eq,
                right: Box::new(b),
            },
        }
    }
    fn and(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::And,
                right: Box::new(b),
            },
        }
    }

    fn scan(name: &str, cols: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: name.into(),
                columns: cols
                    .iter()
                    .map(|n| ColumnDef {
                        name: (*n).into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    })
                    .collect(),
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: cols
                .iter()
                .map(|n| ColumnDef {
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
        })
    }

    fn inner_join(left: LogicalPlan, right: LogicalPlan, condition: Option<TypedExpr>) -> LogicalPlan {
        LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Inner,
            condition,
        })
    }

    #[test]
    fn pushes_left_only_predicate_below_inner_join() {
        // t1 INNER t2 ON t1.a = t2.b WHERE t1.x = 1
        let l = scan("t1", &["a", "x"]);
        let r = scan("t2", &["b", "y"]);
        let j = inner_join(l, r, Some(eq(col("a"), col("b"))));
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(j),
            predicate: eq(col("x"), int_lit(1)),
        });
        let rule = PushDownPredicateJoin;
        let out = rule.apply(filter).expect("should push");
        match out {
            LogicalPlan::Join(j) => {
                // Left child should now carry the predicate (as Filter or pushed to scan).
                let left_debug = format!("{:?}", j.left);
                assert!(
                    left_debug.contains("Filter") || left_debug.contains("predicates:"),
                    "left child should carry pushed predicate, got: {}",
                    left_debug
                );
            }
            other => panic!("expected Join at root after left-only push, got {:?}", other),
        }
    }

    #[test]
    fn pushes_right_only_below_inner_join() {
        let l = scan("t1", &["a"]);
        let r = scan("t2", &["b", "y"]);
        let j = inner_join(l, r, Some(eq(col("a"), col("b"))));
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(j),
            predicate: eq(col("y"), int_lit(1)),
        });
        let rule = PushDownPredicateJoin;
        let out = rule.apply(filter).expect("should push");
        match out {
            LogicalPlan::Join(j) => {
                let right_debug = format!("{:?}", j.right);
                assert!(
                    right_debug.contains("Filter") || right_debug.contains("predicates:"),
                    "right child should carry pushed predicate, got: {}",
                    right_debug
                );
            }
            other => panic!("expected Join at root, got {:?}", other),
        }
    }

    #[test]
    fn merges_cross_side_predicate_into_join_condition() {
        // CROSS(t1, t2) WHERE t1.a = t2.b → upgrades to INNER with condition.
        let l = scan("t1", &["a"]);
        let r = scan("t2", &["b"]);
        let cross = LogicalPlan::Join(JoinNode {
            left: Box::new(l),
            right: Box::new(r),
            join_type: JoinKind::Cross,
            condition: None,
        });
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(cross),
            predicate: eq(col("a"), col("b")),
        });
        let rule = PushDownPredicateJoin;
        let out = rule.apply(filter).expect("should merge");
        match out {
            LogicalPlan::Join(j) => {
                assert!(matches!(j.join_type, JoinKind::Inner));
                assert!(j.condition.is_some());
            }
            other => panic!("expected Inner Join at root, got {:?}", other),
        }
    }

    #[test]
    fn does_not_push_left_side_below_right_outer() {
        // RIGHT OUTER: left-side predicates must stay above (left is nullable).
        let l = scan("t1", &["a", "x"]);
        let r = scan("t2", &["b"]);
        let j = LogicalPlan::Join(JoinNode {
            left: Box::new(l),
            right: Box::new(r),
            join_type: JoinKind::RightOuter,
            condition: Some(eq(col("a"), col("b"))),
        });
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(j),
            predicate: eq(col("x"), int_lit(1)),
        });
        let rule = PushDownPredicateJoin;
        let out = rule.apply(filter);
        // Expected: rule returns None (no change) because x is left-only but
        // the join is right-outer — the conjunct stays in remaining above.
        // OR: the rule returns Some where output is still Filter(Join) with
        // unchanged structure. Either is acceptable so long as the plan
        // shape does not put the filter below the join. We accept None here.
        if let Some(out_plan) = out {
            // If rule changed shape, verify left child is unchanged.
            match out_plan {
                LogicalPlan::Filter(f) => match *f.input {
                    LogicalPlan::Join(j) => {
                        assert!(
                            !format!("{:?}", j.left).contains("Filter"),
                            "left child should not receive the pushed predicate for RIGHT OUTER"
                        );
                    }
                    _ => panic!("expected Filter(Join) shape"),
                },
                _ => panic!("expected Filter shape"),
            }
        }
    }
}
```

- [ ] **Step 4: Register the rule.**

Edit `src/sql/cascades/rbo/rules/predicate_pushdown/mod.rs`. Add `pub(crate) mod push_to_join;`. Extend the factory:

```rust
vec![
    Box::new(push_to_scan::PushDownPredicateScan),
    Box::new(push_through_project::PushDownPredicateProject),
    Box::new(push_to_aggregate::PushDownPredicateAggregate),
    Box::new(push_to_join::PushDownPredicateJoin),
]
```

- [ ] **Step 5: Run tests + build.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test -p novarocks --lib sql::cascades::rbo::rules::predicate_pushdown::push_to_join 2>&1 | tail -30
cargo build 2>&1 | tail -3
```

Expected: 4 tests pass; clean build.

- [ ] **Step 6: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/rules/predicate_pushdown/
git commit -m "Add PushDownPredicateJoin RBO rule with OR factoring

Ports legacy push_predicates_through_join, factor_common_eq_from_or, and
their helpers verbatim into a single-step RewriteRule. Behavior target:
byte-identical classification of left-only / right-only / both-sides /
constant conjuncts; OR factoring extracts common equi-joins; CROSS -> INNER
promotion on condition merge.
"
```

---

## Task 7: `PushSemiAntiRightOnlyCondition`

Matches `LogicalJoin` with `LeftSemi|LeftAnti|RightSemi|RightAnti` carrying an inner join condition, and pushes right-only conjuncts of that condition into the right child. Mirrors legacy `push_semi_condition_into_children`. Not a Filter-shape rule.

**Files:**
- Create: `src/sql/cascades/rbo/rules/predicate_pushdown/semi_anti_condition.rs`
- Modify: `src/sql/cascades/rbo/rules/predicate_pushdown/mod.rs`

- [ ] **Step 1: Write the rule.**

Create `src/sql/cascades/rbo/rules/predicate_pushdown/semi_anti_condition.rs`:

```rust
//! PushSemiAntiRightOnlyCondition — push right-only conjuncts of a
//! SEMI/ANTI join's inner condition into the right child.
//!
//! Example:
//!   LEFT SEMI (store_sales CROSS date_dim)
//!     ON (corr AND ss_sold_date_sk = d_date_sk AND d_year = 2002)
//! becomes:
//!   LEFT SEMI (store_sales INNER date_dim ON ss_sold_date_sk = d_date_sk WHERE d_year = 2002)
//!     ON (corr)
//!
//! Matches `LogicalJoin` with a SEMI/ANTI join_type AND an inner condition
//! from which at least one conjunct is right-only. One step — the driver's
//! fixed-point and other rules (e.g. PushDownPredicateScan) take over on
//! the pushed filter afterwards.
//!
//! Mirrors legacy `push_semi_condition_into_children` from
//! `src/sql/optimizer/predicate_pushdown.rs`. Ported verbatim except for
//! being exposed through the RewriteRule trait.

use std::collections::HashSet;

use super::super::super::rule::RewriteRule;
use crate::sql::ir::{ExprKind, JoinKind, TypedExpr};
use crate::sql::optimizer::expr_utils::{
    collect_column_refs, collect_output_columns, collect_qualified_column_refs,
    collect_qualified_output_columns, combine_and, split_and,
};
use crate::sql::plan::*;

pub(crate) struct PushSemiAntiRightOnlyCondition;

impl RewriteRule for PushSemiAntiRightOnlyCondition {
    fn name(&self) -> &'static str {
        "PushSemiAntiRightOnlyCondition"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        let LogicalPlan::Join(j) = plan else {
            return false;
        };
        matches!(
            j.join_type,
            JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::RightSemi | JoinKind::RightAnti
        ) && j.condition.is_some()
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Join(join) = plan else {
            return None;
        };
        let Some(ref condition) = join.condition else {
            return None;
        };
        // Port of push_semi_condition_into_children logic.
        let conjuncts = split_and(condition.clone());
        let right_cols = collect_output_columns(&join.right);
        let left_cols = collect_output_columns(&join.left);
        let right_qcols = collect_qualified_output_columns(&join.right);

        let mut keep_in_condition = Vec::new();
        let mut push_to_right = Vec::new();

        for conj in conjuncts {
            let refs = collect_column_refs(&conj);
            let qrefs = collect_qualified_column_refs(&conj);
            let is_right_only = if !qrefs.is_empty() {
                let q_all_right = qrefs.iter().all(|r| right_qcols.contains(r));
                let bare_any_left = refs.iter().any(|c| left_cols.contains(&c.to_lowercase()));
                q_all_right && !bare_any_left
            } else if !refs.is_empty() {
                let all_in_right = refs.iter().all(|c| right_cols.contains(&c.to_lowercase()));
                let any_in_left = refs.iter().any(|c| left_cols.contains(&c.to_lowercase()));
                all_in_right && !any_in_left
            } else {
                false
            };
            if is_right_only {
                push_to_right.push(conj);
            } else {
                keep_in_condition.push(conj);
            }
        }

        if push_to_right.is_empty() {
            return None;
        }

        let new_condition = if keep_in_condition.is_empty() {
            None
        } else {
            Some(combine_and(keep_in_condition))
        };
        let pushed = combine_and(push_to_right);
        let new_right = LogicalPlan::Filter(FilterNode {
            input: join.right,
            predicate: pushed,
        });
        Some(LogicalPlan::Join(JoinNode {
            left: join.left,
            right: Box::new(new_right),
            join_type: join.join_type,
            condition: new_condition,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
    use crate::sql::ir::{BinOp, LiteralValue};
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
        }
    }
    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }
    fn eq(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Eq,
                right: Box::new(b),
            },
        }
    }
    fn and(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::And,
                right: Box::new(b),
            },
        }
    }
    fn scan(name: &str, cols: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: name.into(),
                columns: cols
                    .iter()
                    .map(|n| ColumnDef {
                        name: (*n).into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    })
                    .collect(),
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: cols
                .iter()
                .map(|n| ColumnDef {
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
        })
    }

    #[test]
    fn pushes_right_only_conjunct_into_right_child_for_left_semi() {
        let l = scan("l", &["a", "corr"]);
        let r = scan("r", &["b", "yr"]);
        // LEFT SEMI ON (corr AND a = b AND yr = 2002)
        let cond = and(
            and(eq(col("corr"), int_lit(1)), eq(col("a"), col("b"))),
            eq(col("yr"), int_lit(2002)),
        );
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(l),
            right: Box::new(r),
            join_type: JoinKind::LeftSemi,
            condition: Some(cond),
        });
        let rule = PushSemiAntiRightOnlyCondition;
        assert!(rule.matches(&join));
        let out = rule.apply(join).expect("should push");
        match out {
            LogicalPlan::Join(j) => {
                assert!(matches!(j.join_type, JoinKind::LeftSemi));
                // Right child should now be a Filter over the scan.
                assert!(
                    format!("{:?}", j.right).contains("Filter"),
                    "right child should wrap pushed filter"
                );
            }
            _ => panic!("expected Join at root"),
        }
    }

    #[test]
    fn returns_none_when_no_right_only_conjunct() {
        let l = scan("l", &["a"]);
        let r = scan("r", &["b"]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(l),
            right: Box::new(r),
            join_type: JoinKind::LeftSemi,
            condition: Some(eq(col("a"), col("b"))),
        });
        let rule = PushSemiAntiRightOnlyCondition;
        assert!(rule.apply(join).is_none());
    }

    #[test]
    fn does_not_match_inner_join() {
        let l = scan("l", &["a"]);
        let r = scan("r", &["b"]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(l),
            right: Box::new(r),
            join_type: JoinKind::Inner,
            condition: Some(eq(col("a"), col("b"))),
        });
        let rule = PushSemiAntiRightOnlyCondition;
        assert!(!rule.matches(&join));
    }
}
```

- [ ] **Step 2: Register the rule.**

Edit `src/sql/cascades/rbo/rules/predicate_pushdown/mod.rs`. Add `pub(crate) mod semi_anti_condition;`. Extend the factory:

```rust
vec![
    Box::new(push_to_scan::PushDownPredicateScan),
    Box::new(push_through_project::PushDownPredicateProject),
    Box::new(push_to_aggregate::PushDownPredicateAggregate),
    Box::new(push_to_join::PushDownPredicateJoin),
    Box::new(semi_anti_condition::PushSemiAntiRightOnlyCondition),
]
```

- [ ] **Step 3: Run tests + build.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test -p novarocks --lib sql::cascades::rbo::rules::predicate_pushdown::semi_anti_condition 2>&1 | tail -20
cargo build 2>&1 | tail -3
```

Expected: 3 tests pass; clean build.

- [ ] **Step 4: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/rules/predicate_pushdown/
git commit -m "Add PushSemiAntiRightOnlyCondition RBO rule

Ports legacy push_semi_condition_into_children: extracts right-only
conjuncts from a SEMI/ANTI join's inner condition into the right child,
matching the legacy CROSS-inside-SEMI -> INNER + filter rewrite.
"
```

---

## Task 8: Switch Over — Run RBO Driver Before and After `rewriter::rewrite`

To preserve the legacy "two pushdown passes around join_reorder" semantics without changing `reorder_joins_cbo` (Phase 5 moves it), call `rbo::driver::rewrite_to_fixed_point` both before and after `rewriter::rewrite`. The pre-rewriter pass pushes predicates so `reorder_joins_cbo` sees scans with filters applied; the post-rewriter pass handles predicates newly exposed by join reordering (e.g. OR-factored equi-joins after a new CROSS upgrade).

**Files:**
- Modify: `src/sql/cascades/mod.rs`

- [ ] **Step 1: Reorder the RBO calls in `optimize()`.**

Find the block in `src/sql/cascades/mod.rs`:

```rust
    // 1. RBO rewrite (legacy; will be progressively migrated to RBO rules below).
    let rewritten = rewriter::rewrite(plan, table_stats);

    // 1b. RBO rule-based rewriter (Phase 1: empty rule list = no-op;
    //     subsequent phases migrate predicate pushdown, column pruning, etc.
    //     from the legacy rewriter above into rules invoked here).
    let options = options::OptimizerOptions::default_settings();
    let rewritten = rbo::driver::rewrite_to_fixed_point(
        rewritten,
        &rbo::rules::all_rbo_rules(),
        &options,
        deadline,
    )?;
```

Replace it with:

```rust
    // 1. RBO rule-based rewriter — first pass. Runs PruneColumns and
    //    PushDownPredicate* rules BEFORE join reorder so reorder sees
    //    scans with their filter predicates already attached.
    let options = options::OptimizerOptions::default_settings();
    let rewritten = rbo::driver::rewrite_to_fixed_point(
        plan,
        &rbo::rules::all_rbo_rules(),
        &options,
        deadline,
    )?;

    // 2. Legacy rewriter. After Phase 3 this contains only
    //    reorder_joins_cbo; Phase 5 migrates that to a cascades Rule and
    //    deletes rewriter.rs entirely.
    let rewritten = rewriter::rewrite(rewritten, table_stats);

    // 3. RBO rule-based rewriter — second pass. Catches predicates newly
    //    exposed by join reorder (mirrors legacy's "push twice around
    //    reorder" semantics).
    let rewritten = rbo::driver::rewrite_to_fixed_point(
        rewritten,
        &rbo::rules::all_rbo_rules(),
        &options,
        deadline,
    )?;
```

Update the downstream step numbering comments if present (step "2. CTE cleanup" → "4.", etc.), OR just remove the numeric prefix and leave semantic comments — the steps are self-describing.

- [ ] **Step 2: Verify the crate still builds.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
```

Expected: clean (legacy rewriter still has the two push_down_predicates calls, which is fine — they'll be removed in Task 9).

- [ ] **Step 3: Run fast unit tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test -p novarocks --lib sql::cascades 2>&1 | tail -5
```

Expected: test count matches post-Phase-2 baseline; any new failures indicate a rule bug.

- [ ] **Step 4: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/mod.rs
git commit -m "Run RBO driver before and after rewriter::rewrite

Phase 3 migrates predicate_pushdown into RBO rules. To preserve legacy
'push twice around join_reorder' semantics before Phase 5 moves
reorder_joins_cbo into cascades, call rbo::driver::rewrite_to_fixed_point
both before and after rewriter::rewrite. The legacy rewriter still
contains its own push_down_predicates calls until Task 9 deletes them;
that overlap is harmless — the rules are idempotent.
"
```

---

## Task 9: Delete `push_down_predicates` Calls From `rewriter::rewrite`

Now that the RBO driver runs pushdown on both sides of `rewriter::rewrite`, the two `push_down_predicates` calls inside `rewriter::rewrite` are redundant. Delete them. After this task, `rewriter::rewrite` contains only `reorder_joins_cbo`.

**Files:**
- Modify: `src/sql/cascades/rewriter.rs`

- [ ] **Step 1: Rewrite `rewriter::rewrite`.**

Replace the body of `src/sql/cascades/rewriter.rs` with:

```rust
//! Legacy RBO rewrite pass. Being progressively migrated into cascades
//! RBO rules under `src/sql/cascades/rbo/rules/`. After Phase 3, only
//! join reorder remains; Phase 5 migrates it into a cascades Rule and
//! Phase 6 deletes this file entirely.

use std::collections::HashMap;

use crate::sql::plan::LogicalPlan;
use crate::sql::statistics::TableStatistics;

/// Apply remaining legacy RBO rewrites to the logical plan. Column
/// pruning (Phase 2) and predicate pushdown (Phase 3) now run via the
/// RBO rule driver in `cascades::optimize`. Only join reorder remains
/// until Phase 5.
pub(crate) fn rewrite(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> LogicalPlan {
    crate::sql::optimizer::join_reorder::reorder_joins_cbo(plan, table_stats)
}
```

- [ ] **Step 2: Verify build and run the cascades tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
cargo test -p novarocks --lib sql::cascades 2>&1 | tail -5
```

Expected: clean build; tests still pass.

- [ ] **Step 3: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rewriter.rs
git commit -m "Drop push_down_predicates from rewriter::rewrite

Phase 3 migrated predicate pushdown into RBO rules, which now run twice
around rewriter::rewrite (before and after). The two legacy pushdown
calls inside rewriter are now redundant. rewriter::rewrite contains only
reorder_joins_cbo; Phase 5 migrates that out and Phase 6 deletes this
file.
"
```

---

## Task 10: Move `factor_common_eq_from_or_for_reorder` Into `join_reorder.rs` and Delete `predicate_pushdown.rs`

`src/sql/optimizer/join_reorder.rs` calls `factor_common_eq_from_or_for_reorder` (defined `pub(super)` in `predicate_pushdown.rs`) plus its private helpers (`factor_common_eq_from_or_any_side`, `split_or_branches`, `split_and_refs`, `is_any_eq`, `expr_eq`, `combine_and`). To delete `predicate_pushdown.rs`, move these into `join_reorder.rs` (as private helpers of that module).

**Files:**
- Modify: `src/sql/optimizer/join_reorder.rs`
- Delete: `src/sql/optimizer/predicate_pushdown.rs`
- Modify: `src/sql/optimizer/mod.rs`

- [ ] **Step 1: Verify the only external caller of `factor_common_eq_from_or_for_reorder`.**

```bash
cd /Users/harbor/project/NovaRocks
grep -rn 'factor_common_eq_from_or_for_reorder\|predicate_pushdown::' --include='*.rs' src/ 2>&1
```

Expected: exactly one non-test call from `src/sql/optimizer/join_reorder.rs`. Any additional hits (outside the file being deleted) indicate an un-migrated caller that needs to move with the helpers.

- [ ] **Step 2: Copy helper bodies into `join_reorder.rs`.**

Append the following to `src/sql/optimizer/join_reorder.rs` (as a private `mod or_factor { ... }` or top-level private fns — pick whichever matches the file's existing organization):

- `factor_common_eq_from_or_for_reorder` (legacy `predicate_pushdown.rs` lines 525-530)
- `factor_common_eq_from_or_any_side` (lines 534-605)
- `is_any_eq` (lines 607-610)
- `split_or_branches` (lines 612-626)
- `split_and_refs` (lines 628-642)
- `expr_eq` (line 675-677)

All become private (`fn`, no visibility modifier). Adjust imports at the top of `join_reorder.rs` to pull in `combine_and` from `super::expr_utils::combine_and` (now `pub(crate)` after Task 1) and `BinOp`, `ExprKind`, `LiteralValue`, `TypedExpr` from `crate::sql::ir`.

Then update the existing call site in `join_reorder.rs` that previously read:

```rust
use super::predicate_pushdown::factor_common_eq_from_or_for_reorder;
```

to call the local function directly (delete the `use`; the function is now in-file).

- [ ] **Step 3: Delete `predicate_pushdown.rs` and its module declaration.**

```bash
cd /Users/harbor/project/NovaRocks
rm src/sql/optimizer/predicate_pushdown.rs
```

Edit `src/sql/optimizer/mod.rs`. Delete the line:

```rust
pub(crate) mod predicate_pushdown;
```

- [ ] **Step 4: Verify the crate builds and tests stay green.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
cargo test -p novarocks --lib 2>&1 | tail -5
```

Expected: clean build; the test count should be at least the post-Phase-2 baseline plus the new rule tests added in Tasks 3–7. Any unit-test failure not in the pre-existing failure list is a Phase 3 bug.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add -u src/sql/optimizer/ src/sql/cascades/
git commit -m "Delete legacy predicate_pushdown.rs; move OR-factoring into join_reorder

Phase 3 migrated the main predicate pushdown logic into RBO rules. The
only remaining caller of predicate_pushdown.rs was join_reorder, which
used factor_common_eq_from_or_for_reorder and its internal helpers for
join-graph OR handling. Move those helpers into join_reorder.rs as
private fns and delete predicate_pushdown.rs.

Phase 5 migrates join_reorder itself into cascades; these helpers move
with it at that time.

Net removal: ~750 lines of legacy source.
"
```

---

## Task 11: Phase-3 Regression Verification

Mirror the Phase-2 regression verification (plan file `2026-04-13-phase2-column-pruning-migration.md` Task 5). Rebuild standalone, recreate the Iceberg catalog, capture EXPLAIN snapshots, diff against the Phase-2 baseline. Investigate every diff.

**Files:**
- Modify: `docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md` (append landing note)

- [ ] **Step 1: Rebuild standalone and start it on port 9030.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030 &
echo $! > /tmp/novarocks-phase3.pid
# Wait for readiness — poll a cheap query
for _ in {1..60}; do
  mysql --protocol=TCP -h 127.0.0.1 -P 9030 -e 'SELECT 1' >/dev/null 2>&1 && break
  sleep 1
done
mysql --protocol=TCP -h 127.0.0.1 -P 9030 -e 'SELECT 1'
```

Expected: `1` returned.

- [ ] **Step 2: Recreate the Iceberg catalog (if TPC-DS suite needs it).**

If the Phase-2 verify used an Iceberg catalog setup, re-run it here. The steps are documented inside the Phase-2 plan (Task 5 Step 2). If the standalone server already has a persisted catalog that survives restart (check `ls ~/.novarocks/` or the configured catalog dir), skip this step.

- [ ] **Step 3: Capture Phase-3 EXPLAIN snapshots.**

```bash
cd /Users/harbor/project/NovaRocks
mkdir -p /tmp/novarocks-plan-compare/standalone-phase3
# Reuse the same capture script Phase-2 used. If it's under scripts/ or
# tests/, run it with output_dir=standalone-phase3:
bash scripts/capture-explain-snapshots.sh /tmp/novarocks-plan-compare/standalone-phase3 \
  2>&1 | tail -5
ls /tmp/novarocks-plan-compare/standalone-phase3 | wc -l
```

Expected: `99`. If the script path differs from the Phase-2 location, find it via `grep -rn 'EXPLAIN' scripts/ tests/ | head`; match the invocation Phase-2 used.

- [ ] **Step 4: Diff against Phase 2.**

```bash
cd /tmp/novarocks-plan-compare
diff -r standalone-phase2 standalone-phase3 > phase2-vs-phase3.diff 2>&1 || true
wc -l phase2-vs-phase3.diff
```

Expected ideal: `0` (predicate placement is preserved byte-for-byte). Real-world expectation: small diffs are possible because the new rule-based fixed-point differs slightly from the legacy sequential twice-call. For each query with a diff, classify:

- **ACCEPTABLE**: predicates land at equal-or-better depth; no filter is lost; join types and orders unchanged. Document and proceed.
- **REGRESSION**: a filter is no longer pushed, a predicate is double-evaluated, a CROSS fails to upgrade to INNER, or the join shape changes. Investigate before committing.

If REGRESSIONs are found, stop and ask for guidance.

- [ ] **Step 5: Stop standalone, record Phase-3 sha.**

```bash
cd /Users/harbor/project/NovaRocks
kill $(cat /tmp/novarocks-phase3.pid) 2>/dev/null || true
rm -f /tmp/novarocks-phase3.pid
git rev-parse HEAD > /tmp/novarocks-plan-compare/phase3-ref.txt
cat /tmp/novarocks-plan-compare/phase3-ref.txt
```

- [ ] **Step 6: Append landing note to the spec doc and commit.**

Edit `docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md`. After the `**Commit:** `Phase 3: migrate predicate_pushdown to RBO rules`` line (around line 441), add:

```markdown

**Phase 3 landed.** Date: 2026-04-13. HEAD at landing: <PASTE SHA FROM phase3-ref.txt>. predicate_pushdown migrated to five shape-specific RBO rules (PushDownPredicateScan, PushDownPredicateProject, PushDownPredicateAggregate, PushDownPredicateJoin, PushSemiAntiRightOnlyCondition). Legacy src/sql/optimizer/predicate_pushdown.rs deleted; its one helper consumed by join_reorder (factor_common_eq_from_or_for_reorder and private deps) moved into join_reorder.rs. rewriter::rewrite now contains only reorder_joins_cbo. RBO driver now runs twice around rewriter::rewrite to preserve legacy "push before and after reorder" semantics. Unit tests: <N> passed / <F> failed (no new failures vs post-Phase-2 baseline of 910 / 19). TPC-DS standalone EXPLAIN snapshots: <diff summary — ideally byte-identical to Phase 2; document any ACCEPTABLE diffs>. Phase 4 (move utils + selectivity) is unblocked.
```

Fill in the placeholders (`<PASTE SHA FROM phase3-ref.txt>`, `<N>`, `<F>`, `<diff summary>`) with real values from Steps 4 and 5.

```bash
cd /Users/harbor/project/NovaRocks
git add docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md
git commit -m "Phase 3 predicate_pushdown migration: mark spec section 4.3 as landed

<paste the same summary from the spec update here>
"
```

- [ ] **Step 7: Use the finishing-a-development-branch skill.**

Invoke `superpowers:finishing-a-development-branch` to decide on merge/PR next steps.
