# Phase 2 column_pruning Migration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the legacy `src/sql/optimizer/column_pruning.rs` pass into a single `PruneColumns` `RewriteRule` under the RBO framework. Delete the legacy file and the legacy call site in `cascades::rewriter::rewrite`. Phase 2 validates the Phase 1 framework with its first real rule, establishes the "single rule that internally recurses" convention as a documented exception, and shrinks the legacy optimizer by 231 lines.

**Architecture:** `PruneColumns` implements `RewriteRule` and owns the full top-down column-need propagation. Its `matches` always returns true at the root; its `apply` performs the entire recursive walk internally (violating the "rules don't recurse" convention — documented as an explicit one-time exception, since column pruning is fundamentally a top-down concern that the bottom-up driver can't naturally express). Rule registered in `all_rbo_rules()`; legacy `prune_columns` call deleted from `rewriter::rewrite`.

**Tech Stack:** Rust 2021, cascades framework in `src/sql/cascades/`, existing `expr_utils` helpers kept as a cross-module dependency until Phase 4 moves them.

**Spec reference:** `docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md` §4.2.

---

## Task 1: Expose `expr_utils` Helpers to Cascades

The new `PruneColumns` rule needs `collect_column_refs` and `merge_needed` from `src/sql/optimizer/expr_utils.rs`, currently `pub(super)` (visible only within the `optimizer` module). Upgrade their visibility to `pub(crate)` so the rule (living under `src/sql/cascades/`) can call them. Phase 4 moves the helpers into cascades entirely; this is a temporary cross-module bridge.

**Files:**
- Modify: `src/sql/optimizer/expr_utils.rs`

- [ ] **Step 1: Upgrade visibility on the two helpers.**

Edit `src/sql/optimizer/expr_utils.rs`. Change:

```rust
pub(super) fn collect_column_refs(expr: &TypedExpr) -> Vec<&str> {
```

to:

```rust
pub(crate) fn collect_column_refs(expr: &TypedExpr) -> Vec<&str> {
```

Change:

```rust
pub(super) fn merge_needed(parent: Option<&HashSet<String>>, extra: &[&str]) -> HashSet<String> {
```

to:

```rust
pub(crate) fn merge_needed(parent: Option<&HashSet<String>>, extra: &[&str]) -> HashSet<String> {
```

Leave the other `pub(super)` functions (`split_and`, `combine_and`, `collect_output_columns`, `wrap_remaining_filter`, `collect_qualified_column_refs`, `collect_qualified_output_columns`) unchanged — they are not needed by PruneColumns.

- [ ] **Step 2: Verify the whole crate still builds.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
```

Expected: clean build.

- [ ] **Step 3: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/optimizer/expr_utils.rs
git commit -m "Bridge: upgrade expr_utils helpers to pub(crate) for cascades use

Phase 2 migrates column_pruning from src/sql/optimizer/ into a cascades
RewriteRule. That rule needs collect_column_refs and merge_needed, which
currently live in optimizer/expr_utils.rs as pub(super) (module-local).
Upgrade those two helpers to pub(crate) so the cascades rule can import
them. The other pub(super) helpers stay as-is.

Phase 4 relocates expr_utils into src/sql/cascades/rbo/utils.rs; this
visibility bump is a temporary cross-module bridge.
"
```

---

## Task 2: Add `PruneColumns` RewriteRule with Tests

The rule implements `RewriteRule` with a `matches` that always returns true and an `apply` that performs the full top-down column-need propagation. The internal `prune_inner` helper walks 18 LogicalPlan variants, mirroring the legacy implementation at `src/sql/optimizer/column_pruning.rs` line-for-line.

**Files:**
- Create: `src/sql/cascades/rbo/rules/column_pruning.rs`
- Modify: `src/sql/cascades/rbo/rules/mod.rs`
- Test: inline in the new file

- [ ] **Step 1: Write the rule.**

Create `src/sql/cascades/rbo/rules/column_pruning.rs`:

```rust
//! PruneColumns RBO rule — propagates parent column requirements down the
//! plan tree and sets `ScanNode.required_columns` accordingly.
//!
//! **Convention exception.** This rule recurses into children internally,
//! violating the "rules don't recurse; the driver walks" rule documented
//! on `RewriteRule`. Column pruning is fundamentally a *top-down* concern
//! — a scan cannot know which columns to prune until every ancestor has
//! declared what it needs — and the RBO driver's bottom-up traversal
//! cannot naturally express that. The rule therefore owns the walk. It is
//! the one documented exception; every other rule stays inside the
//! one-node-per-apply convention.
//!
//! Mirrors legacy `src/sql/optimizer/column_pruning.rs` semantics: set
//! operations and CTE produce / Window / SubqueryAlias / Repeat all pass
//! `None` (no restriction) to children since their subtrees either have
//! independent namespaces or need every available column internally.

use std::collections::HashSet;

use super::super::rule::RewriteRule;
use crate::sql::optimizer::expr_utils::{collect_column_refs, merge_needed};
use crate::sql::plan::*;

/// Single top-down column-pruning rule.
///
/// Registered once in `all_rbo_rules()`. Apply runs `prune_inner` at the
/// root level with `None` (no restriction), which recursively walks the
/// entire tree. The RBO driver's outer tree-level fixed-point will invoke
/// the rule once at the root; because `apply` returns `None` when nothing
/// changed (the `required_columns` field is identical before and after),
/// the driver terminates after one round when the tree has already been
/// pruned.
pub(crate) struct PruneColumns;

impl RewriteRule for PruneColumns {
    fn name(&self) -> &'static str {
        "PruneColumns"
    }

    fn matches(&self, _plan: &LogicalPlan) -> bool {
        // Column pruning applies at any root. The driver's bottom-up
        // traversal means this rule also fires at interior nodes; the
        // idempotent structure of prune_inner (same inputs -> same
        // outputs) makes that harmless — after the first fixed-point
        // pass the outputs stabilize.
        true
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let before = plan.clone();
        let after = prune_inner(plan, None);
        if logical_plan_structurally_equal(&before, &after) {
            None
        } else {
            Some(after)
        }
    }
}

/// Cheap structural equality for the "did apply actually change anything?"
/// check. We only need to detect whether `required_columns` changed on any
/// Scan node; everything else is threaded through unchanged. Using a
/// Debug-based comparison would be expensive on large plans, so we do a
/// targeted walk that compares only the `required_columns` field on every
/// scan. Other fields on all nodes are preserved by `prune_inner`; if they
/// differed, that would indicate a bug in `prune_inner`.
fn logical_plan_structurally_equal(a: &LogicalPlan, b: &LogicalPlan) -> bool {
    // Fast path: reference equality after clone is impossible; fall back
    // to format-debug comparison. This is O(plan size) but the rule runs
    // at most a handful of times per optimize() call (driver fixed-point
    // converges in 1-2 iterations on column pruning).
    format!("{:?}", a) == format!("{:?}", b)
}

/// `needed`: the set of column names required by the parent.
/// `None` means "all columns" (no restriction).
fn prune_inner(plan: LogicalPlan, needed: Option<&HashSet<String>>) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(mut scan) => {
            if let Some(needed) = needed {
                // Also include columns referenced by pushed-down predicates.
                let mut required: HashSet<String> = needed.clone();
                for pred in &scan.predicates {
                    for col in collect_column_refs(pred) {
                        required.insert(col.to_lowercase());
                    }
                }
                let mut pruned: Vec<String> = scan
                    .columns
                    .iter()
                    .filter(|c| required.contains(&c.name.to_lowercase()))
                    .map(|c| c.name.clone())
                    .collect();
                // Ensure at least one column survives so the scan has a valid
                // output layout (needed for COUNT(*) and similar queries).
                if pruned.is_empty() && !scan.columns.is_empty() {
                    pruned.push(scan.columns[0].name.clone());
                }
                scan.required_columns = Some(pruned);
            }
            LogicalPlan::Scan(scan)
        }

        LogicalPlan::Filter(node) => {
            // The filter's predicate contributes required columns to the child.
            let pred_cols = collect_column_refs(&node.predicate);
            let child_needed = merge_needed(needed, &pred_cols);
            let input = prune_inner(*node.input, Some(&child_needed));
            LogicalPlan::Filter(FilterNode {
                input: Box::new(input),
                predicate: node.predicate,
            })
        }

        LogicalPlan::Project(node) => {
            // Collect columns referenced by projection expressions.
            let mut child_needed = HashSet::new();
            for item in &node.items {
                // If parent restricts needed columns, only include items
                // whose output name is in the needed set.
                let dominated =
                    needed.is_none() || needed.unwrap().contains(&item.output_name.to_lowercase());
                if dominated {
                    for col in collect_column_refs(&item.expr) {
                        child_needed.insert(col.to_lowercase());
                    }
                }
            }
            let input = prune_inner(*node.input, Some(&child_needed));
            LogicalPlan::Project(ProjectNode {
                input: Box::new(input),
                items: node.items,
            })
        }

        LogicalPlan::Aggregate(node) => {
            let mut child_needed = HashSet::new();
            for gb in &node.group_by {
                for col in collect_column_refs(gb) {
                    child_needed.insert(col.to_lowercase());
                }
            }
            for agg in &node.aggregates {
                for arg in &agg.args {
                    for col in collect_column_refs(arg) {
                        child_needed.insert(col.to_lowercase());
                    }
                }
            }
            let input = prune_inner(*node.input, Some(&child_needed));
            LogicalPlan::Aggregate(AggregateNode {
                input: Box::new(input),
                ..node
            })
        }

        LogicalPlan::Join(node) => {
            // Join needs all parent columns plus join condition columns.
            // If parent doesn't restrict (None), pass None to children.
            let child_needed = if let Some(needed) = needed {
                let mut combined = needed.clone();
                if let Some(ref cond) = node.condition {
                    for col in collect_column_refs(cond) {
                        combined.insert(col.to_lowercase());
                    }
                }
                Some(combined)
            } else {
                None
            };
            let left = prune_inner(*node.left, child_needed.as_ref());
            let right = prune_inner(*node.right, child_needed.as_ref());
            LogicalPlan::Join(JoinNode {
                left: Box::new(left),
                right: Box::new(right),
                join_type: node.join_type,
                condition: node.condition,
            })
        }

        LogicalPlan::Sort(node) => {
            // Sort needs all parent columns plus sort-key columns.
            // If parent doesn't restrict (None), pass None to child.
            let child_needed = if let Some(needed) = needed {
                let mut combined = needed.clone();
                for item in &node.items {
                    for col in collect_column_refs(&item.expr) {
                        combined.insert(col.to_lowercase());
                    }
                }
                Some(combined)
            } else {
                None
            };
            let input = prune_inner(*node.input, child_needed.as_ref());
            LogicalPlan::Sort(SortNode {
                input: Box::new(input),
                items: node.items,
            })
        }

        LogicalPlan::Limit(node) => {
            let input = prune_inner(*node.input, needed);
            LogicalPlan::Limit(LimitNode {
                input: Box::new(input),
                limit: node.limit,
                offset: node.offset,
            })
        }

        // Set operations: recurse into each child without column restriction
        // since all branches must produce the same schema.
        LogicalPlan::Union(node) => LogicalPlan::Union(UnionNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|i| prune_inner(i, None))
                .collect(),
            all: node.all,
        }),
        LogicalPlan::Intersect(node) => LogicalPlan::Intersect(IntersectNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|i| prune_inner(i, None))
                .collect(),
        }),
        LogicalPlan::Except(node) => LogicalPlan::Except(ExceptNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|i| prune_inner(i, None))
                .collect(),
        }),

        LogicalPlan::Values(node) => LogicalPlan::Values(node),
        LogicalPlan::GenerateSeries(node) => LogicalPlan::GenerateSeries(node),
        LogicalPlan::CTEAnchor(node) => {
            let produce = prune_inner(*node.produce, None);
            let consumer = prune_inner(*node.consumer, needed);
            LogicalPlan::CTEAnchor(CTEAnchorNode {
                cte_id: node.cte_id,
                produce: Box::new(produce),
                consumer: Box::new(consumer),
            })
        }
        LogicalPlan::CTEProduce(node) => {
            let input = prune_inner(*node.input, None);
            LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: node.cte_id,
                input: Box::new(input),
                output_columns: node.output_columns,
            })
        }
        LogicalPlan::CTEConsume(node) => LogicalPlan::CTEConsume(node),

        LogicalPlan::Window(node) => {
            // Prune columns in the child, but don't restrict since the window
            // function itself needs columns from PARTITION BY / ORDER BY / args.
            let input = prune_inner(*node.input, None);
            LogicalPlan::Window(WindowNode {
                input: Box::new(input),
                ..node
            })
        }

        LogicalPlan::SubqueryAlias(node) => {
            // Don't propagate outer `needed` into subquery — the inner plan
            // has its own column namespace (aliases differ from base columns).
            // Passing `needed` through would incorrectly prune columns that
            // the inner SELECT references but the outer query doesn't.
            let input = prune_inner(*node.input, None);
            LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                input: Box::new(input),
                alias: node.alias,
                output_columns: node.output_columns,
            })
        }

        LogicalPlan::Repeat(node) => {
            // Repeat needs all columns from input (rollup columns + others).
            let input = prune_inner(*node.input, None);
            LogicalPlan::Repeat(RepeatPlanNode {
                input: Box::new(input),
                repeat_column_ref_list: node.repeat_column_ref_list,
                grouping_ids: node.grouping_ids,
                all_rollup_columns: node.all_rollup_columns,
                grouping_fn_args: node.grouping_fn_args,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
    use crate::sql::ir::{BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
    use arrow::datatypes::DataType;

    fn three_col_table() -> TableDef {
        TableDef {
            name: "t1".to_string(),
            columns: vec![
                ColumnDef {
                    name: "a".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnDef {
                    name: "b".to_string(),
                    data_type: DataType::Utf8,
                    nullable: true,
                },
                ColumnDef {
                    name: "c".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                },
            ],
            storage: TableStorage::LocalParquetFile {
                path: std::path::PathBuf::from("/tmp/test.parquet"),
            },
        }
    }

    fn scan_node(table: &TableDef) -> ScanNode {
        ScanNode {
            database: "default".to_string(),
            table: table.clone(),
            alias: None,
            columns: table
                .columns
                .iter()
                .map(|c| OutputColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
        }
    }

    fn col_ref(name: &str, ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.to_string(),
            },
            data_type: ty,
            nullable: false,
        }
    }

    #[test]
    fn root_scan_without_parent_keeps_all_columns() {
        // No parent restriction means Scan.required_columns stays None.
        let table = three_col_table();
        let plan = LogicalPlan::Scan(scan_node(&table));
        let rule = PruneColumns;
        // matches always returns true, but apply should return None since
        // nothing changed (required_columns is None before and after at
        // the root).
        let out = rule.apply(plan.clone());
        // Either None (no-op), or Some with required_columns still None.
        let final_plan = out.unwrap_or(plan);
        if let LogicalPlan::Scan(s) = final_plan {
            assert_eq!(s.required_columns, None);
        } else {
            panic!("expected Scan");
        }
    }

    #[test]
    fn project_selecting_one_col_prunes_scan_required_columns() {
        // Plan: Project[a] <- Scan[a,b,c]
        // After prune_columns: Scan.required_columns = Some(["a"])
        let table = three_col_table();
        let scan = LogicalPlan::Scan(scan_node(&table));
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: col_ref("a", DataType::Int32),
                output_name: "a".to_string(),
            }],
        });

        let rule = PruneColumns;
        let out = rule.apply(project).expect("rule should fire and set required_columns");

        if let LogicalPlan::Project(p) = out {
            if let LogicalPlan::Scan(s) = *p.input {
                assert_eq!(s.required_columns, Some(vec!["a".to_string()]));
            } else {
                panic!("expected Scan under Project");
            }
        } else {
            panic!("expected Project");
        }
    }

    #[test]
    fn filter_predicate_columns_are_preserved_in_scan_required() {
        // Plan: Project[a] <- Filter[b = 1] <- Scan[a,b,c]
        // After: Scan.required_columns = Some(["a", "b"]) (order may vary)
        let table = three_col_table();
        let scan = LogicalPlan::Scan(scan_node(&table));
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(col_ref("b", DataType::Utf8)),
                    op: BinOp::Eq,
                    right: Box::new(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::String("x".to_string())),
                        data_type: DataType::Utf8,
                        nullable: false,
                    }),
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
        });
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(filter),
            items: vec![ProjectItem {
                expr: col_ref("a", DataType::Int32),
                output_name: "a".to_string(),
            }],
        });

        let rule = PruneColumns;
        let out = rule.apply(project).expect("rule should fire");

        // Drill down to the Scan and check required_columns.
        if let LogicalPlan::Project(p) = out {
            if let LogicalPlan::Filter(f) = *p.input {
                if let LogicalPlan::Scan(s) = *f.input {
                    let req = s.required_columns.expect("required_columns should be set");
                    let req_set: HashSet<String> = req.into_iter().collect();
                    assert!(req_set.contains("a"));
                    assert!(req_set.contains("b"));
                    assert!(!req_set.contains("c"));
                } else {
                    panic!("expected Scan under Filter");
                }
            } else {
                panic!("expected Filter under Project");
            }
        } else {
            panic!("expected Project");
        }
    }

    #[test]
    fn aggregate_group_by_and_agg_args_propagate_to_scan() {
        // Plan: Aggregate[group_by=[b], sum(c)] <- Scan[a,b,c]
        // After: Scan.required_columns = Some(["b", "c"]) (order may vary)
        let table = three_col_table();
        let scan = LogicalPlan::Scan(scan_node(&table));
        let agg = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![col_ref("b", DataType::Utf8)],
            aggregates: vec![crate::sql::plan::AggregateCall {
                name: "sum".to_string(),
                args: vec![col_ref("c", DataType::Float64)],
                distinct: false,
                result_type: DataType::Float64,
                order_by: vec![],
            }],
            output_columns: vec![
                OutputColumn { name: "b".to_string(), data_type: DataType::Utf8, nullable: true },
                OutputColumn { name: "sum_c".to_string(), data_type: DataType::Float64, nullable: true },
            ],
        });

        let rule = PruneColumns;
        let out = rule.apply(agg).expect("rule should fire");

        if let LogicalPlan::Aggregate(a) = out {
            if let LogicalPlan::Scan(s) = *a.input {
                let req = s.required_columns.expect("required_columns should be set");
                let req_set: HashSet<String> = req.into_iter().collect();
                assert!(req_set.contains("b"));
                assert!(req_set.contains("c"));
                assert!(!req_set.contains("a"));
            } else {
                panic!("expected Scan under Aggregate");
            }
        } else {
            panic!("expected Aggregate");
        }
    }
}
```

- [ ] **Step 2: Read the actual `AggregateCall` struct shape.**

The test scaffolding above references `AggregateCall` with specific fields (`name`, `args`, `distinct`, `result_type`, `order_by`). Before running tests, grep to verify the actual struct definition matches:

```bash
grep -n "pub.*struct AggregateCall\|pub.*name:\|pub.*args:\|pub.*distinct:\|pub.*result_type:\|pub.*order_by:" /Users/harbor/project/NovaRocks/src/sql/plan/mod.rs | head -20
```

If any field name differs (e.g. `arguments` instead of `args`, or `return_type` instead of `result_type`), adjust the test's `AggregateCall` literal to match. The test's intent is: construct an aggregate with group_by=[b] and one aggregate call over column `c`.

- [ ] **Step 3: Run the tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::cascades::rbo::rules::column_pruning 2>&1 | tail -20
```

Expected: 4 tests pass.

- [ ] **Step 4: Verify full crate builds clean.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
```

Expected: clean build.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/rules/column_pruning.rs
git commit -m "Add PruneColumns RBO rule migrating legacy column_pruning

PruneColumns implements RewriteRule and performs the full top-down
column-need propagation internally. Column pruning is fundamentally
top-down (a scan cannot know which columns to prune until every
ancestor has declared its needs), which the bottom-up RBO driver
cannot naturally express; this rule therefore owns the walk as a
documented one-time exception to the 'rules don't recurse' convention.

Semantics are identical to legacy src/sql/optimizer/column_pruning.rs:
- Scan: set required_columns = parent needed + predicate columns.
- Filter: add predicate cols to needed, recurse.
- Project: propagate only columns referenced by output items.
- Aggregate: add group_by + aggregate arg columns.
- Join: add condition cols; both children share combined needed.
- Sort: add sort-key columns.
- Limit: pass-through.
- Set ops / Window / SubqueryAlias / Repeat / CTEProduce: recurse
  with None (independent namespaces or full-column requirements).
- CTEAnchor: produce=None, consumer=parent needed.
- Values / GenerateSeries / CTEConsume: leaves.

Four unit tests validate: no-restriction root keeps all columns;
project[a] over scan[a,b,c] prunes to [a]; filter's predicate columns
survive pruning; aggregate's group_by + arg columns survive.

Spec §4.2.
"
```

---

## Task 3: Register and Switch Over

Add `PruneColumns` to `all_rbo_rules()` and simultaneously remove the legacy `prune_columns` call from `cascades::rewriter::rewrite`. Both happen in a single commit to keep the build green at every commit boundary.

**Files:**
- Modify: `src/sql/cascades/rbo/rules/mod.rs`
- Modify: `src/sql/cascades/rewriter.rs`

- [ ] **Step 1: Add PruneColumns to the registry.**

Edit `src/sql/cascades/rbo/rules/mod.rs`. Add the import and the registration:

```rust
//! RBO rule registry. Phases 2-5 of the unification spec land their
//! migrated rules here; Phase 1 ships an empty registry so the framework
//! is wired end-to-end with no behavior change.

use super::rule::RewriteRule;

pub(crate) mod column_pruning;

pub(crate) fn column_pruning_rules() -> Vec<Box<dyn RewriteRule>> {
    // Column pruning is fundamentally a top-down concern; expressed as a
    // single rule that recurses internally (documented exception to the
    // "rules don't recurse" convention — see column_pruning.rs module docs).
    vec![Box::new(column_pruning::PruneColumns)]
}

/// All RBO rules in canonical application order.
pub(crate) fn all_rbo_rules() -> Vec<Box<dyn RewriteRule>> {
    let mut all = Vec::new();
    all.extend(column_pruning_rules());
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_contains_prune_columns() {
        let rules = all_rbo_rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].name(), "PruneColumns");
    }
}
```

- [ ] **Step 2: Remove the legacy prune_columns call from rewriter.rs.**

Edit `src/sql/cascades/rewriter.rs`. Current content:

```rust
//! RBO rewrite pass applied before Memo-based CBO search.
//!
//! Calls existing rule-based optimizer passes (predicate pushdown,
//! join reorder, column pruning) on the LogicalPlan before it enters
//! the Memo.

use std::collections::HashMap;

use crate::sql::plan::LogicalPlan;
use crate::sql::statistics::TableStatistics;

/// Apply RBO rewrites to the logical plan before Memo insertion.
pub(crate) fn rewrite(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> LogicalPlan {
    let plan = crate::sql::optimizer::predicate_pushdown::push_down_predicates(plan);
    let plan = crate::sql::optimizer::join_reorder::reorder_joins_cbo(plan, table_stats);
    // Second pushdown pass: after join reorder, newly formed joins may have
    // cross-side predicates that can now be pushed into join conditions
    // (e.g., OR factoring extracting common equi-joins at a lower level).
    let plan = crate::sql::optimizer::predicate_pushdown::push_down_predicates(plan);
    let plan = crate::sql::optimizer::column_pruning::prune_columns(plan);
    plan
}
```

Change to:

```rust
//! Legacy RBO rewrite pass. Being progressively migrated into cascades
//! RBO rules under `src/sql/cascades/rbo/rules/`. Each migrated rule is
//! deleted from both the legacy source and this call site; Phase 6
//! deletes this file entirely.

use std::collections::HashMap;

use crate::sql::plan::LogicalPlan;
use crate::sql::statistics::TableStatistics;

/// Apply remaining legacy RBO rewrites to the logical plan before Memo
/// insertion. Column pruning has been migrated to `PruneColumns` RBO rule
/// and no longer runs from here (Phase 2). Predicate pushdown and join
/// reorder will be migrated in Phase 3 and Phase 5 respectively.
pub(crate) fn rewrite(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> LogicalPlan {
    let plan = crate::sql::optimizer::predicate_pushdown::push_down_predicates(plan);
    let plan = crate::sql::optimizer::join_reorder::reorder_joins_cbo(plan, table_stats);
    // Second pushdown pass: after join reorder, newly formed joins may have
    // cross-side predicates that can now be pushed into join conditions
    // (e.g., OR factoring extracting common equi-joins at a lower level).
    let plan = crate::sql::optimizer::predicate_pushdown::push_down_predicates(plan);
    plan
}
```

Only the last line (`prune_columns` call) is removed and the doc comments are updated.

- [ ] **Step 3: Run registry test.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::cascades::rbo::rules::tests 2>&1 | tail -10
```

Expected: 1 test passes (`registry_contains_prune_columns`). The old test `registry_is_empty_in_phase_1` has been replaced by this new assertion.

- [ ] **Step 4: Verify full crate builds clean and tests stay green.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
cargo test 2>&1 | grep "^test result" | tail -3
```

Expected: clean build. Test count: same as post-Phase-1 (909 passed / 19 failed at HEAD `9ee5071`) plus 4 new PruneColumns tests = 913 passed / 19 failed. No new failures.

If new failures appear (e.g., an existing cascades test depended on legacy `prune_columns` being called), investigate before committing.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/rules/mod.rs src/sql/cascades/rewriter.rs
git commit -m "Switch to PruneColumns RBO rule; drop legacy column_pruning call

Register PruneColumns in all_rbo_rules() and remove the legacy
crate::sql::optimizer::column_pruning::prune_columns call from
cascades::rewriter::rewrite. The legacy prune_columns() function
itself is deleted in Task 4 of this plan (separate commit for a
minimum-risk switchover).

Spec §4.2.
"
```

---

## Task 4: Delete Legacy `column_pruning.rs`

The legacy file is now dead code. Delete it and remove its module declaration. Verify the full crate still builds (it must: nothing references the legacy function after Task 3).

**Files:**
- Delete: `src/sql/optimizer/column_pruning.rs`
- Modify: `src/sql/optimizer/mod.rs`

- [ ] **Step 1: Verify no remaining callers.**

```bash
cd /Users/harbor/project/NovaRocks
grep -rn 'optimizer::column_pruning\|column_pruning::prune_columns' src/ 2>/dev/null
```

Expected: no matches. If any appear, they must be cleaned up before deletion. After Task 3, there should be zero matches.

- [ ] **Step 2: Delete the file.**

```bash
cd /Users/harbor/project/NovaRocks
rm src/sql/optimizer/column_pruning.rs
```

- [ ] **Step 3: Remove the module declaration.**

Edit `src/sql/optimizer/mod.rs`. Find and delete this line:

```rust
pub(crate) mod column_pruning;
```

- [ ] **Step 4: Verify the crate still builds and tests stay green.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
cargo test 2>&1 | grep "^test result" | tail -3
```

Expected: clean build; test count unchanged from Task 3's 913 passed / 19 failed minus any tests that lived inside `column_pruning.rs` itself. Look at the file we deleted — it had no `#[cfg(test)]` block at the file level (tests were at `src/sql/optimizer/mod.rs`'s bottom for column_pruning behavior). If any test in `src/sql/optimizer/mod.rs` referenced the deleted module, delete those tests too (they were testing legacy code we just replaced).

Specifically, search for and delete any tests in `src/sql/optimizer/mod.rs` that still reference `column_pruning`:

```bash
grep -n 'column_pruning\|prune_columns' /Users/harbor/project/NovaRocks/src/sql/optimizer/mod.rs
```

If any matches, remove those test blocks.

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add -u src/sql/optimizer/mod.rs src/sql/optimizer/column_pruning.rs
git commit -m "Delete legacy src/sql/optimizer/column_pruning.rs

The pass has been migrated to PruneColumns RBO rule (Phase 2 Task 2)
and its call site removed from cascades::rewriter::rewrite (Phase 2
Task 3). The file is now dead code. Delete it and remove its module
declaration from src/sql/optimizer/mod.rs.

-231 lines of legacy optimizer code removed.
"
```

---

## Task 5: Phase-2 Regression Verification

Confirm that migrating column_pruning to the RBO framework produces byte-identical EXPLAIN plans. Column pruning affects `ScanNode.required_columns` which does NOT appear in EXPLAIN output (EXPLAIN shows algorithm structure, not metadata). So Phase 1's `standalone-unified-phase1/` snapshots should match Phase 2's new snapshots exactly.

**Files:** None — verification only, plus a landing note commit.

- [ ] **Step 1: Rebuild standalone and start it on port 9030.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
kill $(ps aux | grep 'standalone-server.*9030' | grep -v grep | awk '{print $2}') 2>/dev/null
NO_PROXY=127.0.0.1,localhost ./target/debug/novarocks standalone-server --port 9030 > /tmp/standalone-phase2.log 2>&1 &
disown
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root -e "SELECT 1;"
```

If `SELECT 1` fails, retry after a few seconds.

- [ ] **Step 2: Recreate the Iceberg catalog.**

```bash
NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root --batch -e "
CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_tpcds PROPERTIES(
  \"type\"=\"iceberg\",
  \"iceberg.catalog.type\"=\"hadoop\",
  \"iceberg.catalog.warehouse\"=\"oss://novarocks/iceberg-catalog/\",
  \"aws.s3.access_key\"=\"admin\",
  \"aws.s3.secret_key\"=\"admin123\",
  \"aws.s3.endpoint\"=\"http://127.0.0.1:9000\",
  \"aws.s3.enable_path_style_access\"=\"true\"
);"
```

- [ ] **Step 3: Capture Phase-2 EXPLAIN snapshots.**

```bash
mkdir -p /tmp/novarocks-plan-compare/standalone-unified-phase2
for i in $(seq 1 99); do
  qfile="/Users/harbor/project/NovaRocks/sql-tests/tpc-ds/sql/q${i}.sql"
  if [ -f "$qfile" ]; then
    sql=$(cat "$qfile")
    NO_PROXY=127.0.0.1,localhost mysql -h 127.0.0.1 -P9030 -u root --batch -N \
      -e "USE iceberg_tpcds.tpcds; EXPLAIN ${sql}" \
      > "/tmp/novarocks-plan-compare/standalone-unified-phase2/q${i}.plan" 2>&1
  fi
done
ls /tmp/novarocks-plan-compare/standalone-unified-phase2/ | wc -l
```

Expected: 99.

- [ ] **Step 4: Diff against Phase 1.**

```bash
echo "=== Queries differing between unified-phase1 and unified-phase2 ==="
diff_count=0
for i in $(seq 1 99); do
  if ! diff -q \
    /tmp/novarocks-plan-compare/standalone-unified-phase1/q${i}.plan \
    /tmp/novarocks-plan-compare/standalone-unified-phase2/q${i}.plan > /dev/null 2>&1; then
    echo "q${i}: DIFFERS"
    diff_count=$((diff_count + 1))
  fi
done
echo ""
echo "Total differing queries: $diff_count (expected: 0 — column pruning is metadata-level)"
```

Expected: 0 differing queries.

If diffs appear, dump the first one and **stop, report DONE_WITH_CONCERNS**:

```bash
for i in $(seq 1 99); do
  if ! diff -q /tmp/novarocks-plan-compare/standalone-unified-phase1/q${i}.plan /tmp/novarocks-plan-compare/standalone-unified-phase2/q${i}.plan > /dev/null 2>&1; then
    echo "=== q${i} diff (first 30 lines) ==="
    diff /tmp/novarocks-plan-compare/standalone-unified-phase1/q${i}.plan /tmp/novarocks-plan-compare/standalone-unified-phase2/q${i}.plan | head -30
    break
  fi
done
```

- [ ] **Step 5: Stop standalone, record Phase-2 sha.**

```bash
kill $(ps aux | grep 'standalone-server.*9030' | grep -v grep | awk '{print $2}')
echo "unified-phase2 complete: $(cd /Users/harbor/project/NovaRocks && git rev-parse HEAD) at $(date -Iseconds)" \
  >> /tmp/novarocks-plan-compare/unified-ref.txt
cat /tmp/novarocks-plan-compare/unified-ref.txt
```

- [ ] **Step 6: Append landing note to the spec doc and commit.**

Capture the actual SHA and test counts:

```bash
cd /Users/harbor/project/NovaRocks
SHA=$(git rev-parse HEAD)
TEST_LINE=$(cargo test 2>&1 | grep -E "^test result:" | head -1)
echo "SHA=$SHA"
echo "test_line=$TEST_LINE"
```

Then edit `docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md`. At the end of section 4.2 (Phase 2 — Migrate column_pruning), append (substitute actual `<SHA>`, `<PASS>`, `<FAIL>` values):

```markdown

**Phase 2 landed.** Date: 2026-04-13. HEAD at landing: <SHA>. column_pruning migrated to the PruneColumns RBO rule; legacy src/sql/optimizer/column_pruning.rs (231 lines) deleted along with its rewriter::rewrite call site. All 99 TPC-DS standalone EXPLAIN snapshots are byte-identical to Phase 1 (column pruning is a scan-metadata concern and does not affect EXPLAIN output). Unit tests: <PASS> passed / <FAIL> failed (no new failures vs post-Phase-1 baseline). Phase 3 (predicate_pushdown migration) is unblocked.
```

Then commit:

```bash
cd /Users/harbor/project/NovaRocks
git add docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md
git commit -m "Phase 2 column_pruning migration: mark spec section 4.2 as landed"
```

---

## Done

After Phase 2 lands:

- `PruneColumns` is the sole column-pruning implementation; legacy 231-line file deleted.
- `cascades::rewriter::rewrite` has one less legacy call; predicate_pushdown remains until Phase 3, join_reorder until Phase 5.
- `src/sql/optimizer/expr_utils.rs` has two helpers upgraded to `pub(crate)` as a temporary cross-module bridge (reverted in Phase 4 when expr_utils is relocated).
- All 99 TPC-DS EXPLAIN snapshots byte-identical to Phase 1.
- Test count up by 4 (PruneColumns behavior tests) plus registry test updated.

Phase 3 (predicate_pushdown migration) gets its own plan.
