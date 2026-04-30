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
use crate::sql::optimizer::rbo::utils::{collect_column_refs, merge_needed};
use crate::sql::planner::plan::*;

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
                for item in &agg.order_by {
                    for col in collect_column_refs(&item.expr) {
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
                grouping_key_aliases: node.grouping_key_aliases,
                grouping_fn_args: node.grouping_fn_args,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
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
            iceberg_row_lineage_metadata_columns: vec![],
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
        let out = rule
            .apply(project)
            .expect("rule should fire and set required_columns");

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
        // Plan: Project[a] <- Filter[b = 'x'] <- Scan[a,b,c]
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
            aggregates: vec![crate::sql::planner::plan::AggregateCall {
                name: "sum".to_string(),
                args: vec![col_ref("c", DataType::Float64)],
                distinct: false,
                result_type: DataType::Float64,
                order_by: vec![],
            }],
            output_columns: vec![
                OutputColumn {
                    name: "b".to_string(),
                    data_type: DataType::Utf8,
                    nullable: true,
                },
                OutputColumn {
                    name: "sum_c".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                },
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
