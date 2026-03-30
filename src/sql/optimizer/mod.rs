//! Logical plan optimizer.
//!
//! Currently supports three rule-based optimizations (applied in order):
//!   1. **Predicate pushdown** — moves Filter predicates closer to Scan nodes.
//!   2. **Join reorder** — swaps build/probe sides so the smaller relation is
//!      built into the hash table (right/build) and the larger relation probes
//!      (left/probe).
//!   3. **Column pruning** — removes unreferenced columns from Scan nodes.
//!
//! The optimizer operates on the [`LogicalPlan`] tree before it is handed to
//! the Thrift emitter.

mod column_pruning;
pub(crate) mod expr_utils;
mod join_reorder;
mod predicate_pushdown;

use crate::sql::plan::*;

/// Apply all optimization rules to the logical plan.
///
/// `table_stats` provides per-table statistics from Iceberg metadata.
/// Currently passed through for future use by DP-based join reorder;
/// the existing heuristic-based reorder does not use it yet.
pub(crate) fn optimize(
    plan: LogicalPlan,
    _table_stats: &std::collections::HashMap<String, crate::sql::statistics::TableStatistics>,
) -> LogicalPlan {
    let plan = predicate_pushdown::push_down_predicates(plan);
    let plan = join_reorder::reorder_joins(plan);
    let plan = column_pruning::prune_columns(plan);
    plan
}

/// Apply a function to all direct children of a LogicalPlan node.
pub(super) fn map_children(plan: LogicalPlan, f: fn(LogicalPlan) -> LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(_) | LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) => plan,
        LogicalPlan::Window(n) => LogicalPlan::Window(WindowNode {
            input: Box::new(f(*n.input)),
            ..n
        }),
        LogicalPlan::Filter(n) => LogicalPlan::Filter(FilterNode {
            input: Box::new(f(*n.input)),
            predicate: n.predicate,
        }),
        LogicalPlan::Project(n) => LogicalPlan::Project(ProjectNode {
            input: Box::new(f(*n.input)),
            items: n.items,
        }),
        LogicalPlan::Aggregate(n) => LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(f(*n.input)),
            ..n
        }),
        LogicalPlan::Join(n) => LogicalPlan::Join(JoinNode {
            left: Box::new(f(*n.left)),
            right: Box::new(f(*n.right)),
            join_type: n.join_type,
            condition: n.condition,
        }),
        LogicalPlan::Sort(n) => LogicalPlan::Sort(SortNode {
            input: Box::new(f(*n.input)),
            items: n.items,
        }),
        LogicalPlan::Limit(n) => LogicalPlan::Limit(LimitNode {
            input: Box::new(f(*n.input)),
            limit: n.limit,
            offset: n.offset,
        }),
        LogicalPlan::Union(n) => LogicalPlan::Union(UnionNode {
            inputs: n.inputs.into_iter().map(f).collect(),
            all: n.all,
        }),
        LogicalPlan::Intersect(n) => LogicalPlan::Intersect(IntersectNode {
            inputs: n.inputs.into_iter().map(f).collect(),
        }),
        LogicalPlan::Except(n) => LogicalPlan::Except(ExceptNode {
            inputs: n.inputs.into_iter().map(f).collect(),
        }),
        LogicalPlan::SubqueryAlias(n) => LogicalPlan::SubqueryAlias(SubqueryAliasNode {
            input: Box::new(f(*n.input)),
            alias: n.alias,
            output_columns: n.output_columns,
        }),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
    use crate::sql::ir::{BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
    use arrow::datatypes::DataType;

    fn test_table() -> TableDef {
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

    fn col_ref(name: &str, dt: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.to_string(),
            },
            data_type: dt,
            nullable: false,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn eq_pred(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
        }
    }

    #[test]
    fn predicate_pushdown_filter_over_scan() {
        let table = test_table();
        let scan = LogicalPlan::Scan(scan_node(&table));
        let pred = eq_pred(col_ref("a", DataType::Int32), int_lit(1));
        let plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: pred.clone(),
        });

        let optimized = optimize(plan, &std::collections::HashMap::new());

        // Filter should be eliminated; predicate should be on the scan
        match &optimized {
            LogicalPlan::Scan(scan) => {
                assert_eq!(scan.predicates.len(), 1);
            }
            other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
        }
    }

    #[test]
    fn column_pruning_project_over_scan() {
        let table = test_table();
        let scan = LogicalPlan::Scan(scan_node(&table));
        let proj = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: col_ref("a", DataType::Int32),
                output_name: "a".to_string(),
            }],
        });

        let optimized = optimize(proj, &std::collections::HashMap::new());

        match &optimized {
            LogicalPlan::Project(proj) => match proj.input.as_ref() {
                LogicalPlan::Scan(scan) => {
                    let required = scan.required_columns.as_ref().expect("should be pruned");
                    assert_eq!(required, &["a"]);
                }
                other => panic!(
                    "expected Scan under Project, got {:?}",
                    std::mem::discriminant(other)
                ),
            },
            other => panic!("expected Project, got {:?}", std::mem::discriminant(other)),
        }
    }

    #[test]
    fn combined_pushdown_and_pruning() {
        let table = test_table();
        let scan = LogicalPlan::Scan(scan_node(&table));
        let pred = eq_pred(
            col_ref("b", DataType::Utf8),
            TypedExpr {
                kind: ExprKind::Literal(LiteralValue::String("x".to_string())),
                data_type: DataType::Utf8,
                nullable: false,
            },
        );
        let filtered = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: pred,
        });
        let proj = LogicalPlan::Project(ProjectNode {
            input: Box::new(filtered),
            items: vec![ProjectItem {
                expr: col_ref("a", DataType::Int32),
                output_name: "a".to_string(),
            }],
        });

        let optimized = optimize(proj, &std::collections::HashMap::new());

        // Project → Scan (with predicate on b and required_columns = [a, b])
        match &optimized {
            LogicalPlan::Project(proj) => match proj.input.as_ref() {
                LogicalPlan::Scan(scan) => {
                    assert_eq!(scan.predicates.len(), 1, "predicate should be pushed");
                    let required = scan.required_columns.as_ref().expect("should be pruned");
                    assert!(required.contains(&"a".to_string()));
                    assert!(required.contains(&"b".to_string()));
                    assert!(!required.contains(&"c".to_string()));
                }
                other => panic!(
                    "expected Scan under Project, got {:?}",
                    std::mem::discriminant(other)
                ),
            },
            other => panic!("expected Project, got {:?}", std::mem::discriminant(other)),
        }
    }
}
