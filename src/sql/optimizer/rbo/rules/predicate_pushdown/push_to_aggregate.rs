//! PushDownPredicateAggregate — `Filter(Aggregate)` rewrite.
//!
//! Pushes conjuncts whose refs are entirely GROUP BY key columns below
//! the aggregate. Predicates referencing aggregate outputs (computed
//! expressions) remain above. Constant predicates stay above too —
//! legacy does not push them because aggregate pushability requires at
//! least one GROUP-BY-key reference (`!refs.is_empty()` guard, deliberate
//! asymmetry vs. Project/Scan).
//!
//! Mirrors legacy `push_predicates_through_aggregate`. Does not recurse.

use std::collections::HashSet;

use super::super::super::rule::RewriteRule;
use crate::sql::analysis::ExprKind;
use crate::sql::optimizer::rbo::utils::{
    collect_column_refs, combine_and, split_and, wrap_remaining_filter,
};
use crate::sql::planner::plan::*;

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

        // GROUP BY key column names — only bare ColumnRef items contribute
        // pushable column names; computed GROUP BY expressions do not.
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
            // Keep the `!refs.is_empty()` guard: constant predicates (empty
            // refs) are not pushed through aggregates — they don't depend on
            // any GROUP BY key.
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
            ..agg
        });
        Some(wrap_remaining_filter(new_agg, remaining))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
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
                iceberg_row_lineage_metadata_columns: vec![],
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: cols
                .iter()
                .map(|n| OutputColumn {
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
        })
    }

    /// Build Aggregate(Scan) with GROUP BY `a` and SUM(b).
    fn agg_sum_b_group_by_a(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(input),
            group_by: vec![col("a")],
            aggregates: vec![AggregateCall {
                name: "sum".into(),
                args: vec![col("b")],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![
                OutputColumn {
                    name: "a".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
                OutputColumn {
                    name: "sum_b".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
            ],
        })
    }

    // Test 1: WHERE a = 1, GROUP BY a, SUM(b)
    // a is a GROUP BY key → predicate is pushable below the aggregate.
    // Expected shape: Aggregate(Filter(Scan))
    #[test]
    fn pushes_group_by_column_predicate() {
        let scan = scan_with_cols(&["a", "b"]);
        let agg = agg_sum_b_group_by_a(scan);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(agg),
            predicate: eq(col("a"), int_lit(1)),
        });

        let rule = PushDownPredicateAggregate;
        assert!(rule.matches(&filter));
        let out = rule.apply(filter).expect("should rewrite");

        // Expected: Aggregate(Filter(Scan))
        match out {
            LogicalPlan::Aggregate(a) => match *a.input {
                LogicalPlan::Filter(f) => match *f.input {
                    LogicalPlan::Scan(_) => {}
                    other => panic!("expected Scan under Filter, got {:?}", other),
                },
                other => panic!("expected Filter under Aggregate, got {:?}", other),
            },
            other => panic!("expected Aggregate at top, got {:?}", other),
        }
    }

    // Test 2: WHERE sum_b = 100, GROUP BY a, SUM(b)
    // sum_b is an aggregate output column, not a GROUP BY key → not pushable.
    // Rule must return None.
    #[test]
    fn does_not_push_aggregate_output_predicate() {
        let scan = scan_with_cols(&["a", "b"]);
        let agg = agg_sum_b_group_by_a(scan);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(agg),
            predicate: eq(col("sum_b"), int_lit(100)),
        });

        let rule = PushDownPredicateAggregate;
        assert!(rule.matches(&filter));
        assert!(
            rule.apply(filter).is_none(),
            "aggregate output predicate must not be pushed below the aggregate"
        );
    }

    // Test 3: WHERE 1 = 1 (constant predicate — no column refs)
    // The `!refs.is_empty()` guard keeps this above. Must return None.
    // Contrast: Project/Scan push constants vacuously via all() on empty iter.
    #[test]
    fn does_not_push_constant_predicate() {
        let scan = scan_with_cols(&["a", "b"]);
        let agg = agg_sum_b_group_by_a(scan);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(agg),
            predicate: eq(int_lit(1), int_lit(1)),
        });

        let rule = PushDownPredicateAggregate;
        assert!(rule.matches(&filter));
        assert!(
            rule.apply(filter).is_none(),
            "constant predicate must not be pushed through an aggregate"
        );
    }
}
