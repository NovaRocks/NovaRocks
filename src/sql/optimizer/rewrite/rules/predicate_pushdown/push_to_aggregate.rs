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

use crate::sql::analysis::ExprKind;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule as RewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_column_id_refs_strict, combine_and, split_and, wrap_remaining_filter,
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
            LogicalPlan::Filter(f)
                if matches!(*f.input, LogicalPlan::Aggregate(ref a) if !aggregate_child_is_repeat(&a.input))
        )
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Filter(filter) = plan else {
            return None;
        };
        let LogicalPlan::Aggregate(agg) = *filter.input else {
            return None;
        };

        // ROLLUP/CUBE/GROUPING SETS guard: a Repeat below the aggregate
        // synthesizes subtotal rows where GROUP BY key columns are NULL in the
        // aggregate's *output*. A predicate that holds on the output (e.g. a
        // DeriveJoinNotNull-derived IS NOT NULL on a join key) does NOT hold on
        // the aggregate's input, so pushing it below would drop subtotal rows
        // (wrong results). It also breaks DeriveJoinNotNull idempotency
        // (spine_not_null stops at Aggregate), causing unbounded re-derivation
        // of the same filter and cardinality blowup.
        if aggregate_child_is_repeat(&agg.input) {
            return None;
        }

        // GROUP BY key ColumnIds — only bare ColumnRef items contribute
        // pushable ids; computed GROUP BY expressions do not.
        let group_by_ids: HashSet<ColumnId> = agg
            .group_by
            .iter()
            .filter_map(|e| match &e.kind {
                ExprKind::ColumnRef { column_id, .. } if *column_id != ColumnId::UNSET => {
                    Some(*column_id)
                }
                _ => None,
            })
            .collect();

        let conjuncts = split_and(filter.predicate);
        let mut pushable = Vec::new();
        let mut remaining = Vec::new();
        for conj in conjuncts {
            let refs = collect_column_id_refs_strict(&conj);
            // Keep the `!refs.is_empty()` guard: constant predicates (empty
            // refs) are not pushed through aggregates — they don't depend on
            // any GROUP BY key.
            if let Some(refs) = refs
                && !refs.is_empty()
                && refs.iter().all(|id| group_by_ids.contains(id))
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
            required_output_columns: None,
        });
        let new_agg = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(new_child),
            ..agg
        });
        Some(wrap_remaining_filter(new_agg, remaining))
    }
}

/// True if the aggregate's input is (or passes through to) a Repeat node —
/// i.e. this is a ROLLUP / CUBE / GROUPING SETS aggregate whose GROUP BY keys
/// can be NULL in its output, so output-level predicates must not be pushed
/// below it.
fn aggregate_child_is_repeat(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Repeat(_) => true,
        LogicalPlan::Filter(f) => aggregate_child_is_repeat(&f.input),
        LogicalPlan::Project(p) => aggregate_child_is_repeat(&p.input),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn test_col_id(name: &str) -> ColumnId {
        match name {
            "a" => ColumnId::new_for_test(1),
            "b" => ColumnId::new_for_test(2),
            "sum_b" => ColumnId::new_for_test(3),
            _ => ColumnId::new_for_test(100),
        }
    }

    fn col_with_id(name: &str, column_id: ColumnId) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: name.into(),
            },
        }
    }

    fn col(name: &str) -> TypedExpr {
        col_with_id(name, test_col_id(name))
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
                        write_default: None,
                        logical_type: None,
                    })
                    .collect(),
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: cols
                .iter()
                .map(|n| OutputColumn {
                    column_id: test_col_id(n),
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            required_output_columns: None,
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
                output_column_id: test_col_id("sum_b"),
            }],
            output_columns: vec![
                OutputColumn {
                    column_id: test_col_id("a"),
                    name: "a".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: test_col_id("sum_b"),
                    name: "sum_b".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            already_pushed: false,
            required_output_columns: None,
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
            required_output_columns: None,
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
            required_output_columns: None,
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
            required_output_columns: None,
        });

        let rule = PushDownPredicateAggregate;
        assert!(rule.matches(&filter));
        assert!(
            rule.apply(filter).is_none(),
            "constant predicate must not be pushed through an aggregate"
        );
    }
}
