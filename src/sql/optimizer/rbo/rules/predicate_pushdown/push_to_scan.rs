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
use crate::sql::optimizer::rbo::utils::{collect_column_refs, split_and, wrap_remaining_filter};
use crate::sql::planner::plan::*;

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
            other => panic!("expected bare Scan after full pushdown, got {:?}", other),
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

    #[test]
    fn partial_pushdown_leaves_residual_filter() {
        // a=1 AND zz=2: only a=1 is pushable because `zz` is not in the
        // scan's output columns. Expect Filter(Scan) with one predicate
        // on the scan and the residual conjunct above.
        let scan = scan_with_cols(&["a"]);
        let pred = and(eq(col("a"), int_lit(1)), eq(col("zz"), int_lit(2)));
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: pred,
        });
        let out = PushDownPredicateScan.apply(filter).expect("should rewrite");
        match out {
            LogicalPlan::Filter(f) => match *f.input {
                LogicalPlan::Scan(s) => assert_eq!(s.predicates.len(), 1),
                other => panic!("expected Scan under residual Filter, got {:?}", other),
            },
            other => panic!(
                "expected Filter(Scan) for partial pushdown, got {:?}",
                other
            ),
        }
    }
}
