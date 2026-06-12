//! PushDownPredicateScan — `Filter(Scan)` rewrite.
//!
//! Pushes filter conjuncts into `ScanNode.predicates` when every column
//! the conjunct references is present in the scan's output. Unpushable
//! conjuncts are wrapped back as a residual `Filter` above the scan.
//!
//! Mirrors the `LogicalPlan::Scan(mut scan)` arm of legacy
//! `predicate_pushdown::push_filter_into`.

use std::collections::HashSet;

use crate::sql::analysis::TypedExpr;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule as RewriteRule;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::predicate_key as canonical_predicate_key;
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_column_id_refs_strict, collect_output_ids, split_and, wrap_remaining_filter,
};
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
        let mut scan_ids = collect_output_ids(&LogicalPlan::Scan(scan.clone()));
        scan_ids.remove(&ColumnId::UNSET);

        // Canonical keys of predicates already on the scan, so we never append a
        // structurally-identical duplicate (`P AND P == P`).
        // This keeps the scan's predicate list clean even when an upstream rule
        // (e.g. OQ-2 `DeriveJoinNotNullPredicate`) re-derives the same
        // `IS NOT NULL` across stacked joins on a shared key every fixed-point
        // round. Only exact duplicates collapse; distinct predicates are kept.
        let mut seen: HashSet<String> = scan.predicates.iter().map(predicate_key).collect();

        let mut pushed_any = false;
        let mut remaining = Vec::new();
        for conj in conjuncts {
            let Some(refs) = collect_column_id_refs_strict(&conj) else {
                remaining.push(conj);
                continue;
            };
            if refs.is_empty() || refs.iter().all(|id| scan_ids.contains(id)) {
                // Push only if no structurally-identical predicate is present.
                if seen.insert(predicate_key(&conj)) {
                    scan.predicates.push(conj);
                }
                // A conjunct that targets this scan is "handled" whether it was
                // newly pushed or dropped as a duplicate — it must not survive as
                // a residual filter, otherwise the rule never reaches fixed point.
                pushed_any = true;
            } else {
                remaining.push(conj);
            }
        }

        if !pushed_any {
            // No change — re-wrap the untouched filter so the pipeline's
            // "Option::None = no-op" contract holds.
            return None;
        }

        Some(wrap_remaining_filter(LogicalPlan::Scan(scan), remaining))
    }
}

/// Canonical key for structural predicate equality. `TypedExpr` does not derive
/// `PartialEq`, so this delegates to the shared predicate-group key used by the
/// join pushdown rules.
fn predicate_key(expr: &TypedExpr) -> String {
    canonical_predicate_key(expr).as_str().to_string()
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
            "zz" => ColumnId::new_for_test(99),
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

    #[test]
    fn pushes_single_scan_column_predicate() {
        let scan = scan_with_cols(&["a", "b"]);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: eq(col("a"), int_lit(1)),
            required_output_columns: None,
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
        // is pushable; rule must return None so the pipeline's fixed-point
        // terminates on this shape.
        let scan = scan_with_cols(&["a"]);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: eq(col("zz"), int_lit(1)),
            required_output_columns: None,
        });
        let rule = PushDownPredicateScan;
        assert!(rule.apply(filter).is_none());
    }

    #[test]
    fn p4_scan_does_not_push_same_name_with_different_column_id() {
        let scan = scan_with_cols(&["a"]);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: eq(col_with_id("a", ColumnId::new_for_test(77)), int_lit(1)),
            required_output_columns: None,
        });
        let rule = PushDownPredicateScan;
        assert!(
            rule.apply(filter).is_none(),
            "same name must not push when ColumnId is not produced by the scan"
        );
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
            required_output_columns: None,
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

    fn is_not_null(e: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::IsNull {
                expr: Box::new(e),
                negated: true,
            },
        }
    }

    /// Re-pushing a structurally-identical predicate across fixed-point rounds
    /// (as OQ-2 `DeriveJoinNotNullPredicate` does for a key shared by stacked
    /// joins) must leave exactly ONE copy on the scan, not accumulate. This is
    /// the regression guard for the `g3_broadcast` 31-copy blowup.
    #[test]
    fn repeated_identical_predicate_dedups_to_one() {
        // Round 1: Filter(a IS NOT NULL, Scan) -> Scan with one predicate.
        let scan = scan_with_cols(&["a", "b"]);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: is_not_null(col("a")),
            required_output_columns: None,
        });
        let after_round1 = PushDownPredicateScan
            .apply(filter)
            .expect("first push should rewrite");
        let scan_after_round1 = match after_round1 {
            LogicalPlan::Scan(s) => {
                assert_eq!(s.predicates.len(), 1, "first push lands one predicate");
                s
            }
            other => panic!("expected bare Scan after full pushdown, got {:?}", other),
        };

        // Round 2: an upstream rule re-derives the same conjunct above the scan
        // that already carries it. The duplicate must be dropped — the scan
        // still holds exactly one copy, and the redundant Filter is removed.
        let filter2 = LogicalPlan::Filter(FilterNode {
            input: Box::new(LogicalPlan::Scan(scan_after_round1)),
            predicate: is_not_null(col("a")),
            required_output_columns: None,
        });
        let after_round2 = PushDownPredicateScan
            .apply(filter2)
            .expect("redundant Filter removal is a structural change -> Some");
        match after_round2 {
            LogicalPlan::Scan(s) => assert_eq!(
                s.predicates.len(),
                1,
                "re-pushing an identical predicate must not accumulate"
            ),
            other => panic!("expected bare Scan after dedup, got {:?}", other),
        }
    }

    /// `P AND P` inside a single Filter collapses to one scan predicate.
    #[test]
    fn duplicate_conjuncts_in_one_filter_dedup() {
        let scan = scan_with_cols(&["a"]);
        let pred = and(is_not_null(col("a")), is_not_null(col("a")));
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: pred,
            required_output_columns: None,
        });
        let out = PushDownPredicateScan.apply(filter).expect("should rewrite");
        match out {
            LogicalPlan::Scan(s) => assert_eq!(s.predicates.len(), 1),
            other => panic!("expected bare Scan, got {:?}", other),
        }
    }

    /// Distinct predicates on the same column must NOT collapse — dedup is
    /// exact-match only.
    #[test]
    fn distinct_predicates_are_not_collapsed() {
        let scan = scan_with_cols(&["a"]);
        // a IS NOT NULL AND a = 1 -> two distinct predicates.
        let pred = and(is_not_null(col("a")), eq(col("a"), int_lit(1)));
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: pred,
            required_output_columns: None,
        });
        let out = PushDownPredicateScan.apply(filter).expect("should rewrite");
        match out {
            LogicalPlan::Scan(s) => assert_eq!(
                s.predicates.len(),
                2,
                "distinct predicates on the same column must be preserved"
            ),
            other => panic!("expected bare Scan, got {:?}", other),
        }
    }
}
