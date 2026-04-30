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

use super::super::super::rule::RewriteRule;
use crate::sql::analysis::{JoinKind, TypedExpr};
use crate::sql::optimizer::rbo::utils::{
    collect_column_refs, collect_output_columns, collect_qualified_column_refs,
    collect_qualified_output_columns, combine_and, split_and,
};
use crate::sql::planner::plan::*;

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
        let condition = join.condition.as_ref()?;

        // Port of push_semi_condition_into_children logic (legacy lines 374-431).
        let conjuncts = split_and(condition.clone());
        let right_cols = collect_output_columns(&join.right);
        let left_cols = collect_output_columns(&join.left);
        let right_qcols = collect_qualified_output_columns(&join.right);

        let mut keep_in_condition: Vec<TypedExpr> = Vec::new();
        let mut push_to_right: Vec<TypedExpr> = Vec::new();

        for conj in conjuncts {
            let refs = collect_column_refs(&conj);
            let qrefs = collect_qualified_column_refs(&conj);

            // Use qualified refs when available to handle self-joins
            // (e.g., catalog_sales cs1, catalog_sales cs2 — same bare names).
            // But also check bare refs to avoid pushing cross-side predicates
            // where one side is qualified and the other isn't.
            let is_right_only = if !qrefs.is_empty() {
                // All qualified refs must be in right's qualified columns,
                // AND all bare refs must be right-only (not also in left).
                let q_all_right = qrefs.iter().all(|r| right_qcols.contains(r));
                let bare_any_left = refs.iter().any(|c| left_cols.contains(&c.to_lowercase()));
                q_all_right && !bare_any_left
            } else if !refs.is_empty() {
                // Fallback: all bare refs in right but NOT all in left
                // (avoids pushing cross-side predicates for self-joins)
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

    fn scan(table_name: &str, cols: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: table_name.into(),
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
            alias: Some(table_name.into()),
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

    fn semi_join(
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Option<TypedExpr>,
    ) -> LogicalPlan {
        LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::LeftSemi,
            condition,
        })
    }

    fn inner_join(
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Option<TypedExpr>,
    ) -> LogicalPlan {
        LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Inner,
            condition,
        })
    }

    // Test 1: LEFT SEMI ON (ss_sold_date_sk=d_date_sk AND corr AND d_year=2002)
    // where corr is left-only (ss_item_sk=something), the equi-join is cross-side,
    // and d_year=2002 is right-only → right child wraps Filter, condition drops d_year=2002.
    #[test]
    fn pushes_right_only_conjunct_into_right_child_for_left_semi() {
        // store_sales (left): ss_sold_date_sk, ss_item_sk
        // date_dim (right): d_date_sk, d_year
        let store_sales = scan("store_sales", &["ss_sold_date_sk", "ss_item_sk"]);
        let date_dim = scan("date_dim", &["d_date_sk", "d_year"]);

        // corr = ss_item_sk = 100  (left-only)
        let corr = eq(col("ss_item_sk"), int_lit(100));
        // equi-join condition: ss_sold_date_sk = d_date_sk (cross-side)
        let equi = eq(col("ss_sold_date_sk"), col("d_date_sk"));
        // right-only predicate: d_year = 2002
        let yr = eq(col("d_year"), int_lit(2002));

        // condition = corr AND equi AND yr
        let condition = and(and(corr, equi), yr);
        let join = semi_join(store_sales, date_dim, Some(condition));

        let rule = PushSemiAntiRightOnlyCondition;
        assert!(rule.matches(&join), "should match LEFT SEMI with condition");
        let out = rule.apply(join).expect("should rewrite");

        // Expected shape: LeftSemi(store_sales, Filter(date_dim))
        // with the join condition containing corr AND equi (d_year=2002 pushed down)
        match out {
            LogicalPlan::Join(j) => {
                assert_eq!(j.join_type, JoinKind::LeftSemi);
                // Left child should remain an unmodified scan
                assert!(matches!(*j.left, LogicalPlan::Scan(_)));
                // Right child should be a Filter wrapping the date_dim scan
                match *j.right {
                    LogicalPlan::Filter(f) => {
                        assert!(
                            matches!(*f.input, LogicalPlan::Scan(_)),
                            "Filter should wrap the Scan"
                        );
                    }
                    other => panic!("expected Filter on right child, got {:?}", other),
                }
                // Join condition must still exist (corr AND equi were kept)
                assert!(
                    j.condition.is_some(),
                    "join condition should remain with cross-side and left-only conjuncts"
                );
            }
            other => panic!("expected Join at top, got {:?}", other),
        }
    }

    // Test 2: LEFT SEMI ON (ss_sold_date_sk = d_date_sk)
    // The only conjunct is cross-side — no right-only conjunct → rule returns None.
    #[test]
    fn returns_none_when_no_right_only_conjunct() {
        let store_sales = scan("store_sales", &["ss_sold_date_sk", "ss_item_sk"]);
        let date_dim = scan("date_dim", &["d_date_sk", "d_year"]);

        // cross-side equi-join: not right-only
        let equi = eq(col("ss_sold_date_sk"), col("d_date_sk"));
        let join = semi_join(store_sales, date_dim, Some(equi));

        let rule = PushSemiAntiRightOnlyCondition;
        assert!(rule.matches(&join));
        let out = rule.apply(join);
        assert!(
            out.is_none(),
            "no right-only conjunct — rule must return None; got {:?}",
            out
        );
    }

    // Test 3: INNER join with a condition — `matches()` must return false.
    #[test]
    fn does_not_match_inner_join() {
        let t1 = scan("t1", &["x", "y"]);
        let t2 = scan("t2", &["a", "b"]);
        let condition = eq(col("x"), col("a"));
        let join = inner_join(t1, t2, Some(condition));

        let rule = PushSemiAntiRightOnlyCondition;
        assert!(
            !rule.matches(&join),
            "INNER join must not match PushSemiAntiRightOnlyCondition"
        );
    }
}
