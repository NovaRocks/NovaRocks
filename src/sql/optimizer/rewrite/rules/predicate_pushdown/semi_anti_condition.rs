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
//! from which at least one conjunct is right-only. One step — the rewrite pipeline's
//! fixed-point and other rules (e.g. PushDownPredicateScan) take over on
//! the pushed filter afterwards.
//!
//! Mirrors legacy `push_semi_condition_into_children` from
//! `src/sql/optimizer/predicate_pushdown.rs`. Ported verbatim except for
//! being exposed through the new logical rewrite rule trait.

use crate::sql::analysis::{JoinKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule as RewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_column_id_refs_strict, collect_output_ids, combine_and, split_and,
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
        let mut right_ids = collect_output_ids(&join.right);
        let mut left_ids = collect_output_ids(&join.left);
        right_ids.remove(&ColumnId::UNSET);
        left_ids.remove(&ColumnId::UNSET);

        let mut keep_in_condition: Vec<TypedExpr> = Vec::new();
        let mut push_to_right: Vec<TypedExpr> = Vec::new();

        for conj in conjuncts {
            let is_right_only =
                classify_right_only_by_column_ids(&conj, &left_ids, &right_ids).unwrap_or(false);

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
            required_output_columns: None,
        });
        Some(LogicalPlan::Join(JoinNode {
            left: join.left,
            right: Box::new(new_right),
            join_type: join.join_type,
            condition: new_condition,
            required_output_columns: join.required_output_columns,
        }))
    }
}

fn classify_right_only_by_column_ids(
    expr: &TypedExpr,
    left_ids: &std::collections::HashSet<ColumnId>,
    right_ids: &std::collections::HashSet<ColumnId>,
) -> Option<bool> {
    let ids = collect_column_id_refs_strict(expr)?;
    if ids.is_empty() {
        return Some(false);
    }

    for id in ids {
        match (left_ids.contains(&id), right_ids.contains(&id)) {
            (false, true) => {}
            _ => return Some(false),
        }
    }
    Some(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn test_col_id(name: &str) -> ColumnId {
        match name {
            "ss_sold_date_sk" => ColumnId::new_for_test(1),
            "ss_item_sk" => ColumnId::new_for_test(2),
            "d_date_sk" => ColumnId::new_for_test(3),
            "d_year" => ColumnId::new_for_test(4),
            "x" => ColumnId::new_for_test(10),
            "y" => ColumnId::new_for_test(11),
            "a" => ColumnId::new_for_test(20),
            "b" => ColumnId::new_for_test(21),
            _ => ColumnId::new_for_test(100),
        }
    }

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                column_id: test_col_id(name),
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

    fn gt(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Gt,
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
            alias: Some(table_name.into()),
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
            required_output_columns: None,
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
            required_output_columns: None,
        })
    }

    fn col_with_id(qualifier: &str, name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(qualifier.to_string()),
                column: name.to_string(),
            },
        }
    }

    fn values_with_output(name: &str, id: u32) -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(id),
                name: name.to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
            required_output_columns: None,
        })
    }

    fn derived_project_with_output_id(source_id: u32, output_id: u32) -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(values_with_output("right_source", source_id)),
            items: vec![ProjectItem {
                expr: TypedExpr {
                    data_type: DataType::Int64,
                    nullable: true,
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId::new_for_test(source_id),
                        qualifier: None,
                        column: "right_source".to_string(),
                    },
                },
                output_name: "k".to_string(),
                output_column_id: ColumnId::new_for_test(output_id),
            }],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    #[test]
    fn pushes_right_only_alias_free_project_conjunct_by_column_id() {
        let left = values_with_output("k", 101);
        let right = derived_project_with_output_id(22, 202);
        let join_pred = eq(col_with_id("l", "k", 101), col_with_id("r", "k", 202));
        let right_pred = gt(col_with_id("r", "k", 202), int_lit(10));
        let join = semi_join(left, right, Some(and(join_pred, right_pred)));

        let rule = PushSemiAntiRightOnlyCondition;
        let out = rule
            .apply(join)
            .expect("right-only derived output predicate should push");

        let LogicalPlan::Join(j) = out else {
            panic!("expected Join");
        };
        assert_eq!(j.join_type, JoinKind::LeftSemi);
        assert!(matches!(
            j.condition.as_ref().map(|expr| &expr.kind),
            Some(ExprKind::BinaryOp { op: BinOp::Eq, .. })
        ));
        let LogicalPlan::Filter(filter) = *j.right else {
            panic!("expected Filter on right child");
        };
        assert!(matches!(
            &filter.predicate.kind,
            ExprKind::BinaryOp {
                op: BinOp::Gt,
                left,
                ..
            } if matches!(
                &left.kind,
                ExprKind::ColumnRef { column_id, qualifier: Some(q), column }
                    if *column_id == ColumnId::new_for_test(202) && q == "r" && column == "k"
            )
        ));
        assert!(matches!(*filter.input, LogicalPlan::Project(_)));
    }

    #[test]
    fn p4_semi_anti_does_not_push_same_name_with_wrong_column_id() {
        let left = values_with_output("k", 101);
        let right = values_with_output("k", 202);
        let join_pred = eq(col_with_id("l", "k", 101), col_with_id("r", "k", 202));
        let same_name_wrong_id = gt(col_with_id("r", "k", 999), int_lit(10));
        let join = semi_join(left, right, Some(and(join_pred, same_name_wrong_id)));

        let rule = PushSemiAntiRightOnlyCondition;
        assert!(
            rule.apply(join).is_none(),
            "same column name must not make a predicate right-only without a right ColumnId"
        );
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
