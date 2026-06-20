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
//! `src/sql/optimizer/predicate_pushdown.rs`. Migrated to `OptExpr` / `LogicalRewriteRule`.

use crate::sql::column_id::ColumnId;
use crate::sql::common::JoinKind;
use crate::sql::optimizer::operator::{FilterOp, LogicalJoinOp, Operator};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::collect_output_ids_opt;
use crate::sql::optimizer::scalar::ScalarId;
use crate::sql::optimizer::scalar_expr;

pub(crate) struct PushSemiAntiRightOnlyCondition;

impl LogicalRewriteRule for PushSemiAntiRightOnlyCondition {
    fn name(&self) -> &'static str {
        "PushSemiAntiRightOnlyCondition"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        let Operator::LogicalJoin(j) = &expr.op else {
            return false;
        };
        matches!(
            j.join_type,
            JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::RightSemi | JoinKind::RightAnti
        ) && j.condition.is_some()
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            mut children,
            required_output_columns,
        } = expr;
        let Operator::LogicalJoin(join) = op else {
            return Ok(RewriteResult::Unchanged);
        };
        if children.len() != 2 {
            return Ok(RewriteResult::Unchanged);
        }
        let Some(cond_id) = join.condition else {
            return Ok(RewriteResult::Unchanged);
        };
        let right = children.remove(1);
        let left = children.remove(0);

        let arena_rc = ctx.scalar_arena();
        let mut arena = arena_rc.borrow_mut();

        let mut conjuncts = Vec::new();
        scalar_expr::split_conjuncts(&arena, cond_id, &mut conjuncts);
        let mut right_ids = collect_output_ids_opt(&right);
        let mut left_ids = collect_output_ids_opt(&left);
        right_ids.remove(&ColumnId::UNSET);
        left_ids.remove(&ColumnId::UNSET);

        let mut keep_in_condition = Vec::new();
        let mut push_to_right = Vec::new();

        for conj in conjuncts {
            let is_right_only =
                classify_right_only_by_column_ids(conj, &left_ids, &right_ids, &arena)
                    .unwrap_or(false);

            if is_right_only {
                push_to_right.push(conj);
            } else {
                keep_in_condition.push(conj);
            }
        }

        if push_to_right.is_empty() {
            return Ok(RewriteResult::Unchanged);
        }

        let new_condition = if keep_in_condition.is_empty() {
            None
        } else {
            scalar_expr::combine_conjuncts(&mut arena, keep_in_condition)
        };
        let Some(pushed_id) = scalar_expr::combine_conjuncts(&mut arena, push_to_right) else {
            return Ok(RewriteResult::Unchanged);
        };
        let new_right = OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: pushed_id,
            }),
            vec![right],
        );
        let mut result = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: join.join_type,
                condition: new_condition,
            }),
            vec![left, new_right],
        );
        result.required_output_columns = required_output_columns;
        Ok(RewriteResult::Changed(result))
    }
}

fn classify_right_only_by_column_ids(
    expr: ScalarId,
    left_ids: &std::collections::HashSet<ColumnId>,
    right_ids: &std::collections::HashSet<ColumnId>,
    arena: &crate::sql::optimizer::scalar::ScalarArena,
) -> Option<bool> {
    let ids = scalar_expr::collect_column_ids_strict(arena, expr)?;
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
    use crate::sql::analysis::TypedExpr;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{ScanOp, ValuesOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::scalar::ScalarArena;

    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use arrow::datatypes::DataType;
    use std::cell::RefCell;
    use std::rc::Rc;

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

    fn col_typed(name: &str) -> TypedExpr {
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

    fn col_with_id_typed(qualifier: &str, name: &str, id: u32) -> TypedExpr {
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

    fn int_lit_typed(v: i64) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }

    fn eq_typed(a: TypedExpr, b: TypedExpr) -> TypedExpr {
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

    fn gt_typed(a: TypedExpr, b: TypedExpr) -> TypedExpr {
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

    fn and_typed(a: TypedExpr, b: TypedExpr) -> TypedExpr {
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

    fn make_scan(arena: &mut ScalarArena, table_name: &str, cols: &[&str]) -> OptExpr {
        let table = TableDef {
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
        };
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".into(),
            table,
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
            mv_rewritten_from: None,
        }))
    }

    fn values_with_output(name: &str, id: u32) -> OptExpr {
        OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(id),
                name: name.to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
        }))
    }

    fn make_ctx(arena: ScalarArena) -> RewriteContext {
        let mut ctx = RewriteContext::for_query(std::iter::empty::<String>());
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));
        ctx
    }

    fn semi_join(
        arena: &mut ScalarArena,
        left: OptExpr,
        right: OptExpr,
        condition: Option<TypedExpr>,
    ) -> OptExpr {
        let cond_id = condition.map(|c| intern_typed(arena, &c));
        OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::LeftSemi,
                condition: cond_id,
            }),
            vec![left, right],
        )
    }

    fn inner_join(
        arena: &mut ScalarArena,
        left: OptExpr,
        right: OptExpr,
        condition: Option<TypedExpr>,
    ) -> OptExpr {
        let cond_id = condition.map(|c| intern_typed(arena, &c));
        OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: cond_id,
            }),
            vec![left, right],
        )
    }

    #[test]
    fn pushes_right_only_alias_free_project_conjunct_by_column_id() {
        let mut arena = ScalarArena::new();
        let left = values_with_output("k", 101);
        // right is a project over values: source_id=22, output_id=202
        let right_source = values_with_output("right_source", 22);
        use crate::sql::optimizer::operator::{ProjectOp, ScalarProjectItem};
        let right = OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: vec![ScalarProjectItem {
                    expr: intern_typed(
                        &mut arena,
                        &TypedExpr {
                            data_type: DataType::Int64,
                            nullable: true,
                            kind: ExprKind::ColumnRef {
                                column_id: ColumnId::new_for_test(22),
                                qualifier: None,
                                column: "right_source".to_string(),
                            },
                        },
                    ),
                    output_name: "k".to_string(),
                    output_column_id: ColumnId::new_for_test(202),
                    expr_display: None,
                }],
                output_qualifier: None,
            }),
            vec![right_source],
        );
        let join_pred = eq_typed(
            col_with_id_typed("l", "k", 101),
            col_with_id_typed("r", "k", 202),
        );
        let right_pred = gt_typed(col_with_id_typed("r", "k", 202), int_lit_typed(10));
        let join = semi_join(
            &mut arena,
            left,
            right,
            Some(and_typed(join_pred, right_pred)),
        );

        let rule = PushSemiAntiRightOnlyCondition;
        let mut ctx = make_ctx(arena);
        let result = rule
            .apply(join, &mut ctx)
            .expect("right-only derived output predicate should push");
        let RewriteResult::Changed(out) = result else {
            panic!("expected Changed");
        };

        let Operator::LogicalJoin(j) = &out.op else {
            panic!("expected Join");
        };
        assert_eq!(j.join_type, JoinKind::LeftSemi);
        // The equi-join condition should remain.
        assert!(
            j.condition.is_some(),
            "join condition should remain with the equi-join predicate"
        );
        // Right child should be a Filter.
        assert!(
            matches!(&out.right().op, Operator::LogicalFilter(_)),
            "expected Filter on right child"
        );
    }

    #[test]
    fn p4_semi_anti_does_not_push_same_name_with_wrong_column_id() {
        let mut arena = ScalarArena::new();
        let left = values_with_output("k", 101);
        let right = values_with_output("k", 202);
        let join_pred = eq_typed(
            col_with_id_typed("l", "k", 101),
            col_with_id_typed("r", "k", 202),
        );
        let same_name_wrong_id = gt_typed(col_with_id_typed("r", "k", 999), int_lit_typed(10));
        let join = semi_join(
            &mut arena,
            left,
            right,
            Some(and_typed(join_pred, same_name_wrong_id)),
        );

        let rule = PushSemiAntiRightOnlyCondition;
        let mut ctx = make_ctx(arena);
        let result = rule.apply(join, &mut ctx).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "same column name must not make a predicate right-only without a right ColumnId"
        );
    }

    // Test 1: LEFT SEMI ON (ss_sold_date_sk=d_date_sk AND corr AND d_year=2002)
    // where corr is left-only (ss_item_sk=something), the equi-join is cross-side,
    // and d_year=2002 is right-only → right child wraps Filter, condition drops d_year=2002.
    #[test]
    fn pushes_right_only_conjunct_into_right_child_for_left_semi() {
        let mut arena = ScalarArena::new();
        let store_sales = make_scan(
            &mut arena,
            "store_sales",
            &["ss_sold_date_sk", "ss_item_sk"],
        );
        let date_dim = make_scan(&mut arena, "date_dim", &["d_date_sk", "d_year"]);

        // corr = ss_item_sk = 100  (left-only)
        let corr = eq_typed(col_typed("ss_item_sk"), int_lit_typed(100));
        // equi-join condition: ss_sold_date_sk = d_date_sk (cross-side)
        let equi = eq_typed(col_typed("ss_sold_date_sk"), col_typed("d_date_sk"));
        // right-only predicate: d_year = 2002
        let yr = eq_typed(col_typed("d_year"), int_lit_typed(2002));

        // condition = corr AND equi AND yr
        let condition = and_typed(and_typed(corr, equi), yr);
        let join = semi_join(&mut arena, store_sales, date_dim, Some(condition));

        let rule = PushSemiAntiRightOnlyCondition;
        let mut ctx = make_ctx(arena);
        assert!(
            rule.matches(&join, &ctx),
            "should match LEFT SEMI with condition"
        );
        let result = rule.apply(join, &mut ctx).expect("should rewrite");
        let RewriteResult::Changed(out) = result else {
            panic!("expected Changed");
        };

        match &out.op {
            Operator::LogicalJoin(j) => {
                assert_eq!(j.join_type, JoinKind::LeftSemi);
                // Left child should remain an unmodified scan
                assert!(matches!(&out.left().op, Operator::LogicalScan(_)));
                // Right child should be a Filter wrapping the date_dim scan
                match &out.right().op {
                    Operator::LogicalFilter(_) => {
                        assert!(
                            matches!(&out.right().unary_input().op, Operator::LogicalScan(_)),
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
    // The only conjunct is cross-side — no right-only conjunct → rule returns Unchanged.
    #[test]
    fn returns_unchanged_when_no_right_only_conjunct() {
        let mut arena = ScalarArena::new();
        let store_sales = make_scan(
            &mut arena,
            "store_sales",
            &["ss_sold_date_sk", "ss_item_sk"],
        );
        let date_dim = make_scan(&mut arena, "date_dim", &["d_date_sk", "d_year"]);

        // cross-side equi-join: not right-only
        let equi = eq_typed(col_typed("ss_sold_date_sk"), col_typed("d_date_sk"));
        let join = semi_join(&mut arena, store_sales, date_dim, Some(equi));

        let rule = PushSemiAntiRightOnlyCondition;
        let mut ctx = make_ctx(arena);
        assert!(rule.matches(&join, &ctx));
        let result = rule.apply(join, &mut ctx).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "no right-only conjunct — rule must return Unchanged"
        );
    }

    // Test 3: INNER join with a condition — `matches()` must return false.
    #[test]
    fn does_not_match_inner_join() {
        let mut arena = ScalarArena::new();
        let t1 = make_scan(&mut arena, "t1", &["x", "y"]);
        let t2 = make_scan(&mut arena, "t2", &["a", "b"]);
        let condition = eq_typed(col_typed("x"), col_typed("a"));
        let join = inner_join(&mut arena, t1, t2, Some(condition));

        let ctx = make_ctx(arena);
        let rule = PushSemiAntiRightOnlyCondition;
        assert!(
            !rule.matches(&join, &ctx),
            "INNER join must not match PushSemiAntiRightOnlyCondition"
        );
    }
}
