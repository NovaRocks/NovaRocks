//! OQ-2: derive `IS NOT NULL` predicates on equi-join keys for null-rejecting
//! join types (Inner / LeftSemi / RightSemi), mirroring StarRocks
//! `JoinPredicatePushdown.deriveIsNotNullPredicate`. The derived `Filter` is
//! pushed to the scan by the existing PredicatePushdownPostJoin pushdown rules
//! running in the same fixed-point loop.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;
use crate::sql::common::{BinOp, JoinKind};
use crate::sql::optimizer::operator::{FilterOp, Operator};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::join_equi_keys_opt;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

pub(crate) struct DeriveJoinNotNullPredicate;

/// `(derive_left, derive_right)`: which sides' equi-keys may receive IS NOT NULL.
/// Mirrors StarRocks: inner -> both; left-semi -> right; right-semi -> left.
/// Anti / null-aware-anti / outer / cross -> neither (see spec §3.1).
fn safe_sides(join_type: JoinKind) -> (bool, bool) {
    match join_type {
        JoinKind::Inner => (true, true),
        JoinKind::LeftSemi => (false, true),
        JoinKind::RightSemi => (true, false),
        JoinKind::LeftAnti
        | JoinKind::RightAnti
        | JoinKind::NullAwareLeftAnti
        | JoinKind::LeftOuter
        | JoinKind::RightOuter
        | JoinKind::FullOuter
        | JoinKind::Cross => (false, false),
    }
}

impl LogicalRewriteRule for DeriveJoinNotNullPredicate {
    fn name(&self) -> &'static str {
        "DeriveJoinNotNullPredicate"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        matches!(&expr.op, Operator::LogicalJoin(_))
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
        let right = children.remove(1);
        let left = children.remove(0);
        let (derive_left, derive_right) = safe_sides(join.join_type);
        if !derive_left && !derive_right {
            return Ok(RewriteResult::Unchanged);
        }
        let arena_rc = ctx.scalar_arena();
        let (left_preds, right_preds) = {
            let arena = arena_rc.borrow();
            let keys = join_equi_keys_opt(&join, &left, &right, &arena);
            if keys.is_empty() {
                return Ok(RewriteResult::Unchanged);
            }

            let left_preds = if derive_left {
                eligible_not_null(&left, keys.iter().map(|k| k.left), &arena)
            } else {
                Vec::new()
            };
            let right_preds = if derive_right {
                eligible_not_null(&right, keys.iter().map(|k| k.right), &arena)
            } else {
                Vec::new()
            };
            (left_preds, right_preds)
        };
        if left_preds.is_empty() && right_preds.is_empty() {
            return Ok(RewriteResult::Unchanged);
        }

        let new_left = wrap_not_null(left, left_preds, &mut arena_rc.borrow_mut());
        let new_right = wrap_not_null(right, right_preds, &mut arena_rc.borrow_mut());

        let result = OptExpr {
            op: Operator::LogicalJoin(join),
            children: vec![new_left, new_right],
            required_output_columns,
        };
        Ok(RewriteResult::Changed(result))
    }
}

/// For each candidate key operand (a ColumnRef from `child`), build the
/// `IS NOT NULL` predicates to add: keep only operands that are (a) nullable and
/// (b) not already guaranteed non-null by `child`'s predicate spine. Dedupe by
/// column identity within the side.
fn eligible_not_null(
    child: &OptExpr,
    operands: impl Iterator<Item = ScalarId>,
    arena: &ScalarArena,
) -> Vec<ScalarId> {
    let guaranteed_ids = spine_not_null(child, arena);
    let mut seen_ids: HashSet<ColumnId> = HashSet::new();
    let mut preds = Vec::new();
    for operand in operands {
        if !arena.nullable(operand) {
            continue;
        }
        // Keys from join_equi_keys_opt are always bare ColumnRef (Cast/Nested
        // already peeled); this guard is defensive.
        let ScalarNode::ColumnRef(column_id) = arena.node(operand) else {
            continue;
        };
        if *column_id == ColumnId::UNSET {
            continue;
        }
        if guaranteed_ids.contains(column_id) {
            continue; // already guaranteed non-null -> idempotency
        }
        if seen_ids.insert(*column_id) {
            preds.push(operand);
        }
    }
    preds
}

fn is_not_null(arena: &mut ScalarArena, operand: ScalarId) -> ScalarId {
    arena.intern(
        ScalarNode::IsNull {
            child: operand,
            negated: true,
        },
        DataType::Boolean,
        false,
    )
}

fn combine_and_scalar(arena: &mut ScalarArena, mut exprs: Vec<ScalarId>) -> ScalarId {
    assert!(!exprs.is_empty());
    let mut result = exprs.pop().unwrap();
    while let Some(left) = exprs.pop() {
        let nullable = arena.nullable(left) || arena.nullable(result);
        result = arena.intern(
            ScalarNode::BinaryOp {
                left,
                op: BinOp::And,
                right: result,
            },
            DataType::Boolean,
            nullable,
        );
    }
    result
}

fn wrap_not_null(child: OptExpr, operands: Vec<ScalarId>, arena: &mut ScalarArena) -> OptExpr {
    if operands.is_empty() {
        return child;
    }
    let preds = operands
        .into_iter()
        .map(|operand| is_not_null(arena, operand))
        .collect();
    let predicate = combine_and_scalar(arena, preds);
    OptExpr::new(Operator::LogicalFilter(FilterOp { predicate }), vec![child])
}

/// Walk `plan`'s predicate spine (passthrough single-input nodes down to the
/// root scan) collecting column identities already guaranteed non-null by an
/// `IS NOT NULL` conjunct. Used for idempotency / redundant-filter avoidance.
fn spine_not_null(expr: &OptExpr, arena: &ScalarArena) -> HashSet<ColumnId> {
    let mut ids = HashSet::new();
    spine_not_null_inner(expr, arena, &mut ids);
    ids
}

fn spine_not_null_inner(expr: &OptExpr, arena: &ScalarArena, ids: &mut HashSet<ColumnId>) {
    match &expr.op {
        Operator::LogicalFilter(f) => {
            record_not_null_conjuncts(arena, f.predicate, ids);
            spine_not_null_inner(expr.unary_input(), arena, ids);
        }
        Operator::LogicalScan(s) => {
            for pred_id in &s.predicates {
                record_not_null_conjuncts(arena, *pred_id, ids);
            }
        }
        // Project may rename columns; id-based matching remains valid through
        // passthrough projections and intentionally ignores name-only matches.
        Operator::LogicalProject(_) | Operator::LogicalSort(_) | Operator::LogicalLimit(_) => {
            spine_not_null_inner(expr.unary_input(), arena, ids)
        }
        _ => {}
    }
}

fn record_not_null_conjuncts(arena: &ScalarArena, expr: ScalarId, ids: &mut HashSet<ColumnId>) {
    match arena.node(expr) {
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            record_not_null_conjuncts(arena, *left, ids);
            record_not_null_conjuncts(arena, *right, ids);
        }
        ScalarNode::Nested(inner) => record_not_null_conjuncts(arena, *inner, ids),
        _ => record_not_null_scalar(arena, expr, ids),
    }
}

fn record_not_null_scalar(arena: &ScalarArena, expr: ScalarId, ids: &mut HashSet<ColumnId>) {
    if let ScalarNode::IsNull {
        child,
        negated: true,
    } = arena.node(expr)
        && let ScalarNode::ColumnRef(column_id) = arena.node(*child)
        && *column_id != ColumnId::UNSET
    {
        ids.insert(*column_id);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::optimizer::operator::{LogicalJoinOp, ScanOp};
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::scalar::{self, ScalarArena};

    fn make_ctx(arena: ScalarArena) -> RewriteContext {
        let mut ctx = RewriteContext::for_query(std::iter::empty::<String>());
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));
        ctx
    }

    fn scan_opt(alias: &str, table: &str, cols: &[(&str, u32, bool)]) -> OptExpr {
        let scan = ScanOp {
            database: "default".to_string(),
            table: TableDef {
                name: table.to_string(),
                columns: cols
                    .iter()
                    .map(|(name, _, nullable)| ColumnDef {
                        name: name.to_string(),
                        data_type: DataType::Int32,
                        nullable: *nullable,
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
            alias: Some(alias.to_string()),
            columns: cols
                .iter()
                .map(|(name, id, nullable)| OutputColumn {
                    column_id: ColumnId::new_for_test(*id),
                    name: name.to_string(),
                    data_type: DataType::Int32,
                    nullable: *nullable,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        };
        OptExpr::leaf(Operator::LogicalScan(scan))
    }

    fn col_typed(qualifier: &str, name: &str, id: u32, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(qualifier.to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable,
        }
    }

    fn eq_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn and_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn join_opt(
        arena: &mut ScalarArena,
        jt: JoinKind,
        left: OptExpr,
        right: OptExpr,
        cond: Option<TypedExpr>,
    ) -> OptExpr {
        let condition =
            cond.map(|c| crate::sql::planner::optimizer_bridge::scalar::intern_typed(arena, &c));
        OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: jt,
                condition,
            }),
            vec![left, right],
        )
    }

    /// Returns (left_child_is_filter, right_child_is_filter) for the rule's output.
    fn side_filters(out: Result<RewriteResult, String>) -> (bool, bool) {
        match out.unwrap() {
            RewriteResult::Unchanged => (false, false),
            RewriteResult::Changed(plan) => {
                let Operator::LogicalJoin(_) = &plan.op else {
                    panic!("rule must return a Join");
                };
                (
                    matches!(&plan.left().op, Operator::LogicalFilter(_)),
                    matches!(&plan.right().op, Operator::LogicalFilter(_)),
                )
            }
            RewriteResult::Rejected(_) => (false, false),
        }
    }

    /// Count IS NOT NULL conjuncts in a Filter's predicate (0 if not a Filter).
    fn not_null_count(expr: &OptExpr, arena: &ScalarArena) -> usize {
        let Operator::LogicalFilter(f) = &expr.op else {
            return 0;
        };
        not_null_count_scalar(arena, f.predicate)
    }

    fn not_null_count_scalar(arena: &ScalarArena, expr: ScalarId) -> usize {
        match arena.node(expr) {
            ScalarNode::BinaryOp {
                left,
                op: BinOp::And,
                right,
            } => not_null_count_scalar(arena, *left) + not_null_count_scalar(arena, *right),
            ScalarNode::IsNull { negated: true, .. } => 1,
            _ => 0,
        }
    }

    fn inner_eq_join_opt(
        arena: &mut ScalarArena,
        left_nullable: bool,
        right_nullable: bool,
    ) -> OptExpr {
        join_opt(
            arena,
            JoinKind::Inner,
            scan_opt("l", "tl", &[("a", 1, left_nullable)]),
            scan_opt("r", "tr", &[("b", 2, right_nullable)]),
            Some(eq_expr(
                col_typed("l", "a", 1, left_nullable),
                col_typed("r", "b", 2, right_nullable),
            )),
        )
    }

    #[test]
    fn join_type_safety_table() {
        let cases = [
            (JoinKind::Inner, true, true),
            (JoinKind::LeftSemi, false, true),
            (JoinKind::RightSemi, true, false),
            (JoinKind::LeftAnti, false, false),
            (JoinKind::RightAnti, false, false),
            (JoinKind::NullAwareLeftAnti, false, false),
            (JoinKind::LeftOuter, false, false),
            (JoinKind::RightOuter, false, false),
            (JoinKind::FullOuter, false, false),
            (JoinKind::Cross, false, false),
        ];
        for (jt, exp_l, exp_r) in cases {
            let mut arena = ScalarArena::new();
            let cond = if matches!(jt, JoinKind::Cross) {
                None
            } else {
                Some(eq_expr(
                    col_typed("l", "a", 1, true),
                    col_typed("r", "b", 2, true),
                ))
            };
            let plan = join_opt(
                &mut arena,
                jt,
                scan_opt("l", "tl", &[("a", 1, true)]),
                scan_opt("r", "tr", &[("b", 2, true)]),
                cond,
            );
            let mut ctx = make_ctx(arena);
            assert_eq!(
                side_filters(DeriveJoinNotNullPredicate.apply(plan, &mut ctx)),
                (exp_l, exp_r),
                "join type {jt:?}"
            );
        }
    }

    #[test]
    fn non_nullable_keys_are_skipped() {
        {
            let mut arena = ScalarArena::new();
            let plan = inner_eq_join_opt(&mut arena, false, false);
            let mut ctx = make_ctx(arena);
            assert_eq!(
                side_filters(DeriveJoinNotNullPredicate.apply(plan, &mut ctx)),
                (false, false)
            );
        }
        {
            // Only the nullable side gets a filter.
            let mut arena = ScalarArena::new();
            let plan = inner_eq_join_opt(&mut arena, true, false);
            let mut ctx = make_ctx(arena);
            assert_eq!(
                side_filters(DeriveJoinNotNullPredicate.apply(plan, &mut ctx)),
                (true, false)
            );
        }
    }

    #[test]
    fn composite_inner_key_derives_all_columns_on_each_side() {
        let mut arena = ScalarArena::new();
        let plan = join_opt(
            &mut arena,
            JoinKind::Inner,
            scan_opt("l", "tl", &[("a", 1, true), ("a2", 3, true)]),
            scan_opt("r", "tr", &[("b", 2, true), ("b2", 4, true)]),
            Some(and_expr(
                eq_expr(col_typed("l", "a", 1, true), col_typed("r", "b", 2, true)),
                eq_expr(col_typed("l", "a2", 3, true), col_typed("r", "b2", 4, true)),
            )),
        );
        let mut ctx = make_ctx(arena);
        let result = DeriveJoinNotNullPredicate.apply(plan, &mut ctx).unwrap();
        let RewriteResult::Changed(rewritten) = result else {
            panic!("expected Changed");
        };
        let Operator::LogicalJoin(_) = &rewritten.op else {
            panic!("expected join");
        };
        let arena_ref = ctx.scalar_arena();
        let arena = arena_ref.borrow();
        assert_eq!(not_null_count(rewritten.left(), &arena), 2);
        assert_eq!(not_null_count(rewritten.right(), &arena), 2);
    }

    #[test]
    fn idempotent_second_apply_is_noop() {
        let mut arena = ScalarArena::new();
        let plan = inner_eq_join_opt(&mut arena, true, true);
        let mut ctx = make_ctx(arena);
        let first = DeriveJoinNotNullPredicate.apply(plan, &mut ctx).unwrap();
        let RewriteResult::Changed(once) = first else {
            panic!("first apply expected Changed");
        };
        // Second application over the already-derived plan must not change it.
        let second = DeriveJoinNotNullPredicate.apply(once, &mut ctx).unwrap();
        assert!(matches!(second, RewriteResult::Unchanged));
    }

    #[test]
    fn idempotent_when_not_null_already_in_scan_predicates() {
        // State B: IS NOT NULL already pushed into scan.predicates (as
        // PushDownPredicateScan would do). The rule must NOT re-derive.
        let mut arena = ScalarArena::new();
        let not_null_pred_l = TypedExpr {
            kind: ExprKind::IsNull {
                expr: Box::new(col_typed("l", "a", 1, true)),
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let not_null_pred_r = TypedExpr {
            kind: ExprKind::IsNull {
                expr: Box::new(col_typed("r", "b", 2, true)),
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let pred_l_id = crate::sql::planner::optimizer_bridge::scalar::intern_typed(
            &mut arena,
            &not_null_pred_l,
        );
        let pred_r_id = crate::sql::planner::optimizer_bridge::scalar::intern_typed(
            &mut arena,
            &not_null_pred_r,
        );

        let left_scan = ScanOp {
            database: "default".to_string(),
            table: TableDef {
                name: "tl".to_string(),
                columns: vec![ColumnDef {
                    name: "a".to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: Some("l".to_string()),
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(1),
                name: "a".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                is_internal: false,
            }],
            predicates: vec![pred_l_id],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        };
        let right_scan = ScanOp {
            database: "default".to_string(),
            table: TableDef {
                name: "tr".to_string(),
                columns: vec![ColumnDef {
                    name: "b".to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: Some("r".to_string()),
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(2),
                name: "b".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                is_internal: false,
            }],
            predicates: vec![pred_r_id],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        };

        let cond = eq_expr(col_typed("l", "a", 1, true), col_typed("r", "b", 2, true));
        let cond_id =
            crate::sql::planner::optimizer_bridge::scalar::intern_typed(&mut arena, &cond);
        let plan = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(cond_id),
            }),
            vec![
                OptExpr::leaf(Operator::LogicalScan(left_scan)),
                OptExpr::leaf(Operator::LogicalScan(right_scan)),
            ],
        );
        let mut ctx = make_ctx(arena);
        let result = DeriveJoinNotNullPredicate.apply(plan, &mut ctx).unwrap();
        assert!(matches!(result, RewriteResult::Unchanged));
    }

    #[test]
    fn non_equi_and_missing_condition_skipped() {
        {
            let mut arena = ScalarArena::new();
            let plan = join_opt(
                &mut arena,
                JoinKind::Inner,
                scan_opt("l", "tl", &[("a", 1, true)]),
                scan_opt("r", "tr", &[("b", 2, true)]),
                None,
            );
            let mut ctx = make_ctx(arena);
            assert!(matches!(
                DeriveJoinNotNullPredicate.apply(plan, &mut ctx).unwrap(),
                RewriteResult::Unchanged
            ));
        }
        {
            let mut arena = ScalarArena::new();
            let gt = TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(col_typed("l", "a", 1, true)),
                    op: BinOp::Gt,
                    right: Box::new(col_typed("r", "b", 2, true)),
                },
                data_type: DataType::Boolean,
                nullable: true,
            };
            let plan = join_opt(
                &mut arena,
                JoinKind::Inner,
                scan_opt("l", "tl", &[("a", 1, true)]),
                scan_opt("r", "tr", &[("b", 2, true)]),
                Some(gt),
            );
            let mut ctx = make_ctx(arena);
            assert!(matches!(
                DeriveJoinNotNullPredicate.apply(plan, &mut ctx).unwrap(),
                RewriteResult::Unchanged
            ));
        }
    }
}
