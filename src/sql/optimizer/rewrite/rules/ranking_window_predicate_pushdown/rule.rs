use std::collections::HashSet;

use crate::sql::analysis::{BinOp, LiteralValue};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{Operator, ScalarWindowSpec, SortOp, WindowOp};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::scalar_expr;

pub(crate) struct RankingWindowPredicatePushdownRule;

/// Map a window function name to its `SortTopNType` variant.
/// Returns `None` for non-ranking functions (e.g. avg, sum, lead, lag).
fn ranking_topn_type(name: &str) -> Option<crate::exec::node::sort::SortTopNType> {
    use crate::exec::node::sort::SortTopNType::*;
    match name.to_ascii_lowercase().as_str() {
        "row_number" => Some(RowNumber),
        "rank" => Some(Rank),
        "dense_rank" => Some(DenseRank),
        _ => None,
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct WindowSignatureKey {
    partition_by: Vec<ScalarId>,
    order_by: Vec<(ScalarId, bool, bool)>,
    window_frame: String,
}

/// Returns the number of distinct (partition_by, order_by, frame) groups.
fn count_unique_signatures(window_exprs: &[ScalarWindowSpec]) -> usize {
    window_exprs
        .iter()
        .map(|expr| WindowSignatureKey {
            partition_by: expr.partition_by.clone(),
            order_by: expr
                .order_by
                .iter()
                .map(|item| (item.expr, item.asc, item.nulls_first))
                .collect(),
            window_frame: format!("{:?}", expr.window_frame),
        })
        .collect::<HashSet<_>>()
        .len()
}

impl LogicalRewriteRule for RankingWindowPredicatePushdownRule {
    fn name(&self) -> &'static str {
        "RankingWindowPredicatePushdown"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let Operator::LogicalFilter(_) = &expr.op else {
            return false;
        };
        let Some(filter_child) = expr.children.first() else {
            return false;
        };
        let window_expr = match &filter_child.op {
            Operator::LogicalWindow(_) => filter_child,
            Operator::LogicalProject(_) => {
                let Some(w) = filter_child.children.first() else {
                    return false;
                };
                if !matches!(&w.op, Operator::LogicalWindow(_)) {
                    return false;
                }
                w
            }
            _ => return false,
        };
        let Some(sort_child) = window_expr.children.first() else {
            return false;
        };
        let Operator::LogicalSort(sort) = &sort_child.op else {
            return false;
        };
        if sort.analytic_partition_exprs.is_empty() {
            return false;
        }
        // Check all window exprs are ranking functions (need arena for nothing here,
        // the name field is directly on ScalarWindowSpec).
        let Operator::LogicalWindow(window) = &window_expr.op else {
            return false;
        };
        if window.window_exprs.is_empty() {
            return false;
        }
        // matches() is a quick structural check — full all-ranking guard is in apply().
        let _ = ctx;
        true
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        // --- Step 1: Destructure Filter -> optional Project -> Window -> Sort ---
        let Operator::LogicalFilter(ref filter_op) = expr.op else {
            return Ok(RewriteResult::Unchanged);
        };
        let filter_predicate_id = filter_op.predicate;

        let filter_child = match expr.children.first() {
            Some(c) => c,
            None => return Ok(RewriteResult::Unchanged),
        };

        // Resolve: Filter -> Window, or Filter -> Project -> Window
        let (has_project, window_expr_ref, project_expr_ref) = match &filter_child.op {
            Operator::LogicalWindow(_) => (false, filter_child, None),
            Operator::LogicalProject(_) => {
                let Some(w) = filter_child.children.first() else {
                    return Ok(RewriteResult::Unchanged);
                };
                if !matches!(&w.op, Operator::LogicalWindow(_)) {
                    return Ok(RewriteResult::Unchanged);
                }
                (true, w, Some(filter_child))
            }
            _ => return Ok(RewriteResult::Unchanged),
        };

        let Operator::LogicalWindow(window_op) = &window_expr_ref.op else {
            return Ok(RewriteResult::Unchanged);
        };
        let sort_expr_ref = match window_expr_ref.children.first() {
            Some(s) => s,
            None => return Ok(RewriteResult::Unchanged),
        };
        let Operator::LogicalSort(sort_op) = &sort_expr_ref.op else {
            return Ok(RewriteResult::Unchanged);
        };

        // --- Step 2: Idempotency guard ---
        if sort_op.partition_limit.is_some() {
            return Ok(RewriteResult::Unchanged);
        }

        // --- Step 3: All-ranking guard ---
        if window_op.window_exprs.is_empty()
            || window_op
                .window_exprs
                .iter()
                .any(|w| ranking_topn_type(&w.name).is_none())
        {
            return Ok(RewriteResult::Unchanged);
        }

        // --- Step 3b: Single-signature guard ---
        let arena_rc = ctx.scalar_arena();
        if count_unique_signatures(&window_op.window_exprs) != 1 {
            return Ok(RewriteResult::Unchanged);
        }

        // --- Step 4: Non-empty partition guard ---
        if sort_op.analytic_partition_exprs.is_empty() {
            return Ok(RewriteResult::Unchanged);
        }

        // --- Step 5: Find a ranking window expr with a finite upper bound ---
        // Window output columns are laid out as input columns followed by one
        // result column for each window expression. Keep this in sync with
        // scalar_bridge::materialize_window_exprs.
        let Some(window_output_start) = window_op
            .output_columns
            .len()
            .checked_sub(window_op.window_exprs.len())
        else {
            return Ok(RewriteResult::Unchanged);
        };
        let found = {
            let arena = arena_rc.borrow();
            window_op
                .window_exprs
                .iter()
                .enumerate()
                .find_map(|(i, w_expr)| {
                    let window_output_col_id = window_op
                        .output_columns
                        .get(window_output_start + i)
                        .map(|oc| oc.column_id)
                        .unwrap_or(ColumnId::UNSET);
                    if window_output_col_id == ColumnId::UNSET {
                        return None;
                    }

                    // Determine which ColumnId the filter predicate references for this expr.
                    let filter_col_id = if let Some(project_expr) = project_expr_ref {
                        let Operator::LogicalProject(project_op) = &project_expr.op else {
                            return None;
                        };
                        project_op.items.iter().find_map(|item| {
                            if scalar_expr::column_id(&arena, item.expr)
                                == Some(window_output_col_id)
                            {
                                return Some(item.output_column_id);
                            }
                            None
                        })?
                    } else {
                        window_output_col_id
                    };

                    let k = rank_upper_bound(&arena, filter_predicate_id, filter_col_id)?;
                    Some((
                        k,
                        ranking_topn_type(&w_expr.name).expect("all-ranking guard passed"),
                    ))
                })
        };

        let Some((k, topn_type)) = found else {
            return Ok(RewriteResult::Unchanged);
        };

        // --- Step 6: Rebuild the tree with partition_limit / topn_type on the Sort ---
        // Clone sort's children (the subtree below Sort stays the same).
        let sort_children = sort_expr_ref.children.clone();
        let sort_required = sort_expr_ref.required_output_columns.clone();
        let new_sort = OptExpr {
            op: Operator::LogicalSort(SortOp {
                items: sort_op.items.clone(),
                analytic_partition_exprs: sort_op.analytic_partition_exprs.clone(),
                partition_limit: Some(k),
                topn_type: Some(topn_type),
            }),
            children: sort_children,
            required_output_columns: sort_required,
        };

        // Rebuild Window over the new Sort.
        let window_required = window_expr_ref.required_output_columns.clone();
        let new_window = OptExpr {
            op: Operator::LogicalWindow(WindowOp {
                window_exprs: window_op.window_exprs.clone(),
                output_columns: window_op.output_columns.clone(),
            }),
            children: vec![new_sort],
            required_output_columns: window_required,
        };

        // Rebuild Project (if present) over the new Window.
        let mid = if has_project {
            let project_expr = project_expr_ref.unwrap();
            OptExpr {
                op: project_expr.op.clone(),
                children: vec![new_window],
                required_output_columns: project_expr.required_output_columns.clone(),
            }
        } else {
            new_window
        };

        // Rebuild Filter over mid.
        let new_filter = OptExpr {
            op: expr.op.clone(),
            children: vec![mid],
            required_output_columns: expr.required_output_columns,
        };

        Ok(RewriteResult::Changed(new_filter))
    }
}

/// Smallest finite upper bound K (>= 1) such that the conjunctive predicate can
/// only pass rows with rank_col <= K.  Returns None if no finite positive bound
/// exists (e.g., lower-bound-only predicates, K <= 0, or no reference to rank_col).
pub(crate) fn rank_upper_bound(
    arena: &ScalarArena,
    predicate: ScalarId,
    rank_col: ColumnId,
) -> Option<usize> {
    let mut best: Option<i64> = None;
    let mut conjuncts = Vec::new();
    scalar_expr::split_conjuncts(arena, predicate, &mut conjuncts);
    for conj in conjuncts {
        if let Some(k) = conjunct_upper_bound(arena, conj, rank_col) {
            best = Some(best.map_or(k, |b| b.min(k)));
        }
    }
    match best {
        Some(k) if k >= 1 => usize::try_from(k).ok(),
        _ => None,
    }
}

fn is_rank_col(arena: &ScalarArena, expr: ScalarId, rank_col: ColumnId) -> bool {
    scalar_expr::column_id(arena, expr) == Some(rank_col)
}

fn int_lit(arena: &ScalarArena, expr: ScalarId) -> Option<i64> {
    match arena.node(expr) {
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(v))) => Some(*v),
        _ => None,
    }
}

fn conjunct_upper_bound(arena: &ScalarArena, expr: ScalarId, rank_col: ColumnId) -> Option<i64> {
    match arena.node(expr) {
        ScalarNode::BinaryOp { left, op, right } => {
            let (lit, col_on_left) = if is_rank_col(arena, *left, rank_col) {
                (int_lit(arena, *right)?, true)
            } else if is_rank_col(arena, *right, rank_col) {
                (int_lit(arena, *left)?, false)
            } else {
                return None;
            };
            match (op, col_on_left) {
                // rank_col <= lit  or  lit >= rank_col
                (BinOp::Le, true) | (BinOp::Ge, false) => Some(lit),
                // rank_col < lit  or  lit > rank_col
                (BinOp::Lt, true) | (BinOp::Gt, false) => Some(lit - 1),
                // rank_col = lit  or  lit = rank_col
                (BinOp::Eq, _) => Some(lit),
                _ => None,
            }
        }
        // BETWEEN low AND high: upper bound is `high`
        ScalarNode::Between {
            child,
            high,
            negated: false,
            ..
        } if is_rank_col(arena, *child, rank_col) => int_lit(arena, *high),
        // IN (v1, v2, ...): upper bound is the max value in the list
        ScalarNode::InList {
            child,
            list,
            negated: false,
        } if is_rank_col(arena, *child, rank_col) => list
            .iter()
            .map(|item| int_lit(arena, *item))
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .max(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use super::{RankingWindowPredicatePushdownRule, rank_upper_bound};
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, SortItem, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        FilterOp, LogicalJoinOp, Operator, ProjectOp, ScalarProjectItem, ScalarWindowSpec, SortOp,
        ValuesOp, WindowOp,
    };
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::optimizer::scalar::{self, ScalarArena, ScalarId, SortKey};

    fn make_ctx(arena: ScalarArena) -> RewriteContext {
        let mut ctx = RewriteContext::new(RewriteConsumer::Query);
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));
        ctx
    }

    fn col_typed(id: ColumnId) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: format!("rk_{}", id.0),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn int_typed(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn binop_typed(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn le_typed(col: TypedExpr, v: i64) -> TypedExpr {
        binop_typed(col, BinOp::Le, int_typed(v))
    }

    fn lt_typed(col: TypedExpr, v: i64) -> TypedExpr {
        binop_typed(col, BinOp::Lt, int_typed(v))
    }

    fn eq_typed(col: TypedExpr, v: i64) -> TypedExpr {
        binop_typed(col, BinOp::Eq, int_typed(v))
    }

    fn ge_typed(col: TypedExpr, v: i64) -> TypedExpr {
        binop_typed(col, BinOp::Ge, int_typed(v))
    }

    fn between_typed(expr: TypedExpr, low_v: i64, high_v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Between {
                expr: Box::new(expr),
                low: Box::new(int_typed(low_v)),
                high: Box::new(int_typed(high_v)),
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn in_list_typed(expr: TypedExpr, values: &[i64]) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::InList {
                expr: Box::new(expr),
                list: values.iter().map(|&v| int_typed(v)).collect(),
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn rank_upper_bound_typed(predicate: TypedExpr, rank_col: ColumnId) -> Option<usize> {
        let mut arena = ScalarArena::new();
        let predicate = scalar::intern_typed(&mut arena, &predicate);
        rank_upper_bound(&arena, predicate, rank_col)
    }

    fn empty_values_opt() -> OptExpr {
        OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        }))
    }

    fn output_col(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn make_sort_key(arena: &mut ScalarArena, id: ColumnId) -> SortKey {
        SortKey {
            expr: scalar::intern_typed(arena, &col_typed(id)),
            asc: true,
            nulls_first: true,
            display: None,
        }
    }

    fn make_sort_opt(arena: &mut ScalarArena, p_id: ColumnId) -> OptExpr {
        let partition_expr = scalar::intern_typed(arena, &col_typed(p_id));
        let sort_key = make_sort_key(arena, p_id);
        OptExpr::new(
            Operator::LogicalSort(SortOp {
                items: vec![sort_key],
                analytic_partition_exprs: vec![partition_expr],
                partition_limit: None,
                topn_type: None,
            }),
            vec![empty_values_opt()],
        )
    }

    fn make_sort_opt_with_limit(arena: &mut ScalarArena, p_id: ColumnId, limit: usize) -> OptExpr {
        use crate::exec::node::sort::SortTopNType;
        let partition_expr = scalar::intern_typed(arena, &col_typed(p_id));
        let sort_key = make_sort_key(arena, p_id);
        OptExpr::new(
            Operator::LogicalSort(SortOp {
                items: vec![sort_key],
                analytic_partition_exprs: vec![partition_expr],
                partition_limit: Some(limit),
                topn_type: Some(SortTopNType::Rank),
            }),
            vec![empty_values_opt()],
        )
    }

    fn make_sort_no_partition_opt(arena: &mut ScalarArena, p_id: ColumnId) -> OptExpr {
        let sort_key = make_sort_key(arena, p_id);
        OptExpr::new(
            Operator::LogicalSort(SortOp {
                items: vec![sort_key],
                analytic_partition_exprs: vec![],
                partition_limit: None,
                topn_type: None,
            }),
            vec![empty_values_opt()],
        )
    }

    fn make_window_spec_opt(
        arena: &mut ScalarArena,
        fn_name: &str,
        p_id: ColumnId,
    ) -> ScalarWindowSpec {
        let partition_expr = scalar::intern_typed(arena, &col_typed(p_id));
        let sort_key = make_sort_key(arena, p_id);
        ScalarWindowSpec {
            name: fn_name.to_string(),
            args: vec![],
            distinct: false,
            partition_by: vec![partition_expr],
            order_by: vec![sort_key],
            window_frame: None,
            ignore_nulls: false,
        }
    }

    fn make_window_spec_opt_with_order(
        arena: &mut ScalarArena,
        fn_name: &str,
        p_id: ColumnId,
        order_id: ColumnId,
    ) -> ScalarWindowSpec {
        let partition_expr = scalar::intern_typed(arena, &col_typed(p_id));
        let order_key = make_sort_key(arena, order_id);
        ScalarWindowSpec {
            name: fn_name.to_string(),
            args: vec![],
            distinct: false,
            partition_by: vec![partition_expr],
            order_by: vec![order_key],
            window_frame: None,
            ignore_nulls: false,
        }
    }

    fn window_opt(
        input: OptExpr,
        window_exprs: Vec<ScalarWindowSpec>,
        output_columns: Vec<OutputColumn>,
    ) -> OptExpr {
        OptExpr::new(
            Operator::LogicalWindow(WindowOp {
                window_exprs,
                output_columns,
            }),
            vec![input],
        )
    }

    fn filter_opt(arena: &mut ScalarArena, input: OptExpr, predicate: TypedExpr) -> OptExpr {
        let pred_id = scalar::intern_typed(arena, &predicate);
        OptExpr::new(
            Operator::LogicalFilter(FilterOp { predicate: pred_id }),
            vec![input],
        )
    }

    fn project_opt(
        arena: &mut ScalarArena,
        input: OptExpr,
        items: Vec<(TypedExpr, ColumnId)>,
    ) -> OptExpr {
        let scalar_items = items
            .into_iter()
            .map(|(expr, out_id)| {
                let expr_id = scalar::intern_typed(arena, &expr);
                ScalarProjectItem {
                    expr: expr_id,
                    output_name: format!("c_{}", out_id.0),
                    output_column_id: out_id,
                    expr_display: None,
                }
            })
            .collect();
        OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: scalar_items,
                output_qualifier: None,
            }),
            vec![input],
        )
    }

    fn make_filter_window_sort_opt(
        arena: &mut ScalarArena,
        fn_name: &str,
        rk_id: ColumnId,
        p_id: ColumnId,
        k: i64,
    ) -> OptExpr {
        let sort = make_sort_opt(arena, p_id);
        let w_spec = make_window_spec_opt(arena, fn_name, p_id);
        let window = window_opt(sort, vec![w_spec], vec![output_col(rk_id, fn_name)]);
        filter_opt(
            arena,
            window,
            binop_typed(col_typed(rk_id), BinOp::Le, int_typed(k)),
        )
    }

    fn extract_sort_from_changed(result: RewriteResult) -> SortOp {
        if let RewriteResult::Changed(plan) = result {
            let Operator::LogicalFilter(_) = &plan.op else {
                panic!("expected Changed(Filter(...)), got op {:?}", plan.op);
            };
            let filter_child = plan.children.first().expect("filter must have child");
            let window_expr = match &filter_child.op {
                Operator::LogicalWindow(_) => filter_child,
                Operator::LogicalProject(_) => {
                    let w = filter_child
                        .children
                        .first()
                        .expect("project must have child");
                    assert!(
                        matches!(&w.op, Operator::LogicalWindow(_)),
                        "expected Window under Project"
                    );
                    w
                }
                _ => panic!("expected Window or Project under Filter"),
            };
            let sort_expr = window_expr
                .children
                .first()
                .expect("window must have child");
            let Operator::LogicalSort(sort) = &sort_expr.op else {
                panic!("expected Sort under Window");
            };
            sort.clone()
        } else {
            panic!("expected Changed(...), got {:?}", result);
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: rule is recognized by the registry
    // -----------------------------------------------------------------------

    #[test]
    fn ranking_window_rule_is_known() {
        assert!(
            crate::sql::optimizer::rewrite::registry::is_known_rewrite_rule_name(
                "RankingWindowPredicatePushdown"
            )
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: rank_upper_bound extracts <=, <, =, BETWEEN, IN correctly
    //         and returns None for lower-bound-only / K<=0 / other column
    // -----------------------------------------------------------------------

    #[test]
    fn rank_upper_bound_extracts_le_lt_eq_between_in() {
        let rk = ColumnId::new_for_test(7);
        let other = ColumnId::new_for_test(99);

        assert_eq!(
            rank_upper_bound_typed(le_typed(col_typed(rk), 5), rk),
            Some(5)
        );
        assert_eq!(
            rank_upper_bound_typed(lt_typed(col_typed(rk), 5), rk),
            Some(4)
        );
        assert_eq!(
            rank_upper_bound_typed(eq_typed(col_typed(rk), 3), rk),
            Some(3)
        );
        assert_eq!(
            rank_upper_bound_typed(between_typed(col_typed(rk), 2, 9), rk),
            Some(9)
        );
        assert_eq!(
            rank_upper_bound_typed(in_list_typed(col_typed(rk), &[1, 3, 5]), rk),
            Some(5)
        );
        assert_eq!(rank_upper_bound_typed(ge_typed(col_typed(rk), 5), rk), None);
        assert_eq!(rank_upper_bound_typed(le_typed(col_typed(rk), 0), rk), None);
        assert_eq!(
            rank_upper_bound_typed(le_typed(col_typed(other), 5), rk),
            None
        );
    }

    #[test]
    fn ranking_window_predicate_pushdown_rule_is_constructable() {
        let _ = RankingWindowPredicatePushdownRule;
    }

    // -----------------------------------------------------------------------
    // Test 3: fires on rank() per group — sets partition_limit + topn_type
    // -----------------------------------------------------------------------

    #[test]
    fn fires_on_rank_per_group_sets_partition_limit() {
        use crate::exec::node::sort::SortTopNType;
        let rk_id = ColumnId::new_for_test(1);
        let p_id = ColumnId::new_for_test(2);
        let mut arena = ScalarArena::new();
        let plan = make_filter_window_sort_opt(&mut arena, "rank", rk_id, p_id, 2);

        let rule = RankingWindowPredicatePushdownRule;
        let mut ctx = make_ctx(arena);
        assert!(rule.matches(&plan, &ctx), "matches() must return true");

        let result = rule.apply(plan, &mut ctx).unwrap();
        let sort = extract_sort_from_changed(result);
        assert_eq!(sort.partition_limit, Some(2));
        assert_eq!(sort.topn_type, Some(SortTopNType::Rank));
    }

    #[test]
    fn maps_window_expr_to_trailing_output_column() {
        use crate::exec::node::sort::SortTopNType;
        let rk_id = ColumnId::new_for_test(1);
        let p_id = ColumnId::new_for_test(2);
        let mut arena = ScalarArena::new();
        let sort = make_sort_opt(&mut arena, p_id);
        let w_spec = make_window_spec_opt(&mut arena, "rank", p_id);
        let window = window_opt(
            sort,
            vec![w_spec],
            vec![output_col(p_id, "p"), output_col(rk_id, "rk")],
        );
        let plan = filter_opt(
            &mut arena,
            window,
            binop_typed(col_typed(rk_id), BinOp::Le, int_typed(2)),
        );

        let mut ctx = make_ctx(arena);
        let result = RankingWindowPredicatePushdownRule
            .apply(plan, &mut ctx)
            .unwrap();
        let sort = extract_sort_from_changed(result);
        assert_eq!(sort.partition_limit, Some(2));
        assert_eq!(sort.topn_type, Some(SortTopNType::Rank));
    }

    #[test]
    fn fires_when_window_output_columns_include_passthrough_columns() {
        use crate::exec::node::sort::SortTopNType;
        let region_id = ColumnId::new_for_test(10);
        let amount_id = ColumnId::new_for_test(11);
        let rank_id = ColumnId::new_for_test(12);

        let mut arena = ScalarArena::new();
        let sort = make_sort_opt(&mut arena, region_id);
        let window_spec = make_window_spec_opt_with_order(&mut arena, "rank", region_id, amount_id);
        let window = window_opt(
            sort,
            vec![window_spec],
            vec![
                output_col(region_id, "region"),
                output_col(amount_id, "amount"),
                output_col(rank_id, "rk"),
            ],
        );
        let plan = filter_opt(
            &mut arena,
            window,
            binop_typed(col_typed(rank_id), BinOp::Le, int_typed(2)),
        );

        let mut ctx = make_ctx(arena);
        let sort = extract_sort_from_changed(
            RankingWindowPredicatePushdownRule
                .apply(plan, &mut ctx)
                .unwrap(),
        );
        assert_eq!(sort.partition_limit, Some(2));
        assert_eq!(sort.topn_type, Some(SortTopNType::Rank));
    }

    // -----------------------------------------------------------------------
    // Test 4: fires for row_number and dense_rank too
    // -----------------------------------------------------------------------

    #[test]
    fn fires_for_row_number_and_dense_rank() {
        use crate::exec::node::sort::SortTopNType;
        let rk_id = ColumnId::new_for_test(10);
        let p_id = ColumnId::new_for_test(11);

        {
            let mut arena = ScalarArena::new();
            let plan = make_filter_window_sort_opt(&mut arena, "row_number", rk_id, p_id, 3);
            let mut ctx = make_ctx(arena);
            let sort = extract_sort_from_changed(
                RankingWindowPredicatePushdownRule
                    .apply(plan, &mut ctx)
                    .unwrap(),
            );
            assert_eq!(sort.partition_limit, Some(3));
            assert_eq!(sort.topn_type, Some(SortTopNType::RowNumber));
        }
        {
            let mut arena = ScalarArena::new();
            let plan = make_filter_window_sort_opt(&mut arena, "dense_rank", rk_id, p_id, 5);
            let mut ctx = make_ctx(arena);
            let sort = extract_sort_from_changed(
                RankingWindowPredicatePushdownRule
                    .apply(plan, &mut ctx)
                    .unwrap(),
            );
            assert_eq!(sort.partition_limit, Some(5));
            assert_eq!(sort.topn_type, Some(SortTopNType::DenseRank));
        }
    }

    // -----------------------------------------------------------------------
    // Test 5: rejects when window has a non-ranking aggregate window expr
    // -----------------------------------------------------------------------

    #[test]
    fn rejects_when_window_has_aggregate_over() {
        let rk_id = ColumnId::new_for_test(20);
        let p_id = ColumnId::new_for_test(21);
        let avg_id = ColumnId::new_for_test(22);
        let mut arena = ScalarArena::new();
        let sort = make_sort_opt(&mut arena, p_id);

        let window = window_opt(
            sort,
            vec![
                make_window_spec_opt(&mut arena, "rank", p_id),
                make_window_spec_opt(&mut arena, "avg", p_id),
            ],
            vec![output_col(rk_id, "rank"), output_col(avg_id, "avg")],
        );
        let plan = filter_opt(
            &mut arena,
            window,
            binop_typed(col_typed(rk_id), BinOp::Le, int_typed(2)),
        );
        let mut ctx = make_ctx(arena);
        assert!(matches!(
            RankingWindowPredicatePushdownRule
                .apply(plan, &mut ctx)
                .unwrap(),
            RewriteResult::Unchanged
        ));
    }

    // -----------------------------------------------------------------------
    // Test 6: rejects when sort.analytic_partition_exprs is empty
    // -----------------------------------------------------------------------

    #[test]
    fn rejects_empty_partition_by() {
        let rk_id = ColumnId::new_for_test(30);
        let p_id = ColumnId::new_for_test(31);
        let mut arena = ScalarArena::new();
        let sort = make_sort_no_partition_opt(&mut arena, p_id);
        let window = window_opt(
            sort,
            vec![make_window_spec_opt(&mut arena, "rank", p_id)],
            vec![output_col(rk_id, "rank")],
        );
        let plan = filter_opt(
            &mut arena,
            window,
            binop_typed(col_typed(rk_id), BinOp::Le, int_typed(2)),
        );

        let rule = RankingWindowPredicatePushdownRule;
        let ctx = make_ctx(arena);
        assert!(
            !rule.matches(&plan, &ctx),
            "matches() must return false for empty partition"
        );
    }

    // -----------------------------------------------------------------------
    // Test 7: rejects when predicate has no finite upper bound (rk >= 2)
    // -----------------------------------------------------------------------

    #[test]
    fn rejects_no_upper_bound() {
        let rk_id = ColumnId::new_for_test(40);
        let p_id = ColumnId::new_for_test(41);
        let mut arena = ScalarArena::new();
        let sort = make_sort_opt(&mut arena, p_id);
        let window = window_opt(
            sort,
            vec![make_window_spec_opt(&mut arena, "rank", p_id)],
            vec![output_col(rk_id, "rank")],
        );
        let plan = filter_opt(
            &mut arena,
            window,
            binop_typed(col_typed(rk_id), BinOp::Ge, int_typed(2)),
        );
        let mut ctx = make_ctx(arena);
        assert!(matches!(
            RankingWindowPredicatePushdownRule
                .apply(plan, &mut ctx)
                .unwrap(),
            RewriteResult::Unchanged
        ));
    }

    // -----------------------------------------------------------------------
    // Test 8: idempotent — sort already has partition_limit set
    // -----------------------------------------------------------------------

    #[test]
    fn idempotent_when_sort_already_has_partition_limit() {
        let rk_id = ColumnId::new_for_test(50);
        let p_id = ColumnId::new_for_test(51);
        let mut arena = ScalarArena::new();
        let sort = make_sort_opt_with_limit(&mut arena, p_id, 2);
        let window = window_opt(
            sort,
            vec![make_window_spec_opt(&mut arena, "rank", p_id)],
            vec![output_col(rk_id, "rank")],
        );
        let plan = filter_opt(
            &mut arena,
            window,
            binop_typed(col_typed(rk_id), BinOp::Le, int_typed(2)),
        );
        let mut ctx = make_ctx(arena);
        assert!(matches!(
            RankingWindowPredicatePushdownRule
                .apply(plan, &mut ctx)
                .unwrap(),
            RewriteResult::Unchanged
        ));
    }

    // -----------------------------------------------------------------------
    // Test 9: sees through a bare passthrough Project
    // -----------------------------------------------------------------------

    #[test]
    fn sees_through_bare_passthrough_project() {
        use crate::exec::node::sort::SortTopNType;
        let rk_id = ColumnId::new_for_test(60);
        let proj_rk_id = ColumnId::new_for_test(61);
        let p_id = ColumnId::new_for_test(62);
        let mut arena = ScalarArena::new();

        let sort = make_sort_opt(&mut arena, p_id);
        let window = window_opt(
            sort,
            vec![make_window_spec_opt(&mut arena, "rank", p_id)],
            vec![output_col(rk_id, "rank")],
        );
        // Project: proj_rk_id <- rk_id (bare passthrough)
        let project = project_opt(&mut arena, window, vec![(col_typed(rk_id), proj_rk_id)]);
        // Filter references the projected column (proj_rk_id)
        let plan = filter_opt(
            &mut arena,
            project,
            binop_typed(col_typed(proj_rk_id), BinOp::Le, int_typed(3)),
        );

        let rule = RankingWindowPredicatePushdownRule;
        let mut ctx = make_ctx(arena);
        assert!(
            rule.matches(&plan, &ctx),
            "matches() must fire on Filter->Project->Window->Sort"
        );

        let result = rule.apply(plan, &mut ctx).unwrap();
        let sort = extract_sort_from_changed(result);
        assert_eq!(sort.partition_limit, Some(3));
        assert_eq!(sort.topn_type, Some(SortTopNType::Rank));
    }

    // -----------------------------------------------------------------------
    // Test: rejects mixed ranking+aggregate window (tpc-ds q47/q57 shape)
    // -----------------------------------------------------------------------

    #[test]
    fn rejects_mixed_ranking_and_aggregate_window() {
        let rk_id = ColumnId::new_for_test(80);
        let avg_id = ColumnId::new_for_test(81);
        let p_id = ColumnId::new_for_test(82);
        let mut arena = ScalarArena::new();

        let sort = make_sort_opt(&mut arena, p_id);
        let window = window_opt(
            sort,
            vec![
                make_window_spec_opt(&mut arena, "rank", p_id),
                make_window_spec_opt(&mut arena, "avg", p_id),
            ],
            vec![output_col(rk_id, "rank"), output_col(avg_id, "avg")],
        );
        let plan = filter_opt(
            &mut arena,
            window,
            binop_typed(col_typed(rk_id), BinOp::Le, int_typed(2)),
        );

        let rule = RankingWindowPredicatePushdownRule;
        let mut ctx = make_ctx(arena);
        assert!(
            rule.matches(&plan, &ctx),
            "matches() should fire — the structural shape is valid"
        );
        assert!(
            matches!(
                RankingWindowPredicatePushdownRule
                    .apply(plan, &mut ctx)
                    .unwrap(),
                RewriteResult::Unchanged
            ),
            "apply() must return Unchanged when window contains a non-ranking expr"
        );
    }

    // -----------------------------------------------------------------------
    // Test 10: rejects when the Project transforms the rank column (not bare)
    // -----------------------------------------------------------------------

    #[test]
    fn rejects_when_project_transforms_rank_col() {
        let rk_id = ColumnId::new_for_test(70);
        let proj_rk_id = ColumnId::new_for_test(71);
        let p_id = ColumnId::new_for_test(72);
        let mut arena = ScalarArena::new();

        let sort = make_sort_opt(&mut arena, p_id);
        let window = window_opt(
            sort,
            vec![make_window_spec_opt(&mut arena, "rank", p_id)],
            vec![output_col(rk_id, "rank")],
        );
        // Project: proj_rk_id <- rk_id + 1 (NOT a bare passthrough)
        let transformed_expr = binop_typed(col_typed(rk_id), BinOp::Add, int_typed(1));
        let project = project_opt(&mut arena, window, vec![(transformed_expr, proj_rk_id)]);
        let plan = filter_opt(
            &mut arena,
            project,
            binop_typed(col_typed(proj_rk_id), BinOp::Le, int_typed(3)),
        );
        let mut ctx = make_ctx(arena);
        assert!(matches!(
            RankingWindowPredicatePushdownRule
                .apply(plan, &mut ctx)
                .unwrap(),
            RewriteResult::Unchanged
        ));
    }

    // -----------------------------------------------------------------------
    // Test: rejects multiple ranking fns with DIFFERENT ORDER BY (C1 bug shape)
    // -----------------------------------------------------------------------

    #[test]
    fn rejects_multiple_ranking_signatures_different_order() {
        let rka_id = ColumnId::new_for_test(90);
        let rkb_id = ColumnId::new_for_test(91);
        let p_id = ColumnId::new_for_test(92);
        let a_id = ColumnId::new_for_test(93);
        let b_id = ColumnId::new_for_test(94);
        let mut arena = ScalarArena::new();

        // Sort keyed on partition=[p_id], order=[a_id]
        let partition_expr = scalar::intern_typed(&mut arena, &col_typed(p_id));
        let sort_key_p = make_sort_key(&mut arena, p_id);
        let sort_key_a = make_sort_key(&mut arena, a_id);
        let sort = OptExpr::new(
            Operator::LogicalSort(SortOp {
                items: vec![sort_key_p, sort_key_a],
                analytic_partition_exprs: vec![partition_expr],
                partition_limit: None,
                topn_type: None,
            }),
            vec![empty_values_opt()],
        );

        let w_a = make_window_spec_opt_with_order(&mut arena, "rank", p_id, a_id);
        let w_b = make_window_spec_opt_with_order(&mut arena, "rank", p_id, b_id);

        let window = window_opt(
            sort,
            vec![w_a, w_b],
            vec![output_col(rka_id, "rka"), output_col(rkb_id, "rkb")],
        );
        // Filter on the SECOND ranking expr's column (rkb <= 2)
        let plan = filter_opt(
            &mut arena,
            window,
            binop_typed(col_typed(rkb_id), BinOp::Le, int_typed(2)),
        );

        let rule = RankingWindowPredicatePushdownRule;
        let mut ctx = make_ctx(arena);
        assert!(
            rule.matches(&plan, &ctx),
            "matches() should fire on this structural shape"
        );
        assert!(
            matches!(
                RankingWindowPredicatePushdownRule
                    .apply(plan, &mut ctx)
                    .unwrap(),
                RewriteResult::Unchanged
            ),
            "apply() must return Unchanged when ranking fns have different ORDER BY"
        );
    }

    // -----------------------------------------------------------------------
    // Test: fires when two ranking fns share the SAME (partition_by, order_by)
    // -----------------------------------------------------------------------

    #[test]
    fn fires_for_same_signature_multi_fn() {
        use crate::exec::node::sort::SortTopNType;
        let rk_id = ColumnId::new_for_test(100);
        let drk_id = ColumnId::new_for_test(101);
        let p_id = ColumnId::new_for_test(102);
        let o_id = ColumnId::new_for_test(103);
        let mut arena = ScalarArena::new();

        let partition_expr = scalar::intern_typed(&mut arena, &col_typed(p_id));
        let sort_key_p = make_sort_key(&mut arena, p_id);
        let sort_key_o = make_sort_key(&mut arena, o_id);
        let sort = OptExpr::new(
            Operator::LogicalSort(SortOp {
                items: vec![sort_key_p, sort_key_o],
                analytic_partition_exprs: vec![partition_expr],
                partition_limit: None,
                topn_type: None,
            }),
            vec![empty_values_opt()],
        );

        let w_rank = make_window_spec_opt_with_order(&mut arena, "rank", p_id, o_id);
        let w_dense = make_window_spec_opt_with_order(&mut arena, "dense_rank", p_id, o_id);

        let window = window_opt(
            sort,
            vec![w_rank, w_dense],
            vec![output_col(rk_id, "rk"), output_col(drk_id, "drk")],
        );
        // Filter on the rank column (rk <= 3)
        let plan = filter_opt(
            &mut arena,
            window,
            binop_typed(col_typed(rk_id), BinOp::Le, int_typed(3)),
        );
        let mut ctx = make_ctx(arena);

        let result = RankingWindowPredicatePushdownRule
            .apply(plan, &mut ctx)
            .unwrap();
        let sort_node = extract_sort_from_changed(result);
        assert_eq!(sort_node.partition_limit, Some(3));
        assert_eq!(sort_node.topn_type, Some(SortTopNType::Rank));
    }
}
