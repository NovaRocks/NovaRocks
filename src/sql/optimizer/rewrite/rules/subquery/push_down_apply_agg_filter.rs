//! `PushDownApplyAggFilter` — ports StarRocks `PushDownApplyAggFilterRule`.
//!
//! Matches a correlated scalar `Apply` whose inner is:
//!   `[Project?] Aggregate{group_by: []}( Filter(corr_pred)(inner_scan) )`
//! Rewrites the inner to a **vector** aggregate grouped by the correlation key,
//! hoists the correlated EQ conjuncts onto `Apply.correlation_conjuncts`, keeps
//! residual conjuncts as a `Filter` below the aggregate, and sets
//! `need_check_max_rows = false`.

use std::collections::HashSet;

use super::decorrelate_util::{all_binary_eq_opt, orient_eq_opt, partition_conjuncts_opt};
use super::scalar_utils;
use crate::sql::column_id::ColumnId;
use crate::sql::common::ApplyKind;
use crate::sql::optimizer::operator::{FilterOp, LogicalAggregateOp, Operator, ProjectOp};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId};

pub(crate) struct PushDownApplyAggFilter;

impl LogicalRewriteRule for PushDownApplyAggFilter {
    fn name(&self) -> &'static str {
        "PushDownApplyAggFilter"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let arena = ctx.scalar_arena();
        matches_expr(expr, &arena.borrow())
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let arena = ctx.scalar_arena();
        let mut arena = arena.borrow_mut();
        match apply_expr(expr, &mut arena)? {
            Some(new_expr) => Ok(RewriteResult::Changed(new_expr)),
            None => Ok(RewriteResult::Unchanged),
        }
    }
}

fn matches_expr(expr: &OptExpr, arena: &ScalarArena) -> bool {
    let Operator::LogicalApply(a) = &expr.op else {
        return false;
    };
    if a.kind != ApplyKind::Scalar {
        return false;
    }
    if a.correlation_column_ids.is_empty() {
        return false;
    }
    if !a.need_check_max_rows {
        return false;
    }
    let corr_ids: HashSet<ColumnId> = a.correlation_column_ids.iter().copied().collect();
    inner_is_correlated_scalar_agg(expr.right(), arena, &corr_ids)
}

fn apply_expr(expr: OptExpr, arena: &mut ScalarArena) -> Result<Option<OptExpr>, String> {
    let OptExpr {
        op,
        mut children,
        required_output_columns,
    } = expr;
    let Operator::LogicalApply(mut apply) = op else {
        return Ok(None);
    };
    if children.len() != 2 {
        return Ok(None);
    }
    let right = children.remove(1);
    let left = children.remove(0);
    let corr_ids: HashSet<ColumnId> = apply.correlation_column_ids.iter().copied().collect();

    // Peel the optional leading Project and destructure the inner.
    let peeled = peel_inner(right, arena, &corr_ids)
        .ok_or_else(|| "PushDownApplyAggFilter: inner shape mismatch".to_string())?;

    // Split the Filter predicate into (correlated, residual).
    let predicate = peeled.filter.predicate;
    let (correlated, residual) = partition_conjuncts_opt(arena, predicate, &corr_ids);

    if correlated.is_empty() {
        return Err(
            "correlated subquery without correlation predicate is not supported".to_string(),
        );
    }
    if !all_binary_eq_opt(arena, &correlated) {
        return Err(
            "non-EQ correlated predicate in correlated subquery is not supported".to_string(),
        );
    }

    // For each correlated EQ conjunct, orient it as (outer, inner) and
    // collect distinct inner-side ColumnRef expressions as new group-by keys.
    // Require each inner side to be a ColumnRef (non-column inner is M1c).
    let mut inner_key_exprs: Vec<ScalarId> = Vec::new();
    let mut seen_inner_ids: HashSet<ColumnId> = HashSet::new();
    let filter_input_columns = scalar_utils::opt_output_columns(&peeled.filter_input, arena)?;
    let mut new_group_key_output_columns = Vec::new();

    for conj in &correlated {
        let Some((_, inner_side)) = orient_eq_opt(arena, *conj, &corr_ids) else {
            // Cannot orient the EQ — both/neither side is outer — fall through.
            return Ok(None);
        };
        // Require the inner side to be a ColumnRef.
        let Some(column_id) = scalar_utils::is_column_ref(arena, inner_side) else {
            // Non-column inner side — fall back (M1c concern).
            return Ok(None);
        };
        if seen_inner_ids.insert(column_id) {
            let Some(output) = scalar_utils::find_output_column(&filter_input_columns, column_id)
            else {
                return Ok(None);
            };
            inner_key_exprs.push(inner_side);
            new_group_key_output_columns.push(output.clone());
        }
    }

    // Rebuild the filter input: either drop the Filter entirely (all conjuncts
    // were correlated) or keep a Filter with just the residual conjuncts.
    let new_filter_input = if residual.is_empty() {
        peeled.filter_input
    } else {
        let Some(predicate) = scalar_utils::combine_and(arena, residual) else {
            return Ok(None);
        };
        scalar_utils::filter(peeled.filter_input, predicate)
    };

    // Rebuild the Aggregate: group_by = new_key_exprs (since the original was []),
    // output_columns = [group_key_cols..., original_agg_result_cols...].
    let mut new_output_columns = new_group_key_output_columns.clone();
    new_output_columns.extend(peeled.aggregate.output_columns.clone());

    let mut new_aggregate = peeled.aggregate;
    new_aggregate.group_by = inner_key_exprs;
    new_aggregate.output_columns = new_output_columns;
    let new_agg = OptExpr::new(
        Operator::LogicalAggregate(new_aggregate),
        vec![new_filter_input],
    );

    // Re-wrap in the leading Project if present, extending it to pass through
    // the new group-key columns so they're visible to the join condition.
    // The join condition (built by ScalarApplyToJoin, Task 3) needs the inner
    // key columns to be in the Project's output.
    let new_inner = if let Some(project) = peeled.leading_project {
        let projected_ids: HashSet<ColumnId> = project
            .items
            .iter()
            .map(|item| item.output_column_id)
            .collect();

        let mut new_items = project.items;
        for out_col in &new_group_key_output_columns {
            if !projected_ids.contains(&out_col.column_id) {
                new_items.push(scalar_utils::project_item_for_column(arena, out_col));
            }
        }

        OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: new_items,
                output_qualifier: project.output_qualifier,
            }),
            vec![new_agg],
        )
    } else {
        new_agg
    };

    // Return the rewritten Apply: correlation_conjuncts = the correlated EQ
    // conjuncts (outer == inner), need_check_max_rows = false.
    apply.correlation_conjuncts = correlated;
    apply.need_check_max_rows = false;
    Ok(Some(OptExpr {
        op: Operator::LogicalApply(apply),
        children: vec![left, new_inner],
        required_output_columns,
    }))
}

/// Returns true iff the given plan has the shape:
///   `[Project?] Aggregate{group_by: []}( Filter{corr_pred}(inner) )`
/// where the Filter's predicate has at least one conjunct referencing `corr_ids`.
fn inner_is_correlated_scalar_agg(
    plan: &OptExpr,
    arena: &ScalarArena,
    corr_ids: &HashSet<ColumnId>,
) -> bool {
    let after_project = if matches!(&plan.op, Operator::LogicalProject(_)) {
        plan.unary_input()
    } else {
        plan
    };
    check_agg_over_corr_filter(after_project, arena, corr_ids)
}

fn check_agg_over_corr_filter(
    plan: &OptExpr,
    arena: &ScalarArena,
    corr_ids: &HashSet<ColumnId>,
) -> bool {
    let Operator::LogicalAggregate(agg) = &plan.op else {
        return false;
    };
    if !agg.group_by.is_empty() {
        return false;
    }
    let filter_input = plan.unary_input();
    let Operator::LogicalFilter(filter) = &filter_input.op else {
        return false;
    };
    // At least one conjunct must reference a corr_id.
    scalar_utils::split_and(arena, filter.predicate)
        .iter()
        .any(|conjunct| scalar_utils::scalar_refs_any(arena, *conjunct, corr_ids))
}

struct PeeledInner {
    leading_project: Option<ProjectOp>,
    aggregate: LogicalAggregateOp,
    filter: FilterOp,
    filter_input: OptExpr,
}

/// Destructures the inner plan into `[Project?] Aggregate(Filter(input))`.
/// Returns `None` if the shape doesn't match (no group-by Aggregate over a Filter).
fn peel_inner(
    plan: OptExpr,
    arena: &ScalarArena,
    corr_ids: &HashSet<ColumnId>,
) -> Option<PeeledInner> {
    let OptExpr {
        op,
        children,
        required_output_columns,
    } = plan;
    match op {
        Operator::LogicalProject(project) => {
            if children.len() != 1 {
                return None;
            }
            let input = children.into_iter().next()?;
            let (agg, filter, filter_input) = peel_agg_over_filter(input, arena, corr_ids)?;
            Some(PeeledInner {
                leading_project: Some(project),
                aggregate: agg,
                filter,
                filter_input,
            })
        }
        _ => {
            let plan = OptExpr {
                op,
                children,
                required_output_columns,
            };
            let (agg, filter, filter_input) = peel_agg_over_filter(plan, arena, corr_ids)?;
            Some(PeeledInner {
                leading_project: None,
                aggregate: agg,
                filter,
                filter_input,
            })
        }
    }
}

fn peel_agg_over_filter(
    plan: OptExpr,
    _arena: &ScalarArena,
    _corr_ids: &HashSet<ColumnId>,
) -> Option<(LogicalAggregateOp, FilterOp, OptExpr)> {
    let OptExpr {
        op,
        mut children,
        required_output_columns: _,
    } = plan;
    let Operator::LogicalAggregate(agg) = op else {
        return None;
    };
    if !agg.group_by.is_empty() {
        return None;
    }
    if children.len() != 1 {
        return None;
    }
    let filter_plan = children.remove(0);
    let OptExpr {
        op,
        mut children,
        required_output_columns: _,
    } = filter_plan;
    let Operator::LogicalFilter(filter) = op else {
        return None;
    };
    if children.len() != 1 {
        return None;
    }
    Some((agg, filter, children.remove(0)))
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rules::subquery::bridge::opt_expr_to_plan;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::optimizer_bridge::plan::logical_plan_to_opt_expr;
    use crate::sql::planner::plan::{
        AggregateCall, ApplyKind, LogicalAggregateNode, LogicalApplyNode, LogicalFilterNode,
        LogicalPlanNode, LogicalScanNode, LogicalValuesNode, PlanNodeKind,
    };

    // ---- Column ID constants ------------------------------------------------
    const T2_K: ColumnId = ColumnId(1); // t2.k  (inner correlation column)
    const T2_V2: ColumnId = ColumnId(2); // t2.v2 (inner value column)
    const OUTER_K: ColumnId = ColumnId(100); // t1.k as seen inside the subquery
    const MAX_RESULT: ColumnId = ColumnId(10); // output_column_id for max(v2)
    const APPLY_OUT: ColumnId = ColumnId(20); // the Apply's output column

    fn ctx_with_arena() -> RewriteContext {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        ctx
    }

    fn to_opt_expr(
        plan: LogicalPlanNode,
        ctx: &mut RewriteContext,
    ) -> crate::sql::optimizer::opt_expr::OptExpr {
        let arena = ctx.scalar_arena();
        logical_plan_to_opt_expr(&plan, &mut arena.borrow_mut())
    }

    fn col_ref(id: ColumnId, name: &str, dt: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: dt,
            nullable: false,
        }
    }

    fn col_ref_nullable(id: ColumnId, name: &str, dt: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: dt,
            nullable: true,
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
            nullable: false,
        }
    }

    fn make_t2_scan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "default".to_string(),
                table: TableDef {
                    name: "t2".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: T2_K,
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: T2_V2,
                        name: "v2".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    /// Build the inner plan: Aggregate{group_by:[], max(v2)}(Filter(t2.k==OUTER)(Scan t2))
    fn inner_correlated_agg() -> LogicalPlanNode {
        let corr_pred = eq_expr(
            col_ref(T2_K, "k", DataType::Int64),
            col_ref(OUTER_K, "k", DataType::Int64),
        );
        let filter = LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode {
                predicate: corr_pred,
            }),
            vec![make_t2_scan()],
            None,
        );

        LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![],
                aggregates: vec![AggregateCall {
                    name: "max".to_string(),
                    args: vec![col_ref(T2_V2, "v2", DataType::Int64)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: MAX_RESULT,
                }],
                output_columns: vec![OutputColumn {
                    column_id: MAX_RESULT,
                    name: "max(v2)".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                }],
                already_pushed: false,
            }),
            vec![filter],
            None,
        )
    }

    fn correlated_scalar_agg_apply() -> LogicalPlanNode {
        let outer_values = LogicalPlanNode::new(
            PlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![OutputColumn {
                    column_id: OUTER_K,
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            vec![],
            None,
        );

        LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: MAX_RESULT,
                correlation_column_ids: vec![OUTER_K],
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![outer_values, inner_correlated_agg()],
            None,
        )
    }

    /// Core correctness test: the rule decorrelates a scalar agg Apply.
    /// Input:  Apply{ right: Agg{group_by:[]}(Filter(t2.k==OUTER)(Scan t2)), need_check=true }
    /// Output: Apply{ right: Agg{group_by:[t2.k]}(Scan t2), need_check=false,
    ///                correlation_conjuncts=[OUTER==t2.k] }
    #[test]
    fn push_down_apply_agg_filter_decorrelates_scalar_agg() {
        let rule = PushDownApplyAggFilter;
        let plan = correlated_scalar_agg_apply();
        let mut ctx = ctx_with_arena();

        let expr = to_opt_expr(plan, &mut ctx);

        assert!(
            rule.matches(&expr, &ctx),
            "rule must match a correlated scalar agg Apply"
        );

        let result = rule
            .apply(expr, &mut ctx)
            .expect("rule apply must not error");
        let new_expr = match result {
            RewriteResult::Changed(e) => e,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let arena = ctx.scalar_arena();
        let new_plan = opt_expr_to_plan(&new_expr, &arena.borrow());

        let PlanNodeKind::Apply(new_apply) = &new_plan.kind else {
            panic!("expected Apply, got: {new_plan:?}");
        };

        // need_check_max_rows must be false.
        assert!(
            !new_apply.need_check_max_rows,
            "need_check_max_rows must be false after pushdown"
        );

        // correlation_conjuncts must be the OUTER == t2.k EQ.
        assert_eq!(new_apply.correlation_conjuncts.len(), 1);
        let conj = &new_apply.correlation_conjuncts[0];
        let ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } = &conj.kind
        else {
            panic!("correlation conjunct must be a BinaryOp(Eq), got: {conj:?}");
        };
        use crate::sql::optimizer::rewrite::rules::utils::collect_column_id_refs;
        let all_ids: HashSet<ColumnId> = collect_column_id_refs(left)
            .union(&collect_column_id_refs(right))
            .copied()
            .collect();
        assert!(all_ids.contains(&OUTER_K), "must reference OUTER_K");
        assert!(all_ids.contains(&T2_K), "must reference T2_K");

        // right child: Aggregate{group_by:[t2.k]}(Scan t2)
        let right_plan = new_plan.right();
        let PlanNodeKind::Aggregate(new_agg) = &right_plan.kind else {
            panic!("right child must be Aggregate, got: {:?}", right_plan);
        };

        // group_by must be [t2.k].
        assert_eq!(
            new_agg.group_by.len(),
            1,
            "group_by must have exactly one key"
        );
        let ExprKind::ColumnRef { column_id, .. } = &new_agg.group_by[0].kind else {
            panic!("group_by expression must be a ColumnRef");
        };
        assert_eq!(*column_id, T2_K, "group_by key must be T2_K");

        // max(v2) aggregate is preserved.
        assert_eq!(new_agg.aggregates.len(), 1);
        assert_eq!(new_agg.aggregates[0].name, "max");
        assert_eq!(new_agg.aggregates[0].output_column_id, MAX_RESULT);

        // output_columns: [T2_K (group key), MAX_RESULT (agg result)].
        assert_eq!(new_agg.output_columns.len(), 2);
        assert_eq!(new_agg.output_columns[0].column_id, T2_K);
        assert_eq!(new_agg.output_columns[1].column_id, MAX_RESULT);

        // Correlated Filter was removed; agg input is the Scan directly.
        let PlanNodeKind::Scan(scan) = &right_plan.unary_input().kind else {
            panic!(
                "agg input must be a Scan (correlated Filter removed), got: {:?}",
                right_plan.unary_input()
            );
        };
        assert_eq!(scan.table.name, "t2");
    }

    /// Residual-filter test: when the inner Filter has both correlated AND residual
    /// conjuncts, only the correlated ones are hoisted; residual stays as a Filter.
    #[test]
    fn push_down_apply_agg_filter_keeps_residual_filter() {
        use crate::sql::analysis::LiteralValue;

        // Filter: (t2.k == OUTER) AND (t2.v2 > 5)
        let corr_pred = eq_expr(
            col_ref(T2_K, "k", DataType::Int64),
            col_ref(OUTER_K, "k", DataType::Int64),
        );
        let residual_pred = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(T2_V2, "v2", DataType::Int64)),
                op: BinOp::Gt,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(5)),
                    data_type: DataType::Int64,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let combined_pred = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(corr_pred),
                op: BinOp::And,
                right: Box::new(residual_pred),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };

        let inner = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![],
                aggregates: vec![AggregateCall {
                    name: "max".to_string(),
                    args: vec![col_ref(T2_V2, "v2", DataType::Int64)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: MAX_RESULT,
                }],
                output_columns: vec![OutputColumn {
                    column_id: MAX_RESULT,
                    name: "max(v2)".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                }],
                already_pushed: false,
            }),
            vec![LogicalPlanNode::new(
                PlanNodeKind::Filter(LogicalFilterNode {
                    predicate: combined_pred,
                }),
                vec![make_t2_scan()],
                None,
            )],
            None,
        );

        let apply = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: MAX_RESULT,
                correlation_column_ids: vec![OUTER_K],
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![
                LogicalPlanNode::new(
                    PlanNodeKind::Values(LogicalValuesNode {
                        rows: vec![],
                        columns: vec![],
                    }),
                    vec![],
                    None,
                ),
                inner,
            ],
            None,
        );

        let rule = PushDownApplyAggFilter;
        let mut ctx = ctx_with_arena();
        let expr = to_opt_expr(apply, &mut ctx);
        let result = rule
            .apply(expr, &mut ctx)
            .expect("rule apply must not error");
        let new_expr = match result {
            RewriteResult::Changed(e) => e,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let arena = ctx.scalar_arena();
        let new_plan = opt_expr_to_plan(&new_expr, &arena.borrow());

        let PlanNodeKind::Apply(new_apply) = &new_plan.kind else {
            panic!("expected Apply");
        };
        assert!(!new_apply.need_check_max_rows);
        assert_eq!(new_apply.correlation_conjuncts.len(), 1);

        let right_plan = new_plan.right();
        let PlanNodeKind::Aggregate(_) = &right_plan.kind else {
            panic!("right must be Aggregate");
        };
        // Residual filter must remain below the agg.
        let residual_plan = right_plan.unary_input();
        let PlanNodeKind::Filter(_) = &residual_plan.kind else {
            panic!("agg input must be a Filter (residual) when there are non-correlated preds");
        };
        // The residual filter's input must be the scan.
        assert!(matches!(
            &residual_plan.unary_input().kind,
            PlanNodeKind::Scan(_)
        ));
    }

    #[test]
    fn push_down_apply_agg_filter_no_match_uncorrelated() {
        let rule = PushDownApplyAggFilter;
        let mut ctx = ctx_with_arena();

        let plan = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: APPLY_OUT,
                correlation_column_ids: vec![],
                // uncorrelated
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![
                LogicalPlanNode::new(
                    PlanNodeKind::Values(LogicalValuesNode {
                        rows: vec![],
                        columns: vec![],
                    }),
                    vec![],
                    None,
                ),
                LogicalPlanNode::new(
                    PlanNodeKind::Values(LogicalValuesNode {
                        rows: vec![],
                        columns: vec![],
                    }),
                    vec![],
                    None,
                ),
            ],
            None,
        );

        let expr = to_opt_expr(plan, &mut ctx);
        assert!(
            !rule.matches(&expr, &ctx),
            "must not match uncorrelated Apply"
        );
    }

    #[test]
    fn push_down_apply_agg_filter_no_match_already_decorrelated() {
        let rule = PushDownApplyAggFilter;
        let mut ctx = ctx_with_arena();

        let plan = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: MAX_RESULT,
                correlation_column_ids: vec![OUTER_K],
                correlation_conjuncts: vec![eq_expr(
                    col_ref(OUTER_K, "k", DataType::Int64),
                    col_ref(T2_K, "k", DataType::Int64),
                )],
                residual_predicate: None,
                need_check_max_rows: false,
                // already decorrelated
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![
                LogicalPlanNode::new(
                    PlanNodeKind::Values(LogicalValuesNode {
                        rows: vec![],
                        columns: vec![],
                    }),
                    vec![],
                    None,
                ),
                inner_correlated_agg(),
            ],
            None,
        );

        let expr = to_opt_expr(plan, &mut ctx);
        assert!(
            !rule.matches(&expr, &ctx),
            "must not match when need_check_max_rows is already false"
        );
    }

    /// Multi-correlated-conjunct test: when the inner Filter has TWO correlated EQ
    /// conjuncts (`t2.a == OUTER1 AND t2.b == OUTER2`), the rule must:
    ///   - add both `t2.a` and `t2.b` to `group_by`
    ///   - include both group keys plus the original agg result in `output_columns`
    ///   - set `correlation_conjuncts.len() == 2`
    #[test]
    fn push_down_apply_agg_filter_promotes_multiple_correlation_keys() {
        use crate::sql::optimizer::rewrite::rules::utils::collect_column_id_refs;

        // Additional column IDs for the two-key scenario.
        const T2_A: ColumnId = ColumnId(3); // t2.a  (first correlation key)
        const T2_B: ColumnId = ColumnId(4); // t2.b  (second correlation key)
        const OUTER1: ColumnId = ColumnId(101); // outer ref for t2.a
        const OUTER2: ColumnId = ColumnId(102); // outer ref for t2.b
        const SUM_RESULT: ColumnId = ColumnId(11); // output_column_id for sum(v2)

        // Build an inner plan:
        //   Aggregate{group_by:[]}(Filter(t2.a==OUTER1 AND t2.b==OUTER2)(Scan t2))
        let corr_pred_a = eq_expr(
            col_ref(T2_A, "a", DataType::Int64),
            col_ref(OUTER1, "a", DataType::Int64),
        );
        let corr_pred_b = eq_expr(
            col_ref(T2_B, "b", DataType::Int64),
            col_ref(OUTER2, "b", DataType::Int64),
        );
        let combined = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(corr_pred_a),
                op: BinOp::And,
                right: Box::new(corr_pred_b),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };

        // Extend make_t2_scan() with the two extra columns.
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "default".to_string(),
                table: crate::sql::catalog::TableDef {
                    name: "t2".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::sql::catalog::ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: T2_A,
                        name: "a".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: T2_B,
                        name: "b".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: T2_V2,
                        name: "v2".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );

        let filter = LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode {
                predicate: combined,
            }),
            vec![scan],
            None,
        );

        let inner = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_ref(T2_V2, "v2", DataType::Int64)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: SUM_RESULT,
                }],
                output_columns: vec![OutputColumn {
                    column_id: SUM_RESULT,
                    name: "sum(v2)".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                }],
                already_pushed: false,
            }),
            vec![filter],
            None,
        );

        let outer_values = LogicalPlanNode::new(
            PlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![
                    OutputColumn {
                        column_id: OUTER1,
                        name: "a".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: OUTER2,
                        name: "b".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                ],
            }),
            vec![],
            None,
        );

        let apply = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: SUM_RESULT,
                correlation_column_ids: vec![OUTER1, OUTER2],
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![outer_values, inner],
            None,
        );

        let rule = PushDownApplyAggFilter;
        let mut ctx = ctx_with_arena();

        let expr = to_opt_expr(apply, &mut ctx);

        assert!(rule.matches(&expr, &ctx), "rule must match two-key Apply");

        let result = rule
            .apply(expr, &mut ctx)
            .expect("rule apply must not error");
        let new_expr = match result {
            RewriteResult::Changed(e) => e,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let arena = ctx.scalar_arena();
        let new_plan = opt_expr_to_plan(&new_expr, &arena.borrow());

        let PlanNodeKind::Apply(new_apply) = &new_plan.kind else {
            panic!("expected Apply");
        };

        // correlation_conjuncts must carry both correlated EQ predicates.
        assert_eq!(
            new_apply.correlation_conjuncts.len(),
            2,
            "must have exactly 2 correlation conjuncts"
        );

        let right_plan = new_plan.right();
        let PlanNodeKind::Aggregate(new_agg) = &right_plan.kind else {
            panic!("right child must be Aggregate");
        };

        // group_by must contain both T2_A and T2_B.
        let group_ids: HashSet<ColumnId> = new_agg
            .group_by
            .iter()
            .filter_map(|e| {
                if let ExprKind::ColumnRef { column_id, .. } = &e.kind {
                    Some(*column_id)
                } else {
                    None
                }
            })
            .collect();
        assert!(group_ids.contains(&T2_A), "group_by must include T2_A");
        assert!(group_ids.contains(&T2_B), "group_by must include T2_B");
        assert_eq!(group_ids.len(), 2, "group_by must have exactly 2 keys");

        // output_columns must include both group keys plus the original agg result.
        let out_ids: HashSet<ColumnId> =
            new_agg.output_columns.iter().map(|c| c.column_id).collect();
        assert!(out_ids.contains(&T2_A), "output_columns must include T2_A");
        assert!(out_ids.contains(&T2_B), "output_columns must include T2_B");
        assert!(
            out_ids.contains(&SUM_RESULT),
            "output_columns must include SUM_RESULT"
        );
        assert_eq!(
            new_agg.output_columns.len(),
            3,
            "output_columns must have 3 entries (2 group keys + 1 agg)"
        );

        // need_check_max_rows must be false after the rewrite.
        assert!(!new_apply.need_check_max_rows);

        // Each correlation conjunct must reference one outer id and one inner id.
        let outer_ids: HashSet<ColumnId> = [OUTER1, OUTER2].into();
        let inner_ids: HashSet<ColumnId> = [T2_A, T2_B].into();
        for conj in &new_apply.correlation_conjuncts {
            let all_refs: HashSet<ColumnId> = {
                let ExprKind::BinaryOp { left, right, .. } = &conj.kind else {
                    panic!("conjunct must be BinaryOp");
                };
                collect_column_id_refs(left)
                    .union(&collect_column_id_refs(right))
                    .copied()
                    .collect()
            };
            assert!(
                all_refs.iter().any(|id| outer_ids.contains(id)),
                "conjunct must reference an outer id"
            );
            assert!(
                all_refs.iter().any(|id| inner_ids.contains(id)),
                "conjunct must reference an inner id"
            );
        }
    }
}
