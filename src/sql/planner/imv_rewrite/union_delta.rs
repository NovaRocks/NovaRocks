use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::sql::planner::imv_rewrite::annotation::ImvExtension;
use crate::sql::planner::imv_rewrite::join_delta::{
    mark_delta_scan, normalize_branch_output, plan_output_columns,
};
use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
use crate::sql::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::sql::planner::plan::{
    LogicalAggregateNode, LogicalImvDeltaNode, LogicalPlanNode, LogicalProjectNode,
    LogicalUnionNode, PlanNodeKind,
};
use crate::{
    engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN,
    sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr},
};

pub(crate) struct RewriteUnionAggregateDeltaRule;

impl LogicalRewriteRule for RewriteUnionAggregateDeltaRule {
    fn name(&self) -> &'static str {
        "RewriteUnionAggregateDelta"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        let PlanNodeKind::ImvDelta(delta) = &plan.kind else {
            return false;
        };
        delta.is_root
            && matches!(
                &plan.unary_input().kind,
                PlanNodeKind::Aggregate(_)
                    if unary_chain_reaches_unmarked_source_union(plan.unary_input().unary_input())
            )
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        bridge_apply_result(expr, ctx, |plan, ctx| {
            let LogicalPlanNode {
                kind, mut children, ..
            } = plan;
            let PlanNodeKind::ImvDelta(delta) = kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            if !delta.is_root {
                return Ok(PlanRewriteResult::Unchanged);
            }
            let branch_scope = delta.branch_scope;
            let aggregate_plan = take_unary_child(&mut children);
            let LogicalPlanNode {
                kind: aggregate_kind,
                children: mut aggregate_children,
                required_output_columns: aggregate_required_output_columns,
            } = aggregate_plan;
            let PlanNodeKind::Aggregate(aggregate) = aggregate_kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            let aggregate_input = take_unary_child(&mut aggregate_children);
            // The source union may sit directly under the aggregate, or under a
            // unary projection/filter chain that the optimizer's required-column
            // pruning inserts between the aggregate and a derived UNION ALL subquery
            // (`SELECT .. FROM (.. UNION ALL ..) GROUP BY ..`). Descend through that
            // chain to reach the union.
            if !unary_chain_reaches_unmarked_source_union(&aggregate_input) {
                return Ok(PlanRewriteResult::Unchanged);
            }

            let action_column = match delta.action_column {
                Some(action_column) => action_column,
                None => ctx
                    .extension::<ImvExtension>()
                    .ok_or_else(|| {
                        "RewriteUnionAggregateDelta requires ImvExtension in RewriteContext"
                            .to_string()
                    })?
                    .allocate_column_id(),
            };
            let action_output = ImvActionColumn::output_column(action_column);

            let aggregate_input =
                mark_fan_in_union_through_unary(aggregate_input, action_column, &action_output)?;
            let aggregate = LogicalPlanNode::new(
                PlanNodeKind::Aggregate(LogicalAggregateNode {
                    group_by: aggregate.group_by,
                    aggregates: aggregate.aggregates,
                    output_columns: aggregate.output_columns,
                    already_pushed: aggregate.already_pushed,
                }),
                vec![aggregate_input],
                aggregate_required_output_columns,
            );

            Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                    is_root: true,
                    action_column: Some(action_column),
                    branch_scope,
                }),
                vec![aggregate],
                None,
            )))
        })
    }
}

/// Does `plan` reach an UNMARKED source `UNION ALL` by descending only through
/// unary projection/filter nodes? A-family `Aggregate(.. UNION ALL ..)` may have
/// column-pruning Projects (and Filters) inserted between the aggregate and the
/// union by the optimizer's required-column pruning, so the delta rewrite must
/// see through them. The marker guard distinguishes the SOURCE union from a
/// union whose branches already carry delta/version markers (e.g. join-delta
/// output), which must not be re-processed.
fn unary_chain_reaches_unmarked_source_union(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        PlanNodeKind::Union(union) => union.all && !plan_contains_imv_marker(plan),
        PlanNodeKind::Project(_) | PlanNodeKind::Filter(_) => {
            unary_chain_reaches_unmarked_source_union(plan.unary_input())
        }
        _ => false,
    }
}

/// Mark each branch of the source `UNION ALL` as a delta scan sharing
/// `action_column`, descending through any unary projection/filter chain above
/// the union. Each Project on the way up additionally projects `action_column`
/// so the shared action column stays visible to the aggregate above (the
/// aggregate-state rewrite consumes it as the signed retraction count).
fn mark_fan_in_union_through_unary(
    plan: LogicalPlanNode,
    action_column: crate::sql::column_id::ColumnId,
    action_output: &OutputColumn,
) -> Result<LogicalPlanNode, String> {
    let LogicalPlanNode {
        kind,
        mut children,
        required_output_columns,
    } = plan;
    match kind {
        PlanNodeKind::Union(union) => {
            if !union.all {
                return Err(
                    "Iceberg IMV UNION aggregate delta rewrite supports UNION ALL only".to_string(),
                );
            }
            let mut rewritten_inputs = Vec::with_capacity(children.len());
            for branch in children {
                let mut branch_output = plan_output_columns(&branch)?;
                branch_output.push(action_output.clone());
                let marked = mark_delta_scan(branch, action_column)?;
                rewritten_inputs.push(normalize_branch_output(marked, &branch_output));
            }
            let mut union_output_columns = union.output_columns;
            union_output_columns.push(action_output.clone());
            Ok(LogicalPlanNode::new(
                PlanNodeKind::Union(LogicalUnionNode {
                    all: union.all,
                    output_columns: union_output_columns,
                }),
                rewritten_inputs,
                required_output_columns,
            ))
        }
        PlanNodeKind::Project(mut project) => {
            let input = take_unary_child(&mut children);
            let input = mark_fan_in_union_through_unary(input, action_column, action_output)?;
            project.items.push(action_passthrough_item(action_output));
            Ok(LogicalPlanNode::new(
                PlanNodeKind::Project(project),
                vec![input],
                required_output_columns,
            ))
        }
        PlanNodeKind::Filter(filter) => {
            let input = take_unary_child(&mut children);
            let input = mark_fan_in_union_through_unary(input, action_column, action_output)?;
            Ok(LogicalPlanNode::new(
                PlanNodeKind::Filter(filter),
                vec![input],
                required_output_columns,
            ))
        }
        other_kind => Err(format!(
            "RewriteUnionAggregateDelta: unexpected node above source union: {}",
            plan_kind_from_kind(&other_kind)
        )),
    }
}

/// A Project item that passes the shared action column straight through, so a
/// column-pruning Project between the aggregate and the union keeps exposing it.
fn action_passthrough_item(action_output: &OutputColumn) -> ProjectItem {
    ProjectItem {
        expr: TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: action_output.column_id,
                qualifier: None,
                column: action_output.name.clone(),
            },
            data_type: action_output.data_type.clone(),
            nullable: action_output.nullable,
        },
        output_name: action_output.name.clone(),
        output_column_id: action_output.column_id,
    }
}

pub(crate) struct RewriteTopLevelUnionDeltaRule;

impl LogicalRewriteRule for RewriteTopLevelUnionDeltaRule {
    fn name(&self) -> &'static str {
        "RewriteTopLevelUnionDelta"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        let PlanNodeKind::ImvDelta(delta) = &plan.kind else {
            return false;
        };
        delta.is_root
            && matches!(
                &plan.unary_input().kind,
                PlanNodeKind::Union(union)
                    if union.all && !plan_contains_imv_marker(plan.unary_input())
            )
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        bridge_apply_result(expr, ctx, |plan, ctx| {
            let LogicalPlanNode {
                kind, mut children, ..
            } = plan;
            let PlanNodeKind::ImvDelta(delta) = kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            if !delta.is_root {
                return Ok(PlanRewriteResult::Unchanged);
            }
            let union_plan = take_unary_child(&mut children);
            if !matches!(&union_plan.kind, PlanNodeKind::Union(_)) {
                return Ok(PlanRewriteResult::Unchanged);
            }
            if plan_contains_imv_marker(&union_plan) {
                return Ok(PlanRewriteResult::Unchanged);
            }
            let LogicalPlanNode {
                kind: union_kind,
                children: inputs,
                required_output_columns,
            } = union_plan;
            let PlanNodeKind::Union(union) = union_kind else {
                unreachable!();
            };
            if !union.all {
                return Err(
                    "Iceberg IMV top-level union delta rewrite supports UNION ALL only".to_string(),
                );
            }

            let ext = ctx.extension::<ImvExtension>().ok_or_else(|| {
                "RewriteTopLevelUnionDelta requires ImvExtension in RewriteContext".to_string()
            })?;
            let action_column = delta
                .action_column
                .unwrap_or_else(|| ext.allocate_column_id());
            let branch_id_column = ext.allocate_column_id();

            let action_output = ImvActionColumn::output_column(action_column);
            let branch_output = branch_id_output_column(branch_id_column);

            let mut rewritten_inputs = Vec::with_capacity(inputs.len());
            for (idx, branch) in inputs.into_iter().enumerate() {
                ensure_top_level_union_branch_supported(&branch)?;
                let branch_output_columns = plan_output_columns(&branch)?;
                if branch_output_columns.len() != union.output_columns.len() {
                    return Err(format!(
                        "Iceberg IMV top-level UNION ALL delta rewrite branch {idx} output column count {} does not match union output column count {}",
                        branch_output_columns.len(),
                        union.output_columns.len()
                    ));
                }
                let marked = mark_delta_scan(branch, action_column)?;
                rewritten_inputs.push(normalize_top_level_union_branch_output(
                    marked,
                    &union.output_columns,
                    &branch_output_columns,
                    &action_output,
                    &branch_output,
                    idx,
                ));
            }

            let mut union_output_columns = union.output_columns;
            union_output_columns.push(action_output);
            union_output_columns.push(branch_output);

            Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                PlanNodeKind::Union(LogicalUnionNode {
                    all: union.all,
                    output_columns: union_output_columns,
                }),
                rewritten_inputs,
                required_output_columns,
            )))
        })
    }
}

fn branch_id_output_column(
    column_id: crate::sql::column_id::ColumnId,
) -> crate::sql::analysis::OutputColumn {
    crate::sql::analysis::OutputColumn {
        column_id,
        name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
        data_type: arrow::datatypes::DataType::Int32,
        nullable: false,
        is_internal: true,
    }
}

fn normalize_top_level_union_branch_output(
    input: LogicalPlanNode,
    union_visible_output: &[crate::sql::analysis::OutputColumn],
    branch_visible_output: &[crate::sql::analysis::OutputColumn],
    action_output: &crate::sql::analysis::OutputColumn,
    branch_output: &crate::sql::analysis::OutputColumn,
    branch_idx: usize,
) -> LogicalPlanNode {
    let mut items = union_visible_output
        .iter()
        .zip(branch_visible_output)
        .map(|(union_column, branch_column)| ProjectItem {
            expr: crate::sql::analysis::TypedExpr {
                kind: crate::sql::analysis::ExprKind::ColumnRef {
                    column_id: branch_column.column_id,
                    qualifier: None,
                    column: branch_column.name.clone(),
                },
                data_type: branch_column.data_type.clone(),
                nullable: branch_column.nullable,
            },
            output_name: union_column.name.clone(),
            output_column_id: union_column.column_id,
        })
        .collect::<Vec<_>>();
    items.push(ProjectItem {
        expr: crate::sql::analysis::TypedExpr {
            kind: crate::sql::analysis::ExprKind::ColumnRef {
                column_id: action_output.column_id,
                qualifier: None,
                column: action_output.name.clone(),
            },
            data_type: action_output.data_type.clone(),
            nullable: action_output.nullable,
        },
        output_name: action_output.name.clone(),
        output_column_id: action_output.column_id,
    });
    items.push(ProjectItem {
        expr: crate::sql::analysis::TypedExpr {
            kind: crate::sql::analysis::ExprKind::Cast {
                expr: Box::new(crate::sql::analysis::TypedExpr {
                    kind: crate::sql::analysis::ExprKind::Literal(
                        crate::sql::analysis::LiteralValue::Int(branch_idx as i64),
                    ),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                }),
                target: arrow::datatypes::DataType::Int32,
            },
            data_type: branch_output.data_type.clone(),
            nullable: false,
        },
        output_name: branch_output.name.clone(),
        output_column_id: branch_output.column_id,
    });

    LogicalPlanNode::new(
        PlanNodeKind::Project(LogicalProjectNode {
            items,
            output_qualifier: None,
        }),
        vec![input],
        None,
    )
}

fn ensure_top_level_union_branch_supported(plan: &LogicalPlanNode) -> Result<(), String> {
    match &plan.kind {
        PlanNodeKind::Scan(_) => Ok(()),
        PlanNodeKind::Project(_) | PlanNodeKind::Filter(_) => {
            ensure_top_level_union_branch_supported(plan.unary_input())
        }
        other_kind => Err(format!(
            "Iceberg IMV top-level UNION ALL delta rewrite supports only Scan/Project/Filter branches, got {}",
            plan_kind_from_kind(other_kind)
        )),
    }
}

fn plan_kind(plan: &LogicalPlanNode) -> &'static str {
    plan_kind_from_kind(&plan.kind)
}

fn plan_kind_from_kind(kind: &PlanNodeKind) -> &'static str {
    kind.variant_name()
}

fn take_unary_child(children: &mut Vec<LogicalPlanNode>) -> LogicalPlanNode {
    assert_eq!(children.len(), 1, "expected one logical plan child");
    children.remove(0)
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::plan::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::convert::logical_plan_to_opt_expr;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
    use crate::sql::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::planner::plan::{
        LogicalAggregateNode, LogicalFilterNode, LogicalJoinNode, LogicalProjectNode,
        LogicalScanNode, LogicalUnionNode, PlanNodeKind,
    };

    #[test]
    fn matches_root_delta_over_aggregate_over_source_union() {
        let rule = RewriteUnionAggregateDeltaRule;
        let ctx = build_ctx();
        let plan = delta(aggregate_over(source_union(true)));
        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
    }

    #[test]
    fn does_not_match_union_already_marked() {
        let rule = RewriteUnionAggregateDeltaRule;
        let ctx = build_ctx();
        let plan = delta(aggregate_over(marked_source_union()));
        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn rewrite_marks_each_branch_with_shared_action_column() {
        let rule = RewriteUnionAggregateDeltaRule;
        let mut ctx = build_ctx();
        let plan = delta(aggregate_over(source_union(true)));

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(rewritten_expr) = rule
            .apply(expr, &mut ctx)
            .expect("UNION ALL aggregate delta must rewrite")
        else {
            panic!("expected Changed(ImvDelta)");
        };
        let arena_ref = ctx.scalar_arena();
        let rewritten = crate::sql::optimizer::convert::opt_expr_to_logical_plan(
            rewritten_expr,
            &arena_ref.borrow(),
        );
        let PlanNodeKind::ImvDelta(root_delta) = &rewritten.kind else {
            panic!("expected Changed(ImvDelta), got {rewritten:?}");
        };
        assert!(root_delta.is_root);
        let action_column = root_delta
            .action_column
            .expect("root delta must carry shared action column");
        assert_eq!(action_column, ColumnId(100));

        let aggregate_plan = rewritten.unary_input();
        let PlanNodeKind::Aggregate(_) = &aggregate_plan.kind else {
            panic!("expected root ImvDelta(Aggregate)");
        };
        let union_plan = aggregate_plan.unary_input();
        let PlanNodeKind::Union(union) = &union_plan.kind else {
            panic!("expected Aggregate(Union)");
        };
        assert!(union.all);
        assert_eq!(union_plan.children.len(), 2);
        assert_eq!(
            union
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2), action_column]
        );
        assert_eq!(
            union_plan.required_output_columns,
            required_output_columns()
        );

        assert_normalized_delta_branch(
            &union_plan.children[0],
            action_column,
            0,
            &[ColumnId(1), ColumnId(2), action_column],
        );
        assert_normalized_delta_branch(
            &union_plan.children[1],
            action_column,
            1,
            &[ColumnId(10), ColumnId(11), action_column],
        );
    }

    #[test]
    fn rewrite_top_level_union_delta_adds_branch_and_action_columns() {
        let rule = RewriteTopLevelUnionDeltaRule;
        let mut ctx = build_ctx();
        let plan = delta(project_filter_union(true));

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(rewritten_expr) = rule
            .apply(expr, &mut ctx)
            .expect("top-level UNION ALL delta must rewrite")
        else {
            panic!("expected Changed(Union)");
        };
        let arena_ref = ctx.scalar_arena();
        let rewritten = crate::sql::optimizer::convert::opt_expr_to_logical_plan(
            rewritten_expr,
            &arena_ref.borrow(),
        );
        let PlanNodeKind::Union(union) = &rewritten.kind else {
            panic!("expected Changed(Union), got {rewritten:?}");
        };
        let action_column = ColumnId(100);
        assert_eq!(action_column, ColumnId(100));
        assert!(union.all);
        assert_eq!(rewritten.children.len(), 2);
        assert_eq!(
            union
                .output_columns
                .iter()
                .map(|column| (
                    column.column_id,
                    column.name.as_str(),
                    column.data_type.clone(),
                    column.nullable,
                    column.is_internal
                ))
                .collect::<Vec<_>>(),
            vec![
                (ColumnId(1), "k", DataType::Int64, false, false),
                (ColumnId(2), "v", DataType::Int64, false, false),
                (action_column, "__change_op", DataType::Int8, false, true),
                (ColumnId(101), "__branch_id__", DataType::Int32, false, true),
            ]
        );
        assert_eq!(rewritten.required_output_columns, required_output_columns());

        assert_top_level_union_branch(&rewritten.children[0], action_column, ColumnId(101), 0);
        assert_top_level_union_branch(&rewritten.children[1], action_column, ColumnId(101), 1);
    }

    #[test]
    fn rewrite_top_level_union_delta_rejects_unsupported_branch_shape() {
        let rule = RewriteTopLevelUnionDeltaRule;
        let mut ctx = build_ctx();
        let plan = delta(LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "k"), output_column(2, "v")],
            }),
            vec![aggregate_over(scan("t1", 1)), project_over_filter("t2", 10)],
            required_output_columns(),
        ));

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let err = rule
            .apply(expr, &mut ctx)
            .expect_err("aggregate branch must be rejected");
        assert_eq!(
            err,
            "Iceberg IMV top-level UNION ALL delta rewrite supports only Scan/Project/Filter branches, got Aggregate"
        );
    }

    #[test]
    fn rewrite_top_level_union_delta_rejects_join_branch_shape() {
        let rule = RewriteTopLevelUnionDeltaRule;
        let mut ctx = build_ctx();
        let plan = delta(LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "k"), output_column(2, "v")],
            }),
            vec![join_branch(), project_over_filter("t2", 10)],
            required_output_columns(),
        ));

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let err = rule
            .apply(expr, &mut ctx)
            .expect_err("join branch must be rejected");
        assert_eq!(
            err,
            "Iceberg IMV top-level UNION ALL delta rewrite supports only Scan/Project/Filter branches, got Join"
        );
    }

    #[test]
    fn rewrite_top_level_union_delta_does_not_silently_rewrite_union_distinct() {
        let rule = RewriteTopLevelUnionDeltaRule;
        let mut ctx = build_ctx();
        let plan = delta(project_filter_union(false));

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr, &ctx));
        let err = rule
            .apply(expr, &mut ctx)
            .expect_err("UNION DISTINCT must not be rewritten as UNION ALL");
        assert_eq!(
            err,
            "Iceberg IMV top-level union delta rewrite supports UNION ALL only"
        );
    }

    fn build_ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_scalar_arena(std::rc::Rc::new(
            std::cell::RefCell::new(ScalarArena::new()),
        ));
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(100)),
        });
        ctx
    }

    fn delta(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: true,
                action_column: None,
                branch_scope: None,
            }),
            vec![input],
            None,
        )
    }

    fn aggregate_over(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_expr(1, "k")],
                aggregates: Vec::new(),
                output_columns: vec![output_column(1, "k")],
                already_pushed: false,
            }),
            vec![input],
            None,
        )
    }

    fn source_union(all: bool) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: all,
                output_columns: vec![output_column(1, "k"), output_column(2, "v")],
            }),
            vec![scan("t1", 1), scan("t2", 10)],
            required_output_columns(),
        )
    }

    fn project_filter_union(all: bool) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: all,
                output_columns: vec![output_column(1, "k"), output_column(2, "v")],
            }),
            vec![project_over_filter("t1", 1), project_over_filter("t2", 10)],
            required_output_columns(),
        )
    }

    fn project_over_filter(name: &str, first_id: u32) -> LogicalPlanNode {
        let scan = scan(name, first_id);
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![
                    ProjectItem {
                        expr: col_expr(first_id, "k"),
                        output_name: "k".to_string(),
                        output_column_id: ColumnId(first_id),
                    },
                    ProjectItem {
                        expr: col_expr(first_id + 1, "v"),
                        output_name: "v".to_string(),
                        output_column_id: ColumnId(first_id + 1),
                    },
                ],
                output_qualifier: None,
            }),
            vec![filter_over(scan, first_id, "k")],
            None,
        )
    }

    fn join_branch() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col_expr(20, "k")),
                        op: BinOp::Eq,
                        right: Box::new(col_expr(30, "k")),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                }),
            }),
            vec![scan("j1", 20), scan("j2", 30)],
            None,
        )
    }

    fn filter_over(input: LogicalPlanNode, column_id: u32, column: &str) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col_expr(column_id, column)),
                        op: BinOp::Ge,
                        right: Box::new(TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::Int(0)),
                            data_type: DataType::Int32,
                            nullable: false,
                        }),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![input],
            None,
        )
    }

    fn marked_source_union() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "k"), output_column(2, "v")],
            }),
            vec![
                LogicalPlanNode::new(
                    PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                        is_root: false,
                        action_column: Some(ColumnId(99)),
                        branch_scope: None,
                    }),
                    vec![scan("t1", 1)],
                    None,
                ),
                scan("t2", 1),
            ],
            None,
        )
    }

    fn required_output_columns() -> Option<std::collections::HashSet<ColumnId>> {
        Some([ColumnId(1), ColumnId(2)].into_iter().collect())
    }

    fn scan(name: &str, first_id: u32) -> LogicalPlanNode {
        let columns = vec![column_def("k"), column_def("v")];
        LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: name.to_string(),
                    columns,
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::IcebergDataFiles {
                        table: IcebergTableInfo {
                            catalog: "ice".to_string(),
                            namespace: "db".to_string(),
                            table: name.to_string(),
                            table_uuid: Some(format!("uuid-{name}")),
                            current_snapshot_id: Some(22),
                            schema_id: 7,
                            location: format!("file:///tmp/ice/db/{name}"),
                            schema: IcebergSchemaDef { fields: Vec::new() },
                            serialized_metadata: None,
                            serialized_metadata_rows: None,
                        },
                        files: Vec::new(),
                        cloud_properties: BTreeMap::new(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                columns: vec![
                    output_column(first_id, "k"),
                    output_column(first_id + 1, "v"),
                ],
                predicates: Vec::new(),
                required_columns: None,
                dict_columns: Vec::new(),
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn column_def(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn col_expr(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn assert_normalized_delta_branch(
        plan: &LogicalPlanNode,
        action_column: ColumnId,
        idx: usize,
        expected_item_ids: &[ColumnId],
    ) {
        let PlanNodeKind::Project(project) = &plan.kind else {
            panic!("branch {idx} must be normalized through Project");
        };
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_column_id)
                .collect::<Vec<_>>(),
            expected_item_ids
        );
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| match &item.expr.kind {
                    ExprKind::ColumnRef { column_id, .. } => *column_id,
                    other => panic!("branch {idx} must project ColumnRef, got {other:?}"),
                })
                .collect::<Vec<_>>(),
            expected_item_ids
        );
        assert!(
            project.items.iter().any(|item| item
                .output_name
                .eq_ignore_ascii_case(ImvActionColumn::NAME)
                && item.output_column_id == action_column),
            "branch {idx} must expose shared action column"
        );
        let delta_plan = plan.unary_input();
        let PlanNodeKind::ImvDelta(delta) = &delta_plan.kind else {
            panic!("branch {idx} must wrap source in ImvDelta");
        };
        assert!(!delta.is_root);
        assert_eq!(delta.action_column, Some(action_column));
        assert!(matches!(
            &delta_plan.unary_input().kind,
            PlanNodeKind::Scan(_)
        ));
    }

    fn assert_top_level_union_branch(
        plan: &LogicalPlanNode,
        action_column: ColumnId,
        branch_column: ColumnId,
        expected_branch_id: i64,
    ) {
        let PlanNodeKind::Project(project) = &plan.kind else {
            panic!("branch {expected_branch_id} must be normalized through Project");
        };
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2), action_column, branch_column]
        );
        assert_visible_branch_expr(&project.items[0], expected_branch_id, 0);
        assert_visible_branch_expr(&project.items[1], expected_branch_id, 1);
        let action = project
            .items
            .iter()
            .find(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
            .expect("branch Project must expose shared action column");
        assert_eq!(action.output_column_id, action_column);
        assert!(matches!(
            &action.expr.kind,
            ExprKind::ColumnRef { column_id, column, .. }
                if *column_id == action_column && column == ImvActionColumn::NAME
        ));

        let branch = project
            .items
            .iter()
            .find(|item| item.output_name.eq_ignore_ascii_case("__branch_id__"))
            .expect("branch Project must expose branch id column");
        assert_eq!(branch.output_column_id, branch_column);
        assert_eq!(branch.expr.data_type, DataType::Int32);
        assert!(!branch.expr.nullable);
        assert!(matches!(
            &branch.expr.kind,
            ExprKind::Cast { expr, target }
                if *target == DataType::Int32
                    && matches!(
                        &expr.kind,
                        ExprKind::Literal(LiteralValue::Int(value))
                            if *value == expected_branch_id
                    )
        ));

        assert!(
            contains_non_root_delta(plan.unary_input(), action_column),
            "branch {expected_branch_id} must place a non-root delta marker at the leaf scan"
        );
    }

    fn assert_visible_branch_expr(item: &ProjectItem, expected_branch_id: i64, visible_idx: u32) {
        let expected_column_id = ColumnId(1 + (expected_branch_id as u32 * 9) + visible_idx);
        assert!(matches!(
            &item.expr.kind,
            ExprKind::ColumnRef { column_id, .. } if *column_id == expected_column_id
        ));
    }

    fn contains_non_root_delta(plan: &LogicalPlanNode, action_column: ColumnId) -> bool {
        match &plan.kind {
            PlanNodeKind::ImvDelta(delta) => {
                !delta.is_root && delta.action_column == Some(action_column)
            }
            PlanNodeKind::Project(_) | PlanNodeKind::Filter(_) => {
                contains_non_root_delta(plan.unary_input(), action_column)
            }
            _ => false,
        }
    }
}
