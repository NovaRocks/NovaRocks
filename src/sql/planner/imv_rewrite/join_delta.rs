use arrow::datatypes::DataType;

use crate::sql::analysis::{JoinKind, OutputColumn, ProjectItem};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::sql::planner::imv_rewrite::annotation::ImvExtension;
use crate::sql::planner::imv_rewrite::marker::ImvVersionRef;
use crate::sql::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::sql::planner::plan::{
    LogicalImvDeltaNode, LogicalImvVersionNode, LogicalJoinNode, LogicalPlanNode,
    LogicalProjectNode, LogicalUnionNode, PlanNodeKind,
};

pub(crate) struct RewriteJoinDeltaRule;

pub(crate) fn join_delta_kind_supported(kind: crate::sql::analysis::JoinKind) -> bool {
    matches!(
        kind,
        crate::sql::analysis::JoinKind::Inner | crate::sql::analysis::JoinKind::Cross
    )
}

impl LogicalRewriteRule for RewriteJoinDeltaRule {
    fn name(&self) -> &'static str {
        "RewriteJoinDelta"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        matches!(
            &plan.kind,
            PlanNodeKind::ImvDelta(_) if matches!(&plan.unary_input().kind, PlanNodeKind::Join(_))
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
            let input = take_unary_child(&mut children);
            let LogicalPlanNode {
                kind: join_kind,
                children: mut join_children,
                required_output_columns,
            } = input;
            let PlanNodeKind::Join(join) = join_kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };

            if !join_delta_kind_supported(join.join_type) {
                return Err(format!(
                    "Iceberg IMV join delta rewrite supports inner/cross joins only, got {:?}",
                    join.join_type
                ));
            }

            let action_column = match delta.action_column {
                Some(action_column) => action_column,
                None => ctx
                    .extension::<ImvExtension>()
                    .ok_or_else(|| {
                        "RewriteJoinDelta requires ImvExtension in RewriteContext".to_string()
                    })?
                    .allocate_column_id(),
            };

            let (left, right) = take_binary_children(&mut join_children);
            let LogicalJoinNode {
                join_type,
                condition,
            } = join;
            let mut output_columns = join_output_columns(join_type, &left, &right)?;
            output_columns.push(ImvActionColumn::output_column(action_column));

            let left_delta_branch = normalize_branch_output(
                LogicalPlanNode::new(
                    PlanNodeKind::Join(LogicalJoinNode {
                        join_type,
                        condition: condition.clone(),
                    }),
                    vec![
                        mark_delta_scan(left.clone(), action_column)?,
                        mark_version_scan(right.clone(), ImvVersionRef::from_snapshot())?,
                    ],
                    required_output_columns.clone(),
                ),
                &output_columns,
            );

            let right_delta_branch = normalize_branch_output(
                LogicalPlanNode::new(
                    PlanNodeKind::Join(LogicalJoinNode {
                        join_type,
                        condition,
                    }),
                    vec![
                        mark_version_scan(left, ImvVersionRef::to_snapshot())?,
                        mark_delta_scan(right, action_column)?,
                    ],
                    required_output_columns.clone(),
                ),
                &output_columns,
            );

            Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                PlanNodeKind::Union(LogicalUnionNode {
                    all: true,
                    output_columns,
                }),
                vec![left_delta_branch, right_delta_branch],
                required_output_columns,
            )))
        })
    }
}

fn join_output_columns(
    join_type: JoinKind,
    left: &LogicalPlanNode,
    right: &LogicalPlanNode,
) -> Result<Vec<OutputColumn>, String> {
    if !join_delta_kind_supported(join_type) {
        return Err(format!(
            "Iceberg IMV join delta rewrite cannot derive output columns for unsupported join kind {:?}",
            join_type
        ));
    }
    let left_cols = plan_output_columns(left)?;
    let right_cols = plan_output_columns(right)?;
    let mut out = left_cols
        .into_iter()
        .filter(|column| !column.name.eq_ignore_ascii_case(ImvActionColumn::NAME))
        .collect::<Vec<_>>();
    out.extend(right_cols);
    out.retain(|column| !column.name.eq_ignore_ascii_case(ImvActionColumn::NAME));
    Ok(out)
}

pub(crate) fn mark_delta_scan(
    plan: LogicalPlanNode,
    action_column: ColumnId,
) -> Result<LogicalPlanNode, String> {
    mark_scan(plan, MarkerKind::Delta(action_column))
}

fn mark_version_scan(
    plan: LogicalPlanNode,
    version_ref: ImvVersionRef,
) -> Result<LogicalPlanNode, String> {
    mark_scan(plan, MarkerKind::Version(version_ref))
}

enum MarkerKind {
    Delta(ColumnId),
    Version(ImvVersionRef),
}

fn mark_scan(plan: LogicalPlanNode, marker: MarkerKind) -> Result<LogicalPlanNode, String> {
    let LogicalPlanNode {
        kind,
        mut children,
        required_output_columns,
    } = plan;
    Ok(match kind {
        PlanNodeKind::Scan(_) => wrap_scan_marker(
            LogicalPlanNode::new(kind, children, required_output_columns),
            marker,
        ),
        PlanNodeKind::Project(_) | PlanNodeKind::Filter(_) => {
            let input = take_unary_child(&mut children);
            LogicalPlanNode::new(
                kind,
                vec![mark_scan(input, marker)?],
                required_output_columns,
            )
        }
        PlanNodeKind::Join(join) => match marker {
            MarkerKind::Delta(action_column) => wrap_scan_marker(
                LogicalPlanNode::new(PlanNodeKind::Join(join), children, required_output_columns),
                MarkerKind::Delta(action_column),
            ),
            MarkerKind::Version(version_ref) => {
                let (left, right) = take_binary_children(&mut children);
                LogicalPlanNode::new(
                    PlanNodeKind::Join(LogicalJoinNode {
                        join_type: join.join_type,
                        condition: join.condition,
                    }),
                    vec![
                        mark_scan(left, MarkerKind::Version(version_ref.clone()))?,
                        mark_scan(right, MarkerKind::Version(version_ref))?,
                    ],
                    required_output_columns,
                )
            }
        },
        other_kind => {
            return Err(format!(
                "Iceberg IMV join delta rewrite supports only Scan/Project/Filter/Join join sides, got {}",
                plan_kind_from_kind(&other_kind)
            ));
        }
    })
}

fn wrap_scan_marker(scan: LogicalPlanNode, marker: MarkerKind) -> LogicalPlanNode {
    match marker {
        MarkerKind::Delta(action_column) => LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(action_column),
                branch_scope: None,
            }),
            vec![scan],
            None,
        ),
        MarkerKind::Version(version_ref) => LogicalPlanNode::new(
            PlanNodeKind::ImvVersion(LogicalImvVersionNode { version_ref }),
            vec![scan],
            None,
        ),
    }
}

fn plan_kind(plan: &LogicalPlanNode) -> &'static str {
    plan_kind_from_kind(&plan.kind)
}

fn plan_kind_from_kind(kind: &PlanNodeKind) -> &'static str {
    kind.variant_name()
}

pub(crate) fn normalize_branch_output(
    input: LogicalPlanNode,
    output_columns: &[OutputColumn],
) -> LogicalPlanNode {
    LogicalPlanNode::new(
        PlanNodeKind::Project(LogicalProjectNode {
            output_qualifier: None,
            items: output_columns
                .iter()
                .map(|column| ProjectItem {
                    expr: crate::sql::analysis::TypedExpr {
                        kind: crate::sql::analysis::ExprKind::ColumnRef {
                            column_id: column.column_id,
                            qualifier: None,
                            column: column.name.clone(),
                        },
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                    },
                    output_name: column.name.clone(),
                    output_column_id: column.column_id,
                })
                .collect(),
        }),
        vec![input],
        None,
    )
}

pub(crate) fn plan_output_columns(plan: &LogicalPlanNode) -> Result<Vec<OutputColumn>, String> {
    Ok(match &plan.kind {
        PlanNodeKind::Scan(scan) => scan.columns.clone(),
        PlanNodeKind::Project(project) => project
            .items
            .iter()
            .filter(|item| item.output_column_id != ColumnId::UNSET)
            .map(project_item_output_column)
            .collect(),
        PlanNodeKind::Aggregate(aggregate) => aggregate.output_columns.clone(),
        PlanNodeKind::Join(join) => join_output_columns(join.join_type, plan.left(), plan.right())?,
        PlanNodeKind::Sort(_) => plan_output_columns(plan.unary_input())?,
        PlanNodeKind::Limit(_) => plan_output_columns(plan.unary_input())?,
        PlanNodeKind::Filter(_) => plan_output_columns(plan.unary_input())?,
        PlanNodeKind::Union(union) => union.output_columns.clone(),
        PlanNodeKind::Intersect(intersect) => intersect.output_columns.clone(),
        PlanNodeKind::Except(except) => except.output_columns.clone(),
        PlanNodeKind::Values(values) => values.columns.clone(),
        PlanNodeKind::GenerateSeries(generate) => vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: generate.column_name.clone(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }],
        PlanNodeKind::TableFunction(table_function) => {
            let mut out = plan_output_columns(plan.unary_input())?;
            out.extend(table_function.output_columns.clone());
            out
        }
        PlanNodeKind::Window(window) => window.output_columns.clone(),
        PlanNodeKind::Repeat(_) => plan_output_columns(plan.unary_input())?,
        PlanNodeKind::CTEAnchor(_) => plan_output_columns(plan.child(1))?,
        PlanNodeKind::CTEProduce(produce) => produce.output_columns.clone(),
        PlanNodeKind::CTEConsume(consume) => consume.output_columns.clone(),
        PlanNodeKind::Decode(decode) => decode.output_columns.clone(),
        PlanNodeKind::AggregateStateMerge(merge) => merge.output_columns.clone(),
        PlanNodeKind::Apply(apply) => {
            let mut out = plan_output_columns(plan.left())?;
            out.push(apply.output_column.clone());
            out
        }
        PlanNodeKind::AssertOneRow(_) => plan_output_columns(plan.unary_input())?,
        PlanNodeKind::ImvDelta(_) => plan_output_columns(plan.unary_input())?,
        PlanNodeKind::ImvVersion(_) => plan_output_columns(plan.unary_input())?,
        PlanNodeKind::TopN(_)
        | PlanNodeKind::Exchange(_)
        | PlanNodeKind::HashAggregate(_)
        | PlanNodeKind::HashJoin(_)
        | PlanNodeKind::NestLoopJoin(_)
        | PlanNodeKind::SetOp(_) => {
            return Err(format!(
                "distributed plan node {} leaked into IMV logical output inference",
                plan.kind.variant_name()
            ));
        }
    })
}

fn take_unary_child(children: &mut Vec<LogicalPlanNode>) -> LogicalPlanNode {
    assert_eq!(children.len(), 1, "expected one logical plan child");
    children.remove(0)
}

fn take_binary_children(children: &mut Vec<LogicalPlanNode>) -> (LogicalPlanNode, LogicalPlanNode) {
    assert_eq!(children.len(), 2, "expected two logical plan children");
    let right = children.remove(1);
    let left = children.remove(0);
    (left, right)
}

fn project_item_output_column(item: &ProjectItem) -> OutputColumn {
    OutputColumn {
        column_id: item.output_column_id,
        name: item.output_name.clone(),
        data_type: item.expr.data_type.clone(),
        nullable: item.expr.nullable,
        is_internal: false,
    }
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
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::convert::logical_plan_to_opt_expr;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::planner::imv_rewrite::marker::ImvVersionRef;
    use crate::sql::planner::imv_rewrite::scan_binding::ImvVersionRole;
    use crate::sql::planner::plan::{
        LogicalAggregateNode, LogicalImvVersionNode, LogicalJoinNode, LogicalProjectNode,
        LogicalScanNode, PlanNodeKind,
    };

    #[test]
    fn supported_join_delta_kinds_are_inner_and_cross_only() {
        assert!(join_delta_kind_supported(JoinKind::Inner));
        assert!(join_delta_kind_supported(JoinKind::Cross));
        assert!(!join_delta_kind_supported(JoinKind::LeftOuter));
        assert!(!join_delta_kind_supported(JoinKind::RightOuter));
        assert!(!join_delta_kind_supported(JoinKind::FullOuter));
        assert!(!join_delta_kind_supported(JoinKind::LeftSemi));
        assert!(!join_delta_kind_supported(JoinKind::LeftAnti));
        assert!(!join_delta_kind_supported(JoinKind::RightSemi));
        assert!(!join_delta_kind_supported(JoinKind::RightAnti));
        assert!(!join_delta_kind_supported(JoinKind::NullAwareLeftAnti));
    }

    #[test]
    fn pure_join_delta_matches_imv_delta_over_join_any_root() {
        let rule = RewriteJoinDeltaRule;
        let ctx = build_ctx();
        let non_root = LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(ColumnId(100)),
                branch_scope: None,
            }),
            vec![join_of(scan("l", 1), scan("r", 10))],
            None,
        );
        let arena_rc = ctx.scalar_arena();
        let non_root_expr = logical_plan_to_opt_expr(&non_root, &mut arena_rc.borrow_mut());
        assert!(rule.matches(&non_root_expr, &ctx));

        let over_agg = delta(aggregate_over(join_over(JoinKind::Inner)));
        let over_agg_expr = logical_plan_to_opt_expr(&over_agg, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&over_agg_expr, &ctx));
    }

    #[test]
    fn pure_join_delta_expands_into_union_without_outer_aggregate() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(ColumnId(100)),
                branch_scope: None,
            }),
            vec![join_over(JoinKind::Inner)],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("expand")
        else {
            panic!("pure join-delta must expand ImvDelta(Join) directly into a Union");
        };
        let arena = ctx.scalar_arena();
        let changed =
            crate::sql::optimizer::convert::opt_expr_to_logical_plan(changed_expr, &arena.borrow());
        let PlanNodeKind::Union(union) = &changed.kind else {
            panic!("expected Union");
        };

        assert!(union.all);
        assert_eq!(changed.children.len(), 2);
        let left = assert_normalized_branch(changed.child(0), ColumnId(100));
        let PlanNodeKind::Join(left_join) = &left.kind else {
            panic!("expected Join");
        };
        assert_condition_refs(left_join.condition.as_ref());
        assert_delta(left.left(), "left", ColumnId(100));
        assert_version(left.right(), "right", ImvVersionRole::From);

        let right = assert_normalized_branch(changed.child(1), ColumnId(100));
        let PlanNodeKind::Join(right_join) = &right.kind else {
            panic!("expected Join");
        };
        assert_condition_refs(right_join.condition.as_ref());
        assert_version(right.left(), "left", ImvVersionRole::To);
        assert_delta(right.right(), "right", ColumnId(100));
    }

    #[test]
    fn pure_join_delta_drops_preexisting_action_metadata_outputs() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(ColumnId(100)),
                branch_scope: None,
            }),
            vec![join_of(
                project_over(scan_with_action_metadata("left", 1, 8)),
                project_over(scan_with_action_metadata("right", 10, 15)),
            )],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("expand")
        else {
            panic!("pure join-delta must expand into a Union");
        };
        let arena = ctx.scalar_arena();
        let changed =
            crate::sql::optimizer::convert::opt_expr_to_logical_plan(changed_expr, &arena.borrow());
        let PlanNodeKind::Union(union) = &changed.kind else {
            panic!("expected Union");
        };

        let action_outputs = union
            .output_columns
            .iter()
            .filter(|column| column.name.eq_ignore_ascii_case(ImvActionColumn::NAME))
            .collect::<Vec<_>>();
        assert_eq!(action_outputs.len(), 1);
        assert_eq!(action_outputs[0].column_id, ColumnId(100));
        assert!(action_outputs[0].is_internal);
        for input in &changed.children {
            let PlanNodeKind::Project(project) = &input.kind else {
                panic!("expected normalized branch Project");
            };
            let action_items = project
                .items
                .iter()
                .filter(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
                .collect::<Vec<_>>();
            assert_eq!(action_items.len(), 1);
            assert_eq!(action_items[0].output_column_id, ColumnId(100));
        }
    }

    #[test]
    fn pure_join_delta_rejects_outer_join() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(ColumnId(100)),
                branch_scope: None,
            }),
            vec![join_over(JoinKind::LeftOuter)],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        let err = rule.apply(expr, &mut ctx).expect_err("outer must reject");
        assert!(err.contains("inner/cross"), "unexpected: {err}");
    }

    #[test]
    fn pure_join_delta_nested_leaves_inner_join_delta_for_next_iteration() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let inner = join_of(scan("a", 1), scan("b", 10));
        let outer = join_of_with_left(inner, scan("c", 20));
        let plan = LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(ColumnId(100)),
                branch_scope: None,
            }),
            vec![outer],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        let RewriteResult::Changed(changed_expr) =
            rule.apply(expr, &mut ctx).expect("expand outer")
        else {
            panic!("expected Union");
        };
        let arena = ctx.scalar_arena();
        let changed =
            crate::sql::optimizer::convert::opt_expr_to_logical_plan(changed_expr, &arena.borrow());
        let PlanNodeKind::Union(_) = &changed.kind else {
            panic!("expected Union");
        };

        let left = assert_normalized_branch(changed.child(0), ColumnId(100));
        assert!(
            plan_contains_inner_join_delta(left.left()),
            "outer-left delta side must leave ImvDelta(Join(a,b)) for the next fixpoint iteration"
        );
    }

    #[test]
    fn mark_delta_scan_wraps_nested_join_whole() {
        // Delta marker over a Join must wrap the entire join (pending recursive join-delta expansion),
        // NOT push into the two sides.
        let join = join_of(scan("a", 1), scan("b", 10));
        let marked = mark_delta_scan(join, ColumnId(100)).expect("mark delta over join");
        let PlanNodeKind::ImvDelta(delta) = &marked.kind else {
            panic!("expected ImvDelta wrapping the whole join, got {marked:?}");
        };
        assert!(!delta.is_root, "nested join delta marker is not root");
        assert_eq!(delta.action_column, Some(ColumnId(100)));
        assert!(matches!(&marked.children[0].kind, PlanNodeKind::Join(_)));
    }

    #[test]
    fn mark_version_scan_pushes_same_role_down_both_join_sides() {
        // Version marker over a Join distributes over the join:
        // Version(Join(a,b), from) == Join(Version(a, from), Version(b, from)).
        let join = join_of(scan("a", 1), scan("b", 10));
        let marked = mark_version_scan(join, ImvVersionRef::from_snapshot())
            .expect("mark version over join");
        let PlanNodeKind::Join(j) = &marked.kind else {
            panic!("expected Join with both sides version-marked, got {marked:?}");
        };
        let left_v = assert_version_side(marked.left());
        let right_v = assert_version_side(marked.right());
        assert_eq!(
            left_v.version_ref,
            ImvVersionRef {
                role: ImvVersionRole::From
            }
        );
        assert_eq!(
            right_v.version_ref,
            ImvVersionRef {
                role: ImvVersionRole::From
            }
        );
    }

    fn assert_version_side(plan: &LogicalPlanNode) -> &LogicalImvVersionNode {
        match &plan.kind {
            PlanNodeKind::ImvVersion(v) => v,
            other => panic!("expected ImvVersion on join side, got {other:?}"),
        }
    }

    fn assert_normalized_branch(
        plan: &LogicalPlanNode,
        action_column: ColumnId,
    ) -> &LogicalPlanNode {
        let PlanNodeKind::Project(project) = &plan.kind else {
            panic!("expected normalized branch Project");
        };
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_column_id)
                .collect::<Vec<_>>(),
            plan_output_columns(plan.unary_input())
                .expect("branch output columns")
                .into_iter()
                .map(|column| column.column_id)
                .chain(std::iter::once(action_column))
                .collect::<Vec<_>>()
        );
        assert!(
            project
                .items
                .iter()
                .any(|item| item.output_name.eq_ignore_ascii_case("__change_op")
                    && item.output_column_id == action_column),
            "normalized branch Project must expose shared action column"
        );
        let join_plan = plan.unary_input();
        let PlanNodeKind::Join(_) = &join_plan.kind else {
            panic!("expected Project(Join)");
        };
        join_plan
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
                group_by: vec![col_expr(1, "l_k")],
                aggregates: Vec::new(),
                output_columns: vec![output_column(1, "l_k"), output_column(10, "r_k")],
                already_pushed: false,
            }),
            vec![input],
            None,
        )
    }

    fn join_over(join_type: JoinKind) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: join_type,
                condition: Some(condition()),
            }),
            vec![
                project_over(scan("left", 1)),
                project_over(scan("right", 10)),
            ],
            None,
        )
    }

    fn join_of(left: LogicalPlanNode, right: LogicalPlanNode) -> LogicalPlanNode {
        let left_cols = plan_output_columns(&left).expect("left output columns");
        let right_cols = plan_output_columns(&right).expect("right output columns");
        let left_key = &left_cols[0];
        let right_key = &right_cols[0];
        LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col_expr(left_key.column_id.0, &left_key.name)),
                        op: BinOp::Eq,
                        right: Box::new(col_expr(right_key.column_id.0, &right_key.name)),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                }),
            }),
            vec![left, right],
            None,
        )
    }

    fn join_of_with_left(left: LogicalPlanNode, right: LogicalPlanNode) -> LogicalPlanNode {
        join_of(left, right)
    }

    fn project_over(input: LogicalPlanNode) -> LogicalPlanNode {
        let columns = match &input.kind {
            PlanNodeKind::Scan(scan) => scan.columns.clone(),
            _ => unreachable!(),
        };
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: columns
                    .into_iter()
                    .map(|column| ProjectItem {
                        expr: col_expr(column.column_id.0, &column.name),
                        output_name: column.name,
                        output_column_id: column.column_id,
                    })
                    .collect(),
                output_qualifier: None,
            }),
            vec![input],
            None,
        )
    }

    fn scan(name: &str, first_id: u32) -> LogicalPlanNode {
        let columns = vec![
            column_def(&format!("{name}_k")),
            column_def(&format!("{name}_v")),
        ];
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
                    output_column(first_id, &format!("{name}_k")),
                    output_column(first_id + 1, &format!("{name}_v")),
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

    fn scan_with_action_metadata(name: &str, first_id: u32, action_id: u32) -> LogicalPlanNode {
        let mut plan = scan(name, first_id);
        let PlanNodeKind::Scan(scan) = &mut plan.kind else {
            unreachable!();
        };
        scan.columns.push(OutputColumn {
            column_id: ColumnId(action_id),
            name: ImvActionColumn::NAME.to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: false,
        });
        plan
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

    fn condition() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_expr(1, "left_k")),
                op: BinOp::Eq,
                right: Box::new(col_expr(10, "right_k")),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn assert_condition_refs(condition: Option<&TypedExpr>) {
        let Some(TypedExpr {
            kind: ExprKind::BinaryOp { left, op, right },
            ..
        }) = condition
        else {
            panic!("expected binary join condition");
        };
        assert_eq!(*op, BinOp::Eq);
        assert!(matches!(
            &left.kind,
            ExprKind::ColumnRef { column_id, column, .. }
                if *column_id == ColumnId(1) && column == "left_k"
        ));
        assert!(matches!(
            &right.kind,
            ExprKind::ColumnRef { column_id, column, .. }
                if *column_id == ColumnId(10) && column == "right_k"
        ));
    }

    fn plan_contains_inner_join_delta(plan: &LogicalPlanNode) -> bool {
        match &plan.kind {
            PlanNodeKind::ImvDelta(_) => {
                matches!(&plan.unary_input().kind, PlanNodeKind::Join(_))
                    || plan.children.iter().any(plan_contains_inner_join_delta)
            }
            _ => plan.children.iter().any(plan_contains_inner_join_delta),
        }
    }

    fn assert_delta(plan: &LogicalPlanNode, expected_scan: &str, action_column: ColumnId) {
        let PlanNodeKind::Project(project) = &plan.kind else {
            panic!("expected Project");
        };
        let delta_plan = plan.unary_input();
        let PlanNodeKind::ImvDelta(delta) = &delta_plan.kind else {
            panic!("expected Project(ImvDelta(...))");
        };
        assert!(!delta.is_root);
        assert_eq!(delta.action_column, Some(action_column));
        assert_scan(delta_plan.unary_input(), expected_scan);
    }

    fn assert_version(plan: &LogicalPlanNode, expected_scan: &str, role: ImvVersionRole) {
        let PlanNodeKind::Project(project) = &plan.kind else {
            panic!("expected Project");
        };
        let version_plan = plan.unary_input();
        let PlanNodeKind::ImvVersion(version) = &version_plan.kind else {
            panic!("expected Project(ImvVersion(...))");
        };
        assert_eq!(version.version_ref, ImvVersionRef { role });
        assert_scan(version_plan.unary_input(), expected_scan);
    }

    fn assert_scan(plan: &LogicalPlanNode, expected_scan: &str) {
        let PlanNodeKind::Scan(scan) = &plan.kind else {
            panic!("expected Scan");
        };
        assert_eq!(scan.table.name, expected_scan);
    }
}
