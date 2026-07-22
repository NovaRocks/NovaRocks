// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::datatypes::DataType;

use crate::mv::persistence::schema::BRANCH_ID_COLUMN_NAME;
use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::sql::planner::imv_rewrite::annotation::ImvExtension;
use crate::sql::planner::imv_rewrite::column_alloc::allocate_imv_output_column;
use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
use crate::sql::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::sql::planner::logical::{
    LogicalAggregateNode, LogicalImvDeltaNode, LogicalPlanKind, LogicalPlanNode, LogicalUnionNode,
};

pub(crate) struct RewriteBranchUnionRule;

impl LogicalRewriteRule for RewriteBranchUnionRule {
    fn name(&self) -> &'static str {
        "RewriteBranchUnion"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        let LogicalPlanKind::ImvDelta(delta) = &plan.kind else {
            return false;
        };
        if !delta.is_root {
            return false;
        }
        let input = plan.unary_input();
        matches!(
            &input.kind,
            LogicalPlanKind::Union(union)
                if union.all
                    && input.children.iter().all(is_branch_union_aggregate_branch)
                    && !plan_contains_imv_marker(input)
        )
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        bridge_apply_result(expr, ctx, |plan, ctx| {
            let LogicalPlanNode {
                kind,
                mut children,
                required_output_columns: _,
            } = plan;
            let LogicalPlanKind::ImvDelta(delta) = kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            if !delta.is_root {
                return Ok(PlanRewriteResult::Unchanged);
            }
            let action_column = delta.action_column;
            if children.len() != 1 {
                return Ok(PlanRewriteResult::Unchanged);
            }
            let union_plan = children.remove(0);
            let LogicalPlanNode {
                kind,
                children: inputs,
                required_output_columns,
            } = union_plan;
            let LogicalPlanKind::Union(union) = kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            if !union.all {
                return Err("Iceberg IMV branch UNION rewrite supports UNION ALL only".to_string());
            }
            if inputs.len() < 2 {
                return Err(
                    "Iceberg IMV branch UNION rewrite requires at least two aggregate branches"
                        .to_string(),
                );
            }

            for branch in &inputs {
                if !is_branch_union_aggregate_branch(branch) {
                    return Err(format!(
                        "Iceberg IMV branch UNION rewrite supports only aggregate or Project-over-Aggregate branches, got {}",
                        plan_kind(branch)
                    ));
                }
            }

            let ext = ctx
                .extension::<ImvExtension>()
                .ok_or_else(|| {
                    "RewriteBranchUnion requires ImvExtension in RewriteContext".to_string()
                })?
                .clone();
            let output_columns = branch_union_aggregate_change_stream_output_columns(&ext, ctx)?;
            let mut rewritten_inputs = Vec::with_capacity(inputs.len());
            for (idx, branch) in inputs.into_iter().enumerate() {
                let branch_id = i32::try_from(idx)
                    .map_err(|_| "Iceberg IMV branch UNION branch index overflow".to_string())?;
                let branch_kind = plan_kind(&branch);
                let branch = extract_branch_union_aggregate_branch(branch).ok_or_else(|| {
                    format!(
                        "Iceberg IMV branch UNION rewrite supports only aggregate or Project-over-Aggregate branches, got {}",
                        branch_kind
                    )
                })?;
                // Tag the aggregate core as an independent, branch-scoped delta sub-problem.
                // The existing aggregate-state (and join/union-delta beneath it) rules
                // decompose it in later stages, reading branch_scope off this marker.
                // Each branch becomes its own root delta sub-problem: `is_root` is
                // per-sub-problem here, so the post-branch plan intentionally holds one
                // root delta per branch (not a single global root).
                let scope = crate::sql::planner::table::BranchScope {
                    branch_id_column_name: BRANCH_ID_COLUMN_NAME.to_string(),
                    branch_id,
                };
                let aggregate = LogicalPlanNode::new(
                    LogicalPlanKind::Aggregate(branch.aggregate),
                    vec![branch.aggregate_input],
                    branch.aggregate_required_output_columns,
                );
                let core = LogicalPlanNode::new(
                    LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                        is_root: true,
                        action_column,
                        branch_scope: Some(scope),
                    }),
                    vec![aggregate],
                    None,
                );
                rewritten_inputs.push(core);
            }

            Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                LogicalPlanKind::Union(LogicalUnionNode {
                    all: true,
                    output_columns,
                }),
                rewritten_inputs,
                required_output_columns,
            )))
        })
    }
}

struct BranchUnionAggregateBranch {
    aggregate: LogicalAggregateNode,
    aggregate_input: LogicalPlanNode,
    aggregate_required_output_columns: Option<std::collections::HashSet<ColumnId>>,
}

fn is_branch_union_aggregate_branch(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        LogicalPlanKind::Aggregate(_) => true,
        LogicalPlanKind::Project(_) => {
            matches!(&plan.unary_input().kind, LogicalPlanKind::Aggregate(_))
        }
        _ => false,
    }
}

fn extract_branch_union_aggregate_branch(
    branch: LogicalPlanNode,
) -> Option<BranchUnionAggregateBranch> {
    let LogicalPlanNode {
        kind,
        mut children,
        required_output_columns,
    } = branch;
    match kind {
        LogicalPlanKind::Aggregate(aggregate) => Some(BranchUnionAggregateBranch {
            aggregate,
            aggregate_input: single_child(&mut children)?,
            aggregate_required_output_columns: required_output_columns,
        }),
        LogicalPlanKind::Project(project) => {
            let _ = project;
            let aggregate_plan = single_child(&mut children)?;
            let LogicalPlanNode {
                kind,
                mut children,
                required_output_columns: aggregate_required_output_columns,
            } = aggregate_plan;
            let LogicalPlanKind::Aggregate(aggregate) = kind else {
                return None;
            };
            Some(BranchUnionAggregateBranch {
                aggregate,
                aggregate_input: single_child(&mut children)?,
                aggregate_required_output_columns,
            })
        }
        _ => None,
    }
}

fn single_child(children: &mut Vec<LogicalPlanNode>) -> Option<LogicalPlanNode> {
    if children.len() == 1 {
        Some(children.remove(0))
    } else {
        None
    }
}

fn branch_union_aggregate_change_stream_output_columns(
    ext: &ImvExtension,
    ctx: &RewriteContext,
) -> Result<Vec<OutputColumn>, String> {
    let (_shape, layout) = ext.mv_ctx.aggregate_shape_and_layout_for_execution()?;
    let mut columns =
        Vec::with_capacity(1 + layout.visible_columns.len() + layout.state_columns.len() + 6);
    columns.push(allocate_imv_output_column(
        ctx,
        &layout.row_id_column.column.name,
        DataType::Utf8,
        false,
        true,
    )?);
    for column in &layout.visible_columns {
        columns.push(allocate_imv_output_column(
            ctx,
            &column.name,
            column.data_type.clone(),
            column.nullable,
            false,
        )?);
    }
    for column in &layout.state_columns {
        let data_type = match column.state_role {
            crate::mv::model::AggregateStateRole::Single => DataType::Binary,
            crate::mv::model::AggregateStateRole::RetractionCount => column.data_type.clone(),
        };
        columns.push(allocate_imv_output_column(
            ctx,
            &column.name,
            data_type,
            column.nullable,
            true,
        )?);
    }
    columns.push(allocate_imv_output_column(
        ctx,
        BRANCH_ID_COLUMN_NAME,
        DataType::Int32,
        false,
        true,
    )?);
    columns.push(allocate_imv_output_column(
        ctx,
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        DataType::Utf8,
        true,
        true,
    )?);
    columns.push(allocate_imv_output_column(
        ctx,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        DataType::Int64,
        true,
        true,
    )?);
    columns.push(allocate_imv_output_column(
        ctx,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        DataType::Int64,
        true,
        true,
    )?);
    columns.push(allocate_imv_output_column(
        ctx,
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
        DataType::Int64,
        true,
        true,
    )?);
    columns.push(allocate_imv_output_column(
        ctx,
        ImvActionColumn::NAME,
        DataType::Int8,
        false,
        true,
    )?);
    Ok(columns)
}

fn plan_kind(plan: &LogicalPlanNode) -> &'static str {
    match &plan.kind {
        LogicalPlanKind::Scan(_) => "Scan",
        LogicalPlanKind::Filter(_) => "Filter",
        LogicalPlanKind::Project(_) => "Project",
        LogicalPlanKind::Aggregate(_) => "Aggregate",
        LogicalPlanKind::Join(_) => "Join",
        LogicalPlanKind::Union(_) => "Union",
        _ => "Other",
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::logical::*;
    use crate::sql::planner::payload::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use super::*;
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::mv::persistence::schema::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, BranchIdColumnContract,
        BranchUnionContract, JoinContract, JoinContractKind, JoinPredicateLineage,
        QualifiedFieldLineage,
    };
    use crate::mv::rewrite::context::IcebergMvRewriteContext;
    use crate::mv::rewrite::context::tests_support::{
        make_mv_definition, make_pin, make_ref, make_schema_contract, make_target, parse_query,
    };
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::column_id::{ColumnId, ColumnRefFactory};
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::planner::logical::{
        LogicalAggregateNode, LogicalJoinNode, LogicalPlanKind, LogicalPlanNode, LogicalUnionNode,
    };
    use crate::sql::planner::optimizer_bridge::logical::to_optimizer_expr;
    use crate::sql::planner::payload::{
        AggregateCall, PlanFilterNode, PlanProjectNode, PlanScanNode,
    };
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

    #[test]
    fn rewrites_top_union_of_aggregates_into_branch_scoped_merges() {
        let rule = RewriteBranchUnionRule;
        let mut ctx = build_ctx();
        let plan = root_delta(LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            }),
            vec![
                aggregate_over(scan("t1", 1)),
                aggregate_over(scan("t2", 10)),
            ],
            None,
        ));

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(rewritten_expr) = rule.apply(expr, &mut ctx).expect("rewrite")
        else {
            panic!("expected Changed(Union)");
        };
        let arena = ctx.scalar_arena();
        let rewritten = crate::sql::planner::optimizer_bridge::logical::to_logical_plan(
            rewritten_expr,
            &arena.borrow(),
        );
        let LogicalPlanKind::Union(_) = &rewritten.kind else {
            panic!("expected Changed(Union), got {rewritten:?}");
        };

        assert_eq!(rewritten.children.len(), 2);
        for (idx, branch) in rewritten.children.iter().enumerate() {
            assert_branch_scoped_delta(branch, idx as i32);
        }
    }

    #[test]
    fn rewrites_project_over_aggregate_branches_into_branch_scoped_merges() {
        let rule = RewriteBranchUnionRule;
        let mut ctx = build_ctx();
        let plan = root_delta(LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(30, "total")],
            }),
            vec![
                project_over_aggregate(scan("t1", 1)),
                project_over_aggregate(scan("t2", 10)),
            ],
            None,
        ));

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(rewritten_expr) = rule.apply(expr, &mut ctx).expect("rewrite")
        else {
            panic!("expected Changed(Union)");
        };
        let arena = ctx.scalar_arena();
        let rewritten = crate::sql::planner::optimizer_bridge::logical::to_logical_plan(
            rewritten_expr,
            &arena.borrow(),
        );
        let LogicalPlanKind::Union(_) = &rewritten.kind else {
            panic!("expected Changed(Union), got {rewritten:?}");
        };

        assert_eq!(rewritten.children.len(), 2);
        for (idx, branch) in rewritten.children.iter().enumerate() {
            assert_branch_scoped_delta(branch, idx as i32);
        }
    }

    #[test]
    fn rejects_non_aggregate_branch() {
        let rule = RewriteBranchUnionRule;
        let mut ctx = build_ctx();
        let plan = root_delta(LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            }),
            vec![aggregate_over(scan("t1", 1)), scan("t2", 10)],
            None,
        ));

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        let err = rule
            .apply(expr, &mut ctx)
            .expect_err("scan branch must fail");
        assert!(
            err.contains("supports only aggregate or Project-over-Aggregate branches"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn does_not_match_marked_union() {
        let rule = RewriteBranchUnionRule;
        let ctx = build_ctx();
        let plan = root_delta(LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            }),
            vec![
                LogicalPlanNode::new(
                    LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                        is_root: false,
                        action_column: None,
                        branch_scope: None,
                    }),
                    vec![aggregate_over(scan("t1", 1))],
                    None,
                ),
                aggregate_over(scan("t2", 10)),
            ],
            None,
        ));

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn does_not_match_projection_filter_union() {
        let rule = RewriteBranchUnionRule;
        let ctx = build_ctx();
        let plan = root_delta(LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(2, "amount")],
            }),
            vec![project_over_filter("t1", 1), project_over_filter("t2", 10)],
            None,
        ));

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn pipeline_branch_union_of_aggregates_final_shape_is_stable() {
        use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
        use crate::sql::planner::imv_rewrite::pipeline::build_imv_pipeline;

        let mut ctx = build_ctx();
        // build_ctx() registers ice.db.b as the only known base table; both
        // branches must reference that same table so scan binding succeeds.
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            }),
            vec![aggregate_over(scan("b", 1)), aggregate_over(scan("b", 10))],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        let out_expr = build_imv_pipeline()
            .rewrite(expr, &mut ctx)
            .expect("pipeline must succeed");
        let arena = ctx.scalar_arena();
        let out = crate::sql::planner::optimizer_bridge::logical::to_logical_plan(
            out_expr,
            &arena.borrow(),
        );

        // The root may be an apply-key Project above the branch Union. The
        // branch Union itself carries branch-scoped aggregate change-streams
        // with __branch_id__ and __change_op, and no IMV marker may remain.
        assert!(
            !plan_contains_imv_marker(&out),
            "no marker may survive validation"
        );
        let (union, branches) = top_branch_union(&out);
        assert_eq!(branches.len(), 2);
        assert!(
            union
                .output_columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case("__branch_id__")),
            "union output must expose __branch_id__"
        );
        assert_locator_columns_precede_action(
            &union
                .output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
        );
        for branch in branches {
            assert_aggregate_change_stream_branch(branch);
        }
    }

    fn assert_branch_scoped_delta(branch: &LogicalPlanNode, expected_branch_id: i32) {
        let LogicalPlanKind::ImvDelta(delta) = &branch.kind else {
            panic!("branch core must be a delegated ImvDelta, got {branch:?}")
        };
        assert!(
            delta.is_root,
            "branch sub-problem delta must be a root delta"
        );
        assert_eq!(
            delta.branch_scope.as_ref().map(|s| s.branch_id),
            Some(expected_branch_id)
        );
        assert!(
            matches!(&branch.unary_input().kind, LogicalPlanKind::Aggregate(_)),
            "delta must sit directly over the Aggregate core"
        );
    }

    fn assert_aggregate_change_stream_branch(branch: &LogicalPlanNode) {
        let output_names = aggregate_change_stream_output_names(branch);
        assert!(
            output_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case("__branch_id__")),
            "change-stream branch output must expose __branch_id__"
        );
        assert!(
            output_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(ImvActionColumn::NAME)),
            "change-stream branch output must expose __change_op"
        );
        assert_locator_columns_precede_action(&output_names);
        assert!(
            contains_join_kind(branch, JoinKind::LeftOuter),
            "relational change-stream branch must merge delta and old target state once"
        );
        assert!(
            contains_join_kind(branch, JoinKind::Cross),
            "relational change-stream branch must expand DELETE/INSERT branches once"
        );
        assert!(
            contains_branch_marker_values(branch),
            "relational change-stream branch must generate a branch marker VALUES source"
        );
        assert!(
            project_filter_contains_state_all_zero(branch),
            "relational change-stream branch must guard INSERT output with state_all_zero"
        );
        assert!(
            contains_target_state_scan(branch),
            "change-stream branch must read old target state"
        );
        assert!(
            contains_signed_state_aggregate(branch),
            "change-stream branch must contain signed state aggregate"
        );
    }

    fn aggregate_change_stream_output_names(branch: &LogicalPlanNode) -> Vec<&str> {
        match &branch.kind {
            LogicalPlanKind::Project(project) => project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect(),
            LogicalPlanKind::Union(union) => union
                .output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect(),
            LogicalPlanKind::CTEAnchor(_) => aggregate_change_stream_output_names(branch.child(1)),
            other => panic!("expected aggregate change-stream branch, got {other:?}"),
        }
    }

    fn top_branch_union(plan: &LogicalPlanNode) -> (&LogicalUnionNode, &[LogicalPlanNode]) {
        let union_plan = match &plan.kind {
            LogicalPlanKind::Project(_) => plan.unary_input(),
            LogicalPlanKind::Union(_) => plan,
            other => panic!("expected top Union or Project over Union, got {other:?}"),
        };
        let LogicalPlanKind::Union(union) = &union_plan.kind else {
            unreachable!("top_branch_union only returns a Union plan")
        };
        (union, union_plan.children.as_slice())
    }

    fn assert_locator_columns_precede_action(output_names: &[&str]) {
        let branch_idx = output_names
            .iter()
            .position(|name| name.eq_ignore_ascii_case(BRANCH_ID_COLUMN_NAME))
            .expect("branch-union aggregate output must include branch id");
        let file_idx = output_names
            .iter()
            .position(|name| {
                name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_FILE_PATH_COL)
            })
            .expect("branch-union aggregate output must include file locator");
        let pos_idx = output_names
            .iter()
            .position(|name| {
                name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_POS_COL)
            })
            .expect("branch-union aggregate output must include row-position locator");
        let row_id_idx = output_names
            .iter()
            .position(|name| {
                name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_ID_COL)
            })
            .expect("branch-union aggregate output must include row-lineage id");
        let last_updated_seq_idx = output_names
            .iter()
            .position(|name| {
                name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL)
            })
            .expect("branch-union aggregate output must include last-updated sequence");
        let action_idx = output_names
            .iter()
            .position(|name| name.eq_ignore_ascii_case(ImvActionColumn::NAME))
            .expect("branch-union aggregate output must include action column");
        assert_eq!(
            [
                branch_idx + 1,
                branch_idx + 2,
                branch_idx + 3,
                branch_idx + 4,
                branch_idx + 5,
            ],
            [
                file_idx,
                pos_idx,
                row_id_idx,
                last_updated_seq_idx,
                action_idx
            ],
            "branch-union aggregate output must keep branch id, locator metadata, and action column contiguous: {output_names:?}"
        );
    }

    fn contains_join_kind(plan: &LogicalPlanNode, join_type: JoinKind) -> bool {
        matches!(
            &plan.kind,
            LogicalPlanKind::Join(join) if join.join_type == join_type
        ) || plan
            .children
            .iter()
            .any(|child| contains_join_kind(child, join_type))
    }

    fn contains_branch_marker_values(plan: &LogicalPlanNode) -> bool {
        matches!(&plan.kind, LogicalPlanKind::Values(values)
            if values.columns.iter().any(|column| {
                column.name.eq_ignore_ascii_case("__imv_change_branch")
            })
        ) || plan.children.iter().any(contains_branch_marker_values)
    }

    fn project_filter_contains_state_all_zero(plan: &LogicalPlanNode) -> bool {
        let LogicalPlanKind::Project(_) = &plan.kind else {
            return plan
                .children
                .iter()
                .any(project_filter_contains_state_all_zero);
        };
        let Some(filter_plan) = plan.children.first() else {
            return false;
        };
        let LogicalPlanKind::Filter(filter) = &filter_plan.kind else {
            return false;
        };
        expr_contains_function(&filter.predicate, "state_all_zero")
    }

    fn expr_contains_function(expr: &TypedExpr, name: &str) -> bool {
        match &expr.kind {
            ExprKind::FunctionCall {
                name: func, args, ..
            }
            | ExprKind::AggregateCall {
                name: func, args, ..
            } => {
                func.eq_ignore_ascii_case(name)
                    || args.iter().any(|arg| expr_contains_function(arg, name))
            }
            ExprKind::BinaryOp { left, right, .. } => {
                expr_contains_function(left, name) || expr_contains_function(right, name)
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. } => expr_contains_function(expr, name),
            ExprKind::InList { expr, list, .. } => {
                expr_contains_function(expr, name)
                    || list.iter().any(|item| expr_contains_function(item, name))
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                expr_contains_function(expr, name)
                    || expr_contains_function(low, name)
                    || expr_contains_function(high, name)
            }
            ExprKind::Like { expr, pattern, .. } => {
                expr_contains_function(expr, name) || expr_contains_function(pattern, name)
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_deref()
                    .is_some_and(|expr| expr_contains_function(expr, name))
                    || when_then.iter().any(|(when_expr, then_expr)| {
                        expr_contains_function(when_expr, name)
                            || expr_contains_function(then_expr, name)
                    })
                    || else_expr
                        .as_deref()
                        .is_some_and(|expr| expr_contains_function(expr, name))
            }
            ExprKind::LambdaFunction { body, .. } => expr_contains_function(body, name),
            ExprKind::Nested(expr) | ExprKind::Lambda { body: expr, .. } => {
                expr_contains_function(expr, name)
            }
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => {
                args.iter().any(|arg| expr_contains_function(arg, name))
                    || partition_by
                        .iter()
                        .any(|expr| expr_contains_function(expr, name))
                    || order_by
                        .iter()
                        .any(|item| expr_contains_function(&item.expr, name))
            }
            ExprKind::ColumnRef { .. }
            | ExprKind::LambdaParamRef { .. }
            | ExprKind::Literal(_)
            | ExprKind::SubqueryPlaceholder { .. } => false,
        }
    }

    fn contains_target_state_scan(plan: &LogicalPlanNode) -> bool {
        matches!(
            &plan.kind,
            LogicalPlanKind::Scan(PlanScanNode {
                table: TableDef {
                    source: ScanSource::IcebergMvTargetState(_),
                    ..
                },
                ..
            })
        ) || plan.children.iter().any(contains_target_state_scan)
    }

    fn contains_signed_state_aggregate(plan: &LogicalPlanNode) -> bool {
        matches!(
            &plan.kind,
            LogicalPlanKind::Aggregate(LogicalAggregateNode { aggregates, .. })
                if aggregates.iter().any(|call| call.name.ends_with("_state_signed"))
        ) || plan.children.iter().any(contains_signed_state_aggregate)
    }

    fn single_state_column(type_signature: &str) -> AggregateStateColumnContract {
        AggregateStateColumnContract {
            column_name: "__agg_state_s".to_string(),
            target_field_id: 200,
            type_signature: type_signature.to_string(),
            nullable: true,
            role: AggregateStateRoleContract::Single,
        }
    }

    fn retraction_count_state_column() -> AggregateStateColumnContract {
        AggregateStateColumnContract {
            column_name: "__agg_state___ivm_row_count".to_string(),
            target_field_id: 201,
            type_signature: "long".to_string(),
            nullable: false,
            role: AggregateStateRoleContract::RetractionCount,
        }
    }

    fn build_ctx() -> RewriteContext {
        let mut mv_def = make_mv_definition();
        mv_def.select_sql =
            "SELECT region, sum(amount) AS s FROM ice.db.b GROUP BY region".to_string();
        mv_def.primary_key_columns = vec!["region".to_string()];
        let mut contract = make_schema_contract();
        contract.target.visible_columns[0].output_name = "region".to_string();
        contract.target.visible_columns[1].output_name = "s".to_string();
        contract.target.hidden_apply_key.column_name = "__row_id__".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.target.hidden_apply_key.source = ApplyKeySource::GroupRowId;
        contract.branch = Some(BranchUnionContract {
            branch_id_column: BranchIdColumnContract {
                column_name: crate::mv::persistence::schema::BRANCH_ID_COLUMN_NAME.to_string(),
                target_field_id: 998,
            },
            branch_count: 2,
            inner_apply_key_source: ApplyKeySource::GroupRowId,
        });
        contract.aggregate = Some(AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: vec![
                single_state_column("binary"),
                retraction_count_state_column(),
            ],
        });
        mv_def.schema_contract = Some(contract.clone());

        let target_schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        100,
                        "region",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        101,
                        "s",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        999,
                        "__row_id__",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::required(
                        998,
                        "__branch_id__",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        200,
                        "__agg_state_s",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                    Arc::new(NestedField::required(
                        201,
                        "__agg_state___ivm_row_count",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()
                .expect("build schema"),
        );
        let mv_ctx = Arc::new(
            IcebergMvRewriteContext::from_definition_parts(
                make_target(),
                42,
                Some("sess_cat".to_string()),
                "sess_db".to_string(),
                Arc::new(mv_def),
                Arc::new(parse_query(
                    "SELECT region, sum(amount) AS s FROM ice.db.b GROUP BY region",
                )),
                Arc::from(vec![make_ref("ice", "db", "b")]),
                Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")])),
                Some(99),
                "uuid-tgt".to_string(),
                target_schema,
                Some(Arc::new(contract)),
            )
            .expect("aggregate rewrite context must build"),
        );

        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_scalar_arena(std::rc::Rc::new(
            std::cell::RefCell::new(ScalarArena::new()),
        ));
        let factory = std::rc::Rc::new(std::cell::RefCell::new(ColumnRefFactory::new()));
        factory.borrow_mut().reserve_until(100);
        ctx.set_column_ref_factory(std::rc::Rc::clone(&factory));
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx,
            annotation: ImvPlanAnnotation::default(),
        });
        ctx
    }

    fn build_two_base_join_ctx() -> RewriteContext {
        let mut ctx = build_ctx();
        let mut mv_def = make_mv_definition();
        mv_def.select_sql = concat!(
            "SELECT l.region, sum(l.amount) AS s ",
            "FROM ice.db.l l JOIN ice.db.r r ON l.region = r.region ",
            "GROUP BY l.region"
        )
        .to_string();
        mv_def.base_table_refs = vec!["ice.db.l".to_string(), "ice.db.r".to_string()];
        mv_def.primary_key_columns = vec!["region".to_string()];
        mv_def.last_refresh_snapshots = [
            ("ice.db.l".to_string(), 11i64),
            ("ice.db.r".to_string(), 33i64),
        ]
        .into_iter()
        .collect();
        mv_def.last_refresh_table_uuids = [
            ("ice.db.l".to_string(), "uuid-l".to_string()),
            ("ice.db.r".to_string(), "uuid-r".to_string()),
        ]
        .into_iter()
        .collect();

        let mut contract = make_schema_contract();
        contract.base = join_base_contract("ice.db.l", "uuid-l");
        contract.bases = vec![
            join_base_contract("ice.db.l", "uuid-l"),
            join_base_contract("ice.db.r", "uuid-r"),
        ];
        contract.output.columns[0]
            .expression
            .referenced_base_field_ids
            .clear();
        contract.output.columns[0].expression.referenced_base_fields =
            vec![qualified_field("ice.db.l", "l", 1)];
        contract.output.columns[1]
            .expression
            .referenced_base_field_ids
            .clear();
        contract.output.columns[1].expression.referenced_base_fields =
            vec![qualified_field("ice.db.l", "l", 2)];
        contract.target.visible_columns[0].output_name = "region".to_string();
        contract.target.visible_columns[1].output_name = "s".to_string();
        contract.target.hidden_apply_key.column_name = "__row_id__".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.target.hidden_apply_key.source = ApplyKeySource::GroupRowId;
        contract.join = Some(JoinContract {
            kind: JoinContractKind::InnerEquiJoin,
            predicates: vec![JoinPredicateLineage {
                left: qualified_field("ice.db.l", "l", 1),
                right: qualified_field("ice.db.r", "r", 1),
            }],
        });
        contract.branch = Some(BranchUnionContract {
            branch_id_column: BranchIdColumnContract {
                column_name: crate::mv::persistence::schema::BRANCH_ID_COLUMN_NAME.to_string(),
                target_field_id: 998,
            },
            branch_count: 2,
            inner_apply_key_source: ApplyKeySource::GroupRowId,
        });
        contract.aggregate = Some(AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: vec![
                single_state_column("binary"),
                retraction_count_state_column(),
            ],
        });
        mv_def.schema_contract = Some(contract.clone());

        let target_schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        100,
                        "region",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        101,
                        "s",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        999,
                        "__row_id__",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::required(
                        998,
                        "__branch_id__",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        200,
                        "__agg_state_s",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                    Arc::new(NestedField::required(
                        201,
                        "__agg_state___ivm_row_count",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()
                .expect("build schema"),
        );
        let mv_ctx = Arc::new(
            IcebergMvRewriteContext::from_definition_parts(
                make_target(),
                42,
                Some("sess_cat".to_string()),
                "sess_db".to_string(),
                Arc::new(mv_def),
                Arc::new(parse_query(
                    "SELECT l.region, sum(l.amount) AS s FROM ice.db.l l JOIN ice.db.r r ON l.region = r.region GROUP BY l.region",
                )),
                Arc::from(vec![make_ref("ice", "db", "l"), make_ref("ice", "db", "r")]),
                Arc::new(make_pin(&[
                    ("ice.db.l", 22, "uuid-l"),
                    ("ice.db.r", 22, "uuid-r"),
                ])),
                Some(99),
                "uuid-tgt".to_string(),
                target_schema,
                Some(Arc::new(contract)),
            )
            .expect("join aggregate rewrite context must build"),
        );
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx,
            annotation: ImvPlanAnnotation::default(),
        });
        ctx
    }

    fn join_base_contract(table_fqn: &str, table_uuid: &str) -> BaseContract {
        BaseContract {
            table_fqn: table_fqn.to_string(),
            table_uuid: table_uuid.to_string(),
            alias_at_create: None,
            schema_id_at_create: 7,
            schema_at_create: BaseSchemaSnapshot {
                fields: vec![
                    BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "region".to_string(),
                        type_signature: "long".to_string(),
                        required: true,
                    },
                    BaseFieldRecord {
                        field_id: 2,
                        name_at_create: "amount".to_string(),
                        type_signature: "long".to_string(),
                        required: true,
                    },
                ],
            },
        }
    }

    fn qualified_field(table_fqn: &str, qualifier: &str, field_id: i32) -> QualifiedFieldLineage {
        QualifiedFieldLineage {
            table_fqn: table_fqn.to_string(),
            qualifier_at_create: qualifier.to_string(),
            field_id,
        }
    }

    fn root_delta(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
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
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_expr(1, "region")],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_expr(2, "amount")],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: ColumnId::new_for_test(3),
                }],
                output_columns: vec![output_column(1, "region"), output_column(3, "s")],
                already_pushed: false,
            }),
            vec![input],
            None,
        )
    }

    fn project_over_aggregate(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: col_expr(1, "region"),
                        output_name: "region".to_string(),
                        output_column_id: ColumnId::new_for_test(1),
                    },
                    ProjectItem {
                        expr: col_expr(3, "s"),
                        output_name: "total".to_string(),
                        output_column_id: ColumnId::new_for_test(30),
                    },
                ],
                output_qualifier: None,
            }),
            vec![aggregate_over(input)],
            None,
        )
    }

    fn scan(name: &str, first_id: u32) -> LogicalPlanNode {
        let columns = vec![column_def("region"), column_def("amount")];
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
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
                        binding:
                            crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                columns: vec![
                    output_column(first_id, "region"),
                    output_column(first_id + 1, "amount"),
                ],
                predicates: Vec::new(),
                required_columns: None,
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn project_over_filter(name: &str, first_id: u32) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: col_expr(first_id, "region"),
                        output_name: "region".to_string(),
                        output_column_id: ColumnId::new_for_test(first_id),
                    },
                    ProjectItem {
                        expr: col_expr(first_id + 1, "amount"),
                        output_name: "amount".to_string(),
                        output_column_id: ColumnId::new_for_test(first_id + 1),
                    },
                ],
                output_qualifier: None,
            }),
            vec![filter_over(scan(name, first_id), first_id, "region")],
            None,
        )
    }

    fn filter_over(input: LogicalPlanNode, column_id: u32, column: &str) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
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
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: name.eq_ignore_ascii_case("s"),
            is_internal: false,
        }
    }

    fn col_expr(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn join_of(left: LogicalPlanNode, right: LogicalPlanNode) -> LogicalPlanNode {
        join_of_on(left, right, 1, 10)
    }

    fn join_of_on(
        left: LogicalPlanNode,
        right: LogicalPlanNode,
        left_region_id: u32,
        right_region_id: u32,
    ) -> LogicalPlanNode {
        // An inner equi-join on caller-selected region column ids.
        let condition = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_expr(left_region_id, "region")),
                op: BinOp::Eq,
                right: Box::new(col_expr(right_region_id, "region")),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            vec![left, right],
            None,
        )
    }

    fn assert_rule_changed(ctx: &RewriteContext, rule_name: &str) {
        use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;

        assert!(
            ctx.trace().events().iter().any(|event| {
                matches!(event, RewriteTraceEvent::RuleChanged { rule, .. } if *rule == rule_name)
            }),
            "{rule_name} must change the plan, trace: {:?}",
            ctx.trace().events()
        );
    }

    #[test]
    fn pipeline_aggregate_over_filtered_join_composes() {
        use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
        use crate::sql::planner::imv_rewrite::pipeline::build_imv_pipeline;

        let mut ctx = build_two_base_join_ctx();
        let join = join_of(scan("l", 1), scan("r", 10));
        let filtered = filter_over(join, 1, "region");
        let plan = aggregate_over(filtered);

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        let out_expr = build_imv_pipeline()
            .rewrite(expr, &mut ctx)
            .expect("aggregate over filtered join must compose");
        let arena = ctx.scalar_arena();
        let out = crate::sql::planner::optimizer_bridge::logical::to_logical_plan(
            out_expr,
            &arena.borrow(),
        );

        assert!(
            !plan_contains_imv_marker(&out),
            "no IMV marker may survive: {out:?}"
        );
        assert_rule_changed(&ctx, "RewriteJoinDelta");
    }

    #[test]
    fn pipeline_aggregate_over_nested_join_uses_aggregate_change_stream() {
        use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
        use crate::sql::planner::imv_rewrite::pipeline::build_imv_pipeline;

        let mut ctx = build_two_base_join_ctx();
        let inner = join_of(scan("l", 1), scan("r", 10));
        let outer = join_of_on(inner, scan("r", 20), 1, 20);
        let plan = aggregate_over(outer);

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        let out_expr = build_imv_pipeline()
            .rewrite(expr, &mut ctx)
            .expect("nested join aggregate must use aggregate change stream");
        let arena = ctx.scalar_arena();
        let out = crate::sql::planner::optimizer_bridge::logical::to_logical_plan(
            out_expr,
            &arena.borrow(),
        );

        assert!(
            !plan_contains_imv_marker(&out),
            "no IMV marker may survive: {out:?}"
        );
        assert!(
            ctx.extension::<ImvExtension>()
                .expect("extension")
                .annotation
                .change_stream
                .has_aggregate()
        );
    }

    #[test]
    fn pipeline_branch_union_of_project_over_aggregate_composes() {
        use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
        use crate::sql::planner::imv_rewrite::pipeline::build_imv_pipeline;

        let mut ctx = build_ctx();
        // project_over_aggregate outputs: region (id=1) and total (id=30).
        // Both branches reference the registered base "ice.db.b" so scan binding succeeds.
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(30, "total")],
            }),
            vec![
                project_over_aggregate(scan("b", 1)),
                project_over_aggregate(scan("b", 10)),
            ],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        let out_expr = build_imv_pipeline()
            .rewrite(expr, &mut ctx)
            .expect("branch union of Project-over-Aggregate must compose");
        let arena = ctx.scalar_arena();
        let out = crate::sql::planner::optimizer_bridge::logical::to_logical_plan(
            out_expr,
            &arena.borrow(),
        );
        assert!(
            !plan_contains_imv_marker(&out),
            "no marker may survive: each Project-over-Aggregate branch must fully decompose"
        );
        let (union, branches) = top_branch_union(&out);
        assert_eq!(branches.len(), 2);
        assert!(
            union
                .output_columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case("__branch_id__")),
            "union output must expose __branch_id__"
        );
        assert_locator_columns_precede_action(
            &union
                .output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
        );
        for branch in branches {
            assert_aggregate_change_stream_branch(branch);
        }
    }

    #[test]
    fn pipeline_branch_union_of_aggregate_over_join_composes() {
        use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
        use crate::sql::planner::imv_rewrite::pipeline::build_imv_pipeline;

        let mut ctx = build_two_base_join_ctx();
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            }),
            vec![
                aggregate_over(join_of(scan("l", 1), scan("r", 10))),
                aggregate_over(join_of_on(scan("l", 20), scan("r", 30), 20, 30)),
            ],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        let out_expr = build_imv_pipeline()
            .rewrite(expr, &mut ctx)
            .expect("branch union of aggregate-over-join must compose");
        let arena = ctx.scalar_arena();
        let out = crate::sql::planner::optimizer_bridge::logical::to_logical_plan(
            out_expr,
            &arena.borrow(),
        );
        assert!(
            !plan_contains_imv_marker(&out),
            "no marker may survive: the inner joins must be delta-expanded and bound"
        );
    }
}
