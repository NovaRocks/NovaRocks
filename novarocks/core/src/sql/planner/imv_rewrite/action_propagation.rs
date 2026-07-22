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

//! IMV action column injection and propagation rules.
//!
//! Phase 2: Delta-bound scans get an internal `__change_op` Int8
//! non-nullable column. Project transparently carries **all** internal columns
//! (including `_row_id` added in Task 2, and any future internal column).
//! Filter is a schema-passthrough node and requires no work.
//! Unsupported Join/UnionAll/Aggregate shapes above a Delta scan fail fast;
//! recognized IMV delta algebra rewrites are accepted by shape-specific
//! predicates.

use crate::sql::analysis::{ExprKind, JoinKind, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::sql::planner::imv_rewrite::join_delta_shape::{
    is_supported_join_delta_branch, is_supported_join_delta_union,
};
use crate::sql::planner::imv_rewrite::row_id_column::ImvRowIdColumn;
use crate::sql::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::sql::planner::logical::{LogicalAggregateNode, LogicalPlanKind, LogicalPlanNode};
use crate::sql::planner::table::ScanSource;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns true iff the plan's effective output schema contains the IMV
/// action column. Used by `matches()` predicates and validation.
fn output_has_action_column(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => scan.columns.iter().any(ImvActionColumn::matches),
        LogicalPlanKind::Filter(_) => output_has_action_column(plan.unary_input()),
        LogicalPlanKind::Project(node) => {
            // NOTE: ProjectItem carries no `is_internal` flag (unlike
            // OutputColumn), so we can only detect the propagated action
            // column by its reserved name `__change_op`. Phase 2 assumes no
            // user-visible projection legitimately uses this name; the
            // analyzer does not yet reject it. Task 8's validation (V4)
            // backstops by rejecting internal columns leaking to visible
            // output. Revisit if MV definitions ever expose `__change_op`.
            node.items
                .iter()
                .any(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
        }
        LogicalPlanKind::ImvDelta(_) | LogicalPlanKind::ImvVersion(_) => {
            output_has_action_column(plan.unary_input())
        }
        LogicalPlanKind::Union(node) => node.output_columns.iter().any(ImvActionColumn::matches),
        _ => false,
    }
}

/// Returns the action column descriptor from the first descendant Scan/Project
/// in the subtree that exposes one, or `None` if no descendant carries it.
fn first_propagated_action_column(plan: &LogicalPlanNode) -> Option<OutputColumn> {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => scan
            .columns
            .iter()
            .find(|c| ImvActionColumn::matches(c))
            .cloned(),
        LogicalPlanKind::Filter(_) | LogicalPlanKind::Project(_) => {
            first_propagated_action_column(plan.unary_input())
        }
        LogicalPlanKind::Union(node) => node
            .output_columns
            .iter()
            .find(|c| ImvActionColumn::matches(c))
            .cloned(),
        _ => None,
    }
}

/// Whether any descendant of the plan exposes an action column.
fn subtree_has_action_column(plan: &LogicalPlanNode) -> bool {
    if is_change_stream_propagation_boundary(plan) {
        return false;
    }
    if matches!(&plan.kind, LogicalPlanKind::Aggregate(node) if is_signed_state_aggregate(node)) {
        return false;
    }
    output_has_action_column(plan) || plan.children.iter().any(subtree_has_action_column)
}

/// Returns the fully-qualified name of the first `IcebergDeltaTable`-backed
/// scan found anywhere in the subtree, for use in fail-fast diagnostics.
/// Recurses through every child-bearing variant (unlike the action-column
/// helpers, which only need Scan/Filter/Project), because an unsupported
/// Join/Union/Aggregate node's delta scan can sit under any branch.
pub(crate) fn first_delta_base_fqn(plan: &LogicalPlanNode) -> Option<String> {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => match &scan.table.source {
            ScanSource::IcebergDeltaTable { table, .. } => Some(format!(
                "{}.{}.{}",
                table.catalog, table.namespace, table.table
            )),
            _ => None,
        },
        _ => plan.children.iter().find_map(first_delta_base_fqn),
    }
}

/// Collect every internal (`is_internal`) output column exposed by the first
/// descendant Scan, threaded up through Filter/Project. Used by the generalized
/// propagation rule to carry `__change_op`, `_row_id`, and any future internal
/// column through the unary chain.
pub(crate) fn descendant_internal_columns(plan: &LogicalPlanNode) -> Vec<OutputColumn> {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => scan
            .columns
            .iter()
            .filter(|c| c.is_internal)
            .cloned()
            .collect(),
        LogicalPlanKind::Filter(_) | LogicalPlanKind::Project(_) => {
            descendant_internal_columns(plan.unary_input())
        }
        LogicalPlanKind::Union(node) => node
            .output_columns
            .iter()
            .filter(|c| c.is_internal)
            .cloned()
            .collect(),
        _ => Vec::new(),
    }
}

fn is_signed_state_aggregate(node: &LogicalAggregateNode) -> bool {
    !node.aggregates.is_empty()
        && node
            .aggregates
            .iter()
            .any(|call| call.name.ends_with("_state_signed"))
        && node.aggregates.iter().all(|call| {
            call.name.ends_with("_state_signed") || is_hidden_retraction_count_call(call)
        })
}

fn is_hidden_retraction_count_call(call: &crate::sql::planner::payload::AggregateCall) -> bool {
    call.name.eq_ignore_ascii_case("sum")
        && call.args.len() == 1
        && matches!(
            &call.args[0].kind,
            ExprKind::ColumnRef { column, .. } if column.eq_ignore_ascii_case(ImvActionColumn::NAME)
        )
}

fn is_change_stream_propagation_boundary(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        LogicalPlanKind::Union(_) => {
            (union_outputs_action_column(plan)
                && contains_target_state_scan(plan)
                && contains_signed_state_aggregate(plan))
                || is_cte_consumer_change_stream_boundary(plan)
        }
        LogicalPlanKind::Project(_) => is_relational_change_stream_propagation_boundary(plan),
        LogicalPlanKind::CTEAnchor(_) if plan.children.len() == 2 => {
            union_outputs_action_column(plan.child(1))
                && contains_target_state_scan(plan.child(0))
                && contains_signed_state_aggregate(plan.child(0))
        }
        _ => false,
    }
}

fn is_relational_change_stream_propagation_boundary(plan: &LogicalPlanNode) -> bool {
    let LogicalPlanKind::Project(project) = &plan.kind else {
        return false;
    };
    project
        .items
        .iter()
        .any(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
        && project_filter_contains_state_all_zero(plan)
        && contains_join_kind(plan, JoinKind::LeftOuter)
        && contains_supported_delta_join_kind(plan)
        && contains_branch_marker_values(plan)
        && contains_target_state_scan(plan)
        && contains_signed_state_aggregate(plan)
}

fn project_filter_contains_state_all_zero(plan: &LogicalPlanNode) -> bool {
    let LogicalPlanKind::Project(_) = &plan.kind else {
        return false;
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

fn contains_join_kind(plan: &LogicalPlanNode, join_type: JoinKind) -> bool {
    matches!(
        &plan.kind,
        LogicalPlanKind::Join(join) if join.join_type == join_type
    ) || plan
        .children
        .iter()
        .any(|child| contains_join_kind(child, join_type))
}

fn contains_supported_delta_join_kind(plan: &LogicalPlanNode) -> bool {
    contains_join_kind(plan, JoinKind::Inner) || contains_join_kind(plan, JoinKind::Cross)
}

fn contains_branch_marker_values(plan: &LogicalPlanNode) -> bool {
    matches!(&plan.kind, LogicalPlanKind::Values(values)
        if values.columns.iter().any(|column| {
            column.name.eq_ignore_ascii_case("__imv_change_branch")
        })
    ) || plan.children.iter().any(contains_branch_marker_values)
}

fn is_cte_consumer_change_stream_boundary(plan: &LogicalPlanNode) -> bool {
    matches!(&plan.kind, LogicalPlanKind::Union(_))
        && union_outputs_action_column(plan)
        && !plan.children.is_empty()
        && plan.children.iter().all(branch_consumes_cte)
}

fn branch_consumes_cte(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        LogicalPlanKind::Project(_) | LogicalPlanKind::Filter(_) => {
            branch_consumes_cte(plan.unary_input())
        }
        LogicalPlanKind::CTEConsume(_) => true,
        _ => false,
    }
}

fn union_outputs_action_column(plan: &LogicalPlanNode) -> bool {
    matches!(
        &plan.kind,
        LogicalPlanKind::Union(union)
            if union
                .output_columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(ImvActionColumn::NAME))
    )
}

fn contains_target_state_scan(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => {
            matches!(scan.table.source, ScanSource::IcebergMvTargetState(_))
        }
        _ => plan.children.iter().any(contains_target_state_scan),
    }
}

fn contains_signed_state_aggregate(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        LogicalPlanKind::Aggregate(node) => is_signed_state_aggregate(node),
        _ => plan.children.iter().any(contains_signed_state_aggregate),
    }
}

// ---------------------------------------------------------------------------
// InjectActionColumnRule
// ---------------------------------------------------------------------------

// Registered into the IMV rewrite pipeline's `imv-action-propagation` stage.
pub(crate) struct InjectActionColumnRule;

impl LogicalRewriteRule for InjectActionColumnRule {
    fn name(&self) -> &'static str {
        "InjectActionColumn"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        match &plan.kind {
            LogicalPlanKind::Scan(scan) => {
                matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. })
                    && !scan.columns.iter().any(ImvActionColumn::matches)
            }
            _ => false,
        }
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        bridge_apply_result(expr, ctx, |mut plan, ctx| {
            let LogicalPlanKind::Scan(scan) = &mut plan.kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            let column_id = crate::sql::planner::imv_rewrite::column_alloc::allocate_imv_column(
                ctx,
                ImvActionColumn::NAME,
                arrow::datatypes::DataType::Int8,
                false,
            )?;
            scan.columns
                .retain(|column| !is_action_column_name(&column.name));
            scan.columns.push(ImvActionColumn::output_column(column_id));
            ImvActionColumn::ensure_metadata_column(
                &mut scan.table.iceberg_row_lineage_metadata_columns,
            );
            Ok(PlanRewriteResult::Changed(plan))
        })
    }
}

// ---------------------------------------------------------------------------
// PropagateActionColumnRule
// ---------------------------------------------------------------------------

// Registered into the IMV rewrite pipeline's `imv-action-propagation` stage.
pub(crate) struct PropagateActionColumnRule;

impl LogicalRewriteRule for PropagateActionColumnRule {
    fn name(&self) -> &'static str {
        "PropagateActionColumn"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        match &plan.kind {
            LogicalPlanKind::Project(p) => {
                if project_over_union_needs_row_id_output(&plan) {
                    true
                } else {
                    let internal = descendant_internal_columns(plan.unary_input());
                    !internal.is_empty()
                        && internal.iter().any(|c| {
                            !p.items
                                .iter()
                                .any(|item| item.output_name.eq_ignore_ascii_case(&c.name))
                        })
                }
            }
            // Filter is a schema-passthrough node: it exposes its child's
            // schema verbatim, so once the child has the action column the
            // Filter's effective output also has it. No work needed.
            LogicalPlanKind::Filter(_) => false,
            // Unsupported Aggregate / Join / Union shapes above a delta subtree
            // match here so `apply` can fail-fast with a clear error.
            LogicalPlanKind::Aggregate(a) => {
                subtree_has_action_column(plan.unary_input()) && !is_signed_state_aggregate(a)
            }
            LogicalPlanKind::Join(_) => {
                (subtree_has_action_column(plan.left()) || subtree_has_action_column(plan.right()))
                    && !is_join_delta_fixpoint_candidate(&plan)
                    && !is_supported_join_delta_branch(&plan)
            }
            LogicalPlanKind::Union(_) => {
                if is_change_stream_propagation_boundary(&plan) {
                    false
                } else if branch_delta_union_needs_row_id_output(&plan) {
                    true
                } else if union_outputs_action_column(&plan) {
                    false
                } else {
                    plan.children.iter().any(subtree_has_action_column)
                        && !is_supported_join_delta_union(&plan)
                        && !is_supported_fan_in_delta_union(&plan)
                }
            }
            _ => false,
        }
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        bridge_apply_result(expr, ctx, |mut plan, ctx| {
            // Diagnostic: the delta base under an unsupported node, if any. Computed
            // up-front from `&plan` so the fail-fast arms can name the offending
            // base table; harmless for the Project happy path.
            let base = first_delta_base_fqn(&plan).unwrap_or_else(|| "<unknown>".to_string());
            if matches!(plan.kind, LogicalPlanKind::Project(_)) {
                let mut changed = promote_project_union_row_id_output(&mut plan, ctx)?;
                let internal = descendant_internal_columns(plan.unary_input());
                let LogicalPlanKind::Project(p) = &mut plan.kind else {
                    unreachable!()
                };
                for col in internal {
                    let already = p
                        .items
                        .iter()
                        .any(|item| item.output_name.eq_ignore_ascii_case(&col.name));
                    if already {
                        continue;
                    }
                    changed = true;
                    p.items.push(ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: col.column_id,
                                qualifier: None,
                                column: col.name.clone(),
                            },
                            data_type: col.data_type.clone(),
                            nullable: col.nullable,
                        },
                        output_name: col.name.clone(),
                        output_column_id: col.column_id,
                    });
                }
                return Ok(if changed {
                    PlanRewriteResult::Changed(plan)
                } else {
                    PlanRewriteResult::Unchanged
                });
            }

            if branch_delta_union_needs_row_id_output(&plan) {
                let row_id_column =
                    crate::sql::planner::imv_rewrite::column_alloc::allocate_imv_column(
                        ctx,
                        crate::sql::planner::imv_rewrite::row_id_column::ImvRowIdColumn::NAME,
                        arrow::datatypes::DataType::Int64,
                        false,
                    )?;
                for input in &mut plan.children {
                    normalize_branch_row_id_output(input, row_id_column)?;
                }
                let LogicalPlanKind::Union(u) = &mut plan.kind else {
                    unreachable!()
                };
                u.output_columns
                    .push(ImvRowIdColumn::output_column(row_id_column));
                return Ok(PlanRewriteResult::Changed(plan));
            }

            match &plan.kind {
                LogicalPlanKind::Aggregate(_) => Err(format!(
                    "IMV action column propagation does not support Aggregate above \
                     delta-bound scan {base} in Phase 2; aggregate state rewrite is \
                     scheduled for Phase 4"
                )),
                LogicalPlanKind::Join(_) => Err(format!(
                    "IMV action column propagation does not support Join above \
                     delta-bound scan {base} in Phase 2; join delta algebra is \
                     handled by the delta-pushdown fixpoint"
                )),
                LogicalPlanKind::Union(_) => Err(format!(
                    "IMV action column propagation does not support UNION above \
                     delta-bound scan {base} in Phase 2; union delta rewrite is \
                     scheduled for Phase 6"
                )),
                _ => Ok(PlanRewriteResult::Unchanged),
            }
        })
    }
}

fn is_join_delta_fixpoint_candidate(plan: &LogicalPlanNode) -> bool {
    let LogicalPlanKind::Join(join) = &plan.kind else {
        return false;
    };
    if !matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
        return false;
    }
    let left_has_action = subtree_has_action_column(plan.left());
    let right_has_action = subtree_has_action_column(plan.right());
    let left_has_version = subtree_has_version_scan(plan.left());
    let right_has_version = subtree_has_version_scan(plan.right());
    (left_has_action && right_has_version) || (right_has_action && left_has_version)
}

fn branch_delta_union_needs_row_id_output(plan: &LogicalPlanNode) -> bool {
    let LogicalPlanKind::Union(node) = &plan.kind else {
        return false;
    };
    node.all
        && !plan.children.is_empty()
        && node.output_columns.iter().any(ImvActionColumn::matches)
        && !node.output_columns.iter().any(ImvRowIdColumn::matches)
        && !is_supported_join_delta_union(plan)
        && plan.children.iter().all(|child| {
            branch_project_has_row_id(child) && branch_output_action_column_id(child).is_some()
        })
}

fn branch_project_has_row_id(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        LogicalPlanKind::Project(project) => project
            .items
            .iter()
            .any(|item| item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME)),
        _ => false,
    }
}

fn project_over_union_needs_row_id_output(plan: &LogicalPlanNode) -> bool {
    let LogicalPlanKind::Project(project) = &plan.kind else {
        return false;
    };
    let union_plan = plan.unary_input();
    let LogicalPlanKind::Union(_) = &union_plan.kind else {
        return false;
    };
    project
        .items
        .iter()
        .any(|item| item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
        && branch_delta_union_needs_row_id_output(union_plan)
}

fn promote_project_union_row_id_output(
    plan: &mut LogicalPlanNode,
    ctx: &RewriteContext,
) -> Result<bool, String> {
    if !project_over_union_needs_row_id_output(plan) {
        return Ok(false);
    }
    let row_id_column = crate::sql::planner::imv_rewrite::column_alloc::allocate_imv_column(
        ctx,
        crate::sql::planner::imv_rewrite::row_id_column::ImvRowIdColumn::NAME,
        arrow::datatypes::DataType::Int64,
        false,
    )?;
    let Some(union_plan) = plan.children.get_mut(0) else {
        return Ok(false);
    };
    for input in &mut union_plan.children {
        normalize_branch_row_id_output(input, row_id_column)?;
    }
    let LogicalPlanKind::Union(union) = &mut union_plan.kind else {
        return Ok(false);
    };
    union
        .output_columns
        .push(ImvRowIdColumn::output_column(row_id_column));

    let LogicalPlanKind::Project(project) = &mut plan.kind else {
        return Ok(false);
    };
    if let Some(item) = project
        .items
        .iter_mut()
        .find(|item| item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
    {
        if let ExprKind::ColumnRef {
            column_id, column, ..
        } = &mut item.expr.kind
        {
            *column_id = row_id_column;
            *column = ImvRowIdColumn::NAME.to_string();
        }
        item.expr.data_type = arrow::datatypes::DataType::Int64;
        item.expr.nullable = false;
        item.output_column_id = row_id_column;
        item.output_name = ImvRowIdColumn::NAME.to_string();
    }
    Ok(true)
}

fn normalize_branch_row_id_output(
    plan: &mut LogicalPlanNode,
    row_id_column: crate::sql::column_id::ColumnId,
) -> Result<(), String> {
    let LogicalPlanKind::Project(project) = &mut plan.kind else {
        return Err(
            "IMV branch UNION row-id propagation expected normalized Project branch".to_string(),
        );
    };
    let Some(item) = project
        .items
        .iter_mut()
        .find(|item| item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
    else {
        return Err("IMV branch UNION row-id propagation expected _row_id output".to_string());
    };
    item.output_column_id = row_id_column;
    item.output_name = ImvRowIdColumn::NAME.to_string();
    Ok(())
}

pub(crate) fn is_supported_fan_in_delta_union(plan: &LogicalPlanNode) -> bool {
    let LogicalPlanKind::Union(node) = &plan.kind else {
        return false;
    };
    if !node.all || plan.children.is_empty() {
        return false;
    }
    plan.children.iter().all(|branch| {
        subtree_has_delta_scan(branch)
            && !subtree_has_version_scan(branch)
            && branch_output_action_column_id(branch).is_some()
    })
}

fn branch_output_action_column_id(
    plan: &LogicalPlanNode,
) -> Option<crate::sql::column_id::ColumnId> {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => scan
            .columns
            .iter()
            .find(|column| ImvActionColumn::matches(column))
            .map(|column| column.column_id),
        LogicalPlanKind::Filter(_) => branch_output_action_column_id(plan.unary_input()),
        LogicalPlanKind::Project(project) => project
            .items
            .iter()
            .find(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
            .and_then(|item| {
                let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind else {
                    return None;
                };
                (*column_id == item.output_column_id
                    && item.expr.data_type == arrow::datatypes::DataType::Int8
                    && !item.expr.nullable)
                    .then_some(item.output_column_id)
            }),
        LogicalPlanKind::ImvDelta(_) | LogicalPlanKind::ImvVersion(_) => {
            branch_output_action_column_id(plan.unary_input())
        }
        _ => None,
    }
}

fn is_action_column_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(ImvActionColumn::NAME)
}

fn subtree_has_delta_scan(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => {
            matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. })
        }
        LogicalPlanKind::ImvDelta(_) => true,
        LogicalPlanKind::Filter(_)
        | LogicalPlanKind::Project(_)
        | LogicalPlanKind::Aggregate(_)
        | LogicalPlanKind::Join(_)
        | LogicalPlanKind::Union(_)
        | LogicalPlanKind::ImvVersion(_) => plan.children.iter().any(subtree_has_delta_scan),
        _ => false,
    }
}

fn subtree_has_version_scan(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => {
            matches!(scan.table.source, ScanSource::IcebergVersionTable { .. })
        }
        LogicalPlanKind::ImvVersion(_) => true,
        LogicalPlanKind::Filter(_)
        | LogicalPlanKind::Project(_)
        | LogicalPlanKind::Aggregate(_)
        | LogicalPlanKind::Join(_)
        | LogicalPlanKind::Union(_)
        | LogicalPlanKind::ImvDelta(_) => plan.children.iter().any(subtree_has_version_scan),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::logical::*;
    use crate::sql::planner::payload::*;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::mv::rewrite::context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::analysis::{JoinKind, LiteralValue};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::planner::logical::{
        LogicalAggregateNode, LogicalJoinNode, LogicalPlanKind, LogicalPlanNode, LogicalUnionNode,
    };
    use crate::sql::planner::optimizer_bridge::logical::{to_logical_plan, to_optimizer_expr};
    use crate::sql::planner::payload::{PlanFilterNode, PlanScanNode};

    fn build_ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::new());
        let factory = Rc::new(RefCell::new(crate::sql::column_id::ColumnRefFactory::new()));
        factory.borrow_mut().reserve_until(100);
        ctx.set_column_ref_factory(Rc::clone(&factory));
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
        });
        ctx
    }

    fn delta_scan() -> PlanScanNode {
        PlanScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDeltaTable {
                    table: IcebergTableInfo {
                        catalog: "ice".to_string(),
                        namespace: "db".to_string(),
                        table: "b".to_string(),
                        table_uuid: Some("uuid-b".to_string()),
                        current_snapshot_id: Some(22),
                        schema_id: 7,
                        location: "file:///tmp/ice/db/b".to_string(),
                        schema: IcebergSchemaDef { fields: Vec::new() },
                        serialized_metadata: None,
                        serialized_metadata_rows: None,
                    },
                    from_snapshot_id: 11,
                    to_snapshot_id: 22,
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId(1),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            predicates: Vec::new(),
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }
    }

    fn version_scan() -> PlanScanNode {
        let mut s = delta_scan();
        s.table.source = ScanSource::IcebergVersionTable {
            table: match &delta_scan().table.source {
                ScanSource::IcebergDeltaTable { table, .. } => table.clone(),
                _ => unreachable!(),
            },
            snapshot_id: 22,
        };
        s
    }

    fn starrocks_scan() -> PlanScanNode {
        let mut s = delta_scan();
        s.table.source = ScanSource::StarRocks {
            db_id: 0,
            table_id: 0,
        };
        s
    }

    fn scan_plan(scan: PlanScanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(LogicalPlanKind::Scan(scan), vec![], None)
    }

    #[test]
    fn inject_action_column_on_delta_scan() {
        let rule = InjectActionColumnRule;
        let mut ctx = build_ctx();
        let plan = scan_plan(delta_scan());
        let mut arena = ScalarArena::new();
        let expr = to_optimizer_expr(&plan, &mut arena);
        assert!(rule.matches(&expr, &ctx));
        let result = rule.apply(expr, &mut ctx).expect("apply must succeed");
        let RewriteResult::Changed(changed_expr) = result else {
            panic!("expected Changed(Scan), got {:?}", result);
        };
        let changed = to_logical_plan(changed_expr, &arena);
        let LogicalPlanKind::Scan(scan) = changed.kind else {
            panic!("expected Changed(Scan)");
        };
        let action = scan
            .columns
            .iter()
            .find(|c| ImvActionColumn::matches(c))
            .expect("action column must be present");
        assert_eq!(action.data_type, DataType::Int8);
        assert!(!action.nullable);
        assert!(action.is_internal);
        assert_eq!(action.column_id, ColumnId(100));
        assert!(
            scan.table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(
                    |column| column.name.eq_ignore_ascii_case(ImvActionColumn::NAME)
                        && column.data_type == DataType::Int8
                        && !column.nullable
                ),
            "delta scan table metadata must expose the action pseudo-column for codegen"
        );
    }

    #[test]
    fn inject_replaces_preexisting_non_internal_action_column() {
        let rule = InjectActionColumnRule;
        let mut ctx = build_ctx();
        let mut scan = delta_scan();
        scan.columns.push(output_column(
            9,
            ImvActionColumn::NAME,
            DataType::Int8,
            false,
            false,
        ));
        let plan = scan_plan(scan);

        let mut arena = ScalarArena::new();
        let expr = to_optimizer_expr(&plan, &mut arena);
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Scan)");
        };
        let changed = to_logical_plan(changed_expr, &arena);
        let LogicalPlanKind::Scan(scan) = changed.kind else {
            panic!("expected Changed(Scan)");
        };
        let action_columns = scan
            .columns
            .iter()
            .filter(|column| column.name.eq_ignore_ascii_case(ImvActionColumn::NAME))
            .collect::<Vec<_>>();
        assert_eq!(action_columns.len(), 1);
        assert_eq!(action_columns[0].column_id, ColumnId(100));
        assert!(action_columns[0].is_internal);
    }

    #[test]
    fn inject_does_not_touch_version_scan() {
        let rule = InjectActionColumnRule;
        let ctx = build_ctx();
        let plan = scan_plan(version_scan());
        let mut arena = ScalarArena::new();
        let expr = to_optimizer_expr(&plan, &mut arena);
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn inject_is_idempotent() {
        let rule = InjectActionColumnRule;
        let ctx = build_ctx();
        let mut scan = delta_scan();
        scan.columns
            .push(ImvActionColumn::output_column(ColumnId(9)));
        let plan = scan_plan(scan);
        let mut arena = ScalarArena::new();
        let expr = to_optimizer_expr(&plan, &mut arena);
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn inject_skips_starrocks_scan() {
        let rule = InjectActionColumnRule;
        let ctx = build_ctx();
        let plan = scan_plan(starrocks_scan());
        let mut arena = ScalarArena::new();
        let expr = to_optimizer_expr(&plan, &mut arena);
        assert!(!rule.matches(&expr, &ctx));
    }

    use crate::sql::planner::payload::PlanProjectNode;

    fn project_over(input: LogicalPlanNode, projected_user_col_id: ColumnId) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: projected_user_col_id,
                            qualifier: None,
                            column: "k".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: "k".to_string(),
                    output_column_id: projected_user_col_id,
                }],
                output_qualifier: None,
            }),
            vec![input],
            None,
        )
    }

    fn delta_scan_with_action(action_id: ColumnId) -> PlanScanNode {
        let mut s = delta_scan();
        s.columns.push(ImvActionColumn::output_column(action_id));
        s
    }

    fn normalized_delta_project(action_id: ColumnId, user_col_id: ColumnId) -> LogicalPlanNode {
        let mut scan = delta_scan_with_action(action_id);
        scan.columns[0].column_id = user_col_id;
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: user_col_id,
                                qualifier: None,
                                column: "k".to_string(),
                            },
                            data_type: DataType::Int64,
                            nullable: false,
                        },
                        output_name: "k".to_string(),
                        output_column_id: user_col_id,
                    },
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: action_id,
                                qualifier: None,
                                column: ImvActionColumn::NAME.to_string(),
                            },
                            data_type: DataType::Int8,
                            nullable: false,
                        },
                        output_name: ImvActionColumn::NAME.to_string(),
                        output_column_id: action_id,
                    },
                ],
                output_qualifier: None,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                    is_root: false,
                    action_column: Some(action_id),
                    branch_scope: None,
                }),
                vec![scan_plan(scan)],
                None,
            )],
            None,
        )
    }

    fn normalized_delta_project_with_row_id(
        action_id: ColumnId,
        user_col_id: ColumnId,
        row_id: ColumnId,
    ) -> LogicalPlanNode {
        let mut plan = normalized_delta_project(action_id, user_col_id);
        if let LogicalPlanKind::Project(project) = &mut plan.kind {
            project.items.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: row_id,
                        qualifier: None,
                        column: ImvRowIdColumn::NAME.to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: ImvRowIdColumn::NAME.to_string(),
                output_column_id: row_id,
            });
        }
        plan
    }

    fn normalized_delta_project_without_action(user_col_id: ColumnId) -> LogicalPlanNode {
        let mut scan = delta_scan();
        scan.columns[0].column_id = user_col_id;
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: user_col_id,
                            qualifier: None,
                            column: "k".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: "k".to_string(),
                    output_column_id: user_col_id,
                }],
                output_qualifier: None,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                    is_root: false,
                    action_column: None,
                    branch_scope: None,
                }),
                vec![scan_plan(scan)],
                None,
            )],
            None,
        )
    }

    fn normalized_version_project(user_col_id: ColumnId) -> LogicalPlanNode {
        let mut scan = version_scan();
        scan.columns[0].column_id = user_col_id;
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: user_col_id,
                            qualifier: None,
                            column: "k".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: "k".to_string(),
                    output_column_id: user_col_id,
                }],
                output_qualifier: None,
            }),
            vec![scan_plan(scan)],
            None,
        )
    }

    fn join_plan(left: LogicalPlanNode, right: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![left, right],
            None,
        )
    }

    fn normalized_join_delta_branch(
        left: LogicalPlanNode,
        right: LogicalPlanNode,
        action_id: ColumnId,
        user_col_id: ColumnId,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: user_col_id,
                                qualifier: None,
                                column: "k".to_string(),
                            },
                            data_type: DataType::Int64,
                            nullable: false,
                        },
                        output_name: "k".to_string(),
                        output_column_id: user_col_id,
                    },
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: action_id,
                                qualifier: None,
                                column: ImvActionColumn::NAME.to_string(),
                            },
                            data_type: DataType::Int8,
                            nullable: false,
                        },
                        output_name: ImvActionColumn::NAME.to_string(),
                        output_column_id: action_id,
                    },
                ],
                output_qualifier: None,
            }),
            vec![join_plan(left, right)],
            None,
        )
    }

    fn recursive_join_delta_union(action_id: ColumnId) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![
                    OutputColumn {
                        column_id: ColumnId(1),
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    ImvActionColumn::output_column(action_id),
                ],
            }),
            vec![
                normalized_join_delta_branch(
                    normalized_delta_project(action_id, ColumnId(1)),
                    normalized_version_project(ColumnId(10)),
                    action_id,
                    ColumnId(1),
                ),
                normalized_join_delta_branch(
                    normalized_version_project(ColumnId(1)),
                    normalized_delta_project(action_id, ColumnId(10)),
                    action_id,
                    ColumnId(1),
                ),
            ],
            None,
        )
    }

    fn malformed_delta_project_with_action_name(
        action_id: ColumnId,
        user_col_id: ColumnId,
    ) -> LogicalPlanNode {
        let mut scan = delta_scan_with_action(action_id);
        scan.columns[0].column_id = user_col_id;
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: user_col_id,
                                qualifier: None,
                                column: "k".to_string(),
                            },
                            data_type: DataType::Int64,
                            nullable: false,
                        },
                        output_name: "k".to_string(),
                        output_column_id: user_col_id,
                    },
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: user_col_id,
                                qualifier: None,
                                column: "k".to_string(),
                            },
                            data_type: DataType::Int64,
                            nullable: false,
                        },
                        output_name: ImvActionColumn::NAME.to_string(),
                        output_column_id: action_id,
                    },
                ],
                output_qualifier: None,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                    is_root: false,
                    action_column: Some(action_id),
                    branch_scope: None,
                }),
                vec![scan_plan(scan)],
                None,
            )],
            None,
        )
    }

    fn output_column(
        column_id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
        is_internal: bool,
    ) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(column_id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal,
        }
    }

    #[test]
    fn propagate_through_project() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let scan = scan_plan(delta_scan_with_action(ColumnId(100)));
        let plan = project_over(scan, ColumnId(1));
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let result = rule.apply(expr, &mut ctx).expect("apply must succeed");
        let RewriteResult::Changed(changed_expr) = result else {
            panic!("expected Changed(Project)");
        };
        let changed = to_logical_plan(changed_expr, &arena_rc.borrow());
        let LogicalPlanKind::Project(project) = changed.kind else {
            panic!("expected Changed(Project)");
        };
        assert_eq!(project.items.len(), 2);
        let last = &project.items[1];
        assert_eq!(last.output_name, "__change_op");
        match &last.expr.kind {
            ExprKind::ColumnRef { column_id, .. } => assert_eq!(*column_id, ColumnId(100)),
            other => panic!("expected ColumnRef, got {:?}", other),
        }
        assert_eq!(last.expr.data_type, DataType::Int8);
        assert!(!last.expr.nullable);
    }

    #[test]
    fn propagate_is_idempotent_on_project_with_action() {
        let rule = PropagateActionColumnRule;
        let ctx = build_ctx();
        let scan = scan_plan(delta_scan_with_action(ColumnId(100)));
        let mut plan = project_over(scan, ColumnId(1));
        if let LogicalPlanKind::Project(p) = &mut plan.kind {
            p.items.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(100),
                        qualifier: None,
                        column: "__change_op".to_string(),
                    },
                    data_type: DataType::Int8,
                    nullable: false,
                },
                output_name: "__change_op".to_string(),
                output_column_id: ColumnId(100),
            });
        }
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn propagate_skips_bare_scan() {
        // A bare Scan is not a Project; the rule should not match.
        let rule = PropagateActionColumnRule;
        let ctx = build_ctx();
        let plan = scan_plan(delta_scan_with_action(ColumnId(100)));
        let mut arena = ScalarArena::new();
        let expr = to_optimizer_expr(&plan, &mut arena);
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn propagate_rejects_aggregate() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let scan = scan_plan(delta_scan_with_action(ColumnId(100)));
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: Vec::new(),
                aggregates: Vec::new(),
                output_columns: Vec::new(),
                already_pushed: false,
            }),
            vec![scan],
            None,
        );
        let mut arena = ScalarArena::new();
        let expr = to_optimizer_expr(&plan, &mut arena);
        assert!(rule.matches(&expr, &ctx));
        let err = rule.apply(expr, &mut ctx).expect_err("Aggregate must fail");
        assert!(err.contains("Phase 4"), "unexpected error: {err}");
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
    }

    #[test]
    fn propagate_rejects_join() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let left = scan_plan(delta_scan_with_action(ColumnId(100)));
        let right = scan_plan(delta_scan());
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![left, right],
            None,
        );
        let mut arena = ScalarArena::new();
        let expr = to_optimizer_expr(&plan, &mut arena);
        assert!(rule.matches(&expr, &ctx));
        let err = rule.apply(expr, &mut ctx).expect_err("Join must fail");
        assert!(
            err.contains("delta-pushdown fixpoint"),
            "unexpected error: {err}"
        );
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
    }

    #[test]
    fn propagate_rejects_union() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: Vec::new(),
            }),
            vec![
                scan_plan(delta_scan_with_action(ColumnId(100))),
                scan_plan(starrocks_scan()),
            ],
            None,
        );
        let mut arena = ScalarArena::new();
        let expr = to_optimizer_expr(&plan, &mut arena);
        assert!(rule.matches(&expr, &ctx));
        let err = rule.apply(expr, &mut ctx).expect_err("Union must fail");
        assert!(err.contains("Phase 6"), "unexpected error: {err}");
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
    }

    #[test]
    fn accepts_recursive_join_delta_union_as_delta_like_join_side() {
        let rule = PropagateActionColumnRule;
        let ctx = build_ctx();
        let action_id = ColumnId(100);
        let nested_delta_side = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Bool(true)),
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![recursive_join_delta_union(action_id)],
            None,
        );
        let version_side = join_plan(
            normalized_version_project(ColumnId(20)),
            normalized_version_project(ColumnId(30)),
        );
        let plan = join_plan(nested_delta_side, version_side);

        let LogicalPlanKind::Join(_) = &plan.kind else {
            panic!("expected Join");
        };
        assert!(
            is_supported_join_delta_branch(&plan),
            "recursive join-delta union should classify as the delta-like side"
        );
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(
            !rule.matches(&expr, &ctx),
            "supported recursive join-delta branch must not be rejected"
        );
    }

    #[test]
    fn accepts_fan_in_delta_union_above_delta_scans() {
        let rule = PropagateActionColumnRule;
        let ctx = build_ctx();
        let action_id = ColumnId(100);
        let union = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![
                    OutputColumn {
                        column_id: ColumnId(1),
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    ImvActionColumn::output_column(action_id),
                ],
            }),
            vec![
                normalized_delta_project(action_id, ColumnId(1)),
                normalized_delta_project(action_id, ColumnId(10)),
            ],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&union, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn accepts_bare_delta_scan_union_with_shared_action_column() {
        let rule = PropagateActionColumnRule;
        let ctx = build_ctx();
        let action_id = ColumnId(100);
        let union = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: Vec::new(),
            }),
            vec![
                scan_plan(delta_scan_with_action(action_id)),
                scan_plan(delta_scan_with_action(action_id)),
            ],
            None,
        );

        let mut arena = ScalarArena::new();
        let expr = to_optimizer_expr(&union, &mut arena);
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn rejects_fan_in_delta_union_missing_branch_action_column() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let union = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: Vec::new(),
            }),
            vec![
                normalized_delta_project(ColumnId(100), ColumnId(1)),
                normalized_delta_project_without_action(ColumnId(10)),
            ],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&union, &mut arena_rc.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let err = rule.apply(expr, &mut ctx).expect_err("Union must fail");
        assert!(err.contains("Phase 6"), "unexpected error: {err}");
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
    }

    #[test]
    fn accepts_fan_in_delta_union_with_mismatched_branch_action_column_ids() {
        let rule = PropagateActionColumnRule;
        let ctx = build_ctx();
        let union = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: Vec::new(),
            }),
            vec![
                normalized_delta_project(ColumnId(100), ColumnId(1)),
                normalized_delta_project(ColumnId(101), ColumnId(10)),
            ],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&union, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn promotes_row_id_output_for_fan_in_delta_union() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let action_id = ColumnId(20);
        let union = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![
                    OutputColumn {
                        column_id: ColumnId(1),
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    ImvActionColumn::output_column(action_id),
                ],
            }),
            vec![
                normalized_delta_project_with_row_id(action_id, ColumnId(1), ColumnId(101)),
                normalized_delta_project_with_row_id(action_id, ColumnId(10), ColumnId(201)),
            ],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&union, &mut arena_rc.borrow_mut());
        drop(arena_rc);
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Union)");
        };

        let arena_rc = ctx.scalar_arena();
        let changed = to_logical_plan(changed_expr, &arena_rc.borrow());
        let LogicalPlanKind::Union(union) = &changed.kind else {
            panic!("expected Changed(Union)");
        };
        let row_id = union
            .output_columns
            .iter()
            .find(|column| ImvRowIdColumn::matches(column))
            .expect("union should expose _row_id");
        assert_eq!(row_id.column_id, ColumnId(100));
        for child in &changed.children {
            let LogicalPlanKind::Project(project) = &child.kind else {
                panic!("expected normalized Project branch");
            };
            let item = project
                .items
                .iter()
                .find(|item| item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
                .expect("branch should expose _row_id");
            assert_eq!(item.output_column_id, row_id.column_id);
        }
    }

    #[test]
    fn promotes_row_id_output_for_project_over_fan_in_delta_union() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let action_id = ColumnId(20);
        let union = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![
                    OutputColumn {
                        column_id: ColumnId(1),
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    ImvActionColumn::output_column(action_id),
                ],
            }),
            vec![
                normalized_delta_project_with_row_id(action_id, ColumnId(1), ColumnId(101)),
                normalized_delta_project_with_row_id(action_id, ColumnId(10), ColumnId(201)),
            ],
            None,
        );
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: ColumnId(1),
                                qualifier: None,
                                column: "k".to_string(),
                            },
                            data_type: DataType::Int64,
                            nullable: false,
                        },
                        output_name: "k".to_string(),
                        output_column_id: ColumnId(1),
                    },
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: ColumnId(101),
                                qualifier: None,
                                column: ImvRowIdColumn::NAME.to_string(),
                            },
                            data_type: DataType::Int64,
                            nullable: false,
                        },
                        output_name: ImvRowIdColumn::NAME.to_string(),
                        output_column_id: ColumnId(101),
                    },
                ],
                output_qualifier: None,
            }),
            vec![union],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        drop(arena_rc);
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Project)");
        };

        let arena_rc = ctx.scalar_arena();
        let changed = to_logical_plan(changed_expr, &arena_rc.borrow());
        let LogicalPlanKind::Project(project) = &changed.kind else {
            panic!("expected Changed(Project)");
        };
        let project_row_id = project
            .items
            .iter()
            .find(|item| item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
            .expect("project should expose _row_id");
        let union_plan = changed.unary_input();
        let LogicalPlanKind::Union(union) = &union_plan.kind else {
            panic!("expected child Union");
        };
        let union_row_id = union
            .output_columns
            .iter()
            .find(|column| ImvRowIdColumn::matches(column))
            .expect("union should expose _row_id");
        assert_eq!(project_row_id.output_column_id, union_row_id.column_id);
        for child in &union_plan.children {
            let LogicalPlanKind::Project(project) = &child.kind else {
                panic!("expected normalized Project branch");
            };
            let item = project
                .items
                .iter()
                .find(|item| item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
                .expect("branch should expose _row_id");
            assert_eq!(item.output_column_id, union_row_id.column_id);
        }
    }

    #[test]
    fn rejects_fan_in_delta_union_with_malformed_project_action_item() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let union = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: Vec::new(),
            }),
            vec![
                normalized_delta_project(ColumnId(100), ColumnId(1)),
                malformed_delta_project_with_action_name(ColumnId(100), ColumnId(10)),
            ],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&union, &mut arena_rc.borrow_mut());
        drop(arena_rc);
        assert!(rule.matches(&expr, &ctx));
        let err = rule.apply(expr, &mut ctx).expect_err("Union must fail");
        assert!(err.contains("Phase 6"), "unexpected error: {err}");
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
    }

    #[test]
    fn propagate_carries_all_internal_columns_through_project() {
        use crate::sql::planner::imv_rewrite::row_id_column::ImvRowIdColumn;

        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let mut scan = delta_scan_with_action(ColumnId(100));
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(101)));
        let plan = project_over(scan_plan(scan), ColumnId(1));
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        drop(arena_rc);
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Project)");
        };
        let arena_rc = ctx.scalar_arena();
        let changed = to_logical_plan(changed_expr, &arena_rc.borrow());
        let LogicalPlanKind::Project(project) = changed.kind else {
            panic!("expected Changed(Project)");
        };
        // k + __change_op + _row_id
        assert_eq!(project.items.len(), 3);
        assert!(project.items.iter().any(|i| i.output_name == "__change_op"));
        assert!(project.items.iter().any(|i| i.output_name == "_row_id"));
    }

    #[test]
    fn propagate_through_project_over_filter_over_scan() {
        // Filter is schema-passthrough: it should NOT match, but the action
        // column injected on the Scan must remain findable through the Filter
        // so the Project above can propagate it.
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let scan = scan_plan(delta_scan_with_action(ColumnId(100)));
        let filter = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Bool(true)),
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![scan],
            None,
        );
        let arena_rc = ctx.scalar_arena();
        // Filter itself must not match (schema-passthrough, no work).
        let filter_expr = to_optimizer_expr(&filter, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&filter_expr, &ctx));
        // first_propagated_action_column traverses the Filter to the Scan.
        assert!(first_propagated_action_column(&filter).is_some());
        // Project over the Filter propagates the action column.
        let project = project_over(filter, ColumnId(1));
        let project_expr = to_optimizer_expr(&project, &mut arena_rc.borrow_mut());
        drop(arena_rc);
        assert!(rule.matches(&project_expr, &ctx));
        let result = rule
            .apply(project_expr, &mut ctx)
            .expect("apply must succeed");
        let RewriteResult::Changed(changed_expr) = result else {
            panic!("expected Changed(Project)");
        };
        let arena_rc = ctx.scalar_arena();
        let changed = to_logical_plan(changed_expr, &arena_rc.borrow());
        let LogicalPlanKind::Project(p) = changed.kind else {
            panic!("expected Changed(Project)");
        };
        assert!(p.items.iter().any(|i| i.output_name == "__change_op"));
    }
}
