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

use crate::compiler::SqlCompileError;
use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::analysis::{ExprKind, JoinKind, OutputColumn, ProjectItem, TypedExpr};
use crate::column_id::ColumnId;
use crate::common::ImvVersionRef;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::{RewriteDiagnostic, RewriteResult};
use crate::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::planner::imv_rewrite::annotation::ImvExtension;
use crate::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor;
use crate::planner::imv_rewrite::column_alloc::allocate_imv_column;
use crate::planner::imv_rewrite::row_id_column::ImvRowIdColumn;
use crate::planner::imv_rewrite::{
    PlanRewriteResult, bridge_apply_result_mut_typed, opt_expr_to_plan,
};
use crate::planner::logical::{
    LogicalImvDeltaNode, LogicalImvVersionNode, LogicalJoinNode, LogicalPlanKind, LogicalPlanNode,
    LogicalUnionNode,
};
use crate::planner::payload::PlanProjectNode;

pub(crate) struct RewriteJoinDeltaRule;

pub(crate) fn join_delta_kind_supported(kind: crate::analysis::JoinKind) -> bool {
    matches!(
        kind,
        crate::analysis::JoinKind::Inner | crate::analysis::JoinKind::Cross
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
            LogicalPlanKind::ImvDelta(_) if matches!(&plan.unary_input().kind, LogicalPlanKind::Join(_))
        )
    }

    fn apply(
        &self,
        expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, SqlCompileError> {
        bridge_apply_result_mut_typed(expr, ctx, |plan, ctx| {
            let LogicalPlanNode {
                kind, mut children, ..
            } = plan;
            let LogicalPlanKind::ImvDelta(delta) = kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            let input = take_unary_child(&mut children);
            let LogicalPlanNode {
                kind: join_kind,
                children: mut join_children,
                required_output_columns,
            } = input;
            let LogicalPlanKind::Join(join) = join_kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };

            if !join_delta_kind_supported(join.join_type) {
                return Err(format!(
                    "Iceberg IMV join delta rewrite supports inner/cross joins only, got {:?}",
                    join.join_type
                )
                .into());
            }

            let action_column = match delta.action_column {
                Some(action_column) => action_column,
                None => allocate_imv_column(
                    ctx,
                    ImvActionColumn::NAME,
                    novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
                )?,
            };

            let (left, right) = take_binary_children(&mut join_children);
            let LogicalJoinNode {
                naa_author,
                join_type,
                condition,
            } = join;
            let mut output_columns = join_delta_payload_output_columns(join_type, &left, &right)?;
            if !output_columns
                .iter()
                .any(|column| column.column_id == action_column)
            {
                output_columns.push(ImvActionColumn::output_column(action_column));
            }

            // Both branches retain the definition's selected predicate, including
            // IEEE equality. Key lineage identifies fields for refresh metadata.
            let left_delta_branch = normalize_branch_output(
                LogicalPlanNode::new(
                    LogicalPlanKind::Join(LogicalJoinNode {
                        naa_author: naa_author.clone(),
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
            )?;

            let right_delta_branch = normalize_branch_output(
                LogicalPlanNode::new(
                    LogicalPlanKind::Join(LogicalJoinNode {
                        naa_author,
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
            )?;

            Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                LogicalPlanKind::Union(LogicalUnionNode {
                    all: true,
                    output_columns,
                }),
                vec![left_delta_branch, right_delta_branch],
                required_output_columns,
            )))
        })
    }
}

fn column_ref_expr(column: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: column.column_id,
            qualifier: None,
            column: column.name.clone(),
        },
        value_type: column.value_type.clone(),
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
    let mut out = Vec::new();
    let mut action_output: Option<OutputColumn> = None;
    for column in left_cols.into_iter().chain(right_cols) {
        if ImvActionColumn::matches(&column) {
            match &action_output {
                Some(existing) if existing.column_id != column.column_id => {
                    return Err(format!(
                        "Iceberg IMV join delta rewrite found multiple action columns in join inputs: {:?} and {:?}",
                        existing.column_id, column.column_id
                    ));
                }
                Some(_) => {}
                None => action_output = Some(column),
            }
        } else {
            out.push(column);
        }
    }
    if let Some(action_output) = action_output {
        out.push(action_output);
    }
    Ok(out)
}

fn join_delta_payload_output_columns(
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
    Ok(plan_output_columns(left)?
        .into_iter()
        .chain(plan_output_columns(right)?)
        .filter(|column| {
            !column.name.eq_ignore_ascii_case(ImvActionColumn::NAME)
                && !is_iceberg_row_identity_metadata_output(column)
        })
        .collect())
}

fn is_iceberg_row_identity_metadata_output(column: &OutputColumn) -> bool {
    column
        .name
        .eq_ignore_ascii_case(crate::common::ICEBERG_FILE_PATH_COL)
        || column
            .name
            .eq_ignore_ascii_case(crate::common::ICEBERG_ROW_POS_COL)
        || column
            .name
            .eq_ignore_ascii_case(crate::common::ICEBERG_ROW_ID_COL)
        || column
            .name
            .eq_ignore_ascii_case(crate::common::ICEBERG_LAST_UPDATED_SEQ_COL)
        || super::position_metadata::is_target_file_metadata(&column.name)
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
        LogicalPlanKind::Scan(_) => wrap_scan_marker(
            LogicalPlanNode::new(kind, children, required_output_columns),
            marker,
        ),
        LogicalPlanKind::Project(mut project) => {
            project
                .items
                .retain(|item| !item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME));
            if let MarkerKind::Delta(action_column) = &marker {
                let action_output = ImvActionColumn::output_column(*action_column);
                if !project
                    .items
                    .iter()
                    .any(|item| item.output_column_id == action_output.column_id)
                {
                    project.items.push(action_project_item(&action_output));
                }
            }
            let input = take_unary_child(&mut children);
            LogicalPlanNode::new(
                LogicalPlanKind::Project(project),
                vec![mark_scan(input, marker)?],
                required_output_columns,
            )
        }
        LogicalPlanKind::Filter(_) => {
            let input = take_unary_child(&mut children);
            LogicalPlanNode::new(
                kind,
                vec![mark_scan(input, marker)?],
                required_output_columns,
            )
        }
        LogicalPlanKind::Join(join) => match marker {
            MarkerKind::Delta(action_column) => wrap_scan_marker(
                LogicalPlanNode::new(
                    LogicalPlanKind::Join(join),
                    children,
                    required_output_columns,
                ),
                MarkerKind::Delta(action_column),
            ),
            MarkerKind::Version(version_ref) => {
                let (left, right) = take_binary_children(&mut children);
                LogicalPlanNode::new(
                    LogicalPlanKind::Join(LogicalJoinNode {
                        naa_author: join.naa_author,
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

fn action_project_item(action_output: &OutputColumn) -> ProjectItem {
    ProjectItem {
        expr: TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: action_output.column_id,
                qualifier: None,
                column: action_output.name.clone(),
            },
            value_type: action_output.value_type.clone(),
        },
        output_name: action_output.name.clone(),
        output_column_id: action_output.column_id,
    }
}

fn wrap_scan_marker(scan: LogicalPlanNode, marker: MarkerKind) -> LogicalPlanNode {
    match marker {
        MarkerKind::Delta(action_column) => LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(action_column),
                branch_scope: None,
            }),
            vec![scan],
            None,
        ),
        MarkerKind::Version(version_ref) => LogicalPlanNode::new(
            LogicalPlanKind::ImvVersion(LogicalImvVersionNode { version_ref }),
            vec![scan],
            None,
        ),
    }
}

fn plan_kind_from_kind(kind: &LogicalPlanKind) -> &'static str {
    kind.variant_name()
}

pub(crate) fn normalize_branch_output(
    input: LogicalPlanNode,
    output_columns: &[OutputColumn],
) -> Result<LogicalPlanNode, String> {
    let input_columns = plan_output_columns(&input)?;
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            output_qualifier: None,
            items: normalize_branch_project_items(&input_columns, output_columns)?,
        }),
        vec![input],
        None,
    ))
}

fn normalize_branch_project_items(
    input_columns: &[OutputColumn],
    output_columns: &[OutputColumn],
) -> Result<Vec<ProjectItem>, String> {
    if let Some(items) = normalize_branch_project_items_by_id(input_columns, output_columns) {
        return Ok(items);
    }

    let comparable_inputs = comparable_branch_inputs(input_columns, output_columns);
    if comparable_inputs.len() != output_columns.len() {
        return Err(format!(
            "join delta branch normalization column count mismatch: input has {}, comparable input has {}, output has {}; input_names={:?}; output_names={:?}",
            input_columns.len(),
            comparable_inputs.len(),
            output_columns.len(),
            input_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
        ));
    }
    Ok(comparable_inputs
        .iter()
        .zip(output_columns.iter())
        .map(|(input_column, output_column)| ProjectItem {
            expr: column_ref_expr(input_column),
            output_name: output_column.name.clone(),
            output_column_id: output_column.column_id,
        })
        .collect())
}

fn normalize_branch_project_items_by_id(
    input_columns: &[OutputColumn],
    output_columns: &[OutputColumn],
) -> Option<Vec<ProjectItem>> {
    let input_by_id = input_columns
        .iter()
        .map(|column| (column.column_id, column))
        .collect::<HashMap<_, _>>();
    output_columns
        .iter()
        .map(|output_column| {
            input_by_id
                .get(&output_column.column_id)
                .map(|input_column| ProjectItem {
                    expr: column_ref_expr(input_column),
                    output_name: output_column.name.clone(),
                    output_column_id: output_column.column_id,
                })
        })
        .collect()
}

fn comparable_branch_inputs<'a>(
    input_columns: &'a [OutputColumn],
    output_columns: &[OutputColumn],
) -> Vec<&'a OutputColumn> {
    let output_contains_row_id = output_columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(ImvRowIdColumn::NAME));
    input_columns
        .iter()
        .filter(|column| {
            output_contains_row_id || !column.name.eq_ignore_ascii_case(ImvRowIdColumn::NAME)
        })
        .collect()
}

pub(crate) fn plan_output_columns(plan: &LogicalPlanNode) -> Result<Vec<OutputColumn>, String> {
    Ok(match &plan.kind {
        LogicalPlanKind::ContentApply(node) => node.output_columns.clone(),
        LogicalPlanKind::Scan(scan) => scan.columns.clone(),
        LogicalPlanKind::Project(project) => project
            .items
            .iter()
            .filter(|item| item.output_column_id != ColumnId::UNSET)
            .map(project_item_output_column)
            .collect(),
        LogicalPlanKind::Aggregate(aggregate) => aggregate.output_columns.clone(),
        LogicalPlanKind::Join(join) => {
            join_output_columns(join.join_type, plan.left(), plan.right())?
        }
        LogicalPlanKind::Sort(_) => plan_output_columns(plan.unary_input())?,
        LogicalPlanKind::Limit(_) => plan_output_columns(plan.unary_input())?,
        LogicalPlanKind::Filter(_) => plan_output_columns(plan.unary_input())?,
        LogicalPlanKind::Union(union) => union.output_columns.clone(),
        LogicalPlanKind::Intersect(intersect) => intersect.output_columns.clone(),
        LogicalPlanKind::Except(except) => except.output_columns.clone(),
        LogicalPlanKind::Values(values) => values.columns.clone(),
        LogicalPlanKind::SystemTable(table) => table.columns.clone(),
        LogicalPlanKind::GenerateSeries(generate) => vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: generate.column_name.clone(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

            is_internal: false,
        }],
        LogicalPlanKind::TableFunction(table_function) => {
            let mut out = plan_output_columns(plan.unary_input())?;
            out.extend(table_function.output_columns.clone());
            out
        }
        LogicalPlanKind::Window(window) => window.output_columns.clone(),
        LogicalPlanKind::Repeat(_) => plan_output_columns(plan.unary_input())?,
        LogicalPlanKind::CTEAnchor(_) => plan_output_columns(plan.child(1))?,
        LogicalPlanKind::CTEProduce(produce) => produce.output_columns.clone(),
        LogicalPlanKind::CTEConsume(consume) => consume.output_columns.clone(),
        LogicalPlanKind::Apply(apply) => {
            let mut out = plan_output_columns(plan.left())?;
            out.push(apply.output_column.clone());
            out
        }
        LogicalPlanKind::AssertOneRow(_) => plan_output_columns(plan.unary_input())?,
        LogicalPlanKind::ImvDelta(delta) => {
            let mut out = plan_output_columns(plan.unary_input())?;
            out.retain(|column| {
                !column.name.eq_ignore_ascii_case(ImvActionColumn::NAME)
                    || delta.action_column == Some(column.column_id)
            });
            if let Some(action_column) = delta.action_column
                && !out.iter().any(|column| column.column_id == action_column)
            {
                out.push(ImvActionColumn::output_column(action_column));
            }
            out
        }
        LogicalPlanKind::ImvVersion(_) => plan_output_columns(plan.unary_input())?
            .into_iter()
            .filter(|column| !column.name.eq_ignore_ascii_case(ImvActionColumn::NAME))
            .collect(),
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
        value_type: item.expr.value_type.clone(),

        is_internal: item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME)
            || item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME),
    }
}

/// Defense-in-depth: any Outer/Semi/Anti join that survived into the validated
/// IMV plan is a bug (rewrite should have rejected it). Fail fast before apply.
pub(crate) struct UnsupportedJoinKindCheckRule;

/// Returns true if `plan` contains any Join node whose kind is not supported
/// for incremental delta rewrite (i.e., anything other than Inner/Cross).
fn plan_contains_unsupported_join(
    plan: &LogicalPlanNode,
    change_stream: &ImvChangeStreamDescriptor,
) -> bool {
    if change_stream.covers_aggregate_validation_root(plan) {
        return false;
    }
    match &plan.kind {
        LogicalPlanKind::Join(join) => {
            if !join_delta_kind_supported(join.join_type) {
                return true;
            }
            plan.children
                .iter()
                .any(|child| plan_contains_unsupported_join(child, change_stream))
        }
        _ => plan
            .children
            .iter()
            .any(|child| plan_contains_unsupported_join(child, change_stream)),
    }
}

impl LogicalRewriteRule for UnsupportedJoinKindCheckRule {
    fn name(&self) -> &'static str {
        "UnsupportedJoinKindCheck"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::Validation
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let change_stream = ctx
            .extension::<ImvExtension>()
            .map(|ext| ext.annotation.change_stream.clone())
            .unwrap_or_default();
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        plan_contains_unsupported_join(&plan, &change_stream)
    }

    fn apply(
        &self,
        _expr: OptExpr,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, SqlCompileError> {
        Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
            "UnsupportedJoinKindCheck",
            "incremental apply reached an unsupported join kind (only inner/cross are incrementalizable) — this is a bug: rewrite should have rejected it".to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use crate::planner::logical::*;
    use std::num::{NonZeroU32, NonZeroU64};

    use arrow::datatypes::DataType;

    use super::*;
    use crate::analysis::{BinOp, ExprKind, JoinKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
    use crate::column_id::{ColumnId, ColumnRefFactory};
    use crate::common::ImvVersionRef;
    use crate::optimizer::rewrite::context::RewriteContext;
    use crate::optimizer::scalar::ScalarArena;
    use crate::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::planner::imv_rewrite::change_stream::{
        AggregateChangeStreamDescriptor, AggregateChangeStreamShape, ImvChangeStreamDescriptor,
        SignedStateAggregateProof, TargetStateProof,
    };
    use crate::planner::imv_rewrite::scan_binding::ImvVersionRole;
    use crate::planner::logical::{
        LogicalAggregateNode, LogicalImvVersionNode, LogicalJoinNode, LogicalPlanKind,
    };
    use crate::planner::optimizer_bridge::logical::to_optimizer_expr;
    use crate::planner::payload::{PlanProjectNode, PlanScanNode};
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };
    use novarocks_types::schema::ColumnDef;

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
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(ColumnId(100)),
                branch_scope: None,
            }),
            vec![join_of(scan("l", 1), scan("r", 10))],
            None,
        );
        let arena_rc = ctx.scalar_arena();
        let non_root_expr = to_optimizer_expr(&non_root, &mut arena_rc.borrow_mut());
        assert!(rule.matches(&non_root_expr, &ctx));

        let over_agg = delta(aggregate_over(join_over(JoinKind::Inner)));
        let over_agg_expr = to_optimizer_expr(&over_agg, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&over_agg_expr, &ctx));
    }

    #[test]
    fn pure_join_delta_expands_into_union_without_outer_aggregate() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(ColumnId(100)),
                branch_scope: None,
            }),
            vec![join_over(JoinKind::Inner)],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("expand")
        else {
            panic!("pure join-delta must expand ImvDelta(Join) directly into a Union");
        };
        let arena = ctx.scalar_arena();
        let changed = crate::planner::optimizer_bridge::logical::to_logical_plan(
            changed_expr,
            &arena.borrow(),
        );
        let LogicalPlanKind::Union(union) = &changed.kind else {
            panic!("expected Union");
        };

        assert!(union.all);
        assert_eq!(changed.children.len(), 2);
        let left = assert_normalized_branch(changed.child(0), ColumnId(100));
        let LogicalPlanKind::Join(left_join) = &left.kind else {
            panic!("expected Join");
        };
        assert_condition_refs(left_join.condition.as_ref());
        assert_delta(left.left(), "left", ColumnId(100));
        assert_version(left.right(), "right", ImvVersionRole::From);

        let right = assert_normalized_branch(changed.child(1), ColumnId(100));
        let LogicalPlanKind::Join(right_join) = &right.kind else {
            panic!("expected Join");
        };
        assert_condition_refs(right_join.condition.as_ref());
        assert_version(right.left(), "left", ImvVersionRole::To);
        assert_delta(right.right(), "right", ColumnId(100));
    }

    #[test]
    fn pure_join_delta_preserves_bound_ieee_equality_on_both_branches() {
        use novarocks_functions::{FunctionArgument, FunctionBindingRequest};
        use novarocks_type_contract::{DecimalOverflowPolicy, OperatorFunction};

        let mut ctx = build_ctx();
        let float_key = |name, id| {
            let mut plan = scan(name, id);
            let LogicalPlanKind::Scan(scan) = &mut plan.kind else {
                unreachable!("test scan")
            };
            scan.table.columns[0].data_type = DataType::Float64;
            scan.columns[0].value_type =
                novarocks_type_contract::FunctionValueType::new(DataType::Float64, false);
            plan
        };
        let left = float_key("left", 1);
        let right = float_key("right", 10);
        let operands = [10, 1]
            .into_iter()
            .zip(["right_k", "left_k"])
            .map(|(id, name)| {
                let mut column = col_expr(id, name);
                column.value_type =
                    novarocks_type_contract::FunctionValueType::new(DataType::Float64, false);
                column
            })
            .collect::<Vec<_>>();
        let arguments = operands
            .iter()
            .map(|operand| FunctionArgument::Value {
                value_type: operand.value_type.clone(),
                constant: None,
            })
            .collect::<Vec<_>>();
        let operator = OperatorFunction::Equal;
        let binding = crate::functions::builtin_sql_function_catalog()
            .resolve_scalar_operator_binding(
                operator,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: arguments.len(),
                    expected_result_type: None,
                },
                &ctx.control_view(),
            )
            .expect("installed DOUBLE equality");
        let condition = crate::common::binary_operator::typed_call(
            operator,
            crate::binding::SqlFunctionBinding::from_plan(
                crate::planner::BINDING_AUTHORITY,
                binding,
                DecimalOverflowPolicy::OutputNull,
            ),
            operands,
        );
        let plan = delta(LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                naa_author: None,
                join_type: JoinKind::Inner,
                condition: Some(condition.clone()),
            }),
            vec![left, right],
            None,
        ));
        let arena = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena.borrow_mut());
        let RewriteResult::Changed(changed) = RewriteJoinDeltaRule.apply(expr, &mut ctx).unwrap()
        else {
            panic!("expected join delta expansion")
        };
        let changed =
            crate::planner::optimizer_bridge::logical::to_logical_plan(changed, &arena.borrow());

        assert_eq!(changed.children.len(), 2);
        for branch in &changed.children {
            let LogicalPlanKind::Join(join) = &branch.unary_input().kind else {
                panic!("expected delta Project over Join")
            };
            let actual = join.condition.as_ref().unwrap();
            assert!(
                crate::analysis::expr_identity::typed_expr_semantically_eq(
                    &condition,
                    actual,
                    &ctx.control_view(),
                )
                .unwrap()
            );
        }
    }

    #[test]
    fn pure_join_delta_drops_preexisting_action_metadata_outputs() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
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
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("expand")
        else {
            panic!("pure join-delta must expand into a Union");
        };
        let arena = ctx.scalar_arena();
        let changed = crate::planner::optimizer_bridge::logical::to_logical_plan(
            changed_expr,
            &arena.borrow(),
        );
        let LogicalPlanKind::Union(union) = &changed.kind else {
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
            let LogicalPlanKind::Project(project) = &input.kind else {
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
    fn normalize_branch_output_maps_duplicate_internal_row_ids_by_position() {
        let payload = output_column(1, "payload");
        let left_row_id = internal_output_column(7, ImvRowIdColumn::NAME);
        let right_row_id = internal_output_column(8, ImvRowIdColumn::NAME);
        let output_left_row_id = internal_output_column(6, ImvRowIdColumn::NAME);
        let output_right_row_id = internal_output_column(9, ImvRowIdColumn::NAME);

        let items = normalize_branch_project_items(
            &[payload.clone(), left_row_id, right_row_id],
            &[payload, output_left_row_id, output_right_row_id],
        )
        .expect("branch output normalization");

        assert_project_item_reads_column(&items[1], ColumnId(7));
        assert_eq!(items[1].output_column_id, ColumnId(6));
        assert_project_item_reads_column(&items[2], ColumnId(8));
        assert_eq!(items[2].output_column_id, ColumnId(9));
    }

    #[test]
    fn normalize_branch_output_selects_output_schema_from_wider_branch_inputs() {
        let left_payload = output_column(1, "left_payload");
        let left_row_id = internal_output_column(7, ImvRowIdColumn::NAME);
        let action = ImvActionColumn::output_column(ColumnId(100));
        let right_payload = output_column(10, "right_payload");
        let right_row_id = internal_output_column(12, ImvRowIdColumn::NAME);

        let items = normalize_branch_project_items(
            &[
                left_payload.clone(),
                left_row_id,
                action.clone(),
                right_payload.clone(),
                right_row_id,
            ],
            &[left_payload, right_payload, action],
        )
        .expect("branch output normalization");

        assert_project_item_reads_column(&items[0], ColumnId(1));
        assert_eq!(items[0].output_column_id, ColumnId(1));
        assert_project_item_reads_column(&items[1], ColumnId(10));
        assert_eq!(items[1].output_column_id, ColumnId(10));
        assert_project_item_reads_column(&items[2], ColumnId(100));
        assert_eq!(items[2].output_column_id, ColumnId(100));
    }

    #[test]
    fn join_delta_payload_output_columns_exclude_raw_row_lineage_columns() {
        let left = project_over(scan_with_external_row_id_metadata("left", 1, 6));
        let right = project_over(scan_with_row_id_metadata("right", 10, 12));

        let columns =
            join_delta_payload_output_columns(JoinKind::Inner, &left, &right).expect("payload");

        assert!(
            columns
                .iter()
                .all(|column| !column.name.eq_ignore_ascii_case(ImvRowIdColumn::NAME)),
            "join row-id columns are inputs for join apply-key construction, not UNION payload outputs: {columns:?}"
        );
        assert!(
            columns.iter().any(|column| column.column_id == ColumnId(1))
                && columns
                    .iter()
                    .any(|column| column.column_id == ColumnId(10)),
            "ordinary left/right payload columns must stay visible"
        );
    }

    #[test]
    fn pure_join_delta_rejects_outer_join() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(ColumnId(100)),
                branch_scope: None,
            }),
            vec![join_over(JoinKind::LeftOuter)],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        let err = rule.apply(expr, &mut ctx).expect_err("outer must reject");
        let SqlCompileError::Compilation(err) = err else {
            panic!("expected an ordinary rewrite error");
        };
        assert!(err.to_string().contains("inner/cross"), "unexpected: {err}");
    }

    #[test]
    fn pure_join_delta_nested_leaves_inner_join_delta_for_next_iteration() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let inner = join_of(scan("a", 1), scan("b", 10));
        let outer = join_of_with_left(inner, scan("c", 20));
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(ColumnId(100)),
                branch_scope: None,
            }),
            vec![outer],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        let RewriteResult::Changed(changed_expr) =
            rule.apply(expr, &mut ctx).expect("expand outer")
        else {
            panic!("expected Union");
        };
        let arena = ctx.scalar_arena();
        let changed = crate::planner::optimizer_bridge::logical::to_logical_plan(
            changed_expr,
            &arena.borrow(),
        );
        let LogicalPlanKind::Union(_) = &changed.kind else {
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
        let LogicalPlanKind::ImvDelta(delta) = &marked.kind else {
            panic!("expected ImvDelta wrapping the whole join, got {marked:?}");
        };
        assert!(!delta.is_root, "nested join delta marker is not root");
        assert_eq!(delta.action_column, Some(ColumnId(100)));
        assert!(matches!(&marked.children[0].kind, LogicalPlanKind::Join(_)));
    }

    #[test]
    fn mark_delta_scan_propagates_action_through_project_side() {
        let marked =
            mark_delta_scan(project_over(scan("a", 1)), ColumnId(100)).expect("mark delta");

        let LogicalPlanKind::Project(project) = &marked.kind else {
            panic!("expected Project, got {marked:?}");
        };
        let Some(action_item) = project
            .items
            .iter()
            .find(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
        else {
            panic!("delta-marked Project must expose the action column");
        };
        assert_eq!(action_item.output_column_id, ColumnId(100));
        assert!(matches!(
            &action_item.expr.kind,
            ExprKind::ColumnRef {
                column_id,
                column,
                ..
            } if *column_id == ColumnId(100) && column.eq_ignore_ascii_case(ImvActionColumn::NAME)
        ));
    }

    #[test]
    fn join_output_columns_keep_delta_side_action_for_normalized_branch() {
        let left =
            mark_delta_scan(project_over(scan("a", 1)), ColumnId(100)).expect("mark left delta");
        let right = mark_version_scan(project_over(scan("b", 10)), ImvVersionRef::from_snapshot())
            .expect("mark right version");
        let join = join_of(left, right);

        let columns = plan_output_columns(&join).expect("join output columns");

        assert!(
            columns
                .iter()
                .any(|column| ImvActionColumn::matches(column)
                    && column.column_id == ColumnId(100)),
            "normalized join-delta branch Join must expose the shared action column"
        );
    }

    #[test]
    fn join_delta_payload_excludes_iceberg_row_identity_metadata() {
        let columns = join_delta_payload_output_columns(
            JoinKind::Inner,
            &project_over(scan_with_iceberg_metadata("left", 1)),
            &project_over(scan_with_iceberg_metadata("right", 20)),
        )
        .expect("join delta payload columns");
        let names = columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["left_k", "left_v", "right_k", "right_v"]);
    }

    #[test]
    fn mark_version_scan_pushes_same_role_down_both_join_sides() {
        // Version marker over a Join distributes over the join:
        // Version(Join(a,b), from) == Join(Version(a, from), Version(b, from)).
        let join = join_of(scan("a", 1), scan("b", 10));
        let marked = mark_version_scan(join, ImvVersionRef::from_snapshot())
            .expect("mark version over join");
        let LogicalPlanKind::Join(_) = &marked.kind else {
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
            LogicalPlanKind::ImvVersion(v) => v,
            other => panic!("expected ImvVersion on join side, got {other:?}"),
        }
    }

    fn assert_normalized_branch(
        plan: &LogicalPlanNode,
        action_column: ColumnId,
    ) -> &LogicalPlanNode {
        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected normalized branch Project");
        };
        let mut expected_output_ids = plan_output_columns(plan.unary_input())
            .expect("branch output columns")
            .into_iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>();
        if !expected_output_ids.contains(&action_column) {
            expected_output_ids.push(action_column);
        }
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_column_id)
                .collect::<Vec<_>>(),
            expected_output_ids
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
        let LogicalPlanKind::Join(_) = &join_plan.kind else {
            panic!("expected Project(Join)");
        };
        join_plan
    }

    fn build_ctx() -> RewriteContext<'static> {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_scalar_arena(std::rc::Rc::new(
            std::cell::RefCell::new(ScalarArena::new()),
        ));
        let factory = std::rc::Rc::new(std::cell::RefCell::new(ColumnRefFactory::new()));
        factory.borrow_mut().reserve_until(100);
        ctx.set_column_ref_factory(std::rc::Rc::clone(&factory));
        ctx
    }

    fn delta(input: LogicalPlanNode) -> LogicalPlanNode {
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
                group_by: vec![col_expr(1, "l_k"), col_expr(10, "r_k")],
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
            LogicalPlanKind::Join(LogicalJoinNode {
                naa_author: None,
                join_type,
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
            LogicalPlanKind::Join(LogicalJoinNode {
                naa_author: None,
                join_type: JoinKind::Inner,
                condition: Some(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: novarocks_parser::ast::AstBox::new(col_expr(
                            left_key.column_id.0,
                            &left_key.name,
                        )),
                        op: BinOp::Eq,
                        right: novarocks_parser::ast::AstBox::new(col_expr(
                            right_key.column_id.0,
                            &right_key.name,
                        )),
                        decimal_overflow_policy:
                            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Boolean,
                        false,
                    ),
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
            LogicalPlanKind::Scan(scan) => scan.columns.clone(),
            _ => unreachable!(),
        };
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
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

    fn scan_with_iceberg_metadata(name: &str, first_id: u32) -> LogicalPlanNode {
        let mut plan = scan(name, first_id);
        let LogicalPlanKind::Scan(scan) = &mut plan.kind else {
            unreachable!();
        };
        scan.columns.extend([
            OutputColumn {
                column_id: ColumnId(first_id + 2),
                name: crate::common::ICEBERG_FILE_PATH_COL.to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),

                is_internal: false,
            },
            OutputColumn {
                column_id: ColumnId(first_id + 3),
                name: crate::common::ICEBERG_ROW_POS_COL.to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

                is_internal: false,
            },
            OutputColumn {
                column_id: ColumnId(first_id + 4),
                name: crate::common::ICEBERG_ROW_ID_COL.to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

                is_internal: false,
            },
            OutputColumn {
                column_id: ColumnId(first_id + 5),
                name: crate::common::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

                is_internal: false,
            },
        ]);
        plan
    }

    fn scan(name: &str, first_id: u32) -> LogicalPlanNode {
        let columns = vec![
            column_def(&format!("{name}_k")),
            column_def(&format!("{name}_v")),
        ];
        LogicalPlanNode::new(
            LogicalPlanKind::scan(PlanScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: name.to_string(),
                    columns,
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::Sql(SqlScanSource::new(
                        SqlTableBindingId::new(
                            SqlTableBindingScopeId::new(NonZeroU64::new(1).expect("scope")),
                            NonZeroU32::new(first_id.max(1)).expect("ordinal"),
                        ),
                        SqlTableIdentity {
                            catalog: "ice".to_string(),
                            namespace: "db".to_string(),
                            table: name.to_string(),
                        },
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    )),
                },
                alias: None,
                columns: vec![
                    output_column(first_id, &format!("{name}_k")),
                    output_column(first_id + 1, &format!("{name}_v")),
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

    fn scan_with_action_metadata(name: &str, first_id: u32, action_id: u32) -> LogicalPlanNode {
        let mut plan = scan(name, first_id);
        let LogicalPlanKind::Scan(scan) = &mut plan.kind else {
            unreachable!();
        };
        scan.columns.push(OutputColumn {
            column_id: ColumnId(action_id),
            name: ImvActionColumn::NAME.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),

            is_internal: false,
        });
        plan
    }

    fn scan_with_row_id_metadata(name: &str, first_id: u32, row_id: u32) -> LogicalPlanNode {
        let mut plan = scan(name, first_id);
        let LogicalPlanKind::Scan(scan) = &mut plan.kind else {
            unreachable!();
        };
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(row_id)));
        plan
    }

    fn scan_with_external_row_id_metadata(
        name: &str,
        first_id: u32,
        row_id: u32,
    ) -> LogicalPlanNode {
        let mut plan = scan(name, first_id);
        let LogicalPlanKind::Scan(scan) = &mut plan.kind else {
            unreachable!();
        };
        scan.columns.push(OutputColumn {
            column_id: ColumnId(row_id),
            name: ImvRowIdColumn::NAME.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

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
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

            is_internal: false,
        }
    }

    fn internal_output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            is_internal: true,
            ..output_column(id, name)
        }
    }

    fn assert_project_item_reads_column(item: &ProjectItem, expected: ColumnId) {
        assert!(matches!(
            &item.expr.kind,
            ExprKind::ColumnRef { column_id, .. } if *column_id == expected
        ));
    }

    fn col_expr(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        }
    }

    fn condition() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: novarocks_parser::ast::AstBox::new(col_expr(1, "left_k")),
                op: BinOp::Eq,
                right: novarocks_parser::ast::AstBox::new(col_expr(10, "right_k")),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        }
    }

    fn assert_condition_refs(condition: Option<&TypedExpr>) {
        let Some(TypedExpr {
            kind:
                ExprKind::BinaryOp {
                    left,
                    op,
                    right,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                },
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
            LogicalPlanKind::ImvDelta(_) => {
                matches!(&plan.unary_input().kind, LogicalPlanKind::Join(_))
                    || plan.children.iter().any(plan_contains_inner_join_delta)
            }
            _ => plan.children.iter().any(plan_contains_inner_join_delta),
        }
    }

    fn assert_delta(plan: &LogicalPlanNode, expected_scan: &str, action_column: ColumnId) {
        let LogicalPlanKind::Project(_) = &plan.kind else {
            panic!("expected Project");
        };
        let delta_plan = plan.unary_input();
        let LogicalPlanKind::ImvDelta(delta) = &delta_plan.kind else {
            panic!("expected Project(ImvDelta(...))");
        };
        assert!(!delta.is_root);
        assert_eq!(delta.action_column, Some(action_column));
        assert_scan(delta_plan.unary_input(), expected_scan);
    }

    fn assert_version(plan: &LogicalPlanNode, expected_scan: &str, role: ImvVersionRole) {
        let LogicalPlanKind::Project(_) = &plan.kind else {
            panic!("expected Project");
        };
        let version_plan = plan.unary_input();
        let LogicalPlanKind::ImvVersion(version) = &version_plan.kind else {
            panic!("expected Project(ImvVersion(...))");
        };
        assert_eq!(version.version_ref, ImvVersionRef { role });
        assert_scan(version_plan.unary_input(), expected_scan);
    }

    fn assert_scan(plan: &LogicalPlanNode, expected_scan: &str) {
        let LogicalPlanKind::Scan(scan) = &plan.kind else {
            panic!("expected Scan");
        };
        assert_eq!(scan.table.name, expected_scan);
    }

    #[test]
    fn validation_rejects_outer_join_reaching_apply() {
        // Build a delta-marked plan containing a LEFT OUTER join (which rewrite
        // should have rejected, but defense-in-depth catches it at validation).
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(ColumnId(100)),
                branch_scope: None,
            }),
            vec![join_over(JoinKind::LeftOuter)],
            None,
        );

        let ctx = build_ctx();
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());

        let rule = super::UnsupportedJoinKindCheckRule;
        assert!(
            rule.matches(&expr, &ctx),
            "UnsupportedJoinKindCheckRule must match a plan containing a LeftOuter join"
        );

        let mut ctx2 = build_ctx();
        let expr2 = to_optimizer_expr(&plan, &mut ctx2.scalar_arena().borrow_mut());
        let result = rule
            .apply(expr2, &mut ctx2)
            .expect("apply must not return Err");
        assert!(
            matches!(result, RewriteResult::Rejected(_)),
            "UnsupportedJoinKindCheckRule must return Rejected, got {result:?}"
        );
    }

    #[test]
    fn validation_does_not_use_descriptor_as_global_join_bypass() {
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(ColumnId(100)),
                branch_scope: None,
            }),
            vec![join_over(JoinKind::LeftOuter)],
            None,
        );

        let mut ctx = build_ctx();
        let mut ext = ImvExtension {
            snapshot: crate::compiler::mv_rewrite::test_aggregate_snapshot(vec![], None, None),
            annotation: ImvPlanAnnotation::default(),
        };
        ext.annotation.change_stream = ImvChangeStreamDescriptor {
            aggregate: Some(AggregateChangeStreamDescriptor {
                action_column_id: ColumnId(100),
                action_column_name: ImvActionColumn::NAME.to_string(),
                shape: AggregateChangeStreamShape::RelationalChangeStream,
                target_state: TargetStateProof { present: true },
                signed_state_aggregate: SignedStateAggregateProof { present: true },
            }),
        };
        ctx.set_extension::<ImvExtension>(ext);
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());

        let rule = super::UnsupportedJoinKindCheckRule;
        assert!(
            rule.matches(&expr, &ctx),
            "aggregate change-stream descriptor must only suppress joins under the descriptor root"
        );
    }
}
