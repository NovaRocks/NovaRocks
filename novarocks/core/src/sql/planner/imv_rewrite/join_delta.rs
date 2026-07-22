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

use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::mv::persistence::schema::JOIN_APPLY_KEY_COLUMN_NAME;
use crate::mv::persistence::schema::{
    BaseContract, ExpressionKind, JoinContractKind, MvSchemaContract, QualifiedFieldLineage,
};
use crate::sql::analysis::{
    ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
};
use crate::sql::column_id::ColumnId;
use crate::sql::common::ImvVersionRef;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::{RewriteDiagnostic, RewriteResult};
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::sql::planner::imv_rewrite::annotation::ImvExtension;
use crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor;
use crate::sql::planner::imv_rewrite::column_alloc::{
    allocate_imv_column, allocate_imv_output_column,
};
use crate::sql::planner::imv_rewrite::join_refresh_descriptor::{
    JoinRefreshBranchDescriptor, JoinRefreshBranchSide, JoinRefreshDescriptor,
    JoinRefreshJoinKeyPair, JoinRefreshMode, JoinRefreshMvIdentity, JoinRefreshOutputMapping,
    JoinRefreshOutputSource,
};
use crate::sql::planner::imv_rewrite::row_id_column::ImvRowIdColumn;
use crate::sql::planner::imv_rewrite::target_locator::is_target_locator_join;
use crate::sql::planner::imv_rewrite::{
    PlanRewriteResult, bridge_apply_result_mut, opt_expr_to_plan,
};
use crate::sql::planner::logical::{
    LogicalImvDeltaNode, LogicalImvVersionNode, LogicalJoinNode, LogicalPlanKind, LogicalPlanNode,
    LogicalUnionNode,
};
use crate::sql::planner::payload::PlanProjectNode;
use crate::sql::planner::table::ScanSource;

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
            LogicalPlanKind::ImvDelta(_) if matches!(&plan.unary_input().kind, LogicalPlanKind::Join(_))
        )
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        bridge_apply_result_mut(expr, ctx, |plan, ctx| {
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
                ));
            }

            let action_column = match delta.action_column {
                Some(action_column) => action_column,
                None => allocate_imv_column(ctx, ImvActionColumn::NAME, DataType::Int8, false)?,
            };

            let (left, right) = take_binary_children(&mut join_children);
            let LogicalJoinNode {
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

            let left_delta_branch = normalize_branch_output(
                LogicalPlanNode::new(
                    LogicalPlanKind::Join(LogicalJoinNode {
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

pub(crate) struct InjectJoinApplyKeyRule;

impl LogicalRewriteRule for InjectJoinApplyKeyRule {
    fn name(&self) -> &'static str {
        "InjectJoinApplyKey"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        (is_join_refresh_union_without_apply_key(&plan)
            && is_join_refresh_descriptor_candidate_context(ctx))
            || project_needs_join_refresh_internal_outputs(&plan)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        bridge_apply_result_mut(expr, ctx, |plan, ctx| {
            if project_needs_join_refresh_internal_outputs(&plan) {
                return Ok(PlanRewriteResult::Changed(
                    propagate_join_refresh_internal_outputs_through_project(plan)?,
                ));
            }
            Ok(PlanRewriteResult::Changed(inject_join_apply_key(
                plan, ctx,
            )?))
        })
    }
}

pub(crate) struct RecordJoinRefreshDescriptorRule;

impl LogicalRewriteRule for RecordJoinRefreshDescriptorRule {
    fn name(&self) -> &'static str {
        "RecordJoinRefreshDescriptor"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        is_join_refresh_union_with_apply_key(&plan)
            && is_join_refresh_descriptor_candidate_context(ctx)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        bridge_apply_result_mut(expr, ctx, |plan, ctx| {
            record_join_refresh_descriptor(ctx, &plan)?;
            Ok(PlanRewriteResult::Unchanged)
        })
    }
}

#[derive(Clone)]
struct PlanBaseIdentity {
    fqn: String,
    table_uuid: String,
    source_kind: BranchSourceKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BranchSourceKind {
    Delta,
    Version,
}

#[derive(Clone)]
struct JoinDeltaBranchEvidence {
    side: JoinRefreshBranchSide,
    left_base: PlanBaseIdentity,
    right_base: PlanBaseIdentity,
    left_output_columns: Vec<OutputColumn>,
    right_output_columns: Vec<OutputColumn>,
    left_row_id_column: OutputColumn,
    right_row_id_column: OutputColumn,
}

#[derive(Clone)]
struct JoinDeltaUnionEvidence {
    left_base_fqn: String,
    right_base_fqn: String,
    left_output_columns: Vec<OutputColumn>,
    right_output_columns: Vec<OutputColumn>,
    left_row_id_column: OutputColumn,
    right_row_id_column: OutputColumn,
    action_column: OutputColumn,
    join_apply_key_column: OutputColumn,
    branches: Vec<JoinRefreshBranchDescriptor>,
}

fn is_join_refresh_union_without_apply_key(plan: &LogicalPlanNode) -> bool {
    crate::sql::planner::imv_rewrite::join_delta_shape::is_supported_join_delta_union(plan)
        && matches!(
            &plan.kind,
            LogicalPlanKind::Union(union)
                if !union
                    .output_columns
                    .iter()
                    .any(|column| column.name.eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME))
        )
}

fn is_join_refresh_union_with_apply_key(plan: &LogicalPlanNode) -> bool {
    crate::sql::planner::imv_rewrite::join_delta_shape::is_supported_join_delta_union(plan)
        && matches!(
            &plan.kind,
            LogicalPlanKind::Union(union)
                if union
                    .output_columns
                    .iter()
                    .any(|column| column.name.eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME))
        )
}

fn inject_join_apply_key(
    mut plan: LogicalPlanNode,
    ctx: &mut RewriteContext,
) -> Result<LogicalPlanNode, String> {
    let ext = ctx
        .extension::<ImvExtension>()
        .ok_or_else(|| "InjectJoinApplyKey requires ImvExtension".to_string())?;
    let branch_evidence = collect_join_delta_branch_evidence(&plan)?;
    validate_join_descriptor_contract(ext, &branch_evidence)?;
    let join_apply_key_column =
        allocate_imv_output_column(ctx, JOIN_APPLY_KEY_COLUMN_NAME, DataType::Utf8, false, true)?;

    let LogicalPlanKind::Union(union) = &mut plan.kind else {
        return Ok(plan);
    };
    for (branch, evidence) in plan.children.iter_mut().zip(branch_evidence.iter()) {
        inject_join_apply_key_into_branch(branch, evidence, &join_apply_key_column)?;
        prune_raw_join_row_id_output_from_branch(branch, evidence)?;
    }
    union
        .output_columns
        .retain(|column| !ImvRowIdColumn::matches(column));
    union.output_columns.push(join_apply_key_column);
    Ok(plan)
}

fn project_needs_join_refresh_internal_outputs(plan: &LogicalPlanNode) -> bool {
    let LogicalPlanKind::Project(project) = &plan.kind else {
        return false;
    };
    let required = join_refresh_internal_output_columns(plan.unary_input());
    !required.is_empty()
        && required.iter().any(|required| {
            !project
                .items
                .iter()
                .any(|item| item.output_column_id == required.column_id)
        })
}

fn is_join_refresh_descriptor_candidate_context(ctx: &RewriteContext) -> bool {
    let Some(ext) = ctx.extension::<ImvExtension>() else {
        return false;
    };
    ext.annotation.change_stream.join_refresh.is_none()
        && ext.mv_ctx.schema_contract.aggregate.is_none()
}

fn propagate_join_refresh_internal_outputs_through_project(
    mut plan: LogicalPlanNode,
) -> Result<LogicalPlanNode, String> {
    let required = join_refresh_internal_output_columns(plan.unary_input());
    if required.is_empty() {
        return Err(
            "join refresh internal output propagation expected child output columns".to_string(),
        );
    }
    let LogicalPlanKind::Project(project) = &mut plan.kind else {
        return Ok(plan);
    };
    for column in required {
        if project
            .items
            .iter()
            .any(|item| item.output_column_id == column.column_id)
        {
            continue;
        }
        project.items.push(ProjectItem {
            expr: column_ref_expr(&column),
            output_name: column.name.clone(),
            output_column_id: column.column_id,
        });
    }
    Ok(plan)
}

fn join_refresh_internal_output_columns(plan: &LogicalPlanNode) -> Vec<OutputColumn> {
    let columns = plan_output_columns(plan).unwrap_or_default();
    let join_apply_key = columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME));
    let action = columns
        .iter()
        .find(|column| ImvActionColumn::matches(column));
    match (join_apply_key, action) {
        (Some(join_apply_key), Some(action)) => vec![join_apply_key.clone(), action.clone()],
        (Some(join_apply_key), None) => vec![join_apply_key.clone()],
        _ => Vec::new(),
    }
}

fn inject_join_apply_key_into_branch(
    branch: &mut LogicalPlanNode,
    evidence: &JoinDeltaBranchEvidence,
    join_apply_key_column: &OutputColumn,
) -> Result<(), String> {
    let LogicalPlanKind::Project(project) = &mut branch.kind else {
        return Err("join apply-key injection expected normalized Project branch".to_string());
    };
    if project.items.iter().any(|item| {
        item.output_name
            .eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME)
    }) {
        return Ok(());
    }
    project.items.push(ProjectItem {
        expr: join_row_key_expr(evidence),
        output_name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
        output_column_id: join_apply_key_column.column_id,
    });
    Ok(())
}

fn prune_raw_join_row_id_output_from_branch(
    branch: &mut LogicalPlanNode,
    evidence: &JoinDeltaBranchEvidence,
) -> Result<(), String> {
    let LogicalPlanKind::Project(project) = &mut branch.kind else {
        return Err("join apply-key pruning expected normalized Project branch".to_string());
    };
    let row_id_ids = [
        evidence.left_row_id_column.column_id,
        evidence.right_row_id_column.column_id,
    ];
    project.items.retain(|item| {
        !item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME)
            && !row_id_ids
                .iter()
                .any(|row_id| *row_id == item.output_column_id)
    });
    Ok(())
}

fn join_row_key_expr(evidence: &JoinDeltaBranchEvidence) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "join_row_key".to_string(),
            args: vec![
                string_literal(&evidence.left_base.table_uuid),
                column_ref_expr(&evidence.left_row_id_column),
                string_literal(&evidence.right_base.table_uuid),
                column_ref_expr(&evidence.right_row_id_column),
            ],
            distinct: false,
        },
        data_type: DataType::Utf8,
        nullable: false,
    }
}

fn string_literal(value: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::String(value.to_string())),
        data_type: DataType::Utf8,
        nullable: false,
    }
}

fn column_ref_expr(column: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: column.column_id,
            qualifier: None,
            column: column.name.clone(),
        },
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

fn record_join_refresh_descriptor(
    ctx: &mut RewriteContext,
    union_plan: &LogicalPlanNode,
) -> Result<(), String> {
    let Some(ext) = ctx.extension::<ImvExtension>().cloned() else {
        return Ok(());
    };
    if ext.annotation.change_stream.join_refresh.is_some() {
        return Ok(());
    }

    let evidence = collect_join_delta_union_evidence(union_plan)?;
    let descriptor = build_join_refresh_descriptor(&ext, evidence)?;
    descriptor.validate()?;

    let mut annotation = ext.annotation.clone();
    annotation.change_stream.join_refresh = Some(descriptor);
    ctx.set_extension::<ImvExtension>(ImvExtension { annotation, ..ext });
    Ok(())
}

fn collect_join_delta_union_evidence(
    union_plan: &LogicalPlanNode,
) -> Result<JoinDeltaUnionEvidence, String> {
    let LogicalPlanKind::Union(union) = &union_plan.kind else {
        return Err("join refresh descriptor expected join delta UnionAll".to_string());
    };
    let branch_evidence = collect_join_delta_branch_evidence(union_plan)?;
    let left_delta_branch = branch_evidence
        .iter()
        .find(|branch| branch.side == JoinRefreshBranchSide::LeftDeltaRightSnapshot)
        .ok_or_else(|| {
            "join refresh descriptor requires left-delta/right-snapshot branch".to_string()
        })?;
    let action_column =
        find_unique_internal_column(&union.output_columns, ImvActionColumn::NAME, "action")?;
    let join_apply_key_column = find_unique_internal_column(
        &union.output_columns,
        JOIN_APPLY_KEY_COLUMN_NAME,
        "join apply-key",
    )?;
    let branches = branch_evidence
        .iter()
        .map(|branch| JoinRefreshBranchDescriptor {
            side: branch.side,
            action_column_id: action_column.column_id,
        })
        .collect::<Vec<_>>();

    Ok(JoinDeltaUnionEvidence {
        left_base_fqn: left_delta_branch.left_base.fqn.clone(),
        right_base_fqn: left_delta_branch.right_base.fqn.clone(),
        left_output_columns: left_delta_branch.left_output_columns.clone(),
        right_output_columns: left_delta_branch.right_output_columns.clone(),
        left_row_id_column: left_delta_branch.left_row_id_column.clone(),
        right_row_id_column: left_delta_branch.right_row_id_column.clone(),
        action_column,
        join_apply_key_column,
        branches,
    })
}

fn collect_join_delta_branch_evidence(
    union_plan: &LogicalPlanNode,
) -> Result<Vec<JoinDeltaBranchEvidence>, String> {
    let LogicalPlanKind::Union(union) = &union_plan.kind else {
        return Err("join refresh descriptor expected join delta UnionAll".to_string());
    };
    if !union.all || union_plan.children.len() != 2 {
        return Err(
            "join refresh descriptor requires two UNION ALL join delta branches".to_string(),
        );
    }
    let branches = union_plan
        .children
        .iter()
        .map(join_delta_branch_evidence)
        .collect::<Result<Vec<_>, String>>()?;
    validate_branch_pair(&branches)?;
    Ok(branches)
}

fn join_delta_branch_evidence(branch: &LogicalPlanNode) -> Result<JoinDeltaBranchEvidence, String> {
    let LogicalPlanKind::Project(_) = &branch.kind else {
        return Err("join refresh descriptor expected normalized Project branch".to_string());
    };
    let join_plan = branch.unary_input();
    let LogicalPlanKind::Join(join) = &join_plan.kind else {
        return Err("join refresh descriptor expected Project(Join) branch".to_string());
    };
    if !join_delta_kind_supported(join.join_type) {
        return Err(format!(
            "join refresh descriptor supports inner/cross join delta branches only, got {:?}",
            join.join_type
        ));
    }

    let left_base = unique_branch_base_identity(join_plan.left(), "left")?;
    let right_base = unique_branch_base_identity(join_plan.right(), "right")?;
    let side = match (left_base.source_kind, right_base.source_kind) {
        (BranchSourceKind::Delta, BranchSourceKind::Version) => {
            JoinRefreshBranchSide::LeftDeltaRightSnapshot
        }
        (BranchSourceKind::Version, BranchSourceKind::Delta) => {
            JoinRefreshBranchSide::LeftSnapshotRightDelta
        }
        _ => {
            return Err(
                "join refresh descriptor requires each branch to contain one delta side and one snapshot side"
                    .to_string(),
            );
        }
    };
    let left_output_columns = plan_output_columns(join_plan.left())?;
    let right_output_columns = plan_output_columns(join_plan.right())?;
    let left_row_id_column =
        find_unique_internal_column(&left_output_columns, ImvRowIdColumn::NAME, "left row-id")?;
    let right_row_id_column =
        find_unique_internal_column(&right_output_columns, ImvRowIdColumn::NAME, "right row-id")?;

    Ok(JoinDeltaBranchEvidence {
        side,
        left_base,
        right_base,
        left_output_columns,
        right_output_columns,
        left_row_id_column,
        right_row_id_column,
    })
}

fn validate_branch_pair(branches: &[JoinDeltaBranchEvidence]) -> Result<(), String> {
    let [first, second] = branches else {
        return Err("join refresh descriptor requires exactly two join delta branches".to_string());
    };
    if !first
        .left_base
        .fqn
        .eq_ignore_ascii_case(&second.left_base.fqn)
        || !first
            .right_base
            .fqn
            .eq_ignore_ascii_case(&second.right_base.fqn)
    {
        return Err(format!(
            "join refresh descriptor branch bases do not align: first left={}, right={}; second left={}, right={}",
            first.left_base.fqn, first.right_base.fqn, second.left_base.fqn, second.right_base.fqn
        ));
    }
    if first.side == second.side {
        return Err(
            "join refresh descriptor requires one left-delta branch and one right-delta branch"
                .to_string(),
        );
    }
    Ok(())
}

fn unique_branch_base_identity(
    plan: &LogicalPlanNode,
    role: &str,
) -> Result<PlanBaseIdentity, String> {
    let mut bases = Vec::new();
    collect_branch_base_identities(plan, &mut bases)?;
    match bases.as_slice() {
        [base] => Ok(base.clone()),
        [] => Err(format!(
            "join refresh descriptor cannot derive {role} branch base from plan"
        )),
        _ => Err(format!(
            "join refresh descriptor requires one {role} branch base, found {}",
            bases.len()
        )),
    }
}

fn collect_branch_base_identities(
    plan: &LogicalPlanNode,
    bases: &mut Vec<PlanBaseIdentity>,
) -> Result<(), String> {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => match &scan.table.source {
            ScanSource::IcebergDeltaTable { table, .. } => {
                bases.push(plan_base_identity(table, BranchSourceKind::Delta)?);
            }
            ScanSource::IcebergVersionTable { table, .. } => {
                bases.push(plan_base_identity(table, BranchSourceKind::Version)?);
            }
            _ => {}
        },
        _ => {
            for child in &plan.children {
                collect_branch_base_identities(child, bases)?;
            }
        }
    }
    Ok(())
}

fn plan_base_identity(
    table: &crate::connector::iceberg::scan_model::IcebergTableInfo,
    source_kind: BranchSourceKind,
) -> Result<PlanBaseIdentity, String> {
    let table_uuid = table.table_uuid.clone().ok_or_else(|| {
        format!(
            "join refresh descriptor requires table uuid for {}.{}.{}",
            table.catalog, table.namespace, table.table
        )
    })?;
    Ok(PlanBaseIdentity {
        fqn: format!("{}.{}.{}", table.catalog, table.namespace, table.table),
        table_uuid,
        source_kind,
    })
}

fn validate_join_descriptor_contract(
    ext: &ImvExtension,
    branch_evidence: &[JoinDeltaBranchEvidence],
) -> Result<(), String> {
    let mv_ctx = ext.mv_ctx.as_ref();
    let Some(first) = branch_evidence.first() else {
        return Err("join refresh descriptor requires join delta branch evidence".to_string());
    };
    let join_contract = mv_ctx.schema_contract.join.as_ref().ok_or_else(|| {
        "join refresh descriptor requires schema_contract.join lineage".to_string()
    })?;
    if join_contract.kind != JoinContractKind::InnerEquiJoin {
        return Err(format!(
            "join refresh descriptor supports inner equi-join contract only, got {:?}",
            join_contract.kind
        ));
    }
    if join_contract.predicates.is_empty() {
        return Err(
            "join refresh descriptor requires at least one join predicate lineage".to_string(),
        );
    }
    validate_actual_bases_in_context(ext, &first.left_base.fqn, &first.right_base.fqn)?;
    let left_base_contract =
        base_contract_for_fqn(&mv_ctx.schema_contract.bases, &first.left_base.fqn)?;
    let right_base_contract =
        base_contract_for_fqn(&mv_ctx.schema_contract.bases, &first.right_base.fqn)?;
    build_join_key_pairs(
        join_contract,
        left_base_contract,
        right_base_contract,
        &first.left_base.fqn,
        &first.right_base.fqn,
        &first.left_output_columns,
        &first.right_output_columns,
    )?;
    Ok(())
}

fn validate_actual_bases_in_context(
    ext: &ImvExtension,
    left_base_fqn: &str,
    right_base_fqn: &str,
) -> Result<(), String> {
    let base_refs = &ext.mv_ctx.base_refs;
    if base_refs.len() != 2 {
        return Err(format!(
            "join refresh descriptor requires exactly two base refs, got {}",
            base_refs.len()
        ));
    }
    for fqn in [left_base_fqn, right_base_fqn] {
        if !base_refs
            .iter()
            .any(|base| base.fqn().eq_ignore_ascii_case(fqn))
        {
            return Err(format!(
                "join refresh descriptor actual plan base {fqn} is not in refresh context"
            ));
        }
    }
    Ok(())
}

fn build_join_refresh_descriptor(
    ext: &ImvExtension,
    evidence: JoinDeltaUnionEvidence,
) -> Result<JoinRefreshDescriptor, String> {
    let mv_ctx = ext.mv_ctx.as_ref();
    let join_contract = mv_ctx.schema_contract.join.as_ref().ok_or_else(|| {
        "join refresh descriptor requires schema_contract.join lineage".to_string()
    })?;
    if join_contract.kind != JoinContractKind::InnerEquiJoin {
        return Err(format!(
            "join refresh descriptor supports inner equi-join contract only, got {:?}",
            join_contract.kind
        ));
    }
    if join_contract.predicates.is_empty() {
        return Err(
            "join refresh descriptor requires at least one join predicate lineage".to_string(),
        );
    }
    validate_actual_bases_in_context(ext, &evidence.left_base_fqn, &evidence.right_base_fqn)?;
    let left_base_contract =
        base_contract_for_fqn(&mv_ctx.schema_contract.bases, &evidence.left_base_fqn)?;
    let right_base_contract =
        base_contract_for_fqn(&mv_ctx.schema_contract.bases, &evidence.right_base_fqn)?;
    let join_key_pairs = build_join_key_pairs(
        join_contract,
        left_base_contract,
        right_base_contract,
        &evidence.left_base_fqn,
        &evidence.right_base_fqn,
        &evidence.left_output_columns,
        &evidence.right_output_columns,
    )?;
    let payload_columns = build_join_payload_columns(
        &mv_ctx.schema_contract,
        left_base_contract,
        right_base_contract,
        &evidence.left_base_fqn,
        &evidence.right_base_fqn,
        &evidence.left_output_columns,
        &evidence.right_output_columns,
    )?;
    let output_mappings = join_refresh_output_mappings(
        &payload_columns,
        &evidence.action_column,
        &evidence.join_apply_key_column,
    );

    Ok(JoinRefreshDescriptor {
        mode: JoinRefreshMode::Coalesce,
        mv_identity: JoinRefreshMvIdentity {
            catalog: mv_ctx.target.catalog.clone(),
            database: mv_ctx.target.namespace.clone(),
            name: mv_ctx.target.table.clone(),
        },
        left_base_fqn: evidence.left_base_fqn,
        right_base_fqn: evidence.right_base_fqn,
        left_row_id_column: evidence.left_row_id_column,
        right_row_id_column: evidence.right_row_id_column,
        action_column: evidence.action_column,
        join_apply_key_column: evidence.join_apply_key_column,
        payload_columns,
        join_key_pairs,
        output_mappings,
        branches: evidence.branches,
        needs_target_locator: true,
    })
}

fn build_join_payload_columns(
    schema_contract: &MvSchemaContract,
    left_base_contract: &BaseContract,
    right_base_contract: &BaseContract,
    left_base_fqn: &str,
    right_base_fqn: &str,
    left_output_columns: &[OutputColumn],
    right_output_columns: &[OutputColumn],
) -> Result<Vec<OutputColumn>, String> {
    if schema_contract.output.columns.len() != schema_contract.target.visible_columns.len() {
        return Err(format!(
            "join refresh descriptor output/target column count mismatch: output has {}, target has {}",
            schema_contract.output.columns.len(),
            schema_contract.target.visible_columns.len()
        ));
    }

    let aggregate_contract = schema_contract.aggregate.is_some();
    let mut payload_columns = Vec::new();
    for (idx, (lineage, target)) in schema_contract
        .output
        .columns
        .iter()
        .zip(schema_contract.target.visible_columns.iter())
        .enumerate()
    {
        if lineage.expression.kind != ExpressionKind::Column {
            if aggregate_contract {
                continue;
            }
            return Err(format!(
                "join refresh descriptor payload column {idx} `{}` must be a direct column reference, got {:?}",
                target.output_name, lineage.expression.kind
            ));
        }
        let [field] = lineage.expression.referenced_base_fields.as_slice() else {
            return Err(format!(
                "join refresh descriptor payload column {idx} `{}` must reference exactly one base field, got {}",
                target.output_name,
                lineage.expression.referenced_base_fields.len()
            ));
        };
        let (base_contract, output_columns, role) = if field
            .table_fqn
            .eq_ignore_ascii_case(left_base_fqn)
        {
            (left_base_contract, left_output_columns, "left payload")
        } else if field.table_fqn.eq_ignore_ascii_case(right_base_fqn) {
            (right_base_contract, right_output_columns, "right payload")
        } else {
            return Err(format!(
                "join refresh descriptor payload column {idx} `{}` references base {} outside actual join bases {}, {}",
                target.output_name, field.table_fqn, left_base_fqn, right_base_fqn
            ));
        };
        let field_name = field_name_for_lineage(base_contract, field)?;
        payload_columns.push(find_unique_output_column(output_columns, field_name, role)?);
    }
    Ok(payload_columns)
}

fn build_join_key_pairs(
    join_contract: &crate::mv::persistence::schema::JoinContract,
    left_base_contract: &BaseContract,
    right_base_contract: &BaseContract,
    left_base_fqn: &str,
    right_base_fqn: &str,
    left_output_columns: &[OutputColumn],
    right_output_columns: &[OutputColumn],
) -> Result<Vec<JoinRefreshJoinKeyPair>, String> {
    join_contract
        .predicates
        .iter()
        .map(|predicate| {
            let (left_lineage, right_lineage) =
                predicate_lineage_for_actual_sides(predicate, left_base_fqn, right_base_fqn)?;
            let left_name = field_name_for_lineage(left_base_contract, left_lineage)?;
            let right_name = field_name_for_lineage(right_base_contract, right_lineage)?;
            Ok(JoinRefreshJoinKeyPair {
                left_column: find_unique_output_column(
                    left_output_columns,
                    left_name,
                    "left join key",
                )?,
                right_column: find_unique_output_column(
                    right_output_columns,
                    right_name,
                    "right join key",
                )?,
            })
        })
        .collect()
}

fn predicate_lineage_for_actual_sides<'a>(
    predicate: &'a crate::mv::persistence::schema::JoinPredicateLineage,
    left_base_fqn: &str,
    right_base_fqn: &str,
) -> Result<(&'a QualifiedFieldLineage, &'a QualifiedFieldLineage), String> {
    if predicate.left.table_fqn.eq_ignore_ascii_case(left_base_fqn)
        && predicate
            .right
            .table_fqn
            .eq_ignore_ascii_case(right_base_fqn)
    {
        return Ok((&predicate.left, &predicate.right));
    }
    if predicate
        .left
        .table_fqn
        .eq_ignore_ascii_case(right_base_fqn)
        && predicate
            .right
            .table_fqn
            .eq_ignore_ascii_case(left_base_fqn)
    {
        return Ok((&predicate.right, &predicate.left));
    }
    Err(format!(
        "join refresh descriptor predicate lineage does not align with actual plan bases: predicate left={}, right={}, actual left={}, right={}",
        predicate.left.table_fqn, predicate.right.table_fqn, left_base_fqn, right_base_fqn
    ))
}

fn base_contract_for_fqn<'a>(
    bases: &'a [BaseContract],
    table_fqn: &str,
) -> Result<&'a BaseContract, String> {
    let matches = bases
        .iter()
        .filter(|base| base.table_fqn.eq_ignore_ascii_case(table_fqn))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [base] => Ok(*base),
        [] => Err(format!(
            "join refresh descriptor schema contract missing base {table_fqn}"
        )),
        _ => Err(format!(
            "join refresh descriptor schema contract has duplicate base {table_fqn}"
        )),
    }
}

fn field_name_for_lineage<'a>(
    base: &'a BaseContract,
    field: &QualifiedFieldLineage,
) -> Result<&'a str, String> {
    if !field.table_fqn.eq_ignore_ascii_case(&base.table_fqn) {
        return Err(format!(
            "join refresh descriptor lineage table {} does not match base {}",
            field.table_fqn, base.table_fqn
        ));
    }
    if let Some(alias) = &base.alias_at_create {
        if !field.qualifier_at_create.eq_ignore_ascii_case(alias) {
            return Err(format!(
                "join refresh descriptor lineage qualifier {} does not match base alias {}",
                field.qualifier_at_create, alias
            ));
        }
    }
    base.schema_at_create
        .fields
        .iter()
        .find(|base_field| base_field.field_id == field.field_id)
        .map(|base_field| base_field.name_at_create.as_str())
        .ok_or_else(|| {
            format!(
                "join refresh descriptor lineage references unknown field {} on base {}",
                field.field_id, base.table_fqn
            )
        })
}

fn find_unique_output_column(
    columns: &[OutputColumn],
    name: &str,
    role: &str,
) -> Result<OutputColumn, String> {
    let matches = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name) && !column.is_internal)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [column] => Ok((*column).clone()),
        [] => Err(format!(
            "join refresh descriptor cannot find {role} column {name}"
        )),
        _ => Err(format!(
            "join refresh descriptor found multiple {role} columns named {name}"
        )),
    }
}

fn find_unique_internal_column(
    columns: &[OutputColumn],
    name: &str,
    role: &str,
) -> Result<OutputColumn, String> {
    let matches = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name) && column.is_internal)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [column] => Ok((*column).clone()),
        [] => Err(format!(
            "join refresh descriptor cannot find {role} internal column {name}"
        )),
        _ => Err(format!(
            "join refresh descriptor found multiple {role} internal columns named {name}"
        )),
    }
}

fn join_refresh_output_mappings(
    payload_columns: &[OutputColumn],
    action_column: &OutputColumn,
    join_apply_key_column: &OutputColumn,
) -> Vec<JoinRefreshOutputMapping> {
    let mut seen_names = HashMap::new();
    let mut mappings = payload_columns
        .iter()
        .map(|payload| JoinRefreshOutputMapping {
            mv_output_column: unique_mapping_output_column(payload, &mut seen_names),
            source: JoinRefreshOutputSource::Payload(payload.column_id),
        })
        .collect::<Vec<_>>();
    mappings.push(JoinRefreshOutputMapping {
        mv_output_column: unique_mapping_output_column(action_column, &mut seen_names),
        source: JoinRefreshOutputSource::Action(action_column.column_id),
    });
    mappings.push(JoinRefreshOutputMapping {
        mv_output_column: unique_mapping_output_column(join_apply_key_column, &mut seen_names),
        source: JoinRefreshOutputSource::JoinApplyKey(join_apply_key_column.column_id),
    });
    mappings
}

fn unique_mapping_output_column(
    source: &OutputColumn,
    seen_names: &mut HashMap<String, usize>,
) -> OutputColumn {
    let mut output = source.clone();
    let normalized = output.name.to_ascii_lowercase();
    let count = seen_names.entry(normalized).or_insert(0);
    if *count > 0 {
        output.name = format!("{}__{}", output.name, output.column_id.0);
    }
    *count += 1;
    output
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
        .eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_FILE_PATH_COL)
        || column
            .name
            .eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_POS_COL)
        || column
            .name
            .eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_ID_COL)
        || column
            .name
            .eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL)
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
            data_type: action_output.data_type.clone(),
            nullable: action_output.nullable,
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
        LogicalPlanKind::GenerateSeries(generate) => vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: generate.column_name.clone(),
            data_type: DataType::Int64,
            nullable: false,
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
        data_type: item.expr.data_type.clone(),
        nullable: item.expr.nullable,
        is_internal: item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME)
            || item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME)
            || item
                .output_name
                .eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME),
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
            if !is_target_locator_join(plan) && !join_delta_kind_supported(join.join_type) {
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

    fn apply(&self, _expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
            "UnsupportedJoinKindCheck",
            "incremental apply reached an unsupported join kind (only inner/cross are incrementalizable) — this is a bug: rewrite should have rejected it".to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::logical::*;
    use crate::sql::planner::payload::*;
    use std::collections::BTreeMap;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::mv::rewrite::context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::column_id::{ColumnId, ColumnRefFactory};
    use crate::sql::common::ImvVersionRef;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::planner::imv_rewrite::change_stream::{
        AggregateChangeStreamDescriptor, AggregateChangeStreamShape, ImvChangeStreamDescriptor,
        SignedStateAggregateProof, TargetStateProof,
    };
    use crate::sql::planner::imv_rewrite::scan_binding::ImvVersionRole;
    use crate::sql::planner::logical::{
        LogicalAggregateNode, LogicalImvVersionNode, LogicalJoinNode, LogicalPlanKind,
    };
    use crate::sql::planner::optimizer_bridge::logical::to_optimizer_expr;
    use crate::sql::planner::payload::{PlanProjectNode, PlanScanNode};
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

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
        let changed = crate::sql::planner::optimizer_bridge::logical::to_logical_plan(
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
        let changed = crate::sql::planner::optimizer_bridge::logical::to_logical_plan(
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
        assert!(err.contains("inner/cross"), "unexpected: {err}");
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
        let changed = crate::sql::planner::optimizer_bridge::logical::to_logical_plan(
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
    fn pure_join_delta_does_not_record_descriptor_before_key_injection() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
        });
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
        let result = rule
            .apply(expr, &mut ctx)
            .expect("early join-delta rewrite must not require descriptor lineage");

        assert!(
            matches!(result, RewriteResult::Changed(_)),
            "expected join-delta union rewrite"
        );
        let ext = ctx
            .extension::<ImvExtension>()
            .expect("extension must stay installed");
        assert!(
            ext.annotation.change_stream.join_refresh.is_none(),
            "descriptor must be recorded only after row-id/apply-key injection"
        );
    }

    #[test]
    fn join_apply_key_rule_propagates_key_through_project() {
        let rule = InjectJoinApplyKeyRule;
        let mut ctx = build_ctx();
        let plan = project_payload_only(join_apply_key_union());

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(
            rule.matches(&expr, &ctx),
            "Project above a join apply-key union must expose the key for coalescing"
        );

        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("propagate")
        else {
            panic!("join apply-key propagation must change the Project");
        };
        let arena = ctx.scalar_arena();
        let changed = crate::sql::planner::optimizer_bridge::logical::to_logical_plan(
            changed_expr,
            &arena.borrow(),
        );
        let LogicalPlanKind::Project(project) = &changed.kind else {
            panic!("expected Project");
        };
        assert!(project.items.iter().any(|item| {
            item.output_name
                .eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME)
                && item.output_column_id == ColumnId(21)
        }));
    }

    #[test]
    fn join_apply_key_rule_propagates_action_through_project() {
        let rule = InjectJoinApplyKeyRule;
        let mut ctx = build_ctx();
        let plan = project_payload_only(join_apply_key_union());

        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(
            rule.matches(&expr, &ctx),
            "Project above a join apply-key union must expose action for coalescing"
        );

        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("propagate")
        else {
            panic!("join action propagation must change the Project");
        };
        let arena = ctx.scalar_arena();
        let changed = crate::sql::planner::optimizer_bridge::logical::to_logical_plan(
            changed_expr,
            &arena.borrow(),
        );
        let LogicalPlanKind::Project(project) = &changed.kind else {
            panic!("expected Project");
        };
        assert!(project.items.iter().any(|item| {
            item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME)
                && item.output_column_id == ColumnId(20)
        }));
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

    fn build_ctx() -> RewriteContext {
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
            LogicalPlanKind::Join(LogicalJoinNode {
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

    fn project_payload_only(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: col_expr(1, "payload"),
                    output_name: "payload".to_string(),
                    output_column_id: ColumnId(1),
                }],
                output_qualifier: None,
            }),
            vec![input],
            None,
        )
    }

    fn join_apply_key_union() -> LogicalPlanNode {
        let payload = output_column(1, "payload");
        let action = ImvActionColumn::output_column(ColumnId(20));
        let join_apply_key = OutputColumn {
            column_id: ColumnId(21),
            name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            is_internal: true,
        };
        LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![payload.clone(), action.clone(), join_apply_key.clone()],
            }),
            vec![
                LogicalPlanNode::new(
                    LogicalPlanKind::Values(PlanValuesNode {
                        rows: Vec::new(),
                        columns: vec![payload.clone(), action.clone(), join_apply_key.clone()],
                    }),
                    Vec::new(),
                    None,
                ),
                LogicalPlanNode::new(
                    LogicalPlanKind::Values(PlanValuesNode {
                        rows: Vec::new(),
                        columns: vec![payload, action, join_apply_key],
                    }),
                    Vec::new(),
                    None,
                ),
            ],
            None,
        )
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
                name: crate::exec::row_position::ICEBERG_FILE_PATH_COL.to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                is_internal: false,
            },
            OutputColumn {
                column_id: ColumnId(first_id + 3),
                name: crate::exec::row_position::ICEBERG_ROW_POS_COL.to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            },
            OutputColumn {
                column_id: ColumnId(first_id + 4),
                name: crate::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            },
            OutputColumn {
                column_id: ColumnId(first_id + 5),
                name: crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
                data_type: DataType::Int64,
                nullable: false,
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
            data_type: DataType::Int8,
            nullable: false,
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
            data_type: DataType::Int64,
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
            mv_ctx: dummy_rewrite_context(),
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
            ..Default::default()
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
