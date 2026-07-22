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

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, TimeUnit};
use iceberg::spec::{NestedField, PrimitiveType, Type};

use crate::sql::analysis::expr_display::typed_expr_display_name;
use crate::sql::analysis::{
    BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr, UnOp,
};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::sql::planner::imv_rewrite::annotation::ImvExtension;
use crate::sql::planner::imv_rewrite::column_alloc::{
    allocate_imv_column, allocate_imv_output_column,
};
use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
use crate::sql::planner::imv_rewrite::target_state::build_target_state_scan_source;
use crate::sql::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::sql::planner::logical::{
    LogicalAggregateNode, LogicalImvDeltaNode, LogicalJoinNode, LogicalPlanKind, LogicalPlanNode,
};
use crate::sql::planner::payload::{
    AggregateCall, PlanFilterNode, PlanProjectNode, PlanScanNode, PlanValuesNode,
};
use crate::sql::planner::plan_output_columns as planner_plan_output_columns;
use crate::sql::planner::table::{
    IcebergMvTargetStatePartitionConstraint, IcebergMvTargetStateRowFilter, TableDef,
};
use novarocks_catalog::schema::ColumnDef;

pub(crate) struct RewriteAggregateStateRule;

pub(crate) fn signed_state_function(name: &str) -> Result<&'static str, String> {
    match name.to_ascii_lowercase().as_str() {
        "count" => Ok("count_state_signed"),
        "sum" => Ok("sum_state_signed"),
        "avg" => Ok("avg_state_signed"),
        "min" => Ok("min_state_signed"),
        "max" => Ok("max_state_signed"),
        "bool_or" | "boolor_agg" => Ok("bool_or_state_signed"),
        "bool_and" | "booland_agg" | "every" => Ok("bool_and_state_signed"),
        other => Err(format!("unsupported IMV aggregate function {other}")),
    }
}

impl LogicalRewriteRule for RewriteAggregateStateRule {
    fn name(&self) -> &'static str {
        "RewriteAggregateState"
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
            LogicalPlanKind::ImvDelta(delta)
                if delta.is_root
                    && matches!(&plan.unary_input().kind, LogicalPlanKind::Aggregate(_))
        )
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        bridge_apply_result(expr, ctx, |plan, ctx| {
            let LogicalPlanNode {
                kind, mut children, ..
            } = plan;
            let LogicalPlanKind::ImvDelta(delta) = kind else {
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
            let LogicalPlanKind::Aggregate(aggregate) = aggregate_kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            let aggregate_input = take_unary_child(&mut aggregate_children);

            let ext = ctx
                .extension::<ImvExtension>()
                .ok_or_else(|| {
                    "RewriteAggregateState requires ImvExtension in RewriteContext".to_string()
                })?
                .clone();
            let merge = build_aggregate_state_merge(
                aggregate,
                aggregate_input,
                aggregate_required_output_columns,
                delta.action_column,
                branch_scope,
                ctx,
                &ext,
            )?;
            Ok(PlanRewriteResult::Changed(merge))
        })
    }
}

pub(crate) fn build_aggregate_state_merge(
    aggregate: LogicalAggregateNode,
    aggregate_input: LogicalPlanNode,
    aggregate_required_output_columns: Option<HashSet<ColumnId>>,
    action_column: Option<ColumnId>,
    branch_scope: Option<crate::sql::planner::table::BranchScope>,
    ctx: &RewriteContext,
    ext: &ImvExtension,
) -> Result<LogicalPlanNode, String> {
    if aggregate.group_by.is_empty() {
        return Err("Iceberg IMV aggregate rewrite requires at least one GROUP BY key".to_string());
    }
    if aggregate.aggregates.iter().any(|call| call.distinct) {
        return Err("Iceberg IMV aggregate rewrite does not support SELECT DISTINCT".to_string());
    }

    let (aggregate_calls, aggregate_layout) =
        ext.mv_ctx.aggregate_shape_and_layout_for_execution()?;
    let group_key_names = group_key_names(&aggregate)?;
    let aggregate_state_names = aggregate_state_names(ext, &aggregate, &aggregate_layout)?;
    let row_id_column_name = aggregate_row_id_column_name(ext)?;
    let target_columns = target_columns(ext)?;
    let target = &ext.mv_ctx.target;
    let aggregate_contract = ext
        .mv_ctx
        .schema_contract
        .aggregate
        .as_ref()
        .ok_or_else(|| {
            "Iceberg IMV aggregate rewrite requires aggregate state contract".to_string()
        })?;
    let physical_column_names = aggregate_layout
        .physical_columns
        .iter()
        .map(|column| column.column.name.clone())
        .collect::<Vec<_>>();
    let partition_constraint = if is_unpartitioned_target_contract(&ext.mv_ctx.schema_contract) {
        IcebergMvTargetStatePartitionConstraint::Unpartitioned
    } else {
        IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired
    };

    let old_source = build_target_state_scan_source(
        target.catalog.clone(),
        target.namespace.clone(),
        target.table.clone(),
        ext.mv_ctx.target_table_uuid.clone(),
        ext.mv_ctx.target_snapshot_id,
        aggregate_contract.state_layout_version,
        target_columns.clone(),
        group_key_names.clone(),
        aggregate_state_names.clone(),
        physical_column_names,
        row_id_column_name.clone(),
        IcebergMvTargetStateRowFilter::DeltaInputRowIds {
            row_id_column_name: row_id_column_name.clone(),
            branch_scope: branch_scope.clone(),
        },
        partition_constraint,
    );
    let old_scan = target_state_old_scan(
        target,
        target_columns,
        &group_key_names,
        &aggregate_state_names,
        &row_id_column_name,
        branch_scope.as_ref(),
        old_source,
        ctx,
    )?;
    let old_input = branch_scoped_old_input(old_scan, branch_scope.clone(), &aggregate_layout)?;

    let action_column = match action_column {
        Some(action_column) => action_column,
        None => match existing_delta_action_column(&aggregate_input)? {
            Some(action_column) => action_column,
            None => allocate_imv_column(ctx, ImvActionColumn::NAME, DataType::Int8, false)?,
        },
    };
    let signed_aggregate = signed_aggregate(
        aggregate,
        aggregate_input,
        aggregate_required_output_columns,
        action_column,
        ctx,
        &aggregate_calls,
        &aggregate_layout,
    )?;

    build_relational_aggregate_change_stream(
        old_input,
        signed_aggregate,
        branch_scope,
        ctx,
        &aggregate_layout,
    )
}

fn target_state_old_scan(
    target: &novarocks_catalog::identifier::TableIdentity,
    target_columns: Vec<ColumnDef>,
    group_key_names: &[String],
    aggregate_state_names: &[String],
    row_id_column_name: &str,
    branch_scope: Option<&crate::sql::planner::table::BranchScope>,
    old_source: crate::sql::planner::table::ScanSource,
    ctx: &RewriteContext,
) -> Result<LogicalPlanNode, String> {
    let locator_metadata_columns = target_state_locator_metadata_columns();
    let old_columns = if branch_scope.is_some() {
        target_state_branch_scoped_old_scan_columns(
            ctx,
            &target_columns,
            &locator_metadata_columns,
            aggregate_state_names,
            row_id_column_name,
        )?
    } else {
        target_state_compact_old_scan_columns(
            ctx,
            &target_columns,
            &locator_metadata_columns,
            group_key_names,
            aggregate_state_names,
            row_id_column_name,
        )?
    };
    let required_columns =
        target_state_required_column_names(&target_columns, &locator_metadata_columns);
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Scan(PlanScanNode {
            database: target.namespace.clone(),
            table: TableDef {
                name: target.table.clone(),
                columns: target_columns,
                iceberg_row_lineage_metadata_columns: locator_metadata_columns,
                source: old_source,
            },
            alias: None,
            columns: old_columns,
            predicates: Vec::new(),
            required_columns: Some(required_columns),
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
        vec![],
        None,
    ))
}

fn target_state_branch_scoped_old_scan_columns(
    ctx: &RewriteContext,
    target_columns: &[ColumnDef],
    locator_metadata_columns: &[ColumnDef],
    aggregate_state_names: &[String],
    row_id_column_name: &str,
) -> Result<Vec<OutputColumn>, String> {
    let mut old_columns = Vec::with_capacity(target_columns.len() + locator_metadata_columns.len());
    for column in target_columns {
        old_columns.push(allocate_imv_output_column(
            ctx,
            &column.name,
            column.data_type.clone(),
            column.nullable,
            aggregate_state_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&column.name))
                || column.name.eq_ignore_ascii_case(row_id_column_name),
        )?);
    }
    for column in locator_metadata_columns {
        if old_columns
            .iter()
            .any(|existing| existing.name.eq_ignore_ascii_case(&column.name))
        {
            continue;
        }
        old_columns.push(allocate_imv_output_column(
            ctx,
            &column.name,
            column.data_type.clone(),
            column.nullable,
            true,
        )?);
    }
    Ok(old_columns)
}

fn target_state_compact_old_scan_columns(
    ctx: &RewriteContext,
    target_columns: &[ColumnDef],
    locator_metadata_columns: &[ColumnDef],
    group_key_names: &[String],
    aggregate_state_names: &[String],
    row_id_column_name: &str,
) -> Result<Vec<OutputColumn>, String> {
    let mut names = Vec::with_capacity(
        1 + group_key_names.len() + aggregate_state_names.len() + locator_metadata_columns.len(),
    );
    push_unique_name(&mut names, row_id_column_name);
    for name in group_key_names {
        push_unique_name(&mut names, name);
    }
    for name in aggregate_state_names {
        push_unique_name(&mut names, name);
    }
    for column in locator_metadata_columns {
        push_unique_name(&mut names, &column.name);
    }

    names
        .into_iter()
        .map(|name| {
            let column = target_columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&name))
                .map(|column| (column, false))
                .or_else(|| {
                    locator_metadata_columns
                        .iter()
                        .find(|column| column.name.eq_ignore_ascii_case(&name))
                        .map(|column| (column, true))
                })
                .ok_or_else(|| {
                    format!(
                        "Iceberg IMV aggregate rewrite target-state old input cannot resolve public column {name}"
                    )
                })?;
            Ok(allocate_imv_output_column(
                ctx,
                &column.0.name,
                column.0.data_type.clone(),
                column.0.nullable,
                column.1
                    || aggregate_state_names
                        .iter()
                        .any(|state| state.eq_ignore_ascii_case(&column.0.name))
                    || column.0.name.eq_ignore_ascii_case(row_id_column_name),
            )?)
        })
        .collect()
}

fn target_state_required_column_names(
    target_columns: &[ColumnDef],
    locator_metadata_columns: &[ColumnDef],
) -> Vec<String> {
    let mut names = Vec::with_capacity(target_columns.len() + locator_metadata_columns.len());
    for column in target_columns.iter().chain(locator_metadata_columns.iter()) {
        push_unique_name(&mut names, &column.name);
    }
    names
}

fn push_unique_name(names: &mut Vec<String>, name: &str) {
    if !names
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(name))
    {
        names.push(name.to_string());
    }
}

fn target_state_locator_metadata_columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: crate::exec::row_position::ICEBERG_FILE_PATH_COL.to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: crate::exec::row_position::ICEBERG_ROW_POS_COL.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: crate::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
    ]
}

fn branch_scoped_old_input(
    old_scan: LogicalPlanNode,
    branch_scope: Option<crate::sql::planner::table::BranchScope>,
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
) -> Result<LogicalPlanNode, String> {
    let Some(scope) = branch_scope else {
        return Ok(old_scan);
    };
    let old_outputs = plan_output_columns(&old_scan)?;
    let filtered = LogicalPlanNode::new(
        LogicalPlanKind::Filter(PlanFilterNode {
            predicate: branch_scope_predicate(&scope, &old_outputs)?,
        }),
        vec![old_scan],
        None,
    );
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            items: aggregate_old_state_passthrough_items(layout, &old_outputs)?,
            output_qualifier: None,
        }),
        vec![filtered],
        None,
    ))
}

fn build_relational_aggregate_change_stream(
    old_input: LogicalPlanNode,
    signed_delta: LogicalPlanNode,
    branch_scope: Option<crate::sql::planner::table::BranchScope>,
    ctx: &RewriteContext,
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
) -> Result<LogicalPlanNode, String> {
    let old_outputs = plan_output_columns(&old_input)?;
    let delta_with_row_id = delta_state_with_row_id(signed_delta, layout, ctx)?;
    let delta_outputs = plan_output_columns(&delta_with_row_id)?;
    let row_id_name = &layout.row_id_column.column.name;
    let delta_row_id = find_output_column_by_name(&delta_outputs, row_id_name)?.clone();
    let old_row_id = find_output_column_by_name(&old_outputs, row_id_name)?.clone();

    let join = LogicalPlanNode::new(
        LogicalPlanKind::Join(LogicalJoinNode {
            join_type: JoinKind::LeftOuter,
            condition: Some(TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(column_ref(&delta_row_id)),
                    op: BinOp::Eq,
                    right: Box::new(column_ref(&old_row_id)),
                },
                data_type: DataType::Boolean,
                nullable: false,
            }),
        }),
        vec![delta_with_row_id, old_input],
        None,
    );
    let output_columns =
        aggregate_change_stream_output_columns(layout, branch_scope.as_ref(), ctx)?;
    let branch_marker = change_branch_column(ctx)?;
    let branch_values = change_branch_values(branch_marker.clone());
    let expanded = LogicalPlanNode::new(
        LogicalPlanKind::Join(LogicalJoinNode {
            join_type: JoinKind::Cross,
            condition: None,
        }),
        vec![join, branch_values],
        None,
    );
    let expanded_outputs = plan_output_columns(&expanded)?;
    let branch_marker = find_output_column_by_id(&expanded_outputs, branch_marker.column_id)?;
    let old_row_id_join = find_output_column_by_id(&expanded_outputs, old_row_id.column_id)?;
    let retraction_count = retraction_count_state_column(layout)?;
    let merged_count = merged_state_expr(
        retraction_count,
        &expanded_outputs,
        &delta_outputs,
        &old_outputs,
    )?;
    let delete_predicate = bool_and(
        branch_marker_eq(branch_marker, CHANGE_BRANCH_DELETE),
        TypedExpr {
            kind: ExprKind::IsNull {
                expr: Box::new(column_ref(old_row_id_join)),
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
    );
    let insert_predicate = bool_and(
        branch_marker_eq(branch_marker, CHANGE_BRANCH_INSERT),
        TypedExpr {
            kind: ExprKind::UnaryOp {
                op: UnOp::Not,
                expr: Box::new(TypedExpr {
                    kind: ExprKind::FunctionCall {
                        name: "state_all_zero".to_string(),
                        args: vec![merged_count],
                        distinct: false,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        },
    );
    let filtered = LogicalPlanNode::new(
        LogicalPlanKind::Filter(PlanFilterNode {
            predicate: bool_or(delete_predicate, insert_predicate),
        }),
        vec![expanded],
        None,
    );
    let filtered_outputs = plan_output_columns(&filtered)?;
    aggregate_change_stream_project(
        filtered,
        &filtered_outputs,
        &delta_outputs,
        &old_outputs,
        &output_columns,
        branch_scope.as_ref(),
        layout,
    )
}

const CHANGE_BRANCH_DELETE: i8 = 0;
const CHANGE_BRANCH_INSERT: i8 = 1;

fn change_branch_column(ctx: &RewriteContext) -> Result<OutputColumn, String> {
    allocate_imv_output_column(ctx, "__imv_change_branch", DataType::Int8, false, true)
}

fn change_branch_values(column: OutputColumn) -> LogicalPlanNode {
    LogicalPlanNode::new(
        LogicalPlanKind::Values(PlanValuesNode {
            rows: vec![
                vec![tinyint_literal(CHANGE_BRANCH_DELETE)],
                vec![tinyint_literal(CHANGE_BRANCH_INSERT)],
            ],
            columns: vec![column],
        }),
        Vec::new(),
        None,
    )
}

fn branch_marker_eq(branch_marker: &OutputColumn, branch: i8) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(column_ref(branch_marker)),
            op: BinOp::Eq,
            right: Box::new(tinyint_literal(branch)),
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn tinyint_literal(value: i8) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(value as i64)),
                data_type: DataType::Int64,
                nullable: false,
            }),
            target: DataType::Int8,
        },
        data_type: DataType::Int8,
        nullable: false,
    }
}

fn bool_and(left: TypedExpr, right: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op: BinOp::And,
            right: Box::new(right),
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn bool_or(left: TypedExpr, right: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op: BinOp::Or,
            right: Box::new(right),
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn delta_state_with_row_id(
    signed_delta: LogicalPlanNode,
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
    ctx: &RewriteContext,
) -> Result<LogicalPlanNode, String> {
    let delta_outputs = plan_output_columns(&signed_delta)?;
    let mut row_id_args = Vec::with_capacity(layout.group_key_source_indexes.len());
    for &visible_source_index in &layout.group_key_source_indexes {
        let visible = layout.visible_columns.get(visible_source_index).ok_or_else(|| {
            format!(
                "Iceberg IMV aggregate rewrite group key visible source index {visible_source_index} out of range"
            )
        })?;
        row_id_args.push(column_ref(find_output_column_by_name(
            &delta_outputs,
            &visible.name,
        )?));
    }

    let row_id_name = layout.row_id_column.column.name.clone();
    let row_id_column_id = allocate_imv_column(ctx, &row_id_name, DataType::Utf8, false)?;
    let mut items = Vec::with_capacity(delta_outputs.len() + 1);
    items.push(ProjectItem {
        expr: TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "mv_group_row_id".to_string(),
                args: row_id_args,
                distinct: false,
            },
            data_type: DataType::Utf8,
            nullable: false,
        },
        output_name: row_id_name,
        output_column_id: row_id_column_id,
    });
    for output in delta_outputs {
        items.push(ProjectItem {
            expr: column_ref(&output),
            output_name: output.name.clone(),
            output_column_id: output.column_id,
        });
    }

    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            items,
            output_qualifier: None,
        }),
        vec![signed_delta],
        None,
    ))
}

fn merged_state_expr(
    state_column: &crate::mv::aggregate_state::mv_agg_state::AggregateStateColumn,
    join_outputs: &[OutputColumn],
    delta_outputs: &[OutputColumn],
    old_outputs: &[OutputColumn],
) -> Result<TypedExpr, String> {
    use crate::mv::model::AggregateStateRole;

    let delta = find_output_column_by_name(delta_outputs, &state_column.name)?;
    let delta = find_output_column_by_id(join_outputs, delta.column_id)?;
    let old = find_output_column_by_name(old_outputs, &state_column.name)?;
    let old = find_output_column_by_id(join_outputs, old.column_id)?;
    match state_column.state_role {
        AggregateStateRole::Single => Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
                name: state_union_function(state_column.function)?.to_string(),
                args: vec![column_ref(old), column_ref(delta)],
                distinct: false,
            },
            data_type: DataType::Binary,
            nullable: state_column.nullable,
        }),
        AggregateStateRole::RetractionCount => Ok(TypedExpr {
            kind: ExprKind::Case {
                operand: None,
                when_then: vec![(
                    TypedExpr {
                        kind: ExprKind::IsNull {
                            expr: Box::new(column_ref(old)),
                            negated: false,
                        },
                        data_type: DataType::Boolean,
                        nullable: false,
                    },
                    column_ref(delta),
                )],
                else_expr: Some(Box::new(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(column_ref(old)),
                        op: BinOp::Add,
                        right: Box::new(column_ref(delta)),
                    },
                    data_type: state_column.data_type.clone(),
                    nullable: false,
                })),
            },
            data_type: state_column.data_type.clone(),
            nullable: false,
        }),
    }
}

fn aggregate_change_stream_project(
    input: LogicalPlanNode,
    input_outputs: &[OutputColumn],
    delta_outputs: &[OutputColumn],
    old_outputs: &[OutputColumn],
    output_columns: &[OutputColumn],
    branch_scope: Option<&crate::sql::planner::table::BranchScope>,
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
) -> Result<LogicalPlanNode, String> {
    let branch_marker = find_output_column_by_name(input_outputs, "__imv_change_branch")?;
    let mut items = Vec::with_capacity(output_columns.len());
    for output in output_columns {
        if output.name.eq_ignore_ascii_case(ImvActionColumn::NAME) {
            items.push(ProjectItem {
                expr: branch_case_expr(
                    branch_marker,
                    tinyint_literal(ImvActionColumn::DELETE_VALUE),
                    tinyint_literal(ImvActionColumn::INSERT_VALUE),
                    DataType::Int8,
                    false,
                ),
                output_name: output.name.clone(),
                output_column_id: output.column_id,
            });
            continue;
        }
        if let Some(scope) = branch_scope
            && output
                .name
                .eq_ignore_ascii_case(&scope.branch_id_column_name)
        {
            items.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(scope.branch_id as i64)),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                output_name: output.name.clone(),
                output_column_id: output.column_id,
            });
            continue;
        }

        let delete_expr =
            aggregate_delete_expr_for_output(input_outputs, old_outputs, output, layout)?;
        let insert_expr = aggregate_insert_expr_for_output(
            input_outputs,
            delta_outputs,
            old_outputs,
            output,
            layout,
        )?;
        items.push(ProjectItem {
            expr: branch_case_expr(
                branch_marker,
                delete_expr,
                insert_expr,
                output.data_type.clone(),
                output.nullable,
            ),
            output_name: output.name.clone(),
            output_column_id: output.column_id,
        });
    }

    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            items,
            output_qualifier: None,
        }),
        vec![input],
        None,
    ))
}

fn aggregate_delete_expr_for_output(
    input_outputs: &[OutputColumn],
    old_outputs: &[OutputColumn],
    output: &OutputColumn,
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
) -> Result<TypedExpr, String> {
    let row_id_name = &layout.row_id_column.column.name;
    if output.name.eq_ignore_ascii_case(row_id_name) {
        return source_expr_by_name(input_outputs, old_outputs, row_id_name);
    }

    for (visible_index, visible) in layout.visible_columns.iter().enumerate() {
        if !output.name.eq_ignore_ascii_case(&visible.name) {
            continue;
        }
        if layout
            .group_key_source_indexes
            .iter()
            .any(|&idx| idx == visible_index)
        {
            return source_expr_by_name(input_outputs, old_outputs, &visible.name);
        }
        return Ok(typed_null(output.data_type.clone()));
    }

    for state_column in &layout.state_columns {
        if output.name.eq_ignore_ascii_case(&state_column.name) {
            return source_expr_by_name(input_outputs, old_outputs, &state_column.name);
        }
    }

    if is_locator_metadata_column(&output.name) {
        return source_expr_by_name(input_outputs, old_outputs, &output.name);
    }

    Err(format!(
        "Iceberg IMV aggregate rewrite cannot project delete-side change-stream output column {}",
        output.name
    ))
}

fn aggregate_insert_expr_for_output(
    input_outputs: &[OutputColumn],
    delta_outputs: &[OutputColumn],
    old_outputs: &[OutputColumn],
    output: &OutputColumn,
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
) -> Result<TypedExpr, String> {
    let row_id_name = &layout.row_id_column.column.name;
    if output.name.eq_ignore_ascii_case(row_id_name) {
        return source_expr_by_name(input_outputs, delta_outputs, row_id_name);
    }

    for (visible_index, visible) in layout.visible_columns.iter().enumerate() {
        if !output.name.eq_ignore_ascii_case(&visible.name) {
            continue;
        }
        if layout
            .group_key_source_indexes
            .iter()
            .any(|&idx| idx == visible_index)
        {
            return source_expr_by_name(input_outputs, delta_outputs, &visible.name);
        }
        let state_column = single_state_column_for_visible(layout, visible_index)?;
        let merged_state =
            merged_state_expr(state_column, input_outputs, delta_outputs, old_outputs)?;
        let args = visible_state_args(state_column, merged_state, layout)?;
        return Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
                name: visible_state_function(state_column.function)?.to_string(),
                args,
                distinct: false,
            },
            data_type: visible.data_type.clone(),
            nullable: visible.nullable,
        });
    }

    for state_column in &layout.state_columns {
        if output.name.eq_ignore_ascii_case(&state_column.name) {
            return merged_state_expr(state_column, input_outputs, delta_outputs, old_outputs);
        }
    }

    if is_reuse_lineage_metadata_column(&output.name) {
        return source_expr_by_name(input_outputs, old_outputs, &output.name);
    }

    if is_locator_metadata_column(&output.name) {
        return Ok(typed_null(output.data_type.clone()));
    }

    Err(format!(
        "Iceberg IMV aggregate rewrite cannot project change-stream output column {}",
        output.name
    ))
}

fn typed_null(data_type: DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Null),
        data_type,
        nullable: true,
    }
}

fn source_expr_by_name(
    input_outputs: &[OutputColumn],
    source_outputs: &[OutputColumn],
    name: &str,
) -> Result<TypedExpr, String> {
    let source = find_output_column_by_name(source_outputs, name)?;
    let input_output = find_output_column_by_id(input_outputs, source.column_id)?;
    Ok(column_ref(input_output))
}

fn branch_case_expr(
    branch_marker: &OutputColumn,
    delete_expr: TypedExpr,
    insert_expr: TypedExpr,
    data_type: DataType,
    nullable: bool,
) -> TypedExpr {
    let delete_expr = branch_case_arm_expr(delete_expr, &data_type);
    let insert_expr = branch_case_arm_expr(insert_expr, &data_type);
    TypedExpr {
        kind: ExprKind::Case {
            operand: None,
            when_then: vec![(
                branch_marker_eq(branch_marker, CHANGE_BRANCH_DELETE),
                delete_expr,
            )],
            else_expr: Some(Box::new(insert_expr)),
        },
        data_type,
        nullable,
    }
}

fn branch_case_arm_expr(expr: TypedExpr, target: &DataType) -> TypedExpr {
    if !branch_case_requires_runtime_cast(target) && expr.data_type == *target {
        return expr;
    }
    let nullable = expr.nullable;
    TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(expr),
            target: target.clone(),
        },
        data_type: target.clone(),
        nullable,
    }
}

fn branch_case_requires_runtime_cast(target: &DataType) -> bool {
    matches!(target, DataType::Utf8 | DataType::LargeUtf8)
}

fn aggregate_change_stream_output_columns(
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
    branch_scope: Option<&crate::sql::planner::table::BranchScope>,
    ctx: &RewriteContext,
) -> Result<Vec<OutputColumn>, String> {
    let mut columns = Vec::with_capacity(
        1 + layout.visible_columns.len()
            + layout.state_columns.len()
            + usize::from(branch_scope.is_some())
            + 2
            + 1,
    );
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
        columns.push(allocate_imv_output_column(
            ctx,
            &column.name,
            state_shaped_state_data_type(column),
            column.nullable,
            true,
        )?);
    }
    if let Some(scope) = branch_scope {
        columns.push(allocate_imv_output_column(
            ctx,
            &scope.branch_id_column_name,
            DataType::Int32,
            false,
            true,
        )?);
    }
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

fn is_locator_metadata_column(name: &str) -> bool {
    name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_FILE_PATH_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_POS_COL)
        || is_reuse_lineage_metadata_column(name)
}

fn is_reuse_lineage_metadata_column(name: &str) -> bool {
    name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_ID_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL)
}

fn column_ref(column: &OutputColumn) -> TypedExpr {
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

fn plan_output_columns(plan: &LogicalPlanNode) -> Result<Vec<OutputColumn>, String> {
    match &plan.kind {
        LogicalPlanKind::ImvDelta(_) | LogicalPlanKind::ImvVersion(_) => {
            plan_output_columns(plan.unary_input())
        }
        _ => planner_plan_output_columns(plan),
    }
}

fn find_output_column_by_id(
    outputs: &[OutputColumn],
    column_id: ColumnId,
) -> Result<&OutputColumn, String> {
    outputs
        .iter()
        .find(|column| column.column_id == column_id)
        .ok_or_else(|| {
            format!("Iceberg IMV aggregate rewrite missing output column id {column_id:?}")
        })
}

fn single_state_column_for_visible(
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
    visible_index: usize,
) -> Result<&crate::mv::aggregate_state::mv_agg_state::AggregateStateColumn, String> {
    use crate::mv::model::AggregateStateRole;

    layout
        .state_columns
        .iter()
        .find(|column| {
            column.state_role == AggregateStateRole::Single
                && column.visible_source_index == visible_index
        })
        .ok_or_else(|| {
            format!(
                "Iceberg IMV aggregate rewrite missing single state column for visible output index {visible_index}"
            )
        })
}

fn retraction_count_state_column(
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
) -> Result<&crate::mv::aggregate_state::mv_agg_state::AggregateStateColumn, String> {
    use crate::mv::model::AggregateFunctionKind;
    use crate::mv::model::AggregateStateRole;

    layout
        .state_columns
        .iter()
        .find(|column| column.state_role == AggregateStateRole::RetractionCount)
        .or_else(|| {
            layout.state_columns.iter().find(|column| {
                column.state_role == AggregateStateRole::Single
                    && column.function == AggregateFunctionKind::Count
                    && column.count_star
            })
        })
        .ok_or_else(|| {
            "Iceberg IMV aggregate rewrite requires a retraction-count or COUNT(*) state column"
                .to_string()
        })
}

fn state_union_function(
    function: crate::mv::model::AggregateFunctionKind,
) -> Result<&'static str, String> {
    use crate::mv::model::AggregateFunctionKind;

    match function {
        AggregateFunctionKind::Count => Ok("count_state_union"),
        AggregateFunctionKind::Sum => Ok("sum_state_union"),
        AggregateFunctionKind::Avg => Ok("avg_state_union"),
        AggregateFunctionKind::Min => Ok("min_state_union"),
        AggregateFunctionKind::Max => Ok("max_state_union"),
        AggregateFunctionKind::BoolOr => Ok("bool_or_state_union"),
        AggregateFunctionKind::BoolAnd => Ok("bool_and_state_union"),
        other => Err(format!(
            "Iceberg IMV aggregate rewrite does not support state union for {other:?}"
        )),
    }
}

fn visible_state_function(
    function: crate::mv::model::AggregateFunctionKind,
) -> Result<&'static str, String> {
    use crate::mv::model::AggregateFunctionKind;

    match function {
        AggregateFunctionKind::Count => Ok("count_state_visible"),
        AggregateFunctionKind::Sum => Ok("sum_state_visible"),
        AggregateFunctionKind::Avg => Ok("avg_state_visible"),
        AggregateFunctionKind::Min => Ok("min_state_visible"),
        AggregateFunctionKind::Max => Ok("max_state_visible"),
        AggregateFunctionKind::BoolOr => Ok("bool_or_state_visible"),
        AggregateFunctionKind::BoolAnd => Ok("bool_and_state_visible"),
        other => Err(format!(
            "Iceberg IMV aggregate rewrite does not support visible state for {other:?}"
        )),
    }
}

fn visible_state_args(
    state_column: &crate::mv::aggregate_state::mv_agg_state::AggregateStateColumn,
    merged_state: TypedExpr,
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
) -> Result<Vec<TypedExpr>, String> {
    use crate::mv::model::AggregateFunctionKind;

    let visible = layout
        .visible_columns
        .get(state_column.visible_source_index)
        .ok_or_else(|| {
            format!(
                "Iceberg IMV aggregate rewrite visible source index {} out of range",
                state_column.visible_source_index
            )
        })?;
    if state_column.function != AggregateFunctionKind::Avg
        || !matches!(visible.data_type, DataType::Decimal128(_, _))
    {
        return Ok(vec![merged_state]);
    }
    let Some(DataType::Decimal128(_, input_scale)) = layout
        .aggregate_input_types
        .get(state_column.aggregate_index)
        .and_then(Option::as_ref)
    else {
        return Err(format!(
            "Iceberg IMV aggregate rewrite requires Decimal128 AVG input scale metadata for visible column {}",
            visible.name
        ));
    };
    Ok(vec![merged_state, int64_literal(i64::from(*input_scale))])
}

fn int64_literal(value: i64) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(value)),
        data_type: DataType::Int64,
        nullable: false,
    }
}

fn branch_scope_predicate(
    scope: &crate::sql::planner::table::BranchScope,
    outputs: &[OutputColumn],
) -> Result<TypedExpr, String> {
    let branch_column = find_output_column_by_name(outputs, &scope.branch_id_column_name)?;
    Ok(TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: branch_column.column_id,
                    qualifier: None,
                    column: branch_column.name.clone(),
                },
                data_type: branch_column.data_type.clone(),
                nullable: branch_column.nullable,
            }),
            op: BinOp::Eq,
            right: Box::new(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(scope.branch_id as i64)),
                data_type: branch_column.data_type.clone(),
                nullable: false,
            }),
        },
        data_type: DataType::Boolean,
        nullable: false,
    })
}

fn aggregate_old_state_passthrough_items(
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
    outputs: &[OutputColumn],
) -> Result<Vec<ProjectItem>, String> {
    let mut names = Vec::with_capacity(
        1 + layout.group_key_source_indexes.len() + layout.state_columns.len() + 4,
    );
    push_unique_name(&mut names, &layout.row_id_column.column.name);
    for &visible_source_index in &layout.group_key_source_indexes {
        let visible = layout.visible_columns.get(visible_source_index).ok_or_else(|| {
            format!(
                "Iceberg IMV aggregate rewrite group key visible source index {visible_source_index} out of range"
            )
        })?;
        push_unique_name(&mut names, &visible.name);
    }
    for state_column in &layout.state_columns {
        push_unique_name(&mut names, &state_column.name);
    }
    for name in [
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
    ] {
        push_unique_name(&mut names, name);
    }

    names
        .into_iter()
        .map(|name| {
            let source = find_output_column_by_name(outputs, &name)?;
            Ok(ProjectItem {
                expr: column_ref(source),
                output_name: source.name.clone(),
                output_column_id: source.column_id,
            })
        })
        .collect()
}

fn find_output_column_by_name<'a>(
    outputs: &'a [OutputColumn],
    name: &str,
) -> Result<&'a OutputColumn, String> {
    outputs
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| {
            format!("Iceberg IMV aggregate rewrite target-state old input is missing column {name}")
        })
}

fn is_unpartitioned_target_contract(
    schema_contract: &crate::mv::persistence::schema::MvSchemaContract,
) -> bool {
    schema_contract
        .target
        .partition
        .as_ref()
        .is_none_or(|partition| partition.fields.is_empty())
}

fn group_key_names(aggregate: &LogicalAggregateNode) -> Result<Vec<String>, String> {
    aggregate
        .group_by
        .iter()
        .map(|expr| group_key_output_name(expr, &aggregate.output_columns))
        .collect()
}

fn group_key_output_name(
    expr: &TypedExpr,
    output_columns: &[crate::sql::analysis::OutputColumn],
) -> Result<String, String> {
    if let ExprKind::ColumnRef { column_id, .. } = &expr.kind {
        let matches = output_columns
            .iter()
            .filter(|column| column.column_id == *column_id)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [column] => return Ok(column.name.clone()),
            [] => {}
            _ => {
                return Err(format!(
                    "Iceberg IMV aggregate rewrite found ambiguous GROUP BY output column id {column_id:?}"
                ));
            }
        }
    }

    let display_name = typed_expr_display_name(expr);
    let matches = output_columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(&display_name))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [column] => Ok(column.name.clone()),
        [] => Err(format!(
            "Iceberg IMV aggregate rewrite cannot map GROUP BY expression {display_name} to aggregate output column"
        )),
        _ => Err(format!(
            "Iceberg IMV aggregate rewrite found ambiguous GROUP BY output column name {display_name}"
        )),
    }
}

fn aggregate_state_names(
    ext: &ImvExtension,
    aggregate_node: &LogicalAggregateNode,
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
) -> Result<Vec<String>, String> {
    let aggregate = ext
        .mv_ctx
        .schema_contract
        .aggregate
        .as_ref()
        .ok_or_else(|| {
            "Iceberg IMV aggregate rewrite requires aggregate schema contract".to_string()
        })?;
    if aggregate.state_columns.is_empty() {
        return Err(
            "Iceberg IMV aggregate rewrite requires aggregate state columns in schema contract"
                .to_string(),
        );
    }
    if aggregate.state_columns.len() != layout.state_columns.len() {
        return Err(format!(
            "Iceberg IMV aggregate rewrite aggregate state contract/layout mismatch: contract_columns={} layout_columns={}",
            aggregate.state_columns.len(),
            layout.state_columns.len()
        ));
    }
    let single_state_count = layout
        .state_columns
        .iter()
        .filter(|column| column.state_role == crate::mv::model::AggregateStateRole::Single)
        .count();
    if single_state_count != aggregate_node.aggregates.len() {
        return Err(format!(
            "Iceberg IMV aggregate rewrite aggregate single state column count {} does not match aggregate call count {}",
            single_state_count,
            aggregate_node.aggregates.len()
        ));
    }
    for (index, (contract_column, layout_column)) in aggregate
        .state_columns
        .iter()
        .zip(&layout.state_columns)
        .enumerate()
    {
        if !contract_column
            .column_name
            .eq_ignore_ascii_case(&layout_column.name)
        {
            return Err(format!(
                "Iceberg IMV aggregate rewrite aggregate state contract/layout mismatch at index {index}: contract column {} layout column {}",
                contract_column.column_name, layout_column.name
            ));
        }
        let expected_role = match layout_column.state_role {
            crate::mv::model::AggregateStateRole::Single => {
                crate::mv::persistence::schema::AggregateStateRoleContract::Single
            }
            crate::mv::model::AggregateStateRole::RetractionCount => {
                crate::mv::persistence::schema::AggregateStateRoleContract::RetractionCount
            }
        };
        if contract_column.role != expected_role {
            return Err(format!(
                "Iceberg IMV aggregate rewrite aggregate state contract/layout mismatch at index {index}: column {} role {:?} expected {:?}",
                contract_column.column_name, contract_column.role, expected_role
            ));
        }
        match layout_column.state_role {
            crate::mv::model::AggregateStateRole::Single => {
                if !contract_column
                    .type_signature
                    .eq_ignore_ascii_case("binary")
                {
                    return Err(format!(
                        "Iceberg IMV aggregate rewrite aggregate state column {} must have binary type signature, got {}",
                        contract_column.column_name, contract_column.type_signature
                    ));
                }
                let call = aggregate_node
                    .aggregates
                    .get(layout_column.aggregate_index)
                    .ok_or_else(|| {
                        format!(
                            "Iceberg IMV aggregate rewrite aggregate state column {} references aggregate index {} but only {} aggregate calls exist",
                            contract_column.column_name,
                            layout_column.aggregate_index,
                            aggregate_node.aggregates.len()
                        )
                    })?;
                signed_state_function(&call.name)?;
            }
            crate::mv::model::AggregateStateRole::RetractionCount => {
                if !contract_column.type_signature.eq_ignore_ascii_case("long")
                    && !contract_column
                        .type_signature
                        .eq_ignore_ascii_case("bigint")
                {
                    return Err(format!(
                        "Iceberg IMV aggregate rewrite aggregate retraction count state column {} must have long type signature, got {}",
                        contract_column.column_name, contract_column.type_signature
                    ));
                }
            }
        }
    }
    Ok(aggregate
        .state_columns
        .iter()
        .map(|column| column.column_name.clone())
        .collect())
}

fn aggregate_row_id_column_name(ext: &ImvExtension) -> Result<String, String> {
    let aggregate = ext
        .mv_ctx
        .schema_contract
        .aggregate
        .as_ref()
        .ok_or_else(|| {
            "Iceberg IMV aggregate rewrite requires aggregate schema contract".to_string()
        })?;
    if aggregate.row_id_column_name.trim().is_empty() {
        return Err(
            "Iceberg IMV aggregate rewrite requires aggregate row-id column in schema contract"
                .to_string(),
        );
    }
    Ok(aggregate.row_id_column_name.clone())
}

fn target_columns(ext: &ImvExtension) -> Result<Vec<ColumnDef>, String> {
    ext.mv_ctx
        .target_schema
        .as_ref()
        .as_struct()
        .fields()
        .iter()
        .map(|field| target_column_from_field(field.as_ref()))
        .collect()
}

fn target_column_from_field(field: &NestedField) -> Result<ColumnDef, String> {
    Ok(ColumnDef {
        name: field.name.clone(),
        data_type: iceberg_type_to_arrow(field.field_type.as_ref(), &field.name)?,
        nullable: !field.required,
        write_default: field
            .write_default
            .as_ref()
            .map(|literal| {
                crate::connector::iceberg::default_value::iceberg_literal_to_column_default(
                    literal,
                    field.field_type.as_ref(),
                )
                .map_err(|e| {
                    format!(
                        "convert Iceberg IMV aggregate write-default for column `{}` failed: {e}",
                        field.name
                    )
                })
            })
            .transpose()?,
        logical_type: None,
    })
}

fn iceberg_type_to_arrow(ty: &Type, column_name: &str) -> Result<DataType, String> {
    Ok(match ty {
        Type::Primitive(primitive) => primitive_type_to_arrow(primitive, column_name)?,
        Type::Struct(struct_ty) => {
            let fields = struct_ty
                .fields()
                .iter()
                .map(|field| {
                    Ok(Arc::new(Field::new(
                        field.name.clone(),
                        iceberg_type_to_arrow(field.field_type.as_ref(), &field.name)?,
                        !field.required,
                    )))
                })
                .collect::<Result<Vec<_>, String>>()?;
            DataType::Struct(fields.into())
        }
        Type::List(list_ty) => {
            let element = list_ty.element_field.as_ref();
            DataType::List(Arc::new(Field::new(
                element.name.clone(),
                iceberg_type_to_arrow(element.field_type.as_ref(), &element.name)?,
                !element.required,
            )))
        }
        Type::Map(map_ty) => {
            let key = map_ty.key_field.as_ref();
            let value = map_ty.value_field.as_ref();
            let entries = DataType::Struct(
                vec![
                    Arc::new(Field::new(
                        key.name.clone(),
                        iceberg_type_to_arrow(key.field_type.as_ref(), &key.name)?,
                        !key.required,
                    )),
                    Arc::new(Field::new(
                        value.name.clone(),
                        iceberg_type_to_arrow(value.field_type.as_ref(), &value.name)?,
                        !value.required,
                    )),
                ]
                .into(),
            );
            DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
        }
    })
}

fn primitive_type_to_arrow(
    primitive: &PrimitiveType,
    column_name: &str,
) -> Result<DataType, String> {
    Ok(match primitive {
        PrimitiveType::Boolean => DataType::Boolean,
        PrimitiveType::Int => DataType::Int32,
        PrimitiveType::Long => DataType::Int64,
        PrimitiveType::Float => DataType::Float32,
        PrimitiveType::Double => DataType::Float64,
        PrimitiveType::Decimal { precision, scale } => {
            let precision = u8::try_from(*precision).map_err(|_| {
                format!(
                    "Iceberg IMV aggregate rewrite target column {column_name} has out-of-range decimal precision {precision}"
                )
            })?;
            let scale = i8::try_from(*scale).map_err(|_| {
                format!(
                    "Iceberg IMV aggregate rewrite target column {column_name} has out-of-range decimal scale {scale}"
                )
            })?;
            DataType::Decimal128(precision, scale)
        }
        PrimitiveType::Date => DataType::Date32,
        PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
        PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        PrimitiveType::String => DataType::Utf8,
        PrimitiveType::Binary => DataType::Binary,
        other => {
            return Err(format!(
                "Iceberg IMV aggregate rewrite does not support target column {column_name} with Iceberg type {other:?}"
            ));
        }
    })
}

fn signed_aggregate(
    aggregate: LogicalAggregateNode,
    aggregate_input: LogicalPlanNode,
    aggregate_required_output_columns: Option<HashSet<ColumnId>>,
    action_column: ColumnId,
    ctx: &RewriteContext,
    shape: &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
) -> Result<LogicalPlanNode, String> {
    let input_columns = plan_output_columns(&aggregate_input)?;
    let mut signed_calls = aggregate
        .aggregates
        .iter()
        .map(|call| {
            let call = align_aggregate_call_inputs_to_child(call, &input_columns)?;
            signed_aggregate_call(&call, action_column)
        })
        .collect::<Result<Vec<_>, String>>()?;
    let hidden_retraction_call = layout
        .state_columns
        .iter()
        .any(|column| column.state_role == crate::mv::model::AggregateStateRole::RetractionCount);
    if hidden_retraction_call {
        signed_calls.push(retraction_count_aggregate_call(action_column));
    }
    let input = if plan_contains_imv_marker(&aggregate_input) {
        thread_delta_action_column(aggregate_input, action_column)?
    } else {
        LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(action_column),
                branch_scope: None,
            }),
            vec![aggregate_input],
            None,
        )
    };
    let aggregate_output_columns = signed_aggregate_output_columns(
        &aggregate.group_by,
        shape,
        layout,
        ctx,
        &mut signed_calls,
    )?;
    let project_items = signed_aggregate_project_items(
        &aggregate.group_by,
        shape,
        layout,
        ctx,
        &aggregate_output_columns,
        &signed_calls,
    )?;
    let signed_aggregate = LogicalPlanNode::new(
        LogicalPlanKind::Aggregate(LogicalAggregateNode {
            group_by: aggregate.group_by,
            aggregates: signed_calls,
            output_columns: aggregate_output_columns,
            already_pushed: aggregate.already_pushed,
        }),
        vec![input],
        aggregate_required_output_columns,
    );
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            items: project_items,
            output_qualifier: None,
        }),
        vec![signed_aggregate],
        None,
    ))
}

fn existing_delta_action_column(plan: &LogicalPlanNode) -> Result<Option<ColumnId>, String> {
    fn merge_action(found: &mut Option<ColumnId>, action: Option<ColumnId>) -> Result<(), String> {
        let Some(action) = action else {
            return Ok(());
        };
        match found {
            Some(existing) if *existing != action => Err(format!(
                "Iceberg IMV aggregate rewrite found conflicting delta action columns: {existing:?} and {action:?}"
            )),
            Some(_) => Ok(()),
            None => {
                *found = Some(action);
                Ok(())
            }
        }
    }

    fn visit(plan: &LogicalPlanNode, found: &mut Option<ColumnId>) -> Result<(), String> {
        match &plan.kind {
            LogicalPlanKind::ImvDelta(node) => {
                merge_action(found, node.action_column)?;
            }
            _ => {}
        }
        for child in &plan.children {
            visit(child, found)?;
        }
        Ok(())
    }

    let mut found = None;
    visit(plan, &mut found)?;
    Ok(found)
}

fn thread_delta_action_column(
    mut plan: LogicalPlanNode,
    action_column: ColumnId,
) -> Result<LogicalPlanNode, String> {
    if let LogicalPlanKind::ImvDelta(node) = &mut plan.kind {
        if let Some(existing) = node.action_column
            && existing != action_column
        {
            return Err(format!(
                "Iceberg IMV aggregate rewrite found delta action column {existing:?}, expected {action_column:?}"
            ));
        }
        node.action_column = Some(action_column);
    }
    let children = std::mem::take(&mut plan.children)
        .into_iter()
        .map(|child| thread_delta_action_column(child, action_column))
        .collect::<Result<Vec<_>, _>>()?;
    plan.children = children;
    Ok(plan)
}

fn take_unary_child(children: &mut Vec<LogicalPlanNode>) -> LogicalPlanNode {
    assert_eq!(children.len(), 1, "expected one logical plan child");
    children.remove(0)
}

fn align_aggregate_call_inputs_to_child(
    call: &AggregateCall,
    input_columns: &[crate::sql::analysis::OutputColumn],
) -> Result<AggregateCall, String> {
    let mut call = call.clone();
    for arg in &mut call.args {
        align_expr_column_refs_to_child(arg, input_columns)?;
    }
    for sort in &mut call.order_by {
        align_expr_column_refs_to_child(&mut sort.expr, input_columns)?;
    }
    Ok(call)
}

fn align_expr_column_refs_to_child(
    expr: &mut TypedExpr,
    input_columns: &[crate::sql::analysis::OutputColumn],
) -> Result<(), String> {
    match &mut expr.kind {
        ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } => {
            if let Some(input) = unique_input_column_by_id(input_columns, *column_id)? {
                *qualifier = None;
                *column = input.name.clone();
                expr.data_type = input.data_type.clone();
                expr.nullable = input.nullable;
                return Ok(());
            }
            if qualifier.is_some() {
                let input = unique_input_column_by_name(input_columns, column)?;
                *column_id = input.column_id;
                *qualifier = None;
                *column = input.name.clone();
                expr.data_type = input.data_type.clone();
                expr.nullable = input.nullable;
            }
            Ok(())
        }
        ExprKind::BinaryOp { left, right, .. } => {
            align_expr_column_refs_to_child(left, input_columns)?;
            align_expr_column_refs_to_child(right, input_columns)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::IsTruthValue { expr, .. } => {
            align_expr_column_refs_to_child(expr, input_columns)
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            for arg in args {
                align_expr_column_refs_to_child(arg, input_columns)?;
            }
            Ok(())
        }
        ExprKind::InList { expr, list, .. } => {
            align_expr_column_refs_to_child(expr, input_columns)?;
            for item in list {
                align_expr_column_refs_to_child(item, input_columns)?;
            }
            Ok(())
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            align_expr_column_refs_to_child(expr, input_columns)?;
            align_expr_column_refs_to_child(low, input_columns)?;
            align_expr_column_refs_to_child(high, input_columns)
        }
        ExprKind::Like { expr, pattern, .. } => {
            align_expr_column_refs_to_child(expr, input_columns)?;
            align_expr_column_refs_to_child(pattern, input_columns)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                align_expr_column_refs_to_child(operand, input_columns)?;
            }
            for (when, then) in when_then {
                align_expr_column_refs_to_child(when, input_columns)?;
                align_expr_column_refs_to_child(then, input_columns)?;
            }
            if let Some(else_expr) = else_expr {
                align_expr_column_refs_to_child(else_expr, input_columns)?;
            }
            Ok(())
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                align_expr_column_refs_to_child(arg, input_columns)?;
            }
            for item in partition_by {
                align_expr_column_refs_to_child(item, input_columns)?;
            }
            for sort in order_by {
                align_expr_column_refs_to_child(&mut sort.expr, input_columns)?;
            }
            Ok(())
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            align_expr_column_refs_to_child(body, input_columns)
        }
        ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => Ok(()),
    }
}

fn unique_input_column_by_id(
    input_columns: &[crate::sql::analysis::OutputColumn],
    column_id: ColumnId,
) -> Result<Option<&crate::sql::analysis::OutputColumn>, String> {
    if column_id == ColumnId::UNSET {
        return Ok(None);
    }
    let matches = input_columns
        .iter()
        .filter(|column| column.column_id == column_id)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(None),
        [column] => Ok(Some(*column)),
        _ => Err(format!(
            "Iceberg IMV aggregate rewrite found ambiguous child output column id {column_id:?}"
        )),
    }
}

fn unique_input_column_by_name<'a>(
    input_columns: &'a [crate::sql::analysis::OutputColumn],
    name: &str,
) -> Result<&'a crate::sql::analysis::OutputColumn, String> {
    let matches = input_columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [column] => Ok(*column),
        [] => Err(format!(
            "Iceberg IMV aggregate rewrite cannot map qualified aggregate input {name} to child output"
        )),
        _ => Err(format!(
            "Iceberg IMV aggregate rewrite found ambiguous child output column name {name}"
        )),
    }
}

fn signed_aggregate_output_columns(
    group_by: &[TypedExpr],
    shape: &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
    ctx: &RewriteContext,
    signed_calls: &mut [AggregateCall],
) -> Result<Vec<crate::sql::analysis::OutputColumn>, String> {
    let mut output_columns = Vec::with_capacity(shape.group_keys.len() + signed_calls.len());
    for (group_key_index, &visible_source_index) in
        layout.group_key_source_indexes.iter().enumerate()
    {
        let visible = layout.visible_columns.get(visible_source_index).ok_or_else(|| {
            format!(
                "Iceberg IMV aggregate rewrite group key source index {visible_source_index} out of range"
            )
        })?;
        let group_expr = group_by.get(group_key_index).ok_or_else(|| {
            format!("Iceberg IMV aggregate rewrite group key index {group_key_index} out of range")
        })?;
        let column_id = match &group_expr.kind {
            ExprKind::ColumnRef { column_id, .. } => *column_id,
            _ => allocate_imv_column(
                ctx,
                &visible.name,
                visible.data_type.clone(),
                visible.nullable,
            )?,
        };
        output_columns.push(crate::sql::analysis::OutputColumn {
            column_id,
            name: visible.name.clone(),
            data_type: visible.data_type.clone(),
            nullable: visible.nullable,
            is_internal: false,
        });
    }
    for (state_index, state_column) in layout.state_columns.iter().enumerate() {
        let data_type = state_shaped_state_data_type(state_column);
        let output = allocate_imv_output_column(ctx, &state_column.name, data_type, false, true)?;
        let column_id = output.column_id;
        if let Some(call) = signed_calls.get_mut(state_index) {
            call.output_column_id = column_id;
        }
        output_columns.push(output);
        if state_index >= signed_calls.len() {
            return Err(format!(
                "Iceberg IMV aggregate rewrite missing signed state call for {}",
                state_column.name
            ));
        }
    }
    Ok(output_columns)
}

fn signed_aggregate_project_items(
    group_by: &[TypedExpr],
    shape: &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
    ctx: &RewriteContext,
    aggregate_output_columns: &[OutputColumn],
    signed_calls: &[AggregateCall],
) -> Result<Vec<crate::sql::analysis::ProjectItem>, String> {
    use crate::mv::model::VisibleAggregateOutput;

    let mut items = Vec::with_capacity(shape.visible_outputs.len() + layout.state_columns.len());
    for output in &shape.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(group_key_index) => {
                let _group_expr = group_by.get(*group_key_index).ok_or_else(|| {
                    format!(
                        "Iceberg IMV aggregate rewrite group key index {group_key_index} out of range"
                    )
                })?;
                let visible_source_index = *layout
                    .group_key_source_indexes
                    .get(*group_key_index)
                    .ok_or_else(|| {
                        format!(
                            "Iceberg IMV aggregate rewrite group key index {group_key_index} out of range"
                        )
                    })?;
                let visible = layout.visible_columns.get(visible_source_index).ok_or_else(|| {
                    format!(
                        "Iceberg IMV aggregate rewrite group key visible source index {visible_source_index} out of range"
                    )
                })?;
                let child_output =
                    aggregate_output_columns
                        .get(*group_key_index)
                        .ok_or_else(|| {
                            format!(
                                "Iceberg IMV aggregate rewrite missing signed aggregate group output at index {group_key_index}"
                            )
                        })?;
                items.push(crate::sql::analysis::ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: child_output.column_id,
                            qualifier: None,
                            column: child_output.name.clone(),
                        },
                        data_type: visible.data_type.clone(),
                        nullable: visible.nullable,
                    },
                    output_name: visible.name.clone(),
                    output_column_id: allocate_imv_column(
                        ctx,
                        &visible.name,
                        visible.data_type.clone(),
                        visible.nullable,
                    )?,
                });
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let state_column = layout
                    .state_columns
                    .iter()
                    .find(|column| {
                        column.state_role
                            == crate::mv::model::AggregateStateRole::Single
                            && column.aggregate_index == *aggregate_index
                    })
                    .ok_or_else(|| {
                        format!(
                            "Iceberg IMV aggregate rewrite missing state column for aggregate index {aggregate_index}"
                        )
                    })?;
                let call = signed_calls.get(state_column.aggregate_index).ok_or_else(|| {
                    format!(
                        "Iceberg IMV aggregate rewrite missing signed state call for aggregate index {}",
                        state_column.aggregate_index
                    )
                })?;
                let child_output =
                    signed_aggregate_child_output(aggregate_output_columns, state_column)?;
                items.push(crate::sql::analysis::ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: call.output_column_id,
                            qualifier: None,
                            column: child_output.name.clone(),
                        },
                        data_type: state_shaped_state_data_type(state_column),
                        nullable: false,
                    },
                    output_name: state_column.name.clone(),
                    output_column_id: allocate_imv_column(
                        ctx,
                        &state_column.name,
                        state_shaped_state_data_type(state_column),
                        false,
                    )?,
                });
            }
        }
    }
    for state_column in layout
        .state_columns
        .iter()
        .filter(|column| column.state_role == crate::mv::model::AggregateStateRole::RetractionCount)
    {
        let call = signed_calls.last().ok_or_else(|| {
            format!(
                "Iceberg IMV aggregate rewrite missing hidden retraction state call for {}",
                state_column.name
            )
        })?;
        let child_output = signed_aggregate_child_output(aggregate_output_columns, state_column)?;
        items.push(crate::sql::analysis::ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: call.output_column_id,
                    qualifier: None,
                    column: child_output.name.clone(),
                },
                data_type: state_shaped_state_data_type(state_column),
                nullable: false,
            },
            output_name: state_column.name.clone(),
            output_column_id: allocate_imv_column(
                ctx,
                &state_column.name,
                state_shaped_state_data_type(state_column),
                false,
            )?,
        });
    }
    Ok(items)
}

fn signed_aggregate_child_output<'a>(
    aggregate_output_columns: &'a [OutputColumn],
    state_column: &crate::mv::aggregate_state::mv_agg_state::AggregateStateColumn,
) -> Result<&'a OutputColumn, String> {
    aggregate_output_columns
        .iter()
        .find(|output| output.name.eq_ignore_ascii_case(&state_column.name))
        .ok_or_else(|| {
            format!(
                "Iceberg IMV aggregate rewrite missing signed aggregate output column {}",
                state_column.name
            )
        })
}

fn state_shaped_state_data_type(
    state_column: &crate::mv::aggregate_state::mv_agg_state::AggregateStateColumn,
) -> DataType {
    match state_column.state_role {
        crate::mv::model::AggregateStateRole::Single => DataType::Binary,
        crate::mv::model::AggregateStateRole::RetractionCount => state_column.data_type.clone(),
    }
}

fn retraction_count_aggregate_call(action_column: ColumnId) -> AggregateCall {
    AggregateCall {
        name: "sum".to_string(),
        args: vec![TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: action_column,
                qualifier: None,
                column: ImvActionColumn::NAME.to_string(),
            },
            data_type: DataType::Int8,
            nullable: false,
        }],
        distinct: false,
        result_type: DataType::Int64,
        order_by: Vec::new(),
        output_column_id: ColumnId::UNSET,
    }
}

fn signed_aggregate_call(
    call: &AggregateCall,
    action_column: ColumnId,
) -> Result<AggregateCall, String> {
    let signed_name = signed_state_function(&call.name)?;
    let value = signed_value_arg(call)?;
    Ok(AggregateCall {
        name: signed_name.to_string(),
        args: vec![signed_state_input(value, action_column)],
        distinct: false,
        result_type: DataType::Binary,
        order_by: call.order_by.clone(),
        output_column_id: ColumnId::UNSET,
    })
}

fn signed_value_arg(call: &AggregateCall) -> Result<TypedExpr, String> {
    match call.args.as_slice() {
        [] if call.name.eq_ignore_ascii_case("count") => Ok(TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        }),
        [arg] => Ok(arg.clone()),
        [] => Err(format!(
            "Iceberg IMV aggregate rewrite requires an input for aggregate function {}",
            call.name
        )),
        _ => Err(format!(
            "Iceberg IMV aggregate rewrite supports only single-argument aggregate state inputs for {}",
            call.name
        )),
    }
}

fn signed_state_input(value: TypedExpr, action_column: ColumnId) -> TypedExpr {
    let value_type = value.data_type.clone();
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "named_struct".to_string(),
            args: vec![
                string_literal("value"),
                value,
                string_literal("change_op"),
                TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: action_column,
                        qualifier: None,
                        column: ImvActionColumn::NAME.to_string(),
                    },
                    data_type: DataType::Int8,
                    nullable: false,
                },
            ],
            distinct: false,
        },
        data_type: DataType::Struct(
            vec![
                Arc::new(Field::new("value", value_type, true)),
                Arc::new(Field::new("change_op", DataType::Int8, false)),
            ]
            .into(),
        ),
        nullable: true,
    }
}

fn string_literal(value: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::String(value.to_string())),
        data_type: DataType::Utf8,
        nullable: false,
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::logical::*;
    use crate::sql::planner::payload::*;
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use super::*;
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::mv::persistence::schema::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource, BranchIdColumnContract, BranchUnionContract, MvPartitionContract,
    };
    use crate::mv::rewrite::context::IcebergMvRewriteContext;
    use crate::mv::rewrite::context::tests_support::{
        make_mv_definition, make_pin, make_ref, make_schema_contract, make_target, parse_query,
    };
    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::column_id::{ColumnId, ColumnRefFactory};
    use crate::sql::common::ImvVersionRef;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::planner::logical::{
        LogicalAggregateNode, LogicalImvDeltaNode, LogicalPlanKind,
    };
    use crate::sql::planner::optimizer_bridge::logical::{to_logical_plan, to_optimizer_expr};
    use crate::sql::planner::payload::{AggregateCall, PlanScanNode};
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

    #[test]
    fn signed_state_function_maps_supported_aggregates() {
        assert_eq!(
            signed_state_function("count").unwrap(),
            "count_state_signed"
        );
        assert_eq!(signed_state_function("sum").unwrap(), "sum_state_signed");
        assert_eq!(signed_state_function("avg").unwrap(), "avg_state_signed");
        assert_eq!(signed_state_function("min").unwrap(), "min_state_signed");
        assert_eq!(signed_state_function("max").unwrap(), "max_state_signed");
        assert_eq!(
            signed_state_function("bool_or").unwrap(),
            "bool_or_state_signed"
        );
        assert_eq!(
            signed_state_function("bool_and").unwrap(),
            "bool_and_state_signed"
        );
        assert_eq!(
            signed_state_function("every").unwrap(),
            "bool_and_state_signed"
        );
    }

    #[test]
    fn signed_state_function_rejects_unsupported_aggregate() {
        let err = signed_state_function("median").expect_err("median must be unsupported");
        assert!(
            err.contains("unsupported IMV aggregate function median"),
            "{err}"
        );
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
        build_ctx_with_state_columns(vec![
            single_state_column("binary"),
            retraction_count_state_column(),
        ])
    }

    fn build_ctx_with_state_columns(
        state_columns: Vec<AggregateStateColumnContract>,
    ) -> RewriteContext {
        build_ctx_with_state_columns_and_target_partition(state_columns, None)
    }

    fn build_ctx_with_state_columns_and_target_partition(
        state_columns: Vec<AggregateStateColumnContract>,
        target_partition: Option<MvPartitionContract>,
    ) -> RewriteContext {
        build_ctx_with_state_columns_target_partition_and_branch(
            state_columns,
            target_partition,
            None,
        )
    }

    fn build_branch_ctx() -> RewriteContext {
        build_ctx_with_state_columns_target_partition_and_branch(
            vec![
                single_state_column("binary"),
                retraction_count_state_column(),
            ],
            None,
            Some(BranchUnionContract {
                branch_id_column: BranchIdColumnContract {
                    column_name: crate::mv::persistence::schema::BRANCH_ID_COLUMN_NAME.to_string(),
                    target_field_id: 998,
                },
                branch_count: 2,
                inner_apply_key_source: ApplyKeySource::GroupRowId,
            }),
        )
    }

    fn build_ctx_with_state_columns_target_partition_and_branch(
        state_columns: Vec<AggregateStateColumnContract>,
        target_partition: Option<MvPartitionContract>,
        branch_contract: Option<BranchUnionContract>,
    ) -> RewriteContext {
        let mut mv_def = make_mv_definition();
        let mut contract = make_schema_contract();
        contract.target.hidden_apply_key.column_name = "__row_id__".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.target.hidden_apply_key.source = ApplyKeySource::GroupRowId;
        contract.target.partition = target_partition;
        contract.branch = branch_contract.clone();
        contract.aggregate = Some(AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: state_columns.clone(),
        });
        mv_def.schema_contract = Some(contract.clone());

        let mut fields = vec![
            Arc::new(NestedField::required(
                100,
                "k",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                101,
                "v",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                999,
                "__row_id__",
                Type::Primitive(PrimitiveType::String),
            )),
        ];
        if let Some(branch) = &branch_contract {
            fields.push(Arc::new(NestedField::required(
                branch.branch_id_column.target_field_id,
                branch.branch_id_column.column_name.clone(),
                Type::Primitive(PrimitiveType::Int),
            )));
        }
        for column in &state_columns {
            let primitive = if column.role == AggregateStateRoleContract::RetractionCount {
                PrimitiveType::Long
            } else {
                PrimitiveType::Binary
            };
            fields.push(Arc::new(if column.nullable {
                NestedField::optional(
                    column.target_field_id,
                    column.column_name.clone(),
                    Type::Primitive(primitive),
                )
            } else {
                NestedField::required(
                    column.target_field_id,
                    column.column_name.clone(),
                    Type::Primitive(primitive),
                )
            }));
        }
        let target_schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(fields)
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
                    "SELECT k, sum(v) AS s FROM ice.db.b GROUP BY k",
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
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        factory.borrow_mut().reserve_until(100);
        ctx.set_column_ref_factory(factory);
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx,
            annotation: ImvPlanAnnotation::default(),
        });
        ctx
    }

    fn aggregate_rewrite_test_context_with_factory() -> (
        RewriteContext,
        ImvExtension,
        crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
    ) {
        let mut ctx = build_ctx();
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        ctx.set_column_ref_factory(Rc::clone(&factory));
        let ext = ctx
            .extension::<ImvExtension>()
            .expect("build_ctx installs ImvExtension")
            .clone();
        let (_, layout) = ext
            .mv_ctx
            .aggregate_shape_and_layout_for_execution()
            .expect("aggregate test context has layout");
        (ctx, ext, layout)
    }

    fn expected_row_lineage_metadata_names() -> Vec<&'static str> {
        vec![
            crate::exec::row_position::ICEBERG_FILE_PATH_COL,
            crate::exec::row_position::ICEBERG_ROW_POS_COL,
            crate::exec::row_position::ICEBERG_ROW_ID_COL,
            crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
        ]
    }

    #[test]
    fn aggregate_change_stream_columns_are_registered_in_factory() {
        let (ctx, ext, layout) = aggregate_rewrite_test_context_with_factory();
        let columns = aggregate_change_stream_output_columns(&layout, None, &ctx)
            .expect("change-stream output columns should allocate through factory");
        let factory = ctx
            .column_ref_factory()
            .cloned()
            .expect("aggregate test context must install ColumnRefFactory");

        for column in columns {
            let meta = factory.borrow().get(column.column_id).clone();
            assert_eq!(meta.name, column.name);
            assert_eq!(meta.data_type, column.data_type);
            assert_eq!(meta.nullable, column.nullable);
        }

        assert_eq!(ext.annotation.partition, None);
    }

    #[test]
    fn visible_state_args_threads_avg_decimal_input_scale() {
        let shape = crate::mv::aggregate_state::mv_shape::classify_incremental_mv_query(
            &parse_query("select k, avg(d) as a from ice.db.b group by k"),
        )
        .expect("classify aggregate");
        let crate::mv::aggregate_state::mv_shape::IncrementalMvShape::Aggregate(shape) = shape
        else {
            panic!("expected aggregate shape");
        };
        let calls =
            crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls::from(&shape);
        let layout =
            crate::mv::aggregate_state::mv_agg_state::build_aggregate_mv_layout_with_input_types(
                &calls,
                &[
                    OutputColumn {
                        column_id: ColumnId::new_for_test(1),
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(2),
                        name: "a".to_string(),
                        data_type: DataType::Decimal128(38, 10),
                        nullable: true,
                        is_internal: false,
                    },
                ],
                &[Some(DataType::Decimal128(20, 4))],
            )
            .expect("AVG decimal layout");
        let state_column = layout
            .state_columns
            .iter()
            .find(|column| column.state_role == crate::mv::model::AggregateStateRole::Single)
            .expect("single AVG state column");
        let merged_state = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(10),
                qualifier: None,
                column: state_column.name.clone(),
            },
            data_type: DataType::Binary,
            nullable: false,
        };

        let args = visible_state_args(state_column, merged_state, &layout)
            .expect("AVG decimal visible args");

        assert_eq!(args.len(), 2);
        assert!(matches!(
            &args[1].kind,
            ExprKind::Literal(LiteralValue::Int(4))
        ));
        assert_eq!(args[1].data_type, DataType::Int64);
        assert!(!args[1].nullable);
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

    fn leaf_scan() -> LogicalPlanNode {
        let columns = vec![
            ColumnDef {
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "v".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
        ];
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: "b".to_string(),
                    columns,
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::IcebergDataFiles {
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
                        files: Vec::new(),
                        cloud_properties: BTreeMap::new(),
                        binding:
                            crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: ColumnId::new_for_test(1),
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(2),
                        name: "v".to_string(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
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

    fn aggregate_over(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_expr(1, "k")],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_expr(2, "v")],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: ColumnId::new_for_test(3),
                }],
                output_columns: vec![
                    OutputColumn {
                        column_id: ColumnId::new_for_test(1),
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(3),
                        name: "s".to_string(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                ],
                already_pushed: false,
            }),
            vec![input],
            None,
        )
    }

    fn aggregate_first_output_over(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_expr(1, "k")],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_expr(2, "v")],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: ColumnId::new_for_test(3),
                }],
                output_columns: vec![
                    OutputColumn {
                        column_id: ColumnId::new_for_test(3),
                        name: "s".to_string(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(1),
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                already_pushed: false,
            }),
            vec![input],
            None,
        )
    }

    fn aggregate_with_two_calls(input: LogicalPlanNode) -> LogicalPlanNode {
        let mut plan = aggregate_over(input);
        let LogicalPlanKind::Aggregate(node) = &mut plan.kind else {
            unreachable!()
        };
        node.aggregates.push(AggregateCall {
            name: "count".to_string(),
            args: Vec::new(),
            distinct: false,
            result_type: DataType::Int64,
            order_by: Vec::new(),
            output_column_id: ColumnId::new_for_test(4),
        });
        node.output_columns.push(OutputColumn {
            column_id: ColumnId::new_for_test(4),
            name: "c".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        });
        plan
    }

    fn expect_changed_merge(result: RewriteResult, arena: &ScalarArena) -> LogicalPlanNode {
        let plan = expect_changed_plan(result, arena);
        let _ = aggregate_change_stream_project(&plan);
        plan
    }

    fn aggregate_change_stream_project(plan: &LogicalPlanNode) -> &PlanProjectNode {
        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!(
                "expected aggregate change-stream Project, got {:?}",
                plan.kind
            );
        };
        let LogicalPlanKind::Filter(_) = &plan.unary_input().kind else {
            panic!("expected aggregate change-stream Project over Filter");
        };
        project
    }

    fn aggregate_change_stream_filter(plan: &LogicalPlanNode) -> &PlanFilterNode {
        let project = aggregate_change_stream_project(plan);
        let _ = project;
        let filter_plan = plan.unary_input();
        let LogicalPlanKind::Filter(filter) = &filter_plan.kind else {
            panic!("expected aggregate change-stream Filter");
        };
        filter
    }

    fn expect_changed_plan(result: RewriteResult, arena: &ScalarArena) -> LogicalPlanNode {
        let RewriteResult::Changed(opt) = result else {
            panic!("expected Changed logical plan");
        };
        to_logical_plan(opt, arena)
    }

    fn find_target_state_scan(plan: &LogicalPlanNode) -> &PlanScanNode {
        if let LogicalPlanKind::Scan(scan) = &plan.kind
            && matches!(&scan.table.source, ScanSource::IcebergMvTargetState(_))
        {
            return scan;
        }
        plan.children
            .iter()
            .find_map(|child| {
                if contains_target_state_scan(child) {
                    Some(find_target_state_scan(child))
                } else {
                    None
                }
            })
            .expect("expected target-state scan")
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

    fn find_signed_delta_project(plan: &LogicalPlanNode) -> &LogicalPlanNode {
        if let LogicalPlanKind::Project(_) = &plan.kind
            && matches!(
                &plan.unary_input().kind,
                LogicalPlanKind::Aggregate(LogicalAggregateNode { aggregates, .. })
                    if aggregates.iter().any(|call| call.name.ends_with("_state_signed"))
            )
        {
            return plan;
        }
        plan.children
            .iter()
            .find_map(|child| {
                if contains_signed_delta_project(child) {
                    Some(find_signed_delta_project(child))
                } else {
                    None
                }
            })
            .expect("expected signed aggregate projection")
    }

    fn contains_signed_delta_project(plan: &LogicalPlanNode) -> bool {
        matches!(
            &plan.kind,
            LogicalPlanKind::Project(_)
                if matches!(
                    &plan.unary_input().kind,
                    LogicalPlanKind::Aggregate(LogicalAggregateNode { aggregates, .. })
                        if aggregates.iter().any(|call| call.name.ends_with("_state_signed"))
                )
        ) || plan.children.iter().any(contains_signed_delta_project)
    }

    fn find_branch_scoped_old_input(
        plan: &LogicalPlanNode,
    ) -> (&PlanProjectNode, &PlanFilterNode, &PlanScanNode) {
        if let LogicalPlanKind::Project(project) = &plan.kind {
            let filter_plan = plan.unary_input();
            if let LogicalPlanKind::Filter(filter) = &filter_plan.kind
                && let LogicalPlanKind::Scan(scan) = &filter_plan.unary_input().kind
                && matches!(&scan.table.source, ScanSource::IcebergMvTargetState(_))
            {
                return (project, filter, scan);
            }
        }
        plan.children
            .iter()
            .find_map(|child| {
                if contains_branch_scoped_old_input(child) {
                    Some(find_branch_scoped_old_input(child))
                } else {
                    None
                }
            })
            .expect("expected branch-scoped old input")
    }

    fn contains_branch_scoped_old_input(plan: &LogicalPlanNode) -> bool {
        matches!(
            &plan.kind,
            LogicalPlanKind::Project(_)
                if matches!(
                    &plan.unary_input().kind,
                    LogicalPlanKind::Filter(_)
                ) && matches!(
                    &plan.unary_input().unary_input().kind,
                    LogicalPlanKind::Scan(PlanScanNode {
                        table: TableDef {
                            source: ScanSource::IcebergMvTargetState(_),
                            ..
                        },
                        ..
                    })
                )
        ) || plan.children.iter().any(contains_branch_scoped_old_input)
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

    fn join_expanded_input() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![
                    OutputColumn {
                        column_id: ColumnId::new_for_test(1),
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(2),
                        name: "v".to_string(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                ],
            }),
            vec![
                LogicalPlanNode::new(
                    LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                        is_root: false,
                        action_column: Some(ColumnId::new_for_test(100)),
                        branch_scope: None,
                    }),
                    vec![leaf_scan()],
                    None,
                ),
                LogicalPlanNode::new(
                    LogicalPlanKind::ImvVersion(LogicalImvVersionNode {
                        version_ref: ImvVersionRef::from_snapshot(),
                    }),
                    vec![leaf_scan()],
                    None,
                ),
            ],
            None,
        )
    }

    #[test]
    fn rewrite_aggregate_state_matches_only_root_delta_over_aggregate() {
        let rule = RewriteAggregateStateRule;
        let ctx = build_ctx();
        let arena_rc = ctx.scalar_arena();
        let expr1 = to_optimizer_expr(
            &delta(aggregate_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        assert!(rule.matches(&expr1, &ctx));
        let expr2 = to_optimizer_expr(&aggregate_over(leaf_scan()), &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr2, &ctx));
        let nested_delta = LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: None,
                branch_scope: None,
            }),
            vec![aggregate_over(leaf_scan())],
            None,
        );
        let expr3 = to_optimizer_expr(&nested_delta, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr3, &ctx));
    }

    #[test]
    fn rewrite_aggregate_state_rejects_empty_group_by() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let mut aggregate_plan = aggregate_over(leaf_scan());
        let LogicalPlanKind::Aggregate(aggregate) = &mut aggregate_plan.kind else {
            unreachable!()
        };
        aggregate.group_by.clear();
        aggregate
            .output_columns
            .retain(|column| column.column_id == ColumnId::new_for_test(3));
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&delta(aggregate_plan), &mut arena_rc.borrow_mut());
        let err = rule
            .apply(expr, &mut ctx)
            .expect_err("empty GROUP BY must fail");
        assert_eq!(
            err,
            "Iceberg IMV aggregate rewrite requires at least one GROUP BY key"
        );
    }

    #[test]
    fn rewrite_aggregate_state_rejects_distinct_aggregate() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let mut aggregate_plan = aggregate_over(leaf_scan());
        let LogicalPlanKind::Aggregate(aggregate) = &mut aggregate_plan.kind else {
            unreachable!()
        };
        aggregate.aggregates[0].distinct = true;
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&delta(aggregate_plan), &mut arena_rc.borrow_mut());
        let err = rule
            .apply(expr, &mut ctx)
            .expect_err("distinct aggregate must fail");
        assert_eq!(
            err,
            "Iceberg IMV aggregate rewrite does not support SELECT DISTINCT"
        );
    }

    #[test]
    fn rewrite_aggregate_state_builds_state_merge_with_signed_delta() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(
            &delta(aggregate_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        let result = rule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite must succeed");
        let changed = expect_changed_merge(result, &arena_rc.borrow());

        let old_scan = find_target_state_scan(&changed);
        let ScanSource::IcebergMvTargetState(target_state) = &old_scan.table.source else {
            panic!("expected IcebergMvTargetState source");
        };
        assert_eq!(target_state.fqn(), "tgt.db.mv");
        assert_eq!(target_state.group_key_names, vec!["k"]);
        assert_eq!(
            target_state.aggregate_state_names,
            vec!["__agg_state_s", "__agg_state___ivm_row_count"]
        );
        assert_eq!(
            old_scan
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "__row_id__",
                "k",
                "__agg_state_s",
                "__agg_state___ivm_row_count",
                "_file",
                "_pos",
                "_row_id",
                "_last_updated_sequence_number",
            ]
        );
        assert_eq!(
            old_scan
                .required_columns
                .as_ref()
                .map(|columns| columns.iter().map(String::as_str).collect::<Vec<_>>()),
            Some(vec![
                "k",
                "v",
                "__row_id__",
                "__agg_state_s",
                "__agg_state___ivm_row_count",
                "_file",
                "_pos",
                "_row_id",
                "_last_updated_sequence_number",
            ])
        );
        assert_eq!(
            old_scan
                .table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            expected_row_lineage_metadata_names()
        );

        let delta_input = find_signed_delta_project(&changed);
        let LogicalPlanKind::Project(project) = &delta_input.kind else {
            panic!("expected signed aggregate projection delta input");
        };
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["k", "__agg_state_s", "__agg_state___ivm_row_count"]
        );
        let signed_aggregate_plan = delta_input.unary_input();
        let LogicalPlanKind::Aggregate(signed_aggregate) = &signed_aggregate_plan.kind else {
            panic!("expected signed aggregate under projection");
        };
        assert!(matches!(
            &signed_aggregate_plan.unary_input().kind,
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode { is_root: false, .. })
        ));
        assert_eq!(
            signed_aggregate
                .aggregates
                .iter()
                .map(|call| call.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sum_state_signed", "sum"]
        );
        for item in &project.items {
            let ExprKind::ColumnRef {
                column_id, column, ..
            } = &item.expr.kind
            else {
                panic!("expected signed aggregate Project item to reference child output");
            };
            assert_ne!(*column_id, ColumnId::UNSET);
            assert!(
                signed_aggregate.output_columns.iter().any(|output| {
                    output.column_id == *column_id && output.name.eq_ignore_ascii_case(column)
                }),
                "Project item {} must reference a signed aggregate child output by id",
                item.output_name
            );
        }
        assert_eq!(signed_aggregate.output_columns[1].name, "__agg_state_s");
        assert_eq!(
            signed_aggregate.output_columns[1].data_type,
            DataType::Binary
        );
        let args = &signed_aggregate.aggregates[0].args;
        assert_eq!(args.len(), 1);
        let ExprKind::FunctionCall {
            name,
            args: struct_args,
            ..
        } = &args[0].kind
        else {
            panic!("expected named_struct signed input");
        };
        assert_eq!(name, "named_struct");
        assert!(matches!(
            &struct_args[0].kind,
            ExprKind::Literal(LiteralValue::String(name)) if name == "value"
        ));
        assert!(matches!(
            &struct_args[2].kind,
            ExprKind::Literal(LiteralValue::String(name)) if name == "change_op"
        ));
        assert!(matches!(
            &struct_args[3].kind,
            ExprKind::ColumnRef { column, .. } if column == "__change_op"
        ));
    }

    #[test]
    fn rewrite_aggregate_state_builds_relational_change_stream() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(
            &delta(aggregate_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        let result = rule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite must succeed");
        let changed = expect_changed_plan(result, &arena_rc.borrow());

        assert_eq!(
            count_plan_nodes(&changed, |plan| matches!(
                &plan.kind,
                LogicalPlanKind::Join(LogicalJoinNode {
                    join_type: JoinKind::LeftOuter,
                    ..
                })
            )),
            1,
            "aggregate merge join must not be cloned into both change-stream branches"
        );
        assert_eq!(
            count_plan_nodes(&changed, |plan| matches!(
                &plan.kind,
                LogicalPlanKind::Join(LogicalJoinNode {
                    join_type: JoinKind::Cross,
                    ..
                })
            )),
            1,
            "change-stream branch marker must expand rows through one cross join"
        );
        assert_eq!(
            count_plan_nodes(&changed, |plan| matches!(
                &plan.kind,
                LogicalPlanKind::Values(_)
            )),
            1,
            "change-stream branch marker must come from one VALUES source"
        );
        assert_eq!(
            count_plan_nodes(&changed, |plan| matches!(
                &plan.kind,
                LogicalPlanKind::CTEConsume(_)
            )),
            0,
            "aggregate change-stream must not introduce a CTE fragment boundary"
        );
        assert!(matches!(
            aggregate_change_stream_filter(&changed).predicate.kind,
            ExprKind::BinaryOp { .. }
        ));
        assert!(
            expr_contains_function(
                &aggregate_change_stream_filter(&changed).predicate,
                "state_all_zero"
            ),
            "change-stream filter must guard INSERT branches with state_all_zero"
        );
        let project = aggregate_change_stream_project(&changed);
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "__row_id__",
                "k",
                "v",
                "__agg_state_s",
                "__agg_state___ivm_row_count",
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
                "__change_op"
            ]
        );
        for item in &project.items {
            if branch_case_requires_runtime_cast(&item.expr.data_type) {
                assert_branch_case_arms_cast_to_output_type(item);
            }
        }
    }

    #[test]
    fn rewrite_aggregate_state_locator_case_arms_keep_old_positions_and_null_inserts() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(
            &delta(aggregate_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        let result = rule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite must succeed");
        let changed = expect_changed_plan(result, &arena_rc.borrow());

        let project = aggregate_change_stream_project(&changed);
        let filter_plan = changed.unary_input();
        let expanded_plan = filter_plan.unary_input();
        let left_join_plan = expanded_plan.left();
        let old_outputs = plan_output_columns(left_join_plan.right()).expect("old outputs");
        let old_file = find_output_column_by_name(
            &old_outputs,
            crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        )
        .expect("old file locator")
        .column_id;
        let old_pos = find_output_column_by_name(
            &old_outputs,
            crate::exec::row_position::ICEBERG_ROW_POS_COL,
        )
        .expect("old row position locator")
        .column_id;

        let file_item = project
            .items
            .iter()
            .find(|item| {
                item.output_name
                    .eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_FILE_PATH_COL)
            })
            .expect("expected file locator output");
        let pos_item = project
            .items
            .iter()
            .find(|item| {
                item.output_name
                    .eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_POS_COL)
            })
            .expect("expected row position locator output");

        let (delete_file, insert_file) = case_arm_exprs(&file_item.expr);
        let ExprKind::ColumnRef { column_id, .. } = uncast_expr(delete_file).kind else {
            panic!("DELETE file locator must read old target-state _file");
        };
        assert_eq!(column_id, old_file);
        assert!(
            matches!(
                uncast_expr(insert_file).kind,
                ExprKind::Literal(LiteralValue::Null)
            ),
            "INSERT file locator must be NULL"
        );
        assert!(file_item.expr.nullable);

        let (delete_pos, insert_pos) = case_arm_exprs(&pos_item.expr);
        let ExprKind::ColumnRef { column_id, .. } = uncast_expr(delete_pos).kind else {
            panic!("DELETE row position locator must read old target-state _pos");
        };
        assert_eq!(column_id, old_pos);
        assert!(
            matches!(
                uncast_expr(insert_pos).kind,
                ExprKind::Literal(LiteralValue::Null)
            ),
            "INSERT row position locator must be NULL"
        );
        assert!(pos_item.expr.nullable);
    }

    #[test]
    fn rewrite_aggregate_state_row_id_case_arms_keep_delta_and_old_ids() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(
            &delta(aggregate_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        let result = rule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite must succeed");
        let changed = expect_changed_plan(result, &arena_rc.borrow());

        let project = aggregate_change_stream_project(&changed);
        let row_id_item = project
            .items
            .iter()
            .find(|item| item.output_name.eq_ignore_ascii_case("__row_id__"))
            .expect("expected row id output");
        let filter_plan = changed.unary_input();
        let expanded_plan = filter_plan.unary_input();
        let left_join_plan = expanded_plan.left();
        let delta_row_id = find_output_column_by_name(
            &plan_output_columns(left_join_plan.left()).expect("delta outputs"),
            "__row_id__",
        )
        .expect("delta row id")
        .column_id;
        let old_row_id = find_output_column_by_name(
            &plan_output_columns(left_join_plan.right()).expect("old outputs"),
            "__row_id__",
        )
        .expect("old row id")
        .column_id;
        let branch_marker = find_output_column_by_name(
            &plan_output_columns(expanded_plan).expect("expanded outputs"),
            "__imv_change_branch",
        )
        .expect("branch marker")
        .column_id;

        let (delete_row_id, insert_row_id) = case_arm_column_ids(&row_id_item.expr);
        assert_eq!(
            delete_row_id, old_row_id,
            "DELETE branch must use the old target-state row id"
        );
        assert_eq!(
            insert_row_id, delta_row_id,
            "INSERT branch must use the delta group row id"
        );
        assert_ne!(
            delete_row_id, branch_marker,
            "DELETE row id must not bind to the branch marker"
        );
        assert_ne!(
            insert_row_id, branch_marker,
            "INSERT row id must not bind to the branch marker"
        );
    }

    fn count_plan_nodes(
        plan: &LogicalPlanNode,
        predicate: impl Fn(&LogicalPlanNode) -> bool + Copy,
    ) -> usize {
        usize::from(predicate(plan))
            + plan
                .children
                .iter()
                .map(|child| count_plan_nodes(child, predicate))
                .sum::<usize>()
    }

    fn case_arm_column_ids(expr: &TypedExpr) -> (ColumnId, ColumnId) {
        let (delete_expr, insert_expr) = case_arm_exprs(expr);
        (
            column_id_through_cast(delete_expr),
            column_id_through_cast(insert_expr),
        )
    }

    fn case_arm_exprs(expr: &TypedExpr) -> (&TypedExpr, &TypedExpr) {
        let ExprKind::Case {
            when_then,
            else_expr,
            ..
        } = &expr.kind
        else {
            panic!("expected CASE expression");
        };
        let delete_expr = when_then
            .first()
            .map(|(_, then_expr)| then_expr)
            .expect("expected delete branch");
        let insert_expr = else_expr
            .as_deref()
            .expect("expected insert branch in CASE ELSE");
        (delete_expr, insert_expr)
    }

    fn column_id_through_cast(expr: &TypedExpr) -> ColumnId {
        let expr = uncast_expr(expr);
        let ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
            panic!("expected CASE arm to read a ColumnRef through optional Cast");
        };
        *column_id
    }

    fn uncast_expr(expr: &TypedExpr) -> &TypedExpr {
        match &expr.kind {
            ExprKind::Cast { expr, .. } => expr.as_ref(),
            _ => expr,
        }
    }

    fn assert_branch_case_arms_cast_to_output_type(item: &ProjectItem) {
        let ExprKind::Case {
            when_then,
            else_expr,
            ..
        } = &item.expr.kind
        else {
            panic!(
                "expected aggregate change-stream output {} to use CASE",
                item.output_name
            );
        };
        for (_, then_expr) in when_then {
            assert_cast_target(then_expr, &item.expr.data_type, &item.output_name);
        }
        let else_expr = else_expr
            .as_deref()
            .unwrap_or_else(|| panic!("expected CASE ELSE for {}", item.output_name));
        assert_cast_target(else_expr, &item.expr.data_type, &item.output_name);
    }

    fn assert_cast_target(expr: &TypedExpr, expected: &DataType, output_name: &str) {
        let ExprKind::Cast { target, .. } = &expr.kind else {
            panic!("expected CASE arm for {output_name} to force a cast");
        };
        assert_eq!(
            target, expected,
            "CASE arm for {output_name} must cast to the output column type"
        );
    }

    fn expr_contains_function(expr: &TypedExpr, target: &str) -> bool {
        match &expr.kind {
            ExprKind::FunctionCall { name, args, .. }
            | ExprKind::AggregateCall { name, args, .. }
            | ExprKind::WindowCall { name, args, .. } => {
                name.eq_ignore_ascii_case(target)
                    || args.iter().any(|arg| expr_contains_function(arg, target))
            }
            ExprKind::BinaryOp { left, right, .. } => {
                expr_contains_function(left, target) || expr_contains_function(right, target)
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. }
            | ExprKind::Nested(expr)
            | ExprKind::Lambda { body: expr, .. }
            | ExprKind::LambdaFunction { body: expr, .. } => expr_contains_function(expr, target),
            ExprKind::InList { expr, list, .. } => {
                expr_contains_function(expr, target)
                    || list.iter().any(|item| expr_contains_function(item, target))
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                expr_contains_function(expr, target)
                    || expr_contains_function(low, target)
                    || expr_contains_function(high, target)
            }
            ExprKind::Like { expr, pattern, .. } => {
                expr_contains_function(expr, target) || expr_contains_function(pattern, target)
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_deref()
                    .is_some_and(|expr| expr_contains_function(expr, target))
                    || when_then.iter().any(|(when, then)| {
                        expr_contains_function(when, target) || expr_contains_function(then, target)
                    })
                    || else_expr
                        .as_deref()
                        .is_some_and(|expr| expr_contains_function(expr, target))
            }
            ExprKind::ColumnRef { .. }
            | ExprKind::LambdaParamRef { .. }
            | ExprKind::Literal(_)
            | ExprKind::SubqueryPlaceholder { .. } => false,
        }
    }

    #[test]
    fn rewrite_aggregate_state_treats_empty_target_partition_contract_as_unpartitioned() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx_with_state_columns_and_target_partition(
            vec![
                single_state_column("binary"),
                retraction_count_state_column(),
            ],
            Some(MvPartitionContract {
                target_spec_id: 0,
                fields: Vec::new(),
            }),
        );
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(
            &delta(aggregate_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        let result = rule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite must succeed");
        let changed = expect_changed_merge(result, &arena_rc.borrow());
        let old_scan = find_target_state_scan(&changed);
        let ScanSource::IcebergMvTargetState(target_state) = &old_scan.table.source else {
            panic!("expected IcebergMvTargetState source");
        };

        assert_eq!(
            target_state.partition_constraint,
            IcebergMvTargetStatePartitionConstraint::Unpartitioned
        );
    }

    #[test]
    fn build_aggregate_state_merge_threads_branch_scope() {
        let ctx = build_branch_ctx();
        let ext = ctx.extension::<ImvExtension>().expect("extension").clone();
        let aggregate_plan = aggregate_over(leaf_scan());
        let LogicalPlanKind::Aggregate(aggregate) = &aggregate_plan.kind else {
            panic!("expected aggregate");
        };
        let aggregate_input = aggregate_plan.unary_input().clone();

        let merge = build_aggregate_state_merge(
            aggregate.clone(),
            aggregate_input,
            None,
            None,
            Some(crate::sql::planner::table::BranchScope {
                branch_id_column_name: crate::mv::persistence::schema::BRANCH_ID_COLUMN_NAME
                    .to_string(),
                branch_id: 1,
            }),
            &ctx,
            &ext,
        )
        .expect("branch-scoped merge builds");

        let _ = aggregate_change_stream_project(&merge);
        let (project, filter, old_scan) = find_branch_scoped_old_input(&merge);
        let ScanSource::IcebergMvTargetState(target_state) = &old_scan.table.source else {
            panic!("expected IcebergMvTargetState source");
        };
        assert!(matches!(
            &target_state.row_filter,
            IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                branch_scope: Some(scope),
                ..
            } if scope.branch_id == 1
        ));
        let branch_output =
            find_output_column_by_name(old_scan.columns.as_slice(), "__branch_id__")
                .expect("branch id output column");
        let ExprKind::BinaryOp { left, .. } = &filter.predicate.kind else {
            panic!("expected branch equality predicate");
        };
        let ExprKind::ColumnRef { column_id, .. } = &left.kind else {
            panic!("expected branch predicate to reference branch column");
        };
        assert_eq!(*column_id, branch_output.column_id);
        assert!(
            project
                .items
                .iter()
                .all(|item| !item.output_name.eq_ignore_ascii_case("__branch_id__"))
        );
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "__row_id__",
                "k",
                "__agg_state_s",
                "__agg_state___ivm_row_count",
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
            ]
        );
        for item in &project.items {
            let source = find_output_column_by_name(old_scan.columns.as_slice(), &item.output_name)
                .expect("project source column");
            let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind else {
                panic!("expected passthrough project item");
            };
            assert_eq!(*column_id, source.column_id);
            assert_eq!(item.output_column_id, source.column_id);
        }
    }

    #[test]
    fn aggregate_state_rule_threads_marker_branch_scope() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_branch_ctx();
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: true,
                action_column: None,
                branch_scope: Some(crate::sql::planner::table::BranchScope {
                    branch_id_column_name: crate::mv::persistence::schema::BRANCH_ID_COLUMN_NAME
                        .to_string(),
                    branch_id: 1,
                }),
            }),
            vec![aggregate_over(leaf_scan())],
            None,
        );
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        let changed = expect_changed_merge(
            rule.apply(expr, &mut ctx).expect("rewrite"),
            &arena_rc.borrow(),
        );
        // Branch scope manifests as Project(Filter(Scan)) on the old input.
        let _ = find_branch_scoped_old_input(&changed);
        let _ = aggregate_change_stream_project(&changed);
    }

    #[test]
    fn rewrite_aggregate_state_preserves_pre_expanded_join_delta_input() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(
            &delta(aggregate_over(join_expanded_input())),
            &mut arena_rc.borrow_mut(),
        );
        let result = rule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite must succeed");
        let changed = expect_changed_merge(result, &arena_rc.borrow());

        let delta_input = find_signed_delta_project(&changed);
        let LogicalPlanKind::Project(_) = &delta_input.kind else {
            panic!("expected signed aggregate projection delta input");
        };
        let signed_aggregate_plan = delta_input.unary_input();
        let LogicalPlanKind::Aggregate(_) = &signed_aggregate_plan.kind else {
            panic!("expected signed aggregate under projection");
        };
        assert!(
            matches!(
                &signed_aggregate_plan.unary_input().kind,
                LogicalPlanKind::Union(_)
            ),
            "pre-expanded join delta input must not be wrapped as ImvDelta(Union)"
        );
    }

    #[test]
    fn rewrite_aggregate_state_threads_allocated_action_column_into_existing_delta_marker() {
        assert_existing_delta_action_threads(None);
    }

    #[test]
    fn rewrite_aggregate_state_reuses_existing_delta_action_column() {
        assert_existing_delta_action_threads(Some(ColumnId::new_for_test(901)));
    }

    fn assert_existing_delta_action_threads(existing_action: Option<ColumnId>) {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let input = LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: existing_action,
                branch_scope: None,
            }),
            vec![leaf_scan()],
            None,
        );
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&delta(aggregate_over(input)), &mut arena_rc.borrow_mut());
        let result = rule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite must succeed");
        let changed = expect_changed_merge(result, &arena_rc.borrow());
        let delta_input = find_signed_delta_project(&changed);
        let LogicalPlanKind::Project(_) = &delta_input.kind else {
            panic!("expected signed aggregate projection delta input");
        };
        let signed_aggregate_plan = delta_input.unary_input();
        let LogicalPlanKind::Aggregate(signed_aggregate) = &signed_aggregate_plan.kind else {
            panic!("expected signed aggregate under projection");
        };
        let LogicalPlanKind::ImvDelta(delta_input) = &signed_aggregate_plan.unary_input().kind
        else {
            panic!("expected signed aggregate to reuse existing delta marker");
        };
        let action_column = delta_input
            .action_column
            .expect("existing delta marker must receive an action column");
        if let Some(existing_action) = existing_action {
            assert_eq!(action_column, existing_action);
        }

        let signed_arg = &signed_aggregate.aggregates[0].args[0];
        let ExprKind::FunctionCall {
            args: struct_args, ..
        } = &signed_arg.kind
        else {
            panic!("expected signed aggregate named_struct input");
        };
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &struct_args[3].kind
        else {
            panic!("expected signed aggregate change_op column ref");
        };
        assert_eq!(column, ImvActionColumn::NAME);
        assert_eq!(*column_id, action_column);

        let retraction_arg = &signed_aggregate.aggregates[1].args[0];
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &retraction_arg.kind
        else {
            panic!("expected retraction-count change_op column ref");
        };
        assert_eq!(column, ImvActionColumn::NAME);
        assert_eq!(*column_id, action_column);
    }

    #[test]
    fn rewrite_aggregate_state_maps_group_key_by_column_id_when_output_is_aggregate_first() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(
            &delta(aggregate_first_output_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        let result = rule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite must succeed");
        let changed = expect_changed_merge(result, &arena_rc.borrow());
        let old_scan = find_target_state_scan(&changed);
        let ScanSource::IcebergMvTargetState(target_state) = &old_scan.table.source else {
            panic!("expected IcebergMvTargetState source");
        };
        assert_eq!(target_state.group_key_names, vec!["k"]);
    }

    #[test]
    fn rewrite_aggregate_state_rejects_state_column_count_mismatch() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(
            &delta(aggregate_with_two_calls(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        let err = rule
            .apply(expr, &mut ctx)
            .expect_err("state column count mismatch must fail");
        assert!(
            err.contains("aggregate single state column count"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rewrite_aggregate_state_rejects_non_binary_state_column() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx_with_state_columns(vec![
            single_state_column("string"),
            retraction_count_state_column(),
        ]);
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(
            &delta(aggregate_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        let err = rule
            .apply(expr, &mut ctx)
            .expect_err("non-binary state column must fail");
        assert!(
            err.contains("must have binary type signature"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rewrite_aggregate_state_rejects_missing_hidden_retraction_count_state() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx_with_state_columns(vec![single_state_column("binary")]);
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(
            &delta(aggregate_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        let err = rule
            .apply(expr, &mut ctx)
            .expect_err("missing hidden retraction count state must fail");
        assert!(
            err.contains("aggregate state contract/layout mismatch"),
            "unexpected error: {err}"
        );
    }
}
