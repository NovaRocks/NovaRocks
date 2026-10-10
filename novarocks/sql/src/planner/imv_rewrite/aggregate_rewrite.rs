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
use arrow::datatypes::DataType;
use std::collections::HashSet;

use crate::analysis::{
    BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr, UnOp,
};
use crate::column_id::ColumnId;
use crate::compiler::mv_rewrite::SqlImvAggregateStateRole;
use crate::mv_refresh::AggregateFunctionKind;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::planner::imv_rewrite::annotation::ImvExtension;
use crate::planner::imv_rewrite::column_alloc::{
    allocate_imv_column, allocate_imv_output_column, declared_imv_column_value_type,
};
use crate::planner::imv_rewrite::marker::plan_contains_imv_marker;
use crate::planner::imv_rewrite::target_state::build_target_state_scan_source;
use crate::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result_typed, opt_expr_to_plan};
use crate::planner::logical::{
    LogicalAggregateNode, LogicalImvDeltaNode, LogicalJoinNode, LogicalPlanKind, LogicalPlanNode,
};
use crate::planner::payload::{
    AggregateCall, PlanFilterNode, PlanProjectNode, PlanScanNode, PlanValuesNode,
};
use crate::planner::plan_output_columns as planner_plan_output_columns;
#[cfg(test)]
use crate::planner::table::sql_mv_target_state_scan;
use crate::planner::table::{
    SqlMvTargetStatePartitionConstraint, SqlMvTargetStateRowFilter, TableDef,
};
use novarocks_types::schema::ColumnDef;

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

    fn apply(
        &self,
        expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, SqlCompileError> {
        bridge_apply_result_typed(expr, ctx, |plan, ctx| {
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
                    SqlCompileError::Compilation(
                        "RewriteAggregateState requires ImvExtension in RewriteContext".to_string(),
                    )
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
    branch_scope: Option<crate::planner::table::BranchScope>,
    ctx: &RewriteContext,
    ext: &ImvExtension,
) -> Result<LogicalPlanNode, SqlCompileError> {
    if aggregate.group_by.is_empty() {
        return Err(SqlCompileError::Compilation(
            "Iceberg IMV aggregate rewrite requires at least one GROUP BY key".to_string(),
        ));
    }
    if aggregate.aggregates.iter().any(|call| call.distinct) {
        return Err(SqlCompileError::Compilation(
            "Iceberg IMV aggregate rewrite does not support SELECT DISTINCT".to_string(),
        ));
    }

    let (aggregate_calls, aggregate_layout) = ext
        .snapshot
        .aggregate_shape_and_layout_for_execution()
        .map_err(SqlCompileError::from)?;
    let group_key_names = group_key_names(&aggregate, &ctx.control_view())?;
    let aggregate_state_names =
        aggregate_state_names(ext, &aggregate, &aggregate_layout).map_err(SqlCompileError::from)?;
    let row_id_column_name = aggregate_row_id_column_name(ext).map_err(SqlCompileError::from)?;
    let target_columns = target_columns(ext).map_err(SqlCompileError::from)?;
    let target = &ext.snapshot.target;
    let aggregate_contract = ext
        .snapshot
        .schema_contract
        .aggregate
        .as_ref()
        .ok_or_else(|| {
            "Iceberg IMV aggregate rewrite requires aggregate state contract".to_string()
        })
        .map_err(SqlCompileError::from)?;
    let physical_column_names = aggregate_layout.physical_column_names.clone();
    let partition_constraint = if is_unpartitioned_target_contract(&ext.snapshot.schema_contract) {
        SqlMvTargetStatePartitionConstraint::Unpartitioned
    } else {
        SqlMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired
    };

    let old_source = build_target_state_scan_source(
        ext.snapshot.target_binding,
        crate::planner::table::SqlTableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        },
        ext.snapshot.target_table_uuid.clone(),
        ext.snapshot.target_snapshot_id,
        aggregate_contract.state_layout_version,
        target_columns.clone(),
        group_key_names.clone(),
        aggregate_state_names.clone(),
        physical_column_names,
        row_id_column_name.clone(),
        SqlMvTargetStateRowFilter::DeltaInputRowIds {
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
    let old_input = branch_scoped_old_input(old_scan, branch_scope.clone(), &aggregate_layout)
        .map_err(SqlCompileError::from)?;

    let action_column = match action_column {
        Some(action_column) => action_column,
        None => {
            match existing_delta_action_column(&aggregate_input).map_err(SqlCompileError::from)? {
                Some(action_column) => action_column,
                None => allocate_imv_column(
                    ctx,
                    ImvActionColumn::NAME,
                    novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
                )
                .map_err(SqlCompileError::from)?,
            }
        }
    };
    let signed_aggregate = signed_aggregate(
        aggregate,
        aggregate_input,
        aggregate_required_output_columns,
        action_column,
        ctx,
        &aggregate_calls,
        &aggregate_layout,
    )
    .map_err(SqlCompileError::from)?;

    build_relational_aggregate_change_stream(
        old_input,
        signed_aggregate,
        branch_scope,
        ctx,
        &aggregate_layout,
    )
    .map_err(SqlCompileError::from)
}

#[expect(
    clippy::too_many_arguments,
    reason = "Target-state scan facts are independently validated planner inputs; grouping them would hide their distinct contracts."
)]
fn target_state_old_scan(
    target: &novarocks_types::naming::TableIdentity,
    target_columns: Vec<ColumnDef>,
    group_key_names: &[String],
    aggregate_state_names: &[String],
    row_id_column_name: &str,
    branch_scope: Option<&crate::planner::table::BranchScope>,
    old_source: crate::planner::table::ScanSource,
    ctx: &RewriteContext,
) -> Result<LogicalPlanNode, SqlCompileError> {
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
    let required_columns = old_columns
        .iter()
        .map(|column| column.column_id)
        .collect::<Vec<_>>();
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
) -> Result<Vec<OutputColumn>, SqlCompileError> {
    let mut old_columns = Vec::with_capacity(target_columns.len() + locator_metadata_columns.len());
    for column in target_columns {
        old_columns.push(
            allocate_imv_output_column(
                ctx,
                &column.name,
                declared_imv_column_value_type(column, ctx)?,
                aggregate_state_names
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&column.name))
                    || column.name.eq_ignore_ascii_case(row_id_column_name),
            )
            .map_err(SqlCompileError::from)?,
        );
    }
    for column in locator_metadata_columns {
        if old_columns
            .iter()
            .any(|existing| existing.name.eq_ignore_ascii_case(&column.name))
        {
            continue;
        }
        old_columns.push(
            allocate_imv_output_column(
                ctx,
                &column.name,
                declared_imv_column_value_type(column, ctx)?,
                true,
            )
            .map_err(SqlCompileError::from)?,
        );
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
) -> Result<Vec<OutputColumn>, SqlCompileError> {
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
                }).map_err(SqlCompileError::from)?;
            allocate_imv_output_column(ctx, &column.0.name, declared_imv_column_value_type(column.0, ctx)?, column.1
                    || aggregate_state_names
                        .iter()
                        .any(|state| state.eq_ignore_ascii_case(&column.0.name))
                    || column.0.name.eq_ignore_ascii_case(row_id_column_name)).map_err(SqlCompileError::from)
        })
        .collect()
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
            name: crate::common::ICEBERG_FILE_PATH_COL.to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: crate::common::ICEBERG_ROW_POS_COL.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: crate::common::ICEBERG_ROW_ID_COL.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: crate::common::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
    ]
}

fn branch_scoped_old_input(
    old_scan: LogicalPlanNode,
    branch_scope: Option<crate::planner::table::BranchScope>,
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
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
    branch_scope: Option<crate::planner::table::BranchScope>,
    ctx: &RewriteContext,
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
) -> Result<LogicalPlanNode, crate::compiler::SqlCompileError> {
    let old_outputs = plan_output_columns(&old_input)?;
    let delta_with_row_id = delta_state_with_row_id(signed_delta, layout, ctx)?;
    let delta_outputs = plan_output_columns(&delta_with_row_id)?;
    let row_id_name = &layout.row_id_column_name;
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
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Boolean,
                    false,
                ),
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
        ctx,
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
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        },
    );
    let state_all_zero_args = vec![merged_count];
    let state_all_zero_binding = crate::analysis::resolve_function_binding(
        ctx.function_catalog(),
        "state_all_zero",
        &state_all_zero_args,
        ctx.decimal_overflow_policy(),
        ctx.scalar_arena().borrow().constant_policy(),
        &ctx.control_view(),
    )?;
    let insert_predicate = bool_and(
        branch_marker_eq(branch_marker, CHANGE_BRANCH_INSERT),
        TypedExpr {
            kind: ExprKind::UnaryOp {
                op: UnOp::Not,
                expr: Box::new(TypedExpr {
                    kind: ExprKind::FunctionCall {
                        volatility: crate::functions::builtin_function_volatility("state_all_zero"),
                        name: "state_all_zero".to_string(),
                        args: state_all_zero_args,
                        distinct: false,
                        binding: state_all_zero_binding,
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Boolean,
                        false,
                    ),
                }),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
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
        ctx,
        filtered,
        AggregateChangeStreamProjection {
            input_outputs: &filtered_outputs,
            delta_outputs: &delta_outputs,
            old_outputs: &old_outputs,
            output_columns: &output_columns,
            branch_scope: branch_scope.as_ref(),
            layout,
        },
    )
}

const CHANGE_BRANCH_DELETE: i8 = 0;
const CHANGE_BRANCH_INSERT: i8 = 1;

fn change_branch_column(ctx: &RewriteContext) -> Result<OutputColumn, String> {
    allocate_imv_output_column(
        ctx,
        "__imv_change_branch",
        novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
        true,
    )
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
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
    }
}

fn tinyint_literal(value: i8) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(value as i64)),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            }),
            target: DataType::Int8,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
    }
}

fn bool_and(left: TypedExpr, right: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op: BinOp::And,
            right: Box::new(right),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
    }
}

fn bool_or(left: TypedExpr, right: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op: BinOp::Or,
            right: Box::new(right),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
    }
}

fn delta_state_with_row_id(
    signed_delta: LogicalPlanNode,
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
    ctx: &RewriteContext,
) -> Result<LogicalPlanNode, crate::compiler::SqlCompileError> {
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

    let row_id_name = layout.row_id_column_name.clone();
    let row_id_column_id = allocate_imv_column(
        ctx,
        &row_id_name,
        novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
    )?;
    let mut items = Vec::with_capacity(delta_outputs.len() + 1);
    let row_id_binding = crate::analysis::resolve_function_binding(
        ctx.function_catalog(),
        "mv_group_row_id",
        &row_id_args,
        ctx.decimal_overflow_policy(),
        ctx.scalar_arena().borrow().constant_policy(),
        &ctx.control_view(),
    )?;
    items.push(ProjectItem {
        expr: TypedExpr {
            kind: ExprKind::FunctionCall {
                volatility: crate::functions::builtin_function_volatility("mv_group_row_id"),
                name: "mv_group_row_id".to_string(),
                binding: row_id_binding,
                args: row_id_args,
                distinct: false,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
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
    ctx: &RewriteContext,
    state_column: &crate::compiler::mv_rewrite::SqlImvAggregateStateColumn,
    join_outputs: &[OutputColumn],
    delta_outputs: &[OutputColumn],
    old_outputs: &[OutputColumn],
) -> Result<TypedExpr, crate::compiler::SqlCompileError> {
    let delta = find_output_column_by_name(delta_outputs, &state_column.name)?;
    let delta = find_output_column_by_id(join_outputs, delta.column_id)?;
    let old = find_output_column_by_name(old_outputs, &state_column.name)?;
    let old = find_output_column_by_id(join_outputs, old.column_id)?;
    match state_column.state_role {
        SqlImvAggregateStateRole::Single
        | SqlImvAggregateStateRole::AvgSum
        | SqlImvAggregateStateRole::AvgCount => {
            let name = state_union_function(state_column)?;
            let args = vec![column_ref(old), column_ref(delta)];
            let binding = crate::analysis::resolve_function_binding(
                ctx.function_catalog(),
                name,
                &args,
                ctx.decimal_overflow_policy(),
                ctx.scalar_arena().borrow().constant_policy(),
                &ctx.control_view(),
            )?;
            Ok(TypedExpr {
                kind: ExprKind::FunctionCall {
                    volatility: crate::functions::builtin_function_volatility(name),
                    name: name.to_string(),
                    args,
                    distinct: false,
                    binding,
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Binary,
                    state_column.state_role == SqlImvAggregateStateRole::Single
                        && state_column.value_type.nullable,
                ),
            })
        }
        SqlImvAggregateStateRole::RetractionCount => Ok(TypedExpr {
            kind: ExprKind::Case {
                operand: None,
                when_then: vec![(
                    TypedExpr {
                        kind: ExprKind::IsNull {
                            expr: Box::new(column_ref(old)),
                            negated: false,
                        },
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Boolean,
                            false,
                        ),
                    },
                    column_ref(delta),
                )],
                else_expr: Some(Box::new(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(column_ref(old)),
                        op: BinOp::Add,
                        right: Box::new(column_ref(delta)),
                        decimal_overflow_policy:
                            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    },
                    value_type: novarocks_type_contract::FunctionValueType {
                        nullable: false,
                        ..state_column.value_type.clone()
                    },
                })),
            },
            value_type: novarocks_type_contract::FunctionValueType {
                nullable: false,
                ..state_column.value_type.clone()
            },
        }),
    }
}

struct AggregateChangeStreamProjection<'a> {
    input_outputs: &'a [OutputColumn],
    delta_outputs: &'a [OutputColumn],
    old_outputs: &'a [OutputColumn],
    output_columns: &'a [OutputColumn],
    branch_scope: Option<&'a crate::planner::table::BranchScope>,
    layout: &'a crate::compiler::mv_rewrite::SqlImvAggregateLayout,
}

fn aggregate_change_stream_project(
    ctx: &RewriteContext,
    input: LogicalPlanNode,
    projection: AggregateChangeStreamProjection<'_>,
) -> Result<LogicalPlanNode, crate::compiler::SqlCompileError> {
    let AggregateChangeStreamProjection {
        input_outputs,
        delta_outputs,
        old_outputs,
        output_columns,
        branch_scope,
        layout,
    } = projection;
    let branch_marker = find_output_column_by_name(input_outputs, "__imv_change_branch")?;
    let mut items = Vec::with_capacity(output_columns.len());
    for output in output_columns {
        if output.name.eq_ignore_ascii_case(ImvActionColumn::NAME) {
            items.push(ProjectItem {
                expr: branch_case_expr(
                    branch_marker,
                    tinyint_literal(ImvActionColumn::DELETE_VALUE),
                    tinyint_literal(ImvActionColumn::INSERT_VALUE),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
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
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Int32,
                        false,
                    ),
                },
                output_name: output.name.clone(),
                output_column_id: output.column_id,
            });
            continue;
        }

        let delete_expr =
            aggregate_delete_expr_for_output(input_outputs, old_outputs, output, layout)?;
        let insert_expr = aggregate_insert_expr_for_output(
            ctx,
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
                output.value_type.clone(),
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
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
) -> Result<TypedExpr, String> {
    let row_id_name = &layout.row_id_column_name;
    if output.name.eq_ignore_ascii_case(row_id_name) {
        return source_expr_by_name(input_outputs, old_outputs, row_id_name);
    }

    for (visible_index, visible) in layout.visible_columns.iter().enumerate() {
        if !output.name.eq_ignore_ascii_case(&visible.name) {
            continue;
        }
        if layout.group_key_source_indexes.contains(&visible_index) {
            return source_expr_by_name(input_outputs, old_outputs, &visible.name);
        }
        return Ok(typed_null(output.value_type.clone()));
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
    ctx: &RewriteContext,
    input_outputs: &[OutputColumn],
    delta_outputs: &[OutputColumn],
    old_outputs: &[OutputColumn],
    output: &OutputColumn,
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
) -> Result<TypedExpr, crate::compiler::SqlCompileError> {
    let row_id_name = &layout.row_id_column_name;
    if output.name.eq_ignore_ascii_case(row_id_name) {
        return source_expr_by_name(input_outputs, delta_outputs, row_id_name)
            .map_err(SqlCompileError::from);
    }

    for (visible_index, visible) in layout.visible_columns.iter().enumerate() {
        if !output.name.eq_ignore_ascii_case(&visible.name) {
            continue;
        }
        if layout.group_key_source_indexes.contains(&visible_index) {
            return source_expr_by_name(input_outputs, delta_outputs, &visible.name)
                .map_err(SqlCompileError::from);
        }
        let state_column = single_state_column_for_visible(layout, visible_index)?;
        let args = if state_column.function == AggregateFunctionKind::Avg {
            let sum_column = avg_state_column_for_visible(
                layout,
                visible_index,
                SqlImvAggregateStateRole::AvgSum,
            )?;
            let count_column = avg_state_column_for_visible(
                layout,
                visible_index,
                SqlImvAggregateStateRole::AvgCount,
            )?;
            visible_avg_state_args(
                sum_column,
                merged_state_expr(ctx, sum_column, input_outputs, delta_outputs, old_outputs)?,
                merged_state_expr(ctx, count_column, input_outputs, delta_outputs, old_outputs)?,
                layout,
            )?
        } else {
            let merged_state =
                merged_state_expr(ctx, state_column, input_outputs, delta_outputs, old_outputs)?;
            visible_state_args(state_column, merged_state, &visible.value_type)?
        };
        let name = visible_state_function(state_column.function)?;
        let binding = crate::analysis::resolve_function_binding(
            ctx.function_catalog(),
            name,
            &args,
            ctx.decimal_overflow_policy(),
            ctx.scalar_arena().borrow().constant_policy(),
            &ctx.control_view(),
        )?;
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            return Err(format!("Iceberg IMV {name} must return a scalar value").into());
        };
        if result.logical_type != visible.value_type.logical_type
            || !novarocks_type_contract::arrow_data_types_exact(
                &result.data_type,
                &visible.value_type.data_type,
            )
            || (result.nullable && !visible.value_type.nullable)
        {
            return Err(format!(
                "Iceberg IMV {name} result domain {result:?} does not match admitted visible column {} domain {:?}",
                visible.name, visible.value_type
            ).into());
        }
        let value_type = result.clone();
        return Ok(TypedExpr {
            kind: ExprKind::FunctionCall {
                volatility: crate::functions::builtin_function_volatility(name),
                name: name.to_string(),
                args,
                distinct: false,
                binding,
            },
            value_type,
        });
    }

    for state_column in &layout.state_columns {
        if output.name.eq_ignore_ascii_case(&state_column.name) {
            return merged_state_expr(ctx, state_column, input_outputs, delta_outputs, old_outputs);
        }
    }

    // Preserve row identity while updated rows inherit their actual commit sequence.
    if output
        .name
        .eq_ignore_ascii_case(crate::common::ICEBERG_ROW_ID_COL)
    {
        return source_expr_by_name(input_outputs, old_outputs, &output.name)
            .map_err(SqlCompileError::from);
    }

    if is_locator_metadata_column(&output.name) {
        return Ok(typed_null(output.value_type.clone()));
    }

    Err(format!(
        "Iceberg IMV aggregate rewrite cannot project change-stream output column {}",
        output.name
    )
    .into())
}

fn typed_null(mut value_type: novarocks_type_contract::FunctionValueType) -> TypedExpr {
    value_type.nullable = true;
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Null),
        value_type,
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
    value_type: novarocks_type_contract::FunctionValueType,
) -> TypedExpr {
    let delete_expr = branch_case_arm_expr(delete_expr, &value_type);
    let insert_expr = branch_case_arm_expr(insert_expr, &value_type);
    TypedExpr {
        kind: ExprKind::Case {
            operand: None,
            when_then: vec![(
                branch_marker_eq(branch_marker, CHANGE_BRANCH_DELETE),
                delete_expr,
            )],
            else_expr: Some(Box::new(insert_expr)),
        },
        value_type,
    }
}

fn branch_case_arm_expr(
    expr: TypedExpr,
    target: &novarocks_type_contract::FunctionValueType,
) -> TypedExpr {
    if !branch_case_requires_runtime_cast(&target.data_type)
        && expr.value_type.logical_type == target.logical_type
        && novarocks_type_contract::arrow_data_types_exact(
            &expr.value_type.data_type,
            &target.data_type,
        )
    {
        return expr;
    }
    let nullable = expr.value_type.nullable;
    TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(expr),
            target: target.data_type.clone(),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        value_type: novarocks_type_contract::FunctionValueType {
            nullable,
            ..target.clone()
        },
    }
}

fn branch_case_requires_runtime_cast(target: &DataType) -> bool {
    matches!(target, DataType::Utf8 | DataType::LargeUtf8)
}

fn aggregate_change_stream_output_columns(
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
    branch_scope: Option<&crate::planner::table::BranchScope>,
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
        &layout.row_id_column_name,
        novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
        true,
    )?);
    for column in &layout.visible_columns {
        columns.push(allocate_imv_output_column(
            ctx,
            &column.name,
            column.value_type.clone(),
            false,
        )?);
    }
    for column in &layout.state_columns {
        columns.push(allocate_imv_output_column(
            ctx,
            &column.name,
            state_shaped_state_value_type(column),
            true,
        )?);
    }
    if let Some(scope) = branch_scope {
        columns.push(allocate_imv_output_column(
            ctx,
            &scope.branch_id_column_name,
            novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
            true,
        )?);
    }
    // A row-delta publication's writer input is the provider's signed shape:
    // the target's own columns and the v3 lineage they carry forward, then the
    // `_file`/`_pos` row identity. The provider names each branch's columns by
    // their position in that shape and the router reads this producer at those
    // positions, so these four must stand in the signed order -- swapping the
    // pairs hands the delete branch a row id where it expects a file path.
    columns.push(allocate_imv_output_column(
        ctx,
        crate::common::ICEBERG_ROW_ID_COL,
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        true,
    )?);
    columns.push(allocate_imv_output_column(
        ctx,
        crate::common::ICEBERG_LAST_UPDATED_SEQ_COL,
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        true,
    )?);
    columns.push(allocate_imv_output_column(
        ctx,
        crate::common::ICEBERG_FILE_PATH_COL,
        novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
        true,
    )?);
    columns.push(allocate_imv_output_column(
        ctx,
        crate::common::ICEBERG_ROW_POS_COL,
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        true,
    )?);
    columns.push(allocate_imv_output_column(
        ctx,
        ImvActionColumn::NAME,
        novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
        true,
    )?);
    Ok(columns)
}

fn is_locator_metadata_column(name: &str) -> bool {
    name.eq_ignore_ascii_case(crate::common::ICEBERG_FILE_PATH_COL)
        || name.eq_ignore_ascii_case(crate::common::ICEBERG_ROW_POS_COL)
        || is_reuse_lineage_metadata_column(name)
}

fn is_reuse_lineage_metadata_column(name: &str) -> bool {
    name.eq_ignore_ascii_case(crate::common::ICEBERG_ROW_ID_COL)
        || name.eq_ignore_ascii_case(crate::common::ICEBERG_LAST_UPDATED_SEQ_COL)
}

fn column_ref(column: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: column.column_id,
            qualifier: None,
            column: column.name.clone(),
        },
        value_type: column.value_type.clone(),
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
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
    visible_index: usize,
) -> Result<&crate::compiler::mv_rewrite::SqlImvAggregateStateColumn, String> {
    layout
        .state_columns
        .iter()
        .find(|column| {
            (column.state_role == SqlImvAggregateStateRole::Single
                || column.state_role == SqlImvAggregateStateRole::AvgSum)
                && column.visible_source_index == visible_index
        })
        .ok_or_else(|| {
            format!(
                "Iceberg IMV aggregate rewrite missing state column for visible output index {visible_index}"
            )
        })
}

fn avg_state_column_for_visible(
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
    visible_index: usize,
    role: SqlImvAggregateStateRole,
) -> Result<&crate::compiler::mv_rewrite::SqlImvAggregateStateColumn, String> {
    layout
        .state_columns
        .iter()
        .find(|column| column.state_role == role && column.visible_source_index == visible_index)
        .ok_or_else(|| {
            format!(
                "Iceberg IMV aggregate rewrite missing AVG {role:?} state column for visible output index {visible_index}"
            )
        })
}

fn retraction_count_state_column(
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
) -> Result<&crate::compiler::mv_rewrite::SqlImvAggregateStateColumn, String> {
    use crate::mv_refresh::AggregateFunctionKind;

    layout
        .state_columns
        .iter()
        .find(|column| column.state_role == SqlImvAggregateStateRole::RetractionCount)
        .or_else(|| {
            layout.state_columns.iter().find(|column| {
                column.state_role == SqlImvAggregateStateRole::Single
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
    state_column: &crate::compiler::mv_rewrite::SqlImvAggregateStateColumn,
) -> Result<&'static str, String> {
    use crate::mv_refresh::AggregateFunctionKind;

    match state_column.state_role {
        SqlImvAggregateStateRole::AvgSum => return Ok("sum_state_union"),
        SqlImvAggregateStateRole::AvgCount => return Ok("count_state_union"),
        SqlImvAggregateStateRole::RetractionCount => {
            return Err(
                "Iceberg IMV aggregate rewrite cannot union retraction count as binary state"
                    .to_string(),
            );
        }
        SqlImvAggregateStateRole::Single => {}
    }
    match state_column.function {
        AggregateFunctionKind::Count => Ok("count_state_union"),
        AggregateFunctionKind::Sum => Ok("sum_state_union"),
        AggregateFunctionKind::Avg => Err(
            "Iceberg IMV aggregate rewrite AVG must use explicit sum and count state columns"
                .to_string(),
        ),
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
    function: crate::mv_refresh::AggregateFunctionKind,
) -> Result<&'static str, String> {
    use crate::mv_refresh::AggregateFunctionKind;

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
    state_column: &crate::compiler::mv_rewrite::SqlImvAggregateStateColumn,
    merged_state: TypedExpr,
    visible_type: &novarocks_type_contract::FunctionValueType,
) -> Result<Vec<TypedExpr>, String> {
    use crate::mv_refresh::AggregateFunctionKind;

    if matches!(
        state_column.function,
        AggregateFunctionKind::Sum | AggregateFunctionKind::Min | AggregateFunctionKind::Max
    ) {
        return Ok(vec![
            merged_state,
            TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Null),
                value_type: novarocks_type_contract::FunctionValueType {
                    nullable: true,
                    ..visible_type.clone()
                },
            },
        ]);
    }
    if state_column.function != AggregateFunctionKind::Avg {
        return Ok(vec![merged_state]);
    }
    Err(
        "Iceberg IMV aggregate rewrite AVG requires explicit sum and count state arguments"
            .to_string(),
    )
}

fn visible_avg_state_args(
    state_column: &crate::compiler::mv_rewrite::SqlImvAggregateStateColumn,
    sum_state: TypedExpr,
    count_state: TypedExpr,
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
) -> Result<Vec<TypedExpr>, String> {
    let visible = layout
        .visible_columns
        .get(state_column.visible_source_index)
        .ok_or_else(|| {
            format!(
                "Iceberg IMV aggregate rewrite visible source index {} out of range",
                state_column.visible_source_index
            )
        })?;
    if !matches!(visible.value_type.data_type, DataType::Decimal128(_, _)) {
        return Ok(vec![sum_state, count_state]);
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
    Ok(vec![
        sum_state,
        count_state,
        int64_literal(i64::from(*input_scale)),
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Null),
            value_type: novarocks_type_contract::FunctionValueType {
                nullable: true,
                ..visible.value_type.clone()
            },
        },
    ])
}

fn int64_literal(value: i64) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(value)),
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
    }
}

fn branch_scope_predicate(
    scope: &crate::planner::table::BranchScope,
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
                value_type: branch_column.value_type.clone(),
            }),
            op: BinOp::Eq,
            right: Box::new(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(scope.branch_id as i64)),
                value_type: novarocks_type_contract::FunctionValueType {
                    nullable: false,
                    ..branch_column.value_type.clone()
                },
            }),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
    })
}

fn aggregate_old_state_passthrough_items(
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
    outputs: &[OutputColumn],
) -> Result<Vec<ProjectItem>, String> {
    let mut names = Vec::with_capacity(
        1 + layout.group_key_source_indexes.len() + layout.state_columns.len() + 4,
    );
    push_unique_name(&mut names, &layout.row_id_column_name);
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
        crate::common::ICEBERG_FILE_PATH_COL,
        crate::common::ICEBERG_ROW_POS_COL,
        crate::common::ICEBERG_ROW_ID_COL,
        crate::common::ICEBERG_LAST_UPDATED_SEQ_COL,
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
    schema_contract: &crate::compiler::mv_rewrite::SqlImvSchemaContract,
) -> bool {
    schema_contract
        .target
        .partition
        .as_ref()
        .is_none_or(|partition| partition.fields.is_empty())
}

fn group_key_names(
    aggregate: &LogicalAggregateNode,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Vec<String>, SqlCompileError> {
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = (|| {
        let mut aggregate_ids = HashSet::new();
        for call in &aggregate.aggregates {
            aggregate_ids.insert(call.output_column_id);
            work.step()?;
        }
        // The logical aggregate builder publishes group outputs in group_by
        // order, independently of their names, then appends aggregate outputs.
        // Filtering aggregate IDs also preserves that association when only
        // aggregate outputs have been moved ahead of the group outputs.
        let mut group_outputs = Vec::new();
        for column in &aggregate.output_columns {
            if !aggregate_ids.contains(&column.column_id) {
                group_outputs.push(column);
            }
            work.step()?;
        }
        if group_outputs.len() != aggregate.group_by.len() {
            return Err(SqlCompileError::Compilation(
                "Iceberg IMV aggregate rewrite GROUP BY/output association has inconsistent cardinality"
                    .to_string(),
            ));
        }
        let mut names = Vec::new();
        let mut selected_outputs = HashSet::new();
        for (index, expr) in aggregate.group_by.iter().enumerate() {
            let output_index = if let ExprKind::ColumnRef { column_id, .. } = &expr.kind {
                let mut matched = None;
                for (candidate_index, column) in group_outputs.iter().enumerate() {
                    let same = column.column_id == *column_id;
                    work.step()?;
                    if same {
                        if matched.replace(candidate_index).is_some() {
                            return Err(SqlCompileError::Compilation(
                                "Iceberg IMV aggregate rewrite found ambiguous GROUP BY output column id"
                                    .to_string(),
                            ));
                        }
                    }
                }
                // A rewritten input reference may have a different ID from
                // its published group output. The original ordinal binding
                // remains authoritative when no exact output ID exists.
                matched.unwrap_or(index)
            } else {
                index
            };
            let output = group_outputs[output_index];
            if !selected_outputs.insert(output_index) {
                return Err(SqlCompileError::Compilation(
                    "Iceberg IMV aggregate rewrite GROUP BY output associations overlap"
                        .to_string(),
                ));
            }
            let same_type = expr
                .value_type
                .exactly_equals_observed::<novarocks_functions::ConstantError>(
                    &output.value_type,
                    || work.step().map_err(Into::into),
                )
                .map_err(SqlCompileError::from)?;
            if !same_type {
                return Err(SqlCompileError::Compilation(
                    "Iceberg IMV aggregate rewrite GROUP BY output type differs from its expression"
                        .to_string(),
                ));
            }
            work.step()?;
            // String cloning remains an opaque library operation. The name is
            // the published output fact, never evidence of expression identity.
            work.flush()?;
            names.push(output.name.clone());
            work.flush()?;
        }
        Ok(names)
    })();
    if matches!(
        &result,
        Err(SqlCompileError::Cancelled
            | SqlCompileError::DeadlineExceeded
            | SqlCompileError::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}

fn aggregate_state_names(
    ext: &ImvExtension,
    aggregate_node: &LogicalAggregateNode,
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
) -> Result<Vec<String>, String> {
    let aggregate = ext
        .snapshot
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
    let state_call_count = layout
        .state_columns
        .iter()
        .filter(|column| {
            column.state_role
                != crate::compiler::mv_rewrite::SqlImvAggregateStateRole::RetractionCount
        })
        .count();
    let expected_state_call_count = aggregate_node.aggregates.len()
        + aggregate_node
            .aggregates
            .iter()
            .filter(|call| call.name.eq_ignore_ascii_case("avg"))
            .count();
    if state_call_count != expected_state_call_count {
        return Err(format!(
            "Iceberg IMV aggregate rewrite aggregate state column count {} does not match aggregate call count {}",
            state_call_count, expected_state_call_count
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
            crate::compiler::mv_rewrite::SqlImvAggregateStateRole::Single => {
                crate::compiler::mv_rewrite::SqlImvAggregateStateRoleContract::Single
            }
            crate::compiler::mv_rewrite::SqlImvAggregateStateRole::AvgSum => {
                crate::compiler::mv_rewrite::SqlImvAggregateStateRoleContract::AvgSum
            }
            crate::compiler::mv_rewrite::SqlImvAggregateStateRole::AvgCount => {
                crate::compiler::mv_rewrite::SqlImvAggregateStateRoleContract::AvgCount
            }
            crate::compiler::mv_rewrite::SqlImvAggregateStateRole::RetractionCount => {
                crate::compiler::mv_rewrite::SqlImvAggregateStateRoleContract::RetractionCount
            }
        };
        if contract_column.role != expected_role {
            return Err(format!(
                "Iceberg IMV aggregate rewrite aggregate state contract/layout mismatch at index {index}: column {} role {:?} expected {:?}",
                contract_column.column_name, contract_column.role, expected_role
            ));
        }
        match layout_column.state_role {
            crate::compiler::mv_rewrite::SqlImvAggregateStateRole::Single
            | crate::compiler::mv_rewrite::SqlImvAggregateStateRole::AvgSum
            | crate::compiler::mv_rewrite::SqlImvAggregateStateRole::AvgCount => {
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
                if matches!(
                    layout_column.state_role,
                    crate::compiler::mv_rewrite::SqlImvAggregateStateRole::AvgSum
                        | crate::compiler::mv_rewrite::SqlImvAggregateStateRole::AvgCount
                ) {
                    if !call.name.eq_ignore_ascii_case("avg") {
                        return Err(format!(
                            "Iceberg IMV aggregate rewrite AVG state column {} references non-AVG aggregate {}",
                            contract_column.column_name, call.name
                        ));
                    }
                } else {
                    signed_state_function(&call.name)?;
                }
            }
            crate::compiler::mv_rewrite::SqlImvAggregateStateRole::RetractionCount => {
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
        .snapshot
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
    Ok(ext.snapshot.target_columns.as_ref().to_vec())
}

fn signed_aggregate(
    aggregate: LogicalAggregateNode,
    aggregate_input: LogicalPlanNode,
    aggregate_required_output_columns: Option<HashSet<ColumnId>>,
    action_column: ColumnId,
    ctx: &RewriteContext,
    shape: &crate::compiler::mv_rewrite::SqlImvAggregateShape,
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
) -> Result<LogicalPlanNode, crate::compiler::SqlCompileError> {
    let input_columns = plan_output_columns(&aggregate_input)?;
    let mut signed_calls = layout
        .state_columns
        .iter()
        .filter(|column| column.state_role != SqlImvAggregateStateRole::RetractionCount)
        .map(|state_column| {
            let call = aggregate.aggregates.get(state_column.aggregate_index).ok_or_else(|| {
                format!(
                    "Iceberg IMV aggregate rewrite state column {} references missing aggregate index {}",
                    state_column.name, state_column.aggregate_index
                )
            })?;
            let call = align_aggregate_call_inputs_to_child(call, &input_columns)?;
            signed_aggregate_call(&call, state_column, action_column, ctx.function_catalog(), ctx.scalar_arena().borrow().constant_policy(),
        &ctx.control_view())
        })
        .collect::<Result<Vec<_>, crate::compiler::SqlCompileError>>()?;
    let hidden_retraction_call = layout.state_columns.iter().any(|column| {
        column.state_role == crate::compiler::mv_rewrite::SqlImvAggregateStateRole::RetractionCount
    });
    if hidden_retraction_call {
        signed_calls.push(retraction_count_aggregate_call(
            action_column,
            ctx.function_catalog(),
            ctx.decimal_overflow_policy(),
            ctx.scalar_arena().borrow().constant_policy(),
            &ctx.control_view(),
        )?);
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
        if let LogicalPlanKind::ImvDelta(node) = &plan.kind {
            merge_action(found, node.action_column)?;
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
    input_columns: &[crate::analysis::OutputColumn],
) -> Result<AggregateCall, String> {
    let mut call = call.clone();
    call.source.rewrite_channels(|arguments, order_by| {
        for arg in arguments {
            align_expr_column_refs_to_child(arg, input_columns)?;
        }
        for sort in order_by {
            align_expr_column_refs_to_child(&mut sort.expr, input_columns)?;
        }
        Ok::<_, String>(())
    })?;
    Ok(call)
}

fn align_expr_column_refs_to_child(
    expr: &mut TypedExpr,
    input_columns: &[crate::analysis::OutputColumn],
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
                expr.value_type = input.value_type.clone();
                return Ok(());
            }
            if qualifier.is_some() {
                let input = unique_input_column_by_name(input_columns, column)?;
                *column_id = input.column_id;
                *qualifier = None;
                *column = input.name.clone();
                expr.value_type = input.value_type.clone();
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
        | ExprKind::Constant(_)
        | ExprKind::SubqueryPlaceholder { .. } => Ok(()),
    }
}

fn unique_input_column_by_id(
    input_columns: &[crate::analysis::OutputColumn],
    column_id: ColumnId,
) -> Result<Option<&crate::analysis::OutputColumn>, String> {
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
    input_columns: &'a [crate::analysis::OutputColumn],
    name: &str,
) -> Result<&'a crate::analysis::OutputColumn, String> {
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
    shape: &crate::compiler::mv_rewrite::SqlImvAggregateShape,
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
    ctx: &RewriteContext,
    signed_calls: &mut [AggregateCall],
) -> Result<Vec<crate::analysis::OutputColumn>, String> {
    let mut output_columns = Vec::with_capacity(shape.group_key_count + signed_calls.len());
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
            _ => allocate_imv_column(ctx, &visible.name, visible.value_type.clone())?,
        };
        output_columns.push(crate::analysis::OutputColumn {
            column_id,
            name: visible.name.clone(),
            value_type: visible.value_type.clone(),

            is_internal: false,
        });
    }
    for (state_index, state_column) in layout.state_columns.iter().enumerate() {
        let value_type = state_shaped_state_value_type(state_column);
        let data_type = value_type.data_type.clone();
        let call = signed_calls.get_mut(state_index).ok_or_else(|| {
            format!(
                "Iceberg IMV aggregate rewrite missing signed state call for {}",
                state_column.name
            )
        })?;
        let novarocks_functions::FunctionResultType::Scalar(result_type) =
            &call.source.binding().selected.result_type
        else {
            return Err(format!(
                "Iceberg IMV signed state {} did not bind a scalar aggregate",
                state_column.name
            ));
        };
        if result_type.logical_type != value_type.logical_type
            || !novarocks_type_contract::arrow_data_types_exact(&result_type.data_type, &data_type)
        {
            return Err(format!(
                "Iceberg IMV signed state {} produces {:?}, expected {data_type:?}",
                state_column.name, result_type.data_type
            ));
        }
        let output =
            allocate_imv_output_column(ctx, &state_column.name, result_type.clone(), true)?;
        let column_id = output.column_id;
        call.output_column_id = column_id;
        output_columns.push(output);
    }
    Ok(output_columns)
}

fn signed_aggregate_project_items(
    group_by: &[TypedExpr],
    shape: &crate::compiler::mv_rewrite::SqlImvAggregateShape,
    layout: &crate::compiler::mv_rewrite::SqlImvAggregateLayout,
    ctx: &RewriteContext,
    aggregate_output_columns: &[OutputColumn],
    signed_calls: &[AggregateCall],
) -> Result<Vec<crate::analysis::ProjectItem>, String> {
    use crate::mv_refresh::VisibleAggregateOutput;

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
                items.push(crate::analysis::ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: child_output.column_id,
                            qualifier: None,
                            column: child_output.name.clone(),
                        },
                        value_type: visible.value_type.clone(),
                    },
                    output_name: visible.name.clone(),
                    output_column_id: allocate_imv_column(
                        ctx,
                        &visible.name,
                        visible.value_type.clone(),
                    )?,
                });
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let state_columns = layout
                    .state_columns
                    .iter()
                    .filter(|column| {
                        column.aggregate_index == *aggregate_index
                            && column.state_role != SqlImvAggregateStateRole::RetractionCount
                    })
                    .collect::<Vec<_>>();
                if state_columns.is_empty() {
                    return Err(format!(
                        "Iceberg IMV aggregate rewrite missing state column for aggregate index {aggregate_index}"
                    ));
                }
                for state_column in state_columns {
                    let state_index = layout
                        .state_columns
                        .iter()
                        .position(|column| column.name == state_column.name)
                        .expect("state column was selected from this layout");
                    let call = signed_calls.get(state_index).ok_or_else(|| {
                        format!(
                            "Iceberg IMV aggregate rewrite missing signed state call for {}",
                            state_column.name
                        )
                    })?;
                    let child_output =
                        signed_aggregate_child_output(aggregate_output_columns, state_column)?;
                    items.push(crate::analysis::ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: call.output_column_id,
                                qualifier: None,
                                column: child_output.name.clone(),
                            },
                            value_type: novarocks_type_contract::FunctionValueType {
                                nullable: false,
                                ..state_shaped_state_value_type(state_column)
                            },
                        },
                        output_name: state_column.name.clone(),
                        output_column_id: allocate_imv_column(
                            ctx,
                            &state_column.name,
                            novarocks_type_contract::FunctionValueType {
                                nullable: false,
                                ..state_shaped_state_value_type(state_column)
                            },
                        )?,
                    });
                }
            }
        }
    }
    for state_column in layout.state_columns.iter().filter(|column| {
        column.state_role == crate::compiler::mv_rewrite::SqlImvAggregateStateRole::RetractionCount
    }) {
        let call = signed_calls.last().ok_or_else(|| {
            format!(
                "Iceberg IMV aggregate rewrite missing hidden retraction state call for {}",
                state_column.name
            )
        })?;
        let child_output = signed_aggregate_child_output(aggregate_output_columns, state_column)?;
        if call.output_column_id != child_output.column_id {
            return Err("Iceberg IMV retraction count call and output identity differ".to_string());
        }
        if child_output.value_type.data_type != DataType::Int64 {
            return Err(
                "Iceberg IMV retraction count aggregate did not produce BIGINT".to_string(),
            );
        }
        items.push(crate::analysis::ProjectItem {
            expr: nonnull_retraction_count_expr(child_output, call.output_column_id),
            output_name: state_column.name.clone(),
            output_column_id: allocate_imv_column(
                ctx,
                &state_column.name,
                novarocks_type_contract::FunctionValueType {
                    nullable: false,
                    ..state_shaped_state_value_type(state_column)
                },
            )?,
        });
    }
    Ok(items)
}

fn nonnull_retraction_count_expr(child: &OutputColumn, column_id: ColumnId) -> TypedExpr {
    let value = TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id,
            qualifier: None,
            column: child.name.clone(),
        },
        value_type: child.value_type.clone(),
    };
    TypedExpr {
        kind: ExprKind::Case {
            operand: None,
            when_then: vec![(
                TypedExpr {
                    kind: ExprKind::IsNull {
                        expr: Box::new(value.clone()),
                        negated: false,
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Boolean,
                        false,
                    ),
                },
                TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(0)),
                    value_type: novarocks_type_contract::FunctionValueType {
                        nullable: false,
                        ..child.value_type.clone()
                    },
                },
            )],
            else_expr: Some(Box::new(value)),
        },
        value_type: novarocks_type_contract::FunctionValueType {
            nullable: false,
            ..child.value_type.clone()
        },
    }
}

fn signed_aggregate_child_output<'a>(
    aggregate_output_columns: &'a [OutputColumn],
    state_column: &crate::compiler::mv_rewrite::SqlImvAggregateStateColumn,
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

fn state_shaped_state_value_type(
    state_column: &crate::compiler::mv_rewrite::SqlImvAggregateStateColumn,
) -> novarocks_type_contract::FunctionValueType {
    match state_column.state_role {
        crate::compiler::mv_rewrite::SqlImvAggregateStateRole::Single
        | crate::compiler::mv_rewrite::SqlImvAggregateStateRole::AvgSum
        | crate::compiler::mv_rewrite::SqlImvAggregateStateRole::AvgCount => {
            novarocks_type_contract::FunctionValueType::new(
                DataType::Binary,
                state_column.value_type.nullable,
            )
        }
        crate::compiler::mv_rewrite::SqlImvAggregateStateRole::RetractionCount => {
            state_column.value_type.clone()
        }
    }
}

fn retraction_count_aggregate_call(
    action_column: ColumnId,
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    policy: novarocks_type_contract::DecimalOverflowPolicy,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<AggregateCall, crate::compiler::SqlCompileError> {
    let args = vec![TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: action_column,
            qualifier: None,
            column: ImvActionColumn::NAME.to_string(),
        },
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
    }];
    let resolved = crate::functions::resolve_sql_aggregate_binding(
        function_catalog,
        "sum",
        &args,
        &[],
        true,
        constant_policy,
        control,
    )
    .map_err(|error| match error {
        novarocks_functions::FunctionBindingError::Control(error) => {
            crate::compiler::SqlCompileError::from(error)
        }
        other => {
            let error = other;
            crate::compiler::SqlCompileError::Compilation(format!(
                "failed to resolve IMV retraction aggregate: {error}"
            ))
        }
    })?;
    Ok(AggregateCall {
        name: "sum".to_string(),
        distinct: false,
        result_type: DataType::Int64,
        output_column_id: ColumnId::UNSET,
        source: crate::binding::AggregateArgumentSource::logical_update(
            args,
            Vec::new(),
            crate::binding::SqlFunctionBinding::new(resolved, policy),
        ),
    })
}

fn signed_aggregate_call(
    call: &AggregateCall,
    state_column: &crate::compiler::mv_rewrite::SqlImvAggregateStateColumn,
    action_column: ColumnId,
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<AggregateCall, crate::compiler::SqlCompileError> {
    let signed_name = match state_column.state_role {
        SqlImvAggregateStateRole::AvgSum => "sum_state_signed",
        SqlImvAggregateStateRole::AvgCount => "count_state_signed",
        SqlImvAggregateStateRole::Single => signed_state_function(&call.name)?,
        SqlImvAggregateStateRole::RetractionCount => {
            return Err(
                "Iceberg IMV aggregate rewrite cannot build retraction count as a state call"
                    .into(),
            );
        }
    };
    let value = signed_value_arg(call)?;
    let input = signed_state_input(
        value,
        action_column,
        function_catalog,
        call.source.binding().decimal_overflow_policy(),
        constant_policy,
        control,
    )?;
    let resolved = crate::functions::resolve_sql_aggregate_binding(
        function_catalog,
        signed_name,
        std::slice::from_ref(&input),
        call.source.order_by(),
        true,
        constant_policy,
        control,
    )
    .map_err(|error| match error {
        novarocks_functions::FunctionBindingError::Control(error) => {
            crate::compiler::SqlCompileError::from(error)
        }
        other => {
            let error = other;
            crate::compiler::SqlCompileError::Compilation(format!(
                "failed to resolve IMV signed aggregate `{signed_name}`: {error}"
            ))
        }
    })?;
    Ok(AggregateCall {
        name: signed_name.to_string(),
        distinct: false,
        result_type: DataType::Binary,
        output_column_id: ColumnId::UNSET,
        source: crate::binding::AggregateArgumentSource::logical_update(
            vec![input],
            call.source.order_by().to_vec(),
            crate::binding::SqlFunctionBinding::new(
                resolved,
                call.source.binding().decimal_overflow_policy(),
            ),
        ),
    })
}

fn signed_value_arg(call: &AggregateCall) -> Result<TypedExpr, String> {
    match call.source.arguments() {
        [] if call.name.eq_ignore_ascii_case("count") => Ok(TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
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

fn signed_state_input(
    value: TypedExpr,
    action_column: ColumnId,
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    policy: novarocks_type_contract::DecimalOverflowPolicy,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<TypedExpr, crate::compiler::SqlCompileError> {
    let args = vec![
        string_literal("value"),
        value,
        string_literal("change_op"),
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: action_column,
                qualifier: None,
                column: ImvActionColumn::NAME.to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
        },
    ];
    let binding = crate::analysis::resolve_function_binding(
        function_catalog,
        "named_struct",
        &args,
        policy,
        constant_policy,
        control,
    )?;
    let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
    else {
        return Err("named_struct must return a scalar value".to_string().into());
    };
    let value_type = result.clone();
    Ok(TypedExpr {
        kind: ExprKind::FunctionCall {
            volatility: crate::functions::FunctionVolatility::Immutable,
            name: "named_struct".to_string(),
            args,
            distinct: false,
            binding,
        },
        value_type,
    })
}

fn string_literal(value: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::String(value.to_string())),
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
    }
}

#[cfg(test)]
mod tests {
    use crate::planner::logical::*;
    use crate::planner::payload::*;
    use std::cell::RefCell;

    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::column_id::{ColumnId, ColumnRefFactory};
    use crate::common::ImvVersionRef;
    use crate::compiler::mv_rewrite::{
        SqlImvAggregateStateColumnContract, SqlImvAggregateStateRoleContract, SqlImvBranchContract,
        SqlImvPartitionContract,
    };
    use crate::optimizer::rewrite::context::RewriteContext;
    use crate::optimizer::scalar::ScalarArena;
    use crate::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::planner::logical::{LogicalAggregateNode, LogicalImvDeltaNode, LogicalPlanKind};
    use crate::planner::optimizer_bridge::logical::{to_logical_plan, to_optimizer_expr};
    use crate::planner::payload::{AggregateCall, PlanScanNode};
    use crate::planner::table::{ScanSource, TableDef};
    use novarocks_types::schema::ColumnDef;

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
            err.to_string()
                .contains("unsupported IMV aggregate function median"),
            "{err}"
        );
    }

    fn single_state_column(type_signature: &str) -> SqlImvAggregateStateColumnContract {
        SqlImvAggregateStateColumnContract {
            column_name: "__agg_state_s".to_string(),
            type_signature: type_signature.to_string(),
            role: SqlImvAggregateStateRoleContract::Single,
        }
    }

    fn retraction_count_state_column() -> SqlImvAggregateStateColumnContract {
        SqlImvAggregateStateColumnContract {
            column_name: "__agg_state___ivm_row_count".to_string(),
            type_signature: "long".to_string(),
            role: SqlImvAggregateStateRoleContract::RetractionCount,
        }
    }

    fn build_ctx() -> RewriteContext<'static> {
        build_ctx_with_state_columns(vec![
            single_state_column("binary"),
            retraction_count_state_column(),
        ])
    }

    fn build_ctx_with_state_columns(
        state_columns: Vec<SqlImvAggregateStateColumnContract>,
    ) -> RewriteContext<'static> {
        build_ctx_with_state_columns_and_target_partition(state_columns, None)
    }

    fn build_ctx_with_state_columns_and_target_partition(
        state_columns: Vec<SqlImvAggregateStateColumnContract>,
        target_partition: Option<SqlImvPartitionContract>,
    ) -> RewriteContext<'static> {
        build_ctx_with_state_columns_target_partition_and_branch(
            state_columns,
            target_partition,
            None,
        )
    }

    fn build_branch_ctx() -> RewriteContext<'static> {
        build_ctx_with_state_columns_target_partition_and_branch(
            vec![
                single_state_column("binary"),
                retraction_count_state_column(),
            ],
            None,
            Some(SqlImvBranchContract {
                branch_id_column_name: crate::planner::vocabulary::BRANCH_ID_COLUMN_NAME
                    .to_string(),
            }),
        )
    }

    fn build_ctx_with_state_columns_target_partition_and_branch(
        state_columns: Vec<SqlImvAggregateStateColumnContract>,
        target_partition: Option<SqlImvPartitionContract>,
        branch_contract: Option<SqlImvBranchContract>,
    ) -> RewriteContext<'static> {
        let snapshot = crate::compiler::mv_rewrite::test_aggregate_snapshot(
            state_columns,
            target_partition,
            branch_contract,
        );

        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_function_catalog(crate::functions::test_function_catalog_snapshot());
        ctx.set_scalar_arena(std::rc::Rc::new(
            std::cell::RefCell::new(ScalarArena::new()),
        ));
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        factory.borrow_mut().reserve_until(100);
        ctx.set_column_ref_factory(factory);
        ctx.set_extension::<ImvExtension>(ImvExtension {
            snapshot,
            annotation: ImvPlanAnnotation::default(),
        });
        ctx
    }

    fn aggregate_rewrite_test_context_with_factory() -> (
        RewriteContext<'static>,
        ImvExtension,
        crate::compiler::mv_rewrite::SqlImvAggregateLayout,
    ) {
        let mut ctx = build_ctx();
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        ctx.set_column_ref_factory(Rc::clone(&factory));
        let ext = ctx
            .extension::<ImvExtension>()
            .expect("build_ctx installs ImvExtension")
            .clone();
        let (_, layout) = ext
            .snapshot
            .aggregate_shape_and_layout_for_execution()
            .expect("aggregate test context has layout");
        (ctx, ext, layout)
    }

    fn expected_row_lineage_metadata_names() -> Vec<&'static str> {
        vec![
            crate::common::ICEBERG_FILE_PATH_COL,
            crate::common::ICEBERG_ROW_POS_COL,
            crate::common::ICEBERG_ROW_ID_COL,
            crate::common::ICEBERG_LAST_UPDATED_SEQ_COL,
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
            assert_eq!(meta.value_type, column.value_type);
            assert_eq!(meta.value_type.nullable, column.value_type.nullable);
        }

        assert_eq!(ext.annotation.partition, None);
    }

    #[test]
    fn avg_state_components_keep_distinct_exact_union_bindings() {
        let ctx = build_ctx();
        let mut bindings = Vec::new();
        for (role, expected_name) in [
            (SqlImvAggregateStateRole::AvgSum, "sum_state_union"),
            (SqlImvAggregateStateRole::AvgCount, "count_state_union"),
        ] {
            let state_column = crate::compiler::mv_rewrite::SqlImvAggregateStateColumn {
                name: "avg_component".to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Binary,
                    false,
                ),
                visible_source_index: 0,
                aggregate_index: 0,
                function: AggregateFunctionKind::Avg,
                state_role: role,
                count_star: false,
            };
            let old = OutputColumn {
                column_id: ColumnId::new_for_test(10),
                name: state_column.name.clone(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Binary, true),

                is_internal: true,
            };
            let delta = OutputColumn {
                column_id: ColumnId::new_for_test(11),
                value_type: novarocks_type_contract::FunctionValueType {
                    nullable: false,
                    ..old.value_type.clone()
                },
                ..old.clone()
            };
            let merged = merged_state_expr(
                &ctx,
                &state_column,
                &[old.clone(), delta.clone()],
                &[delta],
                &[old],
            )
            .expect("AVG state component must retain an exact union binding");
            let ExprKind::FunctionCall {
                name,
                args,
                binding,
                ..
            } = merged.kind
            else {
                panic!("AVG state merge must be a scalar function call");
            };
            assert_eq!(name, expected_name);
            assert_eq!(args.len(), 2);
            assert_eq!(binding.logical_argument_count, 2);
            assert_eq!(binding.selected.argument_types.len(), 2);
            assert_eq!(merged.value_type.data_type, DataType::Binary);
            assert!(!merged.value_type.nullable);
            bindings.push(binding.function_id.clone());
        }
        assert_ne!(bindings[0], bindings[1]);
    }

    #[test]
    fn visible_state_args_threads_avg_decimal_input_scale() {
        let layout = crate::compiler::mv_rewrite::SqlImvAggregateLayout {
            row_id_column_name: "_row_id".to_string(),
            visible_columns: vec![
                crate::compiler::mv_rewrite::SqlImvAggregateVisibleColumn {
                    name: "k".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Int64,
                        false,
                    ),
                },
                crate::compiler::mv_rewrite::SqlImvAggregateVisibleColumn {
                    name: "a".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Decimal128(38, 10),
                        true,
                    ),
                },
            ],
            state_columns: vec![
                crate::compiler::mv_rewrite::SqlImvAggregateStateColumn {
                    name: "a__state_avg_sum".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Binary,
                        false,
                    ),
                    visible_source_index: 1,
                    aggregate_index: 0,
                    function: crate::mv_refresh::AggregateFunctionKind::Avg,
                    state_role: crate::compiler::mv_rewrite::SqlImvAggregateStateRole::AvgSum,
                    count_star: false,
                },
                crate::compiler::mv_rewrite::SqlImvAggregateStateColumn {
                    name: "a__state_avg_count".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Binary,
                        false,
                    ),
                    visible_source_index: 1,
                    aggregate_index: 0,
                    function: crate::mv_refresh::AggregateFunctionKind::Avg,
                    state_role: crate::compiler::mv_rewrite::SqlImvAggregateStateRole::AvgCount,
                    count_star: false,
                },
            ],
            group_key_source_indexes: vec![0],
            physical_column_names: vec![
                "k".to_string(),
                "a__state_avg_sum".to_string(),
                "a__state_avg_count".to_string(),
            ],
            aggregate_input_types: vec![Some(DataType::Decimal128(20, 4))],
        };
        let state_column = layout
            .state_columns
            .iter()
            .find(|column| {
                column.state_role == crate::compiler::mv_rewrite::SqlImvAggregateStateRole::AvgSum
            })
            .expect("AVG sum state column");
        let sum_state = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(10),
                qualifier: None,
                column: state_column.name.clone(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Binary, false),
        };

        let count_state = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(11),
                qualifier: None,
                column: "a__state_avg_count".to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Binary, false),
        };

        let args = visible_avg_state_args(state_column, sum_state, count_state, &layout)
            .expect("AVG decimal visible args");

        assert_eq!(args.len(), 4);
        assert!(matches!(
            &args[2].kind,
            ExprKind::Literal(LiteralValue::Int(4))
        ));
        assert_eq!(args[2].value_type.data_type, DataType::Int64);
        assert!(!args[2].value_type.nullable);
        assert!(matches!(
            &args[3].kind,
            ExprKind::Literal(LiteralValue::Null)
        ));
        assert_eq!(args[3].value_type.data_type, DataType::Decimal128(38, 10));
        assert!(args[3].value_type.nullable);
    }

    fn col_expr(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: None,
                column: name.to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
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
                    source: crate::compiler::mv_rewrite::test_data_scan_source(),
                },
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: ColumnId::new_for_test(1),
                        name: "k".to_string(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Int64,
                            false,
                        ),

                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(2),
                        name: "v".to_string(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Int64,
                            true,
                        ),

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
                    distinct: false,
                    result_type: DataType::Int64,
                    output_column_id: ColumnId::new_for_test(3),
                    source: crate::binding::AggregateArgumentSource::uncertified(
                        vec![col_expr(2, "v")],
                        Vec::new(),
                        crate::functions::test_resolved_aggregate("sum", &[DataType::Int64], false),
                    ),
                }],
                output_columns: vec![
                    OutputColumn {
                        column_id: ColumnId::new_for_test(1),
                        name: "k".to_string(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Int64,
                            false,
                        ),

                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(3),
                        name: "s".to_string(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Int64,
                            true,
                        ),

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
                    distinct: false,
                    result_type: DataType::Int64,
                    output_column_id: ColumnId::new_for_test(3),
                    source: crate::binding::AggregateArgumentSource::uncertified(
                        vec![col_expr(2, "v")],
                        Vec::new(),
                        crate::functions::test_resolved_aggregate("sum", &[DataType::Int64], false),
                    ),
                }],
                output_columns: vec![
                    OutputColumn {
                        column_id: ColumnId::new_for_test(3),
                        name: "s".to_string(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Int64,
                            true,
                        ),

                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(1),
                        name: "k".to_string(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Int64,
                            false,
                        ),

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
            distinct: false,
            result_type: DataType::Int64,
            output_column_id: ColumnId::new_for_test(4),
            source: crate::binding::AggregateArgumentSource::uncertified(
                Vec::new(),
                Vec::new(),
                crate::functions::test_resolved_aggregate("count", &[], false),
            ),
        });
        node.output_columns.push(OutputColumn {
            column_id: ColumnId::new_for_test(4),
            name: "c".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),

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
            && sql_mv_target_state_scan(&scan.table.source).is_some()
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
        matches!(&plan.kind, LogicalPlanKind::Scan(scan)
            if sql_mv_target_state_scan(&scan.table.source).is_some())
            || plan.children.iter().any(contains_target_state_scan)
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
                && sql_mv_target_state_scan(&scan.table.source).is_some()
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
                    LogicalPlanKind::Scan(scan)
                        if sql_mv_target_state_scan(&scan.table.source).is_some()
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
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Int64,
                            false,
                        ),

                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(2),
                        name: "v".to_string(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Int64,
                            true,
                        ),

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
        let SqlCompileError::Compilation(err) = err else {
            panic!("expected an ordinary rewrite error");
        };
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
        let SqlCompileError::Compilation(err) = err else {
            panic!("expected an ordinary rewrite error");
        };
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
        let Some(target_state) = sql_mv_target_state_scan(&old_scan.table.source) else {
            panic!("expected IcebergMvTargetState source");
        };
        let ScanSource::Sql(source) = &old_scan.table.source;
        assert_eq!(source.table.catalog, "ice");
        assert_eq!(source.table.namespace, "db");
        assert_eq!(source.table.table, "mv");
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
            old_scan.required_columns.as_ref().map(|columns| {
                columns
                    .iter()
                    .map(|required| {
                        old_scan
                            .columns
                            .iter()
                            .find(|column| column.column_id == *required)
                            .expect("required scan output")
                            .name
                            .as_str()
                    })
                    .collect::<Vec<_>>()
            }),
            Some(vec![
                "__row_id__",
                "k",
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
            let child_expr = if item.output_name == "__agg_state___ivm_row_count" {
                assert!(!item.expr.value_type.nullable);
                let ExprKind::Case {
                    when_then,
                    else_expr: Some(value),
                    ..
                } = &item.expr.kind
                else {
                    panic!("retraction count must normalize a nullable SUM result");
                };
                assert_eq!(when_then.len(), 1);
                assert!(matches!(
                    &when_then[0].1.kind,
                    ExprKind::Literal(LiteralValue::Int(0))
                ));
                value.as_ref()
            } else {
                &item.expr
            };
            let ExprKind::ColumnRef {
                column_id, column, ..
            } = &child_expr.kind
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
            signed_aggregate.output_columns[1].value_type.data_type,
            DataType::Binary
        );
        assert!(signed_aggregate.output_columns[2].value_type.nullable);
        let args = signed_aggregate.aggregates[0].source.arguments();
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
                "s",
                "__agg_state_s",
                "__agg_state___ivm_row_count",
                // The provider signs its row-lineage data columns before the
                // `_file`/`_pos` row identity, and names each branch's columns
                // by their position in that shape.
                crate::common::ICEBERG_ROW_ID_COL,
                crate::common::ICEBERG_LAST_UPDATED_SEQ_COL,
                crate::common::ICEBERG_FILE_PATH_COL,
                crate::common::ICEBERG_ROW_POS_COL,
                "__change_op"
            ]
        );
        for item in &project.items {
            if branch_case_requires_runtime_cast(&item.expr.value_type.data_type) {
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
        let old_file =
            find_output_column_by_name(&old_outputs, crate::common::ICEBERG_FILE_PATH_COL)
                .expect("old file locator")
                .column_id;
        let old_pos = find_output_column_by_name(&old_outputs, crate::common::ICEBERG_ROW_POS_COL)
            .expect("old row position locator")
            .column_id;

        let file_item = project
            .items
            .iter()
            .find(|item| {
                item.output_name
                    .eq_ignore_ascii_case(crate::common::ICEBERG_FILE_PATH_COL)
            })
            .expect("expected file locator output");
        let pos_item = project
            .items
            .iter()
            .find(|item| {
                item.output_name
                    .eq_ignore_ascii_case(crate::common::ICEBERG_ROW_POS_COL)
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
        assert!(file_item.expr.value_type.nullable);

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
        assert!(pos_item.expr.value_type.nullable);
    }

    #[test]
    fn rewrite_aggregate_updated_target_inherits_sequence_and_preserves_physical_row_id() {
        let mut ctx = build_ctx();
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(
            &delta(aggregate_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        let result = RewriteAggregateStateRule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite");
        let changed = expect_changed_plan(result, &arena_rc.borrow());
        let project = aggregate_change_stream_project(&changed);
        let old_outputs = plan_output_columns(changed.unary_input().unary_input().left().right())
            .expect("old outputs");
        let row_id_item = project
            .items
            .iter()
            .find(|item| item.output_name == crate::common::ICEBERG_ROW_ID_COL)
            .unwrap();
        let old_row_id =
            find_output_column_by_name(&old_outputs, crate::common::ICEBERG_ROW_ID_COL)
                .unwrap()
                .column_id;
        assert_eq!(
            case_arm_column_ids(&row_id_item.expr),
            (old_row_id, old_row_id)
        );
        let sequence_item = project
            .items
            .iter()
            .find(|item| item.output_name == crate::common::ICEBERG_LAST_UPDATED_SEQ_COL)
            .unwrap();
        let (delete_sequence, insert_sequence) = case_arm_exprs(&sequence_item.expr);
        assert_eq!(
            column_id_through_cast(delete_sequence),
            find_output_column_by_name(&old_outputs, crate::common::ICEBERG_LAST_UPDATED_SEQ_COL)
                .unwrap()
                .column_id
        );
        assert!(
            matches!(
                uncast_expr(insert_sequence).kind,
                ExprKind::Literal(LiteralValue::Null)
            ),
            "updated MV target rows must inherit their actual commit sequence"
        );
        assert!(insert_sequence.value_type.nullable);
        assert_eq!(insert_sequence.value_type.data_type, DataType::Int64);
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
            assert_cast_target(
                then_expr,
                &item.expr.value_type.data_type,
                &item.output_name,
            );
        }
        let else_expr = else_expr
            .as_deref()
            .unwrap_or_else(|| panic!("expected CASE ELSE for {}", item.output_name));
        assert_cast_target(
            else_expr,
            &item.expr.value_type.data_type,
            &item.output_name,
        );
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
            | ExprKind::Constant(_)
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
            Some(SqlImvPartitionContract {
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
        let Some(target_state) = sql_mv_target_state_scan(&old_scan.table.source) else {
            panic!("expected IcebergMvTargetState source");
        };

        assert_eq!(
            target_state.partition_constraint,
            SqlMvTargetStatePartitionConstraint::Unpartitioned
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
            Some(crate::planner::table::BranchScope {
                branch_id_column_name: crate::planner::vocabulary::BRANCH_ID_COLUMN_NAME
                    .to_string(),
                branch_id: 1,
            }),
            &ctx,
            &ext,
        )
        .expect("branch-scoped merge builds");

        let _ = aggregate_change_stream_project(&merge);
        let (project, filter, old_scan) = find_branch_scoped_old_input(&merge);
        let Some(target_state) = sql_mv_target_state_scan(&old_scan.table.source) else {
            panic!("expected IcebergMvTargetState source");
        };
        assert!(matches!(
            &target_state.row_filter,
            SqlMvTargetStateRowFilter::DeltaInputRowIds {
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
                crate::common::ICEBERG_FILE_PATH_COL,
                crate::common::ICEBERG_ROW_POS_COL,
                crate::common::ICEBERG_ROW_ID_COL,
                crate::common::ICEBERG_LAST_UPDATED_SEQ_COL,
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
                branch_scope: Some(crate::planner::table::BranchScope {
                    branch_id_column_name: crate::planner::vocabulary::BRANCH_ID_COLUMN_NAME
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

        let signed_arg = &signed_aggregate.aggregates[0].source.arguments()[0];
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

        let retraction_arg = &signed_aggregate.aggregates[1].source.arguments()[0];
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
        let Some(target_state) = sql_mv_target_state_scan(&old_scan.table.source) else {
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
        let SqlCompileError::Compilation(err) = err else {
            panic!("expected an ordinary rewrite error");
        };
        assert!(
            err.to_string().contains("aggregate state column count"),
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
        let SqlCompileError::Compilation(err) = err else {
            panic!("expected an ordinary rewrite error");
        };
        assert!(
            err.to_string().contains("must have binary type signature"),
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
        let SqlCompileError::Compilation(err) = err else {
            panic!("expected an ordinary rewrite error");
        };
        assert!(
            err.to_string()
                .contains("requires a retraction-count or COUNT(*) state column"),
            "unexpected error: {err}"
        );
    }

    fn materialized_group_key(value: i64) -> TypedExpr {
        let value_type = novarocks_type_contract::FunctionValueType::new(DataType::Int64, false);
        let value = novarocks_functions::ConstantValue::from_i64(
            std::sync::Arc::new(arrow::datatypes::Field::new(
                "literal",
                DataType::Int64,
                false,
            )),
            value_type.clone(),
            value,
            crate::constant::test_constant_policy(),
            novarocks_type_contract::CompilePhase::Validate,
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        TypedExpr {
            kind: ExprKind::Constant(value),
            value_type,
        }
    }

    #[test]
    fn materialized_group_key_mapping_uses_authored_order_and_published_aliases_not_payload_names()
    {
        let mut plan = aggregate_over(leaf_scan());
        let LogicalPlanKind::Aggregate(aggregate) = &mut plan.kind else {
            unreachable!()
        };
        aggregate.group_by = vec![materialized_group_key(7), materialized_group_key(9)];
        let ty = aggregate.group_by[0].value_type.clone();
        // Aggregate output has a misleading literal-looking name and comes
        // first; group aliases deliberately resemble the opposite key value.
        aggregate.output_columns = vec![
            OutputColumn {
                name: "7".into(),
                ..aggregate.output_columns[1].clone()
            },
            OutputColumn {
                column_id: ColumnId::new_for_test(10),
                name: "9".into(),
                value_type: ty.clone(),
                is_internal: false,
            },
            OutputColumn {
                column_id: ColumnId::new_for_test(11),
                name: "7".into(),
                value_type: ty,
                is_internal: false,
            },
        ];
        assert_eq!(
            group_key_names(aggregate, crate::optimizer::test_optimizer_control()).unwrap(),
            vec!["9", "7"]
        );
        aggregate.output_columns[1].value_type.nullable = true;
        assert!(matches!(
            group_key_names(aggregate, crate::optimizer::test_optimizer_control()),
            Err(SqlCompileError::Compilation(_))
        ));
        aggregate.output_columns.pop();
        assert!(matches!(
            group_key_names(aggregate, crate::optimizer::test_optimizer_control()),
            Err(SqlCompileError::Compilation(_))
        ));
    }

    #[test]
    fn rewrite_aggregate_state_accepts_non_column_cv_group_key_with_published_target_alias() {
        let mut ctx = build_ctx();
        let mut aggregate_plan = aggregate_first_output_over(leaf_scan());
        let LogicalPlanKind::Aggregate(aggregate) = &mut aggregate_plan.kind else {
            unreachable!()
        };
        aggregate.group_by[0] = materialized_group_key(7);
        // This is the actual target's published alias, unrelated to rendering
        // the selected materialized value or its pool.
        aggregate.output_columns[1].name = "k".into();
        let arena = ctx.scalar_arena();
        let expr = to_optimizer_expr(&delta(aggregate_plan), &mut arena.borrow_mut());
        let result = RewriteAggregateStateRule.apply(expr, &mut ctx).unwrap();
        let changed = expect_changed_merge(result, &arena.borrow());
        let old_scan = find_target_state_scan(&changed);
        let target_state = sql_mv_target_state_scan(&old_scan.table.source).unwrap();
        assert_eq!(target_state.group_key_names, vec!["k"]);
        let signed = find_signed_delta_project(&changed).unary_input();
        let LogicalPlanKind::Aggregate(aggregate) = &signed.kind else {
            unreachable!()
        };
        assert!(
            matches!(&aggregate.group_by[0].kind, ExprKind::Constant(value) if value.try_i64().unwrap() == Some(7))
        );
    }

    #[test]
    fn materialized_group_output_mapping_observes_real_loops_and_all_typed_failure_tails() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        struct Control {
            trace: std::sync::Mutex<Vec<(CompilePhase, u32)>>,
            refuse: Option<usize>,
            cause: CompileControlError,
        }
        impl PureCompileControl for Control {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                let mut trace = self.trace.lock().unwrap();
                trace.push((phase, units));
                if self.refuse == Some(trace.len() - 1) {
                    Err(self.cause)
                } else {
                    Ok(())
                }
            }
        }
        let mut plan = aggregate_over(leaf_scan());
        let LogicalPlanKind::Aggregate(aggregate) = &mut plan.kind else {
            unreachable!()
        };
        let key = materialized_group_key(7);
        aggregate.group_by = vec![key.clone(); 320];
        aggregate.output_columns = (0..320)
            .map(|index| OutputColumn {
                column_id: ColumnId::new_for_test(1000 + index),
                name: format!("published_{index}"),
                value_type: key.value_type.clone(),
                is_internal: false,
            })
            .chain([aggregate.output_columns[1].clone()])
            .collect();
        for ordinary in [false, true] {
            if ordinary {
                aggregate.output_columns.last_mut().unwrap().column_id =
                    ColumnId::new_for_test(9999);
            }
            let good = Control {
                trace: Default::default(),
                refuse: None,
                cause: CompileControlError::Cancelled,
            };
            let result = group_key_names(aggregate, &good);
            if ordinary {
                assert!(matches!(result, Err(SqlCompileError::Compilation(_))));
            } else {
                assert_eq!(result.unwrap().len(), 320);
            }
            let trace = good.trace.into_inner().unwrap();
            assert_eq!(trace.first(), Some(&(CompilePhase::Validate, 0)));
            assert!(trace.iter().any(|(_, units)| *units == 256));
            assert!(trace.iter().all(|(_, units)| *units <= 256));
            for index in 0..trace.len() {
                for cause in [
                    CompileControlError::Cancelled,
                    CompileControlError::DeadlineExceeded,
                    CompileControlError::ResourceExhausted,
                ] {
                    let control = Control {
                        trace: Default::default(),
                        refuse: Some(index),
                        cause,
                    };
                    assert!(
                        matches!(group_key_names(aggregate, &control), Err(error) if error == SqlCompileError::from(cause))
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=index]);
                }
            }
        }
    }
}
