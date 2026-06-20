use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, TimeUnit};
use iceberg::spec::{NestedField, PrimitiveType, Type};

use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::catalog::{
    ColumnDef, IcebergMvTargetStatePartitionConstraint, IcebergMvTargetStateRowFilter, TableDef,
};
use crate::sql::codegen::helpers::typed_expr_display_name;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::sql::planner::imv_rewrite::annotation::ImvExtension;
use crate::sql::planner::imv_rewrite::join_delta::plan_output_columns;
use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
use crate::sql::planner::imv_rewrite::target_state::build_target_state_scan_source;
use crate::sql::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::sql::planner::plan::{
    AggregateCall, LogicalAggregateNode, LogicalAggregateStateMergeNode, LogicalFilterNode,
    LogicalImvDeltaNode, LogicalPlanNode, LogicalProjectNode, LogicalScanNode, PlanNodeKind,
};

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
            PlanNodeKind::ImvDelta(delta)
                if delta.is_root
                    && matches!(&plan.unary_input().kind, PlanNodeKind::Aggregate(_))
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
    branch_scope: Option<crate::sql::catalog::BranchScope>,
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
        &aggregate_state_names,
        &row_id_column_name,
        old_source,
        ext,
    );
    let old_input = branch_scoped_old_input(old_scan, branch_scope, &aggregate_layout)?;

    let action_column = match action_column {
        Some(action_column) => action_column,
        None => existing_delta_action_column(&aggregate_input)?
            .unwrap_or_else(|| ext.allocate_column_id()),
    };
    let output_columns = aggregate.output_columns.clone();
    let signed_aggregate = signed_aggregate(
        aggregate,
        aggregate_input,
        aggregate_required_output_columns,
        action_column,
        ext,
        &aggregate_calls,
        &aggregate_layout,
    )?;

    Ok(LogicalPlanNode::new(
        PlanNodeKind::AggregateStateMerge(LogicalAggregateStateMergeNode {
            group_key_names,
            aggregate_state_names,
            change_op_column: ImvActionColumn::NAME.to_string(),
            output_columns,
        }),
        vec![old_input, signed_aggregate],
        None,
    ))
}

fn target_state_old_scan(
    target: &crate::engine::mv::iceberg_refresh::IcebergMvTarget,
    target_columns: Vec<ColumnDef>,
    aggregate_state_names: &[String],
    row_id_column_name: &str,
    old_source: crate::sql::catalog::ScanSource,
    ext: &ImvExtension,
) -> LogicalPlanNode {
    let old_columns = target_columns
        .iter()
        .map(|column| crate::sql::analysis::OutputColumn {
            column_id: ext.allocate_column_id(),
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            is_internal: aggregate_state_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&column.name))
                || column.name.eq_ignore_ascii_case(row_id_column_name),
        })
        .collect();
    LogicalPlanNode::new(
        PlanNodeKind::Scan(LogicalScanNode {
            database: target.namespace.clone(),
            table: TableDef {
                name: target.table.clone(),
                columns: target_columns,
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: old_source,
            },
            alias: None,
            columns: old_columns,
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

fn branch_scoped_old_input(
    old_scan: LogicalPlanNode,
    branch_scope: Option<crate::sql::catalog::BranchScope>,
    layout: &crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
) -> Result<LogicalPlanNode, String> {
    let Some(scope) = branch_scope else {
        return Ok(old_scan);
    };
    let old_outputs = plan_output_columns(&old_scan)?;
    let filtered = LogicalPlanNode::new(
        PlanNodeKind::Filter(LogicalFilterNode {
            predicate: branch_scope_predicate(&scope, &old_outputs)?,
        }),
        vec![old_scan],
        None,
    );
    Ok(LogicalPlanNode::new(
        PlanNodeKind::Project(LogicalProjectNode {
            items: aggregate_physical_passthrough_items(layout, &old_outputs)?,
            output_qualifier: None,
        }),
        vec![filtered],
        None,
    ))
}

fn branch_scope_predicate(
    scope: &crate::sql::catalog::BranchScope,
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

fn aggregate_physical_passthrough_items(
    layout: &crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
    outputs: &[OutputColumn],
) -> Result<Vec<ProjectItem>, String> {
    layout
        .physical_columns
        .iter()
        .map(|physical| {
            let column = &physical.column;
            let source = find_output_column_by_name(outputs, &column.name)?;
            Ok(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: source.column_id,
                        qualifier: None,
                        column: source.name.clone(),
                    },
                    data_type: source.data_type.clone(),
                    nullable: source.nullable,
                },
                output_name: column.name.clone(),
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
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
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
    layout: &crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
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
        .filter(|column| {
            column.state_role
                == crate::connector::starrocks::table::mv_agg_state::AggregateStateRole::Single
        })
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
            crate::connector::starrocks::table::mv_agg_state::AggregateStateRole::Single => {
                crate::meta::repository::mv_contract::AggregateStateRoleContract::Single
            }
            crate::connector::starrocks::table::mv_agg_state::AggregateStateRole::RetractionCount => {
                crate::meta::repository::mv_contract::AggregateStateRoleContract::RetractionCount
            }
        };
        if contract_column.role != expected_role {
            return Err(format!(
                "Iceberg IMV aggregate rewrite aggregate state contract/layout mismatch at index {index}: column {} role {:?} expected {:?}",
                contract_column.column_name, contract_column.role, expected_role
            ));
        }
        match layout_column.state_role {
            crate::connector::starrocks::table::mv_agg_state::AggregateStateRole::Single => {
                if !contract_column.type_signature.eq_ignore_ascii_case("binary") {
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
            crate::connector::starrocks::table::mv_agg_state::AggregateStateRole::RetractionCount => {
                if !contract_column.type_signature.eq_ignore_ascii_case("long")
                    && !contract_column.type_signature.eq_ignore_ascii_case("bigint")
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
        write_default: field.write_default.clone(),
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
    ext: &ImvExtension,
    shape: &crate::connector::starrocks::table::aggregate_sql_calls::AggregateSqlCalls,
    layout: &crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
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
    let hidden_retraction_call = layout.state_columns.iter().any(|column| {
        column.state_role
            == crate::connector::starrocks::table::mv_agg_state::AggregateStateRole::RetractionCount
    });
    if hidden_retraction_call {
        signed_calls.push(retraction_count_aggregate_call(action_column));
    }
    let input = if plan_contains_imv_marker(&aggregate_input) {
        thread_delta_action_column(aggregate_input, action_column)?
    } else {
        LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
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
        ext,
        &mut signed_calls,
    )?;
    let project_items = signed_aggregate_project_items(
        &aggregate.group_by,
        shape,
        layout,
        ext,
        &aggregate_output_columns,
        &signed_calls,
    )?;
    let signed_aggregate = LogicalPlanNode::new(
        PlanNodeKind::Aggregate(LogicalAggregateNode {
            group_by: aggregate.group_by,
            aggregates: signed_calls,
            output_columns: aggregate_output_columns,
            already_pushed: aggregate.already_pushed,
        }),
        vec![input],
        aggregate_required_output_columns,
    );
    Ok(LogicalPlanNode::new(
        PlanNodeKind::Project(LogicalProjectNode {
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
            PlanNodeKind::ImvDelta(node) => {
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
    if let PlanNodeKind::ImvDelta(node) = &mut plan.kind {
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
    shape: &crate::connector::starrocks::table::aggregate_sql_calls::AggregateSqlCalls,
    layout: &crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
    ext: &ImvExtension,
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
            _ => ext.allocate_column_id(),
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
        let column_id = ext.allocate_column_id();
        if let Some(call) = signed_calls.get_mut(state_index) {
            call.output_column_id = column_id;
        }
        output_columns.push(crate::sql::analysis::OutputColumn {
            column_id,
            name: state_column.name.clone(),
            data_type,
            nullable: false,
            is_internal: true,
        });
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
    shape: &crate::connector::starrocks::table::aggregate_sql_calls::AggregateSqlCalls,
    layout: &crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
    ext: &ImvExtension,
    aggregate_output_columns: &[OutputColumn],
    signed_calls: &[AggregateCall],
) -> Result<Vec<crate::sql::analysis::ProjectItem>, String> {
    use crate::connector::starrocks::table::mv_shape::VisibleAggregateOutput;

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
                    output_column_id: ext.allocate_column_id(),
                });
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let state_column = layout
                    .state_columns
                    .iter()
                    .find(|column| {
                        column.state_role
                            == crate::connector::starrocks::table::mv_agg_state::AggregateStateRole::Single
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
                    output_column_id: ext.allocate_column_id(),
                });
            }
        }
    }
    for state_column in layout.state_columns.iter().filter(|column| {
        column.state_role
            == crate::connector::starrocks::table::mv_agg_state::AggregateStateRole::RetractionCount
    }) {
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
            output_column_id: ext.allocate_column_id(),
        });
    }
    Ok(items)
}

fn signed_aggregate_child_output<'a>(
    aggregate_output_columns: &'a [OutputColumn],
    state_column: &crate::connector::starrocks::table::mv_agg_state::AggregateStateColumn,
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
    state_column: &crate::connector::starrocks::table::mv_agg_state::AggregateStateColumn,
) -> DataType {
    match state_column.state_role {
        crate::connector::starrocks::table::mv_agg_state::AggregateStateRole::Single => {
            DataType::Binary
        }
        crate::connector::starrocks::table::mv_agg_state::AggregateStateRole::RetractionCount => {
            state_column.data_type.clone()
        }
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
    use crate::sql::planner::plan::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use super::*;
    use crate::engine::mv::refresh_context::IcebergMvRewriteContext;
    use crate::engine::mv::refresh_context::tests_support::{
        make_mv_definition, make_pin, make_ref, make_schema_contract, make_target, parse_query,
    };
    use crate::meta::repository::mv_contract::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource, BranchIdColumnContract, BranchUnionContract, MvPartitionContract,
    };
    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::convert::{logical_plan_to_opt_expr, opt_expr_to_logical_plan};
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::planner::imv_rewrite::marker::ImvVersionRef;
    use crate::sql::planner::plan::{
        AggregateCall, LogicalAggregateNode, LogicalAggregateStateMergeNode, LogicalImvDeltaNode,
        LogicalScanNode, LogicalUnionNode, PlanNodeKind,
    };

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
                    column_name:
                        crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN
                            .to_string(),
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
            IcebergMvRewriteContext::from_parts(
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
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx,
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(100)),
        });
        ctx
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
            PlanNodeKind::Scan(LogicalScanNode {
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
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
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
                dict_columns: Vec::new(),
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn aggregate_over(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_expr(1, "k")],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_expr(2, "v")],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: ColumnId::UNSET,
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
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_expr(1, "k")],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_expr(2, "v")],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: ColumnId::UNSET,
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
        let PlanNodeKind::Aggregate(node) = &mut plan.kind else {
            unreachable!()
        };
        node.aggregates.push(AggregateCall {
            name: "count".to_string(),
            args: Vec::new(),
            distinct: false,
            result_type: DataType::Int64,
            order_by: Vec::new(),
            output_column_id: ColumnId::UNSET,
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
        let RewriteResult::Changed(opt) = result else {
            panic!("expected Changed(AggregateStateMerge)");
        };
        let plan = opt_expr_to_logical_plan(opt, arena);
        assert!(matches!(&plan.kind, PlanNodeKind::AggregateStateMerge(_)));
        plan
    }

    fn merge_node(plan: &LogicalPlanNode) -> &LogicalAggregateStateMergeNode {
        match &plan.kind {
            PlanNodeKind::AggregateStateMerge(node) => node,
            _ => unreachable!(),
        }
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

    fn join_expanded_input() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
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
                    PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                        is_root: false,
                        action_column: Some(ColumnId::new_for_test(100)),
                        branch_scope: None,
                    }),
                    vec![leaf_scan()],
                    None,
                ),
                LogicalPlanNode::new(
                    PlanNodeKind::ImvVersion(LogicalImvVersionNode {
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
        let expr1 = logical_plan_to_opt_expr(
            &delta(aggregate_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        assert!(rule.matches(&expr1, &ctx));
        let expr2 =
            logical_plan_to_opt_expr(&aggregate_over(leaf_scan()), &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr2, &ctx));
        let nested_delta = LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: None,
                branch_scope: None,
            }),
            vec![aggregate_over(leaf_scan())],
            None,
        );
        let expr3 = logical_plan_to_opt_expr(&nested_delta, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr3, &ctx));
    }

    #[test]
    fn rewrite_aggregate_state_rejects_empty_group_by() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let mut aggregate_plan = aggregate_over(leaf_scan());
        let PlanNodeKind::Aggregate(aggregate) = &mut aggregate_plan.kind else {
            unreachable!()
        };
        aggregate.group_by.clear();
        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&delta(aggregate_plan), &mut arena_rc.borrow_mut());
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
        let PlanNodeKind::Aggregate(aggregate) = &mut aggregate_plan.kind else {
            unreachable!()
        };
        aggregate.aggregates[0].distinct = true;
        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&delta(aggregate_plan), &mut arena_rc.borrow_mut());
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
        let expr = logical_plan_to_opt_expr(
            &delta(aggregate_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        let result = rule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite must succeed");
        let changed = expect_changed_merge(result, &arena_rc.borrow());
        let merge = merge_node(&changed);

        assert_eq!(merge.group_key_names, vec!["k"]);
        assert_eq!(
            merge.aggregate_state_names,
            vec!["__agg_state_s", "__agg_state___ivm_row_count"]
        );
        assert_eq!(merge.change_op_column, "__change_op");
        assert_eq!(merge.output_columns[1].name, "s");

        let PlanNodeKind::Scan(old_scan) = &changed.left().kind else {
            panic!("expected target-state scan");
        };
        let ScanSource::IcebergMvTargetState(target_state) = &old_scan.table.source else {
            panic!("expected IcebergMvTargetState source");
        };
        assert_eq!(target_state.fqn(), "tgt.db.mv");
        assert_eq!(target_state.group_key_names, vec!["k"]);
        assert_eq!(
            target_state.aggregate_state_names,
            vec!["__agg_state_s", "__agg_state___ivm_row_count"]
        );

        let delta_input = changed.right();
        let PlanNodeKind::Project(project) = &delta_input.kind else {
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
        let PlanNodeKind::Aggregate(signed_aggregate) = &signed_aggregate_plan.kind else {
            panic!("expected signed aggregate under projection");
        };
        assert!(matches!(
            &signed_aggregate_plan.unary_input().kind,
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode { is_root: false, .. })
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
        let expr = logical_plan_to_opt_expr(
            &delta(aggregate_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        let result = rule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite must succeed");
        let changed = expect_changed_merge(result, &arena_rc.borrow());
        let PlanNodeKind::Scan(old_scan) = &changed.left().kind else {
            panic!("expected target-state scan");
        };
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
        let PlanNodeKind::Aggregate(aggregate) = &aggregate_plan.kind else {
            panic!("expected aggregate");
        };
        let aggregate_input = aggregate_plan.unary_input().clone();

        let merge =
            build_aggregate_state_merge(
                aggregate.clone(),
                aggregate_input,
                None,
                None,
                Some(crate::sql::catalog::BranchScope {
                    branch_id_column_name:
                        crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN
                            .to_string(),
                    branch_id: 1,
                }),
                &ext,
            )
            .expect("branch-scoped merge builds");

        let PlanNodeKind::AggregateStateMerge(_) = &merge.kind else {
            panic!("expected AggregateStateMerge");
        };
        let old_input = merge.left();
        let PlanNodeKind::Project(project) = &old_input.kind else {
            panic!("expected old input Project dropping branch id");
        };
        let filter_plan = old_input.unary_input();
        let PlanNodeKind::Filter(filter) = &filter_plan.kind else {
            panic!("expected branch filter under old-input Project");
        };
        let PlanNodeKind::Scan(old_scan) = &filter_plan.unary_input().kind else {
            panic!("expected target-state scan under branch filter");
        };
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
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: true,
                action_column: None,
                branch_scope: Some(crate::sql::catalog::BranchScope {
                    branch_id_column_name:
                        crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN
                            .to_string(),
                    branch_id: 1,
                }),
            }),
            vec![aggregate_over(leaf_scan())],
            None,
        );
        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        let changed = expect_changed_merge(
            rule.apply(expr, &mut ctx).expect("rewrite"),
            &arena_rc.borrow(),
        );
        // Branch scope manifests as Project(Filter(Scan)) on the old input.
        assert!(
            matches!(&changed.left().kind, PlanNodeKind::Project(_)),
            "branch-scoped old input must be wrapped in a passthrough Project over a Filter"
        );
    }

    #[test]
    fn rewrite_aggregate_state_preserves_pre_expanded_join_delta_input() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(
            &delta(aggregate_over(join_expanded_input())),
            &mut arena_rc.borrow_mut(),
        );
        let result = rule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite must succeed");
        let changed = expect_changed_merge(result, &arena_rc.borrow());

        let delta_input = changed.right();
        let PlanNodeKind::Project(_) = &delta_input.kind else {
            panic!("expected signed aggregate projection delta input");
        };
        let signed_aggregate_plan = delta_input.unary_input();
        let PlanNodeKind::Aggregate(_) = &signed_aggregate_plan.kind else {
            panic!("expected signed aggregate under projection");
        };
        assert!(
            matches!(
                &signed_aggregate_plan.unary_input().kind,
                PlanNodeKind::Union(_)
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
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: existing_action,
                branch_scope: None,
            }),
            vec![leaf_scan()],
            None,
        );
        let arena_rc = ctx.scalar_arena();
        let expr =
            logical_plan_to_opt_expr(&delta(aggregate_over(input)), &mut arena_rc.borrow_mut());
        let result = rule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite must succeed");
        let changed = expect_changed_merge(result, &arena_rc.borrow());
        let delta_input = changed.right();
        let PlanNodeKind::Project(_) = &delta_input.kind else {
            panic!("expected signed aggregate projection delta input");
        };
        let signed_aggregate_plan = delta_input.unary_input();
        let PlanNodeKind::Aggregate(signed_aggregate) = &signed_aggregate_plan.kind else {
            panic!("expected signed aggregate under projection");
        };
        let PlanNodeKind::ImvDelta(delta_input) = &signed_aggregate_plan.unary_input().kind else {
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
        let expr = logical_plan_to_opt_expr(
            &delta(aggregate_first_output_over(leaf_scan())),
            &mut arena_rc.borrow_mut(),
        );
        let result = rule
            .apply(expr, &mut ctx)
            .expect("aggregate rewrite must succeed");
        let changed = expect_changed_merge(result, &arena_rc.borrow());
        let merge = merge_node(&changed);

        assert_eq!(merge.group_key_names, vec!["k"]);
    }

    #[test]
    fn rewrite_aggregate_state_rejects_state_column_count_mismatch() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(
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
        let expr = logical_plan_to_opt_expr(
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
        let expr = logical_plan_to_opt_expr(
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
