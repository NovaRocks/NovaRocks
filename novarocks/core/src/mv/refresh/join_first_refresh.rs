// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Canonical planning and execution adapter for the first refresh of join MVs.

use crate::mv::persistence::schema as mv_schema;
use crate::mv::rewrite::context::IcebergMvRewriteContext;
use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor;
use crate::sql::planner::logical::LogicalPlanNode;
use crate::sql::planner::vocabulary::JOIN_APPLY_KEY_COLUMN_NAME;
use arrow::datatypes::DataType;
use novarocks_catalog::identifier::TableIdentity;

pub(crate) struct JoinFirstRefreshLogicalInput {
    pub(crate) plan: LogicalPlanNode,
    pub(crate) factory: ColumnRefFactory,
}

pub(crate) struct JoinFirstRefreshLogicalPlan {
    pub(crate) plan: LogicalPlanNode,
    pub(crate) factory: ColumnRefFactory,
    pub(crate) change_stream: ImvChangeStreamDescriptor,
}

/// Canonical fresh-data plan for a join MV first refresh.
///
/// Unlike the legacy change-stream plan, this plan carries only payload and
/// join apply-key columns. It is therefore suitable for a single append cohort
/// into an empty staged target and cannot accidentally route through a delete
/// or rewrite action consumer.
pub(crate) struct JoinFirstRefreshAppendLogicalPlan {
    pub(crate) plan: LogicalPlanNode,
    pub(crate) factory: ColumnRefFactory,
}

pub(crate) fn build_join_first_refresh_append_logical_plan(
    rewrite: &IcebergMvRewriteContext,
    left_ref: &TableIdentity,
    right_ref: &TableIdentity,
    input: JoinFirstRefreshLogicalInput,
) -> Result<JoinFirstRefreshAppendLogicalPlan, String> {
    let JoinFirstRefreshLogicalInput { plan, mut factory } = input;
    let input = build_join_full_refresh_apply_input(
        plan,
        rewrite.schema_contract.as_ref(),
        left_ref,
        right_ref,
    )?;
    reserve_factory_for_logical_plan(&mut factory, &input.plan)?;
    let join_apply_key_column_id = factory.create(
        None,
        JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
        DataType::Utf8,
        false,
    );
    // The descriptor remains the single owner of the canonical key
    // expression. Its action column is validation-only for this append path
    // and is intentionally omitted by the projection builder below.
    let action_column_id = factory.create(
        None,
        novarocks_execution::exec::change_op::CHANGE_OP_COLUMN.to_string(),
        DataType::Int8,
        false,
    );
    let join_apply_key_column = join_full_refresh_output_column(
        join_apply_key_column_id,
        JOIN_APPLY_KEY_COLUMN_NAME,
        DataType::Utf8,
        false,
        true,
    );
    let action_column = join_full_refresh_output_column(
        action_column_id,
        novarocks_execution::exec::change_op::CHANGE_OP_COLUMN,
        DataType::Int8,
        false,
        true,
    );
    let descriptor = build_join_full_refresh_descriptor(
        rewrite,
        left_ref,
        right_ref,
        input.payload_columns,
        input.left_row_id_column,
        input.right_row_id_column,
        action_column,
        join_apply_key_column,
        input.join_key_pairs,
    )?;
    descriptor.validate().map_err(|e| {
        format!(
            "iceberg join MV {} first-refresh append descriptor is invalid: {e}",
            rewrite.target.fqn()
        )
    })?;
    let left_uuid = rewrite
        .pin
        .uuid(left_ref)
        .ok_or_else(|| format!("missing uuid for {}", left_ref.fqn()))?
        .to_string();
    let right_uuid = rewrite
        .pin
        .uuid(right_ref)
        .ok_or_else(|| format!("missing uuid for {}", right_ref.fqn()))?
        .to_string();
    let plan = crate::sql::planner::imv_rewrite::join_refresh_builder::build_join_apply_key_append_project(
        input.plan,
        &descriptor,
        &left_uuid,
        &right_uuid,
        join_apply_key_column_id.0,
    )
    .map_err(|e| format!("build join first-refresh append logical plan: {e}"))?;
    reserve_factory_for_logical_plan(&mut factory, &plan)?;
    Ok(JoinFirstRefreshAppendLogicalPlan { plan, factory })
}

pub(crate) fn build_join_first_refresh_logical_plan(
    rewrite: &IcebergMvRewriteContext,
    left_ref: &TableIdentity,
    right_ref: &TableIdentity,
    input: JoinFirstRefreshLogicalInput,
) -> Result<JoinFirstRefreshLogicalPlan, String> {
    let JoinFirstRefreshLogicalInput { plan, mut factory } = input;
    let input = build_join_full_refresh_apply_input(
        plan,
        rewrite.schema_contract.as_ref(),
        left_ref,
        right_ref,
    )?;
    reserve_factory_for_logical_plan(&mut factory, &input.plan)?;
    let join_apply_key_column_id = factory.create(
        None,
        JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
        DataType::Utf8,
        false,
    );
    let action_column_id = factory.create(
        None,
        novarocks_execution::exec::change_op::CHANGE_OP_COLUMN.to_string(),
        DataType::Int8,
        false,
    );
    let join_apply_key_column = join_full_refresh_output_column(
        join_apply_key_column_id,
        JOIN_APPLY_KEY_COLUMN_NAME,
        DataType::Utf8,
        false,
        true,
    );
    let action_column = join_full_refresh_output_column(
        action_column_id,
        novarocks_execution::exec::change_op::CHANGE_OP_COLUMN,
        DataType::Int8,
        false,
        true,
    );
    let descriptor = build_join_full_refresh_descriptor(
        rewrite,
        left_ref,
        right_ref,
        input.payload_columns,
        input.left_row_id_column,
        input.right_row_id_column,
        action_column.clone(),
        join_apply_key_column.clone(),
        input.join_key_pairs,
    )?;
    descriptor.validate().map_err(|e| {
        format!(
            "iceberg join MV {} full refresh descriptor is invalid: {e}",
            rewrite.target.fqn()
        )
    })?;
    let left_uuid = rewrite
        .pin
        .uuid(left_ref)
        .ok_or_else(|| format!("missing uuid for {}", left_ref.fqn()))?
        .to_string();
    let right_uuid = rewrite
        .pin
        .uuid(right_ref)
        .ok_or_else(|| format!("missing uuid for {}", right_ref.fqn()))?
        .to_string();
    let plan =
        crate::sql::planner::imv_rewrite::join_refresh_builder::build_join_apply_key_project_with_constant_insert_action(
            input.plan,
            &descriptor,
            &left_uuid,
            &right_uuid,
            join_apply_key_column_id.0,
            action_column_id.0,
        )
        .map_err(|e| format!("build join full refresh apply-key logical plan: {e}"))?;
    reserve_factory_for_logical_plan(&mut factory, &plan)?;
    Ok(JoinFirstRefreshLogicalPlan {
        plan,
        factory,
        change_stream: ImvChangeStreamDescriptor {
            aggregate: None,
            join_refresh: Some(descriptor),
        },
    })
}

struct JoinFullRefreshApplyInput {
    plan: crate::sql::planner::logical::LogicalPlanNode,
    payload_columns: Vec<OutputColumn>,
    left_row_id_column: OutputColumn,
    right_row_id_column: OutputColumn,
    join_key_pairs:
        Vec<crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshJoinKeyPair>,
}

fn build_join_full_refresh_apply_input(
    plan: crate::sql::planner::logical::LogicalPlanNode,
    schema_contract: &mv_schema::MvSchemaContract,
    left_ref: &TableIdentity,
    right_ref: &TableIdentity,
) -> Result<JoinFullRefreshApplyInput, String> {
    let crate::sql::planner::logical::LogicalPlanNode {
        kind,
        mut children,
        required_output_columns: _,
    } = plan;
    let crate::sql::planner::logical::LogicalPlanKind::Project(mut project) = kind else {
        return Err("join full refresh logical route requires a root Project".to_string());
    };
    if children.len() != 1 {
        return Err(format!(
            "join full refresh root Project expected one input, got {}",
            children.len()
        ));
    }
    let input = children.remove(0);
    let payload_columns = project
        .items
        .iter()
        .map(|item| OutputColumn {
            column_id: item.output_column_id,
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
            is_internal: false,
        })
        .collect::<Vec<_>>();
    validate_join_full_refresh_payload_columns(schema_contract, &payload_columns)?;
    let left_scan = find_join_full_refresh_base_scan(&input, left_ref, "left")?;
    let right_scan = find_join_full_refresh_base_scan(&input, right_ref, "right")?;
    let left_row_id_column = join_full_refresh_row_id_column(&left_scan, "left")?;
    let right_row_id_column = join_full_refresh_row_id_column(&right_scan, "right")?;
    let join_key_pairs = build_join_full_refresh_key_pairs(
        schema_contract,
        left_ref,
        right_ref,
        &left_scan,
        &right_scan,
    )?;
    project
        .items
        .push(project_item_for_column(&left_row_id_column));
    project
        .items
        .push(project_item_for_column(&right_row_id_column));
    let plan = crate::sql::planner::logical::LogicalPlanNode::new(
        crate::sql::planner::logical::LogicalPlanKind::Project(project),
        vec![input],
        None,
    );
    Ok(JoinFullRefreshApplyInput {
        plan,
        payload_columns,
        left_row_id_column,
        right_row_id_column,
        join_key_pairs,
    })
}

fn validate_join_full_refresh_payload_columns(
    schema_contract: &mv_schema::MvSchemaContract,
    payload_columns: &[OutputColumn],
) -> Result<(), String> {
    if payload_columns.len() != schema_contract.target.visible_columns.len() {
        return Err(format!(
            "join full refresh payload column count mismatch: plan has {}, target contract has {}",
            payload_columns.len(),
            schema_contract.target.visible_columns.len()
        ));
    }
    for (idx, (actual, expected)) in payload_columns
        .iter()
        .zip(schema_contract.target.visible_columns.iter())
        .enumerate()
    {
        if !actual.name.eq_ignore_ascii_case(&expected.output_name) {
            return Err(format!(
                "join full refresh payload column {idx} name mismatch: plan has `{}`, target contract has `{}`",
                actual.name, expected.output_name
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct JoinFullRefreshBaseScan {
    output_columns: Vec<OutputColumn>,
}

fn find_join_full_refresh_base_scan(
    plan: &crate::sql::planner::logical::LogicalPlanNode,
    base_ref: &TableIdentity,
    role: &str,
) -> Result<JoinFullRefreshBaseScan, String> {
    let mut scans = Vec::new();
    collect_join_full_refresh_base_scans(plan, base_ref, &mut scans);
    match scans.as_slice() {
        [scan] => Ok((*scan).clone()),
        [] => Err(format!(
            "join full refresh cannot find {role} base scan {} in logical plan",
            base_ref.fqn()
        )),
        _ => Err(format!(
            "join full refresh found multiple {role} base scans {} in logical plan",
            base_ref.fqn()
        )),
    }
}

fn collect_join_full_refresh_base_scans(
    plan: &crate::sql::planner::logical::LogicalPlanNode,
    base_ref: &TableIdentity,
    scans: &mut Vec<JoinFullRefreshBaseScan>,
) {
    if let crate::sql::planner::logical::LogicalPlanKind::Scan(scan) = &plan.kind
        && let Some(source) = sql_scan_source(&scan.table.source)
        && source.table.catalog.eq_ignore_ascii_case(&base_ref.catalog)
        && source
            .table
            .namespace
            .eq_ignore_ascii_case(&base_ref.namespace)
        && source.table.table.eq_ignore_ascii_case(&base_ref.table)
    {
        scans.push(JoinFullRefreshBaseScan {
            output_columns: scan.columns.clone(),
        });
    }
    for child in &plan.children {
        collect_join_full_refresh_base_scans(child, base_ref, scans);
    }
}

fn sql_scan_source(
    source: &crate::sql::planner::table::ScanSource,
) -> Option<&crate::sql::planner::table::SqlScanSource> {
    match source {
        crate::sql::planner::table::ScanSource::Sql(source) => Some(source),
    }
}

fn join_full_refresh_row_id_column(
    scan: &JoinFullRefreshBaseScan,
    role: &str,
) -> Result<OutputColumn, String> {
    let column = find_unique_scan_output_column(
        &scan.output_columns,
        novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL,
        &format!("{role} row-id"),
    )?;
    if column.data_type != DataType::Int64 || column.nullable {
        return Err(format!(
            "join full refresh {role} row-id column has invalid shape: type={:?}, nullable={}",
            column.data_type, column.nullable
        ));
    }
    Ok(join_full_refresh_output_column(
        column.column_id,
        novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL,
        DataType::Int64,
        false,
        true,
    ))
}

fn build_join_full_refresh_key_pairs(
    schema_contract: &mv_schema::MvSchemaContract,
    left_ref: &TableIdentity,
    right_ref: &TableIdentity,
    left_scan: &JoinFullRefreshBaseScan,
    right_scan: &JoinFullRefreshBaseScan,
) -> Result<
    Vec<crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshJoinKeyPair>,
    String,
> {
    let join_contract = schema_contract
        .join
        .as_ref()
        .ok_or_else(|| "join full refresh schema contract missing join lineage".to_string())?;
    let left_base_contract = schema_base_contract_for_fqn(&schema_contract.bases, &left_ref.fqn())?;
    let right_base_contract =
        schema_base_contract_for_fqn(&schema_contract.bases, &right_ref.fqn())?;
    join_contract
        .predicates
        .iter()
        .map(|predicate| {
            let (left_lineage, right_lineage) =
                join_predicate_lineage_for_sides(predicate, &left_ref.fqn(), &right_ref.fqn())?;
            let left_name =
                current_scan_field_name(left_base_contract, left_scan, left_lineage, "left")?;
            let right_name =
                current_scan_field_name(right_base_contract, right_scan, right_lineage, "right")?;
            Ok(
                crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshJoinKeyPair {
                    left_column: find_unique_scan_output_column(
                        &left_scan.output_columns,
                        &left_name,
                        "left join key",
                    )?,
                    right_column: find_unique_scan_output_column(
                        &right_scan.output_columns,
                        &right_name,
                        "right join key",
                    )?,
                },
            )
        })
        .collect()
}

fn schema_base_contract_for_fqn<'a>(
    bases: &'a [mv_schema::BaseContract],
    table_fqn: &str,
) -> Result<&'a mv_schema::BaseContract, String> {
    let matches = bases
        .iter()
        .filter(|base| base.table_fqn.eq_ignore_ascii_case(table_fqn))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [base] => Ok(*base),
        [] => Err(format!(
            "join full refresh schema contract missing base {table_fqn}"
        )),
        _ => Err(format!(
            "join full refresh schema contract has duplicate base {table_fqn}"
        )),
    }
}

fn join_predicate_lineage_for_sides<'a>(
    predicate: &'a mv_schema::JoinPredicateLineage,
    left_base_fqn: &str,
    right_base_fqn: &str,
) -> Result<
    (
        &'a mv_schema::QualifiedFieldLineage,
        &'a mv_schema::QualifiedFieldLineage,
    ),
    String,
> {
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
        "join full refresh predicate lineage does not align with logical plan bases: predicate left={}, right={}, actual left={}, right={}",
        predicate.left.table_fqn, predicate.right.table_fqn, left_base_fqn, right_base_fqn
    ))
}

fn current_scan_field_name(
    base_contract: &mv_schema::BaseContract,
    _scan: &JoinFullRefreshBaseScan,
    lineage: &mv_schema::QualifiedFieldLineage,
    role: &str,
) -> Result<String, String> {
    if !lineage
        .table_fqn
        .eq_ignore_ascii_case(&base_contract.table_fqn)
    {
        return Err(format!(
            "join full refresh {role} lineage table {} does not match base {}",
            lineage.table_fqn, base_contract.table_fqn
        ));
    }
    base_contract
        .schema_at_create
        .fields
        .iter()
        .find(|field| field.field_id == lineage.field_id)
        .map(|field| field.name_at_create.clone())
        .ok_or_else(|| {
            format!(
                "join full refresh {role} lineage references unknown field {} on base {}",
                lineage.field_id, base_contract.table_fqn
            )
        })
}

fn find_unique_scan_output_column(
    columns: &[OutputColumn],
    name: &str,
    role: &str,
) -> Result<OutputColumn, String> {
    let matches = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [column] => Ok((*column).clone()),
        [] => Err(format!(
            "join full refresh cannot find {role} column {name}"
        )),
        _ => Err(format!(
            "join full refresh found multiple {role} columns named {name}"
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_join_full_refresh_descriptor(
    rewrite: &IcebergMvRewriteContext,
    left_ref: &TableIdentity,
    right_ref: &TableIdentity,
    payload_columns: Vec<OutputColumn>,
    left_row_id_column: OutputColumn,
    right_row_id_column: OutputColumn,
    action_column: OutputColumn,
    join_apply_key_column: OutputColumn,
    join_key_pairs: Vec<
        crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshJoinKeyPair,
    >,
) -> Result<crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshDescriptor, String>
{
    let mut output_mappings = payload_columns
        .iter()
        .map(|column| {
            crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshOutputMapping {
                mv_output_column: column.clone(),
                source:
                    crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshOutputSource::Payload(
                        column.column_id,
                    ),
            }
        })
        .collect::<Vec<_>>();
    output_mappings.push(
        crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshOutputMapping {
            mv_output_column: join_apply_key_column.clone(),
            source:
                crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshOutputSource::JoinApplyKey(
                    join_apply_key_column.column_id,
                ),
        },
    );
    output_mappings.push(
        crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshOutputMapping {
            mv_output_column: action_column.clone(),
            source:
                crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshOutputSource::Action(
                    action_column.column_id,
                ),
        },
    );

    Ok(
        crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshDescriptor {
            mode: crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshMode::Full,
            mv_identity:
                crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshMvIdentity {
                    catalog: rewrite.target.catalog.clone(),
                    database: rewrite.target.namespace.clone(),
                    name: rewrite.target.table.clone(),
                },
            left_base_fqn: left_ref.fqn(),
            right_base_fqn: right_ref.fqn(),
            left_row_id_column,
            right_row_id_column,
            action_column,
            join_apply_key_column,
            payload_columns,
            join_key_pairs,
            output_mappings,
            branches: Vec::new(),
            needs_target_locator: false,
        },
    )
}

fn project_item_for_column(column: &OutputColumn) -> ProjectItem {
    ProjectItem {
        expr: column_ref_expr(column),
        output_name: column.name.clone(),
        output_column_id: column.column_id,
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

fn join_full_refresh_output_column(
    column_id: ColumnId,
    name: &str,
    data_type: DataType,
    nullable: bool,
    is_internal: bool,
) -> OutputColumn {
    OutputColumn {
        column_id,
        name: name.to_string(),
        data_type,
        nullable,
        is_internal,
    }
}

fn reserve_factory_for_logical_plan(
    factory: &mut ColumnRefFactory,
    plan: &LogicalPlanNode,
) -> Result<(), String> {
    let max_id = max_logical_plan_output_column_id(plan)?;
    factory.reserve_until(max_id.saturating_add(1));
    Ok(())
}

fn max_logical_plan_output_column_id(plan: &LogicalPlanNode) -> Result<u32, String> {
    let mut max_id = crate::sql::planner::plan_output_columns(plan)?
        .iter()
        .map(|column| column.column_id.0)
        .max()
        .unwrap_or(0);
    for child in &plan.children {
        max_id = max_id.max(max_logical_plan_output_column_id(child)?);
    }
    Ok(max_id)
}
