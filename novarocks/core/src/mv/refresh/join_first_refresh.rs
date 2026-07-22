// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Canonical planning and execution adapter for the first refresh of join MVs.

use arrow::datatypes::DataType;
use iceberg::TableIdent;

use crate::connector::iceberg::commit::CommitOpKind;
use crate::mv::persistence::schema as mv_schema;
use crate::mv::persistence::schema::JOIN_APPLY_KEY_COLUMN_NAME;
use crate::mv::refresh::change_stream_write::{
    ChangeStreamWriteError, ExecutedChangeStreamWrite, PopulatedChangeStreamWrite,
    execute_and_collect_change_stream_write,
};
use crate::mv::rewrite::context::IcebergMvRewriteContext;
use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor;
use crate::sql::planner::logical::LogicalPlanNode;
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
        crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
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
        crate::exec::change_op::CHANGE_OP_COLUMN,
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_join_first_refresh_write<F>(
    table: &iceberg::table::Table,
    ident: &TableIdent,
    target_ref: &str,
    rewrite: &IcebergMvRewriteContext,
    left_ref: &TableIdentity,
    right_ref: &TableIdentity,
    input: JoinFirstRefreshLogicalInput,
    execute: F,
) -> Result<PopulatedChangeStreamWrite, ChangeStreamWriteError>
where
    F: FnOnce(JoinFirstRefreshLogicalPlan) -> Result<ExecutedChangeStreamWrite, String>,
{
    let logical = build_join_first_refresh_logical_plan(rewrite, left_ref, right_ref, input)
        .map_err(ChangeStreamWriteError::Execution)?;
    execute_and_collect_change_stream_write(
        table,
        ident,
        target_ref,
        CommitOpKind::FastAppend,
        || execute(logical),
    )
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
    schema_fields: Vec<crate::connector::iceberg::scan_model::IcebergSchemaFieldDef>,
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
        && let Some(table) = iceberg_scan_table_info(&scan.table.source)
        && table.catalog.eq_ignore_ascii_case(&base_ref.catalog)
        && table.namespace.eq_ignore_ascii_case(&base_ref.namespace)
        && table.table.eq_ignore_ascii_case(&base_ref.table)
    {
        scans.push(JoinFullRefreshBaseScan {
            output_columns: scan.columns.clone(),
            schema_fields: table.schema.fields.clone(),
        });
    }
    for child in &plan.children {
        collect_join_full_refresh_base_scans(child, base_ref, scans);
    }
}

fn iceberg_scan_table_info(
    source: &crate::sql::planner::table::ScanSource,
) -> Option<&crate::connector::iceberg::scan_model::IcebergTableInfo> {
    match source {
        crate::sql::planner::table::ScanSource::IcebergDataFiles { table, .. }
        | crate::sql::planner::table::ScanSource::IcebergMetadataTable { table, .. }
        | crate::sql::planner::table::ScanSource::IcebergDeltaTable { table, .. }
        | crate::sql::planner::table::ScanSource::IcebergVersionTable { table, .. } => Some(table),
        crate::sql::planner::table::ScanSource::StarRocks { .. }
        | crate::sql::planner::table::ScanSource::IcebergMvTargetState(_)
        | crate::sql::planner::table::ScanSource::IcebergMvTargetLocator(_) => None,
    }
}

fn join_full_refresh_row_id_column(
    scan: &JoinFullRefreshBaseScan,
    role: &str,
) -> Result<OutputColumn, String> {
    let column = find_unique_scan_output_column(
        &scan.output_columns,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
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
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
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
    scan: &JoinFullRefreshBaseScan,
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
    scan.schema_fields
        .iter()
        .find(|field| field.field_id == lineage.field_id)
        .map(|field| field.name.clone())
        .or_else(|| {
            base_contract
                .schema_at_create
                .fields
                .iter()
                .find(|field| field.field_id == lineage.field_id)
                .map(|field| field.name_at_create.clone())
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadataBuilder, Type,
    };
    use iceberg::{NamespaceIdent, TableIdent};

    use crate::connector::iceberg::scan_model::{
        IcebergDataFileBinding, IcebergSchemaDef, IcebergSchemaFieldDef, IcebergTableInfo,
    };
    use crate::mv::rewrite::context::tests_support::join_projection_rewrite_context;
    use crate::sql::analysis::JoinKind;
    use crate::sql::planner::logical::{LogicalJoinNode, LogicalPlanKind};
    use crate::sql::planner::payload::{PlanProjectNode, PlanScanNode};
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

    fn output_column(
        factory: &mut ColumnRefFactory,
        qualifier: &str,
        name: &str,
        data_type: DataType,
        nullable: bool,
        is_internal: bool,
    ) -> OutputColumn {
        OutputColumn {
            column_id: factory.create(
                Some(qualifier.to_string()),
                name.to_string(),
                data_type.clone(),
                nullable,
            ),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal,
        }
    }

    fn schema_field(field_id: i32, name: &str) -> IcebergSchemaFieldDef {
        IcebergSchemaFieldDef {
            field_id,
            name: name.to_string(),
            initial_default: None,
            write_default: None,
            initial_default_json: None,
            write_default_json: None,
            children: Vec::new(),
        }
    }

    fn scan(
        factory: &mut ColumnRefFactory,
        table_name: &str,
        qualifier: &str,
    ) -> (LogicalPlanNode, Vec<OutputColumn>) {
        let columns = vec![
            output_column(factory, qualifier, "k", DataType::Int64, false, false),
            output_column(factory, qualifier, "v", DataType::Int64, true, false),
            output_column(
                factory,
                qualifier,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
        ];
        let table_columns = columns[..2]
            .iter()
            .map(|column| ColumnDef {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                write_default: None,
                logical_type: None,
            })
            .collect();
        let table = TableDef {
            name: table_name.to_string(),
            columns: table_columns,
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::IcebergDataFiles {
                table: IcebergTableInfo {
                    catalog: "ice".to_string(),
                    namespace: "db".to_string(),
                    table: table_name.to_string(),
                    table_uuid: Some(format!("uuid-{table_name}")),
                    current_snapshot_id: Some(1),
                    schema_id: 7,
                    location: format!("file:///warehouse/db/{table_name}"),
                    schema: IcebergSchemaDef {
                        fields: vec![schema_field(1, "k"), schema_field(2, "v")],
                    },
                    serialized_metadata: None,
                    serialized_metadata_rows: None,
                },
                files: Vec::new(),
                cloud_properties: BTreeMap::new(),
                binding: IcebergDataFileBinding::CurrentSnapshot,
            },
        };
        (
            LogicalPlanNode::new(
                LogicalPlanKind::Scan(PlanScanNode {
                    database: "db".to_string(),
                    table,
                    alias: Some(qualifier.to_string()),
                    columns: columns.clone(),
                    predicates: Vec::new(),
                    required_columns: None,
                    variant_columns: Vec::new(),
                    mv_rewritten_from: None,
                }),
                Vec::new(),
                None,
            ),
            columns,
        )
    }

    fn project_item(column: &OutputColumn, output_name: &str) -> ProjectItem {
        ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: column.column_id,
                    qualifier: None,
                    column: column.name.clone(),
                },
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            },
            output_name: output_name.to_string(),
            output_column_id: column.column_id,
        }
    }

    fn valid_input() -> JoinFirstRefreshLogicalInput {
        let mut factory = ColumnRefFactory::new();
        let (left, left_columns) = scan(&mut factory, "l", "l");
        let (right, right_columns) = scan(&mut factory, "r", "r");
        let join = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![left, right],
            None,
        );
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    project_item(&left_columns[0], "k"),
                    project_item(&right_columns[1], "v"),
                ],
                output_qualifier: None,
            }),
            vec![join],
            None,
        );
        JoinFirstRefreshLogicalInput { plan, factory }
    }

    fn test_ident() -> TableIdent {
        TableIdent::new(NamespaceIdent::new("db".to_string()), "mv".to_string())
    }

    fn test_table() -> iceberg::table::Table {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .expect("schema");
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///warehouse/db/mv".to_string(),
            FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("table metadata builder")
        .build()
        .expect("table metadata")
        .metadata;
        iceberg::table::Table::builder()
            .identifier(test_ident())
            .file_io(iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .build()
            .expect("table")
    }

    #[test]
    fn builds_full_join_apply_key_change_stream_contract() {
        let rewrite = join_projection_rewrite_context();
        let logical = build_join_first_refresh_logical_plan(
            &rewrite,
            &rewrite.base_refs[0],
            &rewrite.base_refs[1],
            valid_input(),
        )
        .expect("join first-refresh logical plan");

        let descriptor = logical
            .change_stream
            .join_refresh
            .as_ref()
            .expect("join refresh descriptor");
        assert_eq!(
            descriptor.mode,
            crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshMode::Full
        );
        assert_eq!(descriptor.payload_columns.len(), 2);
        assert_eq!(descriptor.join_key_pairs.len(), 1);
        assert_eq!(descriptor.left_row_id_column.name, "_row_id");
        assert_eq!(descriptor.right_row_id_column.name, "_row_id");
        let outputs = crate::sql::planner::plan_output_columns(&logical.plan)
            .expect("first-refresh plan outputs");
        assert!(
            outputs
                .iter()
                .any(|column| column.name == JOIN_APPLY_KEY_COLUMN_NAME)
        );
        assert!(
            outputs
                .iter()
                .any(|column| column.name == crate::exec::change_op::CHANGE_OP_COLUMN)
        );
    }

    #[test]
    fn execution_adapter_builds_plan_and_invokes_callback_once() {
        let rewrite = join_projection_rewrite_context();
        let calls = Cell::new(0);

        let error = match execute_join_first_refresh_write(
            &test_table(),
            &test_ident(),
            "staging",
            &rewrite,
            &rewrite.base_refs[0],
            &rewrite.base_refs[1],
            valid_input(),
            |logical| {
                calls.set(calls.get() + 1);
                assert!(logical.change_stream.join_refresh.is_some());
                Err("sentinel execution failure".to_string())
            },
        ) {
            Ok(_) => panic!("callback failure must cross the canonical execution seam"),
            Err(error) => error,
        };

        assert_eq!(calls.get(), 1);
        assert_eq!(error.into_message(), "sentinel execution failure");
    }
}
