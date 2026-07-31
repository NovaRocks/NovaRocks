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

use std::collections::BTreeSet;

use arrow::datatypes::DataType;

use crate::sql::analysis::{
    BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
};
use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::planner::imv_rewrite::join_refresh_descriptor::{
    JoinRefreshDescriptor, JoinRefreshMode, JoinRefreshOutputMapping, JoinRefreshOutputSource,
};
use crate::sql::planner::logical::{
    LogicalAggregateNode, LogicalJoinNode, LogicalPlanKind, LogicalPlanNode,
};
use crate::sql::planner::payload::{AggregateCall, PlanFilterNode, PlanProjectNode, PlanScanNode};
use crate::sql::planner::table::{IcebergMvTargetLocatorScan, ScanSource, TableDef};
use novarocks_catalog::schema::ColumnDef;

pub(crate) fn build_join_apply_key_project(
    input: LogicalPlanNode,
    desc: &JoinRefreshDescriptor,
    left_uuid: &str,
    right_uuid: &str,
    apply_key_column_id: u32,
    action_column_id: u32,
) -> Result<LogicalPlanNode, String> {
    build_join_apply_key_project_with_action(
        input,
        desc,
        left_uuid,
        right_uuid,
        apply_key_column_id,
        action_column_id,
        JoinApplyActionProjection::InputColumn,
    )
}

pub(crate) fn build_join_apply_key_project_with_constant_insert_action(
    input: LogicalPlanNode,
    desc: &JoinRefreshDescriptor,
    left_uuid: &str,
    right_uuid: &str,
    apply_key_column_id: u32,
    action_column_id: u32,
) -> Result<LogicalPlanNode, String> {
    build_join_apply_key_project_with_action(
        input,
        desc,
        left_uuid,
        right_uuid,
        apply_key_column_id,
        action_column_id,
        JoinApplyActionProjection::ConstantInsert,
    )
}

fn build_join_apply_key_project_with_action(
    input: LogicalPlanNode,
    desc: &JoinRefreshDescriptor,
    left_uuid: &str,
    right_uuid: &str,
    apply_key_column_id: u32,
    action_column_id: u32,
    action_projection: JoinApplyActionProjection,
) -> Result<LogicalPlanNode, String> {
    desc.validate()?;
    validate_apply_key_project_output_ids(desc, apply_key_column_id, action_column_id)?;
    let input_columns = crate::sql::planner::plan_output_columns(&input).map_err(|err| {
        format!("join refresh apply-key project cannot derive input columns: {err}")
    })?;

    let items = desc
        .output_mappings
        .iter()
        .map(|mapping| {
            project_item_for_mapping(
                mapping,
                desc,
                &input_columns,
                left_uuid,
                right_uuid,
                action_projection,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            items,
            output_qualifier: None,
        }),
        vec![input],
        None,
    ))
}

#[derive(Clone, Copy)]
enum JoinApplyActionProjection {
    InputColumn,
    ConstantInsert,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinRefreshTargetLocatorBinding {
    pub(crate) target_table_uuid: String,
    pub(crate) target_snapshot_id: Option<i64>,
}

impl JoinRefreshTargetLocatorBinding {
    pub(crate) fn from_rewrite_context(
        ctx: &crate::mv::rewrite::context::IcebergMvRewriteContext,
    ) -> Self {
        Self {
            target_table_uuid: ctx.target_table_uuid.clone(),
            target_snapshot_id: ctx.target_snapshot_id,
        }
    }
}

pub(crate) fn build_join_delta_coalesce_plan_with_locator(
    branch_union: LogicalPlanNode,
    desc: &JoinRefreshDescriptor,
    locator: &JoinRefreshTargetLocatorBinding,
    column_ref_factory: &mut ColumnRefFactory,
    net_column_id: u32,
    locator_file_column_id: u32,
    locator_pos_column_id: u32,
    locator_row_id_column_id: u32,
    locator_last_updated_seq_column_id: u32,
) -> Result<LogicalPlanNode, String> {
    desc.validate()?;
    if desc.mode != JoinRefreshMode::Coalesce {
        return Err("join refresh coalesce builder requires coalesce descriptor".to_string());
    }

    let input_columns = crate::sql::planner::plan_output_columns(&branch_union)
        .map_err(|err| format!("join refresh coalesce cannot derive input columns: {err}"))?;
    let payload_mappings = payload_source_output_columns(desc)?;
    let payload_inputs = payload_mappings
        .iter()
        .map(|(input, _)| input.clone())
        .collect::<Vec<_>>();
    let payload_outputs = payload_mappings
        .iter()
        .map(|(_, output)| output.clone())
        .collect::<Vec<_>>();
    validate_coalesce_payload_output_names(&payload_outputs)?;
    let apply_key_input = desc.join_apply_key_column.clone();
    let action_input = desc.action_column.clone();
    let apply_key_output = mapped_output_column(
        desc,
        JoinRefreshOutputSource::JoinApplyKey(desc.join_apply_key_column.column_id),
    )?;
    for column in payload_inputs
        .iter()
        .chain([&apply_key_input, &action_input])
    {
        validate_input_column(&input_columns, column)?;
    }

    let explicit_generated_ids = [
        ("net", ColumnId(net_column_id)),
        (
            crate::exec::row_position::ICEBERG_FILE_PATH_COL,
            ColumnId(locator_file_column_id),
        ),
        (
            crate::exec::row_position::ICEBERG_ROW_POS_COL,
            ColumnId(locator_pos_column_id),
        ),
        (
            crate::exec::row_position::ICEBERG_ROW_ID_COL,
            ColumnId(locator_row_id_column_id),
        ),
        (
            crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
            ColumnId(locator_last_updated_seq_column_id),
        ),
    ];
    validate_generated_column_ids(&input_columns, desc, &explicit_generated_ids)?;
    let net_column = output_column(ColumnId(net_column_id), "net", DataType::Int64, false, true);
    let key_shape_apply_key = output_column_from_factory(
        column_ref_factory,
        &apply_key_output.name,
        apply_key_output.data_type.clone(),
        apply_key_output.nullable,
        apply_key_output.is_internal,
    );
    let pending_insert_count = output_column_from_factory(
        column_ref_factory,
        "__pending_insert_count",
        DataType::Int64,
        false,
        true,
    );
    let pending_delete_count = output_column_from_factory(
        column_ref_factory,
        "__pending_delete_count",
        DataType::Int64,
        false,
        true,
    );
    let locator_apply_key = output_column_from_factory(
        column_ref_factory,
        &apply_key_input.name,
        apply_key_input.data_type.clone(),
        apply_key_input.nullable,
        false,
    );
    let aggregate = build_payload_coalesce_aggregate(
        branch_union,
        &payload_inputs,
        &apply_key_input,
        &action_input,
        &net_column,
    );
    let payload_checked = build_payload_coalesce_assert_filter(aggregate, &net_column);
    let key_shape_checked = build_key_shape_assert_join(
        payload_checked,
        &apply_key_input,
        &net_column,
        &key_shape_apply_key,
        &pending_insert_count,
        &pending_delete_count,
    );
    let locator_join = build_locator_join_shell(
        key_shape_checked,
        desc,
        locator,
        &apply_key_input,
        &net_column,
        locator_apply_key.column_id,
        ColumnId(locator_file_column_id),
        ColumnId(locator_pos_column_id),
        ColumnId(locator_row_id_column_id),
        ColumnId(locator_last_updated_seq_column_id),
    )?;
    let locator_checked = build_locator_assert_filter(
        locator_join,
        &net_column,
        ColumnId(locator_file_column_id),
        ColumnId(locator_pos_column_id),
    );
    Ok(build_final_coalesce_project(
        locator_checked,
        desc,
        &payload_mappings,
        &apply_key_input,
        &apply_key_output,
        &net_column,
        ColumnId(locator_file_column_id),
        ColumnId(locator_pos_column_id),
        ColumnId(locator_row_id_column_id),
        ColumnId(locator_last_updated_seq_column_id),
    )?)
}

fn build_payload_coalesce_aggregate(
    branch_union: LogicalPlanNode,
    payload_inputs: &[OutputColumn],
    apply_key_input: &OutputColumn,
    action_input: &OutputColumn,
    net_column: &OutputColumn,
) -> LogicalPlanNode {
    let mut group_by = payload_inputs.iter().map(column_ref).collect::<Vec<_>>();
    group_by.push(column_ref(apply_key_input));
    let output_columns = payload_inputs
        .iter()
        .cloned()
        .chain([apply_key_input.clone(), net_column.clone()])
        .collect::<Vec<_>>();
    LogicalPlanNode::new(
        LogicalPlanKind::Aggregate(LogicalAggregateNode {
            group_by,
            aggregates: vec![AggregateCall {
                name: "sum".to_string(),
                args: vec![column_ref(action_input)],
                distinct: false,
                result_type: DataType::Int64,
                order_by: Vec::new(),
                output_column_id: net_column.column_id,
            }],
            output_columns,
            already_pushed: false,
        }),
        vec![branch_union],
        None,
    )
}

fn validate_generated_column_ids(
    input_columns: &[OutputColumn],
    desc: &JoinRefreshDescriptor,
    generated_ids: &[(&str, ColumnId)],
) -> Result<(), String> {
    let existing = existing_coalesce_column_ids(input_columns, desc);
    let mut generated = BTreeSet::new();
    for (name, id) in generated_ids {
        if *id == ColumnId::UNSET {
            return Err(format!(
                "join refresh coalesce generated column `{name}` uses unset ColumnId"
            ));
        }
        if existing.contains(id) {
            return Err(format!(
                "join refresh coalesce generated column `{name}` id {id} collides with existing column"
            ));
        }
        if !generated.insert(*id) {
            return Err(format!(
                "join refresh coalesce generated column `{name}` id {id} is duplicated"
            ));
        }
    }
    Ok(())
}

fn existing_coalesce_column_ids(
    input_columns: &[OutputColumn],
    desc: &JoinRefreshDescriptor,
) -> BTreeSet<ColumnId> {
    let mut ids = input_columns
        .iter()
        .map(|column| column.column_id)
        .collect::<BTreeSet<_>>();
    ids.insert(desc.left_row_id_column.column_id);
    ids.insert(desc.right_row_id_column.column_id);
    ids.insert(desc.action_column.column_id);
    ids.insert(desc.join_apply_key_column.column_id);
    for column in &desc.payload_columns {
        ids.insert(column.column_id);
    }
    for pair in &desc.join_key_pairs {
        ids.insert(pair.left_column.column_id);
        ids.insert(pair.right_column.column_id);
    }
    for mapping in &desc.output_mappings {
        ids.insert(mapping.mv_output_column.column_id);
    }
    ids
}

fn build_payload_coalesce_assert_filter(
    aggregate: LogicalPlanNode,
    net_column: &OutputColumn,
) -> LogicalPlanNode {
    let net_ne_zero = binary(
        column_ref(net_column),
        BinOp::Ne,
        int_literal(0, DataType::Int64),
    );
    let abs_net = TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "abs".to_string(),
            args: vec![column_ref(net_column)],
            distinct: false,
        },
        data_type: DataType::Int64,
        nullable: false,
    };
    let abs_net_le_one = binary(abs_net, BinOp::Le, int_literal(1, DataType::Int64));
    let payload_assert = assert_true_call(
        abs_net_le_one,
        "join delta per-payload net change exceeds 1",
    );
    LogicalPlanNode::new(
        LogicalPlanKind::Filter(PlanFilterNode {
            predicate: binary(net_ne_zero, BinOp::And, payload_assert),
        }),
        vec![aggregate],
        None,
    )
}

fn build_key_shape_assert_join(
    payload_checked: LogicalPlanNode,
    apply_key_output: &OutputColumn,
    net_column: &OutputColumn,
    key_shape_apply_key: &OutputColumn,
    pending_insert_count: &OutputColumn,
    pending_delete_count: &OutputColumn,
) -> LogicalPlanNode {
    let key_shape = LogicalPlanNode::new(
        LogicalPlanKind::Aggregate(LogicalAggregateNode {
            group_by: vec![column_ref(apply_key_output)],
            aggregates: vec![
                AggregateCall {
                    name: "sum".to_string(),
                    args: vec![pending_count_expr(net_column, BinOp::Gt)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: pending_insert_count.column_id,
                },
                AggregateCall {
                    name: "sum".to_string(),
                    args: vec![pending_count_expr(net_column, BinOp::Lt)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: pending_delete_count.column_id,
                },
            ],
            output_columns: vec![
                key_shape_apply_key.clone(),
                pending_insert_count.clone(),
                pending_delete_count.clone(),
            ],
            already_pushed: false,
        }),
        vec![payload_checked.clone()],
        None,
    );
    let shape_guard = assert_true_call(
        binary(
            binary(
                column_ref(pending_insert_count),
                BinOp::Le,
                int_literal(1, DataType::Int64),
            ),
            BinOp::And,
            binary(
                column_ref(pending_delete_count),
                BinOp::Le,
                int_literal(1, DataType::Int64),
            ),
        ),
        "join delta multiple pending payloads for key",
    );
    let checked_key_shape = LogicalPlanNode::new(
        LogicalPlanKind::Filter(PlanFilterNode {
            predicate: shape_guard,
        }),
        vec![key_shape],
        None,
    );
    let join_condition = binary(
        column_ref(apply_key_output),
        BinOp::Eq,
        column_ref(key_shape_apply_key),
    );
    LogicalPlanNode::new(
        LogicalPlanKind::Join(LogicalJoinNode {
            join_type: JoinKind::Inner,
            condition: Some(join_condition),
        }),
        vec![payload_checked, checked_key_shape],
        None,
    )
}

fn pending_count_expr(net_column: &OutputColumn, op: BinOp) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Case {
            operand: None,
            when_then: vec![(
                binary(column_ref(net_column), op, int_literal(0, DataType::Int64)),
                int_literal(1, DataType::Int64),
            )],
            else_expr: Some(Box::new(int_literal(0, DataType::Int64))),
        },
        data_type: DataType::Int64,
        nullable: false,
    }
}

fn build_locator_join_shell(
    left: LogicalPlanNode,
    desc: &JoinRefreshDescriptor,
    locator: &JoinRefreshTargetLocatorBinding,
    left_apply_key: &OutputColumn,
    net_column: &OutputColumn,
    right_apply_key_id: ColumnId,
    locator_file_column_id: ColumnId,
    locator_pos_column_id: ColumnId,
    locator_row_id_column_id: ColumnId,
    locator_last_updated_seq_column_id: ColumnId,
) -> Result<LogicalPlanNode, String> {
    let right_scan = build_target_locator_scan(
        desc,
        locator,
        left_apply_key,
        right_apply_key_id,
        locator_file_column_id,
        locator_pos_column_id,
        locator_row_id_column_id,
        locator_last_updated_seq_column_id,
    );
    let delete_only = binary(
        column_ref(net_column),
        BinOp::Lt,
        int_literal(0, DataType::Int64),
    );
    let apply_key_eq = binary(
        column_ref(left_apply_key),
        BinOp::Eq,
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: right_apply_key_id,
                qualifier: None,
                column: left_apply_key.name.clone(),
            },
            data_type: left_apply_key.data_type.clone(),
            nullable: left_apply_key.nullable,
        },
    );
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Join(LogicalJoinNode {
            join_type: JoinKind::LeftOuter,
            condition: Some(binary(delete_only, BinOp::And, apply_key_eq)),
        }),
        vec![left, right_scan],
        None,
    ))
}

fn build_target_locator_scan(
    desc: &JoinRefreshDescriptor,
    locator: &JoinRefreshTargetLocatorBinding,
    apply_key: &OutputColumn,
    right_apply_key_id: ColumnId,
    locator_file_column_id: ColumnId,
    locator_pos_column_id: ColumnId,
    locator_row_id_column_id: ColumnId,
    locator_last_updated_seq_column_id: ColumnId,
) -> LogicalPlanNode {
    let columns = vec![ColumnDef {
        name: apply_key.name.clone(),
        data_type: apply_key.data_type.clone(),
        nullable: apply_key.nullable,
        write_default: None,
        logical_type: None,
    }];
    let metadata_columns = vec![
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
    ];
    let scan_columns = vec![
        output_column(
            right_apply_key_id,
            &apply_key.name,
            apply_key.data_type.clone(),
            apply_key.nullable,
            false,
        ),
        output_column(
            locator_file_column_id,
            crate::exec::row_position::ICEBERG_FILE_PATH_COL,
            DataType::Utf8,
            false,
            true,
        ),
        output_column(
            locator_pos_column_id,
            crate::exec::row_position::ICEBERG_ROW_POS_COL,
            DataType::Int64,
            false,
            true,
        ),
        output_column(
            locator_row_id_column_id,
            crate::exec::row_position::ICEBERG_ROW_ID_COL,
            DataType::Int64,
            false,
            true,
        ),
        output_column(
            locator_last_updated_seq_column_id,
            crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
            DataType::Int64,
            true,
            true,
        ),
    ];
    LogicalPlanNode::new(
        LogicalPlanKind::Scan(PlanScanNode {
            database: desc.mv_identity.database.clone(),
            table: TableDef {
                name: desc.mv_identity.name.clone(),
                columns,
                iceberg_row_lineage_metadata_columns: metadata_columns,
                source: ScanSource::IcebergMvTargetLocator(IcebergMvTargetLocatorScan {
                    catalog: desc.mv_identity.catalog.clone(),
                    database: desc.mv_identity.database.clone(),
                    table: desc.mv_identity.name.clone(),
                    target_table_uuid: locator.target_table_uuid.clone(),
                    target_snapshot_id: locator.target_snapshot_id,
                    apply_key_column: apply_key.name.clone(),
                    branch_id_column: None,
                }),
            },
            alias: None,
            columns: scan_columns,
            predicates: Vec::new(),
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
        Vec::new(),
        None,
    )
}

fn build_locator_assert_filter(
    locator_join: LogicalPlanNode,
    net_column: &OutputColumn,
    locator_file_column_id: ColumnId,
    locator_pos_column_id: ColumnId,
) -> LogicalPlanNode {
    let insert_or_noop = binary(
        column_ref(net_column),
        BinOp::Ge,
        int_literal(0, DataType::Int64),
    );
    let locator_present = binary(
        is_not_null(locator_column_ref(
            locator_file_column_id,
            crate::exec::row_position::ICEBERG_FILE_PATH_COL,
            DataType::Utf8,
        )),
        BinOp::And,
        is_not_null(locator_column_ref(
            locator_pos_column_id,
            crate::exec::row_position::ICEBERG_ROW_POS_COL,
            DataType::Int64,
        )),
    );
    LogicalPlanNode::new(
        LogicalPlanKind::Filter(PlanFilterNode {
            predicate: assert_true_call(
                binary(insert_or_noop, BinOp::Or, locator_present),
                "join delta DELETE row missing target locator",
            ),
        }),
        vec![locator_join],
        None,
    )
}

fn build_final_coalesce_project(
    input: LogicalPlanNode,
    desc: &JoinRefreshDescriptor,
    payload_mappings: &[(OutputColumn, OutputColumn)],
    apply_key_input: &OutputColumn,
    apply_key_output: &OutputColumn,
    net_column: &OutputColumn,
    locator_file_column_id: ColumnId,
    locator_pos_column_id: ColumnId,
    locator_row_id_column_id: ColumnId,
    locator_last_updated_seq_column_id: ColumnId,
) -> Result<LogicalPlanNode, String> {
    let action_output = mapped_output_column(
        desc,
        JoinRefreshOutputSource::Action(desc.action_column.column_id),
    )?;
    let mut items = payload_mappings
        .iter()
        .map(|(input, output)| project_item_from_source_to_output(input, output))
        .collect::<Vec<_>>();
    items.push(project_item_from_source_to_output(
        apply_key_input,
        apply_key_output,
    ));
    items.push(ProjectItem {
        expr: coalesced_action_expr(net_column),
        output_name: action_output.name,
        output_column_id: action_output.column_id,
    });
    items.push(ProjectItem {
        expr: locator_column_ref(
            locator_file_column_id,
            crate::exec::row_position::ICEBERG_FILE_PATH_COL,
            DataType::Utf8,
        ),
        output_name: crate::exec::row_position::ICEBERG_FILE_PATH_COL.to_string(),
        output_column_id: locator_file_column_id,
    });
    items.push(ProjectItem {
        expr: locator_column_ref(
            locator_pos_column_id,
            crate::exec::row_position::ICEBERG_ROW_POS_COL,
            DataType::Int64,
        ),
        output_name: crate::exec::row_position::ICEBERG_ROW_POS_COL.to_string(),
        output_column_id: locator_pos_column_id,
    });
    items.push(ProjectItem {
        expr: locator_column_ref(
            locator_row_id_column_id,
            crate::exec::row_position::ICEBERG_ROW_ID_COL,
            DataType::Int64,
        ),
        output_name: crate::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
        output_column_id: locator_row_id_column_id,
    });
    items.push(ProjectItem {
        expr: locator_column_ref(
            locator_last_updated_seq_column_id,
            crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
            DataType::Int64,
        ),
        output_name: crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
        output_column_id: locator_last_updated_seq_column_id,
    });
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            items,
            output_qualifier: None,
        }),
        vec![input],
        None,
    ))
}

fn coalesced_action_expr(net_column: &OutputColumn) -> TypedExpr {
    let case = TypedExpr {
        kind: ExprKind::Case {
            operand: None,
            when_then: vec![(
                binary(
                    column_ref(net_column),
                    BinOp::Gt,
                    int_literal(0, DataType::Int64),
                ),
                int_literal(
                    i64::from(crate::exec::change_op::CHANGE_OP_INSERT),
                    DataType::Int8,
                ),
            )],
            else_expr: Some(Box::new(int_literal(
                i64::from(crate::exec::change_op::CHANGE_OP_DELETE),
                DataType::Int8,
            ))),
        },
        data_type: DataType::Int8,
        nullable: false,
    };
    TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(case),
            target: DataType::Int8,
        },
        data_type: DataType::Int8,
        nullable: false,
    }
}

fn payload_source_output_columns(
    desc: &JoinRefreshDescriptor,
) -> Result<Vec<(OutputColumn, OutputColumn)>, String> {
    let columns = desc
        .output_mappings
        .iter()
        .filter_map(|mapping| match mapping.source {
            JoinRefreshOutputSource::Payload(column_id) => Some(
                desc.payload_columns
                    .iter()
                    .find(|column| column.column_id == column_id)
                    .cloned()
                    .map(|source| (source, mapping.mv_output_column.clone()))
                    .ok_or_else(|| {
                        format!(
                            "join refresh coalesce references unknown payload column {column_id}"
                        )
                    }),
            ),
            JoinRefreshOutputSource::Action(_) | JoinRefreshOutputSource::JoinApplyKey(_) => None,
        })
        .collect::<Result<Vec<_>, _>>()?;
    if columns.is_empty() {
        return Err("join refresh coalesce requires payload output columns".to_string());
    }
    Ok(columns)
}

fn validate_coalesce_payload_output_names(columns: &[OutputColumn]) -> Result<(), String> {
    for column in columns {
        if is_reserved_coalesce_payload_output_name(&column.name) {
            return Err(format!(
                "join refresh coalesce found reserved target locator metadata column `{}` in payload output",
                column.name
            ));
        }
    }
    Ok(())
}

fn is_reserved_coalesce_payload_output_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "net"
            | "pending_inserts"
            | "pending_deletes"
            | "__pending_insert_count"
            | "__pending_delete_count"
            | "__nr_join_delta_change_stream"
            | "__nr_join_delta_payload_coalesced"
            | "__nr_join_delta_key_shape"
            | "__nr_join_delta_coalesced"
            | "__nr_join_delta_target_locator"
            | crate::exec::row_position::ICEBERG_FILE_PATH_COL
            | crate::exec::row_position::ICEBERG_ROW_POS_COL
            | crate::exec::row_position::ICEBERG_ROW_ID_COL
            | crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL
    )
}

fn mapped_output_column(
    desc: &JoinRefreshDescriptor,
    source: JoinRefreshOutputSource,
) -> Result<OutputColumn, String> {
    desc.output_mappings
        .iter()
        .find(|mapping| mapping.source == source)
        .map(|mapping| mapping.mv_output_column.clone())
        .ok_or_else(|| {
            format!("join refresh coalesce missing output mapping for source {source:?}")
        })
}

fn project_item_from_source_to_output(source: &OutputColumn, output: &OutputColumn) -> ProjectItem {
    ProjectItem {
        expr: column_ref(source),
        output_name: output.name.clone(),
        output_column_id: output.column_id,
    }
}

fn output_column(
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

fn output_column_from_factory(
    factory: &mut ColumnRefFactory,
    name: &str,
    data_type: DataType,
    nullable: bool,
    is_internal: bool,
) -> OutputColumn {
    let column_id = factory.create(None, name.to_string(), data_type.clone(), nullable);
    output_column(column_id, name, data_type, nullable, is_internal)
}

fn binary(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn int_literal(value: i64, data_type: DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(value)),
        data_type,
        nullable: false,
    }
}

fn assert_true_call(predicate: TypedExpr, message: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "assert_true".to_string(),
            args: vec![predicate, string_literal(message)],
            distinct: false,
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn is_not_null(expr: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::IsNull {
            expr: Box::new(expr),
            negated: true,
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn locator_column_ref(column_id: ColumnId, name: &str, data_type: DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id,
            qualifier: None,
            column: name.to_string(),
        },
        data_type,
        nullable: true,
    }
}

fn validate_apply_key_project_output_ids(
    desc: &JoinRefreshDescriptor,
    apply_key_column_id: u32,
    action_column_id: u32,
) -> Result<(), String> {
    let expected_apply_key = ColumnId(apply_key_column_id);
    let expected_action = ColumnId(action_column_id);
    let apply_key_output = desc
        .output_mappings
        .iter()
        .find(|mapping| {
            mapping.source
                == JoinRefreshOutputSource::JoinApplyKey(desc.join_apply_key_column.column_id)
        })
        .map(|mapping| mapping.mv_output_column.column_id)
        .ok_or_else(|| {
            "join refresh apply-key project missing join apply-key output mapping".to_string()
        })?;
    if apply_key_output != expected_apply_key {
        return Err(format!(
            "join refresh apply-key project apply-key output id mismatch: descriptor has {apply_key_output}, builder requested {expected_apply_key}"
        ));
    }

    let action_output = desc
        .output_mappings
        .iter()
        .find(|mapping| {
            mapping.source == JoinRefreshOutputSource::Action(desc.action_column.column_id)
        })
        .map(|mapping| mapping.mv_output_column.column_id)
        .ok_or_else(|| {
            "join refresh apply-key project missing action output mapping".to_string()
        })?;
    if action_output != expected_action {
        return Err(format!(
            "join refresh apply-key project action output id mismatch: descriptor has {action_output}, builder requested {expected_action}"
        ));
    }
    Ok(())
}

fn project_item_for_mapping(
    mapping: &JoinRefreshOutputMapping,
    desc: &JoinRefreshDescriptor,
    input_columns: &[OutputColumn],
    left_uuid: &str,
    right_uuid: &str,
    action_projection: JoinApplyActionProjection,
) -> Result<ProjectItem, String> {
    let expr = match mapping.source {
        JoinRefreshOutputSource::Payload(column_id) => {
            let source = desc
                .payload_columns
                .iter()
                .find(|column| column.column_id == column_id)
                .ok_or_else(|| {
                    format!("join refresh apply-key project references unknown payload column {column_id}")
                })?;
            validate_input_column(input_columns, source)?;
            column_ref(source)
        }
        JoinRefreshOutputSource::Action(column_id) => {
            if column_id != desc.action_column.column_id {
                return Err(format!(
                    "join refresh apply-key project references unknown action column {column_id}"
                ));
            }
            match action_projection {
                JoinApplyActionProjection::InputColumn => {
                    validate_input_column(input_columns, &desc.action_column)?;
                    TypedExpr {
                        kind: ExprKind::Cast {
                            expr: Box::new(column_ref(&desc.action_column)),
                            target: DataType::Int8,
                        },
                        data_type: DataType::Int8,
                        nullable: false,
                    }
                }
                JoinApplyActionProjection::ConstantInsert => TypedExpr {
                    kind: ExprKind::Cast {
                        expr: Box::new(int_literal(
                            i64::from(crate::exec::change_op::CHANGE_OP_INSERT),
                            DataType::Int8,
                        )),
                        target: DataType::Int8,
                    },
                    data_type: DataType::Int8,
                    nullable: false,
                },
            }
        }
        JoinRefreshOutputSource::JoinApplyKey(column_id) => {
            if column_id != desc.join_apply_key_column.column_id {
                return Err(format!(
                    "join refresh apply-key project references unknown join apply-key column {column_id}"
                ));
            }
            validate_input_column(input_columns, &desc.left_row_id_column)?;
            validate_input_column(input_columns, &desc.right_row_id_column)?;
            join_row_key_expr(desc, left_uuid, right_uuid)
        }
    };

    Ok(ProjectItem {
        expr,
        output_name: mapping.mv_output_column.name.clone(),
        output_column_id: mapping.mv_output_column.column_id,
    })
}

fn validate_input_column(
    input_columns: &[OutputColumn],
    expected: &OutputColumn,
) -> Result<(), String> {
    let matches = input_columns
        .iter()
        .filter(|column| column.column_id == expected.column_id)
        .collect::<Vec<_>>();
    let [actual] = matches.as_slice() else {
        if matches.is_empty() {
            return Err(format!(
                "join refresh apply-key project missing input column {} `{}`",
                expected.column_id, expected.name
            ));
        }
        return Err(format!(
            "join refresh apply-key project found duplicate input column id {}",
            expected.column_id
        ));
    };
    if !actual.name.eq_ignore_ascii_case(&expected.name)
        || actual.data_type != expected.data_type
        || actual.nullable != expected.nullable
        || !input_internal_matches(actual, expected)
    {
        return Err(format!(
            "join refresh apply-key project input column {} `{}` shape mismatch: actual name=`{}`, type={:?}, nullable={}, internal={}; expected type={:?}, nullable={}, internal={}",
            expected.column_id,
            expected.name,
            actual.name,
            actual.data_type,
            actual.nullable,
            actual.is_internal,
            expected.data_type,
            expected.nullable,
            expected.is_internal
        ));
    }
    Ok(())
}

fn input_internal_matches(actual: &OutputColumn, expected: &OutputColumn) -> bool {
    actual.is_internal == expected.is_internal
        || (expected.is_internal && !actual.is_internal && is_internal_output_name(&actual.name))
}

fn is_internal_output_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_ID_COL)
        || name.eq_ignore_ascii_case(crate::mv::persistence::schema::JOIN_APPLY_KEY_COLUMN_NAME)
}

fn join_row_key_expr(desc: &JoinRefreshDescriptor, left_uuid: &str, right_uuid: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "join_row_key".to_string(),
            args: vec![
                string_literal(left_uuid),
                column_ref(&desc.left_row_id_column),
                string_literal(right_uuid),
                column_ref(&desc.right_row_id_column),
            ],
            distinct: false,
        },
        data_type: DataType::Utf8,
        nullable: false,
    }
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

fn string_literal(value: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::String(value.to_string())),
        data_type: DataType::Utf8,
        nullable: false,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{HashMap, HashSet};
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use crate::sql::analysis::{ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::imv_rewrite::join_refresh_descriptor::{
        JoinRefreshBranchDescriptor, JoinRefreshBranchSide, JoinRefreshDescriptor,
        JoinRefreshJoinKeyPair, JoinRefreshMode, JoinRefreshMvIdentity, JoinRefreshOutputMapping,
        JoinRefreshOutputSource,
    };
    use crate::sql::planner::logical::{LogicalPlanKind, LogicalPlanNode, LogicalUnionNode};
    use crate::sql::planner::optimizer_bridge::logical::{to_logical_plan, to_optimizer_expr};
    use crate::sql::planner::payload::PlanValuesNode;

    #[test]
    fn apply_key_project_uses_output_mappings_and_validates_sources() {
        let input = test_values_plan(vec![
            out(1, "k", DataType::Int64, false, false),
            out(
                2,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            out(
                3,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            out(
                4,
                crate::exec::change_op::CHANGE_OP_COLUMN,
                DataType::Int8,
                false,
                true,
            ),
        ]);
        let desc = test_descriptor(JoinRefreshMode::AppendOnly);

        let plan =
            super::build_join_apply_key_project(input, &desc, "left-uuid", "right-uuid", 90, 91)
                .expect("apply-key project");

        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.items.len(), 3);
        assert_payload_item(&project.items[0]);
        assert_join_apply_key_item(&project.items[1]);
        assert_action_item(&project.items[2]);
    }

    #[test]
    fn apply_key_project_rejects_missing_input_source_column() {
        let input = test_values_plan(vec![
            out(1, "k", DataType::Int64, false, false),
            out(
                2,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            out(
                4,
                crate::exec::change_op::CHANGE_OP_COLUMN,
                DataType::Int8,
                false,
                true,
            ),
        ]);
        let desc = test_descriptor(JoinRefreshMode::AppendOnly);

        let err =
            super::build_join_apply_key_project(input, &desc, "left-uuid", "right-uuid", 90, 91)
                .expect_err("missing right row-id should fail closed");

        assert!(err.contains("missing input column c3"), "err={err}");
    }

    #[test]
    fn apply_key_project_rejects_output_id_mismatch() {
        let input = test_values_plan(test_input_columns());
        let desc = test_descriptor(JoinRefreshMode::AppendOnly);

        let err =
            super::build_join_apply_key_project(input, &desc, "left-uuid", "right-uuid", 900, 91)
                .expect_err("apply-key id mismatch should fail closed");

        assert!(err.contains("apply-key output id mismatch"), "err={err}");
    }

    #[test]
    fn apply_key_project_can_use_constant_insert_action() {
        let input = test_values_plan(vec![
            out(1, "k", DataType::Int64, false, false),
            out(
                2,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            out(
                3,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
        ]);
        let desc = test_descriptor(JoinRefreshMode::Full);

        let plan = super::build_join_apply_key_project_with_constant_insert_action(
            input,
            &desc,
            "left-uuid",
            "right-uuid",
            90,
            91,
        )
        .expect("apply-key project with constant insert action");

        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.items.len(), 3);
        assert_payload_item(&project.items[0]);
        assert_join_apply_key_item(&project.items[1]);
        assert_constant_insert_action_item(&project.items[2]);
    }

    #[test]
    fn coalesce_plan_contains_aggregate_and_target_locator_join() {
        let desc = test_coalesce_descriptor();
        let branch_union = test_branch_union(&desc);
        let mut factory = test_coalesce_factory();

        let plan = super::build_join_delta_coalesce_plan_with_locator(
            branch_union,
            &desc,
            &test_locator_binding(),
            &mut factory,
            100,
            101,
            102,
            103,
            104,
        )
        .expect("coalesce plan");

        assert!(contains_aggregate(&plan));
        assert!(contains_target_locator_join(&plan));
        assert!(contains_string_literal(
            &plan,
            "join delta multiple pending payloads for key"
        ));
        assert_final_coalesce_output(&plan);
        assert_target_locator_binding(&plan);
    }

    #[test]
    fn coalesce_plan_registers_factory_metadata_for_internal_columns() {
        let desc = test_coalesce_descriptor();
        let branch_union = test_branch_union(&desc);
        let mut factory = test_coalesce_factory();

        let plan = super::build_join_delta_coalesce_plan_with_locator(
            branch_union,
            &desc,
            &test_locator_binding(),
            &mut factory,
            100,
            101,
            102,
            103,
            104,
        )
        .expect("coalesce plan");

        let generated_columns = collect_generated_coalesce_columns(&plan);
        assert_eq!(
            generated_columns.len(),
            4,
            "expected key-shape apply key, two pending counts, and locator apply key"
        );
        for column in generated_columns {
            let metadata = factory.get(column.column_id);
            assert_eq!(metadata.name, column.name);
            assert_eq!(metadata.data_type, column.data_type);
            assert_eq!(metadata.nullable, column.nullable);
        }
    }

    #[test]
    fn coalesce_plan_query_rewrite_keeps_aggregate_args_in_child_scope() {
        let desc = test_coalesce_descriptor();
        let branch_union = test_branch_union(&desc);
        let mut factory = test_coalesce_factory();

        let plan = super::build_join_delta_coalesce_plan_with_locator(
            branch_union,
            &desc,
            &test_locator_binding(),
            &mut factory,
            100,
            101,
            102,
            103,
            104,
        )
        .expect("coalesce plan");

        let rewritten = run_query_rewrite_pipeline(plan);

        assert_aggregate_args_resolve_to_child_outputs(&rewritten);
    }

    #[test]
    fn coalesce_plan_lowers_after_physical_optimization() {
        let desc = test_coalesce_descriptor();
        let branch_union = test_branch_union(&desc);
        let mut factory = test_coalesce_factory();

        let plan = super::build_join_delta_coalesce_plan_with_locator(
            branch_union,
            &desc,
            &test_locator_binding(),
            &mut factory,
            100,
            101,
            102,
            103,
            104,
        )
        .expect("coalesce plan");
        let optimized_tree = optimize_for_test(plan);
        let connectors = crate::connector::ConnectorRegistry::default();
        let controls = crate::connector::FixtureControlResolver::new(connectors.clone());

        let result = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
            .and_then(crate::sql::planner::pipeline::build_distributed_plan)
            .and_then(|distributed_plan| {
                let prepared = crate::query_execution::preparation::prepare_fragments(
                    &distributed_plan,
                    &connectors,
                    &controls,
                    &crate::connector::test_request_context(),
                    None,
                    crate::query_execution::preparation::ScanPreparationOptions::default(),
                )?;
                crate::protocol::native::encode::encode_native_fragment_bundle(
                    &distributed_plan,
                    &prepared,
                )
            });

        if let Err(err) = result {
            assert!(
                !err.contains("ColumnId") && !err.contains("cannot be resolved"),
                "coalesce plan must not fail aggregate argument binding after physical optimization: {err}"
            );
        }
    }

    #[test]
    fn coalesce_plan_rejects_descriptor_without_locator() {
        let mut desc = test_coalesce_descriptor();
        desc.needs_target_locator = false;
        let mut factory = test_coalesce_factory();

        let err = super::build_join_delta_coalesce_plan_with_locator(
            test_branch_union(&desc),
            &desc,
            &test_locator_binding(),
            &mut factory,
            100,
            101,
            102,
            103,
            104,
        )
        .expect_err("coalesce requires locator");

        assert!(err.contains("requires target locator"), "err={err}");
    }

    #[test]
    fn coalesce_plan_rejects_generated_column_id_collision() {
        let desc = test_coalesce_descriptor();
        let mut factory = test_coalesce_factory();
        let err = super::build_join_delta_coalesce_plan_with_locator(
            test_branch_union(&desc),
            &desc,
            &test_locator_binding(),
            &mut factory,
            80,
            101,
            102,
            103,
            104,
        )
        .expect_err("net id collision should fail closed");

        assert!(err.contains("collides with existing column"), "err={err}");
    }

    #[test]
    fn coalesce_plan_rejects_duplicate_generated_column_ids() {
        let desc = test_coalesce_descriptor();
        let mut factory = test_coalesce_factory();
        let err = super::build_join_delta_coalesce_plan_with_locator(
            test_branch_union(&desc),
            &desc,
            &test_locator_binding(),
            &mut factory,
            100,
            100,
            102,
            103,
            104,
        )
        .expect_err("duplicate generated ids should fail closed");

        assert!(err.contains("is duplicated"), "err={err}");
    }

    #[test]
    fn coalesce_plan_rejects_reserved_locator_payload_output_name() {
        for reserved in [
            crate::exec::row_position::ICEBERG_FILE_PATH_COL,
            crate::exec::row_position::ICEBERG_ROW_POS_COL,
        ] {
            let mut desc = test_coalesce_descriptor();
            desc.output_mappings[0].mv_output_column.name = reserved.to_string();
            let mut factory = test_coalesce_factory();

            let err = super::build_join_delta_coalesce_plan_with_locator(
                test_branch_union(&desc),
                &desc,
                &test_locator_binding(),
                &mut factory,
                100,
                101,
                102,
                103,
                104,
            )
            .expect_err("reserved locator payload output should fail closed");

            assert!(
                err.contains("reserved target locator") && err.contains(reserved),
                "err={err}"
            );
        }
    }

    fn contains_aggregate(plan: &LogicalPlanNode) -> bool {
        matches!(&plan.kind, LogicalPlanKind::Aggregate(_))
            || plan.children.iter().any(contains_aggregate)
    }

    fn contains_target_locator_join(plan: &LogicalPlanNode) -> bool {
        matches!(&plan.kind, LogicalPlanKind::Join(join) if join.join_type == JoinKind::LeftOuter)
            || plan.children.iter().any(contains_target_locator_join)
    }

    fn contains_string_literal(plan: &LogicalPlanNode, needle: &str) -> bool {
        plan_exprs(plan)
            .iter()
            .any(|expr| expr_contains_string_literal(expr, needle))
    }

    fn plan_exprs(plan: &LogicalPlanNode) -> Vec<&crate::sql::analysis::TypedExpr> {
        let mut exprs = Vec::new();
        match &plan.kind {
            LogicalPlanKind::Project(project) => {
                exprs.extend(project.items.iter().map(|item| &item.expr));
            }
            LogicalPlanKind::Filter(filter) => exprs.push(&filter.predicate),
            LogicalPlanKind::Aggregate(aggregate) => {
                exprs.extend(aggregate.group_by.iter());
                for call in &aggregate.aggregates {
                    exprs.extend(call.args.iter());
                }
            }
            LogicalPlanKind::Join(join) => {
                if let Some(condition) = &join.condition {
                    exprs.push(condition);
                }
            }
            _ => {}
        }
        for child in &plan.children {
            exprs.extend(plan_exprs(child));
        }
        exprs
    }

    fn expr_contains_string_literal(expr: &crate::sql::analysis::TypedExpr, needle: &str) -> bool {
        match &expr.kind {
            ExprKind::Literal(LiteralValue::String(value)) => value == needle,
            ExprKind::BinaryOp { left, right, .. } => {
                expr_contains_string_literal(left, needle)
                    || expr_contains_string_literal(right, needle)
            }
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => args
                .iter()
                .any(|arg| expr_contains_string_literal(arg, needle)),
            ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::UnaryOp { expr, .. }
            | ExprKind::Nested(expr) => expr_contains_string_literal(expr, needle),
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_deref()
                    .is_some_and(|expr| expr_contains_string_literal(expr, needle))
                    || when_then.iter().any(|(when, then)| {
                        expr_contains_string_literal(when, needle)
                            || expr_contains_string_literal(then, needle)
                    })
                    || else_expr
                        .as_deref()
                        .is_some_and(|expr| expr_contains_string_literal(expr, needle))
            }
            _ => false,
        }
    }

    fn assert_final_coalesce_output(plan: &LogicalPlanNode) {
        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected final Project");
        };
        let output = project
            .items
            .iter()
            .map(|item| (item.output_name.as_str(), item.output_column_id))
            .collect::<Vec<_>>();
        assert_eq!(
            output,
            vec![
                ("mv_k", ColumnId(80)),
                (
                    crate::mv::persistence::schema::JOIN_APPLY_KEY_COLUMN_NAME,
                    ColumnId(90),
                ),
                (crate::exec::change_op::CHANGE_OP_COLUMN, ColumnId(91)),
                (
                    crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                    ColumnId(101)
                ),
                (
                    crate::exec::row_position::ICEBERG_ROW_POS_COL,
                    ColumnId(102)
                ),
                (crate::exec::row_position::ICEBERG_ROW_ID_COL, ColumnId(103)),
                (
                    crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
                    ColumnId(104)
                ),
            ]
        );
    }

    fn assert_target_locator_binding(plan: &LogicalPlanNode) {
        let Some(scan) = find_target_locator_scan(plan) else {
            panic!("expected target locator scan");
        };
        assert_eq!(scan.target_table_uuid, "target-uuid");
        assert_eq!(scan.target_snapshot_id, Some(77));
    }

    fn find_target_locator_scan(
        plan: &LogicalPlanNode,
    ) -> Option<&crate::sql::planner::table::IcebergMvTargetLocatorScan> {
        if let LogicalPlanKind::Scan(scan) = &plan.kind
            && let crate::sql::planner::table::ScanSource::IcebergMvTargetLocator(locator) =
                &scan.table.source
        {
            return Some(locator);
        }
        plan.children.iter().find_map(find_target_locator_scan)
    }

    fn collect_generated_coalesce_columns(plan: &LogicalPlanNode) -> Vec<OutputColumn> {
        let mut columns = Vec::new();
        collect_generated_coalesce_columns_inner(plan, &mut columns);
        columns
    }

    fn collect_generated_coalesce_columns_inner(
        plan: &LogicalPlanNode,
        columns: &mut Vec<OutputColumn>,
    ) {
        match &plan.kind {
            LogicalPlanKind::Aggregate(aggregate) => {
                columns.extend(
                    aggregate
                        .output_columns
                        .iter()
                        .filter(|column| is_generated_coalesce_column(column))
                        .cloned(),
                );
            }
            LogicalPlanKind::Scan(scan) => {
                columns.extend(
                    scan.columns
                        .iter()
                        .filter(|column| is_generated_coalesce_column(column))
                        .cloned(),
                );
            }
            _ => {}
        }
        for child in &plan.children {
            collect_generated_coalesce_columns_inner(child, columns);
        }
    }

    fn is_generated_coalesce_column(column: &OutputColumn) -> bool {
        column.column_id.0 > 104
            && matches!(
                column.name.as_str(),
                crate::mv::persistence::schema::JOIN_APPLY_KEY_COLUMN_NAME
                    | "__pending_insert_count"
                    | "__pending_delete_count"
            )
    }

    fn test_coalesce_factory() -> crate::sql::column_id::ColumnRefFactory {
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        factory.reserve_until(109);
        factory
    }

    fn test_locator_binding() -> super::JoinRefreshTargetLocatorBinding {
        super::JoinRefreshTargetLocatorBinding {
            target_table_uuid: "target-uuid".to_string(),
            target_snapshot_id: Some(77),
        }
    }

    fn run_query_rewrite_pipeline(plan: LogicalPlanNode) -> LogicalPlanNode {
        let pipeline = query_rewrite_pipeline();
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_query_stats_input(
            crate::sql::optimizer::stats_input::OptimizerStatsInput::from_test_table_statistics(
                &HashMap::new(),
            ),
        );
        let mut scalars = ScalarArena::new();
        let opt_plan = to_optimizer_expr(&plan, &mut scalars);
        let arena_rc = Rc::new(RefCell::new(scalars));
        ctx.set_scalar_arena(arena_rc.clone());
        let opt_result = pipeline
            .rewrite(opt_plan, &mut ctx)
            .expect("query rewrite pipeline");
        let arena = arena_rc.borrow();
        to_logical_plan(opt_result, &arena)
    }

    fn optimize_for_test(plan: LogicalPlanNode) -> crate::sql::optimizer::OptimizedOperatorNode {
        let mut scalar_arena = ScalarArena::new();
        let optimizer_expr = to_optimizer_expr(&plan, &mut scalar_arena);
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        factory.reserve_until(200);
        crate::sql::optimizer::optimize_with_test_table_statistics(
            optimizer_expr,
            scalar_arena,
            &HashMap::new(),
            factory,
            Vec::new(),
            &crate::sql::optimizer::options::SessionOptimizerSettings::default(),
        )
        .expect("physical optimization")
    }

    fn assert_aggregate_args_resolve_to_child_outputs(plan: &LogicalPlanNode) {
        if let LogicalPlanKind::Aggregate(aggregate) = &plan.kind {
            let child_output_ids = crate::sql::planner::plan_output_columns(plan.unary_input())
                .expect("aggregate child output columns")
                .into_iter()
                .map(|column| column.column_id)
                .collect::<HashSet<_>>();
            for call in &aggregate.aggregates {
                let mut refs = HashSet::new();
                for arg in &call.args {
                    collect_column_refs(arg, &mut refs);
                }
                for sort_item in &call.order_by {
                    collect_column_refs(&sort_item.expr, &mut refs);
                }
                for column_id in refs {
                    assert!(
                        child_output_ids.contains(&column_id),
                        "aggregate `{}` references {column_id}, but child outputs are {:?}",
                        call.name,
                        child_output_ids
                    );
                }
            }
        }
        for child in &plan.children {
            assert_aggregate_args_resolve_to_child_outputs(child);
        }
    }

    fn collect_column_refs(expr: &crate::sql::analysis::TypedExpr, refs: &mut HashSet<ColumnId>) {
        match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } => {
                if *column_id != ColumnId::UNSET {
                    refs.insert(*column_id);
                }
            }
            ExprKind::BinaryOp { left, right, .. } => {
                collect_column_refs(left, refs);
                collect_column_refs(right, refs);
            }
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
                for arg in args {
                    collect_column_refs(arg, refs);
                }
                if let ExprKind::AggregateCall { order_by, .. } = &expr.kind {
                    for item in order_by {
                        collect_column_refs(&item.expr, refs);
                    }
                }
            }
            ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::UnaryOp { expr, .. }
            | ExprKind::Nested(expr) => collect_column_refs(expr, refs),
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(operand) = operand {
                    collect_column_refs(operand, refs);
                }
                for (when, then) in when_then {
                    collect_column_refs(when, refs);
                    collect_column_refs(then, refs);
                }
                if let Some(else_expr) = else_expr {
                    collect_column_refs(else_expr, refs);
                }
            }
            ExprKind::InList { expr, list, .. } => {
                collect_column_refs(expr, refs);
                for item in list {
                    collect_column_refs(item, refs);
                }
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                collect_column_refs(expr, refs);
                collect_column_refs(low, refs);
                collect_column_refs(high, refs);
            }
            ExprKind::Like { expr, pattern, .. } => {
                collect_column_refs(expr, refs);
                collect_column_refs(pattern, refs);
            }
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    collect_column_refs(arg, refs);
                }
                for expr in partition_by {
                    collect_column_refs(expr, refs);
                }
                for item in order_by {
                    collect_column_refs(&item.expr, refs);
                }
            }
            ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
                collect_column_refs(body, refs);
            }
            ExprKind::IsTruthValue { expr, .. } => collect_column_refs(expr, refs),
            ExprKind::LambdaParamRef { .. }
            | ExprKind::Literal(_)
            | ExprKind::SubqueryPlaceholder { .. } => {}
        }
    }

    fn test_branch_union(desc: &JoinRefreshDescriptor) -> LogicalPlanNode {
        let mut output_columns = desc.payload_columns.clone();
        output_columns.push(desc.action_column.clone());
        output_columns.push(desc.join_apply_key_column.clone());
        let branch = test_values_plan(output_columns.clone());
        LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns,
            }),
            vec![branch.clone(), branch],
            None,
        )
    }

    fn test_coalesce_descriptor() -> JoinRefreshDescriptor {
        let mut desc = test_descriptor(JoinRefreshMode::Coalesce);
        desc.branches = vec![
            JoinRefreshBranchDescriptor {
                side: JoinRefreshBranchSide::LeftDeltaRightSnapshot,
                action_column_id: desc.action_column.column_id,
            },
            JoinRefreshBranchDescriptor {
                side: JoinRefreshBranchSide::LeftSnapshotRightDelta,
                action_column_id: desc.action_column.column_id,
            },
        ];
        desc.needs_target_locator = true;
        desc
    }

    fn test_input_columns() -> Vec<OutputColumn> {
        vec![
            out(1, "k", DataType::Int64, false, false),
            out(
                2,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            out(
                3,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            out(
                4,
                crate::exec::change_op::CHANGE_OP_COLUMN,
                DataType::Int8,
                false,
                true,
            ),
        ]
    }

    fn test_values_plan(columns: Vec<OutputColumn>) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns,
            }),
            Vec::new(),
            None,
        )
    }

    fn test_descriptor(mode: JoinRefreshMode) -> JoinRefreshDescriptor {
        let payload = out(1, "k", DataType::Int64, false, false);
        let payload_output = out(80, "mv_k", DataType::Int64, false, false);
        let action = out(
            4,
            crate::exec::change_op::CHANGE_OP_COLUMN,
            DataType::Int8,
            false,
            true,
        );
        let action_output = out(
            91,
            crate::exec::change_op::CHANGE_OP_COLUMN,
            DataType::Int8,
            false,
            true,
        );
        let join_apply_key = out(
            5,
            crate::mv::persistence::schema::JOIN_APPLY_KEY_COLUMN_NAME,
            DataType::Utf8,
            false,
            true,
        );
        let join_apply_key_output = out(
            90,
            crate::mv::persistence::schema::JOIN_APPLY_KEY_COLUMN_NAME,
            DataType::Utf8,
            false,
            true,
        );

        let branches = if mode == JoinRefreshMode::Full {
            Vec::new()
        } else {
            vec![JoinRefreshBranchDescriptor {
                side: JoinRefreshBranchSide::LeftDeltaRightSnapshot,
                action_column_id: action.column_id,
            }]
        };

        JoinRefreshDescriptor {
            mode,
            mv_identity: JoinRefreshMvIdentity {
                catalog: "ice".to_string(),
                database: "db".to_string(),
                name: "mv_join".to_string(),
            },
            left_base_fqn: "ice.db.left_t".to_string(),
            right_base_fqn: "ice.db.right_t".to_string(),
            left_row_id_column: out(
                2,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            right_row_id_column: out(
                3,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            action_column: action.clone(),
            join_apply_key_column: join_apply_key.clone(),
            payload_columns: vec![payload.clone()],
            join_key_pairs: vec![JoinRefreshJoinKeyPair {
                left_column: out(6, "left_k", DataType::Int64, false, false),
                right_column: out(7, "right_k", DataType::Int64, false, false),
            }],
            output_mappings: vec![
                JoinRefreshOutputMapping {
                    mv_output_column: payload_output,
                    source: JoinRefreshOutputSource::Payload(payload.column_id),
                },
                JoinRefreshOutputMapping {
                    mv_output_column: join_apply_key_output,
                    source: JoinRefreshOutputSource::JoinApplyKey(join_apply_key.column_id),
                },
                JoinRefreshOutputMapping {
                    mv_output_column: action_output,
                    source: JoinRefreshOutputSource::Action(action.column_id),
                },
            ],
            branches,
            needs_target_locator: false,
        }
    }

    fn out(
        id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
        is_internal: bool,
    ) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal,
        }
    }

    fn assert_payload_item(item: &ProjectItem) {
        assert_eq!(item.output_name, "mv_k");
        assert_eq!(item.output_column_id, ColumnId(80));
        assert_column_ref(&item.expr.kind, ColumnId(1), "k");
    }

    fn assert_join_apply_key_item(item: &ProjectItem) {
        assert!(
            item.output_name
                .eq_ignore_ascii_case(crate::mv::persistence::schema::JOIN_APPLY_KEY_COLUMN_NAME)
        );
        assert_eq!(item.output_column_id, ColumnId(90));
        let ExprKind::FunctionCall { name, args, .. } = &item.expr.kind else {
            panic!("expected join apply-key function call");
        };
        assert_eq!(name, "join_row_key");
        assert_eq!(args.len(), 4);
        assert_string_literal(&args[0].kind, "left-uuid");
        assert_column_ref(
            &args[1].kind,
            ColumnId(2),
            crate::exec::row_position::ICEBERG_ROW_ID_COL,
        );
        assert_string_literal(&args[2].kind, "right-uuid");
        assert_column_ref(
            &args[3].kind,
            ColumnId(3),
            crate::exec::row_position::ICEBERG_ROW_ID_COL,
        );
    }

    fn assert_action_item(item: &ProjectItem) {
        assert!(
            item.output_name
                .eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
        );
        assert_eq!(item.output_column_id, ColumnId(91));
        let ExprKind::Cast { target, .. } = &item.expr.kind else {
            panic!("expected action cast");
        };
        assert_eq!(target, &DataType::Int8);
        let ExprKind::Cast { expr, .. } = &item.expr.kind else {
            unreachable!("cast already matched");
        };
        assert_column_ref(
            &expr.kind,
            ColumnId(4),
            crate::exec::change_op::CHANGE_OP_COLUMN,
        );
    }

    fn assert_constant_insert_action_item(item: &ProjectItem) {
        assert!(
            item.output_name
                .eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
        );
        assert_eq!(item.output_column_id, ColumnId(91));
        let ExprKind::Cast { expr, target } = &item.expr.kind else {
            panic!("expected action cast");
        };
        assert_eq!(target, &DataType::Int8);
        let ExprKind::Literal(LiteralValue::Int(value)) = &expr.kind else {
            panic!("expected action literal");
        };
        assert_eq!(*value, i64::from(crate::exec::change_op::CHANGE_OP_INSERT));
    }

    fn assert_string_literal(kind: &ExprKind, expected: &str) {
        let ExprKind::Literal(LiteralValue::String(actual)) = kind else {
            panic!("expected string literal");
        };
        assert_eq!(actual, expected);
    }

    fn assert_column_ref(kind: &ExprKind, expected_id: ColumnId, expected_name: &str) {
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = kind
        else {
            panic!("expected column ref");
        };
        assert_eq!(*column_id, expected_id);
        assert!(column.eq_ignore_ascii_case(expected_name));
    }
}
