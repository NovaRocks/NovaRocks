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

//! Fragment decoding of the two dataflow write nodes.
//!
//! A `TableWriter` is an ordinary unary processor: it names one exact
//! query-leased catalog runtime, one query-local logical write target, and the
//! immutable recipe copied to every placement serving that target. Decoding it
//! means resolving the exact write role binding this attempt leased, turning
//! the carried recipe back into a provider value through that binding's own
//! decoder, and joining it to the attempt-local facts a driver needs.
//!
//! Two budgets exist and only one belongs here. The single-handle cap is
//! re-verified at this ingress, because this is where an untrusted carrier
//! enters. The query-wide unique-handle total is deliberately *not*: this
//! decoder sees one carrier at a time and can never reconstruct the query's
//! unique set, so restating that budget here could only produce a wrong answer.
//!
//! A `TableFinish` is n-ary — the planner gives it one exchange receiver per
//! writer fragment — and knows only which logical targets are legal. It gets a
//! structural carrier validator and nothing else: no commit handle, no control
//! binding, and no way to turn a fragment into a provider value.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::node::table_finish::TableFinishNode;
use novarocks_execution::exec::node::table_write_aggregate::{
    WriterFinalAggregateCall, WriterFinalAggregatePlan, WriterGroupedUnpivotMapping,
    WriterGroupedUnpivotPlan, WriterPartialAggregateCall, WriterPartialAggregatePlan,
};
use novarocks_execution::exec::node::table_write_relation::{
    RootWriteResultRelationSchema, WriterMultiplexRelationSchema,
};
use novarocks_execution::exec::node::table_writer::{
    TableWriterInputProjection, TableWriterNode, TableWriterPhysicalContextTemplate,
};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_execution::runtime::query_options::query_expire_durations;
use novarocks_proto_codec::connector_write::{ConnectorWriteHandleDecoder, ValidatedWriterHandle};
use novarocks_proto_codec::{FieldPath, arrow_physical};
use novarocks_proto_models::plan;
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_spi::connector::write_stack::{
    ROOT_WRITE_RESULT_COLUMN_COUNT, ROOT_WRITE_RESULT_SCHEMA_VERSION, RootWriteResultSchema,
    WRITE_RELATION_COLUMN_COUNT, WRITER_MULTIPLEX_SCHEMA_VERSION, WriteTargetOrdinal,
    WriterAuxiliaryChannel, WriterMultiplexSchema, arrow_schemas_exact,
    validate_writer_handle_bytes,
};
use novarocks_types::SlotId;

const MAX_WRITE_AGGREGATE_CALLS: usize = 4_096;
const MAX_WRITE_UNPIVOT_MAPPINGS: usize = 4_096;
const MAX_WRITE_UNPIVOT_CONSTANTS: usize = 16_384;

use super::DecodedNode;
use super::aggregate::decode_resolved_aggregate_signature;
use super::unpivot::decode_unpivot_constant;
use crate::connector::write_data_plane::{
    ObservedConnectorWriteExecution, RoleBoundCommitFragmentEncoder,
    RootCommitFragmentCarrierValidator,
};
use crate::fragment::decode::plan::context::NativePlanDecodeContext;
use crate::fragment::decode::plan::error::NativeFragmentDecodeError;
use crate::fragment::decode::plan::layout::Layout;

fn decode_writer_multiplex_schema(
    wire: Option<&plan::WriterMultiplexSchema>,
    path: FieldPath,
) -> Result<WriterMultiplexRelationSchema, NativeFragmentDecodeError> {
    let wire = wire.ok_or_else(|| {
        NativeFragmentDecodeError::missing(path.clone(), "writer multiplex schema is required")
    })?;
    if wire.contract_version != WRITER_MULTIPLEX_SCHEMA_VERSION {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("contract_version"),
            format!(
                "writer multiplex schema contract version {} is unsupported; expected {}",
                wire.contract_version, WRITER_MULTIPLEX_SCHEMA_VERSION
            ),
        ));
    }
    if wire.columns.len() < WRITE_RELATION_COLUMN_COUNT {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("columns"),
            "writer multiplex schema is missing its fixed prefix",
        ));
    }
    let decoded = arrow_physical::decode_schema(&wire.columns, &wire.schema_metadata, path.clone())
        .map_err(NativeFragmentDecodeError::from)?;
    for (index, is_internal) in decoded.internal().iter().enumerate() {
        if !is_internal {
            return Err(NativeFragmentDecodeError::invalid_value(
                path.clone()
                    .field("columns")
                    .index(index)
                    .field("is_internal"),
                "write relation columns must be internal",
            ));
        }
    }
    let actual = decoded.schema();
    let channels = wire.columns[WRITE_RELATION_COLUMN_COUNT..]
        .iter()
        .zip(actual.fields()[WRITE_RELATION_COLUMN_COUNT..].iter())
        .map(|(column, field)| {
            WriterAuxiliaryChannel::try_new(
                column.slot_id,
                field.name().clone(),
                field.data_type().clone(),
            )
            .map_err(|error| {
                NativeFragmentDecodeError::invalid_value(
                    path.clone().field("columns"),
                    format!("writer multiplex auxiliary channel: {error}"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let contract = WriterMultiplexSchema::try_new(channels).map_err(|error| {
        NativeFragmentDecodeError::invalid_value(
            path.clone().field("columns"),
            format!("writer multiplex schema: {error}"),
        )
    })?;
    contract
        .validate_exact_arrow_schema(actual.as_ref())
        .map_err(|error| {
            NativeFragmentDecodeError::inconsistent(
                path.clone().field("columns"),
                format!("writer multiplex schema: {error}"),
            )
        })?;
    if decoded.slot_ids() != contract.slot_ids() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.field("columns"),
            "writer multiplex column ids do not match the frozen relation",
        ));
    }
    WriterMultiplexRelationSchema::try_new(contract)
        .map_err(|error| NativeFragmentDecodeError::invalid_value(path, error))
}

fn decode_root_result_schema(
    wire: Option<&plan::RootWriteResultSchema>,
    path: FieldPath,
) -> Result<RootWriteResultRelationSchema, NativeFragmentDecodeError> {
    let wire = wire.ok_or_else(|| {
        NativeFragmentDecodeError::missing(path.clone(), "root write result schema is required")
    })?;
    if wire.contract_version != ROOT_WRITE_RESULT_SCHEMA_VERSION {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("contract_version"),
            format!(
                "root write result schema contract version {} is unsupported; expected {}",
                wire.contract_version, ROOT_WRITE_RESULT_SCHEMA_VERSION
            ),
        ));
    }
    if wire.columns.len() != ROOT_WRITE_RESULT_COLUMN_COUNT {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("columns"),
            format!(
                "root write result schema has {} columns; expected {}",
                wire.columns.len(),
                ROOT_WRITE_RESULT_COLUMN_COUNT
            ),
        ));
    }
    let decoded = arrow_physical::decode_schema(&wire.columns, &wire.schema_metadata, path.clone())
        .map_err(NativeFragmentDecodeError::from)?;
    for (index, is_internal) in decoded.internal().iter().enumerate() {
        if !is_internal {
            return Err(NativeFragmentDecodeError::invalid_value(
                path.clone()
                    .field("columns")
                    .index(index)
                    .field("is_internal"),
                "write relation columns must be internal",
            ));
        }
    }
    let contract = RootWriteResultSchema::new();
    let actual = decoded.schema();
    contract
        .validate_exact_arrow_schema(actual.as_ref())
        .map_err(|error| {
            NativeFragmentDecodeError::inconsistent(
                path.clone().field("columns"),
                format!("root write result schema: {error}"),
            )
        })?;
    if decoded.slot_ids() != contract.slot_ids() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("columns"),
            "root write result column ids do not match the fixed relation",
        ));
    }
    RootWriteResultRelationSchema::try_new(contract)
        .map_err(|error| NativeFragmentDecodeError::invalid_value(path, error))
}

fn decode_partial_aggregate_plan(
    wire: Option<&plan::WriterPartialAggregatePlan>,
    projected_input_schema: &novarocks_execution::exec::chunk::ChunkSchema,
    writer_schema: &WriterMultiplexRelationSchema,
    path: FieldPath,
    ctx: &NativePlanDecodeContext,
) -> Result<WriterPartialAggregatePlan, NativeFragmentDecodeError> {
    let wire = wire.ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone(),
            "writer partial aggregate plan is required",
        )
    })?;
    if wire.calls.len() > MAX_WRITE_AGGREGATE_CALLS {
        return Err(NativeFragmentDecodeError::out_of_range(
            path.clone().field("calls"),
            "writer partial aggregate call count exceeds the limit",
        ));
    }
    let catalog = if wire.calls.is_empty() {
        None
    } else {
        Some(ctx.function_catalog().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone(),
                "writer partial aggregates require the process engine function catalog",
            )
        })?)
    };
    let mut slots = BTreeSet::new();
    let mut calls = Vec::with_capacity(wire.calls.len());
    for (index, call) in wire.calls.iter().enumerate() {
        let call_path = path.clone().field("calls").index(index);
        let planned = decode_resolved_aggregate_signature(
            call.resolved_signature.as_ref(),
            &call.function_name,
            call_path.clone().field("resolved_signature"),
        )?;
        if planned.argument_types.len() != 1 {
            return Err(NativeFragmentDecodeError::inconsistent(
                call_path
                    .clone()
                    .field("resolved_signature")
                    .field("argument_types"),
                "writer partial aggregate requires exactly one input",
            ));
        }
        let selected = catalog
            .expect("nonempty calls require catalog")
            .resolve_aggregate_trusted(&call.function_name, &planned.argument_types)
            .map_err(|error| {
                NativeFragmentDecodeError::invalid_value(
                    call_path.clone(),
                    format!("writer partial aggregate resolution: {error}"),
                )
            })?;
        if selected != planned {
            return Err(NativeFragmentDecodeError::inconsistent(
                call_path.clone().field("resolved_signature"),
                "writer partial aggregate signature differs from the process catalog",
            ));
        }
        let input_slot_id = SlotId::new(call.input_slot_id);
        let input = projected_input_schema.slot(input_slot_id).ok_or_else(|| {
            NativeFragmentDecodeError::inconsistent(
                call_path.clone().field("input_slot_id"),
                "writer partial aggregate input slot is absent from the projected writer input",
            )
        })?;
        if input.data_type() != &planned.argument_types[0] {
            return Err(NativeFragmentDecodeError::inconsistent(
                call_path.clone().field("input_slot_id"),
                "writer partial aggregate input type differs from its resolved signature",
            ));
        }
        let intermediate_slot_id = SlotId::new(call.intermediate_slot_id);
        if !slots.insert(intermediate_slot_id) {
            return Err(NativeFragmentDecodeError::inconsistent(
                call_path.clone().field("intermediate_slot_id"),
                "writer partial aggregate repeats an intermediate slot",
            ));
        }
        let output = writer_schema
            .chunk_schema()
            .slot(intermediate_slot_id)
            .ok_or_else(|| {
                NativeFragmentDecodeError::inconsistent(
                    call_path.clone().field("intermediate_slot_id"),
                    "writer partial aggregate output is absent from the typed writer tail",
                )
            })?;
        if output.data_type() != &planned.intermediate_type || !output.nullable() {
            return Err(NativeFragmentDecodeError::inconsistent(
                call_path.clone().field("intermediate_slot_id"),
                "writer partial aggregate output differs from the typed writer tail",
            ));
        }
        calls.push(WriterPartialAggregateCall {
            input_slot_id,
            function_name: Arc::from(call.function_name.as_str()),
            resolved: planned,
            intermediate_slot_id,
        });
    }
    Ok(WriterPartialAggregatePlan { calls })
}

fn decode_final_aggregate_plan(
    wire: Option<&plan::WriterFinalAggregatePlan>,
    expected_targets: &BTreeSet<WriteTargetOrdinal>,
    writer_schema: &WriterMultiplexRelationSchema,
    root_schema: &RootWriteResultRelationSchema,
    path: FieldPath,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<WriterFinalAggregatePlan, NativeFragmentDecodeError> {
    let wire = wire.ok_or_else(|| {
        NativeFragmentDecodeError::missing(path.clone(), "writer final aggregate plan is required")
    })?;
    if wire.calls.len() > MAX_WRITE_AGGREGATE_CALLS {
        return Err(NativeFragmentDecodeError::out_of_range(
            path.clone().field("calls"),
            "writer final aggregate call count exceeds the limit",
        ));
    }
    let catalog = if wire.calls.is_empty() {
        None
    } else {
        Some(ctx.function_catalog().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone(),
                "writer final aggregates require the process engine function catalog",
            )
        })?)
    };
    let auxiliary_slots = writer_schema
        .contract()
        .auxiliary_channels()
        .iter()
        .map(|channel| SlotId::new(channel.slot_id()))
        .collect::<BTreeSet<_>>();
    let mut consumed_slots = BTreeSet::new();
    let mut final_slots = BTreeSet::new();
    let mut calls = Vec::with_capacity(wire.calls.len());
    for (index, call) in wire.calls.iter().enumerate() {
        let call_path = path.clone().field("calls").index(index);
        let planned = decode_resolved_aggregate_signature(
            call.resolved_signature.as_ref(),
            &call.function_name,
            call_path.clone().field("resolved_signature"),
        )?;
        let selected = catalog
            .expect("nonempty calls require catalog")
            .resolve_aggregate_trusted(&call.function_name, &planned.argument_types)
            .map_err(|error| {
                NativeFragmentDecodeError::invalid_value(
                    call_path.clone(),
                    format!("writer final aggregate resolution: {error}"),
                )
            })?;
        if selected != planned {
            return Err(NativeFragmentDecodeError::inconsistent(
                call_path.clone().field("resolved_signature"),
                "writer final aggregate signature differs from the process catalog",
            ));
        }
        let intermediate_input_slot_id = SlotId::new(call.intermediate_input_slot_id);
        if !consumed_slots.insert(intermediate_input_slot_id) {
            return Err(NativeFragmentDecodeError::inconsistent(
                call_path.clone().field("intermediate_input_slot_id"),
                "writer final aggregate repeats an intermediate input slot",
            ));
        }
        let input = writer_schema
            .chunk_schema()
            .slot(intermediate_input_slot_id)
            .ok_or_else(|| {
                NativeFragmentDecodeError::inconsistent(
                    call_path.clone().field("intermediate_input_slot_id"),
                    "writer final aggregate input is absent from the typed writer tail",
                )
            })?;
        if input.data_type() != &planned.intermediate_type {
            return Err(NativeFragmentDecodeError::inconsistent(
                call_path.clone().field("intermediate_input_slot_id"),
                "writer final aggregate input type differs from its resolved signature",
            ));
        }
        let final_output_slot_id = SlotId::new(call.final_output_slot_id);
        if !final_slots.insert(final_output_slot_id)
            || writer_schema
                .chunk_schema()
                .slot(final_output_slot_id)
                .is_some()
            || root_schema
                .chunk_schema()
                .slot(final_output_slot_id)
                .is_some()
        {
            return Err(NativeFragmentDecodeError::inconsistent(
                call_path.clone().field("final_output_slot_id"),
                "writer final aggregate output slot is duplicated or collides with a relation slot",
            ));
        }
        calls.push(WriterFinalAggregateCall {
            function_name: Arc::from(call.function_name.as_str()),
            resolved: planned,
            intermediate_input_slot_id,
            final_output_slot_id,
        });
    }
    if consumed_slots != auxiliary_slots {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("calls"),
            "writer final aggregates do not exactly cover the typed writer tail",
        ));
    }

    let unpivot = match (&wire.unpivot, calls.is_empty()) {
        (None, true) => None,
        (Some(_), true) => {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("unpivot"),
                "writer grouped Unpivot is present without final aggregates",
            ));
        }
        (None, false) => {
            return Err(NativeFragmentDecodeError::missing(
                path.clone().field("unpivot"),
                "writer final aggregates require grouped Unpivot facts",
            ));
        }
        (Some(unpivot), false) => Some(decode_writer_grouped_unpivot(
            unpivot,
            expected_targets,
            writer_schema,
            root_schema,
            &final_slots,
            path.clone().field("unpivot"),
            arena,
            ctx,
        )?),
    };
    Ok(WriterFinalAggregatePlan { calls, unpivot })
}

#[expect(
    clippy::too_many_arguments,
    reason = "The frozen grouped Unpivot boundary validates each independent relation explicitly."
)]
fn decode_writer_grouped_unpivot(
    wire: &plan::WriterGroupedUnpivotPlan,
    expected_targets: &BTreeSet<WriteTargetOrdinal>,
    writer_schema: &WriterMultiplexRelationSchema,
    root_schema: &RootWriteResultRelationSchema,
    final_slots: &BTreeSet<SlotId>,
    path: FieldPath,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<WriterGroupedUnpivotPlan, NativeFragmentDecodeError> {
    if wire.mappings.is_empty() || wire.mappings.len() > MAX_WRITE_UNPIVOT_MAPPINGS {
        return Err(NativeFragmentDecodeError::out_of_range(
            path.clone().field("mappings"),
            "writer grouped Unpivot mapping count is outside the supported range",
        ));
    }
    let max_output_rows = usize::try_from(wire.max_output_rows)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            NativeFragmentDecodeError::out_of_range(
                path.clone().field("max_output_rows"),
                "writer grouped Unpivot row budget is invalid",
            )
        })?;
    let max_output_bytes = usize::try_from(wire.max_output_bytes)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            NativeFragmentDecodeError::out_of_range(
                path.clone().field("max_output_bytes"),
                "writer grouped Unpivot byte budget is invalid",
            )
        })?;
    let grouping_input_slot_id = SlotId::new(wire.grouping_input_slot_id);
    let grouping_input = writer_schema
        .chunk_schema()
        .slot(grouping_input_slot_id)
        .ok_or_else(|| {
            NativeFragmentDecodeError::inconsistent(
                path.clone().field("grouping_input_slot_id"),
                "writer grouped Unpivot grouping input is absent from the writer relation",
            )
        })?;
    if grouping_input.data_type() != &arrow::datatypes::DataType::Int32 || grouping_input.nullable()
    {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("grouping_input_slot_id"),
            "writer grouped Unpivot grouping input must be non-null Int32",
        ));
    }
    let grouping_output_slot_id = SlotId::new(wire.grouping_output_slot_id);
    let passthrough_output_slot_id = SlotId::new(wire.passthrough_output_slot_id);
    let value_output_slot_id = SlotId::new(wire.value_output_slot_id);
    let literal_output_slot_ids = wire
        .literal_output_slot_ids
        .iter()
        .copied()
        .map(SlotId::new)
        .collect::<Vec<_>>();
    let mut output_roles = BTreeSet::new();
    for slot in std::iter::once(passthrough_output_slot_id)
        .chain(std::iter::once(value_output_slot_id))
        .chain(literal_output_slot_ids.iter().copied())
    {
        if !output_roles.insert(slot) || root_schema.chunk_schema().slot(slot).is_none() {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone(),
                "writer grouped Unpivot output roles are duplicated or absent from Root schema",
            ));
        }
    }
    let root_slots = root_schema
        .chunk_schema()
        .slot_ids()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if grouping_output_slot_id.0 == 0
        || root_slots.contains(&grouping_output_slot_id)
        || writer_schema
            .chunk_schema()
            .slot(grouping_output_slot_id)
            .is_some()
        || final_slots.contains(&grouping_output_slot_id)
    {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("grouping_output_slot_id"),
            "writer grouped Unpivot grouping output collides with another slot",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut referenced_final_slots = BTreeSet::new();
    let mut decoded_constant_count = 0usize;
    let mut decoded_nested_elements = 0usize;
    let mut decoded_constant_bytes = 0usize;
    let mut mappings = Vec::with_capacity(wire.mappings.len());
    for (index, mapping) in wire.mappings.iter().enumerate() {
        let mapping_path = path.clone().field("mappings").index(index);
        let target = WriteTargetOrdinal::try_new(mapping.grouping_key).map_err(|error| {
            NativeFragmentDecodeError::out_of_range(
                mapping_path.clone().field("grouping_key"),
                error.to_string(),
            )
        })?;
        if !expected_targets.contains(&target) {
            return Err(NativeFragmentDecodeError::inconsistent(
                mapping_path.clone().field("grouping_key"),
                "writer grouped Unpivot mapping targets an unexpected group",
            ));
        }
        let input_value_slot_id = SlotId::new(mapping.input_value_slot_id);
        if !final_slots.contains(&input_value_slot_id)
            || !seen.insert((target, input_value_slot_id))
        {
            return Err(NativeFragmentDecodeError::inconsistent(
                mapping_path.clone(),
                "writer grouped Unpivot mapping is duplicated or references an unknown final slot",
            ));
        }
        referenced_final_slots.insert(input_value_slot_id);
        if mapping.constants.len() != literal_output_slot_ids.len() {
            return Err(NativeFragmentDecodeError::inconsistent(
                mapping_path.clone().field("constants"),
                "writer grouped Unpivot constant arity differs from its output roles",
            ));
        }
        decoded_constant_count = decoded_constant_count
            .checked_add(mapping.constants.len())
            .ok_or_else(|| {
                NativeFragmentDecodeError::out_of_range(
                    mapping_path.clone().field("constants"),
                    "writer grouped Unpivot constant count overflowed",
                )
            })?;
        if decoded_constant_count > MAX_WRITE_UNPIVOT_CONSTANTS {
            return Err(NativeFragmentDecodeError::out_of_range(
                mapping_path.clone().field("constants"),
                "writer grouped Unpivot constant count exceeds the limit",
            ));
        }
        let mut constants = Vec::with_capacity(mapping.constants.len());
        for (constant_index, constant) in mapping.constants.iter().enumerate() {
            let constant_path = mapping_path
                .clone()
                .field("constants")
                .index(constant_index);
            let (decoded, data_type, _) = decode_unpivot_constant(
                constant,
                constant_path.clone(),
                arena,
                ctx,
                &mut decoded_nested_elements,
                &mut decoded_constant_bytes,
            )?;
            let output = root_schema
                .chunk_schema()
                .slot(literal_output_slot_ids[constant_index])
                .expect("validated Root output slot");
            if output.data_type() != &data_type {
                return Err(NativeFragmentDecodeError::inconsistent(
                    constant_path,
                    "writer grouped Unpivot constant type differs from its Root output slot",
                ));
            }
            constants.push(decoded);
        }
        mappings.push(WriterGroupedUnpivotMapping {
            grouping_key: target.get(),
            input_value_slot_id,
            constants,
        });
    }
    if referenced_final_slots != *final_slots {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("mappings"),
            "writer grouped Unpivot mappings do not cover every final aggregate output",
        ));
    }
    Ok(WriterGroupedUnpivotPlan {
        grouping_input_slot_id,
        grouping_output_slot_id,
        passthrough_output_slot_id,
        value_output_slot_id,
        literal_output_slot_ids,
        mappings,
        max_output_rows,
        max_output_bytes,
    })
}

/// Decode one `TableWriterNode` and bind it to this attempt's write role
/// binding.
pub(super) fn lower_table_writer_node(
    node: &plan::DistributedNode,
    writer: &plan::TableWriterNode,
    path: FieldPath,
    children: Vec<DecodedNode>,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let child = children
        .into_iter()
        .next()
        .expect("table writer child arity is validated before lowering");
    let node_id = node.node_id;
    let writer_multiplex_schema = decode_writer_multiplex_schema(
        writer.writer_multiplex_schema.as_ref(),
        path.clone().field("writer_multiplex_schema"),
    )?;
    let runtime = ctx.typed_scan_runtime().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("catalog_handle"),
            format!(
                "native node_id={node_id} table writer requires a query-leased catalog runtime"
            ),
        )
    })?;

    let wire_catalog_handle = writer.catalog_handle.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("catalog_handle"),
            format!("native node_id={node_id} table writer requires an exact catalog handle"),
        )
    })?;
    let catalog_handle = novarocks_proto_codec::catalog::decode_catalog_handle(
        wire_catalog_handle.clone(),
        path.clone().field("catalog_handle"),
    )
    .map_err(NativeFragmentDecodeError::from)?;

    let target = WriteTargetOrdinal::try_new(writer.write_target_ordinal).map_err(|error| {
        NativeFragmentDecodeError::out_of_range(
            path.clone().field("write_target_ordinal"),
            format!("native node_id={node_id} table writer target ordinal: {error}"),
        )
    })?;

    let carrier = writer.handle.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("handle"),
            format!("native node_id={node_id} table writer requires its writer handle"),
        )
    })?;
    // Only the single-handle cap. The query-wide unique-handle budget belongs
    // to the frontend, which is the only owner that can see the unique set.
    validate_writer_handle_bytes(prost::Message::encoded_len(carrier)).map_err(|error| {
        NativeFragmentDecodeError::out_of_range(
            path.clone().field("handle"),
            format!("native node_id={node_id} table writer handle: {error}"),
        )
    })?;
    let validated = ValidatedWriterHandle::parse(carrier.clone(), path.clone().field("handle"))
        .map_err(NativeFragmentDecodeError::from)?;

    let binding = runtime
        .catalog_write_execution(&catalog_handle)
        .map_err(|error| {
            NativeFragmentDecodeError::invalid_value(
                path.clone().field("catalog_handle"),
                format!(
                    "native node_id={node_id} table writer cannot resolve its query-leased connector write runtime: {error}"
                ),
            )
        })?;
    let handle = binding
        .handle_decoder()
        .decode_writer_handle(&validated)
        .map_err(|error| {
            NativeFragmentDecodeError::invalid_value(
                path.clone().field("handle"),
                format!("native node_id={node_id} table writer handle: {error}"),
            )
        })?;

    if writer.target_schema.is_empty() {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("target_schema"),
            format!("native node_id={node_id} table writer requires a target schema"),
        ));
    }
    let projected_input_layout =
        ctx.decode_output_layout(&writer.target_schema, path.clone().field("target_schema"))?;
    let expected_slot_ids = (0..writer.target_schema.len())
        .map(|ordinal| {
            ordinal
                .checked_add(1)
                .and_then(|slot| u32::try_from(slot).ok())
                .map(SlotId::new)
                .ok_or_else(|| {
                    NativeFragmentDecodeError::out_of_range(
                        path.clone().field("target_schema"),
                        "table writer projected input slot ID overflowed",
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if projected_input_layout.slot_ids() != expected_slot_ids {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("target_schema"),
            "table writer target schema column ids must equal their one-based ordinals",
        ));
    }
    let projected_input_schema = projected_input_layout.chunk_schema();
    let expected_schema = projected_input_schema.arrow_schema_ref();

    // The input binding is a sealed description of which execution outputs feed
    // the writer. The projection itself is always the sealed output
    // expressions, so the binding is validated against the real child rather
    // than reinterpreted into a second, competing projection.
    validate_input_binding(
        writer.input.as_ref(),
        &child,
        node_id,
        path.clone().field("input"),
    )?;

    if writer.output_exprs.is_empty() {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("output_exprs"),
            format!("native node_id={node_id} table writer requires its sealed output expressions"),
        ));
    }
    let mut projection_arena = ExprArena::default();
    if let Some(options) = ctx.query_options() {
        projection_arena.set_allow_throw_exception(options.allow_throw_exception());
    }
    let mut exprs = Vec::with_capacity(writer.output_exprs.len());
    for (index, expression) in writer.output_exprs.iter().enumerate() {
        exprs.push(ctx.decode_expression(
            expression,
            path.clone().field("output_exprs").index(index),
            &mut projection_arena,
            &child.layout,
        )?);
    }
    let projection =
        TableWriterInputProjection::try_new(projection_arena, exprs, Arc::clone(&expected_schema))
            .map_err(|error| {
                NativeFragmentDecodeError::inconsistent(
                    path.clone().field("output_exprs"),
                    format!("native node_id={node_id} table writer projection: {error}"),
                )
            })?;
    let partial_aggregate_plan = decode_partial_aggregate_plan(
        writer.partial_aggregate_plan.as_ref(),
        projected_input_schema.as_ref(),
        &writer_multiplex_schema,
        path.clone().field("partial_aggregate_plan"),
        ctx,
    )?;

    // The query and attempt come from the execution identity this fragment was
    // admitted under, never from the plan node: a plan carries no attempt, and
    // a replacement attempt must not inherit a predecessor's writer context.
    let execution_id = runtime.execution_id();
    let physical_template = TableWriterPhysicalContextTemplate::new(
        uuid_bytes(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        ),
        execution_id.attempt_id().get(),
        uuid_bytes(
            ctx.fragment_instance_id().get().high(),
            ctx.fragment_instance_id().get().low(),
        ),
        writer.writer_ordinal,
    );

    let (_, query_expire) = query_expire_durations(ctx.query_options());
    let request_context = ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        ctx.connector_cancellation()
            .map_err(|error| error.into_native(path.clone()))?,
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map(|context| context.with_storage_resolver(runtime.storage_resolver()))
    .map_err(|error| {
        NativeFragmentDecodeError::invalid_value(
            path.clone(),
            format!("native node_id={node_id} table writer request context: {error}"),
        )
    })?;

    let execution = Arc::new(ObservedConnectorWriteExecution::new(
        binding.execution(),
        execution_id,
        node_id,
    ));
    let fragment_encoder = Arc::new(RoleBoundCommitFragmentEncoder::new(
        binding.fragment_encoder(),
        execution_id,
        node_id,
    ));

    let lowered = TableWriterNode::try_new_with_relation(
        Box::new(child.node),
        node_id,
        handle,
        target,
        execution,
        expected_schema,
        projection,
        physical_template,
        request_context,
        fragment_encoder,
        writer_multiplex_schema,
        partial_aggregate_plan,
    )
    .map_err(|error| {
        NativeFragmentDecodeError::inconsistent(
            path.clone(),
            format!("native node_id={node_id} table writer: {error}"),
        )
    })?;
    #[cfg(debug_assertions)]
    let lowered = lowered.with_aggregate_guard(Arc::new(
        crate::connector::write_data_plane::QueryScopedTableWriteAggregateGuard::new(
            execution_id,
            node_id,
        ),
    ));

    let output_schema = Arc::clone(lowered.writer_multiplex_schema().chunk_schema());
    let layout = Layout::for_slots(output_schema.slot_ids().iter().copied());
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::TableWriter(lowered),
        },
        layout,
        output_schema,
    })
}

/// Decode one n-ary `TableFinishNode`.
pub(super) fn lower_table_finish_node(
    node: &plan::DistributedNode,
    finish: &plan::TableFinishNode,
    path: FieldPath,
    children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let node_id = node.node_id;
    let writer_multiplex_schema = decode_writer_multiplex_schema(
        finish.writer_multiplex_schema.as_ref(),
        path.clone().field("writer_multiplex_schema"),
    )?;
    let root_result_schema = decode_root_result_schema(
        finish.root_result_schema.as_ref(),
        path.clone().field("root_result_schema"),
    )?;
    let ordinals_path = path.clone().field("expected_target_ordinals");
    let mut expected = Vec::with_capacity(finish.expected_target_ordinals.len());
    for (index, ordinal) in finish.expected_target_ordinals.iter().enumerate() {
        expected.push(WriteTargetOrdinal::try_new(*ordinal).map_err(|error| {
            NativeFragmentDecodeError::out_of_range(
                ordinals_path.clone().index(index),
                format!("native node_id={node_id} table finish target ordinal: {error}"),
            )
        })?);
    }

    // Every input must already carry the writer relation. The finish node reads
    // its columns positionally, so a foreign input would otherwise only fail
    // once rows arrive.
    let expected_schema = writer_multiplex_schema.chunk_schema();
    for (index, child) in children.iter().enumerate() {
        let matches = child.output_schema.slot_ids() == expected_schema.slot_ids()
            && arrow_schemas_exact(
                child.output_schema.arrow_schema_ref().as_ref(),
                expected_schema.arrow_schema_ref().as_ref(),
            );
        if !matches {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("children").index(index),
                format!(
                    "native node_id={node_id} table finish input {index} does not carry the write relation"
                ),
            ));
        }
    }

    // The finish node and its writers are normally in different fragments, so
    // this only fires when a plan places them together. When it does, a writer
    // naming a target the finish node never expects is a self-contradictory
    // plan, not a runtime surprise.
    //
    // Membership is an exact set test: a query's expected set is the targets
    // *this query's* writers feed and need not be dense from zero, so comparing
    // against the highest ordinal would admit a target no writer here compiles.
    let sealed = expected.iter().copied().collect::<BTreeSet<_>>();
    if !sealed.is_empty() {
        let mut offender = None;
        for child in &node.children {
            collect_out_of_set_writer(child, &sealed, &mut offender);
        }
        if let Some((writer_node_id, ordinal)) = offender {
            return Err(NativeFragmentDecodeError::inconsistent(
                ordinals_path.clone(),
                format!(
                    "native node_id={writer_node_id} table writer names write target ordinal {ordinal} outside the finish node's sealed set of {} targets",
                    expected.len()
                ),
            ));
        }
    }

    let final_aggregate_plan = decode_final_aggregate_plan(
        finish.final_aggregate_plan.as_ref(),
        &sealed,
        &writer_multiplex_schema,
        &root_result_schema,
        path.clone().field("final_aggregate_plan"),
        arena,
        ctx,
    )?;

    let execution_id = ctx
        .typed_scan_runtime()
        .map(|runtime| runtime.execution_id())
        .ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone(),
                format!(
                    "native node_id={node_id} table finish requires a query-leased catalog runtime"
                ),
            )
        })?;
    let validator = Arc::new(RootCommitFragmentCarrierValidator::new(
        execution_id,
        node_id,
    ));

    let inputs = children.into_iter().map(|child| child.node).collect();
    let lowered = TableFinishNode::try_new_with_relations(
        inputs,
        node_id,
        expected,
        validator,
        writer_multiplex_schema,
        root_result_schema,
        final_aggregate_plan,
    )
    .map_err(|error| {
        NativeFragmentDecodeError::invalid_value(
            ordinals_path,
            format!("native node_id={node_id} table finish: {error}"),
        )
    })?;
    #[cfg(debug_assertions)]
    let lowered = lowered.with_aggregate_guard(Arc::new(
        crate::connector::write_data_plane::QueryScopedTableWriteAggregateGuard::new(
            execution_id,
            node_id,
        ),
    ));

    let output_schema = Arc::clone(lowered.root_result_schema().chunk_schema());
    let layout = Layout::for_slots(output_schema.slot_ids().iter().copied());
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::TableFinish(lowered),
        },
        layout,
        output_schema,
    })
}

/// Record the first writer under `node` whose target ordinal is outside the
/// dense set bounded by `highest`.
fn collect_out_of_set_writer(
    node: &plan::DistributedNode,
    sealed: &BTreeSet<WriteTargetOrdinal>,
    offender: &mut Option<(i32, u32)>,
) {
    if offender.is_some() {
        return;
    }
    if let Some(plan::distributed_node::Payload::TableWriter(writer)) = node.payload.as_ref()
        && !WriteTargetOrdinal::try_new(writer.write_target_ordinal)
            .is_ok_and(|ordinal| sealed.contains(&ordinal))
    {
        *offender = Some((node.node_id, writer.write_target_ordinal));
        return;
    }
    for child in &node.children {
        collect_out_of_set_writer(child, sealed, offender);
    }
}

/// Validate the sealed input binding against the writer's real child.
fn validate_input_binding(
    input: Option<&plan::ConnectorWriteInputBinding>,
    child: &DecodedNode,
    node_id: i32,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    let input = input.ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone(),
            format!("native node_id={node_id} table writer requires its input binding"),
        )
    })?;
    let width = child.layout.order().len();
    match input.kind.as_ref() {
        None => Err(NativeFragmentDecodeError::missing(
            path.field("kind"),
            format!("native node_id={node_id} table writer input binding requires a kind"),
        )),
        Some(plan::connector_write_input_binding::Kind::RootOutputByOrdinal(true)) => Ok(()),
        Some(plan::connector_write_input_binding::Kind::RootOutputByOrdinal(false)) => {
            Err(NativeFragmentDecodeError::invalid_value(
                path.field("root_output_by_ordinal"),
                format!(
                    "native node_id={node_id} table writer root_output_by_ordinal marker must be true"
                ),
            ))
        }
        Some(plan::connector_write_input_binding::Kind::OutputOrdinals(ordinals)) => {
            let path = path.field("output_ordinals");
            if ordinals.values.is_empty() {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path,
                    format!(
                        "native node_id={node_id} table writer output_ordinals must not be empty"
                    ),
                ));
            }
            let mut seen = std::collections::BTreeSet::new();
            for (index, value) in ordinals.values.iter().enumerate() {
                let ordinal = usize::try_from(*value).map_err(|_| {
                    NativeFragmentDecodeError::out_of_range(
                        path.clone().index(index),
                        format!(
                            "native node_id={node_id} table writer output ordinal {value} is not addressable"
                        ),
                    )
                })?;
                if ordinal >= width {
                    return Err(NativeFragmentDecodeError::out_of_range(
                        path.clone().index(index),
                        format!(
                            "native node_id={node_id} table writer output ordinal {ordinal} is outside its input width {width}"
                        ),
                    ));
                }
                if !seen.insert(ordinal) {
                    return Err(NativeFragmentDecodeError::inconsistent(
                        path.clone().index(index),
                        format!(
                            "native node_id={node_id} table writer repeats output ordinal {ordinal}"
                        ),
                    ));
                }
            }
            Ok(())
        }
    }
}

/// The 16-byte form of a native `(high, low)` identity, in the same big-endian
/// halves its UUID rendering uses.
const fn uuid_bytes(high: i64, low: i64) -> [u8; 16] {
    let high = high.to_be_bytes();
    let low = low.to_be_bytes();
    [
        high[0], high[1], high[2], high[3], high[4], high[5], high[6], high[7], low[0], low[1],
        low[2], low[3], low[4], low[5], low[6], low[7],
    ]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_execution::exec::node::table_write_relation::RootWriteResultRelationSchema;
    use novarocks_execution::exec::pipeline::operator_factory::OperatorFactory;
    use novarocks_execution::runtime::execution_runtime::{
        ExecutionRuntime, ExecutionRuntimeConfig, ExecutionSpillStorageConfig,
    };
    use novarocks_execution::runtime::runtime_state::RuntimeState;
    use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
    use novarocks_proto_models::connector_write as write_dto;
    use novarocks_proto_models::plan;
    use novarocks_spi::connector::write_stack::{
        ConnectorOpenWriterRequest, ConnectorWriterPhysicalContext,
        MAX_CONNECTOR_WRITER_HANDLE_BYTES, WRITE_RELATION_COLUMN_COUNT,
        WRITE_RELATION_FRAGMENT_COLUMN, WRITE_RELATION_KIND_COLUMN,
        WRITE_RELATION_ROW_COUNT_COLUMN, WRITE_RELATION_TARGET_COLUMN, WriteTargetOrdinal,
        WriterAuxiliaryChannel, WriterMultiplexSchema, write_relation_column_id,
    };
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId, SlotId, UniqueId};

    use super::super::tests::{
        column_ref, one_col_values_node_with, output_column, output_column_with_nullable,
        physical_node, resolved_aggregate_signature,
    };
    use super::super::{DecodedNode, NativePlanDecodeContext, decode_node};
    use super::decode_writer_multiplex_schema;
    use crate::connector::write_test_support::{
        RecordingWriteExecution, TEST_WRITE_CATALOG, finish_node, iceberg_writer_handle,
        table_writer_payload, test_request_context, test_write_adapter, test_write_binding,
        test_write_catalog_handle, test_write_scan_runtime, wire_catalog_handle,
        writer_multiplex_schema, writer_node,
    };
    use crate::fragment::decode::plan::error::NativeFragmentDecodeError;

    const CHILD_COLUMN_ID: u32 = 1;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(0x51, 0x52),
            AttemptId::new(3).expect("attempt"),
        )
        .expect("execution id")
    }

    fn fragment_instance_id() -> UniqueId {
        UniqueId::new(0x61, 0x62)
    }

    fn recording_execution() -> Arc<RecordingWriteExecution> {
        Arc::new(RecordingWriteExecution::new())
    }

    fn test_execution_function_set()
    -> Arc<novarocks_execution::exec::expr::agg::SealedExecutionFunctionSet> {
        let mut builder = novarocks_execution::exec::expr::agg::ExecutionFunctionSetBuilder::new();
        novarocks_sql::compiler::contribute_builtin_functions(builder.catalog_builder_mut())
            .expect("builtin function metadata");
        novarocks_execution::exec::expr::agg::contribute_builtin_aggregate_implementations(
            &mut builder,
        )
        .expect("builtin aggregate implementations");
        Arc::new(builder.seal().expect("builtin execution function set"))
    }

    fn writer_runtime_state() -> RuntimeState {
        let runtime = Arc::new(
            ExecutionRuntime::new(
                ExecutionRuntimeConfig {
                    driver_threads: 1,
                    scan_threads: 1,
                    scan_queue_capacity: 1,
                    spill_io_threads: 1,
                    spill_io_queue_capacity: 1,
                    spill_storage: ExecutionSpillStorageConfig::default(),
                    exchange_wait_ms: 120_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 1024,
                    exchange_max_transmit_batched_bytes: 1024,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1024,
                    local_exchange_max_buffered_rows: 1024,
                    connector_io_tasks_per_scan_operator: 1,
                    scan_submit_fail_max: 1,
                    scan_submit_fail_timeout_ms: 1,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                test_execution_function_set(),
            )
            .expect("writer execution runtime"),
        );
        RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(runtime),
            None,
        )
    }

    fn write_decode_context(execution: Arc<RecordingWriteExecution>) -> NativePlanDecodeContext {
        NativePlanDecodeContext::default()
            .with_typed_scan_runtime(Some(test_write_scan_runtime(
                execution_id(),
                fragment_instance_id(),
                execution,
            )))
            .with_connector_cancellation(crate::connector::write_test_support::never_cancelled())
            .with_fragment_instance_id(fragment_instance_id())
            .with_function_catalog(Arc::new(
                novarocks_sql::compiler::build_builtin_engine_function_catalog()
                    .expect("builtin function catalog"),
            ))
    }

    fn writer_payload() -> plan::TableWriterNode {
        table_writer_payload(
            column_ref(CHILD_COLUMN_ID, DataType::Int64),
            vec![output_column(1, "id", DataType::Int64)],
        )
    }

    fn simple_writer_plan(writer: plan::TableWriterNode) -> plan::DistributedNode {
        writer_node(
            30,
            writer,
            vec![one_col_values_node_with(10, CHILD_COLUMN_ID, "id", 42)],
        )
    }

    fn writer_payload_with_count_partial(input_slot_id: u32) -> plan::TableWriterNode {
        let mut writer = writer_payload();
        let channel = WriterAuxiliaryChannel::try_new(17, "count_partial", DataType::Int64)
            .expect("count channel");
        let relation = WriterMultiplexSchema::try_new(vec![channel]).expect("writer relation");
        writer.writer_multiplex_schema = Some(writer_multiplex_schema(&relation));
        writer.partial_aggregate_plan = Some(plan::WriterPartialAggregatePlan {
            calls: vec![plan::WriterPartialAggregateCall {
                input_slot_id,
                function_name: "count".to_string(),
                resolved_signature: resolved_aggregate_signature("count", &[DataType::Int64]),
                intermediate_slot_id: 17,
            }],
        });
        writer
    }

    fn four_column_write_relation_values(
        mutate: impl FnOnce(&mut Vec<novarocks_proto_models::common::OutputColumn>),
    ) -> plan::DistributedNode {
        let mut columns = vec![
            output_column_with_nullable(
                write_relation_column_id(0),
                WRITE_RELATION_KIND_COLUMN,
                DataType::Int8,
                false,
            ),
            output_column_with_nullable(
                write_relation_column_id(1),
                WRITE_RELATION_TARGET_COLUMN,
                DataType::Int32,
                false,
            ),
            output_column_with_nullable(
                write_relation_column_id(2),
                WRITE_RELATION_ROW_COUNT_COLUMN,
                DataType::Int64,
                true,
            ),
            output_column_with_nullable(
                write_relation_column_id(3),
                WRITE_RELATION_FRAGMENT_COLUMN,
                DataType::Binary,
                true,
            ),
        ];
        mutate(&mut columns);
        physical_node(
            10,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: Vec::new(),
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn decode_with(
        node: &plan::DistributedNode,
        execution: Arc<RecordingWriteExecution>,
    ) -> Result<DecodedNode, NativeFragmentDecodeError> {
        let mut arena = ExprArena::default();
        decode_node(node, &mut arena, &write_decode_context(execution))
    }

    fn decode_error(node: &plan::DistributedNode) -> NativeFragmentDecodeError {
        decode_with(node, recording_execution()).expect_err("invalid write node must fail")
    }

    fn assert_protocol(
        error: &NativeFragmentDecodeError,
        expected_path: &str,
        expected_kind: ProtocolErrorKind,
    ) {
        let protocol = error
            .protocol()
            .unwrap_or_else(|| panic!("expected a protocol error, got {error}"));
        assert_eq!(protocol.path().to_string(), expected_path);
        assert_eq!(protocol.kind(), expected_kind);
    }

    // ----------------------------------------------------------------- positives

    #[test]
    fn a_table_writer_decodes_and_binds_to_its_query_leased_write_runtime() {
        let decoded = decode_with(&simple_writer_plan(writer_payload()), recording_execution())
            .expect("table writer decodes");
        let ExecNodeKind::TableWriter(writer) = &decoded.node.kind else {
            panic!("expected a table writer, got {:?}", decoded.node.kind);
        };
        assert_eq!(writer.node_id, 30);
        assert_eq!(writer.target().get(), 0);
        assert_eq!(
            writer.execution().catalog_handle(),
            &test_write_catalog_handle()
        );
        assert_eq!(
            writer.handle().binding().catalog_handle(),
            &test_write_catalog_handle()
        );
        assert_eq!(
            decoded.output_schema,
            *writer.writer_multiplex_schema().chunk_schema()
        );
    }

    #[test]
    fn writer_partial_input_is_bound_to_the_projected_target_schema() {
        let decoded = decode_with(
            &simple_writer_plan(writer_payload_with_count_partial(1)),
            recording_execution(),
        )
        .expect("target-ordinal partial input decodes");
        let ExecNodeKind::TableWriter(writer) = &decoded.node.kind else {
            panic!("expected a table writer");
        };
        assert_eq!(
            writer.partial_aggregate_plan().calls[0].input_slot_id,
            SlotId::new(1)
        );

        let error = decode_error(&simple_writer_plan(writer_payload_with_count_partial(7)));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.partial_aggregate_plan.calls[0].input_slot_id",
            ProtocolErrorKind::InconsistentFields,
        );
    }

    #[test]
    fn writer_target_schema_ids_are_one_based_ordinals() {
        let mut writer = writer_payload();
        writer.target_schema[0].column_id = 7;
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.target_schema",
            ProtocolErrorKind::InconsistentFields,
        );
    }

    #[test]
    fn the_writer_physical_context_comes_from_the_admitted_attempt_not_the_plan() {
        let decoded = decode_with(&simple_writer_plan(writer_payload()), recording_execution())
            .expect("table writer decodes");
        let ExecNodeKind::TableWriter(writer) = &decoded.node.kind else {
            panic!("expected a table writer");
        };
        let physical = writer.physical_template().for_driver(5);
        assert_eq!(
            physical.execution_query_id(),
            super::uuid_bytes(
                execution_id().query_id().high(),
                execution_id().query_id().low()
            )
        );
        assert_eq!(
            physical.execution_attempt_id(),
            execution_id().attempt_id().get()
        );
        assert_eq!(
            physical.fragment_instance_id(),
            super::uuid_bytes(fragment_instance_id().high(), fragment_instance_id().low())
        );
        assert_eq!(physical.driver_id(), 5);
        assert_eq!(physical.writer_ordinal(), 0);
    }

    #[test]
    fn every_driver_opens_its_own_writer_with_its_own_driver_id() {
        let execution = recording_execution();
        let decoded = decode_with(
            &simple_writer_plan(writer_payload()),
            Arc::clone(&execution),
        )
        .expect("table writer decodes");
        let ExecNodeKind::TableWriter(writer) = &decoded.node.kind else {
            panic!("expected a table writer");
        };
        let factory = novarocks_execution::exec::operators::TableWriterOperatorFactory::try_new(
            writer,
            test_execution_function_set(),
        )
        .expect("table writer factory");
        assert!(!factory.is_sink(), "a table writer is not a terminal sink");
        let runtime_state = writer_runtime_state();
        let mut operators = Vec::new();
        for driver_id in 0..4 {
            let mut operator = factory.create(4, driver_id);
            operator
                .bind_runtime_state(&runtime_state)
                .expect("bind writer actor");
            operators.push(operator);
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while execution.opened().len() != 4 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            execution.opened(),
            vec![(0, 0, 0), (1, 0, 0), (2, 0, 0), (3, 0, 0)]
        );
    }

    #[test]
    fn a_table_finish_decodes_over_several_writer_inputs() {
        let node = finish_node(
            40,
            vec![0],
            vec![
                simple_writer_plan(writer_payload()),
                simple_writer_plan(writer_payload()),
            ],
        );
        let decoded = decode_with(&node, recording_execution()).expect("table finish decodes");
        let ExecNodeKind::TableFinish(finish) = &decoded.node.kind else {
            panic!("expected a table finish, got {:?}", decoded.node.kind);
        };
        assert_eq!(finish.inputs.len(), 2);
        assert_eq!(finish.expected_targets().len(), 1);
        assert!(finish.accepts_target(WriteTargetOrdinal::try_new(0).expect("bounded ordinal")));
        assert!(!finish.accepts_target(WriteTargetOrdinal::try_new(1).expect("bounded ordinal")));
        assert_eq!(
            decoded.output_schema,
            RootWriteResultRelationSchema::fixed()
                .chunk_schema()
                .clone()
        );
    }

    // ----------------------------------------------------------------- negatives

    #[test]
    fn a_foreign_catalog_handle_is_refused_at_its_exact_path() {
        let mut writer = writer_payload();
        writer.catalog_handle = Some(wire_catalog_handle("other_catalog"));
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.catalog_handle",
            ProtocolErrorKind::InvalidValue,
        );
        assert!(error.contains("query-leased"), "unexpected detail: {error}");
    }

    #[test]
    fn a_writer_requires_the_versioned_exact_multiplex_schema() {
        let mut writer = writer_payload();
        writer.writer_multiplex_schema = None;
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.writer_multiplex_schema",
            ProtocolErrorKind::MissingField,
        );

        let mut writer = writer_payload();
        writer
            .writer_multiplex_schema
            .as_mut()
            .expect("schema")
            .contract_version += 1;
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.writer_multiplex_schema.contract_version",
            ProtocolErrorKind::InvalidValue,
        );
    }

    #[test]
    fn writer_multiplex_prefix_tampering_is_rejected() {
        let mutators: [fn(&mut plan::ArrowPhysicalColumn); 3] = [
            |column: &mut plan::ArrowPhysicalColumn| {
                column.field.as_mut().expect("field").name = "wrong".to_string()
            },
            |column: &mut plan::ArrowPhysicalColumn| {
                column.field.as_mut().expect("field").nullable = true
            },
            |column: &mut plan::ArrowPhysicalColumn| column.slot_id -= 1,
        ];
        for mutate in mutators {
            let mut writer = writer_payload();
            mutate(
                &mut writer
                    .writer_multiplex_schema
                    .as_mut()
                    .expect("schema")
                    .columns[0],
            );
            let error = decode_error(&simple_writer_plan(writer));
            assert!(
                error.contains("writer multiplex"),
                "unexpected detail: {error}"
            );
        }
    }

    #[test]
    fn writer_multiplex_dictionary_ipc_attributes_are_exact() {
        let contract = WriterMultiplexSchema::try_new(vec![
            WriterAuxiliaryChannel::try_new(
                17,
                "dictionary",
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            )
            .expect("dictionary channel"),
        ])
        .expect("writer schema");
        let mut wire = writer_multiplex_schema(&contract);
        let dictionary = wire.columns[WRITE_RELATION_COLUMN_COUNT]
            .field
            .as_mut()
            .expect("dictionary field");
        dictionary.dictionary_id = Some(41);
        dictionary.dictionary_is_ordered = Some(true);

        let error =
            decode_writer_multiplex_schema(Some(&wire), FieldPath::root("writer_multiplex_schema"))
                .expect_err("dictionary IPC attributes drifted from the frozen schema");
        assert_protocol(
            &error,
            "writer_multiplex_schema.columns",
            ProtocolErrorKind::InconsistentFields,
        );
    }

    #[test]
    fn a_finish_requires_both_exact_relation_contracts() {
        let mut node = finish_node(40, vec![0], vec![simple_writer_plan(writer_payload())]);
        let Some(plan::distributed_node::Payload::TableFinish(finish)) = node.payload.as_mut()
        else {
            panic!("finish payload");
        };
        finish.root_result_schema = None;
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.root_result_schema",
            ProtocolErrorKind::MissingField,
        );

        let mut node = finish_node(40, vec![0], vec![simple_writer_plan(writer_payload())]);
        let Some(plan::distributed_node::Payload::TableFinish(finish)) = node.payload.as_mut()
        else {
            panic!("finish payload");
        };
        finish
            .root_result_schema
            .as_mut()
            .expect("root schema")
            .columns
            .pop();
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.root_result_schema.columns",
            ProtocolErrorKind::InconsistentFields,
        );
    }

    #[test]
    fn root_relation_type_and_internal_marker_tampering_is_rejected() {
        let mut node = finish_node(40, vec![0], vec![simple_writer_plan(writer_payload())]);
        let Some(plan::distributed_node::Payload::TableFinish(finish)) = node.payload.as_mut()
        else {
            panic!("finish payload");
        };
        finish
            .root_result_schema
            .as_mut()
            .expect("root schema")
            .columns[7]
            .is_internal = false;
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.root_result_schema.columns[7].is_internal",
            ProtocolErrorKind::InvalidValue,
        );

        let mut node = finish_node(40, vec![0], vec![simple_writer_plan(writer_payload())]);
        let Some(plan::distributed_node::Payload::TableFinish(finish)) = node.payload.as_mut()
        else {
            panic!("finish payload");
        };
        let root = finish.root_result_schema.as_mut().expect("root schema");
        root.columns[0].field.as_mut().expect("field").r#type =
            Some(Box::new(plan::ArrowPhysicalType {
                kind: Some(plan::arrow_physical_type::Kind::Primitive(
                    plan::ArrowPrimitiveType::Int64 as i32,
                )),
            }));
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.root_result_schema.columns",
            ProtocolErrorKind::InconsistentFields,
        );

        let mut node = finish_node(40, vec![0], vec![simple_writer_plan(writer_payload())]);
        let Some(plan::distributed_node::Payload::TableFinish(finish)) = node.payload.as_mut()
        else {
            panic!("finish payload");
        };
        finish
            .root_result_schema
            .as_mut()
            .expect("root schema")
            .contract_version += 1;
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.root_result_schema.contract_version",
            ProtocolErrorKind::InvalidValue,
        );
    }

    #[test]
    fn a_writer_handle_over_the_single_handle_cap_is_refused_at_its_exact_path() {
        let mut writer = writer_payload();
        writer.handle = Some(iceberg_writer_handle(
            "a".repeat(MAX_CONNECTOR_WRITER_HANDLE_BYTES + 1),
        ));
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.handle",
            ProtocolErrorKind::OutOfRange,
        );
        assert!(
            error.contains("frozen single-handle budget"),
            "unexpected detail: {error}"
        );
    }

    #[test]
    fn a_writer_handle_without_a_provider_payload_is_refused_at_its_exact_path() {
        let mut writer = writer_payload();
        writer.handle = Some(write_dto::ConnectorWriterHandle {
            provider_payload: None,
        });
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.handle.provider_payload",
            ProtocolErrorKind::MissingField,
        );
    }

    #[test]
    fn a_writer_handle_with_the_wrong_payload_category_is_refused_at_the_public_boundary() {
        let mut writer = writer_payload();
        let mut carrier = iceberg_writer_handle("9c2f1f66".to_string());
        carrier
            .provider_payload
            .as_mut()
            .expect("provider payload")
            .header
            .as_mut()
            .expect("envelope header")
            .category =
            novarocks_proto_models::connector_common::ConnectorPayloadCategory::CommitFragment
                as i32;
        writer.handle = Some(carrier);
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.handle.provider_payload.header",
            ProtocolErrorKind::InconsistentFields,
        );
    }

    #[test]
    fn a_writer_target_ordinal_beyond_the_frozen_bound_is_refused() {
        let mut writer = writer_payload();
        writer.write_target_ordinal = u32::MAX;
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.write_target_ordinal",
            ProtocolErrorKind::OutOfRange,
        );
    }

    #[test]
    fn a_writer_input_ordinal_outside_its_input_width_is_refused() {
        let mut writer = writer_payload();
        writer.input = Some(plan::ConnectorWriteInputBinding {
            kind: Some(plan::connector_write_input_binding::Kind::OutputOrdinals(
                plan::UInt64List { values: vec![7] },
            )),
        });
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.input.output_ordinals[0]",
            ProtocolErrorKind::OutOfRange,
        );
    }

    #[test]
    fn a_finish_node_with_repeated_expected_ordinals_is_refused() {
        let mut writer = writer_payload();
        writer.write_target_ordinal = 1;
        let node = finish_node(40, vec![1, 1], vec![simple_writer_plan(writer)]);
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.expected_target_ordinals",
            ProtocolErrorKind::InvalidValue,
        );
        assert!(error.contains("repeats"), "unexpected detail: {error}");
    }

    /// A query set is the targets *this query's* writers feed, so it need not
    /// be dense from zero: a copy-on-write statement compiles one writer per
    /// query, at that group's own ordinal. Denseness stays a property of the
    /// session's sealed set, checked where the session is sealed.
    #[test]
    fn a_finish_node_with_a_single_non_zero_expected_ordinal_decodes() {
        let mut writer = writer_payload();
        writer.write_target_ordinal = 2;
        let node = finish_node(40, vec![2], vec![simple_writer_plan(writer)]);
        let decoded = decode_with(&node, recording_execution()).expect("table finish decodes");
        let ExecNodeKind::TableFinish(finish) = &decoded.node.kind else {
            panic!("expected a table finish, got {:?}", decoded.node.kind);
        };
        assert!(finish.accepts_target(WriteTargetOrdinal::try_new(2).expect("bounded ordinal")));
        // Exact membership, not a bound: ordinal 0 is below the highest
        // expected one and is still not part of this query's set.
        assert!(!finish.accepts_target(WriteTargetOrdinal::try_new(0).expect("bounded ordinal")));
    }

    /// A writer below the highest expected ordinal but outside the set is still
    /// a self-contradictory plan.
    #[test]
    fn a_writer_below_the_highest_expected_ordinal_but_outside_the_set_is_refused() {
        let mut writer = writer_payload();
        writer.write_target_ordinal = 0;
        let node = finish_node(40, vec![2], vec![simple_writer_plan(writer)]);
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.expected_target_ordinals",
            ProtocolErrorKind::InconsistentFields,
        );
        assert!(error.contains("outside"), "unexpected detail: {error}");
    }

    #[test]
    fn a_finish_node_with_no_expected_ordinals_is_refused() {
        let node = finish_node(40, Vec::new(), vec![simple_writer_plan(writer_payload())]);
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.expected_target_ordinals",
            ProtocolErrorKind::InvalidValue,
        );
    }

    #[test]
    fn a_writer_target_outside_the_finish_nodes_sealed_set_is_refused() {
        let mut writer = writer_payload();
        writer.write_target_ordinal = 3;
        let node = finish_node(40, vec![0], vec![simple_writer_plan(writer)]);
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.expected_target_ordinals",
            ProtocolErrorKind::InconsistentFields,
        );
        assert!(error.contains("outside"), "unexpected detail: {error}");
    }

    #[test]
    fn a_limit_on_a_write_dataflow_node_is_refused() {
        // A limit would truncate the write relation and drop commit fragments.
        let mut node = simple_writer_plan(writer_payload());
        node.limit = 1;
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.limit",
            ProtocolErrorKind::InconsistentFields,
        );

        let mut node = finish_node(40, vec![0], vec![simple_writer_plan(writer_payload())]);
        node.limit = 1;
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.limit",
            ProtocolErrorKind::InconsistentFields,
        );
    }

    #[test]
    fn a_finish_input_that_is_not_a_write_relation_is_refused() {
        let node = finish_node(
            40,
            vec![0],
            vec![one_col_values_node_with(10, CHILD_COLUMN_ID, "id", 42)],
        );
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.children[0]",
            ProtocolErrorKind::InconsistentFields,
        );
    }

    #[test]
    fn a_finish_input_must_match_slot_name_type_nullability_and_order() {
        let mutations: [fn(&mut Vec<novarocks_proto_models::common::OutputColumn>); 4] = [
            |columns| columns[0].column_id -= 1,
            |columns| columns[0].name = "wrong_kind".to_string(),
            |columns| {
                columns[0].r#type = Some(
                    crate::fragment::decode::type_decode::encode_type(&DataType::Int64)
                        .expect("type"),
                )
            },
            |columns| columns[0].nullable = true,
        ];
        for mutate in mutations {
            let node = finish_node(40, vec![0], vec![four_column_write_relation_values(mutate)]);
            let error = decode_error(&node);
            assert_protocol(
                &error,
                "plan_fragment.root.payload.table_finish.children[0]",
                ProtocolErrorKind::InconsistentFields,
            );
        }

        let node = finish_node(
            40,
            vec![0],
            vec![four_column_write_relation_values(|columns| {
                columns.swap(0, 1)
            })],
        );
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.children[0]",
            ProtocolErrorKind::InconsistentFields,
        );
    }

    #[test]
    fn root_nested_physical_contract_tampering_is_rejected() {
        let mut node = finish_node(40, vec![0], vec![simple_writer_plan(writer_payload())]);
        let Some(plan::distributed_node::Payload::TableFinish(finish)) = node.payload.as_mut()
        else {
            panic!("finish payload");
        };
        let input_type = finish
            .root_result_schema
            .as_mut()
            .expect("root schema")
            .columns[4]
            .field
            .as_mut()
            .expect("field")
            .r#type
            .as_mut()
            .expect("type");
        let Some(plan::arrow_physical_type::Kind::List(item)) = input_type.kind.as_mut() else {
            panic!("input_fields list");
        };
        item.nullable = true;
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.root_result_schema.columns",
            ProtocolErrorKind::InconsistentFields,
        );

        let mut node = finish_node(40, vec![0], vec![simple_writer_plan(writer_payload())]);
        let Some(plan::distributed_node::Payload::TableFinish(finish)) = node.payload.as_mut()
        else {
            panic!("finish payload");
        };
        let map_type = finish
            .root_result_schema
            .as_mut()
            .expect("root schema")
            .columns[7]
            .field
            .as_mut()
            .expect("field")
            .r#type
            .as_mut()
            .expect("type");
        let Some(plan::arrow_physical_type::Kind::Map(map)) = map_type.kind.as_mut() else {
            panic!("properties map");
        };
        map.ordered = true;
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.root_result_schema.columns",
            ProtocolErrorKind::InconsistentFields,
        );
    }

    #[test]
    fn a_write_node_without_a_query_leased_runtime_is_refused() {
        let mut arena = ExprArena::default();
        let error = decode_node(
            &simple_writer_plan(writer_payload()),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .expect_err("a write node needs a query-leased runtime");
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.catalog_handle",
            ProtocolErrorKind::MissingField,
        );
    }

    // -------------------------------------------------- structural write limits

    #[tokio::test]
    async fn the_backend_write_binding_can_only_open_writers_and_move_carriers() {
        // The proof is the binding's own shape: the single member that reaches a
        // provider is `ConnectorWriteExecution`, whose only method is
        // `open_writer`. There is no commit handle, no control binding, and no
        // metadata mutation reachable from here, and the two codec facets it
        // holds run in opposite directions from the frontend's.
        let execution = recording_execution();
        let binding = test_write_binding(Arc::clone(&execution));
        let write_execution = binding.execution();
        assert_eq!(
            write_execution.catalog_handle(),
            &test_write_catalog_handle()
        );
        let mut writer = write_execution
            .open_writer(ConnectorOpenWriterRequest {
                handle: test_write_adapter().wrap_writer_handle("t".to_string()),
                target: WriteTargetOrdinal::try_new(0).expect("ordinal"),
                expected_schema: Arc::new(arrow::datatypes::Schema::empty()),
                physical: ConnectorWriterPhysicalContext::new([0; 16], 1, [0; 16], 0, 0),
                context: test_request_context(),
            })
            .await
            .expect("open writer");
        // A finished writer only ever yields commit fragments, never a commit.
        assert!(writer.finish().await.expect("finish").is_empty());
        assert_eq!(binding.handle_decoder().owner(), TEST_WRITE_CATALOG);
        assert_eq!(binding.fragment_encoder().owner(), TEST_WRITE_CATALOG);
    }
}
