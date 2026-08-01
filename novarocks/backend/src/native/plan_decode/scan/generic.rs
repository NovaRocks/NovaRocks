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

//! Native decoding for the provider-neutral SPI read carrier.

use std::collections::BTreeSet;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use arrow::ipc::reader::StreamReader;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorExecutionBindingKey, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorOpenReaderRequest, ConnectorRequestContext,
    ConnectorScanHandle, ConnectorSplit,
};

use novarocks::common::ids::SlotId;
use novarocks::connector::runtime::{
    ConnectorBatchTransform, ConnectorReadScanSource, ConnectorScheduledSplit,
};
use novarocks::exec::chunk::ChunkSchema;
use novarocks::exec::expr::ExprArena;
use novarocks::exec::node::scan::BoundScanRanges;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::formats::parquet::{
    ParquetSlotKind, VariantPathSpec, convert_variant_columns, materialize_variant_path_columns,
};
use novarocks::protocol::ProtocolErrorKind;
use novarocks::runtime::query_options::query_expire_durations;
use novarocks_protocol::plan;

use super::super::context::NativePlanDecodeContext;
use super::super::error::NativeFragmentLeafDecodeError;
use super::super::node::DecodedNode;
use super::common::{DecodedScanOutputColumns, lower_scan_predicate, parse_scan_limit};
use super::variant_path::NativeVariantPathPlan;

pub(super) fn lower_connector_read_scan(
    node: &plan::DistributedNode,
    scan: &plan::ScanNode,
    source: &plan::ConnectorReadSource,
    output_columns: &DecodedScanOutputColumns,
    variant_path_plan: NativeVariantPathPlan,
    ctx: &NativePlanDecodeContext,
    arena: &mut ExprArena,
) -> Result<DecodedNode, NativeFragmentLeafDecodeError> {
    let instance_id = ConnectorInstanceId::parse(&source.instance_id).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "instance_id",
            error.to_string(),
        )
    })?;
    let incarnation = ConnectorInstanceIncarnation::from_bytes(
        source
            .instance_incarnation
            .as_slice()
            .try_into()
            .map_err(|_| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InvalidValue,
                    "instance_incarnation",
                    "ConnectorReadSource instance_incarnation must contain exactly 16 bytes",
                )
            })?,
    );
    let binding_key = ConnectorExecutionBindingKey {
        instance_id: instance_id.clone(),
        incarnation,
    };
    let batch = ConnectorBatchBudget {
        max_rows: required_nonzero_usize(source.max_batch_rows, "max_batch_rows")?,
        max_bytes: required_nonzero_usize(source.max_batch_bytes, "max_batch_bytes")?,
    };
    ctx.query_id().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "query_id",
            "ConnectorReadSource requires a native query identity",
        )
    })?;
    let (_, query_expire) = query_expire_durations(ctx.query_options());
    let request_context = ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        ctx.connector_cancellation()?,
        required_usize(source.max_handle_payload_bytes, "max_handle_payload_bytes")?,
        required_usize(source.max_total_payload_bytes, "max_total_payload_bytes")?,
    )
    .map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "payload_budgets",
            error.to_string(),
        )
    })?;
    let _scan = ConnectorScanHandle::try_new(
        instance_id.clone(),
        Bytes::copy_from_slice(&source.scan_payload),
    )
    .map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "scan_payload",
            error.to_string(),
        )
    })?;
    let mut total_payload_bytes = source.scan_payload.len();
    let mut split_ids = BTreeSet::new();
    let scheduled = source
        .splits
        .iter()
        .enumerate()
        .map(|(index, wire_split)| {
            let split = ConnectorSplit::try_new(
                instance_id.clone(),
                wire_split.split_id.clone(),
                Bytes::copy_from_slice(&wire_split.split_payload),
                wire_split.estimated_bytes,
            )
            .map_err(|error| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InvalidValue,
                    "splits",
                    error.to_string(),
                )
                .append_index(index)
            })?;
            if !split_ids.insert(split.split_id().to_string()) {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "splits",
                    "ConnectorReadSource has duplicate split_id values",
                )
                .append_index(index));
            }
            if split.payload().len() > request_context.max_handle_payload_bytes() {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::OutOfRange,
                    "splits",
                    "connector split payload exceeds its request handle budget",
                )
                .append_index(index));
            }
            total_payload_bytes = total_payload_bytes.saturating_add(split.payload().len());
            Ok(ConnectorScheduledSplit::plain(split))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if source.scan_payload.len() > request_context.max_handle_payload_bytes() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "scan_payload",
            "connector scan payload exceeds its request handle budget",
        ));
    }
    if total_payload_bytes > request_context.max_total_payload_bytes() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "splits",
            "connector read payloads exceed their request total budget",
        ));
    }

    let layout = output_columns.layout();
    let output_schema = output_columns.output_schema();
    let expected_schema = if variant_path_plan.specs.is_empty() {
        decode_expected_schema_ipc(
            &source.expected_schema_ipc,
            output_schema.arrow_schema_ref().as_ref(),
            request_context.max_handle_payload_bytes(),
        )?
    } else {
        decode_connector_schema_ipc(
            &source.expected_schema_ipc,
            request_context.max_handle_payload_bytes(),
        )?
    };
    let (output_schema, batch_transform) = if variant_path_plan.specs.is_empty() {
        (
            output_schema_with_connector_fields(&output_schema, expected_schema.as_ref())?,
            None,
        )
    } else {
        let read_slot_ids = connector_read_slot_ids(scan, expected_schema.as_ref())?;
        validate_variant_path_read_slots(&variant_path_plan.specs, &read_slot_ids)?;
        let variant_slot_kinds = output_schema
            .slot_ids()
            .iter()
            .map(|slot_id| {
                variant_path_plan
                    .specs
                    .iter()
                    .any(|spec| spec.source_slot_id == *slot_id)
                    .then_some(ParquetSlotKind::Variant)
                    .unwrap_or(ParquetSlotKind::Regular)
            })
            .collect();
        let output_slot_ids = output_schema.slot_ids().to_vec();
        (
            output_schema,
            Some(ConnectorVariantPathTransform {
                read_slot_ids,
                output_slot_ids,
                specs: variant_path_plan.specs,
                output_slot_kinds: variant_slot_kinds,
            }),
        )
    };
    let binding = ctx
        .execution_resolver()?
        .resolve(&binding_key)
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "instance_id",
                error.to_string(),
            )
        })?;
    if novarocks::common::config::debug_emit_connector_reader_marker() {
        println!(
            "NOVAROCKS_CONNECTOR_READ_SOURCE instance={} splits={}",
            instance_id.as_str(),
            scheduled.len()
        );
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
    let request = ConnectorOpenReaderRequest {
        expected_schema,
        batch,
        context: request_context,
    };
    let predicate = lower_scan_predicate(scan, arena, &layout, ctx)?;
    let source = Arc::new(
        ConnectorReadScanSource::new_scheduled_execution_with_batch_transform(
            binding,
            scheduled,
            request,
            output_schema.clone(),
            batch_transform
                .map(|transform| Arc::new(transform) as Arc<dyn ConnectorBatchTransform>),
        ),
    );
    ctx.capture_scan_ranges(node.node_id, BoundScanRanges::None);
    let scan_node = novarocks::exec::node::scan::ScanNode::new(source)
        .with_node_id(node.node_id)
        .with_output_chunk_schema(output_schema.clone())
        .with_limit(parse_scan_limit(node.limit)?)
        .with_conjunct_predicate(predicate)
        .with_accept_empty_scan_ranges(true);
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan_node),
        },
        layout,
        output_schema,
    })
}

#[derive(Clone)]
struct ConnectorVariantPathTransform {
    read_slot_ids: Vec<SlotId>,
    output_slot_ids: Vec<SlotId>,
    specs: Vec<VariantPathSpec>,
    output_slot_kinds: Vec<ParquetSlotKind>,
}

impl ConnectorVariantPathTransform {
    fn apply(
        &self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, String> {
        materialize_variant_path_columns(
            batch,
            &self.read_slot_ids,
            &self.output_slot_ids,
            &self.specs,
        )
        .and_then(|batch| convert_variant_columns(&self.output_slot_kinds, batch))
    }
}

impl ConnectorBatchTransform for ConnectorVariantPathTransform {
    fn transform(
        &self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, String> {
        self.apply(batch)
    }
}

fn connector_read_slot_ids(
    scan: &plan::ScanNode,
    schema: &arrow::datatypes::Schema,
) -> Result<Vec<SlotId>, NativeFragmentLeafDecodeError> {
    schema
        .fields()
        .iter()
        .map(|field| {
            scan.columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(field.name()))
                .map(|column| SlotId::new(column.column_id))
                .ok_or_else(|| {
                    NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::InconsistentFields,
                        "expected_schema_ipc",
                        format!(
                            "ConnectorReadSource expected Arrow field `{}` is not a ScanNode column",
                            field.name()
                        ),
                    )
                })
        })
        .collect()
}

fn validate_variant_path_read_slots(
    specs: &[VariantPathSpec],
    read_slot_ids: &[SlotId],
) -> Result<(), NativeFragmentLeafDecodeError> {
    for spec in specs {
        if !read_slot_ids.contains(&spec.source_read_slot_id) {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "expected_schema_ipc",
                format!(
                    "ConnectorReadSource expected Arrow schema omits VARIANT source `{}` for output `{}`",
                    spec.source_name, spec.output_name
                ),
            ));
        }
    }
    Ok(())
}

fn decode_expected_schema_ipc(
    encoded: &[u8],
    expected: &arrow::datatypes::Schema,
    max_bytes: usize,
) -> Result<arrow::datatypes::SchemaRef, NativeFragmentLeafDecodeError> {
    if encoded.is_empty() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "expected_schema_ipc",
            "ConnectorReadSource requires an expected Arrow schema",
        ));
    }
    if encoded.len() > max_bytes {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "expected_schema_ipc",
            format!("ConnectorReadSource expected Arrow schema exceeds handle budget {max_bytes}"),
        ));
    }
    let reader = StreamReader::try_new(Cursor::new(encoded), None).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "expected_schema_ipc",
            format!("decode ConnectorReadSource expected Arrow schema: {error}"),
        )
    })?;
    if !schema_matches_connector_contract(reader.schema().as_ref(), expected) {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "expected_schema_ipc",
            format!(
                "ConnectorReadSource expected Arrow schema does not match scan output columns: \
                 carrier={:?} decoded={expected:?}",
                reader.schema()
            ),
        ));
    }
    Ok(reader.schema())
}

fn decode_connector_schema_ipc(
    encoded: &[u8],
    max_bytes: usize,
) -> Result<arrow::datatypes::SchemaRef, NativeFragmentLeafDecodeError> {
    if encoded.is_empty() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "expected_schema_ipc",
            "ConnectorReadSource requires an expected Arrow schema",
        ));
    }
    if encoded.len() > max_bytes {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "expected_schema_ipc",
            format!("ConnectorReadSource expected Arrow schema exceeds handle budget {max_bytes}"),
        ));
    }
    StreamReader::try_new(Cursor::new(encoded), None)
        .map(|reader| reader.schema())
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "expected_schema_ipc",
                format!("decode ConnectorReadSource expected Arrow schema: {error}"),
            )
        })
}

fn output_schema_with_connector_fields(
    output_schema: &ChunkSchema,
    expected: &arrow::datatypes::Schema,
) -> Result<Arc<ChunkSchema>, NativeFragmentLeafDecodeError> {
    let slots = output_schema
        .slots()
        .iter()
        .zip(expected.fields())
        .map(|(slot, field)| slot.with_field(field.as_ref().clone()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "expected_schema_ipc",
                format!("apply ConnectorReadSource expected Arrow schema: {error}"),
            )
        })?;
    ChunkSchema::try_new_with_schema_metadata(slots, expected.metadata().clone())
        .map(Arc::new)
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "expected_schema_ipc",
                format!("build ConnectorReadSource output schema: {error}"),
            )
        })
}

fn schema_matches_connector_contract(
    carrier: &arrow::datatypes::Schema,
    output: &arrow::datatypes::Schema,
) -> bool {
    carrier.fields().len() == output.fields().len()
        && carrier
            .fields()
            .iter()
            .zip(output.fields())
            .all(|(carrier, output)| field_matches_connector_contract(carrier, output))
}

fn field_matches_connector_contract(
    carrier: &arrow::datatypes::Field,
    output: &arrow::datatypes::Field,
) -> bool {
    carrier.name() == output.name()
        && carrier.is_nullable() == output.is_nullable()
        && data_type_matches_connector_contract(carrier.data_type(), output.data_type())
}

fn data_type_matches_connector_contract(
    carrier: &arrow::datatypes::DataType,
    output: &arrow::datatypes::DataType,
) -> bool {
    use arrow::datatypes::DataType;

    match (carrier, output) {
        (DataType::Struct(carrier), DataType::Struct(output)) => {
            carrier.len() == output.len()
                && carrier
                    .iter()
                    .zip(output.iter())
                    .all(|(carrier, output)| field_matches_connector_contract(carrier, output))
        }
        (DataType::List(carrier), DataType::List(output))
        | (DataType::LargeList(carrier), DataType::LargeList(output))
        | (DataType::FixedSizeList(carrier, _), DataType::FixedSizeList(output, _)) => {
            field_matches_connector_contract(carrier, output)
        }
        (DataType::Map(carrier, carrier_sorted), DataType::Map(output, output_sorted)) => {
            carrier_sorted == output_sorted && field_matches_connector_contract(carrier, output)
        }
        _ => carrier == output,
    }
}

fn required_nonzero_usize(
    value: u64,
    field: &'static str,
) -> Result<NonZeroUsize, NativeFragmentLeafDecodeError> {
    NonZeroUsize::new(required_usize(value, field)?).ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            field,
            format!("ConnectorReadSource {field} must be nonzero"),
        )
    })
}

fn required_usize(value: u64, field: &'static str) -> Result<usize, NativeFragmentLeafDecodeError> {
    usize::try_from(value).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            field,
            format!("ConnectorReadSource {field} does not fit usize"),
        )
    })
}
