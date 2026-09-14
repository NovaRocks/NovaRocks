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

//! Shared fragment scan decoding helpers.

use arrow::datatypes::DataType;

use novarocks_execution::exec::chunk::ChunkSchema;
use novarocks_execution::exec::chunk::SlotLayout as Layout;
use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks_execution::exec::variant_read::{
    ParquetSlotKind, VariantPathSpec, convert_variant_columns, materialize_variant_path_columns,
};
use novarocks_native_adapter::fragment_error::NativeFragmentLeafDecodeError;
use novarocks_native_adapter::fragment_expression::decode_expr_for_slot_layout;
use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
use novarocks_proto_models::plan;
use novarocks_types::SlotId;
use novarocks_worker::connector_batch_transform::ConnectorBatchTransform;

/// Turn one scan's read columns into the columns its plan node outputs.
///
/// A scan that projects a VARIANT path reads the physical LargeBinary column
/// and derives the synthetic column from it after the read. Both carriers need
/// exactly this, so it lives here: the opaque one attaches it as its reader's
/// batch transform, the typed one as its output materialization.
#[derive(Clone)]
pub(super) struct ConnectorVariantPathTransform {
    read_slot_ids: Vec<SlotId>,
    output_slot_ids: Vec<SlotId>,
    /// The fragment's declared output fields. Connector pages carry generated
    /// `slot_<id>` field names, so materialization must restore these names
    /// before the chunk is checked against the scan-node contract.
    output_fields: Vec<arrow::datatypes::Field>,
    specs: Vec<VariantPathSpec>,
    output_slot_kinds: Vec<ParquetSlotKind>,
}

impl ConnectorVariantPathTransform {
    /// Build the transform from the read layout and the node's output schema.
    ///
    /// An output slot that is a VARIANT path source stays visible as a variant
    /// column, so it is converted rather than passed through as raw bytes.
    pub(super) fn new(
        read_slot_ids: Vec<SlotId>,
        output_schema: &ChunkSchema,
        specs: Vec<VariantPathSpec>,
    ) -> Self {
        let output_slot_kinds = output_schema
            .slot_ids()
            .iter()
            .map(|slot_id| {
                if specs.iter().any(|spec| spec.source_slot_id == *slot_id) {
                    ParquetSlotKind::Variant
                } else {
                    ParquetSlotKind::Regular
                }
            })
            .collect();
        Self {
            read_slot_ids,
            output_slot_ids: output_schema.slot_ids().to_vec(),
            output_fields: output_schema
                .slots()
                .iter()
                .map(|slot| slot.field().clone())
                .collect(),
            specs,
            output_slot_kinds,
        }
    }

    fn apply(
        &self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, String> {
        let batch = materialize_variant_path_columns(
            batch,
            &self.read_slot_ids,
            &self.output_slot_ids,
            &self.specs,
        )
        .and_then(|batch| convert_variant_columns(&self.output_slot_kinds, batch))?;
        arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(self.output_fields.clone())),
            batch.columns().to_vec(),
        )
        .map_err(|error| format!("restore variant scan output schema failed: {error}"))
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

/// Refuse a plan whose read columns omit a VARIANT path's source column.
///
/// The source column is what the synthetic column is extracted from, so a read
/// layout without it describes a derivation with nothing to derive from.
/// `field` names the wire field that decided the read layout, which differs per
/// carrier: the opaque one publishes it as an Arrow schema, the typed one as
/// its ordered assignments.
pub(super) fn validate_variant_path_read_slots(
    specs: &[VariantPathSpec],
    read_slot_ids: &[SlotId],
    field: &'static str,
) -> Result<(), NativeFragmentLeafDecodeError> {
    for spec in specs {
        if !read_slot_ids.contains(&spec.source_read_slot_id) {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                field,
                format!(
                    "scan read columns omit VARIANT source `{}` for output `{}`",
                    spec.source_name, spec.output_name
                ),
            ));
        }
    }
    Ok(())
}

pub(super) fn lower_scan_predicate(
    scan: &plan::ScanNode,
    arena: &mut ExprArena,
    layout: &Layout,
) -> Result<Option<ExprId>, NativeFragmentLeafDecodeError> {
    let mut predicate = None;
    for (idx, expr) in scan.predicates.iter().enumerate() {
        let expr_id = decode_expr_for_slot_layout(
            expr,
            FieldPath::root("scan").field("predicates").index(idx),
            arena,
            layout,
        )
        .map_err(|err| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "predicates",
                format!("ScanNode predicate {idx}: {err}"),
            )
            .append_index(idx)
        })?;
        predicate = Some(match predicate {
            Some(prev) => arena.push_typed(ExprNode::And(prev, expr_id), DataType::Boolean),
            None => expr_id,
        });
    }
    Ok(predicate)
}

pub(super) fn parse_scan_limit(limit: i64) -> Result<Option<usize>, NativeFragmentLeafDecodeError> {
    if limit == -1 {
        Ok(None)
    } else if limit < 0 {
        Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "limit",
            format!("ScanNode limit must be -1 or >= 0, got {limit}"),
        ))
    } else {
        Ok(Some(limit as usize))
    }
}
