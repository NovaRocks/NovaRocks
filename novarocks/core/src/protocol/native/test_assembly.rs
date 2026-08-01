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

//! Execution-domain assembly seam for a backend-decoded native fragment.
//!
//! The `InstanceParams` execution values, sink assignment, exchange contracts,
//! and fragment-level runtime-filter contract are decoded by the backend role.
//! This module builds the shared execution submission without exposing decode
//! contexts, registries, or runtime state. It invokes the backend decoders at
//! their former core validation points, preserving error ordering.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::common::ids::SlotId;
use crate::exec::chunk::{ChunkSchemaRef, ChunkSlotSchema};
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::fragment::program::{
    ExchangeInputContract, FragmentNodeId, RuntimeFilterContract, ScanSourceContract,
};
use crate::proto::expr;
use crate::proto::novarocks;
use crate::proto::plan;
use crate::protocol::ProtocolError;
use crate::protocol::{FieldPath, ProtocolErrorKind, ProtocolFamily};
use crate::runtime::fragment::instance::{
    BackendNum, ExchangeInputAssignments, FragmentInstanceId, FragmentSinkAssignment,
};
use crate::runtime::query_context::QueryId;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::scan_range::ScanRangeParams;

/// Immutable input-slot value supplied to backend expression decoders.
///
/// It deliberately contains no connector, runtime, or arena state. The
/// backend may only resolve a wire column id to an already-established slot.
#[derive(Clone, Debug, Default)]
pub struct NativeExpressionInputLayout {
    slots: Vec<SlotId>,
}

impl NativeExpressionInputLayout {
    pub fn from_slot_ids(slots: impl IntoIterator<Item = SlotId>) -> Self {
        let mut layout = Self::default();
        for slot in slots {
            if !layout.slots.contains(&slot) {
                layout.slots.push(slot);
            }
        }
        layout
    }

    pub fn resolve_column_id(
        &self,
        column_id: u32,
        path: FieldPath,
    ) -> Result<SlotId, ProtocolError> {
        let slot = SlotId::new(column_id);
        if self.slots.contains(&slot) {
            Ok(slot)
        } else {
            Err(ProtocolError::new(
                ProtocolFamily::Native,
                path.field("column_id"),
                ProtocolErrorKind::InvalidValue,
                format!("ColumnRef column_id={column_id} not found in input layout"),
            ))
        }
    }
}

#[cfg(test)]
mod expression_layout_tests {
    use super::NativeExpressionInputLayout;
    use crate::common::ids::SlotId;
    use crate::protocol::FieldPath;

    #[test]
    fn preserves_unknown_column_error_contract() {
        let error = NativeExpressionInputLayout::from_slot_ids([SlotId::new(7)])
            .resolve_column_id(9, FieldPath::root("expr").field("column_ref"))
            .expect_err("unknown slot must fail");
        assert_eq!(
            error.to_string(),
            "native protocol error at expr.column_ref.column_id (invalid value): ColumnRef column_id=9 not found in input layout"
        );
    }
}

/// Backend-decoded `InstanceParams` execution values required to assemble a
/// fragment. Sink assignment is carried separately to keep its validation at
/// the established assembly point.
#[derive(Debug)]
pub struct NativeFragmentInstanceInput {
    pub(crate) query_id: QueryId,
    pub(crate) fragment_instance_id: FragmentInstanceId,
    pub(crate) backend_num: BackendNum,
    pub(crate) query_options: QueryOptions,
    pub(crate) pipeline_dop: NonZeroUsize,
    pub(crate) raw_scan_ranges: BTreeMap<FragmentNodeId, Vec<ScanRangeParams>>,
    pub(crate) exchange_inputs: ExchangeInputAssignments,
    pub(crate) typed_result_sink: bool,
}

impl NativeFragmentInstanceInput {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_id: QueryId,
        fragment_instance_id: FragmentInstanceId,
        backend_num: BackendNum,
        query_options: QueryOptions,
        pipeline_dop: NonZeroUsize,
        raw_scan_ranges: BTreeMap<FragmentNodeId, Vec<ScanRangeParams>>,
        exchange_inputs: ExchangeInputAssignments,
        typed_result_sink: bool,
    ) -> Self {
        Self {
            query_id,
            fragment_instance_id,
            backend_num,
            query_options,
            pipeline_dop,
            raw_scan_ranges,
            exchange_inputs,
            typed_result_sink,
        }
    }
}

/// Backend-owned sink-assignment decoder invoked during core assembly.
pub trait NativeFragmentSinkAssignmentDecoder: Send + Sync {
    fn decode_sink_assignment(
        &self,
        sink: &plan::DataSink,
        instance: &novarocks::InstanceParams,
    ) -> Result<FragmentSinkAssignment, ProtocolError>;
}

/// Backend-owned envelope decoder invoked at the original fragment root and
/// sink presence-validation points.
pub trait NativeFragmentEnvelopeDecoder: Send + Sync {
    fn require_root<'a>(
        &self,
        fragment: &'a plan::PlanFragment,
    ) -> Result<&'a plan::DistributedNode, ProtocolError>;

    fn require_sink<'a>(
        &self,
        fragment: &'a plan::PlanFragment,
    ) -> Result<&'a plan::DataSink, ProtocolError>;
}

/// Backend-owned structural validation for native fragment wire payloads.
///
/// This intentionally covers only recursive wire-shape checks that precede
/// lowering. It receives no runtime state, connector registry, or execution
/// objects, so the backend remains the DTO owner without widening the core
/// execution seam.
pub trait NativeFragmentSubmissionValidator: Send + Sync {
    fn validate_root_node(
        &self,
        root: &plan::DistributedNode,
        path: FieldPath,
    ) -> Result<(), ProtocolError>;

    fn validate_fragment_expressions(
        &self,
        fragment: &plan::PlanFragment,
    ) -> Result<(), ProtocolError>;
}

/// Backend-owned expression decoder capability used by native plan lowering.
///
/// The shared core supplies only the expression arena and immutable input-slot
/// layout. Query lifecycle, connector, and runtime-registry state remain out
/// of this contract.
pub trait NativeExpressionDecoder: Send + Sync {
    fn decode_expression(
        &self,
        expression: &expr::Expr,
        path: FieldPath,
        arena: &mut ExprArena,
        input: &NativeExpressionInputLayout,
    ) -> Result<ExprId, ProtocolError>;
}

/// Backend-decoded physical output layout for a native plan node.
///
/// The core consumes this as execution metadata only. Wire type decoding and
/// duplicate-slot validation remain owned by the backend role.
#[derive(Clone, Debug)]
pub struct NativeOutputLayout {
    slot_ids: Vec<SlotId>,
    chunk_schema: ChunkSchemaRef,
    slot_schemas: Vec<ChunkSlotSchema>,
}

impl NativeOutputLayout {
    pub fn new(
        slot_ids: Vec<SlotId>,
        chunk_schema: ChunkSchemaRef,
        slot_schemas: Vec<ChunkSlotSchema>,
    ) -> Self {
        Self {
            slot_ids,
            chunk_schema,
            slot_schemas,
        }
    }

    pub fn slot_ids(&self) -> &[SlotId] {
        &self.slot_ids
    }

    pub fn chunk_schema(&self) -> ChunkSchemaRef {
        self.chunk_schema.clone()
    }

    pub fn slot_schemas(&self) -> &[ChunkSlotSchema] {
        &self.slot_schemas
    }
}

/// Backend-owned decoding of native `OutputColumn` wire metadata.
pub trait NativeOutputLayoutDecoder: Send + Sync {
    fn decode_output_layout(
        &self,
        columns: &[crate::proto::common::OutputColumn],
        path: FieldPath,
    ) -> Result<NativeOutputLayout, ProtocolError>;
}

/// Backend-owned runtime-filter contract decoder invoked after plan assembly
/// has consumed the binding table.
pub trait RuntimeFilterExecutionContractDecoder: Send + Sync {
    fn decode_runtime_filter_contract(
        &self,
        fragment: &plan::PlanFragment,
    ) -> Result<RuntimeFilterContract, ProtocolError>;
}

/// Backend-owned exchange contract decoder invoked after the fragment sink
/// program has been assembled.
pub trait NativeExchangeContractDecoder: Send + Sync {
    fn decode_exchange_contracts(
        &self,
        root: &plan::DistributedNode,
        path: crate::protocol::FieldPath,
    ) -> Result<BTreeMap<FragmentNodeId, ExchangeInputContract>, ProtocolError>;
}

/// Backend-owned static scan-source contract decoder invoked before scan-range
/// assignments are cross-checked.
pub trait NativeScanSourceContractDecoder: Send + Sync {
    fn decode_scan_source_contracts(
        &self,
        root: &plan::DistributedNode,
        path: crate::protocol::FieldPath,
    ) -> Result<BTreeMap<FragmentNodeId, ScanSourceContract>, ProtocolError>;
}
