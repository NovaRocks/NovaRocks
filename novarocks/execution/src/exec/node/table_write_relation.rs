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

//! The execution-side binding of the two write row relations, plus the two
//! codec ports the write data plane needs.
//!
//! The relations themselves — their columns, their kind codes, and their
//! kind-versus-nullability invariants — are defined once in
//! `novarocks_spi::connector::write_stack::relation`, because the SQL planner,
//! the execution engine, the backend, and the frontend must all agree on them.
//! This module adds only what is execution-local: the `SlotId` each column
//! carries inside a [`Chunk`], and the two narrow ports below.
//!
//! `novarocks-execution` depends only on `novarocks-types` and `novarocks-spi`,
//! and its dependency closure is forbidden from ever reaching the generated
//! wire crates, so this layer structurally cannot encode or decode a commit
//! fragment. Canonical bytes enter and leave through the ports: execution moves
//! opaque buffers and counts them, and never interprets one.
//!
//! [`Chunk`]: crate::exec::chunk::Chunk

use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::write_stack::{
    ConnectorCommitFragment, ROOT_WRITE_RESULT_COLUMN_COUNT, RootWriteResultSchema,
    WRITE_RELATION_COLUMN_COUNT, WRITE_RELATION_FRAGMENT_INDEX, WRITE_RELATION_KIND_INDEX,
    WRITE_RELATION_ROW_COUNT_INDEX, WRITE_RELATION_TARGET_INDEX, WriteTargetOrdinal,
    WriterAuxiliaryChannel, WriterMultiplexSchema, write_relation_column_id,
};
use novarocks_types::SlotId;

use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef};
use crate::runtime::task::{ExecutionFailure, ExecutionReason, ExecutionResult};
use novarocks_failure_contract::{BoundedDetail, FailureClass, FailureFact, FailureProjection};

#[derive(Clone, Copy, Debug)]
pub(crate) enum WriteRelationViolation {
    Layout,
    SlotBinding,
    NotPrepared,
    Column,
}
#[derive(Debug)]
struct WriteRelationFailure {
    violation: WriteRelationViolation,
    detail: BoundedDetail,
}
impl std::fmt::Display for WriteRelationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.detail.fmt(f)
    }
}
impl std::error::Error for WriteRelationFailure {}
impl FailureProjection for WriteRelationFailure {
    fn failure_fact(&self) -> FailureFact {
        let reason = match self.violation {
            WriteRelationViolation::Layout => ExecutionReason::WriteRelationLayout,
            WriteRelationViolation::SlotBinding => ExecutionReason::WriteRelationBinding,
            WriteRelationViolation::NotPrepared => ExecutionReason::WriteRelationNotPrepared,
            WriteRelationViolation::Column => ExecutionReason::WriteRelationColumn,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
pub(crate) fn relation_failure(
    violation: WriteRelationViolation,
    detail: impl std::fmt::Display,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(WriteRelationFailure {
        violation,
        detail: BoundedDetail::truncating_display(detail),
    })
}
fn relation_connector(source: ConnectorError) -> ExecutionFailure {
    use crate::runtime::task::connector_failure::{ConnectorCallFailure, ConnectorOperation};
    ConnectorCallFailure::new(ConnectorOperation::ValidateWriteRelation, source).into()
}

/// Exact ordinary-aggregate boundary inside the composite write dataflow.
///
/// The enum is deliberately provider-neutral: it names only execution work,
/// while the application-owned guard decides whether a query-scoped test fault
/// rejects that work. A guard may reject; it cannot manufacture a partial or
/// final value and therefore cannot become a data fallback.
#[cfg(novarocks_dev_controls)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableWriteAggregateBoundary {
    PartialUpdate,
    PartialFinalize,
    FinalMerge,
    FinalFinalize,
}

/// Application-supplied rejection guard for composite-write aggregate work.
#[cfg(novarocks_dev_controls)]
pub trait TableWriteAggregateFaultHook: Send + Sync {
    fn check(&self, boundary: TableWriteAggregateBoundary) -> Result<(), ConnectorError>;
}

#[cfg(novarocks_dev_controls)]
#[derive(Debug, Default)]
pub struct AllowTableWriteAggregates;

#[cfg(novarocks_dev_controls)]
impl TableWriteAggregateFaultHook for AllowTableWriteAggregates {
    fn check(&self, _boundary: TableWriteAggregateBoundary) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Slot id of the `kind` column in both write relations.
///
/// These are the ids the write contract reserves, not a fresh tuple layout:
/// the exchange edge from a writer fragment to the root carries this relation
/// and declares those ids as its `output_slot_ids`, so a chunk numbered any
/// other way would not line up with the plan that routes it.
pub const WRITE_RELATION_KIND_SLOT: SlotId =
    SlotId::new(write_relation_column_id(WRITE_RELATION_KIND_INDEX));
/// Slot id of the `write_target_ordinal` column in both write relations.
pub const WRITE_RELATION_TARGET_SLOT: SlotId =
    SlotId::new(write_relation_column_id(WRITE_RELATION_TARGET_INDEX));
/// Slot id of the `row_count` column in both write relations.
pub const WRITE_RELATION_ROW_COUNT_SLOT: SlotId =
    SlotId::new(write_relation_column_id(WRITE_RELATION_ROW_COUNT_INDEX));
/// Slot id of the `commit_fragment` column in both write relations.
pub const WRITE_RELATION_FRAGMENT_SLOT: SlotId =
    SlotId::new(write_relation_column_id(WRITE_RELATION_FRAGMENT_INDEX));

const _: () = {
    assert!(WRITE_RELATION_KIND_INDEX == 0);
    assert!(WRITE_RELATION_TARGET_INDEX == 1);
    assert!(WRITE_RELATION_ROW_COUNT_INDEX == 2);
    assert!(WRITE_RELATION_FRAGMENT_INDEX == 3);
};

/// Execution-local slot binding of a plan-frozen writer multiplex schema.
/// Auxiliary fields remain generic Arrow slots.
#[derive(Clone, Debug)]
pub struct WriterMultiplexRelationSchema {
    contract: WriterMultiplexSchema,
    chunk_schema: ChunkSchemaRef,
}

impl WriterMultiplexRelationSchema {
    /// Bind a compiled program's positional writer relation: the layout is the
    /// exact SPI relation by Arrow schema, and every column, the fixed prefix
    /// included, is carried by its compiler-allocated layout slot. An
    /// auxiliary channel is identified by its layout slot.
    #[cfg(test)]
    pub(crate) fn try_from_compiled_layout(
        layout: &novarocks_local_program::StaticLayout,
    ) -> ExecutionResult<Self> {
        if !layout.is_typed() {
            return Err(relation_failure(
                WriteRelationViolation::Layout,
                "writer multiplex requires a complete typed layout",
            ));
        }
        for ordinal in 0..WRITE_RELATION_COLUMN_COUNT {
            let binding = layout.value_binding_at(ordinal).ok_or_else(|| {
                relation_failure(
                    WriteRelationViolation::SlotBinding,
                    "writer prefix binding is absent",
                )
            })?;
            if !plain_relation_value(binding.value_type()) {
                return Err(relation_failure(
                    WriteRelationViolation::SlotBinding,
                    "writer multiplex fixed prefix has a nominal domain",
                ));
            }
        }
        if layout.slots().len() < WRITE_RELATION_FRAGMENT_INDEX + 1 {
            return Err(relation_failure(
                WriteRelationViolation::Layout,
                "compiled writer multiplex layout lacks fixed prefix",
            ));
        }
        let auxiliary = layout
            .slots()
            .iter()
            .zip(layout.schema().fields())
            .enumerate()
            .skip(WRITE_RELATION_FRAGMENT_INDEX + 1)
            .map(|(ordinal, (slot, field))| {
                let binding = layout.value_binding_at(ordinal).ok_or_else(|| {
                    relation_failure(
                        WriteRelationViolation::SlotBinding,
                        "writer auxiliary channel lacks its complete compiled binding",
                    )
                })?;
                let value_type =
                    novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                        binding.clone(),
                    )
                    .map_err(ExecutionFailure::from_domain)?;
                WriterAuxiliaryChannel::try_new(slot.0, field.name(), value_type)
                    .map_err(relation_connector)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let contract = WriterMultiplexSchema::try_new(auxiliary).map_err(relation_connector)?;
        contract
            .validate_exact_arrow_schema(layout.schema())
            .map_err(relation_connector)?;
        Ok(Self {
            contract,
            chunk_schema: ChunkSchema::from_compiled_layout(layout)?,
        })
    }

    /// Explicit pure relation author for FE declarations and direct fixtures.
    /// Runtime decode supplies its already created schema to the strict binding
    /// constructor below; this helper grants no runtime construction capacity.
    pub fn try_new(contract: WriterMultiplexSchema) -> ExecutionResult<Self> {
        use crate::exec::chunk::ChunkSlotSchema;
        let projection = contract.schema_projection();
        let mut slots = Vec::with_capacity(contract.arrow_schema().fields().len());
        for (ordinal, slot_id) in contract.slot_ids().into_iter().enumerate() {
            let (field, storage) = projection
                .field(ordinal)
                .ok_or_else(|| {
                    relation_failure(
                        WriteRelationViolation::SlotBinding,
                        "writer field occurrence is absent",
                    )
                })?
                .into_parts();
            let value =
                if ordinal < novarocks_spi::connector::write_stack::WRITE_RELATION_COLUMN_COUNT {
                    // This is the fixed protocol purpose; the contract itself is
                    // the sole author of these four plain scalar fields.
                    novarocks_type_contract::FunctionValueType::new(
                        field.data_type().clone(),
                        field.is_nullable(),
                    )
                } else {
                    contract.auxiliary_channels()[ordinal
                        - novarocks_spi::connector::write_stack::WRITE_RELATION_COLUMN_COUNT]
                        .value_type()
                        .clone()
                };
            slots.push(ChunkSlotSchema::try_new_typed(
                SlotId::new(slot_id),
                field,
                value.typed_binding().clone(),
                None,
                storage,
            )?);
        }
        let chunk_schema = std::sync::Arc::new(ChunkSchema::try_new_typed(
            slots,
            contract.storage_lifetime(),
        )?);
        Self::try_new_with_chunk_schema(contract, chunk_schema)
    }

    /// Bind an already created complete relation without parsing annotations or
    /// replacing the actual schema/source creation holder.
    pub fn try_new_with_chunk_schema(
        contract: WriterMultiplexSchema,
        chunk_schema: ChunkSchemaRef,
    ) -> ExecutionResult<Self> {
        if !chunk_schema.is_typed() {
            return Err(relation_failure(
                WriteRelationViolation::Layout,
                "writer multiplex requires a complete typed relation",
            ));
        }
        contract
            .validate_exact_arrow_schema(chunk_schema.arrow_schema_ref().as_ref())
            .map_err(relation_connector)?;
        if (0..WRITE_RELATION_COLUMN_COUNT)
            .map(|ordinal| SlotId::new(write_relation_column_id(ordinal)))
            .chain(
                contract
                    .auxiliary_channels()
                    .iter()
                    .map(|channel| SlotId::new(channel.slot_id())),
            )
            .ne(chunk_schema.slot_ids().iter().copied())
        {
            return Err(relation_failure(
                WriteRelationViolation::SlotBinding,
                "writer multiplex slot order differs from supplied relation",
            ));
        }
        for slot in chunk_schema
            .slots()
            .iter()
            .take(WRITE_RELATION_COLUMN_COUNT)
        {
            if !plain_relation_value(slot.field_schema().value_type()) {
                return Err(relation_failure(
                    WriteRelationViolation::SlotBinding,
                    "writer multiplex fixed prefix has a nominal domain",
                ));
            }
        }
        for (slot, channel) in chunk_schema
            .slots()
            .iter()
            .skip(WRITE_RELATION_COLUMN_COUNT)
            .zip(contract.auxiliary_channels())
        {
            let actual = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                slot.typed_binding().clone(),
            )
            .map_err(ExecutionFailure::from_domain)?;
            if &actual != channel.value_type() {
                return Err(relation_failure(
                    WriteRelationViolation::SlotBinding,
                    "writer auxiliary channel differs from its complete declared binding",
                ));
            }
        }
        Ok(Self {
            contract,
            chunk_schema,
        })
    }

    pub fn empty() -> Self {
        Self::try_new(WriterMultiplexSchema::empty()).expect("fixed writer prefix")
    }

    pub const fn contract(&self) -> &WriterMultiplexSchema {
        &self.contract
    }

    pub const fn chunk_schema(&self) -> &ChunkSchemaRef {
        &self.chunk_schema
    }
}

fn plain_relation_value(value: &novarocks_type_contract::ValueTypeRef) -> bool {
    novarocks_type_contract::visit_value_type_observed(value, |visit| {
        if let novarocks_type_contract::ValueSemanticVisit::Node(value) = visit
            && value.domain().is_some()
        {
            return Err(());
        }
        Ok(())
    })
    .is_ok()
}

/// A pure frozen writer declaration. No runtime schema or SPI Field/set is
/// created until the installed operator owner enters actual preparation.
#[derive(Clone, Debug)]
pub(crate) struct WriterMultiplexRelationTemplate {
    layout: novarocks_local_program::StaticLayout,
}
impl WriterMultiplexRelationTemplate {
    pub(crate) fn try_new(layout: &novarocks_local_program::StaticLayout) -> ExecutionResult<Self> {
        if !layout.is_typed() {
            return Err(relation_failure(
                WriteRelationViolation::SlotBinding,
                "compiled writer relation requires complete bindings",
            ));
        }
        if layout.slots().len() < WRITE_RELATION_COLUMN_COUNT
            || layout.slots().len() - WRITE_RELATION_COLUMN_COUNT
                > novarocks_spi::connector::write_stack::MAX_WRITER_AUXILIARY_CHANNELS
        {
            return Err(relation_failure(
                WriteRelationViolation::Layout,
                "writer relation column count exceeds its frozen contract",
            ));
        }
        novarocks_spi::connector::write_stack::relation::validate_writer_prefix_schema(
            layout.schema().fields(),
        )
        .map_err(relation_connector)?;
        for ordinal in 0..layout.slots().len() {
            let binding = layout.value_binding_at(ordinal).ok_or_else(|| {
                relation_failure(
                    WriteRelationViolation::SlotBinding,
                    "writer relation binding is absent",
                )
            })?;
            if ordinal < WRITE_RELATION_COLUMN_COUNT {
                if binding.value_type().domain().is_some() {
                    return Err(relation_failure(
                        WriteRelationViolation::SlotBinding,
                        "writer fixed prefix has a nominal domain",
                    ));
                }
            } else if !binding.value_type().nullable()
                || *binding.layout().data_type() == arrow::datatypes::DataType::Null
            {
                return Err(relation_failure(
                    WriteRelationViolation::Layout,
                    "writer auxiliary transport must admit sparse NULL padding and a concrete value",
                ));
            }
        }
        Ok(Self {
            layout: layout.clone(),
        })
    }
    pub(crate) fn slots(&self) -> &[SlotId] {
        self.layout.slots()
    }
    pub(crate) fn prepare(
        &self,
        factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
    ) -> Result<WriterMultiplexRelationSchema, crate::exec::pipeline::operator::ConstructionFailure>
    {
        use crate::exec::chunk::ArrayNormalizationStep;
        use crate::exec::pipeline::operator::ConstructionFailure;
        use novarocks_spi::connector::write_stack::relation::WriterSchemaCreationFailure as Failure;
        use novarocks_type_contract::ValueSchemaCreation;
        use std::alloc::Layout;
        let owner = factory.begin_compiled_layout_construction(&self.layout)?;
        let schema = ChunkSchema::from_compiled_layout_owned_observed::<ConstructionFailure>(
            &self.layout,
            owner.storage_lifetime(),
            |layout| owner.request(layout),
            |units| {
                owner.observe(
                    0,
                    &[],
                    ArrayNormalizationStep::InspectConstructionWork { units },
                )
            },
        )?;
        let count = self.layout.slots().len() - WRITE_RELATION_COLUMN_COUNT;
        owner.request(Layout::array::<WriterAuxiliaryChannel>(count).map_err(|_| {
            ConstructionFailure::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?)?;
        let mut channels = Vec::new();
        channels.try_reserve_exact(count).map_err(|_| {
            ConstructionFailure::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?;
        let mut request = |layout| owner.request(layout);
        let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
        let mut work = |units| {
            owner.observe(
                0,
                &[],
                ArrayNormalizationStep::InspectConstructionWork { units },
            )
        };
        let mut creation =
            ValueSchemaCreation::new(owner.storage_lifetime(), &mut request, &mut opaque);
        let map = |error| {
            match error {
            Failure::Creator(error)=>error,
            Failure::Control(error)=>ConstructionFailure::Control(error),
            Failure::Layout(error)=>ConstructionFailure::Resource(error),
            Failure::HashMap(error)=>ConstructionFailure::Resource(match error {
                novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::Arithmetic(_)=>novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
                novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::SourceModel(_)=>novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
            }),
            Failure::Contract(error)=>ConstructionFailure::Source(crate::runtime::task::SharedDomainFailure::new(error)),
        }
        };
        for slot in schema.slots().iter().skip(WRITE_RELATION_COLUMN_COUNT) {
            let value = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                slot.typed_binding().clone(),
            )
            .map_err(|error| {
                ConstructionFailure::Source(crate::runtime::task::SharedDomainFailure::new(error))
            })?;
            channels.push(
                WriterAuxiliaryChannel::try_new_with_creator_work_in(
                    slot.slot_id().0,
                    slot.field().name(),
                    value,
                    Some(&mut creation),
                    &mut work,
                )
                .map_err(&map)?,
            );
        }
        let contract = WriterMultiplexSchema::try_new_with_creator_work_in(
            channels,
            Some(&mut creation),
            &mut work,
        )
        .map_err(map)?;
        contract
            .validate_exact_arrow_schema(&schema.arrow_schema_ref())
            .map_err(|error| {
                ConstructionFailure::Source(crate::runtime::task::SharedDomainFailure::new(error))
            })?;
        if contract
            .auxiliary_channels()
            .iter()
            .map(|channel| SlotId::new(channel.slot_id()))
            .ne(schema
                .slot_ids()
                .iter()
                .copied()
                .skip(WRITE_RELATION_COLUMN_COUNT))
        {
            return Err(ConstructionFailure::InvalidSource(
                "compiled writer auxiliary slot order differs".into(),
            ));
        }
        // Both objects were actually created in this scope. Existing layout
        // bindings retain their own independent original backing responsibility.
        Ok(WriterMultiplexRelationSchema {
            contract,
            chunk_schema: schema,
        })
    }
}
#[derive(Clone, Debug)]
pub(crate) enum WriterRelationPreparation {
    Compiled {
        template: WriterMultiplexRelationTemplate,
        prepared: Option<WriterMultiplexRelationSchema>,
    },
}
impl WriterRelationPreparation {
    pub(crate) fn compiled(template: WriterMultiplexRelationTemplate) -> Self {
        Self::Compiled {
            template,
            prepared: None,
        }
    }
    pub(crate) fn is_compiled(&self) -> bool {
        matches!(self, Self::Compiled { .. })
    }
    pub(crate) fn slots(&self) -> &[SlotId] {
        match self {
            Self::Compiled { template, .. } => template.slots(),
        }
    }
    pub(crate) fn schema(&self) -> ExecutionResult<&WriterMultiplexRelationSchema> {
        match self {
            Self::Compiled {
                prepared: Some(schema),
                ..
            } => Ok(schema),
            Self::Compiled { prepared: None, .. } => Err(relation_failure(
                WriteRelationViolation::NotPrepared,
                "writer relation has not been prepared by its construction owner",
            )),
        }
    }
    pub(crate) fn prepare(
        &mut self,
        factory: Option<
            &std::sync::Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
        >,
    ) -> Result<(), crate::runtime::task::ExecutionFailure> {
        if let Self::Compiled { template, prepared } = self
            && prepared.is_none()
        {
            let factory = factory
                .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
            let schema = template.prepare(factory.as_ref())?;
            *prepared = Some(schema);
        }
        Ok(())
    }
}

/// Pure compiled Root declaration. The runtime schema is created only by
/// TableFinish preparation after its actual construction factory is installed.
#[derive(Clone, Debug)]
pub(crate) struct RootWriteResultRelationTemplate {
    layout: novarocks_local_program::StaticLayout,
}

impl RootWriteResultRelationTemplate {
    pub(crate) fn try_new(layout: &novarocks_local_program::StaticLayout) -> ExecutionResult<Self> {
        if !layout.is_typed() {
            return Err(relation_failure(
                WriteRelationViolation::Layout,
                "compiled root write result requires a complete typed layout",
            ));
        }
        novarocks_spi::connector::write_stack::relation::validate_root_write_result_schema(
            layout.schema(),
        )
        .map_err(relation_connector)?;
        for ordinal in 0..layout.slots().len() {
            let binding = layout.value_binding_at(ordinal).ok_or_else(|| {
                relation_failure(
                    WriteRelationViolation::SlotBinding,
                    "root write result complete binding is absent",
                )
            })?;
            if !plain_relation_value(binding.value_type()) {
                return Err(relation_failure(
                    WriteRelationViolation::SlotBinding,
                    "root write result fixed purpose has a nominal domain",
                ));
            }
        }
        Ok(Self {
            layout: layout.clone(),
        })
    }

    pub(crate) fn layout(&self) -> &novarocks_local_program::StaticLayout {
        &self.layout
    }

    /// Consume the prepared runtime schema without allocating another schema.
    /// Compiler slot IDs are positional, rather than the SPI's default IDs.
    pub(crate) fn bind_created_schema(
        &self,
        schema: ChunkSchemaRef,
    ) -> ExecutionResult<RootWriteResultRelationSchema> {
        if !schema.is_typed()
            || schema.arrow_schema_ref().as_ref() != self.layout.schema().as_ref()
            || schema.slot_ids() != self.layout.slots()
        {
            return Err(relation_failure(
                WriteRelationViolation::Layout,
                "prepared root write result differs from its compiled template",
            ));
        }
        for (ordinal, slot) in schema.slots().iter().enumerate() {
            if self.layout.value_binding_at(ordinal) != Some(slot.typed_binding()) {
                return Err(relation_failure(
                    WriteRelationViolation::SlotBinding,
                    "prepared root write result binding differs from its compiled template",
                ));
            }
        }
        Ok(RootWriteResultRelationSchema {
            contract: RootWriteResultSchema::new(),
            chunk_schema: schema,
        })
    }
}

/// Execution-local slot binding of the fixed Root write result relation.
#[derive(Clone, Debug)]
pub struct RootWriteResultRelationSchema {
    contract: RootWriteResultSchema,
    chunk_schema: ChunkSchemaRef,
}

impl RootWriteResultRelationSchema {
    /// Explicit legacy direct-fixture adapter. Production compiled preparation
    /// uses RootWriteResultRelationTemplate and the installed creating owner.
    #[cfg(test)]
    pub(crate) fn try_from_compiled_layout(
        layout: &novarocks_local_program::StaticLayout,
    ) -> ExecutionResult<Self> {
        let contract = RootWriteResultSchema::new();
        contract
            .validate_exact_arrow_schema(layout.schema())
            .map_err(relation_connector)?;
        Ok(Self {
            contract,
            chunk_schema: ChunkSchema::from_compiled_layout(layout)?,
        })
    }

    pub fn try_new(contract: RootWriteResultSchema) -> ExecutionResult<Self> {
        let slot_ids = contract.slot_ids().map(SlotId::new);
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            contract.arrow_schema().as_ref(),
            &slot_ids,
        )
        .map_err(ExecutionFailure::from)?;
        debug_assert_eq!(slot_ids.len(), ROOT_WRITE_RESULT_COLUMN_COUNT);
        Self::try_new_with_chunk_schema(contract, chunk_schema)
    }

    /// Bind an already constructed relation without replacing its immutable
    /// schema or field metadata owners. Equality validates the contract only;
    /// the supplied ChunkSchema remains the sole provenance source.
    pub fn try_new_with_chunk_schema(
        contract: RootWriteResultSchema,
        chunk_schema: ChunkSchemaRef,
    ) -> ExecutionResult<Self> {
        if chunk_schema.is_typed()
            && chunk_schema
                .slots()
                .iter()
                .any(|slot| !plain_relation_value(slot.field_schema().value_type()))
        {
            return Err(relation_failure(
                WriteRelationViolation::SlotBinding,
                "root write result fixed relation has a nominal domain",
            ));
        }
        contract
            .validate_exact_arrow_schema(chunk_schema.arrow_schema_ref().as_ref())
            .map_err(relation_connector)?;
        if contract.slot_ids().map(SlotId::new).as_slice() != chunk_schema.slot_ids() {
            return Err(relation_failure(
                WriteRelationViolation::SlotBinding,
                "root write result slot order differs from supplied chunk schema",
            ));
        }
        Ok(Self {
            contract,
            chunk_schema,
        })
    }

    pub fn fixed() -> Self {
        Self::try_new(RootWriteResultSchema::new()).expect("fixed root write result")
    }

    pub const fn contract(&self) -> &RootWriteResultSchema {
        &self.contract
    }

    pub const fn chunk_schema(&self) -> &ChunkSchemaRef {
        &self.chunk_schema
    }
}

/// Canonical commit-fragment egress port.
///
/// `TableWriter` hands one finished provider fragment to its owner and receives
/// the canonical carrier bytes back. The execution layer never performs that
/// encoding itself: it cannot reach the generated wire crates, so the backend
/// installs the implementation for the exact catalog generation this query
/// froze.
pub trait ConnectorCommitFragmentEncoder: Send + Sync {
    /// Encode one commit fragment produced for `target` into its canonical
    /// carrier bytes.
    fn encode(
        &self,
        target: WriteTargetOrdinal,
        fragment: &ConnectorCommitFragment,
    ) -> Result<Vec<u8>, ConnectorError>;
}

/// Canonical commit-fragment ingress port.
///
/// `TableFinish` receives opaque canonical bytes from many senders. It must
/// reject a foreign, truncated, or non-canonical carrier before it enters the
/// prepared write set, but it must not decode one into a provider domain
/// object: that happens only in the frontend, on the provider's own control
/// binding. The implementation therefore performs a structural check and
/// nothing more.
pub trait ConnectorCommitFragmentCarrierValidator: Send + Sync {
    /// Structurally verify that `encoded` is a canonical, in-bounds commit
    /// fragment carrier of the provider expected for `target`, without
    /// decoding it into a provider value.
    fn validate(&self, target: WriteTargetOrdinal, encoded: &[u8]) -> Result<(), ConnectorError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_binds_the_spi_relations_without_redefining_them() {
        let writer = WriterMultiplexRelationSchema::empty();
        assert_eq!(
            writer.contract().arrow_schema(),
            novarocks_spi::connector::write_stack::writer_output_schema().as_ref()
        );
        let root = RootWriteResultRelationSchema::fixed();
        assert_eq!(
            root.contract().arrow_schema().as_ref(),
            RootWriteResultSchema::new().arrow_schema().as_ref()
        );
    }

    #[test]
    fn every_relation_column_carries_its_slot_id_at_the_spi_column_index() {
        let writer = WriterMultiplexRelationSchema::empty();
        for (index, slot_id) in writer.contract().slot_ids().into_iter().enumerate() {
            assert_eq!(
                writer.chunk_schema().index_of(SlotId::new(slot_id)),
                Some(index)
            );
        }
        let root = RootWriteResultRelationSchema::fixed();
        for (index, slot_id) in root.contract().slot_ids().into_iter().enumerate() {
            assert_eq!(
                root.chunk_schema().index_of(SlotId::new(slot_id)),
                Some(index)
            );
        }
    }

    /// A compiled layout carries the exact SPI relation under its own slots;
    /// any drift of names, nesting or nullability is refused.
    #[test]
    fn compiled_layouts_bind_the_exact_relations_under_their_own_slots() {
        use std::sync::Arc;

        use arrow::datatypes::{DataType, Field, Schema};
        use novarocks_local_program::StaticLayout;
        use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};

        // Independent fixed SPI purposes; a representation Field is not the
        // semantic author. This pure declaration fixture claims no BE grant.
        let writer_types = vec![
            FunctionValueType::new(DataType::Int8, false),
            FunctionValueType::new(DataType::Int32, false),
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Binary, true),
        ];
        let root_types = vec![
            FunctionValueType::new(DataType::Int8, false),
            FunctionValueType::new(DataType::Int32, true),
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Binary, true),
            FunctionValueType::new(
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                true,
            ),
            FunctionValueType::new(DataType::Utf8, true),
            FunctionValueType::new(DataType::Binary, true),
            FunctionValueType::new(
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Arc::new(Field::new("key", DataType::Utf8, false)),
                                Arc::new(Field::new("value", DataType::Utf8, false)),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
            ),
        ];
        let layout = |schema: arrow::datatypes::SchemaRef,
                      declarations: &[FunctionValueType],
                      first: u32| {
            assert_eq!(schema.fields().len(), declarations.len());
            let slots = (0..declarations.len() as u32)
                .map(|ordinal| SlotId::new(first + ordinal))
                .collect::<Vec<_>>();
            StaticLayout::try_new_typed_for_compile(
                schema,
                Arc::from(slots),
                declarations
                    .iter()
                    .map(|value| (value.typed_binding().clone(), None))
                    .collect(),
                ValueTypeStorageLifetime::pure(),
                |_| Ok(()),
                &crate::exec::expr::test_support::HarnessControl,
            )
        };
        let writer_layout = layout(
            novarocks_spi::connector::write_stack::writer_output_schema(),
            &writer_types,
            3,
        )
        .expect("complete writer layout");
        let writer = WriterMultiplexRelationSchema::try_from_compiled_layout(&writer_layout)
            .expect("writer relation");
        assert_eq!(
            writer.chunk_schema().slot_ids(),
            [3, 4, 5, 6].map(SlotId::new).as_slice()
        );
        assert!(writer.contract().auxiliary_channels().is_empty());
        let root_layout = layout(RootWriteResultSchema::new().arrow_schema(), &root_types, 20)
            .expect("complete root layout");
        let root = RootWriteResultRelationSchema::try_from_compiled_layout(&root_layout)
            .expect("root relation");
        assert_eq!(root.chunk_schema().slot_ids()[0], SlotId::new(20));
        assert_eq!(
            root.chunk_schema().arrow_schema_ref().as_ref(),
            RootWriteResultSchema::new().arrow_schema().as_ref()
        );
        for (relation, declarations) in [
            (writer.chunk_schema(), &writer_types),
            (root.chunk_schema(), &root_types),
        ] {
            assert!(relation.is_typed());
            for (slot, declared) in relation.slots().iter().zip(declarations) {
                assert_eq!(slot.typed_binding(), declared.typed_binding());
            }
        }
        // A noncanonical list role remains refused at the typed author gate;
        // no invalid layout is fabricated to reach the downstream relation.
        let mut drifted = RootWriteResultSchema::new()
            .arrow_schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        drifted[4] = Field::new(
            "input_fields",
            DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
            true,
        );
        assert!(layout(Arc::new(Schema::new(drifted)), &root_types, 20).is_err());
        // A legal complete but nullable kind is still a different writer
        // relation. It reaches the real SPI validator with its own promise.
        let mut nullable_kind = novarocks_spi::connector::write_stack::writer_output_schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        nullable_kind[0] = nullable_kind[0].clone().with_nullable(true);
        let mut nullable_types = writer_types.clone();
        nullable_types[0] = FunctionValueType::new(DataType::Int8, true);
        let nullable_layout = layout(Arc::new(Schema::new(nullable_kind)), &nullable_types, 3)
            .expect("legal nullable source declaration");
        assert!(WriterMultiplexRelationSchema::try_from_compiled_layout(&nullable_layout).is_err());
    }

    #[test]
    fn typed_relation_carriers_bind_every_generic_slot() {
        let writer = WriterMultiplexRelationSchema::try_new(
            WriterMultiplexSchema::try_new(vec![
                novarocks_spi::connector::write_stack::WriterAuxiliaryChannel::try_new(
                    7,
                    "opaque_aux",
                    novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Struct(arrow::datatypes::Fields::from(vec![
                            arrow::datatypes::Field::new(
                                "v",
                                arrow::datatypes::DataType::Utf8,
                                true,
                            ),
                        ])),
                        true,
                    ),
                )
                .expect("channel"),
            ])
            .expect("contract"),
        )
        .expect("execution schema");
        assert_eq!(writer.chunk_schema().slots().len(), 5);
        assert_eq!(writer.chunk_schema().index_of(SlotId::new(7)), Some(4));

        let root = RootWriteResultRelationSchema::fixed();
        assert_eq!(
            root.chunk_schema().slots().len(),
            ROOT_WRITE_RESULT_COLUMN_COUNT
        );
    }
}

#[cfg(test)]
mod writer_template_creator_tests {
    use super::*;
    use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
    use crate::exec::pipeline::pipeline::representation_construction_profile;
    use crate::runtime::mem_tracker::MemTracker;
    fn genuine_template() -> (
        WriterMultiplexRelationTemplate,
        crate::runtime::runtime_state::RuntimeState,
    ) {
        let (program, _binding, state) =
            crate::exec::pipeline::builder::compiled_statistics_finish_fixture();
        let node = program
            .graph()
            .nodes()
            .iter()
            .find(|node| {
                matches!(
                    node.kind(),
                    novarocks_local_program::ProgramNodeKind::TableFinish { .. }
                )
            })
            .expect("genuine compiled finish");
        let novarocks_local_program::ProgramNodeKind::TableFinish {
            writer_multiplex_layout,
            ..
        } = node.kind()
        else {
            unreachable!()
        };
        let template = WriterMultiplexRelationTemplate::try_new(writer_multiplex_layout).unwrap();
        drop(program);
        (template, state)
    }
    #[test]
    fn actual_compiled_writer_template_creates_schema_only_after_owner_and_keeps_last_projection() {
        let (template, state) = genuine_template();
        let tracker = MemTracker::new_root("compiled writer relation last field");
        let factory = try_runtime_input_construction_factory(
            representation_construction_profile().unwrap(),
            tracker.clone(),
            state.error_state(),
        )
        .unwrap();
        let mut relation = WriterRelationPreparation::compiled(template);
        assert!(relation.schema().is_err());
        relation.prepare(Some(&factory)).unwrap();
        let schema = relation.schema().unwrap();
        assert!(schema.chunk_schema().is_typed());
        let projection = schema.contract().schema_projection();
        let late = projection.field(0).unwrap();
        assert!(tracker.current() > 0);
        drop(relation);
        drop(projection);
        drop(factory);
        drop(state);
        assert!(
            tracker.current() > 0,
            "independent Field retains the actual construction guard"
        );
        assert_eq!(
            late.field().name(),
            novarocks_spi::connector::write_stack::WRITE_RELATION_KIND_COLUMN
        );
        drop(late);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn actual_compiled_writer_template_first_limit_refusal_never_publishes_schema() {
        let (template, state) = genuine_template();
        let tracker = MemTracker::new_root("compiled writer relation first refusal");
        let factory = try_runtime_input_construction_factory(
            representation_construction_profile().unwrap(),
            tracker.clone(),
            state.error_state(),
        )
        .unwrap();
        let installed = tracker.current();
        tracker.install_limit_once(installed + 1).unwrap();
        let mut relation = WriterRelationPreparation::compiled(template);
        let error = relation.prepare(Some(&factory)).unwrap_err();
        assert!(matches!(
            error.cause(),
            crate::runtime::task::ExecutionFailureCause::Construction(
                crate::exec::pipeline::operator::ConstructionFailure::Memory(_)
            )
        ));
        assert!(relation.schema().is_err());
        assert_eq!(
            tracker.current(),
            installed,
            "failed independent scope request rolls back its prospective charge"
        );
        drop(relation);
        drop(factory);
        drop(state);
        assert_eq!(tracker.current(), 0);
    }
}
