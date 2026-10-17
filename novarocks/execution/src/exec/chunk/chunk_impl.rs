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
use std::alloc::Layout;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[cfg(test)]
use arrow::array::DictionaryArray;
#[cfg(test)]
use arrow::array::Int32Array;
#[cfg(test)]
use arrow::array::ListArray;
#[cfg(test)]
use arrow::array::StructArray;
use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions};
#[cfg(test)]
use arrow::datatypes::DataType;
#[cfg(test)]
use arrow::datatypes::Int32Type;
use arrow::datatypes::{Schema, SchemaRef};

use crate::runtime::mem_tracker::MemTracker;
use novarocks_spi::connector::ConnectorOutputMemoryToken;
use novarocks_type_contract::ValueTypeStorageLifetime;
use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
use novarocks_types::SlotId;

use super::memory::{ChunkAccounting, ChunkMemoryLease, chunk_bytes_i64, record_batch_bytes};
use super::schema::execution_carrier_matches_layout_observed;
use super::schema::{
    ChunkSchema, ChunkSchemaRef, align_chunk_schema_to_batch, align_chunk_schema_to_columns,
};
use super::type_compatibility::retag_column;
use super::typed_array_normalization::{
    ArrayNormalizationStep, normalize_execution_array_observed,
};

#[derive(Clone)]
enum ChunkAccountingHandle {
    WholeBatch(Arc<ChunkAccounting>),
    AdditionalBuffers(super::AdditionalBufferLedger),
}
impl std::fmt::Debug for ChunkAccountingHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WholeBatch(owner) => std::fmt::Debug::fmt(owner, f),
            Self::AdditionalBuffers(owner) => std::fmt::Debug::fmt(owner, f),
        }
    }
}

/// A chunk of data, consisting of multiple rows.
/// Phase 2: Wrapper around Arrow RecordBatch.
#[derive(Debug, Clone)]
pub struct Chunk {
    batch: Arc<RecordBatch>,
    chunk_schema: ChunkSchemaRef,
    accounting: Option<ChunkAccountingHandle>,
    /// A provider reservation that already charges the Arrow buffers in this
    /// chunk to the admitted fragment hierarchy.
    ///
    /// The mutex makes the opaque SPI lease shareable with zero-copy `Chunk`
    /// clones. While it is present, `transfer_to` must not create a second
    /// charge for the same buffers. The last chunk owner releases the original
    /// provider charge.
    connector_output_memory: Option<Arc<Mutex<ConnectorOutputMemoryToken>>>,
    // Actual array type backing may be shared by later slices. The current
    // wrapper hold can then exit independently of the shared backing hold.
    array_type_storage: ValueTypeStorageLifetime,
    construction_storage: ValueTypeStorageLifetime,
    output_column_metadata: Option<super::OutputColumnMetadata>,
}

/// Owned Arrow access paired with its complete immutable execution binding
/// and actual creation/buffer lineage. It exposes no naked Arc<RecordBatch>.
#[derive(Clone, Debug)]
pub struct ChunkBatchProjection {
    batch: Arc<RecordBatch>,
    chunk_schema: ChunkSchemaRef,
    accounting: Option<ChunkAccountingHandle>,
    connector_output_memory: Option<Arc<Mutex<ConnectorOutputMemoryToken>>>,
    array_type_storage: ValueTypeStorageLifetime,
    construction_storage: ValueTypeStorageLifetime,
    output_column_metadata: Option<super::OutputColumnMetadata>,
}

// Shared source custody contains only existing buffer charges, exact schema
// references and finite creation holds. It carries no query/context capability.
struct ChunkSourceStorageHold {
    schema: ChunkSchemaRef,
    accounting: Option<ChunkAccountingHandle>,
    connector_output_memory: Option<Arc<Mutex<ConnectorOutputMemoryToken>>>,
    array_type_storage: ValueTypeStorageLifetime,
    construction_storage: ValueTypeStorageLifetime,
    storage: ValueTypeStorageLifetime,
    output_column_metadata: Option<super::OutputColumnMetadata>,
}
impl novarocks_type_contract::ValueTypeStorageHold for ChunkSourceStorageHold {}

// Stack-only construction inputs keep actual storage ahead of its creating
// hold in drop order, including validation and original callback refusals.
struct TypedChunkColumns {
    columns: Vec<ArrayRef>,
    storage: ValueTypeStorageLifetime,
}
struct TypedRecordBatchSource {
    batch: RecordBatch,
    storage: ValueTypeStorageLifetime,
}
impl ChunkBatchProjection {
    pub fn record_batch(&self) -> &RecordBatch {
        self.batch.as_ref()
    }
    pub fn columns(&self) -> &[ArrayRef] {
        self.batch.columns()
    }
    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn chunk_schema(&self) -> &ChunkSchema {
        self.chunk_schema.as_ref()
    }
    pub fn chunk_schema_ref(&self) -> ChunkSchemaRef {
        self.chunk_schema.clone()
    }
    pub(crate) fn array_type_storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.array_type_storage.clone()
    }
    pub fn into_chunk(self) -> Chunk {
        Chunk {
            batch: self.batch,
            chunk_schema: self.chunk_schema,
            accounting: self.accounting,
            connector_output_memory: self.connector_output_memory,
            array_type_storage: self.array_type_storage,
            construction_storage: self.construction_storage,
            output_column_metadata: self.output_column_metadata,
        }
    }
}

impl Chunk {
    pub(crate) fn source_authority(&self) -> super::ChunkSourceAuthorityRef<'_> {
        match self.accounting.as_ref() {
            Some(ChunkAccountingHandle::AdditionalBuffers(ledger)) => ledger.source_authority(),
            Some(ChunkAccountingHandle::WholeBatch(owner)) => {
                super::ChunkSourceAuthorityRef::from_attached(
                    Some(owner),
                    self.connector_output_memory.as_ref(),
                )
            }
            None => super::ChunkSourceAuthorityRef::from_attached(
                None,
                self.connector_output_memory.as_ref(),
            ),
        }
    }

    /// Transfer existing finite buffer/type responsibility into an independent
    /// neutral owner. The caller observes the real shared holder before it is
    /// allocated, and its lifetime covers that holder's own new construction.
    pub fn source_storage_lifetime_observed<E: From<LayoutResourceError>>(
        &self,
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
    ) -> Result<ValueTypeStorageLifetime, E> {
        if self.accounting.is_none()
            && self.connector_output_memory.is_none()
            && self.output_column_metadata.is_none()
        {
            let storage = storage.retaining_observed(&self.array_type_storage, &mut request)?;
            let storage = storage.retaining_observed(&self.construction_storage, &mut request)?;
            return storage.retaining_observed(&self.chunk_schema.storage_lifetime(), request);
        }
        request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<ChunkSourceStorageHold>(),
            )?,
        )?;
        Ok(ValueTypeStorageLifetime::held(Arc::new(
            ChunkSourceStorageHold {
                schema: self.chunk_schema.clone(),
                accounting: self.accounting.clone(),
                connector_output_memory: self.connector_output_memory.clone(),
                array_type_storage: self.array_type_storage.clone(),
                construction_storage: self.construction_storage.clone(),
                output_column_metadata: self.output_column_metadata.clone(),
                storage,
            },
        )))
    }

    pub fn record_batch(&self) -> &RecordBatch {
        self.batch.as_ref()
    }

    /// A guarded immutable projection shares the batch and every original
    /// creator/buffer holder. No new column Vec or entire source Chunk is kept.
    pub fn batch_projection(&self) -> ChunkBatchProjection {
        ChunkBatchProjection {
            batch: self.batch.clone(),
            chunk_schema: self.chunk_schema.clone(),
            accounting: self.accounting.clone(),
            connector_output_memory: self.connector_output_memory.clone(),
            array_type_storage: self.array_type_storage.clone(),
            construction_storage: self.construction_storage.clone(),
            output_column_metadata: self.output_column_metadata.clone(),
        }
    }
    pub fn into_batch_projection(self) -> ChunkBatchProjection {
        ChunkBatchProjection {
            batch: self.batch,
            chunk_schema: self.chunk_schema,
            accounting: self.accounting,
            connector_output_memory: self.connector_output_memory,
            array_type_storage: self.array_type_storage,
            construction_storage: self.construction_storage,
            output_column_metadata: self.output_column_metadata,
        }
    }
    #[cfg(test)]
    pub(crate) fn replace_record_batch_for_test(&mut self, batch: RecordBatch) {
        self.batch = Arc::new(batch);
    }
    /// Retain only the original shared buffer funding handles. Newly decoded
    /// buffers are covered by the hydration caller's separate creation hold.
    /// This carries neither an entire source chunk nor a query/context owner.
    pub(super) fn retain_source_buffer_funding(&mut self, source: &Self) {
        self.accounting = source.accounting.clone();
        self.connector_output_memory = source.connector_output_memory.clone();
        self.output_column_metadata = source.output_column_metadata.clone();
    }

    /// Pure expansion/test adapter for explicitly authored typed occurrences.
    /// Runtime callers use the owned observed constructor with an admitted hold.
    /// `row_count` also carries rows for a zero-channel batch. The original
    /// buffer owner is retained by the caller's existing transfer path.
    pub fn try_new_typed_with_columns_observed<E, F>(
        chunk_schema: ChunkSchemaRef,
        columns: Vec<ArrayRef>,
        row_count: usize,
        observe: F,
    ) -> Result<Self, E>
    where
        E: From<String>,
        F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
    {
        Self::try_new_typed_with_columns_owned_observed(
            chunk_schema,
            columns,
            row_count,
            ValueTypeStorageLifetime::pure(),
            observe,
        )
    }

    /// Runtime construction requires the creating owner's already admitted
    /// hold. The observer controls every actual normalization construction.
    pub fn try_new_typed_with_columns_owned_observed<E, F>(
        chunk_schema: ChunkSchemaRef,
        columns: Vec<ArrayRef>,
        row_count: usize,
        storage: ValueTypeStorageLifetime,
        mut observe: F,
    ) -> Result<Self, E>
    where
        E: From<String>,
        F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
    {
        let mut source = TypedChunkColumns { columns, storage };
        if !chunk_schema.is_typed() || source.columns.len() != chunk_schema.slots().len() {
            return Err(E::from(
                "typed chunk requires its exact typed channel contract".into(),
            ));
        }
        for (index, column) in source.columns.iter_mut().enumerate() {
            let slot = &chunk_schema.slots()[index];
            if column.len() != row_count {
                return Err(E::from(format!(
                    "typed chunk channel {index} has a different row count"
                )));
            }
            if !execution_carrier_matches_layout_observed(
                slot.typed_binding().data_type(),
                column.data_type(),
                &mut |units| observe(ArrayNormalizationStep::InspectConstructionWork { units }),
            )? {
                return Err(E::from(format!(
                    "typed chunk channel {index} carrier does not match its explicit value layout"
                )));
            }
            let normalized = normalize_execution_array_observed(column, &mut observe)?;
            if normalized.data_type() != slot.data_type()
                || !execution_carrier_matches_layout_observed(
                    slot.typed_binding().data_type(),
                    normalized.data_type(),
                    &mut |units| observe(ArrayNormalizationStep::InspectConstructionWork { units }),
                )?
            {
                return Err(E::from(format!(
                    "typed chunk channel {index} carrier does not match its explicit binding"
                )));
            }
            let root_has_nulls = validate_encoded_keys_and_root_nulls_observed(
                &normalized,
                &mut observe,
                1,
                &mut 0,
            )?;
            if !slot.nullable() && root_has_nulls {
                return Err(E::from(format!(
                    "typed chunk required channel {index} contains nulls"
                )));
            }
            super::required_dictionary_nulls::validate_required_dictionary_children(
                &normalized,
                slot.typed_binding().value_type(),
                &mut observe,
            )?;
            *column = normalized;
        }
        let batch = build_record_batch(chunk_schema.arrow_schema_ref(), source.columns, row_count)
            .map_err(E::from)?;
        observe(ArrayNormalizationStep::CreateRecordBatchWrapper)?;
        Ok(Self {
            batch: Arc::new(batch),
            chunk_schema,
            accounting: None,
            connector_output_memory: None,
            array_type_storage: source.storage.clone(),
            construction_storage: source.storage,
            output_column_metadata: None,
        })
    }

    /// Consume an already created batch without cloning its column vector.
    /// The original owner covers the transferred Vec and array/type backing;
    /// the construction owner covers only newly normalized wrappers and the
    /// new shared RecordBatch holder. Buffer funding tokens are transferred
    /// independently through the caller's existing source ownership path.
    pub fn try_new_typed_record_batch_owned_observed<E, F, R>(
        batch: RecordBatch,
        chunk_schema: ChunkSchemaRef,
        original_array_storage: ValueTypeStorageLifetime,
        construction_storage: ValueTypeStorageLifetime,
        mut request: R,
        mut observe: F,
    ) -> Result<Self, E>
    where
        E: From<String> + From<LayoutResourceError>,
        F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
        R: FnMut(Layout) -> Result<(), E>,
    {
        let source = TypedRecordBatchSource {
            batch,
            storage: original_array_storage,
        };
        if !chunk_schema.is_typed() || source.batch.num_columns() != chunk_schema.slots().len() {
            return Err(E::from(
                "typed chunk requires its exact typed channel contract".to_owned(),
            ));
        }
        for (index, (field, slot)) in source
            .batch
            .schema_ref()
            .fields()
            .iter()
            .zip(chunk_schema.slots())
            .enumerate()
        {
            let units = field
                .name()
                .len()
                .checked_add(slot.name().len())
                .and_then(|units| units.checked_add(1))
                .ok_or_else(|| E::from(LayoutResourceError::Arithmetic))?;
            observe(ArrayNormalizationStep::InspectConstructionWork { units })?;
            if field.name() != slot.name() || field.is_nullable() != slot.nullable() {
                return Err(E::from(format!(
                    "typed chunk batch occurrence {index} differs from its explicit label or nullability"
                )));
            }
            if !execution_carrier_matches_layout_observed(
                slot.typed_binding().data_type(),
                field.data_type(),
                &mut |units| observe(ArrayNormalizationStep::InspectConstructionWork { units }),
            )? {
                return Err(E::from(format!(
                    "typed chunk batch occurrence {index} differs from its explicit value layout"
                )));
            }
        }
        // The callback runs before the only possible extra shared hold. It is
        // skipped when the actual original and new owner are already identical.
        let array_storage =
            construction_storage.retaining_observed(&source.storage, &mut request)?;
        let (_source_schema, columns, row_count) = source.batch.into_parts();
        let mut result = Self::try_new_typed_with_columns_owned_observed(
            chunk_schema,
            columns,
            row_count,
            construction_storage,
            observe,
        )?;
        result.array_type_storage = array_storage;
        Ok(result)
    }

    pub fn try_new_with_columns(
        chunk_schema: ChunkSchemaRef,
        columns: Vec<ArrayRef>,
    ) -> Result<Self, String> {
        let row_count = columns.first().map(|col| col.len()).unwrap_or(0);
        if chunk_schema.is_typed() {
            return Self::try_new_typed_with_columns_observed(
                chunk_schema,
                columns,
                row_count,
                |_| Ok(()),
            );
        }
        let chunk_schema = align_chunk_schema_to_columns(&columns, chunk_schema.as_ref())?;
        let columns = retag_columns_to_chunk_schema(&columns, chunk_schema.as_ref())?;
        let batch = build_record_batch(chunk_schema.arrow_schema_ref(), columns, row_count)?;
        Ok(Self {
            batch: Arc::new(batch),
            chunk_schema,
            accounting: None,
            connector_output_memory: None,
            array_type_storage: ValueTypeStorageLifetime::pure(),
            construction_storage: ValueTypeStorageLifetime::pure(),
            output_column_metadata: None,
        })
    }

    pub fn try_new_like(batch: RecordBatch, source: &Chunk) -> Result<Self, String> {
        let mut result = Self::try_new_with_chunk_schema(batch, source.chunk_schema_ref())?;
        result.array_type_storage = source.array_type_storage.clone();
        result.output_column_metadata = source.output_column_metadata.clone();
        Ok(result)
    }

    pub fn new_like(batch: RecordBatch, source: &Chunk) -> Self {
        match Self::try_new_like(batch, source) {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        }
    }

    pub fn try_new_with_chunk_schema(
        batch: RecordBatch,
        chunk_schema: ChunkSchemaRef,
    ) -> Result<Self, String> {
        if chunk_schema.is_typed() {
            if batch.num_columns() != chunk_schema.slots().len()
                || batch
                    .schema()
                    .fields()
                    .iter()
                    .zip(chunk_schema.slots())
                    .any(|(field, slot)| field.name() != slot.name())
            {
                return Err(
                    "typed chunk batch occurrence labels differ from its explicit contract".into(),
                );
            }
            let (_source_schema, columns, row_count) = batch.into_parts();
            return Self::try_new_typed_with_columns_observed(
                chunk_schema,
                columns,
                row_count,
                |_| Ok(()),
            );
        }
        let arrow_schema = chunk_schema.arrow_schema_ref();
        if batch.schema().as_ref() == arrow_schema.as_ref() {
            return Ok(Self {
                batch: Arc::new(batch),
                chunk_schema,
                accounting: None,
                connector_output_memory: None,
                array_type_storage: ValueTypeStorageLifetime::pure(),
                construction_storage: ValueTypeStorageLifetime::pure(),
                output_column_metadata: None,
            });
        }
        let row_count = batch.num_rows();
        let chunk_schema = align_chunk_schema_to_batch(&batch, chunk_schema.as_ref())?;
        let columns = retag_columns_to_chunk_schema(batch.columns(), chunk_schema.as_ref())?;
        let batch = build_record_batch(chunk_schema.arrow_schema_ref(), columns, row_count)?;
        Ok(Self {
            batch: Arc::new(batch),
            chunk_schema,
            accounting: None,
            connector_output_memory: None,
            array_type_storage: ValueTypeStorageLifetime::pure(),
            construction_storage: ValueTypeStorageLifetime::pure(),
            output_column_metadata: None,
        })
    }

    /// Preserve the already paid original column Vec while the actual runtime
    /// creator owns normalization and the new shared RecordBatch wrapper.
    pub(crate) fn try_new_with_output_column_metadata(
        batch: RecordBatch,
        chunk_schema: ChunkSchemaRef,
        metadata: super::OutputColumnMetadata,
        owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    ) -> crate::runtime::fragment::ExecutionResult<Self> {
        struct UnpublishedColumns {
            batch: RecordBatch,
            metadata: super::OutputColumnMetadata,
        }
        let source = UnpublishedColumns { batch, metadata };
        if source.batch.num_columns() != source.metadata.columns() {
            return Err(novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "output column metadata requires the original frozen column container",
                ),
            )
            .into());
        }
        let mut output = Self::try_new_typed_record_batch_owned_observed::<
            crate::exec::pipeline::operator::ConstructionFailure,
            _,
            _,
        >(
            source.batch,
            chunk_schema,
            owner.storage_lifetime(),
            owner.storage_lifetime(),
            |layout| owner.request(layout),
            |step| owner.observe(0, &[], step),
        )?;
        output.output_column_metadata = Some(source.metadata);
        Ok(output)
    }

    #[cfg(test)]
    pub(crate) fn try_into_static_batch(self) -> Result<RecordBatch, String> {
        if self.output_column_metadata.is_some() {
            return Err("StaticValues cannot retain runtime output column metadata".to_string());
        }
        Arc::try_unwrap(self.batch).map_err(|_| "StaticValues batch is shared".to_string())
    }

    pub fn new_with_chunk_schema(batch: RecordBatch, chunk_schema: ChunkSchemaRef) -> Self {
        match Self::try_new_with_chunk_schema(batch, chunk_schema) {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    pub fn chunk_schema(&self) -> &ChunkSchema {
        self.chunk_schema.as_ref()
    }

    /// Fixed input holder scaffolds are part of the result input allowance.
    /// Shared MEM tracker/provider governance authorities retain their existing
    /// accounting owner and are not traversed as if they were Arrow payload.
    pub(super) fn inspect_root_holder_scaffolds(
        &self,
        inspection: &mut super::root_schema_backing::RootSchemaInspection,
    ) -> Result<(), super::root_schema_backing::RootSchemaBackingError> {
        inspection.charge(std::mem::size_of::<Self>())?;
        inspection.charge_arc(self.batch.as_ref())?;
        match &self.accounting {
            Some(ChunkAccountingHandle::WholeBatch(accounting)) => {
                inspection.charge_arc(accounting.as_ref())?
            }
            Some(ChunkAccountingHandle::AdditionalBuffers(ledger)) => {
                inspection.charge(ledger.holder_bytes().map_err(|_| {
                    super::root_schema_backing::RootSchemaBackingError::UnsupportedType
                })?)?
            }
            None => {}
        }
        if let Some(output_memory) = &self.connector_output_memory {
            inspection.charge_arc(output_memory.as_ref())?;
        }
        if let Some(metadata) = &self.output_column_metadata {
            inspection.charge(metadata.holder_bytes().map_err(|_| {
                super::root_schema_backing::RootSchemaBackingError::UnsupportedType
            })?)?;
        }
        Ok(())
    }

    pub fn chunk_schema_ref(&self) -> ChunkSchemaRef {
        Arc::clone(&self.chunk_schema)
    }

    pub fn slot_id_to_index(&self) -> &HashMap<SlotId, usize> {
        self.chunk_schema.index_by_slot()
    }

    pub fn column_by_slot_id(&self, slot_id: SlotId) -> Result<ArrayRef, String> {
        let idx = self
            .chunk_schema
            .index_by_slot()
            .get(&slot_id)
            .copied()
            .ok_or_else(|| {
                format!(
                    "slot id {} not found in chunk (num_columns={}, slot_ids={:?})",
                    slot_id,
                    self.batch.num_columns(),
                    self.chunk_schema.index_by_slot().keys().collect::<Vec<_>>()
                )
            })?;
        self.batch
            .columns()
            .get(idx)
            .cloned()
            .ok_or_else(|| format!("slot id {} mapped to invalid index {}", slot_id, idx))
    }

    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            !self.chunk_schema.is_typed()
                && !matches!(
                    self.accounting,
                    Some(ChunkAccountingHandle::AdditionalBuffers(_))
                ),
            "typed chunk slicing requires a supplied creation lifetime and observer"
        );
        let mut out = Self {
            batch: Arc::new(self.batch.slice(offset, length)),
            chunk_schema: Arc::clone(&self.chunk_schema),
            accounting: None,
            connector_output_memory: self.connector_output_memory.clone(),
            array_type_storage: self.array_type_storage.clone(),
            construction_storage: ValueTypeStorageLifetime::pure(),
            output_column_metadata: self.output_column_metadata.clone(),
        };
        if let Some(ChunkAccountingHandle::WholeBatch(accounting)) = self.accounting.as_ref() {
            let tracker = accounting.tracker();
            out.transfer_to(&tracker);
        }
        out
    }

    /// Slice under a new wrapper creation hold while keeping only the original
    /// shared array type backing hold. Intermediate slice wrapper grants can
    /// exit when their wrappers exit; no parent Chunk is retained.
    pub fn slice_typed_observed<E, F>(
        &self,
        offset: usize,
        length: usize,
        storage: ValueTypeStorageLifetime,
        observe: F,
    ) -> Result<Self, E>
    where
        E: From<String>,
        F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
    {
        if !self.chunk_schema.is_typed() {
            return Err(E::from(
                "typed chunk slice requires its explicit source contract".into(),
            ));
        }
        self.slice_with_creation_observed(offset, length, storage, observe)
    }

    /// Create only the new view wrappers under the actual caller's lifetime.
    /// The source's typed/untyped binding is preserved without reconstruction.
    pub(crate) fn slice_with_creation_observed<E, F>(
        &self,
        offset: usize,
        length: usize,
        storage: ValueTypeStorageLifetime,
        mut observe: F,
    ) -> Result<Self, E>
    where
        E: From<String>,
        F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
    {
        if offset
            .checked_add(length)
            .is_none_or(|end| end > self.len())
        {
            return Err(E::from(
                "chunk slice is outside its explicit source contract".into(),
            ));
        }
        observe(ArrayNormalizationStep::CreateChunkColumns {
            channels: self.columns().len(),
        })?;
        let mut columns = Vec::with_capacity(self.columns().len());
        for source in self.columns() {
            observe(ArrayNormalizationStep::SliceArray {
                source,
                offset,
                length,
            })?;
            let sliced = source.slice(offset, length);
            observe(ArrayNormalizationStep::OpaqueOperationFinished)?;
            columns.push(sliced);
        }
        // The private immutable input has already passed strict ingress. A
        // valid subset preserves every type, encoding key and NULL law. Keep
        // Arrow's normal batch validation without repeating ingress work.
        let batch = build_record_batch(self.chunk_schema.arrow_schema_ref(), columns, length)
            .map_err(E::from)?;
        observe(ArrayNormalizationStep::CreateRecordBatchWrapper)?;
        let mut result = Self {
            batch: Arc::new(batch),
            chunk_schema: self.chunk_schema_ref(),
            accounting: None,
            connector_output_memory: None,
            array_type_storage: self.array_type_storage.clone(),
            construction_storage: storage,
            output_column_metadata: self.output_column_metadata.clone(),
        };
        result.retain_source_buffer_funding(self);
        Ok(result)
    }

    /// Retain only independently created array/type structures in this owned
    /// chunk. The immutable schema retains its own separate finite lifetime.
    pub(crate) fn retaining_array_type_storage_observed<E: From<LayoutResourceError>>(
        mut self,
        original: &ValueTypeStorageLifetime,
        request: impl FnMut(Layout) -> Result<(), E>,
    ) -> Result<Self, E> {
        self.array_type_storage = self
            .array_type_storage
            .retaining_observed(original, request)?;
        Ok(self)
    }

    pub fn construction_storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.construction_storage.clone()
    }

    pub fn array_type_storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.array_type_storage.clone()
    }

    pub fn is_empty(&self) -> bool {
        self.batch.num_rows() == 0
    }

    pub fn columns(&self) -> &[ArrayRef] {
        self.batch.columns()
    }

    pub fn estimated_bytes(&self) -> usize {
        self.batch.get_array_memory_size()
    }

    pub fn logical_bytes(&self) -> usize {
        record_batch_bytes(&self.batch)
    }

    pub fn transfer_to(&mut self, tracker: &Arc<MemTracker>) {
        if let Some(accounting) = self.accounting.as_ref() {
            if let ChunkAccountingHandle::WholeBatch(accounting) = accounting {
                accounting.transfer_to(tracker);
            }
            return;
        }
        if self.connector_output_memory.is_some() {
            return;
        }
        let bytes = chunk_bytes_i64(&self.batch);
        if bytes <= 0 {
            return;
        }
        self.accounting = Some(ChunkAccountingHandle::WholeBatch(Arc::new(
            ChunkAccounting::new(bytes, tracker),
        )));
    }

    /// Transfers this chunk's complete accounting lease while enforcing the
    /// destination tracker limit. A failed first charge remains attached to
    /// the chunk so dropping it releases the live bytes instead of leaking an
    /// exceeded counter.
    pub fn try_transfer_to(&mut self, tracker: &Arc<MemTracker>) -> Result<(), String> {
        self.try_transfer_to_typed(tracker)
            .map_err(|error| error.detail().to_owned())
    }

    /// Preserve the original tracker refusal across the same accounting
    /// transfer. The source charge and connector reservation laws are unchanged.
    pub fn try_transfer_to_typed(
        &mut self,
        tracker: &Arc<MemTracker>,
    ) -> Result<(), crate::runtime::mem_tracker::MemTrackerLimitExceeded> {
        if let Some(accounting) = self.accounting.as_ref() {
            return match accounting {
                ChunkAccountingHandle::WholeBatch(accounting) => {
                    accounting.try_transfer_to_typed(tracker)
                }
                ChunkAccountingHandle::AdditionalBuffers(_) => Ok(()),
            };
        }
        if self.connector_output_memory.is_some() {
            return Ok(());
        }
        let bytes = chunk_bytes_i64(&self.batch);
        if bytes <= 0 {
            return Ok(());
        }
        let result = tracker.consume_and_check_limit_typed(bytes);
        self.accounting = Some(ChunkAccountingHandle::WholeBatch(Arc::new(
            ChunkAccounting::from_charged(bytes, tracker),
        )));
        result
    }

    #[cfg(test)]
    pub(crate) fn memory_lease(&self) -> Option<ChunkMemoryLease> {
        match self.accounting.as_ref()? {
            ChunkAccountingHandle::WholeBatch(accounting) => {
                Some(ChunkMemoryLease::native(Arc::clone(accounting)))
            }
            ChunkAccountingHandle::AdditionalBuffers(_) => None,
        }
    }

    /// Moves this chunk's accounting owner without cloning it. Consumers that
    /// retain only a zero-copy projection can then split the projected bytes
    /// from the unprojected remainder exactly.
    pub(crate) fn take_memory_lease(&mut self) -> Option<ChunkMemoryLease> {
        if matches!(
            self.accounting,
            Some(ChunkAccountingHandle::AdditionalBuffers(_))
        ) {
            return None;
        }
        if let Some(ChunkAccountingHandle::WholeBatch(accounting)) = self.accounting.take() {
            return Some(ChunkMemoryLease::native(accounting));
        }
        let output_memory = self.connector_output_memory.take()?;
        if Arc::strong_count(&output_memory) == 1 {
            return Some(ChunkMemoryLease::connector(output_memory));
        }
        // A clone or slice still owns the same provider reservation. Keep this
        // chunk attached as well so the shared charge cannot disappear while
        // either Arrow view remains live.
        self.connector_output_memory = Some(output_memory);
        None
    }

    /// Attach only an initialized actual delta, keeping its original source
    /// handles and creating host. No whole-batch lease is granted by this tag.
    pub(super) fn attach_additional_buffers(
        &mut self,
        ledger: super::AdditionalBufferLedger,
    ) -> Result<(), novarocks_functions::KernelFailure> {
        if !self.chunk_schema.is_typed()
            || !ledger.initialized()
            || self.accounting.is_some()
            || self.connector_output_memory.is_some()
        {
            return Err(novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "additional-buffer output requires an unowned typed batch and initialized extent",
                ),
            ));
        }
        self.accounting = Some(ChunkAccountingHandle::AdditionalBuffers(ledger));
        Ok(())
    }

    /// Move an existing connector output reservation onto this chunk without
    /// charging its Arrow buffers again.
    pub(crate) fn attach_connector_output_memory(
        &mut self,
        output_memory: ConnectorOutputMemoryToken,
    ) -> Result<(), String> {
        if self.accounting.is_some() || self.connector_output_memory.is_some() {
            return Err("chunk output memory already has an accounting owner".to_string());
        }
        self.connector_output_memory = Some(Arc::new(Mutex::new(output_memory)));
        Ok(())
    }
}

#[cfg(test)]
use novarocks_functions::array_projection::validate_dictionary_rows_observed;
/// Validate every actual dictionary's keys regardless of semantic nullability.
/// Root NULL is returned separately; nested active required-child rules remain
/// those of Arrow's validated container construction, including parent masks.
use novarocks_functions::array_projection::validate_encoded_keys_and_root_nulls_observed;

fn retag_columns_to_chunk_schema(
    columns: &[ArrayRef],
    chunk_schema: &ChunkSchema,
) -> Result<Vec<ArrayRef>, String> {
    columns
        .iter()
        .zip(chunk_schema.slots())
        .enumerate()
        .map(|(idx, (column, slot))| {
            retag_column(column, slot.data_type()).map_err(|e| {
                format!(
                    "retag chunk column {} to target descriptor type {:?} failed: {:?}",
                    idx,
                    slot.data_type(),
                    e
                )
            })
        })
        .collect()
}

fn build_record_batch(
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    row_count: usize,
) -> Result<RecordBatch, String> {
    let result = if columns.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        RecordBatch::try_new_with_options(schema, columns, &options)
    } else {
        RecordBatch::try_new(schema, columns)
    };
    result.map_err(|e| format!("build chunk record batch failed: {e}"))
}

impl Default for Chunk {
    fn default() -> Self {
        Self {
            batch: Arc::new(RecordBatch::new_empty(Arc::new(Schema::empty()))),
            chunk_schema: Arc::new(ChunkSchema::empty()),
            accounting: None,
            connector_output_memory: None,
            array_type_storage: ValueTypeStorageLifetime::pure(),
            construction_storage: ValueTypeStorageLifetime::pure(),
            output_column_metadata: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, DictionaryArray, LargeStringDictionaryBuilder};
    use arrow::datatypes::{DataType, Field, Int32Type};

    use super::{Chunk, ChunkSchema};
    use crate::exec::chunk::ChunkSlotSchema;
    use novarocks_types::SlotId;

    fn dict_utf8() -> ArrayRef {
        Arc::new(
            vec!["PAID", "NEW", "PAID"]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        )
    }

    fn dict_large_utf8() -> ArrayRef {
        let mut builder = LargeStringDictionaryBuilder::<Int32Type>::new();
        builder.append_value("PAID");
        builder.append_value("NEW");
        builder.append_value("PAID");
        Arc::new(builder.finish())
    }

    #[test]
    fn chunk_string_slot_can_carry_dictionary_int32_string_column() {
        for (slot_type, column) in [
            (DataType::Utf8, dict_utf8()),
            (DataType::LargeUtf8, dict_large_utf8()),
        ] {
            let dict_type = column.data_type().clone();
            assert!(matches!(
                dict_type,
                DataType::Dictionary(ref key, ref value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &slot_type
            ));

            let chunk_schema = Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                    SlotId::new(3),
                    Field::new("status", slot_type, false),
                    None,
                    None,
                )])
                .expect("chunk schema"),
            );

            let chunk =
                Chunk::try_new_with_columns(chunk_schema, vec![column]).expect("dictionary chunk");

            assert_eq!(chunk.len(), 3);
            assert_eq!(chunk.columns()[0].data_type(), &dict_type);
            assert_eq!(
                chunk
                    .chunk_schema()
                    .slot(SlotId::new(3))
                    .expect("slot schema")
                    .data_type(),
                &dict_type
            );
            assert_eq!(
                chunk
                    .column_by_slot_id(SlotId::new(3))
                    .expect("column by slot")
                    .data_type(),
                &dict_type
            );
        }
    }
}

#[cfg(test)]
mod dictionary_validation_tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn nullable_key_row_validation_rejects_negative_and_out_of_bounds_without_unsafe_array() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![None, Some("good")]));
        let source: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![1, 1]), values.clone())
                .unwrap(),
        );
        // Legal primitive key fixtures exercise the row validator directly.
        // No invalid DictionaryArray is created or published through unsafe.
        for keys in [vec![-1, 1], vec![2, 1], vec![0, -1], vec![0, 2]] {
            let keys = Int32Array::from(keys);
            let error = validate_dictionary_rows_observed::<String, _>(
                &source,
                &keys,
                &values,
                &mut |_| Ok(()),
            )
            .unwrap_err();
            assert!(error.contains("key is outside its value array"));
        }
    }

    #[test]
    fn nullable_and_nested_dictionary_ingress_still_observes_keys() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![None, Some("good")]));
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 1]), values).unwrap(),
        );
        let fields = vec![Arc::new(arrow::datatypes::Field::new(
            "nullable",
            dictionary.data_type().clone(),
            true,
        ))];
        let parent: ArrayRef =
            Arc::new(StructArray::try_new(fields.into(), vec![dictionary.clone()], None).unwrap());
        let mut visited = Vec::new();
        let nulls = validate_encoded_keys_and_root_nulls_observed::<String, _>(
            &parent,
            &mut |step| {
                if let ArrayNormalizationStep::InspectDictionaryRows {
                    source, row_count, ..
                } = step
                {
                    assert!(Arc::ptr_eq(source, &dictionary));
                    visited.push(row_count);
                }
                Ok(())
            },
            1,
            &mut 0,
        )
        .unwrap();
        assert!(!nulls);
        assert_eq!(visited, vec![2]);
        let nulls = validate_encoded_keys_and_root_nulls_observed::<String, _>(
            &dictionary,
            &mut |_| Ok(()),
            1,
            &mut 0,
        )
        .unwrap();
        assert!(nulls);
    }
}

#[cfg(test)]
mod shared_batch_projection_tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};
    use novarocks_type_contract::{
        TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageHold,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn schema() -> ChunkSchemaRef {
        let pure = ValueTypeStorageLifetime::pure();
        let value =
            ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), false, pure.clone()).unwrap();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(&DataType::Int32, pure.clone()).unwrap(),
        )
        .unwrap();
        let slot = super::super::ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(Field::new("value", DataType::Int32, false)),
            binding,
            None,
            pure.clone(),
        )
        .unwrap();
        Arc::new(ChunkSchema::try_new_typed(vec![slot], pure).unwrap())
    }
    #[test]
    fn array_only_storage_consumption_releases_independently_of_schema_and_preserves_source_buffers()
     {
        let drops = Arc::new(AtomicUsize::new(0));
        let array_storage = ValueTypeStorageLifetime::held(Arc::new(Hold(drops.clone())));
        let original: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let chunk = Chunk::try_new_typed_with_columns_observed::<String, _>(
            schema(),
            vec![original.clone()],
            2,
            |_| Ok(()),
        )
        .unwrap();
        let schema = chunk.chunk_schema_ref();
        let mut requests = 0;
        let chunk = chunk
            .retaining_array_type_storage_observed::<LayoutResourceError>(&array_storage, |_| {
                requests += 1;
                Ok(())
            })
            .unwrap();
        assert!(Arc::ptr_eq(&chunk.columns()[0], &original));
        assert_eq!(
            requests, 0,
            "pure base can directly share the existing finite storage marker"
        );
        drop(array_storage);
        let projection = chunk.into_batch_projection();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(projection);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(schema.slots().len(), 1);
    }
    #[test]
    fn neutral_source_storage_preserves_buffer_charge_and_creation_after_chunk_drop() {
        let drops = Arc::new(AtomicUsize::new(0));
        let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(drops.clone())));
        let mut chunk = Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            schema(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            3,
            storage,
            |_| Ok(()),
        )
        .unwrap();
        let tracker = MemTracker::new_root("source-values-fixture");
        chunk.accounting = Some(ChunkAccountingHandle::WholeBatch(Arc::new(
            ChunkAccounting::new(12, &tracker),
        )));
        let neutral = chunk
            .source_storage_lifetime_observed::<LayoutResourceError>(
                ValueTypeStorageLifetime::pure(),
                |_| Ok(()),
            )
            .unwrap();
        drop(chunk);
        assert_eq!(tracker.current(), 12);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(neutral);
        assert_eq!(tracker.current(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn neutral_source_storage_refusal_keeps_original_charge_and_arrays() {
        let mut chunk = Chunk::try_new_typed_with_columns_observed::<String, _>(
            schema(),
            vec![Arc::new(Int32Array::from(vec![1]))],
            1,
            |_| Ok(()),
        )
        .unwrap();
        let tracker = MemTracker::new_root("source-values-refusal");
        chunk.accounting = Some(ChunkAccountingHandle::WholeBatch(Arc::new(
            ChunkAccounting::new(4, &tracker),
        )));
        assert!(matches!(
            chunk.source_storage_lifetime_observed::<LayoutResourceError>(
                ValueTypeStorageLifetime::pure(),
                |_| Err(LayoutResourceError::Arithmetic)
            ),
            Err(LayoutResourceError::Arithmetic)
        ));
        assert_eq!(tracker.current(), 4);
        assert_eq!(chunk.columns()[0].len(), 1);
        drop(chunk);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn chunk_clone_and_guarded_projection_share_the_actual_batch_and_column_vector() {
        let drops = Arc::new(AtomicUsize::new(0));
        let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(drops.clone())));
        let mut wrapper_requests = 0;
        let mut source = Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            schema(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            3,
            storage,
            |step| {
                if matches!(step, ArrayNormalizationStep::CreateRecordBatchWrapper) {
                    wrapper_requests += 1;
                }
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(wrapper_requests, 1);
        let tracker = MemTracker::new_root("shared batch source");
        source.transfer_to(&tracker);
        assert!(tracker.current() > 0);
        let clone = source.clone();
        let projection = source.batch_projection();
        let projection_clone = projection.clone();
        assert!(std::ptr::eq(source.record_batch(), clone.record_batch()));
        assert!(std::ptr::eq(
            source.record_batch(),
            projection.record_batch()
        ));
        assert!(std::ptr::eq(
            projection.record_batch(),
            projection_clone.record_batch()
        ));
        assert_eq!(source.columns().as_ptr(), clone.columns().as_ptr());
        assert_eq!(source.columns().as_ptr(), projection.columns().as_ptr());
        drop(source);
        drop(clone);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(tracker.current() > 0);
        let restored = projection.into_chunk();
        assert_eq!(restored.len(), 3);
        assert!(std::ptr::eq(
            restored.record_batch(),
            projection_clone.record_batch()
        ));
        drop(restored);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(tracker.current() > 0);
        drop(projection_clone);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn record_batch_holder_refusal_precedes_publication_and_keeps_source_array() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let original = array.to_data();
        let result = Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            schema(),
            vec![array.clone()],
            3,
            ValueTypeStorageLifetime::pure(),
            |step| {
                if matches!(step, ArrayNormalizationStep::CreateRecordBatchWrapper) {
                    Err("original holder refusal".into())
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(result.unwrap_err(), "original holder refusal");
        assert_eq!(array.to_data(), original);
    }

    #[test]
    fn owned_batch_moves_the_original_column_vector_and_retains_both_real_creators() {
        use crate::exec::pipeline::operator::input_construction::ConstructionFailure;
        let original_drops = Arc::new(AtomicUsize::new(0));
        let creation_drops = Arc::new(AtomicUsize::new(0));
        let original = ValueTypeStorageLifetime::held(Arc::new(Hold(original_drops.clone())));
        let creation = ValueTypeStorageLifetime::held(Arc::new(Hold(creation_drops.clone())));
        let schema = schema();
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let mut columns = Vec::with_capacity(3);
        columns.push(array.clone());
        let columns_ptr = columns.as_ptr();
        let batch = RecordBatch::try_new(schema.arrow_schema_ref(), columns).unwrap();
        let mut hold_requests = 0;
        let mut holder_requests = 0;
        let chunk = Chunk::try_new_typed_record_batch_owned_observed::<ConstructionFailure, _, _>(
            batch,
            schema,
            original,
            creation,
            |_| {
                hold_requests += 1;
                Ok(())
            },
            |step| {
                match step {
                    ArrayNormalizationStep::CreateRecordBatchWrapper => holder_requests += 1,
                    ArrayNormalizationStep::CreateChunkColumns { .. } => {
                        panic!("transferred columns must not be charged as a new vector")
                    }
                    _ => {}
                }
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(chunk.columns().as_ptr(), columns_ptr);
        assert!(Arc::ptr_eq(&chunk.columns()[0], &array));
        assert_eq!(hold_requests, 1);
        assert_eq!(holder_requests, 1);
        let projection = chunk.into_batch_projection();
        assert_eq!(original_drops.load(Ordering::SeqCst), 0);
        assert_eq!(creation_drops.load(Ordering::SeqCst), 0);
        drop(projection);
        assert_eq!(original_drops.load(Ordering::SeqCst), 1);
        assert_eq!(creation_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn owned_batch_preserves_zero_channel_rows_and_same_creator_needs_no_extra_hold() {
        use crate::exec::pipeline::operator::input_construction::ConstructionFailure;
        let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(Arc::new(AtomicUsize::new(0)))));
        let schema = Arc::new(ChunkSchema::try_new_typed(vec![], storage.clone()).unwrap());
        let batch = RecordBatch::try_new_with_options(
            schema.arrow_schema_ref(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(7)),
        )
        .unwrap();
        let result = Chunk::try_new_typed_record_batch_owned_observed::<ConstructionFailure, _, _>(
            batch,
            schema,
            storage.clone(),
            storage,
            |_| panic!("one creator must not allocate a composite hold"),
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(result.len(), 7);
        assert!(result.columns().is_empty());
    }

    #[test]
    #[allow(deprecated)]
    fn owned_batch_does_not_treat_source_dictionary_descriptors_as_planned_identity() {
        use crate::exec::pipeline::operator::input_construction::ConstructionFailure;
        use arrow::array::StringArray;
        let storage = ValueTypeStorageLifetime::pure();
        let carrier = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let value = ValueTypeRef::try_new(ValueKind::String, false, storage.clone()).unwrap();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(&DataType::Utf8, storage.clone()).unwrap(),
        )
        .unwrap();
        let slot = super::super::ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(Field::new_dict("value", carrier.clone(), false, 11, false)),
            binding,
            None,
            storage.clone(),
        )
        .unwrap();
        let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], storage.clone()).unwrap());
        let actual_field = Field::new_dict("value", carrier, false, 87, true);
        let actual_schema = Arc::new(Schema::new(vec![actual_field]));
        let array: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 1]),
                Arc::new(StringArray::from(vec!["x", "y"])),
            )
            .unwrap(),
        );
        let batch = RecordBatch::try_new(actual_schema.clone(), vec![array.clone()]).unwrap();
        let chunk = Chunk::try_new_typed_record_batch_owned_observed::<ConstructionFailure, _, _>(
            batch,
            schema,
            storage.clone(),
            storage,
            |_| Ok(()),
            |_| Ok(()),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&chunk.columns()[0], &array));
        assert!(matches!(
            chunk.chunk_schema().slots()[0]
                .typed_binding()
                .value_type()
                .kind(),
            ValueKind::String
        ));
        assert_eq!(actual_schema.field(0).dict_id(), Some(87));
        assert_eq!(actual_schema.field(0).dict_is_ordered(), Some(true));
    }

    #[test]
    fn owned_batch_checks_original_occurrences_and_preserves_first_host_refusal() {
        use crate::exec::pipeline::operator::input_construction::ConstructionFailure;
        let schema = schema();
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        for field in [
            Field::new("other", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
        ] {
            let batch =
                RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array.clone()])
                    .unwrap();
            let result =
                Chunk::try_new_typed_record_batch_owned_observed::<ConstructionFailure, _, _>(
                    batch,
                    schema.clone(),
                    ValueTypeStorageLifetime::pure(),
                    ValueTypeStorageLifetime::pure(),
                    |_| panic!("invalid occurrence must fail before any new hold"),
                    |_| Ok(()),
                );
            assert!(matches!(result, Err(ConstructionFailure::InvalidSource(_))));
        }
        let original =
            ValueTypeStorageLifetime::held(Arc::new(Hold(Arc::new(AtomicUsize::new(0)))));
        let creation =
            ValueTypeStorageLifetime::held(Arc::new(Hold(Arc::new(AtomicUsize::new(0)))));
        let batch = RecordBatch::try_new(schema.arrow_schema_ref(), vec![array.clone()]).unwrap();
        let refusal = ConstructionFailure::Caller(Box::new(
            crate::runtime::fragment::ExecutionFailure::from("original construction refusal"),
        ));
        let result = Chunk::try_new_typed_record_batch_owned_observed::<ConstructionFailure, _, _>(
            batch,
            schema,
            original,
            creation,
            |_| Err(refusal.clone()),
            |step| {
                assert!(!matches!(
                    step,
                    ArrayNormalizationStep::CreateRecordBatchWrapper
                ));
                Ok(())
            },
        );
        assert_eq!(result.unwrap_err(), refusal);
        assert_eq!(array.len(), 2);
    }

    #[test]
    fn failed_owned_construction_drops_actual_arrays_before_their_original_creation_hold() {
        use crate::exec::pipeline::operator::input_construction::ConstructionFailure;
        struct LateArrayHold {
            array: std::sync::Weak<dyn Array>,
            drops: Arc<AtomicUsize>,
        }
        impl ValueTypeStorageHold for LateArrayHold {}
        impl Drop for LateArrayHold {
            fn drop(&mut self) {
                assert!(self.array.upgrade().is_none());
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let storage = ValueTypeStorageLifetime::held(Arc::new(LateArrayHold {
            array: Arc::downgrade(&array),
            drops: drops.clone(),
        }));
        let result = Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            schema(),
            vec![array],
            3,
            storage,
            |_| Ok(()),
        );
        assert!(result.is_err());
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let storage = ValueTypeStorageLifetime::held(Arc::new(LateArrayHold {
            array: Arc::downgrade(&array),
            drops: drops.clone(),
        }));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Int32,
                false,
            )])),
            vec![array],
        )
        .unwrap();
        let result = Chunk::try_new_typed_record_batch_owned_observed::<ConstructionFailure, _, _>(
            batch,
            schema(),
            storage,
            ValueTypeStorageLifetime::pure(),
            |_| panic!("source label refusal precedes any new storage"),
            |_| Ok(()),
        );
        assert!(result.is_err());
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let storage = ValueTypeStorageLifetime::held(Arc::new(LateArrayHold {
            array: Arc::downgrade(&array),
            drops: drops.clone(),
        }));
        let result = Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            schema(),
            vec![array],
            2,
            storage,
            |step| {
                if matches!(step, ArrayNormalizationStep::CreateRecordBatchWrapper) {
                    Err("original holder refusal".into())
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(result.unwrap_err(), "original holder refusal");
        assert_eq!(drops.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn strict_nested_dictionary_slice_reuses_ingress_proof_and_keeps_actual_creation_holds() {
        use crate::exec::pipeline::operator::input_construction::{
            ConstructionFailure, try_runtime_input_construction_factory,
        };
        use crate::runtime::runtime_state::RuntimeErrorState;
        use arrow::array::StringArray;
        use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use novarocks_type_contract::{ValueField, ValueLogicalType};

        let pure = ValueTypeStorageLifetime::pure();
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), None, Some(1), Some(2), Some(0)]),
                Arc::new(StringArray::from(vec![
                    r#""first""#,
                    r#""second""#,
                    r#""third""#,
                ])),
            )
            .unwrap(),
        );
        let item = Arc::new(
            StructArray::try_new(
                vec![Arc::new(Field::new(
                    "payload",
                    dictionary.data_type().clone(),
                    true,
                ))]
                .into(),
                vec![dictionary],
                None,
            )
            .unwrap(),
        );
        let list: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("item", item.data_type().clone(), true)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 3, 5])),
                item,
                Some(NullBuffer::from(vec![false, true, true])),
            )
            .unwrap(),
        );
        let json = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            true,
            pure.clone(),
        )
        .unwrap();
        let item = ValueTypeRef::try_new(
            ValueKind::Struct(
                vec![ValueField::new("payload", json, pure.clone())].into_boxed_slice(),
            ),
            true,
            pure.clone(),
        )
        .unwrap();
        let value = ValueTypeRef::try_new(ValueKind::List(item), true, pure.clone()).unwrap();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(list.data_type(), pure.clone()).unwrap(),
        )
        .unwrap();
        let slot = super::super::ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(Field::new("value", list.data_type().clone(), true)),
            binding,
            None,
            pure.clone(),
        )
        .unwrap();
        let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], pure).unwrap());
        let tracker = MemTracker::new_root("nested dictionary slice");
        let error = Arc::new(RuntimeErrorState::default());
        let error_weak = Arc::downgrade(&error);
        let factory = try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            error,
        )
        .unwrap();
        let source_owner = factory.begin_typed_construction(&schema).unwrap();
        source_owner
            .observe(
                0,
                &[],
                ArrayNormalizationStep::CreateChunkColumns { channels: 1 },
            )
            .unwrap();
        let mut source =
            Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
                schema,
                vec![list],
                3,
                source_owner.storage_lifetime(),
                |step| source_owner.observe(0, &[], step),
            )
            .unwrap();
        source.transfer_to(&tracker);
        drop(source_owner);
        let before = tracker.current();
        for refused_constructor in 0..4 {
            let owner = factory.begin_input_construction(&source).unwrap();
            let refusal = ConstructionFailure::Caller(Box::new(
                crate::runtime::fragment::ExecutionFailure::from(
                    "original slice constructor refusal",
                ),
            ));
            let result = source.slice_typed_observed::<ConstructionFailure, _>(
                1,
                2,
                owner.storage_lifetime(),
                |step| {
                    let constructor = match step {
                        ArrayNormalizationStep::CreateChunkColumns { .. } => Some(0),
                        ArrayNormalizationStep::SliceArray { .. } => Some(1),
                        ArrayNormalizationStep::CreateRecordBatchWrapper => Some(2),
                        ArrayNormalizationStep::OpaqueOperationFinished => Some(3),
                        _ => None,
                    };
                    if constructor == Some(refused_constructor) {
                        Err(refusal.clone())
                    } else {
                        owner.observe(0, &[], step)
                    }
                },
            );
            assert_eq!(result.unwrap_err(), refusal);
            assert_eq!(source.len(), 3);
            drop(owner);
            assert_eq!(tracker.current(), before);
        }
        let owner = factory.begin_input_construction(&source).unwrap();
        let output = source
            .slice_typed_observed::<ConstructionFailure, _>(
                1,
                2,
                owner.storage_lifetime(),
                |step| {
                    assert!(
                        matches!(
                            step,
                            ArrayNormalizationStep::CreateChunkColumns { .. }
                                | ArrayNormalizationStep::SliceArray { .. }
                                | ArrayNormalizationStep::CreateRecordBatchWrapper
                                | ArrayNormalizationStep::OpaqueOperationFinished
                        ),
                        "immutable slice must not repeat ingress normalization or key validation"
                    );
                    owner.observe(0, &[], step)
                },
            )
            .unwrap();
        let source_list = source.columns()[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(std::ptr::eq(source.chunk_schema(), output.chunk_schema()));
        assert_eq!(
            output.chunk_schema().slots()[0]
                .typed_binding()
                .value_type()
                .child_type_at(0)
                .unwrap()
                .child_type_at(0)
                .unwrap()
                .domain(),
            Some(ValueLogicalType::Json),
        );
        let output_list = output.columns()[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(Arc::ptr_eq(source_list.values(), output_list.values()));
        assert_eq!(output_list.value_offsets(), &[2, 3, 5]);
        assert_eq!(output_list.null_count(), 0);
        let item = output_list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let dictionary = item
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let values = dictionary
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            values.value(dictionary.keys().value(2) as usize),
            r#""second""#
        );
        assert_eq!(
            values.value(dictionary.keys().value(3) as usize),
            r#""third""#
        );
        assert_eq!(
            values.value(dictionary.keys().value(4) as usize),
            r#""first""#
        );
        let projection = output.into_batch_projection();
        drop(owner);
        drop(source);
        drop(factory);
        assert!(error_weak.upgrade().is_none());
        assert!(tracker.current() > 0);
        let restored = projection.into_chunk();
        assert_eq!(restored.len(), 2);
        drop(restored);
        assert_eq!(tracker.current(), 0);
    }
}

#[path = "evaluation_creation.rs"]
mod evaluation_creation;
