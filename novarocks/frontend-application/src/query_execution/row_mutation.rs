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

//! Provider-neutral row-mutation match collection and validation.
//!
//! This module deliberately knows only the signed SPI contract.  It neither
//! interprets provider identity values nor derives a physical write strategy.

mod cast_footprint;
mod signed_layout;
mod uniqueness;
use cast_footprint::CowSignedLayoutFootprint;
use std::sync::Arc;
use std::time::Instant;
use uniqueness::BoundedMutationKeys;

use arrow::array::{Array, ArrayRef, Int8Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationMatchContract,
    ConnectorPayloadRetentionGuard, ConnectorRequestContext, ConnectorRowConversionFootprint,
    ConnectorRowMutationEffect, ConnectorRowMutationIntent, ConnectorRowMutationSelection,
    ConnectorRowMutationSourceBatch, ConnectorRowMutationSourceBuilder,
    MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES,
};

use novarocks_execution::runtime::query_options::QueryOptions;
#[cfg(test)]
use novarocks_native_adapter::root_cow_selection_codec::CowSelectionCodecError;
use novarocks_native_adapter::root_cow_selection_codec::{
    COW_SELECTION_MAX_RECORD_BYTES, CowOwnedDecodeError, CowSelectionRecordHeader,
    CowSelectionRecordKind, CowSelectionStreamDecoder,
};
use novarocks_native_adapter::root_record_assembly::{
    RootRecordAssembly, RootRecordAssemblyError, RootRecordDomain, RootRecordFramingError,
};

const DELETE_EFFECT_TAG: i8 = 1;
const REPLACE_EFFECT_TAG: i8 = 2;
const INSERT_EFFECT_TAG: i8 = 3;

// Frozen MEM-1 M07 Internal profile. Connector/session budgets may only lower it.
const COW_SELECTION_BYTES: u64 = 64 * 1024 * 1024;
const COW_SELECTION_ROWS: u64 = 1024 * 1024;

/// A non-concatenating collector for a Copy-on-Write match result.
///
/// The row budget is capped by the frozen row profile and the byte budget: every retained
/// row consumes at least one byte of the result budget, while Arrow's actual
/// allocation cost is accounted independently through `get_array_memory_size`.
#[allow(
    dead_code,
    reason = "The bounded collector remains the typed connector-row-mutation API while its DML consumers are target-gated."
)]
pub struct BoundedRowMutationMatchCollector {
    context: ConnectorRequestContext,
    max_rows: u64,
    max_bytes: u64,
    row_count: u64,
    byte_count: u64,
    schema: Option<SchemaRef>,
    batches: Vec<CollectedCowBatch>,
    retention: Option<ConnectorPayloadRetentionGuard>,
}

enum CollectedCowBatch {
    Legacy(RecordBatch),
    Owned(ConnectorRowMutationSourceBatch),
}
impl CollectedCowBatch {
    fn batch(&self) -> &RecordBatch {
        match self {
            Self::Legacy(batch) => batch,
            Self::Owned(source) => source.batch(),
        }
    }
    fn source_bytes(&self) -> Result<usize, ConnectorError> {
        match self {
            Self::Legacy(batch) => ConnectorRowConversionFootprint::retained_batch_bytes(batch),
            Self::Owned(source) => Ok(source.source_bytes()),
        }
    }
}
const _: () = assert!(2 * 4096 * size_of::<CollectedCowBatch>() <= 1024 * 1024);

#[allow(
    dead_code,
    reason = "The bounded collector API remains available to target-gated DML consumers."
)]
impl BoundedRowMutationMatchCollector {
    /// Convenience constructor for the coordinator's admitted query options.
    pub fn try_from_query_options(
        context: ConnectorRequestContext,
        options: &QueryOptions,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(context, options.exec_mem_limit())
    }

    /// Creates a collector with the smaller of connector payload and effective
    /// execution-memory limits.  Non-positive memory limits are not effective
    /// limits and therefore do not lower the admitted connector budget.
    pub fn try_new(
        context: ConnectorRequestContext,
        exec_mem_limit: Option<i64>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_inner(context, exec_mem_limit, None)
    }

    /// Creates a collector with an explicit logical result schema.
    ///
    /// This is required when a valid query can return no batches: an empty
    /// selection still carries the exact schema used by its canonical digest
    /// and by provider activation validation.
    pub fn try_new_with_schema(
        context: ConnectorRequestContext,
        exec_mem_limit: Option<i64>,
        schema: SchemaRef,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_inner(context, exec_mem_limit, Some(schema))
    }

    fn try_new_inner(
        context: ConnectorRequestContext,
        exec_mem_limit: Option<i64>,
        schema: Option<SchemaRef>,
    ) -> Result<Self, ConnectorError> {
        let connector_budget = u64::try_from(context.max_total_payload_bytes()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation connector payload budget does not fit u64",
            )
        })?;
        let effective_memory_budget = exec_mem_limit
            .and_then(|limit| u64::try_from(limit).ok())
            .filter(|limit| *limit > 0)
            .unwrap_or(connector_budget);
        let max_bytes = connector_budget
            .min(effective_memory_budget)
            .min(COW_SELECTION_BYTES);
        if max_bytes == 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation match collection has no usable byte budget",
            ));
        }
        let schema_bytes = match &schema {
            Some(schema) => {
                ConnectorRowConversionFootprint::retained_schema_bytes(schema.as_ref())? as u64
            }
            None => 0,
        };
        if schema_bytes > max_bytes {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "COW selection schema exceeds its retained source budget",
            ));
        }
        Ok(Self {
            context,
            max_rows: max_bytes.min(COW_SELECTION_ROWS),
            max_bytes,
            row_count: 0,
            byte_count: schema_bytes,
            schema,
            batches: Vec::new(),
            retention: None,
        })
    }

    pub const fn max_rows(&self) -> u64 {
        self.max_rows
    }

    pub const fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub const fn byte_count(&self) -> u64 {
        self.byte_count
    }

    fn check_next_batch(&self, rows: u64) -> Result<(), ConnectorError> {
        self.check_control()?;
        if self.batches.len()
            >= novarocks_spi::connector::MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES
            || self
                .row_count
                .checked_add(rows)
                .is_none_or(|total| total > self.max_rows)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation match exceeds its frozen row or batch profile",
            ));
        }
        Ok(())
    }

    /// Retains one result batch without concatenating it with prior batches.
    pub fn push(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.push_item(CollectedCowBatch::Legacy(batch))
    }

    fn push_owned(&mut self, batch: ConnectorRowMutationSourceBatch) -> Result<(), ConnectorError> {
        self.push_item(CollectedCowBatch::Owned(batch))
    }

    fn push_item(&mut self, item: CollectedCowBatch) -> Result<(), ConnectorError> {
        if self
            .batches
            .first()
            .is_some_and(|first| std::mem::discriminant(first) != std::mem::discriminant(&item))
        {
            return Err(invalid_match(
                "COW source carrier changed during collection",
            ));
        }
        let batch = item.batch();
        self.check_next_batch(batch.num_rows() as u64)?;
        match &self.schema {
            Some(schema) if batch.schema_ref() != schema => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "row-mutation match batch schema differs from the retained selection schema",
                ));
            }
            None | Some(_) => {}
        }
        let rows = u64::try_from(batch.num_rows()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation match batch row count does not fit u64",
            )
        })?;
        let bytes = u64::try_from(item.source_bytes()?).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation match batch byte count does not fit u64",
            )
        })?;
        let next_rows = self.row_count.checked_add(rows).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation match row accounting overflowed",
            )
        })?;
        let initial_schema_bytes = if self.schema.is_none() {
            ConnectorRowConversionFootprint::retained_schema_bytes(batch.schema_ref())? as u64
        } else {
            0
        };
        let next_bytes = self
            .byte_count
            .checked_add(bytes)
            .and_then(|v| v.checked_add(initial_schema_bytes))
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "row-mutation match byte accounting overflowed",
                )
            })?;
        if next_rows > self.max_rows || next_bytes > self.max_bytes {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation match result exceeds its row or byte budget",
            ));
        }
        if self.batches.len() == self.batches.capacity() {
            let maximum = novarocks_spi::connector::MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES;
            let desired = self
                .batches
                .capacity()
                .saturating_mul(2)
                .max(1)
                .min(maximum);
            let peak = self
                .batches
                .capacity()
                .checked_add(desired)
                .and_then(|slots| slots.checked_mul(size_of::<CollectedCowBatch>()))
                .ok_or_else(|| invalid_match("COW collector slot accounting overflowed"))?;
            if peak > 1024 * 1024 {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "COW collector batch headers exceed their bookkeeping workspace",
                ));
            }
            self.batches
                .try_reserve_exact(desired - self.batches.len())
                .map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "COW collector batch header allocation was refused",
                    )
                })?;
            if self.batches.capacity() != desired {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "COW collector batch header allocation exceeds its exact capacity",
                ));
            }
        }
        if self.schema.is_none() {
            self.schema = Some(batch.schema());
        }
        self.row_count = next_rows;
        self.byte_count = next_bytes;
        self.batches.push(item);
        Ok(())
    }

    pub fn finish(self) -> Result<ConnectorRowMutationSelection, ConnectorError> {
        self.check_control()?;
        let schema = self.schema.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "empty row-mutation match collection has no explicit selection schema",
            )
        })?;
        // The old/new header vectors fit the frozen one-MiB collector slice.
        if self
            .batches
            .first()
            .is_none_or(|batch| matches!(batch, CollectedCowBatch::Owned(_)))
        {
            let sources = self
                .batches
                .into_iter()
                .map(|batch| match batch {
                    CollectedCowBatch::Owned(source) => source,
                    CollectedCowBatch::Legacy(_) => {
                        unreachable!("collector rejects mixed carriers")
                    }
                })
                .collect();
            match self.retention {
                Some(guard) => ConnectorRowMutationSelection::try_new_owned_with_guard(
                    schema,
                    sources,
                    self.max_rows,
                    self.max_bytes,
                    guard,
                ),
                None => ConnectorRowMutationSelection::try_new_owned(
                    schema,
                    sources,
                    self.max_rows,
                    self.max_bytes,
                ),
            }
        } else {
            let batches = self
                .batches
                .into_iter()
                .map(|batch| match batch {
                    CollectedCowBatch::Legacy(batch) => batch,
                    CollectedCowBatch::Owned(_) => unreachable!("collector rejects mixed carriers"),
                })
                .collect();
            let selection = ConnectorRowMutationSelection::try_new(
                schema,
                batches,
                self.max_rows,
                self.max_bytes,
            )?;
            Ok(match self.retention {
                Some(guard) => selection.retain_carrier(guard),
                None => selection,
            })
        }
    }

    fn check_control(&self) -> Result<(), ConnectorError> {
        if self.context.is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "row-mutation match collection cancelled",
            ));
        }
        if Instant::now() >= self.context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "row-mutation match collection deadline elapsed",
            ));
        }
        Ok(())
    }
}

/// Normalize an already converted match batch to its signed layout. The whole
/// batch is preflighted before dictionary or layout construction allocates.
#[cfg(test)]
pub fn normalize_to_signed_selection(
    schema: &SchemaRef,
    batch: &RecordBatch,
) -> Result<RecordBatch, String> {
    CowSignedLayoutFootprint::for_batch(schema, batch, 0, 0, 0)
        .map_err(|error| error.to_string())?;
    signed_layout::normalize(schema, batch).map_err(|error| error.to_string())
}

#[cfg(test)]
pub(crate) fn normalize_signed_column(
    array: &ArrayRef,
    field: &arrow::datatypes::Field,
) -> Result<ArrayRef, String> {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![field.clone()]));
    let source_schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("input", array.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(source_schema, vec![Arc::clone(array)])
        .map_err(|error| error.to_string())?;
    let normalized = normalize_to_signed_selection(&schema, &batch)?;
    Ok(Arc::clone(normalized.column(0)))
}

/// Preserve source eligibility through signed layout normalization. The source
/// receipt proves all input backing; whole-batch preflight precedes allocation.
/// The copied output is minted by the same safe owner, with no raw adoption.
fn normalize_owned_to_signed_selection(
    schema: &SchemaRef,
    source: &ConnectorRowMutationSourceBatch,
    retained_selection: usize,
    assembly_bytes: usize,
    retention: Option<&ConnectorPayloadRetentionGuard>,
    declarations: &[novarocks_type_contract::FunctionValueType],
    conversions: &mut novarocks_value_arrays::decode::ValueConversionContext,
    sql_output: Option<&signed_layout::CowSqlOutput>,
    work: &mut dyn FnMut(usize) -> Result<(), ConnectorError>,
) -> Result<ConnectorRowMutationSourceBatch, ConnectorError> {
    let projected = sql_output
        .map(|output| output.validate_and_project(source.batch(), declarations, work))
        .transpose()?;
    let input = projected.as_deref().unwrap_or_else(|| source.batch());
    let observed_input = ConnectorRowConversionFootprint::retained_batch_bytes(input)?;
    let source_scratch = source.source_bytes().saturating_sub(observed_input);
    // 32 MiB validator + 8 MiB bookkeeping coexist while collecting. The
    // detached output factory can use its complete 32 MiB construction slice.
    let other_live = assembly_bytes
        .checked_add(40 * 1024 * 1024)
        .and_then(|bytes| bytes.checked_add(MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES))
        .ok_or_else(|| invalid_match("COW layout coexistence accounting overflowed"))?;
    cast_footprint::preflight_inputs(schema, input, retained_selection, source_scratch)?;
    let plan = signed_layout::CowSignedLayoutPlan::plan(
        schema,
        input,
        declarations,
        conversions,
        cast_footprint::TRANSFORM_BYTES,
        work,
    )?;
    let footprint = CowSignedLayoutFootprint::for_plans(
        schema,
        input,
        &plan,
        retained_selection,
        source_scratch,
        other_live,
        work,
    )?;
    tracing::trace!(
        input_bytes = footprint.input_bytes,
        output_bytes = footprint.output_bytes,
        temporary_bytes = footprint.temporary_bytes,
        copy_peak_bytes = footprint.copy_peak_bytes,
        peak_bytes = footprint.peak_bytes,
        "COW signed layout construction preflight"
    );
    // The complete normalization/copy peak was admitted before this owner allocates.
    let mut owner = match retention {
        Some(guard) => ConnectorRowMutationSourceBuilder::try_new_with_guard(
            Arc::clone(schema),
            guard.clone(),
        )?,
        None => ConnectorRowMutationSourceBuilder::try_new(Arc::clone(schema))?,
    };
    // Hydrate supplies its real output/scratch layouts before any allocation.
    // Keep these original receipts until the temporary normalized batch exits;
    // the independent SourceBuilder copies obtain their own Buffer receipts.
    let mut normalization_receipts = Vec::new();
    let normalized = plan.execute_with_growth(
        conversions,
        &mut |layout| {
            if let Some(author) = retention {
                normalization_receipts.push(author.reserve_owned_bytes(layout.size())?);
            }
            Ok(())
        },
        work,
    )?;
    let mut columns = owner.children(schema.fields().len())?;
    let mut node = 0;
    for column in normalized.columns() {
        let data = column.to_data();
        let copy = owner.copy_data(node, &data)?;
        fn nodes(data: &arrow::array::ArrayData) -> usize {
            1 + data.child_data().iter().map(nodes).sum::<usize>()
        }
        node += nodes(&data);
        columns.push(copy)?;
    }
    owner.finish(normalized.num_rows(), columns)
}

/// Collects a relayed CowSelectionArrowV1 stream into the bounded selection.
///
/// Relayed bodies are assembled into whole records whose declared length is
/// checked against the collector's byte budget before any assembly buffer is
/// reserved. Each BATCH record is decoded, normalized to the signed layout and
/// handed to the bounded collector at once, so no second copy of the stream
/// is retained. A body taken into assembly may be acknowledged before its
/// record completes; nothing is a published selection until `finish`.
pub struct RelayedCowSelectionCollector {
    assembly: RootRecordAssembly,
    decoder: CowSelectionStreamDecoder,
    schema: SchemaRef,
    collector: BoundedRowMutationMatchCollector,
    declarations: Vec<novarocks_type_contract::FunctionValueType>,
    conversions: novarocks_value_arrays::decode::ValueConversionContext,
    sql_output: Option<signed_layout::CowSqlOutput>,
    #[cfg(test)]
    local_assembly: bool,
    retention: Option<ConnectorPayloadRetentionGuard>,
}

impl RelayedCowSelectionCollector {
    #[cfg(test)]
    pub fn try_new(
        context: ConnectorRequestContext,
        exec_mem_limit: Option<i64>,
        schema: SchemaRef,
    ) -> Result<Self, ConnectorError> {
        let declarations = signed_layout::fixture_declarations(&schema);
        let mut collector = Self::try_new_declared(context, exec_mem_limit, schema, declarations)?;
        collector.local_assembly = true;
        Ok(collector)
    }

    pub fn try_new_declared(
        context: ConnectorRequestContext,
        exec_mem_limit: Option<i64>,
        schema: SchemaRef,
        declarations: Vec<novarocks_type_contract::FunctionValueType>,
    ) -> Result<Self, ConnectorError> {
        let collector = BoundedRowMutationMatchCollector::try_new_with_schema(
            context,
            exec_mem_limit,
            Arc::clone(&schema),
        )?;
        signed_layout::validate_declarations(&schema, &declarations, &mut |_| {
            collector.check_control()
        })?;
        let record_bound = collector
            .max_bytes()
            .min(COW_SELECTION_MAX_RECORD_BYTES)
            .min(32 * 1024 * 1024);
        let record_bound = usize::try_from(record_bound).unwrap_or(usize::MAX);
        Ok(Self {
            assembly: RootRecordAssembly::new(RootRecordDomain::CowSelection, record_bound),
            decoder: CowSelectionStreamDecoder::new(),
            schema,
            collector,
            retention: None,
            #[cfg(test)]
            local_assembly: false,
            declarations,
            conversions: novarocks_value_arrays::decode::ValueConversionContext::new(0),
            sql_output: None,
        })
    }

    pub fn check_end(&self, output_rows: u64) -> Result<(), ConnectorError> {
        self.assembly.finish().map_err(cow_framing_error)?;
        if self.collector.row_count() != output_rows {
            return Err(invalid_match(
                "COW selection row count differs from Root End",
            ));
        }
        Ok(())
    }

    /// Bytes held by the bounded collector.
    #[cfg(test)]
    pub const fn byte_count(&self) -> u64 {
        self.collector.byte_count()
    }

    /// Bytes held by an unfinished record.
    #[cfg(test)]
    pub fn assembly_bytes(&self) -> usize {
        self.assembly.retained_bytes()
    }

    /// Feed one relayed body.
    pub fn push_body(&mut self, body: &[u8]) -> Result<(), ConnectorError> {
        #[cfg(test)]
        let explicit_local = self.local_assembly;
        #[cfg(not(test))]
        let explicit_local = false;
        let Self {
            assembly,
            decoder,
            schema,
            collector,
            retention,
            declarations,
            conversions,
            sql_output,
            ..
        } = self;
        let consume = |record: &[u8]| {
            let header = CowSelectionRecordHeader::parse(record).map_err(cow_codec_error)?;
            if header.kind() == CowSelectionRecordKind::Batch {
                // Refuse the declaration before the private codec creates Arrow buffers.
                collector.check_next_batch(header.rows())?;
            }
            let decoded = match retention.as_ref() {
                Some(guard) => decoder.apply_owned_record_with_guard(record, guard.clone()),
                None => decoder.apply_owned_record(record),
            };
            let Some(batch) = decoded.map_err(cow_owned_decode_error)? else {
                let width = decoder.schema().map_or(0, |relayed| relayed.fields().len());
                if width != schema.fields().len() {
                    return Err(invalid_match(
                        "relayed COW selection schema width differs from its signed contract",
                    ));
                }
                return Ok(());
            };
            let batch = normalize_owned_to_signed_selection(
                schema,
                &batch,
                collector.byte_count() as usize,
                record.len(),
                retention.as_ref(),
                declarations,
                conversions,
                sql_output.as_ref(),
                &mut |_| collector.check_control(),
            )?;
            collector.push_owned(batch)
        };
        let pushed = match retention.as_ref() {
            Some(author) => {
                assembly.push_budgeted(body, |bytes| author.reserve_owned_bytes(bytes), consume)
            }
            None if explicit_local => assembly.push(body, consume),
            None => Err(RootRecordAssemblyError::Sink(invalid_match(
                "COW Root assembly requires its original byte-budget author",
            ))),
        };
        if pushed.is_err() {
            conversions.clear();
        }
        pushed.map_err(|error| match error {
            RootRecordAssemblyError::Sink(error) => error,
            RootRecordAssemblyError::Framing(error) => cow_framing_error(error),
        })
    }

    /// The stream's End: no record may remain unfinished.
    pub fn finish(mut self) -> Result<ConnectorRowMutationSelection, ConnectorError> {
        self.conversions.clear();
        self.assembly.finish().map_err(cow_framing_error)?;
        // Schema-only streams retain the decoder's holder until the whole
        // selection has its own holder, including its empty indices.
        let _decoder_schema = self.decoder.finish_with_guard();
        self.collector.finish()
    }
}

/// One signed COW consumer for decoded transition batches and relayed records.
/// Its selection is validated before the coordinator can seal root success.
pub(crate) struct CowMatchRootConsumer {
    collector: RelayedCowSelectionCollector,
    validator: RowMutationMatchValidator,
}

impl CowMatchRootConsumer {
    #[cfg(test)]
    fn fixture_output(
        declarations: &[novarocks_type_contract::FunctionValueType],
    ) -> Vec<novarocks_query_application::api::ResultField> {
        declarations
            .iter()
            .enumerate()
            .map(|(ordinal, value)| {
                novarocks_query_application::api::ResultField::new(
                    format!("__nr_sel_{ordinal}"),
                    value.clone(),
                )
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn try_new(
        context: ConnectorRequestContext,
        schema: SchemaRef,
        contract: ConnectorMutationMatchContract,
        intent: ConnectorRowMutationIntent,
    ) -> Result<Self, ConnectorError> {
        let declarations = signed_layout::fixture_declarations(&schema);
        let output = Self::fixture_output(&declarations);
        let mut consumer =
            Self::try_new_declared(context, schema, declarations, contract, intent, &output)?;
        consumer.collector.local_assembly = true;
        Ok(consumer)
    }
    #[cfg(test)]
    pub(crate) fn try_new_with_capacity(
        context: ConnectorRequestContext,
        schema: SchemaRef,
        contract: ConnectorMutationMatchContract,
        intent: ConnectorRowMutationIntent,
        binding: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Result<Self, ConnectorError> {
        let declarations = signed_layout::fixture_declarations(&schema);
        let output = Self::fixture_output(&declarations);
        Self::try_new_declared_with_capacity(
            context,
            schema,
            declarations,
            contract,
            intent,
            binding,
            &output,
        )
    }

    pub(crate) fn try_new_declared(
        context: ConnectorRequestContext,
        schema: SchemaRef,
        declarations: Vec<novarocks_type_contract::FunctionValueType>,
        contract: ConnectorMutationMatchContract,
        intent: ConnectorRowMutationIntent,
        output: &[novarocks_query_application::api::ResultField],
    ) -> Result<Self, ConnectorError> {
        let mut collector =
            RelayedCowSelectionCollector::try_new_declared(context, None, schema, declarations)?;
        let validator = RowMutationMatchValidator::try_new(contract, intent)?;
        let sql_output = signed_layout::CowSqlOutput::try_new(
            output,
            &collector.schema,
            &validator,
            &mut |_| collector.collector.check_control(),
        )?;
        collector.sql_output = Some(sql_output);
        Ok(Self {
            collector,
            validator,
        })
    }

    pub(crate) fn try_new_declared_with_capacity(
        context: ConnectorRequestContext,
        schema: SchemaRef,
        declarations: Vec<novarocks_type_contract::FunctionValueType>,
        contract: ConnectorMutationMatchContract,
        intent: ConnectorRowMutationIntent,
        binding: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
        output: &[novarocks_query_application::api::ResultField],
    ) -> Result<Self, ConnectorError> {
        let window = binding.window_alias();
        crate::query_execution::internal_result_cpu::require_internal_result_capacity(
            binding.scope(),
            &window,
        )
        .map_err(invalid_match)?;
        let retention =
            crate::query_execution::internal_result_cpu::InternalResultRetention::try_new(binding)
                .map_err(invalid_match)?;
        let guard = retention
            .try_spi_guard()
            .map_err(|error| invalid_match(error.to_string()))?;
        let mut consumer =
            Self::try_new_declared(context, schema, declarations, contract, intent, output)?;
        consumer.collector.collector.retention = Some(guard.clone());
        consumer.collector.retention = Some(guard);
        Ok(consumer)
    }

    pub(crate) fn push_body(&mut self, body: &[u8]) -> Result<(), ConnectorError> {
        self.collector.push_body(body)
    }

    pub(crate) fn check_end(&self, output_rows: u64) -> Result<(), ConnectorError> {
        self.collector.check_end(output_rows)
    }

    pub(crate) fn finish(mut self) -> Result<ConnectorRowMutationSelection, ConnectorError> {
        let selection = self.collector.finish()?;
        self.validator.validate_selection(&selection)?;
        Ok(selection)
    }
}

/// Validates that a match result remains within the signed, token-bound
/// contract and that no target row is matched twice.  Insert rows intentionally
/// do not participate in target uniqueness.
pub struct RowMutationMatchValidator {
    contract: ConnectorMutationMatchContract,
    intent: ConnectorRowMutationIntent,
    uniqueness_ordinals: Vec<usize>,
    converter: RowConverter,
    conversion_footprint: ConnectorRowConversionFootprint,
    seen: BoundedMutationKeys,
}

impl RowMutationMatchValidator {
    pub fn try_new(
        contract: ConnectorMutationMatchContract,
        intent: ConnectorRowMutationIntent,
    ) -> Result<Self, ConnectorError> {
        contract.validate()?;
        intent.validate()?;
        // Validate references and borrow the whole type shape before any
        // ordinals, SortField/DataType clones or converter are constructed.
        for token in contract.uniqueness_tokens() {
            match_field(&contract, *token).ok_or_else(|| {
                invalid_match("row-mutation uniqueness token is foreign to the match contract")
            })?;
        }
        let conversion_footprint =
            ConnectorRowConversionFootprint::for_fields(contract.uniqueness_tokens().iter().map(
                |token| match_field(&contract, *token).expect("uniqueness fields were validated"),
            ))?;
        // Digest/cast reserve this exact coexistence allowance for the live
        // validator. Refuse before its converter is constructed.
        if conversion_footprint.converter_bytes > 32 * 1024 * 1024 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "COW validator converter exceeds its retained transform slice",
            ));
        }
        let seen = BoundedMutationKeys::new();
        let vector_bytes = contract
            .uniqueness_tokens()
            .len()
            .checked_mul(size_of::<usize>() + size_of::<ArrayRef>())
            .ok_or_else(|| invalid_match("uniqueness vector byte count overflowed"))?;
        conversion_footprint.checked_constructor_peak_with(seen.retained_bytes(), vector_bytes)?;
        let uniqueness_ordinals = contract
            .uniqueness_tokens()
            .iter()
            .map(|token| {
                match_ordinal(&contract, *token).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "row-mutation uniqueness token is foreign to the match contract",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let fields = contract
            .uniqueness_tokens()
            .iter()
            .map(|token| {
                match_field(&contract, *token)
                    .map(|field| SortField::new(field.data_type().clone()))
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "row-mutation uniqueness token is foreign to the match contract",
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let converter = RowConverter::new(fields).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("row-mutation uniqueness tuple cannot be canonicalized: {error}"),
            )
        })?;
        Ok(Self {
            contract,
            intent,
            uniqueness_ordinals,
            converter,
            conversion_footprint,
            seen,
        })
    }

    pub fn validate_batch(&mut self, batch: &RecordBatch) -> Result<(), ConnectorError> {
        self.validate_schema(&batch.schema())?;
        let effect_ordinal = usize::try_from(self.contract.effect_field().target_ordinal())
            .map_err(|_| invalid_match("row-mutation effect ordinal does not fit usize"))?;
        let effects = batch
            .column(effect_ordinal)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| invalid_match("row-mutation effect column is not Int8"))?;
        if effects.null_count() != 0 {
            return Err(invalid_match("row-mutation effect column contains nulls"));
        }
        let uniqueness_columns = self
            .uniqueness_ordinals
            .iter()
            .map(|ordinal| {
                batch
                    .columns()
                    .get(*ordinal)
                    .cloned()
                    .ok_or_else(|| invalid_match("row-mutation uniqueness ordinal is missing"))
            })
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        // The complete selection can remain live while this batch's encoded
        // Rows and all earlier canonical keys coexist. Use the frozen maximum
        // selection allowance, not only the visible uniqueness-column bytes.
        let footprint = self
            .conversion_footprint
            .for_columns(&uniqueness_columns, COW_SELECTION_BYTES as usize)?;
        footprint.checked_peak_with(self.seen.retained_bytes(), 0)?;
        self.seen.set_external_bytes(footprint.peak_bytes)?;
        let rows = self
            .converter
            .convert_columns(&uniqueness_columns)
            .map_err(|error| {
                invalid_match(format!(
                    "row-mutation uniqueness tuple conversion failed: {error}"
                ))
            })?;
        for row_idx in 0..batch.num_rows() {
            let effect = decode_effect(effects.value(row_idx))?;
            if !self.intent.accepts(effect) {
                return Err(invalid_match(
                    "row-mutation effect is not accepted by the signed intent",
                ));
            }
            if effect == ConnectorRowMutationEffect::Insert {
                continue;
            }
            if uniqueness_columns
                .iter()
                .any(|column| column.is_null(row_idx))
            {
                return Err(invalid_match(
                    "row-mutation delete or replace uniqueness tuple contains null",
                ));
            }
            let key = rows.row(row_idx);
            if !self.seen.insert(key.data())? {
                return Err(invalid_match(
                    "row-mutation delete or replace matched the same target more than once",
                ));
            }
        }
        Ok(())
    }

    pub fn validate_selection(
        &mut self,
        selection: &ConnectorRowMutationSelection,
    ) -> Result<(), ConnectorError> {
        selection.validate()?;
        for batch in selection.batches() {
            self.validate_batch(batch)?;
        }
        Ok(())
    }

    fn validate_schema(&self, schema: &SchemaRef) -> Result<(), ConnectorError> {
        for field in self.contract.identity_fields() {
            validate_schema_field(schema, field.source_ordinal(), field.field())?;
        }
        for field in self
            .contract
            .before_fields()
            .iter()
            .chain(self.contract.after_fields())
        {
            validate_schema_field(schema, field.target_ordinal(), field.field())?;
        }
        validate_schema_field(
            schema,
            self.contract.effect_field().target_ordinal(),
            self.contract.effect_field().field(),
        )
    }
}

fn match_ordinal(
    contract: &ConnectorMutationMatchContract,
    token: novarocks_spi::connector::ConnectorWriteFieldToken,
) -> Option<usize> {
    contract
        .identity_fields()
        .iter()
        .find(|field| field.token() == token)
        .map(|field| field.source_ordinal())
        .or_else(|| {
            contract
                .before_fields()
                .iter()
                .chain(contract.after_fields())
                .find(|field| field.token() == token)
                .map(|field| field.target_ordinal())
        })
        .or_else(|| {
            (contract.effect_field().token() == token)
                .then_some(contract.effect_field().target_ordinal())
        })
        .and_then(|ordinal| usize::try_from(ordinal).ok())
}

fn match_field(
    contract: &ConnectorMutationMatchContract,
    token: novarocks_spi::connector::ConnectorWriteFieldToken,
) -> Option<&arrow::datatypes::Field> {
    contract
        .identity_fields()
        .iter()
        .find(|field| field.token() == token)
        .map(|field| field.field())
        .or_else(|| {
            contract
                .before_fields()
                .iter()
                .chain(contract.after_fields())
                .find(|field| field.token() == token)
                .map(|field| field.field())
        })
        .or_else(|| {
            (contract.effect_field().token() == token).then_some(contract.effect_field().field())
        })
}

fn validate_schema_field(
    schema: &SchemaRef,
    ordinal: u32,
    expected: &arrow::datatypes::Field,
) -> Result<(), ConnectorError> {
    let ordinal = usize::try_from(ordinal)
        .map_err(|_| invalid_match("row-mutation field ordinal does not fit usize"))?;
    let actual = schema
        .fields()
        .get(ordinal)
        .ok_or_else(|| invalid_match("row-mutation contract field ordinal is missing"))?;
    if actual.as_ref() != expected {
        return Err(invalid_match(
            "row-mutation match batch schema does not match the signed contract",
        ));
    }
    Ok(())
}

fn decode_effect(value: i8) -> Result<ConnectorRowMutationEffect, ConnectorError> {
    match value {
        DELETE_EFFECT_TAG => Ok(ConnectorRowMutationEffect::Delete),
        REPLACE_EFFECT_TAG => Ok(ConnectorRowMutationEffect::Replace),
        INSERT_EFFECT_TAG => Ok(ConnectorRowMutationEffect::Insert),
        _ => Err(invalid_match("row-mutation effect tag is unknown")),
    }
}

fn cow_owned_decode_error(error: CowOwnedDecodeError) -> ConnectorError {
    match error {
        CowOwnedDecodeError::Codec(error) => cow_codec_error(error),
        CowOwnedDecodeError::Source(error) => error,
    }
}

fn cow_codec_error<E>(error: E) -> ConnectorError
where
    E: std::error::Error + novarocks_failure_contract::FailureProjection + Send + Sync + 'static,
{
    use novarocks_failure_contract::FailureClass;
    let kind = match error.failure_fact().class() {
        FailureClass::Resource => ConnectorErrorKind::ResourceExhausted,
        _ => ConnectorErrorKind::CorruptData,
    };
    ConnectorError::from_domain(kind, error)
}

fn cow_framing_error(error: RootRecordFramingError) -> ConnectorError {
    match error {
        RootRecordFramingError::Cow(error) => cow_codec_error(error),
        other => cow_codec_error(other),
    }
}

fn invalid_match(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Int8Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorMutationEffectField, ConnectorMutationSourceField,
        ConnectorMutationTargetField, ConnectorProviderBindingKey, ConnectorRequestContext,
        ConnectorTableHandle, ConnectorWriteBaseVersion, ConnectorWriteFieldToken,
        ProviderBindingEpoch,
    };

    use super::*;

    fn context(
        cancellation: Arc<novarocks_spi::connector::ConnectorStopOwner>,
        bytes: usize,
    ) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            cancellation.view(),
            1,
            bytes,
        )
        .unwrap()
    }

    fn contract() -> ConnectorMutationMatchContract {
        let owner = ConnectorProviderBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg").unwrap(),
            incarnation: ProviderBindingEpoch::from_bytes([4; 16]),
        };
        let table = ConnectorTableHandle::try_new(
            owner.instance_id.clone(),
            bytes::Bytes::from_static(b"t"),
        )
        .unwrap();
        let identity = ConnectorMutationSourceField::try_new_typed(
            ConnectorWriteFieldToken::from_bytes([1; 32]),
            Field::new("identity", DataType::Int32, false),
            0,
            novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
        )
        .expect("explicit mutation fixture declaration");
        let before = ConnectorMutationTargetField::try_new_typed(
            ConnectorWriteFieldToken::from_bytes([2; 32]),
            Field::new("before", DataType::Int32, true),
            1,
            novarocks_type_contract::FunctionValueType::new(DataType::Int32, true),
        )
        .expect("explicit mutation fixture declaration");
        let after = ConnectorMutationTargetField::try_new_typed(
            ConnectorWriteFieldToken::from_bytes([3; 32]),
            Field::new("after", DataType::Int32, true),
            2,
            novarocks_type_contract::FunctionValueType::new(DataType::Int32, true),
        )
        .expect("explicit mutation fixture declaration");
        let effect = ConnectorMutationEffectField::try_new(
            ConnectorWriteFieldToken::from_bytes([4; 32]),
            Field::new("effect", DataType::Int8, false),
            3,
        )
        .unwrap();
        ConnectorMutationMatchContract::try_new(
            owner,
            table,
            ConnectorWriteBaseVersion::try_new(bytes::Bytes::from_static(b"v")).unwrap(),
            vec![identity],
            vec![before],
            vec![after],
            vec![
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                ConnectorWriteFieldToken::from_bytes([2; 32]),
            ],
            effect,
        )
        .unwrap()
    }

    fn batch(rows: Vec<(i32, i32, Option<i32>, i8)>) -> RecordBatch {
        let schema = selection_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
                Arc::new(Int8Array::from(
                    rows.iter().map(|row| row.3).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn selection_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("identity", DataType::Int32, false),
            Field::new("before", DataType::Int32, true),
            Field::new("after", DataType::Int32, true),
            Field::new("effect", DataType::Int8, false),
        ]))
    }

    fn required_role_contract(before_nullable: bool) -> ConnectorMutationMatchContract {
        let original = contract();
        let target = |field: &ConnectorMutationTargetField, nullable| {
            ConnectorMutationTargetField::try_new_typed(
                field.token(),
                field.field().clone().with_nullable(nullable),
                field.target_ordinal(),
                field
                    .execution_type()
                    .unwrap()
                    .with_nullable(nullable)
                    .unwrap(),
            )
            .unwrap()
        };
        ConnectorMutationMatchContract::try_new(
            original.owner().clone(),
            original.table().clone(),
            original.base_version().clone(),
            original.identity_fields().to_vec(),
            vec![target(&original.before_fields()[0], before_nullable)],
            vec![target(&original.after_fields()[0], false)],
            original.uniqueness_tokens().to_vec(),
            original.effect_field().clone(),
        )
        .unwrap()
    }
    fn required_role_schema(before_nullable: bool) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("identity", DataType::Int32, false),
            Field::new("before", DataType::Int32, before_nullable),
            Field::new("after", DataType::Int32, false),
            Field::new("effect", DataType::Int8, false),
        ]))
    }
    fn frozen_sql_fields(
        nullable: [bool; 3],
    ) -> Vec<novarocks_query_application::api::ResultField> {
        nullable
            .into_iter()
            .enumerate()
            .map(|(ordinal, nullable)| {
                novarocks_query_application::api::ResultField::new(
                    format!("__nr_sel_{ordinal}"),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int32, nullable),
                )
            })
            .chain(std::iter::once(
                novarocks_query_application::api::ResultField::new(
                    "__nr_sel_3",
                    novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
                ),
            ))
            .collect()
    }
    fn sql_values(values: [Option<i32>; 3]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("sql_identity", DataType::Int32, true),
                Field::new("sql_before", DataType::Int32, true),
                Field::new("sql_after", DataType::Int32, true),
                Field::new("sql_effect", DataType::Int8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![values[0]])),
                Arc::new(Int32Array::from(vec![values[1]])),
                Arc::new(Int32Array::from(vec![values[2]])),
                Arc::new(Int8Array::from(vec![REPLACE_EFFECT_TAG])),
            ],
        )
        .unwrap()
    }
    fn constrained_cow_consumer(
        output: &[novarocks_query_application::api::ResultField],
    ) -> CowMatchRootConsumer {
        let schema = required_role_schema(false);
        let declarations = signed_layout::fixture_declarations(&schema);
        let mut consumer = CowMatchRootConsumer::try_new_declared(
            context(
                Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
                1 << 20,
            ),
            schema,
            declarations,
            required_role_contract(false),
            ConnectorRowMutationIntent::Merge {
                effects: vec![ConnectorRowMutationEffect::Replace],
            },
            output,
        )
        .unwrap();
        consumer.collector.local_assembly = true;
        consumer
    }

    #[test]
    fn nullable_sql_after_null_is_business_data_but_the_sql_promise_remains_strict() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        use novarocks_value_arrays::{ValueError, ValueViolation};
        let input = sql_values([Some(1), Some(10), None]);
        let stream = relayed_stream(std::slice::from_ref(&input));
        let mut consumer = constrained_cow_consumer(&frozen_sql_fields([false, false, true]));
        let error = consumer.push_body(&stream).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert!(
            error
                .to_string()
                .contains("COW assignment value does not satisfy its required target")
        );
        let mut consumer = constrained_cow_consumer(&frozen_sql_fields([false, false, false]));
        let error = consumer.push_body(&stream).unwrap_err();
        assert_eq!(
            error.failure_fact(),
            ValueError::Invariant(ValueViolation::RequiredNull).failure_fact()
        );
        // Complete equality may reuse the middle target check, never the
        // actual SQL promise above or the original Hydrate/selection authors.
        let mut consumer = constrained_cow_consumer(&frozen_sql_fields([false, false, false]));
        consumer
            .push_body(&relayed_stream(&[sql_values([
                Some(1),
                Some(10),
                Some(11),
            ])]))
            .unwrap();
        let selection = consumer.finish().unwrap();
        assert_eq!(
            selection.batches()[0]
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            11,
        );
    }

    #[test]
    fn required_string_after_rejects_a_valid_sql_null_as_business_data() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let original = required_role_contract(false);
        let field = &original.after_fields()[0];
        let contract = ConnectorMutationMatchContract::try_new(
            original.owner().clone(),
            original.table().clone(),
            original.base_version().clone(),
            original.identity_fields().to_vec(),
            original.before_fields().to_vec(),
            vec![
                ConnectorMutationTargetField::try_new_typed(
                    field.token(),
                    Field::new(field.field().name(), DataType::Utf8, false),
                    field.target_ordinal(),
                    novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
                )
                .unwrap(),
            ],
            original.uniqueness_tokens().to_vec(),
            original.effect_field().clone(),
        )
        .unwrap();
        let target = Arc::new(Schema::new(vec![
            Field::new("identity", DataType::Int32, false),
            Field::new("before", DataType::Int32, false),
            Field::new("after", DataType::Utf8, false),
            Field::new("effect", DataType::Int8, false),
        ]));
        let mut output = frozen_sql_fields([false, false, true]);
        output[2] = novarocks_query_application::api::ResultField::new(
            "__nr_sel_2",
            novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
        );
        let mut consumer = CowMatchRootConsumer::try_new_declared(
            context(
                Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
                1 << 20,
            ),
            Arc::clone(&target),
            signed_layout::fixture_declarations(&target),
            contract,
            ConnectorRowMutationIntent::Merge {
                effects: vec![ConnectorRowMutationEffect::Replace],
            },
            &output,
        )
        .unwrap();
        consumer.collector.local_assembly = true;
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("sql_identity", DataType::Int32, false),
                Field::new("sql_before", DataType::Int32, false),
                Field::new("sql_after", DataType::Utf8, true),
                Field::new("sql_effect", DataType::Int8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![10])),
                Arc::new(arrow::array::StringArray::from(vec![None::<&str>])),
                Arc::new(Int8Array::from(vec![REPLACE_EFFECT_TAG])),
            ],
        )
        .unwrap();
        let error = consumer.push_body(&relayed_stream(&[source])).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        let input =
            crate::query_execution::internal_consumer::domain_state::InternalDomainFailure::from(
                error,
            )
            .execution_input()
            .unwrap();
        assert_eq!(input.fact().class(), FailureClass::Data);
        assert_eq!(
            input.detail().as_str(),
            "statement execution failed in its coordinator"
        );
        assert_eq!(
            input.site(),
            novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator
        );
    }

    #[test]
    fn nullable_sql_identity_and_before_do_not_weaken_required_provider_invariants() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_value_arrays::{ValueError, ValueViolation};
        let expected = ValueError::Invariant(ValueViolation::RequiredNull).failure_fact();
        for values in [[None, Some(10), None], [Some(1), None, None]] {
            let mut consumer = constrained_cow_consumer(&frozen_sql_fields([true, true, true]));
            let error = consumer
                .push_body(&relayed_stream(&[sql_values(values)]))
                .unwrap_err();
            assert_eq!(error.failure_fact(), expected);
        }
    }

    #[test]
    fn optional_before_remains_legal_and_frozen_sql_column_order_is_explicit() {
        let schema = required_role_schema(true);
        let validator = RowMutationMatchValidator::try_new(
            required_role_contract(true),
            ConnectorRowMutationIntent::Merge {
                effects: vec![ConnectorRowMutationEffect::Replace],
            },
        )
        .unwrap();
        let output = frozen_sql_fields([false, true, true]);
        let capture =
            signed_layout::CowSqlOutput::try_new(&output, &schema, &validator, &mut |_| Ok(()))
                .unwrap();
        let input = sql_values([Some(1), None, Some(11)]);
        let projected = capture
            .validate_and_project(
                &input,
                &signed_layout::fixture_declarations(&schema),
                &mut |_| Ok(()),
            )
            .unwrap();
        assert!(projected.column(1).is_null(0));
        let output = frozen_sql_fields([false, false, true]);
        let permutation = [2, 1, 0, 3];
        let frozen = permutation
            .iter()
            .map(|&i| output[i].clone())
            .collect::<Vec<_>>();
        let input = sql_values([Some(1), Some(10), Some(11)])
            .project(&permutation)
            .unwrap();
        let mut consumer = constrained_cow_consumer(&frozen);
        consumer.push_body(&relayed_stream(&[input])).unwrap();
        let selection = consumer.finish().unwrap();
        let batch = &selection.batches()[0];
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            11
        );
    }

    #[test]
    fn malformed_sql_carrier_cannot_be_hidden_by_an_after_target_refusal() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_value_arrays::{ValueError, ValueViolation};
        let mut output = frozen_sql_fields([false, false, true]);
        output[2] = novarocks_query_application::api::ResultField::new(
            "__nr_sel_2",
            novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
        );
        let mut consumer = constrained_cow_consumer(&output);
        let error = consumer
            .push_body(&relayed_stream(&[sql_values([Some(1), Some(10), None])]))
            .unwrap_err();
        assert_eq!(
            error.failure_fact(),
            ValueError::Invariant(ValueViolation::Layout).failure_fact()
        );
    }

    #[test]
    fn collector_keeps_batches_separate_and_uses_smaller_memory_budget() {
        let cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let first = batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)]);
        let second = batch(vec![(2, 20, Some(21), REPLACE_EFFECT_TAG)]);
        let max_bytes = u64::try_from(
            ConnectorRowConversionFootprint::retained_schema_bytes(first.schema_ref()).unwrap()
                + ConnectorRowConversionFootprint::retained_batch_bytes(&first).unwrap()
                + ConnectorRowConversionFootprint::retained_batch_bytes(&second).unwrap(),
        )
        .unwrap();
        let mut collector = BoundedRowMutationMatchCollector::try_new(
            context(cancellation, usize::try_from(max_bytes + 10).unwrap()),
            Some(i64::try_from(max_bytes).unwrap()),
        )
        .unwrap();
        assert_eq!(collector.max_bytes(), max_bytes);
        assert_eq!(collector.max_rows(), max_bytes);
        collector.push(first).unwrap();
        collector.push(second).unwrap();
        let selection = collector.finish().unwrap();
        assert_eq!(selection.batches().len(), 2);
        assert_eq!(selection.row_count(), 2);
        assert_eq!(selection.byte_count(), max_bytes);
        assert_eq!(selection.schema(), &selection_schema());
    }

    #[test]
    fn collector_preserves_an_explicit_schema_for_an_empty_selection() {
        let schema = selection_schema();
        let collector = BoundedRowMutationMatchCollector::try_new_with_schema(
            context(
                Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
                1024,
            ),
            None,
            Arc::clone(&schema),
        )
        .unwrap();
        let selection = collector.finish().unwrap();
        assert_eq!(selection.schema(), &schema);
        assert!(selection.batches().is_empty());
        assert_eq!(selection.row_count(), 0);
        assert_eq!(
            selection.byte_count(),
            ConnectorRowConversionFootprint::retained_schema_bytes(schema.as_ref()).unwrap() as u64
        );
    }

    #[test]
    fn collector_rejects_schema_drift_and_untyped_empty_selection() {
        let cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let mut collector = BoundedRowMutationMatchCollector::try_new(
            context(Arc::clone(&cancellation), 4096),
            None,
        )
        .unwrap();
        collector
            .push(batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)]))
            .unwrap();
        let drifted = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int32,
            false,
        )])));
        let error = collector.push(drifted).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(collector.row_count(), 1);

        let untyped =
            BoundedRowMutationMatchCollector::try_new(context(cancellation, 4096), None).unwrap();
        let error = untyped.finish().unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn collector_rejects_budget_cancel_and_deadline_before_retaining_batch() {
        let cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let one = batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)]);
        let limit = ConnectorRowConversionFootprint::retained_schema_bytes(one.schema_ref())
            .unwrap()
            + ConnectorRowConversionFootprint::retained_batch_bytes(&one).unwrap();
        let mut collector = BoundedRowMutationMatchCollector::try_new(
            context(Arc::clone(&cancellation), limit),
            None,
        )
        .unwrap();
        collector.push(one.clone()).unwrap();
        let error = collector.push(one).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        cancellation.request_stop();
        let error = collector.finish().unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);

        let expired = ConnectorRequestContext::try_new(
            Instant::now() - Duration::from_millis(1),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1,
            32,
        )
        .unwrap();
        let error = BoundedRowMutationMatchCollector::try_new(expired, None)
            .unwrap()
            .push(batch(vec![]))
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
    }

    /// Encode inputs as the Backend's CowSelectionArrowV1 stream.
    fn relayed_stream(inputs: &[RecordBatch]) -> Vec<u8> {
        use novarocks_native_adapter::root_cow_selection_codec::{
            CowSelectionEncoder, CowSelectionTotals,
        };
        use novarocks_result_render::RenderTurnStatus;

        let mut totals = CowSelectionTotals::default();
        let mut stream = Vec::new();
        let mut buffer = vec![0_u8; 64];
        for input in inputs {
            let mut encoder = CowSelectionEncoder::try_new(input, totals, usize::MAX).unwrap();
            loop {
                let turn = encoder.step(&mut buffer);
                stream.extend_from_slice(&buffer[..turn.emitted_bytes]);
                if turn.status == RenderTurnStatus::InputComplete {
                    break;
                }
            }
            totals = encoder.totals();
        }
        stream
    }

    #[test]
    fn signed_layout_required_null_fails_with_typed_error_at_relay_boundary() {
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![None::<i32>]))],
        )
        .unwrap();
        let signed = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let stop = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let mut collector =
            RelayedCowSelectionCollector::try_new(context(stop, 1 << 20), None, signed).unwrap();
        let error = collector
            .push_body(&relayed_stream(std::slice::from_ref(&input)))
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        use novarocks_value_arrays::{ValueError, ValueViolation};
        use std::error::Error;
        let expected = ValueError::Invariant(ValueViolation::RequiredNull).failure_fact();
        let assert_original = |error: &ConnectorError| {
            assert_eq!(error.failure_fact(), expected);
            let shared = error
                .source()
                .unwrap()
                .downcast_ref::<SharedFailureCause>()
                .unwrap();
            let original = shared.downcast_ref::<ValueError>().unwrap();
            assert!(matches!(
                original,
                ValueError::Invariant(ValueViolation::RequiredNull)
            ));
            assert_eq!(original.failure_fact(), expected);
        };
        assert_original(&error);
        // Use the real four-column mutation contract at the RootConsumer boundary.
        let signed = selection_schema();
        let mut input_fields = signed
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        input_fields[0] = input_fields[0].clone().with_nullable(true);
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(input_fields)),
            vec![
                Arc::new(Int32Array::from(vec![None::<i32>])),
                Arc::new(Int32Array::from(vec![10])),
                Arc::new(Int32Array::from(vec![Some(11)])),
                Arc::new(Int8Array::from(vec![REPLACE_EFFECT_TAG])),
            ],
        )
        .unwrap();
        let mut consumer = CowMatchRootConsumer::try_new(
            context(
                Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
                1 << 20,
            ),
            signed,
            contract(),
            ConnectorRowMutationIntent::Merge {
                effects: vec![ConnectorRowMutationEffect::Replace],
            },
        )
        .unwrap();
        let error = consumer.push_body(&relayed_stream(&[input])).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(error.failure_fact(), expected);
        assert_original(&error);
    }

    #[test]
    fn relayed_selection_equals_the_arrow_selection_across_any_body_split() {
        let inputs = vec![
            batch(vec![
                (1, 10, Some(11), REPLACE_EFFECT_TAG),
                (2, 20, None, DELETE_EFFECT_TAG),
            ]),
            batch(vec![(3, 30, Some(31), INSERT_EFFECT_TAG)]),
        ];
        let stream = relayed_stream(&inputs);
        let cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let mut arrow = BoundedRowMutationMatchCollector::try_new_with_schema(
            context(Arc::clone(&cancellation), 1 << 20),
            None,
            selection_schema(),
        )
        .unwrap();
        for input in &inputs {
            arrow.push(input.clone()).unwrap();
        }
        let expected = arrow.finish().unwrap();
        assert!(!expected.has_owned_sources());
        for body in [1, 7, 33, stream.len()] {
            let mut relayed = RelayedCowSelectionCollector::try_new(
                context(Arc::clone(&cancellation), 1 << 20),
                None,
                selection_schema(),
            )
            .unwrap();
            for piece in stream.chunks(body) {
                relayed.push_body(piece).unwrap();
            }
            let selection = relayed.finish().unwrap();
            assert!(selection.has_owned_sources());
            assert_eq!(selection.digest(), expected.digest(), "body size {body}");
            assert_eq!(selection.batches(), expected.batches());
        }
        // An empty relayed stream keeps the signed schema.
        let empty = RelayedCowSelectionCollector::try_new(
            context(Arc::clone(&cancellation), 1 << 20),
            None,
            selection_schema(),
        )
        .unwrap()
        .finish()
        .unwrap();
        assert_eq!(empty.schema(), &selection_schema());
        assert_eq!(empty.row_count(), 0);
        assert!(empty.has_owned_sources());
    }

    #[test]
    fn collection_refuses_carrier_changes_without_retaining_the_new_batch() {
        let input = batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)]);
        let stream = relayed_stream(std::slice::from_ref(&input));
        let mut decoder = CowSelectionStreamDecoder::new();
        let schema_len = CowSelectionRecordHeader::parse(&stream)
            .unwrap()
            .record_bytes() as usize;
        assert!(
            decoder
                .apply_owned_record(&stream[..schema_len])
                .unwrap()
                .is_none()
        );
        let owned = decoder
            .apply_owned_record(&stream[schema_len..])
            .unwrap()
            .unwrap();
        let cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let mut collector = BoundedRowMutationMatchCollector::try_new_with_schema(
            context(cancellation, 1 << 20),
            None,
            selection_schema(),
        )
        .unwrap();
        collector.push_owned(owned).unwrap();
        let bytes = collector.byte_count();
        assert_eq!(
            collector.push(input).unwrap_err().kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(collector.byte_count(), bytes);
        assert_eq!(collector.row_count(), 1);
        assert!(collector.finish().unwrap().has_owned_sources());
    }

    #[test]
    fn relayed_selection_refuses_oversized_records_truncation_and_cancellation() {
        use novarocks_failure_contract::FailureProjection;
        let one = batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)]);
        let stream = relayed_stream(std::slice::from_ref(&one));
        let cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());

        // A record declaring more than the whole budget is refused from its
        // header alone, before any assembly buffer beyond the header.
        let schema_record = {
            let header = novarocks_native_adapter::root_cow_selection_codec::CowSelectionRecordHeader::parse(&stream)
                .unwrap();
            header.record_bytes() as usize
        };
        let batch_record = &stream[schema_record..];
        let budget = schema_record.max(
            ConnectorRowConversionFootprint::retained_schema_bytes(selection_schema().as_ref())
                .unwrap(),
        );
        let mut small = RelayedCowSelectionCollector::try_new(
            context(Arc::clone(&cancellation), budget),
            None,
            selection_schema(),
        )
        .unwrap();
        small.push_body(&stream[..schema_record]).unwrap();
        let mut grown = batch_record[..40].to_vec();
        grown[8..16].copy_from_slice(&((budget as u64) + 1).to_le_bytes());
        let error = small.push_body(&grown).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(
            error.failure_fact(),
            RootRecordFramingError::Capacity {
                requested: budget + 1,
                limit: budget,
            }
            .failure_fact()
        );
        let source = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
            .unwrap();
        assert!(matches!(
            source.downcast_ref::<RootRecordFramingError>(),
            Some(RootRecordFramingError::Capacity { requested, limit })
                if *requested == budget + 1 && *limit == budget
        ));
        assert!(small.assembly_bytes() <= 32, "{}", small.assembly_bytes());

        // End inside an unfinished record is refused.
        let mut truncated = RelayedCowSelectionCollector::try_new(
            context(Arc::clone(&cancellation), 1 << 20),
            None,
            selection_schema(),
        )
        .unwrap();
        truncated.push_body(&stream[..stream.len() - 1]).unwrap();
        let error = truncated.finish().unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert_eq!(
            error.failure_fact(),
            RootRecordFramingError::Incomplete.failure_fact()
        );

        // A complete record reaches the actual codec sink without erasing its
        // concrete cause through the generic framing callback.
        let mut malformed_schema = stream[..schema_record].to_vec();
        malformed_schema[32] = u8::MAX;
        let mut malformed = RelayedCowSelectionCollector::try_new(
            context(Arc::clone(&cancellation), 1 << 20),
            None,
            selection_schema(),
        )
        .unwrap();
        let error = malformed.push_body(&malformed_schema).unwrap_err();
        assert_eq!(
            error.failure_fact(),
            CowSelectionCodecError::MalformedSchema.failure_fact()
        );
        let source = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
            .unwrap();
        assert_eq!(
            source.downcast_ref::<CowSelectionCodecError>(),
            Some(&CowSelectionCodecError::MalformedSchema)
        );

        // The collector's own cancellation kind survives the relay sink.
        let mut cancelled = RelayedCowSelectionCollector::try_new(
            context(Arc::clone(&cancellation), 1 << 20),
            None,
            selection_schema(),
        )
        .unwrap();
        cancellation.request_stop();
        assert_eq!(
            cancelled.push_body(&stream).unwrap_err().kind(),
            ConnectorErrorKind::Cancelled
        );
    }

    #[test]
    fn frozen_cow_rows_refuse_before_decode_and_empty_batches_cannot_escape_the_count_bound() {
        let cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let mut collector = BoundedRowMutationMatchCollector::try_new_with_schema(
            context(Arc::clone(&cancellation), COW_SELECTION_BYTES as usize),
            None,
            selection_schema(),
        )
        .unwrap();
        assert_eq!(collector.max_bytes(), COW_SELECTION_BYTES);
        assert_eq!(collector.max_rows(), COW_SELECTION_ROWS);
        for _ in 0..novarocks_spi::connector::MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES {
            collector.push(batch(vec![])).unwrap();
        }
        assert_eq!(
            collector.push(batch(vec![])).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );

        let stream = relayed_stream(&[batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)])]);
        let schema_bytes = CowSelectionRecordHeader::parse(&stream)
            .unwrap()
            .record_bytes() as usize;
        let mut relayed = RelayedCowSelectionCollector::try_new(
            context(cancellation, COW_SELECTION_BYTES as usize),
            None,
            selection_schema(),
        )
        .unwrap();
        relayed.push_body(&stream[..schema_bytes]).unwrap();
        let retained_before_refusal = relayed.byte_count();
        assert!(retained_before_refusal > 0);
        let mut declared = stream[schema_bytes..].to_vec();
        declared[16..24].copy_from_slice(&(COW_SELECTION_ROWS + 1).to_le_bytes());
        // The payload still describes one row. ResourceExhausted proves the
        // declaration was rejected before the decoder would report malformed data.
        assert_eq!(
            relayed.push_body(&declared).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(relayed.byte_count(), retained_before_refusal);
    }

    #[test]
    fn production_cow_collector_without_author_is_refused_even_in_tests() {
        let mut collector = RelayedCowSelectionCollector::try_new_declared(
            context(
                Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
                COW_SELECTION_BYTES as usize,
            ),
            None,
            selection_schema(),
            signed_layout::fixture_declarations(&selection_schema()),
        )
        .unwrap();
        let stream = relayed_stream(&[batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)])]);
        let error = collector.push_body(&stream).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(collector.assembly_bytes(), 0);
        assert_eq!(collector.collector.row_count(), 0);
    }

    #[test]
    fn actual_root_budget_refusal_reaches_cow_consumer_as_original_work_source() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_workload_control::{ResultCapacityConfig, WorkError};
        use std::error::Error;
        let (_control, root, binding, _) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let window = binding.window_alias();
        let mut consumer = CowMatchRootConsumer::try_new_with_capacity(
            context(
                Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
                COW_SELECTION_BYTES as usize,
            ),
            selection_schema(),
            contract(),
            ConnectorRowMutationIntent::Merge {
                effects: vec![ConnectorRowMutationEffect::Replace],
            },
            &binding,
        )
        .unwrap();
        let original_reservation = window
            .reserve_payload_bytes(ResultCapacityConfig::V1.all_objects_bytes[2])
            .unwrap();
        let held = window.retained_payload_bytes();
        let stream = relayed_stream(&[batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)])]);
        let error = consumer.push_body(&stream).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
            .unwrap();
        assert_eq!(
            source.downcast_ref::<WorkError>(),
            Some(&WorkError::Capacity("retained result bytes"))
        );
        assert_eq!(
            error.failure_fact(),
            WorkError::Capacity("retained result bytes").failure_fact()
        );
        assert_eq!(consumer.collector.collector.row_count(), 0);
        assert_eq!(
            consumer.collector.assembly_bytes(),
            0,
            "complete borrowed records create no partial copy"
        );
        assert_eq!(window.retained_payload_bytes(), held);
        drop(consumer);
        assert_eq!(window.retained_payload_bytes(), held);
        drop(original_reservation);
        assert_eq!(window.retained_payload_bytes(), 0);
        drop((binding, window));
        root.owner.complete();
        root.business.release();
    }

    #[test]
    fn admitted_cow_window_reaches_empty_selection_and_last_arrow_alias() {
        use novarocks_workload_control::{
            ResourceConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
            WorkloadConfig, WorkloadControl,
        };
        for empty in [false, true] {
            let control = WorkloadControl::try_new(
                WorkloadConfig::default(),
                ResourceConfig {
                    total_bytes: 1024 * 1024,
                    control_bytes: 1024,
                    per_scope_bytes: 1024 * 1024 - 1024,
                },
            )
            .unwrap();
            let capacity = control
                .configure_result_capacity(ResultCapacityConfig::V1)
                .unwrap();
            control.mark_ready().unwrap();
            let (root, window) = control
                .root_admission()
                .try_begin_root_with_result(
                    WorkRequest::new(WorkClass::Management),
                    ResultWindowClass::Internal,
                )
                .unwrap();
            let binding = novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(
                &root.owner.scope(), window.retain_alias(),
            ).unwrap();
            let mut consumer = CowMatchRootConsumer::try_new_with_capacity(
                context(
                    Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
                    COW_SELECTION_BYTES as usize,
                ),
                selection_schema(),
                contract(),
                ConnectorRowMutationIntent::Merge {
                    effects: vec![ConnectorRowMutationEffect::Replace],
                },
                &binding,
            )
            .unwrap();
            if !empty {
                let stream = relayed_stream(&[batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)])]);
                consumer.push_body(&stream).unwrap();
            }
            consumer.check_end(if empty { 0 } else { 1 }).unwrap();
            let selection = consumer.finish().unwrap();
            let cloned = selection.clone();
            // RecordBatch's actual buffer clone is an independent last owner.
            let array = (!empty).then(|| selection.batches()[0].column(0).clone());
            drop(selection);
            drop(binding);
            drop(window);
            root.owner.complete();
            root.business.release();
            assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
            drop(cloned);
            if empty {
                assert_eq!(capacity.snapshot().held_positions, [0; 4]);
            } else {
                assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
                drop(array);
                assert_eq!(capacity.snapshot().held_positions, [0; 4]);
            }
        }
    }

    #[test]
    fn cow_factory_refuses_local_window_before_collector_growth() {
        use novarocks_workload_control::{
            ResourceConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
            WorkloadConfig, WorkloadControl,
        };
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let binding = novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(
            &root.owner.scope(), window.retain_alias(),
        ).unwrap();
        assert!(
            CowMatchRootConsumer::try_new_with_capacity(
                context(
                    Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
                    COW_SELECTION_BYTES as usize
                ),
                selection_schema(),
                contract(),
                ConnectorRowMutationIntent::Merge {
                    effects: vec![ConnectorRowMutationEffect::Replace]
                },
                &binding,
            )
            .is_err()
        );
        drop(binding);
        drop(window);
        root.owner.complete();
        root.business.release();
    }

    #[test]
    fn cow_consumer_checks_end_and_validates_target_uniqueness_before_handoff() {
        let make = || {
            CowMatchRootConsumer::try_new(
                context(
                    Arc::new(novarocks_spi::connector::ConnectorStopOwner::new()),
                    1 << 20,
                ),
                selection_schema(),
                contract(),
                ConnectorRowMutationIntent::Merge {
                    effects: vec![ConnectorRowMutationEffect::Replace],
                },
            )
            .unwrap()
        };
        let mut consumer = make();
        let stream = relayed_stream(&[batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)])]);
        for body in stream.chunks(7) {
            consumer.push_body(body).unwrap();
        }
        assert!(consumer.check_end(0).is_err());
        consumer.check_end(1).unwrap();
        assert_eq!(consumer.finish().unwrap().row_count(), 1);

        let mut duplicate = make();
        duplicate
            .push_body(&relayed_stream(&[batch(vec![
                (1, 10, Some(11), REPLACE_EFFECT_TAG),
                (1, 10, Some(12), REPLACE_EFFECT_TAG),
            ])]))
            .unwrap();
        duplicate.check_end(2).unwrap();
        assert!(
            duplicate
                .finish()
                .unwrap_err()
                .to_string()
                .contains("more than once")
        );

        let mut partial = make();
        partial.push_body(&stream[..stream.len() - 1]).unwrap();
        assert!(partial.check_end(1).is_err());
        assert!(partial.finish().is_err());
    }

    #[test]
    fn validator_uses_composite_canonical_keys_and_excludes_inserts() {
        let mut validator = RowMutationMatchValidator::try_new(
            contract(),
            ConnectorRowMutationIntent::Merge {
                effects: vec![
                    ConnectorRowMutationEffect::Delete,
                    ConnectorRowMutationEffect::Replace,
                    ConnectorRowMutationEffect::Insert,
                ],
            },
        )
        .unwrap();
        validator
            .validate_batch(&batch(vec![
                (1, 10, Some(11), REPLACE_EFFECT_TAG),
                (1, 11, Some(12), INSERT_EFFECT_TAG),
                (1, 11, Some(13), INSERT_EFFECT_TAG),
            ]))
            .unwrap();
        let error = validator
            .validate_batch(&batch(vec![(1, 10, Some(12), DELETE_EFFECT_TAG)]))
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn validator_fails_closed_for_null_unknown_effect_and_schema_mismatch() {
        let intent = ConnectorRowMutationIntent::Merge {
            effects: vec![
                ConnectorRowMutationEffect::Delete,
                ConnectorRowMutationEffect::Replace,
                ConnectorRowMutationEffect::Insert,
            ],
        };
        let mut null_validator =
            RowMutationMatchValidator::try_new(contract(), intent.clone()).unwrap();
        let null_batch = batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)]);
        let null_columns = vec![
            null_batch.column(0).clone(),
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
            null_batch.column(2).clone(),
            null_batch.column(3).clone(),
        ];
        let null_batch = RecordBatch::try_new(null_batch.schema(), null_columns).unwrap();
        assert_eq!(
            null_validator
                .validate_batch(&null_batch)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );

        let mut unknown_validator =
            RowMutationMatchValidator::try_new(contract(), intent.clone()).unwrap();
        assert_eq!(
            unknown_validator
                .validate_batch(&batch(vec![(1, 10, Some(11), 0)]))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );

        let bad_schema = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("identity", DataType::Int64, false),
                Field::new("before", DataType::Int32, true),
                Field::new("after", DataType::Int32, true),
                Field::new("effect", DataType::Int8, false),
            ])),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(11)])) as ArrayRef,
                Arc::new(Int8Array::from(vec![REPLACE_EFFECT_TAG])) as ArrayRef,
            ],
        )
        .unwrap();
        let mut schema_validator = RowMutationMatchValidator::try_new(contract(), intent).unwrap();
        assert_eq!(
            schema_validator
                .validate_batch(&bad_schema)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
}
