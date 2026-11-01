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
//! Hash join output gather and null-fill construction helpers.

use std::sync::Arc;

use arrow::array::{ArrayRef, UInt32Array, new_null_array};
use arrow::compute::take;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::type_compatibility::{check_exact, retag_column};
use crate::exec::chunk::{ArrayNormalizationStep, Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
use crate::runtime::fragment::ExecutionFailure;
use crate::runtime::profile::clamp_u128_to_i64;
use novarocks_type_contract::ValueTypeStorageLifetime;

#[derive(Debug)]
pub(crate) enum JoinGatherError {
    Source(ExecutionFailure),
    Construction(ConstructionFailure),
    Kernel(novarocks_functions::KernelFailure),
}
impl From<ExecutionFailure> for JoinGatherError {
    fn from(value: ExecutionFailure) -> Self {
        Self::Source(value)
    }
}
impl From<ConstructionFailure> for JoinGatherError {
    fn from(value: ConstructionFailure) -> Self {
        Self::Construction(value)
    }
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for JoinGatherError
{
    fn from(cause: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Construction(ConstructionFailure::Resource(cause))
    }
}
impl JoinGatherError {
    pub(crate) fn into_execution(self) -> ExecutionFailure {
        match self {
            Self::Source(cause) => cause,
            Self::Construction(cause) => cause.into(),
            Self::Kernel(cause) => cause.into(),
        }
    }
}

#[derive(Debug)]
struct JoinGatherContract(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for JoinGatherContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl std::error::Error for JoinGatherContract {}
impl novarocks_failure_contract::FailureProjection for JoinGatherContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::JoinGatherContract.reason(),
        )
    }
}
fn gather_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(JoinGatherContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
#[derive(Debug)]
pub(in crate::exec::operators::hashjoin) struct GatherKernelFailure(
    pub(in crate::exec::operators::hashjoin) arrow::error::ArrowError,
);
impl std::fmt::Display for GatherKernelFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "join gather kernel: {}", self.0)
    }
}
impl std::error::Error for GatherKernelFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for GatherKernelFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use arrow::error::ArrowError;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let (class, reason) = match self.0 {
            ArrowError::MemoryError(_) | ArrowError::ArithmeticOverflow(_) => (
                FailureClass::Resource,
                ExecutionReason::JoinGatherAllocation,
            ),
            ArrowError::OffsetOverflowError(_) => (
                FailureClass::Representation,
                ExecutionReason::JoinGatherOffset,
            ),
            ArrowError::IoError(..) => (
                FailureClass::Unexpected,
                ExecutionReason::JoinGatherUnexpected,
            ),
            _ => (FailureClass::Invariant, ExecutionReason::JoinGatherArrow),
        };
        let fact = FailureFact::new(class, reason.reason());
        if matches!(self.0, ArrowError::OffsetOverflowError(_)) {
            use novarocks_failure_contract::{
                RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
            };
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: std::num::NonZeroU16::new(27).unwrap(),
                },
                required: None,
                extent_limit: None,
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
fn gather_kernel(cause: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(GatherKernelFailure(cause))
}
fn copy_failure(cause: novarocks_functions::selected_copy::CopyError) -> JoinGatherError {
    JoinGatherError::Kernel(cause.into_kernel())
}
impl From<crate::exec::chunk::ChunkConstructionFailure> for JoinGatherError {
    fn from(cause: crate::exec::chunk::ChunkConstructionFailure) -> Self {
        Self::Source(cause.into())
    }
}
impl From<JoinGatherError> for ExecutionFailure {
    fn from(cause: JoinGatherError) -> Self {
        cause.into_execution()
    }
}

/// One owned batch and its actual declaration/header/source custody. This
/// stack carrier allocates no holder and exposes no bare owning batch escape.
/// Take/filter payload allocation is outside this wrapper's construction facts.
#[derive(Debug)]
pub(crate) struct TypedJoinBatch {
    batch: RecordBatch,
    schema: ChunkSchemaRef,
    storage: ValueTypeStorageLifetime,
}
impl TypedJoinBatch {
    pub(crate) fn record_batch(&self) -> &RecordBatch {
        &self.batch
    }
    pub(crate) fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
    pub(crate) fn filter(
        self,
        mask: &arrow::array::BooleanArray,
        owner: &dyn InputConstructionOwner,
    ) -> Result<Self, JoinGatherError> {
        preflight_filter_columns(owner, self.batch.num_columns())?;
        let storage = owner
            .storage_lifetime()
            .retaining_observed(&self.storage, |layout| owner.request(layout))?;
        let batch =
            arrow::compute::filter_record_batch(&self.batch, mask).map_err(gather_kernel)?;
        Ok(Self {
            batch,
            schema: self.schema,
            storage,
        })
    }
    pub(crate) fn republish(
        self,
        schema: ChunkSchemaRef,
        owner: &dyn InputConstructionOwner,
    ) -> Result<Self, JoinGatherError> {
        validate_projection(&self.schema, &schema, owner)?;
        let storage = owner
            .storage_lifetime()
            .retaining_observed(&self.storage, |layout| owner.request(layout))?;
        let (_, columns, rows) = self.batch.into_parts();
        let batch = strict_record_batch(&schema, columns, rows)?;
        Ok(Self {
            batch,
            schema,
            storage,
        })
    }
    pub(crate) fn into_chunk(
        self,
        owner: &dyn InputConstructionOwner,
    ) -> Result<Chunk, JoinGatherError> {
        Chunk::try_new_typed_record_batch_owned_observed::<JoinGatherError, _, _>(
            self.batch,
            self.schema,
            self.storage,
            owner.storage_lifetime(),
            |layout| owner.request(layout).map_err(Into::into),
            |step| owner.observe(0, &[], step).map_err(Into::into),
        )
    }
}

fn work(owner: &dyn InputConstructionOwner, units: usize) -> Result<(), JoinGatherError> {
    owner
        .observe(
            0,
            &[],
            ArrayNormalizationStep::InspectConstructionWork { units },
        )
        .map_err(Into::into)
}
fn request_array<T>(
    owner: &dyn InputConstructionOwner,
    count: usize,
) -> Result<(), JoinGatherError> {
    let layout = std::alloc::Layout::array::<T>(count).map_err(|_| {
        ConstructionFailure::Resource(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?;
    if layout.size() != 0 {
        owner.request(layout)?;
    }
    Ok(())
}
fn strict_record_batch(
    schema: &ChunkSchemaRef,
    columns: Vec<ArrayRef>,
    rows: usize,
) -> Result<RecordBatch, JoinGatherError> {
    // Arrow validates the declared exact carrier/required NULL law. No Field
    // retag or array-derived widening is a semantic author on this path.
    RecordBatch::try_new_with_options(
        schema.arrow_schema_ref(),
        columns,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|cause| crate::exec::chunk::ChunkConstructionFailure::record_batch(cause).into())
    .map_err(JoinGatherError::Source)
}
fn validate_projection(
    source: &ChunkSchema,
    target: &ChunkSchema,
    owner: &dyn InputConstructionOwner,
) -> Result<(), JoinGatherError> {
    if !source.is_typed() || !target.is_typed() || source.slots().len() != target.slots().len() {
        return Err(gather_contract(
            "typed join projection has no exact complete channel declaration",
        )
        .into());
    }
    for (source, target) in source.slots().iter().zip(target.slots()) {
        work(owner, 1)?;
        if source.nullable() && !target.nullable()
            || !source
                .typed_binding()
                .borrowed()
                .same_physical_binding_ignoring_root_nullability_observed(
                    target.typed_binding().borrowed(),
                    || work(owner, 1),
                )?
        {
            return Err(gather_contract(
                "typed join projection changes its complete value binding",
            )
            .into());
        }
    }
    Ok(())
}
/// Scope roots read child occurrences under their original slot identities.
/// The checked local join publisher instead declares a fresh canonical slot
/// namespace, ordered by probe/build ordinal. Neither role changes values.
#[derive(Clone, Copy)]
enum JoinSourceRole {
    Scope,
    CanonicalOutput,
}

fn validate_source(
    source: &Chunk,
    target: &ChunkSchema,
    offset: usize,
    role: JoinSourceRole,
    owner: &dyn InputConstructionOwner,
) -> Result<(), JoinGatherError> {
    if !source.chunk_schema().is_typed()
        || !target.is_typed()
        || offset
            .checked_add(source.chunk_schema().slots().len())
            .is_none_or(|end| end > target.slots().len())
    {
        return Err(
            gather_contract("typed join source has no complete declared channel range").into(),
        );
    }
    for (source, target) in source
        .chunk_schema()
        .slots()
        .iter()
        .zip(&target.slots()[offset..])
    {
        work(owner, 1)?;
        if matches!(role, JoinSourceRole::Scope) && source.slot_id() != target.slot_id()
            || source.nullable() && !target.nullable()
            || !source
                .typed_binding()
                .borrowed()
                .same_physical_binding_ignoring_root_nullability_observed(
                    target.typed_binding().borrowed(),
                    || work(owner, 1),
                )?
        {
            return Err(
                gather_contract("typed join source differs from its complete declaration").into(),
            );
        }
    }
    Ok(())
}
fn preflight_filter_columns(
    owner: &dyn InputConstructionOwner,
    columns: usize,
) -> Result<(), JoinGatherError> {
    work(owner, columns)?;
    // Arrow 60.1.0 FilterPredicate::filter_record_batch collects Result<Vec<_>>.
    // Rust 1.98 GenericShunt has lower size_hint 0; nonempty Arc items use the
    // Vec minimum capacity 4 and geometric doubling. This opaque upper covers
    // only that column vector, excluding FilterBuilder and array payload.
    if columns != 0 {
        let capacity = columns
            .checked_next_power_of_two()
            .ok_or(ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            ))?
            .max(4);
        let bytes = std::alloc::Layout::array::<ArrayRef>(capacity)
            .map_err(|_| {
                ConstructionFailure::Resource(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
            })?
            .size();
        owner.observe_opaque_upper_bound(bytes)?;
    }
    Ok(())
}

pub(crate) fn filter_probe_typed(
    probe: &Chunk,
    mask: &arrow::array::BooleanArray,
    schema: ChunkSchemaRef,
    owner: &dyn InputConstructionOwner,
) -> Result<TypedJoinBatch, JoinGatherError> {
    if probe.record_batch().num_columns() != schema.slots().len() {
        return Err(
            gather_contract("typed join filter channel count differs from declaration").into(),
        );
    }
    validate_source(probe, &schema, 0, JoinSourceRole::CanonicalOutput, owner)?;
    let storage = probe.source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
        owner.request(layout)
    })?;
    preflight_filter_columns(owner, probe.record_batch().num_columns())?;
    let batch =
        arrow::compute::filter_record_batch(probe.record_batch(), mask).map_err(gather_kernel)?;
    let (_, columns, rows) = batch.into_parts();
    let batch = strict_record_batch(&schema, columns, rows)?;
    Ok(TypedJoinBatch {
        batch,
        schema,
        storage,
    })
}

trait GatherAuthor {
    type Batch;
    fn columns(&self, count: usize) -> Result<(), JoinGatherError>;
    fn indexes(&self, indexes: &[u32]) -> Result<ArrayRef, JoinGatherError>;
    fn batches(&self, count: usize) -> Result<Vec<Self::Batch>, JoinGatherError>;
    fn finish(
        &self,
        columns: Vec<ArrayRef>,
        rows: usize,
        context: &str,
    ) -> Result<Self::Batch, JoinGatherError>;
}
struct LegacyGather<'a>(&'a SchemaRef);
impl GatherAuthor for LegacyGather<'_> {
    type Batch = RecordBatch;
    fn columns(&self, _: usize) -> Result<(), JoinGatherError> {
        Ok(())
    }
    fn indexes(&self, indexes: &[u32]) -> Result<ArrayRef, JoinGatherError> {
        Ok(Arc::new(UInt32Array::from(indexes.to_vec())))
    }
    fn batches(&self, _: usize) -> Result<Vec<RecordBatch>, JoinGatherError> {
        Ok(Vec::new())
    }
    fn finish(
        &self,
        columns: Vec<ArrayRef>,
        _: usize,
        context: &str,
    ) -> Result<RecordBatch, JoinGatherError> {
        build_output_record_batch(self.0, columns, context).map_err(Into::into)
    }
}
struct TypedGather<'a> {
    schema: &'a ChunkSchemaRef,
    owner: &'a dyn InputConstructionOwner,
    scratch: &'a dyn InputConstructionOwner,
    control: &'a dyn novarocks_functions::KernelEvaluationControl,
    storage: ValueTypeStorageLifetime,
}
impl GatherAuthor for TypedGather<'_> {
    type Batch = TypedJoinBatch;
    fn columns(&self, count: usize) -> Result<(), JoinGatherError> {
        request_array::<ArrayRef>(self.owner, count)
    }
    fn indexes(&self, indexes: &[u32]) -> Result<ArrayRef, JoinGatherError> {
        use novarocks_functions::selected_copy::{CopyConstructionError, CopyError};
        request_array::<u32>(self.scratch, indexes.len())?;
        let storage = self.scratch.storage_lifetime();
        let mut request = |layout| self.scratch.request(layout);
        let mut opaque = |bytes| self.scratch.observe_opaque_upper_bound(bytes);
        let mut creation =
            novarocks_type_contract::ValueSchemaCreation::new(storage, &mut request, &mut opaque);
        novarocks_functions::selected_copy::preflight_primitive_vec_adoption_construction(
            &mut creation,
            self.control,
        )
        .map_err(|cause| match cause {
            CopyConstructionError::Creator(cause) => JoinGatherError::Construction(cause),
            CopyConstructionError::Resource(cause) => {
                JoinGatherError::Construction(ConstructionFailure::Resource(cause))
            }
            CopyConstructionError::Copy(CopyError::Control(cause)) => {
                JoinGatherError::Kernel(cause)
            }
            CopyConstructionError::Copy(other) => copy_failure(other),
        })?;
        self.scratch.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<UInt32Array>(),
            )?,
        )?;
        Ok(Arc::new(UInt32Array::from(indexes.to_vec())))
    }
    fn batches(&self, count: usize) -> Result<Vec<TypedJoinBatch>, JoinGatherError> {
        request_array::<TypedJoinBatch>(self.owner, count)?;
        Ok(Vec::with_capacity(count))
    }
    fn finish(
        &self,
        columns: Vec<ArrayRef>,
        rows: usize,
        _: &str,
    ) -> Result<TypedJoinBatch, JoinGatherError> {
        let batch = strict_record_batch(self.schema, columns, rows)?;
        Ok(TypedJoinBatch {
            batch,
            schema: self.schema.clone(),
            storage: self.storage.clone(),
        })
    }
}

pub(crate) const MAX_JOIN_OUTPUT_ROWS_PER_BATCH: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct GatherTimings {
    pub(crate) build_ns: i64,
    pub(crate) probe_ns: i64,
}

#[derive(Debug)]
pub(crate) struct GatherBatches<B = RecordBatch> {
    pub(crate) batches: Vec<B>,
    #[cfg(test)]
    pub(crate) timings: GatherTimings,
}

impl GatherTimings {
    fn add_build(&mut self, start: std::time::Instant) {
        self.build_ns = self
            .build_ns
            .saturating_add(clamp_u128_to_i64(start.elapsed().as_nanos()));
    }

    fn add_probe(&mut self, start: std::time::Instant) {
        self.probe_ns = self
            .probe_ns
            .saturating_add(clamp_u128_to_i64(start.elapsed().as_nanos()));
    }
}

fn normalize_columns_to_schema(
    output_schema: &SchemaRef,
    columns: Vec<ArrayRef>,
    context: &str,
) -> Result<Vec<ArrayRef>, ExecutionFailure> {
    if output_schema.fields().len() != columns.len() {
        return Err(gather_contract(format_args!(
            "{context} column count mismatch: schema_fields={} arrays={}",
            output_schema.fields().len(),
            columns.len()
        )));
    }

    let mut normalized = Vec::with_capacity(columns.len());
    for (idx, column) in columns.into_iter().enumerate() {
        let descriptor = output_schema.field(idx).data_type();
        if descriptor == column.data_type() {
            normalized.push(column);
            continue;
        }

        check_exact(descriptor, column.data_type()).map_err(ExecutionFailure::from_domain)?;
        let column = retag_column(&column, descriptor).map_err(ExecutionFailure::from_domain)?;
        normalized.push(column);
    }
    Ok(normalized)
}

fn build_output_record_batch(
    output_schema: &SchemaRef,
    columns: Vec<ArrayRef>,
    context: &str,
) -> Result<RecordBatch, ExecutionFailure> {
    let columns = normalize_columns_to_schema(output_schema, columns, context)?;
    let schema = output_schema_for_columns(output_schema, &columns);
    RecordBatch::try_new(schema, columns)
        .map_err(|cause| crate::exec::chunk::ChunkConstructionFailure::record_batch(cause).into())
}

fn output_schema_for_columns(output_schema: &SchemaRef, columns: &[ArrayRef]) -> SchemaRef {
    let mut changed = false;
    let fields = output_schema
        .fields()
        .iter()
        .zip(columns.iter())
        .map(|(field, column)| {
            if !field.is_nullable() && column.null_count() > 0 {
                changed = true;
                field.as_ref().clone().with_nullable(true)
            } else {
                field.as_ref().clone()
            }
        })
        .collect::<Vec<_>>();
    if !changed {
        return Arc::clone(output_schema);
    }
    Arc::new(Schema::new_with_metadata(
        fields,
        output_schema.metadata().clone(),
    ))
}

pub(crate) fn gather_join_batch(
    left: &Chunk,
    right: &Chunk,
    left_indices: &[u32],
    right_indices: &[u32],
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, ExecutionFailure> {
    gather_join_batch_in(
        left,
        right,
        left_indices,
        right_indices,
        &LegacyGather(output_schema),
    )
    .map_err(JoinGatherError::into_execution)
}

fn gather_join_batch_in<A: GatherAuthor>(
    left: &Chunk,
    right: &Chunk,
    left_indices: &[u32],
    right_indices: &[u32],
    author: &A,
) -> Result<Option<A::Batch>, JoinGatherError> {
    if left_indices.len() != right_indices.len() {
        return Err(gather_contract(format_args!(
            "join index length mismatch: left={} right={}",
            left_indices.len(),
            right_indices.len()
        ))
        .into());
    }
    if left_indices.is_empty() || right_indices.is_empty() {
        return Ok(None);
    }
    let left_idx_ref = author.indexes(left_indices)?;
    let right_idx_ref = author.indexes(right_indices)?;
    let count = left.record_batch().num_columns() + right.record_batch().num_columns();
    author.columns(count)?;
    let mut columns = Vec::with_capacity(count);
    for col in left.record_batch().columns() {
        let taken = take(col.as_ref(), &left_idx_ref, None).map_err(gather_kernel)?;
        columns.push(taken);
    }
    for col in right.record_batch().columns() {
        let taken = take(col.as_ref(), &right_idx_ref, None).map_err(gather_kernel)?;
        columns.push(taken);
    }

    let batch = author.finish(columns, left_indices.len(), "join output")?;
    Ok(Some(batch))
}

#[allow(
    dead_code,
    reason = "Retained for alternate hash-join gather paths under integration coverage."
)]
pub(crate) fn gather_join_batches(
    left: &Chunk,
    right: &Chunk,
    left_indices: &[u32],
    right_indices: &[u32],
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, ExecutionFailure> {
    if left_indices.len() != right_indices.len() {
        return Err(gather_contract(format_args!(
            "join index length mismatch: left={} right={}",
            left_indices.len(),
            right_indices.len()
        )));
    }
    if left_indices.is_empty() {
        return Ok(Vec::new());
    }

    let mut batches = Vec::new();
    let mut offset = 0usize;
    while offset < left_indices.len() {
        let end = (offset + MAX_JOIN_OUTPUT_ROWS_PER_BATCH).min(left_indices.len());
        if let Some(batch) = gather_join_batch(
            left,
            right,
            &left_indices[offset..end],
            &right_indices[offset..end],
            output_schema,
        )? {
            batches.push(batch);
        }
        offset = end;
    }
    Ok(batches)
}

#[cfg(test)]
pub(crate) fn gather_probe_build_batches(
    probe: &Chunk,
    build: &Chunk,
    probe_indices: &[u32],
    build_indices: &[u32],
    output_schema: &SchemaRef,
    probe_is_left: bool,
    all_match_one: bool,
) -> Result<GatherBatches, ExecutionFailure> {
    gather_probe_build_batches_in(
        probe,
        build,
        probe_indices,
        build_indices,
        probe_is_left,
        all_match_one,
        &LegacyGather(output_schema),
    )
    .map_err(JoinGatherError::into_execution)
}

fn gather_probe_build_batches_in<A: GatherAuthor>(
    probe: &Chunk,
    build: &Chunk,
    probe_indices: &[u32],
    build_indices: &[u32],
    probe_is_left: bool,
    all_match_one: bool,
    author: &A,
) -> Result<GatherBatches<A::Batch>, JoinGatherError> {
    if probe_indices.len() != build_indices.len() {
        return Err(gather_contract(format_args!(
            "join index length mismatch: probe={} build={}",
            probe_indices.len(),
            build_indices.len()
        ))
        .into());
    }
    if probe_indices.is_empty() {
        return Ok(GatherBatches {
            batches: Vec::new(),
            #[cfg(test)]
            timings: GatherTimings::default(),
        });
    }

    if all_match_one {
        for (row, probe_row) in probe_indices.iter().enumerate() {
            if *probe_row != row as u32 {
                return Err(gather_contract(format_args!(
                    "ALL_MATCH_ONE probe index mismatch: row={} probe_row={}",
                    row, probe_row
                ))
                .into());
            }
        }
    }

    let mut batches =
        author.batches(probe_indices.len().div_ceil(MAX_JOIN_OUTPUT_ROWS_PER_BATCH))?;
    let mut timings = GatherTimings::default();
    let mut offset = 0usize;
    while offset < probe_indices.len() {
        let end = (offset + MAX_JOIN_OUTPUT_ROWS_PER_BATCH).min(probe_indices.len());
        let row_count = end - offset;
        let build_idx_ref = author.indexes(&build_indices[offset..end])?;
        let count = probe.record_batch().num_columns() + build.record_batch().num_columns();
        author.columns(count)?;
        let mut columns = Vec::with_capacity(count);
        let take_probe_columns = |columns: &mut Vec<ArrayRef>,
                                  timings: &mut GatherTimings|
         -> Result<(), JoinGatherError> {
            if all_match_one {
                if probe.record_batch().num_columns() > 0 {
                    let start = std::time::Instant::now();
                    for col in probe.record_batch().columns() {
                        columns.push(col.slice(offset, row_count));
                    }
                    timings.add_probe(start);
                }
            } else {
                let probe_idx_ref = author.indexes(&probe_indices[offset..end])?;
                if probe.record_batch().num_columns() > 0 {
                    let start = std::time::Instant::now();
                    for col in probe.record_batch().columns() {
                        columns
                            .push(take(col.as_ref(), &probe_idx_ref, None).map_err(gather_kernel)?);
                    }
                    timings.add_probe(start);
                }
            }
            Ok(())
        };
        let take_build_columns = |columns: &mut Vec<ArrayRef>,
                                  timings: &mut GatherTimings|
         -> Result<(), JoinGatherError> {
            if build.record_batch().num_columns() > 0 {
                let start = std::time::Instant::now();
                for col in build.record_batch().columns() {
                    columns.push(take(col.as_ref(), &build_idx_ref, None).map_err(gather_kernel)?);
                }
                timings.add_build(start);
            }
            Ok(())
        };

        if probe_is_left {
            take_probe_columns(&mut columns, &mut timings)?;
            take_build_columns(&mut columns, &mut timings)?;
        } else {
            take_build_columns(&mut columns, &mut timings)?;
            take_probe_columns(&mut columns, &mut timings)?;
        }
        batches.push(author.finish(columns, row_count, "join all-match-one output")?);
        offset = end;
    }
    Ok(GatherBatches {
        batches,
        #[cfg(test)]
        timings,
    })
}

#[cfg(test)]
pub(crate) fn gather_left_with_null_right(
    left: &Chunk,
    left_indices: &[u32],
    right_schema: &SchemaRef,
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, ExecutionFailure> {
    gather_left_with_null_right_in(
        left,
        left_indices,
        right_schema,
        &LegacyGather(output_schema),
    )
    .map_err(JoinGatherError::into_execution)
}

fn gather_left_with_null_right_in<A: GatherAuthor>(
    left: &Chunk,
    left_indices: &[u32],
    right_schema: &SchemaRef,
    author: &A,
) -> Result<Option<A::Batch>, JoinGatherError> {
    if left_indices.is_empty() {
        return Ok(None);
    }
    let len = left_indices.len();
    let left_idx_ref = author.indexes(left_indices)?;
    let count = left.record_batch().num_columns() + right_schema.fields().len();
    author.columns(count)?;
    let mut columns = Vec::with_capacity(count);
    for col in left.record_batch().columns() {
        let taken = take(col.as_ref(), &left_idx_ref, None).map_err(gather_kernel)?;
        columns.push(taken);
    }
    for field in right_schema.fields().iter() {
        columns.push(new_null_array(field.data_type(), len));
    }

    let batch = author.finish(columns, len, "join left outer output")?;
    Ok(Some(batch))
}

fn typed_pair_author<'a>(
    left: &Chunk,
    right: &Chunk,
    schema: &'a ChunkSchemaRef,
    owner: &'a dyn InputConstructionOwner,
    scratch: &'a dyn InputConstructionOwner,
    control: &'a dyn novarocks_functions::KernelEvaluationControl,
) -> Result<TypedGather<'a>, JoinGatherError> {
    if left.record_batch().num_columns() + right.record_batch().num_columns()
        != schema.slots().len()
    {
        return Err(
            gather_contract("typed join pair channel count differs from declaration").into(),
        );
    }
    validate_source(left, schema, 0, JoinSourceRole::Scope, owner)?;
    validate_source(
        right,
        schema,
        left.record_batch().num_columns(),
        JoinSourceRole::Scope,
        owner,
    )?;
    let storage = left.source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
        owner.request(layout)
    })?;
    let storage =
        right.source_storage_lifetime_observed(storage, |layout| owner.request(layout))?;
    Ok(TypedGather {
        schema,
        owner,
        scratch,
        control,
        storage,
    })
}

pub(crate) fn gather_join_batch_typed(
    left: &Chunk,
    right: &Chunk,
    left_indices: &[u32],
    right_indices: &[u32],
    schema: &ChunkSchemaRef,
    owner: &dyn InputConstructionOwner,
    scratch: &dyn InputConstructionOwner,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> Result<Option<TypedJoinBatch>, JoinGatherError> {
    let author = typed_pair_author(left, right, schema, owner, scratch, control)?;
    gather_join_batch_in(left, right, left_indices, right_indices, &author)
}

pub(crate) fn gather_probe_build_batches_typed(
    probe: &Chunk,
    build: &Chunk,
    probe_indices: &[u32],
    build_indices: &[u32],
    schema: &ChunkSchemaRef,
    probe_is_left: bool,
    all_match_one: bool,
    owner: &dyn InputConstructionOwner,
    scratch: &dyn InputConstructionOwner,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> Result<GatherBatches<TypedJoinBatch>, JoinGatherError> {
    let (left, right) = if probe_is_left {
        (probe, build)
    } else {
        (build, probe)
    };
    let author = typed_pair_author(left, right, schema, owner, scratch, control)?;
    gather_probe_build_batches_in(
        probe,
        build,
        probe_indices,
        build_indices,
        probe_is_left,
        all_match_one,
        &author,
    )
}

pub(crate) fn gather_left_with_null_right_typed(
    left: &Chunk,
    left_indices: &[u32],
    right: &ChunkSchemaRef,
    schema: &ChunkSchemaRef,
    owner: &dyn InputConstructionOwner,
    scratch: &dyn InputConstructionOwner,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> Result<Option<TypedJoinBatch>, JoinGatherError> {
    if left.record_batch().num_columns() + right.slots().len() != schema.slots().len()
        || !right.is_typed()
    {
        return Err(
            gather_contract("typed outer join channel count differs from declaration").into(),
        );
    }
    validate_source(left, schema, 0, JoinSourceRole::CanonicalOutput, owner)?;
    for (source, target) in right
        .slots()
        .iter()
        .zip(&schema.slots()[left.record_batch().num_columns()..])
    {
        // Canonical output slots are intentionally fresh; this producer maps
        // right occurrences by their checked ordinal, preserving the complete
        // child semantics/layout and its explicitly frozen NULL extension.
        if !target.nullable()
            || !source
                .typed_binding()
                .borrowed()
                .same_physical_binding_ignoring_root_nullability_observed(
                    target.typed_binding().borrowed(),
                    || work(owner, 1),
                )?
        {
            return Err(gather_contract(
                "typed outer join NULL extension differs from its complete declaration",
            )
            .into());
        }
    }
    let storage = left.source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
        owner.request(layout)
    })?;
    // new_null_array borrows the prepared right layout. Retain precisely that
    // schema, whose slots already carry its finite creation responsibility.
    let storage =
        storage.retaining_observed(&right.storage_lifetime(), |layout| owner.request(layout))?;
    let author = TypedGather {
        schema,
        owner,
        scratch,
        control,
        storage,
    };
    gather_left_with_null_right_in(left, left_indices, &right.arrow_schema_ref(), &author)
}

#[cfg(test)]
pub(crate) fn gather_null_left_with_right(
    right: &Chunk,
    right_indices: &[u32],
    left_schema: &SchemaRef,
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, ExecutionFailure> {
    if right_indices.is_empty() {
        return Ok(None);
    }
    let len = right_indices.len();
    let right_idx_array = UInt32Array::from(right_indices.to_vec());
    let right_idx_ref = Arc::new(right_idx_array) as ArrayRef;

    let mut columns =
        Vec::with_capacity(left_schema.fields().len() + right.record_batch().num_columns());
    for field in left_schema.fields().iter() {
        columns.push(new_null_array(field.data_type(), len));
    }
    for col in right.record_batch().columns() {
        let taken = take(col.as_ref(), &right_idx_ref, None).map_err(gather_kernel)?;
        columns.push(taken);
    }

    let batch = build_output_record_batch(output_schema, columns, "join right outer output")?;
    Ok(Some(batch))
}

#[cfg(test)]
mod tests {
    use novarocks_failure_contract::FailureProjection;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Decimal128Array, Int32Array, ListArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::{RecordBatch, RecordBatchOptions};

    use crate::exec::chunk::{Chunk, ChunkSchema};
    use novarocks_types::SlotId;

    use super::{
        MAX_JOIN_OUTPUT_ROWS_PER_BATCH, gather_join_batches, gather_left_with_null_right,
        gather_null_left_with_right,
    };

    fn one_column_chunk(name: &str, slot_id: SlotId, values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values)) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot_id])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn decimal_chunk(name: &str, slot_id: SlotId, precision: u8, scale: i8) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(
            name,
            DataType::Decimal128(precision, scale),
            false,
        )]));
        let array = Arc::new(
            Decimal128Array::from(vec![Some(123_i128)])
                .with_precision_and_scale(precision, scale)
                .expect("decimal type"),
        ) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot_id])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn empty_chunk(row_count: usize) -> Chunk {
        let schema = Arc::new(Schema::empty());
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        let batch = RecordBatch::try_new_with_options(Arc::clone(&schema), Vec::new(), &options)
            .expect("empty record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[])
            .expect("empty chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn join_schema(left_name: &str, right_name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(left_name, DataType::Int32, true),
            Field::new(right_name, DataType::Int32, true),
        ]))
    }

    fn int32_values(batch: &RecordBatch, column: usize) -> Vec<Option<i32>> {
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        (0..batch.num_rows())
            .map(|row| {
                if array.is_null(row) {
                    None
                } else {
                    Some(array.value(row))
                }
            })
            .collect()
    }

    #[test]
    fn gather_kernel_preserves_actual_arrow_source() {
        use std::error::Error;
        let values = arrow::array::Int32Array::from(vec![7]);
        let indexes = arrow::array::UInt32Array::from(vec![3]);
        let cause = arrow::compute::take(
            &values,
            &indexes,
            Some(arrow::compute::TakeOptions { check_bounds: true }),
        )
        .unwrap_err();
        let wrapped = super::GatherKernelFailure(cause);
        assert!(
            wrapped
                .source()
                .unwrap()
                .downcast_ref::<arrow::error::ArrowError>()
                .is_some()
        );
        let fact = wrapped.failure_fact();
        assert_eq!(
            fact.class(),
            novarocks_failure_contract::FailureClass::Invariant
        );
        let error = crate::runtime::fragment::ExecutionFailure::from_domain(wrapped);
        assert_eq!(error.failure_fact(), fact);
    }

    #[test]
    fn gather_projection_uses_arrow_variant_and_keeps_unknown_extents() {
        use novarocks_failure_contract::FailureClass;
        let allocation = super::GatherKernelFailure(arrow::error::ArrowError::MemoryError(
            "cancelled; invalid input".into(),
        ));
        assert_eq!(allocation.failure_fact().class(), FailureClass::Resource);
        let offset = super::GatherKernelFailure(arrow::error::ArrowError::OffsetOverflowError(0));
        let fact = offset.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        let representation = fact.representation().unwrap();
        assert_eq!(representation.required, None);
        assert_eq!(representation.extent_limit, None);
        let invalid = super::GatherKernelFailure(arrow::error::ArrowError::InvalidArgumentError(
            "out of memory".into(),
        ));
        assert_eq!(invalid.failure_fact().class(), FailureClass::Invariant);
    }

    #[test]
    fn gather_probe_build_batches_directs_probe_when_all_match_one() {
        let left = one_column_chunk("l", SlotId::new(1), vec![1, 2, 3]);
        let right = one_column_chunk("r", SlotId::new(2), vec![10, 20, 30]);
        let output_schema = join_schema("l", "r");
        let probe_indices = vec![0, 1, 2];
        let build_indices = vec![2, 0, 1];

        let out = super::gather_probe_build_batches(
            &left,
            &right,
            &probe_indices,
            &build_indices,
            &output_schema,
            true,
            true,
        )
        .expect("gather");

        assert_eq!(out.batches.len(), 1);
        assert_eq!(out.batches[0].num_rows(), 3);
        assert_eq!(
            int32_values(&out.batches[0], 0),
            vec![Some(1), Some(2), Some(3)]
        );
        assert_eq!(
            int32_values(&out.batches[0], 1),
            vec![Some(30), Some(10), Some(20)]
        );
    }

    #[test]
    fn gather_probe_build_batches_preserves_output_order_when_probe_is_right() {
        let probe = one_column_chunk("r", SlotId::new(2), vec![10, 20, 30]);
        let build = one_column_chunk("l", SlotId::new(1), vec![1, 2, 3]);
        let output_schema = join_schema("l", "r");
        let probe_indices = vec![0, 1, 2];
        let build_indices = vec![2, 0, 1];

        let out = super::gather_probe_build_batches(
            &probe,
            &build,
            &probe_indices,
            &build_indices,
            &output_schema,
            false,
            true,
        )
        .expect("gather");

        assert_eq!(out.batches.len(), 1);
        assert_eq!(out.batches[0].num_rows(), 3);
        assert_eq!(
            int32_values(&out.batches[0], 0),
            vec![Some(3), Some(1), Some(2)]
        );
        assert_eq!(
            int32_values(&out.batches[0], 1),
            vec![Some(10), Some(20), Some(30)]
        );
    }

    #[test]
    fn gather_probe_build_batches_does_not_charge_empty_build_side_when_all_match_one() {
        let probe = one_column_chunk("l", SlotId::new(1), vec![1, 2, 3]);
        let build = empty_chunk(3);
        let output_schema = Arc::new(Schema::new(vec![Field::new("l", DataType::Int32, true)]));
        let probe_indices = vec![0, 1, 2];
        let build_indices = vec![0, 1, 2];

        let out = super::gather_probe_build_batches(
            &probe,
            &build,
            &probe_indices,
            &build_indices,
            &output_schema,
            true,
            true,
        )
        .expect("gather");

        assert_eq!(out.batches.len(), 1);
        assert_eq!(out.batches[0].num_rows(), 3);
        assert_eq!(out.timings.build_ns, 0);
    }

    #[test]
    fn gather_join_batches_splits_large_candidate_output() {
        let rows = MAX_JOIN_OUTPUT_ROWS_PER_BATCH + 1;
        let left = one_column_chunk("l", SlotId::new(1), (0..rows).map(|i| i as i32).collect());
        let right = one_column_chunk("r", SlotId::new(2), vec![7]);
        let left_indices = (0..rows).map(|i| i as u32).collect::<Vec<_>>();
        let right_indices = vec![0u32; rows];

        let batches = gather_join_batches(
            &left,
            &right,
            &left_indices,
            &right_indices,
            &join_schema("l", "r"),
        )
        .expect("join batches");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), MAX_JOIN_OUTPUT_ROWS_PER_BATCH);
        assert_eq!(batches[1].num_rows(), 1);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            rows
        );
    }

    #[test]
    fn gather_join_batch_rejects_decimal_precision_drift() {
        let left = decimal_chunk("l", SlotId::new(1), 38, 2);
        let right = one_column_chunk("r", SlotId::new(2), vec![7]);
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("l", DataType::Decimal128(10, 2), false),
            Field::new("r", DataType::Int32, true),
        ]));

        let err = super::gather_join_batch(&left, &right, &[0], &[0], &output_schema)
            .expect_err("join output must reject actual-widen decimal");

        assert!(
            err.failure_fact().class() == novarocks_failure_contract::FailureClass::Invariant,
            "err={err}"
        );
        assert!(err.detail().contains("Decimal128(10, 2)"), "err={err}");
        assert!(err.detail().contains("Decimal128(38, 2)"), "err={err}");
    }

    #[test]
    fn build_output_record_batch_retags_nested_metadata_to_descriptor() {
        let actual_item = Arc::new(Field::new("item", DataType::Int32, true).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "3".to_string())]),
        ));
        let actual = Arc::new(ListArray::new(
            actual_item,
            OffsetBuffer::from_lengths([2]),
            Arc::new(Int32Array::from(vec![1, 2])),
            None::<NullBuffer>,
        )) as ArrayRef;
        let descriptor_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "arr",
            descriptor_type.clone(),
            true,
        )]));

        let batch = super::build_output_record_batch(&output_schema, vec![actual], "join output")
            .expect("retagged output");

        assert_eq!(batch.schema().field(0).data_type(), &descriptor_type);
        assert_eq!(batch.column(0).data_type(), &descriptor_type);
    }

    #[test]
    fn gather_left_with_null_right_preserves_row_count() {
        let left = one_column_chunk("l", SlotId::new(1), vec![10, 20, 30, 40]);
        let right_schema = Arc::new(Schema::new(vec![Field::new("r", DataType::Int32, true)]));
        let left_indices = vec![3, 0, 3];

        let batch = gather_left_with_null_right(
            &left,
            &left_indices,
            &right_schema,
            &join_schema("l", "r"),
        )
        .expect("left null right")
        .expect("batch");

        assert_eq!(batch.num_rows(), left_indices.len());
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.column(1).len(), left_indices.len());
        assert_eq!(batch.column(1).null_count(), left_indices.len());
    }

    #[test]
    fn gather_left_with_null_right_widens_null_filled_output_field() {
        let left = one_column_chunk("l", SlotId::new(1), vec![10, 20, 30, 40]);
        let right_schema = Arc::new(Schema::new(vec![Field::new("r", DataType::Int32, false)]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("l", DataType::Int32, false),
            Field::new("r", DataType::Int32, false),
        ]));
        let left_indices = vec![3, 0, 3];

        let batch =
            gather_left_with_null_right(&left, &left_indices, &right_schema, &output_schema)
                .expect("left null right")
                .expect("batch");

        assert!(!batch.schema().field(0).is_nullable());
        assert!(batch.schema().field(1).is_nullable());
        assert_eq!(batch.column(1).null_count(), left_indices.len());
    }

    #[test]
    fn gather_null_left_with_right_preserves_row_count() {
        let right = one_column_chunk("r", SlotId::new(2), vec![100, 200, 300, 400]);
        let left_schema = Arc::new(Schema::new(vec![Field::new("l", DataType::Int32, true)]));
        let right_indices = vec![2, 1, 2];

        let batch = gather_null_left_with_right(
            &right,
            &right_indices,
            &left_schema,
            &join_schema("l", "r"),
        )
        .expect("null left right")
        .expect("batch");

        assert_eq!(batch.num_rows(), right_indices.len());
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.column(0).len(), right_indices.len());
        assert_eq!(batch.column(0).null_count(), right_indices.len());
    }
}
