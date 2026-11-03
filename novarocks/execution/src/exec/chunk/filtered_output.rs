// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The direct ordered-scan filter's one unpublished output transaction.
//! Buffer identity measures new stock only. The original input and its actual
//! authority remain in the same paid backing through every returned alias.

use super::{
    AdditionalBufferLedger, Chunk, OutputColumnMetadata, record_batch_additional_bytes_observed,
    retain_column_capacity,
};
use crate::exec::arrow_work::{ArrowWorkBudget, ArrowWorkShape};
use crate::exec::pipeline::operator::InputConstructionOwner;
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, RecordBatch, RecordBatchOptions,
};
use arrow::compute::filter_record_batch;
use novarocks_functions::opaque_memory::OpaqueRetainedCharge;
use novarocks_functions::{
    AggregateStateAllocator, EvaluationCheckpoints, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, PreparedSourceCustody, SourceBackingOwner, source_custody_prefix_facts,
};
use std::sync::Arc;

#[derive(Debug)]
struct FilteredOutputKernelFailure(arrow::error::ArrowError);
impl std::fmt::Display for FilteredOutputKernelFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ordered filtered output kernel: {}", self.0)
    }
}
impl std::error::Error for FilteredOutputKernelFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for FilteredOutputKernelFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use arrow::error::ArrowError;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let (class, reason) = match self.0 {
            ArrowError::MemoryError(_) | ArrowError::ArithmeticOverflow(_) => (
                FailureClass::Resource,
                ExecutionReason::OrderedFilterAllocation,
            ),
            ArrowError::OffsetOverflowError(_) => (
                FailureClass::Representation,
                ExecutionReason::OrderedFilterOffset,
            ),
            ArrowError::IoError(..) => (
                FailureClass::Unexpected,
                ExecutionReason::OrderedFilterUnexpected,
            ),
            _ => (FailureClass::Invariant, ExecutionReason::OrderedFilterArrow),
        };
        let fact = FailureFact::new(class, reason.reason());
        if matches!(self.0, ArrowError::OffsetOverflowError(_)) {
            use novarocks_failure_contract::{
                RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
            };
            use std::num::NonZeroU16;
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: NonZeroU16::new(26).unwrap(),
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

fn invalid(detail: &'static str) -> KernelFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(detail))
}

struct NormalizedMask {
    mask: BooleanArray,
    _stock: Option<OpaqueRetainedCharge>,
}

// Field order is the transaction's cleanup contract, including all refusal
// paths: destroy actual Arrow objects before their fees and public work.
struct FilterTransaction {
    raw: Option<RecordBatch>,
    raw_columns: Option<OpaqueRetainedCharge>,
    mask: Option<NormalizedMask>,
    input: Option<Chunk>,
    ledger: Option<AdditionalBufferLedger>,
    _work: ArrowWorkBudget,
}

struct FilteredBatchBacking<R, C> {
    raw: R,
    _raw_columns: C,
    _input: Chunk,
    _ledger: AdditionalBufferLedger,
}

// A slice retains its original typed constructor and its paid column Vec as
// one raw Chunk. A filter's Arrow result has its separate capacity charge.
// The shared adoption author borrows either batch without cloning a Vec.
trait OrderedRawBatch {
    fn record_batch(&self) -> &RecordBatch;
}
impl OrderedRawBatch for RecordBatch {
    fn record_batch(&self) -> &RecordBatch {
        self
    }
}
impl OrderedRawBatch for Chunk {
    fn record_batch(&self) -> &RecordBatch {
        Chunk::record_batch(self)
    }
}

struct SliceTransaction {
    raw: Option<Chunk>,
    input: Option<Chunk>,
    ledger: Option<AdditionalBufferLedger>,
    _work: ArrowWorkBudget,
}

fn adopt_ordered_output<R: OrderedRawBatch + Send + Sync + 'static, C: Send + Sync + 'static>(
    body: FilteredBatchBacking<R, C>,
    host: Arc<dyn AggregateStateAllocator>,
    authority: &OpaqueRetainedCharge,
    work: &mut EvaluationCheckpoints<'_>,
    control: &dyn KernelEvaluationControl,
    construction: &dyn InputConstructionOwner,
) -> ExecutionResult<Chunk> {
    let schema = body._input.chunk_schema_ref();
    let ledger = body._ledger.clone();
    let backing = SourceBackingOwner::try_from_owned_direct(body, Arc::clone(&host), work)?;
    let metadata = OutputColumnMetadata::try_new(
        backing.borrowed().raw.record_batch().num_columns(),
        Arc::clone(&host),
        control,
    )?;
    // This local guard destroys partially wrapped columns before their
    // already admitted container keeper on every return path.
    struct WrappedColumns {
        columns: Vec<ArrayRef>,
        metadata: OutputColumnMetadata,
    }
    let mut wrapped = WrappedColumns {
        columns: Vec::new(),
        metadata,
    };
    wrapped
        .columns
        .try_reserve_exact(backing.borrowed().raw.record_batch().num_columns())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    if wrapped.columns.capacity() != backing.borrowed().raw.record_batch().num_columns() {
        return Err(invalid(
            "ordered scan wrapped column capacity differs from its admitted request",
        )
        .into());
    }
    for original in backing.borrowed().raw.record_batch().columns() {
        let prepared = PreparedSourceCustody::prepare(original, work)?;
        let mut reservation =
            authority.reserve_operation(prepared.request_facts().global_peak_upper())?;
        wrapped
            .columns
            .push(prepared.retain(&backing, &mut reservation, work)?);
    }
    let options = RecordBatchOptions::new()
        .with_row_count(Some(backing.borrowed().raw.record_batch().num_rows()));
    let batch =
        RecordBatch::try_new_with_options(schema.arrow_schema_ref(), wrapped.columns, &options)
            .map_err(|error| {
                ExecutionFailure::from_domain(super::construction_failure::ChunkRecordBatchFailure(
                    error,
                ))
            })?;
    let mut output =
        Chunk::try_new_with_output_column_metadata(batch, schema, wrapped.metadata, construction)?;
    output.attach_additional_buffers(ledger)?;
    Ok(output)
}

fn shape(input: &Chunk) -> Result<ArrowWorkShape, KernelFailure> {
    let rows = input.len();
    let fields = input.columns().len();
    let cells = rows
        .checked_mul(fields)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let payload_bytes = input.columns().iter().try_fold(0usize, |bytes, column| {
        bytes
            .checked_add(column.get_array_memory_size())
            .ok_or(KernelFailure::ResourceExhausted)
    })?;
    // Every admitted flat Arrow carrier has at most three buffers, including
    // its validity buffer. This is a public operation policy, not private stock.
    let buffers = fields
        .checked_mul(3)
        .and_then(|n| n.checked_add(1))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(ArrowWorkShape {
        rows,
        cells,
        payload_bytes,
        fields,
        buffers,
    })
}

/// The existing TruthOnly normalization shared by the processor and source.
/// The controlled source supplies work observations; the original processor
/// supplies its original no-op policy. NULL is always not TRUE.
pub(crate) fn normalize_filter_mask_observed(
    truth: &BooleanArray,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<BooleanArray, KernelFailure> {
    if truth.null_count() == 0 {
        return Ok(truth.clone());
    }
    let mut builder = BooleanBuilder::with_capacity(truth.len());
    for value in truth {
        observe()?;
        builder.append_value(value == Some(true));
    }
    Ok(builder.finish())
}

impl Chunk {
    /// The ordered source's explicit LIMIT view. Even an unfiltered native or
    /// provider input needs custody on every returned Array/Buffer alias.
    pub(crate) fn slice_ordered_scan_controlled(
        self,
        offset: usize,
        length: usize,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
        construction: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Self> {
        if !self.chunk_schema().is_typed() || !self.source_authority().is_attached() {
            return Err(
                invalid("ordered scan slicing requires its actual typed source authority").into(),
            );
        }
        if offset
            .checked_add(length)
            .is_none_or(|end| end > self.len())
        {
            return Err(
                invalid("ordered scan slice is outside its explicit source contract").into(),
            );
        }
        for slot in self.chunk_schema().slots() {
            let ty = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                slot.typed_binding().clone(),
            )
            .map_err(|_| invalid("ordered scan slice has an invalid complete payload type"))?;
            if !novarocks_local_program::is_ordered_filter_flat_payload_type(&ty) {
                return Err(invalid(
                    "ordered scan slice does not support this complete payload type",
                )
                .into());
            }
        }
        let authority = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let _prefix =
            authority.reserve_operation(source_custody_prefix_facts().requests().bytes())?;
        let mut work = EvaluationCheckpoints::new(control);
        work.flush()?;
        let public_work = ArrowWorkBudget::admit(Arc::clone(&host), shape(&self)?)?;
        let ledger = AdditionalBufferLedger::prepare(&self, Arc::clone(&host), control)?;
        let mut transaction = SliceTransaction {
            raw: None,
            input: Some(self),
            ledger: Some(ledger),
            _work: public_work,
        };
        work.flush()?;
        let input = transaction.input.as_ref().expect("unpublished slice input");
        transaction.raw = Some(input.slice_typed_observed::<ExecutionFailure, _>(
            offset,
            length,
            construction.storage_lifetime(),
            |step| {
                work.step()?;
                construction.observe(0, &[], step)?;
                work.flush()?;
                Ok(())
            },
        )?);
        work.flush()?;
        let delta = record_batch_additional_bytes_observed(
            transaction.raw.as_ref().expect("raw slice").record_batch(),
            input.record_batch(),
            || work.step(),
        )?;
        transaction
            .ledger
            .as_ref()
            .expect("unpublished slice ledger")
            .initialize(delta)?;
        let output = adopt_ordered_output(
            FilteredBatchBacking {
                raw: transaction.raw.take().expect("raw slice"),
                // slice_typed_observed already admitted this exact raw Vec
                // through the original typed construction owner.
                _raw_columns: (),
                _input: transaction.input.take().expect("unpublished slice input"),
                _ledger: transaction.ledger.take().expect("unpublished slice ledger"),
            },
            Arc::clone(&host),
            &authority,
            &mut work,
            control,
            construction,
        )?;
        work.finish()?;
        Ok(output)
    }

    /// Only the closed direct-scan route calls this creator. The supplied
    /// construction owner is the same actual task's explicit typed author.
    /// NULL is not TRUE, exactly as in the existing synchronous filter body.
    pub(crate) fn filter_ordered_scan_controlled(
        self,
        truth: &BooleanArray,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
        construction: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Option<Self>> {
        if !self.chunk_schema().is_typed() || !self.source_authority().is_attached() {
            return Err(invalid(
                "ordered scan filtering requires its actual typed source authority",
            )
            .into());
        }
        if truth.len() != self.len() {
            return Err(invalid(
                "ordered scan filter mask differs from its actual input row count",
            )
            .into());
        }
        for slot in self.chunk_schema().slots() {
            let ty = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                slot.typed_binding().clone(),
            )
            .map_err(|_| invalid("ordered scan filter has an invalid complete payload type"))?;
            if !novarocks_local_program::is_ordered_filter_flat_payload_type(&ty) {
                return Err(invalid(
                    "ordered scan filter does not support this complete payload type",
                )
                .into());
            }
        }
        let authority = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let _prefix =
            authority.reserve_operation(source_custody_prefix_facts().requests().bytes())?;
        let mut work = EvaluationCheckpoints::new(control);
        work.flush()?;
        let mut selected = 0usize;
        for value in truth {
            work.step()?;
            selected += usize::from(value == Some(true));
        }
        if selected == 0 {
            work.finish()?;
            return Ok(None);
        }
        if selected == truth.len() {
            work.finish()?;
            return Ok(Some(self));
        }
        let public_work = ArrowWorkBudget::admit(Arc::clone(&host), shape(&self)?)?;
        // Only mixed output needs these cold holders. Admit them before Arrow
        // creates any raw output, while preserving the direct shortcut above.
        let ledger = AdditionalBufferLedger::prepare(&self, Arc::clone(&host), control)?;
        let mut transaction = FilterTransaction {
            raw: None,
            raw_columns: None,
            mask: None,
            input: Some(self),
            ledger: Some(ledger),
            _work: public_work,
        };
        transaction.mask = Some(NormalizedMask {
            mask: normalize_filter_mask_observed(truth, || work.step())?,
            _stock: None,
        });
        if truth.null_count() != 0 {
            // Keep the new bitmap in the transaction before its stock can be
            // refused. Its actual free then precedes the public work release.
            let mask = transaction.mask.as_mut().expect("created normalized mask");
            let mut stock = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
            let bytes = mask.mask.values().inner().capacity();
            let mut reservation = stock.reserve_operation(bytes)?;
            stock.reconcile_under_reservation(bytes, &mut reservation)?;
            drop(reservation);
            mask._stock = Some(stock);
        }
        work.flush()?;
        let input = transaction.input.as_ref().expect("unpublished input");
        transaction.raw = Some(
            filter_record_batch(
                input.record_batch(),
                &transaction.mask.as_ref().expect("normalized mask").mask,
            )
            .map_err(|error| ExecutionFailure::from_domain(FilteredOutputKernelFailure(error)))?,
        );
        work.flush()?;
        let delta = record_batch_additional_bytes_observed(
            transaction.raw.as_ref().expect("filtered raw output"),
            input.record_batch(),
            || work.step(),
        )?;
        transaction
            .ledger
            .as_ref()
            .expect("unpublished ledger")
            .initialize(delta)?;
        // Arrow owns this actual Vec. Preserve its moved capacity independently
        // of the final wrapped column container's original constructor.
        let (schema, columns, rows) = transaction
            .raw
            .take()
            .expect("filtered raw output")
            .into_parts();
        transaction.raw_columns = Some(retain_column_capacity(
            columns.capacity(),
            Arc::clone(&host),
        )?);
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        transaction.raw = Some(
            RecordBatch::try_new_with_options(schema, columns, &options).map_err(|error| {
                ExecutionFailure::from_domain(super::construction_failure::ChunkRecordBatchFailure(
                    error,
                ))
            })?,
        );
        let output = adopt_ordered_output(
            FilteredBatchBacking {
                raw: transaction.raw.take().expect("filtered raw output"),
                _raw_columns: transaction.raw_columns.take().expect("raw column capacity"),
                _input: transaction.input.take().expect("unpublished input"),
                _ledger: transaction.ledger.take().expect("unpublished ledger"),
            },
            Arc::clone(&host),
            &authority,
            &mut work,
            control,
            construction,
        )?;
        work.finish()?;
        Ok(Some(output))
    }
}

#[cfg(test)]
#[path = "filtered_output_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "ordered_slice_tests.rs"]
mod ordered_slice_tests;
