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

//! Shared geometry, bounded output search, and typed construction helpers.
//! Compiled Unpivot, Writer statistics, and table functions retain one owner.

use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, ResourceDimension, ResourceFacts,
    ResourceScope,
};
#[derive(Debug)]
struct UnpivotContractFailure(BoundedDetail);
impl std::fmt::Display for UnpivotContractFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for UnpivotContractFailure {}
impl FailureProjection for UnpivotContractFailure {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::UnpivotContract.reason(),
        )
    }
}
pub(super) fn unpivot_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(UnpivotContractFailure(BoundedDetail::truncating_display(
        detail,
    )))
}
#[derive(Debug)]
struct UnpivotLimit {
    dimension: ResourceDimension,
    requested: usize,
    limit: usize,
}
impl std::fmt::Display for UnpivotLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unpivot {:?} request {} exceeds limit {}",
            self.dimension, self.requested, self.limit
        )
    }
}
impl std::error::Error for UnpivotLimit {}
impl FailureProjection for UnpivotLimit {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Resource,
            crate::runtime::fragment::ExecutionReason::UnpivotLimit.reason(),
        )
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Request),
            constraint_scope: Some(ResourceScope::Request),
            dimension: self.dimension,
            requested: u64::try_from(self.requested).ok(),
            required_growth: None,
            limit: u64::try_from(self.limit).ok(),
            live: None,
            committed: None,
        })
    }
}
fn unpivot_limit(dimension: ResourceDimension, requested: usize, limit: usize) -> ExecutionFailure {
    ExecutionFailure::from_domain(UnpivotLimit {
        dimension,
        requested,
        limit,
    })
}
pub(super) fn unpivot_arrow(source: arrow::error::ArrowError) -> ExecutionFailure {
    size_probe::UnpivotProbeFailure::Encoding(source).into_execution()
}

/// A failure from the actual column extension, independent of IPC size probes.
#[derive(Debug)]
struct UnpivotExtensionFailure(arrow::error::ArrowError);
impl std::fmt::Display for UnpivotExtensionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unpivot array extension failed: {}", self.0)
    }
}
impl std::error::Error for UnpivotExtensionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for UnpivotExtensionFailure {
    fn failure_fact(&self) -> FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        let (class, reason) = match &self.0 {
            arrow::error::ArrowError::MemoryError(_) => (
                FailureClass::Resource,
                ExecutionReason::UnpivotExtensionAllocation,
            ),
            _ => (FailureClass::Invariant, ExecutionReason::UnpivotExtension),
        };
        FailureFact::new(class, reason.reason())
    }
}
fn unpivot_extension_failure(source: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(UnpivotExtensionFailure(source))
}

#[path = "unpivot_processor/size_probe.rs"]
mod size_probe;

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, make_array};
use arrow_data::transform::MutableArrayData;

use crate::exec::chunk::Chunk;
use crate::runtime::mem_tracker::MemTracker;
use novarocks_functions::array_projection::ArrayNormalizationStep;
use novarocks_types::SlotId;

/// Strong caller entry to the SAME expansion budget search. The caller's
/// constructor must return a fully typed immutable Chunk and retain its own
/// actual creation/source ownership; this function adds no declaration author
/// or fallback. Each IPC/logical-size measurement has independent temp scopes.
pub(crate) fn next_bounded_output_with_construction(
    remaining: usize,
    max_output_rows: usize,
    max_output_bytes: usize,
    hint: &mut Option<usize>,
    tracker: Option<&Arc<MemTracker>>,
    factory: &dyn crate::exec::pipeline::operator::input_construction::InputConstructionFactory,
    control: &dyn novarocks_functions::KernelEvaluationControl,
    build: impl FnMut(usize) -> ExecutionResult<Chunk>,
) -> ExecutionResult<(Chunk, usize)> {
    use crate::exec::pipeline::operator::input_construction::ConstructionFailure;
    next_bounded_output_core(
        remaining,
        max_output_rows,
        max_output_bytes,
        hint,
        build,
        |mut chunk| {
            if let Some(tracker) = tracker {
                chunk
                    .try_transfer_to(tracker)
                    .map_err(ConstructionFailure::Memory)?;
            }
            Ok(chunk)
        },
        |chunk| {
            size_probe::output_size_with_construction(chunk, factory, control)
                .map_err(size_probe::UnpivotProbeFailure::into_execution)
        },
        |actual_bytes, max_output_bytes| {
            unpivot_limit(ResourceDimension::Bytes, actual_bytes, max_output_bytes)
        },
    )
}

fn next_bounded_output_core<E>(
    remaining: usize,
    max_output_rows: usize,
    max_output_bytes: usize,
    hint: &mut Option<usize>,
    mut build: impl FnMut(usize) -> Result<Chunk, E>,
    mut track: impl FnMut(Chunk) -> Result<Chunk, E>,
    mut measure: impl FnMut(&Chunk) -> Result<usize, E>,
    mut budget_refusal: impl FnMut(usize, usize) -> E,
) -> Result<(Chunk, usize), E> {
    let row_limit = remaining.min(max_output_rows);
    let had_size_hint = hint.is_some();
    let mut candidate_rows = if max_output_bytes == usize::MAX {
        row_limit
    } else {
        hint.unwrap_or(1).min(row_limit)
    };
    let mut output = track(build(candidate_rows)?)?;
    let mut upper_bound = row_limit;
    let candidate_bytes = if max_output_bytes == usize::MAX {
        None
    } else {
        Some(measure(&output)?)
    };
    if candidate_bytes.is_some_and(|bytes| bytes > max_output_bytes) {
        upper_bound = candidate_rows - 1;
        loop {
            if candidate_rows == 1 {
                let actual_bytes = measure(&output)?;
                return Err(budget_refusal(actual_bytes, max_output_bytes));
            }
            candidate_rows = (candidate_rows / 2).max(1);
            output = track(build(candidate_rows)?)?;
            if max_output_bytes == usize::MAX || measure(&output)? <= max_output_bytes {
                break;
            }
        }
    } else if had_size_hint || candidate_rows == row_limit {
        // Reuse the preceding batch's exact fit, but allow one bounded
        // doubling probe when it now occupies at most half the budget.
        // This recovers from a hint reduced by an earlier large value
        // without repeating a logarithmic search for every output batch.
        upper_bound = candidate_rows;
        if candidate_rows < row_limit
            && candidate_bytes.is_some_and(|bytes| bytes.saturating_mul(2) <= max_output_bytes)
        {
            let next_rows = candidate_rows.saturating_mul(2).min(row_limit);
            let next = track(build(next_rows)?)?;
            if max_output_bytes == usize::MAX || measure(&next)? <= max_output_bytes {
                candidate_rows = next_rows;
                output = next;
                upper_bound = candidate_rows;
            }
        }
    } else {
        // The first byte-bounded batch grows from one row so a small budget
        // never materializes the complete wide expansion merely to learn it
        // is too large.
        while candidate_rows < row_limit {
            let next_rows = candidate_rows.saturating_mul(2).min(row_limit);
            let next = track(build(next_rows)?)?;
            if max_output_bytes == usize::MAX || measure(&next)? <= max_output_bytes {
                candidate_rows = next_rows;
                output = next;
            } else {
                upper_bound = next_rows - 1;
                break;
            }
        }
    }
    while candidate_rows < upper_bound {
        let middle = candidate_rows + (upper_bound - candidate_rows).div_ceil(2);
        let candidate = track(build(middle)?)?;
        if max_output_bytes == usize::MAX || measure(&candidate)? <= max_output_bytes {
            candidate_rows = middle;
            output = candidate;
        } else {
            upper_bound = middle - 1;
        }
    }
    *hint = Some(candidate_rows);
    Ok((output, candidate_rows))
}

#[derive(Clone, Copy)]
pub(crate) struct Segment {
    pub(crate) mapping_index: usize,
    pub(crate) input_offset: usize,
    pub(crate) len: usize,
}

pub(crate) fn visit_flattened_segments(
    start: usize,
    len: usize,
    input_rows: usize,
    mut emit: impl FnMut(Segment),
) {
    // Mapping-major traversal is the original array algorithm; the relational
    // contract deliberately exposes no output ordering guarantee.
    let mut cursor = start;
    let end = start + len;
    while cursor < end {
        let mapping_index = cursor / input_rows;
        let input_offset = cursor % input_rows;
        let segment_len = (input_rows - input_offset).min(end - cursor);
        emit(Segment {
            mapping_index,
            input_offset,
            len: segment_len,
        });
        cursor += segment_len;
    }
}

/// Actual multi-source MutableArrayData operation. This preserves the original
/// new(refs, true, capacity)/ordered extend/freeze body, and uses the sole
/// functions selection/invoice author before its opaque allocations.
pub(crate) fn concat_owned_with_construction(
    parts: Vec<ArrayRef>,
    output_slot_id: SlotId,
    temporary: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    retained: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> ExecutionResult<ArrayRef> {
    use crate::exec::pipeline::operator::input_construction::ConstructionFailure;
    use novarocks_functions::selected_copy::{
        ExtendSegment, preflight_extend_multi_structure_construction,
    };
    use novarocks_type_contract::ValueSchemaCreation;
    use std::alloc::Layout;
    let capacity = parts
        .iter()
        .try_fold(0usize, |sum, part| sum.checked_add(part.len()))
        .ok_or(novarocks_functions::KernelFailure::ResourceExhausted)?;
    let mut source_refs = unpivot_vec::<&dyn Array>(parts.len(), temporary)?;
    let mut segments = unpivot_vec::<ExtendSegment>(parts.len(), temporary)?;
    for (source, part) in parts.iter().enumerate() {
        control.checkpoint(1)?;
        source_refs.push(part.as_ref());
        segments.push(ExtendSegment {
            source,
            start: 0,
            len: part.len(),
            repeats: 1,
            nulls: 0,
        });
    }
    let mut temporary_request = |layout| temporary.request(layout);
    let mut temporary_opaque = |bytes| temporary.observe_opaque_upper_bound(bytes);
    let mut retained_request = |layout| retained.request(layout);
    let mut retained_opaque = |bytes| retained.observe_opaque_upper_bound(bytes);
    let mut temporary_creation = ValueSchemaCreation::new(
        temporary.storage_lifetime(),
        &mut temporary_request,
        &mut temporary_opaque,
    );
    let mut retained_creation = ValueSchemaCreation::new(
        retained.storage_lifetime(),
        &mut retained_request,
        &mut retained_opaque,
    );
    let mut work = |boundary: bool| {
        temporary.observe(
            0,
            &[],
            ArrayNormalizationStep::InspectConstructionWork {
                units: usize::from(!boundary),
            },
        )
    };
    preflight_extend_multi_structure_construction(
        &source_refs,
        &segments,
        capacity,
        &mut temporary_creation,
        &mut retained_creation,
        &mut work,
    )
    .map_err(unpivot_copy_failure)?;
    // to_data and its Vec belong to this actual operation, separately from the
    // observed preflight's own exact scratch; no new payload is hydrated.
    let mut data = unpivot_vec::<arrow_data::ArrayData>(parts.len(), temporary)?;
    for part in &parts {
        control.checkpoint(0)?;
        data.push(part.to_data());
        control.checkpoint(0)?;
    }
    temporary.request(
        Layout::array::<&arrow_data::ArrayData>(data.len())
            .map_err(|_| novarocks_functions::KernelFailure::ResourceExhausted)?,
    )?;
    let mut refs = Vec::new();
    refs.try_reserve_exact(data.len())
        .map_err(|_| novarocks_functions::KernelFailure::ResourceExhausted)?;
    refs.extend(data.iter());
    control.checkpoint(0)?;
    let mut output = MutableArrayData::new(refs, true, capacity);
    control.checkpoint(0)?;
    for (index, part) in parts.iter().enumerate() {
        control.checkpoint(0)?;
        output
            .try_extend(index, 0, part.len())
            .map_err(unpivot_extension_failure)?;
        control.checkpoint(0)?;
    }
    control.checkpoint(0)?;
    let output = make_array(output.freeze());
    control.checkpoint(0)?;
    if output.len() != capacity {
        return Err(ConstructionFailure::InvalidSource(format!(
            "unpivot output slot {output_slot_id} materialization produced {} rows, expected {capacity}", output.len()
        )).into());
    }
    Ok(output)
}

pub(crate) fn unpivot_vec<T>(
    count: usize,
    owner: &dyn crate::exec::pipeline::operator::input_construction::InputConstructionOwner,
) -> ExecutionResult<Vec<T>> {
    let layout = std::alloc::Layout::array::<T>(count)
        .map_err(|_| novarocks_functions::KernelFailure::ResourceExhausted)?;
    if layout.size() != 0 {
        owner.request(layout)?;
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| novarocks_functions::KernelFailure::ResourceExhausted)?;
    Ok(values)
}

pub(crate) fn unpivot_copy_failure(
    failure: novarocks_functions::selected_copy::CopyConstructionError<
        crate::exec::pipeline::operator::input_construction::ConstructionFailure,
    >,
) -> crate::runtime::fragment::ExecutionFailure {
    use crate::exec::pipeline::operator::input_construction::ConstructionFailure;
    use novarocks_functions::selected_copy::CopyConstructionError;
    match failure {
        CopyConstructionError::Creator(cause) => cause.into(),
        CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
        CopyConstructionError::Copy(cause) => cause.into_kernel().into(),
    }
}

#[cfg(test)]
mod extension_failure_tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn extension_failure_retains_actual_arrow_source_without_ipc_size_facts() {
        let values = arrow::array::Int32Array::from(vec![1]);
        let data = values.to_data();
        let mut output = MutableArrayData::new(vec![&data], true, 1);
        let actual = output.try_extend(0, 0, 2).unwrap_err();
        let original_display = actual.to_string();
        let failure = UnpivotExtensionFailure(actual);
        let fact = failure.failure_fact();
        assert_eq!(fact.class(), FailureClass::Invariant);
        assert_eq!(fact.reason().code(), 616);
        assert_eq!(fact.representation(), None);
        assert_eq!(fact.resource(), None);
        let source = failure.source().unwrap();
        assert!(source.downcast_ref::<arrow::error::ArrowError>().is_some());
        assert_eq!(source.to_string(), original_display);
        let execution = ExecutionFailure::from_domain(failure);
        assert_eq!(execution.failure_fact(), fact);
    }
}
