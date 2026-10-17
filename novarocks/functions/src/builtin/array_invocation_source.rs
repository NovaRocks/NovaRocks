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

//! Shared array-invocation custody. Array algorithms remain their original authors.
use crate::arrow_result_custody::{PreparedSourceCustody, SourceBackingOwner};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{
    AggregateStateAllocator, ConstantValue, KernelFailure, ScalarInvocationScope,
    ScalarInvocationSource,
};
use arrow_array::ArrayRef;
use std::sync::Arc;

/// Actual evaluated sources; a constant retains its checked pool/source owner,
/// not just a raw ArrayRef detached from that owner. Frame arrays already carry
/// their original Chunk/provider custody on the backing buffers.
#[derive(Clone)]
pub(super) enum OwnedArraySource {
    Array(ArrayRef),
    Constant(ConstantValue),
}
impl OwnedArraySource {
    pub(super) fn from_source(source: &ScalarInvocationSource<'_>) -> Self {
        match source {
            ScalarInvocationSource::Constant(value) => Self::Constant((*value).clone()),
            ScalarInvocationSource::Scalar(array) => Self::Array(Arc::clone(array)),
            ScalarInvocationSource::Array(loan) => Self::Array(Arc::clone(loan.array())),
        }
    }
}

/// Hold real evaluated sources through every output Buffer/child/slice alias.
/// This funds only exact existing custody metadata, never a retrospective
/// payload grant for the Arrow operation which has already produced `output`.
pub(super) fn retain_output<C: Send + Sync + 'static>(
    output: ArrayRef,
    actual_sources: C,
    host: Arc<dyn AggregateStateAllocator>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let source =
        SourceBackingOwner::try_from_owned_direct(actual_sources, Arc::clone(&host), work)?;
    let prepared = PreparedSourceCustody::<C>::prepare(&output, work)?;
    let metadata = prepared.request_facts().global_peak_upper();
    let authority = OpaqueRetainedCharge::try_new(host)?;
    work.flush()?;
    let mut reservation = authority.reserve_operation(metadata)?;
    work.flush()?;
    prepared.retain(&source, &mut reservation, work)
}

/// An original Arrow transport failure must retain its full Data text; typed
/// control failures return immediately without any diagnostic/footer callback.
pub(super) enum ArrayInvocationSourceError {
    Kernel(KernelFailure),
    Arrow(arrow_schema::ArrowError),
}
impl From<KernelFailure> for ArrayInvocationSourceError {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}
impl From<crate::EvaluationContractError> for ArrayInvocationSourceError {
    fn from(cause: crate::EvaluationContractError) -> Self {
        Self::Kernel(cause.into())
    }
}
pub(super) fn source_failure(
    failure: ArrayInvocationSourceError,
    contract: &Arc<crate::ScalarCallContract>,
    actual_domain: crate::Selection<'_>,
    allocator: &crate::aggregate_host_allocator::HostAggregateAllocator,
    host: &Arc<dyn AggregateStateAllocator>,
    work: &mut EvaluationCheckpoints<'_>,
) -> crate::ScalarInvocationFailure {
    use super::array_struct_subfield_core::ProjectionPort;
    match failure {
        ArrayInvocationSourceError::Kernel(cause) => cause.into(),
        ArrayInvocationSourceError::Arrow(error) => {
            match super::array_scalar_diagnostic_source::HostProjectionDiagnostics::try_new(
                allocator,
                Arc::clone(host),
                Arc::clone(contract),
                actual_domain,
                work,
            ) {
                Ok(mut diagnostic) => diagnostic.arrow_error(error),
                Err(cause) => cause.into(),
            }
        }
    }
}

/// Expand only an explicit scalar/checked constant. Array loans already carry
/// the actual domain and are neither sliced nor filtered by this author.
pub(super) fn actual_array(
    source: &ScalarInvocationSource<'_>,
    scope: ScalarInvocationScope<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, ArrayInvocationSourceError> {
    let rows = scope.actual_domain().len();
    let (array, ordinal) = match source {
        ScalarInvocationSource::Array(loan) => {
            if !loan
                .domain()
                .same_rows_observed(scope.actual_domain(), || work.step())?
            {
                return Err(crate::kernel_control::invalid(
                    "array source loan differs from the actual invocation domain",
                )
                .into());
            }
            return Ok(Arc::clone(loan.array()));
        }
        ScalarInvocationSource::Scalar(array) => (*array, 0),
        ScalarInvocationSource::Constant(value) => (value.pool().array(), value.ordinal()),
    };
    work.flush()?;
    let mut indices = Vec::with_capacity(rows);
    work.flush()?;
    for _ in 0..rows {
        indices.push(ordinal);
        work.step()?;
    }
    work.flush()?;
    let indices = arrow_array::UInt32Array::from(indices);
    work.flush()?;
    let result = arrow_select::take::take(array.as_ref(), &indices, None)
        .map_err(ArrayInvocationSourceError::Arrow)?;
    work.flush()?;
    Ok(result)
}

/// Select transport output from the explicit actual-domain map. This is not
/// argument normalization, and cannot evaluate or restore an uncomputed row.
pub(super) fn select_output(
    actual: &ArrayRef,
    scope: ScalarInvocationScope<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, ArrayInvocationSourceError> {
    if scope
        .actual_domain()
        .same_rows_observed(scope.demand(), || work.step())?
    {
        return Ok(Arc::clone(actual));
    }
    let loan = crate::EvaluatedSourceLoan::new(actual, scope.actual_domain());
    work.flush()?;
    let mut indices = Vec::with_capacity(scope.demand().len());
    work.flush()?;
    for logical_row in scope.demand().iter() {
        let ordinal = loan.physical_ordinal(logical_row).ok_or_else(|| {
            crate::kernel_control::invalid("output demand is outside the actual invocation domain")
        })?;
        indices.push(u64::try_from(ordinal).map_err(|_| KernelFailure::ResourceExhausted)?);
        work.step()?;
    }
    work.flush()?;
    let indices = arrow_array::UInt64Array::from(indices);
    work.flush()?;
    let selected = arrow_select::take::take(actual.as_ref(), &indices, None)
        .map_err(ArrayInvocationSourceError::Arrow)?;
    work.flush()?;
    Ok(selected)
}
