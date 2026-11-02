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

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::{ArrayNormalizationStep, Chunk, ChunkSchemaRef};
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
use crate::runtime::fragment::ExecutionFailure;
use novarocks_type_contract::ValueTypeStorageLifetime;

mod paged;
pub(crate) use paged::{CanonicalJoinOutput, JoinPairRow, PagedJoinGather, ProjectedJoinGather};

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
pub(super) fn gather_kernel(cause: arrow::error::ArrowError) -> ExecutionFailure {
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
    #[cfg(test)]
    pub(crate) fn record_batch(&self) -> &RecordBatch {
        &self.batch
    }
    #[cfg(test)]
    pub(crate) fn num_rows(&self) -> usize {
        self.batch.num_rows()
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

pub(super) use paged::copy_column as copy_payload_column;
