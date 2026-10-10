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

//! Funding for the original BE type materialization tail, before its first
//! Arrow allocation. No writer exists across a Pending wait or requalification.

use super::preparation_memory_control::{WorkerPreparationMemoryControl, worker_stop};
#[cfg(test)]
use novarocks_execution::runtime::kernel_memory::KernelMemoryAdmission;
use novarocks_execution::runtime::{
    kernel_memory::KernelMemoryJournal,
    preparation_memory::{
        PreparationMemoryJournalLoan, PreparationMemoryRefusal, SynchronousPreparationFailure,
        SynchronousPreparationMemory,
    },
    query_memory::QueryMemoryBinding,
};
use novarocks_memory::{CapacityError, CoverageReceipt, ShortageReceipt};
use novarocks_plan_codec::{
    host_projection_v2::ProjectionFailure,
    physical_type_v2::{
        DecodedTypeTable, PackageTypeMaterializationScope, PackageTypeProjectionFacts,
        TypeCodecError,
    },
};
use novarocks_type_contract::CompleteMetadataRequestFacts;
use novarocks_worker::{PreparationControlLoan, PreparationStop};

#[derive(Debug)]
pub enum TypeMaterializationRefusal {
    Stopped(PreparationStop),
    MissingQueryMemory,
    SharedShortage(ShortageReceipt),
    Capacity(CapacityError),
}
impl std::fmt::Display for TypeMaterializationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stopped(stop) => write!(f, "type materialization stopped: {stop:?}"),
            Self::MissingQueryMemory => f.write_str("type materialization requires query memory"),
            Self::SharedShortage(receipt) => {
                write!(f, "type materialization shared shortage: {receipt:?}")
            }
            Self::Capacity(cause) => cause.fmt(f),
        }
    }
}
impl std::error::Error for TypeMaterializationRefusal {}

/// Fixed local evidence, including the latest real qualification while Pending.
/// Observation faults remain observations; they do not become SQL refusals.
#[derive(Debug, Default)]
pub struct TypeMaterializationJournal {
    pub facts: Option<PackageTypeProjectionFacts>,
    pub workset_bytes: Option<usize>,
    pub pending: Option<CoverageReceipt>,
    pub shortage: Option<ShortageReceipt>,
    pub capacity: Option<CapacityError>,
    pub body: KernelMemoryJournal,
}

/// A synchronous borrowed capability, never retained in a package or wire DTO.
pub struct TypeMaterializationHost<'a> {
    binding: &'a QueryMemoryBinding,
    preparation: &'a PreparationControlLoan<'a>,
    journal: &'a mut TypeMaterializationJournal,
}
impl<'a> TypeMaterializationHost<'a> {
    pub fn new(
        binding: &'a QueryMemoryBinding,
        preparation: &'a PreparationControlLoan<'a>,
        journal: &'a mut TypeMaterializationJournal,
    ) -> Self {
        Self {
            binding,
            preparation,
            journal,
        }
    }
    fn journal_loan(&mut self) -> PreparationMemoryJournalLoan<'_> {
        PreparationMemoryJournalLoan {
            workset_bytes: &mut self.journal.workset_bytes,
            pending: &mut self.journal.pending,
            shortage: &mut self.journal.shortage,
            capacity: &mut self.journal.capacity,
            body: &mut self.journal.body,
        }
    }

    #[cfg(test)]
    fn materialize_with_request<B, R>(
        &mut self,
        facts: &PackageTypeProjectionFacts,
        body: B,
        request: R,
    ) -> Result<DecodedTypeTable, ProjectionFailure<TypeCodecError, TypeMaterializationRefusal>>
    where
        B: FnOnce() -> Result<DecodedTypeTable, TypeCodecError>,
        R: FnMut(&QueryMemoryBinding, usize) -> KernelMemoryAdmission,
    {
        self.journal.facts = Some(*facts);
        let control = WorkerPreparationMemoryControl {
            preparation: self.preparation,
        };
        let binding = self.binding;
        SynchronousPreparationMemory::new(Some(binding), &control, self.journal_loan())
            .materialize_with_request_for_test(
                request_facts(facts),
                "type materialization request bound exceeds funding width",
                body,
                request,
            )
            .map_err(original_failure)
    }
}
impl PackageTypeMaterializationScope for TypeMaterializationHost<'_> {
    type HostError = TypeMaterializationRefusal;
    fn materialize<B>(
        &mut self,
        facts: &PackageTypeProjectionFacts,
        body: B,
    ) -> Result<DecodedTypeTable, ProjectionFailure<TypeCodecError, Self::HostError>>
    where
        B: FnOnce() -> Result<DecodedTypeTable, TypeCodecError>,
    {
        self.journal.facts = Some(*facts);
        let control = WorkerPreparationMemoryControl {
            preparation: self.preparation,
        };
        let binding = self.binding;
        SynchronousPreparationMemory::new(Some(binding), &control, self.journal_loan())
            .materialize(
                request_facts(facts),
                "type materialization request bound exceeds funding width",
                body,
            )
            .map_err(original_failure)
    }
}

fn request_facts(facts: &PackageTypeProjectionFacts) -> CompleteMetadataRequestFacts {
    CompleteMetadataRequestFacts {
        allocation_requests_upper_bound: facts.allocation_requests_upper_bound,
        allocation_request_bytes_upper_bound: facts.allocation_request_bytes_upper_bound,
    }
}
fn original_failure(
    failure: SynchronousPreparationFailure<TypeCodecError>,
) -> ProjectionFailure<TypeCodecError, TypeMaterializationRefusal> {
    match failure {
        SynchronousPreparationFailure::Body(error) => ProjectionFailure::Codec(error),
        SynchronousPreparationFailure::Host(error) => ProjectionFailure::Host(match error {
            PreparationMemoryRefusal::Stopped(stop) => {
                TypeMaterializationRefusal::Stopped(worker_stop(stop))
            }
            PreparationMemoryRefusal::MissingQueryMemory => {
                TypeMaterializationRefusal::MissingQueryMemory
            }
            PreparationMemoryRefusal::SharedShortage(receipt) => {
                TypeMaterializationRefusal::SharedShortage(receipt)
            }
            PreparationMemoryRefusal::Capacity(error) => {
                TypeMaterializationRefusal::Capacity(error)
            }
        }),
    }
}

/// Thin test entry through the SAME production type host and original decoder.
/// The caller supplies explicit source/limits/control and owns ordinary footer.
#[cfg(any(test, feature = "test-support"))]
pub fn materialize_package_types_for_test(
    package: &novarocks_proto_models::physical_package_v2::FragmentPackage,
    source_retained_bytes: usize,
    limits: novarocks_plan_codec::physical_type_v2::PackageTypeProjectionLimits,
    control: &dyn novarocks_type_contract::PureCompileControl,
    binding: &QueryMemoryBinding,
    preparation: &PreparationControlLoan<'_>,
    journal: &mut TypeMaterializationJournal,
) -> Result<DecodedTypeTable, ProjectionFailure<TypeCodecError, TypeMaterializationRefusal>> {
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::Decode,
    )?;
    let mut host = TypeMaterializationHost::new(binding, preparation, journal);
    let result =
        novarocks_plan_codec::physical_type_v2::decode_package_type_table_with_host_observed(
            package,
            source_retained_bytes,
            limits,
            &mut |_| Ok(()),
            &mut work,
            &mut host,
        );
    if matches!(
        &result,
        Err(ProjectionFailure::Host(_) | ProjectionFailure::Codec(TypeCodecError::Control(_)))
    ) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
#[path = "type_materialization_tests.rs"]
mod tests;
