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
//! Private diagnostic producer. No ARRAY owner or scalar ABI is installed.
use super::array_struct_subfield_core::{ProjectionOperation, ProjectionPort};
use super::scalar_invocation_data::{ScalarDataSlot, ScalarInvocationFailure};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostDiagnostic;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use crate::{AggregateStateAllocator, KernelFailure, ScalarCallContract, Selection};
use std::{fmt, sync::Arc};

pub(super) struct HostProjectionDiagnostics<'work, 'control, 'rows> {
    allocator: &'work HostAggregateAllocator,
    opaque: OpaqueRetainedCharge,
    contract: Arc<ScalarCallContract>,
    selection: Selection<'rows>,
    work: &'work mut EvaluationCheckpoints<'control>,
    // The original first.to_string backing lives until project_with_port returns.
    // Keeping its admission on the caller until then never releases it early.
    name_scope: Option<OpaqueReservation>,
}
impl<'work, 'control, 'rows> HostProjectionDiagnostics<'work, 'control, 'rows> {
    pub(super) fn try_new(
        allocator: &'work HostAggregateAllocator,
        host: Arc<dyn AggregateStateAllocator>,
        contract: Arc<ScalarCallContract>,
        selection: Selection<'rows>,
        work: &'work mut EvaluationCheckpoints<'control>,
    ) -> Result<Self, KernelFailure> {
        if !allocator.has_host_authority(&host) {
            return Err(crate::kernel_control::invalid(
                "ARRAY diagnostic uses another actual host authority",
            ));
        }
        Ok(Self {
            allocator,
            opaque: OpaqueRetainedCharge::try_new(host)?,
            contract,
            selection,
            work,
            name_scope: None,
        })
    }
    fn publish_diagnostic(
        &mut self,
        original: fmt::Arguments<'_>,
    ) -> Result<super::scalar_invocation_data::ScalarInvocationData, KernelFailure> {
        // Execute ONE original formatter once. It writes into the existing
        // actually allocated HostDiagnostic author, not a counting renderer.
        let rendered = HostDiagnostic::prepare(self.allocator, self.work, |writer| {
            writer.write_fmt(original)
        })?;
        let text = rendered.message();
        self.work.flush()?;
        // The exact original UTF8 extent is now known without reformatting. The
        // host admits the std String request BEFORE that allocation occurs.
        let reservation = self.opaque.reserve_operation(text.len())?;
        self.work.flush()?;
        // Admit carrier/domain/header before constructing the final String.
        // If preparation fails, no std String exists under the released lease.
        let slot = ScalarDataSlot::prepare_for_actual_invocation(
            Arc::clone(&self.contract),
            self.selection,
            self.allocator,
            reservation,
            self.work,
        )?;
        let mut message = String::new();
        message.try_reserve_exact(text.len()).map_err(|source| {
            KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
                "array field-name diagnostic",
                source,
            ))
        })?;
        // Pinned std Global returns the exact requested byte Layout for this
        // empty Vec<u8> growth. This is not a post-allocation permission check.
        // Copy each valid UTF8 scalar without another formatter or decoder.
        for ch in text.chars() {
            message.push(ch);
            for _ in 0..ch.len_utf8() {
                self.work.step()?;
            }
        }
        self.work.flush()?;
        // All fallible construction is complete. Publication only moves the
        // original complete String. No work.finish or later diagnostic footer.
        Ok(slot.publish_original(crate::DataFailureReason::InvalidValue, message))
    }
}
impl ProjectionPort for HostProjectionDiagnostics<'_, '_, '_> {
    type Failure = ScalarInvocationFailure;
    fn value_error(&mut self, original: fmt::Arguments<'_>) -> Self::Failure {
        match self.publish_diagnostic(original) {
            Ok(data) => ScalarInvocationFailure::Data(data),
            Err(cause) => ScalarInvocationFailure::Kernel(cause),
        }
    }
    fn step(&mut self) -> Result<(), Self::Failure> {
        self.work.step().map_err(ScalarInvocationFailure::Kernel)
    }
    fn after_operation(&mut self) -> Result<(), Self::Failure> {
        self.work.flush().map_err(ScalarInvocationFailure::Kernel)
    }
    fn before_operation(&mut self, original: ProjectionOperation<'_>) -> Result<(), Self::Failure> {
        match original {
            ProjectionOperation::FieldNameCopy { text } => {
                if self.name_scope.is_some() {
                    return Err(ScalarInvocationFailure::Kernel(
                        crate::kernel_control::internal(
                            "ARRAY field name copied twice in one invocation",
                        ),
                    ));
                }
                self.work.flush().map_err(ScalarInvocationFailure::Kernel)?;
                let reservation = self
                    .opaque
                    .reserve_operation(text.len())
                    .map_err(ScalarInvocationFailure::Kernel)?;
                self.work.flush().map_err(ScalarInvocationFailure::Kernel)?;
                self.name_scope = Some(reservation);
                Ok(())
            }
            // This diagnostic-only slice is not an installed owner. It refuses
            // actual compute allocations whose complete author is not present;
            // no signature/profile is narrowed or advertised as supported.
            ProjectionOperation::Indices
            | ProjectionOperation::IndicesArray
            | ProjectionOperation::Take { .. }
            | ProjectionOperation::OutputField
            | ProjectionOperation::ListResult => Err(ScalarInvocationFailure::Kernel(
                crate::kernel_control::invalid(
                    "ARRAY projection computation requires its actual opaque operation host",
                ),
            )),
        }
    }
}
#[cfg(test)]
#[path = "array_scalar_diagnostic_source_tests.rs"]
mod tests;
