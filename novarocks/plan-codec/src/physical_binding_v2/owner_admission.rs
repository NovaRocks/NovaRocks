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

use super::{BindingCodecError, BindingProjectionFacts, BindingProjectionLimits};
use novarocks_type_contract::CompileControlError;
use std::alloc::Layout;

pub(crate) type HostAdmit<'a, H> = dyn FnMut(&BindingProjectionFacts) -> Result<(), crate::host_projection_v2::AdmissionRefusal<H>>
    + 'a;

pub(crate) type Admit<'a> =
    dyn FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError> + 'a;

/// Only the callback/numerical error policy differs. The original facts and
/// mapper remain the single author for both entry policies.
#[derive(Clone, Copy)]
pub(crate) struct Policy(pub bool);
impl Policy {
    fn overflow(self, message: &'static str) -> BindingCodecError {
        if self.0 {
            CompileControlError::ResourceExhausted.into()
        } else {
            BindingCodecError::InvalidShape(message)
        }
    }
    pub(crate) fn add(
        self,
        a: usize,
        b: usize,
        message: &'static str,
    ) -> Result<usize, BindingCodecError> {
        a.checked_add(b).ok_or_else(|| self.overflow(message))
    }
    pub(crate) fn mul(
        self,
        a: usize,
        b: usize,
        message: &'static str,
    ) -> Result<usize, BindingCodecError> {
        a.checked_mul(b).ok_or_else(|| self.overflow(message))
    }
    pub(crate) fn bytes<T>(
        self,
        n: usize,
        message: &'static str,
    ) -> Result<usize, BindingCodecError> {
        Layout::array::<T>(n)
            .map(|l| l.size())
            .map_err(|_| self.overflow(message))
    }
    pub(crate) fn gate(
        self,
        facts: &BindingProjectionFacts,
        limits: impl Into<Option<BindingProjectionLimits>>,
        admit: &mut Admit<'_>,
    ) -> Result<(), BindingCodecError> {
        self.gate_with_host(facts, limits, &mut |facts| {
            admit(facts).map_err(
                crate::host_projection_v2::AdmissionRefusal::<std::convert::Infallible>::Control,
            )
        })
        .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
    }

    pub(crate) fn gate_with_host<H>(
        self,
        facts: &BindingProjectionFacts,
        limits: impl Into<Option<BindingProjectionLimits>>,
        admit: &mut HostAdmit<'_, H>,
    ) -> Result<(), crate::host_projection_v2::ProjectionFailure<BindingCodecError, H>> {
        if !self.0 {
            return Ok(());
        }
        if let Some(limits) = limits.into() {
            for (value, cap) in [
                (facts.definition_count, limits.max_definitions),
                (facts.type_reference_count, limits.max_type_references),
                (
                    facts.allocation_requests_upper_bound,
                    limits.max_allocation_requests,
                ),
                (facts.request_bytes_upper_bound, limits.max_request_bytes),
                (
                    facts.coexisting_source_and_request_bytes_upper_bound,
                    limits.max_coexisting_source_and_request_bytes,
                ),
                (facts.cumulative_work_upper_bound, limits.max_work),
            ] {
                if value > cap {
                    return Err(CompileControlError::ResourceExhausted.into());
                }
            }
        }
        match admit(facts) {
            Ok(()) => {}
            Err(crate::host_projection_v2::AdmissionRefusal::Control(cause)) => {
                return Err(cause.into());
            }
            Err(crate::host_projection_v2::AdmissionRefusal::Host(error)) => {
                return Err(crate::host_projection_v2::ProjectionFailure::Host(error));
            }
        }
        Ok(())
    }
}

/// No allocation or output: one actual consuming lookup contribution. A
/// caller adds repeated operations; this is not the namespace's prior B again.
pub(crate) fn lookup_facts(
    count: usize,
    work: usize,
) -> Result<BindingProjectionFacts, BindingCodecError> {
    Ok(BindingProjectionFacts {
        definition_count: count,
        type_reference_count: 0,
        allocation_requests_upper_bound: 0,
        request_bytes_upper_bound: 0,
        coexisting_source_and_request_bytes_upper_bound: 0,
        cumulative_work_upper_bound: work
            .checked_add(1)
            .ok_or(CompileControlError::ResourceExhausted)?,
    })
}
