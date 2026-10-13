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
//! Same-host loans around the sole original Mann-Whitney core and full Data.
//! Opaque successful output has a separate custody path; it cannot escape here.

use super::aggregate_mann_whitney_core::{MannResourceEvent, MannResources, MannWhitneyInput};
use super::aggregate_mann_whitney_resources::MannSourceParams;
use crate::aggregate_format::AggregateFailureStage;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostDiagnostic;
use crate::aggregate_scalar::{ScalarStateError, ScalarWork};
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use crate::{
    AggregateCallContract, AggregateInvocationPhase, EvaluationFailure, InvocationData,
    KernelEvaluationControl, KernelFailure, Selection,
};
use std::sync::Arc;

pub(super) struct MannDataDomain<'a> {
    pub(super) allocator: &'a HostAggregateAllocator,
    pub(super) contract: &'a Arc<AggregateCallContract>,
    pub(super) phase: AggregateInvocationPhase,
    pub(super) selection: Selection<'a>,
    pub(super) mapping: &'a [usize],
}
impl MannDataDomain<'_> {
    fn stage(&self) -> Result<AggregateFailureStage, KernelFailure> {
        match self.phase {
            AggregateInvocationPhase::Update => Ok(AggregateFailureStage::Update),
            AggregateInvocationPhase::Merge => Ok(AggregateFailureStage::Merge),
            _ => Err(invalid(
                "Mann-Whitney core call requires an update or merge domain",
            )),
        }
    }
    fn diagnostic(
        &self,
        message: &str,
        stage: AggregateFailureStage,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<InvocationData, KernelFailure> {
        let diagnostic = HostDiagnostic::prepare(self.allocator, work, |writer| {
            write!(writer, "{}", stage.message(message))
        })?;
        InvocationData::prepare_aggregate(
            self.allocator,
            Arc::clone(self.contract),
            self.phase,
            self.selection,
            self.mapping,
            diagnostic,
            work,
        )
    }
}

struct MannCallLoans {
    // Fields drop in declaration order: event backing ends before the prefix.
    events: [Option<OpaqueReservation>; 3],
    next: usize,
    prefix: OpaqueReservation,
}
impl MannCallLoans {
    fn acquire(charge: &OpaqueRetainedCharge) -> Result<Self, KernelFailure> {
        let bytes = crate::MAX_ROW_ERROR_MESSAGE_BYTES
            .checked_mul(2)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let prefix = charge.reserve_operation(bytes)?;
        Ok(Self {
            events: std::array::from_fn(|_| None),
            next: 0,
            prefix,
        })
    }
    fn reserve(
        &mut self,
        charge: &OpaqueRetainedCharge,
        bytes: usize,
    ) -> Result<(), KernelFailure> {
        if bytes == 0 {
            return Ok(());
        }
        let slot = self.events.get_mut(self.next).ok_or_else(|| {
            internal("Mann-Whitney call exceeded its three reached allocation frontiers")
        })?;
        if slot.is_some() {
            return Err(internal(
                "Mann-Whitney call attempted to overwrite a live event loan",
            ));
        }
        *slot = Some(charge.reserve_operation(bytes)?);
        self.next += 1;
        Ok(())
    }
}

pub(super) struct BoundMannResources<'a> {
    source: &'a MannSourceParams,
    charge: &'a OpaqueRetainedCharge,
    control: &'a dyn KernelEvaluationControl,
    loans: &'a mut MannCallLoans,
}
impl MannResources for BoundMannResources<'_> {
    fn before(
        &mut self,
        event: MannResourceEvent<'_>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), KernelFailure> {
        match work.flush() {
            Ok(()) => {}
            Err(ScalarStateError::Kernel(cause)) => return Err(cause),
            Err(_) => {
                return Err(internal(
                    "Mann-Whitney work flush returned a non-control failure",
                ));
            }
        }
        let facts = self.source.event_facts(event, self.control)?;
        self.loans.reserve(self.charge, facts.peak)
    }
}

// Private sealing prevents successful String/Vec/ArrayRef from escaping a
// temporary loan. The only successful payload is unit or a borrowed input view.
trait LoanFreeSuccess {}
impl LoanFreeSuccess for () {}
impl LoanFreeSuccess for MannWhitneyInput<'_> {}

fn call<T: LoanFreeSuccess>(
    source: &MannSourceParams,
    charge: &OpaqueRetainedCharge,
    domain: MannDataDomain<'_>,
    control: &dyn KernelEvaluationControl,
    operation: impl FnOnce(
        &mut ScalarWork<'_, '_>,
        &mut BoundMannResources<'_>,
    ) -> Result<T, ScalarStateError>,
) -> Result<T, EvaluationFailure> {
    // This outer scope outlives body locals, the returned original String,
    // partial Data construction and the single inner checkpoint cache.
    let mut loans = MannCallLoans::acquire(charge)?;
    if !loans.prefix.belongs_to_allocator(domain.allocator) {
        return Err(invalid(
            "Mann-Whitney call loans and typed allocator have different host authorities",
        )
        .into());
    }
    let stage = domain.stage()?;
    let mut checkpoints = EvaluationCheckpoints::new(control);
    let result = {
        let mut resources = BoundMannResources {
            source,
            charge,
            control,
            loans: &mut loans,
        };
        let mut work = ScalarWork::new(Some(&mut checkpoints));
        operation(&mut work, &mut resources)
    };
    match result {
        Ok(value) => {
            // finish consumes the only cache; it runs exclusively on success.
            checkpoints.finish()?;
            Ok(value)
        }
        Err(ScalarStateError::Kernel(cause)) => {
            drop(checkpoints);
            Err(cause.into())
        }
        Err(ScalarStateError::Legacy(message)) => {
            let converted = domain.diagnostic(&message, stage, &mut checkpoints);
            drop(message);
            drop(checkpoints);
            match converted {
                Ok(data) => Err(EvaluationFailure::InvocationData(data)),
                Err(cause) => Err(cause.into()),
            }
        }
        Err(ScalarStateError::OutputAllocation(message)) => {
            drop(message);
            drop(checkpoints);
            Err(KernelFailure::ResourceExhausted.into())
        }
    }
}

pub(super) fn call_unit(
    source: &MannSourceParams,
    charge: &OpaqueRetainedCharge,
    domain: MannDataDomain<'_>,
    control: &dyn KernelEvaluationControl,
    operation: impl FnOnce(
        &mut ScalarWork<'_, '_>,
        &mut BoundMannResources<'_>,
    ) -> Result<(), ScalarStateError>,
) -> Result<(), EvaluationFailure> {
    call(source, charge, domain, control, operation)
}

pub(super) fn call_input<'input>(
    source: &MannSourceParams,
    charge: &OpaqueRetainedCharge,
    domain: MannDataDomain<'_>,
    control: &dyn KernelEvaluationControl,
    operation: impl FnOnce(
        &mut ScalarWork<'_, '_>,
        &mut BoundMannResources<'_>,
    ) -> Result<MannWhitneyInput<'input>, ScalarStateError>,
) -> Result<MannWhitneyInput<'input>, EvaluationFailure> {
    call(source, charge, domain, control, operation)
}

#[cfg(test)]
mod tests;
