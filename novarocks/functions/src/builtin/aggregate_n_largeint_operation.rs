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
//! Lossless N operation errors under one actual same-host prefix.
use super::aggregate_n_largeint_resources::{LEGACY_TEXT_UPPER, operation_prefix_bytes};
use crate::aggregate_format::AggregateFailureStage;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostDiagnostic;
use crate::aggregate_scalar::{ScalarStateError, ScalarWork};
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{
    AggregateCallContract, AggregateEmissionContext, AggregateInvocationPhase, EvaluationFailure,
    InvocationData, KernelEvaluationControl, KernelFailure, Selection,
};
use std::sync::Arc;

pub(super) enum NDataDomain<'a> {
    Mutation {
        contract: &'a Arc<AggregateCallContract>,
        phase: AggregateInvocationPhase,
        selection: Selection<'a>,
        mapping: &'a [usize],
    },
    Emission(&'a AggregateEmissionContext<'a>),
}
impl NDataDomain<'_> {
    fn stage(&self) -> Result<AggregateFailureStage, KernelFailure> {
        match self {
            Self::Mutation {
                phase: AggregateInvocationPhase::Update,
                ..
            } => Ok(AggregateFailureStage::Update),
            Self::Mutation {
                phase: AggregateInvocationPhase::Merge,
                ..
            } => Ok(AggregateFailureStage::Merge),
            Self::Emission(context) if context.phase() == AggregateInvocationPhase::Final => {
                Ok(AggregateFailureStage::BuildFinal)
            }
            Self::Emission(context)
                if context.phase() == AggregateInvocationPhase::Intermediate =>
            {
                Ok(AggregateFailureStage::BuildIntermediate)
            }
            _ => Err(invalid(
                "min_n/max_n core call has an unsupported invocation domain",
            )),
        }
    }
    fn diagnostic(
        &self,
        message: &str,
        allocator: &HostAggregateAllocator,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<InvocationData, KernelFailure> {
        let stage = self.stage()?;
        let diagnostic = HostDiagnostic::prepare(allocator, work, |writer| {
            write!(writer, "{}", stage.message(message))
        })?;
        match self {
            Self::Mutation {
                contract,
                phase,
                selection,
                mapping,
            } => InvocationData::prepare_aggregate(
                allocator,
                Arc::clone(contract),
                *phase,
                *selection,
                mapping,
                diagnostic,
                work,
            ),
            Self::Emission(context) => {
                InvocationData::prepare_emission(allocator, context, diagnostic, work)
            }
        }
    }
}
pub(super) fn classify(
    error: ScalarStateError,
    domain: &NDataDomain<'_>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> EvaluationFailure {
    match error {
        ScalarStateError::Kernel(cause) => cause.into(),
        ScalarStateError::OutputAllocation(message) => {
            drop(message);
            KernelFailure::ResourceExhausted.into()
        }
        ScalarStateError::Legacy(message) => {
            if message.len() > LEGACY_TEXT_UPPER {
                drop(message);
                return internal(
                    "min_n/max_n private producer exceeded its original diagnostic extent",
                )
                .into();
            }
            let converted = domain.diagnostic(&message, allocator, work);
            drop(message);
            match converted {
                Ok(data) => EvaluationFailure::InvocationData(data),
                Err(cause) => cause.into(),
            }
        }
    }
}
/// Successful owned output cannot escape this temporary prefix. Output has its
/// own whole-emission reservation and immutable backing custody path.
pub(super) fn call_unit(
    charge: &OpaqueRetainedCharge,
    allocator: &HostAggregateAllocator,
    domain: NDataDomain<'_>,
    control: &dyn KernelEvaluationControl,
    operation: impl FnOnce(&mut ScalarWork<'_, '_>) -> Result<(), ScalarStateError>,
) -> Result<(), EvaluationFailure> {
    let prefix = charge.reserve_operation(operation_prefix_bytes()?)?;
    if !prefix.belongs_to_allocator(allocator) {
        return Err(invalid("min_n/max_n call prefix uses another host authority").into());
    }
    let mut checkpoints = EvaluationCheckpoints::new(control);
    let result = {
        let mut work = ScalarWork::new(Some(&mut checkpoints));
        operation(&mut work)
    };
    match result {
        Ok(()) => {
            checkpoints.finish()?;
            Ok(())
        }
        Err(error) => {
            let failure = classify(error, &domain, allocator, &mut checkpoints);
            drop(checkpoints);
            Err(failure)
        }
    }
}
