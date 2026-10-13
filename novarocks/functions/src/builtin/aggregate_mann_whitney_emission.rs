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

//! Original Mann-Whitney builders under one actual host operation envelope.

use super::aggregate_mann_whitney_core::{MannStateExtents, MannWhitneyState};
use super::aggregate_mann_whitney_resources::{MannEmissionResources, MannSourceParams};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{ScalarStateError, ScalarWork};
use crate::arrow_result_custody::{FlatByteKind, retain_result_backing_direct};
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::scalar_resources::ScalarResourceError;
use crate::{
    AggregateCallContract, AggregateEmissionContext, AggregateInvocationPhase, EvaluationFailure,
    KernelEvaluationControl, KernelFailure,
};
use arrow_array::{
    ArrayRef,
    builder::{BinaryBuilder, StringBuilder},
};
use arrow_schema::DataType;
use std::sync::Arc;

/// The concrete owner keeps its core private and !Sync. Each call opens just
/// one short state borrow; no sample or owned temporary escapes the callback.
pub(super) trait MannEmissionAccess {
    fn validate_host(
        &self,
        host: &Arc<dyn crate::AggregateStateAllocator>,
    ) -> Result<(), KernelFailure>;
    fn extents(&self) -> Result<MannStateExtents, KernelFailure>;
    fn with_core(
        &self,
        operation: impl FnOnce(
            &mut MannWhitneyState<HostAggregateAllocator>,
        ) -> Result<(), ScalarStateError>,
    ) -> Result<(), ScalarStateError>;
}

fn resource_failure(cause: ScalarResourceError) -> KernelFailure {
    match cause {
        ScalarResourceError::Arithmetic => KernelFailure::ResourceExhausted,
        ScalarResourceError::SourceModel(detail) => invalid(detail),
        ScalarResourceError::Invariant(detail) => internal(detail),
    }
}

fn core_failure(cause: ScalarStateError) -> KernelFailure {
    match cause {
        ScalarStateError::Kernel(cause) => cause,
        // The original serialize/render authors have no data/allocation-error
        // branch. Do not turn a future unknown producer into clipped Data.
        ScalarStateError::Legacy(message) | ScalarStateError::OutputAllocation(message) => {
            drop(message);
            internal("Mann-Whitney emission reached an unqualified core failure")
        }
    }
}

/// Production supplies the framework's borrowed TypedStateIter in exact order.
/// Its clone allocates nothing and the private State exposes no mutable handle
/// between the resource fold and body. Those are source predicates, not grants
/// inferred from this generic iterator bound.
pub(super) fn emit_mann<'state, S, I>(
    source: &MannSourceParams,
    expected_contract: &Arc<AggregateCallContract>,
    context: &AggregateEmissionContext<'_>,
    states: I,
    charge: OpaqueRetainedCharge,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, EvaluationFailure>
where
    S: MannEmissionAccess + 'state,
    I: ExactSizeIterator<Item = &'state S> + Clone,
{
    // This admission precedes model projection and the only local checkpoint
    // cache. The complete source peak already includes these two diagnostics.
    let bootstrap_bytes = crate::MAX_ROW_ERROR_MESSAGE_BYTES
        .checked_mul(2)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let bootstrap = charge.reserve_operation(bootstrap_bytes)?;
    let mut work = EvaluationCheckpoints::new(control);
    if !Arc::ptr_eq(expected_contract, context.contract()) {
        return Err(invalid("Mann-Whitney emission uses another call contract").into());
    }
    let rows = states.len();
    if rows != context.state_indices().len() || rows > context.row_capacity() {
        return Err(invalid("Mann-Whitney emission state order exceeds its host domain").into());
    }
    let kind = match context.phase() {
        AggregateInvocationPhase::Intermediate
            if expected_contract.intermediate_type().data_type == DataType::Binary =>
        {
            FlatByteKind::Binary
        }
        AggregateInvocationPhase::Final
            if expected_contract.final_type().data_type == DataType::Utf8 =>
        {
            FlatByteKind::Utf8
        }
        _ => return Err(invalid("Mann-Whitney emission has an unsupported phase or type").into()),
    };
    let host = context
        .allocator()
        .ok_or_else(|| invalid("Mann-Whitney emission requires its actual host allocator"))?;
    // Cold Inner and journal have their existing independent typed/opaque
    // admissions. Neither is the direct custody holder H in the flat facts.
    let allocator = HostAggregateAllocator::try_new_with_journal(Arc::clone(host))?;
    if !bootstrap.belongs_to_allocator(&allocator) {
        return Err(invalid("Mann-Whitney emission loans use another host authority").into());
    }
    let mut resources = MannEmissionResources::new(kind, rows);
    for state in states.clone() {
        work.step()?;
        state.validate_host(host)?;
        resources
            .add_state(state.extents()?, source)
            .map_err(resource_failure)?;
    }
    let facts = resources.finish(source).map_err(resource_failure)?;
    let extra_bytes = facts
        .operation_global_peak
        .checked_sub(bootstrap_bytes)
        .ok_or_else(|| internal("Mann-Whitney emission peak omitted its diagnostic prefix"))?;
    if facts.global_output_retained > extra_bytes
        || facts.global_settlement_charge_upper > extra_bytes
    {
        return Err(internal("Mann-Whitney emission retention exceeds its source envelope").into());
    }
    // Extra is declared before every body temporary; errors and unwind destroy
    // actual backing before either operation loan. No second diagnostic bill.
    let mut extra = charge.reserve_operation(extra_bytes)?;
    let values: ArrayRef = match kind {
        FlatByteKind::Binary => {
            work.flush()?;
            let mut builder = BinaryBuilder::new();
            work.flush()?;
            for state in states {
                work.step()?;
                state
                    .with_core(|core| {
                        let mut scalar_work = ScalarWork::new(Some(&mut work));
                        let bytes = core.serialize_with_work(&mut scalar_work)?;
                        scalar_work.flush()?;
                        builder.append_value(bytes);
                        scalar_work.flush()
                    })
                    .map_err(core_failure)?;
            }
            work.flush()?;
            let values = Arc::new(builder.finish());
            work.flush()?;
            // Drop the original builder reset buffers before custody.
            drop(builder);
            values
        }
        FlatByteKind::Utf8 => {
            work.flush()?;
            let mut builder = StringBuilder::new();
            work.flush()?;
            for state in states {
                work.step()?;
                state
                    .with_core(|core| {
                        let mut scalar_work = ScalarWork::new(Some(&mut work));
                        scalar_work.flush()?;
                        if core.is_uninitialized() || !core.has_samples() {
                            builder.append_null();
                        } else {
                            let value = core.build_result_with_work(&mut scalar_work)?;
                            scalar_work.flush()?;
                            builder.append_value(value);
                        }
                        scalar_work.flush()
                    })
                    .map_err(core_failure)?;
            }
            work.flush()?;
            let values = Arc::new(builder.finish());
            work.flush()?;
            drop(builder);
            values
        }
    };
    let retained = retain_result_backing_direct(values, charge, &mut extra, allocator, &mut work)?;
    // A failed completion destroys retained output before the remaining loans;
    // only a fully owned result can escape this function.
    work.finish()?;
    Ok(retained.values)
}

#[cfg(test)]
mod tests;
