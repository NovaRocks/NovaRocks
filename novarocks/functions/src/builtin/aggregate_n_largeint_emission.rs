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
//! Whole N gather/builder scope and last-alias custody, using original authors.
use super::aggregate_by::HostByBuffer;
use super::aggregate_n_core::NState;
use super::aggregate_n_largeint_operation::{NDataDomain, classify};
use super::aggregate_n_largeint_resources::{
    BoundNCodecResources, NEmissionGeometry, operation_prefix_bytes,
};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{self as scalar, ScalarStateError, ScalarWork};
use crate::arrow_result_custody::retain_result_backing_direct;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{
    AggregateCallContract, AggregateEmissionContext, AggregateStateAllocator, EvaluationFailure,
    KernelEvaluationControl, KernelFailure,
};
use allocator_api2::vec::Vec as HostVec;
use arrow_array::{ArrayRef, builder::BinaryBuilder};
use std::sync::Arc;

pub(super) trait NEmissionAccess {
    fn latch(&self);
    fn validate(
        &self,
        contract: &Arc<AggregateCallContract>,
        host: &Arc<dyn AggregateStateAllocator>,
    ) -> Result<(), KernelFailure>;
    fn with_core(
        &self,
        operation: impl FnOnce(&NState<HostAggregateAllocator>) -> Result<(), ScalarStateError>,
    ) -> Result<(), ScalarStateError>;
}
pub(super) fn emit_n<'s, S, I>(
    contract: &Arc<AggregateCallContract>,
    context: &AggregateEmissionContext<'_>,
    states: I,
    charge: OpaqueRetainedCharge,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, EvaluationFailure>
where
    S: NEmissionAccess + 's,
    I: ExactSizeIterator<Item = &'s S> + Clone,
{
    // The actual framework iterator clone borrows its state column and allocates
    // nothing. Prefix precedes even the sole local checkpoint cache/model fold.
    let prefix = charge.reserve_operation(operation_prefix_bytes()?)?;
    let mut work = EvaluationCheckpoints::new(control);
    if !Arc::ptr_eq(contract, context.contract())
        || states.len() != context.state_indices().len()
        || states.len() > context.row_capacity()
    {
        return Err(
            invalid("min_n/max_n emission differs from its actual call or state order").into(),
        );
    }
    let host = context
        .allocator()
        .ok_or_else(|| invalid("min_n/max_n emission requires its actual host allocator"))?;
    let allocator = HostAggregateAllocator::try_new_with_journal(Arc::clone(host))?;
    if !prefix.belongs_to_allocator(&allocator) {
        return Err(invalid("min_n/max_n emission prefix uses another host authority").into());
    }
    let domain = NDataDomain::Emission(context);
    let mut geometry = NEmissionGeometry::default();
    for state in states.clone() {
        work.step()?;
        state.validate(contract, host)?;
        let result =
            state.with_core(|core| geometry.add_state(core, &mut ScalarWork::new(Some(&mut work))));
        if let Err(error) = result {
            return Err(classify(error, &domain, &allocator, &mut work));
        }
    }
    if geometry.rows != states.len() {
        return Err(internal("min_n/max_n state iterator changed its extent").into());
    }
    let final_result = contract.phase().produces_final_result();
    let facts = geometry.facts(final_result)?;
    let mut operation = charge.reserve_operation(facts.operation)?;
    let result = (|| -> Result<ArrayRef, ScalarStateError> {
        let mut scalar_work = ScalarWork::new(Some(&mut work));
        if final_result {
            let mut values = Vec::new();
            values
                .try_reserve_exact(states.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            for state in states {
                scalar_work.step()?;
                state.with_core(|core| {
                    values.push(Some(core.output(&mut scalar_work)?));
                    Ok(())
                })?;
            }
            scalar::build_scalar_array_with_largeint_observation(
                &contract.final_type().data_type,
                values,
                &mut scalar_work,
            )
        } else {
            scalar_work.flush()?;
            let mut builder = BinaryBuilder::new();
            scalar_work.flush()?;
            for state in states {
                scalar_work.step()?;
                state.with_core(|core| {
                    let mut bytes = HostByBuffer(HostVec::new_in(core.allocator.clone()));
                    let mut codec = BoundNCodecResources {
                        charge: &charge,
                        allocator: &core.allocator,
                    };
                    core.serialize_with_codec_resources(&mut bytes, &mut codec, &mut scalar_work)?;
                    scalar_work.flush()?;
                    builder.append_value(&bytes.0);
                    scalar_work.flush()
                })?;
            }
            scalar_work.flush()?;
            let values = Arc::new(builder.finish()) as ArrayRef;
            scalar_work.flush()?;
            // The original 16-byte reset buffer ends under the whole loan.
            drop(builder);
            Ok(values)
        }
    })();
    let values = match result {
        Ok(values) => values,
        Err(error) => return Err(classify(error, &domain, &allocator, &mut work)),
    };
    let retained =
        retain_result_backing_direct(values, charge, &mut operation, allocator, &mut work)?;
    work.finish()?;
    Ok(retained.values)
}
