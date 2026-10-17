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
//! C03 whole-call source adapter. It never evaluates expressions or guesses a domain.
use super::array_invocation_source::{
    OwnedArraySource, actual_array, retain_output, select_output, source_failure,
};
use super::array_scalar_diagnostic_source::HostProjectionDiagnostics;
use super::array_struct_subfield_actual::project_actual;
use super::array_struct_subfield_core::ProjectionPort;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    AggregateStateAllocator, KernelEvaluationControl, ScalarCallContract, ScalarInvocationFailure,
    ScopedScalarCallInput, SelectedValues,
};
use std::sync::Arc;

pub(super) fn evaluate_scoped<'a>(
    input: ScopedScalarCallInput<'_, 'a>,
    contract: &Arc<ScalarCallContract>,
    control: &dyn KernelEvaluationControl,
    allocator: &HostAggregateAllocator,
    host: &Arc<dyn AggregateStateAllocator>,
) -> Result<SelectedValues<'a>, ScalarInvocationFailure> {
    control.checkpoint(0)?;
    let scope = input.scope();
    let mut work = EvaluationCheckpoints::new(control);
    if let Some(failure) = crate::invocation_arity::ARRAY_STRUCT_SUBFIELD_ARITY
        .failure(contract.call().logical_argument_count())
    {
        let mut diagnostic = HostProjectionDiagnostics::try_new(
            allocator,
            Arc::clone(host),
            Arc::clone(contract),
            scope.actual_domain(),
            &mut work,
        )?;
        return Err(failure
            .with_original_message("array_struct_subfield", |args| diagnostic.diagnostic(args)));
    }
    let [source, names] = input.sources() else {
        return Err(crate::kernel_control::invalid(
            "ARRAY projection requires its actual two evaluated source channels",
        )
        .into());
    };
    let actual_sources = [
        OwnedArraySource::from_source(source),
        OwnedArraySource::from_source(names),
    ];
    let source = actual_array(source, scope, &mut work).map_err(|error| {
        source_failure(
            error,
            contract,
            scope.actual_domain(),
            allocator,
            host,
            &mut work,
        )
    })?;
    let names = actual_array(names, scope, &mut work).map_err(|error| {
        source_failure(
            error,
            contract,
            scope.actual_domain(),
            allocator,
            host,
            &mut work,
        )
    })?;
    let actual = project_actual(
        &source,
        &names,
        scope.actual_domain(),
        contract,
        allocator,
        host,
        control,
    )?;
    let selected = select_output(&actual, scope, &mut work).map_err(|error| {
        source_failure(
            error,
            contract,
            scope.actual_domain(),
            allocator,
            host,
            &mut work,
        )
    })?;
    let result_type = contract.result_type().clone_observed(|| work.step())?;
    let retained = retain_output(
        selected,
        (actual, source, names, actual_sources, result_type),
        Arc::clone(host),
        &mut work,
    )?;
    let result = SelectedValues::try_new_observed::<crate::KernelFailure>(
        scope.demand(),
        contract.result_type().data_type(),
        retained,
        Box::default(),
        || work.step(),
    )?;
    work.flush()?;
    Ok(result)
}
