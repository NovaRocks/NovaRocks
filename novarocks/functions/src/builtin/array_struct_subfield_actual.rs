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
//! Complete original projection over an explicitly supplied actual call domain.
//! The Frame/source adapter owns argument evaluation and logical demand mapping.
use super::array_scalar_diagnostic_source::HostProjectionDiagnostics;
use super::array_struct_subfield_core;
use super::array_struct_subfield_port::ArrayStructSubfieldPort;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    AggregateStateAllocator, KernelEvaluationControl, ScalarCallContract, ScalarInvocationFailure,
    Selection,
};
use arrow_array::ArrayRef;
use std::sync::Arc;

pub(super) fn project_actual(
    source: &ArrayRef,
    names: &ArrayRef,
    actual_domain: Selection<'_>,
    contract: &Arc<ScalarCallContract>,
    field_ordinal: usize,
    allocator: &HostAggregateAllocator,
    host: &Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, ScalarInvocationFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    // No root, IF, slice, filter, constant or selected-domain guessing occurs
    // here. Every array has already been evaluated in this declared domain.
    if source.len() != actual_domain.len() || names.len() != actual_domain.len() {
        return Err(crate::kernel_control::invalid(
            "ARRAY projection operands differ from the explicit actual invocation domain",
        )
        .into());
    }
    let Some(crate::FunctionArgumentType::Value(expected_source)) =
        contract.selected().argument_types.first()
    else {
        return Err(
            crate::kernel_control::invalid("ARRAY projection has no declared source type").into(),
        );
    };
    if !novarocks_type_contract::arrow_data_types_exact_observed::<crate::KernelFailure>(
        source.data_type(),
        expected_source.data_type(),
        || work.step(),
    )? {
        return Err(crate::kernel_control::invalid(
            "ARRAY projection source differs from its prepared declaration",
        )
        .into());
    }
    let result = {
        let diagnostics = HostProjectionDiagnostics::try_new(
            allocator,
            Arc::clone(host),
            Arc::clone(contract),
            actual_domain,
            &mut work,
        )?;
        let mut port = ArrayStructSubfieldPort { diagnostics };
        array_struct_subfield_core::project_prepared_with_port(
            source,
            names,
            contract.result_type().data_type(),
            field_ordinal,
            &mut port,
        )?
    };
    work.flush()?;
    Ok(result)
}
