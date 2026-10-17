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

//! Exact ARRAY_SORTBY profile and original typed operation observations.
use super::array_scalar_diagnostic_source::HostProjectionDiagnostics;
use super::array_sortby_core::{SortByOperation, SortByPort};
use super::array_struct_subfield_core::ProjectionPort;
use crate::{
    FunctionArgumentType, FunctionBindingSelection, FunctionResultType, KernelFailure,
    ScalarCallContract, ScalarInvocationFailure,
};
use arrow_schema::DataType;
use novarocks_type_contract::{ArgumentControl, FunctionNullBehavior, ValueLogicalType};

pub(super) fn original_native_profile(selected: &FunctionBindingSelection, count: usize) -> bool {
    let FunctionResultType::Scalar(target) = &selected.result_type else {
        return false;
    };
    let Some(FunctionArgumentType::Value(source)) = selected.argument_types.first() else {
        return false;
    };
    (2..=7).contains(&count)
        && count == selected.argument_types.len()
        && selected.aggregate.is_none()
        && target.logical_type() == ValueLogicalType::Physical
        && target.nullable()
        && novarocks_type_contract::arrow_data_types_exact(source.data_type(), target.data_type())
        && selected.argument_types.iter().all(|arg| matches!(arg,
            FunctionArgumentType::Value(value)
            if value.logical_type() == ValueLogicalType::Physical && matches!(value.data_type(), DataType::List(_))))
}
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    observe()?;
    if contract.effects().argument_control != ArgumentControl::Eager
        || contract.effects().null_behavior != FunctionNullBehavior::CalledOnNull
        || !original_native_profile(
            contract.selected(),
            contract.call().logical_argument_count(),
        )
    {
        return Err(crate::kernel_control::invalid(
            "array_sortby requires its original List source and one through six independent ordered List keys",
        ));
    }
    observe()
}
struct Port<'port, 'work, 'control, 'rows>(
    &'port mut HostProjectionDiagnostics<'work, 'control, 'rows>,
);
impl SortByPort for Port<'_, '_, '_, '_> {
    type Failure = ScalarInvocationFailure;
    fn diagnostic(&mut self, original: std::fmt::Arguments<'_>) -> Self::Failure {
        ProjectionPort::diagnostic(self.0, original)
    }
    fn step(&mut self) -> Result<(), Self::Failure> {
        ProjectionPort::step(self.0)
    }
    fn before_operation(&mut self, _: SortByOperation<'_>) -> Result<(), Self::Failure> {
        // Actual opaque Arrow operations retain original semantics. The current
        // stage adds no guessed payload capacity or retired measurement grant.
        ProjectionPort::after_operation(self.0)
    }
    fn after_operation(&mut self) -> Result<(), Self::Failure> {
        ProjectionPort::after_operation(self.0)
    }
}

pub(super) fn evaluate<'a>(
    input: crate::ScopedScalarCallInput<'_, 'a>,
    contract: &std::sync::Arc<ScalarCallContract>,
    control: &dyn crate::KernelEvaluationControl,
    allocator: &crate::aggregate_host_allocator::HostAggregateAllocator,
    host: &std::sync::Arc<dyn crate::AggregateStateAllocator>,
) -> Result<crate::SelectedValues<'a>, ScalarInvocationFailure> {
    use super::array_invocation_source::{
        OwnedArraySource, actual_array, retain_output, select_output, source_failure,
    };
    use crate::kernel_input::EvaluationCheckpoints;
    use std::sync::Arc;
    control.checkpoint(0)?;
    if !std::ptr::eq(input.contract(), contract.as_ref()) {
        return Err(crate::kernel_control::invalid(
            "array_sortby invocation replaced its immutable contract",
        )
        .into());
    }
    validate_profile(input.contract(), || control.checkpoint(1))?;
    let scope = input.scope();
    let sources = input.sources();
    let mut work = EvaluationCheckpoints::new(control);
    let source = actual_array(&sources[0], scope, &mut work).map_err(|cause| {
        source_failure(
            cause,
            contract,
            scope.actual_domain(),
            allocator,
            host,
            &mut work,
        )
    })?;
    let actual = {
        let mut diagnostic = HostProjectionDiagnostics::try_new(
            allocator,
            Arc::clone(host),
            Arc::clone(contract),
            scope.actual_domain(),
            &mut work,
        )?;
        super::array_sortby_core::evaluate_with_port(
            &source,
            sources.len() - 1,
            |ordinal| {
                // Preserve the original source-first/key-evaluation order. Each
                // exact source expansion has its own bounded control observer.
                let mut key_work = EvaluationCheckpoints::new(control);
                let key =
                    actual_array(&sources[ordinal + 1], scope, &mut key_work).map_err(|cause| {
                        source_failure(
                            cause,
                            contract,
                            scope.actual_domain(),
                            allocator,
                            host,
                            &mut key_work,
                        )
                    })?;
                key_work.finish()?;
                Ok(key)
            },
            Some(contract.result_type().data_type()),
            scope.actual_domain().len(),
            &mut Port(&mut diagnostic),
        )?
    };
    // The whole original operation has succeeded. Only now may outer demand
    // select results; unselected malformed rows were not hidden from its math.
    let selected = select_output(&actual, scope, &mut work).map_err(|cause| {
        source_failure(
            cause,
            contract,
            scope.actual_domain(),
            allocator,
            host,
            &mut work,
        )
    })?;
    work.flush()?;
    let owned_sources = sources
        .iter()
        .map(OwnedArraySource::from_source)
        .collect::<Vec<_>>();
    work.flush()?;
    let result_type = contract.result_type().clone_observed(|| work.step())?;
    let retained = retain_output(
        selected,
        (owned_sources, result_type),
        Arc::clone(host),
        &mut work,
    )?;
    let output = crate::SelectedValues::try_new_observed::<KernelFailure>(
        scope.demand(),
        contract.result_type().data_type(),
        retained,
        Box::default(),
        || work.step(),
    )?;
    work.finish()?;
    Ok(output)
}
