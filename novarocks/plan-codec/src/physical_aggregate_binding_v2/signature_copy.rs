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

use crate::physical_binding_v2::owner_admission::Admit;
use crate::{
    physical_binding_v2::{
        BindingCodecError, BindingProjectionLimits, MaterializationModel, add,
        copy_scalar_signature_observed, preflight_scalar_signature_copy_counts,
        preflight_scalar_signature_copy_counts_in, preflight_scalar_signature_copy_types,
        preflight_scalar_signature_copy_types_in,
    },
    physical_type_v2::clone_value_type_observed,
};
use novarocks_physical_plan::AggregateBinding;
#[cfg(test)]
use novarocks_type_contract::AggregateStateFormatId;
use novarocks_type_contract::{CompileCheckpoints, FunctionKind};

fn header(
    source: &AggregateBinding,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    let aggregate = source.function.kind == FunctionKind::Aggregate;
    work.step()?;
    if !aggregate {
        return Err(BindingCodecError::InvalidShape(
            "aggregate copy requires an aggregate signature",
        ));
    }
    let exact = source.function.legacy_metadata.is_none();
    work.step()?;
    if !exact {
        return Err(BindingCodecError::InvalidShape(
            "aggregate copy requires an exact binding without legacy metadata",
        ));
    }
    Ok(())
}

/// Charge this actual owned aggregate occurrence into the caller's sole model.
/// The original namespace association and outer AggregateBinding Box belong to
/// the caller. This port neither authorizes compatibility nor grants resources.
#[cfg(test)]
pub(crate) fn preflight_aggregate_binding_copy(
    source: &AggregateBinding,
    model: &mut MaterializationModel,
    limits: impl Into<Option<BindingProjectionLimits>> + Copy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    preflight_aggregate_binding_copy_counts(source, model, limits, work)?;
    preflight_aggregate_binding_copy_types(source, model, limits, work)
}

pub(crate) fn preflight_aggregate_binding_copy_counts(
    source: &AggregateBinding,
    model: &mut MaterializationModel,
    limits: impl Into<Option<BindingProjectionLimits>> + Copy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    counts_core(source, model, limits.into(), None, work)
}
pub(crate) fn preflight_aggregate_binding_copy_counts_in(
    source: &AggregateBinding,
    model: &mut MaterializationModel,
    limits: impl Into<Option<BindingProjectionLimits>> + Copy,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    counts_core(source, model, limits.into(), Some(admit), work)
}
fn counts_core(
    source: &AggregateBinding,
    model: &mut MaterializationModel,
    limits: Option<BindingProjectionLimits>,
    mut admit: Option<&mut Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    if admit.is_some() {
        model.facts.type_reference_count = add(
            model.facts.type_reference_count,
            usize::from(source.merge_state.is_some()),
        )?;
    }
    model.check(limits)?;
    if let Some(parent) = admit.as_mut() {
        parent(&model.facts)?;
    }
    header(source, work)?;
    if admit.is_none() {
        model.facts.type_reference_count = add(
            model.facts.type_reference_count,
            usize::from(source.merge_state.is_some()),
        )?;
    }
    model.check(limits)?;
    if let Some(parent) = admit {
        parent(&model.facts)?;
        preflight_scalar_signature_copy_counts_in(&source.function, model, limits, parent, work)
    } else {
        preflight_scalar_signature_copy_counts(&source.function, model, limits, work)
    }
}
pub(crate) fn preflight_aggregate_binding_copy_types(
    source: &AggregateBinding,
    model: &mut MaterializationModel,
    limits: impl Into<Option<BindingProjectionLimits>> + Copy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    types_core(source, model, limits.into(), None, work)
}
pub(crate) fn preflight_aggregate_binding_copy_types_in(
    source: &AggregateBinding,
    model: &mut MaterializationModel,
    limits: impl Into<Option<BindingProjectionLimits>> + Copy,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    types_core(source, model, limits.into(), Some(admit), work)
}
fn types_core(
    source: &AggregateBinding,
    model: &mut MaterializationModel,
    limits: Option<BindingProjectionLimits>,
    admit: Option<&mut Admit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    if let Some(parent) = admit {
        preflight_scalar_signature_copy_types_in(&source.function, model, limits, parent, work)?;
        if let Some(intermediate) = source.intermediate_type() {
            model.count_owned_type_clone_in(intermediate, limits, parent, work)?;
        }
        model.check_admitted(limits, parent)?;
    } else {
        preflight_scalar_signature_copy_types(&source.function, model, limits, work)?;
        if let Some(intermediate) = source.intermediate_type() {
            model.count_owned_type_clone(intermediate, limits, work)?;
        }
    }
    work.step()?;
    model.check(limits)
}

/// Copy an already admitted occurrence with the original signature/type and
/// state identity authors. No entry/footer is added to the caller's scope.
pub(crate) fn copy_aggregate_binding_observed(
    source: &AggregateBinding,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AggregateBinding, BindingCodecError> {
    header(source, work)?;
    let function = copy_scalar_signature_observed(&source.function, work)?;
    work.flush()?;
    let merge_state = if let Some(state) = &source.merge_state {
        let intermediate_type = clone_value_type_observed(&state.intermediate_type, work)?;
        work.step()?;
        work.flush()?;
        let state_format = state.state_format.clone_observed(|| work.step())?;
        work.flush()?;
        Some(novarocks_physical_plan::AggregateMergeBinding {
            state_interpretation: state
                .state_interpretation
                .as_ref()
                .map(|value| value.clone_observed(work))
                .transpose()?,
            state_argument_contract: state.state_argument_contract,
            intermediate_type,
            state_format,
        })
    } else {
        None
    };
    let output = AggregateBinding {
        function,
        phase: source.phase,
        logical_argument_count: source.logical_argument_count,
        merge_state,
    };
    work.step()?;
    Ok(output)
}

#[cfg(test)]
#[path = "signature_copy_tests.rs"]
mod tests;
