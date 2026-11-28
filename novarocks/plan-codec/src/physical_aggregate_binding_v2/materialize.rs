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

//! Owned aggregate signature vocabulary from the original receiving owners.
//! This grants no installed implementation, phase graph, effects or MEM proof.

use super::{
    AggregateBinding, BindingCodecError, BindingProjectionFacts, BindingProjectionLimits,
    PreparedAggregateBindingHeaders, decode_state_argument_contract,
};
use crate::physical_binding_v2::owner_admission::{Admit, Policy};
use crate::{
    physical_binding_v2::{
        MaterializationModel, MaterializedFunctionBinding, MaterializedFunctionBindings, add,
        completed, finish, mul, preflight_scalar_signature_copy,
        preflight_scalar_signature_copy_in,
    },
    physical_type_v2::{DecodedTypeTable, clone_value_type_observed},
    physical_value_origin_v2::decode_phase_with,
};
use novarocks_expression_contract::BoundFunction;
use novarocks_type_contract::{
    AggregateStateFormatId, CompileCheckpoints, CompileControlError, CompilePhase, FunctionKind,
    FunctionValueType, PureCompileControl,
};
use std::mem::size_of;

type Error = BindingCodecError;
fn shape(message: &'static str) -> Error {
    Error::InvalidShape(message)
}

/// All seven original aggregate fields, in original definition order. The
/// function signature is an owned copy with explicitly absent legacy metadata.
/// Consumption of definitions ends the loans and does not confer provenance.
pub struct MaterializedAggregateBindings<'loan, 'headers, 'source> {
    definitions: Box<[(u32, AggregateBinding)]>,
    headers: &'loan PreparedAggregateBindingHeaders<'headers, 'source>,
    functions: &'loan MaterializedFunctionBindings<'headers, 'source>,
    source_invoice: usize,
    retained_bytes: usize,
    facts: BindingProjectionFacts,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
    construction_request_upper: Option<usize>,
}
impl<'loan, 'headers, 'source> MaterializedAggregateBindings<'loan, 'headers, 'source> {
    pub fn definitions(&self) -> &[(u32, AggregateBinding)] {
        &self.definitions
    }
    /// Completed original-author construction upper, including temporary
    /// requests. Pure authors have no supplied construction provenance.
    /// This is not precise retained bytes, a capacity grant or a MEM ledger.
    pub fn creation_request_bytes_upper_bound(&self) -> Option<usize> {
        self.construction_request_upper
    }
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
    pub(crate) fn headers(&self) -> &'loan PreparedAggregateBindingHeaders<'headers, 'source> {
        self.headers
    }
    pub(crate) fn functions(&self) -> &'loan MaterializedFunctionBindings<'headers, 'source> {
        self.functions
    }
    pub(crate) fn original_control(&self) -> &'source dyn PureCompileControl {
        self.headers.original_control()
    }
    /// The consuming caller admits repeated count-sized lookup work and owns
    /// its entry/footer. Sparse IDs never size a dense allocation.
    pub fn definition_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&AggregateBinding>, Error> {
        self.definition_captured(id, &mut |_, _| Ok(()), work)
    }
    pub fn definition_in(
        &self,
        id: u32,
        admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&AggregateBinding>, Error> {
        admit(&crate::physical_binding_v2::owner_admission::lookup_facts(
            self.definitions.len(),
            self.definitions.len(),
        )?)?;
        self.definition_observed(id, work)
    }
    pub(crate) fn definition_captured<'a>(
        &'a self,
        id: u32,
        capture: &mut impl FnMut(&'a AggregateBinding, &mut CompileCheckpoints<'_>) -> Result<(), Error>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'a AggregateBinding>, Error> {
        let same = std::ptr::addr_eq(work.control(), self.original_control());
        work.step()?;
        if !same {
            return Err(shape(
                "materialized aggregate lookup has a different original control",
            ));
        }
        for (candidate, binding) in &self.definitions {
            let matches = *candidate == id;
            if matches {
                capture(binding, work)?;
            }
            work.step()?;
            if matches {
                return Ok(Some(binding));
            }
        }
        Ok(None)
    }
    pub fn into_definitions(self) -> Box<[(u32, AggregateBinding)]> {
        self.definitions
    }
    /// This owned output only; cumulative Vec-to-Box request bytes are not its
    /// final retained backing. The original source invoice is excluded here.
    pub(crate) fn retained_output_floor(&self) -> Result<usize, Error> {
        add(size_of::<Self>(), self.retained_bytes)
    }
    /// Necessary composition floor, not a complete measured backing or grant.
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        add(self.source_invoice, self.retained_output_floor()?)
    }
}

/// Preparation keeps the same aggregate/function/type/control loans through
/// the sole consuming operation. It allocates no output signature.
pub struct PreparedAggregateBindingsMaterialization<'loan, 'headers, 'source> {
    headers: &'loan PreparedAggregateBindingHeaders<'headers, 'source>,
    functions: &'loan MaterializedFunctionBindings<'headers, 'source>,
    source_invoice: usize,
    retained_bytes: usize,
    facts: BindingProjectionFacts,
}
impl PreparedAggregateBindingsMaterialization<'_, '_, '_> {
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
}

fn function<'a>(
    functions: &'a MaterializedFunctionBindings<'_, '_>,
    id: u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'a BoundFunction, Error> {
    let found = functions.definition_observed(id, work)?;
    let result = match found {
        Some(MaterializedFunctionBinding::Scalar(binding))
            if binding.kind == FunctionKind::Aggregate =>
        {
            Ok(binding)
        }
        _ => Err(shape(
            "materialized aggregate function is absent or non-aggregate",
        )),
    };
    completed(result, work)
}
fn value<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, Error> {
    value_captured(types, id, &mut |_, _| Ok(()), work)
}
fn value_captured<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    capture: &mut impl FnMut(&'a FunctionValueType, &mut CompileCheckpoints<'_>) -> Result<(), Error>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, Error> {
    // The original decoded table uses a standard-library BTree lookup. Its
    // admitted comparison ceiling is numerical, not synthetic checkpoints.
    work.flush()?;
    let found = types.value_type(id);
    if let Some(source) = found {
        capture(source, work)?;
    }
    work.step()?;
    work.flush()?;
    found.ok_or_else(|| shape("materialized aggregate intermediate type is absent"))
}

fn preflight(
    headers: &PreparedAggregateBindingHeaders<'_, '_>,
    functions: &MaterializedFunctionBindings<'_, '_>,
    source: usize,
    limits: Option<BindingProjectionLimits>,
    observed: bool,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(BindingProjectionFacts, usize), Error> {
    let same = std::ptr::eq(functions.headers(), headers.functions());
    if !observed {
        work.step()?;
        if !same {
            return Err(shape(
                "aggregate materialization has a different original function namespace",
            ));
        }
    }
    // The header invoice already includes the original function/type source.
    // Only the separately owned function output is added to that source once.
    // Both borrowed floor authors are pure checked arithmetic/Layout. Keep
    // Plain's original errors while rejecting known numeric failures before
    // the caller-owned path's first completed namespace observation.
    let numeric_floor = |result: Result<usize, Error>| {
        result.map_err(|error| match error {
            Error::InvalidShape(_) if observed => CompileControlError::ResourceExhausted.into(),
            other => other,
        })
    };
    let header_floor = numeric_floor(headers.retained_invoice_floor())?;
    let function_floor = numeric_floor(functions.retained_output_floor())?;
    let known = if observed {
        Policy(true).add(
            header_floor,
            function_floor,
            "aggregate source arithmetic overflow",
        )?
    } else {
        add(header_floor, function_floor)?
    };
    let types = headers.functions().type_table();
    // Every aggregate owns at least its intermediate clone and the function's
    // result clone. This per-reference ceiling covers both function linear
    // lookups and both original decoded-type lookups without a second model.
    let lookup = add(
        mul(functions.definitions().len(), 2)?,
        add(types.value_types().len(), 1)?,
    )?;
    let mut model =
        MaterializationModel::for_composition(headers.as_wire().len(), lookup, source, known);
    // Admit one actual intermediate root per definition before the first
    // namespace lookup, including zero-argument aggregates.
    model.facts.type_reference_count = headers.facts().type_reference_count;
    model.request::<(u32, AggregateBinding)>(headers.as_wire().len(), 2)?;
    if observed {
        model.check_admitted(limits, admit)?;
        let control = std::ptr::addr_eq(work.control(), headers.original_control());
        work.step()?;
        if !same || !control {
            return Err(shape(
                "aggregate materialization has a different original function namespace or control",
            ));
        }
    } else {
        model.check(limits)?;
    }
    for raw in headers.as_wire() {
        if raw.intermediate_value_type_id.is_some() {
            model.request::<u8>(raw.state_format.len(), 1)?;
            model.request_layouts(
                AggregateStateFormatId::storage_allocation_layout()
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
                1,
            )?;
        }
        if let Some(receipt) = &raw.state_interpretation {
            model.request::<novarocks_type_contract::AggregateStateOrderKey>(
                receipt.order_keys.len(),
                2,
            )?;
            if !receipt.order_keys.is_empty() {
                model.request_layouts(
                    novarocks_type_contract::AggregateStateOrderKeys::storage_allocation_layout()
                        .map_err(Error::Resource)?,
                    1,
                )?;
            }
        }
        if observed {
            model.check_admitted(limits, admit)?;
        } else {
            model.check(limits)?;
        }
        let function_id = completed(
            raw.function_binding_id
                .ok_or_else(|| shape("materialized aggregate function reference is absent")),
            work,
        )?;
        if observed {
            let found = functions.definition_captured(
                function_id,
                &mut |source, work| {
                    if let MaterializedFunctionBinding::Scalar(binding) = source
                        && binding.kind == FunctionKind::Aggregate
                    {
                        preflight_scalar_signature_copy_in(
                            binding, &mut model, limits, admit, work,
                        )?;
                    }
                    Ok(())
                },
                work,
            )?;
            let result = match found {
                Some(MaterializedFunctionBinding::Scalar(binding))
                    if binding.kind == FunctionKind::Aggregate =>
                {
                    Ok(binding)
                }
                _ => Err(shape(
                    "materialized aggregate function is absent or non-aggregate",
                )),
            };
            completed(result, work)?;
        } else {
            let source = function(functions, function_id, work)?;
            preflight_scalar_signature_copy(source, &mut model, limits, work)?;
        }
        if let Some(id) = raw.intermediate_value_type_id {
            if observed {
                value_captured(
                    types,
                    id,
                    &mut |source, work| {
                        model.count_owned_type_clone_in(source, limits, admit, work)
                    },
                    work,
                )?;
                model.check_admitted(limits, admit)?;
            } else {
                model.count_owned_type_clone(value(types, id, work)?, limits, work)?;
                model.check(limits)?;
            }
        }
        work.step()?;
    }
    Ok((model.facts, model.retained))
}

pub fn prepare_aggregate_bindings_materialization<'loan, 'headers, 'source>(
    headers: &'loan PreparedAggregateBindingHeaders<'headers, 'source>,
    functions: &'loan MaterializedFunctionBindings<'headers, 'source>,
    source_retained_bytes: usize,
    limits: impl Into<Option<BindingProjectionLimits>>,
) -> Result<PreparedAggregateBindingsMaterialization<'loan, 'headers, 'source>, Error> {
    let mut work = CompileCheckpoints::try_new(headers.original_control(), CompilePhase::Decode)?;
    let result = preflight(
        headers,
        functions,
        source_retained_bytes,
        limits.into(),
        false,
        &mut |_| Ok(()),
        &mut work,
    )
    .map(
        |(facts, retained_bytes)| PreparedAggregateBindingsMaterialization {
            headers,
            functions,
            source_invoice: source_retained_bytes,
            retained_bytes,
            facts,
        },
    );
    finish(work, result)
}

pub fn materialize_aggregate_bindings<'loan, 'headers, 'source>(
    prepared: PreparedAggregateBindingsMaterialization<'loan, 'headers, 'source>,
) -> Result<MaterializedAggregateBindings<'loan, 'headers, 'source>, Error> {
    let mut work =
        CompileCheckpoints::try_new(prepared.headers.original_control(), CompilePhase::Decode)?;
    let result = materialize_core(prepared, &mut work);
    finish(work, result)
}
fn materialize_core<'loan, 'headers, 'source>(
    prepared: PreparedAggregateBindingsMaterialization<'loan, 'headers, 'source>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<MaterializedAggregateBindings<'loan, 'headers, 'source>, Error> {
    materialize_created_core::<std::convert::Infallible>(prepared, None, work)
        .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}
fn materialize_created_core<'loan, 'headers, 'source, H>(
    prepared: PreparedAggregateBindingsMaterialization<'loan, 'headers, 'source>,
    creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    MaterializedAggregateBindings<'loan, 'headers, 'source>,
    crate::physical_binding_v2::creation::Failure<H>,
> {
    let mut construction = crate::physical_binding_v2::creation::Construction { creator: creation };
    let storage = construction.storage();
    let construction_request_upper = construction
        .creator
        .as_ref()
        .map(|_| prepared.facts.request_bytes_upper_bound);
    work.step()?;
    (|| {
        let types = prepared.headers.functions().type_table();
        let mut definitions = construction.reserve(prepared.headers.as_wire().len(), work)?;
        for raw in prepared.headers.as_wire() {
            let function_id = completed(
                raw.function_binding_id
                    .ok_or_else(|| shape("materialized aggregate function reference is absent")),
                work,
            )?;
            let source = function(prepared.functions, function_id, work)?;
            let function = crate::physical_binding_v2::copy_scalar_signature_with_creator_observed(
                source,
                construction.creator_reborrow(),
                work,
            )?;
            let phase = completed(
                raw.phase
                    .as_ref()
                    .ok_or_else(|| shape("materialized aggregate phase is absent")),
                work,
            )?;
            let phase = completed(decode_phase_with(phase, shape), work)?;
            let merge_state = if let Some(id) = raw.intermediate_value_type_id {
                let source = value(types, id, work)?;
                work.flush()?;
                let intermediate_type = clone_value_type_observed(source, work)?;
                work.step()?;
                work.flush()?;
                let state_argument_contract = completed(
                    decode_state_argument_contract(raw.state_argument_contract),
                    work,
                )?;
                let state_format = construction.state(&raw.state_format, work)?;
                let state_interpretation = raw
                    .state_interpretation
                    .as_ref()
                    .map(|value| {
                        super::decode_state_interpretation_created(value, &mut construction, work)
                    })
                    .transpose()?;
                Some(novarocks_physical_plan::AggregateMergeBinding {
                    state_interpretation,
                    state_argument_contract,
                    intermediate_type,
                    state_format,
                })
            } else {
                None
            };
            definitions.push((
                raw.id,
                AggregateBinding {
                    function,
                    phase,
                    logical_argument_count: raw.logical_argument_count,
                    merge_state,
                },
            ));
            work.step()?;
        }
        Ok(MaterializedAggregateBindings {
            definitions: construction.boxed(definitions, work)?,
            headers: prepared.headers,
            functions: prepared.functions,
            source_invoice: prepared.source_invoice,
            retained_bytes: prepared.retained_bytes,
            facts: prepared.facts,
            _storage: storage,
            construction_request_upper,
        })
    })()
}

/// Same owned preparation and complete immutable loans, without entry/footer.
pub fn prepare_aggregate_bindings_materialization_in<'loan, 'headers, 'source>(
    headers: &'loan PreparedAggregateBindingHeaders<'headers, 'source>,
    functions: &'loan MaterializedFunctionBindings<'headers, 'source>,
    source_retained_bytes: usize,
    limits: impl Into<Option<BindingProjectionLimits>>,
    admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedAggregateBindingsMaterialization<'loan, 'headers, 'source>, Error> {
    let (facts, retained_bytes) = preflight(
        headers,
        functions,
        source_retained_bytes,
        limits.into(),
        true,
        admit,
        work,
    )?;
    Ok(PreparedAggregateBindingsMaterialization {
        headers,
        functions,
        source_invoice: source_retained_bytes,
        retained_bytes,
        facts,
    })
}
pub fn materialize_aggregate_bindings_in<'loan, 'headers, 'source>(
    prepared: PreparedAggregateBindingsMaterialization<'loan, 'headers, 'source>,
    admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<MaterializedAggregateBindings<'loan, 'headers, 'source>, Error> {
    admit(&prepared.facts)?;
    let same = std::ptr::addr_eq(work.control(), prepared.headers.original_control());
    work.step()?;
    if !same {
        return Err(shape(
            "aggregate materialization has a different original control",
        ));
    }
    materialize_core(prepared, work)
}

#[cfg(test)]
#[path = "materialize_tests.rs"]
mod tests;

/// Create the already validated aggregate table under the original host.
pub fn materialize_aggregate_bindings_with_creator_in<'loan, 'headers, 'source, H>(
    prepared: PreparedAggregateBindingsMaterialization<'loan, 'headers, 'source>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    MaterializedAggregateBindings<'loan, 'headers, 'source>,
    crate::physical_binding_v2::creation::Failure<H>,
> {
    let same = std::ptr::addr_eq(work.control(), prepared.headers.original_control());
    work.step()?;
    if !same {
        return Err(shape("aggregate materialization has a different original control").into());
    }
    materialize_created_core(prepared, Some(creation), work)
}
