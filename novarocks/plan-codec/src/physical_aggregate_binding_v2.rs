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

//! Aggregate definitions borrowed from the same type and function emissions.
//! Occurrence effects, sequence closure and whole-package coverage remain with
//! their existing owners. Numerical envelopes are not formal MEM grants.

use crate::physical_binding_v2::owner_admission::{Admit, Policy};
use crate::{
    borrowed_type_resources::verify_type_binding,
    physical_binding_v2::{
        BindingCodecError, BindingProjectionFacts, BindingProjectionLimits,
        EncodedFunctionBindings, FunctionBindingInput, verify_scalar_signature,
    },
    physical_type_v2::EncodedTypeTable,
};
use novarocks_expression_contract::BoundFunction;
use novarocks_physical_plan::AggregateBinding;
#[cfg(test)]
use novarocks_physical_plan::AggregatePhase;
#[cfg(test)]
use novarocks_proto_models::physical_control_v2;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    AggregateStateArgumentContract, CompileCheckpoints, CompileControlError, CompilePhase,
    FunctionKind, PureCompileControl,
};
use std::{alloc::Layout, mem::size_of};

mod materialize;
mod read;
mod signature_copy;
pub use materialize::{
    MaterializedAggregateBindings, PreparedAggregateBindingsMaterialization,
    materialize_aggregate_bindings, materialize_aggregate_bindings_in,
    materialize_aggregate_bindings_with_creator_in, prepare_aggregate_bindings_materialization,
    prepare_aggregate_bindings_materialization_in,
};
pub use read::{
    PreparedAggregateBindingHeaders, prepare_aggregate_binding_headers,
    prepare_aggregate_binding_headers_in,
};
pub(crate) use signature_copy::{
    copy_aggregate_binding_observed, preflight_aggregate_binding_copy_counts,
    preflight_aggregate_binding_copy_counts_in, preflight_aggregate_binding_copy_types,
    preflight_aggregate_binding_copy_types_in,
};

// This is part of the original aggregate-binding codec, not a state payload decoder.
pub(crate) fn validate_state_interpretation(
    raw: &wire::AggregateStateInterpretation,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    if raw.distinct.is_none() {
        return Err(invalid("aggregate state DISTINCT interpretation is absent"));
    }
    work.step()?;
    for key in &raw.order_keys {
        let complete = key.ascending.is_some() && key.nulls_first.is_some();
        work.step()?;
        if !complete {
            return Err(invalid(
                "aggregate state ORDER interpretation is incomplete",
            ));
        }
    }
    Ok(())
}
pub(crate) fn encode_state_interpretation(
    source: &novarocks_type_contract::AggregateStateInterpretation,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::AggregateStateInterpretation, BindingCodecError> {
    work.flush()?;
    let mut keys = Vec::new();
    keys.try_reserve_exact(source.order_keys.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    for key in &source.order_keys {
        keys.push(wire::AggregateStateOrderKey {
            ascending: Some(key.ascending),
            nulls_first: Some(key.nulls_first),
        });
        work.step()?;
    }
    Ok(wire::AggregateStateInterpretation {
        distinct: Some(source.distinct),
        order_keys: keys,
    })
}
#[cfg(test)]
#[cfg(test)]
pub(crate) fn decode_state_interpretation(
    raw: &wire::AggregateStateInterpretation,
    work: &mut CompileCheckpoints<'_>,
) -> Result<novarocks_type_contract::AggregateStateInterpretation, BindingCodecError> {
    decode_state_interpretation_created::<std::convert::Infallible>(
        raw,
        &mut crate::physical_binding_v2::creation::Construction { creator: None },
        work,
    )
    .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}
pub(crate) fn decode_state_interpretation_created<H>(
    raw: &wire::AggregateStateInterpretation,
    construction: &mut crate::physical_binding_v2::creation::Construction<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    novarocks_type_contract::AggregateStateInterpretation,
    crate::physical_binding_v2::creation::Failure<H>,
> {
    validate_state_interpretation(raw, work)?;
    let mut keys = construction.reserve(raw.order_keys.len(), work)?;
    for key in &raw.order_keys {
        keys.push(novarocks_type_contract::AggregateStateOrderKey {
            ascending: key.ascending.expect("validated key"),
            nulls_first: key.nulls_first.expect("validated key"),
        });
        work.step()?;
    }
    Ok(novarocks_type_contract::AggregateStateInterpretation {
        distinct: raw.distinct.expect("validated interpretation"),
        order_keys: {
            let boxed = construction.boxed(keys, work)?;
            construction.collection(boxed, work)?
        },
    })
}

/// Projects the explicit state owner's contract, independently of function identity.
pub fn encode_state_argument_contract(contract: AggregateStateArgumentContract) -> i32 {
    match contract {
        AggregateStateArgumentContract::ExactSignature => {
            novarocks_proto_models::common::AggregateStateArgumentContract::ExactSignature as i32
        }
        AggregateStateArgumentContract::ValueRootNullabilityIndependent => {
            novarocks_proto_models::common::AggregateStateArgumentContract::ValueRootNullabilityIndependent as i32
        }
    }
}

pub fn decode_state_argument_contract(
    contract: i32,
) -> Result<AggregateStateArgumentContract, BindingCodecError> {
    match novarocks_proto_models::common::AggregateStateArgumentContract::try_from(contract) {
        Ok(novarocks_proto_models::common::AggregateStateArgumentContract::ExactSignature) => {
            Ok(AggregateStateArgumentContract::ExactSignature)
        }
        Ok(novarocks_proto_models::common::AggregateStateArgumentContract::ValueRootNullabilityIndependent) => {
            Ok(AggregateStateArgumentContract::ValueRootNullabilityIndependent)
        }
        Ok(novarocks_proto_models::common::AggregateStateArgumentContract::Unspecified) | Err(_) => Err(invalid(
            "aggregate header state argument contract is absent or unknown",
        )),
    }
}

#[derive(Clone, Copy)]
pub struct AggregateBindingInput<'source> {
    pub id: u32,
    pub source: &'source AggregateBinding,
    pub function_binding_id: u32,
    pub intermediate_value_type_id: u32,
}

/// Retains immutable loans to all original sources and their same emissions.
/// Singular zero/MAX references are present values, never absence sentinels.
pub struct EncodedAggregateBindings<'loan, 'source> {
    definitions: Vec<wire::AggregateBindingDefinition>,
    inputs: &'loan [AggregateBindingInput<'source>],
    types: &'loan EncodedTypeTable<'source>,
    _functions: &'loan EncodedFunctionBindings<'loan, 'source>,
    facts: BindingProjectionFacts,
}
impl<'loan, 'source> EncodedAggregateBindings<'loan, 'source> {
    pub fn as_wire(&self) -> &[wire::AggregateBindingDefinition] {
        &self.definitions
    }
    pub fn into_wire(self) -> Vec<wire::AggregateBindingDefinition> {
        self.definitions
    }
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
    pub fn binding_in(
        &self,
        id: u32,
        admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source AggregateBinding>, BindingCodecError> {
        admit(&crate::physical_binding_v2::owner_admission::lookup_facts(
            self.inputs.len(),
            self.inputs.len(),
        )?)?;
        self.binding_observed(id, work)
    }
    pub fn source_id_in(
        &self,
        source: &AggregateBinding,
        admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, BindingCodecError> {
        admit(&crate::physical_binding_v2::owner_admission::lookup_facts(
            self.inputs.len(),
            self.inputs.len(),
        )?)?;
        self.source_id_observed(source, work)
    }
    pub(crate) fn type_sources(&self) -> &'loan EncodedTypeTable<'source> {
        self.types
    }
    pub(crate) fn function_sources(&self) -> &'loan EncodedFunctionBindings<'loan, 'source> {
        self._functions
    }
    pub(crate) fn source_counts(&self) -> usize {
        self.inputs.len()
    }
    /// Resolve only the actual borrowed aggregate occurrence. Intentional
    /// aliases use the first ID in the original ascending namespace.
    pub(crate) fn source_id_observed(
        &self,
        source: &AggregateBinding,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, BindingCodecError> {
        for input in self.inputs {
            let same = std::ptr::eq(input.source, source);
            work.step()?;
            if same {
                return Ok(input.id);
            }
        }
        Err(BindingCodecError::InvalidShape(
            "aggregate signature is not an original emitted source",
        ))
    }
    pub(crate) fn binding_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source AggregateBinding>, BindingCodecError> {
        self.binding_captured(id, &mut |_, _| Ok(()), work)
    }
    pub(crate) fn binding_captured(
        &self,
        id: u32,
        capture: &mut impl FnMut(
            &'source AggregateBinding,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), BindingCodecError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source AggregateBinding>, BindingCodecError> {
        for input in self.inputs {
            let matches = input.id == id;
            if matches {
                capture(input.source, work)?;
            }
            work.step()?;
            if matches {
                return Ok(Some(input.source));
            }
        }
        Ok(None)
    }
}

pub fn encode_aggregate_bindings<'loan, 'source>(
    types: &'loan EncodedTypeTable<'source>,
    functions: &'loan EncodedFunctionBindings<'loan, 'source>,
    inputs: &'loan [AggregateBindingInput<'source>],
    source_retained_bytes: usize,
    limits: BindingProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<EncodedAggregateBindings<'loan, 'source>, BindingCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode(
        types,
        functions,
        inputs,
        (source_retained_bytes, limits, Policy(false)),
        &mut |_| Ok(()),
        &mut work,
    );
    if let Err(BindingCodecError::Control(cause)) = &result {
        return Err(BindingCodecError::Control(cause.clone()));
    }
    work.finish()?;
    let (definitions, facts) = result?;
    Ok(EncodedAggregateBindings {
        definitions,
        inputs,
        types,
        _functions: functions,
        facts,
    })
}

/// Same original sender, borrowing the parent's scope and growing admission.
pub fn encode_aggregate_bindings_in<'loan, 'source>(
    types: &'loan EncodedTypeTable<'source>,
    functions: &'loan EncodedFunctionBindings<'loan, 'source>,
    inputs: &'loan [AggregateBindingInput<'source>],
    source_retained_bytes: usize,
    limits: BindingProjectionLimits,
    admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedAggregateBindings<'loan, 'source>, BindingCodecError> {
    let (definitions, facts) = encode(
        types,
        functions,
        inputs,
        (source_retained_bytes, limits, Policy(true)),
        admit,
        work,
    )?;
    Ok(EncodedAggregateBindings {
        definitions,
        inputs,
        types,
        _functions: functions,
        facts,
    })
}

fn invalid(message: &'static str) -> BindingCodecError {
    BindingCodecError::InvalidShape(message)
}
fn add(left: usize, right: usize) -> Result<usize, BindingCodecError> {
    left.checked_add(right)
        .ok_or_else(|| invalid("aggregate projection arithmetic overflow"))
}
fn remaining(maximum: usize, used: usize) -> Result<usize, BindingCodecError> {
    maximum
        .checked_sub(used)
        .ok_or_else(|| invalid("aggregate work exceeds its envelope"))
}
fn bytes<T>(count: usize) -> Result<usize, BindingCodecError> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| invalid("aggregate projection layout is unrepresentable"))
}
fn request<T>(
    count: usize,
    facts: &mut BindingProjectionFacts,
    policy: Policy,
) -> Result<(), BindingCodecError> {
    let size = policy.bytes::<T>(count, "aggregate projection layout is unrepresentable")?;
    facts.request_bytes_upper_bound = policy.add(
        facts.request_bytes_upper_bound,
        size,
        "aggregate projection arithmetic overflow",
    )?;
    if size != 0 {
        facts.allocation_requests_upper_bound = policy.add(
            facts.allocation_requests_upper_bound,
            1,
            "aggregate projection arithmetic overflow",
        )?;
    }
    Ok(())
}
fn preflight(
    types: &EncodedTypeTable<'_>,
    functions: &EncodedFunctionBindings<'_, '_>,
    inputs: &[AggregateBindingInput<'_>],
    envelope: (usize, BindingProjectionLimits, Policy),
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BindingProjectionFacts, BindingCodecError> {
    let (source, limits, policy) = envelope;
    let sum = |a, b| policy.add(a, b, "aggregate projection arithmetic overflow");
    let product = |a, b| policy.mul(a, b, "aggregate projection arithmetic overflow");
    if policy.0 {
        let mut initial = BindingProjectionFacts {
            definition_count: inputs.len(),
            type_reference_count: inputs.len(),
            allocation_requests_upper_bound: 0,
            request_bytes_upper_bound: 0,
            coexisting_source_and_request_bytes_upper_bound: 0,
            cumulative_work_upper_bound: 0,
        };
        request::<wire::AggregateBindingDefinition>(inputs.len(), &mut initial, policy)?;
        let lookups = product(
            inputs.len(),
            sum(types.source_counts().0, functions.source_counts())?,
        )?;
        let base = sum(128, sum(product(inputs.len(), 20)?, lookups)?)?;
        refresh(&mut initial, source, base, 0, policy)?;
        policy.gate(&initial, limits, admit)?;
    }
    let same_types = std::ptr::eq(types, functions.type_sources());
    work.step()?;
    if !same_types {
        return Err(invalid(
            "aggregate and function bindings use different type emissions",
        ));
    }
    if inputs.len() > limits.max_definitions {
        return Err(invalid("aggregate definition count exceeds its envelope"));
    }
    let table = types.as_wire();
    // Known simultaneous source storage, before any output reservation. These
    // visible namespace floors cannot prove a host's full strings/metadata invoice.
    let visible_wire_floor = add(
        bytes::<AggregateBindingInput<'_>>(inputs.len())?,
        add(
            bytes::<wire::FunctionBindingDefinition>(functions.as_wire().len())?,
            add(
                bytes::<novarocks_proto_models::physical_type_v2::CarrierTypeDefinition>(
                    table.carriers.capacity(),
                )?,
                add(
                    bytes::<novarocks_proto_models::physical_type_v2::FieldDefinition>(
                        table.fields.capacity(),
                    )?,
                    bytes::<novarocks_proto_models::physical_type_v2::ValueTypeDefinition>(
                        table.value_types.capacity(),
                    )?,
                )?,
            )?,
        )?,
    )?;
    let visible_wire_floor = add(
        visible_wire_floor,
        bytes::<novarocks_proto_models::value_type::LogicalTypeDefinition>(
            table.logical_types.capacity(),
        )?,
    )?;
    let (value_roots, field_roots) = types.source_counts();
    let original_namespace_floor = add(
        bytes::<FunctionBindingInput<'_>>(functions.source_counts())?,
        add(
            bytes::<(u32, novarocks_type_contract::FunctionValueType)>(value_roots)?,
            bytes::<(u32, std::sync::Arc<arrow::datatypes::Field>)>(field_roots)?,
        )?,
    )?;
    let floor = add(visible_wire_floor, original_namespace_floor)?;
    if source < floor {
        return Err(invalid(
            "aggregate source invoice omits borrowed namespace storage",
        ));
    }
    let add = sum;
    let mul = product;
    let mut facts = BindingProjectionFacts {
        definition_count: inputs.len(),
        type_reference_count: inputs.len(),
        allocation_requests_upper_bound: 0,
        request_bytes_upper_bound: 0,
        coexisting_source_and_request_bytes_upper_bound: 0,
        cumulative_work_upper_bound: 0,
    };
    request::<wire::AggregateBindingDefinition>(inputs.len(), &mut facts, policy)?;
    let lookups = mul(
        inputs.len(),
        add(types.source_counts().0, functions.source_counts())?,
    )?;
    let base = add(128, add(mul(inputs.len(), 20)?, lookups)?)?;
    if policy.0 {
        refresh(&mut facts, source, base, 0, policy)?;
        policy.gate(&facts, limits, admit)?;
    }
    if base > limits.max_work {
        return Err(invalid("aggregate work exceeds its envelope"));
    }
    let mut previous = None;
    let mut chunks = 0;
    for input in inputs {
        let state = input.source.state_format.as_str();
        if let Some(receipt) = &input.source.state_interpretation {
            request::<wire::AggregateStateOrderKey>(receipt.order_keys.len(), &mut facts, policy)?;
            chunks = add(chunks, receipt.order_keys.len())?;
            if source
                < add(
                    size_of::<AggregateBinding>(),
                    add(
                        state.len(),
                        bytes::<novarocks_type_contract::AggregateStateOrderKey>(
                            receipt.order_keys.len(),
                        )?,
                    )?,
                )?
            {
                return Err(invalid(
                    "aggregate source invoice omits original state interpretation",
                ));
            }
            if policy.0 {
                refresh(&mut facts, source, base, chunks, policy)?;
                policy.gate(&facts, limits, admit)?;
            }
        }
        if policy.0 {
            request::<u8>(state.len(), &mut facts, policy)?;
            chunks = add(chunks, state.len().div_ceil(1024))?;
            refresh(&mut facts, source, base, chunks, policy)?;
            policy.gate(&facts, limits, admit)?;
        }
        let ordered = previous.is_none_or(|id| id < input.id);
        previous = Some(input.id);
        let is_aggregate = input.source.function.kind == FunctionKind::Aggregate;
        work.step()?;
        if !ordered {
            return Err(invalid("aggregate input IDs must be unique and ascending"));
        }
        if !is_aggregate {
            return Err(invalid("aggregate source has non-aggregate function kind"));
        }
        // Aliased aggregate owners are not summed as independent backing.
        if source < add(size_of::<AggregateBinding>(), state.len())? {
            return Err(invalid(
                "aggregate source invoice omits original aggregate backing",
            ));
        }
        if !policy.0 {
            request::<u8>(state.len(), &mut facts, policy)?;
            chunks = add(chunks, state.len().div_ceil(1024))?;
        }
    }
    if policy.0 {
        refresh(&mut facts, source, base, chunks, policy)?;
        policy.gate(&facts, limits, admit)?;
    }
    if facts.type_reference_count > limits.max_type_references {
        return Err(invalid("aggregate type references exceed their envelope"));
    }
    if facts.request_bytes_upper_bound > limits.max_request_bytes
        || facts.allocation_requests_upper_bound > limits.max_allocation_requests
    {
        return Err(invalid("aggregate output requests exceed their envelope"));
    }
    facts.coexisting_source_and_request_bytes_upper_bound =
        add(source, facts.request_bytes_upper_bound)?;
    if facts.coexisting_source_and_request_bytes_upper_bound
        > limits.max_coexisting_source_and_request_bytes
    {
        return Err(invalid("aggregate coexistence exceeds its envelope"));
    }
    facts.cumulative_work_upper_bound = add(
        base,
        add(
            mul(facts.request_bytes_upper_bound, 4)?,
            add(facts.allocation_requests_upper_bound, mul(chunks, 2)?)?,
        )?,
    )?;
    policy.gate(&facts, limits, admit)?;
    if facts.cumulative_work_upper_bound > limits.max_work {
        return Err(invalid("aggregate work exceeds its envelope"));
    }
    Ok(facts)
}
fn refresh(
    facts: &mut BindingProjectionFacts,
    source: usize,
    base: usize,
    chunks: usize,
    policy: Policy,
) -> Result<(), BindingCodecError> {
    let add = |a, b| policy.add(a, b, "aggregate projection arithmetic overflow");
    let mul = |a, b| policy.mul(a, b, "aggregate projection arithmetic overflow");
    facts.coexisting_source_and_request_bytes_upper_bound =
        add(source, facts.request_bytes_upper_bound)?;
    facts.cumulative_work_upper_bound = add(
        base,
        add(
            mul(facts.request_bytes_upper_bound, 4)?,
            add(facts.allocation_requests_upper_bound, mul(chunks, 2)?)?,
        )?,
    )?;
    Ok(())
}

fn validate(
    types: &EncodedTypeTable<'_>,
    functions: &EncodedFunctionBindings<'_, '_>,
    inputs: &[AggregateBindingInput<'_>],
    envelope: (usize, BindingProjectionLimits, Policy),
    facts: &mut BindingProjectionFacts,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    let (source, limits, policy) = envelope;
    for input in inputs {
        policy.gate(facts, limits, admit)?;
        if policy.0 {
            let found = functions.scalar_binding_captured(
                input.function_binding_id,
                &mut |function, work| {
                    let base = facts.cumulative_work_upper_bound;
                    let signature = crate::physical_binding_v2::verify_scalar_signature_admitted(
                        &input.source.function,
                        function,
                        source,
                        limits.max_work - base,
                        &mut |prefix| {
                            facts.cumulative_work_upper_bound = policy.add(
                                base,
                                prefix,
                                "aggregate projection arithmetic overflow",
                            )?;
                            policy.gate(facts, limits, admit)
                        },
                        work,
                    )?;
                    facts.cumulative_work_upper_bound = policy.add(
                        base,
                        signature.work_upper_bound(),
                        "aggregate projection arithmetic overflow",
                    )?;
                    if !signature.matches() {
                        return Err(invalid(
                            "aggregate function differs from its original source",
                        ));
                    }
                    Ok(())
                },
                work,
            )?;
            if found.is_none() {
                return Err(invalid(
                    "aggregate function binding ID is absent or relation-valued",
                ));
            }
            let found = types.value_type_captured::<BindingCodecError>(
                input.intermediate_value_type_id,
                &mut |intermediate, work| {
                    let base = facts.cumulative_work_upper_bound;
                    let compared = crate::borrowed_type_resources::verify_type_binding_admitted(
                        &input.source.intermediate_type,
                        intermediate,
                        source,
                        limits.max_work - base,
                        &mut |prefix| {
                            facts.cumulative_work_upper_bound = policy.add(
                                base,
                                prefix.work_upper_bound(),
                                "aggregate projection arithmetic overflow",
                            )?;
                            policy.gate(facts, limits, admit)
                        },
                        work,
                    )?;
                    facts.cumulative_work_upper_bound = policy.add(
                        base,
                        compared.work_upper_bound(),
                        "aggregate projection arithmetic overflow",
                    )?;
                    if !compared.matches() {
                        return Err(invalid(
                            "aggregate intermediate value type differs from its original source",
                        ));
                    }
                    Ok(())
                },
                work,
            )?;
            if found.is_none() {
                return Err(invalid("aggregate intermediate value type ID is absent"));
            }
        } else {
            let function: &BoundFunction = functions
                .scalar_binding_observed(input.function_binding_id, work)?
                .ok_or_else(|| {
                    invalid("aggregate function binding ID is absent or relation-valued")
                })?;
            let signature = verify_scalar_signature(
                &input.source.function,
                function,
                source,
                remaining(limits.max_work, facts.cumulative_work_upper_bound)?,
                work,
            )?;
            facts.cumulative_work_upper_bound = add(
                facts.cumulative_work_upper_bound,
                signature.work_upper_bound(),
            )?;
            if !signature.matches() {
                return Err(invalid(
                    "aggregate function differs from its original source",
                ));
            }
            let intermediate = types
                .value_type_observed(input.intermediate_value_type_id, work)?
                .ok_or_else(|| invalid("aggregate intermediate value type ID is absent"))?;
            let compared = verify_type_binding(
                &input.source.intermediate_type,
                intermediate,
                source,
                remaining(limits.max_work, facts.cumulative_work_upper_bound)?,
                work,
            )?;
            facts.cumulative_work_upper_bound = add(
                facts.cumulative_work_upper_bound,
                compared.work_upper_bound(),
            )?;
            if !compared.matches() {
                return Err(invalid(
                    "aggregate intermediate value type differs from its original source",
                ));
            }
        }
        work.step()?;
    }
    Ok(())
}
fn encode(
    types: &EncodedTypeTable<'_>,
    functions: &EncodedFunctionBindings<'_, '_>,
    inputs: &[AggregateBindingInput<'_>],
    envelope: (usize, BindingProjectionLimits, Policy),
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (
        Vec<wire::AggregateBindingDefinition>,
        BindingProjectionFacts,
    ),
    BindingCodecError,
> {
    let (source, limits, policy) = envelope;
    let mut facts = preflight(types, functions, inputs, envelope, admit, work)?;
    validate(
        types,
        functions,
        inputs,
        (source, limits, policy),
        &mut facts,
        admit,
        work,
    )?;
    work.flush()?;
    let mut definitions = Vec::new();
    let reserved = definitions.try_reserve_exact(inputs.len());
    crate::allocation_exit_v2::reserve_exit::<BindingCodecError>(reserved, work)?;
    for input in inputs {
        work.flush()?;
        let mut state_format = String::new();
        let reserved = state_format.try_reserve_exact(input.source.state_format.as_str().len());
        crate::allocation_exit_v2::reserve_exit::<BindingCodecError>(reserved, work)?;
        // This identity's sole constructor admits at most 1024 ASCII bytes.
        state_format.push_str(input.source.state_format.as_str());
        work.step()?;
        work.flush()?;
        definitions.push(wire::AggregateBindingDefinition {
            id: input.id,
            function_binding_id: Some(input.function_binding_id),
            phase: Some(crate::physical_value_origin_v2::encode_phase(
                input.source.phase,
            )),
            logical_argument_count: input.source.logical_argument_count,
            intermediate_value_type_id: Some(input.intermediate_value_type_id),
            state_format,
            state_interpretation: input
                .source
                .state_interpretation
                .as_ref()
                .map(|value| encode_state_interpretation(value, work))
                .transpose()?,
            state_argument_contract: encode_state_argument_contract(
                input.source.state_argument_contract,
            ),
        });
        work.step()?;
    }
    Ok((definitions, facts))
}

#[cfg(test)]
#[path = "physical_aggregate_binding_v2/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "physical_aggregate_binding_v2/state_interpretation_tests.rs"]
mod state_interpretation_tests;
