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

use super::owner_admission::{Admit, HostAdmit, Policy};
use crate::host_projection_v2::{AdmissionRefusal, ProjectionFailure};
type HostError<H> = ProjectionFailure<BindingCodecError, H>;
use super::*;
use crate::borrowed_type_resources::verify_type_binding;
use novarocks_type_contract::{
    CompileControlError, FunctionArgumentType, FunctionKind, FunctionValueType,
};
use std::{alloc::Layout, mem::size_of};

fn invalid(message: &'static str) -> BindingCodecError {
    BindingCodecError::InvalidShape(message)
}
pub(super) fn add(left: usize, right: usize) -> Result<usize, BindingCodecError> {
    left.checked_add(right)
        .ok_or_else(|| invalid("binding projection arithmetic overflow"))
}
// Both selected source forms use the one original nominal kind projection.
fn scalar_kind(kind: FunctionKind) -> Result<wire::FunctionKind, BindingCodecError> {
    match kind {
        FunctionKind::Scalar => Ok(wire::FunctionKind::Scalar),
        FunctionKind::Aggregate => Ok(wire::FunctionKind::Aggregate),
        FunctionKind::Window => Ok(wire::FunctionKind::Window),
        FunctionKind::Table => Err(invalid("scalar binding source has table kind")),
    }
}
fn names<'a>(source: BindingSource<'a>) -> (&'a str, &'a str) {
    match source {
        BindingSource::Scalar(value) => (value.function_id.as_str(), value.overload.as_str()),
        BindingSource::Table(value) => (value.function_id.as_str(), value.overload.as_str()),
        BindingSource::ResolvedScalar(value) => {
            (value.function_id.as_str(), value.selected.overload.as_str())
        }
    }
}
fn arguments<'a>(source: BindingSource<'a>) -> &'a [FunctionArgumentType] {
    match source {
        BindingSource::Scalar(value) => &value.argument_types,
        BindingSource::Table(value) => &value.argument_types,
        BindingSource::ResolvedScalar(value) => &value.selected.argument_types,
    }
}
fn request<T>(
    count: usize,
    facts: &mut BindingProjectionFacts,
    policy: Policy,
) -> Result<(), BindingCodecError> {
    let bytes = policy.bytes::<T>(count, "binding output layout is unrepresentable")?;
    facts.request_bytes_upper_bound = policy.add(
        facts.request_bytes_upper_bound,
        bytes,
        "binding projection arithmetic overflow",
    )?;
    if bytes != 0 {
        facts.allocation_requests_upper_bound = policy.add(
            facts.allocation_requests_upper_bound,
            1,
            "binding projection arithmetic overflow",
        )?;
    }
    Ok(())
}
fn header_requests(
    function: &str,
    overload: &str,
    arguments: usize,
    facts: &mut BindingProjectionFacts,
    name_chunks: &mut usize,
    policy: Policy,
) -> Result<(), BindingCodecError> {
    for name in [function, overload] {
        request::<u8>(name.len(), facts, policy)?;
        *name_chunks = policy.add(
            *name_chunks,
            name.len().div_ceil(1024),
            "binding projection arithmetic overflow",
        )?;
    }
    request::<wire::FunctionArgumentType>(arguments, facts, policy)
}
fn preflight<H>(
    types: &EncodedTypeTable<'_>,
    inputs: &[FunctionBindingInput<'_>],
    source: usize,
    limits: Option<BindingProjectionLimits>,
    policy: Policy,
    admit: &mut HostAdmit<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BindingProjectionFacts, HostError<H>> {
    let add = |a, b| policy.add(a, b, "binding projection arithmetic overflow");
    let mul = |a, b| policy.mul(a, b, "binding projection arithmetic overflow");
    if !policy.0 && limits.is_some_and(|limits| inputs.len() > limits.max_definitions) {
        return Err(invalid("binding definition count exceeds its envelope").into());
    }
    let mut facts = BindingProjectionFacts {
        definition_count: inputs.len(),
        type_reference_count: 0,
        allocation_requests_upper_bound: 0,
        request_bytes_upper_bound: 0,
        coexisting_source_and_request_bytes_upper_bound: 0,
        cumulative_work_upper_bound: 0,
    };
    let root_bytes = Layout::array::<FunctionBindingInput<'_>>(inputs.len())
        .map_err(|_| invalid("binding source layout is unrepresentable"))?
        .size();
    if !policy.0 && source < root_bytes {
        return Err(invalid("binding source invoice omits original input roots").into());
    }
    // Admit fixed header/tail work and the original definition walk before it
    // starts. Empty tables still perform bounded projection bookkeeping.
    let own_prefix = add(128, mul(inputs.len(), 12)?)?;
    if !policy.0 && limits.is_some_and(|limits| own_prefix > limits.max_work) {
        return Err(invalid("binding work exceeds its envelope").into());
    }
    request::<wire::FunctionBindingDefinition>(inputs.len(), &mut facts, policy)?;
    let mut previous = None;
    let mut name_chunks = 0usize;
    if policy.0 {
        refresh(types, source, own_prefix, name_chunks, &mut facts, policy)?;
        policy.gate_with_host(&facts, limits, admit)?;
        if source < root_bytes {
            return Err(invalid("binding source invoice omits original input roots").into());
        }
    }
    for input in inputs {
        let args = arguments(input.source);
        let (function, overload) = names(input.source);
        if policy.0 {
            header_requests(
                function,
                overload,
                args.len(),
                &mut facts,
                &mut name_chunks,
                policy,
            )?;
            refresh(types, source, own_prefix, name_chunks, &mut facts, policy)?;
            policy.gate_with_host(&facts, limits, admit)?;
        }
        let ordered = previous.is_none_or(|id| id < input.id);
        previous = Some(input.id);
        work.step()?;
        if !ordered {
            return Err(invalid("binding input IDs must be unique and ascending").into());
        }
        if args.len() != input.arguments.len() {
            return Err(invalid("binding argument ID shape differs from its source").into());
        }
        if matches!(input.source, BindingSource::Scalar(value) if value.kind == FunctionKind::Table)
            || matches!(input.source, BindingSource::ResolvedScalar(value) if value.kind == FunctionKind::Table)
        {
            return Err(invalid("scalar binding source has table kind").into());
        }
        let identity_bytes = add(function.len(), overload.len())?;
        // Reused source owners/ID slices can alias. Only actual root-slice
        // storage plus a known individual backing lower bound is required;
        // summing every aliased source occurrence would invent retention.
        let mut known = match input.source {
            BindingSource::Scalar(_) => size_of::<BoundFunction>(),
            BindingSource::ResolvedScalar(_) => {
                size_of::<novarocks_functions::ResolvedFunctionBinding>()
            }
            BindingSource::Table(value) => add(
                size_of::<BoundTableFunction>(),
                Layout::array::<FunctionValueType>(value.result_types.len())
                    .map_err(|_| invalid("binding result source layout is unrepresentable"))?
                    .size(),
            )?,
        };
        known = add(
            add(known, identity_bytes)?,
            Layout::array::<FunctionArgumentType>(args.len())
                .map_err(|_| invalid("binding argument source layout is unrepresentable"))?
                .size(),
        )?;
        let ids_bytes = Layout::array::<ArgumentTypeIds<'_>>(input.arguments.len())
            .map_err(|_| invalid("binding argument source layout is unrepresentable"))?
            .size();
        let mut u32_ids_bytes = 0usize;
        if !policy.0 {
            header_requests(
                function,
                overload,
                args.len(),
                &mut facts,
                &mut name_chunks,
                policy,
            )?;
        }
        for (argument, ids) in args.iter().zip(input.arguments) {
            let references = match (argument, ids) {
                (FunctionArgumentType::Value(_), ArgumentTypeIds::Value(_))
                | (
                    FunctionArgumentType::CandidateGroup { .. },
                    ArgumentTypeIds::CandidateGroup { members: [], .. },
                ) => 1,
                (
                    FunctionArgumentType::Lambda {
                        parameter_types, ..
                    },
                    ArgumentTypeIds::Lambda { parameters, .. },
                ) if parameter_types.len() == parameters.len() => {
                    known = add(
                        known,
                        Layout::array::<FunctionValueType>(parameter_types.len())
                            .map_err(|_| {
                                invalid("binding lambda source layout is unrepresentable")
                            })?
                            .size(),
                    )?;
                    u32_ids_bytes = u32_ids_bytes.max(
                        Layout::array::<u32>(parameters.len())
                            .map_err(|_| invalid("binding lambda ID layout is unrepresentable"))?
                            .size(),
                    );
                    request::<u32>(parameters.len(), &mut facts, policy)?;
                    add(parameters.len(), 1)?
                }
                _ => {
                    return Err(invalid("binding argument ID shape differs from its source").into());
                }
            };
            facts.type_reference_count = add(facts.type_reference_count, references)?;
            if policy.0 {
                refresh(types, source, own_prefix, name_chunks, &mut facts, policy)?;
                policy.gate_with_host(&facts, limits, admit)?;
            }
            work.step()?;
            if limits.is_some_and(|limits| facts.type_reference_count > limits.max_type_references)
            {
                return Err(invalid("binding type references exceed their envelope").into());
            }
        }
        let results = match (input.source, input.result) {
            (BindingSource::Scalar(_), ResultTypeIds::Scalar(_)) => 1,
            (BindingSource::ResolvedScalar(value), ResultTypeIds::Scalar(_))
                if matches!(
                    value.selected.result_type,
                    novarocks_functions::FunctionResultType::Scalar(_)
                ) =>
            {
                1
            }
            (BindingSource::Table(value), ResultTypeIds::Relation(ids))
                if value.result_types.len() == ids.len() =>
            {
                u32_ids_bytes = u32_ids_bytes.max(
                    Layout::array::<u32>(ids.len())
                        .map_err(|_| invalid("binding relation ID layout is unrepresentable"))?
                        .size(),
                );
                request::<u32>(ids.len(), &mut facts, policy)?;
                ids.len()
            }
            _ => return Err(invalid("binding result ID shape differs from its source").into()),
        };
        facts.type_reference_count = add(facts.type_reference_count, results)?;
        if policy.0 {
            refresh(types, source, own_prefix, name_chunks, &mut facts, policy)?;
            policy.gate_with_host(&facts, limits, admit)?;
        }
        work.step()?;
        // Root inputs, owned signature storage and typed ID slices are
        // distinct live storage. The u32 slices can alias one another, so
        // only their maximum known extent is added for this source.
        if source < add(add(root_bytes, known)?, add(ids_bytes, u32_ids_bytes)?)? {
            return Err(invalid("binding source invoice omits original signature backing").into());
        }
    }
    if limits.is_some_and(|limits| facts.type_reference_count > limits.max_type_references) {
        return Err(invalid("binding type references exceed their envelope").into());
    }
    if limits.is_some_and(|limits| {
        facts.request_bytes_upper_bound > limits.max_request_bytes
            || facts.allocation_requests_upper_bound > limits.max_allocation_requests
    }) {
        return Err(invalid("binding output requests exceed their envelope").into());
    }
    facts.coexisting_source_and_request_bytes_upper_bound =
        add(source, facts.request_bytes_upper_bound)?;
    if limits.is_some_and(|limits| {
        facts.coexisting_source_and_request_bytes_upper_bound
            > limits.max_coexisting_source_and_request_bytes
    }) {
        return Err(invalid("binding coexistence exceeds its envelope").into());
    }
    refresh(types, source, own_prefix, name_chunks, &mut facts, policy)?;
    policy.gate_with_host(&facts, limits, admit)?;
    if limits.is_some_and(|limits| facts.cumulative_work_upper_bound > limits.max_work) {
        return Err(invalid("binding work exceeds its envelope").into());
    }
    Ok(facts)
}
fn refresh(
    types: &EncodedTypeTable<'_>,
    source: usize,
    own_prefix: usize,
    name_chunks: usize,
    facts: &mut BindingProjectionFacts,
    policy: Policy,
) -> Result<(), BindingCodecError> {
    let add = |a, b| policy.add(a, b, "binding projection arithmetic overflow");
    let mul = |a, b| policy.mul(a, b, "binding projection arithmetic overflow");
    facts.coexisting_source_and_request_bytes_upper_bound =
        add(source, facts.request_bytes_upper_bound)?;
    let lookups = mul(facts.type_reference_count, types.source_counts().0)?;
    let owned = add(own_prefix, mul(facts.type_reference_count, 8)?)?;
    let copies = add(
        mul(facts.request_bytes_upper_bound, 4)?,
        facts.allocation_requests_upper_bound,
    )?;
    facts.cumulative_work_upper_bound =
        add(add(lookups, owned)?, add(copies, mul(name_chunks, 2)?)?)?;
    Ok(())
}
fn verify_id<H>(
    types: &EncodedTypeTable<'_>,
    id: u32,
    expected: &FunctionValueType,
    envelope: (usize, Option<BindingProjectionLimits>, Policy),
    facts: &mut BindingProjectionFacts,
    admit: &mut HostAdmit<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), HostError<H>> {
    let (source, limits, policy) = envelope;
    policy.gate_with_host(facts, limits, admit)?;
    let actual = types
        .value_type_observed(id, work)?
        .ok_or_else(|| invalid("binding value type ID is absent"))?;
    let base = facts.cumulative_work_upper_bound;
    let mut capture = |prefix: crate::borrowed_type_resources::BoundTypeComparisonFacts| {
        facts.cumulative_work_upper_bound = policy.add(
            base,
            prefix.work_upper_bound(),
            "binding projection arithmetic overflow",
        )?;
        policy.gate_with_host(facts, limits, admit)
    };
    let verified = if let Some(limits) = limits {
        let remaining = limits
            .max_work
            .checked_sub(base)
            .ok_or_else(|| invalid("binding work exceeds its envelope"))?;
        if policy.0 {
            crate::borrowed_type_resources::verify_type_binding_admitted(
                expected,
                actual,
                source,
                remaining,
                &mut capture,
                work,
            )?
        } else {
            verify_type_binding(expected, actual, source, remaining, work)?
        }
    } else {
        crate::borrowed_type_resources::verify_type_binding_optional_work_in(
            expected,
            actual,
            source,
            None,
            &mut capture,
            work,
        )?
    };
    facts.cumulative_work_upper_bound = add(base, verified.work_upper_bound())?;
    policy.gate_with_host(facts, limits, admit)?;
    if !verified.matches() {
        return Err(invalid("binding value type differs from its original source").into());
    }
    Ok(())
}
fn validate<H>(
    types: &EncodedTypeTable<'_>,
    inputs: &[FunctionBindingInput<'_>],
    envelope: (usize, Option<BindingProjectionLimits>, Policy),
    facts: &mut BindingProjectionFacts,
    admit: &mut HostAdmit<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), HostError<H>> {
    for input in inputs {
        for (argument, ids) in arguments(input.source).iter().zip(input.arguments) {
            match (argument, ids) {
                (FunctionArgumentType::Value(value), ArgumentTypeIds::Value(id))
                | (
                    FunctionArgumentType::CandidateGroup {
                        element_type: value,
                    },
                    ArgumentTypeIds::CandidateGroup {
                        element: id,
                        members: [],
                    },
                ) => verify_id(types, *id, value, envelope, facts, admit, work)?,
                (
                    FunctionArgumentType::Lambda {
                        parameter_types,
                        result_type,
                    },
                    ArgumentTypeIds::Lambda { parameters, result },
                ) => {
                    for (value, id) in parameter_types.iter().zip(*parameters) {
                        verify_id(types, *id, value, envelope, facts, admit, work)?;
                    }
                    verify_id(types, *result, result_type, envelope, facts, admit, work)?;
                }
                _ => {
                    return Err(invalid("binding argument ID shape differs from its source").into());
                }
            }
            work.step()?;
        }
        match (input.source, input.result) {
            (BindingSource::Scalar(value), ResultTypeIds::Scalar(id)) => {
                verify_id(types, id, &value.result_type, envelope, facts, admit, work)?
            }
            (BindingSource::ResolvedScalar(value), ResultTypeIds::Scalar(id)) => {
                let novarocks_functions::FunctionResultType::Scalar(result) =
                    &value.selected.result_type
                else {
                    return Err(invalid("binding result ID shape differs from its source").into());
                };
                verify_id(types, id, result, envelope, facts, admit, work)?;
            }
            (BindingSource::Table(value), ResultTypeIds::Relation(ids)) => {
                for (value, id) in value.result_types.iter().zip(ids) {
                    verify_id(types, *id, value, envelope, facts, admit, work)?;
                }
            }
            _ => return Err(invalid("binding result ID shape differs from its source").into()),
        }
        work.step()?;
    }
    Ok(())
}
pub(super) fn owned_backing_bytes(
    definitions: &Vec<wire::FunctionBindingDefinition>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, BindingCodecError> {
    // Invoice only the already-owned DTO's real allocation-request capacities.
    // This never walks source IR, grants stock, validates a binding or emits data.
    let policy = Policy(true);
    let mut bytes = policy.bytes::<wire::FunctionBindingDefinition>(
        definitions.capacity(),
        "binding output layout is unrepresentable",
    )?;
    for definition in definitions {
        bytes = policy.add(
            bytes,
            definition.function_id.capacity(),
            "binding projection arithmetic overflow",
        )?;
        bytes = policy.add(
            bytes,
            definition.overload_id.capacity(),
            "binding projection arithmetic overflow",
        )?;
        bytes = policy.add(
            bytes,
            policy.bytes::<wire::FunctionArgumentType>(
                definition.arguments.capacity(),
                "binding output layout is unrepresentable",
            )?,
            "binding projection arithmetic overflow",
        )?;
        work.step()?;
        for argument in &definition.arguments {
            if let Some(wire::function_argument_type::Kind::Lambda(lambda)) = &argument.kind {
                bytes = policy.add(
                    bytes,
                    policy.bytes::<u32>(
                        lambda.parameter_value_type_ids.capacity(),
                        "binding output layout is unrepresentable",
                    )?,
                    "binding projection arithmetic overflow",
                )?;
            }
            work.step()?;
        }
        if let Some(wire::function_binding_definition::Result::Relation(relation)) =
            &definition.result
        {
            bytes = policy.add(
                bytes,
                policy.bytes::<u32>(
                    relation.value_type_ids.capacity(),
                    "binding output layout is unrepresentable",
                )?,
                "binding projection arithmetic overflow",
            )?;
        }
        work.step()?;
    }
    Ok(bytes)
}
fn vector<T>(count: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, BindingCodecError> {
    work.flush()?;
    let mut value = Vec::new();
    value
        .try_reserve_exact(count)
        .map_err(|_| BindingCodecError::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    Ok(value)
}
fn string(source: &str, work: &mut CompileCheckpoints<'_>) -> Result<String, BindingCodecError> {
    work.flush()?;
    let mut output = String::new();
    output
        .try_reserve_exact(source.len())
        .map_err(|_| BindingCodecError::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    // Identity owners enforce at most 1024 UTF-8 bytes. The complete
    // copy is therefore one bounded operation, bracketed on this meter.
    output.push_str(source);
    work.step()?;
    work.flush()?;
    Ok(output)
}
pub(super) fn encode(
    types: &EncodedTypeTable<'_>,
    inputs: &[FunctionBindingInput<'_>],
    source: usize,
    limits: Option<BindingProjectionLimits>,
    policy: Policy,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Vec<wire::FunctionBindingDefinition>, BindingProjectionFacts), BindingCodecError> {
    encode_with_host(
        types,
        inputs,
        source,
        limits,
        policy,
        &mut |facts| admit(facts).map_err(AdmissionRefusal::<std::convert::Infallible>::Control),
        work,
    )
    .map_err(ProjectionFailure::without_host)
}

pub(super) fn encode_with_host<H>(
    types: &EncodedTypeTable<'_>,
    inputs: &[FunctionBindingInput<'_>],
    source: usize,
    limits: Option<BindingProjectionLimits>,
    policy: Policy,
    admit: &mut HostAdmit<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Vec<wire::FunctionBindingDefinition>, BindingProjectionFacts), HostError<H>> {
    let mut facts = preflight(types, inputs, source, limits, policy, admit, work)?;
    validate(
        types,
        inputs,
        (source, limits, policy),
        &mut facts,
        admit,
        work,
    )?;
    let mut output = vector(inputs.len(), work)?;
    for input in inputs {
        output.push(emit_binding_definition(
            input,
            &mut OriginalBindingEmission(work),
        )?);
        work.step()?;
    }
    Ok((output, facts))
}

/// Sole field emission body. Type references have already been checked by the
/// enclosing actual namespace owner. This is not binding or capability proof.
pub(crate) trait BindingEmission {
    type Error: From<BindingCodecError>;
    fn vector<T>(&mut self, count: usize) -> Result<Vec<T>, Self::Error>;
    fn string(&mut self, source: &str) -> Result<String, Self::Error>;
    fn step(&mut self) -> Result<(), Self::Error>;
}
struct OriginalBindingEmission<'a, 'c>(&'a mut CompileCheckpoints<'c>);
impl BindingEmission for OriginalBindingEmission<'_, '_> {
    type Error = BindingCodecError;
    fn vector<T>(&mut self, count: usize) -> Result<Vec<T>, Self::Error> {
        vector(count, self.0)
    }
    fn string(&mut self, source: &str) -> Result<String, Self::Error> {
        string(source, self.0)
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        self.0.step().map_err(Into::into)
    }
}
pub(crate) fn emit_binding_definition<P: BindingEmission>(
    input: &FunctionBindingInput<'_>,
    emission: &mut P,
) -> Result<wire::FunctionBindingDefinition, P::Error> {
    let mut args = emission.vector(input.arguments.len())?;
    for ids in input.arguments {
        let kind = match ids {
            ArgumentTypeIds::Value(id) => wire::function_argument_type::Kind::ValueTypeId(*id),
            ArgumentTypeIds::CandidateGroup { element, .. } => {
                wire::function_argument_type::Kind::CandidateGroupElementValueTypeId(*element)
            }

            ArgumentTypeIds::Lambda { parameters, result } => {
                let mut ids = emission.vector(parameters.len())?;
                for id in *parameters {
                    ids.push(*id);
                    emission.step()?;
                }
                wire::function_argument_type::Kind::Lambda(wire::LambdaArgumentType {
                    parameter_value_type_ids: ids,
                    result_value_type_id: Some(*result),
                })
            }
        };
        args.push(wire::FunctionArgumentType { kind: Some(kind) });
        emission.step()?;
    }
    let result = match input.result {
        ResultTypeIds::Scalar(id) => {
            wire::function_binding_definition::Result::ScalarValueTypeId(id)
        }
        ResultTypeIds::Relation(ids) => {
            let mut values = emission.vector(ids.len())?;
            for id in ids {
                values.push(*id);
                emission.step()?;
            }
            wire::function_binding_definition::Result::Relation(wire::RelationResultTypes {
                value_type_ids: values,
            })
        }
    };
    let kind = match input.source {
        BindingSource::Scalar(value) => scalar_kind(value.kind)?,
        BindingSource::ResolvedScalar(value) => scalar_kind(value.kind)?,
        BindingSource::Table(_) => wire::FunctionKind::Table,
    };
    let (function, overload) = names(input.source);
    // All FIVE legacy fields are deliberately excluded: volatility,
    // argument_evaluation, failure_behavior, intrinsic_row_error and
    // semantic_parameters belong to migration/occurrence owners.
    Ok(wire::FunctionBindingDefinition {
        id: input.id,
        function_id: emission.string(function)?,
        overload_id: emission.string(overload)?,
        kind: kind as i32,
        arguments: args,
        result: Some(result),
    })
}

pub(crate) fn verify_scalar_signature(
    left: &BoundFunction,
    right: &BoundFunction,
    source: usize,
    max_work: impl Into<Option<usize>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedSignature, BindingCodecError> {
    let result = verify_signature_inner(left, right, source, max_work.into(), None, work);
    if matches!(&result, Err(BindingCodecError::Control(_))) {
        return result;
    }
    work.flush()?;
    result
}
/// Cumulative same-signature comparison facts before each original observation.
pub(crate) fn verify_scalar_signature_admitted(
    left: &BoundFunction,
    right: &BoundFunction,
    source: usize,
    max_work: impl Into<Option<usize>>,
    admit: &mut dyn FnMut(usize) -> Result<(), BindingCodecError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedSignature, BindingCodecError> {
    let result = verify_signature_inner(left, right, source, max_work.into(), Some(admit), work);
    if matches!(&result, Err(BindingCodecError::Control(_))) {
        return result;
    }
    work.flush()?;
    result
}
fn verify_signature_inner(
    left: &BoundFunction,
    right: &BoundFunction,
    source: usize,
    max_work: Option<usize>,
    mut admit: Option<&mut dyn FnMut(usize) -> Result<(), BindingCodecError>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedSignature, BindingCodecError> {
    let observed = admit.is_some();
    let policy = Policy(observed);
    let add = |a, b| policy.add(a, b, "binding projection arithmetic overflow");
    let mul = |a, b| policy.mul(a, b, "binding projection arithmetic overflow");
    let gate =
        |bound: usize,
         parent: &mut Option<&mut dyn FnMut(usize) -> Result<(), BindingCodecError>>| {
            if let Some(parent) = parent.as_mut() {
                if max_work.is_some_and(|max_work| bound > max_work) {
                    return Err(CompileControlError::ResourceExhausted.into());
                }
                parent(bound)?;
            }
            Ok::<_, BindingCodecError>(())
        };
    let same = std::ptr::eq(left, right);
    let mut bound = 8usize;
    if observed {
        for name in [
            left.function_id.as_str(),
            right.function_id.as_str(),
            left.overload.as_str(),
            right.overload.as_str(),
        ] {
            bound = add(bound, name.len())?;
        }
        gate(if same { 1 } else { bound }, &mut admit)?;
    }
    work.step()?;
    let headers = if same {
        size_of::<BoundFunction>()
    } else {
        mul(2, size_of::<BoundFunction>())?
    };
    if source < headers {
        return Err(invalid(
            "binding source invoice omits original signature roots",
        ));
    }
    if max_work == Some(0) {
        return Err(invalid("binding signature work exceeds its envelope"));
    }
    if same {
        return Ok(VerifiedSignature {
            matches: true,
            work: 1,
        });
    }
    if !observed {
        for name in [
            left.function_id.as_str(),
            right.function_id.as_str(),
            left.overload.as_str(),
            right.overload.as_str(),
        ] {
            bound = add(bound, name.len())?;
        }
    }
    for argument in left
        .argument_types
        .iter()
        .chain(right.argument_types.iter())
    {
        let count = match argument {
            FunctionArgumentType::Value(_) | FunctionArgumentType::CandidateGroup { .. } => 1,
            FunctionArgumentType::Lambda {
                parameter_types, ..
            } => add(parameter_types.len(), 1)?,
        };
        bound = add(bound, mul(count, 4)?)?;
        gate(bound, &mut admit)?;
        work.step()?;
    }
    if max_work.is_some_and(|max_work| bound > max_work) {
        return Err(invalid("binding signature work exceeds its envelope"));
    }
    let mut facts = VerifiedSignature {
        matches: false,
        work: bound,
    };
    for (left, right) in [
        (left.function_id.as_str(), right.function_id.as_str()),
        (left.overload.as_str(), right.overload.as_str()),
    ] {
        // Both identity owners already bounded each complete UTF-8 byte
        // comparison to 1024. No formatting or payload hashing is used.
        let equal = left == right;
        work.step()?;
        if !equal {
            return Ok(facts);
        }
    }
    let same_shape =
        left.kind == right.kind && left.argument_types.len() == right.argument_types.len();
    work.step()?;
    if !same_shape {
        return Ok(facts);
    }
    let mut compare =
        |left: &FunctionValueType, right: &FunctionValueType| -> Result<bool, BindingCodecError> {
            let remaining = max_work
                .map(|max_work| {
                    max_work
                        .checked_sub(facts.work)
                        .ok_or_else(|| invalid("binding signature work exceeds its envelope"))
                })
                .transpose()?;
            let base = facts.work;
            let compared = if let Some(parent) = admit.as_mut() {
                crate::borrowed_type_resources::verify_type_binding_admitted(
                    left,
                    right,
                    source,
                    remaining,
                    &mut |prefix| {
                        let total = add(base, prefix.work_upper_bound())?;
                        if max_work.is_some_and(|max_work| total > max_work) {
                            return Err(CompileControlError::ResourceExhausted.into());
                        }
                        parent(total)
                    },
                    work,
                )?
            } else {
                verify_type_binding(left, right, source, remaining, work)?
            };
            facts.work = add(base, compared.work_upper_bound())?;
            Ok(compared.matches())
        };
    for (left, right) in left.argument_types.iter().zip(&right.argument_types) {
        match (left, right) {
            (FunctionArgumentType::Value(left), FunctionArgumentType::Value(right))
            | (
                FunctionArgumentType::CandidateGroup { element_type: left },
                FunctionArgumentType::CandidateGroup {
                    element_type: right,
                },
            ) => {
                if !compare(left, right)? {
                    return Ok(facts);
                }
            }
            (
                FunctionArgumentType::Lambda {
                    parameter_types: left,
                    result_type: lr,
                },
                FunctionArgumentType::Lambda {
                    parameter_types: right,
                    result_type: rr,
                },
            ) => {
                if left.len() != right.len() {
                    return Ok(facts);
                }
                for (left, right) in left.iter().zip(right) {
                    if !compare(left, right)? {
                        return Ok(facts);
                    }
                }
                if !compare(lr, rr)? {
                    return Ok(facts);
                }
            }
            _ => return Ok(facts),
        }
    }
    if !compare(&left.result_type, &right.result_type)? {
        return Ok(facts);
    }
    facts.matches = true;
    Ok(facts)
}
