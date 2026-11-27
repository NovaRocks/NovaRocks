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

use super::owner_admission::{Admit, Policy};
use super::*;
use crate::{binding_index_v2::BindingIndex, physical_type_v2::DecodedTypeTable};
use novarocks_type_contract::{FunctionId, FunctionOverloadId, FunctionValueType};
use std::{alloc::Layout, mem::size_of};

/// Sealed receiving headers retain the original DTO and exact type author.
/// Only a sparse ID index is allocated; no identity, FVT, signature or legacy
/// effect is materialized. This is not an installed-owner proof or a decoded
/// Physical binding. All later semantic owners remain mandatory.
pub struct PreparedFunctionBindingHeaders<'loan> {
    definitions: &'loan [wire::FunctionBindingDefinition],
    types: &'loan DecodedTypeTable,
    indices: BindingIndex,
    facts: BindingProjectionFacts,
    source_invoice: usize,
    control: &'loan dyn PureCompileControl,
}
impl<'loan> PreparedFunctionBindingHeaders<'loan> {
    pub fn as_wire(&self) -> &'loan [wire::FunctionBindingDefinition] {
        self.definitions
    }
    pub fn type_table(&self) -> &'loan DecodedTypeTable {
        self.types
    }
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
    /// Borrow the actual definition, with the original receiving control.
    /// Repeated lookup work belongs to the caller's consuming scope; each
    /// lookup performs at most bit_length(definitions) comparisons plus the
    /// entry and tail. There is no replacement control or cloned definition.
    pub fn definition(
        &self,
        id: u32,
    ) -> Result<Option<&'loan wire::FunctionBindingDefinition>, BindingCodecError> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Decode)?;
        let result = self.definition_observed(id, &mut work)?;
        work.finish()?;
        Ok(result)
    }
    /// Borrow the same receiving namespace/control using the parent's scope.
    pub fn definition_in(
        &self,
        id: u32,
        admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'loan wire::FunctionBindingDefinition>, BindingCodecError> {
        admit(&super::owner_admission::lookup_facts(
            self.definitions.len(),
            crate::binding_index_v2::lookup_work_upper_bound(self.definitions.len()),
        )?)?;
        let same = std::ptr::addr_eq(work.control(), self.control);
        work.step()?;
        if !same {
            return Err(invalid(
                "binding header lookup has a different original control",
            ));
        }
        self.definition_observed(id, work)
    }
    pub(crate) fn definition_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'loan wire::FunctionBindingDefinition>, BindingCodecError> {
        self.definition_captured(id, &mut |_, _| Ok(()), work)
    }
    pub(crate) fn definition_captured(
        &self,
        id: u32,
        capture: &mut impl FnMut(
            &'loan wire::FunctionBindingDefinition,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), BindingCodecError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'loan wire::FunctionBindingDefinition>, BindingCodecError> {
        self.indices
            .find_captured(
                id,
                |at| self.definitions[at].id,
                &mut |at, work| capture(&self.definitions[at], work),
                work,
            )
            .map(|index| index.map(|at| &self.definitions[at]))
    }
    pub(crate) fn original_control(&self) -> &'loan dyn PureCompileControl {
        self.control
    }
    /// Carry the previous caller's source invoice exactly once, plus this
    /// token and its actual reserved index capacity. This is a declared lower
    /// floor for the next coexisting stage, not a measured total/MEM grant.
    pub(crate) fn retained_invoice_floor(&self) -> Result<usize, BindingCodecError> {
        add(
            self.source_invoice,
            add(size_of::<Self>(), self.indices.backing_bytes()?)?,
        )
    }
}
fn invalid(message: &'static str) -> BindingCodecError {
    BindingCodecError::InvalidShape(message)
}
fn add(a: usize, b: usize) -> Result<usize, BindingCodecError> {
    a.checked_add(b)
        .ok_or_else(|| invalid("binding header arithmetic overflow"))
}
fn bytes<T>(count: usize) -> Result<usize, BindingCodecError> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| invalid("binding header layout is unrepresentable"))
}
fn height(count: usize) -> usize {
    (usize::BITS - count.leading_zeros()) as usize
}
fn admit_work(
    facts: &mut BindingProjectionFacts,
    count: usize,
    arguments: usize,
    type_lookup_work: usize,
    limits: Option<BindingProjectionLimits>,
    policy: Policy,
    admit: &mut Admit<'_>,
) -> Result<(), BindingCodecError> {
    let add = |a, b| policy.add(a, b, "binding header arithmetic overflow");
    let mul = |a, b| policy.mul(a, b, "binding header arithmetic overflow");
    // All owned passes, index initialization/sort/duplicate checks, and each
    // original type-map lookup are admitted before their expansion loops.
    // Index capacity and allocator internals remain opaque host obligations.
    let definitions = mul(count, add(32, mul(add(height(count), 1)?, 16)?)?)?;
    let definitions = if policy.0 {
        definitions.max(crate::binding_index_v2::prepare_work_upper_bound(count)?)
    } else {
        definitions
    };
    let references = mul(facts.type_reference_count, add(type_lookup_work, 8)?)?;
    facts.cumulative_work_upper_bound = add(
        add(128, definitions)?,
        add(
            mul(arguments, 8)?,
            add(references, mul(facts.request_bytes_upper_bound, 4)?)?,
        )?,
    )?;
    policy.gate(facts, limits, admit)?;
    if limits.is_some_and(|limits| facts.cumulative_work_upper_bound > limits.max_work) {
        return Err(invalid("binding header work exceeds its envelope"));
    }
    if limits.is_some_and(|limits| facts.type_reference_count > limits.max_type_references) {
        return Err(invalid(
            "binding header type references exceed their envelope",
        ));
    }
    Ok(())
}
fn source_floor(source: usize, known: usize) -> Result<(), BindingCodecError> {
    if source < known {
        return Err(invalid(
            "binding header source invoice omits original backing",
        ));
    }
    Ok(())
}
fn preflight(
    definitions: &[wire::FunctionBindingDefinition],
    types: &DecodedTypeTable,
    source: usize,
    limits: Option<BindingProjectionLimits>,
    policy: Policy,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BindingProjectionFacts, BindingCodecError> {
    preflight_namespace(
        definitions,
        types.value_types().len(),
        size_of::<DecodedTypeTable>(),
        source,
        limits,
        policy,
        admit,
        work,
    )
}
fn preflight_namespace(
    definitions: &[wire::FunctionBindingDefinition],
    type_count: usize,
    type_header_bytes: usize,
    source: usize,
    limits: Option<BindingProjectionLimits>,
    policy: Policy,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BindingProjectionFacts, BindingCodecError> {
    let add = |a, b| policy.add(a, b, "binding header arithmetic overflow");
    let mul = |a, b| policy.mul(a, b, "binding header arithmetic overflow");
    if !policy.0 && limits.is_some_and(|limits| definitions.len() > limits.max_definitions) {
        return Err(invalid(
            "binding header definition count exceeds its envelope",
        ));
    }
    let request_bytes = policy.bytes::<usize>(
        definitions.len(),
        "binding header layout is unrepresentable",
    )?;
    let mut facts = BindingProjectionFacts {
        definition_count: definitions.len(),
        type_reference_count: 0,
        allocation_requests_upper_bound: usize::from(request_bytes != 0),
        request_bytes_upper_bound: request_bytes,
        coexisting_source_and_request_bytes_upper_bound: add(source, request_bytes)?,
        cumulative_work_upper_bound: 0,
    };
    if policy.0 {
        admit_work(
            &mut facts,
            definitions.len(),
            0,
            add(type_count, 1)?,
            limits,
            policy,
            admit,
        )?;
    }
    if limits.is_some_and(|limits| {
        request_bytes > limits.max_request_bytes
            || facts.allocation_requests_upper_bound > limits.max_allocation_requests
            || facts.coexisting_source_and_request_bytes_upper_bound
                > limits.max_coexisting_source_and_request_bytes
    }) {
        return Err(invalid(
            "binding header index requests exceed their envelope",
        ));
    }
    // A map lookup cannot compare more keys than the whole map holds. This
    // conservative bound does not assume Rust's private BTree branching.
    let type_lookup_work = add(type_count, 1)?;
    let mut known = add(
        bytes::<wire::FunctionBindingDefinition>(definitions.len())?,
        add(
            type_header_bytes,
            mul(
                type_count,
                add(size_of::<u32>(), size_of::<FunctionValueType>())?,
            )?,
        )?,
    )?;
    if !policy.0 {
        source_floor(source, known)?;
    }
    let mut argument_count = 0;
    admit_work(
        &mut facts,
        definitions.len(),
        0,
        type_lookup_work,
        limits,
        policy,
        admit,
    )?;
    if policy.0 {
        source_floor(source, known)?;
    }
    for definition in definitions {
        argument_count = add(argument_count, definition.arguments.len())?;
        known = add(
            known,
            add(
                add(
                    definition.function_id.capacity(),
                    definition.overload_id.capacity(),
                )?,
                bytes::<wire::FunctionArgumentType>(definition.arguments.capacity())?,
            )?,
        )?;
        if !policy.0 {
            source_floor(source, known)?;
        }
        admit_work(
            &mut facts,
            definitions.len(),
            argument_count,
            type_lookup_work,
            limits,
            policy,
            admit,
        )?;
        if policy.0 {
            source_floor(source, known)?;
        }
        work.step()?;
        for argument in &definition.arguments {
            let references = match &argument.kind {
                Some(
                    wire::function_argument_type::Kind::ValueTypeId(_)
                    | wire::function_argument_type::Kind::CandidateGroupElementValueTypeId(_),
                ) => 1,
                Some(wire::function_argument_type::Kind::Lambda(lambda)) => {
                    known = add(
                        known,
                        bytes::<u32>(lambda.parameter_value_type_ids.capacity())?,
                    )?;
                    add(lambda.parameter_value_type_ids.len(), 1)?
                }
                None => return Err(invalid("binding header argument kind is absent")),
            };
            facts.type_reference_count = add(facts.type_reference_count, references)?;
            if !policy.0 {
                source_floor(source, known)?;
            }
            admit_work(
                &mut facts,
                definitions.len(),
                argument_count,
                type_lookup_work,
                limits,
                policy,
                admit,
            )?;
            if policy.0 {
                source_floor(source, known)?;
            }
            work.step()?;
        }
        let result_count = match &definition.result {
            Some(wire::function_binding_definition::Result::ScalarValueTypeId(_)) => 1,
            Some(wire::function_binding_definition::Result::Relation(relation)) => {
                known = add(known, bytes::<u32>(relation.value_type_ids.capacity())?)?;
                relation.value_type_ids.len()
            }
            None => return Err(invalid("binding header result kind is absent")),
        };
        facts.type_reference_count = add(facts.type_reference_count, result_count)?;
        if !policy.0 {
            source_floor(source, known)?;
        }
        admit_work(
            &mut facts,
            definitions.len(),
            argument_count,
            type_lookup_work,
            limits,
            policy,
            admit,
        )?;
        if policy.0 {
            source_floor(source, known)?;
        }
        work.step()?;
    }
    Ok(facts)
}
fn validate(
    definitions: &[wire::FunctionBindingDefinition],
    types: &DecodedTypeTable,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    let mut value = |id, work: &mut CompileCheckpoints<'_>| {
        // The lookup is an admitted opaque standard-library operation; these
        // boundaries do not claim cooperation inside BTreeMap::get.
        work.flush()?;
        let present = types.value_type(id).is_some();
        work.step()?;
        work.flush()?;
        if present {
            Ok(())
        } else {
            Err(invalid("binding header value type is absent"))
        }
    };
    validate_definitions_with_lookup(definitions, &mut value, work)
}

pub(super) fn validate_definitions_with_lookup(
    definitions: &[wire::FunctionBindingDefinition],
    value: &mut impl FnMut(u32, &mut CompileCheckpoints<'_>) -> Result<(), BindingCodecError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    for definition in definitions {
        FunctionId::validate_str(&definition.function_id)
            .map_err(|_| invalid("binding header function identity is invalid"))?;
        FunctionOverloadId::validate_str(&definition.overload_id)
            .map_err(|_| invalid("binding header overload identity is invalid"))?;
        work.step()?;
        // Both sole identity checks are O(1), with the existing 1024-byte
        // owner bound. No character scan or allocated identity is introduced.
        match (
            wire::FunctionKind::try_from(definition.kind),
            &definition.result,
        ) {
            (
                Ok(
                    wire::FunctionKind::Scalar
                    | wire::FunctionKind::Aggregate
                    | wire::FunctionKind::Window,
                ),
                Some(wire::function_binding_definition::Result::ScalarValueTypeId(id)),
            ) => value(*id, work)?,
            (
                Ok(wire::FunctionKind::Table),
                Some(wire::function_binding_definition::Result::Relation(relation)),
            ) => {
                for id in &relation.value_type_ids {
                    value(*id, work)?;
                }
            }
            _ => return Err(invalid("binding header kind and result disagree")),
        }
        for argument in &definition.arguments {
            match &argument.kind {
                Some(
                    wire::function_argument_type::Kind::ValueTypeId(id)
                    | wire::function_argument_type::Kind::CandidateGroupElementValueTypeId(id),
                ) => value(*id, work)?,
                Some(wire::function_argument_type::Kind::Lambda(lambda)) => {
                    for id in &lambda.parameter_value_type_ids {
                        value(*id, work)?;
                    }
                    value(
                        lambda
                            .result_value_type_id
                            .ok_or_else(|| invalid("binding header lambda result is absent"))?,
                        work,
                    )?;
                }
                None => return Err(invalid("binding header argument kind is absent")),
            }
            work.step()?;
        }
    }
    Ok(())
}
fn prepare<'loan, 'control: 'loan>(
    definitions: &'loan [wire::FunctionBindingDefinition],
    types: &'loan DecodedTypeTable,
    source: usize,
    limits: Option<BindingProjectionLimits>,
    policy: Policy,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedFunctionBindingHeaders<'loan>, BindingCodecError> {
    let facts = preflight(definitions, types, source, limits, policy, admit, work)?;
    validate(definitions, types, work)?;
    let indices = BindingIndex::prepare(definitions.len(), |at| definitions[at].id, work)?;
    Ok(PreparedFunctionBindingHeaders {
        definitions,
        types,
        indices,
        facts,
        source_invoice: source,
        control: work.control(),
    })
}
/// Prepare the entire original receiving namespace before any index request.
/// The source invoice covers the DTO and decoded type table, including all
/// spare/backing storage. Known owned DTO capacities and value-map storage
/// establish only a lower bound; the complete invoice and formal MEM grant
/// remain caller responsibilities. Lambda arity and relation shape remain
/// with actual selected semantic/Physical owners. No default profile exists.
pub fn prepare_function_binding_headers<'loan>(
    definitions: &'loan [wire::FunctionBindingDefinition],
    types: &'loan DecodedTypeTable,
    source_retained_bytes: usize,
    limits: BindingProjectionLimits,
    control: &'loan dyn PureCompileControl,
) -> Result<PreparedFunctionBindingHeaders<'loan>, BindingCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = prepare(
        definitions,
        types,
        source_retained_bytes,
        Some(limits),
        Policy(false),
        &mut |_| Ok(()),
        &mut work,
    );
    if matches!(&result, Err(BindingCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

/// Same full header/index author, without entry/footer or replacement control.
/// Passing None derives the request and work from the received definitions and
/// the original type namespace, with admission supplied by the caller.
pub fn prepare_function_binding_headers_in<'loan, 'control: 'loan>(
    definitions: &'loan [wire::FunctionBindingDefinition],
    types: &'loan DecodedTypeTable,
    source_retained_bytes: usize,
    limits: impl Into<Option<BindingProjectionLimits>>,
    admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedFunctionBindingHeaders<'loan>, BindingCodecError> {
    prepare(
        definitions,
        types,
        source_retained_bytes,
        limits.into(),
        Policy(true),
        admit,
        work,
    )
}

#[cfg(test)]
mod tests;
