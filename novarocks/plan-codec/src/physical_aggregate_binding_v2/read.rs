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

use super::*;
use crate::physical_binding_v2::owner_admission::{Admit, Policy};
use crate::{binding_index_v2::BindingIndex, physical_binding_v2::PreparedFunctionBindingHeaders};
use novarocks_type_contract::{AggregateStateFormatId, CompileControlError, FunctionIdentityError};

/// Borrowed receiving facts, not a decoded aggregate or an effect declaration.
/// The same function/type owner and original control remain mandatory. The
/// only new heap backing is the admitted sparse index.
pub struct PreparedAggregateBindingHeaders<'loan, 'source> {
    definitions: &'loan [wire::AggregateBindingDefinition],
    functions: &'loan PreparedFunctionBindingHeaders<'source>,
    index: BindingIndex,
    facts: BindingProjectionFacts,
    source_invoice: usize,
}
impl<'loan, 'source> PreparedAggregateBindingHeaders<'loan, 'source> {
    pub fn as_wire(&self) -> &'loan [wire::AggregateBindingDefinition] {
        self.definitions
    }
    pub fn functions(&self) -> &'loan PreparedFunctionBindingHeaders<'source> {
        self.functions
    }
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
    /// Each lookup borrows the original definition and receiving control.
    /// The consuming scope admits its own repeated lookup work.
    pub fn definition(
        &self,
        id: u32,
    ) -> Result<Option<&'loan wire::AggregateBindingDefinition>, BindingCodecError> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.definition_observed(id, &mut work)?;
        work.finish()?;
        Ok(result)
    }
    /// Borrow the original definition inside the consuming scope's meter.
    /// This port neither opens nor finishes a second lookup scope.
    pub fn definition_in(
        &self,
        id: u32,
        admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'loan wire::AggregateBindingDefinition>, BindingCodecError> {
        admit(&crate::physical_binding_v2::owner_admission::lookup_facts(
            self.definitions.len(),
            crate::binding_index_v2::lookup_work_upper_bound(self.definitions.len()),
        )?)?;
        let same = std::ptr::addr_eq(work.control(), self.original_control());
        work.step()?;
        if !same {
            return Err(invalid(
                "aggregate header lookup has a different original control",
            ));
        }
        self.definition_observed(id, work)
    }
    pub(crate) fn definition_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'loan wire::AggregateBindingDefinition>, BindingCodecError> {
        self.definition_captured(id, &mut |_, _| Ok(()), work)
    }
    pub(crate) fn definition_captured(
        &self,
        id: u32,
        capture: &mut impl FnMut(
            &'loan wire::AggregateBindingDefinition,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), BindingCodecError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'loan wire::AggregateBindingDefinition>, BindingCodecError> {
        self.index
            .find_captured(
                id,
                |at| self.definitions[at].id,
                &mut |at, work| capture(&self.definitions[at], work),
                work,
            )
            .map(|index| index.map(|at| &self.definitions[at]))
    }
    pub(crate) fn original_control(&self) -> &'source dyn PureCompileControl {
        self.functions.original_control()
    }
    /// Preserve the supplied original source invoice once, plus this token
    /// and its actual retained sparse index capacity. The invoice already
    /// includes the loaned function owner; do not charge its invoice again.
    /// This is a necessary composition floor, not a measured total/MEM grant.
    pub(crate) fn retained_invoice_floor(&self) -> Result<usize, BindingCodecError> {
        add(
            self.source_invoice,
            add(size_of::<Self>(), self.index.backing_bytes()?)?,
        )
    }
}
fn invalid(message: &'static str) -> BindingCodecError {
    BindingCodecError::InvalidShape(message)
}
fn add(a: usize, b: usize) -> Result<usize, BindingCodecError> {
    a.checked_add(b)
        .ok_or_else(|| invalid("aggregate header arithmetic overflow"))
}
fn bytes<T>(count: usize) -> Result<usize, BindingCodecError> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| invalid("aggregate header layout is unrepresentable"))
}
fn height(count: usize) -> usize {
    (usize::BITS - count.leading_zeros()) as usize
}
fn source_floor(source: usize, known: usize) -> Result<(), BindingCodecError> {
    if source < known {
        return Err(invalid(
            "aggregate header source invoice omits original backing",
        ));
    }
    Ok(())
}
fn admit_work(
    facts: &mut BindingProjectionFacts,
    state_bytes: usize,
    lookup_work: usize,
    limits: Option<BindingProjectionLimits>,
    policy: Policy,
    admit: &mut Admit<'_>,
) -> Result<(), BindingCodecError> {
    let add = |a, b| policy.add(a, b, "aggregate header arithmetic overflow");
    let mul = |a, b| policy.mul(a, b, "aggregate header arithmetic overflow");
    // The common index performs at most four operations per descended heap
    // level. Its initialization, build/extraction and duplicate pass fit this
    // conservative O(N log N) term. Function lookup uses its original index;
    // type lookup is bounded by the whole value map without private layouts.
    let n = facts.definition_count;
    let own = mul(n, add(64, mul(add(height(n), 1)?, 16)?)?)?;
    let own = if policy.0 {
        own.max(crate::binding_index_v2::prepare_work_upper_bound(n)?)
    } else {
        own
    };
    facts.cumulative_work_upper_bound = add(
        128,
        add(
            own,
            add(
                mul(n, lookup_work)?,
                add(
                    mul(state_bytes, 4)?,
                    mul(facts.request_bytes_upper_bound, 4)?,
                )?,
            )?,
        )?,
    )?;
    policy.gate(facts, limits, admit)?;
    if limits.is_some_and(|limits| facts.cumulative_work_upper_bound > limits.max_work) {
        return Err(invalid("aggregate header work exceeds its envelope"));
    }
    Ok(())
}
fn preflight(
    definitions: &[wire::AggregateBindingDefinition],
    functions: &PreparedFunctionBindingHeaders<'_>,
    source: usize,
    limits: Option<BindingProjectionLimits>,
    policy: Policy,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BindingProjectionFacts, BindingCodecError> {
    let add = |a, b| policy.add(a, b, "aggregate header arithmetic overflow");
    let n = definitions.len();
    if !policy.0 && limits.is_some_and(|limits| n > limits.max_definitions) {
        return Err(invalid(
            "aggregate header definition count exceeds its envelope",
        ));
    }
    let requested = policy.bytes::<usize>(n, "aggregate header layout is unrepresentable")?;
    let mut facts = BindingProjectionFacts {
        definition_count: n,
        type_reference_count: 0,
        allocation_requests_upper_bound: usize::from(requested != 0),
        request_bytes_upper_bound: requested,
        coexisting_source_and_request_bytes_upper_bound: add(source, requested)?,
        cumulative_work_upper_bound: 0,
    };
    // This borrowed floor author contains only checked arithmetic and Layout.
    // Capture its known numerical failure before the first completed lookup.
    let function_floor = if policy.0 {
        Some(
            functions
                .retained_invoice_floor()
                .map_err(|error| match error {
                    BindingCodecError::InvalidShape(_) => {
                        CompileControlError::ResourceExhausted.into()
                    }
                    other => other,
                })?,
        )
    } else {
        None
    };
    if policy.0 {
        let lookup = add(
            height(functions.as_wire().len()),
            add(functions.type_table().value_types().len(), 1)?,
        )?;
        admit_work(&mut facts, 0, lookup, limits, policy, admit)?;
        let same = std::ptr::addr_eq(work.control(), functions.original_control());
        work.step()?;
        if !same {
            return Err(invalid(
                "aggregate headers have a different original control",
            ));
        }
    }
    if let Some(limits) = limits
        && (requested > limits.max_request_bytes
            || facts.allocation_requests_upper_bound > limits.max_allocation_requests
            || facts.coexisting_source_and_request_bytes_upper_bound
                > limits.max_coexisting_source_and_request_bytes)
    {
        return Err(invalid(
            "aggregate header index requests exceed their envelope",
        ));
    }
    // The previous original source invoice, token and actual index capacity
    // coexist once. Aggregate DTO roots and each owned state String are
    // independent backing; no source invoice is multiplied by lookup count.
    let mut known = add(
        match function_floor {
            Some(floor) => floor,
            None => functions.retained_invoice_floor()?,
        },
        bytes::<wire::AggregateBindingDefinition>(n)?,
    )?;
    source_floor(source, known)?;
    let lookup_work = add(
        height(functions.as_wire().len()),
        add(functions.type_table().value_types().len(), 1)?,
    )?;
    let mut state_bytes = 0;
    admit_work(&mut facts, state_bytes, lookup_work, limits, policy, admit)?;
    for definition in definitions {
        if definition.intermediate_value_type_id.is_some() {
            facts.type_reference_count = add(facts.type_reference_count, 1)?;
        }
        known = add(known, definition.state_format.capacity())?;
        if let Some(receipt) = &definition.state_interpretation {
            known = add(
                known,
                bytes::<wire::AggregateStateOrderKey>(receipt.order_keys.capacity())?,
            )?;
            state_bytes = add(
                state_bytes,
                bytes::<wire::AggregateStateOrderKey>(receipt.order_keys.len())?,
            )?;
        }
        state_bytes = add(state_bytes, definition.state_format.len())?;
        source_floor(source, known)?;
        admit_work(&mut facts, state_bytes, lookup_work, limits, policy, admit)?;
        work.step()?;
    }
    if limits.is_some_and(|limits| facts.type_reference_count > limits.max_type_references) {
        return Err(invalid(
            "aggregate header type references exceed their envelope",
        ));
    }
    Ok(facts)
}
enum StateValidationFailure {
    Identity,
    Control(CompileControlError),
}
impl From<FunctionIdentityError> for StateValidationFailure {
    fn from(_: FunctionIdentityError) -> Self {
        Self::Identity
    }
}
impl From<CompileControlError> for StateValidationFailure {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
fn validate(
    definitions: &[wire::AggregateBindingDefinition],
    functions: &PreparedFunctionBindingHeaders<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    for definition in definitions {
        let function_id = definition
            .function_binding_id
            .ok_or_else(|| invalid("aggregate header function reference is absent"))?;
        let function = functions
            .definition_observed(function_id, work)?
            .ok_or_else(|| invalid("aggregate header function reference is unknown"))?;
        let kind_is_aggregate = function.kind == wire::FunctionKind::Aggregate as i32;
        work.step()?;
        if !kind_is_aggregate {
            return Err(invalid("aggregate header function has non-aggregate kind"));
        }
        let phase = definition
            .phase
            .as_ref()
            .ok_or_else(|| invalid("aggregate header phase is absent"))?;
        let phase_present = match phase.kind.as_ref() {
            Some(kind) => match kind {
                wire::aggregate_phase::Kind::Single(_)
                | wire::aggregate_phase::Kind::PartialSequenceId(_)
                | wire::aggregate_phase::Kind::IntermediateSequenceId(_)
                | wire::aggregate_phase::Kind::FinalSequenceId(_) => true,
            },
            None => false,
        };
        work.step()?;
        if !phase_present {
            return Err(invalid("aggregate header phase kind is absent"));
        }
        let channels = function.arguments.len();
        let logical = definition.logical_argument_count as usize;
        match wire::AggregateStateCapability::try_from(definition.state_capability) {
            Ok(wire::AggregateStateCapability::SingleOnly)
                if logical < channels
                    && matches!(phase.kind, Some(wire::aggregate_phase::Kind::Single(_)))
                    && definition.intermediate_value_type_id.is_none()
                    && definition.state_format.is_empty()
                    && definition.state_argument_contract == 0
                    && definition.state_interpretation.is_none() =>
            {
                continue;
            }
            Ok(wire::AggregateStateCapability::Mergeable) if logical == channels => {}
            _ => {
                return Err(invalid(
                    "aggregate header has an invalid state capability or phase",
                ));
            }
        }
        if let Some(receipt) = &definition.state_interpretation {
            validate_state_interpretation(receipt, work)?;
        }
        let state_contract = decode_state_argument_contract(definition.state_argument_contract);
        work.step()?;
        state_contract?;
        let intermediate_id = definition
            .intermediate_value_type_id
            .ok_or_else(|| invalid("aggregate header intermediate type is absent"))?;
        work.flush()?;
        let present = functions.type_table().value_type(intermediate_id).is_some();
        work.step()?;
        work.flush()?;
        if !present {
            return Err(invalid("aggregate header intermediate type is unknown"));
        }
        AggregateStateFormatId::validate_str_observed::<StateValidationFailure>(
            &definition.state_format,
            || work.step().map_err(StateValidationFailure::from),
        )
        .map_err(|error| match error {
            StateValidationFailure::Control(cause) => BindingCodecError::Control(cause),
            StateValidationFailure::Identity => invalid("aggregate header state format is invalid"),
        })?;
    }
    Ok(())
}
/// Prepare borrowed receiving headers without materializing legacy effect
/// fields, signature copies or identities. The caller supplies a complete
/// coexisting source invoice and explicit limits; neither is a formal grant.
/// The existing function token supplies the only accepted original control.
fn prepare<'loan, 'source>(
    definitions: &'loan [wire::AggregateBindingDefinition],
    functions: &'loan PreparedFunctionBindingHeaders<'source>,
    source_retained_bytes: usize,
    limits: Option<BindingProjectionLimits>,
    policy: Policy,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedAggregateBindingHeaders<'loan, 'source>, BindingCodecError> {
    let facts = preflight(
        definitions,
        functions,
        source_retained_bytes,
        limits,
        policy,
        admit,
        work,
    )?;
    validate(definitions, functions, work)?;
    let index = BindingIndex::prepare(definitions.len(), |at| definitions[at].id, work)?;
    Ok(PreparedAggregateBindingHeaders {
        definitions,
        functions,
        index,
        facts,
        source_invoice: source_retained_bytes,
    })
}
pub fn prepare_aggregate_binding_headers<'loan, 'source>(
    definitions: &'loan [wire::AggregateBindingDefinition],
    functions: &'loan PreparedFunctionBindingHeaders<'source>,
    source_retained_bytes: usize,
    limits: impl Into<Option<BindingProjectionLimits>>,
) -> Result<PreparedAggregateBindingHeaders<'loan, 'source>, BindingCodecError> {
    let mut work = CompileCheckpoints::try_new(functions.original_control(), CompilePhase::Decode)?;
    let result = prepare(
        definitions,
        functions,
        source_retained_bytes,
        limits.into(),
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
/// Same original complete receiving header/index author, no scope/footer.
pub fn prepare_aggregate_binding_headers_in<'loan, 'source>(
    definitions: &'loan [wire::AggregateBindingDefinition],
    functions: &'loan PreparedFunctionBindingHeaders<'source>,
    source_retained_bytes: usize,
    limits: impl Into<Option<BindingProjectionLimits>>,
    admit: &mut impl FnMut(&BindingProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedAggregateBindingHeaders<'loan, 'source>, BindingCodecError> {
    prepare(
        definitions,
        functions,
        source_retained_bytes,
        limits.into(),
        Policy(true),
        admit,
        work,
    )
}

#[cfg(test)]
mod tests;
