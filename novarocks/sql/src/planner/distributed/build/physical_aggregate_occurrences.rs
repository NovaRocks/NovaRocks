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

//! Fresh aggregate update facts from the actual independent argument roots.

use std::{alloc::Layout, sync::Arc};

use novarocks_functions::{
    CallEffectInput, MAX_CALL_EFFECT_ARGUMENTS, PureCallSpecialization, PureKernelAbi,
    ScopedRelationalEffects,
};
use novarocks_physical_plan::{
    AggregateCall, Fragment, FrozenPhysicalCall, PhysicalCallSite, PhysicalNode,
};
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileCheckpoints, CompileControlError,
    DecimalOverflowPolicy, ExpressionEffects, ExpressionUseId, FunctionKind, SemanticParameterRef,
    SemanticParameters,
};

use super::{
    expression_occurrences::{AuthoredPhysicalOccurrences, ExpressionOccurrenceError},
    physical_aggregate_requests::AuthoredPhysicalAggregateUpdateRequest,
    physical_relational_effects::{
        PhysicalRelationalEffectsError, relational_context_observed,
        relational_root_effects_observed,
    },
};
use crate::compiler::SqlFunctionCatalog;

#[cfg(test)]
#[path = "physical_aggregate_occurrences_tests.rs"]
mod tests;

#[derive(Debug)]
pub(crate) enum PhysicalAggregateOccurrenceError {
    Control(CompileControlError),
    Function(ExpressionOccurrenceError),
    Relational(PhysicalRelationalEffectsError),
    InvalidSource(&'static str),
    UnsupportedAbi(PureKernelAbi),
}
impl From<CompileControlError> for PhysicalAggregateOccurrenceError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ExpressionOccurrenceError> for PhysicalAggregateOccurrenceError {
    fn from(error: ExpressionOccurrenceError) -> Self {
        match error {
            ExpressionOccurrenceError::Control(cause) => Self::Control(cause),
            other => Self::Function(other),
        }
    }
}
impl From<PhysicalRelationalEffectsError> for PhysicalAggregateOccurrenceError {
    fn from(error: PhysicalRelationalEffectsError) -> Self {
        match error {
            PhysicalRelationalEffectsError::Control(cause) => Self::Control(cause),
            other => Self::Relational(other),
        }
    }
}

/// All source loans are original immutable objects. The static request has
/// checked the exact node/site/call association and update phase. Source policy,
/// environment and proof remain explicit per occurrence, never legacy defaults.
pub(crate) struct PhysicalAggregateOccurrenceInput<'a> {
    pub fragment: &'a Fragment,
    pub node: &'a PhysicalNode,
    pub source: &'a AggregateCall,
    pub request: &'a AuthoredPhysicalAggregateUpdateRequest<'a>,
    pub occurrences: &'a AuthoredPhysicalOccurrences<'a>,
    pub child_effects: Option<&'a super::contract_lowering::NodeExpressionEffects>,
    pub parameters: &'a SemanticParameters,
    pub environment: &'a [SemanticParameterRef],
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub proof_scope: CallProofScope,
}

#[derive(Debug)]
pub(crate) struct FreshPhysicalAggregateOccurrence {
    pub frozen: FrozenPhysicalCall,
    pub preparation: PureCallSpecialization,
}

/// Prepare one actual Aggregate/TopNState update occurrence through its exact
/// installed owner supporting ordinary Aggregate preparation. Single/Partial
/// admission and logical request
/// provenance come from the sealed static update token. No merge-state source
/// is reconstructed, no lifecycle fallback is supplied, and no state is run.
///
/// Logical arguments precede function ORDER BY channels. Every repeated
/// definition has its own root/use/domain. The shared SQL-private root checker
/// obtains neutral conservative effects only after exact original root/context
/// validation. Their sole ExpressionEffects::join retains child error/state/
/// observables without moving a child into the operator domain. Complete own
/// call effects come only from fresh preparation with the same selected Arc.
///
/// Complete same-snapshot topology/static/all-definition and physical phase/
/// sequence/output gates remain caller obligations. Caller admission covers
/// source backing, temporary argument-use storage, delegated root lookups,
/// metadata/options/effect clones and retained owner preparation. This is not
/// a new wallet or a formal MEM request/grant. The caller owns entry and the
/// ordinary/success footer; nested originating control has no after check.
pub(crate) fn prepare_physical_aggregate_occurrence_observed(
    input: PhysicalAggregateOccurrenceInput<'_>,
    functions: &dyn SqlFunctionCatalog,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FreshPhysicalAggregateOccurrence, PhysicalAggregateOccurrenceError> {
    let actual = input.fragment.nodes().get(&input.node.id);
    work.step()?;
    let same_source = actual.is_some_and(|node| std::ptr::eq(node, input.node))
        && std::ptr::eq(input.node, input.request.node())
        && std::ptr::eq(input.source, input.request.source())
        && std::ptr::eq(input.occurrences.fragment(), input.fragment);
    work.step()?;
    if !same_source {
        return Err(PhysicalAggregateOccurrenceError::InvalidSource(
            "aggregate occurrence and update request have different actual source loans",
        ));
    }
    if let Some(policy) = input.request.captured_decimal_overflow_policy() {
        let same_policy = policy == input.decimal_overflow_policy;
        work.step()?;
        if !same_policy {
            return Err(PhysicalAggregateOccurrenceError::InvalidSource(
                "aggregate update occurrence changes the original logical source policy",
            ));
        }
    }
    let site = input.request.site();
    let node = match site {
        PhysicalCallSite::Aggregate { node, .. } | PhysicalCallSite::TopNState { node, .. } => node,
        _ => {
            return Err(PhysicalAggregateOccurrenceError::InvalidSource(
                "aggregate update request has a different lifecycle site",
            ));
        }
    };
    let function = input.request.function();
    let same_function = node == input.node.id
        && std::ptr::eq(&input.source.binding, input.request.binding())
        && std::ptr::eq(&input.source.binding.function, function)
        && function.kind == FunctionKind::Aggregate;
    work.step()?;
    if !same_function {
        return Err(PhysicalAggregateOccurrenceError::InvalidSource(
            "aggregate occurrence has a different actual binding or node site",
        ));
    }
    let channel_count = input
        .source
        .arguments
        .len()
        .checked_add(input.source.order_by.len());
    work.step()?;
    let channel_count = channel_count
        .filter(|&count| count <= MAX_CALL_EFFECT_ARGUMENTS)
        .ok_or(CompileControlError::ResourceExhausted)?;
    let complete = channel_count == input.request.request().arguments.len()
        && input.source.arguments.len() == input.request.request().logical_argument_count;
    work.step()?;
    if !complete {
        return Err(PhysicalAggregateOccurrenceError::InvalidSource(
            "aggregate logical and ORDER BY channel coverage differs",
        ));
    }
    let context = relational_context_observed(input.occurrences, site, work)?;
    work.flush()?;
    let declaration = functions
        .pure_overload_declaration_observed(
            &function.function_id,
            FunctionKind::Aggregate,
            &input.request.selected().overload,
            work.control(),
        )
        .map_err(ExpressionOccurrenceError::function)?;
    work.flush()?;
    let abi = declaration.implementation().abi;
    // The original ABI owner permits ordinary Aggregate preparation through
    // both attachments. COUNT also has an AggregateWindowV1 attachment; this
    // branch still prepares only the original ordinary update options.
    let correct = matches!(
        abi,
        PureKernelAbi::AggregateV1 | PureKernelAbi::AggregateWindowV1
    );
    work.step()?;
    if !correct {
        return Err(PhysicalAggregateOccurrenceError::UnsupportedAbi(abi));
    }
    let correct_control = declaration.effects().argument_control == ArgumentControl::Aggregate;
    work.step()?;
    if !correct_control {
        return Err(PhysicalAggregateOccurrenceError::InvalidSource(
            "installed aggregate owner has different argument control",
        ));
    }
    Layout::array::<Option<ExpressionUseId>>(channel_count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    let mut argument_uses = Vec::new();
    argument_uses
        .try_reserve_exact(channel_count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    let mut children = ExpressionEffects::PURE_VALUE;
    let unit = input.node.expressions.as_deref();
    for root in input
        .source
        .arguments
        .iter()
        .copied()
        .chain(input.source.order_by.iter().map(|key| key.expr))
    {
        let unit = unit.ok_or(PhysicalAggregateOccurrenceError::InvalidSource(
            "aggregate parameter has no actual node unit",
        ))?;
        let (id, effects) = relational_root_effects_observed(
            unit,
            root,
            input
                .child_effects
                .ok_or(PhysicalAggregateOccurrenceError::InvalidSource(
                    "parameter has no original unit summaries",
                ))?,
            work,
        )?;
        children = children.join(effects);
        argument_uses.push(Some(id));
        work.step()?;
    }
    let call = CallEffectInput {
        context: context.into(),
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&argument_uses),
        function_id: &function.function_id,
        kind: FunctionKind::Aggregate,
        selected: input.request.selected().as_ref(),
        request: input.request.request(),
        environment: input.environment,
        parameters: input.parameters,
        decimal_overflow_policy: input.decimal_overflow_policy,
        proof_scope: input.proof_scope,
    };
    work.flush()?;
    let options = input
        .request
        .preparation(ScopedRelationalEffects::primitive(context, children));
    work.flush()?;
    let preparation = functions
        .prepare_fresh_selected(
            call,
            Arc::clone(input.request.selected()),
            options,
            work.control(),
        )
        .map_err(ExpressionOccurrenceError::function)?;
    work.flush()?;
    let frozen = FrozenPhysicalCall {
        materialized_writer_input: None,
        site,
        context,
        effects: preparation.call_contract().effects().clone(),
        decimal_overflow_policy: input.decimal_overflow_policy,
    };
    work.flush()?;
    Ok(FreshPhysicalAggregateOccurrence {
        frozen,
        preparation,
    })
}

pub(crate) struct PhysicalAggregateMergeOccurrenceInput<'a> {
    pub fragment: &'a Fragment,
    pub node: &'a PhysicalNode,
    pub source: &'a AggregateCall,
    pub request:
        &'a super::physical_aggregate_requests::AuthoredPhysicalAggregateMergeRequest<'a, 'a>,
    pub occurrences: &'a AuthoredPhysicalOccurrences<'a>,
    pub child_effects: Option<&'a super::contract_lowering::NodeExpressionEffects>,
    pub parameters: &'a SemanticParameters,
    pub environment: &'a [SemanticParameterRef],
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub proof_scope: CallProofScope,
}

/// Merge consumes the producer-certified logical request and one independent
/// actual state root. Its neutral child effects are preserved in the operator
/// context. The sole Functions owner checks state domain and nullable widening.
/// This loan is not Package admission, a lifecycle execution or a host grant.
/// Caller entry/ordinary footer and opaque clones remain caller obligations;
/// all originating control failures return immediately on the same control.
pub(crate) fn prepare_physical_aggregate_merge_occurrence_observed(
    input: PhysicalAggregateMergeOccurrenceInput<'_>,
    functions: &dyn SqlFunctionCatalog,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FreshPhysicalAggregateOccurrence, PhysicalAggregateOccurrenceError> {
    let state_root = input.request.state_inputs().root();
    let same = std::ptr::eq(input.fragment, input.request.fragment())
        && std::ptr::eq(input.node, input.request.node())
        && std::ptr::eq(input.source, input.request.source())
        && std::ptr::eq(input.occurrences.fragment(), input.fragment)
        && state_root.fragment == input.fragment.id()
        && input.node.inputs.as_ref() == [state_root.node]
        && input.request.state().kind.input_value() == Some(state_root.value);
    work.step()?;
    if !same {
        return Err(PhysicalAggregateOccurrenceError::InvalidSource(
            "aggregate merge occurrence borrows a different original journal source",
        ));
    }
    let policy = input.decimal_overflow_policy == input.request.decimal_overflow_policy();
    work.step()?;
    if !policy {
        return Err(PhysicalAggregateOccurrenceError::InvalidSource(
            "aggregate merge occurrence changes the original logical source policy",
        ));
    }
    let site = input.request.site();
    let node = match site {
        PhysicalCallSite::Aggregate { node, .. } | PhysicalCallSite::TopNState { node, .. } => node,
        _ => {
            return Err(PhysicalAggregateOccurrenceError::InvalidSource(
                "aggregate merge has a different lifecycle site",
            ));
        }
    };
    work.step()?;
    if node != input.node.id {
        return Err(PhysicalAggregateOccurrenceError::InvalidSource(
            "aggregate merge site has another actual node",
        ));
    }
    let context = relational_context_observed(input.occurrences, site, work)?;
    let function = input.request.function();
    work.flush()?;
    let declaration = functions
        .pure_overload_declaration_observed(
            &function.function_id,
            FunctionKind::Aggregate,
            &input.request.selected().overload,
            work.control(),
        )
        .map_err(ExpressionOccurrenceError::function)?;
    work.flush()?;
    let abi = declaration.implementation().abi;
    let supported = matches!(
        abi,
        PureKernelAbi::AggregateV1 | PureKernelAbi::AggregateWindowV1
    );
    work.step()?;
    if !supported {
        return Err(PhysicalAggregateOccurrenceError::UnsupportedAbi(abi));
    }
    let supported = declaration.effects().argument_control == ArgumentControl::Aggregate;
    work.step()?;
    if !supported {
        return Err(PhysicalAggregateOccurrenceError::InvalidSource(
            "installed merge owner has different aggregate argument control",
        ));
    }
    let unit = input.node.expressions.as_deref().ok_or(
        PhysicalAggregateOccurrenceError::InvalidSource(
            "aggregate state parameter has no actual node unit",
        ),
    )?;
    let (state_use, children) = relational_root_effects_observed(
        unit,
        input.request.state_root(),
        input
            .child_effects
            .ok_or(PhysicalAggregateOccurrenceError::InvalidSource(
                "state parameter has no original unit summaries",
            ))?,
        work,
    )?;
    let invocation = unit.control_flow().uses().get(&state_use);
    work.step()?;
    let state_context = invocation
        .ok_or(PhysicalAggregateOccurrenceError::InvalidSource(
            "aggregate merge state root has no original invocation",
        ))?
        .context;
    let call = CallEffectInput {
        context: context.into(),
        argument_uses: novarocks_functions::CallArgumentUses::AggregateMerge {
            phase: input.request.phase(),
            state_context: state_context.into(),
            state_input_type: &input.request.state().ty,
        },
        function_id: &function.function_id,
        kind: FunctionKind::Aggregate,
        selected: input.request.selected().as_ref(),
        request: input.request.request(),
        environment: input.environment,
        parameters: input.parameters,
        decimal_overflow_policy: input.decimal_overflow_policy,
        proof_scope: input.proof_scope,
    };
    work.flush()?;
    let options = input
        .request
        .preparation(ScopedRelationalEffects::primitive(context, children));
    work.flush()?;
    let preparation = functions
        .prepare_fresh_selected(
            call,
            Arc::clone(input.request.selected()),
            options,
            work.control(),
        )
        .map_err(ExpressionOccurrenceError::function)?;
    work.flush()?;
    let frozen = FrozenPhysicalCall {
        materialized_writer_input: None,
        site,
        context,
        effects: preparation.call_contract().effects().clone(),
        decimal_overflow_policy: input.decimal_overflow_policy,
    };
    work.flush()?;
    Ok(FreshPhysicalAggregateOccurrence {
        frozen,
        preparation,
    })
}

#[cfg(test)]
#[path = "physical_aggregate_merge_tests.rs"]
mod merge_tests;
