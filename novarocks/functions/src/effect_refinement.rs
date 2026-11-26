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

//! Exact-owner call refinement. Binding validation and semantic refinement
// Design: ADR-0238-self-contained-expression-units-and-explicit-environments.md
//! share the implementation owner; this interface never performs name lookup.
use crate::{FunctionBindingRequest, FunctionBindingSelection, FunctionId, FunctionKind};
use novarocks_type_contract::{
    CallEffects, CallProofScope, CompileCheckpoints, CompileControlError, CompilePhase,
    DecimalOverflowPolicy, EffectContractError, FunctionEffectDeclaration, PureCompileControl,
    SemanticParameterRef, SemanticParameters,
};
use std::{collections::BTreeSet, error::Error, fmt};

pub const MAX_CALL_EFFECT_ARGUMENTS: usize = 4096;

/// Actual runtime demand is separate from the immutable selected signature.
/// A merge reads one state occurrence; it never re-evaluates logical inputs.
#[derive(Clone, Copy, Debug)]
pub enum CallArgumentUses<'a> {
    SelectedChannels(&'a [Option<novarocks_type_contract::ExpressionUseId>]),
    TemporalSources {
        facts: &'a novarocks_type_contract::TemporalSourceFacts,
        channels: &'a [crate::TemporalSourceChannel<'a>],
    },
    MaterializedInput {
        context: novarocks_type_contract::RelationEffectContext,
        input_type: &'a novarocks_type_contract::FunctionValueType,
    },
    AggregateMerge {
        phase: crate::AggregateKernelPhase,
        state_context: novarocks_type_contract::CallEffectContext,
        state_input_type: &'a novarocks_type_contract::FunctionValueType,
    },
}
impl CallArgumentUses<'_> {
    pub(crate) fn same_borrow(self, other: Self) -> bool {
        match (self, other) {
            (Self::SelectedChannels(left), Self::SelectedChannels(right)) => {
                std::ptr::eq(left, right)
            }
            (
                Self::TemporalSources {
                    facts: left,
                    channels: lc,
                },
                Self::TemporalSources {
                    facts: right,
                    channels: rc,
                },
            ) => std::ptr::eq(left, right) && std::ptr::eq(lc, rc),
            (
                Self::AggregateMerge {
                    phase: left_phase,
                    state_context: left_context,
                    state_input_type: left_type,
                },
                Self::AggregateMerge {
                    phase: right_phase,
                    state_context: right_context,
                    state_input_type: right_type,
                },
            ) => {
                left_phase == right_phase
                    && left_context == right_context
                    && std::ptr::eq(left_type, right_type)
            }
            (
                Self::MaterializedInput {
                    context: left,
                    input_type: lt,
                },
                Self::MaterializedInput {
                    context: right,
                    input_type: rt,
                },
            ) => left == right && std::ptr::eq(lt, rt),
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CallEffectInput<'a> {
    pub context: novarocks_type_contract::CallEffectContext,
    pub argument_uses: CallArgumentUses<'a>,
    pub function_id: &'a FunctionId,
    pub kind: FunctionKind,
    pub selected: &'a FunctionBindingSelection,
    pub request: FunctionBindingRequest<'a>,
    pub environment: &'a [SemanticParameterRef],
    pub parameters: &'a SemanticParameters,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub proof_scope: CallProofScope,
}

impl CallEffectInput<'_> {
    pub(crate) fn same_borrow(self, input: CallEffectInput<'_>) -> bool {
        let original = self;
        !(original.context != input.context
            || !original.argument_uses.same_borrow(input.argument_uses)
            || !std::ptr::eq(original.function_id, input.function_id)
            || original.kind != input.kind
            || !std::ptr::eq(original.selected, input.selected)
            || !std::ptr::eq(original.request.arguments, input.request.arguments)
            || original.request.logical_argument_count != input.request.logical_argument_count
            || !match (
                original.request.expected_result_type,
                input.request.expected_result_type,
            ) {
                (None, None) => true,
                (Some(original), Some(actual)) => std::ptr::eq(original, actual),
                _ => false,
            }
            || !std::ptr::eq(original.environment, input.environment)
            || !std::ptr::eq(original.parameters, input.parameters)
            || original.decimal_overflow_policy != input.decimal_overflow_policy
            || original.proof_scope != input.proof_scope)
    }
}

/// Required operations of the exact installed immutable implementation owner.
/// A refiner validates the already-selected signature, never reselects it, and
/// uses only exact constants/types/policies/frozen environment. It proves the
/// complete actual dependency set and exact lexical refs (not just a subset of
/// possible keys); a Stable call may not leave a live environment dependency.
/// Its own loops
/// obey control/work limits. Invalid constant data is not executed here: later
/// specialization may retain a delayed row-error recipe.
pub trait FunctionEffectOwner: Send + Sync {
    type Error: Error;
    /// Lookup the already-selected overload's base declaration using a bounded
    /// index. Recursive signature/constant validation belongs in the observed
    /// validate_and_refine operation, not in this lookup without control.
    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error>;
    /// Exact selected demand before authoring child uses. The default borrows
    /// the original base without a resolver, preparation or callback. A typed
    /// invocation owner may prove original demand-zero from its complete N.
    fn selected_argument_control_observed(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
        _logical_argument_count: usize,
        _control: &dyn PureCompileControl,
    ) -> Result<novarocks_type_contract::ArgumentControl, Self::Error> {
        Ok(self.declaration(function, selected)?.argument_control)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>>;
    /// Internal staged sibling. A checked loan does not authorize a pure
    /// fallback or replace the actual owner's refinement facts.
    fn validate_and_refine_checked(
        &self,
        _input: CallEffectInput<'_>,
        _checked: &crate::CheckedCallEffectInput<'_>,
        _control: &dyn PureCompileControl,
    ) -> Result<CallEffects, crate::CheckedOwnerFailure<FunctionEffectOwnerError<Self::Error>>>
    {
        Err(crate::CheckedOwnerFailure::UnconvertedOwner)
    }
}

/// Implementation control failures remain typed at the neutral boundary.
#[derive(Debug)]
pub enum FunctionEffectOwnerError<E: Error> {
    Control(CompileControlError),
    Owner(E),
}
impl<E: Error> From<E> for FunctionEffectOwnerError<E> {
    fn from(error: E) -> Self {
        Self::Owner(error)
    }
}

#[derive(Debug)]
pub enum CallEffectRefinementError<E: Error> {
    Owner(E),
    Control(CompileControlError),
    Contract(EffectContractError),
    InvalidInput(&'static str),
}
impl<E: Error> fmt::Display for CallEffectRefinementError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Owner(e) => fmt::Display::fmt(e, f),
            Self::Control(e) => fmt::Display::fmt(e, f),
            Self::Contract(e) => fmt::Display::fmt(e, f),
            Self::InvalidInput(e) => f.write_str(e),
        }
    }
}
impl<E: Error> Error for CallEffectRefinementError<E> {}

/// Recompute local call effects from the exact implementation. A BE compares
/// this result with frozen call facts, rather than trusting a FE never-fails bit.
pub fn refine_call_effects<'a, O: FunctionEffectOwner + ?Sized>(
    owner: &O,
    input: CallEffectInput<'a>,
    control: &dyn PureCompileControl,
) -> Result<RefinedCallEffects<'a>, CallEffectRefinementError<O::Error>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
        .map_err(CallEffectRefinementError::Control)?;
    let result = (|| {
        if input.environment.len() > novarocks_type_contract::MAX_SEMANTIC_PARAMETERS {
            return Err(CallEffectRefinementError::Control(
                CompileControlError::ResourceExhausted,
            ));
        }
        if input.request.arguments.len() > MAX_CALL_EFFECT_ARGUMENTS
            || input.selected.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
        {
            return Err(CallEffectRefinementError::Control(
                CompileControlError::ResourceExhausted,
            ));
        }
        if !input.context.matches_kind(input.kind) {
            return Err(CallEffectRefinementError::Contract(
                EffectContractError::KindMismatch,
            ));
        }
        let selected_shape = match input.argument_uses {
            CallArgumentUses::SelectedChannels(uses) => uses.len() == input.request.arguments.len(),
            CallArgumentUses::TemporalSources { facts, channels } => {
                input.kind == FunctionKind::Scalar
                    && facts.validate().is_ok()
                    && channels.len() == facts.shape().source_count()
            }
            CallArgumentUses::MaterializedInput { context, .. } => {
                input.kind == FunctionKind::Aggregate
                    && input
                        .context
                        .relation()
                        .is_some_and(|call| context.is_materialized_input_of(call))
                    && input.request.arguments.len() == 1
                    && input.request.logical_argument_count == 1
            }
            CallArgumentUses::AggregateMerge {
                phase,
                state_context,
                ..
            } => {
                input.kind == FunctionKind::Aggregate
                    && !phase.consumes_logical_arguments()
                    && state_context.is_aggregate_state_input_of(input.context)
                    && input.selected.aggregate.is_some()
                    && matches!(
                        input.selected.result_type,
                        crate::FunctionResultType::Scalar(_)
                    )
            }
        };
        if !selected_shape
            || input.request.arguments.len() != input.selected.argument_types.len()
            || input.request.logical_argument_count > input.request.arguments.len()
            || (input.kind != FunctionKind::Aggregate
                && input.request.logical_argument_count != input.request.arguments.len())
        {
            return Err(CallEffectRefinementError::InvalidInput(
                "call effect input differs from selected argument shape",
            ));
        }
        if input.proof_scope != CallProofScope::Unconditional
            && input.proof_scope != input.context.proof_scope()
        {
            return Err(CallEffectRefinementError::Contract(
                EffectContractError::ProofScopeMismatch,
            ));
        }
        // Specialization consumes already-coerced arguments. Check their complete
        // types before any owner operation; this does not resolve FE coercions or
        // infer a legacy literal payload's type.
        validate_input_types(input, &mut work)?;
        let merge_state = match input.argument_uses {
            CallArgumentUses::SelectedChannels(_)
            | CallArgumentUses::TemporalSources { .. }
            | CallArgumentUses::MaterializedInput { .. } => None,
            CallArgumentUses::AggregateMerge {
                state_input_type, ..
            } => {
                let Some(state) = &input.selected.aggregate else {
                    return Err(CallEffectRefinementError::InvalidInput(
                        "aggregate merge has no selected state contract",
                    ));
                };
                Some(
                    crate::aggregate_call::validate_aggregate_merge_state_observed(
                        state_input_type,
                        &state.intermediate_type,
                        &mut work,
                    )
                    .map_err(input_type_failure::<O::Error>)?,
                )
            }
        };
        work.flush().map_err(CallEffectRefinementError::Control)?;
        work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(CallEffectRefinementError::Control)?;
        let declaration = owner
            .declaration(input.function_id, input.selected)
            .map_err(CallEffectRefinementError::Owner)?;
        declaration
            .validate(input.kind)
            .map_err(CallEffectRefinementError::Contract)?;
        let selected_control = owner
            .selected_argument_control_observed(
                input.function_id,
                input.selected,
                input.request.logical_argument_count,
                control,
            )
            .map_err(CallEffectRefinementError::Owner)?;
        novarocks_type_contract::validate_selected_argument_control(
            declaration.argument_control,
            selected_control,
        )
        .map_err(CallEffectRefinementError::Contract)?;
        // Body demand is an exact owner fact, never inferred from a name or the
        // function result type. This ABI has one lambda body argument; another
        // lambda-bearing protocol must add its own closed declaration.
        if let novarocks_type_contract::ArgumentControl::HigherOrder {
            body_ordinal,
            body_demand,
        } = declaration.argument_control
        {
            let Some(crate::FunctionArgumentType::Lambda { result_type, .. }) =
                input.selected.argument_types.get(body_ordinal as usize)
            else {
                return Err(CallEffectRefinementError::InvalidInput(
                    "higher-order body ordinal does not identify the selected lambda argument",
                ));
            };
            if body_demand == novarocks_type_contract::EvaluationDemand::TruthOnly
                && (*result_type.data_type() != arrow_schema::DataType::Boolean
                    || result_type.logical_type()
                        != novarocks_type_contract::ValueLogicalType::Physical)
            {
                return Err(CallEffectRefinementError::InvalidInput(
                    "higher-order TruthOnly body requires an exact Boolean result type",
                ));
            }
            for (ordinal, argument) in input.selected.argument_types.iter().enumerate() {
                if matches!(argument, crate::FunctionArgumentType::Lambda { .. })
                    != (ordinal == body_ordinal as usize)
                {
                    return Err(CallEffectRefinementError::InvalidInput(
                        "higher-order body declaration differs from selected lambda channels",
                    ));
                }
                work.step().map_err(CallEffectRefinementError::Control)?;
            }
        } else {
            for argument in &input.selected.argument_types {
                if declaration.argument_control
                    != novarocks_type_contract::ArgumentControl::TypeOnly
                    && matches!(argument, crate::FunctionArgumentType::Lambda { .. })
                {
                    return Err(CallEffectRefinementError::InvalidInput(
                        "lambda value arguments require an exact higher-order control declaration",
                    ));
                }
                work.step().map_err(CallEffectRefinementError::Control)?;
            }
        }
        match input.argument_uses {
            CallArgumentUses::SelectedChannels(uses) => {
                if matches!(
                    declaration.argument_control,
                    novarocks_type_contract::ArgumentControl::TemporalSource(_)
                ) {
                    return Err(CallEffectRefinementError::InvalidInput(
                        "temporal source fact is absent",
                    ));
                }
                for use_id in uses {
                    if use_id.is_none()
                        != matches!(
                            selected_control,
                            novarocks_type_contract::ArgumentControl::TypeOnly
                                | novarocks_type_contract::ArgumentControl::NoArguments
                        )
                    {
                        return Err(CallEffectRefinementError::InvalidInput(
                            "call argument demand differs from its exact owner control",
                        ));
                    }
                    work.step().map_err(CallEffectRefinementError::Control)?;
                }
            }
            CallArgumentUses::MaterializedInput {
                context,
                input_type: _,
            } => {
                if declaration.argument_control
                    != novarocks_type_contract::ArgumentControl::Aggregate
                    || !input
                        .context
                        .relation()
                        .is_some_and(|call| context.is_materialized_input_of(call))
                {
                    return Err(CallEffectRefinementError::InvalidInput(
                        "materialized input requires its exact aggregate call",
                    ));
                }
            }
            CallArgumentUses::TemporalSources { facts, channels } => {
                let context =
                    input
                        .context
                        .expression()
                        .ok_or(CallEffectRefinementError::InvalidInput(
                            "temporal source requires an expression context",
                        ))?;
                if declaration.argument_control
                    != novarocks_type_contract::ArgumentControl::TemporalSource(
                        facts.shape().kind(),
                    )
                {
                    return Err(CallEffectRefinementError::InvalidInput(
                        "temporal source channels require their exact owner control",
                    ));
                }
                let roles = facts.shape().roles();
                for (ordinal, channel) in channels.iter().enumerate() {
                    if roles[ordinal] != Some(channel.role)
                        || channel.context.use_id == context.use_id
                        || channel.context.demand
                            != novarocks_type_contract::EvaluationDemand::Value
                        || (ordinal == 0) != (channel.context.region == context.region)
                        || channels[..ordinal]
                            .iter()
                            .any(|other| other.context.use_id == channel.context.use_id)
                    {
                        return Err(CallEffectRefinementError::InvalidInput(
                            "invalid temporal source occurrence roles or domains",
                        ));
                    }
                    crate::kernel_input::validate_type_observed(channel.value_type, &mut work)
                        .map_err(|error| match error {
                            crate::KernelFailure::CompileControl(cause) => {
                                CallEffectRefinementError::Control(cause)
                            }
                            crate::KernelFailure::Cancelled => {
                                CallEffectRefinementError::Control(CompileControlError::Cancelled)
                            }
                            crate::KernelFailure::DeadlineExceeded => {
                                CallEffectRefinementError::Control(
                                    CompileControlError::DeadlineExceeded,
                                )
                            }
                            crate::KernelFailure::ResourceExhausted => {
                                CallEffectRefinementError::Control(
                                    CompileControlError::ResourceExhausted,
                                )
                            }
                            _ => CallEffectRefinementError::InvalidInput(
                                "invalid temporal source value type",
                            ),
                        })?;
                    work.step().map_err(CallEffectRefinementError::Control)?;
                }
            }
            CallArgumentUses::AggregateMerge { .. } => {
                if declaration.argument_control
                    != novarocks_type_contract::ArgumentControl::Aggregate
                {
                    return Err(CallEffectRefinementError::InvalidInput(
                        "aggregate merge requires its exact aggregate owner control",
                    ));
                }
                work.step().map_err(CallEffectRefinementError::Control)?;
            }
        }
        work.step().map_err(CallEffectRefinementError::Control)?;
        let mut refs = BTreeSet::new();
        // One active lexical setting per key in one call. Different call scopes may
        // still use different refs of the same key in the package's sparse table.
        let mut keys = BTreeSet::new();
        for reference in input.environment {
            if !declaration
                .environment_dependencies
                .contains(&reference.expected_key)
                || !refs.insert(*reference)
                || !keys.insert(reference.expected_key)
                || input.parameters.require(*reference).is_err()
            {
                return Err(CallEffectRefinementError::InvalidInput(
                    "call effect environment is not an exact frozen declared dependency",
                ));
            }
            work.step().map_err(CallEffectRefinementError::Control)?;
        }
        // Flush wrapper work before entering the implementation owner's scope.
        work.flush().map_err(CallEffectRefinementError::Control)?;
        let effects = owner
            .validate_and_refine(input, control)
            .map_err(|error| match error {
                FunctionEffectOwnerError::Control(error) => {
                    CallEffectRefinementError::Control(error)
                }
                FunctionEffectOwnerError::Owner(error) => CallEffectRefinementError::Owner(error),
            })?;
        work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(CallEffectRefinementError::Control)?;
        effects
            .validate_refinement_with_argument_control(
                declaration,
                input.proof_scope,
                selected_control,
            )
            .map_err(CallEffectRefinementError::Contract)?;
        for reference in &effects.environment {
            if !refs.contains(reference) {
                return Err(CallEffectRefinementError::InvalidInput(
                    "refiner introduced an unfrozen semantic dependency",
                ));
            }
            work.step().map_err(CallEffectRefinementError::Control)?;
        }
        work.flush().map_err(CallEffectRefinementError::Control)?;
        Ok(RefinedCallEffects {
            input,
            effects,
            merge_state,
        })
    })();
    // Success already flushed before constructing the borrowed receipt.
    // A failed author reads only this scope's existing refusal latch.
    if result.is_err() {
        return work.finish_result(result, CallEffectRefinementError::Control);
    }
    result
}

fn input_type_failure<E: Error>(error: crate::KernelFailure) -> CallEffectRefinementError<E> {
    match error {
        crate::KernelFailure::CompileControl(cause) => CallEffectRefinementError::Control(cause),
        crate::KernelFailure::Cancelled => {
            CallEffectRefinementError::Control(CompileControlError::Cancelled)
        }
        crate::KernelFailure::DeadlineExceeded => {
            CallEffectRefinementError::Control(CompileControlError::DeadlineExceeded)
        }
        crate::KernelFailure::ResourceExhausted => {
            CallEffectRefinementError::Control(CompileControlError::ResourceExhausted)
        }
        _ => CallEffectRefinementError::InvalidInput("call input has an invalid complete type"),
    }
}

fn validate_input_types<E: Error>(
    input: CallEffectInput<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), CallEffectRefinementError<E>> {
    use crate::{FunctionArgument, FunctionArgumentType, FunctionResultType};
    use novarocks_type_contract::FunctionValueType;

    let validate = |value: &FunctionValueType, work: &mut CompileCheckpoints<'_>| {
        crate::kernel_input::validate_type_observed(value, work).map_err(input_type_failure::<E>)
    };
    let exact = |actual: &FunctionValueType,
                 selected: &FunctionValueType,
                 work: &mut CompileCheckpoints<'_>| {
        actual
            .exactly_equals_observed::<crate::KernelFailure>(selected, || {
                work.step().map_err(crate::kernel_control::compile_failure)
            })
            .map_err(input_type_failure::<E>)
    };
    for (actual, selected) in input
        .request
        .arguments
        .iter()
        .zip(&input.selected.argument_types)
    {
        work.step().map_err(CallEffectRefinementError::Control)?;
        match (actual, selected) {
            (FunctionArgument::Value { value_type, .. }, FunctionArgumentType::Value(expected)) => {
                validate(value_type, work)?;
                validate(expected, work)?;
                if !value_type
                    .fits_physical_binding_observed(expected, || {
                        work.step().map_err(crate::kernel_control::compile_failure)
                    })
                    .map_err(input_type_failure::<E>)?
                {
                    return Err(CallEffectRefinementError::InvalidInput(
                        "call value argument differs from its already-coerced selected domain",
                    ));
                }
            }
            (
                FunctionArgument::Lambda {
                    parameter_types,
                    result_type,
                },
                FunctionArgumentType::Lambda {
                    parameter_types: expected_parameters,
                    result_type: expected_result,
                },
            ) => {
                if parameter_types.len() > MAX_CALL_EFFECT_ARGUMENTS
                    || expected_parameters.len() > MAX_CALL_EFFECT_ARGUMENTS
                {
                    return Err(CallEffectRefinementError::Control(
                        CompileControlError::ResourceExhausted,
                    ));
                }
                if parameter_types.len() != expected_parameters.len() {
                    return Err(CallEffectRefinementError::InvalidInput(
                        "call lambda parameter count differs from the selected signature",
                    ));
                }
                validate(result_type, work)?;
                validate(expected_result, work)?;
                if !exact(result_type, expected_result, work)? {
                    return Err(CallEffectRefinementError::InvalidInput(
                        "call lambda result differs from its exact selected type",
                    ));
                }
                for (actual, selected) in parameter_types.iter().zip(expected_parameters) {
                    work.step().map_err(CallEffectRefinementError::Control)?;
                    validate(actual, work)?;
                    validate(selected, work)?;
                    if !exact(actual, selected, work)? {
                        return Err(CallEffectRefinementError::InvalidInput(
                            "call lambda parameter differs from its exact selected type",
                        ));
                    }
                }
            }
            _ => {
                return Err(CallEffectRefinementError::InvalidInput(
                    "call argument kind differs from the selected signature",
                ));
            }
        }
    }
    match &input.selected.result_type {
        FunctionResultType::Scalar(result) => validate(result, work)?,
        FunctionResultType::Relation(results) => {
            if results.len() > MAX_CALL_EFFECT_ARGUMENTS {
                return Err(CallEffectRefinementError::Control(
                    CompileControlError::ResourceExhausted,
                ));
            }
            for result in results {
                work.step().map_err(CallEffectRefinementError::Control)?;
                validate(result, work)?;
            }
        }
    }
    if let Some(aggregate) = &input.selected.aggregate {
        validate(&aggregate.intermediate_type, work)?;
    }
    if let Some(expected) = input.request.expected_result_type {
        // Legacy selection treats this as an owner-specific constraint, not a
        // universal equality requirement. The exact owner checks its meaning.
        validate(expected, work)?;
    }
    if let CallArgumentUses::MaterializedInput { input_type, .. } = input.argument_uses {
        validate(input_type, work)?;
        let Some(crate::FunctionArgumentType::Value(expected)) =
            input.selected.argument_types.first()
        else {
            return Err(CallEffectRefinementError::InvalidInput(
                "materialized input is not a value channel",
            ));
        };
        if !input_type
            .same_physical_binding_ignoring_root_nullability_observed(expected, || work.step())
            .map_err(CallEffectRefinementError::Control)?
            || (input_type.nullable() && !expected.nullable())
        {
            return Err(CallEffectRefinementError::InvalidInput(
                "materialized input differs from its complete logical channel",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "effect_refinement/higher_order_tests.rs"]
mod higher_order_tests;

#[cfg(test)]
#[path = "effect_refinement/input_types_tests.rs"]
mod input_types_tests;

/// Only exact-owner recomputation constructs this token. In particular, Stable
/// eligibility requires the owner to prove a complete frozen environment; the
/// structural possible-key subset check alone cannot construct the token.
#[derive(Clone, Debug)]
pub struct RefinedCallEffects<'a> {
    input: CallEffectInput<'a>,
    effects: CallEffects,
    merge_state: Option<crate::aggregate_call::ValidatedAggregateMergeState<'a>>,
}
impl RefinedCallEffects<'_> {
    pub(crate) fn aggregate_merge_state(
        &self,
    ) -> Option<crate::aggregate_call::ValidatedAggregateMergeState<'_>> {
        self.merge_state
    }
    pub const fn facts(&self) -> &CallEffects {
        &self.effects
    }
    /// Compare the FE's frozen declaration against this exact local receipt.
    /// Environment references are observed individually; no opaque whole-tree
    /// equality or second owner refinement is needed to hand off this receipt.
    pub fn validate_frozen(
        &self,
        input: CallEffectInput<'_>,
        frozen: &CallEffects,
        control: &dyn PureCompileControl,
    ) -> Result<(), CallEffectRefinementError<std::convert::Infallible>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(CallEffectRefinementError::Control)?;
        let result = (|| {
            self.validate_input(input)
                .map_err(CallEffectRefinementError::Contract)?;
            let actual = self.facts();
            let mismatch = || {
                CallEffectRefinementError::InvalidInput(
                    "frozen call effects differ from exact local refinement",
                )
            };
            if actual.value_stability != frozen.value_stability
                || actual.own_row_error != frozen.own_row_error
                || actual.failure_behavior != frozen.failure_behavior
                || actual.null_behavior != frozen.null_behavior
                || actual.argument_control != frozen.argument_control
                || actual.instance_state != frozen.instance_state
                || actual.observable_effects != frozen.observable_effects
                || actual.proof_scope != frozen.proof_scope
                || actual.environment.len() != frozen.environment.len()
            {
                return Err(mismatch());
            }
            for (actual, frozen) in actual.environment.iter().zip(&frozen.environment) {
                work.step().map_err(CallEffectRefinementError::Control)?;
                if actual != frozen {
                    return Err(mismatch());
                }
            }
            Ok(())
        })();
        work.finish_result(result, CallEffectRefinementError::Control)
    }
    pub fn compose_for_use(
        &self,
        input: CallEffectInput<'_>,
        arguments: ScopedExpressionEffects,
    ) -> Result<ScopedExpressionEffects, EffectContractError> {
        if input.context != arguments.context().into() {
            return Err(EffectContractError::CallIdentityMismatch);
        }
        self.validate_input(input)?;
        arguments.with_verified_call(&self.effects)
    }
    /// Validate the exact immutable input borrow before owning specialization
    /// facts. This does not compose or erase any child expression effects.
    pub fn validate_input(&self, input: CallEffectInput<'_>) -> Result<(), EffectContractError> {
        let original = self.input;
        // The receipt borrows one immutable compilation input. Requiring those
        // exact borrows prevents attaching a proof to a different call without
        // repeatedly comparing/copying recursive schemas and constant backing.
        // These local addresses are never a wire, plan or profile identity.
        if !original.same_borrow(input) {
            return Err(EffectContractError::CallIdentityMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopedExpressionEffects {
    context: novarocks_type_contract::ExpressionEffectContext,
    effects: novarocks_type_contract::ExpressionEffects,
}
impl ScopedExpressionEffects {
    pub(crate) fn into_call_effects(self) -> crate::ScopedCallEffects {
        crate::ScopedCallEffects::new(self.context.into(), self.effects)
    }
    pub const fn pure_value(context: novarocks_type_contract::ExpressionEffectContext) -> Self {
        Self {
            context,
            effects: novarocks_type_contract::ExpressionEffects::PURE_VALUE,
        }
    }
    /// The compiler owns non-function primitive effects (cast/comparison/etc.).
    pub const fn primitive(
        context: novarocks_type_contract::ExpressionEffectContext,
        effects: novarocks_type_contract::ExpressionEffects,
    ) -> Self {
        Self { context, effects }
    }
    pub const fn context(self) -> novarocks_type_contract::ExpressionEffectContext {
        self.context
    }
    pub fn for_use(
        self,
        context: novarocks_type_contract::ExpressionEffectContext,
    ) -> Result<novarocks_type_contract::ExpressionEffects, EffectContractError> {
        if self.context != context {
            return Err(EffectContractError::ProofScopeMismatch);
        }
        Ok(self.effects)
    }
    fn with_verified_call(self, call: &CallEffects) -> Result<Self, EffectContractError> {
        if !matches!(call.proof_scope, CallProofScope::Unconditional)
            && call.proof_scope != CallProofScope::Region(self.context.region)
        {
            return Err(EffectContractError::ProofScopeMismatch);
        }
        Ok(Self {
            effects: self
                .effects
                .join(novarocks_type_contract::ExpressionEffects {
                    may_raise_invocation_data: false,
                    value_stability: call.value_stability,
                    may_raise_row_error: call.own_row_error
                        == novarocks_type_contract::FunctionIntrinsicRowError::MayRaise,
                    has_instance_state: call.instance_state
                        != novarocks_type_contract::FunctionInstanceState::None,
                    observable_effects: call.observable_effects,
                }),
            ..self
        })
    }
    /// The complete typed ScalarInvocationV1 specialization is the ONE source
    /// of this own capability. It never changes row-error or NULL behavior.
    pub(crate) const fn with_invocation_data_capability(mut self) -> Self {
        self.effects.may_raise_invocation_data = true;
        self
    }
    /// Compose the conservative summary of one actual ordered control edge.
    /// The immutable flow has already checked its guards and child demand using
    /// the common control owner. Context equality alone does not establish an
    /// edge: both exact occurrences must also occupy this owner/ordinal pair.
    /// This summary retains the parent context, never changes the child's
    /// invocation domain, and grants no permission to hoist or cache that child.
    pub fn join_control_argument<D: Copy>(
        self,
        child: Self,
        flow: &novarocks_type_contract::ExpressionControlFlow<D>,
        ordinal: usize,
    ) -> Result<Self, EffectContractError> {
        let owner = flow
            .uses()
            .get(&self.context.use_id)
            .ok_or(EffectContractError::ProofScopeMismatch)?;
        if owner.context != self.context {
            return Err(EffectContractError::ProofScopeMismatch);
        }
        let child_use = owner
            .arguments
            .get(ordinal)
            .ok_or(EffectContractError::ProofScopeMismatch)?;
        let actual = flow
            .uses()
            .get(child_use)
            .ok_or(EffectContractError::ProofScopeMismatch)?;
        if actual.context != child.context {
            return Err(EffectContractError::ProofScopeMismatch);
        }
        Ok(Self {
            effects: self.effects.join(child.effects),
            ..self
        })
    }
    /// Eager children must be in this same evaluation domain. Strong guarded
    /// children require the compiler's separate checked control-domain join.
    pub fn join_same_region(self, child: Self) -> Result<Self, EffectContractError> {
        if self.context.region != child.context.region {
            return Err(EffectContractError::ProofScopeMismatch);
        }
        Ok(Self {
            effects: self.effects.join(child.effects),
            ..self
        })
    }
}

/// Refine once, check the frozen facts, and preserve the borrowed receipt for
/// child-effect composition and exact contract construction. It is not stored
/// in LocalProgram or retained by a prepared implementation.
pub fn validate_frozen_call_effects<'a, O: FunctionEffectOwner + ?Sized>(
    owner: &O,
    input: CallEffectInput<'a>,
    frozen: &CallEffects,
    control: &dyn PureCompileControl,
) -> Result<RefinedCallEffects<'a>, CallEffectRefinementError<O::Error>> {
    let receipt = refine_call_effects(owner, input, control)?;
    receipt
        .validate_frozen(input, frozen, control)
        .map_err(|error| match error {
            CallEffectRefinementError::Owner(never) => match never {},
            CallEffectRefinementError::Contract(error) => {
                CallEffectRefinementError::Contract(error)
            }
            CallEffectRefinementError::Control(error) => CallEffectRefinementError::Control(error),
            CallEffectRefinementError::InvalidInput(error) => {
                CallEffectRefinementError::InvalidInput(error)
            }
        })?;
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FunctionArgument, FunctionArgumentType, FunctionBindingError, FunctionOverloadId,
        FunctionResultType, FunctionValueType,
    };
    use novarocks_type_contract::{
        ArgumentControl, FunctionFailureBehavior, FunctionInstanceState, FunctionIntrinsicRowError,
        FunctionNullBehavior, FunctionVolatility, ObservableEffects, SemanticParameterId,
        SemanticParameterKey, SemanticParameterValue,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Owner {
        declaration: FunctionEffectDeclaration,
        calls: AtomicUsize,
        control_failure: Option<CompileControlError>,
    }
    impl Owner {
        fn new() -> Self {
            Self {
                declaration: FunctionEffectDeclaration {
                    value_stability: FunctionVolatility::Stable,
                    own_row_error: FunctionIntrinsicRowError::MayRaise,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    null_behavior: FunctionNullBehavior::CalledOnNull,
                    argument_control: ArgumentControl::Eager,
                    instance_state: FunctionInstanceState::None,
                    observable_effects: ObservableEffects::NONE,
                    environment_dependencies: vec![SemanticParameterKey::AllowThrowException]
                        .into_boxed_slice(),
                },
                calls: AtomicUsize::new(0),
                control_failure: None,
            }
        }
    }
    impl FunctionEffectOwner for Owner {
        type Error = FunctionBindingError;
        fn declaration(
            &self,
            function: &FunctionId,
            selected: &FunctionBindingSelection,
        ) -> Result<&FunctionEffectDeclaration, Self::Error> {
            if function.as_str() != "fixture/effect"
                || selected.overload.as_str() != "fixture/effect/i64"
            {
                return Err(FunctionBindingError::UnknownFunction);
            }
            Ok(&self.declaration)
        }
        fn validate_and_refine(
            &self,
            input: CallEffectInput<'_>,
            _: &dyn PureCompileControl,
        ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
            if let Some(error) = self.control_failure.clone() {
                return Err(FunctionEffectOwnerError::Control(error));
            }
            self.calls.fetch_add(1, Ordering::Relaxed);
            let ty = FunctionValueType::from_legacy_declared_projection(
                arrow_schema::DataType::Int64,
                true,
            );
            if input.selected.argument_types.as_ref() != [FunctionArgumentType::Value(ty.clone())]
                || input.selected.result_type != FunctionResultType::Scalar(ty)
                || input.environment.len() != 1
            {
                return Err(FunctionBindingError::InvalidBinding(
                    "fixture exact call differs".into(),
                )
                .into());
            }
            let reference = input.environment[0];
            let Some(SemanticParameterValue::AllowThrowException(strict)) =
                input.parameters.get(reference.id)
            else {
                return Err(FunctionBindingError::InvalidBinding(
                    "fixture requires exact strict policy".into(),
                )
                .into());
            };
            Ok(CallEffects {
                value_stability: self.declaration.value_stability,
                own_row_error: if *strict {
                    FunctionIntrinsicRowError::MayRaise
                } else {
                    FunctionIntrinsicRowError::NoRowError
                },
                failure_behavior: self.declaration.failure_behavior,
                null_behavior: self.declaration.null_behavior,
                argument_control: self.declaration.argument_control,
                instance_state: self.declaration.instance_state,
                observable_effects: self.declaration.observable_effects,
                environment: input.environment.into(),
                proof_scope: input.proof_scope,
            })
        }
    }
    struct Control(bool);
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            if self.0 {
                Err(CompileControlError::Cancelled)
            } else {
                Ok(())
            }
        }
    }
    fn selected() -> FunctionBindingSelection {
        let ty =
            FunctionValueType::from_legacy_declared_projection(arrow_schema::DataType::Int64, true);
        FunctionBindingSelection {
            overload: FunctionOverloadId::try_new("fixture/effect/i64").unwrap(),
            argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                vec![FunctionArgumentType::Value(ty.clone())].into_boxed_slice(),
            ),
            result_type: FunctionResultType::Scalar(ty),
            aggregate: None,
        }
    }
    #[test]
    fn exact_refinement_recomputes_policy_and_refuses_forged_never_fails() {
        let owner = Owner::new();
        let id = FunctionId::try_new("fixture/effect").unwrap();
        let selected = selected();
        let args = [FunctionArgument::Value {
            value_type: FunctionValueType::from_legacy_declared_projection(
                arrow_schema::DataType::Int64,
                true,
            ),
            constant: None,
        }];
        let environment = [SemanticParameterRef {
            id: SemanticParameterId::new(u32::MAX),
            expected_key: SemanticParameterKey::AllowThrowException,
        }];
        let parameters = SemanticParameters::try_new([(
            environment[0].id,
            SemanticParameterValue::AllowThrowException(true),
        )])
        .unwrap();
        let argument_uses = [Some(novarocks_type_contract::ExpressionUseId::new(1))];
        let context = novarocks_type_contract::ExpressionEffectContext {
            use_id: novarocks_type_contract::ExpressionUseId::new(0),
            region: novarocks_type_contract::EvaluationRegionId::new(0),
            demand: novarocks_type_contract::EvaluationDemand::Value,
        };
        let input = CallEffectInput {
            context: context.into(),
            argument_uses: CallArgumentUses::SelectedChannels(&argument_uses),
            function_id: &id,
            kind: FunctionKind::Scalar,
            selected: &selected,
            request: FunctionBindingRequest {
                expected_result_type: None,
                arguments: &args,
                logical_argument_count: 1,
            },
            environment: &environment,
            parameters: &parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        };
        let result = refine_call_effects(&owner, input, &Control(false)).unwrap();
        let arguments = ScopedExpressionEffects::pure_value(context);
        let composed = result.compose_for_use(input, arguments).unwrap();
        assert!(composed.for_use(context).unwrap().may_raise_row_error);
        let other_context = novarocks_type_contract::ExpressionEffectContext {
            use_id: novarocks_type_contract::ExpressionUseId::new(8),
            ..context
        };
        assert_eq!(
            result.compose_for_use(
                CallEffectInput {
                    context: (other_context).into(),
                    ..input
                },
                ScopedExpressionEffects::pure_value(other_context)
            ),
            Err(EffectContractError::CallIdentityMismatch)
        );
        let other_arguments = [Some(novarocks_type_contract::ExpressionUseId::new(8))];
        assert_eq!(
            result.compose_for_use(
                CallEffectInput {
                    argument_uses: CallArgumentUses::SelectedChannels(&other_arguments),
                    ..input
                },
                arguments
            ),
            Err(EffectContractError::CallIdentityMismatch)
        );
        let other_function = FunctionId::try_new("fixture/other").unwrap();
        assert_eq!(
            result.compose_for_use(
                CallEffectInput {
                    function_id: &other_function,
                    ..input
                },
                arguments
            ),
            Err(EffectContractError::CallIdentityMismatch)
        );
        let other_args = [FunctionArgument::Value {
            value_type: FunctionValueType::from_legacy_declared_projection(
                arrow_schema::DataType::Int64,
                true,
            ),
            constant: Some(
                crate::ConstantValue::from_i64(
                    std::sync::Arc::new(arrow_schema::Field::new(
                        "fixture",
                        arrow_schema::DataType::Int64,
                        true,
                    )),
                    FunctionValueType::from_legacy_declared_projection(
                        arrow_schema::DataType::Int64,
                        true,
                    ),
                    1,
                    novarocks_constant_contract::ConstantValidation::bounded(
                        novarocks_constant_contract::ConstantPolicy {
                            max_rows: 1,
                            max_array_nodes: 1,
                            max_logical_elements: 1,
                            max_retained_buffer_bytes: 4096,
                            max_type_depth: 1,
                            max_type_nodes: 1,
                            max_dictionary_depth: 0,
                            max_metadata_bytes: 4096,
                            max_library_validation_work: 65536,
                            max_library_validation_bytes: 65536,
                        },
                    ),
                    CompilePhase::FunctionSpecialization,
                    &Control(false),
                )
                .unwrap(),
            ),
        }];
        assert_eq!(
            result.compose_for_use(
                CallEffectInput {
                    request: FunctionBindingRequest {
                        expected_result_type: None,
                        arguments: &other_args,
                        logical_argument_count: 1
                    },
                    ..input
                },
                arguments
            ),
            Err(EffectContractError::CallIdentityMismatch)
        );
        assert!(
            composed
                .for_use(novarocks_type_contract::ExpressionEffectContext {
                    demand: novarocks_type_contract::EvaluationDemand::TruthOnly,
                    ..context
                })
                .is_err()
        );
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let mut failing = Owner::new();
            failing.control_failure = Some(error.clone());
            assert!(
                matches!(refine_call_effects(&failing, input, &Control(false)), Err(CallEffectRefinementError::Control(actual)) if actual == error)
            );
        }

        assert_eq!(
            result.facts().own_row_error,
            FunctionIntrinsicRowError::MayRaise
        );
        validate_frozen_call_effects(&owner, input, result.facts(), &Control(false)).unwrap();
        let forged = CallEffects {
            own_row_error: FunctionIntrinsicRowError::NoRowError,
            ..result.facts().clone()
        };
        assert!(validate_frozen_call_effects(&owner, input, &forged, &Control(false)).is_err());
        // Exercise the receipt itself: no second owner refinement can mask a
        // swapped immutable input or a forged frozen header.
        #[derive(Default)]
        struct FrozenControl {
            trace: std::sync::Mutex<Vec<u32>>,
            refusal: Option<(usize, CompileControlError)>,
        }
        impl PureCompileControl for FrozenControl {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                assert_eq!(phase, CompilePhase::FunctionSpecialization);
                let mut trace = self.trace.lock().unwrap();
                let ordinal = trace.len();
                if let Some((stop, _)) = self.refusal {
                    assert!(ordinal <= stop, "callback after original refusal");
                }
                trace.push(units);
                match self.refusal.clone() {
                    Some((stop, cause)) if ordinal == stop => Err(cause),
                    _ => Ok(()),
                }
            }
        }
        let foreign_input = CallEffectInput {
            argument_uses: CallArgumentUses::SelectedChannels(&other_arguments),
            ..input
        };
        for (case, (candidate, frozen, succeeds)) in [
            (input, result.facts(), true),
            (input, &forged, false),
            (foreign_input, result.facts(), false),
        ]
        .into_iter()
        .enumerate()
        {
            let control = FrozenControl::default();
            let validation = result.validate_frozen(candidate, frozen, &control);
            assert_eq!(validation.is_ok(), succeeds);
            match case {
                0 => validation.unwrap(),
                1 => assert!(matches!(
                    validation,
                    Err(CallEffectRefinementError::InvalidInput(_))
                )),
                2 => assert!(matches!(
                    validation,
                    Err(CallEffectRefinementError::Contract(
                        EffectContractError::CallIdentityMismatch
                    ))
                )),
                _ => unreachable!(),
            }
            let baseline = control.trace.into_inner().unwrap();
            // Forged frozen metadata and a swapped immutable input already
            // refused before a completion tail. Success retains its real unit.
            assert_eq!(baseline, if succeeds { vec![0, 1] } else { vec![0] });
            for stop in 0..baseline.len() {
                for cause in [
                    CompileControlError::Cancelled,
                    CompileControlError::DeadlineExceeded,
                    CompileControlError::ResourceExhausted,
                    CompileControlError::from_domain(
                        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
                    ),
                ] {
                    let control = FrozenControl {
                        refusal: Some((stop, cause.clone())),
                        ..FrozenControl::default()
                    };
                    assert!(matches!(
                        result.validate_frozen(candidate, frozen, &control),
                        Err(CallEffectRefinementError::Control(actual)) if actual == cause
                    ));
                    assert_eq!(control.trace.into_inner().unwrap(), baseline[..=stop]);
                }
            }
            if !succeeds {
                let late = FrozenControl {
                    refusal: Some((baseline.len(), CompileControlError::Cancelled)),
                    ..FrozenControl::default()
                };
                let actual = result.validate_frozen(candidate, frozen, &late);
                match case {
                    1 => assert!(matches!(
                        actual,
                        Err(CallEffectRefinementError::InvalidInput(_))
                    )),
                    2 => assert!(matches!(
                        actual,
                        Err(CallEffectRefinementError::Contract(
                            EffectContractError::CallIdentityMismatch
                        ))
                    )),
                    _ => unreachable!(),
                }
                assert_eq!(
                    late.trace.into_inner().unwrap(),
                    baseline,
                    "late cancellation cannot replace an authored validation error"
                );
            }
        }
        let relaxed = SemanticParameters::try_new([(
            environment[0].id,
            SemanticParameterValue::AllowThrowException(false),
        )])
        .unwrap();
        let relaxed_input = CallEffectInput {
            parameters: &relaxed,
            ..input
        };
        let no_own_error = refine_call_effects(&owner, relaxed_input, &Control(false)).unwrap();
        let child_context = novarocks_type_contract::ExpressionEffectContext {
            use_id: argument_uses[0].unwrap(),
            ..context
        };
        let child = ScopedExpressionEffects::primitive(
            child_context,
            novarocks_type_contract::ExpressionEffects {
                may_raise_row_error: true,
                ..novarocks_type_contract::ExpressionEffects::PURE_VALUE
            },
        );
        let arguments = ScopedExpressionEffects::pure_value(context)
            .join_same_region(child)
            .unwrap();
        assert!(
            no_own_error
                .compose_for_use(relaxed_input, arguments)
                .unwrap()
                .for_use(context)
                .unwrap()
                .may_raise_row_error
        );
        assert!(
            arguments
                .join_same_region(ScopedExpressionEffects::pure_value(
                    novarocks_type_contract::ExpressionEffectContext {
                        region: novarocks_type_contract::EvaluationRegionId::new(1),
                        ..child_context
                    }
                ))
                .is_err()
        );

        assert_eq!(
            refine_call_effects(
                &owner,
                CallEffectInput {
                    parameters: &relaxed,
                    ..input
                },
                &Control(false)
            )
            .unwrap()
            .facts()
            .own_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
        let count = owner.calls.load(Ordering::Relaxed);
        assert!(matches!(
            refine_call_effects(&owner, input, &Control(true)),
            Err(CallEffectRefinementError::Control(
                CompileControlError::Cancelled
            ))
        ));
        assert_eq!(owner.calls.load(Ordering::Relaxed), count);
        let absent = SemanticParameters::default();
        assert!(
            refine_call_effects(
                &owner,
                CallEffectInput {
                    parameters: &absent,
                    ..input
                },
                &Control(false)
            )
            .is_err()
        );
        assert_eq!(owner.calls.load(Ordering::Relaxed), count);
        let wrong_overload = FunctionBindingSelection {
            overload: FunctionOverloadId::try_new("fixture/other").unwrap(),
            ..selected.clone()
        };
        assert!(
            refine_call_effects(
                &owner,
                CallEffectInput {
                    selected: &wrong_overload,
                    ..input
                },
                &Control(false)
            )
            .is_err()
        );
    }
    #[test]
    fn same_key_in_two_lexical_refs_is_not_one_call_environment() {
        let owner = Owner::new();
        let id = FunctionId::try_new("fixture/effect").unwrap();
        let selected = selected();
        let args = [FunctionArgument::Value {
            value_type: FunctionValueType::from_legacy_declared_projection(
                arrow_schema::DataType::Int64,
                true,
            ),
            constant: None,
        }];
        let environment = [
            SemanticParameterRef {
                id: SemanticParameterId::new(1),
                expected_key: SemanticParameterKey::AllowThrowException,
            },
            SemanticParameterRef {
                id: SemanticParameterId::new(2),
                expected_key: SemanticParameterKey::AllowThrowException,
            },
        ];
        let parameters = SemanticParameters::try_new([
            (
                environment[0].id,
                SemanticParameterValue::AllowThrowException(true),
            ),
            (
                environment[1].id,
                SemanticParameterValue::AllowThrowException(false),
            ),
        ])
        .unwrap();
        let argument_uses = [Some(novarocks_type_contract::ExpressionUseId::new(1))];
        let context = novarocks_type_contract::ExpressionEffectContext {
            use_id: novarocks_type_contract::ExpressionUseId::new(0),
            region: novarocks_type_contract::EvaluationRegionId::new(0),
            demand: novarocks_type_contract::EvaluationDemand::Value,
        };
        let input = CallEffectInput {
            context: context.into(),
            argument_uses: CallArgumentUses::SelectedChannels(&argument_uses),
            function_id: &id,
            kind: FunctionKind::Scalar,
            selected: &selected,
            request: FunctionBindingRequest {
                expected_result_type: None,
                arguments: &args,
                logical_argument_count: 1,
            },
            environment: &environment,
            parameters: &parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        };
        assert!(refine_call_effects(&owner, input, &Control(false)).is_err());
        assert_eq!(owner.calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn real_refinement_declaration_and_frozen_mismatch_precede_late_controls() {
        struct Tail {
            calls: std::sync::Mutex<Vec<u32>>,
            stop: Option<usize>,
            source: CompileControlError,
        }
        impl PureCompileControl for Tail {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                let mut calls = self.calls.lock().unwrap();
                let at = calls.len();
                calls.push(units);
                if self.stop == Some(at) {
                    Err(self.source.clone())
                } else {
                    Ok(())
                }
            }
        }
        let owner = Owner::new();
        let id = FunctionId::try_new("fixture/effect").unwrap();
        let foreign = FunctionId::try_new("fixture/foreign").unwrap();
        let selected = selected();
        let args = [FunctionArgument::Value {
            value_type: FunctionValueType::new(arrow_schema::DataType::Int64, true),
            constant: None,
        }];
        let environment = [SemanticParameterRef {
            id: SemanticParameterId::new(1),
            expected_key: SemanticParameterKey::AllowThrowException,
        }];
        let parameters = SemanticParameters::try_new([(
            environment[0].id,
            SemanticParameterValue::AllowThrowException(true),
        )])
        .unwrap();
        let uses = [Some(novarocks_type_contract::ExpressionUseId::new(1))];
        let context = novarocks_type_contract::ExpressionEffectContext {
            use_id: novarocks_type_contract::ExpressionUseId::new(0),
            region: novarocks_type_contract::EvaluationRegionId::new(0),
            demand: novarocks_type_contract::EvaluationDemand::Value,
        };
        let input = CallEffectInput {
            context: context.into(),
            argument_uses: CallArgumentUses::SelectedChannels(&uses),
            function_id: &id,
            kind: FunctionKind::Scalar,
            selected: &selected,
            request: FunctionBindingRequest {
                arguments: &args,
                logical_argument_count: 1,
                expected_result_type: None,
            },
            environment: &environment,
            parameters: &parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        };
        let source = CompileControlError::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        );
        let baseline = Tail {
            calls: Default::default(),
            stop: None,
            source: source.clone(),
        };
        assert!(matches!(
            refine_call_effects(
                &owner,
                CallEffectInput {
                    function_id: &foreign,
                    ..input
                },
                &baseline
            ),
            Err(CallEffectRefinementError::Owner(
                FunctionBindingError::UnknownFunction
            ))
        ));
        let trace = baseline.calls.lock().unwrap().clone();
        assert!(!trace.is_empty());
        let late = Tail {
            calls: Default::default(),
            stop: Some(trace.len()),
            source: source.clone(),
        };
        assert!(matches!(
            refine_call_effects(
                &owner,
                CallEffectInput {
                    function_id: &foreign,
                    ..input
                },
                &late
            ),
            Err(CallEffectRefinementError::Owner(
                FunctionBindingError::UnknownFunction
            ))
        ));
        assert_eq!(*late.calls.lock().unwrap(), trace);
        assert_eq!(owner.calls.load(Ordering::Relaxed), 0);
        for at in 0..trace.len() {
            let earlier = Tail {
                calls: Default::default(),
                stop: Some(at),
                source: source.clone(),
            };
            assert!(
                matches!(refine_call_effects(&owner,CallEffectInput{function_id:&foreign,..input},&earlier),Err(CallEffectRefinementError::Control(actual)) if actual==source)
            );
            assert_eq!(*earlier.calls.lock().unwrap(), trace[..=at]);
        }
        let receipt = refine_call_effects(&owner, input, &Control(false)).unwrap();
        let mut forged = receipt.facts().clone();
        forged.value_stability = FunctionVolatility::Volatile;
        let late = Tail {
            calls: Default::default(),
            stop: Some(1),
            source: source.clone(),
        };
        assert!(matches!(
            receipt.validate_frozen(input, &forged, &late),
            Err(CallEffectRefinementError::InvalidInput(_))
        ));
        assert_eq!(*late.calls.lock().unwrap(), [0]);
        let baseline = Tail {
            calls: Default::default(),
            stop: None,
            source: source.clone(),
        };
        receipt
            .validate_frozen(input, receipt.facts(), &baseline)
            .unwrap();
        let trace = baseline.calls.lock().unwrap().clone();
        let at = trace.len() - 1;
        let success_tail = Tail {
            calls: Default::default(),
            stop: Some(at),
            source: source.clone(),
        };
        assert!(
            matches!(receipt.validate_frozen(input,receipt.facts(),&success_tail),Err(CallEffectRefinementError::Control(actual)) if actual==source)
        );
        assert_eq!(*success_tail.calls.lock().unwrap(), trace);
    }
}

#[cfg(test)]
mod control_join_tests;

#[cfg(test)]
#[path = "effect_refinement/aggregate_merge_tests.rs"]
mod aggregate_merge_tests;
