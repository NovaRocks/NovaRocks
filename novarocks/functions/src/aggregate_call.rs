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

//! Exact aggregate call phase and separate selected update/merge channels.
//! State placement, group mapping and lineage remain execution/program owners.

use crate::kernel_control::{compile_failure, invalid};
use crate::kernel_input::{validate_argument_observed, validate_type_observed};
use crate::{
    AggregateOrderKey, AggregateStateFormatIdentity, EvaluatedArgument, FunctionArgumentType,
    FunctionCallContract, FunctionResultType, KernelEvaluationControl, KernelFailure, Selection,
};
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompilePhase, FunctionKind, FunctionValueType,
    PureCompileControl,
};
use std::sync::Arc;
mod encoding;
pub use encoding::AggregateArgumentEncoding;

/// Per-call phase; a node may mix update and merge calls. Sequence identity and
/// complete/partial responsibility remain in the checked program lineage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateKernelPhase {
    Single,
    Partial,
    Intermediate,
    Final,
}
impl AggregateKernelPhase {
    pub const fn consumes_logical_arguments(self) -> bool {
        matches!(self, Self::Single | Self::Partial)
    }
    pub const fn produces_final_result(self) -> bool {
        matches!(self, Self::Single | Self::Final)
    }
}

/// The sole state-domain check borrows its exact immutable inputs. This proof
/// is local preparation evidence, not a graph or wire identity.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ValidatedAggregateMergeState<'a> {
    input: &'a FunctionValueType,
    intermediate: &'a FunctionValueType,
}

/// The options own the same full type that was borrowed by the runtime demand.
/// Only the observed exact comparison below can construct this token.
pub(crate) struct AlignedAggregateMergeState<'a> {
    input: &'a FunctionValueType,
    owned: FunctionValueType,
}

pub(crate) fn align_aggregate_merge_state_observed<'a>(
    input: &'a FunctionValueType,
    owned: FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AlignedAggregateMergeState<'a>, KernelFailure> {
    if !input
        .exactly_equals_observed::<KernelFailure>(&owned, || work.step().map_err(compile_failure))?
    {
        return Err(invalid(
            "aggregate options differ from the actual merge state type",
        ));
    }
    Ok(AlignedAggregateMergeState { input, owned })
}

/// Borrow the original state-domain author at a transport input frontier.
/// The caller keeps its existing checkpoint scope; this emits no new footer,
/// selection, options, type tree, or constructor reservation.
pub fn check_aggregate_merge_state_type_observed(
    input: &FunctionValueType,
    intermediate: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    validate_aggregate_merge_state_observed(input, intermediate, work).map(|_| ())
}

pub(crate) fn validate_aggregate_merge_state_observed<'a>(
    input: &'a FunctionValueType,
    intermediate: &'a FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValidatedAggregateMergeState<'a>, KernelFailure> {
    validate_type_observed(input, work)?;
    if !input.same_value_domain_observed::<KernelFailure>(intermediate, || {
        work.step().map_err(compile_failure)
    })? || (intermediate.nullable() && !input.nullable())
    {
        return Err(invalid(
            "aggregate merge state layout differs from its selected intermediate domain",
        ));
    }
    Ok(ValidatedAggregateMergeState {
        input,
        intermediate,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateCallContract {
    call: Arc<FunctionCallContract>,
    phase: AggregateKernelPhase,
    distinct: bool,
    order_keys: Arc<[AggregateOrderKey]>,
    state_input_type: Option<FunctionValueType>,
    state_interpretation: Option<Arc<novarocks_type_contract::AggregateStateInterpretation>>,
}
impl AggregateCallContract {
    /// Preserve the same logical binding in every phase. Merge input layout
    /// may widen the emission's top-level nullability, never its value domain.
    /// Bind semantic options such as output lengths from frozen parameters in
    /// the exact implementation; this contract mints no memory allowance.
    pub fn try_new(
        call: Arc<FunctionCallContract>,
        phase: AggregateKernelPhase,
        distinct: bool,
        order_keys: Arc<[AggregateOrderKey]>,
        state_input_type: Option<FunctionValueType>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        Self::try_new_impl(
            call,
            crate::AggregatePreparationOptions {
                phase,
                distinct,
                order_keys,
                state_input_type,
                state_interpretation: None,
            },
            None,
            control,
        )
    }

    pub(crate) fn try_new_refined(
        call: Arc<FunctionCallContract>,
        phase: AggregateKernelPhase,
        distinct: bool,
        order_keys: Arc<[AggregateOrderKey]>,
        state_interpretation: Option<Arc<novarocks_type_contract::AggregateStateInterpretation>>,
        state: Option<AlignedAggregateMergeState<'_>>,
        proof: Option<ValidatedAggregateMergeState<'_>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let validated = match (state, proof) {
            (None, None) => None,
            (Some(state), Some(proof)) if std::ptr::eq(state.input, proof.input) => {
                Some((state.owned, proof))
            }
            _ => {
                return Err(invalid(
                    "aggregate merge receipt differs from its actual state options",
                ));
            }
        };
        let (state_input_type, proof) = match validated {
            Some((state, proof)) => (Some(state), Some(proof)),
            None => (None, None),
        };
        Self::try_new_impl(
            call,
            crate::AggregatePreparationOptions {
                phase,
                distinct,
                order_keys,
                state_input_type,
                state_interpretation,
            },
            proof,
            control,
        )
    }

    fn try_new_impl(
        call: Arc<FunctionCallContract>,
        options: crate::AggregatePreparationOptions,
        proof: Option<ValidatedAggregateMergeState<'_>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let crate::AggregatePreparationOptions {
            phase,
            distinct,
            order_keys,
            state_input_type,
            state_interpretation,
        } = options;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result = (|| {
            if call.kind() != FunctionKind::Aggregate
                || call.effects().argument_control != ArgumentControl::Aggregate
            {
                return Err(invalid(
                    "aggregate preparation requires its exact aggregate control",
                ));
            }
            let Some(state) = &call.selected().aggregate else {
                return Err(invalid("aggregate binding has no state contract"));
            };
            if !matches!(call.selected().result_type, FunctionResultType::Scalar(_)) {
                return Err(invalid("aggregate binding requires a scalar final result"));
            }
            let channels = &call.selected().argument_types;
            let logical = call.logical_argument_count();
            if logical > channels.len() || order_keys.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
                return Err(invalid("aggregate logical/order channel shape differs"));
            }
            for channel in channels {
                if !matches!(channel, FunctionArgumentType::Value(_)) {
                    return Err(invalid("aggregate channel cannot be a lambda"));
                }
                work.step().map_err(compile_failure)?;
            }
            if let Some(original) = &state_interpretation {
                if original.order_keys.len() != channels.len() - logical {
                    return Err(invalid(
                        "aggregate state interpretation differs from selected order channels",
                    ));
                }
                if phase.consumes_logical_arguments() {
                    if original.distinct != distinct
                        || original.order_keys.len() != order_keys.len()
                    {
                        return Err(invalid(
                            "aggregate update changes its original state interpretation",
                        ));
                    }
                    for (original, actual) in original.order_keys.iter().zip(order_keys.iter()) {
                        work.step().map_err(compile_failure)?;
                        if original.ascending != actual.ascending
                            || original.nulls_first != actual.nulls_first
                        {
                            return Err(invalid(
                                "aggregate update changes its original state ordering",
                            ));
                        }
                    }
                }
            }
            if phase.consumes_logical_arguments() {
                if state_input_type.is_some() || order_keys.len() != channels.len() - logical {
                    return Err(invalid(
                        "aggregate update channels differ from its selected logical/order signature",
                    ));
                }
            } else {
                if distinct || !order_keys.is_empty() {
                    return Err(invalid(
                        "aggregate merge cannot repeat DISTINCT or function ORDER BY",
                    ));
                }
                let Some(input) = &state_input_type else {
                    return Err(invalid("aggregate merge has no exact state input layout"));
                };
                match proof {
                    Some(proof) if std::ptr::eq(proof.intermediate, &state.intermediate_type) => {
                        // The exact runtime source was checked once by refinement;
                        // its owned options were aligned before that refinement.
                        work.step().map_err(compile_failure)?;
                    }
                    Some(_) => {
                        return Err(invalid(
                            "aggregate state receipt has a different selected owner",
                        ));
                    }
                    None => {
                        validate_aggregate_merge_state_observed(
                            input,
                            &state.intermediate_type,
                            &mut work,
                        )?;
                    }
                }
            }
            Ok(())
        })();
        match result {
            Err(error) if error.is_interruption() => return Err(error),
            result => {
                work.finish().map_err(compile_failure)?;
                result?;
            }
        }
        Ok(Self {
            call,
            phase,
            distinct,
            order_keys,
            state_input_type,
            state_interpretation,
        })
    }
    /// Preserve one real Single binding while exposing the original local
    /// partial/merge lifecycle. These addresses are not physical call sites.
    pub(crate) fn local_stages(
        source: &Arc<Self>,
        control: &dyn PureCompileControl,
    ) -> Result<(Arc<Self>, Arc<Self>), KernelFailure> {
        if source.phase != AggregateKernelPhase::Single {
            return Err(invalid(
                "local aggregate stages require an actual Single source",
            ));
        }
        let partial = Self::try_new_impl(
            Arc::clone(&source.call),
            crate::AggregatePreparationOptions {
                phase: AggregateKernelPhase::Partial,
                distinct: source.distinct,
                order_keys: Arc::clone(&source.order_keys),
                state_input_type: None,
                state_interpretation: source.state_interpretation.clone(),
            },
            None,
            control,
        )?;
        let final_stage = Self::try_new_impl(
            Arc::clone(&source.call),
            crate::AggregatePreparationOptions {
                phase: AggregateKernelPhase::Final,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: Some(source.intermediate_type().clone()),
                state_interpretation: source.state_interpretation.clone(),
            },
            None,
            control,
        )?;
        Ok((Arc::new(partial), Arc::new(final_stage)))
    }
    pub fn state_interpretation(
        &self,
    ) -> Option<&novarocks_type_contract::AggregateStateInterpretation> {
        self.state_interpretation.as_deref()
    }
    pub fn call(&self) -> &Arc<FunctionCallContract> {
        &self.call
    }
    pub const fn phase(&self) -> AggregateKernelPhase {
        self.phase
    }
    pub const fn distinct(&self) -> bool {
        self.distinct
    }
    pub fn order_keys(&self) -> &[AggregateOrderKey] {
        &self.order_keys
    }
    pub fn logical_argument_types(&self) -> impl ExactSizeIterator<Item = &FunctionValueType> {
        self.call.selected().argument_types[..self.call.logical_argument_count()]
            .iter()
            .map(value_type)
    }
    pub fn order_argument_types(&self) -> impl ExactSizeIterator<Item = &FunctionValueType> {
        self.call.selected().argument_types[self.call.logical_argument_count()..]
            .iter()
            .map(value_type)
    }
    pub fn intermediate_type(&self) -> &FunctionValueType {
        &self
            .call
            .selected()
            .aggregate
            .as_ref()
            .expect("checked aggregate")
            .intermediate_type
    }
    pub fn state_format(&self) -> &AggregateStateFormatIdentity {
        &self
            .call
            .selected()
            .aggregate
            .as_ref()
            .expect("checked aggregate")
            .state_format
    }
    pub fn final_type(&self) -> &FunctionValueType {
        match &self.call.selected().result_type {
            FunctionResultType::Scalar(value) => value,
            _ => unreachable!("checked scalar result"),
        }
    }
    pub const fn state_input_type(&self) -> Option<&FunctionValueType> {
        self.state_input_type.as_ref()
    }
}
fn value_type(value: &FunctionArgumentType) -> &FunctionValueType {
    match value {
        FunctionArgumentType::Value(value) => value,
        _ => unreachable!("checked aggregate value channel"),
    }
}

/// Evaluated logical and function ORDER BY channels stay independent; no
/// packed Struct carrier is guessed. Empty selections invoke no state update.
/// This input does not sort, authorize reordering, or choose a group state.
#[derive(Clone, Copy, Debug)]
pub struct SelectedAggregateUpdateInput<'call, 'a> {
    contract: &'call AggregateCallContract,
    installed_owner: Option<&'call dyn crate::aggregate_erasure::ErasedAggregateOps>,
    selection: Selection<'a>,
    logical_arguments: &'a [EvaluatedArgument<'a>],
    order_arguments: &'a [EvaluatedArgument<'a>],
}
impl<'call, 'a> SelectedAggregateUpdateInput<'call, 'a> {
    pub fn try_new(
        contract: &'call AggregateCallContract,
        selection: Selection<'a>,
        logical_arguments: &'a [EvaluatedArgument<'a>],
        order_arguments: &'a [EvaluatedArgument<'a>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        if !contract.phase.consumes_logical_arguments()
            || logical_arguments.len() != contract.logical_argument_types().len()
            || order_arguments.len() != contract.order_argument_types().len()
        {
            return Err(invalid(
                "aggregate update input differs from its exact call phase/channels",
            ));
        }
        for (argument, value_type) in logical_arguments
            .iter()
            .zip(contract.logical_argument_types())
            .chain(order_arguments.iter().zip(contract.order_argument_types()))
        {
            validate_argument_observed(*argument, selection, value_type, control)?;
        }
        Ok(Self {
            contract,
            installed_owner: None,
            selection,
            logical_arguments,
            order_arguments,
        })
    }
    /// Use only encoding facts from the actual installed local implementation.
    /// Sources are the caller's existing complete bindings; no type is inferred
    /// from Arrow metadata and no holder or capability vector is allocated.
    pub fn try_new_with_typed_sources(
        contract: &'call AggregateCallContract,
        selection: Selection<'a>,
        logical_arguments: &'a [EvaluatedArgument<'a>],
        order_arguments: &'a [EvaluatedArgument<'a>],
        logical_sources: &[&FunctionValueType],
        order_sources: &[&FunctionValueType],
        encodings: &[AggregateArgumentEncoding<'call>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        if logical_sources.len() != logical_arguments.len()
            || order_sources.len() != order_arguments.len()
            || encodings.len() != logical_arguments.len() + order_arguments.len()
        {
            control.checkpoint(0)?;
            return Err(invalid(
                "typed aggregate update differs from exact call phase/channels",
            ));
        }
        Self::try_new_with_typed_source_lookup(
            contract,
            selection,
            logical_arguments,
            order_arguments,
            |ordinal| {
                let source = if ordinal < logical_sources.len() {
                    logical_sources.get(ordinal)
                } else {
                    order_sources.get(ordinal - logical_sources.len())
                };
                source
                    .copied()
                    .ok_or_else(|| invalid("typed aggregate source ordinal is absent"))
            },
            |ordinal| {
                encodings
                    .get(ordinal)
                    .copied()
                    .ok_or_else(|| invalid("installed aggregate encoding ordinal is absent"))
            },
            control,
        )
    }

    /// Borrow exact checked source definitions and installed implementation
    /// facts directly. The host needs no per-batch source/capability vectors.
    pub fn try_new_with_typed_source_lookup<'source>(
        contract: &'call AggregateCallContract,
        selection: Selection<'a>,
        logical_arguments: &'a [EvaluatedArgument<'a>],
        order_arguments: &'a [EvaluatedArgument<'a>],
        mut source_at: impl FnMut(usize) -> Result<&'source FunctionValueType, KernelFailure>,
        mut encoding_at: impl FnMut(usize) -> Result<AggregateArgumentEncoding<'call>, KernelFailure>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        if !contract.phase.consumes_logical_arguments()
            || logical_arguments.len() != contract.logical_argument_types().len()
            || order_arguments.len() != contract.order_argument_types().len()
        {
            return Err(invalid(
                "typed aggregate update differs from exact call phase/channels",
            ));
        }
        let mut installed_owner: Option<&'call dyn crate::aggregate_erasure::ErasedAggregateOps> =
            None;
        for (ordinal, (argument, expected)) in logical_arguments
            .iter()
            .chain(order_arguments.iter())
            .zip(
                contract
                    .logical_argument_types()
                    .chain(contract.order_argument_types()),
            )
            .enumerate()
        {
            let source = source_at(ordinal)?;
            let encoding = encoding_at(ordinal)?;
            encoding.validate_scope(contract, ordinal)?;
            let owner = encoding.installed_owner()?;
            if installed_owner.is_some_and(|previous| !std::ptr::addr_eq(previous, owner)) {
                return Err(invalid(
                    "aggregate typed inputs differ in installed adapter owner",
                ));
            }
            installed_owner = Some(owner);
            encoding::validate_typed_argument(
                *argument, source, expected, selection, encoding, control,
            )?;
        }
        Ok(Self {
            contract,
            installed_owner,
            selection,
            logical_arguments,
            order_arguments,
        })
    }

    pub(crate) fn validate_installed_owner(
        self,
        owner: Option<&dyn crate::aggregate_erasure::ErasedAggregateOps>,
    ) -> Result<(), KernelFailure> {
        if let Some(expected) = self.installed_owner
            && !owner.is_some_and(|actual| std::ptr::addr_eq(expected, actual))
        {
            return Err(invalid(
                "aggregate typed input differs from actual installed adapter",
            ));
        }
        Ok(())
    }

    pub const fn contract(self) -> &'call AggregateCallContract {
        self.contract
    }
    pub const fn selection(self) -> Selection<'a> {
        self.selection
    }
    pub const fn logical_arguments(self) -> &'a [EvaluatedArgument<'a>] {
        self.logical_arguments
    }
    pub const fn order_arguments(self) -> &'a [EvaluatedArgument<'a>] {
        self.order_arguments
    }
}

/// Merge retains the logical binding and frozen state format. Its one state
/// carrier is validated against the checked input layout, whose nullability
/// may be wider than intermediate emission; the implementation owns NULL rules.
#[derive(Clone, Copy, Debug)]
pub struct SelectedAggregateMergeInput<'call, 'a> {
    contract: &'call AggregateCallContract,
    selection: Selection<'a>,
    state: EvaluatedArgument<'a>,
}
impl<'call, 'a> SelectedAggregateMergeInput<'call, 'a> {
    pub fn try_new(
        contract: &'call AggregateCallContract,
        selection: Selection<'a>,
        state: EvaluatedArgument<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        if contract.phase.consumes_logical_arguments() {
            return Err(invalid(
                "aggregate update phase cannot consume a merge input",
            ));
        }
        let value_type = contract
            .state_input_type
            .as_ref()
            .expect("checked merge layout");
        validate_argument_observed(state, selection, value_type, control)?;
        Ok(Self {
            contract,
            selection,
            state,
        })
    }
    pub const fn contract(self) -> &'call AggregateCallContract {
        self.contract
    }
    pub const fn selection(self) -> Selection<'a> {
        self.selection
    }
    pub const fn state(self) -> EvaluatedArgument<'a> {
        self.state
    }
}

#[cfg(test)]
mod tests;

/// Logical result and exchange state contracts of one selected aggregate.
/// Persistent callers also bind each actual state column and its encoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistentAggregateContract {
    pub function: crate::FunctionId,
    pub overload: crate::FunctionOverloadId,
    pub state_format: AggregateStateFormatIdentity,
}

impl PersistentAggregateContract {
    pub fn from_binding(binding: &crate::ResolvedFunctionBinding) -> Option<Self> {
        let state = binding.selected.aggregate.as_ref()?;
        let function = binding.function_id.clone();
        let overload = binding.selected.overload.clone();
        Some(Self {
            function,
            overload,
            state_format: state.state_format.clone(),
        })
    }
}

impl crate::EngineFunctionCatalog {
    pub fn admits_persistent_aggregate(&self, contract: &PersistentAggregateContract) -> bool {
        let Some(declaration) = self
            .definition_by_id(&contract.function)
            .and_then(crate::FunctionDefinition::binding_declaration)
        else {
            return false;
        };
        declaration.kind() == crate::FunctionKind::Aggregate
            && declaration.overloads().iter().any(|overload| {
                overload.identity == contract.overload
                    && overload
                        .aggregate
                        .as_ref()
                        .is_some_and(|state| state.state_format == contract.state_format)
            })
    }
}
