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

//! Typed whole-invocation scalar lifecycle. It consumes the same immutable
//! ordinary call contract and ONE scalar carrier-validation author. Activation
//! is an actual caller demand fact, never inferred from an empty Selection.
pub use crate::builtin::scalar_invocation_data::{ScalarInvocationData, ScalarInvocationFailure};
use crate::kernel_control::{internal, invalid};
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::scalar_kernel::{ScalarInvocationError, ScalarInvocationRuntime, evaluate_scalar_once};
use crate::*;
use std::{fmt, sync::Arc};

/// Actual invocation demand supplied by the owning expression controller.
/// ValidateOnly is permitted only for a zero-row structural validation root;
/// Activated includes the original direct or IF else call on an empty domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarInvocationActivation {
    ValidateOnly,
    Activated,
}

pub trait PreparedInvocationScalarKernel: Send + Sync + fmt::Debug {
    fn contract(&self) -> &Arc<ScalarCallContract>;
    /// A factual exact-owner scheduling policy. The whole-Data ABI alone
    /// does not request full-domain children for existing owners.
    fn input_domain_policy(&self) -> InvocationInputDomainPolicy {
        InvocationInputDomainPolicy::SelectedDemand
    }
    /// Explicitly uncovered unless this exact prepared owner supplies an
    /// audited complete request source. No name or ABI selects a profile.
    fn invocation_resource_profile(&self) -> Option<WholeInvocationResourceProfile> {
        None
    }
    fn instance_retained_upper_bound(&self) -> usize;
    /// Exact real Box Layout. New owners have no allocator-free default.
    fn instance_inline_allocation_bytes(&self) -> usize;
    fn create_instance_with_allocator(
        &self,
        allocator: Arc<dyn AggregateStateAllocator>,
    ) -> Result<Box<dyn InvocationScalarKernelInstance>, KernelFailure>;
}
pub trait InvocationScalarKernelInstance: Send {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarInvocationFailure>;
    /// Explicit scoped entry for owners declaring OriginalActualDomain.
    /// Older selected-demand instances remain unchanged and refuse this entry.
    fn evaluate_scoped<'a>(
        &mut self,
        _input: ScopedScalarCallInput<'_, 'a>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarInvocationFailure> {
        Err(invalid("scalar owner does not implement explicit invocation scope").into())
    }
    fn retained_bytes(&self) -> usize;
}
impl ScalarInvocationError for ScalarInvocationFailure {
    fn omit_failure_footer(&self) -> bool {
        true
    }
}
impl ScalarInvocationRuntime<ScalarInvocationFailure> for dyn InvocationScalarKernelInstance {
    fn invoke<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarInvocationFailure> {
        self.evaluate(input, control)
    }
    fn retained_bytes(&self) -> usize {
        InvocationScalarKernelInstance::retained_bytes(self)
    }
}

/// ONE actual owner supplies full binding, original parameter facts, effect
/// refinement and pure preparation. A whole Data result is never a row error.
pub trait PureInvocationScalarImplementation:
    FunctionBindingResolver + FunctionEffectOwner<Error = FunctionBindingError>
{
    fn prepare_invocation_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Arc<dyn PreparedInvocationScalarKernel>, KernelFailure>;
    fn prepare_invocation_scalar_checked(
        &self,
        _input: CallEffectInput<'_>,
        _checked: &crate::CheckedCallEffectInput<'_>,
        _contract: Arc<ScalarCallContract>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Arc<dyn PreparedInvocationScalarKernel>, crate::CheckedOwnerFailure<KernelFailure>>
    {
        Err(crate::CheckedOwnerFailure::UnconvertedOwner)
    }
}
#[derive(Debug)]
pub struct InvocationScalarSpecialization {
    prepared: Arc<dyn PreparedInvocationScalarKernel>,
    effects: ScopedExpressionEffects,
}
impl InvocationScalarSpecialization {
    pub fn prepared(&self) -> &Arc<dyn PreparedInvocationScalarKernel> {
        &self.prepared
    }
    pub const fn effects(&self) -> ScopedExpressionEffects {
        self.effects
    }
    pub fn into_prepared(self) -> Arc<dyn PreparedInvocationScalarKernel> {
        self.prepared
    }
}
pub fn specialize_invocation_scalar<O: PureInvocationScalarImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    arguments: ScopedExpressionEffects,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<InvocationScalarSpecialization, FunctionSpecializationFailure> {
    specialize_invocation_scalar_once(owner, input, selected, None, arguments, control)
}
pub fn specialize_frozen_invocation_scalar<O: PureInvocationScalarImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: &novarocks_type_contract::CallEffects,
    arguments: ScopedExpressionEffects,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<InvocationScalarSpecialization, FunctionSpecializationFailure> {
    specialize_invocation_scalar_once(owner, input, selected, Some(frozen), arguments, control)
}
fn specialize_invocation_scalar_once<O: PureInvocationScalarImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&novarocks_type_contract::CallEffects>,
    arguments: ScopedExpressionEffects,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<InvocationScalarSpecialization, FunctionSpecializationFailure> {
    specialize_invocation_scalar_once_core(
        owner,
        input,
        selected,
        frozen,
        arguments,
        control,
        |arguments| {
            crate::specialization::refine_once_for_specialization(
                owner, input, frozen, arguments, control,
            )
        },
        |contract| {
            owner
                .prepare_invocation_scalar(input, contract, control)
                .map_err(FunctionSpecializationFailure::Kernel)
        },
    )
}

pub(crate) fn specialize_invocation_scalar_once_checked<
    O: PureInvocationScalarImplementation + ?Sized,
>(
    owner: &O,
    checked: &crate::CheckedCallEffectInput<'_>,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&novarocks_type_contract::CallEffects>,
    arguments: ScopedExpressionEffects,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<InvocationScalarSpecialization, crate::CheckedOwnerFailure<FunctionSpecializationFailure>>
{
    specialize_invocation_scalar_once_core(
        owner,
        input,
        selected,
        frozen,
        arguments,
        control,
        |arguments| {
            crate::specialization::refine_once_checked(
                owner, checked, input, frozen, arguments, control,
            )
        },
        |contract| {
            owner
                .prepare_invocation_scalar_checked(input, checked, contract, control)
                .map_err(|error| match error {
                    crate::CheckedOwnerFailure::Original(error) => {
                        crate::CheckedOwnerFailure::Original(FunctionSpecializationFailure::Kernel(
                            error,
                        ))
                    }
                    crate::CheckedOwnerFailure::UnconvertedOwner => {
                        crate::CheckedOwnerFailure::UnconvertedOwner
                    }
                })
        },
    )
}

fn specialize_invocation_scalar_once_core<
    'a,
    O: PureInvocationScalarImplementation + ?Sized,
    E: From<FunctionSpecializationFailure>,
>(
    _owner: &O,
    input: CallEffectInput<'a>,
    selected: Arc<FunctionBindingSelection>,
    _frozen: Option<&novarocks_type_contract::CallEffects>,
    arguments: ScopedExpressionEffects,
    control: &dyn novarocks_type_contract::PureCompileControl,
    refine: impl FnOnce(
        ScopedExpressionEffects,
    ) -> Result<(crate::RefinedCallEffects<'a>, ScopedExpressionEffects), E>,
    prepare: impl FnOnce(Arc<ScalarCallContract>) -> Result<Arc<dyn PreparedInvocationScalarKernel>, E>,
) -> Result<InvocationScalarSpecialization, E> {
    let (receipt, effects) = refine(arguments)?;
    let contract = Arc::new(
        ScalarCallContract::from_refined_invocation(input, &receipt, selected, control)
            .map_err(FunctionSpecializationFailure::Kernel)?,
    );
    let prepared = prepare(contract.clone())?;
    if !Arc::ptr_eq(prepared.contract(), &contract) {
        return Err(FunctionSpecializationFailure::Kernel(internal(
            "scalar preparation replaced its exact immutable contract",
        ))
        .into());
    }
    // The typed ABI is the sole own-invocation failure capability author.
    // No names, input value probes, or row-error refinement classify it.
    let effects = effects.with_invocation_data_capability();
    control
        .checkpoint(
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            0,
        )
        .map_err(FunctionSpecializationFailure::Control)?;
    Ok(InvocationScalarSpecialization { prepared, effects })
}

/// One instance's real host and Box custody. Full Data and every Kernel cause
/// latch immediately. Old ScalarV1 wrappers continue their original policy.
pub struct InvocationScalarEvaluationInstance {
    instance: Box<dyn InvocationScalarKernelInstance>,
    _prepared: Arc<dyn PreparedInvocationScalarKernel>,
    contract: Arc<ScalarCallContract>,
    retained_upper_bound: usize,
    failed: bool,
    inline_charge: OpaqueRetainedCharge,
    host: Arc<dyn AggregateStateAllocator>,
}
impl InvocationScalarEvaluationInstance {
    pub fn instantiate_with_allocator(
        prepared: Arc<dyn PreparedInvocationScalarKernel>,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
    ) -> Result<Self, KernelFailure> {
        let contract = Arc::clone(prepared.contract());
        let retained_upper_bound = prepared.instance_retained_upper_bound();
        std::mem::size_of::<Self>()
            .checked_add(retained_upper_bound)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let actual = allocator
            .ok_or_else(|| invalid("whole-invocation scalar requires an actual allocation host"))?;
        let mut inline_charge = OpaqueRetainedCharge::try_new(Arc::clone(&actual))?;
        let inline_bytes = prepared.instance_inline_allocation_bytes();
        let mut reservation = if inline_bytes == 0 {
            None
        } else {
            Some(inline_charge.reserve_operation(inline_bytes)?)
        };
        let instance = prepared.create_instance_with_allocator(actual.clone())?;
        if let Some(reservation) = &mut reservation {
            inline_charge.reconcile_under_reservation(inline_bytes, reservation)?;
        }
        if instance.retained_bytes() > retained_upper_bound {
            return Err(internal(
                "new scalar instance exceeded its authorized bound",
            ));
        }
        Ok(Self {
            instance,
            _prepared: prepared,
            contract,
            retained_upper_bound,
            failed: false,
            inline_charge,
            host: actual,
        })
    }
    pub fn contract(&self) -> &ScalarCallContract {
        &self.contract
    }
    /// Original first-cause latch, readable without a diagnostic or callback.
    pub const fn is_failed(&self) -> bool {
        self.failed
    }
    pub fn input_domain_policy(&self) -> InvocationInputDomainPolicy {
        self._prepared.input_domain_policy()
    }
    pub fn invocation_resource_profile(&self) -> Option<WholeInvocationResourceProfile> {
        self._prepared.invocation_resource_profile()
    }
    pub(crate) fn matches_resource_owner(
        &self,
        owner: &Arc<dyn PreparedInvocationScalarKernel>,
    ) -> bool {
        Arc::ptr_eq(&self._prepared, owner)
    }
    pub fn retained_bytes(&self) -> Result<usize, KernelFailure> {
        let bytes = self.instance.retained_bytes();
        if bytes > self.retained_upper_bound {
            return Err(internal(
                "scalar instance exceeded its lifetime retained bound",
            ));
        }
        Ok(std::mem::size_of::<Self>() + bytes)
    }
    pub fn retained_upper_bound(&self) -> usize {
        std::mem::size_of::<Self>() + self.retained_upper_bound
    }
    pub fn evaluate<'a>(
        &mut self,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        activation: ScalarInvocationActivation,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarInvocationFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = if self.input_domain_policy() != InvocationInputDomainPolicy::SelectedDemand {
            Err(invalid("original-domain scalar requires an explicit invocation scope").into())
        } else if activation == ScalarInvocationActivation::ValidateOnly && !selection.is_empty() {
            Err(invalid("nonempty scalar demand cannot be validation-only").into())
        } else {
            evaluate_scalar_once(
                self.instance.as_mut(),
                &self.contract,
                self.retained_upper_bound,
                selection,
                arguments,
                activation == ScalarInvocationActivation::Activated,
                control,
            )
        };
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub fn evaluate_scoped<'a>(
        &mut self,
        scope: ScalarInvocationScope<'a>,
        sources: &'a [ScalarInvocationSource<'a>],
        activation: ScalarInvocationActivation,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarInvocationFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = (|| {
            if self.input_domain_policy() != InvocationInputDomainPolicy::OriginalActualDomain {
                return Err(invalid(
                    "selected-demand scalar does not accept an original-domain scope",
                )
                .into());
            }
            if activation == ScalarInvocationActivation::ValidateOnly && !scope.demand().is_empty()
            {
                return Err(invalid("nonempty scalar demand cannot be validation-only").into());
            }
            scope.validate(control)?;
            control.checkpoint(0)?;
            let expected = self.contract.value_argument_types();
            if expected.len() != sources.len() {
                return Err(
                    invalid("scoped scalar sources differ from the exact call shape").into(),
                );
            }
            for (source, ty) in sources.iter().zip(expected) {
                source.validate(scope, ty, control)?;
            }
            if activation == ScalarInvocationActivation::ValidateOnly && scope.demand().is_empty() {
                return SelectedValues::try_new(
                    scope.demand(),
                    self.contract.result_type().data_type(),
                    crate::invocation_scope::retain_scoped_result_type(
                        arrow_array::new_empty_array(self.contract.result_type().data_type()),
                        self.contract.result_type(),
                        &self.host,
                        control,
                    )?,
                    Box::default(),
                )
                .map_err(|_| internal("empty scoped scalar result violates its contract").into());
            }
            let output = self.instance.evaluate_scoped(
                ScopedScalarCallInput {
                    contract: &self.contract,
                    scope,
                    sources,
                },
                control,
            )?;
            if self.instance.retained_bytes() > self.retained_upper_bound {
                return Err(
                    internal("scalar instance exceeded its lifetime retained bound").into(),
                );
            }
            crate::scalar_kernel::validate_scalar_output_observed(
                &output,
                scope.demand(),
                &self.contract,
                control,
            )?;
            control.checkpoint(0)?;
            Ok(output)
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}
