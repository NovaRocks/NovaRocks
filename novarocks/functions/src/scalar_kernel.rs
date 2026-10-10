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

//! Pure immutable scalar preparation and instance-owned batch invocation.
//! An ordinary kernel receives evaluated values, never an expression arena.

use std::{fmt, sync::Arc};

use arrow_array::new_empty_array;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CompilePhase, DecimalOverflowPolicy, ExpressionEffectContext,
    FunctionIntrinsicRowError, FunctionKind, FunctionValueType, PureCompileControl,
    SemanticParameters,
};

use crate::{
    CallEffectInput, EvaluatedArgument, FunctionArgumentType, FunctionBindingError,
    FunctionBindingResolver, FunctionBindingSelection, FunctionEffectOwner, FunctionId,
    FunctionResultType, FunctionSpecializationFailure, KernelEvaluationControl, KernelFailure,
    RefinedCallEffects, ScopedExpressionEffects, SelectedValues, Selection,
};

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::{EvaluationCheckpoints, logical_is_null, validate_argument_observed};

/// Ordinary scalar preparation retains the same checked call authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalarCallContract(crate::FunctionCallContract);
impl ScalarCallContract {
    pub fn from_refined(
        input: CallEffectInput<'_>,
        receipt: &RefinedCallEffects<'_>,
        selected: Arc<FunctionBindingSelection>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        if input.kind != FunctionKind::Scalar || input.selected.aggregate.is_some() {
            return Err(invalid(
                "ordinary scalar preparation requires a scalar binding",
            ));
        }
        if !matches!(
            receipt.facts().argument_control,
            ArgumentControl::Eager | ArgumentControl::TypeOnly
        ) {
            return Err(invalid(
                "guarded, higher-order and relational calls require their own control ABI",
            ));
        }
        crate::FunctionCallContract::from_refined(input, receipt, selected, control).map(Self)
    }
    pub(crate) fn from_refined_invocation(
        input: CallEffectInput<'_>,
        receipt: &RefinedCallEffects<'_>,
        selected: Arc<FunctionBindingSelection>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        if input.kind != FunctionKind::Scalar
            || input.selected.aggregate.is_some()
            || !matches!(
                receipt.facts().argument_control,
                ArgumentControl::Eager | ArgumentControl::NoArguments
            )
        {
            return Err(invalid(
                "whole-invocation scalar requires its exact argument demand",
            ));
        }
        crate::FunctionCallContract::from_refined(input, receipt, selected, control).map(Self)
    }
    pub const fn call(&self) -> &crate::FunctionCallContract {
        &self.0
    }
    pub const fn regexp_count_pattern_source(
        &self,
    ) -> Option<novarocks_type_contract::RegexpCountPatternSource> {
        self.0.regexp_count_pattern_source()
    }
    pub const fn to_base64_byte_source(
        &self,
    ) -> Option<novarocks_type_contract::ToBase64ByteSource> {
        self.0.to_base64_byte_source()
    }

    pub const fn function_id(&self) -> &FunctionId {
        self.0.function_id()
    }
    pub const fn context(&self) -> ExpressionEffectContext {
        self.0.context()
    }
    pub const fn decimal_overflow_policy(&self) -> DecimalOverflowPolicy {
        self.0.decimal_overflow_policy()
    }
    pub fn selected(&self) -> &FunctionBindingSelection {
        self.0.selected()
    }
    pub const fn effects(&self) -> &CallEffects {
        self.0.effects()
    }
    pub const fn parameters(&self) -> &SemanticParameters {
        self.0.parameters()
    }
    pub fn result_type(&self) -> &FunctionValueType {
        match &self.selected().result_type {
            FunctionResultType::Scalar(result) => result,
            FunctionResultType::Relation(_) => {
                unreachable!("constructor accepts scalar results only")
            }
        }
    }
    pub fn value_argument_types(&self) -> impl ExactSizeIterator<Item = &FunctionValueType> {
        let arguments = if matches!(
            self.effects().argument_control,
            ArgumentControl::TypeOnly | ArgumentControl::NoArguments
        ) {
            &[][..]
        } else {
            self.selected().argument_types.as_ref()
        };
        arguments.iter().map(|argument| match argument {
            FunctionArgumentType::Value(value) => value,
            FunctionArgumentType::Lambda { .. } => {
                unreachable!("type-only calls have no runtime value arguments")
            }
        })
    }
}

/// Exact ordered selected arguments. Logical identities remain in the checked
/// contract; equal Arrow carriers never reselect an overload or erase them.
#[derive(Clone, Copy, Debug)]
pub struct ScalarCallInput<'call, 'a> {
    contract: &'call ScalarCallContract,
    selection: Selection<'a>,
    arguments: &'a [EvaluatedArgument<'a>],
}
impl<'call, 'a> ScalarCallInput<'call, 'a> {
    pub const fn contract(self) -> &'call ScalarCallContract {
        self.contract
    }
    pub const fn selection(self) -> Selection<'a> {
        self.selection
    }
    pub const fn arguments(self) -> &'a [EvaluatedArgument<'a>] {
        self.arguments
    }
}

/// Immutable implementation preparation can be shared by drivers. It owns no
/// mutable instance, task, connector, credentials or expression lookup. The
/// exact implementation owner supplies this object after pure specialization.
/// Dispatch is once per selected batch, not once per row.
pub trait PreparedScalarKernel: Send + Sync + fmt::Debug {
    fn contract(&self) -> &Arc<ScalarCallContract>;
    /// Closed pure source of the complete original wrapper invocation.
    /// None explicitly leaves this source unproved; a positive source's
    /// arithmetic/model refusal must never be converted back to None.
    fn invocation_resource_profile(&self) -> Option<crate::ScalarInvocationResourceProfile> {
        None
    }
    /// Complete lifetime retained bound of one boxed instance: inline body and
    /// all bounded owned heap, including growth on error exits. The host
    /// authorizes construction and remaining mutation headroom before any
    /// allocation; post-call checking cannot recover an exceeded hard limit.
    fn instance_retained_upper_bound(&self) -> usize;
    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure>;
    /// Exact requested Layout size of the instance's real std Box. Zero
    /// preserves existing owners; a nonzero owner requires actual opaque
    /// admission before construction and retains it until Box deallocation.
    fn instance_inline_allocation_bytes(&self) -> usize {
        0
    }
    /// Borrow a real host capability. Existing fixed instances preserve their
    /// original construction; owners requiring backing refuse a missing host.
    fn create_instance_with_allocator(
        &self,
        _allocator: Option<Arc<dyn crate::AggregateStateAllocator>>,
    ) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        self.create_instance()
    }
}

/// One exact immutable owner supplies binding validation, effect refinement
/// and pure preparation. Invalid constant row data remains a delayed recipe;
/// cancellation, resource and internal failures are immediate outer failures.
pub trait PureScalarImplementation:
    FunctionBindingResolver + FunctionEffectOwner<Error = FunctionBindingError>
{
    fn prepare_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure>;
}

/// One exact-owner preparation and complete use effects. The temporary
/// refinement receipt and compile control are never retained in this result.
#[derive(Debug)]
pub struct ScalarSpecialization {
    prepared: Arc<dyn PreparedScalarKernel>,
    effects: ScopedExpressionEffects,
}
impl ScalarSpecialization {
    pub fn prepared(&self) -> &Arc<dyn PreparedScalarKernel> {
        &self.prepared
    }
    pub const fn effects(&self) -> ScopedExpressionEffects {
        self.effects
    }
    pub fn into_prepared(self) -> Arc<dyn PreparedScalarKernel> {
        self.prepared
    }
}

/// FE's fresh exact-owner entry. Supplied child effects must already include
/// every required child/domain; a function's own no-error fact cannot erase
/// an argument's failure, state or observable effect.
pub fn specialize_scalar<O: PureScalarImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    arguments: ScopedExpressionEffects,
    control: &dyn PureCompileControl,
) -> Result<ScalarSpecialization, FunctionSpecializationFailure> {
    specialize_scalar_once(owner, input, selected, None, arguments, control)
}

/// BE's frozen exact-owner entry. Refine once, compare every frozen fact,
/// compose children in this use's scope, and prepare through that same owner.
/// Inaccurate facts or scopes fail before invoking the implementation prepare.
pub fn specialize_frozen_scalar<O: PureScalarImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: &CallEffects,
    arguments: ScopedExpressionEffects,
    control: &dyn PureCompileControl,
) -> Result<ScalarSpecialization, FunctionSpecializationFailure> {
    specialize_scalar_once(owner, input, selected, Some(frozen), arguments, control)
}

fn specialize_scalar_once<O: PureScalarImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&CallEffects>,
    arguments: ScopedExpressionEffects,
    control: &dyn PureCompileControl,
) -> Result<ScalarSpecialization, FunctionSpecializationFailure> {
    let (receipt, effects) = crate::specialization::refine_once_for_specialization(
        owner, input, frozen, arguments, control,
    )?;
    let contract = Arc::new(
        ScalarCallContract::from_refined(input, &receipt, selected, control)
            .map_err(FunctionSpecializationFailure::Kernel)?,
    );
    let prepared = owner
        .prepare_scalar(input, contract.clone(), control)
        .map_err(FunctionSpecializationFailure::Kernel)?;
    if !Arc::ptr_eq(prepared.contract(), &contract) {
        return Err(FunctionSpecializationFailure::Kernel(internal(
            "scalar preparation replaced its exact immutable contract",
        )));
    }
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(FunctionSpecializationFailure::Control)?;
    Ok(ScalarSpecialization { prepared, effects })
}

/// Mutable state belongs to one expression use in one evaluation instance.
/// FE folding and each driver instantiate separately; cloning preparation
/// cannot clone or replay successful RNG/warning/wait effects.
pub trait ScalarKernelInstance: Send {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure>;
    /// O(1), exact known retained bytes for host reconciliation. The host
    /// supplies headroom for a bounded mutation before calling evaluate.
    fn retained_bytes(&self) -> usize;
}

/// Runtime wrapper retains the exact preparation and validates the selected
/// carrier on both sides of the implementation. Child errors must already be
/// resolved by the expression controller. Strict NULL demand is also decided
/// there; a strict kernel never receives rows skipped for SQL NULL inputs.
pub struct ScalarEvaluationInstance {
    // Drop mutable state while its exact immutable preparation is still alive.
    instance: Box<dyn ScalarKernelInstance>,
    _prepared: Arc<dyn PreparedScalarKernel>,
    contract: Arc<ScalarCallContract>,
    retained_upper_bound: usize,
    failed: bool,
    // The instance Box is destroyed before its actual opaque charge.
    inline_charge: Option<crate::opaque_memory::OpaqueRetainedCharge>,
}
impl ScalarEvaluationInstance {
    pub fn instantiate(prepared: Arc<dyn PreparedScalarKernel>) -> Result<Self, KernelFailure> {
        Self::instantiate_with_allocator(prepared, None)
    }
    pub fn instantiate_with_allocator(
        prepared: Arc<dyn PreparedScalarKernel>,
        allocator: Option<Arc<dyn crate::AggregateStateAllocator>>,
    ) -> Result<Self, KernelFailure> {
        let contract = Arc::clone(prepared.contract());
        let retained_upper_bound = prepared.instance_retained_upper_bound();
        // Reject an unrepresentable lifetime charge before creating state.
        std::mem::size_of::<Self>()
            .checked_add(retained_upper_bound)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let inline_bytes = prepared.instance_inline_allocation_bytes();
        let mut inline_charge = if inline_bytes == 0 {
            None
        } else {
            let actual = allocator.as_ref().cloned().ok_or_else(|| {
                invalid(&format!(
                    "{} requires an actual scalar opaque allocation host",
                    contract.function_id().as_str(),
                ))
            })?;
            if actual.opaque_allocation_host().is_none() {
                return Err(invalid(&format!(
                    "{} requires an actual scalar opaque allocation host",
                    contract.function_id().as_str(),
                )));
            }
            Some(crate::opaque_memory::OpaqueRetainedCharge::try_new(actual)?)
        };
        let mut reservation = inline_charge
            .as_ref()
            .map(|charge| charge.reserve_operation(inline_bytes))
            .transpose()?;
        let instance = prepared.create_instance_with_allocator(allocator)?;
        if let (Some(charge), Some(reservation)) = (&mut inline_charge, &mut reservation) {
            charge.reconcile_under_reservation(inline_bytes, reservation)?;
        }
        if instance.retained_bytes() > retained_upper_bound {
            return Err(internal(
                "new scalar instance exceeded its authorized bound",
            ));
        }
        Ok(Self {
            _prepared: prepared,
            contract,
            retained_upper_bound,
            instance,
            failed: false,
            inline_charge,
        })
    }
    pub fn contract(&self) -> &ScalarCallContract {
        &self.contract
    }
    pub fn invocation_resource_profile(&self) -> Option<crate::ScalarInvocationResourceProfile> {
        self._prepared.invocation_resource_profile()
    }
    pub fn retained_bytes(&self) -> Result<usize, KernelFailure> {
        let bytes = self.instance.retained_bytes();
        if bytes > self.retained_upper_bound {
            return Err(internal(
                "scalar instance exceeded its lifetime retained bound",
            ));
        }
        // The immutable upper bound was checked before instance creation.
        Ok(std::mem::size_of::<Self>() + bytes)
    }
    /// Shared preparation backing is accounted by its immutable owner. This
    /// bound covers this wrapper's inline body plus its one owned boxed state.
    pub fn retained_upper_bound(&self) -> usize {
        std::mem::size_of::<Self>() + self.retained_upper_bound
    }
    pub fn evaluate<'a>(
        &mut self,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = self.evaluate_once(selection, arguments, control);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn evaluate_once<'a>(
        &mut self,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        evaluate_scalar_once(
            self.instance.as_mut(),
            &self.contract,
            self.retained_upper_bound,
            selection,
            arguments,
            false,
            control,
        )
    }
}

/// ONE original scalar argument/output validation and lifecycle traversal.
/// Existing ScalarV1 retains its original empty-skip and three-cause footer
/// policy. The whole-invocation ABI invokes actual activated empty calls and
/// terminates ALL failures before reconciliation/output checks or callbacks.
pub(crate) trait ScalarInvocationError: From<KernelFailure> {
    fn omit_failure_footer(&self) -> bool;
}
impl ScalarInvocationError for KernelFailure {
    fn omit_failure_footer(&self) -> bool {
        matches!(
            self,
            Self::Cancelled | Self::DeadlineExceeded | Self::ResourceExhausted
        )
    }
}
pub(crate) trait ScalarInvocationRuntime<E> {
    fn invoke<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, E>;
    fn retained_bytes(&self) -> usize;
}
impl ScalarInvocationRuntime<KernelFailure> for dyn ScalarKernelInstance {
    fn invoke<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        self.evaluate(input, control)
    }
    fn retained_bytes(&self) -> usize {
        ScalarKernelInstance::retained_bytes(self)
    }
}
pub(crate) fn evaluate_scalar_once<'a, E: ScalarInvocationError>(
    instance: &mut (impl ScalarInvocationRuntime<E> + ?Sized),
    contract: &ScalarCallContract,
    retained_upper_bound: usize,
    selection: Selection<'a>,
    arguments: &'a [EvaluatedArgument<'a>],
    invoke_empty: bool,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, E> {
    control.checkpoint(0)?;
    let expected = contract.value_argument_types();
    if arguments.len() != expected.len() {
        return Err(E::from(invalid(
            "evaluated scalar arguments differ from the exact call shape",
        )));
    }
    for (argument, ty) in arguments.iter().zip(expected) {
        validate_argument_observed(*argument, selection, ty, control)?;
    }
    if selection.is_empty() && !invoke_empty {
        return SelectedValues::try_new(
            selection,
            &contract.result_type().data_type,
            new_empty_array(&contract.result_type().data_type),
            Box::default(),
        )
        .map_err(|_| E::from(internal("empty scalar result violates its contract")));
    }
    let result = instance.invoke(
        ScalarCallInput {
            contract,
            selection,
            arguments,
        },
        control,
    );
    if result
        .as_ref()
        .err()
        .is_some_and(ScalarInvocationError::omit_failure_footer)
    {
        // A private owner can refuse without latching this wrapper's
        // control. Its originating control cause wins over reconciliation.
        return result;
    }
    if instance.retained_bytes() > retained_upper_bound {
        return Err(E::from(internal(
            "scalar instance exceeded its lifetime retained bound",
        )));
    }
    let output = result?;
    if !output.errors().is_empty()
        && contract.effects().own_row_error != FunctionIntrinsicRowError::MayRaise
    {
        return Err(E::from(internal(
            "never-failing scalar implementation returned a row data error",
        )));
    }
    let mut work = EvaluationCheckpoints::new(control);
    if !output
        .selection()
        .same_rows_observed(selection, || work.step())?
        || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            output.values().data_type(),
            &contract.result_type().data_type,
            || work.step(),
        )?
    {
        return Err(E::from(internal(
            "scalar implementation returned an unrelated selection or type",
        )));
    }
    work.finish()?;
    if !contract.result_type().nullable {
        let mut work = EvaluationCheckpoints::new(control);
        let mut errors = output.errors().iter().peekable();
        for row in 0..output.values().len() {
            if errors
                .peek()
                .is_some_and(|error| error.selected_ordinal() == row)
            {
                errors.next();
                work.step()?;
            } else if logical_is_null(output.values().as_ref(), row, 1, &mut work)? {
                return Err(E::from(internal(
                    "non-null scalar implementation returned a successful SQL NULL",
                )));
            }
        }
        work.finish()?;
    }
    control.checkpoint(0)?;
    Ok(output)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "scalar_kernel/specialization_tests.rs"]
mod specialization_tests;
