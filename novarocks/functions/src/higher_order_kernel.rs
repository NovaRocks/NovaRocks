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

//! Immutable higher-order preparation and selected invocation-owned state.
//! Exact collection recipes retain their own zip/pair/map/NULL semantics.

use std::{fmt, sync::Arc};

#[cfg(test)]
use arrow_array::new_empty_array;
use novarocks_type_contract::{
    CallEffects, ExpressionEffectContext, FunctionIntrinsicRowError, PureCompileControl,
};

use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::{EvaluationCheckpoints, logical_is_null, validate_argument_observed};
use crate::{
    AggregateStateAllocator, CallEffectInput, EvaluatedArgument, FunctionArgumentType,
    FunctionBindingError, FunctionBindingResolver, FunctionBindingSelection, FunctionEffectOwner,
    FunctionSpecializationFailure, FunctionValueType, HigherOrderCallContract,
    KernelEvaluationControl, KernelFailure, LambdaBodyContract, RequiredLambdaBodyError,
    RowDataError, ScopedExpressionEffects, SelectedValues, Selection,
};
#[cfg(test)]
use crate::{
    BoundLambdaBodyEvaluator, LambdaBodyInput, LambdaBodyInvocation, LambdaBodyOutput,
    LambdaElementRowMap,
};

mod steps;
pub use steps::*;

/// The compiler supplies the inner root context/captures and complete body
/// effects. Ordinary child effects are supplied in the outer call context.
/// Specialization always joins the body effects; a parent never-fails fact
/// cannot discard them. Actual source/edge proofs remain compiler-owned.
#[derive(Clone, Debug)]
pub struct HigherOrderPreparationOptions {
    pub arguments: ScopedExpressionEffects,
    pub body_context: ExpressionEffectContext,
    pub body_effects: ScopedExpressionEffects,
    pub capture_types: Box<[FunctionValueType]>,
}

pub trait PureHigherOrderImplementation:
    FunctionBindingResolver + FunctionEffectOwner<Error = FunctionBindingError>
{
    fn prepare_higher_order(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<HigherOrderCallContract>,
        body: Arc<LambdaBodyContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedHigherOrderKernel>, KernelFailure>;
    fn prepare_higher_order_checked(
        &self,
        _input: CallEffectInput<'_>,
        _checked: &crate::CheckedCallEffectInput<'_>,
        _contract: Arc<HigherOrderCallContract>,
        _body: Arc<LambdaBodyContract>,
        _control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedHigherOrderKernel>, crate::CheckedOwnerFailure<KernelFailure>> {
        Err(crate::CheckedOwnerFailure::UnconvertedOwner)
    }
}

/// Shared preparation owns no body instance, expansion frame or live service.
/// Both exact call and body authorities are retained without substitution.
pub trait PreparedHigherOrderKernel: Send + Sync + fmt::Debug {
    fn contract(&self) -> &Arc<HigherOrderCallContract>;
    fn body_contract(&self) -> &Arc<LambdaBodyContract>;
    /// Lifetime bound of the boxed owner instance, including bounded owned
    /// heap. It excludes host-owned body state and invocation-local expansion
    /// backing; the host authorizes/account those under their actual owners.
    fn instance_retained_upper_bound(&self) -> usize;
    fn create_instance(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn HigherOrderKernelInstance>, KernelFailure>;
}

#[derive(Debug)]
pub struct HigherOrderSpecialization {
    prepared: Arc<dyn PreparedHigherOrderKernel>,
    effects: ScopedExpressionEffects,
}
impl HigherOrderSpecialization {
    pub fn prepared(&self) -> &Arc<dyn PreparedHigherOrderKernel> {
        &self.prepared
    }
    pub const fn effects(&self) -> ScopedExpressionEffects {
        self.effects
    }
    pub fn into_prepared(self) -> Arc<dyn PreparedHigherOrderKernel> {
        self.prepared
    }
}

pub fn specialize_higher_order<O: PureHigherOrderImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    options: HigherOrderPreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<HigherOrderSpecialization, FunctionSpecializationFailure> {
    specialize_once(owner, input, selected, None, options, control)
}
pub fn specialize_frozen_higher_order<O: PureHigherOrderImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: &CallEffects,
    options: HigherOrderPreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<HigherOrderSpecialization, FunctionSpecializationFailure> {
    specialize_once(owner, input, selected, Some(frozen), options, control)
}
fn specialize_once<O: PureHigherOrderImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&CallEffects>,
    options: HigherOrderPreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<HigherOrderSpecialization, FunctionSpecializationFailure> {
    specialize_once_core(
        owner,
        input,
        selected,
        frozen,
        options,
        control,
        |arguments| {
            crate::specialization::refine_once_for_specialization(
                owner, input, frozen, arguments, control,
            )
        },
        |contract, body| {
            owner
                .prepare_higher_order(input, contract, body, control)
                .map_err(FunctionSpecializationFailure::from)
        },
    )
}

pub(crate) fn specialize_once_checked<O: PureHigherOrderImplementation + ?Sized>(
    owner: &O,
    checked: &crate::CheckedCallEffectInput<'_>,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&CallEffects>,
    options: HigherOrderPreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<HigherOrderSpecialization, crate::CheckedOwnerFailure<FunctionSpecializationFailure>> {
    specialize_once_core(
        owner,
        input,
        selected,
        frozen,
        options,
        control,
        |arguments| {
            crate::specialization::refine_once_checked(
                owner, checked, input, frozen, arguments, control,
            )
        },
        |contract, body| {
            owner
                .prepare_higher_order_checked(input, checked, contract, body, control)
                .map_err(|error| match error {
                    crate::CheckedOwnerFailure::Original(error) => {
                        crate::CheckedOwnerFailure::Original(FunctionSpecializationFailure::from(
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

fn specialize_once_core<
    'a,
    O: PureHigherOrderImplementation + ?Sized,
    E: From<FunctionSpecializationFailure>,
>(
    _owner: &O,
    input: CallEffectInput<'a>,
    selected: Arc<FunctionBindingSelection>,
    _frozen: Option<&CallEffects>,
    options: HigherOrderPreparationOptions,
    control: &dyn PureCompileControl,
    refine: impl FnOnce(
        ScopedExpressionEffects,
    ) -> Result<(crate::RefinedCallEffects<'a>, ScopedExpressionEffects), E>,
    prepare: impl FnOnce(
        Arc<HigherOrderCallContract>,
        Arc<LambdaBodyContract>,
    ) -> Result<Arc<dyn PreparedHigherOrderKernel>, E>,
) -> Result<HigherOrderSpecialization, E> {
    control
        .checkpoint(
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            0,
        )
        .map_err(FunctionSpecializationFailure::Control)?;
    let context = input
        .context
        .expression()
        .ok_or(FunctionSpecializationFailure::InvalidInput(
            "higher-order preparation requires its exact scalar context",
        ))?;
    let ordinary = options
        .arguments
        .for_use(context)
        .map_err(FunctionSpecializationFailure::Effects)?;
    let body_effects = options
        .body_effects
        .for_use(options.body_context)
        .map_err(FunctionSpecializationFailure::Effects)?;
    let children = ScopedExpressionEffects::primitive(context, ordinary.join(body_effects));
    let (receipt, effects) = refine(children)?;
    let contract = Arc::new(
        HigherOrderCallContract::from_refined(input, &receipt, selected, control)
            .map_err(FunctionSpecializationFailure::from)?,
    );
    let body = Arc::new(
        LambdaBodyContract::try_new(
            contract.clone(),
            options.body_context,
            options.body_effects,
            options.capture_types,
            control,
        )
        .map_err(FunctionSpecializationFailure::from)?,
    );
    let prepared = prepare(contract.clone(), body.clone())?;
    if !Arc::ptr_eq(prepared.contract(), &contract) || !Arc::ptr_eq(prepared.body_contract(), &body)
    {
        return Err(FunctionSpecializationFailure::Kernel(internal(
            "higher-order preparation replaced its exact call or body contract",
        ))
        .into());
    }
    control
        .checkpoint(
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
            0,
        )
        .map_err(FunctionSpecializationFailure::Control)?;
    Ok(HigherOrderSpecialization { prepared, effects })
}

/// Ordinary arguments omit the lambda channel but retain their exact order.
/// Captures belong to the expression host and its original lexical frames.
#[derive(Clone, Copy, Debug)]
pub struct HigherOrderCallInput<'contract, 'input> {
    contract: &'contract HigherOrderCallContract,
    selection: Selection<'input>,
    arguments: &'input [EvaluatedArgument<'input>],
    #[cfg(test)]
    captures: &'input [EvaluatedArgument<'input>],
}
impl<'contract, 'input> HigherOrderCallInput<'contract, 'input> {
    pub const fn contract(self) -> &'contract HigherOrderCallContract {
        self.contract
    }
    pub const fn selection(self) -> Selection<'input> {
        self.selection
    }
    pub const fn arguments(self) -> &'input [EvaluatedArgument<'input>] {
        self.arguments
    }
    #[cfg(test)]
    pub const fn captures(self) -> &'input [EvaluatedArgument<'input>] {
        self.captures
    }
}

/// A producer returns ordinary compact values/errors and a sorted subset of
/// required child-error evidence. Every evidence must match an actual error
/// value/diagnostic; it cannot turn a successful parent NULL into an error.
/// Own errors are gated by the parent declaration, child errors by their
/// checked body provenance. The exact owner chooses one diagnostic per parent.
#[derive(Debug)]
pub struct HigherOrderOutput<'input> {
    values: SelectedValues<'input>,
    required_body_errors: Box<[RequiredLambdaBodyError<'input>]>,
}
impl<'input> HigherOrderOutput<'input> {
    pub fn new(
        values: SelectedValues<'input>,
        required_body_errors: Box<[RequiredLambdaBodyError<'input>]>,
    ) -> Self {
        Self {
            values,
            required_body_errors,
        }
    }
    pub const fn values(&self) -> &SelectedValues<'input> {
        &self.values
    }
    pub fn required_body_errors(&self) -> &[RequiredLambdaBodyError<'input>] {
        &self.required_body_errors
    }
}

pub trait HigherOrderKernelInstance: Send {
    fn begin<'call, 'input: 'call>(
        &'call mut self,
        _input: HigherOrderCallInput<'_, 'input>,
        _allocator: &Arc<dyn AggregateStateAllocator>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn HigherOrderCall<'input> + 'call>, KernelFailure> {
        Err(invalid("higher-order kernel has no staged invocation"))
    }
    #[cfg(test)]
    fn evaluate<'input>(
        &mut self,
        _input: HigherOrderCallInput<'_, 'input>,
        _body: &mut LambdaBodyExecutor<'_, 'input>,
        _control: &dyn KernelEvaluationControl,
    ) -> Result<HigherOrderOutput<'input>, KernelFailure> {
        Err(invalid("higher-order kernel has no direct test invocation"))
    }
    fn retained_bytes(&self) -> usize;
}

/// The function can invoke only this already bound body in this exact outer
/// Selection. Input checking and one-shot body frames are mandatory here.
/// An outer failure latches the executor even if private code catches it; the
/// runtime wrapper propagates that failure. Row errors stay a child channel.
///
/// Actual collection offsets, NULL handling, parameter/capture recipes and
/// non-overlap across expansion frames remain the exact owner's responsibility.
/// Expansion bytes and work are authorized by the host before invocation; this
/// object does not issue MEM grants or expose a source-row fallback.
#[cfg(test)]
pub struct LambdaBodyExecutor<'evaluator, 'outer> {
    evaluator: &'evaluator mut dyn BoundLambdaBodyEvaluator,
    contract: Arc<LambdaBodyContract>,
    outer: Selection<'outer>,
    failure: Option<KernelFailure>,
}
#[cfg(test)]
impl<'outer> LambdaBodyExecutor<'_, 'outer> {
    pub fn evaluate<'input>(
        &mut self,
        row_map: LambdaElementRowMap<'input>,
        selection: Selection<'input>,
        parameters: &'input [EvaluatedArgument<'input>],
        captures: &'input [EvaluatedArgument<'input>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<LambdaBodyOutput<'input>, KernelFailure> {
        if self.failure.is_some() {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = (|| {
            control.checkpoint(0)?;
            let mut work = EvaluationCheckpoints::new(control);
            if !row_map
                .outer_selection()
                .same_rows_observed(self.outer, || work.step())?
            {
                return Err(invalid("lambda expansion uses a different outer selection"));
            }
            work.finish()?;
            let input = LambdaBodyInput::try_new(
                &self.contract,
                row_map,
                selection,
                parameters,
                captures,
                control,
            )?;
            LambdaBodyInvocation::try_bind(self.evaluator, input)?.run(control)
        })();
        if let Err(failure) = &result {
            self.failure = Some(failure.clone());
        }
        result
    }
}

/// One use/driver owns one mutable function instance and one separately bound
/// body instance. The host accounts body state/expansion/output under their
/// own lifetime owners, not inside this function state's retained bound.
pub struct HigherOrderEvaluationInstance {
    instance: Box<dyn HigherOrderKernelInstance>,
    prepared: Arc<dyn PreparedHigherOrderKernel>,
    contract: Arc<HigherOrderCallContract>,
    body: Arc<LambdaBodyContract>,
    retained_bound: usize,
    failed: bool,
}
impl HigherOrderEvaluationInstance {
    pub fn instantiate(
        prepared: Arc<dyn PreparedHigherOrderKernel>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        let contract = Arc::clone(prepared.contract());
        let body = Arc::clone(prepared.body_contract());
        if !Arc::ptr_eq(body.call(), &contract) {
            return Err(internal("higher-order body belongs to another call"));
        }
        let retained_bound = prepared.instance_retained_upper_bound();
        size_of::<Self>()
            .checked_add(retained_bound)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let instance = prepared.create_instance(control)?;
        validate_prepared_metadata(&prepared, &contract, &body, retained_bound)?;
        if instance.retained_bytes() > retained_bound {
            return Err(internal(
                "new higher-order instance exceeded its retained bound",
            ));
        }
        let result = Self {
            instance,
            prepared,
            contract,
            body,
            retained_bound,
            failed: false,
        };
        result.validate_metadata()?;
        control.checkpoint(0)?;
        Ok(result)
    }
    pub fn contract(&self) -> &Arc<HigherOrderCallContract> {
        &self.contract
    }
    pub fn body_contract(&self) -> &Arc<LambdaBodyContract> {
        &self.body
    }
    pub fn retained_upper_bound(&self) -> usize {
        size_of::<Self>() + self.retained_bound
    }
    pub fn retained_bytes(&self) -> Result<usize, KernelFailure> {
        let bytes = self.instance.retained_bytes();
        if bytes > self.retained_bound {
            return Err(internal(
                "higher-order instance exceeded its lifetime retained bound",
            ));
        }
        Ok(size_of::<Self>() + bytes)
    }
    #[cfg(test)]
    pub fn evaluate<'input>(
        &mut self,
        selection: Selection<'input>,
        arguments: &'input [EvaluatedArgument<'input>],
        captures: &'input [EvaluatedArgument<'input>],
        body: &mut dyn BoundLambdaBodyEvaluator,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'input>, KernelFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = self.evaluate_once(selection, arguments, captures, body, control);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    #[cfg(test)]
    fn evaluate_once<'input>(
        &mut self,
        selection: Selection<'input>,
        arguments: &'input [EvaluatedArgument<'input>],
        captures: &'input [EvaluatedArgument<'input>],
        body: &mut dyn BoundLambdaBodyEvaluator,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'input>, KernelFailure> {
        control.checkpoint(0)?;
        self.validate_metadata()?;
        self.retained_bytes()?;
        if !Arc::ptr_eq(body.contract(), &self.body) {
            return Err(invalid(
                "higher-order evaluation received a different compiled body",
            ));
        }
        validate_ordinary_inputs(&self.contract, selection, arguments, control)?;
        if captures.len() != self.body.capture_types().len() {
            return Err(invalid(
                "higher-order values differ from their exact input shape",
            ));
        }
        for (argument, ty) in captures.iter().zip(self.body.capture_types()) {
            validate_argument_observed(*argument, selection, ty, control)?;
        }
        if selection.is_empty() {
            return SelectedValues::try_new(
                selection,
                self.contract.result_type().data_type(),
                new_empty_array(self.contract.result_type().data_type()),
                Box::default(),
            )
            .map_err(|_| internal("empty higher-order result violates its exact type"));
        }
        let mut executor = LambdaBodyExecutor {
            evaluator: body,
            contract: self.body.clone(),
            outer: selection,
            failure: None,
        };
        let input = HigherOrderCallInput {
            contract: &self.contract,
            selection,
            arguments,
            captures,
        };
        let result = self.instance.evaluate(input, &mut executor, control);
        // A caught body failure is an originating failure, not a housekeeping
        // post-check. It overrides a private success or unrelated outer error.
        let output = executor.failure.map_or(result, Err)?;
        if !Arc::ptr_eq(executor.evaluator.contract(), &self.body) {
            return Err(internal(
                "bound lambda evaluator changed its immutable contract",
            ));
        }
        self.validate_metadata()?;
        self.retained_bytes()?;
        control.checkpoint(0)?;
        validate_output(
            &self.contract,
            &self.body,
            selection,
            &output,
            None,
            control,
        )?;
        Ok(output.values)
    }
    fn validate_metadata(&self) -> Result<(), KernelFailure> {
        validate_prepared_metadata(
            &self.prepared,
            &self.contract,
            &self.body,
            self.retained_bound,
        )
    }
}

fn validate_ordinary_inputs(
    contract: &HigherOrderCallContract,
    selection: Selection<'_>,
    arguments: &[EvaluatedArgument<'_>],
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    let selected = contract.call().selected();
    if arguments.len() != selected.argument_types.len() - 1 {
        return Err(invalid(
            "higher-order values differ from their exact input shape",
        ));
    }
    let expected = selected
        .argument_types
        .iter()
        .filter_map(|argument| match argument {
            FunctionArgumentType::Value(value) => Some(value),
            FunctionArgumentType::Lambda { .. } => None,
        });
    for (argument, ty) in arguments.iter().zip(expected) {
        validate_argument_observed(*argument, selection, ty, control)?;
    }
    Ok(())
}

fn validate_prepared_metadata(
    prepared: &Arc<dyn PreparedHigherOrderKernel>,
    contract: &Arc<HigherOrderCallContract>,
    body: &Arc<LambdaBodyContract>,
    retained_bound: usize,
) -> Result<(), KernelFailure> {
    if !Arc::ptr_eq(prepared.contract(), contract)
        || !Arc::ptr_eq(prepared.body_contract(), body)
        || prepared.instance_retained_upper_bound() != retained_bound
    {
        return Err(internal(
            "higher-order preparation changed its immutable metadata",
        ));
    }
    Ok(())
}

fn validate_output(
    contract: &HigherOrderCallContract,
    body: &Arc<LambdaBodyContract>,
    selection: Selection<'_>,
    output: &HigherOrderOutput<'_>,
    origin: Option<&HostAggregateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    let values = output.values();
    let mut work = EvaluationCheckpoints::new(control);
    if !values
        .selection()
        .same_rows_observed(selection, || work.step())?
        || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            values.values().data_type(),
            contract.result_type().data_type(),
            || work.step(),
        )?
    {
        return Err(internal(
            "higher-order output has an unrelated selection or type",
        ));
    }
    if output.required_body_errors.len() > values.errors().len() {
        return Err(internal(
            "required lambda evidence exceeds returned row errors",
        ));
    }
    let mut previous = None;
    for evidence in &output.required_body_errors {
        let ordinal = evidence.error().selected_ordinal();
        if !Arc::ptr_eq(evidence.contract(), body)
            || !evidence
                .outer_selection()
                .same_rows_observed(selection, || work.step())?
            || ordinal >= selection.len()
            || previous.is_some_and(|previous| previous >= ordinal)
            || match (&evidence.origin, origin) {
                (Some(source), Some(origin)) => !source.same_allocation(origin),
                (None, None) => false,
                _ => true,
            }
        {
            return Err(internal(
                "required lambda errors have unrelated or duplicate provenance",
            ));
        }
        previous = Some(ordinal);
        work.step()?;
    }
    let mut child_errors = output.required_body_errors.iter().peekable();
    for error in values.errors() {
        match child_errors.peek() {
            Some(evidence) if evidence.error().selected_ordinal() == error.selected_ordinal() => {
                if !same_row_error(evidence.error(), error) {
                    return Err(internal(
                        "required lambda error differs from the parent diagnostic",
                    ));
                }
                child_errors.next();
            }
            _ if contract.call().effects().own_row_error == FunctionIntrinsicRowError::MayRaise => {
            }
            _ => {
                return Err(internal(
                    "never-failing higher-order owner returned its own row error",
                ));
            }
        }
        work.step()?;
    }
    if child_errors.next().is_some() {
        return Err(internal(
            "required lambda evidence has no matching parent row error",
        ));
    }
    if !contract.result_type().nullable() {
        let mut errors = values.errors().iter().peekable();
        for ordinal in 0..values.values().len() {
            if errors
                .peek()
                .is_some_and(|error| error.selected_ordinal() == ordinal)
            {
                errors.next();
                work.step()?;
            } else if logical_is_null(values.values().as_ref(), ordinal, 1, &mut work)? {
                return Err(internal(
                    "non-null higher-order owner returned a successful SQL NULL",
                ));
            }
        }
    }
    work.finish()
}

fn same_row_error(left: &RowDataError, right: &RowDataError) -> bool {
    use novarocks_failure_contract::SharedFailureCause;
    use std::error::Error;
    let left_source = left
        .source()
        .and_then(|source| source.downcast_ref::<SharedFailureCause>());
    let right_source = right
        .source()
        .and_then(|source| source.downcast_ref::<SharedFailureCause>());
    left == right && left_source == right_source
}

#[cfg(test)]
mod tests;
