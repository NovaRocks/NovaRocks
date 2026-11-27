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

//! The installed SLEEP binding, controlled wait and preparation share one owner.
//! Arrow representability checks do not replace host memory admission.

use std::{sync::Arc, time::Duration};

use arrow_array::{Array, ArrayRef, Int64Array, builder::BooleanBuilder};
use arrow_schema::DataType;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl, ValueLogicalType,
};

use super::catalogue::BuiltinScalarResolver;
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    CallEffectInput, FunctionBindingDeclaration, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingResolver, FunctionBindingSelection, FunctionCatalogError, FunctionDefinition,
    FunctionEffectOwner, FunctionEffectOwnerError, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionVisibility, FunctionVolatility,
    KernelEvaluationControl, KernelFailure, PreparedScalarKernel, PureFunctionMetadataOwner,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, PureScalarImplementation,
    ScalarCallContract, ScalarCallInput, ScalarKernelInstance, SelectedValues,
};

/// Waiting is observable even though every non-NULL result is true. Volatility
/// prevents frontend folding; only the runtime control may perform a wait.
pub(super) fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Volatile,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::Strict,
        argument_control: ArgumentControl::OrderedNull,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects {
            rng_sampling: false,
            warnings: false,
            controlled_wait: true,
        },
        environment_dependencies: Box::new([]),
    }
}

pub(super) fn definition(
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(SleepOwner::new(declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar("sleep", FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin sleep pure owner",
            value: error.to_string().into(),
        },
    )
}

struct SleepOwner {
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl SleepOwner {
    fn new(
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let function = "builtin.scalar/sleep/v1";
        let expected = effects();
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 1
            || declaration.overloads().iter().any(|overload| {
                overload.identity.as_str() != "builtin.scalar/sleep/(i64)->bool;strict;legacy"
                    || overload.aggregate.is_some()
                    || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin sleep pure declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
        let implementation = PureImplementationId::try_new("builtin.scalar/sleep/selected-v1")?;
        let implementations = declaration
            .overloads()
            .iter()
            .map(|overload| PureImplementationDeclaration {
                overload: overload.identity.clone(),
                implementation: implementation.clone(),
                abi: PureKernelAbi::ScalarV1,
            })
            .collect();
        Ok(Self {
            resolver: Arc::new(resolver),
            declaration,
            implementations,
        })
    }

    fn call_effects(&self, scope: CallProofScope) -> CallEffects {
        let base = effects();
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::new([]),
            proof_scope: scope,
        }
    }
}

impl FunctionBindingResolver for SleepOwner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver.resolve(request, control)
    }

    fn select_at_overload_observed(
        &self,
        overload: &crate::FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver
            .select_at_overload_observed(overload, request, control)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.resolver.validate_selected(selected, request, control)
    }
}

impl PureFunctionMetadataOwner for SleepOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> crate::FunctionBindingResolverOwner {
        crate::FunctionBindingResolverOwner::fixed_scalar(Arc::clone(&self.resolver))
    }

    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}

impl FunctionEffectOwner for SleepOwner {
    type Error = FunctionBindingError;

    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if function != self.declaration.function_id() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        self.declaration.effect_declaration(&selected.overload)
    }

    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.refine_selected_core(input, control, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }
    fn validate_and_refine_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, crate::CheckedOwnerFailure<FunctionEffectOwnerError<Self::Error>>>
    {
        self.refine_selected_core(input, control, || {
            checked.require_fixed_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

impl PureScalarImplementation for SleepOwner {
    fn prepare_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        self.prepare_selected_core(input, contract, control, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }
    fn prepare_scalar_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, crate::CheckedOwnerFailure<KernelFailure>> {
        self.prepare_selected_core(input, contract, control, || {
            checked.require_fixed_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

#[derive(Debug)]
struct PreparedSleep {
    contract: Arc<ScalarCallContract>,
}
impl PreparedScalarKernel for PreparedSleep {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<SleepInstance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(SleepInstance))
    }
}

struct SleepInstance;
impl ScalarKernelInstance for SleepInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        evaluate_sleep(input, control)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl SleepOwner {
    fn refine_selected_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        work.step().map_err(FunctionEffectOwnerError::Control)?;
        if input.function_id != self.declaration.function_id() || input.kind != FunctionKind::Scalar
        {
            return Err(FunctionBindingError::UnknownFunction.into());
        }
        if !input.environment.is_empty()
            || !matches!(input.proof_scope, CallProofScope::Unconditional)
                && input.proof_scope != input.context.proof_scope()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "sleep has no environment dependencies and requires an exact proof scope".into(),
            )
            .into());
        }
        work.flush().map_err(FunctionEffectOwnerError::Control)?;
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
            other => FunctionEffectOwnerError::Owner(other),
        })?;
        let result = self.call_effects(input.proof_scope);
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        Ok(result)
    }
    fn prepare_selected_core(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        work.step().map_err(compile_failure)?;
        if input.function_id != self.declaration.function_id()
            || input.kind != FunctionKind::Scalar
            || contract.function_id() != input.function_id
            || !std::ptr::eq(contract.selected(), input.selected)
            || novarocks_type_contract::CallEffectContext::from(contract.context()) != input.context
            || contract.decimal_overflow_policy() != input.decimal_overflow_policy
            || contract.effects() != &self.call_effects(input.proof_scope)
            || !input.environment.is_empty()
        {
            return Err(invalid(
                "sleep preparation differs from its exact checked call",
            ));
        }
        work.flush().map_err(compile_failure)?;
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => compile_failure(error),
            _ => invalid("sleep preparation has a stale selected binding"),
        })?;
        work.finish().map_err(compile_failure)?;
        // The prepared object retains the same canonical contract. Its body is
        // a static pure implementation and needs no live resolver or authority.
        Ok(Arc::new(PreparedSleep { contract }))
    }
}

/// The compact output preserves the original selected-row order. A scalar
/// argument still waits once per demanded row, rather than once per array.
fn evaluate_sleep<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    use crate::kernel_control::internal;
    use crate::kernel_input::EvaluationCheckpoints;
    let observed = crate::KernelControlObservation::new(control);
    observed.checkpoint(0)?;
    let ([crate::FunctionArgumentType::Value(source)], [argument]) = (
        input.contract().selected().argument_types.as_ref(),
        input.arguments(),
    ) else {
        return Err(invalid("sleep requires exactly one checked value argument"));
    };
    let target = input.contract().result_type();
    if source.logical_type() != ValueLogicalType::Physical
        || source.data_type() != &DataType::Int64
        || target.logical_type() != ValueLogicalType::Physical
        || target.data_type() != &DataType::Boolean
        || !target.nullable()
    {
        return Err(invalid("sleep differs from its exact installed profile"));
    }
    let values = argument
        .array()
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| internal("sleep selected Int64 carrier cannot be downcast"))?;
    let selection = input.selection();
    // BooleanBuilder retains at most one value and one validity bitmap.
    let bytes = selection
        .len()
        .checked_add(7)
        .and_then(|bits| (bits / 8).checked_mul(2))
        .ok_or(KernelFailure::ResourceExhausted)?;
    isize::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    let mut work = EvaluationCheckpoints::new(&observed);
    let mut output = BooleanBuilder::with_capacity(selection.len());
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let row = argument.value_row(ordinal, batch_row);
        if row >= values.len() {
            return Err(internal(
                "sleep selected row is outside its checked carrier",
            ));
        }
        if values.is_null(row) {
            if !source.nullable() {
                return Err(internal(
                    "sleep non-null argument contains selected SQL NULL",
                ));
            }
            output.append_null();
        } else {
            // A zero-second call must also observe an already stopped fragment.
            work.flush()?;
            observed.wait(Duration::from_secs(values.value(row).max(0) as u64))?;
            output.append_value(true);
        }
    }
    work.flush()?;
    let values = Arc::new(output.finish()) as ArrayRef;
    work.finish()?;
    SelectedValues::try_new(selection, target.data_type(), values, Box::default())
        .map_err(|_| internal("sleep compact output violates its selected contract"))
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FunctionArgument, FunctionArgumentType, FunctionResultType, FunctionSpecializationFailure,
        FunctionValueType, ScopedExpressionEffects, specialize_frozen_scalar, specialize_scalar,
    };
    use arrow_schema::DataType;
    use novarocks_type_contract::{
        CompileControlError, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
        ExpressionEffectContext, ExpressionUseId, SemanticParameters,
    };

    fn owner() -> SleepOwner {
        let (_, signatures) = super::super::registry::builtin_scalar_declarations()
            .into_iter()
            .find(|(name, _)| name == "sleep")
            .unwrap();
        let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
            "sleep",
            &signatures,
            FunctionKind::Scalar,
        )
        .unwrap();
        SleepOwner::new(declaration, resolver).unwrap()
    }
    fn context() -> ExpressionEffectContext {
        ExpressionEffectContext {
            use_id: ExpressionUseId::new(41),
            region: EvaluationRegionId::new(7),
            demand: EvaluationDemand::Value,
        }
    }
    fn argument(source: FunctionValueType) -> FunctionArgument {
        FunctionArgument::Value {
            value_type: source,
            constant: None,
        }
    }
    fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            expected_result_type: None,
            arguments,
            logical_argument_count: arguments.len(),
        }
    }
    fn input<'a>(
        owner: &'a SleepOwner,
        selected: &'a FunctionBindingSelection,
        arguments: &'a [FunctionArgument],
        parameters: &'a SemanticParameters,
        uses: &'a [Option<ExpressionUseId>],
    ) -> CallEffectInput<'a> {
        CallEffectInput {
            context: (context()).into(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(uses),
            function_id: owner.declaration.function_id(),
            kind: FunctionKind::Scalar,
            selected,
            request: request(arguments),
            environment: &[],
            parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        }
    }
    pub(super) fn prepared_for_test_with_policy(
        source: &FunctionValueType,
        policy: DecimalOverflowPolicy,
    ) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
        let owner = owner();
        let arguments = [argument(source.clone())];
        let selected = Arc::new(owner.resolve(request(&arguments), crate::binding_test_control())?);
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
        let mut exact = input(&owner, &selected, &arguments, &parameters, &uses);
        exact.decimal_overflow_policy = policy;
        specialize_scalar(
            &owner,
            exact,
            selected.clone(),
            ScopedExpressionEffects::pure_value(context()),
            crate::binding_test_control(),
        )
        .map(|specialization| specialization.into_prepared())
    }

    #[test]
    fn sleep_fresh_and_frozen_keep_exact_selected_identity_and_observable_wait() {
        let owner = owner();
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
        for nullable in [false, true] {
            let source = FunctionValueType::new(DataType::Int64, nullable);
            let arguments = [argument(source.clone())];
            let selected = Arc::new(
                owner
                    .resolve(request(&arguments), crate::binding_test_control())
                    .unwrap(),
            );
            assert_eq!(
                selected.overload.as_str(),
                "builtin.scalar/sleep/(i64)->bool;strict;legacy"
            );
            assert_eq!(
                selected.argument_types.as_ref(),
                &[FunctionArgumentType::Value(source)]
            );
            assert_eq!(
                selected.result_type,
                FunctionResultType::Scalar(FunctionValueType::new(DataType::Boolean, true))
            );
            let exact = input(&owner, &selected, &arguments, &parameters, &uses);
            let fresh = specialize_scalar(
                &owner,
                exact,
                selected.clone(),
                ScopedExpressionEffects::pure_value(context()),
                crate::binding_test_control(),
            )
            .unwrap();
            let contract = fresh.prepared().contract().clone();
            assert!(std::ptr::eq(contract.selected(), selected.as_ref()));
            let effects = contract.effects();
            assert_eq!(effects.value_stability, FunctionVolatility::Volatile);
            assert_eq!(effects.null_behavior, FunctionNullBehavior::Strict);
            assert_eq!(effects.own_row_error, FunctionIntrinsicRowError::NoRowError);
            assert_eq!(effects.failure_behavior, FunctionFailureBehavior::Propagate);
            assert_eq!(effects.argument_control, ArgumentControl::OrderedNull);
            assert_eq!(effects.instance_state, FunctionInstanceState::None);
            assert_eq!(
                effects.observable_effects,
                ObservableEffects {
                    rng_sampling: false,
                    warnings: false,
                    controlled_wait: true,
                }
            );
            assert!(effects.environment.is_empty());
            let frozen = specialize_frozen_scalar(
                &owner,
                exact,
                selected.clone(),
                effects,
                ScopedExpressionEffects::pure_value(context()),
                crate::binding_test_control(),
            )
            .unwrap();
            assert!(std::ptr::eq(
                frozen.prepared().contract().selected(),
                selected.as_ref()
            ));
            assert_eq!(frozen.prepared().contract().effects(), effects);
            assert_eq!(frozen.prepared().instance_retained_upper_bound(), 0);
            let mut altered = effects.clone();
            altered.observable_effects.controlled_wait = false;
            assert!(matches!(
                specialize_frozen_scalar(
                    &owner,
                    exact,
                    selected.clone(),
                    &altered,
                    ScopedExpressionEffects::pure_value(context()),
                    crate::binding_test_control()
                ),
                Err(FunctionSpecializationFailure::InvalidInput(_))
            ));
        }
    }

    #[derive(Default)]
    struct WaitControl {
        waits: std::sync::Mutex<Vec<Duration>>,
        refuse_at: Option<(usize, KernelFailure)>,
        entry_failure: Option<KernelFailure>,
    }
    impl KernelEvaluationControl for WaitControl {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            self.entry_failure.clone().map_or(Ok(()), Err)
        }
        fn wait(&self, duration: Duration) -> Result<(), KernelFailure> {
            let mut waits = self.waits.lock().unwrap();
            let ordinal = waits.len();
            waits.push(duration);
            if let Some((at, failure)) = &self.refuse_at
                && *at == ordinal
            {
                return Err(failure.clone());
            }
            Ok(())
        }
    }
    fn instance(nullable: bool) -> crate::ScalarEvaluationInstance {
        crate::ScalarEvaluationInstance::instantiate(
            prepared_for_test_with_policy(
                &FunctionValueType::new(DataType::Int64, nullable),
                DecimalOverflowPolicy::ReportError,
            )
            .unwrap(),
        )
        .unwrap()
    }
    fn output(values: &ArrayRef) -> Vec<Option<bool>> {
        values
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap()
            .iter()
            .collect()
    }

    #[test]
    fn sleep_null_negative_zero_and_positive_rows_keep_original_waits_without_real_sleep() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(-7), Some(0), Some(3)]));
        let args = [crate::EvaluatedArgument::Column(&values)];
        let control = WaitControl::default();
        let output = instance(true)
            .evaluate(crate::Selection::all(4), &args, &control)
            .unwrap();
        assert_eq!(
            self::output(output.values()),
            vec![None, Some(true), Some(true), Some(true)]
        );
        assert_eq!(
            *control.waits.lock().unwrap(),
            vec![Duration::ZERO, Duration::ZERO, Duration::from_secs(3)]
        );
    }

    #[test]
    fn sleep_sparse_and_scalar_arguments_wait_only_in_selected_order() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![90, 2, 80, 1]));
        let args = [crate::EvaluatedArgument::Column(&values)];
        let control = WaitControl::default();
        let rows = [1, 3];
        let selection = crate::Selection::try_sparse(4, &rows).unwrap();
        let output = instance(false)
            .evaluate(selection, &args, &control)
            .unwrap();
        assert_eq!(self::output(output.values()), vec![Some(true), Some(true)]);
        assert_eq!(
            *control.waits.lock().unwrap(),
            vec![Duration::from_secs(2), Duration::from_secs(1)]
        );

        let scalar: ArrayRef = Arc::new(Int64Array::from(vec![2]));
        let args = [crate::EvaluatedArgument::Scalar(&scalar)];
        let control = WaitControl::default();
        let output = instance(false)
            .evaluate(selection, &args, &control)
            .unwrap();
        assert_eq!(self::output(output.values()), vec![Some(true), Some(true)]);
        assert_eq!(
            *control.waits.lock().unwrap(),
            vec![Duration::from_secs(2); 2]
        );
    }

    #[test]
    fn sleep_empty_and_null_scalar_domains_never_wait() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>]));
        let args = [crate::EvaluatedArgument::Scalar(&values)];
        for rows in [0, 3] {
            let control = WaitControl::default();
            let output = instance(true)
                .evaluate(crate::Selection::all(rows), &args, &control)
                .unwrap();
            assert_eq!(self::output(output.values()), vec![None; rows]);
            assert!(control.waits.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn sleep_wait_refusal_keeps_original_cause_and_stops_later_rows() {
        for failure in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::Operational(crate::KernelDiagnostic::new("original wait refusal")),
        ] {
            let values: ArrayRef = Arc::new(Int64Array::from(vec![0, 4, 8]));
            let args = [crate::EvaluatedArgument::Column(&values)];
            let control = WaitControl {
                refuse_at: Some((1, failure.clone())),
                ..Default::default()
            };
            let mut instance = instance(false);
            assert!(
                matches!(instance.evaluate(crate::Selection::all(3), &args, &control),
                Err(actual) if actual == failure)
            );
            assert_eq!(
                *control.waits.lock().unwrap(),
                vec![Duration::ZERO, Duration::from_secs(4)]
            );
            assert!(matches!(
                instance.evaluate(crate::Selection::all(3), &args, &control),
                Err(KernelFailure::InstanceFailed)
            ));
            assert_eq!(control.waits.lock().unwrap().len(), 2);
        }
    }

    #[test]
    fn sleep_entry_control_refusal_does_not_wait() {
        for failure in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let values: ArrayRef = Arc::new(Int64Array::from(vec![2]));
            let args = [crate::EvaluatedArgument::Column(&values)];
            let control = WaitControl {
                entry_failure: Some(failure.clone()),
                ..Default::default()
            };
            assert!(
                matches!(instance(false).evaluate(crate::Selection::all(1), &args, &control),
                Err(actual) if actual == failure)
            );
            assert!(control.waits.lock().unwrap().is_empty());
        }
    }

    struct FailCompile(CompileControlError);
    impl PureCompileControl for FailCompile {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0.clone())
        }
    }
    #[test]
    fn sleep_owner_preserves_all_three_compile_control_causes() {
        let owner = owner();
        let arguments = [argument(FunctionValueType::new(DataType::Int64, false))];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
        let exact = input(&owner, &selected, &arguments, &parameters, &uses);
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = FailCompile(cause.clone());
            assert!(matches!(owner.resolve(request(&arguments), &control),
                Err(FunctionBindingError::Control(actual)) if actual == cause));
            assert!(matches!(owner.validate_and_refine(exact, &control),
                Err(FunctionEffectOwnerError::Control(actual)) if actual == cause));
            assert!(matches!(specialize_scalar(&owner, exact, selected.clone(),
                ScopedExpressionEffects::pure_value(context()), &control),
                Err(FunctionSpecializationFailure::Control(actual)) if actual == cause));
        }
    }
}
