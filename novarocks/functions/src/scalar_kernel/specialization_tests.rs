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

use super::*;
use crate::{
    FunctionArgument, FunctionBindingRequest, FunctionEffectOwnerError, FunctionOverloadId,
    validate_frozen_call_effects,
};
use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompileControlError, EffectContractError, EvaluationDemand,
    EvaluationRegionId, ExpressionEffects, ExpressionUseId, FunctionEffectDeclaration,
    FunctionFailureBehavior, FunctionInstanceState, FunctionNullBehavior, FunctionVolatility,
    ObservableEffects, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
    SemanticParameterValue,
};
use std::{
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Debug, Default)]
struct Counts {
    refine: AtomicUsize,
    prepare: AtomicUsize,
    instance: AtomicUsize,
    stage: AtomicUsize,
}
#[derive(Default)]
struct Control {
    failure: Option<CompileControlError>,
    positive_only: bool,
    stage: Option<(Arc<Counts>, usize)>,
    work: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        self.work.lock().unwrap().push(units);
        let stage_matches = self
            .stage
            .as_ref()
            .is_none_or(|(counts, stage)| counts.stage.load(Ordering::Relaxed) == *stage);
        if stage_matches
            && (!self.positive_only || units > 0)
            && let Some(failure) = self.failure.clone()
        {
            Err(failure)
        } else {
            Ok(())
        }
    }
}
struct RuntimeControl;
impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("fixture kernel does not wait")
    }
}
struct Owner {
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    declaration: FunctionEffectDeclaration,
    environment: Vec<SemanticParameterRef>,
    parameters: SemanticParameters,
    counts: Arc<Counts>,
    replace_contract: bool,
}
impl Owner {
    fn new(count: usize, control: ArgumentControl) -> Self {
        assert!(matches!(
            control,
            ArgumentControl::Eager | ArgumentControl::TypeOnly
        ));
        let value = FunctionValueType::new(DataType::Int64, true);
        let arguments = vec![
            FunctionArgument::Value {
                value_type: value.clone(),
                constant: None
            };
            count
        ];
        let environment = vec![SemanticParameterRef {
            id: SemanticParameterId::new(7),
            expected_key: SemanticParameterKey::TimeZone,
        }];
        Self {
            id: FunctionId::try_new("fixture/scalar-specialization/exact-owner").unwrap(),
            selected: Arc::new(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("fixture/scalar-specialization/exact-i64")
                    .unwrap(),
                argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                    arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Box<[_]>>(),
                ),
                result_type: FunctionResultType::Scalar(value),
                aggregate: None,
            }),
            uses: (0..count)
                .map(|ordinal| {
                    if control == ArgumentControl::TypeOnly {
                        None
                    } else {
                        Some(ExpressionUseId::new(100 + ordinal as u32))
                    }
                })
                .collect(),
            arguments,
            declaration: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Stable,
                own_row_error: FunctionIntrinsicRowError::NoRowError,
                failure_behavior: FunctionFailureBehavior::ReturnsNull,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: control,
                instance_state: FunctionInstanceState::None,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::from([SemanticParameterKey::TimeZone]),
            },
            environment,
            parameters: SemanticParameters::try_new([
                (
                    SemanticParameterId::new(7),
                    SemanticParameterValue::TimeZone("UTC".into()),
                ),
                (
                    SemanticParameterId::new(8),
                    SemanticParameterValue::TimeZone("Asia/Shanghai".into()),
                ),
            ])
            .unwrap(),
            counts: Arc::new(Counts::default()),
            replace_contract: false,
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(9),
                region: EvaluationRegionId::new(11),
                demand: EvaluationDemand::Value,
            },
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.id,
            kind: FunctionKind::Scalar,
            selected: self.selected.as_ref(),
            request: FunctionBindingRequest {
                expected_result_type: None,
                arguments: &self.arguments,
                logical_argument_count: self.arguments.len(),
            },
            environment: &self.environment,
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        }
    }
    fn frozen(&self) -> CallEffects {
        CallEffects {
            value_stability: self.declaration.value_stability,
            own_row_error: self.declaration.own_row_error,
            failure_behavior: self.declaration.failure_behavior,
            null_behavior: self.declaration.null_behavior,
            argument_control: self.declaration.argument_control,
            instance_state: self.declaration.instance_state,
            observable_effects: self.declaration.observable_effects,
            environment: self.environment.clone().into_boxed_slice(),
            proof_scope: self.input().proof_scope,
        }
    }
    fn reset(&self) {
        self.counts.refine.store(0, Ordering::Relaxed);
        self.counts.prepare.store(0, Ordering::Relaxed);
        self.counts.stage.store(0, Ordering::Relaxed);
    }
    fn assert_calls(&self, refine: usize, prepare: usize) {
        assert_eq!(self.counts.refine.load(Ordering::Relaxed), refine);
        assert_eq!(self.counts.prepare.load(Ordering::Relaxed), prepare);
        assert_eq!(self.counts.instance.load(Ordering::Relaxed), 0);
    }
}
impl FunctionBindingResolver for Owner {
    fn resolve(
        &self,
        _: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        panic!("exact specialization must not reselect an overload")
    }
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        if !std::ptr::eq(selected, self.selected.as_ref())
            || request.logical_argument_count != self.arguments.len()
            || request.arguments.len() != self.arguments.len()
        {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(())
    }
}
impl FunctionEffectOwner for Owner {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        id: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if id != &self.id || !std::ptr::eq(selected, self.selected.as_ref()) {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(&self.declaration)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.counts.refine.fetch_add(1, Ordering::Relaxed);
        self.counts.stage.store(1, Ordering::Relaxed);
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        self.validate_selected(input.selected, input.request, control)?;
        if input.function_id != &self.id
            || input.kind != FunctionKind::Scalar
            || input.environment != self.environment
            || !std::ptr::eq(input.parameters, &self.parameters)
        {
            return Err(FunctionBindingError::UnknownFunction.into());
        }
        for (argument, expected) in input.request.arguments.iter().zip(&self.arguments) {
            if !argument.equals_observed(expected, CompilePhase::FunctionSpecialization, control)? {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            work.step().map_err(FunctionEffectOwnerError::Control)?;
        }
        for reference in input.environment {
            input.parameters.require(*reference).map_err(|_| {
                FunctionEffectOwnerError::Owner(FunctionBindingError::UnknownFunction)
            })?;
            work.step().map_err(FunctionEffectOwnerError::Control)?;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        self.counts.stage.store(0, Ordering::Relaxed);
        Ok(self.frozen())
    }
}
impl PureScalarImplementation for Owner {
    fn prepare_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        self.counts.prepare.fetch_add(1, Ordering::Relaxed);
        self.counts.stage.store(2, Ordering::Relaxed);
        assert_eq!(contract.context(), input.context);
        assert!(std::ptr::eq(contract.selected(), input.selected));
        assert_eq!(contract.effects(), &self.frozen());
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(crate::kernel_control::compile_failure)?;
        for _ in input.request.arguments {
            work.step()
                .map_err(crate::kernel_control::compile_failure)?;
        }
        work.finish()
            .map_err(crate::kernel_control::compile_failure)?;
        self.counts.stage.store(0, Ordering::Relaxed);
        let contract = if self.replace_contract {
            Arc::new((*contract).clone())
        } else {
            contract
        };
        Ok(Arc::new(Prepared {
            contract,
            counts: self.counts.clone(),
        }))
    }
}
#[derive(Debug)]
struct Prepared {
    contract: Arc<ScalarCallContract>,
    counts: Arc<Counts>,
}
struct Instance;
impl PreparedScalarKernel for Prepared {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }
    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<Instance>()
    }
    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        self.counts.instance.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(Instance))
    }
}
impl ScalarKernelInstance for Instance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        _: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1; input.selection().len()]));
        SelectedValues::try_new(input.selection(), &DataType::Int64, values, Box::default())
            .map_err(|_| internal("fixture selected output differs"))
    }
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
#[test]
fn exact_owner_is_refined_and_prepared_once_without_reselection_or_instance_creation() {
    let owner = Owner::new(1, ArgumentControl::Eager);
    let input = owner.input();
    let result = specialize_scalar(
        &owner,
        input,
        owner.selected.clone(),
        ScopedExpressionEffects::pure_value(input.context),
        &Control::default(),
    )
    .unwrap();
    owner.assert_calls(1, 1);
    assert!(Arc::ptr_eq(
        result.prepared().contract().call().selected_owner(),
        &owner.selected
    ));
    assert_eq!(result.prepared().contract().effects(), &owner.frozen());
    let expected = result.prepared().clone();
    assert!(Arc::ptr_eq(&result.into_prepared(), &expected));
}

fn argument_summary(input: CallEffectInput<'_>) -> ScopedExpressionEffects {
    let child = ScopedExpressionEffects::primitive(
        ExpressionEffectContext {
            use_id: match input.argument_uses {
                crate::CallArgumentUses::SelectedChannels(uses) => uses[0].unwrap(),
                crate::CallArgumentUses::AggregateMerge { .. } => {
                    panic!("scalar fixture has aggregate merge channels")
                }
                crate::CallArgumentUses::TemporalSources { .. }
                | crate::CallArgumentUses::RegexpCountPattern { .. }
                | crate::CallArgumentUses::ToBase64Bytes { .. } => {
                    panic!("eager scalar fixture has temporal source channels")
                }
            },
            ..input.context
        },
        ExpressionEffects {
            may_raise_invocation_data: false,
            value_stability: FunctionVolatility::Volatile,
            may_raise_row_error: true,
            has_instance_state: true,
            observable_effects: ObservableEffects {
                rng_sampling: true,
                warnings: true,
                controlled_wait: true,
            },
        },
    );
    ScopedExpressionEffects::pure_value(input.context)
        .join_same_region(child)
        .unwrap()
}
#[test]
fn fresh_and_frozen_entries_preserve_equivalent_complete_child_and_call_effects_once() {
    let owner = Owner::new(1, ArgumentControl::Eager);
    let input = owner.input();
    let frozen = owner.frozen();
    let arguments = argument_summary(input);
    let fresh = specialize_scalar(
        &owner,
        input,
        owner.selected.clone(),
        arguments,
        &Control::default(),
    )
    .unwrap();
    owner.assert_calls(1, 1);
    owner.reset();
    let checked = specialize_frozen_scalar(
        &owner,
        input,
        owner.selected.clone(),
        &frozen,
        arguments,
        &Control::default(),
    )
    .unwrap();
    owner.assert_calls(1, 1);
    assert_eq!(fresh.effects(), checked.effects());
    assert_eq!(fresh.prepared().contract(), checked.prepared().contract());
    let effects = checked.effects().for_use(input.context).unwrap();
    assert_eq!(effects.value_stability, FunctionVolatility::Volatile);
    assert!(effects.may_raise_row_error);
    assert!(effects.has_instance_state);
    assert_eq!(
        effects.observable_effects,
        ObservableEffects {
            rng_sampling: true,
            warnings: true,
            controlled_wait: true
        }
    );
    assert!(!effects.permits_boolean_reordering());
    assert!(!effects.permits_earlier_evaluation());
    assert_eq!(
        checked.prepared().contract().effects().own_row_error,
        FunctionIntrinsicRowError::NoRowError
    );
    assert_eq!(
        checked.prepared().contract().effects().failure_behavior,
        FunctionFailureBehavior::ReturnsNull
    );
    assert_eq!(
        checked.prepared().contract().parameters().entries().len(),
        1
    );
    assert_eq!(
        checked
            .prepared()
            .contract()
            .parameters()
            .require(owner.environment[0])
            .unwrap(),
        &SemanticParameterValue::TimeZone("UTC".into())
    );
}
#[test]
fn every_frozen_effect_field_and_environment_reference_is_compared_before_preparation() {
    type Mutation = (&'static str, fn(&mut CallEffects));
    let mutations: &[Mutation] = &[
        ("stability", |facts| {
            facts.value_stability = FunctionVolatility::Immutable
        }),
        ("own row error", |facts| {
            facts.own_row_error = FunctionIntrinsicRowError::MayRaise
        }),
        ("failure behavior", |facts| {
            facts.failure_behavior = FunctionFailureBehavior::Propagate
        }),
        ("null behavior", |facts| {
            facts.null_behavior = FunctionNullBehavior::Strict
        }),
        ("argument control", |facts| {
            facts.argument_control = ArgumentControl::TypeOnly
        }),
        ("instance state", |facts| {
            facts.instance_state = FunctionInstanceState::ScalarInstance
        }),
        ("rng", |facts| facts.observable_effects.rng_sampling = true),
        ("warnings", |facts| facts.observable_effects.warnings = true),
        ("wait", |facts| {
            facts.observable_effects.controlled_wait = true
        }),
        ("scope", |facts| {
            facts.proof_scope = CallProofScope::Region(EvaluationRegionId::new(11))
        }),
        ("environment missing", |facts| {
            facts.environment = Box::default()
        }),
        ("environment id", |facts| {
            facts.environment[0].id = SemanticParameterId::new(8)
        }),
        ("environment key", |facts| {
            facts.environment[0].expected_key = SemanticParameterKey::AllowThrowException
        }),
        ("environment additional", |facts| {
            facts.environment = Box::from([facts.environment[0], facts.environment[0]])
        }),
    ];
    let owner = Owner::new(1, ArgumentControl::Eager);
    let input = owner.input();
    for (label, mutate) in mutations {
        owner.reset();
        let mut forged = owner.frozen();
        mutate(&mut forged);
        let error = specialize_frozen_scalar(
            &owner,
            input,
            owner.selected.clone(),
            &forged,
            ScopedExpressionEffects::pure_value(input.context),
            &Control::default(),
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                FunctionSpecializationFailure::InvalidInput(
                    "frozen call effects differ from exact local refinement"
                )
            ),
            "{label}: {error:?}"
        );
        owner.assert_calls(1, 0);
    }
}
#[test]
fn child_summary_use_domain_and_demand_mismatches_fail_before_prepare() {
    let owner = Owner::new(1, ArgumentControl::Eager);
    let input = owner.input();
    for context in [
        ExpressionEffectContext {
            use_id: ExpressionUseId::new(10),
            ..input.context
        },
        ExpressionEffectContext {
            region: EvaluationRegionId::new(12),
            ..input.context
        },
        ExpressionEffectContext {
            demand: EvaluationDemand::TruthOnly,
            ..input.context
        },
    ] {
        for frozen in [false, true] {
            owner.reset();
            let arguments = ScopedExpressionEffects::pure_value(context);
            let error = if frozen {
                specialize_frozen_scalar(
                    &owner,
                    input,
                    owner.selected.clone(),
                    &owner.frozen(),
                    arguments,
                    &Control::default(),
                )
                .unwrap_err()
            } else {
                specialize_scalar(
                    &owner,
                    input,
                    owner.selected.clone(),
                    arguments,
                    &Control::default(),
                )
                .unwrap_err()
            };
            assert!(matches!(
                error,
                FunctionSpecializationFailure::Effects(EffectContractError::CallIdentityMismatch)
            ));
            owner.assert_calls(1, 0);
        }
    }
}
#[test]
fn a_value_equal_foreign_signature_arc_is_not_the_exact_selected_owner() {
    let owner = Owner::new(1, ArgumentControl::Eager);
    let input = owner.input();
    let foreign = Arc::new((*owner.selected).clone());
    assert_eq!(foreign, owner.selected);
    assert!(!Arc::ptr_eq(&foreign, &owner.selected));
    for frozen in [false, true] {
        owner.reset();
        let arguments = ScopedExpressionEffects::pure_value(input.context);
        let error = if frozen {
            specialize_frozen_scalar(
                &owner,
                input,
                foreign.clone(),
                &owner.frozen(),
                arguments,
                &Control::default(),
            )
            .unwrap_err()
        } else {
            specialize_scalar(
                &owner,
                input,
                foreign.clone(),
                arguments,
                &Control::default(),
            )
            .unwrap_err()
        };
        assert!(matches!(
            error,
            FunctionSpecializationFailure::Kernel(KernelFailure::InvalidProgram(_))
        ));
        owner.assert_calls(1, 0);
    }
}
#[test]
fn preparation_cannot_replace_even_a_value_equal_immutable_call_contract() {
    let mut owner = Owner::new(1, ArgumentControl::Eager);
    owner.replace_contract = true;
    let input = owner.input();
    for frozen in [false, true] {
        owner.reset();
        let arguments = ScopedExpressionEffects::pure_value(input.context);
        let error = if frozen {
            specialize_frozen_scalar(
                &owner,
                input,
                owner.selected.clone(),
                &owner.frozen(),
                arguments,
                &Control::default(),
            )
            .unwrap_err()
        } else {
            specialize_scalar(
                &owner,
                input,
                owner.selected.clone(),
                arguments,
                &Control::default(),
            )
            .unwrap_err()
        };
        assert!(matches!(
            error,
            FunctionSpecializationFailure::Kernel(KernelFailure::Internal(_))
        ));
        owner.assert_calls(1, 1);
    }
}
#[test]
fn frozen_validation_returns_one_borrowed_receipt_for_composition_and_contract_creation() {
    let owner = Owner::new(1, ArgumentControl::Eager);
    let input = owner.input();
    let receipt =
        validate_frozen_call_effects(&owner, input, &owner.frozen(), &Control::default()).unwrap();
    let effects = receipt
        .compose_for_use(input, argument_summary(input))
        .unwrap();
    assert!(effects.for_use(input.context).unwrap().may_raise_row_error);
    let contract = ScalarCallContract::from_refined(
        input,
        &receipt,
        owner.selected.clone(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(contract.effects(), receipt.facts());
    owner.assert_calls(1, 0);
}
#[test]
fn type_only_retains_static_signature_without_runtime_argument_uses_or_expression_lookup() {
    let owner = Owner::new(1, ArgumentControl::TypeOnly);
    let input = owner.input();
    assert!(matches!(
        input.argument_uses,
        crate::CallArgumentUses::SelectedChannels([None])
    ));
    let result = specialize_frozen_scalar(
        &owner,
        input,
        owner.selected.clone(),
        &owner.frozen(),
        ScopedExpressionEffects::pure_value(input.context),
        &Control::default(),
    )
    .unwrap();
    owner.assert_calls(1, 1);
    assert_eq!(
        result.prepared().contract().selected().argument_types.len(),
        1
    );
    assert_eq!(result.prepared().contract().value_argument_types().len(), 0);
    let effects = result.effects().for_use(input.context).unwrap();
    assert!(!effects.may_raise_row_error);
    assert!(!effects.has_instance_state);
    assert!(effects.observable_effects.is_empty());
}
#[test]
fn pure_specialization_owns_frozen_facts_after_input_drops_and_creates_state_only_on_instantiation()
{
    let (prepared, counts) = {
        let owner = Owner::new(1, ArgumentControl::Eager);
        let input = owner.input();
        let result = specialize_scalar(
            &owner,
            input,
            owner.selected.clone(),
            ScopedExpressionEffects::pure_value(input.context),
            &Control::default(),
        )
        .unwrap();
        owner.assert_calls(1, 1);
        (result.into_prepared(), owner.counts.clone())
    };
    assert_eq!(counts.instance.load(Ordering::Relaxed), 0);
    assert_eq!(prepared.contract().parameters().entries().len(), 1);
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    assert_eq!(counts.instance.load(Ordering::Relaxed), 1);
    assert!(instance.retained_bytes().unwrap() <= instance.retained_upper_bound());
    let scalar: ArrayRef = Arc::new(Int64Array::from(vec![5]));
    let arguments = [EvaluatedArgument::Scalar(&scalar)];
    let result = instance
        .evaluate(Selection::all(3), &arguments, &RuntimeControl)
        .unwrap();
    assert_eq!(result.values().len(), 3);
    assert!(result.errors().is_empty());
    assert_eq!(counts.refine.load(Ordering::Relaxed), 1);
    assert_eq!(counts.prepare.load(Ordering::Relaxed), 1);
    // This fixture supplies only evaluated values and the owned contract; no
    // ExprArena, compile input borrow or mutable instance crosses preparation.
    // It is not a production MEM authorization or actual implementation proof.
}
#[test]
fn entry_and_bounded_wrapper_or_owner_refinement_failures_keep_typed_compile_categories() {
    let owner = Owner::new(300, ArgumentControl::Eager);
    let input = owner.input();
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for frozen in [false, true] {
            for stage in [None, Some(1)] {
                owner.reset();
                let control = Control {
                    failure: Some(failure.clone()),
                    positive_only: stage.is_some(),
                    stage: stage.map(|stage| (owner.counts.clone(), stage)),
                    ..Default::default()
                };
                let arguments = ScopedExpressionEffects::pure_value(input.context);
                let error = if frozen {
                    specialize_frozen_scalar(
                        &owner,
                        input,
                        owner.selected.clone(),
                        &owner.frozen(),
                        arguments,
                        &control,
                    )
                    .unwrap_err()
                } else {
                    specialize_scalar(&owner, input, owner.selected.clone(), arguments, &control)
                        .unwrap_err()
                };
                assert!(
                    matches!(error,FunctionSpecializationFailure::Control(actual) if actual==failure)
                );
                owner.assert_calls(usize::from(stage.is_some()), 0);
                let work = control.work.lock().unwrap();
                if stage.is_some() {
                    assert!(work.contains(&256));
                } else {
                    assert_eq!(*work, [0]);
                }
            }
            owner.reset();
            let control = Control {
                failure: Some(failure.clone()),
                positive_only: true,
                ..Default::default()
            };
            let arguments = ScopedExpressionEffects::pure_value(input.context);
            let error = if frozen {
                specialize_frozen_scalar(
                    &owner,
                    input,
                    owner.selected.clone(),
                    &owner.frozen(),
                    arguments,
                    &control,
                )
                .unwrap_err()
            } else {
                specialize_scalar(&owner, input, owner.selected.clone(), arguments, &control)
                    .unwrap_err()
            };
            assert!(
                matches!(error,FunctionSpecializationFailure::Control(actual) if actual==failure)
            );
            owner.assert_calls(0, 0);
            assert_eq!(*control.work.lock().unwrap(), [0, 0, 256]);
        }
    }
}
#[test]
fn bounded_prepare_control_failures_are_outer_kernel_failures_and_allocate_no_instance() {
    let owner = Owner::new(300, ArgumentControl::Eager);
    let input = owner.input();
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for positive_only in [false, true] {
            owner.reset();
            let control = Control {
                failure: Some(failure.clone()),
                positive_only,
                stage: Some((owner.counts.clone(), 2)),
                ..Default::default()
            };
            let error = specialize_scalar(
                &owner,
                input,
                owner.selected.clone(),
                ScopedExpressionEffects::pure_value(input.context),
                &control,
            )
            .unwrap_err();
            assert!(
                matches!(error,FunctionSpecializationFailure::Control(actual) if actual==failure)
            );
            owner.assert_calls(1, 1);
            assert!(
                control
                    .work
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|units| *units <= 256)
            );
        }
    }
}
