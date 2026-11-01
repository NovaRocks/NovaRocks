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
use crate::*;
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    ArgumentControl, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext,
    ExpressionEffects, ExpressionUseId, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility, ObservableEffects,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let ordinal = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(ordinal <= stop, "callback after the original refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == ordinal => Err(cause),
            _ => Ok(()),
        }
    }
}
fn operator() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(0),
        domain: EvaluationDomainId::new(u32::MAX),
        demand: EvaluationDemand::Value,
    }
}
fn state_context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(u32::MAX),
        domain: EvaluationDomainId::new(0),
        demand: EvaluationDemand::Value,
    }
}
fn ty(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}

/// A refiner-only fixture proves the public demand grammar for logical arities
/// that have no installed builtin aggregate in this test. It is not a kernel.
struct Refiner {
    declaration: FunctionEffectDeclaration,
    calls: AtomicUsize,
}
impl Refiner {
    fn new() -> Self {
        Self {
            declaration: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: ArgumentControl::Aggregate,
                instance_state: FunctionInstanceState::AggregateInstance,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::new([]),
            },
            calls: AtomicUsize::new(0),
        }
    }
}
impl FunctionEffectOwner for Refiner {
    type Error = std::convert::Infallible;
    fn declaration(
        &self,
        _: &FunctionId,
        _: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        Ok(&self.declaration)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        _: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            input.request.arguments.len(),
            input.selected.argument_types.len()
        );
        Ok(CallEffects {
            value_stability: self.declaration.value_stability,
            own_row_error: self.declaration.own_row_error,
            failure_behavior: self.declaration.failure_behavior,
            null_behavior: self.declaration.null_behavior,
            argument_control: self.declaration.argument_control,
            instance_state: self.declaration.instance_state,
            observable_effects: self.declaration.observable_effects,
            environment: Box::new([]),
            proof_scope: input.proof_scope,
        })
    }
}
struct StaticInput {
    id: FunctionId,
    args: Vec<FunctionArgument>,
    selected: FunctionBindingSelection,
    parameters: SemanticParameters,
    state: FunctionValueType,
}
impl StaticInput {
    fn new(logical: usize, state: FunctionValueType) -> Self {
        let args = (0..logical)
            .map(|_| FunctionArgument::Value {
                value_type: FunctionValueType::new(DataType::Int32, false),
                constant: None,
            })
            .collect();
        Self {
            id: FunctionId::try_new("test.aggregate/demand").unwrap(),
            args,
            selected: FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("test.aggregate/demand/selected").unwrap(),
                argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                    (0..logical)
                        .map(|_| {
                            FunctionArgumentType::Value(FunctionValueType::new(
                                DataType::Int32,
                                false,
                            ))
                        })
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                ),
                result_type: FunctionResultType::Scalar(ty(false)),
                aggregate: Some(AggregateBindingSelection {
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    intermediate_type: state.clone(),
                    state_format: AggregateStateFormatIdentity::try_new(
                        "test.aggregate/demand/state",
                    )
                    .unwrap(),
                }),
            },
            parameters: SemanticParameters::try_new([]).unwrap(),
            state,
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: operator(),
            argument_uses: CallArgumentUses::AggregateMerge {
                phase: AggregateKernelPhase::Final,
                state_context: state_context(),
                state_input_type: &self.state,
            },
            function_id: &self.id,
            kind: FunctionKind::Aggregate,
            selected: &self.selected,
            request: FunctionBindingRequest {
                arguments: &self.args,
                logical_argument_count: self.args.len(),
                expected_result_type: None,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            proof_scope: CallProofScope::Domain(operator().domain),
        }
    }
}

#[test]
fn merge_static_zero_and_two_channels_have_one_distinct_runtime_state_context() {
    for logical in [0, 2] {
        let data = StaticInput::new(logical, ty(false));
        let owner = Refiner::new();
        let input = data.input();
        let receipt = refine_call_effects(&owner, input, &Control::default()).unwrap();
        assert_eq!(owner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(input.request.arguments.len(), logical);
        assert!(receipt.aggregate_merge_state().is_some());
        let child = ExpressionEffects {
            may_raise_row_error: true,
            ..ExpressionEffects::PURE_VALUE
        };
        let composed = receipt
            .compose_for_use(input, ScopedExpressionEffects::primitive(operator(), child))
            .unwrap();
        assert!(composed.for_use(operator()).unwrap().may_raise_row_error);
        assert!(composed.for_use(state_context()).is_err());
    }
}

#[test]
fn merge_refiner_rejects_phase_kind_context_and_complete_state_domain_before_owner() {
    let data = StaticInput::new(2, ty(true));
    let wrong_domain = FunctionValueType::new(DataType::Utf8, true);
    let narrow = ty(false);
    for (phase, context, state) in [
        (AggregateKernelPhase::Single, state_context(), &data.state),
        (AggregateKernelPhase::Partial, state_context(), &data.state),
        (
            AggregateKernelPhase::Final,
            ExpressionEffectContext {
                demand: EvaluationDemand::TruthOnly,
                ..state_context()
            },
            &data.state,
        ),
        (
            AggregateKernelPhase::Final,
            ExpressionEffectContext {
                use_id: operator().use_id,
                ..state_context()
            },
            &data.state,
        ),
        (
            AggregateKernelPhase::Final,
            ExpressionEffectContext {
                domain: operator().domain,
                ..state_context()
            },
            &data.state,
        ),
        (AggregateKernelPhase::Final, state_context(), &wrong_domain),
        (AggregateKernelPhase::Final, state_context(), &narrow),
    ] {
        let owner = Refiner::new();
        let mut input = data.input();
        input.argument_uses = CallArgumentUses::AggregateMerge {
            phase,
            state_context: context,
            state_input_type: state,
        };
        assert!(refine_call_effects(&owner, input, &Control::default()).is_err());
        assert_eq!(owner.calls.load(Ordering::SeqCst), 0);
    }
    for kind in [
        FunctionKind::Scalar,
        FunctionKind::Window,
        FunctionKind::Table,
    ] {
        let owner = Refiner::new();
        let mut input = data.input();
        input.kind = kind;
        assert!(refine_call_effects(&owner, input, &Control::default()).is_err());
        assert_eq!(owner.calls.load(Ordering::SeqCst), 0);
    }
    let mut wide = StaticInput::new(0, ty(false));
    wide.state = wide
        .state
        .with_nullable(true)
        .expect("valid function nullability declaration");
    assert!(
        refine_call_effects(&Refiner::new(), wide.input(), &Control::default()).is_ok(),
        "actual state root nullability may widen"
    );
}

#[test]
fn merge_receipt_rejects_foreign_equal_state_loan_and_changed_actual_demand() {
    let data = StaticInput::new(2, ty(false));
    let owner = Refiner::new();
    let input = data.input();
    let receipt = refine_call_effects(&owner, input, &Control::default()).unwrap();
    let foreign = data.state.clone();
    let changes = [
        CallArgumentUses::AggregateMerge {
            phase: AggregateKernelPhase::Final,
            state_context: state_context(),
            state_input_type: &foreign,
        },
        CallArgumentUses::AggregateMerge {
            phase: AggregateKernelPhase::Intermediate,
            state_context: state_context(),
            state_input_type: &data.state,
        },
        CallArgumentUses::AggregateMerge {
            phase: AggregateKernelPhase::Final,
            state_context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(1),
                ..state_context()
            },
            state_input_type: &data.state,
        },
        CallArgumentUses::SelectedChannels(&[]),
    ];
    for argument_uses in changes {
        assert_eq!(
            receipt.validate_input(CallEffectInput {
                argument_uses,
                ..input
            }),
            Err(EffectContractError::CallIdentityMismatch)
        );
    }
    receipt.validate_input(input).unwrap();
    let expected = ty(false);
    let foreign_expected = expected.clone();
    let with_expected = CallEffectInput {
        request: FunctionBindingRequest {
            expected_result_type: Some(&expected),
            ..input.request
        },
        ..input
    };
    assert_eq!(
        receipt.validate_input(with_expected),
        Err(EffectContractError::CallIdentityMismatch)
    );
    let receipt = refine_call_effects(&owner, with_expected, &Control::default()).unwrap();
    receipt.validate_input(with_expected).unwrap();
    for changed in [None, Some(&foreign_expected)] {
        assert_eq!(
            receipt.validate_input(CallEffectInput {
                request: FunctionBindingRequest {
                    expected_result_type: changed,
                    ..with_expected.request
                },
                ..with_expected
            }),
            Err(EffectContractError::CallIdentityMismatch)
        );
    }
}

struct CountFixture {
    catalog: PureEngineFunctionCatalog,
    id: FunctionId,
    args: Vec<FunctionArgument>,
    selected: Arc<FunctionBindingSelection>,
    parameters: SemanticParameters,
    state: FunctionValueType,
}
impl CountFixture {
    fn new(logical: usize) -> Self {
        let original = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                original
                    .definition("count", FunctionKind::Aggregate)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        let catalog = builder
            .seal_pure([InstalledPureKernel {
                function: FunctionId::try_new("builtin.aggregate/count/v1").unwrap(),
                kind: FunctionKind::Aggregate,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new("builtin.aggregate/count/derived-v1")
                        .unwrap(),
                    implementation: PureImplementationId::try_new(
                        "builtin.aggregate/count/selected-v1",
                    )
                    .unwrap(),
                    abi: PureKernelAbi::AggregateWindowV1,
                },
                aggregate_state_format: Some(
                    AggregateStateFormatIdentity::try_new("novarocks/count/state-v1").unwrap(),
                ),
            }])
            .unwrap();
        let args: Vec<_> = (0..logical)
            .map(|_| FunctionArgument::Value {
                value_type: FunctionValueType::new(DataType::Int32, true),
                constant: None,
            })
            .collect();
        let bound = catalog
            .metadata()
            .resolve_bound_user(
                "count",
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &args,
                    logical_argument_count: logical,
                    expected_result_type: None,
                },
                &Control::default(),
            )
            .unwrap();
        Self {
            catalog,
            args,
            id: bound.function_id,
            selected: Arc::new(bound.selected),
            parameters: SemanticParameters::try_new([]).unwrap(),
            state: ty(true),
        }
    }
    fn input(&self, phase: AggregateKernelPhase) -> CallEffectInput<'_> {
        CallEffectInput {
            context: operator(),
            argument_uses: CallArgumentUses::AggregateMerge {
                phase,
                state_context: state_context(),
                state_input_type: &self.state,
            },
            function_id: &self.id,
            kind: FunctionKind::Aggregate,
            selected: self.selected.as_ref(),
            request: FunctionBindingRequest {
                arguments: &self.args,
                logical_argument_count: self.args.len(),
                expected_result_type: None,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            proof_scope: CallProofScope::Domain(operator().domain),
        }
    }
    fn options(&self, phase: AggregateKernelPhase) -> PureCallPreparation {
        PureCallPreparation::Aggregate {
            arguments: ScopedExpressionEffects::pure_value(operator()),
            options: AggregatePreparationOptions {
                state_interpretation: None,
                phase,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: Some(self.state.clone()),
            },
        }
    }
    fn prepare(
        &self,
        phase: AggregateKernelPhase,
        control: &dyn PureCompileControl,
    ) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
        self.catalog.prepare_fresh(
            self.input(phase),
            Arc::clone(&self.selected),
            self.options(phase),
            control,
        )
    }
}

#[test]
fn installed_count_merge_fresh_and_frozen_keep_static_signature_and_actual_state() {
    for logical in [0, 1] {
        let fixture = CountFixture::new(logical);
        for phase in [
            AggregateKernelPhase::Intermediate,
            AggregateKernelPhase::Final,
        ] {
            let fresh = fixture.prepare(phase, &Control::default()).unwrap();
            let PreparedPureKernel::Aggregate(handle) = fresh.prepared() else {
                panic!("installed aggregate handle");
            };
            assert_eq!(handle.contract().phase(), phase);
            assert_eq!(handle.contract().call().logical_argument_count(), logical);
            assert_eq!(handle.contract().state_input_type(), Some(&fixture.state));
            assert!(std::ptr::eq(
                handle.contract().call().selected(),
                fixture.selected.as_ref()
            ));
            if logical == 1 {
                assert!(
                    matches!(handle.contract().call().selected().argument_types[0], FunctionArgumentType::Value(ref actual) if actual.data_type() == &DataType::Int32 && actual.nullable())
                );
            }
            let frozen = fixture
                .catalog
                .prepare_frozen(
                    fixture.input(phase),
                    Arc::clone(&fixture.selected),
                    handle.contract().call().effects(),
                    fixture.options(phase),
                    &Control::default(),
                )
                .unwrap();
            assert_eq!(frozen.implementation(), fresh.implementation());
            assert_eq!(
                frozen.call_contract().effects(),
                fresh.call_contract().effects()
            );
        }
    }
}

#[test]
fn installed_merge_options_cannot_replace_phase_or_full_actual_state_type() {
    let fixture = CountFixture::new(0);
    for (phase, state) in [
        (AggregateKernelPhase::Single, Some(fixture.state.clone())),
        (
            AggregateKernelPhase::Intermediate,
            Some(fixture.state.clone()),
        ),
        (AggregateKernelPhase::Final, None),
        (AggregateKernelPhase::Final, Some(ty(false))),
        (
            AggregateKernelPhase::Final,
            Some(FunctionValueType::new(DataType::UInt64, true)),
        ),
    ] {
        let options = PureCallPreparation::Aggregate {
            arguments: ScopedExpressionEffects::pure_value(operator()),
            options: AggregatePreparationOptions {
                state_interpretation: None,
                phase,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: state,
            },
        };
        assert!(matches!(
            fixture.catalog.prepare_fresh(
                fixture.input(AggregateKernelPhase::Final),
                Arc::clone(&fixture.selected),
                options,
                &Control::default()
            ),
            Err(FunctionSpecializationFailure::Kernel(
                KernelFailure::InvalidProgram(_)
            ))
        ));
    }
    let invalid_options: [(bool, Arc<[AggregateOrderKey]>); 2] = [
        (true, Arc::from([])),
        (
            false,
            Arc::from([AggregateOrderKey {
                ascending: true,
                nulls_first: true,
            }]),
        ),
    ];
    for (distinct, order_keys) in invalid_options {
        let options = PureCallPreparation::Aggregate {
            arguments: ScopedExpressionEffects::pure_value(operator()),
            options: AggregatePreparationOptions {
                state_interpretation: None,
                phase: AggregateKernelPhase::Final,
                distinct,
                order_keys,
                state_input_type: Some(fixture.state.clone()),
            },
        };
        assert!(
            fixture
                .catalog
                .prepare_fresh(
                    fixture.input(AggregateKernelPhase::Final),
                    Arc::clone(&fixture.selected),
                    options,
                    &Control::default()
                )
                .is_err()
        );
    }
}

#[test]
fn merge_refiner_success_and_ordinary_failure_preserve_every_control_prefix() {
    let data = StaticInput::new(2, ty(false));
    let wrong = FunctionValueType::new(DataType::Utf8, false);
    let mut control_causes = causes().to_vec();
    control_causes.push(CompileControlError::from_domain(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    ));
    for ordinary in [false, true] {
        let mut input = data.input();
        if ordinary {
            input.argument_uses = CallArgumentUses::AggregateMerge {
                phase: AggregateKernelPhase::Final,
                state_context: state_context(),
                state_input_type: &wrong,
            };
        }
        let baseline = Control::default();
        let refiner = Refiner::new();
        let result = refine_call_effects(&refiner, input, &baseline);
        assert_eq!(result.is_err(), ordinary);
        if ordinary {
            assert!(matches!(
                result,
                Err(CallEffectRefinementError::InvalidInput(_))
            ));
        } else {
            result.unwrap();
        }
        assert_eq!(
            refiner.calls.load(Ordering::SeqCst),
            if ordinary { 0 } else { 1 }
        );
        let expected = baseline.trace.lock().unwrap().clone();
        assert_eq!(
            expected.first(),
            Some(&(CompilePhase::FunctionSpecialization, 0))
        );
        if ordinary {
            // The actual UTF-8/Int64 state-domain author refuses before any
            // flush or refiner delegation. Pending steps are not a callback.
            assert_eq!(expected, [(CompilePhase::FunctionSpecialization, 0)]);
        } else {
            assert!(expected.iter().any(|(_, units)| *units > 0));
        }
        for stop in 0..expected.len() {
            for cause in control_causes.iter().cloned() {
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(refine_call_effects(&Refiner::new(), input, &control), Err(CallEffectRefinementError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), expected[..=stop]);
            }
        }
        if ordinary {
            let late = Control {
                refusal: Some((expected.len(), CompileControlError::Cancelled)),
                ..Control::default()
            };
            let refiner = Refiner::new();
            assert!(matches!(
                refine_call_effects(&refiner, input, &late),
                Err(CallEffectRefinementError::InvalidInput(_))
            ));
            assert_eq!(refiner.calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                *late.trace.lock().unwrap(),
                expected,
                "a late cancellation must not follow the real state-domain refusal"
            );
        }
    }
    let fixture = CountFixture::new(0);
    let baseline = Control::default();
    fixture
        .prepare(AggregateKernelPhase::Final, &baseline)
        .unwrap();
    let expected = baseline.trace.lock().unwrap().clone();
    for stop in 0..expected.len() {
        for cause in control_causes.iter().cloned() {
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            let result = fixture.prepare(AggregateKernelPhase::Final, &control);
            let actual = match result {
                Err(FunctionSpecializationFailure::Control(actual)) => Some(actual),
                Err(FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled)) => {
                    Some(CompileControlError::Cancelled)
                }
                Err(FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded)) => {
                    Some(CompileControlError::DeadlineExceeded)
                }
                Err(FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted)) => {
                    Some(CompileControlError::ResourceExhausted)
                }
                _ => None,
            };
            assert_eq!(actual, Some(cause));
            assert_eq!(*control.trace.lock().unwrap(), expected[..=stop]);
        }
    }
}

#[test]
fn merge_wide_nested_state_uses_real_type_walk_quantum_and_exact_metadata() {
    let fields = (0..320)
        .map(|n| {
            Arc::new(
                Field::new(format!("field{n}"), DataType::Int64, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider-id".into(),
                        n.to_string(),
                    )]),
                ),
            )
        })
        .collect::<Vec<_>>();
    let data = StaticInput::new(
        0,
        FunctionValueType::from_legacy_declared_projection(DataType::Struct(fields.into()), false),
    );
    let baseline = Control::default();
    refine_call_effects(&Refiner::new(), data.input(), &baseline).unwrap();
    let expected = baseline.trace.lock().unwrap().clone();
    let quantum = expected
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("actual borrowed nested type walk");
    for stop in [0, quantum, expected.len() - 1] {
        for cause in causes() {
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(refine_call_effects(&Refiner::new(), data.input(), &control), Err(CallEffectRefinementError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=stop]);
        }
    }
    let mut changed = data.state.clone();
    let DataType::Struct(fields) = changed.data_type() else {
        unreachable!()
    };
    let mut changed_fields = fields.to_vec();
    changed_fields[319] = Arc::new(
        Field::new("field319", DataType::Int64, false).with_metadata(std::collections::HashMap::<
            String,
            String,
        >::from([(
            "provider-id".into(),
            "wrong".into(),
        )])),
    );
    changed = FunctionValueType::from_legacy_declared_projection(
        DataType::Struct(changed_fields.into()),
        changed.nullable(),
    );
    let mut input = data.input();
    input.argument_uses = CallArgumentUses::AggregateMerge {
        phase: AggregateKernelPhase::Final,
        state_context: state_context(),
        state_input_type: &changed,
    };
    // Provider annotations do not participate in typed execution identity.
    refine_call_effects(&Refiner::new(), input, &Control::default()).unwrap();
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    assert!(
        crate::aggregate_call::align_aggregate_merge_state_observed(
            &changed,
            data.state.clone(),
            &mut work
        )
        .is_ok()
    );
    work.finish().unwrap();
    let DataType::Struct(fields) = changed.data_type() else {
        unreachable!()
    };
    let mut changed_fields = fields.to_vec();
    changed_fields[319] = Arc::new(Field::new("different-field319", DataType::Int64, false));
    changed = FunctionValueType::from_legacy_declared_projection(
        DataType::Struct(changed_fields.into()),
        changed.nullable(),
    );
    let mut input = data.input();
    input.argument_uses = CallArgumentUses::AggregateMerge {
        phase: AggregateKernelPhase::Final,
        state_context: state_context(),
        state_input_type: &changed,
    };
    assert!(refine_call_effects(&Refiner::new(), input, &Control::default()).is_err());
}
