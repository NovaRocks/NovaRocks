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
use crate::{
    EngineFunctionCatalogBuilder, FunctionArgumentType, FunctionOverloadId, FunctionResultType,
    FunctionSpecializationFailure, FunctionValueType, InstalledPureKernel, PreparedPureKernel,
    PureCallPreparation, PureEngineFunctionCatalog, PurePreparationSource, ScopedExpressionEffects,
};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CompilePhase, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
    ExpressionEffectContext, ExpressionEffects, ExpressionUseId, SemanticParameterId,
    SemanticParameterKey, SemanticParameterRef, SemanticParameters, ValueLogicalType,
};
use std::sync::Mutex;

const NAMES: [&str; 3] = ["if", "coalesce", "ifnull"];
fn parts(name: &str) -> (FunctionBindingDeclaration, BuiltinScalarResolver) {
    let (_, signatures) = super::super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(candidate, _)| candidate == name)
        .unwrap();
    super::super::catalogue::scalar_definition_parts(name, &signatures, FunctionKind::Scalar)
        .unwrap()
}
fn test_owner(name: &str) -> ControlOwner {
    let (declaration, resolver) = parts(name);
    ControlOwner::new(name, declaration, resolver).unwrap()
}
fn catalog() -> PureEngineFunctionCatalog {
    let mut builder = EngineFunctionCatalogBuilder::new();
    for name in NAMES {
        let (declaration, resolver) = parts(name);
        builder
            .register(definition(name, declaration, resolver).unwrap())
            .unwrap();
    }
    // Independent installation inventory for this actual control-owner subset.
    // This is not the Server manifest or a claim about its remaining families.
    builder
        .seal_pure([
            record(
                "if",
                "builtin.scalar/if/(bool,any<T>,any<T>)->any<T>;widen;legacy",
            ),
            record(
                "coalesce",
                "builtin.scalar/coalesce/(any<T>...)->any<T>;widen;legacy",
            ),
            record(
                "ifnull",
                "builtin.scalar/ifnull/(any<T>,any<T>)->any<T>;widen;legacy",
            ),
        ])
        .unwrap()
}
fn record(name: &str, overload: &str) -> InstalledPureKernel {
    InstalledPureKernel {
        function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
        kind: FunctionKind::Scalar,
        implementation: PureImplementationDeclaration {
            overload: FunctionOverloadId::try_new(overload).unwrap(),
            implementation: PureImplementationId::try_new(format!(
                "builtin.scalar/{name}/selected-v1"
            ))
            .unwrap(),
            abi: PureKernelAbi::ControlIntrinsicV1,
        },
        aggregate_state_format: None,
    }
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(17),
        region: EvaluationRegionId::new(5),
        demand: EvaluationDemand::Value,
    }
}
fn argument(ty: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant: None,
    }
}
fn arguments(name: &str, value: FunctionValueType) -> Vec<FunctionArgument> {
    if name == "if" {
        vec![
            argument(FunctionValueType::new(DataType::Boolean, true)),
            argument(value.clone()),
            argument(value),
        ]
    } else {
        vec![argument(value.clone()), argument(value)]
    }
}
fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    }
}
fn input<'a>(
    owner: &'a ControlOwner,
    selected: &'a FunctionBindingSelection,
    arguments: &'a [FunctionArgument],
    uses: &'a [Option<ExpressionUseId>],
    parameters: &'a SemanticParameters,
) -> CallEffectInput<'a> {
    CallEffectInput {
        context: context(),
        argument_uses: crate::CallArgumentUses::SelectedChannels(uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected,
        request: request(arguments),
        environment: &[],
        parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Region(context().region),
    }
}
fn uses(count: usize) -> Vec<Option<ExpressionUseId>> {
    (0..count)
        .map(|ordinal| Some(ExpressionUseId::new(100 + ordinal as u32)))
        .collect()
}
fn preparation() -> PureCallPreparation {
    PureCallPreparation::ControlIntrinsic {
        arguments: ScopedExpressionEffects::pure_value(context()),
    }
}

#[test]
fn actual_catalogue_attaches_exact_control_records_without_scalar_instances() {
    let mut whole = EngineFunctionCatalogBuilder::new();
    super::super::catalogue::contribute_builtin_functions(&mut whole).unwrap();
    let subset = catalog();
    for name in NAMES {
        let owner = test_owner(name);
        let actual = whole.definition(name, FunctionKind::Scalar).unwrap();
        assert_eq!(
            actual.binding_declaration().unwrap(),
            owner.binding_declaration()
        );
        assert!(actual.binding.as_ref().unwrap().pure.is_some());
        assert!(
            subset
                .metadata()
                .definition_by_id(owner.declaration.function_id())
                .is_some()
        );
        let base = effects(operation(name).unwrap());
        assert_eq!(base.null_behavior, FunctionNullBehavior::ControlDefined);
        assert_eq!(base.own_row_error, FunctionIntrinsicRowError::NoRowError);
        assert_eq!(base.instance_state, FunctionInstanceState::None);
        assert_eq!(
            owner.implementations[0].abi,
            PureKernelAbi::ControlIntrinsicV1
        );
    }
    for name in ["IF", "case", "nullif", "nvl"] {
        assert!(operation(name).is_none());
    }
}

#[test]
fn fresh_and_frozen_keep_exact_full_types_nullable_policy_and_selected_arc() {
    let catalog = catalog();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let nested = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )));
    for name in NAMES {
        let owner = test_owner(name);
        for nullable in [false, true] {
            for value in [
                FunctionValueType::new(DataType::Int64, nullable),
                FunctionValueType::try_with_logical_type(
                    DataType::Utf8,
                    nullable,
                    ValueLogicalType::Json,
                )
                .unwrap(),
                FunctionValueType::from_legacy_declared_projection(nested.clone(), nullable),
            ] {
                let arguments = arguments(name, value.clone());
                let selected = Arc::new(
                    owner
                        .resolve(request(&arguments), crate::binding_test_control())
                        .unwrap(),
                );
                assert_eq!(selected.result_type, FunctionResultType::Scalar(value));
                let uses = uses(arguments.len());
                for policy in [
                    DecimalOverflowPolicy::ReportError,
                    DecimalOverflowPolicy::OutputNull,
                ] {
                    let mut exact = input(&owner, &selected, &arguments, &uses, &parameters);
                    exact.decimal_overflow_policy = policy;
                    let fresh = catalog
                        .prepare_fresh(
                            exact,
                            selected.clone(),
                            preparation(),
                            crate::binding_test_control(),
                        )
                        .unwrap();
                    assert_eq!(fresh.source(), PurePreparationSource::Fresh);
                    assert!(matches!(
                        fresh.prepared(),
                        PreparedPureKernel::ControlIntrinsic(_)
                    ));
                    assert!(std::ptr::eq(
                        fresh.call_contract().selected(),
                        selected.as_ref()
                    ));
                    assert_eq!(fresh.call_contract().decimal_overflow_policy(), policy);
                    let frozen = catalog
                        .prepare_frozen(
                            exact,
                            selected.clone(),
                            fresh.call_contract().effects(),
                            preparation(),
                            crate::binding_test_control(),
                        )
                        .unwrap();
                    assert_eq!(frozen.source(), PurePreparationSource::Frozen);
                    assert_eq!(
                        frozen.call_contract().effects(),
                        fresh.call_contract().effects()
                    );
                    assert!(std::ptr::eq(
                        frozen.call_contract().selected(),
                        selected.as_ref()
                    ));
                }
            }
        }
    }
}

#[test]
fn frozen_effect_tampering_never_falls_back_to_fresh_facts() {
    let catalog = catalog();
    let parameters = SemanticParameters::try_new([]).unwrap();
    for name in NAMES {
        let owner = test_owner(name);
        let arguments = arguments(name, FunctionValueType::new(DataType::Int64, true));
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let uses = uses(arguments.len());
        let exact = input(&owner, &selected, &arguments, &uses, &parameters);
        let fresh = catalog
            .prepare_fresh(
                exact,
                selected.clone(),
                preparation(),
                crate::binding_test_control(),
            )
            .unwrap();
        let correct = fresh.call_contract().effects();
        let mut mutations = Vec::new();
        let mut facts = correct.clone();
        facts.value_stability = FunctionVolatility::Volatile;
        mutations.push(facts);
        let mut facts = correct.clone();
        facts.own_row_error = FunctionIntrinsicRowError::MayRaise;
        mutations.push(facts);
        let mut facts = correct.clone();
        facts.failure_behavior = FunctionFailureBehavior::ReturnsNull;
        mutations.push(facts);
        let mut facts = correct.clone();
        facts.null_behavior = FunctionNullBehavior::Strict;
        mutations.push(facts);
        let mut facts = correct.clone();
        facts.argument_control = ArgumentControl::Eager;
        mutations.push(facts);
        let mut facts = correct.clone();
        facts.instance_state = FunctionInstanceState::ScalarInstance;
        mutations.push(facts);
        let mut facts = correct.clone();
        facts.observable_effects.rng_sampling = true;
        mutations.push(facts);
        let mut facts = correct.clone();
        facts.proof_scope = CallProofScope::Region(EvaluationRegionId::new(99));
        mutations.push(facts);
        let mut facts = correct.clone();
        facts.environment = vec![SemanticParameterRef {
            expected_key: SemanticParameterKey::TimeZone,
            id: SemanticParameterId::new(0),
        }]
        .into_boxed_slice();
        mutations.push(facts);
        for facts in mutations {
            assert!(matches!(
                catalog.prepare_frozen(
                    exact,
                    selected.clone(),
                    &facts,
                    preparation(),
                    crate::binding_test_control()
                ),
                Err(FunctionSpecializationFailure::InvalidInput(_))
            ));
        }
    }
}

#[test]
fn wrong_occurrence_scope_and_scalar_preparation_are_refused() {
    let catalog = catalog();
    let owner = test_owner("if");
    let arguments = arguments("if", FunctionValueType::new(DataType::Int64, false));
    let selected = Arc::new(
        owner
            .resolve(request(&arguments), crate::binding_test_control())
            .unwrap(),
    );
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = uses(3);
    let exact = input(&owner, &selected, &arguments, &uses, &parameters);
    let mut foreign = context();
    foreign.use_id = ExpressionUseId::new(18);
    assert!(matches!(
        catalog.prepare_fresh(
            exact,
            selected.clone(),
            PureCallPreparation::ControlIntrinsic {
                arguments: ScopedExpressionEffects::pure_value(foreign)
            },
            crate::binding_test_control()
        ),
        Err(FunctionSpecializationFailure::Effects(_))
    ));
    let mut wrong_scope = exact;
    wrong_scope.proof_scope = CallProofScope::Region(EvaluationRegionId::new(88));
    assert!(
        catalog
            .prepare_fresh(
                wrong_scope,
                selected.clone(),
                preparation(),
                crate::binding_test_control()
            )
            .is_err()
    );
    assert!(
        catalog
            .prepare_fresh(
                exact,
                selected.clone(),
                PureCallPreparation::Scalar {
                    arguments: ScopedExpressionEffects::pure_value(context())
                },
                crate::binding_test_control()
            )
            .is_err()
    );
    assert!(
        catalog
            .prepare_fresh(
                exact,
                Arc::new(selected.as_ref().clone()),
                preparation(),
                crate::binding_test_control()
            )
            .is_err()
    );
}

#[test]
fn actual_arity_lambda_and_full_domain_forgery_are_rejected() {
    let owner = test_owner("if");
    let arguments = arguments("if", FunctionValueType::new(DataType::Utf8, true));
    let selected = owner
        .resolve(request(&arguments), crate::binding_test_control())
        .unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = uses(3);
    for count in [0, 1, 2, 4] {
        let bad = vec![argument(FunctionValueType::new(DataType::Int64, true)); count];
        assert!(
            owner
                .resolve(request(&bad), crate::binding_test_control())
                .is_err()
        );
    }
    assert!(
        test_owner("coalesce")
            .resolve(request(&[]), crate::binding_test_control())
            .is_err()
    );
    let mut lambda = arguments.clone();
    lambda[1] = FunctionArgument::Lambda {
        parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
            Box::new([]),
        ),
        result_type: FunctionValueType::new(DataType::Utf8, true),
    };
    assert!(
        owner
            .resolve(request(&lambda), crate::binding_test_control())
            .is_err()
    );
    let mut wrong = selected.clone();
    let mut revised = wrong.argument_types.to_vec();
    revised[1] = FunctionArgumentType::Value(
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
    );
    wrong.argument_types =
        novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(revised.into_boxed_slice());
    assert!(
        owner
            .validate_selected(&wrong, request(&arguments), crate::binding_test_control())
            .is_err()
    );
    let mut exact = input(&owner, &selected, &arguments, &uses, &parameters);
    let missing_uses = [
        Some(ExpressionUseId::new(100)),
        None,
        Some(ExpressionUseId::new(102)),
    ];
    exact.argument_uses = crate::CallArgumentUses::SelectedChannels(&missing_uses);
    assert!(
        owner
            .validate_and_refine(exact, crate::binding_test_control())
            .is_err()
    );
    let mut wrong_count = exact;
    wrong_count.request.logical_argument_count = 2;
    assert!(
        owner
            .validate_and_refine(wrong_count, crate::binding_test_control())
            .is_err()
    );
}

#[test]
fn no_own_row_error_does_not_erase_guarded_child_errors_state_or_observables() {
    let catalog = catalog();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let child_summary = ExpressionEffects {
        may_raise_invocation_data: false,
        value_stability: FunctionVolatility::Volatile,
        may_raise_row_error: true,
        has_instance_state: true,
        observable_effects: ObservableEffects {
            rng_sampling: true,
            warnings: true,
            controlled_wait: false,
        },
    };
    // The controller/compiler supplies the already-composed actual child-use
    // summary. This test grants no permission to invoke or hoist guarded rows.
    for name in NAMES {
        let owner = test_owner(name);
        let arguments = arguments(name, FunctionValueType::new(DataType::Int64, true));
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let uses = uses(arguments.len());
        let exact = input(&owner, &selected, &arguments, &uses, &parameters);
        let prepared = catalog
            .prepare_fresh(
                exact,
                selected.clone(),
                PureCallPreparation::ControlIntrinsic {
                    arguments: ScopedExpressionEffects::primitive(context(), child_summary),
                },
                crate::binding_test_control(),
            )
            .unwrap();
        assert_eq!(
            prepared.call_contract().effects().own_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
        assert_eq!(
            prepared.effects().for_use(context()).unwrap(),
            child_summary
        );
    }
}

struct Trace {
    trace: Mutex<Vec<u32>>,
    fail_at: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, completed: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(completed <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(completed);
        if let Some((ordinal, error)) = self.fail_at.clone() {
            if trace.len() == ordinal + 1 {
                return Err(error);
            }
            assert!(
                trace.len() <= ordinal + 1,
                "callback after the first control failure"
            );
        }
        Ok(())
    }
}
#[test]
fn real_variadic_owner_keeps_entry_quantum_and_tail_three_control_causes() {
    let owner = test_owner("coalesce");
    let arguments = vec![argument(FunctionValueType::new(DataType::Int64, true)); 320];
    let selected = owner
        .resolve(request(&arguments), crate::binding_test_control())
        .unwrap();
    let uses = uses(arguments.len());
    let parameters = SemanticParameters::try_new([]).unwrap();
    let exact = input(&owner, &selected, &arguments, &uses, &parameters);
    let successful = Trace {
        trace: Mutex::new(Vec::new()),
        fail_at: None,
    };
    owner.validate_and_refine(exact, &successful).unwrap();
    let trace = successful.trace.lock().unwrap().clone();
    assert_eq!(trace[0], 0);
    assert!(trace.contains(&256));
    assert!(trace.iter().any(|&work| work > 0 && work < 256));
    for ordinal in 0..trace.len() {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let stop = Trace {
                trace: Mutex::new(Vec::new()),
                fail_at: Some((ordinal, error.clone())),
            };
            assert!(
                matches!(owner.validate_and_refine(exact, &stop), Err(FunctionEffectOwnerError::Control(actual)) if actual == error)
            );
            assert_eq!(*stop.trace.lock().unwrap(), trace[..=ordinal]);
        }
    }
}

#[test]
fn ordinary_owner_refusal_observes_completed_tail_and_preserves_control() {
    let owner = test_owner("if");
    let arguments = arguments("if", FunctionValueType::new(DataType::Int64, false));
    let selected = owner
        .resolve(request(&arguments), crate::binding_test_control())
        .unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = uses(3);
    let mut exact = input(&owner, &selected, &arguments, &uses, &parameters);
    exact.proof_scope = CallProofScope::Region(EvaluationRegionId::new(99));
    let successful = Trace {
        trace: Mutex::new(Vec::new()),
        fail_at: None,
    };
    assert!(matches!(
        owner.validate_and_refine(exact, &successful),
        Err(FunctionEffectOwnerError::Owner(
            FunctionBindingError::InvalidBinding(_)
        ))
    ));
    let trace = successful.trace.lock().unwrap().clone();
    assert_eq!(trace, vec![0, 1]);
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let stop = Trace {
            trace: Mutex::new(Vec::new()),
            fail_at: Some((1, error.clone())),
        };
        assert!(
            matches!(owner.validate_and_refine(exact, &stop), Err(FunctionEffectOwnerError::Control(actual)) if actual == error)
        );
        assert_eq!(*stop.trace.lock().unwrap(), trace);
    }
}

#[test]
fn actual_fresh_and_frozen_factories_keep_original_control_at_every_boundary() {
    let catalog = catalog();
    let owner = test_owner("if");
    let arguments = arguments("if", FunctionValueType::new(DataType::Boolean, true));
    let selected = Arc::new(
        owner
            .resolve(request(&arguments), crate::binding_test_control())
            .unwrap(),
    );
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = uses(3);
    let mut exact = input(&owner, &selected, &arguments, &uses, &parameters);
    exact.context.demand = EvaluationDemand::TruthOnly;
    let options = || PureCallPreparation::ControlIntrinsic {
        arguments: ScopedExpressionEffects::pure_value(exact.context),
    };
    let canonical = catalog
        .prepare_fresh(
            exact,
            selected.clone(),
            options(),
            crate::binding_test_control(),
        )
        .unwrap();
    assert_eq!(canonical.call_contract().context(), exact.context);
    for frozen in [false, true] {
        let run = |control: &dyn PureCompileControl| {
            if frozen {
                catalog.prepare_frozen(
                    exact,
                    selected.clone(),
                    canonical.call_contract().effects(),
                    options(),
                    control,
                )
            } else {
                catalog.prepare_fresh(exact, selected.clone(), options(), control)
            }
        };
        let successful = Trace {
            trace: Mutex::new(Vec::new()),
            fail_at: None,
        };
        run(&successful).unwrap();
        let trace = successful.trace.lock().unwrap().clone();
        assert!(trace.len() > 2);
        for ordinal in 0..trace.len() {
            for error in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let stop = Trace {
                    trace: Mutex::new(Vec::new()),
                    fail_at: Some((ordinal, error.clone())),
                };
                let result = run(&stop);
                // Refinement returns CompileControlError directly; the shared
                // exact call-contract constructor preserves the same cause
                // through its existing KernelFailure port.
                let preserves_cause = match (&result, error.clone()) {
                    (Err(FunctionSpecializationFailure::Control(actual)), expected) => {
                        *actual == expected
                    }
                    (
                        Err(FunctionSpecializationFailure::Kernel(crate::KernelFailure::Cancelled)),
                        CompileControlError::Cancelled,
                    )
                    | (
                        Err(FunctionSpecializationFailure::Kernel(
                            crate::KernelFailure::DeadlineExceeded,
                        )),
                        CompileControlError::DeadlineExceeded,
                    )
                    | (
                        Err(FunctionSpecializationFailure::Kernel(
                            crate::KernelFailure::ResourceExhausted,
                        )),
                        CompileControlError::ResourceExhausted,
                    ) => true,
                    _ => false,
                };
                assert!(
                    preserves_cause,
                    "frozen={frozen}, callback={ordinal}, cause={error:?}, result={result:?}, trace={:?}",
                    stop.trace.lock().unwrap()
                );
                assert_eq!(*stop.trace.lock().unwrap(), trace[..=ordinal]);
            }
        }
    }
}
