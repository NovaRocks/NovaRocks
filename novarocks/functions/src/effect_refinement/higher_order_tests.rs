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
    FunctionArgument, FunctionBindingError, FunctionOverloadId, FunctionResultType,
    FunctionValueType,
};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, EvaluationDemand, EvaluationRegionId, ExpressionEffectContext,
    ExpressionUseId, FunctionFailureBehavior, FunctionInstanceState, FunctionIntrinsicRowError,
    FunctionNullBehavior, FunctionVolatility, MAX_UNOBSERVED_COMPILE_WORK, ObservableEffects,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Fixture {
    id: FunctionId,
    selected: FunctionBindingSelection,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    parameters: SemanticParameters,
}

fn value() -> FunctionArgument {
    FunctionArgument::Value {
        value_type: FunctionValueType::new(DataType::Int64, false),
        constant: None,
    }
}

#[test]
fn type_only_scalar_preparation_retains_lambda_types_without_body_value_demand() {
    let mut fixture = Fixture::body(FunctionValueType::new(DataType::Boolean, true));
    fixture.uses.fill(None);
    let owner = Owner::new(&fixture, ArgumentControl::TypeOnly);
    let selected = Arc::new(fixture.selected.clone());
    let input = CallEffectInput {
        selected: selected.as_ref(),
        ..fixture.input()
    };
    let control = Control::default();
    let receipt = refine_call_effects(&owner, input, &control).unwrap();
    let contract =
        crate::ScalarCallContract::from_refined(input, &receipt, selected.clone(), &control)
            .unwrap();
    assert_eq!(contract.selected().argument_types, selected.argument_types);
    assert_eq!(
        contract.effects().argument_control,
        ArgumentControl::TypeOnly
    );
    assert_eq!(contract.value_argument_types().len(), 0);
    assert_eq!(owner.calls(), 1);
}

#[test]
fn type_only_preparation_rejects_malformed_or_unbounded_lambda_metadata() {
    assert!(
        FunctionValueType::try_with_logical_type(
            DataType::Boolean,
            true,
            novarocks_type_contract::ValueLogicalType::LargeInt
        )
        .is_err()
    );
    {
        let (parameter_types, result_type, resource_failure) = (
            vec![FunctionValueType::new(DataType::Int64, false); MAX_CALL_EFFECT_ARGUMENTS + 1],
            FunctionValueType::new(DataType::Boolean, true),
            true,
        );
        let mut fixture = Fixture::new(vec![FunctionArgument::Lambda {
            parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                parameter_types.into_boxed_slice(),
            ),
            result_type,
        }]);
        fixture.uses.fill(None);
        let owner = Owner::new(&fixture, ArgumentControl::TypeOnly);
        let selected = Arc::new(fixture.selected.clone());
        let input = CallEffectInput {
            selected: selected.as_ref(),
            ..fixture.input()
        };
        let control = Control::default();
        let result = refine_call_effects(&owner, input, &control);
        if resource_failure {
            assert!(matches!(
                result,
                Err(CallEffectRefinementError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
        } else {
            assert!(matches!(
                result,
                Err(CallEffectRefinementError::InvalidInput(_))
            ));
        }
        assert_eq!(owner.calls(), 0);
    }
}

fn lambda(result_type: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Lambda {
        parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
            vec![FunctionValueType::new(DataType::Int64, false)].into_boxed_slice(),
        ),
        result_type,
    }
}

impl Fixture {
    fn new(arguments: Vec<FunctionArgument>) -> Self {
        Self {
            id: FunctionId::try_new("fixture/higher-order/exact-owner").unwrap(),
            selected: FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("fixture/higher-order/exact-signature")
                    .unwrap(),
                argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                    arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                ),
                // The body demand is independent of the outer call result.
                result_type: FunctionResultType::Scalar(FunctionValueType::new(
                    DataType::Int64,
                    false,
                )),
                aggregate: None,
            },
            uses: (0..arguments.len())
                .map(|ordinal| Some(ExpressionUseId::new(ordinal as u32 + 1)))
                .collect(),
            arguments,
            parameters: SemanticParameters::default(),
        }
    }

    fn body(result_type: FunctionValueType) -> Self {
        Self::new(vec![value(), lambda(result_type)])
    }

    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: (ExpressionEffectContext {
                use_id: ExpressionUseId::new(0),
                region: EvaluationRegionId::new(7),
                demand: EvaluationDemand::Value,
            })
            .into(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.id,
            kind: FunctionKind::Scalar,
            selected: &self.selected,
            request: FunctionBindingRequest {
                expected_result_type: None,
                arguments: &self.arguments,
                logical_argument_count: self.arguments.len(),
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Region(EvaluationRegionId::new(7)),
        }
    }
}

struct Owner {
    id: FunctionId,
    selected: FunctionBindingSelection,
    declaration: FunctionEffectDeclaration,
    refined_control: Option<ArgumentControl>,
    calls: AtomicUsize,
}

impl Owner {
    fn new(fixture: &Fixture, argument_control: ArgumentControl) -> Self {
        Self {
            id: fixture.id.clone(),
            selected: fixture.selected.clone(),
            declaration: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NoRowError,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: if matches!(argument_control, ArgumentControl::HigherOrder { .. }) {
                    FunctionNullBehavior::ControlDefined
                } else {
                    FunctionNullBehavior::CalledOnNull
                },
                argument_control,
                instance_state: FunctionInstanceState::None,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::new([]),
            },
            refined_control: None,
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl FunctionEffectOwner for Owner {
    type Error = FunctionBindingError;

    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if function != &self.id || selected.overload != self.selected.overload {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(&self.declaration)
    }

    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        if input.function_id != &self.id
            || input.kind != FunctionKind::Scalar
            || input.selected != &self.selected
            || input.request.logical_argument_count != self.selected.argument_types.len()
            || input.request.arguments.len() != self.selected.argument_types.len()
            || !input.environment.is_empty()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "fixture exact selected signature differs".into(),
            )
            .into());
        }
        for (argument, selected) in input
            .request
            .arguments
            .iter()
            .zip(&self.selected.argument_types)
        {
            if argument.argument_type() != *selected
                || matches!(
                    argument,
                    FunctionArgument::Value {
                        constant: Some(_),
                        ..
                    }
                )
            {
                return Err(FunctionBindingError::InvalidBinding(
                    "fixture exact argument type or constant differs".into(),
                )
                .into());
            }
            work.step().map_err(FunctionEffectOwnerError::Control)?;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        Ok(CallEffects {
            value_stability: self.declaration.value_stability,
            own_row_error: self.declaration.own_row_error,
            failure_behavior: self.declaration.failure_behavior,
            null_behavior: self.declaration.null_behavior,
            argument_control: self
                .refined_control
                .unwrap_or(self.declaration.argument_control),
            instance_state: self.declaration.instance_state,
            observable_effects: self.declaration.observable_effects,
            environment: Box::new([]),
            proof_scope: input.proof_scope,
        })
    }
}

#[derive(Default)]
struct Control {
    fail_on_work: Option<CompileControlError>,
    checkpoints: Mutex<Vec<u32>>,
}

impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= MAX_UNOBSERVED_COMPILE_WORK);
        self.checkpoints.lock().unwrap().push(units);
        if units > 0
            && let Some(error) = self.fail_on_work.clone()
        {
            return Err(error);
        }
        Ok(())
    }
}

fn higher_order(body_ordinal: u32, body_demand: EvaluationDemand) -> ArgumentControl {
    ArgumentControl::HigherOrder {
        body_ordinal,
        body_demand,
    }
}

fn assert_invalid_before_refiner(fixture: &Fixture, control: ArgumentControl, reason: &str) {
    let owner = Owner::new(fixture, control);
    assert!(
        matches!(
            refine_call_effects(&owner, fixture.input(), &Control::default()),
            Err(CallEffectRefinementError::InvalidInput(actual)) if actual == reason
        ),
        "expected pre-refiner rejection: {reason}"
    );
    assert_eq!(owner.calls(), 0);
}

#[test]
fn exact_owner_body_demand_accepts_boolean_value_truth_and_numeric_value() {
    for (body_type, demand) in [
        (
            FunctionValueType::new(DataType::Boolean, false),
            EvaluationDemand::Value,
        ),
        (
            FunctionValueType::new(DataType::Boolean, true),
            EvaluationDemand::Value,
        ),
        (
            FunctionValueType::new(DataType::Boolean, false),
            EvaluationDemand::TruthOnly,
        ),
        (
            FunctionValueType::new(DataType::Boolean, true),
            EvaluationDemand::TruthOnly,
        ),
        (
            FunctionValueType::new(DataType::Int64, true),
            EvaluationDemand::Value,
        ),
    ] {
        let fixture = Fixture::body(body_type);
        let protocol = higher_order(1, demand);
        let owner = Owner::new(&fixture, protocol);
        let result = refine_call_effects(&owner, fixture.input(), &Control::default()).unwrap();
        assert_eq!(result.facts().argument_control, protocol);
        assert_eq!(result.facts().proof_scope, fixture.input().proof_scope);
        assert_eq!(owner.calls(), 1);
    }
}

#[test]
fn numeric_truth_body_and_wrong_body_ordinal_fail_before_refinement() {
    let numeric = Fixture::body(FunctionValueType::new(DataType::Int64, true));
    assert_invalid_before_refiner(
        &numeric,
        higher_order(1, EvaluationDemand::TruthOnly),
        "higher-order TruthOnly body requires an exact Boolean result type",
    );
    let boolean = Fixture::body(FunctionValueType::new(DataType::Boolean, true));
    for ordinal in [0, 2, u32::MAX] {
        assert_invalid_before_refiner(
            &boolean,
            higher_order(ordinal, EvaluationDemand::Value),
            "higher-order body ordinal does not identify the selected lambda argument",
        );
    }
}

#[test]
fn multiple_lambda_channels_and_eager_lambda_fail_before_refinement() {
    let multiple = Fixture::new(vec![
        lambda(FunctionValueType::new(DataType::Int64, false)),
        lambda(FunctionValueType::new(DataType::Boolean, true)),
    ]);
    assert_invalid_before_refiner(
        &multiple,
        higher_order(1, EvaluationDemand::TruthOnly),
        "higher-order body declaration differs from selected lambda channels",
    );
    let fixture = Fixture::body(FunctionValueType::new(DataType::Boolean, true));
    assert_invalid_before_refiner(
        &fixture,
        ArgumentControl::Eager,
        "lambda value arguments require an exact higher-order control declaration",
    );
}

#[test]
fn type_only_inspects_lambda_types_without_any_argument_uses() {
    let mut fixture = Fixture::new(vec![
        lambda(FunctionValueType::new(DataType::Int64, true)),
        lambda(FunctionValueType::new(DataType::Boolean, true)),
        value(),
    ]);
    fixture.uses.fill(None);
    let owner = Owner::new(&fixture, ArgumentControl::TypeOnly);
    let result = refine_call_effects(&owner, fixture.input(), &Control::default()).unwrap();
    assert_eq!(result.facts().argument_control, ArgumentControl::TypeOnly);
    assert_eq!(owner.calls(), 1);

    fixture.uses[1] = Some(ExpressionUseId::new(17));
    assert_invalid_before_refiner(
        &fixture,
        ArgumentControl::TypeOnly,
        "call argument demand differs from its exact owner control",
    );
}

#[test]
fn higher_order_requires_argument_uses_and_scalar_kind_before_refinement() {
    let mut fixture = Fixture::body(FunctionValueType::new(DataType::Boolean, true));
    let protocol = higher_order(1, EvaluationDemand::TruthOnly);
    fixture.uses[1] = None;
    assert_invalid_before_refiner(
        &fixture,
        protocol,
        "call argument demand differs from its exact owner control",
    );
    fixture.uses[1] = Some(ExpressionUseId::new(2));
    let owner = Owner::new(&fixture, protocol);
    for kind in [
        FunctionKind::Aggregate,
        FunctionKind::Window,
        FunctionKind::Table,
    ] {
        assert!(matches!(
            refine_call_effects(
                &owner,
                CallEffectInput {
                    kind,
                    ..fixture.input()
                },
                &Control::default()
            ),
            Err(CallEffectRefinementError::Contract(
                EffectContractError::KindMismatch
            ))
        ));
        assert_eq!(owner.calls(), 0);
    }
}

#[test]
fn refiner_cannot_change_exact_body_position_or_demand() {
    let fixture = Fixture::body(FunctionValueType::new(DataType::Boolean, true));
    let protocol = higher_order(1, EvaluationDemand::Value);
    for forged in [
        higher_order(0, EvaluationDemand::Value),
        higher_order(1, EvaluationDemand::TruthOnly),
        ArgumentControl::Eager,
    ] {
        let mut owner = Owner::new(&fixture, protocol);
        owner.refined_control = Some(forged);
        assert!(matches!(
            refine_call_effects(&owner, fixture.input(), &Control::default()),
            Err(CallEffectRefinementError::Contract(
                EffectContractError::ControlMismatch
            ))
        ));
        assert_eq!(owner.calls(), 1);
    }
}

#[test]
fn maximum_arity_body_validation_observes_control_before_refiner() {
    let mut arguments = vec![value(); MAX_CALL_EFFECT_ARGUMENTS];
    arguments[MAX_CALL_EFFECT_ARGUMENTS - 1] =
        lambda(FunctionValueType::new(DataType::Boolean, true));
    let fixture = Fixture::new(arguments);
    let protocol = higher_order(
        (MAX_CALL_EFFECT_ARGUMENTS - 1) as u32,
        EvaluationDemand::TruthOnly,
    );
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let owner = Owner::new(&fixture, protocol);
        let control = Control {
            fail_on_work: Some(error.clone()),
            ..Control::default()
        };
        assert!(matches!(
            refine_call_effects(&owner, fixture.input(), &control),
            Err(CallEffectRefinementError::Control(actual)) if actual == error
        ));
        assert_eq!(owner.calls(), 0);
        assert_eq!(
            *control.checkpoints.lock().unwrap(),
            vec![0, MAX_UNOBSERVED_COMPILE_WORK]
        );
    }
}

#[test]
fn over_limit_arity_is_resource_exhausted_before_refiner() {
    let mut arguments = vec![value(); MAX_CALL_EFFECT_ARGUMENTS + 1];
    arguments[MAX_CALL_EFFECT_ARGUMENTS] = lambda(FunctionValueType::new(DataType::Boolean, true));
    let fixture = Fixture::new(arguments);
    let owner = Owner::new(
        &fixture,
        higher_order(
            MAX_CALL_EFFECT_ARGUMENTS as u32,
            EvaluationDemand::TruthOnly,
        ),
    );
    let control = Control::default();
    assert!(matches!(
        refine_call_effects(&owner, fixture.input(), &control),
        Err(CallEffectRefinementError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(owner.calls(), 0);
    assert_eq!(*control.checkpoints.lock().unwrap(), vec![0]);
}
