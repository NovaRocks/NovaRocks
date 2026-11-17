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
    AggregateBindingSelection, AggregateStateFormatIdentity, CallArgumentUses, FunctionArgument,
    FunctionBindingRequest, FunctionEffectOwner, FunctionEffectOwnerError, FunctionOverloadId,
    FunctionValueType, refine_call_effects,
};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileControlError, EvaluationDemand, EvaluationRegionId,
    ExpressionUseId, FunctionEffectDeclaration, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility, ObservableEffects,
    SemanticParameterId, SemanticParameterRef, SemanticParameterValue,
};
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= 256);
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
fn failures() -> [(CompileControlError, KernelFailure); 3] {
    [
        (
            CompileControlError::Cancelled,
            KernelFailure::CompileControl(CompileControlError::Cancelled),
        ),
        (
            CompileControlError::DeadlineExceeded,
            KernelFailure::CompileControl(CompileControlError::DeadlineExceeded),
        ),
        (
            CompileControlError::ResourceExhausted,
            KernelFailure::CompileControl(CompileControlError::ResourceExhausted),
        ),
    ]
}

// This fixture owns real admitted signature and environment facts, but no
// installed CPU kernel. Refinement does not invent a runtime capability.
struct Fixture {
    function: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    declaration: FunctionEffectDeclaration,
    environment: [SemanticParameterRef; 1],
    parameters: SemanticParameters,
}
impl Fixture {
    fn new(arguments: usize) -> Self {
        let value = FunctionValueType::new(DataType::Int64, false);
        let arguments = vec![
            FunctionArgument::Value {
                value_type: value.clone(),
                constant: None
            };
            arguments
        ];
        let environment = [SemanticParameterRef {
            id: SemanticParameterId::new(u32::MAX),
            expected_key: SemanticParameterValue::TimeZone("UTC".into()).key(),
        }];
        Self {
            function: FunctionId::try_new("fixture/call-footer").unwrap(),
            selected: Arc::new(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("fixture/call-footer/i64").unwrap(),
                argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                    arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Box<[_]>>(),
                ),
                result_type: FunctionResultType::Scalar(value),
                aggregate: None,
            }),
            uses: (0..arguments.len())
                .map(|n| Some(ExpressionUseId::new(n as u32)))
                .collect(),
            arguments,
            declaration: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Stable,
                own_row_error: FunctionIntrinsicRowError::NoRowError,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: ArgumentControl::Eager,
                instance_state: FunctionInstanceState::None,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: vec![environment[0].expected_key].into_boxed_slice(),
            },
            environment,
            parameters: SemanticParameters::try_new([
                (
                    SemanticParameterId::new(u32::MAX),
                    SemanticParameterValue::TimeZone("Asia/Shanghai".into()),
                ),
                (
                    SemanticParameterId::new(0),
                    SemanticParameterValue::TimeZone("UTC".into()),
                ),
            ])
            .unwrap(),
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(u32::MAX),
                region: EvaluationRegionId::new(0),
                demand: EvaluationDemand::Value,
            },
            argument_uses: CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.function,
            kind: FunctionKind::Scalar,
            selected: self.selected.as_ref(),
            request: FunctionBindingRequest {
                arguments: &self.arguments,
                logical_argument_count: self.arguments.len(),
                expected_result_type: None,
            },
            environment: &self.environment,
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        }
    }
}
impl FunctionEffectOwner for Fixture {
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
        Ok(CallEffects {
            value_stability: self.declaration.value_stability,
            own_row_error: self.declaration.own_row_error,
            failure_behavior: self.declaration.failure_behavior,
            null_behavior: self.declaration.null_behavior,
            argument_control: self.declaration.argument_control,
            instance_state: self.declaration.instance_state,
            observable_effects: self.declaration.observable_effects,
            environment: input.environment.to_vec().into_boxed_slice(),
            proof_scope: input.proof_scope,
        })
    }
}
fn check_prefixes(
    run: impl Fn(&Control) -> Result<FunctionCallContract, KernelFailure>,
    ordinary: Option<&str>,
) -> Vec<u32> {
    let baseline = Control::default();
    match (run(&baseline), ordinary) {
        (Ok(_), None) => {}
        (Err(KernelFailure::InvalidProgram(actual)), Some(expected)) => {
            assert_eq!(actual.message(), expected)
        }
        (actual, expected) => panic!("unexpected baseline: {actual:?}, {expected:?}"),
    }
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&0));
    for stop in 0..trace.len() {
        for (cause, expected) in failures() {
            let control = Control {
                refusal: Some((stop, cause)),
                ..Control::default()
            };
            assert_eq!(run(&control).unwrap_err(), expected);
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
    trace
}

#[test]
fn success_keeps_original_projection_and_full_contract_with_every_control_prefix() {
    let fixture = Fixture::new(1);
    let input = fixture.input();
    let receipt = refine_call_effects(&fixture, input, &Control::default()).unwrap();
    let run = |control: &Control| {
        FunctionCallContract::from_refined(input, &receipt, Arc::clone(&fixture.selected), control)
    };
    let trace = check_prefixes(run, None);
    assert_eq!(
        trace.iter().filter(|n| **n == 0).count(),
        2,
        "original constructor and projection entries"
    );
    assert!(trace.last().unwrap() > &0);
    let contract = run(&Control::default()).unwrap();
    assert!(Arc::ptr_eq(contract.selected_owner(), &fixture.selected));
    assert_eq!(contract.kind(), FunctionKind::Scalar);
    assert_eq!(contract.context(), input.context);
    assert_eq!(
        contract.decimal_overflow_policy(),
        DecimalOverflowPolicy::ReportError
    );
    assert_eq!(contract.logical_argument_count(), 1);
    assert_eq!(contract.effects(), receipt.facts());
    assert_eq!(contract.parameters().entries().len(), 1);
    assert_eq!(
        contract
            .parameters()
            .require(fixture.environment[0])
            .unwrap(),
        fixture.parameters.require(fixture.environment[0]).unwrap()
    );
    assert!(
        contract
            .parameters()
            .get(SemanticParameterId::new(0))
            .is_none()
    );
}

#[test]
fn foreign_input_source_and_kind_failures_observe_zero_work_ordinary_tail() {
    let fixture = Fixture::new(0);
    let input = fixture.input();
    let receipt = refine_call_effects(&fixture, input, &Control::default()).unwrap();
    let foreign_parameters = fixture.parameters.clone();
    let foreign_function = fixture.function.clone();
    let expected = FunctionValueType::new(DataType::Int64, false);
    for changed in [
        CallEffectInput {
            parameters: &foreign_parameters,
            ..input
        },
        CallEffectInput {
            function_id: &foreign_function,
            ..input
        },
        CallEffectInput {
            kind: FunctionKind::Table,
            ..input
        },
        CallEffectInput {
            request: FunctionBindingRequest {
                expected_result_type: Some(&expected),
                ..input.request
            },
            ..input
        },
    ] {
        let trace = check_prefixes(
            |control| {
                FunctionCallContract::from_refined(
                    changed,
                    &receipt,
                    Arc::clone(&fixture.selected),
                    control,
                )
            },
            Some("call refinement receipt differs from exact input"),
        );
        assert_eq!(
            trace,
            [0, 0],
            "ordinary early failure still has the original footer"
        );
    }
}

#[test]
fn equal_foreign_selected_owner_refuses_with_original_footer_and_first_control() {
    let fixture = Fixture::new(0);
    let input = fixture.input();
    let receipt = refine_call_effects(&fixture, input, &Control::default()).unwrap();
    let foreign = Arc::new(fixture.selected.as_ref().clone());
    assert_eq!(foreign.as_ref(), fixture.selected.as_ref());
    assert!(!Arc::ptr_eq(&foreign, &fixture.selected));
    assert_eq!(
        check_prefixes(
            |control| FunctionCallContract::from_refined(
                input,
                &receipt,
                Arc::clone(&foreign),
                control
            ),
            Some("signature owner differs from the exact input borrow")
        ),
        [0, 0]
    );
}

#[test]
fn admitted_refinement_headers_still_require_call_kind_and_result_shape() {
    for aggregate_header in [false, true] {
        let mut fixture = Fixture::new(0);
        let selected = Arc::get_mut(&mut fixture.selected).unwrap();
        if aggregate_header {
            selected.aggregate = Some(AggregateBindingSelection {
                state_argument_contract:
                    novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                intermediate_type: FunctionValueType::new(DataType::Int64, false),
                state_format: AggregateStateFormatIdentity::try_new("fixture/call-footer/state")
                    .unwrap(),
            });
        } else {
            selected.result_type = FunctionResultType::Relation(
                novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                    vec![FunctionValueType::new(DataType::Int64, false)].into_boxed_slice(),
                ),
            );
        }
        let input = fixture.input();
        let receipt = refine_call_effects(&fixture, input, &Control::default()).unwrap();
        let message = if aggregate_header {
            "aggregate state contract differs from the exact function kind"
        } else {
            "result shape differs from the exact function kind"
        };
        assert_eq!(
            check_prefixes(
                |control| FunctionCallContract::from_refined(
                    input,
                    &receipt,
                    Arc::clone(&fixture.selected),
                    control
                ),
                Some(message)
            ),
            [0, 0]
        );
    }
}

#[test]
fn real_wide_signature_reports_256_and_stops_before_projection_on_refusal() {
    let fixture = Fixture::new(320);
    let input = fixture.input();
    let receipt = refine_call_effects(&fixture, input, &Control::default()).unwrap();
    let baseline = Control::default();
    FunctionCallContract::from_refined(input, &receipt, Arc::clone(&fixture.selected), &baseline)
        .unwrap();
    let trace = baseline.trace.lock().unwrap().clone();
    let quantum = trace.iter().position(|units| *units == 256).unwrap();
    for stop in [0, quantum, trace.len() - 1] {
        for (cause, expected) in failures() {
            let control = Control {
                refusal: Some((stop, cause)),
                ..Control::default()
            };
            assert_eq!(
                FunctionCallContract::from_refined(
                    input,
                    &receipt,
                    Arc::clone(&fixture.selected),
                    &control
                )
                .unwrap_err(),
                expected
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
    assert_eq!(
        &trace[..=quantum],
        &[0, 256],
        "real signature walk reaches the first quantum before delegated projection"
    );
}
