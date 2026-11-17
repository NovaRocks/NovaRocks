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

//! Public trait calls on the original COUNT author, not a replacement refiner.
use super::*;
use novarocks_type_contract::{
    CompileControlError, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
    ExpressionEffectContext, ExpressionUseId, SemanticParameterId, SemanticParameterKey,
    SemanticParameterRef, SemanticParameters,
};
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "COUNT compile callback after first refusal");
        }
        trace.push((phase, units));
        match &self.refusal {
            Some((stop, original)) if at == *stop => Err(original.clone()),
            _ => Ok(()),
        }
    }
}
struct Fixture {
    owner: CountOwner,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    parameters: SemanticParameters,
}
impl Fixture {
    fn new(arity: usize) -> Self {
        let (declaration, resolver) = super::super::catalogue::count_compile_owner_parts_for_test();
        let owner = CountOwner::new("count", declaration, resolver).unwrap();
        let arguments: Vec<_> = (0..arity)
            .map(|_| FunctionArgument::Value {
                value_type: FunctionValueType::from_legacy_declared_projection(
                    DataType::Int64,
                    true,
                ),
                constant: None,
            })
            .collect();
        let selected = Arc::new(
            owner
                .resolve(
                    FunctionBindingRequest {
                        arguments: &arguments,
                        logical_argument_count: arity,
                        expected_result_type: None,
                    },
                    &Control::default(),
                )
                .unwrap(),
        );
        Self {
            owner,
            selected,
            arguments,
            uses: (0..arity)
                .map(|at| Some(ExpressionUseId::new(at as u32 + 1)))
                .collect(),
            parameters: SemanticParameters::try_new([]).unwrap(),
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        let context = ExpressionEffectContext {
            use_id: ExpressionUseId::new(0),
            region: EvaluationRegionId::new(7),
            demand: EvaluationDemand::Value,
        };
        CallEffectInput {
            function_id: self.owner.binding_declaration().function_id(),
            kind: FunctionKind::Aggregate,
            selected: &self.selected,
            request: FunctionBindingRequest {
                arguments: &self.arguments,
                logical_argument_count: self.arguments.len(),
                expected_result_type: None,
            },
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            context,
            parameters: &self.parameters,
            environment: &[],
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Region(context.region),
        }
    }
    fn canonical(&self) -> Arc<AggregateCallContract> {
        let input = self.input();
        let actual = specialize_aggregate(
            &self.owner,
            input,
            self.selected.clone(),
            ScopedExpressionEffects::pure_value(input.context),
            AggregatePreparationOptions {
                state_interpretation: None,
                phase: AggregateKernelPhase::Single,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: None,
            },
            &Control::default(),
        )
        .unwrap();
        actual.prepared().contract().clone()
    }
}
fn original_causes() -> [CompileControlError; 4] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
        CompileControlError::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        ),
    ]
}
fn assert_original_control(actual: CompileControlError, original: &CompileControlError) {
    // SharedFailureCause equality is Arc identity. Also compare the original
    // domain source addresses so this cannot pass after textual reconstruction.
    assert_eq!(&actual, original);
    if let (CompileControlError::Source(actual), CompileControlError::Source(original)) =
        (&actual, original)
    {
        type Source = novarocks_type_contract::owned_resources::layout::LayoutResourceError;
        assert!(std::ptr::eq(
            actual.downcast_ref::<Source>().unwrap(),
            original.downcast_ref::<Source>().unwrap()
        ));
    }
}
fn assert_refine_semantic(error: FunctionEffectOwnerError<FunctionBindingError>, field: usize) {
    match (field, error) {
        (0, FunctionEffectOwnerError::Owner(FunctionBindingError::UnknownFunction)) => {}
        (_, FunctionEffectOwnerError::Owner(FunctionBindingError::InvalidBinding(detail)))
            if field != 0 =>
        {
            assert_eq!(
                &*detail,
                "COUNT requires its original scope and no environment"
            );
        }
        (_, other) => panic!("actual COUNT semantic refusal changed: {other:?}"),
    }
}
fn assert_prepare_semantic(error: KernelFailure) {
    match error {
        KernelFailure::InvalidProgram(detail) => assert_eq!(
            detail.message(),
            "COUNT preparation differs from its exact checked call"
        ),
        other => panic!("actual COUNT exact-call refusal changed: {other:?}"),
    }
}

#[test]
fn count_public_refiner_preserves_authored_identity_scope_and_environment_errors_before_late_control()
 {
    for arity in [0, 1] {
        let fixture = Fixture::new(arity);
        let foreign = FunctionId::try_new("fixture/foreign/count-owner").unwrap();
        let environment = [SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::StatementStartUtc,
        }];
        for field in 0..3 {
            let mut wrong = fixture.input();
            match field {
                0 => wrong.function_id = &foreign,
                1 => wrong.proof_scope = CallProofScope::Region(EvaluationRegionId::new(99)),
                _ => wrong.environment = &environment,
            }
            let selected_arcs = Arc::strong_count(&fixture.selected);
            let baseline = Control::default();
            assert_refine_semantic(
                fixture
                    .owner
                    .validate_and_refine(wrong, &baseline)
                    .unwrap_err(),
                field,
            );
            let trace = baseline.trace.lock().unwrap().clone();
            assert_eq!(trace, [(CompilePhase::FunctionSpecialization, 0)]);
            for cause in original_causes() {
                let earlier = Control {
                    trace: Mutex::default(),
                    refusal: Some((0, cause.clone())),
                };
                match fixture
                    .owner
                    .validate_and_refine(wrong, &earlier)
                    .unwrap_err()
                {
                    FunctionEffectOwnerError::Control(actual) => {
                        assert_original_control(actual, &cause)
                    }
                    other => panic!("earlier COUNT control source was replaced: {other:?}"),
                }
                assert_eq!(*earlier.trace.lock().unwrap(), trace);
                let late = Control {
                    trace: Mutex::default(),
                    refusal: Some((trace.len(), cause)),
                };
                assert_refine_semantic(
                    fixture.owner.validate_and_refine(wrong, &late).unwrap_err(),
                    field,
                );
                assert_eq!(
                    *late.trace.lock().unwrap(),
                    trace,
                    "semantic refusal must not start the armed late cancellation/source callback"
                );
                assert_eq!(Arc::strong_count(&fixture.selected), selected_arcs);
            }
        }
    }
}

#[test]
fn count_public_prepare_preserves_exact_contract_error_and_releases_original_contract_before_late_control()
 {
    for arity in [0, 1] {
        let fixture = Fixture::new(arity);
        let contract = fixture.canonical();
        let selected_arcs = Arc::strong_count(&fixture.selected);
        let contract_arcs = Arc::strong_count(&contract);
        let foreign = FunctionId::try_new("fixture/foreign/count-prepare").unwrap();
        for field in 0..2 {
            let mut wrong = fixture.input();
            match field {
                0 => wrong.function_id = &foreign,
                _ => wrong.decimal_overflow_policy = DecimalOverflowPolicy::OutputNull,
            }
            let baseline = Control::default();
            assert_prepare_semantic(
                fixture
                    .owner
                    .prepare_aggregate(wrong, contract.clone(), &baseline)
                    .unwrap_err(),
            );
            let trace = baseline.trace.lock().unwrap().clone();
            assert_eq!(trace, [(CompilePhase::FunctionSpecialization, 0)]);
            for cause in original_causes() {
                let earlier = Control {
                    trace: Mutex::default(),
                    refusal: Some((0, cause.clone())),
                };
                match fixture
                    .owner
                    .prepare_aggregate(wrong, contract.clone(), &earlier)
                    .unwrap_err()
                {
                    KernelFailure::CompileControl(actual) => {
                        assert_original_control(actual, &cause)
                    }
                    other => panic!("earlier COUNT preparation source changed: {other:?}"),
                }
                assert_eq!(*earlier.trace.lock().unwrap(), trace);
                let late = Control {
                    trace: Mutex::default(),
                    refusal: Some((trace.len(), cause)),
                };
                assert_prepare_semantic(
                    fixture
                        .owner
                        .prepare_aggregate(wrong, contract.clone(), &late)
                        .unwrap_err(),
                );
                assert_eq!(
                    *late.trace.lock().unwrap(),
                    trace,
                    "known exact-call error cannot start a later callback"
                );
                assert_eq!(Arc::strong_count(&contract), contract_arcs);
                assert_eq!(Arc::strong_count(&fixture.selected), selected_arcs);
            }
        }
    }
}

#[test]
fn count_public_success_keeps_positive_work_all_control_prefixes_sources_and_actual_prepared_owner_drop()
 {
    for arity in [0, 1] {
        let fixture = Fixture::new(arity);
        let input = fixture.input();
        let baseline = Control::default();
        fixture.owner.validate_and_refine(input, &baseline).unwrap();
        let refine_trace = baseline.trace.lock().unwrap().clone();
        assert!(refine_trace.len() >= 2);
        assert!(refine_trace.iter().any(|(_, units)| *units > 0));
        for stop in 0..refine_trace.len() {
            for cause in original_causes() {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((stop, cause.clone())),
                };
                match fixture
                    .owner
                    .validate_and_refine(input, &control)
                    .unwrap_err()
                {
                    FunctionEffectOwnerError::Control(actual) => {
                        assert_original_control(actual, &cause)
                    }
                    other => panic!("real COUNT refiner changed its control prefix: {other:?}"),
                }
                assert_eq!(*control.trace.lock().unwrap(), refine_trace[..=stop]);
            }
        }
        let contract = fixture.canonical();
        let original_arcs = Arc::strong_count(&contract);
        let baseline = Control::default();
        let prepared = fixture
            .owner
            .prepare_aggregate(input, contract.clone(), &baseline)
            .unwrap();
        assert!(Arc::ptr_eq(prepared.contract(), &contract));
        assert_eq!(Arc::strong_count(&contract), original_arcs + 1);
        let weak = Arc::downgrade(&prepared);
        drop(prepared);
        assert!(
            weak.upgrade().is_none(),
            "the actual prepared CountKernel exited"
        );
        drop(weak);
        assert_eq!(Arc::strong_count(&contract), original_arcs);
        let prepare_trace = baseline.trace.lock().unwrap().clone();
        assert!(prepare_trace.len() >= 2);
        assert!(prepare_trace.iter().any(|(_, units)| *units > 0));
        for stop in 0..prepare_trace.len() {
            for cause in original_causes() {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((stop, cause.clone())),
                };
                match fixture
                    .owner
                    .prepare_aggregate(input, contract.clone(), &control)
                    .unwrap_err()
                {
                    KernelFailure::CompileControl(actual) => {
                        assert_original_control(actual, &cause)
                    }
                    other => {
                        panic!("real COUNT prepared author changed its control prefix: {other:?}")
                    }
                }
                assert_eq!(*control.trace.lock().unwrap(), prepare_trace[..=stop]);
                // Some refusal points run after Arc<CountKernel> construction.
                // Its original contract must still leave with that real owner.
                assert_eq!(Arc::strong_count(&contract), original_arcs);
            }
        }
    }
}
