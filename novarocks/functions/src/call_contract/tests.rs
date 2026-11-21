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
    FunctionArgument, FunctionBindingError, FunctionBindingRequest, FunctionEffectOwner,
    FunctionEffectOwnerError, FunctionOverloadId, FunctionValueType, refine_call_effects,
};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileControlError, EvaluationDemand, EvaluationRegionId,
    ExpressionUseId, FunctionEffectDeclaration, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility,
    MAX_UNOBSERVED_COMPILE_WORK, ObservableEffects, SemanticParameterId, SemanticParameterRef,
    SemanticParameterValue,
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Allow;
impl PureCompileControl for Allow {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= MAX_UNOBSERVED_COMPILE_WORK);
        Ok(())
    }
}

struct Fixture {
    function: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    declaration: FunctionEffectDeclaration,
    environment: Vec<SemanticParameterRef>,
    parameters: SemanticParameters,
    refinements: AtomicUsize,
}
impl Fixture {
    fn new(argument_count: usize) -> Self {
        // Every key is a real admitted setting, with separate sparse identities.
        // The fixture has no CPU kernel or runtime capability.
        let values = [
            (0, SemanticParameterValue::StatementStartUtc(-17)),
            (
                u32::MAX,
                SemanticParameterValue::TimeZone("Asia/Shanghai".into()),
            ),
            (7, SemanticParameterValue::AllowThrowException(true)),
            (300, SemanticParameterValue::GroupConcatLegacy(true)),
            (19, SemanticParameterValue::GroupConcatMaxLen(-9)),
        ];
        let environment = values
            .iter()
            .map(|(id, value)| SemanticParameterRef {
                id: SemanticParameterId::new(*id),
                expected_key: value.key(),
            })
            .collect::<Vec<_>>();
        let parameters = SemanticParameters::try_new(
            values
                .into_iter()
                .map(|(id, value)| (SemanticParameterId::new(id), value))
                .chain([(
                    SemanticParameterId::new(88),
                    SemanticParameterValue::TimeZone("UTC".into()),
                )]),
        )
        .unwrap();
        let value = FunctionValueType::new(DataType::Int64, false);
        let arguments = vec![
            FunctionArgument::Value {
                value_type: value.clone(),
                constant: None
            };
            argument_count
        ];
        Self {
            function: FunctionId::try_new("fixture/call-contract/frozen-settings").unwrap(),
            selected: Arc::new(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("fixture/call-contract/frozen-settings-i64")
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
            uses: (0..argument_count)
                .map(|index| Some(ExpressionUseId::new(index as u32 + 1)))
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
                environment_dependencies: environment
                    .iter()
                    .map(|reference| reference.expected_key)
                    .collect(),
            },
            environment,
            parameters,
            refinements: AtomicUsize::new(0),
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: (ExpressionEffectContext {
                use_id: ExpressionUseId::new(u32::MAX),
                region: EvaluationRegionId::new(0),
                demand: EvaluationDemand::Value,
            })
            .into(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.function,
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
}
impl FunctionEffectOwner for Fixture {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if function != &self.function || selected.overload != self.selected.overload {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(&self.declaration)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.refinements.fetch_add(1, Ordering::Relaxed);
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        if input.selected != self.selected.as_ref()
            || input.request.arguments.len() != self.arguments.len()
        {
            return Err(FunctionBindingError::UnknownFunction.into());
        }
        for (argument, expected) in input
            .request
            .arguments
            .iter()
            .zip(&self.selected.argument_types)
        {
            if argument.argument_type() != *expected {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            work.step().map_err(FunctionEffectOwnerError::Control)?;
        }
        for reference in input.environment {
            input
                .parameters
                .require(*reference)
                .map_err(|_| FunctionBindingError::UnknownFunction)?;
            work.step().map_err(FunctionEffectOwnerError::Control)?;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
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

#[derive(Clone, Copy)]
enum Stop {
    Never,
    Entry(usize),
    ProjectionWork,
    Quantum,
}
struct Observe {
    stop: Stop,
    failure: CompileControlError,
    events: Mutex<Vec<u32>>,
    entries: AtomicUsize,
}
impl Observe {
    fn new(stop: Stop, failure: CompileControlError) -> Self {
        Self {
            stop,
            failure,
            events: Mutex::default(),
            entries: AtomicUsize::new(0),
        }
    }
}
impl PureCompileControl for Observe {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= MAX_UNOBSERVED_COMPILE_WORK);
        self.events.lock().unwrap().push(units);
        let entries = if units == 0 {
            self.entries.fetch_add(1, Ordering::Relaxed) + 1
        } else {
            self.entries.load(Ordering::Relaxed)
        };
        let stop = match self.stop {
            Stop::Never => false,
            Stop::Entry(entry) => units == 0 && entries == entry,
            // The nested observed projection starts after the constructor's
            // entry. With zero arguments, its six lookups are the first work
            // reported after that distinct entry; type work is still pending.
            Stop::ProjectionWork => entries == 2 && units > 0,
            Stop::Quantum => units == MAX_UNOBSERVED_COMPILE_WORK,
        };
        if stop {
            Err(self.failure.clone())
        } else {
            Ok(())
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

#[test]
fn exact_nonempty_frozen_parameter_closure_preserves_values_and_drops_unused_scope() {
    let fixture = Fixture::new(0);
    let input = fixture.input();
    let receipt = refine_call_effects(&fixture, input, &Allow).unwrap();
    let observer = Observe::new(Stop::Never, CompileControlError::Cancelled);
    let contract =
        FunctionCallContract::from_refined(input, &receipt, fixture.selected.clone(), &observer)
            .unwrap();
    assert_eq!(contract.parameters().entries().len(), 5);
    for reference in &fixture.environment {
        assert_eq!(
            contract.parameters().require(*reference).unwrap(),
            fixture.parameters.require(*reference).unwrap()
        );
    }
    assert!(
        contract
            .parameters()
            .get(SemanticParameterId::new(88))
            .is_none()
    );
    assert_eq!(
        contract.parameters().get(SemanticParameterId::new(0)),
        Some(&SemanticParameterValue::StatementStartUtc(-17))
    );
    assert_eq!(
        contract
            .parameters()
            .get(SemanticParameterId::new(u32::MAX)),
        Some(&SemanticParameterValue::TimeZone("Asia/Shanghai".into()))
    );
    assert_eq!(
        contract.parameters().get(SemanticParameterId::new(19)),
        Some(&SemanticParameterValue::GroupConcatMaxLen(-9))
    );
    assert!(Arc::ptr_eq(contract.selected_owner(), &fixture.selected));
    assert_eq!(contract.context(), input.context);
    assert_eq!(contract.effects(), receipt.facts());
    assert_eq!(fixture.refinements.load(Ordering::Relaxed), 1);
    assert_eq!(observer.entries.load(Ordering::Relaxed), 2);
    assert!(observer.events.lock().unwrap().contains(&5));
}

#[test]
fn constructor_and_projection_entries_preserve_all_typed_control_failures() {
    let fixture = Fixture::new(0);
    let input = fixture.input();
    let receipt = refine_call_effects(&fixture, input, &Allow).unwrap();
    for (failure, expected) in failures() {
        for entry in [1, 2] {
            let observer = Observe::new(Stop::Entry(entry), failure.clone());
            assert_eq!(
                FunctionCallContract::from_refined(
                    input,
                    &receipt,
                    fixture.selected.clone(),
                    &observer
                )
                .unwrap_err(),
                expected
            );
            assert_eq!(observer.entries.load(Ordering::Relaxed), entry);
            assert!(
                observer
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|units| *units == 0)
            );
        }
    }
    assert_eq!(fixture.refinements.load(Ordering::Relaxed), 1);
}

#[test]
fn nonempty_projection_work_preserves_control_failures_without_invalid_program_flattening() {
    let fixture = Fixture::new(0);
    let input = fixture.input();
    let receipt = refine_call_effects(&fixture, input, &Allow).unwrap();
    for (failure, expected) in failures() {
        let observer = Observe::new(Stop::ProjectionWork, failure);
        assert_eq!(
            FunctionCallContract::from_refined(
                input,
                &receipt,
                fixture.selected.clone(),
                &observer
            )
            .unwrap_err(),
            expected
        );
        assert_eq!(observer.entries.load(Ordering::Relaxed), 2);
        assert_eq!(observer.events.lock().unwrap().as_slice(), &[0, 0, 5]);
    }
}

#[test]
fn wide_exact_signature_stops_at_256_work_before_projecting_the_closure() {
    let fixture = Fixture::new(300);
    let input = fixture.input();
    let receipt = refine_call_effects(&fixture, input, &Allow).unwrap();
    for (failure, expected) in failures() {
        let observer = Observe::new(Stop::Quantum, failure);
        assert_eq!(
            FunctionCallContract::from_refined(
                input,
                &receipt,
                fixture.selected.clone(),
                &observer
            )
            .unwrap_err(),
            expected
        );
        assert_eq!(observer.entries.load(Ordering::Relaxed), 1);
        assert_eq!(
            observer.events.lock().unwrap().as_slice(),
            &[0, MAX_UNOBSERVED_COMPILE_WORK]
        );
    }
    assert_eq!(fixture.refinements.load(Ordering::Relaxed), 1);
}

#[test]
fn value_equal_foreign_parameter_table_is_not_the_refined_input_borrow() {
    let fixture = Fixture::new(0);
    let input = fixture.input();
    let receipt = refine_call_effects(&fixture, input, &Allow).unwrap();
    let foreign = fixture.parameters.clone();
    assert_eq!(foreign, fixture.parameters);
    let changed = CallEffectInput {
        parameters: &foreign,
        ..input
    };
    assert!(matches!(
        FunctionCallContract::from_refined(changed, &receipt, fixture.selected.clone(), &Allow),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(fixture.refinements.load(Ordering::Relaxed), 1);
}

use novarocks_type_contract::ExpressionEffectContext;
