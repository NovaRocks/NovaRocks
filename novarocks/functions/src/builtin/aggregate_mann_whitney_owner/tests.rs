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
use novarocks_type_contract::{
    CompileControlError, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
    ExpressionEffectContext, ExpressionUseId, SemanticParameters,
};
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    calls: Mutex<usize>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut calls = self.calls.lock().unwrap();
        let at = *calls;
        *calls += 1;
        if let Some((stop, cause)) = self.refusal {
            assert!(at <= stop, "callback after primary compile refusal");
            if at == stop {
                return Err(cause);
            }
        }
        Ok(())
    }
}
struct Fixture {
    catalogue: EngineFunctionCatalog,
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    params: SemanticParameters,
}
impl Fixture {
    fn new(types: &[DataType]) -> Self {
        let catalogue = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
        let arguments: Vec<_> = types
            .iter()
            .cloned()
            .map(|ty| FunctionArgument::Value {
                value_type: FunctionValueType::new(ty, true),
                constant: None,
            })
            .collect();
        let bound = catalogue
            .resolve_bound_user(
                "mann_whitney_u_test",
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: arguments.len(),
                    expected_result_type: None,
                },
                &Control::default(),
            )
            .unwrap();
        Self {
            catalogue,
            id: bound.function_id,
            selected: Arc::new(bound.selected),
            uses: (0..arguments.len())
                .map(|i| Some(ExpressionUseId::new(i as u32 + 1)))
                .collect(),
            arguments,
            params: SemanticParameters::default(),
        }
    }
    fn context(&self) -> ExpressionEffectContext {
        ExpressionEffectContext {
            use_id: ExpressionUseId::new(17),
            domain: EvaluationDomainId::new(19),
            demand: EvaluationDemand::Value,
        }
    }
    fn prepare(
        &self,
        phase: AggregateKernelPhase,
        distinct: bool,
        scope: CallProofScope,
        control: &Control,
    ) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
        let state_type = FunctionValueType::new(DataType::Binary, true);
        let context = self.context();
        let argument_uses = if phase.consumes_logical_arguments() {
            CallArgumentUses::SelectedChannels(&self.uses)
        } else {
            CallArgumentUses::AggregateMerge {
                phase,
                state_context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(71),
                    domain: EvaluationDomainId::new(73),
                    demand: EvaluationDemand::Value,
                },
                state_input_type: &state_type,
            }
        };
        self.catalogue.prepare_fresh_selected(
            CallEffectInput {
                function_id: &self.id,
                kind: FunctionKind::Aggregate,
                selected: &self.selected,
                request: FunctionBindingRequest {
                    arguments: &self.arguments,
                    logical_argument_count: self.arguments.len(),
                    expected_result_type: None,
                },
                argument_uses,
                context,
                parameters: &self.params,
                environment: &[],
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: scope,
            },
            self.selected.clone(),
            PureCallPreparation::Aggregate {
                arguments: ScopedExpressionEffects::pure_value(context),
                options: AggregatePreparationOptions {
                    phase,
                    distinct,
                    order_keys: Arc::from([]),
                    state_interpretation: None,
                    state_input_type: (!phase.consumes_logical_arguments())
                        .then_some(state_type.clone()),
                },
            },
            control,
        )
    }
}
#[test]
fn mann_whitney_owner_original_any_profiles_and_four_phases_are_installed() {
    for value in [
        DataType::Null,
        DataType::Utf8,
        DataType::UInt64,
        DataType::FixedSizeBinary(16),
        DataType::Decimal128(20, 2),
    ] {
        for arity in 2..=4 {
            let mut types = vec![value.clone(), DataType::Boolean];
            if arity >= 3 {
                types.push(DataType::Utf8);
            }
            if arity == 4 {
                types.push(DataType::Int16);
            }
            let fixture = Fixture::new(&types);
            let declaration = fixture
                .catalogue
                .pure_overload_declaration_observed(
                    &fixture.id,
                    FunctionKind::Aggregate,
                    &fixture.selected.overload,
                    &Control::default(),
                )
                .unwrap();
            assert_eq!(declaration.implementation().abi, PureKernelAbi::AggregateV1);
            assert_eq!(
                declaration.implementation().implementation.as_str(),
                "builtin.aggregate/mann_whitney_u_test/selected-v1"
            );
            for phase in [
                AggregateKernelPhase::Single,
                AggregateKernelPhase::Partial,
                AggregateKernelPhase::Intermediate,
                AggregateKernelPhase::Final,
            ] {
                let prepared = fixture
                    .prepare(
                        phase,
                        false,
                        CallProofScope::Domain(fixture.context().domain),
                        &Control::default(),
                    )
                    .unwrap();
                let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
                    panic!("aggregate ABI")
                };
                assert_eq!(handle.contract().phase(), phase);
                assert_eq!(
                    handle.memory_policy(),
                    AggregateStateMemoryPolicy::AllocationTracked
                );
                assert_eq!(handle.contract().call().effects().environment.as_ref(), &[]);
                assert_eq!(
                    handle.contract().call().selected(),
                    fixture.selected.as_ref()
                );
            }
        }
    }
}
#[test]
fn mann_whitney_owner_rejects_foreign_scope_distinct_and_stale_full_result() {
    let mut fixture = Fixture::new(&[DataType::Int64, DataType::Boolean]);
    assert!(
        fixture
            .prepare(
                AggregateKernelPhase::Single,
                false,
                CallProofScope::Domain(EvaluationDomainId::new(999)),
                &Control::default()
            )
            .is_err()
    );
    assert!(
        fixture
            .prepare(
                AggregateKernelPhase::Single,
                true,
                CallProofScope::Unconditional,
                &Control::default()
            )
            .is_err()
    );
    Arc::make_mut(&mut fixture.selected).result_type =
        FunctionResultType::Scalar(FunctionValueType::new(DataType::Utf8, false));
    assert!(
        fixture
            .prepare(
                AggregateKernelPhase::Single,
                false,
                CallProofScope::Unconditional,
                &Control::default()
            )
            .is_err()
    );
}
#[test]
fn mann_whitney_owner_each_actual_compile_frontier_preserves_primary_cause() {
    let fixture = Fixture::new(&[
        DataType::Int64,
        DataType::Boolean,
        DataType::Utf8,
        DataType::Int64,
    ]);
    for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Final] {
        let good = Control::default();
        fixture
            .prepare(phase, false, CallProofScope::Unconditional, &good)
            .unwrap();
        let count = *good.calls.lock().unwrap();
        assert!(count > 0);
        for stop in 0..count {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    calls: Mutex::new(0),
                    refusal: Some((stop, cause)),
                };
                let error = fixture
                    .prepare(phase, false, CallProofScope::Unconditional, &control)
                    .unwrap_err();
                let actual = match error {
                    FunctionSpecializationFailure::Control(e) => e,
                    FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                        CompileControlError::Cancelled
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
                        CompileControlError::DeadlineExceeded
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
                        CompileControlError::ResourceExhausted
                    }
                    other => panic!("primary compile cause replaced: {other:?}"),
                };
                assert_eq!(actual, cause);
                assert_eq!(*control.calls.lock().unwrap(), stop + 1);
            }
        }
    }
}
