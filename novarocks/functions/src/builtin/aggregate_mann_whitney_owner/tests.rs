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
    CompileControlError, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
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
        if let Some((stop, cause)) = self.refusal.clone() {
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
            region: EvaluationRegionId::new(19),
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
                    region: EvaluationRegionId::new(73),
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
                        CallProofScope::Region(fixture.context().region),
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
                CallProofScope::Region(EvaluationRegionId::new(999)),
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
                    refusal: Some((stop, cause.clone())),
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

// The real staged catalogue entry must retain the original aggregate binding
// and all phase/state facts, rather than use its unconverted-owner default.
#[test]
fn mann_whitney_checked_creator_preserves_four_phases_and_original_control() {
    use novarocks_type_contract::{ValueSchemaCreation, ValueTypeStorageLifetime};
    use std::{alloc::Layout, cell::Cell};
    let fixture = Fixture::new(&[DataType::Float64, DataType::Boolean]);
    let policy = ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    };
    for phase in [
        AggregateKernelPhase::Single,
        AggregateKernelPhase::Partial,
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        let context = fixture.context();
        let state_type = fixture
            .selected
            .aggregate
            .as_ref()
            .unwrap()
            .intermediate_type
            .clone();
        let argument_uses = if phase.consumes_logical_arguments() {
            CallArgumentUses::SelectedChannels(&fixture.uses)
        } else {
            CallArgumentUses::AggregateMerge {
                phase,
                state_context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(71),
                    region: EvaluationRegionId::new(73),
                    demand: EvaluationDemand::Value,
                },
                state_input_type: &state_type,
            }
        };
        let input = || CallEffectInput {
            function_id: &fixture.id,
            kind: FunctionKind::Aggregate,
            selected: &fixture.selected,
            request: FunctionBindingRequest {
                arguments: &fixture.arguments,
                logical_argument_count: fixture.arguments.len(),
                expected_result_type: None,
            },
            argument_uses,
            context,
            parameters: &fixture.params,
            environment: &[],
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Region(context.region),
        };
        let options = || PureCallPreparation::Aggregate {
            arguments: ScopedExpressionEffects::pure_value(context),
            options: AggregatePreparationOptions {
                phase,
                distinct: false,
                order_keys: Arc::from([]),
                state_interpretation: None,
                state_input_type: (!phase.consumes_logical_arguments())
                    .then_some(state_type.clone()),
            },
        };
        let original = fixture
            .prepare(
                phase,
                false,
                CallProofScope::Region(context.region),
                &Control::default(),
            )
            .unwrap();
        let frozen = original.prepared().call_contract().effects();
        let used = Cell::new(0usize);
        let charge = |bytes| {
            let next = used
                .get()
                .checked_add(bytes)
                .filter(|bytes| *bytes <= 65_536)
                .ok_or(())?;
            used.set(next);
            Ok::<_, ()>(())
        };
        let mut exact = |layout: Layout| charge(layout.size());
        let mut opaque = |bytes| charge(bytes);
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let checked = fixture
            .catalogue
            .prepare_selected_checked_with_creator_in(
                input(),
                fixture.selected.clone(),
                Some(frozen),
                options(),
                &policy,
                &mut creation,
                &Control::default(),
            )
            .unwrap();
        let PreparedPureKernel::Aggregate(handle) = checked.prepared() else {
            panic!("original aggregate ABI")
        };
        assert_eq!(handle.contract().phase(), phase);
        assert_eq!(handle.contract().intermediate_type(), &state_type);
        assert_eq!(handle.contract().call().effects(), frozen);
        assert_eq!(
            handle.contract().call().selected(),
            fixture.selected.as_ref()
        );
        assert_eq!(
            handle.memory_policy(),
            AggregateStateMemoryPolicy::AllocationTracked
        );
        let stop = Control {
            calls: Mutex::new(0),
            refusal: Some((0, CompileControlError::Cancelled)),
        };
        let error = fixture
            .catalogue
            .prepare_selected_checked_with_creator_in(
                input(),
                fixture.selected.clone(),
                Some(frozen),
                options(),
                &policy,
                &mut creation,
                &stop,
            )
            .expect_err("the original control must refuse checked preparation");
        assert!(matches!(
            error,
            CheckedCallPreparationFailure::Preparation(CheckedOwnerFailure::Original(
                FunctionSpecializationFailure::Control(CompileControlError::Cancelled)
            ))
        ));
        assert_eq!(*stop.calls.lock().unwrap(), 1);
    }
}

#[test]
fn mann_whitney_borrowed_primitive_checks_preserve_exact_types_and_control_prefixes() {
    let profiles = [
        FunctionValueType::new(DataType::Utf8, true),
        FunctionValueType::new(DataType::Utf8, false),
        FunctionValueType::new(DataType::LargeUtf8, true),
        FunctionValueType::new(DataType::Binary, true),
        FunctionValueType::new(DataType::LargeBinary, true),
        FunctionValueType::try_with_logical_type(
            DataType::Utf8,
            true,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap(),
        FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            novarocks_type_contract::ValueLogicalType::Bitmap,
        )
        .unwrap(),
    ];
    for carrier in [DataType::Utf8, DataType::Binary] {
        let original = FunctionValueType::new(carrier.clone(), true);
        for actual in &profiles {
            let mut original_calls = 0;
            let expected = original
                .exactly_equals_observed::<KernelFailure>(actual, || {
                    original_calls += 1;
                    Ok(())
                })
                .unwrap();
            let mut borrowed_calls = 0;
            assert_eq!(
                original_nullable_type_matches::<KernelFailure>(actual, &carrier, || {
                    borrowed_calls += 1;
                    Ok(())
                })
                .unwrap(),
                expected
            );
            assert_eq!(borrowed_calls, original_calls);
            for stop in 0..original_calls {
                let mut original_at = 0;
                let original_error = original
                    .exactly_equals_observed::<KernelFailure>(actual, || {
                        let at = original_at;
                        original_at += 1;
                        if at == stop {
                            Err(KernelFailure::Cancelled)
                        } else {
                            Ok(())
                        }
                    })
                    .unwrap_err();
                let mut borrowed_at = 0;
                let borrowed_error =
                    original_nullable_type_matches::<KernelFailure>(actual, &carrier, || {
                        let at = borrowed_at;
                        borrowed_at += 1;
                        if at == stop {
                            Err(KernelFailure::Cancelled)
                        } else {
                            Ok(())
                        }
                    })
                    .unwrap_err();
                assert_eq!(borrowed_error, original_error);
                assert_eq!(borrowed_at, original_at);
            }
        }
    }
}
