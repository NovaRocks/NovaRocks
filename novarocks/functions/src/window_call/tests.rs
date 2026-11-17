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
    AggregateBindingSelection, AggregateStateFormatIdentity, CallEffectInput, FunctionArgument,
    FunctionBindingError, FunctionBindingRequest, FunctionBindingSelection, FunctionEffectOwner,
    FunctionEffectOwnerError, FunctionId, FunctionOverloadId, RowDataError, SelectedValues,
    refine_call_effects,
};
use arrow_array::{ArrayRef, Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallEffects, CallProofScope, CompileControlError, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionEffectDeclaration,
    FunctionFailureBehavior, FunctionNullBehavior, FunctionVolatility, ObservableEffects,
    SemanticParameters, WindowFrameExclusion,
};
use std::{sync::Mutex, time::Duration};

fn assert_first_quantum_failure(work: &[u32]) {
    let (quantum, entries) = work.split_last().expect("observed control failure");
    assert_eq!(*quantum, 256);
    // Nested admitted constant checks may each observe their own entry. The
    // first positive quantum must still fail immediately, with no later work.
    assert!(!entries.is_empty());
    assert!(entries.iter().all(|units| *units == 0));
}

#[derive(Default)]
struct CompileControl {
    failure: Option<CompileControlError>,
    positive_only: bool,
    work: Mutex<Vec<u32>>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        self.work.lock().unwrap().push(units);
        if (!self.positive_only || units > 0)
            && let Some(failure) = self.failure.clone()
        {
            Err(failure)
        } else {
            Ok(())
        }
    }
}
#[derive(Default)]
struct RuntimeControl {
    failure: Option<KernelFailure>,
    positive_only: bool,
    work: Mutex<Vec<u32>>,
}
impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        self.work.lock().unwrap().push(units);
        if (!self.positive_only || units > 0)
            && let Some(failure) = &self.failure
        {
            Err(failure.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("window contract validation must not wait")
    }
}
struct Fixture {
    kind: FunctionKind,
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    argument_uses: Vec<Option<ExpressionUseId>>,
    logical: usize,
    declaration: FunctionEffectDeclaration,
    parameters: SemanticParameters,
}
impl Fixture {
    fn new(logical: &[FunctionValueType], order: &[FunctionValueType], kind: FunctionKind) -> Self {
        let arguments = logical
            .iter()
            .chain(order)
            .cloned()
            .map(|value_type| FunctionArgument::Value {
                value_type,
                constant: None,
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            kind,
            FunctionKind::Window | FunctionKind::Aggregate
        ));
        Self {
            kind,
            id: FunctionId::try_new("fixture/window-contract/exact-owner").unwrap(),
            selected: Arc::new(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new(
                    "fixture/window-contract/exact-logical-signature",
                )
                .unwrap(),
                argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                    arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Box<[_]>>(),
                ),
                result_type: FunctionResultType::Scalar(i64_type(false)),
                aggregate: (kind == FunctionKind::Aggregate).then(|| AggregateBindingSelection {
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    intermediate_type: i64_type(false),
                    state_format: AggregateStateFormatIdentity::try_new(
                        "fixture/window-contract/state-v1",
                    )
                    .unwrap(),
                }),
            }),
            argument_uses: (0..arguments.len())
                .map(|ordinal| Some(ExpressionUseId::new(ordinal as u32 + 1)))
                .collect(),
            arguments,
            logical: logical.len(),
            declaration: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: if kind == FunctionKind::Window {
                    ArgumentControl::Window
                } else {
                    ArgumentControl::Aggregate
                },
                instance_state: if kind == FunctionKind::Window {
                    FunctionInstanceState::WindowPartition
                } else {
                    FunctionInstanceState::AggregateInstance
                },
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::new([]),
            },
            parameters: SemanticParameters::default(),
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(0),
                region: EvaluationRegionId::new(9),
                demand: EvaluationDemand::Value,
            },
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.argument_uses),
            function_id: &self.id,
            kind: self.kind,
            selected: self.selected.as_ref(),
            request: FunctionBindingRequest {
                expected_result_type: None,
                arguments: &self.arguments,
                logical_argument_count: self.logical,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        }
    }
    fn call(&self) -> Arc<FunctionCallContract> {
        let input = self.input();
        let receipt = refine_call_effects(self, input, &CompileControl::default()).unwrap();
        Arc::new(
            FunctionCallContract::from_refined(
                input,
                &receipt,
                self.selected.clone(),
                &CompileControl::default(),
            )
            .unwrap(),
        )
    }
}
impl FunctionEffectOwner for Fixture {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        id: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if id != &self.id || selected != self.selected.as_ref() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(&self.declaration)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        if input.function_id != &self.id
            || input.kind != self.kind
            || input.selected != self.selected.as_ref()
            || input.request.logical_argument_count != self.logical
            || input.request.arguments.len() != self.arguments.len()
            || !input.environment.is_empty()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "fixture exact window or aggregate call differs".into(),
            )
            .into());
        }
        for (argument, expected) in input.request.arguments.iter().zip(&self.arguments) {
            if !argument.equals_observed(expected, CompilePhase::FunctionSpecialization, control)? {
                return Err(FunctionBindingError::InvalidBinding(
                    "fixture exact argument differs".into(),
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
            argument_control: self.declaration.argument_control,
            instance_state: self.declaration.instance_state,
            observable_effects: self.declaration.observable_effects,
            environment: Box::new([]),
            proof_scope: input.proof_scope,
        })
    }
}

fn i64_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}
fn options() -> WindowCallOptions {
    WindowCallOptions::try_new(None, false, &CompileControl::default()).unwrap()
}
fn window(fixture: &Fixture) -> WindowCallContract {
    WindowCallContract::try_window(fixture.call(), options(), &CompileControl::default()).unwrap()
}
#[test]
fn sealed_window_call_preserves_exact_binding_and_scalar_partition_input() {
    let fixture = Fixture::new(&[i64_type(false)], &[], FunctionKind::Window);
    let call = fixture.call();
    let contract =
        WindowCallContract::try_window(call.clone(), options(), &CompileControl::default())
            .unwrap();
    assert!(Arc::ptr_eq(contract.call(), &call));
    assert!(contract.aggregate().is_none());
    let scalar: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let args = [EvaluatedArgument::Scalar(&scalar)];
    let input =
        FullPartitionWindowInput::try_new(&contract, 3, &args, &[], &RuntimeControl::default())
            .unwrap();
    assert_eq!(input.partition_rows(), 3);
    assert!(std::ptr::eq(input.contract(), &contract));
    assert_eq!(input.logical_arguments().len(), 1);
    assert!(input.order_arguments().is_empty());
    input
        .validate_output_selection(Selection::try_sparse(3, &[1]).unwrap())
        .unwrap();
}

fn aggregate_contract(
    fixture: &Fixture,
    phase: AggregateKernelPhase,
    distinct: bool,
    order_keys: Arc<[AggregateOrderKey]>,
) -> Arc<AggregateCallContract> {
    Arc::new(
        AggregateCallContract::try_new(
            fixture.call(),
            phase,
            distinct,
            order_keys,
            (!phase.consumes_logical_arguments()).then(|| i64_type(false)),
            &CompileControl::default(),
        )
        .unwrap(),
    )
}
fn assert_invalid<T: std::fmt::Debug>(result: Result<T, KernelFailure>) {
    assert!(matches!(
        result.unwrap_err(),
        ref cause if crate::kernel_control::test_contract::input(cause)
    ));
}
fn selected<'a>(
    selection: Selection<'a>,
    array: ArrayRef,
    errors: Box<[RowDataError]>,
) -> SelectedValues<'a> {
    SelectedValues::try_new(selection, array.data_type(), array.clone(), errors).unwrap()
}
#[test]
fn window_and_single_aggregate_sources_are_distinct_and_function_order_stays_separate() {
    let window_fixture = Fixture::new(&[i64_type(false)], &[], FunctionKind::Window);
    let window_call = window_fixture.call();
    assert_invalid(AggregateCallContract::try_new(
        window_call,
        AggregateKernelPhase::Single,
        false,
        Arc::from([]),
        None,
        &CompileControl::default(),
    ));
    let fixture = Fixture::new(
        &[i64_type(true)],
        &[FunctionValueType::new(DataType::Int32, false)],
        FunctionKind::Aggregate,
    );
    assert_invalid(WindowCallContract::try_window(
        fixture.call(),
        options(),
        &CompileControl::default(),
    ));
    let keys: Arc<[AggregateOrderKey]> = Arc::from([AggregateOrderKey {
        ascending: false,
        nulls_first: true,
    }]);
    let aggregate = aggregate_contract(&fixture, AggregateKernelPhase::Single, true, keys.clone());
    let frame = WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowBound::Preceding(2),
        end: WindowBound::CurrentRow,
        exclusion: WindowFrameExclusion::Ties,
    };
    let prepared =
        WindowCallOptions::try_new(Some(frame), true, &CompileControl::default()).unwrap();
    let contract =
        WindowCallContract::try_aggregate(aggregate.clone(), prepared, &CompileControl::default())
            .unwrap();
    assert!(Arc::ptr_eq(contract.aggregate().unwrap(), &aggregate));
    assert!(Arc::ptr_eq(contract.call(), aggregate.call()));
    assert!(contract.aggregate().unwrap().distinct());
    assert_eq!(contract.function_order_keys(), keys.as_ref());
    assert_eq!(
        contract
            .logical_argument_types()
            .cloned()
            .collect::<Vec<_>>(),
        [i64_type(true)]
    );
    assert_eq!(
        contract.order_argument_types().cloned().collect::<Vec<_>>(),
        [FunctionValueType::new(DataType::Int32, false)]
    );
    assert_eq!(contract.options().frame(), Some(&frame));
    assert!(contract.options().ignore_nulls());
    // OVER ordering is deliberately absent from this call-level API. Its
    // function ORDER BY retains its own column channel and exact modifiers.
    let logical: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    let order: ArrayRef = Arc::new(Int32Array::from(vec![30, 10, 20]));
    let args = [EvaluatedArgument::Column(&logical)];
    let order_args = [EvaluatedArgument::Column(&order)];
    let input = FullPartitionWindowInput::try_new(
        &contract,
        3,
        &args,
        &order_args,
        &RuntimeControl::default(),
    )
    .unwrap();
    assert_eq!(input.order_arguments().len(), 1);
    assert_invalid(FullPartitionWindowInput::try_new(
        &contract,
        3,
        &args,
        &[],
        &RuntimeControl::default(),
    ));
    for phase in [
        AggregateKernelPhase::Partial,
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        let phase_keys = if phase.consumes_logical_arguments() {
            keys.clone()
        } else {
            Arc::from([])
        };
        let non_single = aggregate_contract(&fixture, phase, false, phase_keys);
        assert_invalid(WindowCallContract::try_aggregate(
            non_single,
            options(),
            &CompileControl::default(),
        ));
    }
}

#[test]
fn prepared_frame_vocabulary_and_absence_are_preserved_without_capability_claims() {
    for ignore in [false, true] {
        let absent = WindowCallOptions::try_new(None, ignore, &CompileControl::default()).unwrap();
        assert_eq!(absent.frame(), None);
        assert_eq!(absent.ignore_nulls(), ignore);
        for units in [
            WindowFrameUnits::Rows,
            WindowFrameUnits::Range,
            WindowFrameUnits::Groups,
        ] {
            for exclusion in [
                WindowFrameExclusion::NoOthers,
                WindowFrameExclusion::CurrentRow,
                WindowFrameExclusion::Group,
                WindowFrameExclusion::Ties,
            ] {
                let frame = WindowFrame {
                    units,
                    start: WindowBound::UnboundedPreceding,
                    end: WindowBound::CurrentRow,
                    exclusion,
                };
                let prepared =
                    WindowCallOptions::try_new(Some(frame), ignore, &CompileControl::default())
                        .unwrap();
                assert_eq!(prepared.frame(), Some(&frame));
                assert_eq!(prepared.ignore_nulls(), ignore);
            }
        }
    }
    for units in [WindowFrameUnits::Rows, WindowFrameUnits::Groups] {
        let frame = WindowFrame {
            units,
            start: WindowBound::Preceding(u64::MAX),
            end: WindowBound::Following(u64::MAX),
            exclusion: WindowFrameExclusion::NoOthers,
        };
        assert_eq!(
            WindowCallOptions::try_new(Some(frame), false, &CompileControl::default())
                .unwrap()
                .frame(),
            Some(&frame)
        );
    }
}
#[test]
fn zero_reversed_and_range_offset_frames_fail_closed() {
    for (start, end) in [
        (WindowBound::Preceding(0), WindowBound::CurrentRow),
        (WindowBound::CurrentRow, WindowBound::Following(0)),
        (WindowBound::Preceding(1), WindowBound::Preceding(2)),
        (WindowBound::Following(2), WindowBound::Following(1)),
        (WindowBound::Following(1), WindowBound::CurrentRow),
        (
            WindowBound::UnboundedFollowing,
            WindowBound::UnboundedFollowing,
        ),
        (
            WindowBound::UnboundedPreceding,
            WindowBound::UnboundedPreceding,
        ),
    ] {
        assert_invalid(WindowCallOptions::try_new(
            Some(WindowFrame {
                units: WindowFrameUnits::Rows,
                start,
                end,
                exclusion: WindowFrameExclusion::NoOthers,
            }),
            false,
            &CompileControl::default(),
        ));
    }
    for (start, end) in [
        (WindowBound::Preceding(1), WindowBound::CurrentRow),
        (WindowBound::CurrentRow, WindowBound::Following(1)),
    ] {
        assert_invalid(WindowCallOptions::try_new(
            Some(WindowFrame {
                units: WindowFrameUnits::Range,
                start,
                end,
                exclusion: WindowFrameExclusion::NoOthers,
            }),
            false,
            &CompileControl::default(),
        ));
    }
}

#[test]
fn complete_columns_and_dense_selected_columns_allow_independent_output_selection() {
    let fixture = Fixture::new(&[i64_type(false)], &[], FunctionKind::Window);
    let contract = window(&fixture);
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let dense = selected(Selection::all(3), array.clone(), Box::new([]));
    for argument in [
        EvaluatedArgument::Column(&array),
        EvaluatedArgument::SelectedColumn(&dense),
    ] {
        let args = [argument];
        let input =
            FullPartitionWindowInput::try_new(&contract, 3, &args, &[], &RuntimeControl::default())
                .unwrap();
        for output in [
            Selection::all(3),
            Selection::try_sparse(3, &[0, 2]).unwrap(),
            Selection::try_sparse(3, &[]).unwrap(),
        ] {
            input.validate_output_selection(output).unwrap();
            assert_eq!(input.partition_rows(), 3);
            assert_eq!(input.logical_arguments()[0].array().len(), 3);
        }
        assert_invalid(input.validate_output_selection(Selection::all(2)));
        assert_invalid(input.validate_output_selection(Selection::try_sparse(4, &[0, 2]).unwrap()));
    }
    assert!(Selection::try_sparse(3, &[3]).is_err());
    assert!(Selection::try_sparse(3, &[1, 1]).is_err());
    let empty: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    let args = [EvaluatedArgument::Column(&empty)];
    FullPartitionWindowInput::try_new(&contract, 0, &args, &[], &RuntimeControl::default())
        .unwrap()
        .validate_output_selection(Selection::all(0))
        .unwrap();
    let zero_arg = window(&Fixture::new(&[], &[], FunctionKind::Window));
    FullPartitionWindowInput::try_new(&zero_arg, 3, &[], &[], &RuntimeControl::default()).unwrap();
}
#[test]
fn sparse_errored_wrong_length_and_wrong_type_inputs_cannot_impersonate_partition() {
    let fixture = Fixture::new(&[i64_type(true)], &[], FunctionKind::Window);
    let contract = window(&fixture);
    let compact: ArrayRef = Arc::new(Int64Array::from(vec![1, 3]));
    let sparse = selected(
        Selection::try_sparse(3, &[0, 2]).unwrap(),
        compact,
        Box::new([]),
    );
    let errored_array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    let errored = selected(
        Selection::all(3),
        errored_array,
        Box::from([RowDataError::new(1, "required input failed")]),
    );
    let short: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let wrong: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let non_scalar: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    for argument in [
        EvaluatedArgument::SelectedColumn(&sparse),
        EvaluatedArgument::SelectedColumn(&errored),
        EvaluatedArgument::Column(&short),
        EvaluatedArgument::Column(&wrong),
        EvaluatedArgument::Scalar(&non_scalar),
    ] {
        assert_invalid(FullPartitionWindowInput::try_new(
            &contract,
            3,
            &[argument],
            &[],
            &RuntimeControl::default(),
        ));
    }
    assert_invalid(FullPartitionWindowInput::try_new(
        &contract,
        3,
        &[],
        &[],
        &RuntimeControl::default(),
    ));
    assert_invalid(FullPartitionWindowInput::try_new(
        &contract,
        3,
        &[EvaluatedArgument::Scalar(&short); 2],
        &[],
        &RuntimeControl::default(),
    ));
}
#[test]
fn null_on_an_unoutput_partition_row_still_violates_nonnullable_input() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    let args = [EvaluatedArgument::Column(&array)];
    let output = Selection::try_sparse(3, &[0, 2]).unwrap();
    let nullable = window(&Fixture::new(&[i64_type(true)], &[], FunctionKind::Window));
    FullPartitionWindowInput::try_new(&nullable, 3, &args, &[], &RuntimeControl::default())
        .unwrap()
        .validate_output_selection(output)
        .unwrap();
    let nonnullable = window(&Fixture::new(&[i64_type(false)], &[], FunctionKind::Window));
    assert_invalid(FullPartitionWindowInput::try_new(
        &nonnullable,
        3,
        &args,
        &[],
        &RuntimeControl::default(),
    ));
    let null_scalar: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>]));
    assert_invalid(FullPartitionWindowInput::try_new(
        &nonnullable,
        3,
        &[EvaluatedArgument::Scalar(&null_scalar)],
        &[],
        &RuntimeControl::default(),
    ));
}

#[test]
fn approximate_top_k_shaped_extra_arguments_are_complete_columns_not_assumed_constants() {
    // Only the three logical channels are modeled here. This fixture is not
    // the production APPROX_TOP_K owner, implementation or modifier receipt.
    let fixture = Fixture::new(
        &[i64_type(false), i64_type(false), i64_type(false)],
        &[],
        FunctionKind::Aggregate,
    );
    assert!(
        fixture
            .arguments
            .iter()
            .all(|argument| matches!(argument, FunctionArgument::Value { constant: None, .. }))
    );
    let single = aggregate_contract(&fixture, AggregateKernelPhase::Single, false, Arc::from([]));
    let contract =
        WindowCallContract::try_aggregate(single, options(), &CompileControl::default()).unwrap();
    let data: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let k: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let counters: ArrayRef = Arc::new(Int64Array::from(vec![100, 200, 300]));
    let args = [
        EvaluatedArgument::Column(&data),
        EvaluatedArgument::Column(&k),
        EvaluatedArgument::Column(&counters),
    ];
    let input =
        FullPartitionWindowInput::try_new(&contract, 3, &args, &[], &RuntimeControl::default())
            .unwrap();
    assert_eq!(input.logical_arguments().len(), 3);
    assert!(
        input
            .logical_arguments()
            .iter()
            .all(|argument| matches!(argument, EvaluatedArgument::Column(_)))
    );
    input
        .validate_output_selection(Selection::try_sparse(3, &[2]).unwrap())
        .unwrap();
}

#[test]
fn compile_control_failures_preserve_entry_and_positive_work_classification() {
    let fixture = Fixture::new(&[i64_type(false)], &[], FunctionKind::Window);
    let call = fixture.call();
    let aggregate_fixture = Fixture::new(&[i64_type(false)], &[], FunctionKind::Aggregate);
    let single = aggregate_contract(
        &aggregate_fixture,
        AggregateKernelPhase::Single,
        false,
        Arc::from([]),
    );
    let wide_call = Fixture::new(&vec![i64_type(false); 300], &[], FunctionKind::Window).call();
    for (failure, expected) in [
        (CompileControlError::Cancelled, KernelFailure::Cancelled),
        (
            CompileControlError::DeadlineExceeded,
            KernelFailure::DeadlineExceeded,
        ),
        (
            CompileControlError::ResourceExhausted,
            KernelFailure::ResourceExhausted,
        ),
    ] {
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(&expected).class(),
            novarocks_failure_contract::FailureProjection::failure_fact(&failure).class()
        );
        let expected = KernelFailure::CompileControl(failure.clone());
        let control = CompileControl {
            failure: Some(failure.clone()),
            ..Default::default()
        };
        assert_eq!(
            WindowCallOptions::try_new(None, false, &control).unwrap_err(),
            expected
        );
        assert_eq!(
            WindowCallContract::try_window(call.clone(), options(), &control).unwrap_err(),
            expected
        );
        assert_eq!(
            WindowCallContract::try_aggregate(single.clone(), options(), &control).unwrap_err(),
            expected
        );
        let control = CompileControl {
            failure: Some(failure.clone()),
            positive_only: true,
            ..Default::default()
        };
        assert_eq!(
            WindowCallContract::try_window(wide_call.clone(), options(), &control).unwrap_err(),
            expected
        );
        assert_first_quantum_failure(&control.work.lock().unwrap());
        let control = CompileControl {
            failure: Some(failure),
            positive_only: true,
            ..Default::default()
        };
        let frame = WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowBound::UnboundedPreceding,
            end: WindowBound::CurrentRow,
            exclusion: WindowFrameExclusion::NoOthers,
        };
        assert_eq!(
            WindowCallOptions::try_new(Some(frame), false, &control).unwrap_err(),
            expected
        );
        assert_eq!(*control.work.lock().unwrap(), [0, 2]);
    }
}
#[test]
fn runtime_control_failures_preserve_entry_and_mid_partition_quantum() {
    let contract = window(&Fixture::new(&[i64_type(false)], &[], FunctionKind::Window));
    let array: ArrayRef = Arc::new(Int64Array::from(vec![7; 300]));
    let dense = selected(Selection::all(300), array.clone(), Box::new([]));
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original output validation refusal"),
        internal("original output validation refusal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new(
            "original output validation refusal",
        )),
        KernelFailure::InstanceFailed,
    ] {
        for argument in [
            EvaluatedArgument::Column(&array),
            EvaluatedArgument::SelectedColumn(&dense),
        ] {
            let args = [argument];
            let control = RuntimeControl {
                failure: Some(failure.clone()),
                ..Default::default()
            };
            assert_eq!(
                FullPartitionWindowInput::try_new(&contract, 300, &args, &[], &control)
                    .unwrap_err(),
                failure
            );
            assert_eq!(*control.work.lock().unwrap(), [0]);
            let control = RuntimeControl {
                failure: Some(failure.clone()),
                positive_only: true,
                ..Default::default()
            };
            assert_eq!(
                FullPartitionWindowInput::try_new(&contract, 300, &args, &[], &control)
                    .unwrap_err(),
                failure
            );
            assert_first_quantum_failure(&control.work.lock().unwrap());
        }
    }
}

fn assert_internal<T: std::fmt::Debug>(result: Result<T, KernelFailure>) {
    assert!(
        matches!(result.unwrap_err(), ref cause if crate::kernel_control::test_contract::output(cause))
    );
}
#[test]
fn output_projection_borrows_complete_input_and_checks_sparse_and_empty_results() {
    let contract = window(&Fixture::new(&[i64_type(false)], &[], FunctionKind::Window));
    let data: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let args = [EvaluatedArgument::Column(&data)];
    let input =
        FullPartitionWindowInput::try_new(&contract, 3, &args, &[], &RuntimeControl::default())
            .unwrap();
    for (rows, values) in [(&[0, 2][..], vec![10, 30]), (&[][..], vec![])] {
        let selection = Selection::try_sparse(3, rows).unwrap();
        let projection =
            WindowOutputProjection::try_new(&input, selection, &RuntimeControl::default()).unwrap();
        assert!(std::ptr::eq(projection.input(), &input));
        assert_eq!(projection.input().partition_rows(), 3);
        assert_eq!(projection.selection(), selection);
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let output = selected(selection, array, Box::new([]));
        projection
            .validate_result(&output, selection.len(), &RuntimeControl::default())
            .unwrap();
    }
    assert_invalid(WindowOutputProjection::try_new(
        &input,
        Selection::all(2),
        &RuntimeControl::default(),
    ));
}
#[test]
fn output_producer_mistakes_are_internal_and_host_capacity_is_an_exact_row_grant() {
    let contract = window(&Fixture::new(&[i64_type(false)], &[], FunctionKind::Window));
    let data: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let args = [EvaluatedArgument::Column(&data)];
    let input =
        FullPartitionWindowInput::try_new(&contract, 3, &args, &[], &RuntimeControl::default())
            .unwrap();
    let selection = Selection::try_sparse(3, &[0, 2]).unwrap();
    let projection =
        WindowOutputProjection::try_new(&input, selection, &RuntimeControl::default()).unwrap();
    let valid = selected(
        selection,
        Arc::new(Int64Array::from(vec![10, 30])),
        Box::new([]),
    );
    projection
        .validate_result(&valid, 2, &RuntimeControl::default())
        .unwrap();
    assert_internal(projection.validate_result(&valid, 1, &RuntimeControl::default()));
    let other_selection = selected(
        Selection::try_sparse(3, &[0, 1]).unwrap(),
        Arc::new(Int64Array::from(vec![10, 20])),
        Box::new([]),
    );
    let other_partition = selected(
        Selection::try_sparse(4, &[0, 2]).unwrap(),
        Arc::new(Int64Array::from(vec![10, 30])),
        Box::new([]),
    );
    let wrong_type = selected(
        selection,
        Arc::new(Int32Array::from(vec![10, 30])),
        Box::new([]),
    );
    let null = selected(
        selection,
        Arc::new(Int64Array::from(vec![Some(10), None])),
        Box::new([]),
    );
    let error = selected(
        selection,
        Arc::new(Int64Array::from(vec![Some(10), None])),
        Box::from([RowDataError::new(
            1,
            "window producer used scalar row errors",
        )]),
    );
    for output in [other_selection, other_partition, wrong_type, null, error] {
        assert_internal(projection.validate_result(&output, 2, &RuntimeControl::default()));
    }
}
#[test]
fn output_projection_preserves_entry_and_midwork_outer_control_failures() {
    let contract = window(&Fixture::new(&[i64_type(false)], &[], FunctionKind::Window));
    let data: ArrayRef = Arc::new(Int64Array::from(vec![7; 600]));
    let args = [EvaluatedArgument::Column(&data)];
    let input =
        FullPartitionWindowInput::try_new(&contract, 600, &args, &[], &RuntimeControl::default())
            .unwrap();
    let rows = (0..600).step_by(2).collect::<Vec<_>>();
    let independent_rows = rows.clone();
    let selection = Selection::try_sparse(600, &rows).unwrap();
    let output_selection = Selection::try_sparse(600, &independent_rows).unwrap();
    let output = selected(
        output_selection,
        Arc::new(Int64Array::from(vec![14; 300])),
        Box::new([]),
    );
    let projection =
        WindowOutputProjection::try_new(&input, selection, &RuntimeControl::default()).unwrap();
    projection
        .validate_result(&output, 300, &RuntimeControl::default())
        .unwrap();
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original output validation refusal"),
        internal("original output validation refusal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new(
            "original output validation refusal",
        )),
        KernelFailure::InstanceFailed,
    ] {
        let control = RuntimeControl {
            failure: Some(failure.clone()),
            ..Default::default()
        };
        assert_eq!(
            WindowOutputProjection::try_new(&input, selection, &control).unwrap_err(),
            failure
        );
        assert_eq!(*control.work.lock().unwrap(), [0]);
        let control = RuntimeControl {
            failure: Some(failure.clone()),
            ..Default::default()
        };
        assert_eq!(
            projection
                .validate_result(&output, 300, &control)
                .unwrap_err(),
            failure
        );
        assert_eq!(*control.work.lock().unwrap(), [0]);
        let control = RuntimeControl {
            failure: Some(failure.clone()),
            positive_only: true,
            ..Default::default()
        };
        assert_eq!(
            projection
                .validate_result(&output, 300, &control)
                .unwrap_err(),
            failure
        );
        assert_first_quantum_failure(&control.work.lock().unwrap());
    }
}
