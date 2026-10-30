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
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int32Array, Int64Array, NullArray, StructArray,
    types::Int8Type,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, SemanticParameters, WindowBound, WindowFrame,
    WindowFrameExclusion, WindowFrameUnits,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after primary");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "runtime callback after primary");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("COUNT OVER never waits")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original refusal"),
        internal("original refusal"),
        KernelFailure::Operational(KernelDiagnostic::new("original refusal")),
        KernelFailure::InstanceFailed,
    ]
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    }
}

struct Fixture {
    catalog: EngineFunctionCatalog,
    function: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    parameters: SemanticParameters,
    policy: DecimalOverflowPolicy,
}
impl Fixture {
    fn new(source: Option<FunctionValueType>, policy: DecimalOverflowPolicy) -> Self {
        let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let arguments = source
            .into_iter()
            .map(|value_type| FunctionArgument::Value {
                value_type,
                constant: None,
            })
            .collect::<Vec<_>>();
        let resolved = catalog
            .resolve_bound_user(
                "count",
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: arguments.len(),
                    expected_result_type: None,
                },
                &CompileControl::default(),
            )
            .unwrap();
        let uses = (0..arguments.len())
            .map(|_| Some(ExpressionUseId::new(11)))
            .collect();
        Self {
            catalog,
            function: resolved.function_id,
            selected: Arc::new(resolved.selected),
            arguments,
            uses,
            parameters: SemanticParameters::try_new([]).unwrap(),
            policy,
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: context(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.function,
            kind: FunctionKind::Aggregate,
            selected: &self.selected,
            request: FunctionBindingRequest {
                arguments: &self.arguments,
                logical_argument_count: self.arguments.len(),
                expected_result_type: None,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: self.policy,
            proof_scope: CallProofScope::Domain(context().domain),
        }
    }
    fn preparation(&self, ignore: bool) -> PureCallPreparation {
        PureCallPreparation::AggregateWindow {
            arguments: ScopedExpressionEffects::pure_value(context()),
            options: AggregateWindowPreparationOptions {
                aggregate: AggregatePreparationOptions {
                    state_interpretation: None,
                    phase: AggregateKernelPhase::Single,
                    distinct: false,
                    order_keys: Arc::from([]),
                    state_input_type: None,
                },
                window: WindowCallOptions::try_new(None, ignore, &CompileControl::default())
                    .unwrap(),
            },
        }
    }
    fn prepare(
        &self,
        ignore: bool,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, FunctionSpecializationFailure> {
        let value = self.catalog.prepare_fresh_selected(
            self.input(),
            self.selected.clone(),
            self.preparation(ignore),
            control,
        )?;
        assert_eq!(value.implementation().abi, PureKernelAbi::AggregateWindowV1);
        assert!(Arc::ptr_eq(
            value.call_contract().selected_owner(),
            &self.selected
        ));
        match value.into_prepared() {
            PreparedPureKernel::Window(value) => Ok(value),
            _ => panic!("installed COUNT AggregateWindowV1"),
        }
    }
}
fn ty(array: &ArrayRef, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), nullable)
}
fn geometry<'a>(
    prepared: &'a Arc<dyn PreparedWindowKernel>,
    args: &'a [EvaluatedArgument<'a>],
    peers: &'a [WindowRowRange],
    frames: &'a [WindowRowRange],
) -> WindowPartitionInput<'a> {
    let full = FullPartitionWindowInput::try_new(
        prepared.contract(),
        frames.len(),
        args,
        &[],
        &Control::default(),
    )
    .unwrap();
    WindowPartitionInput::try_new(full, peers, frames, &Control::default()).unwrap()
}
fn ints(value: &SelectedValues<'_>) -> Vec<i64> {
    assert!(value.errors().is_empty());
    assert_eq!(value.values().null_count(), 0);
    value
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}
fn compile_cause(error: FunctionSpecializationFailure) -> CompileControlError {
    match error {
        FunctionSpecializationFailure::Control(cause) => cause,
        FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
            CompileControlError::Cancelled
        }
        FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
            CompileControlError::DeadlineExceeded
        }
        FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
            CompileControlError::ResourceExhausted
        }
        other => panic!("unexpected cause {other:?}"),
    }
}
fn constant_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 * 1024 * 1024,
        max_library_validation_bytes: 4 * 1024 * 1024,
    }
}

#[test]
fn installed_star_and_value_frames_have_hand_counts_in_all_frame_units_and_both_policies() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        None,
        Some(10),
        None,
        Some(30),
        Some(40),
        None,
    ]));
    let peers = [(0, 2), (2, 4), (4, 6)].map(|(start, end)| WindowRowRange { start, end });
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for star in [true, false] {
            for ignore in [true, false] {
                let fixture = Fixture::new((!star).then(|| ty(&values, true)), policy);
                for units in [
                    WindowFrameUnits::Rows,
                    WindowFrameUnits::Range,
                    WindowFrameUnits::Groups,
                ] {
                    // Independent tables from the stated bounds and peer groups.
                    // No implementation computes frame membership here.
                    let (bounds, ranges, star_hand, value_hand) = match units {
                        WindowFrameUnits::Rows => (
                            (WindowBound::Following(1), WindowBound::Following(1)),
                            [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 6)],
                            [1, 1, 1, 1, 1, 0],
                            [1, 0, 1, 1, 0, 0],
                        ),
                        WindowFrameUnits::Range => (
                            (WindowBound::UnboundedPreceding, WindowBound::CurrentRow),
                            [(0, 2), (0, 2), (0, 4), (0, 4), (0, 6), (0, 6)],
                            [2, 2, 4, 4, 6, 6],
                            [1, 1, 2, 2, 3, 3],
                        ),
                        WindowFrameUnits::Groups => (
                            (WindowBound::Preceding(1), WindowBound::CurrentRow),
                            [(0, 2), (0, 2), (0, 4), (0, 4), (2, 6), (2, 6)],
                            [2, 2, 4, 4, 4, 4],
                            [1, 1, 2, 2, 2, 2],
                        ),
                    };
                    let frames = ranges.map(|(start, end)| WindowRowRange { start, end });
                    let mut preparation = fixture.preparation(ignore);
                    if let PureCallPreparation::AggregateWindow { options, .. } = &mut preparation {
                        options.window = WindowCallOptions::try_new(
                            Some(WindowFrame {
                                units,
                                start: bounds.0,
                                end: bounds.1,
                                exclusion: WindowFrameExclusion::NoOthers,
                            }),
                            ignore,
                            &CompileControl::default(),
                        )
                        .unwrap();
                    }
                    let prepared = fixture
                        .catalog
                        .prepare_fresh_selected(
                            fixture.input(),
                            fixture.selected.clone(),
                            preparation,
                            &CompileControl::default(),
                        )
                        .unwrap();
                    assert_eq!(
                        prepared.call_contract().effects().instance_state,
                        FunctionInstanceState::AggregateInstance
                    );
                    assert_eq!(
                        prepared.call_contract().effects().own_row_error,
                        FunctionIntrinsicRowError::NotRowEvaluated
                    );
                    assert_eq!(
                        prepared.call_contract().effects().null_behavior,
                        FunctionNullBehavior::CalledOnNull
                    );
                    let prepared = match prepared.into_prepared() {
                        PreparedPureKernel::Window(value) => value,
                        _ => panic!("COUNT window"),
                    };
                    assert!(!prepared.contract().result_type().nullable());
                    let args = if star {
                        Vec::new()
                    } else {
                        vec![EvaluatedArgument::Column(&values)]
                    };
                    let input = geometry(&prepared, &args, &peers, &frames);
                    let mut part = WindowEvaluationPartition::begin(
                        prepared.clone(),
                        input,
                        &Control::default(),
                    )
                    .unwrap();
                    let hand = if star {
                        star_hand.to_vec()
                    } else {
                        value_hand.to_vec()
                    };
                    assert_eq!(
                        ints(
                            &part
                                .evaluate(Selection::all(6), 6, &Control::default())
                                .unwrap()
                        ),
                        hand
                    );
                    let rows = [1, 4, 5];
                    let selected = Selection::try_sparse(6, &rows).unwrap();
                    for _ in 0..2 {
                        assert_eq!(
                            ints(&part.evaluate(selected, 3, &Control::default()).unwrap()),
                            rows.map(|r| hand[r])
                        );
                    }
                    assert!(
                        part.evaluate(
                            Selection::try_sparse(6, &[]).unwrap(),
                            0,
                            &Control::default()
                        )
                        .unwrap()
                        .values()
                        .is_empty()
                    );
                    assert!(part.retained_bytes().unwrap() <= part.retained_upper_bound());
                    part.finish(&Control::default()).unwrap();
                }
            }
        }
    }
}

#[test]
fn complete_selected_source_addresses_include_slices_scalars_nonzero_cv_and_compact_columns() {
    let backing: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(99),
        None,
        Some(8),
        Some(9),
        Some(77),
    ]));
    let slice = backing.slice(1, 3);
    let scalar: ArrayRef = Arc::new(Int32Array::from(vec![Some(42)]));
    let pool = ConstantPool::try_new(
        Arc::new(ty(&backing, true).try_to_field("original").unwrap()),
        ty(&backing, true),
        backing.to_data(),
        constant_policy(),
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap();
    let constant = pool.value(2).unwrap();
    let null_constant = pool.value(1).unwrap();
    let compact = SelectedValues::try_new(
        Selection::all(3),
        slice.data_type(),
        slice.clone(),
        Box::default(),
    )
    .unwrap();
    let peers = [WindowRowRange { start: 0, end: 3 }];
    let frames = [WindowRowRange { start: 0, end: 3 }; 3];
    let fixture = Fixture::new(Some(ty(&backing, true)), DecimalOverflowPolicy::OutputNull);
    let prepared = fixture.prepare(false, &CompileControl::default()).unwrap();
    for (arg, hand) in [
        (EvaluatedArgument::Column(&slice), 2),
        (EvaluatedArgument::Scalar(&scalar), 3),
        (EvaluatedArgument::Constant(&constant), 3),
        (EvaluatedArgument::Constant(&null_constant), 0),
        (EvaluatedArgument::SelectedColumn(&compact), 2),
    ] {
        let args = [arg];
        let mut part = WindowEvaluationPartition::begin(
            prepared.clone(),
            geometry(&prepared, &args, &peers, &frames),
            &Control::default(),
        )
        .unwrap();
        assert_eq!(
            ints(
                &part
                    .evaluate(Selection::all(3), 3, &Control::default())
                    .unwrap()
            ),
            vec![hand; 3]
        );
    }
}

#[test]
fn original_nested_metadata_and_child_encoded_nulls_preserve_root_contribution_and_frozen_attachment()
 {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            arrow_array::Int8Array::from(vec![Some(0), Some(1), Some(0)]),
            Arc::new(Int32Array::from(vec![Some(5), None])),
        )
        .unwrap(),
    );
    let field = Arc::new(
        Field::new("source", dictionary.data_type().clone(), true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "deep-source".into(),
                "x".repeat(1300),
            )]),
        ),
    );
    let values: ArrayRef = Arc::new(StructArray::new(
        vec![field.clone()].into(),
        vec![dictionary],
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let fixture = Fixture::new(Some(ty(&values, true)), DecimalOverflowPolicy::ReportError);
    let fresh = fixture.prepare(true, &CompileControl::default()).unwrap();
    let FunctionArgumentType::Value(selected) = &fixture.selected.argument_types[0] else {
        panic!("selected source")
    };
    let decoded = arrow_cast::cast(values.as_ref(), selected.data_type()).unwrap();
    let values = crate::test_value_ingress::array_for_binding(&decoded, selected).unwrap();
    assert!(novarocks_type_contract::arrow_data_types_exact(
        selected.data_type(),
        values.data_type()
    ));
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            fixture
                .catalog
                .definition("count", FunctionKind::Aggregate)
                .unwrap()
                .clone(),
        )
        .unwrap();
    let subset = builder
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
    let frozen = subset
        .prepare_frozen(
            fixture.input(),
            fixture.selected.clone(),
            fresh.contract().call().effects(),
            fixture.preparation(true),
            &CompileControl::default(),
        )
        .unwrap();
    assert_eq!(frozen.source(), PurePreparationSource::Frozen);
    let prepared = match frozen.into_prepared() {
        PreparedPureKernel::Window(value) => value,
        _ => panic!("same COUNT window"),
    };
    assert!(Arc::ptr_eq(
        prepared.contract().call().selected_owner(),
        &fixture.selected
    ));
    assert_eq!(
        prepared.contract().call().decimal_overflow_policy(),
        fixture.policy
    );
    let args = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 3 }];
    let frames = [WindowRowRange { start: 0, end: 3 }; 3];
    let mut part = WindowEvaluationPartition::begin(
        prepared.clone(),
        geometry(&prepared, &args, &peers, &frames),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        ints(
            &part
                .evaluate(Selection::all(3), 3, &Control::default())
                .unwrap()
        ),
        vec![2; 3]
    );
}

#[test]
fn encoded_plain_ingress_and_bare_null_follow_logical_counts_and_generic_profiles() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            arrow_array::Int8Array::from(vec![Some(0), Some(1), None, Some(1), Some(0)]),
            Arc::new(Int32Array::from(vec![Some(5), None])),
        )
        .unwrap(),
    );
    // Physical keys and logical selected values carry distinct NULL facts.
    assert_eq!((0..5).filter(|row| !dictionary.is_null(*row)).count(), 4);
    let null: ArrayRef = Arc::new(NullArray::new(5));
    assert_eq!((0..5).filter(|r| !null.is_null(*r)).count(), 5);
    let union = DataType::Union(
        arrow_schema::UnionFields::try_new([0], [Field::new("v", DataType::Int32, true)]).unwrap(),
        arrow_schema::UnionMode::Sparse,
    );
    let run = DataType::RunEndEncoded(
        Arc::new(Field::new("run_ends", DataType::Int16, false)),
        Arc::new(Field::new("values", DataType::Int32, true)),
    );
    for source in [
        ty(&dictionary, true),
        ty(&null, true),
        FunctionValueType::from_legacy_declared_projection(union, true),
        FunctionValueType::from_legacy_declared_projection(run, true),
    ] {
        let fixture = Fixture::new(Some(source), DecimalOverflowPolicy::OutputNull);
        assert!(fixture.prepare(false, &CompileControl::default()).is_ok());
    }
    let plain = arrow_cast::cast(dictionary.as_ref(), &DataType::Int32).unwrap();
    for (values, expected) in [(plain, 2), (null, 0)] {
        let fixture = Fixture::new(Some(ty(&values, true)), DecimalOverflowPolicy::OutputNull);
        let prepared = fixture.prepare(false, &CompileControl::default()).unwrap();
        let args = [EvaluatedArgument::Column(&values)];
        let peers = [WindowRowRange { start: 0, end: 5 }];
        let frames = [WindowRowRange { start: 0, end: 5 }; 5];
        let mut partition = WindowEvaluationPartition::begin(
            prepared.clone(),
            geometry(&prepared, &args, &peers, &frames),
            &Control::default(),
        )
        .unwrap();
        assert_eq!(
            ints(
                &partition
                    .evaluate(Selection::all(5), 5, &Control::default())
                    .unwrap()
            ),
            vec![expected; 5]
        );
    }
}

#[test]
fn compile_callbacks_keep_original_three_causes_before_success_or_semantic_refusal() {
    for source in [
        Some(FunctionValueType::new(DataType::Int32, true)),
        Some(FunctionValueType::new(DataType::Null, true)),
    ] {
        let fixture = Fixture::new(source, DecimalOverflowPolicy::OutputNull);
        let baseline = CompileControl::default();
        let result = fixture.prepare(false, &baseline);
        assert!(result.is_ok());
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(trace.len() > 2);
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert_eq!(
                    compile_cause(fixture.prepare(false, &control).unwrap_err()),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
    let fixture = Fixture::new(
        Some(FunctionValueType::new(DataType::Int32, true)),
        DecimalOverflowPolicy::OutputNull,
    );
    for phase in [
        AggregateKernelPhase::Partial,
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        let mut opts = fixture.preparation(false);
        if let PureCallPreparation::AggregateWindow { options, .. } = &mut opts {
            options.aggregate.phase = phase;
        }
        assert!(
            fixture
                .catalog
                .prepare_fresh_selected(
                    fixture.input(),
                    fixture.selected.clone(),
                    opts,
                    &CompileControl::default()
                )
                .is_err()
        );
    }
    let mut opts = fixture.preparation(false);
    if let PureCallPreparation::AggregateWindow { options, .. } = &mut opts {
        options.aggregate.distinct = true;
    }
    assert!(
        fixture
            .catalog
            .prepare_fresh_selected(
                fixture.input(),
                fixture.selected.clone(),
                opts,
                &CompileControl::default()
            )
            .is_err()
    );
}

#[test]
fn every_actual_window_lifecycle_callback_preserves_seven_causes_and_poison_without_replay() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(7), Some(9)]));
    let args = [EvaluatedArgument::Column(&values)];
    let fixture = Fixture::new(Some(ty(&values, true)), DecimalOverflowPolicy::OutputNull);
    let prepared = fixture.prepare(false, &CompileControl::default()).unwrap();
    let peers = [WindowRowRange { start: 0, end: 3 }];
    let frames = [WindowRowRange { start: 0, end: 3 }; 3];
    let input = geometry(&prepared, &args, &peers, &frames);
    let rows = [0, 2];
    let selected = Selection::try_sparse(3, &rows).unwrap();
    for stage in 0..4 {
        let baseline = Control::default();
        if stage == 0 {
            WindowEvaluationPartition::begin(prepared.clone(), input, &baseline).unwrap();
        } else {
            let mut part =
                WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default())
                    .unwrap();
            match stage {
                1 => {
                    part.evaluate(selected, 2, &baseline).unwrap();
                }
                2 => {
                    assert!(part.evaluate(selected, 1, &baseline).is_err());
                }
                _ => {
                    part.finish(&baseline).unwrap();
                }
            }
        }
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(trace.len() >= 2);
        for at in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                if stage == 0 {
                    assert_eq!(
                        WindowEvaluationPartition::begin(prepared.clone(), input, &control)
                            .map(|_| ())
                            .unwrap_err(),
                        cause
                    );
                } else {
                    let mut part = WindowEvaluationPartition::begin(
                        prepared.clone(),
                        input,
                        &Control::default(),
                    )
                    .unwrap();
                    let result = match stage {
                        1 => part.evaluate(selected, 2, &control).map(|_| ()),
                        2 => part.evaluate(selected, 1, &control).map(|_| ()),
                        _ => part.finish(&control),
                    };
                    assert_eq!(result.unwrap_err(), cause);
                    let stopped = Control::default();
                    assert_eq!(
                        part.evaluate(selected, 2, &stopped).unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                    assert_eq!(
                        part.finish(&stopped).unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                    assert!(stopped.trace.lock().unwrap().is_empty());
                }
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn required_complete_input_refuses_child_errors_and_false_nonnull_promises_before_any_output() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(2)]));
    let fixture = Fixture::new(Some(ty(&values, true)), DecimalOverflowPolicy::OutputNull);
    let prepared = fixture.prepare(false, &CompileControl::default()).unwrap();
    let failed = SelectedValues::try_new(
        Selection::all(2),
        values.data_type(),
        values.clone(),
        vec![RowDataError::new(0, "required child")].into_boxed_slice(),
    )
    .unwrap();
    assert!(
        FullPartitionWindowInput::try_new(
            prepared.contract(),
            2,
            &[EvaluatedArgument::SelectedColumn(&failed)],
            &[],
            &Control::default()
        )
        .is_err()
    );
    let strict = Fixture::new(Some(ty(&values, false)), DecimalOverflowPolicy::OutputNull);
    let prepared = strict.prepare(false, &CompileControl::default()).unwrap();
    assert!(
        FullPartitionWindowInput::try_new(
            prepared.contract(),
            2,
            &[EvaluatedArgument::Column(&values)],
            &[],
            &Control::default()
        )
        .is_err()
    );
}

#[test]
fn wide_complete_prefix_and_selected_emitter_cross_true_quantum_with_sampled_primary_causes() {
    let values: ArrayRef = Arc::new(Int32Array::from(
        (0..320)
            .map(|r| (r % 2 == 1).then_some(r))
            .collect::<Vec<_>>(),
    ));
    let args = [EvaluatedArgument::Column(&values)];
    let fixture = Fixture::new(Some(ty(&values, true)), DecimalOverflowPolicy::OutputNull);
    let prepared = fixture.prepare(false, &CompileControl::default()).unwrap();
    let peers = [WindowRowRange { start: 0, end: 320 }];
    let frames = vec![WindowRowRange { start: 0, end: 320 }; 320];
    let input = geometry(&prepared, &args, &peers, &frames);
    for setup in [true, false] {
        let baseline = Control::default();
        if setup {
            WindowEvaluationPartition::begin(prepared.clone(), input, &baseline).unwrap();
        } else {
            let mut part =
                WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default())
                    .unwrap();
            assert_eq!(
                ints(&part.evaluate(Selection::all(320), 320, &baseline).unwrap()),
                vec![160; 320]
            );
        }
        let trace = baseline.trace.lock().unwrap().clone();
        let q = trace
            .iter()
            .position(|u| *u == 256)
            .expect("real completed quantum");
        for at in [0, q, trace.len() - 1] {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                let result = if setup {
                    WindowEvaluationPartition::begin(prepared.clone(), input, &control).map(|_| ())
                } else {
                    let mut part = WindowEvaluationPartition::begin(
                        prepared.clone(),
                        input,
                        &Control::default(),
                    )
                    .unwrap();
                    part.evaluate(Selection::all(320), 320, &control)
                        .map(|_| ())
                };
                assert_eq!(result.unwrap_err(), cause);
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn zero_partition_and_retained_representation_bound_keep_typed_nonnull_empty_output() {
    let fixture = Fixture::new(None, DecimalOverflowPolicy::OutputNull);
    let prepared = fixture.prepare(false, &CompileControl::default()).unwrap();
    assert!(matches!(
        prepared.partition_retained_upper_bound(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    ));
    let args = [];
    let peers = [];
    let frames = [];
    let input = geometry(&prepared, &args, &peers, &frames);
    let mut part =
        WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default()).unwrap();
    assert!(
        part.evaluate(Selection::all(0), 0, &Control::default())
            .unwrap()
            .values()
            .is_empty()
    );
    part.finish(&Control::default()).unwrap();
    assert!(part.retained_bytes().unwrap() <= part.retained_upper_bound());
}

#[test]
fn static_source_options_and_direct_first_cause_keep_original_control() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let fixture = Fixture::new(Some(ty(&values, false)), DecimalOverflowPolicy::ReportError);
    let changed = [FunctionArgument::Value {
        value_type: FunctionValueType::new(DataType::Int64, false),
        constant: None,
    }];
    let mut input = fixture.input();
    input.request.arguments = &changed;
    assert!(
        fixture
            .catalog
            .prepare_fresh_selected(
                input,
                fixture.selected.clone(),
                fixture.preparation(false),
                &CompileControl::default()
            )
            .is_err()
    );
    let mut ordered = fixture.preparation(false);
    if let PureCallPreparation::AggregateWindow { options, .. } = &mut ordered {
        options.aggregate.order_keys = Arc::from([AggregateOrderKey {
            ascending: true,
            nulls_first: false,
        }]);
    }
    assert!(
        fixture
            .catalog
            .prepare_fresh_selected(
                fixture.input(),
                fixture.selected.clone(),
                ordered,
                &CompileControl::default()
            )
            .is_err()
    );
    for exclusion in [
        WindowFrameExclusion::CurrentRow,
        WindowFrameExclusion::Group,
        WindowFrameExclusion::Ties,
    ] {
        let mut opts = fixture.preparation(false);
        if let PureCallPreparation::AggregateWindow { options, .. } = &mut opts {
            options.window = WindowCallOptions::try_new(
                Some(WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start: WindowBound::UnboundedPreceding,
                    end: WindowBound::CurrentRow,
                    exclusion,
                }),
                false,
                &CompileControl::default(),
            )
            .unwrap();
        }
        assert!(
            fixture
                .catalog
                .prepare_fresh_selected(
                    fixture.input(),
                    fixture.selected.clone(),
                    opts,
                    &CompileControl::default()
                )
                .is_err()
        );
    }
    let prepared = fixture.prepare(false, &CompileControl::default()).unwrap();
    let args = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 2 }];
    let frames = [WindowRowRange { start: 0, end: 2 }; 2];
    let input = geometry(&prepared, &args, &peers, &frames);
    let baseline = Control::default();
    let mut part = prepared
        .clone()
        .begin_partition(input, &Control::default())
        .unwrap();
    assert!(matches!(
        part.evaluate(Selection::all(3), 3, &baseline),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let trace = baseline.trace.lock().unwrap().clone();
    // The actual shape author already refused. Successful callbacks before
    // that point remain observable; a new completion tail cannot replace it.
    assert_eq!(trace, [0]);
    for at in 0..trace.len() {
        for cause in causes() {
            let mut part = prepared
                .clone()
                .begin_partition(input, &Control::default())
                .unwrap();
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert_eq!(
                part.evaluate(Selection::all(3), 3, &control).unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            let stopped = Control::default();
            assert_eq!(
                part.finish(&stopped).unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(stopped.trace.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn count_real_partition_shape_error_precedes_every_armed_completion_refusal() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let fixture = Fixture::new(Some(ty(&values, false)), DecimalOverflowPolicy::ReportError);
    let prepared = fixture.prepare(false, &CompileControl::default()).unwrap();
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 2 }];
    let frames = [WindowRowRange { start: 0, end: 2 }; 2];
    let input = geometry(&prepared, &arguments, &peers, &frames);
    for late in causes() {
        let mut partition = prepared
            .clone()
            .begin_partition(input, &Control::default())
            .unwrap();
        let control = Control {
            trace: Mutex::default(),
            refusal: Some((1, late)),
        };
        assert!(matches!(
            partition.evaluate(Selection::all(3), 3, &control),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert_eq!(*control.trace.lock().unwrap(), [0]);
        let stopped = Control::default();
        assert_eq!(
            partition.finish(&stopped),
            Err(KernelFailure::InstanceFailed)
        );
        assert!(stopped.trace.lock().unwrap().is_empty());
    }
}

#[test]
fn count_real_finish_clears_original_counts_before_success_tail_refusal_and_owner_drop() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let fixture = Fixture::new(Some(ty(&values, false)), DecimalOverflowPolicy::ReportError);
    let prepared = fixture.prepare(false, &CompileControl::default()).unwrap();
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 2 }];
    let frames = [WindowRowRange { start: 0, end: 2 }; 2];
    let input = geometry(&prepared, &arguments, &peers, &frames);
    let before = Arc::strong_count(&prepared);
    let mut partition = prepared
        .clone()
        .begin_partition(input, &Control::default())
        .unwrap();
    assert_eq!(Arc::strong_count(&prepared), before + 1);
    assert!(partition.retained_bytes() > size_of::<CountPartition>());
    // Entry succeeds, real counts=Box::default runs, then successful work's
    // genuine final checkpoint refuses. Error must not suppress that cleanup.
    let stop = Control {
        trace: Mutex::default(),
        refusal: Some((1, KernelFailure::Cancelled)),
    };
    assert_eq!(partition.finish(&stop), Err(KernelFailure::Cancelled));
    assert_eq!(*stop.trace.lock().unwrap(), [0, 1]);
    assert_eq!(partition.retained_bytes(), size_of::<CountPartition>());
    assert_eq!(Arc::strong_count(&prepared), before + 1);
    let repeated = Control::default();
    assert_eq!(
        partition.finish(&repeated),
        Err(KernelFailure::InstanceFailed)
    );
    assert!(repeated.trace.lock().unwrap().is_empty());
    drop(partition);
    assert_eq!(Arc::strong_count(&prepared), before);
}

#[test]
fn count_installed_compile_semantic_error_keeps_first_cause_and_drops_temporary_owners() {
    let fixture = Fixture::new(
        Some(FunctionValueType::new(DataType::Int64, true)),
        DecimalOverflowPolicy::OutputNull,
    );
    let run = |control: &dyn PureCompileControl| {
        let mut options = fixture.preparation(false);
        if let PureCallPreparation::AggregateWindow { options, .. } = &mut options {
            options.window = WindowCallOptions::try_new(
                Some(WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start: WindowBound::UnboundedPreceding,
                    end: WindowBound::CurrentRow,
                    exclusion: WindowFrameExclusion::CurrentRow,
                }),
                false,
                &CompileControl::default(),
            )
            .unwrap();
        }
        fixture.catalog.prepare_fresh_selected(
            fixture.input(),
            fixture.selected.clone(),
            options,
            control,
        )
    };
    let baseline = CompileControl::default();
    assert!(matches!(
        run(&baseline),
        Err(FunctionSpecializationFailure::Kernel(
            KernelFailure::InvalidProgram(_)
        ))
    ));
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    let source = CompileControlError::from_domain(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    );
    let original_owners = Arc::strong_count(&fixture.selected);
    let control = CompileControl {
        trace: Mutex::default(),
        refusal: Some((trace.len(), source.clone())),
    };
    assert!(matches!(
        run(&control),
        Err(FunctionSpecializationFailure::Kernel(
            KernelFailure::InvalidProgram(_)
        ))
    ));
    assert_eq!(*control.trace.lock().unwrap(), trace);
    assert_eq!(Arc::strong_count(&fixture.selected), original_owners);
    for at in 0..trace.len() {
        let control = CompileControl {
            trace: Mutex::default(),
            refusal: Some((at, source.clone())),
        };
        assert!(
            matches!(run(&control),Err(FunctionSpecializationFailure::Control(actual)) if actual==source)
        );
        assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        assert_eq!(Arc::strong_count(&fixture.selected), original_owners);
    }
}
