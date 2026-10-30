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

use crate::kernel_control::{internal, invalid};
use crate::*;
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Float64Array, Int8Array, Int32Array, StringArray,
    StructArray, types::Int8Type,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    EvaluationDemand, EvaluationDomainId, ExpressionEffectContext, ExpressionUseId,
    FunctionInstanceState, FunctionIntrinsicRowError, FunctionNullBehavior, PureCompileControl,
    SemanticParameters, WindowBound, WindowFrame, WindowFrameExclusion, WindowFrameUnits,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const NAMES: [&str; 2] = ["first_value", "last_value"];
const ARGUMENT_USES: [Option<ExpressionUseId>; 1] = [Some(ExpressionUseId::new(11))];
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
        panic!("FIRST/LAST never waits")
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
    parameters: SemanticParameters,
    policy: DecimalOverflowPolicy,
}
impl Fixture {
    fn new(name: &str, source: FunctionValueType, policy: DecimalOverflowPolicy) -> Self {
        let catalog = super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let arguments = vec![FunctionArgument::Value {
            value_type: source,
            constant: None,
        }];
        let resolved = catalog
            .resolve_bound_user(
                name,
                FunctionKind::Window,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 1,
                    expected_result_type: None,
                },
                &CompileControl::default(),
            )
            .unwrap();
        Self {
            catalog,
            function: resolved.function_id,
            selected: Arc::new(resolved.selected),
            arguments,
            parameters: SemanticParameters::try_new([]).unwrap(),
            policy,
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: context(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(&ARGUMENT_USES),
            function_id: &self.function,
            kind: FunctionKind::Window,
            selected: &self.selected,
            request: FunctionBindingRequest {
                arguments: &self.arguments,
                logical_argument_count: 1,
                expected_result_type: None,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: self.policy,
            proof_scope: CallProofScope::Domain(context().domain),
        }
    }
    fn prepare(
        &self,
        options: WindowCallOptions,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, FunctionSpecializationFailure> {
        let prepared = self.catalog.prepare_fresh_selected(
            self.input(),
            self.selected.clone(),
            PureCallPreparation::Window {
                arguments: ScopedExpressionEffects::pure_value(context()),
                options,
            },
            control,
        )?;
        assert_eq!(prepared.implementation().abi, PureKernelAbi::WindowV1);
        assert!(Arc::ptr_eq(
            prepared.call_contract().selected_owner(),
            &self.selected
        ));
        match prepared.into_prepared() {
            PreparedPureKernel::Window(value) => Ok(value),
            _ => panic!("actual Window attachment"),
        }
    }
}
fn options(ignore: bool) -> WindowCallOptions {
    WindowCallOptions::try_new(None, ignore, &CompileControl::default()).unwrap()
}
fn geometry<'a>(
    prepared: &'a Arc<dyn PreparedWindowKernel>,
    arguments: &'a [EvaluatedArgument<'a>],
    peers: &'a [WindowRowRange],
    frames: &'a [WindowRowRange],
) -> WindowPartitionInput<'a> {
    let full = FullPartitionWindowInput::try_new(
        prepared.contract(),
        frames.len(),
        arguments,
        &[],
        &Control::default(),
    )
    .unwrap();
    WindowPartitionInput::try_new(full, peers, frames, &Control::default()).unwrap()
}
fn ints(output: &SelectedValues<'_>) -> Vec<Option<i32>> {
    assert!(output.errors().is_empty());
    output
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
fn ty(array: &ArrayRef, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), nullable)
}
fn policy() -> ConstantPolicy {
    // Explicit finite test invoice; not a production profile or a host MEM grant.
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
fn small_frames() -> [WindowRowRange; 6] {
    [(0, 0), (0, 2), (1, 4), (2, 5), (4, 6), (5, 6)]
        .map(|(start, end)| WindowRowRange { start, end })
}
fn expected(name: &str, ignore: bool) -> [Option<i32>; 6] {
    match (name, ignore) {
        ("first_value", false) => [None, None, Some(10), None, Some(40), None],
        ("first_value", true) => [None, Some(10), Some(10), Some(30), Some(40), None],
        ("last_value", false) => [None, Some(10), Some(30), Some(40), None, None],
        ("last_value", true) => [None, Some(10), Some(30), Some(40), Some(40), None],
        _ => unreachable!("two actual installed names"),
    }
}

#[test]
fn installed_first_last_preserve_hand_authored_frames_null_treatment_policies_and_split_demand() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        None,
        Some(10),
        None,
        Some(30),
        Some(40),
        None,
    ]));
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 6 }];
    let frames = small_frames();
    for name in NAMES {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for ignore in [false, true] {
                let fixture = Fixture::new(name, ty(&values, true), policy);
                let prepared = fixture
                    .prepare(options(ignore), &CompileControl::default())
                    .unwrap();
                let call = prepared.contract().call();
                assert_eq!(call.decimal_overflow_policy(), policy);
                assert_eq!(call.effects().argument_control, ArgumentControl::Window);
                assert_eq!(
                    call.effects().instance_state,
                    FunctionInstanceState::WindowPartition
                );
                assert_eq!(
                    call.effects().null_behavior,
                    FunctionNullBehavior::CalledOnNull
                );
                assert_eq!(
                    call.effects().own_row_error,
                    FunctionIntrinsicRowError::NotRowEvaluated
                );
                assert!(call.effects().environment.is_empty());
                let input = geometry(&prepared, &arguments, &peers, &frames);
                let mut part =
                    WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default())
                        .unwrap();
                assert!(part.retained_bytes().unwrap() <= part.retained_upper_bound());
                assert_eq!(
                    ints(
                        &part
                            .evaluate(Selection::all(6), 6, &Control::default())
                            .unwrap()
                    ),
                    expected(name, ignore)
                );
                let rows = [1, 4, 5];
                let selected = Selection::try_sparse(6, &rows).unwrap();
                let hand: Vec<_> = rows
                    .iter()
                    .map(|row| expected(name, ignore)[*row])
                    .collect();
                for _ in 0..2 {
                    assert_eq!(
                        ints(&part.evaluate(selected, 3, &Control::default()).unwrap()),
                        hand
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
                part.finish(&Control::default()).unwrap();
            }
        }
    }
}

#[test]
fn sliced_scalar_nonzero_constant_and_dense_compact_arguments_keep_original_addresses() {
    let backing: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(99),
        Some(7),
        Some(8),
        Some(9),
        Some(77),
    ]));
    let sliced = backing.slice(1, 3);
    let scalar: ArrayRef = Arc::new(Int32Array::from(vec![Some(42)]));
    let source = ty(&backing, true);
    let pool = ConstantPool::try_new(
        Arc::new(source.try_to_field("original").unwrap()),
        source.clone(),
        backing.to_data(),
        policy(),
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap();
    let constant = pool.value(2).unwrap();
    let compact = SelectedValues::try_new(
        Selection::all(3),
        sliced.data_type(),
        sliced.clone(),
        Box::default(),
    )
    .unwrap();
    let peers = [WindowRowRange { start: 0, end: 3 }];
    let frames = [WindowRowRange { start: 0, end: 3 }; 3];
    let selected_rows = [0, 2];
    for name in NAMES {
        let fixture = Fixture::new(name, source.clone(), DecimalOverflowPolicy::OutputNull);
        let prepared = fixture
            .prepare(options(false), &CompileControl::default())
            .unwrap();
        for (argument, hand) in [
            (
                EvaluatedArgument::Column(&sliced),
                if name == "first_value" { 7 } else { 9 },
            ),
            (EvaluatedArgument::Scalar(&scalar), 42),
            (EvaluatedArgument::Constant(&constant), 8),
            (
                EvaluatedArgument::SelectedColumn(&compact),
                if name == "first_value" { 7 } else { 9 },
            ),
        ] {
            let arguments = [argument];
            let mut part = prepared
                .clone()
                .begin_partition(
                    geometry(&prepared, &arguments, &peers, &frames),
                    &Control::default(),
                )
                .unwrap();
            let selection = Selection::try_sparse(3, &selected_rows).unwrap();
            assert_eq!(
                ints(&part.evaluate(selection, 2, &Control::default()).unwrap()),
                [Some(hand), Some(hand)]
            );
        }
    }
}

#[test]
fn ignore_nulls_follows_dictionary_selected_values_after_explicit_plain_ingress() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None]),
            Arc::new(StringArray::from(vec![None, Some("visible")])),
        )
        .unwrap(),
    );
    assert!(!dictionary.is_null(0));
    let plain = arrow_cast::cast(dictionary.as_ref(), &DataType::Utf8).unwrap();
    assert!(plain.is_null(0));
    let arguments = [EvaluatedArgument::Column(&plain)];
    let peers = [WindowRowRange { start: 0, end: 3 }];
    let frames = [WindowRowRange { start: 0, end: 3 }; 3];
    for name in NAMES {
        let fixture = Fixture::new(
            name,
            ty(&dictionary, true),
            DecimalOverflowPolicy::OutputNull,
        );
        let prepared = fixture
            .prepare(options(true), &CompileControl::default())
            .unwrap();
        let mut part = WindowEvaluationPartition::begin(
            prepared.clone(),
            geometry(&prepared, &arguments, &peers, &frames),
            &Control::default(),
        )
        .unwrap();
        let output = part
            .evaluate(Selection::all(3), 3, &Control::default())
            .unwrap();
        let output = output
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(output.iter().collect::<Vec<_>>(), vec![Some("visible"); 3]);
    }
}

#[test]
fn exact_nested_metadata_fresh_frozen_owner_and_empty_frames_preserve_full_source_type() {
    let field = Arc::new(
        Field::new("source-field", DataType::Int32, false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "long-source".into(),
                "z".repeat(1300),
            )]),
        ),
    );
    let fields: arrow_schema::Fields = vec![field.clone()].into();
    let values: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let values = crate::test_value_ingress::array_for_binding(&values, &ty(&values, true)).unwrap();
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 3 }];
    let frames = [
        WindowRowRange { start: 0, end: 3 },
        WindowRowRange { start: 1, end: 1 },
        WindowRowRange { start: 2, end: 3 },
    ];
    for name in NAMES {
        let fixture = Fixture::new(name, ty(&values, true), DecimalOverflowPolicy::ReportError);
        let options = options(true);
        let fresh = fixture
            .prepare(options, &CompileControl::default())
            .unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                fixture
                    .catalog
                    .definition(name, FunctionKind::Window)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        let subset = builder
            .seal_pure([InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.window/{name}/v1")).unwrap(),
                kind: FunctionKind::Window,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!(
                        "builtin.window/{name}/(any<T>)->any<T>;strict;legacy"
                    ))
                    .unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.window/{name}/selected-v1"
                    ))
                    .unwrap(),
                    abi: PureKernelAbi::WindowV1,
                },
                aggregate_state_format: None,
            }])
            .unwrap();
        let frozen = subset
            .prepare_frozen(
                fixture.input(),
                fixture.selected.clone(),
                fresh.contract().call().effects(),
                PureCallPreparation::Window {
                    arguments: ScopedExpressionEffects::pure_value(context()),
                    options,
                },
                &CompileControl::default(),
            )
            .unwrap();
        assert_eq!(frozen.source(), PurePreparationSource::Frozen);
        let frozen = match frozen.into_prepared() {
            PreparedPureKernel::Window(value) => value,
            _ => panic!("actual Window"),
        };
        assert!(Arc::ptr_eq(
            frozen.contract().call().selected_owner(),
            &fixture.selected
        ));
        assert_eq!(*frozen.contract().options(), options);
        assert!(frozen.contract().result_type().nullable());
        assert!(novarocks_type_contract::arrow_data_types_exact(
            frozen.contract().result_type().data_type(),
            values.data_type()
        ));
        let mut part = frozen
            .clone()
            .begin_partition(
                geometry(&frozen, &arguments, &peers, &frames),
                &Control::default(),
            )
            .unwrap();
        let output = part
            .evaluate(Selection::all(3), 3, &Control::default())
            .unwrap();
        let actual = output
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            actual.nulls().unwrap().iter().collect::<Vec<_>>(),
            [true, false, true]
        );
        assert!(novarocks_type_contract::arrow_data_types_exact(
            actual.data_type(),
            values.data_type()
        ));
        assert!(actual.fields()[0].metadata().is_empty());
        let actual_values = actual
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            actual_values.value(0),
            if name == "first_value" { 1 } else { 3 }
        );
        assert_eq!(actual_values.value(2), 3);
        // A stale selected source must not be repaired by generic Any matching.
        let drift = FunctionValueType::from_legacy_declared_projection(
            DataType::Struct(vec![Arc::new(Field::new("other", DataType::Int32, false))].into()),
            true,
        );
        let wrong = [FunctionArgument::Value {
            value_type: drift,
            constant: None,
        }];
        let mut input = fixture.input();
        input.request.arguments = &wrong;
        assert!(
            fixture
                .catalog
                .prepare_fresh_selected(
                    input,
                    fixture.selected.clone(),
                    PureCallPreparation::Window {
                        arguments: ScopedExpressionEffects::pure_value(context()),
                        options
                    },
                    &CompileControl::default()
                )
                .is_err()
        );
    }
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
#[test]
fn every_compile_callback_preserves_three_primary_causes_on_success_and_ordinary_exclusion_tail() {
    for name in NAMES {
        let fixture = Fixture::new(
            name,
            FunctionValueType::new(DataType::Int32, true),
            DecimalOverflowPolicy::OutputNull,
        );
        for exclusion in [
            WindowFrameExclusion::NoOthers,
            WindowFrameExclusion::CurrentRow,
            WindowFrameExclusion::Group,
            WindowFrameExclusion::Ties,
        ] {
            let options = WindowCallOptions::try_new(
                Some(WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start: WindowBound::UnboundedPreceding,
                    end: WindowBound::CurrentRow,
                    exclusion,
                }),
                true,
                &CompileControl::default(),
            )
            .unwrap();
            let baseline = CompileControl::default();
            let outcome = fixture.prepare(options, &baseline);
            assert_eq!(outcome.is_ok(), exclusion == WindowFrameExclusion::NoOthers);
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
                        compile_cause(fixture.prepare(options, &control).unwrap_err()),
                        cause
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}

#[test]
fn every_runtime_callback_preserves_seven_causes_required_setup_ordinary_tail_and_failed_latch() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        None,
        Some(10),
        None,
        Some(30),
        Some(40),
        None,
    ]));
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 6 }];
    let frames = small_frames();
    let rows = [1, 4, 5];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    for name in NAMES {
        let fixture = Fixture::new(name, ty(&values, true), DecimalOverflowPolicy::OutputNull);
        let prepared = fixture
            .prepare(options(true), &CompileControl::default())
            .unwrap();
        let input = geometry(&prepared, &arguments, &peers, &frames);
        for stage in 0..4 {
            let baseline = Control::default();
            if stage == 0 {
                prepared.clone().begin_partition(input, &baseline).unwrap();
            } else {
                let mut part = prepared
                    .clone()
                    .begin_partition(input, &Control::default())
                    .unwrap();
                match stage {
                    1 => {
                        part.evaluate(selection, 3, &baseline).unwrap();
                    }
                    2 => {
                        assert!(matches!(
                            part.evaluate(Selection::all(7), 7, &baseline),
                            Err(KernelFailure::InvalidProgram(_))
                        ));
                    }
                    _ => part.finish(&baseline).unwrap(),
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
                            prepared
                                .clone()
                                .begin_partition(input, &control)
                                .map(|_| ())
                                .unwrap_err(),
                            cause
                        );
                    } else {
                        let mut part = prepared
                            .clone()
                            .begin_partition(input, &Control::default())
                            .unwrap();
                        let result = match stage {
                            1 => part.evaluate(selection, 3, &control).map(|_| ()),
                            2 => part.evaluate(Selection::all(7), 7, &control).map(|_| ()),
                            _ => part.finish(&control),
                        };
                        assert_eq!(result.unwrap_err(), cause);
                        let clean = Control::default();
                        assert!(matches!(
                            part.evaluate(selection, 3, &clean),
                            Err(KernelFailure::InstanceFailed)
                        ));
                        assert!(matches!(
                            part.finish(&clean),
                            Err(KernelFailure::InstanceFailed)
                        ));
                        assert!(clean.trace.lock().unwrap().is_empty());
                    }
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}

#[test]
fn wide_original_null_scan_addresses_and_take_cross_real_quantum_with_sampled_first_causes() {
    let values: ArrayRef = Arc::new(Int32Array::from(
        (0..320)
            .map(|row| (row != 0).then_some(row))
            .collect::<Vec<_>>(),
    ));
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 320 }];
    let frames = vec![WindowRowRange { start: 0, end: 320 }; 320];
    for name in NAMES {
        let fixture = Fixture::new(name, ty(&values, true), DecimalOverflowPolicy::OutputNull);
        let prepared = fixture
            .prepare(options(true), &CompileControl::default())
            .unwrap();
        let input = geometry(&prepared, &arguments, &peers, &frames);
        for setup in [true, false] {
            let baseline = Control::default();
            if setup {
                prepared.clone().begin_partition(input, &baseline).unwrap();
            } else {
                let mut part = prepared
                    .clone()
                    .begin_partition(input, &Control::default())
                    .unwrap();
                let output = part.evaluate(Selection::all(320), 320, &baseline).unwrap();
                assert_eq!(
                    ints(&output),
                    vec![Some(if name == "first_value" { 1 } else { 319 }); 320]
                );
            }
            let trace = baseline.trace.lock().unwrap().clone();
            let quantum = trace.iter().position(|units| *units == 256).unwrap();
            for at in [0, quantum, trace.len() - 1] {
                for cause in causes() {
                    let control = Control {
                        trace: Mutex::default(),
                        refusal: Some((at, cause.clone())),
                    };
                    let outcome = if setup {
                        prepared
                            .clone()
                            .begin_partition(input, &control)
                            .map(|_| ())
                    } else {
                        let mut part = prepared
                            .clone()
                            .begin_partition(input, &Control::default())
                            .unwrap();
                        part.evaluate(Selection::all(320), 320, &control)
                            .map(|_| ())
                    };
                    assert_eq!(outcome.unwrap_err(), cause);
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
        assert!(
            prepared.partition_retained_upper_bound(320).unwrap()
                > prepared.partition_retained_upper_bound(0).unwrap()
        );
        assert_eq!(
            prepared.partition_retained_upper_bound(usize::MAX),
            Err(KernelFailure::ResourceExhausted)
        );
    }
}

#[test]
fn required_original_contract_complete_child_and_host_capacity_refuse_without_new_geometry_or_defaults()
 {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
    let fixture = Fixture::new(
        "first_value",
        ty(&values, false),
        DecimalOverflowPolicy::OutputNull,
    );
    let a = fixture
        .prepare(options(false), &CompileControl::default())
        .unwrap();
    let b = fixture
        .prepare(options(false), &CompileControl::default())
        .unwrap();
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 2 }];
    let frames = [WindowRowRange { start: 0, end: 2 }; 2];
    let baseline = Control::default();
    assert!(matches!(
        a.clone()
            .begin_partition(geometry(&b, &arguments, &peers, &frames), &baseline),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(
        baseline
            .trace
            .lock()
            .unwrap()
            .last()
            .is_some_and(|units| *units > 0)
    );
    let input = geometry(&a, &arguments, &peers, &frames);
    let mut part = a
        .clone()
        .begin_partition(input, &Control::default())
        .unwrap();
    assert_eq!(
        ints(
            &part
                .evaluate(Selection::all(2), 2, &Control::default())
                .unwrap()
        ),
        [Some(1), Some(1)]
    );
    let mut part = a
        .clone()
        .begin_partition(input, &Control::default())
        .unwrap();
    assert!(matches!(
        part.evaluate(Selection::all(2), 1, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        part.finish(&Control::default()),
        Err(KernelFailure::InstanceFailed)
    ));
    let wrong: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
    assert!(
        FullPartitionWindowInput::try_new(
            a.contract(),
            2,
            &[EvaluatedArgument::Column(&wrong)],
            &[],
            &Control::default()
        )
        .is_err()
    );
    let short: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    assert!(
        FullPartitionWindowInput::try_new(
            a.contract(),
            2,
            &[EvaluatedArgument::Column(&short)],
            &[],
            &Control::default()
        )
        .is_err()
    );
    let failed: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(2)]));
    let failed = SelectedValues::try_new(
        Selection::all(2),
        values.data_type(),
        failed,
        vec![RowDataError::new(0, "required child")].into_boxed_slice(),
    )
    .unwrap();
    assert!(
        FullPartitionWindowInput::try_new(
            a.contract(),
            2,
            &[EvaluatedArgument::SelectedColumn(&failed)],
            &[],
            &Control::default()
        )
        .is_err()
    );
    let full =
        FullPartitionWindowInput::try_new(a.contract(), 2, &arguments, &[], &Control::default())
            .unwrap();
    assert!(
        WindowPartitionInput::try_new(full, &peers, &frames[..1], &Control::default()).is_err()
    );
    assert!(
        WindowPartitionInput::try_new(
            full,
            &[WindowRowRange { start: 1, end: 2 }],
            &frames,
            &Control::default()
        )
        .is_err()
    );
    let raw_args = vec![fixture.arguments[0].clone(), fixture.arguments[0].clone()];
    assert!(
        fixture
            .catalog
            .resolve_bound_user(
                "first_value",
                FunctionKind::Window,
                FunctionBindingRequest {
                    arguments: &raw_args,
                    logical_argument_count: 2,
                    expected_result_type: None
                },
                &CompileControl::default()
            )
            .is_err()
    );
}

#[test]
fn nonnull_float_bits_and_empty_partition_keep_original_options_and_nullable_result() {
    let bits = [
        0x7ff8_0000_0000_0042,
        0x8000_0000_0000_0000,
        0,
        0x7ff0_0000_0000_0000,
    ];
    let values: ArrayRef = Arc::new(Float64Array::from(bits.map(f64::from_bits).to_vec()));
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 4 }];
    let frames = [
        WindowRowRange { start: 0, end: 4 },
        WindowRowRange { start: 1, end: 2 },
        WindowRowRange { start: 2, end: 3 },
        WindowRowRange { start: 3, end: 3 },
    ];
    let options = WindowCallOptions::try_new(
        Some(WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowBound::Preceding(2),
            end: WindowBound::Following(1),
            exclusion: WindowFrameExclusion::NoOthers,
        }),
        true,
        &CompileControl::default(),
    )
    .unwrap();
    for name in NAMES {
        let fixture = Fixture::new(name, ty(&values, false), DecimalOverflowPolicy::OutputNull);
        let prepared = fixture
            .prepare(options, &CompileControl::default())
            .unwrap();
        assert_eq!(*prepared.contract().options(), options);
        assert!(prepared.contract().result_type().nullable());
        let mut part = WindowEvaluationPartition::begin(
            prepared.clone(),
            geometry(&prepared, &arguments, &peers, &frames),
            &Control::default(),
        )
        .unwrap();
        let output = part
            .evaluate(Selection::all(4), 4, &Control::default())
            .unwrap();
        let array = output
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            array.value(0).to_bits(),
            if name == "first_value" {
                bits[0]
            } else {
                bits[3]
            }
        );
        assert_eq!(array.value(1).to_bits(), bits[1]);
        assert_eq!(array.value(2).to_bits(), bits[2]);
        assert!(array.is_null(3));
        let empty: ArrayRef = Arc::new(Float64Array::from(Vec::<f64>::new()));
        let arguments = [EvaluatedArgument::Column(&empty)];
        let mut part = WindowEvaluationPartition::begin(
            prepared.clone(),
            geometry(&prepared, &arguments, &[], &[]),
            &Control::default(),
        )
        .unwrap();
        assert!(
            part.evaluate(Selection::all(0), 0, &Control::default())
                .unwrap()
                .values()
                .is_empty()
        );
        part.finish(&Control::default()).unwrap();
    }
}

#[test]
fn wide_full_field_binding_observation_crosses_actual_compile_quantum_without_type_retag() {
    let fields: arrow_schema::Fields = (0..320)
        .map(|at| {
            Arc::new(
                Field::new(format!("source-{at}"), DataType::Int32, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "original".into(),
                        format!("value-{at}"),
                    )]),
                ),
            )
        })
        .collect::<Vec<_>>()
        .into();
    let source =
        FunctionValueType::from_legacy_declared_projection(DataType::Struct(fields), false);
    for name in NAMES {
        let fixture = Fixture::new(name, source.clone(), DecimalOverflowPolicy::ReportError);
        let baseline = CompileControl::default();
        fixture.prepare(options(true), &baseline).unwrap();
        let trace = baseline.trace.lock().unwrap().clone();
        let quantum = trace.iter().position(|(_, units)| *units == 256).unwrap();
        // Actual source/full-binding traversal; only entry, first quantum and
        // final tail are sampled here. The small test covers every callback.
        for at in [0, quantum, trace.len() - 1] {
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
                    compile_cause(fixture.prepare(options(true), &control).unwrap_err()),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn actual_window_invocation_output_validation_keeps_every_original_control_cause() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        None,
        Some(10),
        None,
        Some(30),
        Some(40),
        None,
    ]));
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = [WindowRowRange { start: 0, end: 6 }];
    let frames = small_frames();
    let rows = [1, 4, 5];
    for name in NAMES {
        let fixture = Fixture::new(name, ty(&values, true), DecimalOverflowPolicy::OutputNull);
        let prepared = fixture
            .prepare(options(true), &CompileControl::default())
            .unwrap();
        let input = geometry(&prepared, &arguments, &peers, &frames);
        for selection in [
            Selection::all(6),
            Selection::try_sparse(6, &rows).unwrap(),
            Selection::try_sparse(6, &[]).unwrap(),
        ] {
            let run = |control: &Control| {
                // Setup is separate. Exercise the real invocation wrapper and
                // its exact output validator rather than only the leaf body.
                let mut part =
                    WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default())
                        .unwrap();
                let outcome = part.evaluate(selection, selection.len(), control);
                if outcome.is_err() {
                    assert!(matches!(
                        part.evaluate(selection, selection.len(), control),
                        Err(KernelFailure::InstanceFailed)
                    ));
                    assert!(matches!(
                        part.finish(control),
                        Err(KernelFailure::InstanceFailed)
                    ));
                }
                outcome
            };
            let baseline = Control::default();
            run(&baseline).unwrap();
            let trace = baseline.trace.lock().unwrap().clone();
            for at in 0..trace.len() {
                for cause in causes() {
                    let control = Control {
                        trace: Mutex::default(),
                        refusal: Some((at, cause.clone())),
                    };
                    let actual = run(&control);
                    assert!(
                        matches!(&actual, Err(error) if error == &cause),
                        "at={at}, cause={cause:?}, actual={actual:?}"
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}
