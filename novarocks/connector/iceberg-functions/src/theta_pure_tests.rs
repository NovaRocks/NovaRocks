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
use arrow_array::*;
use arrow_schema::{DataType, TimeUnit};
use novarocks_functions::ScopedRelationalEffects;
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    EvaluationDemand, EvaluationRegionId, ExpressionEffectContext, ExpressionUseId,
    FunctionInstanceState, FunctionNullBehavior, ObservableEffects, PureCompileControl,
    RelationCallId, RelationEffectContext, SemanticParameters,
};
use std::{
    mem::MaybeUninit,
    sync::{Arc, Mutex},
    time::Duration,
};

const SUFFIXES: [&str; 17] = [
    "boolean",
    "tinyint",
    "smallint",
    "int",
    "long",
    "float",
    "double",
    "decimal",
    "date",
    "time-micros",
    "timestamp-micros",
    "timestamp-nanos",
    "string",
    "large-string",
    "binary",
    "large-binary",
    "fixed",
];
const EMPTY: [u8; 8] = [1, 3, 3, 0, 0, 0x1e, 0, 0];

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
            assert!(at <= stop, "compile callback after first refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
#[derive(Default)]
struct RuntimeControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "runtime callback after first refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("Theta never waits")
    }
}
fn compile_causes() -> [CompileControlError; 4] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
        CompileControlError::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        ),
    ]
}
fn runtime_causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::Internal(KernelDiagnostic::new("original internal")),
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
        KernelFailure::Operational(KernelDiagnostic::new("original operational")),
        KernelFailure::InstanceFailed,
    ]
}
fn context() -> RelationEffectContext {
    RelationEffectContext::call(RelationCallId::new(u32::MAX))
}
fn catalog() -> PureEngineFunctionCatalog {
    let mut builder = EngineFunctionCatalogBuilder::new();
    // The real bundle installs the same provider-owned definition used by the
    // server; these independent records describe only this installed family.
    IcebergFunctionBundle.contribute(&mut builder).unwrap();
    builder
        .seal_pure(SUFFIXES.map(|suffix| InstalledPureKernel {
            function: FunctionId::try_new("parametric.aggregate/$iceberg_theta_stat/v1").unwrap(),
            kind: FunctionKind::Aggregate,
            implementation: PureImplementationDeclaration {
                overload:
                    FunctionOverloadId::try_new(format!("iceberg/theta-stat/{suffix}/v1")).unwrap(),
                implementation:
                    PureImplementationId::try_new(ICEBERG_THETA_IMPLEMENTATION_IDENTITY).unwrap(),
                abi: PureKernelAbi::AggregateV1,
            },
            aggregate_state_format: Some(
                AggregateStateFormatIdentity::try_new(ICEBERG_THETA_STATE_FORMAT_IDENTITY).unwrap(),
            ),
        }))
        .unwrap()
}
struct Fixture {
    catalog: PureEngineFunctionCatalog,
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    args: Vec<FunctionArgument>,
    uses: [Option<ExpressionUseId>; 1],
    parameters: SemanticParameters,
    state_type: FunctionValueType,
    policy: DecimalOverflowPolicy,
}
impl Fixture {
    fn new(source: FunctionValueType) -> Self {
        Self::argument(FunctionArgument::Value {
            value_type: source,
            constant: None,
        })
    }
    fn argument(argument: FunctionArgument) -> Self {
        let catalog = catalog();
        let args = vec![argument];
        let bound = catalog
            .metadata()
            .resolve_bound_trusted(
                ICEBERG_THETA_AGGREGATE_NAME,
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &args,
                    logical_argument_count: 1,
                    expected_result_type: None,
                },
                &CompileControl::default(),
            )
            .unwrap();
        Self {
            catalog,
            id: bound.function_id,
            selected: Arc::new(bound.selected),
            args,
            uses: [Some(ExpressionUseId::new(1))],
            parameters: SemanticParameters::try_new([]).unwrap(),
            state_type: FunctionValueType::new(DataType::Binary, true),
            policy: DecimalOverflowPolicy::OutputNull,
        }
    }
    fn input(&self, phase: AggregateKernelPhase) -> CallEffectInput<'_> {
        CallEffectInput {
            function_id: &self.id,
            kind: FunctionKind::Aggregate,
            selected: &self.selected,
            request: FunctionBindingRequest {
                arguments: &self.args,
                logical_argument_count: 1,
                expected_result_type: None,
            },
            argument_uses: if phase.consumes_logical_arguments() {
                CallArgumentUses::SelectedChannels(&self.uses)
            } else {
                CallArgumentUses::AggregateMerge {
                    phase,
                    state_context: ExpressionEffectContext {
                        use_id: ExpressionUseId::new(2),
                        region: EvaluationRegionId::new(u32::MAX - 1),
                        demand: EvaluationDemand::Value,
                    }
                    .into(),
                    state_input_type: &self.state_type,
                }
            },
            context: context().into(),
            parameters: &self.parameters,
            environment: &[],
            decimal_overflow_policy: self.policy,
            proof_scope: CallProofScope::Relation(context().identity),
        }
    }
    fn options(&self, phase: AggregateKernelPhase) -> PureCallPreparation {
        PureCallPreparation::Aggregate {
            arguments: ScopedRelationalEffects::pure_value(context()),
            options: AggregatePreparationOptions {
                state_interpretation: None,
                phase,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: (!phase.consumes_logical_arguments())
                    .then(|| self.state_type.clone()),
            },
        }
    }
    fn prepare(
        &self,
        phase: AggregateKernelPhase,
        control: &dyn PureCompileControl,
    ) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
        self.catalog.prepare_fresh(
            self.input(phase),
            self.selected.clone(),
            self.options(phase),
            control,
        )
    }
    fn handle(&self, phase: AggregateKernelPhase) -> PreparedAggregateHandle {
        let actual = self.prepare(phase, &CompileControl::default()).unwrap();
        let PreparedPureKernel::Aggregate(handle) = actual.prepared() else {
            panic!("Theta must install AggregateV1")
        };
        handle.clone()
    }
}

// Fixture-owned inline state bytes, checked against the actual handle layout.
// This is no assertion about a production memory grant or allocator coverage.
#[repr(align(64))]
struct Storage([MaybeUninit<u8>; 4096]);
impl Storage {
    fn new() -> Self {
        Self([MaybeUninit::uninit(); 4096])
    }
    fn bytes(&mut self, handle: &PreparedAggregateHandle) -> &mut [MaybeUninit<u8>] {
        let layout = handle.state_layout();
        assert!(layout.align() <= 64);
        assert!(layout.size() <= self.0.len());
        &mut self.0[..layout.size()]
    }
}
fn update(
    handle: &PreparedAggregateHandle,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure> {
    let mut storage = Storage::new();
    let slot = handle.initialize_in(storage.bytes(handle), control)?;
    let mut states = [slot];
    let args = [argument];
    let input =
        SelectedAggregateUpdateInput::try_new(handle.contract(), selection, &args, &[], control)?;
    let mapping = vec![0; selection.len()];
    {
        let mut frame = handle.prepare_update_batch(&mut states, &mapping, input, control)?;
        frame.run(control)?;
        assert_eq!(frame.rows_processed(), selection.len());
    }
    match handle.memory_policy() {
        AggregateStateMemoryPolicy::BoundedRetained {
            max_retained_bytes_per_state,
        } => {
            assert!(states[0].retained_heap_bytes() <= max_retained_bytes_per_state);
        }
        _ => panic!("Theta has variable retained state"),
    }
    handle.emit(&states, &[0], 1, control)
}
fn merge(
    handle: &PreparedAggregateHandle,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure> {
    let mut storage = Storage::new();
    let slot = handle.initialize_in(storage.bytes(handle), control)?;
    let mut states = [slot];
    let input =
        SelectedAggregateMergeInput::try_new(handle.contract(), selection, argument, control)?;
    let mapping = vec![0; selection.len()];
    {
        let mut frame = handle.prepare_merge_batch(&mut states, &mapping, input, control)?;
        frame.run(control)?;
        assert_eq!(frame.rows_processed(), selection.len());
    }
    handle.emit(&states, &[0], 1, control)
}
fn body(array: &ArrayRef) -> &[u8] {
    assert_eq!(array.data_type(), &DataType::Binary);
    assert_eq!(array.len(), 1);
    assert!(!array.is_null(0));
    array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0)
}
fn java_bytes(label: &str) -> Vec<u8> {
    let line = include_str!(
        "../../../../tests/datasketches-tck/fixtures/theta/iceberg_java62_single_value_vectors.tsv"
    )
    .lines()
    .find(|line| line.starts_with(label) && line.as_bytes().get(label.len()) == Some(&b'\t'))
    .unwrap();
    line.split('\t')
        .nth(2)
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
fn oracle(label: &str, actual: &ArrayRef) {
    let expected = java_bytes(label);
    if expected.len() == 8 {
        assert_eq!(body(actual), expected);
    } else {
        // Java's read-only advisory header bit differs. Retained hash bytes
        // are the independent original Iceberg canonicalization oracle.
        assert_eq!(&body(actual)[8..], &expected[8..], "{label}");
        assert_eq!(estimate_compact_theta(body(actual)).unwrap(), 1.0);
    }
}
fn profiles() -> Vec<(&'static str, ArrayRef, Option<&'static str>)> {
    vec![
        (
            "boolean",
            Arc::new(BooleanArray::from(vec![false])),
            Some("boolean_false"),
        ),
        ("tinyint", Arc::new(Int8Array::from(vec![-128])), None),
        ("smallint", Arc::new(Int16Array::from(vec![-129])), None),
        (
            "int",
            Arc::new(Int32Array::from(vec![-123_456_789])),
            Some("int32_negative"),
        ),
        (
            "long",
            Arc::new(Int64Array::from(vec![-1_234_567_890_123_456_789])),
            Some("int64_negative"),
        ),
        (
            "float",
            Arc::new(Float32Array::from(vec![f32::from_bits(0x8000_0000)])),
            Some("float_negative_zero"),
        ),
        (
            "double",
            Arc::new(Float64Array::from(vec![f64::from_bits(
                0x7ff8_0000_0000_0001,
            )])),
            Some("double_nan_payload"),
        ),
        (
            "decimal",
            Arc::new(
                Decimal128Array::from(vec![-129])
                    .with_precision_and_scale(38, 4)
                    .unwrap(),
            ),
            Some("decimal_negative"),
        ),
        (
            "date",
            Arc::new(Date32Array::from(vec![-12_345])),
            Some("date_negative"),
        ),
        (
            "time-micros",
            Arc::new(Time64MicrosecondArray::from(vec![1_234_567_890])),
            Some("time_micros"),
        ),
        (
            "timestamp-micros",
            Arc::new(
                TimestampMicrosecondArray::from(vec![-1_234_567_890_123]).with_timezone("UTC"),
            ),
            Some("timestamp_micros"),
        ),
        (
            "timestamp-nanos",
            Arc::new(TimestampNanosecondArray::from(vec![
                1_234_567_890_123_456_789,
            ])),
            Some("timestamp_nanos"),
        ),
        (
            "string",
            Arc::new(StringArray::from(vec!["NovaRocks-雪"])),
            Some("utf8"),
        ),
        (
            "large-string",
            Arc::new(LargeStringArray::from(vec!["NovaRocks-雪"])),
            Some("utf8"),
        ),
        (
            "binary",
            Arc::new(BinaryArray::from(vec![&[0, 1, 255, 127][..]])),
            Some("binary"),
        ),
        (
            "large-binary",
            Arc::new(LargeBinaryArray::from(vec![&[0, 1, 255, 127][..]])),
            Some("binary"),
        ),
        (
            "fixed",
            Arc::new(FixedSizeBinaryArray::try_from_iter([[0, 1, 255, 127]].into_iter()).unwrap()),
            Some("fixed"),
        ),
    ]
}
fn constant(array: &ArrayRef, ty: FunctionValueType, ordinal: u32) -> ConstantValue {
    let field = Arc::new(ty.try_to_field("original-theta-cv").unwrap());
    let pool = ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        ConstantPolicy {
            max_rows: 1024,
            max_array_nodes: 8,
            max_logical_elements: 1024,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 8,
            max_type_nodes: 8,
            max_dictionary_depth: 0,
            max_metadata_bytes: 4096,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        },
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap();
    pool.value(ordinal).unwrap()
}

#[test]
fn theta_pure_all_seventeen_installed_profiles_keep_exact_fresh_frozen_phase_contracts() {
    let mut seen = Vec::new();
    for (suffix, array, _) in profiles() {
        let ty = FunctionValueType::new(array.data_type().clone(), true);
        let mut f = Fixture::new(ty.clone());
        seen.push(suffix);
        assert_eq!(
            f.selected.overload.as_str(),
            format!("iceberg/theta-stat/{suffix}/v1")
        );
        assert_eq!(
            f.selected.argument_types.as_ref(),
            &[FunctionArgumentType::Value(ty)]
        );
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            f.policy = policy;
            for phase in [
                AggregateKernelPhase::Single,
                AggregateKernelPhase::Partial,
                AggregateKernelPhase::Intermediate,
                AggregateKernelPhase::Final,
            ] {
                let fresh = f.prepare(phase, &CompileControl::default()).unwrap();
                let frozen = f
                    .catalog
                    .prepare_frozen(
                        f.input(phase),
                        f.selected.clone(),
                        fresh.call_contract().effects(),
                        f.options(phase),
                        &CompileControl::default(),
                    )
                    .unwrap();
                for actual in [&fresh, &frozen] {
                    assert!(Arc::ptr_eq(
                        actual.call_contract().selected_owner(),
                        &f.selected
                    ));
                    assert_eq!(actual.call_contract().decimal_overflow_policy(), policy);
                    let effects = actual.call_contract().effects();
                    assert_eq!(effects.null_behavior, FunctionNullBehavior::CalledOnNull);
                    assert_eq!(effects.argument_control, ArgumentControl::Aggregate);
                    assert_eq!(
                        effects.instance_state,
                        FunctionInstanceState::AggregateInstance
                    );
                    assert_eq!(
                        effects.own_row_error,
                        FunctionIntrinsicRowError::NotRowEvaluated
                    );
                    assert_eq!(effects.observable_effects, ObservableEffects::NONE);
                    let PreparedPureKernel::Aggregate(handle) = actual.prepared() else {
                        panic!("AggregateV1")
                    };
                    assert_eq!(handle.contract().phase(), phase);
                    assert_eq!(
                        handle.contract().intermediate_type(),
                        &FunctionValueType::new(DataType::Binary, false)
                    );
                    assert_eq!(
                        handle.contract().final_type(),
                        &FunctionValueType::new(DataType::Binary, false)
                    );
                    assert_eq!(
                        handle.contract().state_format().as_str(),
                        ICEBERG_THETA_STATE_FORMAT_IDENTITY
                    );
                }
            }
        }
    }
    assert_eq!(seen, SUFFIXES);
}

#[test]
fn theta_pure_seventeen_real_update_carriers_match_independent_iceberg_oracles() {
    for (suffix, source, label) in profiles() {
        let f = Fixture::new(FunctionValueType::new(source.data_type().clone(), true));
        let out = update(
            &f.handle(AggregateKernelPhase::Single),
            EvaluatedArgument::Column(&source),
            Selection::all(1),
            &RuntimeControl::default(),
        )
        .unwrap();
        if let Some(label) = label {
            oracle(label, &out);
        } else {
            assert_eq!(estimate_compact_theta(body(&out)).unwrap(), 1.0, "{suffix}");
            let value = if suffix == "tinyint" { -128 } else { -129 };
            let wide: ArrayRef = Arc::new(Int32Array::from(vec![value]));
            let wide_fixture = Fixture::new(FunctionValueType::new(DataType::Int32, false));
            let reference = update(
                &wide_fixture.handle(AggregateKernelPhase::Single),
                EvaluatedArgument::Column(&wide),
                Selection::all(1),
                &RuntimeControl::default(),
            )
            .unwrap();
            assert_eq!(body(&out), body(&reference), "Iceberg INT sign extension");
        }
    }
}

#[test]
fn theta_pure_selected_slice_compact_scalar_and_nonzero_cv_preserve_original_addresses() {
    let f = Fixture::new(FunctionValueType::new(DataType::Int64, true));
    let handle = f.handle(AggregateKernelPhase::Single);
    let rows = [1, 3];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let array: ArrayRef = Arc::new(
        Int64Array::from(vec![
            Some(999),
            Some(8),
            None,
            Some(8),
            Some(-1_234_567_890_123_456_789),
            Some(8),
        ])
        .slice(1, 5),
    );
    let out = update(
        &handle,
        EvaluatedArgument::Column(&array),
        selection,
        &RuntimeControl::default(),
    )
    .unwrap();
    assert_eq!(estimate_compact_theta(body(&out)).unwrap(), 1.0);
    oracle("int64_negative", &out);
    let scalar: ArrayRef = Arc::new(Int64Array::from(vec![Some(-1_234_567_890_123_456_789)]));
    let out = update(
        &handle,
        EvaluatedArgument::Scalar(&scalar),
        selection,
        &RuntimeControl::default(),
    )
    .unwrap();
    assert_eq!(estimate_compact_theta(body(&out)).unwrap(), 1.0);
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        Arc::new(Int64Array::from(vec![
            None,
            Some(-1_234_567_890_123_456_789),
        ])),
        Box::default(),
    )
    .unwrap();
    let out = update(
        &handle,
        EvaluatedArgument::SelectedColumn(&compact),
        selection,
        &RuntimeControl::default(),
    )
    .unwrap();
    assert_eq!(estimate_compact_theta(body(&out)).unwrap(), 1.0);
    let pool: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(999),
        Some(-1_234_567_890_123_456_789),
        Some(7),
    ]));
    let cv = constant(&pool, FunctionValueType::new(DataType::Int64, false), 1);
    let original_field = cv.pool().field_ref().clone();
    let constant_fixture = Fixture::argument(FunctionArgument::Value {
        value_type: cv.pool().value_type().clone(),
        constant: Some(cv.clone()),
    });
    let out = update(
        &constant_fixture.handle(AggregateKernelPhase::Single),
        EvaluatedArgument::Constant(&cv),
        selection,
        &RuntimeControl::default(),
    )
    .unwrap();
    assert_eq!(estimate_compact_theta(body(&out)).unwrap(), 1.0);
    let FunctionArgument::Value {
        constant: Some(captured),
        ..
    } = &constant_fixture.args[0]
    else {
        panic!("original CV")
    };
    oracle("int64_negative", &out);
    assert_eq!(captured.ordinal(), 1);
    assert!(Arc::ptr_eq(captured.pool().field_ref(), &original_field));
    assert!(Arc::ptr_eq(captured.pool().array(), cv.pool().array()));
    let null: ArrayRef = Arc::new(Int64Array::from(vec![None]));
    assert_eq!(
        body(
            &update(
                &handle,
                EvaluatedArgument::Scalar(&null),
                selection,
                &RuntimeControl::default()
            )
            .unwrap()
        ),
        EMPTY
    );
    assert_eq!(
        body(
            &update(
                &handle,
                EvaluatedArgument::Column(&array),
                Selection::try_sparse(5, &[]).unwrap(),
                &RuntimeControl::default()
            )
            .unwrap()
        ),
        EMPTY
    );
}

#[test]
fn theta_pure_partitioned_partial_intermediate_final_keep_real_nonnull_compact_states() {
    let f = Fixture::new(FunctionValueType::new(DataType::Int64, true));
    let left: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(2), Some(1)]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), Some(3), None]));
    let first = update(
        &f.handle(AggregateKernelPhase::Partial),
        EvaluatedArgument::Column(&left),
        Selection::all(4),
        &RuntimeControl::default(),
    )
    .unwrap();
    let second = update(
        &f.handle(AggregateKernelPhase::Partial),
        EvaluatedArgument::Column(&right),
        Selection::all(3),
        &RuntimeControl::default(),
    )
    .unwrap();
    let states: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(body(&first)),
        None,
        Some(body(&second)),
        Some(body(&first)),
    ]));
    let mid = merge(
        &f.handle(AggregateKernelPhase::Intermediate),
        EvaluatedArgument::Column(&states),
        Selection::all(4),
        &RuntimeControl::default(),
    )
    .unwrap();
    let out = merge(
        &f.handle(AggregateKernelPhase::Final),
        EvaluatedArgument::Scalar(&mid),
        Selection::all(2),
        &RuntimeControl::default(),
    )
    .unwrap();
    assert_eq!(estimate_compact_theta(body(&out)).unwrap(), 3.0);
    let all: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let single = update(
        &f.handle(AggregateKernelPhase::Single),
        EvaluatedArgument::Column(&all),
        Selection::all(3),
        &RuntimeControl::default(),
    )
    .unwrap();
    assert_eq!(body(&out), body(&single));
    let null: ArrayRef = Arc::new(BinaryArray::from(vec![None::<&[u8]>]));
    assert_eq!(
        body(
            &merge(
                &f.handle(AggregateKernelPhase::Final),
                EvaluatedArgument::Scalar(&null),
                Selection::all(3),
                &RuntimeControl::default()
            )
            .unwrap()
        ),
        EMPTY
    );
}

#[test]
fn theta_pure_nominal_domains_and_distinct_order_stale_selection_refuse_precisely() {
    assert!(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(4),
            false,
            ValueLogicalType::Uuid
        )
        .is_err()
    );
    let catalog = catalog();
    for source in [
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .expect("valid temporary legacy value projection"),
        FunctionValueType::new(DataType::Time64(TimeUnit::Nanosecond), false),
        FunctionValueType::new(DataType::Timestamp(TimeUnit::Millisecond, None), false),
        FunctionValueType::new(
            DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Int64,
                true,
            ))),
            true,
        ),
    ] {
        let args = [FunctionArgument::Value {
            value_type: source,
            constant: None,
        }];
        assert!(
            catalog
                .metadata()
                .resolve_bound_trusted(
                    ICEBERG_THETA_AGGREGATE_NAME,
                    FunctionKind::Aggregate,
                    FunctionBindingRequest {
                        arguments: &args,
                        logical_argument_count: 1,
                        expected_result_type: None
                    },
                    &CompileControl::default()
                )
                .is_err()
        );
    }
    for (logical, array) in [
        (
            ValueLogicalType::Uuid,
            Arc::new(FixedSizeBinaryArray::try_from_iter([[0u8; 16]].into_iter()).unwrap())
                as ArrayRef,
        ),
        (
            ValueLogicalType::LargeInt,
            Arc::new(FixedSizeBinaryArray::try_from_iter([[1u8; 16]].into_iter()).unwrap()),
        ),
        (
            ValueLogicalType::Hll,
            Arc::new(BinaryArray::from(vec![&[1, 2][..]])),
        ),
        (
            ValueLogicalType::Bitmap,
            Arc::new(BinaryArray::from(vec![&[1, 2][..]])),
        ),
        (
            ValueLogicalType::Variant,
            Arc::new(LargeBinaryArray::from(vec![&[1, 2][..]])),
        ),
    ] {
        let f = Fixture::new(
            FunctionValueType::try_from_legacy_declared_projection(
                array.data_type().clone(),
                false,
                logical,
            )
            .expect("valid temporary legacy value projection"),
        );
        assert_eq!(
            estimate_compact_theta(body(
                &update(
                    &f.handle(AggregateKernelPhase::Single),
                    EvaluatedArgument::Column(&array),
                    Selection::all(1),
                    &RuntimeControl::default()
                )
                .unwrap()
            ))
            .unwrap(),
            1.0
        );
    }
    let f = Fixture::new(FunctionValueType::new(DataType::Int64, false));
    let mut distinct = f.options(AggregateKernelPhase::Single);
    let PureCallPreparation::Aggregate { options, .. } = &mut distinct else {
        unreachable!()
    };
    options.distinct = true;
    assert!(
        f.catalog
            .prepare_fresh(
                f.input(AggregateKernelPhase::Single),
                f.selected.clone(),
                distinct,
                &CompileControl::default()
            )
            .is_err()
    );
    let args = vec![
        f.args[0].clone(),
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int64, false),
            constant: None,
        },
    ];
    // The original family has no ordered-update channel author. Refusal
    // occurs at real binding; an invented two-channel selection is not lawful.
    assert!(
        f.catalog
            .metadata()
            .resolve_bound_trusted(
                ICEBERG_THETA_AGGREGATE_NAME,
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &args,
                    logical_argument_count: 1,
                    expected_result_type: None,
                },
                &CompileControl::default(),
            )
            .is_err()
    );
    let mut ordered = f.options(AggregateKernelPhase::Single);
    let PureCallPreparation::Aggregate { options, .. } = &mut ordered else {
        unreachable!()
    };
    options.order_keys = Arc::from([AggregateOrderKey {
        ascending: false,
        nulls_first: true,
    }]);
    assert!(
        f.catalog
            .prepare_fresh(
                f.input(AggregateKernelPhase::Single),
                f.selected.clone(),
                ordered,
                &CompileControl::default()
            )
            .is_err()
    );
    let mut stale = f.selected.as_ref().clone();
    let mut arguments = stale.argument_types.to_vec();
    arguments[0] = FunctionArgumentType::Value(FunctionValueType::new(DataType::Int64, true));
    stale.argument_types =
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(arguments.into_boxed_slice());
    let stale = Arc::new(stale);
    let mut input = f.input(AggregateKernelPhase::Single);
    input.selected = &stale;
    assert!(
        f.catalog
            .prepare_fresh(
                input,
                stale.clone(),
                f.options(AggregateKernelPhase::Single),
                &CompileControl::default()
            )
            .is_err()
    );
}

#[test]
fn theta_pure_malformed_compacts_are_operational_and_inactive_bytes_are_not_inputs() {
    let f = Fixture::new(FunctionValueType::new(DataType::Int64, false));
    let handle = f.handle(AggregateKernelPhase::Final);
    let empty: ArrayRef = Arc::new(BinaryArray::from(vec![EMPTY.as_slice()]));
    assert_eq!(
        body(
            &merge(
                &handle,
                EvaluatedArgument::Scalar(&empty),
                Selection::all(1),
                &RuntimeControl::default()
            )
            .unwrap()
        ),
        EMPTY
    );
    for bad in [
        vec![],
        vec![1, 3, 3],
        vec![0; ICEBERG_THETA_MAX_COMPACT_BYTES + 1],
    ] {
        let array: ArrayRef = Arc::new(BinaryArray::from(vec![bad.as_slice()]));
        assert!(matches!(
            merge(
                &handle,
                EvaluatedArgument::Column(&array),
                Selection::all(1),
                &RuntimeControl::default()
            ),
            Err(KernelFailure::Operational(_))
        ));
    }
    let source: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(&[255][..]),
        Some(EMPTY.as_slice()),
        None,
    ]));
    let rows = [1, 2];
    assert_eq!(
        body(
            &merge(
                &handle,
                EvaluatedArgument::Column(&source),
                Selection::try_sparse(3, &rows).unwrap(),
                &RuntimeControl::default()
            )
            .unwrap()
        ),
        EMPTY
    );
}

fn runtime_prefixes<T>(
    operation: impl Fn(&RuntimeControl) -> Result<T, KernelFailure>,
    good: bool,
) {
    let baseline = RuntimeControl::default();
    assert_eq!(operation(&baseline).is_ok(), good);
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.iter().any(|units| *units > 0));
    for stop in 0..trace.len() {
        for cause in runtime_causes() {
            let control = RuntimeControl {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            assert!(matches!(operation(&control), Err(actual) if actual == cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}
#[test]
fn theta_pure_original_mixed_update_merge_state_is_operational_with_original_prefixes() {
    let f = Fixture::new(FunctionValueType::new(DataType::Int64, false));
    let update_handle = f.handle(AggregateKernelPhase::Partial);
    let merge_handle = f.handle(AggregateKernelPhase::Final);
    // Deliberate misuse of the original typed state across phases tests its
    // lifecycle fault. This is not a lawful erased-host state transport.
    let update_kernel = PreparedThetaKernel {
        contract: update_handle.contract().clone(),
        original: IcebergThetaKernel,
    };
    let merge_kernel = PreparedThetaKernel {
        contract: merge_handle.contract().clone(),
        original: IcebergThetaKernel,
    };
    let values: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let states: ArrayRef = Arc::new(BinaryArray::from(vec![EMPTY.as_slice()]));
    let operation = |control: &RuntimeControl| {
        let mut state = update_kernel.create_state(control)?;
        let arguments = [EvaluatedArgument::Column(&values)];
        let input = SelectedAggregateUpdateInput::try_new(
            update_handle.contract(),
            Selection::all(1),
            &arguments,
            &[],
            control,
        )?;
        let prepared = update_kernel.prepare_update(input, control)?;
        update_kernel.update_row(&mut state, &prepared, 0, control)?;
        let input = SelectedAggregateMergeInput::try_new(
            merge_handle.contract(),
            Selection::all(1),
            EvaluatedArgument::Column(&states),
            control,
        )?;
        let prepared = merge_kernel.prepare_merge(input, control)?;
        merge_kernel.merge_row(&mut state, &prepared, 0, control)
    };
    assert!(matches!(
        operation(&RuntimeControl::default()),
        Err(KernelFailure::Operational(_))
    ));
    runtime_prefixes(operation, false);
}

#[test]
fn theta_pure_original_compile_causes_preserve_all_actual_success_and_ordinary_prefixes() {
    let f = Fixture::new(FunctionValueType::new(DataType::Int64, false));
    for distinct in [false, true] {
        let run = |control: &CompileControl| {
            let mut preparation = f.options(AggregateKernelPhase::Partial);
            let PureCallPreparation::Aggregate { options, .. } = &mut preparation else {
                unreachable!()
            };
            options.distinct = distinct;
            f.catalog.prepare_fresh(
                f.input(AggregateKernelPhase::Partial),
                f.selected.clone(),
                preparation,
                control,
            )
        };
        let baseline = CompileControl::default();
        assert_eq!(run(&baseline).is_ok(), !distinct);
        let trace = baseline.trace.into_inner().unwrap();
        assert!(trace.iter().any(|(_, units)| *units > 0));
        for stop in 0..trace.len() {
            for cause in compile_causes() {
                let control = CompileControl {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((stop, cause.clone())),
                };
                let result = run(&control);
                let actual = match result {
                    Err(FunctionSpecializationFailure::Control(actual)) => actual,
                    Err(FunctionSpecializationFailure::Kernel(KernelFailure::CompileControl(
                        actual,
                    ))) => actual,
                    Err(FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled)) => {
                        CompileControlError::Cancelled
                    }
                    Err(FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded)) => {
                        CompileControlError::DeadlineExceeded
                    }
                    Err(FunctionSpecializationFailure::Kernel(
                        KernelFailure::ResourceExhausted,
                    )) => CompileControlError::ResourceExhausted,
                    other => panic!("original compile refusal was lost: {other:?}"),
                };
                assert_eq!(actual, cause);
                if let (
                    CompileControlError::Source(actual),
                    CompileControlError::Source(original),
                ) = (&actual, &cause)
                {
                    type Source =
                        novarocks_type_contract::owned_resources::layout::LayoutResourceError;
                    assert!(std::ptr::eq(
                        actual.downcast_ref::<Source>().unwrap(),
                        original.downcast_ref::<Source>().unwrap()
                    ));
                }
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}
#[test]
fn theta_pure_erased_runtime_preserves_all_seven_original_causes_and_failed_frame_latch() {
    let f = Fixture::new(FunctionValueType::new(DataType::Int64, true));
    let handle = f.handle(AggregateKernelPhase::Single);
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]));
    runtime_prefixes(
        |control| {
            update(
                &handle,
                EvaluatedArgument::Column(&array),
                Selection::all(3),
                control,
            )
        },
        true,
    );
    let merge_handle = f.handle(AggregateKernelPhase::Final);
    let bad: ArrayRef = Arc::new(BinaryArray::from(vec![&[255][..]]));
    runtime_prefixes(
        |control| {
            merge(
                &merge_handle,
                EvaluatedArgument::Column(&bad),
                Selection::all(1),
                control,
            )
        },
        false,
    );
    let good: ArrayRef = Arc::new(BinaryArray::from(vec![EMPTY.as_slice()]));
    runtime_prefixes(
        |control| {
            merge(
                &merge_handle,
                EvaluatedArgument::Column(&good),
                Selection::all(1),
                control,
            )
        },
        true,
    );
    let runtime = RuntimeControl::default();
    let mut storage = Storage::new();
    let slot = merge_handle
        .initialize_in(storage.bytes(&merge_handle), &runtime)
        .unwrap();
    let mut states = [slot];
    let input = SelectedAggregateMergeInput::try_new(
        merge_handle.contract(),
        Selection::all(1),
        EvaluatedArgument::Column(&bad),
        &runtime,
    )
    .unwrap();
    let mapping = [0];
    let mut frame = merge_handle
        .prepare_merge_batch(&mut states, &mapping, input, &runtime)
        .unwrap();
    assert!(matches!(
        frame.run(&runtime),
        Err(KernelFailure::Operational(_))
    ));
    let before = runtime.trace.lock().unwrap().len();
    assert_eq!(frame.run(&runtime), Err(KernelFailure::InstanceFailed));
    assert_eq!(runtime.trace.lock().unwrap().len(), before);
    assert_eq!(frame.rows_processed(), 0);
}
#[test]
fn theta_pure_actual_erased_layout_owner_and_output_capacity_are_not_guessed() {
    let f = Fixture::new(FunctionValueType::new(DataType::Int64, false));
    let handle = f.handle(AggregateKernelPhase::Single);
    let foreign = f.handle(AggregateKernelPhase::Single);
    let runtime = RuntimeControl::default();
    let mut storage = Storage::new();
    let slot = handle
        .initialize_in(storage.bytes(&handle), &runtime)
        .unwrap();
    let states = [slot];
    assert!(matches!(
        foreign.emit(&states, &[0], 1, &runtime),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        handle.emit(&states, &[0], 0, &runtime),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert!(matches!(
        handle.emit(&states, &[1], 1, &runtime),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let actual = handle.emit(&states, &[0, 0], 2, &runtime).unwrap();
    assert_eq!(actual.len(), 2);
    let actual = actual.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(actual.value(0), EMPTY);
    assert_eq!(actual.value(1), EMPTY);
    drop(states);
    // The host may reuse the same aligned backing only after the unique slot
    // has actually dropped; this exercises real erasure, not a fake destructor.
    let fresh = handle
        .initialize_in(storage.bytes(&handle), &runtime)
        .unwrap();
    assert!(fresh.retained_heap_bytes() > 0);
    drop(fresh);
    let mut small = Storage::new();
    let size = handle.state_layout().size();
    assert!(matches!(
        handle.initialize_in(&mut small.0[..size - 1], &runtime),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn theta_pure_wide_selected_rows_sample_actual_wrapper_quantum_without_library_claim() {
    let f = Fixture::new(FunctionValueType::new(DataType::Int64, false));
    let handle = f.handle(AggregateKernelPhase::Partial);
    let array: ArrayRef = Arc::new(Int64Array::from(
        (0..320).map(i64::from).collect::<Vec<_>>(),
    ));
    let run = |control: &RuntimeControl| {
        update(
            &handle,
            EvaluatedArgument::Column(&array),
            Selection::all(320),
            control,
        )
    };
    let baseline = RuntimeControl::default();
    let out = run(&baseline).unwrap();
    assert_eq!(estimate_compact_theta(body(&out)).unwrap(), 320.0);
    let trace = baseline.trace.into_inner().unwrap();
    let quantum = trace
        .iter()
        .position(|units| *units == 256)
        .expect("wide original selected-carrier/serialization work reaches a real quantum");
    let positions = std::collections::BTreeSet::from([
        0,
        quantum,
        (quantum + 1).min(trace.len() - 1),
        trace.len() / 2,
        trace.len() - 1,
    ]);
    for stop in positions {
        for cause in runtime_causes() {
            let control = RuntimeControl {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            assert!(matches!(run(&control), Err(actual) if actual == cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn theta_pure_attachment_keeps_original_family_resolver_and_rejects_metadata_drift() {
    let registration = iceberg_theta_registration().unwrap();
    let family = registration.family().clone();
    let resolver = registration
        .definition()
        .binding_resolver()
        .unwrap()
        .clone();
    let owner = ThetaPureOwner::from_registration(&registration).unwrap();
    assert!(Arc::ptr_eq(&owner.family, &family));
    assert!(std::ptr::addr_eq(
        owner.resolver.as_ref(),
        resolver.as_ref()
    ));
    let attached = registration
        .try_attach_pure_aggregate(Arc::new(owner))
        .unwrap();
    assert!(Arc::ptr_eq(attached.family(), &family));
    assert!(std::ptr::addr_eq(
        attached.definition().binding_resolver().unwrap().as_ref(),
        resolver.as_ref()
    ));
    assert_eq!(
        attached
            .definition()
            .binding_declaration()
            .unwrap()
            .overloads()
            .len(),
        17
    );

    for drift in 0..5 {
        let registration = iceberg_theta_registration().unwrap();
        let mut owner = ThetaPureOwner::from_registration(&registration).unwrap();
        let mut overloads = owner.declaration.overloads().to_vec();
        let mut id = owner.declaration.function_id().clone();
        match drift {
            0 => {
                overloads[0]
                    .aggregate
                    .as_mut()
                    .unwrap()
                    .state_argument_contract = AggregateStateArgumentContract::ExactSignature
            }
            1 => {
                overloads[0].aggregate.as_mut().unwrap().state_format =
                    AggregateStateFormatIdentity::try_new("test/foreign/theta-state-v1").unwrap()
            }
            2 => id = FunctionId::try_new("test/foreign/theta/v1").unwrap(),
            3 => overloads[0].argument_pattern = "test-foreign-pattern".into(),
            4 => {
                let effect = overloads[0].effects.as_mut().unwrap();
                effect.value_stability = FunctionVolatility::Volatile;
                overloads[0].semantics = FunctionSemantics::from_effects(effect);
            }
            _ => unreachable!(),
        }
        owner.declaration =
            FunctionBindingDeclaration::try_new_complete(id, FunctionKind::Aggregate, overloads)
                .unwrap();
        assert!(
            registration
                .try_attach_pure_aggregate(Arc::new(owner))
                .is_err(),
            "metadata drift {drift}"
        );
    }
}

#[test]
fn theta_pure_measured_output_requests_keep_payload_offsets_and_numeric_resource_primary() {
    let facts = theta_output_resource_facts(3, 24).unwrap();
    assert_eq!(facts.payload_request_bytes, 24);
    assert_eq!(facts.offsets_request_bytes, 4 * std::mem::size_of::<i32>());
    assert_eq!(
        facts.state_reference_request_bytes,
        3 * std::mem::size_of::<&IcebergThetaState>()
    );
    assert_eq!(facts.library_temporary_request_bytes, 131_136);
    assert_eq!(
        facts.combined_request_bytes,
        facts.state_reference_request_bytes
            + facts.offsets_request_bytes
            + facts.payload_request_bytes
            + facts.library_temporary_request_bytes
            + facts.array_owner_request_bytes
    );
    // The empty output has no payload; its conservative library bound is a
    // request-model upper bound, not a statement of actual allocations.
    assert_eq!(
        theta_output_resource_facts(0, 0)
            .unwrap()
            .payload_request_bytes,
        0
    );
    for (rows, payload) in [(usize::MAX, 0), (1, i32::MAX as usize + 1)] {
        assert_eq!(
            theta_output_resource_facts(rows, payload),
            Err(KernelFailure::ResourceExhausted)
        );
    }
}

#[test]
fn theta_pure_actual_output_rejects_lying_state_counts_with_original_callback_prefixes() {
    struct Claimed<'a> {
        state: &'a IcebergThetaState,
        claim: usize,
        remaining: usize,
    }
    impl<'a> Iterator for Claimed<'a> {
        type Item = &'a IcebergThetaState;
        fn next(&mut self) -> Option<Self::Item> {
            if self.remaining == 0 {
                return None;
            }
            self.remaining -= 1;
            Some(self.state)
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.claim, Some(self.claim))
        }
    }
    impl ExactSizeIterator for Claimed<'_> {}
    let state = TypedAggregateKernel::create_state(&IcebergThetaKernel).unwrap();
    for (claim, remaining) in [(3, 1), (1, 2), (0, 1)] {
        let operation = |control: &RuntimeControl| {
            build_output_observed(
                Claimed {
                    state: &state,
                    claim,
                    remaining,
                },
                Some(control),
            )
        };
        assert!(matches!(
            operation(&RuntimeControl::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        runtime_prefixes(operation, false);
    }
    // The numeric extent refuses before next(), and a pending later host cause
    // must not replace it or add another observation.
    let control = RuntimeControl {
        trace: Mutex::new(Vec::new()),
        refusal: Some((
            1,
            KernelFailure::Internal(KernelDiagnostic::new("later host refusal")),
        )),
    };
    assert!(matches!(
        build_output_observed(
            Claimed {
                state: &state,
                claim: usize::MAX,
                remaining: 0
            },
            Some(&control)
        ),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert_eq!(*control.trace.lock().unwrap(), [0]);
}
