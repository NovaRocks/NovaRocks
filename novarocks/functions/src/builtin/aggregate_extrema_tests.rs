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

use super::super::aggregate_extrema_dispatch::ExtremaState as InstalledExtremaState;
use super::*;
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, FunctionInstanceState,
    FunctionNullBehavior, PureCompileControl, SemanticParameters,
};
use std::{mem::MaybeUninit, sync::Mutex, time::Duration};

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
            Some((stop, cause)) if at == stop => Err(cause),
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
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("MIN/MAX never waits")
    }
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(0),
        domain: EvaluationDomainId::new(u32::MAX),
        demand: EvaluationDemand::Value,
    }
}
fn ty(nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(arrow_schema::DataType::Int64, nullable)
}
struct Fixture {
    catalog: PureEngineFunctionCatalog,
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    args: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    parameters: SemanticParameters,
    policy: DecimalOverflowPolicy,
    state_input_type: FunctionValueType,
}
impl Fixture {
    fn new(name: &str, source: FunctionValueType, policy: DecimalOverflowPolicy) -> Self {
        let types = [source];
        let original = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                original
                    .definition(name, FunctionKind::Aggregate)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        // Independent inventory of the one actually installed MIN/MAX owner, not
        // an assertion that every builtin has a complete pure implementation.
        let catalog = builder
            .seal_pure([InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.aggregate/{name}/v1")).unwrap(),
                kind: FunctionKind::Aggregate,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!(
                        "builtin.aggregate/{name}/derived-v1"
                    ))
                    .unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.aggregate/{name}/selected-v1"
                    ))
                    .unwrap(),
                    abi: PureKernelAbi::AggregateWindowV1,
                },
                aggregate_state_format: Some(
                    AggregateStateFormatIdentity::try_new(format!("novarocks/{name}/state-v1"))
                        .unwrap(),
                ),
            }])
            .unwrap();
        let args = types
            .iter()
            .map(|value_type| FunctionArgument::Value {
                value_type: value_type.clone(),
                constant: None,
            })
            .collect::<Vec<_>>();
        let bound = catalog
            .metadata()
            .resolve_bound_user(
                name,
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &args,
                    logical_argument_count: args.len(),
                    expected_result_type: None,
                },
                &CompileControl::default(),
            )
            .unwrap();
        let state_input_type = match &bound.selected.result_type {
            FunctionResultType::Scalar(ty) => ty.clone(),
            _ => panic!("actual scalar extrema state source"),
        };
        Self {
            catalog,
            id: bound.function_id,
            selected: Arc::new(bound.selected),
            uses: (0..args.len())
                .map(|n| Some(ExpressionUseId::new(n as u32 + 1)))
                .collect(),
            args,
            parameters: SemanticParameters::try_new([]).unwrap(),
            policy,
            state_input_type,
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            function_id: &self.id,
            kind: FunctionKind::Aggregate,
            selected: &self.selected,
            request: FunctionBindingRequest {
                arguments: &self.args,
                logical_argument_count: self.args.len(),
                expected_result_type: None,
            },
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            context: context(),
            parameters: &self.parameters,
            environment: &[],
            decimal_overflow_policy: self.policy,
            proof_scope: CallProofScope::Domain(context().domain),
        }
    }
    fn input_for_phase(&self, phase: AggregateKernelPhase) -> CallEffectInput<'_> {
        let mut input = self.input();
        if !phase.consumes_logical_arguments() {
            // This fixture owns the actual nullable state carrier separately
            // from the selected logical signature. No production flow is inferred.
            input.argument_uses = crate::CallArgumentUses::AggregateMerge {
                phase,
                state_context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(u32::MAX),
                    domain: EvaluationDomainId::new(u32::MAX - 1),
                    demand: EvaluationDemand::Value,
                },
                state_input_type: &self.state_input_type,
            };
        }
        input
    }
    fn options(&self, phase: AggregateKernelPhase) -> PureCallPreparation {
        PureCallPreparation::Aggregate {
            arguments: ScopedExpressionEffects::pure_value(context()),
            options: AggregatePreparationOptions {
                state_interpretation: None,
                phase,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: (!phase.consumes_logical_arguments())
                    .then(|| self.state_input_type.clone()),
            },
        }
    }
    fn prepare(
        &self,
        phase: AggregateKernelPhase,
        control: &dyn PureCompileControl,
    ) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
        self.catalog.prepare_fresh(
            self.input_for_phase(phase),
            self.selected.clone(),
            self.options(phase),
            control,
        )
    }
    fn kernel(&self, phase: AggregateKernelPhase) -> ExtremaKernel {
        let prepared = self.prepare(phase, &CompileControl::default()).unwrap();
        let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
            panic!("actual aggregate handle")
        };
        ExtremaKernel {
            contract: handle.contract().clone(),
            operation: if self.id.as_str() == "builtin.aggregate/min/v1" {
                ExtremaOperation::Min
            } else {
                ExtremaOperation::Max
            },
        }
    }
}
fn apply(
    kernel: &ExtremaKernel,
    args: &[EvaluatedArgument<'_>],
    selection: Selection<'_>,
) -> Option<ExtremaValue> {
    let ctrl = RuntimeControl::default();
    let mut state = kernel.create_state(&ctrl).unwrap();
    let input =
        SelectedAggregateUpdateInput::try_new(&kernel.contract, selection, args, &[], &ctrl)
            .unwrap();
    let mut call = AggregateUpdateInvocation::try_new(kernel, input, &ctrl).unwrap();
    while call.next_selected_ordinal().is_some() {
        call.update_next(&mut state, &ctrl).unwrap();
    }
    assert_eq!(kernel.retained_bytes(&state), 0);
    state
}
fn emitted(kernel: &ExtremaKernel, state: &Option<ExtremaValue>) -> ArrayRef {
    kernel
        .build_final([state].into_iter(), &RuntimeControl::default())
        .unwrap()
}
fn i64_value(array: &ArrayRef) -> Option<i64> {
    let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
    (!array.is_null(0)).then(|| array.value(0))
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::Internal(KernelDiagnostic::new("original")),
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original")),
        KernelFailure::Operational(KernelDiagnostic::new("original")),
        KernelFailure::InstanceFailed,
    ]
}
fn runtime_prefixes<T>(
    operation: impl Fn(&RuntimeControl) -> Result<T, KernelFailure>,
    good: bool,
) {
    let base = RuntimeControl::default();
    assert_eq!(operation(&base).is_ok(), good);
    let trace = base.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&0));
    for stop in 0..trace.len() {
        for cause in causes() {
            let ctrl = RuntimeControl {
                trace: Mutex::new(vec![]),
                refusal: Some((stop, cause.clone())),
            };
            assert!(matches!(operation(&ctrl),Err(actual) if actual==cause));
            assert_eq!(*ctrl.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn aggregate_extrema_installed_fresh_frozen_all_phases_exact_full_types_and_policies() {
    let types = [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, Some("".into())),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Decimal128(17, -3),
        DataType::Decimal256(64, 9),
    ];
    for name in ["min", "max"] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for source in types
                .iter()
                .map(|dt| FunctionValueType::from_legacy_declared_projection(dt.clone(), false))
                .chain([FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    true,
                    ValueLogicalType::LargeInt,
                )
                .unwrap()])
            {
                let fixture = Fixture::new(name, source.clone(), policy);
                for phase in [
                    AggregateKernelPhase::Single,
                    AggregateKernelPhase::Partial,
                    AggregateKernelPhase::Intermediate,
                    AggregateKernelPhase::Final,
                ] {
                    let fresh = fixture.prepare(phase, &CompileControl::default()).unwrap();
                    let frozen = fixture
                        .catalog
                        .prepare_frozen(
                            fixture.input_for_phase(phase),
                            fixture.selected.clone(),
                            fresh.call_contract().effects(),
                            fixture.options(phase),
                            &CompileControl::default(),
                        )
                        .unwrap();
                    assert_eq!(fresh.source(), PurePreparationSource::Fresh);
                    assert_eq!(frozen.source(), PurePreparationSource::Frozen);
                    for actual in [&fresh, &frozen] {
                        assert_eq!(
                            actual.implementation().abi,
                            PureKernelAbi::AggregateWindowV1
                        );
                        assert!(std::ptr::eq(
                            actual.call_contract().selected(),
                            fixture.selected.as_ref()
                        ));
                        assert_eq!(actual.call_contract().context(), context());
                        assert_eq!(actual.call_contract().parameters(), &fixture.parameters);
                        assert_eq!(actual.call_contract().decimal_overflow_policy(), policy);
                        assert_eq!(
                            actual.call_contract().effects().instance_state,
                            FunctionInstanceState::AggregateInstance
                        );
                        assert_eq!(
                            actual.call_contract().effects().null_behavior,
                            FunctionNullBehavior::CalledOnNull
                        );
                        assert_eq!(
                            actual.call_contract().effects().own_row_error,
                            FunctionIntrinsicRowError::NotRowEvaluated
                        );
                        let PreparedPureKernel::Aggregate(handle) = actual.prepared() else {
                            panic!()
                        };
                        assert_eq!(
                            handle.contract().state_format().as_str(),
                            format!("novarocks/{name}/state-v1")
                        );
                        let mut expected = source.clone();
                        expected = expected
                            .with_nullable(true)
                            .expect("valid function nullability declaration");
                        assert_eq!(handle.contract().intermediate_type(), &expected);
                        assert_eq!(handle.contract().final_type(), &expected);
                    }
                }
            }
        }
    }
}

#[test]
fn aggregate_extrema_independent_fixed_width_and_signed_largeint_oracles() {
    // Hand-authored values cover the original fixed-width readers, not a
    // result generated by the candidate state comparison.
    macro_rules! primitive_case {
        ($array:ty,$values:expr,$min:expr,$max:expr) => {{
            let source: ArrayRef = Arc::new(<$array>::from($values));
            for (name, expected) in [("min", $min), ("max", $max)] {
                let fixture = Fixture::new(
                    name,
                    FunctionValueType::from_legacy_declared_projection(
                        source.data_type().clone(),
                        true,
                    ),
                    DecimalOverflowPolicy::ReportError,
                );
                let kernel = fixture.kernel(AggregateKernelPhase::Single);
                let state = apply(
                    &kernel,
                    &[EvaluatedArgument::Column(&source)],
                    Selection::all(source.len()),
                );
                let out = emitted(&kernel, &state);
                assert_eq!(out.data_type(), source.data_type());
                assert_eq!(
                    out.as_any().downcast_ref::<$array>().unwrap().value(0),
                    expected
                );
                assert_eq!(out.null_count(), 0);
                let empty = apply(
                    &kernel,
                    &[EvaluatedArgument::Column(&source)],
                    Selection::try_sparse(source.len(), &[]).unwrap(),
                );
                assert!(emitted(&kernel, &empty).is_null(0));
            }
        }};
    }
    primitive_case!(
        BooleanArray,
        vec![None, Some(true), Some(false)],
        false,
        true
    );
    primitive_case!(
        Int8Array,
        vec![None, Some(i8::MAX), Some(i8::MIN)],
        i8::MIN,
        i8::MAX
    );
    primitive_case!(
        Int16Array,
        vec![Some(i16::MIN), None, Some(i16::MAX)],
        i16::MIN,
        i16::MAX
    );
    primitive_case!(
        Int32Array,
        vec![Some(i32::MAX), Some(i32::MIN), None],
        i32::MIN,
        i32::MAX
    );
    primitive_case!(
        Int64Array,
        vec![Some(i64::MIN), None, Some(i64::MAX)],
        i64::MIN,
        i64::MAX
    );
    primitive_case!(
        Date32Array,
        vec![Some(-719528), None, Some(2932896)],
        -719528,
        2932896
    );
    for ty in [
        DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, Some("".into())),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ] {
        let source: ArrayRef = match &ty {
            DataType::Timestamp(TimeUnit::Second, _) => Arc::new(
                TimestampSecondArray::from(vec![Some(i64::MAX), None, Some(i64::MIN)])
                    .with_data_type(ty.clone()),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Arc::new(
                TimestampMillisecondArray::from(vec![Some(i64::MAX), None, Some(i64::MIN)])
                    .with_data_type(ty.clone()),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Arc::new(
                TimestampMicrosecondArray::from(vec![Some(i64::MAX), None, Some(i64::MIN)])
                    .with_data_type(ty.clone()),
            ),
            _ => Arc::new(
                TimestampNanosecondArray::from(vec![Some(i64::MAX), None, Some(i64::MIN)])
                    .with_data_type(ty.clone()),
            ),
        };
        for (name, expected) in [("min", i64::MIN), ("max", i64::MAX)] {
            let kernel = Fixture::new(
                name,
                FunctionValueType::from_legacy_declared_projection(ty.clone(), true),
                DecimalOverflowPolicy::OutputNull,
            )
            .kernel(AggregateKernelPhase::Single);
            let out = emitted(
                &kernel,
                &apply(
                    &kernel,
                    &[EvaluatedArgument::Column(&source)],
                    Selection::all(3),
                ),
            );
            assert_eq!(out.data_type(), &ty);
            let value = match &ty {
                DataType::Timestamp(TimeUnit::Second, _) => out
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .value(0),
                DataType::Timestamp(TimeUnit::Millisecond, _) => out
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(0),
                DataType::Timestamp(TimeUnit::Microsecond, _) => out
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(0),
                _ => out
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(0),
            };
            assert_eq!(value, expected);
        }
    }
    for source in [
        Arc::new(
            Decimal128Array::from(vec![Some(-12345), None, Some(54321)])
                .with_precision_and_scale(9, -2)
                .unwrap(),
        ) as ArrayRef,
        Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(-12345)),
                None,
                Some(i256::from_i128(54321)),
            ])
            .with_precision_and_scale(64, 19)
            .unwrap(),
        ),
    ] {
        for (name, expected) in [("min", -12345), ("max", 54321)] {
            let kernel = Fixture::new(
                name,
                FunctionValueType::from_legacy_declared_projection(
                    source.data_type().clone(),
                    true,
                ),
                DecimalOverflowPolicy::ReportError,
            )
            .kernel(AggregateKernelPhase::Single);
            let out = emitted(
                &kernel,
                &apply(
                    &kernel,
                    &[EvaluatedArgument::Column(&source)],
                    Selection::all(3),
                ),
            );
            assert_eq!(out.data_type(), source.data_type());
            if let Some(a) = out.as_any().downcast_ref::<Decimal128Array>() {
                assert_eq!(a.value(0), expected)
            } else {
                assert_eq!(
                    out.as_any()
                        .downcast_ref::<Decimal256Array>()
                        .unwrap()
                        .value(0),
                    i256::from_i128(expected)
                )
            }
        }
    }
    let bytes = [
        i128::MAX.to_be_bytes(),
        i128::MIN.to_be_bytes(),
        (-1i128).to_be_bytes(),
    ];
    let source: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter(bytes.iter().map(|v| v.as_slice())).unwrap());
    let source_ty = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    for (name, expected) in [("min", i128::MIN), ("max", i128::MAX)] {
        let kernel = Fixture::new(name, source_ty.clone(), DecimalOverflowPolicy::ReportError)
            .kernel(AggregateKernelPhase::Single);
        let out = emitted(
            &kernel,
            &apply(
                &kernel,
                &[EvaluatedArgument::Column(&source)],
                Selection::all(3),
            ),
        );
        assert_eq!(
            out.as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            &expected.to_be_bytes()
        );
    }
}

#[test]
fn aggregate_extrema_float_total_order_zero_nan_payload_and_f32_native_roundtrip() {
    for (name, expected) in [("min", 0xfff8000000000007u64), ("max", 0x7ff8000000000005)] {
        let source: ArrayRef = Arc::new(Float64Array::from(vec![
            None,
            Some(f64::from_bits(0x7ff8000000000005)),
            Some(f64::NEG_INFINITY),
            Some(-0.0),
            Some(0.0),
            Some(f64::INFINITY),
            Some(f64::from_bits(0xfff8000000000007)),
        ]));
        let kernel = Fixture::new(
            name,
            FunctionValueType::new(DataType::Float64, true),
            DecimalOverflowPolicy::OutputNull,
        )
        .kernel(AggregateKernelPhase::Single);
        let out = emitted(
            &kernel,
            &apply(
                &kernel,
                &[EvaluatedArgument::Column(&source)],
                Selection::all(7),
            ),
        );
        assert_eq!(
            out.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            expected
        );
    }
    for (name, expected) in [("min", 0x80000000u32), ("max", 0u32)] {
        let source: ArrayRef = Arc::new(Float32Array::from(vec![-0.0, 0.0]));
        let kernel = Fixture::new(
            name,
            FunctionValueType::new(DataType::Float32, false),
            DecimalOverflowPolicy::ReportError,
        )
        .kernel(AggregateKernelPhase::Single);
        let out = emitted(
            &kernel,
            &apply(
                &kernel,
                &[EvaluatedArgument::Column(&source)],
                Selection::all(2),
            ),
        );
        assert_eq!(
            out.as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            expected
        );
    }
    let source: ArrayRef = Arc::new(Float32Array::from(vec![f32::from_bits(0x7f800001)]));
    let kernel = Fixture::new(
        "max",
        FunctionValueType::new(DataType::Float32, false),
        DecimalOverflowPolicy::OutputNull,
    )
    .kernel(AggregateKernelPhase::Single);
    let out = emitted(
        &kernel,
        &apply(
            &kernel,
            &[EvaluatedArgument::Column(&source)],
            Selection::all(1),
        ),
    );
    // Original F32 state is F64: signaling NaN is quieted by native casts.
    assert_eq!(
        out.as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        0x7fc00001
    );
}

#[test]
fn aggregate_extrema_selected_slice_scalar_compact_constant_and_required_child_errors() {
    let source: ArrayRef = Arc::new(
        Int64Array::from(vec![Some(-999), Some(71), None, Some(9), Some(999)]).slice(1, 3),
    );
    let selection = Selection::try_sparse(3, &[0, 2]).unwrap();
    for (name, expected) in [("min", 9), ("max", 71)] {
        let kernel = Fixture::new(name, ty(true), DecimalOverflowPolicy::OutputNull)
            .kernel(AggregateKernelPhase::Single);
        assert_eq!(
            i64_value(&emitted(
                &kernel,
                &apply(&kernel, &[EvaluatedArgument::Column(&source)], selection)
            )),
            Some(expected)
        );
        let compact = SelectedValues::try_new(
            selection,
            &DataType::Int64,
            Arc::new(Int64Array::from(vec![71, 9])),
            Box::default(),
        )
        .unwrap();
        assert_eq!(
            i64_value(&emitted(
                &kernel,
                &apply(
                    &kernel,
                    &[EvaluatedArgument::SelectedColumn(&compact)],
                    selection
                )
            )),
            Some(expected)
        );
        let scalar: ArrayRef = Arc::new(Int64Array::from(vec![Some(-13)]));
        assert_eq!(
            i64_value(&emitted(
                &kernel,
                &apply(&kernel, &[EvaluatedArgument::Scalar(&scalar)], selection)
            )),
            Some(-13)
        );
        let pool = ConstantPool::try_new(
            Arc::new(ty(true).try_to_field("original-selected-source").unwrap()),
            ty(true),
            source.to_data(),
            ConstantPolicy {
                max_rows: 8,
                max_array_nodes: 1,
                max_logical_elements: 8,
                max_retained_buffer_bytes: 4096,
                max_type_depth: 1,
                max_type_nodes: 1,
                max_dictionary_depth: 0,
                max_metadata_bytes: 4096,
                max_library_validation_work: 65536,
                max_library_validation_bytes: 65536,
            },
            CompilePhase::Validate,
            &CompileControl::default(),
        )
        .unwrap();
        let value = pool.value(2).unwrap();
        let null = pool.value(1).unwrap();
        assert_eq!(value.ordinal(), 2);
        assert_eq!(value.pool().backing_identity(), pool.backing_identity());
        assert_eq!(
            i64_value(&emitted(
                &kernel,
                &apply(&kernel, &[EvaluatedArgument::Constant(&value)], selection)
            )),
            Some(9)
        );
        assert_eq!(
            i64_value(&emitted(
                &kernel,
                &apply(&kernel, &[EvaluatedArgument::Constant(&null)], selection)
            )),
            None
        );
        let errors = SelectedValues::try_new(
            selection,
            &DataType::Int64,
            Arc::new(Int64Array::from(vec![None, Some(9)])),
            vec![RowDataError::new(0, "required extrema child error")].into_boxed_slice(),
        )
        .unwrap();
        let args = [EvaluatedArgument::SelectedColumn(&errors)];
        assert!(
            SelectedAggregateUpdateInput::try_new(
                &kernel.contract,
                selection,
                &args,
                &[],
                &RuntimeControl::default()
            )
            .is_err()
        );
        let wrong: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let args = [EvaluatedArgument::Column(&wrong)];
        assert!(
            SelectedAggregateUpdateInput::try_new(
                &kernel.contract,
                selection,
                &args,
                &[],
                &RuntimeControl::default()
            )
            .is_err()
        );
    }
}

#[test]
fn aggregate_extrema_split_phases_distinct_idempotence_and_unsupported_options() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(-3), Some(7)]));
    for (name, expected) in [("min", -3), ("max", 7)] {
        let fixture = Fixture::new(name, ty(true), DecimalOverflowPolicy::ReportError);
        let partial = fixture.kernel(AggregateKernelPhase::Partial);
        let a = apply(
            &partial,
            &[EvaluatedArgument::Column(&source)],
            Selection::try_sparse(4, &[0, 1]).unwrap(),
        );
        let b = apply(
            &partial,
            &[EvaluatedArgument::Column(&source)],
            Selection::try_sparse(4, &[2, 3]).unwrap(),
        );
        let states = partial
            .build_intermediate([&a, &b].into_iter(), &RuntimeControl::default())
            .unwrap();
        for phase in [
            AggregateKernelPhase::Intermediate,
            AggregateKernelPhase::Final,
        ] {
            let kernel = fixture.kernel(phase);
            let ctrl = RuntimeControl::default();
            let mut state = kernel.create_state(&ctrl).unwrap();
            let input = SelectedAggregateMergeInput::try_new(
                &kernel.contract,
                Selection::all(2),
                EvaluatedArgument::Column(&states),
                &ctrl,
            )
            .unwrap();
            let mut call = AggregateMergeInvocation::try_new(&kernel, input, &ctrl).unwrap();
            while call.next_selected_ordinal().is_some() {
                call.merge_next(&mut state, &ctrl).unwrap()
            }
            assert_eq!(i64_value(&emitted(&kernel, &state)), Some(expected));
            assert_eq!(
                kernel.build_final(std::iter::empty(), &ctrl).unwrap().len(),
                0
            );
        }
        let mut options = fixture.options(AggregateKernelPhase::Single);
        if let PureCallPreparation::Aggregate { options, .. } = &mut options {
            options.distinct = true;
        }
        let prepared = fixture
            .catalog
            .prepare_fresh(
                fixture.input(),
                fixture.selected.clone(),
                options,
                &CompileControl::default(),
            )
            .unwrap();
        let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
            panic!()
        };
        let kernel = ExtremaKernel {
            contract: handle.contract().clone(),
            operation: if name == "min" {
                ExtremaOperation::Min
            } else {
                ExtremaOperation::Max
            },
        };
        assert_eq!(
            i64_value(&emitted(
                &kernel,
                &apply(
                    &kernel,
                    &[EvaluatedArgument::Column(&source)],
                    Selection::all(4)
                )
            )),
            Some(expected)
        );
        let mut options = fixture.options(AggregateKernelPhase::Final);
        if let PureCallPreparation::Aggregate { options, .. } = &mut options {
            options.distinct = true;
        }
        assert!(
            fixture
                .catalog
                .prepare_fresh(
                    fixture.input_for_phase(AggregateKernelPhase::Final),
                    fixture.selected.clone(),
                    options,
                    &CompileControl::default()
                )
                .is_err()
        );
        let mut options = fixture.options(AggregateKernelPhase::Single);
        if let PureCallPreparation::Aggregate { options, .. } = &mut options {
            options.order_keys = Arc::from([AggregateOrderKey {
                ascending: true,
                nulls_first: false,
            }]);
        }
        assert!(
            fixture
                .catalog
                .prepare_fresh(
                    fixture.input(),
                    fixture.selected.clone(),
                    options,
                    &CompileControl::default()
                )
                .is_err()
        );
        let text = Fixture::new(
            name,
            FunctionValueType::new(DataType::LargeUtf8, true),
            DecimalOverflowPolicy::ReportError,
        );
        assert!(matches!(
            text.prepare(AggregateKernelPhase::Single, &CompileControl::default()),
            Err(FunctionSpecializationFailure::Kernel(
                KernelFailure::InvalidProgram(_)
            ))
        ));
    }
}
#[repr(align(32))]
struct Storage([MaybeUninit<u8>; size_of::<InstalledExtremaState>()]);

#[test]
fn aggregate_extrema_actual_erased_group_mapping_and_all_emission_first_causes() {
    for name in ["min", "max"] {
        let fixture = Fixture::new(name, ty(true), DecimalOverflowPolicy::OutputNull);
        let prepared = fixture
            .prepare(AggregateKernelPhase::Single, &CompileControl::default())
            .unwrap();
        let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
            panic!()
        };
        assert_eq!(
            handle.state_layout(),
            Layout::new::<InstalledExtremaState>()
        );
        let setup = RuntimeControl::default();
        let mut first = Storage([MaybeUninit::uninit(); size_of::<InstalledExtremaState>()]);
        let mut second = Storage([MaybeUninit::uninit(); size_of::<InstalledExtremaState>()]);
        let mut states = [
            handle.initialize_in(&mut first.0, &setup).unwrap(),
            handle.initialize_in(&mut second.0, &setup).unwrap(),
        ];
        let source: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(-99),
            Some(7),
            None,
            Some(-3),
            Some(99),
        ]));
        let args = [EvaluatedArgument::Column(&source)];
        let input = SelectedAggregateUpdateInput::try_new(
            handle.contract(),
            Selection::try_sparse(5, &[1, 2, 3]).unwrap(),
            &args,
            &[],
            &setup,
        )
        .unwrap();
        {
            let mut call = handle
                .prepare_update_batch(&mut states, &[0, 1, 0], input, &setup)
                .unwrap();
            call.run(&setup).unwrap();
            assert_eq!(call.rows_processed(), 3);
        }
        let out = handle.emit(&states, &[1, 0, 0], 3, &setup).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(out.is_null(0));
        assert_eq!(out.value(1), if name == "min" { -3 } else { 7 });
        assert_eq!(out.value(2), out.value(1));
        let final_prepared = fixture
            .prepare(AggregateKernelPhase::Final, &CompileControl::default())
            .unwrap();
        let PreparedPureKernel::Aggregate(final_handle) = final_prepared.prepared() else {
            panic!()
        };
        assert_eq!(
            final_handle.memory_policy(),
            AggregateStateMemoryPolicy::FixedZero
        );
        let mut merged_storage =
            Storage([MaybeUninit::uninit(); size_of::<InstalledExtremaState>()]);
        let mut merged = [final_handle
            .initialize_in(&mut merged_storage.0, &setup)
            .unwrap()];
        let merge_values: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(-3)]));
        let input = SelectedAggregateMergeInput::try_new(
            final_handle.contract(),
            Selection::all(3),
            EvaluatedArgument::Column(&merge_values),
            &setup,
        )
        .unwrap();
        {
            let mut call = final_handle
                .prepare_merge_batch(&mut merged, &[0, 0, 0], input, &setup)
                .unwrap();
            call.run(&setup).unwrap();
            assert_eq!(call.rows_processed(), 3);
        }
        assert_eq!(
            i64_value(&final_handle.emit(&merged, &[0], 1, &setup).unwrap()),
            Some(if name == "min" { -3 } else { 7 })
        );
        for phase in [
            AggregateKernelPhase::Single,
            AggregateKernelPhase::Partial,
            AggregateKernelPhase::Intermediate,
            AggregateKernelPhase::Final,
        ] {
            let prepared = fixture.prepare(phase, &CompileControl::default()).unwrap();
            let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
                panic!()
            };
            let foreign = fixture.prepare(phase, &CompileControl::default()).unwrap();
            let PreparedPureKernel::Aggregate(foreign) = foreign.prepared() else {
                panic!()
            };
            for scenario in 0..5 {
                let action = |ctrl: &RuntimeControl| {
                    let setup = RuntimeControl::default();
                    let mut first =
                        Storage([MaybeUninit::uninit(); size_of::<InstalledExtremaState>()]);
                    let mut second =
                        Storage([MaybeUninit::uninit(); size_of::<InstalledExtremaState>()]);
                    let states = [
                        handle.initialize_in(&mut first.0, &setup).unwrap(),
                        handle.initialize_in(&mut second.0, &setup).unwrap(),
                    ];
                    match scenario {
                        0 => handle.emit(&states, &[1, 0, 0], 3, ctrl),
                        1 => handle.emit(&states, &[], 0, ctrl),
                        2 => handle.emit(&states, &[2], 1, ctrl),
                        3 => foreign.emit(&states, &[0], 1, ctrl),
                        _ => handle.emit(&states, &[0], 0, ctrl),
                    }
                };
                let base = RuntimeControl::default();
                let out = action(&base);
                if scenario < 2 {
                    let out = out.unwrap();
                    assert_eq!(out.len(), if scenario == 0 { 3 } else { 0 });
                    assert_eq!(out.null_count(), out.len());
                } else if scenario == 4 {
                    assert!(matches!(out, Err(KernelFailure::ResourceExhausted)));
                    assert_eq!(*base.trace.lock().unwrap(), vec![0, 1, 0]);
                } else {
                    assert!(matches!(out, Err(KernelFailure::InvalidProgram(_))));
                }
                runtime_prefixes(action, scenario < 2);
            }
        }
    }
}

#[test]
fn aggregate_extrema_complete_source_binding_stale_domains_and_metadata_refuse() {
    for name in ["min", "max"] {
        for (actual, changed) in [
            (
                FunctionValueType::new(DataType::Timestamp(TimeUnit::Microsecond, None), true),
                FunctionValueType::new(
                    DataType::Timestamp(TimeUnit::Microsecond, Some("".into())),
                    true,
                ),
            ),
            (
                FunctionValueType::new(DataType::Decimal128(18, 2), false),
                FunctionValueType::new(DataType::Decimal128(19, 2), false),
            ),
            (
                FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    true,
                    ValueLogicalType::LargeInt,
                )
                .unwrap(),
                FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    true,
                    ValueLogicalType::Uuid,
                )
                .unwrap(),
            ),
            (ty(false), ty(true)),
        ] {
            let fixture = Fixture::new(name, actual, DecimalOverflowPolicy::OutputNull);
            fixture
                .prepare(AggregateKernelPhase::Single, &CompileControl::default())
                .unwrap();
            let args = [FunctionArgument::Value {
                value_type: changed,
                constant: None,
            }];
            let mut input = fixture.input();
            input.request.arguments = &args;
            assert!(
                fixture
                    .catalog
                    .prepare_fresh(
                        input,
                        fixture.selected.clone(),
                        fixture.options(AggregateKernelPhase::Single),
                        &CompileControl::default()
                    )
                    .is_err()
            );
        }
        for ty in [
            FunctionValueType::new(DataType::FixedSizeBinary(16), true),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
            FunctionValueType::from_legacy_declared_projection(
                DataType::List(Arc::new(
                    Field::new("item", DataType::Int64, true).with_metadata(
                        std::collections::HashMap::<String, String>::from([(
                            "opaque".into(),
                            "source".into(),
                        )]),
                    ),
                )),
                true,
            ),
        ] {
            let fixture = Fixture::new(name, ty, DecimalOverflowPolicy::ReportError);
            assert!(matches!(
                fixture.prepare(AggregateKernelPhase::Single, &CompileControl::default()),
                Err(FunctionSpecializationFailure::Kernel(
                    KernelFailure::InvalidProgram(_)
                ))
            ));
        }
        let fixture = Fixture::new(name, ty(true), DecimalOverflowPolicy::OutputNull);
        let mut options = fixture.options(AggregateKernelPhase::Final);
        if let PureCallPreparation::Aggregate { options, .. } = &mut options {
            options.state_input_type = Some(FunctionValueType::new(DataType::Int32, true));
        }
        assert!(
            fixture
                .catalog
                .prepare_fresh(
                    fixture.input_for_phase(AggregateKernelPhase::Final),
                    fixture.selected.clone(),
                    options,
                    &CompileControl::default()
                )
                .is_err()
        );
        let mut selected = (*fixture.selected).clone();
        selected.result_type =
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, false));
        let selected = Arc::new(selected);
        let mut input = fixture.input();
        input.selected = &selected;
        assert!(
            fixture
                .catalog
                .prepare_fresh(
                    input,
                    selected.clone(),
                    fixture.options(AggregateKernelPhase::Single),
                    &CompileControl::default()
                )
                .is_err()
        );
    }
}

#[test]
fn aggregate_extrema_compile_every_actual_callback_success_and_ordinary_refusal() {
    for (source, good) in [
        (ty(true), true),
        (FunctionValueType::new(DataType::LargeUtf8, true), false),
    ] {
        let fixture = Fixture::new("min", source, DecimalOverflowPolicy::ReportError);
        let action = |ctrl: &CompileControl| fixture.prepare(AggregateKernelPhase::Single, ctrl);
        let base = CompileControl::default();
        assert_eq!(action(&base).is_ok(), good);
        let trace = base.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for stop in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let ctrl = CompileControl {
                    trace: Mutex::new(vec![]),
                    refusal: Some((stop, cause.clone())),
                };
                let out = action(&ctrl);
                match &out {
                    Err(FunctionSpecializationFailure::Control(actual))
                    | Err(FunctionSpecializationFailure::Kernel(KernelFailure::CompileControl(
                        actual,
                    ))) => assert_eq!(actual, &cause),
                    other => panic!("original compile control source changed: {other:?}"),
                }
                assert_eq!(*ctrl.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn aggregate_extrema_runtime_every_callback_update_merge_latch_and_wide_output() {
    let fixture = Fixture::new("min", ty(true), DecimalOverflowPolicy::OutputNull);
    let kernel = fixture.kernel(AggregateKernelPhase::Single);
    let merge = fixture.kernel(AggregateKernelPhase::Final);
    let source: ArrayRef = Arc::new(Int64Array::from(vec![Some(-3)]));
    let args = [EvaluatedArgument::Column(&source)];
    let setup = RuntimeControl::default();
    let input = SelectedAggregateUpdateInput::try_new(
        &kernel.contract,
        Selection::all(1),
        &args,
        &[],
        &setup,
    )
    .unwrap();
    let update = kernel.prepare_update(input, &setup).unwrap();
    let input = SelectedAggregateMergeInput::try_new(
        &merge.contract,
        Selection::all(1),
        EvaluatedArgument::Column(&source),
        &setup,
    )
    .unwrap();
    let merge_input = merge.prepare_merge(input, &setup).unwrap();
    runtime_prefixes(|ctrl| kernel.create_state(ctrl), true);
    runtime_prefixes(|ctrl| kernel.prepare_update(update, ctrl), true);
    runtime_prefixes(
        |ctrl| {
            let mut state = Some(ExtremaValue::I64(7));
            kernel.update_row(&mut state, &update, 0, ctrl)
        },
        true,
    );
    runtime_prefixes(|ctrl| merge.prepare_merge(merge_input, ctrl), true);
    runtime_prefixes(
        |ctrl| {
            let mut state = None;
            merge.merge_row(&mut state, &merge_input, 0, ctrl)
        },
        true,
    );
    runtime_prefixes(
        |ctrl| {
            let mut state = None;
            kernel.update_row(&mut state, &update, 1, ctrl)
        },
        false,
    );
    runtime_prefixes(
        |ctrl| kernel.build_final([&None, &Some(ExtremaValue::I64(9))].into_iter(), ctrl),
        true,
    );
    runtime_prefixes(
        |ctrl| kernel.build_final([&Some(ExtremaValue::Bool(true))].into_iter(), ctrl),
        false,
    );
    for cause in causes() {
        let ctrl = RuntimeControl {
            trace: Mutex::new(vec![]),
            refusal: Some((0, cause.clone())),
        };
        let input = SelectedAggregateUpdateInput::try_new(
            &kernel.contract,
            Selection::all(1),
            &args,
            &[],
            &setup,
        )
        .unwrap();
        let mut call = AggregateUpdateInvocation::try_new(&kernel, input, &setup).unwrap();
        let mut state = None;
        assert!(matches!(call.update_next(&mut state,&ctrl),Err(actual) if actual==cause));
        let at = ctrl.trace.lock().unwrap().len();
        assert!(matches!(
            call.update_next(&mut state, &ctrl),
            Err(KernelFailure::InstanceFailed)
        ));
        assert_eq!(ctrl.trace.lock().unwrap().len(), at);
        assert!(state.is_none());
    }
    let states = [Some(ExtremaValue::I64(7)); 320];
    let base = RuntimeControl::default();
    let out = kernel.build_final(states.iter(), &base).unwrap();
    assert_eq!(out.len(), 320);
    assert_eq!(out.null_count(), 0);
    assert!(
        out.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .iter()
            .all(|v| *v == 7)
    );
    let trace = base.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for stop in [
        0,
        trace.iter().position(|u| *u == 256).unwrap(),
        trace.len() - 1,
    ] {
        for cause in causes() {
            let ctrl = RuntimeControl {
                trace: Mutex::new(vec![]),
                refusal: Some((stop, cause.clone())),
            };
            assert!(matches!(kernel.build_final(states.iter(),&ctrl),Err(actual) if actual==cause));
            assert_eq!(*ctrl.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn aggregate_extrema_layout_and_dishonest_iterator_refuse_before_growth() {
    assert!(matches!(
        output_capacity(usize::MAX, 32),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert!(matches!(
        bitmap_bytes(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    ));
    struct Liar<'a> {
        values: std::slice::Iter<'a, Option<ExtremaValue>>,
        reported: usize,
    }
    impl<'a> Iterator for Liar<'a> {
        type Item = &'a Option<ExtremaValue>;
        fn next(&mut self) -> Option<Self::Item> {
            self.values.next()
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.reported, Some(self.reported))
        }
    }
    impl ExactSizeIterator for Liar<'_> {
        fn len(&self) -> usize {
            self.reported
        }
    }
    let kernel = Fixture::new("max", ty(true), DecimalOverflowPolicy::OutputNull)
        .kernel(AggregateKernelPhase::Single);
    let values = [Some(ExtremaValue::I64(1)), None];
    for reported in [1, 3] {
        runtime_prefixes(
            |ctrl| {
                kernel.build_final(
                    Liar {
                        values: values.iter(),
                        reported,
                    },
                    ctrl,
                )
            },
            false,
        );
    }
}

#[test]
fn installed_extrema_dispatch_clone_honors_compile_control() {
    use crate::builtin::aggregate_extrema_dispatch::PreparedExtrema;

    let fixture = Fixture::new("min", ty(true), DecimalOverflowPolicy::OutputNull);
    let kernel = PreparedExtrema::Fixed(fixture.kernel(AggregateKernelPhase::Single));
    let expected = CompileControlError::Cancelled;
    let control = CompileControl {
        refusal: Some((0, expected.clone())),
        ..Default::default()
    };

    assert_eq!(
        kernel
            .clone_for_local_phase(std::sync::Arc::clone(kernel.contract()), &control)
            .unwrap_err(),
        KernelFailure::CompileControl(expected)
    );
    assert_eq!(
        *control.trace.lock().unwrap(),
        vec![(CompilePhase::FunctionSpecialization, 0)]
    );
}

#[test]
fn installed_extrema_dispatch_borrowed_output_keeps_actual_prefix_and_typed_iterator_failures() {
    use crate::builtin::aggregate_extrema_dispatch::{ExtremaState, PreparedExtrema};
    let fixture = Fixture::new("min", ty(true), DecimalOverflowPolicy::OutputNull);
    let kernel = PreparedExtrema::Fixed(fixture.kernel(AggregateKernelPhase::Single));
    let states = [
        ExtremaState::Fixed(None),
        ExtremaState::Fixed(Some(ExtremaValue::I64(7))),
    ];
    let baseline = RuntimeControl::default();
    let output = kernel.build_final(states.iter(), &baseline).unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(7)]
    );
    let trace = baseline.trace.lock().unwrap().clone();
    for at in 0..trace.len() {
        for cause in causes() {
            let control = RuntimeControl {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(kernel.build_final(states.iter(), &control), Err(actual) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
    struct Declared<'a> {
        states: std::slice::Iter<'a, ExtremaState>,
        count: usize,
    }
    impl<'a> Iterator for Declared<'a> {
        type Item = &'a ExtremaState;
        fn next(&mut self) -> Option<Self::Item> {
            self.states.next()
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.count, Some(self.count))
        }
    }
    impl ExactSizeIterator for Declared<'_> {
        fn len(&self) -> usize {
            self.count
        }
    }
    for count in [0, 3] {
        let baseline = RuntimeControl::default();
        assert!(matches!(
            kernel.build_final(
                Declared {
                    states: states.iter(),
                    count
                },
                &baseline
            ),
            Err(KernelFailure::Internal(_))
        ));
        let trace = baseline.trace.lock().unwrap().clone();
        for at in 0..trace.len() {
            for cause in causes() {
                let control = RuntimeControl {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert!(
                    matches!(kernel.build_final(Declared { states: states.iter(), count }, &control), Err(actual) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
    let control = RuntimeControl::default();
    assert!(matches!(
        kernel.build_final(
            Declared {
                states: states.iter(),
                count: usize::MAX
            },
            &control
        ),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert_eq!(*control.trace.lock().unwrap(), vec![0]);
}

#[test]
fn installed_extrema_dispatch_counts_actual_heap_and_refuses_a_foreign_private_state() {
    use crate::builtin::aggregate_extrema_dispatch::{ExtremaState, PreparedExtrema};
    use crate::builtin::aggregate_extrema_utf8::Utf8ExtremaKernel;
    let fixed = Fixture::new("min", ty(true), DecimalOverflowPolicy::ReportError);
    let kernel = PreparedExtrema::Fixed(fixed.kernel(AggregateKernelPhase::Single));
    let text = Fixture::new(
        "min",
        FunctionValueType::new(DataType::Utf8, true),
        DecimalOverflowPolicy::ReportError,
    );
    let leaf = Utf8ExtremaKernel {
        contract: text.kernel(AggregateKernelPhase::Single).contract,
        operation: ExtremaOperation::Min,
    };
    let wrong = ExtremaState::Utf8(leaf.create_state(&RuntimeControl::default()).unwrap());
    assert_eq!(
        kernel.retained_bytes(&wrong),
        0,
        "actual empty state heap, not a diagnostic sentinel"
    );
    let baseline = RuntimeControl::default();
    assert!(matches!(
        kernel.build_final([&wrong].into_iter(), &baseline),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let trace = baseline.trace.lock().unwrap().clone();
    for at in 0..trace.len() {
        for cause in causes() {
            let control = RuntimeControl {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(kernel.build_final([&wrong].into_iter(), &control), Err(actual) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
    let values: ArrayRef = Arc::new(Int64Array::from(vec![9]));
    let args = [EvaluatedArgument::Column(&values)];
    let input = SelectedAggregateUpdateInput::try_new(
        kernel.contract(),
        Selection::all(1),
        &args,
        &[],
        &RuntimeControl::default(),
    )
    .unwrap();
    let prepared = kernel
        .prepare_update(input, &RuntimeControl::default())
        .unwrap();
    let baseline = RuntimeControl::default();
    let mut state = ExtremaState::Utf8(leaf.create_state(&RuntimeControl::default()).unwrap());
    assert!(matches!(
        kernel.update_row(&mut state, &prepared, 0, &baseline),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let trace = baseline.trace.lock().unwrap().clone();
    for at in 0..trace.len() {
        for cause in causes() {
            let mut state =
                ExtremaState::Utf8(leaf.create_state(&RuntimeControl::default()).unwrap());
            let control = RuntimeControl {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert_eq!(
                kernel.update_row(&mut state, &prepared, 0, &control),
                Err(cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            assert_eq!(kernel.retained_bytes(&state), 0);
        }
    }
}
