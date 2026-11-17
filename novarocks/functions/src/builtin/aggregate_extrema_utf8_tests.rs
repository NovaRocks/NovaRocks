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

use super::*;
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionInstanceState,
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
        region: EvaluationRegionId::new(u32::MAX),
        demand: EvaluationDemand::Value,
    }
}
fn ty(nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(arrow_schema::DataType::Utf8, nullable)
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
            proof_scope: CallProofScope::Region(context().region),
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
                    region: EvaluationRegionId::new(u32::MAX - 1),
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
    fn kernel(&self, phase: AggregateKernelPhase) -> Utf8ExtremaKernel {
        let prepared = self.prepare(phase, &CompileControl::default()).unwrap();
        let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
            panic!("actual aggregate handle")
        };
        Utf8ExtremaKernel {
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
    kernel: &Utf8ExtremaKernel,
    args: &[EvaluatedArgument<'_>],
    selection: Selection<'_>,
) -> Utf8ExtremaState {
    let control = RuntimeControl::default();
    let mut state = kernel.create_state(&control).unwrap();
    let input =
        SelectedAggregateUpdateInput::try_new(&kernel.contract, selection, args, &[], &control)
            .unwrap();
    let mut call = AggregateUpdateInvocation::try_new(kernel, input, &control).unwrap();
    while call.next_selected_ordinal().is_some() {
        call.update_next(&mut state, &control).unwrap();
    }
    assert_eq!(
        kernel.retained_bytes(&state),
        state.value.as_ref().map_or(0, Vec::capacity)
    );
    state
}
fn strings(array: &ArrayRef) -> Vec<Option<&str>> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .collect()
}
fn text(value: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(value.to_vec()))
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        internal("original"),
        invalid("original"),
        KernelFailure::Operational(KernelDiagnostic::new("original")),
        KernelFailure::InstanceFailed,
    ]
}
fn runtime_prefixes<T>(
    operation: impl Fn(&RuntimeControl) -> Result<T, KernelFailure>,
    good: bool,
) {
    let baseline = RuntimeControl::default();
    assert_eq!(operation(&baseline).is_ok(), good);
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for stop in 0..trace.len() {
        for cause in causes() {
            let control = RuntimeControl {
                trace: Mutex::default(),
                refusal: Some((stop, cause.clone())),
            };
            assert!(matches!(operation(&control), Err(actual) if actual == cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}
fn frozen(fixture: &Fixture, phase: AggregateKernelPhase) -> PureCallSpecialization {
    let fresh = fixture.prepare(phase, &CompileControl::default()).unwrap();
    fixture
        .catalog
        .prepare_frozen(
            fixture.input_for_phase(phase),
            fixture.selected.clone(),
            fresh.call_contract().effects(),
            fixture.options(phase),
            &CompileControl::default(),
        )
        .unwrap()
}

#[test]
fn utf8_extrema_real_installed_fresh_frozen_four_phases_preserve_physical_json_and_policy() {
    for name in ["min", "max"] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for logical_type in [ValueLogicalType::Physical, ValueLogicalType::Json] {
                for nullable in [false, true] {
                    let source = FunctionValueType::try_from_legacy_declared_projection(
                        DataType::Utf8,
                        nullable,
                        logical_type,
                    )
                    .expect("valid temporary legacy function value declaration");
                    let fixture = Fixture::new(name, source.clone(), policy);
                    for phase in [
                        AggregateKernelPhase::Single,
                        AggregateKernelPhase::Partial,
                        AggregateKernelPhase::Intermediate,
                        AggregateKernelPhase::Final,
                    ] {
                        let fresh = fixture.prepare(phase, &CompileControl::default()).unwrap();
                        let frozen = frozen(&fixture, phase);
                        for prepared in [&fresh, &frozen] {
                            assert!(Arc::ptr_eq(
                                prepared.call_contract().selected_owner(),
                                &fixture.selected
                            ));
                            assert_eq!(prepared.call_contract().decimal_overflow_policy(), policy);
                            assert_eq!(
                                prepared.call_contract().effects().null_behavior,
                                FunctionNullBehavior::CalledOnNull
                            );
                            assert_eq!(
                                prepared.call_contract().effects().instance_state,
                                FunctionInstanceState::AggregateInstance
                            );
                            let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
                                panic!("actual aggregate attachment")
                            };
                            let mut expected = source.clone();
                            expected = expected
                                .with_nullable(true)
                                .expect("valid function nullability declaration");
                            assert_eq!(handle.contract().final_type(), &expected);
                            assert_eq!(handle.contract().intermediate_type(), &expected);
                            assert_eq!(
                                handle.contract().state_format().as_str(),
                                format!("novarocks/{name}/state-v1")
                            );
                            assert_eq!(
                                handle.memory_policy(),
                                AggregateStateMemoryPolicy::BoundedRetained {
                                    max_retained_bytes_per_state: maximum_retained()
                                }
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn utf8_extrema_independent_bytelex_empty_null_unicode_and_nul_oracles() {
    json_byte_oracle();
    let values = text(&[
        Some("中"),
        Some("a\0z"),
        None,
        Some("é"),
        Some("a"),
        Some("中国"),
        Some(""),
        Some("a\0"),
    ]);
    for (name, expected) in [("min", ""), ("max", "中国")] {
        let fixture = Fixture::new(name, ty(true), DecimalOverflowPolicy::OutputNull);
        let kernel = fixture.kernel(AggregateKernelPhase::Single);
        let state = apply(
            &kernel,
            &[EvaluatedArgument::Column(&values)],
            Selection::all(8),
        );
        assert_eq!(
            strings(
                &kernel
                    .build_final([&state].into_iter(), &RuntimeControl::default())
                    .unwrap()
            ),
            vec![Some(expected)]
        );
        let selected = apply(
            &kernel,
            &[EvaluatedArgument::Column(&values)],
            Selection::try_sparse(8, &[1, 4, 7]).unwrap(),
        );
        assert_eq!(
            strings(
                &kernel
                    .build_final([&selected].into_iter(), &RuntimeControl::default())
                    .unwrap()
            ),
            vec![Some(if name == "min" { "a" } else { "a\0z" })]
        );
        let nulls = text(&[None, None]);
        let none = apply(
            &kernel,
            &[EvaluatedArgument::Column(&nulls)],
            Selection::all(2),
        );
        assert!(none.value.is_none());
        let empty = text(&[Some("")]);
        let some = apply(
            &kernel,
            &[EvaluatedArgument::Column(&empty)],
            Selection::all(1),
        );
        assert_eq!(some.value, Some(Vec::new()));
        assert_eq!(kernel.retained_bytes(&some), 0);
        assert_eq!(
            strings(
                &kernel
                    .build_final([&none, &some].into_iter(), &RuntimeControl::default())
                    .unwrap()
            ),
            vec![None, Some("")]
        );
    }
}

// JSON identity does not select a JSON semantic comparator: the original
// installed Utf8 path compares these complete valid JSON texts as bytes.
fn json_byte_oracle() {
    let source = text(&[Some("1"), Some("2"), Some("10"), Some("\"é\"")]);
    for (name, expected) in [("min", "\"é\""), ("max", "2")] {
        let value_type =
            FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
                .unwrap();
        let kernel = Fixture::new(name, value_type, DecimalOverflowPolicy::OutputNull)
            .kernel(AggregateKernelPhase::Single);
        let state = apply(
            &kernel,
            &[EvaluatedArgument::Column(&source)],
            Selection::all(4),
        );
        assert_eq!(
            kernel.contract.final_type().logical_type(),
            ValueLogicalType::Json
        );
        assert_eq!(
            strings(
                &kernel
                    .build_final([&state].into_iter(), &RuntimeControl::default())
                    .unwrap()
            ),
            vec![Some(expected)]
        );
    }
}

fn cv(array: ArrayRef, ordinal: u32) -> ConstantValue {
    let source = ty(true);
    let policy = ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 << 20,
        max_library_validation_bytes: 4 << 20,
    };
    ConstantPool::try_new(
        Arc::new(source.try_to_field("original").unwrap()),
        source,
        array.to_data(),
        policy,
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
#[test]
fn utf8_extrema_selected_slices_compact_scalar_nonzero_cv_never_read_inactive_rows() {
    let backing = text(&[
        Some("A hidden min"),
        Some("中"),
        None,
        Some("a"),
        Some("🦀 hidden max"),
    ]);
    let sliced = backing.slice(1, 3);
    let scalar = text(&[Some("scalar")]);
    let constant = cv(backing.clone(), 1);
    let compact_values = text(&[Some("中"), Some("a")]);
    let selection = Selection::try_sparse(5, &[1, 3]).unwrap();
    let compact =
        SelectedValues::try_new(selection, &DataType::Utf8, compact_values, Box::default())
            .unwrap();
    for name in ["min", "max"] {
        let kernel = Fixture::new(name, ty(true), DecimalOverflowPolicy::ReportError)
            .kernel(AggregateKernelPhase::Single);
        for (argument, selected, expected) in [
            (
                EvaluatedArgument::Column(&backing),
                selection,
                if name == "min" { "a" } else { "中" },
            ),
            (
                EvaluatedArgument::Column(&sliced),
                Selection::all(3),
                if name == "min" { "a" } else { "中" },
            ),
            (
                EvaluatedArgument::SelectedColumn(&compact),
                selection,
                if name == "min" { "a" } else { "中" },
            ),
            (EvaluatedArgument::Scalar(&scalar), selection, "scalar"),
            (EvaluatedArgument::Constant(&constant), selection, "中"),
        ] {
            let state = apply(&kernel, &[argument], selected);
            assert_eq!(
                strings(
                    &kernel
                        .build_final([&state].into_iter(), &RuntimeControl::default())
                        .unwrap()
                ),
                vec![Some(expected)]
            );
        }
    }
}

#[test]
fn utf8_extrema_partitioned_partial_intermediate_final_matches_independent_oracle() {
    let left = text(&[Some("é"), None, Some("z")]);
    let right = text(&[Some("中"), Some("a\0"), Some("a")]);
    for (name, expected) in [("min", "a"), ("max", "中")] {
        let fixture = Fixture::new(name, ty(true), DecimalOverflowPolicy::ReportError);
        let partial = fixture.kernel(AggregateKernelPhase::Partial);
        let a = apply(
            &partial,
            &[EvaluatedArgument::Column(&left)],
            Selection::all(3),
        );
        let b = apply(
            &partial,
            &[EvaluatedArgument::Column(&right)],
            Selection::all(3),
        );
        let intermediates = partial
            .build_intermediate([&a, &b].into_iter(), &RuntimeControl::default())
            .unwrap();
        for phase in [
            AggregateKernelPhase::Intermediate,
            AggregateKernelPhase::Final,
        ] {
            let kernel = fixture.kernel(phase);
            let control = RuntimeControl::default();
            let mut state = kernel.create_state(&control).unwrap();
            let input = SelectedAggregateMergeInput::try_new(
                &kernel.contract,
                Selection::all(2),
                EvaluatedArgument::Column(&intermediates),
                &control,
            )
            .unwrap();
            let mut call = AggregateMergeInvocation::try_new(&kernel, input, &control).unwrap();
            while call.next_selected_ordinal().is_some() {
                call.merge_next(&mut state, &control).unwrap();
            }
            let out = if phase == AggregateKernelPhase::Intermediate {
                kernel.build_intermediate([&state].into_iter(), &control)
            } else {
                kernel.build_final([&state].into_iter(), &control)
            }
            .unwrap();
            assert_eq!(strings(&out), vec![Some(expected)]);
        }
    }
}

#[test]
fn utf8_extrema_distinct_is_idempotent_order_and_foreign_sources_refuse() {
    let source = text(&[Some("z"), Some("a"), Some("a"), None]);
    for name in ["min", "max"] {
        let fixture = Fixture::new(name, ty(true), DecimalOverflowPolicy::OutputNull);
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
        let kernel = Utf8ExtremaKernel {
            contract: handle.contract().clone(),
            operation: if name == "min" {
                ExtremaOperation::Min
            } else {
                ExtremaOperation::Max
            },
        };
        let state = apply(
            &kernel,
            &[EvaluatedArgument::Column(&source)],
            Selection::all(4),
        );
        assert_eq!(
            strings(
                &kernel
                    .build_final([&state].into_iter(), &RuntimeControl::default())
                    .unwrap()
            ),
            vec![Some(if name == "min" { "a" } else { "z" })]
        );
        let mut order = fixture.options(AggregateKernelPhase::Single);
        if let PureCallPreparation::Aggregate { options, .. } = &mut order {
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
                    order,
                    &CompileControl::default()
                )
                .is_err()
        );
        let wrong: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![7]));
        assert!(
            SelectedAggregateUpdateInput::try_new(
                &kernel.contract,
                Selection::all(1),
                &[EvaluatedArgument::Column(&wrong)],
                &[],
                &RuntimeControl::default()
            )
            .is_err()
        );
        let nullable = text(&[None]);
        let strict = Fixture::new(name, ty(false), DecimalOverflowPolicy::OutputNull)
            .kernel(AggregateKernelPhase::Single);
        assert!(
            SelectedAggregateUpdateInput::try_new(
                &strict.contract,
                Selection::all(1),
                &[EvaluatedArgument::Column(&nullable)],
                &[],
                &RuntimeControl::default()
            )
            .is_err()
        );
        let errors = SelectedValues::try_new(
            Selection::all(1),
            &DataType::Utf8,
            nullable.clone(),
            vec![RowDataError::new(0, "required child")].into(),
        )
        .unwrap();
        assert!(
            SelectedAggregateUpdateInput::try_new(
                &kernel.contract,
                Selection::all(1),
                &[EvaluatedArgument::SelectedColumn(&errors)],
                &[],
                &RuntimeControl::default()
            )
            .is_err()
        );
        let foreign = fixture.kernel(AggregateKernelPhase::Single);
        let arguments = [EvaluatedArgument::Column(&source)];
        let input = SelectedAggregateUpdateInput::try_new(
            &foreign.contract,
            Selection::all(4),
            &arguments,
            &[],
            &RuntimeControl::default(),
        )
        .unwrap();
        assert!(
            kernel
                .prepare_update(input, &RuntimeControl::default())
                .is_err()
        );
    }
}

#[test]
fn utf8_extrema_leaf_replacement_preserves_old_state_for_every_actual_refusal() {
    let fixture = Fixture::new("min", ty(true), DecimalOverflowPolicy::OutputNull);
    let kernel = fixture.kernel(AggregateKernelPhase::Single);
    let old = text(&[Some("z")]);
    let candidate = text(&[Some("aé中")]);
    let args = [EvaluatedArgument::Column(&candidate)];
    let setup = RuntimeControl::default();
    let input = SelectedAggregateUpdateInput::try_new(
        &kernel.contract,
        Selection::all(1),
        &args,
        &[],
        &setup,
    )
    .unwrap();
    let input = kernel.prepare_update(input, &setup).unwrap();
    runtime_prefixes(
        |control| {
            let mut state = apply(
                &kernel,
                &[EvaluatedArgument::Column(&old)],
                Selection::all(1),
            );
            let old_capacity = kernel.retained_bytes(&state);
            let result = kernel.update_row(&mut state, &input, 0, control);
            if result.is_err() {
                assert_eq!(state.value.as_deref(), Some(b"z".as_slice()));
                assert_eq!(kernel.retained_bytes(&state), old_capacity);
            }
            result
        },
        true,
    );
    runtime_prefixes(
        |control| {
            let mut state = kernel.create_state(&setup).unwrap();
            kernel.update_row(&mut state, &input, 7, control)
        },
        false,
    );
    runtime_prefixes(|control| kernel.create_state(control), true);
    runtime_prefixes(|control| kernel.prepare_update(input, control), true);
}

#[test]
fn utf8_extrema_merge_and_emission_keep_seven_causes_and_real_ordinary_tails() {
    let fixture = Fixture::new("max", ty(true), DecimalOverflowPolicy::ReportError);
    let kernel = fixture.kernel(AggregateKernelPhase::Final);
    let source = text(&[Some("中国"), None]);
    let setup = RuntimeControl::default();
    let input = SelectedAggregateMergeInput::try_new(
        &kernel.contract,
        Selection::all(2),
        EvaluatedArgument::Column(&source),
        &setup,
    )
    .unwrap();
    let input = kernel.prepare_merge(input, &setup).unwrap();
    runtime_prefixes(|control| kernel.prepare_merge(input, control), true);
    runtime_prefixes(
        |control| {
            let mut state = kernel.create_state(&setup).unwrap();
            kernel.merge_row(&mut state, &input, 0, control)
        },
        true,
    );
    runtime_prefixes(
        |control| {
            let mut state = kernel.create_state(&setup).unwrap();
            kernel.merge_row(&mut state, &input, 3, control)
        },
        false,
    );
    let partial = fixture.kernel(AggregateKernelPhase::Partial);
    let state = apply(
        &partial,
        &[EvaluatedArgument::Column(&source)],
        Selection::all(2),
    );
    runtime_prefixes(
        |control| kernel.build_final([&state, &state].into_iter(), control),
        true,
    );
    runtime_prefixes(
        |control| kernel.build_intermediate([&state].into_iter(), control),
        true,
    );
    runtime_prefixes(|control| kernel.build_final([].into_iter(), control), true);
    struct Wrong<'a> {
        inner: std::slice::Iter<'a, Utf8ExtremaState>,
        declared: usize,
    }
    impl<'a> Iterator for Wrong<'a> {
        type Item = &'a Utf8ExtremaState;
        fn next(&mut self) -> Option<Self::Item> {
            self.inner.next()
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.declared, Some(self.declared))
        }
    }
    impl ExactSizeIterator for Wrong<'_> {
        fn len(&self) -> usize {
            self.declared
        }
    }
    let states = [state];
    for declared in [0, 2] {
        runtime_prefixes(
            |control| {
                kernel.build_final(
                    Wrong {
                        inner: states.iter(),
                        declared,
                    },
                    control,
                )
            },
            false,
        );
    }
}

#[test]
fn utf8_extrema_actual_compile_boundaries_keep_three_causes_without_after_callback() {
    for source in [ty(true), FunctionValueType::new(DataType::LargeUtf8, true)] {
        let fixture = Fixture::new("min", source.clone(), DecimalOverflowPolicy::ReportError);
        for phase in [
            AggregateKernelPhase::Single,
            AggregateKernelPhase::Partial,
            AggregateKernelPhase::Intermediate,
            AggregateKernelPhase::Final,
        ] {
            let baseline = CompileControl::default();
            let _ = fixture.prepare(phase, &baseline);
            let trace = baseline.trace.lock().unwrap().clone();
            assert!(!trace.is_empty());
            for stop in 0..trace.len() {
                for cause in [
                    CompileControlError::Cancelled,
                    CompileControlError::DeadlineExceeded,
                    CompileControlError::ResourceExhausted,
                ] {
                    let control = CompileControl {
                        trace: Mutex::default(),
                        refusal: Some((stop, cause.clone())),
                    };
                    let out = fixture.prepare(phase, &control);
                    assert!(
                        matches!(&out, Err(FunctionSpecializationFailure::Control(actual)) if *actual == cause)
                            || matches!(&out, Err(FunctionSpecializationFailure::Kernel(actual)) if *actual == crate::kernel_control::compile_failure(cause))
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
                }
            }
        }
    }
}

const STORAGE_BYTES: usize = size_of::<super::super::aggregate_extrema_dispatch::ExtremaState>();
#[repr(align(64))]
struct Storage([MaybeUninit<u8>; STORAGE_BYTES]);
#[test]
fn utf8_extrema_real_erased_owner_group_mapping_emit_and_failure_latch() {
    for name in ["min", "max"] {
        let fixture = Fixture::new(name, ty(true), DecimalOverflowPolicy::OutputNull);
        let prepared = fixture
            .prepare(AggregateKernelPhase::Single, &CompileControl::default())
            .unwrap();
        let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
            panic!()
        };
        let control = RuntimeControl::default();
        assert_eq!(handle.state_layout().size(), STORAGE_BYTES);
        assert!(handle.state_layout().align() <= 64);
        let mut a = Storage([MaybeUninit::uninit(); STORAGE_BYTES]);
        let mut b = Storage([MaybeUninit::uninit(); STORAGE_BYTES]);
        let mut states = [
            handle.initialize_in(&mut a.0, &control).unwrap(),
            handle.initialize_in(&mut b.0, &control).unwrap(),
        ];
        let source = text(&[Some("z"), None, Some("中"), Some("a")]);
        let args = [EvaluatedArgument::Column(&source)];
        let input = SelectedAggregateUpdateInput::try_new(
            handle.contract(),
            Selection::all(4),
            &args,
            &[],
            &control,
        )
        .unwrap();
        {
            let mut call = handle
                .prepare_update_batch(&mut states, &[0, 1, 0, 0], input, &control)
                .unwrap();
            call.run(&control).unwrap();
        }
        assert_eq!(
            strings(&handle.emit(&states, &[1, 0, 0], 3, &control).unwrap()),
            vec![
                None,
                Some(if name == "min" { "a" } else { "中" }),
                Some(if name == "min" { "a" } else { "中" })
            ]
        );
        runtime_prefixes(|control| handle.emit(&states, &[0, 1, 0], 3, control), true);
        runtime_prefixes(|control| handle.emit(&states, &[9], 1, control), false);
        // Every actual run callback is refused against fresh original states;
        // no equal-value second run changes the baseline's allocation trace.
        runtime_prefixes(
            |control| {
                let mut fresh_a = Storage([MaybeUninit::uninit(); STORAGE_BYTES]);
                let mut fresh_b = Storage([MaybeUninit::uninit(); STORAGE_BYTES]);
                let setup = RuntimeControl::default();
                let mut fresh = [
                    handle.initialize_in(&mut fresh_a.0, &setup).unwrap(),
                    handle.initialize_in(&mut fresh_b.0, &setup).unwrap(),
                ];
                let mut call = handle
                    .prepare_update_batch(&mut fresh, &[0, 1, 0, 0], input, &setup)
                    .unwrap();
                let result = call.run(control);
                if result.is_err() {
                    let before = control.trace.lock().unwrap().len();
                    assert_eq!(call.run(control), Err(KernelFailure::InstanceFailed));
                    assert_eq!(control.trace.lock().unwrap().len(), before);
                }
                result
            },
            true,
        );
    }
}

#[test]
fn utf8_extrema_wide_copy_quantum_natural_bound_and_no_one_mib_cap() {
    let fixture = Fixture::new("max", ty(true), DecimalOverflowPolicy::OutputNull);
    let kernel = fixture.kernel(AggregateKernelPhase::Single);
    let bytes = "中".repeat(107); // 321 real copied bytes, not fake source row steps.
    let source = text(&[Some(&bytes)]);
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
    let input = kernel.prepare_update(input, &setup).unwrap();
    let control = RuntimeControl::default();
    let mut state = kernel.create_state(&setup).unwrap();
    kernel.update_row(&mut state, &input, 0, &control).unwrap();
    let trace = control.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for stop in [
        0,
        trace.iter().position(|n| *n == 256).unwrap(),
        trace.len() - 1,
    ] {
        for cause in causes() {
            let mut state = kernel.create_state(&setup).unwrap();
            let control = RuntimeControl {
                trace: Mutex::default(),
                refusal: Some((stop, cause.clone())),
            };
            assert_eq!(
                kernel.update_row(&mut state, &input, 0, &control),
                Err(cause)
            );
            assert!(state.value.is_none());
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
    let output_control = RuntimeControl::default();
    assert_eq!(
        strings(
            &kernel
                .build_final([&state].into_iter(), &output_control)
                .unwrap()
        ),
        vec![Some(bytes.as_str())]
    );
    assert!(output_control.trace.lock().unwrap().contains(&256));
    let large = "x".repeat(1_048_577);
    let values = text(&[Some(&large)]);
    let larger = apply(
        &kernel,
        &[EvaluatedArgument::Column(&values)],
        Selection::all(1),
    );
    assert_eq!(larger.value.as_ref().unwrap().len(), 1_048_577);
    assert_eq!(
        kernel.retained_bytes(&larger),
        larger.value.as_ref().unwrap().capacity()
    );
    assert_eq!(
        maximum_retained(),
        (i32::MAX as usize).min(isize::MAX as usize)
    );
    assert!(
        crate::selected_copy::byte_interleave_payload_extent(i32::MAX as usize + 1, false).is_err()
    );
    assert!(crate::selected_copy::guarded_interleave_extent(&DataType::Utf8, usize::MAX).is_err());
}
