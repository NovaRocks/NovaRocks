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
use arrow::array::{
    Date32Array, Float32Array, NullArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};

fn checked_package(
    functions: &PureEngineFunctionCatalog,
    fragment: Fragment,
    authors: &BTreeMap<ExprId, Author>,
    result: ResultPort,
) -> Arc<FragmentPackage> {
    let (fragment, constants) = original_request_sources(fragment, authors);
    let fragment_id = fragment.id();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).unwrap();
    let mut flow_author = FlowAuthor::new();
    let mut bindings = vec![];
    for (&site, root) in roots.sites() {
        let id = flow_author.visit(
            &fragment,
            authors,
            root.expr,
            EvaluationDomainId::new(u32::MAX),
            root.demand,
        );
        bindings.push((site, id));
    }
    let ordered_uses = flow_author.uses;
    let flow = ExpressionControlFlow::try_new(
        flow_author.domains,
        ordered_uses.clone(),
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow.clone(), bindings, &Control).unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut summaries = BTreeMap::new();
    let mut frozen = vec![];
    for invocation in ordered_uses {
        let context = invocation.context;
        let scoped = if let Some(owner) = authors.get(&invocation.definition) {
            let mut children = ScopedExpressionEffects::pure_value(context);
            for (ordinal, child) in invocation.arguments.iter().enumerate() {
                children = if matches!(owner.shape, ControlShape::If | ControlShape::Coalesce) {
                    children
                        .join_control_argument(summaries[child], &flow, ordinal)
                        .unwrap()
                } else {
                    children.join_same_domain(summaries[child]).unwrap()
                };
            }
            let argument_uses = invocation
                .arguments
                .iter()
                .map(|id| Some(*id))
                .collect::<Vec<_>>();
            let preparation = if matches!(owner.shape, ControlShape::If | ControlShape::Coalesce) {
                PureCallPreparation::ControlIntrinsic {
                    arguments: children,
                }
            } else {
                PureCallPreparation::Scalar {
                    arguments: children,
                }
            };
            let token = functions
                .prepare_fresh(
                    CallEffectInput {
                        context,
                        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(
                            &argument_uses,
                        ),
                        function_id: &owner.function.function_id,
                        kind: FunctionKind::Scalar,
                        selected: owner.selected.as_ref(),
                        request: owner.request(),
                        environment: &[],
                        parameters: &parameters,
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        proof_scope: CallProofScope::Domain(context.domain),
                    },
                    owner.selected.clone(),
                    preparation,
                    &Control,
                )
                .unwrap();
            frozen.push(FrozenPhysicalCall {
                materialized_writer_input: None,
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                site: PhysicalCallSite::Expression(context.use_id),
                context,
                effects: token.call_contract().effects().clone(),
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            });
            token.effects()
        } else {
            let mut joined = ScopedExpressionEffects::pure_value(context);
            for (ordinal, child) in invocation.arguments.iter().enumerate() {
                joined = joined
                    .join_control_argument(summaries[child], &flow, ordinal)
                    .unwrap();
            }
            joined
        };
        summaries.insert(context.use_id, scoped);
    }
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, frozen, &Control).unwrap();
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([193; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants,
                fragment,
                expression_uses: uses,
                calls,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &Control).unwrap(),
            },
            package_admission(),
            &Control,
        )
        .unwrap(),
    )
}
fn compile(
    functions: &PureEngineFunctionCatalog,
    package: Arc<FragmentPackage>,
    control: &dyn PureCompileControl,
) -> Result<Arc<LocalProgram>, novarocks_local_compiler::FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let validated = validate_fragment_providers(package, &providers, &Control).unwrap();
    compile_fragment(validated, functions, options(), control).map(Arc::new)
}

#[derive(Clone, Copy)]
enum Right {
    Column,
    Random,
    Round,
}

fn nullsafe_fixture(
    ty: FunctionValueType,
    right_kind: Right,
) -> (PureEngineFunctionCatalog, Arc<FragmentPackage>) {
    let functions = catalogue(Shape::Decimal);
    let mut builder = FragmentBuilder::new(FragmentId::new(307));
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(43);
    let output = NodeId::new(0);
    let ids = [ValueId::new(903), ValueId::new(71)];
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = vec![];
    for id in ids {
        let expr = builder
            .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id,
                ty: ty.clone(),
                origin: ValueOrigin::Expr { node: input, expr },
            })
            .unwrap();
        items.push((expr, id));
    }
    builder
        .add_project(input, source, items.into_boxed_slice(), Box::from(ids))
        .unwrap();
    let mut authors = BTreeMap::new();
    let left = builder
        .add_expression(output, ty.clone(), ExprKind::Value(ids[0]))
        .unwrap();
    let right = match right_kind {
        Right::Column => builder
            .add_expression(output, ty.clone(), ExprKind::Value(ids[1]))
            .unwrap(),
        Right::Random => {
            let integer = FunctionValueType::new(DataType::Int64, false);
            let seed = literal(&mut builder, &integer, LiteralValue::Int64(42));
            call(
                &mut builder,
                &mut authors,
                author(
                    &functions,
                    "rand",
                    vec![integer_argument(integer, 42)],
                    ControlShape::Eager,
                ),
                vec![seed],
            )
        }
        Right::Round => {
            let source = builder
                .add_expression(output, ty.clone(), ExprKind::Value(ids[1]))
                .unwrap();
            let integer = FunctionValueType::new(DataType::Int64, false);
            let digits = literal(&mut builder, &integer, LiteralValue::Int64(-1));
            call(
                &mut builder,
                &mut authors,
                author(
                    &functions,
                    "round",
                    vec![argument(ty.clone(), None), integer_argument(integer, -1)],
                    ControlShape::Eager,
                ),
                vec![source, digits],
            )
        }
    };
    let result_type = FunctionValueType::new(DataType::Boolean, false);
    let expr = builder
        .add_expression(
            output,
            result_type.clone(),
            ExprKind::Binary {
                op: BinaryOperator::EqForNull,
                left,
                right,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: None,
            },
        )
        .unwrap();
    let result = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr { node: output, expr },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(expr, result)]),
            Box::from([result]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            output,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: FragmentId::new(307),
        output: fragment.nodes()[&output].output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_type),
            name: "null_safe_result".into(),
            alias: None,
            value: result,
            ty: result_type,
        }]),
    };
    let package = checked_package(&functions, fragment, &authors, result);
    (functions, package)
}
fn nullsafe_program(ty: FunctionValueType, right_kind: Right) -> Arc<LocalProgram> {
    let (functions, package) = nullsafe_fixture(ty, right_kind);
    compile(&functions, package, &Control).unwrap()
}
fn nullsafe_batch(program: &LocalProgram, left: ArrayRef, right: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![left, right],
    )
    .unwrap()
}
fn boolean_values(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<bool>> {
    output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn flat_arrays() -> Vec<(ArrayRef, ArrayRef)> {
    let mut arrays: Vec<(ArrayRef, ArrayRef)> = vec![];
    macro_rules! primitive {
        ($array:ty, $one:expr, $two:expr, $three:expr) => {
            arrays.push((
                Arc::new(<$array>::from(vec![
                    Some($one),
                    Some($two),
                    None,
                    None,
                    Some($one),
                ])),
                Arc::new(<$array>::from(vec![
                    Some($one),
                    Some($three),
                    Some($three),
                    None,
                    Some($one),
                ])),
            ));
        };
    }
    primitive!(BooleanArray, true, true, false);
    primitive!(Int8Array, 1, 2, 3);
    primitive!(Int16Array, 1, 2, 3);
    primitive!(Int32Array, 1, 2, 3);
    primitive!(Int64Array, 1, 2, 3);
    primitive!(Float32Array, 1.0, 2.0, 3.0);
    primitive!(Float64Array, 1.0, 2.0, 3.0);
    arrays.push((
        Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            None,
            None,
            Some("a"),
        ])),
        Arc::new(StringArray::from(vec![
            Some("a"),
            Some("c"),
            Some("c"),
            None,
            Some("a"),
        ])),
    ));
    primitive!(Date32Array, 1, 2, 3);
    primitive!(TimestampSecondArray, 1, 2, 3);
    primitive!(TimestampMillisecondArray, 1, 2, 3);
    primitive!(TimestampMicrosecondArray, 1, 2, 3);
    primitive!(TimestampNanosecondArray, 1, 2, 3);
    arrays.push((
        Arc::new(
            Decimal128Array::from(vec![Some(1), Some(2), None, None, Some(1)])
                .with_precision_and_scale(18, -2)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![Some(1), Some(3), Some(3), None, Some(1)])
                .with_precision_and_scale(18, -2)
                .unwrap(),
        ),
    ));
    arrays.push((Arc::new(NullArray::new(5)), Arc::new(NullArray::new(5))));
    arrays
}

#[test]
fn all_fifteen_frozen_nullsafe_profiles_keep_nonnullable_boolean_sparse_rows_and_empty_selection() {
    let arrays = flat_arrays();
    assert_eq!(arrays.len(), 15);
    for (left, right) in arrays {
        let ty = FunctionValueType::new(left.data_type().clone(), true);
        let program = nullsafe_program(ty.clone(), Right::Column);
        let snapshot = program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot();
        let root_use = snapshot.bindings()[&root()];
        let recipe = program
            .null_safe_comparison_recipe(ProgramUseRef {
                arena: novarocks_local_program::ProgramExpressionArena::Main,
                use_id: root_use,
            })
            .unwrap();
        assert_eq!(recipe.left_type(), &ty);
        assert_eq!(recipe.right_type(), &ty);
        assert!(!recipe.nullable_result());
        let flow = &snapshot.flows()[&root().arena()];
        let invocation = &flow.uses()[&root_use];
        assert_eq!(invocation.control, ControlShape::Eager);
        assert_eq!(invocation.context.demand, EvaluationDemand::Value);
        assert_eq!(invocation.arguments.len(), 2);
        for child in &invocation.arguments {
            assert_eq!(flow.uses()[child].context.domain, invocation.context.domain);
            assert_eq!(flow.uses()[child].context.demand, EvaluationDemand::Value);
        }
        assert!(matches!(
            snapshot.roots().arenas()[&root().arena()]
                .node(invocation.definition)
                .unwrap()
                .kind(),
            StaticExprKind::PreparedNullSafeComparison { .. }
        ));
        let batch = nullsafe_batch(&program, left, right);
        let rows = [1, 2, 3, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let mut evaluator = instance(&program);
        for _ in 0..2 {
            let output = evaluator.evaluate(&batch, selection, &Control).unwrap();
            let expected = if *ty.data_type() == DataType::Null {
                vec![Some(true); 4]
            } else {
                vec![Some(false), Some(false), Some(true), Some(true)]
            };
            assert_eq!(boolean_values(&output), expected);
            assert!(output.errors().is_empty());
            assert_eq!(output.values().null_count(), 0);
        }
        let empty = evaluator
            .evaluate(&batch, Selection::try_sparse(5, &[]).unwrap(), &Control)
            .unwrap();
        assert!(empty.values().is_empty());
        assert_eq!(evaluator.instances.len(), 0);
    }
}

#[test]
fn floating_nullsafe_keeps_nan_any_nonnull_and_signed_zero_in_both_operand_orders() {
    let left = [
        Some(0.0),
        Some(-0.0),
        Some(f64::from_bits(0x7ff8_0000_0000_0011)),
        Some(19.0),
        None,
        Some(f64::NEG_INFINITY),
        Some(2.0),
        None,
    ];
    let right = [
        Some(-0.0),
        Some(0.0),
        Some(-7.0),
        Some(f64::from_bits(0xfff8_0000_0000_0022)),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(3.0),
        None,
    ];
    let expected = vec![
        Some(true),
        Some(true),
        Some(true),
        Some(true),
        Some(false),
        Some(false),
        Some(false),
        Some(true),
    ];
    for ty in [DataType::Float32, DataType::Float64] {
        let program = nullsafe_program(FunctionValueType::new(ty.clone(), true), Right::Column);
        for swapped in [false, true] {
            let arrays = if ty == DataType::Float32 {
                (
                    Arc::new(Float32Array::from(
                        left.iter()
                            .map(|v| {
                                v.map(|v| {
                                    if v.is_nan() {
                                        f32::from_bits(0x7fc0_0011)
                                    } else {
                                        v as f32
                                    }
                                })
                            })
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                    Arc::new(Float32Array::from(
                        right
                            .iter()
                            .map(|v| {
                                v.map(|v| {
                                    if v.is_nan() {
                                        f32::from_bits(0xffc0_0022)
                                    } else {
                                        v as f32
                                    }
                                })
                            })
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                )
            } else {
                (
                    Arc::new(Float64Array::from(left.to_vec())) as ArrayRef,
                    Arc::new(Float64Array::from(right.to_vec())) as ArrayRef,
                )
            };
            let (left, right) = if swapped {
                (arrays.1, arrays.0)
            } else {
                arrays
            };
            let output = instance(&program)
                .evaluate(
                    &nullsafe_batch(&program, left, right),
                    Selection::all(8),
                    &Control,
                )
                .unwrap();
            assert_eq!(boolean_values(&output), expected);
        }
    }
}

#[test]
fn null_left_keeps_required_right_random_state_and_round_row_error() {
    let random = nullsafe_program(
        FunctionValueType::new(DataType::Float64, true),
        Right::Random,
    );
    let calls = random
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .calls();
    assert!(calls.values().any(|call| {
        call.effects()
            .for_use(call.effects().context())
            .unwrap()
            .value_stability
            == novarocks_type_contract::FunctionVolatility::Volatile
    }));
    let batch = nullsafe_batch(
        &random,
        Arc::new(Float64Array::from(vec![None; 4])),
        Arc::new(Float64Array::from(vec![None; 4])),
    );
    let mut evaluator = instance(&random);
    let empty = evaluator
        .evaluate(&batch, Selection::try_sparse(4, &[]).unwrap(), &Control)
        .unwrap();
    assert!(empty.values().is_empty());
    assert_eq!(evaluator.instances.len(), 0);
    let rows = [1, 3];
    let selected = Selection::try_sparse(4, &rows).unwrap();
    for _ in 0..2 {
        assert_eq!(
            boolean_values(&evaluator.evaluate(&batch, selected, &Control).unwrap()),
            vec![Some(false); 2]
        );
    }
    assert_eq!(evaluator.instances.len(), 1);
    let decimal = nullsafe_program(
        FunctionValueType::new(DataType::Decimal128(38, 0), true),
        Right::Round,
    );
    let raw = 10_i128.pow(38) - 1;
    let array = |values| {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ) as ArrayRef
    };
    let batch = nullsafe_batch(
        &decimal,
        array(vec![None; 4]),
        array(vec![Some(raw), Some(10), None, Some(raw)]),
    );
    let output = instance(&decimal)
        .evaluate(&batch, selected, &Control)
        .unwrap();
    assert_eq!(errors(&output), vec![1]);
    assert_eq!(boolean_values(&output), vec![Some(false), None]);
    // Error placeholders are terminal evidence, not successful NULL operands.
    assert!(!output.errors()[0].message().is_empty());
}

#[test]
fn nullsafe_controller_preserves_every_primary_refusal_prefix_and_failed_latch() {
    let program = nullsafe_program(FunctionValueType::new(DataType::Int64, true), Right::Column);
    let batch = nullsafe_batch(
        &program,
        Arc::new(Int64Array::from(vec![Some(7); 320])),
        Arc::new(Int64Array::from(vec![Some(7); 320])),
    );
    let constructor = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    CompiledExpressionInstance::try_new(program.clone(), root(), &constructor).unwrap();
    let construction = constructor.trace.lock().unwrap().clone();
    for stop_at in 1..=construction.len() {
        for cause in causes() {
            let refusal = CallbackControl::new(cause.clone(), stop_at);
            assert!(
                matches!(CompiledExpressionInstance::try_new(program.clone(), root(), &refusal), Err(actual) if actual == cause)
            );
            assert_eq!(*refusal.trace.lock().unwrap(), construction[..stop_at]);
        }
    }
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program)
        .evaluate(&batch, Selection::all(320), &recorder)
        .unwrap();
    assert_eq!(boolean_values(&output), vec![Some(true); 320]);
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for stop_at in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let refusal = CallbackControl::new(cause.clone(), stop_at);
            assert!(
                matches!(evaluator.evaluate(&batch, Selection::all(320), &refusal), Err(actual) if actual == cause)
            );
            assert_eq!(*refusal.trace.lock().unwrap(), trace[..stop_at]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(&batch, Selection::all(320), &after),
                Err(KernelFailure::InstanceFailed)
            ));
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}

struct CompileCallbacks {
    cause: CompileControlError,
    stop_at: usize,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refused: Mutex<bool>,
}
impl CompileCallbacks {
    fn new(cause: CompileControlError, stop_at: usize) -> Self {
        Self {
            cause,
            stop_at,
            trace: Mutex::new(vec![]),
            refused: Mutex::new(false),
        }
    }
}
impl PureCompileControl for CompileCallbacks {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut refused = self.refused.lock().unwrap();
        assert!(!*refused, "no callback may follow the original refusal");
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        if trace.len() == self.stop_at {
            *refused = true;
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}
#[test]
fn nullsafe_compilation_keeps_all_original_refusals_including_unsupported_tail() {
    // UInt32 is a valid physical snapshot domain, but not a legacy null-safe leaf.
    for supported in [true, false] {
        let ty = if supported {
            DataType::Int64
        } else {
            DataType::UInt32
        };
        let (functions, package) =
            nullsafe_fixture(FunctionValueType::new(ty, true), Right::Column);
        let recorder = CompileCallbacks::new(CompileControlError::Cancelled, usize::MAX);
        let result = compile(&functions, package.clone(), &recorder);
        assert_eq!(result.is_ok(), supported);
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for stop_at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let refusal = CompileCallbacks::new(cause.clone(), stop_at);
                let failure = compile(&functions, package.clone(), &refusal).unwrap_err();
                assert!(
                    matches!(failure, novarocks_local_compiler::FragmentCompileError::Control(actual) if actual == cause)
                );
                assert_eq!(*refusal.trace.lock().unwrap(), trace[..stop_at]);
            }
        }
    }
}
#[test]
fn nullsafe_source_carrier_and_logical_metadata_are_checked_before_selected_null() {
    let program = nullsafe_program(FunctionValueType::new(DataType::Int64, true), Right::Column);
    let schema = program.graph().nodes()[1].output_layout().schema();
    let foreign = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(schema.field(0).name(), DataType::Float64, true),
            arrow::datatypes::Field::new(schema.field(1).name(), DataType::Float64, true),
        ])),
        vec![
            Arc::new(Float64Array::from(vec![None])),
            Arc::new(Float64Array::from(vec![None])),
        ],
    )
    .unwrap();
    assert!(matches!(
        instance(&program).evaluate(&foreign, Selection::all(1), &Control),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let program = nullsafe_program(FunctionValueType::new(DataType::Utf8, true), Right::Column);
    let fields = program.graph().nodes()[1]
        .output_layout()
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let mut metadata = field.metadata().clone();
            metadata.insert("nr_logical_type".to_string(), "Json".to_string());
            field.as_ref().clone().with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    let foreign = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(fields)),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )
    .unwrap();
    assert!(matches!(
        instance(&program).evaluate(&foreign, Selection::all(1), &Control),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

// Conservative retained-source invoice and independent projection ceilings for
// these small fixtures only; this is not a production default or a MEM grant.
fn package_admission() -> novarocks_physical_plan::FragmentPackageAdmission {
    novarocks_physical_plan::FragmentPackageAdmission {
        plan_limits: novarocks_physical_plan::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: novarocks_physical_plan::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}
