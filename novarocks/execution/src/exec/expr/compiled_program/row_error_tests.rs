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

use super::CompiledExpressionInstance;
use arrow::{
    array::{Array, Decimal128Array, Float64Array, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, ConstantValue, EngineFunctionCatalogBuilder, FunctionArgument,
    FunctionBindingRequest, FunctionBindingSelection, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionResultType, InstalledPureKernel, KernelEvaluationControl, KernelFailure,
    PureCallPreparation, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, ScopedExpressionEffects, Selection,
};
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{
    KernelAbiVersion, LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId,
};
use novarocks_physical_plan::{
    BoundFunction, ExprId, ExprKind, Fragment, FragmentBuilder, FragmentCuts, FragmentId,
    FragmentPackage, FragmentPackageInput, FragmentSink, FrozenFragmentCalls,
    FrozenFragmentPruning, FrozenPhysicalCall, LiteralValue, NodeId, PhysicalCallSite,
    PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain, PlanVersionId, RequiredContracts,
    ResultField, ResultPort, ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, ControlShape, DecimalOverflowPolicy,
    EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId, FunctionValueType,
    PureCompileControl, SemanticParameters,
};
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc, time::Duration};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("ROUND must not wait")
    }
}

fn catalogue() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    for name in ["round", "rand"] {
        builder
            .register(
                actual
                    .definition(name, FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
    }
    // This is an explicitly sealed two-family test subset, not Server coverage.
    let mut manifest = vec![];
    for (name, overloads) in [
        ("round", vec!["builtin.scalar/round/dynamic-v1"]),
        (
            "rand",
            vec![
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i64)->f64;strict;legacy",
            ],
        ),
    ] {
        for overload in overloads {
            manifest.push(InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.scalar/{name}/selected-v1"
                    ))
                    .unwrap(),
                    abi: PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            });
        }
    }
    builder.seal_pure(manifest).unwrap()
}

struct Author {
    function: BoundFunction,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    logical_argument_count: usize,
    constant_policy: ConstantPolicy,
}
impl Author {
    fn request(&self) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.logical_argument_count,
            expected_result_type: None,
        }
    }
    fn result(&self) -> FunctionValueType {
        let FunctionResultType::Scalar(result) = &self.selected.result_type else {
            panic!("scalar owner")
        };
        result.clone()
    }
}
fn author(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
) -> Author {
    let logical_argument_count = arguments.len();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count,
        expected_result_type: None,
    };
    let bound = functions
        .metadata()
        .resolve_bound_user(name, FunctionKind::Scalar, request, &Control)
        .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        panic!("scalar owner")
    };
    let function = BoundFunction {
        function_id: bound.function_id,
        overload: selected.overload.clone(),
        kind: bound.kind,
        argument_types: selected.argument_types.clone(),
        result_type: result.clone(),
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: bound.semantics.volatility,
            argument_evaluation: bound.semantics.argument_evaluation,
            failure_behavior: bound.semantics.failure_behavior,
            intrinsic_row_error: bound.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    Author {
        function,
        selected,
        arguments,
        logical_argument_count,
        constant_policy: constant_policy(),
    }
}
fn constant_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}
// Transfer the first resolver request; physical child shapes are not request sources.
fn original_request_sources(
    fragment: Fragment,
    authors: &BTreeMap<ExprId, Author>,
) -> (Fragment, novarocks_physical_plan::ConstantPools) {
    use novarocks_physical_plan::{
        ConstantPoolId, ConstantPools, ConstantReference, PhysicalCallDefinition,
        PhysicalCallRequest, StaticFunctionArgument,
    };
    let mut pools = ConstantPools::empty();
    let mut backing_ids = BTreeMap::new();
    let mut entries = vec![];
    for (&definition, owner) in authors {
        let original = owner.request();
        let arguments = original
            .arguments
            .iter()
            .map(|argument| match argument {
                FunctionArgument::Value {
                    value_type,
                    constant,
                } => {
                    let constant = constant.as_ref().map(|value| {
                        let identity = value.pool().backing_identity();
                        let pool = *backing_ids.entry(identity).or_insert_with(|| {
                            let id =
                                ConstantPoolId::new(u32::try_from(pools.entries().len()).unwrap());
                            pools.insert(id, value.pool().clone()).unwrap();
                            id
                        });
                        ConstantReference {
                            pool,
                            ordinal: value.ordinal(),
                        }
                    });
                    StaticFunctionArgument::Value {
                        value_type: value_type.clone(),
                        constant,
                    }
                }
                FunctionArgument::Lambda {
                    parameter_types,
                    result_type,
                } => StaticFunctionArgument::Lambda {
                    parameter_types: parameter_types.clone(),
                    result_type: result_type.clone(),
                },
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        entries.push((
            PhysicalCallDefinition::Expression(definition),
            PhysicalCallRequest {
                arguments,
                logical_argument_count: original.logical_argument_count,
                expected_result_type: original.expected_result_type.cloned(),
                constant_policy: owner.constant_policy,
            },
        ));
    }
    (
        fragment
            .with_call_requests_observed(entries, &Control)
            .unwrap(),
        pools,
    )
}
fn integer_constant(ty: &FunctionValueType, value: i64) -> ConstantValue {
    ConstantValue::from_i64(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        constant_policy(),
        CompilePhase::FunctionSpecialization,
        &Control,
    )
    .unwrap()
}
fn integer_argument(ty: FunctionValueType, value: i64) -> FunctionArgument {
    let constant = integer_constant(&ty, value);
    argument(ty, Some(constant))
}
fn argument(ty: FunctionValueType, constant: Option<ConstantValue>) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant,
    }
}
fn call(
    builder: &mut FragmentBuilder,
    owner: NodeId,
    authors: &mut BTreeMap<ExprId, Author>,
    author: Author,
    args: Vec<ExprId>,
) -> ExprId {
    let id = builder
        .add_expression(
            owner,
            author.result(),
            ExprKind::FunctionCall {
                function: author.function.clone(),
                args: args.into_boxed_slice(),
            },
        )
        .unwrap();
    authors.insert(id, author);
    id
}
fn invocation(
    fragment: &Fragment,
    expr: ExprId,
    demand: EvaluationDemand,
    next: &mut u32,
    out: &mut Vec<ExpressionInvocation<ExprId>>,
) -> ExpressionUseId {
    let id = ExpressionUseId::new(*next);
    *next += 13;
    let args = match &fragment.expressions().get(expr).unwrap().kind {
        ExprKind::FunctionCall { args, .. } => args
            .iter()
            .map(|child| invocation(fragment, *child, EvaluationDemand::Value, next, out))
            .collect::<Vec<_>>(),
        ExprKind::Literal(_) | ExprKind::Value(_) => vec![],
        _ => panic!("unsupported fixture shape"),
    };
    out.push(ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: id,
            domain: EvaluationDomainId::new(91),
            demand,
        },
        definition: expr,
        control: ControlShape::Eager,
        arguments: args.into_boxed_slice(),
    });
    id
}

#[derive(Clone, Copy)]
enum Shape {
    Single,
    Nested,
    ObservableSibling,
}

fn program(shape: Shape) -> Arc<LocalProgram> {
    let functions = catalogue();
    let fragment_id = FragmentId::new(93);
    let source = NodeId::new(100);
    let input = NodeId::new(17);
    let output = NodeId::new(2);
    let value = ValueId::new(333);
    let digits_value = ValueId::new(901);
    let result = ValueId::new(77);
    let reference = ValueId::new(78);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let integer = FunctionValueType::new(DataType::Int64, true);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut input_items = vec![];
    for (id, ty) in [(value, decimal.clone()), (digits_value, integer.clone())] {
        let expr = builder
            .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id,
                ty,
                origin: ValueOrigin::Expr { node: input, expr },
            })
            .unwrap();
        input_items.push((expr, id));
    }
    builder
        .add_project(
            input,
            source,
            input_items.into_boxed_slice(),
            Box::from([value, digits_value]),
        )
        .unwrap();
    let mut authors = BTreeMap::new();
    let first = if matches!(shape, Shape::ObservableSibling) {
        let ty = FunctionValueType::new(DataType::Int64, false);
        let seed = builder
            .add_expression(
                output,
                ty.clone(),
                ExprKind::Literal(LiteralValue::Int64(42)),
            )
            .unwrap();
        call(
            &mut builder,
            output,
            &mut authors,
            author(&functions, "rand", vec![integer_argument(ty, 42)]),
            vec![seed],
        )
    } else {
        builder
            .add_expression(output, decimal.clone(), ExprKind::Value(value))
            .unwrap()
    };
    let source_type = if matches!(shape, Shape::ObservableSibling) {
        authors[&first].result()
    } else {
        decimal.clone()
    };
    let digit = builder
        .add_expression(
            output,
            integer.clone(),
            if matches!(shape, Shape::ObservableSibling) {
                ExprKind::Value(digits_value)
            } else {
                ExprKind::Literal(LiteralValue::Int64(-1))
            },
        )
        .unwrap();
    let first_round = call(
        &mut builder,
        output,
        &mut authors,
        author(
            &functions,
            "round",
            vec![
                argument(source_type, None),
                argument(
                    integer.clone(),
                    if matches!(shape, Shape::ObservableSibling) {
                        None
                    } else {
                        Some(integer_constant(&integer, -1))
                    },
                ),
            ],
        ),
        vec![first, digit],
    );
    let root = if matches!(shape, Shape::Nested) {
        let ty = authors[&first_round].result();
        let zero = builder
            .add_expression(
                output,
                integer.clone(),
                ExprKind::Literal(LiteralValue::Int64(0)),
            )
            .unwrap();
        call(
            &mut builder,
            output,
            &mut authors,
            author(
                &functions,
                "round",
                vec![argument(ty, None), integer_argument(integer, 0)],
            ),
            vec![first_round, zero],
        )
    } else {
        first_round
    };
    let mut outputs = vec![(root, result)];
    if matches!(shape, Shape::ObservableSibling) {
        outputs.push((first, reference));
    }
    let mut fields = vec![];
    for &(expr, id) in &outputs {
        let ty = authors[&expr].result();
        builder
            .insert_value(ValueDef {
                id,
                ty: ty.clone(),
                origin: ValueOrigin::Expr { node: output, expr },
            })
            .unwrap();
        fields.push(ResultField {
            domain: crate::test_result_domain::result_value_domain(&ty),
            name: format!("result_{}", fields.len()).into(),
            alias: None,
            value: id,
            ty,
        });
    }
    let output_ids = outputs
        .iter()
        .map(|(_, id)| *id)
        .collect::<Vec<_>>()
        .into_boxed_slice();
    builder
        .add_project(output, input, outputs.into_boxed_slice(), output_ids)
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
        )
        .unwrap();
    let (fragment, constants) = original_request_sources(fragment, &authors);
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).unwrap();
    let mut next = 1000;
    let mut ordered_uses = vec![];
    let bindings = roots
        .sites()
        .iter()
        .map(|(site, root)| {
            (
                *site,
                invocation(
                    &fragment,
                    root.expr,
                    root.demand,
                    &mut next,
                    &mut ordered_uses,
                ),
            )
        })
        .collect();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(91),
            parent: None,
            guard: None,
        }],
        ordered_uses.clone(),
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &Control).unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut effects = BTreeMap::new();
    let mut frozen = vec![];
    // This is actual postorder: parent facts compose its real children, including
    // the MayRaise nested ROUND and the instance-observable seeded RAND.
    for invocation in ordered_uses {
        let context = invocation.context;
        let scoped = if let Some(author) = authors.get(&invocation.definition) {
            let mut children = ScopedExpressionEffects::pure_value(context);
            for child in &invocation.arguments {
                children = children.join_same_domain(effects[child]).unwrap();
            }
            let argument_uses = invocation
                .arguments
                .iter()
                .map(|id| Some(*id))
                .collect::<Vec<_>>();
            let token = functions
                .prepare_fresh(
                    CallEffectInput {
                        context,
                        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(
                            &argument_uses,
                        ),
                        function_id: &author.function.function_id,
                        kind: FunctionKind::Scalar,
                        selected: author.selected.as_ref(),
                        request: author.request(),
                        environment: &[],
                        parameters: &parameters,
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        proof_scope: CallProofScope::Domain(context.domain),
                    },
                    author.selected.clone(),
                    PureCallPreparation::Scalar {
                        arguments: children,
                    },
                    &Control,
                )
                .unwrap();
            frozen.push(FrozenPhysicalCall {
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
            ScopedExpressionEffects::pure_value(context)
        };
        effects.insert(context.use_id, scoped);
    }
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, frozen, &Control).unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&output].output.clone(),
        fields: fields.into_boxed_slice(),
    };
    let package = Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([93; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants,
                pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &Control).unwrap(),
                fragment,
                expression_uses: uses,
                calls,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
            },
            package_admission(),
            &Control,
        )
        .unwrap(),
    );
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let validated = validate_fragment_providers(package, &providers, &Control).unwrap();
    Arc::new(
        compile_fragment(
            validated,
            &functions,
            LocalCompileOptions {
                pipeline_dop: NonZeroUsize::new(1).unwrap(),
                root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
                kernel_abi: KernelAbiVersion::CURRENT,
                exchange_wait: std::time::Duration::from_secs(120),
                constants: constant_policy(),
            },
            &Control,
        )
        .unwrap(),
    )
}
fn instance(program: &Arc<LocalProgram>, expression: u32) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new(
        program.clone(),
        ProgramExpressionRootSite::Node {
            node: ProgramNodeId::new(2),
            role: ProgramNodeExpressionRole::ProjectOutput { expression },
        },
        &Control,
    )
    .unwrap()
}
fn batch(
    program: &LocalProgram,
    values: Vec<Option<i128>>,
    digits: Vec<Option<i64>>,
) -> RecordBatch {
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(Int64Array::from(digits)),
        ],
    )
    .unwrap()
}

#[test]
fn strict_null_and_real_decimal_overflow_remap_to_parent_selected_ordinal() {
    let program = program(Shape::Single);
    let max38 = 10_i128.pow(38) - 1;
    let input = batch(
        &program,
        vec![Some(123), None, Some(max38), Some(25), Some(15)],
        vec![None; 5],
    );
    let rows = [1, 2, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let mut evaluator = instance(&program, 0);
    let result = evaluator.evaluate(&input, selection, &Control).unwrap();
    let values = result
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(result.selection(), selection);
    assert_eq!(values.len(), 3);
    assert!(values.is_null(0));
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 20);
    assert_eq!(result.errors().len(), 1);
    assert_eq!(result.errors()[0].selected_ordinal(), 1);
    assert!(result.errors()[0].message().contains("overflow"));
    // Row errors do not poison a successful evaluator instance.
    let clean = batch(&program, vec![Some(25)], vec![None]);
    let next = evaluator
        .evaluate(&clean, Selection::all(1), &Control)
        .unwrap();
    assert!(next.errors().is_empty());
}

#[test]
fn nested_round_keeps_required_child_error_and_successful_null_distinct() {
    let program = program(Shape::Nested);
    let input = batch(
        &program,
        vec![Some(15), None, Some(10_i128.pow(38) - 1), Some(25)],
        vec![None; 4],
    );
    let rows = [1, 2, 3];
    let result = instance(&program, 0)
        .evaluate(&input, Selection::try_sparse(4, &rows).unwrap(), &Control)
        .unwrap();
    let values = result
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(values.is_null(0));
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 30);
    assert_eq!(result.errors().len(), 1);
    assert_eq!(result.errors()[0].selected_ordinal(), 1);
}

#[test]
fn eager_rng_child_advances_even_when_parent_strict_digits_are_null() {
    let program = program(Shape::ObservableSibling);
    let mut parent = instance(&program, 0);
    let mut reference = instance(&program, 1);
    let null_digits = batch(&program, vec![None; 2], vec![None; 2]);
    let skipped = parent
        .evaluate(&null_digits, Selection::all(2), &Control)
        .unwrap();
    assert_eq!(skipped.values().null_count(), 2);
    assert!(skipped.errors().is_empty());
    let first_two = reference
        .evaluate(&null_digits, Selection::all(2), &Control)
        .unwrap();
    assert_eq!(first_two.values().null_count(), 0);
    let next = batch(&program, vec![None], vec![Some(15)]);
    let actual = parent.evaluate(&next, Selection::all(1), &Control).unwrap();
    let reference = reference
        .evaluate(&next, Selection::all(1), &Control)
        .unwrap();
    let source = reference
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    // Independent formula for this finite, positive, small-magnitude digit case.
    let expected = (source * 1e15).round() / 1e15;
    assert_eq!(
        actual
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        expected.to_bits()
    );
    assert!(actual.errors().is_empty());
}

// The operator helper behind ChangeEventExpand assignments evaluates only the
// selected rows: an overflow on an unselected row raises nothing, and a
// selected row's overflow is a required error reported at its batch row.
#[test]
fn operator_selected_evaluation_skips_unselected_row_errors_and_reports_batch_rows() {
    use crate::exec::operators::compiled_expression::evaluate_selected;
    let program = program(Shape::Single);
    let max38 = 10_i128.pow(38) - 1;
    let input = batch(
        &program,
        vec![Some(15), Some(max38), Some(25)],
        vec![None; 3],
    );
    let site = ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(2),
        role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    };
    let mut evaluator = instance(&program, 0);
    let control =
        crate::exec::operators::compiled_expression::RuntimeKernelControl::new(Arc::default());
    let values = evaluate_selected(&mut evaluator, site, &input, &[0, 2], &control).unwrap();
    let values = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!((values.value(0), values.value(1)), (20, 30));
    let error = evaluate_selected(&mut instance(&program, 0), site, &input, &[1, 2], &control)
        .expect_err("the selected overflow row is required");
    let message = error.to_string();
    assert!(
        message.contains("failed at batch row 1 (selected ordinal 0)"),
        "{message}"
    );
    assert!(message.contains("overflow"), "{message}");
    let unordered = evaluate_selected(&mut instance(&program, 0), site, &input, &[2, 0], &control)
        .expect_err("rows must be strictly increasing");
    assert!(unordered.to_string().contains("not ordered"), "{unordered}");
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
