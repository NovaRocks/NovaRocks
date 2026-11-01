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
    array::{BooleanArray, Float64Array, Int64Array, new_empty_array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, ConstantValue, EngineFunctionCatalogBuilder, FunctionArgument,
    FunctionBindingRequest, FunctionId, FunctionKind, FunctionOverloadId, FunctionResultType,
    InstalledPureKernel, KernelEvaluationControl, KernelFailure, PureCallPreparation,
    PureEngineFunctionCatalog, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    ScopedExpressionEffects, SelectedValues, Selection,
};
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{
    KernelAbiVersion, LocalProgram, ProgramExpressionArena, ProgramExpressionRootSite,
    ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind, StaticExprKind,
};
use novarocks_physical_plan::{
    BoundFunction, ExprId, ExprKind, Fragment, FragmentBuilder, FragmentCuts, FragmentId,
    FragmentPackage, FragmentPackageInput, FragmentSink, FrozenFragmentCalls,
    FrozenFragmentPruning, FrozenPhysicalCall, LiteralValue, NodeId, PhysicalCallSite,
    PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain, PlanVersionId, RequiredContracts,
    ResultField, ResultPort, ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, FunctionValueType, PureCompileControl,
    SemanticParameters,
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
pub(crate) fn rng_subset() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let definition = actual
        .definition("rand", FunctionKind::Scalar)
        .unwrap()
        .clone();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition).unwrap();
    // This deliberately seals a real RAND-only subset, not the Server catalogue.
    // These independent records name the actual installed implementation ABI.
    builder
        .seal_pure(
            [
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i64)->f64;strict;legacy",
            ]
            .into_iter()
            .map(|overload| InstalledPureKernel {
                function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: PureImplementationId::try_new(
                        "builtin.scalar/rand/selected-v1",
                    )
                    .unwrap(),
                    abi: PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            }),
        )
        .unwrap()
}

fn options(dop: usize) -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(dop).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        // Explicit fixture admission; these values are not production defaults.
        constants: ConstantPolicy {
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
        },
    }
}

#[derive(Clone, Copy)]
pub(crate) enum SeedMode {
    Input,
    DirectConstant,
}

fn invocation(
    fragment: &Fragment,
    expression: ExprId,
    demand: EvaluationDemand,
    next: &mut u32,
    uses: &mut Vec<ExpressionInvocation<ExprId>>,
) -> ExpressionUseId {
    let id = ExpressionUseId::new(*next);
    *next += 17;
    let definition = fragment.expressions().get(expression).unwrap();
    let children = match &definition.kind {
        ExprKind::FunctionCall { function, args } => {
            assert_eq!(function.function_id.as_str(), "builtin.scalar/rand/v1");
            assert_eq!(args.len(), 1);
            args.iter()
                .map(|child| invocation(fragment, *child, EvaluationDemand::Value, next, uses))
                .collect::<Vec<_>>()
        }
        ExprKind::Literal(_) | ExprKind::Value(_) => vec![],
        other => panic!("fixture has no exact control protocol for {other:?}"),
    };
    uses.push(ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: id,
            domain: EvaluationDomainId::new(u32::MAX),
            demand,
        },
        definition: expression,
        control: ControlShape::Eager,
        arguments: children.into_boxed_slice(),
    });
    id
}

fn package(
    functions: &PureEngineFunctionCatalog,
    mode: SeedMode,
    twin: bool,
) -> Arc<FragmentPackage> {
    package_with_dictionary(functions, mode, twin, false)
}

fn package_with_dictionary(
    functions: &PureEngineFunctionCatalog,
    mode: SeedMode,
    twin: bool,
    dictionary: bool,
) -> Arc<FragmentPackage> {
    let seed_type = FunctionValueType::new(DataType::Int64, matches!(mode, SeedMode::Input));
    let arguments = [FunctionArgument::Value {
        value_type: seed_type.clone(),
        constant: if matches!(mode, SeedMode::DirectConstant) {
            Some(
                ConstantValue::from_i64(
                    Arc::new(seed_type.try_to_field("fixture").unwrap()),
                    seed_type.clone(),
                    42,
                    options(1).constants,
                    CompilePhase::FunctionSpecialization,
                    &FixtureControl,
                )
                .unwrap(),
            )
        } else {
            None
        },
    }];
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 1,
        expected_result_type: None,
    };
    let bound = functions
        .metadata()
        .resolve_bound_user("rand", FunctionKind::Scalar, request, &FixtureControl)
        .unwrap();
    // Retain the actual first-resolution request independently of the later
    // physical Literal/Value shape. Existing CVs loan their original backing;
    // no literal factory or admission policy is replayed for this table.
    let mut constants = novarocks_physical_plan::ConstantPools::empty();
    let original_arguments = request
        .arguments
        .iter()
        .map(|argument| {
            let FunctionArgument::Value {
                value_type,
                constant,
            } = argument
            else {
                panic!("RAND fixture has one actual Value argument");
            };
            let constant = constant.as_ref().map(|value| {
                let pool = novarocks_physical_plan::ConstantPoolId::new(u32::MAX);
                constants.insert(pool, value.pool().clone()).unwrap();
                novarocks_physical_plan::ConstantReference {
                    pool,
                    ordinal: value.ordinal(),
                }
            });
            novarocks_physical_plan::StaticFunctionArgument::Value {
                value_type: value_type.clone(),
                constant,
            }
        })
        .collect::<Box<[_]>>();
    let original_request = novarocks_physical_plan::PhysicalCallRequest {
        arguments: original_arguments,
        logical_argument_count: request.logical_argument_count,
        expected_result_type: request.expected_result_type.cloned(),
        constant_policy: options(1).constants,
    };
    let selected = Arc::new(bound.selected.clone());
    let FunctionResultType::Scalar(result_type) = &selected.result_type else {
        panic!("RAND result")
    };
    let result_type = result_type.clone();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let fragment_id = FragmentId::new(72);
    let values_node = NodeId::new(u32::MAX);
    let first_project = NodeId::new(21);
    let filter = NodeId::new(3);
    let second_project = NodeId::new(8);
    let limit = NodeId::new(1);
    let seed_value = ValueId::new(100);
    let bool_value = ValueId::new(7);
    let sample_value = ValueId::new(200);
    let extra_value = ValueId::new(0);
    let twin_value = ValueId::new(201);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(values_node, Box::from([Box::default()]), Box::default())
        .unwrap();
    let extra_type = FunctionValueType::new(DataType::Int64, false);
    let extra_expr = builder
        .add_expression(
            first_project,
            extra_type.clone(),
            ExprKind::Literal(LiteralValue::Int64(999)),
        )
        .unwrap();
    let seed_expr = builder
        .add_expression(
            first_project,
            seed_type.clone(),
            ExprKind::Literal(LiteralValue::Int64(42)),
        )
        .unwrap();
    let bool_type = FunctionValueType::new(DataType::Boolean, false);
    let bool_expr = builder
        .add_expression(
            first_project,
            bool_type.clone(),
            ExprKind::Literal(LiteralValue::Boolean(true)),
        )
        .unwrap();
    let mut first_definitions = vec![
        (extra_value, extra_type, extra_expr),
        (seed_value, seed_type.clone(), seed_expr),
        (bool_value, bool_type.clone(), bool_expr),
    ];
    if dictionary {
        let ty = FunctionValueType::new(
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        );
        let expr = builder
            .add_expression(
                first_project,
                ty.clone(),
                ExprKind::Literal(LiteralValue::Null),
            )
            .unwrap();
        first_definitions.push((ValueId::new(400), ty, expr));
    }
    let first_outputs = first_definitions
        .iter()
        .map(|(value, _, expr)| (*expr, *value))
        .collect::<Vec<_>>();
    let first_values = first_definitions
        .iter()
        .map(|(value, _, _)| *value)
        .collect::<Vec<_>>();
    for (id, ty, expr) in first_definitions {
        builder
            .insert_value(ValueDef {
                id,
                ty,
                origin: ValueOrigin::Expr {
                    node: first_project,
                    expr,
                },
            })
            .unwrap();
    }
    // The extra actual output shifts the seed channel to ordinal one. ValueId
    // 100 must never become either its ordinal or a local SlotId.
    builder
        .add_project(
            first_project,
            values_node,
            first_outputs.into_boxed_slice(),
            first_values.into_boxed_slice(),
        )
        .unwrap();
    let predicate = builder
        .add_expression(filter, bool_type, ExprKind::Value(bool_value))
        .unwrap();
    builder
        .add_filter(filter, first_project, Box::from([predicate]))
        .unwrap();
    let call_argument = builder
        .add_expression(
            second_project,
            seed_type.clone(),
            match mode {
                SeedMode::Input => ExprKind::Value(seed_value),
                SeedMode::DirectConstant => ExprKind::Literal(LiteralValue::Int64(42)),
            },
        )
        .unwrap();
    let function = BoundFunction {
        function_id: bound.function_id.clone(),
        overload: selected.overload.clone(),
        kind: bound.kind,
        argument_types: selected.argument_types.clone(),
        result_type: result_type.clone(),
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: bound.semantics.volatility,
            argument_evaluation: bound.semantics.argument_evaluation,
            failure_behavior: bound.semantics.failure_behavior,
            intrinsic_row_error: bound.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    let call = builder
        .add_expression(
            second_project,
            result_type.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::from([call_argument]),
            },
        )
        .unwrap();
    builder
        .insert_value(ValueDef {
            id: sample_value,
            ty: result_type.clone(),
            origin: ValueOrigin::Expr {
                node: second_project,
                expr: call,
            },
        })
        .unwrap();
    let identity_a = builder
        .add_expression(
            second_project,
            seed_type.clone(),
            ExprKind::Value(seed_value),
        )
        .unwrap();
    let identity_b = builder
        .add_expression(
            second_project,
            seed_type.clone(),
            ExprKind::Value(seed_value),
        )
        .unwrap();
    let mut project_expressions = vec![(call, sample_value)];
    let mut project_output = vec![sample_value];
    if twin {
        builder
            .insert_value(ValueDef {
                id: twin_value,
                ty: result_type.clone(),
                origin: ValueOrigin::Expr {
                    node: second_project,
                    expr: call,
                },
            })
            .unwrap();
        // One definition, two independent root uses and produced values.
        project_expressions.push((call, twin_value));
        project_output.push(twin_value);
    }
    project_expressions.extend([(identity_a, seed_value), (identity_b, seed_value)]);
    project_output.extend([seed_value, seed_value]);
    builder
        .add_project(
            second_project,
            filter,
            project_expressions.into_boxed_slice(),
            project_output.into_boxed_slice(),
        )
        .unwrap();
    builder
        .add_limit(limit, second_project, Some(1), 0)
        .unwrap();
    let fragment = builder
        .finish_definition(
            limit,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(
            vec![(
                novarocks_physical_plan::PhysicalCallDefinition::Expression(call),
                original_request,
            )],
            &FixtureControl,
        )
        .unwrap();
    let actual_roots = PhysicalExpressionRoots::try_new(&fragment, &FixtureControl).unwrap();
    let mut next = 4096;
    let mut uses = Vec::new();
    let roots = actual_roots
        .sites()
        .iter()
        .map(|(site, root)| {
            (
                *site,
                invocation(&fragment, root.expr, root.demand, &mut next, &mut uses),
            )
        })
        .collect();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(u32::MAX),
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let physical_uses = PhysicalRootUses::try_new(&fragment, flow, roots, &FixtureControl).unwrap();
    let mut call_entries = Vec::new();
    for (call_use, invocation) in physical_uses
        .flow()
        .uses()
        .iter()
        .filter(|(_, invocation)| invocation.definition == call)
    {
        let argument_uses = [Some(invocation.arguments[0])];
        let context = invocation.context;
        let authored = functions
            .prepare_fresh(
                CallEffectInput {
                    context,
                    argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(
                        &argument_uses,
                    ),
                    function_id: &bound.function_id,
                    kind: bound.kind,
                    selected: selected.as_ref(),
                    request,
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    proof_scope: novarocks_type_contract::CallProofScope::Domain(context.domain),
                },
                selected.clone(),
                PureCallPreparation::Scalar {
                    arguments: ScopedExpressionEffects::pure_value(context),
                },
                &FixtureControl,
            )
            .unwrap();
        call_entries.push(FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site: PhysicalCallSite::Expression(*call_use),
            context,
            effects: authored.call_contract().effects().clone(),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        });
    }
    let calls =
        FrozenFragmentCalls::try_new(&fragment, &physical_uses, call_entries, &FixtureControl)
            .unwrap();
    let mut result_fields = vec![ResultField {
        domain: crate::test_result_domain::result_value_domain(&result_type),
        name: "sample".into(),
        alias: None,
        value: sample_value,
        ty: result_type.clone(),
    }];
    if twin {
        result_fields.push(ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_type),
            name: "sample_twin".into(),
            alias: None,
            value: twin_value,
            ty: result_type,
        });
    }
    result_fields.extend([
        ResultField {
            domain: crate::test_result_domain::result_value_domain(&seed_type),
            name: "seed_a".into(),
            alias: None,
            value: seed_value,
            ty: seed_type.clone(),
        },
        ResultField {
            domain: crate::test_result_domain::result_value_domain(&seed_type),
            name: "seed_b".into(),
            alias: None,
            value: seed_value,
            ty: seed_type,
        },
    ]);
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&limit].output.clone(),
        fields: result_fields.into_boxed_slice(),
    };
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([72; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants,
                pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &FixtureControl)
                    .unwrap(),
                fragment,
                expression_uses: physical_uses,
                calls,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
            },
            package_admission(),
            &FixtureControl,
        )
        .unwrap(),
    )
}

pub(crate) fn program(mode: SeedMode, twin: bool) -> Arc<LocalProgram> {
    let functions = rng_subset();
    let source = package(&functions, mode, twin);
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated = validate_fragment_providers(source, &providers, &FixtureControl).unwrap();
    Arc::new(compile_fragment(validated, &functions, options(1), &FixtureControl).unwrap())
}

fn project_site(expression: u32) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(3),
        role: ProgramNodeExpressionRole::ProjectOutput { expression },
    }
}

fn batch(program: &LocalProgram, seeds: Vec<Option<i64>>) -> RecordBatch {
    let rows = seeds.len();
    // Borrow the exact immediate Filter child schema, including the unrelated
    // ordinal-zero output. The seed is ordinal one; ValueId 100 is no index.
    let schema = program.graph().nodes()[2].output_layout().schema().clone();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![999; rows])),
            Arc::new(Int64Array::from(seeds)),
            Arc::new(BooleanArray::from(vec![true; rows])),
        ],
    )
    .unwrap()
}

struct RuntimeControl;
impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("compiled RAND expression must not wait")
    }
}
fn instance(program: &Arc<LocalProgram>, expression: u32) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new(program.clone(), project_site(expression), &RuntimeControl)
        .unwrap()
}
fn bits(result: SelectedValues<'_>) -> Vec<u64> {
    assert!(result.errors().is_empty());
    result
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .values()
        .iter()
        .map(|value| value.to_bits())
        .collect()
}

// Independent PCG32 seed expansion + ChaCha12 / rand 0.8.5 oracles.
pub(crate) const SEED_42_FIRST: u64 = 0x3fe0d98eec6444e4;
const SEED_42_SECOND: u64 = 0x3fe15e014267f5aa;
const SEED_42_THIRD: u64 = 0x3fe45dec0e3bca26;
const SEED_7_FIRST: u64 = 0x3f9f0b83a5aaa3e0;
const SEED_0_FIRST: u64 = 0x3fe76547f659a58d;

#[test]
fn actual_value_seed_root_maps_sparse_parent_rows_and_nullable_seed_zero() {
    let program = program(SeedMode::Input, false);
    let input = batch(&program, vec![Some(42), Some(7), Some(42), None]);
    let mut evaluator = instance(&program, 0);
    let rows = [1, 2, 3];
    let selected = Selection::try_sparse(4, &rows).unwrap();
    let result = evaluator
        .evaluate(&input, selected, &RuntimeControl)
        .unwrap();
    assert_eq!(result.selection(), selected);
    assert_eq!(
        bits(result),
        vec![SEED_7_FIRST, SEED_42_FIRST, SEED_0_FIRST]
    );
    let result = evaluator
        .evaluate(&input, Selection::all(4), &RuntimeControl)
        .unwrap();
    assert_eq!(
        bits(result),
        vec![SEED_42_FIRST, SEED_7_FIRST, SEED_42_FIRST, SEED_0_FIRST]
    );
}

#[test]
fn actual_constant_seed_root_continues_across_batches_without_driver_state_sharing() {
    let program = program(SeedMode::DirectConstant, false);
    let two = batch(&program, vec![Some(42), Some(42)]);
    let one = batch(&program, vec![Some(42)]);
    let mut a = instance(&program, 0);
    let mut b = instance(&program, 0);
    assert_eq!(
        bits(
            a.evaluate(&two, Selection::all(2), &RuntimeControl)
                .unwrap()
        ),
        vec![SEED_42_FIRST, SEED_42_SECOND]
    );
    assert_eq!(
        bits(
            b.evaluate(&one, Selection::all(1), &RuntimeControl)
                .unwrap()
        ),
        vec![SEED_42_FIRST]
    );
    assert_eq!(
        bits(
            a.evaluate(&one, Selection::all(1), &RuntimeControl)
                .unwrap()
        ),
        vec![SEED_42_THIRD]
    );
    assert_eq!(
        bits(
            b.evaluate(&one, Selection::all(1), &RuntimeControl)
                .unwrap()
        ),
        vec![SEED_42_SECOND]
    );
}

#[test]
fn one_definition_with_distinct_actual_root_occurrences_keeps_independent_rng_streams() {
    let program = program(SeedMode::DirectConstant, true);
    let graph = program.graph();
    let ProgramNodeKind::Project { exprs, .. } = graph.nodes()[3].kind() else {
        panic!("actual second Project")
    };
    assert_eq!(exprs[0], exprs[1]);
    assert!(matches!(
        graph.expressions().node(exprs[0]).unwrap().kind(),
        StaticExprKind::BoundCall { .. }
    ));
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let root_a = snapshot.bindings()[&project_site(0)];
    let root_b = snapshot.bindings()[&project_site(1)];
    assert_ne!(root_a, root_b);
    let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
    assert_eq!(
        flow.uses()[&root_a].definition,
        flow.uses()[&root_b].definition
    );
    assert_ne!(
        flow.uses()[&root_a].arguments[0],
        flow.uses()[&root_b].arguments[0]
    );
    let input = batch(&program, vec![Some(42), Some(42)]);
    let mut a = instance(&program, 0);
    let mut b = instance(&program, 1);
    assert_eq!(
        bits(
            a.evaluate(&input, Selection::all(2), &RuntimeControl)
                .unwrap()
        ),
        vec![SEED_42_FIRST, SEED_42_SECOND]
    );
    let row = [1];
    assert_eq!(
        bits(
            b.evaluate(
                &input,
                Selection::try_sparse(2, &row).unwrap(),
                &RuntimeControl
            )
            .unwrap()
        ),
        vec![SEED_42_FIRST]
    );
    assert_eq!(
        bits(
            a.evaluate(
                &input,
                Selection::try_sparse(2, &row).unwrap(),
                &RuntimeControl
            )
            .unwrap()
        ),
        vec![SEED_42_THIRD]
    );
    assert_eq!(
        bits(
            b.evaluate(
                &input,
                Selection::try_sparse(2, &row).unwrap(),
                &RuntimeControl
            )
            .unwrap()
        ),
        vec![SEED_42_SECOND]
    );
}

#[test]
fn empty_selection_does_not_advance_actual_constant_rng_instance() {
    let program = program(SeedMode::DirectConstant, false);
    let input = batch(&program, vec![Some(42), Some(42)]);
    let mut evaluator = instance(&program, 0);
    let none: [usize; 0] = [];
    let empty = Selection::try_sparse(2, &none).unwrap();
    let result = evaluator.evaluate(&input, empty, &RuntimeControl).unwrap();
    assert_eq!(result.selection(), empty);
    assert!(result.selection().is_empty());
    assert!(result.errors().is_empty());
    assert_eq!(
        bits(
            evaluator
                .evaluate(&input, Selection::all(2), &RuntimeControl)
                .unwrap()
        ),
        vec![SEED_42_FIRST, SEED_42_SECOND]
    );
    let empty_batch = batch(&program, vec![]);
    assert!(
        evaluator
            .evaluate(&empty_batch, Selection::all(0), &RuntimeControl)
            .unwrap()
            .selection()
            .is_empty()
    );
    let first = [0];
    assert_eq!(
        bits(
            evaluator
                .evaluate(
                    &input,
                    Selection::try_sparse(2, &first).unwrap(),
                    &RuntimeControl
                )
                .unwrap()
        ),
        vec![SEED_42_THIRD]
    );
}

#[test]
fn value_root_materializes_exact_immediate_child_slot_with_sparse_nulls() {
    let program = program(SeedMode::Input, false);
    let input = batch(&program, vec![Some(11), Some(42), None, Some(7)]);
    let mut evaluator = instance(&program, 1);
    let selected_rows = [1, 2, 3];
    let selected = Selection::try_sparse(4, &selected_rows).unwrap();
    let result = evaluator
        .evaluate(&input, selected, &RuntimeControl)
        .unwrap();
    assert_eq!(result.selection(), selected);
    assert!(result.errors().is_empty());
    assert_eq!(result.values().data_type(), &DataType::Int64);
    let values = result
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // An exact Value root retains source NULL rather than substituting RAND's
    // normalized seed-zero rule. The unrelated ordinal-zero value is 999.
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some(42), None, Some(7)]
    );
}

#[test]
fn actual_root_rejects_wrong_schema_and_selection_domain_as_invalid_program() {
    let program = program(SeedMode::Input, false);
    let input = batch(&program, vec![Some(42), Some(7)]);
    let mut fields = input.schema().fields().to_vec();
    fields.swap(0, 1);
    let wrong_schema =
        RecordBatch::try_new(Arc::new(Schema::new(fields)), input.columns().to_vec()).unwrap();
    let mut evaluator = instance(&program, 0);
    assert!(matches!(
        evaluator.evaluate(&wrong_schema, Selection::all(2), &RuntimeControl),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let mut evaluator = instance(&program, 0);
    assert!(matches!(
        evaluator.evaluate(&input, Selection::all(3), &RuntimeControl),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn constructor_rejects_absent_actual_root_instead_of_using_a_definition_or_default_site() {
    let program = program(SeedMode::Input, false);
    assert!(matches!(
        CompiledExpressionInstance::try_new(program, project_site(u32::MAX), &RuntimeControl),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

struct CallbackControl {
    cause: KernelFailure,
    stop_at: usize,
    trace: Mutex<Vec<u32>>,
    refused: Mutex<bool>,
}
impl CallbackControl {
    fn new(cause: KernelFailure, stop_at: usize) -> Self {
        Self {
            cause,
            stop_at,
            trace: Mutex::new(vec![]),
            refused: Mutex::new(false),
        }
    }
}
impl KernelEvaluationControl for CallbackControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let mut refused = self.refused.lock().unwrap();
        assert!(
            !*refused,
            "primary runtime control failure must not trigger another callback"
        );
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if trace.len() == self.stop_at {
            *refused = true;
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("compiled RAND expression must not wait")
    }
}
fn runtime_causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(novarocks_functions::KernelDiagnostic::new(
            "controller-origin-invalid",
        )),
        KernelFailure::Internal(novarocks_functions::KernelDiagnostic::new(
            "controller-origin-internal",
        )),
        KernelFailure::Operational(novarocks_functions::KernelDiagnostic::new(
            "controller-origin-operational",
        )),
        KernelFailure::InstanceFailed,
    ]
}

#[test]
fn every_constructor_control_callback_preserves_original_failure_without_retry() {
    let program = program(SeedMode::Input, false);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let _instance =
        CompiledExpressionInstance::try_new(program.clone(), project_site(0), &recorder).unwrap();
    let successful = recorder.trace.lock().unwrap().clone();
    assert!(!successful.is_empty());
    for index in 1..=successful.len() {
        for cause in runtime_causes() {
            let control = CallbackControl::new(cause.clone(), index);
            let result =
                CompiledExpressionInstance::try_new(program.clone(), project_site(0), &control);
            assert!(
                matches!(result, Err(actual) if actual == cause),
                "constructor callback {index}"
            );
            assert_eq!(*control.trace.lock().unwrap(), successful[..index]);
        }
    }
}

#[test]
fn every_selected_evaluation_callback_preserves_control_and_poisoned_instance_never_replays() {
    let program = program(SeedMode::Input, false);
    let input = batch(
        &program,
        (0..320)
            .map(|row| Some(if row % 2 == 0 { 42 } else { 7 }))
            .collect(),
    );
    let mut evaluator = instance(&program, 0);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let _result = evaluator
        .evaluate(&input, Selection::all(320), &recorder)
        .unwrap();
    let successful = recorder.trace.lock().unwrap().clone();
    assert!(successful.contains(&256));
    assert!(successful.iter().all(|units| *units <= 256));
    for index in 1..=successful.len() {
        for cause in runtime_causes() {
            let mut evaluator = instance(&program, 0);
            let control = CallbackControl::new(cause.clone(), index);
            let result = evaluator.evaluate(&input, Selection::all(320), &control);
            assert!(
                matches!(result, Err(actual) if actual == cause),
                "evaluation callback {index}"
            );
            assert_eq!(*control.trace.lock().unwrap(), successful[..index]);
            assert!(matches!(
                evaluator.evaluate(&input, Selection::all(320), &RuntimeControl),
                Err(KernelFailure::InstanceFailed)
            ));
        }
    }
}

#[test]
#[allow(deprecated)] // The actual source descriptors are independent IPC facts.
fn unused_incoming_dictionary_descriptors_are_not_value_identity_but_layout_and_nullability_are() {
    let functions = rng_subset();
    let source = package_with_dictionary(&functions, SeedMode::Input, false, true);
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated = validate_fragment_providers(source, &providers, &FixtureControl).unwrap();
    let program =
        Arc::new(compile_fragment(validated, &functions, options(1), &FixtureControl).unwrap());
    let schema = program.graph().nodes()[2].output_layout().schema().clone();
    assert_eq!(schema.fields().len(), 4);
    let dictionary = schema.field(3);
    assert_eq!(dictionary.data_type(), &DataType::Utf8);
    assert!(dictionary.is_nullable());
    let columns = schema
        .fields()
        .iter()
        .map(|field| new_empty_array(field.data_type()))
        .collect::<Vec<_>>();
    let valid = RecordBatch::try_new(schema.clone(), columns.clone()).unwrap();
    let mut evaluator = instance(&program, 0);
    assert!(
        evaluator
            .evaluate(&valid, Selection::all(0), &RuntimeControl)
            .unwrap()
            .selection()
            .is_empty()
    );
    for (changed_id, changed_order) in [(17, false), (88, true)] {
        let mut fields = schema.fields().to_vec();
        fields[3] = Arc::new(
            Field::new_dict(
                dictionary.name(),
                dictionary.data_type().clone(),
                dictionary.is_nullable(),
                changed_id,
                changed_order,
            )
            .with_metadata(dictionary.metadata().clone()),
        );
        let drifted = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        // Planned layouts describe plain values. Actual source IPC attributes
        // do not change that same value layout, name or nullability.
        assert_eq!(schema.as_ref(), drifted.as_ref());
        let actual_input = RecordBatch::try_new(drifted, columns.clone()).unwrap();
        let mut evaluator = instance(&program, 0);
        assert!(
            evaluator
                .evaluate(&actual_input, Selection::all(0), &RuntimeControl)
                .unwrap()
                .selection()
                .is_empty()
        );
    }
    for (data_type, nullable) in [(DataType::Binary, true), (DataType::Utf8, false)] {
        let mut fields = schema.fields().to_vec();
        fields[3] = Arc::new(Field::new(dictionary.name(), data_type.clone(), nullable));
        let mut actual_columns = columns.clone();
        actual_columns[3] = new_empty_array(&data_type);
        let actual_input =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), actual_columns).unwrap();
        let mut evaluator = instance(&program, 0);
        assert!(matches!(
            evaluator.evaluate(&actual_input, Selection::all(0), &RuntimeControl),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}

#[test]
fn oversized_actual_empty_port_domain_is_refused_before_controller_allocation() {
    let program = program(SeedMode::DirectConstant, false);
    let root = ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(1),
        role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    };
    let schema = program.graph().nodes()[0].output_layout().schema().clone();
    assert!(schema.fields().is_empty());
    let input = RecordBatch::try_new_with_options(
        schema,
        vec![],
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(usize::MAX)),
    )
    .unwrap();
    let mut evaluator =
        CompiledExpressionInstance::try_new(program, root, &RuntimeControl).unwrap();
    assert!(matches!(
        evaluator.evaluate(&input, Selection::all(usize::MAX), &RuntimeControl),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert!(evaluator.instances.is_empty());
    assert!(matches!(
        evaluator.evaluate(&input, Selection::all(usize::MAX), &RuntimeControl),
        Err(KernelFailure::InstanceFailed)
    ));
}

#[path = "literal_tests.rs"]
mod literal_tests;

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
