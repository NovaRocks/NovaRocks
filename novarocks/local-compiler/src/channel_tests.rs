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
use arrow_array::{ArrayRef, Float64Array, Int64Array};
use arrow_schema::DataType;
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, ConstantValue, EngineFunctionCatalogBuilder,
    EvaluatedArgument, FunctionArgument, FunctionBindingRequest, FunctionId, FunctionKind,
    FunctionOverloadId, FunctionResultType, InstalledPureKernel, KernelEvaluationControl,
    KernelFailure, PureCallPreparation, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, ScalarEvaluationInstance, ScopedExpressionEffects,
    SelectedValues, Selection,
};
use novarocks_local_program::{
    KernelAbiVersion, ProgramCallSite, ProgramChannelLayoutRole, ProgramChannelSite,
    ProgramExpressionArena, ProgramLexicalSource, ProgramNodeId, ProgramNodeKind,
    ProgramStateTemplate, ProgramUseRef, StaticExprKind,
};
use novarocks_physical_plan::{
    BoundFunction, ExprId, ExprKind, Fragment, FragmentBuilder, FragmentCuts, FragmentId,
    FragmentPackageInput, FragmentSink, FrozenFragmentCalls, FrozenFragmentPruning,
    FrozenPhysicalCall, LiteralValue, NodeId, PhysicalCallSite, PhysicalExpressionRoots,
    PhysicalRootUses, PipelineDopDomain, PlanVersionId, RequiredContracts, ResultField, ResultPort,
    ValueDef, ValueId, ValueOrigin,
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
fn rng_subset() -> PureEngineFunctionCatalog {
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
enum SeedMode {
    Input,
    DirectConstant,
    LiteralWithoutConstant,
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

fn package(functions: &PureEngineFunctionCatalog, mode: SeedMode) -> Arc<FragmentPackage> {
    package_with_outputs(functions, mode, false, false)
}

fn package_with_outputs(
    functions: &PureEngineFunctionCatalog,
    mode: SeedMode,
    downstream_consumer: bool,
    repeated_rand: bool,
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
    let downstream = NodeId::new(13);
    let limit = NodeId::new(1);
    let seed_value = ValueId::new(100);
    let bool_value = ValueId::new(7);
    let sample_value = ValueId::new(200);
    let extra_value = ValueId::new(0);
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
    for (id, ty, expr) in [
        (extra_value, extra_type, extra_expr),
        (seed_value, seed_type.clone(), seed_expr),
        (bool_value, bool_type.clone(), bool_expr),
    ] {
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
            Box::from([
                (extra_expr, extra_value),
                (seed_expr, seed_value),
                (bool_expr, bool_value),
            ]),
            Box::from([extra_value, seed_value, bool_value]),
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
                SeedMode::DirectConstant | SeedMode::LiteralWithoutConstant => {
                    ExprKind::Literal(LiteralValue::Int64(42))
                }
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
    if repeated_rand {
        // The same definition still has two independently controlled root uses.
        // It cannot prove that two stateful evaluations publish one value.
        project_expressions.push((call, sample_value));
        project_output.push(sample_value);
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
    let limit_input = if downstream_consumer {
        let downstream_expr = builder
            .add_expression(downstream, seed_type.clone(), ExprKind::Value(seed_value))
            .unwrap();
        builder
            .add_project(
                downstream,
                second_project,
                Box::from([(downstream_expr, seed_value)]),
                Box::from([seed_value]),
            )
            .unwrap();
        downstream
    } else {
        second_project
    };
    builder.add_limit(limit, limit_input, Some(1), 0).unwrap();
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
    let mut result_fields = if downstream_consumer {
        vec![ResultField {
            domain: novarocks_physical_plan::ResultValueDomain::Plain,
            name: "seed_downstream".into(),
            alias: None,
            value: seed_value,
            ty: seed_type.clone(),
        }]
    } else {
        vec![ResultField {
            domain: novarocks_physical_plan::ResultValueDomain::Plain,
            name: "sample".into(),
            alias: None,
            value: sample_value,
            ty: result_type.clone(),
        }]
    };
    if !downstream_consumer {
        if repeated_rand {
            result_fields.push(ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: "sample_repeated".into(),
                alias: None,
                value: sample_value,
                ty: result_type,
            });
        }
        result_fields.extend([
            ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: "seed_a".into(),
                alias: None,
                value: seed_value,
                ty: seed_type.clone(),
            },
            ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: "seed_b".into(),
                alias: None,
                value: seed_value,
                ty: seed_type,
            },
        ]);
    }
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

fn compile(
    source: Arc<FragmentPackage>,
    functions: &PureEngineFunctionCatalog,
) -> novarocks_local_program::LocalProgram {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated = validate_fragment_providers(source, &providers, &FixtureControl).unwrap();
    compile_fragment(validated, functions, options(1), &FixtureControl).unwrap()
}

#[test]
fn value_channels_keep_original_source_ordinals_and_each_occurrence_lexical_binding() {
    let functions = rng_subset();
    let source = package(&functions, SeedMode::Input);
    let program = compile(source.clone(), &functions);
    let graph = program.graph();
    assert_eq!(graph.nodes().len(), 5);
    for (index, physical) in [u32::MAX, 21, 3, 8, 1].into_iter().enumerate() {
        assert_eq!(graph.nodes()[index].physical_sources()[0].get(), physical);
    }
    let ProgramNodeKind::Values { values } = graph.nodes()[0].kind() else {
        panic!("Values")
    };
    assert_eq!(values.batch().unwrap().num_rows(), 1);
    let checked = program.checked();
    let channels = checked.channels();
    let resolved = channels.expressions().resolved_calls();
    assert_eq!(resolved.calls().len(), 1);
    let local_flow = &resolved.snapshot().flows()[&ProgramExpressionArena::Main];
    let mut seen = 0;
    for (physical_use, invocation) in source.expression_uses().flow().uses() {
        let physical_expr = source
            .fragment()
            .expressions()
            .get(invocation.definition)
            .unwrap();
        let ExprKind::Value(value) = physical_expr.kind else {
            continue;
        };
        let expected = if physical_expr.owner == NodeId::new(3) {
            assert_eq!(value, ValueId::new(7));
            ProgramChannelSite::Layout {
                node: ProgramNodeId::new(1),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: 2,
            }
        } else {
            assert_eq!(value, ValueId::new(100));
            ProgramChannelSite::Layout {
                node: ProgramNodeId::new(2),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: 1,
            }
        };
        let occurrence = ProgramUseRef {
            arena: ProgramExpressionArena::Main,
            use_id: *physical_use,
        };
        assert_eq!(
            checked.slots().get(&occurrence),
            Some(&ProgramLexicalSource::Input(expected))
        );
        let local_definition = local_flow.uses()[physical_use].definition;
        let StaticExprKind::SlotId(slot) =
            graph.expressions().node(local_definition).unwrap().kind()
        else {
            panic!("Value lowers to exact input slot")
        };
        assert_eq!(channels.channel_slot(expected), Some(*slot));
        assert_ne!(slot.as_u32(), value.get());
        assert_ne!(physical_use.get(), invocation.definition.get());
        assert_eq!(local_flow.uses()[physical_use].context, invocation.context);
        assert_eq!(channels.channel_type(expected), Some(&physical_expr.ty));
        seen += 1;
    }
    assert_eq!(seen, 4);
    assert_eq!(checked.slots().len(), 4);
}

#[test]
fn transparent_repeated_value_outputs_preserve_all_three_positions_and_distinct_slots() {
    let functions = rng_subset();
    let source = package(&functions, SeedMode::Input);
    assert_eq!(
        source.result().unwrap().output.columns.as_ref(),
        &[ValueId::new(200), ValueId::new(100), ValueId::new(100)]
    );
    let program = compile(source, &functions);
    let graph = program.graph();
    let layout = graph.nodes()[3].output_layout();
    assert_eq!(layout.slots().len(), 3);
    assert_ne!(layout.slots()[1], layout.slots()[2]);
    assert_eq!(layout.schema().field(0).name(), "sample");
    assert_eq!(layout.schema().field(1).name(), "seed_a");
    assert_eq!(layout.schema().field(2).name(), "seed_b");
    assert_eq!(graph.nodes()[4].output_layout().slots(), layout.slots());
    let ProgramNodeKind::Project {
        exprs,
        expr_slot_ids,
        ..
    } = graph.nodes()[3].kind()
    else {
        panic!("Project")
    };
    assert_eq!(exprs.len(), 3);
    assert_ne!(exprs[1], exprs[2]);
    assert_eq!(expr_slot_ids.as_slice(), layout.slots());
    let channels = program.checked().channels();
    for node in [ProgramNodeId::new(3), ProgramNodeId::new(4)] {
        for ordinal in [1, 2] {
            assert_eq!(
                channels.channel_type(ProgramChannelSite::Layout {
                    node,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal
                }),
                Some(&FunctionValueType::new(DataType::Int64, true))
            );
        }
    }
}

#[test]
fn downstream_value_uses_first_proven_repeated_child_output_without_collapsing_positions() {
    let functions = rng_subset();
    let source = package_with_outputs(&functions, SeedMode::Input, true, false);
    let upstream = &source.fragment().nodes()[&NodeId::new(8)];
    assert_eq!(
        upstream.output.columns.as_ref(),
        &[ValueId::new(200), ValueId::new(100), ValueId::new(100)]
    );
    assert_eq!(
        source.result().unwrap().output.columns.as_ref(),
        &[ValueId::new(100)]
    );
    let program = compile(source.clone(), &functions);
    let graph = program.graph();
    assert_eq!(graph.nodes().len(), 6);
    for (index, physical) in [u32::MAX, 21, 3, 8, 13, 1].into_iter().enumerate() {
        assert_eq!(graph.nodes()[index].physical_sources()[0].get(), physical);
    }
    let repeated_layout = graph.nodes()[3].output_layout();
    assert_eq!(repeated_layout.slots().len(), 3);
    for left in 0..3 {
        for right in left + 1..3 {
            assert_ne!(
                repeated_layout.slots()[left],
                repeated_layout.slots()[right]
            );
        }
    }
    let ProgramNodeKind::Project {
        exprs,
        expr_slot_ids,
        ..
    } = graph.nodes()[3].kind()
    else {
        panic!("upstream Project")
    };
    assert_eq!(exprs.len(), 3);
    assert_ne!(exprs[1], exprs[2]);
    assert_eq!(expr_slot_ids.as_slice(), repeated_layout.slots());

    let (physical_use, invocation) = source
        .expression_uses()
        .flow()
        .uses()
        .iter()
        .find(|(_, invocation)| {
            source
                .fragment()
                .expressions()
                .get(invocation.definition)
                .unwrap()
                .owner
                == NodeId::new(13)
        })
        .unwrap();
    let definition = source
        .fragment()
        .expressions()
        .get(invocation.definition)
        .unwrap();
    assert!(matches!(definition.kind, ExprKind::Value(value) if value == ValueId::new(100)));
    let expected_source = ProgramChannelSite::Layout {
        node: ProgramNodeId::new(3),
        role: ProgramChannelLayoutRole::NodeOutput,
        ordinal: 1,
    };
    let occurrence = ProgramUseRef {
        arena: ProgramExpressionArena::Main,
        use_id: *physical_use,
    };
    let checked = program.checked();
    let channels = checked.channels();
    assert_eq!(
        checked.slots().get(&occurrence),
        Some(&ProgramLexicalSource::Input(expected_source))
    );
    assert_eq!(channels.channel_type(expected_source), Some(&definition.ty));
    let local_flow =
        &channels.expressions().resolved_calls().snapshot().flows()[&ProgramExpressionArena::Main];
    let local_definition = local_flow.uses()[physical_use].definition;
    let StaticExprKind::SlotId(input_slot) =
        graph.expressions().node(local_definition).unwrap().kind()
    else {
        panic!("downstream Value is an exact child slot")
    };
    assert_eq!(*input_slot, repeated_layout.slots()[1]);
    assert_ne!(*input_slot, repeated_layout.slots()[2]);
    assert_ne!(input_slot.as_u32(), ValueId::new(100).get());
    assert_eq!(channels.channel_slot(expected_source), Some(*input_slot));
    assert_eq!(local_flow.uses()[physical_use].context, invocation.context);
    for ordinal in [1, 2] {
        assert_eq!(
            channels.channel_type(ProgramChannelSite::Layout {
                node: ProgramNodeId::new(3),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal,
            }),
            Some(&definition.ty)
        );
    }
    let ProgramNodeKind::Project {
        exprs,
        expr_slot_ids,
        ..
    } = graph.nodes()[4].kind()
    else {
        panic!("downstream Project")
    };
    assert_eq!(exprs.as_slice(), &[local_definition]);
    assert_eq!(
        expr_slot_ids.as_slice(),
        graph.nodes()[4].output_layout().slots()
    );
    assert_eq!(expr_slot_ids.len(), 1);
    assert_ne!(expr_slot_ids[0], *input_slot);
    assert_eq!(
        graph.nodes()[5].output_layout().slots(),
        expr_slot_ids.as_slice()
    );

    // Complete frozen facts for both independently controlled RAND roots do
    // not make their repeated publication a transparent value read.
    let repeated_rand = package_with_outputs(&functions, SeedMode::Input, false, true);
    assert_eq!(repeated_rand.calls().entries().len(), 2);
    assert_eq!(
        repeated_rand.fragment().nodes()[&NodeId::new(8)]
            .output
            .columns
            .as_ref(),
        &[
            ValueId::new(200),
            ValueId::new(200),
            ValueId::new(100),
            ValueId::new(100)
        ]
    );
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated =
        validate_fragment_providers(repeated_rand, &providers, &FixtureControl).unwrap();
    assert!(matches!(
        compile_fragment(validated, &functions, options(1), &FixtureControl),
        Err(FragmentCompileError::Invalid(
            "independent project roots share a produced value"
        ))
    ));
}

struct CallbackControl {
    cause: CompileControlError,
    stop_at: usize,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refused: Mutex<bool>,
}
impl CallbackControl {
    fn new(cause: CompileControlError, stop_at: usize) -> Self {
        Self {
            cause,
            stop_at,
            trace: Mutex::new(Vec::new()),
            refused: Mutex::new(false),
        }
    }
}
impl PureCompileControl for CallbackControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut refused = self.refused.lock().unwrap();
        assert!(
            !*refused,
            "primary control failure must not trigger another callback"
        );
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
fn every_actual_value_channel_callback_and_ordinary_tail_preserves_primary_control_prefix() {
    let functions = rng_subset();
    let source = package_with_outputs(&functions, SeedMode::Input, true, false);
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validate =
        || validate_fragment_providers(source.clone(), &providers, &FixtureControl).unwrap();
    let recorder = CallbackControl::new(CompileControlError::Cancelled, usize::MAX);
    let program = compile_fragment(validate(), &functions, options(1), &recorder).unwrap();
    assert_eq!(program.checked().slots().len(), 5);
    let successful = recorder.trace.lock().unwrap().clone();
    assert!(successful.iter().any(|(_, units)| *units > 0));
    assert!(successful.iter().all(|(_, units)| *units <= 256));
    for index in 1..=successful.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = CallbackControl::new(cause.clone(), index);
            let result = compile_fragment(validate(), &functions, options(1), &control);
            assert!(
                matches!(result, Err(FragmentCompileError::Control(actual)) if actual == cause),
                "Value-path callback {index} must preserve {cause:?}"
            );
            assert_eq!(
                *control.trace.lock().unwrap(),
                successful[..index],
                "Value-path callback {index} must end at the original refusal"
            );
        }
    }

    // Exercise the ordinary channel refusal separately: its actual final
    // observation must still expose control, without retrying a primary cause.
    let source = package_with_outputs(&functions, SeedMode::Input, false, true);
    let validate =
        || validate_fragment_providers(source.clone(), &providers, &FixtureControl).unwrap();
    let recorder = CallbackControl::new(CompileControlError::Cancelled, usize::MAX);
    assert!(matches!(
        compile_fragment(validate(), &functions, options(1), &recorder),
        Err(FragmentCompileError::Invalid(
            "independent project roots share a produced value"
        ))
    ));
    let ordinary = recorder.trace.lock().unwrap().clone();
    // The outer mandatory finish may observe zero after the nested owner
    // already flushed its completed work. It is still a real control boundary.
    assert!(!ordinary.is_empty());
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = CallbackControl::new(cause.clone(), ordinary.len());
        assert!(matches!(
            compile_fragment(validate(), &functions, options(1), &control),
            Err(FragmentCompileError::Control(actual)) if actual == cause
        ));
        assert_eq!(*control.trace.lock().unwrap(), ordinary);
    }
}

struct EvaluationControl;
impl KernelEvaluationControl for EvaluationControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("RAND must not wait")
    }
}
fn instance(program: &novarocks_local_program::LocalProgram) -> ScalarEvaluationInstance {
    let calls = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .calls();
    let (site, call) = calls.iter().next().unwrap();
    let ProgramCallSite::Expression(occurrence) = site else {
        panic!("scalar occurrence")
    };
    let ProgramStateTemplate::Scalar { scope, kernel } = program.state_template(*site).unwrap()
    else {
        panic!("scalar state")
    };
    assert_eq!(scope.occurrence, *occurrence);
    assert!(std::ptr::eq(kernel.contract().call(), call.call_contract()));
    ScalarEvaluationInstance::instantiate(kernel.clone()).unwrap()
}
fn bits(output: SelectedValues<'_>) -> Vec<u64> {
    assert!(output.errors().is_empty());
    output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .values()
        .iter()
        .map(|value| value.to_bits())
        .collect()
}

#[test]
fn compiled_value_seed_rand_uses_actual_sparse_rows_and_is_invariant_across_batches() {
    let functions = rng_subset();
    let program = compile(package(&functions, SeedMode::Input), &functions);
    let mut instance = instance(&program);
    let seed: ArrayRef = Arc::new(Int64Array::from(vec![Some(42), Some(7), Some(42), None]));
    let args = [EvaluatedArgument::Column(&seed)];
    // Independent PCG32 seed expansion + ChaCha12 reference, matching locked
    // rand 0.8.5 seed 0/42 published oracle bits; seed 7 is independently derived.
    const S42: u64 = 0x3fe0d98eec6444e4;
    const S7: u64 = 0x3f9f0b83a5aaa3e0;
    const S0: u64 = 0x3fe76547f659a58d;
    assert_eq!(
        bits(
            instance
                .evaluate(Selection::all(4), &args, &EvaluationControl)
                .unwrap()
        ),
        vec![S42, S7, S42, S0]
    );
    let rows = [1, 2, 3];
    let sparse = Selection::try_sparse(4, &rows).unwrap();
    assert_eq!(
        bits(
            instance
                .evaluate(sparse, &args, &EvaluationControl)
                .unwrap()
        ),
        vec![S7, S42, S0]
    );
    let a: ArrayRef = Arc::new(Int64Array::from(vec![Some(42), Some(7)]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![Some(42), None]));
    let first = bits(
        instance
            .evaluate(
                Selection::all(2),
                &[EvaluatedArgument::Column(&a)],
                &EvaluationControl,
            )
            .unwrap(),
    );
    let second = bits(
        instance
            .evaluate(
                Selection::all(2),
                &[EvaluatedArgument::Column(&b)],
                &EvaluationControl,
            )
            .unwrap(),
    );
    assert_eq!([first, second].concat(), vec![S42, S7, S42, S0]);
}

#[test]
fn original_nonconstant_seed_is_not_promoted_by_its_physical_literal() {
    let functions = rng_subset();
    let source = package(&functions, SeedMode::LiteralWithoutConstant);
    let (&definition, request) = source
        .fragment()
        .call_requests()
        .entries()
        .iter()
        .next()
        .unwrap();
    let novarocks_physical_plan::PhysicalCallDefinition::Expression(id) = definition else {
        panic!("actual RAND definition");
    };
    assert!(request.expected_result_type.is_none());
    assert_eq!(request.logical_argument_count, 1);
    assert!(matches!(
        request.arguments[0],
        novarocks_physical_plan::StaticFunctionArgument::Value { constant: None, .. }
    ));
    assert!(source.constants().entries().is_empty());
    let ExprKind::FunctionCall { args, .. } =
        &source.fragment().expressions().get(id).unwrap().kind
    else {
        panic!("actual RAND call");
    };
    assert!(matches!(
        source.fragment().expressions().get(args[0]).unwrap().kind,
        ExprKind::Literal(LiteralValue::Int64(42))
    ));
    let program = compile(source, &functions);
    let mut instance = instance(&program);
    let seed: ArrayRef = Arc::new(Int64Array::from(vec![42]));
    let arguments = [EvaluatedArgument::Scalar(&seed)];
    // Original None selects the original per-row recipe: equal row seeds each
    // produce the first draw, rather than a constant recipe's advancing state.
    for _ in 0..2 {
        assert_eq!(
            bits(
                instance
                    .evaluate(Selection::all(2), &arguments, &EvaluationControl)
                    .unwrap()
            ),
            vec![0x3fe0d98eec6444e4, 0x3fe0d98eec6444e4]
        );
    }
}

#[test]
fn compiled_direct_constant_seed_retains_exact_value_and_continuous_independent_state() {
    let functions = rng_subset();
    let program = compile(package(&functions, SeedMode::DirectConstant), &functions);
    let mut a = instance(&program);
    let mut b = instance(&program);
    let seed: ArrayRef = Arc::new(Int64Array::from(vec![42]));
    let arguments = [EvaluatedArgument::Scalar(&seed)];
    assert_eq!(
        bits(
            a.evaluate(Selection::all(2), &arguments, &EvaluationControl)
                .unwrap()
        ),
        vec![0x3fe0d98eec6444e4, 0x3fe15e014267f5aa]
    );
    assert_eq!(
        bits(
            b.evaluate(Selection::all(1), &arguments, &EvaluationControl)
                .unwrap()
        ),
        vec![0x3fe0d98eec6444e4]
    );
    assert_eq!(
        bits(
            a.evaluate(Selection::all(1), &arguments, &EvaluationControl)
                .unwrap()
        ),
        vec![0x3fe45dec0e3bca26]
    );
    assert_eq!(
        bits(
            b.evaluate(Selection::all(1), &arguments, &EvaluationControl)
                .unwrap()
        ),
        vec![0x3fe15e014267f5aa]
    );
}

#[test]
fn compiled_seeded_kernel_empty_selection_keeps_zero_rows_without_fallback() {
    let functions = rng_subset();
    let program = compile(package(&functions, SeedMode::Input), &functions);
    let mut instance = instance(&program);
    let empty: ArrayRef = Arc::new(Int64Array::from(Vec::<Option<i64>>::new()));
    let arguments = [EvaluatedArgument::Column(&empty)];
    let output = instance
        .evaluate(Selection::all(0), &arguments, &EvaluationControl)
        .unwrap();
    assert!(output.selection().is_empty());
    assert!(output.errors().is_empty());
    assert!(output.values().is_empty());
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
