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

use crate::test_units::PureUnitFixture;
use novarocks_expression::StaticExprKind;
use novarocks_expression_contract::{BoundFunction, ExprKind, ExpressionCallClaim, RootScope};

use super::*;
use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::DataType;
use novarocks_functions::{
    CallEffectInput, ConstantValidation, ConstantValue, EngineFunctionCatalogBuilder,
    EvaluatedArgument, FunctionArgument, FunctionBindingRequest, FunctionId, FunctionKind,
    FunctionOverloadId, FunctionResultType, InstalledPureKernel, KernelEvaluationControl,
    KernelFailure, PureCallPreparation, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, ScalarEvaluationInstance, ScopedExpressionEffects,
    SelectedValues, Selection,
};
use novarocks_local_program::{
    KernelAbiVersion, ProgramChannelLayoutRole, ProgramChannelSite, ProgramNodeId, ProgramNodeKind,
};
use novarocks_physical_plan::{
    FragmentBuilder, FragmentCuts, FragmentId, FragmentPackageInput, FragmentSink,
    FrozenFragmentPruning, NodeId, PipelineDopDomain, PlanVersionId, RequiredContracts,
    ResultField, ResultPort, ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand, FunctionValueType,
    PureCompileControl, SemanticParameters,
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
                "builtin.scalar/rand/(i8)->i8;strict;coerce",
                "builtin.scalar/rand/(i16)->i16;strict;coerce",
                "builtin.scalar/rand/(i32)->i32;strict;coerce",
                "builtin.scalar/rand/(i64)->i64;strict;coerce",
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
        constants: novarocks_functions::ConstantValidation::bounded(
            novarocks_functions::ConstantPolicy {
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
        ),
    }
}

#[derive(Clone, Copy)]
enum BoundMode {
    Input,
    DirectConstant,
    LiteralWithoutConstant,
}

fn package(functions: &PureEngineFunctionCatalog, mode: BoundMode) -> Arc<FragmentPackage> {
    package_with_outputs(functions, mode, false, false)
}

fn package_with_outputs(
    functions: &PureEngineFunctionCatalog,
    mode: BoundMode,
    downstream_consumer: bool,
    repeated_rand: bool,
) -> Arc<FragmentPackage> {
    let bound_type = FunctionValueType::new(DataType::Int64, matches!(mode, BoundMode::Input));
    let arguments = [FunctionArgument::Value {
        value_type: bound_type.clone(),
        constant: if matches!(mode, BoundMode::DirectConstant) {
            Some(
                ConstantValue::from_i64(
                    Arc::new(bound_type.try_to_field("fixture").unwrap()),
                    bound_type.clone(),
                    1,
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
    let original_request = novarocks_expression_contract::PhysicalCallRequest {
        arguments: original_arguments,
        logical_argument_count: request.logical_argument_count,
        expected_result_type: request.expected_result_type.cloned(),
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
    let bound_value = ValueId::new(100);
    let bool_value = ValueId::new(7);
    let sample_value = ValueId::new(200);
    let extra_value = ValueId::new(0);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(values_node, Box::from([Box::default()]), Box::default())
        .unwrap();
    let extra_type = FunctionValueType::new(DataType::Int64, false);
    let extra_expr = builder
        .fixture_value_root(
            first_project,
            extra_type.clone(),
            crate::test_constants::i64(&mut constants, &extra_type, 999, options(1).constants),
        )
        .unwrap();
    let bound_expr = builder
        .fixture_value_root(
            first_project,
            bound_type.clone(),
            crate::test_constants::i64(&mut constants, &bound_type, 1, options(1).constants),
        )
        .unwrap();
    let bool_type = FunctionValueType::new(DataType::Boolean, false);
    let bool_expr = builder
        .fixture_value_root(
            first_project,
            bool_type.clone(),
            crate::test_constants::boolean(&mut constants, &bool_type, true, options(1).constants),
        )
        .unwrap();
    for (id, ty, expr) in [
        (extra_value, extra_type, extra_expr),
        (bound_value, bound_type.clone(), bound_expr),
        (bool_value, bool_type.clone(), bool_expr),
    ] {
        builder
            .insert_value(ValueDef {
                id,
                ty,
                origin: ValueOrigin::Expr {
                    node: first_project,
                    root: expr,
                },
            })
            .unwrap();
    }
    // The extra actual output shifts the bound channel to ordinal one. ValueId
    // 100 must never become either its ordinal or a local SlotId.
    builder
        .add_project(
            first_project,
            values_node,
            Box::from([
                (extra_expr, extra_value),
                (bound_expr, bound_value),
                (bool_expr, bool_value),
            ]),
            Box::from([extra_value, bound_value, bool_value]),
        )
        .unwrap();
    let predicate_definition = builder
        .fixture_definition(filter, bool_type, ExprKind::Value(bool_value))
        .unwrap();
    let predicate = builder
        .fixture_occurrence(
            filter,
            predicate_definition,
            EvaluationDemand::TruthOnly,
            RootScope::Port(0),
        )
        .unwrap();
    builder
        .add_filter(filter, first_project, Box::from([predicate]))
        .unwrap();
    let call_argument = builder
        .fixture_definition(
            second_project,
            bound_type.clone(),
            match mode {
                BoundMode::Input => ExprKind::Value(bound_value),
                BoundMode::DirectConstant | BoundMode::LiteralWithoutConstant => {
                    crate::test_constants::i64(&mut constants, &bound_type, 1, options(1).constants)
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
        legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
            volatility: bound.semantics.volatility,
            argument_evaluation: bound.semantics.argument_evaluation,
            failure_behavior: bound.semantics.failure_behavior,
            intrinsic_row_error: bound.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    let call_definition = builder
        .fixture_definition(
            second_project,
            result_type.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::from([call_argument]),
            },
        )
        .unwrap();
    let declaration = functions
        .metadata()
        .definition("rand", FunctionKind::Scalar)
        .unwrap();
    let effects = declaration
        .binding_declaration()
        .unwrap()
        .effect_declaration(&selected.overload)
        .unwrap();
    assert_eq!(
        effects.argument_control,
        novarocks_type_contract::ArgumentControl::Eager
    );
    builder
        .unit(second_project)
        .unwrap()
        .definition_control(
            call_definition,
            novarocks_expression_contract::DefinitionControl {
                shape: novarocks_type_contract::ControlShape::Eager,
                temporal_sources: None,
            },
        )
        .unwrap();
    builder
        .unit(second_project)
        .unwrap()
        .request(call_definition, original_request)
        .unwrap();
    let call = builder
        .fixture_occurrence(
            second_project,
            call_definition,
            EvaluationDemand::Value,
            RootScope::Port(0),
        )
        .unwrap();
    builder
        .insert_value(ValueDef {
            id: sample_value,
            ty: result_type.clone(),
            origin: ValueOrigin::Expr {
                node: second_project,
                root: call,
            },
        })
        .unwrap();
    let identity_a = builder
        .fixture_value_root(
            second_project,
            bound_type.clone(),
            ExprKind::Value(bound_value),
        )
        .unwrap();
    let identity_b = builder
        .fixture_value_root(
            second_project,
            bound_type.clone(),
            ExprKind::Value(bound_value),
        )
        .unwrap();
    let mut project_expressions = vec![(call, sample_value)];
    let mut project_output = vec![sample_value];
    if repeated_rand {
        // The same definition still has two independently controlled root uses.
        // It cannot prove that two stateful evaluations publish one value.
        let repeated = builder.fixture_repeated_root(second_project, call).unwrap();
        project_expressions.push((repeated, sample_value));
        project_output.push(sample_value);
    }
    project_expressions.extend([(identity_a, bound_value), (identity_b, bound_value)]);
    project_output.extend([bound_value, bound_value]);
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
            .fixture_value_root(downstream, bound_type.clone(), ExprKind::Value(bound_value))
            .unwrap();
        builder
            .add_project(
                downstream,
                second_project,
                Box::from([(downstream_expr, bound_value)]),
                Box::from([bound_value]),
            )
            .unwrap();
        downstream
    } else {
        second_project
    };
    builder.add_limit(limit, limit_input, Some(1), 0).unwrap();
    let nodes = builder
        .construction_nodes()
        .filter_map(|(&id, _)| builder.construction_unit(id).is_some().then_some(id))
        .collect::<Vec<_>>();
    for node in nodes {
        let ports = builder.node_ports(node).unwrap();
        let draft = builder.take_unit_builder(node).unwrap();
        let mut work =
            CompileCheckpoints::try_new(&FixtureControl, CompilePhase::Validate).unwrap();
        let sealed = draft.seal(ports, &mut work).unwrap();
        let mut claims = BTreeMap::new();
        if node == second_project {
            for (&call_use, invocation) in sealed
                .uses()
                .iter()
                .filter(|(_, invocation)| invocation.definition == call_definition)
            {
                let argument_uses = [Some(invocation.arguments[0])];
                let context = invocation.context;
                let authored = functions
                    .prepare_fresh(
                        CallEffectInput {
                            context: context.into(),
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
                            proof_scope: novarocks_type_contract::CallProofScope::Region(
                                context.region,
                            ),
                        },
                        selected.clone(),
                        PureCallPreparation::Scalar {
                            arguments: ScopedExpressionEffects::pure_value(context),
                        },
                        &FixtureControl,
                    )
                    .unwrap();
                claims.insert(
                    call_use,
                    ExpressionCallClaim {
                        context,
                        effects: authored.call_contract().effects().clone(),
                        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                        temporal_source: None,
                    },
                );
            }
        }
        let unit = sealed.finish(claims, &mut work).unwrap();
        work.finish().unwrap();
        builder.install_unit(node, Arc::new(unit)).unwrap();
    }
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
        .unwrap();
    let mut result_fields = if downstream_consumer {
        vec![ResultField {
            domain: novarocks_physical_plan::ResultValueDomain::Plain,
            name: "bound_downstream".into(),
            alias: None,
            value: bound_value,
            ty: bound_type.clone(),
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
                name: "bound_a".into(),
                alias: None,
                value: bound_value,
                ty: bound_type.clone(),
            },
            ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: "bound_b".into(),
                alias: None,
                value: bound_value,
                ty: bound_type,
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

                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                layouts: Default::default(),
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

fn compiled_unit_for(
    program: &novarocks_local_program::LocalProgram,
    physical: NodeId,
) -> &novarocks_expression::CompiledExprSet {
    let node = program
        .graph()
        .nodes()
        .iter()
        .find(|node| {
            node.physical_sources()
                .iter()
                .any(|source| source.get() == physical.get())
        })
        .expect("actual physical node");
    let reference = match node.kind() {
        ProgramNodeKind::Project { exprs, .. } => exprs[0],
        ProgramNodeKind::Filter { predicates, .. } => predicates[0],
        _ => panic!("this fixture node owns actual roots"),
    };
    program.unit(reference.unit).unwrap()
}
fn input_count(program: &novarocks_local_program::LocalProgram) -> usize {
    program
        .units()
        .iter()
        .map(|unit| {
            unit.control_flow()
                .uses()
                .keys()
                .filter(|id| unit.input(**id).is_some())
                .count()
        })
        .sum()
}

#[test]
fn value_channels_keep_original_source_ordinals_and_each_occurrence_lexical_binding() {
    let functions = rng_subset();
    let source = package(&functions, BoundMode::Input);
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
    let channels = program.channels();
    assert_eq!(
        program
            .units()
            .iter()
            .map(|unit| unit.calls().len())
            .sum::<usize>(),
        1
    );
    let mut seen = 0;
    for physical in source.fragment().nodes().values() {
        let Some(bound) = &physical.expressions else {
            continue;
        };
        let unit = compiled_unit_for(&program, physical.id);
        for (&physical_use, invocation) in bound.control_flow().uses() {
            let physical_expr = bound.definition(invocation.definition).unwrap();
            let ExprKind::Value(value) = physical_expr.kind else {
                continue;
            };
            let expected = if physical.id == NodeId::new(3) {
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
            let local = &unit.control_flow().uses()[&physical_use];
            let local_definition = unit.compiled_definition(invocation.definition).unwrap();
            assert_eq!(local.definition, local_definition);
            assert!(matches!(
                unit.definitions().node(local_definition).unwrap().kind(),
                StaticExprKind::Input
            ));
            let input = unit.input(physical_use).unwrap();
            let ProgramChannelSite::Layout {
                node,
                ordinal,
                role,
            } = expected
            else {
                unreachable!()
            };
            assert_eq!(input.ordinal, ordinal);
            let layout = channels.channel_layout(graph, node, role).unwrap();
            assert_eq!(
                unit.scope(input.scope).unwrap().schema().as_ref(),
                layout.schema().as_ref()
            );
            let slot = layout.slots()[ordinal as usize];
            assert_eq!(channels.channel_slot(graph, expected), Some(slot));
            assert_ne!(slot.as_u32(), value.get());
            assert_eq!(local.context, invocation.context);
            assert_eq!(channels.channel_type(expected), Some(&physical_expr.ty));
            seen += 1;
        }
    }
    assert_eq!(seen, 4);
    assert_eq!(input_count(&program), 4);
}

#[test]
fn transparent_repeated_value_outputs_preserve_all_three_positions_and_distinct_slots() {
    let functions = rng_subset();
    let source = package(&functions, BoundMode::Input);
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
    assert_eq!(layout.schema().field(1).name(), "bound_a");
    assert_eq!(layout.schema().field(2).name(), "bound_b");
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
    let channels = program.channels();
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
    let source = package_with_outputs(&functions, BoundMode::Input, true, false);
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

    let bound = source.fragment().nodes()[&NodeId::new(13)]
        .expressions
        .as_ref()
        .unwrap();
    let physical_use = bound
        .root_use(novarocks_expression_contract::RootOrdinal::new(0))
        .unwrap();
    let invocation = &bound.control_flow().uses()[&physical_use];
    let definition = bound.definition(invocation.definition).unwrap();
    assert!(matches!(definition.kind, ExprKind::Value(value) if value == ValueId::new(100)));
    let expected_source = ProgramChannelSite::Layout {
        node: ProgramNodeId::new(3),
        role: ProgramChannelLayoutRole::NodeOutput,
        ordinal: 1,
    };
    let channels = program.channels();
    let unit = compiled_unit_for(&program, NodeId::new(13));
    let local = &unit.control_flow().uses()[&physical_use];
    let local_definition = unit.compiled_definition(invocation.definition).unwrap();
    assert_eq!(local.definition, local_definition);
    assert!(matches!(
        unit.definitions().node(local_definition).unwrap().kind(),
        StaticExprKind::Input
    ));
    let input = unit.input(physical_use).unwrap();
    assert_eq!(input.ordinal, 1);
    assert_eq!(
        unit.scope(input.scope).unwrap().schema().as_ref(),
        repeated_layout.schema().as_ref()
    );
    let input_slot = repeated_layout.slots()[input.ordinal as usize];
    assert_eq!(input_slot, repeated_layout.slots()[1]);
    assert_ne!(input_slot, repeated_layout.slots()[2]);
    assert_ne!(input_slot.as_u32(), ValueId::new(100).get());
    assert_eq!(
        channels.channel_slot(graph, expected_source),
        Some(input_slot)
    );
    assert_eq!(channels.channel_type(expected_source), Some(&definition.ty));
    assert_eq!(local.context, invocation.context);
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
    let reference = exprs[0];
    assert_eq!(exprs.len(), 1);
    assert_eq!(
        program
            .unit(reference.unit)
            .unwrap()
            .root(reference.root)
            .unwrap()
            .definition,
        local_definition
    );
    assert_eq!(
        expr_slot_ids.as_slice(),
        graph.nodes()[4].output_layout().slots()
    );
    assert_eq!(expr_slot_ids.len(), 1);
    assert_ne!(expr_slot_ids[0], input_slot);
    assert_eq!(
        graph.nodes()[5].output_layout().slots(),
        expr_slot_ids.as_slice()
    );

    // Complete frozen facts for both independently controlled RAND roots do
    // not make their repeated publication a transparent value read.
    let repeated_rand = package_with_outputs(&functions, BoundMode::Input, false, true);
    assert_eq!(
        repeated_rand.fragment().nodes()[&NodeId::new(8)]
            .expressions
            .as_ref()
            .unwrap()
            .claims()
            .len(),
        2
    );
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
    let source = package_with_outputs(&functions, BoundMode::Input, true, false);
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validate =
        || validate_fragment_providers(source.clone(), &providers, &FixtureControl).unwrap();
    let recorder = CallbackControl::new(CompileControlError::Cancelled, usize::MAX);
    let program = compile_fragment(validate(), &functions, options(1), &recorder).unwrap();
    assert_eq!(input_count(&program), 5);
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
    let source = package_with_outputs(&functions, BoundMode::Input, false, true);
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
    let unit = program
        .units()
        .iter()
        .find(|unit| !unit.calls().is_empty())
        .expect("actual RAND unit");
    assert_eq!(unit.calls().len(), 1);
    let (&occurrence, call) = unit.calls().iter().next().unwrap();
    let novarocks_expression::evaluator_view::UnitStateTemplate::Scalar { scope, kernel } =
        novarocks_expression::evaluator_view::state_template(unit, occurrence).unwrap()
    else {
        panic!("scalar state")
    };
    assert_eq!(scope.occurrence, occurrence);
    assert_eq!(scope.root.get(), 0);
    assert!(std::ptr::eq(kernel.contract().call(), call.call_contract()));
    ScalarEvaluationInstance::instantiate(kernel.clone()).unwrap()
}
fn selected_integers(output: &SelectedValues<'_>) -> Vec<Option<i64>> {
    output
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn compiled_bounded_rand_preserves_selected_rows_nulls_and_row_errors() {
    let functions = rng_subset();
    let program = compile(package(&functions, BoundMode::Input), &functions);
    let mut instance = instance(&program);
    let bounds: ArrayRef = Arc::new(Int64Array::from(vec![Some(-1), Some(1), None, Some(2)]));
    let arguments = [EvaluatedArgument::Column(&bounds)];

    let sparse = instance
        .evaluate(
            Selection::try_sparse(4, &[1, 2, 3]).unwrap(),
            &arguments,
            &EvaluationControl,
        )
        .unwrap();
    assert!(sparse.errors().is_empty());
    let values = selected_integers(&sparse);
    assert_eq!(values[..2], [Some(0), None]);
    assert!((0..2).contains(&values[2].unwrap()));

    let all = instance
        .evaluate(Selection::all(4), &arguments, &EvaluationControl)
        .unwrap();
    assert_eq!(all.errors().len(), 1);
    assert_eq!(all.errors()[0].selected_ordinal(), 0);
    let values = selected_integers(&all);
    assert_eq!(values[..3], [None, Some(0), None]);
    assert!((0..2).contains(&values[3].unwrap()));

    let bounds: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(1)]));
    let arguments = [EvaluatedArgument::Column(&bounds)];
    let next = instance
        .evaluate(Selection::all(2), &arguments, &EvaluationControl)
        .unwrap();
    assert!(next.errors().is_empty());
    assert_eq!(selected_integers(&next), [None, Some(0)]);
}

#[test]
fn original_nonconstant_bound_is_not_promoted_by_its_physical_literal() {
    let functions = rng_subset();
    let source = package(&functions, BoundMode::LiteralWithoutConstant);
    let unit = source.fragment().nodes()[&NodeId::new(8)]
        .expressions
        .as_ref()
        .unwrap();
    let (&id, request) = unit.requests().iter().next().unwrap();
    assert!(request.expected_result_type.is_none());
    assert_eq!(request.logical_argument_count, 1);
    assert!(matches!(
        request.arguments[0],
        novarocks_physical_plan::StaticFunctionArgument::Value { constant: None, .. }
    ));
    let ExprKind::FunctionCall { args, .. } = &unit.definition(id).unwrap().kind else {
        panic!("actual RAND call");
    };
    let child = unit.definition(args[0]).unwrap();
    let ExprKind::Constant(reference) = child.kind else {
        panic!("actual checked bound")
    };
    assert_eq!(
        source
            .constants()
            .resolve_observed(
                reference,
                &child.ty,
                &mut CompileCheckpoints::try_new(&FixtureControl, CompilePhase::Validate).unwrap()
            )
            .unwrap()
            .try_i64()
            .unwrap(),
        Some(1)
    );
    let program = compile(source, &functions);
    let mut instance = instance(&program);
    let bound: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let arguments = [EvaluatedArgument::Scalar(&bound)];
    for _ in 0..2 {
        let output = instance
            .evaluate(Selection::all(2), &arguments, &EvaluationControl)
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(selected_integers(&output), [Some(0), Some(0)]);
    }
}

#[test]
fn compiled_direct_constant_bound_retains_exact_value_across_instances_and_batches() {
    let functions = rng_subset();
    let program = compile(package(&functions, BoundMode::DirectConstant), &functions);
    let mut a = instance(&program);
    let mut b = instance(&program);
    let bound: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let arguments = [EvaluatedArgument::Scalar(&bound)];
    let evaluate = |instance: &mut ScalarEvaluationInstance, rows| {
        let output = instance
            .evaluate(Selection::all(rows), &arguments, &EvaluationControl)
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(selected_integers(&output), vec![Some(0); rows]);
    };
    evaluate(&mut a, 2);
    evaluate(&mut b, 1);
    evaluate(&mut a, 1);
    evaluate(&mut b, 2);
}

#[test]
fn compiled_bounded_rand_kernel_empty_selection_keeps_zero_rows_without_fallback() {
    let functions = rng_subset();
    let program = compile(package(&functions, BoundMode::Input), &functions);
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
