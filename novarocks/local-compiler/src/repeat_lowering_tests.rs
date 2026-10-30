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
use arrow_schema::DataType;
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
    InstalledPureKernel, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi,
};
use novarocks_local_program::{
    KernelAbiVersion, LocalOperatorId, LocalOperatorOrigin, ProgramChannelLayoutRole,
    ProgramChannelSite, ProgramLexicalSource, ProgramNodeId, ProgramNodeKind, StaticExprKind,
    StaticSinkProgram,
};
use novarocks_physical_plan::{
    ExprKind, FragmentBuilder, FragmentCuts, FragmentId, FragmentPackage, FragmentPackageAdmission,
    FragmentPackageInput, FragmentSink, FrozenFragmentCalls, FrozenFragmentPruning, GroupingOutput,
    LiteralValue, NodeId, PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain, PlanLimits,
    PlanVersionId, PropertyProofProjectionLimits, RequiredContracts, ResultField, ResultPort,
    ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionValueType, PureCompileControl, SemanticParameters,
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn functions() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("rand", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    // Actual unused RAND owner, with independently authored two receipts.
    // This fixture is not a Server catalogue closure claim.
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
fn admission() -> FragmentPackageAdmission {
    // Explicit small-fixture invoice and independent projection ceilings.
    FragmentPackageAdmission {
        plan_limits: PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}
fn options() -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        exchange_wait: std::time::Duration::from_secs(120),
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
enum Case {
    Ordinary,
    NestedKey,
    EmptySets,
    BadGroupingCarrier,
    BadGroupingNullable,
    BadReplacementNullable,
    BadReplacementType,
    TooManyArguments,
}
struct Fixture {
    package: Arc<FragmentPackage>,
    repeat: NodeId,
    duplicate: NodeId,
    downstream: Option<NodeId>,
}
fn add_projection(
    builder: &mut FragmentBuilder,
    node: NodeId,
    source: NodeId,
    items: &[(ExprKind, FunctionValueType)],
) -> Vec<ValueId> {
    let mut projected = Vec::new();
    let mut values = Vec::new();
    for (kind, ty) in items {
        let expression = builder
            .add_expression(node, ty.clone(), kind.clone())
            .unwrap();
        let value = builder
            .add_value(
                ty.clone(),
                ValueOrigin::Expr {
                    node,
                    expr: expression,
                },
            )
            .unwrap();
        projected.push((expression, value));
        values.push(value);
    }
    builder
        .add_project(
            node,
            source,
            projected.into_boxed_slice(),
            values.clone().into_boxed_slice(),
        )
        .unwrap();
    values
}

fn fixture(
    keys: usize,
    repetitions: usize,
    downstream: bool,
    case: Case,
) -> Result<Fixture, Box<dyn std::error::Error>> {
    assert!(keys >= 2);
    let fragment_id = FragmentId::new(73);
    let empty = NodeId::new(u32::MAX);
    let initial = NodeId::new(41);
    let duplicate = NodeId::new(0);
    let repeat = NodeId::new(900);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(empty, Box::from([Box::default()]), Box::default())
        .unwrap();
    let integer = FunctionValueType::new(DataType::Int64, false);
    let items = (0..=keys)
        .map(|index| {
            if index == 0 && matches!(case, Case::NestedKey) {
                let field = arrow_schema::Field::new("nested-source", DataType::Utf8, true)
                    .with_metadata(std::collections::HashMap::<String, String>::from([(
                        "source-fidelity".to_owned(),
                        "kept-exact".to_owned(),
                    )]));
                (
                    ExprKind::Literal(LiteralValue::Null),
                    FunctionValueType::new(DataType::Struct(vec![Arc::new(field)].into()), true),
                )
            } else {
                (
                    ExprKind::Literal(LiteralValue::Int64(11 + index as i64)),
                    integer.clone(),
                )
            }
        })
        .collect::<Vec<_>>();
    let initial_values = add_projection(&mut builder, initial, empty, &items);
    let mut projection = Vec::new();
    let mut published = Vec::new();
    let mut output = Vec::new();
    for (index, source) in initial_values.iter().enumerate() {
        let expression = builder
            .add_expression(duplicate, items[index].1.clone(), ExprKind::Value(*source))
            .unwrap();
        let value = builder
            .add_value(
                items[index].1.clone(),
                ValueOrigin::Expr {
                    node: duplicate,
                    expr: expression,
                },
            )
            .unwrap();
        projection.push((expression, value));
        published.push(value);
        output.push(value);
        if index == 0 {
            // One authored definition can appear twice in the output port;
            // its origin still names its one exact defining expression.
            projection.push((expression, value));
            output.push(value);
        }
    }
    builder
        .add_project(
            duplicate,
            initial,
            projection.into_boxed_slice(),
            output.clone().into_boxed_slice(),
        )
        .unwrap();
    let rollup = &published[..keys];
    let sets = if matches!(case, Case::EmptySets) {
        Vec::new()
    } else {
        (0..repetitions)
            .map(|index| match index % 4 {
                0 => rollup.to_vec().into_boxed_slice(),
                1 | 3 => Box::from([rollup[keys - 1]]),
                _ => Box::default(),
            })
            .collect::<Vec<_>>()
    };
    let mut replacements = BTreeMap::new();
    if !sets.is_empty() {
        for source in rollup {
            let source_index = published.iter().position(|value| value == source).unwrap();
            let ty = match case {
                Case::BadReplacementNullable => FunctionValueType::new(DataType::Int64, false),
                Case::BadReplacementType => FunctionValueType::new(DataType::Float64, true),
                _ => {
                    let mut ty = items[source_index].1.clone();
                    ty = ty.with_nullable(true).unwrap();
                    ty
                }
            };
            let value = builder
                .add_value(
                    ty,
                    ValueOrigin::NullExtended {
                        node: repeat,
                        of: *source,
                    },
                )
                .unwrap();
            replacements.insert(*source, value);
        }
    }
    let mut repeat_output = output
        .iter()
        .map(|value| replacements.get(value).copied().unwrap_or(*value))
        .collect::<Vec<_>>();
    let arguments: Vec<ValueId> = if keys == 64 {
        rollup[..if matches!(case, Case::TooManyArguments) {
            64
        } else {
            63
        }]
            .to_vec()
    } else {
        vec![rollup[0], rollup[keys - 1]]
    };
    let mut grouping_outputs = Vec::new();
    for reverse in [false, true] {
        let ty = match case {
            Case::BadGroupingCarrier => FunctionValueType::new(DataType::Float64, false),
            Case::BadGroupingNullable => FunctionValueType::new(DataType::Int64, true),
            _ => integer.clone(),
        };
        let value = builder
            .add_value(
                ty,
                ValueOrigin::NodeOutput {
                    node: repeat,
                    output_ordinal: repeat_output.len() as u32,
                },
            )
            .unwrap();
        repeat_output.push(value);
        let mut ordered = arguments.clone();
        if reverse {
            ordered.reverse();
        }
        grouping_outputs.push(GroupingOutput {
            output: value,
            arguments: ordered.into_boxed_slice(),
        });
    }
    builder
        .add_repeat(
            repeat,
            duplicate,
            rollup.to_vec().into_boxed_slice(),
            sets.into_boxed_slice(),
            replacements
                .into_iter()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            grouping_outputs.into_boxed_slice(),
            repeat_output.clone().into_boxed_slice(),
        )
        .unwrap();
    let (root, result_values, downstream) = if downstream {
        let node = NodeId::new(7);
        let source = repeat_output[0];
        let ty = FunctionValueType::new(DataType::Int64, true);
        let values = add_projection(&mut builder, node, repeat, &[(ExprKind::Value(source), ty)]);
        (node, values, Some(node))
    } else {
        (repeat, repeat_output, None)
    };
    let fragment = builder.finish_structure(
        root,
        FragmentSink::Result,
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        PlanLimits::FROZEN,
        &Control,
        novarocks_physical_plan::StagePlacementPolicy::Singleton,
    )?;
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control)?;
    let domain = EvaluationDomainId::new(u32::MAX);
    let mut invocations = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        assert!(matches!(
            fragment.expressions().get(root.expr).unwrap().kind,
            ExprKind::Value(_) | ExprKind::Literal(_)
        ));
        let use_id = ExpressionUseId::new(u32::MAX - ordinal as u32);
        invocations.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id,
                domain,
                demand: root.demand,
            },
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, use_id));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )?;
    let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &Control)?;
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &Control)?;
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&root].output.clone(),
        fields: result_values
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: format!("original_{ordinal}").into_boxed_str(),
                alias: Some(format!("selected_{ordinal}").into_boxed_str()),
                value: *value,
                ty: fragment.values()[value].ty.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    };
    let package = FragmentPackage::try_new(
        FragmentPackageInput {
            constants: novarocks_physical_plan::ConstantPools::empty(),
            version: PlanVersionId::try_new([73; 16]).unwrap(),
            required: RequiredContracts::default(),
            fragment,
            expression_uses: uses,
            calls,
            pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &Control)?,
            cuts: FragmentCuts::default(),
            result: Some(result),
            parameters: SemanticParameters::try_new([])?,
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        admission(),
        &Control,
    )?;
    Ok(Fixture {
        package: Arc::new(package),
        repeat,
        duplicate,
        downstream,
    })
}

#[test]
fn repeat_nullable_replacement_preserves_nested_field_metadata_and_complete_channel_type() {
    let source = fixture(2, 4, false, Case::NestedKey).unwrap();
    let program = compile(&source, &functions(), &Control).unwrap();
    let node = &program.graph().nodes()[3];
    for ordinal in [0, 1] {
        let field = node.output_layout().schema().field(ordinal);
        let DataType::Struct(children) = field.data_type() else {
            panic!("actual nested source carrier");
        };
        assert_eq!(children[0].name(), "nested-source");
        assert!(children[0].metadata().is_empty());
        assert!(children[0].is_nullable());
        assert!(field.is_nullable());
        let expected = &source.package.fragment().values()[&source.package.fragment().nodes()
            [&source.repeat]
            .output
            .columns[ordinal]]
            .ty;
        let actual = program
            .checked()
            .channels()
            .channel_type(ProgramChannelSite::Layout {
                node: node.local_id().expect("compiled local identity"),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: ordinal as u32,
            })
            .unwrap();
        assert!(
            actual
                .exactly_equals_observed::<crate::repeat::RepeatLoweringError>(expected, || Control
                    .checkpoint(CompilePhase::LowerProgram, 0)
                    .map_err(Into::into))
                .unwrap()
        );
    }
}

fn compile(
    fixture: &Fixture,
    functions: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<novarocks_local_program::LocalProgram, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let validated =
        validate_fragment_providers(fixture.package.clone(), &providers, &Control).unwrap();
    compile_fragment(validated, functions, options(), control)
}

#[test]
fn repeat_complete_lowering_preserves_every_occurrence_ordered_bits_headers_and_provenance() {
    let fixture = fixture(2, 4, false, Case::Ordinary).unwrap();
    let program = compile(&fixture, &functions(), &Control).unwrap();
    let graph = program.graph();
    assert_eq!(graph.nodes().len(), 4);
    assert_eq!(graph.root(), ProgramNodeId::new(3));
    let child = &graph.nodes()[2];
    let node = &graph.nodes()[3];
    let ProgramNodeKind::Repeat {
        input,
        null_slot_ids,
        grouping_slot_ids,
        grouping_list,
        repeat_times,
    } = node.kind()
    else {
        panic!("complete Repeat lowering");
    };
    let slots = child.output_layout().slots();
    assert_eq!(*input, ProgramNodeId::new(2));
    assert_eq!(*repeat_times, 4);
    assert_eq!(
        null_slot_ids,
        &vec![
            vec![],
            vec![slots[0], slots[1]],
            vec![slots[0], slots[1], slots[2]],
            vec![slots[0], slots[1]]
        ]
    );
    // Independent hand oracle: [a,b], [b], [], [b], with two argument orders.
    assert_eq!(grouping_list, &vec![vec![0, 2, 3, 2], vec![0, 1, 3, 1]]);
    assert_eq!(&node.output_layout().slots()[..4], slots);
    assert_eq!(&node.output_layout().slots()[4..], grouping_slot_ids);
    assert_ne!(slots[0], slots[1]);
    for ordinal in 0..6 {
        let field = node.output_layout().schema().field(ordinal);
        assert_eq!(field.name(), &format!("selected_{ordinal}"));
        assert_eq!(field.data_type(), &DataType::Int64);
        assert_eq!(field.is_nullable(), ordinal < 3);
        let complete = program
            .checked()
            .channels()
            .channel_type(ProgramChannelSite::Layout {
                node: node.local_id().expect("compiled local identity"),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: ordinal as u32,
            })
            .unwrap();
        assert_eq!(
            complete,
            &FunctionValueType::new(DataType::Int64, ordinal < 3)
        );
    }
    assert!(matches!(graph.sink(), Some(StaticSinkProgram::Result)));
    assert_eq!(graph.profile().pipeline_dop().get(), 1);
    assert_eq!(node.physical_sources()[0].get(), fixture.repeat.get());
    assert_eq!(child.physical_sources()[0].get(), fixture.duplicate.get());
    let provenance = program.provenance().get(LocalOperatorId::new(3)).unwrap();
    assert!(matches!(provenance.origin, LocalOperatorOrigin::Direct));
    assert_eq!(provenance.cost_owner, LocalOperatorId::new(3));
    assert!(
        program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .calls()
            .is_empty()
    );
}

#[test]
fn repeat_downstream_value_uses_proved_replacement_representative_and_keeps_root_binding() {
    let fixture = fixture(2, 4, true, Case::Ordinary).unwrap();
    let program = compile(&fixture, &functions(), &Control).unwrap();
    let graph = program.graph();
    let repeat = &graph.nodes()[3];
    let final_node = &graph.nodes()[4];
    assert_eq!(
        final_node.physical_sources()[0].get(),
        fixture.downstream.unwrap().get()
    );
    let ProgramNodeKind::Project { exprs, .. } = final_node.kind() else {
        panic!("actual downstream Project");
    };
    assert!(
        matches!(graph.expressions().node(exprs[0]).unwrap().kind(), StaticExprKind::SlotId(slot) if *slot == repeat.output_layout().slots()[0])
    );
    assert!(program.checked().slots().values().any(|source| *source
        == ProgramLexicalSource::Input(ProgramChannelSite::Layout {
            node: repeat.local_id().expect("compiled local identity"),
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 0
        })));
    assert_eq!(
        final_node.output_layout().schema().field(0).name(),
        "selected_0"
    );
    assert!(final_node.output_layout().schema().field(0).is_nullable());
    assert!(
        program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot()
            .bindings()
            .keys()
            .all(|site| match site {
                novarocks_local_program::ProgramExpressionRootSite::Node { node, .. } =>
                    *node != repeat.local_id().expect("compiled local identity"),
                _ => true,
            })
    );
}

#[test]
fn repeat_sixty_four_rollup_keys_preserve_existing_sixty_three_grouping_argument_domain() {
    let source = fixture(64, 4, false, Case::Ordinary).unwrap();
    let program = compile(&source, &functions(), &Control).unwrap();
    let ProgramNodeKind::Repeat {
        grouping_list,
        repeat_times,
        ..
    } = program.graph().nodes()[3].kind()
    else {
        panic!("Repeat");
    };
    assert_eq!(*repeat_times, 4);
    assert_eq!(
        grouping_list,
        &vec![vec![0, i64::MAX, i64::MAX, i64::MAX]; 2]
    );
    let too_many = fixture(64, 4, false, Case::TooManyArguments).unwrap();
    assert!(matches!(
        compile(&too_many, &functions(), &Control),
        Err(FragmentCompileError::Invalid(
            "GROUPING output source or argument count differs"
        ))
    ));
}

#[test]
fn repeat_static_gates_refuse_empty_sets_noninteger_grouping_and_nullable_source_drift() {
    let functions = functions();
    for (case, expected) in [
        (
            Case::EmptySets,
            "Repeat requires one input and nonempty grouping sets",
        ),
        (
            Case::BadGroupingCarrier,
            "GROUPING output must be required physical Int64",
        ),
        (
            Case::BadGroupingNullable,
            "GROUPING output must be required physical Int64",
        ),
    ] {
        let source =
            fixture(2, 4, false, case).expect("complete valid source reaches compiler gate");
        let result = compile(&source, &functions, &Control);
        assert!(
            matches!(&result,
            Err(FragmentCompileError::Invalid(actual)) if *actual == expected),
            "expected {expected}, actual {result:?}"
        );
    }
    for case in [Case::BadReplacementNullable, Case::BadReplacementType] {
        let error = match fixture(2, 4, false, case) {
            Err(error) => error,
            Ok(_) => panic!("malformed NULL extension passed the source author"),
        };
        let Some(novarocks_physical_plan::FragmentStructureError::Structure(errors)) =
            error.downcast_ref::<novarocks_physical_plan::FragmentStructureError>()
        else {
            panic!("exact source structural gate: {error:?}");
        };
        // The fixture intentionally malforms both rollup-key replacements.
        assert_eq!(errors.errors().len(), 2);
        for error in errors.errors() {
            assert_eq!(
                error.category(),
                novarocks_physical_plan::ValidationErrorCategory::StructuralInvariant
            );
            assert!(error.path().contains(".values["));
            assert_eq!(
                error.message(),
                "null-extended value must preserve the data type and be nullable"
            );
        }
    }
}

struct Trace {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    at: Option<usize>,
    cause: CompileControlError,
    refused: AtomicBool,
}
impl Trace {
    fn new(at: Option<usize>, cause: CompileControlError) -> Self {
        Self {
            events: Mutex::new(vec![]),
            at,
            cause,
            refused: AtomicBool::new(false),
        }
    }
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(
            !self.refused.load(Ordering::SeqCst),
            "no checkpoint after original refusal"
        );
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        events.push((phase, units));
        if self.at == Some(at) {
            self.refused.store(true, Ordering::SeqCst);
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}
#[test]
fn repeat_full_compiler_preserves_every_original_control_prefix_and_ordinary_tail() {
    let functions = functions();
    for case in [Case::Ordinary, Case::BadGroupingCarrier, Case::EmptySets] {
        let fixture = fixture(2, 4, false, case).unwrap();
        let trace = Trace::new(None, CompileControlError::Cancelled);
        let result = compile(&fixture, &functions, &trace);
        if matches!(case, Case::Ordinary) {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(FragmentCompileError::Invalid(_))));
        }
        let expected = trace.events.into_inner().unwrap();
        assert!(!expected.is_empty());
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..expected.len() {
                let control = Trace::new(Some(at), cause.clone());
                assert!(
                    matches!(compile(&fixture, &functions, &control), Err(FragmentCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
            }
        }
    }
}
#[test]
fn repeat_wide_actual_grouping_work_has_entry_quantum_tail_refusal_without_replay() {
    let functions = functions();
    let fixture = fixture(2, 320, false, Case::Ordinary).unwrap();
    let trace = Trace::new(None, CompileControlError::Cancelled);
    let program = compile(&fixture, &functions, &trace).unwrap();
    let ProgramNodeKind::Repeat {
        repeat_times,
        grouping_list,
        ..
    } = program.graph().nodes()[3].kind()
    else {
        panic!("Repeat");
    };
    assert_eq!(*repeat_times, 320);
    assert_eq!(grouping_list[0].len(), 320);
    let expected = trace.events.into_inner().unwrap();
    let quantum = expected
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("actual grouping work crosses 256");
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [0, quantum, expected.len() - 1] {
            let control = Trace::new(Some(at), cause.clone());
            assert!(
                matches!(compile(&fixture, &functions, &control), Err(FragmentCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
        }
    }
}
