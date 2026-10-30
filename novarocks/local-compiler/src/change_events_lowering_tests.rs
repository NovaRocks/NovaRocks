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
use arrow_array::{Array, ListArray};
use arrow_schema::{DataType, Field};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    ConstantPolicy, ConstantPool, EngineFunctionCatalogBuilder, FunctionId, FunctionKind,
    FunctionOverloadId, InstalledPureKernel, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
};
use novarocks_local_program::{KernelAbiVersion, ProgramNodeId, ProgramNodeKind, StaticExprKind};
use novarocks_physical_plan::{
    ConstantPoolId, ConstantPools, ConstantReference, ExprKind, FragmentBuilder, FragmentCuts,
    FragmentId, FragmentPackage, FragmentPackageAdmission, FragmentPackageInput, FragmentSink,
    FrozenFragmentCalls, FrozenFragmentPruning, LiteralValue, NodeId, PhysicalExpressionRoots,
    PhysicalRootUses, PipelineDopDomain, PlanLimits, PlanVersionId, PropertyProofProjectionLimits,
    RequiredContracts, ResultField, ResultPort, ValueId, ValueOrigin,
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

fn project(
    builder: &mut FragmentBuilder,
    node: NodeId,
    input: NodeId,
    items: Vec<(ExprKind, FunctionValueType)>,
) -> Vec<ValueId> {
    let mut definitions = Vec::new();
    let mut output = Vec::new();
    for (kind, ty) in items {
        let expr = builder.add_expression(node, ty.clone(), kind).unwrap();
        let value = builder
            .add_value(ty, ValueOrigin::Expr { node, expr })
            .unwrap();
        definitions.push((expr, value));
        output.push(value);
    }
    builder
        .add_project(
            node,
            input,
            definitions.into_boxed_slice(),
            output.clone().into_boxed_slice(),
        )
        .unwrap();
    output
}
struct Fixture {
    package: Arc<FragmentPackage>,
}
fn try_fixture(
    count: usize,
    downstream: bool,
    repeated: bool,
) -> Result<Fixture, Box<dyn std::error::Error>> {
    let fid = FragmentId::new(81);
    let empty = NodeId::new(u32::MAX);
    let initial = NodeId::new(0);
    let event_node = NodeId::new(901);
    let mut builder = FragmentBuilder::new(fid);
    builder
        .add_values(empty, Box::from([Box::default()]), Box::default())
        .unwrap();
    let field = Arc::new(
        Field::new("original-child", DataType::Int32, true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "source-key".to_owned(),
                "kept-λ".to_owned(),
            )]),
        ),
    );
    let list = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
        Some(vec![Some(99)]),
        Some(vec![Some(2), None]),
        None,
    ]);
    let list = ListArray::try_new(
        field.clone(),
        list.offsets().clone(),
        list.values().clone(),
        list.nulls().cloned(),
    )
    .unwrap();
    let nested = FunctionValueType::new(DataType::List(field), true);
    let pool_id = ConstantPoolId::new(u32::MAX);
    let pool = ConstantPool::try_new(
        Arc::new(Field::new(
            "original-pool",
            list.data_type().clone(),
            nested.nullable(),
        )),
        nested.clone(),
        list.to_data(),
        options().constants,
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let selected = ConstantReference {
        pool: pool_id,
        ordinal: 1,
    };
    let integer = FunctionValueType::new(DataType::Int64, false);
    let text = FunctionValueType::new(DataType::Utf8, true);
    let initial_values = project(
        &mut builder,
        initial,
        empty,
        vec![
            (ExprKind::Literal(LiteralValue::Int64(11)), integer.clone()),
            (ExprKind::Constant(selected), nested.clone()),
            (ExprKind::Literal(LiteralValue::Null), text.clone()),
        ],
    );

    let expr = builder
        .add_expression(
            event_node,
            integer.clone(),
            ExprKind::Value(initial_values[0]),
        )
        .unwrap();
    let nested_expr = builder
        .add_expression(
            event_node,
            nested.clone(),
            ExprKind::Value(initial_values[1]),
        )
        .unwrap();
    let predicate = builder
        .add_expression(
            event_node,
            FunctionValueType::new(DataType::Boolean, false),
            ExprKind::Literal(LiteralValue::Boolean(true)),
        )
        .unwrap();
    let value = builder
        .add_value(
            FunctionValueType::new(DataType::Int64, true),
            ValueOrigin::NodeOutput {
                node: event_node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let effect = builder
        .add_value(
            FunctionValueType::new(DataType::Int8, false),
            ValueOrigin::NodeOutput {
                node: event_node,
                output_ordinal: 1,
            },
        )
        .unwrap();
    let nested_value = builder
        .add_value(
            nested.clone(),
            ValueOrigin::NodeOutput {
                node: event_node,
                output_ordinal: 2,
            },
        )
        .unwrap();
    let mut output = vec![value, effect, nested_value];
    // NodeOutput owns one exact ordinal; repetition is a structural refusal.
    if repeated {
        output.push(value);
    }
    let events = (0..count)
        .map(|index| novarocks_physical_plan::ChangeEventSpec {
            predicate: (index % 2 == 0).then_some(predicate),
            effect: [
                novarocks_connector_contract::ConnectorRowMutationEffect::Delete,
                novarocks_connector_contract::ConnectorRowMutationEffect::Replace,
                novarocks_connector_contract::ConnectorRowMutationEffect::Insert,
            ][index % 3],
            assignments: if index % 2 == 0 {
                Box::from([(nested_value, Some(nested_expr)), (value, Some(expr))])
            } else {
                Box::from([(value, None), (nested_value, None)])
            },
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let properties = builder.node_output_properties(initial).unwrap().clone();
    builder
        .insert_node_unchecked(novarocks_physical_plan::PhysicalNode {
            id: event_node,
            inputs: Box::from([initial]),
            required_inputs: Box::from([novarocks_physical_plan::passthrough_requirement(
                &properties,
            )]),
            output_properties: novarocks_physical_plan::PhysicalProperties {
                distribution: novarocks_physical_plan::Distribution::Unconstrained,
                row_multiplicity: novarocks_physical_plan::RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: novarocks_physical_plan::OutputPort {
                node: event_node,
                columns: output.into_boxed_slice(),
            },
            kind: novarocks_physical_plan::NodeKind::ChangeEventExpand {
                events,
                effect_output: effect,
            },
        })
        .unwrap();
    let root = if downstream {
        let next = NodeId::new(7);
        project(
            &mut builder,
            next,
            event_node,
            vec![
                (ExprKind::Value(nested_value), nested),
                (
                    ExprKind::Value(effect),
                    FunctionValueType::new(DataType::Int8, false),
                ),
            ],
        );
        next
    } else {
        event_node
    };
    let fragment = builder.finish_structure(
        root,
        FragmentSink::Result,
        PipelineDopDomain {
            min: 1,
            max: 2,
            requires_power_of_two: false,
        },
        PlanLimits::FROZEN,
        &Control,
        novarocks_physical_plan::StagePlacementPolicy::Singleton,
    )?;
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).unwrap();
    let domain = EvaluationDomainId::new(u32::MAX);
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let id = ExpressionUseId::new(u32::MAX - ordinal as u32);
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                domain,
                demand: root.demand,
            },
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, id));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &Control).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &Control).unwrap();
    let output = fragment.nodes()[&root].output.columns.clone();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fid,
        output: fragment.nodes()[&root].output.clone(),
        fields: output
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
    let mut constants = ConstantPools::empty();
    constants.insert(pool_id, pool).unwrap();
    let package = FragmentPackage::try_new(
        FragmentPackageInput {
            constants,
            version: PlanVersionId::try_new([81; 16]).unwrap(),
            required: RequiredContracts::default(),
            fragment,
            expression_uses: uses,
            calls,
            pruning: FrozenFragmentPruning::try_new(fid, vec![], &Control).unwrap(),
            cuts: FragmentCuts::default(),
            result: Some(result),
            parameters: SemanticParameters::try_new([]).unwrap(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        admission(),
        &Control,
    )
    .unwrap();

    Ok(Fixture {
        package: Arc::new(package),
    })
}
fn fixture(count: usize, downstream: bool, repeated: bool) -> Fixture {
    try_fixture(count, downstream, repeated).unwrap()
}
fn compile(
    source: &Fixture,
    control: &dyn PureCompileControl,
) -> Result<novarocks_local_program::LocalProgram, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let checked =
        validate_fragment_providers(source.package.clone(), &providers, &Control).unwrap();
    compile_fragment(checked, &functions(), options(), control)
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
            "no callback after original refusal"
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
fn complete_change_events_keep_original_order_effects_sparse_provenance_null_and_full_types() {
    let source = fixture(3, false, false);
    let program = compile(&source, &Control).unwrap();
    let graph = program.graph();
    let node = &graph.nodes()[2];
    let ProgramNodeKind::ChangeEventExpand {
        input,
        events,
        output_slot_ids,
        effect_slot_id,
    } = node.kind()
    else {
        panic!("real change-event lowering");
    };
    assert_eq!(*input, ProgramNodeId::new(1));
    assert_eq!(output_slot_ids, node.output_layout().slots());
    assert_eq!(*effect_slot_id, output_slot_ids[1]);
    assert_eq!(events.len(), 3);
    assert_eq!(
        events.iter().map(|e| e.effect as i8).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert!(events[0].predicate.is_some());
    assert!(events[1].predicate.is_none());
    assert_eq!(
        events[0]
            .assignments
            .iter()
            .map(|a| a.output_slot_id)
            .collect::<Vec<_>>(),
        [output_slot_ids[2], output_slot_ids[0]]
    );
    assert!(events[1].assignments.iter().all(|a| a.expr.is_none()));
    assert!(events[0].assignments.iter().all(|a| a.expr.is_some()));
    for (ordinal, value) in source.package.fragment().nodes()[&NodeId::new(901)]
        .output
        .columns
        .iter()
        .enumerate()
    {
        let ty = FunctionValueType::try_from_field(node.output_layout().schema().field(ordinal))
            .unwrap();
        assert_eq!(ty, source.package.fragment().values()[value].ty);
        assert_eq!(
            node.output_layout().schema().field(ordinal).name(),
            &format!("selected_{ordinal}")
        );
    }
    assert_eq!(graph.nodes().len(), 3);
}

#[test]
fn downstream_reads_change_event_output_ordinal_not_original_input_slot() {
    let source = fixture(3, true, false);
    let program = compile(&source, &Control).unwrap();
    let graph = program.graph();
    assert_eq!(graph.nodes().len(), 4);
    let ProgramNodeKind::Project { exprs, .. } = graph.nodes()[3].kind() else {
        panic!("project");
    };
    for (expr, ordinal) in exprs.iter().zip([2, 1]) {
        let StaticExprKind::SlotId(slot) = graph.expressions().node(*expr).unwrap().kind() else {
            panic!("actual slot");
        };
        assert_eq!(*slot, graph.nodes()[2].output_layout().slots()[ordinal]);
    }
}

#[test]
fn mandatory_source_refuses_repeated_node_output_before_compiler() {
    let Err(error) = try_fixture(3, false, true) else {
        panic!("invalid duplicate NodeOutput");
    };
    assert!(
        error
            .downcast_ref::<novarocks_physical_plan::FragmentStructureError>()
            .is_some()
    );
    assert!(error.to_string().contains("is not produced by this node"));
}

#[test]
fn change_events_preserve_parallel_driver_width_and_every_compile_callback_prefix() {
    for parallel in [false, true] {
        let source = fixture(3, false, false);
        let invoke = |control: &dyn PureCompileControl| {
            let providers =
                PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control)
                    .unwrap();
            let checked =
                validate_fragment_providers(source.package.clone(), &providers, &Control).unwrap();
            let mut selected_options = options();
            if parallel {
                selected_options.root_sink_dop = None;
                selected_options.pipeline_dop = NonZeroUsize::new(2).unwrap();
            }
            compile_fragment(checked, &functions(), selected_options, control)
        };
        let baseline = Trace::new(None, CompileControlError::Cancelled);
        let result = invoke(&baseline);
        let program = result.expect("change events preserve the admitted driver width");
        assert_eq!(
            program.graph().profile().pipeline_dop().get(),
            if parallel { 2 } else { 1 }
        );
        let expected = baseline.events.lock().unwrap().clone();
        for at in 0..expected.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Trace::new(Some(at), cause.clone());
                assert!(
                    matches!(invoke(&control), Err(FragmentCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
            }
        }
    }
}

#[test]
fn wide_actual_change_event_list_crosses_quantum_without_partial_program() {
    let source = fixture(320, false, false);
    let baseline = Trace::new(None, CompileControlError::Cancelled);
    compile(&source, &baseline).unwrap();
    let expected = baseline.events.lock().unwrap().clone();
    let quantum = expected
        .iter()
        .position(|(_, units)| *units == 256)
        .unwrap();
    for at in [0, quantum, expected.len() - 1] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Trace::new(Some(at), cause.clone());
            assert!(
                matches!(compile(&source, &control), Err(FragmentCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
        }
    }
}
