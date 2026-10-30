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
use novarocks_local_program::{
    AssertRowsMode, KernelAbiVersion, LocalOperatorId, LocalOperatorOrigin,
    ProgramChannelLayoutRole, ProgramChannelSite, ProgramLexicalSource, ProgramNodeId,
    ProgramNodeKind, RowAssertion, StaticExprKind, StaticSinkProgram,
};
use novarocks_physical_plan::{
    ConstantPoolId, ConstantPools, ConstantReference, ExprKind, FragmentBuilder, FragmentCuts,
    FragmentId, FragmentPackage, FragmentPackageAdmission, FragmentPackageInput, FragmentSink,
    FrozenFragmentCalls, FrozenFragmentPruning, LiteralValue, NodeId, PhysicalExpressionRoots,
    PhysicalRootUses, PipelineDopDomain, PlanLimits, PlanVersionId, PropertyProofProjectionLimits,
    RequiredContracts, ResultField, ResultPort, RowCountAssertion, RowCountAssertionSpec, ValueId,
    ValueOrigin,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, EvaluationDomainId,
    ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, FunctionValueType, PureCompileControl,
    SemanticParameters,
};
use novarocks_types::SlotId;
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
enum Shape {
    Global(RowCountAssertion, u64),
    Keyed(usize),
    Conditional,
    ForeignMask,
    WrongMaskType,
    EmptySubject,
    EmptyKeys,
    EmptyLabel,
    WrongLabelCount,
    EmptyMessage,
    ForeignKey,
}
struct Fixture {
    package: Arc<FragmentPackage>,
    assertion: NodeId,
    selected: ConstantReference,
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
fn fixture(shape: Shape, downstream: bool) -> Result<Fixture, Box<dyn std::error::Error>> {
    let fid = FragmentId::new(81);
    let empty = NodeId::new(u32::MAX);
    let initial = NodeId::new(0);
    let duplicate = NodeId::new(43);
    let assertion = NodeId::new(901);
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
    )?;
    let selected = ConstantReference {
        pool: pool_id,
        ordinal: 1,
    };
    let integer = FunctionValueType::new(DataType::Int64, false);
    let text = FunctionValueType::new(
        if matches!(shape, Shape::Conditional | Shape::ForeignMask) {
            DataType::Boolean
        } else {
            DataType::Utf8
        },
        true,
    );
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
    let mut projected = Vec::new();
    let mut child = Vec::new();
    for (index, (value, ty)) in initial_values
        .iter()
        .copied()
        .zip([integer.clone(), nested, text.clone()])
        .enumerate()
    {
        let expr = builder
            .add_expression(duplicate, ty.clone(), ExprKind::Value(value))
            .unwrap();
        let output = builder
            .add_value(
                ty,
                ValueOrigin::Expr {
                    node: duplicate,
                    expr,
                },
            )
            .unwrap();
        projected.push((expr, output));
        child.push(output);
        if index == 0 {
            projected.push((expr, output));
            child.push(output);
        }
    }
    builder
        .add_project(
            duplicate,
            initial,
            projected.into_boxed_slice(),
            child.clone().into_boxed_slice(),
        )
        .unwrap();
    let spec = match shape {
        Shape::Global(comparison, desired_rows) => RowCountAssertionSpec::Global {
            subject: "actual scalar query\0λ".into(),
            desired_rows,
            comparison,
        },
        Shape::EmptySubject => RowCountAssertionSpec::Global {
            subject: "".into(),
            desired_rows: 1,
            comparison: RowCountAssertion::Le,
        },
        shape => {
            let count = match shape {
                Shape::Keyed(count) => count,
                Shape::EmptyKeys => 0,
                _ => 4,
            };
            let mut keys = (0..count)
                .map(|index| [child[2], child[0], child[3], child[0]][index % 4])
                .collect::<Vec<_>>();
            if matches!(shape, Shape::ForeignKey) {
                keys[0] = initial_values[0];
            }
            let mut labels = (0..count)
                .map(|index| {
                    if count == 320 && index == 0 {
                        format!("{}λ\0end", "a".repeat(255)).into_boxed_str()
                    } else {
                        format!("label_{index}_λ\0").into_boxed_str()
                    }
                })
                .collect::<Vec<_>>();
            if matches!(shape, Shape::EmptyLabel) {
                labels[1] = "".into();
            }
            if matches!(shape, Shape::WrongLabelCount) {
                labels.pop();
            }
            RowCountAssertionSpec::PerKeyAtMostOne {
                applicability: match shape {
                    Shape::Conditional | Shape::WrongMaskType => {
                        novarocks_physical_plan::AssertionApplicability::WhenTrue(child[3])
                    }
                    Shape::ForeignMask => {
                        novarocks_physical_plan::AssertionApplicability::WhenTrue(initial_values[2])
                    }
                    _ => novarocks_physical_plan::AssertionApplicability::AllRows,
                },
                keys: keys.into_boxed_slice(),
                labels: labels.into_boxed_slice(),
                message: if matches!(shape, Shape::EmptyMessage) {
                    "".into()
                } else {
                    "actual duplicate key: λ\0".into()
                },
            }
        }
    };
    builder
        .add_assert_one_row(assertion, duplicate, spec)
        .unwrap();
    let (root, output) = if downstream {
        let root = NodeId::new(7);
        let output = project(
            &mut builder,
            root,
            assertion,
            vec![
                (ExprKind::Value(child[0]), integer),
                (ExprKind::Value(child[3]), text),
            ],
        );
        (root, output)
    } else {
        (assertion, child)
    };
    let fragment = builder.finish_structure(
        root,
        FragmentSink::Result,
        PipelineDopDomain {
            min: 1,
            max: if matches!(shape, Shape::Conditional) {
                4
            } else {
                1
            },
            requires_power_of_two: false,
        },
        PlanLimits::FROZEN,
        &Control,
        novarocks_physical_plan::StagePlacementPolicy::Singleton,
    )?;
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control)?;
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
    )?;
    let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &Control)?;
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &Control)?;
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
    constants.insert(pool_id, pool)?;
    let package = FragmentPackage::try_new(
        FragmentPackageInput {
            constants,
            version: PlanVersionId::try_new([81; 16]).unwrap(),
            required: RequiredContracts::default(),
            fragment,
            expression_uses: uses,
            calls,
            pruning: FrozenFragmentPruning::try_new(fid, vec![], &Control)?,
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
        assertion,
        selected,
    })
}
fn compile(
    source: &Fixture,
    functions: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<novarocks_local_program::LocalProgram, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let checked =
        validate_fragment_providers(source.package.clone(), &providers, &Control).unwrap();
    compile_fragment(checked, functions, options(), control)
}

#[test]
fn assert_rows_complete_global_keeps_all_six_comparisons_zero_and_max_mandatory_fields() {
    let functions = functions();
    for (physical, expected) in [
        (RowCountAssertion::Eq, RowAssertion::Eq),
        (RowCountAssertion::Ne, RowAssertion::Ne),
        (RowCountAssertion::Lt, RowAssertion::Lt),
        (RowCountAssertion::Le, RowAssertion::Le),
        (RowCountAssertion::Gt, RowAssertion::Gt),
        (RowCountAssertion::Ge, RowAssertion::Ge),
    ] {
        for desired in [0, 1, u64::MAX] {
            let source = fixture(Shape::Global(physical, desired), false).unwrap();
            let result = compile(&source, &functions, &Control);
            if let Ok(desired) = usize::try_from(desired) {
                let program = result.unwrap();
                let node = &program.graph().nodes()[3];
                let ProgramNodeKind::AssertNumRows {
                    input,
                    mode:
                        AssertRowsMode::Global {
                            desired_num_rows,
                            assertion,
                            subquery_string,
                        },
                } = node.kind()
                else {
                    panic!("actual Global assertion");
                };
                assert_eq!(*input, ProgramNodeId::new(2));
                assert_eq!(*desired_num_rows, Some(desired));
                assert_eq!(*assertion, expected);
                assert_eq!(subquery_string.as_deref(), Some("actual scalar query\0λ"));
            } else {
                assert!(matches!(
                    result,
                    Err(FragmentCompileError::Invalid(
                        "assertion row count exceeds host range"
                    ))
                ));
            }
        }
    }
}

#[test]
fn assert_rows_complete_keyed_preserves_order_duplicate_occurrences_full_source_and_provenance() {
    let source = fixture(Shape::Keyed(4), false).unwrap();
    let program = compile(&source, &functions(), &Control).unwrap();
    let graph = program.graph();
    let child = &graph.nodes()[2];
    let node = &graph.nodes()[3];
    let ProgramNodeKind::AssertNumRows {
        input,
        mode:
            AssertRowsMode::PerKeyAtMostOne {
                applicability: novarocks_local_program::ProgramAssertionApplicability::AllRows,
                key_slots,
                key_labels,
                message_prefix,
            },
    } = node.kind()
    else {
        panic!("actual keyed assertion");
    };
    assert_eq!(*input, ProgramNodeId::new(2));
    let slots = child.output_layout().slots();
    assert_ne!(slots[0], slots[1]);
    assert_eq!(key_slots, &vec![slots[2], slots[0], slots[3], slots[0]]);
    assert_eq!(
        key_labels.iter().map(AsRef::as_ref).collect::<Vec<&str>>(),
        vec!["label_0_λ\0", "label_1_λ\0", "label_2_λ\0", "label_3_λ\0"]
    );
    assert_eq!(message_prefix.as_ref(), "actual duplicate key: λ\0");
    assert_eq!(node.output_layout().slots(), slots);
    assert!(Arc::ptr_eq(
        node.output_layout().schema(),
        child.output_layout().schema()
    ));
    let DataType::List(field) = node.output_layout().schema().field(2).data_type() else {
        panic!("original nested source");
    };
    assert_eq!(field.name(), "item");
    assert!(field.metadata().is_empty());
    let source_pool = source
        .package
        .constants()
        .entries()
        .values()
        .next()
        .unwrap();
    let DataType::List(source_field) = source_pool.field_ref().data_type() else {
        panic!("original source List");
    };
    assert_eq!(source_field.name(), "original-child");
    assert_eq!(source_field.metadata().get("source-key").unwrap(), "kept-λ");
    assert!(field.is_nullable());
    for ordinal in 0..4 {
        let site = ProgramChannelSite::Layout {
            node: ProgramNodeId::new(3),
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal,
        };
        let original = source.package.fragment().nodes()[&source.assertion]
            .output
            .columns[ordinal as usize];
        assert!(
            program
                .checked()
                .channels()
                .channel_type(site)
                .unwrap()
                .exactly_equals_observed::<FragmentCompileError>(
                    &source.package.fragment().values()[&original].ty,
                    || Control
                        .checkpoint(CompilePhase::Validate, 0)
                        .map_err(Into::into)
                )
                .unwrap()
        );
    }
    let value = graph
        .expressions()
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            StaticExprKind::Constant(value) if value.ordinal() == 1 => Some(value),
            _ => None,
        })
        .unwrap();
    assert!(Arc::ptr_eq(
        value.pool().field_ref(),
        source.package.constants().entries()[&source.selected.pool].field_ref()
    ));
    assert_eq!(value.ordinal(), 1);
    assert_eq!(graph.root(), ProgramNodeId::new(3));
    assert_eq!(node.physical_sources()[0].get(), 901);
    assert_eq!(graph.nodes()[0].physical_sources()[0].get(), u32::MAX);
    let provenance = program.provenance().get(LocalOperatorId::new(3)).unwrap();
    assert!(matches!(provenance.origin, LocalOperatorOrigin::Direct));
    assert_eq!(provenance.cost_owner, LocalOperatorId::new(3));
    assert_eq!(graph.profile().pipeline_dop().get(), 1);
    assert!(matches!(graph.sink(), Some(StaticSinkProgram::Result)));
    let [novarocks_local_program::BindingRequirement::ResultSink { layout }] =
        graph.requirements().entries()
    else {
        panic!("exact result requirement without a runtime input capability");
    };
    assert_eq!(layout.slots(), slots);
    assert!(Arc::ptr_eq(layout.schema(), node.output_layout().schema()));
    for ordinal in 0..4 {
        assert_eq!(
            node.output_layout().schema().field(ordinal).name(),
            &format!("selected_{ordinal}")
        );
    }
}

#[test]
fn assert_rows_downstream_value_uses_original_passthrough_occurrence_and_keeps_slots() {
    let source = fixture(Shape::Keyed(4), true).unwrap();
    let program = compile(&source, &functions(), &Control).unwrap();
    let graph = program.graph();
    let child = &graph.nodes()[2];
    let assertion = &graph.nodes()[3];
    let ProgramNodeKind::Project { exprs, .. } = graph.nodes()[4].kind() else {
        panic!("actual downstream Project");
    };
    assert_eq!(
        assertion.output_layout().slots(),
        child.output_layout().slots()
    );
    for (expression, ordinal) in exprs.iter().zip([0, 3]) {
        assert!(
            matches!(graph.expressions().node(*expression).unwrap().kind(), StaticExprKind::SlotId(slot)
            if *slot == assertion.output_layout().slots()[ordinal])
        );
        assert!(program.checked().slots().values().any(|source| *source
            == ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                node: ProgramNodeId::new(3),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: ordinal as u32
            })));
    }
}

#[test]
fn assert_rows_original_static_owner_refuses_empty_and_foreign_source_fields() {
    for (shape, expected) in [
        (
            Shape::EmptySubject,
            "global row-count assertion subject is empty",
        ),
        (
            Shape::EmptyKeys,
            "keyed row-count assertion requires matching non-empty keys and labels",
        ),
        (
            Shape::WrongLabelCount,
            "keyed row-count assertion requires matching non-empty keys and labels",
        ),
        (
            Shape::EmptyLabel,
            "keyed row-count assertion labels and message must be non-empty",
        ),
        (
            Shape::EmptyMessage,
            "keyed row-count assertion labels and message must be non-empty",
        ),
        (
            Shape::ForeignKey,
            "keyed row-count assertion key is absent from its exact child port",
        ),
    ] {
        let error = match fixture(shape, false) {
            Ok(_) => panic!("invalid authored source"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains(expected),
            "original exact rejection: {error}"
        );
        assert!(
            error
                .downcast_ref::<novarocks_physical_plan::FragmentStructureError>()
                .is_some()
                || error
                    .downcast_ref::<novarocks_physical_plan::FragmentPackageError>()
                    .is_some()
        );
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
fn assert_rows_complete_compiler_preserves_every_small_control_prefix_and_ordinary_tail() {
    let source = fixture(Shape::Keyed(4), false).unwrap();
    let functions = functions();
    for invalid_dop in [false, true] {
        let invoke = |control: &dyn PureCompileControl| {
            if invalid_dop {
                let providers =
                    PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control)
                        .unwrap();
                let checked =
                    validate_fragment_providers(source.package.clone(), &providers, &Control)
                        .unwrap();
                let mut options = options();
                options.pipeline_dop = NonZeroUsize::new(2).unwrap();
                compile_fragment(checked, &functions, options, control)
            } else {
                compile(&source, &functions, control)
            }
        };
        let trace = Trace::new(None, CompileControlError::Cancelled);
        let result = invoke(&trace);
        assert_eq!(result.is_err(), invalid_dop);
        let expected = trace.events.into_inner().unwrap();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..expected.len() {
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
fn assert_rows_wide_real_keys_and_multibyte_label_copy_cross_quantum_without_replay() {
    let source = fixture(Shape::Keyed(320), false).unwrap();
    let functions = functions();
    let trace = Trace::new(None, CompileControlError::Cancelled);
    let program = compile(&source, &functions, &trace).unwrap();
    let graph = program.graph();
    let ProgramNodeKind::AssertNumRows {
        mode:
            AssertRowsMode::PerKeyAtMostOne {
                key_slots,
                key_labels,
                ..
            },
        ..
    } = graph.nodes()[3].kind()
    else {
        panic!("keyed");
    };
    let slots = graph.nodes()[2].output_layout().slots();
    assert_eq!(key_slots.len(), 320);
    for index in [0, 1, 255, 256, 319] {
        assert_eq!(
            key_slots[index],
            [slots[2], slots[0], slots[3], slots[0]][index % 4]
        );
        let expected_label = if index == 0 {
            format!("{}λ\0end", "a".repeat(255))
        } else {
            format!("label_{index}_λ\0")
        };
        assert_eq!(key_labels[index].as_ref(), expected_label);
    }
    let expected = trace.events.into_inner().unwrap();
    let quantum = expected
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("real keys cross quantum");
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [0, quantum, expected.len() - 1] {
            let control = Trace::new(Some(at), cause.clone());
            assert!(
                matches!(compile(&source, &functions, &control), Err(FragmentCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
        }
    }
}

#[test]
fn assert_rows_leaf_ordinary_shape_tail_and_unrepresentable_reservation_keep_original_primary() {
    let source = fixture(Shape::Global(RowCountAssertion::Le, 1), false).unwrap();
    let program = compile(&source, &functions(), &Control).unwrap();
    let node = &source.package.fragment().nodes()[&source.assertion];
    let layout = program.graph().nodes()[2].output_layout();
    let invoke = |control: &dyn PureCompileControl| {
        crate::assert_rows::lower_assert_rows(
            node,
            ProgramNodeId::new(2),
            layout,
            &[SlotId::new(0)],
            novarocks_local_program::ProgramAssertionApplicability::AllRows,
            control,
        )
    };
    let trace = Trace::new(None, CompileControlError::Cancelled);
    assert!(matches!(
        invoke(&trace),
        Err(FragmentCompileError::Invalid(
            "global assertion source fields differ"
        ))
    ));
    let expected = trace.events.into_inner().unwrap();
    assert_eq!(
        expected,
        vec![
            (CompilePhase::LowerProgram, 0),
            (CompilePhase::LowerProgram, 1)
        ]
    );
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..expected.len() {
            let trace = Trace::new(Some(at), cause.clone());
            assert!(
                matches!(invoke(&trace), Err(FragmentCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(*trace.events.lock().unwrap(), expected[..=at]);
        }
    }
    let trace = Trace::new(None, CompileControlError::Cancelled);
    let mut work = CompileCheckpoints::try_new(&trace, CompilePhase::LowerProgram).unwrap();
    let mut output = Vec::<u64>::new();
    assert_eq!(
        crate::assert_rows::reserve_vec(&mut output, usize::MAX, &mut work),
        Err(CompileControlError::ResourceExhausted)
    );
    assert!(output.is_empty());
    assert_eq!(
        *trace.events.lock().unwrap(),
        vec![(CompilePhase::LowerProgram, 0)]
    );
    // The individual request Layout is legal, but adding it to a nonempty Vec
    // exceeds the actual RawVec capacity bound. This exercises try_reserve's
    // own refusal without requesting a large allocation from the allocator.
    let trace = Trace::new(None, CompileControlError::Cancelled);
    let mut work = CompileCheckpoints::try_new(&trace, CompilePhase::LowerProgram).unwrap();
    let mut output = vec![7_u64];
    work.step().unwrap();
    let additional = isize::MAX as usize / std::mem::size_of::<u64>();
    std::alloc::Layout::array::<u64>(additional).unwrap();
    assert_eq!(
        crate::assert_rows::reserve_vec(&mut output, additional, &mut work),
        Err(CompileControlError::ResourceExhausted)
    );
    assert_eq!(output, vec![7]);
    assert_eq!(
        *trace.events.lock().unwrap(),
        vec![
            (CompilePhase::LowerProgram, 0),
            (CompilePhase::LowerProgram, 1)
        ]
    );
}

#[test]
fn conditional_assertion_keeps_the_exact_nullable_child_mask_at_parallel_width() {
    let source = fixture(Shape::Conditional, false).unwrap();
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let checked =
        validate_fragment_providers(source.package.clone(), &providers, &Control).unwrap();
    let mut selected = options();
    selected.pipeline_dop = NonZeroUsize::new(4).unwrap();
    let program = compile_fragment(checked, &functions(), selected, &Control)
        .expect("keyed assertion admits the exact parallel mask");
    assert_eq!(program.graph().profile().pipeline_dop().get(), 4);
    let assertion = &program.graph().nodes()[program.graph().root().index()];
    let ProgramNodeKind::AssertNumRows {
        input,
        mode:
            AssertRowsMode::PerKeyAtMostOne {
                applicability,
                key_slots,
                ..
            },
    } = assertion.kind()
    else {
        panic!("keyed assertion")
    };
    let child = program.graph().nodes()[input.index()].output_layout();
    assert_eq!(
        *applicability,
        novarocks_local_program::ProgramAssertionApplicability::WhenTrue(child.slots()[3])
    );
    assert_eq!(child.schema().field(3).data_type(), &DataType::Boolean);
    assert!(child.schema().field(3).is_nullable());
    assert_eq!(
        key_slots,
        &vec![
            child.slots()[2],
            child.slots()[0],
            child.slots()[3],
            child.slots()[0]
        ]
    );
    for shape in [Shape::ForeignMask, Shape::WrongMaskType] {
        assert!(
            fixture(shape, false).is_err(),
            "same-shaped foreign or non-Boolean masks never become a local program"
        );
    }
}
