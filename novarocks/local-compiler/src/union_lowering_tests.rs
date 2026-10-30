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
use arrow_schema::{DataType, Field};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
    InstalledPureKernel, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi,
};
use novarocks_local_program::{
    KernelAbiVersion, LocalOperatorId, LocalOperatorOrigin, ProgramChannelLayoutRole,
    ProgramChannelSite, ProgramExpressionArena, ProgramExpressionRootSite, ProgramLexicalSource,
    ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind, ProgramUseRef, StaticExprKind,
    StaticSinkProgram,
};
use novarocks_physical_plan::{
    BuildError, Distribution, ExprKind, FragmentBuilder, FragmentCuts, FragmentId, FragmentPackage,
    FragmentPackageAdmission, FragmentPackageInput, FragmentSink, FrozenFragmentCalls,
    FrozenFragmentPruning, LiteralValue, NodeId, PhysicalExpressionRoots, PhysicalRootUses,
    PipelineDopDomain, PlanLimits, PlanVersionId, PropertyProofProjectionLimits, RequiredContracts,
    RequiredInputs, ResultField, ResultPort, SetOperationKind, ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionArgumentType, FunctionValueType, PureCompileControl, SemanticParameters,
};
use std::{
    collections::{BTreeMap, BTreeSet},
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
struct Case {
    width: usize,
    project_branches: bool,
    downstream: bool,
    right_nullable: bool,
    nested: bool,
    shared: bool,
    kind: SetOperationKind,
}
impl Case {
    fn ordinary() -> Self {
        Self {
            width: 3,
            project_branches: false,
            downstream: false,
            right_nullable: false,
            nested: false,
            shared: false,
            kind: SetOperationKind::UnionAll,
        }
    }
}
struct Fixture {
    package: Arc<FragmentPackage>,
    union: NodeId,
    branch_nodes: [NodeId; 2],
    branch_values: [Vec<ValueId>; 2],
    downstream: Option<NodeId>,
}

fn fixture(case: Case) -> Fixture {
    let fragment_id = FragmentId::new(73);
    let left = NodeId::new(u32::MAX);
    let right = NodeId::new(0);
    let union = NodeId::new(41);
    let mut builder = FragmentBuilder::new(fragment_id);
    let nested_field = Arc::new(
        Field::new("original-nested", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "owner".to_owned(),
                "kept".to_owned(),
            )]),
        ),
    );
    let mut branch_nodes = [left, right];
    let mut branch_values: [Vec<ValueId>; 2] = [vec![], vec![]];
    for branch in 0..if case.shared { 1 } else { 2 } {
        let node = branch_nodes[branch];
        let mut cells = Vec::new();
        for column in 0..2 {
            let ty = if case.nested {
                FunctionValueType::new(DataType::Struct(vec![nested_field.clone()].into()), true)
            } else {
                FunctionValueType::new(DataType::Int64, branch == 1 && case.right_nullable)
            };
            let literal = if case.nested || (branch == 1 && case.right_nullable && column == 0) {
                LiteralValue::Null
            } else {
                LiteralValue::Int64([[11, 22], [33, 44]][branch][column])
            };
            let expression = builder
                .add_expression(node, ty.clone(), ExprKind::Literal(literal))
                .unwrap();
            // Explicit sparse value identities are authored by the real builder.
            let value = ValueId::new([[0, 7], [900, 901]][branch][column]);
            builder
                .insert_value(ValueDef {
                    id: value,
                    ty,
                    origin: ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: column as u32,
                    },
                })
                .unwrap();
            branch_values[branch].push(value);
            cells.push(expression);
        }
        builder
            .add_values(
                node,
                Box::from([cells.into_boxed_slice()]),
                branch_values[branch].clone().into_boxed_slice(),
            )
            .unwrap();
        if case.project_branches {
            let projected = NodeId::new([19, 2][branch]);
            let mut expressions = Vec::new();
            let mut outputs = Vec::new();
            for value in &branch_values[branch] {
                let ty = if case.nested {
                    FunctionValueType::new(
                        DataType::Struct(vec![nested_field.clone()].into()),
                        true,
                    )
                } else {
                    FunctionValueType::new(DataType::Int64, branch == 1 && case.right_nullable)
                };
                let expr = builder
                    .add_expression(projected, ty.clone(), ExprKind::Value(*value))
                    .unwrap();
                let output = builder
                    .add_value(
                        ty,
                        ValueOrigin::Expr {
                            node: projected,
                            expr,
                        },
                    )
                    .unwrap();
                expressions.push((expr, output));
                outputs.push(output);
            }
            builder
                .add_project(
                    projected,
                    node,
                    expressions.into_boxed_slice(),
                    outputs.clone().into_boxed_slice(),
                )
                .unwrap();
            branch_values[branch] = outputs;
            branch_nodes[branch] = projected;
        }
    }
    if case.shared {
        branch_nodes[1] = branch_nodes[0];
        branch_values[1] = branch_values[0].clone();
    }
    let target = if case.nested {
        FunctionValueType::new(DataType::Struct(vec![nested_field].into()), true)
    } else {
        FunctionValueType::new(DataType::Int64, case.right_nullable)
    };
    let outputs = (0..case.width)
        .map(|ordinal| {
            builder
                .add_value(
                    target.clone(),
                    ValueOrigin::NodeOutput {
                        node: union,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    // The independent oracle is [b,a,b] / [d,c,d], repeated for wide rows.
    let mappings = branch_values
        .iter()
        .map(|values| {
            (0..case.width)
                .map(|ordinal| values[if ordinal % 3 == 1 { 0 } else { 1 }])
                .collect::<Vec<_>>()
                .into_boxed_slice()
        })
        .collect::<Vec<_>>();
    builder
        .add_row_consuming(
            union,
            branch_nodes.into(),
            RequiredInputs::Singleton,
            Distribution::Singleton,
            outputs.clone().into_boxed_slice(),
            novarocks_physical_plan::NodeKind::SetOp {
                kind: case.kind,
                input_mappings: mappings.into_boxed_slice(),
            },
        )
        .unwrap();
    let (root, result_values, downstream) = if case.downstream {
        assert_eq!(case.width, 3);
        let project = NodeId::new(17);
        let first = builder
            .add_expression(project, target.clone(), ExprKind::Value(outputs[2]))
            .unwrap();
        let second = builder
            .add_expression(project, target.clone(), ExprKind::Value(outputs[0]))
            .unwrap();
        let a = builder
            .add_value(
                target.clone(),
                ValueOrigin::Expr {
                    node: project,
                    expr: first,
                },
            )
            .unwrap();
        let b = builder
            .add_value(
                target.clone(),
                ValueOrigin::Expr {
                    node: project,
                    expr: second,
                },
            )
            .unwrap();
        let values = vec![a, b, a];
        builder
            .add_project(
                project,
                union,
                Box::from([(first, a), (second, b), (first, a)]),
                values.clone().into_boxed_slice(),
            )
            .unwrap();
        let filter = NodeId::new(18);
        let predicate = builder
            .add_expression(
                filter,
                FunctionValueType::new(DataType::Boolean, false),
                ExprKind::Literal(LiteralValue::Boolean(true)),
            )
            .unwrap();
        builder
            .add_filter(filter, project, Box::from([predicate]))
            .unwrap();
        let limit = NodeId::new(20);
        builder.add_limit(limit, filter, Some(7), 2).unwrap();
        (limit, values, Some(project))
    } else {
        (union, outputs, None)
    };
    let package = finish_package(builder, fragment_id, root, &result_values);
    Fixture {
        package,
        union,
        branch_nodes,
        branch_values,
        downstream,
    }
}

fn finish_package(
    builder: FragmentBuilder,
    fragment_id: FragmentId,
    root: NodeId,
    result_values: &[ValueId],
) -> Arc<FragmentPackage> {
    let fragment = builder
        .finish_structure(
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
        )
        .unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).unwrap();
    let domain = EvaluationDomainId::new(u32::MAX);
    let mut invocations = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let use_id = ExpressionUseId::new(match ordinal {
            0 => u32::MAX,
            1 => 0,
            n => n as u32 + 7,
        });
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
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &Control).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &Control).unwrap();
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
            pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &Control).unwrap(),
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
    Arc::new(package)
}

fn compile(
    fixture: &Fixture,
    functions: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<novarocks_local_program::LocalProgram, FragmentCompileError> {
    compile_package(&fixture.package, functions, control)
}

fn compile_package(
    package: &Arc<FragmentPackage>,
    functions: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<novarocks_local_program::LocalProgram, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let validated = validate_fragment_providers(package.clone(), &providers, &Control).unwrap();
    compile_fragment(validated, functions, options(), control)
}

fn union_node(
    program: &novarocks_local_program::LocalProgram,
) -> &novarocks_local_program::ProgramNode {
    program
        .graph()
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), ProgramNodeKind::UnionAll { .. }))
        .unwrap()
}
fn physical_local(
    program: &novarocks_local_program::LocalProgram,
    physical: NodeId,
) -> ProgramNodeId {
    program
        .graph()
        .nodes()
        .iter()
        .find(|node| {
            node.physical_sources()
                .iter()
                .any(|source| source.get() == physical.get())
        })
        .unwrap()
        .local_id()
        .unwrap()
}
fn assert_mapping(
    program: &novarocks_local_program::LocalProgram,
    fixture: &Fixture,
    width: usize,
) {
    let node = union_node(program);
    let ProgramNodeKind::UnionAll { inputs } = node.kind() else {
        unreachable!()
    };
    assert_eq!(inputs.len(), 2);
    for (branch, &normalizer) in inputs.iter().enumerate() {
        let project = &program.graph().nodes()[normalizer.index()];
        let ProgramNodeKind::Project {
            input,
            is_subordinate,
            exprs,
            expr_slot_ids,
            output_indices,
            ..
        } = project.kind()
        else {
            panic!("actual normalization Project")
        };
        assert!(*is_subordinate);
        assert_eq!(
            *input,
            physical_local(program, fixture.branch_nodes[branch])
        );
        assert!(output_indices.is_none());
        assert_eq!(expr_slot_ids, node.output_layout().slots());
        assert_eq!(
            project.output_layout().schema(),
            node.output_layout().schema()
        );
        assert_eq!(exprs.len(), width);
        let source_slots = program.graph().nodes()[input.index()]
            .output_layout()
            .slots();
        for (ordinal, &expr) in exprs.iter().enumerate() {
            let expected = if ordinal % 3 == 1 { 0 } else { 1 };
            assert!(
                matches!(program.graph().expressions().node(expr).unwrap().kind(),
                StaticExprKind::SlotId(slot) if *slot==source_slots[expected])
            );
        }
        if width >= 3 {
            assert_ne!(exprs[0], exprs[2]);
        }
    }
}

#[test]
fn union_all_reordered_sparse_sources_use_actual_branch_ordinals() {
    let source = fixture(Case::ordinary());
    assert_eq!(
        source.branch_values[0],
        vec![ValueId::new(0), ValueId::new(7)]
    );
    assert_eq!(
        source.branch_values[1],
        vec![ValueId::new(900), ValueId::new(901)]
    );
    let program = compile(&source, &functions(), &Control).unwrap();
    assert_eq!(program.graph().nodes().len(), 5);
    assert_eq!(program.graph().root(), ProgramNodeId::new(4));
    assert_mapping(&program, &source, 3);
    assert!(matches!(
        program.graph().sink(),
        Some(StaticSinkProgram::Result)
    ));
    assert_eq!(program.graph().profile().pipeline_dop().get(), 1);
    // The actual public builder does not admit ValueId::MAX, so this leaf
    // does not fabricate a MAX value namespace or claim it compiled.
    let mut builder = FragmentBuilder::new(FragmentId::new(7));
    assert!(matches!(
        builder.insert_value(ValueDef {
            id: ValueId::new(u32::MAX),
            ty: FunctionValueType::new(DataType::Int64, false),
            origin: ValueOrigin::NodeOutput {
                node: NodeId::new(0),
                output_ordinal: 0
            }
        }),
        Err(BuildError::IdentitySpaceExhausted("value"))
    ));
}

#[test]
fn union_all_branch_projects_and_downstream_aliases_keep_sources() {
    let source = fixture(Case {
        project_branches: true,
        downstream: true,
        ..Case::ordinary()
    });
    let program = compile(&source, &functions(), &Control).unwrap();
    assert_eq!(program.graph().nodes().len(), 10);
    assert_mapping(&program, &source, 3);
    let projected =
        &program.graph().nodes()[physical_local(&program, source.downstream.unwrap()).index()];
    let ProgramNodeKind::Project { exprs, .. } = projected.kind() else {
        panic!("downstream Project")
    };
    let slots = union_node(&program).output_layout().slots();
    for (&expr, ordinal) in exprs.iter().zip([2, 0, 2]) {
        assert!(
            matches!(program.graph().expressions().node(expr).unwrap().kind(),StaticExprKind::SlotId(slot) if *slot==slots[ordinal])
        );
    }
    assert_eq!(exprs[0], exprs[2]);
    assert_ne!(
        projected.output_layout().slots()[0],
        projected.output_layout().slots()[2]
    );
    assert_eq!(
        program.graph().nodes()[program.graph().root().index()]
            .output_layout()
            .slots(),
        projected.output_layout().slots()
    );
    let ProgramNodeKind::Limit { limit, offset, .. } =
        program.graph().nodes()[program.graph().root().index()].kind()
    else {
        panic!("actual limit")
    };
    assert_eq!((*limit, *offset), (Some(7), 2));
}

#[test]
fn union_all_root_nullable_widening_preserves_source_full_types_and_nested_fields() {
    for case in [
        Case {
            right_nullable: true,
            ..Case::ordinary()
        },
        Case {
            nested: true,
            ..Case::ordinary()
        },
    ] {
        let source = fixture(case);
        let program = compile(&source, &functions(), &Control).unwrap();
        assert_mapping(&program, &source, 3);
        let ProgramNodeKind::UnionAll { inputs } = union_node(&program).kind() else {
            unreachable!()
        };
        for (branch, &normalizer) in inputs.iter().enumerate() {
            let ProgramNodeKind::Project { exprs, .. } =
                program.graph().nodes()[normalizer.index()].kind()
            else {
                unreachable!()
            };
            for (ordinal, &expr) in exprs.iter().enumerate() {
                let source_value =
                    source.branch_values[branch][if ordinal % 3 == 1 { 0 } else { 1 }];
                let original = &source.package.fragment().values()[&source_value].ty;
                assert_eq!(
                    program
                        .checked()
                        .channels()
                        .expressions()
                        .definition_type(ProgramExpressionArena::Main, expr),
                    Some(&FunctionArgumentType::Value(original.clone()))
                );
                if case.nested {
                    let DataType::Struct(fields) = &original.data_type() else {
                        panic!("source Struct")
                    };
                    assert_eq!(fields[0].name(), "original-nested");
                    assert!(fields[0].metadata().is_empty());
                    let FunctionArgumentType::Value(actual) = program
                        .checked()
                        .channels()
                        .expressions()
                        .definition_type(ProgramExpressionArena::Main, expr)
                        .unwrap()
                    else {
                        unreachable!()
                    };
                    let DataType::Struct(actual_fields) = &actual.data_type() else {
                        unreachable!()
                    };
                    assert!(Arc::ptr_eq(&actual_fields[0], &fields[0]));
                } else {
                    assert_eq!(original.nullable(), branch == 1);
                }
                assert!(
                    program.graph().nodes()[normalizer.index()]
                        .output_layout()
                        .schema()
                        .field(ordinal)
                        .is_nullable()
                );
            }
        }
    }
}

#[test]
fn union_all_split_provenance_and_fresh_root_lexical_coverage_are_complete() {
    let source = fixture(Case {
        project_branches: true,
        ..Case::ordinary()
    });
    let program = compile(&source, &functions(), &Control).unwrap();
    let union = union_node(&program);
    let owner = LocalOperatorId::new(union.local_id().unwrap().index() as u32);
    let pieces = program
        .provenance()
        .operators()
        .values()
        .filter(|p| p.sources.iter().any(|id| id.get() == source.union.get()))
        .collect::<Vec<_>>();
    assert_eq!(pieces.len(), 3);
    assert_eq!(
        pieces.iter().map(|p| p.cost_owner).collect::<BTreeSet<_>>(),
        BTreeSet::from([owner])
    );
    assert_eq!(
        pieces
            .iter()
            .map(|p| match p.origin {
                LocalOperatorOrigin::Split { piece } => piece,
                _ => panic!("Union pieces must be Split"),
            })
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 1, 2])
    );
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    assert!(
        program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .calls()
            .is_empty()
    );
    let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
    let ProgramNodeKind::UnionAll { inputs } = union.kind() else {
        unreachable!()
    };
    let mut new_uses = BTreeSet::new();
    let mut new_domains = BTreeSet::new();
    for &normalizer in inputs {
        let ProgramNodeKind::Project { exprs, input, .. } =
            program.graph().nodes()[normalizer.index()].kind()
        else {
            unreachable!()
        };
        for (ordinal, &definition) in exprs.iter().enumerate() {
            let root = ProgramExpressionRootSite::Node {
                node: normalizer,
                role: ProgramNodeExpressionRole::ProjectOutput {
                    expression: ordinal as u32,
                },
            };
            let use_id = snapshot.bindings()[&root];
            let invocation = &flow.uses()[&use_id];
            assert_eq!(invocation.definition, definition);
            assert_eq!(invocation.control, ControlShape::Eager);
            assert_eq!(
                invocation.context.demand,
                novarocks_type_contract::EvaluationDemand::Value
            );
            assert!(invocation.arguments.is_empty());
            assert!(new_uses.insert(use_id));
            assert!(new_domains.insert(invocation.context.domain));
            let domain = &flow.domains()[&invocation.context.domain];
            assert!(domain.parent.is_none() && domain.guard.is_none());
            assert_eq!(
                program.checked().slots()[&ProgramUseRef {
                    arena: ProgramExpressionArena::Main,
                    use_id
                }],
                ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                    node: *input,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal: if ordinal % 3 == 1 { 0 } else { 1 }
                })
            );
        }
    }
    assert_eq!(new_uses.len(), 6);
    assert_eq!(new_domains.len(), 6);
    // The original branch reads survive with their original sparse context.
    for (site, &original_use) in source.package.expression_uses().bindings() {
        if matches!(
            site.role,
            novarocks_physical_plan::ExpressionRootRole::ProjectOutput { .. }
        ) {
            assert!(flow.uses().contains_key(&original_use));
            assert!(!new_uses.contains(&original_use));
            assert_eq!(
                flow.uses()[&original_use].context,
                source.package.expression_uses().flow().uses()[&original_use].context
            );
        }
    }
}

#[test]
fn union_all_shared_input_and_other_set_operations_remain_explicitly_unsupported() {
    for case in [
        Case {
            shared: true,
            ..Case::ordinary()
        },
        Case {
            kind: SetOperationKind::Intersect,
            ..Case::ordinary()
        },
        Case {
            kind: SetOperationKind::Except,
            ..Case::ordinary()
        },
    ] {
        let source = fixture(case);
        assert!(matches!(
            compile(&source, &functions(), &Control),
            Err(FragmentCompileError::Unsupported { .. })
        ));
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
            "callback after original refusal"
        );
        assert!(units <= 256);
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
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}

#[test]
fn union_all_every_small_complete_compiler_control_prefix_and_ordinary_tail() {
    let functions = functions();
    for shared in [false, true] {
        let source = fixture(Case {
            shared,
            ..Case::ordinary()
        });
        let baseline = Trace::new(None, CompileControlError::Cancelled);
        let outcome = compile(&source, &functions, &baseline);
        if shared {
            assert!(matches!(
                outcome,
                Err(FragmentCompileError::Unsupported { .. })
            ));
        } else {
            outcome.unwrap();
        }
        let expected = baseline.events.into_inner().unwrap();
        assert!(!expected.is_empty());
        if shared {
            assert!(
                expected.last().unwrap().1 > 0,
                "ordinary completed source traversal tail"
            );
        }
        for cause in causes() {
            for at in 0..expected.len() {
                let control = Trace::new(Some(at), cause.clone());
                assert!(
                    matches!(compile(&source,&functions,&control),Err(FragmentCompileError::Control(actual)) if actual==cause)
                );
                assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
            }
        }
    }
}

#[test]
fn union_all_wide_real_mapping_work_keeps_order_and_quantum_prefix() {
    let source = fixture(Case {
        width: 320,
        ..Case::ordinary()
    });
    let functions = functions();
    let baseline = Trace::new(None, CompileControlError::Cancelled);
    let program = compile(&source, &functions, &baseline).unwrap();
    assert_mapping(&program, &source, 320);
    let expected = baseline.events.into_inner().unwrap();
    let quantum = expected
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("real source/mapping/flow work quantum");
    for cause in causes() {
        for at in [0, quantum, expected.len() - 1] {
            let control = Trace::new(Some(at), cause.clone());
            assert!(
                matches!(compile(&source,&functions,&control),Err(FragmentCompileError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
        }
    }
}

fn nested_union_package(layers: usize) -> (Arc<FragmentPackage>, Vec<NodeId>) {
    let fragment_id = FragmentId::new(97);
    let mut builder = FragmentBuilder::new(fragment_id);
    let integer = FunctionValueType::new(DataType::Int64, false);
    let add_values = |builder: &mut FragmentBuilder, node: NodeId, numbers: [i64; 2]| {
        let mut cells = Vec::new();
        let mut values = Vec::new();
        for (ordinal, number) in numbers.into_iter().enumerate() {
            cells.push(
                builder
                    .add_expression(
                        node,
                        integer.clone(),
                        ExprKind::Literal(LiteralValue::Int64(number)),
                    )
                    .unwrap(),
            );
            values.push(
                builder
                    .add_value(
                        integer.clone(),
                        ValueOrigin::NodeOutput {
                            node,
                            output_ordinal: ordinal as u32,
                        },
                    )
                    .unwrap(),
            );
        }
        builder
            .add_values(
                node,
                Box::from([cells.into_boxed_slice()]),
                values.clone().into_boxed_slice(),
            )
            .unwrap();
        values
    };
    let mut root = NodeId::new(u32::MAX);
    let mut previous = add_values(&mut builder, root, [101, 202]);
    let mut unions = Vec::new();
    for layer in 0..layers {
        let sibling = NodeId::new(1000 + 2 * layer as u32);
        let sibling_values = add_values(
            &mut builder,
            sibling,
            [300 + layer as i64, 400 + layer as i64],
        );
        let union = NodeId::new(1001 + 2 * layer as u32);
        let output = (0..2)
            .map(|ordinal| {
                builder
                    .add_value(
                        integer.clone(),
                        ValueOrigin::NodeOutput {
                            node: union,
                            output_ordinal: ordinal,
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        // Each level has two distinct execution children and explicit [1,0]
        // reads from both. The preceding union is not shared by a sibling.
        builder
            .add_row_consuming(
                union,
                Box::from([root, sibling]),
                RequiredInputs::Singleton,
                Distribution::Singleton,
                output.clone().into_boxed_slice(),
                novarocks_physical_plan::NodeKind::SetOp {
                    kind: SetOperationKind::UnionAll,
                    input_mappings: Box::from([
                        Box::from([previous[1], previous[0]]),
                        Box::from([sibling_values[1], sibling_values[0]]),
                    ]),
                },
            )
            .unwrap();
        root = union;
        previous = output;
        unions.push(union);
    }
    (
        finish_package(builder, fragment_id, root, &previous),
        unions,
    )
}

#[test]
fn union_all_nested_actual_tree_accepts_depth_sixty_three_and_refuses_sixty_five() {
    let functions = functions();
    let (source, original_unions) = nested_union_package(31);
    assert_eq!(source.fragment().nodes().len(), 63);
    assert_eq!(original_unions.len(), 31);
    let program = compile_package(&source, &functions, &Control).unwrap();
    assert_eq!(program.graph().nodes().len(), 125);
    // Compute depth from actual emitted nodes. Every original union contributes
    // its own Project input edge and Union edge; source IDs are not depth.
    let mut depths = Vec::<usize>::new();
    for node in program.graph().nodes() {
        let depth = match node.kind() {
            ProgramNodeKind::Values { .. } => 1,
            ProgramNodeKind::Project { input, .. } => depths[input.index()] + 1,
            ProgramNodeKind::UnionAll { inputs } => inputs
                .iter()
                .map(|input| depths[input.index()] + 1)
                .max()
                .unwrap(),
            _ => panic!("unexpected node in the genuine nested source"),
        };
        depths.push(depth);
    }
    assert_eq!(depths[program.graph().root().index()], 63);
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
    assert_eq!(snapshot.bindings().len(), 31 * 4);
    assert_eq!(program.checked().slots().len(), 31 * 4);
    assert!(
        program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .calls()
            .is_empty()
    );
    let mut all_uses = BTreeSet::new();
    let mut cost_owners = BTreeSet::new();
    for source_union in original_unions {
        let local_union = program
            .graph()
            .nodes()
            .iter()
            .find(|node| {
                matches!(node.kind(), ProgramNodeKind::UnionAll { .. })
                    && node.physical_sources()[0].get() == source_union.get()
            })
            .unwrap();
        let owner = LocalOperatorId::new(local_union.local_id().unwrap().index() as u32);
        assert!(cost_owners.insert(owner));
        let group = program
            .provenance()
            .operators()
            .values()
            .filter(|piece| {
                piece
                    .sources
                    .iter()
                    .any(|id| id.get() == source_union.get())
            })
            .collect::<Vec<_>>();
        assert_eq!(group.len(), 3);
        assert!(group.iter().all(|piece| piece.cost_owner == owner));
        assert_eq!(
            group
                .iter()
                .map(|piece| match piece.origin {
                    LocalOperatorOrigin::Split { piece } => piece,
                    _ => panic!("each nested union owns distinct Split pieces"),
                })
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([0, 1, 2])
        );
        let ProgramNodeKind::UnionAll { inputs } = local_union.kind() else {
            unreachable!()
        };
        for &normalizer in inputs {
            let node = &program.graph().nodes()[normalizer.index()];
            let ProgramNodeKind::Project {
                input,
                exprs,
                is_subordinate,
                ..
            } = node.kind()
            else {
                unreachable!()
            };
            assert!(*is_subordinate);
            assert_eq!(
                node.output_layout().slots(),
                local_union.output_layout().slots()
            );
            let child_slots = program.graph().nodes()[input.index()]
                .output_layout()
                .slots();
            for (ordinal, &definition) in exprs.iter().enumerate() {
                assert!(
                    matches!(program.graph().expressions().node(definition).unwrap().kind(),
                    StaticExprKind::SlotId(slot) if *slot == child_slots[1-ordinal])
                );
                assert_eq!(
                    program
                        .checked()
                        .channels()
                        .expressions()
                        .definition_type(ProgramExpressionArena::Main, definition),
                    Some(&FunctionArgumentType::Value(FunctionValueType::new(
                        DataType::Int64,
                        false
                    )))
                );
                let site = ProgramExpressionRootSite::Node {
                    node: normalizer,
                    role: ProgramNodeExpressionRole::ProjectOutput {
                        expression: ordinal as u32,
                    },
                };
                let use_id = snapshot.bindings()[&site];
                assert!(all_uses.insert(use_id));
                assert_eq!(flow.uses()[&use_id].definition, definition);
                assert_eq!(
                    program.checked().slots()[&ProgramUseRef {
                        arena: ProgramExpressionArena::Main,
                        use_id
                    }],
                    ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                        node: *input,
                        role: ProgramChannelLayoutRole::NodeOutput,
                        ordinal: (1 - ordinal) as u32
                    })
                );
            }
        }
    }
    assert_eq!(cost_owners.len(), 31);
    assert_eq!(all_uses.len(), 124);
    // Each physical Values and each physical Union retains one actual cost
    // owner despite the three local pieces per union.
    assert_eq!(program.provenance().cost_owners().count(), 63);

    let (over_source, over_unions) = nested_union_package(32);
    assert_eq!(over_source.fragment().nodes().len(), 65);
    assert_eq!(over_unions.len(), 32);
    // The original full Package admits physical depth 33. Only the compiler's
    // actual 65-layer expanded local tree exceeds its existing depth-64 gate.
    assert!(matches!(
        compile_package(&over_source, &functions, &Control),
        Err(FragmentCompileError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
}
