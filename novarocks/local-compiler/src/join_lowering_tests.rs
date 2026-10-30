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

//! Join lowering: orientation normalization with kind mirroring, per-use
//! lexical sources, the canonical join output with its selection Project,
//! and explicit refusals. Fixtures are authored with the real physical
//! builder and published as checked packages.

use super::*;
use arrow_schema::DataType;
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
    InstalledPureKernel, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi,
};
use novarocks_local_program::{
    JoinType, KernelAbiVersion, LocalOperatorOrigin, LocalProgram, NestedLoopJoinType,
    ProgramChannelLayoutRole, ProgramChannelSite, ProgramExpressionArena,
    ProgramExpressionRootSite, ProgramLexicalSource, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind, ProgramUseRef, StaticExprKind,
};
use novarocks_physical_plan::{
    BinaryOperator, Distribution, ExprId, ExprKind, Fragment, FragmentBuilder, FragmentCuts,
    FragmentId, FragmentPackage, FragmentPackageAdmission, FragmentPackageInput, FragmentSink,
    FrozenFragmentCalls, FrozenFragmentPruning, JoinDistribution, JoinKey, JoinKind, JoinSide,
    LiteralValue, NestLoopJoinDistribution, NodeId, NodeKind, PhysicalExpressionRoots,
    PhysicalProperties, PhysicalRootUses, PipelineDopDomain, PlanLimits, PlanVersionId,
    PropertyProofProjectionLimits, RequiredContracts, ResultField, ResultPort, RowMultiplicity,
    ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDomainId,
    ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, FunctionValueType, PureCompileControl,
    SemanticParameters, control_argument_semantics,
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
    // An actual unused owner; an empty sealed catalogue is refused.
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

fn int64() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}

fn int64_nullable() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, true)
}

const LEFT: NodeId = NodeId::new(10);
const RIGHT: NodeId = NodeId::new(20);
const JOIN: NodeId = NodeId::new(30);

#[derive(Clone, Copy, Debug)]
enum Family {
    Hash {
        kind: JoinKind,
        build_side: JoinSide,
        null_safe: bool,
    },
    NestLoop {
        kind: JoinKind,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Residual {
    None,
    /// `left.v < right.w`.
    Separate,
    /// `<left key definition> < right.w`: one definition shared by the probe
    /// key root and the residual root.
    SharedKey,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Output {
    /// Every output occurrence the kind may publish, in physical input order.
    Natural,
    /// `[right.w (or its NULL extension), left.k]`.
    Permuted,
}

#[derive(Clone, Copy, Debug)]
struct Case {
    family: Family,
    residual: Residual,
    output: Output,
}

struct Fixture {
    package: Arc<FragmentPackage>,
    /// `[k, v]` of the left Values and `[k, w]` of the right Values.
    left: [ValueId; 2],
    right: [ValueId; 2],
    output: Vec<ValueId>,
    key: Option<JoinKey>,
    residual: Option<ExprId>,
}

fn values(builder: &mut FragmentBuilder, node: NodeId, rows: &[[i64; 2]]) -> [ValueId; 2] {
    let columns = [0u32, 1].map(|ordinal| {
        builder
            .add_value(
                int64(),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: ordinal,
                },
            )
            .unwrap()
    });
    let cells = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|cell| {
                    builder
                        .add_expression(
                            node,
                            int64(),
                            ExprKind::Literal(LiteralValue::Int64(*cell)),
                        )
                        .unwrap()
                })
                .collect::<Vec<_>>()
                .into_boxed_slice()
        })
        .collect::<Vec<_>>();
    builder
        .add_values(node, cells.into_boxed_slice(), Box::from(columns))
        .unwrap();
    columns
}

fn singleton() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn kind_of(family: Family) -> JoinKind {
    match family {
        Family::Hash { kind, .. } | Family::NestLoop { kind } => kind,
    }
}

fn fixture(case: Case) -> Fixture {
    let fragment_id = FragmentId::new(41);
    let mut builder = FragmentBuilder::new(fragment_id);
    let left = values(&mut builder, LEFT, &[[1, 10], [2, 20], [3, 30]]);
    let right = values(&mut builder, RIGHT, &[[1, 15], [2, 5], [4, 40]]);
    let kind = kind_of(case.family);
    let key = match case.family {
        Family::Hash { null_safe, .. } => Some(JoinKey {
            left: builder
                .add_expression(JOIN, int64(), ExprKind::Value(left[0]))
                .unwrap(),
            right: builder
                .add_expression(JOIN, int64(), ExprKind::Value(right[0]))
                .unwrap(),
            null_safe,
        }),
        Family::NestLoop { .. } => None,
    };
    let residual = match case.residual {
        Residual::None => None,
        Residual::Separate | Residual::SharedKey => {
            let lhs = if case.residual == Residual::SharedKey {
                key.as_ref().expect("a shared key").left
            } else {
                builder
                    .add_expression(JOIN, int64(), ExprKind::Value(left[1]))
                    .unwrap()
            };
            let rhs = builder
                .add_expression(JOIN, int64(), ExprKind::Value(right[1]))
                .unwrap();
            Some(
                builder
                    .add_expression(
                        JOIN,
                        FunctionValueType::new(DataType::Boolean, false),
                        ExprKind::Binary {
                            left: lhs,
                            op: BinaryOperator::Lt,
                            right: rhs,
                            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                            allow_throw_exception: None,
                        },
                    )
                    .unwrap(),
            )
        }
    };
    let mut extend = |of: ValueId| {
        builder
            .add_value(
                int64_nullable(),
                ValueOrigin::NullExtended { node: JOIN, of },
            )
            .unwrap()
    };
    // Each kind publishes what its output law allows, in input order.
    let (left_out, right_out): (Vec<ValueId>, Vec<ValueId>) = match kind {
        JoinKind::Cross | JoinKind::Inner => (left.to_vec(), right.to_vec()),
        JoinKind::LeftOuter => (left.to_vec(), right.iter().map(|v| extend(*v)).collect()),
        JoinKind::RightOuter => (left.iter().map(|v| extend(*v)).collect(), right.to_vec()),
        JoinKind::FullOuter => (
            left.iter().map(|v| extend(*v)).collect(),
            right.iter().map(|v| extend(*v)).collect(),
        ),
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => {
            (left.to_vec(), vec![])
        }
        JoinKind::RightSemi | JoinKind::RightAnti => (vec![], right.to_vec()),
    };
    let output = match case.output {
        Output::Natural => left_out.iter().chain(&right_out).copied().collect(),
        Output::Permuted => vec![right_out[1], left_out[0]],
    };
    let null_extended = output
        .iter()
        .copied()
        .filter(|value| {
            matches!(
                builder.value(*value).unwrap().origin,
                ValueOrigin::NullExtended { .. }
            )
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let node_kind = match case.family {
        Family::Hash { build_side, .. } => NodeKind::HashJoin {
            kind,
            keys: Box::from([key.clone().unwrap()]),
            build_side,
            distribution: JoinDistribution::Singleton,
            residual,
            null_extended,
        },
        Family::NestLoop { .. } => NodeKind::NestLoopJoin {
            kind,
            distribution: NestLoopJoinDistribution::Singleton,
            predicate: residual,
            null_extended,
        },
    };
    builder
        .add_join(
            JOIN,
            [LEFT, RIGHT],
            Box::from([singleton(), singleton()]),
            output.clone().into_boxed_slice(),
            Distribution::Singleton,
            node_kind,
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            JOIN,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    Fixture {
        package: publish(fragment),
        left,
        right,
        output,
        key,
        residual,
    }
}

/// One eager use per actual occurrence: a value or literal leaf, or a binary
/// comparison over its two ordered operands.
fn root_uses(fragment: &Fragment) -> PhysicalRootUses {
    struct Author<'a> {
        fragment: &'a Fragment,
        next: u32,
        uses: Vec<ExpressionInvocation<ExprId>>,
    }
    impl Author<'_> {
        fn visit(
            &mut self,
            expr: ExprId,
            domain: EvaluationDomainId,
            demand: novarocks_type_contract::EvaluationDemand,
        ) -> ExpressionUseId {
            let id = ExpressionUseId::new(self.next);
            self.next += 3;
            let children = match &self.fragment.expressions().get(expr).unwrap().kind {
                ExprKind::Binary { left, right, .. } => vec![*left, *right],
                ExprKind::Literal(_) | ExprKind::Value(_) => vec![],
                other => panic!("fixture has no {other:?}"),
            };
            let arguments = children
                .iter()
                .enumerate()
                .map(|(ordinal, &child)| {
                    let (child_demand, guard) = control_argument_semantics(
                        ControlShape::Eager,
                        children.len(),
                        ordinal,
                        demand,
                    )
                    .unwrap();
                    assert!(guard.is_none());
                    self.visit(child, domain, child_demand)
                })
                .collect::<Vec<_>>();
            self.uses.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: id,
                    domain,
                    demand,
                },
                definition: expr,
                control: ControlShape::Eager,
                arguments: arguments.into_boxed_slice(),
            });
            id
        }
    }
    let domain = EvaluationDomainId::new(5);
    let mut author = Author {
        fragment,
        next: 7,
        uses: vec![],
    };
    let roots = PhysicalExpressionRoots::try_new(fragment, &Control).unwrap();
    let bindings = roots
        .sites()
        .iter()
        .map(|(&site, root)| (site, author.visit(root.expr, domain, root.demand)))
        .collect();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        author.uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &Control).unwrap()
}

fn publish(fragment: Fragment) -> Arc<FragmentPackage> {
    let uses = root_uses(&fragment);
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &Control).unwrap();
    let root = &fragment.nodes()[&fragment.root()];
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: root.output.clone(),
        fields: root
            .output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: format!("join_{ordinal}").into_boxed_str(),
                alias: None,
                value: *value,
                ty: fragment.values()[value].ty.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    };
    let id = fragment.id();
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                constants: novarocks_physical_plan::ConstantPools::empty(),
                version: PlanVersionId::try_new([41; 16]).unwrap(),
                required: RequiredContracts::default(),
                fragment,
                expression_uses: uses,
                calls,
                pruning: FrozenFragmentPruning::try_new(id, vec![], &Control).unwrap(),
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
        .unwrap(),
    )
}

fn compile_with(
    package: &Arc<FragmentPackage>,
    control: &dyn PureCompileControl,
) -> Result<LocalProgram, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let validated = validate_fragment_providers(package.clone(), &providers, &Control).unwrap();
    compile_fragment(validated, &functions(), options(), control)
}

fn compile(package: &Arc<FragmentPackage>) -> LocalProgram {
    compile_with(package, &Control).unwrap_or_else(|error| panic!("join compiles: {error}"))
}

fn local_of(program: &LocalProgram, physical: NodeId) -> ProgramNodeId {
    program
        .graph()
        .nodes()
        .iter()
        .find(|node| {
            node.physical_sources()
                .iter()
                .any(|source| source.get() == physical.get())
                && !matches!(
                    node.kind(),
                    ProgramNodeKind::Project {
                        is_subordinate: true,
                        ..
                    }
                )
        })
        .unwrap()
        .local_id()
        .unwrap()
}

fn join_node(program: &LocalProgram) -> (ProgramNodeId, &ProgramNodeKind) {
    let node = program
        .graph()
        .nodes()
        .iter()
        .find(|node| {
            matches!(
                node.kind(),
                ProgramNodeKind::Join { .. } | ProgramNodeKind::NestedLoopJoin { .. }
            )
        })
        .unwrap();
    (node.local_id().unwrap(), node.kind())
}

fn site(node: ProgramNodeId, role: ProgramNodeExpressionRole) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node { node, role }
}

/// Every slot read under `root`, with its checked lexical source.
fn slot_sources(
    program: &LocalProgram,
    root: ProgramExpressionRootSite,
) -> Vec<(novarocks_types::SlotId, ProgramChannelSite)> {
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
    let arena = &snapshot.roots().arenas()[&ProgramExpressionArena::Main];
    let mut stack = vec![snapshot.bindings()[&root]];
    let mut out = Vec::new();
    while let Some(use_id) = stack.pop() {
        let invocation = &flow.uses()[&use_id];
        if let StaticExprKind::SlotId(slot) = arena.node(invocation.definition).unwrap().kind() {
            let ProgramLexicalSource::Input(source) = program.checked().slots()[&ProgramUseRef {
                arena: ProgramExpressionArena::Main,
                use_id,
            }] else {
                panic!("join slot reads an input channel")
            };
            out.push((*slot, source));
        }
        stack.extend(invocation.arguments.iter().rev().copied());
    }
    out
}

fn layout(node: ProgramNodeId, role: ProgramChannelLayoutRole, ordinal: u32) -> ProgramChannelSite {
    ProgramChannelSite::Layout {
        node,
        role,
        ordinal,
    }
}

fn hash(kind: JoinKind, build_side: JoinSide) -> Family {
    Family::Hash {
        kind,
        build_side,
        null_safe: false,
    }
}

#[test]
fn inner_join_with_right_build_keeps_orientation_and_publishes_canonical_output() {
    let source = fixture(Case {
        family: hash(JoinKind::Inner, JoinSide::Right),
        residual: Residual::Separate,
        output: Output::Natural,
    });
    let program = compile(&source.package);
    let (join, kind) = join_node(&program);
    let ProgramNodeKind::Join {
        left,
        right,
        join_type,
        left_layout,
        right_layout,
        join_scope_layout,
        probe_keys,
        build_keys,
        eq_null_safe,
        residual_predicate,
        runtime_filters,
        ..
    } = kind
    else {
        unreachable!()
    };
    assert_eq!(*join_type, JoinType::Inner);
    assert_eq!(*left, local_of(&program, LEFT));
    assert_eq!(*right, local_of(&program, RIGHT));
    assert_eq!((probe_keys.len(), build_keys.len()), (1, 1));
    assert_eq!(eq_null_safe, &vec![false]);
    assert!(residual_predicate.is_some() && runtime_filters.is_empty());
    // The scope is the probe slots followed by the build slots, unchanged.
    let scope = [left_layout.slots(), right_layout.slots()].concat();
    assert_eq!(join_scope_layout.slots(), scope.as_slice());
    // The canonical output is the physical output: no selection Project.
    assert_eq!(program.graph().root(), join);
    let output = program.graph().nodes()[join.index()].output_layout();
    assert_eq!(output.slots().len(), 4);
    assert!(output.slots().iter().all(|slot| !scope.contains(slot)));
    let names = output
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(names, ["join_0", "join_1", "join_2", "join_3"]);
    assert_eq!(
        program
            .provenance()
            .operators()
            .values()
            .filter(|operator| operator.sources.iter().any(|s| s.get() == JOIN.get()))
            .map(|operator| operator.origin)
            .collect::<Vec<_>>(),
        vec![LocalOperatorOrigin::Direct]
    );
    // Each key root reads its own side; the residual reads the scope.
    use ProgramChannelLayoutRole as Role;
    assert_eq!(
        slot_sources(
            &program,
            site(join, ProgramNodeExpressionRole::JoinProbeKey { key: 0 })
        ),
        vec![(left_layout.slots()[0], layout(join, Role::JoinLeft, 0))]
    );
    assert_eq!(
        slot_sources(
            &program,
            site(join, ProgramNodeExpressionRole::JoinBuildKey { key: 0 })
        ),
        vec![(right_layout.slots()[0], layout(join, Role::JoinRight, 0))]
    );
    assert_eq!(
        slot_sources(
            &program,
            site(join, ProgramNodeExpressionRole::JoinResidual)
        ),
        vec![
            (left_layout.slots()[1], layout(join, Role::JoinScope, 1)),
            (right_layout.slots()[1], layout(join, Role::JoinScope, 3)),
        ]
    );
    // Every join layout role is typed; the residual root is TruthOnly.
    let channels = program.checked().channels();
    for role in [
        Role::JoinLeft,
        Role::JoinRight,
        Role::JoinScope,
        Role::NodeOutput,
    ] {
        let width = channels.channel_layout(join, role).unwrap().slots().len();
        for ordinal in 0..width {
            assert!(
                channels
                    .channel_type(layout(join, role, ordinal as u32))
                    .is_some()
            );
        }
    }
    let snapshot = channels.expressions().resolved_calls().snapshot();
    let residual_use = snapshot.bindings()[&site(join, ProgramNodeExpressionRole::JoinResidual)];
    assert_eq!(
        snapshot.flows()[&ProgramExpressionArena::Main].uses()[&residual_use]
            .context
            .demand,
        novarocks_type_contract::EvaluationDemand::TruthOnly
    );
    assert!(source.residual.is_some() && source.key.is_some());
}

#[test]
fn left_build_right_semi_and_anti_mirror_to_probe_preserving_left_kinds() {
    for (physical, local) in [
        (JoinKind::RightSemi, JoinType::LeftSemi),
        (JoinKind::RightAnti, JoinType::LeftAnti),
        (JoinKind::RightOuter, JoinType::LeftOuter),
        (JoinKind::Inner, JoinType::Inner),
    ] {
        let source = fixture(Case {
            family: hash(physical, JoinSide::Left),
            residual: Residual::Separate,
            output: Output::Natural,
        });
        let program = compile(&source.package);
        let (join, kind) = join_node(&program);
        let ProgramNodeKind::Join {
            left,
            right,
            join_type,
            left_layout,
            right_layout,
            ..
        } = kind
        else {
            unreachable!()
        };
        assert_eq!(*join_type, local, "{physical:?}");
        // The physical right child is the local probe.
        assert_eq!(*left, local_of(&program, RIGHT));
        assert_eq!(*right, local_of(&program, LEFT));
        use ProgramChannelLayoutRole as Role;
        // The physical right key is the probe key, the left key the build key.
        assert_eq!(
            slot_sources(
                &program,
                site(join, ProgramNodeExpressionRole::JoinProbeKey { key: 0 })
            ),
            vec![(left_layout.slots()[0], layout(join, Role::JoinLeft, 0))]
        );
        assert_eq!(
            slot_sources(
                &program,
                site(join, ProgramNodeExpressionRole::JoinBuildKey { key: 0 })
            ),
            vec![(right_layout.slots()[0], layout(join, Role::JoinRight, 0))]
        );
        // `left.v < right.w` reads the build side (physical left) after the
        // probe side (physical right) in the scope.
        assert_eq!(
            slot_sources(
                &program,
                site(join, ProgramNodeExpressionRole::JoinResidual)
            ),
            vec![
                (right_layout.slots()[1], layout(join, Role::JoinScope, 3)),
                (left_layout.slots()[1], layout(join, Role::JoinScope, 1)),
            ]
        );
        // Semi and anti publish exactly the probe side, which is their
        // physical output; the outer and inner kinds reorder through a
        // selection Project.
        let selection = matches!(physical, JoinKind::RightOuter | JoinKind::Inner);
        assert_eq!(program.graph().root() != join, selection, "{physical:?}");
        let _ = &source.right;
    }
}

#[test]
fn mirrored_right_outer_permutation_keeps_source_and_nullable_target_ordinals() {
    let source = fixture(Case {
        family: hash(JoinKind::RightOuter, JoinSide::Left),
        residual: Residual::Separate,
        output: Output::Permuted,
    });
    let program = compile(&source.package);
    let (join, kind) = join_node(&program);
    let ProgramNodeKind::Join {
        left,
        right,
        join_type,
        runtime_filters,
        ..
    } = kind
    else {
        unreachable!()
    };
    assert_eq!(*join_type, JoinType::LeftOuter);
    assert_eq!(*left, local_of(&program, RIGHT));
    assert_eq!(*right, local_of(&program, LEFT));
    assert!(runtime_filters.is_empty());
    let canonical = program.graph().nodes()[join.index()].output_layout();
    assert_eq!(
        canonical
            .schema()
            .fields()
            .iter()
            .map(|field| field.is_nullable())
            .collect::<Vec<_>>(),
        [false, false, true, true]
    );
    let root = program.graph().root();
    let selection = &program.graph().nodes()[root.index()];
    let ProgramNodeKind::Project {
        input,
        is_subordinate,
        ..
    } = selection.kind()
    else {
        panic!("physical output needs a selection Project")
    };
    assert_eq!((*input, *is_subordinate), (join, true));
    // Physical [source.w, target.k] maps to canonical [probe.w, extended-build.k].
    for (expression, ordinal) in [(0u32, 1u32), (1, 2)] {
        assert_eq!(
            slot_sources(
                &program,
                site(
                    root,
                    ProgramNodeExpressionRole::ProjectOutput { expression }
                )
            ),
            vec![(
                canonical.slots()[ordinal as usize],
                layout(join, ProgramChannelLayoutRole::NodeOutput, ordinal)
            )]
        );
    }
    assert!(!selection.output_layout().schema().field(0).is_nullable());
    assert!(selection.output_layout().schema().field(1).is_nullable());
}

#[test]
fn one_definition_shared_by_key_and_residual_keeps_a_source_per_use() {
    let source = fixture(Case {
        family: hash(JoinKind::Inner, JoinSide::Right),
        residual: Residual::SharedKey,
        output: Output::Natural,
    });
    let program = compile(&source.package);
    let (join, kind) = join_node(&program);
    let ProgramNodeKind::Join {
        probe_keys,
        residual_predicate,
        left_layout,
        right_layout,
        ..
    } = kind
    else {
        unreachable!()
    };
    let arena = program.graph().expressions();
    let Some(StaticExprKind::Lt(shared, _)) = arena
        .node(residual_predicate.unwrap())
        .map(|node| node.kind())
    else {
        panic!("the residual is the ordered comparison")
    };
    // One definition, two occurrences with different scopes.
    assert_eq!(*shared, probe_keys[0]);
    use ProgramChannelLayoutRole as Role;
    let key = left_layout.slots()[0];
    assert_eq!(
        slot_sources(
            &program,
            site(join, ProgramNodeExpressionRole::JoinProbeKey { key: 0 })
        ),
        vec![(key, layout(join, Role::JoinLeft, 0))]
    );
    assert_eq!(
        slot_sources(
            &program,
            site(join, ProgramNodeExpressionRole::JoinResidual)
        ),
        vec![
            (key, layout(join, Role::JoinScope, 0)),
            (right_layout.slots()[1], layout(join, Role::JoinScope, 3)),
        ]
    );
}

#[test]
fn permuted_outer_output_selects_canonical_ordinals_and_null_extensions() {
    let source = fixture(Case {
        family: hash(JoinKind::LeftOuter, JoinSide::Right),
        residual: Residual::None,
        output: Output::Permuted,
    });
    let program = compile(&source.package);
    let (join, kind) = join_node(&program);
    assert!(matches!(
        kind,
        ProgramNodeKind::Join {
            join_type: JoinType::LeftOuter,
            ..
        }
    ));
    let canonical = program.graph().nodes()[join.index()].output_layout();
    // Probe fields keep their types; build fields are NULL-extended.
    let nullable = canonical
        .schema()
        .fields()
        .iter()
        .map(|field| field.is_nullable())
        .collect::<Vec<_>>();
    assert_eq!(nullable, [false, false, true, true]);
    let root = program.graph().root();
    assert_eq!(root.index(), join.index() + 1);
    let selection = &program.graph().nodes()[root.index()];
    let ProgramNodeKind::Project {
        input,
        exprs,
        is_subordinate,
        ..
    } = selection.kind()
    else {
        panic!("a selection Project publishes the physical output")
    };
    assert_eq!((*input, *is_subordinate), (join, true));
    assert_eq!(exprs.len(), 2);
    // `[NULL-extended right.w, left.k]` reads canonical `[3, 0]`.
    for (expression, canonical_ordinal) in [(0u32, 3u32), (1, 0)] {
        assert_eq!(
            slot_sources(
                &program,
                site(
                    root,
                    ProgramNodeExpressionRole::ProjectOutput { expression }
                )
            ),
            vec![(
                canonical.slots()[canonical_ordinal as usize],
                layout(
                    join,
                    ProgramChannelLayoutRole::NodeOutput,
                    canonical_ordinal
                )
            )]
        );
    }
    let fields = selection.output_layout().schema();
    assert_eq!(fields.field(0).name(), "join_0");
    assert!(fields.field(0).is_nullable() && !fields.field(1).is_nullable());
    // Two split pieces under one cost owner.
    let pieces = program
        .provenance()
        .operators()
        .values()
        .filter(|operator| operator.sources.iter().any(|s| s.get() == JOIN.get()))
        .map(|operator| (operator.origin, operator.cost_owner))
        .collect::<Vec<_>>();
    assert_eq!(pieces.len(), 2);
    assert!(
        pieces
            .iter()
            .all(|(_, owner)| owner.get() as usize == join.index())
    );
    assert_eq!(source.output.len(), 2);
}

#[test]
fn nested_loop_kinds_lower_with_scope_predicates_and_singleton_mirroring() {
    for (physical, local, probe) in [
        (JoinKind::Cross, NestedLoopJoinType::Cross, LEFT),
        (JoinKind::Inner, NestedLoopJoinType::Inner, LEFT),
        (JoinKind::LeftOuter, NestedLoopJoinType::LeftOuter, LEFT),
        (JoinKind::LeftSemi, NestedLoopJoinType::LeftSemi, LEFT),
        (JoinKind::LeftAnti, NestedLoopJoinType::LeftAnti, LEFT),
        (JoinKind::RightSemi, NestedLoopJoinType::LeftSemi, RIGHT),
        (JoinKind::RightAnti, NestedLoopJoinType::LeftAnti, RIGHT),
    ] {
        let source = fixture(Case {
            family: Family::NestLoop { kind: physical },
            residual: if physical == JoinKind::Cross {
                Residual::None
            } else {
                Residual::Separate
            },
            output: Output::Natural,
        });
        let program = compile(&source.package);
        let (join, kind) = join_node(&program);
        let ProgramNodeKind::NestedLoopJoin {
            left,
            join_type,
            join_conjunct,
            join_scope_layout,
            ..
        } = kind
        else {
            panic!("{physical:?} lowers to a nested-loop join")
        };
        assert_eq!(*join_type, local);
        assert_eq!(*left, local_of(&program, probe));
        assert_eq!(join_conjunct.is_some(), physical != JoinKind::Cross);
        if physical != JoinKind::Cross {
            let reads = slot_sources(
                &program,
                site(join, ProgramNodeExpressionRole::NestedLoopPredicate),
            );
            assert_eq!(reads.len(), 2);
            for (slot, source) in reads {
                let ProgramChannelSite::Layout { role, ordinal, .. } = source else {
                    unreachable!()
                };
                assert_eq!(role, ProgramChannelLayoutRole::JoinScope);
                assert_eq!(join_scope_layout.slots()[ordinal as usize], slot);
            }
        }
        let _ = &source.left;
    }
}

#[test]
fn unsupported_kinds_and_orientations_are_explicit_refusals() {
    let refused = [
        // No mirror keeps a left-build null-aware anti join probe-preserving.
        Case {
            family: hash(JoinKind::NullAwareLeftAnti, JoinSide::Left),
            residual: Residual::None,
            output: Output::Natural,
        },
        // Build-preserving kinds need merged build match flags.
        Case {
            family: hash(JoinKind::RightOuter, JoinSide::Right),
            residual: Residual::None,
            output: Output::Natural,
        },
        Case {
            family: hash(JoinKind::FullOuter, JoinSide::Right),
            residual: Residual::None,
            output: Output::Natural,
        },
        Case {
            family: hash(JoinKind::LeftSemi, JoinSide::Left),
            residual: Residual::None,
            output: Output::Natural,
        },
        // The key conjuncts of a nested-loop null-aware anti join are not
        // separated from its residual.
        Case {
            family: Family::NestLoop {
                kind: JoinKind::NullAwareLeftAnti,
            },
            residual: Residual::Separate,
            output: Output::Natural,
        },
        Case {
            family: Family::NestLoop {
                kind: JoinKind::FullOuter,
            },
            residual: Residual::Separate,
            output: Output::Natural,
        },
    ];
    for case in refused {
        let source = fixture(case);
        assert!(
            matches!(
                compile_with(&source.package, &Control),
                Err(FragmentCompileError::Unsupported {
                    node: Some(JOIN),
                    ..
                })
            ),
            "{case:?}"
        );
    }
    // A null-aware anti join without a predicate keeps rows iff the build is
    // empty, which needs no key split.
    let source = fixture(Case {
        family: Family::NestLoop {
            kind: JoinKind::NullAwareLeftAnti,
        },
        residual: Residual::None,
        output: Output::Natural,
    });
    compile(&source.package);
}

struct Trace {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    at: Option<usize>,
    cause: CompileControlError,
    refused: AtomicBool,
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(
            !self.refused.load(Ordering::SeqCst),
            "callback after original refusal"
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
fn every_join_compiler_control_prefix_refuses_with_its_own_cause() {
    let source = fixture(Case {
        family: hash(JoinKind::LeftOuter, JoinSide::Right),
        residual: Residual::SharedKey,
        output: Output::Permuted,
    });
    let baseline = Trace {
        events: Mutex::new(vec![]),
        at: None,
        cause: CompileControlError::Cancelled,
        refused: AtomicBool::new(false),
    };
    compile_with(&source.package, &baseline).unwrap();
    let expected = baseline.events.into_inner().unwrap();
    assert!(!expected.is_empty());
    // Every phase boundary, the last checkpoint and a fixed stride cover the
    // join owners' checkpoints without replaying every one of them.
    let stride = (expected.len() / 48).max(1);
    let positions = (0..expected.len())
        .filter(|&at| {
            at % stride == 0
                || at + 1 == expected.len()
                || expected[at.saturating_sub(1)].0 != expected[at].0
        })
        .collect::<Vec<_>>();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for &at in &positions {
            let control = Trace {
                events: Mutex::new(vec![]),
                at: Some(at),
                cause: cause.clone(),
                refused: AtomicBool::new(false),
            };
            assert!(matches!(
                compile_with(&source.package, &control),
                Err(FragmentCompileError::Control(actual)) if actual == cause
            ));
            assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
        }
    }
}
