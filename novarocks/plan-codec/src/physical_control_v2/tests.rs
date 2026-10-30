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
use arrow::datatypes::DataType;
use novarocks_physical_plan::{
    BinaryOperator, BoundFunction, Distribution, ExprKind, FragmentBuilder, FragmentId,
    FragmentSink, FunctionArgumentEvaluation, FunctionArgumentType, FunctionFailureBehavior,
    FunctionId, FunctionIntrinsicRowError, FunctionKind, FunctionOverloadId, FunctionVolatility,
    JoinDistribution, JoinKey, JoinKind, LiteralValue, NodeKind, OutputPort,
    PhysicalExpressionRoots, PhysicalNode, PhysicalProperties, PipelineDopDomain, RowMultiplicity,
    ValueOrigin, ValueType,
};
use novarocks_type_contract::{DecimalOverflowPolicy, control_argument_semantics};
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    failure: Option<(CompilePhase, CompileControlError)>,
    observations: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        self.observations.lock().unwrap().push((phase, units));
        if units > 0
            && let Some((failed_phase, failure)) = self.failure.clone()
            && phase == failed_phase
        {
            Err(failure)
        } else {
            Ok(())
        }
    }
}
fn properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    }
}
fn finish_values(mut builder: FragmentBuilder, node: NodeId, roots: &[ExprId]) -> Fragment {
    let columns = roots
        .iter()
        .enumerate()
        .map(|(ordinal, root)| {
            let ty = builder.expressions().get(*root).unwrap().ty.clone();
            builder
                .add_value(
                    ty,
                    ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties(),
            output: OutputPort {
                node,
                columns: columns.into_boxed_slice(),
            },
            kind: NodeKind::Values {
                rows: Box::from([roots.into()]),
            },
        })
        .unwrap();
    builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap()
}
fn invocation(
    id: u32,
    definition: ExprId,
    domain: EvaluationDomainId,
    demand: EvaluationDemand,
    control: ControlShape,
    arguments: &[u32],
) -> ExpressionInvocation<ExprId> {
    ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            domain,
            demand,
        },
        definition,
        control,
        arguments: arguments
            .iter()
            .copied()
            .map(ExpressionUseId::new)
            .collect(),
    }
}
fn binary_fixture() -> (Fragment, PhysicalRootUses) {
    let mut builder = FragmentBuilder::new(FragmentId::new(701));
    let node = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Boolean, false);
    let left = builder
        .add_expression(
            node,
            ty.clone(),
            ExprKind::Literal(LiteralValue::Boolean(true)),
        )
        .unwrap();
    let right = builder
        .add_expression(
            node,
            ty.clone(),
            ExprKind::Literal(LiteralValue::Boolean(false)),
        )
        .unwrap();
    let root = builder
        .add_expression(
            node,
            ty,
            ExprKind::Binary {
                allow_throw_exception: None,
                left,
                op: BinaryOperator::Eq,
                right,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
        )
        .unwrap();
    let fragment = finish_values(builder, node, &[root]);
    let domain = EvaluationDomainId::new(u32::MAX);
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        vec![
            invocation(
                0,
                root,
                domain,
                EvaluationDemand::Value,
                ControlShape::Eager,
                &[u32::MAX, 7],
            ),
            invocation(
                u32::MAX,
                left,
                domain,
                EvaluationDemand::Value,
                ControlShape::Eager,
                &[],
            ),
            invocation(
                7,
                right,
                domain,
                EvaluationDemand::Value,
                ControlShape::Eager,
                &[],
            ),
        ],
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(
        &fragment,
        flow,
        vec![(
            ExpressionRootSite {
                node,
                role: ExpressionRootRole::ValuesCell { row: 0, column: 0 },
            },
            ExpressionUseId::new(0),
        )],
        &Control::default(),
    )
    .unwrap();
    (fragment, uses)
}

#[test]
fn typed_control_roundtrip_preserves_presence_and_ordered_children() {
    let (fragment, roots) = binary_fixture();
    let control = Control::default();
    let encoded = encode_expression_control(&roots, &control).unwrap();
    assert_eq!(encoded.domains[0].id, u32::MAX);
    assert_eq!(encoded.uses[0].id, 0);
    assert_eq!(encoded.uses[0].domain_id, Some(u32::MAX));
    assert_eq!(encoded.uses[0].argument_use_ids, [u32::MAX, 7]);
    assert_eq!(
        encoded
            .uses
            .iter()
            .find(|item| item.id == u32::MAX)
            .unwrap()
            .definition_id,
        Some(0)
    );
    assert_eq!(encoded.roots[0].use_id, Some(0));
    assert_eq!(encoded.roots[0].site.as_ref().unwrap().node_id, Some(0));
    let decoded = decode_expression_control(&fragment, &encoded, &control).unwrap();
    assert_eq!(decoded, roots);
    assert!(
        control
            .observations
            .lock()
            .unwrap()
            .iter()
            .any(|(phase, _)| *phase == CompilePhase::Encode)
    );
}

fn boolean(builder: &mut FragmentBuilder, node: NodeId, value: bool) -> ExprId {
    builder
        .add_expression(
            node,
            ValueType::new(DataType::Boolean, false),
            ExprKind::Literal(LiteralValue::Boolean(value)),
        )
        .unwrap()
}
fn leaf_root_uses(fragment: &Fragment) -> PhysicalRootUses {
    let sites = PhysicalExpressionRoots::try_new(fragment, &Control::default()).unwrap();
    let domain = EvaluationDomainId::new(u32::MAX);
    let mut bindings = Vec::new();
    let uses = sites
        .sites()
        .iter()
        .enumerate()
        .map(|(ordinal, (site, root))| {
            // This fixture deliberately contains only literal/value leaves.
            assert!(matches!(
                fragment.expressions().get(root.expr).unwrap().kind,
                ExprKind::Literal(_) | ExprKind::Value(_)
            ));
            let id = ordinal as u32;
            bindings.push((*site, ExpressionUseId::new(id)));
            invocation(id, root.expr, domain, root.demand, ControlShape::Eager, &[])
        })
        .collect();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &Control::default()).unwrap()
}
fn shared_definition_fixture() -> (Fragment, PhysicalRootUses) {
    let mut builder = FragmentBuilder::new(FragmentId::new(702));
    let mut inputs = Vec::new();
    let mut columns = Vec::new();
    for value in [true, false] {
        let node = builder.reserve_node_id().unwrap();
        let expr = boolean(&mut builder, node, value);
        let column = builder
            .add_value(
                ValueType::new(DataType::Boolean, false),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                id: node,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node,
                    columns: Box::from([column]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([expr])]),
                },
            })
            .unwrap();
        inputs.push(node);
        columns.push(column);
    }
    let node = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Boolean, false);
    let left = builder
        .add_expression(node, ty.clone(), ExprKind::Value(columns[0]))
        .unwrap();
    let right = builder
        .add_expression(node, ty, ExprKind::Value(columns[1]))
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: inputs.into_boxed_slice(),
            required_inputs: Box::from([properties(), properties()]),
            output_properties: properties(),
            output: OutputPort {
                node,
                columns: columns.into_boxed_slice(),
            },
            kind: NodeKind::HashJoin {
                kind: JoinKind::Inner,
                condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                    keys: Box::from([JoinKey {
                        left,
                        right,
                        null_safe: false,
                    }]),
                    residual: Some(left),
                },
                build_side: JoinSide::Right,
                distribution: JoinDistribution::Singleton,
                null_extended: Box::default(),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let roots = leaf_root_uses(&fragment);
    (fragment, roots)
}
#[test]
fn shared_definition_retains_independent_value_and_truth_only_occurrences() {
    let (fragment, roots) = shared_definition_fixture();
    let encoded = encode_expression_control(&roots, &Control::default()).unwrap();
    let duplicates = encoded
        .uses
        .iter()
        .filter(|item| item.definition_id == Some(2))
        .collect::<Vec<_>>();
    assert_eq!(duplicates.len(), 2);
    assert_ne!(duplicates[0].id, duplicates[1].id);
    assert!(
        duplicates
            .iter()
            .any(|item| item.demand == wire::EvaluationDemand::Value as i32)
    );
    assert!(
        duplicates
            .iter()
            .any(|item| item.demand == wire::EvaluationDemand::TruthOnly as i32)
    );
    assert_eq!(
        decode_expression_control(&fragment, &encoded, &Control::default()).unwrap(),
        roots
    );
}

fn branch_fixture(is_case: bool) -> (Fragment, PhysicalRootUses) {
    let mut builder = FragmentBuilder::new(FragmentId::new(703));
    let node = builder.reserve_node_id().unwrap();
    let arguments = [
        boolean(&mut builder, node, true),
        boolean(&mut builder, node, true),
        boolean(&mut builder, node, false),
    ];
    let ty = ValueType::new(DataType::Boolean, false);
    let kind = if is_case {
        ExprKind::Case {
            operand: None,
            when_then: Box::from([(arguments[0], arguments[1])]),
            else_expr: Some(arguments[2]),
        }
    } else {
        ExprKind::FunctionCall {
            function: BoundFunction {
                function_id: FunctionId::try_new("fixture/control-codec/branch").unwrap(),
                overload: FunctionOverloadId::try_new("fixture/control-codec/boolean-branch")
                    .unwrap(),
                kind: FunctionKind::Scalar,
                argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    vec![FunctionArgumentType::Value(ty.clone()); 3].into_boxed_slice(),
                ),
                result_type: ty.clone(),

                legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
                    semantic_parameters: Box::default(),
                    volatility: FunctionVolatility::Immutable,
                    argument_evaluation: FunctionArgumentEvaluation::Eager,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
                }),
            },
            args: Box::from(arguments),
        }
    };
    let root = builder.add_expression(node, ty, kind).unwrap();
    let fragment = finish_values(builder, node, &[root]);
    // The exact selected owner remains a compiler obligation. The fixture
    // explicitly requests If; neither its name nor the legacy flag selects it.
    let shape = if is_case {
        ControlShape::Case {
            simple: false,
            arms: 1,
            has_else: true,
        }
    } else {
        ControlShape::If
    };
    let domain = EvaluationDomainId::new(u32::MAX);
    let mut domains = vec![ExpressionEvaluationDomain {
        id: domain,
        parent: None,
        guard: None,
    }];
    let mut uses = vec![invocation(
        0,
        root,
        domain,
        EvaluationDemand::Value,
        shape,
        &[7, 8, u32::MAX],
    )];
    for (ordinal, (id, definition)) in [7, 8, u32::MAX].into_iter().zip(arguments).enumerate() {
        let (demand, guard) =
            control_argument_semantics(shape, 3, ordinal, EvaluationDemand::Value).unwrap();
        let child_domain = if let Some(kind) = guard {
            let child = EvaluationDomainId::new(ordinal as u32);
            domains.push(ExpressionEvaluationDomain {
                id: child,
                parent: Some(domain),
                guard: Some(DomainGuard {
                    owner: ExpressionUseId::new(0),
                    kind,
                }),
            });
            child
        } else {
            domain
        };
        uses.push(invocation(
            id,
            definition,
            child_domain,
            demand,
            ControlShape::Eager,
            &[],
        ));
    }
    let flow = ExpressionControlFlow::try_new(
        domains,
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let roots = PhysicalRootUses::try_new(
        &fragment,
        flow,
        vec![(
            ExpressionRootSite {
                node,
                role: ExpressionRootRole::ValuesCell { row: 0, column: 0 },
            },
            ExpressionUseId::new(0),
        )],
        &Control::default(),
    )
    .unwrap();
    (fragment, roots)
}
#[test]
fn actual_case_and_explicit_if_preserve_guard_owner_ordinal_and_demand() {
    for is_case in [false, true] {
        let (fragment, roots) = branch_fixture(is_case);
        let encoded = encode_expression_control(&roots, &Control::default()).unwrap();
        for domain in encoded
            .domains
            .iter()
            .filter(|domain| domain.guard.is_some())
        {
            assert_eq!(domain.parent_domain_id, Some(u32::MAX));
            assert_eq!(domain.guard.as_ref().unwrap().owner_use_id, Some(0));
        }
        assert_eq!(
            encoded
                .uses
                .iter()
                .find(|item| item.id == 7)
                .unwrap()
                .demand,
            wire::EvaluationDemand::TruthOnly as i32
        );
        assert_eq!(
            decode_expression_control(&fragment, &encoded, &Control::default()).unwrap(),
            roots
        );
    }
}

#[test]
fn every_root_role_roundtrips_with_zero_and_maximum_positions() {
    use ExpressionRootRole::*;
    for position in [0, u32::MAX] {
        let roles = [
            ScanResidual {
                predicate: position,
            },
            ScanDerived { derived: position },
            FilterPredicate {
                predicate: position,
            },
            ProjectOutput {
                expression: position,
            },
            AggregateGroup { group: position },
            AggregateArgument {
                call: position,
                argument: position,
            },
            AggregateOrder {
                call: position,
                key: position,
            },
            JoinKey {
                key: position,
                side: JoinSide::Left,
            },
            HashJoinResidual,
            NestLoopPredicate,
            SortOrder { key: position },
            SortPartition { key: position },
            TopNOrder { key: position },
            TopNGroup { group: position },
            TopNStateArgument {
                call: position,
                argument: position,
            },
            TopNStateOrder {
                call: position,
                key: position,
            },
            WindowPartition { key: position },
            WindowOrder { key: position },
            WindowCall { call: position },
            ValuesCell {
                row: position,
                column: position,
            },
            SeriesStart,
            SeriesStop,
            SeriesStep,
            TableFunctionArgument { argument: position },
            UnpivotConstant {
                mapping: position,
                constant: position,
            },
            ChangePredicate { event: position },
            ChangeAssignment {
                event: position,
                assignment: position,
            },
            FinishUnpivotConstant {
                mapping: position,
                constant: position,
            },
        ];
        assert_eq!(roles.len(), 28);
        for role in roles.into_iter().chain([JoinKey {
            key: position,
            side: JoinSide::Right,
        }]) {
            let site = ExpressionRootSite {
                node: NodeId::new(position),
                role,
            };
            let encoded = encode_site(site);
            assert_eq!(encoded.node_id, Some(position));
            assert_eq!(decode_site(&encoded).unwrap(), site);
        }
    }
}
#[test]
fn every_control_and_guard_variant_roundtrips() {
    for shape in [
        ControlShape::Eager,
        ControlShape::TypeOnly,
        ControlShape::LambdaBody,
        ControlShape::Conjunction,
        ControlShape::Disjunction,
        ControlShape::If,
        ControlShape::Coalesce,
        ControlShape::Case {
            simple: true,
            arms: u32::MAX,
            has_else: true,
        },
        ControlShape::HigherOrder {
            body_ordinal: u32::MAX,
            body_demand: EvaluationDemand::TruthOnly,
        },
        ControlShape::HigherOrder {
            body_ordinal: 0,
            body_demand: EvaluationDemand::Value,
        },
    ] {
        assert_eq!(decode_shape(&encode_shape(shape)).unwrap(), shape);
    }
    for owner in [0, u32::MAX] {
        for kind in [
            GuardKind::IfThen,
            GuardKind::IfElse,
            GuardKind::CaseElse,
            GuardKind::LambdaInvocation,
            GuardKind::CoalesceAfterNull { ordinal: u32::MAX },
            GuardKind::CaseWhen { arm: 0 },
            GuardKind::CaseThen { arm: u32::MAX },
        ] {
            let guard = DomainGuard {
                owner: ExpressionUseId::new(owner),
                kind,
            };
            assert_eq!(decode_guard(&encode_guard(guard)).unwrap(), guard);
        }
    }
}

#[test]
fn absent_mandatory_ids_and_oneofs_never_default_to_zero_or_eager() {
    let (fragment, roots) = binary_fixture();
    let encoded = encode_expression_control(&roots, &Control::default()).unwrap();
    type MissingFieldMutation = (&'static str, fn(&mut wire::ExpressionControl));
    let mutations: &[MissingFieldMutation] = &[
        ("use domain is missing", |dto| dto.uses[0].domain_id = None),
        ("use definition is missing", |dto| {
            dto.uses[0].definition_id = None
        }),
        ("use control is missing", |dto| dto.uses[0].control = None),
        ("control kind is missing", |dto| {
            dto.uses[0].control.as_mut().unwrap().kind = None
        }),
        ("root site is missing", |dto| dto.roots[0].site = None),
        ("root use is missing", |dto| dto.roots[0].use_id = None),
        ("root node is missing", |dto| {
            dto.roots[0].site.as_mut().unwrap().node_id = None
        }),
        ("root role is missing", |dto| {
            dto.roots[0].site.as_mut().unwrap().role = None
        }),
    ];
    for (message, mutate) in mutations {
        let mut forged = encoded.clone();
        mutate(&mut forged);
        assert_eq!(
            decode_expression_control(&fragment, &forged, &Control::default()).unwrap_err(),
            ControlCodecError::InvalidShape(message)
        );
    }
    let (fragment, roots) = branch_fixture(true);
    let encoded = encode_expression_control(&roots, &Control::default()).unwrap();
    for (missing_owner, message) in [
        (true, "guard owner is missing"),
        (false, "guard kind is missing"),
    ] {
        let mut forged = encoded.clone();
        let guard = forged
            .domains
            .iter_mut()
            .find_map(|domain| domain.guard.as_mut())
            .unwrap();
        if missing_owner {
            guard.owner_use_id = None;
        } else {
            guard.kind = None;
        }
        assert_eq!(
            decode_expression_control(&fragment, &forged, &Control::default()).unwrap_err(),
            ControlCodecError::InvalidShape(message)
        );
    }
}
#[test]
fn unspecified_and_unknown_enums_fail_closed() {
    let (fragment, roots) = binary_fixture();
    let encoded = encode_expression_control(&roots, &Control::default()).unwrap();
    for unknown in [0, -1, i32::MAX] {
        let mut forged = encoded.clone();
        forged.uses[0].demand = unknown;
        assert_eq!(
            decode_expression_control(&fragment, &forged, &Control::default()).unwrap_err(),
            ControlCodecError::InvalidShape("unknown or unspecified evaluation demand")
        );
        forged = encoded.clone();
        forged.uses[0].control = Some(wire::ControlShape {
            kind: Some(wire::control_shape::Kind::Simple(unknown)),
        });
        assert_eq!(
            decode_expression_control(&fragment, &forged, &Control::default()).unwrap_err(),
            ControlCodecError::InvalidShape("unknown or unspecified control shape")
        );
        let higher = wire::ControlShape {
            kind: Some(wire::control_shape::Kind::HigherOrder(
                wire::HigherOrderControl {
                    body_ordinal: 0,
                    body_demand: unknown,
                },
            )),
        };
        assert_eq!(
            decode_shape(&higher).unwrap_err(),
            ControlCodecError::InvalidShape("unknown or unspecified evaluation demand")
        );
        let guard = wire::DomainGuard {
            owner_use_id: Some(0),
            kind: Some(wire::domain_guard::Kind::Simple(unknown)),
        };
        assert_eq!(
            decode_guard(&guard).unwrap_err(),
            ControlCodecError::InvalidShape("unknown or unspecified guard kind")
        );
        let site = wire::RootSite {
            node_id: Some(0),
            role: Some(wire::root_site::Role::JoinKey(wire::JoinKeyRoot {
                key: 0,
                side: unknown,
            })),
        };
        assert_eq!(
            decode_site(&site).unwrap_err(),
            ControlCodecError::InvalidShape("unknown or unspecified join side")
        );
    }
}

#[test]
fn duplicate_dangling_shared_and_reordered_use_graphs_keep_typed_rejections() {
    let (fragment, roots) = binary_fixture();
    let encoded = encode_expression_control(&roots, &Control::default()).unwrap();
    let mut forged = encoded.clone();
    forged.domains.push(forged.domains[0]);
    assert_eq!(
        decode_expression_control(&fragment, &forged, &Control::default()).unwrap_err(),
        ControlCodecError::Flow(ExpressionControlFlowError::DuplicateIdentity)
    );
    forged = encoded.clone();
    forged.uses.push(forged.uses[0].clone());
    assert_eq!(
        decode_expression_control(&fragment, &forged, &Control::default()).unwrap_err(),
        ControlCodecError::Flow(ExpressionControlFlowError::DuplicateIdentity)
    );
    for field in [0, 1, 2] {
        forged = encoded.clone();
        match field {
            0 => forged.uses[0].definition_id = Some(12345),
            1 => forged.uses[0].domain_id = Some(12345),
            _ => forged.uses[0].argument_use_ids[0] = 12345,
        }
        assert_eq!(
            decode_expression_control(&fragment, &forged, &Control::default()).unwrap_err(),
            ControlCodecError::Flow(ExpressionControlFlowError::InvalidReference)
        );
    }
    forged = encoded.clone();
    forged.uses[0].argument_use_ids = vec![7, 7];
    assert_eq!(
        decode_expression_control(&fragment, &forged, &Control::default()).unwrap_err(),
        ControlCodecError::Flow(ExpressionControlFlowError::SharedUse)
    );
    forged = encoded.clone();
    forged.uses[0].argument_use_ids.reverse();
    assert_eq!(
        decode_expression_control(&fragment, &forged, &Control::default()).unwrap_err(),
        ControlCodecError::Roots(RootUseBindingError::WrongArguments)
    );
    forged = encoded.clone();
    forged.roots[0].use_id = Some(12345);
    assert_eq!(
        decode_expression_control(&fragment, &forged, &Control::default()).unwrap_err(),
        ControlCodecError::Roots(RootUseBindingError::InvalidUse)
    );
    forged = encoded.clone();
    forged.roots.clear();
    assert_eq!(
        decode_expression_control(&fragment, &forged, &Control::default()).unwrap_err(),
        ControlCodecError::Roots(RootUseBindingError::IncompleteCoverage)
    );
    let (fragment, roots) = many_roots_fixture(2);
    let mut forged = encode_expression_control(&roots, &Control::default()).unwrap();
    forged.roots[1].use_id = forged.roots[0].use_id;
    assert_eq!(
        decode_expression_control(&fragment, &forged, &Control::default()).unwrap_err(),
        ControlCodecError::Roots(RootUseBindingError::SharedUse)
    );
    let mut forged = encode_expression_control(&roots, &Control::default()).unwrap();
    forged.roots[1].site = forged.roots[0].site;
    assert_eq!(
        decode_expression_control(&fragment, &forged, &Control::default()).unwrap_err(),
        ControlCodecError::Roots(RootUseBindingError::DuplicateSite)
    );
}
fn many_roots_fixture(count: usize) -> (Fragment, PhysicalRootUses) {
    let mut builder = FragmentBuilder::new(FragmentId::new(704));
    let node = builder.reserve_node_id().unwrap();
    let expr = boolean(&mut builder, node, true);
    let fragment = finish_values(builder, node, &vec![expr; count]);
    let roots = leaf_root_uses(&fragment);
    (fragment, roots)
}
#[test]
fn each_projection_and_validation_phase_observes_midwork_typed_control_failures() {
    let (fragment, roots) = many_roots_fixture(300);
    let encoded = encode_expression_control(&roots, &Control::default()).unwrap();
    for phase in [
        CompilePhase::Encode,
        CompilePhase::Decode,
        CompilePhase::Validate,
    ] {
        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                failure: Some((phase, failure.clone())),
                ..Control::default()
            };
            let error = if phase == CompilePhase::Encode {
                encode_expression_control(&roots, &control).unwrap_err()
            } else {
                decode_expression_control(&fragment, &encoded, &control).unwrap_err()
            };
            assert_eq!(error, ControlCodecError::Control(failure));
            let observations = control.observations.lock().unwrap();
            assert!(observations.contains(&(phase, 256)));
            assert!(observations.iter().all(|(_, units)| *units <= 256));
        }
    }
}

#[test]
fn count_and_edge_limits_are_checked_before_malformed_dto_projection() {
    let (fragment, _) = binary_fixture();
    let too_many = ControlCodecError::Flow(ExpressionControlFlowError::TooManyItems);
    for count in [MAX_CONTROL_USE_REFERENCES, MAX_CONTROL_USE_REFERENCES + 1] {
        let dto = wire::ExpressionControl {
            uses: vec![wire::ExpressionUse::default(); count],
            ..Default::default()
        };
        let error = decode_expression_control(&fragment, &dto, &Control::default()).unwrap_err();
        if count > MAX_CONTROL_USE_REFERENCES {
            assert_eq!(error, too_many);
        } else {
            assert_eq!(
                error,
                ControlCodecError::InvalidShape("use domain is missing")
            );
        }
    }
    for dto in [
        wire::ExpressionControl {
            domains: vec![wire::EvaluationDomain::default(); MAX_CONTROL_DEFINITIONS + 1],
            ..Default::default()
        },
        wire::ExpressionControl {
            roots: vec![wire::RootBinding::default(); MAX_CONTROL_USE_REFERENCES + 1],
            ..Default::default()
        },
        wire::ExpressionControl {
            uses: vec![wire::ExpressionUse {
                argument_use_ids: vec![0; MAX_CONTROL_USE_REFERENCES],
                ..Default::default()
            }],
            ..Default::default()
        },
    ] {
        assert_eq!(
            decode_expression_control(&fragment, &dto, &Control::default()).unwrap_err(),
            too_many
        );
    }
    // Equality at the reference bound reaches mandatory-field validation. This
    // malformed probe does not assert that its graph is a valid program.
    let dto = wire::ExpressionControl {
        uses: vec![wire::ExpressionUse {
            argument_use_ids: vec![0; MAX_CONTROL_USE_REFERENCES - 1],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_eq!(
        decode_expression_control(&fragment, &dto, &Control::default()).unwrap_err(),
        ControlCodecError::InvalidShape("use domain is missing")
    );
}

#[test]
fn valid_reference_budget_boundary_roundtrips_and_one_more_edge_fails_before_projection() {
    let count = MAX_CONTROL_USE_REFERENCES / 2 - 1;
    let mut builder = FragmentBuilder::new(FragmentId::new(705));
    let node = builder.reserve_node_id().unwrap();
    let leaf = boolean(&mut builder, node, true);
    let root = builder
        .add_expression(
            node,
            ValueType::new(DataType::Boolean, false),
            ExprKind::Conjunction {
                args: vec![leaf; count].into_boxed_slice(),
            },
        )
        .unwrap();
    let fragment = finish_values(builder, node, &[root, leaf]);
    let domain = EvaluationDomainId::new(u32::MAX);
    let arguments = (1..=count as u32).collect::<Vec<_>>();
    let mut uses = vec![invocation(
        0,
        root,
        domain,
        EvaluationDemand::Value,
        ControlShape::Conjunction,
        &arguments,
    )];
    uses.extend(arguments.iter().map(|id| {
        invocation(
            *id,
            leaf,
            domain,
            EvaluationDemand::Value,
            ControlShape::Eager,
            &[],
        )
    }));
    // A second independent root makes uses + edges exactly the even bound.
    uses.push(invocation(
        u32::MAX,
        leaf,
        domain,
        EvaluationDemand::Value,
        ControlShape::Eager,
        &[],
    ));
    assert_eq!(uses.len() + arguments.len(), MAX_CONTROL_USE_REFERENCES);
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let roots = PhysicalRootUses::try_new(
        &fragment,
        flow,
        vec![
            (
                ExpressionRootSite {
                    node,
                    role: ExpressionRootRole::ValuesCell { row: 0, column: 0 },
                },
                ExpressionUseId::new(0),
            ),
            (
                ExpressionRootSite {
                    node,
                    role: ExpressionRootRole::ValuesCell { row: 0, column: 1 },
                },
                ExpressionUseId::new(u32::MAX),
            ),
        ],
        &Control::default(),
    )
    .unwrap();
    let mut encoded = encode_expression_control(&roots, &Control::default()).unwrap();
    assert_eq!(
        encoded.uses.len()
            + encoded
                .uses
                .iter()
                .map(|item| item.argument_use_ids.len())
                .sum::<usize>(),
        MAX_CONTROL_USE_REFERENCES
    );
    assert_eq!(
        decode_expression_control(&fragment, &encoded, &Control::default()).unwrap(),
        roots
    );
    encoded.uses[0].argument_use_ids.push(u32::MAX);
    let control = Control::default();
    assert_eq!(
        decode_expression_control(&fragment, &encoded, &control).unwrap_err(),
        ControlCodecError::Flow(ExpressionControlFlowError::TooManyItems)
    );
    assert_eq!(
        *control.observations.lock().unwrap(),
        [(CompilePhase::Decode, 0), (CompilePhase::Decode, 0)]
    );
}

#[test]
fn domain_and_root_count_equality_reaches_shape_checks_but_overflow_does_not_project() {
    let (fragment, _) = binary_fixture();
    let dto = wire::ExpressionControl {
        domains: vec![wire::EvaluationDomain::default(); MAX_CONTROL_DEFINITIONS],
        ..Default::default()
    };
    assert_eq!(
        decode_expression_control(&fragment, &dto, &Control::default()).unwrap_err(),
        ControlCodecError::Flow(ExpressionControlFlowError::DuplicateIdentity)
    );
    let dto = wire::ExpressionControl {
        roots: vec![wire::RootBinding::default(); MAX_CONTROL_USE_REFERENCES],
        ..Default::default()
    };
    assert_eq!(
        decode_expression_control(&fragment, &dto, &Control::default()).unwrap_err(),
        ControlCodecError::InvalidShape("root site is missing")
    );
    // These equality probes are malformed DTOs, not accepted programs. Each
    // over-limit count must reject before projecting a single dynamic item.
    for dto in [
        wire::ExpressionControl {
            domains: vec![wire::EvaluationDomain::default(); MAX_CONTROL_DEFINITIONS + 1],
            ..Default::default()
        },
        wire::ExpressionControl {
            uses: vec![wire::ExpressionUse::default(); MAX_CONTROL_USE_REFERENCES + 1],
            ..Default::default()
        },
        wire::ExpressionControl {
            roots: vec![wire::RootBinding::default(); MAX_CONTROL_USE_REFERENCES + 1],
            ..Default::default()
        },
    ] {
        let control = Control::default();
        assert_eq!(
            decode_expression_control(&fragment, &dto, &control).unwrap_err(),
            ControlCodecError::Flow(ExpressionControlFlowError::TooManyItems)
        );
        assert_eq!(
            *control.observations.lock().unwrap(),
            [(CompilePhase::Decode, 0), (CompilePhase::Decode, 0)]
        );
    }
}

#[path = "failure_tail_tests.rs"]
mod failure_tail_tests;

#[test]
fn null_aware_root_sites_keep_role_and_ordinal_and_reject_missing_side() {
    for role in [
        ExpressionRootRole::NaaValueEqualKey {
            key: 7,
            side: JoinSide::Left,
        },
        ExpressionRootRole::NaaValueEqualKey {
            key: 7,
            side: JoinSide::Right,
        },
        ExpressionRootRole::NaaScopeKey {
            key: 7,
            side: JoinSide::Left,
        },
        ExpressionRootRole::NaaScopeKey {
            key: 7,
            side: JoinSide::Right,
        },
        ExpressionRootRole::NaaEligibility,
    ] {
        let site = ExpressionRootSite {
            node: NodeId::new(3),
            role,
        };
        let mut wire_site = encode_site(site);
        assert_eq!(decode_site(&wire_site).unwrap(), site);
        match wire_site.role.as_mut() {
            Some(wire::root_site::Role::NaaValueEqualKey(key))
            | Some(wire::root_site::Role::NaaScopeKey(key)) => {
                key.side = 0;
                assert!(matches!(
                    decode_site(&wire_site),
                    Err(ControlCodecError::InvalidShape(_))
                ));
            }
            _ => (),
        }
    }
}
