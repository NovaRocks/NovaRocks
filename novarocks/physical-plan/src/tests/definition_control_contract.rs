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
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDemand, EvaluationRegionId,
    ExpressionControlFlow, ExpressionControlFlowError, ExpressionEffectContext,
    ExpressionEvaluationRegion, ExpressionInvocation, ExpressionUseId, GuardKind,
    PureCompileControl, RegionGuard, control_argument_semantics,
};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
struct Fixture {
    fragment: Fragment,
    node: NodeId,
    root: ExprId,
    definitions: Vec<ExprId>,
}
fn fixture(make: impl FnOnce(&mut FragmentBuilder, NodeId) -> (ExprId, Vec<ExprId>)) -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(991));
    let node = builder.reserve_node_id().unwrap();
    let (root, definitions) = make(&mut builder, node);
    let value_type = builder.expressions().get(root).unwrap().ty.clone();
    let value = builder
        .add_value(
            value_type,
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
            output_properties: singleton(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([root])]),
            },
        })
        .unwrap();
    // Every test reaches the control boundary with a checked real definition.
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    validate_fragment_definition(&fragment).unwrap();
    Fixture {
        fragment,
        node,
        root,
        definitions,
    }
}
fn integer(builder: &mut FragmentBuilder, node: NodeId, value: i64) -> ExprId {
    builder
        .add_expression(
            node,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(value)),
        )
        .unwrap()
}
fn boolean(builder: &mut FragmentBuilder, node: NodeId, value: bool) -> ExprId {
    builder
        .add_expression(
            node,
            ty(DataType::Boolean, false),
            ExprKind::Literal(LiteralValue::Boolean(value)),
        )
        .unwrap()
}
fn function(arguments: Box<[FunctionArgumentType]>, result: ValueType) -> BoundFunction {
    BoundFunction {
        legacy_metadata: Some(crate::LegacyBindingMetadata {
            semantic_parameters: Box::default(),
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: novarocks_type_contract::FunctionIntrinsicRowError::NoRowError,
        }),
        function_id: FunctionId::try_new("fixture/definition-control/exact-shape").unwrap(),
        overload: FunctionOverloadId::try_new("fixture/definition-control/selected-shape").unwrap(),
        kind: FunctionKind::Scalar,
        argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(arguments),
        result_type: result,
    }
}

struct UseTree {
    id: ExpressionUseId,
    definition: ExprId,
    control: ControlShape,
    arguments: Vec<UseTree>,
}
fn leaf(id: u32, definition: ExprId) -> UseTree {
    tree(id, definition, ControlShape::Eager, vec![])
}
fn tree(id: u32, definition: ExprId, control: ControlShape, arguments: Vec<UseTree>) -> UseTree {
    UseTree {
        id: ExpressionUseId::new(id),
        definition,
        control,
        arguments,
    }
}
fn graph(
    fixture: &Fixture,
    root: UseTree,
) -> Result<ExpressionControlFlow<ExprId>, ExpressionControlFlowError> {
    fn collect(
        tree: UseTree,
        domain: EvaluationRegionId,
        demand: EvaluationDemand,
        domains: &mut Vec<ExpressionEvaluationRegion>,
        uses: &mut Vec<ExpressionInvocation<ExprId>>,
    ) -> Result<(), ExpressionControlFlowError> {
        let children = tree.arguments.iter().map(|child| child.id).collect();
        let count = tree.arguments.len();
        for (ordinal, child) in tree.arguments.into_iter().enumerate() {
            let (child_demand, guard) =
                control_argument_semantics(tree.control, count, ordinal, demand)?;
            let child_region = if let Some(kind) = guard {
                let id = EvaluationRegionId::new(domains.len() as u32);
                domains.push(ExpressionEvaluationRegion {
                    id,
                    parent: Some(domain),
                    guard: Some(RegionGuard {
                        owner: tree.id,
                        kind,
                    }),
                });
                id
            } else {
                domain
            };
            collect(child, child_region, child_demand, domains, uses)?;
        }
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: tree.id,
                region: domain,
                demand,
            },
            definition: tree.definition,
            control: tree.control,
            arguments: children,
        });
        Ok(())
    }
    let domain = EvaluationRegionId::new(0);
    let mut domains = vec![ExpressionEvaluationRegion {
        id: domain,
        parent: None,
        guard: None,
    }];
    let mut uses = Vec::new();
    collect(
        root,
        domain,
        EvaluationDemand::Value,
        &mut domains,
        &mut uses,
    )?;
    ExpressionControlFlow::try_new(
        domains,
        uses,
        fixture.fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
}
fn bind(
    fixture: &Fixture,
    graph: ExpressionControlFlow<ExprId>,
) -> Result<PhysicalRootUses, RootUseBindingError> {
    PhysicalRootUses::try_new(
        &fixture.fragment,
        graph,
        vec![(
            ExpressionRootSite {
                node: fixture.node,
                role: ExpressionRootRole::ValuesCell { row: 0, column: 0 },
            },
            ExpressionUseId::new(7),
        )],
        &Control,
    )
}

#[test]
fn literal_cannot_invent_a_runtime_child_from_an_existing_valid_definition() {
    let fixture = fixture(|builder, node| {
        let root = integer(builder, node, 1);
        (root, vec![root])
    });
    bind(&fixture, graph(&fixture, leaf(7, fixture.root)).unwrap()).unwrap();
    let forged = graph(
        &fixture,
        tree(
            7,
            fixture.root,
            ControlShape::Eager,
            vec![leaf(1000, fixture.definitions[0])],
        ),
    )
    .unwrap();
    assert_eq!(
        bind(&fixture, forged).unwrap_err(),
        RootUseBindingError::WrongArguments
    );
}

fn binary_fixture(repeated: bool) -> Fixture {
    fixture(|builder, node| {
        let left = integer(builder, node, 11);
        let right = if repeated {
            left
        } else {
            integer(builder, node, 22)
        };
        let root = builder
            .add_expression(
                node,
                ty(DataType::Int64, false),
                ExprKind::Binary {
                    allow_throw_exception: Some(novarocks_type_contract::SemanticParameterRef {
                        id: novarocks_type_contract::SemanticParameterId::new(0),
                        expected_key:
                            novarocks_type_contract::SemanticParameterKey::AllowThrowException,
                    }),
                    left,
                    op: BinaryOperator::Subtract,
                    right,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                },
            )
            .unwrap();
        (root, vec![left, right])
    })
}
#[test]
fn binary_children_must_match_the_actual_ordered_definitions() {
    let fixture = binary_fixture(false);
    let correct = graph(
        &fixture,
        tree(
            7,
            fixture.root,
            ControlShape::Eager,
            vec![
                leaf(1000, fixture.definitions[0]),
                leaf(u32::MAX, fixture.definitions[1]),
            ],
        ),
    )
    .unwrap();
    bind(&fixture, correct).unwrap();
    let swapped = graph(
        &fixture,
        tree(
            7,
            fixture.root,
            ControlShape::Eager,
            vec![
                leaf(1000, fixture.definitions[1]),
                leaf(u32::MAX, fixture.definitions[0]),
            ],
        ),
    )
    .unwrap();
    assert_eq!(
        bind(&fixture, swapped).unwrap_err(),
        RootUseBindingError::WrongArguments
    );
}

#[test]
fn repeated_definition_preserves_two_occurrences_and_cannot_share_one_use() {
    let fixture = binary_fixture(true);
    let flow = graph(
        &fixture,
        tree(
            7,
            fixture.root,
            ControlShape::Eager,
            vec![
                leaf(1000, fixture.definitions[0]),
                leaf(u32::MAX, fixture.definitions[1]),
            ],
        ),
    )
    .unwrap();
    let bound = bind(&fixture, flow.clone()).unwrap();
    let root = &bound.flow().uses()[&ExpressionUseId::new(7)];
    assert_eq!(
        root.arguments.as_ref(),
        [ExpressionUseId::new(1000), ExpressionUseId::new(u32::MAX)]
    );
    assert_eq!(
        bound.flow().uses()[&root.arguments[0]].definition,
        bound.flow().uses()[&root.arguments[1]].definition
    );
    let mut forged = flow.uses().values().cloned().collect::<Vec<_>>();
    forged
        .iter_mut()
        .find(|invocation| invocation.context.use_id == ExpressionUseId::new(7))
        .unwrap()
        .arguments = Box::from([ExpressionUseId::new(1000), ExpressionUseId::new(1000)]);
    assert_eq!(
        ExpressionControlFlow::try_new(
            flow.regions().values().copied().collect(),
            forged,
            fixture.fragment.expressions(),
            CompilePhase::Validate,
            &Control
        )
        .unwrap_err(),
        ExpressionControlFlowError::SharedUse
    );
}

#[test]
fn case_shape_metadata_and_ordered_when_then_occurrences_follow_actual_case() {
    let fixture = fixture(|builder, node| {
        let first_when = boolean(builder, node, true);
        let first_then = integer(builder, node, 11);
        let second_when = boolean(builder, node, false);
        let second_then = integer(builder, node, 22);
        let otherwise = integer(builder, node, 33);
        let root = builder
            .add_expression(
                node,
                ty(DataType::Int64, false),
                ExprKind::Case {
                    operand: None,
                    when_then: Box::from([(first_when, first_then), (second_when, second_then)]),
                    else_expr: Some(otherwise),
                },
            )
            .unwrap();
        (
            root,
            vec![first_when, first_then, second_when, second_then, otherwise],
        )
    });
    let shape = ControlShape::Case {
        simple: false,
        arms: 2,
        has_else: true,
    };
    let children = || {
        fixture
            .definitions
            .iter()
            .enumerate()
            .map(|(ordinal, definition)| leaf(1000 + ordinal as u32, *definition))
            .collect()
    };
    bind(
        &fixture,
        graph(&fixture, tree(7, fixture.root, shape, children())).unwrap(),
    )
    .unwrap();
    let forged_shape = ControlShape::Case {
        simple: true,
        arms: 2,
        has_else: false,
    };
    let forged = graph(&fixture, tree(7, fixture.root, forged_shape, children())).unwrap();
    assert_eq!(
        bind(&fixture, forged).unwrap_err(),
        RootUseBindingError::WrongControl
    );
    let swapped = [2, 1, 0, 3, 4]
        .into_iter()
        .enumerate()
        .map(|(ordinal, definition)| leaf(1000 + ordinal as u32, fixture.definitions[definition]))
        .collect();
    let forged = graph(&fixture, tree(7, fixture.root, shape, swapped)).unwrap();
    assert_eq!(
        bind(&fixture, forged).unwrap_err(),
        RootUseBindingError::WrongArguments
    );
}

#[test]
fn type_only_keeps_checked_static_arguments_without_runtime_children() {
    let fixture = fixture(|builder, node| {
        let argument = integer(builder, node, 11);
        let result = ty(DataType::Boolean, false);
        let root = builder
            .add_expression(
                node,
                result.clone(),
                ExprKind::FunctionCall {
                    function: function(
                        Box::from([FunctionArgumentType::Value(ty(DataType::Int64, false))]),
                        result,
                    ),
                    args: Box::from([argument]),
                },
            )
            .unwrap();
        (root, vec![argument])
    });
    // Exact installed TypeOnly ownership remains the pure compiler's obligation;
    // this test checks the correspondence contract and static definition only.
    let bound = bind(
        &fixture,
        graph(
            &fixture,
            tree(7, fixture.root, ControlShape::TypeOnly, vec![]),
        )
        .unwrap(),
    )
    .unwrap();
    let ExprKind::FunctionCall { args, .. } = &fixture
        .fragment
        .expressions()
        .get(fixture.root)
        .unwrap()
        .kind
    else {
        unreachable!();
    };
    assert_eq!(args.as_ref(), fixture.definitions.as_slice());
    assert!(
        bound.flow().uses()[&ExpressionUseId::new(7)]
            .arguments
            .is_empty()
    );
    assert_eq!(
        graph(
            &fixture,
            tree(
                7,
                fixture.root,
                ControlShape::TypeOnly,
                vec![leaf(1000, fixture.definitions[0])]
            )
        )
        .unwrap_err(),
        ExpressionControlFlowError::InvalidControlShape
    );
}

fn higher_fixture() -> Fixture {
    fixture(|builder, node| {
        let ordinary = boolean(builder, node, true);
        let lambda = builder.reserve_expression_id().unwrap();
        let bool_type = ty(DataType::Boolean, false);
        let body = builder
            .add_expression_in_scope(
                node,
                Some(lambda),
                bool_type.clone(),
                ExprKind::LambdaParameter { lambda, ordinal: 0 },
            )
            .unwrap();
        builder
            .insert_expression(ExprNode {
                id: lambda,
                owner: node,
                lambda_scope: None,
                ty: bool_type.clone(),
                kind: ExprKind::Lambda {
                    parameter_types: Box::from([bool_type.clone()]),
                    body,
                },
            })
            .unwrap();
        let root = builder
            .add_expression(
                node,
                bool_type.clone(),
                ExprKind::FunctionCall {
                    function: function(
                        Box::from([
                            FunctionArgumentType::Value(bool_type.clone()),
                            FunctionArgumentType::Lambda {
                                parameter_types:
                                    novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                                        Box::from([bool_type.clone()]),
                                    ),
                                result_type: bool_type.clone(),
                            },
                        ]),
                        bool_type,
                    ),
                    args: Box::from([ordinary, lambda]),
                },
            )
            .unwrap();
        (root, vec![ordinary, lambda, body])
    })
}
fn higher_tree(fixture: &Fixture, body_ordinal: u32, wrapper: ControlShape) -> UseTree {
    tree(
        7,
        fixture.root,
        ControlShape::HigherOrder {
            body_ordinal,
            body_demand: EvaluationDemand::TruthOnly,
        },
        vec![
            leaf(1000, fixture.definitions[0]),
            tree(
                2000,
                fixture.definitions[1],
                wrapper,
                vec![leaf(u32::MAX, fixture.definitions[2])],
            ),
        ],
    )
}

#[test]
fn truth_only_lambda_wrapper_and_body_share_the_exact_invocation_guard_and_demand() {
    let fixture = higher_fixture();
    let flow = graph(&fixture, higher_tree(&fixture, 1, ControlShape::LambdaBody)).unwrap();
    let bound = bind(&fixture, flow).unwrap();
    let lambda = &bound.flow().uses()[&ExpressionUseId::new(2000)];
    let body = &bound.flow().uses()[&ExpressionUseId::new(u32::MAX)];
    assert_eq!(lambda.context.region, body.context.region);
    assert_eq!(lambda.context.demand, EvaluationDemand::TruthOnly);
    assert_eq!(body.context.demand, EvaluationDemand::TruthOnly);
    assert_eq!(
        bound.flow().regions()[&lambda.context.region].guard,
        Some(RegionGuard {
            owner: ExpressionUseId::new(7),
            kind: GuardKind::LambdaInvocation
        })
    );
}

#[test]
fn structurally_legal_higher_body_ordinal_and_eager_lambda_wrapper_are_rejected() {
    let fixture = higher_fixture();
    let wrong_ordinal =
        graph(&fixture, higher_tree(&fixture, 0, ControlShape::LambdaBody)).unwrap();
    assert_eq!(
        bind(&fixture, wrong_ordinal).unwrap_err(),
        RootUseBindingError::WrongArguments
    );
    let eager_wrapper = graph(&fixture, higher_tree(&fixture, 1, ControlShape::Eager)).unwrap();
    assert_eq!(
        bind(&fixture, eager_wrapper).unwrap_err(),
        RootUseBindingError::WrongControl
    );
}
