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
use novarocks_expression::ComparisonSite;
use novarocks_functions::ComparisonOperator;

const OPERATORS: [(BinaryOperator, ComparisonOperator); 4] = [
    (BinaryOperator::Lt, ComparisonOperator::Lt),
    (BinaryOperator::LtEq, ComparisonOperator::Le),
    (BinaryOperator::Gt, ComparisonOperator::Gt),
    (BinaryOperator::GtEq, ComparisonOperator::Ge),
];

#[test]
fn four_ordered_operators_keep_exact_types_order_domain_and_consumer_demand() {
    let functions = functions();
    for (physical, expected) in OPERATORS {
        for carrier in [DataType::Boolean, DataType::Int64, DataType::Float64] {
            for (ln, rn) in [(false, false), (false, true), (true, false), (true, true)] {
                for mode in [Mode::Project, Mode::Filter] {
                    let left = FunctionValueType::new(carrier.clone(), ln);
                    let right = FunctionValueType::new(carrier.clone(), rn);
                    let fixture = fixture(
                        &functions,
                        Shape::Binary(physical),
                        mode,
                        left.clone(),
                        right.clone(),
                        OutputClaim::Accurate,
                        None,
                    )
                    .unwrap();
                    let ordered = fixture.ordered.clone();
                    let source = package(&functions, fixture);
                    let program = compile(source.clone(), &functions, &Control).unwrap();
                    let compiled = unit(&program);
                    let calls = compiled;
                    let flow = compiled.control_flow();
                    let arena = compiled.definitions();
                    let use_id = occurrence(&program);
                    let use_ = &flow.uses()[&use_id];
                    assert_eq!(use_.control, ControlShape::Eager);
                    assert_eq!(
                        use_.context.demand,
                        if matches!(mode, Mode::Filter) {
                            EvaluationDemand::TruthOnly
                        } else {
                            EvaluationDemand::Value
                        }
                    );
                    let (a, b) = match (physical, arena.node(use_.definition).unwrap().kind()) {
                        (BinaryOperator::Lt, StaticExprKind::Lt(a, b))
                        | (BinaryOperator::LtEq, StaticExprKind::Le(a, b))
                        | (BinaryOperator::Gt, StaticExprKind::Gt(a, b))
                        | (BinaryOperator::GtEq, StaticExprKind::Ge(a, b)) => (*a, *b),
                        _ => panic!("the exact ordered opcode must survive lowering"),
                    };
                    assert_eq!(use_.arguments.len(), 2);
                    assert_ne!(use_.arguments[0], use_.arguments[1]);
                    for (ordinal, definition) in [a, b].into_iter().enumerate() {
                        let child = &flow.uses()[&use_.arguments[ordinal]];
                        assert_eq!(child.definition, definition);
                        assert_eq!(child.context.region, use_.context.region);
                        assert_eq!(child.context.demand, EvaluationDemand::Value);
                        assert_eq!(
                            source_unit(&source).control_flow().uses()[&child.context.use_id]
                                .definition,
                            ordered[ordinal]
                        );
                    }
                    let occurrence = use_id;
                    let recipe = unit(&program)
                        .recipes()
                        .comparison(ComparisonSite::Binary(occurrence))
                        .unwrap();
                    assert_eq!(recipe.operator(), expected);
                    assert_eq!(recipe.left_type(), &left);
                    assert_eq!(recipe.right_type(), &right);
                    assert_eq!(recipe.nullable_result(), ln || rn);
                    assert!(
                        unit(&program)
                            .recipes()
                            .comparison(ComparisonSite::CaseWhen { occurrence, arm: 0 })
                            .is_none()
                    );
                    assert!(calls.calls().is_empty());
                    assert!(source_unit(&source).claims().is_empty());
                }
            }
        }
    }
}

#[test]
fn ordered_float_sources_keep_nan_payload_and_signed_zero_bits() {
    let functions = functions();
    for (physical, expected) in OPERATORS {
        for bits in [
            [0f64.to_bits(), (-0f64).to_bits()],
            [0x7ff8_0000_0000_0042, 0x7ff8_0000_0000_0043],
        ] {
            let ty = FunctionValueType::new(DataType::Float64, false);
            let source = package(
                &functions,
                fixture(
                    &functions,
                    Shape::Binary(physical),
                    Mode::Project,
                    ty.clone(),
                    ty.clone(),
                    OutputClaim::Accurate,
                    Some(bits),
                )
                .unwrap(),
            );
            let program = compile(source, &functions, &Control).unwrap();
            let values = program
                .units()
                .iter()
                .flat_map(|unit| unit.definitions().nodes().iter())
                .filter_map(|node| match node.kind() {
                    StaticExprKind::Constant(value) => Some(value.try_f64_bits().unwrap().unwrap()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(values, bits);
            let recipe = unit(&program)
                .recipes()
                .comparison(ComparisonSite::Binary(occurrence(&program)))
                .unwrap();
            assert_eq!(recipe.operator(), expected);
            assert_eq!(recipe.left_type(), &ty);
            assert_eq!(recipe.right_type(), &ty);
        }
    }
}

#[test]
fn physical_author_rejects_wrong_domains_temporal_decimal_and_result_claims() {
    let functions = functions();
    let uuid = FunctionValueType::try_from_legacy_declared_projection(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::Uuid,
    )
    .expect("valid temporary legacy value projection");
    let bad_pairs = [
        (
            uuid,
            FunctionValueType::new(DataType::FixedSizeBinary(16), true),
        ),
        (
            FunctionValueType::new(
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            FunctionValueType::new(
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("+02:00".into())),
                true,
            ),
        ),
        (
            FunctionValueType::new(DataType::Decimal128(18, 6), true),
            FunctionValueType::new(DataType::Decimal128(18, 5), true),
        ),
        (
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Float64, true),
        ),
    ];
    for (physical, _) in OPERATORS {
        for (left, right) in &bad_pairs {
            assert!(
                matches!(fixture(&functions, Shape::Binary(physical), Mode::Project, left.clone(), right.clone(), OutputClaim::Accurate, None), Err(error) if error.is_producer_defect())
            );
        }
        for claim in [OutputClaim::NonBoolean, OutputClaim::Narrow] {
            let ty = FunctionValueType::new(DataType::Int64, true);
            assert!(
                matches!(fixture(&functions, Shape::Binary(physical), Mode::Project, ty.clone(), ty, claim, None), Err(error) if error.is_producer_defect())
            );
        }
    }
}

#[test]
fn exact_uuid_and_successful_null_sources_keep_ordered_recipe_identity() {
    let functions = functions();
    for ty in [
        FunctionValueType::try_from_legacy_declared_projection(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .expect("valid temporary legacy value projection"),
        FunctionValueType::new(DataType::Null, true),
    ] {
        for (physical, expected) in OPERATORS {
            let source = package(
                &functions,
                fixture(
                    &functions,
                    Shape::Binary(physical),
                    Mode::Project,
                    ty.clone(),
                    ty.clone(),
                    OutputClaim::Accurate,
                    None,
                )
                .unwrap(),
            );
            let program = compile(source, &functions, &Control).unwrap();
            let _compiled = unit(&program);
            let occurrence = occurrence(&program);
            let recipe = unit(&program)
                .recipes()
                .comparison(ComparisonSite::Binary(occurrence))
                .unwrap();
            assert_eq!(recipe.operator(), expected);
            assert_eq!(recipe.left_type(), &ty);
            assert_eq!(recipe.right_type(), &ty);
            assert!(recipe.nullable_result());
        }
    }
    let ty = FunctionValueType::new(DataType::Int64, true);
    let source = package(
        &functions,
        fixture(
            &functions,
            Shape::SimpleCase,
            Mode::Project,
            ty.clone(),
            ty,
            OutputClaim::Accurate,
            None,
        )
        .unwrap(),
    );
    let program = compile(source, &functions, &Control).unwrap();
    let _compiled = unit(&program);
    let occurrence = occurrence(&program);
    for arm in 0..2 {
        assert_eq!(
            unit(&program)
                .recipes()
                .comparison(ComparisonSite::CaseWhen { occurrence, arm })
                .unwrap()
                .operator(),
            ComparisonOperator::Eq
        );
    }
}

#[test]
fn ordered_preparation_keeps_every_original_control_cause_without_retry() {
    let functions = functions();
    for (physical, _) in OPERATORS {
        let ty = FunctionValueType::new(DataType::Int64, true);
        let source = package(
            &functions,
            fixture(
                &functions,
                Shape::Binary(physical),
                Mode::Project,
                ty.clone(),
                ty,
                OutputClaim::Accurate,
                None,
            )
            .unwrap(),
        );
        let recorder = RefusingControl::new(CompileControlError::Cancelled, usize::MAX);
        compile(source.clone(), &functions, &recorder).unwrap();
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        assert!(trace.iter().all(|(_, units)| *units <= 256));
        for stop_at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = RefusingControl::new(cause.clone(), stop_at);
                assert!(
                    matches!(compile(source.clone(), &functions, &control), Err(FragmentCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..stop_at]);
            }
        }
    }
}

fn rand_operand_fixture(
    functions: &PureEngineFunctionCatalog,
    op: BinaryOperator,
    mode: Mode,
) -> Fixture {
    let mut constants = novarocks_physical_plan::ConstantPools::empty();
    let mut builder = FragmentBuilder::new(FragmentId::new(101));
    builder
        .add_values(
            NodeId::new(u32::MAX),
            Box::from([Box::default()]),
            Box::default(),
        )
        .unwrap();
    let request = FunctionBindingRequest {
        arguments: &[],
        logical_argument_count: 0,
        expected_result_type: None,
    };
    let bound = functions
        .metadata()
        .resolve_bound_user("rand", FunctionKind::Scalar, request, &Control)
        .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let FunctionResultType::Scalar(ty) = &selected.result_type else {
        panic!("actual RAND result")
    };
    let ty = ty.clone();
    let literal = builder
        .fixture_value_root(
            NodeId::new(44),
            ty.clone(),
            crate::test_constants::f64_bits(
                &mut constants,
                &ty,
                1f64.to_bits(),
                options().constants,
            ),
        )
        .unwrap();
    let input = builder
        .add_value(
            ty.clone(),
            ValueOrigin::Expr {
                node: NodeId::new(44),
                root: literal,
            },
        )
        .unwrap();
    builder
        .add_project(
            NodeId::new(44),
            NodeId::new(u32::MAX),
            Box::from([(literal, input)]),
            Box::from([input]),
        )
        .unwrap();
    let call = builder
        .fixture_definition(
            NodeId::new(0),
            ty.clone(),
            ExprKind::FunctionCall {
                function: BoundFunction {
                    function_id: bound.function_id,
                    overload: selected.overload.clone(),
                    kind: bound.kind,
                    argument_types: selected.argument_types.clone(),
                    result_type: ty.clone(),
                    legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
                        volatility: bound.semantics.volatility,
                        argument_evaluation: bound.semantics.argument_evaluation,
                        failure_behavior: bound.semantics.failure_behavior,
                        intrinsic_row_error: bound.semantics.intrinsic_row_error,
                        semantic_parameters: Box::default(),
                    }),
                },
                args: Box::default(),
            },
        )
        .unwrap();
    let value = builder
        .fixture_definition(NodeId::new(0), ty, ExprKind::Value(input))
        .unwrap();
    let result = FunctionValueType::new(DataType::Boolean, true);
    let expression = builder
        .fixture_definition(
            NodeId::new(0),
            result.clone(),
            ExprKind::Binary {
                left: call,
                op,
                right: value,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: None,
            },
        )
        .unwrap();
    let root = builder
        .fixture_occurrence(
            NodeId::new(0),
            expression,
            if matches!(mode, Mode::Filter) {
                EvaluationDemand::TruthOnly
            } else {
                EvaluationDemand::Value
            },
            RootScope::Port(0),
        )
        .unwrap();
    match mode {
        Mode::Project => {
            let output = builder
                .add_value(
                    result,
                    ValueOrigin::Expr {
                        node: NodeId::new(0),
                        root,
                    },
                )
                .unwrap();
            builder
                .add_project(
                    NodeId::new(0),
                    NodeId::new(44),
                    Box::from([(root, output)]),
                    Box::from([output]),
                )
                .unwrap();
        }
        Mode::Filter => {
            builder
                .add_filter(NodeId::new(0), NodeId::new(44), Box::from([root]))
                .unwrap();
        }
    }
    seal_units(
        &mut builder,
        functions,
        Some(&selected),
        Some(request),
        &[call],
    )
    .unwrap();
    Fixture {
        constants,
        fragment: builder
            .finish_definition(
                NodeId::new(0),
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
        expression,
        ordered: vec![call, value],
        rand_selected: Some(selected),
        rand_request: Some(request),
        rand_definitions: vec![call],
        constant_policy: options().constants,
        parameters: SemanticParameters::default(),
    }
}

#[test]
fn real_rand_operand_keeps_volatile_state_and_exact_frozen_effects_under_comparison() {
    let functions = functions();
    for (physical, expected) in OPERATORS {
        for mode in [Mode::Project, Mode::Filter] {
            let source = package(&functions, rand_operand_fixture(&functions, physical, mode));
            let program = compile(source.clone(), &functions, &Control).unwrap();
            let compiled = unit(&program);
            let calls = compiled;
            let flow = compiled.control_flow();
            let use_id = occurrence(&program);
            let invocation = &flow.uses()[&use_id];
            assert_eq!(invocation.arguments.len(), 2);
            assert_eq!(calls.calls().len(), 1);
            assert_eq!(source_unit(&source).claims().len(), 1);
            let call = calls.calls().values().next().unwrap();
            let context = call.call_contract().context().expression().unwrap();
            assert_eq!(context.use_id, invocation.arguments[0]);
            assert_eq!(context.region, invocation.context.region);
            assert_eq!(context.demand, EvaluationDemand::Value);
            let summary = call.effects().for_use(context).unwrap();
            assert_eq!(summary.value_stability, FunctionVolatility::Volatile);
            assert!(summary.has_instance_state);
            assert!(summary.observable_effects.rng_sampling);
            let frozen = &source_unit(&source).claims()[&context.use_id];
            assert_eq!(call.call_contract().effects(), &frozen.effects);
            assert_eq!(frozen.context, context);
            assert!(!source_unit(&source).claims().contains_key(&use_id));
            let recipe = unit(&program)
                .recipes()
                .comparison(ComparisonSite::Binary(use_id))
                .unwrap();
            assert_eq!(recipe.operator(), expected);
        }
    }
}

#[test]
fn explicitly_authored_largeint_sources_preserve_their_domain_without_retagging_fixed16() {
    let functions = functions();
    let signed = FunctionValueType::try_from_legacy_declared_projection(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .expect("valid temporary legacy value projection");
    let plain = FunctionValueType::new(DataType::FixedSizeBinary(16), true);
    let uuid = FunctionValueType::try_from_legacy_declared_projection(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::Uuid,
    )
    .expect("valid temporary legacy value projection");
    // Source authority is explicit. Compiler tests do not serve as a row-order oracle.
    for (physical, expected) in OPERATORS {
        for ty in [&signed, &plain, &uuid] {
            let source = package(
                &functions,
                fixture(
                    &functions,
                    Shape::Binary(physical),
                    Mode::Project,
                    ty.clone(),
                    ty.clone(),
                    OutputClaim::Accurate,
                    None,
                )
                .unwrap(),
            );
            let program = compile(source, &functions, &Control).unwrap();
            let _compiled = unit(&program);
            let recipe = unit(&program)
                .recipes()
                .comparison(ComparisonSite::Binary(occurrence(&program)))
                .unwrap();
            assert_eq!(recipe.operator(), expected);
            assert_eq!(recipe.left_type(), ty);
            assert_eq!(recipe.right_type(), ty);
        }
        for foreign in [&plain, &uuid] {
            assert!(matches!(fixture(
                &functions, Shape::Binary(physical), Mode::Project, signed.clone(), foreign.clone(),
                OutputClaim::Accurate, None,
            ), Err(error) if error.is_producer_defect()));
        }
    }
}
