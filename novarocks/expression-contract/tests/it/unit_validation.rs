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

use super::support::*;
use arrow_schema::DataType;
use novarocks_expression_contract::*;
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_type_contract::*;
use std::collections::BTreeMap;

#[test]
fn value_must_be_declared_in_every_root_scope_with_its_exact_type() {
    for (port, ty) in [(1, boolean(true)), (0, boolean(false))] {
        let mut builder = BoundExprSetBuilder::new(limits());
        let id = builder
            .add(None, boolean(true), ExprKind::input(ValueId::new(9)))
            .unwrap();
        builder
            .root(id, EvaluationDemand::Value, RootScope::Port(port))
            .unwrap();
        let error = no_calls(
            builder,
            vec![
                ExprPort {
                    values: Box::new([(ValueId::new(9), ty)]),
                },
                ExprPort {
                    values: Box::new([(ValueId::new(8), boolean(true))]),
                },
            ],
            &Control::good(),
        )
        .unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert!(
            matches!(error,UnitError::Invalid {root:Some(_),definition:Some(actual),..} if actual==id)
        );
    }
    let mut builder = BoundExprSetBuilder::new(limits());
    let id = builder
        .add(None, boolean(true), ExprKind::input(ValueId::new(123)))
        .unwrap();
    builder
        .root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    assert!(no_calls(builder, vec![], &Control::good()).is_err());
}
#[test]
fn same_definition_cannot_hide_a_second_narrower_root_scope() {
    let mut builder = BoundExprSetBuilder::new(limits());
    let id = builder
        .add(None, boolean(true), ExprKind::input(ValueId::new(9)))
        .unwrap();
    builder
        .root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let wrong = builder
        .root(id, EvaluationDemand::TruthOnly, RootScope::Port(1))
        .unwrap();
    let error = no_calls(
        builder,
        vec![
            ExprPort {
                values: Box::new([(ValueId::new(9), boolean(true))]),
            },
            ExprPort {
                values: Box::default(),
            },
        ],
        &Control::good(),
    )
    .unwrap_err();
    assert!(matches!(error,UnitError::Invalid {root:Some(actual),..} if actual==wrong));
}

#[test]
fn let_body_dominates_every_incoming_local_use() {
    enum Placement {
        BodyOnly,
        SecondRoot,
        Sibling,
        Initializer,
    }

    for placement in [
        Placement::BodyOnly,
        Placement::SecondRoot,
        Placement::Sibling,
        Placement::Initializer,
    ] {
        let mut builder = BoundExprSetBuilder::new(limits());
        let binding = builder.reserve().unwrap();
        let input = ValueId::new(9);
        let value = builder
            .add(None, boolean(true), ExprKind::input(input))
            .unwrap();
        let local = builder
            .add(
                None,
                boolean(true),
                ExprKind::Value(ValueAddress::Local(binding)),
            )
            .unwrap();
        let body = builder
            .add(
                None,
                boolean(true),
                ExprKind::Conjunction {
                    args: Box::from([local, local]),
                },
            )
            .unwrap();
        let value = if matches!(placement, Placement::Initializer) {
            body
        } else {
            value
        };
        builder
            .insert(binding, None, boolean(true), ExprKind::Let { value, body })
            .unwrap();
        let root = if matches!(placement, Placement::Sibling) {
            builder
                .add(
                    None,
                    boolean(true),
                    ExprKind::Conjunction {
                        args: Box::from([binding, body]),
                    },
                )
                .unwrap()
        } else {
            binding
        };
        builder
            .root(root, EvaluationDemand::TruthOnly, RootScope::AllPorts)
            .unwrap();
        if matches!(placement, Placement::SecondRoot) {
            builder
                .root(body, EvaluationDemand::Value, RootScope::AllPorts)
                .unwrap();
        }
        let result = no_calls(
            builder,
            vec![ExprPort {
                values: Box::from([(input, boolean(true))]),
            }],
            &Control::good(),
        );
        if matches!(placement, Placement::BodyOnly) {
            let unit = result.unwrap();
            let root = unit.control_flow().root_use_ids()[0];
            let invocation = &unit.control_flow().uses()[&root];
            let uses = unit.control_flow().uses();
            assert_eq!(invocation.control, ControlShape::Let);
            assert_eq!(
                uses[&invocation.arguments[0]].context.demand,
                EvaluationDemand::Value
            );
            assert_eq!(
                uses[&invocation.arguments[1]].context.demand,
                EvaluationDemand::TruthOnly
            );
            let body = &uses[&invocation.arguments[1]];
            let region = unit.control_flow().regions()[&body.context.region];
            assert_eq!(
                region.guard,
                Some(RegionGuard {
                    owner: root,
                    kind: GuardKind::LetBody,
                })
            );
            assert_eq!(region.parent, Some(invocation.context.region));
            assert_ne!(body.arguments[0], body.arguments[1]);
            assert_eq!(
                uses[&body.arguments[0]].definition,
                uses[&body.arguments[1]].definition
            );
        } else {
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains("outside its Let body"),
                "{error:?}"
            );
        }
    }
}

#[test]
fn same_carrier_local_value_cannot_change_its_initializer_nullability() {
    let mut builder = BoundExprSetBuilder::new(limits());
    let binder = builder.reserve().unwrap();
    let input = ValueId::new(9);
    let value = builder
        .add(None, boolean(true), ExprKind::input(input))
        .unwrap();
    let body = builder
        .add(
            None,
            boolean(false),
            ExprKind::Value(ValueAddress::Local(binder)),
        )
        .unwrap();
    builder
        .insert(binder, None, boolean(false), ExprKind::Let { value, body })
        .unwrap();
    builder
        .root(binder, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let error = no_calls(
        builder,
        vec![ExprPort {
            values: Box::from([(input, boolean(true))]),
        }],
        &Control::good(),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("matching Let initializer"),
        "{error:?}"
    );
}

#[test]
fn control_expansion_requires_exact_wrappers_and_ownership() {
    let id = ExprId::new;
    let node = |ordinal, kind| ExprNode {
        id: id(ordinal),
        lambda_scope: None,
        ty: boolean(false),
        kind,
    };
    let comparison = OperatorFunction::LessThanOrEqual;
    let branch = |left, right| ExprKind::FunctionCall {
        function: BoundFunction::from_exact_signature(
            comparison.function_id(),
            FunctionOverloadId::try_new(comparison.comparison_overload_text().unwrap()).unwrap(),
            FunctionKind::Scalar,
            TypedFactSlice::from_pure_boxed(Box::from([
                FunctionArgumentType::Value(boolean(false)),
                FunctionArgumentType::Value(boolean(false)),
            ])),
            boolean(false),
        ),
        args: Box::from([id(left), id(right)]),
    };
    let mut definitions = vec![
        node(0, ExprKind::input(ValueId::new(9))),
        node(1, branch(0, 0)),
        node(2, branch(0, 0)),
        node(
            3,
            ExprKind::Conjunction {
                args: Box::from([id(1), id(2)]),
            },
        ),
        node(
            4,
            ExprKind::Let {
                value: id(0),
                body: id(3),
            },
        ),
    ];
    let expansion = ControlExpansion {
        root: id(4),
        positive: id(4),
        conjunction: id(3),
        branches: [id(1), id(2)],
    };
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let checked =
        ControlExpansions::try_new_observed(Box::from([expansion]), &definitions, &mut work)
            .unwrap();
    assert_eq!(checked.records(), &[expansion]);
    assert!(checked.shares_source_position(id(4), id(3)));
    assert!(checked.shares_source_position(id(3), id(1)));
    assert!(!checked.shares_source_position(id(4), id(0)));

    definitions[2].kind = branch(1, 0);
    assert!(
        ControlExpansions::try_new_observed(Box::from([expansion]), &definitions, &mut work)
            .is_err()
    );
    definitions[2].kind = branch(0, 0);
    definitions.push(node(
        5,
        ExprKind::IsNull {
            expr: id(2),
            negated: false,
        },
    ));
    assert!(
        ControlExpansions::try_new_observed(Box::from([expansion]), &definitions, &mut work)
            .is_err()
    );
    work.finish().unwrap();
}

#[test]
fn reservation_refuses_duplicate_fill_and_seal_refuses_holes_cycles_and_unreachable_definitions() {
    let mut builder = BoundExprSetBuilder::new(limits());
    let id = builder.reserve().unwrap();
    builder
        .insert(id, None, boolean(true), ExprKind::input(ValueId::new(9)))
        .unwrap();
    assert!(
        builder
            .insert(id, None, integer(false), ExprKind::input(ValueId::new(8)))
            .is_err()
    );
    assert_eq!(builder.definition(id).unwrap().ty, boolean(true));
    let mut hole = BoundExprSetBuilder::new(limits());
    let id = hole.reserve().unwrap();
    hole.root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    assert!(no_calls(hole, vec![], &Control::good()).is_err());
    let mut cycle = BoundExprSetBuilder::new(limits());
    let id = cycle.reserve().unwrap();
    cycle
        .insert(
            id,
            None,
            boolean(true),
            ExprKind::IsNull {
                negated: true,
                expr: id,
            },
        )
        .unwrap();
    cycle
        .root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    assert!(no_calls(cycle, vec![], &Control::good()).is_err());
    let mut dead = BoundExprSetBuilder::new(limits());
    let first = dead
        .add(None, boolean(true), ExprKind::input(ValueId::new(9)))
        .unwrap();
    dead.add(None, boolean(true), ExprKind::input(ValueId::new(9)))
        .unwrap();
    dead.root(first, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    assert!(
        no_calls(
            dead,
            vec![ExprPort {
                values: Box::new([(ValueId::new(9), boolean(true))])
            }],
            &Control::good()
        )
        .is_err()
    );
}
#[test]
fn lambda_parameter_requires_its_real_lexical_owner_and_root_cannot_enter_a_lambda() {
    let mut builder = BoundExprSetBuilder::new(limits());
    let lambda = builder.reserve().unwrap();
    let parameter = builder
        .add(
            None,
            integer(false),
            ExprKind::LambdaParameter { lambda, ordinal: 0 },
        )
        .unwrap();
    builder
        .insert(
            lambda,
            None,
            integer(false),
            ExprKind::Lambda {
                parameter_types: Box::new([integer(false)]),
                body: parameter,
            },
        )
        .unwrap();
    builder
        .root(lambda, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    assert!(no_calls(builder, vec![], &Control::good()).is_err());
    let mut builder = BoundExprSetBuilder::new(limits());
    let lambda = builder.reserve().unwrap();
    let parameter = builder
        .add(
            Some(lambda),
            integer(false),
            ExprKind::LambdaParameter { lambda, ordinal: 2 },
        )
        .unwrap();
    builder
        .insert(
            lambda,
            None,
            integer(false),
            ExprKind::Lambda {
                parameter_types: Box::new([integer(false)]),
                body: parameter,
            },
        )
        .unwrap();
    builder
        .root(parameter, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    assert!(no_calls(builder, vec![], &Control::good()).is_err());
}
#[test]
fn semantic_depth_keeps_resource_class_and_truth_only_keeps_boolean_contract() {
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 3,
    });
    let (pools, reference) = null_source(&boolean(true));
    let mut id = builder
        .add(None, boolean(true), ExprKind::Constant(reference))
        .unwrap();
    for _ in 0..3 {
        id = builder
            .add(
                None,
                boolean(false),
                ExprKind::IsNull {
                    negated: true,
                    expr: id,
                },
            )
            .unwrap();
    }
    builder
        .root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    assert_eq!(
        no_calls(builder, vec![], &Control::good())
            .unwrap_err()
            .failure_fact()
            .class(),
        FailureClass::Resource
    );
    drop(pools);
    // A previously sealed unit must still obey the enclosing deployment's
    // stricter semantic limit through the same unit validation author.
    let mut fresh = BoundExprSetBuilder::new(limits());
    let (source, reference) = null_source(&boolean(true));
    let mut id = fresh
        .add(None, boolean(true), ExprKind::Constant(reference))
        .unwrap();
    for _ in 0..2 {
        id = fresh
            .add(
                None,
                boolean(false),
                ExprKind::IsNull {
                    negated: true,
                    expr: id,
                },
            )
            .unwrap();
    }
    fresh
        .root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let unit = no_calls(fresh, vec![], &Control::good()).unwrap();
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    assert_eq!(
        unit.validate_structure_in(
            UnitLimits {
                expression_semantic_depth: 2
            },
            &mut work
        )
        .unwrap_err()
        .failure_fact()
        .class(),
        FailureClass::Resource
    );
    drop(source);
    let mut builder = BoundExprSetBuilder::new(limits());
    let (pools, reference) = i64_source(1);
    let id = builder
        .add(None, integer(false), ExprKind::Constant(reference))
        .unwrap();
    builder
        .root(id, EvaluationDemand::TruthOnly, RootScope::AllPorts)
        .unwrap();
    assert_eq!(
        no_calls(builder, vec![], &Control::good())
            .unwrap_err()
            .failure_fact()
            .class(),
        FailureClass::Invariant
    );
    drop(pools);
}
fn call_builder(add_request: bool, add_control: bool) -> BoundExprSetBuilder {
    let mut b = BoundExprSetBuilder::new(limits());
    let f = function("fixture/unit/call", vec![], integer(false));
    let request = request(&f);
    let id = b
        .add(
            None,
            integer(false),
            ExprKind::FunctionCall {
                function: f,
                args: Box::default(),
            },
        )
        .unwrap();
    if add_request {
        b.request(id, request).unwrap();
    }
    if add_control {
        b.definition_control(
            id,
            DefinitionControl {
                shape: ControlShape::Eager,
                temporal_sources: None,
            },
        )
        .unwrap();
    }
    b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    b
}
#[test]
fn invoked_calls_require_explicit_control_every_claim_and_every_original_request() {
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    assert!(call_builder(true, false).seal(vec![], &mut work).is_err());
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = call_builder(true, true).seal(vec![], &mut work).unwrap();
    assert!(sealed.finish(BTreeMap::new(), &mut work).is_err());
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = call_builder(false, true).seal(vec![], &mut work).unwrap();
    assert!(with_call_claims(sealed, ArgumentControl::Eager, &mut work).is_err());
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = call_builder(true, true).seal(vec![], &mut work).unwrap();
    let use_id = sealed.root_use_ids()[0];
    let mut incorrect = claim(sealed.uses()[&use_id].context, ArgumentControl::Eager);
    incorrect.context.region = EvaluationRegionId::new(777);
    assert!(
        sealed
            .finish(BTreeMap::from([(use_id, incorrect)]), &mut work)
            .is_err()
    );
}

fn not_unit(
    input: ValueType,
    output: ValueType,
) -> Result<(BoundExprSet, novarocks_constant_contract::ConstantPools), UnitError> {
    let (pools, reference) = null_source(&input);
    let mut b = BoundExprSetBuilder::new(limits());
    let child = b.add(None, input, ExprKind::Constant(reference)).unwrap();
    let function = BoundFunction::from_exact_signature(
        OperatorFunction::BooleanNot.function_id(),
        FunctionOverloadId::try_new("fixture/boolean-not").unwrap(),
        FunctionKind::Scalar,
        TypedFactSlice::from_pure_boxed(Box::from([FunctionArgumentType::Value(boolean(true))])),
        output.clone(),
    );
    let original = request(&function);
    let id = b
        .add(
            None,
            output,
            ExprKind::FunctionCall {
                function,
                args: Box::from([child]),
            },
        )
        .unwrap();
    b.request(id, original).unwrap();
    b.definition_control(
        id,
        DefinitionControl {
            shape: ControlShape::OrderedNull,
            temporal_sources: None,
        },
    )
    .unwrap();
    b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate)?;
    let sealed = b.seal(vec![], &mut work)?;
    let use_id = sealed.root_use_ids()[0];
    let mut binding_claim = claim(sealed.uses()[&use_id].context, ArgumentControl::OrderedNull);
    binding_claim.effects.null_behavior = FunctionNullBehavior::Strict;
    let unit = sealed.finish(BTreeMap::from([(use_id, binding_claim)]), &mut work)?;
    work.finish()?;
    Ok((unit, pools))
}

#[test]
fn boolean_not_requires_boolean_input_and_keeps_successful_nullability() {
    assert!(not_unit(boolean(true), boolean(true)).is_ok());
    assert!(not_unit(boolean(true), boolean(false)).is_err());
    for carrier in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(18, 4),
        DataType::UInt64,
        DataType::Decimal256(50, 4),
        DataType::Date32,
        DataType::Null,
        DataType::FixedSizeBinary(16),
    ] {
        assert!(not_unit(ValueType::new(carrier, true), boolean(true)).is_err());
    }
}

// A table binding is a relation call, even when its advertised result is scalar.
#[test]
fn table_function_binding_cannot_be_published_as_a_scalar_call() {
    let mut builder = BoundExprSetBuilder::new(limits());
    let mut binding = function("builtin/test_relation/v1", vec![], integer(false));
    binding.overload = FunctionOverloadId::try_new("disguised-scalar").unwrap();
    binding.kind = FunctionKind::Table;
    binding.legacy_metadata = Some(LegacyBindingMetadata {
        semantic_parameters: Box::default(),
        volatility: FunctionVolatility::Immutable,
        argument_evaluation: FunctionArgumentEvaluation::Eager,
        failure_behavior: FunctionFailureBehavior::Propagate,
        intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
    });
    let definition = builder
        .add(
            None,
            integer(false),
            ExprKind::FunctionCall {
                function: binding,
                args: Box::default(),
            },
        )
        .unwrap();
    builder
        .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let error = no_calls(builder, vec![], &Control::good()).unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert!(
        error
            .to_string()
            .contains("scalar call has non-scalar binding")
    );
}

#[test]
fn deciding_boolean_source_uses_the_actual_pool_and_preserves_every_control_prefix() {
    use novarocks_constant_contract::{
        ConstantPoolId, ConstantPools, ConstantTableError, ConstantValue,
    };
    use std::sync::Arc;
    let value = ConstantValue::from_boolean(
        Arc::new(arrow_schema::Field::new(
            "actual_false",
            DataType::Boolean,
            true,
        )),
        boolean(true),
        false,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let reference = novarocks_constant_contract::ConstantReference {
        pool: ConstantPoolId::new(7),
        ordinal: value.ordinal(),
    };
    let mut pools = ConstantPools::empty();
    pools.insert(reference.pool, value.pool().clone()).unwrap();
    let mut b = BoundExprSetBuilder::new(limits());
    let input = b
        .add(None, boolean(true), ExprKind::input(ValueId::new(9)))
        .unwrap();
    let literal = b
        .add(None, boolean(true), ExprKind::Constant(reference))
        .unwrap();
    let expression = b
        .add(
            None,
            boolean(false),
            ExprKind::Conjunction {
                args: Box::new([input, literal]),
            },
        )
        .unwrap();
    b.root(expression, EvaluationDemand::TruthOnly, RootScope::Port(0))
        .unwrap();
    let unit = no_calls(
        b,
        vec![ExprPort {
            values: Box::new([(ValueId::new(9), boolean(true))]),
        }],
        &Control::good(),
    )
    .unwrap();
    let run = |control: &Control, pools: &ConstantPools| -> Result<bool, ConstantTableError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let valid = boolean_connective_nullability_observed(
            unit.definition(expression).unwrap(),
            |id| unit.definition(id),
            |source, work| {
                let ExprKind::Constant(reference) = source.kind else {
                    panic!("only the actual immediate source is requested")
                };
                pools
                    .resolve_observed(reference, &source.ty, work)?
                    .try_boolean()
                    .map_err(ConstantTableError::Constant)
            },
            &mut work,
        )?;
        work.finish()?;
        Ok(valid)
    };
    let good = Control::good();
    assert!(run(&good, &pools).unwrap());
    assert!(
        matches!(run(&Control::good(),&ConstantPools::empty()),Err(ConstantTableError::MissingPool(id)) if id==reference.pool)
    );
    for at in 0..good.trace().len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::at(at, cause.clone());
            assert!(
                matches!(run(&control,&pools),Err(ConstantTableError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), good.trace()[..=at]);
        }
    }
}
