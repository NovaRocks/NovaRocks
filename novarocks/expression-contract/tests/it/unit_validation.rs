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
            .add(None, boolean(true), ExprKind::Value(ValueId::new(9)))
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
        .add(None, boolean(true), ExprKind::Value(ValueId::new(123)))
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
        .add(None, boolean(true), ExprKind::Value(ValueId::new(9)))
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
fn reservation_refuses_duplicate_fill_and_seal_refuses_holes_cycles_and_unreachable_definitions() {
    let mut builder = BoundExprSetBuilder::new(limits());
    let id = builder.reserve().unwrap();
    builder
        .insert(id, None, boolean(true), ExprKind::Value(ValueId::new(9)))
        .unwrap();
    assert!(
        builder
            .insert(id, None, integer(false), ExprKind::Value(ValueId::new(8)))
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
            ExprKind::Unary {
                op: UnaryOperator::Not,
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
        .add(None, boolean(true), ExprKind::Value(ValueId::new(9)))
        .unwrap();
    dead.add(None, boolean(true), ExprKind::Value(ValueId::new(9)))
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
                boolean(true),
                ExprKind::Unary {
                    op: UnaryOperator::Not,
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
                boolean(true),
                ExprKind::Unary {
                    op: UnaryOperator::Not,
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

// Existing arithmetic admission scenarios, moved from physical-plan.
fn largeint(nullable: bool) -> ValueType {
    ValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        nullable,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}

// Exercise the public unit gate with actual declared input ports.
fn binary_valid(left: ValueType, op: BinaryOperator, right: ValueType, output: ValueType) -> bool {
    let mut b = BoundExprSetBuilder::new(limits());
    let a = b
        .add(None, left.clone(), ExprKind::Value(ValueId::new(0)))
        .unwrap();
    let c = b
        .add(None, right.clone(), ExprKind::Value(ValueId::new(1)))
        .unwrap();
    let id = b
        .add(
            None,
            output,
            ExprKind::Binary {
                op,
                left: a,
                right: c,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: op.arithmetic_operator().map(|_| SemanticParameterRef {
                    id: SemanticParameterId::new(0),
                    expected_key: SemanticParameterKey::AllowThrowException,
                }),
            },
        )
        .unwrap();
    b.root(id, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    no_calls(
        b,
        vec![ExprPort {
            values: Box::new([(ValueId::new(0), left), (ValueId::new(1), right)]),
        }],
        &Control::good(),
    )
    .is_ok()
}

#[test]
fn arithmetic_gate_preserves_exact_domain_and_only_root_nullable_widening() {
    assert!(binary_valid(
        largeint(false),
        BinaryOperator::Add,
        ValueType::new(DataType::Int64, false),
        largeint(false)
    ));
    assert!(binary_valid(
        largeint(false),
        BinaryOperator::Add,
        ValueType::new(DataType::Int64, false),
        largeint(true)
    ));
    assert!(!binary_valid(
        largeint(true),
        BinaryOperator::Add,
        ValueType::new(DataType::Int64, false),
        largeint(false)
    ));
    assert!(!binary_valid(
        largeint(false),
        BinaryOperator::Add,
        ValueType::new(DataType::Int64, false),
        ValueType::new(DataType::FixedSizeBinary(16), true)
    ));
    assert!(binary_valid(
        largeint(false),
        BinaryOperator::Divide,
        largeint(false),
        ValueType::new(DataType::Float64, true)
    ));
    assert!(!binary_valid(
        largeint(false),
        BinaryOperator::Divide,
        largeint(false),
        largeint(true)
    ));
    assert!(binary_valid(
        ValueType::new(DataType::Decimal128(38, 15), true),
        BinaryOperator::Subtract,
        largeint(false),
        ValueType::new(DataType::Decimal256(55, 15), true)
    ));
    assert!(!binary_valid(
        ValueType::new(DataType::Decimal128(38, 15), true),
        BinaryOperator::Multiply,
        largeint(false),
        ValueType::new(DataType::Decimal256(55, 15), true)
    ));
}

#[test]
fn arithmetic_rejects_opaque_fixed16_and_uuid() {
    for opaque in [
        ValueType::new(DataType::FixedSizeBinary(16), false),
        ValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        assert!(!binary_valid(
            opaque,
            BinaryOperator::Add,
            largeint(false),
            largeint(true)
        ));
    }
}

fn not_unit(
    input: ValueType,
    output: ValueType,
) -> Result<(BoundExprSet, novarocks_constant_contract::ConstantPools), UnitError> {
    let (pools, reference) = null_source(&input);
    let mut b = BoundExprSetBuilder::new(limits());
    let child = b.add(None, input, ExprKind::Constant(reference)).unwrap();
    let id = b
        .add(
            None,
            output,
            ExprKind::Unary {
                op: UnaryOperator::Not,
                expr: child,
            },
        )
        .unwrap();
    b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    Ok((no_calls(b, vec![], &Control::good())?, pools))
}

#[test]
fn numeric_not_keeps_the_original_successful_domain_and_nullability() {
    for carrier in [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(18, 4),
    ] {
        assert!(not_unit(ValueType::new(carrier.clone(), true), boolean(true)).is_ok());
        assert!(not_unit(ValueType::new(carrier, true), boolean(false)).is_err());
    }
    for carrier in [
        DataType::UInt64,
        DataType::Decimal256(50, 4),
        DataType::Date32,
        DataType::Null,
        DataType::FixedSizeBinary(16),
    ] {
        assert!(not_unit(ValueType::new(carrier, true), boolean(true)).is_err());
    }
}

#[test]
fn uuid_is_not_a_numeric_largeint_domain() {
    let uuid = ValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    assert!(!binary_valid(
        uuid.clone(),
        BinaryOperator::Add,
        uuid.clone(),
        uuid
    ));
    let integer = largeint(false);
    let plain = ValueType::new(DataType::FixedSizeBinary(16), false);
    for (input, output, valid) in [
        (&integer, &integer, true),
        (&integer, &plain, false),
        (&plain, &integer, false),
    ] {
        assert_eq!(
            binary_valid(
                input.clone(),
                BinaryOperator::Add,
                input.clone(),
                output.clone()
            ),
            valid
        );
    }
}
#[test]
fn comparison_refuses_equal_carriers_with_different_root_domains() {
    let json =
        ValueType::try_with_logical_type(DataType::LargeBinary, true, ValueLogicalType::Json)
            .unwrap();
    let plain = ValueType::new(DataType::LargeBinary, true);
    assert!(!binary_valid(
        json.clone(),
        BinaryOperator::Eq,
        plain,
        boolean(true)
    ));
    assert!(binary_valid(
        json.clone(),
        BinaryOperator::Eq,
        json,
        boolean(true)
    ));
}
#[test]
fn mixed_decimal_largeint_validates_only_exact_new_add_sub_output() {
    let decimal = ValueType::new(DataType::Decimal128(38, 15), true);
    let integer = largeint(true);
    let exact = ValueType::new(DataType::Decimal256(55, 15), true);
    for op in [BinaryOperator::Add, BinaryOperator::Subtract] {
        assert!(binary_valid(
            decimal.clone(),
            op,
            integer.clone(),
            exact.clone()
        ));
        assert!(!binary_valid(
            decimal.clone(),
            op,
            integer.clone(),
            decimal.clone()
        ));
    }
    for op in [
        BinaryOperator::Multiply,
        BinaryOperator::Divide,
        BinaryOperator::Modulo,
    ] {
        assert!(!binary_valid(
            decimal.clone(),
            op,
            integer.clone(),
            exact.clone()
        ));
    }
}
#[test]
fn binary_arithmetic_uses_the_type_contract_largeint_domain() {
    let left = largeint(false);
    let right = integer(false);
    assert!(binary_valid(
        left.clone(),
        BinaryOperator::Add,
        right.clone(),
        left.clone()
    ));
    let opaque = ValueType::new(
        DataType::FixedSizeBinary(novarocks_type_contract::LARGEINT_BYTE_WIDTH),
        false,
    );
    for (input, result) in [(&opaque, &left), (&left, &opaque)] {
        assert!(!binary_valid(
            input.clone(),
            BinaryOperator::Add,
            right.clone(),
            result.clone()
        ));
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
        .add(None, boolean(true), ExprKind::Value(ValueId::new(9)))
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

#[test]
fn ordinary_comparisons_keep_successful_null_even_for_required_null_carriers() {
    for op in [
        BinaryOperator::Eq,
        BinaryOperator::NotEq,
        BinaryOperator::Lt,
        BinaryOperator::LtEq,
        BinaryOperator::Gt,
        BinaryOperator::GtEq,
        BinaryOperator::EqForNull,
    ] {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            for nullable in [false, true] {
                let source = ValueType::new(DataType::Null, false);
                let mut b = BoundExprSetBuilder::new(limits());
                let input = b
                    .add(None, source.clone(), ExprKind::Value(ValueId::new(7)))
                    .unwrap();
                let comparison = b
                    .add(
                        None,
                        boolean(nullable),
                        ExprKind::Binary {
                            left: input,
                            right: input,
                            op,
                            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                            allow_throw_exception: None,
                        },
                    )
                    .unwrap();
                b.root(comparison, demand, RootScope::Port(0)).unwrap();
                let result = no_calls(
                    b,
                    vec![ExprPort {
                        values: Box::new([(ValueId::new(7), source.clone())]),
                    }],
                    &Control::good(),
                );
                let accepted = if op == BinaryOperator::EqForNull {
                    !nullable
                } else {
                    nullable
                };
                if accepted {
                    let unit = result.unwrap();
                    assert_eq!(unit.definition(input).unwrap().ty, source);
                    assert_eq!(unit.definition(comparison).unwrap().ty, boolean(nullable));
                    assert_eq!(unit.roots()[0].demand, demand);
                } else {
                    let error=result.expect_err("NULL carrier cannot lose successful NULL, while null-safe equality is required Boolean");
                    assert!(
                        matches!(&error,UnitError::Invalid{definition:Some(id),message,..} if *id==comparison && message.contains("binary expression types are inconsistent with its operator")),
                        "{op:?}/{demand:?}/{nullable}: {error}"
                    );
                    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
                }
            }
        }
    }
}
