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
    let (pools, reference) = i64_source(1);
    let mut id = builder
        .add(None, integer(false), ExprKind::Constant(reference))
        .unwrap();
    for _ in 0..3 {
        id = builder
            .add(
                None,
                integer(false),
                ExprKind::Unary {
                    op: UnaryOperator::Plus,
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
fn binary_numeric_gates_reject_opaque_fixed16_and_preserve_unsigned_bitwise() {
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
            opaque.clone(),
            BinaryOperator::Add,
            largeint(false),
            largeint(true)
        ));
        assert!(!binary_valid(
            opaque.clone(),
            BinaryOperator::BitAnd,
            opaque.clone(),
            opaque
        ));
    }
    for dtype in [
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ] {
        assert!(binary_valid(
            ValueType::new(dtype.clone(), false),
            BinaryOperator::BitXor,
            ValueType::new(dtype.clone(), true),
            ValueType::new(dtype, true)
        ));
    }
}

fn unary_unit(
    input: ValueType,
    output: ValueType,
    op: UnaryOperator,
    nonliteral_null_child: bool,
) -> Result<(BoundExprSet, novarocks_constant_contract::ConstantPools), UnitError> {
    let (pools, reference) = null_source(&input);
    let mut b = BoundExprSetBuilder::new(limits());
    let mut child = b
        .add(None, input.clone(), ExprKind::Constant(reference))
        .unwrap();
    if nonliteral_null_child {
        child = b
            .add(
                None,
                input,
                ExprKind::Unary {
                    op: UnaryOperator::Plus,
                    expr: child,
                },
            )
            .unwrap();
    }
    let id = b
        .add(None, output, ExprKind::Unary { op, expr: child })
        .unwrap();
    b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    Ok((no_calls(b, vec![], &Control::good())?, pools))
}

#[test]
fn checked_unary_units_preserve_decimal_uint_largeint_and_strict_null() {
    for dtype in [
        DataType::Float16,
        DataType::Decimal32(7, 2),
        DataType::Decimal64(10, 2),
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 2),
        DataType::UInt64,
    ] {
        let ty = ValueType::new(dtype, true);
        unary_unit(ty.clone(), ty, UnaryOperator::Minus, false).unwrap();
    }
    unary_unit(
        largeint(true),
        largeint(true),
        UnaryOperator::BitwiseNot,
        false,
    )
    .unwrap();
    for op in [
        UnaryOperator::Plus,
        UnaryOperator::Minus,
        UnaryOperator::BitwiseNot,
    ] {
        let ty = ValueType::new(DataType::Null, true);
        let (fragment, _constants) = unary_unit(ty.clone(), ty.clone(), op, true).unwrap();
        assert_eq!(fragment.definitions().len(), 3);
        assert!(fragment.definitions().iter().all(|expr| expr.ty == ty));
    }
    for opaque in [
        ValueType::new(DataType::FixedSizeBinary(16), true),
        ValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        for op in [UnaryOperator::Minus, UnaryOperator::BitwiseNot] {
            let errors = unary_unit(opaque.clone(), opaque.clone(), op, false).unwrap_err();
            assert!(
                matches!(errors,UnitError::Invalid {message,..} if message.contains("unary expression types are inconsistent with its operator"))
            );
        }
    }
    assert!(
        unary_unit(
            ValueType::new(DataType::Null, true),
            ValueType::new(DataType::Int64, true),
            UnaryOperator::Minus,
            false
        )
        .is_err()
    );
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
        {
            let nullable = true;
            assert!(
                unary_unit(
                    ValueType::new(carrier.clone(), nullable),
                    ValueType::new(DataType::Boolean, nullable),
                    UnaryOperator::Not,
                    false
                )
                .is_ok()
            );
        }
        assert!(
            unary_unit(
                ValueType::new(carrier, true),
                ValueType::new(DataType::Boolean, false),
                UnaryOperator::Not,
                false
            )
            .is_err()
        );
    }
    for carrier in [
        DataType::UInt64,
        DataType::Decimal256(50, 4),
        DataType::Date32,
        DataType::Null,
        DataType::FixedSizeBinary(16),
    ] {
        assert!(
            unary_unit(
                ValueType::new(carrier, true),
                ValueType::new(DataType::Boolean, true),
                UnaryOperator::Not,
                false
            )
            .is_err()
        );
    }
}
