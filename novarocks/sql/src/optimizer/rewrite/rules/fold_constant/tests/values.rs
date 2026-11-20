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

use super::{fixtures::*, *};
use crate::common::LiteralValue;
use crate::optimizer::scalar::HashableLiteral;

#[test]
fn folds_integer_addition_to_single_literal() {
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let sum = fixture.binary(BinOp::Add, one, one);
    let plan = project(sum);

    assert!(fixture.matches(&plan));
    let rewritten = changed(fixture.apply(plan));

    let folded = project_expr(&rewritten);
    assert_int_literal(&fixture, folded, 2);
    // Type metadata is preserved verbatim.
    assert_eq!(fixture.data_type(folded), DataType::Int64);
    assert!(fixture.nullable(folded));
}

#[test]
fn preserves_non_default_type_metadata_of_the_folded_node() {
    let mut fixture = Fixture::new();
    let one = fixture.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1))),
        novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
    );
    // The result retains its nullable arena type.
    let sum = fixture.binary_typed(BinOp::Add, one, one, DataType::Int32, true);
    let plan = project(sum);

    let rewritten = changed(fixture.apply(plan));
    let folded = project_expr(&rewritten);

    assert_int_literal(&fixture, folded, 2);
    assert_eq!(fixture.data_type(folded), DataType::Int32);
    assert!(fixture.nullable(folded));
}

#[test]
fn folds_nested_arithmetic_fully() {
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let two = fixture.int_literal(2);
    let three = fixture.int_literal(3);
    let sum = fixture.binary(BinOp::Add, one, two);
    let product = fixture.binary(BinOp::Mul, sum, three);
    let plan = project(product);

    let rewritten = changed(fixture.apply(plan));
    let folded = project_expr(&rewritten);

    assert_int_literal(&fixture, folded, 9);
    assert_eq!(fixture.calls(), 2, "one unit per folded node");
}

#[test]
fn folds_immutable_function_call() {
    let mut fixture = Fixture::new();

    let one = fixture.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1))),
        novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
    );
    let call = fixture.function_call(
        "abs",
        vec![one],
        false,
        FunctionVolatility::Immutable,
        DataType::Int64,
        false,
    );
    let plan = project(call);

    let rewritten = changed(fixture.apply(plan));
    assert_int_literal(&fixture, project_expr(&rewritten), 1);
}

#[test]
fn throwing_fold_request_retains_original_node_and_policy_and_nullable_sibling_folds() {
    use novarocks_type_contract::{
        DecimalOverflowPolicy::{OutputNull, ReportError},
        FunctionValueType,
    };
    let mut fixture = Fixture::new();
    let wide = DataType::Decimal128(38, 0);
    let narrow = DataType::Decimal128(9, 0);
    let child = fixture.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Decimal(
            "99999999999999999999999999999999999999".into(),
        ))),
        FunctionValueType::new(wide.clone(), false),
    );
    for cast in [false, true] {
        let node = |decimal_overflow_policy| {
            if cast {
                ScalarNode::Cast {
                    array_failure_mode: None,
                    child,
                    target: narrow.clone(),
                    decimal_overflow_policy,
                }
            } else {
                ScalarNode::BinaryOp {
                    op: BinOp::Add,
                    left: child,
                    right: child,
                    decimal_overflow_policy,
                }
            }
        };
        let ty = FunctionValueType::new(if cast { narrow.clone() } else { wide.clone() }, true);
        let throwing = fixture.intern(node(ReportError), ty.clone());
        let nullable = fixture.intern(node(OutputNull), ty);
        let original = fixture.node(throwing);
        assert!(matches!(
            fixture.apply(project(throwing)),
            RewriteResult::Unchanged
        ));
        let value_type = fixture.arena.borrow().value_type(throwing).clone();
        assert_eq!(fixture.intern(original, value_type), throwing);
        let folded = project_expr(&changed(fixture.apply(project(nullable))));
        let ScalarNode::Constant(value) = fixture.node(folded) else {
            panic!("checked NULL expected")
        };
        assert!(
            value
                .is_null_observed(
                    CompilePhase::Validate,
                    crate::optimizer::test_optimizer_control()
                )
                .unwrap()
        );
    }
}

#[test]
fn legacy_allow_disables_numeric_fold_without_disabling_pure_ast_reduction() {
    use novarocks_type_contract::FunctionValueType;
    let mut fixture = Fixture::new();
    fixture.ctx.set_root_allow_throw_exception(true);
    let one = fixture.int_literal(1);
    let sum = fixture.binary(BinOp::Add, one, one);
    assert!(matches!(
        fixture.apply(project(sum)),
        RewriteResult::Unchanged
    ));
    let nested = fixture.intern(
        ScalarNode::Nested(one),
        FunctionValueType::new(DataType::Int64, false),
    );
    let folded = project_expr(&changed(fixture.apply(project(nested))));
    assert_eq!(folded, one);
    assert_eq!(fixture.calls(), 0);
}

#[test]
fn real_fold_author_preserves_root_domains_and_nested_field_identity() {
    use crate::compiler::SqlFoldDependencyObserver;
    use novarocks_functions::ConstantValue;
    use novarocks_type_contract::{CompileControlError, FunctionValueType, PureCompileControl};
    use std::sync::Arc;

    struct ReadUnit(FunctionValueType);
    impl SqlFoldDependencyObserver for ReadUnit {
        fn observe_fold_unit_observed(
            &self,
            source: &SqlAuthoredScalarUnit,
            result: Option<&ConstantValue>,
            _control: &dyn PureCompileControl,
        ) -> Result<(), CompileControlError> {
            assert_eq!(source.value_type(), &self.0);
            assert_eq!(result.unwrap().value_type(), &self.0);
            assert_eq!(source.unit().roots().len(), 1);
            assert!(source.unit().ports().is_empty());
            Ok(())
        }
    }

    let mut fixture = Fixture::new();
    let ty = FunctionValueType::new(DataType::Decimal256(76, -7), false);
    fixture
        .ctx
        .set_fold_dependency_observer(Some(Arc::new(ReadUnit(ty.clone()))));
    let literal = fixture.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Decimal("0".into()))),
        ty.clone(),
    );
    let identity = fixture.intern(
        ScalarNode::Cast {
            array_failure_mode: None,
            child: literal,
            target: ty.data_type().clone(),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        ty,
    );
    let folded = project_expr(&changed(fixture.apply(project(identity))));
    assert!(matches!(fixture.node(folded), ScalarNode::Constant(_)));
}

#[test]
fn constant_inputs_share_outcomes_across_operators_and_rounds() {
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let two = fixture.int_literal(2);
    let zero = fixture.int_literal(0);
    let kept = fixture.binary(BinOp::Div, one, zero);
    for _round in 0..2 {
        assert!(matches!(
            fixture.apply(project(kept)),
            RewriteResult::Unchanged
        ));
    }
    assert_eq!(fixture.calls(), 1);

    let both_ones = fixture.binary(BinOp::Add, one, one);
    let original = fixture.binary(BinOp::Add, both_ones, both_ones);
    let first = project_expr(&changed(fixture.apply(project(original))));
    assert_eq!(fixture.calls(), 3, "division, 1 + 1, and 2 + 2");

    // Both computations of 2 have the same complete nullable type. They
    // share the lowered input for the outer addition across operator visits.
    let four = fixture.int_literal(4);
    let difference = fixture.binary(BinOp::Sub, four, two);
    let equivalent = fixture.binary(BinOp::Add, difference, both_ones);
    let second = project_expr(&changed(fixture.apply(project(equivalent))));
    assert_eq!(first, second);
    assert_int_literal(&fixture, first, 4);
    assert_eq!(fixture.calls(), 4, "only 4 - 2 needs a new unit");
}
