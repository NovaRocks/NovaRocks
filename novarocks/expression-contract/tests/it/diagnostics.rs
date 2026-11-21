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

//! Public malformed-type diagnostics retain original local IDs and payloads.
//! These fixed examples protect the lazy first-issue representation while the
//! existing validation suites cover the accepted type domains and work order.
use super::support::*;
use arrow_schema::DataType;
use novarocks_expression_contract::*;
use novarocks_type_contract::{
    DecimalOverflowPolicy, EvaluationDemand, FunctionArgumentType, SemanticParameterId,
    SemanticParameterKey, SemanticParameterRef,
};

fn message(builder: BoundExprSetBuilder, ports: Vec<ExprPort>) -> String {
    match no_calls(builder, ports, &Control::good()).unwrap_err() {
        UnitError::Invalid {
            definition,
            root,
            message,
        } => {
            assert!(definition.is_some());
            assert!(root.is_none());
            message
        }
        other => panic!("expected original type diagnosis, got {other:?}"),
    }
}
fn port(types: &[ValueType]) -> ExprPort {
    ExprPort {
        values: types
            .iter()
            .enumerate()
            .map(|(i, ty)| (ValueId::new(i as u32), ty.clone()))
            .collect(),
    }
}
fn value(builder: &mut BoundExprSetBuilder, id: u32, ty: ValueType) -> ExprId {
    builder
        .add(None, ty, ExprKind::Value(ValueId::new(id)))
        .unwrap()
}
fn root(builder: &mut BoundExprSetBuilder, definition: ExprId) {
    builder
        .root(definition, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
}
#[test]
fn lazy_type_issues_keep_cast_connective_case_and_arity_details() {
    let mut builder = BoundExprSetBuilder::new(limits());
    let source = value(&mut builder, 0, integer(false));
    let cast = builder
        .add(
            None,
            integer(false),
            ExprKind::Cast {
                array_failure_mode: None,
                expr: source,
                target: DataType::Int32,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: SemanticParameterRef {
                    id: SemanticParameterId::new(0),
                    expected_key: SemanticParameterKey::AllowThrowException,
                },
            },
        )
        .unwrap();
    root(&mut builder, cast);
    assert_eq!(
        message(builder, vec![port(&[integer(false)])]),
        "definitions[1]: cast result type Int64 differs from its target Int32"
    );

    let mut builder = BoundExprSetBuilder::new(limits());
    let a = value(&mut builder, 0, boolean(false));
    let b = value(&mut builder, 1, integer(false));
    let conjunction = builder
        .add(
            None,
            boolean(false),
            ExprKind::Conjunction {
                args: Box::new([a, b]),
            },
        )
        .unwrap();
    root(&mut builder, conjunction);
    assert_eq!(
        message(builder, vec![port(&[boolean(false), integer(false)])]),
        "definitions[2]: boolean connective argument 1 is not boolean"
    );

    for simple in [false, true] {
        let mut builder = BoundExprSetBuilder::new(limits());
        let a = value(&mut builder, 0, integer(false));
        let b = value(&mut builder, 1, boolean(false));
        let case = builder
            .add(
                None,
                integer(true),
                ExprKind::Case {
                    operand: simple.then_some(a),
                    when_then: Box::new([(if simple { b } else { a }, a)]),
                    else_expr: None,
                },
            )
            .unwrap();
        // Keep every definition reachable in the searched case as well.
        if !simple {
            root(&mut builder, b);
        }
        root(&mut builder, case);
        assert_eq!(
            message(builder, vec![port(&[integer(false), boolean(false)])]),
            if simple {
                "definitions[2]: CASE compares Int64 against a branch of Boolean"
            } else {
                "definitions[2]: CASE condition is Int64, not a boolean"
            }
        );
    }

    let mut builder = BoundExprSetBuilder::new(limits());
    let call = builder
        .add(
            None,
            integer(false),
            ExprKind::FunctionCall {
                function: function(
                    "fixture/diagnostic/arity",
                    vec![FunctionArgumentType::Value(integer(false))],
                    integer(false),
                ),
                args: Box::default(),
            },
        )
        .unwrap();
    root(&mut builder, call);
    assert_eq!(
        message(builder, vec![port(&[])]),
        "definitions[0]: bound function expects 1 arguments, got 0"
    );
}
#[test]
fn lazy_type_issues_keep_lambda_parent_and_function_argument_payload() {
    let mut builder = BoundExprSetBuilder::new(limits());
    let lambda = builder.reserve().unwrap();
    let parameter = builder
        .add(
            Some(lambda),
            boolean(false),
            ExprKind::LambdaParameter { lambda, ordinal: 0 },
        )
        .unwrap();
    builder
        .insert(
            lambda,
            None,
            boolean(false),
            ExprKind::Lambda {
                parameter_types: Box::new([boolean(false)]),
                body: parameter,
            },
        )
        .unwrap();
    let wrong_parent = builder
        .add(
            None,
            boolean(false),
            ExprKind::Unary {
                op: UnaryOperator::Not,
                expr: lambda,
            },
        )
        .unwrap();
    root(&mut builder, wrong_parent);
    assert_eq!(
        message(builder, vec![port(&[])]),
        "definitions[0]: lambda must appear only as an exact bound-function lambda argument; expression 2 references it outside a bound lambda argument position"
    );

    let mut builder = BoundExprSetBuilder::new(limits());
    let actual = integer(false);
    let expected = FunctionArgumentType::Value(boolean(false));
    let input = value(&mut builder, 0, actual.clone());
    let call = builder
        .add(
            None,
            integer(false),
            ExprKind::FunctionCall {
                function: function(
                    "fixture/diagnostic/argument",
                    vec![expected.clone()],
                    integer(false),
                ),
                args: Box::new([input]),
            },
        )
        .unwrap();
    root(&mut builder, call);
    assert_eq!(
        message(builder, vec![port(std::slice::from_ref(&actual))]),
        format!(
            "definitions[1]: function `fixture/diagnostic/argument` argument 0 shape differs from its bound signature: bound {expected:?}, got {actual:?}"
        )
    );
}
