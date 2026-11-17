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
use novarocks_physical_plan::{ConstantPoolId, ConstantReference, NodeId, ValueType};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, SemanticParameterId,
};
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    observations: Mutex<Vec<u32>>,
    reject: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        assert!(units <= 256);
        let mut observations = self.observations.lock().unwrap();
        let ordinal = observations.len();
        observations.push(units);
        if let Some((at, cause)) = self.reject.clone()
            && at == ordinal
        {
            return Err(cause);
        }
        Ok(())
    }
}

fn ids<'a>(lambda_parameter_type_ids: &'a [u32]) -> PreparedExpressionIds<'a> {
    PreparedExpressionIds {
        root_carrier_type_id: Some(u32::MAX),
        lambda_parameter_type_ids,
        function_binding_id: Some(0),
        aggregate_binding_id: None,
    }
}
fn node(kind: ExprKind) -> ExprNode {
    ExprNode {
        id: ExprId::new(u32::MAX),
        owner: NodeId::new(0),
        lambda_scope: None,
        ty: ValueType::new(DataType::Int64, true),
        kind,
    }
}
fn encoded(kind: ExprKind) -> wire::expression_definition::Kind {
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let result = encode_kind(&node(kind), &ids(&[]), &mut work).unwrap();
    work.finish().unwrap();
    result
}

#[test]
fn kind_projection_preserves_sparse_constant_ids_and_every_operator() {
    use wire::expression_definition::Kind;
    assert_eq!(
        encoded(ExprKind::Constant(ConstantReference {
            pool: ConstantPoolId::new(u32::MAX),
            ordinal: 0,
        })),
        Kind::Literal(wire::ConstantReference {
            pool_id: Some(u32::MAX),
            row_ordinal: 0,
        })
    );
    for (source, expected) in [
        (UnaryOperator::Plus, wire::UnaryOperator::Plus),
        (UnaryOperator::Minus, wire::UnaryOperator::Minus),
        (UnaryOperator::Not, wire::UnaryOperator::Not),
        (UnaryOperator::BitwiseNot, wire::UnaryOperator::BitwiseNot),
    ] {
        assert_eq!(
            encoded(ExprKind::Unary {
                op: source,
                expr: ExprId::new(0)
            }),
            Kind::Unary(wire::UnaryExpression {
                op: expected as i32,
                expr_id: Some(0)
            })
        );
    }
    for (source, expected) in [
        (BinaryOperator::Add, wire::BinaryOperator::Add),
        (BinaryOperator::Subtract, wire::BinaryOperator::Subtract),
        (BinaryOperator::Multiply, wire::BinaryOperator::Multiply),
        (BinaryOperator::Divide, wire::BinaryOperator::Divide),
        (BinaryOperator::Modulo, wire::BinaryOperator::Modulo),
        (BinaryOperator::Eq, wire::BinaryOperator::Eq),
        (BinaryOperator::EqForNull, wire::BinaryOperator::EqForNull),
        (BinaryOperator::NotEq, wire::BinaryOperator::NotEq),
        (BinaryOperator::Lt, wire::BinaryOperator::Lt),
        (BinaryOperator::LtEq, wire::BinaryOperator::LtEq),
        (BinaryOperator::Gt, wire::BinaryOperator::Gt),
        (BinaryOperator::GtEq, wire::BinaryOperator::GtEq),
        (BinaryOperator::BitAnd, wire::BinaryOperator::BitAnd),
        (BinaryOperator::BitOr, wire::BinaryOperator::BitOr),
        (BinaryOperator::BitXor, wire::BinaryOperator::BitXor),
    ] {
        let result = encoded(ExprKind::Binary {
            left: ExprId::new(u32::MAX),
            op: source,
            right: ExprId::new(0),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            allow_throw_exception: None,
        });
        let Kind::Binary(result) = result else {
            panic!("expected binary")
        };
        assert_eq!(
            (result.left_expr_id, result.right_expr_id),
            (Some(u32::MAX), Some(0))
        );
        assert_eq!(result.op, expected as i32);
        assert!(result.allow_throw_exception.is_none());
    }
}

#[test]
fn ordered_ids_case_lambda_and_window_vocabulary_are_not_canonicalized() {
    use wire::expression_definition::Kind;
    let ordered: Box<[ExprId]> =
        Box::from([ExprId::new(u32::MAX), ExprId::new(0), ExprId::new(u32::MAX)]);
    assert_eq!(
        encoded(ExprKind::Conjunction {
            args: ordered.clone()
        }),
        Kind::Conjunction(wire::ExpressionIds {
            expr_ids: vec![u32::MAX, 0, u32::MAX]
        })
    );
    assert_eq!(
        encoded(ExprKind::Disjunction { args: ordered }),
        Kind::Disjunction(wire::ExpressionIds {
            expr_ids: vec![u32::MAX, 0, u32::MAX]
        })
    );
    let Kind::CaseExpression(case) = encoded(ExprKind::Case {
        operand: Some(ExprId::new(0)),
        when_then: Box::from([
            (ExprId::new(9), ExprId::new(8)),
            (ExprId::new(9), ExprId::new(7)),
        ]),
        else_expr: None,
    }) else {
        panic!("expected case")
    };
    assert_eq!(case.operand_expr_id, Some(0));
    assert!(case.else_expr_id.is_none());
    assert_eq!(
        case.arms
            .iter()
            .map(|arm| (arm.when_expr_id, arm.then_expr_id))
            .collect::<Vec<_>>(),
        vec![(Some(9), Some(8)), (Some(9), Some(7))]
    );
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let Kind::Lambda(lambda) = encode_kind(
        &node(ExprKind::Lambda {
            parameter_types: Box::from([
                ValueType::new(DataType::Int64, true),
                ValueType::new(DataType::Int64, false),
            ]),
            body: ExprId::new(0),
        }),
        &ids(&[u32::MAX, 0]),
        &mut work,
    )
    .unwrap() else {
        panic!("expected lambda")
    };
    assert_eq!(lambda.parameter_value_type_ids, vec![u32::MAX, 0]);
    for (bound, expected) in [
        (
            WindowBound::UnboundedPreceding,
            wire::window_bound::Kind::UnboundedPreceding(physical_control_v2::Empty {}),
        ),
        (
            WindowBound::Preceding(ExprId::new(0)),
            wire::window_bound::Kind::PrecedingExprId(0),
        ),
        (
            WindowBound::CurrentRow,
            wire::window_bound::Kind::CurrentRow(physical_control_v2::Empty {}),
        ),
        (
            WindowBound::Following(ExprId::new(u32::MAX)),
            wire::window_bound::Kind::FollowingExprId(u32::MAX),
        ),
        (
            WindowBound::UnboundedFollowing,
            wire::window_bound::Kind::UnboundedFollowing(physical_control_v2::Empty {}),
        ),
    ] {
        assert_eq!(window_bound(bound, &mut work).unwrap().kind, Some(expected));
    }
    for (units, wire_units) in [
        (WindowFrameUnits::Rows, wire::WindowFrameUnits::Rows),
        (WindowFrameUnits::Range, wire::WindowFrameUnits::Range),
        (WindowFrameUnits::Groups, wire::WindowFrameUnits::Groups),
    ] {
        for (exclusion, wire_exclusion) in [
            (
                WindowFrameExclusion::NoOthers,
                wire::WindowFrameExclusion::NoOthers,
            ),
            (
                WindowFrameExclusion::CurrentRow,
                wire::WindowFrameExclusion::CurrentRow,
            ),
            (
                WindowFrameExclusion::Group,
                wire::WindowFrameExclusion::Group,
            ),
            (WindowFrameExclusion::Ties, wire::WindowFrameExclusion::Ties),
        ] {
            let frame = window_frame(
                &WindowFrame {
                    units,
                    start: WindowBound::CurrentRow,
                    end: WindowBound::Following(ExprId::new(0)),
                    exclusion,
                },
                &mut work,
            )
            .unwrap();
            assert_eq!(
                (frame.units, frame.exclusion),
                (wire_units as i32, wire_exclusion as i32)
            );
        }
    }
    work.finish().unwrap();
}

#[test]
fn kind_refuses_mismatched_lambda_and_wrong_parameter_key() {
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    assert!(matches!(
        encode_kind(
            &node(ExprKind::Lambda {
                parameter_types: Box::from([ValueType::new(DataType::Int64, false)]),
                body: ExprId::new(0),
            }),
            &ids(&[]),
            &mut work
        ),
        Err(ExpressionCodecError::InvalidShape(_))
    ));
    assert!(matches!(
        allow_throw(
            &SemanticParameterRef {
                id: SemanticParameterId::new(0),
                expected_key: SemanticParameterKey::TimeZone
            },
            &mut work
        ),
        Err(ExpressionCodecError::InvalidShape(_))
    ));
    assert!(required_binding(None).is_err());
    assert_eq!(required_binding(Some(0)).unwrap(), 0);
    assert_eq!(required_binding(Some(u32::MAX)).unwrap(), u32::MAX);
    work.finish().unwrap();
}

#[test]
fn ordered_projection_preserves_every_original_control_failure_prefix() {
    let source = node(ExprKind::Conjunction {
        args: (0..320).map(ExprId::new).collect(),
    });
    let run = |control: &Control| {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let result = encode_kind(&source, &ids(&[]), &mut work);
        if matches!(&result, Err(ExpressionCodecError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    };
    let success = Control::default();
    run(&success).unwrap();
    let trace = success.observations.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert_eq!(trace.last(), Some(&64));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..trace.len() {
            let control = Control {
                reject: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(run(&control), Err(ExpressionCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.observations.lock().unwrap(), trace[..=at]);
        }
    }
}
