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

//! Ordered kind projection after the caller admits resources and binds source IDs.
//! This leaf neither authors namespaces nor clones type or binding definitions.

use super::{ExpressionCodecError, PreparedExpressionIds};
use crate::allocation_exit_v2::reserve_exit;
use crate::physical_semantics_v2::{encode_decimal_policy, encode_reference};
use novarocks_physical_plan::{
    BinaryOperator, ExprId, ExprKind, ExprNode, NullOrdering, SortDirection, UnaryOperator,
    WindowBound, WindowFrame, WindowFrameExclusion, WindowFrameUnits,
};
use novarocks_proto_models::{physical_control_v2, physical_package_v2 as wire};
use novarocks_type_contract::{CompileCheckpoints, SemanticParameterKey, SemanticParameterRef};

pub(super) fn encode_kind(
    node: &ExprNode,
    authored: &PreparedExpressionIds<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::expression_definition::Kind, ExpressionCodecError> {
    use wire::expression_definition::Kind;
    // The borrowed variant has been visited; children are observed separately.
    work.step()?;
    Ok(match &node.kind {
        ExprKind::Value(value) => Kind::ValueId(value.get()),
        ExprKind::LambdaParameter { lambda, ordinal } => {
            Kind::LambdaParameter(wire::LambdaParameter {
                lambda_expr_id: Some(lambda.get()),
                ordinal: *ordinal,
            })
        }
        ExprKind::Constant(reference) => Kind::Literal(wire::ConstantReference {
            pool_id: Some(reference.pool.get()),
            row_ordinal: reference.ordinal,
        }),
        ExprKind::Unary { op, expr } => Kind::Unary(wire::UnaryExpression {
            op: unary(*op),
            expr_id: Some(expr.get()),
        }),
        ExprKind::Binary {
            left,
            op,
            right,
            decimal_overflow_policy,
            allow_throw_exception,
        } => Kind::Binary(wire::BinaryExpression {
            left_expr_id: Some(left.get()),
            op: binary(*op),
            right_expr_id: Some(right.get()),
            decimal_overflow_policy: encode_decimal_policy(*decimal_overflow_policy),
            allow_throw_exception: allow_throw_exception
                .as_ref()
                .map(|reference| allow_throw(reference, work))
                .transpose()?,
        }),
        ExprKind::Conjunction { args } => Kind::Conjunction(wire::ExpressionIds {
            expr_ids: expression_ids(args, work)?,
        }),
        ExprKind::Disjunction { args } => Kind::Disjunction(wire::ExpressionIds {
            expr_ids: expression_ids(args, work)?,
        }),
        ExprKind::FunctionCall { args, .. } => {
            let binding = required_binding(authored.function_binding_id)?;
            Kind::FunctionCall(wire::FunctionCall {
                function_binding_id: Some(binding),
                argument_expr_ids: expression_ids(args, work)?,
            })
        }
        ExprKind::Lambda {
            parameter_types,
            body,
        } => {
            let count_matches = parameter_types.len() == authored.lambda_parameter_type_ids.len();
            work.step()?;
            if !count_matches {
                return Err(ExpressionCodecError::InvalidShape(
                    "lambda parameter type IDs do not match its parameters",
                ));
            }
            Kind::Lambda(wire::LambdaExpression {
                parameter_value_type_ids: project(
                    authored.lambda_parameter_type_ids,
                    work,
                    |id| *id,
                )?,
                body_expr_id: Some(body.get()),
            })
        }
        ExprKind::Cast {
            expr,
            decimal_overflow_policy,
            allow_throw_exception,
            ..
        } => Kind::Cast(wire::CastExpression {
            expr_id: Some(expr.get()),
            target_carrier_type_id: Some(authored.root_carrier_type_id.ok_or(
                ExpressionCodecError::InvalidShape("cast is missing its authored root carrier ID"),
            )?),
            decimal_overflow_policy: encode_decimal_policy(*decimal_overflow_policy),
            allow_throw_exception: Some(allow_throw(allow_throw_exception, work)?),
        }),
        ExprKind::IsNull { expr, negated } => Kind::IsNull(wire::IsNullExpression {
            expr_id: Some(expr.get()),
            negated: *negated,
        }),
        ExprKind::InList {
            expr,
            list,
            negated,
        } => Kind::InList(wire::InListExpression {
            expr_id: Some(expr.get()),
            list_expr_ids: expression_ids(list, work)?,
            negated: *negated,
        }),
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => Kind::Between(wire::BetweenExpression {
            expr_id: Some(expr.get()),
            low_expr_id: Some(low.get()),
            high_expr_id: Some(high.get()),
            negated: *negated,
        }),
        ExprKind::Like {
            expr,
            pattern,
            negated,
        } => Kind::Like(wire::LikeExpression {
            expr_id: Some(expr.get()),
            pattern_expr_id: Some(pattern.get()),
            negated: *negated,
        }),
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => Kind::CaseExpression(wire::CaseExpression {
            operand_expr_id: operand.map(ExprId::get),
            arms: project(when_then, work, |(when, then)| wire::WhenThen {
                when_expr_id: Some(when.get()),
                then_expr_id: Some(then.get()),
            })?,
            else_expr_id: else_expr.map(ExprId::get),
        }),
        ExprKind::IsTruthValue {
            expr,
            value,
            negated,
        } => Kind::IsTruthValue(wire::TruthValueExpression {
            expr_id: Some(expr.get()),
            value: *value,
            negated: *negated,
        }),
        ExprKind::WindowCall {
            distinct,
            args,
            function_order_by,
            frame,
            ignore_nulls,
            aggregate_binding,
            ..
        } => {
            let binding = required_binding(authored.function_binding_id)?;
            let aggregate_matches =
                aggregate_binding.is_some() == authored.aggregate_binding_id.is_some();
            work.step()?;
            if !aggregate_matches {
                return Err(ExpressionCodecError::InvalidShape(
                    "window aggregate binding ID presence does not match its source",
                ));
            }
            Kind::WindowCall(wire::WindowCall {
                function_binding_id: Some(binding),
                distinct: *distinct,
                argument_expr_ids: expression_ids(args, work)?,
                function_order_by: project(function_order_by, work, |sort| wire::SortExpression {
                    expr_id: Some(sort.expr.get()),
                    direction: match sort.direction {
                        SortDirection::Ascending => wire::SortDirection::Ascending,
                        SortDirection::Descending => wire::SortDirection::Descending,
                    } as i32,
                    null_ordering: match sort.null_ordering {
                        NullOrdering::First => wire::NullOrdering::First,
                        NullOrdering::Last => wire::NullOrdering::Last,
                    } as i32,
                })?,
                frame: frame
                    .as_ref()
                    .map(|frame| window_frame(frame, work))
                    .transpose()?,
                ignore_nulls: *ignore_nulls,
                aggregate_binding_id: authored.aggregate_binding_id,
            })
        }
    })
}

fn required_binding(id: Option<u32>) -> Result<u32, ExpressionCodecError> {
    id.ok_or(ExpressionCodecError::InvalidShape(
        "function call is missing its authored binding ID",
    ))
}

fn allow_throw(
    reference: &SemanticParameterRef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<novarocks_proto_models::physical_semantics_v2::SemanticParameterRef, ExpressionCodecError>
{
    let valid_key = reference.expected_key == SemanticParameterKey::AllowThrowException;
    work.step()?;
    if !valid_key {
        return Err(ExpressionCodecError::InvalidShape(
            "intrinsic parameter reference has the wrong expected key",
        ));
    }
    Ok(encode_reference(reference, work)?)
}

fn expression_ids(
    ids: &[ExprId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u32>, ExpressionCodecError> {
    project(ids, work, |id| id.get())
}

fn project<T, U>(
    items: &[T],
    work: &mut CompileCheckpoints<'_>,
    mut map: impl FnMut(&T) -> U,
) -> Result<Vec<U>, ExpressionCodecError> {
    let mut output = Vec::new();
    // Allocation requests were admitted by the caller. Reserve is opaque;
    // preserve original control precedence on both success and refusal.
    work.flush()?;
    let reserved = output.try_reserve_exact(items.len());
    reserve_exit::<ExpressionCodecError>(reserved, work)?;
    for item in items {
        output.push(map(item));
        work.step()?;
    }
    Ok(output)
}

fn unary(op: UnaryOperator) -> i32 {
    (match op {
        UnaryOperator::Plus => wire::UnaryOperator::Plus,
        UnaryOperator::Minus => wire::UnaryOperator::Minus,
        UnaryOperator::Not => wire::UnaryOperator::Not,
        UnaryOperator::BitwiseNot => wire::UnaryOperator::BitwiseNot,
    }) as i32
}

fn binary(op: BinaryOperator) -> i32 {
    (match op {
        BinaryOperator::Add => wire::BinaryOperator::Add,
        BinaryOperator::Subtract => wire::BinaryOperator::Subtract,
        BinaryOperator::Multiply => wire::BinaryOperator::Multiply,
        BinaryOperator::Divide => wire::BinaryOperator::Divide,
        BinaryOperator::Modulo => wire::BinaryOperator::Modulo,
        BinaryOperator::Eq => wire::BinaryOperator::Eq,
        BinaryOperator::EqForNull => wire::BinaryOperator::EqForNull,
        BinaryOperator::NotEq => wire::BinaryOperator::NotEq,
        BinaryOperator::Lt => wire::BinaryOperator::Lt,
        BinaryOperator::LtEq => wire::BinaryOperator::LtEq,
        BinaryOperator::Gt => wire::BinaryOperator::Gt,
        BinaryOperator::GtEq => wire::BinaryOperator::GtEq,
        BinaryOperator::BitAnd => wire::BinaryOperator::BitAnd,
        BinaryOperator::BitOr => wire::BinaryOperator::BitOr,
        BinaryOperator::BitXor => wire::BinaryOperator::BitXor,
    }) as i32
}

fn window_frame(
    frame: &WindowFrame,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::WindowFrame, ExpressionCodecError> {
    let units = match frame.units {
        WindowFrameUnits::Rows => wire::WindowFrameUnits::Rows,
        WindowFrameUnits::Range => wire::WindowFrameUnits::Range,
        WindowFrameUnits::Groups => wire::WindowFrameUnits::Groups,
    } as i32;
    let exclusion = match frame.exclusion {
        WindowFrameExclusion::NoOthers => wire::WindowFrameExclusion::NoOthers,
        WindowFrameExclusion::CurrentRow => wire::WindowFrameExclusion::CurrentRow,
        WindowFrameExclusion::Group => wire::WindowFrameExclusion::Group,
        WindowFrameExclusion::Ties => wire::WindowFrameExclusion::Ties,
    } as i32;
    work.step()?;
    Ok(wire::WindowFrame {
        units,
        start: Some(window_bound(frame.start, work)?),
        end: Some(window_bound(frame.end, work)?),
        exclusion,
    })
}

fn window_bound(
    bound: WindowBound,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::WindowBound, ExpressionCodecError> {
    use wire::window_bound::Kind;
    let kind = match bound {
        WindowBound::UnboundedPreceding => Kind::UnboundedPreceding(physical_control_v2::Empty {}),
        WindowBound::Preceding(expr) => Kind::PrecedingExprId(expr.get()),
        WindowBound::CurrentRow => Kind::CurrentRow(physical_control_v2::Empty {}),
        WindowBound::Following(expr) => Kind::FollowingExprId(expr.get()),
        WindowBound::UnboundedFollowing => Kind::UnboundedFollowing(physical_control_v2::Empty {}),
    };
    work.step()?;
    Ok(wire::WindowBound { kind: Some(kind) })
}

#[cfg(test)]
mod tests {
    include!("kind_tests.rs");
}
