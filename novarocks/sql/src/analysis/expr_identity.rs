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

//! Observed identity of analyzed expressions. Selected constant equality is
//! independent of display text, source pool addresses and unused pool rows.
use super::{ExprKind, LambdaParam, SortItem, SubqueryKind, TypedExpr};
use crate::column_id::ColumnId;
use crate::compiler::SqlCompileError;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

pub(crate) fn typed_expr_semantically_eq(
    left: &TypedExpr,
    right: &TypedExpr,
    control: &dyn PureCompileControl,
) -> Result<bool, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = typed_expr_semantically_eq_with(left, right, &mut work);
    if matches!(
        &result,
        Err(SqlCompileError::Cancelled
            | SqlCompileError::DeadlineExceeded
            | SqlCompileError::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}

fn typed_expr_semantically_eq_with(
    left: &TypedExpr,
    right: &TypedExpr,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    let same_type = left
        .value_type
        .exactly_equals_observed::<novarocks_functions::FunctionBindingError>(
            &right.value_type,
            || work.step().map_err(Into::into),
        )?;
    if !same_type {
        return Ok(false);
    }
    work.step()?;

    Ok(match (&left.kind, &right.kind) {
        (
            ExprKind::ColumnRef {
                column_id: left_id,
                qualifier: left_qualifier,
                column: left_column,
            },
            ExprKind::ColumnRef {
                column_id: right_id,
                qualifier: right_qualifier,
                column: right_column,
            },
        ) => {
            if *left_id != ColumnId::UNSET && *right_id != ColumnId::UNSET {
                left_id == right_id
            } else {
                left_qualifier.as_ref().map(|q| q.to_lowercase())
                    == right_qualifier.as_ref().map(|q| q.to_lowercase())
                    && left_column.eq_ignore_ascii_case(right_column)
            }
        }
        (
            ExprKind::LambdaParamRef {
                name: left_name,
                slot_id: left_slot,
            },
            ExprKind::LambdaParamRef {
                name: right_name,
                slot_id: right_slot,
            },
        ) => left_slot == right_slot && left_name.eq_ignore_ascii_case(right_name),
        (ExprKind::Literal(left), ExprKind::Literal(right)) => left == right,
        (ExprKind::Constant(left_value), ExprKind::Constant(right_value)) => {
            for (declared, source) in [
                (&left.value_type, left_value.value_type()),
                (&right.value_type, right_value.value_type()),
            ] {
                if !declared.exactly_equals_observed::<novarocks_constant_contract::ConstantError>(
                    source,
                    || work.step().map_err(Into::into),
                )? {
                    return Err(SqlCompileError::InvalidRequest(
                        "constant source differs from its frozen analyzed expression type".into(),
                    ));
                }
            }
            work.flush()?;
            left_value.equals_observed(right_value, CompilePhase::LowerProgram, work.control())?
        }
        (
            ExprKind::BinaryOp {
                left: left_left,
                op: left_op,
                right: left_right,
                decimal_overflow_policy: left_policy,
            },
            ExprKind::BinaryOp {
                left: right_left,
                op: right_op,
                right: right_right,
                decimal_overflow_policy: right_policy,
            },
        ) => {
            left_policy == right_policy
                && left_op == right_op
                && typed_expr_semantically_eq_with(left_left, right_left, work)?
                && typed_expr_semantically_eq_with(left_right, right_right, work)?
        }
        (
            ExprKind::UnaryOp {
                op: left_op,
                expr: left_expr,
            },
            ExprKind::UnaryOp {
                op: right_op,
                expr: right_expr,
            },
        ) => left_op == right_op && typed_expr_semantically_eq_with(left_expr, right_expr, work)?,
        (
            ExprKind::FunctionCall {
                name: left_name,
                args: left_args,
                distinct: left_distinct,
                binding: left_binding,
                volatility: left_volatility,
            },
            ExprKind::FunctionCall {
                name: right_name,
                args: right_args,
                distinct: right_distinct,
                binding: right_binding,
                volatility: right_volatility,
            },
        ) => {
            left_name.eq_ignore_ascii_case(right_name)
                && left_distinct == right_distinct
                && left_binding == right_binding
                && left_volatility == right_volatility
                && typed_expr_slices_semantically_eq_with(left_args, right_args, work)?
        }
        (
            ExprKind::LambdaFunction {
                params: left_params,
                body: left_body,
            },
            ExprKind::LambdaFunction {
                params: right_params,
                body: right_body,
            },
        ) => {
            lambda_parameters_semantically_eq_with(left_params, right_params, work)?
                && typed_expr_semantically_eq_with(left_body, right_body, work)?
        }
        (
            ExprKind::AggregateCall {
                name: left_name,
                args: left_args,
                distinct: left_distinct,
                order_by: left_order_by,
                resolved: left_resolved,
            },
            ExprKind::AggregateCall {
                name: right_name,
                args: right_args,
                distinct: right_distinct,
                order_by: right_order_by,
                resolved: right_resolved,
            },
        ) => {
            left_resolved == right_resolved
                && left_name.eq_ignore_ascii_case(right_name)
                && left_distinct == right_distinct
                && typed_expr_slices_semantically_eq_with(left_args, right_args, work)?
                && sort_item_slices_semantically_eq_with(left_order_by, right_order_by, work)?
        }
        (
            ExprKind::Cast {
                expr: left_expr,
                target: left_target,
                decimal_overflow_policy: left_policy,
            },
            ExprKind::Cast {
                expr: right_expr,
                target: right_target,
                decimal_overflow_policy: right_policy,
            },
        ) => {
            left_policy == right_policy
                && left_target == right_target
                && typed_expr_semantically_eq_with(left_expr, right_expr, work)?
        }
        (
            ExprKind::IsNull {
                expr: left_expr,
                negated: left_negated,
            },
            ExprKind::IsNull {
                expr: right_expr,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_semantically_eq_with(left_expr, right_expr, work)?
        }
        (
            ExprKind::InList {
                expr: left_expr,
                list: left_list,
                negated: left_negated,
            },
            ExprKind::InList {
                expr: right_expr,
                list: right_list,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_semantically_eq_with(left_expr, right_expr, work)?
                && typed_expr_slices_semantically_eq_with(left_list, right_list, work)?
        }
        (
            ExprKind::Between {
                expr: left_expr,
                low: left_low,
                high: left_high,
                negated: left_negated,
            },
            ExprKind::Between {
                expr: right_expr,
                low: right_low,
                high: right_high,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_semantically_eq_with(left_expr, right_expr, work)?
                && typed_expr_semantically_eq_with(left_low, right_low, work)?
                && typed_expr_semantically_eq_with(left_high, right_high, work)?
        }
        (
            ExprKind::Like {
                expr: left_expr,
                pattern: left_pattern,
                negated: left_negated,
            },
            ExprKind::Like {
                expr: right_expr,
                pattern: right_pattern,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_semantically_eq_with(left_expr, right_expr, work)?
                && typed_expr_semantically_eq_with(left_pattern, right_pattern, work)?
        }
        (
            ExprKind::Case {
                operand: left_operand,
                when_then: left_when_then,
                else_expr: left_else,
            },
            ExprKind::Case {
                operand: right_operand,
                when_then: right_when_then,
                else_expr: right_else,
            },
        ) => {
            optional_typed_expr_semantically_eq_with(
                left_operand.as_deref(),
                right_operand.as_deref(),
                work,
            )? && left_when_then.len() == right_when_then.len()
                && left_when_then.iter().zip(right_when_then.iter()).try_fold(
                    true,
                    |matched, ((left_when, left_then), (right_when, right_then))| {
                        Ok::<bool, SqlCompileError>(
                            matched
                                && typed_expr_semantically_eq_with(left_when, right_when, work)?
                                && typed_expr_semantically_eq_with(left_then, right_then, work)?,
                        )
                    },
                )?
                && optional_typed_expr_semantically_eq_with(
                    left_else.as_deref(),
                    right_else.as_deref(),
                    work,
                )?
        }
        (
            ExprKind::IsTruthValue {
                expr: left_expr,
                value: left_value,
                negated: left_negated,
            },
            ExprKind::IsTruthValue {
                expr: right_expr,
                value: right_value,
                negated: right_negated,
            },
        ) => {
            left_value == right_value
                && left_negated == right_negated
                && typed_expr_semantically_eq_with(left_expr, right_expr, work)?
        }
        (ExprKind::Nested(left), ExprKind::Nested(right)) => {
            typed_expr_semantically_eq_with(left, right, work)?
        }
        (
            ExprKind::WindowCall {
                name: left_name,
                args: left_args,
                distinct: left_distinct,
                binding: left_binding,
                function_order_by: left_function_order_by,
                aggregate_binding: left_aggregate_binding,
                partition_by: left_partition_by,
                order_by: left_order_by,
                window_frame: left_frame,
                ignore_nulls: left_ignore_nulls,
            },
            ExprKind::WindowCall {
                name: right_name,
                args: right_args,
                distinct: right_distinct,
                binding: right_binding,
                function_order_by: right_function_order_by,
                aggregate_binding: right_aggregate_binding,
                partition_by: right_partition_by,
                order_by: right_order_by,
                window_frame: right_frame,
                ignore_nulls: right_ignore_nulls,
            },
        ) => {
            left_name.eq_ignore_ascii_case(right_name)
                && left_distinct == right_distinct
                && left_binding == right_binding
                && left_aggregate_binding == right_aggregate_binding
                && left_ignore_nulls == right_ignore_nulls
                && left_frame == right_frame
                && typed_expr_slices_semantically_eq_with(left_args, right_args, work)?
                && sort_item_slices_semantically_eq_with(
                    left_function_order_by,
                    right_function_order_by,
                    work,
                )?
                && typed_expr_slices_semantically_eq_with(
                    left_partition_by,
                    right_partition_by,
                    work,
                )?
                && sort_item_slices_semantically_eq_with(left_order_by, right_order_by, work)?
        }
        (
            ExprKind::SubqueryPlaceholder {
                id: left_id,
                kind: left_kind,
                data_type: left_type,
            },
            ExprKind::SubqueryPlaceholder {
                id: right_id,
                kind: right_kind,
                data_type: right_type,
            },
        ) => {
            left_id == right_id
                && match (left_kind, right_kind) {
                    (SubqueryKind::Scalar, SubqueryKind::Scalar) => true,
                    (
                        SubqueryKind::Exists { negated: left },
                        SubqueryKind::Exists { negated: right },
                    )
                    | (
                        SubqueryKind::InSubquery { negated: left },
                        SubqueryKind::InSubquery { negated: right },
                    ) => left == right,
                    _ => false,
                }
                && left_type == right_type
        }
        (
            ExprKind::Lambda {
                params: left_params,
                body: left_body,
            },
            ExprKind::Lambda {
                params: right_params,
                body: right_body,
            },
        ) => {
            left_params == right_params
                && typed_expr_semantically_eq_with(left_body, right_body, work)?
        }
        _ => false,
    })
}

fn lambda_parameters_semantically_eq_with(
    left: &[LambdaParam],
    right: &[LambdaParam],
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        work.step()?;
        if left.slot_id != right.slot_id
            || left.name != right.name
            || !left
                .value_type
                .exactly_equals_observed::<novarocks_constant_contract::ConstantError>(
                    &right.value_type,
                    || work.step().map_err(Into::into),
                )?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn optional_typed_expr_semantically_eq_with(
    left: Option<&TypedExpr>,
    right: Option<&TypedExpr>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    Ok(match (left, right) {
        (Some(left), Some(right)) => typed_expr_semantically_eq_with(left, right, work)?,
        (None, None) => true,
        _ => false,
    })
}

fn typed_expr_slices_semantically_eq_with(
    left: &[TypedExpr],
    right: &[TypedExpr],
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    Ok(left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .try_fold(true, |matched, (left, right)| {
                Ok::<bool, SqlCompileError>(
                    matched && typed_expr_semantically_eq_with(left, right, work)?,
                )
            })?)
}

fn sort_item_semantically_eq_with(
    left: &SortItem,
    right: &SortItem,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    Ok(left.asc == right.asc
        && left.nulls_first == right.nulls_first
        && typed_expr_semantically_eq_with(&left.expr, &right.expr, work)?)
}

fn sort_item_slices_semantically_eq_with(
    left: &[SortItem],
    right: &[SortItem],
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    Ok(left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .try_fold(true, |matched, (left, right)| {
                Ok::<bool, SqlCompileError>(
                    matched && sort_item_semantically_eq_with(left, right, work)?,
                )
            })?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use novarocks_type_contract::FunctionValueType;
    use std::sync::Arc;

    #[test]
    fn lambda_identity_keeps_parameter_slot_name_and_complete_source_type() {
        let parameter = LambdaParam {
            name: "x".into(),
            slot_id: 7,
            value_type: FunctionValueType::new(
                DataType::List(Arc::new(
                    Field::new("provider_item", DataType::Int64, false).with_metadata(
                        std::collections::HashMap::<String, String>::from([(
                            "provider.id".into(),
                            "19".into(),
                        )]),
                    ),
                )),
                false,
            ),
        };
        let expression = |parameter: LambdaParam| TypedExpr {
            kind: ExprKind::LambdaFunction {
                params: vec![parameter],
                body: Box::new(TypedExpr {
                    kind: ExprKind::Literal(super::super::LiteralValue::Int(1)),
                    value_type: FunctionValueType::new(DataType::Int64, false),
                }),
            },
            value_type: FunctionValueType::new(DataType::Int64, false),
        };
        let left = expression(parameter.clone());
        let control = crate::compiler::SqlCompileControl::unbounded();
        assert!(
            typed_expr_semantically_eq(&left, &expression(parameter.clone()), &control).unwrap()
        );
        let mut changed_slot = parameter.clone();
        changed_slot.slot_id = 8;
        let mut changed_name = parameter.clone();
        changed_name.name = "y".into();
        let mut changed_type = parameter.clone();
        changed_type.value_type = FunctionValueType::new(
            DataType::List(Arc::new(
                Field::new("provider_item", DataType::Int64, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider.id".into(),
                        "20".into(),
                    )]),
                ),
            )),
            false,
        );
        assert!(typed_expr_semantically_eq(&left, &expression(changed_type), &control).unwrap());
        let mut changed_nullability = parameter.clone();
        changed_nullability.value_type = FunctionValueType::new(
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            false,
        );
        for changed in [changed_slot, changed_name, changed_nullability] {
            assert!(!typed_expr_semantically_eq(&left, &expression(changed), &control).unwrap());
        }
    }
}
