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

//! Fragment cast expression lowering.

use arrow::datatypes::DataType;

use super::{decode_expr_at, decode_type, nested};
use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::{common, expr};

use super::NativeExpressionInputLayout;

pub(crate) fn lower_cast(
    cast: &expr::CastExpr,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    let policy = super::decode_decimal_overflow_policy(
        cast.decimal_overflow_policy,
        path.clone().field("decimal_overflow_policy"),
    )?;
    let operand = cast.operand.as_ref().ok_or_else(|| {
        super::NativeExpressionDecodeError::missing(
            path.clone().field("operand"),
            "native Cast requires operand",
        )
    })?;
    let target = cast.target.as_ref().ok_or_else(|| {
        super::NativeExpressionDecodeError::missing(
            path.clone().field("target"),
            "native Cast requires target",
        )
    })?;
    let target_type = decode_type(target).map_err(|error| {
        super::NativeExpressionDecodeError::invalid_value(path.clone().field("target"), error)
    })?;
    if target_type != data_type {
        return Err(super::NativeExpressionDecodeError::inconsistent(
            path.clone().field("target"),
            format!("Cast target type {target_type:?} does not match Expr.type {data_type:?}"),
        ));
    }

    let source_type = super::decode_expr_type_at(operand, path.clone().field("operand"))?;
    if !novarocks_type_contract::decimal_error_policy_cast_supported(
        &source_type,
        &target_type,
        policy,
    ) {
        return Err(super::NativeExpressionDecodeError::unsupported(
            path.clone().field("decimal_overflow_policy"),
            "ReportError decimal CAST does not support nested type conversion",
        ));
    }
    let child = decode_expr_at(operand, path.clone().field("operand"), arena, input_layout)?;

    if matches!(data_type, DataType::LargeBinary) {
        let child_type = arena.data_type(child).ok_or_else(|| {
            super::NativeExpressionDecodeError::inconsistent(
                path.clone().field("operand"),
                "CAST child missing data type",
            )
        })?;
        if !nested::is_encoded_variant_payload_source(child_type) {
            return Err(super::NativeExpressionDecodeError::unsupported(
                path.clone().field("target"),
                "CAST to VARIANT is not supported",
            ));
        }
    }
    if let Some(child_type) = arena.data_type(child)
        && matches!(child_type, DataType::LargeBinary)
        && !matches!(data_type, DataType::LargeBinary)
    {
        let supported = matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::Date32
                | DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
        if !supported {
            return Err(super::NativeExpressionDecodeError::unsupported(
                path.clone().field("target"),
                "CAST from VARIANT is not supported",
            ));
        }
    }

    let target_primitive = scalar_primitive_from_type_desc(target, path.clone().field("target"))?;
    let source_primitive = operand
        .r#type
        .as_ref()
        .map(|desc| {
            scalar_primitive_from_type_desc(desc, path.clone().field("operand").field("type"))
        })
        .transpose()?
        .flatten();
    let node = if target_primitive == Some(common::PrimitiveType::Time) {
        if source_primitive == Some(common::PrimitiveType::Datetime) {
            ExprNode::CastTimeFromDatetime(child, policy)
        } else {
            ExprNode::CastTime(child, policy)
        }
    } else {
        ExprNode::Cast(child, policy)
    };
    Ok(arena.push_typed(node, data_type))
}

fn scalar_primitive_from_type_desc(
    desc: &common::TypeDesc,
    path: FieldPath,
) -> Result<Option<common::PrimitiveType>, super::NativeExpressionDecodeError> {
    let Some(common::type_desc::Kind::Scalar(scalar)) = desc.kind.as_ref() else {
        return Ok(None);
    };
    let primitive = common::PrimitiveType::try_from(scalar.r#type).map_err(|_| {
        super::NativeExpressionDecodeError::invalid_enum(
            path.clone(),
            format!("unknown primitive type {}", scalar.r#type),
        )
    })?;
    if primitive == common::PrimitiveType::Unspecified {
        return Err(super::NativeExpressionDecodeError::invalid_enum(
            path,
            "primitive type is unspecified",
        ));
    }
    Ok(Some(primitive))
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        col, lower_err_with_slots, lower_with_slots, map_string_json_type, scalar_expr, type_desc,
    };
    use super::{NativeExpressionInputLayout, lower_cast};
    use arrow::datatypes::{DataType, TimeUnit};
    use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode};
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_models::expr;

    #[test]
    fn cast_rejects_target_type_mismatch() {
        let expr = scalar_expr(
            DataType::Float64,
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                decimal_overflow_policy: expr::DecimalOverflowPolicy::OutputNull as i32,
                operand: Some(Box::new(col(1, DataType::Int64))),
                target: Some(type_desc(&DataType::Utf8)),
            })),
        );

        let err = lower_err_with_slots(&expr, &[1]);
        assert!(err.contains("Cast target type Utf8 does not match Expr.type Float64"));
    }

    #[test]
    fn cast_selects_time_special_case_nodes() {
        let time_type = DataType::Time64(TimeUnit::Microsecond);
        let datetime_type = DataType::Timestamp(TimeUnit::Microsecond, None);
        let datetime_to_time = scalar_expr(
            time_type.clone(),
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                decimal_overflow_policy: expr::DecimalOverflowPolicy::OutputNull as i32,
                operand: Some(Box::new(col(1, datetime_type))),
                target: Some(type_desc(&time_type)),
            })),
        );
        let int_to_time = scalar_expr(
            time_type.clone(),
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                decimal_overflow_policy: expr::DecimalOverflowPolicy::OutputNull as i32,
                operand: Some(Box::new(col(7, DataType::Int64))),
                target: Some(type_desc(&time_type)),
            })),
        );

        let (arena, id) = lower_with_slots(&datetime_to_time, &[1, 7]);
        assert!(matches!(
            arena.node(id),
            Some(ExprNode::CastTimeFromDatetime(
                _,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull
            ))
        ));

        let (arena, id) = lower_with_slots(&int_to_time, &[1, 7]);
        assert!(matches!(
            arena.node(id),
            Some(ExprNode::CastTime(
                _,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull
            ))
        ));
    }

    #[test]
    fn cast_preserves_nested_json_field_schema() {
        let map_type = map_string_json_type();
        let cast = scalar_expr(
            map_type.clone(),
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                decimal_overflow_policy: expr::DecimalOverflowPolicy::OutputNull as i32,
                operand: Some(Box::new(col(1, DataType::Utf8))),
                target: Some(type_desc(&map_type)),
            })),
        );

        let (arena, id) = lower_with_slots(&cast, &[1]);
        let field_schema = arena.field_schema(id).expect("cast field schema");
        assert!(
            field_schema
                .map_value()
                .is_some_and(|schema| schema.json_semantic())
        );
    }

    #[test]
    fn cast_preserves_variant_guards() {
        let scalar_to_variant = scalar_expr(
            DataType::LargeBinary,
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                decimal_overflow_policy: expr::DecimalOverflowPolicy::OutputNull as i32,
                operand: Some(Box::new(col(1, DataType::Int64))),
                target: Some(type_desc(&DataType::LargeBinary)),
            })),
        );
        let variant_to_decimal = scalar_expr(
            DataType::Decimal128(10, 2),
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                decimal_overflow_policy: expr::DecimalOverflowPolicy::OutputNull as i32,
                operand: Some(Box::new(col(1, DataType::LargeBinary))),
                target: Some(type_desc(&DataType::Decimal128(10, 2))),
            })),
        );

        let err = lower_err_with_slots(&scalar_to_variant, &[1]);
        assert!(err.contains("CAST to VARIANT is not supported"));
        let err = lower_err_with_slots(&variant_to_decimal, &[1]);
        assert!(err.contains("CAST from VARIANT is not supported"));
    }
    #[test]
    fn nested_reporting_decimal_cast_is_rejected_before_child_lowering() {
        use arrow::datatypes::Field;
        use novarocks_type_contract::DecimalOverflowPolicy as Policy;
        use std::sync::Arc;
        let source = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(10, 2),
            true,
        )));
        let target = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(12, 3),
            true,
        )));
        let cast = expr::CastExpr {
            operand: Some(Box::new(col(1, source.clone()))),
            target: Some(type_desc(&target)),
            decimal_overflow_policy: expr::DecimalOverflowPolicy::ReportError as i32,
        };
        let mut arena = ExprArena::default();
        let error = lower_cast(
            &cast,
            FieldPath::root("expr").field("cast"),
            &mut arena,
            &NativeExpressionInputLayout::default(),
            target.clone(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not support nested type conversion"),
            "{error}"
        );
        assert!(
            arena.node(ExprId(0)).is_none(),
            "policy is checked before mutating the arena"
        );
        let wire = scalar_expr(target.clone(), expr::expr::Kind::Cast(Box::new(cast)));
        let error =
            super::super::validate_proto_expr_shape_at(&wire, FieldPath::root("expr")).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not support nested type conversion"),
            "{error}"
        );
        for (result_type, wire_policy, expected_policy) in [
            (
                target,
                expr::DecimalOverflowPolicy::OutputNull,
                Policy::OutputNull,
            ),
            (
                source,
                expr::DecimalOverflowPolicy::ReportError,
                Policy::ReportError,
            ),
        ] {
            let operand = col(
                1,
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Decimal128(10, 2),
                    true,
                ))),
            );
            let wire = scalar_expr(
                result_type.clone(),
                expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                    operand: Some(Box::new(operand)),
                    target: Some(type_desc(&result_type)),
                    decimal_overflow_policy: wire_policy as i32,
                })),
            );
            let (arena, id) = lower_with_slots(&wire, &[1]);
            assert_eq!(arena.decimal_overflow_policy(id), Some(expected_policy));
        }
    }
}
