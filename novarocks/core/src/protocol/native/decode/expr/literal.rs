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

//! Literal expression lowering.

use arrow::datatypes::DataType;
use arrow_buffer::i256;

use crate::protocol::common::error::{FieldPath, ProtocolErrorKind};
use crate::protocol::native::decode::error::NativeFragmentLeafDecodeError;
use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use novarocks_protocol::{common, expr};

pub(crate) fn lower_literal(
    literal: &expr::LiteralExpr,
    data_type: &DataType,
) -> Result<LiteralValue, super::super::NativeFragmentDecodeError> {
    lower_literal_at(literal, data_type)
        .map_err(|error| error.into_native(FieldPath::root("expr").field("literal")))
}

pub(super) fn lower_literal_at(
    literal: &expr::LiteralExpr,
    data_type: &DataType,
) -> Result<LiteralValue, NativeFragmentLeafDecodeError> {
    let value = literal.value.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "value",
            "LiteralExpr.value missing",
        )
    })?;
    let value = value.value.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "value",
            "LiteralValue.value missing",
        )
        .append_field("value")
    })?;
    use common::literal_value::Value;
    match value {
        Value::NullValue(true) => Ok(LiteralValue::Null),
        Value::NullValue(false) => Err(literal_value_error(
            ProtocolErrorKind::InvalidValue,
            "null_value",
            "LiteralValue.null_value must be true",
        )),
        Value::BoolValue(value) => {
            require_literal_type(
                data_type,
                matches!(data_type, DataType::Boolean),
                "bool_value",
                "bool literal",
            )?;
            Ok(LiteralValue::Bool(*value))
        }
        Value::IntValue(value) => lower_int_literal(*value, data_type),
        Value::LargeintValue(bytes) => {
            require_literal_type(
                data_type,
                novarocks_types::largeint::is_largeint_data_type(data_type),
                "largeint_value",
                "largeint literal",
            )?;
            novarocks_types::largeint::i128_from_be_bytes(bytes)
                .map(LiteralValue::LargeInt)
                .map_err(|error| {
                    literal_value_error(ProtocolErrorKind::InvalidValue, "largeint_value", error)
                })
        }
        Value::FloatValue(value) => match data_type {
            DataType::Float32 => Ok(LiteralValue::Float32(*value as f32)),
            DataType::Float64 => Ok(LiteralValue::Float64(*value)),
            _ => Err(literal_value_error(
                ProtocolErrorKind::InvalidValue,
                "float_value",
                format!("float literal cannot be lowered as {data_type:?}"),
            )),
        },
        Value::StringValue(value) => {
            require_literal_type(
                data_type,
                matches!(data_type, DataType::Utf8 | DataType::LargeUtf8),
                "string_value",
                "string literal",
            )?;
            Ok(LiteralValue::Utf8(value.clone()))
        }
        Value::BinaryValue(value) => {
            require_literal_type(
                data_type,
                matches!(data_type, DataType::Binary | DataType::LargeBinary),
                "binary_value",
                "binary literal",
            )?;
            Ok(LiteralValue::Binary(value.clone()))
        }
        Value::Date32Value(value) => {
            require_literal_type(
                data_type,
                matches!(data_type, DataType::Date32),
                "date32_value",
                "date32 literal",
            )?;
            Ok(LiteralValue::Date32(*value))
        }
        Value::DecimalValue(decimal) => lower_decimal_literal(decimal, data_type)
            .map_err(|error| error.prepend_field("decimal_value").prepend_field("value")),
    }
}

fn literal_value_error(
    kind: ProtocolErrorKind,
    variant: &'static str,
    detail: impl std::fmt::Display,
) -> NativeFragmentLeafDecodeError {
    NativeFragmentLeafDecodeError::at_field(kind, "value", detail).append_field(variant)
}

fn require_literal_type(
    data_type: &DataType,
    ok: bool,
    variant: &'static str,
    context: &str,
) -> Result<(), NativeFragmentLeafDecodeError> {
    if ok {
        Ok(())
    } else {
        Err(literal_value_error(
            ProtocolErrorKind::InvalidValue,
            variant,
            format!("{context} cannot be lowered as {data_type:?}"),
        ))
    }
}

fn lower_int_literal(
    value: i64,
    data_type: &DataType,
) -> Result<LiteralValue, NativeFragmentLeafDecodeError> {
    let out_of_range = || {
        literal_value_error(
            ProtocolErrorKind::OutOfRange,
            "int_value",
            format!("int literal {value} is outside {data_type:?} range"),
        )
    };
    match data_type {
        DataType::Int8 => i8::try_from(value)
            .map(LiteralValue::Int8)
            .map_err(|_| out_of_range()),
        DataType::Int16 => i16::try_from(value)
            .map(LiteralValue::Int16)
            .map_err(|_| out_of_range()),
        DataType::Int32 => i32::try_from(value)
            .map(LiteralValue::Int32)
            .map_err(|_| out_of_range()),
        DataType::Int64 => Ok(LiteralValue::Int64(value)),
        DataType::Date32 => i32::try_from(value)
            .map(LiteralValue::Date32)
            .map_err(|_| out_of_range()),
        _ => Err(literal_value_error(
            ProtocolErrorKind::InvalidValue,
            "int_value",
            format!("int literal cannot be lowered as {data_type:?}"),
        )),
    }
}

fn lower_decimal_literal(
    decimal: &common::DecimalLiteral,
    data_type: &DataType,
) -> Result<LiteralValue, NativeFragmentLeafDecodeError> {
    let precision = u8::try_from(decimal.precision).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "precision",
            format!("invalid decimal precision {}", decimal.precision),
        )
    })?;
    let scale = i8::try_from(decimal.scale).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "scale",
            format!("invalid decimal scale {}", decimal.scale),
        )
    })?;
    validate_decimal_parts(precision, scale)?;

    match data_type {
        DataType::Decimal128(expected_precision, expected_scale) => {
            validate_decimal_type_match(precision, scale, *expected_precision, *expected_scale)?;
            let bytes = decimal_bytes::<16>(&decimal.value, "Decimal128")?;
            Ok(LiteralValue::Decimal128 {
                value: i128::from_be_bytes(bytes),
                precision,
                scale,
            })
        }
        DataType::Decimal256(expected_precision, expected_scale) => {
            validate_decimal_type_match(precision, scale, *expected_precision, *expected_scale)?;
            let bytes = decimal_bytes::<32>(&decimal.value, "Decimal256")?;
            Ok(LiteralValue::Decimal256 {
                value: i256::from_be_bytes(bytes),
                precision,
                scale,
            })
        }
        _ => Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "value",
            format!("decimal literal requires Decimal128/Decimal256 type, got {data_type:?}"),
        )),
    }
}

fn validate_decimal_parts(precision: u8, scale: i8) -> Result<(), NativeFragmentLeafDecodeError> {
    if precision == 0 || precision > 76 {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "precision",
            format!("decimal precision {precision} must be between 1 and 76"),
        ));
    }
    if scale < 0 || scale > precision as i8 {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "scale",
            format!("decimal scale {scale} must be between 0 and precision {precision}"),
        ));
    }
    Ok(())
}

fn validate_decimal_type_match(
    precision: u8,
    scale: i8,
    expected_precision: u8,
    expected_scale: i8,
) -> Result<(), NativeFragmentLeafDecodeError> {
    if precision != expected_precision {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "precision",
            format!(
                "decimal literal precision {precision} does not match Expr.type precision {expected_precision}"
            ),
        ));
    }
    if scale != expected_scale {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "scale",
            format!(
                "decimal literal scale {scale} does not match Expr.type scale {expected_scale}"
            ),
        ));
    }
    Ok(())
}

fn decimal_bytes<const N: usize>(
    value: &[u8],
    label: &str,
) -> Result<[u8; N], NativeFragmentLeafDecodeError> {
    value.try_into().map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "value",
            format!("{label} literal requires {N} bytes, got {}", value.len()),
        )
    })
}

pub(super) fn push_zero_literal(
    arena: &mut ExprArena,
    data_type: &DataType,
) -> Result<ExprId, String> {
    let literal = match data_type {
        DataType::Int8 => LiteralValue::Int8(0),
        DataType::Int16 => LiteralValue::Int16(0),
        DataType::Int32 => LiteralValue::Int32(0),
        DataType::Int64 => LiteralValue::Int64(0),
        DataType::Float32 => LiteralValue::Float32(0.0),
        DataType::Float64 => LiteralValue::Float64(0.0),
        DataType::Decimal128(precision, scale) => LiteralValue::Decimal128 {
            value: 0,
            precision: *precision,
            scale: *scale,
        },
        DataType::Decimal256(precision, scale) => LiteralValue::Decimal256 {
            value: i256::ZERO,
            precision: *precision,
            scale: *scale,
        },
        dt if novarocks_types::largeint::is_largeint_data_type(dt) => LiteralValue::LargeInt(0),
        _ => {
            return Err(format!(
                "NEGATE is not supported for data type {data_type:?}"
            ));
        }
    };
    Ok(arena.push_typed(ExprNode::Literal(literal), data_type.clone()))
}

#[cfg(test)]
mod tests {
    use super::super::tests::{bool_lit, int_lit, lower, scalar_expr, string_lit};
    use crate::protocol::common::error::ProtocolErrorKind;
    use arrow::datatypes::DataType;
    use arrow_buffer::i256;
    use novarocks_execution::exec::expr::{ExprNode, LiteralValue};
    use novarocks_protocol::{common, expr};

    fn literal_error(
        literal: expr::LiteralExpr,
        data_type: DataType,
    ) -> super::super::super::NativeFragmentDecodeError {
        super::lower_literal(&literal, &data_type).expect_err("invalid literal must fail")
    }

    fn assert_literal_error(
        literal: expr::LiteralExpr,
        data_type: DataType,
        expected_path: &str,
        expected_kind: ProtocolErrorKind,
    ) {
        let error = literal_error(literal, data_type);
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.path().to_string(), expected_path);
        assert_eq!(protocol.kind(), expected_kind);
    }

    #[test]
    fn false_null_marker_uses_exact_path_and_kind() {
        assert_literal_error(
            expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::NullValue(false)),
                }),
            },
            DataType::Int64,
            "expr.literal.value.null_value",
            ProtocolErrorKind::InvalidValue,
        );
    }

    #[test]
    fn literal_type_mismatch_uses_exact_value_variant_path_and_kind() {
        assert_literal_error(
            expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::BoolValue(true)),
                }),
            },
            DataType::Int64,
            "expr.literal.value.bool_value",
            ProtocolErrorKind::InvalidValue,
        );
    }

    #[test]
    fn decimal_precision_uses_exact_nested_path_and_kind() {
        assert_literal_error(
            expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::DecimalValue(
                        common::DecimalLiteral {
                            value: 0i128.to_be_bytes().to_vec(),
                            precision: 0,
                            scale: 0,
                        },
                    )),
                }),
            },
            DataType::Decimal128(10, 2),
            "expr.literal.value.decimal_value.precision",
            ProtocolErrorKind::OutOfRange,
        );
    }

    #[test]
    fn decimal_scale_uses_exact_nested_path_and_kind() {
        assert_literal_error(
            expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::DecimalValue(
                        common::DecimalLiteral {
                            value: 0i128.to_be_bytes().to_vec(),
                            precision: 10,
                            scale: 11,
                        },
                    )),
                }),
            },
            DataType::Decimal128(10, 2),
            "expr.literal.value.decimal_value.scale",
            ProtocolErrorKind::OutOfRange,
        );
    }

    #[test]
    fn decimal_value_uses_exact_nested_path_and_kind() {
        assert_literal_error(
            expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::DecimalValue(
                        common::DecimalLiteral {
                            value: vec![1],
                            precision: 10,
                            scale: 2,
                        },
                    )),
                }),
            },
            DataType::Decimal128(10, 2),
            "expr.literal.value.decimal_value.value",
            ProtocolErrorKind::InvalidValue,
        );
    }

    #[test]
    fn lowers_typed_literals() {
        let cases = vec![
            scalar_expr(
                DataType::Int32,
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::NullValue(true)),
                    }),
                }),
            ),
            bool_lit(true),
            int_lit(123),
            scalar_expr(
                DataType::Float64,
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::FloatValue(1.25)),
                    }),
                }),
            ),
            string_lit("abc"),
            scalar_expr(
                DataType::Binary,
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::BinaryValue(vec![1, 2, 3])),
                    }),
                }),
            ),
            scalar_expr(
                DataType::Date32,
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::Date32Value(20_000)),
                    }),
                }),
            ),
            scalar_expr(
                DataType::FixedSizeBinary(16),
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::LargeintValue(
                            (-12_345i128).to_be_bytes().to_vec(),
                        )),
                    }),
                }),
            ),
            scalar_expr(
                DataType::Decimal128(10, 2),
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::DecimalValue(
                            common::DecimalLiteral {
                                value: 12345i128.to_be_bytes().to_vec(),
                                precision: 10,
                                scale: 2,
                            },
                        )),
                    }),
                }),
            ),
            scalar_expr(
                DataType::Decimal256(40, 3),
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::DecimalValue(
                            common::DecimalLiteral {
                                value: i256::from_i128(123_456).to_be_bytes().to_vec(),
                                precision: 40,
                                scale: 3,
                            },
                        )),
                    }),
                }),
            ),
        ];

        for expr in cases {
            let (arena, id) = lower(&expr);
            assert!(matches!(arena.node(id), Some(ExprNode::Literal(_))));
        }

        let (arena, id) = lower(&scalar_expr(
            DataType::Decimal128(10, 2),
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::DecimalValue(
                        common::DecimalLiteral {
                            value: 12345i128.to_be_bytes().to_vec(),
                            precision: 10,
                            scale: 2,
                        },
                    )),
                }),
            }),
        ));
        assert!(matches!(
            arena.node(id),
            Some(ExprNode::Literal(LiteralValue::Decimal128 {
                value: 12345,
                precision: 10,
                scale: 2
            }))
        ));
    }
}
