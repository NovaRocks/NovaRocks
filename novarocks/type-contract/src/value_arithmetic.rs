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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Full value-domain classification and the frozen arithmetic result rule.
//! Numeric carriers never confer a logical identity on opaque values.

use arrow_schema::DataType;

use crate::{ArithmeticOperator, FunctionValueType, ValueLogicalType};

/// Classify an admitted integer value. This is not a complete schema validator.
pub fn is_integer_value_type(value: &FunctionValueType) -> bool {
    match value.logical_type() {
        ValueLogicalType::LargeInt => crate::is_largeint_data_type(value.data_type()),
        ValueLogicalType::Physical => matches!(
            value.data_type(),
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        ),
        _ => false,
    }
}

/// Preserve numeric classification independently of binary arithmetic support.
/// A classified carrier can still be outside a particular frozen operator rule.
pub fn is_numeric_value_type(value: &FunctionValueType) -> bool {
    is_integer_value_type(value)
        || (value.logical_type() == ValueLogicalType::Physical
            && matches!(
                value.data_type(),
                DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal32(_, _)
                    | DataType::Decimal64(_, _)
                    | DataType::Decimal128(_, _)
                    | DataType::Decimal256(_, _)
            ))
}

/// Apply the existing carrier arithmetic algorithm only to exact numeric domains.
/// Unsupported pairs remain unsupported; callers authorize any explicit casts.
/// The result admits exactly the operands' NULLs. A validator may separately
/// allow an outer NULL widening required by its operation's failure policy.
pub fn arithmetic_result_value_type_with_op(
    left: &FunctionValueType,
    right: &FunctionValueType,
    op: ArithmeticOperator,
) -> Option<FunctionValueType> {
    let (data_type, logical_type) = arithmetic_result_value_shape_with_op(left, right, op)?;
    FunctionValueType::try_with_logical_type(
        data_type,
        left.nullable() || right.nullable(),
        logical_type,
    )
    .ok()
}

/// Borrowed validation uses this same arithmetic decision without constructing
/// an otherwise discarded semantic/layout owner. The decision never infers a
/// nominal domain from a non-numeric carrier.
pub fn arithmetic_result_value_shape_with_op(
    left: &FunctionValueType,
    right: &FunctionValueType,
    op: ArithmeticOperator,
) -> Option<(DataType, ValueLogicalType)> {
    if !is_numeric_value_type(left) || !is_numeric_value_type(right) {
        return None;
    }
    let data_type = crate::arithmetic_result_type_with_op(left.data_type(), right.data_type(), op)?;
    let logical_type = if crate::is_largeint_data_type(&data_type) {
        // Physical fixed binary was rejected above. This result is authored
        // solely by the frozen algorithm acting on exact LARGEINT input.
        ValueLogicalType::LargeInt
    } else {
        ValueLogicalType::Physical
    };
    Some((data_type, logical_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn largeint(nullable: bool) -> FunctionValueType {
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            nullable,
            ValueLogicalType::LargeInt,
        )
        .unwrap()
    }

    #[test]
    fn signed_common_width_keeps_the_original_domain_and_nullable_union() {
        let small = FunctionValueType::new(DataType::Int16, false);
        for nullable in [false, true] {
            let integer = FunctionValueType::new(DataType::Int32, nullable);
            for (left, right) in [(&small, &integer), (&integer, &small)] {
                for op in [
                    ArithmeticOperator::Add,
                    ArithmeticOperator::Subtract,
                    ArithmeticOperator::Multiply,
                    ArithmeticOperator::Divide,
                    ArithmeticOperator::Modulo,
                ] {
                    let expected = FunctionValueType::new(DataType::Int32, nullable);
                    assert_eq!(
                        arithmetic_result_value_type_with_op(left, right, op),
                        Some(expected)
                    );
                    assert_eq!(
                        arithmetic_result_value_shape_with_op(left, right, op),
                        Some((DataType::Int32, ValueLogicalType::Physical))
                    );
                }
            }
        }
    }

    #[test]
    fn classifications_preserve_all_physical_numeric_carriers_and_exact_largeint() {
        for data_type in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            let value = FunctionValueType::from_legacy_declared_projection(data_type, false);
            assert!(is_integer_value_type(&value));
            assert!(is_numeric_value_type(&value));
        }
        for data_type in [
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal32(7, 2),
            DataType::Decimal64(10, 2),
            DataType::Decimal128(38, 2),
            DataType::Decimal256(76, 2),
        ] {
            let value = FunctionValueType::from_legacy_declared_projection(data_type, true);
            assert!(!is_integer_value_type(&value));
            assert!(is_numeric_value_type(&value));
        }
        assert!(is_integer_value_type(&largeint(false)));
        assert!(is_numeric_value_type(&largeint(true)));
    }

    #[test]
    fn opaque_and_malformed_tags_never_become_numeric_from_the_carrier() {
        for value in [
            FunctionValueType::new(DataType::FixedSizeBinary(16), false),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
        ] {
            assert!(!is_integer_value_type(&value));
            assert!(!is_numeric_value_type(&value));
            for op in [
                ArithmeticOperator::Add,
                ArithmeticOperator::Subtract,
                ArithmeticOperator::Multiply,
                ArithmeticOperator::Divide,
                ArithmeticOperator::Modulo,
            ] {
                assert_eq!(
                    arithmetic_result_value_type_with_op(&value, &largeint(false), op),
                    None
                );
                assert_eq!(
                    arithmetic_result_value_type_with_op(&largeint(false), &value, op),
                    None
                );
            }
        }
        for logical in [ValueLogicalType::LargeInt, ValueLogicalType::Json] {
            assert_eq!(
                FunctionValueType::try_with_logical_type(DataType::Int64, false, logical),
                Err(crate::ValueTypeError::InvalidLogicalCarrier(logical))
            );
        }
    }

    #[test]
    fn all_admitted_carrier_rules_are_preserved_with_root_nullability_or() {
        let inputs = [
            FunctionValueType::new(DataType::Int8, false),
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::UInt64, false),
            FunctionValueType::new(DataType::Float16, false),
            FunctionValueType::new(DataType::Float32, false),
            FunctionValueType::new(DataType::Float64, true),
            FunctionValueType::new(DataType::Decimal128(20, 6), false),
            FunctionValueType::new(DataType::Decimal256(55, 15), true),
            largeint(false),
        ];
        for left in &inputs {
            for right in &inputs {
                for op in [
                    ArithmeticOperator::Add,
                    ArithmeticOperator::Subtract,
                    ArithmeticOperator::Multiply,
                    ArithmeticOperator::Divide,
                    ArithmeticOperator::Modulo,
                ] {
                    let carrier = crate::arithmetic_result_type_with_op(
                        left.data_type(),
                        right.data_type(),
                        op,
                    );
                    let result = arithmetic_result_value_type_with_op(left, right, op);
                    assert_eq!(result.as_ref().map(|ty| ty.data_type().clone()), carrier);
                    if let Some(result) = result {
                        assert_eq!(result.nullable(), left.nullable() || right.nullable());
                        assert_eq!(
                            result.logical_type(),
                            if crate::is_largeint_data_type(result.data_type()) {
                                ValueLogicalType::LargeInt
                            } else {
                                ValueLogicalType::Physical
                            }
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn frozen_decimal_largeint_width_and_division_domains_are_exact() {
        let decimal = FunctionValueType::new(DataType::Decimal128(38, 15), true);
        for op in [ArithmeticOperator::Add, ArithmeticOperator::Subtract] {
            assert_eq!(
                arithmetic_result_value_type_with_op(&decimal, &largeint(false), op),
                Some(FunctionValueType::new(DataType::Decimal256(55, 15), true))
            );
            assert_eq!(
                arithmetic_result_value_type_with_op(&largeint(false), &decimal, op),
                Some(FunctionValueType::new(DataType::Decimal256(55, 15), true))
            );
        }
        for op in [
            ArithmeticOperator::Multiply,
            ArithmeticOperator::Divide,
            ArithmeticOperator::Modulo,
        ] {
            assert_eq!(
                arithmetic_result_value_type_with_op(&decimal, &largeint(false), op),
                None
            );
        }
        assert_eq!(
            arithmetic_result_value_type_with_op(
                &largeint(false),
                &largeint(false),
                ArithmeticOperator::Divide
            ),
            Some(FunctionValueType::new(DataType::Float64, false))
        );
        assert_eq!(
            arithmetic_result_value_type_with_op(
                &largeint(false),
                &FunctionValueType::new(DataType::Int64, false),
                ArithmeticOperator::Add
            ),
            Some(largeint(false))
        );
    }
    #[test]
    fn borrowed_arithmetic_decision_matches_the_original_complete_author() {
        let plain = FunctionValueType::new(DataType::Int64, false);
        let integer = largeint(true);
        for left in [&plain, &integer] {
            for right in [&plain, &integer] {
                for op in [
                    ArithmeticOperator::Add,
                    ArithmeticOperator::Subtract,
                    ArithmeticOperator::Multiply,
                    ArithmeticOperator::Divide,
                ] {
                    let raw = arithmetic_result_value_shape_with_op(left, right, op);
                    let owned = arithmetic_result_value_type_with_op(left, right, op);
                    assert_eq!(
                        raw.as_ref().map(|(carrier, domain)| (carrier, *domain)),
                        owned.as_ref().map(|ty| (ty.data_type(), ty.logical_type()))
                    );
                    if let Some(owned) = owned {
                        assert_eq!(owned.nullable(), left.nullable() || right.nullable());
                    }
                }
            }
        }
        let bytes = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
        assert!(
            arithmetic_result_value_shape_with_op(&bytes, &integer, ArithmeticOperator::Add)
                .is_none()
        );
    }
}
