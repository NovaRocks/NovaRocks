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

//! Coefficient arithmetic for the standard Decimal128 operator overloads.

use arrow_buffer::i256;
use arrow_schema::DataType;
use novarocks_type_contract::ArithmeticOperator;

use crate::DataFailureReason;

const PRECISION: u8 = 38;

pub(crate) fn result_type(
    operator: ArithmeticOperator,
    left: (u8, i8),
    right: (u8, i8),
) -> DataType {
    let (p1, s1) = (i16::from(left.0), i16::from(left.1));
    let (p2, s2) = (i16::from(right.0), i16::from(right.1));
    let max_precision = i16::from(PRECISION);
    let (i1, i2) = (p1 - s1, p2 - s2);
    let (precision, scale) = match operator {
        ArithmeticOperator::Add | ArithmeticOperator::Subtract => {
            let integral = i1.max(i2);
            let scale = s1.max(s2);
            let precision = (integral + scale + 1).min(max_precision);
            (precision, scale.min(precision - integral))
        }
        ArithmeticOperator::Multiply | ArithmeticOperator::Divide => {
            let (integral, scale) = if operator == ArithmeticOperator::Multiply {
                (i1 + i2, s1 + s2)
            } else {
                (i1 + s2, 6.max(s1 + p2 + 1))
            };
            let max_scale = if integral > 32 {
                6
            } else {
                max_precision - integral
            };
            ((integral + scale).min(max_precision), scale.min(max_scale))
        }
        ArithmeticOperator::Modulo => {
            let scale = s1.max(s2);
            (i1.min(i2) + scale, scale)
        }
    };
    DataType::Decimal128(precision as u8, scale as i8)
}

#[derive(Debug)]
enum Operation {
    Add {
        left: i128,
        right: i128,
        shrink: i128,
    },
    Subtract {
        left: i128,
        right: i128,
        shrink: i128,
    },
    Multiply {
        shrink: i128,
    },
    Divide {
        expand: Option<i256>,
    },
    Modulo {
        left: i256,
        right: i256,
    },
}

#[derive(Debug)]
pub(crate) struct DecimalArithmetic {
    operation: Operation,
    limit: u128,
}

impl DecimalArithmetic {
    // The selected overload admitted both decimal layouts and the original
    // binding author validated the result. These scale factors are bounded by
    // those declarations, independent of the values supplied to the kernel.
    pub(crate) fn prepare(operator: ArithmeticOperator, left: (u8, i8), right: (u8, i8)) -> Self {
        let DataType::Decimal128(precision, scale) = result_type(operator, left, right) else {
            unreachable!("standard decimal operators publish Decimal128");
        };
        let output_scale = i16::from(scale);
        let (left_scale, right_scale) = (i16::from(left.1), i16::from(right.1));
        let common_scale = left_scale.max(right_scale);
        let narrow = |exponent: i16| 10_i128.pow(exponent as u32);
        let wide = |exponent: i16| i256::from_i128(narrow(exponent));
        let operation = match operator {
            ArithmeticOperator::Add => Operation::Add {
                left: narrow(common_scale - left_scale),
                right: narrow(common_scale - right_scale),
                shrink: narrow(common_scale - output_scale),
            },
            ArithmeticOperator::Subtract => Operation::Subtract {
                left: narrow(common_scale - left_scale),
                right: narrow(common_scale - right_scale),
                shrink: narrow(common_scale - output_scale),
            },
            ArithmeticOperator::Multiply => Operation::Multiply {
                shrink: narrow(left_scale + right_scale - output_scale),
            },
            ArithmeticOperator::Divide => {
                let expand = output_scale + right_scale - left_scale;
                // Trino's long division checks this factor before multiplying.
                Operation::Divide {
                    expand: (expand < i16::from(PRECISION)).then(|| wide(expand)),
                }
            }
            ArithmeticOperator::Modulo => Operation::Modulo {
                left: wide(common_scale - left_scale),
                right: wide(common_scale - right_scale),
            },
        };
        Self {
            operation,
            limit: 10_u128.pow(u32::from(precision)),
        }
    }

    pub(crate) fn evaluate(
        &self,
        left_value: i128,
        right_value: i128,
    ) -> Result<i128, DataFailureReason> {
        use DataFailureReason::{DivisionByZero, NumericOutOfRange};

        if right_value == 0
            && matches!(
                self.operation,
                Operation::Divide { .. } | Operation::Modulo { .. }
            )
        {
            return Err(DivisionByZero);
        }
        let result = match self.operation {
            Operation::Add {
                left,
                right,
                shrink,
            }
            | Operation::Subtract {
                left,
                right,
                shrink,
            } => {
                let left = left_value.checked_mul(left).ok_or(NumericOutOfRange)?;
                let right = right_value.checked_mul(right).ok_or(NumericOutOfRange)?;
                let coefficient = if matches!(self.operation, Operation::Add { .. }) {
                    left.checked_add(right)
                } else {
                    left.checked_sub(right)
                };
                coefficient.and_then(|value| shrink_coefficient(value, shrink))
            }
            Operation::Multiply { shrink } => left_value
                .checked_mul(right_value)
                .filter(|coefficient| *coefficient != i128::MIN)
                .and_then(|coefficient| shrink_coefficient(coefficient, shrink)),
            Operation::Divide { expand } => {
                let expand = expand.ok_or(NumericOutOfRange)?;
                let numerator = i256::from_i128(left_value) * expand;
                let denominator = i256::from_i128(right_value);
                // A coefficient's magnitude is below 10^38. The numerator
                // is below 10^75, and doubling its remainder also fits i256.
                let quotient = numerator / denominator;
                let remainder = numerator % denominator;
                let round =
                    remainder.wrapping_abs() * i256::from_i128(2) >= denominator.wrapping_abs();
                let adjustment = if round {
                    if (left_value < 0) == (right_value < 0) {
                        i256::ONE
                    } else {
                        i256::MINUS_ONE
                    }
                } else {
                    i256::ZERO
                };
                (quotient + adjustment).to_i128()
            }
            Operation::Modulo { left, right } => {
                // The largest scaled magnitude is strictly below 10^76,
                // which fits signed i256 even at the full allowed scale gap.
                let numerator = i256::from_i128(left_value) * left;
                let denominator = i256::from_i128(right_value) * right;
                (numerator % denominator).to_i128()
            }
        };
        result
            .filter(|value| value.unsigned_abs() < self.limit)
            .ok_or(NumericOutOfRange)
    }
}

fn shrink_coefficient(value: i128, shrink: i128) -> Option<i128> {
    if shrink == 1 {
        return Some(value);
    }
    // The reference rescales a positive magnitude, so Int128::MIN also
    // fails before rounding whenever this step reduces the scale.
    let magnitude = value.checked_abs()?;
    let rounded = magnitude / shrink + i128::from(magnitude % shrink >= shrink / 2);
    Some(if value < 0 { -rounded } else { rounded })
}
