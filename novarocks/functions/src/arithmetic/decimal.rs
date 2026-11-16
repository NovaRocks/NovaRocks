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

use super::{ArithmeticPrepareError, ArithmeticRowResult};
use crate::kernel_control::internal;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{KernelFailure, RowDataError};
use arrow_array::{
    Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Int8Array, Int16Array,
    Int32Array, Int64Array,
};
use arrow_buffer::i256;
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArithmeticOperator, CompileCheckpoints, DecimalOverflowPolicy, ExpressionEffects,
    FunctionValueType, ValueLogicalType, arithmetic_result_value_shape_with_op,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Reader {
    I8,
    I16,
    I32,
    I64,
    Decimal128,
    Decimal256,
    LargeInt,
}
impl Reader {
    fn from_type(value: &FunctionValueType) -> Option<Self> {
        match (&value.logical_type(), value.data_type()) {
            (ValueLogicalType::Physical, DataType::Int8) => Some(Self::I8),
            (ValueLogicalType::Physical, DataType::Int16) => Some(Self::I16),
            (ValueLogicalType::Physical, DataType::Int32) => Some(Self::I32),
            (ValueLogicalType::Physical, DataType::Int64) => Some(Self::I64),
            (ValueLogicalType::Physical, DataType::Decimal128(..)) => Some(Self::Decimal128),
            (ValueLogicalType::Physical, DataType::Decimal256(..)) => Some(Self::Decimal256),
            (ValueLogicalType::LargeInt, DataType::FixedSizeBinary(16)) => Some(Self::LargeInt),
            _ => None,
        }
    }
    fn validate(self, array: &dyn Array) -> Result<(), KernelFailure> {
        let valid = match self {
            Self::I8 => array.as_any().is::<Int8Array>(),
            Self::I16 => array.as_any().is::<Int16Array>(),
            Self::I32 => array.as_any().is::<Int32Array>(),
            Self::I64 => array.as_any().is::<Int64Array>(),
            Self::Decimal128 => array.as_any().is::<Decimal128Array>(),
            Self::Decimal256 => array.as_any().is::<Decimal256Array>(),
            Self::LargeInt => array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .is_some_and(|array| array.value_length() == 16),
        };
        if valid {
            Ok(())
        } else {
            Err(internal(
                "decimal arithmetic has a foreign frozen array implementation",
            ))
        }
    }
    fn read128(self, array: &dyn Array, row: usize) -> Result<i128, KernelFailure> {
        macro_rules! read {
            ($array:ty) => {
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .map(|array| i128::from(array.value(row)))
                    .ok_or_else(|| internal("decimal arithmetic has a foreign coefficient reader"))
            };
        }
        match self {
            Self::I8 => read!(Int8Array),
            Self::I16 => read!(Int16Array),
            Self::I32 => read!(Int32Array),
            Self::I64 => read!(Int64Array),
            Self::Decimal128 => array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .map(|array| array.value(row))
                .ok_or_else(|| internal("decimal arithmetic has a foreign coefficient reader")),
            Self::LargeInt => {
                let array = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| internal("decimal arithmetic has a foreign LARGEINT reader"))?;
                let bytes: [u8; 16] = array
                    .value(row)
                    .try_into()
                    .map_err(|_| internal("decimal arithmetic LARGEINT coefficient is not BE16"))?;
                Ok(i128::from_be_bytes(bytes))
            }
            Self::Decimal256 => Err(internal(
                "wide coefficient cannot enter a Decimal128 recipe",
            )),
        }
    }
    fn read256(self, array: &dyn Array, row: usize) -> Result<i256, KernelFailure> {
        if self == Self::Decimal256 {
            array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .map(|array| array.value(row))
                .ok_or_else(|| internal("decimal arithmetic has a foreign wide coefficient reader"))
        } else {
            self.read128(array, row).map(i256::from_i128)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Algorithm {
    Decimal128 {
        left_factor: Option<i128>,
        right_factor: Option<i128>,
        product_factor: Option<i128>,
        product_diff: i32,
        division_factor: Option<i128>,
        division_diff: i32,
        precision_limit: u128,
    },
    Mixed256 {
        left_factor: Option<i256>,
        right_factor: Option<i256>,
        precision_limit: i256,
    },
}

/// Closed coefficient recipe. It owns no arrays, mutable state or control.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DecimalArithmetic {
    operator: ArithmeticOperator,
    left: Reader,
    right: Reader,
    algorithm: Algorithm,
    policy: DecimalOverflowPolicy,
    allow: bool,
}
impl DecimalArithmetic {
    pub(super) fn prepare(
        operator: ArithmeticOperator,
        left: &FunctionValueType,
        right: &FunctionValueType,
        result: (&DataType, ValueLogicalType, bool),
        policy: DecimalOverflowPolicy,
        allow: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<Self>, ArithmeticPrepareError> {
        if !matches!(
            result.0,
            DataType::Decimal128(..) | DataType::Decimal256(..)
        ) {
            return Ok(None);
        }
        let expected = arithmetic_result_value_shape_with_op(left, right, operator);
        // The caller supplies the same original result decision with the
        // executable nullable law. No second expected FVT is published here.
        let matches = expected
            .as_ref()
            .is_some_and(|expected| result.1 == expected.1 && result.0 == &expected.0 && result.2);
        work.step()?;
        if !matches {
            return Err(ArithmeticPrepareError::TypeMismatch);
        }
        let (Some(left_reader), Some(right_reader)) =
            (Reader::from_type(left), Reader::from_type(right))
        else {
            return Err(ArithmeticPrepareError::Unsupported);
        };
        let scale = |ty: &FunctionValueType| match ty.data_type() {
            DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale) => i32::from(*scale),
            _ => 0,
        };
        let ls = scale(left);
        let rs = scale(right);
        let algorithm = match result.0 {
            DataType::Decimal128(precision, out_scale) => {
                if matches!(left_reader, Reader::Decimal256 | Reader::LargeInt)
                    || matches!(right_reader, Reader::Decimal256 | Reader::LargeInt)
                    || !matches!(
                        (left_reader, right_reader),
                        (Reader::Decimal128, _) | (_, Reader::Decimal128)
                    )
                {
                    return Err(ArithmeticPrepareError::Unsupported);
                }
                let os = i32::from(*out_scale);
                let product_diff = os - ls - rs;
                let division_diff = os + rs - ls;
                Algorithm::Decimal128 {
                    left_factor: power128((os - ls).unsigned_abs(), work)?,
                    right_factor: power128((os - rs).unsigned_abs(), work)?,
                    product_factor: power128(product_diff.unsigned_abs(), work)?,
                    product_diff,
                    division_factor: power128(division_diff.unsigned_abs(), work)?,
                    division_diff,
                    precision_limit: power128(u32::from(*precision), work)?
                        .ok_or(ArithmeticPrepareError::TypeMismatch)?
                        as u128,
                }
            }
            DataType::Decimal256(precision, out_scale) => {
                let mixed = matches!(
                    (left_reader, right_reader),
                    (Reader::LargeInt, Reader::Decimal128 | Reader::Decimal256)
                        | (Reader::Decimal128 | Reader::Decimal256, Reader::LargeInt)
                );
                if !mixed
                    || !matches!(
                        operator,
                        ArithmeticOperator::Add | ArithmeticOperator::Subtract
                    )
                {
                    return Err(ArithmeticPrepareError::Unsupported);
                }
                let os = i32::from(*out_scale);
                Algorithm::Mixed256 {
                    left_factor: power256((os - ls).unsigned_abs(), work)?,
                    right_factor: power256((os - rs).unsigned_abs(), work)?,
                    precision_limit: power256(u32::from(*precision), work)?
                        .ok_or(ArithmeticPrepareError::TypeMismatch)?,
                }
            }
            _ => unreachable!("checked decimal result"),
        };
        work.step()?;
        Ok(Some(Self {
            operator,
            left: left_reader,
            right: right_reader,
            algorithm,
            policy,
            allow,
        }))
    }
    pub(super) fn validate_left(&self, array: &dyn Array) -> Result<(), KernelFailure> {
        self.left.validate(array)
    }
    pub(super) fn validate_right(&self, array: &dyn Array) -> Result<(), KernelFailure> {
        self.right.validate(array)
    }
    pub(super) fn own_effects(&self) -> ExpressionEffects {
        ExpressionEffects {
            may_raise_row_error: self.policy == DecimalOverflowPolicy::ReportError
                || (self.allow && self.operator == ArithmeticOperator::Multiply),
            ..ExpressionEffects::PURE_VALUE
        }
    }
    /// The parent validates both addresses/carriers and strict NULL first.
    /// Coefficients may exceed source declared precision, as in the legacy
    /// stored-column contract; only arithmetic/output overflow is classified.
    pub(super) fn evaluate_non_null(
        &self,
        left: &dyn Array,
        left_row: usize,
        right: &dyn Array,
        right_row: usize,
        ordinal: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<ArithmeticRowResult, KernelFailure> {
        use ArithmeticOperator::{Add, Divide, Modulo, Multiply, Subtract};
        let value = match self.algorithm {
            Algorithm::Decimal128 {
                left_factor,
                right_factor,
                product_factor,
                product_diff,
                division_factor,
                division_diff,
                precision_limit,
            } => {
                let left = self.left.read128(left, left_row);
                work.step()?;
                let left = left?;
                let right = self.right.read128(right, right_row);
                work.step()?;
                let right = right?;
                if matches!(self.operator, Divide | Modulo) && right == 0 {
                    work.step()?;
                    return Ok(ArithmeticRowResult::Null);
                }
                let value = match self.operator {
                    Add | Subtract | Modulo => left_factor
                        .and_then(|f| left.checked_mul(f))
                        .zip(right_factor.and_then(|f| right.checked_mul(f)))
                        .and_then(|(left, right)| match self.operator {
                            Add => left.checked_add(right),
                            Subtract => left.checked_sub(right),
                            Modulo => left.checked_rem(right),
                            _ => unreachable!("checked aligned operation"),
                        }),
                    Multiply => left.checked_mul(right).and_then(|product| {
                        product_factor.and_then(|factor| {
                            if product_diff >= 0 {
                                product.checked_mul(factor)
                            } else {
                                product.checked_div(factor)
                            }
                        })
                    }),
                    Divide => division_factor
                        .and_then(|factor| {
                            if division_diff >= 0 {
                                left.checked_mul(factor)
                            } else {
                                left.checked_div(factor)
                            }
                        })
                        .and_then(|numerator| divide_half_away(numerator, right)),
                }
                .filter(|value| value.unsigned_abs() < precision_limit);
                value.map(ArithmeticRowResult::Decimal128)
            }
            Algorithm::Mixed256 {
                left_factor,
                right_factor,
                precision_limit,
            } => {
                let left = self.left.read256(left, left_row);
                work.step()?;
                let left = left?;
                let right = self.right.read256(right, right_row);
                work.step()?;
                let right = right?;
                left_factor
                    .and_then(|f| left.checked_mul(f))
                    .zip(right_factor.and_then(|f| right.checked_mul(f)))
                    .and_then(|(left, right)| {
                        if self.operator == Add {
                            left.checked_add(right)
                        } else {
                            left.checked_sub(right)
                        }
                    })
                    .filter(|value| *value > -precision_limit && *value < precision_limit)
                    .map(ArithmeticRowResult::Decimal256)
            }
        };
        work.step()?;
        Ok(value.unwrap_or_else(|| self.overflow(ordinal)))
    }
    fn overflow(&self, ordinal: usize) -> ArithmeticRowResult {
        if self.own_effects().may_raise_row_error {
            let message = match self.operator {
                ArithmeticOperator::Add => {
                    "Expr evaluate meet error: The 'add' operation involving decimal values overflows"
                }
                ArithmeticOperator::Subtract => {
                    "Expr evaluate meet error: The 'sub' operation involving decimal values overflows"
                }
                ArithmeticOperator::Multiply => {
                    "Expr evaluate meet error: The 'mul' operation involving decimal values overflows"
                }
                ArithmeticOperator::Divide => {
                    "Expr evaluate meet error: The 'div' operation involving decimal values overflows"
                }
                ArithmeticOperator::Modulo => {
                    "Expr evaluate meet error: The 'mod' operation involving decimal values overflows"
                }
            };
            ArithmeticRowResult::RowError(RowDataError::with_reason(
                ordinal,
                crate::DataFailureReason::NumericOutOfRange,
                message,
            ))
        } else {
            ArithmeticRowResult::Null
        }
    }
}
fn power128(
    exponent: u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<i128>, ArithmeticPrepareError> {
    let mut power = 1_i128;
    for _ in 0..exponent {
        let value = power.checked_mul(10);
        work.step()?;
        let Some(value) = value else { return Ok(None) };
        power = value;
    }
    Ok(Some(power))
}
fn power256(
    exponent: u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<i256>, ArithmeticPrepareError> {
    let mut power = i256::ONE;
    for _ in 0..exponent {
        let value = power.checked_mul(i256::from_i128(10));
        work.step()?;
        let Some(value) = value else { return Ok(None) };
        power = value;
    }
    Ok(Some(power))
}
fn divide_half_away(numerator: i128, denominator: i128) -> Option<i128> {
    let quotient = numerator.checked_div(denominator)?;
    let remainder = numerator.checked_rem(denominator)?;
    let magnitude = denominator.unsigned_abs();
    let threshold = (magnitude >> 1) + (magnitude & 1);
    if remainder.unsigned_abs() >= threshold {
        quotient.checked_add(if (numerator < 0) ^ (denominator < 0) {
            -1
        } else {
            1
        })
    } else {
        Some(quotient)
    }
}

#[cfg(test)]
mod tests;
