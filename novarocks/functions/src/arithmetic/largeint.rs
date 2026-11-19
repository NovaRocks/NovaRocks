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

//! Exact signed LARGEINT arithmetic. Carrier width alone grants no numeric domain.
//! The parent owns full result proof, strict NULLs and independent selected addresses.

use arrow_array::{Array, FixedSizeBinaryArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArithmeticOperator, CompileCheckpoints, ExpressionEffects, FunctionValueType, ValueLogicalType,
};

use super::{ArithmeticPrepareError, ArithmeticRowResult, SignedWidth};
use crate::KernelFailure;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IntegerReader {
    LargeInt,
    Signed(SignedWidth),
}
impl IntegerReader {
    fn from_type(ty: &FunctionValueType) -> Option<Self> {
        if ty.logical_type() == ValueLogicalType::LargeInt
            && ty.data_type() == &DataType::FixedSizeBinary(16)
        {
            Some(Self::LargeInt)
        } else {
            SignedWidth::from_type(ty).map(Self::Signed)
        }
    }

    fn validate(self, array: &dyn Array) -> Result<(), KernelFailure> {
        let carrier_matches = match self {
            Self::LargeInt => array.data_type() == &DataType::FixedSizeBinary(16),
            Self::Signed(width) => SignedWidth::from_carrier(array.data_type()) == Some(width),
        };
        if !carrier_matches {
            return Err(invalid(
                "LARGEINT arithmetic argument differs from its frozen carrier",
            ));
        }
        let implementation_matches = match self {
            Self::LargeInt => array.as_any().is::<FixedSizeBinaryArray>(),
            Self::Signed(width) => width.validate(array),
        };
        if !implementation_matches {
            return Err(internal(
                "LARGEINT arithmetic carrier has a foreign array implementation",
            ));
        }
        Ok(())
    }
    fn read(self, array: &dyn Array, row: usize) -> Result<i128, KernelFailure> {
        if row >= array.len() {
            return Err(invalid("LARGEINT arithmetic address is out of range"));
        }
        self.validate(array)?;
        if array.is_null(row) {
            return Err(invalid(
                "LARGEINT non-NULL arithmetic received a NULL address",
            ));
        }
        match self {
            Self::LargeInt => {
                let array = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        internal("LARGEINT operand has a foreign array implementation")
                    })?;
                let bytes: [u8; 16] = array
                    .value(row)
                    .try_into()
                    .map_err(|_| internal("LARGEINT operand does not contain sixteen bytes"))?;
                Ok(i128::from_be_bytes(bytes))
            }
            Self::Signed(width) => width.read(array, row).map(i128::from),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LargeIntArithmetic {
    operator: ArithmeticOperator,
    left: IntegerReader,
    right: IntegerReader,
}
impl LargeIntArithmetic {
    /// Only the exact LARGEINT/integer family is selected here. The parent's
    /// shared result proof decides the complete result FVT before publication.
    pub(super) fn prepare(
        operator: ArithmeticOperator,
        left: &FunctionValueType,
        right: &FunctionValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<Self>, ArithmeticPrepareError> {
        let readers = (
            IntegerReader::from_type(left),
            IntegerReader::from_type(right),
        );
        let family = matches!(
            readers,
            (Some(IntegerReader::LargeInt), Some(_)) | (Some(_), Some(IntegerReader::LargeInt))
        );
        work.step()?;
        if !family {
            return Ok(None);
        }
        let (Some(left), Some(right)) = readers else {
            return Ok(None);
        };
        let result = Self {
            operator,
            left,
            right,
        };
        work.step()?;
        Ok(Some(result))
    }
    pub(super) fn validate_left(&self, array: &dyn Array) -> Result<(), KernelFailure> {
        self.left.validate(array)
    }
    pub(super) fn validate_right(&self, array: &dyn Array) -> Result<(), KernelFailure> {
        self.right.validate(array)
    }
    pub(super) fn own_effects(&self) -> ExpressionEffects {
        ExpressionEffects::PURE_VALUE
    }
    pub(super) fn evaluate_non_null(
        &self,
        left: &dyn Array,
        left_row: usize,
        right: &dyn Array,
        right_row: usize,
        _ordinal: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<ArithmeticRowResult, KernelFailure> {
        let lhs = self.left.read(left, left_row);
        work.step()?;
        let lhs = lhs?;
        let rhs = self.right.read(right, right_row);
        work.step()?;
        let rhs = rhs?;
        use ArithmeticOperator::*;
        let result = match self.operator {
            Add => ArithmeticRowResult::LargeInt(lhs.wrapping_add(rhs)),
            Subtract => ArithmeticRowResult::LargeInt(lhs.wrapping_sub(rhs)),
            Multiply => ArithmeticRowResult::LargeInt(lhs.wrapping_mul(rhs)),
            Modulo if rhs == 0 => ArithmeticRowResult::Null,
            Modulo => ArithmeticRowResult::LargeInt(lhs.wrapping_rem(rhs)),
            Divide if rhs == 0 => ArithmeticRowResult::Null,
            // The accepted numerical conversion rounds each signed integer to
            // nearest F64, ties to even. No binary reinterpretation or i128
            // truncated quotient substitutes for the frozen fractional result.
            Divide => ArithmeticRowResult::Float((lhs as f64) / (rhs as f64)),
        };
        work.step()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests;
