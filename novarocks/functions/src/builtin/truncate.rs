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

//! Selected TRUNCATE delegates all numeric computation to the original core.

use super::dround_core::{DroundComputation, compute_dround};
use crate::math_numeric::{MathNumericError, NumericArrayView};
use crate::{
    EvaluatedArgument, FunctionArgumentType, FunctionValueType, KernelEvaluationControl,
    KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
};
use arrow_array::types::{Decimal128Type, validate_decimal_precision_and_scale};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TruncateOp {
    Unary,
    Binary,
}
impl TruncateOp {
    fn arity(self) -> usize {
        match self {
            Self::Unary => 1,
            Self::Binary => 2,
        }
    }
}

struct NumericValue<'a> {
    argument: EvaluatedArgument<'a>,
    view: NumericArrayView<'a>,
    nullable: bool,
}
impl<'a> NumericValue<'a> {
    fn checked(
        argument: EvaluatedArgument<'a>,
        source: &FunctionValueType,
    ) -> Result<Self, KernelFailure> {
        if source.logical_type != ValueLogicalType::Physical
            || argument.array().data_type() != &source.data_type
        {
            return Err(invalid(
                "truncate input differs from its exact numeric carrier",
            ));
        }
        match source.data_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64 => {}
            DataType::Decimal128(precision, scale) => {
                validate_decimal_precision_and_scale::<Decimal128Type>(precision, scale)
                    .map_err(|_| invalid("truncate selected decimal parameters are invalid"))?;
            }
            DataType::Null if source.nullable => {}
            _ => {
                return Err(invalid(
                    "truncate input is not an installed numeric carrier",
                ));
            }
        }
        let view = NumericArrayView::new(argument.array()).map_err(|message| internal(&message))?;
        Ok(Self {
            argument,
            view,
            nullable: source.nullable,
        })
    }
    fn row(&self, ordinal: usize, batch_row: usize) -> Result<Option<usize>, KernelFailure> {
        let row = self.argument.value_row(ordinal, batch_row);
        let array = self.argument.array();
        if row >= array.len() {
            return Err(internal("truncate selected row is outside its carrier"));
        }
        if array.is_null(row) || matches!(self.view, NumericArrayView::Null(_)) {
            if !self.nullable {
                return Err(internal(
                    "truncate non-null input contains selected SQL NULL",
                ));
            }
            Ok(None)
        } else {
            Ok(Some(row))
        }
    }
    fn value(&self, ordinal: usize, batch_row: usize) -> Result<Option<f64>, KernelFailure> {
        Ok(self
            .row(ordinal, batch_row)?
            .and_then(|row| self.view.value_f64(row)))
    }
    fn digits(&self, ordinal: usize, batch_row: usize) -> Result<Option<i64>, KernelFailure> {
        Ok(self
            .row(ordinal, batch_row)?
            .and_then(|row| self.view.value_i64(row)))
    }
}

pub(super) fn evaluate_truncate<'a>(
    op: TruncateOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    if input.arguments().len() != op.arity()
        || input.contract().selected().argument_types.len() != op.arity()
    {
        return Err(invalid(
            "truncate arguments differ from the frozen operation arity",
        ));
    }
    let (value, digits) = match (
        op,
        input.contract().selected().argument_types.as_ref(),
        input.arguments(),
    ) {
        (TruncateOp::Unary, [FunctionArgumentType::Value(source)], [argument]) => {
            (NumericValue::checked(*argument, source)?, None)
        }
        (
            TruncateOp::Binary,
            [
                FunctionArgumentType::Value(left),
                FunctionArgumentType::Value(right),
            ],
            [l, r],
        ) => (
            NumericValue::checked(*l, left)?,
            Some(NumericValue::checked(*r, right)?),
        ),
        _ => {
            return Err(invalid(
                "truncate selected call is not an exact value profile",
            ));
        }
    };
    let target = input.contract().result_type();
    if target.logical_type != ValueLogicalType::Physical || !target.nullable {
        return Err(invalid(
            "truncate requires its exact nullable Physical result",
        ));
    }
    match (value.argument.array().data_type(), &target.data_type, op) {
        (DataType::Decimal128(..), DataType::Decimal128(38, scale), _) => {
            validate_decimal_precision_and_scale::<Decimal128Type>(38, *scale)
                .map_err(|_| invalid("truncate selected decimal result parameters are invalid"))?;
        }
        (source, DataType::Int64, TruncateOp::Unary)
            if !matches!(source, DataType::Decimal128(..)) => {}
        (source, DataType::Float64, TruncateOp::Binary)
            if !matches!(source, DataType::Decimal128(..)) => {}
        _ => {
            return Err(invalid(
                "truncate result is not an installed output carrier",
            ));
        }
    }
    let selection = input.selection();
    output_capacity(selection.len())?;
    let computation = match op {
        TruncateOp::Unary => DroundComputation::Truncate,
        TruncateOp::Binary => DroundComputation::TruncateDigits,
    };
    let values = compute_dround(
        computation,
        selection,
        Some(&target.data_type),
        Some(control),
        |ordinal, batch_row| {
            let x = value.value(ordinal, batch_row)?;
            let digits = digits
                .as_ref()
                .map(|digits| digits.digits(ordinal, batch_row))
                .transpose()?
                .flatten();
            Ok((x, digits))
        },
    )
    .map_err(|error| match error {
        MathNumericError::Kernel(error) => error,
        // Exact supported inputs and safe output casts have no lawful whole
        // data-error route. Preserve an unexpected core diagnostic as Internal.
        MathNumericError::Legacy(message) => internal(&message),
    })?;
    SelectedValues::try_new(selection, &target.data_type, values, Box::default())
        .map_err(|_| internal("truncate compact output violates its selected contract"))
}

/// Rust/Arrow representation limits only; these checks grant no capacity.
fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let exhausted = || KernelFailure::ResourceExhausted;
    Layout::array::<Option<f64>>(rows).map_err(|_| exhausted())?;
    // The original Float64 sanitizer starts with a 1024-element Vec and
    // doubles while appending. Validate every possible final growth capacity,
    // not just the compact result length. Its nullable builder materializes
    // an Arrow bitmap; the initial Float64 and safe cast also create buffers.
    let capacity = rows
        .max(1024)
        .checked_next_power_of_two()
        .ok_or_else(exhausted)?;
    Layout::array::<f64>(capacity).map_err(|_| exhausted())?;
    let bitmap = capacity.checked_add(7).ok_or_else(exhausted)? / 8;
    for bytes in [
        rows.checked_mul(8).ok_or_else(exhausted)?,
        rows.checked_mul(16).ok_or_else(exhausted)?,
        bitmap,
    ] {
        arrow_layout(bytes)?;
    }
    Ok(())
}
fn arrow_layout(bytes: usize) -> Result<(), KernelFailure> {
    let rounded = bytes
        .checked_add(63)
        .ok_or(KernelFailure::ResourceExhausted)?
        & !63;
    Layout::from_size_align(rounded, arrow_buffer::alloc::ALIGNMENT)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(())
}

#[cfg(test)]
#[path = "truncate_tests.rs"]
mod tests;
