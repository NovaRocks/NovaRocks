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

//! The vector operations consume only the elements of selected list rows.

use arrow_array::builder::Float64Builder;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal32Array, Decimal64Array, Decimal256Array, Float16Array,
    ListArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use std::ops::Range;
use std::sync::Arc;

use super::round_cast_float_text::parse_f64;
use crate::decimal_float_cast::decimal256_unscaled_to_f64;
use crate::kernel_control::invalid;
use crate::kernel_input::EvaluationCheckpoints;
use crate::math_numeric::NumericArrayView;
use crate::selected_copy::{CopyError, fixed_interleave_extent};
use crate::{
    KernelEvaluationControl, KernelFailure, RowDataError, ScalarCallInput, SelectedValues,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Operation {
    Cosine,
    SquaredL2,
}

pub(super) fn operation(name: &str) -> Option<Operation> {
    match name {
        "cosine_similarity" => Some(Operation::Cosine),
        "l2_distance" => Some(Operation::SquaredL2),
        _ => None,
    }
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Cosine => "cosine_similarity",
            Self::SquaredL2 => "l2_distance",
        }
    }
}

enum Elements<'a> {
    Boolean(&'a BooleanArray),
    Utf8(&'a StringArray),
    Numeric(NumericArrayView<'a>),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float16(&'a Float16Array),
    Decimal32(&'a Decimal32Array, f64),
    Decimal64(&'a Decimal64Array, f64),
    Decimal256(&'a Decimal256Array, f64),
    Unsupported,
}

impl<'a> Elements<'a> {
    fn new(values: &'a ArrayRef) -> Result<Self, KernelFailure> {
        fn as_array<T: Array + 'static>(values: &ArrayRef) -> Result<&T, KernelFailure> {
            values
                .as_any()
                .downcast_ref()
                .ok_or_else(|| invalid("vector child differs from its numeric carrier"))
        }

        Ok(match values.data_type() {
            DataType::Boolean => Self::Boolean(as_array(values)?),
            DataType::Utf8 => Self::Utf8(as_array(values)?),
            DataType::UInt8 => Self::UInt8(as_array(values)?),
            DataType::UInt16 => Self::UInt16(as_array(values)?),
            DataType::UInt32 => Self::UInt32(as_array(values)?),
            DataType::UInt64 => Self::UInt64(as_array(values)?),
            DataType::Float16 => Self::Float16(as_array(values)?),
            // Match the original Arrow conversion, including division at negative scales.
            DataType::Decimal32(_, scale) => {
                Self::Decimal32(as_array(values)?, 10_f64.powi(i32::from(*scale)))
            }
            DataType::Decimal64(_, scale) => {
                Self::Decimal64(as_array(values)?, 10_f64.powi(i32::from(*scale)))
            }
            DataType::Decimal256(_, scale) => {
                Self::Decimal256(as_array(values)?, 10_f64.powi(i32::from(*scale)))
            }
            DataType::Null
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..) => {
                Self::Numeric(NumericArrayView::new(values).map_err(KernelFailure::from_domain)?)
            }
            _ => Self::Unsupported,
        })
    }

    /// Callers checked the selected offsets and their reachable NULLs first.
    fn value(
        &self,
        row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<f64, KernelFailure> {
        Ok(match self {
            Self::Boolean(values) => f64::from(u8::from(values.value(row))),
            Self::Utf8(values) => parse_f64(values.value(row), work)?
                .ok_or_else(|| invalid("vector text reader lost a valid numeric child"))?,
            Self::Numeric(values) => values
                .value_f64(row)
                .ok_or_else(|| invalid("vector numeric reader lost a non-null child"))?,
            Self::UInt8(values) => f64::from(values.value(row)),
            Self::UInt16(values) => f64::from(values.value(row)),
            Self::UInt32(values) => f64::from(values.value(row)),
            Self::UInt64(values) => values.value(row) as f64,
            Self::Float16(values) => values.value(row).to_f64(),
            Self::Decimal32(values, divisor) => f64::from(values.value(row)) / divisor,
            Self::Decimal64(values, divisor) => values.value(row) as f64 / divisor,
            Self::Decimal256(values, divisor) => {
                decimal256_unscaled_to_f64(values.value(row)) / divisor
            }
            Self::Unsupported => return Err(invalid("vector numeric reader is unsupported")),
        })
    }
}

struct Vector<'a> {
    list: &'a ListArray,
    elements: Elements<'a>,
}

impl<'a> Vector<'a> {
    fn new(array: &'a ArrayRef) -> Result<Self, KernelFailure> {
        let list = array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| invalid("vector operation requires a checked List argument"))?;
        let elements = Elements::new(list.values())?;
        Ok(Self { list, elements })
    }

    fn range(&self, row: usize) -> Result<Range<usize>, KernelFailure> {
        let offsets = self.list.value_offsets();
        if row >= self.list.len() {
            return Err(invalid("vector row is outside its checked list"));
        }
        Ok(offsets[row] as usize..offsets[row + 1] as usize)
    }

    fn element_error(
        &self,
        range: Range<usize>,
        side: &'static str,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<RowFailure>, KernelFailure> {
        if matches!(self.elements, Elements::Unsupported) {
            return Ok(Some(RowFailure::NonNumeric(side)));
        }
        if range.is_empty() {
            return Ok(None);
        }
        if let Elements::Utf8(values) = &self.elements {
            // Failed safe casts made the original vector NULL on this side.
            // Check each side before checking dimensions. Reading again
            // during arithmetic keeps only borrowed element backing.
            for row in range {
                work.step()?;
                if values.is_null(row) || parse_f64(values.value(row), work)?.is_none() {
                    return Ok(Some(RowFailure::Null(side)));
                }
            }
            return Ok(None);
        }
        let values = self.list.values();
        if values.data_type() == &DataType::Null {
            return Ok(Some(RowFailure::Null(side)));
        }
        if values.null_count() != 0 {
            for row in range {
                work.step()?;
                if values.is_null(row) {
                    return Ok(Some(RowFailure::Null(side)));
                }
            }
        }
        Ok(None)
    }
}

enum RowFailure {
    Null(&'static str),
    NonNumeric(&'static str),
    Dimensions { base: usize, target: usize },
    Empty,
}

impl RowFailure {
    fn diagnostic(self, function: &str) -> String {
        match self {
            Self::Null(side) => {
                format!("{function} does not support null values. {side} array has null value.")
            }
            Self::NonNumeric(side) => {
                format!("{function} expects numeric array elements for {side} argument")
            }
            Self::Dimensions { base, target } => format!(
                "{function} requires equal length arrays in each row. base array dimension size is {base}, target array dimension size is {target}."
            ),
            Self::Empty => format!("{function} requires non-empty arrays in each row"),
        }
    }
}

fn evaluate_row(
    operation: Operation,
    base: &Vector<'_>,
    base_row: usize,
    target: &Vector<'_>,
    target_row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<f64, RowFailure>, KernelFailure> {
    let left = base.range(base_row)?;
    let right = target.range(target_row)?;
    if base.list.is_null(base_row) {
        return Ok(Err(RowFailure::Null("base")));
    }
    if target.list.is_null(target_row) {
        return Ok(Err(RowFailure::Null("target")));
    }
    if let Some(error) = base.element_error(left.clone(), "base", work)? {
        return Ok(Err(error));
    }
    if let Some(error) = target.element_error(right.clone(), "target", work)? {
        return Ok(Err(error));
    }
    if left.len() != right.len() {
        return Ok(Err(RowFailure::Dimensions {
            base: left.len(),
            target: right.len(),
        }));
    }
    if left.is_empty() {
        return Ok(Err(RowFailure::Empty));
    }

    let mut value = 0.0;
    let mut base_norm = 0.0;
    let mut target_norm = 0.0;
    for (left, right) in left.zip(right) {
        work.step()?;
        let left = base.elements.value(left, work)?;
        let right = target.elements.value(right, work)?;
        match operation {
            Operation::SquaredL2 => {
                let diff = left - right;
                value += diff * diff;
            }
            Operation::Cosine => {
                value += left * right;
                base_norm += left * left;
                target_norm += right * right;
            }
        }
    }
    if operation == Operation::Cosine {
        value = if base_norm == 0.0 || target_norm == 0.0 {
            0.0
        } else {
            value / (base_norm.sqrt() * target_norm.sqrt())
        };
    }
    Ok(Ok(value))
}

pub(super) fn evaluate<'a>(
    operation: Operation,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let [base, target] = input.arguments() else {
        return Err(invalid("vector operation requires two checked arguments"));
    };
    let selection = input.selection();
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let left = Vector::new(base.array())?;
        let right = Vector::new(target.array())?;
        fixed_interleave_extent(&DataType::Float64, selection.len())
            .map_err(CopyError::into_kernel)?;
        work.flush()?;
        let mut output = Float64Builder::with_capacity(selection.len());
        let mut errors = Vec::new();
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let value = evaluate_row(
                operation,
                &left,
                base.value_row(ordinal, row),
                &right,
                target.value_row(ordinal, row),
                &mut work,
            )?;
            match value {
                Ok(value) => output.append_option(value.is_finite().then_some(value)),
                Err(error) => {
                    errors
                        .try_reserve(1)
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    errors.push(RowDataError::new(
                        ordinal,
                        &error.diagnostic(operation.name()),
                    ));
                    output.append_null();
                }
            }
        }
        work.flush()?;
        let output = Arc::new(output.finish());
        SelectedValues::try_new_observed(
            selection,
            input.contract().result_type().data_type(),
            output,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}
