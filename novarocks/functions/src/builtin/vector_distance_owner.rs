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

use arrow_array::{Array, ArrayRef, Float64Array, ListArray, builder::Float64Builder};
use arrow_schema::DataType;
use novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT;
use std::{ops::Range, sync::Arc};

use crate::kernel_control::invalid;
use crate::kernel_input::EvaluationCheckpoints;
use crate::selected_copy::{CopyError, fixed_interleave_extent};
use crate::{
    DataFailureReason, KernelEvaluationControl, KernelFailure, RowDataError, ScalarCallInput,
    SelectedValues,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Operation {
    Cosine,
    Euclidean,
}

pub(super) fn operation(name: &str) -> Option<Operation> {
    match name {
        "cosine_similarity" => Some(Operation::Cosine),
        "euclidean_distance" => Some(Operation::Euclidean),
        _ => None,
    }
}

struct Vector<'a> {
    list: &'a ListArray,
    elements: &'a Float64Array,
}

impl<'a> Vector<'a> {
    fn new(array: &'a ArrayRef) -> Result<Self, KernelFailure> {
        let list = array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| invalid("vector operation requires a checked List argument"))?;
        let elements = list
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| invalid("vector child differs from its checked DOUBLE carrier"))?;
        Ok(Self { list, elements })
    }

    fn range(&self, row: usize) -> Result<Range<usize>, KernelFailure> {
        let offsets = self.list.value_offsets();
        if row >= self.list.len() {
            return Err(invalid("vector row is outside its checked list"));
        }
        Ok(offsets[row] as usize..offsets[row + 1] as usize)
    }

    fn element(&self, row: usize) -> Option<f64> {
        (!self.elements.is_null(row)).then(|| self.elements.value(row))
    }
}

fn evaluate_row(
    operation: Operation,
    base: &Vector<'_>,
    base_row: usize,
    target: &Vector<'_>,
    target_row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<Option<f64>, &'static str>, KernelFailure> {
    let left = base.range(base_row)?;
    let right = target.range(target_row)?;
    if base.list.is_null(base_row) || target.list.is_null(target_row) {
        return Ok(Ok(None));
    }
    if left.len() != right.len() {
        return Ok(Err("The arguments must have the same length"));
    }

    let mut value = 0.0;
    let mut base_norm = 0.0;
    let mut target_norm = 0.0;
    for (left, right) in left.zip(right) {
        work.step()?;
        let left = base.element(left);
        let right = target.element(right);
        match operation {
            Operation::Euclidean => {
                let difference = left.unwrap_or(0.0) - right.unwrap_or(0.0);
                value += difference * difference;
            }
            Operation::Cosine => {
                let (Some(left), Some(right)) = (left, right) else {
                    return Ok(Ok(None));
                };
                value += left * right;
                base_norm += left * left;
                target_norm += right * right;
            }
        }
    }
    Ok(match operation {
        Operation::Euclidean => Ok(Some(value.sqrt())),
        Operation::Cosine if base_norm == 0.0 || target_norm == 0.0 => {
            Err("Vector magnitude cannot be zero")
        }
        Operation::Cosine => Ok(Some(value / (base_norm * target_norm).sqrt())),
    })
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
                Ok(value) => output.append_option(value),
                Err(error) => {
                    errors
                        .try_reserve(1)
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    errors.push(RowDataError::with_reason(
                        ordinal,
                        DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT),
                        error,
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
