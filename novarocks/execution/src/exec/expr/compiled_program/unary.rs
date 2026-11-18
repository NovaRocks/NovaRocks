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
//! Exact unary intrinsic evaluation over the already evaluated Value domain.
//! Required row errors remain errors even when their value is a NULL placeholder.
use super::*;
use arrow::array::{Array, BooleanArray};

#[derive(Clone, Copy)]
pub(super) enum UnaryIntrinsic {
    Not,
    IsNull,
    IsNotNull,
}

pub(super) fn evaluate<'a>(
    kind: UnaryIntrinsic,
    child: &Value<'a>,
    selection: Selection<'a>,
    work: &mut Work<'_, '_, impl Sized>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let argument = child.argument();
    let not = if matches!(kind, UnaryIntrinsic::Not) {
        Some(NotOperand::try_new(argument.array().as_ref())?)
    } else {
        None
    };
    let mut required = child.errors().iter().peekable();
    let mut values = Vec::with_capacity(selection.len());
    let mut errors = Vec::new();
    work.flush()?;
    let control = work.control;
    visit_selected_nulls(argument, selection, control, |ordinal, row, is_null| {
        if required
            .peek()
            .is_some_and(|error| error.selected_ordinal() == ordinal)
        {
            let error = required
                .next()
                .ok_or_else(|| internal("missing required unary error"))?;
            errors.push(error.clone());
            values.push(None);
        } else if let Some(not) = &not {
            values.push(if is_null {
                None
            } else {
                Some(not.value(argument.value_row(ordinal, row)))
            });
        } else {
            values.push(Some(is_null != matches!(kind, UnaryIntrinsic::IsNotNull)));
        }
        // The NULL visitor charges this bounded projection in its own row
        // scope, together with the logical address traversal.
        Ok(())
    })?;
    work.flush()?;
    let array = Arc::new(BooleanArray::from(values)) as ArrayRef;
    work.flush()?;
    SelectedValues::try_new_observed(
        selection,
        &DataType::Boolean,
        array,
        errors.into_boxed_slice(),
        || work.step(),
    )
}

// NOT's original scalar protocol accepts precisely these physical numeric
// carriers. This is shared by the constant entry and every compiled program;
// neither caller performs Boolean normalization or calculates a value.
enum NotOperand<'a> {
    Boolean(&'a BooleanArray),
    Int8(&'a arrow::array::Int8Array),
    Int16(&'a arrow::array::Int16Array),
    Int32(&'a arrow::array::Int32Array),
    Int64(&'a arrow::array::Int64Array),
    Float32(&'a arrow::array::Float32Array),
    Float64(&'a arrow::array::Float64Array),
    Decimal128(&'a arrow::array::Decimal128Array),
}
impl<'a> NotOperand<'a> {
    fn try_new(array: &'a dyn Array) -> Result<Self, KernelFailure> {
        macro_rules! source {
            ($array:ty, $variant:ident) => {
                Self::$variant(
                    array
                        .as_any()
                        .downcast_ref::<$array>()
                        .ok_or_else(|| invalid("NOT operand differs from its frozen carrier"))?,
                )
            };
        }
        Ok(match array.data_type() {
            DataType::Boolean => source!(BooleanArray, Boolean),
            DataType::Int8 => source!(arrow::array::Int8Array, Int8),
            DataType::Int16 => source!(arrow::array::Int16Array, Int16),
            DataType::Int32 => source!(arrow::array::Int32Array, Int32),
            DataType::Int64 => source!(arrow::array::Int64Array, Int64),
            DataType::Float32 => source!(arrow::array::Float32Array, Float32),
            DataType::Float64 => source!(arrow::array::Float64Array, Float64),
            DataType::Decimal128(..) => source!(arrow::array::Decimal128Array, Decimal128),
            _ => {
                return Err(invalid(
                    "NOT operand requires its admitted Boolean or numeric carrier",
                ));
            }
        })
    }
    fn value(&self, row: usize) -> bool {
        match self {
            Self::Boolean(v) => !v.value(row),
            Self::Int8(v) => v.value(row) == 0,
            Self::Int16(v) => v.value(row) == 0,
            Self::Int32(v) => v.value(row) == 0,
            Self::Int64(v) => v.value(row) == 0,
            Self::Float32(v) => v.value(row) == 0.0,
            Self::Float64(v) => v.value(row) == 0.0,
            Self::Decimal128(v) => v.value(row) == 0,
        }
    }
}
