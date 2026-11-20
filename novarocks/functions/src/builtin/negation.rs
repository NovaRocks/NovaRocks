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

use std::sync::Arc;

use arrow_array::builder::{FixedSizeBinaryBuilder, PrimitiveBuilder};
use arrow_array::types::{
    ArrowPrimitiveType, Decimal128Type, Decimal256Type, DecimalType, Float32Type, Float64Type,
    Int8Type, Int16Type, Int32Type, Int64Type,
};
use arrow_array::{Array, ArrayRef, PrimitiveArray};
use arrow_schema::DataType;
use novarocks_type_contract::{LARGEINT_BYTE_WIDTH, ValueLogicalType};

use crate::evaluation::NegationOverflow;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, RowDataError, ScalarCallInput,
    SelectedValues, Selection, largeint,
};

pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let [argument] = input.arguments() else {
        return Err(invalid("Negation requires one evaluated argument"));
    };
    let selection = input.selection();
    let result = input.contract().result_type();
    let carrier = result.data_type();
    let mut work = EvaluationCheckpoints::new(control);
    let mut errors = Vec::new();
    let output = match (result.logical_type(), carrier) {
        (ValueLogicalType::Physical, DataType::Int8) => {
            primitive::<Int8Type>(*argument, selection, &mut work, |row, value| {
                integer(
                    value.checked_neg(),
                    NegationOverflow::TinyInt,
                    row,
                    &mut errors,
                )
            })
        }
        (ValueLogicalType::Physical, DataType::Int16) => {
            primitive::<Int16Type>(*argument, selection, &mut work, |row, value| {
                integer(
                    value.checked_neg(),
                    NegationOverflow::SmallInt,
                    row,
                    &mut errors,
                )
            })
        }
        (ValueLogicalType::Physical, DataType::Int32) => {
            primitive::<Int32Type>(*argument, selection, &mut work, |row, value| {
                integer(
                    value.checked_neg(),
                    NegationOverflow::Integer,
                    row,
                    &mut errors,
                )
            })
        }
        (ValueLogicalType::Physical, DataType::Int64) => {
            primitive::<Int64Type>(*argument, selection, &mut work, |row, value| {
                integer(
                    value.checked_neg(),
                    NegationOverflow::BigInt,
                    row,
                    &mut errors,
                )
            })
        }
        (ValueLogicalType::LargeInt, DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH)) => {
            negate_largeint(*argument, selection, &mut work, &mut errors)
        }
        (ValueLogicalType::Physical, DataType::Float32) => {
            primitive::<Float32Type>(*argument, selection, &mut work, |_, value| Ok(Some(-value)))
        }
        (ValueLogicalType::Physical, DataType::Float64) => {
            primitive::<Float64Type>(*argument, selection, &mut work, |_, value| Ok(Some(-value)))
        }
        (ValueLogicalType::Physical, DataType::Decimal128(precision, scale)) => {
            primitive::<Decimal128Type>(*argument, selection, &mut work, |_, value| {
                Decimal128Type::validate_decimal_precision(value, *precision, *scale).map_err(
                    |_| internal("Negation input exceeds its declared Decimal128 precision"),
                )?;
                value
                    .checked_neg()
                    .map(Some)
                    .ok_or_else(|| internal("Checked Decimal128 negation cannot overflow"))
            })
        }
        (ValueLogicalType::Physical, DataType::Decimal256(precision, scale)) => {
            primitive::<Decimal256Type>(*argument, selection, &mut work, |_, value| {
                Decimal256Type::validate_decimal_precision(value, *precision, *scale).map_err(
                    |_| internal("Negation input exceeds its declared Decimal256 precision"),
                )?;
                value
                    .checked_neg()
                    .map(Some)
                    .ok_or_else(|| internal("Checked Decimal256 negation cannot overflow"))
            })
        }
        _ => Err(invalid("Negation result has no exact numeric overload")),
    }?;
    work.flush()?;
    let result = SelectedValues::try_new_observed(
        selection,
        carrier,
        output,
        errors.into_boxed_slice(),
        || work.step(),
    );
    work.finish_result(result)
}

fn primitive<T: ArrowPrimitiveType>(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
    mut negate: impl FnMut(usize, T::Native) -> Result<Option<T::Native>, KernelFailure>,
) -> Result<ArrayRef, KernelFailure> {
    let array = argument
        .array()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| internal("Negation input differs from its checked numeric carrier"))?;
    output_capacity(selection.len(), std::mem::size_of::<T::Native>())?;
    work.flush()?;
    let mut builder = PrimitiveBuilder::<T>::with_capacity(selection.len())
        .with_data_type(array.data_type().clone());
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let row = argument.value_row(ordinal, batch_row);
        let value = if array.is_null(row) {
            None
        } else {
            negate(ordinal, array.value(row))?
        };
        builder.append_option(value);
    }
    work.flush()?;
    Ok(Arc::new(builder.finish()))
}

fn negate_largeint(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
    errors: &mut Vec<RowDataError>,
) -> Result<ArrayRef, KernelFailure> {
    let array = largeint::as_fixed_size_binary_array(argument.array(), "Negation input")
        .map_err(|_| internal("Negation input differs from its checked LARGEINT carrier"))?;
    output_capacity(selection.len(), LARGEINT_BYTE_WIDTH as usize)?;
    work.flush()?;
    let mut builder = FixedSizeBinaryBuilder::with_capacity(selection.len(), LARGEINT_BYTE_WIDTH);
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let row = argument.value_row(ordinal, batch_row);
        if array.is_null(row) {
            builder.append_null();
            continue;
        }
        let value = largeint::value_at(array, row)
            .map_err(|_| internal("Invalid LARGEINT in checked negation input"))?;
        match integer(
            value.checked_neg(),
            NegationOverflow::LargeInt,
            ordinal,
            errors,
        )? {
            Some(value) => builder
                .append_value(largeint::i128_to_be_bytes(value))
                .map_err(|_| internal("Invalid LARGEINT negation result width"))?,
            None => builder.append_null(),
        }
    }
    work.flush()?;
    Ok(Arc::new(builder.finish()))
}

fn integer<T>(
    negated: Option<T>,
    overflow: NegationOverflow,
    ordinal: usize,
    errors: &mut Vec<RowDataError>,
) -> Result<Option<T>, KernelFailure> {
    if negated.is_none() {
        errors
            .try_reserve(1)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        errors.push(RowDataError::negation_overflow(ordinal, overflow));
    }
    Ok(negated)
}

fn output_capacity(rows: usize, value_width: usize) -> Result<(), KernelFailure> {
    let bytes = rows
        .checked_mul(value_width)
        .ok_or(KernelFailure::ResourceExhausted)?;
    isize::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(())
}
