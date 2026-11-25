// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The shared selected scalar CAST result author. The prepared row body owns
//! conversion semantics; this module retains the actual address and journal.
use super::*;
use crate::{SelectedValues, Selection};
use arrow_array::ArrayRef;
use std::sync::Arc;

pub(super) fn evaluate<'a>(
    recipe: &PreparedCastRecipe,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'a>,
    inherited: &[RowDataError],
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    use super::CastRowResult as R;
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let outcome = (|| {
        match argument {
            EvaluatedArgument::SelectedColumn(values) => {
                if !values
                    .selection()
                    .same_rows_observed(selection, || work.step())?
                    || values.values().len() != selection.len()
                    || !std::ptr::eq(inherited, values.errors())
                {
                    return Err(invalid(
                        "CAST requires its exact selected address and inherited journal",
                    ));
                }
            }
            _ => {
                if !inherited.is_empty() {
                    return Err(invalid("CAST has a foreign inherited journal"));
                }
                argument.validate_shape_observed::<KernelFailure>(selection, || work.step())?;
            }
        }
        let ty = &recipe.result_type().data_type();
        // An unzoned primitive rendering fits within 64 bytes, including the
        // widest Chrono year and nanosecond fraction. Check Arrow's i32 offset
        // extent before producing text. This is not a host memory grant.
        let variable_text = *ty == &DataType::Utf8
            && (*recipe.source_type().data_type() == DataType::Binary
                || matches!(recipe.body, CastBody::Trino(_)));
        if *ty == &DataType::Utf8 && !variable_text {
            selection
                .len()
                .checked_mul(64)
                .filter(|n| *n <= i32::MAX as usize)
                .ok_or(KernelFailure::ResourceExhausted)?;
        }
        // Checked representation and fallible reservation are not a host memory grant.
        let bitmap = selection
            .len()
            .checked_add(7)
            .map(|n| n / 8)
            .and_then(|n| n.checked_add(63))
            .map(|n| n / 64 * 64)
            .ok_or(KernelFailure::ResourceExhausted)?;
        selection
            .len()
            .checked_mul(
                if matches!(ty, DataType::Decimal128(..)) {
                    std::mem::size_of::<Option<i128>>()
                } else {
                    std::mem::size_of::<Option<i64>>()
                } + std::mem::size_of::<RowDataError>()
                    + crate::MAX_ROW_ERROR_MESSAGE_BYTES,
            )
            .and_then(|n| n.checked_add(bitmap))
            .filter(|n| *n <= isize::MAX as usize)
            .ok_or(KernelFailure::ResourceExhausted)?;
        work.flush()?;
        enum Output {
            Boolean(Vec<Option<bool>>),
            Text(Vec<Option<String>>),
            FixedUuid(arrow_array::builder::FixedSizeBinaryBuilder),
            BinaryUuid(arrow_array::builder::BinaryBuilder),
            I8(Vec<Option<i8>>),
            I16(Vec<Option<i16>>),
            I32(Vec<Option<i32>>),
            Date32(Vec<Option<i32>>),
            I64(Vec<Option<i64>>),
            Timestamp(Vec<Option<i64>>),
            TimeMicroseconds(Vec<Option<i64>>),
            U8(Vec<Option<u8>>),
            U16(Vec<Option<u16>>),
            U32(Vec<Option<u32>>),
            U64(Vec<Option<u64>>),
            F32(Vec<Option<f32>>),
            F64(Vec<Option<f64>>),
            Decimal128(Vec<Option<i128>>),
        }
        let mut output = match ty {
            DataType::Boolean => Output::Boolean(Vec::new()),
            DataType::Utf8 => Output::Text(Vec::new()),
            DataType::FixedSizeBinary(16) | DataType::Binary
                if matches!(recipe.body, CastBody::Trino(super::trino::Scalar::Uuid(_))) =>
            {
                let payload = selection
                    .len()
                    .checked_mul(16)
                    .filter(|bytes| *bytes <= i32::MAX as usize)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                if *ty == &DataType::Binary {
                    Output::BinaryUuid(arrow_array::builder::BinaryBuilder::with_capacity(
                        selection.len(),
                        payload,
                    ))
                } else {
                    Output::FixedUuid(arrow_array::builder::FixedSizeBinaryBuilder::with_capacity(
                        selection.len(),
                        16,
                    ))
                }
            }

            DataType::Int8 => Output::I8(Vec::new()),
            DataType::Int16 => Output::I16(Vec::new()),
            DataType::Int32 => Output::I32(Vec::new()),
            DataType::Date32 => Output::Date32(Vec::new()),
            DataType::Int64 => Output::I64(Vec::new()),
            DataType::Timestamp(_, None) => Output::Timestamp(Vec::new()),
            DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
                Output::TimeMicroseconds(Vec::new())
            }
            DataType::UInt8 => Output::U8(Vec::new()),
            DataType::UInt16 => Output::U16(Vec::new()),
            DataType::UInt32 => Output::U32(Vec::new()),
            DataType::UInt64 => Output::U64(Vec::new()),
            DataType::Float32 => Output::F32(Vec::new()),
            DataType::Float64 => Output::F64(Vec::new()),
            DataType::Decimal128(..) => Output::Decimal128(Vec::new()),
            _ => return Err(internal("cast recipe has a foreign frozen result carrier")),
        };
        match &mut output {
            Output::Boolean(v) => v.try_reserve_exact(selection.len()),
            Output::Text(v) => v.try_reserve_exact(selection.len()),
            Output::FixedUuid(_) | Output::BinaryUuid(_) => Ok(()),
            Output::I8(v) => v.try_reserve_exact(selection.len()),
            Output::I16(v) => v.try_reserve_exact(selection.len()),
            Output::I32(v) => v.try_reserve_exact(selection.len()),
            Output::Date32(v) => v.try_reserve_exact(selection.len()),
            Output::I64(v) => v.try_reserve_exact(selection.len()),
            Output::Timestamp(v) => v.try_reserve_exact(selection.len()),
            Output::TimeMicroseconds(v) => v.try_reserve_exact(selection.len()),
            Output::U8(v) => v.try_reserve_exact(selection.len()),
            Output::U16(v) => v.try_reserve_exact(selection.len()),
            Output::U32(v) => v.try_reserve_exact(selection.len()),
            Output::U64(v) => v.try_reserve_exact(selection.len()),
            Output::F32(v) => v.try_reserve_exact(selection.len()),
            Output::F64(v) => v.try_reserve_exact(selection.len()),
            Output::Decimal128(v) => v.try_reserve_exact(selection.len()),
        }
        .map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut errors = Vec::new();
        errors
            .try_reserve_exact(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut inherited = inherited.iter().peekable();
        let mut variable_text_bytes = 0_usize;
        for (ordinal, row) in selection.iter().enumerate() {
            let value = if inherited
                .peek()
                .is_some_and(|e| e.selected_ordinal() == ordinal)
            {
                R::RowError(inherited.next().expect("checked inherited error").clone())
            } else {
                work.flush()?;
                recipe.evaluate_row(argument, ordinal, row, control)?
            };
            let value = match value {
                R::RowError(error) => {
                    errors.push(error.with_selected_ordinal(ordinal));
                    R::Null
                }
                other => other,
            };
            match (&mut output, value) {
                (Output::Boolean(v), R::Boolean(n)) => v.push(Some(n)),
                (Output::Text(v), R::Text(n)) => {
                    if variable_text {
                        variable_text_bytes = variable_text_bytes
                            .checked_add(n.len())
                            .filter(|bytes| *bytes <= i32::MAX as usize)
                            .ok_or(KernelFailure::ResourceExhausted)?;
                    }
                    v.push(Some(n));
                }
                (Output::Text(v), R::Null) => v.push(None),
                (Output::FixedUuid(v), R::UuidBytes(bytes)) => v
                    .append_value(bytes)
                    .map_err(|_| internal("CAST result lost its UUID byte width"))?,
                (Output::BinaryUuid(v), R::UuidBytes(bytes)) => v.append_value(bytes),
                (Output::FixedUuid(v), R::Null) => v.append_null(),
                (Output::BinaryUuid(v), R::Null) => v.append_null(),

                (Output::I8(v), R::Signed(n)) => v.push(Some(
                    i8::try_from(n).map_err(|_| internal("cast returned an out-of-range Int8"))?,
                )),
                (Output::I16(v), R::Signed(n)) => v
                    .push(Some(i16::try_from(n).map_err(|_| {
                        internal("cast returned an out-of-range Int16")
                    })?)),
                (Output::I32(v), R::Signed(n)) => v
                    .push(Some(i32::try_from(n).map_err(|_| {
                        internal("cast returned an out-of-range Int32")
                    })?)),
                (Output::I64(v), R::Signed(n)) => v.push(Some(n)),
                (Output::TimeMicroseconds(v), R::Signed(n)) => v.push(Some(n)),
                (Output::Date32(v), R::Signed(n)) => v
                    .push(Some(i32::try_from(n).map_err(|_| {
                        internal("cast returned an out-of-range Date32")
                    })?)),
                (Output::Timestamp(v), R::Timestamp(n)) => v.push(Some(n)),
                (Output::U8(v), R::Unsigned(n)) => {
                    v.push(Some(u8::try_from(n).map_err(|_| {
                        internal("cast returned an out-of-range UInt8")
                    })?))
                }
                (Output::U16(v), R::Unsigned(n)) => v
                    .push(Some(u16::try_from(n).map_err(|_| {
                        internal("cast returned an out-of-range UInt16")
                    })?)),
                (Output::U32(v), R::Unsigned(n)) => v
                    .push(Some(u32::try_from(n).map_err(|_| {
                        internal("cast returned an out-of-range UInt32")
                    })?)),
                (Output::U64(v), R::Unsigned(n)) => v.push(Some(n)),
                (Output::F32(v), R::Float32(n)) => v.push(Some(n)),
                (Output::F64(v), R::Float64(n)) => v.push(Some(n)),
                (Output::Decimal128(v), R::Decimal128(n)) => v.push(Some(n)),
                (Output::I8(v), R::Null) => v.push(None),
                (Output::Boolean(v), R::Null) => v.push(None),
                (Output::I16(v), R::Null) => v.push(None),
                (Output::I32(v), R::Null) => v.push(None),
                (Output::Date32(v), R::Null) => v.push(None),
                (Output::I64(v), R::Null) => v.push(None),
                (Output::Timestamp(v), R::Null) => v.push(None),
                (Output::TimeMicroseconds(v), R::Null) => v.push(None),
                (Output::U8(v), R::Null) => v.push(None),
                (Output::U16(v), R::Null) => v.push(None),
                (Output::U32(v), R::Null) => v.push(None),
                (Output::U64(v), R::Null) => v.push(None),
                (Output::F32(v), R::Null) => v.push(None),
                (Output::F64(v), R::Null) => v.push(None),
                (Output::Decimal128(v), R::Null) => v.push(None),
                _ => {
                    return Err(internal(
                        "cast body returned a foreign result representation",
                    ));
                }
            }
            work.step()?;
        }
        work.flush()?;
        // Arrow construction is an opaque observed boundary, not internally cooperative allocation.
        let array: ArrayRef = match output {
            Output::Boolean(v) => Arc::new(BooleanArray::from(v)),
            Output::Text(v) => Arc::new(arrow_array::StringArray::from(v)),
            Output::FixedUuid(mut v) => Arc::new(v.finish()),
            Output::BinaryUuid(mut v) => Arc::new(v.finish()),

            Output::I8(v) => Arc::new(Int8Array::from(v)),
            Output::I16(v) => Arc::new(Int16Array::from(v)),
            Output::I32(v) => Arc::new(Int32Array::from(v)),
            Output::Date32(v) => Arc::new(arrow_array::Date32Array::from(v)),
            Output::I64(v) => Arc::new(Int64Array::from(v)),
            Output::TimeMicroseconds(v) => Arc::new(arrow_array::Time64MicrosecondArray::from(v)),
            Output::Timestamp(v) => match ty {
                DataType::Timestamp(arrow_schema::TimeUnit::Second, None) => {
                    Arc::new(arrow_array::TimestampSecondArray::from(v))
                }
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None) => {
                    Arc::new(arrow_array::TimestampMillisecondArray::from(v))
                }
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => {
                    Arc::new(arrow_array::TimestampMicrosecondArray::from(v))
                }
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None) => {
                    Arc::new(arrow_array::TimestampNanosecondArray::from(v))
                }
                _ => {
                    return Err(internal(
                        "timestamp cast has a foreign frozen result carrier",
                    ));
                }
            },
            Output::U8(v) => Arc::new(arrow_array::UInt8Array::from(v)),
            Output::U16(v) => Arc::new(arrow_array::UInt16Array::from(v)),
            Output::U32(v) => Arc::new(arrow_array::UInt32Array::from(v)),
            Output::U64(v) => Arc::new(arrow_array::UInt64Array::from(v)),
            Output::F32(v) => Arc::new(Float32Array::from(v)),
            Output::F64(v) => Arc::new(Float64Array::from(v)),
            Output::Decimal128(v) => {
                let DataType::Decimal128(precision, scale) = *ty else {
                    return Err(internal(
                        "Decimal128 cast output metadata differs from its frozen result",
                    ));
                };
                Arc::new(
                    Decimal128Array::from(v)
                        .with_precision_and_scale(*precision, *scale)
                        .map_err(|_| {
                            internal(
                                "Decimal128 cast output differs from its frozen precision and scale",
                            )
                        })?,
                )
            }
        };
        work.flush()?;
        SelectedValues::try_new_observed(selection, ty, array, errors.into_boxed_slice(), || {
            work.step()
        })
    })();
    work.finish_result(outcome)
}
