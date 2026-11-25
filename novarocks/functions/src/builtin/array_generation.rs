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

//! Hosted selected-row generation for Trino repeat and scalar sequence.

use std::{alloc::Layout, sync::Arc};

use allocator_api2::vec::Vec as HostVec;
use arrow_array::{Array, Date32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT;
use novarocks_type_contract::{
    PICOS_PER_DAY, TemporalMicros, TemporalType, TemporalValue, ValueKind,
};
use novarocks_value_arrays::decode::PreparedTemporalArray;

use super::{
    array_scalar_output::scratch,
    date_shift::{self, ShiftError, Unit},
    temporal_output::TemporalOutputBuilder,
};
use crate::{
    AggregateStateAllocator, DataFailureReason, FunctionValueType, KernelEvaluationControl,
    KernelFailure, RowDataError, ScalarCallInput, SelectedValues,
    aggregate_host_allocator::HostAggregateAllocator,
    arrow_result_custody::{custody_type_metadata_upper_bound, retain_result_backing},
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    opaque_memory::OpaqueRetainedCharge,
    selected_copy::{ExtendSegment, list_child_count, list_from_segments_in},
    temporal_arithmetic::Value,
    temporal_cast::Type,
};

const MAX_REPEAT: i64 = 100_000;
const MAX_SEQUENCE: i128 = 10_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Operation {
    Repeat,
    Sequence,
}

#[derive(Debug)]
enum ValueFailure {
    InvalidArgument(&'static str),
    Calendar(ShiftError),
}

impl From<ShiftError> for ValueFailure {
    fn from(error: ShiftError) -> Self {
        Self::Calendar(error)
    }
}

impl ValueFailure {
    fn at(self, ordinal: usize) -> Result<RowDataError, KernelFailure> {
        match self {
            Self::InvalidArgument(message) => Ok(RowDataError::with_reason(
                ordinal,
                DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT),
                message,
            )),
            Self::Calendar(error) => RowDataError::from_domain(ordinal, error),
        }
    }
}

fn repeat_length(count: i64) -> Result<usize, ValueFailure> {
    match count {
        count if count < 0 => Err(ValueFailure::InvalidArgument(
            "count argument of repeat function must be greater than or equal to 0",
        )),
        count if count > MAX_REPEAT => Err(ValueFailure::InvalidArgument(
            "count argument of repeat function must be less than or equal to 100000",
        )),
        count => Ok(count as usize),
    }
}

fn check_step<T: Ord>(start: T, stop: T, step: i128) -> Result<(), ValueFailure> {
    if step == 0 {
        return Err(ValueFailure::InvalidArgument("step must not be zero"));
    }
    if (step > 0 && start > stop) || (step < 0 && start < stop) {
        return Err(ValueFailure::InvalidArgument(
            "sequence stop value must follow the direction of step",
        ));
    }
    Ok(())
}

fn checked_length(length: i128) -> Result<usize, ValueFailure> {
    if !(1..=MAX_SEQUENCE).contains(&length) {
        return Err(ValueFailure::InvalidArgument(
            "result of sequence function must not have more than 10000 entries",
        ));
    }
    Ok(length as usize)
}

#[derive(Clone, Copy, Debug)]
enum Series {
    Bigint {
        start: i64,
        step: i64,
    },
    Date {
        start: i32,
        days: i128,
    },
    Timestamp {
        start: TemporalMicros,
        picos: i128,
    },
    Months {
        start: Value,
        step: i32,
        ty: Option<TemporalType>,
    },
}

impl Series {
    fn append(
        self,
        position: usize,
        output: &mut TemporalOutputBuilder,
        output_type: Type,
    ) -> Result<(), KernelFailure> {
        let index = position as i128;
        match self {
            Self::Bigint { start, step } => {
                let value = i128::from(start) + index * i128::from(step);
                output.bigint(
                    i64::try_from(value)
                        .map_err(|_| internal("BIGINT sequence value exceeds its checked range"))?,
                )
            }
            Self::Date { start, days } => {
                let day = i128::from(start) + index * days;
                output.date(
                    i32::try_from(day)
                        .map_err(|_| internal("DATE sequence value exceeds its checked range"))?,
                )
            }
            Self::Timestamp { start, picos } => {
                let value = TemporalMicros::from_picos(start.as_picos() + index * picos)
                    .ok_or_else(|| internal("TIMESTAMP sequence exceeds its checked range"))?;
                output.temporal(
                    output_type.temporal().ok_or_else(|| {
                        internal("TIMESTAMP sequence result has no temporal declaration")
                    })?,
                    TemporalValue::Timestamp(value),
                )
            }
            Self::Months { start, step, ty } => {
                let value = if position == 0 {
                    start
                } else {
                    date_shift::add(Unit::Month, position as i64 * i64::from(step), ty, start)
                        .map_err(|_| internal("month sequence exceeds its checked range"))?
                };
                match value {
                    Value::Date(day) => output.date(day),
                    Value::Temporal(value @ TemporalValue::Timestamp(_)) => output.temporal(
                        ty.ok_or_else(|| internal("month sequence has a foreign declaration"))?,
                        value,
                    ),
                    _ => Err(internal("month sequence produced a foreign value")),
                }
            }
        }
    }
}

fn bigint_series(start: i64, stop: i64, step: i64) -> Result<(Series, usize), ValueFailure> {
    let step_wide = i128::from(step);
    check_step(start, stop, step_wide)?;
    let count = checked_length((i128::from(stop) - i128::from(start)) / step_wide + 1)?;
    Ok((Series::Bigint { start, step }, count))
}

fn date_series(start: i32, stop: i32, days: i128) -> Result<(Series, usize), ValueFailure> {
    check_step(start, stop, days)?;
    let count = checked_length((i128::from(stop) - i128::from(start)) / days + 1)?;
    Ok((Series::Date { start, days }, count))
}

fn timestamp_series(
    start: TemporalMicros,
    stop: TemporalMicros,
    picos: i128,
) -> Result<(Series, usize), ValueFailure> {
    check_step(start, stop, picos)?;
    let count = checked_length((stop.as_picos() - start.as_picos()) / picos + 1)?;
    Ok((Series::Timestamp { start, picos }, count))
}

fn month_series(
    start: Value,
    stop: Value,
    step: i32,
    ty: Option<TemporalType>,
) -> Result<(Series, usize), ValueFailure> {
    let step_wide = i128::from(step);
    match (start, stop) {
        (Value::Date(start), Value::Date(stop)) => check_step(start, stop, step_wide)?,
        (
            Value::Temporal(TemporalValue::Timestamp(start)),
            Value::Temporal(TemporalValue::Timestamp(stop)),
        ) => check_step(start, stop, step_wide)?,
        _ => return Err(ShiftError::ForeignValue.into()),
    }
    let months = date_shift::difference(Unit::Month, start, stop)?;
    let count = checked_length(i128::from(months) / step_wide + 1)?;
    if count > 1 {
        // The calendar step is monotone, so validating the far endpoint
        // also proves that the intermediate generated values fit the carrier.
        date_shift::add(Unit::Month, (count as i64 - 1) * i64::from(step), ty, start)?;
    }
    Ok((Series::Months { start, step, ty }, count))
}

enum Input<'a> {
    Bigint(&'a Int64Array),
    Date(&'a Date32Array),
    Temporal(PreparedTemporalArray<'a>),
}

impl<'a> Input<'a> {
    fn bind(ty: &FunctionValueType, array: &'a dyn Array) -> Result<Self, KernelFailure> {
        let mismatch = || invalid("sequence argument differs from its frozen type");
        if let Some(temporal) = ty.value_type().temporal() {
            return PreparedTemporalArray::bind(temporal, array, ty.nullable())
                .map(Self::Temporal)
                .map_err(|_| mismatch());
        }
        match ty.data_type() {
            DataType::Int64 => array.as_any().downcast_ref().map(Self::Bigint),
            DataType::Date32 => array.as_any().downcast_ref().map(Self::Date),
            _ => None,
        }
        .ok_or_else(mismatch)
    }

    fn value(&self, row: usize) -> Result<Option<Value>, KernelFailure> {
        Ok(match self {
            Self::Bigint(array) => (!array.is_null(row)).then(|| Value::Bigint(array.value(row))),
            Self::Date(array) => (!array.is_null(row)).then(|| Value::Date(array.value(row))),
            Self::Temporal(array) => array
                .value(row)
                .map_err(|_| invalid("sequence received an invalid temporal value"))?
                .map(Value::Temporal),
        })
    }
}

fn selected_series(
    start: Value,
    stop: Value,
    step: Option<Value>,
    output_type: Type,
) -> Result<(Series, usize), ValueFailure> {
    match (start, stop, step) {
        (Value::Bigint(start), Value::Bigint(stop), None) => {
            bigint_series(start, stop, if start <= stop { 1 } else { -1 })
        }
        (Value::Bigint(start), Value::Bigint(stop), Some(Value::Bigint(step))) => {
            bigint_series(start, stop, step)
        }
        (Value::Date(start), Value::Date(stop), None) => {
            date_series(start, stop, if start <= stop { 1 } else { -1 })
        }
        (
            Value::Date(start),
            Value::Date(stop),
            Some(Value::Temporal(TemporalValue::IntervalDayTime(step))),
        ) => {
            let (step, day) = (step.as_picos(), i128::from(PICOS_PER_DAY));
            if step % day != 0 {
                return Err(ValueFailure::InvalidArgument(
                    "sequence step must be a day interval if start and end values are dates",
                ));
            }
            date_series(start, stop, step / day)
        }
        (
            Value::Temporal(TemporalValue::Timestamp(start)),
            Value::Temporal(TemporalValue::Timestamp(stop)),
            Some(Value::Temporal(TemporalValue::IntervalDayTime(step))),
        ) => timestamp_series(start, stop, step.as_picos()),
        (start, stop, Some(Value::Temporal(TemporalValue::IntervalYearMonth(step)))) => {
            month_series(start, stop, step, output_type.temporal())
        }
        _ => Err(ShiftError::ForeignValue.into()),
    }
}

pub(super) fn evaluate<'a>(
    operation: Operation,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = evaluate_selected(operation, input, host, allocator, &mut work);
    work.finish_result(result)
}

fn evaluate_selected<'a>(
    operation: Operation,
    input: ScalarCallInput<'_, 'a>,
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let target = input.contract().result_type().data_type();
    let DataType::List(field) = target else {
        return Err(internal("array generator result is not a List"));
    };
    let selection = input.selection();
    work.flush()?;
    let mut row_lengths = scratch(selection.len(), allocator)?;
    let mut failures = scratch(selection.len(), allocator)?;

    let output = match operation {
        Operation::Repeat => {
            let [value, count] = input.arguments() else {
                return Err(internal("repeat has an invalid arity"));
            };
            let counts = count
                .array()
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| internal("repeat count is not BIGINT"))?;
            let mut segments = scratch(selection.len(), allocator)?;
            for (ordinal, logical_row) in selection.iter().enumerate() {
                work.step()?;
                let count_row = count.value_row(ordinal, logical_row);
                if counts.is_null(count_row) {
                    row_lengths.push(None);
                    continue;
                }
                match repeat_length(counts.value(count_row)) {
                    Ok(length) => {
                        row_lengths.push(Some(length));
                        if length != 0 {
                            segments.push(ExtendSegment {
                                source: 0,
                                start: value.value_row(ordinal, logical_row),
                                len: 1,
                                repeats: length,
                                nulls: 0,
                            });
                        }
                    }
                    Err(error) => {
                        row_lengths.push(None);
                        failures.push((ordinal, error));
                    }
                }
            }
            list_from_segments_in(
                field,
                std::slice::from_ref(value.array()),
                &segments,
                &row_lengths,
                host,
                allocator,
                work,
            )?
        }
        Operation::Sequence => {
            let (values, len) = sequence_values(
                input,
                &mut row_lengths,
                &mut failures,
                host,
                allocator,
                work,
            )?;
            let span = [ExtendSegment {
                source: 0,
                start: 0,
                len,
                repeats: 1,
                nulls: 0,
            }];
            list_from_segments_in(field, &[values], &span, &row_lengths, host, allocator, work)?
        }
    };

    let charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
    let layout = Layout::array::<RowDataError>(failures.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let _reservation = charge.reserve_operation(layout.size())?;
    let mut errors = Vec::new();
    errors
        .try_reserve_exact(failures.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    for (ordinal, cause) in failures {
        work.step()?;
        errors.push(cause.at(ordinal)?);
    }
    SelectedValues::try_new_observed(selection, target, output, errors.into_boxed_slice(), || {
        work.step()
    })
}

fn sequence_values(
    input: ScalarCallInput<'_, '_>,
    row_lengths: &mut HostVec<Option<usize>, HostAggregateAllocator>,
    failures: &mut HostVec<(usize, ValueFailure), HostAggregateAllocator>,
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(arrow_array::ArrayRef, usize), KernelFailure> {
    let arguments = input.arguments();
    let mut types = input.contract().value_argument_types();
    let [start, stop, steps @ ..] = arguments else {
        return Err(internal("sequence has an invalid arity"));
    };
    let (Some(start_type), Some(stop_type)) = (types.next(), types.next()) else {
        return Err(internal("sequence has invalid frozen arguments"));
    };
    let starts = Input::bind(start_type, start.array().as_ref())?;
    let stops = Input::bind(stop_type, stop.array().as_ref())?;
    let step = match (steps, types.next()) {
        ([], None) => None,
        ([step], Some(ty)) => Some((*step, Input::bind(ty, step.array().as_ref())?)),
        _ => return Err(invalid("sequence has an invalid step type")),
    };
    let result = input.contract().result_type();
    let (ValueKind::List(item), DataType::List(field)) =
        (result.value_type().kind(), result.data_type())
    else {
        return Err(invalid("sequence result has no frozen child type"));
    };
    let output_type = match item.kind() {
        ValueKind::Scalar(DataType::Int64) => Type::Bigint,
        ValueKind::Scalar(DataType::Date32) => Type::Date,
        ValueKind::Temporal(ty @ TemporalType::Timestamp(_)) => Type::Temporal(*ty),
        _ => return Err(invalid("sequence result has an invalid child type")),
    };

    let selection = input.selection();
    let mut series = scratch(selection.len(), allocator)?;
    for (ordinal, logical_row) in selection.iter().enumerate() {
        work.step()?;
        let start_row = start.value_row(ordinal, logical_row);
        let stop_row = stop.value_row(ordinal, logical_row);
        if start.array().is_null(start_row) || stop.array().is_null(stop_row) {
            row_lengths.push(None);
            continue;
        }
        let step = if let Some((argument, steps)) = &step {
            let step_row = argument.value_row(ordinal, logical_row);
            if argument.array().is_null(step_row) {
                row_lengths.push(None);
                continue;
            }
            steps.value(step_row)?
        } else {
            None
        };
        let start = starts
            .value(start_row)?
            .ok_or_else(|| internal("sequence start vanished"))?;
        let stop = stops
            .value(stop_row)?
            .ok_or_else(|| internal("sequence stop vanished"))?;
        match selected_series(start, stop, step, output_type) {
            Ok((shape, length)) => {
                series.push((shape, length));
                row_lengths.push(Some(length));
            }
            Err(cause) => {
                failures.push((ordinal, cause));
                row_lengths.push(None);
            }
        }
    }
    let total = list_child_count(row_lengths, work)?;
    let width = field
        .data_type()
        .primitive_width()
        .or_else(|| match field.data_type() {
            DataType::FixedSizeBinary(width) => usize::try_from(*width).ok(),
            _ => None,
        })
        .ok_or_else(|| internal("sequence result child must have a fixed width"))?;
    // The fixed-width builder never appends NULL and receives the exact
    // element count. Arrow rounds buffer capacity up to a 64-byte boundary.
    let payload = total
        .checked_mul(width)
        .and_then(|bytes| bytes.checked_add(63))
        .ok_or(KernelFailure::ResourceExhausted)?
        & !63;
    let capacity = payload
        .checked_add(custody_type_metadata_upper_bound(1)?)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
    work.flush()?;
    let mut reservation = charge.reserve_operation(capacity)?;
    work.flush()?;
    let mut output = TemporalOutputBuilder::new(output_type, total)?;
    for (shape, length) in series {
        for index in 0..length {
            work.step()?;
            shape.append(index, &mut output, output_type)?;
        }
    }
    work.flush()?;
    let values = output.finish();
    let retained =
        retain_result_backing(values, charge, &mut reservation, allocator.clone(), work)?;
    Ok((retained.values, total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_generation_limits_and_wide_ranges() {
        assert_eq!(repeat_length(0).unwrap(), 0);
        assert_eq!(repeat_length(MAX_REPEAT).unwrap(), MAX_REPEAT as usize);
        assert!(repeat_length(-1).is_err());
        assert!(repeat_length(MAX_REPEAT + 1).is_err());
        assert_eq!(bigint_series(0, 9_999, 1).unwrap().1, MAX_SEQUENCE as usize);
        assert!(bigint_series(0, 10_000, 1).is_err());
        assert!(bigint_series(i64::MIN, i64::MAX, 1).is_err());
        assert_eq!(bigint_series(i64::MIN, i64::MAX, i64::MAX).unwrap().1, 3);
        assert_eq!(bigint_series(i64::MAX, i64::MIN, i64::MIN).unwrap().1, 2);
        assert!(bigint_series(1, 1, 0).is_err());
        assert_eq!(bigint_series(1, 1, -1).unwrap().1, 1);
    }

    #[test]
    fn array_generation_pico_direction_and_epoch_crossing() {
        let near_zero = TemporalMicros::new(0, 1).unwrap();
        let before_zero = TemporalMicros::new(-1, 999_999).unwrap();
        assert_eq!(timestamp_series(before_zero, near_zero, 1).unwrap().1, 3);
        let early = Value::Temporal(TemporalValue::Timestamp(TemporalMicros::whole_micros(0)));
        let late = Value::Temporal(TemporalValue::Timestamp(near_zero));
        let ty = Some(TemporalType::Timestamp(
            novarocks_type_contract::TemporalPrecision::PICOSECONDS,
        ));
        assert!(month_series(early, late, -1, ty).is_err());
        assert!(month_series(late, early, 1, ty).is_err());
        assert_eq!(month_series(early, late, 1, ty).unwrap().1, 1);
        assert_eq!(month_series(late, early, -1, ty).unwrap().1, 1);
        assert_eq!(month_series(late, late, -1, ty).unwrap().1, 1);
    }

    #[test]
    fn array_generation_month_end_is_anchored() {
        use arrow_array::cast::AsArray;
        let start = crate::temporal_value::gregorian_epoch_day(2025, 1, 31).unwrap() as i32;
        let end = crate::temporal_value::gregorian_epoch_day(2025, 4, 30).unwrap() as i32;
        let march = crate::temporal_value::gregorian_epoch_day(2025, 3, 31).unwrap() as i32;
        let (series, count) = month_series(Value::Date(start), Value::Date(end), 1, None).unwrap();
        assert_eq!(count, 4);
        let mut output = TemporalOutputBuilder::new(Type::Date, count).unwrap();
        for index in 0..count {
            series.append(index, &mut output, Type::Date).unwrap();
        }
        let output = output.finish();
        assert_eq!(
            output
                .as_primitive::<arrow_array::types::Date32Type>()
                .value(2),
            march
        );
        let subday = Value::Temporal(TemporalValue::IntervalDayTime(
            TemporalMicros::whole_micros(1),
        ));
        assert!(
            selected_series(
                Value::Date(start),
                Value::Date(end),
                Some(subday),
                Type::Date
            )
            .is_err()
        );
    }
}
