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

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

use crate::runtime_filter::port::value_domain::{
    CanonicalF32, CanonicalF64, MembershipValues, ValueDomainDelta,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MembershipEncodingUnavailable {
    ResourceOrSize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipEncodingOutcome {
    Deltas(Vec<ValueDomainDelta>),
    Unavailable(MembershipEncodingUnavailable),
}

impl MembershipEncodingOutcome {
    pub fn into_deltas(self) -> Option<Vec<ValueDomainDelta>> {
        match self {
            Self::Deltas(deltas) => Some(deltas),
            Self::Unavailable(_) => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipEncodingError {
    TypeMismatch {
        expected: DataType,
        actual: DataType,
    },
    UnsupportedType(DataType),
    InvalidArray {
        data_type: DataType,
        detail: String,
    },
    InvalidDecimal {
        precision: u8,
        scale: i8,
        detail: String,
    },
}

impl fmt::Display for MembershipEncodingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TypeMismatch { expected, actual } => write!(
                formatter,
                "runtime filter membership type mismatch: expected {expected:?}, got {actual:?}"
            ),
            Self::UnsupportedType(data_type) => {
                write!(
                    formatter,
                    "unsupported runtime filter membership type: {data_type:?}"
                )
            }
            Self::InvalidArray { data_type, detail } => write!(
                formatter,
                "invalid runtime filter membership array for {data_type:?}: {detail}"
            ),
            Self::InvalidDecimal {
                precision,
                scale,
                detail,
            } => write!(
                formatter,
                "invalid runtime filter Decimal128({precision}, {scale}) membership value: {detail}"
            ),
        }
    }
}

impl Error for MembershipEncodingError {}

pub struct MembershipDeltaEncoder;

impl MembershipDeltaEncoder {
    pub fn encode(
        array: &dyn Array,
        expected_type: &DataType,
        max_bytes: usize,
    ) -> Result<MembershipEncodingOutcome, MembershipEncodingError> {
        if array.data_type() != expected_type {
            return Err(MembershipEncodingError::TypeMismatch {
                expected: expected_type.clone(),
                actual: array.data_type().clone(),
            });
        }

        macro_rules! primitive {
            ($array_ty:ty, $width:expr, $ctor:expr) => {{
                let typed = downcast::<$array_ty>(array, expected_type)?;
                if !minimum_frame_fits(
                    expected_type,
                    typed.null_count() != typed.len(),
                    $width,
                    max_bytes,
                )? {
                    return Ok(unavailable());
                }
                encode_ordered_rows(
                    (0..typed.len())
                        .map(|index| (!typed.is_null(index)).then(|| typed.value(index))),
                    |_| Some($width),
                    $ctor,
                    max_bytes,
                )
            }};
        }

        match expected_type {
            DataType::Boolean => primitive!(BooleanArray, 1, MembershipValues::boolean_set),
            DataType::Int8 => primitive!(Int8Array, 1, MembershipValues::int8_set),
            DataType::Int16 => primitive!(Int16Array, 2, MembershipValues::int16_set),
            DataType::Int32 => primitive!(Int32Array, 4, MembershipValues::int32_set),
            DataType::Int64 => primitive!(Int64Array, 8, MembershipValues::int64_set),
            DataType::FixedSizeBinary(width)
                if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
            {
                let typed = downcast::<FixedSizeBinaryArray>(array, expected_type)?;
                if !minimum_frame_fits(
                    expected_type,
                    typed.null_count() != typed.len(),
                    16,
                    max_bytes,
                )? {
                    return Ok(unavailable());
                }
                let rows = (0..typed.len()).map(|index| {
                    (!typed.is_null(index)).then(|| {
                        novarocks_types::largeint::i128_from_be_bytes(typed.value(index))
                            .expect("FixedSizeBinary(16) always contains one complete i128")
                    })
                });
                encode_ordered_rows(
                    rows,
                    |_| Some(16),
                    MembershipValues::large_int_set,
                    max_bytes,
                )
            }
            DataType::Float32 => {
                let typed = downcast::<Float32Array>(array, expected_type)?;
                if !minimum_frame_fits(
                    expected_type,
                    typed.null_count() != typed.len(),
                    4,
                    max_bytes,
                )? {
                    return Ok(unavailable());
                }
                encode_ordered_rows(
                    (0..typed.len()).map(|index| {
                        (!typed.is_null(index)).then(|| CanonicalF32::new(typed.value(index)))
                    }),
                    |_| Some(4),
                    MembershipValues::float32_set,
                    max_bytes,
                )
            }
            DataType::Float64 => {
                let typed = downcast::<Float64Array>(array, expected_type)?;
                if !minimum_frame_fits(
                    expected_type,
                    typed.null_count() != typed.len(),
                    8,
                    max_bytes,
                )? {
                    return Ok(unavailable());
                }
                encode_ordered_rows(
                    (0..typed.len()).map(|index| {
                        (!typed.is_null(index)).then(|| CanonicalF64::new(typed.value(index)))
                    }),
                    |_| Some(8),
                    MembershipValues::float64_set,
                    max_bytes,
                )
            }
            DataType::Utf8 => {
                let typed = downcast::<StringArray>(array, expected_type)?;
                if !minimum_frame_fits(
                    expected_type,
                    typed.null_count() != typed.len(),
                    8,
                    max_bytes,
                )? {
                    return Ok(unavailable());
                }
                if !utf8_scalar_frames_fit(typed, max_bytes)? {
                    return Ok(unavailable());
                }
                encode_utf8_rows(typed, max_bytes)
            }
            DataType::Date32 => primitive!(Date32Array, 4, MembershipValues::date32_set),
            DataType::Timestamp(unit, timezone) => match unit {
                TimeUnit::Second => encode_timestamp_rows::<TimestampSecondArray>(
                    array,
                    expected_type,
                    unit,
                    timezone,
                    max_bytes,
                ),
                TimeUnit::Millisecond => encode_timestamp_rows::<TimestampMillisecondArray>(
                    array,
                    expected_type,
                    unit,
                    timezone,
                    max_bytes,
                ),
                TimeUnit::Microsecond => encode_timestamp_rows::<TimestampMicrosecondArray>(
                    array,
                    expected_type,
                    unit,
                    timezone,
                    max_bytes,
                ),
                TimeUnit::Nanosecond => encode_timestamp_rows::<TimestampNanosecondArray>(
                    array,
                    expected_type,
                    unit,
                    timezone,
                    max_bytes,
                ),
            },
            DataType::Decimal128(precision, scale) => {
                let typed = downcast::<Decimal128Array>(array, expected_type)?;
                let precision = *precision;
                let scale = *scale;
                let mut validated_contains_null = false;
                for index in 0..typed.len() {
                    if typed.is_null(index) {
                        validated_contains_null = true;
                        continue;
                    }
                    MembershipValues::validate_decimal128_scalar(
                        precision,
                        scale,
                        typed.value(index),
                    )
                    .map_err(|error| {
                        MembershipEncodingError::InvalidDecimal {
                            precision,
                            scale,
                            detail: error.to_string(),
                        }
                    })?;
                }
                if !minimum_frame_fits(
                    expected_type,
                    typed.null_count() != typed.len(),
                    16,
                    max_bytes,
                )? {
                    return Ok(unavailable());
                }
                let (values, contains_null) = collect_ordered_rows(
                    (0..typed.len())
                        .map(|index| (!typed.is_null(index)).then(|| typed.value(index))),
                );
                debug_assert_eq!(contains_null, validated_contains_null);
                encode_ordered_set(
                    values,
                    contains_null,
                    |_| Some(16),
                    move |values| {
                        MembershipValues::decimal128_set(precision, scale, values).map_err(
                            |error| MembershipEncodingError::InvalidDecimal {
                                precision,
                                scale,
                                detail: error.to_string(),
                            },
                        )
                    },
                    max_bytes,
                )
            }
            other => Err(MembershipEncodingError::UnsupportedType(other.clone())),
        }
    }
}

fn downcast<'a, T: Array + 'static>(
    array: &'a dyn Array,
    data_type: &DataType,
) -> Result<&'a T, MembershipEncodingError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| MembershipEncodingError::InvalidArray {
            data_type: data_type.clone(),
            detail: "Arrow physical array does not match its declared data type".to_string(),
        })
}

fn encode_timestamp_rows<T>(
    array: &dyn Array,
    data_type: &DataType,
    unit: &TimeUnit,
    timezone: &Option<std::sync::Arc<str>>,
    max_bytes: usize,
) -> Result<MembershipEncodingOutcome, MembershipEncodingError>
where
    T: Array + 'static,
    for<'a> &'a T: TimestampValueAt,
{
    let typed = downcast::<T>(array, data_type)?;
    if !minimum_frame_fits(data_type, typed.null_count() != typed.len(), 8, max_bytes)? {
        return Ok(unavailable());
    }
    let unit = unit.clone();
    let timezone = timezone.clone();
    encode_ordered_rows(
        (0..typed.len()).map(|index| (!typed.is_null(index)).then(|| typed.timestamp_value(index))),
        |_| Some(8),
        move |values| MembershipValues::timestamp_set(unit.clone(), timezone.clone(), values),
        max_bytes,
    )
}

trait TimestampValueAt {
    fn timestamp_value(self, index: usize) -> i64;
}

macro_rules! timestamp_value_at {
    ($array_ty:ty) => {
        impl TimestampValueAt for &$array_ty {
            fn timestamp_value(self, index: usize) -> i64 {
                self.value(index)
            }
        }
    };
}

timestamp_value_at!(TimestampSecondArray);
timestamp_value_at!(TimestampMillisecondArray);
timestamp_value_at!(TimestampMicrosecondArray);
timestamp_value_at!(TimestampNanosecondArray);

fn encode_ordered_rows<T: Ord>(
    rows: impl IntoIterator<Item = Option<T>>,
    increment: impl Fn(&T) -> Option<usize>,
    mut build: impl FnMut(BTreeSet<T>) -> MembershipValues,
    max_bytes: usize,
) -> Result<MembershipEncodingOutcome, MembershipEncodingError> {
    let (values, contains_null) = collect_ordered_rows(rows);
    encode_ordered_set(
        values,
        contains_null,
        increment,
        move |values| Ok(build(values)),
        max_bytes,
    )
}

fn minimum_frame_fits(
    data_type: &DataType,
    has_non_null_value: bool,
    scalar_increment: usize,
    max_bytes: usize,
) -> Result<bool, MembershipEncodingError> {
    let empty = MembershipValues::empty_for_data_type(data_type)
        .ok_or_else(|| MembershipEncodingError::UnsupportedType(data_type.clone()))?;
    let empty_len = ValueDomainDelta::new(empty, false)
        .canonical_encoded_len()
        .map_err(|error| MembershipEncodingError::InvalidArray {
            data_type: data_type.clone(),
            detail: error.to_string(),
        })?;
    if empty_len > max_bytes {
        return Ok(false);
    }
    if !has_non_null_value {
        return Ok(true);
    }
    Ok(empty_len
        .checked_add(scalar_increment)
        .is_some_and(|minimum| minimum <= max_bytes))
}

fn utf8_scalar_frames_fit(
    array: &StringArray,
    max_bytes: usize,
) -> Result<bool, MembershipEncodingError> {
    let empty_len = ValueDomainDelta::new(MembershipValues::utf8_set(BTreeSet::new()), false)
        .canonical_encoded_len()
        .map_err(|error| MembershipEncodingError::InvalidArray {
            data_type: DataType::Utf8,
            detail: error.to_string(),
        })?;
    for index in 0..array.len() {
        if array.is_null(index) {
            continue;
        }
        let Some(frame_len) = 8usize.checked_add(array.value(index).len()) else {
            return Ok(false);
        };
        if empty_len
            .checked_add(frame_len)
            .is_none_or(|bytes| bytes > max_bytes)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn collect_ordered_rows<T: Ord>(rows: impl IntoIterator<Item = Option<T>>) -> (BTreeSet<T>, bool) {
    // BTreeSet has no fallible reserve API. The Arrow batch bounds its node
    // count, values move directly into the set (no Vec -> BTreeSet copy), and
    // every explicit String/output Vec allocation remains fallible.
    let mut values = BTreeSet::new();
    let mut contains_null = false;
    for row in rows {
        #[cfg(test)]
        COLLECTED_ROWS.with(|count| count.set(count.get() + 1));
        match row {
            Some(value) => {
                values.insert(value);
            }
            None => contains_null = true,
        }
    }
    (values, contains_null)
}

#[cfg(test)]
thread_local! {
    static COLLECTED_ROWS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_collected_rows_for_test() {
    COLLECTED_ROWS.with(|count| count.set(0));
}

#[cfg(test)]
fn collected_rows_for_test() -> usize {
    COLLECTED_ROWS.with(std::cell::Cell::get)
}

fn encode_ordered_set<T: Ord>(
    mut remaining: BTreeSet<T>,
    contains_null: bool,
    increment: impl Fn(&T) -> Option<usize>,
    mut build: impl FnMut(BTreeSet<T>) -> Result<MembershipValues, MembershipEncodingError>,
    max_bytes: usize,
) -> Result<MembershipEncodingOutcome, MembershipEncodingError> {
    let mut deltas = Vec::new();
    let mut values = BTreeSet::new();
    let empty_len = ValueDomainDelta::new(build(BTreeSet::new())?, false)
        .canonical_encoded_len()
        .map_err(|error| MembershipEncodingError::InvalidArray {
            data_type: DataType::Null,
            detail: error.to_string(),
        })?;
    if empty_len > max_bytes {
        return Ok(unavailable());
    }
    if deltas.try_reserve(remaining.len().max(1)).is_err() {
        return Ok(unavailable());
    }
    let mut encoded_len = empty_len;
    let mut emit_null = contains_null;
    while let Some(value) = remaining.pop_first() {
        let Some(scalar_bytes) = increment(&value) else {
            return Ok(unavailable());
        };
        if encoded_len
            .checked_add(scalar_bytes)
            .is_none_or(|len| len > max_bytes)
        {
            if values.is_empty() {
                return Ok(unavailable());
            }
            if !push_set_delta(&mut deltas, &mut values, emit_null, &mut build)? {
                return Ok(unavailable());
            }
            emit_null = false;
            encoded_len = empty_len;
            if encoded_len
                .checked_add(scalar_bytes)
                .is_none_or(|len| len > max_bytes)
            {
                return Ok(unavailable());
            }
        }
        values.insert(value);
        encoded_len += scalar_bytes;
    }
    if !push_set_delta(&mut deltas, &mut values, emit_null, &mut build)? {
        return Ok(unavailable());
    }
    debug_assert!(deltas.iter().all(|delta| {
        delta
            .canonical_encoded_len()
            .is_ok_and(|len| len <= max_bytes)
    }));
    Ok(MembershipEncodingOutcome::Deltas(deltas))
}

fn encode_utf8_rows(
    array: &StringArray,
    max_bytes: usize,
) -> Result<MembershipEncodingOutcome, MembershipEncodingError> {
    let mut values = BTreeSet::new();
    let mut contains_null = false;
    for index in 0..array.len() {
        #[cfg(test)]
        COLLECTED_ROWS.with(|count| count.set(count.get() + 1));
        if array.is_null(index) {
            contains_null = true;
            continue;
        }
        let value = array.value(index);
        if values.contains(value) {
            continue;
        }
        let mut owned = String::new();
        if owned.try_reserve_exact(value.len()).is_err() {
            return Ok(unavailable());
        }
        owned.push_str(value);
        values.insert(owned);
    }
    encode_ordered_set(
        values,
        contains_null,
        |value| 8usize.checked_add(value.len()),
        |values| Ok(MembershipValues::utf8_set(values)),
        max_bytes,
    )
}

fn push_set_delta<T: Ord>(
    deltas: &mut Vec<ValueDomainDelta>,
    values: &mut BTreeSet<T>,
    contains_null: bool,
    build: &mut impl FnMut(BTreeSet<T>) -> Result<MembershipValues, MembershipEncodingError>,
) -> Result<bool, MembershipEncodingError> {
    if deltas.try_reserve(1).is_err() {
        return Ok(false);
    }
    let values = std::mem::take(values);
    deltas.push(ValueDomainDelta::new(build(values)?, contains_null));
    Ok(true)
}

fn unavailable() -> MembershipEncodingOutcome {
    MembershipEncodingOutcome::Unavailable(MembershipEncodingUnavailable::ResourceOrSize)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};

    use super::{
        MembershipDeltaEncoder, MembershipEncodingError, MembershipEncodingOutcome,
        MembershipEncodingUnavailable, collected_rows_for_test, reset_collected_rows_for_test,
    };
    use crate::runtime_filter::port::value_domain::MembershipValues;

    fn assert_typed(array: ArrayRef, expected: DataType) {
        let outcome = MembershipDeltaEncoder::encode(array.as_ref(), &expected, usize::MAX)
            .expect("supported Arrow array must encode");
        let MembershipEncodingOutcome::Deltas(deltas) = outcome else {
            panic!("supported small array must remain available");
        };
        assert!(!deltas.is_empty());
        assert!(deltas.iter().all(|delta| delta.data_type() == expected));
        assert!(
            deltas
                .iter()
                .all(|delta| delta.canonical_encoded_len().is_ok())
        );
    }

    #[test]
    fn supported_arrow_types_encode_as_typed_membership_deltas() {
        assert_typed(
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            DataType::Boolean,
        );
        assert_typed(
            Arc::new(Int8Array::from(vec![Some(-1), None, Some(2)])),
            DataType::Int8,
        );
        assert_typed(Arc::new(Int16Array::from(vec![-2, 3])), DataType::Int16);
        assert_typed(Arc::new(Int32Array::from(vec![-3, 4])), DataType::Int32);
        assert_typed(Arc::new(Int64Array::from(vec![-4, 5])), DataType::Int64);
        assert_typed(
            novarocks_types::largeint::array_from_i128(&[
                Some(i128::MIN + 1),
                None,
                Some(i128::MAX),
            ])
            .unwrap(),
            DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH),
        );
        assert_typed(
            Arc::new(Float32Array::from(vec![f32::NAN, -0.0, 0.0, 1.5])),
            DataType::Float32,
        );
        assert_typed(
            Arc::new(Float64Array::from(vec![f64::NAN, -0.0, 0.0, 2.5])),
            DataType::Float64,
        );
        assert_typed(
            Arc::new(StringArray::from(vec![Some("alpha"), None, Some("beta")])),
            DataType::Utf8,
        );
        assert_typed(Arc::new(Date32Array::from(vec![1, 2])), DataType::Date32);

        let timezone: Arc<str> = Arc::from("Asia/Shanghai");
        let timestamp = TimestampMicrosecondArray::from(vec![Some(10), None, Some(20)])
            .with_timezone(timezone.clone());
        assert_typed(
            Arc::new(timestamp),
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
        );
        assert_typed(
            Arc::new(TimestampSecondArray::from(vec![1, 2])),
            DataType::Timestamp(TimeUnit::Second, None),
        );
        assert_typed(
            Arc::new(TimestampMillisecondArray::from(vec![1, 2])),
            DataType::Timestamp(TimeUnit::Millisecond, None),
        );
        assert_typed(
            Arc::new(TimestampNanosecondArray::from(vec![1, 2])),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        );

        let decimal = Decimal128Array::from(vec![Some(-1234), None, Some(5678)])
            .with_precision_and_scale(18, 2)
            .unwrap();
        assert_typed(Arc::new(decimal), DataType::Decimal128(18, 2));
    }

    fn assert_exact_values_and_null(
        array: ArrayRef,
        data_type: DataType,
        expected: MembershipValues,
    ) {
        let deltas = MembershipDeltaEncoder::encode(array.as_ref(), &data_type, usize::MAX)
            .unwrap()
            .into_deltas()
            .unwrap();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].values(), &expected);
        assert!(deltas[0].contains_null());
        assert!(!deltas[0].values().is_empty());
    }

    #[test]
    fn every_supported_arrow_type_preserves_exact_non_empty_values_and_nulls() {
        assert_exact_values_and_null(
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
            ])),
            DataType::Boolean,
            MembershipValues::boolean([false, true]),
        );
        assert_exact_values_and_null(
            Arc::new(Int8Array::from(vec![Some(2), None, Some(-1), Some(2)])),
            DataType::Int8,
            MembershipValues::int8([-1, 2]),
        );
        assert_exact_values_and_null(
            Arc::new(Int16Array::from(vec![Some(20), None, Some(-10), Some(20)])),
            DataType::Int16,
            MembershipValues::int16([-10, 20]),
        );
        assert_exact_values_and_null(
            Arc::new(Int32Array::from(vec![
                Some(200),
                None,
                Some(-100),
                Some(200),
            ])),
            DataType::Int32,
            MembershipValues::int32([-100, 200]),
        );
        assert_exact_values_and_null(
            Arc::new(Int64Array::from(vec![
                Some(2_000),
                None,
                Some(-1_000),
                Some(2_000),
            ])),
            DataType::Int64,
            MembershipValues::int64([-1_000, 2_000]),
        );

        let large_type = DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH);
        assert_exact_values_and_null(
            novarocks_types::largeint::array_from_i128(&[
                Some(i128::MAX - 9),
                None,
                Some(i128::MIN + 7),
                Some(i128::MAX - 9),
            ])
            .unwrap(),
            large_type,
            MembershipValues::large_int([i128::MIN + 7, i128::MAX - 9]),
        );
        assert_exact_values_and_null(
            Arc::new(Float32Array::from(vec![
                Some(f32::NAN),
                None,
                Some(-0.0),
                Some(1.5),
            ])),
            DataType::Float32,
            MembershipValues::float32([f32::NAN, 0.0, 1.5]),
        );
        assert_exact_values_and_null(
            Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                None,
                Some(-0.0),
                Some(2.5),
            ])),
            DataType::Float64,
            MembershipValues::float64([f64::NAN, 0.0, 2.5]),
        );
        assert_exact_values_and_null(
            Arc::new(StringArray::from(vec![
                Some("beta"),
                None,
                Some("alpha"),
                Some("beta"),
            ])),
            DataType::Utf8,
            MembershipValues::utf8(["alpha", "beta"]),
        );
        assert_exact_values_and_null(
            Arc::new(Date32Array::from(vec![Some(20), None, Some(-10), Some(20)])),
            DataType::Date32,
            MembershipValues::date32([-10, 20]),
        );

        let timezone: Arc<str> = Arc::from("Asia/Shanghai");
        assert_exact_values_and_null(
            Arc::new(
                TimestampSecondArray::from(vec![Some(20), None, Some(-10), Some(20)])
                    .with_timezone(timezone.clone()),
            ),
            DataType::Timestamp(TimeUnit::Second, Some(timezone.clone())),
            MembershipValues::timestamp(TimeUnit::Second, Some(timezone.clone()), [-10, 20]),
        );
        assert_exact_values_and_null(
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(200),
                None,
                Some(-100),
                Some(200),
            ])),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            MembershipValues::timestamp(TimeUnit::Millisecond, None, [-100, 200]),
        );
        assert_exact_values_and_null(
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(2_000), None, Some(-1_000), Some(2_000)])
                    .with_timezone(timezone.clone()),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.clone())),
            MembershipValues::timestamp(
                TimeUnit::Microsecond,
                Some(timezone.clone()),
                [-1_000, 2_000],
            ),
        );
        assert_exact_values_and_null(
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(20_000),
                None,
                Some(-10_000),
                Some(20_000),
            ])),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            MembershipValues::timestamp(TimeUnit::Nanosecond, None, [-10_000, 20_000]),
        );

        let decimal = Decimal128Array::from(vec![Some(1234), None, Some(-5678), Some(1234)])
            .with_precision_and_scale(18, 2)
            .unwrap();
        assert_exact_values_and_null(
            Arc::new(decimal),
            DataType::Decimal128(18, 2),
            MembershipValues::decimal128(18, 2, [-5678, 1234]).unwrap(),
        );
    }

    #[test]
    fn float_encoding_canonicalizes_nan_and_negative_zero() {
        let array = Float32Array::from(vec![f32::from_bits(0x7fa0_0001), f32::NAN, -0.0, 0.0]);
        let MembershipEncodingOutcome::Deltas(deltas) =
            MembershipDeltaEncoder::encode(&array, &DataType::Float32, usize::MAX).unwrap()
        else {
            panic!("small float input must encode");
        };
        assert_eq!(deltas.len(), 1);
        assert_eq!(
            deltas[0].values().float32_bits().unwrap(),
            vec![0, 0x7fc0_0000]
        );

        let noncanonical = Float64Array::from(vec![f64::from_bits(0x7ff0_0000_0000_0001), -0.0]);
        let canonical = Float64Array::from(vec![f64::NAN, 0.0]);
        assert_eq!(
            MembershipDeltaEncoder::encode(&noncanonical, &DataType::Float64, usize::MAX).unwrap(),
            MembershipDeltaEncoder::encode(&canonical, &DataType::Float64, usize::MAX).unwrap(),
        );
    }

    #[test]
    fn bounded_encoding_includes_the_complete_canonical_delta_frame() {
        let array = Int64Array::from(vec![1, 2, 3]);
        let one = MembershipDeltaEncoder::encode(
            &Int64Array::from(vec![1]),
            &DataType::Int64,
            usize::MAX,
        )
        .unwrap()
        .into_deltas()
        .unwrap();
        let exact_one = one[0].canonical_encoded_len().unwrap();
        let deltas = MembershipDeltaEncoder::encode(&array, &DataType::Int64, exact_one)
            .unwrap()
            .into_deltas()
            .unwrap();
        assert_eq!(deltas.len(), 3);
        assert!(
            deltas
                .iter()
                .all(|delta| delta.canonical_encoded_len().unwrap() <= exact_one)
        );
    }

    #[test]
    fn empty_array_still_emits_one_exact_typed_delta() {
        let array = StringArray::from(Vec::<Option<&str>>::new());
        let deltas = MembershipDeltaEncoder::encode(&array, &DataType::Utf8, usize::MAX)
            .unwrap()
            .into_deltas()
            .unwrap();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].data_type(), DataType::Utf8);
        assert!(!deltas[0].contains_null());
    }

    #[test]
    fn one_legal_scalar_larger_than_limit_is_typed_unavailable() {
        let array = StringArray::from(vec!["x".repeat(128)]);
        let outcome = MembershipDeltaEncoder::encode(&array, &DataType::Utf8, 32).unwrap();
        assert_eq!(
            outcome,
            MembershipEncodingOutcome::Unavailable(MembershipEncodingUnavailable::ResourceOrSize)
        );
    }

    #[test]
    fn tiny_budget_rejects_before_collecting_a_non_decimal_domain() {
        let array = StringArray::from(vec!["large-value"; 10_000]);
        reset_collected_rows_for_test();
        assert_eq!(
            MembershipDeltaEncoder::encode(&array, &DataType::Utf8, 1).unwrap(),
            MembershipEncodingOutcome::Unavailable(MembershipEncodingUnavailable::ResourceOrSize)
        );
        assert_eq!(collected_rows_for_test(), 0);
    }

    #[test]
    fn bounded_split_deduplicates_values_across_every_emitted_delta() {
        let one = MembershipDeltaEncoder::encode(
            &Int64Array::from(vec![1]),
            &DataType::Int64,
            usize::MAX,
        )
        .unwrap()
        .into_deltas()
        .unwrap();
        let exact_one = one[0].canonical_encoded_len().unwrap();
        let deltas = MembershipDeltaEncoder::encode(
            &Int64Array::from(vec![1, 2, 1, 2, 1]),
            &DataType::Int64,
            exact_one,
        )
        .unwrap()
        .into_deltas()
        .unwrap();
        assert_eq!(deltas.len(), 2);
        assert_eq!(
            deltas
                .iter()
                .map(|delta| delta.values().len())
                .sum::<usize>(),
            2
        );
    }

    #[test]
    fn duplicate_heavy_permutations_have_identical_bounded_output() {
        let mut left = vec![Some(2), None, Some(1)];
        left.extend(std::iter::repeat_n(Some(2), 10_000));
        let mut right = vec![Some(1), Some(2), None];
        right.extend(std::iter::repeat_n(Some(1), 10_000));
        let one = MembershipDeltaEncoder::encode(
            &Int64Array::from(vec![1]),
            &DataType::Int64,
            usize::MAX,
        )
        .unwrap()
        .into_deltas()
        .unwrap()[0]
            .canonical_encoded_len()
            .unwrap();
        assert_eq!(
            MembershipDeltaEncoder::encode(&Int64Array::from(left), &DataType::Int64, one).unwrap(),
            MembershipDeltaEncoder::encode(&Int64Array::from(right), &DataType::Int64, one)
                .unwrap(),
        );
    }

    #[test]
    fn invalid_decimal_is_structural_before_any_budget_outcome() {
        let array = Decimal128Array::from(vec![100])
            .with_precision_and_scale(2, 0)
            .unwrap();
        reset_collected_rows_for_test();
        assert!(matches!(
            MembershipDeltaEncoder::encode(&array, &DataType::Decimal128(2, 0), 1),
            Err(MembershipEncodingError::InvalidDecimal { .. })
        ));
        assert_eq!(collected_rows_for_test(), 0);

        let valid = Decimal128Array::from(vec![99])
            .with_precision_and_scale(2, 0)
            .unwrap();
        reset_collected_rows_for_test();
        assert_eq!(
            MembershipDeltaEncoder::encode(&valid, &DataType::Decimal128(2, 0), 1).unwrap(),
            MembershipEncodingOutcome::Unavailable(MembershipEncodingUnavailable::ResourceOrSize)
        );
        assert_eq!(collected_rows_for_test(), 0);
    }

    #[test]
    fn type_drift_and_unsupported_arrow_type_are_structural_errors() {
        let int64 = Int64Array::from(vec![1]);
        assert!(matches!(
            MembershipDeltaEncoder::encode(&int64, &DataType::Int32, usize::MAX),
            Err(MembershipEncodingError::TypeMismatch { .. })
        ));

        let unsigned = arrow::array::UInt64Array::from(vec![1]);
        assert!(matches!(
            MembershipDeltaEncoder::encode(&unsigned, &DataType::UInt64, usize::MAX),
            Err(MembershipEncodingError::UnsupportedType(DataType::UInt64))
        ));
    }
}
