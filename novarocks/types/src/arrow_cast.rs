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

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
    StringBuilder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Decimal256Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, make_array, new_null_array,
};
use arrow_buffer::i256;
use arrow_cast::cast;
use arrow_schema::{DataType, TimeUnit};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};

use crate::largeint;
use novarocks_functions::calendar_numeric::numeric_datetime_literal_to_naive as datetime_literal_to_naive_datetime;

#[derive(Debug)]
pub struct ScalarCastFailure(Box<ScalarCastCause>);
impl ScalarCastFailure {
    #[cold]
    fn new(cause: ScalarCastCause) -> Self {
        Self(Box::new(cause))
    }
    fn contract(cause: novarocks_failure_contract::BoundedDetail) -> Self {
        Self::new(ScalarCastCause::Contract(cause))
    }
    fn numeric(cause: novarocks_failure_contract::BoundedDetail) -> Self {
        Self::new(ScalarCastCause::Numeric(cause))
    }
    fn cast(cause: arrow_schema::ArrowError) -> Self {
        Self::new(ScalarCastCause::Cast(cause))
    }
    fn build(cause: arrow_schema::ArrowError) -> Self {
        Self::new(ScalarCastCause::Build(cause))
    }
    fn slice(cause: std::array::TryFromSliceError) -> Self {
        Self::new(ScalarCastCause::Slice(cause))
    }
    fn temporal(cause: novarocks_functions::temporal_carrier::TemporalCastFailure) -> Self {
        Self::new(ScalarCastCause::Temporal(cause))
    }
    fn text(cause: novarocks_functions::carrier_text::CarrierTextFailure) -> Self {
        Self::new(ScalarCastCause::Text(cause))
    }
    fn binary(cause: novarocks_functions::binary_text::BinaryTextFailure) -> Self {
        Self::new(ScalarCastCause::Binary(cause))
    }
    fn decimal(cause: novarocks_functions::decimal128_rescale::DecimalCastFailure) -> Self {
        Self::new(ScalarCastCause::Decimal(cause))
    }
    fn largeint_text(cause: novarocks_functions::largeint::LargeIntCarrierFailure) -> Self {
        Self::new(ScalarCastCause::LargeIntText(cause))
    }
}
#[derive(Debug)]
enum ScalarCastCause {
    Contract(novarocks_failure_contract::BoundedDetail),
    Decimal(novarocks_functions::decimal128_rescale::DecimalCastFailure),
    LargeIntText(novarocks_functions::largeint::LargeIntCarrierFailure),
    Numeric(novarocks_failure_contract::BoundedDetail),
    Cast(arrow_schema::ArrowError),
    Build(arrow_schema::ArrowError),
    Slice(std::array::TryFromSliceError),
    Temporal(novarocks_functions::temporal_carrier::TemporalCastFailure),
    Text(novarocks_functions::carrier_text::CarrierTextFailure),
    Binary(novarocks_functions::binary_text::BinaryTextFailure),
}
impl std::fmt::Display for ScalarCastFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.as_ref() {
            ScalarCastCause::Contract(detail) | ScalarCastCause::Numeric(detail) => {
                f.write_str(detail.as_str())
            }
            ScalarCastCause::Cast(cause) | ScalarCastCause::Build(cause) => cause.fmt(f),
            ScalarCastCause::Decimal(cause) => cause.fmt(f),
            ScalarCastCause::LargeIntText(cause) => cause.fmt(f),
            ScalarCastCause::Slice(cause) => cause.fmt(f),
            ScalarCastCause::Temporal(cause) => cause.fmt(f),
            ScalarCastCause::Text(cause) => cause.fmt(f),
            ScalarCastCause::Binary(cause) => cause.fmt(f),
        }
    }
}
impl std::error::Error for ScalarCastFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.0.as_ref() {
            ScalarCastCause::Cast(cause) | ScalarCastCause::Build(cause) => Some(cause),
            ScalarCastCause::Decimal(cause) => Some(cause),
            ScalarCastCause::LargeIntText(cause) => Some(cause),
            ScalarCastCause::Slice(cause) => Some(cause),
            ScalarCastCause::Temporal(cause) => Some(cause),
            ScalarCastCause::Text(cause) => Some(cause),
            ScalarCastCause::Binary(cause) => Some(cause),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ScalarCastFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use arrow_schema::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        match self.0.as_ref() {
            ScalarCastCause::Temporal(cause) => return cause.failure_fact(),
            ScalarCastCause::Decimal(cause) => return cause.failure_fact(),
            ScalarCastCause::LargeIntText(cause) => return cause.failure_fact(),
            ScalarCastCause::Text(cause) => return cause.failure_fact(),
            ScalarCastCause::Binary(cause) => return cause.failure_fact(),
            ScalarCastCause::Numeric(_)
            | ScalarCastCause::Cast(ArrowError::ArithmeticOverflow(_)) => {
                return novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact();
            }
            _ => {}
        }
        let (class, code) = match self.0.as_ref() {
            ScalarCastCause::Contract(_) => (FailureClass::Invariant, 50),
            ScalarCastCause::Slice(_) => (FailureClass::Invariant, 51),
            ScalarCastCause::Build(ArrowError::MemoryError(_))
            | ScalarCastCause::Cast(ArrowError::MemoryError(_)) => (FailureClass::Resource, 52),
            ScalarCastCause::Cast(ArrowError::CastError(_)) => (FailureClass::Data, 53),
            ScalarCastCause::Cast(ArrowError::OffsetOverflowError(_)) => {
                (FailureClass::Representation, 54)
            }
            ScalarCastCause::Cast(ArrowError::IoError(..)) => (FailureClass::Unexpected, 55),
            _ => (FailureClass::Invariant, 56),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(
                domain::TYPE_CONTRACT,
                std::num::NonZeroU16::new(code).unwrap(),
            ),
        );
        if matches!(
            self.0.as_ref(),
            ScalarCastCause::Cast(ArrowError::OffsetOverflowError(_))
        ) {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::TYPE_CONTRACT,
                    code: std::num::NonZeroU16::new(1).unwrap(),
                },
                required: None,
                extent_limit: None,
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
// Scalar casts can produce value failures and deliberately have no outer-only marker.
fn scalar_contract(detail: impl std::fmt::Display) -> ScalarCastFailure {
    ScalarCastFailure::contract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    )
}
fn numeric_failure(detail: impl std::fmt::Display) -> ScalarCastFailure {
    ScalarCastFailure::numeric(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    )
}
fn largeint_source<'a>(
    array: &'a ArrayRef,
    context: &str,
) -> Result<&'a arrow_array::FixedSizeBinaryArray, ScalarCastFailure> {
    let array = array
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .ok_or_else(|| scalar_contract(format_args!("{context}: expected FixedSizeBinaryArray")))?;
    if array.value_length() != largeint::LARGEINT_BYTE_WIDTH {
        return Err(scalar_contract(format_args!(
            "{context}: expected LARGEINT width {}, got {}",
            largeint::LARGEINT_BYTE_WIDTH,
            array.value_length()
        )));
    }
    Ok(array)
}
fn largeint_value(
    array: &arrow_array::FixedSizeBinaryArray,
    row: usize,
) -> Result<i128, ScalarCastFailure> {
    let bytes: [u8; 16] = array
        .value(row)
        .try_into()
        .map_err(ScalarCastFailure::slice)?;
    Ok(i128::from_be_bytes(bytes))
}
fn build_largeint(values: &[Option<i128>]) -> Result<ArrayRef, ScalarCastFailure> {
    match largeint::array_from_i128_observed(
        values,
        &mut |_| Ok::<(), std::convert::Infallible>(()),
    ) {
        Ok(array) => Ok(array),
        Err(never) => match never {},
    }
}

const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;

pub fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(s, "%Y%m%d"))
        .ok()
}

fn parse_datetime_flexible(raw: &str) -> Option<NaiveDateTime> {
    let text = raw.trim();
    let bytes = text.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_digit() {
        return None;
    }

    let mut pos = 0usize;
    while pos < bytes.len() && (bytes[pos].is_ascii_digit() || bytes[pos] == b'T') {
        pos += 1;
    }
    let compact_digits = bytes[..pos].iter().filter(|b| b.is_ascii_digit()).count();
    let is_compact = pos == bytes.len() || bytes.get(pos) == Some(&b'.');
    let mut field_len = if is_compact {
        if compact_digits == 4 || compact_digits == 8 || compact_digits >= 14 {
            4usize
        } else {
            2usize
        }
    } else {
        4usize
    };

    let mut values = [0u32; 7];
    let mut lengths = [0usize; 7];
    let mut field_idx = 0usize;
    let mut ptr = 0usize;
    while ptr < bytes.len() && bytes[ptr].is_ascii_digit() && field_idx < 7 {
        let start = ptr;
        let mut value = 0u32;
        let scan_to_delim = !is_compact && field_idx != 6;
        while ptr < bytes.len() && bytes[ptr].is_ascii_digit() && (scan_to_delim || field_len > 0) {
            value = value
                .checked_mul(10)?
                .checked_add((bytes[ptr] - b'0') as u32)?;
            ptr += 1;
            if !scan_to_delim {
                field_len -= 1;
            }
        }
        values[field_idx] = value;
        lengths[field_idx] = ptr - start;
        field_len = 2;

        if ptr == bytes.len() {
            field_idx += 1;
            break;
        }
        if field_idx == 2 && bytes[ptr] == b'T' {
            ptr += 1;
            field_idx += 1;
            continue;
        }
        if field_idx == 5 {
            if bytes[ptr] == b'.' {
                ptr += 1;
                field_len = 6;
            } else if bytes[ptr].is_ascii_digit() {
                field_idx += 1;
                break;
            }
            field_idx += 1;
            continue;
        }
        while ptr < bytes.len()
            && (bytes[ptr].is_ascii_punctuation() || bytes[ptr].is_ascii_whitespace())
        {
            ptr += 1;
        }
        field_idx += 1;
    }

    let parsed_fields = field_idx;
    if parsed_fields < 3 {
        return None;
    }

    let mut year = values[0] as i32;
    let month = values[1];
    let day = values[2];
    let hour = values[3];
    let minute = values[4];
    let second = values[5];
    let mut microsecond = values[6];

    if lengths[6] > 0 && lengths[6] < 6 {
        microsecond = microsecond.checked_mul(10u32.pow((6 - lengths[6]) as u32))?;
    }

    if lengths[0] == 2 {
        year = if year < 70 { year + 2000 } else { year + 1900 };
    }

    if !(1..=12).contains(&month)
        || day == 0
        || hour > 23
        || minute > 59
        || second > 59
        || microsecond >= 1_000_000
    {
        return None;
    }

    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    date.and_hms_micro_opt(hour, minute, second, microsecond)
}

pub fn parse_datetime(s: &str) -> Option<NaiveDateTime> {
    // chrono's %S accepts 60 (leap second) and normalizes it to the next minute;
    // reject that to match StarRocks behavior (second=60 is invalid).
    let from_chrono = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
        .filter(|dt| dt.nanosecond() < 1_000_000_000);
    from_chrono.or_else(|| parse_datetime_flexible(s))
}

fn parse_date_value(value: &str) -> Option<NaiveDate> {
    parse_date(value)
}

fn parse_datetime_value(value: &str) -> Option<NaiveDateTime> {
    parse_datetime(value)
}

fn pow10_i256(exp: usize) -> Result<i256, ScalarCastFailure> {
    let mut out = i256::ONE;
    let ten = i256::from_i128(10);
    for _ in 0..exp {
        out = out
            .checked_mul(ten)
            .ok_or_else(|| scalar_contract("decimal overflow"))?;
    }
    Ok(out)
}

fn pow10_i128(scale: u32) -> Option<i128> {
    novarocks_functions::legacy_decimal::checked_pow10_i128(scale as usize)
}

fn decimal128_to_i64_literal(value: i128, scale: i8) -> Option<i64> {
    let integral = if scale >= 0 {
        let divisor = pow10_i128(scale as u32)?;
        value / divisor
    } else {
        let factor = pow10_i128((-scale) as u32)?;
        value.checked_mul(factor)?
    };
    i64::try_from(integral).ok()
}

fn decimal128_to_i128_literal(value: i128, scale: i8) -> Option<i128> {
    if scale >= 0 {
        let divisor = pow10_i128(scale as u32)?;
        Some(value / divisor)
    } else {
        let factor = pow10_i128((-scale) as u32)?;
        value.checked_mul(factor)
    }
}

fn decimal256_to_i128_literal(value: i256, scale: i8) -> Option<i128> {
    let integral = if scale >= 0 {
        let divisor = pow10_i256(scale as usize).ok()?;
        value.checked_div(divisor)?
    } else {
        let factor = pow10_i256((-scale) as usize).ok()?;
        value.checked_mul(factor)?
    };
    integral.to_i128()
}

fn format_decimal_with_scale(unscaled: i128, scale: i8) -> String {
    novarocks_functions::decimal_text::format_decimal_with_scale(unscaled, scale)
}

fn format_decimal256_with_scale(unscaled: i256, scale: i8) -> String {
    novarocks_functions::decimal_text::format_decimal256_with_scale(unscaled, scale)
}

fn numeric_largeint_literal_at(
    array: &ArrayRef,
    row: usize,
) -> Result<Option<i128>, ScalarCastFailure> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| scalar_contract("failed to downcast to BooleanArray"))?;
            Ok(Some(if arr.value(row) { 1 } else { 0 }))
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Int8Array"))?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Int16Array"))?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Int32Array"))?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Int64Array"))?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::UInt8 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to UInt8Array"))?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::UInt16 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to UInt16Array"))?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to UInt32Array"))?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to UInt64Array"))?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Float32Array"))?;
            let value = arr.value(row) as f64;
            if !value.is_finite() || value < i128::MIN as f64 || value > i128::MAX as f64 {
                Ok(None)
            } else {
                Ok(Some(value.trunc() as i128))
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Float64Array"))?;
            let value = arr.value(row);
            if !value.is_finite() || value < i128::MIN as f64 || value > i128::MAX as f64 {
                Ok(None)
            } else {
                Ok(Some(value.trunc() as i128))
            }
        }
        DataType::Decimal128(_, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Decimal128Array"))?;
            Ok(decimal128_to_i128_literal(arr.value(row), *scale))
        }
        other => Err(scalar_contract(format_args!(
            "unsupported numeric LARGEINT source type: {:?}",
            other
        ))),
    }
}

fn cast_numeric_to_largeint_binary_array(array: &ArrayRef) -> Result<ArrayRef, ScalarCastFailure> {
    let mut values = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        values.push(numeric_largeint_literal_at(array, row)?);
    }
    build_largeint(&values)
}

fn is_numeric_datetime_source(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::FixedSizeBinary(16)
    )
}

fn numeric_datetime_literal_at(
    array: &ArrayRef,
    row: usize,
) -> Result<Option<i64>, ScalarCastFailure> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| scalar_contract("failed to downcast to BooleanArray"))?;
            Ok(Some(if arr.value(row) { 1 } else { 0 }))
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Int8Array"))?;
            Ok(Some(arr.value(row) as i64))
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Int16Array"))?;
            Ok(Some(arr.value(row) as i64))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Int32Array"))?;
            Ok(Some(arr.value(row) as i64))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Int64Array"))?;
            Ok(Some(arr.value(row)))
        }
        DataType::UInt8 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to UInt8Array"))?;
            Ok(Some(arr.value(row) as i64))
        }
        DataType::UInt16 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to UInt16Array"))?;
            Ok(Some(arr.value(row) as i64))
        }
        DataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to UInt32Array"))?;
            Ok(Some(arr.value(row) as i64))
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to UInt64Array"))?;
            let value = arr.value(row);
            Ok((value <= i64::MAX as u64).then_some(value as i64))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Float32Array"))?;
            let value = arr.value(row) as f64;
            if !value.is_finite() || value < i64::MIN as f64 || value > i64::MAX as f64 {
                Ok(None)
            } else {
                Ok(Some(value.trunc() as i64))
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Float64Array"))?;
            let value = arr.value(row);
            if !value.is_finite() || value < i64::MIN as f64 || value > i64::MAX as f64 {
                Ok(None)
            } else {
                Ok(Some(value.trunc() as i64))
            }
        }
        DataType::Decimal128(_, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Decimal128Array"))?;
            Ok(decimal128_to_i64_literal(arr.value(row), *scale))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = largeint_source(array, "cast LARGEINT to DATETIME source")?;
            let value = largeint_value(arr, row)?;
            Ok(i64::try_from(value).ok())
        }
        other => Err(scalar_contract(format_args!(
            "unsupported numeric datetime source type: {:?}",
            other
        ))),
    }
}

fn cast_numeric_to_date32_array(array: &ArrayRef) -> Result<ArrayRef, ScalarCastFailure> {
    let mut builder = Date32Builder::new();
    for row in 0..array.len() {
        let value = numeric_datetime_literal_at(array, row)?;
        let days = value
            .and_then(datetime_literal_to_naive_datetime)
            .map(|dt| dt.date().num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
        match days {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_numeric_to_timestamp_array(
    array: &ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, ScalarCastFailure> {
    // For nanosecond targets, build directly from nanoseconds to avoid losing sub-microsecond
    // precision when the numeric literal encodes fractional seconds.
    if matches!(target_type, DataType::Timestamp(TimeUnit::Nanosecond, _)) {
        let mut nanos = Vec::with_capacity(array.len());
        for row in 0..array.len() {
            let value = numeric_datetime_literal_at(array, row)?;
            let nanos_value = value
                .and_then(datetime_literal_to_naive_datetime)
                .map(|dt| {
                    dt.and_utc().timestamp_nanos_opt().ok_or_else(|| {
                        numeric_failure(
                            "CAST failed: numeric datetime literal is out of nanosecond i64 range",
                        )
                    })
                })
                .transpose()?;
            nanos.push(nanos_value);
        }
        return Ok(Arc::new(TimestampNanosecondArray::from(nanos)) as ArrayRef);
    }

    let mut micros = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        let value = numeric_datetime_literal_at(array, row)?;
        let micros_value = value
            .and_then(datetime_literal_to_naive_datetime)
            .map(|dt| dt.and_utc().timestamp_micros());
        micros.push(micros_value);
    }
    let micro_array = Arc::new(TimestampMicrosecondArray::from(micros)) as ArrayRef;
    if micro_array.data_type() == target_type {
        return Ok(micro_array);
    }
    cast(micro_array.as_ref(), target_type).map_err(ScalarCastFailure::cast)
}

fn parse_string_to_naive_datetime(raw: &str) -> Option<NaiveDateTime> {
    parse_datetime_value(raw)
        .or_else(|| parse_date_value(raw).and_then(|d: NaiveDate| d.and_hms_opt(0, 0, 0)))
}

fn cast_utf8_to_date32_array(arr: &StringArray) -> ArrayRef {
    let mut builder = Date32Builder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let days = parse_string_to_naive_datetime(arr.value(row))
            .map(|dt| dt.date().num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
        match days {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn cast_utf8_to_timestamp_array(
    arr: &StringArray,
    target_type: &DataType,
) -> Result<ArrayRef, ScalarCastFailure> {
    // For nanosecond targets, parse directly to nanoseconds to preserve sub-microsecond
    // precision. Going through a microsecond intermediate would silently truncate
    // the last 3 significant digits (e.g. '...05.000000001' → '...05.000000').
    if matches!(target_type, DataType::Timestamp(TimeUnit::Nanosecond, _)) {
        let nanos_vec: Vec<Option<i64>> = (0..arr.len())
            .map(|row| {
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    let dt = parse_string_to_naive_datetime(arr.value(row));
                    match dt {
                        None => Ok(None),
                        Some(dt) => dt
                            .and_utc()
                            .timestamp_nanos_opt()
                            .ok_or_else(|| {
                                numeric_failure(format_args!(
                                    "CAST failed: timestamp value '{}' is out of nanosecond range",
                                    arr.value(row)
                                ))
                            })
                            .map(Some),
                    }
                }
            })
            .collect::<Result<_, ScalarCastFailure>>()?;
        return Ok(Arc::new(TimestampNanosecondArray::from(nanos_vec)) as ArrayRef);
    }

    let micros = (0..arr.len())
        .map(|row| {
            if arr.is_null(row) {
                None
            } else {
                parse_string_to_naive_datetime(arr.value(row))
                    .map(|dt| dt.and_utc().timestamp_micros())
            }
        })
        .collect::<Vec<_>>();
    let micro_array = Arc::new(TimestampMicrosecondArray::from(micros)) as ArrayRef;
    if micro_array.data_type() == target_type {
        return Ok(micro_array);
    }
    cast(micro_array.as_ref(), target_type).map_err(|e| {
        scalar_contract(format_args!(
            "CAST failed: from {:?} to {:?}: {}",
            arr.data_type(),
            target_type,
            e
        ))
    })
}

fn parse_varchar_to_boolean_starrocks(value: &str) -> Option<bool> {
    // StarRocks BE first parses VARCHAR as int32; when that succeeds, non-zero is true.
    // If integer parsing fails, it falls back to strict boolean text parsing.
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(v) = trimmed.parse::<i32>() {
        return Some(v != 0);
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Some(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Some(false);
    }
    None
}

fn cast_utf8_to_boolean_array(arr: &StringArray) -> ArrayRef {
    let mut builder = BooleanBuilder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        match parse_varchar_to_boolean_starrocks(arr.value(i)) {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn format_float64_for_varchar(value: f64) -> String {
    novarocks_functions::builtin::string_extended::format_float64_for_varchar(value)
}

fn format_float32_for_varchar(value: f32) -> String {
    novarocks_functions::builtin::string_extended::format_float32_for_varchar(value)
}

fn cast_float64_to_utf8_array(arr: &Float64Array) -> ArrayRef {
    let mut builder = StringBuilder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        builder.append_value(format_float64_for_varchar(arr.value(i)));
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn cast_float32_to_utf8_array(arr: &Float32Array) -> ArrayRef {
    let mut builder = StringBuilder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        builder.append_value(format_float32_for_varchar(arr.value(i)));
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn decimal_precision_limit(precision: u8) -> Option<i128> {
    if precision == 0 {
        return Some(1);
    }
    pow10_i128(precision as u32)
}

fn decimal_value_within_precision(value: i128, precision: u8) -> bool {
    let Some(limit) = decimal_precision_limit(precision) else {
        return false;
    };
    value > -limit && value < limit
}

fn decimal256_precision_limit(precision: u8) -> Option<i256> {
    if precision == 0 {
        return Some(i256::ONE);
    }
    pow10_i256(precision as usize).ok()
}

fn decimal256_value_within_precision(value: i256, precision: u8) -> bool {
    let Some(limit) = decimal256_precision_limit(precision) else {
        return false;
    };
    let Some(neg_limit) = limit.checked_neg() else {
        return false;
    };
    value > neg_limit && value < limit
}

fn cast_float_to_decimal_with_rounding(
    len: usize,
    value_at: impl FnMut(usize) -> Option<f64>,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    novarocks_functions::float_decimal128::evaluate_legacy(len, value_at, precision, scale)
        .map_err(ScalarCastFailure::decimal)
}

pub fn format_timestamp_for_varchar(unit: &TimeUnit, value: i64, tz: Option<&str>) -> String {
    novarocks_functions::builtin::string_extended::format_timestamp_for_varchar(unit, value, tz)
}

fn cast_timestamp_to_utf8_array(
    array: &ArrayRef,
    unit: &TimeUnit,
    tz: Option<&str>,
) -> Result<ArrayRef, ScalarCastFailure> {
    let mut builder = StringBuilder::new();
    match unit {
        TimeUnit::Second => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| scalar_contract("failed to downcast to TimestampSecondArray"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(format_timestamp_for_varchar(unit, arr.value(i), tz));
                }
            }
        }
        TimeUnit::Millisecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    scalar_contract("failed to downcast to TimestampMillisecondArray")
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(format_timestamp_for_varchar(unit, arr.value(i), tz));
                }
            }
        }
        TimeUnit::Microsecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    scalar_contract("failed to downcast to TimestampMicrosecondArray")
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(format_timestamp_for_varchar(unit, arr.value(i), tz));
                }
            }
        }
        TimeUnit::Nanosecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| scalar_contract("failed to downcast to TimestampNanosecondArray"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(format_timestamp_for_varchar(unit, arr.value(i), tz));
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn is_container_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
    )
}

/// Cast a non-container Arrow value using NovaRocks' canonical scalar rules.
///
/// Containers require execution-owned field-schema and JSON semantics.
pub fn cast_scalar_with_special_rules(
    array: &ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, ScalarCastFailure> {
    if is_container_type(array.data_type()) || is_container_type(target_type) {
        return Err(scalar_contract(format_args!(
            "scalar cast does not accept container types: {:?} -> {:?}",
            array.data_type(),
            target_type
        )));
    }
    if array.data_type() == target_type {
        return Ok(array.clone());
    }
    if target_type == &DataType::Null {
        return Ok(new_null_array(&DataType::Null, array.len()));
    }
    if array.data_type() == &DataType::Null {
        return Ok(new_null_array(target_type, array.len()));
    }

    if novarocks_functions::temporal_carrier::supports(array.data_type(), target_type) {
        return novarocks_functions::temporal_carrier::cast(array.as_ref(), target_type)
            .map_err(ScalarCastFailure::temporal);
    }

    if target_type == &DataType::Utf8
        && matches!(
            array.data_type(),
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Timestamp(_, _)
        )
    {
        let mut builder = StringBuilder::new();
        for row in 0..array.len() {
            if array.is_null(row) {
                builder.append_null();
            } else {
                builder.append_value(
                    novarocks_functions::carrier_text::render(array.as_ref(), row)
                        .map_err(ScalarCastFailure::text)?,
                );
            }
        }
        return Ok(Arc::new(builder.finish()));
    }
    match (array.data_type(), target_type) {
        (DataType::LargeUtf8, DataType::Date32) => {
            let source = array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .ok_or_else(|| scalar_contract("DATE text CAST has a foreign carrier"))?;
            Ok(novarocks_functions::temporal_carrier::cast_large_date_text(
                source,
            ))
        }
        (DataType::Utf8, DataType::Date32) => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| scalar_contract("failed to downcast to StringArray"))?;
            Ok(cast_utf8_to_date32_array(arr))
        }
        (DataType::Utf8, DataType::Timestamp(_, _)) => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| scalar_contract("failed to downcast to StringArray"))?;
            cast_utf8_to_timestamp_array(arr, target_type)
        }
        (source, DataType::Date32) if is_numeric_datetime_source(source) => {
            cast_numeric_to_date32_array(array)
        }
        (source, DataType::Timestamp(_, _)) if is_numeric_datetime_source(source) => {
            cast_numeric_to_timestamp_array(array, target_type)
        }
        (source, DataType::FixedSizeBinary(width))
            if *width == largeint::LARGEINT_BYTE_WIDTH && is_numeric_datetime_source(source) =>
        {
            cast_numeric_to_largeint_binary_array(array)
        }
        (DataType::Utf8, DataType::FixedSizeBinary(width))
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_utf8_to_largeint_binary(array)
        }
        (DataType::Utf8, DataType::Boolean) => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| scalar_contract("failed to downcast to StringArray"))?;
            Ok(cast_utf8_to_boolean_array(arr))
        }
        (DataType::Float64, DataType::Decimal128(precision, scale)) => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Float64Array"))?;
            cast_float_to_decimal_with_rounding(
                arr.len(),
                |row| (!arr.is_null(row)).then(|| arr.value(row)),
                *precision,
                *scale,
            )
        }
        (DataType::Float32, DataType::Decimal128(precision, scale)) => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Float32Array"))?;
            cast_float_to_decimal_with_rounding(
                arr.len(),
                |row| (!arr.is_null(row)).then(|| arr.value(row) as f64),
                *precision,
                *scale,
            )
        }
        (DataType::Float64, DataType::Utf8) => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Float64Array"))?;
            Ok(cast_float64_to_utf8_array(arr))
        }
        (DataType::Float32, DataType::Utf8) => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| scalar_contract("failed to downcast to Float32Array"))?;
            Ok(cast_float32_to_utf8_array(arr))
        }
        (
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            DataType::Decimal128(p, s),
        ) => cast_integral_to_decimal128_relaxed(array, *p, *s),
        (DataType::FixedSizeBinary(width), DataType::Decimal128(p, s))
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_decimal(array, *p, *s)
        }
        (DataType::FixedSizeBinary(width), DataType::Decimal256(p, s))
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_decimal256(array, *p, *s)
        }
        (DataType::Boolean, DataType::Decimal128(p, s)) => {
            cast_boolean_to_decimal128_array(array, *p, *s)
        }
        (DataType::Boolean, DataType::Decimal256(p, s)) => {
            cast_boolean_to_decimal256_array(array, *p, *s)
        }
        (DataType::Decimal128(_, s), DataType::Utf8) => cast_decimal_to_utf8_array(array, *s),
        (DataType::Decimal256(_, s), DataType::Utf8) => cast_decimal256_to_utf8_array(array, *s),
        (DataType::Decimal128(_, s), DataType::Float32) => {
            use arrow_array::cast::AsArray;
            let arr = array.as_primitive::<arrow_array::types::Decimal128Type>();
            Ok(Arc::new(
                novarocks_functions::decimal_float_cast::decimal128_array_to_f32(arr, *s),
            ))
        }
        (DataType::Decimal256(_, s), DataType::Float32) => cast_decimal256_to_float32(array, *s),
        (DataType::Decimal128(_, s), DataType::Float64) => {
            // Retain the original pinned Arrow primitive unary operation, including
            // hidden NULL backing values; only its value author is now shared.
            use arrow_array::cast::AsArray;
            let arr = array.as_primitive::<arrow_array::types::Decimal128Type>();
            Ok(Arc::new(
                novarocks_functions::decimal_float_cast::decimal128_array_to_f64(arr, *s),
            ))
        }
        (DataType::Decimal256(_, s), DataType::Float64) => cast_decimal256_to_float64(array, *s),
        (DataType::Decimal256(_, s), DataType::Boolean) => cast_decimal256_to_boolean(array, *s),
        (DataType::Decimal256(_, s), DataType::Int8) => cast_decimal256_to_int8(array, *s),
        (DataType::Decimal256(_, s), DataType::Int16) => cast_decimal256_to_int16(array, *s),
        (DataType::Decimal256(_, s), DataType::Int32) => cast_decimal256_to_int32(array, *s),
        (DataType::Decimal256(_, s), DataType::Int64) => cast_decimal256_to_int64(array, *s),
        (DataType::Decimal256(_, s), DataType::FixedSizeBinary(width))
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_decimal256_to_largeint_binary(array, *s)
        }
        (DataType::FixedSizeBinary(width), DataType::Int8)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_int8(array)
        }
        (DataType::FixedSizeBinary(width), DataType::Int16)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_int16(array)
        }
        (DataType::FixedSizeBinary(width), DataType::Int32)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_int32(array)
        }
        (DataType::FixedSizeBinary(width), DataType::Int64)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_int64(array)
        }
        (DataType::Timestamp(unit, tz), DataType::Utf8) => {
            cast_timestamp_to_utf8_array(array, unit, tz.as_deref())
        }
        (DataType::Utf8, DataType::Decimal128(_, _)) => {
            cast_utf8_to_decimal_with_empty_as_null(array, target_type)
        }
        (DataType::Decimal128(_, source_scale), DataType::Decimal128(p, s)) => {
            cast_decimal_to_decimal_relaxed(array, *source_scale, *p, *s)
        }
        (DataType::Decimal256(source_precision, source_scale), DataType::Decimal256(p, s)) => {
            if source_precision == p && source_scale == s {
                return Ok(Arc::clone(array));
            }
            cast_decimal256_to_decimal256_relaxed(array, *source_scale, *p, *s)
        }
        (DataType::FixedSizeBinary(width), DataType::Utf8)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_utf8(array)
        }
        (DataType::FixedSizeBinary(width), DataType::Boolean)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_boolean(array)
        }
        (DataType::Timestamp(source_unit, _), DataType::Timestamp(target_unit, _))
            if source_unit == target_unit =>
        {
            retag_timestamp_same_unit(array, target_type)
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            DataType::Timestamp(TimeUnit::Nanosecond, _),
        ) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    scalar_contract("failed to downcast to TimestampMicrosecondArray")
                })?;
            for row in 0..arr.len() {
                if !arr.is_null(row) {
                    arr.value(row).checked_mul(1_000).ok_or_else(|| numeric_failure(format_args!("CAST timestamp microsecond->nanosecond overflow: value {} cannot be represented as nanoseconds in i64", arr.value(row))))?;
                }
            }
            cast(array.as_ref(), target_type).map_err(ScalarCastFailure::cast)
        }
        (DataType::Binary, DataType::Utf8) => {
            novarocks_functions::binary_text::cast_array(array).map_err(ScalarCastFailure::binary)
        }
        _ => cast(array.as_ref(), target_type).map_err(ScalarCastFailure::cast),
    }
}

fn retag_timestamp_same_unit(
    array: &ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, ScalarCastFailure> {
    let data = array
        .to_data()
        .into_builder()
        .data_type(target_type.clone())
        .build()
        .map_err(ScalarCastFailure::build)?;
    Ok(make_array(data))
}

fn cast_utf8_to_decimal_with_empty_as_null(
    child_array: &ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| scalar_contract("failed to downcast to StringArray"))?;

    let mut builder = StringBuilder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let value = arr.value(i);
        if value.trim().is_empty() {
            builder.append_null();
        } else {
            builder.append_value(value);
        }
    }

    let normalized = Arc::new(builder.finish()) as ArrayRef;
    cast(normalized.as_ref(), target_type).map_err(ScalarCastFailure::cast)
}

fn cast_decimal_to_utf8_array(
    child_array: &ArrayRef,
    scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| scalar_contract("failed to downcast to Decimal128Array"))?;

    let mut builder = StringBuilder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            builder.append_null();
            continue;
        }
        builder.append_value(format_decimal_with_scale(arr.value(row), scale));
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_decimal256_to_utf8_array(
    child_array: &ArrayRef,
    scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| scalar_contract("failed to downcast to Decimal256Array"))?;

    let mut builder = StringBuilder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            builder.append_null();
            continue;
        }
        builder.append_value(format_decimal256_with_scale(arr.value(row), scale));
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_boolean_to_decimal256_array(
    child_array: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| scalar_contract("failed to downcast to BooleanArray"))?;

    let factor = if scale == 0 {
        None
    } else {
        Some(pow10_i256(scale.unsigned_abs() as usize)?)
    };

    let mut out = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.push(None);
            continue;
        }
        let mut value = if arr.value(row) {
            i256::from_i128(1)
        } else {
            i256::ZERO
        };
        if let Some(factor) = factor {
            if scale > 0 {
                value = value
                    .checked_mul(factor)
                    .ok_or_else(|| scalar_contract("decimal overflow while casting BOOLEAN"))?;
            } else {
                value = value
                    .checked_div(factor)
                    .ok_or_else(|| scalar_contract("decimal overflow while casting BOOLEAN"))?;
            }
        }
        out.push(Some(value));
    }

    let array = Decimal256Array::from(out)
        .with_precision_and_scale(precision, scale)
        .map_err(ScalarCastFailure::build)?;
    Ok(Arc::new(array))
}

fn retag_decimal_array(
    array: &Decimal128Array,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let data = array
        .to_data()
        .into_builder()
        .data_type(DataType::Decimal128(precision, scale))
        .build()
        .map_err(ScalarCastFailure::build)?;
    Ok(make_array(data))
}

fn retag_decimal256_array(
    array: &Decimal256Array,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let data = array
        .to_data()
        .into_builder()
        .data_type(DataType::Decimal256(precision, scale))
        .build()
        .map_err(ScalarCastFailure::build)?;
    Ok(make_array(data))
}

fn cast_integral_to_decimal128_relaxed(
    child_array: &ArrayRef,
    target_precision: u8,
    target_scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    novarocks_functions::integral_decimal128::relaxed_legacy(
        child_array,
        target_precision,
        target_scale,
    )
    .map_err(ScalarCastFailure::decimal)
}

fn cast_decimal_to_decimal_relaxed(
    child_array: &ArrayRef,
    source_scale: i8,
    target_precision: u8,
    target_scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| scalar_contract("failed to downcast to Decimal128Array"))?;
    let mut values = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        let mut value = arr.value(row);
        if source_scale < target_scale {
            let factor = pow10_i128((target_scale - source_scale) as u32)
                .ok_or_else(|| scalar_contract("decimal scale overflow while casting DECIMAL"))?;
            let Some(scaled) = value.checked_mul(factor) else {
                values.push(None);
                continue;
            };
            value = scaled;
        } else if source_scale > target_scale {
            let factor = pow10_i128((source_scale - target_scale) as u32)
                .ok_or_else(|| scalar_contract("decimal scale overflow while casting DECIMAL"))?;
            let quotient = value / factor;
            let remainder = value % factor;
            let needs_round = remainder.abs().saturating_mul(2) >= factor;
            value = if needs_round {
                let carry = if value < 0 { -1 } else { 1 };
                let Some(rounded) = quotient.checked_add(carry) else {
                    values.push(None);
                    continue;
                };
                rounded
            } else {
                quotient
            };
        }
        // StarRocks does not enforce decimal precision at query execution time:
        // columns like DECIMAL64(4,3) can legitimately store values like 10.000 or
        // 100.000 that exceed the declared precision. Imposing a precision check here
        // would NULL out valid stored values during comparisons and aggregate functions.
        // Overflow beyond i128 range is already guarded by the checked_mul above.
        values.push(Some(value));
    }

    // Build with max precision first, then retag to FE-declared precision/scale.
    let wide = Decimal128Array::from(values)
        .with_precision_and_scale(38, target_scale)
        .map_err(ScalarCastFailure::build)?;
    retag_decimal_array(&wide, target_precision, target_scale)
}

fn cast_decimal256_to_decimal256_relaxed(
    child_array: &ArrayRef,
    source_scale: i8,
    target_precision: u8,
    target_scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| scalar_contract("failed to downcast to Decimal256Array"))?;

    let upscale = if source_scale < target_scale {
        Some(pow10_i256((target_scale - source_scale) as usize)?)
    } else {
        None
    };
    let downscale = if source_scale > target_scale {
        Some(pow10_i256((source_scale - target_scale) as usize)?)
    } else {
        None
    };

    let mut values = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }

        let mut value = arr.value(row);
        if let Some(factor) = upscale {
            let Some(scaled) = value.checked_mul(factor) else {
                values.push(None);
                continue;
            };
            value = scaled;
        } else if let Some(factor) = downscale {
            let quotient = value / factor;
            let remainder = value % factor;
            let remainder_abs = if remainder < i256::ZERO {
                let Some(abs) = remainder.checked_neg() else {
                    values.push(None);
                    continue;
                };
                abs
            } else {
                remainder
            };
            let doubled = match remainder_abs.checked_mul(i256::from_i128(2)) {
                Some(v) => v,
                None => {
                    values.push(None);
                    continue;
                }
            };
            let needs_round = doubled >= factor;
            value = if needs_round {
                let carry = if value < i256::ZERO {
                    i256::from_i128(-1)
                } else {
                    i256::from_i128(1)
                };
                let Some(rounded) = quotient.checked_add(carry) else {
                    values.push(None);
                    continue;
                };
                rounded
            } else {
                quotient
            };
        }
        // For upscale casts (source_scale < target_scale) we multiplied the value and
        // need to enforce precision so that values which would overflow the target type
        // become NULL.  For same-scale and downscale casts the value is unchanged or
        // smaller, so we skip pipeline-level precision enforcement and let the write-path
        // filter (filter_decimal_cast_overflow_rows) detect and drop overflow rows.
        // This matches StarRocks behaviour where the pipeline CAST is a pass-through and
        // the BE write path owns the overflow-rejection decision.
        let enforce_precision = source_scale < target_scale;
        if enforce_precision && !decimal256_value_within_precision(value, target_precision) {
            values.push(None);
            continue;
        }
        values.push(Some(value));
    }

    let wide = Decimal256Array::from(values);
    retag_decimal256_array(&wide, target_precision, target_scale)
}

fn cast_largeint_binary_to_decimal(
    child_array: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = largeint_source(child_array, "cast LARGEINT to DECIMAL")?;
    let mut values = Vec::with_capacity(arr.len());
    let multiplier = if scale >= 0 {
        Some(
            10_i128
                .checked_pow(scale as u32)
                .ok_or_else(|| scalar_contract(format_args!("decimal scale overflow: {scale}")))?,
        )
    } else {
        None
    };
    let divisor = if scale < 0 {
        Some(
            10_i128
                .checked_pow((-scale) as u32)
                .ok_or_else(|| scalar_contract(format_args!("decimal scale overflow: {scale}")))?,
        )
    } else {
        None
    };

    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        let mut value = largeint_value(arr, row)?;
        if let Some(m) = multiplier {
            let Some(scaled) = value.checked_mul(m) else {
                values.push(None);
                continue;
            };
            value = scaled;
        } else if let Some(d) = divisor {
            value /= d;
        }
        let enforce_precision = precision <= 18;
        if enforce_precision && !decimal_value_within_precision(value, precision) {
            values.push(None);
            continue;
        }
        values.push(Some(value));
    }

    let out = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(ScalarCastFailure::build)?;
    Ok(Arc::new(out) as ArrayRef)
}

fn cast_largeint_binary_to_decimal256(
    child_array: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = largeint_source(child_array, "cast LARGEINT to DECIMAL256")?;
    let mut values = Vec::with_capacity(arr.len());
    let multiplier = if scale >= 0 {
        Some(pow10_i256(scale as usize)?)
    } else {
        None
    };
    let divisor = if scale < 0 {
        Some(pow10_i256((-scale) as usize)?)
    } else {
        None
    };

    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        let mut value = i256::from_i128(largeint_value(arr, row)?);
        if let Some(m) = multiplier {
            let Some(scaled) = value.checked_mul(m) else {
                values.push(None);
                continue;
            };
            value = scaled;
        } else if let Some(d) = divisor {
            let Some(scaled) = value.checked_div(d) else {
                values.push(None);
                continue;
            };
            value = scaled;
        }
        if !decimal256_value_within_precision(value, precision) {
            values.push(None);
            continue;
        }
        values.push(Some(value));
    }

    let out = Decimal256Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(ScalarCastFailure::build)?;
    Ok(Arc::new(out) as ArrayRef)
}

fn decimal256_integral_values(arr: &Decimal256Array, source_scale: i8) -> Vec<Option<i128>> {
    let mut values = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        values.push(decimal256_to_i128_literal(arr.value(row), source_scale));
    }
    values
}

fn decimal256_to_f64(value: i256, scale: i8) -> f64 {
    novarocks_functions::decimal_float_cast::decimal256_to_f64(value, scale)
}

fn cast_decimal256_to_float64(
    child_array: &ArrayRef,
    scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| scalar_contract("failed to downcast to Decimal256Array for float64 cast"))?;
    let mut values: Vec<Option<f64>> = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
        } else {
            values.push(Some(decimal256_to_f64(arr.value(row), scale)));
        }
    }
    Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
}

fn cast_decimal256_to_float32(
    child_array: &ArrayRef,
    scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| scalar_contract("failed to downcast to Decimal256Array for float32 cast"))?;
    let mut values: Vec<Option<f32>> = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
        } else {
            // Convert to f64 first for precision, then narrow to f32.
            // f64->f32 narrowing preserves +inf/-inf for out-of-range values.
            values.push(Some(
                novarocks_functions::decimal_float_cast::decimal256_to_f32(arr.value(row), scale),
            ));
        }
    }
    Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
}

fn cast_decimal256_to_boolean(
    child_array: &ArrayRef,
    _source_scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| scalar_contract("failed to downcast to Decimal256Array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.push(None);
            continue;
        }
        out.push(Some(arr.value(row) != i256::ZERO));
    }
    Ok(Arc::new(BooleanArray::from(out)) as ArrayRef)
}

fn cast_decimal256_to_int8(
    child_array: &ArrayRef,
    source_scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| scalar_contract("failed to downcast to Decimal256Array"))?;
    let out = decimal256_integral_values(arr, source_scale)
        .into_iter()
        .map(|v| v.and_then(|n| i8::try_from(n).ok()))
        .collect::<Vec<_>>();
    Ok(Arc::new(Int8Array::from(out)) as ArrayRef)
}

fn cast_decimal256_to_int16(
    child_array: &ArrayRef,
    source_scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| scalar_contract("failed to downcast to Decimal256Array"))?;
    let out = decimal256_integral_values(arr, source_scale)
        .into_iter()
        .map(|v| v.and_then(|n| i16::try_from(n).ok()))
        .collect::<Vec<_>>();
    Ok(Arc::new(Int16Array::from(out)) as ArrayRef)
}

fn cast_decimal256_to_int32(
    child_array: &ArrayRef,
    source_scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| scalar_contract("failed to downcast to Decimal256Array"))?;
    let out = decimal256_integral_values(arr, source_scale)
        .into_iter()
        .map(|v| v.and_then(|n| i32::try_from(n).ok()))
        .collect::<Vec<_>>();
    Ok(Arc::new(Int32Array::from(out)) as ArrayRef)
}

fn cast_decimal256_to_int64(
    child_array: &ArrayRef,
    source_scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| scalar_contract("failed to downcast to Decimal256Array"))?;
    let out = decimal256_integral_values(arr, source_scale)
        .into_iter()
        .map(|v| v.and_then(|n| i64::try_from(n).ok()))
        .collect::<Vec<_>>();
    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}

fn cast_decimal256_to_largeint_binary(
    child_array: &ArrayRef,
    source_scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| scalar_contract("failed to downcast to Decimal256Array"))?;
    let values = decimal256_integral_values(arr, source_scale);
    build_largeint(&values)
}

fn cast_boolean_to_decimal128_array(
    child_array: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| scalar_contract("failed to downcast to BooleanArray"))?;
    let factor = if scale == 0 {
        None
    } else {
        Some(pow10_i128(scale.unsigned_abs() as u32).ok_or_else(|| {
            scalar_contract(format_args!(
                "decimal scale overflow while casting BOOLEAN: {scale}"
            ))
        })?)
    };
    let mut out = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.push(None);
            continue;
        }
        let mut value = if arr.value(row) { 1_i128 } else { 0_i128 };
        if let Some(factor) = factor {
            if scale > 0 {
                let Some(scaled) = value.checked_mul(factor) else {
                    out.push(None);
                    continue;
                };
                value = scaled;
            } else {
                value /= factor;
            }
        }
        if !decimal_value_within_precision(value, precision) {
            out.push(None);
            continue;
        }
        out.push(Some(value));
    }
    let array = Decimal128Array::from(out)
        .with_precision_and_scale(precision, scale)
        .map_err(ScalarCastFailure::build)?;
    Ok(Arc::new(array))
}

fn cast_largeint_binary_to_int8(child_array: &ArrayRef) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = largeint_source(child_array, "cast LARGEINT to TINYINT")?;
    let mut out = Int8Builder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        let value = largeint_value(arr, row)?;
        if value < i8::MIN as i128 || value > i8::MAX as i128 {
            out.append_null();
            continue;
        }
        out.append_value(value as i8);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn cast_largeint_binary_to_int16(child_array: &ArrayRef) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = largeint_source(child_array, "cast LARGEINT to SMALLINT")?;
    let mut out = Int16Builder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        let value = largeint_value(arr, row)?;
        if value < i16::MIN as i128 || value > i16::MAX as i128 {
            out.append_null();
            continue;
        }
        out.append_value(value as i16);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn cast_largeint_binary_to_int32(child_array: &ArrayRef) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = largeint_source(child_array, "cast LARGEINT to INT")?;
    let mut out = Int32Builder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        let value = largeint_value(arr, row)?;
        if value < i32::MIN as i128 || value > i32::MAX as i128 {
            out.append_null();
            continue;
        }
        out.append_value(value as i32);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn cast_largeint_binary_to_int64(child_array: &ArrayRef) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = largeint_source(child_array, "cast LARGEINT to BIGINT")?;
    let mut out = Int64Builder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        let value = largeint_value(arr, row)?;
        if value < i64::MIN as i128 || value > i64::MAX as i128 {
            out.append_null();
            continue;
        }
        out.append_value(value as i64);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn cast_largeint_binary_to_boolean(child_array: &ArrayRef) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = largeint_source(child_array, "cast LARGEINT to BOOLEAN")?;
    let mut out = BooleanBuilder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        out.append_value(largeint_value(arr, row)? != 0);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn parse_utf8_to_largeint(value: &str) -> Option<i128> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<i128>().ok()
}

fn cast_utf8_to_largeint_binary(child_array: &ArrayRef) -> Result<ArrayRef, ScalarCastFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| scalar_contract("failed to downcast to StringArray"))?;
    let mut values = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        values.push(parse_utf8_to_largeint(arr.value(row)));
    }
    build_largeint(&values)
}

fn cast_largeint_binary_to_utf8(child_array: &ArrayRef) -> Result<ArrayRef, ScalarCastFailure> {
    novarocks_functions::largeint_text::cast_array(child_array)
        .map_err(ScalarCastFailure::largeint_text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::FixedSizeBinaryArray;

    #[test]
    fn scalar_numeric_and_frozen_carrier_failures_keep_their_owner_facts() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let input: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![i64::MAX]));
        let weak = Arc::downgrade(&input);
        let failure = cast_scalar_with_special_rules(
            &input,
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap_err();
        assert_eq!(
            failure.failure_fact(),
            novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
        );
        drop(input);
        assert!(weak.upgrade().is_none());
        let field = Arc::new(arrow_schema::Field::new("child", DataType::Int32, true));
        let field_weak = Arc::downgrade(&field);
        let target = DataType::List(field);
        let input: ArrayRef = Arc::new(StringArray::from(vec!["[1]"]));
        let cause = cast_scalar_with_special_rules(&input, &target).unwrap_err();
        drop(target);
        assert!(field_weak.upgrade().is_none());
        assert_eq!(cause.failure_fact().class(), FailureClass::Invariant);
    }

    #[test]
    fn scalar_largeint_parser_trims_and_nulls_invalid_values() {
        let input = Arc::new(StringArray::from(vec![
            Some("42"),
            Some("  -17 "),
            Some("not-a-number"),
            Some(""),
            None,
        ])) as ArrayRef;

        let output = cast_scalar_with_special_rules(
            &input,
            &DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH),
        )
        .expect("cast UTF8 to LARGEINT");
        let output = output
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("LARGEINT array");

        assert_eq!(largeint::value_at(output, 0).unwrap(), 42);
        assert_eq!(largeint::value_at(output, 1).unwrap(), -17);
        assert!(output.is_null(2));
        assert!(output.is_null(3));
        assert!(output.is_null(4));
    }

    #[test]
    fn scalar_decimal256_to_largeint_truncates_and_nulls_overflow() {
        let huge = i256::from_string("123456789012345678901234567890123456789012").unwrap();
        let input = Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(12_345)),
                Some(i256::from_i128(-5)),
                Some(huge),
                None,
            ])
            .with_precision_and_scale(50, 2)
            .unwrap(),
        ) as ArrayRef;

        let output = cast_scalar_with_special_rules(
            &input,
            &DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH),
        )
        .expect("cast DECIMAL256 to LARGEINT");
        let output = output
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("LARGEINT array");

        assert_eq!(largeint::value_at(output, 0).unwrap(), 123);
        assert_eq!(largeint::value_at(output, 1).unwrap(), 0);
        assert!(output.is_null(2));
        assert!(output.is_null(3));
    }

    #[test]
    fn scalar_largeint_to_int64_nulls_overflow() {
        let input = largeint::array_from_i128(&[
            Some(i64::MAX as i128),
            Some(i64::MIN as i128),
            Some(i64::MAX as i128 + 1),
            Some(i64::MIN as i128 - 1),
            None,
        ])
        .expect("LARGEINT input");

        let output = cast_scalar_with_special_rules(&input, &DataType::Int64)
            .expect("cast LARGEINT to BIGINT");
        let output = output
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("BIGINT array");

        assert_eq!(output.value(0), i64::MAX);
        assert_eq!(output.value(1), i64::MIN);
        assert!(output.is_null(2));
        assert!(output.is_null(3));
        assert!(output.is_null(4));
    }

    #[test]
    fn scalar_api_rejects_execution_owned_containers() {
        let input = Arc::new(StringArray::from(vec![Some("[1]")])) as ArrayRef;
        let error = cast_scalar_with_special_rules(
            &input,
            &DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Int64,
                true,
            ))),
        )
        .expect_err("container cast must stay execution owned");
        assert!(
            error
                .to_string()
                .contains("does not accept container types")
        );
    }
}
