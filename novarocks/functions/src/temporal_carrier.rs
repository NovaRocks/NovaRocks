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

//! Frozen v1 Arrow temporal casts shared by full-array and selected calls.
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, TimeUnit};

/// Admit only these exact carrier conversions; timezone policy stays explicit.
pub fn supports(source: &DataType, target: &DataType) -> bool {
    matches!(
        (source, target),
        (
            DataType::Date32,
            DataType::Utf8 | DataType::Timestamp(_, None)
        ) | (DataType::Timestamp(_, None), DataType::Date32)
    )
}

/// Date32 multiplication and Timestamp calendar conversion retain their original
/// active-value authors. SDK formatting remains a separate operation.
pub fn cast(array: &dyn Array, target: &DataType) -> Result<ArrayRef, TemporalCastFailure> {
    if !supports(array.data_type(), target) {
        return Err(TemporalCastFailure::Contract(
            novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
                "unsupported exact temporal cast from {:?} to {target:?}",
                array.data_type()
            )),
        ));
    }
    if let (DataType::Date32, DataType::Timestamp(unit, None)) = (array.data_type(), target) {
        let dates = array
            .as_any()
            .downcast_ref::<arrow_array::Date32Array>()
            .ok_or_else(|| {
                TemporalCastFailure::Contract(
                    novarocks_failure_contract::BoundedDetail::truncating_display(
                        "Date32 temporal cast has a foreign carrier",
                    ),
                )
            })?;
        return match unit {
            TimeUnit::Second => {
                dates_to_timestamp::<arrow_array::types::TimestampSecondType>(dates, *unit, 86_400)
            }
            TimeUnit::Millisecond => dates_to_timestamp::<
                arrow_array::types::TimestampMillisecondType,
            >(dates, *unit, 86_400_000),
            TimeUnit::Microsecond => dates_to_timestamp::<
                arrow_array::types::TimestampMicrosecondType,
            >(dates, *unit, 86_400_000_000),
            TimeUnit::Nanosecond => {
                dates_to_timestamp::<arrow_array::types::TimestampNanosecondType>(
                    dates,
                    *unit,
                    86_400_000_000_000,
                )
            }
        };
    }
    if let (DataType::Timestamp(unit, None), DataType::Date32) = (array.data_type(), target) {
        return match unit {
            TimeUnit::Second => {
                timestamps_to_date32::<arrow_array::types::TimestampSecondType>(array)
            }
            TimeUnit::Millisecond => {
                timestamps_to_date32::<arrow_array::types::TimestampMillisecondType>(array)
            }
            TimeUnit::Microsecond => {
                timestamps_to_date32::<arrow_array::types::TimestampMicrosecondType>(array)
            }
            TimeUnit::Nanosecond => {
                timestamps_to_date32::<arrow_array::types::TimestampNanosecondType>(array)
            }
        };
    }
    arrow_cast::cast(array, target).map_err(TemporalCastFailure::Arrow)
}

/// The actual original calendar author rejected this timestamp. This is a
/// first-party value source, not a reconstructed SDK error.
#[derive(Debug)]
pub struct TimestampDateOutOfRange {
    timestamp_type: &'static str,
    value: i64,
}
impl std::fmt::Display for TimestampDateOutOfRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cast error: Cannot convert {} {} to datetime",
            self.timestamp_type, self.value
        )
    }
}
impl std::error::Error for TimestampDateOutOfRange {}

fn timestamps_to_date32<T: arrow_array::types::ArrowTimestampType>(
    array: &dyn Array,
) -> Result<ArrayRef, TemporalCastFailure> {
    let array = array
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<T>>()
        .ok_or_else(|| {
            TemporalCastFailure::Contract(
                novarocks_failure_contract::BoundedDetail::truncating_display(
                    "Timestamp temporal cast has a foreign carrier",
                ),
            )
        })?;
    // try_unary visits only valid values, retains the original first failure,
    // and shares the original validity instead of interpreting hidden payloads.
    let output = array
        .try_unary::<_, arrow_array::types::Date32Type, _>(|value| {
            arrow_array::temporal_conversions::as_datetime::<T>(value)
                .map(|date| arrow_array::types::Date32Type::from_naive_date(date.date()))
                .ok_or_else(|| TimestampDateOutOfRange {
                    timestamp_type: std::any::type_name::<T>(),
                    value,
                })
        })
        .map_err(TemporalCastFailure::TimestampDate)?;
    Ok(std::sync::Arc::new(output))
}

/// Checked scaling for the already admitted Time32/Time64 unit pairs. This
/// entry does not expand the general temporal CAST profile or its timezone policy.
pub(crate) fn cast_time_units(
    array: &dyn Array,
    target: &DataType,
) -> Result<ArrayRef, TemporalCastFailure> {
    use arrow_array::types::*;
    let source_unit = match array.data_type() {
        DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => match array.data_type()
        {
            DataType::Time32(unit) | DataType::Time64(unit) => *unit,
            _ => unreachable!(),
        },
        _ => return Err(time_contract()),
    };
    let target_unit = match target {
        DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => match target {
            DataType::Time32(unit) | DataType::Time64(unit) => *unit,
            _ => unreachable!(),
        },
        _ => return Err(time_contract()),
    };
    macro_rules! source {
        ($ty:ty) => {{
            let source = array
                .as_any()
                .downcast_ref::<arrow_array::PrimitiveArray<$ty>>()
                .ok_or_else(time_contract)?;
            match target {
                DataType::Time32(TimeUnit::Second) => {
                    time_units::<$ty, Time32SecondType>(source, source_unit, target_unit, 32)
                }
                DataType::Time32(TimeUnit::Millisecond) => {
                    time_units::<$ty, Time32MillisecondType>(source, source_unit, target_unit, 32)
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    time_units::<$ty, Time64MicrosecondType>(source, source_unit, target_unit, 64)
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    time_units::<$ty, Time64NanosecondType>(source, source_unit, target_unit, 64)
                }
                _ => Err(time_contract()),
            }
        }};
    }
    match array.data_type() {
        DataType::Time32(TimeUnit::Second) => source!(Time32SecondType),
        DataType::Time32(TimeUnit::Millisecond) => source!(Time32MillisecondType),
        DataType::Time64(TimeUnit::Microsecond) => source!(Time64MicrosecondType),
        DataType::Time64(TimeUnit::Nanosecond) => source!(Time64NanosecondType),
        _ => Err(time_contract()),
    }
}
fn time_contract() -> TemporalCastFailure {
    TemporalCastFailure::Contract(
        novarocks_failure_contract::BoundedDetail::truncating_display(
            "TIME unit conversion has a foreign frozen carrier",
        ),
    )
}
fn time_units<S, T>(
    source: &arrow_array::PrimitiveArray<S>,
    source_unit: TimeUnit,
    target_unit: TimeUnit,
    width: u8,
) -> Result<ArrayRef, TemporalCastFailure>
where
    S: arrow_array::types::ArrowPrimitiveType,
    S::Native: Into<i64>,
    T: arrow_array::types::ArrowPrimitiveType,
    T::Native: TryFrom<i128, Error = std::num::TryFromIntError>,
{
    let factor = |unit| match unit {
        TimeUnit::Second => 1_i128,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    };
    let from = factor(source_unit);
    let to = factor(target_unit);
    // try_unary visits active values only and preserves the original NULL mask.
    let out = source.try_unary::<_, T, _>(|value| {
        let value = value.into();
        // i64 * 10^9 fits i128. A native-width conversion checks both scaling
        // and narrowing after division toward zero; no day-range rule applies.
        let ticks = if from >= to {
            i128::from(value) / (from / to)
        } else {
            i128::from(value) * (to / from)
        };
        T::Native::try_from(ticks).map_err(|source| TemporalCastFailure::TimeUnits {
            value,
            source_unit,
            target_unit,
            ticks,
            width,
            source,
        })
    })?;
    Ok(std::sync::Arc::new(out))
}

#[derive(Debug)]
pub enum TemporalCastFailure {
    TimeUnits {
        value: i64,
        source_unit: TimeUnit,
        target_unit: TimeUnit,
        ticks: i128,
        width: u8,
        source: std::num::TryFromIntError,
    },
    Contract(novarocks_failure_contract::BoundedDetail),
    Arrow(arrow_schema::ArrowError),
    TimestampDate(TimestampDateOutOfRange),
    Numeric {
        days: i32,
        unit: TimeUnit,
        ticks: i128,
        source: std::num::TryFromIntError,
    },
}
impl TemporalCastFailure {
    pub fn data_reason(&self) -> Option<crate::DataFailureReason> {
        match self {
            Self::TimeUnits { .. }
            | Self::Numeric { .. }
            | Self::Arrow(arrow_schema::ArrowError::ArithmeticOverflow(_)) => {
                Some(crate::DataFailureReason::NumericOutOfRange)
            }
            Self::TimestampDate(_) | Self::Arrow(arrow_schema::ArrowError::CastError(_)) => {
                Some(crate::DataFailureReason::InvalidValue)
            }
            _ => None,
        }
    }
    pub fn is_data(&self) -> bool {
        self.data_reason().is_some()
    }
}
fn dates_to_timestamp<T: arrow_array::types::ArrowPrimitiveType<Native = i64>>(
    dates: &arrow_array::Date32Array,
    unit: TimeUnit,
    units_per_day: i64,
) -> Result<ArrayRef, TemporalCastFailure> {
    let mut builder = arrow_array::builder::PrimitiveBuilder::<T>::with_capacity(dates.len());
    for value in dates.iter() {
        match value {
            None => builder.append_null(),
            Some(days) => {
                let ticks = days as i128 * units_per_day as i128;
                let value =
                    i64::try_from(ticks).map_err(|source| TemporalCastFailure::Numeric {
                        days,
                        unit,
                        ticks,
                        source,
                    })?;
                builder.append_value(value);
            }
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

impl std::fmt::Display for TemporalCastFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimeUnits {
                value,
                source_unit,
                target_unit,
                ticks,
                width,
                ..
            } => write!(
                f,
                "TIME value {value} {source_unit:?} requires {ticks} {target_unit:?} units, outside i{width}"
            ),
            Self::Contract(detail) => f.write_str(detail.as_str()),
            Self::Arrow(source) => source.fmt(f),
            Self::TimestampDate(source) => source.fmt(f),
            Self::Numeric {
                days, unit, ticks, ..
            } => write!(
                f,
                "Date32 value {days} requires {ticks} {unit:?} timestamp units, outside i64"
            ),
        }
    }
}
impl std::error::Error for TemporalCastFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Arrow(source) => Some(source),
            Self::TimestampDate(source) => Some(source),
            Self::TimeUnits { source, .. } | Self::Numeric { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for TemporalCastFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use arrow_schema::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        if let Some(reason) = self.data_reason() {
            return reason.failure_fact();
        }
        let (class, code) = match self {
            Self::Contract(_) => (FailureClass::Invariant, 207),
            Self::Arrow(ArrowError::CastError(_)) => unreachable!("value facts returned above"),
            Self::Arrow(ArrowError::MemoryError(_)) => (FailureClass::Resource, 209),
            Self::Arrow(ArrowError::OffsetOverflowError(_)) => (FailureClass::Representation, 210),
            Self::Arrow(ArrowError::IoError(..)) => (FailureClass::Unexpected, 211),
            Self::Arrow(_) => (FailureClass::Invariant, 212),
            Self::TimeUnits { .. } | Self::Numeric { .. } => {
                unreachable!("numeric temporal facts returned above")
            }
            Self::TimestampDate(_) => unreachable!("calendar value facts returned above"),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        );
        if matches!(self, Self::Arrow(ArrowError::OffsetOverflowError(_))) {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: std::num::NonZeroU16::new(34).unwrap(),
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
/// Value and outer witnesses can only be constructed by this domain's classifier.
#[derive(Debug)]
pub enum ClassifiedTemporalCastFailure {
    Value(TemporalCastValueFailure),
    Outer(TemporalCastOuterFailure),
}
#[derive(Debug)]
/// A value failure cannot be promoted through the outer-only kernel constructor.
/// ```compile_fail
/// fn invalid(cause: novarocks_functions::temporal_carrier::TemporalCastValueFailure) {
///     let _ = novarocks_functions::KernelFailure::from_domain(cause);
/// }
/// ```
pub struct TemporalCastValueFailure {
    source: TemporalCastFailure,
    reason: crate::DataFailureReason,
}
#[derive(Debug)]
pub struct TemporalCastOuterFailure(TemporalCastFailure);
impl TemporalCastFailure {
    pub fn classify(self) -> ClassifiedTemporalCastFailure {
        match self.data_reason() {
            Some(reason) => ClassifiedTemporalCastFailure::Value(TemporalCastValueFailure {
                source: self,
                reason,
            }),
            None => ClassifiedTemporalCastFailure::Outer(TemporalCastOuterFailure(self)),
        }
    }
}
impl TemporalCastValueFailure {
    pub fn reason(&self) -> crate::DataFailureReason {
        self.reason
    }
}
impl std::fmt::Display for TemporalCastValueFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for TemporalCastValueFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for TemporalCastValueFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.reason)
    }
}
impl std::fmt::Display for TemporalCastOuterFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for TemporalCastOuterFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for TemporalCastOuterFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.0)
    }
}
impl novarocks_failure_contract::OuterFailureProjection for TemporalCastOuterFailure {}

#[cfg(test)]
mod domain_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn temporal_date32_checks_both_exact_timestamp_boundaries() {
        for (unit, per_day) in [
            (TimeUnit::Microsecond, 86_400_000_000_i64),
            (TimeUnit::Nanosecond, 86_400_000_000_000_i64),
        ] {
            let low = (i64::MIN / per_day) as i32;
            let high = (i64::MAX / per_day) as i32;
            let active = arrow_array::Date32Array::from(vec![low, 0, high]);
            let output = cast(&active, &DataType::Timestamp(unit, None)).unwrap();
            assert_eq!(output.null_count(), 0);
            assert_eq!(output.len(), 3);
            for days in [low - 1, high + 1] {
                let error = cast(
                    &arrow_array::Date32Array::from(vec![days]),
                    &DataType::Timestamp(unit, None),
                )
                .unwrap_err();
                assert_eq!(
                    error.failure_fact(),
                    crate::DataFailureReason::NumericOutOfRange.failure_fact()
                );
            }
        }
    }

    #[test]
    fn temporal_date32_overflow_is_typed_and_hidden_null_payload_is_ignored() {
        use std::error::Error;
        for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            let active = arrow_array::Date32Array::from(vec![i32::MAX]);
            let error = cast(&active, &DataType::Timestamp(unit, None)).unwrap_err();
            assert_eq!(
                error.failure_fact(),
                crate::DataFailureReason::NumericOutOfRange.failure_fact()
            );
            assert!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<std::num::TryFromIntError>()
                    .is_some()
            );
            let hidden = arrow_array::Date32Array::new(
                vec![i32::MAX, 0].into(),
                Some(arrow_buffer::NullBuffer::from(vec![false, true])),
            );
            let output = cast(&hidden, &DataType::Timestamp(unit, None)).unwrap();
            assert!(output.is_null(0));
            assert!(!output.is_null(1));
            let data = output.to_data();
            assert_eq!(&data.buffers()[0].as_slice()[8..16], &0_i64.to_ne_bytes());
        }
    }

    #[test]
    fn temporal_actual_arrow_failure_preserves_source_and_not_payload_owner() {
        use std::error::Error;
        let array = std::sync::Arc::new(arrow_array::Date32Array::from(vec![i32::MAX]));
        let weak = std::sync::Arc::downgrade(&array);
        let error = cast(array.as_ref(), &DataType::Utf8).unwrap_err();
        assert!(error.is_data());
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert!(
            error
                .source()
                .unwrap()
                .downcast_ref::<arrow_schema::ArrowError>()
                .is_some()
        );
        drop(array);
        assert!(weak.upgrade().is_none());
    }
    #[test]
    fn temporal_failure_variant_preserves_outer_resource_and_numeric_facts() {
        let error = TemporalCastFailure::Arrow(arrow_schema::ArrowError::MemoryError(
            "invalid value; cancelled".into(),
        ));
        assert!(!error.is_data());
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        let numeric = TemporalCastFailure::Arrow(arrow_schema::ArrowError::ArithmeticOverflow(
            "overflow".into(),
        ));
        assert!(numeric.is_data());
        assert_eq!(
            numeric.failure_fact(),
            crate::DataFailureReason::NumericOutOfRange.failure_fact()
        );
    }
}

#[cfg(test)]
mod outer_witness_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn temporal_cast_outer_witness_preserves_actual_value_and_contract_sources() {
        let dates = arrow_array::Date32Array::from(vec![i32::MAX]);
        let cause = cast(&dates, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap_err();
        let original = cause.failure_fact();
        let ClassifiedTemporalCastFailure::Value(value) = cause.classify() else {
            panic!("expected actual numeric value failure")
        };
        assert_eq!(value.failure_fact(), original);
        assert_eq!(value.reason(), crate::DataFailureReason::NumericOutOfRange);
        use std::error::Error;
        assert!(
            value
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<std::num::TryFromIntError>()
        );
        let cause = cast(&dates, &DataType::Boolean).unwrap_err();
        let original = cause.failure_fact();
        let ClassifiedTemporalCastFailure::Outer(outer) = cause.classify() else {
            panic!("expected frozen contract failure")
        };
        let kernel = crate::KernelFailure::from_domain(outer);
        assert_eq!(kernel.failure_fact(), original);
        assert_eq!(original.class(), FailureClass::Invariant);
    }
}

/// Original LargeUtf8 DATE conversion. Its raw date grammar and Chrono range
/// remain distinct from Utf8's SQL calendar parser and later SDK extensions.
pub fn cast_large_date_text(array: &arrow_array::LargeStringArray) -> ArrayRef {
    use chrono::Datelike;
    let values = array.iter().map(|text| {
        text.and_then(parse_date_text)
            .map(|date| date.num_days_from_ce() - 719_163)
    });
    // SAFETY: mapping LargeStringArray's ArrayIter preserves its exact upper length.
    std::sync::Arc::new(unsafe { arrow_array::Date32Array::from_trusted_len_iter(values) })
}

// Original Arrow date grammar, now owned at this exact temporal boundary.
// Do not retry with trimmed input or extend signed years past Chrono's range.
fn parse_date_text(string: &str) -> Option<chrono::NaiveDate> {
    // If the date has an extended (signed) year such as "+10999-12-31" or "-0012-05-06"
    //
    // According to [ISO 8601], years have:
    //  Four digits or more for the year. Years in the range 0000 to 9999 will be pre-padded by
    //  zero to ensure four digits. Years outside that range will have a prefixed positive or negative symbol.
    //
    // [ISO 8601]: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/format/DateTimeFormatter.html#ISO_LOCAL_DATE
    if string.starts_with('+') || string.starts_with('-') {
        // Skip the sign and look for the hyphen that terminates the year digits.
        // According to ISO 8601 the unsigned part must be at least 4 digits.
        let rest = &string[1..];
        let hyphen = rest.find('-')?;
        if hyphen < 4 {
            return None;
        }
        // The year substring is the sign and the digits (but not the separator)
        // e.g. for "+10999-12-31", hyphen is 5 and s[..6] is "+10999"
        let year: i32 = string[..hyphen + 1].parse().ok()?;
        // The remainder should begin with a '-' which we strip off, leaving the month-day part.
        let remainder = string[hyphen + 1..].strip_prefix('-')?;
        let mut parts = remainder.splitn(2, '-');
        let month: u32 = parts.next()?.parse().ok()?;
        let day: u32 = parts.next()?.parse().ok()?;
        return chrono::NaiveDate::from_ymd_opt(year, month, day);
    }

    if string.len() > 10 {
        // Try to parse as datetime and return just the date part
        return arrow_cast::parse::string_to_datetime(&chrono::Utc, string)
            .map(|dt| dt.date_naive())
            .ok();
    };
    let mut digits = [0; 10];
    let mut mask = 0;

    // Treating all bytes the same way, helps LLVM vectorise this correctly
    for (idx, (o, i)) in digits.iter_mut().zip(string.bytes()).enumerate() {
        *o = i.wrapping_sub(b'0');
        mask |= ((*o < 10) as u16) << idx
    }

    const HYPHEN: u8 = b'-'.wrapping_sub(b'0');

    //  refer to https://www.rfc-editor.org/rfc/rfc3339#section-3
    if digits[4] != HYPHEN {
        let (year, month, day) = match (mask, string.len()) {
            (0b11111111, 8) => (
                digits[0] as u16 * 1000
                    + digits[1] as u16 * 100
                    + digits[2] as u16 * 10
                    + digits[3] as u16,
                digits[4] * 10 + digits[5],
                digits[6] * 10 + digits[7],
            ),
            _ => return None,
        };
        return chrono::NaiveDate::from_ymd_opt(year as _, month as _, day as _);
    }

    let (month, day) = match mask {
        0b1101101111 => {
            if digits[7] != HYPHEN {
                return None;
            }
            (digits[5] * 10 + digits[6], digits[8] * 10 + digits[9])
        }
        0b101101111 => {
            if digits[7] != HYPHEN {
                return None;
            }
            (digits[5] * 10 + digits[6], digits[8])
        }
        0b110101111 => {
            if digits[6] != HYPHEN {
                return None;
            }
            (digits[5], digits[7] * 10 + digits[8])
        }
        0b10101111 => {
            if digits[6] != HYPHEN {
                return None;
            }
            (digits[5], digits[7])
        }
        _ => return None,
    };

    let year =
        digits[0] as u16 * 1000 + digits[1] as u16 * 100 + digits[2] as u16 * 10 + digits[3] as u16;

    chrono::NaiveDate::from_ymd_opt(year as _, month as _, day as _)
}

#[cfg(test)]
mod frozen_calendar_tests {
    use super::*;
    use std::error::Error;
    #[test]
    fn timestamp_date_actual_range_failure_keeps_typed_value_source_and_first_error() {
        let source = arrow_array::TimestampMicrosecondArray::from(vec![i64::MAX, i64::MIN]);
        let error = cast(&source, &DataType::Date32).unwrap_err();
        assert_eq!(
            error.data_reason(),
            Some(crate::DataFailureReason::InvalidValue)
        );
        assert_eq!(
            error.to_string(),
            "Cast error: Cannot convert arrow_array::types::TimestampMicrosecondType 9223372036854775807 to datetime"
        );
        let cause = error
            .source()
            .unwrap()
            .downcast_ref::<TimestampDateOutOfRange>()
            .unwrap();
        assert_eq!(cause.value, i64::MAX);
        assert!(!error.source().unwrap().is::<arrow_schema::ArrowError>());
        let source = arrow_array::TimestampMicrosecondArray::new(
            vec![i64::MAX, 0].into(),
            Some(arrow_buffer::NullBuffer::from(vec![false, true])),
        );
        let output = cast(&source, &DataType::Date32).unwrap();
        assert!(output.is_null(0));
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<arrow_array::Date32Array>()
                .unwrap()
                .value(1),
            0
        );
    }
    #[test]
    fn large_date_text_keeps_raw_grammar_and_original_calendar_range() {
        let input = arrow_array::LargeStringArray::from(vec![
            Some("1970-1-1"),
            Some("19700102"),
            Some(" 1970-01-01 "),
            Some("+5881580-07-11"),
            Some("1970-01-01T12:00:00Z"),
            None,
        ]);
        let output = cast_large_date_text(&input);
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<arrow_array::Date32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1), None, None, Some(0), None]
        );
    }
}
