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
//! The original recursive aggregate text formatter, including legacy quirks.
use crate::KernelFailure;
use crate::aggregate_scalar::{AggScalarValue, ScalarWork};
use arrow_buffer::i256;
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, NaiveDate};
use std::fmt;

/// The original aggregate batch boundary's diagnostic stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateFailureStage {
    Input,
    CreateState,
    Update,
    Merge,
    BuildIntermediate,
    BuildFinal,
}

impl AggregateFailureStage {
    pub fn message<T: fmt::Display + ?Sized>(self, message: &T) -> AggregateFailureMessage<'_, T> {
        AggregateFailureMessage {
            stage: self,
            message,
        }
    }
}

/// Borrow the original diagnostic without allocating or changing its text.
pub struct AggregateFailureMessage<'a, T: fmt::Display + ?Sized> {
    stage: AggregateFailureStage,
    message: &'a T,
}

impl<T: fmt::Display + ?Sized> fmt::Display for AggregateFailureMessage<'_, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.stage {
            AggregateFailureStage::Input => "aggregate input: ",
            AggregateFailureStage::CreateState => "create aggregate state: ",
            AggregateFailureStage::Update => "update aggregate state: ",
            AggregateFailureStage::Merge => "merge aggregate state: ",
            AggregateFailureStage::BuildIntermediate => "build aggregate intermediate output: ",
            AggregateFailureStage::BuildFinal => "build aggregate final output: ",
        })?;
        write!(formatter, "{}", self.message)
    }
}

fn date32_to_naive(days: i32) -> Option<NaiveDate> {
    719163_i32
        .checked_add(days)
        .and_then(NaiveDate::from_num_days_from_ce_opt)
}
pub fn scalar_to_string(
    value: &AggScalarValue,
    data_type: &DataType,
    work: &mut ScalarWork<'_, '_>,
) -> Result<String, KernelFailure> {
    work.step()?;
    work.flush()?;
    let result = scalar_to_string_inner(value, data_type, work);
    work.flush()?;
    result
}
fn scalar_to_string_inner(
    value: &AggScalarValue,
    data_type: &DataType,
    work: &mut ScalarWork<'_, '_>,
) -> Result<String, KernelFailure> {
    match value {
        AggScalarValue::Bool(v) => Ok(if *v { "1".to_string() } else { "0".to_string() }),
        AggScalarValue::Int64(v) => Ok(v.to_string()),
        AggScalarValue::Float64(v) => Ok(v.to_string()),
        AggScalarValue::Utf8(v) => Ok(v.clone()),
        AggScalarValue::Date32(v) => {
            let date = date32_to_naive(*v)
                .ok_or_else(|| KernelFailure::from_domain(FormatRange::Date32(*v)))?;
            Ok(date.format("%Y-%m-%d").to_string())
        }
        AggScalarValue::Timestamp(v) => match data_type {
            DataType::Timestamp(unit, tz) => format_timestamp(*unit, *v, tz.as_deref()),
            _ => Ok(v.to_string()),
        },
        AggScalarValue::Temporal(..) => Err(crate::aggregate_scalar::temporal_scalar_contract()),
        AggScalarValue::Decimal128(v) => match data_type {
            DataType::Decimal128(_, scale) => Ok(format_decimal(*v, *scale)),
            _ => Ok(v.to_string()),
        },
        AggScalarValue::Decimal256(v) => match data_type {
            DataType::Decimal256(_, scale) => Ok(format_decimal256(*v, *scale)),
            _ => Ok(v.to_string()),
        },
        AggScalarValue::Binary(v) => Ok(hex::encode(v)),
        AggScalarValue::Struct(items) => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                work.step()?;
                match item {
                    Some(v) => rendered.push(scalar_to_string_inner(v, data_type, work)?),
                    None => rendered.push("NULL".to_string()),
                }
            }
            Ok(format!("{{{}}}", rendered.join(",")))
        }
        AggScalarValue::Map(items) => {
            let mut rendered = Vec::with_capacity(items.len());
            for (k, v) in items {
                work.step()?;
                let key = match k {
                    Some(k) => scalar_to_string_inner(k, data_type, work)?,
                    None => "NULL".to_string(),
                };
                let value = match v {
                    Some(v) => scalar_to_string_inner(v, data_type, work)?,
                    None => "NULL".to_string(),
                };
                rendered.push(format!("{}:{}", key, value));
            }
            Ok(format!("{{{}}}", rendered.join(",")))
        }
        AggScalarValue::List(items) => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                work.step()?;
                match item {
                    Some(v) => rendered.push(scalar_to_string_inner(v, data_type, work)?),
                    None => rendered.push("NULL".to_string()),
                }
            }
            Ok(format!("[{}]", rendered.join(",")))
        }
    }
}

fn format_timestamp(unit: TimeUnit, value: i64, tz: Option<&str>) -> Result<String, KernelFailure> {
    // Align with StarRocks: omit fractional part when zero (e.g. "2020-01-01 00:10:00" not "2020-01-01 00:10:00.000000")
    let timestamp_str = match unit {
        TimeUnit::Second => {
            let dt = DateTime::from_timestamp(value, 0).ok_or_else(|| {
                KernelFailure::from_domain(FormatRange::Timestamp { unit, value })
            })?;
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
        }
        TimeUnit::Millisecond => {
            let seconds = value.div_euclid(1_000);
            let millis = value.rem_euclid(1_000) as u32;
            let nanos = millis * 1_000_000;
            let dt = DateTime::from_timestamp(seconds, nanos).ok_or_else(|| {
                KernelFailure::from_domain(FormatRange::Timestamp { unit, value })
            })?;
            if millis == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
            }
        }
        TimeUnit::Microsecond => {
            let seconds = value.div_euclid(1_000_000);
            let micros = value.rem_euclid(1_000_000) as u32;
            let nanos = micros * 1_000;
            let dt = DateTime::from_timestamp(seconds, nanos).ok_or_else(|| {
                KernelFailure::from_domain(FormatRange::Timestamp { unit, value })
            })?;
            if micros == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()
            }
        }
        TimeUnit::Nanosecond => {
            let seconds = value.div_euclid(1_000_000_000);
            let nanos = value.rem_euclid(1_000_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos).ok_or_else(|| {
                KernelFailure::from_domain(FormatRange::Timestamp { unit, value })
            })?;
            if nanos == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string()
            }
        }
    };
    Ok(if let Some(tz) = tz {
        format!("{} {}", timestamp_str, tz)
    } else {
        timestamp_str
    })
}

fn format_decimal(unscaled: i128, scale: i8) -> String {
    crate::decimal_text::format_decimal_with_scale(unscaled, scale)
}

fn format_decimal256(unscaled: i256, scale: i8) -> String {
    crate::decimal_text::format_decimal256_with_scale(unscaled, scale)
}

/// Scalar range refusal is a whole-format result, never a row-mask permission.
#[derive(Debug)]
enum FormatRange {
    Date32(i32),
    Timestamp { unit: TimeUnit, value: i64 },
}
impl fmt::Display for FormatRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Date32(value) => write!(
                f,
                "aggregate date32 value {value} exceeds the calendar range"
            ),
            Self::Timestamp { unit, value } => write!(
                f,
                "aggregate timestamp {value} ({unit:?}) exceeds the calendar range"
            ),
        }
    }
}
impl std::error::Error for FormatRange {}
impl novarocks_failure_contract::FailureProjection for FormatRange {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(
            &crate::DataFailureReason::NumericOutOfRange,
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for FormatRange {}
#[cfg(test)]
mod range_properties {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    #[test]
    fn real_negative_millisecond_formats_the_instant_before_epoch() {
        assert_eq!(
            format_timestamp(TimeUnit::Millisecond, -1, None).unwrap(),
            "1969-12-31 23:59:59.999"
        );
        assert_eq!(
            format_timestamp(TimeUnit::Microsecond, -1, None).unwrap(),
            "1969-12-31 23:59:59.999999"
        );
        assert_eq!(
            format_timestamp(TimeUnit::Nanosecond, -1, None).unwrap(),
            "1969-12-31 23:59:59.999999999"
        );
    }
    #[test]
    fn calendar_range_failure_is_typed_and_never_replaced_with_epoch_or_panics() {
        let fact = crate::DataFailureReason::NumericOutOfRange.failure_fact();
        for value in [i32::MAX, i32::MIN] {
            let error = scalar_to_string(
                &AggScalarValue::Date32(value),
                &DataType::Date32,
                &mut ScalarWork::new(None),
            )
            .unwrap_err();
            assert_eq!(error.failure_fact(), fact);
        }
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
        ] {
            let error = format_timestamp(unit, i64::MAX, None).unwrap_err();
            assert_eq!(error.failure_fact(), fact);
        }
    }
}
