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

//! Selected calendar conversions retain the v1 scalar date-literal grammar.
use super::*;
use crate::calendar_numeric::numeric_datetime_literal_to_naive as datetime_literal_to_naive_datetime;
use crate::datetime_value::{self, DateParseObservation};
use chrono::Datelike;
use num_traits::ToPrimitive;

pub(super) fn evaluate(
    source: Source,
    array: &dyn Array,
    row: usize,
    ordinal: usize,
    unit: Option<TimeUnit>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, KernelFailure> {
    let datetime = if source == Source::Utf8 {
        let text = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("calendar cast requires its checked text carrier"))?
            .value(row);
        let mut observe = |event| match event {
            DateParseObservation::Step => work.step(),
            DateParseObservation::OpaqueBoundary => work.flush(),
        };
        match datetime_value::parse_datetime_observed(text, &mut observe)? {
            Some(datetime) => Some(datetime),
            None => datetime_value::parse_date_observed(text, &mut observe)?
                .and_then(|date| date.and_hms_opt(0, 0, 0)),
        }
    } else {
        macro_rules! value {
            ($array:ty) => {
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .ok_or_else(|| internal("calendar cast requires its checked numeric carrier"))?
                    .value(row)
            };
        }
        let number = match source {
            Source::Boolean => Some(i64::from(value!(BooleanArray))),
            Source::Signed(SignedWidth::I8) => Some(i64::from(value!(Int8Array))),
            Source::Signed(SignedWidth::I16) => Some(i64::from(value!(Int16Array))),
            Source::Signed(SignedWidth::I32) => Some(i64::from(value!(Int32Array))),
            Source::Signed(SignedWidth::I64) => Some(value!(Int64Array)),
            Source::Unsigned(UnsignedWidth::U8) => Some(i64::from(value!(UInt8Array))),
            Source::Unsigned(UnsignedWidth::U16) => Some(i64::from(value!(UInt16Array))),
            Source::Unsigned(UnsignedWidth::U32) => Some(i64::from(value!(UInt32Array))),
            Source::Unsigned(UnsignedWidth::U64) => i64::try_from(value!(UInt64Array)).ok(),
            Source::F32 => value!(Float32Array).to_i64(),
            Source::F64 => value!(Float64Array).to_i64(),
            _ => return Err(internal("calendar cast has a foreign numeric source")),
        };
        work.step()?;
        number.and_then(datetime_literal_to_naive_datetime)
    };
    work.step()?;
    let Some(datetime) = datetime else {
        return Ok(CastRowResult::Null);
    };
    let Some(unit) = unit else {
        return Ok(CastRowResult::Signed(i64::from(
            datetime.date().num_days_from_ce() - datetime_value::UNIX_EPOCH_DAY_OFFSET,
        )));
    };
    let utc = datetime.and_utc();
    let value = match unit {
        TimeUnit::Second => Some(utc.timestamp_micros() / 1_000_000),
        TimeUnit::Millisecond => Some(utc.timestamp_micros() / 1_000),
        TimeUnit::Microsecond => Some(utc.timestamp_micros()),
        TimeUnit::Nanosecond => utc.timestamp_nanos_opt(),
    };
    work.step()?;
    Ok(match value {
        Some(value) => CastRowResult::Timestamp(value),
        None => {
            let message = if source == Source::Utf8 {
                let text = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| internal("calendar cast requires its checked text carrier"))?
                    .value(row);
                {
                    let mut end = text.len().min(crate::MAX_ROW_ERROR_MESSAGE_BYTES);
                    while !text.is_char_boundary(end) {
                        end -= 1;
                    }
                    format!(
                        "CAST failed: timestamp value '{}' is out of nanosecond range",
                        &text[..end]
                    )
                }
            } else {
                "CAST failed: numeric datetime literal is out of nanosecond i64 range".to_owned()
            };
            CastRowResult::RowError(RowDataError::new(ordinal, &message))
        }
    })
}

/// The original calendar/date author emits the original selected row fact.
/// Only this explicit profile uses Trino's text grammar; legacy None is intact.
pub(super) fn evaluate_trino_array_date(
    text: &str,
    ordinal: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, KernelFailure> {
    let value = datetime_value::parse_trino_date32_observed(text, |event| match event {
        DateParseObservation::Step => work.step(),
        DateParseObservation::OpaqueBoundary => work.flush(),
    })?;
    Ok(match value {
        Some(days) => CastRowResult::Signed(i64::from(days)),
        None => {
            work.flush()?;
            CastRowResult::RowError(crate::RowDataError::from_domain(ordinal, InvalidArrayDate)?)
        }
    })
}
#[derive(Debug)]
struct InvalidArrayDate;
impl std::fmt::Display for InvalidArrayDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Cannot cast array element to DATE")
    }
}
impl std::error::Error for InvalidArrayDate {}
impl novarocks_failure_contract::FailureProjection for InvalidArrayDate {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Data,
            novarocks_function_contract::kernel_failure::INVALID_CAST_ARGUMENT,
        )
    }
}
