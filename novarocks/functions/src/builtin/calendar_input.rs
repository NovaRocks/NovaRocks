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

//! Legacy Arrow datetime reader used by scalar extrema.
use crate::{
    KernelFailure,
    datetime_value::{self, DateParseObservation},
    kernel_control::invalid,
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{
    Array, Date32Array, FixedSizeBinaryArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::NaiveDateTime;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    OuterFailureProjection, domain,
};
use std::{fmt, num::NonZeroU16};

pub(super) enum CalendarCarrierError<'a> {
    UnsupportedDatetime(&'a DataType),
    Downcast(&'static str),
}
impl CalendarCarrierError<'_> {
    pub(super) fn into_kernel(self) -> KernelFailure {
        let detail = match self {
            Self::UnsupportedDatetime(ty) => BoundedDetail::truncating_display(format_args!(
                "unsupported datetime input type: {ty:?}"
            )),
            Self::Downcast(detail) => BoundedDetail::truncating(detail),
        };
        KernelFailure::from_domain(CalendarInputFailure(detail))
    }

    pub(super) fn legacy_message(self) -> String {
        match self {
            Self::UnsupportedDatetime(data_type) => {
                format!("unsupported datetime input type: {data_type:?}")
            }
            Self::Downcast(message) => message.to_string(),
        }
    }
}

#[derive(Debug)]
struct CalendarInputFailure(BoundedDetail);

impl fmt::Display for CalendarInputFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0.as_str())
    }
}

impl std::error::Error for CalendarInputFailure {}

impl FailureProjection for CalendarInputFailure {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(273).unwrap()),
        )
    }
}

impl OuterFailureProjection for CalendarInputFailure {}

pub(super) enum DateInput<'a> {
    Date(&'a Date32Array),
    Text(&'a StringArray),
    RawTimestamp(&'a dyn Array, TimeUnit),
    RawLargeInt(&'a FixedSizeBinaryArray),
}
impl<'a> DateInput<'a> {
    pub(super) fn raw(array: &'a dyn Array) -> Result<Self, CalendarCarrierError<'a>> {
        match array.data_type() {
            DataType::Date32 => {
                array
                    .as_any()
                    .downcast_ref()
                    .map(Self::Date)
                    .ok_or(CalendarCarrierError::Downcast(
                        "failed to downcast to Date32Array",
                    ))
            }
            DataType::Timestamp(unit, _) => Ok(Self::RawTimestamp(array, *unit)),
            DataType::Utf8 => {
                array
                    .as_any()
                    .downcast_ref()
                    .map(Self::Text)
                    .ok_or(CalendarCarrierError::Downcast(
                        "failed to downcast to StringArray",
                    ))
            }
            DataType::FixedSizeBinary(16) => {
                array.as_any().downcast_ref().map(Self::RawLargeInt).ok_or(
                    CalendarCarrierError::Downcast(
                        "datetime LARGEINT input: expected FixedSizeBinaryArray",
                    ),
                )
            }
            other => Err(CalendarCarrierError::UnsupportedDatetime(other)),
        }
    }
    pub(super) fn read(
        &self,
        row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<NaiveDateTime>, KernelFailure> {
        work.step()?;
        match self {
            Self::Date(values) => {
                work.flush()?;
                let value = datetime_value::date32_to_naive(values.value(row))
                    .and_then(|d| d.and_hms_opt(0, 0, 0));
                work.flush()?;
                Ok(value)
            }
            Self::RawTimestamp(values, unit) => {
                macro_rules! value {
                    ($array:ty, $error:literal) => {
                        values
                            .as_any()
                            .downcast_ref::<$array>()
                            .ok_or_else(|| invalid($error))?
                            .value(row)
                    };
                }
                let value = match unit {
                    TimeUnit::Second => value!(
                        TimestampSecondArray,
                        "failed to downcast to TimestampSecondArray"
                    ),
                    TimeUnit::Millisecond => value!(
                        TimestampMillisecondArray,
                        "failed to downcast to TimestampMillisecondArray"
                    ),
                    TimeUnit::Microsecond => value!(
                        TimestampMicrosecondArray,
                        "failed to downcast to TimestampMicrosecondArray"
                    ),
                    TimeUnit::Nanosecond => value!(
                        TimestampNanosecondArray,
                        "failed to downcast to TimestampNanosecondArray"
                    ),
                };
                work.flush()?;
                let value = datetime_value::timestamp_to_naive(unit, value);
                work.flush()?;
                Ok(value)
            }
            Self::RawLargeInt(values) => {
                let value = i64::try_from(i128::from_be_bytes(
                    values
                        .value(row)
                        .try_into()
                        .map_err(|_| invalid("LARGEINT value must have sixteen bytes"))?,
                ))
                .ok();
                work.flush()?;
                let value =
                    value.and_then(crate::calendar_numeric::numeric_datetime_literal_to_naive);
                work.flush()?;
                Ok(value)
            }
            Self::Text(values) => {
                let mut observe = |event| match event {
                    DateParseObservation::Step => work.step(),
                    DateParseObservation::OpaqueBoundary => work.flush(),
                };
                let text = values.value(row);
                if let Some(value) = datetime_value::parse_datetime_observed(text, &mut observe)? {
                    return Ok(Some(value));
                }
                let date = datetime_value::parse_date_observed(text, &mut observe)?;
                observe(DateParseObservation::OpaqueBoundary)?;
                let value = date.and_then(|d| d.and_hms_opt(0, 0, 0));
                observe(DateParseObservation::OpaqueBoundary)?;
                Ok(value)
            }
        }
    }
}
