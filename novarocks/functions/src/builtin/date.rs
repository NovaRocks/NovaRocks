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

//! Existing date owner delegates to the one original selected date core.
use crate::{KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues};
pub(super) fn evaluate_date<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    super::calendar_to_date::evaluate_to_date(
        super::calendar_extended_shared::CalendarInput::owner(input),
        control,
    )
}
#[cfg(test)]
use super::calendar_to_date::output_capacity;
#[cfg(test)]
use crate::datetime_value::UNIX_EPOCH_DAY_OFFSET;
#[cfg(test)]
use arrow_array::{Array, ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray};
#[cfg(test)]
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
#[cfg(test)]
use arrow_schema::{DataType, TimeUnit};
#[cfg(test)]
use chrono::{Datelike, NaiveDate};
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
#[path = "date_tests.rs"]
mod tests;
