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

use std::{fmt, num::NonZeroU16, sync::Arc};

use arrow_array::{Array, BooleanArray, StringArray, builder::BooleanBuilder};
use arrow_schema::DataType;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason, domain,
};

use crate::{
    KernelEvaluationControl, KernelFailure, RowDataError, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};

const FALSE_MESSAGE: &str = "assert_true failed due to false value";
const NULL_MESSAGE: &str = "assert_true failed due to null value";

#[derive(Debug)]
struct AssertionFailed(BoundedDetail);

impl fmt::Display for AssertionFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0.as_str())
    }
}

impl std::error::Error for AssertionFailed {}

impl FailureProjection for AssertionFailed {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(422).unwrap()),
        )
    }
}

pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let selection = input.selection();
    let [condition, message @ ..] = input.arguments() else {
        return Err(invalid("assert_true requires a selected Boolean condition"));
    };
    let conditions = condition
        .array()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| internal("assert_true condition is not Boolean"))?;
    let mut work = EvaluationCheckpoints::new(control);
    work.flush()?;
    let mut values = BooleanBuilder::with_capacity(selection.len());
    let mut errors = Vec::new();
    let mut false_message = None;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let row = condition.value_row(ordinal, batch_row);
        let failure = if conditions.is_null(row) {
            Some(NULL_MESSAGE)
        } else if conditions.value(row) {
            None
        } else {
            if false_message.is_none() {
                let mut selected_message = FALSE_MESSAGE;
                if let [argument] = message {
                    let messages = argument
                        .array()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| internal("assert_true message is not Utf8"))?;
                    for (ordinal, batch_row) in selection.iter().enumerate() {
                        work.step()?;
                        let row = argument.value_row(ordinal, batch_row);
                        if !messages.is_null(row) {
                            selected_message = messages.value(row);
                            break;
                        }
                    }
                }
                false_message = Some(selected_message);
            }
            false_message
        };
        if let Some(message) = failure {
            values.append_null();
            errors.push(RowDataError::from_domain(
                ordinal,
                AssertionFailed(BoundedDetail::truncating_display(message)),
            )?);
        } else {
            values.append_value(true);
        }
    }
    work.flush()?;
    let output = SelectedValues::try_new_observed(
        selection,
        &DataType::Boolean,
        Arc::new(values.finish()),
        errors.into_boxed_slice(),
        || work.step(),
    );
    work.finish_result(output)
}
