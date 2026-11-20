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

use arrow_array::{ArrayRef, builder::StringBuilder};
use arrow_schema::DataType;

use super::{
    aes_rows::{self, Operation, Row, RowFailure},
    md5_shared::{into_kernel, observe, to_owned_bytes_array_observed},
};
use crate::{
    KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    aggregate_scalar::checked_utf8_output_offset, kernel_control::invalid,
    kernel_input::EvaluationCheckpoints,
};

pub(super) fn evaluate<'a>(
    operation: Operation,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let function = match operation {
        Operation::Encrypt => "aes_encrypt",
        Operation::Decrypt => "aes_decrypt",
    };
    let selection = input.selection();
    let arguments = input.arguments();
    if !matches!(arguments.len(), 2 | 4 | 5) {
        return Err(invalid("AES requires 2, 4, or 5 byte arguments"));
    }
    let mut work = EvaluationCheckpoints::new(control);
    work.flush()?;
    let inputs = arguments
        .iter()
        .enumerate()
        .map(|(index, arg)| {
            to_owned_bytes_array_observed(Arc::clone(arg.array()), index, &mut |event| {
                observe(&mut work, event)
            })
            .map_err(|error| into_kernel(error, function))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut builder = StringBuilder::new();
    let mut rendered = String::new();
    let mut offset = 0;
    let mut errors = Vec::new();
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let mut rows = [0; 5];
        for (address, argument) in rows.iter_mut().zip(arguments) {
            *address = argument.value_row(ordinal, batch_row);
        }
        let row = aes_rows::evaluate_row_observed(
            operation,
            &inputs,
            &rows[..inputs.len()],
            &mut |event| observe(&mut work, event),
        )?;
        match row {
            Row::Value(Some(bytes)) => {
                rendered.clear();
                match operation {
                    Operation::Encrypt => {
                        for byte in bytes {
                            work.step()?;
                            let character = char::from(byte);
                            offset = checked_utf8_output_offset(offset, character.len_utf8())?;
                            rendered
                                .try_reserve(character.len_utf8())
                                .map_err(|_| KernelFailure::ResourceExhausted)?;
                            rendered.push(character);
                        }
                    }
                    Operation::Decrypt => {
                        for _ in &bytes {
                            work.step()?;
                        }
                        work.flush()?;
                        for chunk in bytes.utf8_chunks() {
                            work.step()?;
                            let text = chunk.valid();
                            let replacement = if chunk.invalid().is_empty() {
                                ""
                            } else {
                                "�"
                            };
                            let length = text.len() + replacement.len();
                            offset = checked_utf8_output_offset(offset, length)?;
                            work.flush()?;
                            rendered
                                .try_reserve(length)
                                .map_err(|_| KernelFailure::ResourceExhausted)?;
                            rendered.push_str(text);
                            rendered.push_str(replacement);
                        }
                    }
                }
                work.flush()?;
                builder.append_value(&rendered);
            }
            Row::Value(None) => builder.append_null(),
            Row::Failure(RowFailure::AadRequiresGcm) => {
                errors.push(aes_rows::aad_error(operation, ordinal)?);
                builder.append_null();
            }
            Row::Failure(failure) => return Err(invalid(&failure.to_string())),
        }
    }
    work.flush()?;
    let output: ArrayRef = Arc::new(builder.finish());
    let output = SelectedValues::try_new_observed(
        selection,
        &DataType::Utf8,
        output,
        errors.into_boxed_slice(),
        || work.step(),
    );
    work.finish_result(output)
}
