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

use arrow_array::builder::StringBuilder;
use arrow_schema::DataType;
use rand::{
    RngCore, SeedableRng,
    rngs::{OsRng, StdRng},
};

use crate::{
    KernelEvaluationControl, KernelFailure, ScalarCallInput, ScalarKernelInstance, SelectedValues,
    aggregate_scalar::checked_utf8_output_offset,
    kernel_control::{KernelDiagnostic, internal},
    kernel_input::EvaluationCheckpoints,
};

#[derive(Default)]
pub(super) struct Instance(Option<StdRng>);

const HEX: &[u8; 16] = b"0123456789abcdef";

fn format_uuid(bytes: [u8; 16]) -> [u8; 36] {
    let mut text = [b'-'; 36];
    let mut cursor = 0;
    for byte in bytes {
        if matches!(cursor, 8 | 13 | 18 | 23) {
            cursor += 1;
        }
        text[cursor] = HEX[usize::from(byte >> 4)];
        text[cursor + 1] = HEX[usize::from(byte & 0x0f)];
        cursor += 2;
    }
    text
}

impl ScalarKernelInstance for Instance {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        let selection = input.selection();
        let mut work = EvaluationCheckpoints::new(control);
        work.flush()?;
        let payload = selection
            .len()
            .checked_mul(36)
            .ok_or(KernelFailure::ResourceExhausted)?;
        checked_utf8_output_offset(0, payload)?;
        let mut values = StringBuilder::with_capacity(selection.len(), payload);
        if !selection.is_empty() {
            if self.0.is_none() {
                self.0 = Some(StdRng::from_rng(OsRng).map_err(|_| {
                    KernelFailure::Operational(KernelDiagnostic::new(
                        "UUID entropy source could not initialize its instance",
                    ))
                })?);
                work.flush()?;
            }
            let rng = self
                .0
                .as_mut()
                .ok_or_else(|| internal("UUID random source was not initialized"))?;
            for _ in selection.iter() {
                work.step()?;
                let mut bytes = [0; 16];
                rng.fill_bytes(&mut bytes);
                bytes[6] = (bytes[6] & 0x0f) | 0x40;
                bytes[8] = (bytes[8] & 0x3f) | 0x80;
                let text = format_uuid(bytes);
                values.append_value(
                    std::str::from_utf8(&text).map_err(|_| internal("UUID is not ASCII"))?,
                );
            }
        }
        work.flush()?;
        let output = SelectedValues::try_new_observed(
            selection,
            &DataType::Utf8,
            Arc::new(values.finish()),
            Box::new([]),
            || work.step(),
        );
        work.finish_result(output)
    }
}
