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

use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_schema::DataType;
use rand::{
    RngCore, SeedableRng,
    rngs::{OsRng, StdRng},
};

use crate::{
    KernelEvaluationControl, KernelFailure, ScalarCallInput, ScalarKernelInstance, SelectedValues,
    kernel_control::{KernelDiagnostic, internal},
    kernel_input::EvaluationCheckpoints,
};

#[derive(Default)]
pub(super) struct Instance(Option<StdRng>);

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
        selection
            .len()
            .checked_mul(16)
            .filter(|bytes| *bytes <= i32::MAX as usize)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let mut values = FixedSizeBinaryBuilder::with_capacity(selection.len(), 16);
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
                values
                    .append_value(bytes)
                    .map_err(|_| internal("Generated UUID lost its exact byte width"))?;
            }
        }
        work.flush()?;
        let output = SelectedValues::try_new_observed(
            selection,
            &DataType::FixedSizeBinary(16),
            Arc::new(values.finish()),
            Box::new([]),
            || work.step(),
        );
        work.finish_result(output)
    }
}
