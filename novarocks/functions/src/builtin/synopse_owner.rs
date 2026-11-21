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

use arrow_array::{
    Array, ArrayRef, Int64Array,
    builder::{Int64Builder, StringBuilder},
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};

use super::catalogue::constant_source;
use crate::{
    DataFailureReason, FunctionBindingError, FunctionBindingRequest, KernelEvaluationControl,
    KernelFailure, RowDataError, ScalarCallInput, SelectedValues,
    aggregate_scalar::checked_utf8_output_offset, kernel_control::internal,
    kernel_input::EvaluationCheckpoints,
};

#[derive(Clone, Copy, Debug)]
pub(super) struct Recipe {
    bar: bool,
    min: i64,
    max: i64,
    count: i64,
}

impl Recipe {
    pub(super) fn bind(
        bar: bool,
        request: FunctionBindingRequest<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, FunctionBindingError> {
        let name = if bar { "bar" } else { "equiwidth_bucket" };
        let mut get = |index, parameter| {
            let missing = || {
                FunctionBindingError::InvalidBinding(
                    format!("{name}: argument[{parameter}] must be constant").into(),
                )
            };
            let value = constant_source(request.arguments.get(index), work)?.ok_or_else(missing)?;
            work.flush()?;
            value
                .signed_integer_observed(CompilePhase::FunctionSpecialization, work.control())?
                .ok_or_else(missing)
        };
        Ok(Self {
            bar,
            min: get(1, "min")?,
            max: get(2, "max")?,
            count: get(3, if bar { "width" } else { "buckets" })?,
        })
    }

    fn check(self, value: i64) -> Result<(), String> {
        let name = if self.bar { "bar" } else { "equiwidth_bucket" };
        if self.min >= self.max {
            return Err(if self.bar {
                format!("bar requirement: min < max ({} >= {})", self.min, self.max)
            } else {
                "equiwidth_bucket requirement: min < max".into()
            });
        }
        if self.count <= 0 {
            return Err(if self.bar {
                format!("bar requirement: width > 0 ({})", self.count)
            } else {
                "equiwidth_bucket requirement: buckets > 0".into()
            });
        }
        if value < self.min {
            return Err(if self.bar {
                format!("bar requirement: size >= min ({value} < {})", self.min)
            } else {
                format!("{name} requirement: size >= min")
            });
        }
        if value > self.max {
            return Err(if self.bar {
                format!("bar requirement: size <= max ({value} > {})", self.max)
            } else {
                format!("{name} requirement: size <= max")
            });
        }
        Ok(())
    }

    pub(super) fn evaluate<'a>(
        self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        let selection = input.selection();
        let argument = input.arguments()[0];
        let values = argument
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| internal("histogram selected input is not Int64"))?;
        let mut work = EvaluationCheckpoints::new(control);
        work.flush()?;
        let mut errors = Vec::new();
        let mut result = if self.bar {
            Output::Bar(StringBuilder::new(), 0, String::new())
        } else {
            Output::Bucket(Int64Builder::with_capacity(selection.len()))
        };
        for (ordinal, batch_row) in selection.iter().enumerate() {
            work.step()?;
            let row = argument.value_row(ordinal, batch_row);
            if values.is_null(row) {
                result.append_null();
                continue;
            }
            let value = values.value(row);
            if let Err(message) = self.check(value) {
                errors.push(RowDataError::new(ordinal, &message));
                result.append_null();
                continue;
            }
            let numerator = i128::from(value) - i128::from(self.min);
            let range = i128::from(self.max) - i128::from(self.min);
            match &mut result {
                Output::Bar(builder, offset, rendered) => {
                    let width =
                        ((numerator as f64 / range as f64).min(1.0) * self.count as f64) as usize;
                    let bytes = width
                        .checked_mul('▓'.len_utf8())
                        .ok_or(KernelFailure::ResourceExhausted)?;
                    *offset = checked_utf8_output_offset(*offset, bytes)?;
                    rendered.clear();
                    work.flush()?;
                    rendered
                        .try_reserve(bytes)
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    for _ in 0..width {
                        work.step()?;
                        rendered.push('▓');
                    }
                    work.flush()?;
                    builder.append_value(rendered.as_str());
                }
                Output::Bucket(builder) => {
                    let bucket = numerator / (range / i128::from(self.count)).max(1);
                    match i64::try_from(bucket) {
                        Ok(value) => builder.append_value(value),
                        Err(_) => {
                            errors.push(RowDataError::with_reason(
                                ordinal,
                                DataFailureReason::NumericOutOfRange,
                                "equiwidth_bucket result exceeds BIGINT",
                            ));
                            builder.append_null();
                        }
                    }
                }
            }
        }
        work.flush()?;
        let output = SelectedValues::try_new_observed(
            selection,
            input.contract().result_type().data_type(),
            result.finish(),
            errors.into_boxed_slice(),
            || work.step(),
        );
        work.finish_result(output)
    }
}

enum Output {
    Bar(StringBuilder, i32, String),
    Bucket(Int64Builder),
}
impl Output {
    fn append_null(&mut self) {
        match self {
            Self::Bar(builder, ..) => builder.append_null(),
            Self::Bucket(builder) => builder.append_null(),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Bar(mut builder, ..) => Arc::new(builder.finish()),
            Self::Bucket(mut builder) => Arc::new(builder.finish()),
        }
    }
}
