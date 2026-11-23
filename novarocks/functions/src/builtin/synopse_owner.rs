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

use std::{fmt::Write, sync::Arc};

use crate::{
    DataFailureReason, KernelEvaluationControl, KernelFailure, RowDataError, ScalarCallInput,
    SelectedValues, aggregate_scalar::checked_utf8_output_offset, kernel_control::internal,
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array,
    builder::{Int64Builder, StringBuilder},
};
use novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT;

/// The Trino overloads accept ordinary row arguments, including the bounds.
#[derive(Clone, Copy, Debug)]
pub(super) enum TrinoSynopse {
    Bar,
    WidthBucket,
}

impl TrinoSynopse {
    pub(super) fn evaluate<'a>(
        self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        let mut work = EvaluationCheckpoints::new(control);
        work.flush()?;
        let arguments = input.arguments();
        let selection = input.selection();
        let output = match (self, arguments) {
            (Self::Bar, [percent, width]) => {
                let percentages = float64(percent.array())?;
                let widths = bigint(width.array())?;
                let mut builder = StringBuilder::new();
                let mut offset = 0;
                for (ordinal, batch_row) in selection.iter().enumerate() {
                    work.step()?;
                    let percent_row = percent.value_row(ordinal, batch_row);
                    let width_row = width.value_row(ordinal, batch_row);
                    if percentages.is_null(percent_row) || widths.is_null(width_row) {
                        builder.append_null();
                        continue;
                    }
                    render_bar(
                        percentages.value(percent_row),
                        widths.value(width_row),
                        &mut builder,
                        &mut offset,
                        &mut work,
                    )?;
                    builder.append_value("");
                }
                (Arc::new(builder.finish()) as ArrayRef, Vec::new())
            }
            (Self::WidthBucket, [value, bound1, bound2, count]) => {
                let values = float64(value.array())?;
                let bounds1 = float64(bound1.array())?;
                let bounds2 = float64(bound2.array())?;
                let counts = bigint(count.array())?;
                let mut builder = Int64Builder::with_capacity(selection.len());
                let mut errors = Vec::new();
                for (ordinal, batch_row) in selection.iter().enumerate() {
                    work.step()?;
                    let value_row = value.value_row(ordinal, batch_row);
                    let bound1_row = bound1.value_row(ordinal, batch_row);
                    let bound2_row = bound2.value_row(ordinal, batch_row);
                    let count_row = count.value_row(ordinal, batch_row);
                    if values.is_null(value_row)
                        || bounds1.is_null(bound1_row)
                        || bounds2.is_null(bound2_row)
                        || counts.is_null(count_row)
                    {
                        builder.append_null();
                        continue;
                    }
                    match width_bucket(
                        values.value(value_row),
                        bounds1.value(bound1_row),
                        bounds2.value(bound2_row),
                        counts.value(count_row),
                    ) {
                        Ok(bucket) => builder.append_value(bucket),
                        Err((reason, message)) => {
                            errors.push(RowDataError::with_reason(ordinal, reason, message));
                            builder.append_null();
                        }
                    }
                }
                (Arc::new(builder.finish()) as ArrayRef, errors)
            }
            _ => {
                return Err(internal(
                    "histogram arguments differ from the selected overload",
                ));
            }
        };
        let result = SelectedValues::try_new_observed(
            selection,
            input.contract().result_type().data_type(),
            output.0,
            output.1.into_boxed_slice(),
            || work.step(),
        );
        work.finish_result(result)
    }
}

fn float64(array: &ArrayRef) -> Result<&Float64Array, KernelFailure> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| internal("histogram selected input is not Float64"))
}

fn bigint(array: &ArrayRef) -> Result<&Int64Array, KernelFailure> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| internal("histogram selected input is not Int64"))
}

fn width_bucket(
    operand: f64,
    bound1: f64,
    bound2: f64,
    count: i64,
) -> Result<i64, (DataFailureReason, &'static str)> {
    let invalid = |message| {
        (
            DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT),
            message,
        )
    };
    if count <= 0 {
        return Err(invalid("bucketCount must be greater than 0"));
    }
    if operand.is_nan() {
        return Err(invalid("operand must not be NaN"));
    }
    if !bound1.is_finite() {
        return Err(invalid("first bound must be finite"));
    }
    if !bound2.is_finite() {
        return Err(invalid("second bound must be finite"));
    }
    if bound1 == bound2 {
        return Err(invalid("bounds cannot equal each other"));
    }
    let lower = bound1.min(bound2);
    let upper = bound1.max(bound2);
    let bucket = if operand < lower {
        0
    } else if operand >= upper {
        count.checked_add(1).ok_or((
            DataFailureReason::NumericOutOfRange,
            "width_bucket result exceeds BIGINT",
        ))?
    } else {
        (count as f64 * (operand - lower) / (upper - lower) + 1.0) as i64
    };
    Ok(if bound1 > bound2 {
        // Trino performs the descending transformation with Java long arithmetic.
        count.wrapping_sub(bucket).wrapping_add(1)
    } else {
        bucket
    })
}

fn render_bar(
    percent: f64,
    width: i64,
    output: &mut impl Write,
    offset: &mut i32,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    const RESET: &str = "\x1b[0m";
    let filled = i64::from((percent * width as f64) as i32).min(width).max(0);
    let spaces =
        usize::try_from((width - filled).max(0)).map_err(|_| KernelFailure::ResourceExhausted)?;
    let mut extent = checked_utf8_output_offset(*offset, RESET.len())?;
    extent = checked_utf8_output_offset(extent, spaces)?;
    for index in 0..filled {
        work.step()?;
        let color = bar_ansi_color(index, width);
        let color_digits = if color >= 100 { 3 } else { 2 };
        extent = checked_utf8_output_offset(extent, "\x1b[38;5;m█".len() + color_digits)?;
    }
    for index in 0..filled {
        work.step()?;
        write!(output, "\x1b[38;5;{}m█", bar_ansi_color(index, width))
            .map_err(|_| internal("cannot write ANSI bar color"))?;
    }
    output
        .write_str(RESET)
        .map_err(|_| internal("cannot write ANSI reset"))?;
    for _ in 0..spaces {
        work.step()?;
        output
            .write_char(' ')
            .map_err(|_| internal("cannot write ANSI bar padding"))?;
    }
    *offset = extent;
    Ok(())
}

fn bar_ansi_color(index: i64, width: i64) -> u16 {
    // Trino interpolates hue from Java AWT's red (0) to green (1/3), at full
    // saturation and brightness, before converting to the ANSI 6×6×6 cube.
    if width == 1 {
        return 16;
    }
    let hue = (index as f64 / (width - 1) as f64) as f32 * (2.0 / 6.0);
    let angle = hue * 6.0;
    let sector = angle as i32;
    let fraction = angle - angle.floor();
    let (red, green) = match sector {
        0 => (255, ((1.0 - (1.0 - fraction)) * 255.0 + 0.5) as u16),
        1 => (((1.0 - fraction) * 255.0 + 0.5) as u16, 255),
        _ => (0, 255),
    };
    16 + red * 6 / 256 * 36 + green * 6 / 256 * 6
}

#[cfg(test)]
mod trino_tests {
    use super::*;
    use crate::KernelDiagnostic;

    struct Control;

    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Ok(())
        }

        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            Err(KernelFailure::Internal(KernelDiagnostic::new(
                "unexpected wait",
            )))
        }
    }

    #[test]
    fn bar_matches_trino_terminal_bytes() {
        let mut work = EvaluationCheckpoints::new(&Control);
        let mut rendered = String::new();
        let mut offset = 0;
        render_bar(0.5, 6, &mut rendered, &mut offset, &mut work).unwrap();
        assert_eq!(
            rendered,
            "\x1b[38;5;196m█\x1b[38;5;208m█\x1b[38;5;220m█\x1b[0m   "
        );
        assert_eq!(offset as usize, rendered.len());
        rendered.clear();
        render_bar(1.0, 1, &mut rendered, &mut offset, &mut work).unwrap();
        assert_eq!(rendered, "\x1b[38;5;16m█\x1b[0m");
        rendered.clear();
        render_bar(1.0, -1, &mut rendered, &mut offset, &mut work).unwrap();
        assert_eq!(rendered, "\x1b[0m");
        assert_eq!(bar_ansi_color(28_301, 168_814), 208);
    }

    #[test]
    fn width_bucket_matches_trino_descending_and_overflow_rules() {
        for (value, rising, falling) in [
            (-1.0, 0, 6),
            (0.0, 1, 5),
            (2.0, 2, 4),
            (9.999, 5, 1),
            (10.0, 6, 0),
        ] {
            assert_eq!(width_bucket(value, 0.0, 10.0, 5), Ok(rising));
            assert_eq!(width_bucket(value, 10.0, 0.0, 5), Ok(falling));
        }
        assert_eq!(width_bucket(-1.0, 10.0, 0.0, i64::MAX), Ok(i64::MIN));
        assert!(matches!(
            width_bucket(10.0, 0.0, 10.0, i64::MAX),
            Err((DataFailureReason::NumericOutOfRange, _))
        ));
        assert!(matches!(
            width_bucket(f64::NAN, 0.0, 10.0, 5),
            Err((DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT), _))
        ));
    }
}
