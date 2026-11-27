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

//! SQL constant presentation and ordered literal runs.

use arrow::datatypes::DataType;
use novarocks_constant_contract::{ConstantValidation, ConstantValue};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

mod literal_run;
#[cfg(test)]
pub(crate) use literal_run::admit_syntax_constant_run;
pub(crate) use literal_run::{SyntaxLiteralPrefix, syntax_run_carrier};

pub(crate) use novarocks_expression::typing::admit_syntax_constant;
use novarocks_expression::typing::syntax_constant::{
    decimal_coefficient_observed, syntax_float32_value, syntax_integer_value, validate_syntax_type,
};

#[cfg(any(test, novarocks_dev_controls))]
pub(crate) fn test_constant_policy() -> ConstantValidation {
    test_constant_validation(1 << 20)
}

#[cfg(any(test, novarocks_dev_controls))]
pub(crate) fn test_constant_validation(max_rows: u64) -> ConstantValidation {
    novarocks_constant_contract::ConstantValidation::bounded(
        novarocks_constant_contract::ConstantPolicy {
            max_rows,
            ..test_constant_fixture()
        },
    )
}

#[cfg(any(test, novarocks_dev_controls))]
pub(crate) fn test_constant_fixture() -> novarocks_constant_contract::ConstantPolicy {
    novarocks_constant_contract::ConstantPolicy {
        max_rows: 1 << 20,
        max_array_nodes: 1 << 20,
        max_logical_elements: 1 << 24,
        max_retained_buffer_bytes: 1 << 30,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 30,
        max_library_validation_bytes: 1 << 32,
    }
}

#[cfg(test)]
#[path = "constant_tests.rs"]
mod constant_tests;

fn is_physical_binary(value: &ConstantValue) -> bool {
    let ty = value.value_type();
    ty.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
        && matches!(
            ty.data_type(),
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        )
}

// SQL interval spelling reads all three components from the checked ordinal.
// This presentation view does not create a value, key, or semantic conversion.
#[derive(Clone, Copy)]
struct SqlIntervalDisplay(Option<(i32, i32, i64)>);
impl SqlIntervalDisplay {
    fn len(self) -> usize {
        fn signed_width(value: i64) -> usize {
            let magnitude = value.unsigned_abs();
            usize::from(value < 0)
                + if magnitude == 0 {
                    1
                } else {
                    magnitude.ilog10() as usize + 1
                }
        }
        match self.0 {
            None => 4,
            Some((months, days, nanoseconds)) => {
                "INTERVAL(months=, days=, nanoseconds=)".len()
                    + signed_width(i64::from(months))
                    + signed_width(i64::from(days))
                    + signed_width(nanoseconds)
            }
        }
    }
}
impl std::fmt::Display for SqlIntervalDisplay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some((months, days, nanoseconds)) => write!(
                f,
                "INTERVAL(months={months}, days={days}, nanoseconds={nanoseconds})"
            ),
            None => f.write_str("NULL"),
        }
    }
}
fn is_physical_interval(value: &ConstantValue) -> bool {
    value.value_type().logical_type() == novarocks_type_contract::ValueLogicalType::Physical
        && value.value_type().data_type()
            == &DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
}
fn write_interval_display(
    selected: SqlIntervalDisplay,
    work: &mut CompileCheckpoints<'_>,
    output: &mut dyn std::fmt::Write,
) -> Result<std::fmt::Result, crate::compiler::SqlCompileError> {
    use std::fmt::Write;
    struct Observed<'output, 'work, 'control> {
        output: &'output mut dyn Write,
        work: &'work mut CompileCheckpoints<'control>,
        primary: Option<novarocks_type_contract::CompileControlError>,
        refused: bool,
    }
    impl Write for Observed<'_, '_, '_> {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            if self.refused || self.primary.is_some() {
                return Err(std::fmt::Error);
            }
            // Labels and signed i32/i64 decimal components are fixed-width
            // formatting work, each smaller than the original work quantum.
            if let Err(cause) = self.work.flush() {
                self.primary = Some(cause);
                return Err(std::fmt::Error);
            }
            if self.output.write_str(text).is_err() {
                self.refused = true;
                return Err(std::fmt::Error);
            }
            for _ in text.as_bytes() {
                if let Err(cause) = self.work.step() {
                    self.primary = Some(cause);
                    return Err(std::fmt::Error);
                }
            }
            Ok(())
        }
    }
    let mut writer = Observed {
        output,
        work,
        primary: None,
        refused: false,
    };
    let result = write!(&mut writer, "{selected}");
    if let Some(cause) = writer.primary {
        return Err(cause.into());
    }
    Ok(result)
}

// SQL display follows the existing binary literal spelling. This reads only
// the checked ordinal and does not alter the shared diagnostic formatter.
fn write_binary_display(
    selected: Option<&[u8]>,
    work: &mut CompileCheckpoints<'_>,
    output: &mut dyn std::fmt::Write,
) -> Result<std::fmt::Result, crate::compiler::SqlCompileError> {
    let Some(bytes) = selected else {
        work.flush()?;
        let result = output.write_str("NULL");
        for _ in 0..4 {
            work.step()?;
        }
        return Ok(result);
    };
    work.flush()?;
    if output.write_str("X'").is_err() {
        return Ok(Err(std::fmt::Error));
    }
    work.step()?;
    work.step()?;
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in bytes {
        let pair = [HEX[usize::from(byte >> 4)], HEX[usize::from(byte & 15)]];
        if output
            .write_str(std::str::from_utf8(&pair).expect("hex pair is ASCII"))
            .is_err()
        {
            return Ok(Err(std::fmt::Error));
        }
        work.step()?;
        work.step()?;
    }
    work.flush()?;
    let result = output.write_str("'");
    work.step()?;
    Ok(result)
}

/// SQL display borrows the checked selected value; it is not a semantic key
/// or a representation for re-entering expression binding.
pub(crate) fn format_constant_observed(
    value: &ConstantValue,
    control: &dyn PureCompileControl,
) -> Result<String, crate::compiler::SqlCompileError> {
    use crate::compiler::SqlCompileError;
    if is_physical_interval(value) {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        work.step()?;
        let selected = SqlIntervalDisplay(value.try_interval_month_day_nano()?);
        let capacity = selected.len();
        work.step()?;
        work.flush()?;
        let mut text = String::new();
        text.try_reserve_exact(capacity)
            .map_err(|_| SqlCompileError::ResourceExhausted)?;
        write_interval_display(selected, &mut work, &mut text)?
            .map_err(|_| SqlCompileError::ResourceExhausted)?;
        work.finish()?;
        return Ok(text);
    }
    if is_physical_binary(value) {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let selected = value.try_binary()?;
        let capacity = match selected {
            Some(bytes) => bytes
                .len()
                .checked_mul(2)
                .and_then(|len| len.checked_add(3)),
            None => Some(4),
        }
        .ok_or(SqlCompileError::ResourceExhausted)?;
        let mut text = String::new();
        text.try_reserve_exact(capacity)
            .map_err(|_| SqlCompileError::ResourceExhausted)?;
        write_binary_display(selected, &mut work, &mut text)?
            .map_err(|_| SqlCompileError::ResourceExhausted)?;
        work.finish()?;
        return Ok(text);
    }
    let text = value
        .format_diagnostic_observed(CompilePhase::LowerProgram, control)
        .map_err(SqlCompileError::from)?;
    let ty = value.value_type();
    if ty.logical_type() != novarocks_type_contract::ValueLogicalType::Physical
        || !matches!(
            ty.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
        || value
            .try_utf8_borrowed_observed(CompilePhase::LowerProgram, control)?
            .is_none()
    {
        return Ok(text);
    }
    // SQL display retains string delimiters. This output never becomes a
    // binding key or a new constant, and each copied byte remains observed.
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let capacity = text
        .len()
        .checked_add(2)
        .ok_or(SqlCompileError::ResourceExhausted)?;
    let mut quoted = String::new();
    quoted
        .try_reserve_exact(capacity)
        .map_err(|_| SqlCompileError::ResourceExhausted)?;
    quoted.push('\'');
    work.step()?;
    let mut start = 0;
    while start < text.len() {
        let mut end = start.saturating_add(256).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        quoted.push_str(&text[start..end]);
        for _ in start..end {
            work.step()?;
        }
        start = end;
    }
    quoted.push('\'');
    work.step()?;
    work.finish()?;
    Ok(quoted)
}

/// Stream selected diagnostic output into the caller's bounded writer.
/// An inner fmt refusal leaves the caller's typed budget journal authoritative.
pub(crate) fn write_constant_observed(
    value: &ConstantValue,
    control: &dyn PureCompileControl,
    output: &mut dyn std::fmt::Write,
) -> Result<std::fmt::Result, crate::compiler::SqlCompileError> {
    if is_physical_interval(value) {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        work.step()?;
        let selected = SqlIntervalDisplay(value.try_interval_month_day_nano()?);
        let result = write_interval_display(selected, &mut work, output)?;
        if result.is_ok() {
            work.finish()?;
        }
        return Ok(result);
    }
    if is_physical_binary(value) {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let result = write_binary_display(value.try_binary()?, &mut work, output)?;
        work.finish()?;
        return Ok(result);
    }
    let ty = value.value_type();
    let quoted = ty.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
        && matches!(
            ty.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
        && value
            .try_utf8_borrowed_observed(CompilePhase::LowerProgram, control)?
            .is_some();
    if quoted && output.write_str("'").is_err() {
        return Ok(Err(std::fmt::Error));
    }
    let result = value.write_diagnostic_observed(CompilePhase::LowerProgram, control, output)?;
    if result.is_err() {
        return Ok(result);
    }
    if quoted {
        return Ok(output.write_str("'"));
    }
    Ok(Ok(()))
}
