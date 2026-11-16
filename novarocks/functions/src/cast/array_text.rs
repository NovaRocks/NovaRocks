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

//! The explicit array VARCHAR-to-INTEGER conversion value author.
use super::*;
use icu_properties::{CodePointMapData, props::GeneralCategory};

#[derive(Debug)]
struct InvalidInteger;
impl fmt::Display for InvalidInteger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Cannot cast array element to INTEGER")
    }
}
impl Error for InvalidInteger {}
impl novarocks_failure_contract::FailureProjection for InvalidInteger {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Data,
            novarocks_function_contract::kernel_failure::INVALID_CAST_ARGUMENT,
        )
    }
}

// JDK Long.parseLong uses Character.digit(char), so supplementary Unicode
// digits are rejected. Unicode UAX44 supplies contiguous ascending Nd ranges;
// ICU owns their actual data. No normalization or Rust Unicode trim applies.
fn digit(ch: char, work: &mut EvaluationCheckpoints<'_>) -> Result<Option<i64>, KernelFailure> {
    if ch.is_ascii_digit() {
        return Ok(Some(i64::from(ch as u8 - b'0')));
    }
    if u32::from(ch) > 0xffff {
        return Ok(None);
    }
    let categories = CodePointMapData::<GeneralCategory>::new();
    if categories.get(ch) != GeneralCategory::DecimalNumber {
        return Ok(None);
    }
    for range in categories.iter_ranges_for_value(GeneralCategory::DecimalNumber) {
        work.step()?;
        if range.contains(&u32::from(ch)) {
            return Ok(Some(i64::from((u32::from(ch) - *range.start()) % 10)));
        }
    }
    Err(internal("Unicode decimal digit has no property range"))
}
fn parse(value: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<Option<i32>, KernelFailure> {
    let bytes = value.as_bytes();
    let (mut start, mut end) = (0, bytes.len());
    while start < end && bytes[start] <= 0x20 {
        work.step()?;
        start += 1;
    }
    while end > start && bytes[end - 1] <= 0x20 {
        work.step()?;
        end -= 1;
    }
    if start == end {
        return Ok(None);
    }
    let negative = bytes[start] == b'-';
    if negative || bytes[start] == b'+' {
        start += 1;
    }
    if start == end {
        return Ok(None);
    }
    // Negative accumulation also admits the exact signed minimum. Parsing a
    // legal Long and narrowing to Int uses the same failure reason as syntax.
    let mut value = 0_i64;
    for ch in value_slice(bytes, start, end)?.chars() {
        work.step()?;
        let Some(digit) = digit(ch, work)? else {
            return Ok(None);
        };
        let Some(next) = value.checked_mul(10).and_then(|n| n.checked_sub(digit)) else {
            return Ok(None);
        };
        value = next;
    }
    let value = if negative {
        Some(value)
    } else {
        value.checked_neg()
    };
    Ok(value.and_then(|n| i32::try_from(n).ok()))
}
fn value_slice(bytes: &[u8], start: usize, end: usize) -> Result<&str, KernelFailure> {
    std::str::from_utf8(&bytes[start..end])
        .map_err(|_| invalid("array CAST trimmed outside UTF8 boundaries"))
}
pub(super) fn evaluate_row(
    recipe: &PreparedCastRecipe,
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    logical_row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, KernelFailure> {
    let row = recipe.checked_row_with_shape(argument, ordinal, logical_row, work, |array| {
        array.as_any().is::<StringArray>()
    })?;
    if logical_is_null(argument.array().as_ref(), row, 1, work)? {
        return if recipe.source.nullable() {
            Ok(CastRowResult::Null)
        } else {
            Err(invalid("non-null array CAST text contains a selected NULL"))
        };
    }
    let value = argument
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("checked text carrier")
        .value(row);
    Ok(match parse(value, work)? {
        Some(value) => CastRowResult::Signed(i64::from(value)),
        None => {
            work.flush()?;
            CastRowResult::RowError(RowDataError::from_domain(ordinal, InvalidInteger)?)
        }
    })
}
