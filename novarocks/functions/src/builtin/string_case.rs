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

//! Selected Unicode case conversion and normalization, using Trino 484's Unicode profile.

pub(super) mod unicode;

use super::string_measure::fixed_char_width;
use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use icu_normalizer::{ComposingNormalizer, DecomposingNormalizer};
use novarocks_type_contract::ValueLogicalType;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StringCaseOp {
    Lower,
    Upper,
}

fn visit_case(
    op: StringCaseOp,
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
    mut visit: impl FnMut(char, &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    for character in text.chars() {
        let mapped = match op {
            StringCaseOp::Lower => unicode::lowercase(character),
            StringCaseOp::Upper => unicode::uppercase(character),
        };
        work.step()?;
        visit(mapped, work)?;
    }
    Ok(())
}

fn mapped_length(
    op: StringCaseOp,
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<u128, KernelFailure> {
    let mut length = 0u128;
    visit_case(op, text, work, |character, work| {
        length += character.len_utf8() as u128;
        work.step()
    })?;
    Ok(length)
}

fn output_layout(rows: usize, bytes: usize) -> Result<(), KernelFailure> {
    let offsets = rows
        .checked_add(1)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let offsets = super::string_failure::array_layout::<i32>(offsets)?.size();
    let bitmap = rows
        .checked_add(63)
        .map(|bits| bits / 64)
        .and_then(|words| words.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    super::string_failure::array_layout::<u8>(bitmap)?;
    bytes
        .checked_add(offsets)
        .and_then(|value| value.checked_add(bitmap))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

#[derive(Clone, Copy)]
enum NormalizationForm {
    Nfc,
    Nfd,
    Nfkc,
    Nfkd,
}

impl NormalizationForm {
    fn from_argument(form: &str) -> Option<Self> {
        match form {
            "NFC" => Some(Self::Nfc),
            "NFD" => Some(Self::Nfd),
            "NFKC" => Some(Self::Nfkc),
            "NFKD" => Some(Self::Nfkd),
            _ => None,
        }
    }

    fn visit(
        self,
        text: &str,
        work: &mut EvaluationCheckpoints<'_>,
        mut visit: impl FnMut(char, &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure>,
    ) -> Result<(), KernelFailure> {
        let mut start = 0;
        for (byte, character) in text.char_indices() {
            work.step()?;
            if !unicode::assigned_in_unicode_16(character) {
                // The reference did not assign this scalar. Normalization
                // treats it as an unchanged starter and breaks the segment.
                self.visit_segment(&text[start..byte], |character| visit(character, work))?;
                visit(character, work)?;
                start = byte + character.len_utf8();
            }
        }
        self.visit_segment(&text[start..], |character| visit(character, work))
    }

    fn visit_segment(
        self,
        text: &str,
        visit: impl FnMut(char) -> Result<(), KernelFailure>,
    ) -> Result<(), KernelFailure> {
        match self {
            Self::Nfc => ComposingNormalizer::new_nfc()
                .normalize_iter(text.chars())
                .try_for_each(visit),
            Self::Nfd => DecomposingNormalizer::new_nfd()
                .normalize_iter(text.chars())
                .try_for_each(visit),
            Self::Nfkc => ComposingNormalizer::new_nfkc()
                .normalize_iter(text.chars())
                .try_for_each(visit),
            Self::Nfkd => DecomposingNormalizer::new_nfkd()
                .normalize_iter(text.chars())
                .try_for_each(visit),
        }
    }
}

pub(super) fn evaluate_normalize<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let (
            [
                FunctionArgumentType::Value(source_type),
                FunctionArgumentType::Value(form_type),
            ],
            [source, form],
        ) = (
            input.contract().selected().argument_types.as_ref(),
            input.arguments(),
        )
        else {
            return Err(invalid("normalize requires two checked arguments"));
        };
        let target = input.contract().result_type();
        if [source_type, form_type, target].iter().any(|ty| {
            ty.logical_type() != ValueLogicalType::Physical || ty.data_type() != &DataType::Utf8
        }) {
            return Err(invalid("normalize differs from its checked string profile"));
        }
        let strings = source
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("normalize selected source is not Utf8"))?;
        let forms = form
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("normalize selected form is not Utf8"))?;
        let selection = input.selection();
        let row_values = |ordinal, batch_row, work: &mut EvaluationCheckpoints<'_>| {
            let source_row = source.value_row(ordinal, batch_row);
            let form_row = form.value_row(ordinal, batch_row);
            work.step()?;
            if source_row >= strings.len() || form_row >= forms.len() {
                return Err(internal("normalize selected row is outside its carrier"));
            }
            if strings.is_null(source_row) || forms.is_null(form_row) {
                return Ok(None);
            }
            Ok(Some((
                strings.value(source_row),
                NormalizationForm::from_argument(forms.value(form_row)),
            )))
        };

        let mut extent = 0i32;
        let mut error_count = 0;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let Some((text, form)) = row_values(ordinal, batch_row, &mut work)? {
                match form {
                    Some(form) => {
                        work.flush()?;
                        form.visit(text, &mut work, |character, work| {
                            extent = crate::aggregate_scalar::checked_utf8_output_offset(
                                extent,
                                character.len_utf8(),
                            )?;
                            work.step()
                        })?;
                        work.flush()?;
                    }
                    None => error_count += 1,
                }
            }
        }
        let bytes_capacity = extent as usize;
        output_layout(selection.len(), bytes_capacity)?;
        work.flush()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(bytes_capacity)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut offsets = Vec::new();
        offsets
            .try_reserve_exact(selection.len() + 1)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut errors = Vec::new();
        errors
            .try_reserve_exact(error_count)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        offsets.push(0i32);
        let mut has_null = false;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let valid = match row_values(ordinal, batch_row, &mut work)? {
                Some((text, Some(form))) => {
                    work.flush()?;
                    form.visit(text, &mut work, |character, work| {
                        if character.len_utf8() > bytes_capacity - bytes.len() {
                            return Err(internal("normalize exceeded its measured output extent"));
                        }
                        let mut encoded = [0u8; 4];
                        bytes.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
                        work.step()
                    })?;
                    work.flush()?;
                    true
                }
                Some((_, None)) => {
                    errors.push(crate::RowDataError::with_reason(
                        ordinal,
                        crate::DataFailureReason::Domain(
                            novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
                        ),
                        "Normalization form must be one of [NFD, NFC, NFKD, NFKC]",
                    ));
                    false
                }
                None => false,
            };
            validity.append(valid);
            has_null |= !valid;
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != bytes_capacity || errors.len() != error_count {
            return Err(internal(
                "normalize differs from its measured output extent",
            ));
        }
        work.flush()?;
        let array = Arc::new(StringArray::new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(bytes),
            has_null.then(|| NullBuffer::new(validity.finish())),
        )) as ArrayRef;
        SelectedValues::try_new_observed(
            selection,
            target.data_type(),
            array,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

pub(super) fn evaluate_string_case<'a>(
    op: StringCaseOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let ([FunctionArgumentType::Value(source)], [argument]) = (
            input.contract().selected().argument_types.as_ref(),
            input.arguments(),
        ) else {
            return Err(invalid(
                "string case conversion requires one checked value argument",
            ));
        };
        let target = input.contract().result_type();
        if source.logical_type() != ValueLogicalType::Physical
            || source.data_type() != &DataType::Utf8
            || target.logical_type() != ValueLogicalType::Physical
            || target.data_type() != &DataType::Utf8
            || target.nullable() != source.nullable()
            || fixed_char_width(source) != fixed_char_width(target)
        {
            return Err(invalid(
                "string case conversion differs from its exact installed profile",
            ));
        }
        let selection = input.selection();
        let array = build_case_array(op, *argument, selection, source.nullable(), &mut work)?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            target.data_type(),
            array,
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

/// Both output passes use the same simple scalar map without a temporary string.
fn build_case_array(
    op: StringCaseOp,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    source_nullable: bool,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let values = argument
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| internal("string case conversion selected carrier is not Utf8"))?;
    output_layout(selection.len(), 0)?;
    let mut output_extent = 0i32;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        let row = argument.value_row(ordinal, batch_row);
        work.step()?;
        if row >= values.len() {
            return Err(internal(
                "string case conversion selected row is outside its carrier",
            ));
        }
        if values.is_null(row) {
            if !source_nullable {
                return Err(internal(
                    "string case conversion non-null source contains selected SQL NULL",
                ));
            }
            continue;
        }
        let text = values.value(row);
        let length = value_extent(mapped_length(op, text, work)?)?;
        output_extent = crate::aggregate_scalar::checked_utf8_output_offset(output_extent, length)?;
    }
    let output_bytes = output_extent as usize;
    output_layout(selection.len(), output_bytes)?;
    work.flush()?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(output_bytes)
        .map_err(|source| super::string_failure::reservation::<u8>(output_bytes, source))?;
    work.flush()?;
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(selection.len() + 1)
        .map_err(|source| super::string_failure::reservation::<i32>(selection.len() + 1, source))?;
    work.flush()?;
    let mut validity = BooleanBufferBuilder::new(selection.len());
    work.flush()?;
    offsets.push(0i32);
    let mut has_null = false;
    walk_case_values(
        argument,
        selection,
        source_nullable,
        work,
        |converted, work| {
            if let Some(converted) = converted {
                visit_case(op, converted, work, |character, work| {
                    if character.len_utf8() > output_bytes - bytes.len() {
                        return Err(internal(
                            "string case conversion exceeded its measured output extent",
                        ));
                    }
                    let mut utf8 = [0u8; 4];
                    for byte in character.encode_utf8(&mut utf8).bytes() {
                        bytes.push(byte);
                        work.step()?;
                    }
                    Ok(())
                })?;
                validity.append(true);
            } else {
                has_null = true;
                validity.append(false);
            }
            offsets.push(
                i32::try_from(bytes.len())
                    .map_err(|_| internal("validated string case output extent drifted"))?,
            );
            work.step()?;
            Ok(())
        },
    )?;
    if bytes.len() != output_bytes {
        return Err(internal(
            "string case conversion differs from its measured output extent",
        ));
    }
    work.flush()?;
    let array = Arc::new(StringArray::new(
        OffsetBuffer::new(offsets.into()),
        Buffer::from(bytes),
        has_null.then(|| NullBuffer::new(validity.finish())),
    )) as ArrayRef;
    work.flush()?;
    Ok(array)
}

fn walk_case_values(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    source_nullable: bool,
    work: &mut EvaluationCheckpoints<'_>,
    mut append: impl FnMut(Option<&str>, &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let values = argument
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| internal("string case conversion selected carrier is not Utf8"))?;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        work.step()?;
        let row = argument.value_row(ordinal, batch_row);
        if row >= values.len() {
            return Err(internal(
                "string case conversion selected row is outside its carrier",
            ));
        }
        let converted = if values.is_null(row) {
            if !source_nullable {
                return Err(internal(
                    "string case conversion non-null source contains selected SQL NULL",
                ));
            }
            None
        } else {
            Some(values.value(row))
        };
        append(converted, work)?;
    }
    Ok(())
}
/// Arguments are already evaluated. Both entry points share Unicode conversion,
/// per-value representation planning and the final Arrow output author.
pub fn evaluate_required_case(
    op: StringCaseOp,
    input: &ArrayRef,
) -> Result<ArrayRef, KernelFailure> {
    let values = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| KernelFailure::from_domain(CaseFailure::Carrier(op)))?;
    let mut work = EvaluationCheckpoints::new(&DirectCaseControl);
    let result = build_case_array(
        op,
        EvaluatedArgument::Column(input),
        Selection::all(values.len()),
        true,
        &mut work,
    );
    work.finish_result(result)
}
struct DirectCaseControl;
impl KernelEvaluationControl for DirectCaseControl {
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Err(internal("direct string case calculation must not wait"))
    }

    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "string_case_tests.rs"]
mod tests;

#[derive(Debug)]
enum CaseFailure {
    Carrier(StringCaseOp),
    ValueExtent { required: u128 },
}
impl std::fmt::Display for CaseFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Carrier(StringCaseOp::Lower) => f.write_str("lower expects string"),
            Self::Carrier(StringCaseOp::Upper) => {
                f.write_str("upper: argument must be a string array")
            }
            Self::ValueExtent { required } => write!(
                f,
                "Unicode case value requires {required} UTF-8 bytes, exceeding i32 offsets"
            ),
        }
    }
}
impl std::error::Error for CaseFailure {}
impl novarocks_failure_contract::FailureProjection for CaseFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let reason =
            |code| FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap());
        match self {
            Self::Carrier(_) => FailureFact::new(FailureClass::Invariant, reason(359)),
            Self::ValueExtent { required } => {
                FailureFact::new(FailureClass::Representation, reason(358)).with_representation(
                    RepresentationFacts {
                        boundary: RepresentationBoundary::MinimumUnit,
                        layout: RepresentationLayout {
                            domain: domain::KERNEL,
                            code: std::num::NonZeroU16::new(60).unwrap(),
                        },
                        required: u64::try_from(*required).ok(),
                        extent_limit: Some(i32::MAX as u64),
                        representable_prefix: None,
                    },
                )
            }
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for CaseFailure {}
fn value_extent(required: u128) -> Result<usize, KernelFailure> {
    if required > i32::MAX as u128 {
        return Err(KernelFailure::from_domain(CaseFailure::ValueExtent {
            required,
        }));
    }
    Ok(required as usize)
}
