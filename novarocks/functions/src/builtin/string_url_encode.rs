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

//! Selected original uppercase percent encoding over borrowed UTF-8 bytes.
//! Checks cover requested-layout representability. Arrow construction and
//! allocator internals remain opaque; this is not a formal host memory grant.

use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::sync::Arc;

#[derive(Clone, Copy)]
enum Encoding {
    Legacy,
    Form,
}

impl Encoding {
    fn unescaped(self, byte: u8) -> Option<u8> {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            return Some(byte);
        }
        match (self, byte) {
            (Self::Legacy, b'~') | (Self::Form, b'*') => Some(byte),
            (Self::Form, b' ') => Some(b'+'),
            _ => None,
        }
    }
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

pub(super) fn evaluate_trino_url_encode<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    evaluate_encoding(input, control, Encoding::Form)
}

fn evaluate_encoding<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    encoding: Encoding,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let ([FunctionArgumentType::Value(source)], [argument]) = (
            input.contract().selected().argument_types.as_ref(),
            input.arguments(),
        ) else {
            return Err(invalid("URL encoding requires one checked value argument"));
        };
        let target = input.contract().result_type();
        if source.data_type() != &DataType::Utf8
            || target.data_type() != &DataType::Utf8
            || matches!(encoding, Encoding::Legacy)
                && (source.logical_type() != ValueLogicalType::Physical
                    || target.logical_type() != ValueLogicalType::Physical
                    || !target.nullable())
        {
            return Err(invalid(
                "URL encoding differs from its exact installed profile",
            ));
        }
        let selection = input.selection();
        let array = build_encoded(*argument, selection, source.nullable(), encoding, &mut work)?;
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

fn build_encoded(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    nullable: bool,
    encoding: Encoding,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let values = argument
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| internal("URL encoding selected carrier is not Utf8"))?;
    output_layout(selection.len(), 0)?;
    let mut output_offset = 0i32;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        let row = argument.value_row(ordinal, batch_row);
        work.step()?;
        if row >= values.len() {
            return Err(internal("URL encoding selected row is outside its carrier"));
        }
        if values.is_null(row) {
            if !nullable {
                return Err(internal(
                    "URL encoding non-null source contains selected SQL NULL",
                ));
            }
            continue;
        }
        let selected = values.value(row);
        let mut value_bytes = 0u128;
        for byte in selected.bytes() {
            value_bytes += if encoding.unescaped(byte).is_some() {
                1
            } else {
                3
            };
            work.step()?;
        }
        output_offset = crate::aggregate_scalar::checked_utf8_output_offset(
            output_offset,
            value_extent(value_bytes)?,
        )?;
    }
    let output_bytes = output_offset as usize;
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
    for (ordinal, batch_row) in selection.iter().enumerate() {
        let row = argument.value_row(ordinal, batch_row);
        if values.is_null(row) {
            has_null = true;
            validity.append(false);
        } else {
            let selected = values.value(row);
            if selected.len() > output_bytes - bytes.len() {
                return Err(internal("URL encoding exceeded its measured output extent"));
            }
            for byte in selected.bytes() {
                if let Some(byte) = encoding.unescaped(byte) {
                    if bytes.len() == output_bytes {
                        return Err(internal("URL encoding exceeded its measured output extent"));
                    }
                    bytes.push(byte);
                    work.step()?;
                } else {
                    if output_bytes - bytes.len() < 3 {
                        return Err(internal("URL encoding exceeded its measured output extent"));
                    }
                    for encoded in [
                        b'%',
                        b"0123456789ABCDEF"[(byte >> 4) as usize],
                        b"0123456789ABCDEF"[(byte & 0x0f) as usize],
                    ] {
                        bytes.push(encoded);
                        work.step()?;
                    }
                }
            }
            validity.append(true);
        }
        offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
        work.step()?;
    }
    if bytes.len() != output_bytes {
        return Err(internal(
            "URL encoding differs from its measured output extent",
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

/// Direct consumers use the same byte walker and output author as selected calls.
pub fn evaluate_required(input: &ArrayRef, batch_rows: usize) -> Result<ArrayRef, KernelFailure> {
    let values = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            KernelFailure::from_domain(EncodeFailure::Contract(
                novarocks_failure_contract::BoundedDetail::truncating("url_encode expects string"),
            ))
        })?;
    let rows = values.len().max(batch_rows);
    if values.len() != 1 && values.len() != rows {
        return Err(KernelFailure::from_domain(EncodeFailure::Contract(
            novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
                "url_encode input length mismatch: {} vs {rows}",
                values.len()
            )),
        )));
    }
    let argument = if values.len() == 1 {
        EvaluatedArgument::Scalar(input)
    } else {
        EvaluatedArgument::Column(input)
    };
    let mut work = EvaluationCheckpoints::new(&DirectControl);
    let result = build_encoded(
        argument,
        Selection::all(rows),
        true,
        Encoding::Legacy,
        &mut work,
    );
    work.finish_result(result)
}
struct DirectControl;
impl KernelEvaluationControl for DirectControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Err(internal("direct URL encoding must not wait"))
    }
}
#[derive(Debug)]
enum EncodeFailure {
    Contract(novarocks_failure_contract::BoundedDetail),
    ValueExtent(u128),
}
impl std::fmt::Display for EncodeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) => detail.fmt(f),
            Self::ValueExtent(required) => write!(
                f,
                "URL encoded value requires {required} UTF-8 bytes, exceeding i32 offsets"
            ),
        }
    }
}
impl std::error::Error for EncodeFailure {}
impl novarocks_failure_contract::FailureProjection for EncodeFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let reason =
            |code| FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap());
        match self {
            Self::Contract(_) => FailureFact::new(FailureClass::Invariant, reason(362)),
            Self::ValueExtent(required) => {
                FailureFact::new(FailureClass::Representation, reason(363)).with_representation(
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
impl novarocks_failure_contract::OuterFailureProjection for EncodeFailure {}
fn value_extent(required: u128) -> Result<usize, KernelFailure> {
    if required > i32::MAX as u128 {
        return Err(KernelFailure::from_domain(EncodeFailure::ValueExtent(
            required,
        )));
    }
    Ok(required as usize)
}

#[cfg(test)]
#[path = "string_url_encode_tests.rs"]
mod tests;
