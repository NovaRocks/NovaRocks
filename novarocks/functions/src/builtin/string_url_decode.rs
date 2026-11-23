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

//! URI and form decoding, plus the required byte-decoding API.

use crate::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, Selection, kernel_control::internal,
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
enum DecodeError {
    ShortPercent,
    HexPair(u8, u8),
    Utf8 {
        index: usize,
        source: std::str::Utf8Error,
    },
}
fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
fn walk_decoded(
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
    mut emit: impl FnMut(u8, &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure>,
) -> Result<Result<usize, DecodeError>, KernelFailure> {
    let source = text.as_bytes();
    let mut at = 0usize;
    let mut length = 0usize;
    while at < source.len() {
        let byte = source[at];
        work.step()?;
        if byte == b'%' {
            if source.len() - at < 3 {
                return Ok(Err(DecodeError::ShortPercent));
            }
            let left = source[at + 1];
            work.step()?;
            let right = source[at + 2];
            work.step()?;
            let (Some(left_hex), Some(right_hex)) = (hex(left), hex(right)) else {
                return Ok(Err(DecodeError::HexPair(left, right)));
            };
            emit((left_hex << 4) | right_hex, work)?;
            at += 3;
        } else {
            // A literal plus is unchanged, exactly as in the original body.
            emit(byte, work)?;
            at += 1;
        }
        length += 1; // decoded length never exceeds the original Utf8 extent.
    }
    Ok(Ok(length))
}

struct Utf8Scan {
    bytes: [u8; 256],
    length: usize,
    processed: usize,
    failure: Option<DecodeError>,
}
impl Utf8Scan {
    fn new() -> Self {
        Self {
            bytes: [0; 256],
            length: 0,
            processed: 0,
            failure: None,
        }
    }
    fn push(
        &mut self,
        byte: u8,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        if self.failure.is_some() {
            return Ok(());
        }
        self.bytes[self.length] = byte;
        self.length += 1;
        work.step()?;
        if self.length == self.bytes.len() {
            self.check(false, work)?;
        }
        Ok(())
    }
    fn check(
        &mut self,
        final_block: bool,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        if self.failure.is_some() || self.length == 0 {
            return Ok(());
        }
        work.flush()?;
        let error = std::str::from_utf8(&self.bytes[..self.length]).err();
        work.flush()?;
        match error {
            None => {
                self.processed += self.length;
                self.length = 0;
            }
            Some(error) if final_block || error.error_len().is_some() => {
                self.failure = Some(DecodeError::Utf8 {
                    index: self.processed + error.valid_up_to(),
                    source: error,
                });
            }
            Some(error) => {
                // The original UTF-8 owner identifies an incomplete final
                // scalar. Carry at most three bytes into the next block.
                let valid = error.valid_up_to();
                let tail = self.length - valid;
                if tail > 3 {
                    return Err(internal("url_decode UTF-8 block tail exceeds one scalar"));
                }
                for index in 0..tail {
                    self.bytes[index] = self.bytes[valid + index];
                    work.step()?;
                }
                self.processed += valid;
                self.length = tail;
            }
        }
        Ok(())
    }
}
fn inspect(
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<usize, DecodeError>, KernelFailure> {
    let mut utf8 = Utf8Scan::new();
    let length = walk_decoded(text, work, |byte, work| utf8.push(byte, work))?;
    // Malformed percent anywhere wins over invalid decoded UTF-8, because the
    // original algorithm finishes percent parsing before String::from_utf8.
    let Ok(length) = length else {
        return Ok(length);
    };
    utf8.check(true, work)?;
    Ok(match utf8.failure {
        Some(error) => Err(error),
        None => Ok(length),
    })
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

fn build_decoded(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let values = argument
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| internal("URL decoding selected carrier is not Utf8"))?;
    output_layout(selection.len(), 0)?;
    let mut output_offset = 0i32;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        let row = argument.value_row(ordinal, batch_row);
        work.step()?;
        if row >= values.len() {
            return Err(internal("URL decoding selected row is outside its carrier"));
        }
        if values.is_null(row) {
            continue;
        }
        let selected = values.value(row);
        match inspect(selected, work)? {
            Ok(length) => {
                output_offset =
                    crate::aggregate_scalar::checked_utf8_output_offset(output_offset, length)?;
            }
            Err(error) => {
                return Err(KernelFailure::from_domain(RequiredDecodeFailure {
                    _ordinal: ordinal,
                    cause: error,
                }));
            }
        }
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
            match inspect(selected, work)? {
                Err(_) => {
                    return Err(internal("URL decoding changed after inspection"));
                }
                Ok(length) => {
                    if length > output_bytes - bytes.len() {
                        return Err(internal("url_decode exceeded its measured output extent"));
                    }
                    let start = bytes.len();
                    let copied = walk_decoded(selected, work, |byte, work| {
                        bytes.push(byte);
                        work.step()
                    })?
                    .map_err(|_| {
                        internal("url_decode immutable source changed after inspection")
                    })?;
                    if copied != length || bytes.len() - start != length {
                        return Err(internal("url_decode differs from its measured row extent"));
                    }
                    validity.append(true);
                }
            }
        }
        offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
        work.step()?;
    }
    if bytes.len() != output_bytes {
        return Err(internal(
            "URL decoding differs from its measured output extent",
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

/// Required consumers fail on their first invalid value.
pub fn evaluate_required(input: &ArrayRef, batch_rows: usize) -> Result<ArrayRef, KernelFailure> {
    let values = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| decode_contract("url_decode expects string"))?;
    let rows = values.len().max(batch_rows);
    if values.len() != 1 && values.len() != rows {
        return Err(decode_contract(format_args!(
            "url_decode input length mismatch: {} vs {rows}",
            values.len()
        )));
    }
    let argument = if values.len() == 1 {
        EvaluatedArgument::Scalar(input)
    } else {
        EvaluatedArgument::Column(input)
    };
    let mut work = EvaluationCheckpoints::new(&DirectControl);
    let result = build_decoded(argument, Selection::all(rows), &mut work);
    work.finish_result(result)
}
struct DirectControl;
impl KernelEvaluationControl for DirectControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Err(internal("direct URL decoding must not wait"))
    }
}
impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ShortPercent => f.write_str("decode string contains illegal hex chars: %"),
            Self::HexPair(left, right) => write!(
                f,
                "decode string contains illegal hex chars: {}{}",
                char::from(*left),
                char::from(*right)
            ),
            Self::Utf8 { index, source } => match source.error_len() {
                Some(length) => write!(
                    f,
                    "url_decode utf8 decode failed: invalid utf-8 sequence of {length} bytes from index {index}"
                ),
                None => write!(
                    f,
                    "url_decode utf8 decode failed: incomplete utf-8 byte sequence from index {index}"
                ),
            },
        }
    }
}
impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Utf8 { source, .. } => Some(source),
            _ => None,
        }
    }
}
#[derive(Debug)]
struct RequiredDecodeFailure {
    _ordinal: usize,
    cause: DecodeError,
}
impl std::fmt::Display for RequiredDecodeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::error::Error for RequiredDecodeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
impl novarocks_failure_contract::FailureProjection for RequiredDecodeFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(
            &crate::DataFailureReason::InvalidEncoding,
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for RequiredDecodeFailure {}
#[derive(Debug)]
struct DecodeContract(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for DecodeContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for DecodeContract {}
impl novarocks_failure_contract::FailureProjection for DecodeContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(364).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for DecodeContract {}
fn decode_contract(detail: impl std::fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(DecodeContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}

#[cfg(test)]
#[path = "string_url_decode_tests.rs"]
mod tests;

#[derive(Clone, Copy)]
pub(super) enum TrinoUrlDecode {
    Form,
    UriComponent,
}

impl TrinoUrlDecode {
    pub(super) fn decode(
        self,
        input: &str,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Result<String, &'static str>, KernelFailure> {
        let mut output = String::new();
        work.flush()?;
        output
            .try_reserve_exact(input.len())
            .map_err(|source| super::string_failure::reservation::<u8>(input.len(), source))?;
        let mut remaining = input;
        while let Some(ch) = remaining.chars().next() {
            work.step()?;
            if ch == '%' {
                let (decoded, consumed) = match decode_escape(remaining, work)? {
                    Ok(value) => value,
                    Err(error) => return Ok(Err(error)),
                };
                output.push(decoded);
                remaining = &remaining[consumed..];
            } else {
                let ch = if matches!(self, Self::Form) && ch == '+' {
                    ' '
                } else {
                    ch
                };
                output.push(ch);
                remaining = &remaining[ch.len_utf8()..];
            }
        }
        Ok(Ok(output))
    }
}

fn decode_escape(
    input: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<(char, usize), &'static str>, KernelFailure> {
    let mut decoded = [0; 4];
    let mut boundaries = [0; 4];
    let mut length = 0;
    let mut consumed = 0;
    let mut expected = 1;
    while length < expected && input[consumed..].starts_with('%') {
        work.step()?;
        let mut chars = input[consumed + 1..].chars();
        let (Some(left), Some(right)) = (chars.next(), chars.next()) else {
            return Ok(Err("URLDecoder: incomplete trailing escape (%) pattern"));
        };
        if !left.is_ascii() || !right.is_ascii() {
            return Ok(Err(
                "URLDecoder: illegal hexadecimal digit in escape (%) pattern",
            ));
        }
        let (Some(high), Some(low)) = (
            hex((left as u8).to_ascii_uppercase()),
            hex((right as u8).to_ascii_uppercase()),
        ) else {
            return Ok(Err(
                "URLDecoder: illegal hexadecimal digit in escape (%) pattern",
            ));
        };
        let byte = high * 16 + low;
        if length == 0 {
            expected = match byte {
                0xc2..=0xdf => 2,
                0xe0..=0xef => 3,
                0xf0..=0xf4 => 4,
                _ => 1,
            };
        }
        consumed += 1 + left.len_utf8() + right.len_utf8();
        decoded[length] = byte;
        boundaries[length] = consumed;
        length += 1;
    }
    let text = &decoded[..length];
    match std::str::from_utf8(text) {
        Ok(value) => Ok(Ok((
            value
                .chars()
                .next()
                .expect("an escape decodes at least one byte"),
            consumed,
        ))),
        Err(error) => {
            // Java UTF-8 consumes an entire encoded surrogate, or its available
            // continuation prefix, as a single malformed sequence.
            let malformed =
                if length >= 2 && decoded[0] == 0xed && (0xa0..=0xbf).contains(&decoded[1]) {
                    if length == 3 && (0x80..=0xbf).contains(&decoded[2]) {
                        3
                    } else {
                        2
                    }
                } else {
                    error.error_len().unwrap_or(length)
                };
            Ok(Ok(('\u{fffd}', boundaries[malformed - 1])))
        }
    }
}
