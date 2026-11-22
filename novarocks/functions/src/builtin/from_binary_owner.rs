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

//! Formats selected binary values with the installed FROM_BINARY semantics.

use arrow_array::{Array, BinaryArray, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use base64::{Engine, engine::general_purpose::STANDARD};
use std::sync::Arc;

use crate::{
    KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};

#[derive(Clone, Copy)]
pub(super) enum Format {
    Hex,
    Base64,
    Utf8,
}

impl Format {
    fn parse(format: Option<&str>) -> Self {
        match format {
            Some(format) if format.eq_ignore_ascii_case("encode64") => Self::Base64,
            Some(format) if format.eq_ignore_ascii_case("utf8") => Self::Utf8,
            _ => Self::Hex,
        }
    }

    pub(super) fn output_len(
        self,
        bytes: &[u8],
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<usize, KernelFailure> {
        match self {
            Self::Hex => bytes
                .len()
                .checked_mul(2)
                .ok_or(KernelFailure::ResourceExhausted),
            Self::Base64 => {
                base64::encoded_len(bytes.len(), true).ok_or(KernelFailure::ResourceExhausted)
            }
            Self::Utf8 => {
                let mut length = 0usize;
                visit_utf8(bytes, Some("\u{fffd}".as_bytes()), work, |chunk| {
                    length = length
                        .checked_add(chunk.len())
                        .ok_or(KernelFailure::ResourceExhausted)?;
                    Ok(())
                })?;
                Ok(length)
            }
        }
    }

    pub(super) fn write(
        self,
        bytes: &[u8],
        work: &mut EvaluationCheckpoints<'_>,
        mut emit: impl FnMut(&[u8]) -> Result<(), KernelFailure>,
    ) -> Result<(), KernelFailure> {
        let mut scratch = [0u8; 256];
        match self {
            Self::Hex => {
                for chunk in bytes.chunks(scratch.len() / 2) {
                    for _ in chunk {
                        work.step()?;
                    }
                    let encoded = &mut scratch[..chunk.len() * 2];
                    work.flush()?;
                    hex::encode_to_slice(chunk, &mut *encoded)
                        .map_err(|_| internal("FROM_BINARY hex scratch has an invalid extent"))?;
                    encoded.make_ascii_uppercase();
                    emit(encoded)?;
                    work.flush()?;
                }
            }
            Self::Base64 => {
                // Nonfinal chunks have no padding because their size is a multiple of three.
                for chunk in bytes.chunks(scratch.len() / 4 * 3) {
                    for _ in chunk {
                        work.step()?;
                    }
                    work.flush()?;
                    let written = STANDARD.encode_slice(chunk, &mut scratch).map_err(|_| {
                        internal("FROM_BINARY base64 scratch has an invalid extent")
                    })?;
                    emit(&scratch[..written])?;
                    work.flush()?;
                }
            }
            Self::Utf8 => {
                visit_utf8(bytes, Some("\u{fffd}".as_bytes()), work, emit)?;
            }
        }
        Ok(())
    }
}

pub(super) fn visit_utf8(
    mut bytes: &[u8],
    replacement: Option<&[u8]>,
    work: &mut EvaluationCheckpoints<'_>,
    mut emit: impl FnMut(&[u8]) -> Result<(), KernelFailure>,
) -> Result<bool, KernelFailure> {
    while let Some((&lead, tail)) = bytes.split_first() {
        // Trino 484 FROM_UTF8 uses SliceUtf8 2.9. Group the continuation
        // prefix before validating it, including obsolete 5- and 6-byte forms.
        // Overlong encodings and UTF-16 surrogates each get one replacement.
        let width = match lead {
            0xc0..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf7 => 4,
            0xf8..=0xfb => 5,
            0xfc..=0xfd => 6,
            _ => 1,
        };
        let length = 1 + tail
            .iter()
            .take(width - 1)
            .take_while(|&&byte| matches!(byte, 0x80..=0xbf))
            .count();
        let (candidate, rest) = bytes.split_at(length);
        for _ in candidate {
            work.step()?;
        }
        let output = if std::str::from_utf8(candidate).is_ok() {
            candidate
        } else {
            let Some(replacement) = replacement else {
                return Ok(false);
            };
            replacement
        };
        emit(output)?;
        bytes = rest;
    }
    Ok(true)
}

pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let (source, format) = match input.arguments() {
            [source] => (source, None),
            [source, format] => (source, Some(format)),
            _ => return Err(invalid("FROM_BINARY requires one or two checked arguments")),
        };
        let source_array = source
            .array()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("FROM_BINARY source must have a Binary carrier"))?;
        let format = format
            .map(|argument| {
                argument
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(|array| (argument, array))
                    .ok_or_else(|| invalid("FROM_BINARY format must have a Utf8 carrier"))
            })
            .transpose()?;
        let row = |ordinal, batch_row| {
            let row = source.value_row(ordinal, batch_row);
            if source_array.is_null(row) {
                return None;
            }
            let bytes = source_array.value(row);
            let format = Format::parse(format.and_then(|(argument, values)| {
                let row = argument.value_row(ordinal, batch_row);
                (!values.is_null(row)).then(|| values.value(row))
            }));
            if bytes.is_empty() && matches!(format, Format::Base64) {
                return None;
            }
            Some((bytes, format))
        };

        let selection = input.selection();
        let mut length = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let Some((bytes, format)) = row(ordinal, batch_row) {
                length = length
                    .checked_add(format.output_len(bytes, &mut work)?)
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
            work.step()?;
        }
        i32::try_from(length).map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut payload = Vec::new();
        let mut offsets = Vec::new();
        let offsets_len = selection
            .len()
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        work.flush()?;
        payload
            .try_reserve_exact(length)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        offsets
            .try_reserve_exact(offsets_len)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        offsets.push(0i32);
        let mut has_null = false;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            match row(ordinal, batch_row) {
                Some((bytes, format)) => {
                    format.write(bytes, &mut work, |chunk| {
                        if chunk.len() > length - payload.len() {
                            return Err(internal(
                                "FROM_BINARY exceeded its measured output extent",
                            ));
                        }
                        payload.extend_from_slice(chunk);
                        Ok(())
                    })?;
                    validity.append(true);
                }
                None => {
                    validity.append(false);
                    has_null = true;
                }
            }
            offsets
                .push(i32::try_from(payload.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if payload.len() != length {
            return Err(internal(
                "FROM_BINARY differs from its measured output extent",
            ));
        }
        work.flush()?;
        let array = StringArray::try_new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(payload),
            has_null.then(|| NullBuffer::new(validity.finish())),
        )
        .map_err(|_| internal("FROM_BINARY produced an invalid string array"))?;
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &DataType::Utf8,
            Arc::new(array),
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}
