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

//! Encoders and UTF-8 recovery for actual binary inputs.
use crate::{KernelFailure, kernel_control::internal, kernel_input::EvaluationCheckpoints};
use base64::{Engine, engine::general_purpose::STANDARD};

#[derive(Clone, Copy)]
pub(super) enum Format {
    Hex,
    Base64,
}

impl Format {
    pub(super) fn output_len(self, bytes: &[u8]) -> Result<usize, KernelFailure> {
        match self {
            Self::Hex => bytes
                .len()
                .checked_mul(2)
                .ok_or(KernelFailure::ResourceExhausted),
            Self::Base64 => {
                base64::encoded_len(bytes.len(), true).ok_or(KernelFailure::ResourceExhausted)
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
                        .map_err(|_| internal("hex scratch has an invalid extent"))?;
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
                    let written = STANDARD
                        .encode_slice(chunk, &mut scratch)
                        .map_err(|_| internal("base64 scratch has an invalid extent"))?;
                    emit(&scratch[..written])?;
                    work.flush()?;
                }
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
