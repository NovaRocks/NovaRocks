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

//! Bounded selected byte decoding.
use crate::{KernelFailure, kernel_control::internal, kernel_input::EvaluationCheckpoints};
use base64::{
    DecodeSliceError, Engine, alphabet,
    engine::{
        DecodePaddingMode,
        general_purpose::{GeneralPurpose, GeneralPurposeConfig, STANDARD_NO_PAD},
    },
};

mod crc32;
mod digest;
mod encoding;
mod trino;
mod trino_owner;
pub(super) use trino::TrinoByteOp;
pub(super) use trino_owner::{definition as trino_definition, effects as trino_effects};

// Java's basic decoder accepts omitted final padding and nonzero unused bits.
// Nonfinal quads cannot contain padding in either decoder.
const TRINO_BASE64: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_decode_allow_trailing_bits(true)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

// This is the same hex crate parser v1 called through hex::decode. The
// selected adapter bounds each opaque invocation without introducing a parser.
fn hex(
    text: &[u8],
    work: &mut EvaluationCheckpoints<'_>,
    mut emit: impl FnMut(u8) -> Result<(), KernelFailure>,
) -> Result<bool, KernelFailure> {
    work.step()?;
    if !text.len().is_multiple_of(2) {
        return Ok(false);
    }
    let mut scratch = [0u8; 128];
    for chunk in text.chunks(256) {
        for _ in chunk {
            work.step()?;
        }
        work.flush()?;
        let result = hex::decode_to_slice(chunk, &mut scratch[..chunk.len() / 2]);
        work.flush()?;
        if result.is_err() {
            return Ok(false);
        }
        for byte in &scratch[..chunk.len() / 2] {
            emit(*byte)?;
            work.step()?;
        }
    }
    Ok(true)
}
fn base64(
    text: &[u8],
    final_engine: &GeneralPurpose,
    work: &mut EvaluationCheckpoints<'_>,
    mut emit: impl FnMut(u8) -> Result<(), KernelFailure>,
) -> Result<bool, KernelFailure> {
    let mut scratch = [0u8; 192];
    let chunks = text.chunks(256);
    let count = chunks.len();
    for (i, chunk) in chunks.enumerate() {
        for _ in chunk {
            work.step()?;
        }
        work.flush()?;
        let decoded = if i + 1 == count {
            final_engine.decode_slice(chunk, &mut scratch)
        } else {
            STANDARD_NO_PAD.decode_slice(chunk, &mut scratch)
        };
        work.flush()?;
        let length = match decoded {
            Ok(n) => n,
            Err(DecodeSliceError::DecodeError(_)) => return Ok(false),
            Err(DecodeSliceError::OutputSliceTooSmall) => {
                return Err(internal("binary base64 fixed scratch too small"));
            }
        };
        for byte in &scratch[..length] {
            emit(*byte)?;
            work.step()?;
        }
    }
    Ok(true)
}
