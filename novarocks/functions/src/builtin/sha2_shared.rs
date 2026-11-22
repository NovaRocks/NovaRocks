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

//! ONE original SHA2 bit selector, incremental digest stream and lowercase hex emission.
//! Byte admission remains the existing MD5 byte author. Allocation is a caller
//! projection; observation carries no formal memory grant.
use super::md5_shared::OwnedBytesArray;
use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};
use std::{convert::Infallible, sync::Arc};
#[derive(Clone, Copy)]
pub enum Observation {
    Step,
    OpaqueBoundary,
}
#[derive(Clone, Copy)]
pub enum DigestKind {
    Sha224,
    Sha256,
    Sha384,
    Sha512,
}
impl DigestKind {
    /// Original 0 alias and unsupported-successful-NULL rule, shared by sizing and execution.
    pub fn for_bits(bits: i64) -> Option<Self> {
        match bits {
            224 => Some(Self::Sha224),
            0 | 256 => Some(Self::Sha256),
            384 => Some(Self::Sha384),
            512 => Some(Self::Sha512),
            _ => None,
        }
    }
    pub fn bytes(self) -> usize {
        match self {
            Self::Sha224 => 28,
            Self::Sha256 => 32,
            Self::Sha384 => 48,
            Self::Sha512 => 64,
        }
    }
    pub fn visit<E>(
        self,
        text: &[u8],
        observe: &mut impl FnMut(Observation) -> Result<(), E>,
        emit: &mut impl FnMut(u8),
    ) -> Result<(), E> {
        let rendered = self.render(text, observe)?;
        for pair in rendered.as_bytes().as_chunks::<2>().0 {
            emit(pair[0]);
            emit(pair[1]);
            observe(Observation::Step)?;
        }
        Ok(())
    }
    fn render<E>(
        self,
        text: &[u8],
        observe: &mut impl FnMut(Observation) -> Result<(), E>,
    ) -> Result<String, E> {
        match self {
            Self::Sha224 => digest::<Sha224, E>(text, observe),
            Self::Sha256 => digest::<Sha256, E>(text, observe),
            Self::Sha384 => digest::<Sha384, E>(text, observe),
            Self::Sha512 => digest::<Sha512, E>(text, observe),
        }
    }
}
fn digest<D: Digest, E>(
    text: &[u8],
    observe: &mut impl FnMut(Observation) -> Result<(), E>,
) -> Result<String, E> {
    let output = digest_bytes::<D, E>(text, observe)?;
    // Keep the original v1 renderer as the sole author of digest text.
    let rendered = hex::encode(output);
    observe(Observation::OpaqueBoundary)?;
    Ok(rendered)
}

pub(super) fn digest_bytes<D: Digest, E>(
    text: &[u8],
    observe: &mut impl FnMut(Observation) -> Result<(), E>,
) -> Result<sha2::digest::Output<D>, E> {
    observe(Observation::OpaqueBoundary)?;
    let mut digest = D::new();
    observe(Observation::OpaqueBoundary)?;
    // SHA update is the same original stream author. Batching exposes the
    // existing selected kernel's boundaries; digest compression stays opaque.
    for chunk in text.chunks(256) {
        observe(Observation::OpaqueBoundary)?;
        digest.update(chunk);
        observe(Observation::OpaqueBoundary)?;
        for _ in chunk {
            observe(Observation::Step)?;
        }
    }
    observe(Observation::OpaqueBoundary)?;
    let output = digest.finalize();
    observe(Observation::OpaqueBoundary)?;
    Ok(output)
}
/// Original full-batch result projection over already-evaluated normalized inputs.
/// Caller length/index panics and the ignored requested carrier remain unchanged.
pub fn evaluate_legacy(input: &OwnedBytesArray, length: &Int64Array, rows: usize) -> ArrayRef {
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        if input.is_null(row) || length.is_null(row) {
            out.push(None);
            continue;
        }
        let bits = length.value(row);
        let bytes = input.bytes(row);
        let result = DigestKind::for_bits(bits).map(|kind| {
            kind.render(bytes, &mut |_| Ok::<(), Infallible>(()))
                .unwrap_or_else(|never| match never {})
        });
        out.push(result);
    }
    Arc::new(StringArray::from(out)) as ArrayRef
}
