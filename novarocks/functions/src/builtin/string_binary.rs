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

//! Original UNHEX and TO_BINARY grammars with bounded selected decoding.
use super::string_extended::Row;
use crate::{KernelFailure, kernel_control::internal, kernel_input::EvaluationCheckpoints};
use base64::{
    DecodeSliceError, Engine, alphabet,
    engine::{
        DecodePaddingMode,
        general_purpose::{GeneralPurpose, GeneralPurposeConfig, STANDARD, STANDARD_NO_PAD},
    },
};

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
pub(super) fn visit(
    text: &str,
    format: Option<&str>,
    unhex: bool,
    work: &mut EvaluationCheckpoints<'_>,
    mut emit: impl FnMut(u8) -> Result<(), KernelFailure>,
) -> Result<Row, KernelFailure> {
    // Unknown or NULL format has always selected HEX. Matching needs no allocation.
    let utf8 = !unhex && format.is_some_and(|f| f.eq_ignore_ascii_case("utf8"));
    let b64 = !unhex && format.is_some_and(|f| f.eq_ignore_ascii_case("encode64"));
    if utf8 {
        for byte in text.bytes() {
            emit(byte)?;
            work.step()?;
        }
        return Ok(Row::Value);
    }
    if b64 {
        if text.is_empty() || !base64(text.as_bytes(), &STANDARD, work, |_| Ok(()))? {
            return Ok(Row::Null);
        }
        if !base64(text.as_bytes(), &STANDARD, work, &mut emit)? {
            return Err(internal("binary source changed after validation"));
        }
        return Ok(Row::Value);
    }
    if !hex(text.as_bytes(), work, |_| Ok(()))? {
        return Ok(if unhex { Row::Value } else { Row::Null });
    }
    if !hex(text.as_bytes(), work, &mut emit)? {
        return Err(internal("binary hex source changed after validation"));
    }

    Ok(Row::Value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KernelEvaluationControl;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    struct Control {
        calls: AtomicUsize,
        reject: Option<usize>,
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            let calls = self.calls.fetch_add(1, Ordering::Relaxed);
            if self.reject == Some(calls) {
                Err(KernelFailure::Cancelled)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("binary decoding must not wait")
        }
    }
    fn decode(text: &str, format: Option<&str>, unhex: bool) -> (Row, Vec<u8>) {
        let control = Control {
            calls: AtomicUsize::new(0),
            reject: None,
        };
        let mut work = EvaluationCheckpoints::new(&control);
        let mut bytes = vec![];
        let row = visit(text, format, unhex, &mut work, |byte| {
            bytes.push(byte);
            Ok(())
        })
        .unwrap();
        work.finish().unwrap();
        (row, bytes)
    }
    #[test]
    fn selected_binary_original_invalid_hex_semantics() {
        assert!(matches!(decode("F", None, true), (Row::Value, bytes) if bytes.is_empty()));
        assert!(matches!(decode("F", None, false), (Row::Null, bytes) if bytes.is_empty()));
        assert!(matches!(decode("", None, false), (Row::Value, bytes) if bytes.is_empty()));
        assert!(
            matches!(decode("00aBFF", None, true), (Row::Value, bytes) if bytes == [0, 171, 255])
        );
        assert!(matches!(
            decode("xx", Some("unknown"), false),
            (Row::Null, _)
        ));
        assert!(
            matches!(decode("世界", Some("UTF8"), false), (Row::Value, bytes) if bytes == "世界".as_bytes())
        );
    }
    #[test]
    fn selected_binary_base64_matches_padding_boundaries() {
        for n in [1, 2, 3, 191, 192, 193, 384, 385] {
            let bytes: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
            let text = STANDARD.encode(&bytes);
            assert!(
                matches!(decode(&text, Some("EnCoDe64"), false), (Row::Value, output) if output == bytes)
            );
        }
        for bad in ["", "AA", "AB==", "@@@@", "YWJj=", "YWJj===="] {
            assert!(
                matches!(decode(bad, Some("encode64"), false), (Row::Null, _)),
                "{bad}"
            );
        }
    }
    #[test]
    fn selected_binary_long_decoding_observes_cancellation() {
        let control = Control {
            calls: AtomicUsize::new(0),
            reject: Some(1),
        };
        let mut work = EvaluationCheckpoints::new(&control);
        let text = "ab".repeat(8192);
        assert!(matches!(
            visit(&text, None, true, &mut work, |_| Ok(())),
            Err(KernelFailure::Cancelled)
        ));
        assert_eq!(control.calls.load(Ordering::Relaxed), 2);
    }
}
