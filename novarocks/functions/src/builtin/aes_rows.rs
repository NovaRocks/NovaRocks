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
//! AES row computation over normalized text and binary arguments.
use super::{
    aes_primitive::{self, AesMode},
    md5_shared::{Observation, OwnedBytesArray},
    to_base64_shared::latin1_string_to_bytes_observed,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Encrypt,
    Decrypt,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowFailure {
    ArgumentShape,
    AddressShape,
    Address {
        argument: usize,
        row: usize,
        len: usize,
    },
    AadRequiresGcm,
}
pub enum Row {
    Value(Option<Vec<u8>>),
    Failure(RowFailure),
}
fn decrypt_ciphertext<E>(
    mode: AesMode,
    src: &OwnedBytesArray,
    row: usize,
    key: &[u8],
    iv: Option<&[u8]>,
    aad: Option<&[u8]>,
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<Option<Vec<u8>>, E> {
    if let Some(text) = src.utf8(row) {
        let Some(bytes) = latin1_string_to_bytes_observed(text, observe)? else {
            return Ok(None);
        };
        aes_primitive::aes_decrypt_raw_observed(mode, &bytes, key, iv, aad, observe)
    } else {
        aes_primitive::aes_decrypt_raw_observed(mode, src.bytes(row), key, iv, aad, observe)
    }
}
/// Addresses are supplied by the caller's exact evaluated-carrier projection.
/// Eager child evaluation and normalization remain outside the pure row body.
pub fn evaluate_row_observed<E>(
    operation: Operation,
    inputs: &[OwnedBytesArray],
    rows: &[usize],
    observe: &mut dyn FnMut(Observation) -> Result<(), E>,
) -> Result<Row, E> {
    observe(Observation::Step)?;
    if !matches!(inputs.len(), 2 | 4 | 5) {
        return Ok(Row::Failure(RowFailure::ArgumentShape));
    }
    if rows.len() != inputs.len() {
        return Ok(Row::Failure(RowFailure::AddressShape));
    }
    for (argument, (input, &row)) in inputs.iter().zip(rows).enumerate() {
        if row >= input.len() {
            return Ok(Row::Failure(RowFailure::Address {
                argument,
                row,
                len: input.len(),
            }));
        }
    }
    let src = &inputs[0];
    let key = &inputs[1];
    let src_row = rows[0];
    let key_row = rows[1];
    if src.is_null(src_row) || key.is_null(key_row) {
        return Ok(Row::Value(None));
    }
    let src_bytes = src.bytes(src_row);
    let key_bytes = key.bytes(key_row);
    if operation == Operation::Decrypt && (src_bytes.is_empty() || key_bytes.is_empty()) {
        return Ok(Row::Value(None));
    }
    if inputs.len() == 2 {
        return Ok(Row::Value(match operation {
            Operation::Encrypt => aes_primitive::aes_encrypt_raw_observed(
                AesMode::Aes128Ecb,
                src_bytes,
                key_bytes,
                None,
                None,
                observe,
            )?,
            Operation::Decrypt => decrypt_ciphertext(
                AesMode::Aes128Ecb,
                src,
                src_row,
                key_bytes,
                None,
                None,
                observe,
            )?,
        }));
    }
    let mode_arr = &inputs[3];
    let mode_row = rows[3];
    if mode_arr.is_null(mode_row) {
        return Ok(Row::Value(None));
    }
    let mode = AesMode::parse_observed(mode_arr.bytes(mode_row), observe)?;
    let iv_arr = &inputs[2];
    let iv_row = rows[2];
    if !mode.is_ecb() && iv_arr.is_null(iv_row) {
        return Ok(Row::Value(None));
    }
    let iv_bytes = if iv_arr.is_null(iv_row) {
        None
    } else {
        Some(iv_arr.bytes(iv_row))
    };
    // then_some intentionally preserves the original eager hidden payload read.
    let aad_bytes = if inputs.len() == 5 {
        let arr = &inputs[4];
        let row = rows[4];
        (!arr.is_null(row)).then_some(arr.bytes(row))
    } else {
        None
    };
    if aad_bytes.is_some() && !mode.is_gcm() {
        return Ok(Row::Failure(RowFailure::AadRequiresGcm));
    }
    Ok(Row::Value(match operation {
        Operation::Encrypt => aes_primitive::aes_encrypt_raw_observed(
            mode, src_bytes, key_bytes, iv_bytes, aad_bytes, observe,
        )?,
        Operation::Decrypt => {
            decrypt_ciphertext(mode, src, src_row, key_bytes, iv_bytes, aad_bytes, observe)?
        }
    }))
}

/// Calculate one row with an explicit domain outcome and no external control.
pub fn evaluate_row(operation: Operation, inputs: &[OwnedBytesArray], rows: &[usize]) -> Row {
    match evaluate_row_observed(operation, inputs, rows, &mut |_| {
        Ok::<(), std::convert::Infallible>(())
    }) {
        Ok(value) => value,
        Err(never) => match never {},
    }
}

#[cfg(test)]
#[path = "aes_rows_tests.rs"]
mod tests;

impl std::fmt::Display for RowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ArgumentShape => f.write_str("AES requires 2, 4, or 5 prepared byte arguments"),
            Self::AddressShape => {
                f.write_str("AES row addresses differ from its prepared arguments")
            }
            Self::Address { argument, row, len } => {
                write!(f, "AES arg{argument} row {row} is outside its {len} values")
            }
            Self::AadRequiresGcm => f.write_str("requires GCM mode to use AAD parameter"),
        }
    }
}
impl std::error::Error for RowFailure {}
impl novarocks_failure_contract::FailureProjection for RowFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, code) = match self {
            Self::AadRequiresGcm => (FailureClass::Data, 373),
            _ => (FailureClass::Invariant, 372),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
#[derive(Debug)]
struct RequiredAesFailure {
    operation: Operation,
    _ordinal: usize,
    cause: RowFailure,
}
impl std::fmt::Display for RequiredAesFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self.operation {
            Operation::Encrypt => "aes_encrypt",
            Operation::Decrypt => "aes_decrypt",
        };
        write!(f, "{name}: {}", self.cause)
    }
}
impl std::error::Error for RequiredAesFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
impl novarocks_failure_contract::FailureProjection for RequiredAesFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.cause)
    }
}
impl novarocks_failure_contract::OuterFailureProjection for RequiredAesFailure {}

pub(super) fn aad_error(
    operation: Operation,
    ordinal: usize,
) -> Result<crate::RowDataError, crate::KernelFailure> {
    crate::RowDataError::from_domain(
        ordinal,
        RequiredAesFailure {
            operation,
            _ordinal: ordinal,
            cause: RowFailure::AadRequiresGcm,
        },
    )
}
/// Only this explicit required-row boundary promotes the row's domain result.
pub fn evaluate_required_row(
    operation: Operation,
    inputs: &[OwnedBytesArray],
    rows: &[usize],
    ordinal: usize,
) -> Result<Option<Vec<u8>>, crate::KernelFailure> {
    match evaluate_row(operation, inputs, rows) {
        Row::Value(value) => Ok(value),
        Row::Failure(cause) => Err(crate::KernelFailure::from_domain(RequiredAesFailure {
            operation,
            _ordinal: ordinal,
            cause,
        })),
    }
}
