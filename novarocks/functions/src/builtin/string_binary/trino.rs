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

//! Selected Trino byte operations over their original physical carriers.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, StringArray, builder::Int64Builder};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use md5::Md5;
use sha2::{Sha256, Sha512};

use super::{
    crc32::crc32_zlib_observed,
    digest::{Observation, digest_bytes},
    encoding::{Format, visit_utf8},
};
use crate::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, RowDataError, ScalarCallInput,
    SelectedValues,
    aggregate_host_allocator::HostAggregateAllocator,
    arrow_result_custody::{custody_type_metadata_upper_bound, retain_result_backing_direct},
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    opaque_memory::OpaqueRetainedCharge,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::builtin) enum TrinoByteOp {
    ToUtf8,
    FromUtf8,
    ToBigEndian64,
    ToHex,
    FromHex,
    ToBase64,
    FromBase64,
    Md5,
    Sha256,
    Sha512,
    Crc32,
}

impl TrinoByteOp {
    pub(in crate::builtin) fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "to_utf8" => Self::ToUtf8,
            "from_utf8" => Self::FromUtf8,
            "to_big_endian_64" => Self::ToBigEndian64,
            "to_hex" => Self::ToHex,
            "from_hex" => Self::FromHex,
            "to_base64" => Self::ToBase64,
            "from_base64" => Self::FromBase64,
            "md5" => Self::Md5,
            "sha256" => Self::Sha256,
            "sha512" => Self::Sha512,
            "crc32" => Self::Crc32,
            _ => return None,
        })
    }

    pub(super) fn may_raise(self) -> bool {
        matches!(self, Self::FromUtf8 | Self::FromHex | Self::FromBase64)
    }

    pub(super) fn accepts(self, arguments: &[&DataType]) -> bool {
        match (self, arguments) {
            (Self::ToUtf8, [DataType::Utf8]) | (Self::ToBigEndian64, [DataType::Int64]) => true,
            (Self::FromUtf8, [DataType::Binary, DataType::Utf8 | DataType::Int64]) => true,
            (Self::FromHex | Self::FromBase64, [DataType::Utf8]) => true,
            (Self::ToUtf8 | Self::ToBigEndian64, _) => false,
            (_, [DataType::Binary]) => true,
            _ => false,
        }
    }

    pub(super) fn result_type(self) -> DataType {
        match self {
            Self::Crc32 => DataType::Int64,
            Self::FromUtf8 | Self::ToHex | Self::ToBase64 => DataType::Utf8,
            _ => DataType::Binary,
        }
    }
}

type Host = (OpaqueRetainedCharge, HostAggregateAllocator);

pub(super) fn evaluate<'a>(
    operation: TrinoByteOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    allocator: Option<&HostAggregateAllocator>,
    host: Option<&Arc<dyn crate::AggregateStateAllocator>>,
    char_width: Option<usize>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let source = row_reader(input.arguments())?;
        let host = host
            .zip(allocator)
            .map(|(host, allocator)| {
                Ok::<_, KernelFailure>((
                    OpaqueRetainedCharge::try_new(Arc::clone(host))?,
                    allocator.clone(),
                ))
            })
            .transpose()?;
        if operation == TrinoByteOp::Crc32 {
            let rows = input.selection().len();
            let bytes = rows
                .checked_mul(size_of::<i64>())
                .ok_or(KernelFailure::ResourceExhausted)?;
            // Int64Builder uses a cache-line-rounded MutableBuffer.
            let bytes = add(bytes, 63)? / 64 * 64;
            let output = build_with_custody(bytes, rows, host, &mut work, |work| {
                let mut values = Int64Builder::with_capacity(rows);
                for (ordinal, row) in input.selection().iter().enumerate() {
                    work.step()?;
                    match source.value(ordinal, row) {
                        Some(ByteRow::Bytes(bytes, _)) => {
                            values.append_value(i64::from(crc32_zlib_observed(bytes, work)?));
                        }
                        None => values.append_null(),
                        Some(ByteRow::Integer(_)) => {
                            return Err(internal("CRC32 received an integer carrier"));
                        }
                    }
                }
                Ok(Arc::new(values.finish()))
            })?;
            return SelectedValues::try_new_observed(
                input.selection(),
                &DataType::Int64,
                output,
                Box::default(),
                || work.step(),
            );
        }

        let selection = input.selection();
        let mut bytes = 0usize;
        let mut error_count = 0;
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            if let Some(row) = source.value(ordinal, row) {
                match measure(operation, row, char_width, &mut work)? {
                    Ok(length) => bytes = add(bytes, length)?,
                    Err(_) => error_count += 1,
                }
            }
        }
        i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
        let offsets_bytes = add(selection.len(), 1)?
            .checked_mul(size_of::<i32>())
            .ok_or(KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut errors = Vec::new();
        errors
            .try_reserve_exact(error_count)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        let output = build_with_custody(
            add(bytes, offsets_bytes)?,
            selection.len(),
            host,
            &mut work,
            |work| {
                work.flush()?;
                let mut payload = Vec::new();
                payload
                    .try_reserve_exact(bytes)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                work.flush()?;
                let mut offsets = Vec::new();
                offsets
                    .try_reserve_exact(selection.len() + 1)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                work.flush()?;
                let mut validity = BooleanBufferBuilder::new(selection.len());
                work.flush()?;
                offsets.push(0i32);
                let mut has_null = false;
                for (ordinal, row) in selection.iter().enumerate() {
                    work.step()?;
                    let mut valid = false;
                    if let Some(row) = source.value(ordinal, row) {
                        let start = payload.len();
                        let mut exceeded = false;
                        let emitted = visit(operation, row, char_width, work, |chunk| {
                            if chunk.len() <= bytes - payload.len() {
                                payload.extend_from_slice(chunk);
                            } else {
                                exceeded = true;
                            }
                            Ok(())
                        })?;
                        match emitted {
                            Ok(()) if exceeded => {
                                return Err(internal("byte result exceeded its measured extent"));
                            }
                            Ok(()) => valid = true,
                            Err(message) => {
                                payload.truncate(start);
                                errors.push(RowDataError::with_reason(
                                    ordinal,
                                    crate::DataFailureReason::Domain(
                                        novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
                                    ),
                                    message,
                                ));
                            }
                        }
                    }
                    has_null |= !valid;
                    validity.append(valid);
                    offsets.push(
                        i32::try_from(payload.len())
                            .map_err(|_| KernelFailure::ResourceExhausted)?,
                    );
                }
                if payload.len() != bytes || errors.len() != error_count {
                    return Err(internal("byte result differs from its measured extent"));
                }
                work.flush()?;
                let offsets = OffsetBuffer::new(offsets.into());
                let payload = Buffer::from(payload);
                let nulls = has_null.then(|| NullBuffer::new(validity.finish()));
                match operation.result_type() {
                    DataType::Binary => BinaryArray::try_new(offsets, payload, nulls)
                        .map(|array| Arc::new(array) as ArrayRef)
                        .map_err(|_| internal("invalid selected Binary result")),
                    DataType::Utf8 => StringArray::try_new(offsets, payload, nulls)
                        .map(|array| Arc::new(array) as ArrayRef)
                        .map_err(|_| internal("invalid selected Utf8 result")),
                    _ => Err(internal("unexpected selected byte result type")),
                }
            },
        )?;
        SelectedValues::try_new_observed(
            selection,
            &operation.result_type(),
            output,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

fn add(left: usize, right: usize) -> Result<usize, KernelFailure> {
    left.checked_add(right)
        .ok_or(KernelFailure::ResourceExhausted)
}

fn build_with_custody(
    buffers: usize,
    rows: usize,
    host: Option<Host>,
    work: &mut EvaluationCheckpoints<'_>,
    build: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<ArrayRef, KernelFailure>,
) -> Result<ArrayRef, KernelFailure> {
    let Some((charge, allocator)) = host else {
        return build(work);
    };
    // The shared Arrow constructors round validity to 64-byte capacities.
    let validity = add(add(rows, 7)? / 8, 63)? / 64 * 64;
    let stock = add(buffers, validity)?;
    let peak = add(stock, custody_type_metadata_upper_bound(1)?)?;
    work.flush()?;
    let mut reservation = charge.reserve_operation(peak)?;
    work.flush()?;
    let output = build(work)?;
    Ok(retain_result_backing_direct(output, charge, &mut reservation, allocator, work)?.values)
}

#[derive(Clone, Copy)]
enum Replacement<'a> {
    Default,
    Text(&'a str),
    CodePoint(i64),
}

#[derive(Clone, Copy)]
enum ByteRow<'a> {
    Bytes(&'a [u8], Replacement<'a>),
    Integer(i64),
}

enum Values<'a> {
    Binary(&'a BinaryArray),
    Text(&'a StringArray),
    Integer(&'a Int64Array),
}

impl<'a> Values<'a> {
    fn checked(argument: EvaluatedArgument<'a>) -> Result<Self, KernelFailure> {
        let array = argument.array().as_any();
        if let Some(bytes) = array.downcast_ref::<BinaryArray>() {
            Ok(Self::Binary(bytes))
        } else if let Some(text) = array.downcast_ref::<StringArray>() {
            Ok(Self::Text(text))
        } else if let Some(int) = array.downcast_ref::<Int64Array>() {
            Ok(Self::Integer(int))
        } else {
            Err(internal(
                "byte operation argument differs from its frozen carrier",
            ))
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Binary(array) => array.is_null(row),
            Self::Text(array) => array.is_null(row),
            Self::Integer(array) => array.is_null(row),
        }
    }
}

struct Reader<'a> {
    source: (EvaluatedArgument<'a>, Values<'a>),
    replacement: Option<(EvaluatedArgument<'a>, Values<'a>)>,
}

fn row_reader<'a>(arguments: &[EvaluatedArgument<'a>]) -> Result<Reader<'a>, KernelFailure> {
    let (source, replacement) = match arguments {
        [source] => (*source, None),
        [source, replacement] => (
            *source,
            Some((*replacement, Values::checked(*replacement)?)),
        ),
        _ => {
            return Err(invalid(
                "byte operation received an unexpected argument count",
            ));
        }
    };
    Ok(Reader {
        source: (source, Values::checked(source)?),
        replacement,
    })
}

impl<'a> Reader<'a> {
    fn value(&self, ordinal: usize, row: usize) -> Option<ByteRow<'a>> {
        let (argument, source) = &self.source;
        let source_row = argument.value_row(ordinal, row);
        if source.is_null(source_row) {
            return None;
        }
        let replacement = match &self.replacement {
            None => Replacement::Default,
            Some((argument, values)) => {
                let row = argument.value_row(ordinal, row);
                if values.is_null(row) {
                    return None;
                }
                match values {
                    Values::Text(text) => Replacement::Text(text.value(row)),
                    Values::Integer(code) => Replacement::CodePoint(code.value(row)),
                    Values::Binary(_) => unreachable!("checked UTF-8 replacement carrier"),
                }
            }
        };
        Some(match source {
            Values::Binary(array) => ByteRow::Bytes(array.value(source_row), replacement),
            Values::Text(array) => ByteRow::Bytes(array.value(source_row).as_bytes(), replacement),
            Values::Integer(array) => ByteRow::Integer(array.value(source_row)),
        })
    }
}

fn measure(
    operation: TrinoByteOp,
    row: ByteRow<'_>,
    char_width: Option<usize>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<usize, &'static str>, KernelFailure> {
    use TrinoByteOp as Op;
    let result = match (operation, row) {
        (Op::ToBigEndian64, ByteRow::Integer(_)) => 8,
        (Op::Md5, _) => 16,
        (Op::Sha256, _) => 32,
        (Op::Sha512, _) => 64,
        (Op::ToUtf8, ByteRow::Bytes(bytes, _)) if char_width.is_none() => bytes.len(),
        (Op::ToHex, ByteRow::Bytes(bytes, _)) => Format::Hex.output_len(bytes)?,
        (Op::ToBase64, ByteRow::Bytes(bytes, _)) => Format::Base64.output_len(bytes)?,
        _ => {
            let mut length = 0;
            if let Err(message) = visit(operation, row, char_width, work, |chunk| {
                length = add(length, chunk.len())?;
                Ok(())
            })? {
                return Ok(Err(message));
            }
            length
        }
    };
    Ok(Ok(result))
}

fn visit(
    operation: TrinoByteOp,
    row: ByteRow<'_>,
    char_width: Option<usize>,
    work: &mut EvaluationCheckpoints<'_>,
    mut emit: impl FnMut(&[u8]) -> Result<(), KernelFailure>,
) -> Result<Result<(), &'static str>, KernelFailure> {
    use TrinoByteOp as Op;
    let (bytes, replacement) = match row {
        ByteRow::Bytes(bytes, replacement) => (bytes, replacement),
        ByteRow::Integer(integer) if operation == Op::ToBigEndian64 => {
            emit(&integer.to_be_bytes())?;
            return Ok(Ok(()));
        }
        ByteRow::Integer(_) => return Err(internal("unexpected integer byte operation input")),
    };
    match operation {
        Op::ToUtf8 => {
            for chunk in bytes.chunks(256) {
                for _ in chunk {
                    work.step()?;
                }
                emit(chunk)?;
            }
            if let Some(width) = char_width {
                let mut points = 0usize;
                for &byte in bytes {
                    points += usize::from(byte & 0xc0 != 0x80);
                    work.step()?;
                }
                let padding = width
                    .checked_sub(points)
                    .ok_or_else(|| internal("CHAR input exceeds its frozen width"))?;
                for _ in 0..padding {
                    emit(b" ")?;
                    work.step()?;
                }
            }
        }
        Op::FromUtf8 => {
            let mut encoded = [0u8; 4];
            let replacement = match replacement {
                Replacement::Default => Some("\u{fffd}".as_bytes()),
                Replacement::Text(text) if text.chars().take(2).count() <= 1 => {
                    Some(text.as_bytes())
                }
                Replacement::Text(_) => {
                    return Ok(Err("Replacement must be empty or a single character"));
                }
                Replacement::CodePoint(code) => {
                    // Trino validates the BIGINT upper bound and the Java
                    // int surrogate range before narrowing to a code point.
                    let narrowed = code as u32;
                    if code > 0x10ffff || (0xd800..=0xdfff).contains(&narrowed) {
                        return Ok(Err("Invalid UTF-8 replacement code point"));
                    }
                    char::from_u32(narrowed).map(|ch| &*ch.encode_utf8(&mut encoded).as_bytes())
                }
            };
            if !visit_utf8(bytes, replacement, work, emit)? {
                // A narrowed, otherwise unvalidated Java int fails only
                // when used to replace an invalid sequence. Trino exposes
                // this SDK failure outside the maskable row-error channel.
                return Err(internal("Invalid UTF-8 replacement code point"));
            }
        }
        Op::ToHex => Format::Hex.write(bytes, work, emit)?,
        Op::ToBase64 => Format::Base64.write(bytes, work, emit)?,
        Op::FromHex => {
            if !super::hex(bytes, work, |byte| emit(&[byte]))? {
                return Ok(Err("Invalid hexadecimal input"));
            }
        }
        Op::FromBase64 => {
            // Padding is optional, but Java requires a complete final quad
            // if the input contains padding.
            if (bytes.last() == Some(&b'=') && !bytes.len().is_multiple_of(4))
                || !super::base64(bytes, &super::TRINO_BASE64, work, |byte| emit(&[byte]))?
            {
                return Ok(Err("Invalid Base64 input"));
            }
        }
        Op::Md5 | Op::Sha256 | Op::Sha512 => {
            let mut observe = |event| match event {
                Observation::Step => work.step(),
                Observation::OpaqueBoundary => work.flush(),
            };
            match operation {
                Op::Md5 => emit(&digest_bytes::<Md5, _>(bytes, &mut observe)?)?,
                Op::Sha256 => emit(&digest_bytes::<Sha256, _>(bytes, &mut observe)?)?,
                Op::Sha512 => emit(&digest_bytes::<Sha512, _>(bytes, &mut observe)?)?,
                _ => unreachable!("checked byte digest operation"),
            }
        }
        Op::ToBigEndian64 | Op::Crc32 => return Err(internal("unexpected integer byte recipe")),
    }
    Ok(Ok(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Control {
        polls: AtomicUsize,
        stop_at: Option<usize>,
    }

    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            if self.stop_at == Some(self.polls.fetch_add(1, Ordering::Relaxed)) {
                Err(KernelFailure::Cancelled)
            } else {
                Ok(())
            }
        }

        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("byte operations do not wait")
        }
    }

    fn apply(
        operation: TrinoByteOp,
        row: ByteRow<'_>,
        char_width: Option<usize>,
    ) -> Result<Vec<u8>, &'static str> {
        let control = Control {
            polls: AtomicUsize::new(0),
            stop_at: None,
        };
        let mut work = EvaluationCheckpoints::new(&control);
        let measured = measure(operation, row, char_width, &mut work).unwrap();
        let mut output = Vec::new();
        let emitted = visit(operation, row, char_width, &mut work, |chunk| {
            output.extend_from_slice(chunk);
            Ok(())
        })
        .unwrap();
        work.finish().unwrap();
        assert_eq!(measured, emitted.map(|()| output.len()));
        emitted.map(|()| output)
    }

    fn bytes(input: &[u8]) -> ByteRow<'_> {
        ByteRow::Bytes(input, Replacement::Default)
    }

    #[test]
    fn trino_big_endian_binary_is_exact_twos_complement() {
        for (number, expected) in [
            (0, "0000000000000000"),
            (26, "000000000000001A"),
            (-1, "FFFFFFFFFFFFFFFF"),
            (i64::MIN, "8000000000000000"),
            (i64::MAX, "7FFFFFFFFFFFFFFF"),
        ] {
            let result = apply(TrinoByteOp::ToBigEndian64, ByteRow::Integer(number), None).unwrap();
            assert_eq!(result.len(), 8);
            assert_eq!(hex::encode_upper(result), expected);
        }
    }

    #[test]
    fn trino_hex_and_base64_have_binary_and_empty_results() {
        for (op, input, expected) in [
            (TrinoByteOp::FromHex, &b"fF00"[..], &b"\xff\x00"[..]),
            (TrinoByteOp::ToHex, b"\xff\x00", b"FF00"),
            (TrinoByteOp::FromBase64, b"/wA=", b"\xff\x00"),
            (TrinoByteOp::FromBase64, b"/wA", b"\xff\x00"),
            (TrinoByteOp::FromBase64, b"/w", b"\xff"),
            (TrinoByteOp::FromBase64, b"/x==", b"\xff"),
            (TrinoByteOp::ToBase64, b"\xff\x00", b"/wA="),
            (TrinoByteOp::FromHex, b"", b""),
            (TrinoByteOp::ToHex, b"", b""),
            (TrinoByteOp::FromBase64, b"", b""),
            (TrinoByteOp::ToBase64, b"", b""),
        ] {
            assert_eq!(apply(op, bytes(input), None).unwrap(), expected);
        }
        for invalid in [&b"F"[..], b"**", b"00\xffF"] {
            assert!(apply(TrinoByteOp::FromHex, bytes(invalid), None).is_err());
        }
        for invalid in [
            &b"A"[..],
            b"AA=",
            b"AA===",
            b"/w ==",
            b"YWJj====",
            b"\xff===",
        ] {
            assert!(apply(TrinoByteOp::FromBase64, bytes(invalid), None).is_err());
        }
        let long = [0x80u8; 385];
        let encoded = apply(TrinoByteOp::ToBase64, bytes(&long), None).unwrap();
        assert_eq!(
            apply(TrinoByteOp::FromBase64, bytes(&encoded), None).unwrap(),
            long
        );
    }

    #[test]
    fn trino_utf8_handles_maximal_invalid_sequence_and_replacement() {
        use TrinoByteOp::{FromUtf8, ToUtf8};
        let invalid = b"A\xc0\xaf\xed\xa0\x80\xffB";
        for (replacement, expected) in [
            (Replacement::Default, "A���B"),
            (Replacement::Text(""), "AB"),
            (Replacement::Text("中"), "A中中中B"),
            (Replacement::CodePoint(0x1f600), "A😀😀😀B"),
        ] {
            assert_eq!(
                apply(FromUtf8, ByteRow::Bytes(invalid, replacement), None).unwrap(),
                expected.as_bytes()
            );
        }
        for replacement in [
            Replacement::Text("ab"),
            Replacement::CodePoint(0xd800),
            Replacement::CodePoint(0x110000),
        ] {
            assert!(
                apply(
                    FromUtf8,
                    ByteRow::Bytes(b"already valid", replacement),
                    None
                )
                .is_err()
            );
        }
        assert_eq!(
            apply(ToUtf8, bytes("é中".as_bytes()), Some(4)).unwrap(),
            "é中  ".as_bytes()
        );
        assert_eq!(
            apply(ToUtf8, bytes("é".as_bytes()), None).unwrap(),
            [0xc3, 0xa9]
        );
        assert_eq!(
            apply(
                FromUtf8,
                ByteRow::Bytes(b"A", Replacement::CodePoint(-1)),
                None
            )
            .unwrap(),
            b"A"
        );
        assert_eq!(
            apply(
                FromUtf8,
                ByteRow::Bytes(b"\xff", Replacement::CodePoint(-4294967231)),
                None,
            )
            .unwrap(),
            b"A"
        );
    }

    #[test]
    fn trino_md5_and_sha2_return_raw_digest_bytes() {
        for (operation, expected) in [
            (TrinoByteOp::Md5, "e0e8bfafbb0689563b2fba789c97b3cc"),
            (
                TrinoByteOp::Sha256,
                "ea5dbf9596d187e9500f23e9a680109475341cf4e81f7e043f7d97152c10772f",
            ),
            (
                TrinoByteOp::Sha512,
                "823ff44c829aa3b7ca7926fb0f864f86b2a8e4ea4b4b3f2da6e25e7df5985a1eaa6bbb40460de1e6703b9405f7c0a0f3ceb20ede771d4241d55bd4b6f459f464",
            ),
        ] {
            assert_eq!(
                hex::encode(apply(operation, bytes(b"\xff\x00"), None).unwrap()),
                expected
            );
        }
    }

    #[test]
    fn trino_binary_digest_observes_stop_inside_one_selected_value() {
        let control = Control {
            polls: AtomicUsize::new(0),
            stop_at: Some(3),
        };
        let mut work = EvaluationCheckpoints::new(&control);
        let input = [0x80u8; 4096];
        let result = visit(TrinoByteOp::Sha256, bytes(&input), None, &mut work, |_| {
            panic!("cancelled digest must not publish output")
        });
        assert!(matches!(result, Err(KernelFailure::Cancelled)));
    }
}
