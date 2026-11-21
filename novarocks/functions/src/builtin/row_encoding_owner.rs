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

//! The original fingerprint markers and sort-key byte order for selected rows.

use std::{fmt, num::NonZeroU16, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, TimeUnit};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, OuterFailureProjection,
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
};
use sha2::{Digest, Sha256};

use crate::{
    KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    aggregate_scalar::{BinaryResultBuilder, checked_binary_output_offset},
    allocation_failure::AllocationRefusal,
    kernel_control::internal,
    kernel_input::EvaluationCheckpoints,
};

const SHA256_BYTES: usize = 32;
const BYTE_OBSERVATION_UNIT: usize = 64;

const NULL_MARKER: u8 = 0;
const NOT_NULL_MARKER: u8 = 1;
const COLUMN_SEPARATOR: u8 = 0;

const INT8: u8 = 1;
const INT16: u8 = 2;
const INT32: u8 = 3;
const INT64: u8 = 4;
const INT128: u8 = 5;
const FLOAT: u8 = 6;
const DOUBLE: u8 = 7;
const BYTES: u8 = 8;
const DECIMAL: u8 = 9;
const DATE: u8 = 10;
const DATETIME: u8 = 11;

enum Field<'a> {
    Ignored,
    Null,
    Variable(&'a [u8]),
    Fixed {
        marker: u8,
        len: usize,
        bytes: [u8; 16],
    },
}

impl<'a> Field<'a> {
    fn fixed(marker: u8, bytes: &[u8]) -> Self {
        let mut storage = [0; 16];
        storage[..bytes.len()].copy_from_slice(bytes);
        Self::Fixed {
            marker,
            len: bytes.len(),
            bytes: storage,
        }
    }

    fn read(array: &'a ArrayRef, row: usize, sort_key: bool) -> Result<Self, KernelFailure> {
        macro_rules! read {
            ($array:ty, $encode:expr) => {{
                let input = array
                    .as_any()
                    .downcast_ref::<$array>()
                    .ok_or_else(|| internal("row encoding received a foreign array carrier"))?;
                if input.is_null(row) {
                    Self::Null
                } else {
                    ($encode)(input.value(row))
                }
            }};
        }
        macro_rules! signed {
            ($array:ty, $native:ty, $marker:expr) => {
                read!($array, |value: $native| {
                    if sort_key {
                        let mut bytes = value.to_be_bytes();
                        bytes[0] ^= 0x80;
                        Self::fixed($marker, &bytes)
                    } else {
                        Self::fixed($marker, &value.to_le_bytes())
                    }
                })
            };
        }
        macro_rules! unsigned {
            ($array:ty, $native:ty, $marker:expr) => {
                read!($array, |value: $native| {
                    let bytes = if sort_key {
                        value.to_be_bytes()
                    } else {
                        value.to_le_bytes()
                    };
                    Self::fixed($marker, &bytes)
                })
            };
        }
        macro_rules! float {
            ($array:ty, $native:ty, $marker:expr) => {
                read!($array, |value: $native| {
                    if sort_key {
                        let mut bytes = value.to_be_bytes();
                        if bytes[0] & 0x80 == 0 {
                            bytes[0] ^= 0x80;
                        } else {
                            for byte in &mut bytes {
                                *byte = !*byte;
                            }
                        }
                        Self::fixed($marker, &bytes)
                    } else {
                        Self::fixed($marker, &value.to_le_bytes())
                    }
                })
            };
        }

        Ok(match array.data_type() {
            DataType::Null => Self::Null,
            DataType::Boolean => read!(BooleanArray, |value| Self::fixed(INT8, &[u8::from(value)])),
            DataType::Int8 => signed!(Int8Array, i8, INT8),
            DataType::Int16 => signed!(Int16Array, i16, INT16),
            DataType::Int32 => signed!(Int32Array, i32, INT32),
            DataType::Int64 => signed!(Int64Array, i64, INT64),
            DataType::UInt8 => unsigned!(UInt8Array, u8, INT8),
            DataType::UInt16 => unsigned!(UInt16Array, u16, INT16),
            DataType::UInt32 => unsigned!(UInt32Array, u32, INT32),
            DataType::UInt64 => unsigned!(UInt64Array, u64, INT64),
            DataType::Float32 => float!(Float32Array, f32, FLOAT),
            DataType::Float64 => float!(Float64Array, f64, DOUBLE),
            DataType::Date32 => signed!(Date32Array, i32, DATE),
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => signed!(TimestampSecondArray, i64, DATETIME),
                TimeUnit::Millisecond => signed!(TimestampMillisecondArray, i64, DATETIME),
                TimeUnit::Microsecond => signed!(TimestampMicrosecondArray, i64, DATETIME),
                TimeUnit::Nanosecond => signed!(TimestampNanosecondArray, i64, DATETIME),
            },
            DataType::Decimal128(precision, _) => read!(Decimal128Array, |value: i128| {
                if sort_key {
                    let mut bytes = value.to_be_bytes();
                    bytes[0] ^= 0x80;
                    Self::fixed(DECIMAL, &bytes)
                } else {
                    let (marker, len) = match precision {
                        ..=9 => (INT32, 4),
                        ..=18 => (INT64, 8),
                        _ => (DECIMAL, 16),
                    };
                    Self::fixed(marker, &value.to_le_bytes()[..len])
                }
            }),
            DataType::FixedSizeBinary(crate::largeint::LARGEINT_BYTE_WIDTH) => {
                read!(FixedSizeBinaryArray, |value: &'a [u8]| {
                    let mut bytes = [0; 16];
                    bytes.copy_from_slice(value);
                    if sort_key {
                        bytes[0] ^= 0x80;
                    }
                    Self::Fixed {
                        marker: INT128,
                        len: bytes.len(),
                        bytes,
                    }
                })
            }
            DataType::Utf8 => read!(StringArray, |value: &'a str| Self::Variable(
                value.as_bytes()
            )),
            DataType::Binary => read!(BinaryArray, Self::Variable),
            // Ignored carriers, including NULLs of those types, contribute no bytes.
            _ if !sort_key => Self::Ignored,
            _ => {
                return Err(internal(
                    "encode_sort_key received an unsupported argument carrier",
                ));
            }
        })
    }

    fn hash(
        &self,
        digest: &mut Sha256,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        match self {
            Self::Ignored => {}
            Self::Null => digest.update([NULL_MARKER]),
            Self::Fixed { marker, len, bytes } => {
                digest.update([*marker]);
                digest.update(&bytes[..*len]);
            }
            Self::Variable(bytes) => {
                digest.update([BYTES]);
                for chunk in bytes.chunks(BYTE_OBSERVATION_UNIT) {
                    work.step()?;
                    digest.update(chunk);
                }
            }
        }
        Ok(())
    }

    fn append_key(
        &self,
        row: &mut Vec<u8>,
        last: bool,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        match self {
            Self::Null => append_key_bytes(row, &[NULL_MARKER], work)?,
            Self::Fixed { len, bytes, .. } => {
                append_key_bytes(row, &[NOT_NULL_MARKER], work)?;
                append_key_bytes(row, &bytes[..*len], work)?;
            }
            Self::Variable(bytes) => {
                append_key_bytes(row, &[NOT_NULL_MARKER], work)?;
                for chunk in bytes.chunks(BYTE_OBSERVATION_UNIT) {
                    if last {
                        append_key_bytes(row, chunk, work)?;
                    } else {
                        let mut escaped = [0; BYTE_OBSERVATION_UNIT * 2];
                        let mut len = 0;
                        for &byte in chunk {
                            escaped[len] = byte;
                            len += 1;
                            if byte == 0 {
                                escaped[len] = 1;
                                len += 1;
                            }
                        }
                        append_key_bytes(row, &escaped[..len], work)?;
                    }
                }
                if !last {
                    append_key_bytes(row, &[0, 0], work)?;
                }
            }
            Self::Ignored => return Err(internal("encode_sort_key cannot ignore an argument")),
        }
        if !last {
            append_key_bytes(row, &[COLUMN_SEPARATOR], work)?;
        }
        Ok(())
    }
}

fn append_key_bytes(
    row: &mut Vec<u8>,
    bytes: &[u8],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    work.step()?;
    let len = row.len() + bytes.len();
    checked_binary_output_offset(0, len)?;
    if len > row.capacity() {
        work.flush()?;
        row.try_reserve(bytes.len()).map_err(|source| {
            KernelFailure::from_domain(AllocationRefusal::new("reserve sort key row", source))
        })?;
        work.flush()?;
    }
    row.extend_from_slice(bytes);
    Ok(())
}

pub(super) fn evaluate<'a>(
    sort_key: bool,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let selection = input.selection();
        let arguments = input.arguments();
        if arguments.is_empty() {
            return Err(internal("row encoding requires at least one argument"));
        }
        if !sort_key {
            let required = selection.len() as u128 * SHA256_BYTES as u128;
            if required > i32::MAX as u128 {
                return Err(KernelFailure::from_domain(FingerprintExtent(required)));
            }
        }
        let mut key = Vec::new();
        work.flush()?;
        let mut output = BinaryResultBuilder::new();
        work.flush()?;
        for (ordinal, logical_row) in selection.iter().enumerate() {
            work.step()?;
            if sort_key {
                key.clear();
                for (index, argument) in arguments.iter().enumerate() {
                    work.step()?;
                    let row = argument.value_row(ordinal, logical_row);
                    let field = Field::read(argument.array(), row, sort_key)?;
                    field.append_key(&mut key, index + 1 == arguments.len(), &mut work)?;
                }
                work.flush()?;
                output.append_value(&key)?;
            } else {
                let mut digest = Sha256::new();
                for argument in arguments {
                    work.step()?;
                    let row = argument.value_row(ordinal, logical_row);
                    let field = Field::read(argument.array(), row, sort_key)?;
                    field.hash(&mut digest, &mut work)?;
                }
                work.flush()?;
                output.append_value(digest.finalize())?;
            }
            work.flush()?;
        }
        work.flush()?;
        let values = Arc::new(output.finish()?) as ArrayRef;
        work.flush()?;
        SelectedValues::try_new_observed(
            selection,
            input.contract().result_type().data_type(),
            values,
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

#[derive(Debug)]
struct FingerprintExtent(u128);

impl fmt::Display for FingerprintExtent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "SHA256 fingerprint output requires {} Binary bytes, exceeding i32 offsets",
            self.0
        )
    }
}

impl std::error::Error for FingerprintExtent {}

impl FailureProjection for FingerprintExtent {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(421).unwrap()),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::KERNEL,
                code: NonZeroU16::new(61).unwrap(),
            },
            required: u64::try_from(self.0).ok(),
            extent_limit: Some(i32::MAX as u64),
            representable_prefix: None,
        })
    }
}

impl OuterFailureProjection for FingerprintExtent {}
