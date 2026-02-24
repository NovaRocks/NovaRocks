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
//! Output column model and Arrow materialization for native segment scans.
//!
//! This module owns the in-memory column buffers used while scanning multiple
//! segment pages and converting them to Arrow arrays at the end of the scan.
//!
//! Current limitations:
//! - Only scalar output columns are supported.
//! - Complex/nested output types (ARRAY/MAP/STRUCT/JSON) are not supported.
//! - Temporal conversion follows StarRocks DATE/DATETIME internal storage only.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow_buffer::i256;

use crate::common::largeint;
use crate::formats::starrocks::plan::StarRocksNativeReadPlan;

use super::schema_map::{
    decimal_output_meta_from_arrow_type, expected_logical_type_from_schema_type,
    is_char_schema_type,
};
use super::types::decimal::{DecimalOutputMeta, build_decimal128_array, build_decimal256_array};
use super::types::temporal::{
    convert_date32_julian_to_unix_days, convert_datetime_storage_to_unix_micros,
};
/// Canonical output type categories used by native page decoders.
#[derive(Clone, Copy, Debug)]
pub(super) enum OutputColumnKind {
    Int8,
    Int16,
    Int32,
    Int64,
    LargeInt,
    Float32,
    Float64,
    Boolean,
    Date32,
    TimestampMicrosecond,
    Decimal128,
    Decimal256,
    Utf8,
    Binary,
}

impl OutputColumnKind {
    /// Map Arrow projection type to native decoder output kind.
    pub(super) fn from_arrow_type(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Int8 => Some(Self::Int8),
            DataType::Int16 => Some(Self::Int16),
            DataType::Int32 => Some(Self::Int32),
            DataType::Int64 => Some(Self::Int64),
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                Some(Self::LargeInt)
            }
            DataType::Float32 => Some(Self::Float32),
            DataType::Float64 => Some(Self::Float64),
            DataType::Boolean => Some(Self::Boolean),
            DataType::Date32 => Some(Self::Date32),
            DataType::Timestamp(TimeUnit::Microsecond, None) => Some(Self::TimestampMicrosecond),
            DataType::Decimal128(_, _) => Some(Self::Decimal128),
            DataType::Decimal256(_, _) => Some(Self::Decimal256),
            DataType::Utf8 => Some(Self::Utf8),
            DataType::Binary => Some(Self::Binary),
            _ => None,
        }
    }

    /// Whether this output kind uses variable-length payload encoding.
    pub(super) fn is_variable_len(self) -> bool {
        matches!(self, Self::Utf8 | Self::Binary)
    }

    /// Fixed element size for non-variable-length output kinds.
    pub(super) fn value_size_bytes(self) -> usize {
        match self {
            Self::Int8 => 1,
            Self::Int16 => 2,
            Self::Int32 => 4,
            Self::Int64 => 8,
            Self::LargeInt => 16,
            Self::Float32 => 4,
            Self::Float64 => 8,
            Self::Boolean => 1,
            Self::Date32 => 4,
            Self::TimestampMicrosecond => 8,
            Self::Decimal128 | Self::Decimal256 => {
                panic!("decimal output column value size depends on logical type")
            }
            Self::Utf8 | Self::Binary => {
                panic!("variable-length output column does not have fixed value size")
            }
        }
    }

    /// Bit width used by StarRocks RLE decoder for this output kind.
    pub(super) fn rle_bit_width(self, elem_size: usize) -> usize {
        match self {
            Self::Boolean => 1,
            _ => elem_size * 8,
        }
    }

    /// Human-readable Arrow type label for error messages.
    pub(super) fn arrow_type_name(self) -> &'static str {
        match self {
            Self::Int8 => "Int8",
            Self::Int16 => "Int16",
            Self::Int32 => "Int32",
            Self::Int64 => "Int64",
            Self::LargeInt => "FixedSizeBinary(16)",
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Boolean => "Boolean",
            Self::Date32 => "Date32",
            Self::TimestampMicrosecond => "Timestamp(Microsecond,None)",
            Self::Decimal128 => "Decimal128",
            Self::Decimal256 => "Decimal256",
            Self::Utf8 => "Utf8",
            Self::Binary => "Binary",
        }
    }
}

/// Materialized output values collected from all scanned segments.
pub(super) enum OutputColumnData {
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    LargeInt(Vec<i128>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Boolean(Vec<bool>),
    Date32(Vec<i32>),
    TimestampMicrosecond(Vec<i64>),
    Decimal128(Vec<i128>),
    Decimal256(Vec<i256>),
    Utf8(Vec<String>),
    Binary(Vec<Vec<u8>>),
}

impl OutputColumnData {
    /// Create a typed output buffer for a projected column.
    pub(super) fn with_capacity(kind: OutputColumnKind, capacity: usize) -> Self {
        match kind {
            OutputColumnKind::Int8 => Self::Int8(Vec::with_capacity(capacity)),
            OutputColumnKind::Int16 => Self::Int16(Vec::with_capacity(capacity)),
            OutputColumnKind::Int32 => Self::Int32(Vec::with_capacity(capacity)),
            OutputColumnKind::Int64 => Self::Int64(Vec::with_capacity(capacity)),
            OutputColumnKind::LargeInt => Self::LargeInt(Vec::with_capacity(capacity)),
            OutputColumnKind::Float32 => Self::Float32(Vec::with_capacity(capacity)),
            OutputColumnKind::Float64 => Self::Float64(Vec::with_capacity(capacity)),
            OutputColumnKind::Boolean => Self::Boolean(Vec::with_capacity(capacity)),
            OutputColumnKind::Date32 => Self::Date32(Vec::with_capacity(capacity)),
            OutputColumnKind::TimestampMicrosecond => {
                Self::TimestampMicrosecond(Vec::with_capacity(capacity))
            }
            OutputColumnKind::Decimal128 => Self::Decimal128(Vec::with_capacity(capacity)),
            OutputColumnKind::Decimal256 => Self::Decimal256(Vec::with_capacity(capacity)),
            OutputColumnKind::Utf8 => Self::Utf8(Vec::with_capacity(capacity)),
            OutputColumnKind::Binary => Self::Binary(Vec::with_capacity(capacity)),
        }
    }

    /// Append fixed-width payload bytes decoded from a data page.
    pub(super) fn append_from_bytes(
        &mut self,
        bytes: &[u8],
        elem_size: usize,
        segment_path: &str,
        output_name: &str,
    ) -> Result<(), String> {
        match self {
            Self::Int8(values) => {
                if elem_size != 1 {
                    return Err(format!(
                        "unsupported elem_size for Int8 output column: output_column={}, segment={}, elem_size={}, expected=1",
                        output_name, segment_path, elem_size
                    ));
                }
                values.extend(bytes.iter().map(|v| *v as i8));
                Ok(())
            }
            Self::Int16(values) => {
                if elem_size != 2 {
                    return Err(format!(
                        "unsupported elem_size for Int16 output column: output_column={}, segment={}, elem_size={}, expected=2",
                        output_name, segment_path, elem_size
                    ));
                }
                if bytes.len() % 2 != 0 {
                    return Err(format!(
                        "invalid Int16 value bytes length: output_column={}, segment={}, bytes={}",
                        output_name,
                        segment_path,
                        bytes.len()
                    ));
                }
                for chunk in bytes.chunks_exact(2) {
                    values.push(i16::from_le_bytes(
                        chunk
                            .try_into()
                            .map_err(|_| "convert Int16 bytes failed".to_string())?,
                    ));
                }
                Ok(())
            }
            Self::Int32(values) => {
                if elem_size != 4 {
                    return Err(format!(
                        "unsupported elem_size for Int32 output column: output_column={}, segment={}, elem_size={}, expected=4",
                        output_name, segment_path, elem_size
                    ));
                }
                if bytes.len() % 4 != 0 {
                    return Err(format!(
                        "invalid Int32 value bytes length: output_column={}, segment={}, bytes={}",
                        output_name,
                        segment_path,
                        bytes.len()
                    ));
                }
                for chunk in bytes.chunks_exact(4) {
                    values.push(i32::from_le_bytes(
                        chunk
                            .try_into()
                            .map_err(|_| "convert Int32 bytes failed".to_string())?,
                    ));
                }
                Ok(())
            }
            Self::Date32(values) => {
                if elem_size != 4 {
                    return Err(format!(
                        "unsupported elem_size for Date32 output column: output_column={}, segment={}, elem_size={}, expected=4",
                        output_name, segment_path, elem_size
                    ));
                }
                if bytes.len() % 4 != 0 {
                    return Err(format!(
                        "invalid Date32 value bytes length: output_column={}, segment={}, bytes={}",
                        output_name,
                        segment_path,
                        bytes.len()
                    ));
                }
                for chunk in bytes.chunks_exact(4) {
                    let julian = i32::from_le_bytes(
                        chunk
                            .try_into()
                            .map_err(|_| "convert Date32 bytes failed".to_string())?,
                    );
                    values.push(convert_date32_julian_to_unix_days(
                        julian,
                        segment_path,
                        output_name,
                    )?);
                }
                Ok(())
            }
            Self::Int64(values) => {
                if elem_size != 8 {
                    return Err(format!(
                        "unsupported elem_size for Int64 output column: output_column={}, segment={}, elem_size={}, expected=8",
                        output_name, segment_path, elem_size
                    ));
                }
                if bytes.len() % 8 != 0 {
                    return Err(format!(
                        "invalid Int64 value bytes length: output_column={}, segment={}, bytes={}",
                        output_name,
                        segment_path,
                        bytes.len()
                    ));
                }
                for chunk in bytes.chunks_exact(8) {
                    values.push(i64::from_le_bytes(
                        chunk
                            .try_into()
                            .map_err(|_| "convert Int64 bytes failed".to_string())?,
                    ));
                }
                Ok(())
            }
            Self::LargeInt(values) => {
                if elem_size != 16 {
                    return Err(format!(
                        "unsupported elem_size for FixedSizeBinary(16) output column: output_column={}, segment={}, elem_size={}, expected=16",
                        output_name, segment_path, elem_size
                    ));
                }
                if bytes.len() % 16 != 0 {
                    return Err(format!(
                        "invalid FixedSizeBinary(16) value bytes length: output_column={}, segment={}, bytes={}",
                        output_name,
                        segment_path,
                        bytes.len()
                    ));
                }
                for chunk in bytes.chunks_exact(16) {
                    values.push(i128::from_le_bytes(
                        chunk
                            .try_into()
                            .map_err(|_| "convert LARGEINT bytes failed".to_string())?,
                    ));
                }
                Ok(())
            }
            Self::TimestampMicrosecond(values) => {
                if elem_size != 8 {
                    return Err(format!(
                        "unsupported elem_size for Timestamp(Microsecond,None) output column: output_column={}, segment={}, elem_size={}, expected=8",
                        output_name, segment_path, elem_size
                    ));
                }
                if bytes.len() % 8 != 0 {
                    return Err(format!(
                        "invalid Timestamp(Microsecond,None) value bytes length: output_column={}, segment={}, bytes={}",
                        output_name,
                        segment_path,
                        bytes.len()
                    ));
                }
                for chunk in bytes.chunks_exact(8) {
                    let encoded = i64::from_le_bytes(
                        chunk
                            .try_into()
                            .map_err(|_| "convert DATETIME bytes failed".to_string())?,
                    );
                    values.push(convert_datetime_storage_to_unix_micros(
                        encoded,
                        segment_path,
                        output_name,
                    )?);
                }
                Ok(())
            }
            Self::Decimal128(values) => {
                if elem_size != 4 && elem_size != 8 && elem_size != 16 {
                    return Err(format!(
                        "unsupported elem_size for Decimal128 output column: output_column={}, segment={}, elem_size={}, expected=[4,8,16]",
                        output_name, segment_path, elem_size
                    ));
                }
                if bytes.len() % elem_size != 0 {
                    return Err(format!(
                        "invalid Decimal128 value bytes length: output_column={}, segment={}, bytes={}, elem_size={}",
                        output_name,
                        segment_path,
                        bytes.len(),
                        elem_size
                    ));
                }
                for chunk in bytes.chunks_exact(elem_size) {
                    let value =
                        match elem_size {
                            4 => {
                                i32::from_le_bytes(chunk.try_into().map_err(|_| {
                                    "convert Decimal128(i32) bytes failed".to_string()
                                })?) as i128
                            }
                            8 => {
                                i64::from_le_bytes(chunk.try_into().map_err(|_| {
                                    "convert Decimal128(i64) bytes failed".to_string()
                                })?) as i128
                            }
                            16 => i128::from_le_bytes(chunk.try_into().map_err(|_| {
                                "convert Decimal128(i128) bytes failed".to_string()
                            })?),
                            _ => unreachable!(),
                        };
                    values.push(value);
                }
                Ok(())
            }
            Self::Decimal256(values) => {
                if elem_size != 32 {
                    return Err(format!(
                        "unsupported elem_size for Decimal256 output column: output_column={}, segment={}, elem_size={}, expected=32",
                        output_name, segment_path, elem_size
                    ));
                }
                if bytes.len() % elem_size != 0 {
                    return Err(format!(
                        "invalid Decimal256 value bytes length: output_column={}, segment={}, bytes={}, elem_size={}",
                        output_name,
                        segment_path,
                        bytes.len(),
                        elem_size
                    ));
                }
                for chunk in bytes.chunks_exact(elem_size) {
                    values.push(i256::from_le_bytes(
                        chunk
                            .try_into()
                            .map_err(|_| "convert Decimal256(i256) bytes failed".to_string())?,
                    ));
                }
                Ok(())
            }
            Self::Float32(values) => {
                if elem_size != 4 {
                    return Err(format!(
                        "unsupported elem_size for Float32 output column: output_column={}, segment={}, elem_size={}, expected=4",
                        output_name, segment_path, elem_size
                    ));
                }
                if bytes.len() % 4 != 0 {
                    return Err(format!(
                        "invalid Float32 value bytes length: output_column={}, segment={}, bytes={}",
                        output_name,
                        segment_path,
                        bytes.len()
                    ));
                }
                for chunk in bytes.chunks_exact(4) {
                    values.push(f32::from_le_bytes(
                        chunk
                            .try_into()
                            .map_err(|_| "convert Float32 bytes failed".to_string())?,
                    ));
                }
                Ok(())
            }
            Self::Float64(values) => {
                if elem_size != 8 {
                    return Err(format!(
                        "unsupported elem_size for Float64 output column: output_column={}, segment={}, elem_size={}, expected=8",
                        output_name, segment_path, elem_size
                    ));
                }
                if bytes.len() % 8 != 0 {
                    return Err(format!(
                        "invalid Float64 value bytes length: output_column={}, segment={}, bytes={}",
                        output_name,
                        segment_path,
                        bytes.len()
                    ));
                }
                for chunk in bytes.chunks_exact(8) {
                    values.push(f64::from_le_bytes(
                        chunk
                            .try_into()
                            .map_err(|_| "convert Float64 bytes failed".to_string())?,
                    ));
                }
                Ok(())
            }
            Self::Boolean(values) => {
                if elem_size != 1 {
                    return Err(format!(
                        "unsupported elem_size for Boolean output column: output_column={}, segment={}, elem_size={}, expected=1",
                        output_name, segment_path, elem_size
                    ));
                }
                values.extend(bytes.iter().map(|v| *v != 0));
                Ok(())
            }
            Self::Utf8(_) => Err(format!(
                "unexpected fixed-width byte append for Utf8 output column: output_column={}, segment={}",
                output_name, segment_path
            )),
            Self::Binary(_) => Err(format!(
                "unexpected fixed-width byte append for Binary output column: output_column={}, segment={}",
                output_name, segment_path
            )),
        }
    }

    /// Append variable-length payload values decoded from a data page.
    pub(super) fn append_variable_values(
        &mut self,
        values: Vec<Vec<u8>>,
        segment_path: &str,
        output_name: &str,
        trim_char_zero: bool,
    ) -> Result<(), String> {
        match self {
            Self::Utf8(out) => {
                for raw in values {
                    let view = if trim_char_zero {
                        let end = raw.iter().position(|b| *b == 0).unwrap_or(raw.len());
                        &raw[..end]
                    } else {
                        raw.as_slice()
                    };
                    let s = std::str::from_utf8(view).map_err(|e| {
                        format!(
                            "invalid UTF-8 value for rust native reader: output_column={}, segment={}, error={}",
                            output_name, segment_path, e
                        )
                    })?;
                    out.push(s.to_string());
                }
                Ok(())
            }
            Self::Binary(out) => {
                out.extend(values);
                Ok(())
            }
            _ => Err(format!(
                "unexpected variable-length value append for fixed-width output column: output_column={}, segment={}",
                output_name, segment_path
            )),
        }
    }

    /// Number of logical values appended so far.
    #[allow(dead_code)]
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Int8(values) => values.len(),
            Self::Int16(values) => values.len(),
            Self::Int32(values) => values.len(),
            Self::Int64(values) => values.len(),
            Self::LargeInt(values) => values.len(),
            Self::Float32(values) => values.len(),
            Self::Float64(values) => values.len(),
            Self::Boolean(values) => values.len(),
            Self::Date32(values) => values.len(),
            Self::TimestampMicrosecond(values) => values.len(),
            Self::Decimal128(values) => values.len(),
            Self::Decimal256(values) => values.len(),
            Self::Utf8(values) => values.len(),
            Self::Binary(values) => values.len(),
        }
    }

    /// Finalize the buffered values into an Arrow array.
    pub(super) fn into_array(
        self,
        null_flags: &[u8],
        has_null: bool,
        output_name: &str,
        decimal_meta: Option<DecimalOutputMeta>,
    ) -> Result<ArrayRef, String> {
        match self {
            Self::Int8(values) => build_int8_array(values, null_flags, has_null, output_name),
            Self::Int16(values) => build_int16_array(values, null_flags, has_null, output_name),
            Self::Int32(values) => build_int32_array(values, null_flags, has_null, output_name),
            Self::Int64(values) => build_int64_array(values, null_flags, has_null, output_name),
            Self::LargeInt(values) => {
                build_largeint_array(values, null_flags, has_null, output_name)
            }
            Self::Float32(values) => build_float32_array(values, null_flags, has_null, output_name),
            Self::Float64(values) => build_float64_array(values, null_flags, has_null, output_name),
            Self::Boolean(values) => build_boolean_array(values, null_flags, has_null, output_name),
            Self::Date32(values) => build_date32_array(values, null_flags, has_null, output_name),
            Self::TimestampMicrosecond(values) => {
                build_timestamp_microsecond_array(values, null_flags, has_null, output_name)
            }
            Self::Decimal128(values) => {
                build_decimal128_array(values, null_flags, has_null, output_name, decimal_meta)
            }
            Self::Decimal256(values) => {
                build_decimal256_array(values, null_flags, has_null, output_name, decimal_meta)
            }
            Self::Utf8(values) => build_utf8_array(values, null_flags, has_null, output_name),
            Self::Binary(values) => build_binary_array(values, null_flags, has_null, output_name),
        }
    }
}

macro_rules! build_array_with_nulls {
    ($name:ident, $array_ty:ty, $value_ty:ty, $type_label:literal) => {
        fn $name(
            values: Vec<$value_ty>,
            null_flags: &[u8],
            has_null: bool,
            output_name: &str,
        ) -> Result<ArrayRef, String> {
            if !has_null {
                return Ok(Arc::new(<$array_ty>::from(values)));
            }
            if values.len() != null_flags.len() {
                return Err(format!(
                    "null flag length mismatch for {} output column: output_column={}, values={}, null_flags={}",
                    $type_label,
                    output_name,
                    values.len(),
                    null_flags.len()
                ));
            }
            let mut values_with_null = Vec::with_capacity(values.len());
            for (idx, value) in values.into_iter().enumerate() {
                match null_flags[idx] {
                    0 => values_with_null.push(Some(value)),
                    1 => values_with_null.push(None),
                    other => {
                        return Err(format!(
                            "invalid null flag value for {} output column: output_column={}, row_index={}, null_flag={}, expected=[0,1]",
                            $type_label, output_name, idx, other
                        ));
                    }
                }
            }
            Ok(Arc::new(<$array_ty>::from(values_with_null)))
        }
    };
}

build_array_with_nulls!(build_int8_array, Int8Array, i8, "Int8");
build_array_with_nulls!(build_int16_array, Int16Array, i16, "Int16");
build_array_with_nulls!(build_int32_array, Int32Array, i32, "Int32");
build_array_with_nulls!(build_int64_array, Int64Array, i64, "Int64");
build_array_with_nulls!(build_float32_array, Float32Array, f32, "Float32");
build_array_with_nulls!(build_float64_array, Float64Array, f64, "Float64");
build_array_with_nulls!(build_boolean_array, BooleanArray, bool, "Boolean");
build_array_with_nulls!(build_date32_array, Date32Array, i32, "Date32");
build_array_with_nulls!(
    build_timestamp_microsecond_array,
    TimestampMicrosecondArray,
    i64,
    "Timestamp(Microsecond,None)"
);

fn build_largeint_array(
    values: Vec<i128>,
    null_flags: &[u8],
    has_null: bool,
    output_name: &str,
) -> Result<ArrayRef, String> {
    if !has_null {
        let values = values.into_iter().map(Some).collect::<Vec<_>>();
        return largeint::array_from_i128(&values);
    }
    if values.len() != null_flags.len() {
        return Err(format!(
            "null flag length mismatch for FixedSizeBinary(16) output column: output_column={}, values={}, null_flags={}",
            output_name,
            values.len(),
            null_flags.len()
        ));
    }
    let mut values_with_null = Vec::with_capacity(values.len());
    for (idx, value) in values.into_iter().enumerate() {
        match null_flags[idx] {
            0 => values_with_null.push(Some(value)),
            1 => values_with_null.push(None),
            other => {
                return Err(format!(
                    "invalid null flag value for FixedSizeBinary(16) output column: output_column={}, row_index={}, null_flag={}, expected=[0,1]",
                    output_name, idx, other
                ));
            }
        }
    }
    largeint::array_from_i128(&values_with_null)
}

fn build_utf8_array(
    values: Vec<String>,
    null_flags: &[u8],
    has_null: bool,
    output_name: &str,
) -> Result<ArrayRef, String> {
    if !has_null {
        return Ok(Arc::new(StringArray::from(values)));
    }
    if values.len() != null_flags.len() {
        return Err(format!(
            "null flag length mismatch for Utf8 output column: output_column={}, values={}, null_flags={}",
            output_name,
            values.len(),
            null_flags.len()
        ));
    }
    let mut values_with_null = Vec::with_capacity(values.len());
    for (idx, value) in values.into_iter().enumerate() {
        match null_flags[idx] {
            0 => values_with_null.push(Some(value)),
            1 => values_with_null.push(None),
            other => {
                return Err(format!(
                    "invalid null flag value for Utf8 output column: output_column={}, row_index={}, null_flag={}, expected=[0,1]",
                    output_name, idx, other
                ));
            }
        }
    }
    Ok(Arc::new(StringArray::from(values_with_null)))
}

fn build_binary_array(
    values: Vec<Vec<u8>>,
    null_flags: &[u8],
    has_null: bool,
    output_name: &str,
) -> Result<ArrayRef, String> {
    if !has_null {
        let mut builder = BinaryBuilder::new();
        for value in values {
            builder.append_value(value);
        }
        return Ok(Arc::new(builder.finish()));
    }
    if values.len() != null_flags.len() {
        return Err(format!(
            "null flag length mismatch for Binary output column: output_column={}, values={}, null_flags={}",
            output_name,
            values.len(),
            null_flags.len()
        ));
    }
    let mut builder = BinaryBuilder::new();
    for (idx, value) in values.into_iter().enumerate() {
        match null_flags[idx] {
            0 => builder.append_value(value),
            1 => builder.append_null(),
            other => {
                return Err(format!(
                    "invalid null flag value for Binary output column: output_column={}, row_index={}, null_flag={}, expected=[0,1]",
                    output_name, idx, other
                ));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Per-column scan state spanning all segment files in the read plan.
#[allow(dead_code)]
pub(super) struct OutputColumnState {
    pub(super) output_index: usize,
    pub(super) output_name: String,
    pub(super) schema_unique_id: u32,
    pub(super) expected_logical_type: i32,
    pub(super) trim_char_zero: bool,
    pub(super) kind: OutputColumnKind,
    pub(super) decimal_meta: Option<DecimalOutputMeta>,
    pub(super) data: OutputColumnData,
    pub(super) null_flags: Vec<u8>,
    pub(super) has_null: bool,
}

/// Build projected column states from plan metadata and output schema.
#[allow(dead_code)]
pub(super) fn build_output_columns(
    plan: &StarRocksNativeReadPlan,
    output_schema: &SchemaRef,
) -> Result<Vec<OutputColumnState>, String> {
    let mut columns = Vec::with_capacity(plan.projected_columns.len());
    for projected in &plan.projected_columns {
        let field = output_schema
            .fields()
            .get(projected.output_index)
            .ok_or_else(|| {
                format!(
                    "output schema index out of bounds for projected column: output_column={}, output_index={}",
                    projected.output_name, projected.output_index
                )
            })?;
        let kind = OutputColumnKind::from_arrow_type(field.data_type()).ok_or_else(|| {
            format!(
                "unsupported output data type for rust native starrocks reader: output_column={}, data_type={:?}, supported=[Int8,Int16,Int32,Int64,FixedSizeBinary(16),Float32,Float64,Boolean,Date32,Timestamp(Microsecond,None),Decimal128,Decimal256,Utf8,Binary]",
                projected.output_name,
                field.data_type()
            )
        })?;
        let expected_logical_type =
            expected_logical_type_from_schema_type(&projected.schema_type).ok_or_else(|| {
                format!(
                    "unsupported projected schema type in native reader: output_column={}, schema_type={}",
                    projected.output_name, projected.schema_type
                )
            })?;
        let decimal_meta = decimal_output_meta_from_arrow_type(field.data_type());
        columns.push(OutputColumnState {
            output_index: projected.output_index,
            output_name: projected.output_name.clone(),
            schema_unique_id: projected.schema_unique_id,
            expected_logical_type,
            trim_char_zero: is_char_schema_type(&projected.schema_type),
            kind,
            decimal_meta,
            data: OutputColumnData::with_capacity(kind, plan.estimated_rows as usize),
            null_flags: Vec::with_capacity(plan.estimated_rows as usize),
            has_null: false,
        });
    }
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::super::constants::{DATE_UNIX_EPOCH_JULIAN, TIMESTAMP_BITS, USECS_PER_DAY_I64};
    use super::*;
    use arrow::array::{
        Array, Date32Array, Decimal128Array, Int64Array, TimestampMicrosecondArray,
    };

    fn encode_internal_datetime(julian_day: u64, micros_of_day: u64) -> [u8; 8] {
        ((julian_day << TIMESTAMP_BITS) | micros_of_day).to_le_bytes()
    }

    #[test]
    fn build_int64_array_applies_null_flags() {
        let array = build_int64_array(vec![10, 20, 30], &[0, 1, 0], true, "c1")
            .expect("build int64 array with nulls");
        let int64 = array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("downcast to Int64Array");
        assert_eq!(int64.len(), 3);
        assert!(!int64.is_null(0));
        assert!(int64.is_null(1));
        assert_eq!(int64.value(2), 30);
    }

    #[test]
    fn append_date32_values_converts_from_julian_days() {
        let mut data = OutputColumnData::with_capacity(OutputColumnKind::Date32, 2);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&DATE_UNIX_EPOCH_JULIAN.to_le_bytes());
        bytes.extend_from_slice(&(DATE_UNIX_EPOCH_JULIAN + 1).to_le_bytes());
        data.append_from_bytes(&bytes, 4, "segment.dat", "c_date")
            .expect("append date32 bytes");
        match data {
            OutputColumnData::Date32(values) => assert_eq!(values, vec![0, 1]),
            _ => panic!("unexpected output column data variant"),
        }
    }

    #[test]
    fn append_datetime_values_converts_to_unix_micros() {
        let mut data = OutputColumnData::with_capacity(OutputColumnKind::TimestampMicrosecond, 2);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&encode_internal_datetime(DATE_UNIX_EPOCH_JULIAN as u64, 0));
        bytes.extend_from_slice(&encode_internal_datetime(
            (DATE_UNIX_EPOCH_JULIAN as u64) + 1,
            1,
        ));
        data.append_from_bytes(&bytes, 8, "segment.dat", "c_datetime")
            .expect("append datetime bytes");
        match data {
            OutputColumnData::TimestampMicrosecond(values) => {
                assert_eq!(values, vec![0, USECS_PER_DAY_I64 + 1]);
            }
            _ => panic!("unexpected output column data variant"),
        }
    }

    #[test]
    fn append_datetime_rejects_invalid_micros_of_day() {
        let mut data = OutputColumnData::with_capacity(OutputColumnKind::TimestampMicrosecond, 1);
        let bytes =
            encode_internal_datetime(DATE_UNIX_EPOCH_JULIAN as u64, USECS_PER_DAY_I64 as u64);
        let err = data
            .append_from_bytes(&bytes, 8, "segment.dat", "c_datetime")
            .expect_err("invalid datetime micros should fail");
        assert!(
            err.contains("invalid DATETIME microseconds-of-day"),
            "err={err}"
        );
    }

    #[test]
    fn build_temporal_arrays_apply_null_flags() {
        let date_array = build_date32_array(vec![0, 1], &[0, 1], true, "c_date")
            .expect("build date32 array with nulls");
        let date32 = date_array
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("downcast to Date32Array");
        assert_eq!(date32.len(), 2);
        assert!(!date32.is_null(0));
        assert!(date32.is_null(1));

        let ts_array =
            build_timestamp_microsecond_array(vec![100, 200], &[0, 1], true, "c_datetime")
                .expect("build timestamp array with nulls");
        let ts = ts_array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("downcast to TimestampMicrosecondArray");
        assert_eq!(ts.len(), 2);
        assert!(!ts.is_null(0));
        assert!(ts.is_null(1));
        assert_eq!(ts.value(0), 100);
    }

    #[test]
    fn append_decimal_values_from_4_8_16_byte_storage() {
        let mut data = OutputColumnData::with_capacity(OutputColumnKind::Decimal128, 3);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(-7_i32).to_le_bytes());
        bytes.extend_from_slice(&(123456789_i64).to_le_bytes());
        bytes.extend_from_slice(&(-1234567890123456789_i128).to_le_bytes());

        data.append_from_bytes(&bytes[0..4], 4, "segment.dat", "c_dec")
            .expect("append decimal32 bytes");
        data.append_from_bytes(&bytes[4..12], 8, "segment.dat", "c_dec")
            .expect("append decimal64 bytes");
        data.append_from_bytes(&bytes[12..28], 16, "segment.dat", "c_dec")
            .expect("append decimal128 bytes");

        match data {
            OutputColumnData::Decimal128(values) => {
                assert_eq!(values, vec![-7, 123456789, -1234567890123456789]);
            }
            _ => panic!("unexpected output column data variant"),
        }
    }

    #[test]
    fn build_decimal128_array_applies_precision_scale_and_nulls() {
        let array = build_decimal128_array(
            vec![100, 200],
            &[0, 1],
            true,
            "c_dec",
            Some(DecimalOutputMeta {
                precision: 9,
                scale: 2,
            }),
        )
        .expect("build decimal128 array with nulls");
        let arr = array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("downcast Decimal128Array");
        assert_eq!(arr.len(), 2);
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
        assert_eq!(arr.value(0), 100);
        assert_eq!(arr.precision(), 9);
        assert_eq!(arr.scale(), 2);
    }
}
