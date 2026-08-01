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

//! Arrow-only scalar segment kernel for frozen StarRocks direct splits.
//!
//! This deliberately starts with a strict, single-data-page PLAIN closure. A
//! segment requiring an unimplemented index or encoding fails the
//! attempt rather than selecting an RPC reader or looking for another tablet.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::compute::concat;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::direct::StarRocksDirectColumnBinding;

use super::page::{
    StarRocksIndexPageNodeType, decode_binary_dictionary_page, decode_binary_dictionary_values,
    decode_binary_plain_values, decode_bitshuffle_fixed_values, decode_data_page,
    decode_fixed_plain_values, decode_fixed_rle_values, decode_index_page, page_slice,
};
use super::segment::{
    StarRocksLogicalType, StarRocksPageEncoding, StarRocksPagePointer, StarRocksSegmentColumnMeta,
    StarRocksSegmentFooter,
};

const STARROCKS_UNIX_EPOCH_JULIAN: i64 = 2_440_588;
const STARROCKS_DATETIME_TIME_BITS: u32 = 40;
const STARROCKS_DATETIME_TIME_MASK: u64 = (1_u64 << STARROCKS_DATETIME_TIME_BITS) - 1;
const STARROCKS_MICROS_PER_DAY: i64 = 86_400_000_000;

/// Decode one fully loaded immutable segment. The caller must obtain the exact
/// frozen segment object through the startup-local storage binding.
pub(crate) fn decode_plain_segment(
    segment_path: &str,
    segment: &[u8],
    footer: &StarRocksSegmentFooter,
    output_schema: SchemaRef,
    bindings: &[StarRocksDirectColumnBinding],
) -> Result<RecordBatch, ConnectorError> {
    if output_schema.fields().len() != bindings.len() {
        return Err(corrupt(
            "StarRocks direct output schema and bindings differ",
        ));
    }
    let mut arrays = Vec::with_capacity(bindings.len());
    for (output_index, binding) in bindings.iter().enumerate() {
        if binding.output_index != output_index {
            return Err(corrupt("StarRocks direct output mapping is not contiguous"));
        }
        let field = output_schema
            .fields()
            .get(output_index)
            .ok_or_else(|| corrupt("StarRocks direct output field is missing"))?;
        if field.name() != binding.name.as_ref() || field.is_nullable() != binding.nullable {
            return Err(corrupt(
                "StarRocks direct output field differs from frozen mapping",
            ));
        }
        arrays.push(decode_frozen_column(
            segment_path,
            segment,
            footer,
            binding,
            field.data_type(),
        )?);
    }
    RecordBatch::try_new(output_schema, arrays)
        .map_err(|_| corrupt("StarRocks direct Arrow batch does not match frozen schema"))
}

/// Decode one physical column from an already frozen segment.  This is used
/// only for provider-private storage metadata semantics such as delete
/// predicates; the public reader always returns the frozen output projection.
pub(crate) fn decode_frozen_column(
    segment_path: &str,
    segment: &[u8],
    footer: &StarRocksSegmentFooter,
    binding: &StarRocksDirectColumnBinding,
    output_type: &DataType,
) -> Result<ArrayRef, ConnectorError> {
    let column = footer
        .columns
        .iter()
        .find(|column| column.unique_id == u32::try_from(binding.unique_id).ok());
    if let Some(column) = column {
        decode_column(
            segment_path,
            segment,
            column,
            output_type,
            footer.num_rows as usize,
        )
    } else {
        default_array(binding, output_type, footer.num_rows as usize)
    }
}

fn default_array(
    binding: &StarRocksDirectColumnBinding,
    data_type: &DataType,
    rows: usize,
) -> Result<ArrayRef, ConnectorError> {
    let Some(default) = binding.default_value.as_deref() else {
        if binding.nullable {
            return Ok(arrow::array::new_null_array(data_type, rows));
        }
        return Err(corrupt(
            "StarRocks historical segment omits a frozen non-nullable column without a default",
        ));
    };
    let text = std::str::from_utf8(default)
        .map_err(|_| corrupt("StarRocks column default is not UTF-8"))?
        .trim();
    if text.eq_ignore_ascii_case("null") {
        if !binding.nullable {
            return Err(corrupt("StarRocks non-nullable column has a NULL default"));
        }
        return Ok(arrow::array::new_null_array(data_type, rows));
    }
    let text = strip_wrapping_quotes(text);
    match data_type {
        DataType::Int8 => Ok(Arc::new(Int8Array::from(vec![
            Some(
                text.parse::<i8>()
                    .map_err(|_| corrupt("invalid StarRocks TINYINT default"))?,
            );
            rows
        ]))),
        DataType::Int16 => Ok(Arc::new(Int16Array::from(vec![
            Some(
                text.parse::<i16>()
                    .map_err(|_| corrupt("invalid StarRocks SMALLINT default"))?,
            );
            rows
        ]))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(vec![
            Some(
                text.parse::<i64>()
                    .map_err(|_| corrupt("invalid StarRocks BIGINT default"))?,
            );
            rows
        ]))),
        DataType::Int32 => Ok(Arc::new(Int32Array::from(vec![
            Some(
                text.parse::<i32>()
                    .map_err(|_| corrupt("invalid StarRocks INT default"))?,
            );
            rows
        ]))),
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(vec![
            Some(match text {
                "0" | "false" | "FALSE" => false,
                "1" | "true" | "TRUE" => true,
                _ => return Err(corrupt("invalid StarRocks BOOLEAN default")),
            });
            rows
        ]))),
        DataType::Float32 => Ok(Arc::new(Float32Array::from(vec![
            Some(
                text.parse::<f32>()
                    .map_err(|_| corrupt("invalid StarRocks FLOAT default"))?,
            );
            rows
        ]))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(vec![
            Some(
                text.parse::<f64>()
                    .map_err(|_| corrupt("invalid StarRocks DOUBLE default"))?,
            );
            rows
        ]))),
        DataType::Utf8 => Ok(Arc::new(StringArray::from(vec![Some(text); rows]))),
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(rows, rows.saturating_mul(text.len()));
            for _ in 0..rows {
                builder.append_value(text.as_bytes());
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => Ok(Arc::new(Date32Array::from(vec![
            Some(parse_date_default(
                text
            )?,);
            rows
        ]))),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Ok(Arc::new(TimestampMicrosecondArray::from(vec![
                Some(
                    parse_timestamp_default(text)?,
                );
                rows
            ])))
        }
        DataType::Decimal128(precision, scale) => {
            let values = vec![Some(parse_decimal_default(text, *precision, *scale)?); rows];
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|_| corrupt("StarRocks DECIMAL default exceeds frozen precision"))?;
            Ok(Arc::new(array))
        }
        _ => Err(unsupported(
            "StarRocks default literal type is not implemented",
        )),
    }
}

fn parse_date_default(text: &str) -> Result<i32, ConnectorError> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map(|date| date.num_days_from_ce() - 719_163)
        .map_err(|_| corrupt("invalid StarRocks DATE default"))
}

fn parse_timestamp_default(text: &str) -> Result<i64, ConnectorError> {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S"))
        .map(|timestamp| timestamp.and_utc().timestamp_micros())
        .map_err(|_| corrupt("invalid StarRocks DATETIME default"))
}

fn parse_decimal_default(text: &str, precision: u8, scale: i8) -> Result<i128, ConnectorError> {
    let (negative, text) = match text.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > scale as usize
    {
        return Err(corrupt("invalid StarRocks DECIMAL default"));
    }
    let mut digits = String::with_capacity(whole.len() + scale as usize);
    digits.push_str(whole);
    digits.push_str(fraction);
    for _ in fraction.len()..scale as usize {
        digits.push('0');
    }
    let value = digits
        .parse::<i128>()
        .map_err(|_| corrupt("invalid StarRocks DECIMAL default"))?;
    let value = if negative { -value } else { value };
    let significant = digits.trim_start_matches('0').len().max(1);
    if significant > precision as usize {
        return Err(corrupt(
            "StarRocks DECIMAL default exceeds frozen precision",
        ));
    }
    Ok(value)
}

fn strip_wrapping_quotes(value: &str) -> &str {
    let bytes = value.as_bytes();
    if bytes.len() >= 2 && matches!(bytes[0], b'\'' | b'"') && bytes[0] == bytes[bytes.len() - 1] {
        &value[1..value.len() - 1]
    } else {
        value
    }
}

fn decode_column(
    segment_path: &str,
    segment: &[u8],
    column: &StarRocksSegmentColumnMeta,
    output_type: &DataType,
    expected_rows: usize,
) -> Result<ArrayRef, ConnectorError> {
    let pages = resolve_data_pages(segment_path, segment, column, expected_rows)?;
    let arrays = pages
        .iter()
        .map(|page| {
            decode_single_data_page(
                segment_path,
                segment,
                column,
                output_type,
                &page.pointer,
                page.num_values,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    if arrays.len() == 1 {
        return Ok(Arc::clone(&arrays[0]));
    }
    let arrays = arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>();
    concat(&arrays).map_err(|_| corrupt("cannot concatenate StarRocks direct data pages"))
}

#[derive(Clone)]
struct DataPageReference {
    pointer: StarRocksPagePointer,
    num_values: usize,
}

fn resolve_data_pages(
    segment_path: &str,
    segment: &[u8],
    column: &StarRocksSegmentColumnMeta,
    expected_rows: usize,
) -> Result<Vec<DataPageReference>, ConnectorError> {
    if expected_rows == 0 {
        return Err(corrupt("StarRocks direct segment has zero rows"));
    }
    let root = column
        .ordinal_index_page
        .as_ref()
        .ok_or_else(|| corrupt("StarRocks direct segment is missing its ordinal index"))?;
    if column.ordinal_index_is_data_page {
        return Ok(vec![DataPageReference {
            pointer: root.clone(),
            num_values: expected_rows,
        }]);
    }
    let entries = resolve_ordinal_leaf_entries(segment_path, segment, root, 0)?;
    let mut pages = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let first = decode_ordinal_key(&entry.0)?;
        let next = if let Some(next) = entries.get(index + 1) {
            decode_ordinal_key(&next.0)?
        } else {
            expected_rows as u64
        };
        if first >= next || next > expected_rows as u64 {
            return Err(corrupt("StarRocks ordinal index entries are invalid"));
        }
        pages.push(DataPageReference {
            pointer: entry.1.clone(),
            num_values: usize::try_from(next - first)
                .map_err(|_| corrupt("StarRocks ordinal page value count is out of range"))?,
        });
    }
    if pages.is_empty() || pages.iter().map(|page| page.num_values).sum::<usize>() != expected_rows
    {
        return Err(corrupt(
            "StarRocks ordinal index does not cover the frozen segment rows",
        ));
    }
    Ok(pages)
}

fn resolve_ordinal_leaf_entries(
    segment_path: &str,
    segment: &[u8],
    pointer: &StarRocksPagePointer,
    depth: usize,
) -> Result<Vec<(Vec<u8>, StarRocksPagePointer)>, ConnectorError> {
    const MAX_DEPTH: usize = 16;
    if depth > MAX_DEPTH {
        return Err(corrupt("StarRocks ordinal index exceeds maximum depth"));
    }
    let page = decode_index_page(segment_path, page_slice(segment_path, segment, pointer)?)?;
    match page.node_type {
        StarRocksIndexPageNodeType::Leaf => Ok(page
            .entries
            .into_iter()
            .map(|entry| (entry.key, entry.pointer))
            .collect()),
        StarRocksIndexPageNodeType::Internal => {
            let mut entries = Vec::new();
            for entry in page.entries {
                entries.extend(resolve_ordinal_leaf_entries(
                    segment_path,
                    segment,
                    &entry.pointer,
                    depth + 1,
                )?);
            }
            Ok(entries)
        }
    }
}

fn decode_ordinal_key(key: &[u8]) -> Result<u64, ConnectorError> {
    let raw: [u8; 8] = key
        .try_into()
        .map_err(|_| corrupt("StarRocks ordinal index key is not an unsigned bigint"))?;
    Ok(u64::from_be_bytes(raw))
}

fn decode_single_data_page(
    segment_path: &str,
    segment: &[u8],
    column: &StarRocksSegmentColumnMeta,
    output_type: &DataType,
    pointer: &StarRocksPagePointer,
    expected_values: usize,
) -> Result<ArrayRef, ConnectorError> {
    let encoding = column
        .encoding
        .ok_or_else(|| corrupt("StarRocks direct segment is missing a page encoding"))?;
    if !matches!(
        encoding,
        StarRocksPageEncoding::Plain
            | StarRocksPageEncoding::Rle
            | StarRocksPageEncoding::Dictionary
    ) {
        return Err(unsupported(
            "StarRocks direct segment encoding is not implemented",
        ));
    }
    let bytes = page_slice(segment_path, segment, pointer)?;
    let page = decode_data_page(
        segment_path,
        bytes,
        column
            .compression
            .unwrap_or(super::segment::StarRocksCompression::None),
    )?;
    if page.num_values != expected_values {
        return Err(corrupt(
            "StarRocks data page value count differs from its ordinal index",
        ));
    }
    if output_type.is_null() {
        return Err(unsupported(
            "StarRocks NULL-only output columns are not supported",
        ));
    }
    if page.null_flags.is_some() && !column.nullable {
        return Err(corrupt(
            "StarRocks non-nullable column has a nullable data page",
        ));
    }
    let body = &page.body[..page.body.len() - page.nullmap_size];
    let null_flags = page.null_flags.as_deref();
    let dictionary = if encoding == StarRocksPageEncoding::Dictionary {
        let pointer = column.dictionary_page.as_ref().ok_or_else(|| {
            corrupt("StarRocks dictionary-encoded segment is missing its dictionary page")
        })?;
        let bytes = page_slice(segment_path, segment, pointer)?;
        Some(decode_binary_dictionary_page(
            segment_path,
            bytes,
            column
                .compression
                .unwrap_or(super::segment::StarRocksCompression::None),
        )?)
    } else {
        None
    };
    match (column.logical_type, output_type) {
        (StarRocksLogicalType::Boolean, DataType::Boolean) => {
            let values = decode_fixed_values(body, encoding, page.num_values, 1, 1)?;
            let values = values
                .into_iter()
                .map(|value| match value {
                    0 => Ok(false),
                    1 => Ok(true),
                    _ => Err(corrupt("invalid StarRocks BOOLEAN PLAIN value")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(BooleanArray::from(apply_nulls(
                values, null_flags,
            )?)))
        }
        (StarRocksLogicalType::TinyInt, DataType::Int8) => {
            fixed_i8(body, encoding, page.num_values, null_flags)
        }
        (StarRocksLogicalType::SmallInt, DataType::Int16) => {
            fixed_i16(body, encoding, page.num_values, null_flags)
        }
        (StarRocksLogicalType::Int, DataType::Int32) => {
            fixed_i32(body, encoding, page.num_values, null_flags)
        }
        (StarRocksLogicalType::BigInt, DataType::Int64) => {
            fixed_i64(body, encoding, page.num_values, null_flags)
        }
        (StarRocksLogicalType::Float, DataType::Float32) => {
            fixed_f32(body, encoding, page.num_values, null_flags)
        }
        (StarRocksLogicalType::Double, DataType::Float64) => {
            fixed_f64(body, encoding, page.num_values, null_flags)
        }
        (StarRocksLogicalType::Date, DataType::Date32) => {
            let values = decode_fixed_values(body, encoding, page.num_values, 4, 32)?
                .chunks_exact(4)
                .map(|value| i32::from_le_bytes(value.try_into().expect("fixed date size")))
                .map(decode_starrocks_date)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(Date32Array::from(apply_nulls(
                values, null_flags,
            )?)))
        }
        (StarRocksLogicalType::DateTime, DataType::Timestamp(TimeUnit::Microsecond, None)) => {
            let values = decode_fixed_values(body, encoding, page.num_values, 8, 64)?
                .chunks_exact(8)
                .map(|value| i64::from_le_bytes(value.try_into().expect("fixed datetime size")))
                .map(decode_starrocks_datetime)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(TimestampMicrosecondArray::from(apply_nulls(
                values, null_flags,
            )?)))
        }
        (StarRocksLogicalType::Decimal32, DataType::Decimal128(precision, scale)) => fixed_decimal(
            body,
            encoding,
            page.num_values,
            4,
            *precision,
            *scale,
            null_flags,
        ),
        (StarRocksLogicalType::Decimal64, DataType::Decimal128(precision, scale)) => fixed_decimal(
            body,
            encoding,
            page.num_values,
            8,
            *precision,
            *scale,
            null_flags,
        ),
        (StarRocksLogicalType::Decimal128, DataType::Decimal128(precision, scale)) => {
            fixed_decimal(
                body,
                encoding,
                page.num_values,
                16,
                *precision,
                *scale,
                null_flags,
            )
        }
        (StarRocksLogicalType::Char | StarRocksLogicalType::Varchar, DataType::Utf8) => {
            if encoding == StarRocksPageEncoding::Rle {
                return Err(unsupported(
                    "StarRocks variable-width RLE pages are not supported",
                ));
            }
            let values = if let Some(dictionary) = dictionary.as_deref() {
                decode_binary_dictionary_values(body, page.num_values, dictionary)?
            } else {
                decode_binary_plain_values(body, page.num_values)?
            }
            .into_iter()
            .map(|value| {
                String::from_utf8(value)
                    .map_err(|_| corrupt("invalid UTF-8 StarRocks VARCHAR value"))
            })
            .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(StringArray::from(apply_nulls(
                values, null_flags,
            )?)))
        }
        (
            StarRocksLogicalType::Binary
            | StarRocksLogicalType::VarBinary
            | StarRocksLogicalType::Object
            | StarRocksLogicalType::Hll
            | StarRocksLogicalType::Percentile
            | StarRocksLogicalType::Json,
            DataType::Binary,
        ) => {
            if encoding == StarRocksPageEncoding::Rle {
                return Err(unsupported(
                    "StarRocks variable-width RLE pages are not supported",
                ));
            }
            let values = if let Some(dictionary) = dictionary.as_deref() {
                decode_binary_dictionary_values(body, page.num_values, dictionary)?
            } else {
                decode_binary_plain_values(body, page.num_values)?
            };
            let values = apply_nulls(values, null_flags)?;
            let mut builder = BinaryBuilder::new();
            for value in values {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(unsupported(
            "StarRocks direct physical type does not match a supported Arrow scalar type",
        )),
    }
}

fn decode_starrocks_date(value: i32) -> Result<i32, ConnectorError> {
    i64::from(value)
        .checked_sub(STARROCKS_UNIX_EPOCH_JULIAN)
        .and_then(|days| i32::try_from(days).ok())
        .ok_or_else(|| corrupt("StarRocks direct DATE is outside Arrow Date32 range"))
}

fn decode_starrocks_datetime(value: i64) -> Result<i64, ConnectorError> {
    let value =
        u64::try_from(value).map_err(|_| corrupt("StarRocks direct DATETIME is negative"))?;
    let julian = value >> STARROCKS_DATETIME_TIME_BITS;
    let micros_of_day = value & STARROCKS_DATETIME_TIME_MASK;
    if micros_of_day >= STARROCKS_MICROS_PER_DAY as u64 {
        return Err(corrupt("StarRocks direct DATETIME has invalid time-of-day"));
    }
    let days = i64::try_from(julian)
        .ok()
        .and_then(|julian| julian.checked_sub(STARROCKS_UNIX_EPOCH_JULIAN))
        .ok_or_else(|| corrupt("StarRocks direct DATETIME Julian day is invalid"))?;
    days.checked_mul(STARROCKS_MICROS_PER_DAY)
        .and_then(|base| base.checked_add(micros_of_day as i64))
        .ok_or_else(|| corrupt("StarRocks direct DATETIME overflows Arrow timestamp"))
}

fn fixed_decimal(
    body: &[u8],
    encoding: StarRocksPageEncoding,
    values: usize,
    width: usize,
    precision: u8,
    scale: i8,
    null_flags: Option<&[u8]>,
) -> Result<ArrayRef, ConnectorError> {
    let bytes = decode_fixed_values(body, encoding, values, width, width * 8)?;
    let values = bytes
        .chunks_exact(width)
        .map(|value| match width {
            4 => Ok(i128::from(i32::from_le_bytes(
                value.try_into().expect("fixed decimal32 size"),
            ))),
            8 => Ok(i128::from(i64::from_le_bytes(
                value.try_into().expect("fixed decimal64 size"),
            ))),
            16 => Ok(i128::from_le_bytes(
                value.try_into().expect("fixed decimal128 size"),
            )),
            _ => Err(corrupt("StarRocks direct DECIMAL has an invalid width")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let array = Decimal128Array::from(apply_nulls(values, null_flags)?)
        .with_precision_and_scale(precision, scale)
        .map_err(|_| corrupt("StarRocks direct DECIMAL exceeds its frozen precision"))?;
    Ok(Arc::new(array))
}

fn fixed_i8(
    body: &[u8],
    encoding: StarRocksPageEncoding,
    values: usize,
    null_flags: Option<&[u8]>,
) -> Result<ArrayRef, ConnectorError> {
    Ok(Arc::new(Int8Array::from(apply_nulls(
        decode_fixed_values(body, encoding, values, 1, 8)?
            .into_iter()
            .map(|value| value as i8)
            .collect::<Vec<_>>(),
        null_flags,
    )?)))
}

macro_rules! fixed_values {
    ($name:ident, $array:ident, $type:ty, $size:expr) => {
        fn $name(
            body: &[u8],
            encoding: StarRocksPageEncoding,
            values: usize,
            null_flags: Option<&[u8]>,
        ) -> Result<ArrayRef, ConnectorError> {
            let decoded = decode_fixed_values(body, encoding, values, $size, $size * 8)?;
            let output = decoded
                .chunks_exact($size)
                .map(|value| <$type>::from_le_bytes(value.try_into().expect("fixed chunk size")))
                .collect::<Vec<_>>();
            Ok(Arc::new($array::from(apply_nulls(output, null_flags)?)))
        }
    };
}

fixed_values!(fixed_i16, Int16Array, i16, 2);
fixed_values!(fixed_i32, Int32Array, i32, 4);
fixed_values!(fixed_i64, Int64Array, i64, 8);
fixed_values!(fixed_f32, Float32Array, f32, 4);
fixed_values!(fixed_f64, Float64Array, f64, 8);

fn decode_fixed_values(
    body: &[u8],
    encoding: StarRocksPageEncoding,
    values: usize,
    value_size: usize,
    bit_width: usize,
) -> Result<Vec<u8>, ConnectorError> {
    match encoding {
        StarRocksPageEncoding::Plain => decode_fixed_plain_values(body, values, value_size),
        StarRocksPageEncoding::Rle => decode_fixed_rle_values(body, values, value_size, bit_width),
        StarRocksPageEncoding::BitShuffle => {
            decode_bitshuffle_fixed_values(body, values, value_size)
        }
        StarRocksPageEncoding::Dictionary => Err(unsupported(
            "StarRocks direct fixed-width dictionary pages are not supported",
        )),
    }
}

fn apply_nulls<T>(
    values: Vec<T>,
    null_flags: Option<&[u8]>,
) -> Result<Vec<Option<T>>, ConnectorError> {
    let Some(null_flags) = null_flags else {
        return Ok(values.into_iter().map(Some).collect());
    };
    if null_flags.len() != values.len() || null_flags.iter().any(|flag| *flag > 1) {
        return Err(corrupt(
            "StarRocks nullable page bitmap does not match page values",
        ));
    }
    Ok(values
        .into_iter()
        .zip(null_flags)
        .map(|(value, flag)| (*flag == 0).then_some(value))
        .collect())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int64Array,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{Field, Schema};
    use bytes::Bytes;
    use crc32c::crc32c;
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    struct PageFooter {
        #[prost(int32, optional, tag = "1")]
        page_type: Option<i32>,
        #[prost(uint32, optional, tag = "2")]
        uncompressed_size: Option<u32>,
        #[prost(message, optional, tag = "7")]
        data: Option<DataFooter>,
        #[prost(message, optional, tag = "8")]
        index: Option<IndexFooter>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct DataFooter {
        #[prost(uint64, optional, tag = "2")]
        num_values: Option<u64>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct IndexFooter {
        #[prost(uint32, optional, tag = "1")]
        entries: Option<u32>,
        #[prost(int32, optional, tag = "2")]
        node_type: Option<i32>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Footer {
        #[prost(uint32, optional, tag = "1")]
        version: Option<u32>,
        #[prost(message, repeated, tag = "2")]
        columns: Vec<Column>,
        #[prost(uint32, optional, tag = "3")]
        num_rows: Option<u32>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Column {
        #[prost(uint32, optional, tag = "2")]
        unique_id: Option<u32>,
        #[prost(int32, optional, tag = "3")]
        logical_type: Option<i32>,
        #[prost(int32, optional, tag = "5")]
        encoding: Option<i32>,
        #[prost(int32, optional, tag = "6")]
        compression: Option<i32>,
        #[prost(bool, optional, tag = "7")]
        nullable: Option<bool>,
        #[prost(message, repeated, tag = "8")]
        indexes: Vec<Index>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Index {
        #[prost(int32, optional, tag = "1")]
        index_type: Option<i32>,
        #[prost(message, optional, tag = "7")]
        ordinal: Option<Ordinal>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Ordinal {
        #[prost(message, optional, tag = "1")]
        root: Option<Btree>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Btree {
        #[prost(message, optional, tag = "1")]
        page: Option<Pointer>,
        #[prost(bool, optional, tag = "2")]
        root_is_data: Option<bool>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Pointer {
        #[prost(uint64, optional, tag = "1")]
        offset: Option<u64>,
        #[prost(uint32, optional, tag = "2")]
        size: Option<u32>,
    }

    fn segment(values: &[i64]) -> Vec<u8> {
        let mut page = (values.len() as u32).to_le_bytes().to_vec();
        for value in values {
            page.extend_from_slice(&value.to_le_bytes());
        }
        let page_footer = PageFooter {
            page_type: Some(1),
            uncompressed_size: Some(page.len() as u32),
            data: Some(DataFooter {
                num_values: Some(values.len() as u64),
            }),
            index: None,
        }
        .encode_to_vec();
        page.extend_from_slice(&page_footer);
        page.extend_from_slice(&(page_footer.len() as u32).to_le_bytes());
        page.extend_from_slice(&crc32c(&page).to_le_bytes());
        let footer = Footer {
            version: Some(1),
            columns: vec![Column {
                unique_id: Some(1),
                logical_type: Some(7),
                encoding: Some(2),
                compression: Some(0),
                nullable: Some(false),
                indexes: vec![Index {
                    index_type: Some(1),
                    ordinal: Some(Ordinal {
                        root: Some(Btree {
                            page: Some(Pointer {
                                offset: Some(0),
                                size: Some(page.len() as u32),
                            }),
                            root_is_data: Some(true),
                        }),
                    }),
                }],
            }],
            num_rows: Some(values.len() as u32),
        }
        .encode_to_vec();
        let mut segment = page;
        segment.extend_from_slice(&footer);
        segment.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        segment.extend_from_slice(&crc32c(&footer).to_le_bytes());
        segment.extend_from_slice(b"D0R1");
        segment
    }

    fn rle_segment(value: i64, count: usize) -> Vec<u8> {
        let mut page = (count as u32).to_le_bytes().to_vec();
        page.push((count as u32 * 2) as u8);
        page.extend_from_slice(&value.to_le_bytes());
        let page_footer = PageFooter {
            page_type: Some(1),
            uncompressed_size: Some(page.len() as u32),
            data: Some(DataFooter {
                num_values: Some(count as u64),
            }),
            index: None,
        }
        .encode_to_vec();
        page.extend_from_slice(&page_footer);
        page.extend_from_slice(&(page_footer.len() as u32).to_le_bytes());
        page.extend_from_slice(&crc32c(&page).to_le_bytes());
        let footer = Footer {
            version: Some(1),
            columns: vec![Column {
                unique_id: Some(1),
                logical_type: Some(7),
                encoding: Some(4),
                compression: Some(0),
                nullable: Some(false),
                indexes: vec![Index {
                    index_type: Some(1),
                    ordinal: Some(Ordinal {
                        root: Some(Btree {
                            page: Some(Pointer {
                                offset: Some(0),
                                size: Some(page.len() as u32),
                            }),
                            root_is_data: Some(true),
                        }),
                    }),
                }],
            }],
            num_rows: Some(count as u32),
        }
        .encode_to_vec();
        page.extend_from_slice(&footer);
        page.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        page.extend_from_slice(&crc32c(&footer).to_le_bytes());
        page.extend_from_slice(b"D0R1");
        page
    }

    #[test]
    fn decodes_frozen_plain_segment_to_arrow() {
        let bytes = segment(&[7, 9]);
        let footer = super::super::segment::decode_segment_footer("seg", &bytes).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = decode_plain_segment(
            "seg",
            &bytes,
            &footer,
            schema,
            &[StarRocksDirectColumnBinding::try_new(0, 1, "id", "BIGINT", false, None).unwrap()],
        )
        .unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[7, 9]
        );
    }

    #[test]
    fn decodes_frozen_auxiliary_column_for_storage_metadata_semantics() {
        let bytes = segment(&[7, 9]);
        let footer = super::super::segment::decode_segment_footer("seg", &bytes).unwrap();
        let binding =
            StarRocksDirectColumnBinding::try_new(0, 1, "predicate_id", "BIGINT", false, None)
                .unwrap();
        let values =
            decode_frozen_column("seg", &bytes, &footer, &binding, &DataType::Int64).unwrap();
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[7, 9]
        );
    }

    #[test]
    fn decodes_frozen_rle_segment_to_arrow() {
        let bytes = rle_segment(7, 3);
        let footer = super::super::segment::decode_segment_footer("seg", &bytes).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = decode_plain_segment(
            "seg",
            &bytes,
            &footer,
            schema,
            &[StarRocksDirectColumnBinding::try_new(0, 1, "id", "BIGINT", false, None).unwrap()],
        )
        .unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[7, 7, 7]
        );
    }

    #[test]
    fn materializes_historical_column_defaults() {
        let binding = StarRocksDirectColumnBinding::try_new(
            1,
            2,
            "added",
            "INT",
            false,
            Some(Bytes::from_static(b"17")),
        )
        .unwrap();
        let values = default_array(&binding, &DataType::Int32, 2).unwrap();
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[17, 17]
        );
        let nullable = StarRocksDirectColumnBinding::try_new(
            1,
            3,
            "nullable",
            "VARCHAR",
            true,
            Some(Bytes::from_static(b"NULL")),
        )
        .unwrap();
        assert_eq!(
            default_array(&nullable, &DataType::Utf8, 2)
                .unwrap()
                .null_count(),
            2
        );
        let nullable_without_default = StarRocksDirectColumnBinding::try_new(
            1,
            4,
            "nullable_without_default",
            "VARCHAR",
            true,
            None,
        )
        .unwrap();
        assert_eq!(
            default_array(&nullable_without_default, &DataType::Utf8, 2)
                .unwrap()
                .null_count(),
            2
        );
    }

    #[test]
    fn materializes_quoted_float_and_binary_historical_defaults() {
        let float = StarRocksDirectColumnBinding::try_new(
            0,
            2,
            "ratio",
            "FLOAT",
            false,
            Some(Bytes::from_static(b"'1.25'")),
        )
        .unwrap();
        let double = StarRocksDirectColumnBinding::try_new(
            1,
            3,
            "score",
            "DOUBLE",
            false,
            Some(Bytes::from_static(b"2.5")),
        )
        .unwrap();
        let binary = StarRocksDirectColumnBinding::try_new(
            2,
            4,
            "payload",
            "VARBINARY",
            false,
            Some(Bytes::from_static(b"'raw'")),
        )
        .unwrap();

        assert_eq!(
            default_array(&float, &DataType::Float32, 2)
                .unwrap()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .values(),
            &[1.25, 1.25]
        );
        assert_eq!(
            default_array(&double, &DataType::Float64, 2)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[2.5, 2.5]
        );
        let binary = default_array(&binary, &DataType::Binary, 2).unwrap();
        let binary = binary.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary.value(0), b"raw");
        assert_eq!(binary.value(1), b"raw");
    }

    #[test]
    fn materializes_temporal_and_decimal_historical_defaults() {
        let binding = |value| {
            StarRocksDirectColumnBinding::try_new(
                0,
                1,
                "value",
                "fixture",
                false,
                Some(Bytes::from_static(value)),
            )
            .unwrap()
        };
        let date = default_array(&binding(b"'1970-01-02'"), &DataType::Date32, 1).unwrap();
        assert_eq!(
            date.as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(0),
            1
        );
        let timestamp = default_array(
            &binding(b"'1970-01-01 00:00:01.5'"),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            1,
        )
        .unwrap();
        assert_eq!(
            timestamp
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            1_500_000
        );
        let decimal = default_array(&binding(b"'-12.34'"), &DataType::Decimal128(8, 2), 1).unwrap();
        assert_eq!(
            decimal
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            -1234
        );
    }

    #[test]
    fn decodes_temporal_and_decimal_plain_pages() {
        fn page(mut body: Vec<u8>, values: usize) -> Vec<u8> {
            let footer = PageFooter {
                page_type: Some(1),
                uncompressed_size: Some(body.len() as u32),
                data: Some(DataFooter {
                    num_values: Some(values as u64),
                }),
                index: None,
            }
            .encode_to_vec();
            body.extend_from_slice(&footer);
            body.extend_from_slice(&(footer.len() as u32).to_le_bytes());
            body.extend_from_slice(&crc32c(&body).to_le_bytes());
            body
        }
        fn column(logical_type: StarRocksLogicalType) -> StarRocksSegmentColumnMeta {
            StarRocksSegmentColumnMeta {
                column_id: None,
                unique_id: Some(1),
                logical_type,
                encoding: Some(StarRocksPageEncoding::Plain),
                compression: Some(super::super::segment::StarRocksCompression::None),
                nullable: false,
                dictionary_page: None,
                ordinal_index_page: None,
                ordinal_index_is_data_page: true,
                num_rows: None,
                children: Vec::new(),
            }
        }
        let decode = |column: StarRocksSegmentColumnMeta, bytes: Vec<u8>, data_type: DataType| {
            decode_single_data_page(
                "fixture",
                &bytes,
                &column,
                &data_type,
                &StarRocksPagePointer {
                    offset: 0,
                    size: bytes.len() as u32,
                },
                1,
            )
            .unwrap()
        };
        let mut date = 1_u32.to_le_bytes().to_vec();
        date.extend_from_slice(&2_440_589_i32.to_le_bytes());
        let date = decode(
            column(StarRocksLogicalType::Date),
            page(date, 1),
            DataType::Date32,
        );
        assert_eq!(
            date.as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(0),
            1
        );

        let encoded_datetime = (2_440_588_i64 << STARROCKS_DATETIME_TIME_BITS) | 1_000_000;
        let mut datetime = 1_u32.to_le_bytes().to_vec();
        datetime.extend_from_slice(&encoded_datetime.to_le_bytes());
        let datetime = decode(
            column(StarRocksLogicalType::DateTime),
            page(datetime, 1),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        assert_eq!(
            datetime
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            1_000_000
        );

        let mut decimal = 1_u32.to_le_bytes().to_vec();
        decimal.extend_from_slice(&(-1234_i64).to_le_bytes());
        let decimal = decode(
            column(StarRocksLogicalType::Decimal64),
            page(decimal, 1),
            DataType::Decimal128(8, 2),
        );
        assert_eq!(
            decimal
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            -1234
        );
    }

    #[test]
    fn decodes_frozen_multi_page_ordinal_index_to_arrow() {
        fn finish_page(mut body: Vec<u8>, footer: PageFooter) -> Vec<u8> {
            let footer = footer.encode_to_vec();
            body.extend_from_slice(&footer);
            body.extend_from_slice(&(footer.len() as u32).to_le_bytes());
            body.extend_from_slice(&crc32c(&body).to_le_bytes());
            body
        }
        fn data_page(values: &[i64]) -> Vec<u8> {
            let mut body = (values.len() as u32).to_le_bytes().to_vec();
            for value in values {
                body.extend_from_slice(&value.to_le_bytes());
            }
            finish_page(
                body.clone(),
                PageFooter {
                    page_type: Some(1),
                    uncompressed_size: Some(body.len() as u32),
                    data: Some(DataFooter {
                        num_values: Some(values.len() as u64),
                    }),
                    index: None,
                },
            )
        }

        let first = data_page(&[7, 9]);
        let second = data_page(&[11, 13]);
        let mut index_body = Vec::new();
        for (ordinal, offset, size) in [
            (0_u64, 0_u64, first.len() as u32),
            (2_u64, first.len() as u64, second.len() as u32),
        ] {
            index_body.push(8);
            index_body.extend_from_slice(&ordinal.to_be_bytes());
            index_body.push(offset as u8);
            index_body.push(size as u8);
        }
        let index = finish_page(
            index_body.clone(),
            PageFooter {
                page_type: Some(2),
                uncompressed_size: Some(index_body.len() as u32),
                data: None,
                index: Some(IndexFooter {
                    entries: Some(2),
                    node_type: Some(1),
                }),
            },
        );
        let index_offset = (first.len() + second.len()) as u64;
        let footer = Footer {
            version: Some(1),
            columns: vec![Column {
                unique_id: Some(1),
                logical_type: Some(7),
                encoding: Some(2),
                compression: Some(0),
                nullable: Some(false),
                indexes: vec![Index {
                    index_type: Some(1),
                    ordinal: Some(Ordinal {
                        root: Some(Btree {
                            page: Some(Pointer {
                                offset: Some(index_offset),
                                size: Some(index.len() as u32),
                            }),
                            root_is_data: Some(false),
                        }),
                    }),
                }],
            }],
            num_rows: Some(4),
        }
        .encode_to_vec();
        let mut bytes = first;
        bytes.extend_from_slice(&second);
        bytes.extend_from_slice(&index);
        bytes.extend_from_slice(&footer);
        bytes.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32c(&footer).to_le_bytes());
        bytes.extend_from_slice(b"D0R1");
        let footer = super::super::segment::decode_segment_footer("seg", &bytes).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = decode_plain_segment(
            "seg",
            &bytes,
            &footer,
            schema,
            &[StarRocksDirectColumnBinding::try_new(0, 1, "id", "BIGINT", false, None).unwrap()],
        )
        .unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[7, 9, 11, 13]
        );
    }

    #[test]
    fn converts_starrocks_temporal_storage_scalars() {
        assert_eq!(decode_starrocks_date(2_440_588).unwrap(), 0);
        let encoded = (2_440_588_i64 << STARROCKS_DATETIME_TIME_BITS) | 123;
        assert_eq!(decode_starrocks_datetime(encoded).unwrap(), 123);
        assert!(
            decode_starrocks_datetime(1_i64 << STARROCKS_DATETIME_TIME_BITS | 86_400_000_000)
                .is_err()
        );
    }

    #[test]
    fn allows_nullable_output_when_the_page_has_no_null_values() {
        let bytes = segment(&[7]);
        let mut footer = super::super::segment::decode_segment_footer("seg", &bytes).unwrap();
        footer.columns[0].nullable = true;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        assert_eq!(
            decode_plain_segment(
                "seg",
                &bytes,
                &footer,
                schema,
                &[
                    StarRocksDirectColumnBinding::try_new(0, 1, "id", "BIGINT", true, None)
                        .unwrap()
                ],
            )
            .unwrap()
            .num_rows(),
            1
        );
    }

    #[test]
    fn applies_nullable_page_flags_to_arrow_values() {
        assert_eq!(
            apply_nulls(vec![7_i64, 9], Some(&[0, 1])).unwrap(),
            vec![Some(7), None]
        );
        assert_eq!(
            apply_nulls(vec![7_i64], Some(&[2])).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
    }
}
