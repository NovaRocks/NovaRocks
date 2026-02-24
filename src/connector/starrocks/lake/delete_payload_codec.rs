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

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int8Array, Int16Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

const SLICE_ESCAPE_BYTE: u8 = 0x00;
const SLICE_ESCAPE_SUFFIX: u8 = 0x01;

pub(crate) fn encode_delete_keys_payload(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    if batch.num_columns() == 0 {
        return Err("delete-key batch has no key column".to_string());
    }
    if batch.num_columns() == 1 {
        return encode_single_key_payload(batch.column(0).as_ref());
    }
    let encoded_rows = encode_composite_keys(batch)?;
    encode_binary_payload(&encoded_rows)
}

fn encode_single_key_payload(key_array: &dyn Array) -> Result<Vec<u8>, String> {
    match key_array.data_type() {
        DataType::Boolean => {
            let arr = key_array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "downcast BOOLEAN key column failed".to_string())?;
            encode_fixed_width_payload(arr.len(), 1, "BOOLEAN", |out, row_idx| {
                if arr.is_null(row_idx) {
                    return Err(format!(
                        "delete-key batch contains NULL key value at row_idx={} col_idx=0",
                        row_idx
                    ));
                }
                out.push(u8::from(arr.value(row_idx)));
                Ok(())
            })
        }
        DataType::Int8 => {
            let arr = key_array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "downcast INT8 key column failed".to_string())?;
            encode_fixed_width_payload(arr.len(), 1, "INT8", |out, row_idx| {
                if arr.is_null(row_idx) {
                    return Err(format!(
                        "delete-key batch contains NULL key value at row_idx={} col_idx=0",
                        row_idx
                    ));
                }
                out.push(arr.value(row_idx) as u8);
                Ok(())
            })
        }
        DataType::Int16 => {
            let arr = key_array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "downcast INT16 key column failed".to_string())?;
            encode_fixed_width_payload(arr.len(), 2, "INT16", |out, row_idx| {
                if arr.is_null(row_idx) {
                    return Err(format!(
                        "delete-key batch contains NULL key value at row_idx={} col_idx=0",
                        row_idx
                    ));
                }
                out.extend_from_slice(&arr.value(row_idx).to_le_bytes());
                Ok(())
            })
        }
        DataType::Int32 => {
            let arr = key_array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "downcast INT32 key column failed".to_string())?;
            encode_fixed_width_payload(arr.len(), 4, "INT32", |out, row_idx| {
                if arr.is_null(row_idx) {
                    return Err(format!(
                        "delete-key batch contains NULL key value at row_idx={} col_idx=0",
                        row_idx
                    ));
                }
                out.extend_from_slice(&arr.value(row_idx).to_le_bytes());
                Ok(())
            })
        }
        DataType::Int64 => {
            let arr = key_array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "downcast INT64 key column failed".to_string())?;
            encode_fixed_width_payload(arr.len(), 8, "INT64", |out, row_idx| {
                if arr.is_null(row_idx) {
                    return Err(format!(
                        "delete-key batch contains NULL key value at row_idx={} col_idx=0",
                        row_idx
                    ));
                }
                out.extend_from_slice(&arr.value(row_idx).to_le_bytes());
                Ok(())
            })
        }
        DataType::Date32 => {
            let arr = key_array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "downcast DATE32 key column failed".to_string())?;
            encode_fixed_width_payload(arr.len(), 4, "DATE32", |out, row_idx| {
                if arr.is_null(row_idx) {
                    return Err(format!(
                        "delete-key batch contains NULL key value at row_idx={} col_idx=0",
                        row_idx
                    ));
                }
                out.extend_from_slice(&arr.value(row_idx).to_le_bytes());
                Ok(())
            })
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = key_array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "downcast TIMESTAMP_MICROSECOND key column failed".to_string())?;
            encode_fixed_width_payload(arr.len(), 8, "TIMESTAMP_MICROSECOND", |out, row_idx| {
                if arr.is_null(row_idx) {
                    return Err(format!(
                        "delete-key batch contains NULL key value at row_idx={} col_idx=0",
                        row_idx
                    ));
                }
                out.extend_from_slice(&arr.value(row_idx).to_le_bytes());
                Ok(())
            })
        }
        DataType::Decimal128(_, _) => {
            let arr = key_array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "downcast DECIMAL128 key column failed".to_string())?;
            encode_fixed_width_payload(arr.len(), 16, "DECIMAL128", |out, row_idx| {
                if arr.is_null(row_idx) {
                    return Err(format!(
                        "delete-key batch contains NULL key value at row_idx={} col_idx=0",
                        row_idx
                    ));
                }
                out.extend_from_slice(&arr.value(row_idx).to_le_bytes());
                Ok(())
            })
        }
        DataType::Utf8 => {
            let arr = key_array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "downcast UTF8 key column failed".to_string())?;
            let mut encoded_rows = Vec::with_capacity(arr.len());
            for row_idx in 0..arr.len() {
                if arr.is_null(row_idx) {
                    return Err(format!(
                        "delete-key batch contains NULL key value at row_idx={} col_idx=0",
                        row_idx
                    ));
                }
                encoded_rows.push(arr.value(row_idx).as_bytes().to_vec());
            }
            encode_binary_payload(&encoded_rows)
        }
        DataType::Binary => {
            let arr = key_array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "downcast BINARY key column failed".to_string())?;
            let mut encoded_rows = Vec::with_capacity(arr.len());
            for row_idx in 0..arr.len() {
                if arr.is_null(row_idx) {
                    return Err(format!(
                        "delete-key batch contains NULL key value at row_idx={} col_idx=0",
                        row_idx
                    ));
                }
                encoded_rows.push(arr.value(row_idx).to_vec());
            }
            encode_binary_payload(&encoded_rows)
        }
        other => Err(format!(
            "unsupported delete-key column type for single primary key: {:?}",
            other
        )),
    }
}

fn encode_fixed_width_payload(
    row_count: usize,
    elem_width: usize,
    type_name: &str,
    mut write_one: impl FnMut(&mut Vec<u8>, usize) -> Result<(), String>,
) -> Result<Vec<u8>, String> {
    let value_bytes = row_count
        .checked_mul(elem_width)
        .ok_or_else(|| format!("{type_name} delete-key payload size overflow: rows={row_count}"))?;
    let value_bytes_u32 = u32::try_from(value_bytes).map_err(|_| {
        format!("{type_name} delete-key payload exceeds 4GiB: payload_size={value_bytes}")
    })?;
    let mut out = Vec::with_capacity(4 + value_bytes);
    out.extend_from_slice(&value_bytes_u32.to_le_bytes());
    for row_idx in 0..row_count {
        write_one(&mut out, row_idx)?;
    }
    Ok(out)
}

fn encode_composite_keys(batch: &RecordBatch) -> Result<Vec<Vec<u8>>, String> {
    let mut encoded_rows = Vec::with_capacity(batch.num_rows());
    let last_col_idx = batch.num_columns().saturating_sub(1);
    for row_idx in 0..batch.num_rows() {
        let mut key = Vec::new();
        for col_idx in 0..batch.num_columns() {
            let array = batch.column(col_idx);
            encode_composite_key_cell(
                array.as_ref(),
                row_idx,
                col_idx,
                col_idx == last_col_idx,
                &mut key,
            )?;
        }
        encoded_rows.push(key);
    }
    Ok(encoded_rows)
}

fn encode_composite_key_cell(
    array: &dyn Array,
    row_idx: usize,
    col_idx: usize,
    is_last_col: bool,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    if array.is_null(row_idx) {
        return Err(format!(
            "delete-key batch contains NULL key value at row_idx={} col_idx={} type={:?}",
            row_idx,
            col_idx,
            array.data_type()
        ));
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "downcast BOOLEAN key column failed".to_string())?;
            out.push(u8::from(arr.value(row_idx)));
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "downcast INT8 key column failed".to_string())?;
            let value = arr.value(row_idx) ^ i8::MIN;
            out.push(value as u8);
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "downcast INT16 key column failed".to_string())?;
            let value = (arr.value(row_idx) as u16) ^ (1_u16 << 15);
            out.extend_from_slice(&value.to_be_bytes());
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "downcast INT32 key column failed".to_string())?;
            let value = (arr.value(row_idx) as u32) ^ (1_u32 << 31);
            out.extend_from_slice(&value.to_be_bytes());
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "downcast INT64 key column failed".to_string())?;
            let value = (arr.value(row_idx) as u64) ^ (1_u64 << 63);
            out.extend_from_slice(&value.to_be_bytes());
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "downcast DATE32 key column failed".to_string())?;
            let value = (arr.value(row_idx) as u32) ^ (1_u32 << 31);
            out.extend_from_slice(&value.to_be_bytes());
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "downcast TIMESTAMP_MICROSECOND key column failed".to_string())?;
            let value = (arr.value(row_idx) as u64) ^ (1_u64 << 63);
            out.extend_from_slice(&value.to_be_bytes());
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "downcast UTF8 key column failed".to_string())?;
            encode_slice_key_bytes(arr.value(row_idx).as_bytes(), is_last_col, out)?;
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "downcast BINARY key column failed".to_string())?;
            encode_slice_key_bytes(arr.value(row_idx), is_last_col, out)?;
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "downcast DECIMAL128 key column failed".to_string())?;
            let raw = arr.value(row_idx);
            let value = (raw as u128) ^ (1_u128 << 127);
            out.extend_from_slice(&value.to_be_bytes());
        }
        other => {
            return Err(format!(
                "unsupported key column type while encoding composite delete payload: col_idx={} type={:?}",
                col_idx, other
            ));
        }
    }
    Ok(())
}

fn encode_slice_key_bytes(
    value: &[u8],
    is_last_col: bool,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    if is_last_col {
        out.extend_from_slice(value);
        return Ok(());
    }
    for byte in value {
        if *byte == SLICE_ESCAPE_BYTE {
            out.push(SLICE_ESCAPE_BYTE);
            out.push(SLICE_ESCAPE_SUFFIX);
        } else {
            out.push(*byte);
        }
    }
    out.push(SLICE_ESCAPE_BYTE);
    out.push(SLICE_ESCAPE_BYTE);
    Ok(())
}

fn encode_binary_payload(encoded_rows: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    let mut total_bytes = 0usize;
    for row in encoded_rows {
        total_bytes = total_bytes
            .checked_add(row.len())
            .ok_or_else(|| "binary delete-key payload bytes size overflow".to_string())?;
    }
    let total_bytes_u32 = u32::try_from(total_bytes).map_err(|_| {
        format!(
            "binary delete-key payload bytes exceed 4GiB: bytes_size={}",
            total_bytes
        )
    })?;
    let offsets_count = encoded_rows
        .len()
        .checked_add(1)
        .ok_or_else(|| "binary delete-key payload offsets count overflow".to_string())?;
    let offsets_bytes = offsets_count
        .checked_mul(4)
        .ok_or_else(|| "binary delete-key payload offsets size overflow".to_string())?;
    let offsets_bytes_u32 = u32::try_from(offsets_bytes).map_err(|_| {
        format!(
            "binary delete-key payload offsets exceed 4GiB: offsets_size={}",
            offsets_bytes
        )
    })?;

    let mut out = Vec::with_capacity(4 + total_bytes + 4 + offsets_bytes);
    out.extend_from_slice(&total_bytes_u32.to_le_bytes());

    let mut offsets = Vec::with_capacity(offsets_count);
    offsets.push(0_u32);
    let mut current = 0_u32;
    for row in encoded_rows {
        out.extend_from_slice(row);
        let row_len_u32 = u32::try_from(row.len()).map_err(|_| {
            format!(
                "binary delete-key row length exceeds u32: row_len={}",
                row.len()
            )
        })?;
        current = current
            .checked_add(row_len_u32)
            .ok_or_else(|| "binary delete-key payload offsets overflow".to_string())?;
        offsets.push(current);
    }

    out.extend_from_slice(&offsets_bytes_u32.to_le_bytes());
    for offset in offsets {
        out.extend_from_slice(&offset.to_le_bytes());
    }
    Ok(out)
}

pub(crate) fn decode_delete_keys_payload(
    payload: &[u8],
    key_output_schema: &SchemaRef,
) -> Result<Vec<Vec<u8>>, String> {
    if key_output_schema.fields().is_empty() {
        return Err("invalid key output schema for delete decode: no key fields".to_string());
    }

    if key_output_schema.fields().len() > 1 {
        return decode_binary_payload(payload);
    }

    let data_type = key_output_schema.fields()[0].data_type();
    match data_type {
        DataType::Boolean => {
            let raw = decode_fixed_column_payload(payload, 1, "BOOLEAN")?;
            Ok(raw
                .iter()
                .map(|v| vec![u8::from(*v != 0)])
                .collect::<Vec<Vec<u8>>>())
        }
        DataType::Int8 => {
            let raw = decode_fixed_column_payload(payload, 1, "INT8")?;
            Ok(raw.iter().map(|v| vec![*v]).collect::<Vec<Vec<u8>>>())
        }
        DataType::Int16 => {
            let raw = decode_fixed_column_payload(payload, 2, "INT16")?;
            let mut out = Vec::with_capacity(raw.len() / 2);
            for chunk in raw.chunks_exact(2) {
                out.push(chunk.to_vec());
            }
            Ok(out)
        }
        DataType::Int32 => {
            let raw = decode_fixed_column_payload(payload, 4, "INT32")?;
            let mut out = Vec::with_capacity(raw.len() / 4);
            for chunk in raw.chunks_exact(4) {
                out.push(chunk.to_vec());
            }
            Ok(out)
        }
        DataType::Int64 => {
            let raw = decode_fixed_column_payload(payload, 8, "INT64")?;
            let mut out = Vec::with_capacity(raw.len() / 8);
            for chunk in raw.chunks_exact(8) {
                out.push(chunk.to_vec());
            }
            Ok(out)
        }
        DataType::Date32 => {
            let raw = decode_fixed_column_payload(payload, 4, "DATE32")?;
            let mut out = Vec::with_capacity(raw.len() / 4);
            for chunk in raw.chunks_exact(4) {
                out.push(chunk.to_vec());
            }
            Ok(out)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let raw = decode_fixed_column_payload(payload, 8, "TIMESTAMP_MICROSECOND")?;
            let mut out = Vec::with_capacity(raw.len() / 8);
            for chunk in raw.chunks_exact(8) {
                out.push(chunk.to_vec());
            }
            Ok(out)
        }
        DataType::Utf8 | DataType::Binary => decode_binary_payload(payload),
        DataType::Decimal128(_, _) => {
            let raw = decode_fixed_column_payload(payload, 16, "DECIMAL128")?;
            let mut out = Vec::with_capacity(raw.len() / 16);
            for chunk in raw.chunks_exact(16) {
                out.push(chunk.to_vec());
            }
            Ok(out)
        }
        other => Err(format!(
            "unsupported key column type while decoding op_write.dels: {:?}",
            other
        )),
    }
}

fn decode_fixed_column_payload<'a>(
    payload: &'a [u8],
    elem_width: usize,
    type_name: &str,
) -> Result<&'a [u8], String> {
    if payload.len() < 4 {
        return Err(format!(
            "invalid {} delete payload: too small (payload_size={})",
            type_name,
            payload.len()
        ));
    }
    let size = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    let expected = 4_usize.checked_add(size).ok_or_else(|| {
        format!(
            "invalid {} delete payload size overflow: declared_size={}",
            type_name, size
        )
    })?;
    if payload.len() != expected {
        return Err(format!(
            "invalid {} delete payload size mismatch: declared_size={} payload_size={}",
            type_name,
            size,
            payload.len()
        ));
    }
    if size % elem_width != 0 {
        return Err(format!(
            "invalid {} delete payload element alignment: declared_size={} elem_width={}",
            type_name, size, elem_width
        ));
    }
    Ok(&payload[4..expected])
}

fn decode_binary_payload(payload: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if payload.len() < 8 {
        return Err(format!(
            "invalid binary delete payload: too small (payload_size={})",
            payload.len()
        ));
    }
    let bytes_size = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    let bytes_start = 4_usize;
    let bytes_end = bytes_start.checked_add(bytes_size).ok_or_else(|| {
        format!(
            "invalid binary delete payload bytes range overflow: bytes_size={}",
            bytes_size
        )
    })?;
    if bytes_end + 4 > payload.len() {
        return Err(format!(
            "invalid binary delete payload bytes section out of range: bytes_size={} payload_size={}",
            bytes_size,
            payload.len()
        ));
    }

    let offsets_size = u32::from_le_bytes([
        payload[bytes_end],
        payload[bytes_end + 1],
        payload[bytes_end + 2],
        payload[bytes_end + 3],
    ]) as usize;
    let offsets_start = bytes_end + 4;
    let offsets_end = offsets_start.checked_add(offsets_size).ok_or_else(|| {
        format!(
            "invalid binary delete payload offsets range overflow: offsets_size={}",
            offsets_size
        )
    })?;
    if offsets_end != payload.len() {
        return Err(format!(
            "invalid binary delete payload offsets section mismatch: offsets_size={} payload_size={}",
            offsets_size,
            payload.len()
        ));
    }
    if offsets_size % 4 != 0 {
        return Err(format!(
            "invalid binary delete payload offsets alignment: offsets_size={}",
            offsets_size
        ));
    }

    let offsets_count = offsets_size / 4;
    if offsets_count == 0 {
        return Err("invalid binary delete payload offsets: empty offsets".to_string());
    }
    let mut offsets = Vec::with_capacity(offsets_count);
    for i in 0..offsets_count {
        let pos = offsets_start + i * 4;
        let off = u32::from_le_bytes([
            payload[pos],
            payload[pos + 1],
            payload[pos + 2],
            payload[pos + 3],
        ]) as usize;
        offsets.push(off);
    }
    if offsets[0] != 0 {
        return Err(format!(
            "invalid binary delete payload offsets: first offset must be 0, got={}",
            offsets[0]
        ));
    }
    if offsets[offsets.len() - 1] != bytes_size {
        return Err(format!(
            "invalid binary delete payload offsets: last offset mismatch, expected={} actual={}",
            bytes_size,
            offsets[offsets.len() - 1]
        ));
    }

    let bytes = &payload[bytes_start..bytes_end];
    let mut out = Vec::with_capacity(offsets_count.saturating_sub(1));
    let mut prev = 0_usize;
    for (idx, &end) in offsets.iter().enumerate().skip(1) {
        if end < prev || end > bytes_size {
            return Err(format!(
                "invalid binary delete payload offset order: index={} prev={} end={} bytes_size={}",
                idx, prev, end, bytes_size
            ));
        }
        out.push(bytes[prev..end].to_vec());
        prev = end;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, BinaryArray, Date32Array, Decimal128Array, Int32Array, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use super::{decode_delete_keys_payload, encode_delete_keys_payload};

    fn i32_to_raw_key_bytes(value: i32) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    fn i64_to_raw_key_bytes(value: i64) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    fn i128_to_raw_key_bytes(value: i128) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    #[test]
    fn encode_decode_delete_payload_for_single_date32_key() {
        let schema = Arc::new(Schema::new(vec![Field::new("k1", DataType::Date32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Date32Array::from(vec![-1, 0, 42]))],
        )
        .expect("build date32 key batch");

        let payload = encode_delete_keys_payload(&batch).expect("encode delete payload");
        let keys = decode_delete_keys_payload(&payload, &schema).expect("decode delete payload");
        assert_eq!(
            keys,
            vec![
                i32_to_raw_key_bytes(-1),
                i32_to_raw_key_bytes(0),
                i32_to_raw_key_bytes(42),
            ]
        );
    }

    #[test]
    fn encode_decode_delete_payload_for_single_timestamp_key() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "k1",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                -1, 0, 123_456,
            ]))],
        )
        .expect("build timestamp key batch");

        let payload = encode_delete_keys_payload(&batch).expect("encode delete payload");
        let keys = decode_delete_keys_payload(&payload, &schema).expect("decode delete payload");
        assert_eq!(
            keys,
            vec![
                i64_to_raw_key_bytes(-1),
                i64_to_raw_key_bytes(0),
                i64_to_raw_key_bytes(123_456),
            ]
        );
    }

    #[test]
    fn encode_decode_delete_payload_for_single_decimal128_key() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "k1",
            DataType::Decimal128(20, 4),
            false,
        )]));
        let arr = Decimal128Array::from(vec![Some(-1_i128), Some(0_i128), Some(123_456_i128)])
            .with_precision_and_scale(20, 4)
            .expect("decimal precision/scale");
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)])
            .expect("build decimal key batch");

        let payload = encode_delete_keys_payload(&batch).expect("encode delete payload");
        let keys = decode_delete_keys_payload(&payload, &schema).expect("decode delete payload");
        assert_eq!(
            keys,
            vec![
                i128_to_raw_key_bytes(-1),
                i128_to_raw_key_bytes(0),
                i128_to_raw_key_bytes(123_456),
            ]
        );
    }

    #[test]
    fn encode_decode_delete_payload_for_single_binary_key() {
        let schema = Arc::new(Schema::new(vec![Field::new("k1", DataType::Binary, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BinaryArray::from(vec![
                Some(b"".as_slice()),
                Some(b"\x00ab".as_slice()),
                Some(b"xy".as_slice()),
            ]))],
        )
        .expect("build binary key batch");

        let payload = encode_delete_keys_payload(&batch).expect("encode delete payload");
        let keys = decode_delete_keys_payload(&payload, &schema).expect("decode delete payload");
        assert_eq!(keys, vec![b"".to_vec(), b"\x00ab".to_vec(), b"xy".to_vec()]);
    }

    #[test]
    fn encode_decode_delete_payload_for_composite_binary_int_key() {
        let key_schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Binary, false),
            Field::new("k2", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_from_iter(vec![
            (
                "k1",
                Arc::new(BinaryArray::from(vec![
                    Some(b"a\x00b".as_slice()),
                    Some(b"x".as_slice()),
                ])) as Arc<dyn Array>,
            ),
            (
                "k2",
                Arc::new(Int32Array::from(vec![-1, 2])) as Arc<dyn Array>,
            ),
        ])
        .expect("build composite key batch");

        let payload = encode_delete_keys_payload(&batch).expect("encode delete payload");
        let keys =
            decode_delete_keys_payload(&payload, &key_schema).expect("decode delete payload");
        assert_eq!(
            keys,
            vec![
                vec![0x61, 0x00, 0x01, 0x62, 0x00, 0x00, 0x7f, 0xff, 0xff, 0xff],
                vec![0x78, 0x00, 0x00, 0x80, 0x00, 0x00, 0x02],
            ]
        );
    }
}
