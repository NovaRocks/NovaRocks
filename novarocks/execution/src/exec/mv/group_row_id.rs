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

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

/// Builds the stable aggregate-MV group-row identifier from its group-key columns.
pub fn aggregate_group_row_id_array(columns: &[ArrayRef]) -> Result<ArrayRef, String> {
    let rows = columns.first().map(|column| column.len()).unwrap_or(0);
    for (idx, column) in columns.iter().enumerate() {
        if column.len() != rows {
            return Err(format!(
                "aggregate MV row id group key column {idx} length mismatch: {} vs {rows}",
                column.len()
            ));
        }
    }
    let mut row_ids = Vec::with_capacity(rows);
    for row in 0..rows {
        let mut cells = Vec::with_capacity(columns.len());
        for array in columns {
            cells.push(hex_encode(&encoded_cell(array, row)?));
        }
        row_ids.push(cells.join("|"));
    }
    Ok(Arc::new(StringArray::from(row_ids)))
}

/// Returns the stable aggregate-MV group-row identifier for a single row.
pub fn aggregate_group_row_id_at(columns: &[ArrayRef], row: usize) -> Result<String, String> {
    let mut cells = Vec::with_capacity(columns.len());
    for (idx, array) in columns.iter().enumerate() {
        if row >= array.len() {
            return Err(format!(
                "aggregate MV row id group key column {idx} row {row} is out of bounds for len {}",
                array.len()
            ));
        }
        cells.push(hex_encode(&encoded_cell(array, row)?));
    }
    Ok(cells.join("|"))
}

fn encoded_cell(array: &ArrayRef, row: usize) -> Result<Vec<u8>, String> {
    match array.data_type() {
        DataType::Boolean => encode_typed_cell::<BooleanArray, _>(array, row, "boolean", |arr| {
            vec![u8::from(arr.value(row))]
        }),
        DataType::Int8 => encode_typed_cell::<Int8Array, _>(array, row, "int8", |arr| {
            arr.value(row).to_le_bytes().to_vec()
        }),
        DataType::Int16 => encode_typed_cell::<Int16Array, _>(array, row, "int16", |arr| {
            arr.value(row).to_le_bytes().to_vec()
        }),
        DataType::Int32 => encode_typed_cell::<Int32Array, _>(array, row, "int32", |arr| {
            arr.value(row).to_le_bytes().to_vec()
        }),
        DataType::Date32 => encode_typed_cell::<Date32Array, _>(array, row, "date32", |arr| {
            arr.value(row).to_le_bytes().to_vec()
        }),
        DataType::Int64 => encode_typed_cell::<Int64Array, _>(array, row, "int64", |arr| {
            arr.value(row).to_le_bytes().to_vec()
        }),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            encode_typed_cell::<TimestampMicrosecondArray, _>(
                array,
                row,
                "timestamp_microsecond",
                |arr| arr.value(row).to_le_bytes().to_vec(),
            )
        }
        DataType::Utf8 => encode_typed_cell::<StringArray, _>(array, row, "utf8", |arr| {
            arr.value(row).as_bytes().to_vec()
        }),
        DataType::Decimal128(precision, scale) => {
            let type_name = format!("decimal128({precision},{scale})");
            encode_typed_cell::<Decimal128Array, _>(array, row, &type_name, |arr| {
                arr.value(row).to_le_bytes().to_vec()
            })
        }
        other => Err(format!(
            "aggregate MV row id does not support group key type {other:?}"
        )),
    }
}

fn encode_typed_cell<A, F>(
    array: &ArrayRef,
    row: usize,
    type_name: &str,
    value_bytes: F,
) -> Result<Vec<u8>, String>
where
    A: Array + 'static,
    F: FnOnce(&A) -> Vec<u8>,
{
    let typed = array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| format!("aggregate MV row id downcast failed for {type_name}"))?;
    let mut out = Vec::new();
    out.extend_from_slice(type_name.as_bytes());
    out.push(b':');
    if typed.is_null(row) {
        out.extend_from_slice(b"N");
    } else {
        out.extend_from_slice(b"V:");
        out.extend_from_slice(&value_bytes(typed));
    }
    Ok(out)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
