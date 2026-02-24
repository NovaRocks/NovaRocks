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
use arrow::array::{ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder};
use arrow::datatypes::DataType;
use std::sync::Arc;

pub const LARGEINT_BYTE_WIDTH: i32 = 16;

pub fn is_largeint_data_type(dt: &DataType) -> bool {
    matches!(dt, DataType::FixedSizeBinary(w) if *w == LARGEINT_BYTE_WIDTH)
}

pub fn i128_to_be_bytes(value: i128) -> [u8; 16] {
    value.to_be_bytes()
}

pub fn i128_from_be_bytes(bytes: &[u8]) -> Result<i128, String> {
    if bytes.len() != LARGEINT_BYTE_WIDTH as usize {
        return Err(format!(
            "invalid LARGEINT byte length: expected {}, got {}",
            LARGEINT_BYTE_WIDTH,
            bytes.len()
        ));
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(bytes);
    Ok(i128::from_be_bytes(buf))
}

pub fn array_from_i128(values: &[Option<i128>]) -> Result<ArrayRef, String> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), LARGEINT_BYTE_WIDTH);
    for value in values {
        match value {
            Some(v) => builder
                .append_value(i128_to_be_bytes(*v))
                .map_err(|e| e.to_string())?,
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn as_fixed_size_binary_array<'a>(
    array: &'a ArrayRef,
    context: &str,
) -> Result<&'a FixedSizeBinaryArray, String> {
    let arr = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| format!("{context}: expected FixedSizeBinaryArray"))?;
    if arr.value_length() != LARGEINT_BYTE_WIDTH {
        return Err(format!(
            "{context}: expected FixedSizeBinary({}), got FixedSizeBinary({})",
            LARGEINT_BYTE_WIDTH,
            arr.value_length()
        ));
    }
    Ok(arr)
}

pub fn value_at(arr: &FixedSizeBinaryArray, row: usize) -> Result<i128, String> {
    i128_from_be_bytes(arr.value(row))
}
