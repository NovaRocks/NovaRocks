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

use arrow::array::{Array, ArrayRef, BinaryArray, LargeBinaryArray};
use arrow::datatypes::DataType;

pub(super) fn row_count(fn_name: &str, lhs_len: usize, rhs_len: usize) -> Result<usize, String> {
    if lhs_len == rhs_len {
        return Ok(lhs_len);
    }
    if lhs_len == 1 {
        return Ok(rhs_len);
    }
    if rhs_len == 1 {
        return Ok(lhs_len);
    }
    Err(format!(
        "{fn_name} row count mismatch: lhs_len={lhs_len} rhs_len={rhs_len}"
    ))
}

pub(super) fn row_index(row: usize, len: usize) -> Result<usize, String> {
    if len == 1 {
        Ok(0)
    } else if row < len {
        Ok(row)
    } else {
        Err(format!("row index {row} out of bounds for len {len}"))
    }
}

pub(super) fn binary_value_or_empty<'a>(
    array: &'a ArrayRef,
    row: usize,
    fn_name: &str,
    arg_idx: usize,
) -> Result<&'a [u8], String> {
    match array.data_type() {
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    format!("{fn_name} downcast BinaryArray failed for arg {arg_idx}")
                })?;
            if arr.is_null(row) {
                Ok(&[])
            } else {
                Ok(arr.value(row))
            }
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    format!("{fn_name} downcast LargeBinaryArray failed for arg {arg_idx}")
                })?;
            if arr.is_null(row) {
                Ok(&[])
            } else {
                Ok(arr.value(row))
            }
        }
        other => Err(format!(
            "{fn_name} expects Binary or LargeBinary input for arg {arg_idx}, got {other:?}"
        )),
    }
}
