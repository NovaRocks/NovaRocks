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
//! Original LARGEINT text computations, with original parse and reader policy.
use crate::largeint;
use arrow_array::{Array, ArrayRef, FixedSizeBinaryArray, builder::StringBuilder};
use std::sync::Arc;

/// Original VARCHAR -> LARGEINT grammar. Parse failure is a successful NULL,
/// including empty/sign-only text and values outside signed i128. No i64
/// intermediate, saturation, extra syntax or error suppression is involved.
/// Selected/runtime callers bracket this opaque std parser with their control.
pub fn parse_utf8(value: &str) -> Option<i128> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<i128>().ok()
}

/// Same original signed big-endian reader and native i128 text conversion.
pub fn value_text(source: &FixedSizeBinaryArray, row: usize) -> Result<String, String> {
    Ok(largeint::value_at(source, row)?.to_string())
}

/// Original v1 batch adapter; selected callers reuse value_text at exact addresses.
pub fn cast_array(child_array: &ArrayRef) -> Result<ArrayRef, String> {
    let arr = largeint::as_fixed_size_binary_array(child_array, "cast LARGEINT to VARCHAR")?;
    let mut builder = StringBuilder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            builder.append_null();
            continue;
        }
        builder.append_value(value_text(arr, row)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}
