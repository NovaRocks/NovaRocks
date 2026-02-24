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
//! Decimal helpers for DECIMALV3 native reads.
//!
//! This module normalizes StarRocks DECIMALV3 storage widths
//! (`DECIMAL32/64/128`) into Arrow `Decimal128` output arrays.
//!
//! Current limitations:
//! - Supports DECIMALV3 only.
//! - DECIMALV2 is rejected by plan/schema mapping before reaching this module.
//! - Runtime output type is Arrow `Decimal128` or `Decimal256`.

use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, Decimal256Array};
use arrow_buffer::i256;

use super::super::constants::{
    LOGICAL_TYPE_DECIMAL32, LOGICAL_TYPE_DECIMAL64, LOGICAL_TYPE_DECIMAL128,
    LOGICAL_TYPE_DECIMAL256,
};

/// Decimal metadata from output Arrow schema (DECIMALV3 only).
#[derive(Clone, Copy, Debug)]
pub(crate) struct DecimalOutputMeta {
    /// Decimal precision from output Arrow field.
    pub(crate) precision: u8,
    /// Decimal scale from output Arrow field.
    pub(crate) scale: i8,
}

/// Map StarRocks decimal logical type id to on-page element byte size.
pub(crate) fn decimal_elem_size_from_logical_type(logical_type: i32) -> Result<usize, String> {
    match logical_type {
        LOGICAL_TYPE_DECIMAL32 => Ok(4),
        LOGICAL_TYPE_DECIMAL64 => Ok(8),
        LOGICAL_TYPE_DECIMAL128 => Ok(16),
        LOGICAL_TYPE_DECIMAL256 => Ok(32),
        other => Err(format!(
            "unsupported logical_type for decimal output column in rust native reader: logical_type={}, supported=[{},{},{},{}]",
            other,
            LOGICAL_TYPE_DECIMAL32,
            LOGICAL_TYPE_DECIMAL64,
            LOGICAL_TYPE_DECIMAL128,
            LOGICAL_TYPE_DECIMAL256
        )),
    }
}

/// Build Arrow `Decimal128Array` with FE-provided precision/scale semantics.
pub(crate) fn build_decimal128_array(
    values: Vec<i128>,
    null_flags: &[u8],
    has_null: bool,
    output_name: &str,
    decimal_meta: Option<DecimalOutputMeta>,
) -> Result<ArrayRef, String> {
    let DecimalOutputMeta { precision, scale } = decimal_meta.ok_or_else(|| {
        format!(
            "missing decimal output metadata for Decimal128 output column: output_column={}",
            output_name
        )
    })?;
    let array = if !has_null {
        Decimal128Array::from(values)
    } else {
        if values.len() != null_flags.len() {
            return Err(format!(
                "null flag length mismatch for Decimal128 output column: output_column={}, values={}, null_flags={}",
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
                        "invalid null flag value for Decimal128 output column: output_column={}, row_index={}, null_flag={}, expected=[0,1]",
                        output_name, idx, other
                    ));
                }
            }
        }
        Decimal128Array::from(values_with_null)
    };
    let array = array.with_precision_and_scale(precision, scale).map_err(|e| {
        format!(
            "set precision/scale for Decimal128 output column failed: output_column={}, precision={}, scale={}, error={}",
            output_name, precision, scale, e
        )
    })?;
    Ok(Arc::new(array))
}

/// Build Arrow `Decimal256Array` with FE-provided precision/scale semantics.
pub(crate) fn build_decimal256_array(
    values: Vec<i256>,
    null_flags: &[u8],
    has_null: bool,
    output_name: &str,
    decimal_meta: Option<DecimalOutputMeta>,
) -> Result<ArrayRef, String> {
    let DecimalOutputMeta { precision, scale } = decimal_meta.ok_or_else(|| {
        format!(
            "missing decimal output metadata for Decimal256 output column: output_column={}",
            output_name
        )
    })?;
    let array = if !has_null {
        Decimal256Array::from(values)
    } else {
        if values.len() != null_flags.len() {
            return Err(format!(
                "null flag length mismatch for Decimal256 output column: output_column={}, values={}, null_flags={}",
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
                        "invalid null flag value for Decimal256 output column: output_column={}, row_index={}, null_flag={}, expected=[0,1]",
                        output_name, idx, other
                    ));
                }
            }
        }
        Decimal256Array::from(values_with_null)
    };
    let array = array.with_precision_and_scale(precision, scale).map_err(|e| {
        format!(
            "set precision/scale for Decimal256 output column failed: output_column={}, precision={}, scale={}, error={}",
            output_name, precision, scale, e
        )
    })?;
    Ok(Arc::new(array))
}
