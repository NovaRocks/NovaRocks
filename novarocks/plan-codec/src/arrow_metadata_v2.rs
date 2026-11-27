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

//! Exact sorted Arrow metadata projection with cooperative string copies.

use crate::{allocation_exit_v2::reserve_exit, physical_type_v2::TypeCodecError};
use novarocks_proto_models::plan;
use novarocks_type_contract::CompileCheckpoints;
use std::cmp::Ordering;

fn reserve<T>(n: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, TypeCodecError> {
    let mut output = Vec::new();
    work.flush()?;
    let result = output.try_reserve_exact(n);
    // A captured failure is already primary, before any opaque-exit callback.
    if result.is_ok() {
        work.step()?;
    }
    reserve_exit::<TypeCodecError>(result, work)?;
    Ok(output)
}

pub(crate) fn copy_string(
    input: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, TypeCodecError> {
    novarocks_type_contract::owned_resources::copy::copy_string(input, work)
}

/// Exact UTF-8 byte lexical order, including prefix length and embedded NUL.
/// Observe each actual compared byte, never a post-comparison estimate loop.
pub(crate) fn compare_keys(
    left: &str,
    right: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Ordering, TypeCodecError> {
    for (left, right) in left.as_bytes().iter().zip(right.as_bytes()) {
        let order = left.cmp(right);
        work.step()?;
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    let order = left.len().cmp(&right.len());
    work.step()?;
    Ok(order)
}

/// Strict ascending order is the original sorted/unique metadata grammar.
/// The consuming owner chooses its existing diagnostic when this is false.
pub(crate) fn ordered_key(
    previous: Option<&str>,
    current: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, TypeCodecError> {
    match previous {
        Some(previous) => Ok(compare_keys(previous, current, work)? == Ordering::Less),
        None => {
            work.step()?;
            Ok(true)
        }
    }
}

/// The original Arrow metadata order is lexical key order. Emit each actual
/// entry once, copying key and value into the prepared wire output.
pub(crate) fn encode_metadata(
    source: &arrow::datatypes::Metadata,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<plan::ArrowFieldMetadataEntry>, TypeCodecError> {
    let mut output = reserve(source.len(), work)?;
    for (key, value) in source {
        let entry = plan::ArrowFieldMetadataEntry {
            key: copy_string(key, work)?,
            value: copy_string(value, work)?,
        };
        output.push(entry);
        work.step()?;
    }
    Ok(output)
}

#[cfg(test)]
#[path = "arrow_metadata_v2/tests.rs"]
mod tests;
