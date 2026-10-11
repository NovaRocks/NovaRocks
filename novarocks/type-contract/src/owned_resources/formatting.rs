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

//! Cumulative destination requests of the original default-Global formatter.
//! The caller authors its exact initial capacity and output-byte ceiling from
//! its original template and borrowed operands. These facts neither format a
//! value nor cover arbitrary Debug implementations, scratch or a MEM grant.

use crate::{CompileControlError, ControlResourceError};
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FormatStringRequestFacts {
    pub allocation_requests_upper_bound: usize,
    pub request_bytes_upper_bound: usize,
    pub request_alignment: usize,
}

fn resource() -> ControlResourceError {
    CompileControlError::ResourceExhausted.into()
}

/// Bound one original `format!` destination whose dynamic template authors a
/// nonzero estimated capacity. Each write appends part of the bounded output.
/// Rust 1.98.1 String uses byte RawVec::grow_amortized: a growth at capacity c
/// requests max(2*c, required_len, 8), with c < required_len <= output bound.
/// Thus every request is at most max(initial, 2*output, 8), and consecutive
/// requests at least double. Sum their reversed geometric series and count
/// the possible doublings; do not recreate the original formatter or predict
/// how it splits writes. Successful RawVec requests also fit isize::MAX.
///
/// Initial capacity is an original source fact, not a caller's desired budget.
/// Zero-initial templates and allocating custom formatters need other authors.
pub fn original_format_string_request_bound(
    initial_capacity: usize,
    output_bytes_upper_bound: usize,
) -> Result<FormatStringRequestFacts, ControlResourceError> {
    if !super::profile::LOCKED_TOOLCHAIN {
        return Err(ControlResourceError::SourceModel(
            "Format String growth source model drift",
        ));
    }
    if initial_capacity == 0 {
        return Err(ControlResourceError::SourceModel(
            "Format String requires an authored nonzero initial capacity",
        ));
    }
    Layout::array::<u8>(initial_capacity).map_err(|_| resource())?;
    if output_bytes_upper_bound <= initial_capacity {
        return Ok(FormatStringRequestFacts {
            allocation_requests_upper_bound: 1,
            request_bytes_upper_bound: initial_capacity,
            request_alignment: 1,
        });
    }
    let last_capacity_upper_bound = output_bytes_upper_bound
        .checked_mul(2)
        .ok_or_else(resource)?
        .max(initial_capacity)
        .max(8)
        .min(isize::MAX as usize);
    let doublings = (last_capacity_upper_bound / initial_capacity).ilog2() as usize;
    Ok(FormatStringRequestFacts {
        allocation_requests_upper_bound: doublings.checked_add(1).ok_or_else(resource)?,
        request_bytes_upper_bound: last_capacity_upper_bound
            .checked_mul(2)
            .ok_or_else(resource)?,
        request_alignment: 1,
    })
}

#[cfg(test)]
mod tests;
