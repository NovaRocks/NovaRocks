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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Stable reasons authored by SQL's physical planning stage.
//! Projection and the public SQL identity remain with their respective owners.

use crate::{FailureReason, domain};
use std::num::NonZeroU16;

pub const STRUCTURAL_WAIT_CYCLE: FailureReason =
    FailureReason::new(domain::SQL, NonZeroU16::new(14).unwrap());

pub const MERGE_TARGET_ROW_MULTIPLE_MATCHES: FailureReason =
    FailureReason::new(domain::SQL, NonZeroU16::new(15).unwrap());
pub const UPDATE_TARGET_ROW_MULTIPLE_MATCHES: FailureReason =
    FailureReason::new(domain::SQL, NonZeroU16::new(16).unwrap());

/// Closed vocabulary permitted on a planner-declared keyed row assertion.
pub const fn row_match_violation_from_code(code: u32) -> Option<FailureReason> {
    match code {
        15 => Some(MERGE_TARGET_ROW_MULTIPLE_MATCHES),
        16 => Some(UPDATE_TARGET_ROW_MULTIPLE_MATCHES),
        _ => None,
    }
}

pub fn is_row_match_violation(reason: FailureReason) -> bool {
    row_match_violation_from_code(u32::from(reason.code())) == Some(reason)
}
