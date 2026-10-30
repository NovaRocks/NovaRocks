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
//! Join-build sink state abstraction.
//!
//! Responsibilities:
//! - Defines the contract for collecting build-side rows and producing probe-ready artifacts.
//! - Allows broadcast and partitioned hash-join build paths to share sink orchestration logic.
//!
//! Key exported interfaces:
//! - Types: `JoinBuildSinkState`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::hash_table::key_index::{BulkDomain, IndexPurpose};
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::ops::Range;

/// Original K instances shared by the installed input producers. Full NAA
/// keys retain SQL scope followed by value components. The grouping projection
/// is explicit: an admitted linear specialization may additionally group by
/// required value components without changing their SQL expression roles.
/// Each producer captures the same evaluated arrays in these original domains.
pub(crate) enum ProjectionDomains {
    Ordinary {
        keys: BulkDomain,
        purpose: IndexPurpose,
    },
    Naa {
        group: Option<BulkDomain>,
        full: BulkDomain,
        group_full_components: Vec<usize>,
        value_components: Range<usize>,
        full_purpose: IndexPurpose,
        _storage: ValueTypeStorageLifetime,
    },
}

#[derive(Debug)]
struct JoinBuildStateContract(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for JoinBuildStateContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl std::error::Error for JoinBuildStateContract {}
impl novarocks_failure_contract::FailureProjection for JoinBuildStateContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::JoinBuildStateContract.reason(),
        )
    }
}
pub(super) fn build_contract(
    detail: impl std::fmt::Display,
) -> crate::runtime::fragment::ExecutionFailure {
    crate::runtime::fragment::ExecutionFailure::from_domain(JoinBuildStateContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
