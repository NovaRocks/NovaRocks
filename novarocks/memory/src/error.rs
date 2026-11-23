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

//! Refusals preserve request and constraint identities; only completed
//! settlement can produce actionable shared shortage.
use crate::{
    ids::{AccountId, PolicyVersion},
    policy::LimitDimension,
};
use std::{error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetadataRegistryLabel {
    Accounts,
    Owners,
    Events,
    FundingRecords,
}
impl fmt::Display for MetadataRegistryLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConstraintKind {
    ProcessCapacity,
    AccountPolicy,
    AncestorPolicy,
    GrowthFrozen,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub request_account: AccountId,
    pub constraint_account: AccountId,
    pub constraint_kind: ConstraintKind,
    pub dimension: Option<LimitDimension>,
    pub requested: u64,
    pub required_growth: u64,
    pub ledger_revision: u64,
    pub limit: u64,
    pub committed: u64,
    pub policy_revision: PolicyVersion,
    pub capacity_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapacityError {
    QueryLimit(Refusal),
    ImpossibleRequest(Refusal),
    ShortageCandidate(Refusal),
    Closed {
        account: AccountId,
    },
    MetadataExhausted {
        registry: MetadataRegistryLabel,
        limit: u64,
    },
    Invalid {
        detail: &'static str,
    },
}
impl fmt::Display for CapacityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "memory request refused: {self:?}")
    }
}
impl Error for CapacityError {}
/// An invalid authority configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    OwnersExceedFundingRecords {
        owners: u32,
        funding_records: u32,
    },
    CoreMetadataExceedsCapacity {
        metadata_bytes: u64,
        capacity_bytes: u64,
    },
    /// Managed capacity plus the headroom budget exceeds the process bound.
    ///
    /// `B` and `H` partition `P`; letting them exceed it would promise
    /// capacity the process does not have.
    CapacityExceedsProcessBound {
        /// Managed capacity `B`.
        capacity_bytes: u64,
        /// Headroom budget `H`.
        headroom_budget_bytes: u64,
        /// Process bound `P`.
        process_bound_bytes: u64,
    },
    /// The process bound is zero, so nothing could ever be granted.
    ProcessBoundIsZero,
    /// A bounded registry was configured with a zero limit, which would refuse
    /// even the root account.
    MetadataLimitIsZero {
        /// Which registry was misconfigured.
        registry: MetadataRegistryLabel,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OwnersExceedFundingRecords {
                owners,
                funding_records,
            } => write!(
                f,
                "invalid memory authority configuration: {owners} active owners exceeds {funding_records} funding records"
            ),
            Self::CoreMetadataExceedsCapacity {
                metadata_bytes,
                capacity_bytes,
            } => write!(
                f,
                "core metadata requires {metadata_bytes} bytes but configured capacity is {capacity_bytes}"
            ),
            Self::CapacityExceedsProcessBound {
                capacity_bytes,
                headroom_budget_bytes,
                process_bound_bytes,
            } => write!(
                f,
                "invalid memory authority configuration: managed capacity {capacity_bytes} bytes \
                 plus headroom budget {headroom_budget_bytes} bytes exceeds the process bound of \
                 {process_bound_bytes} bytes"
            ),
            Self::ProcessBoundIsZero => {
                f.write_str("invalid memory authority configuration: the process bound is zero")
            }
            Self::MetadataLimitIsZero { registry } => write!(
                f,
                "invalid memory authority configuration: the {registry} registry limit is zero"
            ),
        }
    }
}

impl Error for ConfigError {}

impl CapacityError {
    pub(crate) fn with_requested(mut self, requested: u64) -> Self {
        match &mut self {
            Self::QueryLimit(r) | Self::ImpossibleRequest(r) | Self::ShortageCandidate(r) => {
                r.requested = requested
            }
            _ => {}
        }
        self
    }
}
