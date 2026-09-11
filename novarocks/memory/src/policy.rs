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

//! Versioned policy limits (MEM-1 wave-1 T02).
//!
//! A constraint carries its scope, unit, dimension and version, and policies
//! do not overwrite each other. Lowering a limit erases nothing: existing
//! commitments may exceed the new limit, the excess is displayed honestly,
//! growth stops, and revocable grants are asked back rather than deducted by
//! fiat.

use std::fmt;

use crate::ids::PolicyVersion;

/// The unit a limit is expressed in.
///
/// Only bytes exist today. The unit is still explicit because a limit without
/// a stated unit is exactly the kind of number that later gets compared
/// against a different one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LimitUnit {
    /// Managed backing capacity, in bytes, in the unit the adapters declare.
    Bytes,
}

impl LimitUnit {
    /// Returns the label used in messages and snapshots.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Bytes => "bytes",
        }
    }
}

impl fmt::Display for LimitUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Which product dimension a limit came from.
///
/// The core enforces every installed limit identically. The dimension exists
/// so a refusal can say which product rule refused, and so two rules cannot be
/// silently collapsed into one number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LimitDimension {
    /// A per-work limit, such as a query memory limit.
    Work,
    /// A resource-group limit.
    ResourceGroup,
    /// A limit an operator installed for a specific service.
    Service,
}

impl LimitDimension {
    /// Returns the label used in messages and snapshots.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Work => "work",
            Self::ResourceGroup => "resource-group",
            Self::Service => "service",
        }
    }
}

impl fmt::Display for LimitDimension {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// One installed limit.
///
/// The version is what makes a lowered limit auditable: a snapshot reports the
/// version its numbers were judged under, so a consumer can tell "this reading
/// is stale" from "the policy changed underneath me".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyLimit {
    bytes: u64,
    unit: LimitUnit,
    dimension: LimitDimension,
    version: PolicyVersion,
}

impl PolicyLimit {
    /// Builds a byte limit for one dimension at one version.
    pub const fn bytes(bytes: u64, dimension: LimitDimension, version: PolicyVersion) -> Self {
        Self {
            bytes,
            unit: LimitUnit::Bytes,
            dimension,
            version,
        }
    }

    /// Returns the limit in its declared unit.
    pub const fn limit_bytes(self) -> u64 {
        self.bytes
    }

    /// Returns the unit.
    pub const fn unit(self) -> LimitUnit {
        self.unit
    }

    /// Returns the product dimension the limit came from.
    pub const fn dimension(self) -> LimitDimension {
        self.dimension
    }

    /// Returns the version.
    pub const fn version(self) -> PolicyVersion {
        self.version
    }

    /// Returns how much of the limit remains against a commitment.
    ///
    /// Saturates at zero: a commitment above the limit reports no remainder,
    /// and the overshoot is reported separately as excess rather than as a
    /// negative remainder.
    pub const fn remaining_against(self, committed_bytes: u64) -> u64 {
        self.bytes.saturating_sub(committed_bytes)
    }

    /// Returns the bytes held beyond this limit.
    pub const fn excess_against(self, committed_bytes: u64) -> u64 {
        committed_bytes.saturating_sub(self.bytes)
    }
}

impl fmt::Display for PolicyLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {} limit at {}",
            self.bytes, self.unit, self.dimension, self.version
        )
    }
}

/// What installing a limit did to an account.
///
/// A lowered limit never rewrites history. When the account already holds more
/// than the new limit allows, the install succeeds, the excess is reported, and
/// the account stops growing; it does not retroactively fail the commitments
/// that were validly issued under the previous version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyInstallOutcome {
    /// The version the account now carries.
    pub version: PolicyVersion,
    /// The commitment observed at install time.
    pub committed_bytes: u64,
    /// Bytes held beyond the new limit, if any.
    pub excess_bytes: u64,
    /// Whether the install closed the account to further growth.
    pub growth_frozen: bool,
}

impl PolicyInstallOutcome {
    /// Reports whether the account is over its new policy.
    pub const fn is_over_policy(&self) -> bool {
        self.excess_bytes > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_limit_carries_its_unit_dimension_and_version() {
        let limit = PolicyLimit::bytes(1024, LimitDimension::Work, PolicyVersion::new(3));
        assert_eq!(limit.limit_bytes(), 1024);
        assert_eq!(limit.unit(), LimitUnit::Bytes);
        assert_eq!(limit.dimension(), LimitDimension::Work);
        assert_eq!(limit.version(), PolicyVersion::new(3));
        let rendered = limit.to_string();
        assert!(rendered.contains("bytes"), "{rendered}");
        assert!(rendered.contains("work"), "{rendered}");
        assert!(rendered.contains("policy-v3"), "{rendered}");
    }

    #[test]
    fn remainder_saturates_and_excess_is_reported_separately() {
        let limit = PolicyLimit::bytes(100, LimitDimension::Work, PolicyVersion::new(1));
        assert_eq!(limit.remaining_against(40), 60);
        assert_eq!(limit.excess_against(40), 0);
        assert_eq!(limit.remaining_against(160), 0);
        assert_eq!(limit.excess_against(160), 60);
    }

    #[test]
    fn install_outcome_distinguishes_over_policy_from_within_policy() {
        let within = PolicyInstallOutcome {
            version: PolicyVersion::new(1),
            committed_bytes: 10,
            excess_bytes: 0,
            growth_frozen: false,
        };
        assert!(!within.is_over_policy());

        let over = PolicyInstallOutcome {
            version: PolicyVersion::new(2),
            committed_bytes: 160,
            excess_bytes: 60,
            growth_frozen: true,
        };
        assert!(over.is_over_policy());
        assert_eq!(over.committed_bytes, 160);
    }

    #[test]
    fn dimensions_are_distinct_so_two_rules_cannot_collapse() {
        assert_ne!(LimitDimension::Work, LimitDimension::ResourceGroup);
        assert_eq!(LimitDimension::ResourceGroup.label(), "resource-group");
        assert_eq!(LimitDimension::Service.to_string(), "service");
    }
}
