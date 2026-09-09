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

//! Typed rejections.
//!
//! Every refusal carries the scope, the constraint that refused it, the
//! requested amount and the policy version it was judged under. Consumers
//! branch on the variant and the fields; they must never parse the message.
//! Messages are English and exist for humans reading a log.

use std::error::Error;
use std::fmt;

use crate::ids::{AccountId, PolicyVersion};

/// Which bounded registry a metadata refusal came from.
///
/// The core's own metadata is budgeted like any other growing state, so an
/// exhausted registry is a first-class typed refusal rather than an
/// unexplained failure. `budget` owns the limits; this label names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetadataRegistryLabel {
    /// Accounts in the strict tree.
    Accounts,
    /// Retention leases and pins.
    Holders,
    /// Registered reclaimers.
    Reclaimers,
    /// Buffered observation events.
    Events,
}

impl MetadataRegistryLabel {
    /// Returns the label used in messages and snapshots.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Accounts => "account",
            Self::Holders => "holder",
            Self::Reclaimers => "reclaimer",
            Self::Events => "event",
        }
    }
}

impl fmt::Display for MetadataRegistryLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Which constraint refused a request.
///
/// The distinction matters to policy: a process-capacity refusal is a
/// node-wide condition, an account-policy refusal is that work's own limit,
/// and a grant-remainder refusal is a caller bug or an under-sized grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConstraintKind {
    /// The stable managed capacity `B` of the process authority.
    ProcessCapacity,
    /// A policy limit installed on the account itself.
    AccountPolicy,
    /// A policy limit installed on an ancestor account.
    AncestorPolicy,
    /// The unfulfilled remainder of a specific grant.
    GrantRemainder,
    /// The account is not permitted to grow because it is over policy.
    GrowthFrozen,
}

impl ConstraintKind {
    /// Returns the label used in messages and snapshots.
    pub const fn label(self) -> &'static str {
        match self {
            Self::ProcessCapacity => "process capacity",
            Self::AccountPolicy => "account policy",
            Self::AncestorPolicy => "ancestor policy",
            Self::GrantRemainder => "grant remainder",
            Self::GrowthFrozen => "growth frozen",
        }
    }
}

impl fmt::Display for ConstraintKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// A refusal to grant, fulfil or move capacity.
///
/// Refusals never leave a partial charge behind: an account's counters are
/// exactly what they were before the refused call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapacityError {
    /// A constraint refused the request. `available` is what the refusing
    /// constraint could still offer at the moment of the decision, which is
    /// advisory: a concurrent request may take it first.
    Denied {
        /// The account the request was made against.
        scope: AccountId,
        /// The constraint that refused.
        constraint: ConstraintKind,
        /// Bytes requested.
        requested: u64,
        /// Bytes the refusing constraint could still offer.
        available: u64,
        /// Policy version the decision was taken under.
        version: PolicyVersion,
    },
    /// The scope was cancelled or closed to growth, so no new capacity is
    /// issued regardless of the balance.
    Cancelled {
        /// The account the request was made against.
        scope: AccountId,
    },
    /// The account already holds more than its policy allows, so growth is
    /// frozen until an arbitrator resolves the excess.
    FrozenByExcess {
        /// The account the request was made against.
        scope: AccountId,
        /// Bytes held beyond the installed policy.
        excess_bytes: u64,
    },
    /// A bounded metadata registry is full. The caller must fail the
    /// operation; no live charge is ever discarded to make room.
    MetadataExhausted {
        /// Which registry is full.
        registry: MetadataRegistryLabel,
        /// The configured capacity of that registry.
        limit: u32,
    },
    /// The request names a shape the core deliberately does not support, such
    /// as an account parented outside its authority.
    Unsupported {
        /// A short English explanation.
        detail: &'static str,
    },
}

impl fmt::Display for CapacityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Denied {
                scope,
                constraint,
                requested,
                available,
                version,
            } => write!(
                f,
                "capacity denied for {scope} by {constraint}: requested {requested} bytes, \
                 {available} bytes available under {version}"
            ),
            Self::Cancelled { scope } => {
                write!(f, "capacity denied for {scope}: scope is closed to growth")
            }
            Self::FrozenByExcess {
                scope,
                excess_bytes,
            } => write!(
                f,
                "capacity denied for {scope}: growth frozen while {excess_bytes} bytes exceed policy"
            ),
            Self::MetadataExhausted { registry, limit } => write!(
                f,
                "memory core {registry} registry is full at its configured limit of {limit} entries"
            ),
            Self::Unsupported { detail } => {
                write!(f, "unsupported memory core request: {detail}")
            }
        }
    }
}

impl Error for CapacityError {}

impl CapacityError {
    /// Returns the account the refusal was reported against, when the variant
    /// names one.
    pub const fn scope(&self) -> Option<AccountId> {
        match self {
            Self::Denied { scope, .. }
            | Self::Cancelled { scope }
            | Self::FrozenByExcess { scope, .. } => Some(*scope),
            Self::MetadataExhausted { .. } | Self::Unsupported { .. } => None,
        }
    }

    /// Reports whether waiting for capacity could plausibly change the
    /// outcome.
    ///
    /// This is advisory input for an arbitrator, not a retry promise: a
    /// denial that is retryable in principle can still never be satisfied.
    /// Cancellation, exhausted metadata and unsupported shapes are never
    /// resolved by waiting.
    pub const fn may_resolve_by_waiting(&self) -> bool {
        match self {
            Self::Denied { .. } | Self::FrozenByExcess { .. } => true,
            Self::Cancelled { .. } | Self::MetadataExhausted { .. } | Self::Unsupported { .. } => {
                false
            }
        }
    }
}

/// A refusal to turn granted capacity into a live charge.
///
/// Fulfilment inside a grant's remainder cannot fail on capacity, which is the
/// property that lets a caller commit an allocation it has already made. The
/// remaining failures are a cancelled scope and a caller asking for more than
/// the grant holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FulfilError {
    /// The scope lost the race against cancellation or revocation.
    Cancelled {
        /// The grant that was being fulfilled.
        grant: crate::ids::GrantId,
    },
    /// The request exceeds the grant's unfulfilled remainder.
    ExceedsRemainder {
        /// The grant that was being fulfilled.
        grant: crate::ids::GrantId,
        /// Bytes requested.
        requested: u64,
        /// Bytes still unfulfilled on that grant.
        remainder: u64,
    },
}

impl fmt::Display for FulfilError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled { grant } => {
                write!(f, "cannot fulfil {grant}: its scope is cancelled")
            }
            Self::ExceedsRemainder {
                grant,
                requested,
                remainder,
            } => write!(
                f,
                "cannot fulfil {requested} bytes from {grant}: only {remainder} bytes remain \
                 unfulfilled"
            ),
        }
    }
}

impl Error for FulfilError {}

/// A refusal to move an existing charge to another sponsor.
///
/// A failed transfer never modifies the source: the debt stays exactly where
/// it was, and no common ancestor ever observes a gap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferError {
    /// The destination branch refused the debt.
    Denied {
        /// The underlying capacity refusal from the destination branch.
        cause: CapacityError,
    },
    /// The charge has already been released, so there is nothing to move.
    AlreadyReleased,
    /// Source and destination are the same account, which is not a move.
    SameAccount {
        /// The account named twice.
        scope: AccountId,
    },
    /// The two accounts belong to different authorities, so no common
    /// ancestor exists and the debt cannot be moved without double charging.
    ForeignAuthority,
}

impl fmt::Display for TransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Denied { cause } => write!(f, "sponsor transfer denied: {cause}"),
            Self::AlreadyReleased => {
                f.write_str("sponsor transfer rejected: the charge is already released")
            }
            Self::SameAccount { scope } => {
                write!(
                    f,
                    "sponsor transfer rejected: source and destination are both {scope}"
                )
            }
            Self::ForeignAuthority => f.write_str(
                "sponsor transfer rejected: the accounts belong to different authorities",
            ),
        }
    }
}

impl Error for TransferError {}

/// An invalid authority configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denial_reports_scope_constraint_and_version_without_string_parsing() {
        let error = CapacityError::Denied {
            scope: AccountId::new(4),
            constraint: ConstraintKind::ProcessCapacity,
            requested: 4096,
            available: 1024,
            version: PolicyVersion::new(2),
        };
        assert_eq!(error.scope(), Some(AccountId::new(4)));
        assert!(error.may_resolve_by_waiting());
        let rendered = error.to_string();
        assert!(rendered.contains("account#4"), "{rendered}");
        assert!(rendered.contains("process capacity"), "{rendered}");
        assert!(rendered.contains("policy-v2"), "{rendered}");
    }

    #[test]
    fn cancellation_and_exhaustion_are_not_resolved_by_waiting() {
        assert!(
            !CapacityError::Cancelled {
                scope: AccountId::new(1)
            }
            .may_resolve_by_waiting()
        );
        assert!(
            !CapacityError::MetadataExhausted {
                registry: MetadataRegistryLabel::Accounts,
                limit: 8,
            }
            .may_resolve_by_waiting()
        );
        assert!(
            !CapacityError::Unsupported {
                detail: "second process root"
            }
            .may_resolve_by_waiting()
        );
    }

    #[test]
    fn frozen_by_excess_is_distinguishable_from_a_plain_denial() {
        let frozen = CapacityError::FrozenByExcess {
            scope: AccountId::new(9),
            excess_bytes: 512,
        };
        assert!(matches!(frozen, CapacityError::FrozenByExcess { .. }));
        assert!(frozen.to_string().contains("512"));
    }

    #[test]
    fn config_error_reports_all_three_quantities() {
        let error = ConfigError::CapacityExceedsProcessBound {
            capacity_bytes: 900,
            headroom_budget_bytes: 200,
            process_bound_bytes: 1000,
        };
        let rendered = error.to_string();
        assert!(rendered.contains("900"), "{rendered}");
        assert!(rendered.contains("200"), "{rendered}");
        assert!(rendered.contains("1000"), "{rendered}");
    }

    #[test]
    fn fulfil_and_transfer_errors_render_their_subjects() {
        let fulfil = FulfilError::ExceedsRemainder {
            grant: crate::ids::GrantId::new(3),
            requested: 64,
            remainder: 16,
        };
        assert!(fulfil.to_string().contains("grant#3"));
        let transfer = TransferError::SameAccount {
            scope: AccountId::new(5),
        };
        assert!(transfer.to_string().contains("account#5"));
        assert!(
            TransferError::Denied {
                cause: CapacityError::Cancelled {
                    scope: AccountId::new(5)
                }
            }
            .to_string()
            .contains("closed to growth")
        );
    }
}
