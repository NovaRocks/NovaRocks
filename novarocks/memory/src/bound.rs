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

//! Third-party bounded steps (MEM-1 wave-1 T02).
//!
//! A bound covers one short step — a page, a batch — including its scratch,
//! its output, its failure tail and its applicable background work. It is an
//! upper bound on capacity, not a measurement: while it is open, the account
//! holds `O`, and `O` is deliberately not reported as known live allocation.
//!
//! # Why the window is short
//!
//! A conservative bound held for a whole reader's lifetime would depress
//! utilisation for as long as that reader lived, and would keep claiming
//! capacity long after the step it was sized for finished. So a bound covers
//! a step, and long-lived third-party state is either converted into a proven
//! charge or reported by the observation tier instead. An indefinitely
//! extended bound is not a substitute for either.
//!
//! # Converting requires a set relation
//!
//! Turning `O` into `L` is only sound when the measured output is *part of*
//! the allocation the bound covered. Then the commitment does not change: the
//! same bytes stop being an upper bound and start being proven. When the
//! output is a separate copy, the original external memory and the new output
//! are both real at the same time, so both are charged and the caller needs
//! its own capacity for the copy. [`SetRelation`] makes the caller state which
//! case it is in, so the proof is at the call site rather than in a comment.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::account::AccountHandle;
use crate::charge::Charge;
use crate::error::CapacityError;
use crate::ids::AccountId;

/// How a measured output relates to the allocation a bound covered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SetRelation {
    /// The measured output is part of the allocation the bound covered, so
    /// converting it changes nothing about the total commitment.
    WithinAuthorisedAllocation,
    /// The output is a separate copy. The external memory and the copy are
    /// both alive, so both are charged for the overlap, and the copy needs its
    /// own capacity rather than this bound's.
    SeparatelyCopied,
}

/// Why a conversion was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConvertError {
    /// The measured output is larger than the bound that was supposed to
    /// cover it, so the bound was not in fact an upper bound. The bound stays
    /// open and the caller must treat its own sizing proof as broken.
    ExceedsBound {
        /// The account holding the bound.
        scope: AccountId,
        /// Bytes measured.
        measured: u64,
        /// Bytes the bound still covers.
        covered: u64,
    },
    /// The output is a separate copy, so it cannot be converted out of this
    /// bound. The caller obtains its own capacity for the copy and keeps this
    /// bound open until the external memory is really gone.
    SeparateCopyNeedsOwnCapacity {
        /// The account holding the bound.
        scope: AccountId,
        /// Bytes measured in the copy.
        measured: u64,
    },
    /// The bound has already been settled.
    AlreadySettled {
        /// The account that held the bound.
        scope: AccountId,
    },
}

impl std::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExceedsBound {
                scope,
                measured,
                covered,
            } => write!(
                f,
                "cannot convert a bound for {scope}: measured {measured} bytes exceed the \
                 {covered} bytes it covers, so the upper bound was not one"
            ),
            Self::SeparateCopyNeedsOwnCapacity { scope, measured } => write!(
                f,
                "cannot convert a bound for {scope}: the {measured} measured bytes are a separate \
                 copy and need their own capacity while the external memory is still alive"
            ),
            Self::AlreadySettled { scope } => {
                write!(
                    f,
                    "cannot convert a bound for {scope}: it is already settled"
                )
            }
        }
    }
}

impl std::error::Error for ConvertError {}

/// An open upper bound covering one third-party step.
///
/// Dropping it settles the bound and returns the uncovered capacity. Settling
/// is not proof that the third party released anything: it means this
/// authorisation is over. Where the third party may still hold memory, the
/// caller converts what it can prove and reports the rest through the
/// observation tier.
#[derive(Debug)]
pub struct ExternalBound {
    account: AccountHandle,
    covered: AtomicU64,
    settled: AtomicBool,
}

impl ExternalBound {
    pub(crate) fn new(account: AccountHandle, bytes: u64) -> Self {
        Self {
            account,
            covered: AtomicU64::new(bytes),
            settled: AtomicBool::new(false),
        }
    }

    /// Returns the account holding this bound.
    pub fn account(&self) -> &AccountHandle {
        &self.account
    }

    /// Returns the account's identity.
    pub fn account_id(&self) -> AccountId {
        self.account.id()
    }

    /// Returns the capacity this bound still covers.
    pub fn covered_bytes(&self) -> u64 {
        self.covered.load(Ordering::Acquire)
    }

    /// Reports whether the bound has settled.
    pub fn is_settled(&self) -> bool {
        self.settled.load(Ordering::Acquire)
    }

    /// Converts measured output into a proven charge.
    ///
    /// Only [`SetRelation::WithinAuthorisedAllocation`] converts. A separate
    /// copy is refused with the reason, because charging it out of this bound
    /// would report one allocation where there are two.
    pub fn convert(
        &self,
        measured_bytes: u64,
        relation: SetRelation,
    ) -> Result<Charge, ConvertError> {
        if self.settled.load(Ordering::Acquire) {
            return Err(ConvertError::AlreadySettled {
                scope: self.account.id(),
            });
        }
        if relation == SetRelation::SeparatelyCopied {
            return Err(ConvertError::SeparateCopyNeedsOwnCapacity {
                scope: self.account.id(),
                measured: measured_bytes,
            });
        }
        let mut current = self.covered.load(Ordering::Acquire);
        loop {
            if measured_bytes > current {
                return Err(ConvertError::ExceedsBound {
                    scope: self.account.id(),
                    measured: measured_bytes,
                    covered: current,
                });
            }
            match self.covered.compare_exchange_weak(
                current,
                current - measured_bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.account.convert_bounded_to_live(measured_bytes);
                    return Ok(Charge::new(self.account.clone(), measured_bytes));
                }
                Err(observed) => current = observed,
            }
        }
    }

    /// Settles the bound, returning the capacity it still covered.
    ///
    /// A function returning is not evidence that a hidden cache, a background
    /// task or an escaped output has gone away. Settle only when the step's
    /// real lifetime is over.
    pub fn settle(self) -> u64 {
        self.settle_inner()
    }

    fn settle_inner(&self) -> u64 {
        if self.settled.swap(true, Ordering::AcqRel) {
            return 0;
        }
        let remaining = self.covered.swap(0, Ordering::AcqRel);
        self.account.release_bounded(remaining);
        remaining
    }
}

impl Drop for ExternalBound {
    fn drop(&mut self) {
        self.settle_inner();
    }
}

impl AccountHandle {
    /// Opens an upper bound without going through a grant first.
    ///
    /// The capacity is still obtained before the step runs: this is the same
    /// request-then-authorise sequence, expressed in one call for the common
    /// case where the bound is the only thing the step needs.
    pub fn begin_bounded_step(&self, bytes: u64) -> Result<ExternalBound, CapacityError> {
        let grant = self.request_grant(bytes)?;
        match grant.begin_external(bytes) {
            Ok(bound) => Ok(bound),
            // The exact amount just granted cannot exceed its own remainder.
            Err(_) => Err(CapacityError::Unsupported {
                detail: "grant remainder shrank before the bounded step opened",
            }),
        }
    }
}
