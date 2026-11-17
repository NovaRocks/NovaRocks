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

//! Neutral messages between logical execution and one physical attempt.
//!
//! Tokens and grant references are correlation values, never authorization.
//! The issuing owner retains the actual permits and rich request allocations.
//! Correlation identities cannot mint authority. Each request prepays exactly
//! one reply position; cross-core failure messages live in logical_port.

use crate::{AttemptActivationIdentity, MonotonicInstant, SelectedTaskTermination};
use novarocks_execution_contract::root_result::RootResultEnd;
use novarocks_execution_contract::{
    OperationOutcome, ParticipationAdmissionTicketReceipt, ParticipationRef, TaskIdentity,
    TaskStatus,
};
use novarocks_types::BackendProcessId;
use std::num::NonZeroU64;

/// Process-local request identity in the original activation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AttemptRequestToken(NonZeroU64);
impl AttemptRequestToken {
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Identity of one fact or action. It does not identify or own executable work.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AttemptActionId(NonZeroU64);
impl AttemptActionId {
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Deterministic correlation identity issued by the original logical core.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AttemptAuthorizationRef(NonZeroU64);
impl AttemptAuthorizationRef {
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Initialization-only evidence. Cleanup remains with the active isolation owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptInheritedAdmission {
    pub participation: ParticipationRef,
    pub receipt: ParticipationAdmissionTicketReceipt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptCompletionPolicy {
    AcceptedRootSuccessSeal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptLifecyclePolicy {
    pub completion: AttemptCompletionPolicy,
    pub retain_diagnostics: bool,
}

/// Passive projection of the original accepted computation cut. It does not
/// grant publication, cleanup, or a newly extended observation window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptComputationCut {
    pub activation: AttemptActivationIdentity,
    pub root: TaskIdentity,
    pub cut: MonotonicInstant,
    pub diagnostic_deadline: MonotonicInstant,
    pub convergence_deadline: MonotonicInstant,
    pub end: RootResultEnd,
}

/// The logical owner's frozen activation facts, separate from host plan projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptActivation {
    pub identity: AttemptActivationIdentity,
    pub policy: AttemptLifecyclePolicy,
    pub inherited_admissions: Box<[AttemptInheritedAdmission]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptStopReason {
    Client,
    Deadline,
    DrainDeadline,
    Shutdown,
    LogicalExecutionFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptConvergenceMode {
    Normal,
    Cleanup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptParticipationConvergence {
    NeverEstablished,
    WorkerStoppedAndParticipationFenced,
    WorkerProcessReplaced,
}

/// Unknown means only that finite remote tracking ended, never actual Worker exit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttemptConvergenceClosure {
    /// Rows are assembled under the charged original pass. Vec avoids an
    /// unbounded shrink/copy when the last row transfers to publication.
    Evidence(Vec<(ParticipationRef, AttemptParticipationConvergence)>),
    RemoteTrackingEnded {
        // The same immutable frozen set, not a new per-progress graph copy.
        exact_participations: std::sync::Arc<[ParticipationRef]>,
    },
}

/// Exact classification accepted alongside the root status in the same serial step.
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "the accepted root projection retains the original selected Task termination by value"
)]
pub enum AttemptRootFailure {
    None,
    DerivedPending,
    Authoritative(SelectedTaskTermination),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptRootProjection {
    pub status: TaskStatus,
    pub failure: AttemptRootFailure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptAuthorizationDecision {
    AdmissionStarted(AttemptAuthorizationRef),
    AdmissionGranted,
    AdmissionRetryable,
    EstablishAuthorized(AttemptAuthorizationRef),
}

/// Original QA configuration frozen for this activation's metadata pricing.
/// This is neither capacity nor permission to issue or retry an authorization;
/// QA remains the sole author of its budget and every authorization decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptAuthorizationLimits {
    pub activation: AttemptActivationIdentity,
    pub establish_per_participation: std::num::NonZeroUsize,
    pub abort_per_participation: std::num::NonZeroUsize,
}

pub const ABORT_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(50);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptIssueSettlement {
    TransportOwned,
    DefinitelyUnsent,
    TransportUnknown,
    PreWorkerRejected,
    Worker(OperationOutcome),
}

/// Exact contribution references selected by the original attempt Core.
/// These references grant no authority to read or retain the original data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterDiagnostic {
    pub contributions: Vec<(BackendProcessId, NonZeroU64)>,
    pub complete: bool,
}
