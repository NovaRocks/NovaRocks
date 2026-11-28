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

use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{
    EffectIdentity, EffectPath, EffectScope, ManagedMvTarget, ProcessIncarnation, UnsettledEffect,
};

const MAX_EVIDENCE_TEXT_BYTES: usize = 1024;

/// Absolute wall-clock point used for evidence that must survive an FE restart.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ManagementTimestamp(u64);

impl ManagementTimestamp {
    pub const fn from_unix_millis(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_unix_millis(self) -> u64 {
        self.0
    }

    pub(super) fn checked_add(self, duration: Duration) -> Result<Self, ReadmissionError> {
        let millis = duration.as_nanos().div_ceil(1_000_000);
        let millis = u64::try_from(millis).map_err(|_| ReadmissionError::Overflow)?;
        self.0
            .checked_add(millis)
            .map(Self)
            .ok_or(ReadmissionError::Overflow)
    }
}

pub trait ManagementClock: Send + Sync {
    fn now(&self) -> Result<ManagementTimestamp, ReadmissionError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemManagementClock;

impl ManagementClock for SystemManagementClock {
    fn now(&self) -> Result<ManagementTimestamp, ReadmissionError> {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ReadmissionError::ClockRegressed)?;
        let millis = u64::try_from(elapsed.as_millis()).map_err(|_| ReadmissionError::Overflow)?;
        Ok(ManagementTimestamp::from_unix_millis(millis))
    }
}

#[derive(Clone)]
pub struct VirtualManagementClock {
    now: Arc<Mutex<ManagementTimestamp>>,
}

impl VirtualManagementClock {
    pub fn new(now: ManagementTimestamp) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
        }
    }

    pub fn set(&self, now: ManagementTimestamp) {
        *lock(&self.now) = now;
    }
}

impl ManagementClock for VirtualManagementClock {
    fn now(&self) -> Result<ManagementTimestamp, ReadmissionError> {
        Ok(*lock(&self.now))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ReadmissionChallenge([u8; 16]);

impl ReadmissionChallenge {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IsolationEvidence {
    target: ManagedMvTarget,
    old_incarnation: ProcessIncarnation,
    isolated_at: ManagementTimestamp,
    basis: Arc<str>,
}

impl IsolationEvidence {
    pub fn try_new(
        target: ManagedMvTarget,
        old_incarnation: ProcessIncarnation,
        isolated_at: ManagementTimestamp,
        basis: impl AsRef<str>,
    ) -> Result<Self, ReadmissionError> {
        validate_evidence_text(basis.as_ref())?;
        Ok(Self {
            target,
            old_incarnation,
            isolated_at,
            basis: Arc::from(basis.as_ref()),
        })
    }

    pub const fn target(&self) -> &ManagedMvTarget {
        &self.target
    }

    pub const fn old_incarnation(&self) -> &ProcessIncarnation {
        &self.old_incarnation
    }

    pub const fn isolated_at(&self) -> ManagementTimestamp {
        self.isolated_at
    }

    pub fn basis(&self) -> &str {
        &self.basis
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActualCompletionEvidence {
    effect: EffectIdentity,
    target: ManagedMvTarget,
    scope: EffectScope,
    completed_at: ManagementTimestamp,
    basis: Arc<str>,
}

impl ActualCompletionEvidence {
    pub fn try_new(
        effect: EffectIdentity,
        target: ManagedMvTarget,
        scope: EffectScope,
        completed_at: ManagementTimestamp,
        basis: impl AsRef<str>,
    ) -> Result<Self, ReadmissionError> {
        validate_evidence_text(basis.as_ref())?;
        Ok(Self {
            effect,
            target,
            scope,
            completed_at,
            basis: Arc::from(basis.as_ref()),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteEffectLifetimeGuarantee {
    scope: EffectScope,
    lifetime: Duration,
    margin: Duration,
    source: Arc<str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteEffectGuaranteeBasis {
    ProviderServiceContract,
    DeploymentEnforcedBound,
    ClientRequestTimeout,
    GatewayTimeout,
    CredentialExpiry,
    IdempotencyRetention,
    EmpiricalPercentile,
}

impl RemoteEffectGuaranteeBasis {
    const fn proves_remote_effect_lifetime(self) -> bool {
        matches!(
            self,
            Self::ProviderServiceContract | Self::DeploymentEnforcedBound
        )
    }
}

impl RemoteEffectLifetimeGuarantee {
    pub fn try_new(
        scope: EffectScope,
        lifetime: Duration,
        margin: Duration,
        basis: RemoteEffectGuaranteeBasis,
        source: impl AsRef<str>,
    ) -> Result<Self, ReadmissionError> {
        if lifetime.is_zero() || !basis.proves_remote_effect_lifetime() {
            return Err(ReadmissionError::InvalidGuarantee);
        }
        validate_evidence_text(source.as_ref())?;
        Ok(Self {
            scope,
            lifetime,
            margin,
            source: Arc::from(source.as_ref()),
        })
    }

    pub const fn scope(&self) -> EffectScope {
        self.scope
    }

    pub const fn lifetime(&self) -> Duration {
        self.lifetime
    }

    pub const fn margin(&self) -> Duration {
        self.margin
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    /// The same bound, stated for a scope whose every path is guaranteed.
    ///
    /// A guarantee is declared per path, but an effect can span both, and a
    /// single-path bound never covers a spanning effect however long it is.
    /// Restating the deciding bound for the whole scope is what lets the
    /// window check see the fact the policy already established -- both paths
    /// are bounded, and this is the longer of the two. Nothing is invented:
    /// the lifetime, margin and source are carried across unchanged.
    pub(crate) fn covering(&self, scope: EffectScope) -> Self {
        Self {
            scope,
            lifetime: self.lifetime,
            margin: self.margin,
            source: Arc::clone(&self.source),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadmissionMode {
    AutomaticWhenGuaranteed,
    OperatorDeclarationOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManualReadmissionDeclaration {
    challenge: ReadmissionChallenge,
    effect: EffectIdentity,
    target: ManagedMvTarget,
    old_incarnation: ProcessIncarnation,
    scope: EffectScope,
    operator: Arc<str>,
    basis: Arc<str>,
    declared_at: ManagementTimestamp,
}

impl ManualReadmissionDeclaration {
    pub fn try_new(
        challenge: ReadmissionChallenge,
        effect: EffectIdentity,
        target: ManagedMvTarget,
        old_incarnation: ProcessIncarnation,
        scope: EffectScope,
        operator: impl AsRef<str>,
        basis: impl AsRef<str>,
        declared_at: ManagementTimestamp,
    ) -> Result<Self, ReadmissionError> {
        validate_evidence_text(operator.as_ref())?;
        validate_evidence_text(basis.as_ref())?;
        Ok(Self {
            challenge,
            effect,
            target,
            old_incarnation,
            scope,
            operator: Arc::from(operator.as_ref()),
            basis: Arc::from(basis.as_ref()),
            declared_at,
        })
    }

    pub fn operator(&self) -> &str {
        &self.operator
    }

    pub fn basis(&self) -> &str {
        &self.basis
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PermitBasis {
    ActualCompletion,
    PolicyWindow,
    OperatorDeclaration,
}

/// Permission to perform a new exact lake observation. It is intentionally not
/// a management-write permit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadmissionPermit {
    effect: EffectIdentity,
    target: ManagedMvTarget,
    original_disposition_unknown: bool,
    basis: PermitBasis,
}

impl ReadmissionPermit {
    pub const fn effect(&self) -> EffectIdentity {
        self.effect
    }

    pub const fn target(&self) -> &ManagedMvTarget {
        &self.target
    }

    pub const fn preserves_unknown_disposition(&self) -> bool {
        self.original_disposition_unknown
    }
}

/// Stateful evaluator detects wall-clock rollback and one-shot challenge use.
/// The state is diagnostic process runtime, never a durable attempt authority.
#[derive(Default)]
pub struct ReadmissionEvaluator {
    last_clock_reading: Option<ManagementTimestamp>,
    clock_regressed: bool,
    active_challenges: HashSet<ReadmissionChallenge>,
    consumed_challenges: HashSet<ReadmissionChallenge>,
}

impl ReadmissionEvaluator {
    pub fn issue_challenge(
        &mut self,
        challenge: ReadmissionChallenge,
    ) -> Result<(), ReadmissionError> {
        if self.active_challenges.contains(&challenge)
            || self.consumed_challenges.contains(&challenge)
        {
            return Err(ReadmissionError::ReusedChallenge);
        }
        self.active_challenges.insert(challenge);
        Ok(())
    }

    pub fn from_actual_completion(
        &mut self,
        effect: &UnsettledEffect,
        isolation: &IsolationEvidence,
        evidence: &ActualCompletionEvidence,
    ) -> Result<ReadmissionPermit, ReadmissionError> {
        validate_isolation(effect, isolation)?;
        let responsibility = effect.responsibility();
        if evidence.effect != responsibility.identity()
            || evidence.target != *responsibility.target()
            || !evidence.scope.covers(responsibility.scope())
            || evidence.completed_at < responsibility.last_possible_dispatch_at()
        {
            return Err(ReadmissionError::EvidenceScopeMismatch);
        }
        Ok(permit(effect, PermitBasis::ActualCompletion))
    }

    pub fn from_policy_window(
        &mut self,
        effect: &UnsettledEffect,
        isolation: &IsolationEvidence,
        guarantee: &RemoteEffectLifetimeGuarantee,
        mode: ReadmissionMode,
        clock: &dyn ManagementClock,
    ) -> Result<ReadmissionPermit, ReadmissionError> {
        if mode == ReadmissionMode::OperatorDeclarationOnly {
            return Err(ReadmissionError::ManualMode);
        }
        validate_isolation(effect, isolation)?;
        if !guarantee.scope.covers(effect.responsibility().scope()) {
            return Err(ReadmissionError::GuaranteeScopeMismatch);
        }
        if self.clock_regressed {
            return Err(ReadmissionError::ClockRegressed);
        }
        let now = clock.now()?;
        if self
            .last_clock_reading
            .is_some_and(|previous| now < previous)
        {
            self.clock_regressed = true;
            return Err(ReadmissionError::ClockRegressed);
        }
        self.last_clock_reading = Some(now);
        let conservative_dispatch = effect
            .responsibility()
            .last_possible_dispatch_at()
            .max(isolation.isolated_at());
        let deadline = conservative_dispatch
            .checked_add(guarantee.lifetime)?
            .checked_add(guarantee.margin)?;
        if now < deadline {
            return Err(ReadmissionError::WindowNotElapsed { deadline, now });
        }
        Ok(permit(effect, PermitBasis::PolicyWindow))
    }

    pub fn from_manual_declaration(
        &mut self,
        effect: &UnsettledEffect,
        isolation: &IsolationEvidence,
        declaration: &ManualReadmissionDeclaration,
    ) -> Result<ReadmissionPermit, ReadmissionError> {
        self.consume_challenge(declaration.challenge)?;
        Self::permit_declared(effect, isolation, declaration)
    }

    /// Spend one challenge.
    ///
    /// A challenge binds a statement to the state it was read from, not to one
    /// effect: an operator declares that a writer is isolated, and that single
    /// fact may settle several of its effects at once. Spending it separately
    /// is what lets a statement cover them all while still being usable only
    /// once.
    pub fn consume_challenge(
        &mut self,
        challenge: ReadmissionChallenge,
    ) -> Result<(), ReadmissionError> {
        if !self.active_challenges.remove(&challenge) {
            return Err(ReadmissionError::MissingChallenge);
        }
        self.consumed_challenges.insert(challenge);
        Ok(())
    }

    /// Whether one declaration covers one effect. The challenge is not
    /// consulted here; [`Self::consume_challenge`] owns that.
    pub fn permit_declared(
        effect: &UnsettledEffect,
        isolation: &IsolationEvidence,
        declaration: &ManualReadmissionDeclaration,
    ) -> Result<ReadmissionPermit, ReadmissionError> {
        validate_isolation(effect, isolation)?;
        let responsibility = effect.responsibility();
        if declaration.effect != responsibility.identity()
            || declaration.target != *responsibility.target()
            || declaration.old_incarnation != *responsibility.dispatching_incarnation()
            || !declaration.scope.covers(responsibility.scope())
            || declaration.declared_at < isolation.isolated_at()
        {
            return Err(ReadmissionError::EvidenceScopeMismatch);
        }
        Ok(permit(effect, PermitBasis::OperatorDeclaration))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GarbageCollectionSafetyPolicy {
    max_local_attempt: Duration,
    remote_reference_tail: Option<RemoteEffectLifetimeGuarantee>,
    remote_delete_tail: Option<RemoteEffectLifetimeGuarantee>,
    clock_skew: Duration,
    listing_visibility: Duration,
    scheduling_margin: Duration,
}

impl GarbageCollectionSafetyPolicy {
    pub const fn new(
        max_local_attempt: Duration,
        remote_reference_tail: Option<RemoteEffectLifetimeGuarantee>,
        remote_delete_tail: Option<RemoteEffectLifetimeGuarantee>,
        clock_skew: Duration,
        listing_visibility: Duration,
        scheduling_margin: Duration,
    ) -> Self {
        Self {
            max_local_attempt,
            remote_reference_tail,
            remote_delete_tail,
            clock_skew,
            listing_visibility,
            scheduling_margin,
        }
    }

    pub fn minimum_safe_age(&self) -> Result<Duration, ReadmissionError> {
        let reference_tail = self
            .remote_reference_tail
            .as_ref()
            .filter(|guarantee| guarantee.scope().contains(EffectPath::CatalogCommit))
            .ok_or(ReadmissionError::MissingReferenceGuarantee)?;
        self.deletion_effect_window()?;
        checked_duration_sum(&[
            self.max_local_attempt,
            reference_tail.lifetime(),
            reference_tail.margin(),
            self.clock_skew,
            self.listing_visibility,
            self.scheduling_margin,
        ])
    }

    /// The wait required before an unknown GC deletion can be considered
    /// unable to produce another object-store effect. This is deliberately
    /// separate from the catalog-reference tail used by `minimum_safe_age`.
    pub fn deletion_effect_window(&self) -> Result<Duration, ReadmissionError> {
        let delete_tail = self
            .remote_delete_tail
            .as_ref()
            .filter(|guarantee| guarantee.scope().contains(EffectPath::ObjectDeletion))
            .ok_or(ReadmissionError::MissingDeleteGuarantee)?;
        checked_duration_sum(&[delete_tail.lifetime(), delete_tail.margin()])
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadmissionError {
    InvalidEvidence,
    InvalidGuarantee,
    TargetMismatch,
    IncarnationMismatch,
    EvidenceScopeMismatch,
    GuaranteeScopeMismatch,
    WriterNotIsolated,
    ManualMode,
    MissingChallenge,
    ReusedChallenge,
    ClockRegressed,
    WindowNotElapsed {
        deadline: ManagementTimestamp,
        now: ManagementTimestamp,
    },
    MissingReferenceGuarantee,
    MissingDeleteGuarantee,
    /// The process-local evaluator state is unusable, so no continuation
    /// decision can be made without risking a replayed challenge.
    EvaluatorUnavailable,
    Overflow,
}

impl fmt::Display for ReadmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEvidence => formatter.write_str("readmission evidence is invalid"),
            Self::InvalidGuarantee => formatter.write_str("remote effect guarantee is invalid"),
            Self::TargetMismatch => formatter.write_str("readmission evidence targets another MV"),
            Self::IncarnationMismatch => {
                formatter.write_str("readmission evidence targets another incarnation")
            }
            Self::EvidenceScopeMismatch => {
                formatter.write_str("readmission evidence does not cover the exact effect")
            }
            Self::GuaranteeScopeMismatch => {
                formatter.write_str("remote guarantee does not cover the effect path")
            }
            Self::WriterNotIsolated => {
                formatter.write_str("old writer isolation is not established")
            }
            Self::ManualMode => formatter.write_str("automatic readmission is disabled"),
            Self::MissingChallenge => {
                formatter.write_str("manual declaration challenge is absent or stale")
            }
            Self::ReusedChallenge => {
                formatter.write_str("manual declaration challenge was already issued")
            }
            Self::ClockRegressed => formatter.write_str("management wall clock regressed"),
            Self::WindowNotElapsed { .. } => {
                formatter.write_str("remote effect lifetime window has not elapsed")
            }
            Self::MissingReferenceGuarantee => {
                formatter.write_str("automatic GC requires a catalog reference effect guarantee")
            }
            Self::MissingDeleteGuarantee => {
                formatter.write_str("automatic GC requires an object deletion effect guarantee")
            }
            Self::EvaluatorUnavailable => {
                formatter.write_str("management continuation evaluator is unavailable")
            }
            Self::Overflow => formatter.write_str("management deadline arithmetic overflowed"),
        }
    }
}

impl std::error::Error for ReadmissionError {}

fn permit(effect: &UnsettledEffect, basis: PermitBasis) -> ReadmissionPermit {
    ReadmissionPermit {
        effect: effect.responsibility().identity(),
        target: effect.responsibility().target().clone(),
        original_disposition_unknown: true,
        basis,
    }
}

fn validate_isolation(
    effect: &UnsettledEffect,
    isolation: &IsolationEvidence,
) -> Result<(), ReadmissionError> {
    if isolation.target() != effect.responsibility().target() {
        return Err(ReadmissionError::TargetMismatch);
    }
    if isolation.old_incarnation() != effect.responsibility().dispatching_incarnation() {
        return Err(ReadmissionError::IncarnationMismatch);
    }
    if isolation.isolated_at() < effect.responsibility().last_possible_dispatch_at() {
        return Err(ReadmissionError::WriterNotIsolated);
    }
    Ok(())
}

fn validate_evidence_text(value: &str) -> Result<(), ReadmissionError> {
    if value.is_empty() || value.len() > MAX_EVIDENCE_TEXT_BYTES || value.contains('\0') {
        return Err(ReadmissionError::InvalidEvidence);
    }
    Ok(())
}

fn checked_duration_sum(parts: &[Duration]) -> Result<Duration, ReadmissionError> {
    parts.iter().try_fold(Duration::ZERO, |sum, part| {
        sum.checked_add(*part).ok_or(ReadmissionError::Overflow)
    })
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
