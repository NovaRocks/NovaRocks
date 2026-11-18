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

//! Pure qualification identity, bounded fact validation and original horizons.
//! The isolation reservation and admission grants remain with the Native owner.

use crate::input::{
    QualificationIdentity, QualificationSettlement, QualifiedAdmission, QualifiedReplacementFacts,
};
use novarocks_coordination_contract::{
    ExecutionFailureInput, ExecutionFailureSite, MonotonicInstant, QueryExecutionError,
};
use novarocks_execution_contract::{
    ParticipationRef, SafeDetail, TaskOperationId,
    failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
};
use std::{collections::BTreeSet, num::NonZeroU16, sync::Arc, time::Duration};

/// These are observations selected by the original core, never host resources.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReplacementPrerequisites {
    pub reachable_participations_closed: bool,
    pub attempt_isolation_proven: bool,
    pub stale_usage_accounted: bool,
    pub new_capacity_admitted: bool,
}

impl ReplacementPrerequisites {
    pub(crate) const fn complete(self) -> bool {
        self.reachable_participations_closed
            && self.attempt_isolation_proven
            && self.stale_usage_accounted
            && self.new_capacity_admitted
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplacementQualificationFailure {
    Rejected,
    InvalidReservation,
    Expired,
    OutcomeUnknown,
    EffectOwnerClosed,
    MismatchedReceipt,
    DomainRefused(FailureFact),
}

impl FailureProjection for ReplacementQualificationFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, code, observed_stop) = match self {
            Self::Rejected => (FailureClass::Control, 94, false),
            Self::InvalidReservation => (FailureClass::Invariant, 95, false),
            Self::Expired => (FailureClass::Control, 96, false),
            Self::OutcomeUnknown => (FailureClass::Dependency, 97, false),
            Self::EffectOwnerClosed => (FailureClass::Control, 98, true),
            Self::MismatchedReceipt => (FailureClass::Invariant, 100, false),
            Self::DomainRefused(fact) => return *fact,
        };
        let reason = FailureReason::new(domain::QUERY, NonZeroU16::new(code).unwrap());
        if observed_stop {
            FailureFact::stop_observation(reason)
        } else {
            FailureFact::new(class, reason)
        }
    }
}

impl ReplacementQualificationFailure {
    pub fn safe_detail(self) -> SafeDetail {
        SafeDetail::truncating(match self {
            Self::Rejected => "attempt replacement qualification was refused",
            Self::InvalidReservation => {
                "attempt replacement qualification returned an invalid reservation"
            }
            Self::Expired => "attempt replacement qualification expired",
            Self::OutcomeUnknown => "attempt replacement qualification outcome is unknown",
            Self::EffectOwnerClosed => "attempt replacement qualification effect owner is closed",
            Self::MismatchedReceipt => {
                "attempt replacement receipt does not match its exact operation and identity"
            }
            Self::DomainRefused(_) => "owning service refused replacement qualification",
        })
    }

    pub fn error(self) -> QueryExecutionError {
        QueryExecutionError::from_execution(ExecutionFailureInput::new(
            self.failure_fact(),
            self.safe_detail(),
            ExecutionFailureSite::Coordinator,
        ))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct QualificationRequest {
    pub identity: QualificationIdentity,
    /// Supplied by the single logical core's seeded OperationIds author.
    pub operation_id: TaskOperationId,
    pub failed_participations: Arc<[ParticipationRef]>,
    pub replacement_participations: Arc<[ParticipationRef]>,
    pub issued_at: MonotonicInstant,
    pub expires_at: MonotonicInstant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QualificationPhase {
    AwaitDispatch,
    AwaitSettlement,
    Validating,
    Qualified,
    Activating,
    Activated,
    Failed,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QualificationProgress {
    Pending,
    Qualified,
    Failed,
}

#[derive(Debug)]
pub(crate) struct QualificationState {
    request: QualificationRequest,
    phase: QualificationPhase,
    dispatched_at: Option<MonotonicInstant>,
    effective_expiry: MonotonicInstant,
    facts: Option<QualifiedReplacementFacts>,
    failure: Option<QueryExecutionError>,
    expected: BTreeSet<ParticipationRef>,
    admitted: BTreeSet<ParticipationRef>,
    expected_cursor: usize,
    admission_cursor: usize,
    shortest_ticket: Option<Duration>,
}

impl QualificationState {
    pub(crate) fn new(
        request: QualificationRequest,
    ) -> Result<Self, ReplacementQualificationFailure> {
        if request.identity.failed.query_id() != request.identity.replacement.query_id()
            || request.identity.replacement.attempt_id() <= request.identity.failed.attempt_id()
            || request.expires_at <= request.issued_at
        {
            return Err(ReplacementQualificationFailure::InvalidReservation);
        }
        let effective_expiry = request.expires_at;
        Ok(Self {
            request,
            phase: QualificationPhase::AwaitDispatch,
            dispatched_at: None,
            effective_expiry,
            facts: None,
            failure: None,
            expected: BTreeSet::new(),
            admitted: BTreeSet::new(),
            expected_cursor: 0,
            admission_cursor: 0,
            shortest_ticket: None,
        })
    }

    pub(crate) const fn request(&self) -> &QualificationRequest {
        &self.request
    }
    pub(crate) const fn phase(&self) -> QualificationPhase {
        self.phase
    }
    pub(crate) const fn dispatched_at(&self) -> Option<MonotonicInstant> {
        self.dispatched_at
    }
    pub(crate) const fn deadline(&self) -> MonotonicInstant {
        self.effective_expiry
    }
    pub(crate) fn facts(&self) -> Option<&QualifiedReplacementFacts> {
        self.facts.as_ref()
    }
    pub(crate) fn failure(&self) -> Option<&QueryExecutionError> {
        self.failure.as_ref()
    }

    fn verify_header(
        &self,
        identity: QualificationIdentity,
        operation_id: TaskOperationId,
    ) -> Result<(), ReplacementQualificationFailure> {
        if identity != self.request.identity || operation_id != self.request.operation_id {
            Err(ReplacementQualificationFailure::MismatchedReceipt)
        } else {
            Ok(())
        }
    }

    /// The real handoff is independently reported; effect creation and receipt
    /// arrival cannot stand in for it or renew the original request horizon.
    pub(crate) fn record_dispatched(
        &mut self,
        identity: QualificationIdentity,
        operation_id: TaskOperationId,
        dispatched_at: MonotonicInstant,
    ) -> Result<(), ReplacementQualificationFailure> {
        self.verify_header(identity, operation_id)?;
        if self.phase != QualificationPhase::AwaitDispatch || dispatched_at < self.request.issued_at
        {
            return Err(ReplacementQualificationFailure::MismatchedReceipt);
        }
        if dispatched_at.has_reached(self.request.expires_at) {
            return Err(ReplacementQualificationFailure::Expired);
        }
        self.dispatched_at = Some(dispatched_at);
        self.phase = QualificationPhase::AwaitSettlement;
        Ok(())
    }

    /// Starts incremental validation. Every participant is processed by
    /// validate_next, so a large successor cannot monopolize one actor turn.
    pub(crate) fn settle(
        &mut self,
        identity: QualificationIdentity,
        operation_id: TaskOperationId,
        settlement: QualificationSettlement,
        now: MonotonicInstant,
    ) -> Result<QualificationProgress, ReplacementQualificationFailure> {
        self.verify_header(identity, operation_id)?;
        let awaiting_dispatch = self.phase == QualificationPhase::AwaitDispatch;
        if self.phase != QualificationPhase::AwaitSettlement
            && !(awaiting_dispatch && matches!(&settlement, QualificationSettlement::Rejected(_)))
        {
            return Err(ReplacementQualificationFailure::MismatchedReceipt);
        }
        if now.has_reached(self.effective_expiry) {
            return Err(ReplacementQualificationFailure::Expired);
        }
        match settlement {
            QualificationSettlement::Qualified(facts) => {
                self.verify_header(facts.identity, facts.operation_id)?;
                if facts.topology_revision == 0
                    || facts.admissions.len() != self.request.replacement_participations.len()
                {
                    return Err(ReplacementQualificationFailure::InvalidReservation);
                }
                self.facts = Some(facts);
                self.phase = QualificationPhase::Validating;
                Ok(QualificationProgress::Pending)
            }
            QualificationSettlement::Rejected(error) => {
                self.failure = Some(error);
                self.phase = QualificationPhase::Failed;
                Ok(QualificationProgress::Failed)
            }
            QualificationSettlement::OutcomeUnknown => {
                self.failure = Some(ReplacementQualificationFailure::OutcomeUnknown.error());
                self.phase = QualificationPhase::Failed;
                Ok(QualificationProgress::Failed)
            }
        }
    }

    /// Examines at most one expected participant or original admission grant.
    /// The caller charges one step before every invocation, including the
    /// final constant-work transition. No effects are emitted by this module.
    pub(crate) fn validate_next(
        &mut self,
        now: MonotonicInstant,
    ) -> Result<QualificationProgress, ReplacementQualificationFailure> {
        if self.phase != QualificationPhase::Validating {
            return Err(ReplacementQualificationFailure::MismatchedReceipt);
        }
        if now.has_reached(self.effective_expiry) {
            return Err(ReplacementQualificationFailure::Expired);
        }
        if let Some(participation) = self
            .request
            .replacement_participations
            .get(self.expected_cursor)
        {
            if participation.query_execution_id() != self.request.identity.replacement
                || !self.expected.insert(*participation)
            {
                return Err(ReplacementQualificationFailure::InvalidReservation);
            }
            self.expected_cursor += 1;
            return Ok(QualificationProgress::Pending);
        }
        let facts = self
            .facts
            .as_ref()
            .expect("validation retains the original qualified facts");
        if let Some(admission) = facts.admissions.get(self.admission_cursor) {
            validate_admission(self.request.identity.replacement, admission)?;
            let participation = admission.request.participation();
            if !self.expected.contains(&participation) || !self.admitted.insert(participation) {
                return Err(ReplacementQualificationFailure::InvalidReservation);
            }
            let validity = admission.receipt.valid_for().get();
            self.shortest_ticket = Some(
                self.shortest_ticket
                    .map_or(validity, |old| old.min(validity)),
            );
            let dispatched_at = self
                .dispatched_at
                .expect("qualified validation follows the original dispatch");
            self.effective_expiry = self
                .effective_expiry
                .min(dispatched_at.saturating_add(validity));
            if now.has_reached(self.effective_expiry) {
                return Err(ReplacementQualificationFailure::Expired);
            }
            self.admission_cursor += 1;
            return Ok(QualificationProgress::Pending);
        }
        let dispatched_at = self
            .dispatched_at
            .ok_or(ReplacementQualificationFailure::InvalidReservation)?;
        if let Some(validity) = self.shortest_ticket {
            self.effective_expiry = self
                .effective_expiry
                .min(dispatched_at.saturating_add(validity));
        }
        if now.has_reached(self.effective_expiry) {
            return Err(ReplacementQualificationFailure::Expired);
        }
        self.phase = QualificationPhase::Qualified;
        Ok(QualificationProgress::Qualified)
    }

    pub(crate) fn begin_activation(
        &mut self,
        now: MonotonicInstant,
        prerequisites: ReplacementPrerequisites,
    ) -> Result<&QualifiedReplacementFacts, ReplacementQualificationFailure> {
        if self.phase != QualificationPhase::Qualified || !prerequisites.complete() {
            return Err(ReplacementQualificationFailure::InvalidReservation);
        }
        if now.has_reached(self.effective_expiry) {
            return Err(ReplacementQualificationFailure::Expired);
        }
        self.phase = QualificationPhase::Activating;
        Ok(self
            .facts
            .as_ref()
            .expect("qualified state retains original admission facts"))
    }

    /// Activation returns only its exact correlation. The core continues to use
    /// the already-validated original facts rather than re-authoring grants
    /// from another copy carried by a completion.
    pub(crate) fn activation_completed(
        &mut self,
        identity: QualificationIdentity,
        operation_id: TaskOperationId,
        now: MonotonicInstant,
    ) -> Result<(), ReplacementQualificationFailure> {
        self.verify_header(identity, operation_id)?;
        if self.phase != QualificationPhase::Activating {
            return Err(ReplacementQualificationFailure::MismatchedReceipt);
        }
        if now.has_reached(self.effective_expiry) {
            return Err(ReplacementQualificationFailure::Expired);
        }
        self.phase = QualificationPhase::Activated;
        Ok(())
    }

    /// Metadata remains until the step owner has observed the real effect exit
    /// and executed finish/abandon for the host-owned reservation.
    pub(crate) fn close(&mut self) {
        self.phase = QualificationPhase::Closed;
    }
}

pub(crate) fn validate_admission(
    replacement: novarocks_types::QueryExecutionId,
    admission: &QualifiedAdmission,
) -> Result<(), ReplacementQualificationFailure> {
    if admission.request.participation() != admission.receipt.participation()
        || admission.request.valid_for() != admission.receipt.valid_for()
        || admission.request.participation().query_execution_id() != replacement
    {
        Err(ReplacementQualificationFailure::InvalidReservation)
    } else {
        Ok(())
    }
}

/// One logical preparation window. Repeated attempts reuse it without renewal.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PreparationWindow {
    deadline: Option<MonotonicInstant>,
}
impl PreparationWindow {
    pub(crate) fn open_once(
        &mut self,
        now: MonotonicInstant,
        valid_for: Duration,
    ) -> MonotonicInstant {
        *self
            .deadline
            .get_or_insert_with(|| now.saturating_add(valid_for))
    }
    pub(crate) const fn deadline(self) -> Option<MonotonicInstant> {
        self.deadline
    }
    pub(crate) fn expired(self, now: MonotonicInstant) -> bool {
        self.deadline
            .is_some_and(|deadline| now.has_reached(deadline))
    }
}

#[cfg(test)]
mod tests;
