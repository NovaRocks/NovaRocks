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

//! Closed effect boundary for qualifying one read-only attempt replacement.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, QueryContextAdmissionTicketReceipt, QueryContextRef,
    TaskOperationId,
};
use novarocks_types::identity::QueryExecutionId;
use tokio::sync::{mpsc, watch};

use super::{LogicalExecutionActorId, MonotonicInstant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplacementQualificationIdentity {
    actor: LogicalExecutionActorId,
    failed: QueryExecutionId,
    replacement: QueryExecutionId,
    eligibility_generation: u64,
}

impl ReplacementQualificationIdentity {
    pub(crate) const fn new(
        actor: LogicalExecutionActorId,
        failed: QueryExecutionId,
        replacement: QueryExecutionId,
        eligibility_generation: u64,
    ) -> Self {
        Self {
            actor,
            failed,
            replacement,
            eligibility_generation,
        }
    }

    pub const fn actor(self) -> LogicalExecutionActorId {
        self.actor
    }
    pub const fn failed(self) -> QueryExecutionId {
        self.failed
    }
    pub const fn replacement(self) -> QueryExecutionId {
        self.replacement
    }
    pub const fn eligibility_generation(self) -> u64 {
        self.eligibility_generation
    }
}

#[derive(Clone, Debug)]
pub struct ReplacementQualificationRequest {
    operation_id: TaskOperationId,
    identity: ReplacementQualificationIdentity,
    failed_contexts: Arc<[QueryContextRef]>,
    replacement_contexts: Arc<[QueryContextRef]>,
    issued_at: MonotonicInstant,
    conservative_expiry: MonotonicInstant,
    absolute_expiry: std::time::Instant,
}

impl ReplacementQualificationRequest {
    pub(crate) fn new(
        identity: ReplacementQualificationIdentity,
        failed_contexts: Arc<[QueryContextRef]>,
        replacement_contexts: Arc<[QueryContextRef]>,
        issued_at: MonotonicInstant,
        conservative_expiry: MonotonicInstant,
    ) -> Self {
        let absolute_expiry = std::time::Instant::now()
            .checked_add(conservative_expiry.saturating_duration_since(issued_at))
            .unwrap_or_else(std::time::Instant::now);
        Self {
            operation_id: TaskOperationId::new_v7(),
            identity,
            failed_contexts,
            replacement_contexts,
            issued_at,
            conservative_expiry,
            absolute_expiry,
        }
    }

    pub const fn operation_id(&self) -> TaskOperationId {
        self.operation_id
    }
    pub const fn identity(&self) -> ReplacementQualificationIdentity {
        self.identity
    }
    pub fn failed_contexts(&self) -> &[QueryContextRef] {
        self.failed_contexts.as_ref()
    }
    pub fn replacement_contexts(&self) -> &[QueryContextRef] {
        self.replacement_contexts.as_ref()
    }
    pub const fn issued_at(&self) -> MonotonicInstant {
        self.issued_at
    }
    /// Actor-owned upper bound for reserving, returning, and activating all
    /// resources in this replacement. Effect owners must stop work and settle
    /// the submission when this deadline or its cancellation signal fires.
    pub const fn conservative_expiry(&self) -> MonotonicInstant {
        self.conservative_expiry
    }

    pub const fn absolute_expiry(&self) -> std::time::Instant {
        self.absolute_expiry
    }
}

#[derive(Debug)]
pub struct QualifiedWorkerAdmission {
    request: AcquireQueryContextAdmissionTicket,
    receipt: QueryContextAdmissionTicketReceipt,
}

impl QualifiedWorkerAdmission {
    pub fn try_new(
        replacement: QueryExecutionId,
        request: AcquireQueryContextAdmissionTicket,
        receipt: QueryContextAdmissionTicketReceipt,
    ) -> Result<Self, ReplacementQualificationFailure> {
        if request.context() != receipt.context()
            || request.valid_for() != receipt.valid_for()
            || request.context().query_execution_id() != replacement
        {
            return Err(ReplacementQualificationFailure::InvalidReservation);
        }
        Ok(Self { request, receipt })
    }

    pub(crate) const fn request(&self) -> AcquireQueryContextAdmissionTicket {
        self.request
    }
    pub const fn receipt(&self) -> QueryContextAdmissionTicketReceipt {
        self.receipt
    }
    pub const fn context(&self) -> QueryContextRef {
        self.request.context()
    }

    fn copy_for_actor(&self) -> Self {
        Self {
            request: self.request,
            receipt: self.receipt,
        }
    }
}

/// Initialization-only evidence for one already actor-recorded Worker grant.
/// The active opaque owner retains the grant's cleanup responsibility; this
/// value carries neither the Acquire operation nor ownership of the ticket.
#[derive(Debug)]
pub struct ReplacementWorkerAdmissionEvidence {
    context: QueryContextRef,
    receipt: QueryContextAdmissionTicketReceipt,
}

impl ReplacementWorkerAdmissionEvidence {
    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn receipt(&self) -> QueryContextAdmissionTicketReceipt {
        self.receipt
    }
}

/// Move-only ownership of the candidate attempt's isolation and every exact
/// Worker admission grant described by `validate_binding`.
///
/// Before activation, `abandon` must cancel, release, or rehome supervision of
/// all those grants. Successful activation transfers that responsibility to
/// the returned active owner, whose `finish` and `abandon` paths remain
/// distinct.
pub trait AttemptIsolationReservation: fmt::Debug + Send {
    fn identity(&self) -> ReplacementQualificationIdentity;
    fn topology_revision(&self) -> u64;
    /// Borrows the exact admission evidence owned by this reservation. The
    /// slice cannot outlive or be separated from the cleanup owner.
    fn admissions(&self) -> &[QualifiedWorkerAdmission];
    /// Proves that every exact reachable failed context is closed to new
    /// admission and work, its old eligibility is revoked, its registry usage
    /// remains owned as last-known/current-unknown residual accounting, and
    /// every candidate admission belongs to the exact isolated successor.
    /// The actor independently retains the same-scope `RetiredAttempt`
    /// obligation which charges old-attempt capacity. These are identity and
    /// accounting fences; neither asserts actual Worker stop or resource
    /// release for the failed attempt.
    fn validate_binding(
        &self,
        failed_contexts: &[QueryContextRef],
    ) -> Result<(), ReplacementQualificationFailure>;
    fn activate(
        self: Box<Self>,
    ) -> Result<Box<dyn ActiveAttemptIsolationOwner>, AttemptIsolationActivationFailure>;
    fn abandon(self: Box<Self>);
}

/// Failed activation returns the still-owned reservation so the actor can
/// explicitly abandon or rehome every Worker grant. A consumed guard can
/// never disappear through an error-only return.
pub struct AttemptIsolationActivationFailure {
    failure: ReplacementQualificationFailure,
    reservation: Box<dyn AttemptIsolationReservation>,
}

impl AttemptIsolationActivationFailure {
    pub fn new(
        failure: ReplacementQualificationFailure,
        reservation: Box<dyn AttemptIsolationReservation>,
    ) -> Self {
        Self {
            failure,
            reservation,
        }
    }

    fn into_parts(
        self,
    ) -> (
        ReplacementQualificationFailure,
        Box<dyn AttemptIsolationReservation>,
    ) {
        (self.failure, self.reservation)
    }
}

impl fmt::Debug for AttemptIsolationActivationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttemptIsolationActivationFailure")
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}

pub trait ActiveAttemptIsolationOwner: fmt::Debug + Send {
    fn execution(&self) -> QueryExecutionId;
    fn finish(self: Box<Self>);
    fn abandon(self: Box<Self>);
}

#[derive(Debug)]
pub struct QualifiedReplacementReservation {
    identity: ReplacementQualificationIdentity,
    reservation: Option<Box<dyn AttemptIsolationReservation>>,
}

impl QualifiedReplacementReservation {
    pub fn try_new(
        request: &ReplacementQualificationRequest,
        reservation: Box<dyn AttemptIsolationReservation>,
    ) -> Result<Self, ReplacementQualificationFailure> {
        let invalid = reservation.identity() != request.identity;
        if invalid {
            reservation.abandon();
            return Err(ReplacementQualificationFailure::InvalidReservation);
        }
        let admissions = reservation.admissions();
        let expected: BTreeSet<_> = request.replacement_contexts.iter().copied().collect();
        let actual: BTreeSet<_> = admissions
            .iter()
            .map(QualifiedWorkerAdmission::context)
            .collect();
        if expected.len() != request.replacement_contexts.len()
            || actual.len() != admissions.len()
            || expected != actual
            || reservation.topology_revision() == 0
        {
            reservation.abandon();
            return Err(ReplacementQualificationFailure::InvalidReservation);
        }
        if let Err(error) = reservation.validate_binding(request.failed_contexts()) {
            reservation.abandon();
            return Err(error);
        }
        Ok(Self {
            identity: request.identity,
            reservation: Some(reservation),
        })
    }

    pub const fn identity(&self) -> ReplacementQualificationIdentity {
        self.identity
    }

    pub(crate) fn conservative_expiry(
        &self,
        dispatched_at: MonotonicInstant,
        maximum_lifetime: std::time::Duration,
    ) -> MonotonicInstant {
        let effective = self
            .reservation
            .as_deref()
            .and_then(|reservation| {
                reservation
                    .admissions()
                    .iter()
                    .map(|admission| admission.receipt().valid_for().get())
                    .min()
            })
            .map_or(maximum_lifetime, |valid_for| {
                valid_for.min(maximum_lifetime)
            });
        dispatched_at.saturating_add(effective)
    }

    fn reject_unpublished(mut self) {
        if let Some(reservation) = self.reservation.take() {
            reservation.abandon();
        }
    }

    pub(crate) fn activate(
        mut self,
    ) -> Result<ActivatedReplacementReservation, ReplacementQualificationFailure> {
        let reservation = self
            .reservation
            .take()
            .expect("qualified replacement retains its opaque reservation");
        let admissions = reservation
            .admissions()
            .iter()
            .map(QualifiedWorkerAdmission::copy_for_actor)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let isolation = match reservation.activate() {
            Ok(isolation) => isolation,
            Err(failure) => {
                let (failure, reservation) = failure.into_parts();
                reservation.abandon();
                return Err(failure);
            }
        };
        if isolation.execution() != self.identity.replacement() {
            isolation.abandon();
            return Err(ReplacementQualificationFailure::InvalidReservation);
        }
        Ok(ActivatedReplacementReservation {
            execution: self.identity.replacement(),
            admissions: Some(admissions),
            isolation: Some(isolation),
        })
    }
}

impl Drop for QualifiedReplacementReservation {
    fn drop(&mut self) {
        if let Some(reservation) = self.reservation.take() {
            reservation.abandon();
        }
    }
}

pub(crate) struct ActivatedReplacementReservation {
    execution: QueryExecutionId,
    admissions: Option<Box<[QualifiedWorkerAdmission]>>,
    isolation: Option<Box<dyn ActiveAttemptIsolationOwner>>,
}

pub(crate) struct ActiveReplacementResources {
    execution: QueryExecutionId,
    admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
    isolation: Option<Box<dyn ActiveAttemptIsolationOwner>>,
}

impl ActivatedReplacementReservation {
    pub(crate) fn admissions(&self) -> &[QualifiedWorkerAdmission] {
        self.admissions.as_deref().unwrap_or_default()
    }
}

impl Drop for ActivatedReplacementReservation {
    fn drop(&mut self) {
        if let Some(isolation) = self.isolation.take() {
            isolation.abandon();
        }
    }
}

impl ActiveReplacementResources {
    pub(crate) fn replacement(mut activated: ActivatedReplacementReservation) -> Self {
        let admissions = activated.admissions.take().map(|admissions| {
            admissions
                .iter()
                .map(|admission| ReplacementWorkerAdmissionEvidence {
                    context: admission.context(),
                    receipt: admission.receipt(),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice()
        });
        Self {
            execution: activated.execution,
            admissions,
            isolation: activated.isolation.take(),
        }
    }

    pub(crate) fn take_admissions(&mut self) -> Option<Box<[ReplacementWorkerAdmissionEvidence]>> {
        self.admissions.take()
    }

    pub(crate) fn restore_admissions(
        &mut self,
        admissions: Box<[ReplacementWorkerAdmissionEvidence]>,
    ) -> Result<(), Box<[ReplacementWorkerAdmissionEvidence]>> {
        if self.admissions.is_some() {
            return Err(admissions);
        }
        self.admissions = Some(admissions);
        Ok(())
    }

    /// Closes resources after the actor has observed a definitive attempt
    /// lifecycle transition.
    pub(crate) fn finish(mut self) {
        if let Some(isolation) = self.isolation.take() {
            isolation.finish();
        }
    }
}

impl fmt::Debug for ActiveReplacementResources {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveReplacementResources")
            .field("execution", &self.execution)
            .field("admissions", &self.admissions)
            .field("isolation", &self.isolation)
            .finish()
    }
}

impl Drop for ActiveReplacementResources {
    fn drop(&mut self) {
        if let Some(isolation) = self.isolation.take() {
            isolation.abandon();
        }
    }
}

pub enum ReplacementQualificationEffectAdmission {
    Admitted(Box<dyn ReplacementQualificationEffectReservation>),
    Backpressured,
}

impl fmt::Debug for ReplacementQualificationEffectAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admitted(_) => formatter.write_str("Admitted(..)"),
            Self::Backpressured => formatter.write_str("Backpressured"),
        }
    }
}

pub trait ReplacementQualificationEffectReservation: fmt::Debug + Send {
    fn submit(self: Box<Self>, submission: ReplacementQualificationEffectSubmission);
}

pub trait ReplacementQualificationEffectPort: fmt::Debug + Send + Sync {
    fn subscribe_capacity(&self) -> watch::Receiver<u64>;
    fn try_reserve(
        &self,
        request: &ReplacementQualificationRequest,
    ) -> ReplacementQualificationEffectAdmission;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplacementQualificationFailure {
    Rejected,
    InvalidReservation,
    Expired,
    OutcomeUnknown,
    EffectOwnerClosed,
}

#[derive(Debug)]
pub(crate) enum ReplacementQualificationSettlement {
    Qualified(QualifiedReplacementReservation),
    Failed(ReplacementQualificationFailure),
}

#[derive(Debug)]
pub(crate) struct ReplacementQualificationEffectReceipt {
    operation_id: TaskOperationId,
    identity: ReplacementQualificationIdentity,
    settlement: ReplacementQualificationSettlement,
}

impl ReplacementQualificationEffectReceipt {
    pub(crate) fn into_parts(
        self,
    ) -> (
        TaskOperationId,
        ReplacementQualificationIdentity,
        ReplacementQualificationSettlement,
    ) {
        (self.operation_id, self.identity, self.settlement)
    }
}

#[derive(Debug)]
#[must_use = "replacement qualification effects must settle or report unknown outcome"]
pub struct ReplacementQualificationEffectSubmission {
    request: Arc<ReplacementQualificationRequest>,
    receipts: mpsc::UnboundedSender<ReplacementQualificationEffectReceipt>,
    cancellation: watch::Receiver<bool>,
    settled: bool,
}

impl ReplacementQualificationEffectSubmission {
    pub(crate) fn new(
        request: Arc<ReplacementQualificationRequest>,
        receipts: mpsc::UnboundedSender<ReplacementQualificationEffectReceipt>,
        cancellation: watch::Receiver<bool>,
    ) -> Self {
        Self {
            request,
            receipts,
            cancellation,
            settled: false,
        }
    }

    pub fn request(&self) -> &ReplacementQualificationRequest {
        self.request.as_ref()
    }

    /// Subscribes to actor cancellation. The effect owner must combine this
    /// signal with `request().conservative_expiry()`, clean up any acquired
    /// Worker grants, and settle or drop this submission.
    pub fn subscribe_cancellation(&self) -> watch::Receiver<bool> {
        self.cancellation.clone()
    }

    pub fn cancellation_requested(&self) -> bool {
        *self.cancellation.borrow()
    }

    pub fn qualified(
        mut self,
        reservation: QualifiedReplacementReservation,
    ) -> Result<(), ReplacementQualificationFailure> {
        if reservation.identity() != self.request.identity() {
            reservation.reject_unpublished();
            let _ = self.publish(ReplacementQualificationSettlement::Failed(
                ReplacementQualificationFailure::InvalidReservation,
            ));
            self.settled = true;
            return Err(ReplacementQualificationFailure::InvalidReservation);
        }
        self.publish(ReplacementQualificationSettlement::Qualified(reservation))?;
        self.settled = true;
        Ok(())
    }

    pub fn rejected(mut self) {
        let _ = self.publish(ReplacementQualificationSettlement::Failed(
            ReplacementQualificationFailure::Rejected,
        ));
        self.settled = true;
    }

    fn publish(
        &self,
        settlement: ReplacementQualificationSettlement,
    ) -> Result<(), ReplacementQualificationFailure> {
        self.receipts
            .send(ReplacementQualificationEffectReceipt {
                operation_id: self.request.operation_id,
                identity: self.request.identity,
                settlement,
            })
            .map_err(|_| ReplacementQualificationFailure::EffectOwnerClosed)
    }
}

impl Drop for ReplacementQualificationEffectSubmission {
    fn drop(&mut self) {
        if !self.settled {
            let _ = self.publish(ReplacementQualificationSettlement::Failed(
                ReplacementQualificationFailure::OutcomeUnknown,
            ));
        }
    }
}

pub(crate) fn receipt_channel() -> (
    mpsc::UnboundedSender<ReplacementQualificationEffectReceipt>,
    mpsc::UnboundedReceiver<ReplacementQualificationEffectReceipt>,
) {
    mpsc::unbounded_channel()
}
