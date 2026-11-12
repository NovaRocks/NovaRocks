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

//! Frontend projection of actor-owned Abort effects into Native task intents.
//!
//! The attempt bridge uses the original submission and settlement values.
//! TaskRound fixtures also retain the legacy bounded channel adapter.

use std::fmt;
use std::sync::Arc;
#[cfg(test)]
use std::{collections::VecDeque, num::NonZeroUsize, sync::Mutex};

#[cfg(test)]
use crate::task_execution::operation::OperationDispatchResult;
#[cfg(test)]
use novarocks_execution::task_execution::OperationKind;
use novarocks_execution::task_execution::{
    AbortCause, AbortParticipation, OperationOutcome, ParticipationReceipt, ParticipationState,
};
#[cfg(test)]
use novarocks_query_application::coordination::{
    AbortParticipationEffectAdmission, AbortParticipationEffectPort,
    AbortParticipationEffectReservation,
};
use novarocks_query_application::coordination::{
    AbortParticipationEffectSubmission, AbortParticipationIssueIdentity,
    LateAbortParticipationWorkerSettlement, ParticipationStandDownCause,
    ParticipationStandDownError,
};
#[cfg(test)]
use tokio::sync::{mpsc, watch};

use super::intent::{AckPayload, OperationAcknowledgement, OperationIntent};
#[cfg(test)]
use super::status_intake::StatusIntakeWake;

/// Bounded application-to-Native Abort projection.
#[cfg(test)]
pub(crate) struct NativeAbortEffectAdapter {
    effects: Option<mpsc::Sender<NativeAbortEffect>>,
    submission_order: Arc<Mutex<VecDeque<OperationIntent>>>,
    capacity_epoch: watch::Sender<u64>,
    wake: Arc<dyn StatusIntakeWake>,
    worker_closed:
        Option<Arc<dyn Fn(novarocks_execution_contract::ParticipationRef) + Send + Sync>>,
}

#[cfg(test)]
impl fmt::Debug for NativeAbortEffectAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAbortEffectAdapter")
            .field(
                "remaining_capacity",
                &self.effects.as_ref().map(mpsc::Sender::capacity),
            )
            .finish()
    }
}

#[cfg(test)]
impl NativeAbortEffectAdapter {
    pub(crate) fn bounded(
        capacity: NonZeroUsize,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> (Arc<Self>, NativeAbortEffectIntake) {
        let (capacity_epoch, _) = watch::channel(0);
        Self::bounded_with_capacity_epoch(capacity, wake, capacity_epoch)
    }

    /// Builds one attempt-local intake while publishing capacity changes on
    /// its logical execution's shared Abort port.
    pub(crate) fn bounded_with_capacity_epoch(
        capacity: NonZeroUsize,
        wake: Arc<dyn StatusIntakeWake>,
        capacity_epoch: watch::Sender<u64>,
    ) -> (Arc<Self>, NativeAbortEffectIntake) {
        Self::bounded_with_observer(capacity, wake, capacity_epoch, None)
    }

    pub(crate) fn bounded_with_observer(
        capacity: NonZeroUsize,
        wake: Arc<dyn StatusIntakeWake>,
        capacity_epoch: watch::Sender<u64>,
        worker_closed: Option<
            Arc<dyn Fn(novarocks_execution_contract::ParticipationRef) + Send + Sync>,
        >,
    ) -> (Arc<Self>, NativeAbortEffectIntake) {
        let (effects, receiver) = mpsc::channel(capacity.get());
        let submission_order = Arc::new(Mutex::new(VecDeque::with_capacity(capacity.get())));
        let adapter = Arc::new(Self {
            effects: Some(effects),
            submission_order: Arc::clone(&submission_order),
            capacity_epoch: capacity_epoch.clone(),
            wake,
            worker_closed,
        });
        let intake = NativeAbortEffectIntake {
            receiver,
            submission_order,
            capacity_epoch,
        };
        (adapter, intake)
    }
}

#[cfg(test)]
impl AbortParticipationEffectPort for NativeAbortEffectAdapter {
    fn subscribe_capacity(&self) -> watch::Receiver<u64> {
        self.capacity_epoch.subscribe()
    }

    fn try_reserve(
        &self,
        identity: AbortParticipationIssueIdentity,
    ) -> AbortParticipationEffectAdmission {
        let Some(effects) = self.effects.as_ref() else {
            return AbortParticipationEffectAdmission::Closed;
        };
        match effects.clone().try_reserve_owned() {
            Ok(permit) => AbortParticipationEffectAdmission::Admitted(Box::new(
                NativeAbortEffectReservation {
                    identity,
                    permit: Some(permit),
                    submission_order: Arc::clone(&self.submission_order),
                    capacity_epoch: self.capacity_epoch.clone(),
                    wake: Arc::clone(&self.wake),
                    worker_closed: self.worker_closed.clone(),
                },
            )),
            Err(mpsc::error::TrySendError::Full(_)) => {
                AbortParticipationEffectAdmission::Backpressured
            }
            Err(mpsc::error::TrySendError::Closed(_)) => AbortParticipationEffectAdmission::Closed,
        }
    }
}

#[cfg(test)]
impl Drop for NativeAbortEffectAdapter {
    fn drop(&mut self) {
        // Closing the final sender is the state transition the serial runner
        // must observe. Publish the wake only after that transition so a
        // runner cannot wake, still see an open empty intake, and park forever.
        drop(self.effects.take());
        self.wake.wake();
    }
}

/// The Registry-owned side of the bounded Abort handoff.
///
/// Receiving an item releases one adapter slot and publishes the capacity
/// epoch awaited by the logical-execution actor. The receiver is intentionally
/// move-only so one serial owner drains the effects.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct NativeAbortEffectIntake {
    receiver: mpsc::Receiver<NativeAbortEffect>,
    submission_order: Arc<Mutex<VecDeque<OperationIntent>>>,
    capacity_epoch: watch::Sender<u64>,
}

#[cfg(test)]
impl NativeAbortEffectIntake {
    /// Copies the exact front intent without releasing the adapter slot.
    ///
    /// The serial runner uses this description to obtain both dispatcher and
    /// process-transport capacity before it takes ownership of the effect.
    /// A producer serializes the carrier send and this preview under the same
    /// lock, so the preview cannot name a different channel entry.
    pub(crate) fn front_intent(&self) -> Result<OperationIntent, mpsc::error::TryRecvError> {
        let order = self
            .submission_order
            .lock()
            .expect("Native Abort effect submission order");
        if let Some(intent) = order.front() {
            return Ok(intent.clone());
        }
        if self.receiver.is_closed() {
            Err(mpsc::error::TryRecvError::Disconnected)
        } else {
            Err(mpsc::error::TryRecvError::Empty)
        }
    }

    pub(crate) fn try_recv(&mut self) -> Result<NativeAbortEffect, mpsc::error::TryRecvError> {
        let mut order = self
            .submission_order
            .lock()
            .expect("Native Abort effect submission order");
        let effect = self.receiver.try_recv()?;
        let preview = order
            .pop_front()
            .expect("every Native Abort carrier has one ordered preview");
        assert_eq!(
            preview.operation_id(),
            effect.intent().operation_id(),
            "Native Abort preview and carrier order diverged"
        );
        publish_capacity(&self.capacity_epoch);
        Ok(effect)
    }

    pub(crate) fn queued(&self) -> usize {
        self.submission_order
            .lock()
            .expect("Native Abort effect submission order")
            .len()
    }

    async fn recv(&mut self) -> Option<NativeAbortEffect> {
        let effect = self.receiver.recv().await?;
        let preview = self
            .submission_order
            .lock()
            .expect("Native Abort effect submission order")
            .pop_front()
            .expect("every Native Abort carrier has one ordered preview");
        assert_eq!(
            preview.operation_id(),
            effect.intent().operation_id(),
            "Native Abort preview and carrier order diverged"
        );
        publish_capacity(&self.capacity_epoch);
        Some(effect)
    }
}

#[cfg(test)]
struct NativeAbortEffectReservation {
    identity: AbortParticipationIssueIdentity,
    permit: Option<mpsc::OwnedPermit<NativeAbortEffect>>,
    submission_order: Arc<Mutex<VecDeque<OperationIntent>>>,
    capacity_epoch: watch::Sender<u64>,
    wake: Arc<dyn StatusIntakeWake>,
    worker_closed:
        Option<Arc<dyn Fn(novarocks_execution_contract::ParticipationRef) + Send + Sync>>,
}

#[cfg(test)]
impl fmt::Debug for NativeAbortEffectReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAbortEffectReservation")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
impl AbortParticipationEffectReservation for NativeAbortEffectReservation {
    fn submit(mut self: Box<Self>, submission: AbortParticipationEffectSubmission) {
        assert_eq!(
            submission.identity(),
            self.identity,
            "an Abort effect reservation may accept only its exact actor-owned identity"
        );
        let effect = NativeAbortEffect::from_submission(submission, self.worker_closed.clone());
        let intent = effect.intent().clone();
        let mut order = self
            .submission_order
            .lock()
            .expect("Native Abort effect submission order");
        self.permit
            .take()
            .expect("a live Abort reservation retains its bounded slot")
            .send(effect);
        order.push_back(intent);
        drop(order);
        self.wake.wake();
    }
}

#[cfg(test)]
impl Drop for NativeAbortEffectReservation {
    fn drop(&mut self) {
        if self.permit.is_some() {
            release_permit_then_publish_capacity(&mut self.permit, &self.capacity_epoch);
            self.wake.wake();
        }
    }
}

#[cfg(test)]
fn release_permit_then_publish_capacity(
    permit: &mut Option<mpsc::OwnedPermit<NativeAbortEffect>>,
    capacity_epoch: &watch::Sender<u64>,
) {
    if let Some(permit) = permit.take() {
        // Releasing capacity must happen before publishing its epoch; a
        // waiter that wakes first could otherwise observe the old full
        // channel and sleep without another notification.
        drop(permit);
        publish_capacity(capacity_epoch);
    }
}

fn native_cause(cause: ParticipationStandDownCause) -> AbortCause {
    match cause {
        ParticipationStandDownCause::LogicalExecutionCancelled
        | ParticipationStandDownCause::LogicalExecutionFailed => AbortCause::QueryFailed,
    }
}

#[cfg(test)]
fn publish_capacity(capacity_epoch: &watch::Sender<u64>) {
    capacity_epoch.send_modify(|epoch| {
        *epoch = epoch
            .checked_add(1)
            .expect("Abort effect capacity epoch exhausted");
    });
}

/// One exact Native Abort intent bound to its actor settlement authority.
#[must_use = "a Native Abort effect must enter lifecycle dispatch or retain its settlement"]
pub(crate) struct NativeAbortEffect {
    intent: OperationIntent,
    submission: Option<AbortParticipationEffectSubmission>,
    worker_closed:
        Option<Arc<dyn Fn(novarocks_execution_contract::ParticipationRef) + Send + Sync>>,
}

impl fmt::Debug for NativeAbortEffect {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAbortEffect")
            .field("intent", &self.intent)
            .finish_non_exhaustive()
    }
}

impl Drop for NativeAbortEffect {
    fn drop(&mut self) {
        if let Some(submission) = self.submission.take() {
            // Losing the frontend owner is a local protocol failure, not
            // evidence that a transport call had an unknown remote outcome.
            // Stop replay and leave residual convergence to the supervisor.
            let _ = submission.fail_closed();
        }
    }
}

impl NativeAbortEffect {
    pub(crate) fn from_submission(
        submission: AbortParticipationEffectSubmission,
        worker_closed: Option<
            Arc<dyn Fn(novarocks_execution_contract::ParticipationRef) + Send + Sync>,
        >,
    ) -> Self {
        let identity = submission.request().identity();
        let request = AbortParticipation::new(
            identity.operation_id(),
            identity.participation(),
            native_cause(identity.cause()),
        );
        Self {
            intent: OperationIntent::AbortParticipation(request),
            submission: Some(submission),
            worker_closed,
        }
    }

    pub(crate) fn intent(&self) -> &OperationIntent {
        &self.intent
    }

    /// Validates the acknowledgement without consuming actor authority.
    /// TaskRound uses this before it releases dispatcher ownership so a bad
    /// receipt cannot partially settle either side of the transaction.
    #[cfg(test)]
    pub(crate) fn validate_acknowledgement(
        &self,
        acknowledgement: &OperationAcknowledgement,
    ) -> Result<(), NativeAbortEffectAckError> {
        if acknowledgement.operation_id() != self.intent.operation_id() {
            return Err(NativeAbortEffectAckError::OperationIdentityMismatch);
        }
        if acknowledgement.kind() != OperationKind::AbortParticipation {
            return Err(NativeAbortEffectAckError::OperationKindMismatch);
        }
        if acknowledgement.dispatch_result() == OperationDispatchResult::TransportUnknown {
            return Ok(());
        }
        let expected_participation = match &self.intent {
            OperationIntent::AbortParticipation(request) => request.participation(),
            _ => unreachable!("a Native Abort effect always retains an Abort intent"),
        };
        closing_receipt(expected_participation, acknowledgement).map(drop)
    }

    /// Stops replay after a local protocol or dispatcher violation while the
    /// actor's residual supervisor retains the exact participation responsibility.
    pub(crate) fn fail_closed(mut self) -> Result<(), ParticipationStandDownError> {
        self.submission
            .take()
            .expect("a live Native Abort retains actor settlement authority")
            .fail_closed()
    }

    /// Returns an Abort that expired while still dispatcher-queued to the
    /// actor as definitely unsent. The actor may spend its next bounded
    /// authorization generation on the same immutable operation.
    pub(crate) fn definitely_unsent(mut self) -> Result<(), ParticipationStandDownError> {
        self.submission
            .take()
            .expect("a live Native Abort retains actor settlement authority")
            .definitely_unsent()
    }

    /// Another exact generation supplied the definitive Worker receipt while
    /// this carrier was still definitely unsent in the dispatcher.
    pub(crate) fn resolved_by_other_generation(mut self) {
        self.submission
            .take()
            .expect("a live Native Abort retains actor settlement authority")
            .resolved_by_other_generation();
    }

    pub(crate) fn transport_unknown(
        mut self,
    ) -> Result<NativeLateAbortEffect, ParticipationStandDownError> {
        let late = self
            .submission
            .take()
            .expect("original Abort submission")
            .transport_unknown()?;
        Ok(NativeLateAbortEffect::new(late, self.worker_closed.clone()))
    }

    pub(crate) fn worker_settled(
        mut self,
        receipt: ParticipationReceipt,
    ) -> Result<(), ParticipationStandDownError> {
        let participation = receipt.participation();
        self.submission
            .take()
            .expect("original Abort submission")
            .worker_settled(receipt)?;
        if let Some(observer) = &self.worker_closed {
            observer(participation);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn settle(
        self,
        acknowledgement: &OperationAcknowledgement,
    ) -> Result<NativeAbortEffectSettlement, NativeAbortEffectSettleError> {
        if let Err(reason) = self.validate_acknowledgement(acknowledgement) {
            return Err(NativeAbortEffectSettleError::rejected(reason, self));
        }

        match acknowledgement.dispatch_result() {
            OperationDispatchResult::IngressRejected(_)
            | OperationDispatchResult::NonWorkerRejected => {
                Err(NativeAbortEffectSettleError::rejected(
                    NativeAbortEffectAckError::MissingWorkerReceipt,
                    self,
                ))
            }
            OperationDispatchResult::TransportUnknown => self
                .transport_unknown()
                .map(NativeAbortEffectSettlement::TransportUnknown)
                .map_err(NativeAbortEffectSettleError::Application),
            OperationDispatchResult::WorkerReceipt(_) => {
                let expected_participation = match &self.intent {
                    OperationIntent::AbortParticipation(request) => request.participation(),
                    _ => unreachable!("a Native Abort effect always retains an Abort intent"),
                };
                let receipt = closing_receipt(expected_participation, acknowledgement)
                    .expect("Native Abort acknowledgement was validated before settlement");
                self.worker_settled(receipt)
                    .map_err(NativeAbortEffectSettleError::Application)?;
                Ok(NativeAbortEffectSettlement::WorkerSettled)
            }
        }
    }
}

/// Applies a definitive Worker receipt to an earlier transport-unknown issue.
///
/// This path deliberately bypasses `QueryTaskExecution::acknowledge`: that
/// dispatcher generation was already released by the unknown transport
/// outcome. The retained application authority is exact in both operation and
/// participation, so only its matching closing receipt may resolve it.
#[cfg(test)]
pub(crate) fn settle_late_abort_receipt(
    settlement: NativeLateAbortEffect,
    acknowledgement: &OperationAcknowledgement,
) -> Result<(), NativeLateAbortEffectSettleError> {
    let identity = settlement.identity();
    if acknowledgement.operation_id() != identity.operation_id() {
        return Err(NativeLateAbortEffectSettleError::rejected(
            NativeAbortEffectAckError::OperationIdentityMismatch,
            settlement,
        ));
    }
    if acknowledgement.kind() != OperationKind::AbortParticipation {
        return Err(NativeLateAbortEffectSettleError::rejected(
            NativeAbortEffectAckError::OperationKindMismatch,
            settlement,
        ));
    }
    if acknowledgement.dispatch_result() == OperationDispatchResult::TransportUnknown {
        return Err(NativeLateAbortEffectSettleError::rejected(
            NativeAbortEffectAckError::MissingWorkerReceipt,
            settlement,
        ));
    }
    let receipt = match closing_receipt(identity.participation(), acknowledgement) {
        Ok(receipt) => receipt,
        Err(reason) => {
            return Err(NativeLateAbortEffectSettleError::rejected(
                reason, settlement,
            ));
        }
    };
    settlement
        .worker_settled(receipt)
        .map_err(NativeLateAbortEffectSettleError::Application)
}

/// Frontend ownership of a transport-unknown generation's late settlement.
/// Dropping it is an explicit local failure, never another unknown outcome.
pub(crate) struct NativeLateAbortEffect {
    settlement: Option<LateAbortParticipationWorkerSettlement>,
    worker_closed:
        Option<Arc<dyn Fn(novarocks_execution_contract::ParticipationRef) + Send + Sync>>,
}

impl fmt::Debug for NativeLateAbortEffect {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeLateAbortEffect")
            .field("identity", &self.identity())
            .finish_non_exhaustive()
    }
}

impl NativeLateAbortEffect {
    fn new(
        settlement: LateAbortParticipationWorkerSettlement,
        worker_closed: Option<
            Arc<dyn Fn(novarocks_execution_contract::ParticipationRef) + Send + Sync>,
        >,
    ) -> Self {
        Self {
            settlement: Some(settlement),
            worker_closed,
        }
    }

    pub(crate) fn identity(&self) -> AbortParticipationIssueIdentity {
        self.settlement
            .as_ref()
            .expect("a live late Abort owner retains its settlement")
            .identity()
    }

    pub(crate) fn worker_settled(
        mut self,
        receipt: ParticipationReceipt,
    ) -> Result<(), ParticipationStandDownError> {
        let participation = self.identity().participation();
        self.settlement
            .take()
            .expect("a live late Abort owner retains its settlement")
            .worker_settled(receipt)?;
        if let Some(observer) = &self.worker_closed {
            observer(participation);
        }
        Ok(())
    }

    pub(crate) fn fail_closed(mut self) -> Result<(), ParticipationStandDownError> {
        self.settlement
            .take()
            .expect("a live late Abort owner retains its settlement")
            .fail_closed()
    }

    /// A newer unknown retains the same operation's late authority.
    pub(crate) fn superseded_by_newer_unknown(mut self) {
        drop(self.settlement.take());
    }

    /// Another exact generation published the definitive Worker fact.
    pub(crate) fn resolved_by_other_generation(mut self) {
        drop(self.settlement.take());
    }
}

impl Drop for NativeLateAbortEffect {
    fn drop(&mut self) {
        if let Some(settlement) = self.settlement.take() {
            let _ = settlement.fail_closed();
        }
    }
}

pub(crate) fn closing_receipt(
    expected_participation: novarocks_execution::task_execution::ParticipationRef,
    acknowledgement: &OperationAcknowledgement,
) -> Result<ParticipationReceipt, NativeAbortEffectAckError> {
    let Some(outcome) = acknowledgement.worker_outcome() else {
        return Err(NativeAbortEffectAckError::MissingWorkerReceipt);
    };
    if !legal_abort_closing_outcome(outcome) {
        return Err(NativeAbortEffectAckError::NonClosingWorkerOutcome(outcome));
    }
    let AckPayload::Participation(receipt) = acknowledgement.payload() else {
        return Err(NativeAbortEffectAckError::MissingParticipationReceipt);
    };
    if receipt.participation() != expected_participation {
        return Err(NativeAbortEffectAckError::ParticipationReceiptIdentityMismatch);
    }
    if !matches!(
        receipt.state(),
        ParticipationState::Aborting
            | ParticipationState::Releasing
            | ParticipationState::TerminalRetained
            | ParticipationState::Gone
    ) {
        return Err(NativeAbortEffectAckError::NonClosingParticipationReceipt);
    }
    Ok(receipt.clone())
}

fn legal_abort_closing_outcome(outcome: OperationOutcome) -> bool {
    matches!(
        outcome,
        OperationOutcome::Accepted
            | OperationOutcome::Idempotent
            | OperationOutcome::ParticipationTerminalReceipt
            | OperationOutcome::LeaseExpired
            | OperationOutcome::Gone
    )
}

#[cfg(test)]
#[derive(Debug)]
#[must_use = "the late settlement authority must be retained or explicitly resolved"]
pub(crate) enum NativeAbortEffectSettlement {
    WorkerSettled,
    TransportUnknown(NativeLateAbortEffect),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeAbortEffectAckError {
    #[cfg(test)]
    OperationIdentityMismatch,
    #[cfg(test)]
    OperationKindMismatch,
    MissingWorkerReceipt,
    NonClosingWorkerOutcome(OperationOutcome),
    MissingParticipationReceipt,
    ParticipationReceiptIdentityMismatch,
    NonClosingParticipationReceipt,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) enum NativeLateAbortEffectSettleError {
    /// The receipt does not resolve this exact late issue. The authority is
    /// returned so callers cannot accidentally discard it as a normal miss.
    Rejected {
        reason: NativeAbortEffectAckError,
        settlement: NativeLateAbortEffect,
    },
    Application(ParticipationStandDownError),
}

#[cfg(test)]
impl NativeLateAbortEffectSettleError {
    fn rejected(reason: NativeAbortEffectAckError, settlement: NativeLateAbortEffect) -> Self {
        Self::Rejected { reason, settlement }
    }

    pub(crate) fn fail_closed(self) -> Result<(), ParticipationStandDownError> {
        match self {
            Self::Rejected { settlement, .. } => settlement.fail_closed(),
            Self::Application(error) => Err(error),
        }
    }
}

#[cfg(test)]
impl fmt::Display for NativeLateAbortEffectSettleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { reason, settlement } => write!(
                formatter,
                "receipt rejected as {reason:?} for {:?}",
                settlement.identity()
            ),
            Self::Application(error) => write!(formatter, "application settlement failed: {error}"),
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) enum NativeAbortEffectSettleError {
    /// The acknowledgement cannot settle this effect. The exact effect is
    /// returned so its owner must fail closed or retain it; it is never
    /// silently reclassified as an unknown transport result.
    Rejected {
        reason: NativeAbortEffectAckError,
        effect: Box<NativeAbortEffect>,
    },
    Application(ParticipationStandDownError),
}

#[cfg(test)]
impl NativeAbortEffectSettleError {
    fn rejected(reason: NativeAbortEffectAckError, effect: NativeAbortEffect) -> Self {
        Self::Rejected {
            reason,
            effect: Box::new(effect),
        }
    }

    pub(crate) const fn reason(&self) -> Option<NativeAbortEffectAckError> {
        match self {
            Self::Rejected { reason, .. } => Some(*reason),
            Self::Application(_) => None,
        }
    }

    pub(crate) fn into_effect(self) -> Option<NativeAbortEffect> {
        match self {
            Self::Rejected { effect, .. } => Some(*effect),
            Self::Application(_) => None,
        }
    }

    pub(crate) fn fail_closed(self) -> Result<(), ParticipationStandDownError> {
        match self {
            Self::Rejected { effect, .. } => (*effect).fail_closed(),
            Self::Application(error) => Err(error),
        }
    }
}

#[cfg(test)]
impl fmt::Display for NativeAbortEffectSettleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { reason, effect } => write!(
                formatter,
                "receipt rejected as {reason:?} for {:?}",
                effect.intent().operation_id()
            ),
            Self::Application(error) => write!(formatter, "application settlement failed: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use novarocks_attempt_protocol::config::DispatchLane;
    use novarocks_coordination_contract::ExecutionEffect;
    use novarocks_execution::task_execution::{
        AcquireParticipationAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
        LeaseValidFor, ParticipationAdmissionTicketReceipt, ParticipationReceipt, ParticipationRef,
        ParticipationState, TaskOperationId,
    };
    use novarocks_query_application::coordination::{
        AbortParticipationEffectPort, AdmissionIssueSettlement, LogicalConclusion,
        LogicalExecutionActorConfig, ParticipationClosureState,
    };
    use novarocks_query_application::test_support::LogicalExecutionTestHarness;
    use novarocks_types::NativeCompatibilityId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };
    use novarocks_workload_control::{
        ResourceConfig, Stage, StagePermit, WorkClass, WorkOwner, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };

    use super::*;

    fn execution() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(89, 1), AttemptId::new(1).unwrap()).unwrap()
    }

    fn participation(backend: BackendProcessId) -> ParticipationRef {
        ParticipationRef::new(execution(), FrontendProcessId::new_v7(), backend)
    }

    fn governed_execution() -> (WorkOwner, StagePermit) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 30,
                control_bytes: 1 << 20,
                per_scope_bytes: 1 << 28,
            },
        )
        .expect("valid test workload control");
        control.mark_ready().expect("test workload is ready");
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("query root is admitted");
        let stage = root
            .owner
            .scope()
            .try_acquire(Stage::Execution)
            .expect("execution stage is admitted");
        (root.owner, stage)
    }

    async fn next_effect(intake: &mut NativeAbortEffectIntake) -> NativeAbortEffect {
        tokio::time::timeout(Duration::from_secs(1), intake.recv())
            .await
            .expect("the actor must publish its Abort without blocking")
            .expect("the adapter intake remains connected")
    }

    #[test]
    fn cancelled_reservation_releases_capacity_before_publishing_its_epoch() {
        let (sender, _receiver) = mpsc::channel(1);
        let mut permit = Some(sender.clone().try_reserve_owned().unwrap());
        assert_eq!(sender.capacity(), 0);
        let (capacity_epoch, receiver) = watch::channel(0);

        release_permit_then_publish_capacity(&mut permit, &capacity_epoch);

        assert_eq!(sender.capacity(), 1);
        assert!(receiver.has_changed().unwrap());
    }

    #[test]
    fn final_adapter_owner_closes_the_sender_before_waking_the_runner() {
        let wake = Arc::new(super::super::status_intake::CountingWake::default());
        let (adapter, intake) = NativeAbortEffectAdapter::bounded(
            NonZeroUsize::MIN,
            Arc::clone(&wake) as Arc<dyn StatusIntakeWake>,
        );

        drop(adapter);

        assert_eq!(wake.count(), 1);
        assert!(matches!(
            intake.front_intent(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[tokio::test]
    async fn attempt_abort_rearms_only_after_its_original_control_input_retires() {
        exercise_registered_abort_route(RegisteredAbortCase::Replay).await;
    }

    #[tokio::test]
    async fn failed_abort_route_fences_settlement_and_returns_the_original_control_input() {
        exercise_registered_abort_route(RegisteredAbortCase::OwnerFailed).await;
    }

    #[tokio::test]
    async fn registered_abort_rejects_invalid_participation_receipts_without_losing_authority() {
        exercise_registered_abort_route(RegisteredAbortCase::InvalidReceipts).await;
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum RegisteredAbortCase {
        Replay,
        OwnerFailed,
        InvalidReceipts,
    }

    async fn exercise_registered_abort_route(case: RegisteredAbortCase) {
        use crate::{
            control_runtime::actor::FrontendActor,
            query_execution::native_execution_adapter::LogicalAbortRouter,
            task_execution::{
                bridge::abort::{
                    AbortReceiptSlot, AbortSettlementOwner, AbortSettlementRefusal,
                    AttemptAbortIngress,
                },
                host::{custody::tests::WithdrawalInbox, transport::SendKey},
            },
        };
        use novarocks_attempt_protocol::input::{AttemptInput, SendGeneration};
        use novarocks_control_runtime::{actor_turn::InputPhase, transaction::RuntimeLedger};
        use novarocks_control_runtime_core::{
            capacity::{ReservationResources, ResourceVector},
            identity::Generation,
            mailbox::InputClass,
            scope::{WorkAdmission, WorkKind},
        };
        use novarocks_coordination_contract::{
            AbortIssueSettlement, AttemptActionId, AttemptToLogical, AttemptToLogicalBody,
            LogicalToAttemptBody,
        };
        use novarocks_query_application::coordination::{
            AbortParticipationIssueState, LogicalExecutionJoinReadiness,
        };
        use std::num::NonZeroU64;

        let exact = participation(BackendProcessId::new_v7());
        let router = LogicalAbortRouter::new();
        let route = router.reserve_route(execution()).unwrap();
        assert!(router.reserve_route(execution()).is_err());
        let (work_owner, execution_stage) = governed_execution();
        let limit = NonZeroUsize::new(2).unwrap();
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution(),
            ExecutionEffect::None,
            limit,
            vec![exact],
            limit,
            limit,
            work_owner,
            execution_stage,
        )
        .unwrap()
        .with_abort_participation_effect_port(router.clone(), limit);
        let mut logical = LogicalExecutionTestHarness::install(
            tokio::runtime::Handle::current(),
            config,
            execution(),
            vec![exact],
        )
        .unwrap();
        let activation = logical.activate_initial().await.unwrap();

        let generation = Generation::new(1).unwrap();
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let inbox = WithdrawalInbox::new(&scope);
        let (mut work, mut payer) = scope
            .reserve_next(
                generation,
                ReservationResources::work_only(),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        work.start().unwrap();
        let actor = inbox.registration().actor();
        let runtime: &novarocks_control_runtime::actor_turn::ActorRuntime<FrontendActor> =
            inbox.runtime();
        let position = runtime
            .reserve_admitted_input_for(inbox.registration(), actor, InputClass::Control, &work)
            .unwrap()
            .into_standing_input()
            .unwrap();
        let standing_id = position.id();
        let port = Arc::new(AttemptAbortIngress::new(
            inbox.registration().clone(),
            activation,
            [(exact, position)],
            router.capacity_author(),
            None,
        ));
        route.publish(port.clone());
        let settlement_owner = AbortSettlementOwner::new(port.clone());
        let admission = AcquireParticipationAdmissionTicket::new(
            TaskOperationId::new_v7(),
            exact,
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
            NativeCompatibilityId::new([7; 32]),
            AdmissionEpochCapability::try_from_bytes([7; 16]).unwrap(),
        );
        let (admission_activation, pending) = logical
            .begin_admission_issue_and_abandon(admission)
            .await
            .unwrap();
        logical
            .settle_late_admission_issue(
                admission_activation,
                pending,
                AdmissionIssueSettlement::applied(
                    pending.operation_id(),
                    OperationOutcome::Accepted,
                    ParticipationAdmissionTicketReceipt::new(
                        AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
                        exact,
                        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
                    ),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let first = AttemptActionId::new(NonZeroU64::MIN);
        let second = AttemptActionId::new(NonZeroU64::new(2).unwrap());
        let identity = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(snapshot) = logical.stand_down_snapshot(exact).await.unwrap()
                    && snapshot.issue_state() == Some(AbortParticipationIssueState::TransportOwned)
                {
                    break snapshot.identity().unwrap();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let operation = identity.operation_id();
        assert!(port.take_submission(second, exact, operation).is_none());
        let physical = |generation| AttemptActionId::new(NonZeroU64::new(generation).unwrap());
        let settle = |abort_action, generation, receipt_generation, settlement| AttemptToLogical {
            activation,
            action: physical(100),
            body: AttemptToLogicalBody::AbortSettlement {
                abort_action,
                generation,
                receipt_generation,
                participation: exact,
                operation_id: operation,
                settlement,
            },
        };
        let finish = async |logical: &mut LogicalExecutionTestHarness, inbox: WithdrawalInbox| {
            logical
                .finish_until(std::time::Instant::now() + Duration::from_secs(2))
                .await
                .unwrap();
            drop(route);
            work.body_exited().unwrap();
            work.captures_exited().unwrap();
            assert!(port.close_after_exit().unwrap());
            payer.finish().unwrap();
            scope.close_external().unwrap();
            ledger.reap_scope_ready(scope.id(), 8).unwrap();
            inbox.finish();
            ledger.remove_scope(scope.id()).unwrap();
            assert!(ledger.is_empty());
        };
        if case == RegisteredAbortCase::InvalidReceipts {
            let (input, AttemptInput::Port(original)) = inbox.try_commit_owner_control().unwrap()
            else {
                panic!("original Abort Control input");
            };
            assert_eq!(original.activation, activation);
            assert_eq!(identity.activation(), activation);
            assert_eq!(identity.participation(), exact);
            let LogicalToAttemptBody::Abort { action, request } = original.body else {
                panic!("original Abort request");
            };
            assert_eq!(action, first);
            assert_eq!(request.envelope().operation_id(), operation);
            assert_eq!(request.participation(), exact);
            assert_eq!(request.cause(), AbortCause::QueryFailed);
            let custody = runtime.input_custody(actor, input).unwrap().unwrap();
            assert_eq!(custody.standing_input_id, Some(standing_id));
            assert_eq!(custody.class, InputClass::Control);
            assert_eq!(custody.phase, InputPhase::Committed);
            let original_closure = logical
                .stand_down_snapshot(exact)
                .await
                .unwrap()
                .unwrap()
                .closure();
            let send = physical(10);
            let worker = settle(
                first,
                send,
                Some(send),
                AbortIssueSettlement::Worker(OperationOutcome::Accepted),
            );
            let foreign = participation(BackendProcessId::new_v7());
            for (payload, reason) in [
                (
                    AckPayload::None,
                    NativeAbortEffectAckError::MissingParticipationReceipt,
                ),
                (
                    AckPayload::Participation(ParticipationReceipt::new(
                        foreign,
                        ParticipationState::Aborting,
                    )),
                    NativeAbortEffectAckError::ParticipationReceiptIdentityMismatch,
                ),
                (
                    AckPayload::Participation(ParticipationReceipt::new(
                        exact,
                        ParticipationState::Active,
                    )),
                    NativeAbortEffectAckError::NonClosingParticipationReceipt,
                ),
            ] {
                // Each candidate is independent. The real receipt slot is
                // single-assignment and all candidates name the original send.
                let receipt = AbortReceiptSlot::new(
                    SendKey {
                        operation,
                        generation: SendGeneration::new(send),
                    },
                    exact,
                );
                receipt.record(&OperationAcknowledgement::worker_receipt(
                    operation,
                    OperationKind::AbortParticipation,
                    OperationOutcome::Accepted,
                    payload,
                ));
                assert!(matches!(
                    settlement_owner.apply(&worker, Some(&receipt)),
                    Err(AbortSettlementRefusal::InvalidReceipt(original)) if original == reason
                ));
                assert_eq!(runtime.input_custody(actor, input).unwrap(), Some(custody));
                assert!(matches!(
                    router.try_reserve(identity),
                    AbortParticipationEffectAdmission::Backpressured
                ));
                let snapshot = logical.stand_down_snapshot(exact).await.unwrap().unwrap();
                assert_eq!(snapshot.identity(), Some(identity));
                assert_eq!(
                    snapshot.issue_state(),
                    Some(AbortParticipationIssueState::TransportOwned)
                );
                assert_eq!(snapshot.closure(), original_closure);
            }
            let failed = settlement_owner
                .apply(
                    &settle(first, send, None, AbortIssueSettlement::FailedClosed),
                    None,
                )
                .unwrap();
            failed.result.unwrap();
            let mut retirement = failed
                .retirement
                .expect("original authority survived every refusal");
            assert!(retirement.retire_ready().unwrap());
            assert!(runtime.input_custody(actor, input).unwrap().is_none());
            drop(retirement);
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if logical
                        .stand_down_snapshot(exact)
                        .await
                        .unwrap()
                        .is_some_and(|snapshot| {
                            snapshot.closure() == ParticipationClosureState::AbortIssueExhausted
                                && snapshot.issue_state()
                                    == Some(AbortParticipationIssueState::FailedClosed)
                        })
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            // Leave the original route open so a mistaken unknown-outcome
            // replay would still be able to occupy this paid Control slot.
            tokio::time::sleep(Duration::from_millis(20)).await;
            let roles = runtime.standing_input_inventory(actor, None, 16).unwrap();
            let role = roles
                .iter()
                .find(|role| role.standing_input_id == standing_id)
                .unwrap();
            assert_eq!(
                role.in_flight, None,
                "FailedClosed cannot authorize an Abort replay"
            );
            assert!(!matches!(
                logical.join_readiness().await.unwrap(),
                LogicalExecutionJoinReadiness::Ready
            ));
            drop(settlement_owner);
            logical
                .observe_worker_process_replaced(exact)
                .await
                .unwrap();
            finish(&mut logical, inbox).await;
            return;
        }

        let mut capacity = port.subscribe_capacity();
        let unknown = settlement_owner
            .apply(
                &settle(
                    first,
                    physical(10),
                    None,
                    AbortIssueSettlement::TransportUnknown,
                ),
                None,
            )
            .unwrap();
        unknown.result.unwrap();
        let mut retirement = unknown.retirement.unwrap();
        assert!(!retirement.retire_ready().unwrap());
        assert!(matches!(
            router.try_reserve(identity),
            AbortParticipationEffectAdmission::Backpressured
        ));
        assert!(!capacity.has_changed().unwrap());
        let (_, AttemptInput::Port(original_input)) = inbox.try_commit_owner_control().unwrap()
        else {
            panic!("original Abort Control input");
        };
        assert_eq!(original_input.activation, activation);
        let LogicalToAttemptBody::Abort { action, request } = original_input.body else {
            panic!("original Abort request");
        };
        assert_eq!(action, first);
        assert_eq!(request.envelope().operation_id(), operation);
        assert_eq!(request.participation(), exact);
        assert!(retirement.retire_ready().unwrap());
        capacity.changed().await.unwrap();
        drop(retirement);
        let (_, AttemptInput::Port(replay_input)) =
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if let Some(input) = inbox.try_commit_owner_control() {
                        break input;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap()
        else {
            panic!("original Abort replay Control input");
        };
        let LogicalToAttemptBody::Abort {
            action,
            request: replay_request,
        } = replay_input.body
        else {
            panic!("original Abort replay request");
        };
        assert_eq!(action, second);
        assert_eq!(replay_request.envelope(), request.envelope());
        assert_eq!(replay_request.participation(), request.participation());
        assert_eq!(replay_request.cause(), request.cause());
        if case == RegisteredAbortCase::OwnerFailed {
            let mut drained = settlement_owner.drain_failed();
            assert!(drained.errors.is_empty());
            assert_eq!(drained.retirements.len(), 1);
            assert!(matches!(
                router.try_reserve(identity),
                AbortParticipationEffectAdmission::Closed
            ));
            let stale = settlement_owner
                .apply(
                    &settle(
                        first,
                        physical(10),
                        Some(physical(10)),
                        AbortIssueSettlement::Worker(
                            OperationOutcome::ParticipationTerminalReceipt,
                        ),
                    ),
                    None,
                )
                .unwrap();
            stale.result.unwrap();
            assert!(stale.retirement.is_none());
            assert!(
                drained
                    .retirements
                    .pop_front()
                    .unwrap()
                    .retire_ready()
                    .unwrap()
            );
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if logical
                        .stand_down_snapshot(exact)
                        .await
                        .unwrap()
                        .is_some_and(|snapshot| {
                            snapshot.issue_state()
                                == Some(AbortParticipationIssueState::FailedClosed)
                        })
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        } else {
            let closing = OperationAcknowledgement::worker_receipt(
                operation,
                OperationKind::AbortParticipation,
                OperationOutcome::ParticipationTerminalReceipt,
                AckPayload::Participation(ParticipationReceipt::new(
                    exact,
                    ParticipationState::TerminalRetained,
                )),
            );
            let receipt = |generation| {
                let receipt = AbortReceiptSlot::new(
                    SendKey {
                        operation,
                        generation: SendGeneration::new(physical(generation)),
                    },
                    exact,
                );
                receipt.record(&closing);
                receipt
            };
            let worker = settle(
                first,
                physical(10),
                Some(physical(10)),
                AbortIssueSettlement::Worker(OperationOutcome::ParticipationTerminalReceipt),
            );
            assert!(matches!(
                settlement_owner.apply(&worker, Some(&receipt(20))),
                Err(AbortSettlementRefusal::WrongReceiptGeneration)
            ));
            let applied = settlement_owner.apply(&worker, Some(&receipt(10))).unwrap();
            applied.result.unwrap();
            let mut retirement = applied
                .retirement
                .expect("newer exact replay is relinquished");
            assert!(retirement.retire_ready().unwrap());
            drop(retirement);
            let redundant = settlement_owner
                .apply(
                    &settle(
                        second,
                        physical(20),
                        None,
                        AbortIssueSettlement::DefinitelyUnsent,
                    ),
                    None,
                )
                .unwrap();
            redundant.result.unwrap();
            assert!(redundant.retirement.is_none());
        }
        drop(settlement_owner);
        logical
            .observe_worker_stopped_and_participation_fenced(exact)
            .await
            .unwrap();
        finish(&mut logical, inbox).await;
    }

    #[tokio::test]
    async fn projects_actor_identity_and_replays_the_exact_native_abort() {
        let exact_participation = participation(BackendProcessId::new_v7());
        let (adapter, mut intake) = NativeAbortEffectAdapter::bounded(
            NonZeroUsize::MIN,
            Arc::new(super::super::status_intake::CountingWake::default()),
        );
        let (work_owner, execution_stage) = governed_execution();
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution(),
            ExecutionEffect::None,
            NonZeroUsize::new(2).unwrap(),
            vec![exact_participation],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work_owner,
            execution_stage,
        )
        .expect("valid single-attempt actor configuration")
        .with_abort_participation_effect_port(
            Arc::clone(&adapter) as Arc<dyn AbortParticipationEffectPort>,
            NonZeroUsize::new(2).unwrap(),
        );
        let mut logical = LogicalExecutionTestHarness::install(
            tokio::runtime::Handle::current(),
            config,
            execution(),
            vec![exact_participation],
        )
        .unwrap();
        logical.activate_initial().await.unwrap();
        let admission = AcquireParticipationAdmissionTicket::new(
            TaskOperationId::new_v7(),
            exact_participation,
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
            NativeCompatibilityId::new([7; 32]),
            AdmissionEpochCapability::try_from_bytes([7; 16]).unwrap(),
        );
        let (activation, pending) = logical
            .begin_admission_issue_and_abandon(admission)
            .await
            .unwrap();
        logical
            .settle_late_admission_issue(
                activation,
                pending,
                AdmissionIssueSettlement::applied(
                    pending.operation_id(),
                    OperationOutcome::Accepted,
                    ParticipationAdmissionTicketReceipt::new(
                        AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
                        exact_participation,
                        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
                    ),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let first = next_effect(&mut intake).await;
        let OperationIntent::AbortParticipation(first_request) = first.intent() else {
            panic!("the adapter must emit an AbortParticipation intent");
        };
        let first_request = *first_request;
        assert_eq!(first.intent().lane(), DispatchLane::Lifecycle);
        assert!(first.intent().requires_control_progress());
        assert_eq!(first_request.participation(), exact_participation);
        assert_eq!(first_request.cause(), AbortCause::QueryFailed);
        let operation_id = first_request.envelope().operation_id();
        let late = match first
            .settle(&OperationAcknowledgement::transport_unknown(
                operation_id,
                OperationKind::AbortParticipation,
            ))
            .unwrap()
        {
            NativeAbortEffectSettlement::TransportUnknown(late) => late,
            NativeAbortEffectSettlement::WorkerSettled => {
                panic!("a transport-unknown acknowledgement retains late authority")
            }
        };

        let second = next_effect(&mut intake).await;
        let OperationIntent::AbortParticipation(second_request) = second.intent() else {
            panic!("the exact replay remains an AbortParticipation intent");
        };
        let second_request = *second_request;
        assert_eq!(second_request.envelope().operation_id(), operation_id);
        assert_eq!(
            second_request.participation(),
            first_request.participation()
        );
        assert_eq!(second_request.cause(), first_request.cause());
        assert!(matches!(
            second
                .settle(&OperationAcknowledgement::worker_receipt(
                    operation_id,
                    OperationKind::AbortParticipation,
                    OperationOutcome::ParticipationTerminalReceipt,
                    AckPayload::Participation(ParticipationReceipt::new(
                        exact_participation,
                        ParticipationState::TerminalRetained,
                    )),
                ))
                .unwrap(),
            NativeAbortEffectSettlement::WorkerSettled
        ));
        late.resolved_by_other_generation();

        loop {
            if logical
                .stand_down_snapshot(exact_participation)
                .await
                .unwrap()
                .is_some_and(|snapshot| {
                    snapshot.closure() == ParticipationClosureState::TerminalRetained
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            logical.actor_snapshot().await.unwrap().conclusion,
            Some(LogicalConclusion::Cancelled)
        );
        assert!(
            !matches!(
                logical.join_readiness().await.unwrap(),
                novarocks_query_application::coordination::LogicalExecutionJoinReadiness::Ready
            ),
            "a terminal Abort receipt does not prove actual Worker stop or process replacement"
        );
        logical
            .observe_worker_stopped_and_participation_fenced(exact_participation)
            .await
            .unwrap();
        logical
            .finish_until(std::time::Instant::now() + Duration::from_secs(1))
            .await
            .expect("exact Worker stop and participation fencing lets the owner finish");
    }

    #[tokio::test]
    async fn invalid_participation_receipts_are_rejected_without_losing_the_effect() {
        let exact_participation = participation(BackendProcessId::new_v7());
        let (adapter, mut intake) = NativeAbortEffectAdapter::bounded(
            NonZeroUsize::MIN,
            Arc::new(super::super::status_intake::CountingWake::default()),
        );
        let (work_owner, execution_stage) = governed_execution();
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution(),
            ExecutionEffect::None,
            NonZeroUsize::new(2).unwrap(),
            vec![exact_participation],
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            work_owner,
            execution_stage,
        )
        .expect("valid single-attempt actor configuration")
        .with_abort_participation_effect_port(
            Arc::clone(&adapter) as Arc<dyn AbortParticipationEffectPort>,
            NonZeroUsize::MIN,
        );
        let mut logical = LogicalExecutionTestHarness::install(
            tokio::runtime::Handle::current(),
            config,
            execution(),
            vec![exact_participation],
        )
        .unwrap();
        logical.activate_initial().await.unwrap();
        let admission = AcquireParticipationAdmissionTicket::new(
            TaskOperationId::new_v7(),
            exact_participation,
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
            NativeCompatibilityId::new([7; 32]),
            AdmissionEpochCapability::try_from_bytes([7; 16]).unwrap(),
        );
        let (activation, pending) = logical
            .begin_admission_issue_and_abandon(admission)
            .await
            .unwrap();
        logical
            .settle_late_admission_issue(
                activation,
                pending,
                AdmissionIssueSettlement::applied(
                    pending.operation_id(),
                    OperationOutcome::Accepted,
                    ParticipationAdmissionTicketReceipt::new(
                        AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
                        exact_participation,
                        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
                    ),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let effect = next_effect(&mut intake).await;
        let operation_id = effect.intent().operation_id();
        let error = effect
            .settle(&OperationAcknowledgement::worker_receipt(
                operation_id,
                OperationKind::AbortParticipation,
                OperationOutcome::Accepted,
                AckPayload::None,
            ))
            .unwrap_err();
        assert_eq!(
            error.reason(),
            Some(NativeAbortEffectAckError::MissingParticipationReceipt)
        );
        let effect = error.into_effect().expect("the exact effect is retained");
        assert_eq!(effect.intent().operation_id(), operation_id);

        let foreign_participation = participation(BackendProcessId::new_v7());
        let error = effect
            .settle(&OperationAcknowledgement::worker_receipt(
                operation_id,
                OperationKind::AbortParticipation,
                OperationOutcome::Accepted,
                AckPayload::Participation(ParticipationReceipt::new(
                    foreign_participation,
                    ParticipationState::Aborting,
                )),
            ))
            .unwrap_err();
        assert_eq!(
            error.reason(),
            Some(NativeAbortEffectAckError::ParticipationReceiptIdentityMismatch)
        );
        let effect = error.into_effect().expect("the exact effect is retained");

        let error = effect
            .settle(&OperationAcknowledgement::worker_receipt(
                operation_id,
                OperationKind::AbortParticipation,
                OperationOutcome::Accepted,
                AckPayload::Participation(ParticipationReceipt::new(
                    exact_participation,
                    ParticipationState::Active,
                )),
            ))
            .unwrap_err();
        assert_eq!(
            error.reason(),
            Some(NativeAbortEffectAckError::NonClosingParticipationReceipt)
        );
        let effect = error.into_effect().expect("the exact effect is retained");

        effect
            .fail_closed()
            .expect("a rejected protocol receipt explicitly fails the effect closed");
        loop {
            if logical
                .stand_down_snapshot(exact_participation)
                .await
                .unwrap()
                .is_some_and(|snapshot| {
                    snapshot.closure() == ParticipationClosureState::AbortIssueExhausted
                        && snapshot.issue_state()
                            == Some(
                                novarocks_query_application::coordination::AbortParticipationIssueState::FailedClosed,
                            )
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(20), intake.recv())
                .await
                .is_err(),
            "a typed protocol failure must not become an unknown-outcome replay"
        );
        assert!(
            !matches!(
                logical.join_readiness().await.unwrap(),
                novarocks_query_application::coordination::LogicalExecutionJoinReadiness::Ready
            ),
            "a failed-closed Abort retains residual Worker responsibility"
        );
        logical
            .observe_worker_process_replaced(exact_participation)
            .await
            .unwrap();
        logical
            .finish_until(std::time::Instant::now() + Duration::from_secs(1))
            .await
            .expect("the retained effect plus exact process replacement can settle");
    }

    #[test]
    fn native_abort_projection_uses_the_actor_identity_without_reminting() {
        let backend = BackendProcessId::new_v7();
        let participation = participation(backend);
        let operation_id = TaskOperationId::new_v7();
        let request = AbortParticipation::new(operation_id, participation, AbortCause::QueryFailed);
        let intent = OperationIntent::AbortParticipation(request);
        assert_eq!(intent.operation_id(), operation_id);
        assert_eq!(intent.backend_process_id(), backend);
        assert_eq!(intent.lane(), DispatchLane::Lifecycle);
    }
}
