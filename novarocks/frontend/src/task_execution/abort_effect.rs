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
//! The query application freezes the exact operation identity. This adapter
//! adds the Native carrier and owns a bounded handoff slot before it accepts
//! the application submission. This slot is only the application-to-runner
//! handoff; it is not a process transport or control-progress reservation.
//! Production composition must drain the intake and enqueue
//! [`OperationIntent::AbortQueryContext`] through the runner's priority
//! lifecycle lane, where the process authority reserves those capacities.
//! This module neither owns a wire nor creates a second context-lifecycle
//! state machine.

use std::collections::VecDeque;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use novarocks_execution::task_execution::{
    AbortCause, AbortQueryContext, OperationKind, OperationOutcome, QueryContextReceipt,
    QueryContextState,
};
use novarocks_query_application::coordination::{
    AbortQueryContextEffectAdmission, AbortQueryContextEffectPort,
    AbortQueryContextEffectReservation, AbortQueryContextEffectSubmission,
    AbortQueryContextIssueIdentity, ContextStandDownCause, ContextStandDownError,
    LateAbortQueryContextWorkerSettlement, OperationDispatchResult,
};
use tokio::sync::{mpsc, watch};

use super::intent::{AckPayload, OperationAcknowledgement, OperationIntent};
use super::status_intake::StatusIntakeWake;

/// Bounded application-to-Native Abort projection.
pub(crate) struct NativeAbortEffectAdapter {
    effects: Option<mpsc::Sender<NativeAbortEffect>>,
    submission_order: Arc<Mutex<VecDeque<OperationIntent>>>,
    capacity_epoch: watch::Sender<u64>,
    wake: Arc<dyn StatusIntakeWake>,
    worker_closed: Option<Arc<dyn Fn(novarocks_execution_contract::QueryContextRef) + Send + Sync>>,
}

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
            Arc<dyn Fn(novarocks_execution_contract::QueryContextRef) + Send + Sync>,
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

impl AbortQueryContextEffectPort for NativeAbortEffectAdapter {
    fn subscribe_capacity(&self) -> watch::Receiver<u64> {
        self.capacity_epoch.subscribe()
    }

    fn try_reserve(
        &self,
        identity: AbortQueryContextIssueIdentity,
    ) -> AbortQueryContextEffectAdmission {
        let Some(effects) = self.effects.as_ref() else {
            return AbortQueryContextEffectAdmission::Closed;
        };
        match effects.clone().try_reserve_owned() {
            Ok(permit) => {
                AbortQueryContextEffectAdmission::Admitted(Box::new(NativeAbortEffectReservation {
                    identity,
                    permit: Some(permit),
                    submission_order: Arc::clone(&self.submission_order),
                    capacity_epoch: self.capacity_epoch.clone(),
                    wake: Arc::clone(&self.wake),
                    worker_closed: self.worker_closed.clone(),
                }))
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                AbortQueryContextEffectAdmission::Backpressured
            }
            Err(mpsc::error::TrySendError::Closed(_)) => AbortQueryContextEffectAdmission::Closed,
        }
    }
}

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
#[derive(Debug)]
pub(crate) struct NativeAbortEffectIntake {
    receiver: mpsc::Receiver<NativeAbortEffect>,
    submission_order: Arc<Mutex<VecDeque<OperationIntent>>>,
    capacity_epoch: watch::Sender<u64>,
}

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

    #[cfg(test)]
    pub(crate) fn queued(&self) -> usize {
        self.submission_order
            .lock()
            .expect("Native Abort effect submission order")
            .len()
    }

    #[cfg(test)]
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

struct NativeAbortEffectReservation {
    identity: AbortQueryContextIssueIdentity,
    permit: Option<mpsc::OwnedPermit<NativeAbortEffect>>,
    submission_order: Arc<Mutex<VecDeque<OperationIntent>>>,
    capacity_epoch: watch::Sender<u64>,
    wake: Arc<dyn StatusIntakeWake>,
    worker_closed: Option<Arc<dyn Fn(novarocks_execution_contract::QueryContextRef) + Send + Sync>>,
}

impl fmt::Debug for NativeAbortEffectReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAbortEffectReservation")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl AbortQueryContextEffectReservation for NativeAbortEffectReservation {
    fn submit(mut self: Box<Self>, submission: AbortQueryContextEffectSubmission) {
        assert_eq!(
            submission.identity(),
            self.identity,
            "an Abort effect reservation may accept only its exact actor-owned identity"
        );
        let identity = submission.request().identity();
        let request = AbortQueryContext::new(
            identity.operation_id(),
            identity.context(),
            native_cause(identity.cause()),
        );
        let effect = NativeAbortEffect {
            intent: OperationIntent::AbortQueryContext(request),
            submission: Some(submission),
            worker_closed: self.worker_closed.clone(),
        };
        let mut order = self
            .submission_order
            .lock()
            .expect("Native Abort effect submission order");
        self.permit
            .take()
            .expect("a live Abort reservation retains its bounded slot")
            .send(effect);
        order.push_back(OperationIntent::AbortQueryContext(request));
        drop(order);
        self.wake.wake();
    }
}

impl Drop for NativeAbortEffectReservation {
    fn drop(&mut self) {
        if self.permit.is_some() {
            release_permit_then_publish_capacity(&mut self.permit, &self.capacity_epoch);
            self.wake.wake();
        }
    }
}

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

fn native_cause(cause: ContextStandDownCause) -> AbortCause {
    match cause {
        ContextStandDownCause::LogicalExecutionCancelled
        | ContextStandDownCause::LogicalExecutionFailed => AbortCause::QueryFailed,
    }
}

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
    submission: Option<AbortQueryContextEffectSubmission>,
    worker_closed: Option<Arc<dyn Fn(novarocks_execution_contract::QueryContextRef) + Send + Sync>>,
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
    pub(crate) fn intent(&self) -> &OperationIntent {
        &self.intent
    }

    /// Validates the acknowledgement without consuming actor authority.
    /// TaskRound uses this before it releases dispatcher ownership so a bad
    /// receipt cannot partially settle either side of the transaction.
    pub(crate) fn validate_acknowledgement(
        &self,
        acknowledgement: &OperationAcknowledgement,
    ) -> Result<(), NativeAbortEffectAckError> {
        if acknowledgement.operation_id() != self.intent.operation_id() {
            return Err(NativeAbortEffectAckError::OperationIdentityMismatch);
        }
        if acknowledgement.kind() != OperationKind::AbortQueryContext {
            return Err(NativeAbortEffectAckError::OperationKindMismatch);
        }
        if acknowledgement.dispatch_result() == OperationDispatchResult::TransportUnknown {
            return Ok(());
        }
        let expected_context = match &self.intent {
            OperationIntent::AbortQueryContext(request) => request.context(),
            _ => unreachable!("a Native Abort effect always retains an Abort intent"),
        };
        closing_receipt(expected_context, acknowledgement).map(drop)
    }

    /// Stops replay after a local protocol or dispatcher violation while the
    /// actor's residual supervisor retains the exact context responsibility.
    pub(crate) fn fail_closed(mut self) -> Result<(), ContextStandDownError> {
        self.submission
            .take()
            .expect("a live Native Abort retains actor settlement authority")
            .fail_closed()
    }

    /// Returns an Abort that expired while still dispatcher-queued to the
    /// actor as definitely unsent. The actor may spend its next bounded
    /// authorization generation on the same immutable operation.
    pub(crate) fn definitely_unsent(mut self) -> Result<(), ContextStandDownError> {
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

    pub(crate) fn settle(
        mut self,
        acknowledgement: &OperationAcknowledgement,
    ) -> Result<NativeAbortEffectSettlement, NativeAbortEffectSettleError> {
        if let Err(reason) = self.validate_acknowledgement(acknowledgement) {
            return Err(NativeAbortEffectSettleError::rejected(reason, self));
        }

        match acknowledgement.dispatch_result() {
            OperationDispatchResult::TransportUnknown => {
                let late = self
                    .submission
                    .take()
                    .expect("a live Native Abort retains actor settlement authority")
                    .transport_unknown()
                    .map_err(NativeAbortEffectSettleError::Application)?;
                Ok(NativeAbortEffectSettlement::TransportUnknown(
                    NativeLateAbortEffect::new(late, self.worker_closed.clone()),
                ))
            }
            OperationDispatchResult::WorkerReceipt(_) => {
                let expected_context = match &self.intent {
                    OperationIntent::AbortQueryContext(request) => request.context(),
                    _ => unreachable!("a Native Abort effect always retains an Abort intent"),
                };
                let receipt = closing_receipt(expected_context, acknowledgement)
                    .expect("Native Abort acknowledgement was validated before settlement");
                self.submission
                    .take()
                    .expect("a live Native Abort retains actor settlement authority")
                    .worker_settled(receipt)
                    .map_err(NativeAbortEffectSettleError::Application)?;
                if let Some(observer) = &self.worker_closed {
                    observer(expected_context);
                }
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
/// context, so only its matching closing receipt may resolve it.
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
    if acknowledgement.kind() != OperationKind::AbortQueryContext {
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
    let receipt = match closing_receipt(identity.context(), acknowledgement) {
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
    settlement: Option<LateAbortQueryContextWorkerSettlement>,
    worker_closed: Option<Arc<dyn Fn(novarocks_execution_contract::QueryContextRef) + Send + Sync>>,
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
        settlement: LateAbortQueryContextWorkerSettlement,
        worker_closed: Option<
            Arc<dyn Fn(novarocks_execution_contract::QueryContextRef) + Send + Sync>,
        >,
    ) -> Self {
        Self {
            settlement: Some(settlement),
            worker_closed,
        }
    }

    pub(crate) fn identity(&self) -> AbortQueryContextIssueIdentity {
        self.settlement
            .as_ref()
            .expect("a live late Abort owner retains its settlement")
            .identity()
    }

    fn worker_settled(mut self, receipt: QueryContextReceipt) -> Result<(), ContextStandDownError> {
        let context = self.identity().context();
        self.settlement
            .take()
            .expect("a live late Abort owner retains its settlement")
            .worker_settled(receipt)?;
        if let Some(observer) = &self.worker_closed {
            observer(context);
        }
        Ok(())
    }

    pub(crate) fn fail_closed(mut self) -> Result<(), ContextStandDownError> {
        self.settlement
            .take()
            .expect("a live late Abort owner retains its settlement")
            .fail_closed()
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
    expected_context: novarocks_execution::task_execution::QueryContextRef,
    acknowledgement: &OperationAcknowledgement,
) -> Result<QueryContextReceipt, NativeAbortEffectAckError> {
    let Some(outcome) = acknowledgement.worker_outcome() else {
        return Err(NativeAbortEffectAckError::MissingWorkerReceipt);
    };
    if !legal_abort_closing_outcome(outcome) {
        return Err(NativeAbortEffectAckError::NonClosingWorkerOutcome(outcome));
    }
    let AckPayload::Context(receipt) = acknowledgement.payload() else {
        return Err(NativeAbortEffectAckError::MissingContextReceipt);
    };
    if receipt.context() != expected_context {
        return Err(NativeAbortEffectAckError::ContextReceiptIdentityMismatch);
    }
    if !matches!(
        receipt.state(),
        QueryContextState::Aborting
            | QueryContextState::Releasing
            | QueryContextState::TerminalRetained
            | QueryContextState::Gone
    ) {
        return Err(NativeAbortEffectAckError::NonClosingContextReceipt);
    }
    Ok(receipt.clone())
}

fn legal_abort_closing_outcome(outcome: OperationOutcome) -> bool {
    matches!(
        outcome,
        OperationOutcome::Accepted
            | OperationOutcome::Idempotent
            | OperationOutcome::ContextTerminalReceipt
            | OperationOutcome::LeaseExpired
            | OperationOutcome::Gone
    )
}

#[derive(Debug)]
#[must_use = "the late settlement authority must be retained or explicitly resolved"]
pub(crate) enum NativeAbortEffectSettlement {
    WorkerSettled,
    TransportUnknown(NativeLateAbortEffect),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeAbortEffectAckError {
    OperationIdentityMismatch,
    OperationKindMismatch,
    MissingWorkerReceipt,
    NonClosingWorkerOutcome(OperationOutcome),
    MissingContextReceipt,
    ContextReceiptIdentityMismatch,
    NonClosingContextReceipt,
}

#[derive(Debug)]
pub(crate) enum NativeLateAbortEffectSettleError {
    /// The receipt does not resolve this exact late issue. The authority is
    /// returned so callers cannot accidentally discard it as a normal miss.
    Rejected {
        reason: NativeAbortEffectAckError,
        settlement: NativeLateAbortEffect,
    },
    Application(ContextStandDownError),
}

impl NativeLateAbortEffectSettleError {
    fn rejected(reason: NativeAbortEffectAckError, settlement: NativeLateAbortEffect) -> Self {
        Self::Rejected { reason, settlement }
    }

    pub(crate) fn fail_closed(self) -> Result<(), ContextStandDownError> {
        match self {
            Self::Rejected { settlement, .. } => settlement.fail_closed(),
            Self::Application(error) => Err(error),
        }
    }
}

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

#[derive(Debug)]
pub(crate) enum NativeAbortEffectSettleError {
    /// The acknowledgement cannot settle this effect. The exact effect is
    /// returned so its owner must fail closed or retain it; it is never
    /// silently reclassified as an unknown transport result.
    Rejected {
        reason: NativeAbortEffectAckError,
        effect: NativeAbortEffect,
    },
    Application(ContextStandDownError),
}

impl NativeAbortEffectSettleError {
    fn rejected(reason: NativeAbortEffectAckError, effect: NativeAbortEffect) -> Self {
        Self::Rejected { reason, effect }
    }

    pub(crate) const fn reason(&self) -> Option<NativeAbortEffectAckError> {
        match self {
            Self::Rejected { reason, .. } => Some(*reason),
            Self::Application(_) => None,
        }
    }

    pub(crate) fn into_effect(self) -> Option<NativeAbortEffect> {
        match self {
            Self::Rejected { effect, .. } => Some(effect),
            Self::Application(_) => None,
        }
    }

    pub(crate) fn fail_closed(self) -> Result<(), ContextStandDownError> {
        match self {
            Self::Rejected { effect, .. } => effect.fail_closed(),
            Self::Application(error) => Err(error),
        }
    }
}

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

    use novarocks_execution::task_execution::{
        AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
        LeaseValidFor, QueryContextAdmissionTicketReceipt, QueryContextReceipt, QueryContextRef,
        QueryContextState, TaskOperationId,
    };
    use novarocks_query_application::coordination::{
        AbortQueryContextEffectPort, AdmissionIssueSettlement, ContextClosureState, DispatchLane,
        ExecutionEffect, LogicalConclusion, LogicalExecutionActorConfig,
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

    fn context(backend: BackendProcessId) -> QueryContextRef {
        QueryContextRef::new(execution(), FrontendProcessId::new_v7(), backend)
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
    async fn projects_actor_identity_and_replays_the_exact_native_abort() {
        let exact_context = context(BackendProcessId::new_v7());
        let (adapter, mut intake) = NativeAbortEffectAdapter::bounded(
            NonZeroUsize::MIN,
            Arc::new(super::super::status_intake::CountingWake::default()),
        );
        let (work_owner, execution_stage) = governed_execution();
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution(),
            ExecutionEffect::None,
            NonZeroUsize::new(2).unwrap(),
            vec![exact_context],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work_owner,
            execution_stage,
        )
        .expect("valid single-attempt actor configuration")
        .with_abort_query_context_effect_port(
            Arc::clone(&adapter) as Arc<dyn AbortQueryContextEffectPort>,
            NonZeroUsize::new(2).unwrap(),
        );
        let mut logical = LogicalExecutionTestHarness::install(
            tokio::runtime::Handle::current(),
            config,
            execution(),
            vec![exact_context],
        )
        .unwrap();
        logical.activate_initial().await.unwrap();
        let admission = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            exact_context,
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
                    QueryContextAdmissionTicketReceipt::new(
                        AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
                        exact_context,
                        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
                    ),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let first = next_effect(&mut intake).await;
        let OperationIntent::AbortQueryContext(first_request) = first.intent() else {
            panic!("the adapter must emit an AbortQueryContext intent");
        };
        let first_request = *first_request;
        assert_eq!(first.intent().lane(), DispatchLane::Lifecycle);
        assert!(first.intent().requires_control_progress());
        assert_eq!(first_request.context(), exact_context);
        assert_eq!(first_request.cause(), AbortCause::QueryFailed);
        let operation_id = first_request.envelope().operation_id();
        let late = match first
            .settle(&OperationAcknowledgement::transport_unknown(
                operation_id,
                OperationKind::AbortQueryContext,
            ))
            .unwrap()
        {
            NativeAbortEffectSettlement::TransportUnknown(late) => late,
            NativeAbortEffectSettlement::WorkerSettled => {
                panic!("a transport-unknown acknowledgement retains late authority")
            }
        };

        let second = next_effect(&mut intake).await;
        let OperationIntent::AbortQueryContext(second_request) = second.intent() else {
            panic!("the exact replay remains an AbortQueryContext intent");
        };
        let second_request = *second_request;
        assert_eq!(second_request.envelope().operation_id(), operation_id);
        assert_eq!(second_request.context(), first_request.context());
        assert_eq!(second_request.cause(), first_request.cause());
        assert!(matches!(
            second
                .settle(&OperationAcknowledgement::worker_receipt(
                    operation_id,
                    OperationKind::AbortQueryContext,
                    OperationOutcome::ContextTerminalReceipt,
                    AckPayload::Context(QueryContextReceipt::new(
                        exact_context,
                        QueryContextState::TerminalRetained,
                    )),
                ))
                .unwrap(),
            NativeAbortEffectSettlement::WorkerSettled
        ));
        late.resolved_by_other_generation();

        loop {
            if logical
                .stand_down_snapshot(exact_context)
                .await
                .unwrap()
                .is_some_and(|snapshot| snapshot.closure() == ContextClosureState::TerminalRetained)
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
            .observe_worker_stopped_and_context_fenced(exact_context)
            .await
            .unwrap();
        logical
            .finish_until(std::time::Instant::now() + Duration::from_secs(1))
            .await
            .expect("exact Worker stop and context fencing lets the owner finish");
    }

    #[tokio::test]
    async fn invalid_context_receipts_are_rejected_without_losing_the_effect() {
        let exact_context = context(BackendProcessId::new_v7());
        let (adapter, mut intake) = NativeAbortEffectAdapter::bounded(
            NonZeroUsize::MIN,
            Arc::new(super::super::status_intake::CountingWake::default()),
        );
        let (work_owner, execution_stage) = governed_execution();
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution(),
            ExecutionEffect::None,
            NonZeroUsize::new(2).unwrap(),
            vec![exact_context],
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            work_owner,
            execution_stage,
        )
        .expect("valid single-attempt actor configuration")
        .with_abort_query_context_effect_port(
            Arc::clone(&adapter) as Arc<dyn AbortQueryContextEffectPort>,
            NonZeroUsize::MIN,
        );
        let mut logical = LogicalExecutionTestHarness::install(
            tokio::runtime::Handle::current(),
            config,
            execution(),
            vec![exact_context],
        )
        .unwrap();
        logical.activate_initial().await.unwrap();
        let admission = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            exact_context,
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
                    QueryContextAdmissionTicketReceipt::new(
                        AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
                        exact_context,
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
                OperationKind::AbortQueryContext,
                OperationOutcome::Accepted,
                AckPayload::None,
            ))
            .unwrap_err();
        assert_eq!(
            error.reason(),
            Some(NativeAbortEffectAckError::MissingContextReceipt)
        );
        let effect = error.into_effect().expect("the exact effect is retained");
        assert_eq!(effect.intent().operation_id(), operation_id);

        let foreign_context = context(BackendProcessId::new_v7());
        let error = effect
            .settle(&OperationAcknowledgement::worker_receipt(
                operation_id,
                OperationKind::AbortQueryContext,
                OperationOutcome::Accepted,
                AckPayload::Context(QueryContextReceipt::new(
                    foreign_context,
                    QueryContextState::Aborting,
                )),
            ))
            .unwrap_err();
        assert_eq!(
            error.reason(),
            Some(NativeAbortEffectAckError::ContextReceiptIdentityMismatch)
        );
        let effect = error.into_effect().expect("the exact effect is retained");

        let error = effect
            .settle(&OperationAcknowledgement::worker_receipt(
                operation_id,
                OperationKind::AbortQueryContext,
                OperationOutcome::Accepted,
                AckPayload::Context(QueryContextReceipt::new(
                    exact_context,
                    QueryContextState::Active,
                )),
            ))
            .unwrap_err();
        assert_eq!(
            error.reason(),
            Some(NativeAbortEffectAckError::NonClosingContextReceipt)
        );
        let effect = error.into_effect().expect("the exact effect is retained");

        effect
            .fail_closed()
            .expect("a rejected protocol receipt explicitly fails the effect closed");
        loop {
            if logical
                .stand_down_snapshot(exact_context)
                .await
                .unwrap()
                .is_some_and(|snapshot| {
                    snapshot.closure() == ContextClosureState::AbortIssueExhausted
                        && snapshot.issue_state()
                            == Some(
                                novarocks_query_application::coordination::AbortQueryContextIssueState::FailedClosed,
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
            .observe_worker_process_replaced(exact_context)
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
        let context = context(backend);
        let operation_id = TaskOperationId::new_v7();
        let request = AbortQueryContext::new(operation_id, context, AbortCause::QueryFailed);
        let intent = OperationIntent::AbortQueryContext(request);
        assert_eq!(intent.operation_id(), operation_id);
        assert_eq!(intent.backend_process_id(), backend);
        assert_eq!(intent.lane(), DispatchLane::Lifecycle);
    }
}
