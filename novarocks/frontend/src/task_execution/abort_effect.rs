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

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use novarocks_execution::task_execution::{
    AbortCause, AbortQueryContext, OperationKind, OperationOutcome, QueryContextState,
};
use novarocks_query_application::coordination::{
    AbortQueryContextEffectAdmission, AbortQueryContextEffectPort,
    AbortQueryContextEffectReservation, AbortQueryContextEffectSubmission,
    AbortQueryContextIssueIdentity, ContextStandDownCause, ContextStandDownError,
    LateAbortQueryContextWorkerSettlement, OperationDispatchResult,
};
use tokio::sync::{mpsc, watch};

use super::intent::{AckPayload, OperationAcknowledgement, OperationIntent};

/// Bounded application-to-Native Abort projection.
pub(crate) struct NativeAbortEffectAdapter {
    effects: mpsc::Sender<NativeAbortEffect>,
    capacity_epoch: watch::Sender<u64>,
}

impl fmt::Debug for NativeAbortEffectAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAbortEffectAdapter")
            .field("remaining_capacity", &self.effects.capacity())
            .finish()
    }
}

impl NativeAbortEffectAdapter {
    pub(crate) fn bounded(capacity: NonZeroUsize) -> (Arc<Self>, NativeAbortEffectIntake) {
        let (effects, receiver) = mpsc::channel(capacity.get());
        let (capacity_epoch, _) = watch::channel(0);
        let adapter = Arc::new(Self {
            effects,
            capacity_epoch: capacity_epoch.clone(),
        });
        let intake = NativeAbortEffectIntake {
            receiver,
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
        match self.effects.clone().try_reserve_owned() {
            Ok(permit) => {
                AbortQueryContextEffectAdmission::Admitted(Box::new(NativeAbortEffectReservation {
                    identity,
                    permit: Some(permit),
                    capacity_epoch: self.capacity_epoch.clone(),
                }))
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                AbortQueryContextEffectAdmission::Backpressured
            }
            Err(mpsc::error::TrySendError::Closed(_)) => AbortQueryContextEffectAdmission::Closed,
        }
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
    capacity_epoch: watch::Sender<u64>,
}

impl NativeAbortEffectIntake {
    pub(crate) fn try_recv(&mut self) -> Result<NativeAbortEffect, mpsc::error::TryRecvError> {
        let effect = self.receiver.try_recv()?;
        publish_capacity(&self.capacity_epoch);
        Ok(effect)
    }

    #[cfg(test)]
    async fn recv(&mut self) -> Option<NativeAbortEffect> {
        let effect = self.receiver.recv().await?;
        publish_capacity(&self.capacity_epoch);
        Some(effect)
    }
}

#[derive(Debug)]
struct NativeAbortEffectReservation {
    identity: AbortQueryContextIssueIdentity,
    permit: Option<mpsc::OwnedPermit<NativeAbortEffect>>,
    capacity_epoch: watch::Sender<u64>,
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
        };
        self.permit
            .take()
            .expect("a live Abort reservation retains its bounded slot")
            .send(effect);
    }
}

impl Drop for NativeAbortEffectReservation {
    fn drop(&mut self) {
        if self.permit.is_some() {
            publish_capacity(&self.capacity_epoch);
        }
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
#[derive(Debug)]
#[must_use = "a Native Abort effect must enter lifecycle dispatch or retain its settlement"]
pub(crate) struct NativeAbortEffect {
    intent: OperationIntent,
    submission: Option<AbortQueryContextEffectSubmission>,
}

impl NativeAbortEffect {
    pub(crate) fn intent(&self) -> &OperationIntent {
        &self.intent
    }

    pub(crate) fn settle(
        mut self,
        acknowledgement: &OperationAcknowledgement,
    ) -> Result<NativeAbortEffectSettlement, NativeAbortEffectSettleError> {
        if acknowledgement.operation_id() != self.intent.operation_id() {
            return Err(NativeAbortEffectSettleError::rejected(
                NativeAbortEffectAckError::OperationIdentityMismatch,
                self,
            ));
        }
        if acknowledgement.kind() != OperationKind::AbortQueryContext {
            return Err(NativeAbortEffectSettleError::rejected(
                NativeAbortEffectAckError::OperationKindMismatch,
                self,
            ));
        }

        match acknowledgement.dispatch_result() {
            OperationDispatchResult::TransportUnknown => {
                let late = self
                    .submission
                    .take()
                    .expect("a live Native Abort retains actor settlement authority")
                    .transport_unknown()
                    .map_err(NativeAbortEffectSettleError::Application)?;
                Ok(NativeAbortEffectSettlement::TransportUnknown(late))
            }
            OperationDispatchResult::WorkerReceipt(_) => {
                let Some(outcome) = acknowledgement.worker_outcome() else {
                    unreachable!("a Worker receipt carries its outcome");
                };
                if !legal_abort_closing_outcome(outcome) {
                    return Err(NativeAbortEffectSettleError::rejected(
                        NativeAbortEffectAckError::NonClosingWorkerOutcome(outcome),
                        self,
                    ));
                }
                let AckPayload::Context(receipt) = acknowledgement.payload() else {
                    return Err(NativeAbortEffectSettleError::rejected(
                        NativeAbortEffectAckError::MissingContextReceipt,
                        self,
                    ));
                };
                let expected_context = match &self.intent {
                    OperationIntent::AbortQueryContext(request) => request.context(),
                    _ => unreachable!("a Native Abort effect always retains an Abort intent"),
                };
                if receipt.context() != expected_context {
                    return Err(NativeAbortEffectSettleError::rejected(
                        NativeAbortEffectAckError::ContextReceiptIdentityMismatch,
                        self,
                    ));
                }
                if !matches!(
                    receipt.state(),
                    QueryContextState::Aborting
                        | QueryContextState::Releasing
                        | QueryContextState::TerminalRetained
                        | QueryContextState::Gone
                ) {
                    return Err(NativeAbortEffectSettleError::rejected(
                        NativeAbortEffectAckError::NonClosingContextReceipt,
                        self,
                    ));
                }
                self.submission
                    .take()
                    .expect("a live Native Abort retains actor settlement authority")
                    .worker_settled(receipt.clone())
                    .map_err(NativeAbortEffectSettleError::Application)?;
                Ok(NativeAbortEffectSettlement::WorkerSettled)
            }
        }
    }
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
    TransportUnknown(LateAbortQueryContextWorkerSettlement),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeAbortEffectAckError {
    OperationIdentityMismatch,
    OperationKindMismatch,
    NonClosingWorkerOutcome(OperationOutcome),
    MissingContextReceipt,
    ContextReceiptIdentityMismatch,
    NonClosingContextReceipt,
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
        spawn_logical_execution_actor,
    };
    use novarocks_types::NativeCompatibilityId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };

    use super::*;

    fn execution() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(89, 1), AttemptId::new(1).unwrap()).unwrap()
    }

    fn context(backend: BackendProcessId) -> QueryContextRef {
        QueryContextRef::new(execution(), FrontendProcessId::new_v7(), backend)
    }

    async fn next_effect(intake: &mut NativeAbortEffectIntake) -> NativeAbortEffect {
        tokio::time::timeout(Duration::from_secs(1), intake.recv())
            .await
            .expect("the actor must publish its Abort without blocking")
            .expect("the adapter intake remains connected")
    }

    #[tokio::test]
    async fn projects_actor_identity_and_replays_the_exact_native_abort() {
        let exact_context = context(BackendProcessId::new_v7());
        let (adapter, mut intake) = NativeAbortEffectAdapter::bounded(NonZeroUsize::MIN);
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution(),
            ExecutionEffect::None,
            NonZeroUsize::new(2).unwrap(),
            vec![exact_context],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        )
        .with_abort_query_context_effect_port(
            Arc::clone(&adapter) as Arc<dyn AbortQueryContextEffectPort>,
            NonZeroUsize::new(2).unwrap(),
        );
        let (owner, initial) =
            spawn_logical_execution_actor(&tokio::runtime::Handle::current(), config).unwrap();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let admission = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            exact_context,
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
            NativeCompatibilityId::new([7; 32]),
            AdmissionEpochCapability::try_from_bytes([7; 16]).unwrap(),
        );
        let activation = running.identity();
        let pending = running.begin_admission_issue(admission).await.unwrap();
        drop(running);
        actor
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
        assert!(matches!(
            first
                .settle(&OperationAcknowledgement::transport_unknown(
                    operation_id,
                    OperationKind::AbortQueryContext,
                ))
                .unwrap(),
            NativeAbortEffectSettlement::TransportUnknown(_)
        ));

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

        loop {
            if actor
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
            actor.snapshot().await.unwrap().conclusion,
            Some(LogicalConclusion::Cancelled)
        );
        let supervisor = owner.into_residual_stand_down_supervisor();
        drop(actor);
        tokio::time::timeout(Duration::from_secs(1), supervisor.join())
            .await
            .expect("settled Abort lets the owner finish")
            .unwrap();
    }

    #[tokio::test]
    async fn invalid_context_receipts_are_rejected_without_losing_the_effect() {
        let exact_context = context(BackendProcessId::new_v7());
        let (adapter, mut intake) = NativeAbortEffectAdapter::bounded(NonZeroUsize::MIN);
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution(),
            ExecutionEffect::None,
            NonZeroUsize::new(2).unwrap(),
            vec![exact_context],
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
        )
        .with_abort_query_context_effect_port(
            Arc::clone(&adapter) as Arc<dyn AbortQueryContextEffectPort>,
            NonZeroUsize::MIN,
        );
        let (owner, initial) =
            spawn_logical_execution_actor(&tokio::runtime::Handle::current(), config).unwrap();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let admission = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            exact_context,
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
            NativeCompatibilityId::new([7; 32]),
            AdmissionEpochCapability::try_from_bytes([7; 16]).unwrap(),
        );
        let activation = running.identity();
        let pending = running.begin_admission_issue(admission).await.unwrap();
        drop(running);
        actor
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

        // The test owns the retained effect and resolves it explicitly so the
        // actor can converge; rejection itself never classified it as unknown.
        assert!(matches!(
            effect
                .settle(&OperationAcknowledgement::worker_receipt(
                    operation_id,
                    OperationKind::AbortQueryContext,
                    OperationOutcome::Accepted,
                    AckPayload::Context(QueryContextReceipt::new(
                        exact_context,
                        QueryContextState::Aborting,
                    )),
                ))
                .unwrap(),
            NativeAbortEffectSettlement::WorkerSettled
        ));
        let supervisor = owner.into_residual_stand_down_supervisor();
        drop(actor);
        tokio::time::timeout(Duration::from_secs(1), supervisor.join())
            .await
            .expect("the retained effect can still settle")
            .unwrap();
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
