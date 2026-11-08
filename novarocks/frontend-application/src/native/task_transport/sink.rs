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

//! Original operation sink and acknowledgement intake.

use super::*;

#[derive(Debug)]
pub(super) struct TaskAckIntakeInner {
    pub(super) queue: Mutex<Vec<TaskOperationIntakeEvent>>,
    pub(super) wake: Arc<dyn StatusIntakeWake>,
}

/// Transport facts and receipts share one local order before reaching the
/// serial Participation owner. An acknowledgement cannot overtake its send fact.
#[derive(Debug)]
pub(crate) enum TaskOperationIntakeEvent {
    EstablishSendStarted {
        operation_id: TaskOperationId,
        participation: ParticipationRef,
    },
    Acknowledgement(Box<OperationAcknowledgement>),
}

/// The only handle the send path holds.
///
/// It can enqueue transport facts and settled acknowledgements and wake the
/// runner. It has no mutable Participation owner, task, stage, or lease; every fact
/// is interpreted on the serial runner.
#[derive(Clone, Debug)]
pub(crate) struct TaskAckIntakeHandle {
    pub(super) inner: Arc<TaskAckIntakeInner>,
}

impl TaskAckIntakeHandle {
    /// Enqueues one settled acknowledgement and wakes the runner.
    ///
    /// This queue is deliberately not capacity-bounded. Each ACK corresponds
    /// to a dispatch permit, and each Establish adds at most one send fact per
    /// exact transport attempt. Dropping either can stall a live owner.
    pub(crate) fn publish(&self, ack: OperationAcknowledgement) {
        self.inner
            .queue
            .lock()
            .expect("task acknowledgement queue")
            .push(TaskOperationIntakeEvent::Acknowledgement(Box::new(ack)));
        self.inner.wake.wake();
    }

    pub(crate) fn publish_establish_send_started(
        &self,
        operation_id: TaskOperationId,
        participation: ParticipationRef,
    ) {
        self.inner
            .queue
            .lock()
            .expect("task acknowledgement queue")
            .push(TaskOperationIntakeEvent::EstablishSendStarted {
                operation_id,
                participation,
            });
        self.inner.wake.wake();
    }
}

impl NativeTransportReadyWake for TaskAckIntakeHandle {
    fn wake(&self) {
        self.inner.wake.wake();
    }
}

/// The runner side of the acknowledgement intake.
#[derive(Debug)]
pub(crate) struct TaskAckIntake {
    pub(super) inner: Arc<TaskAckIntakeInner>,
}

impl TaskAckIntake {
    pub(crate) fn new(wake: Arc<dyn StatusIntakeWake>) -> Self {
        Self {
            inner: Arc::new(TaskAckIntakeInner {
                queue: Mutex::new(Vec::new()),
                wake,
            }),
        }
    }

    pub(crate) fn handle(&self) -> TaskAckIntakeHandle {
        TaskAckIntakeHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    pub(crate) fn queued(&self) -> usize {
        self.inner
            .queue
            .lock()
            .expect("task acknowledgement queue")
            .iter()
            .filter(|event| matches!(event, TaskOperationIntakeEvent::Acknowledgement(_)))
            .count()
    }

    /// Takes every transport fact in its local publication order.
    pub(crate) fn drain_events(&self) -> Vec<TaskOperationIntakeEvent> {
        std::mem::take(&mut *self.inner.queue.lock().expect("task acknowledgement queue"))
    }

    /// Test-only acknowledgement view of the intake.
    #[cfg(test)]
    pub(crate) fn drain(&self) -> Vec<OperationAcknowledgement> {
        self.drain_events()
            .into_iter()
            .filter_map(|event| match event {
                TaskOperationIntakeEvent::Acknowledgement(ack) => Some(*ack),
                TaskOperationIntakeEvent::EstablishSendStarted { .. } => None,
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Transport classification
// ---------------------------------------------------------------------------

/// Classifies one unary RPC status by type.
///
/// Only a status that leaves the remote outcome genuinely unknown becomes a
/// transport-unknown dispatch result, the single category the protocol allows
/// the identical immutable request to be resent under.
/// Everything else is settled and fails closed, however transient its wording
/// looks: this never reads a status message.
/// One frozen backend process this attempt may address.
#[derive(Clone)]
pub(super) struct TaskBackendTarget {
    pub(super) client: Client,
}

/// Construction facts authored before any attempt operation can be sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeTaskTransportCreationError {
    BudgetMismatch,
    NoBackends,
    DuplicateBackend { process: BackendProcessId },
    Registration(crate::native::transport_supervisor::NativeTransportRegistrationError),
    ZeroStatusErrorBudget,
}

impl novarocks_execution_contract::failure::FailureProjection for NativeTaskTransportCreationError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        let code = match self {
            Self::BudgetMismatch => 101,
            Self::NoBackends => 102,
            Self::DuplicateBackend { .. } => 103,
            Self::ZeroStatusErrorBudget => 106,
            Self::Registration(error) => return error.failure_fact(),
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
impl NativeTaskTransportCreationError {
    pub(crate) fn safe_detail(&self) -> novarocks_execution_contract::SafeDetail {
        let message = match self {
            Self::BudgetMismatch => {
                "attempt task transport budget differs from the process supervisor"
            }
            Self::NoBackends => "native task transport requires at least one frozen backend",
            Self::DuplicateBackend { .. } => "native task transport repeats a backend process",
            Self::ZeroStatusErrorBudget => {
                "native status subscription error budget must be nonzero"
            }
            Self::Registration(error) => return error.safe_detail(),
        };
        novarocks_execution_contract::SafeDetail::truncating(message)
    }
}
impl fmt::Display for NativeTaskTransportCreationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.safe_detail().as_str())
    }
}
impl std::error::Error for NativeTaskTransportCreationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Registration(error) => Some(error),
            _ => None,
        }
    }
}
impl From<crate::native::transport_supervisor::NativeTransportRegistrationError>
    for NativeTaskTransportCreationError
{
    fn from(error: crate::native::transport_supervisor::NativeTransportRegistrationError) -> Self {
        Self::Registration(error)
    }
}

pub(super) fn freeze_targets(
    backends: &[BackendProcessDescriptor],
    data_runtime: &FrontendDataRuntime,
) -> Result<BTreeMap<BackendProcessId, TaskBackendTarget>, NativeTaskTransportCreationError> {
    if backends.is_empty() {
        return Err(NativeTaskTransportCreationError::NoBackends);
    }
    let mut targets = BTreeMap::new();
    for descriptor in backends {
        let process_id = descriptor.process_id();
        let target = TaskBackendTarget {
            client: Client::for_backend(descriptor.clone(), data_runtime.clone()),
        };
        if targets.insert(process_id, target).is_some() {
            return Err(NativeTaskTransportCreationError::DuplicateBackend {
                process: process_id,
            });
        }
    }
    Ok(targets)
}

/// The native [`TaskOperationSink`].
///
/// The backend set is frozen at construction from one live snapshot, exactly
/// as the lifecycle transport freezes its participants. An operation addressed
/// to a process that is not in that set is not looked up elsewhere and not
/// retried: it fails the attempt, because a replaced process is a different
/// process.
pub(crate) struct NativeTaskOperationSink {
    pub(super) targets: BTreeMap<BackendProcessId, TaskBackendTarget>,
    pub(super) transport: TransportBudget,
    pub(super) attempt: AttemptWireFacts,
    pub(super) acks: TaskAckIntakeHandle,
    pub(super) data_runtime: FrontendDataRuntime,
    pub(super) proposal_wait: proposal_wait::OriginalProposalWait,
    pub(super) transport_waiter: std::mem::ManuallyDrop<NativeTransportWaiter>,
    pub(super) authority: novarocks_workload_control::joint_admission::NativeAsyncActionAuthority,
}

// The original registration position cannot be returned while its shared
// notification F is still registered. No preparation owns an alias to this F.
impl Drop for NativeTaskOperationSink {
    fn drop(&mut self) {
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.proposal_wait.retire()));
        // SAFETY: this Drop is the only release/take author. ManuallyDrop
        // prevents field cleanup from releasing the registration on unwind.
        match result {
            Ok(()) => unsafe { std::mem::ManuallyDrop::drop(&mut self.transport_waiter) },
            Err(cause) => {
                let lease = unsafe { std::mem::ManuallyDrop::take(&mut self.transport_waiter) };
                std::panic::resume_unwind(Box::new(proposal_wait::OriginalWaitExitFailure::new(
                    cause, lease,
                )));
            }
        }
    }
}

impl fmt::Debug for NativeTaskOperationSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeTaskOperationSink")
            .field("backends", &self.targets.len())
            .finish()
    }
}

impl NativeTaskOperationSink {
    pub(crate) fn new(
        backends: &[BackendProcessDescriptor],
        transport: TransportBudget,
        attempt: AttemptWireFacts,
        acks: TaskAckIntakeHandle,
        data_runtime: FrontendDataRuntime,
        authority: novarocks_workload_control::joint_admission::NativeAsyncActionAuthority,
    ) -> Result<Self, NativeTaskTransportCreationError> {
        if !data_runtime
            .task_transport_supervisor()
            .accepts_transport_budget(transport)
        {
            return Err(NativeTaskTransportCreationError::BudgetMismatch);
        }
        let transport_waiter = data_runtime
            .task_transport_supervisor()
            .register_waiter(Arc::new(acks.clone()))?;
        Ok(Self {
            targets: freeze_targets(backends, &data_runtime)?,
            transport,
            attempt,
            acks,
            data_runtime,
            proposal_wait: proposal_wait::OriginalProposalWait::default(),
            transport_waiter: std::mem::ManuallyDrop::new(transport_waiter),
            authority,
        })
    }

    /// Settles one operation locally, without a wire round trip.
    pub(super) fn settle_locally(
        &self,
        operation_id: TaskOperationId,
        kind: OperationKind,
        lease_renewal: bool,
        outcome: OperationOutcome,
    ) {
        observe_dispatch_result(
            kind,
            lease_renewal,
            OperationDispatchResult::NonWorkerRejected,
        );
        self.acks.publish(OperationAcknowledgement::local_refusal(
            operation_id,
            kind,
            crate::task_execution::error::local_protocol_refusal(kind, outcome),
        ));
    }

    pub(super) fn settle_batch_locally(
        &self,
        batch: &DispatchBatch,
        outcome: OperationOutcome,
        reason: &str,
    ) {
        observe_refusal(reason, batch.operations().len());
        for intent in batch.operations() {
            self.settle_locally(
                intent.operation_id(),
                intent.kind(),
                is_lease_renewal(intent),
                outcome,
            );
        }
    }
}

impl TaskOperationSink for NativeTaskOperationSink {
    fn try_reserve_queue(
        &self,
        request: crate::task_execution::intent::TaskOperationQueueRequest,
        preparation: &mut crate::task_execution::intent::TaskOperationQueuePreparation,
    ) -> TaskOperationQueueAdmission {
        let result = self.reserve_paid_queue_in(request, preparation);
        if matches!(
            &result,
            TaskOperationQueueAdmission::Rejected(novarocks_workload_control::WorkError::Conflict)
        ) {
            tracing::warn!("Native queued input source reported conflicting work facts");
        }
        result
    }

    fn try_submit(&self, mut batch: DispatchBatch) -> TaskOperationSubmit {
        let backend = batch.backend();
        let Some(target) = self.targets.get(&backend) else {
            // The batch addresses a process this attempt never froze, so no
            // endpoint could legally answer it.
            self.settle_batch_locally(
                &batch,
                OperationOutcome::IdentityMismatch,
                REFUSAL_UNKNOWN_BACKEND,
            );
            drop(batch.commit_queue_permits());
            return TaskOperationSubmit::Accepted;
        };
        if let Some(intent) = batch
            .operations()
            .iter()
            .find(|intent| intent.backend_process_id() != backend)
        {
            // One misaddressed item poisons the whole batch: the request is
            // per-backend, so an item for another process would be applied by
            // the wrong owner if it travelled with the rest.
            tracing::warn!(
                batch_backend = %backend,
                item_backend = %intent.backend_process_id(),
                "task operation batch mixes backend processes"
            );
            self.settle_batch_locally(
                &batch,
                OperationOutcome::IdentityMismatch,
                REFUSAL_PROCESS_MISMATCH,
            );
            drop(batch.commit_queue_permits());
            return TaskOperationSubmit::Accepted;
        }
        let input = match action_input::NativeApplyInputPlan::from_operations(
            batch.operations(),
            self.transport,
        ) {
            Ok(input) => input,
            Err(detail) => {
                tracing::warn!(
                    detail,
                    "Native Apply input exceeds its original batch bound"
                );
                self.settle_batch_locally(
                    &batch,
                    OperationOutcome::ResourceExhausted,
                    REFUSAL_OVER_BUDGET,
                );
                drop(batch.commit_queue_permits());
                return TaskOperationSubmit::Accepted;
            }
        };
        let encoding_plan =
            match joint_action::original_run_encoding(batch.operations(), self.transport) {
                Ok(plan) => plan,
                Err(error) => return TaskOperationSubmit::AdmissionFailed { batch, error },
            };
        let target = target.clone();
        let joint_action = match self.admit_original_batch(&mut batch, &target.client) {
            Ok(Some(action)) => action,
            Ok(None) => return TaskOperationSubmit::Backpressured(batch),
            Err(joint_action::NativeAdmissionFailure::Governance(error)) => {
                if matches!(&error, novarocks_workload_control::WorkError::Conflict) {
                    tracing::warn!("Native action source reported conflicting work facts");
                }
                return TaskOperationSubmit::AdmissionFailed { batch, error };
            }
            Err(joint_action::NativeAdmissionFailure::Unsent(reason)) => {
                return TaskOperationSubmit::Rejected { batch, reason };
            }
        };
        let joint_action::AcceptedNativeAction {
            slot: original_slot,
            publisher,
            notifications,
        } = joint_action;
        let mut operations = Vec::with_capacity(input.items());
        let mut sent = Vec::with_capacity(input.items());
        let mut unencodable = Vec::with_capacity(input.items());
        for intent in batch.operations() {
            let encoded = encode_operation(intent, &self.attempt);
            match encoded {
                Ok(operation) => {
                    operations.push((is_small_control(intent), operation));
                    sent.push(SentOperation {
                        operation_id: intent.operation_id(),
                        kind: intent.kind(),
                        establish_participation: match intent {
                            OperationIntent::EstablishParticipation(request) => {
                                Some(request.participation())
                            }
                            _ => None,
                        },
                        lease_renewal: is_lease_renewal(intent),
                        address: AckAddress::of(intent),
                    });
                }
                Err(detail) => {
                    // Do not settle this item until the sendable part of the
                    // batch has passed process admission. On backpressure the
                    // dispatcher must receive the whole exact batch back.
                    unencodable.push((
                        intent.operation_id(),
                        intent.kind(),
                        is_lease_renewal(intent),
                        detail,
                    ));
                }
            }
        }
        if operations.is_empty() {
            settle_unencodable(self, unencodable);
            drop(original_slot);
            drop(batch.commit_queue_permits());
            return TaskOperationSubmit::Accepted;
        }

        let items = operations.len();
        // The receiver applies the same bound. Applying it here as well turns
        // an oversized batch into a local failure with an exact cause, instead
        // of a round trip rejected after crossing the wire and consuming the
        // operations' own deadlines.
        let requests = match encode_method_batches(operations, self.transport) {
            Ok(requests) => requests,
            Err(error) => {
                tracing::warn!(
                    items,
                    detail = %error,
                    "task operation batch exceeds its transport budget"
                );
                observe_refusal(REFUSAL_OVER_BUDGET, sent.len());
                for item in sent {
                    self.settle_locally(
                        item.operation_id,
                        item.kind,
                        item.lease_renewal,
                        OperationOutcome::ResourceExhausted,
                    );
                }
                settle_unencodable(self, unencodable);
                drop(original_slot);
                drop(batch.commit_queue_permits());
                return TaskOperationSubmit::Accepted;
            }
        };
        let encoded_bytes: usize = requests.iter().map(EncodedMethodBatch::encoded_len).sum();
        // A dispatcher batch is one exact method/deadline run. Recheck the
        // borrowed invoice before Tonic can allocate its encoding buffer;
        // malformed codec facts cannot turn an underestimated size into growth.
        if requests.len() != 1
            || novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan::for_original_operation(encoded_bytes) != Some(encoding_plan)
        {
            return TaskOperationSubmit::AdmissionFailed {
                batch,
                error: novarocks_workload_control::WorkError::Conflict,
            };
        }
        original_slot
            .shrink_native_encoding(encoded_bytes)
            .expect("original paid encoding source remains installed");
        settle_unencodable(self, unencodable);
        observe_batch(batch.lane(), items, encoded_bytes);
        let queue_permits = batch.commit_queue_permits();
        // All method runs inherit the same submit-time origin. A later run
        // cannot gain another full max-wait after earlier runs consumed time.
        let submitted_at = tokio::time::Instant::now();

        let client = target.client.clone();
        let acks = self.acks.clone();
        // A submission never blocks on a round trip: the batch leaves on the
        // role's runtime and its receipts come back through the intake, which
        // is what lets the frontend keep one serial runner.
        let action_tail = publisher
            .protocol_tail()
            .expect("installed original Native action tail");
        let mut physical = original_slot
            .take_native_physical()
            .expect("original physical source transfers once to final commit");
        let encoding_permit = physical
            .encoding
            .take()
            .expect("same original encoding reservation");
        let send = ApplySend {
            client,
            receipts: AcceptedOperationReceipts::new_canonical(acks, sent, publisher),
            physical: physical.call.take(),
            action_tail: Some(action_tail),
            reply_plan:
                novarocks_task_codec::resource_preflight::OperationReplyDecodePlan::for_transport(
                    self.transport,
                ),
        };
        let reservations = ApplyReservations::new(queue_permits, encoding_permit);
        // The pure async constructor does not poll user code. Once committed,
        // this SAME slot owns F, its receipts and both original reservations,
        // including close races. Accepted reports ownership, not wire success.
        match original_slot.commit(reservations, apply_operations(send, requests, submitted_at)) {
            OriginalDisposition::Started => {}
            OriginalDisposition::ClosedAndRetiring => {
                tracing::debug!("Native Apply original retired after role close")
            }
            OriginalDisposition::SpawnUnjoinable => {
                tracing::error!("Native Apply spawn unwound without an original join handle")
            }
        }
        // The same original slot now owns the actual F and its physical
        // source. A callback unwind cannot drop the only accepted authority.
        for notification in notifications {
            notification.notify()
        }
        TaskOperationSubmit::Accepted
    }
}

pub(super) fn supervisor_lane(batch: &DispatchBatch) -> NativeTransportLane {
    if batch
        .operations()
        .iter()
        .any(OperationIntent::requires_control_progress)
    {
        NativeTransportLane::Control
    } else {
        NativeTransportLane::Ordinary
    }
}

pub(super) fn supervisor_lane_for_intent(intent: &OperationIntent) -> NativeTransportLane {
    if intent.requires_control_progress() {
        NativeTransportLane::Control
    } else {
        NativeTransportLane::Ordinary
    }
}

pub(super) fn is_small_control(intent: &OperationIntent) -> bool {
    intent.native_method()
        == novarocks_proto_codec::native_rpc::NativeRpcMethod::ApplyTaskControlOperations
}

pub(super) fn settle_unencodable(
    sink: &NativeTaskOperationSink,
    items: Vec<(TaskOperationId, OperationKind, bool, String)>,
) {
    for (operation_id, kind, lease_renewal, detail) in items {
        tracing::warn!(
            kind = kind.as_str(),
            detail,
            "task operation cannot be encoded"
        );
        observe_refusal(REFUSAL_UNENCODABLE, 1);
        sink.settle_locally(
            operation_id,
            kind,
            lease_renewal,
            OperationOutcome::InvalidStateOrRequest,
        );
    }
}
