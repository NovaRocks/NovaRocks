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

//! Async Query Application authorization in front of the synchronous Task
//! protocol transport seam.
//!
//! `TaskRound` remains the sole mutator of Task and context state. Its sink is
//! synchronous, while the logical-execution actor which authorizes admission
//! and Establish is asynchronous. This gate is the ownership bridge between
//! those two contracts: the first sink call records immutable authorization
//! facts and returns the exact batch to the dispatcher; the active attempt
//! drives actor authorization, and a later sink call can then transfer that
//! unchanged carrier into Native transport.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use novarocks_execution::task_execution::{OperationKind, TaskOperationId};
use novarocks_query_application::coordination::{
    AdmissionIssueReceipt, AdmissionIssueSettlement, EstablishIssueError, EstablishIssueSubmit,
    EstablishTransportAdmission, EstablishTransportReservation, EstablishTransportSink,
    EstablishTransportSubmission, LateEstablishWorkerSettlement, LogicalExecutionActorError,
    NativeAttemptDrive,
};
use novarocks_types::NativeCompatibilityId;

use super::error::TaskExecutionError;
use super::intent::{
    AckPayload, DispatchBatch, OperationAcknowledgement, OperationIntent,
    TaskOperationQueueAdmission, TaskOperationQueueRequest, TaskOperationSink, TaskOperationSubmit,
};
use super::round::AcknowledgementObserver;
use super::status_intake::StatusIntakeWake;

#[derive(Debug, Default)]
struct ActorGateQueue {
    authorization_requests: VecDeque<Vec<OperationIntent>>,
    requested: BTreeSet<TaskOperationId>,
    authorized: BTreeSet<TaskOperationId>,
    rejected: BTreeMap<TaskOperationId, String>,
    acknowledgements: VecDeque<OperationAcknowledgement>,
    closing: bool,
}

#[derive(Debug)]
struct ActorGateShared {
    queue: Mutex<ActorGateQueue>,
    wake: Arc<dyn StatusIntakeWake>,
}

impl ActorGateShared {
    fn gate_batch(&self, batch: DispatchBatch) -> GateBatchDecision {
        let gated = batch
            .operations()
            .iter()
            .filter(|intent| requires_actor_authorization(intent))
            .cloned()
            .collect::<Vec<_>>();
        if gated.is_empty() {
            return GateBatchDecision::Authorized(batch);
        }
        // A backend owns exactly one QueryContext per attempt, and that
        // context serializes Acquire before Establish. Therefore a production
        // lifecycle batch can contain exactly one actor-gated intent. Reject
        // any other shape before the actor ledger is touched; this keeps
        // authorization atomic without inventing cross-intent rollback.
        if gated.len() != 1 {
            return GateBatchDecision::Rejected(
                batch,
                "Task protocol batch contains multiple actor-gated intents for one backend"
                    .to_owned(),
            );
        }
        let operation_ids = gated
            .iter()
            .map(OperationIntent::operation_id)
            .collect::<Vec<_>>();
        let mut queue = self.queue.lock().expect("Task actor gate queue");
        if queue.closing {
            for operation_id in &operation_ids {
                queue.requested.remove(operation_id);
                queue.authorized.remove(operation_id);
                queue.rejected.remove(operation_id);
            }
            queue.authorization_requests.retain_mut(|intents| {
                intents.retain(|intent| !operation_ids.contains(&intent.operation_id()));
                !intents.is_empty()
            });
            return GateBatchDecision::Rejected(
                batch,
                "Task protocol actor gate is closing before Native transport ownership".to_owned(),
            );
        }
        if let Some(reason) = operation_ids
            .iter()
            .find_map(|operation_id| queue.rejected.get(operation_id).cloned())
        {
            for operation_id in &operation_ids {
                queue.requested.remove(operation_id);
                queue.authorized.remove(operation_id);
                queue.rejected.remove(operation_id);
            }
            return GateBatchDecision::Rejected(batch, reason);
        }
        if operation_ids
            .iter()
            .all(|operation_id| queue.authorized.contains(operation_id))
        {
            return GateBatchDecision::Authorized(batch);
        }
        let missing = gated
            .into_iter()
            .filter(|intent| {
                let operation_id = intent.operation_id();
                !queue.authorized.contains(&operation_id) && queue.requested.insert(operation_id)
            })
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            queue.authorization_requests.push_back(missing);
            drop(queue);
            self.wake.wake();
        }
        GateBatchDecision::Pending(batch)
    }

    fn take_authorization_request(&self) -> Option<Vec<OperationIntent>> {
        self.queue
            .lock()
            .expect("Task actor gate queue")
            .authorization_requests
            .pop_front()
    }

    fn finish_authorization(
        &self,
        intents: &[OperationIntent],
        result: &Result<(), TaskExecutionError>,
    ) {
        let mut queue = self.queue.lock().expect("Task actor gate queue");
        for intent in intents {
            let operation_id = intent.operation_id();
            queue.requested.remove(&operation_id);
            match result {
                Ok(()) => {
                    queue.authorized.insert(operation_id);
                }
                Err(error) => {
                    queue.rejected.insert(operation_id, error.to_string());
                }
            }
        }
        drop(queue);
        self.wake.wake();
    }

    fn note_transport_accepted(&self, operations: &[OperationIntent]) {
        let mut queue = self.queue.lock().expect("Task actor gate queue");
        for operation in operations {
            if requires_actor_authorization(operation) {
                queue.authorized.remove(&operation.operation_id());
            }
        }
    }

    fn enqueue_acknowledgement(&self, acknowledgement: OperationAcknowledgement) {
        self.queue
            .lock()
            .expect("Task actor gate queue")
            .acknowledgements
            .push_back(acknowledgement);
        self.wake.wake();
    }

    fn take_acknowledgement(&self) -> Option<OperationAcknowledgement> {
        self.queue
            .lock()
            .expect("Task actor gate queue")
            .acknowledgements
            .pop_front()
    }
}

enum GateBatchDecision {
    Pending(DispatchBatch),
    Authorized(DispatchBatch),
    Rejected(DispatchBatch, String),
}

/// Task sink installed in `QueryTaskExecution` for an actor-owned attempt.
///
/// TaskRound retains the process queue permit while authorization is pending;
/// this owner stores only a bounded clone of the immutable gated intent.
#[derive(Debug)]
pub(crate) struct ActorGatedTaskOperationSink {
    inner: Arc<dyn TaskOperationSink>,
    shared: Arc<ActorGateShared>,
}

impl ActorGatedTaskOperationSink {
    pub(crate) fn pair(
        inner: Arc<dyn TaskOperationSink>,
        wake: Arc<dyn StatusIntakeWake>,
        native_compatibility_id: NativeCompatibilityId,
    ) -> (Self, ActorGateOwner, Arc<dyn AcknowledgementObserver>) {
        let shared = Arc::new(ActorGateShared {
            queue: Mutex::new(ActorGateQueue::default()),
            wake,
        });
        let sink = Self {
            inner: Arc::clone(&inner),
            shared: Arc::clone(&shared),
        };
        let owner = ActorGateOwner {
            shared: Arc::clone(&shared),
            native_compatibility_id,
            admission_issues: BTreeMap::new(),
            establish_authorization_attempted: BTreeSet::new(),
            establish_submissions: BTreeMap::new(),
            late_establish_settlements: BTreeMap::new(),
            active_authorization: None,
            active_acknowledgement: None,
        };
        let observer = Arc::new(ActorGateAcknowledgementObserver { shared })
            as Arc<dyn AcknowledgementObserver>;
        (sink, owner, observer)
    }
}

impl TaskOperationSink for ActorGatedTaskOperationSink {
    fn try_reserve_queue(&self, request: TaskOperationQueueRequest) -> TaskOperationQueueAdmission {
        self.inner.try_reserve_queue(request)
    }

    fn try_submit(&self, batch: DispatchBatch) -> TaskOperationSubmit {
        match self.shared.gate_batch(batch) {
            GateBatchDecision::Pending(batch) => TaskOperationSubmit::Backpressured(batch),
            GateBatchDecision::Rejected(batch, reason) => {
                TaskOperationSubmit::Rejected { batch, reason }
            }
            GateBatchDecision::Authorized(batch) => {
                let operations = batch.operations().to_vec();
                let result = self.inner.try_submit(batch);
                if matches!(result, TaskOperationSubmit::Accepted) {
                    self.shared.note_transport_accepted(&operations);
                }
                result
            }
        }
    }
}

fn requires_actor_authorization(intent: &OperationIntent) -> bool {
    match intent {
        OperationIntent::AcquireQueryContextAdmissionTicket(_) => true,
        OperationIntent::EstablishQueryContext(_) => true,
        _ => false,
    }
}

#[derive(Debug)]
struct ActorGateAcknowledgementObserver {
    shared: Arc<ActorGateShared>,
}

impl AcknowledgementObserver for ActorGateAcknowledgementObserver {
    fn observe_acknowledgement(
        &self,
        acknowledgement: &OperationAcknowledgement,
    ) -> Result<(), String> {
        if matches!(
            acknowledgement.kind(),
            OperationKind::AcquireQueryContextAdmissionTicket | OperationKind::UpdateQueryContext
        ) {
            self.shared.enqueue_acknowledgement(acknowledgement.clone());
        }
        Ok(())
    }
}

/// Active-attempt owner of all actor authorizations accepted by the gate.
///
/// It is driven only by the active Native behavior which borrows the exact
/// `NativeAttemptDrive`. Dropping it cannot silently make an Establish
/// definitely absent: the actor's move-only submission guards publish unknown
/// ownership from their `Drop` implementation.
pub(crate) struct ActorGateOwner {
    shared: Arc<ActorGateShared>,
    native_compatibility_id: NativeCompatibilityId,
    admission_issues: BTreeMap<TaskOperationId, AdmissionIssueReceipt>,
    establish_authorization_attempted: BTreeSet<TaskOperationId>,
    establish_submissions: BTreeMap<TaskOperationId, EstablishTransportSubmission>,
    late_establish_settlements: BTreeMap<TaskOperationId, VecDeque<LateEstablishWorkerSettlement>>,
    active_authorization: Option<Vec<OperationIntent>>,
    active_acknowledgement: Option<OperationAcknowledgement>,
}

impl std::fmt::Debug for ActorGateOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActorGateOwner")
            .field("admission_issues", &self.admission_issues.len())
            .field("establish_submissions", &self.establish_submissions.len())
            .field(
                "late_establish_settlements",
                &self.late_establish_settlements.len(),
            )
            .finish_non_exhaustive()
    }
}

impl ActorGateOwner {
    /// Settles observed Worker facts and releases at most one newly authorized
    /// batch. The one-batch bound preserves TaskRound fairness under a burst of
    /// contexts.
    pub(crate) async fn drive(
        &mut self,
        drive: &NativeAttemptDrive,
    ) -> Result<usize, TaskExecutionError> {
        let mut moved = self.settle_acknowledgements(drive).await?;
        if self.active_authorization.is_none() {
            self.active_authorization = self.shared.take_authorization_request();
        }
        let Some(intents) = self.active_authorization.as_ref().cloned() else {
            return Ok(moved);
        };
        // `active_authorization` remains in this owner across every await. If
        // the caller cancels this borrowed future, the exact request remains
        // retryable while TaskRound still owns the unchanged dispatch batch.
        let result = self.authorize_intents(intents.clone(), drive).await;
        self.shared.finish_authorization(&intents, &result);
        self.active_authorization = None;
        moved += usize::from(result.is_ok());
        result.map(|()| moved)
    }

    /// Settles every already observed Worker verdict after the Task round has
    /// failed. No new actor authorization is legal for a terminal attempt.
    pub(crate) async fn settle_terminal_acknowledgements(
        &mut self,
        drive: &NativeAttemptDrive,
    ) -> Result<usize, TaskExecutionError> {
        self.settle_acknowledgements(drive).await
    }

    /// Closes the role-local half after the active run has stopped issuing
    /// actor-gated operations.
    ///
    /// A transport-owned Establish must still publish either Worker-settled
    /// or transport-unknown before this succeeds. Late settlement handles are
    /// already represented by the actor's residual ledger, so the active
    /// owner may drop its local handles when it hands the attempt to
    /// convergence. Admission receipts are copyable correlation proofs; the
    /// actor remains the owner of every issue that did not receive a Worker
    /// verdict.
    pub(crate) fn prepare_convergence(&mut self) -> bool {
        let definitely_unsent_establish = {
            let mut queue = self.shared.queue.lock().expect("Task actor gate queue");
            queue.closing = true;
            self.establish_submissions
                .keys()
                .filter(|operation_id| queue.authorized.contains(operation_id))
                .copied()
                .collect::<Vec<_>>()
        };
        for operation_id in definitely_unsent_establish {
            if let Some(submission) = self.establish_submissions.remove(&operation_id) {
                let _ = submission.definitely_unsent();
            }
        }
        let queue = self.shared.queue.lock().expect("Task actor gate queue");
        let transport_owned = !queue.authorization_requests.is_empty()
            || !queue.requested.is_empty()
            || !queue.authorized.is_empty()
            || !queue.rejected.is_empty()
            || !queue.acknowledgements.is_empty()
            || !self.establish_submissions.is_empty()
            || self.active_authorization.is_some()
            || self.active_acknowledgement.is_some();
        drop(queue);
        if transport_owned {
            return false;
        }
        self.admission_issues.clear();
        self.late_establish_settlements.clear();
        true
    }

    async fn authorize_intents(
        &mut self,
        intents: Vec<OperationIntent>,
        drive: &NativeAttemptDrive,
    ) -> Result<(), TaskExecutionError> {
        for intent in &intents {
            match intent {
                OperationIntent::AcquireQueryContextAdmissionTicket(request) => {
                    if !self
                        .admission_issues
                        .contains_key(&request.envelope().operation_id())
                    {
                        let receipt = drive
                            .begin_admission_issue(*request)
                            .await
                            .map_err(actor_error)?;
                        self.admission_issues
                            .insert(request.envelope().operation_id(), receipt);
                    }
                }
                OperationIntent::EstablishQueryContext(establish) => {
                    let operation_id = establish.envelope().operation_id();
                    if self.establish_submissions.contains_key(&operation_id) {
                        continue;
                    }
                    let transport_unknown = self
                        .late_establish_settlements
                        .get(&operation_id)
                        .is_some_and(|settlements| !settlements.is_empty());
                    let replay = self
                        .establish_authorization_attempted
                        .contains(&operation_id)
                        || transport_unknown;
                    // Record the call before awaiting it. If cancellation
                    // drops the actor reply permit, its Drop publishes
                    // DefinitelyUnsent and the next drive must reauthorize
                    // the actor-retained exact request.
                    self.establish_authorization_attempted.insert(operation_id);
                    let permit = if replay {
                        drive
                            .reauthorize_establish(establish.context())
                            .await
                            .map_err(|error| match error {
                                LogicalExecutionActorError::Establish(
                                    EstablishIssueError::AuthorizationBudgetExhausted,
                                ) if transport_unknown => {
                                    // Exhausting bounded exact replays leaves
                                    // an infrastructure fact for the attempt
                                    // recovery owner, not a contract violation.
                                    TaskExecutionError::PreReadyEstablishTransportUnknown {
                                        backend: establish.context().backend_process_id(),
                                    }
                                }
                                error => actor_error(error),
                            })?
                    } else {
                        drive
                            .authorize_establish(
                                Arc::clone(establish),
                                self.native_compatibility_id,
                            )
                            .await
                            .map_err(actor_error)?
                    };
                    let capture = EstablishSubmissionCapture::default();
                    match permit.try_submit(&capture).map_err(actor_establish_error)? {
                        EstablishIssueSubmit::Accepted => {}
                        EstablishIssueSubmit::Backpressured => {
                            return Err(TaskExecutionError::Schedule(
                                "actor Establish capture unexpectedly backpressured after the Task transport queue was reserved"
                                    .to_owned(),
                            ));
                        }
                    }
                    let submission = capture.take().ok_or_else(|| {
                        TaskExecutionError::Schedule(
                            "actor accepted Establish without transferring its submission guard"
                                .to_owned(),
                        )
                    })?;
                    if submission.request().operation_id() != operation_id
                        || submission.request().context() != establish.context()
                    {
                        return Err(TaskExecutionError::Schedule(
                            "actor authorized an Establish different from the Task protocol intent"
                                .to_owned(),
                        ));
                    }
                    self.establish_submissions.insert(operation_id, submission);
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn settle_acknowledgements(
        &mut self,
        drive: &NativeAttemptDrive,
    ) -> Result<usize, TaskExecutionError> {
        let mut moved = 0;
        let mut first_error = None;
        loop {
            if self.active_acknowledgement.is_none() {
                self.active_acknowledgement = self.shared.take_acknowledgement();
            }
            let Some(acknowledgement) = self.active_acknowledgement.clone() else {
                break;
            };
            let settled = match acknowledgement.kind() {
                OperationKind::AcquireQueryContextAdmissionTicket => {
                    self.settle_admission_acknowledgement(drive, &acknowledgement)
                        .await
                }
                OperationKind::UpdateQueryContext => {
                    self.settle_establish_acknowledgement(&acknowledgement)
                }
                _ => Ok(0),
            };
            match settled {
                Ok(settled) => {
                    self.active_acknowledgement = None;
                    moved += settled;
                }
                Err(error) => {
                    // Consume this failed fact, but settle later independent
                    // Worker verdicts before returning the first error.
                    self.active_acknowledgement = None;
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(moved), Err)
    }

    async fn settle_admission_acknowledgement(
        &mut self,
        drive: &NativeAttemptDrive,
        acknowledgement: &OperationAcknowledgement,
    ) -> Result<usize, TaskExecutionError> {
        let operation_id = acknowledgement.operation_id();
        let Some(&issue) = self.admission_issues.get(&operation_id) else {
            return Ok(0);
        };
        let Some(outcome) = acknowledgement.worker_outcome() else {
            // The same Task protocol request is retained for exact replay. Its
            // first-send receipt remains the actor ledger's authority.
            return Ok(0);
        };
        let settlement = match acknowledgement.payload() {
            AckPayload::AdmissionTicket(ticket) => {
                AdmissionIssueSettlement::applied(operation_id, outcome, *ticket)
            }
            _ => AdmissionIssueSettlement::rejected(operation_id, outcome),
        }
        .map_err(actor_establish_error)?;
        drive
            .settle_admission_issue(issue, settlement)
            .await
            .map_err(actor_error)?;
        self.admission_issues.remove(&operation_id);
        Ok(1)
    }

    fn settle_establish_acknowledgement(
        &mut self,
        acknowledgement: &OperationAcknowledgement,
    ) -> Result<usize, TaskExecutionError> {
        let operation_id = acknowledgement.operation_id();
        let Some(outcome) = acknowledgement.worker_outcome() else {
            let Some(submission) = self.establish_submissions.remove(&operation_id) else {
                return Ok(0);
            };
            let late = submission
                .transport_unknown()
                .map_err(actor_establish_error)?;
            self.late_establish_settlements
                .entry(operation_id)
                .or_default()
                .push_back(late);
            // The context owner retains the exact request for replay. The
            // next actor authorization preserves its operation, ticket and
            // BackendProcessId, so a replacement Worker still rejects the
            // old identity. Report progress so this unknown cannot strand an
            // already queued retry while no topology change is occurring.
            return Ok(1);
        };
        let active = self.establish_submissions.remove(&operation_id);
        let Some(mut late) = self.late_establish_settlements.remove(&operation_id) else {
            if let Some(submission) = active {
                submission
                    .worker_settled(outcome)
                    .map_err(actor_establish_error)?;
                return Ok(1);
            }
            return Ok(0);
        };

        // One definitive Worker result settles every transport generation of
        // this exact operation. Publish oldest-first so the actor observes the
        // same causal order in which unknown outcomes and reauthorizations
        // were created. Consume every guard even if its channel is already
        // closed; retaining an unobservable late settlement would leak owner
        // responsibility forever.
        let mut settled = 0;
        let mut first_error = None;
        while let Some(settlement) = late.pop_front() {
            settled += 1;
            if let Err(error) = settlement.worker_settled(outcome)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Some(submission) = active {
            settled += 1;
            if let Err(error) = submission.worker_settled(outcome)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Some(error) = first_error {
            return Err(actor_establish_error(error));
        }
        Ok(settled)
    }

    #[cfg(test)]
    fn pending_authorizations(&self) -> usize {
        let queue = self.shared.queue.lock().expect("Task actor gate queue");
        queue.authorization_requests.len() + usize::from(self.active_authorization.is_some())
    }
}

fn actor_error(error: impl std::fmt::Display) -> TaskExecutionError {
    TaskExecutionError::Schedule(format!(
        "logical execution actor refused Task protocol work: {error}"
    ))
}

fn actor_establish_error(error: impl std::fmt::Display) -> TaskExecutionError {
    TaskExecutionError::Schedule(format!(
        "logical execution actor Establish gate failed: {error}"
    ))
}

#[derive(Debug, Default)]
struct EstablishSubmissionCapture {
    submission: Arc<Mutex<Option<EstablishTransportSubmission>>>,
}

impl EstablishSubmissionCapture {
    fn take(&self) -> Option<EstablishTransportSubmission> {
        self.submission
            .lock()
            .expect("Establish submission capture")
            .take()
    }
}

impl EstablishTransportSink for EstablishSubmissionCapture {
    fn try_reserve(
        &self,
        _identity: novarocks_query_application::coordination::EstablishIssueIdentity,
    ) -> EstablishTransportAdmission {
        EstablishTransportAdmission::Admitted(Box::new(EstablishSubmissionCaptureReservation {
            submission: Arc::clone(&self.submission),
        }))
    }
}

#[derive(Debug)]
struct EstablishSubmissionCaptureReservation {
    submission: Arc<Mutex<Option<EstablishTransportSubmission>>>,
}

impl EstablishTransportReservation for EstablishSubmissionCaptureReservation {
    fn submit(self: Box<Self>, submission: EstablishTransportSubmission) {
        let previous = self
            .submission
            .lock()
            .expect("Establish submission capture")
            .replace(submission);
        assert!(
            previous.is_none(),
            "one Establish authorization transfers exactly one submission guard"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use novarocks_execution::task_execution::{
        AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
        CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch,
        CredentialLeaseId, CredentialUpdate, EstablishQueryContext, LeaseReceipt, LeaseSequence,
        LeaseValidFor, OperationOutcome, QueryContextAdmissionTicketReceipt, QueryContextReceipt,
        QueryContextRef, QueryContextState, TaskOperationId,
    };
    use novarocks_query_application::coordination::{
        AbortQueryContextEffectPort, DispatchLane, ExecutionEffect, LogicalExecutionActorConfig,
        MonotonicInstant,
    };
    use novarocks_query_application::test_support::LogicalExecutionTestHarness;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };
    use novarocks_workload_control::{
        ResourceConfig, Stage, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::*;
    use crate::task_execution::context_owner::{ContextEstablishFacts, QueryContextOwner};
    use crate::task_execution::intent::test_queue_permit;
    use crate::task_execution::status_intake::CountingWake;

    #[derive(Debug, Default)]
    struct CountingSink {
        submitted: AtomicUsize,
    }

    impl TaskOperationSink for CountingSink {
        fn try_reserve_queue(
            &self,
            _request: TaskOperationQueueRequest,
        ) -> TaskOperationQueueAdmission {
            TaskOperationQueueAdmission::Admitted(test_queue_permit())
        }

        fn try_submit(&self, _batch: DispatchBatch) -> TaskOperationSubmit {
            self.submitted.fetch_add(1, Ordering::SeqCst);
            TaskOperationSubmit::Accepted
        }
    }

    #[derive(Debug, Default)]
    struct AlwaysBackpressuredSink;

    impl TaskOperationSink for AlwaysBackpressuredSink {
        fn try_reserve_queue(
            &self,
            _request: TaskOperationQueueRequest,
        ) -> TaskOperationQueueAdmission {
            TaskOperationQueueAdmission::Admitted(test_queue_permit())
        }

        fn try_submit(&self, batch: DispatchBatch) -> TaskOperationSubmit {
            TaskOperationSubmit::Backpressured(batch)
        }
    }

    fn batch(backend: BackendProcessId, operation: OperationIntent) -> DispatchBatch {
        let bytes = operation.queue_request().queued_bytes();
        DispatchBatch::test_fixture(backend, DispatchLane::Lifecycle, vec![operation], bytes)
    }

    fn context(backend: BackendProcessId) -> QueryContextRef {
        QueryContextRef::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            backend,
        )
    }

    fn admission(context: QueryContextRef, compatibility: u8) -> OperationIntent {
        OperationIntent::AcquireQueryContextAdmissionTicket(
            AcquireQueryContextAdmissionTicket::new(
                TaskOperationId::new_v7(),
                context,
                LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
                NativeCompatibilityId::new([compatibility; 32]),
                AdmissionEpochCapability::try_from_bytes([9; 16]).unwrap(),
            ),
        )
    }

    async fn logical_execution(
        context: QueryContextRef,
    ) -> (
        LogicalExecutionTestHarness,
        NativeAttemptDrive,
        super::super::abort_effect::NativeAbortEffectIntake,
    ) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 10,
                per_scope_bytes: (1 << 20) - (1 << 10),
            },
        )
        .expect("valid workload control");
        control.mark_ready().expect("workload control ready");
        let governed = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("query work admitted");
        let execution_stage = governed
            .owner
            .scope()
            .try_acquire(Stage::Execution)
            .expect("execution stage admitted");
        let (abort_adapter, abort_intake) =
            super::super::abort_effect::NativeAbortEffectAdapter::bounded(
                NonZeroUsize::new(2).unwrap(),
                Arc::new(CountingWake::default()),
            );
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            context.query_execution_id(),
            ExecutionEffect::None,
            NonZeroUsize::new(2).unwrap(),
            vec![context],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            governed.owner,
            execution_stage,
        )
        .expect("valid actor config")
        .with_abort_query_context_effect_port(
            abort_adapter as Arc<dyn AbortQueryContextEffectPort>,
            NonZeroUsize::new(2).unwrap(),
        );
        let mut logical = LogicalExecutionTestHarness::install(
            tokio::runtime::Handle::current(),
            config,
            context.query_execution_id(),
            vec![context],
        )
        .expect("logical execution installed");
        logical.activate_initial().await.expect("attempt activated");
        let drive = logical.native_attempt_drive();
        (logical, drive, abort_intake)
    }

    #[derive(Debug)]
    struct TestContent;

    impl CodecOwnedContent for TestContent {
        fn fingerprint(&self) -> ContentFingerprint {
            ContentFingerprint::from_bytes([31; 16])
        }

        fn encoded_len(&self) -> usize {
            1
        }
    }

    struct TestSecret;

    impl ConfidentialContent for TestSecret {
        fn encoded_len(&self) -> usize {
            1
        }

        fn matches(&self, other: &dyn ConfidentialContent) -> bool {
            other.encoded_len() == 1
        }
    }

    fn establish_facts() -> ContextEstablishFacts {
        ContextEstablishFacts {
            catalog_binding: Arc::new(TestContent),
            initial_runtime_filter: Arc::new(TestContent),
            query_options: Arc::new(TestContent),
            initial_credential: CredentialUpdate::new(
                CredentialLeaseId::new(31),
                CredentialEpoch::new(1).unwrap(),
                Arc::new(TestSecret),
            ),
        }
    }

    struct EstablishReplayHarness {
        logical: LogicalExecutionTestHarness,
        drive: NativeAttemptDrive,
        _abort_intake: super::super::abort_effect::NativeAbortEffectIntake,
        sink: ActorGatedTaskOperationSink,
        owner: ActorGateOwner,
        observer: Arc<dyn AcknowledgementObserver>,
        context_owner: QueryContextOwner,
        request: Arc<EstablishQueryContext>,
    }

    impl EstablishReplayHarness {
        async fn new() -> Self {
            let context = context(BackendProcessId::new_v7());
            let (logical, drive, abort_intake) = logical_execution(context).await;
            let (sink, mut owner, observer) = ActorGatedTaskOperationSink::pair(
                Arc::new(CountingSink::default()),
                Arc::new(CountingWake::default()),
                NativeCompatibilityId::new([31; 32]),
            );
            let mut context_owner = QueryContextOwner::new(
                context,
                0,
                NativeCompatibilityId::new([31; 32]),
                AdmissionEpochCapability::try_from_bytes([9; 16]).unwrap(),
            );
            let admission = context_owner
                .admission_intent(MonotonicInstant::ORIGIN)
                .unwrap()
                .unwrap();
            let OperationIntent::AcquireQueryContextAdmissionTicket(request) = admission else {
                panic!("the context first requests admission");
            };
            let issue = drive.begin_admission_issue(request).await.unwrap();
            let ticket = QueryContextAdmissionTicketReceipt::new(
                AdmissionTicketId::try_from_bytes([31; 16]).unwrap(),
                context,
                request.valid_for(),
            );
            drive
                .settle_admission_issue(
                    issue,
                    AdmissionIssueSettlement::applied(
                        request.envelope().operation_id(),
                        OperationOutcome::Accepted,
                        ticket,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            context_owner
                .on_admission_ack(
                    &OperationAcknowledgement::worker_receipt(
                        request.envelope().operation_id(),
                        OperationKind::AcquireQueryContextAdmissionTicket,
                        OperationOutcome::Accepted,
                        AckPayload::AdmissionTicket(ticket),
                    ),
                    MonotonicInstant::ORIGIN,
                )
                .unwrap();
            let establish = context_owner
                .establish_intent(establish_facts(), MonotonicInstant::ORIGIN)
                .unwrap()
                .unwrap();
            let OperationIntent::EstablishQueryContext(request) = &establish else {
                panic!("the admitted context issues Establish");
            };
            let request = Arc::clone(request);
            let retained = match sink.try_submit(batch(context.backend_process_id(), establish)) {
                TaskOperationSubmit::Backpressured(batch) => batch,
                other => panic!("the actor must authorize Establish first: {other:?}"),
            };
            assert_eq!(owner.drive(&drive).await.unwrap(), 1);
            assert!(matches!(
                sink.try_submit(retained),
                TaskOperationSubmit::Accepted
            ));
            Self {
                logical,
                drive,
                _abort_intake: abort_intake,
                sink,
                owner,
                observer,
                context_owner,
                request,
            }
        }

        async fn lose_ack_and_queue_replay(&mut self) -> DispatchBatch {
            let acknowledgement = OperationAcknowledgement::transport_unknown(
                self.request.envelope().operation_id(),
                OperationKind::UpdateQueryContext,
            );
            self.observer
                .observe_acknowledgement(&acknowledgement)
                .unwrap();
            self.context_owner.on_context_ack(&acknowledgement).unwrap();
            // TaskRound queues the exact retry before the actor gate settles
            // the observed unknown and authorizes that retry in one drive.
            let replay = self
                .context_owner
                .establish_intent(establish_facts(), MonotonicInstant::ORIGIN)
                .unwrap()
                .unwrap();
            let OperationIntent::EstablishQueryContext(request) = &replay else {
                panic!("an unknown Establish keeps its exact request");
            };
            assert!(Arc::ptr_eq(request, &self.request));
            match self
                .sink
                .try_submit(batch(self.request.context().backend_process_id(), replay))
            {
                TaskOperationSubmit::Backpressured(batch) => batch,
                other => panic!("the exact replay requires new actor authority: {other:?}"),
            }
        }

        async fn finish(mut self) {
            let context = self.request.context();
            drop(self.drive);
            drop(self.owner);
            self.logical.abandon_running_attempt();
            tokio::time::timeout(Duration::from_secs(2), async {
                while self
                    .logical
                    .stand_down_snapshot(context)
                    .await
                    .unwrap()
                    .is_none()
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("abandoned attempt must enter stand-down before test cleanup");
            self.logical
                .observe_worker_process_replaced(context)
                .await
                .unwrap();
            self.logical
                .finish_until(std::time::Instant::now() + Duration::from_secs(2))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn unknown_establish_replays_exact_request_and_settles_idempotent_receipt() {
        let mut harness = EstablishReplayHarness::new().await;
        let replay = harness.lose_ack_and_queue_replay().await;
        assert_eq!(harness.owner.drive(&harness.drive).await.unwrap(), 2);
        assert!(matches!(
            harness.sink.try_submit(replay),
            TaskOperationSubmit::Accepted
        ));
        let receipt =
            QueryContextReceipt::new(harness.request.context(), QueryContextState::Active)
                .with_lease(LeaseReceipt::new(
                    LeaseSequence::INITIAL,
                    harness.request.initial_lease_valid_for(),
                    Duration::from_secs(30),
                ));
        let acknowledgement = OperationAcknowledgement::worker_receipt(
            harness.request.envelope().operation_id(),
            OperationKind::UpdateQueryContext,
            OperationOutcome::Idempotent,
            AckPayload::Context(receipt),
        );
        harness
            .observer
            .observe_acknowledgement(&acknowledgement)
            .unwrap();
        harness
            .context_owner
            .on_context_ack(&acknowledgement)
            .unwrap();
        assert_eq!(harness.owner.drive(&harness.drive).await.unwrap(), 2);
        assert!(!harness.context_owner.needs_establish());
        assert!(harness.owner.establish_submissions.is_empty());
        assert!(harness.owner.late_establish_settlements.is_empty());
        assert!(harness.owner.prepare_convergence());
        harness.finish().await;
    }

    #[tokio::test]
    async fn replay_rejected_by_replacement_process_preserves_fencing_failure() {
        let mut harness = EstablishReplayHarness::new().await;
        let replay = harness.lose_ack_and_queue_replay().await;
        assert_eq!(harness.owner.drive(&harness.drive).await.unwrap(), 2);
        assert!(matches!(
            harness.sink.try_submit(replay),
            TaskOperationSubmit::Accepted
        ));
        let acknowledgement = OperationAcknowledgement::worker_receipt(
            harness.request.envelope().operation_id(),
            OperationKind::UpdateQueryContext,
            OperationOutcome::IdentityMismatch,
            AckPayload::None,
        );
        harness
            .observer
            .observe_acknowledgement(&acknowledgement)
            .unwrap();
        assert!(matches!(
            harness.context_owner.on_context_ack(&acknowledgement),
            Err(TaskExecutionError::PreReadyEstablishRejected {
                backend,
                outcome: OperationOutcome::IdentityMismatch,
            }) if backend == harness.request.context().backend_process_id()
        ));
        assert_eq!(harness.owner.drive(&harness.drive).await.unwrap(), 2);
        assert!(harness.owner.prepare_convergence());
        harness.finish().await;
    }

    #[tokio::test]
    async fn unknown_establish_replay_budget_exhaustion_preserves_attempt_recovery() {
        let mut harness = EstablishReplayHarness::new().await;
        let replay = harness.lose_ack_and_queue_replay().await;
        assert_eq!(harness.owner.drive(&harness.drive).await.unwrap(), 2);
        assert!(matches!(
            harness.sink.try_submit(replay),
            TaskOperationSubmit::Accepted
        ));
        let retained = harness.lose_ack_and_queue_replay().await;
        assert!(matches!(
            harness.owner.drive(&harness.drive).await,
            Err(TaskExecutionError::PreReadyEstablishTransportUnknown { backend })
                if backend == harness.request.context().backend_process_id()
        ));
        assert!(matches!(
            harness.sink.try_submit(retained),
            TaskOperationSubmit::Rejected { .. }
        ));
        assert!(harness.owner.prepare_convergence());
        harness.finish().await;
    }

    #[test]
    fn ordinary_batches_pass_directly_to_native_transport() {
        let inner = Arc::new(CountingSink::default());
        let wake = Arc::new(CountingWake::default());
        let (sink, owner, _observer) = ActorGatedTaskOperationSink::pair(
            Arc::clone(&inner) as Arc<dyn TaskOperationSink>,
            wake,
            NativeCompatibilityId::new([7; 32]),
        );
        let context = context(BackendProcessId::new_v7());
        let abort = novarocks_execution::task_execution::AbortQueryContext::new(
            novarocks_execution::task_execution::TaskOperationId::new_v7(),
            context,
            novarocks_execution::task_execution::AbortCause::QueryFailed,
        );

        assert!(matches!(
            sink.try_submit(batch(
                context.backend_process_id(),
                OperationIntent::AbortQueryContext(abort)
            )),
            TaskOperationSubmit::Accepted
        ));
        assert_eq!(inner.submitted.load(Ordering::SeqCst), 1);
        assert_eq!(owner.pending_authorizations(), 0);
    }

    #[test]
    fn admission_batches_remain_owned_until_async_actor_drive() {
        let inner = Arc::new(CountingSink::default());
        let wake = Arc::new(CountingWake::default());
        let (sink, owner, _observer) = ActorGatedTaskOperationSink::pair(
            Arc::clone(&inner) as Arc<dyn TaskOperationSink>,
            Arc::clone(&wake) as Arc<dyn StatusIntakeWake>,
            NativeCompatibilityId::new([8; 32]),
        );
        let backend = BackendProcessId::new_v7();
        let context = context(backend);
        let admission = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context,
            LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
            NativeCompatibilityId::new([8; 32]),
            AdmissionEpochCapability::try_from_bytes([9; 16]).unwrap(),
        );

        let retained = match sink.try_submit(batch(
            backend,
            OperationIntent::AcquireQueryContextAdmissionTicket(admission),
        )) {
            TaskOperationSubmit::Backpressured(batch) => batch,
            other => panic!("first gated submission must remain dispatcher-owned: {other:?}"),
        };
        assert_eq!(inner.submitted.load(Ordering::SeqCst), 0);
        assert_eq!(
            retained.operations()[0].operation_id(),
            admission.envelope().operation_id()
        );
        assert_eq!(owner.pending_authorizations(), 1);
        assert_eq!(wake.count(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_actor_drive_settles_the_exact_admission_receipt() {
        let backend = BackendProcessId::new_v7();
        let context = context(backend);
        let (mut logical, drive, _abort_intake) = logical_execution(context).await;
        let inner = Arc::new(CountingSink::default());
        let wake = Arc::new(CountingWake::default());
        let (sink, mut owner, observer) = ActorGatedTaskOperationSink::pair(
            Arc::clone(&inner) as Arc<dyn TaskOperationSink>,
            wake,
            NativeCompatibilityId::new([21; 32]),
        );
        let intent = admission(context, 21);
        let request = match &intent {
            OperationIntent::AcquireQueryContextAdmissionTicket(request) => *request,
            _ => unreachable!(),
        };
        let retained = match sink.try_submit(batch(backend, intent)) {
            TaskOperationSubmit::Backpressured(batch) => batch,
            other => panic!("actor gate must retain the first carrier: {other:?}"),
        };

        assert_eq!(owner.drive(&drive).await.expect("actor authorizes"), 1);
        assert!(matches!(
            sink.try_submit(retained),
            TaskOperationSubmit::Accepted
        ));
        observer
            .observe_acknowledgement(&OperationAcknowledgement::worker_receipt(
                request.envelope().operation_id(),
                OperationKind::AcquireQueryContextAdmissionTicket,
                novarocks_execution::task_execution::OperationOutcome::Accepted,
                AckPayload::AdmissionTicket(
                    novarocks_execution::task_execution::QueryContextAdmissionTicketReceipt::new(
                        novarocks_execution::task_execution::AdmissionTicketId::try_from_bytes(
                            [22; 16],
                        )
                        .unwrap(),
                        context,
                        request.valid_for(),
                    ),
                ),
            ))
            .expect("acknowledgement observed");
        assert_eq!(owner.drive(&drive).await.expect("actor settles"), 1);
        assert!(owner.admission_issues.is_empty());

        drop(drive);
        drop(owner);
        logical.abandon_running_attempt();
        while logical
            .stand_down_snapshot(context)
            .await
            .expect("stand-down remains observable")
            .is_none()
        {
            tokio::task::yield_now().await;
        }
        logical
            .observe_worker_process_replaced(context)
            .await
            .expect("replacement closes residual responsibility");
        logical
            .finish_until(std::time::Instant::now() + Duration::from_secs(2))
            .await
            .expect("logical test owner shuts down");
    }

    #[tokio::test]
    async fn cancelling_admission_settlement_retains_the_ack_and_blocks_shutdown() {
        let backend = BackendProcessId::new_v7();
        let context = context(backend);
        let (mut logical, drive, _abort_intake) = logical_execution(context).await;
        let inner = Arc::new(CountingSink::default());
        let wake = Arc::new(CountingWake::default());
        let (sink, mut owner, observer) = ActorGatedTaskOperationSink::pair(
            Arc::clone(&inner) as Arc<dyn TaskOperationSink>,
            wake,
            NativeCompatibilityId::new([23; 32]),
        );
        let intent = admission(context, 23);
        let request = match &intent {
            OperationIntent::AcquireQueryContextAdmissionTicket(request) => *request,
            _ => unreachable!(),
        };
        let retained = match sink.try_submit(batch(backend, intent)) {
            TaskOperationSubmit::Backpressured(batch) => batch,
            other => panic!("actor gate must retain the first carrier: {other:?}"),
        };
        assert_eq!(owner.drive(&drive).await.expect("actor authorizes"), 1);
        assert!(matches!(
            sink.try_submit(retained),
            TaskOperationSubmit::Accepted
        ));
        let acknowledgement = OperationAcknowledgement::worker_receipt(
            request.envelope().operation_id(),
            OperationKind::AcquireQueryContextAdmissionTicket,
            novarocks_execution::task_execution::OperationOutcome::Accepted,
            AckPayload::AdmissionTicket(
                novarocks_execution::task_execution::QueryContextAdmissionTicketReceipt::new(
                    novarocks_execution::task_execution::AdmissionTicketId::try_from_bytes(
                        [24; 16],
                    )
                    .unwrap(),
                    context,
                    request.valid_for(),
                ),
            ),
        );
        observer
            .observe_acknowledgement(&acknowledgement)
            .expect("acknowledgement observed");

        // Poll on this current-thread runtime without yielding to the actor.
        // The command enters the actor mailbox, its reply is pending, and then
        // cancellation drops this drive future at the exact vulnerable await.
        let mut cancelled = Box::pin(owner.drive(&drive));
        let waker = Waker::noop();
        let mut task_context = Context::from_waker(waker);
        assert_eq!(cancelled.as_mut().poll(&mut task_context), Poll::Pending);
        drop(cancelled);
        assert_eq!(owner.active_acknowledgement, Some(acknowledgement));
        assert!(
            !owner.prepare_convergence(),
            "cancelling a settlement future must retain the exact acknowledgement"
        );
        assert_eq!(
            owner.drive(&drive).await.expect("exact settlement retries"),
            1
        );
        assert!(owner.admission_issues.is_empty());

        drop(drive);
        drop(owner);
        logical.abandon_running_attempt();
        while logical
            .stand_down_snapshot(context)
            .await
            .expect("stand-down remains observable")
            .is_none()
        {
            tokio::task::yield_now().await;
        }
        logical
            .observe_worker_process_replaced(context)
            .await
            .expect("replacement closes residual responsibility");
        logical
            .finish_until(std::time::Instant::now() + Duration::from_secs(2))
            .await
            .expect("logical test owner shuts down");
    }

    #[test]
    fn authorized_batch_reenters_with_the_exact_carrier_before_transport_acceptance() {
        let inner = Arc::new(CountingSink::default());
        let wake = Arc::new(CountingWake::default());
        let (sink, owner, _observer) = ActorGatedTaskOperationSink::pair(
            Arc::clone(&inner) as Arc<dyn TaskOperationSink>,
            wake,
            NativeCompatibilityId::new([10; 32]),
        );
        let backend = BackendProcessId::new_v7();
        let context = context(backend);
        let operation = admission(context, 10);
        let operation_id = operation.operation_id();
        let original = batch(backend, operation);
        let retained = match sink.try_submit(original) {
            TaskOperationSubmit::Backpressured(batch) => batch,
            other => panic!("first gated submission must backpressure: {other:?}"),
        };
        let requested = owner
            .shared
            .take_authorization_request()
            .expect("the immutable intent is offered to the async owner");
        assert_eq!(requested.len(), 1);
        assert_eq!(requested[0].operation_id(), operation_id);
        owner.shared.finish_authorization(&requested, &Ok(()));

        assert!(matches!(
            sink.try_submit(retained),
            TaskOperationSubmit::Accepted
        ));
        assert_eq!(inner.submitted.load(Ordering::SeqCst), 1);
        assert_eq!(owner.pending_authorizations(), 0);
    }

    #[test]
    fn permanent_authorization_failure_rejects_exact_unsent_carrier_once() {
        let inner = Arc::new(CountingSink::default());
        let wake = Arc::new(CountingWake::default());
        let (sink, mut owner, _observer) = ActorGatedTaskOperationSink::pair(
            Arc::clone(&inner) as Arc<dyn TaskOperationSink>,
            wake,
            NativeCompatibilityId::new([11; 32]),
        );
        let backend = BackendProcessId::new_v7();
        let operation = admission(context(backend), 11);
        let operation_id = operation.operation_id();
        let retained = match sink.try_submit(batch(backend, operation)) {
            TaskOperationSubmit::Backpressured(batch) => batch,
            other => panic!("first gated submission must backpressure: {other:?}"),
        };
        let requested = owner.shared.take_authorization_request().unwrap();
        owner.shared.finish_authorization(
            &requested,
            &Err(TaskExecutionError::Schedule(
                "actor permanently refused the exact issue".to_owned(),
            )),
        );

        match sink.try_submit(retained) {
            TaskOperationSubmit::Rejected { batch, reason } => {
                assert_eq!(batch.operations()[0].operation_id(), operation_id);
                assert!(reason.contains("permanently refused"));
            }
            other => panic!("permanent failure must reject the unsent carrier: {other:?}"),
        }
        assert_eq!(inner.submitted.load(Ordering::SeqCst), 0);
        assert!(owner.prepare_convergence());
    }

    #[test]
    fn closing_rejects_authorized_carrier_held_by_permanent_inner_backpressure() {
        let inner = Arc::new(AlwaysBackpressuredSink);
        let wake = Arc::new(CountingWake::default());
        let (sink, mut owner, _observer) = ActorGatedTaskOperationSink::pair(
            inner as Arc<dyn TaskOperationSink>,
            wake,
            NativeCompatibilityId::new([14; 32]),
        );
        let backend = BackendProcessId::new_v7();
        let operation = admission(context(backend), 14);
        let operation_id = operation.operation_id();
        let retained = match sink.try_submit(batch(backend, operation)) {
            TaskOperationSubmit::Backpressured(batch) => batch,
            other => panic!("first gated submission must backpressure: {other:?}"),
        };
        let requested = owner.shared.take_authorization_request().unwrap();
        owner.shared.finish_authorization(&requested, &Ok(()));
        let retained = match sink.try_submit(retained) {
            TaskOperationSubmit::Backpressured(batch) => batch,
            other => panic!("raw Native backpressure must retain the carrier: {other:?}"),
        };

        assert!(!owner.prepare_convergence());
        match sink.try_submit(retained) {
            TaskOperationSubmit::Rejected { batch, reason } => {
                assert_eq!(batch.operations()[0].operation_id(), operation_id);
                assert!(reason.contains("closing"));
            }
            other => panic!("closing must reject the unsent carrier: {other:?}"),
        }
        assert!(owner.prepare_convergence());
    }

    #[test]
    fn multiple_actor_gated_intents_are_rejected_before_authorization() {
        let inner = Arc::new(CountingSink::default());
        let wake = Arc::new(CountingWake::default());
        let (sink, owner, _observer) = ActorGatedTaskOperationSink::pair(
            Arc::clone(&inner) as Arc<dyn TaskOperationSink>,
            wake,
            NativeCompatibilityId::new([12; 32]),
        );
        let backend = BackendProcessId::new_v7();
        let context = context(backend);
        let operations = vec![admission(context, 12), admission(context, 12)];
        let queued_bytes = operations.iter().map(OperationIntent::queued_bytes).sum();
        let carrier =
            DispatchBatch::test_fixture(backend, DispatchLane::Lifecycle, operations, queued_bytes);

        assert!(matches!(
            sink.try_submit(carrier),
            TaskOperationSubmit::Rejected { .. }
        ));
        assert_eq!(owner.pending_authorizations(), 0);
        assert_eq!(inner.submitted.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn terminal_ack_drain_settles_later_verdict_after_first_error() {
        let backend = BackendProcessId::new_v7();
        let context = context(backend);
        let (mut logical, drive, _abort_intake) = logical_execution(context).await;
        let inner = Arc::new(CountingSink::default());
        let wake = Arc::new(CountingWake::default());
        let (_sink, mut owner, observer) = ActorGatedTaskOperationSink::pair(
            inner as Arc<dyn TaskOperationSink>,
            wake,
            NativeCompatibilityId::new([13; 32]),
        );
        let OperationIntent::AcquireQueryContextAdmissionTicket(request) = admission(context, 13)
        else {
            unreachable!()
        };
        let issue = drive.begin_admission_issue(request).await.unwrap();
        let invalid_operation = TaskOperationId::new_v7();
        owner.admission_issues.insert(invalid_operation, issue);
        owner
            .admission_issues
            .insert(request.envelope().operation_id(), issue);
        let first = OperationAcknowledgement::worker_receipt(
            invalid_operation,
            OperationKind::AcquireQueryContextAdmissionTicket,
            OperationOutcome::InvalidStateOrRequest,
            AckPayload::None,
        );
        let second = OperationAcknowledgement::worker_receipt(
            request.envelope().operation_id(),
            OperationKind::AcquireQueryContextAdmissionTicket,
            OperationOutcome::Accepted,
            AckPayload::AdmissionTicket(QueryContextAdmissionTicketReceipt::new(
                AdmissionTicketId::try_from_bytes([13; 16]).unwrap(),
                context,
                request.valid_for(),
            )),
        );
        observer.observe_acknowledgement(&first).unwrap();
        observer.observe_acknowledgement(&second).unwrap();

        let result = owner.settle_terminal_acknowledgements(&drive).await;
        assert!(
            result.is_err(),
            "the first settlement must fail: {result:?}"
        );
        assert_eq!(owner.shared.take_acknowledgement(), None);
        assert_eq!(owner.active_acknowledgement, None);
        assert!(
            !owner
                .admission_issues
                .contains_key(&request.envelope().operation_id())
        );
        assert!(owner.prepare_convergence());

        drop(drive);
        drop(owner);
        logical.abandon_running_attempt();
        while logical
            .stand_down_snapshot(context)
            .await
            .unwrap()
            .is_none()
        {
            tokio::task::yield_now().await;
        }
        logical
            .observe_worker_process_replaced(context)
            .await
            .unwrap();
        logical
            .finish_until(std::time::Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn terminal_ack_drain_does_not_authorize_queued_admission() {
        let backend = BackendProcessId::new_v7();
        let context = context(backend);
        let (mut logical, drive, _abort_intake) = logical_execution(context).await;
        let inner = Arc::new(CountingSink::default());
        let wake = Arc::new(CountingWake::default());
        let (sink, mut owner, _observer) = ActorGatedTaskOperationSink::pair(
            inner as Arc<dyn TaskOperationSink>,
            wake,
            NativeCompatibilityId::new([14; 32]),
        );
        let retained = match sink.try_submit(batch(backend, admission(context, 14))) {
            TaskOperationSubmit::Backpressured(batch) => batch,
            other => panic!("admission awaits actor authorization: {other:?}"),
        };
        assert_eq!(owner.pending_authorizations(), 1);

        // This is the gate path ManifestAssembledRound takes after turn() fails.
        assert_eq!(
            owner
                .settle_terminal_acknowledgements(&drive)
                .await
                .unwrap(),
            0
        );
        assert!(owner.admission_issues.is_empty());
        assert_eq!(owner.pending_authorizations(), 1);
        assert!(!owner.prepare_convergence());
        assert!(matches!(
            sink.try_submit(retained),
            TaskOperationSubmit::Rejected { .. }
        ));
        assert!(owner.prepare_convergence());

        drop(drive);
        drop(owner);
        logical.abandon_running_attempt();
        while logical
            .stand_down_snapshot(context)
            .await
            .unwrap()
            .is_none()
        {
            tokio::task::yield_now().await;
        }
        logical
            .observe_worker_process_replaced(context)
            .await
            .unwrap();
        logical
            .finish_until(std::time::Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    }
}
