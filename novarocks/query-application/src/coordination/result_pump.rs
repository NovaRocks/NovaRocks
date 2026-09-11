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

//! Actor-owned root-result pumping.
//!
//! Role adapters supply opaque, preflighted packets. This module owns the one
//! ordered path from governed fetch capacity through decode and protocol
//! delivery. It deliberately has no Native codec or execution-kernel
//! dependency.

use std::{future::Future, num::NonZeroUsize, pin::Pin, sync::Arc, time::Instant};

use arrow::record_batch::RecordBatch;
use novarocks_execution_contract::{
    AbortCause, MaxWait, QueryContextRef, ResultByteLimit, ResultPacketSequence,
    TaskFailureCategory, TaskIdentity, TaskState, TaskStatus, TerminationDetail,
};
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::{
    CancellationReason, CancellationView, LocalResourceAuthority, ResultCredit, WorkError,
    WorkScope,
};
use tokio::sync::{oneshot, watch};

use crate::api::NativeAttemptTerminal;
use crate::api::{QueryExecutionError, QueryExecutionErrorKind, ResultSchema};

use super::result_decode::{
    BoundedResultDecodeHandle, BoundedResultDecodeOwner, ResultDecodeExecutorConfig,
    ResultDecodeJob, ResultDecodeShutdownError, ResultDecodeWorkerError,
};
use super::{
    AttemptFailureClass, LogicalConclusion, LogicalExecutionActor, LogicalExecutionActorError,
    ReplacementQualification, RootResultObserver, RootTerminalFailure, RunningAttemptHandoffError,
    RunningAttemptPermit,
};
use crate::api::DecodedResultBatch;

/// Unique process-lifetime owner of the synchronous Arrow decode workers.
///
/// Frontend composition retains this value and gives query pumps only cloned
/// [`RootResultDecodeRuntime`] handles. Explicit shutdown is blocking and must
/// run on the process blocking-shutdown path, never on a Tokio coordinator
/// worker.
#[doc(hidden)]
pub struct RootResultDecodeRuntimeOwner {
    owner: BoundedResultDecodeOwner<Result<DecodedRootResult, RootResultFetchFailure>>,
    runtime: RootResultDecodeRuntime,
}

impl RootResultDecodeRuntimeOwner {
    pub fn try_new(
        worker_threads: NonZeroUsize,
        queue_capacity: NonZeroUsize,
    ) -> Result<Self, QueryExecutionError> {
        let owner = BoundedResultDecodeOwner::try_new(ResultDecodeExecutorConfig::new(
            worker_threads,
            queue_capacity,
        ))
        .map_err(|error| {
            QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                format!("open result decode runtime failed: {error}"),
            )
        })?;
        let runtime = RootResultDecodeRuntime {
            handle: owner.handle(),
        };
        Ok(Self { owner, runtime })
    }

    pub fn runtime(&self) -> RootResultDecodeRuntime {
        self.runtime.clone()
    }

    /// Closes decode admission without waiting when the process owner has
    /// committed to exit after bounded graceful shutdown failed.
    pub fn request_shutdown_for_process_exit(&self) {
        self.owner.request_close();
    }

    pub fn shutdown_and_join_blocking(self) -> Result<(), QueryExecutionError> {
        let Self { owner, runtime } = self;
        drop(runtime);
        owner.shutdown_and_join().map_err(|error| {
            QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                format!("shut down result decode runtime failed: {error}"),
            )
        })
    }

    /// Closes decode admission and awaits the exact worker set without moving
    /// this process owner into a detached blocking task.
    ///
    /// Deadline or cancellation leaves this owner intact, so role composition
    /// can retry the same shutdown after in-flight decode work converges.
    pub async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), QueryExecutionError> {
        self.owner.shutdown_until(deadline).await.map_err(|error| {
            let kind = if matches!(error, ResultDecodeShutdownError::DeadlineExceeded { .. }) {
                QueryExecutionErrorKind::DeadlineExceeded
            } else {
                QueryExecutionErrorKind::Failed
            };
            QueryExecutionError::new(
                kind,
                format!("shut down result decode runtime failed: {error}"),
            )
        })
    }
}

/// Cloneable submission handle for the process-owned decode workers.
///
/// Dropping this handle never closes or joins the process executor.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct RootResultDecodeRuntime {
    handle: BoundedResultDecodeHandle<Result<DecodedRootResult, RootResultFetchFailure>>,
}

/// Metadata-only bounds established before an adapter may return a raw packet.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RootResultDecodeBounds {
    decode_operation_upper_bound: u64,
    retained_backing_upper_bound: u64,
}

impl RootResultDecodeBounds {
    pub fn new(
        decode_operation_upper_bound: u64,
        retained_backing_upper_bound: u64,
    ) -> Result<Self, QueryExecutionError> {
        if decode_operation_upper_bound == 0 {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "root result decode operation bound must be nonzero",
            ));
        }
        Ok(Self {
            decode_operation_upper_bound,
            retained_backing_upper_bound,
        })
    }

    pub const fn decode_operation_upper_bound(self) -> u64 {
        self.decode_operation_upper_bound
    }

    pub const fn retained_backing_upper_bound(self) -> u64 {
        self.retained_backing_upper_bound
    }
}

type DecodePacket = Box<dyn FnOnce() -> Result<RecordBatch, QueryExecutionError> + Send + 'static>;

/// Move-only packet whose adapter already validated wire shape and bounds.
#[doc(hidden)]
pub struct PreflightedRootResultPacket {
    sequence: ResultPacketSequence,
    payload_bytes: u64,
    bounds: RootResultDecodeBounds,
    decode: Option<DecodePacket>,
}

impl PreflightedRootResultPacket {
    pub fn new(
        sequence: ResultPacketSequence,
        payload_bytes: u64,
        bounds: RootResultDecodeBounds,
        decode: impl FnOnce() -> Result<RecordBatch, QueryExecutionError> + Send + 'static,
    ) -> Result<Self, QueryExecutionError> {
        if payload_bytes == 0 {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "root result packet payload must be nonzero",
            ));
        }
        Ok(Self {
            sequence,
            payload_bytes,
            bounds,
            decode: Some(Box::new(decode)),
        })
    }

    pub const fn sequence(&self) -> ResultPacketSequence {
        self.sequence
    }

    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    pub const fn bounds(&self) -> RootResultDecodeBounds {
        self.bounds
    }

    fn decode(mut self) -> Result<RecordBatch, QueryExecutionError> {
        self.decode
            .take()
            .expect("move-only root result packet decodes at most once")()
    }
}

/// One typed answer from the exact root Task.
#[doc(hidden)]
pub enum RootResultFetchOutcome {
    Ready(PreflightedRootResultPacket),
    NotReady,
    EndPending(ResultPacketSequence),
    EndAcknowledged(ResultPacketSequence),
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RootResultFetchRequest {
    pub root: TaskIdentity,
    pub max_wait: MaxWait,
    pub acknowledged: Option<ResultPacketSequence>,
    pub max_result_bytes: ResultByteLimit,
}

type FetchFuture = Pin<
    Box<
        dyn Future<Output = Result<RootResultFetchOutcome, RootResultFetchFailure>>
            + Send
            + 'static,
    >,
>;
type FetchFn = dyn Fn(RootResultFetchRequest) -> FetchFuture + Send + Sync + 'static;

/// Move-only role binding. Only the actor-owned pump can drive its ordered
/// fetch sequence; adapters cannot obtain delivery or ACK authority from it.
#[doc(hidden)]
pub struct RootResultPumpBinding {
    fetch: Box<FetchFn>,
    decode: RootResultDecodeRuntime,
}

pub(crate) struct NativeAttemptTerminalSender {
    sender: Option<oneshot::Sender<NativeAttemptTerminal>>,
}

pub(crate) struct NativeAttemptTerminalSource {
    receiver: Option<oneshot::Receiver<NativeAttemptTerminal>>,
}

pub(crate) fn native_attempt_terminal_channel()
-> (NativeAttemptTerminalSender, NativeAttemptTerminalSource) {
    let (sender, receiver) = oneshot::channel();
    (
        NativeAttemptTerminalSender {
            sender: Some(sender),
        },
        NativeAttemptTerminalSource {
            receiver: Some(receiver),
        },
    )
}

impl NativeAttemptTerminalSender {
    pub(crate) fn publish(mut self, terminal: NativeAttemptTerminal) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(terminal);
        }
    }
}

impl NativeAttemptTerminalSource {
    async fn next(&mut self) -> Result<NativeAttemptTerminal, QueryExecutionError> {
        let receiver = self
            .receiver
            .as_mut()
            .ok_or_else(|| contract_error("Native attempt terminal was consumed more than once"))?;
        let terminal = receiver.await.map_err(|_| {
            contract_error("Native attempt terminal owner dropped without a verdict")
        })?;
        self.receiver = None;
        Ok(terminal)
    }

    fn try_next(&mut self) -> Result<Option<NativeAttemptTerminal>, QueryExecutionError> {
        let receiver = self
            .receiver
            .as_mut()
            .ok_or_else(|| contract_error("Native attempt terminal was consumed more than once"))?;
        match receiver.try_recv() {
            Ok(terminal) => {
                self.receiver = None;
                Ok(Some(terminal))
            }
            Err(oneshot::error::TryRecvError::Empty) => Ok(None),
            Err(oneshot::error::TryRecvError::Closed) => Err(contract_error(
                "Native attempt terminal owner dropped without a verdict",
            )),
        }
    }
}

impl RootResultPumpBinding {
    pub fn new<F, Fut>(decode: RootResultDecodeRuntime, fetch: F) -> Self
    where
        F: Fn(RootResultFetchRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RootResultFetchOutcome, RootResultFetchFailure>>
            + Send
            + 'static,
    {
        Self {
            fetch: Box::new(move |request| Box::pin(fetch(request))),
            decode,
        }
    }

    fn fetch(&self, request: RootResultFetchRequest) -> FetchFuture {
        (self.fetch)(request)
    }
}

/// Typed adapter failure and its attempt-level recovery class.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootResultFetchFailure {
    class: AttemptFailureClass,
    error: QueryExecutionError,
}

impl RootResultFetchFailure {
    pub fn new(class: AttemptFailureClass, error: QueryExecutionError) -> Self {
        Self { class, error }
    }

    pub const fn class(&self) -> AttemptFailureClass {
        self.class
    }

    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }
}

/// Raw payload and its exact retained/decode credit move as one unit. Drop
/// order is part of the memory-accounting contract: payload first, credit
/// second.
struct RootResultDecodeInput {
    packet: Option<PreflightedRootResultPacket>,
    credit: Option<ResultCredit>,
}

impl RootResultDecodeInput {
    fn new(packet: PreflightedRootResultPacket, credit: ResultCredit) -> Self {
        Self {
            packet: Some(packet),
            credit: Some(credit),
        }
    }

    fn credit_mut(&mut self) -> &mut ResultCredit {
        self.credit
            .as_mut()
            .expect("decode input owns credit until execution")
    }

    fn into_job(
        self,
        schema: ResultSchema,
    ) -> ResultDecodeJob<Result<DecodedRootResult, RootResultFetchFailure>> {
        ResultDecodeJob::new(move || self.decode(schema))
    }

    fn decode(mut self, schema: ResultSchema) -> Result<DecodedRootResult, RootResultFetchFailure> {
        let packet = self
            .packet
            .take()
            .expect("decode input owns one raw packet");
        let bounds = packet.bounds();
        let batch = packet.decode().map_err(|error| {
            RootResultFetchFailure::new(AttemptFailureClass::ContractViolation, error)
        })?;
        let decoded = DecodedResultBatch::try_new(batch).map_err(|error| {
            RootResultFetchFailure::new(AttemptFailureClass::ContractViolation, error)
        })?;
        if !schema.accepts(decoded.batch())
            || decoded.unique_backing_bytes() > bounds.retained_backing_upper_bound()
            || decoded.governance_charge_bytes() > bounds.decode_operation_upper_bound()
        {
            drop(decoded);
            return Err(contract_failure(contract_error(
                "decoded result exceeded its metadata-preflighted memory bound",
            )));
        }
        let credit = self
            .credit
            .take()
            .expect("decode input owns credit through decode completion");
        Ok(DecodedRootResult {
            decoded: Some(decoded),
            credit: Some(credit),
        })
    }
}

impl Drop for RootResultDecodeInput {
    fn drop(&mut self) {
        drop(self.packet.take());
        drop(self.credit.take());
    }
}

struct DecodedRootResult {
    decoded: Option<DecodedResultBatch>,
    credit: Option<ResultCredit>,
}

impl DecodedRootResult {
    fn into_parts(mut self) -> (DecodedResultBatch, ResultCredit) {
        let decoded = self.decoded.take().expect("decoded result owns one batch");
        let credit = self
            .credit
            .take()
            .expect("decoded result owns one credit token");
        (decoded, credit)
    }
}

impl Drop for DecodedRootResult {
    fn drop(&mut self) {
        drop(self.decoded.take());
        drop(self.credit.take());
    }
}

/// Single accepted-status projection. Frontend publishes the complete
/// snapshot in the same serial turn that its TaskRound accepts it.
#[doc(hidden)]
pub struct AcceptedRootStatusSender {
    root: TaskIdentity,
    sender: watch::Sender<Option<AcceptedRootProjection>>,
}

#[doc(hidden)]
pub struct AcceptedRootStatusSource {
    root: TaskIdentity,
    receiver: watch::Receiver<Option<AcceptedRootProjection>>,
    observed: Option<AcceptedRootProjection>,
    success_seal_port: Option<Arc<dyn AcceptedRootSuccessSealPort>>,
}

/// Frontend's single serialized path for ordering success against accepted
/// Task status. Implementations enqueue the request beside status events and
/// must not decide it on the caller's async task.
#[doc(hidden)]
pub trait AcceptedRootSuccessSealPort: std::fmt::Debug + Send + Sync {
    fn enqueue_success_seal(
        &self,
        request: AcceptedRootSuccessSealRequest,
    ) -> Result<(), AcceptedRootSuccessSealRequest>;
}

/// Move-only request whose reply proves that the serialized status owner
/// consumed its only publisher at one precise point in the status order.
#[doc(hidden)]
#[derive(Debug)]
pub struct AcceptedRootSuccessSealRequest {
    reply: oneshot::Sender<Result<(), QueryExecutionError>>,
}

impl AcceptedRootSuccessSealRequest {
    pub fn accept(self, sender: AcceptedRootStatusSender) -> Result<(), QueryExecutionError> {
        let result = sender.seal_success();
        let _ = self.reply.send(result.clone());
        result
    }

    pub fn reject(self, error: QueryExecutionError) {
        let _ = self.reply.send(Err(error));
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AcceptedRootProjection {
    Observation(AcceptedRootObservation),
    SuccessSealed(TaskStatus),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AcceptedRootObservation {
    status: TaskStatus,
    attempt_failure: AcceptedAttemptFailure,
}

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcceptedAttemptFailure {
    None,
    DerivedPending,
    Authoritative(TerminationDetail),
}

#[cfg(test)]
fn accepted_root_status_projection(
    root: TaskIdentity,
) -> (AcceptedRootStatusSender, AcceptedRootStatusSource) {
    accepted_root_status_projection_inner(root, None)
}

#[doc(hidden)]
pub fn accepted_root_status_projection_with_seal_port(
    root: TaskIdentity,
    success_seal_port: Arc<dyn AcceptedRootSuccessSealPort>,
) -> (AcceptedRootStatusSender, AcceptedRootStatusSource) {
    accepted_root_status_projection_inner(root, Some(success_seal_port))
}

fn accepted_root_status_projection_inner(
    root: TaskIdentity,
    success_seal_port: Option<Arc<dyn AcceptedRootSuccessSealPort>>,
) -> (AcceptedRootStatusSender, AcceptedRootStatusSource) {
    let (sender, receiver) = watch::channel(None);
    (
        AcceptedRootStatusSender { root, sender },
        AcceptedRootStatusSource {
            root,
            receiver,
            observed: None,
            success_seal_port,
        },
    )
}

impl AcceptedRootStatusSender {
    #[cfg(test)]
    fn publish(&self, status: TaskStatus) -> Result<(), QueryExecutionError> {
        let attempt_failure = if status
            .termination()
            .is_some_and(|detail| !detail.is_success_compatible())
        {
            AcceptedAttemptFailure::DerivedPending
        } else {
            AcceptedAttemptFailure::None
        };
        self.publish_attempt_observation(status, attempt_failure)
    }

    /// Atomically publishes the exact root status and the serialized
    /// attempt-level failure latch observed in the same owner turn.
    pub fn publish_attempt_observation(
        &self,
        status: TaskStatus,
        attempt_failure: AcceptedAttemptFailure,
    ) -> Result<(), QueryExecutionError> {
        if let AcceptedAttemptFailure::Authoritative(authoritative) = &attempt_failure
            && (authoritative.is_derived() || authoritative.is_success_compatible())
        {
            return Err(contract_error(
                "authoritative attempt failure must be a non-derived failure cause",
            ));
        }
        self.publish_observation(AcceptedRootObservation {
            status,
            attempt_failure,
        })
    }

    /// Publishes the current root status together with the non-derived cause
    /// selected by the same serialized attempt status owner.
    ///
    /// The authoritative attempt failure may originate from another Task, so
    /// the root itself need not be terminal or failed.
    pub fn publish_with_attempt_failure(
        &self,
        status: TaskStatus,
        authoritative_attempt_failure: TerminationDetail,
    ) -> Result<(), QueryExecutionError> {
        self.publish_attempt_observation(
            status,
            AcceptedAttemptFailure::Authoritative(authoritative_attempt_failure),
        )
    }

    fn publish_observation(
        &self,
        observation: AcceptedRootObservation,
    ) -> Result<(), QueryExecutionError> {
        let status = &observation.status;
        if status.identity() != self.root {
            return Err(contract_error("accepted root status names another Task"));
        }
        if let Some(AcceptedRootProjection::Observation(held)) = self.sender.borrow().as_ref() {
            let cause_refinement = held.status == observation.status
                && matches!(
                    (&held.attempt_failure, &observation.attempt_failure),
                    (
                        AcceptedAttemptFailure::None,
                        AcceptedAttemptFailure::DerivedPending
                            | AcceptedAttemptFailure::Authoritative(_)
                    ) | (
                        AcceptedAttemptFailure::DerivedPending,
                        AcceptedAttemptFailure::Authoritative(_)
                    )
                );
            if status.version() < held.status.version()
                || (status.version() == held.status.version()
                    && observation != *held
                    && !cause_refinement)
                || (held.status.is_terminal() && observation.status != held.status)
            {
                return Err(contract_error(
                    "accepted root status regressed or overwrote a published version",
                ));
            }
            if observation == *held {
                return Ok(());
            }
        } else if self.sender.borrow().is_some() {
            return Err(contract_error(
                "accepted root status cannot be published after success was sealed",
            ));
        }
        self.sender
            .send_replace(Some(AcceptedRootProjection::Observation(observation)));
        Ok(())
    }

    /// Consumes the only publisher and seals that the serialized attempt
    /// owner proved success. The carried Finished snapshot lets a watch
    /// receiver observe status and seal atomically even if it skipped the
    /// immediately preceding observation.
    fn seal_success(self) -> Result<(), QueryExecutionError> {
        let status = match self.sender.borrow().as_ref() {
            Some(AcceptedRootProjection::Observation(observation))
                if observation.status.state() == TaskState::Finished
                    && matches!(observation.attempt_failure, AcceptedAttemptFailure::None) =>
            {
                observation.status.clone()
            }
            _ => {
                return Err(contract_error(
                    "success seal requires a Finished root and no attempt failure",
                ));
            }
        };
        self.sender
            .send_replace(Some(AcceptedRootProjection::SuccessSealed(status)));
        Ok(())
    }
}

impl AcceptedRootStatusSource {
    async fn next(&mut self) -> Result<AcceptedRootProjection, QueryExecutionError> {
        loop {
            let current = self.receiver.borrow_and_update().clone();
            if let Some(status) = current
                && self.observed.as_ref() != Some(&status)
            {
                self.observed = Some(status.clone());
                return Ok(status);
            }
            self.receiver.changed().await.map_err(|_| {
                contract_error(
                    "accepted root status source closed without an explicit success seal",
                )
            })?;
        }
    }

    fn begin_success_seal_request(
        &self,
    ) -> Result<oneshot::Receiver<Result<(), QueryExecutionError>>, QueryExecutionError> {
        let Some(port) = self.success_seal_port.as_ref() else {
            return Err(contract_error(
                "accepted root status source has no serialized success-seal port",
            ));
        };
        let (reply, outcome) = oneshot::channel();
        port.enqueue_success_seal(AcceptedRootSuccessSealRequest { reply })
            .map_err(|_| contract_error("success-seal request intake is closed"))?;
        Ok(outcome)
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub enum ResultPumpFailure {
    DecisionPending(ResultPumpDecision),
    Concluded(ConcludedResultPumpFailure),
    ActorOutcomeUnknown(ActorOutcomeUnknownResultPumpFailure),
}

/// An attempt failure whose final retry-or-fail decision still belongs to the
/// running permit owner.
#[doc(hidden)]
#[derive(Debug)]
pub struct ResultPumpDecision {
    permit: RunningAttemptPermit,
    observer: Option<RootResultObserver>,
    class: AttemptFailureClass,
    error: QueryExecutionError,
}

impl ResultPumpDecision {
    pub const fn class(&self) -> AttemptFailureClass {
        self.class
    }

    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }

    /// Installs the final typed failure before releasing the last root
    /// observer, so observer loss cannot race and replace the chosen result.
    pub async fn fail_logical(
        self,
        actor: &LogicalExecutionActor,
    ) -> Result<LogicalConclusion, LogicalExecutionActorError> {
        let permit = self.permit;
        let result = actor
            .fail_attempt_with_error(permit, self.error.clone())
            .await;
        drop(self.observer);
        result
    }

    /// Transfers the retained attempt authority into replacement while the
    /// root observer remains alive through the actor's decision turn.
    pub async fn begin_replacement(
        self,
        actor: &LogicalExecutionActor,
        replacement: QueryExecutionId,
        replacement_contexts: Vec<QueryContextRef>,
    ) -> Result<(ReplacementQualification, QueryExecutionError), LogicalExecutionActorError> {
        let permit = self.permit;
        let qualification = actor
            .begin_replacement_with_error(
                permit,
                self.class,
                self.error.clone(),
                replacement,
                replacement_contexts,
            )
            .await?;
        drop(self.observer);
        Ok((qualification, self.error))
    }
}

/// The actor already fixed the logical terminal state; no attempt authority is
/// returned and no caller can ask this value to begin replacement.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConcludedResultPumpFailure {
    conclusion: LogicalConclusion,
    class: AttemptFailureClass,
    error: QueryExecutionError,
}

impl ConcludedResultPumpFailure {
    pub const fn conclusion(&self) -> LogicalConclusion {
        self.conclusion
    }

    pub const fn class(&self) -> AttemptFailureClass {
        self.class
    }

    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }
}

/// The actor accepted the running-attempt authority, but no actor reply proved
/// which logical conclusion, if any, was fixed.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorOutcomeUnknownResultPumpFailure {
    error: QueryExecutionError,
}

impl ActorOutcomeUnknownResultPumpFailure {
    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }
}

enum PumpInterruption {
    Decision(RootResultFetchFailure),
    Cancellation(RootResultFetchFailure),
    Concluded(LogicalConclusion, RootResultFetchFailure),
}

struct PumpRuntime {
    observer: RootResultObserver,
    statuses: AcceptedRootStatusSource,
    native_terminal: NativeAttemptTerminalSource,
    cancellation: CancellationView,
    root_finished: bool,
    success_sealed: bool,
    native_completed: bool,
    pending_root_failure: Option<TaskStatus>,
}

impl PumpRuntime {
    fn observe_ready_native_terminal(&mut self) -> Result<(), PumpInterruption> {
        if self.native_completed {
            return Ok(());
        }
        if let Some(terminal) = self
            .native_terminal
            .try_next()
            .map_err(|error| PumpInterruption::Decision(contract_failure(error)))?
        {
            self.observe_native_terminal(Ok(terminal))?;
        }
        Ok(())
    }

    fn observe_native_terminal(
        &mut self,
        terminal: Result<NativeAttemptTerminal, QueryExecutionError>,
    ) -> Result<(), PumpInterruption> {
        match terminal {
            Ok(NativeAttemptTerminal::Completed) => {
                self.native_completed = true;
                Ok(())
            }
            Ok(NativeAttemptTerminal::Failed(failure)) => Err(PumpInterruption::Decision(
                RootResultFetchFailure::new(failure.class(), failure.error().clone()),
            )),
            Err(error) => Err(PumpInterruption::Decision(contract_failure(error))),
        }
    }

    async fn observe_projection(
        &mut self,
        projection: AcceptedRootProjection,
    ) -> Result<(), PumpInterruption> {
        match projection {
            AcceptedRootProjection::Observation(observation) => {
                self.observe_status(observation).await
            }
            AcceptedRootProjection::SuccessSealed(status) => {
                if self.pending_root_failure.is_some() || status.state() != TaskState::Finished {
                    return Err(PumpInterruption::Decision(contract_failure(
                        contract_error("invalid success seal for the accepted root status"),
                    )));
                }
                self.observe_status(AcceptedRootObservation {
                    status,
                    attempt_failure: AcceptedAttemptFailure::None,
                })
                .await?;
                if !self.root_finished {
                    return Err(PumpInterruption::Decision(contract_failure(
                        contract_error("success seal did not carry a Finished root status"),
                    )));
                }
                self.success_sealed = true;
                Ok(())
            }
        }
    }

    async fn observe_status(
        &mut self,
        observation: AcceptedRootObservation,
    ) -> Result<(), PumpInterruption> {
        let AcceptedRootObservation {
            status,
            attempt_failure,
        } = observation;
        if let Some(pending) = self.pending_root_failure.as_ref() {
            if pending != &status {
                return Err(PumpInterruption::Decision(contract_failure(
                    contract_error("authoritative attempt failure refined a different root status"),
                )));
            }
            let AcceptedAttemptFailure::Authoritative(authoritative) = attempt_failure else {
                return Ok(());
            };
            let failure =
                classify_root_termination(&status, Some(&authoritative)).ok_or_else(|| {
                    PumpInterruption::Decision(contract_failure(contract_error(
                        "authoritative attempt failure did not classify the pending root terminal",
                    )))
                })?;
            return match self
                .observer
                .observe_status_with_failure(
                    status,
                    RootTerminalFailure::Authoritative(failure.error.clone()),
                )
                .await
            {
                Err(LogicalExecutionActorError::RootAttemptTerminal) => {
                    Err(PumpInterruption::Decision(failure))
                }
                Err(LogicalExecutionActorError::ExecutionConcluded(conclusion)) => {
                    Err(PumpInterruption::Concluded(conclusion, failure))
                }
                Err(error) => Err(PumpInterruption::Decision(actor_failure(
                    "refine root Task failure",
                    error,
                ))),
                Ok(()) => Err(PumpInterruption::Decision(contract_failure(
                    contract_error(
                        "authoritative root failure refinement did not retain a terminal disposition",
                    ),
                ))),
            };
        }
        let finished = status.state() == TaskState::Finished;
        let terminal_failure = match &attempt_failure {
            AcceptedAttemptFailure::None => classify_root_termination(&status, None),
            AcceptedAttemptFailure::DerivedPending => None,
            AcceptedAttemptFailure::Authoritative(authoritative) => {
                classify_root_termination(&status, Some(authoritative))
            }
        };
        let terminal_actor_fact = match (&attempt_failure, terminal_failure.as_ref()) {
            (AcceptedAttemptFailure::DerivedPending, _) => RootTerminalFailure::Pending,
            (_, Some(failure)) => RootTerminalFailure::Authoritative(failure.error.clone()),
            _ => RootTerminalFailure::Unspecified,
        };
        match self
            .observer
            .observe_status_with_failure(status.clone(), terminal_actor_fact)
            .await
        {
            Ok(()) => {
                self.root_finished |= finished;
                Ok(())
            }
            Err(LogicalExecutionActorError::RootAttemptTerminal) if terminal_failure.is_some() => {
                Err(PumpInterruption::Decision(terminal_failure.expect(
                    "terminal failure was checked before actor observation",
                )))
            }
            Err(LogicalExecutionActorError::RootAttemptTerminal)
                if matches!(attempt_failure, AcceptedAttemptFailure::DerivedPending) =>
            {
                self.pending_root_failure = Some(status);
                Ok(())
            }
            Err(LogicalExecutionActorError::ExecutionConcluded(conclusion))
                if terminal_failure.is_some() =>
            {
                Err(PumpInterruption::Concluded(
                    conclusion,
                    terminal_failure
                        .expect("terminal failure was checked before actor observation"),
                ))
            }
            Err(error @ LogicalExecutionActorError::ExecutionConcluded(conclusion)) => {
                Err(PumpInterruption::Concluded(
                    conclusion,
                    actor_failure("observe root Task status", error),
                ))
            }
            Err(error) => Err(PumpInterruption::Decision(actor_failure(
                "observe root Task status",
                error,
            ))),
        }
    }

    /// Waits until the serialized Task owner has accepted at least one status
    /// for the exact root. The Frontend publishes this projection only after
    /// the root CreateTask acknowledgement, so crossing this gate proves that
    /// a result fetch cannot overtake root-task creation on its owning Worker.
    async fn await_initial_root_status(&mut self) -> Result<(), PumpInterruption> {
        loop {
            tokio::select! {
                biased;
                reason = self.cancellation.cancelled() => {
                    return Err(PumpInterruption::Cancellation(cancellation_failure(reason)));
                }
                terminal = self.native_terminal.next(), if !self.native_completed => {
                    self.observe_native_terminal(terminal)?;
                }
                status = self.statuses.next() => {
                    let projection = status.map_err(|error| PumpInterruption::Decision(contract_failure(error)))?;
                    self.observe_projection(projection).await?;
                    return Ok(());
                }
            }
        }
    }

    async fn await_step<F, T>(&mut self, future: F) -> Result<T, PumpInterruption>
    where
        F: Future<Output = T>,
    {
        tokio::pin!(future);
        loop {
            if self.pending_root_failure.is_some() {
                tokio::select! {
                    biased;
                    reason = self.cancellation.cancelled() => {
                        return Err(PumpInterruption::Cancellation(cancellation_failure(reason)));
                    }
                    terminal = self.native_terminal.next(), if !self.native_completed => {
                        self.observe_native_terminal(terminal)?;
                    }
                    status = self.statuses.next() => {
                        let projection = status.map_err(|error| PumpInterruption::Decision(contract_failure(error)))?;
                        self.observe_projection(projection).await?;
                    }
                }
                continue;
            }
            if self.success_sealed {
                return tokio::select! {
                    biased;
                    reason = self.cancellation.cancelled() => Err(PumpInterruption::Cancellation(cancellation_failure(reason))),
                    terminal = self.native_terminal.next(), if !self.native_completed => {
                        self.observe_native_terminal(terminal)?;
                        continue;
                    }
                    output = &mut future => {
                        self.observe_ready_native_terminal()?;
                        Ok(output)
                    },
                };
            }
            tokio::select! {
                biased;
                reason = self.cancellation.cancelled() => {
                    return Err(PumpInterruption::Cancellation(cancellation_failure(reason)));
                }
                terminal = self.native_terminal.next(), if !self.native_completed => {
                    self.observe_native_terminal(terminal)?;
                }
                status = self.statuses.next() => {
                    let projection = status.map_err(|error| PumpInterruption::Decision(contract_failure(error)))?;
                    self.observe_projection(projection).await?;
                }
                output = &mut future => {
                    self.observe_ready_native_terminal()?;
                    return Ok(output);
                },
            }
        }
    }

    async fn await_success_seal(&mut self) -> Result<(), PumpInterruption> {
        // The serialized Task owner publishes SuccessSealed only after the
        // exact root is Finished, no authoritative attempt failure exists,
        // and every required Task creation was accepted. That is the logical
        // result boundary. Native terminal convergence may still be waiting
        // for transport ownership or residual Worker cleanup and must remain
        // observable without delaying the actor-owned success EOF.
        while !self.success_sealed {
            tokio::select! {
                biased;
                reason = self.cancellation.cancelled() => {
                    return Err(PumpInterruption::Cancellation(cancellation_failure(reason)));
                }
                terminal = self.native_terminal.next(), if !self.native_completed => {
                    self.observe_native_terminal(terminal)?;
                }
                status = self.statuses.next() => {
                    let projection = status.map_err(|error| PumpInterruption::Decision(contract_failure(error)))?;
                    self.observe_projection(projection).await?;
                }
            }
        }
        Ok(())
    }
}

/// Runs the only raw-fetch/decode/delivery/ACK loop for one exact attempt.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_root_result_pump(
    permit: RunningAttemptPermit,
    root: TaskIdentity,
    scope: WorkScope,
    authority: LocalResourceAuthority,
    schema: ResultSchema,
    binding: RootResultPumpBinding,
    statuses: AcceptedRootStatusSource,
    native_terminal: NativeAttemptTerminalSource,
    max_wait: MaxWait,
    max_result_bytes: ResultByteLimit,
) -> Result<LogicalConclusion, ResultPumpFailure> {
    if statuses.root != root || permit.identity().execution() != root.query_execution_id() {
        return Err(pump_failure(
            permit,
            contract_failure(contract_error(
                "result pump root identity does not match its attempt or status source",
            )),
        ));
    }
    let cancellation = match scope.cancellation() {
        Ok(cancellation) => cancellation,
        Err(error) => {
            return Err(pump_failure(permit, work_failure(error)));
        }
    };
    if let Some(reason) = cancellation.reason() {
        return Err(conclude_cancellation(None, permit, cancellation_failure(reason)).await);
    }
    let observer = match permit.bind_root_result(root).await {
        Ok(observer) => observer,
        Err(error) => {
            if let Some(reason) = cancellation.reason() {
                return Err(
                    conclude_cancellation(None, permit, cancellation_failure(reason)).await,
                );
            }
            return Err(pump_actor_failure(permit, "bind root result", error));
        }
    };
    // Keep this owner until the attempt decision. A terminal relay result must
    // reach the outer supervisor before the last observer can disappear.
    let observer_owner = observer;
    let mut runtime = PumpRuntime {
        observer: observer_owner.clone(),
        statuses,
        native_terminal,
        cancellation,
        root_finished: false,
        success_sealed: false,
        native_completed: false,
        pending_root_failure: None,
    };
    let mut expected = ResultPacketSequence::new(0);
    let mut acknowledged = None;
    let mut pending_eos = None;

    if let Err(interruption) = runtime.await_initial_root_status().await {
        return Err(bound_pump_interruption(&observer_owner, permit, interruption).await);
    }

    loop {
        let credit = match runtime
            .await_step(
                authority.reserve_result_credit_when_available(&scope, max_result_bytes.get()),
            )
            .await
        {
            Ok(Ok(credit)) => credit,
            Ok(Err(error)) => {
                return Err(bound_pump_interruption(
                    &observer_owner,
                    permit,
                    work_interruption(error),
                )
                .await);
            }
            Err(interruption) => {
                return Err(bound_pump_interruption(&observer_owner, permit, interruption).await);
            }
        };
        let credit = match credit.begin_fetch() {
            Ok(credit) => credit,
            Err(error) => {
                return Err(bound_pump_interruption(
                    &observer_owner,
                    permit,
                    work_interruption(error),
                )
                .await);
            }
        };
        let request = RootResultFetchRequest {
            root,
            max_wait,
            acknowledged,
            max_result_bytes,
        };
        let outcome = match runtime.await_step(binding.fetch(request)).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => {
                drop(credit);
                return Err(bound_pump_failure(&observer_owner, permit, error).await);
            }
            Err(interruption) => {
                drop(credit);
                return Err(bound_pump_interruption(&observer_owner, permit, interruption).await);
            }
        };

        match outcome {
            RootResultFetchOutcome::NotReady if pending_eos.is_none() => {
                drop(credit);
            }
            RootResultFetchOutcome::Ready(packet) if pending_eos.is_none() => {
                if packet.sequence() != expected {
                    drop(packet);
                    drop(credit);
                    return Err(bound_pump_failure(
                        &observer_owner,
                        permit,
                        contract_failure(contract_error(
                            "root result packet sequence is not the next expected sequence",
                        )),
                    )
                    .await);
                }
                let bounds = packet.bounds();
                let payload_bytes = packet.payload_bytes();
                if payload_bytes > max_result_bytes.get() {
                    drop(packet);
                    drop(credit);
                    return Err(bound_pump_failure(
                        &observer_owner,
                        permit,
                        contract_failure(contract_error(
                            "root result packet exceeded the requested byte limit",
                        )),
                    )
                    .await);
                }
                let credit = match credit.retain_raw(payload_bytes) {
                    Ok(credit) => credit,
                    Err(error) => {
                        let (error, credit) = error.into_parts();
                        drop(packet);
                        drop(credit);
                        return Err(bound_pump_interruption(
                            &observer_owner,
                            permit,
                            work_interruption(error),
                        )
                        .await);
                    }
                };
                let mut decode_input = RootResultDecodeInput::new(packet, credit);
                match runtime
                    .await_step(
                        decode_input
                            .credit_mut()
                            .reserve_decode_when_available_in_place(
                                &authority,
                                bounds.decode_operation_upper_bound(),
                            ),
                    )
                    .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        drop(decode_input);
                        return Err(bound_pump_interruption(
                            &observer_owner,
                            permit,
                            work_interruption(error),
                        )
                        .await);
                    }
                    Err(interruption) => {
                        drop(decode_input);
                        return Err(
                            bound_pump_interruption(&observer_owner, permit, interruption).await,
                        );
                    }
                }
                let job = decode_input.into_job(schema.clone());
                let receipt = match runtime.await_step(binding.decode.handle.submit(job)).await {
                    Ok(Ok(receipt)) => receipt,
                    Ok(Err(error)) => {
                        drop(error.into_job());
                        return Err(bound_pump_failure(
                            &observer_owner,
                            permit,
                            decode_supervisor_failure(ResultDecodeWorkerError::ExecutorClosed),
                        )
                        .await);
                    }
                    Err(interruption) => {
                        return Err(
                            bound_pump_interruption(&observer_owner, permit, interruption).await,
                        );
                    }
                };
                let decoded = match runtime.await_step(receipt.complete()).await {
                    Ok(Ok(Ok(decoded))) => decoded,
                    Ok(Ok(Err(failure))) => {
                        return Err(bound_pump_failure(&observer_owner, permit, failure).await);
                    }
                    Ok(Err(error)) => {
                        return Err(bound_pump_failure(
                            &observer_owner,
                            permit,
                            decode_supervisor_failure(error),
                        )
                        .await);
                    }
                    Err(interruption) => {
                        return Err(
                            bound_pump_interruption(&observer_owner, permit, interruption).await,
                        );
                    }
                };
                let (decoded, credit) = decoded.into_parts();
                let charge = decoded.governance_charge_bytes();
                let credit = match credit.queue_decoded(charge) {
                    Ok(credit) => credit,
                    Err(error) => {
                        let (error, credit) = error.into_parts();
                        drop(decoded);
                        drop(credit);
                        return Err(bound_pump_interruption(
                            &observer_owner,
                            permit,
                            work_interruption(error),
                        )
                        .await);
                    }
                };
                match runtime
                    .await_step(permit.deliver_result_batch(expected, decoded, credit))
                    .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        return Err(bound_actor_failure(
                            &observer_owner,
                            permit,
                            "deliver root result batch",
                            error,
                        )
                        .await);
                    }
                    Err(interruption) => {
                        return Err(
                            bound_pump_interruption(&observer_owner, permit, interruption).await,
                        );
                    }
                }
                acknowledged = Some(expected);
                expected = match expected.next() {
                    Some(next) => next,
                    None => {
                        return Err(bound_pump_failure(
                            &observer_owner,
                            permit,
                            contract_failure(contract_error(
                                "root result packet sequence overflowed",
                            )),
                        )
                        .await);
                    }
                };
            }
            RootResultFetchOutcome::EndPending(sequence) if pending_eos.is_none() => {
                drop(credit);
                if sequence != expected {
                    return Err(bound_pump_failure(
                        &observer_owner,
                        permit,
                        contract_failure(contract_error(
                            "root result pending EOF does not match the next sequence",
                        )),
                    )
                    .await);
                }
                pending_eos = Some(sequence);
                acknowledged = Some(sequence);
            }
            RootResultFetchOutcome::EndAcknowledged(sequence) => {
                drop(credit);
                if pending_eos != Some(sequence) || sequence != expected {
                    return Err(bound_pump_failure(
                        &observer_owner,
                        permit,
                        contract_failure(contract_error(
                            "root result EOF acknowledgement does not match pending EOF",
                        )),
                    )
                    .await);
                }
                if let Err(error) = runtime
                    .observer
                    .observe_final_worker_eos_ack(root, sequence)
                    .await
                {
                    return Err(bound_actor_failure(
                        &observer_owner,
                        permit,
                        "observe final Worker result ACK",
                        error,
                    )
                    .await);
                }
                if !runtime.success_sealed {
                    let seal_reply = match runtime.statuses.begin_success_seal_request() {
                        Ok(reply) => reply,
                        Err(error) => {
                            return Err(bound_pump_failure(
                                &observer_owner,
                                permit,
                                contract_failure(error),
                            )
                            .await);
                        }
                    };
                    let seal_result = runtime
                        .await_step(async move {
                            seal_reply.await.map_err(|_| {
                                contract_error(
                                    "success-seal request owner dropped without a verdict",
                                )
                            })?
                        })
                        .await;
                    match seal_result {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            return Err(bound_pump_failure(
                                &observer_owner,
                                permit,
                                contract_failure(error),
                            )
                            .await);
                        }
                        Err(interruption) => {
                            return Err(bound_pump_interruption(
                                &observer_owner,
                                permit,
                                interruption,
                            )
                            .await);
                        }
                    }
                }
                if let Err(interruption) = runtime.await_success_seal().await {
                    return Err(
                        bound_pump_interruption(&observer_owner, permit, interruption).await,
                    );
                }
                return match permit.finish_result_stream().await {
                    Ok(conclusion) => Ok(conclusion),
                    Err(error) => Err(finish_handoff_failure(observer_owner, error).await),
                };
            }
            _ => {
                drop(credit);
                return Err(bound_pump_failure(
                    &observer_owner,
                    permit,
                    contract_failure(contract_error(
                        "root result outcome is invalid for the current stream phase",
                    )),
                )
                .await);
            }
        }
    }
}

fn pump_failure(
    permit: RunningAttemptPermit,
    failure: RootResultFetchFailure,
) -> ResultPumpFailure {
    ResultPumpFailure::DecisionPending(ResultPumpDecision {
        permit,
        observer: None,
        class: failure.class,
        error: failure.error,
    })
}

fn pump_actor_failure(
    permit: RunningAttemptPermit,
    operation: &'static str,
    error: LogicalExecutionActorError,
) -> ResultPumpFailure {
    let conclusion = actor_error_conclusion(error);
    let failure = actor_failure(operation, error);
    if let Some(conclusion) = conclusion {
        drop(permit);
        concluded_pump_failure(conclusion, failure)
    } else {
        pump_failure(permit, failure)
    }
}

async fn bound_pump_failure(
    observer: &RootResultObserver,
    permit: RunningAttemptPermit,
    failure: RootResultFetchFailure,
) -> ResultPumpFailure {
    match permit
        .freeze_result_attempt_failure(failure.error.clone())
        .await
    {
        Ok(()) => ResultPumpFailure::DecisionPending(ResultPumpDecision {
            permit,
            observer: Some(observer.clone()),
            class: failure.class,
            error: failure.error,
        }),
        Err(LogicalExecutionActorError::ExecutionConcluded(conclusion)) => {
            drop(permit);
            ResultPumpFailure::Concluded(ConcludedResultPumpFailure {
                conclusion,
                class: failure.class,
                error: failure.error,
            })
        }
        Err(error) => {
            drop(permit);
            ResultPumpFailure::ActorOutcomeUnknown(ActorOutcomeUnknownResultPumpFailure {
                error: actor_failure("freeze result attempt failure", error).error,
            })
        }
    }
}

async fn bound_actor_failure(
    observer: &RootResultObserver,
    permit: RunningAttemptPermit,
    operation: &'static str,
    error: LogicalExecutionActorError,
) -> ResultPumpFailure {
    let conclusion = actor_error_conclusion(error);
    let failure = actor_failure(operation, error);
    if let Some(conclusion) = conclusion {
        drop(permit);
        concluded_pump_failure(conclusion, failure)
    } else {
        bound_pump_failure(observer, permit, failure).await
    }
}

async fn bound_pump_interruption(
    observer: &RootResultObserver,
    permit: RunningAttemptPermit,
    interruption: PumpInterruption,
) -> ResultPumpFailure {
    match interruption {
        PumpInterruption::Decision(failure) => bound_pump_failure(observer, permit, failure).await,
        PumpInterruption::Cancellation(failure) => {
            conclude_cancellation(Some(observer.clone()), permit, failure).await
        }
        PumpInterruption::Concluded(conclusion, failure) => {
            drop(permit);
            ResultPumpFailure::Concluded(ConcludedResultPumpFailure {
                conclusion,
                class: failure.class,
                error: failure.error,
            })
        }
    }
}

async fn conclude_cancellation(
    observer: Option<RootResultObserver>,
    permit: RunningAttemptPermit,
    failure: RootResultFetchFailure,
) -> ResultPumpFailure {
    match permit.await_work_cancellation().await {
        Ok(conclusion) | Err(RunningAttemptHandoffError::ExecutionConcluded(conclusion)) => {
            drop(observer);
            concluded_pump_failure(conclusion, failure)
        }
        Err(RunningAttemptHandoffError::NotSubmitted { permit, .. }) => {
            if let Some(observer) = observer {
                bound_pump_failure(&observer, permit, failure).await
            } else {
                pump_failure(permit, failure)
            }
        }
        Err(RunningAttemptHandoffError::ActorOutcomeUnknown(error)) => {
            drop(observer);
            actor_outcome_unknown("await work cancellation", error)
        }
    }
}

fn concluded_pump_failure(
    conclusion: LogicalConclusion,
    failure: RootResultFetchFailure,
) -> ResultPumpFailure {
    ResultPumpFailure::Concluded(ConcludedResultPumpFailure {
        conclusion,
        class: failure.class,
        error: failure.error,
    })
}

async fn finish_handoff_failure(
    observer: RootResultObserver,
    error: RunningAttemptHandoffError,
) -> ResultPumpFailure {
    match error {
        RunningAttemptHandoffError::ExecutionConcluded(conclusion) => {
            drop(observer);
            concluded_pump_failure(
                conclusion,
                actor_failure(
                    "finish root result stream",
                    LogicalExecutionActorError::ExecutionConcluded(conclusion),
                ),
            )
        }
        RunningAttemptHandoffError::NotSubmitted { permit, error } => {
            bound_actor_failure(&observer, permit, "finish root result stream", error).await
        }
        RunningAttemptHandoffError::ActorOutcomeUnknown(error) => {
            drop(observer);
            actor_outcome_unknown("finish root result stream", error)
        }
    }
}

fn actor_error_conclusion(error: LogicalExecutionActorError) -> Option<LogicalConclusion> {
    match error {
        LogicalExecutionActorError::ExecutionConcluded(conclusion) => Some(conclusion),
        _ => None,
    }
}

fn actor_outcome_unknown(
    operation: &'static str,
    error: LogicalExecutionActorError,
) -> ResultPumpFailure {
    ResultPumpFailure::ActorOutcomeUnknown(ActorOutcomeUnknownResultPumpFailure {
        error: actor_failure(operation, error).error,
    })
}

fn contract_error(message: impl Into<Arc<str>>) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::InvalidRequest, message)
}

fn contract_failure(error: QueryExecutionError) -> RootResultFetchFailure {
    RootResultFetchFailure::new(AttemptFailureClass::ContractViolation, error)
}

fn actor_failure(
    operation: &'static str,
    error: LogicalExecutionActorError,
) -> RootResultFetchFailure {
    RootResultFetchFailure::new(
        AttemptFailureClass::ContractViolation,
        QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            format!("{operation} failed: {error}"),
        ),
    )
}

fn work_failure(error: WorkError) -> RootResultFetchFailure {
    match error {
        WorkError::Cancelled(reason) => cancellation_failure(reason),
        WorkError::Capacity(_) | WorkError::CapacityWaitTimeout => RootResultFetchFailure::new(
            AttemptFailureClass::ResourceGovernance,
            QueryExecutionError::new(
                QueryExecutionErrorKind::Rejected,
                format!("result capacity is unavailable: {error}"),
            ),
        ),
        error => contract_failure(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            format!("result capacity contract failed: {error}"),
        )),
    }
}

fn work_interruption(error: WorkError) -> PumpInterruption {
    let failure = work_failure(error);
    if matches!(
        failure.class,
        AttemptFailureClass::Cancelled | AttemptFailureClass::DeadlineExceeded
    ) {
        PumpInterruption::Cancellation(failure)
    } else {
        PumpInterruption::Decision(failure)
    }
}

fn decode_supervisor_failure(error: ResultDecodeWorkerError) -> RootResultFetchFailure {
    let (class, kind) = match error {
        ResultDecodeWorkerError::Panicked => (
            AttemptFailureClass::ContractViolation,
            QueryExecutionErrorKind::Failed,
        ),
        ResultDecodeWorkerError::ExecutorClosed => (
            AttemptFailureClass::ResourceGovernance,
            QueryExecutionErrorKind::Rejected,
        ),
    };
    RootResultFetchFailure::new(
        class,
        QueryExecutionError::new(kind, format!("result decode executor failed: {error}")),
    )
}

fn cancellation_failure(reason: CancellationReason) -> RootResultFetchFailure {
    let (class, kind, message) = match reason {
        CancellationReason::DeadlineExceeded
        | CancellationReason::FrontendDrainDeadlineExceeded => (
            AttemptFailureClass::DeadlineExceeded,
            QueryExecutionErrorKind::DeadlineExceeded,
            "logical execution deadline expired while pumping results",
        ),
        _ => (
            AttemptFailureClass::Cancelled,
            QueryExecutionErrorKind::Cancelled,
            "logical execution was cancelled while pumping results",
        ),
    };
    RootResultFetchFailure::new(class, QueryExecutionError::new(kind, message))
}

fn classify_root_termination(
    status: &TaskStatus,
    authoritative_attempt_failure: Option<&TerminationDetail>,
) -> Option<RootResultFetchFailure> {
    let detail = authoritative_attempt_failure.or_else(|| status.termination())?;
    let (class, message) = match detail {
        TerminationDetail::Canceled(reason) => (
            AttemptFailureClass::ExecutionFailure,
            format!("attempt ended without stable result success: CANCELED(reason={reason})"),
        ),
        TerminationDetail::Aborted(AbortCause::LeaseExpired) => (
            AttemptFailureClass::RecoverableInfrastructure,
            "attempt ended without stable result success: ABORTED(cause=LEASE_EXPIRED)".to_owned(),
        ),
        TerminationDetail::Aborted(AbortCause::PeerTaskFailed) => {
            return Some(contract_failure(contract_error(
                "derived root termination reached classification without an authoritative attempt failure",
            )));
        }
        TerminationDetail::Aborted(cause) => (
            AttemptFailureClass::ExecutionFailure,
            format!("attempt ended without stable result success: ABORTED(cause={cause})"),
        ),
        TerminationDetail::Failed(failure) => {
            let class = match failure.category() {
                TaskFailureCategory::ResourceExhausted => {
                    AttemptFailureClass::RecoverableInfrastructure
                }
                TaskFailureCategory::Execution | TaskFailureCategory::Exchange => {
                    AttemptFailureClass::ExecutionFailure
                }
                TaskFailureCategory::Protocol | TaskFailureCategory::Internal => {
                    AttemptFailureClass::ContractViolation
                }
            };
            (
                class,
                format!(
                    "attempt ended without stable result success: FAILED(category={}, detail={})",
                    failure.category(),
                    failure.detail()
                ),
            )
        }
    };
    Some(RootResultFetchFailure::new(
        class,
        QueryExecutionError::new(QueryExecutionErrorKind::Failed, message),
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        num::NonZeroUsize,
        sync::{
            Arc, Mutex, OnceLock,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        time::Duration,
    };

    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure, TaskOutputFacts, TaskState, TaskStatusVersion,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryId, StageId, TaskId, identity::QueryExecutionId,
    };
    use novarocks_workload_control::{
        ResourceConfig, Stage, WorkClass, WorkOwner, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    use tokio::{runtime::Handle, sync::Notify};

    use crate::{
        api::{ExecutionOutput, NativeAttemptPreparationFailure, ResultDelivery, ResultField},
        coordination::{
            ExecutionEffect, LogicalExecutionActor, LogicalExecutionActorConfig,
            spawn_logical_execution_actor,
        },
    };

    use super::*;

    #[derive(Debug, Default)]
    struct TestSuccessSealPort {
        requests: Mutex<VecDeque<AcceptedRootSuccessSealRequest>>,
        ready: Notify,
    }

    impl AcceptedRootSuccessSealPort for TestSuccessSealPort {
        fn enqueue_success_seal(
            &self,
            request: AcceptedRootSuccessSealRequest,
        ) -> Result<(), AcceptedRootSuccessSealRequest> {
            self.requests.lock().unwrap().push_back(request);
            self.ready.notify_one();
            Ok(())
        }
    }

    impl TestSuccessSealPort {
        async fn next(&self) -> AcceptedRootSuccessSealRequest {
            loop {
                if let Some(request) = self.requests.lock().unwrap().pop_front() {
                    return request;
                }
                self.ready.notified().await;
            }
        }
    }

    fn execution(tag: i64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(77, tag), AttemptId::new(1).unwrap()).unwrap()
    }

    fn root_task(execution: QueryExecutionId) -> TaskIdentity {
        TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }

    fn result_schema() -> ResultSchema {
        ResultSchema::new(vec![ResultField::new(
            "value",
            DataType::Int64,
            false,
            None,
        )])
    }

    fn empty_result_schema() -> ResultSchema {
        ResultSchema::new(Vec::new())
    }

    fn result_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![7_i64, 11]))],
        )
        .unwrap()
    }

    fn empty_result_batch() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    fn finished(root: TaskIdentity) -> TaskStatus {
        TaskStatus::try_new(
            root,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap()
    }

    fn running(root: TaskIdentity) -> TaskStatus {
        TaskStatus::try_new(
            root,
            TaskStatusVersion::new(1).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::new(false),
        )
        .unwrap()
    }

    fn failed(root: TaskIdentity, detail: &str) -> TaskStatus {
        TaskStatus::try_new(
            root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Failed,
            Some(TerminationDetail::Failed(TaskFailure::new(
                TaskFailureCategory::Execution,
                SafeDetail::new(detail).unwrap(),
            ))),
            TaskOutputFacts::new(false),
        )
        .unwrap()
    }

    struct Harness {
        control: WorkloadControl,
        scope: WorkScope,
        actor: LogicalExecutionActor,
        owner: super::super::LogicalExecutionActorOwner,
        permit: RunningAttemptPermit,
        stream: crate::api::QueryResultStream,
        root: TaskIdentity,
    }

    async fn harness(tag: i64) -> Harness {
        harness_with_schema(tag, result_schema()).await
    }

    async fn harness_with_schema(tag: i64, schema: ResultSchema) -> Harness {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let stage = scope.try_acquire(Stage::Execution).unwrap();
        let execution = execution(tag);
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            ExecutionEffect::None,
            NonZeroUsize::new(8).unwrap(),
            Vec::new(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            work.owner,
            stage,
        )
        .unwrap()
        .with_result_stream(schema, NonZeroUsize::new(1).unwrap());
        let (owner, initial, output) = spawn_logical_execution_actor(&Handle::current(), config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result actor must expose a stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let permit = actor.activate(initial.ready()).await.unwrap();
        Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            stream,
            root: root_task(execution),
        }
    }

    async fn cancellable_harness(tag: i64) -> (Harness, WorkOwner) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let parent = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap()
            .owner;
        let work_owner = parent
            .scope()
            .child(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work_owner.scope();
        let stage = scope.try_acquire(Stage::Execution).unwrap();
        let execution = execution(tag);
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            ExecutionEffect::None,
            NonZeroUsize::new(8).unwrap(),
            Vec::new(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            work_owner,
            stage,
        )
        .unwrap()
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap());
        let (owner, initial, output) = spawn_logical_execution_actor(&Handle::current(), config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result actor must expose a stream");
        };
        stream.begin_schema().unwrap().complete();
        let actor = owner.actor().clone();
        let permit = actor.activate(initial.ready()).await.unwrap();
        (
            Harness {
                control,
                scope,
                actor,
                owner,
                permit,
                stream,
                root: root_task(execution),
            },
            parent,
        )
    }

    fn decode_runtime() -> RootResultDecodeRuntime {
        static OWNER: OnceLock<RootResultDecodeRuntimeOwner> = OnceLock::new();
        OWNER
            .get_or_init(|| {
                RootResultDecodeRuntimeOwner::try_new(
                    NonZeroUsize::new(4).unwrap(),
                    NonZeroUsize::new(64).unwrap(),
                )
                .unwrap()
            })
            .runtime()
    }

    fn completed_native_terminal() -> NativeAttemptTerminalSource {
        let (sender, source) = native_attempt_terminal_channel();
        sender.publish(NativeAttemptTerminal::Completed);
        source
    }

    fn failed_native_terminal(message: &str) -> NativeAttemptTerminal {
        NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::new(
            AttemptFailureClass::RecoverableInfrastructure,
            QueryExecutionError::new(QueryExecutionErrorKind::Failed, message),
        ))
    }

    #[test]
    fn dropping_a_query_binding_does_not_close_the_process_decode_runtime() {
        let owner = RootResultDecodeRuntimeOwner::try_new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        let runtime = owner.runtime();
        let binding = RootResultPumpBinding::new(runtime.clone(), |_| async {
            Ok(RootResultFetchOutcome::NotReady)
        });
        drop(binding);

        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let receipt = tokio
            .block_on(runtime.handle.submit(ResultDecodeJob::new(|| {
                Err(contract_failure(contract_error("expected test result")))
            })))
            .unwrap();
        let decoded = tokio.block_on(receipt.complete()).unwrap();
        let Err(failure) = decoded else {
            panic!("the test decode job must return its scripted failure");
        };
        assert_eq!(failure.error().message(), "expected test result");
        drop(runtime);
        owner.shutdown_and_join_blocking().unwrap();
    }

    fn scripted_binding(outcomes: Vec<RootResultFetchOutcome>) -> RootResultPumpBinding {
        let outcomes = Arc::new(Mutex::new(VecDeque::from(outcomes)));
        RootResultPumpBinding::new(decode_runtime(), move |_request| {
            let outcome = outcomes
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted fetch must have an answer");
            async move { Ok(outcome) }
        })
    }

    fn packet(sequence: u64, decode_calls: Arc<AtomicUsize>) -> PreflightedRootResultPacket {
        PreflightedRootResultPacket::new(
            ResultPacketSequence::new(sequence),
            64,
            RootResultDecodeBounds::new(4_096, 4_096).unwrap(),
            move || {
                decode_calls.fetch_add(1, Ordering::SeqCst);
                Ok(result_batch())
            },
        )
        .unwrap()
    }

    struct RawDropProbe {
        authority: LocalResourceAuthority,
        observed_raw_credit: Option<mpsc::Sender<u64>>,
    }

    impl Drop for RawDropProbe {
        fn drop(&mut self) {
            if let Some(observed) = self.observed_raw_credit.take() {
                let _ = observed.send(self.authority.snapshot().result_credit.raw_retained_bytes);
            }
        }
    }

    #[test]
    fn zero_retained_backing_is_a_valid_decode_bound() {
        let bounds = RootResultDecodeBounds::new(1, 0).unwrap();
        assert_eq!(bounds.decode_operation_upper_bound(), 1);
        assert_eq!(bounds.retained_backing_upper_bound(), 0);
    }

    #[test]
    fn local_capacity_wait_timeout_is_not_an_attempt_retry_signal() {
        let failure = work_failure(WorkError::CapacityWaitTimeout);
        assert_eq!(failure.class(), AttemptFailureClass::ResourceGovernance);
        assert_eq!(failure.error().kind(), QueryExecutionErrorKind::Rejected);
    }

    #[test]
    fn actor_conclusion_classifier_requires_an_explicit_actor_fact() {
        for conclusion in [
            LogicalConclusion::Succeeded,
            LogicalConclusion::Failed,
            LogicalConclusion::Cancelled,
            LogicalConclusion::BusinessDecisionRequired,
        ] {
            assert_eq!(
                actor_error_conclusion(LogicalExecutionActorError::ExecutionConcluded(conclusion,)),
                Some(conclusion)
            );
        }
        assert_eq!(
            actor_error_conclusion(LogicalExecutionActorError::ResultDeliveryFailed),
            None
        );
        assert_eq!(
            actor_error_conclusion(LogicalExecutionActorError::MailboxClosed),
            None
        );
    }

    #[tokio::test]
    async fn borrowed_delivery_error_retains_the_attempt_decision() {
        let Harness {
            actor,
            owner,
            permit,
            mut stream,
            ..
        } = harness(140).await;
        let ResultPumpFailure::DecisionPending(decision) = pump_actor_failure(
            permit,
            "deliver root result batch",
            LogicalExecutionActorError::ResultDeliveryFailed,
        ) else {
            panic!("an unproven delivery error must retain attempt authority");
        };
        assert_eq!(
            decision.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn unsubmitted_finish_handoff_retains_the_permit() {
        let Harness {
            actor,
            owner,
            permit,
            mut stream,
            root,
            ..
        } = harness(141).await;
        let observer = permit.bind_root_result(root).await.unwrap();
        let ResultPumpFailure::DecisionPending(decision) = finish_handoff_failure(
            observer,
            RunningAttemptHandoffError::NotSubmitted {
                permit,
                error: LogicalExecutionActorError::MailboxClosed,
            },
        )
        .await
        else {
            panic!("a finish handoff rejected before submission must retain authority");
        };
        assert_eq!(
            decision.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn accepted_finish_with_lost_outcome_is_explicitly_unknown() {
        let Harness {
            actor,
            owner,
            permit,
            stream,
            root,
            ..
        } = harness(142).await;
        let observer = permit.bind_root_result(root).await.unwrap();
        let failure = finish_handoff_failure(
            observer,
            RunningAttemptHandoffError::ActorOutcomeUnknown(
                LogicalExecutionActorError::MailboxClosed,
            ),
        )
        .await;
        let ResultPumpFailure::ActorOutcomeUnknown(failure) = failure else {
            panic!("an accepted handoff without a reply must have unknown outcome");
        };
        assert_eq!(failure.error().kind(), QueryExecutionErrorKind::Failed);
        drop(permit);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn adapter_cancellation_label_cannot_impersonate_work_scope_cancellation() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(13).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let binding = RootResultPumpBinding::new(decode_runtime(), |_| async {
            Err(RootResultFetchFailure::new(
                AttemptFailureClass::DeadlineExceeded,
                QueryExecutionError::new(
                    QueryExecutionErrorKind::DeadlineExceeded,
                    "adapter-local timeout",
                ),
            ))
        });
        let ResultPumpFailure::DecisionPending(failure) = run_root_result_pump(
            permit,
            root,
            scope,
            control.resources(),
            result_schema(),
            binding,
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        )
        .await
        .unwrap_err() else {
            panic!("only WorkScope may create terminal cancellation handoff");
        };
        assert_eq!(failure.class(), AttemptFailureClass::DeadlineExceeded);
        failure.fail_logical(&actor).await.unwrap();
        assert!(stream.next().await.is_err());
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn root_fetch_waits_for_the_first_accepted_root_status() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(143).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        let fetch_calls = Arc::new(AtomicUsize::new(0));
        let fetch_calls_for_binding = Arc::clone(&fetch_calls);
        let pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            control.resources(),
            result_schema(),
            RootResultPumpBinding::new(decode_runtime(), move |_| {
                fetch_calls_for_binding.fetch_add(1, Ordering::SeqCst);
                async {
                    Err(RootResultFetchFailure::new(
                        AttemptFailureClass::ContractViolation,
                        contract_error("scripted fetch after root admission"),
                    ))
                }
            }),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        ));

        let premature_fetch = tokio::time::timeout(Duration::from_millis(20), async {
            while fetch_calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            premature_fetch.is_err(),
            "root fetch must not start before an accepted root status"
        );

        status_sender.publish(running(root)).unwrap();
        let ResultPumpFailure::DecisionPending(failure) =
            tokio::time::timeout(Duration::from_secs(1), pump)
                .await
                .expect("accepted root status must release the first fetch")
                .unwrap()
                .unwrap_err()
        else {
            panic!("the scripted fetch failure must retain attempt authority");
        };
        assert_eq!(fetch_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            failure.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        drop(status_sender);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn derived_root_failure_requires_and_uses_authoritative_attempt_failure() {
        let root = root_task(execution(7));
        let (status_sender, mut statuses) = accepted_root_status_projection(root);
        let derived = TaskStatus::try_new(
            root,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Aborted,
            Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            TaskOutputFacts::new(false),
        )
        .unwrap();
        status_sender.publish(derived.clone()).unwrap();
        let AcceptedRootProjection::Observation(observation) = statuses.next().await.unwrap()
        else {
            panic!("derived failure must be published before success can be sealed");
        };
        assert!(matches!(
            observation.attempt_failure,
            AcceptedAttemptFailure::DerivedPending
        ));
        let authoritative = TerminationDetail::Failed(TaskFailure::new(
            TaskFailureCategory::ResourceExhausted,
            SafeDetail::new("remote task exceeded its local memory limit").unwrap(),
        ));
        status_sender
            .publish_with_attempt_failure(derived, authoritative)
            .unwrap();
        let AcceptedRootProjection::Observation(observation) = statuses.next().await.unwrap()
        else {
            panic!("authoritative failure cannot be a success seal");
        };
        let AcceptedAttemptFailure::Authoritative(authoritative) = observation.attempt_failure
        else {
            panic!("same-version failure cause must refine pending to authoritative");
        };
        let failure = classify_root_termination(&observation.status, Some(&authoritative)).unwrap();
        assert_eq!(
            failure.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert!(
            failure
                .error()
                .message()
                .contains("remote task exceeded its local memory limit")
        );
        assert!(!failure.error().message().contains("PEER_TASK_FAILED"));
    }

    #[tokio::test]
    async fn zero_column_batch_traverses_decode_queue_and_delivery() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness_with_schema(8, empty_result_schema()).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(finished(root)).unwrap();
        status_sender.seal_success().unwrap();
        let packet = PreflightedRootResultPacket::new(
            ResultPacketSequence::new(0),
            16,
            RootResultDecodeBounds::new(128, 0).unwrap(),
            || Ok(empty_result_batch()),
        )
        .unwrap();
        let authority = control.resources();
        let pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            authority.clone(),
            empty_result_schema(),
            scripted_binding(vec![
                RootResultFetchOutcome::Ready(packet),
                RootResultFetchOutcome::EndPending(ResultPacketSequence::new(1)),
                RootResultFetchOutcome::EndAcknowledged(ResultPacketSequence::new(1)),
            ]),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(128).unwrap(),
        ));
        let next = stream.next().await.unwrap().unwrap();
        let ResultDelivery::Batch(delivery) = next else {
            panic!("zero-column batch must remain a data delivery");
        };
        assert_eq!(delivery.batch().num_columns(), 0);
        assert_eq!(delivery.decoded_bytes(), 1);
        delivery
            .reserve_protocol(&authority, 1)
            .unwrap()
            .begin_protocol_write(1)
            .unwrap()
            .complete()
            .unwrap();
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("zero-column stream must terminate with stable EOF");
        };
        end.complete();
        assert_eq!(pump.await.unwrap().unwrap(), LogicalConclusion::Succeeded);
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn sequence_is_rejected_before_decode_or_visibility() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(1).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(finished(root)).unwrap();
        let decode_calls = Arc::new(AtomicUsize::new(0));
        let ResultPumpFailure::DecisionPending(failure) = run_root_result_pump(
            permit,
            root,
            scope,
            control.resources(),
            result_schema(),
            scripted_binding(vec![RootResultFetchOutcome::Ready(packet(
                1,
                Arc::clone(&decode_calls),
            ))]),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        )
        .await
        .unwrap_err() else {
            panic!("sequence rejection must return an attempt decision");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        assert_eq!(decode_calls.load(Ordering::SeqCst), 0);
        assert!(!actor.snapshot().await.unwrap().output_visible);
        assert_eq!(control.resources().snapshot().result_credit.held_bytes(), 0);
        let replacement = QueryExecutionId::new(
            root.query_execution_id().query_id(),
            AttemptId::new(2).unwrap(),
        )
        .unwrap();
        assert_eq!(
            failure
                .begin_replacement(&actor, replacement, Vec::new())
                .await
                .unwrap_err(),
            LogicalExecutionActorError::RecoveryRefused(super::super::RecoveryRefusal::Mode)
        );
        assert!(stream.next().await.is_err());
        drop(status_sender);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn requested_payload_limit_is_checked_before_decode() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(3).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let decode_calls = Arc::new(AtomicUsize::new(0));
        let ResultPumpFailure::DecisionPending(failure) = run_root_result_pump(
            permit,
            root,
            scope,
            control.resources(),
            result_schema(),
            scripted_binding(vec![RootResultFetchOutcome::Ready(packet(
                0,
                Arc::clone(&decode_calls),
            ))]),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(32).unwrap(),
        )
        .await
        .unwrap_err() else {
            panic!("payload contract rejection must return an attempt decision");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        assert_eq!(decode_calls.load(Ordering::SeqCst), 0);
        assert_eq!(control.resources().snapshot().result_credit.held_bytes(), 0);
        failure.fail_logical(&actor).await.unwrap();
        assert!(stream.next().await.is_err());
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn decode_worker_panic_releases_real_result_credit() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(18).await;
        let decode_owner = RootResultDecodeRuntimeOwner::try_new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        let decode = decode_owner.runtime();
        let packet = Arc::new(Mutex::new(Some(
            PreflightedRootResultPacket::new(
                ResultPacketSequence::new(0),
                64,
                RootResultDecodeBounds::new(4_096, 4_096).unwrap(),
                || panic!("scripted result decode panic"),
            )
            .unwrap(),
        )));
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let authority = control.resources();
        let ResultPumpFailure::DecisionPending(failure) = run_root_result_pump(
            permit,
            root,
            scope,
            authority.clone(),
            result_schema(),
            RootResultPumpBinding::new(decode.clone(), move |_| {
                let packet = Arc::clone(&packet);
                async move {
                    Ok(RootResultFetchOutcome::Ready(
                        packet.lock().unwrap().take().unwrap(),
                    ))
                }
            }),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        )
        .await
        .unwrap_err() else {
            panic!("decode panic must retain the attempt decision");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        assert_eq!(
            failure.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        drop(stream);
        drop(actor);
        drop(owner);
        drop(decode);
        decode_owner.shutdown_and_join_blocking().unwrap();
    }

    #[tokio::test]
    async fn process_decode_shutdown_releases_queued_real_result_credit() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(19).await;
        let decode_owner = RootResultDecodeRuntimeOwner::try_new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        let decode = decode_owner.runtime();
        let (started, decode_started) = mpsc::channel();
        let (release, decode_release) = mpsc::channel();
        let blocker = decode
            .handle
            .submit(ResultDecodeJob::new(move || {
                started.send(()).unwrap();
                decode_release.recv().unwrap();
                Err(contract_failure(contract_error(
                    "scripted blocking decode result",
                )))
            }))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match decode_started.try_recv() {
                    Ok(()) => break,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        panic!("decode worker exited before starting")
                    }
                }
            }
        })
        .await
        .expect("decode worker must start");
        drop(blocker);

        let packet = Arc::new(Mutex::new(Some(packet(0, Arc::new(AtomicUsize::new(0))))));
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let authority = control.resources();
        let pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            authority.clone(),
            result_schema(),
            RootResultPumpBinding::new(decode.clone(), move |_| {
                let packet = Arc::clone(&packet);
                async move {
                    Ok(RootResultFetchOutcome::Ready(
                        packet.lock().unwrap().take().unwrap(),
                    ))
                }
            }),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while decode.handle.snapshot().queued != 1
                || authority.snapshot().result_credit.decode_reserved_bytes == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pump decode must be queued with real result credit");

        let shutdown = std::thread::spawn(move || decode_owner.shutdown_and_join_blocking());
        tokio::time::timeout(Duration::from_secs(1), async {
            while authority.snapshot().result_credit.held_bytes() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("closing the decode queue must release its queued result credit");
        release.send(()).unwrap();
        shutdown.join().unwrap().unwrap();

        let ResultPumpFailure::DecisionPending(failure) = pump.await.unwrap().unwrap_err() else {
            panic!("decode shutdown must retain the attempt decision");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ResourceGovernance);
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        assert_eq!(
            failure.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        drop(stream);
        drop(actor);
        drop(owner);
        drop(decode);
    }

    #[tokio::test]
    async fn cancellation_drops_raw_packet_before_releasing_decode_wait_credit() {
        let (
            Harness {
                control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            parent,
        ) = cancellable_harness(15).await;
        let authority = control.resources();
        let blocker = authority
            .reserve(
                &scope,
                261_000,
                novarocks_workload_control::ResourceClass::Data,
            )
            .unwrap();
        let (observed, observed_raw_credit) = mpsc::channel();
        let probe = RawDropProbe {
            authority: authority.clone(),
            observed_raw_credit: Some(observed),
        };
        let raw = PreflightedRootResultPacket::new(
            ResultPacketSequence::new(0),
            64,
            RootResultDecodeBounds::new(4_096, 4_096).unwrap(),
            move || {
                drop(probe);
                Ok(result_batch())
            },
        )
        .unwrap();
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            authority.clone(),
            result_schema(),
            scripted_binding(vec![RootResultFetchOutcome::Ready(raw)]),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(128).unwrap(),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = control.snapshot();
                if snapshot.resource_waiters == 1
                    && authority.snapshot().result_credit.raw_retained_bytes == 64
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pump must reach the decode-capacity wait");
        parent.cancel(CancellationReason::Requested);
        let ResultPumpFailure::Concluded(failure) = pump.await.unwrap().unwrap_err() else {
            panic!("WorkScope cancellation must conclude the result pump");
        };
        assert_eq!(failure.class(), AttemptFailureClass::Cancelled);
        assert_eq!(
            observed_raw_credit
                .recv_timeout(Duration::from_secs(1))
                .unwrap(),
            64
        );
        assert_eq!(authority.snapshot().result_credit.raw_retained_bytes, 0);
        drop(blocker);
        assert_eq!(authority.snapshot().held_bytes(), 0);
        assert!(stream.next().await.is_err());
        drop(stream);
        drop(actor);
        owner
            .into_residual_stand_down_supervisor()
            .join()
            .await
            .unwrap();
        parent.complete();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_status_progresses_while_decode_worker_is_blocked() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(16).await;
        let (started, decode_started) = mpsc::channel();
        let (release, decode_release) = mpsc::channel();
        let raw = PreflightedRootResultPacket::new(
            ResultPacketSequence::new(0),
            64,
            RootResultDecodeBounds::new(4_096, 4_096).unwrap(),
            move || {
                started.send(()).unwrap();
                decode_release.recv().unwrap();
                Ok(result_batch())
            },
        )
        .unwrap();
        let decode = decode_runtime();
        let packet = Arc::new(Mutex::new(Some(raw)));
        let binding = RootResultPumpBinding::new(decode.clone(), move |_| {
            let outcome = RootResultFetchOutcome::Ready(
                packet
                    .lock()
                    .unwrap()
                    .take()
                    .expect("blocking decode test fetches once"),
            );
            async move { Ok(outcome) }
        });
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let authority = control.resources();
        let pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            authority.clone(),
            result_schema(),
            binding,
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match decode_started.try_recv() {
                    Ok(()) => break,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        panic!("decode worker exited before starting")
                    }
                }
            }
        })
        .await
        .expect("decode worker must start");
        assert!(authority.snapshot().result_credit.decode_reserved_bytes > 0);

        let terminal = failed(root, "upstream Task failed during result decode");
        status_sender
            .publish_with_attempt_failure(terminal.clone(), terminal.termination().unwrap().clone())
            .unwrap();
        let ResultPumpFailure::DecisionPending(failure) =
            tokio::time::timeout(Duration::from_secs(1), pump)
                .await
                .expect("terminal status must not wait for synchronous decode")
                .unwrap()
                .unwrap_err()
        else {
            panic!("pre-visibility failure must retain the attempt decision");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        assert!(authority.snapshot().result_credit.decode_reserved_bytes > 0);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while authority.snapshot().result_credit.held_bytes() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("orphaned decode completion must release its result credit");
        failure.fail_logical(&actor).await.unwrap();
        assert!(stream.next().await.is_err());
        drop(status_sender);
        drop(stream);
        drop(actor);
        drop(owner);
        drop(decode);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn consumer_drop_during_decode_reports_the_actors_actual_conclusion() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            stream,
            root,
        } = harness(20).await;
        let decode_owner = RootResultDecodeRuntimeOwner::try_new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        let decode = decode_owner.runtime();
        let (started, decode_started) = mpsc::channel();
        let (release, decode_release) = mpsc::channel();
        let raw = PreflightedRootResultPacket::new(
            ResultPacketSequence::new(0),
            64,
            RootResultDecodeBounds::new(4_096, 4_096).unwrap(),
            move || {
                started.send(()).unwrap();
                decode_release.recv().unwrap();
                Ok(result_batch())
            },
        )
        .unwrap();
        let packet = Arc::new(Mutex::new(Some(raw)));
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let authority = control.resources();
        let pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            authority.clone(),
            result_schema(),
            RootResultPumpBinding::new(decode.clone(), move |_| {
                let packet = Arc::clone(&packet);
                async move {
                    Ok(RootResultFetchOutcome::Ready(
                        packet.lock().unwrap().take().unwrap(),
                    ))
                }
            }),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match decode_started.try_recv() {
                    Ok(()) => break,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        panic!("decode worker exited before starting")
                    }
                }
            }
        })
        .await
        .expect("decode worker must start");
        assert!(authority.snapshot().result_credit.decode_reserved_bytes > 0);

        drop(stream);
        tokio::time::timeout(Duration::from_secs(1), async {
            while actor.snapshot().await.unwrap().conclusion != Some(LogicalConclusion::Failed) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("consumer closure must conclude the logical execution");
        release.send(()).unwrap();

        let ResultPumpFailure::Concluded(failure) = pump.await.unwrap().unwrap_err() else {
            panic!("late decoded delivery must report the actor's actual conclusion");
        };
        assert_eq!(failure.conclusion(), LogicalConclusion::Failed);
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop(actor);
        drop(owner);
        drop(decode);
        decode_owner.shutdown_and_join_blocking().unwrap();
    }

    #[tokio::test]
    async fn terminal_status_interrupts_slow_delivery_and_preserves_worker_detail() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(4).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let decode_calls = Arc::new(AtomicUsize::new(0));
        let pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            control.resources(),
            result_schema(),
            scripted_binding(vec![RootResultFetchOutcome::Ready(packet(
                0,
                Arc::clone(&decode_calls),
            ))]),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        ));

        let ResultDelivery::Batch(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("first delivery must be held by the slow consumer");
        };
        let terminal = failed(root, "connector read rejected the frozen snapshot");
        status_sender
            .publish_with_attempt_failure(terminal.clone(), terminal.termination().unwrap().clone())
            .unwrap();
        let ResultPumpFailure::Concluded(failure) =
            tokio::time::timeout(Duration::from_secs(1), pump)
                .await
                .expect("terminal status must interrupt a held delivery")
                .unwrap()
                .unwrap_err()
        else {
            panic!("visible terminal status must report an already concluded execution");
        };
        assert_eq!(failure.conclusion(), LogicalConclusion::Failed);
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        assert!(
            failure
                .error()
                .message()
                .contains("connector read rejected the frozen snapshot")
        );
        assert_eq!(decode_calls.load(Ordering::SeqCst), 1);
        drop(delivery);
        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("terminal root status must fail the visible result stream"),
        };
        assert!(
            error
                .message()
                .contains("connector read rejected the frozen snapshot")
        );
        drop(status_sender);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn pending_derived_failure_freezes_ack_until_authoritative_refinement() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(9).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let observed_requests = Arc::new(Mutex::new(Vec::new()));
        let binding = RootResultPumpBinding::new(decode_runtime(), {
            let observed_requests = Arc::clone(&observed_requests);
            let packet = Mutex::new(Some(packet(0, Arc::new(AtomicUsize::new(0)))));
            move |request| {
                observed_requests.lock().unwrap().push(request);
                let outcome = packet
                    .lock()
                    .unwrap()
                    .take()
                    .map(RootResultFetchOutcome::Ready)
                    .expect("pending failure must prevent a second fetch");
                async move { Ok(outcome) }
            }
        });
        let authority = control.resources();
        let pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            authority.clone(),
            result_schema(),
            binding,
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        ));
        let ResultDelivery::Batch(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("first result must reach the protocol owner");
        };
        let derived = TaskStatus::try_new(
            root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Aborted,
            Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            TaskOutputFacts::new(false),
        )
        .unwrap();
        status_sender.publish(derived.clone()).unwrap();
        tokio::task::yield_now().await;
        let bytes = delivery.decoded_bytes();
        delivery
            .reserve_protocol(&authority, bytes)
            .unwrap()
            .begin_protocol_write(bytes)
            .unwrap()
            .complete()
            .unwrap();
        tokio::task::yield_now().await;
        assert!(!pump.is_finished());
        assert_eq!(observed_requests.lock().unwrap().len(), 1);

        let authoritative = TerminationDetail::Failed(TaskFailure::new(
            TaskFailureCategory::ResourceExhausted,
            SafeDetail::new("upstream task exhausted its memory grant").unwrap(),
        ));
        status_sender
            .publish_with_attempt_failure(derived, authoritative)
            .unwrap();
        let ResultPumpFailure::Concluded(failure) =
            tokio::time::timeout(Duration::from_secs(1), pump)
                .await
                .expect("authoritative refinement must fix the visible execution")
                .unwrap()
                .unwrap_err()
        else {
            panic!("a completed protocol delivery must close attempt replacement");
        };
        assert_eq!(failure.conclusion(), LogicalConclusion::Failed);
        assert_eq!(
            failure.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert!(
            failure
                .error()
                .message()
                .contains("upstream task exhausted its memory grant")
        );
        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("refined attempt failure must terminate the stream"),
        };
        assert!(
            error
                .message()
                .contains("upstream task exhausted its memory grant")
        );
        drop(status_sender);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn pending_failure_refinement_observes_an_in_flight_writer_failure() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(11).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let authority = control.resources();
        let pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            authority.clone(),
            result_schema(),
            scripted_binding(vec![RootResultFetchOutcome::Ready(packet(
                0,
                Arc::new(AtomicUsize::new(0)),
            ))]),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        ));
        let ResultDelivery::Batch(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("first result must reach the protocol owner");
        };
        let derived = TaskStatus::try_new(
            root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Aborted,
            Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            TaskOutputFacts::new(false),
        )
        .unwrap();
        status_sender.publish(derived.clone()).unwrap();
        tokio::task::yield_now().await;
        delivery.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "protocol writer rejected the visible batch",
        ));
        loop {
            if actor.snapshot().await.unwrap().conclusion == Some(LogicalConclusion::Failed) {
                break;
            }
            tokio::task::yield_now().await;
        }

        let authoritative = TerminationDetail::Failed(TaskFailure::new(
            TaskFailureCategory::ResourceExhausted,
            SafeDetail::new("upstream task exhausted its memory grant").unwrap(),
        ));
        status_sender
            .publish_with_attempt_failure(derived, authoritative)
            .unwrap();
        let ResultPumpFailure::Concluded(failure) =
            tokio::time::timeout(Duration::from_secs(1), pump)
                .await
                .expect("refinement must observe the actor's fixed conclusion")
                .unwrap()
                .unwrap_err()
        else {
            panic!("writer failure must not reopen an attempt decision");
        };
        assert_eq!(failure.conclusion(), LogicalConclusion::Failed);
        assert_eq!(
            failure.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop(status_sender);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn final_worker_ack_after_actor_conclusion_cannot_open_a_decision() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            stream,
            root,
        } = harness(12).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let second_fetch_started = Arc::new(AtomicUsize::new(0));
        let release_second_fetch = Arc::new(Notify::new());
        let fetch_index = Arc::new(AtomicUsize::new(0));
        let binding = RootResultPumpBinding::new(decode_runtime(), {
            let second_fetch_started = Arc::clone(&second_fetch_started);
            let release_second_fetch = Arc::clone(&release_second_fetch);
            let fetch_index = Arc::clone(&fetch_index);
            move |_| {
                let index = fetch_index.fetch_add(1, Ordering::SeqCst);
                let second_fetch_started = Arc::clone(&second_fetch_started);
                let release_second_fetch = Arc::clone(&release_second_fetch);
                async move {
                    if index == 0 {
                        Ok(RootResultFetchOutcome::EndPending(
                            ResultPacketSequence::new(0),
                        ))
                    } else {
                        second_fetch_started.store(1, Ordering::Release);
                        release_second_fetch.notified().await;
                        Ok(RootResultFetchOutcome::EndAcknowledged(
                            ResultPacketSequence::new(0),
                        ))
                    }
                }
            }
        });
        let pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            control.resources(),
            result_schema(),
            binding,
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        ));
        while second_fetch_started.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        drop(stream);
        loop {
            if actor.snapshot().await.unwrap().conclusion == Some(LogicalConclusion::Failed) {
                break;
            }
            tokio::task::yield_now().await;
        }
        release_second_fetch.notify_one();
        let ResultPumpFailure::Concluded(failure) =
            tokio::time::timeout(Duration::from_secs(1), pump)
                .await
                .expect("final ACK must observe the actor's fixed conclusion")
                .unwrap()
                .unwrap_err()
        else {
            panic!("final ACK after conclusion must not reopen an attempt decision");
        };
        assert_eq!(failure.conclusion(), LogicalConclusion::Failed);
        drop(status_sender);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn cancellation_interrupts_wait_for_finished_after_eof_ack() {
        let Harness {
            actor,
            owner,
            permit,
            stream,
            root,
            ..
        } = harness(5).await;
        let observer = permit.bind_root_result(root).await.unwrap();
        let (_status_sender, statuses) = accepted_root_status_projection(root);
        let cancellation_control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        cancellation_control.mark_ready().unwrap();
        let cancellation_work = cancellation_control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let cancellation = cancellation_work.owner.scope().cancellation().unwrap();
        let mut runtime = PumpRuntime {
            observer,
            statuses,
            native_terminal: completed_native_terminal(),
            cancellation,
            root_finished: false,
            success_sealed: false,
            native_completed: false,
            pending_root_failure: None,
        };

        cancellation_work
            .owner
            .cancel(CancellationReason::DeadlineExceeded);
        let PumpInterruption::Cancellation(failure) =
            tokio::time::timeout(Duration::from_secs(1), runtime.await_success_seal())
                .await
                .expect("deadline must interrupt the final status wait")
                .unwrap_err()
        else {
            panic!("deadline must be a cancellation observation");
        };
        assert_eq!(failure.class(), AttemptFailureClass::DeadlineExceeded);
        assert_eq!(
            failure.error().kind(),
            QueryExecutionErrorKind::DeadlineExceeded
        );

        cancellation_work.owner.complete();
        drop(permit);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn cancellation_failure_waits_for_actor_deadline_conclusion() {
        let (
            Harness {
                control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            parent,
        ) = cancellable_harness(10).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let fetch_started = Arc::new(AtomicUsize::new(0));
        let fetch_started_for_binding = Arc::clone(&fetch_started);
        let pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            control.resources(),
            result_schema(),
            RootResultPumpBinding::new(decode_runtime(), move |_| {
                let fetch_started = Arc::clone(&fetch_started_for_binding);
                async move {
                    fetch_started.store(1, Ordering::Release);
                    std::future::pending::<Result<RootResultFetchOutcome, RootResultFetchFailure>>()
                        .await
                }
            }),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        ));
        while fetch_started.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        parent.cancel(CancellationReason::DeadlineExceeded);
        let ResultPumpFailure::Concluded(cancellation) =
            tokio::time::timeout(Duration::from_secs(1), pump)
                .await
                .expect("pump must observe inherited deadline")
                .unwrap()
                .unwrap_err()
        else {
            panic!("deadline must conclude without opening an attempt decision");
        };
        assert_eq!(cancellation.class(), AttemptFailureClass::DeadlineExceeded);
        assert_eq!(cancellation.conclusion(), LogicalConclusion::Failed);
        assert_eq!(
            cancellation.error().kind(),
            QueryExecutionErrorKind::DeadlineExceeded
        );
        let stream_error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("deadline must fail the result stream"),
        };
        assert_eq!(
            stream_error.kind(),
            QueryExecutionErrorKind::DeadlineExceeded
        );
        drop(status_sender);
        drop(stream);
        drop(actor);
        owner
            .into_residual_stand_down_supervisor()
            .join()
            .await
            .unwrap();
        parent.complete();
    }

    #[tokio::test]
    async fn inherited_deadline_before_root_binding_is_already_concluded() {
        let (
            Harness {
                control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            parent,
        ) = cancellable_harness(14).await;
        let (_status_sender, statuses) = accepted_root_status_projection(root);
        parent.cancel(CancellationReason::DeadlineExceeded);
        let ResultPumpFailure::Concluded(failure) = run_root_result_pump(
            permit,
            root,
            scope,
            control.resources(),
            result_schema(),
            RootResultPumpBinding::new(decode_runtime(), |_| async {
                panic!("a pre-existing deadline must prevent root fetch")
            }),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        )
        .await
        .unwrap_err() else {
            panic!("pre-existing WorkScope cancellation must already be terminal");
        };
        assert_eq!(failure.conclusion(), LogicalConclusion::Failed);
        assert_eq!(failure.class(), AttemptFailureClass::DeadlineExceeded);
        let stream_error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("deadline must fail the result stream"),
        };
        assert_eq!(
            stream_error.kind(),
            QueryExecutionErrorKind::DeadlineExceeded
        );
        drop(stream);
        drop(actor);
        owner
            .into_residual_stand_down_supervisor()
            .join()
            .await
            .unwrap();
        parent.complete();
    }

    #[tokio::test]
    async fn accepted_success_seal_publishes_eof_before_native_convergence() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(2).await;
        let seal_port = Arc::new(TestSuccessSealPort::default());
        let (status_sender, statuses) = accepted_root_status_projection_with_seal_port(
            root,
            Arc::clone(&seal_port) as Arc<dyn AcceptedRootSuccessSealPort>,
        );
        status_sender.publish(finished(root)).unwrap();
        let decode_calls = Arc::new(AtomicUsize::new(0));
        let observed_requests = Arc::new(Mutex::new(Vec::new()));
        let outcomes = Arc::new(Mutex::new(VecDeque::from(vec![
            RootResultFetchOutcome::Ready(packet(0, Arc::clone(&decode_calls))),
            RootResultFetchOutcome::EndPending(ResultPacketSequence::new(1)),
            RootResultFetchOutcome::EndAcknowledged(ResultPacketSequence::new(1)),
        ])));
        let binding = RootResultPumpBinding::new(decode_runtime(), {
            let observed_requests = Arc::clone(&observed_requests);
            move |request| {
                observed_requests.lock().unwrap().push(request);
                let outcome = outcomes.lock().unwrap().pop_front().unwrap();
                async move { Ok(outcome) }
            }
        });
        let authority = control.resources();
        let pump_authority = authority.clone();
        let (_terminal_sender, terminal_source) = native_attempt_terminal_channel();
        let pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            pump_authority,
            result_schema(),
            binding,
            statuses,
            terminal_source,
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        ));

        let ResultDelivery::Batch(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("first delivery must be the decoded batch");
        };
        assert_eq!(observed_requests.lock().unwrap().len(), 1);
        let bytes = delivery.decoded_bytes();
        delivery
            .reserve_protocol(&authority, bytes)
            .unwrap()
            .begin_protocol_write(bytes)
            .unwrap()
            .complete()
            .unwrap();
        while observed_requests.lock().unwrap().len() < 3 {
            tokio::task::yield_now().await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(20), stream.next())
                .await
                .is_err(),
            "final Worker ACK alone must not authorize success"
        );
        seal_port.next().await.accept(status_sender).unwrap();
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("stable Worker success must produce actor-owned EOF");
        };
        end.complete();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), pump)
                .await
                .expect("logical EOF must not wait for Native convergence")
                .unwrap()
                .unwrap(),
            LogicalConclusion::Succeeded
        );
        assert_eq!(decode_calls.load(Ordering::SeqCst), 1);
        let requests = observed_requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].acknowledged, None);
        assert_eq!(requests[1].acknowledged, Some(ResultPacketSequence::new(0)));
        assert_eq!(requests[2].acknowledged, Some(ResultPacketSequence::new(1)));
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop(requests);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn dropped_status_sender_after_finished_never_authorizes_success() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(6).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(finished(root)).unwrap();
        drop(status_sender);
        let authority = control.resources();
        let failure = run_root_result_pump(
            permit,
            root,
            scope,
            authority.clone(),
            result_schema(),
            scripted_binding(vec![
                RootResultFetchOutcome::Ready(packet(0, Arc::new(AtomicUsize::new(0)))),
                RootResultFetchOutcome::EndPending(ResultPacketSequence::new(1)),
                RootResultFetchOutcome::EndAcknowledged(ResultPacketSequence::new(1)),
            ]),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        )
        .await
        .unwrap_err();
        let ResultPumpFailure::DecisionPending(failure) = failure else {
            panic!("an unsealed sender drop must retain the attempt decision");
        };
        assert!(failure.error().message().contains("explicit success seal"));
        assert_eq!(
            failure.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn authoritative_attempt_failure_refines_a_finished_root_status() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(21).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        let root_finished = finished(root);
        status_sender.publish(root_finished.clone()).unwrap();
        let fetch_started = Arc::new(AtomicUsize::new(0));
        let fetch_started_for_binding = Arc::clone(&fetch_started);
        let authority = control.resources();
        let mut pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            authority.clone(),
            result_schema(),
            RootResultPumpBinding::new(decode_runtime(), move |_| {
                let fetch_started = Arc::clone(&fetch_started_for_binding);
                async move {
                    fetch_started.store(1, Ordering::Release);
                    std::future::pending::<Result<RootResultFetchOutcome, RootResultFetchFailure>>()
                        .await
                }
            }),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        ));
        while fetch_started.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }

        status_sender
            .publish_attempt_observation(
                root_finished.clone(),
                AcceptedAttemptFailure::DerivedPending,
            )
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut pump)
                .await
                .is_err(),
            "Unspecified to Pending must freeze the actor without deciding the attempt"
        );

        let authoritative = TerminationDetail::Failed(TaskFailure::new(
            TaskFailureCategory::Execution,
            SafeDetail::new("another Task failed after root Finished").unwrap(),
        ));
        status_sender
            .publish_with_attempt_failure(root_finished, authoritative)
            .unwrap();
        let ResultPumpFailure::DecisionPending(failure) =
            tokio::time::timeout(Duration::from_secs(1), pump)
                .await
                .expect("attempt failure must interrupt a finished-root fetch")
                .unwrap()
                .unwrap_err()
        else {
            panic!("pre-visibility attempt failure must retain the attempt decision");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        assert!(
            failure
                .error()
                .message()
                .contains("another Task failed after root Finished")
        );
        assert!(!actor.snapshot().await.unwrap().output_visible);
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        assert_eq!(
            failure.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        drop(status_sender);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn visible_pending_refinement_waits_for_the_authoritative_cause() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(23).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        let root_finished = finished(root);
        status_sender.publish(root_finished.clone()).unwrap();
        let fetch_index = Arc::new(AtomicUsize::new(0));
        let mut pump = tokio::spawn(run_root_result_pump(
            permit,
            root,
            scope,
            control.resources(),
            result_schema(),
            RootResultPumpBinding::new(decode_runtime(), {
                let fetch_index = Arc::clone(&fetch_index);
                move |_| {
                    let index = fetch_index.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if index == 0 {
                            Ok(RootResultFetchOutcome::Ready(packet(
                                0,
                                Arc::new(AtomicUsize::new(0)),
                            )))
                        } else {
                            std::future::pending::<
                                Result<RootResultFetchOutcome, RootResultFetchFailure>,
                            >()
                            .await
                        }
                    }
                }
            }),
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        ));
        let ResultDelivery::Batch(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("the first batch must cross the visibility boundary");
        };
        let bytes = delivery.decoded_bytes();
        delivery
            .reserve_protocol(&control.resources(), bytes)
            .unwrap()
            .begin_protocol_write(bytes)
            .unwrap()
            .complete()
            .unwrap();
        assert!(actor.snapshot().await.unwrap().output_visible);

        status_sender
            .publish_attempt_observation(
                root_finished.clone(),
                AcceptedAttemptFailure::DerivedPending,
            )
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut pump)
                .await
                .is_err(),
            "a derived cause must freeze visible output without choosing its final error"
        );

        let authoritative = TerminationDetail::Failed(TaskFailure::new(
            TaskFailureCategory::Execution,
            SafeDetail::new("authoritative failure after visible output").unwrap(),
        ));
        status_sender
            .publish_with_attempt_failure(root_finished, authoritative)
            .unwrap();
        let ResultPumpFailure::Concluded(failure) =
            tokio::time::timeout(Duration::from_secs(1), pump)
                .await
                .expect("the authoritative cause must finish the visible stream")
                .unwrap()
                .unwrap_err()
        else {
            panic!("visible output fixes the authoritative failure as logical failure");
        };
        assert_eq!(failure.conclusion(), LogicalConclusion::Failed);
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        let Err(stream_error) = stream.next().await else {
            panic!("authoritative failure must terminate the visible result stream");
        };
        assert!(
            stream_error
                .message()
                .contains("authoritative failure after visible output")
        );
        drop(status_sender);
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn final_ack_cannot_overtake_failure_published_by_the_same_fetch_poll() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(22).await;
        let seal_port = Arc::new(TestSuccessSealPort::default());
        let (status_sender, statuses) = accepted_root_status_projection_with_seal_port(
            root,
            Arc::clone(&seal_port) as Arc<dyn AcceptedRootSuccessSealPort>,
        );
        let root_finished = finished(root);
        status_sender.publish(root_finished.clone()).unwrap();
        let status_sender = Arc::new(Mutex::new(Some(status_sender)));
        let fetch_index = Arc::new(AtomicUsize::new(0));
        let authoritative = TerminationDetail::Failed(TaskFailure::new(
            TaskFailureCategory::Execution,
            SafeDetail::new("failure published while final ACK became ready").unwrap(),
        ));
        let binding = RootResultPumpBinding::new(decode_runtime(), {
            let status_sender = Arc::clone(&status_sender);
            let fetch_index = Arc::clone(&fetch_index);
            move |_| {
                let status_sender = Arc::clone(&status_sender);
                let root_finished = root_finished.clone();
                let authoritative = authoritative.clone();
                let index = fetch_index.fetch_add(1, Ordering::SeqCst);
                async move {
                    if index == 0 {
                        Ok(RootResultFetchOutcome::EndPending(
                            ResultPacketSequence::new(0),
                        ))
                    } else {
                        status_sender
                            .lock()
                            .unwrap()
                            .take()
                            .expect("the final fetch owns the status publisher")
                            .publish_with_attempt_failure(root_finished, authoritative)
                            .unwrap();
                        Ok(RootResultFetchOutcome::EndAcknowledged(
                            ResultPacketSequence::new(0),
                        ))
                    }
                }
            }
        });
        let ResultPumpFailure::DecisionPending(failure) = run_root_result_pump(
            permit,
            root,
            scope,
            control.resources(),
            result_schema(),
            binding,
            statuses,
            completed_native_terminal(),
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        )
        .await
        .unwrap_err() else {
            panic!("a final ACK cannot commit success ahead of its same-poll failure");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        assert!(
            failure
                .error()
                .message()
                .contains("failure published while final ACK became ready")
        );
        assert_eq!(seal_port.requests.lock().unwrap().len(), 1);
        assert_eq!(
            failure.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn native_failure_without_a_root_status_returns_the_attempt_decision() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(24).await;
        let (_status_sender, statuses) = accepted_root_status_projection(root);
        let (terminal_sender, terminal_source) = native_attempt_terminal_channel();
        terminal_sender.publish(failed_native_terminal(
            "Native run failed before root status",
        ));
        let ResultPumpFailure::DecisionPending(failure) = run_root_result_pump(
            permit,
            root,
            scope,
            control.resources(),
            result_schema(),
            RootResultPumpBinding::new(decode_runtime(), |_| async {
                std::future::pending::<Result<RootResultFetchOutcome, RootResultFetchFailure>>()
                    .await
            }),
            statuses,
            terminal_source,
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        )
        .await
        .unwrap_err() else {
            panic!("Native failure must retain the pre-visibility attempt decision");
        };
        assert_eq!(
            failure.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert_eq!(
            failure.error().message(),
            "Native run failed before root status"
        );
        assert_eq!(
            failure.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn native_failure_published_by_a_ready_fetch_precedes_batch_delivery() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(25).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let (terminal_sender, terminal_source) = native_attempt_terminal_channel();
        let terminal_sender = Arc::new(Mutex::new(Some(terminal_sender)));
        let decode_calls = Arc::new(AtomicUsize::new(0));
        let binding = RootResultPumpBinding::new(decode_runtime(), {
            let terminal_sender = Arc::clone(&terminal_sender);
            let decode_calls = Arc::clone(&decode_calls);
            move |_| {
                terminal_sender
                    .lock()
                    .unwrap()
                    .take()
                    .expect("the first fetch owns the Native terminal sender")
                    .publish(failed_native_terminal(
                        "Native run failed while a result packet became ready",
                    ));
                let packet = packet(0, Arc::clone(&decode_calls));
                async move { Ok(RootResultFetchOutcome::Ready(packet)) }
            }
        });
        let ResultPumpFailure::DecisionPending(failure) = run_root_result_pump(
            permit,
            root,
            scope,
            control.resources(),
            result_schema(),
            binding,
            statuses,
            terminal_source,
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        )
        .await
        .unwrap_err() else {
            panic!("Native failure must win the same-poll packet race");
        };
        assert_eq!(decode_calls.load(Ordering::SeqCst), 0);
        assert!(!actor.snapshot().await.unwrap().output_visible);
        assert_eq!(
            failure.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        drop(stream);
        drop(actor);
        drop(owner);
    }

    #[tokio::test]
    async fn native_failure_published_by_final_ack_precedes_success() {
        let Harness {
            control,
            scope,
            actor,
            owner,
            permit,
            mut stream,
            root,
        } = harness(26).await;
        let seal_port = Arc::new(TestSuccessSealPort::default());
        let (status_sender, statuses) = accepted_root_status_projection_with_seal_port(
            root,
            Arc::clone(&seal_port) as Arc<dyn AcceptedRootSuccessSealPort>,
        );
        status_sender.publish(finished(root)).unwrap();
        let (terminal_sender, terminal_source) = native_attempt_terminal_channel();
        let terminal_sender = Arc::new(Mutex::new(Some(terminal_sender)));
        let fetch_index = Arc::new(AtomicUsize::new(0));
        let binding = RootResultPumpBinding::new(decode_runtime(), {
            let terminal_sender = Arc::clone(&terminal_sender);
            let fetch_index = Arc::clone(&fetch_index);
            move |_| {
                let index = fetch_index.fetch_add(1, Ordering::SeqCst);
                if index == 1 {
                    terminal_sender
                        .lock()
                        .unwrap()
                        .take()
                        .expect("the final ACK owns the Native terminal sender")
                        .publish(failed_native_terminal(
                            "Native run failed while final ACK became ready",
                        ));
                }
                async move {
                    Ok(if index == 0 {
                        RootResultFetchOutcome::EndPending(ResultPacketSequence::new(0))
                    } else {
                        RootResultFetchOutcome::EndAcknowledged(ResultPacketSequence::new(0))
                    })
                }
            }
        });
        let ResultPumpFailure::DecisionPending(failure) = run_root_result_pump(
            permit,
            root,
            scope,
            control.resources(),
            result_schema(),
            binding,
            statuses,
            terminal_source,
            MaxWait::new(Duration::from_secs(1)).unwrap(),
            ResultByteLimit::new(1 << 12).unwrap(),
        )
        .await
        .unwrap_err() else {
            panic!("Native failure must win the same-poll success race");
        };
        assert_eq!(
            failure.error().message(),
            "Native run failed while final ACK became ready"
        );
        assert!(seal_port.requests.lock().unwrap().is_empty());
        assert_eq!(
            failure.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        drop(status_sender);
        drop(stream);
        drop(actor);
        drop(owner);
    }
}
