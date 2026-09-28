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

//! The native carrier of the frontend task protocol.
//!
//! Two independent channels live here. [`NativeTaskOperationSink`] is the
//! mutation path: it encodes one released [`DispatchBatch`], sends exactly one
//! `ApplyTaskOperations` to the batch's own backend, and hands every receipt
//! back through [`TaskAckIntake`] in request order.
//! [`TaskStatusSubscriber`] is the observation path: one `SubscribeTaskStatus`
//! stream per query context, resubscribed by cursor inside a bounded error
//! budget.
//!
//! The two are deliberately unable to reach each other. The observation path
//! holds a [`StatusIntakeHandle`] and nothing else, so it can enqueue a
//! snapshot and wake the runner but cannot create a task, complete a stage,
//! settle an operation, or renew a lease. Nothing here renews a lease at all:
//! a renewal is minted by the query context owner from its own clock and
//! reaches the wire as an ordinary operation, so a healthy transport can never
//! keep an unhealthy query alive and a broken observation channel can never
//! stop a renewal.
//!
//! Both paths reuse the role's one native client stack — the cached channel,
//! the deployment JWT interceptor, and the typed channel-acquisition boundary
//! of [`super::transport`]. There is no second client here.
#![allow(
    dead_code,
    reason = "The native task protocol is not routed into production yet; the coordinator cutover constructs this carrier."
)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use once_cell::sync::Lazy;
use prometheus::{
    HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry,
};

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::{
    AcquireQueryContextAdmissionTicket, OperationKind, OperationOutcome, OperationShape,
    QueryContextConvergenceCursor, QueryContextConvergenceReceipt, QueryContextRef, TaskIdentity,
    TaskOperationId, TaskStatusCursor, UpdateQueryContext,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::catalog::CatalogSet;
use novarocks_proto_models::novarocks as proto;
use novarocks_query_application::coordination::{
    DispatchLane, OperationDispatchResult, WorkerReceiptOutcome,
};
use novarocks_task_codec::TransportBudget;
use novarocks_task_codec::domain as codec_domain;
use novarocks_task_codec::domain::{stored_credential, stored_message};
use novarocks_task_codec::operation as codec;
use novarocks_task_codec::operation::{
    ContextAwareStatusStreamEvent, CoveredStatusStreamEvent, CoveredStatusStreamFact,
    DecodedCoveredSubscription, ReceiptHeader, decode_context_aware_status_event,
    decode_covered_status_event, decode_receipt_batch, encode_abort_query_context,
    encode_acquire_query_context_admission_ticket, encode_advance_query_context_domain,
    encode_cancel_task, encode_context_aware_subscribe_task_status, encode_control_operation_batch,
    encode_covered_subscribe_task_status, encode_create_task, encode_establish_query_context,
    encode_operation_batch, encode_release_query_context, encode_renew_lease, encode_update_task,
};
use novarocks_types::identity::BackendProcessId;

use novarocks_execution::task_execution::operation::QueryContextReceipt;

use crate::task_execution::context_convergence::{
    ContextConvergenceIntakeHandle, ContextConvergencePublishError,
};
use crate::task_execution::intent::{
    AckPayload, DispatchBatch, OperationAcknowledgement, OperationIntent,
    TaskOperationQueueAdmission, TaskOperationQueuePermit, TaskOperationSink, TaskOperationSubmit,
};
use crate::task_execution::status_intake::{
    ObservationFrame, ObservationIntake, ObservationPublisher, StatusEvent, StatusIntakeAdmission,
    StatusIntakeHandle, StatusIntakeWake,
};

use super::data_runtime::FrontendDataRuntime;
use super::transport::{ChannelAcquisitionError, Client};
use super::transport_supervisor::{
    NativeTransportBackpressure, NativeTransportEncodingPermit, NativeTransportLane,
    NativeTransportReadyWake, NativeTransportWaiter,
};

/// How long a lost subscription waits before its next attempt, per failure in
/// the current run.
///
/// The error budget alone bounds the number of attempts; this bounds their
/// rate, so a flapping backend cannot spend the whole budget inside one tick.
const RESUBSCRIBE_BACKOFF_STEP: Duration = Duration::from_millis(100);
const COVERED_STREAM_LIVENESS_WINDOW: Duration = Duration::from_secs(5);

fn covered_reconnect_backoff(failures: u32, jitter_sample: u64) -> Duration {
    let base_ms = 100_u64
        .saturating_mul(1_u64 << failures.saturating_sub(1).min(4))
        .min(1_000);
    let percent = 80 + jitter_sample % 41;
    Duration::from_millis(base_ms * percent / 100)
}

// ---------------------------------------------------------------------------
// The codec content a neutral intent cannot carry
// ---------------------------------------------------------------------------

/// The five typed messages one establish carries.
///
/// Boxed inside [`OperationWireContent`]: an establish is by far the largest
/// request in the protocol, and every other variant would otherwise pay for
/// its size.
pub(crate) struct EstablishWireContent {
    pub(crate) catalog_set: CatalogSet,
    pub(crate) initial_runtime_filter: proto::RuntimeFilterContribution,
    pub(crate) initial_credential: proto::QueryContextCredentialDomain,
    pub(crate) query_options: proto::QueryOptions,
    pub(crate) native_compatibility_id: Option<proto::NativeCompatibilityId>,
}

/// Encodes one intent, projecting its own payloads back to the wire.
///
/// Nothing is handed in from outside: an intent carries its payloads behind a
/// fingerprint, and the codec that produced them is the only thing that can
/// recover them. Query options come from the immutable establish intent. The
/// compatibility identity still comes from the sink because it belongs to the
/// attempt-wide wire island rather than to the query-context owner.
fn encode_operation(
    intent: &OperationIntent,
    attempt: &AttemptWireFacts,
) -> Result<proto::TaskOperation, String> {
    match intent {
        OperationIntent::AcquireQueryContextAdmissionTicket(request) => {
            Ok(encode_acquire_query_context_admission_ticket(*request))
        }
        OperationIntent::EstablishQueryContext(request) => {
            encode_establish_query_context_operation(request, attempt)
        }
        // The two carriers were frozen once; a send only wraps them in this
        // operation's envelope.
        OperationIntent::CreateTask(request) => Ok(encode_create_task(
            request.envelope(),
            request.parts().fragment().content(),
            request.parts().metadata(),
        )),
        OperationIntent::UpdateTask(request) => {
            let domains = encode_task_domains(request.domains())?;
            Ok(encode_update_task(request, domains))
        }
        OperationIntent::UpdateQueryContext(request) => {
            encode_query_context_operation(request, attempt)
        }
        OperationIntent::CancelTask(request) => Ok(encode_cancel_task(*request)),
        OperationIntent::QuiesceQueryContext(request) => {
            Ok(codec::encode_quiesce_query_context(*request))
        }
        OperationIntent::AbortQueryContext(request) => Ok(encode_abort_query_context(*request)),
        OperationIntent::ReleaseQueryContext(request) => Ok(encode_release_query_context(*request)),
        // Both reads are their own RPC with their own response shape, so they
        // have no batch receipt to be answered by and can never be encoded as
        // a batch item.
        OperationIntent::FetchTaskDynamicFilters(_) | OperationIntent::GetFinalTaskInfo(_) => {
            Err(format!(
                "{} is a separate RPC and has no operation-batch encoding",
                intent.kind()
            ))
        }
    }
}

/// The attempt-level compatibility fact an establish carries outside its
/// neutral intent.
#[derive(Clone, Debug)]
pub(crate) struct AttemptWireFacts {
    pub(crate) native_compatibility_id: novarocks_types::NativeCompatibilityId,
}

fn encode_task_domains(
    domains: &[novarocks_execution::task_execution::operation::TaskDomainUpdate],
) -> Result<Vec<proto::TaskDomainUpdate>, String> {
    domains
        .iter()
        .map(|domain| {
            codec_domain::encode_neutral_task_domain(domain, FieldPath::root("initial_domains"))
                .map_err(|error| error.to_string())
        })
        .collect()
}

fn encode_query_context_operation(
    request: &UpdateQueryContext,
    attempt: &AttemptWireFacts,
) -> Result<proto::TaskOperation, String> {
    match request {
        UpdateQueryContext::Establish(establish) => {
            let catalog_set = stored_message::<CatalogSet>(establish.catalog_binding().as_ref())
                .ok_or("establish catalog binding is not a codec-produced catalog set")?;
            let filter = stored_message::<proto::RuntimeFilterContribution>(
                establish.initial_runtime_filter().as_ref(),
            )
            .ok_or("establish runtime filter is not a codec-produced contribution")?;
            let query_options =
                stored_message::<proto::QueryOptions>(establish.query_options().as_ref())
                    .ok_or("establish query options are not codec-produced")?;
            let credential = stored_credential(establish.initial_credential().material().as_ref())
                .ok_or("establish credential is not codec-produced material")?;
            Ok(encode_establish_query_context(
                establish,
                catalog_set.clone(),
                filter.clone(),
                proto::QueryContextCredentialDomain {
                    lease_id: establish.initial_credential().lease_id().get(),
                    epoch: establish.initial_credential().epoch().get(),
                    descriptors: credential.descriptors().to_vec(),
                },
                *query_options,
                attempt.native_compatibility_id,
            ))
        }
        UpdateQueryContext::AdvanceDomain(advance) => {
            let domain = codec_domain::encode_neutral_query_context_domain(
                advance.domain(),
                FieldPath::root("advance_domain"),
            )
            .map_err(|error| error.to_string())?;
            Ok(encode_advance_query_context_domain(advance, domain))
        }
        UpdateQueryContext::RenewLease(renew) => Ok(encode_renew_lease(renew)),
    }
}

fn encode_establish_query_context_operation(
    establish: &novarocks_execution::task_execution::EstablishQueryContext,
    attempt: &AttemptWireFacts,
) -> Result<proto::TaskOperation, String> {
    let catalog_set = stored_message::<CatalogSet>(establish.catalog_binding().as_ref())
        .ok_or("establish catalog binding is not a codec-produced catalog set")?;
    let filter = stored_message::<proto::RuntimeFilterContribution>(
        establish.initial_runtime_filter().as_ref(),
    )
    .ok_or("establish runtime filter is not a codec-produced contribution")?;
    let query_options = stored_message::<proto::QueryOptions>(establish.query_options().as_ref())
        .ok_or("establish query options are not codec-produced")?;
    let credential = stored_credential(establish.initial_credential().material().as_ref())
        .ok_or("establish credential is not codec-produced material")?;
    Ok(encode_establish_query_context(
        establish,
        catalog_set.clone(),
        filter.clone(),
        proto::QueryContextCredentialDomain {
            lease_id: establish.initial_credential().lease_id().get(),
            epoch: establish.initial_credential().epoch().get(),
            descriptors: credential.descriptors().to_vec(),
        },
        *query_options,
        attempt.native_compatibility_id,
    ))
}

/// Whether an owner consumes this kind's acknowledgement body.
///
/// `CancelTask` is deliberately absent even though its receipt carries a task
/// status on the wire. Per-task snapshots have exactly one publisher — the
/// status subscription — and reading the same fact from a second path would
/// let two producers publish status with no ordering between them, so a
/// cancel's status is observed through the subscription like every other.
const fn consumes_ack_body(kind: OperationKind) -> bool {
    matches!(
        kind,
        OperationKind::AcquireQueryContextAdmissionTicket
            | OperationKind::CreateTask
            | OperationKind::UpdateTask
            | OperationKind::UpdateQueryContext
            | OperationKind::QuiesceQueryContext
            | OperationKind::AbortQueryContext
            | OperationKind::ReleaseQueryContext
    )
}

const fn is_applied(outcome: OperationOutcome) -> bool {
    matches!(
        outcome,
        OperationOutcome::Accepted | OperationOutcome::Idempotent
    )
}

/// Whether this exact operation result must carry its typed acknowledgement.
///
/// Abort differs from the other mutations: losing to an already-closing
/// context is a successful stand-down observation even when the operation was
/// not newly applied. Its context state is therefore part of the mandatory
/// receipt for every legal closing outcome.
const fn consumes_ack_body_for_outcome(kind: OperationKind, outcome: OperationOutcome) -> bool {
    if matches!(kind, OperationKind::AbortQueryContext) {
        return matches!(
            outcome,
            OperationOutcome::Accepted
                | OperationOutcome::Idempotent
                | OperationOutcome::ContextTerminalReceipt
                | OperationOutcome::LeaseExpired
                | OperationOutcome::Gone
        );
    }
    is_applied(outcome) && consumes_ack_body(kind)
}

/// Whether this intent is a lease renewal, decided from the neutral command
/// rather than from its kind, which an establish shares.
fn is_lease_renewal(intent: &OperationIntent) -> bool {
    matches!(
        intent,
        OperationIntent::UpdateQueryContext(request)
            if matches!(request.as_ref(), UpdateQueryContext::RenewLease(_))
    )
}

/// One item this transport actually put on the wire.
#[derive(Copy, Clone, Debug)]
struct SentOperation {
    operation_id: TaskOperationId,
    kind: OperationKind,
    establish_context: Option<QueryContextRef>,
    lease_renewal: bool,
    address: AckAddress,
}

/// What an acknowledgement body must be addressed to.
///
/// A receipt that agrees with itself proves nothing. This is the address the
/// frontend actually sent to, so a body naming a different task or context is
/// refused rather than read as this request's proof.
#[derive(Clone, Copy, Debug)]
enum AckAddress {
    Admission(AcquireQueryContextAdmissionTicket),
    Task(TaskIdentity),
    Context(QueryContextRef),
    /// The kind carries no acknowledgement body.
    None,
}

impl AckAddress {
    fn of(intent: &OperationIntent) -> Self {
        match intent {
            OperationIntent::AcquireQueryContextAdmissionTicket(request) => {
                Self::Admission(*request)
            }
            OperationIntent::EstablishQueryContext(request) => Self::Context(request.context()),
            OperationIntent::CreateTask(request) => Self::Task(request.identity()),
            OperationIntent::UpdateTask(request) => Self::Task(request.identity()),
            OperationIntent::CancelTask(request) => Self::Task(request.identity()),
            OperationIntent::QuiesceQueryContext(request) => Self::Context(request.context()),
            OperationIntent::UpdateQueryContext(request) => Self::Context(request.context()),
            OperationIntent::AbortQueryContext(request) => Self::Context(request.context()),
            OperationIntent::ReleaseQueryContext(request) => Self::Context(request.context()),
            OperationIntent::FetchTaskDynamicFilters(_) | OperationIntent::GetFinalTaskInfo(_) => {
                Self::None
            }
        }
    }
}

/// Reads one applied receipt's acknowledgement body.
///
/// Called only for an applied outcome of a kind whose owner consumes a
/// receipt. A body that does not match its kind, or that is addressed
/// elsewhere, is an error and never a silently empty payload: an applied
/// create with no receipt is a protocol violation, not a create that carried
/// nothing.
fn decode_ack(
    kind: OperationKind,
    address: AckAddress,
    receipt: &proto::TaskOperationReceipt,
) -> Result<AckPayload, String> {
    let path = || FieldPath::root("apply_task_operations").field("receipts");
    let body = receipt
        .ack
        .as_ref()
        .ok_or_else(|| format!("{kind} requires an acknowledgement body for this outcome"))?;
    match (kind, body, address) {
        (
            OperationKind::AcquireQueryContextAdmissionTicket,
            proto::task_operation_receipt::Ack::QueryContextAdmissionTicket(ack),
            AckAddress::Admission(expected),
        ) => codec::decode_query_context_admission_ticket_ack(ack, expected, path())
            .map(AckPayload::AdmissionTicket)
            .map_err(|error| error.to_string()),
        (
            OperationKind::CreateTask,
            proto::task_operation_receipt::Ack::CreateTask(ack),
            AckAddress::Task(identity),
        ) => codec::decode_create_task_ack(ack, identity, path())
            .map(AckPayload::Create)
            .map_err(|error| error.to_string()),
        (
            OperationKind::UpdateTask,
            proto::task_operation_receipt::Ack::UpdateTask(ack),
            AckAddress::Task(identity),
        ) => codec::decode_update_task_ack(ack, identity, path())
            .map(AckPayload::Update)
            .map_err(|error| error.to_string()),
        (
            OperationKind::UpdateQueryContext | OperationKind::AbortQueryContext,
            proto::task_operation_receipt::Ack::QueryContext(ack),
            AckAddress::Context(context),
        ) => codec::decode_query_context_ack(ack, context, path())
            .map(|(receipt, cause)| {
                if let Some(cause) = cause {
                    // The state is what the owner acts on; the cause is why,
                    // and it has no slot on a context receipt.
                    tracing::info!(
                        %context,
                        ?cause,
                        "query context acknowledgement reports a termination cause"
                    );
                }
                AckPayload::Context(receipt)
            })
            .map_err(|error| error.to_string()),
        (
            OperationKind::QuiesceQueryContext,
            proto::task_operation_receipt::Ack::QuiesceQueryContext(ack),
            AckAddress::Context(context),
        ) => {
            let decoded =
                codec::decode_quiesce_ack(ack, path()).map_err(|error| error.to_string())?;
            if decoded.context() != context {
                return Err("a quiesce acknowledgement names a different context".to_owned());
            }
            Ok(AckPayload::Quiesce(decoded))
        }
        (
            OperationKind::ReleaseQueryContext,
            proto::task_operation_receipt::Ack::ReleaseQueryContext(ack),
            AckAddress::Context(context),
        ) => {
            let decoded =
                codec::decode_release_ack(ack, path()).map_err(|error| error.to_string())?;
            let acked = decoded.context;
            let cause = decoded.termination_cause;
            if acked != context {
                return Err(
                    "a release acknowledgement names a different context than the request"
                        .to_owned(),
                );
            }
            if let Some(cause) = cause {
                // A release that answers on a terminated context reports why.
                tracing::info!(
                    %context,
                    ?cause,
                    "release acknowledgement reports a termination cause"
                );
            }
            Ok(AckPayload::Release {
                receipt: QueryContextReceipt::new(acked, decoded.state),
                outcome: decoded.outcome,
                runtime_filter: decoded.runtime_filter,
            })
        }
        (kind, _, _) => Err(format!(
            "an applied {kind} carries an acknowledgement body of another kind"
        )),
    }
}

// ---------------------------------------------------------------------------
// Acknowledgement intake
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct TaskAckIntakeInner {
    queue: Mutex<Vec<TaskOperationIntakeEvent>>,
    wake: Arc<dyn StatusIntakeWake>,
}

/// Transport facts and receipts share one local order before reaching the
/// serial Context owner. An acknowledgement cannot overtake its send fact.
#[derive(Debug)]
pub(crate) enum TaskOperationIntakeEvent {
    EstablishSendStarted {
        operation_id: TaskOperationId,
        context: QueryContextRef,
    },
    Acknowledgement(OperationAcknowledgement),
}

/// The only handle the send path holds.
///
/// It can enqueue transport facts and settled acknowledgements and wake the
/// runner. It has no mutable Context owner, task, stage, or lease; every fact
/// is interpreted on the serial runner.
#[derive(Clone, Debug)]
pub(crate) struct TaskAckIntakeHandle {
    inner: Arc<TaskAckIntakeInner>,
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
            .push(TaskOperationIntakeEvent::Acknowledgement(ack));
        self.inner.wake.wake();
    }

    pub(crate) fn publish_establish_send_started(
        &self,
        operation_id: TaskOperationId,
        context: QueryContextRef,
    ) {
        self.inner
            .queue
            .lock()
            .expect("task acknowledgement queue")
            .push(TaskOperationIntakeEvent::EstablishSendStarted {
                operation_id,
                context,
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
    inner: Arc<TaskAckIntakeInner>,
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
                TaskOperationIntakeEvent::Acknowledgement(ack) => Some(ack),
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
fn classify_apply_status(status: &tonic::Status) -> OperationDispatchResult {
    match status.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown => OperationDispatchResult::TransportUnknown,
        // A gRPC failure is never a Worker receipt. Native ingress marks its
        // pre-admission category in metadata; an unmarked limit fails closed.
        tonic::Code::ResourceExhausted => OperationDispatchResult::IngressRejected(
            match status
                .metadata()
                .get("x-novarocks-ingress-rejection")
                .and_then(|value| value.to_str().ok())
            {
                Some("waiting_capacity") => novarocks_query_application::coordination::IngressRejection::WaitingCapacity,
                Some("body_limit") => novarocks_query_application::coordination::IngressRejection::BodyLimit,
                _ => novarocks_query_application::coordination::IngressRejection::UnclassifiedCapacity,
            },
        ),
        _ => OperationDispatchResult::NonWorkerRejected,
    }
}

/// Classifies a channel acquisition failure by type.
///
/// URI and connector construction are deterministic local failures that
/// resending cannot repair. A completed connector that then cannot dial or
/// establish its HTTP/2 stream leaves the remote outcome unknown.
fn classify_channel_error(error: &ChannelAcquisitionError) -> OperationDispatchResult {
    match error {
        ChannelAcquisitionError::Fatal(_) => OperationDispatchResult::NonWorkerRejected,
        ChannelAcquisitionError::RetryableNetwork(_) => OperationDispatchResult::TransportUnknown,
    }
}

const fn worker_result(outcome: OperationOutcome) -> OperationDispatchResult {
    OperationDispatchResult::WorkerReceipt(WorkerReceiptOutcome::from_contract(outcome))
}

/// Whether a status stream rejection is fatal to the attempt.
///
/// A subscription is an observation channel: a lost stream is recovered by
/// resubscribing from the cursors, and only a typed rejection saying the
/// subscription itself is illegal or unauthorized can never be repaired that
/// way.
fn subscription_rejection_is_fatal(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::InvalidArgument
            | tonic::Code::DataLoss
            | tonic::Code::Unimplemented
            | tonic::Code::PermissionDenied
            | tonic::Code::Unauthenticated
            | tonic::Code::FailedPrecondition
            | tonic::Code::NotFound
    )
}

// ---------------------------------------------------------------------------
// The operation sink
// ---------------------------------------------------------------------------

/// One frozen backend process this attempt may address.
#[derive(Clone)]
struct TaskBackendTarget {
    client: Client,
}

fn freeze_targets(
    backends: &[(BackendProcessId, RuntimeEndpoint)],
    data_runtime: &FrontendDataRuntime,
) -> Result<BTreeMap<BackendProcessId, TaskBackendTarget>, String> {
    if backends.is_empty() {
        return Err("the native task transport requires at least one backend".to_owned());
    }
    let mut targets = BTreeMap::new();
    for (process_id, endpoint) in backends {
        let target = TaskBackendTarget {
            client: Client::new(endpoint.native_endpoint().clone(), data_runtime.clone()),
        };
        if targets.insert(*process_id, target).is_some() {
            return Err(format!("duplicate backend process {process_id}"));
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
    targets: BTreeMap<BackendProcessId, TaskBackendTarget>,
    transport: TransportBudget,
    attempt: AttemptWireFacts,
    acks: TaskAckIntakeHandle,
    data_runtime: FrontendDataRuntime,
    transport_waiter: NativeTransportWaiter,
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
        backends: &[(BackendProcessId, RuntimeEndpoint)],
        transport: TransportBudget,
        attempt: AttemptWireFacts,
        acks: TaskAckIntakeHandle,
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, String> {
        if !data_runtime
            .task_transport_supervisor()
            .accepts_transport_budget(transport)
        {
            return Err(
                "attempt task transport budget differs from the process supervisor".to_owned(),
            );
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
            transport_waiter,
        })
    }

    /// Settles one operation locally, without a wire round trip.
    fn settle_locally(
        &self,
        operation_id: TaskOperationId,
        kind: OperationKind,
        lease_renewal: bool,
        outcome: OperationOutcome,
    ) {
        observe_settled(kind, lease_renewal, outcome);
        self.acks.publish(OperationAcknowledgement::new(
            operation_id,
            kind,
            outcome,
            AckPayload::None,
        ));
    }

    fn settle_batch_locally(&self, batch: &DispatchBatch, outcome: OperationOutcome, reason: &str) {
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
    ) -> TaskOperationQueueAdmission {
        let lane = if request.requires_control_progress() {
            NativeTransportLane::Control
        } else {
            NativeTransportLane::Ordinary
        };
        match self
            .data_runtime
            .task_transport_supervisor()
            .try_reserve_queue(
                &self.transport_waiter,
                request.backend_process_id(),
                lane,
                1,
                request.queued_bytes(),
            ) {
            Ok(permit) => TaskOperationQueueAdmission::Admitted(Box::new(permit)),
            Err(NativeTransportBackpressure::Target) => TaskOperationQueueAdmission::TargetFull,
            Err(NativeTransportBackpressure::Process) => TaskOperationQueueAdmission::ProcessFull,
        }
    }

    fn try_submit(&self, batch: DispatchBatch) -> TaskOperationSubmit {
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
        let target = target.clone();
        let supervisor_lane = supervisor_lane(&batch);
        let mut encoding_permit = match self
            .data_runtime
            .task_transport_supervisor()
            .try_reserve_encoding(&self.transport_waiter, backend, supervisor_lane)
        {
            Ok(permit) => permit,
            Err(_) => return TaskOperationSubmit::Backpressured(batch),
        };

        let mut operations = Vec::with_capacity(batch.operations().len());
        let mut sent = Vec::with_capacity(batch.operations().len());
        let mut unencodable = Vec::new();
        for intent in batch.operations() {
            let encoded = encode_operation(intent, &self.attempt);
            match encoded {
                Ok(operation) => {
                    operations.push((is_small_control(intent.shape()), operation));
                    sent.push(SentOperation {
                        operation_id: intent.operation_id(),
                        kind: intent.kind(),
                        establish_context: match intent {
                            OperationIntent::EstablishQueryContext(request) => {
                                Some(request.context())
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
                drop(batch.commit_queue_permits());
                return TaskOperationSubmit::Accepted;
            }
        };
        let encoded_bytes = requests.iter().map(EncodedMethodBatch::encoded_len).sum();
        encoding_permit.shrink_to(encoded_bytes);
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
        // Construct the settlement owner before spawning: the runtime can
        // drop a task before its first poll, and that must still settle every
        // accepted operation before returning its reservations.
        let send = ApplySend {
            client,
            receipts: AcceptedOperationReceipts::new(acks, sent),
            _queue_permits: queue_permits,
            _encoding_permit: encoding_permit,
        };
        self.data_runtime
            .spawn(apply_operations(send, requests, submitted_at));
        TaskOperationSubmit::Accepted
    }
}

fn supervisor_lane(batch: &DispatchBatch) -> NativeTransportLane {
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

fn supervisor_lane_for_intent(intent: &OperationIntent) -> NativeTransportLane {
    if intent.requires_control_progress() {
        NativeTransportLane::Control
    } else {
        NativeTransportLane::Ordinary
    }
}

const fn is_small_control(shape: OperationShape) -> bool {
    match shape {
        OperationShape::RenewQueryExecutionLease
        | OperationShape::CancelTask
        | OperationShape::QuiesceQueryContext
        | OperationShape::AbortQueryContext
        | OperationShape::ReleaseQueryContext => true,
        OperationShape::AcquireQueryContextAdmissionTicket
        | OperationShape::EstablishQueryContext
        | OperationShape::AdvanceQueryContextDomain
        | OperationShape::CreateTask
        | OperationShape::UpdateTask
        | OperationShape::FetchTaskDynamicFilters
        | OperationShape::GetFinalTaskInfo => false,
    }
}

enum EncodedMethodRequest {
    Ordinary(proto::ApplyTaskOperationsRequest),
    Control(proto::ApplyTaskControlOperationsRequest),
}

struct EncodedMethodBatch {
    request: EncodedMethodRequest,
    items: usize,
    deadline: Duration,
}

impl EncodedMethodBatch {
    fn encoded_len(&self) -> usize {
        match &self.request {
            EncodedMethodRequest::Ordinary(request) => prost::Message::encoded_len(request),
            EncodedMethodRequest::Control(request) => prost::Message::encoded_len(request),
        }
    }

    fn expires_at(&self, submitted_at: tokio::time::Instant) -> tokio::time::Instant {
        submitted_at + self.deadline
    }
}

/// Preserve dispatcher order while directing each contiguous run to the
/// method whose closed input grammar accepts it.
fn encode_method_batches(
    operations: Vec<(bool, proto::TaskOperation)>,
    budget: TransportBudget,
) -> Result<Vec<EncodedMethodBatch>, novarocks_proto_codec::ProtocolError> {
    let mut batches = Vec::new();
    let mut iter = operations.into_iter().peekable();
    while let Some((control, first)) = iter.next() {
        let mut run = vec![first];
        while iter
            .peek()
            .is_some_and(|(next_control, _)| *next_control == control)
        {
            run.push(iter.next().expect("peeked operation exists").1);
        }
        let deadline = run
            .iter()
            .filter_map(|operation| operation.envelope.as_ref())
            .map(|envelope| Duration::from_millis(envelope.max_wait_millis))
            .max()
            .unwrap_or_default();
        let items = run.len();
        let request = if control {
            EncodedMethodRequest::Control(encode_control_operation_batch(run, budget)?)
        } else {
            EncodedMethodRequest::Ordinary(encode_operation_batch(run, budget)?)
        };
        batches.push(EncodedMethodBatch {
            request,
            items,
            deadline,
        });
    }
    let item_count = batches.iter().map(|batch| batch.items).sum();
    let encoded_bytes = batches.iter().fold(0usize, |total, batch| {
        total.saturating_add(batch.encoded_len())
    });
    if !budget.batch_fits(item_count, encoded_bytes) {
        return Err(novarocks_proto_codec::ProtocolError::new(
            FieldPath::root("task_operation_method_batches"),
            novarocks_proto_codec::ProtocolErrorKind::OutOfRange,
            "combined operation methods exceed the original batch budget",
        ));
    }
    Ok(batches)
}

fn settle_unencodable(
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

/// Everything one send owns beyond its request.
struct ApplySend {
    client: Client,
    receipts: AcceptedOperationReceipts,
    _queue_permits: Vec<Box<dyn TaskOperationQueuePermit>>,
    _encoding_permit: NativeTransportEncodingPermit,
}

/// First-wins settlement for every operation an accepted send owns.
///
/// Dropping the send future is an unknown transport outcome, including runtime
/// shutdown and task abort. The guard publishes that fact for every operation
/// not already settled before releasing the process reservations, so an owner
/// can never remain in flight merely because its transport future disappeared.
struct AcceptedOperationReceipts {
    acks: TaskAckIntakeHandle,
    pending: VecDeque<SentOperation>,
}

impl AcceptedOperationReceipts {
    fn new(acks: TaskAckIntakeHandle, sent: Vec<SentOperation>) -> Self {
        Self {
            acks,
            pending: sent.into(),
        }
    }

    fn prefix_ids(&self, count: usize) -> Vec<TaskOperationId> {
        self.pending
            .iter()
            .take(count)
            .map(|item| item.operation_id)
            .collect()
    }

    fn prefix_establishes(&self, count: usize) -> Vec<(TaskOperationId, QueryContextRef)> {
        self.pending
            .iter()
            .take(count)
            .filter_map(|item| {
                item.establish_context
                    .map(|context| (item.operation_id, context))
            })
            .collect()
    }

    fn front(&self) -> Option<SentOperation> {
        self.pending.front().copied()
    }

    fn publish_next(&mut self, ack: OperationAcknowledgement) {
        let item = self
            .pending
            .pop_front()
            .expect("an accepted response cannot outnumber its request");
        assert_eq!(
            item.operation_id,
            ack.operation_id(),
            "an accepted response must settle the request at the queue head"
        );
        self.acks.publish(ack);
    }

    fn publish_uniform(&mut self, result: OperationDispatchResult) {
        while let Some(item) = self.pending.pop_front() {
            observe_dispatch_result(item.kind, item.lease_renewal, result);
            self.acks
                .publish(OperationAcknowledgement::from_dispatch_result(
                    item.operation_id,
                    item.kind,
                    result,
                    AckPayload::None,
                ));
        }
    }

    fn publish_prefix_uniform(&mut self, count: usize, result: OperationDispatchResult) {
        for _ in 0..count {
            let item = self
                .pending
                .pop_front()
                .expect("accepted batch count cannot exceed pending operations");
            observe_dispatch_result(item.kind, item.lease_renewal, result);
            self.acks
                .publish(OperationAcknowledgement::from_dispatch_result(
                    item.operation_id,
                    item.kind,
                    result,
                    AckPayload::None,
                ));
        }
    }
}

impl Drop for AcceptedOperationReceipts {
    fn drop(&mut self) {
        if !self.pending.is_empty() {
            tracing::warn!(
                operations = self.pending.len(),
                "accepted task operation send dropped before settlement"
            );
            self.publish_uniform(OperationDispatchResult::TransportUnknown);
        }
    }
}

/// Sends one batch and publishes one acknowledgement per request item.
async fn apply_operations(
    mut send: ApplySend,
    requests: Vec<EncodedMethodBatch>,
    submitted_at: tokio::time::Instant,
) {
    for batch in requests {
        let expires_at = batch.expires_at(submitted_at);
        let establishes = send.receipts.prefix_establishes(batch.items);
        let response = match send_operations(
            &send.client,
            batch.request,
            expires_at,
            &send.receipts.acks,
            &establishes,
        )
        .await
        {
            Ok(response) => response,
            Err(result) => {
                send.receipts.publish_prefix_uniform(batch.items, result);
                continue;
            }
        };

        let ids = send.receipts.prefix_ids(batch.items);
        let headers = match decode_receipt_batch(
            &response,
            &ids,
            FieldPath::root("apply_task_operations_response"),
        ) {
            Ok(headers) => headers,
            Err(error) => {
                // The answer arrived but cannot be attributed to the requests. A
                // short or reordered response is refused whole: guessing which
                // item a receipt belongs to could settle an operation the backend
                // never applied, and resending is not allowed once an answer has
                // been received.
                tracing::warn!(detail = %error, "task operation batch response is unusable");
                observe_refusal(REFUSAL_UNUSABLE_RESPONSE, ids.len());
                send.receipts.publish_prefix_uniform(
                    batch.items,
                    worker_result(OperationOutcome::InvalidStateOrRequest),
                );
                continue;
            }
        };

        for (header, receipt) in headers.iter().zip(response.receipts.iter()) {
            let item = send
                .receipts
                .front()
                .expect("validated receipt count matches the accepted request");
            let ack = acknowledgement(&item, header, receipt);
            send.receipts.publish_next(ack);
        }
    }
}

/// Builds one acknowledgement from one receipt, in its request item's order.
fn acknowledgement(
    item: &SentOperation,
    header: &ReceiptHeader,
    receipt: &proto::TaskOperationReceipt,
) -> OperationAcknowledgement {
    let outcome = header.outcome();
    if !consumes_ack_body_for_outcome(item.kind, outcome) {
        observe_settled(item.kind, item.lease_renewal, outcome);
        return OperationAcknowledgement::worker_receipt(
            item.operation_id,
            item.kind,
            outcome,
            AckPayload::None,
        )
        .with_detail(header.detail().cloned());
    }
    match decode_ack(item.kind, item.address, receipt) {
        Ok(payload) => {
            observe_settled(item.kind, item.lease_renewal, outcome);
            OperationAcknowledgement::worker_receipt(item.operation_id, item.kind, outcome, payload)
                .with_detail(header.detail().cloned())
        }
        Err(detail) => {
            // An operation result that requires a typed acknowledgement but
            // whose body cannot be read is a protocol violation, not an
            // operation that carried nothing.
            tracing::warn!(
                kind = item.kind.as_str(),
                detail,
                "task operation receipt has a required but unreadable acknowledgement"
            );
            observe_refusal(REFUSAL_UNUSABLE_ACK, 1);
            observe_settled(
                item.kind,
                item.lease_renewal,
                OperationOutcome::InvalidStateOrRequest,
            );
            OperationAcknowledgement::worker_receipt(
                item.operation_id,
                item.kind,
                OperationOutcome::InvalidStateOrRequest,
                AckPayload::None,
            )
        }
    }
}

/// One `ApplyTaskOperations` round trip, classified by type.
///
/// The deadline is the longest wait the batch itself requested. A batch that
/// has not answered by then has a genuinely unknown outcome, which is exactly
/// what the retry rule exists for.
async fn send_operations(
    client: &Client,
    request: EncodedMethodRequest,
    expires_at: tokio::time::Instant,
    intake: &TaskAckIntakeHandle,
    establishes: &[(TaskOperationId, QueryContextRef)],
) -> Result<proto::ApplyTaskOperationsResponse, OperationDispatchResult> {
    let (mut grpc, acquired) =
        tokio::time::timeout_at(expires_at, client.grpc_with_channel_identity())
            .await
            .map_err(|_| OperationDispatchResult::TransportUnknown)?
            .map_err(|error| {
                let outcome = classify_channel_error(&error);
                tracing::warn!(detail = %error, "task operation channel acquisition failed");
                outcome
            })?;
    let remaining = expires_at.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        // Nothing was submitted, so nothing was applied. It is still reported
        // as unknown rather than as a rejection, because a caller may only
        // conclude "not applied" from an answer it received.
        client.invalidate_channel_if_current(&acquired);
        return Err(OperationDispatchResult::TransportUnknown);
    }
    let call = async {
        match request {
            EncodedMethodRequest::Ordinary(request) => {
                let mut wire = tonic::Request::new(request);
                wire.set_timeout(remaining);
                let rpc = grpc.apply_task_operations(wire);
                tokio::pin!(rpc);
                let mut started = false;
                std::future::poll_fn(|cx| {
                    let result = rpc.as_mut().poll(cx);
                    if !started {
                        started = true;
                        for &(operation_id, context) in establishes {
                            intake.publish_establish_send_started(operation_id, context);
                        }
                    }
                    result
                })
                .await
            }
            EncodedMethodRequest::Control(request) => {
                let mut wire = tonic::Request::new(request);
                wire.set_timeout(remaining);
                let rpc = grpc.apply_task_control_operations(wire);
                tokio::pin!(rpc);
                let mut started = false;
                std::future::poll_fn(|cx| {
                    let result = rpc.as_mut().poll(cx);
                    if !started {
                        started = true;
                        for &(operation_id, context) in establishes {
                            intake.publish_establish_send_started(operation_id, context);
                        }
                    }
                    result
                })
                .await
            }
        }
    };
    let outcome = match tokio::time::timeout_at(expires_at, call).await {
        Ok(result) => result.map(tonic::Response::into_inner).map_err(|status| {
            let outcome = classify_apply_status(&status);
            tracing::warn!(
                code = ?status.code(),
                detail = status.message(),
                "task operation rpc failed"
            );
            outcome
        }),
        Err(_) => Err(OperationDispatchResult::TransportUnknown),
    };
    if matches!(outcome, Err(OperationDispatchResult::TransportUnknown)) {
        // An unknown outcome may have poisoned this stream. An old request
        // cannot evict a replacement channel already installed by another
        // attempt or by membership reconciliation.
        client.invalidate_channel_if_current(&acquired);
    }
    outcome
}

// ---------------------------------------------------------------------------
// The status subscription
// ---------------------------------------------------------------------------

/// The observable state of one logical subscription.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum SubscriptionState {
    Opening,
    Live,
    /// The stream is gone and the cursors are being replayed.
    Resubscribing,
    /// The bounded resubscription budget ran out.
    BudgetExhausted,
    /// The backend rejected the subscription in a way resubscribing cannot
    /// repair.
    Rejected,
    /// An event addressed a different backend process. An exact process
    /// replacement is fatal to the attempt and is never followed.
    ProcessMismatch,
    /// An event addressed a different query execution on the expected
    /// backend process. It cannot contribute to this context's cursor.
    QueryMismatch,
}

impl SubscriptionState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Opening => "opening",
            Self::Live => "live",
            Self::Resubscribing => "resubscribing",
            Self::BudgetExhausted => "budget_exhausted",
            Self::Rejected => "rejected",
            Self::ProcessMismatch => "process_mismatch",
            Self::QueryMismatch => "query_mismatch",
        }
    }

    /// Whether this state is fatal to the query attempt.
    pub(crate) const fn is_fatal(self) -> bool {
        matches!(
            self,
            Self::BudgetExhausted | Self::Rejected | Self::ProcessMismatch | Self::QueryMismatch
        )
    }
}

/// One running subscription. Dropping it stops the stream.
struct Subscription {
    state: Arc<Mutex<SubscriptionState>>,
    reconciliation: tokio::sync::watch::Sender<TaskCursorReconciliation>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The `SubscribeTaskStatus` client of one attempt.
///
/// One logical subscription per query context, started lazily the first time
/// that context has a task to observe. A new task joins the running stream on
/// its own, so nothing here freezes a task set.
pub(crate) struct TaskStatusSubscriber {
    targets: BTreeMap<BackendProcessId, TaskBackendTarget>,
    intake: StatusIntakeHandle,
    context_convergence: Option<ContextConvergenceIntakeHandle>,
    error_budget: u32,
    data_runtime: FrontendDataRuntime,
    active: Mutex<BTreeMap<QueryContextRef, Subscription>>,
}

impl fmt::Debug for TaskStatusSubscriber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskStatusSubscriber")
            .field("backends", &self.targets.len())
            .field("error_budget", &self.error_budget)
            .finish()
    }
}

impl TaskStatusSubscriber {
    /// Builds the task-only transport retained until the production owner cut
    /// installs the convergence intake in the same atomic change.
    ///
    /// This is a migration-only constructor. New ownership must use
    /// [`Self::new_context_aware`].
    pub(crate) fn new(
        backends: &[(BackendProcessId, RuntimeEndpoint)],
        intake: StatusIntakeHandle,
        error_budget: u32,
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, String> {
        if error_budget == 0 {
            return Err("the status subscription error budget must be nonzero".to_owned());
        }
        Ok(Self {
            targets: freeze_targets(backends, &data_runtime)?,
            intake,
            context_convergence: None,
            error_budget,
            data_runtime,
            active: Mutex::new(BTreeMap::new()),
        })
    }

    /// Builds the single-stream task and query-context observation transport.
    pub(crate) fn new_context_aware(
        backends: &[(BackendProcessId, RuntimeEndpoint)],
        intake: StatusIntakeHandle,
        context_convergence: ContextConvergenceIntakeHandle,
        error_budget: u32,
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, String> {
        if error_budget == 0 {
            return Err("the status subscription error budget must be nonzero".to_owned());
        }
        Ok(Self {
            targets: freeze_targets(backends, &data_runtime)?,
            intake,
            context_convergence: Some(context_convergence),
            error_budget,
            data_runtime,
            active: Mutex::new(BTreeMap::new()),
        })
    }

    /// Starts this context's one subscription if it is not already running.
    pub(crate) fn ensure(
        &self,
        context: QueryContextRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<(), String> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| "task status subscription lock poisoned".to_owned())?;
        if active.contains_key(&context) {
            return Ok(());
        }
        let subscription = self.start(context, cursors)?;
        active.insert(context, subscription);
        Ok(())
    }

    /// Reconciles this context's one subscription to the runner's cursors.
    ///
    /// This is the answer to observation loss the runner saw on its own side,
    /// such as a full intake queue. Loss the stream itself sees is recovered
    /// by the subscription without anyone asking. The bounded watch channel
    /// coalesces repeated reconciliation requests and gives the same loop the
    /// later one-way lifecycle control seam; it never creates a second stream.
    pub(crate) fn resubscribe(
        &self,
        context: QueryContextRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<(), String> {
        let cursors = task_cursor_map(context, cursors)?;
        let mut active = self
            .active
            .lock()
            .map_err(|_| "task status subscription lock poisoned".to_owned())?;
        if let Some(subscription) = active.get(&context) {
            subscription.reconciliation.send_modify(|current| {
                current.generation = current
                    .generation
                    .checked_add(1)
                    .expect("task cursor reconciliation generation exhausted");
                current.cursors.clone_from(&cursors);
            });
        } else {
            active.insert(context, self.start_with_cursors(context, cursors)?);
        }
        observe_resubscribe();
        Ok(())
    }

    /// Stops this context's subscription.
    pub(crate) fn stop(&self, context: QueryContextRef) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&context);
        }
    }

    /// What this context's subscription is currently doing.
    pub(crate) fn state(&self, context: QueryContextRef) -> Option<SubscriptionState> {
        let active = self.active.lock().ok()?;
        let subscription = active.get(&context)?;
        subscription.state.lock().ok().map(|state| *state)
    }

    fn start(
        &self,
        context: QueryContextRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<Subscription, String> {
        self.start_with_cursors(context, task_cursor_map(context, cursors)?)
    }

    fn start_with_cursors(
        &self,
        context: QueryContextRef,
        cursors: BTreeMap<TaskIdentity, TaskStatusCursor>,
    ) -> Result<Subscription, String> {
        let target = self
            .targets
            .get(&context.backend_process_id())
            .ok_or_else(|| {
                format!(
                    "query context {} addresses a backend process outside the frozen topology",
                    context.backend_process_id()
                )
            })?;
        let state = Arc::new(Mutex::new(SubscriptionState::Opening));
        let (reconciliation, receiver) = tokio::sync::watch::channel(TaskCursorReconciliation {
            generation: 0,
            cursors,
        });
        let task = self.data_runtime.spawn(run_subscription(
            target.client.clone(),
            context,
            receiver,
            self.intake.clone(),
            self.context_convergence.clone(),
            self.error_budget,
            Arc::clone(&state),
        ));
        Ok(Subscription {
            state,
            reconciliation,
            task,
        })
    }
}

#[derive(Clone, Debug)]
struct TaskCursorReconciliation {
    generation: u64,
    cursors: BTreeMap<TaskIdentity, TaskStatusCursor>,
}

fn task_cursor_map(
    context: QueryContextRef,
    cursors: Vec<TaskStatusCursor>,
) -> Result<BTreeMap<TaskIdentity, TaskStatusCursor>, String> {
    let count = cursors.len();
    let cursors = cursors
        .into_iter()
        .map(|cursor| {
            let identity = cursor.identity();
            if identity.query_execution_id() != context.query_execution_id()
                || identity.backend_process_id() != context.backend_process_id()
            {
                return Err(format!(
                    "task status cursor {identity} is outside query context {context}"
                ));
            }
            Ok((identity, cursor))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    if cursors.len() != count {
        return Err("task status reconciliation contains duplicate task cursors".to_owned());
    }
    Ok(cursors)
}

fn set_state(state: &Mutex<SubscriptionState>, next: SubscriptionState) {
    if let Ok(mut current) = state.lock() {
        *current = next;
    }
    observe_subscription_state(next);
}

/// The subscription loop of one query context.
///
/// A dropped stream is never a task failure: the backend process is intact and
/// still running the query, so the loop reports observation loss and
/// resubscribes from the cursors it holds. It never cancels a task, never
/// settles an operation, and never touches a lease.
async fn run_subscription(
    client: Client,
    context: QueryContextRef,
    mut reconciliation: tokio::sync::watch::Receiver<TaskCursorReconciliation>,
    intake: StatusIntakeHandle,
    context_convergence: Option<ContextConvergenceIntakeHandle>,
    error_budget: u32,
    state: Arc<Mutex<SubscriptionState>>,
) {
    let initial = reconciliation.borrow_and_update().clone();
    let mut reconciliation_generation = initial.generation;
    let mut cursors = initial.cursors;
    let mut context_cursor = context_convergence
        .as_ref()
        .map(|_| QueryContextConvergenceCursor::unobserved(context));
    let mut failures = 0_u32;
    'subscription: loop {
        let opened = tokio::select! {
            changed = reconciliation.changed() => {
                if changed.is_err() {
                    return;
                }
                apply_task_cursor_reconciliation(
                    &mut reconciliation,
                    &mut reconciliation_generation,
                    &mut cursors,
                );
                set_state(&state, SubscriptionState::Resubscribing);
                continue 'subscription;
            }
            opened = open_subscription(&client, context, &cursors, context_cursor) => opened,
        };
        match opened {
            Ok(mut stream) => {
                set_state(&state, SubscriptionState::Live);
                let mut delivered = false;
                loop {
                    let message = tokio::select! {
                        changed = reconciliation.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            apply_task_cursor_reconciliation(
                                &mut reconciliation,
                                &mut reconciliation_generation,
                                &mut cursors,
                            );
                            set_state(&state, SubscriptionState::Resubscribing);
                            continue 'subscription;
                        }
                        message = stream.message() => message,
                    };
                    match message {
                        Ok(Some(event)) => {
                            match observe_stream_event(
                                context,
                                &event,
                                &mut cursors,
                                &intake,
                                context_convergence.as_ref(),
                                &mut context_cursor,
                            )
                            .await
                            {
                                Ok(StreamObservation::Delivered) => delivered = true,
                                Ok(StreamObservation::AwaitTaskReconciliation) => {
                                    set_state(&state, SubscriptionState::Resubscribing);
                                    if reconciliation.changed().await.is_err() {
                                        return;
                                    }
                                    apply_task_cursor_reconciliation(
                                        &mut reconciliation,
                                        &mut reconciliation_generation,
                                        &mut cursors,
                                    );
                                    continue 'subscription;
                                }
                                Err(next) => {
                                    set_state(&state, next);
                                    intake.note_observation_incomplete();
                                    return;
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(status) => {
                            if subscription_rejection_is_fatal(&status) {
                                tracing::warn!(
                                    code = ?status.code(),
                                    detail = status.message(),
                                    "SubscribeTaskStatus stream rejected"
                                );
                                set_state(&state, SubscriptionState::Rejected);
                                intake.note_observation_incomplete();
                                return;
                            }
                            break;
                        }
                    }
                }
                if delivered {
                    // A stream that actually observed something is not part of
                    // the previous failure run. A stream that opened and
                    // delivered nothing is, so a flapping backend cannot reset
                    // the budget forever.
                    failures = 0;
                }
            }
            Err(status) if subscription_rejection_is_fatal(&status) => {
                tracing::warn!(
                    code = ?status.code(),
                    detail = status.message(),
                    "SubscribeTaskStatus subscription rejected"
                );
                set_state(&state, SubscriptionState::Rejected);
                intake.note_observation_incomplete();
                return;
            }
            Err(_) => {}
        }
        // The observation channel is gone while the query is not. Telling the
        // runner is the whole recovery: the backend's tasks are untouched, and
        // no lease and no task terminal decision is involved.
        intake.note_observation_loss();
        failures += 1;
        if failures >= error_budget {
            set_state(&state, SubscriptionState::BudgetExhausted);
            return;
        }
        set_state(&state, SubscriptionState::Resubscribing);
        observe_resubscribe();
        tokio::time::sleep(RESUBSCRIBE_BACKOFF_STEP * failures).await;
    }
}

fn apply_task_cursor_reconciliation(
    reconciliation: &mut tokio::sync::watch::Receiver<TaskCursorReconciliation>,
    generation: &mut u64,
    cursors: &mut BTreeMap<TaskIdentity, TaskStatusCursor>,
) {
    let latest = reconciliation.borrow_and_update();
    assert!(
        latest.generation > *generation,
        "task cursor reconciliation generations are strictly increasing"
    );
    *generation = latest.generation;
    cursors.clone_from(&latest.cursors);
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum StreamObservation {
    Delivered,
    AwaitTaskReconciliation,
}

async fn observe_stream_event(
    context: QueryContextRef,
    event: &proto::TaskStatusStreamEvent,
    cursors: &mut BTreeMap<TaskIdentity, TaskStatusCursor>,
    intake: &StatusIntakeHandle,
    context_convergence: Option<&ContextConvergenceIntakeHandle>,
    context_cursor: &mut Option<QueryContextConvergenceCursor>,
) -> Result<StreamObservation, SubscriptionState> {
    let decoded =
        decode_context_aware_status_event(event, FieldPath::root("task_status_stream_event"))
            .map_err(|error| {
                tracing::warn!(detail = %error, "SubscribeTaskStatus event is malformed");
                SubscriptionState::Rejected
            })?;
    match decoded {
        ContextAwareStatusStreamEvent::Status(status) => {
            let identity = status.identity();
            let version = status.version();
            let cursor = cursors
                .get(&identity)
                .copied()
                .unwrap_or_else(|| TaskStatusCursor::unobserved(identity));
            if let Some(current) = cursor.current_version() {
                if version < current {
                    tracing::warn!(
                        task = %identity,
                        current_version = current.get(),
                        event_version = version.get(),
                        "SubscribeTaskStatus event would move its task cursor backwards"
                    );
                    return Err(SubscriptionState::Rejected);
                }
                if version == current {
                    return Ok(StreamObservation::Delivered);
                }
            }
            observe_task_event(
                context,
                identity,
                StatusEvent::Published(status),
                Some(cursor.advanced_to(version)),
                cursors,
                intake,
            )
        }
        ContextAwareStatusStreamEvent::Gone(identity) => observe_task_event(
            context,
            identity,
            StatusEvent::Gone(identity),
            None,
            cursors,
            intake,
        ),
        ContextAwareStatusStreamEvent::ContextConvergence(receipt) => {
            observe_context_convergence(context, receipt, context_convergence, context_cursor).await
        }
    }
}

/// Enqueues one observed event and advances its task's cursor only after the
/// bounded intake retained it.
///
/// This is everything the receive path may do. It classifies nothing, opens no
/// edge, completes no stage, and settles no operation.
fn observe_task_event(
    context: QueryContextRef,
    identity: TaskIdentity,
    event: StatusEvent,
    next_cursor: Option<TaskStatusCursor>,
    cursors: &mut BTreeMap<TaskIdentity, TaskStatusCursor>,
    intake: &StatusIntakeHandle,
) -> Result<StreamObservation, SubscriptionState> {
    if identity.query_execution_id() != context.query_execution_id() {
        tracing::warn!(
            context_execution = ?context.query_execution_id(),
            event_execution = ?identity.query_execution_id(),
            "SubscribeTaskStatus event addresses a different query execution"
        );
        return Err(SubscriptionState::QueryMismatch);
    }
    if identity.backend_process_id() != context.backend_process_id() {
        tracing::warn!(
            context_backend = %context.backend_process_id(),
            event_backend = %identity.backend_process_id(),
            "SubscribeTaskStatus event addresses a different backend process"
        );
        return Err(SubscriptionState::ProcessMismatch);
    }
    match intake.publish(event) {
        StatusIntakeAdmission::Enqueued => {
            if let Some(next_cursor) = next_cursor {
                cursors.insert(identity, next_cursor);
            }
            Ok(StreamObservation::Delivered)
        }
        // The intake has already recorded observation loss and woken the
        // TaskRound. Stop this stream immediately so no later version can
        // leap over the snapshot it could not retain. This is local
        // backpressure, not a network failure, so the subscription loop must
        // not spend its transport error budget or resubscribe from its private
        // cursor. TaskRound will replace it from the state machine's cursor.
        StatusIntakeAdmission::Overflowed => Ok(StreamObservation::AwaitTaskReconciliation),
    }
}

#[cfg(test)]
fn observe_event(
    context: QueryContextRef,
    event: &proto::TaskStatusStreamEvent,
    cursors: &mut BTreeMap<TaskIdentity, TaskStatusCursor>,
    intake: &StatusIntakeHandle,
) -> Result<(), SubscriptionState> {
    let decoded =
        decode_context_aware_status_event(event, FieldPath::root("task_status_stream_event"))
            .map_err(|_| SubscriptionState::Rejected)?;
    let observed = match decoded {
        ContextAwareStatusStreamEvent::Status(status) => {
            let identity = status.identity();
            let cursor = cursors
                .get(&identity)
                .copied()
                .unwrap_or_else(|| TaskStatusCursor::unobserved(identity));
            observe_task_event(
                context,
                identity,
                StatusEvent::Published(status.clone()),
                Some(cursor.advanced_to(status.version())),
                cursors,
                intake,
            )?
        }
        ContextAwareStatusStreamEvent::Gone(identity) => observe_task_event(
            context,
            identity,
            StatusEvent::Gone(identity),
            None,
            cursors,
            intake,
        )?,
        ContextAwareStatusStreamEvent::ContextConvergence(_) => {
            return Err(SubscriptionState::Rejected);
        }
    };
    match observed {
        StreamObservation::Delivered => Ok(()),
        StreamObservation::AwaitTaskReconciliation => Err(SubscriptionState::Resubscribing),
    }
}

async fn observe_context_convergence(
    context: QueryContextRef,
    receipt: QueryContextConvergenceReceipt,
    intake: Option<&ContextConvergenceIntakeHandle>,
    cursor: &mut Option<QueryContextConvergenceCursor>,
) -> Result<StreamObservation, SubscriptionState> {
    let (Some(intake), Some(current)) = (intake, cursor.as_mut()) else {
        tracing::warn!(
            %context,
            "a task-only SubscribeTaskStatus stream received a context convergence event"
        );
        return Err(SubscriptionState::Rejected);
    };
    if receipt.context() != context {
        tracing::warn!(
            %context,
            receipt_context = %receipt.context(),
            "SubscribeTaskStatus convergence event addresses a different query context"
        );
        return Err(SubscriptionState::Rejected);
    }
    if let Some(version) = current.current_version() {
        if receipt.version() < version {
            tracing::warn!(
                %context,
                cursor_version = %version,
                receipt_version = %receipt.version(),
                "SubscribeTaskStatus convergence cursor would move backwards"
            );
            return Err(SubscriptionState::Rejected);
        }
        if receipt.version() == version {
            return Ok(StreamObservation::Delivered);
        }
    }

    loop {
        // Read the epoch before publishing. A release that races with the
        // overflow is then visible to `wait_for_capacity_change`; reading it
        // afterwards could wait on an epoch that already incorporated the only
        // capacity transition owed to this receipt.
        let capacity = intake.capacity_state();
        match intake.publish(context, receipt) {
            Ok(admission) => {
                assert!(admission.authorizes_cursor_advance());
                *current = current.advanced_to(receipt.version());
                return Ok(StreamObservation::Delivered);
            }
            Err(ContextConvergencePublishError::Overflow) => {
                let changed = intake.wait_for_capacity_change(capacity.epoch()).await;
                if changed.is_closed() {
                    tracing::warn!(
                        %context,
                        "context convergence intake closed while waiting for capacity"
                    );
                    return Err(SubscriptionState::Rejected);
                }
            }
            Err(error) => {
                tracing::warn!(
                    %context,
                    detail = %error,
                    "SubscribeTaskStatus convergence event was rejected"
                );
                return Err(SubscriptionState::Rejected);
            }
        }
    }
}

async fn open_subscription(
    client: &Client,
    context: QueryContextRef,
    cursors: &BTreeMap<TaskIdentity, TaskStatusCursor>,
    context_cursor: Option<QueryContextConvergenceCursor>,
) -> Result<tonic::Streaming<proto::TaskStatusStreamEvent>, tonic::Status> {
    let cursors = cursors.values().copied().collect::<Vec<_>>();
    let request = encode_context_aware_subscribe_task_status(context, &cursors, context_cursor)
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let mut grpc = client
        .grpc_with_channel_error()
        .await
        .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
    grpc.subscribe_task_status(tonic::Request::new(request))
        .await
        .map(tonic::Response::into_inner)
}

/// One covered observation stream per Context. Its request is always built
/// from cursors the serial owner has applied; receiving a frame does not make
/// that frame a safe reconnect cursor.
pub(crate) struct CoveredTaskStatusSubscriber {
    targets: BTreeMap<BackendProcessId, TaskBackendTarget>,
    intake: Arc<ObservationIntake>,
    error_budget: u32,
    data_runtime: FrontendDataRuntime,
    active: Mutex<BTreeMap<QueryContextRef, CoveredSubscription>>,
}

impl fmt::Debug for CoveredTaskStatusSubscriber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoveredTaskStatusSubscriber")
            .field("backends", &self.targets.len())
            .field("error_budget", &self.error_budget)
            .finish()
    }
}

struct CoveredSubscription {
    state: Arc<Mutex<SubscriptionState>>,
    applied_snapshot: Arc<Mutex<DecodedCoveredSubscription>>,
    reconciliation: tokio::sync::watch::Sender<DecodedCoveredSubscription>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for CoveredSubscription {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CoveredTaskStatusSubscriber {
    pub(crate) fn new(
        backends: &[(BackendProcessId, RuntimeEndpoint)],
        intake: Arc<ObservationIntake>,
        error_budget: u32,
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, String> {
        if error_budget == 0 {
            return Err("the covered subscription error budget must be nonzero".to_owned());
        }
        Ok(Self {
            targets: freeze_targets(backends, &data_runtime)?,
            intake,
            error_budget,
            data_runtime,
            active: Mutex::new(BTreeMap::new()),
        })
    }

    pub(crate) fn ensure(&self, request: DecodedCoveredSubscription) -> Result<(), String> {
        let context = request.context;
        let mut active = self
            .active
            .lock()
            .map_err(|_| "covered subscription lock poisoned".to_owned())?;
        if let Some(subscription) = active.get(&context) {
            Self::replace_applied_snapshot(subscription, request)?;
            return Ok(());
        }
        active.insert(context, self.start(request)?);
        Ok(())
    }

    /// Updates only the replay snapshot used by the next automatic reconnect.
    /// The current stream keeps running so ordinary cursor progress cannot
    /// induce an unnecessary new physical subscription generation.
    pub(crate) fn update_applied_request(
        &self,
        request: DecodedCoveredSubscription,
    ) -> Result<(), String> {
        let active = self
            .active
            .lock()
            .map_err(|_| "covered subscription lock poisoned".to_owned())?;
        let subscription = active
            .get(&request.context)
            .ok_or_else(|| "covered subscription is not active".to_owned())?;
        Self::replace_applied_snapshot(subscription, request)
    }

    fn replace_applied_snapshot(
        subscription: &CoveredSubscription,
        request: DecodedCoveredSubscription,
    ) -> Result<(), String> {
        encode_covered_subscribe_task_status(&request).map_err(|error| error.to_string())?;
        *subscription
            .applied_snapshot
            .lock()
            .map_err(|_| "covered applied snapshot lock poisoned".to_owned())? = request;
        Ok(())
    }

    pub(crate) fn reconcile(&self, request: DecodedCoveredSubscription) -> Result<(), String> {
        let context = request.context;
        encode_covered_subscribe_task_status(&request).map_err(|error| error.to_string())?;
        let mut active = self
            .active
            .lock()
            .map_err(|_| "covered subscription lock poisoned".to_owned())?;
        if let Some(subscription) = active.get(&context) {
            Self::replace_applied_snapshot(subscription, request.clone())?;
            subscription.reconciliation.send_replace(request);
        } else {
            active.insert(context, self.start(request)?);
        }
        observe_resubscribe();
        Ok(())
    }

    pub(crate) fn stop(&self, context: QueryContextRef) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&context);
        }
    }

    pub(crate) fn state(&self, context: QueryContextRef) -> Option<SubscriptionState> {
        let active = self.active.lock().ok()?;
        active.get(&context)?.state.lock().ok().map(|state| *state)
    }

    fn start(&self, request: DecodedCoveredSubscription) -> Result<CoveredSubscription, String> {
        encode_covered_subscribe_task_status(&request).map_err(|error| error.to_string())?;
        let target = self
            .targets
            .get(&request.context.backend_process_id())
            .ok_or_else(|| {
                format!(
                    "query context {} addresses a backend process outside the frozen topology",
                    request.context.backend_process_id()
                )
            })?;
        let publisher = self
            .intake
            .subscribe()
            .map_err(|_| "covered observation subscription capacity exhausted".to_owned())?;
        let state = Arc::new(Mutex::new(SubscriptionState::Opening));
        let applied_snapshot = Arc::new(Mutex::new(request.clone()));
        let (reconciliation, receiver) = tokio::sync::watch::channel(request);
        let task = self.data_runtime.spawn(run_covered_subscription(
            target.client.clone(),
            receiver,
            Arc::clone(&applied_snapshot),
            publisher,
            self.error_budget,
            Arc::clone(&state),
        ));
        Ok(CoveredSubscription {
            state,
            applied_snapshot,
            reconciliation,
            task,
        })
    }
}

struct CoveredRequestedVersions {
    statuses: BTreeSet<TaskIdentity>,
    task_convergence: BTreeSet<TaskIdentity>,
}

impl CoveredRequestedVersions {
    fn from_request(request: &DecodedCoveredSubscription) -> Self {
        Self {
            statuses: request
                .status_cursors
                .iter()
                .filter(|cursor| cursor.current_version().is_some())
                .map(|cursor| cursor.identity())
                .collect(),
            task_convergence: request
                .task_convergence_cursors
                .iter()
                .filter(|cursor| cursor.current_version().is_some())
                .map(|cursor| cursor.identity())
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum CoveredEventValidationError {
    ProcessMismatch,
    QueryMismatch,
    Protocol(&'static str),
}

impl CoveredEventValidationError {
    const fn subscription_state(self) -> SubscriptionState {
        match self {
            Self::ProcessMismatch => SubscriptionState::ProcessMismatch,
            Self::QueryMismatch => SubscriptionState::QueryMismatch,
            Self::Protocol(_) => SubscriptionState::Rejected,
        }
    }
}

impl fmt::Display for CoveredEventValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProcessMismatch => {
                formatter.write_str("covered observation addresses another backend process")
            }
            Self::QueryMismatch => {
                formatter.write_str("covered observation addresses another query context")
            }
            Self::Protocol(detail) => formatter.write_str(detail),
        }
    }
}

fn validate_covered_task_context(
    identity: TaskIdentity,
    context: QueryContextRef,
) -> Result<(), CoveredEventValidationError> {
    if identity.query_execution_id() != context.query_execution_id() {
        return Err(CoveredEventValidationError::QueryMismatch);
    }
    if identity.backend_process_id() != context.backend_process_id() {
        return Err(CoveredEventValidationError::ProcessMismatch);
    }
    Ok(())
}

fn validate_covered_receipt_context(
    observed: QueryContextRef,
    expected: QueryContextRef,
) -> Result<(), CoveredEventValidationError> {
    if observed.query_execution_id() != expected.query_execution_id()
        || observed.frontend_process_id() != expected.frontend_process_id()
    {
        return Err(CoveredEventValidationError::QueryMismatch);
    }
    if observed.backend_process_id() != expected.backend_process_id() {
        return Err(CoveredEventValidationError::ProcessMismatch);
    }
    Ok(())
}

fn validate_covered_event_context(
    request: &DecodedCoveredSubscription,
    requested_versions: &CoveredRequestedVersions,
    event: &CoveredStatusStreamEvent,
) -> Result<(), CoveredEventValidationError> {
    let context = request.context;
    // Classify identity before version prerequisites: a foreign Unchanged
    // frame is an identity violation even though no foreign cursor was sent.
    match &event.fact {
        CoveredStatusStreamFact::Status(status) => {
            validate_covered_task_context(status.identity(), context)?
        }
        CoveredStatusStreamFact::Gone(identity)
        | CoveredStatusStreamFact::Unknown(identity)
        | CoveredStatusStreamFact::StatusUnchanged(identity)
        | CoveredStatusStreamFact::TaskConvergenceUnchanged(identity) => {
            validate_covered_task_context(*identity, context)?
        }
        CoveredStatusStreamFact::TaskConvergence(receipt) => {
            validate_covered_task_context(receipt.identity(), context)?
        }
        CoveredStatusStreamFact::ContextConvergence(receipt) => {
            validate_covered_receipt_context(receipt.context(), context)?
        }
        CoveredStatusStreamFact::Quiesce(receipt) => {
            validate_covered_receipt_context(receipt.context(), context)?;
            for identity in receipt.accepted_tasks() {
                validate_covered_task_context(*identity, context)?;
            }
        }
        CoveredStatusStreamFact::CatchUpComplete(_) | CoveredStatusStreamFact::Bookmark(_) => {}
    }
    match &event.fact {
        CoveredStatusStreamFact::StatusUnchanged(identity)
            if !requested_versions.statuses.contains(identity) =>
        {
            return Err(CoveredEventValidationError::Protocol(
                "covered StatusUnchanged has no version in this subscription request",
            ));
        }
        CoveredStatusStreamFact::TaskConvergenceUnchanged(identity)
            if !requested_versions.task_convergence.contains(identity) =>
        {
            return Err(CoveredEventValidationError::Protocol(
                "covered TaskConvergenceUnchanged has no version in this subscription request",
            ));
        }
        CoveredStatusStreamFact::CatchUpComplete(marker)
            if marker.generation != request.generation.get() =>
        {
            return Err(CoveredEventValidationError::Protocol(
                "covered catch-up names another stream generation",
            ));
        }
        CoveredStatusStreamFact::Bookmark(marker)
            if marker.generation != request.generation.get() =>
        {
            return Err(CoveredEventValidationError::Protocol(
                "covered bookmark names another stream generation",
            ));
        }
        _ => {}
    }
    Ok(())
}

async fn open_covered_subscription(
    client: &Client,
    request: &DecodedCoveredSubscription,
) -> Result<tonic::Streaming<proto::TaskStatusStreamEvent>, tonic::Status> {
    let request = encode_covered_subscribe_task_status(request)
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let mut grpc = client
        .grpc_with_channel_error()
        .await
        .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
    grpc.subscribe_task_status(tonic::Request::new(request))
        .await
        .map(tonic::Response::into_inner)
}

async fn run_covered_subscription(
    client: Client,
    mut reconciliation: tokio::sync::watch::Receiver<DecodedCoveredSubscription>,
    applied_snapshot: Arc<Mutex<DecodedCoveredSubscription>>,
    publisher: ObservationPublisher,
    error_budget: u32,
    state: Arc<Mutex<SubscriptionState>>,
) {
    reconciliation.borrow_and_update();
    let mut applied: DecodedCoveredSubscription;
    let mut last_generation = 0_u64;
    let jitter = std::collections::hash_map::RandomState::new();
    let mut failures = 0_u32;
    'subscription: loop {
        applied = match applied_snapshot.lock() {
            Ok(snapshot) => snapshot.clone(),
            Err(_) => {
                set_state(&state, SubscriptionState::Rejected);
                return;
            }
        };
        let Some(next_generation) = last_generation.checked_add(1) else {
            set_state(&state, SubscriptionState::Rejected);
            return;
        };
        let generation = applied.generation.get().max(next_generation);
        last_generation = generation;
        applied.generation = NonZeroU64::new(generation).expect("generation is nonzero");
        let requested_versions = CoveredRequestedVersions::from_request(&applied);
        let context = applied.context;
        let opened = tokio::select! {
            changed = reconciliation.changed() => {
                if changed.is_err() { return; }
                reconciliation.borrow_and_update();
                set_state(&state, SubscriptionState::Resubscribing);
                continue 'subscription;
            }
            opened = tokio::time::timeout(
                COVERED_STREAM_LIVENESS_WINDOW, open_covered_subscription(&client, &applied)
            ) => opened.unwrap_or_else(|_| Err(tonic::Status::deadline_exceeded(
                "covered observation subscription opening exceeded its liveness budget"
            ))),
        };
        match opened {
            Ok(mut stream) => {
                set_state(&state, SubscriptionState::Live);
                let mut observed_liveness = false;
                let mut catch_up_seen = false;
                let mut bookmark_sequence = 0_u64;
                let mut liveness_deadline =
                    tokio::time::Instant::now() + COVERED_STREAM_LIVENESS_WINDOW;
                loop {
                    // A permit is obtained before the next network read. It
                    // reserves one bounded frame even if the serial queue is
                    // full, and dropping it on EOF/error returns that credit.
                    let reservation_started = tokio::time::Instant::now();
                    let permit = tokio::select! {
                        changed = reconciliation.changed() => {
                            if changed.is_err() { return; }
                            reconciliation.borrow_and_update();
                            set_state(&state, SubscriptionState::Resubscribing);
                            continue 'subscription;
                        }
                        permit = publisher.reserve_read() => permit,
                    };
                    // Time spent stopped by local intake capacity does not
                    // count as transport silence. The stream was not read
                    // while this permit was unavailable.
                    liveness_deadline += tokio::time::Instant::now() - reservation_started;
                    let message = tokio::select! {
                        changed = reconciliation.changed() => {
                            if changed.is_err() { return; }
                            reconciliation.borrow_and_update();
                            set_state(&state, SubscriptionState::Resubscribing);
                            continue 'subscription;
                        }
                        message = tokio::time::timeout_at(liveness_deadline, stream.message()) => {
                            match message {
                                Ok(message) => message,
                                Err(_) => break,
                            }
                        },
                    };
                    match message {
                        Ok(Some(wire)) => {
                            let encoded_bytes = prost::Message::encoded_len(&wire);
                            let event = match decode_covered_status_event(
                                &wire,
                                FieldPath::root("covered_status_stream_event"),
                            ) {
                                Ok(event) => event,
                                Err(error) => {
                                    tracing::warn!(%context, detail = %error, "covered observation frame is malformed");
                                    set_state(&state, SubscriptionState::Rejected);
                                    return;
                                }
                            };
                            if let Err(error) = validate_covered_event_context(
                                &applied,
                                &requested_versions,
                                &event,
                            ) {
                                tracing::warn!(%context, detail = %error, "covered observation frame conflicts with its stream");
                                set_state(&state, error.subscription_state());
                                return;
                            }
                            let new_liveness = match &event.fact {
                                CoveredStatusStreamFact::CatchUpComplete(_) if !catch_up_seen => {
                                    catch_up_seen = true;
                                    true
                                }
                                CoveredStatusStreamFact::Bookmark(marker)
                                    if marker.sequence > bookmark_sequence =>
                                {
                                    bookmark_sequence = marker.sequence;
                                    true
                                }
                                _ => false,
                            };
                            if let Err(error) = permit.publish(
                                ObservationFrame::Covered {
                                    context,
                                    generation: applied.generation.get(),
                                    event,
                                },
                                encoded_bytes,
                            ) {
                                tracing::warn!(
                                    %context,
                                    charged_bytes = error.charged_bytes,
                                    max_frame_bytes = error.max_frame_bytes,
                                    "covered observation frame exceeds its reserved budget"
                                );
                                set_state(&state, SubscriptionState::Rejected);
                                return;
                            }
                            if new_liveness {
                                observed_liveness = true;
                                liveness_deadline =
                                    tokio::time::Instant::now() + COVERED_STREAM_LIVENESS_WINDOW;
                            }
                        }
                        Ok(None) => break,
                        Err(status) if subscription_rejection_is_fatal(&status) => {
                            tracing::warn!(%context, code = ?status.code(), detail = status.message(), "covered observation stream rejected");
                            set_state(&state, SubscriptionState::Rejected);
                            return;
                        }
                        Err(_) => break,
                    }
                }
                if observed_liveness {
                    failures = 0;
                }
            }
            Err(status) if subscription_rejection_is_fatal(&status) => {
                tracing::warn!(%context, code = ?status.code(), detail = status.message(), "covered observation subscription rejected");
                set_state(&state, SubscriptionState::Rejected);
                return;
            }
            Err(_) => {}
        }
        // A disconnection does not erase accepted facts or manufacture a
        // local observation gap. Reconnect from the owner's applied cursors.
        failures += 1;
        if failures >= error_budget {
            set_state(&state, SubscriptionState::BudgetExhausted);
            return;
        }
        set_state(&state, SubscriptionState::Resubscribing);
        observe_resubscribe();
        let sample = std::hash::BuildHasher::hash_one(&jitter, (applied.context, last_generation));
        let backoff = covered_reconnect_backoff(failures, sample);
        tokio::select! {
            changed = reconciliation.changed() => {
                if changed.is_err() { return; }
                reconciliation.borrow_and_update();
            }
            _ = tokio::time::sleep(backoff) => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

const REFUSAL_UNKNOWN_BACKEND: &str = "unknown_backend";
const REFUSAL_PROCESS_MISMATCH: &str = "process_mismatch";
const REFUSAL_UNENCODABLE: &str = "unencodable";
const REFUSAL_OVER_BUDGET: &str = "over_budget";
const REFUSAL_UNUSABLE_RESPONSE: &str = "unusable_response";
const REFUSAL_UNUSABLE_ACK: &str = "unusable_ack";

const fn lane_name(lane: DispatchLane) -> &'static str {
    match lane {
        DispatchLane::Create => "create",
        DispatchLane::Update => "update",
        DispatchLane::Lifecycle => "lifecycle",
        DispatchLane::Control => "control",
    }
}

/// The machine-readable category name of one outcome.
///
/// This is the same closed category set the protocol classifies retry and
/// failure from, so an operator reading the metric and an owner reading the
/// receipt share one vocabulary.
const fn outcome_name(outcome: OperationOutcome) -> &'static str {
    match outcome {
        OperationOutcome::Accepted => "accepted",
        OperationOutcome::Idempotent => "idempotent",
        OperationOutcome::OperationTimedOut => "operation_timed_out",
        OperationOutcome::IdentityMismatch => "identity_mismatch",
        OperationOutcome::CompatibilityMismatch => "compatibility_mismatch",
        OperationOutcome::ContextNotEstablished => "context_not_established",
        OperationOutcome::ContextConflict => "context_conflict",
        OperationOutcome::DomainConflict => "domain_conflict",
        OperationOutcome::LeaseExpired => "lease_expired",
        OperationOutcome::ReleaseNotReady => "release_not_ready",
        OperationOutcome::ContextTerminalReceipt => "context_terminal_receipt",
        OperationOutcome::InvalidStateOrRequest => "invalid_state_or_request",
        OperationOutcome::TerminalRejected => "terminal_rejected",
        OperationOutcome::Gone => "gone",
        OperationOutcome::ResourceExhausted => "resource_exhausted",
        OperationOutcome::NotReady => "not_ready",
        OperationOutcome::PreparationBusy => "preparation_busy",
        OperationOutcome::AdmissionTicketStillActive => "admission_ticket_still_active",
    }
}

static TASK_DISPATCH_LANE_DEPTH: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_task_dispatch_lane_depth",
            "Task operations queued or in flight on one frontend dispatch lane.",
        ),
        &["lane", "phase"],
    )
    .expect("register novarocks_task_dispatch_lane_depth")
});

static TASK_OPERATION_BATCH_ITEMS: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "novarocks_task_operation_batch_items",
            "Operations carried by one ApplyTaskOperations request.",
        )
        .buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0]),
        &["lane"],
    )
    .expect("register novarocks_task_operation_batch_items")
});

static TASK_OPERATION_BATCH_BYTES: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "novarocks_task_operation_batch_bytes",
            "Encoded size of one ApplyTaskOperations request in bytes.",
        )
        .buckets(vec![
            1024.0,
            64.0 * 1024.0,
            1024.0 * 1024.0,
            8.0 * 1024.0 * 1024.0,
            48.0 * 1024.0 * 1024.0,
        ]),
        &["lane"],
    )
    .expect("register novarocks_task_operation_batch_bytes")
});

static TASK_OPERATION_RECEIPTS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_task_operation_receipt_total",
            "Settled task operations by operation kind and outcome category.",
        ),
        &["kind", "outcome"],
    )
    .expect("register novarocks_task_operation_receipt_total")
});

static TASK_OPERATION_REFUSALS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_task_operation_refused_total",
            "Task operations the frontend refused without a usable backend receipt.",
        ),
        &["reason"],
    )
    .expect("register novarocks_task_operation_refused_total")
});

static TASK_STATUS_SUBSCRIPTION_STATE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_task_status_subscription_state_total",
            "Observed states of the frontend task status subscriptions.",
        ),
        &["state"],
    )
    .expect("register novarocks_task_status_subscription_state_total")
});

static TASK_STATUS_RESUBSCRIPTIONS: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(Opts::new(
        "novarocks_task_status_resubscribe_total",
        "Task status subscriptions reopened from their per-task cursors.",
    ))
    .expect("register novarocks_task_status_resubscribe_total")
});

static TASK_LEASE_RENEWALS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_task_lease_renewal_total",
            "Query execution lease renewals settled by the frontend, by outcome category.",
        ),
        &["outcome"],
    )
    .expect("register novarocks_task_lease_renewal_total")
});

static TASK_ATTEMPT_PUMPS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_task_attempt_pump_installed_total",
            "Per-attempt owners handed to a task runner, by owner.",
        ),
        &["pump"],
    )
    .expect("register novarocks_task_attempt_pump_installed_total")
});

/// Registers every task transport collector.
///
/// The frontend management host owns the role-local registry; this is the one
/// entry point it needs.
pub(crate) fn register_metric_collectors(registry: &Registry) -> Result<(), String> {
    for collector in [
        Box::new(TASK_DISPATCH_LANE_DEPTH.clone()) as Box<dyn prometheus::core::Collector>,
        Box::new(TASK_OPERATION_BATCH_ITEMS.clone()),
        Box::new(TASK_OPERATION_BATCH_BYTES.clone()),
        Box::new(TASK_OPERATION_RECEIPTS.clone()),
        Box::new(TASK_OPERATION_REFUSALS.clone()),
        Box::new(TASK_STATUS_SUBSCRIPTION_STATE.clone()),
        Box::new(TASK_STATUS_RESUBSCRIPTIONS.clone()),
        Box::new(TASK_LEASE_RENEWALS.clone()),
        Box::new(TASK_ATTEMPT_PUMPS.clone()),
    ] {
        registry
            .register(collector)
            .map_err(|error| format!("register task transport metrics failed: {error}"))?;
    }
    Ok(())
}

/// Records that one attempt-local owner was handed to a task runner.
///
/// This is the supply observable of the per-turn owners. It counts installs,
/// not constructions: an owner that is built and then dropped -- which is
/// exactly how the dynamic filter and credential loops were lost -- never
/// reaches this.
pub(crate) fn observe_attempt_pump_installed(pump: &'static str) {
    TASK_ATTEMPT_PUMPS.with_label_values(&[pump]).inc();
}

/// The installs recorded for one owner so far.
#[cfg(test)]
pub(crate) fn installed_attempt_pumps(pump: &str) -> u64 {
    TASK_ATTEMPT_PUMPS.with_label_values(&[pump]).get()
}

/// Publishes one dispatch lane's depth.
///
/// The queue belongs to the dispatcher, not to this module, so the owner that
/// pumps it reports both numbers together and there is exactly one writer.
pub(crate) fn observe_dispatch_lane(lane: DispatchLane, queued: usize, in_flight: usize) {
    TASK_DISPATCH_LANE_DEPTH
        .with_label_values(&[lane_name(lane), "queued"])
        .set(queued as i64);
    TASK_DISPATCH_LANE_DEPTH
        .with_label_values(&[lane_name(lane), "in_flight"])
        .set(in_flight as i64);
}

fn observe_batch(lane: DispatchLane, items: usize, encoded_bytes: usize) {
    TASK_OPERATION_BATCH_ITEMS
        .with_label_values(&[lane_name(lane)])
        .observe(items as f64);
    TASK_OPERATION_BATCH_BYTES
        .with_label_values(&[lane_name(lane)])
        .observe(encoded_bytes as f64);
}

/// Counts one settled operation, and one lease renewal when that is what it
/// was.
///
/// The renewal counter only observes. There is deliberately no path from a
/// transport observation back to a lease: a renewal is minted by the query
/// context owner from its own clock, so transport health can never extend one.
fn observe_settled(kind: OperationKind, lease_renewal: bool, outcome: OperationOutcome) {
    TASK_OPERATION_RECEIPTS
        .with_label_values(&[kind.as_str(), outcome_name(outcome)])
        .inc();
    if lease_renewal {
        TASK_LEASE_RENEWALS
            .with_label_values(&[outcome_name(outcome)])
            .inc();
    }
}

fn observe_dispatch_result(
    kind: OperationKind,
    lease_renewal: bool,
    result: OperationDispatchResult,
) {
    let name = match result {
        OperationDispatchResult::WorkerReceipt(receipt) => outcome_name(receipt.outcome()),
        OperationDispatchResult::IngressRejected(kind) => match kind {
            novarocks_query_application::coordination::IngressRejection::WaitingCapacity => {
                "ingress_waiting_capacity"
            }
            novarocks_query_application::coordination::IngressRejection::BodyLimit => {
                "ingress_body_limit"
            }
            novarocks_query_application::coordination::IngressRejection::UnclassifiedCapacity => {
                "ingress_capacity_unclassified"
            }
        },
        OperationDispatchResult::NonWorkerRejected => "non_worker_rejected",
        OperationDispatchResult::TransportUnknown => "transport_unknown",
    };
    TASK_OPERATION_RECEIPTS
        .with_label_values(&[kind.as_str(), name])
        .inc();
    if lease_renewal {
        TASK_LEASE_RENEWALS.with_label_values(&[name]).inc();
    }
}

fn observe_refusal(reason: &str, operations: usize) {
    TASK_OPERATION_REFUSALS
        .with_label_values(&[reason])
        .inc_by(operations as u64);
}

fn observe_subscription_state(state: SubscriptionState) {
    TASK_STATUS_SUBSCRIPTION_STATE
        .with_label_values(&[state.as_str()])
        .inc();
}

fn observe_resubscribe() {
    TASK_STATUS_RESUBSCRIPTIONS.inc();
}

#[cfg(test)]
mod tests {
    //! Wire-level coverage of the task carrier.
    //!
    //! Every test drives the real client against a loopback Tonic peer built
    //! from the frontend's own generated stub, so batch framing, receipt
    //! attribution, status streaming, and the deployment JWT interceptor are
    //! exercised rather than mocked.

    use std::collections::VecDeque;
    use std::num::NonZeroUsize;
    use std::pin::Pin;

    use novarocks_execution::task_execution::{
        AbortCause, AbortQueryContext, AdmissionTicketId, CancelReason, CancelTask,
        CreateTaskReceipt, CredentialEpoch, CredentialLeaseId, CredentialUpdate, DomainVersion,
        EstablishQueryContext, FetchTaskDynamicFilters, LeaseSequence, LeaseValidFor,
        QueryContextConvergenceCursor, QueryContextConvergenceReceipt,
        QueryContextConvergenceState, QueryContextConvergenceVersion, QueryContextReceipt,
        QueryContextState, QuiesceQueryContextReceipt, RenewQueryExecutionLease,
        TaskConvergenceCursor, TaskConvergenceReceipt, TaskConvergenceVersion, TaskDomainUpdate,
        TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion, UpdateTask,
    };
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_models::{catalog, filter};
    use novarocks_task_codec::domain::{WireContent, WireCredential};
    use novarocks_task_codec::operation::{
        CoveredCatchUpComplete, CoveredObservationBookmark, QuiesceObservationCursor,
        decode_context_aware_subscribe_task_status, decode_covered_subscribe_task_status,
        decode_subscribe_task_status, encode_context_convergence_event,
        encode_covered_status_event, encode_operation_outcome, encode_query_context_ack,
        encode_status_event,
    };
    use novarocks_task_codec::operation::{
        ESTABLISH_CATALOG_DOMAIN_TAG, ESTABLISH_FILTER_DOMAIN_TAG,
        ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG,
    };
    use novarocks_types::identity::{FrontendProcessId, QueryExecutionId, StageId, TaskId};
    use novarocks_types::{AttemptId, QueryId};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status};

    use crate::native::transport_supervisor::NativeTransportSupervisor;
    use crate::task_execution::context_convergence::ContextConvergenceIntake;
    use crate::task_execution::dispatch::OperationDispatcher;
    use crate::task_execution::status_intake::{
        CountingWake, ObservationIntake, ObservationIntakeEntry, StatusIntake,
    };
    use novarocks_native_adapter::generated::nova_rocks_grpc_server::{
        NovaRocksGrpc, NovaRocksGrpcServer,
    };
    use novarocks_query_application::coordination::{DispatchBudget, MonotonicInstant};

    use super::*;

    #[test]
    fn covered_reconnect_backoff_is_exponential_bounded_and_jittered() {
        let bases = [100_u64, 200, 400, 800, 1_000, 1_000];
        for (index, base) in bases.into_iter().enumerate() {
            let failure = index as u32 + 1;
            assert_eq!(
                covered_reconnect_backoff(failure, 0),
                Duration::from_millis(base * 80 / 100)
            );
            assert_eq!(
                covered_reconnect_backoff(failure, 20),
                Duration::from_millis(base)
            );
            assert_eq!(
                covered_reconnect_backoff(failure, 40),
                Duration::from_millis(base * 120 / 100)
            );
        }
        assert_eq!(
            covered_reconnect_backoff(u32::MAX, 40),
            Duration::from_millis(1_200)
        );
    }

    // -----------------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------------

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(0x7a5c, 0x0006),
            AttemptId::new(1).expect("attempt one is nonzero"),
        )
        .expect("a nonzero query id")
    }

    fn identity(task: u32, backend: BackendProcessId) -> TaskIdentity {
        TaskIdentity::new(
            execution_id(),
            StageId::new(1).expect("a nonzero stage id"),
            TaskId::new(task).expect("a nonzero task id"),
            backend,
        )
    }

    fn status_at(identity: TaskIdentity, version: TaskStatusVersion) -> TaskStatus {
        TaskStatus::try_new(
            identity,
            version,
            TaskState::Running,
            None,
            TaskOutputFacts::default(),
        )
        .expect("a running task status")
    }

    fn context(backend: BackendProcessId) -> QueryContextRef {
        QueryContextRef::new(execution_id(), FrontendProcessId::new_v7(), backend)
    }

    fn cancel_intent(task: u32, backend: BackendProcessId) -> OperationIntent {
        OperationIntent::CancelTask(CancelTask::new(
            TaskOperationId::new_v7(),
            identity(task, backend),
            CancelReason::UpstreamNoLongerNeeded,
        ))
    }

    fn abort_intent(context: QueryContextRef) -> OperationIntent {
        OperationIntent::AbortQueryContext(AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ))
    }

    fn update_intent(task: u32, backend: BackendProcessId, version: u64) -> OperationIntent {
        OperationIntent::UpdateTask(Arc::new(
            UpdateTask::try_new(
                TaskOperationId::new_v7(),
                identity(task, backend),
                vec![TaskDomainUpdate::TaskDynamicFilter {
                    version: DomainVersion::new(version).expect("a positive domain version"),
                    payload: Arc::new(WireContent::new(
                        b"novarocks.test.task_dynamic_filter.v1",
                        filter::RuntimeFilterEnvelope::default(),
                    )),
                }],
            )
            .expect("one domain is an update"),
        ))
    }

    fn renew_intent(backend: BackendProcessId) -> OperationIntent {
        OperationIntent::UpdateQueryContext(Arc::new(UpdateQueryContext::RenewLease(
            RenewQueryExecutionLease::new(
                TaskOperationId::new_v7(),
                context(backend),
                LeaseSequence::INITIAL.next().expect("no overflow"),
                LeaseValidFor::new(Duration::from_secs(5)).expect("a legal lease duration"),
            ),
        )))
    }

    /// Releases one batch through the real dispatcher, which is the only owner
    /// that may mint a [`DispatchBatch`].
    fn released_batch(backend: BackendProcessId, intents: Vec<OperationIntent>) -> DispatchBatch {
        let mut dispatcher =
            OperationDispatcher::new(DispatchBudget::DEFAULT, TransportBudget::DEFAULT);
        dispatcher
            .register_task(backend)
            .expect("one task fits the per-backend bound");
        for intent in intents {
            dispatcher
                .enqueue(intent, MonotonicInstant::ORIGIN)
                .expect("the queue bounds admit a small batch");
        }
        dispatcher
            .take_batch()
            .expect("a queued lane releases a batch")
    }

    /// A seam double for operations that need no retained typed content.
    /// The attempt-level wire facts every test sink carries.
    fn test_attempt_facts() -> AttemptWireFacts {
        AttemptWireFacts {
            native_compatibility_id: novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        }
    }

    #[test]
    fn an_establish_encodes_the_query_options_owned_by_its_intent() {
        let backend = BackendProcessId::new_v7();
        let expected = proto::QueryOptions {
            query_mem_limit: 4096,
            pipeline_dop: 3,
            ..proto::QueryOptions::default()
        };
        let request = UpdateQueryContext::Establish(EstablishQueryContext::new(
            TaskOperationId::new_v7(),
            context(backend),
            AdmissionTicketId::try_from_bytes([0x54; 16]).expect("the ticket is nonzero"),
            Arc::new(WireContent::new(
                ESTABLISH_CATALOG_DOMAIN_TAG,
                catalog::CatalogSet::default(),
            )),
            Arc::new(WireContent::new(
                ESTABLISH_FILTER_DOMAIN_TAG,
                proto::RuntimeFilterContribution::default(),
            )),
            Arc::new(WireContent::new(
                ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG,
                expected,
            )),
            CredentialUpdate::new(
                CredentialLeaseId::new(1),
                CredentialEpoch::FIRST,
                Arc::new(
                    WireCredential::decode(&[], FieldPath::root("credential"))
                        .expect("an empty credential table is legal"),
                ),
            ),
            LeaseValidFor::new(Duration::from_secs(30)).expect("a legal lease"),
        ));

        let encoded = encode_query_context_operation(&request, &test_attempt_facts())
            .expect("the establish is codec-owned");
        let Some(proto::task_operation::Operation::UpdateQueryContext(update)) = encoded.operation
        else {
            panic!("an establish encodes as UpdateQueryContext");
        };
        let Some(proto::update_query_context_request::Command::Establish(establish)) =
            update.command
        else {
            panic!("the update carries an establish");
        };
        assert_eq!(establish.query_options, Some(expected));
    }

    // -----------------------------------------------------------------------
    // The loopback peer
    // -----------------------------------------------------------------------

    /// How the peer answers one `ApplyTaskOperations`.
    #[derive(Clone, Debug)]
    enum ApplyAnswer {
        /// One receipt per request item, in request order.
        Outcomes(Vec<OperationOutcome>),
        /// The same receipts, reversed.
        Reversed(Vec<OperationOutcome>),
        /// The same receipts with the last one missing.
        Truncated(Vec<OperationOutcome>),
        /// One receipt per request item, each with its own body.
        WithAck(Vec<(OperationOutcome, Option<proto::task_operation_receipt::Ack>)>),
        Reject(tonic::Code),
    }

    /// How the peer answers one `SubscribeTaskStatus`.
    enum SubscribeAnswer {
        /// Deliver these events, then close the stream.
        EventsThenClose(Vec<proto::TaskStatusStreamEvent>),
        /// Deliver these events and keep the stream open.
        EventsThenHold(Vec<proto::TaskStatusStreamEvent>),
        Reject(tonic::Code),
        StallOpening,
    }

    #[derive(Default)]
    struct PeerState {
        applied: Vec<proto::ApplyTaskOperationsRequest>,
        control_requests: usize,
        apply_answers: VecDeque<ApplyAnswer>,
        subscribed: Vec<proto::SubscribeTaskStatusRequest>,
        subscribe_answers: VecDeque<SubscribeAnswer>,
        held: Vec<tokio::sync::mpsc::Sender<Result<proto::TaskStatusStreamEvent, Status>>>,
    }

    #[derive(Clone, Default)]
    struct TaskWirePeer {
        state: Arc<Mutex<PeerState>>,
    }

    impl TaskWirePeer {
        fn rejected(rpc: &str) -> Status {
            Status::failed_precondition(format!("task wire test peer rejects {rpc}"))
        }

        fn expect_apply(&self, answer: ApplyAnswer) {
            self.state
                .lock()
                .expect("peer state")
                .apply_answers
                .push_back(answer);
        }

        fn expect_subscribe(&self, answer: SubscribeAnswer) {
            self.state
                .lock()
                .expect("peer state")
                .subscribe_answers
                .push_back(answer);
        }

        fn applied(&self) -> Vec<proto::ApplyTaskOperationsRequest> {
            self.state.lock().expect("peer state").applied.clone()
        }

        fn control_requests(&self) -> usize {
            self.state.lock().expect("peer state").control_requests
        }

        fn subscribed(&self) -> Vec<proto::SubscribeTaskStatusRequest> {
            self.state.lock().expect("peer state").subscribed.clone()
        }

        fn open_held_subscriptions(&self) -> usize {
            self.state
                .lock()
                .expect("peer state")
                .held
                .iter()
                .filter(|sender| !sender.is_closed())
                .count()
        }

        fn close_held_subscriptions(&self) {
            self.state.lock().expect("peer state").held.clear();
        }

        async fn send_held_event(&self, event: proto::TaskStatusStreamEvent) {
            let sender = self
                .state
                .lock()
                .expect("peer state")
                .held
                .last()
                .expect("one held subscription")
                .clone();
            sender.send(Ok(event)).await.expect("held stream is open");
        }
    }

    type EmptyExchangeStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<proto::ExchangeResponse, Status>> + Send>>;
    type StatusStream = ReceiverStream<Result<proto::TaskStatusStreamEvent, Status>>;

    #[tonic::async_trait]
    impl NovaRocksGrpc for TaskWirePeer {
        type ExchangeStream = EmptyExchangeStream;
        type SubscribeTaskStatusStream = StatusStream;

        async fn apply_task_operations(
            &self,
            request: Request<proto::ApplyTaskOperationsRequest>,
        ) -> Result<Response<proto::ApplyTaskOperationsResponse>, Status> {
            let request = request.into_inner();
            let answer = {
                let mut state = self.state.lock().expect("peer state");
                state.applied.push(request.clone());
                state
                    .apply_answers
                    .pop_front()
                    .unwrap_or(ApplyAnswer::Outcomes(vec![
                        OperationOutcome::Accepted;
                        request.operations.len()
                    ]))
            };
            let (outcomes, reversed, truncated) = match answer {
                ApplyAnswer::Outcomes(outcomes) => (
                    outcomes
                        .into_iter()
                        .map(|outcome| (outcome, None))
                        .collect(),
                    false,
                    false,
                ),
                ApplyAnswer::Reversed(outcomes) => (
                    outcomes
                        .into_iter()
                        .map(|outcome| (outcome, None))
                        .collect(),
                    true,
                    false,
                ),
                ApplyAnswer::Truncated(outcomes) => (
                    outcomes
                        .into_iter()
                        .map(|outcome| (outcome, None))
                        .collect(),
                    false,
                    true,
                ),
                ApplyAnswer::WithAck(items) => (items, false, false),
                ApplyAnswer::Reject(code) => {
                    return Err(Status::new(code, "task wire test peer rejection"));
                }
            };
            let mut receipts = request
                .operations
                .iter()
                .zip(outcomes)
                .map(|(operation, (outcome, ack))| proto::TaskOperationReceipt {
                    operation_id: operation
                        .envelope
                        .as_ref()
                        .and_then(|envelope| envelope.operation_id.clone()),
                    outcome: encode_operation_outcome(outcome),
                    safe_detail: String::new(),
                    safe_field_path: None,
                    ack,
                })
                .collect::<Vec<_>>();
            if reversed {
                receipts.reverse();
            }
            if truncated {
                receipts.pop();
            }
            Ok(Response::new(proto::ApplyTaskOperationsResponse {
                receipts,
            }))
        }

        async fn apply_task_control_operations(
            &self,
            request: Request<proto::ApplyTaskControlOperationsRequest>,
        ) -> Result<Response<proto::ApplyTaskOperationsResponse>, Status> {
            self.state.lock().expect("peer state").control_requests += 1;
            let operations = request
                .into_inner()
                .operations
                .into_iter()
                .map(|operation| {
                    let body = match operation.control.expect("control body") {
                        proto::task_control_operation::Control::RenewLease(renew) => {
                            proto::task_operation::Operation::UpdateQueryContext(
                                proto::UpdateQueryContextRequest {
                                    command: Some(
                                        proto::update_query_context_request::Command::RenewLease(
                                            renew,
                                        ),
                                    ),
                                },
                            )
                        }
                        proto::task_control_operation::Control::CancelTask(cancel) => {
                            proto::task_operation::Operation::CancelTask(cancel)
                        }
                        proto::task_control_operation::Control::AbortQueryContext(abort) => {
                            proto::task_operation::Operation::AbortQueryContext(abort)
                        }
                        proto::task_control_operation::Control::QuiesceQueryContext(quiesce) => {
                            proto::task_operation::Operation::QuiesceQueryContext(quiesce)
                        }
                        proto::task_control_operation::Control::ReleaseQueryContext(release) => {
                            proto::task_operation::Operation::ReleaseQueryContext(release)
                        }
                    };
                    proto::TaskOperation {
                        envelope: operation.envelope,
                        operation: Some(body),
                    }
                })
                .collect();
            self.apply_task_operations(Request::new(proto::ApplyTaskOperationsRequest {
                operations,
            }))
            .await
        }

        async fn subscribe_task_status(
            &self,
            request: Request<proto::SubscribeTaskStatusRequest>,
        ) -> Result<Response<Self::SubscribeTaskStatusStream>, Status> {
            let answer = {
                let mut state = self.state.lock().expect("peer state");
                state.subscribed.push(request.into_inner());
                state
                    .subscribe_answers
                    .pop_front()
                    .unwrap_or(SubscribeAnswer::EventsThenHold(Vec::new()))
            };
            let (events, hold) = match answer {
                SubscribeAnswer::StallOpening => return std::future::pending().await,
                SubscribeAnswer::EventsThenClose(events) => (events, false),
                SubscribeAnswer::EventsThenHold(events) => (events, true),
                SubscribeAnswer::Reject(code) => {
                    return Err(Status::new(code, "task wire test peer rejection"));
                }
            };
            let (sender, receiver) = tokio::sync::mpsc::channel(16);
            for event in events {
                sender.send(Ok(event)).await.expect("test stream capacity");
            }
            if hold {
                // Retaining the sender keeps the stream open, which is how a
                // healthy subscription looks between events.
                self.state.lock().expect("peer state").held.push(sender);
            }
            Ok(Response::new(ReceiverStream::new(receiver)))
        }

        async fn fetch_task_dynamic_filters(
            &self,
            _request: Request<proto::FetchTaskDynamicFiltersRequest>,
        ) -> Result<Response<proto::FetchTaskDynamicFiltersResponse>, Status> {
            Err(Self::rejected("FetchTaskDynamicFilters"))
        }

        async fn get_final_task_info(
            &self,
            _request: Request<proto::GetFinalTaskInfoRequest>,
        ) -> Result<Response<proto::GetFinalTaskInfoResponse>, Status> {
            Err(Self::rejected("GetFinalTaskInfo"))
        }

        async fn fetch_task_result(
            &self,
            _request: Request<proto::FetchTaskResultRequest>,
        ) -> Result<Response<proto::FetchResultResponse>, Status> {
            Err(Self::rejected("FetchTaskResult"))
        }

        async fn exchange(
            &self,
            _request: Request<tonic::Streaming<proto::ExchangeRequest>>,
        ) -> Result<Response<Self::ExchangeStream>, Status> {
            Err(Self::rejected("Exchange"))
        }

        async fn exchange_unary(
            &self,
            _request: Request<proto::ExchangeRequest>,
        ) -> Result<Response<proto::ExchangeResponse>, Status> {
            Err(Self::rejected("ExchangeUnary"))
        }

        async fn transmit_runtime_filter_envelope(
            &self,
            _request: Request<filter::RuntimeFilterEnvelope>,
        ) -> Result<Response<filter::RuntimeFilterEnvelopeResponse>, Status> {
            Err(Self::rejected("TransmitRuntimeFilterEnvelope"))
        }

        async fn fetch_result(
            &self,
            _request: Request<proto::FetchResultRequest>,
        ) -> Result<Response<proto::FetchResultResponse>, Status> {
            Err(Self::rejected("FetchResult"))
        }

        async fn prune_catalogs(
            &self,
            _request: Request<catalog::PruneCatalogsRequest>,
        ) -> Result<Response<catalog::PruneCatalogsResponse>, Status> {
            Err(Self::rejected("PruneCatalogs"))
        }

        async fn announce_backend(
            &self,
            _request: Request<proto::AnnounceBackendRequest>,
        ) -> Result<Response<proto::AnnounceBackendResponse>, Status> {
            Err(Self::rejected("AnnounceBackend"))
        }

        async fn heartbeat(
            &self,
            _request: Request<proto::HeartbeatRequest>,
        ) -> Result<Response<proto::HeartbeatResponse>, Status> {
            Err(Self::rejected("Heartbeat"))
        }
    }

    /// One live loopback peer and the endpoint that reaches it.
    struct Loopback {
        peer: TaskWirePeer,
        endpoint: RuntimeEndpoint,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        served: tokio::task::JoinHandle<()>,
    }

    impl Loopback {
        async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind the loopback task peer");
            let address = listener.local_addr().expect("loopback address");
            let incoming = futures::stream::unfold(listener, |listener| async {
                let item = listener.accept().await.map(|(stream, _)| stream);
                Some((item, listener))
            });
            let peer = TaskWirePeer::default();
            let service = peer.clone();
            let (shutdown, stop) = tokio::sync::oneshot::channel();
            let served = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(NovaRocksGrpcServer::new(service))
                    .serve_with_incoming_shutdown(incoming, async {
                        let _ = stop.await;
                    })
                    .await
                    .expect("serve the loopback task peer");
            });
            Self {
                peer,
                endpoint: RuntimeEndpoint::new("127.0.0.1", i32::from(address.port()))
                    .expect("a valid loopback endpoint"),
                shutdown: Some(shutdown),
                served,
            }
        }
    }

    impl Drop for Loopback {
        fn drop(&mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            self.served.abort();
        }
    }

    struct SinkFixture {
        loopback: Loopback,
        sink: NativeTaskOperationSink,
        acks: TaskAckIntake,
    }

    fn sink_fixture(loopback: Loopback, backend: BackendProcessId) -> SinkFixture {
        let data_runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let acks = TaskAckIntake::new(Arc::new(CountingWake::default()));
        let sink = NativeTaskOperationSink::new(
            &[(backend, loopback.endpoint.clone())],
            TransportBudget::DEFAULT,
            test_attempt_facts(),
            acks.handle(),
            data_runtime,
        )
        .expect("one frozen backend target");
        SinkFixture {
            loopback,
            sink,
            acks,
        }
    }

    /// Waits for the transport to settle `count` operations.
    async fn settled(acks: &TaskAckIntake, count: usize) -> Vec<OperationAcknowledgement> {
        for _ in 0..600 {
            if acks.queued() >= count {
                return acks.drain();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "the transport settled {} of {count} operations",
            acks.queued()
        );
    }

    fn submit_batch(sink: &NativeTaskOperationSink, mut batch: DispatchBatch) {
        // The synthetic dispatcher fixture uses untracked permits. Exercise
        // the real process owner before submission so its target lifetime is
        // the same as the production dispatcher path.
        let permits = batch
            .operations()
            .iter()
            .map(
                |intent| match sink.try_reserve_queue(intent.queue_request()) {
                    TaskOperationQueueAdmission::Admitted(permit) => permit,
                    TaskOperationQueueAdmission::TargetFull
                    | TaskOperationQueueAdmission::ProcessFull => {
                        panic!("a small loopback fixture needs target queue capacity")
                    }
                },
            )
            .collect();
        batch.replace_fixture_permits(permits);
        assert!(matches!(
            sink.try_submit(batch),
            TaskOperationSubmit::Accepted
        ));
    }

    async fn subscribe_requests(
        peer: &TaskWirePeer,
        count: usize,
    ) -> Vec<proto::SubscribeTaskStatusRequest> {
        for _ in 0..600 {
            let requests = peer.subscribed();
            if requests.len() >= count {
                return requests;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "the subscriber opened {} of {count} streams",
            peer.subscribed().len()
        );
    }

    // -----------------------------------------------------------------------
    // The operation sink
    // -----------------------------------------------------------------------

    #[test]
    fn task_cancel_uses_the_process_control_reserve() {
        let backend = BackendProcessId::new_v7();
        let batch = released_batch(backend, vec![cancel_intent(1, backend)]);
        assert_eq!(supervisor_lane(&batch), NativeTransportLane::Control);
    }

    #[test]
    fn mixed_operation_shapes_keep_order_across_method_batches() {
        let backend = BackendProcessId::new_v7();
        let intents = [
            update_intent(1, backend, 1),
            renew_intent(backend),
            cancel_intent(1, backend),
            update_intent(1, backend, 2),
        ];
        let expected_ids = intents
            .iter()
            .map(OperationIntent::operation_id)
            .collect::<Vec<_>>();
        let operations = intents
            .iter()
            .map(|intent| {
                (
                    is_small_control(intent.shape()),
                    encode_operation(intent, &test_attempt_facts()).expect("encodable intent"),
                )
            })
            .collect();
        let batches = encode_method_batches(operations, TransportBudget::DEFAULT)
            .expect("mixed batch fits transport budget");
        assert_eq!(
            batches.iter().map(|batch| batch.items).collect::<Vec<_>>(),
            vec![1, 2, 1]
        );
        let actual_ids = batches
            .iter()
            .flat_map(|batch| match &batch.request {
                EncodedMethodRequest::Ordinary(request) => request
                    .operations
                    .iter()
                    .map(|operation| operation.envelope.as_ref().unwrap().operation_id.clone())
                    .collect::<Vec<_>>(),
                EncodedMethodRequest::Control(request) => request
                    .operations
                    .iter()
                    .map(|operation| operation.envelope.as_ref().unwrap().operation_id.clone())
                    .collect::<Vec<_>>(),
            })
            .collect::<Vec<_>>();
        let expected_wire_ids = expected_ids
            .into_iter()
            .map(|id| {
                Some(proto::TaskOperationId {
                    value: id.to_bytes().to_vec(),
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(actual_ids, expected_wire_ids);
        assert!(matches!(
            batches[0].request,
            EncodedMethodRequest::Ordinary(_)
        ));
        assert!(matches!(
            batches[1].request,
            EncodedMethodRequest::Control(_)
        ));
        assert!(matches!(
            batches[2].request,
            EncodedMethodRequest::Ordinary(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_later_control_run_does_not_restart_its_expired_submit_deadline() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let intents = [update_intent(1, backend, 1), cancel_intent(1, backend)];
        let operations = intents
            .iter()
            .enumerate()
            .map(|(index, intent)| {
                let mut operation =
                    encode_operation(intent, &test_attempt_facts()).expect("encodable intent");
                operation
                    .envelope
                    .as_mut()
                    .expect("envelope")
                    .max_wait_millis = if index == 0 { 300_000 } else { 100 };
                (is_small_control(intent.shape()), operation)
            })
            .collect();
        let batches =
            encode_method_batches(operations, TransportBudget::DEFAULT).expect("two method runs");
        assert_eq!(batches.len(), 2);
        // Model time already consumed by an earlier run without sleeping or
        // depending on scheduler timing. The ordinary item still has time;
        // the later control item had only 100 ms from the same submit origin.
        let submitted_at = tokio::time::Instant::now() - Duration::from_secs(10);
        let client = &fixture.sink.targets[&backend].client;
        let mut batches = batches.into_iter();
        let first = batches.next().expect("ordinary run");
        let first_expiry = first.expires_at(submitted_at);
        send_operations(
            client,
            first.request,
            first_expiry,
            &fixture.acks.handle(),
            &[],
        )
        .await
        .expect("ordinary run still has time");
        let second = batches.next().expect("control run");
        assert!(matches!(&second.request, EncodedMethodRequest::Control(_)));
        let second_expiry = second.expires_at(submitted_at);
        assert_eq!(
            send_operations(
                client,
                second.request,
                second_expiry,
                &fixture.acks.handle(),
                &[]
            )
            .await,
            Err(OperationDispatchResult::TransportUnknown),
            "expired control run must not reach its RPC"
        );
        assert_eq!(fixture.loopback.peer.control_requests(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_started_is_published_only_when_the_exact_rpc_future_is_polled() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let intent = update_intent(1, backend, 1);
        let operation_id = intent.operation_id();
        let context = context(backend);
        let operation = encode_operation(&intent, &test_attempt_facts()).expect("encodable");
        let batch = encode_method_batches(vec![(false, operation)], TransportBudget::DEFAULT)
            .expect("one legal request")
            .pop()
            .expect("one method batch");
        let client = &fixture.sink.targets[&backend].client;
        let intake = fixture.acks.handle();
        let establishes = [(operation_id, context)];
        let send = send_operations(
            client,
            batch.request,
            tokio::time::Instant::now() + Duration::from_secs(5),
            &intake,
            &establishes,
        );
        assert!(
            fixture.acks.drain_events().is_empty(),
            "future construction is not send start"
        );
        send.await.expect("the loopback RPC answers");
        let events = fixture.acks.drain_events();
        assert!(matches!(
            events.as_slice(),
            [TaskOperationIntakeEvent::EstablishSendStarted {
                operation_id: observed,
                context: observed_context,
            }] if *observed == operation_id && *observed_context == context
        ));

        let expired = encode_method_batches(
            vec![(
                false,
                encode_operation(&intent, &test_attempt_facts()).expect("encodable"),
            )],
            TransportBudget::DEFAULT,
        )
        .expect("one legal request")
        .pop()
        .expect("one method batch");
        assert_eq!(
            send_operations(
                client,
                expired.request,
                tokio::time::Instant::now() - Duration::from_secs(1),
                &intake,
                &establishes,
            )
            .await,
            Err(OperationDispatchResult::TransportUnknown)
        );
        assert!(
            fixture.acks.drain_events().is_empty(),
            "an expired preflight cannot report send start"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn submitting_to_a_closed_runtime_settles_once_and_returns_reservations() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build a runtime to close before submission");
        let handle = runtime.handle().clone();
        runtime.shutdown_background();
        let mut sink = NativeTaskOperationSink::new(
            &[(backend, fixture.loopback.endpoint.clone())],
            TransportBudget::DEFAULT,
            test_attempt_facts(),
            fixture.acks.handle(),
            FrontendDataRuntime::new(handle),
        )
        .expect("one frozen backend target on the closed runtime");
        // Keep the actual wire client on the live test runtime. Only the
        // production send scheduling capability has been shut down.
        sink.targets
            .get_mut(&backend)
            .expect("frozen target")
            .client = fixture.sink.targets[&backend].client.clone();
        let supervisor = sink.data_runtime.task_transport_supervisor();
        let before = supervisor.snapshot();
        let mut batch = released_batch(
            backend,
            vec![cancel_intent(1, backend), cancel_intent(2, backend)],
        );
        let expected_ids = batch
            .operations()
            .iter()
            .map(OperationIntent::operation_id)
            .collect::<Vec<_>>();
        assert_eq!(expected_ids.len(), 2);
        let permits = batch
            .operations()
            .iter()
            .map(
                |intent| match sink.try_reserve_queue(intent.queue_request()) {
                    TaskOperationQueueAdmission::Admitted(permit) => permit,
                    TaskOperationQueueAdmission::TargetFull
                    | TaskOperationQueueAdmission::ProcessFull => panic!("two cancels fit"),
                },
            )
            .collect();
        batch.replace_fixture_permits(permits);
        assert_eq!(
            supervisor.snapshot().retained_items,
            before.retained_items + 2
        );
        assert!(
            supervisor.snapshot().retained_queued_and_encoded_bytes
                > before.retained_queued_and_encoded_bytes
        );
        assert!(fixture.acks.drain_events().is_empty());

        // Exercise the actual sink: a closed runtime drops its accepted
        // spawned future before the first poll, including its encoding permit.
        assert!(matches!(
            sink.try_submit(batch),
            TaskOperationSubmit::Accepted
        ));

        let acknowledgements = fixture.acks.drain();
        assert_eq!(
            acknowledgements
                .iter()
                .map(OperationAcknowledgement::operation_id)
                .collect::<Vec<_>>(),
            expected_ids,
        );
        assert!(acknowledgements.iter().all(|ack| matches!(
            ack.dispatch_result(),
            OperationDispatchResult::TransportUnknown
        )));
        assert!(
            fixture.acks.drain_events().is_empty(),
            "settled exactly once"
        );
        assert_eq!(supervisor.snapshot(), before, "all reservations returned");
        assert!(fixture.loopback.peer.applied().is_empty());
        assert_eq!(fixture.loopback.peer.control_requests(), 0);
    }

    #[test]
    fn dropping_an_accepted_send_settles_only_its_remaining_items_as_unknown() {
        let intake = TaskAckIntake::new(Arc::new(CountingWake::default()));
        let backend = BackendProcessId::new_v7();
        let intents = [update_intent(1, backend, 1), cancel_intent(2, backend)];
        let sent = intents
            .iter()
            .map(|intent| SentOperation {
                operation_id: intent.operation_id(),
                kind: intent.kind(),
                establish_context: None,
                lease_renewal: is_lease_renewal(intent),
                address: AckAddress::of(intent),
            })
            .collect::<Vec<_>>();
        let ids = sent
            .iter()
            .map(|item| item.operation_id)
            .collect::<Vec<_>>();
        let mut receipts = AcceptedOperationReceipts::new(intake.handle(), sent);
        receipts.publish_next(OperationAcknowledgement::transport_unknown(
            ids[0],
            intents[0].kind(),
        ));

        drop(receipts);

        let acknowledgements = intake.drain();
        assert_eq!(acknowledgements.len(), 2);
        assert_eq!(
            acknowledgements
                .iter()
                .map(OperationAcknowledgement::operation_id)
                .collect::<Vec<_>>(),
            ids,
            "drop settlement preserves request order and never republishes a settled item"
        );
        assert!(acknowledgements.iter().all(|ack| matches!(
            ack.dispatch_result(),
            OperationDispatchResult::TransportUnknown
        )));
    }

    #[test]
    fn task_cancel_leaves_update_backlog_first_and_reaches_control_capacity() {
        let backend = BackendProcessId::new_v7();
        let transport =
            TransportBudget::new(2, 1024, 512, 4, 4096, 4, 4096, 1, 2, Duration::from_secs(1))
                .expect("one ordinary and one control batch fit");
        let dispatch = DispatchBudget::new(1, 1, 1, 1).expect("nonzero lane permits");
        let mut dispatcher = OperationDispatcher::new(dispatch, transport);
        dispatcher
            .register_task(backend)
            .expect("one task fits the backend");
        dispatcher
            .enqueue(update_intent(1, backend, 1), MonotonicInstant::ORIGIN)
            .expect("first ordinary update");
        let released_update = dispatcher
            .take_batch()
            .expect("the first update reaches transport");
        let update_acceptance = released_update.acceptance();
        dispatcher
            .accept(update_acceptance)
            .expect("the first update saturates its only permit");
        assert_eq!(dispatcher.lane_in_flight(backend, DispatchLane::Update), 1);
        dispatcher
            .enqueue(update_intent(1, backend, 2), MonotonicInstant::ORIGIN)
            .expect("second ordinary update");
        dispatcher
            .enqueue(cancel_intent(1, backend), MonotonicInstant::ORIGIN)
            .expect("task cancellation");

        let cancel = dispatcher
            .take_batch()
            .expect("control is selected before the update backlog");
        assert_eq!(cancel.operations().len(), 1);
        assert_eq!(cancel.operations()[0].kind(), OperationKind::CancelTask);
        assert_eq!(
            cancel.lane(),
            DispatchLane::Control,
            "task cancellation has an independent dispatch permit"
        );
        assert_eq!(supervisor_lane(&cancel), NativeTransportLane::Control);

        let supervisor = NativeTransportSupervisor::from_transport(transport)
            .expect("the process windows were validated");
        let intake = TaskAckIntake::new(Arc::new(CountingWake::default()));
        let waiter = supervisor
            .register_waiter(Arc::new(intake.handle()))
            .expect("register the attempt");
        let mut ordinary_queue = supervisor
            .try_reserve_queue(
                &waiter,
                backend,
                NativeTransportLane::Ordinary,
                transport.max_batch_items(),
                transport.max_operation_queued_bytes(),
            )
            .expect("fill the ordinary queue window exactly");
        let ordinary_encoding = supervisor
            .try_reserve_encoding(&waiter, backend, NativeTransportLane::Ordinary)
            .expect("ordinary head can always reserve its encoding window");
        ordinary_queue.mark_in_flight();
        let mut control_queue = supervisor
            .try_reserve_queue(
                &waiter,
                backend,
                supervisor_lane(&cancel),
                cancel.operations().len(),
                cancel.queued_bytes(),
            )
            .expect("the cancellation directly consumes the reserved control queue");
        let mut control_encoding = supervisor
            .try_reserve_encoding(&waiter, backend, supervisor_lane(&cancel))
            .expect("the cancellation can reserve encoding beside ordinary I/O");
        control_encoding.shrink_to(1);
        control_queue.mark_in_flight();
        drop(control_encoding);
        drop(control_queue);
        drop(ordinary_encoding);
        drop(ordinary_queue);

        assert_eq!(dispatcher.lane_queued(backend, DispatchLane::Update), 1);
        assert!(
            dispatcher.take_batch().is_none(),
            "the queued update remains blocked by its saturated permit"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_partial_batch_failure_is_settled_per_item_in_request_order() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let batch = released_batch(
            backend,
            vec![
                cancel_intent(1, backend),
                cancel_intent(2, backend),
                cancel_intent(3, backend),
            ],
        );
        let expected = batch
            .operations()
            .iter()
            .map(OperationIntent::operation_id)
            .collect::<Vec<_>>();
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::Outcomes(vec![
                OperationOutcome::Accepted,
                OperationOutcome::TerminalRejected,
                OperationOutcome::Idempotent,
            ]));

        submit_batch(&fixture.sink, batch);
        let acks = settled(&fixture.acks, 3).await;

        assert_eq!(
            acks.iter()
                .map(OperationAcknowledgement::operation_id)
                .collect::<Vec<_>>(),
            expected,
            "receipts are handed back in request order"
        );
        assert_eq!(
            acks.iter()
                .map(OperationAcknowledgement::worker_outcome)
                .collect::<Vec<_>>(),
            vec![
                Some(OperationOutcome::Accepted),
                Some(OperationOutcome::TerminalRejected),
                Some(OperationOutcome::Idempotent),
            ],
            "one item's failure leaves every other item exactly as its own receipt reports"
        );
        assert_eq!(
            fixture.loopback.peer.applied().len(),
            1,
            "one batch, one RPC"
        );
        assert_eq!(fixture.loopback.peer.control_requests(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reordered_receipts_are_refused() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let batch = released_batch(
            backend,
            vec![cancel_intent(1, backend), cancel_intent(2, backend)],
        );
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::Reversed(vec![
                OperationOutcome::Accepted,
                OperationOutcome::TerminalRejected,
            ]));

        submit_batch(&fixture.sink, batch);
        let acks = settled(&fixture.acks, 2).await;

        for ack in &acks {
            assert_eq!(
                ack.worker_outcome(),
                Some(OperationOutcome::InvalidStateOrRequest)
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_short_receipt_batch_is_refused() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let batch = released_batch(
            backend,
            vec![cancel_intent(1, backend), cancel_intent(2, backend)],
        );
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::Truncated(vec![
                OperationOutcome::Accepted,
                OperationOutcome::Accepted,
            ]));

        submit_batch(&fixture.sink, batch);
        let acks = settled(&fixture.acks, 2).await;

        assert!(
            acks.iter()
                .all(|ack| ack.worker_outcome() == Some(OperationOutcome::InvalidStateOrRequest)),
            "a response with fewer receipts than items cannot be attributed"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unknown_transport_outcome_is_retryable_and_resends_the_identical_request() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let intent = cancel_intent(1, backend);
        let batch = released_batch(backend, vec![intent.clone()]);
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::Reject(tonic::Code::Unavailable));

        submit_batch(&fixture.sink, released_batch(backend, vec![intent]));
        let first = settled(&fixture.acks, 1).await;
        assert_eq!(
            first[0].dispatch_result(),
            OperationDispatchResult::TransportUnknown
        );

        // The owner replays the same immutable intent, so the same bytes go
        // back out under the same operation id.
        submit_batch(&fixture.sink, batch);
        let second = settled(&fixture.acks, 1).await;
        assert_eq!(second[0].worker_outcome(), Some(OperationOutcome::Accepted));
        assert_eq!(second[0].operation_id(), first[0].operation_id());

        let applied = fixture.loopback.peer.applied();
        assert_eq!(applied.len(), 2);
        assert_eq!(
            applied[0], applied[1],
            "a retry resends the identical request rather than a new one"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_typed_rejection_is_not_retried() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let batch = released_batch(backend, vec![cancel_intent(1, backend)]);
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::Reject(tonic::Code::InvalidArgument));

        submit_batch(&fixture.sink, batch);
        let acks = settled(&fixture.acks, 1).await;

        assert_eq!(
            acks[0].dispatch_result(),
            OperationDispatchResult::NonWorkerRejected
        );
        assert_eq!(acks[0].worker_outcome(), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_process_mismatch_fails_the_attempt_without_sending() {
        let frozen = BackendProcessId::new_v7();
        let replacement = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, frozen);
        let batch = released_batch(replacement, vec![cancel_intent(1, replacement)]);

        submit_batch(&fixture.sink, batch);
        let acks = settled(&fixture.acks, 1).await;

        assert_eq!(
            acks[0].worker_outcome(),
            Some(OperationOutcome::IdentityMismatch)
        );
        assert!(
            fixture.loopback.peer.applied().is_empty(),
            "a replaced process is never retried elsewhere"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_observation_read_is_refused_before_the_wire() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let batch = released_batch(
            backend,
            vec![OperationIntent::FetchTaskDynamicFilters(
                FetchTaskDynamicFilters::new(TaskOperationId::new_v7(), identity(1, backend), None),
            )],
        );

        submit_batch(&fixture.sink, batch);
        let acks = settled(&fixture.acks, 1).await;

        assert_eq!(acks[0].kind(), OperationKind::FetchTaskDynamicFilters);
        assert_eq!(
            acks[0].worker_outcome(),
            Some(OperationOutcome::InvalidStateOrRequest)
        );
        assert!(
            fixture.loopback.peer.applied().is_empty(),
            "a read has its own RPC and never becomes a batch item"
        );
    }

    #[test]
    fn the_transport_status_classification_is_exact() {
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Cancelled,
            tonic::Code::Unknown,
        ] {
            let result = classify_apply_status(&Status::new(code, "unknown outcome"));
            assert_eq!(result, OperationDispatchResult::TransportUnknown);
        }
        assert_eq!(
            classify_apply_status(&Status::new(tonic::Code::ResourceExhausted, "full")),
            OperationDispatchResult::IngressRejected(
                novarocks_query_application::coordination::IngressRejection::UnclassifiedCapacity
            )
        );
        let mut waiting = Status::resource_exhausted("ignored detail");
        waiting.metadata_mut().insert(
            "x-novarocks-ingress-rejection",
            tonic::metadata::MetadataValue::from_static("waiting_capacity"),
        );
        assert_eq!(
            classify_apply_status(&waiting),
            OperationDispatchResult::IngressRejected(
                novarocks_query_application::coordination::IngressRejection::WaitingCapacity
            )
        );
        let mut body = Status::resource_exhausted("ignored detail");
        body.metadata_mut().insert(
            "x-novarocks-ingress-rejection",
            tonic::metadata::MetadataValue::from_static("body_limit"),
        );
        assert_eq!(
            classify_apply_status(&body),
            OperationDispatchResult::IngressRejected(
                novarocks_query_application::coordination::IngressRejection::BodyLimit
            )
        );
        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::NotFound,
            tonic::Code::AlreadyExists,
            tonic::Code::PermissionDenied,
            tonic::Code::FailedPrecondition,
            tonic::Code::Aborted,
            tonic::Code::OutOfRange,
            tonic::Code::Unimplemented,
            tonic::Code::Internal,
            tonic::Code::DataLoss,
            tonic::Code::Unauthenticated,
        ] {
            let result = classify_apply_status(&Status::new(code, "settled"));
            assert!(
                matches!(result, OperationDispatchResult::NonWorkerRejected),
                "{code:?} must not be resent"
            );
        }
        assert!(matches!(
            classify_channel_error(&ChannelAcquisitionError::fatal("bad material")),
            OperationDispatchResult::NonWorkerRejected
        ));
        assert!(matches!(
            classify_channel_error(&ChannelAcquisitionError::retryable_network("dial failed")),
            OperationDispatchResult::TransportUnknown
        ));
    }

    // -----------------------------------------------------------------------
    // The status subscription
    // -----------------------------------------------------------------------

    struct SubscriberFixture {
        loopback: Loopback,
        subscriber: TaskStatusSubscriber,
        intake: StatusIntake,
        wake: Arc<CountingWake>,
        acks: TaskAckIntake,
    }

    struct ContextAwareSubscriberFixture {
        loopback: Loopback,
        subscriber: TaskStatusSubscriber,
        status_intake: StatusIntake,
        convergence_intake: ContextConvergenceIntake,
    }

    fn subscriber_fixture(
        loopback: Loopback,
        backend: BackendProcessId,
        error_budget: u32,
    ) -> SubscriberFixture {
        let data_runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let wake = Arc::new(CountingWake::default());
        let intake = StatusIntake::new(16, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
        let subscriber = TaskStatusSubscriber::new(
            &[(backend, loopback.endpoint.clone())],
            intake.handle(),
            error_budget,
            data_runtime,
        )
        .expect("one frozen backend target");
        SubscriberFixture {
            loopback,
            subscriber,
            intake,
            wake,
            acks: TaskAckIntake::new(Arc::new(CountingWake::default())),
        }
    }

    fn context_aware_subscriber_fixture(
        loopback: Loopback,
        backend: BackendProcessId,
        error_budget: u32,
        convergence_capacity: NonZeroUsize,
    ) -> ContextAwareSubscriberFixture {
        let data_runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let wake = Arc::new(CountingWake::default());
        let status_intake = StatusIntake::new(16, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
        let convergence_intake = ContextConvergenceIntake::bounded(convergence_capacity);
        let subscriber = TaskStatusSubscriber::new_context_aware(
            &[(backend, loopback.endpoint.clone())],
            status_intake.handle(),
            convergence_intake.handle(),
            error_budget,
            data_runtime,
        )
        .expect("one frozen context-aware backend target");
        ContextAwareSubscriberFixture {
            loopback,
            subscriber,
            status_intake,
            convergence_intake,
        }
    }

    fn convergence_receipt(context: QueryContextRef) -> QueryContextConvergenceReceipt {
        QueryContextConvergenceReceipt::new(
            context,
            QueryContextConvergenceVersion::FIRST,
            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_stream_preserves_every_frame_and_reconnects_from_applied_cursors() {
        let backend = BackendProcessId::new_v7();
        let loopback = Loopback::start().await;
        let context = context(backend);
        let task = identity(1, backend);
        let frames = vec![
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::Status(status_at(task, TaskStatusVersion::FIRST)),
                source_revision: Some(1),
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::TaskConvergence(
                    TaskConvergenceReceipt::actual_stopped(task, TaskConvergenceVersion::FIRST),
                ),
                source_revision: Some(2),
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::ContextConvergence(convergence_receipt(context)),
                source_revision: Some(3),
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::Quiesce(QuiesceQueryContextReceipt::new(
                    context,
                    1,
                    vec![task],
                    QueryContextState::Quiescing,
                )),
                source_revision: Some(4),
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::StatusUnchanged(task),
                source_revision: None,
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::TaskConvergenceUnchanged(task),
                source_revision: None,
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::Unknown(task),
                source_revision: None,
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::Gone(task),
                source_revision: Some(5),
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: 7,
                    initial_cut: 4,
                }),
                source_revision: None,
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                    generation: 7,
                    sequence: 1,
                    covered_prefix: 4,
                    source_cut: 5,
                }),
                source_revision: None,
            },
        ];
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenClose(
                frames
                    .iter()
                    .map(|frame| encode_covered_status_event(frame).unwrap())
                    .collect(),
            ));
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(Vec::new()));
        let wake = Arc::new(CountingWake::default());
        let intake = Arc::new(
            ObservationIntake::new(16, 32, 32 * 4096, 4096, wake).expect("bounded intake"),
        );
        let subscriber = CoveredTaskStatusSubscriber::new(
            &[(backend, loopback.endpoint.clone())],
            Arc::clone(&intake),
            3,
            FrontendDataRuntime::new(tokio::runtime::Handle::current()),
        )
        .expect("covered transport");
        subscriber
            .ensure(DecodedCoveredSubscription {
                context,
                generation: NonZeroU64::new(7).unwrap(),
                status_cursors: vec![TaskStatusCursor::at(task, TaskStatusVersion::FIRST)],
                task_convergence_cursors: vec![TaskConvergenceCursor::at(
                    task,
                    TaskConvergenceVersion::FIRST,
                )],
                context_cursor: Some(QueryContextConvergenceCursor::unobserved(context)),
                quiesce_cursor: Some(QuiesceObservationCursor {
                    context,
                    fence_version: None,
                }),
                required_identities: vec![task],
            })
            .expect("covered subscription starts");
        let requests = subscribe_requests(&loopback.peer, 2).await;
        let first = decode_covered_subscribe_task_status(
            &requests[0],
            FieldPath::root("first_covered_request"),
        )
        .unwrap();
        let second = decode_covered_subscribe_task_status(
            &requests[1],
            FieldPath::root("second_covered_request"),
        )
        .unwrap();
        assert_eq!(first.generation.get(), 7);
        assert_eq!(second.generation.get(), 8);
        assert_eq!(first.required_identities, vec![task]);
        assert_eq!(
            second.status_cursors[0].current_version(),
            Some(TaskStatusVersion::FIRST)
        );
        assert_eq!(
            second.task_convergence_cursors[0].current_version(),
            Some(TaskConvergenceVersion::FIRST)
        );
        assert_eq!(second.quiesce_cursor.unwrap().fence_version, None);

        let mut runner = intake.try_enter().expect("one serial owner");
        let observed = runner.drain_ordered(16);
        assert_eq!(observed.len(), frames.len());
        for (entry, expected) in observed.into_iter().zip(frames) {
            assert!(matches!(
                entry,
                ObservationIntakeEntry::Frame(ObservationFrame::Covered {
                    context: observed_context,
                    generation: 7,
                    event,
                }) if observed_context == context && event == expected
            ));
        }
        subscriber.stop(context);
    }

    /// Real HTTP/2 transport plus production FE serial owners. The peer is a
    /// scripted covered source, so this does not claim a full BE system gate.
    #[tokio::test(flavor = "multi_thread")]
    async fn covered_loopback_backpressure_recovery_quiesce_settles_actual_root_seal() {
        use crate::task_execution::clock::TaskProtocolClock;
        use crate::task_execution::round::TaskRound;
        use novarocks_execution::task_execution::TerminationDetail;

        async fn drive_until(round: &mut TaskRound, ready: impl Fn(&TaskRound) -> bool) {
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    round.turn().expect("the production serial owner advances");
                    if ready(round) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("the expected applied fact reaches the serial owner");
        }

        let (mut round, clock, context, missing) =
            crate::task_execution::tests::covered_loopback_recovery_round();
        let loopback = Loopback::start().await;
        let frame = |fact, revision| {
            encode_covered_status_event(&CoveredStatusStreamEvent {
                fact,
                source_revision: revision,
            })
            .unwrap()
        };
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![
                frame(
                    CoveredStatusStreamFact::Status(status_at(
                        missing,
                        TaskStatusVersion::new(2).unwrap(),
                    )),
                    None,
                ),
                frame(
                    CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                        generation: 2,
                        initial_cut: 2,
                    }),
                    None,
                ),
                frame(
                    CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                        generation: 2,
                        sequence: 1,
                        covered_prefix: 2,
                        source_cut: 3,
                    }),
                    None,
                ),
            ]));
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(Vec::new()));
        let wake = Arc::new(CountingWake::default());
        let intake = Arc::new(
            ObservationIntake::new_with_clock(
                1,
                3,
                3 * 4096,
                4096,
                Arc::clone(&wake) as Arc<dyn StatusIntakeWake>,
                Arc::clone(&clock) as Arc<dyn TaskProtocolClock>,
            )
            .unwrap(),
        );
        let subscriber = Arc::new(
            CoveredTaskStatusSubscriber::new(
                &[(context.backend_process_id(), loopback.endpoint.clone())],
                Arc::clone(&intake),
                2,
                FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            )
            .unwrap(),
        );
        round.install_covered_observation(Arc::clone(&subscriber), Arc::clone(&intake));
        let source = round.take_root_status_source().unwrap();
        subscriber
            .ensure(
                round
                    .execution()
                    .covered_subscription_request(context, NonZeroU64::new(2).unwrap())
                    .unwrap(),
            )
            .unwrap();
        subscribe_requests(&loopback.peer, 1).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while wake.count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(5_200)).await;
        clock.advance(Duration::from_millis(5_200));
        assert_eq!(
            wake.count(),
            2,
            "local pressure must stop the third wire read"
        );
        assert_eq!(subscriber.state(context), Some(SubscriptionState::Live));
        assert_eq!(
            loopback.peer.subscribed().len(),
            1,
            "local pressure must not reconnect"
        );
        drive_until(&mut round, |_| wake.count() >= 3).await;
        // The last frame may have been registered just after this turn.
        drive_until(&mut round, |round| {
            round
                .execution()
                .status_cursors(context)
                .iter()
                .any(|cursor| {
                    cursor.identity() == missing
                        && cursor.current_version() == Some(TaskStatusVersion::new(2).unwrap())
                })
        })
        .await;
        round.turn().unwrap();
        let mut reply = source.begin_success_seal_request().unwrap();
        round.turn().unwrap();
        assert!(matches!(
            reply.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(
            !round.root_success_sealed(),
            "a missing terminal remains required after a covered prefix"
        );

        loopback.peer.close_held_subscriptions();
        let requests = subscribe_requests(&loopback.peer, 2).await;
        let replay = decode_covered_subscribe_task_status(
            &requests[1],
            FieldPath::root("owner_applied_replay"),
        )
        .unwrap();
        assert_eq!(replay.generation.get(), 3);
        assert!(
            replay
                .status_cursors
                .iter()
                .any(|cursor| cursor.identity() == missing
                    && cursor.current_version() == Some(TaskStatusVersion::new(2).unwrap()))
        );
        let accepted = round
            .execution()
            .graph()
            .tasks()
            .filter(|task| task.context() == context)
            .map(|task| task.identity())
            .collect::<Vec<_>>();
        for (version, state) in [(3, TaskState::Canceling), (4, TaskState::Canceled)] {
            loopback
                .peer
                .send_held_event(frame(
                    CoveredStatusStreamFact::Status(
                        TaskStatus::try_new(
                            missing,
                            TaskStatusVersion::new(version).unwrap(),
                            state,
                            Some(TerminationDetail::Canceled(
                                CancelReason::UpstreamNoLongerNeeded,
                            )),
                            TaskOutputFacts::new(false),
                        )
                        .unwrap(),
                    ),
                    None,
                ))
                .await;
        }
        loopback
            .peer
            .send_held_event(frame(
                CoveredStatusStreamFact::Quiesce(QuiesceQueryContextReceipt::new(
                    context,
                    1,
                    accepted.clone(),
                    QueryContextState::Quiescing,
                )),
                None,
            ))
            .await;
        drive_until(&mut round, |round| {
            round
                .execution()
                .covered_subscription_request(context, NonZeroU64::new(3).unwrap())
                .unwrap()
                .quiesce_cursor
                .is_some_and(|cursor| cursor.fence_version.is_some())
        })
        .await;
        assert!(
            !round.covered_observation_ready(),
            "terminal and Quiesce cannot cover an unfinished initial cut"
        );
        assert!(matches!(
            reply.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        let applied = round
            .execution()
            .covered_subscription_request(context, NonZeroU64::new(3).unwrap())
            .unwrap();
        assert_eq!(
            applied.quiesce_cursor.unwrap().fence_version.unwrap().get(),
            1
        );
        assert_eq!(
            round.execution().owner(context).unwrap().state(),
            QueryContextState::Quiescing
        );
        loopback
            .peer
            .send_held_event(frame(
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: 3,
                    initial_cut: 5,
                }),
                None,
            ))
            .await;
        drive_until(&mut round, |round| round.root_success_sealed()).await;
        assert_eq!(
            reply.try_recv().unwrap(),
            Ok(()),
            "the actual result-pump seal reply must settle"
        );
        assert!(round.covered_observation_ready());
        subscriber.stop(context);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_unchanged_requires_a_version_in_the_physical_request() {
        for convergence in [false, true] {
            let backend = BackendProcessId::new_v7();
            let loopback = Loopback::start().await;
            let context = context(backend);
            let task = identity(1, backend);
            loopback
                .peer
                .expect_subscribe(SubscribeAnswer::EventsThenHold(Vec::new()));
            let intake = Arc::new(
                ObservationIntake::new(2, 4, 4 * 4096, 4096, Arc::new(CountingWake::default()))
                    .unwrap(),
            );
            let subscriber = CoveredTaskStatusSubscriber::new(
                &[(backend, loopback.endpoint.clone())],
                Arc::clone(&intake),
                2,
                FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            )
            .unwrap();
            let mut request = DecodedCoveredSubscription {
                context,
                generation: NonZeroU64::new(1).unwrap(),
                status_cursors: vec![TaskStatusCursor::unobserved(task)],
                task_convergence_cursors: vec![TaskConvergenceCursor::unobserved(task)],
                context_cursor: None,
                quiesce_cursor: None,
                required_identities: vec![task],
            };
            subscriber.ensure(request.clone()).unwrap();
            subscribe_requests(&loopback.peer, 1).await;
            tokio::time::timeout(Duration::from_secs(2), async {
                while loopback.peer.open_held_subscriptions() != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the original physical subscription is open");

            // An unrelated ACK can advance the serial owner's applied cursor
            // while this physical stream still carries its older request.
            request.status_cursors = vec![TaskStatusCursor::at(task, TaskStatusVersion::FIRST)];
            request.task_convergence_cursors = vec![TaskConvergenceCursor::at(
                task,
                TaskConvergenceVersion::FIRST,
            )];
            subscriber.update_applied_request(request).unwrap();
            assert_eq!(loopback.peer.subscribed().len(), 1);
            let fact = if convergence {
                CoveredStatusStreamFact::TaskConvergenceUnchanged(task)
            } else {
                CoveredStatusStreamFact::StatusUnchanged(task)
            };
            loopback
                .peer
                .send_held_event(
                    encode_covered_status_event(&CoveredStatusStreamEvent {
                        fact,
                        source_revision: None,
                    })
                    .unwrap(),
                )
                .await;
            tokio::time::timeout(Duration::from_secs(2), async {
                while subscriber.state(context) != Some(SubscriptionState::Rejected) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("Unchanged cannot borrow a later applied cursor");
            let mut runner = intake.try_enter().unwrap();
            assert!(runner.drain_ordered(1).is_empty());
            subscriber.stop(context);
        }
    }

    #[test]
    fn covered_unchanged_requires_the_exact_requested_identity() {
        let backend = BackendProcessId::new_v7();
        let context = context(backend);
        let requested = identity(1, backend);
        let other = identity(2, backend);
        let request = DecodedCoveredSubscription {
            context,
            generation: NonZeroU64::new(3).unwrap(),
            status_cursors: vec![TaskStatusCursor::at(requested, TaskStatusVersion::FIRST)],
            task_convergence_cursors: vec![TaskConvergenceCursor::at(
                requested,
                TaskConvergenceVersion::FIRST,
            )],
            context_cursor: None,
            quiesce_cursor: None,
            required_identities: vec![requested, other],
        };
        for fact in [
            CoveredStatusStreamFact::StatusUnchanged(other),
            CoveredStatusStreamFact::TaskConvergenceUnchanged(other),
        ] {
            let event = CoveredStatusStreamEvent {
                fact,
                source_revision: None,
            };
            assert!(
                validate_covered_event_context(
                    &request,
                    &CoveredRequestedVersions::from_request(&request),
                    &event
                )
                .is_err()
            );
        }
    }

    /// Real H2 validates the foreign frame before the actual serial Task
    /// owner receives it. This is FE transport/owner integration evidence.
    #[tokio::test(flavor = "multi_thread")]
    async fn covered_foreign_status_process_reaches_actual_round_as_identity_violation() {
        use crate::task_execution::clock::TaskProtocolClock;
        use crate::task_execution::error::{ParticipantObservationFailure, TaskExecutionError};

        let (mut round, clock, context, task) =
            crate::task_execution::tests::covered_loopback_recovery_round();
        let foreign = TaskIdentity::new(
            task.query_execution_id(),
            task.stage_id(),
            task.task_id(),
            BackendProcessId::new_v7(),
        );
        let loopback = Loopback::start().await;
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![
                encode_covered_status_event(&CoveredStatusStreamEvent {
                    fact: CoveredStatusStreamFact::Status(status_at(
                        foreign,
                        TaskStatusVersion::FIRST,
                    )),
                    source_revision: None,
                })
                .unwrap(),
            ]));
        let intake = Arc::new(
            ObservationIntake::new_with_clock(
                1,
                3,
                3 * 4096,
                4096,
                Arc::new(CountingWake::default()),
                clock as Arc<dyn TaskProtocolClock>,
            )
            .unwrap(),
        );
        let subscriber = Arc::new(
            CoveredTaskStatusSubscriber::new(
                &[(context.backend_process_id(), loopback.endpoint.clone())],
                Arc::clone(&intake),
                2,
                FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            )
            .unwrap(),
        );
        round.install_covered_observation(Arc::clone(&subscriber), Arc::clone(&intake));
        subscriber
            .ensure(
                round
                    .execution()
                    .covered_subscription_request(context, NonZeroU64::new(2).unwrap())
                    .unwrap(),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while subscriber.state(context) != Some(SubscriptionState::ProcessMismatch) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the H2 foreign frame fixes an exact identity violation");
        {
            let mut runner = intake.try_enter().unwrap();
            assert!(
                runner.drain_ordered(1).is_empty(),
                "foreign status never reaches the reducer"
            );
        }
        intake.acknowledge_applied();
        let error = round
            .turn()
            .expect_err("the actual Task owner must fail closed");
        assert!(
            matches!(error, TaskExecutionError::ParticipantUnobservable {
            backend, state: ParticipantObservationFailure::IdentityViolation("process_mismatch")
        } if backend == context.backend_process_id()),
            "{error:?}"
        );
        assert_eq!(
            loopback.peer.subscribed().len(),
            1,
            "identity violations cannot reopen the source as transport recovery"
        );
        subscriber.stop(context);
    }

    fn covered_validation_request(
        context: QueryContextRef,
        task: TaskIdentity,
    ) -> DecodedCoveredSubscription {
        DecodedCoveredSubscription {
            context,
            generation: NonZeroU64::new(3).unwrap(),
            status_cursors: vec![TaskStatusCursor::at(task, TaskStatusVersion::FIRST)],
            task_convergence_cursors: vec![TaskConvergenceCursor::at(
                task,
                TaskConvergenceVersion::FIRST,
            )],
            context_cursor: None,
            quiesce_cursor: None,
            required_identities: vec![task],
        }
    }

    #[test]
    fn covered_identity_violations_retain_their_typed_failure_for_every_carrier() {
        let backend = BackendProcessId::new_v7();
        let exact_context = context(backend);
        let exact_task = identity(1, backend);
        let request = covered_validation_request(exact_context, exact_task);
        let requested_versions = CoveredRequestedVersions::from_request(&request);
        let other_query =
            QueryExecutionId::new(QueryId::new(91, 92), AttemptId::new(1).unwrap()).unwrap();
        for (query, process, expected) in [
            (
                exact_context.query_execution_id(),
                BackendProcessId::new_v7(),
                SubscriptionState::ProcessMismatch,
            ),
            (other_query, backend, SubscriptionState::QueryMismatch),
        ] {
            let foreign_task =
                TaskIdentity::new(query, exact_task.stage_id(), exact_task.task_id(), process);
            let foreign_context =
                QueryContextRef::new(query, exact_context.frontend_process_id(), process);
            let facts = [
                CoveredStatusStreamFact::Status(status_at(foreign_task, TaskStatusVersion::FIRST)),
                CoveredStatusStreamFact::Gone(foreign_task),
                CoveredStatusStreamFact::Unknown(foreign_task),
                CoveredStatusStreamFact::StatusUnchanged(foreign_task),
                CoveredStatusStreamFact::TaskConvergenceUnchanged(foreign_task),
                CoveredStatusStreamFact::TaskConvergence(TaskConvergenceReceipt::actual_stopped(
                    foreign_task,
                    TaskConvergenceVersion::FIRST,
                )),
                CoveredStatusStreamFact::ContextConvergence(convergence_receipt(foreign_context)),
                CoveredStatusStreamFact::Quiesce(QuiesceQueryContextReceipt::new(
                    foreign_context,
                    1,
                    vec![exact_task],
                    QueryContextState::Quiescing,
                )),
                // A correct Context cannot launder a foreign accepted member.
                CoveredStatusStreamFact::Quiesce(QuiesceQueryContextReceipt::new(
                    exact_context,
                    1,
                    vec![foreign_task],
                    QueryContextState::Quiescing,
                )),
            ];
            for fact in facts {
                let event = CoveredStatusStreamEvent {
                    fact,
                    source_revision: None,
                };
                let error = validate_covered_event_context(&request, &requested_versions, &event)
                    .expect_err("foreign identity must be rejected");
                assert_eq!(error.subscription_state(), expected, "{event:?}");
            }
        }
        let foreign_frontend = QueryContextRef::new(
            exact_context.query_execution_id(),
            FrontendProcessId::new_v7(),
            backend,
        );
        for fact in [
            CoveredStatusStreamFact::ContextConvergence(convergence_receipt(foreign_frontend)),
            CoveredStatusStreamFact::Quiesce(QuiesceQueryContextReceipt::new(
                foreign_frontend,
                1,
                vec![exact_task],
                QueryContextState::Quiescing,
            )),
        ] {
            assert_eq!(
                validate_covered_event_context(
                    &request,
                    &requested_versions,
                    &CoveredStatusStreamEvent {
                        fact,
                        source_revision: None
                    }
                )
                .unwrap_err()
                .subscription_state(),
                SubscriptionState::QueryMismatch
            );
        }
    }

    #[test]
    fn covered_version_and_generation_violations_remain_protocol_rejections() {
        let backend = BackendProcessId::new_v7();
        let request = covered_validation_request(context(backend), identity(1, backend));
        let versions = CoveredRequestedVersions::from_request(&request);
        for fact in [
            CoveredStatusStreamFact::StatusUnchanged(identity(2, backend)),
            CoveredStatusStreamFact::TaskConvergenceUnchanged(identity(2, backend)),
            CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                generation: 4,
                initial_cut: 1,
            }),
            CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                generation: 4,
                sequence: 1,
                covered_prefix: 0,
                source_cut: 1,
            }),
        ] {
            let event = CoveredStatusStreamEvent {
                fact,
                source_revision: None,
            };
            assert_eq!(
                validate_covered_event_context(&request, &versions, &event)
                    .unwrap_err()
                    .subscription_state(),
                SubscriptionState::Rejected,
                "{event:?}"
            );
        }
        for fact in [
            CoveredStatusStreamFact::StatusUnchanged(identity(1, backend)),
            CoveredStatusStreamFact::TaskConvergenceUnchanged(identity(1, backend)),
            CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                generation: 3,
                initial_cut: 1,
            }),
            CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                generation: 3,
                sequence: 1,
                covered_prefix: 0,
                source_cut: 1,
            }),
        ] {
            let event = CoveredStatusStreamEvent {
                fact,
                source_revision: None,
            };
            validate_covered_event_context(&request, &versions, &event)
                .expect("exact version and generation remain valid");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_subscription_blackhole_opening_exhausts_its_bounded_budget() {
        let backend = BackendProcessId::new_v7();
        let loopback = Loopback::start().await;
        let context = context(backend);
        let task = identity(1, backend);
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::StallOpening);
        let intake = Arc::new(
            ObservationIntake::new(1, 3, 3 * 4096, 4096, Arc::new(CountingWake::default()))
                .unwrap(),
        );
        let subscriber = CoveredTaskStatusSubscriber::new(
            &[(backend, loopback.endpoint.clone())],
            intake,
            1,
            FrontendDataRuntime::new(tokio::runtime::Handle::current()),
        )
        .unwrap();
        subscriber
            .ensure(DecodedCoveredSubscription {
                context,
                generation: NonZeroU64::new(1).unwrap(),
                status_cursors: vec![TaskStatusCursor::unobserved(task)],
                task_convergence_cursors: Vec::new(),
                context_cursor: None,
                quiesce_cursor: None,
                required_identities: vec![task],
            })
            .unwrap();
        subscribe_requests(&loopback.peer, 1).await;
        tokio::time::timeout(Duration::from_secs(8), async {
            while subscriber.state(context) != Some(SubscriptionState::BudgetExhausted) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("an opening blackhole cannot retain its transport indefinitely");
        assert_eq!(loopback.peer.subscribed().len(), 1);
        subscriber.stop(context);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_stream_stops_reading_while_its_one_pending_frame_waits() {
        let backend = BackendProcessId::new_v7();
        let loopback = Loopback::start().await;
        let context = context(backend);
        let task = identity(1, backend);
        let frames = (1..=3)
            .map(|version| {
                encode_covered_status_event(&CoveredStatusStreamEvent {
                    fact: CoveredStatusStreamFact::Status(status_at(
                        task,
                        TaskStatusVersion::new(version).unwrap(),
                    )),
                    source_revision: Some(version),
                })
                .unwrap()
            })
            .collect();
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(frames));
        let wake = Arc::new(CountingWake::default());
        let intake = Arc::new(
            ObservationIntake::new(
                1,
                3,
                3 * 4096,
                4096,
                Arc::clone(&wake) as Arc<dyn StatusIntakeWake>,
            )
            .expect("one queue frame and one pending frame"),
        );
        let subscriber = CoveredTaskStatusSubscriber::new(
            &[(backend, loopback.endpoint.clone())],
            Arc::clone(&intake),
            2,
            FrontendDataRuntime::new(tokio::runtime::Handle::current()),
        )
        .unwrap();
        subscriber
            .ensure(DecodedCoveredSubscription {
                context,
                generation: NonZeroU64::new(1).unwrap(),
                status_cursors: vec![TaskStatusCursor::unobserved(task)],
                task_convergence_cursors: Vec::new(),
                context_cursor: None,
                quiesce_cursor: None,
                required_identities: vec![task],
            })
            .unwrap();
        subscribe_requests(&loopback.peer, 1).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while wake.count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the queue and pending slot fill");
        assert_eq!(wake.count(), 2);
        // Local application backpressure outlasts the wire idle budget. It
        // must not consume transport failures or create a replacement stream.
        tokio::time::sleep(Duration::from_millis(5_200)).await;
        assert_eq!(
            wake.count(),
            2,
            "paused local reads must retain both frames"
        );
        assert_eq!(subscriber.state(context), Some(SubscriptionState::Live));
        assert_eq!(
            loopback.peer.subscribed().len(),
            1,
            "local pressure caused a reconnect"
        );
        {
            let mut runner = intake.try_enter().unwrap();
            assert_eq!(runner.drain_ordered(1).len(), 1);
            intake.acknowledge_applied();
        }
        assert_eq!(wake.count(), 2, "the pending slot still stops this stream");
        {
            let mut runner = intake.try_enter().unwrap();
            assert_eq!(runner.drain_ordered(1).len(), 1);
            intake.acknowledge_applied();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while wake.count() < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("draining the pending slot resumes the stream");
        let mut runner = intake.try_enter().unwrap();
        assert_eq!(runner.drain_ordered(1).len(), 1);
        intake.acknowledge_applied();
        assert_eq!(subscriber.state(context), Some(SubscriptionState::Live));
        subscriber.stop(context);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_reconciliation_releases_an_unread_stream_permit() {
        let backend = BackendProcessId::new_v7();
        let loopback = Loopback::start().await;
        let context = context(backend);
        let task = identity(1, backend);
        let first = encode_covered_status_event(&CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Status(status_at(task, TaskStatusVersion::FIRST)),
            source_revision: Some(1),
        })
        .unwrap();
        let second = encode_covered_status_event(&CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                generation: 2,
                sequence: 1,
                covered_prefix: 0,
                source_cut: 1,
            }),
            source_revision: None,
        })
        .unwrap();
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![first]));
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![second]));
        let wake = Arc::new(CountingWake::default());
        let intake = Arc::new(
            ObservationIntake::new(
                1,
                3,
                3 * 4096,
                4096,
                Arc::clone(&wake) as Arc<dyn StatusIntakeWake>,
            )
            .unwrap(),
        );
        let subscriber = CoveredTaskStatusSubscriber::new(
            &[(backend, loopback.endpoint.clone())],
            Arc::clone(&intake),
            2,
            FrontendDataRuntime::new(tokio::runtime::Handle::current()),
        )
        .unwrap();
        let request = DecodedCoveredSubscription {
            context,
            generation: NonZeroU64::new(1).unwrap(),
            status_cursors: vec![TaskStatusCursor::unobserved(task)],
            task_convergence_cursors: Vec::new(),
            context_cursor: None,
            quiesce_cursor: None,
            required_identities: vec![task],
        };
        subscriber.ensure(request.clone()).unwrap();
        subscribe_requests(&loopback.peer, 1).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while wake.count() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first generation delivered its status");
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        subscriber.reconcile(request).unwrap();
        let requests = subscribe_requests(&loopback.peer, 2).await;
        let replay = decode_covered_subscribe_task_status(
            &requests[1],
            FieldPath::root("reconciled_covered_request"),
        )
        .unwrap();
        assert_eq!(replay.generation.get(), 2);
        tokio::time::timeout(Duration::from_secs(2), async {
            while wake.count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the next stream can reserve after the old read was canceled");
        let mut runner = intake.try_enter().unwrap();
        assert_eq!(runner.drain_ordered(2).len(), 2);
        subscriber.stop(context);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_automatic_reconnect_uses_the_latest_applied_cursor_without_restarting_early() {
        let backend = BackendProcessId::new_v7();
        let loopback = Loopback::start().await;
        let context = context(backend);
        let task = identity(1, backend);
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![
                encode_covered_status_event(&CoveredStatusStreamEvent {
                    fact: CoveredStatusStreamFact::Status(status_at(
                        task,
                        TaskStatusVersion::FIRST,
                    )),
                    source_revision: Some(1),
                })
                .unwrap(),
            ]));
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(Vec::new()));
        let wake = Arc::new(CountingWake::default());
        let intake = Arc::new(
            ObservationIntake::new(
                2,
                4,
                4 * 4096,
                4096,
                Arc::clone(&wake) as Arc<dyn StatusIntakeWake>,
            )
            .unwrap(),
        );
        let subscriber = CoveredTaskStatusSubscriber::new(
            &[(backend, loopback.endpoint.clone())],
            Arc::clone(&intake),
            3,
            FrontendDataRuntime::new(tokio::runtime::Handle::current()),
        )
        .unwrap();
        let mut request = DecodedCoveredSubscription {
            context,
            generation: NonZeroU64::new(1).unwrap(),
            status_cursors: vec![TaskStatusCursor::unobserved(task)],
            task_convergence_cursors: Vec::new(),
            context_cursor: None,
            quiesce_cursor: None,
            required_identities: vec![task],
        };
        subscriber.ensure(request.clone()).unwrap();
        subscribe_requests(&loopback.peer, 1).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while wake.count() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut runner = intake.try_enter().unwrap();
        assert_eq!(runner.drain_ordered(1).len(), 1);
        drop(runner);
        request.status_cursors = vec![TaskStatusCursor::at(task, TaskStatusVersion::FIRST)];
        subscriber.update_applied_request(request).unwrap();
        assert_eq!(loopback.peer.subscribed().len(), 1);

        loopback.peer.close_held_subscriptions();
        let requests = subscribe_requests(&loopback.peer, 2).await;
        let replay = decode_covered_subscribe_task_status(
            &requests[1],
            FieldPath::root("latest_applied_reconnect"),
        )
        .unwrap();
        assert_eq!(replay.generation.get(), 2);
        assert_eq!(
            replay.status_cursors[0].current_version(),
            Some(TaskStatusVersion::FIRST)
        );
        subscriber.stop(context);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_context_aware_stream_retains_convergence_and_reconnects_from_its_cursor() {
        let backend = BackendProcessId::new_v7();
        let mut fixture = context_aware_subscriber_fixture(
            Loopback::start().await,
            backend,
            2,
            NonZeroUsize::MIN,
        );
        let query_context = context(backend);
        let receipt = convergence_receipt(query_context);
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenClose(vec![
                encode_context_convergence_event(receipt),
            ]));
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(Vec::new()));

        fixture
            .subscriber
            .ensure(query_context, Vec::new())
            .expect("the convergence-aware subscription starts");
        let requests = subscribe_requests(&fixture.loopback.peer, 2).await;

        let (first_context, first_tasks, first_context_cursor) =
            decode_context_aware_subscribe_task_status(
                &requests[0],
                FieldPath::root("first_context_aware_subscription"),
            )
            .expect("a legal context-aware subscription");
        assert_eq!(first_context, query_context);
        assert!(first_tasks.is_empty());
        let first_context_cursor = first_context_cursor.expect("convergence is always requested");
        assert_eq!(first_context_cursor.context(), query_context);
        assert_eq!(first_context_cursor.current_version(), None);

        let (_, _, replayed_context_cursor) = decode_context_aware_subscribe_task_status(
            &requests[1],
            FieldPath::root("replayed_context_aware_subscription"),
        )
        .expect("a legal replay");
        assert_eq!(
            replayed_context_cursor.and_then(|cursor| cursor.current_version()),
            Some(QueryContextConvergenceVersion::FIRST),
            "the stream advances its context cursor only after the inbox retained the receipt"
        );
        assert_eq!(fixture.convergence_intake.pending(), 1);
        assert_eq!(
            fixture
                .convergence_intake
                .peek_retained()
                .expect("the complete convergence receipt is retained")
                .receipt(),
            receipt
        );
        assert_eq!(
            fixture.status_intake.queued(),
            0,
            "context convergence never enters the task status route"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn task_reconciliation_reopens_one_stream_and_preserves_the_context_cursor() {
        let backend = BackendProcessId::new_v7();
        let fixture = context_aware_subscriber_fixture(
            Loopback::start().await,
            backend,
            2,
            NonZeroUsize::MIN,
        );
        let query_context = context(backend);
        let task = identity(1, backend);
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![
                encode_context_convergence_event(convergence_receipt(query_context)),
            ]));
        fixture
            .subscriber
            .ensure(query_context, vec![TaskStatusCursor::unobserved(task)])
            .expect("the first subscription starts");
        for _ in 0..600 {
            if fixture.convergence_intake.pending() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(fixture.convergence_intake.pending(), 1);

        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(Vec::new()));
        fixture
            .subscriber
            .resubscribe(
                query_context,
                vec![TaskStatusCursor::at(task, TaskStatusVersion::FIRST)],
            )
            .expect("TaskRound reconciles its authoritative task cursor");
        let requests = subscribe_requests(&fixture.loopback.peer, 2).await;
        for _ in 0..600 {
            if fixture.loopback.peer.open_held_subscriptions() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            fixture.loopback.peer.open_held_subscriptions(),
            1,
            "reconciliation closes the old stream before the same loop opens its replacement"
        );
        let (_, task_cursors, context_cursor) = decode_context_aware_subscribe_task_status(
            &requests[1],
            FieldPath::root("reconciled_context_aware_subscription"),
        )
        .expect("a legal reconciled subscription");
        assert_eq!(
            task_cursors[0].current_version(),
            Some(TaskStatusVersion::FIRST)
        );
        assert_eq!(
            context_cursor.and_then(|cursor| cursor.current_version()),
            Some(QueryContextConvergenceVersion::FIRST),
            "task reconciliation cannot reset the independently retained convergence cursor"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn convergence_overflow_waits_for_capacity_without_spending_the_network_budget() {
        let backend = BackendProcessId::new_v7();
        let mut fixture = context_aware_subscriber_fixture(
            Loopback::start().await,
            backend,
            1,
            NonZeroUsize::MIN,
        );
        let first = context(backend);
        let second = context(backend);
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![
                encode_context_convergence_event(convergence_receipt(first)),
            ]));
        fixture
            .subscriber
            .ensure(first, Vec::new())
            .expect("the first subscription starts");
        for _ in 0..600 {
            if fixture.convergence_intake.pending() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(fixture.convergence_intake.pending(), 1);

        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![
                encode_context_convergence_event(convergence_receipt(second)),
            ]));
        fixture
            .subscriber
            .ensure(second, Vec::new())
            .expect("the second subscription starts");
        let _ = subscribe_requests(&fixture.loopback.peer, 2).await;
        tokio::time::sleep(RESUBSCRIBE_BACKOFF_STEP * 2).await;
        assert_eq!(
            fixture.loopback.peer.subscribed().len(),
            2,
            "local convergence backpressure does not reconnect or consume network budget"
        );
        assert_ne!(
            fixture.subscriber.state(second),
            Some(SubscriptionState::BudgetExhausted)
        );

        let first_lease = fixture
            .convergence_intake
            .peek_retained()
            .expect("the first context occupies the only slot");
        assert_eq!(first_lease.receipt(), convergence_receipt(first));
        assert_eq!(
            first_lease.ack(),
            crate::task_execution::context_convergence::ContextConvergenceRetainedAck::Released
        );
        for _ in 0..600 {
            let Some(lease) = fixture.convergence_intake.peek_retained() else {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            assert_eq!(
                lease.receipt(),
                convergence_receipt(second),
                "the blocked transport retries the same complete receipt after the epoch changes"
            );
            return;
        }
        panic!("the second convergence receipt was not retried after capacity became available");
    }

    #[tokio::test]
    async fn convergence_protocol_errors_leave_the_cursor_unchanged() {
        let backend = BackendProcessId::new_v7();
        let exact = context(backend);
        let stranger = context(backend);
        let owner = ContextConvergenceIntake::bounded(NonZeroUsize::MIN);
        let handle = owner.handle();
        let second_version = QueryContextConvergenceVersion::FIRST
            .next()
            .expect("version two");
        let mut cursor = Some(QueryContextConvergenceCursor::at(exact, second_version));
        assert_eq!(
            observe_context_convergence(
                exact,
                convergence_receipt(exact),
                Some(&handle),
                &mut cursor,
            )
            .await,
            Err(SubscriptionState::Rejected)
        );
        assert_eq!(
            cursor.and_then(|cursor| cursor.current_version()),
            Some(second_version),
            "an older receipt cannot move the subscriber cursor backwards"
        );
        assert_eq!(owner.pending(), 0);

        let mut cursor = Some(QueryContextConvergenceCursor::unobserved(exact));
        assert_eq!(
            observe_context_convergence(
                exact,
                convergence_receipt(stranger),
                Some(&handle),
                &mut cursor,
            )
            .await,
            Err(SubscriptionState::Rejected)
        );
        assert_eq!(cursor.and_then(|cursor| cursor.current_version()), None);
        assert_eq!(owner.pending(), 0);

        let closed_owner = ContextConvergenceIntake::bounded(NonZeroUsize::MIN);
        let closed = closed_owner.handle();
        drop(closed_owner);
        assert_eq!(
            observe_context_convergence(
                exact,
                convergence_receipt(exact),
                Some(&closed),
                &mut cursor,
            )
            .await,
            Err(SubscriptionState::Rejected)
        );
        assert_eq!(cursor.and_then(|cursor| cursor.current_version()), None);
    }

    #[tokio::test]
    async fn task_status_replay_cannot_move_the_transport_cursor_backwards() {
        let backend = BackendProcessId::new_v7();
        let query_context = context(backend);
        let task = identity(1, backend);
        let second_version = TaskStatusVersion::FIRST.next().expect("version two");
        let intake = StatusIntake::new(2, Arc::new(CountingWake::default()));
        let mut cursors = BTreeMap::from([(task, TaskStatusCursor::at(task, second_version))]);

        assert_eq!(
            observe_stream_event(
                query_context,
                &encode_status_event(&status_at(task, TaskStatusVersion::FIRST)),
                &mut cursors,
                &intake.handle(),
                None,
                &mut None,
            )
            .await,
            Err(SubscriptionState::Rejected)
        );
        assert_eq!(
            cursors
                .get(&task)
                .and_then(|cursor| cursor.current_version()),
            Some(second_version)
        );
        assert_eq!(intake.queued(), 0);

        assert_eq!(
            observe_stream_event(
                query_context,
                &encode_status_event(&status_at(task, second_version)),
                &mut cursors,
                &intake.handle(),
                None,
                &mut None,
            )
            .await,
            Ok(StreamObservation::Delivered)
        );
        assert_eq!(
            cursors
                .get(&task)
                .and_then(|cursor| cursor.current_version()),
            Some(second_version)
        );
        assert_eq!(
            intake.queued(),
            0,
            "an exact replay is idempotent and does not consume bounded intake capacity"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_dropped_status_stream_resubscribes_from_its_cursors() {
        let backend = BackendProcessId::new_v7();
        let fixture = subscriber_fixture(Loopback::start().await, backend, 4);
        let query_context = context(backend);
        let task = identity(1, backend);
        let status = TaskStatus::created(task);
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenClose(vec![encode_status_event(
                &status,
            )]));
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(Vec::new()));

        fixture
            .subscriber
            .ensure(query_context, vec![TaskStatusCursor::unobserved(task)])
            .expect("the subscription starts");
        let requests = subscribe_requests(&fixture.loopback.peer, 2).await;

        let (_, first) =
            decode_subscribe_task_status(&requests[0], FieldPath::root("first_subscription"))
                .expect("a legal subscription");
        assert_eq!(first[0].current_version(), None, "nothing observed yet");
        let (_, replayed) =
            decode_subscribe_task_status(&requests[1], FieldPath::root("second_subscription"))
                .expect("a legal subscription");
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].identity(), task);
        assert_eq!(
            replayed[0].current_version(),
            Some(TaskStatusVersion::FIRST),
            "the replay resumes from the version this stream observed"
        );

        assert_eq!(fixture.intake.queued(), 1, "the snapshot was enqueued once");
        let mut runner = fixture.intake.try_enter().expect("the runner slot is free");
        let (observation_loss, statuses) = runner.drain_statuses(8);
        assert!(
            observation_loss,
            "a lost stream is reported as observation loss"
        );
        assert_eq!(
            statuses,
            vec![StatusEvent::Published(status)],
            "the receive path enqueues the snapshot and nothing else"
        );
        drop(runner);
        assert_eq!(
            fixture.acks.queued(),
            0,
            "a lost stream never settles an operation, aborts a task, or touches a lease"
        );
        assert!(
            !fixture
                .subscriber
                .state(query_context)
                .expect("the subscription is tracked")
                .is_fatal(),
            "a lost observation channel is not fatal to the attempt"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_status_receive_path_only_enqueues_and_wakes() {
        let backend = BackendProcessId::new_v7();
        let fixture = subscriber_fixture(Loopback::start().await, backend, 2);
        let query_context = context(backend);
        let first = identity(1, backend);
        let second = identity(2, backend);
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![
                encode_status_event(&TaskStatus::created(first)),
                encode_status_event(&TaskStatus::created(second)),
            ]));

        fixture
            .subscriber
            .ensure(query_context, Vec::new())
            .expect("the subscription starts");
        for _ in 0..600 {
            if fixture.intake.queued() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(fixture.intake.queued(), 2);
        assert!(
            fixture.wake.count() >= 2,
            "every publish wakes the serial runner"
        );
        assert_eq!(fixture.acks.queued(), 0);
        let mut runner = fixture.intake.try_enter().expect("the runner slot is free");
        let (observation_loss, statuses) = runner.drain_statuses(8);
        assert!(!observation_loss, "a live stream reports no loss");
        assert_eq!(
            statuses,
            vec![
                StatusEvent::Published(TaskStatus::created(first)),
                StatusEvent::Published(TaskStatus::created(second)),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_status_event_from_another_process_is_fatal() {
        let backend = BackendProcessId::new_v7();
        let replacement = BackendProcessId::new_v7();
        let fixture = subscriber_fixture(Loopback::start().await, backend, 2);
        let query_context = context(backend);
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![encode_status_event(
                &TaskStatus::created(identity(1, replacement)),
            )]));

        fixture
            .subscriber
            .ensure(query_context, Vec::new())
            .expect("the subscription starts");
        let mut observed = None;
        for _ in 0..600 {
            let state = fixture.subscriber.state(query_context);
            if state == Some(SubscriptionState::ProcessMismatch) {
                observed = state;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(observed, Some(SubscriptionState::ProcessMismatch));
        assert!(observed.expect("a settled state").is_fatal());
        let mut runner = fixture.intake.try_enter().expect("the runner is available");
        let (observation_incomplete, statuses) = runner.drain_statuses(8);
        assert!(
            observation_incomplete,
            "a fatal status identity mismatch fences a pending success seal"
        );
        assert!(
            statuses.is_empty(),
            "an event from a replaced process is never published"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_status_event_from_another_query_execution_is_fatal() {
        let backend = BackendProcessId::new_v7();
        let fixture = subscriber_fixture(Loopback::start().await, backend, 2);
        let query_context = context(backend);
        let foreign_execution = QueryExecutionId::new(
            execution_id().query_id(),
            AttemptId::new(2).expect("attempt two is nonzero"),
        )
        .expect("a nonzero query id");
        let foreign_task = TaskIdentity::new(
            foreign_execution,
            StageId::new(1).expect("a nonzero stage id"),
            TaskId::new(1).expect("a nonzero task id"),
            backend,
        );
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![encode_status_event(
                &TaskStatus::created(foreign_task),
            )]));

        fixture
            .subscriber
            .ensure(query_context, Vec::new())
            .expect("the subscription starts");
        let mut observed = None;
        for _ in 0..600 {
            let state = fixture.subscriber.state(query_context);
            if state == Some(SubscriptionState::QueryMismatch) {
                observed = state;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(observed, Some(SubscriptionState::QueryMismatch));
        assert!(observed.expect("a settled state").is_fatal());
        assert_eq!(
            fixture.intake.queued(),
            0,
            "an event from another query execution is never published"
        );
    }

    #[test]
    fn intake_overflow_preserves_the_last_enqueued_status_cursor() {
        let backend = BackendProcessId::new_v7();
        let query_context = context(backend);
        let task = identity(1, backend);
        let first = TaskStatus::created(task);
        let second_version = TaskStatusVersion::FIRST.next().expect("version two");
        let second = status_at(task, second_version);
        let intake = StatusIntake::new(1, Arc::new(CountingWake::default()));
        let handle = intake.handle();
        let mut cursors = BTreeMap::from([(task, TaskStatusCursor::unobserved(task))]);

        assert_eq!(
            observe_event(
                query_context,
                &encode_status_event(&first),
                &mut cursors,
                &handle,
            ),
            Ok(())
        );
        assert_eq!(
            cursors
                .get(&task)
                .and_then(|cursor| cursor.current_version()),
            Some(TaskStatusVersion::FIRST)
        );
        assert_eq!(
            observe_event(
                query_context,
                &encode_status_event(&second),
                &mut cursors,
                &handle,
            ),
            Err(SubscriptionState::Resubscribing)
        );
        assert_eq!(
            cursors
                .get(&task)
                .and_then(|cursor| cursor.current_version()),
            Some(TaskStatusVersion::FIRST),
            "a status the bounded intake did not retain cannot advance the stream cursor"
        );
        let mut runner = intake.try_enter().expect("the runner slot is free");
        let (observation_loss, statuses) = runner.drain_statuses(2);
        assert!(observation_loss);
        assert_eq!(statuses, vec![StatusEvent::Published(first)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn intake_overflow_waits_for_task_round_resubscription_without_spending_error_budget() {
        let backend = BackendProcessId::new_v7();
        let loopback = Loopback::start().await;
        let data_runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let intake = StatusIntake::new(1, Arc::new(CountingWake::default()));
        let subscriber = TaskStatusSubscriber::new(
            &[(backend, loopback.endpoint.clone())],
            intake.handle(),
            1,
            data_runtime,
        )
        .expect("one frozen backend target");
        let query_context = context(backend);
        let task = identity(1, backend);
        let first = TaskStatus::created(task);
        let second = status_at(task, TaskStatusVersion::FIRST.next().expect("version two"));
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![
                encode_status_event(&first),
                encode_status_event(&second),
            ]));

        subscriber
            .ensure(query_context, vec![TaskStatusCursor::unobserved(task)])
            .expect("the subscription starts");
        for _ in 0..600 {
            if subscriber.state(query_context) == Some(SubscriptionState::Resubscribing) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            subscriber.state(query_context),
            Some(SubscriptionState::Resubscribing),
            "local overflow waits for the serial runner instead of exhausting the network budget"
        );
        tokio::time::sleep(RESUBSCRIBE_BACKOFF_STEP * 2).await;
        assert_eq!(
            loopback.peer.subscribed().len(),
            1,
            "the receive task never privately resubscribes past a locally lost status"
        );

        let mut runner = intake.try_enter().expect("the runner slot is free");
        let (observation_loss, statuses) = runner.drain_statuses(2);
        assert!(observation_loss);
        assert_eq!(statuses, vec![StatusEvent::Published(first)]);
        drop(runner);

        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(Vec::new()));
        subscriber
            .resubscribe(
                query_context,
                vec![TaskStatusCursor::at(task, TaskStatusVersion::FIRST)],
            )
            .expect("TaskRound replaces the subscription from its cursor");
        let requests = subscribe_requests(&loopback.peer, 2).await;
        let (_, cursors) = decode_subscribe_task_status(
            &requests[1],
            FieldPath::root("post_overflow_subscription"),
        )
        .expect("a legal subscription");
        assert_eq!(cursors.len(), 1);
        assert_eq!(cursors[0].identity(), task);
        assert_eq!(
            cursors[0].current_version(),
            Some(TaskStatusVersion::FIRST),
            "TaskRound's applied cursor is the only position allowed after overflow"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_rejected_subscription_is_not_resubscribed() {
        let backend = BackendProcessId::new_v7();
        let fixture = subscriber_fixture(Loopback::start().await, backend, 4);
        let query_context = context(backend);
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::Reject(tonic::Code::InvalidArgument));

        fixture
            .subscriber
            .ensure(query_context, Vec::new())
            .expect("the subscription starts");
        for _ in 0..600 {
            if fixture.subscriber.state(query_context) == Some(SubscriptionState::Rejected) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(
            fixture.subscriber.state(query_context),
            Some(SubscriptionState::Rejected)
        );
        assert_eq!(
            fixture.loopback.peer.subscribed().len(),
            1,
            "a typed rejection is never replayed"
        );
    }

    #[test]
    fn a_subscription_needs_a_nonzero_error_budget() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a test runtime");
        let data_runtime = FrontendDataRuntime::new(runtime.handle().clone());
        let error = TaskStatusSubscriber::new(
            &[(
                BackendProcessId::new_v7(),
                RuntimeEndpoint::new("127.0.0.1", 9000).expect("a valid endpoint"),
            )],
            StatusIntake::new(1, Arc::new(CountingWake::default())).handle(),
            0,
            data_runtime,
        )
        .expect_err("a zero budget cannot bound anything");
        assert!(error.contains("nonzero"));
    }

    #[test]
    fn every_task_transport_collector_registers() {
        let registry = Registry::new();
        register_metric_collectors(&registry).expect("a fresh registry accepts every collector");
        observe_dispatch_lane(DispatchLane::Create, 3, 1);
        let names = registry
            .gather()
            .into_iter()
            .map(|family| family.get_name().to_owned())
            .collect::<Vec<_>>();
        assert!(names.contains(&"novarocks_task_dispatch_lane_depth".to_owned()));
    }

    // -----------------------------------------------------------------------
    // Acknowledgement bodies
    // -----------------------------------------------------------------------

    #[test]
    fn an_admission_request_and_receipt_keep_the_exact_context_validity_and_compatibility() {
        let backend = BackendProcessId::new_v7();
        let context = context(backend);
        let valid_for = LeaseValidFor::new(Duration::from_secs(10)).expect("a legal validity");
        let compatibility = novarocks_types::NativeCompatibilityId::new([0x71; 32]);
        let admission_epoch_capability =
            novarocks_execution::task_execution::AdmissionEpochCapability::try_from_bytes(
                [0x61; 16],
            )
            .expect("nonzero epoch");
        let request = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context,
            valid_for,
            compatibility,
            admission_epoch_capability,
        );
        let encoded = encode_operation(
            &OperationIntent::AcquireQueryContextAdmissionTicket(request),
            &test_attempt_facts(),
        )
        .expect("the admission request is encodable");
        let Some(proto::task_operation::Operation::AcquireQueryContextAdmissionTicket(encoded)) =
            encoded.operation
        else {
            panic!("admission uses its own batch operation");
        };
        assert_eq!(encoded.valid_for_millis, 10_000);
        assert_eq!(
            encoded
                .native_compatibility_id
                .expect("compatibility is required")
                .value,
            compatibility.as_bytes()
        );

        let ticket = AdmissionTicketId::try_from_bytes([0x57; 16]).expect("the ticket is nonzero");
        let receipt = proto::TaskOperationReceipt {
            operation_id: None,
            outcome: 0,
            safe_detail: String::new(),
            safe_field_path: None,
            ack: Some(
                proto::task_operation_receipt::Ack::QueryContextAdmissionTicket(
                    codec::encode_query_context_admission_ticket_ack(
                        novarocks_execution::task_execution::QueryContextAdmissionTicketReceipt::new(
                            ticket,
                            context,
                            valid_for,
                        ),
                    ),
                ),
            ),
        };
        let payload = decode_ack(
            OperationKind::AcquireQueryContextAdmissionTicket,
            AckAddress::Admission(request),
            &receipt,
        )
        .expect("the exact receipt decodes");
        assert!(matches!(
            payload,
            AckPayload::AdmissionTicket(receipt) if receipt.ticket_id() == ticket
        ));
    }

    /// A create acknowledgement is the frontend's only proof that a task was
    /// installed, and its status snapshot closes the window between creating a
    /// task and observing it. These read the real wire bodies through the real
    /// codec.
    #[test]
    fn an_applied_create_acknowledgement_carries_the_task_status() {
        let backend = BackendProcessId::new_v7();
        let mine = identity(1, backend);
        let status = TaskStatus::created(mine);
        let ack = proto::task_operation_receipt::Ack::CreateTask(
            codec::encode_create_task_ack(&CreateTaskReceipt::new(
                mine,
                Vec::new(),
                status.clone(),
            ))
            .expect("an applied create encodes"),
        );
        let receipt = proto::TaskOperationReceipt {
            operation_id: None,
            outcome: 0,
            safe_detail: String::new(),
            safe_field_path: None,
            ack: Some(ack.clone()),
        };

        let payload = decode_ack(OperationKind::CreateTask, AckAddress::Task(mine), &receipt)
            .expect("its own acknowledgement decodes");
        let AckPayload::Create(decoded) = payload else {
            panic!("a create acknowledgement decodes as a create payload");
        };
        assert_eq!(decoded.identity(), mine);
        assert_eq!(decoded.current_status().state(), status.state());

        // The same body addressed elsewhere is not this request's proof.
        let theirs = identity(2, backend);
        assert!(
            decode_ack(
                OperationKind::CreateTask,
                AckAddress::Task(theirs),
                &receipt
            )
            .is_err(),
            "an acknowledgement for another task must not settle this one"
        );

        // An applied create with no body at all is a protocol violation, not
        // a create that installed nothing.
        let empty = proto::TaskOperationReceipt {
            ack: None,
            ..receipt.clone()
        };
        assert!(
            decode_ack(OperationKind::CreateTask, AckAddress::Task(mine), &empty).is_err(),
            "an applied create must carry its acknowledgement"
        );

        // A body of the wrong kind is refused rather than read as empty.
        assert!(
            decode_ack(OperationKind::UpdateTask, AckAddress::Task(mine), &receipt).is_err(),
            "a create body must not settle an update"
        );
    }

    #[tokio::test]
    async fn an_applied_operation_whose_body_cannot_be_read_settles_as_a_refusal() {
        // The whole point of reading the body is that an applied operation the
        // frontend cannot interpret must not settle as applied-with-nothing.
        // A lease renewal consumes its context body, so a create body here is
        // a producer bug the frontend must refuse rather than absorb.
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let batch = released_batch(backend, vec![renew_intent(backend)]);
        let task = identity(1, backend);
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::WithAck(vec![(
                OperationOutcome::Accepted,
                Some(proto::task_operation_receipt::Ack::CreateTask(
                    codec::encode_create_task_ack(&CreateTaskReceipt::new(
                        task,
                        Vec::new(),
                        TaskStatus::created(task),
                    ))
                    .expect("an applied create encodes"),
                )),
            )]));

        submit_batch(&fixture.sink, batch);
        let acks = settled(&fixture.acks, 1).await;
        assert_eq!(
            acks[0].worker_outcome(),
            Some(OperationOutcome::InvalidStateOrRequest)
        );
        assert_eq!(*acks[0].payload(), AckPayload::None);
    }

    #[tokio::test]
    async fn every_legal_abort_closing_outcome_keeps_its_context_receipt() {
        for (outcome, state) in [
            (OperationOutcome::Accepted, QueryContextState::Aborting),
            (OperationOutcome::Idempotent, QueryContextState::Aborting),
            (
                OperationOutcome::ContextTerminalReceipt,
                QueryContextState::Releasing,
            ),
            (
                OperationOutcome::LeaseExpired,
                QueryContextState::TerminalRetained,
            ),
            (OperationOutcome::Gone, QueryContextState::Gone),
        ] {
            let backend = BackendProcessId::new_v7();
            let context = context(backend);
            let fixture = sink_fixture(Loopback::start().await, backend);
            fixture
                .loopback
                .peer
                .expect_apply(ApplyAnswer::WithAck(vec![(
                    outcome,
                    Some(proto::task_operation_receipt::Ack::QueryContext(
                        encode_query_context_ack(
                            &QueryContextReceipt::new(context, state),
                            Some(AbortCause::QueryFailed),
                        )
                        .expect("a closing context receipt encodes"),
                    )),
                )]));
            submit_batch(
                &fixture.sink,
                released_batch(backend, vec![abort_intent(context)]),
            );
            let acks = settled(&fixture.acks, 1).await;
            assert_eq!(acks[0].worker_outcome(), Some(outcome));
            assert!(matches!(
                acks[0].payload(),
                AckPayload::Context(receipt)
                    if receipt.context() == context && receipt.state() == state
            ));
        }
    }

    #[tokio::test]
    async fn a_legal_abort_closing_outcome_without_its_body_fails_closed() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::WithAck(vec![(
                OperationOutcome::ContextTerminalReceipt,
                None,
            )]));
        submit_batch(
            &fixture.sink,
            released_batch(backend, vec![abort_intent(context(backend))]),
        );
        let acks = settled(&fixture.acks, 1).await;
        assert_eq!(
            acks[0].worker_outcome(),
            Some(OperationOutcome::InvalidStateOrRequest)
        );
        assert_eq!(*acks[0].payload(), AckPayload::None);
    }

    /// A cancel's status body is on the wire but has exactly one consumer, and
    /// it is not this path. This pins that as a decision rather than an
    /// oversight: an unread body must not turn an applied cancel into a
    /// refusal.
    #[tokio::test]
    async fn a_cancel_settles_on_its_outcome_and_ignores_its_status_body() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let batch = released_batch(backend, vec![cancel_intent(1, backend)]);
        let task = identity(1, backend);
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::WithAck(vec![(
                OperationOutcome::Accepted,
                Some(proto::task_operation_receipt::Ack::CancelTask(
                    novarocks_task_codec::status::encode_task_status(&TaskStatus::created(task)),
                )),
            )]));

        submit_batch(&fixture.sink, batch);
        let acks = settled(&fixture.acks, 1).await;
        assert_eq!(acks[0].worker_outcome(), Some(OperationOutcome::Accepted));
        assert_eq!(*acks[0].payload(), AckPayload::None);
    }
}
