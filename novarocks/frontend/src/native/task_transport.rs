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

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use once_cell::sync::Lazy;
use prometheus::{
    HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry,
};

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::{
    DispatchLane, OperationKind, OperationOutcome, QueryContextRef, TaskIdentity, TaskOperationId,
    TaskStatusCursor, TransportBudget, UpdateQueryContext,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::task_execution::descriptor::WireFragmentPlan;
use novarocks_proto_codec::task_execution::domain as codec_domain;
use novarocks_proto_codec::task_execution::domain::{stored_credential, stored_message};
use novarocks_proto_codec::task_execution::operation as codec;
use novarocks_proto_codec::task_execution::operation::{
    ReceiptHeader, StatusStreamEvent, decode_receipt_batch, decode_status_event,
    encode_abort_query_context, encode_advance_query_context_domain, encode_cancel_task,
    encode_create_task, encode_establish_query_context, encode_operation_batch,
    encode_release_query_context, encode_renew_lease, encode_subscribe_task_status,
    encode_update_task,
};
use novarocks_proto_models::catalog::CatalogSet;
use novarocks_proto_models::novarocks as proto;
use novarocks_types::NativeEndpoint;
use novarocks_types::identity::BackendProcessId;

use novarocks_execution::task_execution::operation::QueryContextReceipt;

use crate::task_execution::intent::{
    AckPayload, DispatchBatch, OperationAcknowledgement, OperationIntent, TaskOperationSink,
};
use crate::task_execution::status_intake::{StatusEvent, StatusIntakeHandle, StatusIntakeWake};

use super::data_runtime::FrontendDataRuntime;
use super::transport::{ChannelAcquisitionError, Client};

/// How long a lost subscription waits before its next attempt, per failure in
/// the current run.
///
/// The error budget alone bounds the number of attempts; this bounds their
/// rate, so a flapping backend cannot spend the whole budget inside one tick.
const RESUBSCRIBE_BACKOFF_STEP: Duration = Duration::from_millis(100);

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
/// recover them. The two attempt-level facts an establish needs -- the query
/// options and the compatibility identity -- come from the sink, because they
/// belong to the attempt rather than to any one operation and the neutral
/// request deliberately does not name a generated type.
fn encode_operation(
    intent: &OperationIntent,
    attempt: &AttemptWireFacts,
) -> Result<proto::TaskOperation, String> {
    match intent {
        OperationIntent::CreateTask(request) => {
            let fragment = wire_fragment_plan(request.descriptor().plan())?;
            let domains = encode_task_domains(request.initial_domains())?;
            Ok(encode_create_task(request, fragment, domains))
        }
        OperationIntent::UpdateTask(request) => {
            let domains = encode_task_domains(request.domains())?;
            Ok(encode_update_task(request, domains))
        }
        OperationIntent::UpdateQueryContext(request) => {
            encode_query_context_operation(request, attempt)
        }
        OperationIntent::CancelTask(request) => Ok(encode_cancel_task(*request)),
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

/// The two attempt-level facts an establish carries that no intent names.
#[derive(Clone, Debug)]
pub(crate) struct AttemptWireFacts {
    pub(crate) query_options: proto::QueryOptions,
    pub(crate) native_compatibility_id: Option<proto::NativeCompatibilityId>,
}

fn wire_fragment_plan(
    plan: &Arc<dyn novarocks_execution::task_execution::descriptor::PhysicalFragmentPlan>,
) -> Result<&WireFragmentPlan, String> {
    plan.stored_representation()
        .and_then(|stored| stored.downcast_ref::<WireFragmentPlan>())
        .ok_or_else(|| "task descriptor plan is not a codec-produced fragment plan".to_owned())
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
                    envelopes: credential.envelopes().to_vec(),
                },
                attempt.query_options.clone(),
                attempt.native_compatibility_id.clone(),
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

/// Whether an applied acknowledgement of this kind carries a body its owner
/// consumes.
///
/// A cancel and an abort are answered by a receipt header alone: the terminal
/// fact still arrives as a published status, so their owners read the outcome
/// and nothing else.
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
        OperationKind::CreateTask
            | OperationKind::UpdateTask
            | OperationKind::UpdateQueryContext
            | OperationKind::ReleaseQueryContext
    )
}

const fn is_applied(outcome: OperationOutcome) -> bool {
    matches!(
        outcome,
        OperationOutcome::Accepted | OperationOutcome::Idempotent
    )
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
    Task(TaskIdentity),
    Context(QueryContextRef),
    /// The kind carries no acknowledgement body.
    None,
}

impl AckAddress {
    fn of(intent: &OperationIntent) -> Self {
        match intent {
            OperationIntent::CreateTask(request) => Self::Task(request.identity()),
            OperationIntent::UpdateTask(request) => Self::Task(request.identity()),
            OperationIntent::CancelTask(request) => Self::Task(request.identity()),
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
        .ok_or_else(|| format!("an applied {kind} carries no acknowledgement body"))?;
    match (kind, body, address) {
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
            OperationKind::UpdateQueryContext,
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
            OperationKind::ReleaseQueryContext,
            proto::task_operation_receipt::Ack::ReleaseQueryContext(ack),
            AckAddress::Context(context),
        ) => {
            let (acked, outcome, state, cause) =
                codec::decode_release_ack(ack, path()).map_err(|error| error.to_string())?;
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
                receipt: QueryContextReceipt::new(acked, state),
                outcome,
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
    queue: Mutex<Vec<OperationAcknowledgement>>,
    wake: Arc<dyn StatusIntakeWake>,
}

/// The only handle the send path holds.
///
/// It can enqueue one settled acknowledgement and wake the runner. It has no
/// path to a task, a stage, a context, or a lease, so an operation is never
/// settled on a transport thread.
#[derive(Clone, Debug)]
pub(crate) struct TaskAckIntakeHandle {
    inner: Arc<TaskAckIntakeInner>,
}

impl TaskAckIntakeHandle {
    /// Enqueues one settled acknowledgement and wakes the runner.
    ///
    /// This queue is deliberately not capacity-bounded. Every entry
    /// corresponds to one dispatch permit the serial runner frees when it
    /// settles the operation, so its depth is bounded by the dispatch budget;
    /// dropping an entry instead would leak that permit and stall the attempt
    /// with no cause anyone could read.
    pub(crate) fn publish(&self, ack: OperationAcknowledgement) {
        self.inner
            .queue
            .lock()
            .expect("task acknowledgement queue")
            .push(ack);
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
            .len()
    }

    /// Takes every queued acknowledgement, in the order the transport settled
    /// them.
    pub(crate) fn drain(&self) -> Vec<OperationAcknowledgement> {
        std::mem::take(&mut *self.inner.queue.lock().expect("task acknowledgement queue"))
    }
}

// ---------------------------------------------------------------------------
// Transport classification
// ---------------------------------------------------------------------------

/// Classifies one unary RPC status by type.
///
/// Only a status that leaves the remote outcome genuinely unknown becomes
/// [`OperationOutcome::RetryableTransportUnknown`], the single category the
/// protocol allows the identical immutable request to be resent under.
/// Everything else is settled and fails closed, however transient its wording
/// looks: this never reads a status message.
fn classify_apply_status(status: &tonic::Status) -> OperationOutcome {
    match status.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown => OperationOutcome::RetryableTransportUnknown,
        // A typed capacity rejection. The backend answered, so there is
        // nothing unknown about it and no older path to degrade onto.
        tonic::Code::ResourceExhausted => OperationOutcome::ResourceExhausted,
        _ => OperationOutcome::InvalidStateOrRequest,
    }
}

/// Classifies a channel acquisition failure by type.
///
/// URI and connector construction are deterministic local failures that
/// resending cannot repair. A completed connector that then cannot dial or
/// establish its HTTP/2 stream leaves the remote outcome unknown.
fn classify_channel_error(error: &ChannelAcquisitionError) -> OperationOutcome {
    match error {
        ChannelAcquisitionError::Fatal(_) => OperationOutcome::InvalidStateOrRequest,
        ChannelAcquisitionError::RetryableNetwork(_) => OperationOutcome::RetryableTransportUnknown,
    }
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
    endpoint: NativeEndpoint,
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
            endpoint: endpoint.native_endpoint().clone(),
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
        Ok(Self {
            targets: freeze_targets(backends, &data_runtime)?,
            transport,
            attempt,
            acks,
            data_runtime,
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
    fn submit(&self, batch: &DispatchBatch) {
        let backend = batch.backend();
        let Some(target) = self.targets.get(&backend) else {
            // The batch addresses a process this attempt never froze, so no
            // endpoint could legally answer it.
            self.settle_batch_locally(
                batch,
                OperationOutcome::IdentityMismatch,
                REFUSAL_UNKNOWN_BACKEND,
            );
            return;
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
                batch,
                OperationOutcome::IdentityMismatch,
                REFUSAL_PROCESS_MISMATCH,
            );
            return;
        }

        let mut operations = Vec::with_capacity(batch.operations().len());
        let mut sent = Vec::with_capacity(batch.operations().len());
        for intent in batch.operations() {
            let encoded = encode_operation(intent, &self.attempt);
            match encoded {
                Ok(operation) => {
                    operations.push(operation);
                    sent.push(SentOperation {
                        operation_id: intent.operation_id(),
                        kind: intent.kind(),
                        lease_renewal: is_lease_renewal(intent),
                        address: AckAddress::of(intent),
                    });
                }
                Err(detail) => {
                    // One item that cannot be expressed on the wire is that
                    // item's own failure. Every other item keeps its own
                    // receipt, which is the only property a batch has.
                    tracing::warn!(
                        kind = intent.kind().as_str(),
                        detail,
                        "task operation cannot be encoded"
                    );
                    observe_refusal(REFUSAL_UNENCODABLE, 1);
                    self.settle_locally(
                        intent.operation_id(),
                        intent.kind(),
                        is_lease_renewal(intent),
                        OperationOutcome::InvalidStateOrRequest,
                    );
                }
            }
        }
        if operations.is_empty() {
            return;
        }

        let deadline = operations
            .iter()
            .filter_map(|operation| operation.envelope.as_ref())
            .map(|envelope| Duration::from_millis(envelope.max_wait_millis))
            .max()
            .unwrap_or_default();
        let items = operations.len();
        // The receiver applies the same bound. Applying it here as well turns
        // an oversized batch into a local failure with an exact cause, instead
        // of a round trip rejected after crossing the wire and consuming the
        // operations' own deadlines.
        let request = match encode_operation_batch(operations, self.transport) {
            Ok(request) => request,
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
                return;
            }
        };
        observe_batch(batch.lane(), items, prost::Message::encoded_len(&request));

        let client = target.client.clone();
        let endpoint = target.endpoint.clone();
        let acks = self.acks.clone();
        let data_runtime = self.data_runtime.clone();
        // A submission never blocks on a round trip: the batch leaves on the
        // role's runtime and its receipts come back through the intake, which
        // is what lets the frontend keep one serial runner.
        self.data_runtime.spawn(async move {
            apply_operations(
                ApplySend {
                    client,
                    endpoint,
                    data_runtime,
                    acks,
                },
                request,
                sent,
                deadline,
            )
            .await;
        });
    }
}

/// Everything one send owns beyond its request.
struct ApplySend {
    client: Client,
    endpoint: NativeEndpoint,
    data_runtime: FrontendDataRuntime,
    acks: TaskAckIntakeHandle,
}

/// Sends one batch and publishes one acknowledgement per request item.
async fn apply_operations(
    send: ApplySend,
    request: proto::ApplyTaskOperationsRequest,
    sent: Vec<SentOperation>,
    deadline: Duration,
) {
    let response = match send_operations(&send.client, request, deadline).await {
        Ok(response) => response,
        Err(outcome) => {
            if outcome.is_retryable() {
                // An unknown outcome may have left the shared HTTP/2 stream
                // unusable. Drop the cached channel so the identical request
                // is replayed on a fresh one instead of stalling on a poisoned
                // cache.
                send.data_runtime.invalidate_channel(&send.endpoint);
            }
            publish_uniform(&send.acks, &sent, outcome);
            return;
        }
    };

    let ids = sent
        .iter()
        .map(|item| item.operation_id)
        .collect::<Vec<_>>();
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
            observe_refusal(REFUSAL_UNUSABLE_RESPONSE, sent.len());
            publish_uniform(&send.acks, &sent, OperationOutcome::InvalidStateOrRequest);
            return;
        }
    };

    for (item, (header, receipt)) in sent
        .iter()
        .zip(headers.iter().zip(response.receipts.iter()))
    {
        send.acks.publish(acknowledgement(item, header, receipt));
    }
}

fn publish_uniform(acks: &TaskAckIntakeHandle, sent: &[SentOperation], outcome: OperationOutcome) {
    for item in sent {
        observe_settled(item.kind, item.lease_renewal, outcome);
        acks.publish(OperationAcknowledgement::new(
            item.operation_id,
            item.kind,
            outcome,
            AckPayload::None,
        ));
    }
}

/// Builds one acknowledgement from one receipt, in its request item's order.
fn acknowledgement(
    item: &SentOperation,
    header: &ReceiptHeader,
    receipt: &proto::TaskOperationReceipt,
) -> OperationAcknowledgement {
    let outcome = header.outcome();
    if !is_applied(outcome) || !consumes_ack_body(item.kind) {
        observe_settled(item.kind, item.lease_renewal, outcome);
        return OperationAcknowledgement::new(
            item.operation_id,
            item.kind,
            outcome,
            AckPayload::None,
        );
    }
    match decode_ack(item.kind, item.address, receipt) {
        Ok(payload) => {
            observe_settled(item.kind, item.lease_renewal, outcome);
            OperationAcknowledgement::new(item.operation_id, item.kind, outcome, payload)
        }
        Err(detail) => {
            // An applied operation whose acknowledgement body cannot be read
            // is a protocol violation, not an operation that carried nothing.
            tracing::warn!(
                kind = item.kind.as_str(),
                detail,
                "applied task operation receipt has an unreadable acknowledgement"
            );
            observe_refusal(REFUSAL_UNUSABLE_ACK, 1);
            observe_settled(
                item.kind,
                item.lease_renewal,
                OperationOutcome::InvalidStateOrRequest,
            );
            OperationAcknowledgement::new(
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
    request: proto::ApplyTaskOperationsRequest,
    deadline: Duration,
) -> Result<proto::ApplyTaskOperationsResponse, OperationOutcome> {
    let expires_at = tokio::time::Instant::now() + deadline;
    let mut grpc = tokio::time::timeout_at(expires_at, client.grpc_with_channel_error())
        .await
        .map_err(|_| OperationOutcome::RetryableTransportUnknown)?
        .map_err(|error| {
            let outcome = classify_channel_error(&error);
            tracing::warn!(detail = %error, "apply_task_operations channel acquisition failed");
            outcome
        })?;
    let remaining = expires_at.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        // Nothing was submitted, so nothing was applied. It is still reported
        // as unknown rather than as a rejection, because a caller may only
        // conclude "not applied" from an answer it received.
        return Err(OperationOutcome::RetryableTransportUnknown);
    }
    let mut wire = tonic::Request::new(request);
    wire.set_timeout(remaining);
    tokio::time::timeout_at(expires_at, grpc.apply_task_operations(wire))
        .await
        .map_err(|_| OperationOutcome::RetryableTransportUnknown)?
        .map(tonic::Response::into_inner)
        .map_err(|status| {
            let outcome = classify_apply_status(&status);
            tracing::warn!(
                code = ?status.code(),
                detail = status.message(),
                "apply_task_operations rpc failed"
            );
            outcome
        })
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
        }
    }

    /// Whether this state is fatal to the query attempt.
    pub(crate) const fn is_fatal(self) -> bool {
        matches!(
            self,
            Self::BudgetExhausted | Self::Rejected | Self::ProcessMismatch
        )
    }
}

/// One running subscription. Dropping it stops the stream.
struct Subscription {
    state: Arc<Mutex<SubscriptionState>>,
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

    /// Replaces this context's subscription, replaying the given cursors.
    ///
    /// This is the answer to observation loss the runner saw on its own side,
    /// such as a full intake queue. Loss the stream itself sees is recovered
    /// by the subscription without anyone asking.
    pub(crate) fn resubscribe(
        &self,
        context: QueryContextRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<(), String> {
        let subscription = self.start(context, cursors)?;
        let mut active = self
            .active
            .lock()
            .map_err(|_| "task status subscription lock poisoned".to_owned())?;
        active.insert(context, subscription);
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
        let task = self.data_runtime.spawn(run_subscription(
            target.client.clone(),
            context,
            cursors,
            self.intake.clone(),
            self.error_budget,
            Arc::clone(&state),
        ));
        Ok(Subscription { state, task })
    }
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
    initial: Vec<TaskStatusCursor>,
    intake: StatusIntakeHandle,
    error_budget: u32,
    state: Arc<Mutex<SubscriptionState>>,
) {
    let mut cursors = initial
        .into_iter()
        .map(|cursor| (cursor.identity(), cursor))
        .collect::<BTreeMap<TaskIdentity, TaskStatusCursor>>();
    let mut failures = 0_u32;
    loop {
        match open_subscription(&client, context, &cursors).await {
            Ok(mut stream) => {
                set_state(&state, SubscriptionState::Live);
                let mut delivered = false;
                loop {
                    match stream.message().await {
                        Ok(Some(event)) => {
                            match observe_event(context, &event, &mut cursors, &intake) {
                                Ok(()) => delivered = true,
                                Err(next) => {
                                    set_state(&state, next);
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

/// Enqueues one observed event and advances its task's cursor.
///
/// This is everything the receive path may do. It classifies nothing, opens no
/// edge, completes no stage, and settles no operation.
fn observe_event(
    context: QueryContextRef,
    event: &proto::TaskStatusStreamEvent,
    cursors: &mut BTreeMap<TaskIdentity, TaskStatusCursor>,
    intake: &StatusIntakeHandle,
) -> Result<(), SubscriptionState> {
    let decoded = match decode_status_event(event, FieldPath::root("task_status_stream_event")) {
        Ok(decoded) => decoded,
        Err(error) => {
            tracing::warn!(detail = %error, "SubscribeTaskStatus event is malformed");
            return Err(SubscriptionState::Rejected);
        }
    };
    let (identity, event) = match decoded {
        StatusStreamEvent::Status(status) => {
            let identity = status.identity();
            let cursor = cursors
                .get(&identity)
                .copied()
                .unwrap_or_else(|| TaskStatusCursor::unobserved(identity));
            cursors.insert(identity, cursor.advanced_to(status.version()));
            (identity, StatusEvent::Published(status))
        }
        StatusStreamEvent::Gone(identity) => (identity, StatusEvent::Gone(identity)),
    };
    if identity.backend_process_id() != context.backend_process_id() {
        tracing::warn!(
            context_backend = %context.backend_process_id(),
            event_backend = %identity.backend_process_id(),
            "SubscribeTaskStatus event addresses a different backend process"
        );
        return Err(SubscriptionState::ProcessMismatch);
    }
    intake.publish(event);
    Ok(())
}

async fn open_subscription(
    client: &Client,
    context: QueryContextRef,
    cursors: &BTreeMap<TaskIdentity, TaskStatusCursor>,
) -> Result<tonic::Streaming<proto::TaskStatusStreamEvent>, tonic::Status> {
    let cursors = cursors.values().copied().collect::<Vec<_>>();
    let request = encode_subscribe_task_status(context, &cursors)
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let mut grpc = client
        .grpc_with_channel_error()
        .await
        .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
    grpc.subscribe_task_status(tonic::Request::new(request))
        .await
        .map(tonic::Response::into_inner)
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
        OperationOutcome::RetryableTransportUnknown => "retryable_transport_unknown",
        OperationOutcome::OperationTimedOut => "operation_timed_out",
        OperationOutcome::RetryableObservationLoss => "retryable_observation_loss",
        OperationOutcome::IdentityMismatch => "identity_mismatch",
        OperationOutcome::CreateConflict => "create_conflict",
        OperationOutcome::ContextNotEstablished => "context_not_established",
        OperationOutcome::ContextConflict => "context_conflict",
        OperationOutcome::DomainConflict => "domain_conflict",
        OperationOutcome::LeaseExpired => "lease_expired",
        OperationOutcome::ReleaseNotReady => "release_not_ready",
        OperationOutcome::ContextTerminalReceipt => "context_terminal_receipt",
        OperationOutcome::NormalDestinationCanceled => "normal_destination_canceled",
        OperationOutcome::DestinationFailure => "destination_failure",
        OperationOutcome::InvalidStateOrRequest => "invalid_state_or_request",
        OperationOutcome::TerminalRejected => "terminal_rejected",
        OperationOutcome::Gone => "gone",
        OperationOutcome::ResourceExhausted => "resource_exhausted",
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
    use std::pin::Pin;

    use novarocks_execution::task_execution::{
        CancelReason, CancelTask, CreateTaskReceipt, FetchTaskDynamicFilters, LeaseSequence,
        LeaseValidFor, MonotonicInstant, RenewQueryExecutionLease, TaskStatus, TaskStatusVersion,
    };
    use novarocks_proto_codec::task_execution::operation::{
        decode_subscribe_task_status, encode_operation_outcome, encode_status_event,
    };
    use novarocks_proto_models::{catalog, filter};
    use novarocks_types::identity::{FrontendProcessId, QueryExecutionId, StageId, TaskId};
    use novarocks_types::{AttemptId, QueryId};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status};

    use crate::native::generated::nova_rocks_grpc_server::{NovaRocksGrpc, NovaRocksGrpcServer};
    use crate::task_execution::dispatch::OperationDispatcher;
    use crate::task_execution::status_intake::{CountingWake, StatusIntake};
    use novarocks_execution::task_execution::DispatchBudget;

    use super::*;

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
            query_options: proto::QueryOptions::default(),
            native_compatibility_id: None,
        }
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
    }

    #[derive(Default)]
    struct PeerState {
        applied: Vec<proto::ApplyTaskOperationsRequest>,
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

        fn subscribed(&self) -> Vec<proto::SubscribeTaskStatusRequest> {
            self.state.lock().expect("peer state").subscribed.clone()
        }
    }

    type EmptyExchangeStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<proto::ExchangeResponse, Status>> + Send>>;
    type EmptyControlStream = Pin<
        Box<dyn tokio_stream::Stream<Item = Result<proto::QueryControlResponse, Status>> + Send>,
    >;
    type StatusStream = ReceiverStream<Result<proto::TaskStatusStreamEvent, Status>>;

    #[tonic::async_trait]
    impl NovaRocksGrpc for TaskWirePeer {
        type ExchangeStream = EmptyExchangeStream;
        type QueryControlStreamStream = EmptyControlStream;
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
                    outcome: encode_operation_outcome(outcome)
                        .expect("every test outcome has a wire form"),
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

        async fn lookup(
            &self,
            _request: Request<filter::LookupRequest>,
        ) -> Result<Response<filter::LookupResponse>, Status> {
            Err(Self::rejected("Lookup"))
        }

        async fn fetch_result(
            &self,
            _request: Request<proto::FetchResultRequest>,
        ) -> Result<Response<proto::FetchResultResponse>, Status> {
            Err(Self::rejected("FetchResult"))
        }

        async fn init_query(
            &self,
            _request: Request<proto::InitQueryRequest>,
        ) -> Result<Response<proto::InitQueryResponse>, Status> {
            Err(Self::rejected("InitQuery"))
        }

        async fn stage_fragments(
            &self,
            _request: Request<proto::StageFragmentsRequest>,
        ) -> Result<Response<proto::StageFragmentsResponse>, Status> {
            Err(Self::rejected("StageFragments"))
        }

        async fn start_prepared_query(
            &self,
            _request: Request<proto::StartPreparedQueryRequest>,
        ) -> Result<Response<proto::StartPreparedQueryResponse>, Status> {
            Err(Self::rejected("StartPreparedQuery"))
        }

        async fn task_update(
            &self,
            _request: Request<proto::TaskUpdateRequest>,
        ) -> Result<Response<proto::TaskUpdateResponse>, Status> {
            Err(Self::rejected("TaskUpdate"))
        }

        async fn abort_query(
            &self,
            _request: Request<proto::AbortQueryRequest>,
        ) -> Result<Response<proto::AbortQueryResponse>, Status> {
            Err(Self::rejected("AbortQuery"))
        }

        async fn query_control_stream(
            &self,
            _request: Request<tonic::Streaming<proto::QueryControlRequest>>,
        ) -> Result<Response<Self::QueryControlStreamStream>, Status> {
            Err(Self::rejected("QueryControlStream"))
        }

        async fn report_query_terminal(
            &self,
            _request: Request<proto::ReportQueryTerminalRequest>,
        ) -> Result<Response<proto::ReportQueryTerminalResponse>, Status> {
            Err(Self::rejected("ReportQueryTerminal"))
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

        fixture.sink.submit(&batch);
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
                .map(OperationAcknowledgement::outcome)
                .collect::<Vec<_>>(),
            vec![
                OperationOutcome::Accepted,
                OperationOutcome::TerminalRejected,
                OperationOutcome::Idempotent,
            ],
            "one item's failure leaves every other item exactly as its own receipt reports"
        );
        assert_eq!(
            fixture.loopback.peer.applied().len(),
            1,
            "one batch, one RPC"
        );
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

        fixture.sink.submit(&batch);
        let acks = settled(&fixture.acks, 2).await;

        for ack in &acks {
            assert_eq!(ack.outcome(), OperationOutcome::InvalidStateOrRequest);
            assert!(
                !ack.outcome().is_retryable(),
                "an answer that arrived is never resent"
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

        fixture.sink.submit(&batch);
        let acks = settled(&fixture.acks, 2).await;

        assert!(
            acks.iter()
                .all(|ack| ack.outcome() == OperationOutcome::InvalidStateOrRequest),
            "a response with fewer receipts than items cannot be attributed"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unknown_transport_outcome_is_retryable_and_resends_the_identical_request() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let batch = released_batch(backend, vec![cancel_intent(1, backend)]);
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::Reject(tonic::Code::Unavailable));

        fixture.sink.submit(&batch);
        let first = settled(&fixture.acks, 1).await;
        assert_eq!(
            first[0].outcome(),
            OperationOutcome::RetryableTransportUnknown
        );
        assert!(first[0].outcome().is_retryable());

        // The owner replays the same immutable intent, so the same bytes go
        // back out under the same operation id.
        fixture.sink.submit(&batch);
        let second = settled(&fixture.acks, 1).await;
        assert_eq!(second[0].outcome(), OperationOutcome::Accepted);
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

        fixture.sink.submit(&batch);
        let acks = settled(&fixture.acks, 1).await;

        assert_eq!(acks[0].outcome(), OperationOutcome::InvalidStateOrRequest);
        assert!(!acks[0].outcome().is_retryable());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_process_mismatch_fails_the_attempt_without_sending() {
        let frozen = BackendProcessId::new_v7();
        let replacement = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, frozen);
        let batch = released_batch(replacement, vec![cancel_intent(1, replacement)]);

        fixture.sink.submit(&batch);
        let acks = settled(&fixture.acks, 1).await;

        assert_eq!(acks[0].outcome(), OperationOutcome::IdentityMismatch);
        assert!(!acks[0].outcome().is_retryable());
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

        fixture.sink.submit(&batch);
        let acks = settled(&fixture.acks, 1).await;

        assert_eq!(acks[0].kind(), OperationKind::FetchTaskDynamicFilters);
        assert_eq!(acks[0].outcome(), OperationOutcome::InvalidStateOrRequest);
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
            let outcome = classify_apply_status(&Status::new(code, "unknown outcome"));
            assert_eq!(outcome, OperationOutcome::RetryableTransportUnknown);
            assert!(
                outcome.is_retryable(),
                "{code:?} keeps the request replayable"
            );
        }
        assert_eq!(
            classify_apply_status(&Status::new(tonic::Code::ResourceExhausted, "full")),
            OperationOutcome::ResourceExhausted
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
            let outcome = classify_apply_status(&Status::new(code, "settled"));
            assert!(!outcome.is_retryable(), "{code:?} must not be resent");
        }
        assert!(
            !classify_channel_error(&ChannelAcquisitionError::fatal("bad material")).is_retryable()
        );
        assert!(
            classify_channel_error(&ChannelAcquisitionError::retryable_network("dial failed"))
                .is_retryable()
        );
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
        assert!(
            runner.take_observation_loss(),
            "a lost stream is reported as observation loss"
        );
        assert_eq!(
            runner.drain(8),
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
        assert!(
            !runner.take_observation_loss(),
            "a live stream reports no loss"
        );
        assert_eq!(
            runner.drain(8),
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
        assert_eq!(
            fixture.intake.queued(),
            0,
            "an event from a replaced process is never published"
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

        fixture.sink.submit(&batch);
        let acks = settled(&fixture.acks, 1).await;
        assert_eq!(acks[0].outcome(), OperationOutcome::InvalidStateOrRequest);
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
                    novarocks_proto_codec::task_execution::status::encode_task_status(
                        &TaskStatus::created(task),
                    ),
                )),
            )]));

        fixture.sink.submit(&batch);
        let acks = settled(&fixture.acks, 1).await;
        assert_eq!(acks[0].outcome(), OperationOutcome::Accepted);
        assert_eq!(*acks[0].payload(), AckPayload::None);
    }
}
