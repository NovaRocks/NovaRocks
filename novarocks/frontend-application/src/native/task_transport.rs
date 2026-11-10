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
//! stream per participation, resubscribed by cursor inside a bounded error
//! budget.
//!
//! The two are deliberately unable to reach each other. The observation path
//! holds a [`StatusIntakeHandle`] and nothing else, so it can enqueue a
//! snapshot and wake the runner but cannot create a task, complete a stage,
//! settle an operation, or renew a lease. Nothing here renews a lease at all:
//! a renewal is minted by the participation owner from its own clock and
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
use std::task::Poll;
use std::time::Duration;

use once_cell::sync::Lazy;
use prometheus::{
    HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry,
};

use crate::task_execution::operation::{OperationDispatchResult, WorkerReceiptOutcome};
use crate::task_protocol_config::DispatchLane;
#[cfg(test)]
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::{
    AcquireParticipationAdmissionTicket, OperationKind, OperationOutcome,
    ParticipationConvergenceCursor, ParticipationConvergenceReceipt, ParticipationRef,
    TaskIdentity, TaskOperationId, TaskStatusCursor, UpdateParticipation,
};
use novarocks_execution_contract::BackendProcessDescriptor;
use novarocks_proto_codec::FieldPath;
#[cfg(test)]
use novarocks_proto_codec::native_rpc::NativeEndpointDomain;
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
use novarocks_proto_models::catalog::CatalogSet;
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::domain::{stored_credential, stored_message};
use novarocks_task_codec::operation as codec;
use novarocks_task_codec::operation::{
    CoveredStatusStreamEvent, CoveredStatusStreamFact, DecodedCoveredSubscription,
    ParticipationAwareStatusStreamEvent, ReceiptHeader, decode_covered_status_event,
    decode_participation_aware_status_event, decode_receipt_batch, encode_abort_participation,
    encode_acquire_participation_admission_ticket, encode_advance_participation_domain,
    encode_cancel_task, encode_control_operation_batch, encode_covered_subscribe_task_status,
    encode_create_task, encode_establish_participation, encode_operation_batch,
    encode_participation_aware_subscribe_task_status, encode_release_participation,
    encode_renew_lease, encode_update_task,
};
use novarocks_task_codec::{TransportBudget, domain as codec_domain};
use novarocks_types::identity::BackendProcessId;

use novarocks_execution::task_execution::operation::ParticipationReceipt;

use crate::task_execution::intent::{
    AckPayload, DispatchBatch, OperationAcknowledgement, OperationIntent,
    TaskOperationQueueAdmission, TaskOperationSink, TaskOperationSubmit,
};
use crate::task_execution::participation_convergence::{
    ParticipationConvergenceIntakeHandle, ParticipationConvergencePublishError,
};
use crate::task_execution::status_intake::{
    ObservationFrame, ObservationIntake, ObservationPublisher, StatusEvent, StatusIntakeAdmission,
    StatusIntakeHandle, StatusIntakeWake,
};

use super::apply_send_owner::{ApplyReservations, OriginalDisposition};
use super::data_runtime::FrontendDataRuntime;
use super::transport::{ChannelAcquisitionError, Client};
use super::transport_supervisor::{
    NativeTransportBackpressure, NativeTransportLane, NativeTransportReadyWake,
    NativeTransportWaiter,
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

mod joint_action;
use joint_action::NativeCallSource;
pub(crate) use joint_action::{NativeBatchAdmission, NativePhysicalOffer};
mod queue_input;
pub(crate) use queue_input::{queued_creation_quote, queued_intent_quote, queued_update_quote};

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
    pub(crate) initial_credential: proto::ParticipationCredentialDomain,
    pub(crate) query_options: proto::QueryOptions,
    pub(crate) native_compatibility_id: Option<proto::NativeCompatibilityId>,
}

/// Encodes one intent, projecting its own payloads back to the wire.
///
/// Nothing is handed in from outside: an intent carries its payloads behind a
/// fingerprint, and the codec that produced them is the only thing that can
/// recover them. Query options come from the immutable establish intent. The
/// compatibility identity still comes from the sink because it belongs to the
/// attempt-wide wire island rather than to the participation owner.
fn encode_operation(
    intent: &OperationIntent,
    attempt: &AttemptWireFacts,
) -> Result<proto::TaskOperation, String> {
    match intent {
        OperationIntent::AcquireParticipationAdmissionTicket(request) => {
            Ok(encode_acquire_participation_admission_ticket(*request))
        }
        OperationIntent::EstablishParticipation(request) => {
            encode_establish_participation_operation(request, attempt)
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
        OperationIntent::UpdateParticipation(request) => {
            encode_participation_operation(request, attempt)
        }
        OperationIntent::CancelTask(request) => Ok(encode_cancel_task(*request)),
        OperationIntent::QuiesceParticipation(request) => {
            Ok(codec::encode_quiesce_participation(*request))
        }
        OperationIntent::AbortParticipation(request) => Ok(encode_abort_participation(*request)),
        OperationIntent::ReleaseParticipation(request) => {
            Ok(encode_release_participation(*request))
        }
        // This read is its own RPC with its own response shape, so it has no
        // batch receipt to be answered by and can never be encoded as a batch item.
        #[cfg(test)]
        OperationIntent::FetchTaskDynamicFilters(_) => Err(format!(
            "{} is a separate RPC and has no operation-batch encoding",
            intent.kind()
        )),
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

fn encode_participation_operation(
    request: &UpdateParticipation,
    attempt: &AttemptWireFacts,
) -> Result<proto::TaskOperation, String> {
    match request {
        UpdateParticipation::Establish(establish) => {
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
            Ok(encode_establish_participation(
                establish,
                catalog_set.clone(),
                filter.clone(),
                proto::ParticipationCredentialDomain {
                    credential_domain_id: establish
                        .initial_credential()
                        .credential_domain_id()
                        .get(),
                    epoch: establish.initial_credential().epoch().get(),
                    descriptors: credential.descriptors().to_vec(),
                },
                *query_options,
                attempt.native_compatibility_id,
            ))
        }
        UpdateParticipation::AdvanceDomain(advance) => {
            let domain = codec_domain::encode_neutral_participation_domain(
                advance.domain(),
                FieldPath::root("advance_domain"),
            )
            .map_err(|error| error.to_string())?;
            Ok(encode_advance_participation_domain(advance, domain))
        }
        UpdateParticipation::RenewLease(renew) => Ok(encode_renew_lease(renew)),
    }
}

fn encode_establish_participation_operation(
    establish: &novarocks_execution::task_execution::EstablishParticipation,
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
    Ok(encode_establish_participation(
        establish,
        catalog_set.clone(),
        filter.clone(),
        proto::ParticipationCredentialDomain {
            credential_domain_id: establish.initial_credential().credential_domain_id().get(),
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
        OperationKind::AcquireParticipationAdmissionTicket
            | OperationKind::CreateTask
            | OperationKind::UpdateTask
            | OperationKind::UpdateParticipation
            | OperationKind::QuiesceParticipation
            | OperationKind::AbortParticipation
            | OperationKind::ReleaseParticipation
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
/// participation is a successful stand-down observation even when the operation was
/// not newly applied. Its participation state is therefore part of the mandatory
/// receipt for every legal closing outcome.
const fn consumes_ack_body_for_outcome(kind: OperationKind, outcome: OperationOutcome) -> bool {
    if matches!(kind, OperationKind::AbortParticipation) {
        return matches!(
            outcome,
            OperationOutcome::Accepted
                | OperationOutcome::Idempotent
                | OperationOutcome::ParticipationTerminalReceipt
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
        OperationIntent::UpdateParticipation(request)
            if matches!(request.as_ref(), UpdateParticipation::RenewLease(_))
    )
}

/// One item this transport actually put on the wire.
#[derive(Copy, Clone, Debug)]
struct SentOperation {
    operation_id: TaskOperationId,
    kind: OperationKind,
    establish_participation: Option<ParticipationRef>,
    lease_renewal: bool,
    address: AckAddress,
}

/// What an acknowledgement body must be addressed to.
///
/// A receipt that agrees with itself proves nothing. This is the address the
/// frontend actually sent to, so a body naming a different task or participation is
/// refused rather than read as this request's proof.
#[derive(Clone, Copy, Debug)]
enum AckAddress {
    Admission(AcquireParticipationAdmissionTicket),
    Task(TaskIdentity),
    Participation(ParticipationRef),
    /// The kind carries no acknowledgement body.
    None,
}

impl AckAddress {
    fn of(intent: &OperationIntent) -> Self {
        match intent {
            OperationIntent::AcquireParticipationAdmissionTicket(request) => {
                Self::Admission(*request)
            }
            OperationIntent::EstablishParticipation(request) => {
                Self::Participation(request.participation())
            }
            OperationIntent::CreateTask(request) => Self::Task(request.identity()),
            OperationIntent::UpdateTask(request) => Self::Task(request.identity()),
            OperationIntent::CancelTask(request) => Self::Task(request.identity()),
            OperationIntent::QuiesceParticipation(request) => {
                Self::Participation(request.participation())
            }
            OperationIntent::UpdateParticipation(request) => {
                Self::Participation(request.participation())
            }
            OperationIntent::AbortParticipation(request) => {
                Self::Participation(request.participation())
            }
            OperationIntent::ReleaseParticipation(request) => {
                Self::Participation(request.participation())
            }
            #[cfg(test)]
            OperationIntent::FetchTaskDynamicFilters(_) => Self::None,
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
    let Some(proto::task_operation_receipt::Result::Verdict(verdict)) = &receipt.result else {
        return Err(format!(
            "{kind} requires a protocol verdict acknowledgement"
        ));
    };
    let body = verdict
        .ack
        .as_ref()
        .ok_or_else(|| format!("{kind} requires an acknowledgement body for this outcome"))?;
    match (kind, body, address) {
        (
            OperationKind::AcquireParticipationAdmissionTicket,
            proto::task_operation_verdict::Ack::ParticipationAdmissionTicket(ack),
            AckAddress::Admission(expected),
        ) => codec::decode_participation_admission_ticket_ack(ack, expected, path())
            .map(AckPayload::AdmissionTicket)
            .map_err(|error| error.to_string()),
        (
            OperationKind::CreateTask,
            proto::task_operation_verdict::Ack::CreateTask(ack),
            AckAddress::Task(identity),
        ) => codec::decode_create_task_ack(ack, identity, path())
            .map(|receipt| AckPayload::Create(Box::new(receipt)))
            .map_err(|error| error.to_string()),
        (
            OperationKind::UpdateTask,
            proto::task_operation_verdict::Ack::UpdateTask(ack),
            AckAddress::Task(identity),
        ) => codec::decode_update_task_ack(ack, identity, path())
            .map(AckPayload::Update)
            .map_err(|error| error.to_string()),
        (
            OperationKind::UpdateParticipation | OperationKind::AbortParticipation,
            proto::task_operation_verdict::Ack::Participation(ack),
            AckAddress::Participation(participation),
        ) => codec::decode_participation_ack(ack, participation, path())
            .map(|(receipt, cause)| {
                if let Some(cause) = cause {
                    // The state is what the owner acts on; the cause is why,
                    // and it has no slot on a participation receipt.
                    tracing::info!(
                        %participation,
                        ?cause,
                        "participation acknowledgement reports a termination cause"
                    );
                }
                AckPayload::Participation(receipt)
            })
            .map_err(|error| error.to_string()),
        (
            OperationKind::QuiesceParticipation,
            proto::task_operation_verdict::Ack::QuiesceParticipation(ack),
            AckAddress::Participation(participation),
        ) => {
            let decoded =
                codec::decode_quiesce_ack(ack, path()).map_err(|error| error.to_string())?;
            if decoded.participation() != participation {
                return Err("a quiesce acknowledgement names a different participation".to_owned());
            }
            Ok(AckPayload::Quiesce(decoded))
        }
        (
            OperationKind::ReleaseParticipation,
            proto::task_operation_verdict::Ack::ReleaseParticipation(ack),
            AckAddress::Participation(participation),
        ) => {
            let decoded =
                codec::decode_release_ack(ack, path()).map_err(|error| error.to_string())?;
            release_payload(decoded, participation)
        }

        (kind, _, _) => Err(format!(
            "an applied {kind} carries an acknowledgement body of another kind"
        )),
    }
}

fn release_payload(
    decoded: codec::DecodedReleaseAck,
    participation: ParticipationRef,
) -> Result<AckPayload, String> {
    if decoded.participation != participation {
        return Err(
            "a release acknowledgement names a different participation than the request".to_owned(),
        );
    }
    if let Some(cause) = decoded.termination_cause {
        tracing::info!(%participation, ?cause, "release acknowledgement reports a termination cause");
    }
    Ok(AckPayload::Release {
        receipt: ParticipationReceipt::new(decoded.participation, decoded.state),
        outcome: decoded.outcome,
        runtime_filter: decoded.runtime_filter,
    })
}
fn decode_ack_owned(
    kind: OperationKind,
    address: AckAddress,
    receipt: proto::TaskOperationReceipt,
) -> Result<AckPayload, String> {
    if let (OperationKind::ReleaseParticipation, AckAddress::Participation(participation)) =
        (kind, address)
    {
        let Some(proto::task_operation_receipt::Result::Verdict(verdict)) = receipt.result else {
            return Err(format!(
                "{kind} requires a protocol verdict acknowledgement"
            ));
        };
        let Some(proto::task_operation_verdict::Ack::ReleaseParticipation(ack)) = verdict.ack
        else {
            return Err(format!(
                "an applied {kind} carries an acknowledgement body of another kind"
            ));
        };
        let decoded = codec::decode_release_ack_owned(
            ack,
            FieldPath::root("apply_task_operations").field("receipts"),
        )
        .map_err(|error| error.to_string())?;
        return release_payload(decoded, participation);
    }
    decode_ack(kind, address, &receipt)
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
fn classify_apply_status(status: &tonic::Status) -> OperationDispatchResult {
    if novarocks_native_adapter::native_codec::is_operation_reply_preflight_failure(status) {
        // A reply arrived but its generated tree exceeded the original reply
        // grammar. This is a local refusal, never a Worker verdict or proof
        // of no remote effect. It must not authorize another dispatch.
        return OperationDispatchResult::NonWorkerRejected;
    }
    // Remote NO_ERROR GOAWAY leaves this immutable operation's outcome unknown.
    if status.code() == tonic::Code::Internal
        && task_operation_h2_source(status).is_some_and(|source| {
            source.is_go_away
                && source.is_remote
                && source.reason == Some(u32::from(h2::Reason::NO_ERROR))
        })
    {
        return OperationDispatchResult::TransportUnknown;
    }
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
                Some("waiting_capacity") => {
                    crate::task_execution::operation::IngressRejection::WaitingCapacity
                }
                Some("body_limit") => crate::task_execution::operation::IngressRejection::BodyLimit,
                _ => crate::task_execution::operation::IngressRejection::UnclassifiedCapacity,
            },
        ),
        _ => OperationDispatchResult::NonWorkerRejected,
    }
}

struct TaskOperationH2Source {
    reason: Option<u32>,
    is_go_away: bool,
    is_reset: bool,
    is_remote: bool,
    is_library: bool,
    is_io: bool,
    io_kind: Option<std::io::ErrorKind>,
    io_raw_os_error: Option<i32>,
}

impl fmt::Debug for TaskOperationH2Source {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskOperationH2Source")
            .field("reason", &self.reason)
            .field("is_go_away", &self.is_go_away)
            .field("is_reset", &self.is_reset)
            .field("is_remote", &self.is_remote)
            .field("is_library", &self.is_library)
            .field("is_io", &self.is_io)
            .field("io_kind", &self.io_kind)
            .field("io_raw_os_error", &self.io_raw_os_error)
            .finish()
    }
}

// Diagnostics borrow only the original typed source; messages grant no facts.
fn task_operation_h2_source(status: &tonic::Status) -> Option<TaskOperationH2Source> {
    let mut source = std::error::Error::source(status);
    while let Some(error) = source {
        if let Some(error) = error.downcast_ref::<h2::Error>() {
            let io = error.get_io();
            return Some(TaskOperationH2Source {
                reason: error.reason().map(u32::from),
                is_go_away: error.is_go_away(),
                is_reset: error.is_reset(),
                is_remote: error.is_remote(),
                is_library: error.is_library(),
                is_io: error.is_io(),
                io_kind: io.map(std::io::Error::kind),
                io_raw_os_error: io.and_then(std::io::Error::raw_os_error),
            });
        }
        source = error.source();
    }
    None
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

/// Construction facts authored before any attempt operation can be sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeTaskTransportCreationError {
    BudgetMismatch,
    NoBackends,
    DuplicateBackend { process: BackendProcessId },
    Registration(super::transport_supervisor::NativeTransportRegistrationError),
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
impl From<super::transport_supervisor::NativeTransportRegistrationError>
    for NativeTaskTransportCreationError
{
    fn from(error: super::transport_supervisor::NativeTransportRegistrationError) -> Self {
        Self::Registration(error)
    }
}

fn freeze_targets(
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
    targets: BTreeMap<BackendProcessId, TaskBackendTarget>,
    transport: TransportBudget,
    attempt: AttemptWireFacts,
    acks: TaskAckIntakeHandle,
    data_runtime: FrontendDataRuntime,
    proposal_wait: proposal_wait::OriginalProposalWait,
    transport_waiter: std::mem::ManuallyDrop<NativeTransportWaiter>,
    authority: novarocks_workload_control::joint_admission::NativeAsyncActionAuthority,
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
    fn settle_locally(
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
        preparation: &mut crate::task_execution::intent::TaskOperationQueuePreparation,
    ) -> TaskOperationQueueAdmission {
        let sizing = (|| -> Result<(), novarocks_workload_control::WorkError> {
            use novarocks_workload_control::WorkError;
            let bytes = request.encoded_bytes().map_err(|error| match error {
                novarocks_task_codec::operation::wire_size::WireSizeError::Overflow => {
                    WorkError::ArithmeticOverflow
                }
                novarocks_task_codec::operation::wire_size::WireSizeError::NotCodecProduced => {
                    WorkError::Conflict
                }
            })?;
            if bytes
                > self
                    .transport
                    .message_limits()
                    .request_max_bytes(request.requires_control_progress())
            {
                return Err(WorkError::Capacity("Native operation message length"));
            }
            let windows =
                crate::task_execution::intent::NativeRequestWindows::for_transport(self.transport)
                    .map_err(|_| WorkError::InvalidConfig("Native request windows"))?;
            if request.queued_bytes()
                > windows.empty_queue_capacity(request.requires_control_progress())
            {
                return Err(WorkError::Capacity("Native operation empty queue window"));
            }
            Ok(())
        })();
        if let Err(error) = sizing {
            return TaskOperationQueueAdmission::Rejected(error);
        }
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
        let batch_requires_control = batch.operations()[0].requires_control_progress();
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
                novarocks_task_codec::resource_preflight::OperationReplyDecodePlan::for_response(
                    items,
                    self.transport
                        .message_limits()
                        .response_max_bytes(batch_requires_control),
                )
                .expect("the same original reply layout was registered before admission"),
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

fn is_small_control(intent: &OperationIntent) -> bool {
    intent.native_method()
        == novarocks_proto_codec::native_rpc::NativeRpcMethod::ApplyTaskControlOperations
}

enum EncodedMethodRequest {
    Ordinary(proto::ApplyTaskOperationsRequest),
    Control(proto::ApplyTaskControlOperationsRequest),
}

impl EncodedMethodRequest {
    fn encoded_len(&self) -> usize {
        match self {
            Self::Ordinary(value) => prost::Message::encoded_len(value),
            Self::Control(value) => prost::Message::encoded_len(value),
        }
    }
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
/// method whose closed input grammar accepts it. A run also shares one
/// absolute submit deadline: a longer item must not renew a shorter item.
fn encode_method_batches(
    operations: Vec<(bool, proto::TaskOperation)>,
    budget: TransportBudget,
) -> Result<Vec<EncodedMethodBatch>, novarocks_proto_codec::ProtocolError> {
    // This is also a direct internal test seam: apply the same source bound
    // before allocating any method-run carrier, even without a dispatcher.
    if operations.is_empty() || operations.len() > budget.max_batch_items() {
        return Err(novarocks_proto_codec::ProtocolError::new(
            FieldPath::root("task_operation_method_batches"),
            novarocks_proto_codec::ProtocolErrorKind::OutOfRange,
            "operation method batches exceed the original item bound",
        ));
    }
    fn key(item: &(bool, proto::TaskOperation)) -> (bool, Option<u64>) {
        (
            item.0,
            item.1
                .envelope
                .as_ref()
                .map(|envelope| envelope.max_wait_millis),
        )
    }
    let runs = 1 + operations
        .windows(2)
        .filter(|pair| key(&pair[0]) != key(&pair[1]))
        .count();
    let mut batches = Vec::with_capacity(runs);
    let mut iter = operations.into_iter();
    while let Some((control, first)) = iter.next() {
        let wait_millis = first
            .envelope
            .as_ref()
            .map(|envelope| envelope.max_wait_millis);
        let run_items = 1 + iter
            .as_slice()
            .iter()
            .take_while(|item| key(item) == (control, wait_millis))
            .count();
        let mut run = Vec::with_capacity(run_items);
        run.push(first);
        for (_, operation) in iter.by_ref().take(run_items - 1) {
            run.push(operation);
        }
        let deadline = Duration::from_millis(wait_millis.unwrap_or_default());
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

/// The original client and first-wins receipts; its fixed record owns reservations.
struct ApplySend {
    client: Client,
    receipts: AcceptedOperationReceipts,
    physical: Option<NativeCallSource>,
    action_tail: Option<novarocks_resource_contract::ExistingOwnerRetentionPin>,
    reply_plan: novarocks_task_codec::resource_preflight::OperationReplyDecodePlan,
}

/// First-wins settlement for every operation an accepted send owns.
///
/// Dropping the original send future is an unknown transport outcome, including
/// role shutdown and task abort. Its record waits for actual task join and
/// actual protected destructor join before returning process reservations. The guard publishes that fact for every operation
/// not already settled before releasing the process reservations, so an owner
/// can never remain in flight merely because its transport future disappeared.
struct AcceptedOperationReceipts {
    acks: TaskAckIntakeHandle,
    pending: VecDeque<SentOperation>,
    canonical: Option<super::apply_send_owner::OriginalReplyPublisher>,
}

impl AcceptedOperationReceipts {
    fn new(acks: TaskAckIntakeHandle, sent: Vec<SentOperation>) -> Self {
        Self {
            acks,
            pending: sent.into(),
            canonical: None,
        }
    }
    fn new_canonical(
        acks: TaskAckIntakeHandle,
        sent: Vec<SentOperation>,
        canonical: super::apply_send_owner::OriginalReplyPublisher,
    ) -> Self {
        Self {
            acks,
            pending: sent.into(),
            canonical: Some(canonical),
        }
    }
    fn publish_canonical(&self, ack: OperationAcknowledgement) {
        let ack = match &self.canonical {
            Some(publisher) => publisher.canonicalize(ack),
            None => ack,
        };
        self.acks.publish(ack);
    }

    fn prefix_ids(&self, count: usize) -> Vec<TaskOperationId> {
        assert!(
            count <= self.pending.len(),
            "prefix is part of the same accepted batch"
        );
        let mut ids = Vec::with_capacity(count);
        for item in self.pending.iter().take(count) {
            ids.push(item.operation_id);
        }
        ids
    }

    fn prefix_establishes(&self, count: usize) -> Vec<(TaskOperationId, ParticipationRef)> {
        assert!(
            count <= self.pending.len(),
            "prefix is part of the same accepted batch"
        );
        let selected = self
            .pending
            .iter()
            .take(count)
            .filter(|item| item.establish_participation.is_some())
            .count();
        let mut establishes = Vec::with_capacity(selected);
        for item in self.pending.iter().take(count) {
            if let Some(participation) = item.establish_participation {
                establishes.push((item.operation_id, participation));
            }
        }
        establishes
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
        self.publish_canonical(ack);
    }

    fn publish_recorded_dispatch_rejection(&mut self) {
        assert!(
            self.canonical
                .as_ref()
                .is_some_and(|p| p.dispatch_rejected())
        );
        // The original typed fact precedes every acknowledgement and wake.
        self.publish_uniform(OperationDispatchResult::NonWorkerRejected);
    }

    fn publish_uniform(&mut self, result: OperationDispatchResult) {
        while let Some(item) = self.pending.pop_front() {
            observe_dispatch_result(item.kind, item.lease_renewal, result);
            self.publish_canonical(OperationAcknowledgement::from_dispatch_result(
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
            self.publish_canonical(OperationAcknowledgement::from_dispatch_result(
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
            let result = if self
                .canonical
                .as_ref()
                .is_some_and(|p| p.dispatch_rejected())
            {
                // A wake/Drop unwind cannot erase the already installed
                // whole-batch no-RPC fact or authorize an Unknown replay.
                OperationDispatchResult::NonWorkerRejected
            } else {
                OperationDispatchResult::TransportUnknown
            };
            self.publish_uniform(result);
        }
    }
}

/// Sends one batch and publishes one acknowledgement per request item.
async fn apply_operations(
    mut send: ApplySend,
    requests: Vec<EncodedMethodBatch>,
    submitted_at: tokio::time::Instant,
) {
    let last_expiry = requests
        .iter()
        .map(|batch| batch.expires_at(submitted_at))
        .max()
        .expect("the original Native Apply has at least one encoded method run");
    let frozen = {
        let publisher = send
            .receipts
            .canonical
            .as_ref()
            .expect("governed Native dispatch retains its original canonical backing");
        publisher.freeze_dispatch(last_expiry).await
    };
    if let Err(()) = frozen {
        send.receipts.publish_recorded_dispatch_rejection();
        return;
    }
    // The same original F crosses the atomic dispatch cut before any RPC
    // body poll. Every run keeps its own unchanged absolute deadline.
    for batch in requests {
        let expires_at = batch.expires_at(submitted_at);
        let establishes = send.receipts.prefix_establishes(batch.items);
        let response = match send_operations_in(
            &send.client,
            batch.request,
            expires_at,
            &send.receipts.acks,
            &establishes,
            &mut send.physical,
            &mut send.action_tail,
            send.reply_plan,
        )
        .await
        {
            Ok(response) => response,
            Err(result) => {
                for item in send.receipts.pending.iter().take(batch.items) {
                    tracing::warn!(
                        operation = ?item.operation_id,
                        kind = item.kind.as_str(),
                        address = ?item.address,
                        dispatch_result = ?result,
                        "task operation method run failed before receipt settlement"
                    );
                }
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
                    OperationDispatchResult::NonWorkerRejected,
                );
                continue;
            }
        };

        for (header, receipt) in headers.iter().zip(response.receipts) {
            let item = send
                .receipts
                .front()
                .expect("validated receipt count matches the accepted request");
            let ack = acknowledgement_owned(&item, header, receipt);
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
    acknowledgement_with(item, header, || {
        decode_ack(item.kind, item.address, receipt)
    })
}
fn acknowledgement_owned(
    item: &SentOperation,
    header: &ReceiptHeader,
    receipt: proto::TaskOperationReceipt,
) -> OperationAcknowledgement {
    acknowledgement_with(item, header, || {
        decode_ack_owned(item.kind, item.address, receipt)
    })
}
fn acknowledgement_with(
    item: &SentOperation,
    header: &ReceiptHeader,
    decode: impl FnOnce() -> Result<AckPayload, String>,
) -> OperationAcknowledgement {
    if let novarocks_execution_contract::operation::OperationResult::DomainFailure {
        outcome,
        failure,
    } = header.result()
    {
        observe_settled(item.kind, item.lease_renewal, outcome.outcome());
        return OperationAcknowledgement::worker_domain_failure(
            item.operation_id,
            item.kind,
            *outcome,
            failure.clone(),
        );
    }
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
    match decode() {
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
            observe_dispatch_result(
                item.kind,
                item.lease_renewal,
                OperationDispatchResult::NonWorkerRejected,
            );
            OperationAcknowledgement::from_dispatch_result(
                item.operation_id,
                item.kind,
                OperationDispatchResult::NonWorkerRejected,
                AckPayload::None,
            )
        }
    }
}

/// One `ApplyTaskOperations` round trip, classified by type.
///
/// The deadline is the shared absolute submit deadline of this run. A batch that
/// has not answered by then has a genuinely unknown outcome, which is exactly
/// what the retry rule exists for.
async fn send_operations_in(
    client: &Client,
    request: EncodedMethodRequest,
    expires_at: tokio::time::Instant,
    intake: &TaskAckIntakeHandle,
    establishes: &[(TaskOperationId, ParticipationRef)],
    selected: &mut Option<NativeCallSource>,
    action_tail: &mut Option<novarocks_resource_contract::ExistingOwnerRetentionPin>,
    reply_plan: novarocks_task_codec::resource_preflight::OperationReplyDecodePlan,
) -> Result<proto::ApplyTaskOperationsResponse, OperationDispatchResult> {
    let method = match &request {
        EncodedMethodRequest::Ordinary(_) => NativeRpcMethod::ApplyTaskOperations,
        EncodedMethodRequest::Control(_) => NativeRpcMethod::ApplyTaskControlOperations,
    };
    let source = selected
        .take()
        .ok_or(OperationDispatchResult::NonWorkerRejected)?;
    let (reserved, acquired) = match source {
        NativeCallSource::Warm { acquired, call } => (call, acquired),
        NativeCallSource::Cold(handoff) => {
            let acquired = match handoff {
                super::transport::NativeDialHandoff::Ready(acquired) => acquired,
                super::transport::NativeDialHandoff::Reserved(dial) => {
                    let mut acquired = None;
                    tokio::time::timeout_at(expires_at, dial.connect_in(&mut acquired))
                        .await
                        .map_err(|_| OperationDispatchResult::TransportUnknown)?
                        .map_err(|error| classify_channel_error(&error))?;
                    acquired.ok_or(OperationDispatchResult::NonWorkerRejected)?
                }
            };
            // A fresh connection's first local stream/Buffer is attempted once.
            // No async wait holds an admitted Joint vector for another local
            // physical position. Refusal proves no Apply was dispatched.
            let call = std::future::poll_fn(|cx| {
                Poll::Ready(acquired.channel().select_local_call().try_reserve(cx))
            })
            .await;
            let call = match call {
                Poll::Ready(Ok(call)) => call,
                _ => return Err(OperationDispatchResult::NonWorkerRejected),
            };
            (call, acquired)
        }
    };
    let mut grpc = client.grpc_for_reserved_call(reserved, method);
    let remaining = expires_at.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        // Nothing was submitted, so nothing was applied. It is still reported
        // as unknown rather than as a rejection, because a caller may only
        // conclude "not applied" from an answer it received.
        client.invalidate_channel_if_current(method, &acquired);
        return Err(OperationDispatchResult::TransportUnknown);
    }
    // Reconstruct the pure plan from the now-formed DTO and require the same
    // source bound used before its first clone. No Joint refund occurs here.
    let encoding_plan =
        novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan::for_original_operation(
            request.encoded_len(),
        )
        .expect("one nonempty original unary method run fits its source invoice");
    let call = async {
        match request {
            EncodedMethodRequest::Ordinary(request) => {
                let mut wire = tonic::Request::new(request);
                wire.set_timeout(remaining);
                wire.extensions_mut().insert(novarocks_native_adapter::native_lane::NativeRequestCustody::from_retained_action(
                    action_tail.take().expect("one original unary method run retains its action source")));
                let codec=novarocks_native_adapter::native_codec::NativeProstCodec::with_original_operation_plans(reply_plan, encoding_plan);
                let rpc = grpc.unary(
                    wire,
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/novarocks.NovaRocksGrpc/ApplyTaskOperations",
                    ),
                    codec,
                );
                tokio::pin!(rpc);
                let mut started = false;
                std::future::poll_fn(|cx| {
                    let result = rpc.as_mut().poll(cx);
                    if !started {
                        started = true;
                        for &(operation_id, participation) in establishes {
                            intake.publish_establish_send_started(operation_id, participation);
                        }
                    }
                    result
                })
                .await
            }
            EncodedMethodRequest::Control(request) => {
                let mut wire = tonic::Request::new(request);
                wire.set_timeout(remaining);
                wire.extensions_mut().insert(novarocks_native_adapter::native_lane::NativeRequestCustody::from_retained_action(
                    action_tail.take().expect("one original unary method run retains its action source")));
                let codec=novarocks_native_adapter::native_codec::NativeProstCodec::with_original_operation_plans(reply_plan, encoding_plan);
                let rpc = grpc.unary(
                    wire,
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/novarocks.NovaRocksGrpc/ApplyTaskControlOperations",
                    ),
                    codec,
                );
                tokio::pin!(rpc);
                let mut started = false;
                std::future::poll_fn(|cx| {
                    let result = rpc.as_mut().poll(cx);
                    if !started {
                        started = true;
                        for &(operation_id, participation) in establishes {
                            intake.publish_establish_send_started(operation_id, participation);
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
                method = ?method,
                channel_slot = ?acquired.slot(),
                h2_source = ?task_operation_h2_source(&status),
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
        client.invalidate_channel_if_current(method, &acquired);
    }
    outcome
}

#[cfg(test)]
async fn fixture_apply_request<T: prost::Message + Send + 'static>(
    grpc: &mut super::transport::AuthenticatedOperationTestGrpc,
    request: tonic::Request<T>,
    method: NativeRpcMethod,
    original_count: usize,
    response_max: usize,
) -> Result<tonic::Response<proto::ApplyTaskOperationsResponse>, tonic::Status> {
    let reply = novarocks_task_codec::resource_preflight::OperationReplyDecodePlan::for_response(
        original_count,
        response_max,
    )
    .expect("the fixture retains its original request count and method limit");
    let encoding =
        novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan::for_original_operation(
            request.get_ref().encoded_len(),
        )
        .expect("a fixture method request is nonempty");
    let path = match method {
        NativeRpcMethod::ApplyTaskOperations => "/novarocks.NovaRocksGrpc/ApplyTaskOperations",
        NativeRpcMethod::ApplyTaskControlOperations => {
            "/novarocks.NovaRocksGrpc/ApplyTaskControlOperations"
        }
        _ => unreachable!("only original Apply fixtures use this codec"),
    };
    grpc.ready()
        .await
        .map_err(|error| tonic::Status::unknown(format!("Service was not ready: {error}")))?;
    grpc.unary(
        request,
        tonic::codegen::http::uri::PathAndQuery::from_static(path),
        novarocks_native_adapter::native_codec::NativeProstCodec::with_original_operation_plans(
            reply, encoding,
        ),
    )
    .await
}

// Legacy wire-only fixtures exercise their original request/classifier
// contract directly. Production has no source-free call to this author.
#[cfg(test)]
async fn send_operations(
    client: &Client,
    request: EncodedMethodRequest,
    expires_at: tokio::time::Instant,
    intake: &TaskAckIntakeHandle,
    establishes: &[(TaskOperationId, ParticipationRef)],
) -> Result<proto::ApplyTaskOperationsResponse, OperationDispatchResult> {
    let method = match &request {
        EncodedMethodRequest::Ordinary(_) => NativeRpcMethod::ApplyTaskOperations,
        EncodedMethodRequest::Control(_) => NativeRpcMethod::ApplyTaskControlOperations,
    };
    let (mut grpc, acquired) = tokio::time::timeout_at(
        expires_at,
        client.operation_test_grpc_with_channel_identity(method),
    )
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
        client.invalidate_channel_if_current(method, &acquired);
        return Err(OperationDispatchResult::TransportUnknown);
    }
    let response_max = client.method_message_limits(method).1;
    let call = async {
        match request {
            EncodedMethodRequest::Ordinary(request) => {
                let original_count = request.operations.len();
                let mut wire = tonic::Request::new(request);
                wire.set_timeout(remaining);
                let rpc =
                    fixture_apply_request(&mut grpc, wire, method, original_count, response_max);
                tokio::pin!(rpc);
                let mut started = false;
                std::future::poll_fn(|cx| {
                    let result = rpc.as_mut().poll(cx);
                    if !started {
                        started = true;
                        for &(operation_id, participation) in establishes {
                            intake.publish_establish_send_started(operation_id, participation);
                        }
                    }
                    result
                })
                .await
            }
            EncodedMethodRequest::Control(request) => {
                let original_count = request.operations.len();
                let mut wire = tonic::Request::new(request);
                wire.set_timeout(remaining);
                let rpc =
                    fixture_apply_request(&mut grpc, wire, method, original_count, response_max);
                tokio::pin!(rpc);
                let mut started = false;
                std::future::poll_fn(|cx| {
                    let result = rpc.as_mut().poll(cx);
                    if !started {
                        started = true;
                        for &(operation_id, participation) in establishes {
                            intake.publish_establish_send_started(operation_id, participation);
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
        client.invalidate_channel_if_current(method, &acquired);
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
    /// The same retry budget ended after a typed original connection loss
    /// that no subsequently accepted stream liveness repaired.
    ConnectionLossExhausted,
    /// The backend rejected the subscription in a way resubscribing cannot
    /// repair.
    Rejected,
    /// An event addressed a different backend process. An exact process
    /// replacement is fatal to the attempt and is never followed.
    ProcessMismatch,
    /// An event addressed a different query execution on the expected
    /// backend process. It cannot contribute to this participation's cursor.
    QueryMismatch,
}

impl SubscriptionState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Opening => "opening",
            Self::Live => "live",
            Self::Resubscribing => "resubscribing",
            Self::BudgetExhausted => "budget_exhausted",
            Self::ConnectionLossExhausted => "connection_loss_exhausted",
            Self::Rejected => "rejected",
            Self::ProcessMismatch => "process_mismatch",
            Self::QueryMismatch => "query_mismatch",
        }
    }

    /// Whether this state is fatal to the query attempt.
    pub(crate) const fn is_fatal(self) -> bool {
        matches!(
            self,
            Self::BudgetExhausted
                | Self::ConnectionLossExhausted
                | Self::Rejected
                | Self::ProcessMismatch
                | Self::QueryMismatch
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
/// One logical subscription per participation, started lazily the first time
/// that participation has a task to observe. A new task joins the running stream on
/// its own, so nothing here freezes a task set.
pub(crate) struct TaskStatusSubscriber {
    targets: BTreeMap<BackendProcessId, TaskBackendTarget>,
    intake: StatusIntakeHandle,
    participation_convergence: Option<ParticipationConvergenceIntakeHandle>,
    error_budget: u32,
    data_runtime: FrontendDataRuntime,
    active: Mutex<BTreeMap<ParticipationRef, Subscription>>,
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
    #[cfg(test)]
    pub(crate) fn new(
        backends: &[BackendProcessDescriptor],
        intake: StatusIntakeHandle,
        error_budget: u32,
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, NativeTaskTransportCreationError> {
        if error_budget == 0 {
            return Err(NativeTaskTransportCreationError::ZeroStatusErrorBudget);
        }
        Ok(Self {
            targets: freeze_targets(backends, &data_runtime)?,
            intake,
            participation_convergence: None,
            error_budget,
            data_runtime,
            active: Mutex::new(BTreeMap::new()),
        })
    }

    /// Builds the single-stream task and participation observation transport.
    #[cfg(test)]
    pub(crate) fn new_participation_aware(
        backends: &[BackendProcessDescriptor],
        intake: StatusIntakeHandle,
        participation_convergence: ParticipationConvergenceIntakeHandle,
        error_budget: u32,
        data_runtime: FrontendDataRuntime,
    ) -> Result<Self, NativeTaskTransportCreationError> {
        if error_budget == 0 {
            return Err(NativeTaskTransportCreationError::ZeroStatusErrorBudget);
        }
        Ok(Self {
            targets: freeze_targets(backends, &data_runtime)?,
            intake,
            participation_convergence: Some(participation_convergence),
            error_budget,
            data_runtime,
            active: Mutex::new(BTreeMap::new()),
        })
    }

    /// Starts this participation's one subscription if it is not already running.
    pub(crate) fn ensure(
        &self,
        participation: ParticipationRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<(), String> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| "task status subscription lock poisoned".to_owned())?;
        if active.contains_key(&participation) {
            return Ok(());
        }
        let subscription = self.start(participation, cursors)?;
        active.insert(participation, subscription);
        Ok(())
    }

    /// Reconciles this participation's one subscription to the runner's cursors.
    ///
    /// This is the answer to observation loss the runner saw on its own side,
    /// such as a full intake queue. Loss the stream itself sees is recovered
    /// by the subscription without anyone asking. The bounded watch channel
    /// coalesces repeated reconciliation requests and gives the same loop the
    /// later one-way lifecycle control seam; it never creates a second stream.
    pub(crate) fn resubscribe(
        &self,
        participation: ParticipationRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<(), String> {
        let cursors = task_cursor_map(participation, cursors)?;
        let mut active = self
            .active
            .lock()
            .map_err(|_| "task status subscription lock poisoned".to_owned())?;
        if let Some(subscription) = active.get(&participation) {
            subscription.reconciliation.send_modify(|current| {
                current.generation = current
                    .generation
                    .checked_add(1)
                    .expect("task cursor reconciliation generation exhausted");
                current.cursors.clone_from(&cursors);
            });
        } else {
            active.insert(
                participation,
                self.start_with_cursors(participation, cursors)?,
            );
        }
        observe_resubscribe();
        Ok(())
    }

    /// Stops this participation's subscription.
    pub(crate) fn stop(&self, participation: ParticipationRef) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&participation);
        }
    }

    /// What this participation's subscription is currently doing.
    pub(crate) fn state(&self, participation: ParticipationRef) -> Option<SubscriptionState> {
        let active = self.active.lock().ok()?;
        let subscription = active.get(&participation)?;
        subscription.state.lock().ok().map(|state| *state)
    }

    fn start(
        &self,
        participation: ParticipationRef,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<Subscription, String> {
        self.start_with_cursors(participation, task_cursor_map(participation, cursors)?)
    }

    fn start_with_cursors(
        &self,
        participation: ParticipationRef,
        cursors: BTreeMap<TaskIdentity, TaskStatusCursor>,
    ) -> Result<Subscription, String> {
        let target = self
            .targets
            .get(&participation.backend_process_id())
            .ok_or_else(|| {
                format!(
                    "participation {} addresses a backend process outside the frozen topology",
                    participation.backend_process_id()
                )
            })?;
        let state = Arc::new(Mutex::new(SubscriptionState::Opening));
        let (reconciliation, receiver) = tokio::sync::watch::channel(TaskCursorReconciliation {
            generation: 0,
            cursors,
        });
        let task = self.data_runtime.spawn(run_subscription(
            target.client.clone(),
            participation,
            receiver,
            self.intake.clone(),
            self.participation_convergence.clone(),
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
    participation: ParticipationRef,
    cursors: Vec<TaskStatusCursor>,
) -> Result<BTreeMap<TaskIdentity, TaskStatusCursor>, String> {
    let count = cursors.len();
    let cursors = cursors
        .into_iter()
        .map(|cursor| {
            let identity = cursor.identity();
            if identity.query_execution_id() != participation.query_execution_id()
                || identity.backend_process_id() != participation.backend_process_id()
            {
                return Err(format!(
                    "task status cursor {identity} is outside participation {participation}"
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

/// The subscription loop of one participation.
///
/// A dropped stream is never a task failure: the backend process is intact and
/// still running the query, so the loop reports observation loss and
/// resubscribes from the cursors it holds. It never cancels a task, never
/// settles an operation, and never touches a lease.
async fn run_subscription(
    client: Client,
    participation: ParticipationRef,
    mut reconciliation: tokio::sync::watch::Receiver<TaskCursorReconciliation>,
    intake: StatusIntakeHandle,
    participation_convergence: Option<ParticipationConvergenceIntakeHandle>,
    error_budget: u32,
    state: Arc<Mutex<SubscriptionState>>,
) {
    let initial = reconciliation.borrow_and_update().clone();
    let mut reconciliation_generation = initial.generation;
    let mut cursors = initial.cursors;
    let mut participation_cursor = participation_convergence
        .as_ref()
        .map(|_| ParticipationConvergenceCursor::unobserved(participation));
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
            opened = open_subscription(&client, participation, &cursors, participation_cursor) => opened,
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
                                participation,
                                &event,
                                &mut cursors,
                                &intake,
                                participation_convergence.as_ref(),
                                &mut participation_cursor,
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
    participation: ParticipationRef,
    event: &proto::TaskStatusStreamEvent,
    cursors: &mut BTreeMap<TaskIdentity, TaskStatusCursor>,
    intake: &StatusIntakeHandle,
    participation_convergence: Option<&ParticipationConvergenceIntakeHandle>,
    participation_cursor: &mut Option<ParticipationConvergenceCursor>,
) -> Result<StreamObservation, SubscriptionState> {
    let decoded =
        decode_participation_aware_status_event(event, FieldPath::root("task_status_stream_event"))
            .map_err(|error| {
                tracing::warn!(detail = %error, "SubscribeTaskStatus event is malformed");
                SubscriptionState::Rejected
            })?;
    match decoded {
        ParticipationAwareStatusStreamEvent::Status(status) => {
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
                participation,
                identity,
                StatusEvent::Published(Box::new(status)),
                Some(cursor.advanced_to(version)),
                cursors,
                intake,
            )
        }
        ParticipationAwareStatusStreamEvent::Gone(identity) => observe_task_event(
            participation,
            identity,
            StatusEvent::Gone(identity),
            None,
            cursors,
            intake,
        ),
        ParticipationAwareStatusStreamEvent::ParticipationConvergence(receipt) => {
            observe_participation_convergence(
                participation,
                receipt,
                participation_convergence,
                participation_cursor,
            )
            .await
        }
    }
}

/// Enqueues one observed event and advances its task's cursor only after the
/// bounded intake retained it.
///
/// This is everything the receive path may do. It classifies nothing, opens no
/// edge, completes no stage, and settles no operation.
fn observe_task_event(
    participation: ParticipationRef,
    identity: TaskIdentity,
    event: StatusEvent,
    next_cursor: Option<TaskStatusCursor>,
    cursors: &mut BTreeMap<TaskIdentity, TaskStatusCursor>,
    intake: &StatusIntakeHandle,
) -> Result<StreamObservation, SubscriptionState> {
    if identity.query_execution_id() != participation.query_execution_id() {
        tracing::warn!(
            context_execution = ?participation.query_execution_id(),
            event_execution = ?identity.query_execution_id(),
            "SubscribeTaskStatus event addresses a different query execution"
        );
        return Err(SubscriptionState::QueryMismatch);
    }
    if identity.backend_process_id() != participation.backend_process_id() {
        tracing::warn!(
            context_backend = %participation.backend_process_id(),
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
    participation: ParticipationRef,
    event: &proto::TaskStatusStreamEvent,
    cursors: &mut BTreeMap<TaskIdentity, TaskStatusCursor>,
    intake: &StatusIntakeHandle,
) -> Result<(), SubscriptionState> {
    let decoded =
        decode_participation_aware_status_event(event, FieldPath::root("task_status_stream_event"))
            .map_err(|_| SubscriptionState::Rejected)?;
    let observed = match decoded {
        ParticipationAwareStatusStreamEvent::Status(status) => {
            let identity = status.identity();
            let cursor = cursors
                .get(&identity)
                .copied()
                .unwrap_or_else(|| TaskStatusCursor::unobserved(identity));
            observe_task_event(
                participation,
                identity,
                StatusEvent::Published(Box::new(status.clone())),
                Some(cursor.advanced_to(status.version())),
                cursors,
                intake,
            )?
        }
        ParticipationAwareStatusStreamEvent::Gone(identity) => observe_task_event(
            participation,
            identity,
            StatusEvent::Gone(identity),
            None,
            cursors,
            intake,
        )?,
        ParticipationAwareStatusStreamEvent::ParticipationConvergence(_) => {
            return Err(SubscriptionState::Rejected);
        }
    };
    match observed {
        StreamObservation::Delivered => Ok(()),
        StreamObservation::AwaitTaskReconciliation => Err(SubscriptionState::Resubscribing),
    }
}

async fn observe_participation_convergence(
    participation: ParticipationRef,
    receipt: ParticipationConvergenceReceipt,
    intake: Option<&ParticipationConvergenceIntakeHandle>,
    cursor: &mut Option<ParticipationConvergenceCursor>,
) -> Result<StreamObservation, SubscriptionState> {
    let (Some(intake), Some(current)) = (intake, cursor.as_mut()) else {
        tracing::warn!(
            %participation,
            "a task-only SubscribeTaskStatus stream received a participation convergence event"
        );
        return Err(SubscriptionState::Rejected);
    };
    if receipt.participation() != participation {
        tracing::warn!(
            %participation,
            receipt_context = %receipt.participation(),
            "SubscribeTaskStatus convergence event addresses a different participation"
        );
        return Err(SubscriptionState::Rejected);
    }
    if let Some(version) = current.current_version() {
        if receipt.version() < version {
            tracing::warn!(
                %participation,
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
        match intake.publish(participation, receipt) {
            Ok(admission) => {
                assert!(admission.authorizes_cursor_advance());
                *current = current.advanced_to(receipt.version());
                return Ok(StreamObservation::Delivered);
            }
            Err(ParticipationConvergencePublishError::Overflow) => {
                let changed = intake.wait_for_capacity_change(capacity.epoch()).await;
                if changed.is_closed() {
                    tracing::warn!(
                        %participation,
                        "participation convergence intake closed while waiting for capacity"
                    );
                    return Err(SubscriptionState::Rejected);
                }
            }
            Err(error) => {
                tracing::warn!(
                    %participation,
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
    participation: ParticipationRef,
    cursors: &BTreeMap<TaskIdentity, TaskStatusCursor>,
    participation_cursor: Option<ParticipationConvergenceCursor>,
) -> Result<tonic::Streaming<proto::TaskStatusStreamEvent>, tonic::Status> {
    let cursors = cursors.values().copied().collect::<Vec<_>>();
    let request = encode_participation_aware_subscribe_task_status(
        participation,
        &cursors,
        participation_cursor,
    )
    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let mut grpc = client
        .grpc_with_channel_error(NativeRpcMethod::SubscribeTaskStatus)
        .await
        .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
    grpc.subscribe_task_status(tonic::Request::new(request))
        .await
        .map(tonic::Response::into_inner)
}

/// One covered observation stream per Participation. Its request is always built
/// from cursors the serial owner has applied; receiving a frame does not make
/// that frame a safe reconnect cursor.
fn covered_protocol(site: &'static str) -> crate::task_execution::error::TaskExecutionError {
    crate::task_execution::error::TaskExecutionError::Schedule(site.to_owned())
}
fn covered_registration(
    error: super::subscription_owner::RegistrationError,
) -> crate::task_execution::error::TaskExecutionError {
    use super::subscription_owner::RegistrationError;
    match error {
        RegistrationError::Work(error) => {
            if matches!(&error, novarocks_workload_control::WorkError::Conflict) {
                tracing::warn!(
                    "Native covered subscription source reported conflicting work facts"
                );
            }
            crate::task_execution::error::TaskExecutionError::Workload(error)
        }
        RegistrationError::Closed => covered_protocol("Native subscription source closed"),
        RegistrationError::Storage => {
            covered_protocol("Native subscription source positions exhausted")
        }
        RegistrationError::Generation => {
            covered_protocol("Native subscription generation exhausted")
        }
        RegistrationError::SpawnUnjoinable => {
            covered_protocol("Native subscription original spawn unjoinable")
        }
    }
}

enum CoveredSourceChildren {
    LegacyIndividual,
    Cohort(crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort),
}

pub(crate) struct CoveredTaskStatusSubscriber {
    targets: BTreeMap<BackendProcessId, TaskBackendTarget>,
    intake: Arc<ObservationIntake>,
    error_budget: u32,
    data_runtime: FrontendDataRuntime,
    work_scope: novarocks_workload_control::WorkScope,
    active: Mutex<BTreeMap<ParticipationRef, CoveredEntry>>,
    source_children: CoveredSourceChildren,
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

enum CoveredEntry {
    Pending(super::subscription_owner::SubscriptionReservation),
    Starting(super::subscription_owner::SubscriptionStopGuard),
    Running(CoveredSubscription),
}
struct CoveredSubscription {
    state: Arc<Mutex<SubscriptionState>>,
    applied_snapshot: Arc<Mutex<DecodedCoveredSubscription>>,
    reconciliation: tokio::sync::watch::Sender<DecodedCoveredSubscription>,
    task: super::subscription_owner::SubscriptionStopGuard,
}

impl Drop for CoveredSubscription {
    fn drop(&mut self) {
        self.task.stop();
    }
}

impl CoveredTaskStatusSubscriber {
    /// Explicit old coordinator/fixture API; production manifest uses the cohort constructor.
    pub(crate) fn new(
        backends: &[BackendProcessDescriptor],
        intake: Arc<ObservationIntake>,
        error_budget: u32,
        data_runtime: FrontendDataRuntime,
        work_scope: novarocks_workload_control::WorkScope,
    ) -> Result<Self, NativeTaskTransportCreationError> {
        if error_budget == 0 {
            return Err(NativeTaskTransportCreationError::ZeroStatusErrorBudget);
        }
        Ok(Self {
            targets: freeze_targets(backends, &data_runtime)?,
            intake,
            error_budget,
            data_runtime,
            work_scope,
            active: Mutex::new(BTreeMap::new()),
            source_children: CoveredSourceChildren::LegacyIndividual,
        })
    }

    pub(crate) fn new_with_source_cohort(
        backends: &[BackendProcessDescriptor],
        intake: Arc<ObservationIntake>,
        error_budget: u32,
        data_runtime: FrontendDataRuntime,
        work_scope: novarocks_workload_control::WorkScope,
        source_cohort: crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort,
    ) -> Result<Self, NativeTaskTransportCreationError> {
        if error_budget == 0 {
            return Err(NativeTaskTransportCreationError::ZeroStatusErrorBudget);
        }
        Ok(Self {
            targets: freeze_targets(backends, &data_runtime)?,
            intake,
            error_budget,
            data_runtime,
            work_scope,
            active: Mutex::new(BTreeMap::new()),
            source_children: CoveredSourceChildren::Cohort(source_cohort),
        })
    }

    /// Reserve only the existing finite source header. The SAME subscription
    /// service polls child admission; Pending never suspends the serial fold.
    /// Call before constructing covered_request or its request/watch/F captures.
    pub(crate) fn prepare(
        &self,
        participation: ParticipationRef,
    ) -> Result<bool, crate::task_execution::error::TaskExecutionError> {
        if !self
            .targets
            .contains_key(&participation.backend_process_id())
        {
            return Err(covered_protocol(
                "covered participation is outside the frozen topology",
            ));
        }
        let port = self.data_runtime.subscription_port();
        let runner_wake = self.intake.progress_wake();
        let mut created = false;
        let result = {
            let mut active = self
                .active
                .lock()
                .map_err(|_| covered_protocol("covered subscription lock poisoned"))?;
            if let std::collections::btree_map::Entry::Vacant(entry) = active.entry(participation) {
                // Both constructors only record metadata under caller/Core locks.
                // The original service consumes the paid cohort outside them.
                let original = match &self.source_children {
                    CoveredSourceChildren::Cohort(cohort) => {
                        port.prepare_from_cohort(&self.work_scope, cohort.clone(), None, None)
                    }
                    CoveredSourceChildren::LegacyIndividual => {
                        #[cfg(not(test))]
                        let original = port.prepare(&self.work_scope, None, None);
                        #[cfg(test)]
                        let original = if !self.work_scope.has_responsibility_service() {
                            port.reserve(&self.work_scope, None, None)
                        } else {
                            port.prepare(&self.work_scope, None, None)
                        };
                        original
                    }
                }
                .map_err(covered_registration)?;
                entry.insert(CoveredEntry::Pending(original));
                created = true;
            }
            match active
                .get(&participation)
                .expect("same original source entry")
            {
                CoveredEntry::Pending(original) => {
                    original.ready(&runner_wake).map_err(covered_registration)
                }
                CoveredEntry::Starting(_) => Ok(false),
                CoveredEntry::Running(_) => Ok(true),
            }
        };
        // Source is stable before any callback; no active/Core lock escapes.
        if created {
            port.notify();
        }
        result
    }
    pub(crate) fn ensure(
        &self,
        request: DecodedCoveredSubscription,
    ) -> Result<(), crate::task_execution::error::TaskExecutionError> {
        self.apply_request(request, false)
    }
    pub(crate) fn reconcile(
        &self,
        request: DecodedCoveredSubscription,
    ) -> Result<(), crate::task_execution::error::TaskExecutionError> {
        self.apply_request(request, true)?;
        observe_resubscribe();
        Ok(())
    }
    /// Existing Running aliases only; no pending source creates an SDK request.
    pub(crate) fn update_applied_request(
        &self,
        request: DecodedCoveredSubscription,
    ) -> Result<(), crate::task_execution::error::TaskExecutionError> {
        encode_covered_subscribe_task_status(&request)
            .map_err(|_| covered_protocol("covered subscription encoding refused"))?;
        let snapshot = {
            let active = self
                .active
                .lock()
                .map_err(|_| covered_protocol("covered subscription lock poisoned"))?;
            match active.get(&request.participation) {
                Some(CoveredEntry::Running(original)) => Arc::clone(&original.applied_snapshot),
                _ => return Err(covered_protocol("covered subscription is not running")),
            }
        };
        *snapshot
            .lock()
            .map_err(|_| covered_protocol("covered applied snapshot lock poisoned"))? = request;
        Ok(())
    }
    fn apply_request(
        &self,
        request: DecodedCoveredSubscription,
        reconcile: bool,
    ) -> Result<(), crate::task_execution::error::TaskExecutionError> {
        encode_covered_subscribe_task_status(&request)
            .map_err(|_| covered_protocol("covered subscription encoding refused"))?;
        let participation = request.participation;
        #[cfg(test)]
        if !self
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&participation)
        {
            // The old unbound wire fixtures called ensure directly. Production
            // serial turns always run the explicit prepare gate before captures.
            if !self.prepare(participation)? {
                return Err(covered_protocol("covered child not ready"));
            }
        }
        // Copy only existing handles. watch callbacks and old request destruction
        // are outside active; a concurrent stop cannot resurrect this incarnation.
        let running = {
            let active = self
                .active
                .lock()
                .map_err(|_| covered_protocol("covered subscription lock poisoned"))?;
            match active.get(&participation) {
                Some(CoveredEntry::Running(original)) => Some((
                    Arc::clone(&original.applied_snapshot),
                    original.reconciliation.clone(),
                )),
                _ => None,
            }
        };
        if let Some((snapshot, sender)) = running {
            *snapshot
                .lock()
                .map_err(|_| covered_protocol("covered applied snapshot lock poisoned"))? =
                request.clone();
            if reconcile {
                drop(sender.send_replace(request));
            }
            return Ok(());
        }
        let (reservation, publication) = {
            let mut active = self
                .active
                .lock()
                .map_err(|_| covered_protocol("covered subscription lock poisoned"))?;
            if !matches!(active.get(&participation), Some(CoveredEntry::Pending(_))) {
                return Err(covered_protocol(
                    "covered subscription requires prepared original child",
                ));
            }
            let Some(CoveredEntry::Pending(reservation)) = active.remove(&participation) else {
                unreachable!()
            };
            let publication = reservation.stop_view();
            let replacement = reservation.stop_view();
            active.insert(participation, CoveredEntry::Starting(replacement));
            (reservation, publication)
        };
        // Original Core/header and the Starting stop view already exist. All
        // input construction, user spawn, wake and Drop are outside active.
        let started = self.start(reservation, request)?;
        let displaced = {
            let mut active = self
                .active
                .lock()
                .map_err(|_| covered_protocol("covered subscription lock poisoned"))?;
            let same = matches!(active.get(&participation), Some(CoveredEntry::Starting(original)) if original.same_original(&publication));
            if same {
                active.insert(participation, CoveredEntry::Running(started))
            } else {
                drop(active);
                drop(started);
                drop(publication);
                return Ok(());
            }
        };
        if let Some(CoveredEntry::Starting(original)) = displaced {
            original.disarm();
        }
        publication.disarm();
        Ok(())
    }
    pub(crate) fn stop(&self, participation: ParticipationRef) {
        let original = self
            .active
            .lock()
            .ok()
            .and_then(|mut active| active.remove(&participation));
        drop(original);
    }
    pub(crate) fn state(&self, participation: ParticipationRef) -> Option<SubscriptionState> {
        let state = {
            let active = self.active.lock().ok()?;
            match active.get(&participation)? {
                CoveredEntry::Running(original) => Arc::clone(&original.state),
                CoveredEntry::Pending(_) | CoveredEntry::Starting(_) => return None,
            }
        };
        state.lock().ok().map(|state| *state)
    }
    fn start(
        &self,
        reservation: super::subscription_owner::SubscriptionReservation,
        request: DecodedCoveredSubscription,
    ) -> Result<CoveredSubscription, crate::task_execution::error::TaskExecutionError> {
        let target = self
            .targets
            .get(&request.participation.backend_process_id())
            .ok_or_else(|| {
                covered_protocol("covered participation is outside the frozen topology")
            })?;
        let publisher = Arc::new(self.intake.subscribe().map_err(|_| {
            covered_protocol("covered observation subscription capacity exhausted")
        })?);
        let state = Arc::new(Mutex::new(SubscriptionState::Opening));
        let applied_snapshot = Arc::new(Mutex::new(request.clone()));
        let (reconciliation, receiver) = tokio::sync::watch::channel(request);
        reservation.attach(Arc::clone(&publisher), Arc::clone(&state));
        let task = reservation
            .start(run_covered_subscription(
                target.client.clone(),
                receiver,
                Arc::clone(&applied_snapshot),
                publisher,
                self.error_budget,
                Arc::clone(&state),
            ))
            .map_err(covered_registration)?;
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
                formatter.write_str("covered observation addresses another participation")
            }
            Self::Protocol(detail) => formatter.write_str(detail),
        }
    }
}

fn validate_covered_task_participation(
    identity: TaskIdentity,
    participation: ParticipationRef,
) -> Result<(), CoveredEventValidationError> {
    if identity.query_execution_id() != participation.query_execution_id() {
        return Err(CoveredEventValidationError::QueryMismatch);
    }
    if identity.backend_process_id() != participation.backend_process_id() {
        return Err(CoveredEventValidationError::ProcessMismatch);
    }
    Ok(())
}

fn validate_covered_receipt_participation(
    observed: ParticipationRef,
    expected: ParticipationRef,
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

fn validate_covered_event_participation(
    request: &DecodedCoveredSubscription,
    requested_versions: &CoveredRequestedVersions,
    event: &CoveredStatusStreamEvent,
) -> Result<(), CoveredEventValidationError> {
    let participation = request.participation;
    // Classify identity before version prerequisites: a foreign Unchanged
    // frame is an identity violation even though no foreign cursor was sent.
    match &event.fact {
        CoveredStatusStreamFact::Status(status) => {
            validate_covered_task_participation(status.identity(), participation)?
        }
        CoveredStatusStreamFact::Gone(identity)
        | CoveredStatusStreamFact::Unknown(identity)
        | CoveredStatusStreamFact::StatusUnchanged(identity)
        | CoveredStatusStreamFact::TaskConvergenceUnchanged(identity) => {
            validate_covered_task_participation(*identity, participation)?
        }
        CoveredStatusStreamFact::TaskConvergence(receipt) => {
            validate_covered_task_participation(receipt.identity(), participation)?
        }
        CoveredStatusStreamFact::ParticipationConvergence(receipt) => {
            validate_covered_receipt_participation(receipt.participation(), participation)?
        }
        CoveredStatusStreamFact::Quiesce(receipt) => {
            validate_covered_receipt_participation(receipt.participation(), participation)?;
            for identity in receipt.accepted_tasks() {
                validate_covered_task_participation(*identity, participation)?;
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

/// Preserve the channel owner's observed IO loss before any diagnostic display.
#[derive(Debug)]
enum CoveredSubscriptionOpenError {
    Channel(super::transport::ChannelAcquisitionError),
    Rpc(tonic::Status),
}
impl CoveredSubscriptionOpenError {
    fn fatal_status(&self) -> Option<&tonic::Status> {
        match self {
            Self::Rpc(status) if subscription_rejection_is_fatal(status) => Some(status),
            _ => None,
        }
    }
    fn observed_connection_loss(&self) -> bool {
        match self {
            Self::Channel(error) => error.observed_connection_loss(),
            Self::Rpc(status) => super::transport::is_observed_connection_loss(status),
        }
    }
}

/// Evidence belongs to this original reconnect streak, not to a status label.
#[derive(Default)]
struct CoveredConnectionEvidence {
    lost: bool,
}
impl CoveredConnectionEvidence {
    fn observe_loss(&mut self, observed: bool) {
        self.lost |= observed;
    }
    fn observe_liveness(&mut self) {
        self.lost = false;
    }
    fn exhausted_state(&self) -> SubscriptionState {
        if self.lost {
            SubscriptionState::ConnectionLossExhausted
        } else {
            SubscriptionState::BudgetExhausted
        }
    }
}

async fn open_covered_subscription(
    client: &Client,
    request: &DecodedCoveredSubscription,
) -> Result<tonic::Streaming<proto::TaskStatusStreamEvent>, CoveredSubscriptionOpenError> {
    let request = encode_covered_subscribe_task_status(request).map_err(|error| {
        CoveredSubscriptionOpenError::Rpc(tonic::Status::invalid_argument(error.to_string()))
    })?;
    let mut grpc = client
        .grpc_with_channel_error(NativeRpcMethod::SubscribeTaskStatus)
        .await
        .map_err(CoveredSubscriptionOpenError::Channel)?;
    grpc.subscribe_task_status(tonic::Request::new(request))
        .await
        .map(tonic::Response::into_inner)
        .map_err(CoveredSubscriptionOpenError::Rpc)
}

/// Actual same-factory F layout; does not construct a client, request or F.
#[cfg(test)]
pub(super) fn covered_subscription_future_layout() -> std::alloc::Layout {
    type CoveredSubscriptionFactory<F> = fn(
        Client,
        tokio::sync::watch::Receiver<DecodedCoveredSubscription>,
        Arc<Mutex<DecodedCoveredSubscription>>,
        Arc<ObservationPublisher>,
        u32,
        Arc<Mutex<SubscriptionState>>,
    ) -> F;
    fn layout<F: Future<Output = ()>>(_: CoveredSubscriptionFactory<F>) -> std::alloc::Layout {
        std::alloc::Layout::new::<F>()
    }
    layout(run_covered_subscription)
}

async fn run_covered_subscription(
    client: Client,
    mut reconciliation: tokio::sync::watch::Receiver<DecodedCoveredSubscription>,
    applied_snapshot: Arc<Mutex<DecodedCoveredSubscription>>,
    publisher: Arc<ObservationPublisher>,
    error_budget: u32,
    state: Arc<Mutex<SubscriptionState>>,
) {
    reconciliation.borrow_and_update();
    let mut applied: DecodedCoveredSubscription;
    let mut last_generation = 0_u64;
    let jitter = std::collections::hash_map::RandomState::new();
    let mut failures = 0_u32;
    let mut connection = CoveredConnectionEvidence::default();
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
        let participation = applied.participation;
        let opened = tokio::select! {
            changed = reconciliation.changed() => {
                if changed.is_err() { return; }
                reconciliation.borrow_and_update();
                set_state(&state, SubscriptionState::Resubscribing);
                continue 'subscription;
            }
            opened = tokio::time::timeout(
                COVERED_STREAM_LIVENESS_WINDOW, open_covered_subscription(&client, &applied)
            ) => opened.unwrap_or_else(|_| Err(CoveredSubscriptionOpenError::Rpc(tonic::Status::deadline_exceeded(
                "covered observation subscription opening exceeded its liveness budget"
            )))),
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
                                    tracing::warn!(%participation, detail = %error, "covered observation frame is malformed");
                                    set_state(&state, SubscriptionState::Rejected);
                                    return;
                                }
                            };
                            if let Err(error) = validate_covered_event_participation(
                                &applied,
                                &requested_versions,
                                &event,
                            ) {
                                tracing::warn!(%participation, detail = %error, "covered observation frame conflicts with its stream");
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
                            let convergence = match &event.fact {
                                CoveredStatusStreamFact::ParticipationConvergence(receipt) => {
                                    Some(*receipt)
                                }
                                _ => None,
                            };
                            let terminal = match &event.fact {
                                CoveredStatusStreamFact::Status(status) if status.is_terminal() => {
                                    Some((status.identity(), status.state()))
                                }
                                _ => None,
                            };
                            if let Err(error) = permit.publish(
                                ObservationFrame::Covered {
                                    participation,
                                    generation: applied.generation.get(),
                                    event,
                                },
                                encoded_bytes,
                            ) {
                                tracing::warn!(
                                    %participation,
                                    charged_bytes = error.charged_bytes,
                                    max_frame_bytes = error.max_frame_bytes,
                                    "covered observation frame exceeds its reserved budget"
                                );
                                set_state(&state, SubscriptionState::Rejected);
                                return;
                            }
                            if let Some(receipt) = convergence {
                                tracing::debug!(
                                    %participation,
                                    execution_id = ?participation.query_execution_id(),
                                    version = %receipt.version(),
                                    state = ?receipt.state(),
                                    "frontend retained worker stop and participation fence"
                                );
                            }
                            if let Some((identity, state)) = terminal {
                                tracing::debug!(
                                    execution_id = ?identity.query_execution_id(),
                                    stage = identity.stage_id().get(),
                                    task = identity.task_id().get(),
                                    backend = %identity.backend_process_id(),
                                    ?state,
                                    "frontend retained task terminal"
                                );
                            }
                            if new_liveness {
                                connection.observe_liveness();
                                observed_liveness = true;
                                liveness_deadline =
                                    tokio::time::Instant::now() + COVERED_STREAM_LIVENESS_WINDOW;
                            }
                        }
                        Ok(None) => break,
                        Err(status) if subscription_rejection_is_fatal(&status) => {
                            tracing::warn!(%participation, code = ?status.code(), detail = status.message(), "covered observation stream rejected");
                            set_state(&state, SubscriptionState::Rejected);
                            return;
                        }
                        Err(status) => {
                            connection.observe_loss(super::transport::is_observed_connection_loss(
                                &status,
                            ));
                            break;
                        }
                    }
                }
                if observed_liveness {
                    failures = 0;
                }
            }
            Err(error) if error.fatal_status().is_some() => {
                let status = error.fatal_status().expect("fatal status was checked");
                tracing::warn!(%participation, code = ?status.code(), detail = status.message(), "covered observation subscription rejected");
                set_state(&state, SubscriptionState::Rejected);
                return;
            }
            Err(error) => connection.observe_loss(error.observed_connection_loss()),
        }
        // A disconnection does not erase accepted facts or manufacture a
        // local observation gap. Reconnect from the owner's applied cursors.
        failures += 1;
        if failures >= error_budget {
            set_state(&state, connection.exhausted_state());
            return;
        }
        set_state(&state, SubscriptionState::Resubscribing);
        observe_resubscribe();
        let sample =
            std::hash::BuildHasher::hash_one(&jitter, (applied.participation, last_generation));
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
        OperationOutcome::OwnerFailed => "owner_failed",
        OperationOutcome::Accepted => "accepted",
        OperationOutcome::Idempotent => "idempotent",
        OperationOutcome::OperationTimedOut => "operation_timed_out",
        OperationOutcome::IdentityMismatch => "identity_mismatch",
        OperationOutcome::CompatibilityMismatch => "compatibility_mismatch",
        OperationOutcome::ParticipationNotEstablished => "context_not_established",
        OperationOutcome::ParticipationConflict => "context_conflict",
        OperationOutcome::DomainConflict => "domain_conflict",
        OperationOutcome::LeaseExpired => "lease_expired",
        OperationOutcome::ReleaseNotReady => "release_not_ready",
        OperationOutcome::ParticipationTerminalReceipt => "context_terminal_receipt",
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

static PARTICIPATION_LEASE_RENEWALS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            "novarocks_participation_lease_renewal_total",
            "Query execution lease renewals settled by the frontend, by outcome category.",
        ),
        &["outcome"],
    )
    .expect("register novarocks_participation_lease_renewal_total")
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
        Box::new(PARTICIPATION_LEASE_RENEWALS.clone()),
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
/// participation owner from its own clock, so transport health can never extend one.
fn observe_settled(kind: OperationKind, lease_renewal: bool, outcome: OperationOutcome) {
    TASK_OPERATION_RECEIPTS
        .with_label_values(&[kind.as_str(), outcome_name(outcome)])
        .inc();
    if lease_renewal {
        PARTICIPATION_LEASE_RENEWALS
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
            crate::task_execution::operation::IngressRejection::WaitingCapacity => {
                "ingress_waiting_capacity"
            }
            crate::task_execution::operation::IngressRejection::BodyLimit => "ingress_body_limit",
            crate::task_execution::operation::IngressRejection::UnclassifiedCapacity => {
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
        PARTICIPATION_LEASE_RENEWALS
            .with_label_values(&[name])
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

    #[test]
    fn original_method_run_progress_quotes_the_full_supported_count_and_rejects_mixed_runs() {
        use novarocks_workload_control::joint_admission::JointResource;
        let budget = TransportBudget::DEFAULT;
        let backend = BackendProcessId::new_v7();
        let operations: Vec<_> = (0..budget.max_batch_items())
            .map(|task| cancel_intent(task as u32 + 1, backend))
            .collect();
        let quote =
            NativeTaskOperationSink::progress_for_original_method_run(&operations, budget).unwrap();
        let foreground = quote.foreground().unwrap();
        let settlement = quote.settlement().unwrap();
        assert_eq!(foreground, settlement);
        assert_eq!(foreground.get(JointResource::AsyncIo), 1);
        assert_eq!(foreground.get(JointResource::PhysicalQueueRecords), 1);
        assert_eq!(
            foreground.get(JointResource::PhysicalQueueBytes),
            joint_action::original_run_encoding(&operations, budget)
                .unwrap()
                .protobuf_bytes() as u64
        );
        assert_eq!(
            quote.custody().get(JointResource::ReplyRecords),
            operations.len() as u64
        );
        assert_eq!(quote.custody().get(JointResource::QueueRecords), 0);
        assert_eq!(
            quote.custody().get(JointResource::PayloadBytes),
            (budget.message_limits().control_response_max_bytes
                + novarocks_task_codec::resource_preflight::OperationReplyDecodePlan::for_response(
                    operations.len(),
                    budget.message_limits().control_response_max_bytes
                )
                .unwrap()
                .peak_bytes()) as u64
        );
        let mut over = operations.clone();
        over.push(cancel_intent(99, backend));
        assert!(NativeTaskOperationSink::progress_for_original_method_run(&over, budget).is_err());
        assert!(NativeTaskOperationSink::progress_for_original_method_run(&[], budget).is_err());
        let same_control_method = [
            cancel_intent(1, backend),
            abort_intent(participation(backend)),
        ];
        assert_eq!(
            same_control_method[0].native_run_key(),
            same_control_method[1].native_run_key()
        );
        assert!(
            NativeTaskOperationSink::progress_for_original_method_run(&same_control_method, budget)
                .is_ok()
        );
        let mixed = [cancel_intent(1, backend), update_intent(2, backend, 1)];
        assert_ne!(mixed[0].native_run_key(), mixed[1].native_run_key());
        assert!(NativeTaskOperationSink::progress_for_original_method_run(&mixed, budget).is_err());
    }

    #[test]
    fn original_small_method_runs_invoice_the_actual_codec_without_a_composition_envelope() {
        use novarocks_workload_control::joint_admission::JointResource;
        let budget = TransportBudget::DEFAULT;
        let backend = BackendProcessId::new_v7();
        let runs = [
            vec![
                cancel_intent(1, backend),
                abort_intent(participation(backend)),
            ],
            vec![renew_intent(backend)],
            vec![update_intent(1, backend, 128)],
            (1..=budget.max_batch_items())
                .map(|id| cancel_intent(id as u32, backend))
                .collect(),
        ];
        for operations in runs {
            let plan = joint_action::original_run_encoding(&operations, budget).unwrap();
            let encoded = operations
                .iter()
                .map(|item| {
                    (
                        is_small_control(item),
                        encode_operation(item, &test_attempt_facts()).unwrap(),
                    )
                })
                .collect();
            let batches = encode_method_batches(encoded, budget).unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(plan.retained_bytes(), batches[0].encoded_len() + 5);
            let actual =
                NativeTaskOperationSink::progress_for_original_method_run(&operations, budget)
                    .unwrap();
            let payload = actual
                .foreground()
                .unwrap()
                .get(JointResource::PayloadBytes);
            assert_eq!(
                payload,
                plan.retained_bytes() as u64
                    + joint_action::clone_input_backing(&operations).unwrap() as u64
            );
            assert!(
                payload < 64 * 1024,
                "small full runs must not reserve the maximum 96 MiB wire envelope"
            );
            assert_eq!(
                actual.custody().get(JointResource::ReplyRecords),
                operations.len() as u64
            );
            assert_eq!(
                actual
                    .foreground()
                    .unwrap()
                    .get(JointResource::PhysicalQueueRecords),
                1
            );
            assert_eq!(
                actual
                    .foreground()
                    .unwrap()
                    .get(JointResource::PhysicalQueueBytes),
                plan.protobuf_bytes() as u64
            );
            assert_eq!(plan.retained_bytes(), plan.protobuf_bytes() + 5);
        }
    }

    #[test]
    fn original_dispatch_batch_drives_the_input_gate_before_dto_construction() {
        use super::action_input::{NativeApplyInputPlan, known_input_layouts_for_composition};
        use std::{alloc::Layout, num::NonZeroUsize};
        let backend = BackendProcessId::new_v7();
        let batch = released_batch(
            backend,
            vec![cancel_intent(1, backend), cancel_intent(2, backend)],
        );
        let plan =
            NativeApplyInputPlan::from_operations(batch.operations(), TransportBudget::DEFAULT)
                .unwrap();
        assert_eq!(plan.items(), batch.operations().len());
        let layouts = known_input_layouts_for_composition(
            NonZeroUsize::new(plan.items()).unwrap(),
            TransportBudget::DEFAULT,
        )
        .unwrap();
        fn frame<F: Future<Output = ()>>(
            _: fn(ApplySend, Vec<EncodedMethodBatch>, tokio::time::Instant) -> F,
        ) -> Layout {
            Layout::new::<F>()
        }
        assert_eq!(layouts.original.future, frame(apply_operations));
        let too_many =
            vec![cancel_intent(1, backend); TransportBudget::DEFAULT.max_batch_items() + 1];
        assert!(
            NativeApplyInputPlan::from_operations(&too_many, TransportBudget::DEFAULT).is_err()
        );
        assert!(NativeApplyInputPlan::from_operations(&[], TransportBudget::DEFAULT).is_err());
        let error = encode_method_batches(
            vec![
                (false, proto::TaskOperation::default());
                TransportBudget::DEFAULT.max_batch_items() + 1
            ],
            TransportBudget::DEFAULT,
        );
        assert!(error.is_err());
    }

    use std::collections::VecDeque;
    use std::num::NonZeroUsize;
    use std::pin::Pin;

    use novarocks_execution::task_execution::{
        AbortCause, AbortParticipation, AdmissionTicketId, CancelReason, CancelTask,
        CreateTaskReceipt, CredentialDomainId, CredentialEpoch, CredentialUpdate, DomainVersion,
        EstablishParticipation, FetchTaskDynamicFilters, LeaseSequence, LeaseValidFor,
        ParticipationConvergenceCursor, ParticipationConvergenceReceipt,
        ParticipationConvergenceState, ParticipationConvergenceVersion, ParticipationReceipt,
        ParticipationState, QuiesceParticipationReceipt, RenewParticipationLease,
        TaskConvergenceCursor, TaskConvergenceReceipt, TaskConvergenceVersion, TaskDomainUpdate,
        TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion, UpdateTask,
    };
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_models::{catalog, filter};
    use novarocks_task_codec::domain::{WireContent, WireCredential};
    use novarocks_task_codec::operation::{
        CoveredCatchUpComplete, CoveredObservationBookmark, QuiesceObservationCursor,
        decode_covered_subscribe_task_status, decode_participation_aware_subscribe_task_status,
        decode_subscribe_task_status, encode_covered_status_event, encode_operation_outcome,
        encode_participation_ack, encode_participation_convergence_event, encode_status_event,
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
    use crate::task_execution::dispatch::OperationDispatcher;
    use crate::task_execution::participation_convergence::ParticipationConvergenceIntake;
    use crate::task_execution::status_intake::{
        CountingWake, ObservationIntake, ObservationIntakeEntry, StatusIntake,
    };
    use crate::task_protocol_config::DispatchBudget;
    use novarocks_coordination_contract::MonotonicInstant;
    use novarocks_native_adapter::generated::nova_rocks_grpc_server::{
        NovaRocksGrpc, NovaRocksGrpcServer,
    };

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

    /// The original test role and the SAME Workload authority supplied to its
    /// Covered subscriber. Cleanup joins original tasks before settling root.
    struct CoveredRoleFixture {
        runtime: FrontendDataRuntime,
        workload: novarocks_workload_control::WorkloadControl,
        root: Option<novarocks_workload_control::RootWork>,
    }

    impl CoveredRoleFixture {
        fn new() -> Self {
            use novarocks_workload_control::{
                WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
            };
            let config = WorkloadConfig::default();
            config.validate().unwrap();
            let workload = WorkloadControl::try_new_counted(config).unwrap().owner;
            workload.mark_ready().unwrap();
            let root = workload
                .try_begin_root(WorkRequest::new(WorkClass::Query))
                .unwrap();
            Self {
                runtime: FrontendDataRuntime::new(tokio::runtime::Handle::current()),
                workload,
                root: Some(root),
            }
        }

        fn scope(&self) -> novarocks_workload_control::WorkScope {
            self.root.as_ref().unwrap().owner.scope().clone()
        }

        async fn settle(mut self) {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            self.runtime
                .drain_original_subscriptions_until(deadline)
                .await
                .unwrap();
            self.runtime
                .drain_original_apply_until(deadline)
                .await
                .unwrap();
            self.runtime
                .drain_native_outgoing_until(deadline)
                .await
                .unwrap();
            let root = self.root.take().unwrap();
            root.owner.complete_after_terminal_cancel_settled();
            root.business.release();
            self.workload.close_admission();
            assert!(self.workload.shutdown().is_ok());
        }
    }

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

    fn participation(backend: BackendProcessId) -> ParticipationRef {
        ParticipationRef::new(execution_id(), FrontendProcessId::new_v7(), backend)
    }

    fn cancel_intent(task: u32, backend: BackendProcessId) -> OperationIntent {
        OperationIntent::CancelTask(CancelTask::new(
            TaskOperationId::new_v7(),
            identity(task, backend),
            CancelReason::UpstreamNoLongerNeeded,
        ))
    }

    fn abort_intent(participation: ParticipationRef) -> OperationIntent {
        OperationIntent::AbortParticipation(AbortParticipation::new(
            TaskOperationId::new_v7(),
            participation,
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
        OperationIntent::UpdateParticipation(Arc::new(UpdateParticipation::RenewLease(
            RenewParticipationLease::new(
                TaskOperationId::new_v7(),
                participation(backend),
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
        let request = UpdateParticipation::Establish(EstablishParticipation::new(
            TaskOperationId::new_v7(),
            participation(backend),
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
                CredentialDomainId::new(1),
                CredentialEpoch::FIRST,
                Arc::new(
                    WireCredential::decode(&[], FieldPath::root("credential"))
                        .expect("an empty credential table is legal"),
                ),
            ),
            LeaseValidFor::new(Duration::from_secs(30)).expect("a legal lease"),
        ));

        let encoded = encode_participation_operation(&request, &test_attempt_facts())
            .expect("the establish is codec-owned");
        let UpdateParticipation::Establish(original) = &request else {
            unreachable!()
        };
        assert_eq!(
            novarocks_task_codec::operation::wire_size::establish(original).unwrap(),
            prost::Message::encoded_len(&encoded)
        );
        let Some(proto::task_operation::Operation::UpdateParticipation(update)) = encoded.operation
        else {
            panic!("an establish encodes as UpdateParticipation");
        };
        let Some(proto::update_participation_request::Command::Establish(establish)) =
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
        WithAck(Vec<(OperationOutcome, Option<proto::task_operation_verdict::Ack>)>),
        Reject(tonic::Code),
        RejectWithMessage(tonic::Code, &'static str),
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
        heartbeat_descriptor: Option<BackendProcessDescriptor>,
        heartbeats: Vec<proto::HeartbeatRequest>,
        apply_answers: VecDeque<ApplyAnswer>,
        subscribed: Vec<proto::SubscribeTaskStatusRequest>,
        subscribe_answers: VecDeque<SubscribeAnswer>,
        held: Vec<tokio::sync::mpsc::Sender<Result<proto::TaskStatusStreamEvent, Status>>>,
    }

    #[derive(Clone)]
    struct TaskWirePeer {
        state: Arc<Mutex<PeerState>>,
        domain: NativeEndpointDomain,
    }

    impl Default for TaskWirePeer {
        fn default() -> Self {
            Self {
                state: Arc::new(Mutex::new(PeerState::default())),
                domain: NativeEndpointDomain::BackendData,
            }
        }
    }

    impl TaskWirePeer {
        async fn apply_operations(
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
                ApplyAnswer::RejectWithMessage(code, message) => {
                    return Err(Status::new(code, message));
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
                    result: Some(proto::task_operation_receipt::Result::Verdict(
                        proto::TaskOperationVerdict {
                            outcome: encode_operation_outcome(outcome),
                            safe_detail: String::new(),
                            safe_field_path: None,
                            ack,
                        },
                    )),
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

    type StatusStream = ReceiverStream<Result<proto::TaskStatusStreamEvent, Status>>;

    #[tonic::async_trait]
    impl NovaRocksGrpc for TaskWirePeer {
        type ExchangeStream = tokio_stream::Empty<Result<proto::ExchangeResponse, tonic::Status>>;

        async fn exchange(
            &self,
            _request: tonic::Request<tonic::Streaming<proto::ExchangeRequest>>,
        ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "Exchange is retired; use DeliverExchangeUnit",
            ))
        }

        type SubscribeExchangeGrantsStream = std::pin::Pin<
            Box<
                dyn tokio_stream::Stream<Item = Result<proto::ExchangeGrantBatch, tonic::Status>>
                    + Send
                    + 'static,
            >,
        >;

        async fn deliver_exchange_unit(
            &self,
            _request: tonic::Request<proto::DeliverExchangeUnitRequest>,
        ) -> Result<tonic::Response<proto::DeliverExchangeUnitResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "Exchange unit delivery is not installed",
            ))
        }

        async fn register_exchange_demand(
            &self,
            _request: tonic::Request<proto::RegisterExchangeDemandRequest>,
        ) -> Result<tonic::Response<proto::RegisterExchangeDemandResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "Exchange demand registration is not installed",
            ))
        }

        async fn subscribe_exchange_grants(
            &self,
            _request: tonic::Request<proto::SubscribeExchangeGrantsRequest>,
        ) -> Result<tonic::Response<Self::SubscribeExchangeGrantsStream>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "Exchange grant subscription is not installed",
            ))
        }

        type SubscribeTaskStatusStream = StatusStream;

        async fn apply_task_operations(
            &self,
            request: Request<proto::ApplyTaskOperationsRequest>,
        ) -> Result<Response<proto::ApplyTaskOperationsResponse>, Status> {
            if !NativeRpcMethod::ApplyTaskOperations.is_allowed_at(self.domain) {
                return Err(Self::rejected("wrong endpoint domain"));
            }
            self.apply_operations(request).await
        }

        async fn apply_task_control_operations(
            &self,
            request: Request<proto::ApplyTaskControlOperationsRequest>,
        ) -> Result<Response<proto::ApplyTaskOperationsResponse>, Status> {
            if !NativeRpcMethod::ApplyTaskControlOperations.is_allowed_at(self.domain) {
                return Err(Self::rejected("wrong endpoint domain"));
            }
            self.state.lock().expect("peer state").control_requests += 1;
            let operations = request
                .into_inner()
                .operations
                .into_iter()
                .map(|operation| {
                    let body = match operation.control.expect("control body") {
                        proto::task_control_operation::Control::CloseExchangeDestinations(close) => {
                            proto::task_operation::Operation::UpdateTask(proto::UpdateTaskRequest {
                                identity: close.identity,
                                domains: close.destinations.into_iter().map(|destination| proto::TaskDomainUpdate {
                                    domain: Some(proto::task_domain_update::Domain::CloseExchangeDestination(destination)),
                                }).collect(),
                            })
                        }
                        proto::task_control_operation::Control::RenewLease(renew) => {
                            proto::task_operation::Operation::UpdateParticipation(
                                proto::UpdateParticipationRequest {
                                    command: Some(
                                        proto::update_participation_request::Command::RenewLease(
                                            renew,
                                        ),
                                    ),
                                },
                            )
                        }
                        proto::task_control_operation::Control::CancelTask(cancel) => {
                            proto::task_operation::Operation::CancelTask(cancel)
                        }
                        proto::task_control_operation::Control::AbortParticipation(abort) => {
                            proto::task_operation::Operation::AbortParticipation(abort)
                        }
                        proto::task_control_operation::Control::QuiesceParticipation(quiesce) => {
                            proto::task_operation::Operation::QuiesceParticipation(quiesce)
                        }
                        proto::task_control_operation::Control::ReleaseParticipation(release) => {
                            proto::task_operation::Operation::ReleaseParticipation(release)
                        }
                    };
                    proto::TaskOperation {
                        envelope: operation.envelope,
                        operation: Some(body),
                    }
                })
                .collect();
            self.apply_operations(Request::new(proto::ApplyTaskOperationsRequest {
                operations,
            }))
            .await
        }

        async fn subscribe_task_status(
            &self,
            request: Request<proto::SubscribeTaskStatusRequest>,
        ) -> Result<Response<Self::SubscribeTaskStatusStream>, Status> {
            if !NativeRpcMethod::SubscribeTaskStatus.is_allowed_at(self.domain) {
                return Err(Self::rejected("wrong endpoint domain"));
            }
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

        async fn fetch_root_result(
            &self,
            _request: Request<proto::FetchRootResultRequest>,
        ) -> Result<Response<proto::FetchRootResultResponse>, Status> {
            Err(Self::rejected("FetchRootResult"))
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
            request: Request<proto::HeartbeatRequest>,
        ) -> Result<Response<proto::HeartbeatResponse>, Status> {
            if !NativeRpcMethod::Heartbeat.is_allowed_at(self.domain) {
                return Err(Self::rejected("wrong endpoint domain"));
            }
            let descriptor = {
                let mut state = self.state.lock().expect("peer state");
                state.heartbeats.push(request.into_inner());
                state
                    .heartbeat_descriptor
                    .clone()
                    .ok_or_else(|| Self::rejected("missing heartbeat fixture"))?
            };
            Ok(Response::new(proto::HeartbeatResponse {
                num_cores: 2,
                descriptor: Some(
                    novarocks_proto_codec::membership::BackendProcessDescriptor::from_contract(
                        descriptor,
                    )
                    .as_proto()
                    .clone(),
                ),
                reported_state: proto::BackendReportedState::Running as i32,
                admission_epoch_capability: Some(
                    novarocks_task_codec::identity::encode_admission_epoch_capability(
                        novarocks_execution_contract::AdmissionEpochCapability::try_from_bytes(
                            [0x61; 16],
                        )
                        .unwrap(),
                    ),
                ),
            }))
        }
    }

    /// Two independent sockets for one shared process fixture.
    struct Loopback {
        peer: TaskWirePeer,
        endpoint: RuntimeEndpoint,
        control_endpoint: RuntimeEndpoint,
        shutdown: [Option<tokio::sync::oneshot::Sender<()>>; 2],
        served: [tokio::task::JoinHandle<()>; 2],
    }

    impl Loopback {
        async fn serve(
            peer: TaskWirePeer,
        ) -> (
            RuntimeEndpoint,
            tokio::sync::oneshot::Sender<()>,
            tokio::task::JoinHandle<()>,
        ) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind the loopback task peer");
            let address = listener.local_addr().expect("loopback address");
            let incoming = futures::stream::unfold(listener, |listener| async {
                let item = listener.accept().await.map(|(stream, _)| stream);
                Some((item, listener))
            });
            let (shutdown, stop) = tokio::sync::oneshot::channel();
            let served = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(
                        NovaRocksGrpcServer::new(peer)
                            .max_decoding_message_size(128 * 1024 * 1024)
                            .max_encoding_message_size(128 * 1024 * 1024),
                    )
                    .serve_with_incoming_shutdown(incoming, async {
                        let _ = stop.await;
                    })
                    .await
                    .expect("serve the loopback task peer");
            });
            (
                RuntimeEndpoint::new("127.0.0.1", i32::from(address.port()))
                    .expect("valid loopback endpoint"),
                shutdown,
                served,
            )
        }

        async fn start() -> Self {
            let peer = TaskWirePeer::default();
            let (endpoint, data_shutdown, data_served) = Self::serve(peer.clone()).await;
            let mut control_peer = peer.clone();
            control_peer.domain = NativeEndpointDomain::BackendControl;
            let (control_endpoint, control_shutdown, control_served) =
                Self::serve(control_peer).await;
            assert_ne!(endpoint, control_endpoint);
            Self {
                peer,
                endpoint,
                control_endpoint,
                shutdown: [Some(data_shutdown), Some(control_shutdown)],
                served: [data_served, control_served],
            }
        }

        fn descriptor(&self, process: BackendProcessId) -> BackendProcessDescriptor {
            test_descriptor(
                process,
                self.endpoint.clone(),
                self.control_endpoint.clone(),
            )
        }

        async fn settle(&mut self) -> bool {
            for shutdown in &mut self.shutdown {
                if let Some(shutdown) = shutdown.take() {
                    let _ = shutdown.send(());
                }
            }
            let mut joined = true;
            for original in &mut self.served {
                original.abort();
                let result = original.await;
                joined &= result.is_ok() || result.is_err_and(|error| error.is_cancelled());
            }
            joined
        }
    }

    fn test_descriptor(
        process: BackendProcessId,
        data: RuntimeEndpoint,
        control: RuntimeEndpoint,
    ) -> BackendProcessDescriptor {
        BackendProcessDescriptor::try_new(
            process,
            data,
            control,
            "task-wire-test",
            "task-wire-test",
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            4096,
        )
        .expect("independent fixture endpoints")
    }

    impl Drop for Loopback {
        fn drop(&mut self) {
            for shutdown in &mut self.shutdown {
                if let Some(shutdown) = shutdown.take() {
                    let _ = shutdown.send(());
                }
            }
            for served in &self.served {
                served.abort();
            }
        }
    }

    struct SinkFixture {
        loopback: Loopback,
        sink: NativeTaskOperationSink,
        acks: TaskAckIntake,
        // Only this original retains joins; the sink receives a projection.
        runtime: FrontendDataRuntime,
        governed: governed_wire::GovernedWireRole,
    }

    mod governed_wire;
    mod message_limits;

    fn sink_fixture(loopback: Loopback, backend: BackendProcessId) -> SinkFixture {
        sink_fixture_with_budget(loopback, backend, TransportBudget::DEFAULT)
    }

    fn sink_fixture_with_budget(
        loopback: Loopback,
        backend: BackendProcessId,
        budget: TransportBudget,
    ) -> SinkFixture {
        let runtime =
            FrontendDataRuntime::new_with_task_budget(tokio::runtime::Handle::current(), budget);
        let acks = TaskAckIntake::new(Arc::new(CountingWake::default()));
        let governed = governed_wire::GovernedWireRole::new(&loopback);
        let sink = NativeTaskOperationSink::new(
            &[loopback.descriptor(backend)],
            budget,
            test_attempt_facts(),
            acks.handle(),
            runtime.clone(),
            governed.authority(),
        )
        .expect("one frozen backend target");
        SinkFixture {
            loopback,
            sink,
            acks,
            runtime,
            governed,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn missing_one_input_capability_rejects_the_original_before_any_rpc() {
        use crate::task_execution::intent::TaskOperationQueuePermit;
        use novarocks_workload_control::WorkError;

        #[derive(Debug)]
        struct HiddenInput(Box<dyn TaskOperationQueuePermit>);
        impl TaskOperationQueuePermit for HiddenInput {
            fn mark_in_flight(&mut self) {
                self.0.mark_in_flight()
            }
            // Deliberately no typed capability. The actual paid original
            // remains owned by this wrapper through physical retirement.
        }

        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let intents = (1..=3)
            .map(|id| cancel_intent(id, backend))
            .collect::<Vec<_>>();
        assert!(intents.len() > governed_wire::FIXTURE_CHANGES_PER_REQUEST);
        let permits = intents
            .iter()
            .enumerate()
            .map(|(index, intent)| {
                let original = fixture.governed.reserve(&fixture.sink, intent);
                if index == 1 {
                    Box::new(HiddenInput(original)) as Box<dyn TaskOperationQueuePermit>
                } else {
                    original
                }
            })
            .collect();
        let mut batch = released_batch(backend, intents);
        batch.replace_fixture_permits(permits);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        tokio::time::timeout_at(deadline, async {
            loop {
                batch = match fixture.sink.try_submit(batch) {
                    TaskOperationSubmit::Accepted => break,
                    TaskOperationSubmit::Backpressured(original) => original,
                    other => panic!("original source refused before the actual gate: {other:?}"),
                };
                fixture.governed.drive();
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the same finite original source must reach its dispatch gate");

        let received = tokio::time::timeout_at(deadline, async {
            loop {
                if fixture.acks.queued() == 3 {
                    break fixture.acks.drain();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all original operations retain their local typed refusal");
        let clone = received[0].clone();
        for ack in &received {
            assert_eq!(
                ack.dispatch_result(),
                OperationDispatchResult::NonWorkerRejected
            );
            assert_eq!(ack.worker_outcome(), None);
            assert_eq!(ack.governance_failure(), Some(&WorkError::Conflict));
            assert_eq!(
                crate::task_execution::error::dispatch_rejection(ack, ack.kind()),
                crate::task_execution::error::TaskExecutionError::Workload(WorkError::Conflict)
            );
        }
        assert!(std::ptr::eq(
            received[0].governance_failure().unwrap(),
            clone.governance_failure().unwrap()
        ));
        assert!(received[0].shares_original_canonical_with(&clone));
        drop(received);
        tokio::time::timeout_at(deadline, async {
            loop {
                if fixture
                    .sink
                    .data_runtime
                    .task_transport_supervisor()
                    .snapshot()
                    .retained_items
                    == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the actual original F and physical input cleanup must exit");
        assert_eq!(clone.governance_failure(), Some(&WorkError::Conflict));
        assert!(fixture.loopback.peer.applied().is_empty());
        assert_eq!(fixture.loopback.peer.control_requests(), 0);
        drop(clone);

        let SinkFixture {
            mut loopback,
            sink,
            acks,
            mut runtime,
            governed,
        } = fixture;
        drop((sink, acks));
        governed.drive();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let subscriptions = runtime.drain_original_subscriptions_until(deadline).await;
        governed.drive();
        let apply = runtime.drain_original_apply_until(deadline).await;
        governed.drive();
        let outgoing = runtime.drain_native_outgoing_until(deadline).await;
        let peer_exit = loopback.settle().await;
        drop((runtime, loopback));
        let shutdown = governed.finish();
        if subscriptions.is_err()
            || apply.is_err()
            || outgoing.is_err()
            || !peer_exit
            || shutdown.is_err()
        {
            std::panic::panic_any((subscriptions, apply, outgoing, peer_exit, shutdown));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn actual_dispatch_freezes_more_inputs_than_the_original_change_body() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let intents = (1..=3)
            .map(|id| cancel_intent(id, backend))
            .collect::<Vec<_>>();
        assert!(intents.len() > governed_wire::FIXTURE_CHANGES_PER_REQUEST);
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::Outcomes(vec![
            OperationOutcome::ResourceExhausted; 3
        ]));
        submit_batch(&fixture, released_batch(backend, intents));
        let received = settled(&fixture.acks, 3).await;
        for ack in &received {
            assert_eq!(
                ack.worker_outcome(),
                Some(OperationOutcome::ResourceExhausted)
            );
            assert!(ack.governance_failure().is_none());
        }
        assert_eq!(fixture.loopback.peer.control_requests(), 1);
        drop(received);
        let SinkFixture {
            mut loopback,
            sink,
            acks,
            mut runtime,
            governed,
        } = fixture;
        drop((sink, acks));
        governed.drive();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let subscriptions = runtime.drain_original_subscriptions_until(deadline).await;
        governed.drive();
        let apply = runtime.drain_original_apply_until(deadline).await;
        governed.drive();
        let outgoing = runtime.drain_native_outgoing_until(deadline).await;
        let peer_exit = loopback.settle().await;
        drop((runtime, loopback));
        let shutdown = governed.finish();
        if subscriptions.is_err()
            || apply.is_err()
            || outgoing.is_err()
            || !peer_exit
            || shutdown.is_err()
        {
            std::panic::panic_any((subscriptions, apply, outgoing, peer_exit, shutdown));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn canonical_reply_alias_survives_original_send_and_physical_exit() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let intent = cancel_intent(1, backend);
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::Outcomes(vec![
                OperationOutcome::ResourceExhausted,
            ]));
        submit_batch(&fixture, released_batch(backend, vec![intent]));
        let mut acks = settled(&fixture.acks, 1).await;
        let original = acks.pop().unwrap();
        let retained = original.clone();
        assert!(
            original.shares_original_canonical_with(&retained),
            "both values borrow the same original canonical receipt"
        );
        drop(original);
        // The real original task and cleanup execute on the actual fixture
        // runtime; reply aliases are independent of process queue occupancy.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if fixture
                    .sink
                    .data_runtime
                    .task_transport_supervisor()
                    .snapshot()
                    .retained_items
                    == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual original queue/encoding exit");
        assert_eq!(
            retained.worker_outcome(),
            Some(OperationOutcome::ResourceExhausted)
        );
        assert_eq!(fixture.loopback.peer.control_requests(), 1);
        drop(retained);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn frozen_descriptor_routes_ordinary_and_control_runs_to_independent_sockets() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        assert_ne!(fixture.loopback.endpoint, fixture.loopback.control_endpoint);
        let operations = [update_intent(1, backend, 1), cancel_intent(1, backend)]
            .iter()
            .map(|intent| {
                (
                    is_small_control(intent),
                    encode_operation(intent, &test_attempt_facts()).unwrap(),
                )
            })
            .collect();
        let runs = encode_method_batches(operations, TransportBudget::DEFAULT).unwrap();
        assert_eq!(runs.len(), 2);
        for run in runs {
            send_operations(
                &fixture.sink.targets[&backend].client,
                run.request,
                tokio::time::Instant::now() + Duration::from_secs(5),
                &fixture.acks.handle(),
                &[],
            )
            .await
            .expect("the correct socket accepts its method");
        }
        assert_eq!(fixture.loopback.peer.applied().len(), 2);
        assert_eq!(fixture.loopback.peer.control_requests(), 1);

        // Deliberately bypass the frozen client selector to prove each actual
        // peer rejects the other socket's method, before consuming its grammar.
        let mut data = super::super::transport::Client::for_endpoint(
            fixture.loopback.endpoint.native_endpoint().clone(),
            NativeEndpointDomain::BackendData,
            backend,
            fixture.sink.data_runtime.clone(),
        )
        .grpc_with_channel_error(NativeRpcMethod::ApplyTaskOperations)
        .await
        .unwrap();
        let error = data
            .apply_task_control_operations(proto::ApplyTaskControlOperationsRequest {
                operations: Vec::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        let mut control = super::super::transport::Client::for_endpoint(
            fixture.loopback.control_endpoint.native_endpoint().clone(),
            NativeEndpointDomain::BackendControl,
            backend,
            fixture.sink.data_runtime.clone(),
        )
        .grpc_with_channel_error(NativeRpcMethod::ApplyTaskControlOperations)
        .await
        .unwrap();
        let error = control
            .apply_task_operations(proto::ApplyTaskOperationsRequest {
                operations: Vec::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(fixture.loopback.peer.applied().len(), 2);
        assert_eq!(fixture.loopback.peer.control_requests(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn heartbeat_adapter_uses_the_frozen_control_socket_and_preserves_the_whole_descriptor() {
        use novarocks_query_application::api::HeartbeatOutcome;
        let backend = BackendProcessId::new_v7();
        let loopback = Loopback::start().await;
        let descriptor = loopback.descriptor(backend);
        loopback.peer.state.lock().unwrap().heartbeat_descriptor = Some(descriptor.clone());
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let control = loopback.control_endpoint.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            super::super::transport::heartbeat(&runtime, backend, control, Duration::from_secs(5))
        })
        .await
        .unwrap();
        let HeartbeatOutcome::Ok {
            descriptor: observed,
            ..
        } = outcome
        else {
            panic!("control heartbeat must complete");
        };
        assert_eq!(observed, descriptor);
        {
            let state = loopback.peer.state.lock().unwrap();
            assert_eq!(state.heartbeats.len(), 1);
            assert_eq!(
                state.heartbeats[0]
                    .expected_process_id
                    .as_ref()
                    .unwrap()
                    .value,
                backend.to_bytes()
            );
        }
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let mut data = Client::for_endpoint(
            loopback.endpoint.native_endpoint().clone(),
            NativeEndpointDomain::BackendData,
            backend,
            runtime,
        )
        .grpc_with_channel_error(NativeRpcMethod::ApplyTaskOperations)
        .await
        .unwrap();
        assert_eq!(
            data.heartbeat(proto::HeartbeatRequest {
                expected_process_id: None
            })
            .await
            .unwrap_err()
            .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(loopback.peer.state.lock().unwrap().heartbeats.len(), 1);
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

    fn submit_batch(fixture: &SinkFixture, mut batch: DispatchBatch) {
        let permits = batch
            .operations()
            .iter()
            .map(|intent| fixture.governed.reserve(&fixture.sink, intent))
            .collect();
        batch.replace_fixture_permits(permits);
        let batch = match fixture.sink.try_submit(batch) {
            TaskOperationSubmit::Backpressured(batch) => batch,
            result => {
                assert!(matches!(result, TaskOperationSubmit::Accepted));
                return;
            }
        };
        fixture.governed.drive();
        assert!(matches!(
            fixture.sink.try_submit(batch),
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
                    is_small_control(intent),
                    encode_operation(intent, &test_attempt_facts()).expect("encodable intent"),
                )
            })
            .collect();
        let batches = encode_method_batches(operations, TransportBudget::DEFAULT)
            .expect("mixed batch fits transport budget");
        assert_eq!(
            batches.iter().map(|batch| batch.items).collect::<Vec<_>>(),
            vec![1, 1, 1, 1]
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
            EncodedMethodRequest::Control(_)
        ));
        assert!(matches!(
            batches[3].request,
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
                (is_small_control(intent), operation)
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
    async fn adjacent_control_items_with_different_waits_keep_each_submit_deadline() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let intents = [
            update_intent(1, backend, 1),
            cancel_intent(1, backend),
            renew_intent(backend),
        ];
        let operations = intents
            .iter()
            .enumerate()
            .map(|(index, intent)| {
                let mut operation = encode_operation(intent, &test_attempt_facts()).unwrap();
                operation.envelope.as_mut().unwrap().max_wait_millis =
                    if index == 1 { 100 } else { 300_000 };
                (is_small_control(intent), operation)
            })
            .collect();
        let batches = encode_method_batches(operations, TransportBudget::DEFAULT).unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.items).collect::<Vec<_>>(),
            [1, 1, 1]
        );
        let submitted_at = tokio::time::Instant::now() - Duration::from_secs(10);
        let client = &fixture.sink.targets[&backend].client;
        let mut batches = batches.into_iter();
        let first = batches.next().unwrap();
        let deadline = first.expires_at(submitted_at);
        send_operations(client, first.request, deadline, &fixture.acks.handle(), &[])
            .await
            .unwrap();
        let short = batches.next().unwrap();
        let deadline = short.expires_at(submitted_at);
        assert_eq!(
            send_operations(client, short.request, deadline, &fixture.acks.handle(), &[]).await,
            Err(OperationDispatchResult::TransportUnknown)
        );
        assert_eq!(fixture.loopback.peer.control_requests(), 0);
        let long = batches.next().unwrap();
        let deadline = long.expires_at(submitted_at);
        send_operations(client, long.request, deadline, &fixture.acks.handle(), &[])
            .await
            .unwrap();
        assert_eq!(fixture.loopback.peer.control_requests(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_started_is_published_only_when_the_exact_rpc_future_is_polled() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let intent = update_intent(1, backend, 1);
        let operation_id = intent.operation_id();
        let participation = participation(backend);
        let operation = encode_operation(&intent, &test_attempt_facts()).expect("encodable");
        let batch = encode_method_batches(vec![(false, operation)], TransportBudget::DEFAULT)
            .expect("one legal request")
            .pop()
            .expect("one method batch");
        let client = &fixture.sink.targets[&backend].client;
        let intake = fixture.acks.handle();
        let establishes = [(operation_id, participation)];
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
                participation: observed_participation,
            }] if *observed == operation_id && *observed_participation == participation
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
    async fn submitting_after_original_role_drain_rejects_the_original_batch_without_a_send() {
        let backend = BackendProcessId::new_v7();
        let mut fixture = sink_fixture(Loopback::start().await, backend);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build the original role runtime");
        let mut original_role = FrontendDataRuntime::new(runtime.handle().clone());
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let original_drain = original_role.drain_original_apply_until(deadline).await;
        let original_joined = original_role.original_apply_joined();
        // The actual original task owner is drained while its runtime is alive.
        tokio::task::block_in_place(|| runtime.shutdown_timeout(Duration::from_secs(2)));
        let mut sink = NativeTaskOperationSink::new(
            &[fixture.loopback.descriptor(backend)],
            TransportBudget::DEFAULT,
            test_attempt_facts(),
            fixture.acks.handle(),
            original_role,
            fixture.governed.authority(),
        )
        .expect("one frozen backend target after original role drain");
        // Keep the actual wire client on the live test runtime. Only the
        // original role has closed. Rejection must precede any wire task.
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
            .map(|intent| fixture.governed.reserve(&sink, intent))
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

        // A closed original role rejects BEFORE constructing F. The caller
        // keeps the exact batch for the existing definitely-unsent rollback.
        let (rejected, returned_ids, reason, held_returned_items) = match sink.try_submit(batch) {
            TaskOperationSubmit::Rejected { batch, reason } => {
                let ids = batch
                    .operations()
                    .iter()
                    .map(OperationIntent::operation_id)
                    .collect::<Vec<_>>();
                let held = supervisor.snapshot().retained_items;
                drop(batch);
                (true, ids, reason, held)
            }
            TaskOperationSubmit::Backpressured(batch) => {
                drop(batch);
                (false, Vec::new(), String::new(), 0)
            }
            TaskOperationSubmit::Accepted => (false, Vec::new(), String::new(), 0),
            TaskOperationSubmit::AdmissionFailed { batch, error } => {
                drop(batch);
                panic!("original source must admit before testing closed role: {error}")
            }
        };
        let acknowledgements = fixture.acks.drain_events();
        let returned = supervisor.snapshot();
        let applied = fixture.loopback.peer.applied();
        let control_requests = fixture.loopback.peer.control_requests();
        // Join the actual fixture servers before checking the collected result.
        // Forced server cancellation is test cleanup, not role drain evidence.
        for served in &fixture.loopback.served {
            served.abort();
        }
        let mut server_cleanup = true;
        for served in &mut fixture.loopback.served {
            match tokio::time::timeout_at(deadline.into(), served).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if error.is_cancelled() => {}
                _ => server_cleanup = false,
            }
        }

        assert!(original_drain.is_ok(), "{original_drain:?}");
        assert!(original_joined);
        assert!(rejected);
        assert!(reason.contains("closed"));
        assert_eq!(returned_ids, expected_ids);
        assert_eq!(held_returned_items, before.retained_items + 2);
        assert!(
            acknowledgements.is_empty(),
            "rejection cannot manufacture an ACK"
        );
        assert_eq!(
            returned, before,
            "caller returned its original reservations"
        );
        assert!(applied.is_empty());
        assert_eq!(control_requests, 0);
        assert!(server_cleanup);
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
                establish_participation: None,
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
        let transport = TransportBudget::new(
            2,
            1024,
            novarocks_task_codec::TransportMessageLimits::DEFAULT,
            4,
            256 * 1024 * 1024,
            4,
            512 * 1024 * 1024,
            1,
            2,
            Duration::from_secs(1),
        )
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
                crate::task_execution::intent::NativeRequestWindows::for_transport(transport)
                    .unwrap()
                    .empty_queue_capacity(false),
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

        submit_batch(&fixture, batch);
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

        submit_batch(&fixture, batch);
        let acks = settled(&fixture.acks, 2).await;

        for ack in &acks {
            assert_eq!(
                ack.dispatch_result(),
                OperationDispatchResult::NonWorkerRejected
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

        submit_batch(&fixture, batch);
        let acks = settled(&fixture.acks, 2).await;

        assert!(
            acks.iter()
                .all(|ack| ack.dispatch_result() == OperationDispatchResult::NonWorkerRejected),
            "a response with fewer receipts than items cannot be attributed"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_reply_preflight_failure_preserves_remote_effect_without_worker_verdict_or_replay()
     {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let batch = released_batch(backend, vec![cancel_intent(1, backend)]);
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::WithAck(vec![(
                OperationOutcome::Accepted,
                Some(proto::task_operation_verdict::Ack::CreateTask(
                    proto::CreateTaskAck {
                        accepted_domains: vec![
                            proto::TaskDomainReceipt::default();
                            novarocks_task_codec::domain::MAX_DOMAIN_UPDATES + 1
                        ],
                        ..Default::default()
                    },
                )),
            )]));
        submit_batch(&fixture, batch);
        let acks = settled(&fixture.acks, 1).await;
        assert_eq!(
            fixture.loopback.peer.applied().len(),
            1,
            "the remote effect was dispatched once before its reply was refused locally"
        );
        assert_eq!(
            acks[0].dispatch_result(),
            OperationDispatchResult::NonWorkerRejected
        );
        assert_eq!(
            acks[0].worker_outcome(),
            None,
            "a local decoder never mints a Worker verdict"
        );
        assert_eq!(
            crate::task_execution::operation::frontend_action(acks[0].dispatch_result()),
            crate::task_execution::operation::FrontendAction::FailAttempt
        );
        assert_eq!(*acks[0].payload(), AckPayload::None);
    }

    struct GoAwayWireProof {
        stream_id: u32,
        last_stream_id: u32,
        reason: u32,
        body: Vec<u8>,
    }

    // This is a real HTTP/2 wire peer, not a manufactured h2::Error. The
    // request is received but never handed to a Task application before the
    // real GOAWAY declares last_stream_id=0. The client library owns parsing
    // that frame and constructing its original tonic Status/source chain.
    async fn receive_then_goaway(
        socket: &mut tokio::net::TcpStream,
        reason: h2::Reason,
    ) -> anyhow::Result<GoAwayWireProof> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut preface = [0; 24];
        socket.read_exact(&mut preface).await?;
        anyhow::ensure!(
            &preface == b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n",
            "actual client preface differs"
        );
        socket.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await?;
        let mut stream_id = None;
        let mut body = Vec::new();
        loop {
            let mut header = [0; 9];
            socket.read_exact(&mut header).await?;
            let len = (usize::from(header[0]) << 16)
                | (usize::from(header[1]) << 8)
                | usize::from(header[2]);
            // Empty peer SETTINGS leave the protocol's default frame size.
            anyhow::ensure!(len <= 16_384, "client exceeded default HTTP/2 frame size");
            let id = u32::from_be_bytes(header[5..9].try_into()?) & 0x7fff_ffff;
            let mut payload = vec![0; len];
            socket.read_exact(&mut payload).await?;
            match header[3] {
                4 => {
                    anyhow::ensure!(id == 0, "SETTINGS addressed an application stream");
                    if header[4] & 1 == 0 {
                        socket.write_all(&[0, 0, 0, 4, 1, 0, 0, 0, 0]).await?;
                    }
                }
                6 => {
                    anyhow::ensure!(id == 0 && len == 8, "invalid client PING");
                    if header[4] & 1 == 0 {
                        socket.write_all(&[0, 0, 8, 6, 1, 0, 0, 0, 0]).await?;
                        socket.write_all(&payload).await?;
                    }
                }
                1 => {
                    anyhow::ensure!(id > 0 && id & 1 == 1, "invalid client request stream");
                    anyhow::ensure!(
                        stream_id.is_none_or(|original| original == id),
                        "a second request preceded the controlled GOAWAY"
                    );
                    stream_id = Some(id);
                    anyhow::ensure!(
                        header[4] & 1 == 0,
                        "unary request ended without its real message"
                    );
                }
                0 => {
                    anyhow::ensure!(
                        stream_id == Some(id),
                        "DATA differs from original request stream"
                    );
                    let bytes = if header[4] & 8 != 0 {
                        let padding = usize::from(
                            *payload
                                .first()
                                .ok_or_else(|| anyhow::anyhow!("missing DATA padding length"))?,
                        );
                        anyhow::ensure!(padding < len, "invalid DATA padding");
                        &payload[1..len - padding]
                    } else {
                        payload.as_slice()
                    };
                    body.extend_from_slice(bytes);
                    anyhow::ensure!(body.len() <= novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES + 5, "test request exceeded its original codec bound");
                    if header[4] & 1 != 0 {
                        break;
                    }
                }
                2 | 8 | 9 => {}
                kind => anyhow::bail!("unexpected client frame before GOAWAY: {kind}"),
            }
        }
        anyhow::ensure!(
            body.len() >= 5 && body[0] == 0,
            "missing uncompressed unary message"
        );
        anyhow::ensure!(
            u32::from_be_bytes(body[1..5].try_into()?) as usize == body.len() - 5,
            "unary message length differs"
        );
        let _request = <proto::ApplyTaskOperationsRequest as prost::Message>::decode(&body[5..])?;
        let last_stream_id = 0u32;
        let mut frame = vec![0, 0, 8, 7, 0, 0, 0, 0, 0];
        frame.extend_from_slice(&last_stream_id.to_be_bytes());
        frame.extend_from_slice(&u32::from(reason).to_be_bytes());
        socket.write_all(&frame).await?;
        socket.flush().await?;
        Ok(GoAwayWireProof {
            stream_id: stream_id.ok_or_else(|| anyhow::anyhow!("no original request stream"))?,
            last_stream_id,
            reason: u32::from(reason),
            body,
        })
    }

    async fn goaway_loopback(
        reason: h2::Reason,
    ) -> anyhow::Result<(Loopback, tokio::sync::oneshot::Receiver<GoAwayWireProof>)> {
        let peer = TaskWirePeer::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let endpoint = RuntimeEndpoint::new("127.0.0.1", i32::from(address.port()))
            .map_err(anyhow::Error::msg)?;
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let (observed, proof) = tokio::sync::oneshot::channel();
        let healthy = peer.clone();
        let served = tokio::spawn(async move {
            let (mut original_socket, _) = listener
                .accept()
                .await
                .expect("accept original Native connection");
            let original = receive_then_goaway(&mut original_socket, reason)
                .await
                .expect("send real controlled GOAWAY");
            let _ = observed.send(original);
            // A transparent Hyper reconnect, or an allowed exact replay, uses
            // this same endpoint and the existing real Tonic receipt author.
            let incoming = futures::stream::unfold(listener, |listener| async {
                let accepted = listener.accept().await.map(|(socket, _)| socket);
                Some((accepted, listener))
            });
            tonic::transport::Server::builder()
                .add_service(NovaRocksGrpcServer::new(healthy))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stopped.await;
                })
                .await
                .expect("serve subsequent original Native connection");
            // Keep the first socket alive until the original owner stops; an
            // injected EOF must not replace the real GOAWAY source evidence.
            drop(original_socket);
        });
        let mut control_peer = peer.clone();
        control_peer.domain = NativeEndpointDomain::BackendControl;
        let (control_endpoint, control_shutdown, control_served) =
            Loopback::serve(control_peer).await;
        Ok((
            Loopback {
                peer,
                endpoint,
                control_endpoint,
                shutdown: [Some(shutdown), Some(control_shutdown)],
                served: [served, control_served],
            },
            proof,
        ))
    }

    async fn real_goaway_conformance_case(reason: h2::Reason) -> anyhow::Result<()> {
        let backend = BackendProcessId::new_v7();
        // Dynamic-filter updates select ApplyTaskOperations/Submission. The
        // old cancellation fixture would exercise the separate Control lane.
        let intent = update_intent(1, backend, 1);
        let wire = proto::ApplyTaskOperationsRequest {
            operations: vec![
                encode_operation(&intent, &test_attempt_facts()).map_err(anyhow::Error::msg)?,
            ],
        };
        let encoded = prost::Message::encode_to_vec(&wire);
        let mut expected = vec![0];
        expected.extend_from_slice(&u32::try_from(encoded.len())?.to_be_bytes());
        expected.extend_from_slice(&encoded);
        let (loopback, proof) = goaway_loopback(reason).await?;
        let mut fixture = sink_fixture(loopback, backend);
        let client = &fixture.sink.targets[&backend].client;
        let operation = tokio::time::timeout(Duration::from_secs(5), async {
            let method = NativeRpcMethod::ApplyTaskOperations;
            let (mut grpc, acquired) = client.operation_test_grpc_with_channel_identity(method).await.map_err(|error| anyhow::anyhow!("{error}"))?;
            let response = fixture_apply_request(&mut grpc, Request::new(wire.clone()), method, wire.operations.len(), client.method_message_limits(method).1).await;
            let proof = proof.await?;
            anyhow::ensure!(proof.stream_id > proof.last_stream_id && proof.last_stream_id == 0, "actual stream did not exceed actual GOAWAY last id");
            anyhow::ensure!(proof.reason == u32::from(reason) && proof.body == expected, "actual frame or original request bytes differ");
            let mut receipt = None;
            let observation = match response {
                Ok(reply) => {
                    receipt = Some(reply.into_inner());
                    (None, None, None)
                }
                Err(status) => {
                    let code = status.code();
                    let observed = task_operation_h2_source(&status);
                    let classified = classify_apply_status(&status);
                    eprintln!("REAL_GOAWAY_CONFORMANCE reason={} stream={} last={} code={:?} h2_source={:?} classified={:?}", proof.reason, proof.stream_id, proof.last_stream_id, code, observed, classified);
                    drop(status);
                    if reason == h2::Reason::NO_ERROR && classified == OperationDispatchResult::TransportUnknown {
                        client.invalidate_channel_if_current(method, &acquired);
                        let (mut grpc, _) = client.operation_test_grpc_with_channel_identity(method).await.map_err(|error| anyhow::anyhow!("{error}"))?;
                        receipt = Some(fixture_apply_request(&mut grpc, Request::new(wire.clone()), method, wire.operations.len(), client.method_message_limits(method).1).await?.into_inner());
                    }
                    (Some(code), observed, Some(classified))
                }
            };
            if let Some(reply) = receipt {
                let headers = decode_receipt_batch(&reply, &[intent.operation_id()], FieldPath::root("real_goaway_receipt"))?;
                anyhow::ensure!(headers.len() == 1 && headers[0].outcome() == OperationOutcome::Accepted, "original wire receipt did not accept the same operation");
                let applied = fixture.loopback.peer.applied();
                anyhow::ensure!(applied.len() == 1 && applied[0] == wire, "reconnect/replay changed the immutable request");
                eprintln!("REAL_GOAWAY_CONFORMANCE actual_wire_receipt=true transparent_recovery={}", observation.0.is_none());
            }
            Ok::<_, anyhow::Error>(observation)
        }).await;
        let joined = fixture.loopback.settle().await;
        anyhow::ensure!(joined, "original GOAWAY peer handles did not join");
        let (code, observed, classified) = operation
            .map_err(|_| anyhow::anyhow!("real GOAWAY conformance five-second watchdog"))??;
        match code {
            Some(tonic::Code::Internal) => {
                let observed = observed.ok_or_else(|| {
                    anyhow::anyhow!(
                        "actual Internal status did not retain typed H2; no typed classifier proof"
                    )
                })?;
                anyhow::ensure!(
                    observed.reason == Some(u32::from(reason))
                        && observed.is_remote
                        && observed.is_go_away
                        && !observed.is_reset,
                    "actual Internal source differs from controlled remote GOAWAY"
                );
                assert_eq!(
                    classified,
                    Some(if reason == h2::Reason::NO_ERROR {
                        OperationDispatchResult::TransportUnknown
                    } else {
                        OperationDispatchResult::NonWorkerRejected
                    }),
                    "classify the real typed transport source, never its message"
                );
            }
            Some(code) => {
                eprintln!(
                    "REAL_GOAWAY_CONFORMANCE counterevidence=no_Internal actual_code={code:?} h2_source={observed:?} classified={classified:?}"
                );
                anyhow::ensure!(
                    classified == Some(OperationDispatchResult::TransportUnknown),
                    "non-Internal outcome must retain its existing classifier contract"
                );
            }
            None => {
                eprintln!(
                    "REAL_GOAWAY_CONFORMANCE counterevidence=transparent_success no_Internal_or_typed_failure"
                );
                anyhow::ensure!(
                    reason == h2::Reason::NO_ERROR,
                    "protocol error transparently recovered without exposing an error"
                );
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn real_network_no_error_goaway_observes_actual_status_and_retry_contract()
    -> anyhow::Result<()> {
        real_goaway_conformance_case(h2::Reason::NO_ERROR).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn real_network_protocol_error_goaway_keeps_typed_internal_rejected() -> anyhow::Result<()>
    {
        real_goaway_conformance_case(h2::Reason::PROTOCOL_ERROR).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn real_network_remote_internal_same_literal_has_no_h2_source_and_is_rejected()
    -> anyhow::Result<()> {
        let backend = BackendProcessId::new_v7();
        let mut fixture = sink_fixture(Loopback::start().await, backend);
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::RejectWithMessage(
                tonic::Code::Internal,
                "h2 protocol error: http2 error",
            ));
        let request = proto::ApplyTaskOperationsRequest {
            operations: vec![
                encode_operation(&update_intent(1, backend, 1), &test_attempt_facts())
                    .map_err(anyhow::Error::msg)?,
            ],
        };
        let operation = tokio::time::timeout(Duration::from_secs(5), async {
            let client = &fixture.sink.targets[&backend].client;
            let (mut grpc, _) = client
                .grpc_with_channel_identity(NativeRpcMethod::ApplyTaskOperations)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            let result = grpc.apply_task_operations(Request::new(request)).await;
            match result {
                Err(status) => Ok::<_, anyhow::Error>((
                    status.code(),
                    status.message().to_owned(),
                    task_operation_h2_source(&status),
                    classify_apply_status(&status),
                )),
                Ok(_) => anyhow::bail!("remote Internal unexpectedly returned a receipt"),
            }
        })
        .await;
        let joined = fixture.loopback.settle().await;
        anyhow::ensure!(joined, "original remote Internal peer handles did not join");
        let (code, message, source, classified) =
            operation.map_err(|_| anyhow::anyhow!("remote Internal five-second watchdog"))??;
        assert_eq!(code, tonic::Code::Internal);
        assert_eq!(message, "h2 protocol error: http2 error");
        assert!(
            source.is_none(),
            "remote wire status has no local typed H2 cause"
        );
        assert_eq!(classified, OperationDispatchResult::NonWorkerRejected);
        Ok(())
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

        submit_batch(&fixture, released_batch(backend, vec![intent]));
        let first = settled(&fixture.acks, 1).await;
        assert_eq!(
            first[0].dispatch_result(),
            OperationDispatchResult::TransportUnknown
        );

        // The owner replays the same immutable intent, so the same bytes go
        // back out under the same operation id.
        submit_batch(&fixture, batch);
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

        submit_batch(&fixture, batch);
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

        submit_batch(&fixture, batch);
        let acks = settled(&fixture.acks, 1).await;

        assert_eq!(acks[0].worker_outcome(), None);
        assert_eq!(
            acks[0].dispatch_result(),
            OperationDispatchResult::NonWorkerRejected
        );
        let original = acks[0]
            .local_failure()
            .expect("original local identity fact");
        assert_eq!(
            original.fact().reason(),
            novarocks_query_application::execution_error::TASK_PROTOCOL_IDENTITY_MISMATCH_REASON
        );
        assert_eq!(
            crate::task_execution::error::dispatch_rejection(&acks[0], acks[0].kind())
                .preparation_input(),
            *original
        );
        assert_eq!(acks[0].clone().local_failure(), Some(original));
        assert!(
            fixture.loopback.peer.applied().is_empty(),
            "a replaced process is never retried elsewhere"
        );

        // The same outcome arriving through an actual validated response
        // retains its Worker branch; local refusal does not widen that branch.
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::Outcomes(vec![
                OperationOutcome::IdentityMismatch,
            ]));
        submit_batch(
            &fixture,
            released_batch(frozen, vec![cancel_intent(2, frozen)]),
        );
        let remote = settled(&fixture.acks, 1).await;
        assert_eq!(
            remote[0].worker_outcome(),
            Some(OperationOutcome::IdentityMismatch)
        );
        assert!(remote[0].local_failure().is_none());
        assert_eq!(fixture.loopback.peer.applied().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_observation_read_is_refused_before_the_wire() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        let supervisor = fixture.sink.data_runtime.task_transport_supervisor();
        let before = supervisor.snapshot();
        let intent = OperationIntent::FetchTaskDynamicFilters(FetchTaskDynamicFilters::new(
            TaskOperationId::new_v7(),
            identity(1, backend),
            None,
        ));
        let mut preparation = crate::task_execution::intent::TaskOperationQueuePreparation::new();
        assert!(matches!(
            fixture
                .sink
                .try_reserve_queue(intent.queue_request(), &mut preparation),
            TaskOperationQueueAdmission::Rejected(novarocks_workload_control::WorkError::Conflict)
        ));
        assert_eq!(
            supervisor.snapshot(),
            before,
            "a non-operation has no batch encoding and cannot acquire a queue reservation"
        );
        assert!(fixture.acks.drain_events().is_empty());
        assert!(fixture.loopback.peer.applied().is_empty());
        assert_eq!(fixture.loopback.peer.control_requests(), 0);
        drop(preparation);
        message_limits::close(fixture).await;
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
                crate::task_execution::operation::IngressRejection::UnclassifiedCapacity
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
                crate::task_execution::operation::IngressRejection::WaitingCapacity
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
                crate::task_execution::operation::IngressRejection::BodyLimit
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

    struct ParticipationAwareSubscriberFixture {
        loopback: Loopback,
        subscriber: TaskStatusSubscriber,
        status_intake: StatusIntake,
        convergence_intake: ParticipationConvergenceIntake,
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
            &[loopback.descriptor(backend)],
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

    fn participation_aware_subscriber_fixture(
        loopback: Loopback,
        backend: BackendProcessId,
        error_budget: u32,
        convergence_capacity: NonZeroUsize,
    ) -> ParticipationAwareSubscriberFixture {
        let data_runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let wake = Arc::new(CountingWake::default());
        let status_intake = StatusIntake::new(16, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
        let convergence_intake = ParticipationConvergenceIntake::bounded(convergence_capacity);
        let subscriber = TaskStatusSubscriber::new_participation_aware(
            &[loopback.descriptor(backend)],
            status_intake.handle(),
            convergence_intake.handle(),
            error_budget,
            data_runtime,
        )
        .expect("one frozen participation-aware backend target");
        ParticipationAwareSubscriberFixture {
            loopback,
            subscriber,
            status_intake,
            convergence_intake,
        }
    }

    fn convergence_receipt(participation: ParticipationRef) -> ParticipationConvergenceReceipt {
        ParticipationConvergenceReceipt::new(
            participation,
            ParticipationConvergenceVersion::FIRST,
            ParticipationConvergenceState::WorkerStoppedAndParticipationFenced,
        )
    }

    /// Real covered HTTP/2 observation must survive logical terminal cleanup.
    /// This scripted peer proves FE ownership/cursor behavior; the Worker Work
    /// gate and actual exit author are covered by the native system scenario.
    #[tokio::test]
    async fn covered_terminal_cleanup_keeps_original_stream_until_actual_participation_receipt() {
        let (mut round, _) = crate::task_execution::tests::manifest_terminal_regression_fixture();
        let participation = *round.execution().graph().participations().next().unwrap();
        let mut loopback = Loopback::start().await;
        let frame = |fact| {
            encode_covered_status_event(&CoveredStatusStreamEvent {
                fact,
                source_revision: None,
            })
            .unwrap()
        };
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![frame(
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: 1,
                    initial_cut: 0,
                }),
            )]));
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(Vec::new()));
        let wake = Arc::new(CountingWake::default());
        let intake = Arc::new(ObservationIntake::new(8, 16, 16 * 4096, 4096, wake).unwrap());
        let role = CoveredRoleFixture::new();
        let subscriber = Arc::new(
            CoveredTaskStatusSubscriber::new(
                &[loopback.descriptor(participation.backend_process_id())],
                Arc::clone(&intake),
                2,
                role.runtime.clone(),
                role.scope(),
            )
            .unwrap(),
        );
        round.install_covered_observation(Arc::clone(&subscriber), Arc::clone(&intake));
        round.turn().unwrap();
        subscribe_requests(&loopback.peer, 1).await;
        round
            .execution_mut()
            .observe_actor_abort_participation_closed(participation)
            .unwrap();
        round.begin_terminal_cleanup();
        round.turn().unwrap();
        assert!(
            subscriber.state(participation).is_some(),
            "closing ACK cannot abort residual observation"
        );
        assert!(
            round
                .execution()
                .participation_convergence_receipt(participation)
                .is_none()
        );
        loopback.peer.close_held_subscriptions();
        let requests = subscribe_requests(&loopback.peer, 2).await;
        let request = decode_covered_subscribe_task_status(
            &requests[1],
            FieldPath::root("cleanup_reconnect"),
        )
        .unwrap();
        assert_eq!(request.participation, participation);
        assert!(
            request.participation_cursor.is_none(),
            "no unobserved proof is fabricated"
        );
        let receipt = convergence_receipt(participation);
        loopback
            .peer
            .send_held_event(frame(CoveredStatusStreamFact::ParticipationConvergence(
                receipt,
            )))
            .await;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                round.turn().unwrap();
                if round
                    .execution()
                    .participation_convergence_receipt(participation)
                    == Some(receipt)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the original cleanup owner applies the late proof");
        assert_eq!(
            subscriber.state(participation),
            None,
            "only the applied proof retires this stream"
        );
        let replay = round
            .execution()
            .covered_subscription_request(participation, NonZeroU64::new(3).unwrap())
            .unwrap();
        assert_eq!(
            replay.participation_cursor.unwrap().current_version(),
            Some(receipt.version())
        );
        role.settle().await;
        assert!(loopback.settle().await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_stream_preserves_every_frame_and_reconnects_from_applied_cursors() {
        let backend = BackendProcessId::new_v7();
        let mut loopback = Loopback::start().await;
        let participation = participation(backend);
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
                fact: CoveredStatusStreamFact::ParticipationConvergence(convergence_receipt(
                    participation,
                )),
                source_revision: Some(3),
            },
            CoveredStatusStreamEvent {
                fact: CoveredStatusStreamFact::Quiesce(QuiesceParticipationReceipt::new(
                    participation,
                    1,
                    vec![task],
                    ParticipationState::Quiescing,
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
        let subscription_role = CoveredRoleFixture::new();
        let subscriber = CoveredTaskStatusSubscriber::new(
            &[loopback.descriptor(backend)],
            Arc::clone(&intake),
            3,
            subscription_role.runtime.clone(),
            subscription_role.scope(),
        )
        .expect("covered transport");
        subscriber
            .ensure(DecodedCoveredSubscription {
                participation,
                generation: NonZeroU64::new(7).unwrap(),
                status_cursors: vec![TaskStatusCursor::at(task, TaskStatusVersion::FIRST)],
                task_convergence_cursors: vec![TaskConvergenceCursor::at(
                    task,
                    TaskConvergenceVersion::FIRST,
                )],
                participation_cursor: Some(ParticipationConvergenceCursor::unobserved(
                    participation,
                )),
                quiesce_cursor: Some(QuiesceObservationCursor {
                    participation,
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
                ObservationIntakeEntry::Frame(frame)
                    if matches!(
                        frame.as_ref(),
                        ObservationFrame::Covered {
                            participation: observed_participation,
                            generation: 7,
                            event,
                        } if *observed_participation == participation && event == &expected
                    )
            ));
        }
        subscriber.stop(participation);
        subscription_role.settle().await;
        assert!(loopback.settle().await);
    }

    /// Real HTTP/2 transport plus production FE serial owners. The peer is a
    /// scripted covered source, so this does not claim a full BE system gate.
    #[tokio::test(flavor = "multi_thread")]
    async fn covered_loopback_backpressure_recovery_quiesce_settles_actual_root_seal() {
        use crate::task_execution::round::TaskRound;
        use novarocks_coordination_contract::MonotonicClock;
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

        let (mut round, clock, participation, missing) =
            crate::task_execution::tests::covered_loopback_recovery_round();
        let mut loopback = Loopback::start().await;
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
                Arc::clone(&clock) as Arc<dyn MonotonicClock>,
            )
            .unwrap(),
        );
        let subscription_role = CoveredRoleFixture::new();
        let subscriber = Arc::new(
            CoveredTaskStatusSubscriber::new(
                &[loopback.descriptor(participation.backend_process_id())],
                Arc::clone(&intake),
                2,
                subscription_role.runtime.clone(),
                subscription_role.scope(),
            )
            .unwrap(),
        );
        round.install_covered_observation(Arc::clone(&subscriber), Arc::clone(&intake));
        let source = round.take_root_status_source().unwrap();
        subscriber
            .ensure(
                round
                    .execution()
                    .covered_subscription_request(participation, NonZeroU64::new(2).unwrap())
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
        assert_eq!(
            subscriber.state(participation),
            Some(SubscriptionState::Live)
        );
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
                .status_cursors(participation)
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
            .filter(|task| task.participation() == participation)
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
                CoveredStatusStreamFact::Quiesce(QuiesceParticipationReceipt::new(
                    participation,
                    1,
                    accepted.clone(),
                    ParticipationState::Quiescing,
                )),
                None,
            ))
            .await;
        drive_until(&mut round, |round| {
            round
                .execution()
                .covered_subscription_request(participation, NonZeroU64::new(3).unwrap())
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
            .covered_subscription_request(participation, NonZeroU64::new(3).unwrap())
            .unwrap();
        assert_eq!(
            applied.quiesce_cursor.unwrap().fence_version.unwrap().get(),
            1
        );
        assert_eq!(
            round.execution().owner(participation).unwrap().state(),
            ParticipationState::Quiescing
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
        subscriber.stop(participation);
        subscription_role.settle().await;
        assert!(loopback.settle().await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_unchanged_requires_a_version_in_the_physical_request() {
        for convergence in [false, true] {
            let backend = BackendProcessId::new_v7();
            let mut loopback = Loopback::start().await;
            let participation = participation(backend);
            let task = identity(1, backend);
            loopback
                .peer
                .expect_subscribe(SubscribeAnswer::EventsThenHold(Vec::new()));
            let intake = Arc::new(
                ObservationIntake::new(2, 4, 4 * 4096, 4096, Arc::new(CountingWake::default()))
                    .unwrap(),
            );
            let subscription_role = CoveredRoleFixture::new();
            let subscriber = CoveredTaskStatusSubscriber::new(
                &[loopback.descriptor(backend)],
                Arc::clone(&intake),
                2,
                subscription_role.runtime.clone(),
                subscription_role.scope(),
            )
            .unwrap();
            let mut request = DecodedCoveredSubscription {
                participation,
                generation: NonZeroU64::new(1).unwrap(),
                status_cursors: vec![TaskStatusCursor::unobserved(task)],
                task_convergence_cursors: vec![TaskConvergenceCursor::unobserved(task)],
                participation_cursor: None,
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
                while subscriber.state(participation) != Some(SubscriptionState::Rejected) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("Unchanged cannot borrow a later applied cursor");
            let mut runner = intake.try_enter().unwrap();
            assert!(runner.drain_ordered(1).is_empty());
            subscriber.stop(participation);
            subscription_role.settle().await;
            assert!(loopback.settle().await);
        }
    }

    #[test]
    fn covered_unchanged_requires_the_exact_requested_identity() {
        let backend = BackendProcessId::new_v7();
        let participation = participation(backend);
        let requested = identity(1, backend);
        let other = identity(2, backend);
        let request = DecodedCoveredSubscription {
            participation,
            generation: NonZeroU64::new(3).unwrap(),
            status_cursors: vec![TaskStatusCursor::at(requested, TaskStatusVersion::FIRST)],
            task_convergence_cursors: vec![TaskConvergenceCursor::at(
                requested,
                TaskConvergenceVersion::FIRST,
            )],
            participation_cursor: None,
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
                validate_covered_event_participation(
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
        use crate::task_execution::error::{ParticipantObservationFailure, TaskExecutionError};
        use novarocks_coordination_contract::MonotonicClock;

        let (mut round, clock, participation, task) =
            crate::task_execution::tests::covered_loopback_recovery_round();
        let foreign = TaskIdentity::new(
            task.query_execution_id(),
            task.stage_id(),
            task.task_id(),
            BackendProcessId::new_v7(),
        );
        let mut loopback = Loopback::start().await;
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
                clock as Arc<dyn MonotonicClock>,
            )
            .unwrap(),
        );
        let subscription_role = CoveredRoleFixture::new();
        let subscriber = Arc::new(
            CoveredTaskStatusSubscriber::new(
                &[loopback.descriptor(participation.backend_process_id())],
                Arc::clone(&intake),
                2,
                subscription_role.runtime.clone(),
                subscription_role.scope(),
            )
            .unwrap(),
        );
        round.install_covered_observation(Arc::clone(&subscriber), Arc::clone(&intake));
        subscriber
            .ensure(
                round
                    .execution()
                    .covered_subscription_request(participation, NonZeroU64::new(2).unwrap())
                    .unwrap(),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while subscriber.state(participation) != Some(SubscriptionState::ProcessMismatch) {
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
        } if backend == participation.backend_process_id()),
            "{error:?}"
        );
        assert_eq!(
            loopback.peer.subscribed().len(),
            1,
            "identity violations cannot reopen the source as transport recovery"
        );
        subscriber.stop(participation);
        subscription_role.settle().await;
        assert!(loopback.settle().await);
    }

    fn covered_validation_request(
        participation: ParticipationRef,
        task: TaskIdentity,
    ) -> DecodedCoveredSubscription {
        DecodedCoveredSubscription {
            participation,
            generation: NonZeroU64::new(3).unwrap(),
            status_cursors: vec![TaskStatusCursor::at(task, TaskStatusVersion::FIRST)],
            task_convergence_cursors: vec![TaskConvergenceCursor::at(
                task,
                TaskConvergenceVersion::FIRST,
            )],
            participation_cursor: None,
            quiesce_cursor: None,
            required_identities: vec![task],
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actual_covered_rpc_abort_keeps_original_publisher_until_future_destruction_exits() {
        use std::sync::{
            Condvar,
            atomic::{AtomicBool, Ordering},
        };
        use std::task::Poll;
        struct Hold {
            entered: tokio::sync::Notify,
            release: Mutex<bool>,
            changed: Condvar,
            exited: AtomicBool,
        }
        struct HeldCoveredFuture {
            inner: Pin<Box<dyn Future<Output = ()> + Send>>,
            hold: Arc<Hold>,
        }
        impl Future for HeldCoveredFuture {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
                self.inner.as_mut().poll(cx)
            }
        }
        impl Drop for HeldCoveredFuture {
            fn drop(&mut self) {
                self.hold.entered.notify_one();
                let mut release = self.hold.release.lock().unwrap();
                while !*release {
                    release = self.hold.changed.wait(release).unwrap();
                }
                self.hold.exited.store(true, Ordering::Release);
                // The original Covered future/body fields drop only after this
                // genuine destructor hold, on the protected original callback.
            }
        }
        let mut loopback = Loopback::start().await;
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(Vec::new()));
        let mut role = CoveredRoleFixture::new();
        let backend = BackendProcessId::new_v7();
        let request = covered_validation_request(participation(backend), identity(1, backend));
        let intake = Arc::new(
            ObservationIntake::new(1, 3, 3 * 4096, 4096, Arc::new(CountingWake::default()))
                .unwrap(),
        );
        let others = [intake.subscribe().unwrap(), intake.subscribe().unwrap()];
        let publisher = Arc::new(intake.subscribe().unwrap());
        let state = Arc::new(Mutex::new(SubscriptionState::Opening));
        let applied = Arc::new(Mutex::new(request.clone()));
        let (reconciliation, receiver) = tokio::sync::watch::channel(request);
        let hold = Arc::new(Hold {
            entered: tokio::sync::Notify::new(),
            release: Mutex::new(false),
            changed: Condvar::new(),
            exited: AtomicBool::new(false),
        });
        let watchdog_hold = Arc::clone(&hold);
        let watchdog = std::thread::spawn(move || {
            let release = watchdog_hold.release.lock().unwrap();
            let (mut release, _) = watchdog_hold
                .changed
                .wait_timeout_while(release, Duration::from_secs(3), |release| !*release)
                .unwrap();
            *release = true;
            watchdog_hold.changed.notify_all();
        });
        let reservation = role
            .runtime
            .subscription_port()
            .reserve(
                &role.scope(),
                Some(Arc::clone(&publisher)),
                Some(Arc::clone(&state)),
            )
            .unwrap();
        let client = Client::for_endpoint(
            loopback.endpoint.native_endpoint().clone(),
            NativeEndpointDomain::BackendData,
            backend,
            role.runtime.clone(),
        );
        let lease = reservation
            .start(HeldCoveredFuture {
                inner: Box::pin(run_covered_subscription(
                    client,
                    receiver,
                    applied,
                    Arc::clone(&publisher),
                    3,
                    Arc::clone(&state),
                )),
                hold: Arc::clone(&hold),
            })
            .unwrap();
        drop(publisher);
        subscribe_requests(&loopback.peer, 1).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while *state.lock().unwrap() != SubscriptionState::Live {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        lease.stop();
        tokio::time::timeout(Duration::from_secs(2), hold.entered.notified())
            .await
            .unwrap();
        let original_deadline = std::time::Instant::now() + Duration::from_millis(20);
        let held_drain = role
            .runtime
            .drain_original_subscriptions_until(original_deadline)
            .await;
        let still_charged = intake.subscribe().is_err();
        let not_joined = !role.runtime.original_subscriptions_joined();
        *hold.release.lock().unwrap() = true;
        hold.changed.notify_all();
        let actual_drain = role
            .runtime
            .drain_original_subscriptions_until(std::time::Instant::now() + Duration::from_secs(2))
            .await;
        let publisher_returned = intake.subscribe().is_ok();
        drop(lease);
        drop(reconciliation);
        drop(others);
        watchdog.join().unwrap();
        role.settle().await;
        let peer_joined = loopback.settle().await;
        assert!(held_drain.is_err() && still_charged && not_joined);
        assert!(
            actual_drain.is_ok()
                && publisher_returned
                && hold.exited.load(Ordering::Acquire)
                && peer_joined
        );
    }

    #[test]
    fn covered_identity_violations_retain_their_typed_failure_for_every_carrier() {
        let backend = BackendProcessId::new_v7();
        let exact_participation = participation(backend);
        let exact_task = identity(1, backend);
        let request = covered_validation_request(exact_participation, exact_task);
        let requested_versions = CoveredRequestedVersions::from_request(&request);
        let other_query =
            QueryExecutionId::new(QueryId::new(91, 92), AttemptId::new(1).unwrap()).unwrap();
        for (query, process, expected) in [
            (
                exact_participation.query_execution_id(),
                BackendProcessId::new_v7(),
                SubscriptionState::ProcessMismatch,
            ),
            (other_query, backend, SubscriptionState::QueryMismatch),
        ] {
            let foreign_task =
                TaskIdentity::new(query, exact_task.stage_id(), exact_task.task_id(), process);
            let foreign_participation =
                ParticipationRef::new(query, exact_participation.frontend_process_id(), process);
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
                CoveredStatusStreamFact::ParticipationConvergence(convergence_receipt(
                    foreign_participation,
                )),
                CoveredStatusStreamFact::Quiesce(QuiesceParticipationReceipt::new(
                    foreign_participation,
                    1,
                    vec![exact_task],
                    ParticipationState::Quiescing,
                )),
                // A correct Participation cannot launder a foreign accepted member.
                CoveredStatusStreamFact::Quiesce(QuiesceParticipationReceipt::new(
                    exact_participation,
                    1,
                    vec![foreign_task],
                    ParticipationState::Quiescing,
                )),
            ];
            for fact in facts {
                let event = CoveredStatusStreamEvent {
                    fact,
                    source_revision: None,
                };
                let error =
                    validate_covered_event_participation(&request, &requested_versions, &event)
                        .expect_err("foreign identity must be rejected");
                assert_eq!(error.subscription_state(), expected, "{event:?}");
            }
        }
        let foreign_frontend = ParticipationRef::new(
            exact_participation.query_execution_id(),
            FrontendProcessId::new_v7(),
            backend,
        );
        for fact in [
            CoveredStatusStreamFact::ParticipationConvergence(convergence_receipt(
                foreign_frontend,
            )),
            CoveredStatusStreamFact::Quiesce(QuiesceParticipationReceipt::new(
                foreign_frontend,
                1,
                vec![exact_task],
                ParticipationState::Quiescing,
            )),
        ] {
            assert_eq!(
                validate_covered_event_participation(
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
        let request = covered_validation_request(participation(backend), identity(1, backend));
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
                validate_covered_event_participation(&request, &versions, &event)
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
            validate_covered_event_participation(&request, &versions, &event)
                .expect("exact version and generation remain valid");
        }
    }

    #[test]
    fn covered_connection_evidence_requires_typed_loss_and_resets_only_on_liveness() {
        let mut evidence = CoveredConnectionEvidence::default();
        for status in [
            Status::unavailable("connection refused is only untrusted text"),
            Status::deadline_exceeded("local read timeout"),
        ] {
            let error = CoveredSubscriptionOpenError::Rpc(status);
            evidence.observe_loss(error.observed_connection_loss());
            assert_eq!(
                evidence.exhausted_state(),
                SubscriptionState::BudgetExhausted
            );
        }
        let opaque_channel = CoveredSubscriptionOpenError::Channel(
            super::super::transport::ChannelAcquisitionError::retryable_network("connection reset"),
        );
        assert!(!opaque_channel.observed_connection_loss());
        let actual = CoveredSubscriptionOpenError::Rpc(Status::from_error(Box::new(
            std::io::Error::from(std::io::ErrorKind::ConnectionReset),
        )));
        evidence.observe_loss(actual.observed_connection_loss());
        assert_eq!(
            evidence.exhausted_state(),
            SubscriptionState::ConnectionLossExhausted
        );
        // A newly opened stream without a real liveness frame cannot clear loss.
        evidence.observe_loss(false);
        assert_eq!(
            evidence.exhausted_state(),
            SubscriptionState::ConnectionLossExhausted
        );
        evidence.observe_liveness();
        assert_eq!(
            evidence.exhausted_state(),
            SubscriptionState::BudgetExhausted
        );
        evidence.observe_loss(actual.observed_connection_loss());
        assert_eq!(
            evidence.exhausted_state(),
            SubscriptionState::ConnectionLossExhausted
        );
    }

    async fn covered_terminal_after_original_transport_failure(connection_lost: bool) {
        let backend = BackendProcessId::new_v7();
        let mut loopback = Loopback::start().await;
        let descriptor = loopback.descriptor(backend);
        if connection_lost {
            assert!(loopback.settle().await);
        } else {
            loopback
                .peer
                .expect_subscribe(SubscribeAnswer::Reject(tonic::Code::Unavailable));
        }
        let participation = participation(backend);
        let task = identity(1, backend);
        let intake = Arc::new(
            ObservationIntake::new(1, 3, 3 * 4096, 4096, Arc::new(CountingWake::default()))
                .unwrap(),
        );
        let role = CoveredRoleFixture::new();
        let subscriber = CoveredTaskStatusSubscriber::new(
            &[descriptor],
            intake,
            1,
            role.runtime.clone(),
            role.scope(),
        )
        .unwrap();
        subscriber
            .ensure(DecodedCoveredSubscription {
                participation,
                generation: NonZeroU64::new(1).unwrap(),
                status_cursors: vec![TaskStatusCursor::unobserved(task)],
                task_convergence_cursors: Vec::new(),
                participation_cursor: None,
                quiesce_cursor: None,
                required_identities: vec![task],
            })
            .unwrap();
        let expected = if connection_lost {
            SubscriptionState::ConnectionLossExhausted
        } else {
            SubscriptionState::BudgetExhausted
        };
        tokio::time::timeout(Duration::from_secs(8), async {
            while !subscriber
                .state(participation)
                .is_some_and(SubscriptionState::is_fatal)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("original bounded transport failure must settle");
        assert_eq!(subscriber.state(participation), Some(expected));
        subscriber.stop(participation);
        role.settle().await;
        if !connection_lost {
            assert!(loopback.settle().await);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_original_closed_endpoint_retains_typed_connection_loss() {
        covered_terminal_after_original_transport_failure(true).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_plain_remote_unavailable_is_not_original_connection_loss() {
        covered_terminal_after_original_transport_failure(false).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_subscription_blackhole_opening_exhausts_its_bounded_budget() {
        let backend = BackendProcessId::new_v7();
        let mut loopback = Loopback::start().await;
        let participation = participation(backend);
        let task = identity(1, backend);
        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::StallOpening);
        let intake = Arc::new(
            ObservationIntake::new(1, 3, 3 * 4096, 4096, Arc::new(CountingWake::default()))
                .unwrap(),
        );
        let subscription_role = CoveredRoleFixture::new();
        let subscriber = CoveredTaskStatusSubscriber::new(
            &[loopback.descriptor(backend)],
            intake,
            1,
            subscription_role.runtime.clone(),
            subscription_role.scope(),
        )
        .unwrap();
        subscriber
            .ensure(DecodedCoveredSubscription {
                participation,
                generation: NonZeroU64::new(1).unwrap(),
                status_cursors: vec![TaskStatusCursor::unobserved(task)],
                task_convergence_cursors: Vec::new(),
                participation_cursor: None,
                quiesce_cursor: None,
                required_identities: vec![task],
            })
            .unwrap();
        subscribe_requests(&loopback.peer, 1).await;
        tokio::time::timeout(Duration::from_secs(8), async {
            while subscriber.state(participation) != Some(SubscriptionState::BudgetExhausted) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("an opening blackhole cannot retain its transport indefinitely");
        assert_eq!(loopback.peer.subscribed().len(), 1);
        subscriber.stop(participation);
        subscription_role.settle().await;
        assert!(loopback.settle().await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_stream_stops_reading_while_its_one_pending_frame_waits() {
        let backend = BackendProcessId::new_v7();
        let mut loopback = Loopback::start().await;
        let participation = participation(backend);
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
        let subscription_role = CoveredRoleFixture::new();
        let subscriber = CoveredTaskStatusSubscriber::new(
            &[loopback.descriptor(backend)],
            Arc::clone(&intake),
            2,
            subscription_role.runtime.clone(),
            subscription_role.scope(),
        )
        .unwrap();
        subscriber
            .ensure(DecodedCoveredSubscription {
                participation,
                generation: NonZeroU64::new(1).unwrap(),
                status_cursors: vec![TaskStatusCursor::unobserved(task)],
                task_convergence_cursors: Vec::new(),
                participation_cursor: None,
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
        assert_eq!(
            subscriber.state(participation),
            Some(SubscriptionState::Live)
        );
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
        assert_eq!(
            subscriber.state(participation),
            Some(SubscriptionState::Live)
        );
        subscriber.stop(participation);
        subscription_role.settle().await;
        assert!(loopback.settle().await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_reconciliation_releases_an_unread_stream_permit() {
        let backend = BackendProcessId::new_v7();
        let mut loopback = Loopback::start().await;
        let participation = participation(backend);
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
        let subscription_role = CoveredRoleFixture::new();
        let subscriber = CoveredTaskStatusSubscriber::new(
            &[loopback.descriptor(backend)],
            Arc::clone(&intake),
            2,
            subscription_role.runtime.clone(),
            subscription_role.scope(),
        )
        .unwrap();
        let request = DecodedCoveredSubscription {
            participation,
            generation: NonZeroU64::new(1).unwrap(),
            status_cursors: vec![TaskStatusCursor::unobserved(task)],
            task_convergence_cursors: Vec::new(),
            participation_cursor: None,
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
        subscriber.stop(participation);
        subscription_role.settle().await;
        assert!(loopback.settle().await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn covered_automatic_reconnect_uses_the_latest_applied_cursor_without_restarting_early() {
        let backend = BackendProcessId::new_v7();
        let mut loopback = Loopback::start().await;
        let participation = participation(backend);
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
        let subscription_role = CoveredRoleFixture::new();
        let subscriber = CoveredTaskStatusSubscriber::new(
            &[loopback.descriptor(backend)],
            Arc::clone(&intake),
            3,
            subscription_role.runtime.clone(),
            subscription_role.scope(),
        )
        .unwrap();
        let mut request = DecodedCoveredSubscription {
            participation,
            generation: NonZeroU64::new(1).unwrap(),
            status_cursors: vec![TaskStatusCursor::unobserved(task)],
            task_convergence_cursors: Vec::new(),
            participation_cursor: None,
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
        subscriber.stop(participation);
        subscription_role.settle().await;
        assert!(loopback.settle().await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_participation_aware_stream_retains_convergence_and_reconnects_from_its_cursor() {
        let backend = BackendProcessId::new_v7();
        let mut fixture = participation_aware_subscriber_fixture(
            Loopback::start().await,
            backend,
            2,
            NonZeroUsize::MIN,
        );
        let subscribed_participation = participation(backend);
        let receipt = convergence_receipt(subscribed_participation);
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenClose(vec![
                encode_participation_convergence_event(receipt),
            ]));
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(Vec::new()));

        fixture
            .subscriber
            .ensure(subscribed_participation, Vec::new())
            .expect("the convergence-aware subscription starts");
        let requests = subscribe_requests(&fixture.loopback.peer, 2).await;

        let (first_participation, first_tasks, first_participation_cursor) =
            decode_participation_aware_subscribe_task_status(
                &requests[0],
                FieldPath::root("first_context_aware_subscription"),
            )
            .expect("a legal participation-aware subscription");
        assert_eq!(first_participation, subscribed_participation);
        assert!(first_tasks.is_empty());
        let first_participation_cursor =
            first_participation_cursor.expect("convergence is always requested");
        assert_eq!(
            first_participation_cursor.participation(),
            subscribed_participation
        );
        assert_eq!(first_participation_cursor.current_version(), None);

        let (_, _, replayed_participation_cursor) =
            decode_participation_aware_subscribe_task_status(
                &requests[1],
                FieldPath::root("replayed_context_aware_subscription"),
            )
            .expect("a legal replay");
        assert_eq!(
            replayed_participation_cursor.and_then(|cursor| cursor.current_version()),
            Some(ParticipationConvergenceVersion::FIRST),
            "the stream advances its participation cursor only after the inbox retained the receipt"
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
            "participation convergence never enters the task status route"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn task_reconciliation_reopens_one_stream_and_preserves_the_participation_cursor() {
        let backend = BackendProcessId::new_v7();
        let fixture = participation_aware_subscriber_fixture(
            Loopback::start().await,
            backend,
            2,
            NonZeroUsize::MIN,
        );
        let subscribed_participation = participation(backend);
        let task = identity(1, backend);
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![
                encode_participation_convergence_event(convergence_receipt(
                    subscribed_participation,
                )),
            ]));
        fixture
            .subscriber
            .ensure(
                subscribed_participation,
                vec![TaskStatusCursor::unobserved(task)],
            )
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
                subscribed_participation,
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
        let (_, task_cursors, participation_cursor) =
            decode_participation_aware_subscribe_task_status(
                &requests[1],
                FieldPath::root("reconciled_context_aware_subscription"),
            )
            .expect("a legal reconciled subscription");
        assert_eq!(
            task_cursors[0].current_version(),
            Some(TaskStatusVersion::FIRST)
        );
        assert_eq!(
            participation_cursor.and_then(|cursor| cursor.current_version()),
            Some(ParticipationConvergenceVersion::FIRST),
            "task reconciliation cannot reset the independently retained convergence cursor"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn convergence_overflow_waits_for_capacity_without_spending_the_network_budget() {
        let backend = BackendProcessId::new_v7();
        let mut fixture = participation_aware_subscriber_fixture(
            Loopback::start().await,
            backend,
            1,
            NonZeroUsize::MIN,
        );
        let first = participation(backend);
        let second = participation(backend);
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![
                encode_participation_convergence_event(convergence_receipt(first)),
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
                encode_participation_convergence_event(convergence_receipt(second)),
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
            .expect("the first participation occupies the only slot");
        assert_eq!(first_lease.receipt(), convergence_receipt(first));
        assert_eq!(
            first_lease.ack(),
            crate::task_execution::participation_convergence::ParticipationConvergenceRetainedAck::Released
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
        let exact = participation(backend);
        let stranger = participation(backend);
        let owner = ParticipationConvergenceIntake::bounded(NonZeroUsize::MIN);
        let handle = owner.handle();
        let second_version = ParticipationConvergenceVersion::FIRST
            .next()
            .expect("version two");
        let mut cursor = Some(ParticipationConvergenceCursor::at(exact, second_version));
        assert_eq!(
            observe_participation_convergence(
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

        let mut cursor = Some(ParticipationConvergenceCursor::unobserved(exact));
        assert_eq!(
            observe_participation_convergence(
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

        let closed_owner = ParticipationConvergenceIntake::bounded(NonZeroUsize::MIN);
        let closed = closed_owner.handle();
        drop(closed_owner);
        assert_eq!(
            observe_participation_convergence(
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
        let subscribed_participation = participation(backend);
        let task = identity(1, backend);
        let second_version = TaskStatusVersion::FIRST.next().expect("version two");
        let intake = StatusIntake::new(2, Arc::new(CountingWake::default()));
        let mut cursors = BTreeMap::from([(task, TaskStatusCursor::at(task, second_version))]);

        assert_eq!(
            observe_stream_event(
                subscribed_participation,
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
                subscribed_participation,
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
        let subscribed_participation = participation(backend);
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
            .ensure(
                subscribed_participation,
                vec![TaskStatusCursor::unobserved(task)],
            )
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
            vec![StatusEvent::Published(Box::new(status))],
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
                .state(subscribed_participation)
                .expect("the subscription is tracked")
                .is_fatal(),
            "a lost observation channel is not fatal to the attempt"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_status_receive_path_only_enqueues_and_wakes() {
        let backend = BackendProcessId::new_v7();
        let fixture = subscriber_fixture(Loopback::start().await, backend, 2);
        let subscribed_participation = participation(backend);
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
            .ensure(subscribed_participation, Vec::new())
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
                StatusEvent::Published(Box::new(TaskStatus::created(first))),
                StatusEvent::Published(Box::new(TaskStatus::created(second))),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_status_event_from_another_process_is_fatal() {
        let backend = BackendProcessId::new_v7();
        let replacement = BackendProcessId::new_v7();
        let fixture = subscriber_fixture(Loopback::start().await, backend, 2);
        let subscribed_participation = participation(backend);
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(vec![encode_status_event(
                &TaskStatus::created(identity(1, replacement)),
            )]));

        fixture
            .subscriber
            .ensure(subscribed_participation, Vec::new())
            .expect("the subscription starts");
        let mut observed = None;
        for _ in 0..600 {
            let state = fixture.subscriber.state(subscribed_participation);
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
        let subscribed_participation = participation(backend);
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
            .ensure(subscribed_participation, Vec::new())
            .expect("the subscription starts");
        let mut observed = None;
        for _ in 0..600 {
            let state = fixture.subscriber.state(subscribed_participation);
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
        let subscribed_participation = participation(backend);
        let task = identity(1, backend);
        let first = TaskStatus::created(task);
        let second_version = TaskStatusVersion::FIRST.next().expect("version two");
        let second = status_at(task, second_version);
        let intake = StatusIntake::new(1, Arc::new(CountingWake::default()));
        let handle = intake.handle();
        let mut cursors = BTreeMap::from([(task, TaskStatusCursor::unobserved(task))]);

        assert_eq!(
            observe_event(
                subscribed_participation,
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
                subscribed_participation,
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
        assert_eq!(statuses, vec![StatusEvent::Published(Box::new(first))]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn intake_overflow_waits_for_task_round_resubscription_without_spending_error_budget() {
        let backend = BackendProcessId::new_v7();
        let loopback = Loopback::start().await;
        let data_runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let intake = StatusIntake::new(1, Arc::new(CountingWake::default()));
        let subscriber = TaskStatusSubscriber::new(
            &[loopback.descriptor(backend)],
            intake.handle(),
            1,
            data_runtime,
        )
        .expect("one frozen backend target");
        let subscribed_participation = participation(backend);
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
            .ensure(
                subscribed_participation,
                vec![TaskStatusCursor::unobserved(task)],
            )
            .expect("the subscription starts");
        for _ in 0..600 {
            if subscriber.state(subscribed_participation) == Some(SubscriptionState::Resubscribing)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            subscriber.state(subscribed_participation),
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
        assert_eq!(statuses, vec![StatusEvent::Published(Box::new(first))]);
        drop(runner);

        loopback
            .peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(Vec::new()));
        subscriber
            .resubscribe(
                subscribed_participation,
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
        let subscribed_participation = participation(backend);
        fixture
            .loopback
            .peer
            .expect_subscribe(SubscribeAnswer::Reject(tonic::Code::InvalidArgument));

        fixture
            .subscriber
            .ensure(subscribed_participation, Vec::new())
            .expect("the subscription starts");
        for _ in 0..600 {
            if fixture.subscriber.state(subscribed_participation)
                == Some(SubscriptionState::Rejected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(
            fixture.subscriber.state(subscribed_participation),
            Some(SubscriptionState::Rejected)
        );
        assert_eq!(
            fixture.loopback.peer.subscribed().len(),
            1,
            "a typed rejection is never replayed"
        );
    }

    #[tokio::test]
    async fn constructor_refusals_keep_their_actual_producer_facts() {
        use novarocks_coordination_contract::AttemptFailureClass;
        use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
        use novarocks_query_application::coordination::ReplacementQualificationFailure;
        use novarocks_query_application::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite,
        };
        let fixture = CoveredRoleFixture::new();
        let acks = TaskAckIntake::new(Arc::new(CountingWake::default()));
        let descriptor = test_descriptor(
            BackendProcessId::new_v7(),
            RuntimeEndpoint::new("127.0.0.1", 9000).unwrap(),
            RuntimeEndpoint::new("control.test.invalid", 19061).unwrap(),
        );
        let source_loopback = Loopback::start().await;
        let governed = governed_wire::GovernedWireRole::new(&source_loopback);
        let error = NativeTaskOperationSink::new(
            &[],
            TransportBudget::DEFAULT,
            test_attempt_facts(),
            acks.handle(),
            fixture.runtime.clone(),
            governed.authority(),
        )
        .unwrap_err();
        assert_eq!(error, NativeTaskTransportCreationError::NoBackends);
        assert_eq!(error.failure_fact().reason().code(), 102);
        let error = NativeTaskOperationSink::new(
            &[descriptor.clone(), descriptor.clone()],
            TransportBudget::DEFAULT,
            test_attempt_facts(),
            acks.handle(),
            fixture.runtime.clone(),
            governed.authority(),
        )
        .unwrap_err();
        assert_eq!(
            error,
            NativeTaskTransportCreationError::DuplicateBackend {
                process: descriptor.process_id(),
            }
        );
        assert_eq!(error.failure_fact().reason().code(), 103);
        let other_budget = TransportBudget::new(
            2,
            1024,
            novarocks_task_codec::TransportMessageLimits::DEFAULT,
            4,
            256 * 1024 * 1024,
            4,
            512 * 1024 * 1024,
            1,
            2,
            Duration::from_secs(1),
        )
        .unwrap();
        let error = NativeTaskOperationSink::new(
            &[descriptor],
            other_budget,
            test_attempt_facts(),
            acks.handle(),
            fixture.runtime.clone(),
            governed.authority(),
        )
        .unwrap_err();
        assert_eq!(error, NativeTaskTransportCreationError::BudgetMismatch);
        assert_eq!(error.failure_fact().reason().code(), 101);
        let original = error.failure_fact();
        let refusal = ReplacementQualificationFailure::DomainRefused(original);
        let input = ExecutionFailureInput::new(
            refusal.failure_fact(),
            refusal.safe_detail(),
            ExecutionFailureSite::Coordinator,
        );
        assert_eq!(input.fact(), original);
        assert_eq!(input.fact().class(), FailureClass::Invariant);
        assert_eq!(
            input.detail().as_str(),
            "owning service refused replacement qualification"
        );
        assert_eq!(
            input.attempt_class(),
            AttemptFailureClass::ContractViolation
        );
        // Real original process owners, including both reapers, settle before this test returns.
        fixture.settle().await;
    }

    #[test]
    fn a_subscription_needs_a_nonzero_error_budget() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a test runtime");
        let data_runtime = FrontendDataRuntime::new(runtime.handle().clone());
        let error = TaskStatusSubscriber::new(
            &[test_descriptor(
                BackendProcessId::new_v7(),
                RuntimeEndpoint::new("127.0.0.1", 9000).expect("a valid data endpoint"),
                RuntimeEndpoint::new("control.test.invalid", 19061)
                    .expect("a valid control endpoint"),
            )],
            StatusIntake::new(1, Arc::new(CountingWake::default())).handle(),
            0,
            data_runtime,
        )
        .expect_err("a zero budget cannot bound anything");
        assert_eq!(
            error,
            NativeTaskTransportCreationError::ZeroStatusErrorBudget
        );
        use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(error.failure_fact().reason().code(), 106);
        assert_eq!(
            error.safe_detail().as_str(),
            "native status subscription error budget must be nonzero"
        );
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
    fn an_admission_request_and_receipt_keep_the_exact_participation_validity_and_compatibility() {
        let backend = BackendProcessId::new_v7();
        let participation = participation(backend);
        let valid_for = LeaseValidFor::new(Duration::from_secs(10)).expect("a legal validity");
        let compatibility = novarocks_types::NativeCompatibilityId::new([0x71; 32]);
        let admission_epoch_capability =
            novarocks_execution::task_execution::AdmissionEpochCapability::try_from_bytes(
                [0x61; 16],
            )
            .expect("nonzero epoch");
        let request = AcquireParticipationAdmissionTicket::new(
            TaskOperationId::new_v7(),
            participation,
            valid_for,
            compatibility,
            admission_epoch_capability,
        );
        let encoded = encode_operation(
            &OperationIntent::AcquireParticipationAdmissionTicket(request),
            &test_attempt_facts(),
        )
        .expect("the admission request is encodable");
        assert_eq!(
            codec::wire_size::acquire(request).unwrap(),
            prost::Message::encoded_len(&encoded)
        );
        let Some(proto::task_operation::Operation::AcquireParticipationAdmissionTicket(encoded)) =
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
        let receipt = proto::TaskOperationReceipt{operation_id:None,result:Some(proto::task_operation_receipt::Result::Verdict(proto::TaskOperationVerdict{outcome:0,safe_detail:String::new(),safe_field_path:None,ack:Some(
                proto::task_operation_verdict::Ack::ParticipationAdmissionTicket(
                    codec::encode_participation_admission_ticket_ack(
                        novarocks_execution::task_execution::ParticipationAdmissionTicketReceipt::new(
                            ticket,
                            participation,
                            valid_for,
                        ),
                    ),
                ),
            )}))};
        let payload = decode_ack(
            OperationKind::AcquireParticipationAdmissionTicket,
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
    fn native_domain_failure_reaches_the_original_ack_without_proving_task_acceptance() {
        use novarocks_execution_contract::failure::*;
        use novarocks_execution_contract::operation::{
            DomainFailureOutcome, OperationDomainFailure, OperationResult,
        };
        let fact = FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(
                std::num::NonZeroU16::new(u16::MAX).unwrap(),
                std::num::NonZeroU16::new(65001).unwrap(),
            ),
        )
        .with_row_position(FailureRowPosition {
            selected_ordinal: 0,
            batch_row: 19,
        });
        let failure = OperationDomainFailure::new(
            fact,
            novarocks_execution_contract::SafeDetail::new("producer representation boundary")
                .unwrap(),
        );
        let operation_id = TaskOperationId::new_v7();
        let wire = codec::encode_receipt(
            operation_id,
            OperationResult::DomainFailure {
                outcome: DomainFailureOutcome::InvalidStateOrRequest,
                failure: failure.clone(),
            },
        );
        let header = codec::decode_receipt_header(&wire, FieldPath::root("receipt")).unwrap();
        let item = SentOperation {
            operation_id,
            kind: OperationKind::CreateTask,
            establish_participation: None,
            lease_renewal: false,
            address: AckAddress::None,
        };
        let ack = acknowledgement(&item, &header, &wire);
        assert!(!ack.is_applied());
        assert!(matches!(ack.payload(), AckPayload::None));
        assert_eq!(
            ack.worker_outcome(),
            Some(OperationOutcome::InvalidStateOrRequest)
        );
        let OperationResult::DomainFailure {
            failure: carried, ..
        } = ack.worker_header().unwrap()
        else {
            panic!("domain branch")
        };
        assert_eq!(carried, failure);
    }

    #[test]
    fn an_applied_create_acknowledgement_carries_the_task_status() {
        let backend = BackendProcessId::new_v7();
        let mine = identity(1, backend);
        let status = TaskStatus::created(mine);
        let ack = proto::task_operation_verdict::Ack::CreateTask(
            codec::encode_create_task_ack(&CreateTaskReceipt::new(
                mine,
                Vec::new(),
                status.clone(),
            ))
            .expect("an applied create encodes"),
        );
        let receipt = proto::TaskOperationReceipt {
            operation_id: None,
            result: Some(proto::task_operation_receipt::Result::Verdict(
                proto::TaskOperationVerdict {
                    outcome: 0,
                    safe_detail: String::new(),
                    safe_field_path: None,
                    ack: Some(ack.clone()),
                },
            )),
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
        let empty = {
            let mut value = receipt.clone();
            let Some(proto::task_operation_receipt::Result::Verdict(verdict)) = &mut value.result
            else {
                panic!("fixture requires a protocol verdict")
            };
            verdict.ack = None;
            value
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
        // A lease renewal consumes its participation body, so a create body here is
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
                Some(proto::task_operation_verdict::Ack::CreateTask(
                    codec::encode_create_task_ack(&CreateTaskReceipt::new(
                        task,
                        Vec::new(),
                        TaskStatus::created(task),
                    ))
                    .expect("an applied create encodes"),
                )),
            )]));

        submit_batch(&fixture, batch);
        let acks = settled(&fixture.acks, 1).await;
        assert_eq!(
            acks[0].dispatch_result(),
            OperationDispatchResult::NonWorkerRejected
        );
        assert_eq!(*acks[0].payload(), AckPayload::None);
    }

    #[test]
    fn owner_failure_never_consumes_a_success_or_closure_acknowledgement() {
        for kind in [
            OperationKind::AcquireParticipationAdmissionTicket,
            OperationKind::CreateTask,
            OperationKind::UpdateTask,
            OperationKind::UpdateParticipation,
            OperationKind::QuiesceParticipation,
            OperationKind::AbortParticipation,
            OperationKind::ReleaseParticipation,
        ] {
            assert!(!consumes_ack_body_for_outcome(
                kind,
                OperationOutcome::OwnerFailed
            ));
        }
        assert_eq!(outcome_name(OperationOutcome::OwnerFailed), "owner_failed");
    }

    #[test]
    fn owner_failed_subscription_rejection_is_fatal_but_network_loss_can_resubscribe() {
        assert!(subscription_rejection_is_fatal(
            &tonic::Status::failed_precondition("the participation protocol owner failed",)
        ));
        assert!(!subscription_rejection_is_fatal(
            &tonic::Status::unavailable("connection closed",)
        ));
    }

    #[tokio::test]
    async fn owner_failed_abort_keeps_its_exact_verdict_without_a_closure_body() {
        let backend = BackendProcessId::new_v7();
        let fixture = sink_fixture(Loopback::start().await, backend);
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::WithAck(vec![(
                OperationOutcome::OwnerFailed,
                None,
            )]));
        submit_batch(
            &fixture,
            released_batch(backend, vec![abort_intent(participation(backend))]),
        );
        let acks = settled(&fixture.acks, 1).await;
        assert_eq!(
            acks[0].worker_outcome(),
            Some(OperationOutcome::OwnerFailed)
        );
        assert_eq!(*acks[0].payload(), AckPayload::None);
        assert_eq!(
            crate::task_execution::operation::frontend_action(acks[0].dispatch_result()),
            crate::task_execution::operation::FrontendAction::FailAttempt,
        );
    }

    #[tokio::test]
    async fn every_legal_abort_closing_outcome_keeps_its_participation_receipt() {
        for (outcome, state) in [
            (OperationOutcome::Accepted, ParticipationState::Aborting),
            (OperationOutcome::Idempotent, ParticipationState::Aborting),
            (
                OperationOutcome::ParticipationTerminalReceipt,
                ParticipationState::Releasing,
            ),
            (
                OperationOutcome::LeaseExpired,
                ParticipationState::TerminalRetained,
            ),
            (OperationOutcome::Gone, ParticipationState::Gone),
        ] {
            let backend = BackendProcessId::new_v7();
            let participation = participation(backend);
            let fixture = sink_fixture(Loopback::start().await, backend);
            fixture
                .loopback
                .peer
                .expect_apply(ApplyAnswer::WithAck(vec![(
                    outcome,
                    Some(proto::task_operation_verdict::Ack::Participation(
                        encode_participation_ack(
                            &ParticipationReceipt::new(participation, state),
                            Some(AbortCause::QueryFailed),
                        )
                        .expect("a closing participation receipt encodes"),
                    )),
                )]));
            submit_batch(
                &fixture,
                released_batch(backend, vec![abort_intent(participation)]),
            );
            let acks = settled(&fixture.acks, 1).await;
            assert_eq!(acks[0].worker_outcome(), Some(outcome));
            assert!(matches!(
                acks[0].payload(),
                AckPayload::Participation(receipt)
                    if receipt.participation() == participation && receipt.state() == state
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
                OperationOutcome::ParticipationTerminalReceipt,
                None,
            )]));
        submit_batch(
            &fixture,
            released_batch(backend, vec![abort_intent(participation(backend))]),
        );
        let acks = settled(&fixture.acks, 1).await;
        assert_eq!(
            acks[0].dispatch_result(),
            OperationDispatchResult::NonWorkerRejected
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
                Some(proto::task_operation_verdict::Ack::CancelTask(
                    novarocks_task_codec::status::encode_task_status(&TaskStatus::created(task)),
                )),
            )]));

        submit_batch(&fixture, batch);
        let acks = settled(&fixture.acks, 1).await;
        assert_eq!(acks[0].worker_outcome(), Some(OperationOutcome::Accepted));
        assert_eq!(*acks[0].payload(), AckPayload::None);
    }
}

#[path = "task_transport/action_input.rs"]
mod action_input;
pub(crate) use action_input::{NativeApplyKnownInputLayouts, known_input_layouts_for_composition};

mod proposal_wait;
