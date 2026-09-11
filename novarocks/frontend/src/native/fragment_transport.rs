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

//! Fragment dispatcher port and native submission DTO.
//!
//! Two result faces live here. [`FragmentDispatcher`] is the old one, keyed by
//! a fragment instance id — a key any participant of any attempt could name.
//! [`TaskResultTransport`] is the task protocol's: it addresses the root
//! result by exact [`TaskIdentity`], so the backend can fence the poll against
//! the exact task, the exact process, and result responsibility before it
//! touches a buffer, and it carries the packet sequence back so a frontend can
//! prove that no packet was lost on the way to it.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_execution::exec::chunk::{Chunk, ChunkSchemaRef};
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::runtime::exchange::{
    TypedRootResultDecodeBounds, preflight_typed_root_result_decode,
};
use novarocks_execution::task_execution::domain::DomainVersion;
use novarocks_execution::task_execution::operation::FetchTaskDynamicFilters;
use novarocks_execution::task_execution::{
    FinalTaskInfo, MaxWait, OperationOutcome, ResultByteLimit, ResultPacketSequence, TaskIdentity,
    TaskOperationId,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks::fetch_result_response::Status as FetchStatus;
use novarocks_query_application::{
    api::{QueryExecutionError, QueryExecutionErrorKind},
    coordination::{
        AttemptFailureClass, PreflightedRootResultPacket, RootResultDecodeBounds,
        RootResultDecodeRuntime, RootResultFetchFailure,
        RootResultFetchOutcome as PumpRootResultFetchOutcome, RootResultPumpBinding,
    },
};
use novarocks_task_codec::operation::{
    MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES, decode_operation_outcome, encode_fetch_dynamic_filters,
    encode_fetch_task_result, encode_get_final_task_info,
};
use novarocks_task_codec::status::decode_final_task_info;
use novarocks_types::UniqueId;
use novarocks_types::identity::BackendProcessId;

use crate::runtime_filter::feedback::TaskRuntimeFilterFeedback;

use super::data_runtime::FrontendDataRuntime;
use super::transport::Client;

/// Opaque data-plane batch returned by a fragment dispatcher.
///
/// The execution-layer `Chunk` remains owned by core. Role crates may route
/// this value through the query-execution contract but cannot inspect or
/// manufacture execution batches.
pub struct FetchedQueryBatch {
    chunk: Chunk,
}

impl FetchedQueryBatch {
    pub fn new(chunk: Chunk) -> Self {
        Self { chunk }
    }

    pub fn into_chunk(self) -> Chunk {
        self.chunk
    }
}

/// Borrowed opaque view of the root fetch schema.
#[derive(Clone, Copy)]
pub struct ExpectedOutputSchemaView<'a> {
    schema: &'a ChunkSchemaRef,
}

impl<'a> ExpectedOutputSchemaView<'a> {
    pub const fn new(schema: &'a ChunkSchemaRef) -> Self {
        Self { schema }
    }

    pub const fn chunk_schema(self) -> &'a ChunkSchemaRef {
        self.schema
    }
}

/// Decode one typed root-result payload into the opaque dispatcher value.
///
/// Native transports live in role crates, while Core retains the execution
/// batch representation and the canonical wire-to-chunk conversion.  This
/// keeps that conversion available without exposing `Chunk` construction to a
/// transport owner.
pub fn decode_fetched_query_batch(
    payload: &[u8],
    expected_output_schema: Option<ExpectedOutputSchemaView<'_>>,
) -> Result<FetchedQueryBatch, String> {
    let mut chunks = novarocks_execution::runtime::exchange::decode_root_result_chunks(
        payload,
        expected_output_schema.map(|view| view.chunk_schema()),
    )?;
    if chunks.len() != 1 {
        return Err(format!(
            "typed root result decoded {} chunks, expected 1",
            chunks.len()
        ));
    }
    Ok(FetchedQueryBatch::new(chunks.remove(0)))
}

/// Move-only ownership of one validated but not yet decoded root-result packet.
///
/// Construction performs the complete metadata-only IPC preflight. Keeping the
/// payload opaque ensures the transport can return it without allocating an
/// Arrow `RecordBatch`, while the later result pump can retain the raw bytes,
/// reserve decode capacity from the trusted bounds, and consume this value
/// exactly once to decode.
pub struct RawRootResultPacket {
    packet_sequence: ResultPacketSequence,
    payload: Bytes,
    payload_bytes: u64,
    decode_bounds: TypedRootResultDecodeBounds,
}

impl RawRootResultPacket {
    fn try_new(
        packet_sequence: ResultPacketSequence,
        payload: Bytes,
        payload_limit: ResultByteLimit,
    ) -> Result<Self, String> {
        let payload_bytes = u64::try_from(payload.len())
            .map_err(|_| "root result payload length does not fit u64".to_string())?;
        let decode_bounds = preflight_typed_root_result_decode(&payload, payload_limit)?;
        Ok(Self {
            packet_sequence,
            payload,
            payload_bytes,
            decode_bounds,
        })
    }

    pub const fn packet_sequence(&self) -> ResultPacketSequence {
        self.packet_sequence
    }

    /// Logical protobuf payload bytes used by result-credit accounting.
    ///
    /// The opaque receive buffer may retain transport allocator slack. That
    /// process overhead remains bounded by the Native message cap and the
    /// process-wide result-fetch concurrency supervisor; it is not presented
    /// as exact query-owned Arrow or payload backing.
    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    pub const fn decode_bounds(&self) -> TypedRootResultDecodeBounds {
        self.decode_bounds
    }

    /// Consume the sole raw owner and allocate the decoded Arrow batch.
    pub fn decode(
        self,
        expected_output_schema: Option<ExpectedOutputSchemaView<'_>>,
    ) -> Result<FetchedQueryBatch, String> {
        decode_fetched_query_batch(&self.payload, expected_output_schema)
    }
}

/// Outcome of a single `fetch_result` call.
pub enum FetchOutcome {
    /// A result batch is available.
    Ready(FetchedQueryBatch),
    /// No chunk available yet; fragment is still running.
    NotReady,
    /// All chunks have been delivered; the root fragment is complete.
    Eof,
    /// Fragment execution failed.
    Err(String),
}

/// Result transport for an already-running native query.
///
/// Query startup belongs exclusively to the query lifecycle Stage/Start
/// barrier. Query lifecycle owns cancellation and terminal convergence after
/// that barrier has entered `Running`.
#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
pub trait FragmentDispatcher: Send + Sync + 'static {
    /// Poll for the next result chunk from the root fragment on the given backend.
    fn fetch_result(
        &self,
        backend_idx: usize,
        finst_id: UniqueId,
        max_wait_ms: i64,
        expected_output_schema: Option<ExpectedOutputSchemaView<'_>>,
    ) -> Result<FetchOutcome, String>;

    /// Number of backends this dispatcher can route to.
    fn backend_count(&self) -> usize;
}

/// One answer from the root result data plane.
///
/// Unlike [`FetchOutcome`], the end of the stream carries its own packet
/// sequence. That is what lets a frontend distinguish "the stream ended after
/// everything I received" from "the stream ended after packets I never saw",
/// which the backend cannot tell it: it drops each packet as it hands it over.
#[allow(
    dead_code,
    reason = "The production cutover routes the coordinator's result loop onto this face."
)]
pub enum RootResultOutcome {
    /// One result packet.
    Ready(RawRootResultPacket),
    /// Nothing available within this poll's wait.
    NotReady,
    /// EOS arrived at this sequence but still needs the frontend's final ACK.
    EndOfStreamPending { packet_sequence: u64 },
    /// The stream ended and the backend accepted the final packet ACK.
    EndOfStream { packet_sequence: u64 },
    /// The poll was refused, or the root's execution failed.
    Failed(String),
}

impl fmt::Debug for RootResultOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready(packet) => formatter
                .debug_struct("Ready")
                .field("packet_sequence", &packet.packet_sequence())
                .field("payload_bytes", &packet.payload_bytes())
                .field("decode_bounds", &packet.decode_bounds())
                .finish_non_exhaustive(),
            Self::NotReady => formatter.write_str("NotReady"),
            Self::EndOfStreamPending { packet_sequence } => formatter
                .debug_struct("EndOfStreamPending")
                .field("packet_sequence", packet_sequence)
                .finish(),
            Self::EndOfStream { packet_sequence } => formatter
                .debug_struct("EndOfStream")
                .field("packet_sequence", packet_sequence)
                .finish(),
            Self::Failed(detail) => formatter.debug_tuple("Failed").field(detail).finish(),
        }
    }
}

/// What a final task info read answered.
///
/// Losing final info costs diagnostics only, so absence is a reported category
/// rather than an error: success and failure are decided by `TaskStatus`
/// alone.
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "The production cutover routes the coordinator's result loop onto this face."
)]
pub enum FinalTaskInfoRead {
    Available(FinalTaskInfo),
    Unavailable(OperationOutcome),
}

/// The longest one dynamic filter read may hold the coordinator's turn.
///
/// The read is answered immediately by the backend -- it is a projection of a
/// retained payload, not a long poll -- so this only bounds a backend that has
/// stopped answering. It has to be bounded here because the request carries no
/// wait field of its own, and the same thread that makes this call also settles
/// acknowledgements and opens exchange edges: an unbounded read would stop the
/// whole attempt to chase a pruning optimization.
const DYNAMIC_FILTER_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// How much longer than the wait it asked for a task-addressed read may take
/// before the backend is treated as having stopped answering.
///
/// A root result poll tells the backend how long it may block, so an answer
/// that has not arrived by that wait plus this allowance is not a slow answer,
/// it is no answer. Bounding it is not an optimization: the coordinator thread
/// that makes this call is the same one that settles acknowledgements, folds
/// status and opens exchange edges, so an unbounded call stops the whole
/// attempt -- past its own statement deadline, silently, with no fact named.
///
/// The allowance is the transport's own frontend queue residence rather than a
/// number invented here. That is already how long this attempt lets a released
/// operation sit before it calls it lost, and a second number would put two
/// answers on one question.
#[derive(Copy, Clone, Debug)]
pub(crate) struct TaskReadGrace(std::time::Duration);

impl TaskReadGrace {
    pub(crate) const fn new(grace: std::time::Duration) -> Self {
        Self(grace)
    }

    /// The deadline for a read that asked the backend to block for `wait`.
    fn deadline_for(self, wait: std::time::Duration) -> std::time::Duration {
        wait.saturating_add(self.0)
    }
}

/// What one dynamic filter read answered.
///
/// A version of `None` is the settled "nothing to fetch": either the task has
/// advertised nothing at all, or it went terminal and no longer retains the
/// payload it advertised. Both are answers, not failures -- the split source's
/// own wait cap degrades to unpruned enumeration -- so neither is an error
/// here.
#[derive(Clone, Debug)]
pub struct DynamicFilterRead {
    version: Option<DomainVersion>,
    feedback: Vec<TaskRuntimeFilterFeedback>,
}

impl DynamicFilterRead {
    /// The transport builds this from a decoded response; only a test that
    /// scripts a read builds one directly.
    #[cfg(test)]
    pub(crate) const fn new(
        version: Option<DomainVersion>,
        feedback: Vec<TaskRuntimeFilterFeedback>,
    ) -> Self {
        Self { version, feedback }
    }

    pub const fn version(&self) -> Option<DomainVersion> {
        self.version
    }

    pub fn feedback(&self) -> &[TaskRuntimeFilterFeedback] {
        &self.feedback
    }
}

/// Why one dynamic filter read produced no answer.
///
/// The two are acted on differently and must not be collapsed. `Unavailable`
/// means the read did not complete -- the next turn asks again, and the split
/// source keeps waiting inside its own cap. `Refused` means the backend
/// answered that the request itself is not legal against the task it names,
/// which is a real disagreement between this frontend's view of the task and
/// the backend's, and is never repaired by asking again.
#[derive(Clone, Debug)]
pub enum DynamicFilterReadError {
    Unavailable(String),
    Refused(String),
}

impl fmt::Display for DynamicFilterReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(detail) | Self::Refused(detail) => formatter.write_str(detail),
        }
    }
}

/// The task protocol's three observation reads over the data plane.
///
/// All three are addressed by exact [`TaskIdentity`]; none creates a task,
/// advances a status, or renews a lease.
#[allow(
    dead_code,
    reason = "The production cutover routes the coordinator's result loop onto this face."
)]
pub trait TaskResultTransport: Send + Sync + 'static {
    /// Polls the root task's result stream.
    fn fetch_root_result(
        &self,
        root_task: TaskIdentity,
        max_wait: MaxWait,
        acknowledged: Option<ResultPacketSequence>,
        max_result_bytes: ResultByteLimit,
    ) -> Pin<Box<dyn Future<Output = Result<RootResultOutcome, String>> + Send + 'static>>;

    /// Reads one terminal task's bounded final info.
    fn final_task_info(&self, identity: TaskIdentity) -> Result<FinalTaskInfoRead, String>;

    /// Reads whatever one task advertised above the reader's own cursor.
    fn dynamic_filters(
        &self,
        identity: TaskIdentity,
        acknowledged: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError>;
}

/// The native implementation over one frozen backend process set.
///
/// The set is frozen at construction from one live topology snapshot, exactly
/// as the operation transport freezes its own: a task identity naming a
/// process that is not in it is not looked up elsewhere, because a replaced
/// process is a different process.
#[allow(
    dead_code,
    reason = "The production cutover routes the coordinator's result loop onto this face."
)]
pub(crate) struct NativeTaskResultTransport {
    clients: BTreeMap<BackendProcessId, Client>,
    endpoints: BTreeMap<BackendProcessId, RuntimeEndpoint>,
    data_runtime: FrontendDataRuntime,
    grace: TaskReadGrace,
}

impl fmt::Debug for NativeTaskResultTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeTaskResultTransport")
            .field("backends", &self.clients.len())
            .finish_non_exhaustive()
    }
}

#[allow(
    dead_code,
    reason = "The production cutover routes the coordinator's result loop onto this face."
)]
impl NativeTaskResultTransport {
    pub(crate) fn new(
        backends: &[(BackendProcessId, RuntimeEndpoint)],
        data_runtime: FrontendDataRuntime,
        grace: TaskReadGrace,
    ) -> Result<Self, String> {
        if backends.is_empty() {
            return Err("the root result transport requires at least one backend".to_owned());
        }
        let mut clients = BTreeMap::new();
        let mut endpoints = BTreeMap::new();
        for (process_id, endpoint) in backends {
            let client = Client::new(endpoint.native_endpoint().clone(), data_runtime.clone());
            if clients.insert(*process_id, client).is_some() {
                return Err(format!("duplicate backend process {process_id}"));
            }
            endpoints.insert(*process_id, endpoint.clone());
        }
        Ok(Self {
            clients,
            endpoints,
            data_runtime,
            grace,
        })
    }

    fn client_of(&self, identity: TaskIdentity) -> Result<(&Client, String), String> {
        let process = identity.backend_process_id();
        let client = self.clients.get(&process).ok_or_else(|| {
            format!("task {identity} names a backend process this attempt did not freeze")
        })?;
        let address = self.endpoints[&process].to_string();
        Ok((client, address))
    }

    fn fetch_root_result_for_pump(
        &self,
        root_task: TaskIdentity,
        max_wait: MaxWait,
        acknowledged: Option<ResultPacketSequence>,
        max_result_bytes: ResultByteLimit,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<RootResultOutcome, NativeRootResultFetchError>>
                + Send
                + 'static,
        >,
    > {
        let route = self
            .client_of(root_task)
            .map(|(client, address)| (client.clone(), address))
            .map_err(NativeRootResultFetchError::contract);
        let validation = validate_native_result_byte_limit(max_result_bytes)
            .map_err(NativeRootResultFetchError::contract);
        let grace = self.grace;
        let data_runtime = self.data_runtime.clone();
        Box::pin(async move {
            let (client, address) = route?;
            validation?;
            let _fetch_permit = data_runtime
                .acquire_result_fetch()
                .await
                .map_err(NativeRootResultFetchError::resource_governance)?;
            let request =
                encode_fetch_task_result(root_task, max_wait, acknowledged, max_result_bytes);
            let wait = max_wait.get();
            let deadline = grace.deadline_for(wait);
            let expires_at = tokio::time::Instant::now() + deadline;
            let mut grpc = tokio::time::timeout_at(expires_at, client.grpc_with_channel_error())
                .await
                .map_err(|_| {
                    NativeRootResultFetchError::infrastructure(format!(
                        "{address}: root result poll for task {root_task} could not acquire a \
                         channel within {deadline:?}"
                    ))
                })?
                .map_err(|error| NativeRootResultFetchError::infrastructure(error.to_string()))?;
            let response = tokio::time::timeout_at(expires_at, grpc.fetch_task_result(request))
                .await
                .map_err(|_| {
                    NativeRootResultFetchError::infrastructure(format!(
                        "{address}: root result poll for task {root_task} did not answer within \
                         {deadline:?}; it was asked to wait at most {wait:?}"
                    ))
                })?
                .map(tonic::Response::into_inner)
                .map_err(classify_fetch_task_result_rpc_status)?;
            classify_root_result_response(&address, response, acknowledged, max_result_bytes)
                .map_err(NativeRootResultFetchError::contract)
        })
    }
}

fn classify_fetch_task_result_rpc_status(error: tonic::Status) -> NativeRootResultFetchError {
    let unknown_is_transport = error.code() == tonic::Code::Unknown
        && (std::error::Error::source(&error).is_some()
            || error.message().starts_with("Service was not ready: "));
    let detail = format!("fetch_task_result rpc failed: {error}");
    match error.code() {
        // These statuses state that the exact backend endpoint or its HTTP/2
        // transport could not complete the request in this attempt's bounded
        // transport window.
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            NativeRootResultFetchError::infrastructure(detail)
        }
        tonic::Code::Unknown if unknown_is_transport => {
            NativeRootResultFetchError::infrastructure(detail)
        }
        // A peer that explicitly refuses work for capacity reasons has made a
        // resource-governance decision rather than disappearing.
        tonic::Code::ResourceExhausted => NativeRootResultFetchError::resource_governance(detail),
        // The FetchResult response carries every application-owned refusal in
        // band. Tonic's generated client maps service-readiness failures to a
        // source-less Unknown with a fixed generated prefix; HTTP/2 failures
        // retain an error source. A source-less remote Unknown is ambiguous and
        // therefore remains a contract failure. Every other gRPC status is an
        // answered protocol, identity, authorization, or server-contract
        // refusal. Attempt execution failure is learned from the accepted Task
        // status projection, never inferred from this transport status.
        _ => NativeRootResultFetchError::contract(detail),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeRootResultFetchErrorClass {
    Infrastructure,
    ResourceGovernance,
    ContractViolation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeRootResultFetchError {
    class: NativeRootResultFetchErrorClass,
    detail: String,
}

impl NativeRootResultFetchError {
    fn infrastructure(detail: impl Into<String>) -> Self {
        Self {
            class: NativeRootResultFetchErrorClass::Infrastructure,
            detail: detail.into(),
        }
    }

    fn resource_governance(detail: impl Into<String>) -> Self {
        Self {
            class: NativeRootResultFetchErrorClass::ResourceGovernance,
            detail: detail.into(),
        }
    }

    fn contract(detail: impl Into<String>) -> Self {
        Self {
            class: NativeRootResultFetchErrorClass::ContractViolation,
            detail: detail.into(),
        }
    }

    fn into_pump_failure(self) -> RootResultFetchFailure {
        let (class, kind) = match self.class {
            NativeRootResultFetchErrorClass::Infrastructure => (
                AttemptFailureClass::RecoverableInfrastructure,
                QueryExecutionErrorKind::Failed,
            ),
            NativeRootResultFetchErrorClass::ResourceGovernance => (
                AttemptFailureClass::ResourceGovernance,
                QueryExecutionErrorKind::Rejected,
            ),
            NativeRootResultFetchErrorClass::ContractViolation => (
                AttemptFailureClass::ContractViolation,
                QueryExecutionErrorKind::InvalidRequest,
            ),
        };
        RootResultFetchFailure::new(class, QueryExecutionError::new(kind, self.detail))
    }
}

impl fmt::Display for NativeRootResultFetchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for NativeRootResultFetchError {}

impl TaskResultTransport for NativeTaskResultTransport {
    fn fetch_root_result(
        &self,
        root_task: TaskIdentity,
        max_wait: MaxWait,
        acknowledged: Option<ResultPacketSequence>,
        max_result_bytes: ResultByteLimit,
    ) -> Pin<Box<dyn Future<Output = Result<RootResultOutcome, String>> + Send + 'static>> {
        let fetch =
            self.fetch_root_result_for_pump(root_task, max_wait, acknowledged, max_result_bytes);
        Box::pin(async move { fetch.await.map_err(|error| error.to_string()) })
    }

    fn final_task_info(&self, identity: TaskIdentity) -> Result<FinalTaskInfoRead, String> {
        let (client, address) = self.client_of(identity)?;
        let request = encode_get_final_task_info(identity);
        // Final info is a projection of a retained record, answered
        // immediately, so this bound only covers a backend that stopped
        // answering. It has to be bounded for the same reason as the poll
        // above, and more sharply: this read runs inside the attempt's drain,
        // whose whole point is not to hold a client-visible completion.
        let deadline = self.grace.deadline_for(std::time::Duration::ZERO);
        let response = self.data_runtime.block_on(async {
            let expires_at = tokio::time::Instant::now() + deadline;
            let mut grpc = tokio::time::timeout_at(expires_at, client.grpc_with_channel_error())
                .await
                .map_err(|_| {
                    format!(
                        "{address}: final info read for task {identity} could not acquire a \
                         channel within {deadline:?}"
                    )
                })?
                .map_err(|error| error.to_string())?;
            tokio::time::timeout_at(expires_at, grpc.get_final_task_info(request))
                .await
                .map_err(|_| {
                    format!(
                        "{address}: final info read for task {identity} did not answer within \
                         {deadline:?}"
                    )
                })?
                .map(tonic::Response::into_inner)
                .map_err(|error| format!("get_final_task_info rpc failed: {error}"))
        })??;
        let result = response
            .result
            .ok_or_else(|| format!("{address}: final info response carries no result"))?;
        match result {
            novarocks_proto_models::novarocks::get_final_task_info_response::Result::Info(info) => {
                decode_final_task_info(identity, &info, FieldPath::root("final_task_info"))
                    .map(FinalTaskInfoRead::Available)
                    .map_err(|error| format!("{address}: {error}"))
            }
            novarocks_proto_models::novarocks::get_final_task_info_response::Result::Unavailable(
                outcome,
            ) => decode_operation_outcome(outcome, FieldPath::root("final_task_info"))
                .map(FinalTaskInfoRead::Unavailable)
                .map_err(|error| format!("{address}: {error}")),
        }
    }

    fn dynamic_filters(
        &self,
        identity: TaskIdentity,
        acknowledged: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
        let (client, address) = self
            .client_of(identity)
            .map_err(DynamicFilterReadError::Refused)?;
        // The operation identity is minted per call and never replayed: this
        // read creates nothing, so there is nothing for a replay to be
        // idempotent against.
        let request = encode_fetch_dynamic_filters(FetchTaskDynamicFilters::new(
            TaskOperationId::new_v7(),
            identity,
            acknowledged,
        ));
        let response = self
            .data_runtime
            .block_on(async {
                let expires_at = tokio::time::Instant::now() + DYNAMIC_FILTER_READ_TIMEOUT;
                let mut grpc =
                    tokio::time::timeout_at(expires_at, client.grpc_with_channel_error())
                        .await
                        .map_err(|_| {
                            DynamicFilterReadError::Unavailable(format!(
                                "{address}: dynamic filter read could not acquire a channel in time"
                            ))
                        })?
                        .map_err(|error| DynamicFilterReadError::Unavailable(error.to_string()))?;
                tokio::time::timeout_at(expires_at, grpc.fetch_task_dynamic_filters(request))
                    .await
                    .map_err(|_| {
                        DynamicFilterReadError::Unavailable(format!(
                            "{address}: dynamic filter read did not answer in time"
                        ))
                    })?
                    .map(tonic::Response::into_inner)
                    .map_err(|error| classify_dynamic_filter_status(&address, &error))
            })
            .map_err(DynamicFilterReadError::Unavailable)??;
        let answered = response
            .identity
            .as_ref()
            .map(|answered| {
                novarocks_task_codec::identity::decode_task_identity(
                    answered,
                    FieldPath::root("fetch_task_dynamic_filters").field("identity"),
                )
            })
            .transpose()
            .map_err(|error| DynamicFilterReadError::Refused(format!("{address}: {error}")))?
            .ok_or_else(|| {
                DynamicFilterReadError::Refused(format!(
                    "{address}: dynamic filter read answered without a task identity"
                ))
            })?;
        // A read that agrees with itself proves nothing. This is the identity
        // this frontend asked about, so an answer naming another task is
        // refused rather than admitted as that task's feedback.
        if answered != identity {
            return Err(DynamicFilterReadError::Refused(format!(
                "{address}: dynamic filter read for {identity} answered for {answered}"
            )));
        }
        let version = DomainVersion::new(response.version).ok();
        if version.is_none() && !response.domains.is_empty() {
            return Err(DynamicFilterReadError::Refused(format!(
                "{address}: dynamic filter read carries domains under version zero"
            )));
        }
        let mut feedback = Vec::with_capacity(response.domains.len());
        for domain in &response.domains {
            let envelope = domain.envelope.as_ref().ok_or_else(|| {
                DynamicFilterReadError::Refused(format!(
                    "{address}: dynamic filter domain carries no envelope"
                ))
            })?;
            feedback.push(
                TaskRuntimeFilterFeedback::parse(envelope).map_err(|error| {
                    DynamicFilterReadError::Refused(format!("{address}: {error}"))
                })?,
            );
        }
        Ok(DynamicFilterRead { version, feedback })
    }
}

/// Binds one frozen Native result transport to the query application's sole
/// fetch/decode/ACK pump. The binding preserves the exact request identity and
/// bounds; it does not create a second polling or acknowledgement authority.
#[allow(
    dead_code,
    reason = "The production coordinator cutover consumes this Native result-pump adapter."
)]
pub(crate) fn native_root_result_pump_binding(
    decode_runtime: RootResultDecodeRuntime,
    transport: Arc<NativeTaskResultTransport>,
    expected_output_schema: ChunkSchemaRef,
) -> RootResultPumpBinding {
    RootResultPumpBinding::new(decode_runtime, move |request| {
        let transport = Arc::clone(&transport);
        let expected_output_schema = Arc::clone(&expected_output_schema);
        async move {
            let outcome = transport
                .fetch_root_result_for_pump(
                    request.root,
                    request.max_wait,
                    request.acknowledged,
                    request.max_result_bytes,
                )
                .await
                .map_err(NativeRootResultFetchError::into_pump_failure)?;
            adapt_native_root_result_outcome(outcome, expected_output_schema)
        }
    })
}

fn adapt_native_root_result_outcome(
    outcome: RootResultOutcome,
    expected_output_schema: ChunkSchemaRef,
) -> Result<PumpRootResultFetchOutcome, RootResultFetchFailure> {
    match outcome {
        RootResultOutcome::Ready(packet) => {
            let sequence = packet.packet_sequence();
            let payload_bytes = packet.payload_bytes();
            let native_bounds = packet.decode_bounds();
            let bounds = RootResultDecodeBounds::new(
                native_bounds.decode_operation_upper_bound(),
                native_bounds.retained_backing_upper_bound(),
            )
            .map_err(|error| {
                RootResultFetchFailure::new(AttemptFailureClass::ContractViolation, error)
            })?;
            PreflightedRootResultPacket::new(sequence, payload_bytes, bounds, move || {
                packet
                    .decode(Some(ExpectedOutputSchemaView::new(&expected_output_schema)))
                    .map(|batch| batch.into_chunk().batch)
                    .map_err(|error| {
                        QueryExecutionError::new(
                            QueryExecutionErrorKind::InvalidRequest,
                            format!("decode Native root result packet failed: {error}"),
                        )
                    })
            })
            .map(PumpRootResultFetchOutcome::Ready)
            .map_err(|error| {
                RootResultFetchFailure::new(AttemptFailureClass::ContractViolation, error)
            })
        }
        RootResultOutcome::NotReady => Ok(PumpRootResultFetchOutcome::NotReady),
        RootResultOutcome::EndOfStreamPending { packet_sequence } => Ok(
            PumpRootResultFetchOutcome::EndPending(ResultPacketSequence::new(packet_sequence)),
        ),
        RootResultOutcome::EndOfStream { packet_sequence } => Ok(
            PumpRootResultFetchOutcome::EndAcknowledged(ResultPacketSequence::new(packet_sequence)),
        ),
        // The wire ERROR variant currently has no discriminator: it can mean
        // an exact-route refusal, a result-buffer protocol failure, or a
        // canceled execution. The accepted Task status projection is the
        // attempt's execution-failure authority, so guessing ExecutionFailure
        // here could make a contract or authorization refusal retryable.
        RootResultOutcome::Failed(detail) => Err(RootResultFetchFailure::new(
            AttemptFailureClass::ContractViolation,
            QueryExecutionError::new(QueryExecutionErrorKind::InvalidRequest, detail),
        )),
    }
}

fn validate_native_result_byte_limit(limit: ResultByteLimit) -> Result<(), String> {
    if limit.get() > MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES {
        return Err(format!(
            "root result byte limit {} exceeds the Native payload limit {}",
            limit.get(),
            MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES
        ));
    }
    Ok(())
}

fn classify_root_result_response(
    address: &str,
    response: novarocks_proto_models::novarocks::FetchResultResponse,
    acknowledged: Option<ResultPacketSequence>,
    max_result_bytes: ResultByteLimit,
) -> Result<RootResultOutcome, String> {
    validate_result_payload_size(address, response.result_arrow_ipc.len(), max_result_bytes)?;
    let status = FetchStatus::try_from(response.status).map_err(|_| {
        format!(
            "{address}: root result poll returned unknown status {}",
            response.status
        )
    })?;
    match status {
        FetchStatus::Ready => {
            // The sequence orders the whole stream, so a value that is not a
            // sequence is refused rather than mapped onto one.
            let packet_sequence = u64::try_from(response.packet_seq).map_err(|_| {
                format!(
                    "{address}: root result packet sequence {} is not a sequence",
                    response.packet_seq
                )
            })?;
            if response.eos {
                if !response.result_arrow_ipc.is_empty() {
                    return Err(format!(
                        "{address}: root result READY end marker carries {} unexpected payload bytes",
                        response.result_arrow_ipc.len()
                    ));
                }
                return Ok(RootResultOutcome::EndOfStreamPending { packet_sequence });
            }
            if response.result_arrow_ipc.is_empty() {
                return Err(format!("{address}: root result READY carries no payload"));
            }
            RawRootResultPacket::try_new(
                ResultPacketSequence::new(packet_sequence),
                response.result_arrow_ipc,
                max_result_bytes,
            )
            .map(RootResultOutcome::Ready)
            .map_err(|error| format!("{address}: {error}"))
        }
        FetchStatus::NotReady => {
            require_empty_root_result_payload(address, "NOT_READY", &response)?;
            if response.packet_seq != 0 || response.eos {
                return Err(format!(
                    "{address}: root result NOT_READY carries terminal fields packet_seq={} eos={}",
                    response.packet_seq, response.eos
                ));
            }
            Ok(RootResultOutcome::NotReady)
        }
        FetchStatus::Error => {
            require_empty_root_result_payload(address, "ERROR", &response)?;
            if response.packet_seq != 0 || response.eos {
                return Err(format!(
                    "{address}: root result ERROR carries terminal fields packet_seq={} eos={}",
                    response.packet_seq, response.eos
                ));
            }
            Ok(RootResultOutcome::Failed(response.message))
        }
        FetchStatus::Eof => {
            require_empty_root_result_payload(address, "EOF", &response)?;
            if !response.eos {
                return Err(format!(
                    "{address}: root result EOF is missing its eos marker"
                ));
            }
            let acknowledged = acknowledged.ok_or_else(|| {
                format!(
                    "{address}: root result poll answered EOF before any packet acknowledgement"
                )
            })?;
            let packet_sequence = u64::try_from(response.packet_seq).map_err(|_| {
                format!(
                    "{address}: root result EOF packet sequence {} is not a sequence",
                    response.packet_seq
                )
            })?;
            if packet_sequence != acknowledged.get() {
                return Err(format!(
                    "{address}: root result EOF sequence {packet_sequence} does not match acknowledged {}",
                    acknowledged.get()
                ));
            }
            Ok(RootResultOutcome::EndOfStream { packet_sequence })
        }
        FetchStatus::ResultStatusUnspecified => Err(format!(
            "{address}: root result poll returned an unspecified status"
        )),
    }
}

fn require_empty_root_result_payload(
    address: &str,
    status: &str,
    response: &novarocks_proto_models::novarocks::FetchResultResponse,
) -> Result<(), String> {
    if response.result_arrow_ipc.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{address}: root result {status} carries {} unexpected payload bytes",
            response.result_arrow_ipc.len()
        ))
    }
}

fn validate_result_payload_size(
    address: &str,
    payload_bytes: usize,
    limit: ResultByteLimit,
) -> Result<(), String> {
    let payload_bytes = u64::try_from(payload_bytes)
        .map_err(|_| format!("{address}: root result payload length is not representable"))?;
    if payload_bytes > limit.get() {
        // Tonic has already applied the process-wide 64 MiB decoded-message
        // ceiling. This check prevents Arrow decode and enforces the request
        // credit, but it is not a per-request allocation reserve.
        return Err(format!(
            "{address}: root result payload has {payload_bytes} bytes, exceeding the requested limit {}",
            limit.get()
        ));
    }
    Ok(())
}

/// Classifies one dynamic filter read failure by type.
///
/// A status that leaves the read unfinished is `Unavailable`, and the next turn
/// asks again. Everything else is the backend having answered that the request
/// is illegal against the task it names, which asking again cannot repair.
fn classify_dynamic_filter_status(address: &str, status: &tonic::Status) -> DynamicFilterReadError {
    let detail = format!(
        "{address}: fetch_task_dynamic_filters rpc failed: {}",
        status.message()
    );
    match status.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::ResourceExhausted => DynamicFilterReadError::Unavailable(detail),
        _ => DynamicFilterReadError::Refused(detail),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{datatypes::Schema, record_batch::RecordBatch};
    use bytes::Bytes;
    use novarocks_execution::{
        exec::chunk::{Chunk, ChunkSchema},
        runtime::exchange::encode_chunks,
        task_execution::ResultByteLimit,
    };
    use novarocks_proto_models::novarocks::{FetchResultResponse, fetch_result_response::Status};
    use novarocks_query_application::coordination::{
        AttemptFailureClass, RootResultFetchOutcome as PumpRootResultFetchOutcome,
    };
    use novarocks_task_codec::operation::MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES;

    use super::{
        NativeRootResultFetchError, RootResultOutcome, adapt_native_root_result_outcome,
        classify_fetch_task_result_rpc_status, classify_root_result_response,
        decode_fetched_query_batch, validate_native_result_byte_limit,
        validate_result_payload_size,
    };

    fn typed_empty_result_payload() -> Vec<u8> {
        let chunk = Chunk::new_with_chunk_schema(
            RecordBatch::new_empty(Arc::new(Schema::empty())),
            Arc::new(ChunkSchema::empty()),
        );
        encode_chunks(&[chunk], true).expect("encode typed root result")
    }

    fn ready_response(payload: impl Into<Bytes>) -> FetchResultResponse {
        FetchResultResponse {
            status: Status::Ready as i32,
            result_arrow_ipc: payload.into(),
            packet_seq: 7,
            ..FetchResultResponse::default()
        }
    }

    #[test]
    fn opaque_fetch_decode_requires_exactly_one_chunk() {
        let Err(error) = decode_fetched_query_batch(&[], None) else {
            panic!("empty payload is not a batch");
        };
        assert!(error.contains("greater than zero"), "actual: {error}");
    }

    #[test]
    fn ready_response_returns_preflighted_raw_packet_without_arrow_decode() {
        let payload = typed_empty_result_payload();
        let limit = ResultByteLimit::new(u64::try_from(payload.len()).unwrap()).unwrap();
        let outcome =
            classify_root_result_response("backend", ready_response(payload.clone()), None, limit)
                .expect("current writer output passes transport preflight");
        let RootResultOutcome::Ready(packet) = outcome else {
            panic!("READY data must remain a raw packet");
        };

        assert_eq!(packet.packet_sequence().get(), 7);
        assert_eq!(
            packet.payload_bytes(),
            u64::try_from(payload.len()).unwrap()
        );
        assert!(packet.decode_bounds().decode_operation_upper_bound() > 0);
    }

    #[test]
    fn ready_response_rejects_malformed_ipc_during_metadata_preflight() {
        let malformed = Bytes::from_static(b"not-an-nrx1-packet");
        let limit = ResultByteLimit::new(u64::try_from(malformed.len()).unwrap()).unwrap();
        let error =
            classify_root_result_response("backend", ready_response(malformed), None, limit)
                .expect_err("malformed READY data must fail before Arrow decode");

        assert!(error.contains("missing NRX1 envelope"), "actual: {error}");
    }

    #[test]
    fn raw_packet_is_consumed_by_one_explicit_decode() {
        let payload = typed_empty_result_payload();
        let limit = ResultByteLimit::new(u64::try_from(payload.len()).unwrap()).unwrap();
        let RootResultOutcome::Ready(packet) =
            classify_root_result_response("backend", ready_response(payload), None, limit)
                .expect("current writer output passes transport preflight")
        else {
            panic!("READY data must remain a raw packet");
        };

        let batch = packet.decode(None).expect("explicit packet decode");
        assert_eq!(batch.into_chunk().len(), 0);
    }

    #[test]
    fn query_adapter_preserves_preflighted_packet_identity_and_bounds() {
        let payload = typed_empty_result_payload();
        let payload_bytes = u64::try_from(payload.len()).unwrap();
        let limit = ResultByteLimit::new(payload_bytes).unwrap();
        let RootResultOutcome::Ready(packet) =
            classify_root_result_response("backend", ready_response(payload), None, limit)
                .expect("current writer output passes transport preflight")
        else {
            panic!("READY data must remain a raw packet");
        };
        let native_bounds = packet.decode_bounds();

        let PumpRootResultFetchOutcome::Ready(packet) = adapt_native_root_result_outcome(
            RootResultOutcome::Ready(packet),
            Arc::new(ChunkSchema::empty()),
        )
        .expect("metadata-preflighted Native packet binds to the query pump") else {
            panic!("READY must remain READY across the application adapter");
        };
        assert_eq!(packet.sequence().get(), 7);
        assert_eq!(packet.payload_bytes(), payload_bytes);
        assert_eq!(
            packet.bounds().decode_operation_upper_bound(),
            native_bounds.decode_operation_upper_bound()
        );
        assert_eq!(
            packet.bounds().retained_backing_upper_bound(),
            native_bounds.retained_backing_upper_bound()
        );
    }

    #[test]
    fn query_adapter_preserves_terminal_sequences_and_failure_class() {
        let schema = Arc::new(ChunkSchema::empty());
        assert!(matches!(
            adapt_native_root_result_outcome(
                RootResultOutcome::EndOfStreamPending { packet_sequence: 9 },
                Arc::clone(&schema),
            )
            .unwrap(),
            PumpRootResultFetchOutcome::EndPending(sequence) if sequence.get() == 9
        ));
        assert!(matches!(
            adapt_native_root_result_outcome(
                RootResultOutcome::EndOfStream { packet_sequence: 9 },
                schema,
            )
            .unwrap(),
            PumpRootResultFetchOutcome::EndAcknowledged(sequence) if sequence.get() == 9
        ));
        let error = match adapt_native_root_result_outcome(
            RootResultOutcome::Failed("root failed".to_string()),
            Arc::new(ChunkSchema::empty()),
        ) {
            Ok(_) => panic!("Native root failure cannot become a pump outcome"),
            Err(error) => error,
        };
        assert_eq!(error.class(), AttemptFailureClass::ContractViolation);
        assert_eq!(error.error().message(), "root failed");
    }

    #[test]
    fn query_adapter_classifies_native_transport_failures_without_guessing() {
        let contract =
            NativeRootResultFetchError::contract("malformed response").into_pump_failure();
        assert_eq!(contract.class(), AttemptFailureClass::ContractViolation);
        let unavailable =
            NativeRootResultFetchError::infrastructure("backend unavailable").into_pump_failure();
        assert_eq!(
            unavailable.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        let capacity = NativeRootResultFetchError::resource_governance("fetch intake closed")
            .into_pump_failure();
        assert_eq!(capacity.class(), AttemptFailureClass::ResourceGovernance);
    }

    #[test]
    fn grpc_status_classification_retries_only_endpoint_unavailability() {
        for code in [tonic::Code::Unavailable, tonic::Code::DeadlineExceeded] {
            let failure = classify_fetch_task_result_rpc_status(tonic::Status::new(code, "lost"))
                .into_pump_failure();
            assert_eq!(
                failure.class(),
                AttemptFailureClass::RecoverableInfrastructure
            );
        }

        let generated_readiness = classify_fetch_task_result_rpc_status(tonic::Status::unknown(
            "Service was not ready: transport error",
        ))
        .into_pump_failure();
        assert_eq!(
            generated_readiness.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        let sourced_transport =
            classify_fetch_task_result_rpc_status(tonic::Status::from_error(Box::new(
                std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset"),
            )))
            .into_pump_failure();
        assert_eq!(
            sourced_transport.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        let remote_unknown = classify_fetch_task_result_rpc_status(tonic::Status::unknown(
            "remote application returned an unknown status",
        ))
        .into_pump_failure();
        assert_eq!(
            remote_unknown.class(),
            AttemptFailureClass::ContractViolation
        );

        let capacity = classify_fetch_task_result_rpc_status(tonic::Status::new(
            tonic::Code::ResourceExhausted,
            "full",
        ))
        .into_pump_failure();
        assert_eq!(capacity.class(), AttemptFailureClass::ResourceGovernance);

        for code in [
            tonic::Code::InvalidArgument,
            tonic::Code::FailedPrecondition,
            tonic::Code::Unauthenticated,
            tonic::Code::PermissionDenied,
            tonic::Code::Internal,
            tonic::Code::Cancelled,
        ] {
            let failure =
                classify_fetch_task_result_rpc_status(tonic::Status::new(code, "refused"))
                    .into_pump_failure();
            assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        }
    }

    #[test]
    fn terminal_result_responses_require_empty_payload_and_exact_ack() {
        let limit = ResultByteLimit::new(1024).unwrap();
        let mut pending = ready_response(Bytes::from_static(b"unexpected"));
        pending.eos = true;
        let error = classify_root_result_response("backend", pending, None, limit)
            .expect_err("an EOS marker cannot discard a data payload");
        assert!(error.contains("unexpected payload bytes"), "{error}");

        let acknowledged = super::ResultPacketSequence::new(7);
        let eof = FetchResultResponse {
            status: Status::Eof as i32,
            packet_seq: 8,
            eos: true,
            ..FetchResultResponse::default()
        };
        let error = classify_root_result_response("backend", eof, Some(acknowledged), limit)
            .expect_err("EOF must echo the exact acknowledged sequence");
        assert!(error.contains("does not match acknowledged 7"), "{error}");

        let eof = FetchResultResponse {
            status: Status::Eof as i32,
            packet_seq: 7,
            eos: true,
            ..FetchResultResponse::default()
        };
        let outcome = classify_root_result_response("backend", eof, Some(acknowledged), limit)
            .expect("exact terminal acknowledgement");
        assert!(matches!(
            outcome,
            RootResultOutcome::EndOfStream { packet_sequence: 7 }
        ));
    }

    #[test]
    fn native_result_limit_must_fit_the_global_decode_envelope() {
        let limit = ResultByteLimit::new(MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES + 1)
            .expect("the oversized limit is still positive");
        assert!(validate_native_result_byte_limit(limit).is_err());
    }

    #[test]
    fn response_payload_is_checked_before_arrow_decode() {
        let limit = ResultByteLimit::new(3).expect("positive limit");
        let error = validate_result_payload_size("backend", 4, limit)
            .expect_err("the payload exceeds its request credit");
        assert!(error.contains("exceeding the requested limit 3"));
    }
}
