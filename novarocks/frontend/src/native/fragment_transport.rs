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

use novarocks_execution::exec::chunk::{Chunk, ChunkSchemaRef};
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::domain::DomainVersion;
use novarocks_execution::task_execution::operation::FetchTaskDynamicFilters;
use novarocks_execution::task_execution::{
    FinalTaskInfo, MaxWait, OperationOutcome, ResultByteLimit, ResultPacketSequence, TaskIdentity,
    TaskOperationId,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks::fetch_result_response::Status as FetchStatus;
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
    Ready {
        packet_sequence: u64,
        batch: FetchedQueryBatch,
    },
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
            Self::Ready {
                packet_sequence, ..
            } => formatter
                .debug_struct("Ready")
                .field("packet_sequence", packet_sequence)
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
        expected_output_schema: Option<ChunkSchemaRef>,
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
}

impl TaskResultTransport for NativeTaskResultTransport {
    fn fetch_root_result(
        &self,
        root_task: TaskIdentity,
        max_wait: MaxWait,
        acknowledged: Option<ResultPacketSequence>,
        max_result_bytes: ResultByteLimit,
        expected_output_schema: Option<ChunkSchemaRef>,
    ) -> Pin<Box<dyn Future<Output = Result<RootResultOutcome, String>> + Send + 'static>> {
        let route = self
            .client_of(root_task)
            .map(|(client, address)| (client.clone(), address));
        let validation = validate_native_result_byte_limit(max_result_bytes);
        let grace = self.grace;
        let data_runtime = self.data_runtime.clone();
        Box::pin(async move {
            let (client, address) = route?;
            validation?;
            let _fetch_permit = data_runtime.acquire_result_fetch().await?;
            let request =
                encode_fetch_task_result(root_task, max_wait, acknowledged, max_result_bytes);
            let wait = max_wait.get();
            let deadline = grace.deadline_for(wait);
            let response = {
                let mut grpc = tokio::time::timeout(deadline, client.grpc_with_channel_error())
                    .await
                    .map_err(|_| {
                        format!(
                            "{address}: root result poll for task {root_task} could not acquire a \
                         channel within {deadline:?}"
                        )
                    })?
                    .map_err(|error| error.to_string())?;
                // Bounded, and the bound names the fact: a poll that outlives the
                // wait it asked for plus the transport's own residence budget is a
                // backend that stopped answering, and reporting that is what keeps
                // it from stopping this attempt's only thread indefinitely.
                tokio::time::timeout(deadline, grpc.fetch_task_result(request))
                    .await
                    .map_err(|_| {
                        format!(
                            "{address}: root result poll for task {root_task} did not answer within \
                         {deadline:?}; it was asked to wait at most {wait:?}"
                        )
                    })?
                    .map(tonic::Response::into_inner)
                    .map_err(|error| format!("fetch_task_result rpc failed: {error}"))
            }?;
            validate_result_payload_size(
                &address,
                response.result_arrow_ipc.len(),
                max_result_bytes,
            )?;
            let status = FetchStatus::try_from(response.status).map_err(|_| {
                format!(
                    "{address}: root result poll returned unknown status {}",
                    response.status
                )
            })?;
            match status {
            FetchStatus::Ready => {
                // The sequence orders the whole stream, so a value that is not
                // a sequence is refused rather than mapped onto one.
                let packet_sequence = u64::try_from(response.packet_seq).map_err(|_| {
                    format!(
                        "{address}: root result packet sequence {} is not a sequence",
                        response.packet_seq
                    )
                })?;
                if response.eos {
                    return Ok(RootResultOutcome::EndOfStreamPending { packet_sequence });
                }
                if response.result_arrow_ipc.is_empty() {
                    return Err(format!("{address}: root result READY carries no payload"));
                }
                decode_fetched_query_batch(
                    &response.result_arrow_ipc,
                    expected_output_schema
                        .as_ref()
                        .map(ExpectedOutputSchemaView::new),
                )
                    .map(|batch| RootResultOutcome::Ready {
                        packet_sequence,
                        batch,
                    })
                    .map_err(|error| format!("{address}: {error}"))
            }
            FetchStatus::NotReady => Ok(RootResultOutcome::NotReady),
            FetchStatus::Error => Ok(RootResultOutcome::Failed(response.message)),
            FetchStatus::Eof => acknowledged
                .map(|sequence| RootResultOutcome::EndOfStream {
                    packet_sequence: sequence.get(),
                })
                .ok_or_else(|| {
                    format!(
                        "{address}: root result poll answered EOF before any packet acknowledgement"
                    )
                }),
            FetchStatus::ResultStatusUnspecified => Err(format!(
                "{address}: root result poll returned an unspecified status"
            )),
            }
        })
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
            let mut grpc = tokio::time::timeout(deadline, client.grpc_with_channel_error())
                .await
                .map_err(|_| {
                    format!(
                        "{address}: final info read for task {identity} could not acquire a \
                         channel within {deadline:?}"
                    )
                })?
                .map_err(|error| error.to_string())?;
            tokio::time::timeout(deadline, grpc.get_final_task_info(request))
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
                let mut grpc = tokio::time::timeout(
                    DYNAMIC_FILTER_READ_TIMEOUT,
                    client.grpc_with_channel_error(),
                )
                .await
                .map_err(|_| {
                    DynamicFilterReadError::Unavailable(format!(
                        "{address}: dynamic filter read could not acquire a channel in time"
                    ))
                })?
                .map_err(|error| DynamicFilterReadError::Unavailable(error.to_string()))?;
                tokio::time::timeout(
                    DYNAMIC_FILTER_READ_TIMEOUT,
                    grpc.fetch_task_dynamic_filters(request),
                )
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
    use novarocks_execution::task_execution::ResultByteLimit;
    use novarocks_task_codec::operation::MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES;

    use super::{
        decode_fetched_query_batch, validate_native_result_byte_limit, validate_result_payload_size,
    };

    #[test]
    fn opaque_fetch_decode_requires_exactly_one_chunk() {
        let Err(error) = decode_fetched_query_batch(&[], None) else {
            panic!("empty payload is not a batch");
        };
        assert_eq!(error, "typed root result decoded 0 chunks, expected 1");
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
