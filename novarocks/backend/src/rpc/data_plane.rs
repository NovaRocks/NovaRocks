//! Backend RPC data-plane capability.
//!
//! This value owns no listener, query lifecycle admission, backend identity, or
//! report policy. Role-owned gRPC services keep their wire gates and delegate
//! only exchange, lookup, typed-result fetch, and runtime-filter delivery here.

use std::sync::atomic::{AtomicUsize, Ordering};

use novarocks_types::UniqueId;

use super::data_plane_handlers;
use super::data_plane_handlers::{ExchangeRouteAuthority, ExchangeRouteClaim, ExchangeRouteQuery};
use crate::runtime::result_buffer::{
    TryFetchTypedResult, replays_task_terminal_ack, wait_fetch_task_typed, wait_fetch_typed_legacy,
};
use crate::task_execution::{
    RootResultRoute, StatusAdvance, TaskExecutionRegistry, TaskInboundCapabilities,
};
use novarocks_execution::runtime::fragment::io::{
    ExchangeReceiverPort, UnavailableExchangeReceiverPort,
};
use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models as proto;
use novarocks_task_codec::operation::decode_fetch_task_result;
use std::sync::Arc;

static FETCH_RESULT_CALLS: AtomicUsize = AtomicUsize::new(0);

/// The task substrate as an exchange-route authority.
///
/// It holds the destinations of every created task, and its frozen descriptor
/// is the only place a task's inbound topology exists. Without this authority
/// wired, no query on the task protocol can receive an exchange frame at all.
struct TaskRouteAuthority(Arc<TaskInboundCapabilities>);

impl ExchangeRouteAuthority for TaskRouteAuthority {
    fn authority_name(&self) -> &'static str {
        "the task substrate"
    }

    fn claim_exchange_route(&self, query: ExchangeRouteQuery) -> ExchangeRouteClaim {
        self.0.claim_frame(query)
    }
}

#[derive(Clone)]
pub struct BackendDataPlane {
    exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
    /// Every owner of exchange destinations on this backend. One frame is
    /// admitted only when exactly one of them claims its destination.
    exchange_route_authorities: Vec<Arc<dyn ExchangeRouteAuthority>>,
}

impl std::fmt::Debug for BackendDataPlane {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BackendDataPlane")
            .finish_non_exhaustive()
    }
}

impl Default for BackendDataPlane {
    fn default() -> Self {
        Self::query_scoped()
    }
}

impl BackendDataPlane {
    pub fn query_scoped() -> Self {
        Self {
            exchange_receiver_port: Arc::new(UnavailableExchangeReceiverPort),
            exchange_route_authorities: Vec::new(),
        }
    }

    /// Composes the data plane with every exchange-destination owner.
    ///
    /// One owner is wired today, so `settle_exchange_route`'s conflict arm
    /// cannot fire from this composition -- a single authority can produce at
    /// most one claimant. Its zero-claimant refusal stays live and is the
    /// normal answer for a frame naming a destination this backend no longer
    /// holds. The list is still a list because the conflict arm is the reason
    /// this shape exists: whoever wires a second owner here needs it to
    /// already be there, so that two owners claiming one destination is a
    /// refusal rather than a race the wiring order settles.
    pub fn with_exchange_receiver_port(
        exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
        task_inbound_capabilities: Arc<TaskInboundCapabilities>,
    ) -> Self {
        Self {
            exchange_receiver_port,
            exchange_route_authorities: vec![Arc::new(TaskRouteAuthority(
                task_inbound_capabilities,
            ))],
        }
    }

    pub fn exchange(
        &self,
        request: proto::novarocks::ExchangeRequest,
    ) -> proto::novarocks::ExchangeResponse {
        let authorities: Vec<&dyn ExchangeRouteAuthority> = self
            .exchange_route_authorities
            .iter()
            .map(Arc::as_ref)
            .collect();
        data_plane_handlers::handle_transmit_chunk(
            self.exchange_receiver_port.as_ref(),
            &authorities,
            request,
        )
    }

    pub fn fetch_result(
        &self,
        request: proto::novarocks::FetchResultRequest,
    ) -> proto::novarocks::FetchResultResponse {
        use proto::novarocks::fetch_result_response::Status as FetchStatus;

        let Some(finst_id) = request.finst_id else {
            return fetch_response(
                FetchStatus::Error,
                "missing finst_id in FetchResultRequest".to_string(),
                0,
                false,
                Vec::new(),
            );
        };
        let finst_id = UniqueId::new(finst_id.hi, finst_id.lo);
        let call_index = FETCH_RESULT_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
        if crate::config::debug_fault_inject_fetch_not_ready_count()
            .is_some_and(|limit| call_index <= limit)
        {
            return fetch_response(FetchStatus::NotReady, String::new(), 0, false, Vec::new());
        }

        match wait_fetch_typed_legacy(finst_id, request.max_wait_ms) {
            TryFetchTypedResult::Ready(result) => {
                emit_typed_fetch_marker(
                    FetchMarkerIdentity::Fragment(finst_id),
                    FetchStatus::Ready,
                    result.packet_seq,
                    result.eos,
                    result.payload.len(),
                );
                fetch_response(
                    FetchStatus::Ready,
                    String::new(),
                    result.packet_seq,
                    result.eos,
                    result.payload,
                )
            }
            TryFetchTypedResult::NotReady => {
                fetch_response(FetchStatus::NotReady, String::new(), 0, false, Vec::new())
            }
            TryFetchTypedResult::EndAcknowledged => {
                fetch_response(FetchStatus::Eof, String::new(), 0, true, Vec::new())
            }
            TryFetchTypedResult::Error(error) => {
                emit_typed_fetch_marker(
                    FetchMarkerIdentity::Fragment(finst_id),
                    FetchStatus::Error,
                    0,
                    false,
                    0,
                );
                fetch_response(FetchStatus::Error, error.message, 0, false, Vec::new())
            }
        }
    }
}

/// Serves one root result poll addressed by an exact task identity.
///
/// The wire form this replaces addressed a fragment instance, which is a key
/// any participant of any attempt could name. This one is fenced three ways
/// before a buffer is touched — exact task, exact backend process, and result
/// responsibility — and all three refusals are reported in band, because
/// `FetchResultResponse` has an error status and a refusal must not arrive
/// looking like an empty answer.
///
/// The end-of-stream packet remains retained like every data packet. Only a
/// later exact acknowledgement releases it and completes the root's output
/// responsibility; a lost EOS response is therefore replayed byte-for-byte
/// without publishing a completion the frontend has not observed.
pub async fn fetch_task_result(
    registry: &TaskExecutionRegistry,
    request: proto::novarocks::FetchTaskResultRequest,
) -> Result<proto::novarocks::FetchResultResponse, tonic::Status> {
    use proto::novarocks::fetch_result_response::Status as FetchStatus;

    let (identity, max_wait, acknowledged) =
        decode_fetch_task_result(&request, FieldPath::root("fetch_task_result"))
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let acknowledged = acknowledged
        .map(|sequence| {
            i64::try_from(sequence.get()).map_err(|_| {
                tonic::Status::invalid_argument(format!(
                    "acknowledged result packet sequence {} exceeds the wire response range",
                    sequence.get()
                ))
            })
        })
        .transpose()?;
    let route = registry.root_result_route(identity);
    let binding = match route {
        RootResultRoute::Serve(binding) => binding,
        RootResultRoute::TerminalResultOwner(_)
            if replays_task_terminal_ack(identity, acknowledged) =>
        {
            let sequence = acknowledged.expect("an exact terminal replay carries its sequence");
            emit_typed_fetch_marker(
                FetchMarkerIdentity::Task(identity),
                FetchStatus::Eof,
                sequence,
                true,
                0,
            );
            return Ok(fetch_response(
                FetchStatus::Eof,
                String::new(),
                sequence,
                true,
                Vec::new(),
            ));
        }
        route => {
            let detail = route
                .refusal_detail()
                .expect("only a served route has no refusal detail");
            // A refusal fails the frontend's read, and the frontend sees only this
            // text. Recording it here is what attributes it to a backend process
            // and to the route that produced it: a coordinator log alone cannot
            // say which of this backend's task states the poll landed in.
            tracing::warn!(
                task = %identity,
                route = ?route,
                "root result poll refused"
            );
            emit_typed_fetch_marker(
                FetchMarkerIdentity::Task(identity),
                FetchStatus::Error,
                0,
                false,
                0,
            );
            return Ok(fetch_response(
                FetchStatus::Error,
                detail,
                0,
                false,
                Vec::new(),
            ));
        }
    };
    Ok(
        match wait_fetch_task_typed(identity, acknowledged, max_wait).await {
            TryFetchTypedResult::Ready(result) => {
                emit_typed_fetch_marker(
                    FetchMarkerIdentity::Task(identity),
                    FetchStatus::Ready,
                    result.packet_seq,
                    result.eos,
                    result.payload.len(),
                );
                fetch_response(
                    FetchStatus::Ready,
                    String::new(),
                    result.packet_seq,
                    result.eos,
                    result.payload,
                )
            }
            TryFetchTypedResult::EndAcknowledged => {
                let advance = binding.note_result_stream_drained();
                if matches!(
                    advance,
                    StatusAdvance::Illegal { .. }
                        | StatusAdvance::Rejected(_)
                        | StatusAdvance::VersionExhausted
                ) {
                    return Err(tonic::Status::internal(format!(
                        "root task {identity} could not record its acknowledged result drain: {advance:?}"
                    )));
                }
                emit_typed_fetch_marker(
                    FetchMarkerIdentity::Task(identity),
                    FetchStatus::Eof,
                    acknowledged.expect("an acknowledged end carries its packet sequence"),
                    true,
                    0,
                );
                fetch_response(
                    FetchStatus::Eof,
                    String::new(),
                    acknowledged.expect("an acknowledged end carries its packet sequence"),
                    true,
                    Vec::new(),
                )
            }
            TryFetchTypedResult::NotReady => {
                fetch_response(FetchStatus::NotReady, String::new(), 0, false, Vec::new())
            }
            TryFetchTypedResult::Error(error) => {
                emit_typed_fetch_marker(
                    FetchMarkerIdentity::Task(identity),
                    FetchStatus::Error,
                    0,
                    false,
                    0,
                );
                fetch_response(FetchStatus::Error, error.message, 0, false, Vec::new())
            }
        },
    )
}

fn fetch_response(
    status: proto::novarocks::fetch_result_response::Status,
    message: String,
    packet_seq: i64,
    eos: bool,
    result_arrow_ipc: impl Into<bytes::Bytes>,
) -> proto::novarocks::FetchResultResponse {
    proto::novarocks::FetchResultResponse {
        status: status as i32,
        message,
        packet_seq,
        eos,
        result_arrow_ipc: result_arrow_ipc.into(),
    }
}

#[derive(Clone, Copy)]
enum FetchMarkerIdentity {
    Fragment(UniqueId),
    Task(TaskIdentity),
}

fn emit_typed_fetch_marker(
    identity: FetchMarkerIdentity,
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: i64,
    eos: bool,
    payload_bytes: usize,
) {
    if crate::config::debug_emit_grpc_fragment_marker()
        && should_emit_typed_fetch_marker(status, packet_seq, eos)
    {
        println!(
            "{}",
            typed_fetch_marker(identity, status, packet_seq, eos, payload_bytes)
        );
    }
}

fn should_emit_typed_fetch_marker(
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: i64,
    eos: bool,
) -> bool {
    use proto::novarocks::fetch_result_response::Status as FetchStatus;

    match status {
        FetchStatus::Ready => packet_seq == 0 || eos,
        FetchStatus::Eof | FetchStatus::Error => true,
        FetchStatus::ResultStatusUnspecified | FetchStatus::NotReady => false,
    }
}

fn typed_fetch_marker(
    identity: FetchMarkerIdentity,
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: i64,
    eos: bool,
    payload_bytes: usize,
) -> String {
    let identity = match identity {
        FetchMarkerIdentity::Fragment(finst_id) => {
            format!("finst_hi={} finst_lo={}", finst_id.high(), finst_id.low())
        }
        FetchMarkerIdentity::Task(identity) => {
            let execution = identity.query_execution_id();
            format!(
                "query_hi={} query_lo={} attempt={} stage={} task={} backend={}",
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                identity.stage_id().get(),
                identity.task_id().get(),
                identity.backend_process_id(),
            )
        }
    };
    format!(
        "NOVAROCKS_GRPC_FETCH_TYPED {identity} status={} packet_seq={packet_seq} eos={eos} payload_bytes={payload_bytes}",
        status as i32,
    )
}

#[cfg(test)]
mod tests {
    use super::{FetchMarkerIdentity, proto, should_emit_typed_fetch_marker, typed_fetch_marker};
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId, UniqueId,
    };
    use proto::novarocks::fetch_result_response::Status as FetchStatus;

    #[test]
    fn typed_fetch_marker_identifies_payload_and_eof_without_contents() {
        assert_eq!(
            typed_fetch_marker(
                FetchMarkerIdentity::Fragment(UniqueId::new(7, 9)),
                FetchStatus::Ready,
                3,
                true,
                41,
            ),
            "NOVAROCKS_GRPC_FETCH_TYPED finst_hi=7 finst_lo=9 status=1 packet_seq=3 eos=true payload_bytes=41"
        );
    }

    #[test]
    fn typed_fetch_marker_keeps_the_complete_task_route_identity() {
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(2).expect("attempt id"))
                .expect("execution id"),
            StageId::new(3).expect("stage id"),
            TaskId::new(4).expect("task id"),
            BackendProcessId::new_v7(),
        );
        let marker = typed_fetch_marker(
            FetchMarkerIdentity::Task(identity),
            FetchStatus::Error,
            0,
            false,
            0,
        );
        assert!(marker.contains("query_hi=7 query_lo=9 attempt=2 stage=3 task=4"));
        assert!(marker.contains(&format!("backend={}", identity.backend_process_id())));
        assert!(!marker.contains("unknown"));
    }

    #[test]
    fn typed_fetch_markers_are_limited_to_first_packet_eof_and_failure() {
        assert!(should_emit_typed_fetch_marker(FetchStatus::Ready, 0, false));
        assert!(should_emit_typed_fetch_marker(FetchStatus::Ready, 9, true));
        assert!(should_emit_typed_fetch_marker(FetchStatus::Error, 0, false));
        assert!(!should_emit_typed_fetch_marker(
            FetchStatus::Ready,
            9,
            false
        ));
        assert!(!should_emit_typed_fetch_marker(
            FetchStatus::NotReady,
            0,
            false
        ));
    }
}
