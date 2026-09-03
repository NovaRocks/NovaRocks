//! Backend RPC data-plane capability.
//!
//! This value owns no listener, query lifecycle admission, backend identity, or
//! report policy. Role-owned gRPC services keep their wire gates and delegate
//! only exchange, lookup, typed-result fetch, and runtime-filter delivery here.

use std::sync::atomic::{AtomicUsize, Ordering};

use novarocks_types::UniqueId;

use super::data_plane_handlers;
use super::data_plane_handlers::{ExchangeRouteAuthority, ExchangeRouteClaim, ExchangeRouteQuery};
use crate::query_lifecycle::QueryLifecycleIngress;
use crate::runtime::result_buffer::{TryFetchTypedResult, wait_fetch_typed};
use crate::task_execution::{
    RootResultRoute, StatusAdvance, TaskExecutionRegistry, TaskInboundCapabilities,
};
use novarocks_execution::runtime::fragment::io::{
    ExchangeReceiverPort, UnavailableExchangeReceiverPort,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::task_execution::operation::decode_fetch_task_result;
use novarocks_proto_models as proto;
use std::sync::Arc;

static FETCH_RESULT_CALLS: AtomicUsize = AtomicUsize::new(0);

/// The fragment-based query lifecycle as an exchange-route authority.
///
/// It holds the destinations of every query admitted through `InitQuery`,
/// which is what `EXPLAIN ANALYZE` still runs on.
struct LifecycleRouteAuthority(Arc<dyn QueryLifecycleIngress>);

impl ExchangeRouteAuthority for LifecycleRouteAuthority {
    fn authority_name(&self) -> &'static str {
        "the fragment query lifecycle"
    }

    fn claim_exchange_route(&self, query: ExchangeRouteQuery) -> ExchangeRouteClaim {
        self.0.claim_exchange_route(query)
    }
}

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

    /// Composes the data plane with both exchange-destination owners.
    ///
    /// Both are wired unconditionally. They are not a fallback chain: each
    /// answers only for the destinations it holds, and a frame is admitted
    /// only when exactly one of them claims its destination.
    pub fn with_exchange_receiver_port(
        exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
        query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
        task_inbound_capabilities: Arc<TaskInboundCapabilities>,
    ) -> Self {
        Self {
            exchange_receiver_port,
            exchange_route_authorities: vec![
                Arc::new(LifecycleRouteAuthority(query_lifecycle_ingress)),
                Arc::new(TaskRouteAuthority(task_inbound_capabilities)),
            ],
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

    pub fn lookup(&self, request: proto::filter::LookupRequest) -> proto::filter::LookupResponse {
        data_plane_handlers::handle_lookup(request)
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

        match wait_fetch_typed(finst_id, request.max_wait_ms) {
            TryFetchTypedResult::Ready(result) => {
                emit_typed_fetch_marker(FetchStatus::Ready as i32);
                fetch_response(
                    FetchStatus::Ready,
                    String::new(),
                    result.packet_seq,
                    result.eos,
                    result.payload,
                )
            }
            TryFetchTypedResult::NotReady => {
                emit_typed_fetch_marker(FetchStatus::NotReady as i32);
                fetch_response(FetchStatus::NotReady, String::new(), 0, false, Vec::new())
            }
            TryFetchTypedResult::Error(error) => {
                emit_typed_fetch_marker(FetchStatus::Error as i32);
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
/// The end-of-stream packet is where this stops being a read: delivering it is
/// the moment the root's output responsibility is complete, so the status
/// owner is told before the response leaves. A frontend can then never see the
/// end of its result stream before the status that corroborates it.
pub fn fetch_task_result(
    registry: &TaskExecutionRegistry,
    request: proto::novarocks::FetchTaskResultRequest,
) -> Result<proto::novarocks::FetchResultResponse, tonic::Status> {
    use proto::novarocks::fetch_result_response::Status as FetchStatus;

    let (identity, max_wait) =
        decode_fetch_task_result(&request, FieldPath::root("fetch_task_result"))
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let route = registry.root_result_route(identity);
    let RootResultRoute::Serve(binding) = route else {
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
        emit_typed_fetch_marker(FetchStatus::Error as i32);
        return Ok(fetch_response(
            FetchStatus::Error,
            detail,
            0,
            false,
            Vec::new(),
        ));
    };
    // The decoder already bounds the wait, so this conversion cannot shorten a
    // wait the caller asked for.
    let max_wait_ms = i64::try_from(max_wait.as_millis()).unwrap_or(i64::MAX);
    Ok(match wait_fetch_typed(binding.kernel_key(), max_wait_ms) {
        TryFetchTypedResult::Ready(result) => {
            if result.eos {
                let advance = binding.note_result_stream_drained();
                // A refused advance means this task's status can never
                // corroborate the end of stream about to be returned. Failing
                // the poll is the only answer that does not hand a frontend an
                // unsubstantiated completion.
                if matches!(
                    advance,
                    StatusAdvance::Illegal { .. }
                        | StatusAdvance::Rejected(_)
                        | StatusAdvance::VersionExhausted
                ) {
                    return Err(tonic::Status::internal(format!(
                        "root task {identity} could not record its result drain: {advance:?}"
                    )));
                }
            }
            emit_typed_fetch_marker(FetchStatus::Ready as i32);
            fetch_response(
                FetchStatus::Ready,
                String::new(),
                result.packet_seq,
                result.eos,
                result.payload,
            )
        }
        TryFetchTypedResult::NotReady => {
            emit_typed_fetch_marker(FetchStatus::NotReady as i32);
            fetch_response(FetchStatus::NotReady, String::new(), 0, false, Vec::new())
        }
        TryFetchTypedResult::Error(error) => {
            emit_typed_fetch_marker(FetchStatus::Error as i32);
            fetch_response(FetchStatus::Error, error.message, 0, false, Vec::new())
        }
    })
}

fn fetch_response(
    status: proto::novarocks::fetch_result_response::Status,
    message: String,
    packet_seq: i64,
    eos: bool,
    result_arrow_ipc: Vec<u8>,
) -> proto::novarocks::FetchResultResponse {
    proto::novarocks::FetchResultResponse {
        status: status as i32,
        message,
        packet_seq,
        eos,
        result_arrow_ipc,
    }
}

fn emit_typed_fetch_marker(status: i32) {
    if crate::config::debug_emit_grpc_fragment_marker() {
        println!("NOVAROCKS_GRPC_FETCH_TYPED status={status}");
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}
