//! Backend RPC data-plane capability.
//!
//! This value owns no listener, query lifecycle admission, backend identity, or
//! report policy. Role-owned gRPC services keep their wire gates and delegate
//! only exchange, lookup, typed-result fetch, and runtime-filter delivery here.

use std::sync::atomic::{AtomicUsize, Ordering};

use novarocks_types::UniqueId;

use super::data_plane_handlers;
use crate::query_lifecycle::QueryLifecycleIngress;
use crate::runtime::result_buffer::{TryFetchTypedResult, wait_fetch_typed};
use crate::task_execution::{RootResultRoute, StatusAdvance, TaskExecutionRegistry};
use novarocks_execution::runtime::fragment::io::{
    ExchangeReceiverPort, UnavailableExchangeReceiverPort,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::task_execution::operation::decode_fetch_task_result;
use novarocks_proto_models as proto;
use std::sync::Arc;

static FETCH_RESULT_CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct BackendDataPlane {
    exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
    query_lifecycle_ingress: Option<Arc<dyn QueryLifecycleIngress>>,
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
            query_lifecycle_ingress: None,
        }
    }

    pub fn with_exchange_receiver_port(
        exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
        query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
    ) -> Self {
        Self {
            exchange_receiver_port,
            query_lifecycle_ingress: Some(query_lifecycle_ingress),
        }
    }

    pub fn exchange(
        &self,
        request: proto::novarocks::ExchangeRequest,
    ) -> proto::novarocks::ExchangeResponse {
        data_plane_handlers::handle_transmit_chunk(
            self.exchange_receiver_port.as_ref(),
            self.query_lifecycle_ingress.as_deref(),
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
