//! Role-neutral native RPC data-plane capability.
//!
//! This value owns no listener, query lifecycle admission, backend identity, or
//! report policy. Role-owned gRPC services keep their wire gates and delegate
//! only exchange, lookup, typed-result fetch, and runtime-filter delivery here.

use std::sync::atomic::{AtomicUsize, Ordering};

use novarocks_types::UniqueId;

use crate::proto;
use crate::runtime::result_buffer::{TryFetchTypedResult, wait_fetch_typed};
use crate::service::internal_rpc;

static FETCH_RESULT_CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct NativeDataPlaneKernel {}

impl std::fmt::Debug for NativeDataPlaneKernel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeDataPlaneKernel")
            .finish_non_exhaustive()
    }
}

impl Default for NativeDataPlaneKernel {
    fn default() -> Self {
        Self::query_scoped()
    }
}

impl NativeDataPlaneKernel {
    pub fn query_scoped() -> Self {
        Self {}
    }

    pub fn exchange(
        &self,
        request: proto::novarocks::ExchangeRequest,
    ) -> proto::novarocks::ExchangeResponse {
        internal_rpc::handle_transmit_chunk(request)
    }

    pub fn lookup(&self, request: proto::filter::LookupRequest) -> proto::filter::LookupResponse {
        internal_rpc::handle_lookup(request)
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
        if crate::common::config::debug_fault_inject_fetch_not_ready_count()
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
    if crate::common::config::debug_emit_grpc_fragment_marker() {
        println!("NOVAROCKS_GRPC_FETCH_TYPED status={status}");
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}
