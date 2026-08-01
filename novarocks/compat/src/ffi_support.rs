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
//
//! StarRocks-facing Rust C ABI adapters.
//!
//! The core runtime exposes only native requests and domain operations.  This
//! module owns the remaining StarRocks protobuf/Thrift conversion, result wire
//! encoding, buffer ownership, and BRPC-facing status mapping.

#![allow(clippy::not_unsafe_ptr_arg_deref)]

use novarocks::FetchResult;
use novarocks::common::failpoint::{self, FailPointMode};
use novarocks::common::result_batch::ResultBatch;
use novarocks::novarocks_logging::error;
use novarocks::service::internal_rpc;
use novarocks_types::QueryId;
use novarocks_types::UniqueId;
use prost::Message;

mod proto {
    pub(crate) use crate::proto::starrocks;
    pub(crate) use ::novarocks::proto::*;
}

#[repr(C)]
pub(crate) struct NovaRocksRustBuf {
    pub(crate) ptr: *mut u8,
    pub(crate) len: usize,
}

const FETCH_OK: i32 = 0;
const FETCH_NOT_FOUND: i32 = 1;
const FETCH_CANCELLED: i32 = 2;
const FETCH_FAILED: i32 = 3;
const FETCH_TIMEOUT: i32 = 4;
const FETCH_NOT_READY: i32 = 4;

fn unique_id(hi: i64, lo: i64) -> UniqueId {
    UniqueId::new(hi, lo)
}

fn result_batch_to_thrift(
    batch: &ResultBatch,
    packet_seq: i64,
) -> crate::thrift::data::TResultBatch {
    crate::thrift::data::TResultBatch::new(
        batch.rows.clone(),
        batch.is_compressed,
        packet_seq,
        batch.statistic_version,
    )
}

fn estimate_result_batch_bytes(batch: &ResultBatch) -> usize {
    let rows_bytes = batch.rows.iter().fold(0usize, |total, row| {
        total.saturating_add(4).saturating_add(row.len())
    });
    let total = 24usize.saturating_add(rows_bytes);
    let total = if batch.statistic_version.is_some() {
        total.saturating_add(7)
    } else {
        total
    };
    total.saturating_add(64)
}

fn thrift_serialize_result_batch(batch: &ResultBatch, packet_seq: i64) -> Vec<u8> {
    use thrift::protocol::{TBinaryOutputProtocol, TSerializable};
    use thrift::transport::{TBufferChannel, TIoChannel};

    let wire_batch = result_batch_to_thrift(batch, packet_seq);
    let channel = TBufferChannel::with_capacity(0, estimate_result_batch_bytes(batch));
    let (_, writer) = channel.split().expect("split TBufferChannel");
    let mut protocol = TBinaryOutputProtocol::new(writer, true);
    wire_batch
        .write_to_out_protocol(&mut protocol)
        .expect("write TResultBatch");
    protocol.transport.write_bytes()
}

pub(crate) fn init_out_buf(out: *mut NovaRocksRustBuf) {
    // SAFETY: callers supply optional writable C ABI output storage.
    unsafe {
        if !out.is_null() {
            (*out).ptr = std::ptr::null_mut();
            (*out).len = 0;
        }
    }
}

pub(crate) fn write_bytes_buf(bytes: Vec<u8>, out: *mut NovaRocksRustBuf) {
    if out.is_null() {
        return;
    }
    let boxed = bytes.into_boxed_slice();
    let len = boxed.len();
    let ptr = Box::into_raw(boxed).cast::<u8>();
    // SAFETY: the caller supplied writable output storage, checked above.
    unsafe {
        (*out).ptr = ptr;
        (*out).len = len;
    }
}

fn write_string_buf(message: String, out: *mut NovaRocksRustBuf) {
    write_bytes_buf(message.into_bytes(), out);
}

fn write_fetch_result(
    result: FetchResult,
    out_packet_seq: *mut i64,
    out_eos: *mut bool,
    out_batch: *mut NovaRocksRustBuf,
) {
    // SAFETY: each non-null pointer is caller-provided writable output storage.
    unsafe {
        if !out_packet_seq.is_null() {
            *out_packet_seq = result.packet_seq;
        }
        if !out_eos.is_null() {
            *out_eos = result.eos;
        }
    }
    init_out_buf(out_batch);
    // Align with StarRocks BE: EOS closes the stream with packet_seq/eos only
    // and does not send an empty TResultBatch attachment.
    if !(result.eos && result.result_batch.rows.is_empty()) {
        write_bytes_buf(
            thrift_serialize_result_batch(&result.result_batch, result.packet_seq),
            out_batch,
        );
    }
}

fn handle_unary_proto_rpc<Request, Response, F>(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
    rpc_name: &str,
    handler: F,
) -> i32
where
    Request: Message + Default,
    Response: Message,
    F: FnOnce(Request) -> Response,
{
    init_out_buf(out_resp);
    init_out_buf(out_err);
    if ptr.is_null() {
        write_string_buf(format!("{rpc_name} request ptr is null"), out_err);
        return 2;
    }

    // SAFETY: a non-null C ABI request pointer refers to `len` readable bytes.
    let request = match Request::decode(unsafe { std::slice::from_raw_parts(ptr, len) }) {
        Ok(request) => request,
        Err(error) => {
            let message = format!("decode {rpc_name} request failed: {error}");
            write_string_buf(message.clone(), out_err);
            error!(target: "novarocks::ffi", error = %message, rpc = rpc_name, "decode failed");
            return 2;
        }
    };
    write_bytes_buf(handler(request).encode_to_vec(), out_resp);
    0
}

fn ok_status() -> proto::starrocks::StatusPb {
    proto::starrocks::StatusPb {
        status_code: 0,
        error_msgs: Vec::new(),
    }
}

fn error_status(message: impl Into<String>) -> proto::starrocks::StatusPb {
    proto::starrocks::StatusPb {
        status_code: 1,
        error_msgs: vec![message.into()],
    }
}

fn compat_status(status: Option<proto::common::Status>) -> proto::starrocks::StatusPb {
    let status = status.unwrap_or_else(|| proto::common::Status {
        code: 1,
        message: "missing native response status".to_string(),
    });
    proto::starrocks::StatusPb {
        status_code: status.code,
        error_msgs: if status.message.is_empty() {
            Vec::new()
        } else {
            vec![status.message]
        },
    }
}

fn handle_transmit_chunk(
    params: proto::starrocks::PTransmitChunkParams,
) -> proto::starrocks::PTransmitChunkResult {
    let mut response = proto::starrocks::PTransmitChunkResult {
        status: Some(ok_status()),
        receive_timestamp: None,
        receiver_post_process_time: None,
    };
    let Some(finst_id) = params.finst_id.as_ref() else {
        response.status = Some(error_status("missing finst_id for transmit_chunk"));
        return response;
    };
    let Some(node_id) = params.node_id else {
        response.status = Some(error_status("missing node_id for transmit_chunk"));
        return response;
    };
    let Some(sender_id) = params.sender_id else {
        response.status = Some(error_status("missing sender_id for transmit_chunk"));
        return response;
    };
    let Some(be_number) = params.be_number else {
        response.status = Some(error_status("missing be_number for transmit_chunk"));
        return response;
    };
    let Some(eos) = params.eos else {
        response.status = Some(error_status("missing eos for transmit_chunk"));
        return response;
    };
    let Some(sequence) = params.sequence else {
        response.status = Some(error_status("missing sequence for transmit_chunk"));
        return response;
    };
    let Some(payload) = params.chunks.first().and_then(|chunk| chunk.data.as_ref()) else {
        response.status = Some(error_status("missing chunks[0].data for transmit_chunk"));
        return response;
    };

    let native = proto::novarocks::ExchangeRequest {
        finst_id_hi: finst_id.hi,
        finst_id_lo: finst_id.lo,
        node_id,
        sender_id,
        be_number,
        eos,
        sequence,
        payload: payload.clone(),
    };
    response.status = Some(compat_status(
        internal_rpc::handle_transmit_chunk(native).status,
    ));
    response
}

fn handle_lookup(req: proto::starrocks::PLookUpRequest) -> proto::starrocks::PLookUpResponse {
    let Some(tuple_id) = req.request_tuple_id else {
        return proto::starrocks::PLookUpResponse {
            status: Some(error_status("missing request_tuple_id for lookup")),
            columns: Vec::new(),
        };
    };
    let mut request_columns = Vec::with_capacity(req.request_columns.len());
    for column in req.request_columns {
        let Some(slot_id) = column.slot_id else {
            return proto::starrocks::PLookUpResponse {
                status: Some(error_status("lookup request column missing slot_id")),
                columns: Vec::new(),
            };
        };
        let data = column.data.unwrap_or_default();
        if data.is_empty() {
            return proto::starrocks::PLookUpResponse {
                status: Some(error_status(format!(
                    "lookup request column {slot_id} missing data"
                ))),
                columns: Vec::new(),
            };
        }
        request_columns.push(proto::filter::Column {
            slot_id,
            data_size: column.data_size.unwrap_or(data.len() as i64),
            data,
        });
    }
    let native = proto::filter::LookupRequest {
        query_id: req
            .query_id
            .as_ref()
            .map(|query_id| proto::common::UniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            }),
        lookup_node_id: req.lookup_node_id.unwrap_or_default(),
        request_tuple_id: tuple_id,
        request_columns,
    };
    let response = internal_rpc::handle_lookup(native);
    proto::starrocks::PLookUpResponse {
        status: Some(compat_status(response.status)),
        columns: response
            .columns
            .into_iter()
            .map(|column| proto::starrocks::PColumn {
                slot_id: Some(column.slot_id),
                data_size: Some(column.data_size),
                data: Some(column.data),
            })
            .collect(),
    }
}

fn handle_lookup_close(
    request: proto::starrocks::PLookUpCloseRequest,
) -> proto::starrocks::PLookUpCloseResponse {
    let Some(query_id) = request.query_id else {
        return proto::starrocks::PLookUpCloseResponse {
            status: Some(error_status("missing query_id for lookup_close")),
        };
    };
    let Some(lookup_node_id) = request.lookup_node_id else {
        return proto::starrocks::PLookUpCloseResponse {
            status: Some(error_status("missing lookup_node_id for lookup_close")),
        };
    };
    let status = match internal_rpc::handle_lookup_close(
        QueryId::new(query_id.hi, query_id.lo),
        lookup_node_id,
    ) {
        Ok(()) => ok_status(),
        Err(error) => error_status(error),
    };
    proto::starrocks::PLookUpCloseResponse {
        status: Some(status),
    }
}

fn handle_update_fail_point_status(
    request: proto::starrocks::PUpdateFailPointStatusRequest,
) -> proto::starrocks::PUpdateFailPointStatusResponse {
    let mut response = proto::starrocks::PUpdateFailPointStatusResponse {
        status: Some(ok_status()),
    };
    let Some(name) = request.fail_point_name.as_deref() else {
        response.status = Some(error_status("missing fail_point_name"));
        return response;
    };
    let Some(trigger_mode) = request.trigger_mode.as_ref() else {
        response.status = Some(error_status("missing trigger_mode"));
        return response;
    };
    let Some(mode) = trigger_mode.mode else {
        response.status = Some(error_status("missing trigger_mode.mode"));
        return response;
    };
    let mode = match proto::starrocks::FailPointTriggerModeType::try_from(mode) {
        Ok(proto::starrocks::FailPointTriggerModeType::Enable) => FailPointMode::Enable,
        Ok(proto::starrocks::FailPointTriggerModeType::Disable) => FailPointMode::Disable,
        Ok(proto::starrocks::FailPointTriggerModeType::ProbabilityEnable) => {
            let Some(probability) = trigger_mode.probability else {
                response.status = Some(error_status("missing trigger_mode.probability"));
                return response;
            };
            FailPointMode::Probability(probability)
        }
        Ok(proto::starrocks::FailPointTriggerModeType::EnableNTimes) => {
            let Some(n_times) = trigger_mode.n_times else {
                response.status = Some(error_status("missing trigger_mode.n_times"));
                return response;
            };
            FailPointMode::EnableNTimes(n_times)
        }
        Err(_) => {
            response.status = Some(error_status(format!("invalid trigger_mode.mode={mode}")));
            return response;
        }
    };
    if let Err(error) = failpoint::update(name, mode) {
        response.status = Some(error_status(error));
    }
    response
}

/// Notify the BRPC shim only when core published new fetch-visible state.
pub(crate) fn notify_fetch_ready(finst_id: UniqueId) {
    #[cfg(test)]
    let _ = finst_id;

    #[cfg(not(test))]
    // SAFETY: the compat artifact links this symbol from the existing C++ shim.
    unsafe {
        unsafe extern "C" {
            fn novarocks_compat_notify_fetch_ready(finst_id_hi: i64, finst_id_lo: i64);
        }
        novarocks_compat_notify_fetch_ready(finst_id.high(), finst_id.low());
    }
}

/// Returns 0 for a ready batch, 1 for missing, 2 for cancelled, 3 for failed,
/// and 4 for timeout.
#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_fetch_result_batch(
    finst_id_hi: i64,
    finst_id_lo: i64,
    out_packet_seq: *mut i64,
    out_eos: *mut bool,
    out_batch: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    init_out_buf(out_err);
    let finst_id = unique_id(finst_id_hi, finst_id_lo);
    let timeout = novarocks::runtime::result_buffer::fetch_wait_timeout(finst_id);
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match novarocks::runtime::result_buffer::try_fetch(finst_id) {
            novarocks::runtime::result_buffer::TryFetchResult::Ready(result) => {
                write_fetch_result(result, out_packet_seq, out_eos, out_batch);
                return FETCH_OK;
            }
            novarocks::runtime::result_buffer::TryFetchResult::NotReady => {
                if std::time::Instant::now() >= deadline {
                    write_string_buf(
                        format!("timeout waiting for result after {timeout:?}"),
                        out_err,
                    );
                    return FETCH_TIMEOUT;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            novarocks::runtime::result_buffer::TryFetchResult::Error(error) => {
                write_string_buf(error.message, out_err);
                return match error.kind {
                    novarocks::runtime::result_buffer::FetchErrorKind::NotFound => FETCH_NOT_FOUND,
                    novarocks::runtime::result_buffer::FetchErrorKind::Cancelled => FETCH_CANCELLED,
                    novarocks::runtime::result_buffer::FetchErrorKind::Failed => FETCH_FAILED,
                };
            }
        }
    }
}

// The current BRPC shim uses the non-blocking fetch entrypoint, but the
// blocking entrypoint remains part of the stable C ABI. Keep its function
// pointer in the artifact so the final linker cannot dead-strip that ABI
// surface merely because this version of the shim does not call it directly.
#[used]
static KEEP_FETCH_RESULT_BATCH_EXPORT: extern "C" fn(
    i64,
    i64,
    *mut i64,
    *mut bool,
    *mut NovaRocksRustBuf,
    *mut NovaRocksRustBuf,
) -> i32 = novarocks_rs_fetch_result_batch;

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_try_fetch_result_batch(
    finst_id_hi: i64,
    finst_id_lo: i64,
    out_packet_seq: *mut i64,
    out_eos: *mut bool,
    out_batch: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    init_out_buf(out_err);
    match novarocks::runtime::result_buffer::try_fetch(unique_id(finst_id_hi, finst_id_lo)) {
        novarocks::runtime::result_buffer::TryFetchResult::Ready(result) => {
            write_fetch_result(result, out_packet_seq, out_eos, out_batch);
            FETCH_OK
        }
        novarocks::runtime::result_buffer::TryFetchResult::NotReady => FETCH_NOT_READY,
        novarocks::runtime::result_buffer::TryFetchResult::Error(error) => {
            write_string_buf(error.message, out_err);
            match error.kind {
                novarocks::runtime::result_buffer::FetchErrorKind::NotFound => FETCH_NOT_FOUND,
                novarocks::runtime::result_buffer::FetchErrorKind::Cancelled => FETCH_CANCELLED,
                novarocks::runtime::result_buffer::FetchErrorKind::Failed => FETCH_FAILED,
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_fetch_wait_timeout_ms(finst_id_hi: i64, finst_id_lo: i64) -> i64 {
    novarocks::runtime::result_buffer::fetch_wait_timeout_ms(unique_id(finst_id_hi, finst_id_lo))
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_transmit_chunk(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    handle_unary_proto_rpc::<
        proto::starrocks::PTransmitChunkParams,
        proto::starrocks::PTransmitChunkResult,
        _,
    >(
        ptr,
        len,
        out_resp,
        out_err,
        "transmit_chunk",
        handle_transmit_chunk,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_lookup(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    handle_unary_proto_rpc::<proto::starrocks::PLookUpRequest, proto::starrocks::PLookUpResponse, _>(
        ptr,
        len,
        out_resp,
        out_err,
        "lookup",
        handle_lookup,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_lookup_close(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    handle_unary_proto_rpc::<
        proto::starrocks::PLookUpCloseRequest,
        proto::starrocks::PLookUpCloseResponse,
        _,
    >(
        ptr,
        len,
        out_resp,
        out_err,
        "lookup_close",
        handle_lookup_close,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_update_fail_point_status(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    handle_unary_proto_rpc::<
        proto::starrocks::PUpdateFailPointStatusRequest,
        proto::starrocks::PUpdateFailPointStatusResponse,
        _,
    >(
        ptr,
        len,
        out_resp,
        out_err,
        "update_fail_point_status",
        handle_update_fail_point_status,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_free_buf(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        // SAFETY: all buffers returned by this module are allocated from a
        // boxed byte slice with precisely this length.
        unsafe { drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len))) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn take_buffer(buffer: NovaRocksRustBuf) -> Vec<u8> {
        if buffer.ptr.is_null() {
            return Vec::new();
        }
        // SAFETY: the test takes ownership of a boxed byte slice allocated by
        // `write_bytes_buf`.
        unsafe { Vec::from_raw_parts(buffer.ptr, buffer.len, buffer.len) }
    }

    #[test]
    fn transmit_adapter_preserves_required_field_error() {
        let response = handle_transmit_chunk(proto::starrocks::PTransmitChunkParams::default());
        assert_eq!(
            response.status.expect("transmit status").error_msgs,
            vec!["missing finst_id for transmit_chunk".to_string()]
        );
    }

    #[test]
    fn ffi_decoder_initializes_outputs_and_preserves_error_text() {
        let mut response = NovaRocksRustBuf {
            ptr: 1usize as *mut u8,
            len: 1,
        };
        let mut error = NovaRocksRustBuf {
            ptr: 1usize as *mut u8,
            len: 1,
        };
        assert_eq!(
            novarocks_rs_lookup(
                std::ptr::null(),
                0,
                std::ptr::from_mut(&mut response),
                std::ptr::from_mut(&mut error),
            ),
            2
        );
        assert!(response.ptr.is_null());
        assert_eq!(
            String::from_utf8(take_buffer(error)).expect("error is utf-8"),
            "lookup request ptr is null"
        );
    }

    #[test]
    fn failpoint_adapter_preserves_missing_field_error() {
        let response = handle_update_fail_point_status(
            proto::starrocks::PUpdateFailPointStatusRequest::default(),
        );
        assert_eq!(
            response.status.expect("failpoint status").error_msgs,
            vec!["missing fail_point_name".to_string()]
        );
    }
}
