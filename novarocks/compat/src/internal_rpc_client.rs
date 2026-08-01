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

use std::ffi::CString;

use novarocks_types::QueryId;
use novarocks_types::UniqueId;
use prost::Message;

use crate::ffi_support::NovaRocksRustBuf;

mod proto {
    pub(crate) use crate::proto::starrocks;
    pub(crate) use ::novarocks::proto::*;
}

unsafe extern "C" {
    fn novarocks_compat_transmit_chunk(
        host: *const std::os::raw::c_char,
        port: u16,
        ptr: *const u8,
        len: usize,
        out_resp: *mut NovaRocksRustBuf,
        out_err: *mut NovaRocksRustBuf,
    ) -> i32;
    fn novarocks_compat_lookup(
        host: *const std::os::raw::c_char,
        port: u16,
        ptr: *const u8,
        len: usize,
        out_resp: *mut NovaRocksRustBuf,
        out_err: *mut NovaRocksRustBuf,
    ) -> i32;
    fn novarocks_compat_lookup_close(
        host: *const std::os::raw::c_char,
        port: u16,
        ptr: *const u8,
        len: usize,
        out_resp: *mut NovaRocksRustBuf,
        out_err: *mut NovaRocksRustBuf,
    ) -> i32;
    fn novarocks_compat_free_buf(ptr: *mut u8, len: usize);
}

type UnaryClientFn = unsafe extern "C" fn(
    host: *const std::os::raw::c_char,
    port: u16,
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32;

fn take_compat_buf(buf: &mut NovaRocksRustBuf) -> Vec<u8> {
    let bytes = if buf.ptr.is_null() || buf.len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(buf.ptr, buf.len) }.to_vec()
    };
    if !buf.ptr.is_null() {
        unsafe { novarocks_compat_free_buf(buf.ptr, buf.len) };
        buf.ptr = std::ptr::null_mut();
        buf.len = 0;
    }
    bytes
}

fn status_error(status: Option<&proto::starrocks::StatusPb>, rpc: &str) -> Result<(), String> {
    let Some(status) = status else {
        return Ok(());
    };
    if status.status_code == 0 {
        return Ok(());
    }
    if status.error_msgs.is_empty() {
        return Err(format!("{rpc} returned status_code={}", status.status_code));
    }
    Err(format!("{rpc} failed: {}", status.error_msgs.join("; ")))
}

fn lookup_close_log_line(result: Result<(), &str>) -> String {
    match result {
        Ok(()) => "compat_rpc method=lookup_close direction=send status=ok".to_string(),
        Err(error) => {
            format!("compat_rpc method=lookup_close direction=send status=error error={error}")
        }
    }
}

fn lookup_close_status_error(status: Option<&proto::starrocks::StatusPb>) -> Result<(), String> {
    let status = status.ok_or_else(|| "lookup_close response missing status".to_string())?;
    status_error(Some(status), "lookup_close")
}

fn call_unary<Request, Response>(
    dest_host: &str,
    dest_port: u16,
    request: Request,
    rpc_name: &str,
    func: UnaryClientFn,
) -> Result<Response, String>
where
    Request: Message,
    Response: Message + Default,
{
    let host = CString::new(dest_host)
        .map_err(|e| format!("{rpc_name} invalid destination host {dest_host:?}: {e}"))?;
    let req_bytes = request.encode_to_vec();
    let mut resp_buf = NovaRocksRustBuf {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    let mut err_buf = NovaRocksRustBuf {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    let rc = unsafe {
        func(
            host.as_ptr(),
            dest_port,
            req_bytes.as_ptr(),
            req_bytes.len(),
            &mut resp_buf,
            &mut err_buf,
        )
    };
    let err_bytes = take_compat_buf(&mut err_buf);
    let resp_bytes = take_compat_buf(&mut resp_buf);
    if rc != 0 {
        let err = String::from_utf8(err_bytes)
            .unwrap_or_else(|_| format!("{rpc_name} returned non-utf8 error"));
        return Err(if err.is_empty() {
            format!("{rpc_name} failed with rc={rc}")
        } else {
            err
        });
    }
    Response::decode(resp_bytes.as_slice())
        .map_err(|e| format!("{rpc_name} decode response failed: {e}"))
}

pub(crate) fn send_chunks(
    dest_host: &str,
    dest_port: u16,
    finst_id: UniqueId,
    node_id: i32,
    sender_id: i32,
    be_number: i32,
    eos: bool,
    sequence: i64,
    payload: Vec<u8>,
) -> Result<(), String> {
    let params = proto::starrocks::PTransmitChunkParams {
        finst_id: Some(proto::starrocks::PUniqueId {
            hi: finst_id.high(),
            lo: finst_id.low(),
        }),
        node_id: Some(node_id),
        sender_id: Some(sender_id),
        be_number: Some(be_number),
        eos: Some(eos),
        sequence: Some(sequence),
        chunks: vec![proto::starrocks::ChunkPb {
            data: Some(payload),
            data_size: Some(0),
            ..Default::default()
        }],
        ..Default::default()
    };
    let response: proto::starrocks::PTransmitChunkResult = call_unary(
        dest_host,
        dest_port,
        params,
        "transmit_chunk",
        novarocks_compat_transmit_chunk,
    )?;
    status_error(response.status.as_ref(), "transmit_chunk")
}

pub(crate) fn lookup(
    dest_host: &str,
    dest_port: u16,
    params: proto::filter::LookupRequest,
) -> Result<proto::filter::LookupResponse, String> {
    let compat_request = proto::starrocks::PLookUpRequest {
        query_id: params
            .query_id
            .as_ref()
            .map(|query_id| proto::starrocks::PUniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            }),
        lookup_node_id: Some(params.lookup_node_id),
        request_tuple_id: Some(params.request_tuple_id),
        request_columns: params
            .request_columns
            .into_iter()
            .map(|col| proto::starrocks::PColumn {
                slot_id: Some(col.slot_id),
                data_size: Some(col.data_size),
                data: Some(col.data),
            })
            .collect(),
        lookup_slots: Vec::new(),
    };

    let response: proto::starrocks::PLookUpResponse = call_unary(
        dest_host,
        dest_port,
        compat_request,
        "lookup",
        novarocks_compat_lookup,
    )?;

    let status = response.status.map(|status| proto::common::Status {
        code: status.status_code,
        message: status.error_msgs.join("; "),
    });
    let mut columns = Vec::with_capacity(response.columns.len());
    for col in response.columns {
        let slot_id = col
            .slot_id
            .ok_or_else(|| "lookup response column missing slot_id".to_string())?;
        let data = col
            .data
            .ok_or_else(|| "lookup response column missing data".to_string())?;
        columns.push(proto::filter::Column {
            slot_id,
            data_size: col.data_size.unwrap_or(data.len() as i64),
            data,
        });
    }
    Ok(proto::filter::LookupResponse { status, columns })
}

pub(crate) fn lookup_close(
    dest_host: &str,
    dest_port: u16,
    query_id: QueryId,
    lookup_node_id: i32,
) -> Result<(), String> {
    let request = proto::starrocks::PLookUpCloseRequest {
        query_id: Some(proto::starrocks::PUniqueId {
            hi: query_id.high(),
            lo: query_id.low(),
        }),
        lookup_node_id: Some(lookup_node_id),
    };
    let response: proto::starrocks::PLookUpCloseResponse = match call_unary(
        dest_host,
        dest_port,
        request,
        "lookup_close",
        novarocks_compat_lookup_close,
    ) {
        Ok(response) => response,
        Err(error) => {
            eprintln!("[WARN] {}", lookup_close_log_line(Err(error.as_str())));
            return Err(error);
        }
    };
    let result = lookup_close_status_error(response.status.as_ref());
    match &result {
        Ok(()) => eprintln!("[INFO] {}", lookup_close_log_line(Ok(()))),
        Err(error) => eprintln!("[WARN] {}", lookup_close_log_line(Err(error.as_str()))),
    }
    result
}

#[cfg(test)]
mod tests {
    use crate::proto::starrocks;

    use super::{lookup_close_log_line, lookup_close_status_error, status_error};
    fn status(status_code: i32, error_msgs: &[&str]) -> starrocks::StatusPb {
        starrocks::StatusPb {
            status_code,
            error_msgs: error_msgs
                .iter()
                .map(|message| (*message).to_string())
                .collect(),
        }
    }

    #[test]
    fn status_error_accepts_missing_and_success_status() {
        assert_eq!(status_error(None, "transmit_chunk"), Ok(()));
        assert_eq!(
            status_error(Some(&status(0, &["ignored"])), "transmit_chunk"),
            Ok(())
        );
    }

    #[test]
    fn status_error_preserves_code_and_messages() {
        assert_eq!(
            status_error(Some(&status(7, &[])), "transmit_chunk"),
            Err("transmit_chunk returned status_code=7".to_string())
        );
        assert_eq!(
            status_error(Some(&status(8, &["first", "second"])), "lookup"),
            Err("lookup failed: first; second".to_string())
        );
    }

    #[test]
    fn lookup_close_requires_status_and_propagates_failure() {
        assert_eq!(
            lookup_close_status_error(None),
            Err("lookup_close response missing status".to_string())
        );
        assert_eq!(
            lookup_close_status_error(Some(&status(9, &["close failed"]))),
            Err("lookup_close failed: close failed".to_string())
        );
    }

    #[test]
    fn lookup_close_log_line_preserves_observable_text() {
        assert_eq!(
            lookup_close_log_line(Ok(())),
            "compat_rpc method=lookup_close direction=send status=ok"
        );
        assert_eq!(
            lookup_close_log_line(Err("network unavailable")),
            "compat_rpc method=lookup_close direction=send status=error error=network unavailable"
        );
    }
}
