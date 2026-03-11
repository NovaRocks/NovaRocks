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

use prost::Message;

use crate::common::types::UniqueId;
use crate::service::engine_ffi::NovaRocksRustBuf;

pub use crate::service::grpc_proto as proto;

unsafe extern "C" {
    fn novarocks_compat_transmit_chunk(
        host: *const std::os::raw::c_char,
        port: u16,
        ptr: *const u8,
        len: usize,
        out_resp: *mut NovaRocksRustBuf,
        out_err: *mut NovaRocksRustBuf,
    ) -> i32;
    fn novarocks_compat_transmit_runtime_filter(
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

pub fn send_chunks(
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
            hi: finst_id.hi,
            lo: finst_id.lo,
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
    #[cfg(test)]
    if let Some(result) = maybe_transmit_chunk_hook(dest_host, dest_port, params.clone()) {
        return result.and_then(|resp| status_error(resp.status.as_ref(), "transmit_chunk"));
    }

    let response: proto::starrocks::PTransmitChunkResult = call_unary(
        dest_host,
        dest_port,
        params,
        "transmit_chunk",
        novarocks_compat_transmit_chunk,
    )?;
    status_error(response.status.as_ref(), "transmit_chunk")
}

pub fn transmit_runtime_filter(
    dest_host: &str,
    dest_port: u16,
    params: proto::starrocks::PTransmitRuntimeFilterParams,
) -> Result<(), String> {
    #[cfg(test)]
    if let Some(result) = maybe_transmit_runtime_filter_hook(dest_host, dest_port, params.clone()) {
        return result
            .and_then(|resp| status_error(resp.status.as_ref(), "transmit_runtime_filter"));
    }

    let response: proto::starrocks::PTransmitRuntimeFilterResult = call_unary(
        dest_host,
        dest_port,
        params,
        "transmit_runtime_filter",
        novarocks_compat_transmit_runtime_filter,
    )?;
    status_error(response.status.as_ref(), "transmit_runtime_filter")
}

pub fn lookup(
    dest_host: &str,
    dest_port: u16,
    params: proto::starrocks::PLookUpRequest,
) -> Result<proto::starrocks::PLookUpResponse, String> {
    #[cfg(test)]
    if let Some(result) = maybe_lookup_hook(dest_host, dest_port, params.clone()) {
        return result;
    }

    call_unary(
        dest_host,
        dest_port,
        params,
        "lookup",
        novarocks_compat_lookup,
    )
}

#[cfg(test)]
type TransmitChunkHook = std::sync::Arc<
    dyn Fn(
            &str,
            u16,
            proto::starrocks::PTransmitChunkParams,
        ) -> Result<proto::starrocks::PTransmitChunkResult, String>
        + Send
        + Sync,
>;

#[cfg(test)]
type TransmitRuntimeFilterHook = std::sync::Arc<
    dyn Fn(
            &str,
            u16,
            proto::starrocks::PTransmitRuntimeFilterParams,
        ) -> Result<proto::starrocks::PTransmitRuntimeFilterResult, String>
        + Send
        + Sync,
>;

#[cfg(test)]
type LookupHook = std::sync::Arc<
    dyn Fn(
            &str,
            u16,
            proto::starrocks::PLookUpRequest,
        ) -> Result<proto::starrocks::PLookUpResponse, String>
        + Send
        + Sync,
>;

#[cfg(test)]
fn transmit_chunk_hook() -> &'static std::sync::Mutex<Option<TransmitChunkHook>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<TransmitChunkHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn transmit_runtime_filter_hook() -> &'static std::sync::Mutex<Option<TransmitRuntimeFilterHook>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<TransmitRuntimeFilterHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn lookup_hook() -> &'static std::sync::Mutex<Option<LookupHook>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<LookupHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn test_hook_mutex() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

#[cfg(test)]
fn maybe_transmit_chunk_hook(
    host: &str,
    port: u16,
    params: proto::starrocks::PTransmitChunkParams,
) -> Option<Result<proto::starrocks::PTransmitChunkResult, String>> {
    let hook = transmit_chunk_hook()
        .lock()
        .expect("transmit_chunk hook lock")
        .clone();
    hook.map(|hook| hook(host, port, params))
}

#[cfg(test)]
fn maybe_transmit_runtime_filter_hook(
    host: &str,
    port: u16,
    params: proto::starrocks::PTransmitRuntimeFilterParams,
) -> Option<Result<proto::starrocks::PTransmitRuntimeFilterResult, String>> {
    let hook = transmit_runtime_filter_hook()
        .lock()
        .expect("transmit_runtime_filter hook lock")
        .clone();
    hook.map(|hook| hook(host, port, params))
}

#[cfg(test)]
fn maybe_lookup_hook(
    host: &str,
    port: u16,
    params: proto::starrocks::PLookUpRequest,
) -> Option<Result<proto::starrocks::PLookUpResponse, String>> {
    let hook = lookup_hook().lock().expect("lookup hook lock").clone();
    hook.map(|hook| hook(host, port, params))
}

#[cfg(test)]
pub(crate) fn test_hook_lock() -> std::sync::MutexGuard<'static, ()> {
    test_hook_mutex().lock().expect("test hook global lock")
}

#[cfg(test)]
pub(crate) fn clear_test_hooks() {
    *transmit_chunk_hook()
        .lock()
        .expect("transmit_chunk hook lock") = None;
    *transmit_runtime_filter_hook()
        .lock()
        .expect("transmit_runtime_filter hook lock") = None;
    *lookup_hook().lock().expect("lookup hook lock") = None;
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn set_transmit_chunk_hook<F>(hook: F)
where
    F: Fn(
            &str,
            u16,
            proto::starrocks::PTransmitChunkParams,
        ) -> Result<proto::starrocks::PTransmitChunkResult, String>
        + Send
        + Sync
        + 'static,
{
    *transmit_chunk_hook()
        .lock()
        .expect("transmit_chunk hook lock") = Some(std::sync::Arc::new(hook));
}

#[cfg(test)]
pub(crate) fn set_transmit_runtime_filter_hook<F>(hook: F)
where
    F: Fn(
            &str,
            u16,
            proto::starrocks::PTransmitRuntimeFilterParams,
        ) -> Result<proto::starrocks::PTransmitRuntimeFilterResult, String>
        + Send
        + Sync
        + 'static,
{
    *transmit_runtime_filter_hook()
        .lock()
        .expect("transmit_runtime_filter hook lock") = Some(std::sync::Arc::new(hook));
}

#[cfg(test)]
pub(crate) fn set_lookup_hook<F>(hook: F)
where
    F: Fn(
            &str,
            u16,
            proto::starrocks::PLookUpRequest,
        ) -> Result<proto::starrocks::PLookUpResponse, String>
        + Send
        + Sync
        + 'static,
{
    *lookup_hook().lock().expect("lookup hook lock") = Some(std::sync::Arc::new(hook));
}
