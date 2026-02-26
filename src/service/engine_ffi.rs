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
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::common::thrift::thrift_serialize_result_batch;
use crate::connector::starrocks::lake::{
    abort_txn as lake_abort_txn, delete_data as lake_delete_data,
    delete_tablet as lake_delete_tablet, drop_table as lake_drop_table,
    get_tablet_stats as lake_get_tablet_stats, publish_log_version as lake_publish_log_version,
    publish_log_version_batch as lake_publish_log_version_batch,
    publish_version as lake_publish_version, vacuum as lake_vacuum,
};
use crate::novarocks_logging::error;
use crate::service::grpc_client::proto::starrocks::{
    AbortTxnRequest, DeleteDataRequest, DeleteTabletRequest, DropTableRequest,
    PublishLogVersionBatchRequest, PublishLogVersionRequest, PublishVersionRequest,
    TabletStatRequest, VacuumRequest,
};
use crate::{FetchResult, UniqueId};
use prost::Message;

#[repr(C)]
pub struct NovaRocksRustBuf {
    pub ptr: *mut u8,
    pub len: usize,
}

#[repr(C)]
pub struct NovaRocksUniqueId {
    pub hi: i64,
    pub lo: i64,
}

const FETCH_OK: i32 = 0;
const FETCH_NOT_FOUND: i32 = 1;
const FETCH_CANCELLED: i32 = 2;
const FETCH_FAILED: i32 = 3;
const FETCH_TIMEOUT: i32 = 4;

fn unique_id(hi: i64, lo: i64) -> UniqueId {
    UniqueId { hi, lo }
}

fn write_fetch_result(
    mut result: FetchResult,
    out_packet_seq: *mut i64,
    out_eos: *mut bool,
    out_batch: *mut NovaRocksRustBuf,
) {
    unsafe {
        if !out_packet_seq.is_null() {
            *out_packet_seq = result.packet_seq;
        }
        if !out_eos.is_null() {
            *out_eos = result.eos;
        }
        if !out_batch.is_null() {
            result.result_batch.packet_seq = result.packet_seq;
            let bytes = thrift_serialize_result_batch(&result.result_batch);
            let boxed = bytes.into_boxed_slice();
            let len = boxed.len();
            let ptr = Box::into_raw(boxed) as *mut u8;
            *out_batch = NovaRocksRustBuf { ptr, len };
        }
    }
}

fn write_string_buf(message: String, out: *mut NovaRocksRustBuf) {
    unsafe {
        if out.is_null() {
            return;
        }
        let boxed = message.into_bytes().into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut u8;
        *out = NovaRocksRustBuf { ptr, len };
    }
}

fn write_bytes_buf(bytes: Vec<u8>, out: *mut NovaRocksRustBuf) {
    unsafe {
        if out.is_null() {
            return;
        }
        let boxed = bytes.into_boxed_slice();
        let len = boxed.len();
        let ptr = Box::into_raw(boxed) as *mut u8;
        *out = NovaRocksRustBuf { ptr, len };
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_submit_exec_batch_plan_fragments(ptr: *const u8, len: usize) -> i32 {
    if ptr.is_null() {
        return 2;
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    match crate::submit_exec_batch_plan_fragments(bytes) {
        Ok(_) => 0,
        Err(e) => {
            error!(target: "novarocks::ffi", error = %e, "submit_exec_batch_plan_fragments failed");
            1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_submit_exec_plan_fragment(ptr: *const u8, len: usize) -> i32 {
    if ptr.is_null() {
        return 2;
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    match crate::submit_exec_plan_fragment(bytes) {
        Ok(()) => 0,
        Err(e) => {
            error!(target: "novarocks::ffi", error = %e, "submit_exec_plan_fragment failed");
            1
        }
    }
}

/// Returns:
/// - 0: OK (a result batch is returned; may be EOS)
/// - 1: NOT_FOUND
/// - 2: CANCELLED
/// - 3: FAILED
/// - 4: TIMEOUT
#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_fetch_result_batch(
    finst_id_hi: i64,
    finst_id_lo: i64,
    out_packet_seq: *mut i64,
    out_eos: *mut bool,
    out_batch: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    unsafe {
        if !out_err.is_null() {
            (*out_err).ptr = std::ptr::null_mut();
            (*out_err).len = 0;
        }
    }

    let finst_id = unique_id(finst_id_hi, finst_id_lo);
    match crate::runtime::result_buffer::fetch(finst_id) {
        Ok(result) => {
            write_fetch_result(result, out_packet_seq, out_eos, out_batch);
            FETCH_OK
        }
        Err(err) => {
            write_string_buf(err.message, out_err);
            match err.kind {
                crate::runtime::result_buffer::FetchErrorKind::NotFound => FETCH_NOT_FOUND,
                crate::runtime::result_buffer::FetchErrorKind::Cancelled => FETCH_CANCELLED,
                crate::runtime::result_buffer::FetchErrorKind::Failed => FETCH_FAILED,
                crate::runtime::result_buffer::FetchErrorKind::Timeout => FETCH_TIMEOUT,
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_cancel(finst_id_hi: i64, finst_id_lo: i64) -> i32 {
    crate::cancel(unique_id(finst_id_hi, finst_id_lo));
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_lake_publish_version(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    unsafe {
        if !out_resp.is_null() {
            (*out_resp).ptr = std::ptr::null_mut();
            (*out_resp).len = 0;
        }
        if !out_err.is_null() {
            (*out_err).ptr = std::ptr::null_mut();
            (*out_err).len = 0;
        }
    }
    if ptr.is_null() {
        write_string_buf(
            "lake publish_version request ptr is null".to_string(),
            out_err,
        );
        return 2;
    }

    let req_bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let request = match PublishVersionRequest::decode(req_bytes) {
        Ok(v) => v,
        Err(e) => {
            let err = format!("decode lake publish_version request failed: {e}");
            write_string_buf(err.clone(), out_err);
            error!(target: "novarocks::ffi", error = %err, "lake publish_version decode failed");
            return 2;
        }
    };

    match lake_publish_version(&request) {
        Ok(response) => {
            write_bytes_buf(response.encode_to_vec(), out_resp);
            0
        }
        Err(e) => {
            error!(target: "novarocks::ffi", error = %e, "lake publish_version failed");
            write_string_buf(e, out_err);
            1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_lake_publish_log_version(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    unsafe {
        if !out_resp.is_null() {
            (*out_resp).ptr = std::ptr::null_mut();
            (*out_resp).len = 0;
        }
        if !out_err.is_null() {
            (*out_err).ptr = std::ptr::null_mut();
            (*out_err).len = 0;
        }
    }
    if ptr.is_null() {
        write_string_buf(
            "lake publish_log_version request ptr is null".to_string(),
            out_err,
        );
        return 2;
    }

    let req_bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let request = match PublishLogVersionRequest::decode(req_bytes) {
        Ok(v) => v,
        Err(e) => {
            let err = format!("decode lake publish_log_version request failed: {e}");
            write_string_buf(err.clone(), out_err);
            error!(target: "novarocks::ffi", error = %err, "lake publish_log_version decode failed");
            return 2;
        }
    };

    match lake_publish_log_version(&request) {
        Ok(response) => {
            write_bytes_buf(response.encode_to_vec(), out_resp);
            0
        }
        Err(e) => {
            error!(target: "novarocks::ffi", error = %e, "lake publish_log_version failed");
            write_string_buf(e, out_err);
            1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_lake_publish_log_version_batch(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    unsafe {
        if !out_resp.is_null() {
            (*out_resp).ptr = std::ptr::null_mut();
            (*out_resp).len = 0;
        }
        if !out_err.is_null() {
            (*out_err).ptr = std::ptr::null_mut();
            (*out_err).len = 0;
        }
    }
    if ptr.is_null() {
        write_string_buf(
            "lake publish_log_version_batch request ptr is null".to_string(),
            out_err,
        );
        return 2;
    }

    let req_bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let request = match PublishLogVersionBatchRequest::decode(req_bytes) {
        Ok(v) => v,
        Err(e) => {
            let err = format!("decode lake publish_log_version_batch request failed: {e}");
            write_string_buf(err.clone(), out_err);
            error!(target: "novarocks::ffi", error = %err, "lake publish_log_version_batch decode failed");
            return 2;
        }
    };

    match lake_publish_log_version_batch(&request) {
        Ok(response) => {
            write_bytes_buf(response.encode_to_vec(), out_resp);
            0
        }
        Err(e) => {
            error!(target: "novarocks::ffi", error = %e, "lake publish_log_version_batch failed");
            write_string_buf(e, out_err);
            1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_lake_abort_txn(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    unsafe {
        if !out_resp.is_null() {
            (*out_resp).ptr = std::ptr::null_mut();
            (*out_resp).len = 0;
        }
        if !out_err.is_null() {
            (*out_err).ptr = std::ptr::null_mut();
            (*out_err).len = 0;
        }
    }
    if ptr.is_null() {
        write_string_buf("lake abort_txn request ptr is null".to_string(), out_err);
        return 2;
    }

    let req_bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let request = match AbortTxnRequest::decode(req_bytes) {
        Ok(v) => v,
        Err(e) => {
            let err = format!("decode lake abort_txn request failed: {e}");
            write_string_buf(err.clone(), out_err);
            error!(target: "novarocks::ffi", error = %err, "lake abort_txn decode failed");
            return 2;
        }
    };

    match lake_abort_txn(&request) {
        Ok(response) => {
            write_bytes_buf(response.encode_to_vec(), out_resp);
            0
        }
        Err(e) => {
            error!(target: "novarocks::ffi", error = %e, "lake abort_txn failed");
            write_string_buf(e, out_err);
            1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_lake_drop_table(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    unsafe {
        if !out_resp.is_null() {
            (*out_resp).ptr = std::ptr::null_mut();
            (*out_resp).len = 0;
        }
        if !out_err.is_null() {
            (*out_err).ptr = std::ptr::null_mut();
            (*out_err).len = 0;
        }
    }
    if ptr.is_null() {
        write_string_buf("lake drop_table request ptr is null".to_string(), out_err);
        return 2;
    }

    let req_bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let request = match DropTableRequest::decode(req_bytes) {
        Ok(v) => v,
        Err(e) => {
            let err = format!("decode lake drop_table request failed: {e}");
            write_string_buf(err.clone(), out_err);
            error!(target: "novarocks::ffi", error = %err, "lake drop_table decode failed");
            return 2;
        }
    };

    match lake_drop_table(&request) {
        Ok(response) => {
            write_bytes_buf(response.encode_to_vec(), out_resp);
            0
        }
        Err(e) => {
            error!(target: "novarocks::ffi", error = %e, "lake drop_table failed");
            write_string_buf(e, out_err);
            1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_lake_delete_tablet(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    unsafe {
        if !out_resp.is_null() {
            (*out_resp).ptr = std::ptr::null_mut();
            (*out_resp).len = 0;
        }
        if !out_err.is_null() {
            (*out_err).ptr = std::ptr::null_mut();
            (*out_err).len = 0;
        }
    }
    if ptr.is_null() {
        write_string_buf(
            "lake delete_tablet request ptr is null".to_string(),
            out_err,
        );
        return 2;
    }

    let req_bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let request = match DeleteTabletRequest::decode(req_bytes) {
        Ok(v) => v,
        Err(e) => {
            let err = format!("decode lake delete_tablet request failed: {e}");
            write_string_buf(err.clone(), out_err);
            error!(target: "novarocks::ffi", error = %err, "lake delete_tablet decode failed");
            return 2;
        }
    };

    match lake_delete_tablet(&request) {
        Ok(response) => {
            write_bytes_buf(response.encode_to_vec(), out_resp);
            0
        }
        Err(e) => {
            error!(target: "novarocks::ffi", error = %e, "lake delete_tablet failed");
            write_string_buf(e, out_err);
            1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_lake_delete_data(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    unsafe {
        if !out_resp.is_null() {
            (*out_resp).ptr = std::ptr::null_mut();
            (*out_resp).len = 0;
        }
        if !out_err.is_null() {
            (*out_err).ptr = std::ptr::null_mut();
            (*out_err).len = 0;
        }
    }
    if ptr.is_null() {
        write_string_buf("lake delete_data request ptr is null".to_string(), out_err);
        return 2;
    }

    let req_bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let request = match DeleteDataRequest::decode(req_bytes) {
        Ok(v) => v,
        Err(e) => {
            let err = format!("decode lake delete_data request failed: {e}");
            write_string_buf(err.clone(), out_err);
            error!(target: "novarocks::ffi", error = %err, "lake delete_data decode failed");
            return 2;
        }
    };

    match lake_delete_data(&request) {
        Ok(response) => {
            write_bytes_buf(response.encode_to_vec(), out_resp);
            0
        }
        Err(e) => {
            error!(target: "novarocks::ffi", error = %e, "lake delete_data failed");
            write_string_buf(e, out_err);
            1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_lake_get_tablet_stats(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    unsafe {
        if !out_resp.is_null() {
            (*out_resp).ptr = std::ptr::null_mut();
            (*out_resp).len = 0;
        }
        if !out_err.is_null() {
            (*out_err).ptr = std::ptr::null_mut();
            (*out_err).len = 0;
        }
    }
    if ptr.is_null() {
        write_string_buf(
            "lake get_tablet_stats request ptr is null".to_string(),
            out_err,
        );
        return 2;
    }

    let req_bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let request = match TabletStatRequest::decode(req_bytes) {
        Ok(v) => v,
        Err(e) => {
            let err = format!("decode lake get_tablet_stats request failed: {e}");
            write_string_buf(err.clone(), out_err);
            error!(target: "novarocks::ffi", error = %err, "lake get_tablet_stats decode failed");
            return 2;
        }
    };

    match lake_get_tablet_stats(&request) {
        Ok(response) => {
            write_bytes_buf(response.encode_to_vec(), out_resp);
            0
        }
        Err(e) => {
            error!(target: "novarocks::ffi", error = %e, "lake get_tablet_stats failed");
            write_string_buf(e, out_err);
            1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_lake_vacuum(
    ptr: *const u8,
    len: usize,
    out_resp: *mut NovaRocksRustBuf,
    out_err: *mut NovaRocksRustBuf,
) -> i32 {
    unsafe {
        if !out_resp.is_null() {
            (*out_resp).ptr = std::ptr::null_mut();
            (*out_resp).len = 0;
        }
        if !out_err.is_null() {
            (*out_err).ptr = std::ptr::null_mut();
            (*out_err).len = 0;
        }
    }
    if ptr.is_null() {
        write_string_buf("lake vacuum request ptr is null".to_string(), out_err);
        return 2;
    }

    let req_bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    let request = match VacuumRequest::decode(req_bytes) {
        Ok(v) => v,
        Err(e) => {
            let err = format!("decode lake vacuum request failed: {e}");
            write_string_buf(err.clone(), out_err);
            error!(target: "novarocks::ffi", error = %err, "lake vacuum decode failed");
            return 2;
        }
    };

    match lake_vacuum(&request) {
        Ok(response) => {
            write_bytes_buf(response.encode_to_vec(), out_resp);
            0
        }
        Err(e) => {
            error!(target: "novarocks::ffi", error = %e, "lake vacuum failed");
            write_string_buf(e, out_err);
            1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn novarocks_rs_free_buf(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let slice = std::ptr::slice_from_raw_parts_mut(ptr, len);
        drop(Box::from_raw(slice));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ffi_fetch_propagates_close_error() {
        let finst = UniqueId { hi: 10, lo: 20 };
        crate::runtime::result_buffer::create_sender(finst);
        crate::runtime::result_buffer::close_error(finst, "boom".to_string());

        let mut packet_seq: i64 = -1;
        let mut eos: bool = false;
        let mut batch = NovaRocksRustBuf {
            ptr: std::ptr::null_mut(),
            len: 0,
        };
        let mut err = NovaRocksRustBuf {
            ptr: std::ptr::null_mut(),
            len: 0,
        };

        let rc = novarocks_rs_fetch_result_batch(
            finst.hi,
            finst.lo,
            &mut packet_seq,
            &mut eos,
            &mut batch,
            &mut err,
        );
        assert_eq!(rc, FETCH_FAILED);
        assert!(batch.ptr.is_null());
        assert_eq!(batch.len, 0);
        assert!(!err.ptr.is_null());
        assert!(err.len > 0);
        let msg = unsafe { std::slice::from_raw_parts(err.ptr, err.len) };
        assert_eq!(std::str::from_utf8(msg).unwrap(), "boom");
        novarocks_rs_free_buf(err.ptr, err.len);
    }

    #[test]
    fn ffi_fetch_propagates_cancel() {
        let finst = UniqueId { hi: 11, lo: 22 };
        crate::runtime::result_buffer::create_sender(finst);
        crate::runtime::result_buffer::cancel(finst);

        let mut packet_seq: i64 = -1;
        let mut eos: bool = false;
        let mut batch = NovaRocksRustBuf {
            ptr: std::ptr::null_mut(),
            len: 0,
        };
        let mut err = NovaRocksRustBuf {
            ptr: std::ptr::null_mut(),
            len: 0,
        };

        let rc = novarocks_rs_fetch_result_batch(
            finst.hi,
            finst.lo,
            &mut packet_seq,
            &mut eos,
            &mut batch,
            &mut err,
        );
        assert_eq!(rc, FETCH_CANCELLED);
        assert!(batch.ptr.is_null());
        assert_eq!(batch.len, 0);
        assert!(!err.ptr.is_null());
        assert!(err.len > 0);
        novarocks_rs_free_buf(err.ptr, err.len);
    }
}
