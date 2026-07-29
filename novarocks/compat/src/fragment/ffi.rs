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

use std::ffi::c_void;

use novarocks::common::types::UniqueId;
use novarocks::novarocks_logging::error;

use super::service::CompatFragmentService;

/// # Safety
///
/// `fragment_service_context` must point to a live `CompatFragmentService` owned by the compat
/// application host for the duration of this call. `ptr` must reference `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn novarocks_rs_submit_exec_batch_plan_fragments(
    fragment_service_context: *const c_void,
    ptr: *const u8,
    len: usize,
) -> i32 {
    if fragment_service_context.is_null() || ptr.is_null() {
        return 2;
    }
    let service = unsafe { &*fragment_service_context.cast::<CompatFragmentService>() };
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    match service.submit_exec_batch_plan_fragments(bytes) {
        Ok(_) => 0,
        Err(error) => {
            error!(
                target: "novarocks::ffi",
                error = %error,
                "submit_exec_batch_plan_fragments failed"
            );
            1
        }
    }
}

/// # Safety
///
/// `fragment_service_context` must point to a live `CompatFragmentService` owned by the compat
/// application host for the duration of this call. `ptr` must reference `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn novarocks_rs_submit_exec_plan_fragment(
    fragment_service_context: *const c_void,
    ptr: *const u8,
    len: usize,
) -> i32 {
    if fragment_service_context.is_null() || ptr.is_null() {
        return 2;
    }
    let service = unsafe { &*fragment_service_context.cast::<CompatFragmentService>() };
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    match service.submit_exec_plan_fragment(bytes) {
        Ok(()) => 0,
        Err(error) => {
            error!(
                target: "novarocks::ffi",
                error = %error,
                "submit_exec_plan_fragment failed"
            );
            1
        }
    }
}

/// # Safety
///
/// `fragment_service_context` must point to a live `CompatFragmentService` owned by the compat
/// application host for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn novarocks_rs_cancel(
    fragment_service_context: *const c_void,
    finst_id_hi: i64,
    finst_id_lo: i64,
) -> i32 {
    if fragment_service_context.is_null() {
        return 2;
    }
    let service = unsafe { &*fragment_service_context.cast::<CompatFragmentService>() };
    let finst_id = UniqueId {
        hi: finst_id_hi,
        lo: finst_id_lo,
    };
    service.cancel_fragment(finst_id);
    0
}

#[cfg(test)]
mod tests {
    use super::{
        novarocks_rs_cancel, novarocks_rs_submit_exec_batch_plan_fragments,
        novarocks_rs_submit_exec_plan_fragment,
    };
    use crate::fragment::CompatFragmentService;

    #[test]
    fn fragment_ffi_requires_an_explicit_service_context() {
        let service = CompatFragmentService::new(
            novarocks::runtime::starrocks_fragment_query::StarRocksFragmentQueryRuntime::new(),
            crate::fragment::brpc_exchange_transmitter(),
            crate::fragment::brpc_fragment_lookup_client(),
            crate::fragment::compat_result_writer(),
            crate::fragment::compat_fragment_event_sink(),
        );
        let context = std::ptr::from_ref(&service).cast();
        let malformed_payload = [0_u8];

        assert_eq!(
            unsafe { novarocks_rs_submit_exec_plan_fragment(context, std::ptr::null(), 0) },
            2
        );
        assert_eq!(
            unsafe { novarocks_rs_submit_exec_batch_plan_fragments(context, std::ptr::null(), 0) },
            2
        );
        assert_eq!(
            unsafe {
                novarocks_rs_submit_exec_plan_fragment(
                    std::ptr::null(),
                    malformed_payload.as_ptr(),
                    malformed_payload.len(),
                )
            },
            2
        );
        assert_eq!(
            unsafe {
                novarocks_rs_submit_exec_plan_fragment(
                    context,
                    malformed_payload.as_ptr(),
                    malformed_payload.len(),
                )
            },
            1
        );
        assert_eq!(unsafe { novarocks_rs_cancel(context, 0, 0) }, 0);
    }
}
