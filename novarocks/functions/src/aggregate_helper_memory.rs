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

//! Original platform-private aggregate helper backing on the actual host.
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use crate::{AggregateStateAllocator, KernelFailure};
use std::{alloc::Layout, sync::Arc};

/// The typed adapter supplies its actual frame Layout. The returned lease
/// lives outside the Box, so its release follows the Box's own deallocation.
pub(crate) fn reserve_frame_backing(
    host: Option<&Arc<dyn AggregateStateAllocator>>,
    layout: Layout,
) -> Result<OpaqueReservation, KernelFailure> {
    let host = host.ok_or_else(|| {
        crate::kernel_control::invalid(
            "aggregate helper memory requires its actual allocation host",
        )
    })?;
    OpaqueRetainedCharge::try_new(Arc::clone(host))?.reserve_operation(layout.size())
}

/// Each observer's original fixed branches can keep at most three newly
/// cloned/replacement diagnostics live at once. Input and escaping causes
/// retain their original GLOBAL responsibility; this lease is temporary.
pub(crate) fn reserve_observation_backing(
    host: Option<&Arc<dyn AggregateStateAllocator>>,
) -> Result<OpaqueReservation, KernelFailure> {
    // Preserve the named rejection for an unsupported platform.
    let mutex_bytes = private_mutex_backing_bytes()?;
    let bytes = crate::MAX_ROW_ERROR_MESSAGE_BYTES
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(mutex_bytes))
        .ok_or(KernelFailure::ResourceExhausted)?;
    let host = host.ok_or_else(|| {
        crate::kernel_control::invalid(
            "aggregate helper memory requires its actual allocation host",
        )
    })?;
    OpaqueRetainedCharge::try_new(Arc::clone(host))?.reserve_operation(bytes)
}

/// Pinned std 1.98.1 uses inline futex storage on Linux. On 64-bit macOS,
/// pthread Mutex owns one Box<pal::Mutex>: vendored libc 0.2.185 defines its
/// payload as c_long plus 56 opaque bytes, size 64 and alignment 8. This fact
/// applies only to exclusive initialization before any handle is published;
/// racing OnceBox initialization can allocate additional losing candidates.
fn private_mutex_backing_bytes() -> Result<usize, KernelFailure> {
    #[cfg(target_os = "linux")]
    {
        Ok(0)
    }
    #[cfg(all(target_os = "macos", target_pointer_width = "64"))]
    {
        Ok(64)
    }
    #[cfg(not(any(
        target_os = "linux",
        all(target_os = "macos", target_pointer_width = "64")
    )))]
    {
        Err(crate::kernel_control::invalid(
            "aggregate helper memory does not support this target Mutex backing",
        ))
    }
}

/// The caller retains this original reservation until the private backing
/// has actually been destroyed. Even a zero-byte platform needs the same
/// actual opaque host capability; it cannot borrow a process fallback.
pub(crate) fn reserve_mutex_backing(
    host: Arc<dyn AggregateStateAllocator>,
) -> Result<OpaqueReservation, KernelFailure> {
    let bytes = private_mutex_backing_bytes()?;
    OpaqueRetainedCharge::try_new(host)?.reserve_operation(bytes)
}
