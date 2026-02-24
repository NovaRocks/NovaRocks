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
//! Range read helper for native segment bytes.
//!
//! This wrapper centralizes OpenDAL range-read error mapping used by both
//! metadata and segment/page loading paths.
//!
//! Current limitations:
//! - Reads are synchronous from caller perspective (`Runtime::block_on`).

use opendal::{ErrorKind, Operator};

/// Read `[start, end)` bytes from object storage.
pub(crate) fn read_range_bytes(
    rt: &tokio::runtime::Runtime,
    op: &Operator,
    path: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, String> {
    if end <= start {
        return Err(format!(
            "invalid read range for native data loader: path={}, start={}, end={}",
            path, start, end
        ));
    }
    const MAX_READ_ATTEMPTS: usize = 4;
    for attempt in 1..=MAX_READ_ATTEMPTS {
        match rt.block_on(op.read_with(path).range(start..end).into_future()) {
            Ok(v) => return Ok(v.to_vec()),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(format!("segment file not found: {}", path));
            }
            Err(e) if e.is_temporary() && attempt < MAX_READ_ATTEMPTS => {
                let backoff_ms = (100_u64).saturating_mul(1_u64 << (attempt - 1)).min(2_000);
                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
            }
            Err(e) => {
                return Err(format!(
                    "read segment file range failed in native data loader: path={}, range={}..{}, error={}",
                    path, start, end, e
                ));
            }
        }
    }
    Err(format!(
        "read segment file range failed in native data loader after retries: path={}, range={}..{}",
        path, start, end
    ))
}
