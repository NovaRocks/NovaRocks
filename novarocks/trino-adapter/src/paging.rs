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

//! One pre-admitted backing, shared with replay and in-flight response bodies.
use bytes::{Buf, Bytes, BytesMut};
use novarocks_result_contract::{BorrowedPayloadSpan, RootProfileV1};
use novarocks_workload_control::ResultWindowAlias;

pub(crate) const PAGE_TARGET: usize = 1024 * 1024;
pub(crate) const PAGE_ENVELOPE_METADATA_BYTES: usize = crate::diagnostics::MAX_METADATA_JSON_BYTES;
pub(crate) const PAGE_ENVELOPE_RESERVE_BYTES: usize =
    crate::diagnostics::MAX_MESSAGE_JSON_BYTES + PAGE_ENVELOPE_METADATA_BYTES;

#[derive(Clone, Copy)]
pub(crate) enum ResultSourceBacking {
    RootSegments,
    Local { retained_bytes: u64 },
}

impl ResultSourceBacking {
    pub(crate) fn retained_bytes(self) -> u64 {
        match self {
            Self::RootSegments => (2 * RootProfileV1::SEGMENT_BYTES) as u64,
            Self::Local { retained_bytes } => retained_bytes,
        }
    }
}

pub(crate) struct PagePipeline {
    buffer: BytesMut,
    complete_bytes: usize,
    complete_rows: usize,
    row_remaining: usize,
    limit: usize,
    pub(crate) window: ResultWindowAlias,
}
impl PagePipeline {
    pub(crate) fn new(
        window: ResultWindowAlias,
        columns_bytes: usize,
        source: ResultSourceBacking,
    ) -> Result<Self, String> {
        let source_bytes = usize::try_from(source.retained_bytes()).map_err(|e| e.to_string())?;
        let reserved = source_bytes
            .checked_add(columns_bytes)
            .and_then(|bytes| bytes.checked_add(2 * PAGE_ENVELOPE_RESERVE_BYTES))
            .ok_or("page source backing overflows")?;
        let total = usize::try_from(window.all_objects_bytes().map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        let limit = total
            .checked_sub(reserved)
            .filter(|p| *p > 0)
            .ok_or("the original result window cannot hold a Trino page")?;
        window
            .check_backing_total(total as u64)
            .map_err(|e| e.to_string())?;
        let buffer = BytesMut::with_capacity(limit);
        if buffer.capacity() > limit {
            return Err("page allocation exceeds the original result window".into());
        }
        Ok(Self {
            buffer,
            complete_bytes: 0,
            complete_rows: 0,
            row_remaining: 0,
            limit,
            window,
        })
    }
    pub(crate) fn ready(&self) -> bool {
        self.complete_bytes >= PAGE_TARGET
    }
    #[cfg(test)]
    pub(crate) fn has_rows(&self) -> bool {
        self.complete_rows != 0
    }
    pub(crate) fn at_row_boundary(&self) -> bool {
        self.row_remaining == 0
    }
    /// Reclaim only after every replay/body alias actually exits. Never grow a shared backing.
    pub(crate) fn reclaim(&mut self) -> bool {
        self.buffer.try_reclaim(self.limit - self.buffer.len())
    }
    pub(crate) fn append(&mut self, span: BorrowedPayloadSpan<'_>) -> Result<(), String> {
        if let Some(length) = span.starts_row {
            if self.row_remaining != 0 {
                return Err("new row starts inside a previous row".into());
            }
            let length = length.get() as usize;
            // A row must fit even when it is the only outstanding payload.
            if length > self.limit {
                return Err("Trino row exceeds the original result window".into());
            }
            let comma = usize::from(!self.buffer.is_empty());
            if self
                .buffer
                .len()
                .checked_add(comma + length)
                .is_none_or(|n| n > self.limit)
            {
                return Err("page pipeline needs acknowledgement before this row".into());
            }
            if comma != 0 {
                self.buffer.extend_from_slice(b",");
            }
            self.row_remaining = length;
        }
        if span.bytes.len() > self.row_remaining
            || self.buffer.len() + span.bytes.len() > self.limit
        {
            return Err("invalid bounded Trino row span".into());
        }
        self.buffer.extend_from_slice(span.bytes);
        self.row_remaining -= span.bytes.len();
        if span.completes_row {
            if self.row_remaining != 0 {
                return Err("row ended before its frozen length".into());
            }
            self.complete_bytes = self.buffer.len();
            self.complete_rows += 1;
        }
        Ok(())
    }
    pub(crate) fn can_start(&self, bytes: usize) -> bool {
        self.buffer
            .len()
            .checked_add(bytes + usize::from(!self.buffer.is_empty()))
            .is_some_and(|n| n <= self.limit)
    }
    pub(crate) fn row_limit(&self) -> usize {
        self.limit
    }
    /// Moving disjoint slices does not copy the page or the unfinished row.
    pub(crate) fn take_rows(&mut self) -> Option<(Bytes, usize)> {
        if self.complete_rows == 0 {
            return None;
        }
        let rows = self.buffer.split_to(self.complete_bytes).freeze();
        let count = self.complete_rows;
        self.complete_bytes = 0;
        self.complete_rows = 0;
        if !self.buffer.is_empty() {
            self.buffer.advance(1);
        } // separator before the partial row
        Some((rows, count))
    }
}
