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
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::ptr::NonNull;
use std::sync::Arc;

use crate::runtime::mem_tracker::MemTracker;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RowKey {
    ptr: usize,
    len: usize,
}

impl RowKey {
    pub(crate) fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len) }
    }

    pub(crate) fn empty() -> Self {
        Self {
            ptr: NonNull::<u8>::dangling().as_ptr() as usize,
            len: 0,
        }
    }
}

impl Hash for RowKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(self.as_slice(), state);
    }
}

impl Borrow<[u8]> for RowKey {
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq for RowKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for RowKey {}

pub(crate) struct RowStorage {
    blocks: Vec<Box<[u8]>>,
    cursor: usize,
    block_size: usize,
    mem_tracker: Option<Arc<MemTracker>>,
    accounted_bytes: i64,
}

impl RowStorage {
    pub(crate) fn new(block_size: usize) -> Self {
        Self {
            blocks: Vec::new(),
            cursor: 0,
            block_size: block_size.max(1),
            mem_tracker: None,
            accounted_bytes: 0,
        }
    }

    pub(crate) fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        if let Some(current) = self.mem_tracker.as_ref() {
            if Arc::ptr_eq(current, &tracker) {
                return;
            }
            current.release(self.accounted_bytes);
        }
        let bytes = self.blocks.iter().map(|b| b.len()).sum::<usize>();
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        tracker.consume(bytes);
        self.mem_tracker = Some(tracker);
        self.accounted_bytes = bytes;
    }

    pub(crate) fn alloc_copy(&mut self, bytes: &[u8]) -> RowKey {
        let needed = bytes.len().max(1);
        let current_block_len = self.blocks.last().map(|b| b.len()).unwrap_or(0);
        if self.blocks.is_empty() || self.cursor + needed > current_block_len {
            let block_size = self.block_size.max(needed);
            self.blocks.push(vec![0u8; block_size].into_boxed_slice());
            self.block_size = self.block_size.max(block_size);
            self.cursor = 0;
            if let Some(tracker) = self.mem_tracker.as_ref() {
                let bytes = i64::try_from(block_size).unwrap_or(i64::MAX);
                tracker.consume(bytes);
                self.accounted_bytes = self.accounted_bytes.saturating_add(bytes);
            }
        }
        let block = self.blocks.last_mut().expect("row storage block");
        let start = self.cursor;
        let end = start + bytes.len();
        block[start..end].copy_from_slice(bytes);
        self.cursor = end;
        let ptr = block.as_mut_ptr().wrapping_add(start) as usize;
        RowKey {
            ptr,
            len: bytes.len(),
        }
    }
}

impl Drop for RowStorage {
    fn drop(&mut self) {
        if let Some(tracker) = self.mem_tracker.as_ref() {
            tracker.release(self.accounted_bytes);
        }
    }
}
