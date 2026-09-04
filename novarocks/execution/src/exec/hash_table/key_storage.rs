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
use std::alloc::Layout;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::ptr::NonNull;
use std::sync::Arc;

use allocator_api2::alloc::Allocator;

use crate::exec::expr::agg::{AggregateAllocator, AggregateVec};
use crate::runtime::mem_tracker::MemTracker;
#[cfg(test)]
use crate::runtime::mem_tracker::process_mem_tracker;

#[derive(Clone, Copy, Debug)]
pub struct RowKey {
    ptr: usize,
    len: usize,
}

impl RowKey {
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len) }
    }

    pub fn empty() -> Self {
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

struct RowStorageBlock {
    ptr: NonNull<u8>,
    layout: Layout,
    allocator: AggregateAllocator,
}

impl RowStorageBlock {
    fn try_new(size: usize, allocator: AggregateAllocator) -> Result<Self, String> {
        let layout = Layout::array::<u8>(size.max(1))
            .map_err(|error| format!("invalid row-storage block layout: {error}"))?;
        let ptr = allocator
            .allocate_zeroed(layout)
            .map_err(|_| allocator.allocation_error("allocate row-storage block"))?
            .cast::<u8>();
        Ok(Self {
            ptr,
            layout,
            allocator,
        })
    }

    fn len(&self) -> usize {
        self.layout.size()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    fn copy_from(&mut self, start: usize, bytes: &[u8]) {
        debug_assert!(start.saturating_add(bytes.len()) <= self.len());
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.as_mut_ptr().add(start),
                bytes.len(),
            );
        }
    }
}

impl Drop for RowStorageBlock {
    fn drop(&mut self) {
        unsafe { self.allocator.deallocate(self.ptr, self.layout) };
    }
}

// Blocks are exclusively owned and mutated by their driver-local RowStorage.
unsafe impl Send for RowStorageBlock {}
// Published join artifacts only expose immutable RowKey slices after the build
// owner has stopped mutating RowStorage.
unsafe impl Sync for RowStorageBlock {}

pub struct RowStorage {
    blocks: AggregateVec<RowStorageBlock>,
    cursor: usize,
    block_size: usize,
    memory_limit_exceeded: bool,
}

impl RowStorage {
    #[cfg(test)]
    pub fn new(block_size: usize) -> Self {
        Self::new_with_tracker(
            block_size,
            MemTracker::new_child("RowStorageUnbounded", &process_mem_tracker()),
        )
    }

    pub fn new_with_tracker(block_size: usize, tracker: Arc<MemTracker>) -> Self {
        Self {
            blocks: AggregateVec::new_in(AggregateAllocator::new(tracker)),
            cursor: 0,
            block_size: block_size.max(1),
            memory_limit_exceeded: false,
        }
    }

    pub fn alloc_copy(&mut self, bytes: &[u8]) -> Result<RowKey, String> {
        if self.memory_limit_exceeded {
            return Err("row storage memory limit was previously exceeded".to_string());
        }
        let needed = bytes.len().max(1);
        let current_block_len = self.blocks.last().map_or(0, RowStorageBlock::len);
        if self.blocks.is_empty()
            || self
                .cursor
                .checked_add(needed)
                .is_none_or(|end| end > current_block_len)
        {
            let block_size = self.block_size.max(needed);
            let allocator = self.blocks.allocator().clone();
            if self.blocks.try_reserve(1).is_err() {
                self.memory_limit_exceeded = true;
                return Err(allocator.allocation_error("reserve row-storage blocks"));
            }
            let block = match RowStorageBlock::try_new(block_size, allocator) {
                Ok(block) => block,
                Err(error) => {
                    self.memory_limit_exceeded = true;
                    return Err(error);
                }
            };
            self.blocks.push(block);
            self.block_size = self.block_size.max(block_size);
            self.cursor = 0;
        }
        let block = self.blocks.last_mut().expect("row storage block");
        let start = self.cursor;
        let end = start
            .checked_add(bytes.len())
            .ok_or_else(|| "row-storage cursor overflow".to_string())?;
        block.copy_from(start, bytes);
        self.cursor = end;
        let ptr = block.as_mut_ptr().wrapping_add(start) as usize;
        Ok(RowKey {
            ptr,
            len: bytes.len(),
        })
    }

    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.blocks
            .capacity()
            .saturating_mul(std::mem::size_of::<RowStorageBlock>())
            .saturating_add(
                self.blocks
                    .iter()
                    .map(RowStorageBlock::len)
                    .fold(0usize, usize::saturating_add),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::RowStorage;
    use crate::runtime::mem_tracker::MemTracker;

    #[test]
    fn limit_rejects_block_before_allocation() {
        let query = MemTracker::new_root("query");
        query.install_limit_once(1).expect("install limit");

        {
            let mut storage =
                RowStorage::new_with_tracker(32, MemTracker::new_child("RowStorage", &query));
            let error = storage
                .alloc_copy(b"key")
                .expect_err("first block must cross the limit");
            assert!(error.contains("ResourceExhausted"), "{error}");
            assert_eq!(storage.retained_bytes(), 0);
            assert_eq!(query.current(), 0);
            let retry_error = storage
                .alloc_copy(b"second")
                .expect_err("limit failure must be latched");
            assert!(retry_error.contains("previously exceeded"), "{retry_error}");
            assert_eq!(query.current(), 0);
        }

        assert_eq!(query.current(), 0);
    }
}
