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
use std::ptr::NonNull;
use std::sync::Arc;

use allocator_api2::alloc::Allocator;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use novarocks_functions::{AggregateBindOptions, AggregateInputBatch, ResolvedAggregateSignature};

use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;

use super::{
    AggregateAllocator, AggregatePrepareContext, AggregateVec, LegacyAggregateBindContext,
    PreparedAggregateKernel, RetainedMemoryPolicy, SealedExecutionFunctionSet,
};

pub type AggStatePtr = usize;

#[derive(Clone, Debug)]
pub struct AggStateDesc {
    pub offset: usize,
    pub size: usize,
    pub align: usize,
}

#[derive(Clone, Debug)]
pub struct AggStateLayout {
    pub total_size: usize,
    pub descs: Vec<AggStateDesc>,
}

#[derive(Clone, Debug)]
pub struct AggKernelEntry {
    selected: ResolvedAggregateSignature,
    kernel: PreparedAggregateKernel,
    pub(super) state: AggStateDesc,
}

#[derive(Clone, Debug)]
pub struct AggKernelSet {
    pub entries: Vec<AggKernelEntry>,
    pub layout: AggStateLayout,
}

struct AggStateBlock {
    ptr: NonNull<u8>,
    layout: Layout,
    allocator: AggregateAllocator,
}

impl AggStateBlock {
    fn try_new(size: usize, align: usize, allocator: AggregateAllocator) -> Result<Self, String> {
        let layout = Layout::from_size_align(size.max(1), align.max(1))
            .map_err(|error| format!("invalid aggregate state block layout: {error}"))?;
        let ptr = allocator
            .allocate_zeroed(layout)
            .map_err(|_| allocator.allocation_error("allocate aggregate state block"))?
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

    fn align(&self) -> usize {
        self.layout.align()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl std::fmt::Debug for AggStateBlock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AggStateBlock")
            .field("len", &self.len())
            .field("align", &self.align())
            .finish_non_exhaustive()
    }
}

impl Drop for AggStateBlock {
    fn drop(&mut self) {
        unsafe { self.allocator.deallocate(self.ptr, self.layout) };
    }
}

// The allocation is exclusively owned by the arena. State access remains
// driver-local and all raw pointers are invalidated when the arena is dropped.
unsafe impl Send for AggStateBlock {}

#[derive(Debug)]
pub struct AggStateArena {
    blocks: Option<AggregateVec<AggStateBlock>>,
    cursor: usize,
    block_size: usize,
    mem_tracker: Option<Arc<MemTracker>>,
    memory_limit_exceeded: bool,
}

impl AggStateArena {
    pub fn new(block_size: usize) -> Self {
        Self {
            blocks: None,
            cursor: 0,
            block_size: block_size.max(1),
            mem_tracker: None,
            memory_limit_exceeded: false,
        }
    }

    pub fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.try_set_mem_tracker(tracker)
            .expect("aggregate state tracker must be bound before allocation");
    }

    pub fn try_set_mem_tracker(&mut self, tracker: Arc<MemTracker>) -> Result<(), String> {
        if let Some(current) = self.mem_tracker.as_ref()
            && Arc::ptr_eq(current, &tracker)
        {
            return Ok(());
        }
        if self
            .blocks
            .as_ref()
            .is_some_and(|blocks| !blocks.is_empty())
        {
            return Err("aggregate state tracker must be bound before allocation".to_string());
        }
        if self.blocks.is_some() {
            return Err("aggregate state tracker cannot be rebound".to_string());
        }
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));
        self.blocks = Some(AggregateVec::new_in(allocator));
        self.mem_tracker = Some(tracker);
        self.memory_limit_exceeded = false;
        Ok(())
    }

    pub fn alloc(&mut self, size: usize, align: usize) -> AggStatePtr {
        self.alloc_inner(size, align)
            .expect("unchecked aggregate arena allocation cannot report a memory limit")
    }

    pub fn try_alloc(&mut self, size: usize, align: usize) -> Result<AggStatePtr, String> {
        self.alloc_inner(size, align)
    }

    pub fn ensure_available(&self) -> Result<(), String> {
        if self.memory_limit_exceeded {
            Err("aggregate state arena memory limit was previously exceeded".to_string())
        } else {
            Ok(())
        }
    }

    fn alloc_inner(&mut self, size: usize, align: usize) -> Result<AggStatePtr, String> {
        self.ensure_available()?;
        if self.blocks.is_none() {
            self.try_set_mem_tracker(MemTracker::new_child(
                "AggStateArenaUnbounded",
                &crate::runtime::mem_tracker::process_mem_tracker(),
            ))?;
        }
        assert!(
            align.is_power_of_two(),
            "aggregate state alignment must be a power of two"
        );
        let align_mask = align.saturating_sub(1);
        let needed = size.max(1);
        let mut cursor = self
            .cursor
            .checked_add(align_mask)
            .expect("aggregate state arena cursor overflow")
            & !align_mask;
        let blocks = self.blocks.as_mut().expect("aggregate arena tracker");
        let current_block_usable = blocks.last().is_some_and(|block| {
            block.align() >= align
                && cursor
                    .checked_add(needed)
                    .is_some_and(|end| end <= block.len())
        });
        if !current_block_usable {
            let block_size = self.block_size.max(needed);
            let allocator = blocks.allocator().clone();
            if blocks.try_reserve(1).is_err() {
                self.memory_limit_exceeded = true;
                return Err(allocator.allocation_error("reserve aggregate state blocks"));
            }
            let block = match AggStateBlock::try_new(block_size, align, allocator) {
                Ok(block) => block,
                Err(error) => {
                    self.memory_limit_exceeded = true;
                    return Err(error);
                }
            };
            blocks.push(block);
            self.block_size = self.block_size.max(block_size);
            self.cursor = 0;
            cursor = 0;
        }
        let block = blocks.last_mut().expect("arena block");
        let ptr = unsafe { block.as_mut_ptr().add(cursor) } as usize;
        self.cursor = cursor
            .checked_add(needed)
            .expect("aggregate state arena cursor overflow");
        Ok(ptr)
    }
}

pub fn build_kernel_set(
    function_set: &SealedExecutionFunctionSet,
    functions: &[AggFunction],
    input_types: &[Option<DataType>],
    selected_signatures: &[ResolvedAggregateSignature],
) -> Result<AggKernelSet, String> {
    if input_types.len() != functions.len() || selected_signatures.len() != functions.len() {
        return Err(format!(
            "aggregate kernel binding length mismatch: functions={} input_types={} resolved_signatures={}",
            functions.len(),
            input_types.len(),
            selected_signatures.len()
        ));
    }

    let mut entries = Vec::with_capacity(functions.len());
    let mut descs = Vec::with_capacity(functions.len());
    let mut offset = 0usize;

    for (idx, func) in functions.iter().enumerate() {
        let selected = &selected_signatures[idx];
        let options = AggregateBindOptions::try_new(
            func.order.is_distinct,
            &func.order.is_asc_order,
            &func.order.nulls_first,
            func.order.group_concat_max_len,
        )
        .map_err(|error| format!("bind aggregate `{}` options: {error}", func.name))?;
        let context = AggregatePrepareContext {
            selected,
            options: &options,
            legacy: Some(LegacyAggregateBindContext {
                function: func,
                evaluated_input_type: input_types[idx].as_ref(),
                input_is_intermediate: func.input_is_intermediate,
            }),
        };
        let kernel = function_set
            .prepare_resolved_aggregate(&func.name, &context)
            .map_err(|error| format!("bind aggregate `{}`: {error}", func.name))?;
        let layout = kernel.state_layout();
        let size = layout.size();
        let align = layout.align();
        let align_mask = align.saturating_sub(1);
        offset = offset
            .checked_add(align_mask)
            .ok_or_else(|| "aggregate state layout offset overflow".to_string())?
            & !align_mask;
        let state = AggStateDesc {
            offset,
            size,
            align,
        };
        offset = offset
            .checked_add(size)
            .ok_or_else(|| "aggregate state layout size overflow".to_string())?;
        descs.push(state.clone());
        entries.push(AggKernelEntry {
            selected: selected.clone(),
            kernel,
            state,
        });
    }

    Ok(AggKernelSet {
        entries,
        layout: AggStateLayout {
            total_size: offset.max(1),
            descs,
        },
    })
}

impl AggKernelEntry {
    pub fn output_type(&self, output_intermediate: bool) -> DataType {
        if output_intermediate {
            self.selected.intermediate_type.clone()
        } else {
            self.selected.output_type.clone()
        }
    }

    pub fn state_align(&self) -> usize {
        self.state.align
    }

    pub(crate) fn retained_memory_policy(&self) -> RetainedMemoryPolicy {
        self.kernel.retained_memory_policy()
    }

    pub fn init_state(&self, base: AggStatePtr) -> Result<(), String> {
        // SAFETY: the arena allocated this entry's exact layout and this call
        // is made once before the state becomes visible to an operator.
        unsafe { self.kernel.init_state(base, self.state.offset) }
            .map_err(|error| error.to_string())
    }

    pub(crate) fn init_state_with_tracker(
        &self,
        base: AggStatePtr,
        tracker: Arc<MemTracker>,
    ) -> Result<(), String> {
        // SAFETY: identical to `init_state`; the tracker is installed before
        // an allocation-tracked state can construct any heap owner.
        unsafe {
            self.kernel
                .init_state_with_tracker(base, self.state.offset, tracker)
        }
        .map_err(|error| error.to_string())
    }

    pub fn update_batch(
        &self,
        state_ptrs: &[AggStatePtr],
        input: AggregateInputBatch<'_>,
    ) -> Result<(), String> {
        // SAFETY: each pointer denotes an initialized state for this entry.
        // Repeated pointers are supported by the erased adapter, which borrows
        // one state at a time.
        unsafe {
            self.kernel
                .update_batch(self.state.offset, state_ptrs, input)
        }
        .map_err(|error| error.to_string())
    }

    pub fn merge_batch(
        &self,
        state_ptrs: &[AggStatePtr],
        input: AggregateInputBatch<'_>,
    ) -> Result<(), String> {
        // SAFETY: see `update_batch`.
        unsafe {
            self.kernel
                .merge_batch(self.state.offset, state_ptrs, input)
        }
        .map_err(|error| error.to_string())
    }

    pub fn build_array(
        &self,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        // SAFETY: all group pointers remain initialized for the duration of
        // output materialization.
        let result = unsafe {
            if output_intermediate {
                self.kernel
                    .build_intermediate(self.state.offset, group_states)
            } else {
                self.kernel.build_final(self.state.offset, group_states)
            }
        };
        result.map_err(|error| error.to_string())
    }

    pub fn drop_state(&self, base: AggStatePtr) {
        // SAFETY: the operator calls this exactly once for every successfully
        // initialized state.
        unsafe { self.kernel.drop_state(base, self.state.offset) };
    }

    pub fn retained_bytes(&self, base: AggStatePtr) -> usize {
        // SAFETY: the state remains initialized while retained bytes are read.
        unsafe { self.kernel.retained_bytes(base, self.state.offset) }
    }
}

#[cfg(test)]
mod tests {
    use super::AggStateArena;
    use crate::runtime::mem_tracker::MemTracker;

    #[test]
    fn arena_honors_each_requested_state_alignment() {
        let mut arena = AggStateArena::new(31);
        for align in [1, 2, 4, 8, 16, 32, 64, 128] {
            let ptr = arena.alloc(3, align);
            assert_eq!(ptr % align, 0, "alignment {align}");
        }
    }

    #[test]
    fn arena_accounts_block_payload_and_block_vector_capacity() {
        let tracker = MemTracker::new_root("query");
        {
            let mut arena = AggStateArena::new(31);
            arena
                .try_set_mem_tracker(MemTracker::new_child("arena", &tracker))
                .expect("bind tracker");
            arena.try_alloc(3, 8).expect("allocate state");
            assert!(tracker.current() > 31);
        }
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn arena_rejects_late_tracker_binding() {
        let tracker = MemTracker::new_root("query");
        let mut arena = AggStateArena::new(31);
        arena.alloc(3, 8);
        let error = arena
            .try_set_mem_tracker(tracker)
            .expect_err("late binding must fail");
        assert!(error.contains("before allocation"), "{error}");
    }

    #[test]
    fn arena_limit_crossing_is_latched_and_released() {
        let tracker = MemTracker::new_root("query");
        tracker.install_limit_once(1).expect("install limit");
        {
            let mut arena = AggStateArena::new(31);
            arena
                .try_set_mem_tracker(MemTracker::new_child("arena", &tracker))
                .expect("bind tracker");
            let error = arena
                .try_alloc(3, 8)
                .expect_err("allocation must cross the limit");
            assert!(error.contains("ResourceExhausted"), "{error}");
            assert_eq!(tracker.current(), 0);
            let retry = arena
                .try_alloc(3, 8)
                .expect_err("limit crossing must latch the arena");
            assert!(retry.contains("previously exceeded"), "{retry}");
            assert_eq!(tracker.current(), 0);
        }
        assert_eq!(tracker.current(), 0);
    }
}
