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
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;

use super::functions;
use super::*;

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
    pub(super) spec: AggSpec,
    pub(super) state: AggStateDesc,
}

#[derive(Clone, Debug)]
pub struct AggKernelSet {
    pub entries: Vec<AggKernelEntry>,
    pub layout: AggStateLayout,
}

#[derive(Debug)]
pub struct AggStateArena {
    blocks: Vec<Box<[u8]>>,
    cursor: usize,
    block_size: usize,
    mem_tracker: Option<Arc<MemTracker>>,
    accounted_bytes: i64,
}

impl AggStateArena {
    pub fn new(block_size: usize) -> Self {
        Self {
            blocks: Vec::new(),
            cursor: 0,
            block_size: block_size.max(1),
            mem_tracker: None,
            accounted_bytes: 0,
        }
    }

    pub fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
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

    pub fn alloc(&mut self, size: usize, align: usize) -> AggStatePtr {
        let align_mask = align.saturating_sub(1);
        let needed = size.max(1);
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
        let mut cursor = (self.cursor + align_mask) & !align_mask;
        let current_block_len = self.blocks.last().map(|b| b.len()).unwrap_or(0);
        if cursor + needed > current_block_len {
            let block_size = self.block_size.max(needed);
            self.blocks.push(vec![0u8; block_size].into_boxed_slice());
            self.block_size = self.block_size.max(block_size);
            self.cursor = 0;
            cursor = 0;
            if let Some(tracker) = self.mem_tracker.as_ref() {
                let bytes = i64::try_from(block_size).unwrap_or(i64::MAX);
                tracker.consume(bytes);
                self.accounted_bytes = self.accounted_bytes.saturating_add(bytes);
            }
        }
        let block = self.blocks.last_mut().expect("arena block");
        let ptr = unsafe { block.as_mut_ptr().add(cursor) } as usize;
        self.cursor = cursor + needed;
        ptr
    }
}

impl Drop for AggStateArena {
    fn drop(&mut self) {
        if let Some(tracker) = self.mem_tracker.as_ref() {
            tracker.release(self.accounted_bytes);
        }
    }
}

pub fn build_kernel_set(
    functions: &[AggFunction],
    input_types: &[Option<DataType>],
) -> Result<AggKernelSet, String> {
    if input_types.len() != functions.len() {
        return Err("aggregate input type length mismatch".to_string());
    }

    let mut entries = Vec::with_capacity(functions.len());
    let mut descs = Vec::with_capacity(functions.len());
    let mut offset = 0usize;

    for (idx, func) in functions.iter().enumerate() {
        let spec =
            build_spec_from_type(func, input_types[idx].as_ref(), func.input_is_intermediate)?;
        let (size, align) = functions::state_layout_for_kind(&spec.kind);
        let align_mask = align.saturating_sub(1);
        offset = (offset + align_mask) & !align_mask;
        let state = AggStateDesc {
            offset,
            size,
            align,
        };
        offset += size;
        descs.push(state.clone());
        entries.push(AggKernelEntry { spec, state });
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
    pub fn build_input_view<'a>(
        &self,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        functions::build_input_view(&self.spec, array)
    }

    pub fn build_merge_view<'a>(
        &self,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        functions::build_merge_view(&self.spec, array)
    }

    pub fn output_type(&self, output_intermediate: bool) -> DataType {
        if output_intermediate {
            self.spec.intermediate_type.clone()
        } else {
            self.spec.output_type.clone()
        }
    }

    pub fn state_align(&self) -> usize {
        self.state.align
    }

    pub fn init_state(&self, base: AggStatePtr) {
        let ptr = unsafe { (base as *mut u8).add(self.state.offset) };
        functions::init_state(&self.spec, ptr);
    }

    pub fn update_batch(
        &self,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        functions::update_batch(&self.spec, self.state.offset, state_ptrs, input)
    }

    pub fn merge_batch(
        &self,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        functions::merge_batch(&self.spec, self.state.offset, state_ptrs, input)
    }

    pub fn build_array(
        &self,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        functions::build_array(
            &self.spec,
            self.state.offset,
            group_states,
            output_intermediate,
        )
    }

    pub fn drop_state(&self, base: AggStatePtr) {
        let ptr = unsafe { (base as *mut u8).add(self.state.offset) };
        functions::drop_state(&self.spec, ptr);
    }
}
