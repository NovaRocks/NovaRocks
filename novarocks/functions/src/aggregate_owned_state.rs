// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! A uniquely owned prepared state whose backing survives movement and eviction.
//! Computation stays with the original exact aggregate handle.

use crate::aggregate_state_column::{StateBackingBlock, initialize_owned_slot};
use crate::{
    AggregateEvaluationBatchInvocation, AggregateStateAllocator, AggregateStateSlot,
    EvaluationFailure, KernelEvaluationControl, KernelFailure, PreparedAggregateHandle,
    SelectedAggregateMergeInput,
};
use arrow_array::ArrayRef;
use std::{fmt, sync::Arc};

/// One initialized state with its exact host backing. It is neither Clone nor
/// Sync. Moving the owner never moves the initialized state, and dropping it
/// always destroys that state before returning its block.
pub struct OwnedAggregateState {
    slot: Option<AggregateStateSlot<'static>>,
    backing: StateBackingBlock,
    handle: PreparedAggregateHandle,
    allocator: Arc<dyn AggregateStateAllocator>,
}

// SAFETY: this owner has exclusive custody of the stable backing. Its unique
// typed slot is Send and never exposed; the prepared handle and host are
// Send + Sync. The slot prevents this facade from becoming Sync.
unsafe impl Send for OwnedAggregateState {}

impl fmt::Debug for OwnedAggregateState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedAggregateState")
            .field("state_layout", &self.handle.state_layout())
            .field("initialized", &self.slot.is_some())
            .finish_non_exhaustive()
    }
}

impl OwnedAggregateState {
    pub fn try_new(
        handle: PreparedAggregateHandle,
        allocator: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        let layout = handle.state_layout().pad_to_align();
        let backing = StateBackingBlock::try_new(Arc::clone(&allocator), layout)?;
        // SAFETY: this separate block stays stable while the facade moves.
        // The unique slot is installed immediately and dropped before backing.
        let slot = unsafe {
            initialize_owned_slot(
                &handle,
                backing.pointer(),
                layout.size(),
                Arc::clone(&allocator),
                control,
            )
        }?;
        Ok(Self {
            slot: Some(slot),
            backing,
            handle,
            allocator,
        })
    }

    pub fn handle(&self) -> &PreparedAggregateHandle {
        &self.handle
    }

    /// Actual typed backing, separate from the owner's variable retained heap.
    pub fn backing_bytes(&self) -> usize {
        self.backing.bytes()
    }

    /// Owner-reported retained heap is an observation, not allocation authority.
    pub fn retained_heap_bytes(&self) -> usize {
        self.slot
            .as_ref()
            .expect("owned aggregate state remains initialized")
            .retained_heap_bytes()
    }

    /// Merge selected rows into this one state. Every mapping entry must be
    /// zero; the original handle validates the mapping and exact state input.
    /// The returned frame borrows this owner and cannot survive its eviction.
    pub fn prepare_merge_batch_evaluation<'frame>(
        &'frame mut self,
        mapping: &'frame [usize],
        input: SelectedAggregateMergeInput<'frame, 'frame>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<AggregateEvaluationBatchInvocation<'frame>, EvaluationFailure> {
        let slot = self
            .slot
            .as_mut()
            .expect("owned aggregate state remains initialized");
        self.handle.prepare_merge_batch_evaluation(
            std::slice::from_mut(slot),
            mapping,
            input,
            Some(Arc::clone(&self.allocator)),
            control,
        )
    }

    /// Preserve the prepared phase and its lossless allocator-aware emission.
    pub fn emit_evaluation(
        &self,
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure> {
        let slot = self
            .slot
            .as_ref()
            .expect("owned aggregate state remains initialized");
        self.handle.emit_evaluation_with_allocator(
            std::slice::from_ref(slot),
            &[0],
            row_capacity,
            Some(&self.allocator),
            control,
        )
    }
}

impl Drop for OwnedAggregateState {
    fn drop(&mut self) {
        // No control observation may skip typed destruction. Field teardown
        // returns the backing only after this unique slot has been destroyed.
        drop(self.slot.take());
    }
}
