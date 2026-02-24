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
//! Runtime membership-filter abstractions.
//!
//! Responsibilities:
//! - Defines polymorphic membership filters including bloom and empty-filter variants.
//! - Provides unified probe interface used by runtime-filter apply paths.
//!
//! Key exported interfaces:
//! - Types: `RuntimeMembershipFilter`, `RuntimeEmptyFilter`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::{filter, filter_record_batch};

use crate::common::ids::SlotId;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

use super::bitset::RuntimeBitsetFilter;
use super::bloom::RuntimeBloomFilter;
use super::min_max::RuntimeMinMaxFilter;

#[derive(Clone, Debug)]
/// Polymorphic runtime membership filter wrapper (bloom/empty/etc.).
pub(crate) enum RuntimeMembershipFilter {
    Bloom(RuntimeBloomFilter),
    Bitset(RuntimeBitsetFilter),
    Empty(RuntimeEmptyFilter),
}

#[derive(Clone, Debug)]
/// Membership filter variant that always rejects rows.
pub(crate) struct RuntimeEmptyFilter {
    filter_id: i32,
    slot_id: SlotId,
    ltype: crate::types::TPrimitiveType,
    has_null: bool,
    join_mode: i8,
    size: u64,
    min_max: RuntimeMinMaxFilter,
}

impl RuntimeEmptyFilter {
    pub(crate) fn new(
        filter_id: i32,
        slot_id: SlotId,
        ltype: crate::types::TPrimitiveType,
        has_null: bool,
        join_mode: i8,
        size: u64,
        min_max: RuntimeMinMaxFilter,
    ) -> Self {
        Self {
            filter_id,
            slot_id,
            ltype,
            has_null,
            join_mode,
            size,
            min_max,
        }
    }

    pub(crate) fn filter_id(&self) -> i32 {
        self.filter_id
    }

    pub(crate) fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub(crate) fn ltype(&self) -> crate::types::TPrimitiveType {
        self.ltype
    }

    pub(crate) fn has_null(&self) -> bool {
        self.has_null
    }

    pub(crate) fn join_mode(&self) -> i8 {
        self.join_mode
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn min_max(&self) -> &RuntimeMinMaxFilter {
        &self.min_max
    }

    #[allow(dead_code)]
    pub(crate) fn min_max_mut(&mut self) -> &mut RuntimeMinMaxFilter {
        &mut self.min_max
    }

    pub(crate) fn set_min_max(&mut self, min_max: RuntimeMinMaxFilter) {
        self.min_max = min_max;
    }

    pub(crate) fn with_slot_id(&self, slot_id: SlotId) -> Self {
        if self.slot_id == slot_id {
            return self.clone();
        }
        Self {
            filter_id: self.filter_id,
            slot_id,
            ltype: self.ltype,
            has_null: self.has_null,
            join_mode: self.join_mode,
            size: self.size,
            min_max: self.min_max.clone(),
        }
    }
}

impl RuntimeMembershipFilter {
    pub(crate) fn filter_id(&self) -> i32 {
        match self {
            RuntimeMembershipFilter::Bloom(filter) => filter.filter_id(),
            RuntimeMembershipFilter::Bitset(filter) => filter.filter_id(),
            RuntimeMembershipFilter::Empty(filter) => filter.filter_id(),
        }
    }

    pub(crate) fn slot_id(&self) -> SlotId {
        match self {
            RuntimeMembershipFilter::Bloom(filter) => filter.slot_id(),
            RuntimeMembershipFilter::Bitset(filter) => filter.slot_id(),
            RuntimeMembershipFilter::Empty(filter) => filter.slot_id(),
        }
    }

    pub(crate) fn ltype(&self) -> crate::types::TPrimitiveType {
        match self {
            RuntimeMembershipFilter::Bloom(filter) => filter.ltype(),
            RuntimeMembershipFilter::Bitset(filter) => filter.ltype(),
            RuntimeMembershipFilter::Empty(filter) => filter.ltype(),
        }
    }

    pub(crate) fn has_null(&self) -> bool {
        match self {
            RuntimeMembershipFilter::Bloom(filter) => filter.has_null(),
            RuntimeMembershipFilter::Bitset(filter) => filter.has_null(),
            RuntimeMembershipFilter::Empty(filter) => filter.has_null(),
        }
    }

    pub(crate) fn join_mode(&self) -> i8 {
        match self {
            RuntimeMembershipFilter::Bloom(filter) => filter.join_mode(),
            RuntimeMembershipFilter::Bitset(filter) => filter.join_mode(),
            RuntimeMembershipFilter::Empty(filter) => filter.join_mode(),
        }
    }

    pub(crate) fn size(&self) -> u64 {
        match self {
            RuntimeMembershipFilter::Bloom(filter) => filter.size(),
            RuntimeMembershipFilter::Bitset(filter) => filter.size(),
            RuntimeMembershipFilter::Empty(filter) => filter.size(),
        }
    }

    pub(crate) fn min_max(&self) -> &RuntimeMinMaxFilter {
        match self {
            RuntimeMembershipFilter::Bloom(filter) => filter.min_max(),
            RuntimeMembershipFilter::Bitset(filter) => filter.min_max(),
            RuntimeMembershipFilter::Empty(filter) => filter.min_max(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn min_max_mut(&mut self) -> &mut RuntimeMinMaxFilter {
        match self {
            RuntimeMembershipFilter::Bloom(filter) => filter.min_max_mut(),
            RuntimeMembershipFilter::Bitset(filter) => filter.min_max_mut(),
            RuntimeMembershipFilter::Empty(filter) => filter.min_max_mut(),
        }
    }

    pub(crate) fn set_min_max(&mut self, min_max: RuntimeMinMaxFilter) {
        match self {
            RuntimeMembershipFilter::Bloom(filter) => filter.set_min_max(min_max),
            RuntimeMembershipFilter::Bitset(filter) => filter.set_min_max(min_max),
            RuntimeMembershipFilter::Empty(filter) => filter.set_min_max(min_max),
        }
    }

    pub(crate) fn with_slot_id(&self, slot_id: SlotId) -> Self {
        match self {
            RuntimeMembershipFilter::Bloom(filter) => {
                RuntimeMembershipFilter::Bloom(filter.with_slot_id(slot_id))
            }
            RuntimeMembershipFilter::Bitset(filter) => {
                RuntimeMembershipFilter::Bitset(filter.with_slot_id(slot_id))
            }
            RuntimeMembershipFilter::Empty(filter) => {
                RuntimeMembershipFilter::Empty(filter.with_slot_id(slot_id))
            }
        }
    }

    pub(crate) fn filter_chunk(&self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        if chunk.is_empty() {
            return Ok(Some(chunk));
        }
        if !chunk.slot_id_to_index().contains_key(&self.slot_id()) {
            return Ok(Some(chunk));
        }
        let array = chunk.column_by_slot_id(self.slot_id())?;
        self.filter_chunk_with_array(&array, chunk)
    }

    pub(crate) fn filter_chunk_with_array(
        &self,
        array: &ArrayRef,
        chunk: Chunk,
    ) -> Result<Option<Chunk>, String> {
        if chunk.is_empty() {
            return Ok(Some(chunk));
        }
        if array.len() != chunk.len() {
            return Err("runtime membership filter array length mismatch".to_string());
        }
        let mut current_chunk = chunk;
        let mut current_array = array.clone();
        let len = current_chunk.len();
        let mut keep = vec![true; len];
        let check_null = matches!(self, RuntimeMembershipFilter::Empty(_));
        self.min_max()
            .apply_to_array(&current_array, self.has_null(), check_null, &mut keep)?;
        if keep.iter().all(|v| !*v) {
            return Ok(None);
        }
        if !keep.iter().all(|v| *v) {
            let mask = BooleanArray::from(keep);
            let filtered_batch =
                filter_record_batch(&current_chunk.batch, &mask).map_err(|e| e.to_string())?;
            let filtered_array = filter(&current_array, &mask).map_err(|e| e.to_string())?;
            current_chunk = Chunk::new(filtered_batch);
            current_array = filtered_array;
        }
        match self {
            RuntimeMembershipFilter::Bloom(filter) => {
                filter.filter_chunk_with_array(&current_array, current_chunk)
            }
            RuntimeMembershipFilter::Bitset(filter) => {
                filter.filter_chunk_with_array(&current_array, current_chunk)
            }
            RuntimeMembershipFilter::Empty(_) => Ok(Some(current_chunk)),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn filter_chunk_with_expr(
        &self,
        arena: &ExprArena,
        expr_id: ExprId,
        chunk: Chunk,
    ) -> Result<Option<Chunk>, String> {
        let array = arena.eval(expr_id, &chunk).map_err(|e| e.to_string())?;
        self.filter_chunk_with_array(&array, chunk)
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            RuntimeMembershipFilter::Bloom(filter) => filter.is_empty(),
            RuntimeMembershipFilter::Bitset(filter) => filter.is_empty(),
            RuntimeMembershipFilter::Empty(_) => true,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn merge_from(&mut self, other: &RuntimeMembershipFilter) -> Result<(), String> {
        match (self, other) {
            (RuntimeMembershipFilter::Bloom(lhs), RuntimeMembershipFilter::Bloom(rhs)) => {
                lhs.merge_from(rhs)
            }
            (RuntimeMembershipFilter::Bitset(lhs), RuntimeMembershipFilter::Bitset(rhs)) => {
                lhs.merge_from(rhs)
            }
            (RuntimeMembershipFilter::Empty(lhs), RuntimeMembershipFilter::Empty(rhs)) => {
                lhs.min_max_mut().merge_from(rhs.min_max())
            }
            _ => Err("runtime membership filter type mismatch".to_string()),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn merge_min_max_from(
        &mut self,
        other: &RuntimeMembershipFilter,
    ) -> Result<(), String> {
        self.min_max_mut().merge_from(other.min_max())
    }

    pub(crate) fn merge_membership_from(
        &mut self,
        other: &RuntimeMembershipFilter,
    ) -> Result<(), String> {
        match (self, other) {
            (RuntimeMembershipFilter::Bloom(lhs), RuntimeMembershipFilter::Bloom(rhs)) => {
                lhs.merge_from(rhs)
            }
            (RuntimeMembershipFilter::Bitset(lhs), RuntimeMembershipFilter::Bitset(rhs)) => {
                lhs.merge_from(rhs)
            }
            (RuntimeMembershipFilter::Empty(_), RuntimeMembershipFilter::Empty(_)) => Ok(()),
            _ => Err("runtime membership filter type mismatch".to_string()),
        }
    }

    pub(crate) fn can_use_for_merge(&self) -> bool {
        let size = self.size();
        match self {
            RuntimeMembershipFilter::Bloom(filter) => size == 0 || filter.can_use_bf(),
            RuntimeMembershipFilter::Bitset(filter) => size == 0 || !filter.is_empty(),
            RuntimeMembershipFilter::Empty(_) => size == 0,
        }
    }

    pub(crate) fn to_empty(&self) -> RuntimeMembershipFilter {
        RuntimeMembershipFilter::Empty(RuntimeEmptyFilter::new(
            self.filter_id(),
            self.slot_id(),
            self.ltype(),
            self.has_null(),
            self.join_mode(),
            self.size(),
            self.min_max().clone(),
        ))
    }
}
