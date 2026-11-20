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

//! The published key space holds the immutable index and membership bitmap.
//! Tail reads membership only after the shared probe completion barrier.

use super::contract;
use crate::{
    exec::{
        hash_table::key_index::{BulkKey, BulkWork, IndexView},
        shared_stage::{funded_vec, key_bitmap::KeyBitmap},
    },
    runtime::task::ExecutionResult,
};
use novarocks_functions::KernelEvaluationControl;
use novarocks_local_program::SetOpKind;
use novarocks_type_contract::{MAX_UNOBSERVED_COMPILE_WORK, ValueTypeStorageLifetime};
use std::{ops::Range, sync::Arc};

pub(super) struct KeySpace {
    pub(super) view: Arc<IndexView>,
    ends: Vec<usize>,
    _storage: ValueTypeStorageLifetime,
}

impl KeySpace {
    pub(super) fn new(view: Arc<IndexView>, work: &BulkWork<'_>) -> ExecutionResult<Self> {
        let mut ends = funded_vec(work.owner, view.partition_count())?;
        let mut end = 0usize;
        for partition in 0..view.partition_count() {
            work.control.checkpoint(0)?;
            end = end
                .checked_add(view.key_count(partition)?)
                .ok_or_else(|| contract("set operation key space overflow"))?;
            ends.push(end);
        }
        Ok(Self {
            view,
            ends,
            _storage: work.owner.storage_lifetime(),
        })
    }

    pub(super) fn len(&self) -> usize {
        self.ends.last().copied().unwrap_or(0)
    }

    fn start(&self, partition: usize) -> usize {
        partition
            .checked_sub(1)
            .map_or(0, |previous| self.ends[previous])
    }

    fn ordinal(&self, key: BulkKey) -> ExecutionResult<usize> {
        let partition = key.partition();
        let end = self
            .ends
            .get(partition)
            .copied()
            .ok_or_else(|| contract("set operation probe has no matching partition"))?;
        let start = self.start(partition);
        let id = key.id().index();
        if id >= end - start {
            return Err(contract("set operation probe ID is outside its partition"));
        }
        Ok(start + id)
    }

    /// Split a leased global range at the current partition boundary.
    pub(super) fn partition_prefix(
        &self,
        rows: Range<usize>,
    ) -> ExecutionResult<(usize, usize, Range<usize>)> {
        let partition = self.ends.partition_point(|end| *end <= rows.start);
        let end = self
            .ends
            .get(partition)
            .copied()
            .ok_or_else(|| contract("set operation tail position is outside its key space"))?;
        let start = self.start(partition);
        Ok((
            partition,
            start,
            rows.start - start..rows.end.min(end) - start,
        ))
    }
}

#[derive(Clone, Copy)]
pub(super) struct Membership {
    kind: SetOpKind,
    probes: usize,
}

impl Membership {
    pub(super) fn new(kind: SetOpKind, probes: usize) -> Self {
        Self { kind, probes }
    }

    fn lanes(self) -> usize {
        match self.kind {
            SetOpKind::Intersect => self.probes,
            SetOpKind::Except => 1,
        }
    }

    pub(super) fn len(self, keys: usize) -> ExecutionResult<usize> {
        self.lanes()
            .checked_mul(keys)
            .ok_or_else(|| contract("set operation membership bitmap extent overflow"))
    }
}

pub(super) struct Artifact {
    pub(super) keys: Arc<KeySpace>,
    pub(super) bits: KeyBitmap,
    pub(super) membership: Membership,
}

impl Artifact {
    pub(super) fn mark(&self, probe: usize, key: BulkKey) -> ExecutionResult<()> {
        if probe >= self.membership.probes {
            return Err(contract("set operation probe has no membership lane"));
        }
        let lane = match self.membership.kind {
            SetOpKind::Intersect => probe,
            SetOpKind::Except => 0,
        };
        self.bits
            .mark(lane * self.keys.len() + self.keys.ordinal(key)?)
    }

    pub(super) fn survives(
        &self,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<bool> {
        match self.membership.kind {
            SetOpKind::Except => Ok(!self.bits.is_marked(ordinal)?),
            SetOpKind::Intersect => {
                for probe in 0..self.membership.lanes() {
                    if probe.is_multiple_of(MAX_UNOBSERVED_COMPILE_WORK as usize) {
                        control.checkpoint(0)?;
                    }
                    if !self.bits.is_marked(probe * self.keys.len() + ordinal)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
        }
    }
}
