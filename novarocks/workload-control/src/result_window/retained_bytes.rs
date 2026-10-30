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

//! Actual retained result buffers share the original result owner's budget.
//! Responsibility quantities and SDK layout estimates never reserve bytes.

use super::{AliasBacking, ResultWindowAlias};
use crate::WorkError;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct RetainedBytes {
    limit: u64,
    held: AtomicU64,
}
impl RetainedBytes {
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            limit,
            held: AtomicU64::new(0),
        }
    }
    fn reserve(&self, bytes: u64) -> Result<(), WorkError> {
        let mut held = self.held.load(Ordering::Acquire);
        loop {
            let next = held
                .checked_add(bytes)
                .ok_or(WorkError::ArithmeticOverflow)?;
            if next > self.limit {
                // This is the same Root's retained data. Waiting for itself
                // cannot make this append or old+new replacement fit.
                return Err(WorkError::Capacity("retained result bytes"));
            }
            match self
                .held
                .compare_exchange_weak(held, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(()),
                Err(current) => held = current,
            }
        }
    }
    fn release(&self, bytes: u64) {
        let old = self.held.fetch_sub(bytes, Ordering::AcqRel);
        assert!(old >= bytes, "retained result byte receipt underflow");
    }
    pub(crate) fn limit(&self) -> u64 {
        self.limit
    }
}

/// A receipt for actual buffer backing, retained by its original data owner.
/// Move it on handoff; keep it until the data and its aliases actually exit.
#[must_use = "the actual result backing must retain its byte receipt"]
pub struct ResultPayloadBytes {
    original: ResultWindowAlias,
    bytes: u64,
}
impl ResultPayloadBytes {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Reserve before constructing additional backing. A refused append does
    /// not change this receipt or the original owner's retained byte count.
    pub fn try_grow(&mut self, additional: u64) -> Result<(), WorkError> {
        let next = self
            .bytes
            .checked_add(additional)
            .ok_or(WorkError::ArithmeticOverflow)?;
        self.original.retained_bytes().reserve(additional)?;
        self.bytes = next;
        Ok(())
    }

    /// Call only after the original owner has destroyed the released backing.
    pub fn release_destroyed(&mut self, bytes: u64) -> Result<(), WorkError> {
        let next = self.bytes.checked_sub(bytes).ok_or(WorkError::Conflict)?;
        self.original.retained_bytes().release(bytes);
        self.bytes = next;
        Ok(())
    }
}
impl Drop for ResultPayloadBytes {
    fn drop(&mut self) {
        self.original.retained_bytes().release(self.bytes);
    }
}
impl std::fmt::Debug for ResultPayloadBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultPayloadBytes")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}
impl ResultWindowAlias {
    fn retained_bytes(&self) -> &RetainedBytes {
        match &self.backing {
            AliasBacking::Window { holder, .. } => &holder.retained_bytes,
            AliasBacking::InternalRoot { budget, .. }
            | AliasBacking::RootRetention { budget, .. } => budget,
        }
    }

    /// Reserve actual backing against this original result owner's budget.
    /// Cloning retention or a manifest binding does not itself reserve bytes.
    pub fn reserve_payload_bytes(&self, bytes: u64) -> Result<ResultPayloadBytes, WorkError> {
        let original = self.try_clone()?;
        original.retained_bytes().reserve(bytes)?;
        Ok(ResultPayloadBytes { original, bytes })
    }

    /// Observe the original configured data budget. This neither reserves
    /// backing nor grants a computation, Source, manifest, or dispatch right.
    pub fn payload_capacity_bytes(&self) -> u64 {
        self.retained_bytes().limit()
    }

    pub fn retained_payload_bytes(&self) -> u64 {
        self.retained_bytes().held.load(Ordering::Acquire)
    }
}
