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

//! The reservation Arrow stores inside a shared buffer.

use std::sync::Arc;

use arrow_buffer::MemoryReservation;
use novarocks_memory::charge::ChargeState;
use novarocks_memory::grant::SharedGrant;

/// A memory-core charge, in the shape Arrow keeps inside a `Bytes`.
///
/// Arrow owns this object for as long as the backing lives: it is stored in
/// the shared `Bytes`, so every slice, clone and exported array observes the
/// same charge, and the last alias to go away drops it. Dropping it settles
/// the charge exactly once, which is why the accounting follows the backing
/// rather than whichever wrapper happened to measure it.
#[derive(Debug)]
pub struct ChargeReservation {
    state: Arc<ChargeState>,
    grant: SharedGrant,
}

impl ChargeReservation {
    pub(crate) fn new(state: Arc<ChargeState>, grant: SharedGrant) -> Self {
        Self { state, grant }
    }

    /// Returns the shared charge state, which is what a receipt keeps so the
    /// debt can later be moved rather than re-claimed.
    pub fn state(&self) -> &Arc<ChargeState> {
        &self.state
    }

    /// Returns the bytes currently charged.
    pub fn charged_bytes(&self) -> u64 {
        self.state.bytes()
    }
}

impl MemoryReservation for ChargeReservation {
    fn size(&self) -> usize {
        usize::try_from(self.state.bytes()).unwrap_or(usize::MAX)
    }

    /// Follows a change in the buffer's own accounting.
    ///
    /// Arrow calls this from `reallocate`, where the capacity really did
    /// change, and also from `truncate`, `resize` and `clear`, where only the
    /// logical length moved and the allocation kept its capacity. This cannot
    /// tell the two apart from the number alone, and it cannot fail, so it
    /// takes the conservative side of each: growth is charged, and a shrink is
    /// released. That is safe for a capacity authority — it never
    /// under-reports live capacity at the moment of growth — but it is why a
    /// reservation seen on a `MutableBuffer` is not evidence of retained
    /// capacity, and why this crate claims only immutable buffers at a
    /// handover point.
    fn resize(&mut self, new_size: usize) {
        let new_size = new_size as u64;
        let current = self.state.bytes();
        if new_size > current {
            let additional = new_size - current;
            // Growth inside the grant is ordinary settlement. Beyond it the
            // bytes still exist, so they are absorbed as excess and the
            // account's growth freezes; refusing here would only hide them.
            if self.state.grow_within(&self.grant, additional).is_err() {
                self.state.absorb_growth(additional);
            }
        } else if new_size < current {
            self.state.shrink_to(new_size);
        }
    }
}

impl Drop for ChargeReservation {
    fn drop(&mut self) {
        self.state.release();
    }
}
