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

use std::sync::{Arc, Mutex};

use novarocks_spi::connector::{ConnectorOutputMemoryToken, SharedSourceHold};

use super::memory::ChunkAccounting;

/// Borrows only the authorities actually attached to one Chunk. Construction
/// stays beside its private owner fields; no byte or buffer identity is used.
#[derive(Clone, Copy)]
pub(crate) struct ChunkSourceAuthorityRef<'a> {
    native: Option<&'a Arc<ChunkAccounting>>,
    provider: Option<&'a Arc<Mutex<ConnectorOutputMemoryToken>>>,
    shared_provider: Option<&'a Arc<Vec<SharedSourceHold>>>,
}

impl<'a> ChunkSourceAuthorityRef<'a> {
    pub(super) fn from_attached(
        native: Option<&'a Arc<ChunkAccounting>>,
        provider: Option<&'a Arc<Mutex<ConnectorOutputMemoryToken>>>,
        shared_provider: Option<&'a Arc<Vec<SharedSourceHold>>>,
    ) -> Self {
        Self {
            native,
            provider,
            shared_provider,
        }
    }

    pub(crate) fn is_attached(self) -> bool {
        self.native.is_some() || self.provider.is_some() || self.shared_provider.is_some()
    }

    /// Shares the original charge without allocation, transfer or a new
    /// reservation. Attaching this handle to escaping buffers is a separate
    /// operation, with its own admission and writer handoff obligations.
    #[allow(
        dead_code,
        reason = "Prepared internal capture port; production callers only borrow"
    )]
    pub(crate) fn capture(self) -> Option<ChunkSourceAuthority> {
        self.is_attached().then(|| ChunkSourceAuthority {
            _native: self.native.map(Arc::clone),
            _provider: self.provider.map(Arc::clone),
            _shared_provider: self.shared_provider.map(Arc::clone),
        })
    }

    #[cfg(test)]
    pub(crate) fn strong_counts(self) -> (Option<usize>, Option<usize>) {
        (
            self.native.map(Arc::strong_count),
            self.provider.map(Arc::strong_count),
        )
    }

    #[cfg(test)]
    pub(crate) fn owner_identity(self) -> (Option<usize>, Option<usize>) {
        (
            self.native.map(|owner| Arc::as_ptr(owner) as usize),
            self.provider.map(|owner| Arc::as_ptr(owner) as usize),
        )
    }
}

/// Fixed-size original owner handles. Their existing last-Arc Drop releases
/// the original charge. This grants no split, shrink or transfer capability.
#[derive(Clone)]
#[allow(
    dead_code,
    reason = "Prepared capture handle; not installed on production output buffers"
)]
pub(crate) struct ChunkSourceAuthority {
    _native: Option<Arc<ChunkAccounting>>,
    _provider: Option<Arc<Mutex<ConnectorOutputMemoryToken>>>,
    _shared_provider: Option<Arc<Vec<SharedSourceHold>>>,
}

impl ChunkSourceAuthority {
    pub(super) fn borrowed(&self) -> ChunkSourceAuthorityRef<'_> {
        ChunkSourceAuthorityRef::from_attached(
            self._native.as_ref(),
            self._provider.as_ref(),
            self._shared_provider.as_ref(),
        )
    }
}
