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
//! Shared queue state for UNION ALL execution.
//!
//! Responsibilities:
//! - Buffers incoming chunks from multiple sink branches for source-side consumption.
//! - Coordinates sink completion and fair chunk draining across input branches.
//!
//! Key exported interfaces:
//! - Types: `UnionAllSharedState`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;

struct UnionAllState {
    buffer: VecDeque<Chunk>,
    remaining_producers: usize,
}

#[derive(Clone)]
/// Shared queue state for UNION ALL branch fan-in and source-side draining.
pub(crate) struct UnionAllSharedState {
    inner: Arc<Mutex<UnionAllState>>,
    observable: Arc<Observable>,
    label: String,
    queue_tracker: Arc<OnceLock<Arc<MemTracker>>>,
}

impl UnionAllSharedState {
    pub(crate) fn new(producer_count: usize, node_id: i32) -> Self {
        let label = if node_id >= 0 {
            format!("union_all_queue_{node_id}")
        } else {
            "union_all_queue".to_string()
        };
        Self {
            inner: Arc::new(Mutex::new(UnionAllState {
                buffer: VecDeque::new(),
                remaining_producers: producer_count,
            })),
            observable: Arc::new(Observable::new()),
            label,
            queue_tracker: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn push_chunk(&self, state: &RuntimeState, mut chunk: Chunk) {
        if let Some(tracker) = self.queue_mem_tracker(state).as_ref() {
            chunk.transfer_to(tracker);
        }
        let notify = self.observable.defer_notify();
        let should_notify = {
            let mut guard = self.inner.lock().expect("union all state lock");
            let was_empty = guard.buffer.is_empty();
            guard.buffer.push_back(chunk);
            was_empty
        };
        if should_notify {
            notify.arm();
        }
    }

    pub(crate) fn pop_chunk(&self) -> Option<Chunk> {
        let mut guard = self.inner.lock().expect("union all state lock");
        let chunk = guard.buffer.pop_front();
        chunk
    }

    pub(crate) fn has_buffered(&self) -> bool {
        let guard = self.inner.lock().expect("union all state lock");
        !guard.buffer.is_empty()
    }

    pub(crate) fn remaining_producers(&self) -> usize {
        let guard = self.inner.lock().expect("union all state lock");
        guard.remaining_producers
    }

    pub(crate) fn producer_finished(&self) {
        let notify = self.observable.defer_notify();
        let should_notify = {
            let mut guard = self.inner.lock().expect("union all state lock");
            if guard.remaining_producers > 0 {
                guard.remaining_producers -= 1;
            }
            guard.remaining_producers == 0
        };
        if should_notify {
            notify.arm();
        }
    }

    pub(crate) fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.observable)
    }

    fn queue_mem_tracker(&self, state: &RuntimeState) -> Option<Arc<MemTracker>> {
        let root = state.mem_tracker()?;
        let label = self.label.clone();
        let tracker = self
            .queue_tracker
            .get_or_init(|| MemTracker::new_child(label, &root));
        Some(Arc::clone(tracker))
    }
}
