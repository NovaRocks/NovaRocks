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
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::common::config::operator_buffer_chunks;
use crate::exec::chunk::Chunk;
use crate::exec::pipeline::schedule::observer::Observable;

struct BufferState {
    chunks: VecDeque<Chunk>,
    sink_finished: bool,
}

#[derive(Clone)]
pub(crate) struct AggregateStreamingState {
    inner: Arc<Mutex<BufferState>>,
    observable: Arc<Observable>,
    buffer_limit: usize,
}

impl AggregateStreamingState {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BufferState {
                chunks: VecDeque::new(),
                sink_finished: false,
            })),
            observable: Arc::new(Observable::new()),
            buffer_limit: operator_buffer_chunks().max(1),
        }
    }

    /// Push a chunk into the buffer. Called by the Sink operator.
    /// Notifies the Source via the observable.
    pub(crate) fn offer_chunk(&self, chunk: Chunk) {
        let notify = self.observable.defer_notify();
        let mut guard = self.inner.lock().expect("streaming state lock");
        if !chunk.is_empty() {
            guard.chunks.push_back(chunk);
        }
        drop(guard);
        notify.arm();
    }

    /// Mark the Sink as finished. No more chunks will be offered.
    pub(crate) fn mark_sink_finished(&self) {
        let notify = self.observable.defer_notify();
        let mut guard = self.inner.lock().expect("streaming state lock");
        guard.sink_finished = true;
        drop(guard);
        notify.arm();
    }

    /// Pull a chunk from the buffer. Called by the Source operator.
    pub(crate) fn poll_chunk(&self) -> Option<Chunk> {
        let mut guard = self.inner.lock().expect("streaming state lock");
        guard.chunks.pop_front()
    }

    /// Returns true if the buffer has chunks available.
    pub(crate) fn has_chunks(&self) -> bool {
        let guard = self.inner.lock().expect("streaming state lock");
        !guard.chunks.is_empty()
    }

    /// Returns true if the buffer is full (at or above the limit).
    pub(crate) fn is_buffer_full(&self) -> bool {
        let guard = self.inner.lock().expect("streaming state lock");
        guard.chunks.len() >= self.buffer_limit
    }

    /// Returns true when the Sink has finished AND the buffer is drained.
    pub(crate) fn is_done(&self) -> bool {
        let guard = self.inner.lock().expect("streaming state lock");
        guard.sink_finished && guard.chunks.is_empty()
    }

    /// Returns the Observable for driver scheduling (Source side).
    pub(crate) fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.observable)
    }
}
