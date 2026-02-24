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
//! Shared scan runtime data types.
//!
//! Responsibilities:
//! - Defines scan async state, push results, and runtime-filter probe helpers.
//! - Centralizes scan worker/source communication contracts.
//!
//! Key exported interfaces:
//! - Types: `ScanAsyncState`, `PushResult`, `ScanRuntimeFilterProbe`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_filter_hub::RuntimeFilterProbe;
use crate::runtime::runtime_state::RuntimeState;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

struct ScanBufferState {
    queue: VecDeque<Chunk>,
    finished: bool,
    error: Option<String>,
}

/// Shared async scan queue/state observed by scan workers and scan source operators.
pub(super) struct ScanAsyncState {
    mu: Mutex<ScanBufferState>,
    observable: Arc<Observable>,
    canceled: AtomicBool,
    max_buffered: usize,
    label: String,
    queue_tracker: OnceLock<Arc<MemTracker>>,
}

/// Result of pushing one scanned chunk into the scan async state buffer.
pub(super) enum PushResult {
    Pushed,
    Full(Chunk),
    Canceled,
}

impl ScanAsyncState {
    pub(super) fn new(max_buffered: usize, label: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            mu: Mutex::new(ScanBufferState {
                queue: VecDeque::new(),
                finished: false,
                error: None,
            }),
            observable: Arc::new(Observable::new()),
            canceled: AtomicBool::new(false),
            max_buffered: max_buffered.max(1),
            label: label.into(),
            queue_tracker: OnceLock::new(),
        })
    }

    pub(super) fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.observable)
    }

    pub(super) fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::Acquire)
    }

    pub(super) fn cancel(&self) {
        self.canceled.store(true, Ordering::Release);
        let notify = self.observable.defer_notify();
        notify.arm();
    }

    pub(super) fn has_capacity(&self) -> bool {
        let guard = self.mu.lock().expect("scan async state lock");
        guard.queue.len() < self.max_buffered
    }

    pub(super) fn has_output(&self) -> bool {
        let guard = self.mu.lock().expect("scan async state lock");
        guard.error.is_some() || !guard.queue.is_empty()
    }

    pub(super) fn is_finished(&self) -> bool {
        let guard = self.mu.lock().expect("scan async state lock");
        guard.finished && guard.queue.is_empty() && guard.error.is_none()
    }

    pub(super) fn pop_chunk(&self) -> Result<Option<Chunk>, String> {
        let mut guard = self.mu.lock().expect("scan async state lock");
        if let Some(err) = guard.error.take() {
            return Err(err);
        }
        if let Some(chunk) = guard.queue.pop_front() {
            return Ok(Some(chunk));
        }
        if guard.finished {
            return Ok(None);
        }
        Ok(None)
    }

    pub(super) fn push_chunk(&self, mut chunk: Chunk) -> PushResult {
        if self.is_canceled() {
            return PushResult::Canceled;
        }
        if let Some(tracker) = self.queue_tracker.get() {
            chunk.transfer_to(tracker);
        }
        let mut guard = self.mu.lock().expect("scan async state lock");
        if guard.queue.len() >= self.max_buffered {
            return PushResult::Full(chunk);
        }
        guard.queue.push_back(chunk);
        drop(guard);
        let notify = self.observable.defer_notify();
        notify.arm();
        PushResult::Pushed
    }

    pub(super) fn mark_finished(&self) {
        let notify = self.observable.defer_notify();
        let mut guard = self.mu.lock().expect("scan async state lock");
        guard.finished = true;
        drop(guard);
        notify.arm();
    }

    pub(super) fn set_error(&self, err: String) {
        let notify = self.observable.defer_notify();
        let mut guard = self.mu.lock().expect("scan async state lock");
        guard.error = Some(err);
        guard.finished = true;
        drop(guard);
        notify.arm();
    }

    pub(super) fn ensure_mem_tracker(&self, state: &RuntimeState) {
        let Some(root) = state.mem_tracker() else {
            return;
        };
        if self.queue_tracker.get().is_some() {
            return;
        }
        let label = self.label.clone();
        let tracker = MemTracker::new_child(label, &root);
        if self.queue_tracker.set(Arc::clone(&tracker)).is_ok() {
            let mut guard = self.mu.lock().expect("scan async state lock");
            for chunk in guard.queue.iter_mut() {
                chunk.transfer_to(&tracker);
            }
        }
    }
}

#[derive(Clone)]
/// Runtime-filter probe helper attached to scan pipelines for early row pruning.
pub(super) struct ScanRuntimeFilterProbe {
    probe: RuntimeFilterProbe,
}

impl ScanRuntimeFilterProbe {
    pub(super) fn new(probe: RuntimeFilterProbe) -> Self {
        Self { probe }
    }

    pub(super) fn dependency_or_timeout(&self) -> Option<DependencyHandle> {
        self.probe.dependency_or_timeout(true)
    }

    #[allow(dead_code)]
    pub(super) fn dependency(&self) -> DependencyHandle {
        self.probe.dependency()
    }

    pub(super) fn snapshot(&self) -> crate::runtime::runtime_filter_hub::RuntimeFilterSnapshot {
        self.probe.snapshot()
    }

    pub(super) fn handle(&self) -> crate::runtime::runtime_filter_hub::RuntimeFilterHandle {
        self.probe.handle()
    }

    pub(super) fn mark_ready(&self) -> Option<std::time::Duration> {
        self.probe.mark_ready()
    }
}
