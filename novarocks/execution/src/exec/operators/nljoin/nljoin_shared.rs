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
//! Shared artifact/state for nested-loop join execution.
//!
//! Responsibilities:
//! - Stores build-side chunks and synchronization flags shared by NL-join operators.
//! - Coordinates visibility and completion between NL-join build and probe stages.
//!
//! Key exported interfaces:
//! - Types: `NlJoinBuildArtifact`, `NlJoinSharedState`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::dependency::{DependencyHandle, DependencyManager};

/// Materialized build-side chunk storage used by nested-loop join probing.
pub(crate) struct NlJoinBuildArtifact {
    pub(crate) build_batches: Arc<Vec<Chunk>>,
}

impl NlJoinBuildArtifact {
    pub(crate) fn new(build_batches: Vec<Chunk>) -> Self {
        Self {
            build_batches: Arc::new(build_batches),
        }
    }
}

/// Shared state coordinating nested-loop join build publication and probe visibility.
pub(crate) struct NlJoinSharedState {
    node_id: i32,
    build_dep: DependencyHandle,
    build_rows: AtomicUsize,
    build: Mutex<Option<Arc<NlJoinBuildArtifact>>>,
    probe_total: usize,
    probe_finished: AtomicUsize,
    shared_build_matched: Mutex<Option<Vec<Vec<bool>>>>,
}

impl NlJoinSharedState {
    pub(crate) fn new(node_id: i32, probe_count: usize, dep_manager: DependencyManager) -> Self {
        let build_dep = dep_manager.get_or_create(format!("nljoin_build:{}", node_id));
        Self {
            node_id,
            build_dep,
            build_rows: AtomicUsize::new(0),
            build: Mutex::new(None),
            probe_total: probe_count.max(1),
            probe_finished: AtomicUsize::new(0),
            shared_build_matched: Mutex::new(None),
        }
    }

    pub(crate) fn build_dep(&self) -> DependencyHandle {
        self.build_dep.clone()
    }

    pub(crate) fn node_id(&self) -> i32 {
        self.node_id
    }

    pub(crate) fn set_build(&self, artifact: Arc<NlJoinBuildArtifact>) -> Result<(), String> {
        let mut guard = self.build.lock().expect("nljoin build lock");
        if guard.is_some() {
            return Err("nljoin build already set".to_string());
        }
        let total_rows = artifact
            .build_batches
            .iter()
            .map(|b| b.len())
            .sum::<usize>();
        self.build_rows.store(total_rows, Ordering::Release);
        *guard = Some(artifact);
        self.build_dep.set_ready();
        Ok(())
    }

    pub(crate) fn get_build(&self) -> Option<Arc<NlJoinBuildArtifact>> {
        let guard = self.build.lock().expect("nljoin build lock");
        guard.clone()
    }

    pub(crate) fn has_build(&self) -> bool {
        let guard = self.build.lock().expect("nljoin build lock");
        guard.is_some()
    }

    pub(crate) fn is_build_empty(&self) -> bool {
        self.has_build() && self.build_rows.load(Ordering::Acquire) == 0
    }

    pub(crate) fn finish_probe(&self, local_flags: Option<Vec<Vec<bool>>>) -> bool {
        if let Some(local) = local_flags {
            let mut shared = self
                .shared_build_matched
                .lock()
                .expect("nljoin build match flags lock");
            match shared.as_mut() {
                Some(shared_flags) => {
                    if shared_flags.len() != local.len() {
                        panic!(
                            "nljoin build match flags batch mismatch: shared={} local={}",
                            shared_flags.len(),
                            local.len()
                        );
                    }
                    for (shared_batch, local_batch) in
                        shared_flags.iter_mut().zip(local.into_iter())
                    {
                        if shared_batch.len() != local_batch.len() {
                            panic!(
                                "nljoin build match flags row mismatch: shared={} local={}",
                                shared_batch.len(),
                                local_batch.len()
                            );
                        }
                        for (shared_flag, local_flag) in
                            shared_batch.iter_mut().zip(local_batch.into_iter())
                        {
                            *shared_flag |= local_flag;
                        }
                    }
                }
                None => {
                    *shared = Some(local);
                }
            }
        }

        let finished = self.probe_finished.fetch_add(1, Ordering::AcqRel) + 1;
        finished >= self.probe_total
    }

    pub(crate) fn shared_build_matched(&self) -> Option<Vec<Vec<bool>>> {
        let guard = self
            .shared_build_matched
            .lock()
            .expect("nljoin build match flags lock");
        guard.clone()
    }
}
