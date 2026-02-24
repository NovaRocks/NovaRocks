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
//! Shared state for partitioned hash-join execution.
//!
//! Responsibilities:
//! - Stores per-partition build artifacts and synchronization flags for probe operators.
//! - Coordinates build completion and partition-level probe readiness across drivers.
//!
//! Key exported interfaces:
//! - Types: `PartitionedJoinSharedState`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::{Arc, Mutex};

use super::build_artifact::JoinBuildArtifact;
use super::build_state::JoinBuildSinkState;
use crate::exec::pipeline::dependency::{DependencyHandle, DependencyManager};

struct JoinPartitionState {
    ready: bool,
    artifact: Option<Arc<JoinBuildArtifact>>,
}

/// Shared state for partitioned hash joins, including partition artifact publication and synchronization.
pub(crate) struct PartitionedJoinSharedState {
    partitions: Vec<Mutex<JoinPartitionState>>,
    dep_handles: Arc<Vec<DependencyHandle>>,
    unknown_dep: DependencyHandle,
    null_aware: bool,
    ready_count: Mutex<usize>,
    global_build_row_count: Mutex<usize>,
    global_build_has_null_key: Mutex<bool>,
}

impl PartitionedJoinSharedState {
    pub(crate) fn new(
        node_id: i32,
        partition_count: usize,
        dep_manager: DependencyManager,
        null_aware: bool,
    ) -> Self {
        let partition_count = partition_count.max(1);
        let dep_handles = (0..partition_count)
            .map(|p| dep_manager.get_or_create(format!("join_build:{}:{}", node_id, p)))
            .collect::<Vec<_>>();
        let partitions = (0..partition_count)
            .map(|_| {
                Mutex::new(JoinPartitionState {
                    ready: false,
                    artifact: None,
                })
            })
            .collect::<Vec<_>>();
        Self {
            partitions,
            dep_handles: Arc::new(dep_handles),
            unknown_dep: dep_manager.get_or_create("join_build:unknown".to_string()),
            null_aware,
            ready_count: Mutex::new(0),
            global_build_row_count: Mutex::new(0),
            global_build_has_null_key: Mutex::new(false),
        }
    }

    pub(crate) fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub(crate) fn global_build_row_count(&self) -> usize {
        *self
            .global_build_row_count
            .lock()
            .expect("join build row count lock")
    }

    pub(crate) fn global_build_has_null_key(&self) -> bool {
        *self
            .global_build_has_null_key
            .lock()
            .expect("join build null lock")
    }

    pub(crate) fn dep_handle(&self, partition: usize) -> DependencyHandle {
        self.dep_handles
            .get(partition)
            .cloned()
            .unwrap_or_else(|| self.unknown_dep.clone())
    }

    pub(crate) fn dep_name(&self, partition: usize) -> &str {
        self.dep_handles
            .get(partition)
            .map(|dep| dep.name())
            .unwrap_or("join_build:unknown")
    }

    pub(crate) fn is_partition_ready(&self, partition: usize) -> bool {
        let Some(mu) = self.partitions.get(partition) else {
            return false;
        };
        let guard = mu.lock().expect("join partition lock");
        guard.ready
    }

    pub(crate) fn set_build_partition(
        &self,
        partition: usize,
        artifact: Arc<JoinBuildArtifact>,
    ) -> Result<(), String> {
        let mu = self
            .partitions
            .get(partition)
            .ok_or_else(|| "join partition out of bounds".to_string())?;
        let mut guard = mu.lock().expect("join partition lock");
        guard.artifact = Some(Arc::clone(&artifact));
        guard.ready = true;

        {
            let mut rows = self
                .global_build_row_count
                .lock()
                .expect("join build row count lock");
            *rows = rows.saturating_add(artifact.build_row_count);
        }
        {
            let mut has_null = self
                .global_build_has_null_key
                .lock()
                .expect("join build null lock");
            *has_null |= artifact.build_has_null_key;
        }

        if self.null_aware {
            let mut ready = self.ready_count.lock().expect("join ready count lock");
            *ready = ready.saturating_add(1);
            if *ready >= self.partitions.len() {
                for dep in self.dep_handles.iter() {
                    dep.set_ready();
                }
            }
        } else if let Some(dep) = self.dep_handles.get(partition) {
            dep.set_ready();
        }
        Ok(())
    }

    pub(crate) fn take_build_partition(
        &self,
        partition: usize,
    ) -> Result<Option<Arc<JoinBuildArtifact>>, String> {
        let mu = self
            .partitions
            .get(partition)
            .ok_or_else(|| "join partition out of bounds".to_string())?;
        let mut guard = mu.lock().expect("join partition lock");
        if !guard.ready {
            return Ok(None);
        }
        let artifact = guard.artifact.take();
        Ok(artifact)
    }
}

impl JoinBuildSinkState for PartitionedJoinSharedState {
    fn partition_for_driver(&self, driver_id: i32) -> usize {
        driver_id.max(0) as usize
    }

    fn dep_name(&self, partition: usize) -> &str {
        PartitionedJoinSharedState::dep_name(self, partition)
    }

    fn set_build(&self, partition: usize, artifact: Arc<JoinBuildArtifact>) -> Result<(), String> {
        self.set_build_partition(partition, artifact)
    }
}
