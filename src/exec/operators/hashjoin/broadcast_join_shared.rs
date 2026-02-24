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
//! Shared state for broadcast hash-join build/probe coordination.
//!
//! Responsibilities:
//! - Publishes one build artifact to all probe operators and tracks build completion.
//! - Coordinates probe visibility and shared error propagation across drivers.
//!
//! Key exported interfaces:
//! - Types: `BroadcastJoinSharedState`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::{Arc, Mutex};

use super::build_artifact::JoinBuildArtifact;
use super::build_state::JoinBuildSinkState;
use crate::exec::pipeline::dependency::{DependencyHandle, DependencyManager};

/// Shared state that publishes one broadcast join build artifact and coordinates probe readiness.
pub(crate) struct BroadcastJoinSharedState {
    dep: DependencyHandle,
    build: Mutex<Option<Arc<JoinBuildArtifact>>>,
}

impl BroadcastJoinSharedState {
    pub(crate) fn new(node_id: i32, dep_manager: DependencyManager) -> Self {
        let dep = dep_manager.get_or_create(format!("broadcast_join_build:{}", node_id));
        Self {
            dep,
            build: Mutex::new(None),
        }
    }

    pub(crate) fn dep(&self) -> DependencyHandle {
        self.dep.clone()
    }

    pub(crate) fn dep_name(&self) -> &str {
        self.dep.name()
    }

    pub(crate) fn set_build(&self, artifact: Arc<JoinBuildArtifact>) -> Result<(), String> {
        let mut guard = self.build.lock().expect("broadcast join build lock");
        if guard.is_some() {
            return Err("broadcast join build already set".to_string());
        }
        *guard = Some(artifact);
        self.dep.set_ready();
        Ok(())
    }

    pub(crate) fn get_build(&self) -> Option<Arc<JoinBuildArtifact>> {
        let guard = self.build.lock().expect("broadcast join build lock");
        guard.clone()
    }

    pub(crate) fn has_build(&self) -> bool {
        let guard = self.build.lock().expect("broadcast join build lock");
        guard.is_some()
    }
}

impl JoinBuildSinkState for BroadcastJoinSharedState {
    fn partition_for_driver(&self, _driver_id: i32) -> usize {
        0
    }

    fn dep_name(&self, _partition: usize) -> &str {
        self.dep.name()
    }

    fn set_build(&self, _partition: usize, artifact: Arc<JoinBuildArtifact>) -> Result<(), String> {
        BroadcastJoinSharedState::set_build(self, artifact)
    }
}
