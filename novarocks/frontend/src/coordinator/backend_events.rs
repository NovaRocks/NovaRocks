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

use std::sync::Arc;

use novarocks::query_execution::backend::{
    BackendQueryEvent, BackendQueryEventSink, LiveBackendTarget,
};
use novarocks_types::QueryId;

use super::query_registry::FrontendQueryRegistry;

/// Frontend-owned view used to translate backend lifecycle events into
/// query-wide failure and dispatcher cancellation.
#[derive(Clone)]
pub struct BackendQueryActivity {
    registry: Arc<FrontendQueryRegistry>,
}

impl BackendQueryActivity {
    pub(crate) fn new(registry: Arc<FrontendQueryRegistry>) -> Self {
        Self { registry }
    }

    pub fn backend_lost(&self, backend_idx: usize) -> Vec<QueryId> {
        self.registry
            .backend_failed(backend_idx, format!("backend {backend_idx} lost"))
    }

    pub fn backend_restarted(
        &self,
        backend_idx: usize,
        old_epoch: u64,
        new_epoch: u64,
    ) -> Vec<QueryId> {
        self.registry.backend_restarted(
            backend_idx,
            old_epoch,
            format!("backend {backend_idx} restarted (epoch {old_epoch} -> {new_epoch})"),
        )
    }
}

impl BackendQueryEventSink for BackendQueryActivity {
    fn on_backend_event(&self, event: BackendQueryEvent) {
        match event {
            BackendQueryEvent::Unavailable {
                backend_idx,
                reason,
            } => {
                self.registry.backend_failed(backend_idx, reason);
            }
            BackendQueryEvent::Restarted {
                backend_idx,
                old_epoch,
                new_epoch,
            } => {
                self.backend_restarted(backend_idx, old_epoch, new_epoch);
            }
        }
    }

    fn backend_has_active_queries(&self, backend_idx: usize) -> bool {
        self.registry.backend_has_active_queries(backend_idx)
    }

    fn replace_live_backends(&self, revision: u64, backends: Vec<LiveBackendTarget>) {
        self.registry.replace_live_backends(revision, &backends);
    }
}
