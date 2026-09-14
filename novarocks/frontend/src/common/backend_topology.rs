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

/// Frontend-owned values rendered by the process metrics surface.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BackendTopologyMetricsSnapshot {
    pub entries: usize,
    pub announce_lease_valid: usize,
    pub identity_verified: usize,
    pub reported_running: usize,
    pub reported_draining: usize,
    pub compatibility_compatible: usize,
    pub compatibility_other_island: usize,
    pub compatibility_unknown_or_invalid: usize,
    pub endpoint_owned: usize,
    pub endpoint_unowned: usize,
    pub eligible: usize,
    pub revision: u64,
}

/// Publishes frontend-owned topology counts without giving the query
/// application access to frontend metric state or labels.
pub fn publish_backend_topology_metrics(snapshot: BackendTopologyMetricsSnapshot) {
    crate::metrics::publish_backend_topology_metrics(
        snapshot.entries,
        snapshot.announce_lease_valid,
        snapshot.identity_verified,
        snapshot.reported_running,
        snapshot.reported_draining,
        snapshot.compatibility_compatible,
        snapshot.compatibility_other_island,
        snapshot.compatibility_unknown_or_invalid,
        snapshot.endpoint_owned,
        snapshot.endpoint_unowned,
        snapshot.eligible,
        snapshot.revision,
    );
}

/// Core-local scheduling metric. Topology accounting is performed by the
/// frontend-owned port at the composition boundary.
pub fn record_successful_stage(_backend_idx: usize, fragment_count: usize) {
    crate::metrics::observe_fragments_scheduled(fragment_count);
}
