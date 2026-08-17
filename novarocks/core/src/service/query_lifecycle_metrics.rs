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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FrontendQueryLifecycleMetricsSnapshot {
    pub active_attempts: usize,
    pub init_applied: u64,
    pub init_idempotent: u64,
    pub init_failed: u64,
    pub init_uncertain_cleanup: u64,
    pub manifest_conflicts: u64,
    pub init_latency_micros_total: u64,
    pub init_latency_samples: u64,
    pub control_ready: u64,
    pub attach_failed: u64,
    pub attach_latency_micros_total: u64,
    pub attach_latency_samples: u64,
    pub heartbeat_timeouts: u64,
    pub coordinator_lost: u64,
    pub local_failures: u64,
    pub backend_epoch_mismatches: u64,
    pub cleanup_failures: u64,
    pub terminal_locally_drained: u64,
    pub terminal_snapshots_accepted: u64,
    pub terminal_snapshots_idempotent: u64,
    pub terminal_snapshot_conflicts: u64,
    pub terminal_finalize_failures: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BackendQueryLifecycleMetricsSnapshot {
    pub initializing: usize,
    pub initialized: usize,
    pub control_attached: usize,
    pub terminating: usize,
    pub tombstones: usize,
    pub admission_rejected: u64,
    pub init_conflicts: u64,
    pub heartbeat_timeouts: u64,
    pub terminations: u64,
    pub terminal_facts: u64,
    pub terminal_locally_drained: u64,
    pub terminal_records_frozen: u64,
    pub terminal_acknowledged: u64,
    pub terminal_retention_expired: u64,
    pub terminal_fallback_accepted: u64,
    pub terminal_fallback_rejected: u64,
    pub terminal_retained: usize,
    pub terminal_retained_bytes: usize,
}
