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

//! Process-scoped frontend query counters.
//!
//! These are process facts, never attempt-scoped ones. The counter set is read
//! by the `metrics` field of the query convergence rollup that the coordinator
//! publishes once a task-protocol attempt has converged.
//!
//! # No producer increments this set
//!
//! The retired distributed query lifecycle chain was the only producer these
//! counters ever had, and the phases the field names describe -- InitQuery,
//! control-stream attach, ControlReady, lifecycle heartbeat, terminal snapshot
//! storage -- have no counterpart in the task protocol that replaced it.
//! Retiring that chain left this set with readers and no writer, so a rollup
//! reports [`FrontendProcessQueryCountersSnapshot::default`] and every value is
//! zero.
//!
//! The type is kept rather than deleted because two surfaces still read it: the
//! rollup's `metrics` field, mirrored by the cluster-harness wire type and the
//! SQL runner's `expect_lifecycle_metric_delta` directive, and the four
//! `novarocks_frontend_query_lifecycle_*` Prometheus families that
//! `super::ensure_frontend_metric_label_families` registers on every scrape and
//! that the harness role-scoped metrics barrier requires on every frontend
//! start. Retiring the counter set means retiring those surfaces together, in
//! one coordinated change across the verification infrastructure.
//!
//! Do not give an individual field a producer to make a number move. Either a
//! task-protocol phase genuinely owns the fact and the field should be named
//! for it, or the field belongs in the retirement described above.

/// One immutable read of the process-scoped frontend query counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FrontendProcessQueryCountersSnapshot {
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
