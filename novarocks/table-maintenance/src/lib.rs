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

//! Application-owned table-maintenance primitives.
//!
//! This crate owns no SQL session, provider client, executor, or workload
//! policy. Products supply their exact target binding and execution adapter;
//! this crate supplies the single target conflict gate and rewrite/cleanup
//! state machines shared by SQL and MV.

use std::collections::BTreeMap;

pub mod activity;
pub mod gc_observation;
pub mod product;
pub mod runtime;

/// Stable product identity of one external table-maintenance target.
///
/// The target contains no connector handle, snapshot, or session state. The
/// caller captures and rebinds those provider facts around this durable
/// process-local identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MaintenanceTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

/// Caller-frozen identity of one automatic maintenance effect.
///
/// The owner freezes this value before entering the target's management gate.
/// Table Maintenance passes it through unchanged to the provider adapter.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct MaintenanceEffectId([u8; 16]);

impl MaintenanceEffectId {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// One typed maintenance operation after SQL lowering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceActionRequest {
    RewriteDataFiles {
        target: MaintenanceTarget,
        base_snapshot_id: i64,
        job_id: Option<i64>,
        options: BTreeMap<String, String>,
        branch: Option<String>,
        where_clause: Option<String>,
    },
    RewriteManifests {
        target: MaintenanceTarget,
        use_caching: Option<bool>,
        spec_id: Option<i32>,
    },
    ExpireSnapshots {
        target: MaintenanceTarget,
        older_than_ms: Option<i64>,
        retain_last: Option<u32>,
    },
    RemoveOrphanFiles {
        target: MaintenanceTarget,
        older_than_ms: i64,
    },
    RewritePositionDeleteFiles {
        target: MaintenanceTarget,
        options: BTreeMap<String, String>,
        where_clause: Option<String>,
    },
}

/// Provider facts projected by one completed maintenance operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceActionOutcome {
    RewriteDataFiles {
        target_snapshot_id: Option<i64>,
        rewritten_data_files_count: i32,
        /// `None` is an externally committed rewrite whose provider receipt
        /// does not prove an output file count. It is not a known zero.
        added_data_files_count: Option<i32>,
        /// Includes position deletes, deletion vectors, and equality deletes
        /// in the provider's typed receipt projection. `None` is unknown.
        added_delete_files_count: Option<i32>,
        rewritten_bytes_count: i64,
        failed_data_files_count: i32,
        removed_delete_files_count: i32,
        /// `None` is an unknown publication fact, distinct from zero rows.
        output_record_count: Option<i64>,
    },
    RewriteManifests {
        rewritten_manifests_count: i32,
        added_manifests_count: i32,
    },
    ExpireSnapshots {
        deleted_data_files_count: Option<i64>,
        deleted_position_delete_files_count: Option<i64>,
        deleted_equality_delete_files_count: Option<i64>,
        deleted_manifest_files_count: Option<i64>,
        deleted_manifest_lists_count: Option<i64>,
        deleted_statistics_files_count: Option<i64>,
    },
    RemoveOrphanFiles {
        orphan_file_locations: Vec<String>,
    },
    RewritePositionDeleteFiles {
        rewritten_delete_files_count: i32,
        /// `None` is an unknown publication fact, distinct from zero files.
        added_delete_files_count: Option<i32>,
        rewritten_bytes_count: i64,
        added_bytes_count: i64,
    },
}

/// Successful terminal fact for one automatic maintenance action.
///
/// A rewrite with no selected work has no external commit and must not be
/// accounted as a committed effect by its management owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AutomaticMaintenanceOutcome {
    KnownCommitted(MaintenanceActionOutcome),
    NoOpWithoutCommit(MaintenanceActionOutcome),
}

impl AutomaticMaintenanceOutcome {
    /// Projects to the SQL-facing action receipt when a user statement does
    /// not own automatic management-effect accounting.
    pub fn into_action_outcome(self) -> MaintenanceActionOutcome {
        match self {
            Self::KnownCommitted(outcome) | Self::NoOpWithoutCommit(outcome) => outcome,
        }
    }
}

/// Result of rebinding one durable maintenance target to its current table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceTargetRebind {
    Bound,
    Replaced,
    Missing,
}

#[cfg(test)]
#[path = "tests_ports.rs"]
mod port_tests;
