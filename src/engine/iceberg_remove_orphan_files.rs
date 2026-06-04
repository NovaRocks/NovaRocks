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

//! Standalone-mode iceberg `ALTER TABLE x REMOVE ORPHAN FILES OLDER THAN '<ts>'`
//! entry point.
//!
//! Routes from `mod.rs::execute_in_context` for any iceberg target. Synchronous
//! execution; no OCC retry needed (ORPHAN does not commit a new metadata.json).
//!
//! Mirrors `iceberg_expire_snapshots.rs` structurally: resolve catalog entry →
//! build Hadoop catalog handle → `block_on_iceberg` → `run_remove_orphan_files`.

use std::sync::Arc;

use crate::engine::backend_resolver::TargetBackend;
use crate::engine::iceberg_maintenance::{
    MaintenanceActionKind, MaintenanceActionOptions, MaintenanceActionRequest,
    MaintenanceActionSource, execute_maintenance_action,
};
use crate::engine::statement::AlterTableRemoveOrphanFilesStmt;
use crate::engine::{StandaloneState, StatementResult};

/// Execute `ALTER TABLE x REMOVE ORPHAN FILES OLDER THAN '<ts>'` for an
/// iceberg-backed table.
///
/// Resolves the catalog entry from `state`, builds a Hadoop catalog handle,
/// and runs `run_remove_orphan_files` inside the iceberg tokio runtime.
///
/// On success logs the outcome (deleted file count + scanned count) at INFO
/// level and returns `StatementResult::Ok`.
pub(crate) fn execute_iceberg_remove_orphan_files(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    stmt: &AlterTableRemoveOrphanFilesStmt,
) -> Result<StatementResult, String> {
    debug_assert_eq!(
        target.backend_name, "iceberg",
        "execute_iceberg_remove_orphan_files called with non-iceberg backend"
    );

    execute_maintenance_action(
        state,
        MaintenanceActionRequest {
            source: MaintenanceActionSource::LegacyAlter,
            kind: MaintenanceActionKind::RemoveOrphanFiles,
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            options: MaintenanceActionOptions::default(),
            older_than_ms: Some(stmt.older_than_ms),
            retain_last: None,
            use_caching: None,
            spec_id: None,
            branch: None,
            where_clause: None,
        },
    )
}
