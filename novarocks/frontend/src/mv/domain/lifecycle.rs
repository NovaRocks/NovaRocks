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

//! Typed materialized-view refresh lifecycle contracts.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use crate::mv::domain::application::{
    MvCreateStatement, MvDropStatement, MvRefreshRequest, MvShowStatement,
};
use crate::mv::domain::model::MvTarget;
use crate::mv::domain::refresh::planning::RefreshPlanContract;

#[derive(Clone)]
pub struct CreateMvRequest {
    pub stmt: MvCreateStatement,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

#[derive(Clone)]
pub struct DropMvRequest {
    pub stmt: MvDropStatement,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

#[derive(Clone, Debug)]
pub struct ListMvsRequest {
    pub stmt: MvShowStatement,
    pub current_catalog: Option<String>,
}

#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub struct RefreshRequest {
    pub target: MvTarget,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub statement: MvRefreshRequest,
}

#[derive(Clone, Debug)]
pub struct RefreshPlan {
    pub contract: RefreshPlanContract,
    pub backend_plan: BackendRefreshPlan,
}

#[derive(Clone, Debug)]
pub enum BackendRefreshPlan {
    StarRocks(StarRocksTableRefreshPlan),
    Iceberg(IcebergRefreshPlan),
}

#[derive(Clone, Debug)]
pub struct StarRocksTableRefreshPlan {
    pub stmt: MvRefreshRequest,
    pub current_catalog: Option<String>,
    pub current_database: String,
}

#[derive(Clone, Debug)]
pub struct IcebergRefreshPlan {
    pub stmt: MvRefreshRequest,
    pub current_catalog: Option<String>,
    pub current_database: String,
}

#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub struct RefreshOutcome {
    pub mv_id: Option<i64>,
    pub target: MvTarget,
    pub rows: Option<i64>,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_object_ids: BTreeMap<String, novarocks_spi::connector::ConnectorTableObjectId>,
    pub target_snapshot_id: Option<i64>,
    pub backend_outcome: BackendRefreshOutcome,
}

#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub enum BackendRefreshOutcome {
    StarRocks(StarRocksTableRefreshOutcome),
    Iceberg(IcebergRefreshOutcome),
}

#[derive(Clone, Debug, Default)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub struct StarRocksTableRefreshOutcome {
    pub completed_inside_execute: bool,
}

#[derive(Clone, Debug, Default)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub struct IcebergRefreshOutcome {
    pub completed_inside_execute: bool,
}

#[derive(Clone)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub struct RefreshCtx {
    pub refresh_id: Option<i64>,
    pub expected_target_snapshot_id: Option<i64>,
    pub recovery_required: bool,
    pub connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

impl RefreshCtx {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub fn new(connector_context: novarocks_spi::connector::ConnectorRequestContext) -> Self {
        Self {
            refresh_id: None,
            expected_target_snapshot_id: None,
            recovery_required: false,
            connector_context,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvListRow {
    pub name: String,
    pub database: String,
    pub storage_engine: String,
    pub refresh_mode: String,
    pub last_refresh_time: Option<String>,
    pub last_refresh_rows: Option<String>,
    pub base_tables: String,
    pub select_text: String,
    pub dependencies: String,
    pub refresh_paused: String,
    pub next_refresh_time: Option<String>,
    pub last_scheduler_error: Option<String>,
    pub max_staleness_ms: Option<String>,
    pub refresh_state: String,
    pub retry_after_time: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshErrorKind {
    UserError,
    PreCommitFailed,
    CommitFailedKnownUncommitted,
    CommitFailedKnownCommitted,
    CommitUnknown,
    MetadataFinalizeFailed,
}

impl RefreshErrorKind {
    pub fn should_rollback_after_commit(self) -> bool {
        matches!(
            self,
            Self::UserError | Self::PreCommitFailed | Self::CommitFailedKnownUncommitted
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshError {
    pub kind: RefreshErrorKind,
    pub message: String,
}

impl RefreshError {
    pub fn new(kind: RefreshErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn user(message: impl Into<String>) -> Self {
        Self::new(RefreshErrorKind::UserError, message)
    }

    pub fn pre_commit(message: impl Into<String>) -> Self {
        Self::new(RefreshErrorKind::PreCommitFailed, message)
    }

    pub fn commit_known_uncommitted(message: impl Into<String>) -> Self {
        Self::new(RefreshErrorKind::CommitFailedKnownUncommitted, message)
    }

    pub fn commit_known_committed_finalize_failed(message: impl Into<String>) -> Self {
        Self::new(RefreshErrorKind::CommitFailedKnownCommitted, message)
    }

    pub fn commit_unknown(message: impl Into<String>) -> Self {
        Self::new(RefreshErrorKind::CommitUnknown, message)
    }

    pub fn metadata_finalize(message: impl Into<String>) -> Self {
        Self::new(RefreshErrorKind::MetadataFinalizeFailed, message)
    }
}

impl From<String> for RefreshError {
    fn from(message: String) -> Self {
        Self::user(message)
    }
}

impl fmt::Display for RefreshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for RefreshError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::model::MvStorageEngine;

    #[test]
    fn refresh_error_kind_drives_commit_rollback_policy() {
        assert!(RefreshErrorKind::UserError.should_rollback_after_commit());
        assert!(RefreshErrorKind::PreCommitFailed.should_rollback_after_commit());
        assert!(RefreshErrorKind::CommitFailedKnownUncommitted.should_rollback_after_commit());
        assert!(!RefreshErrorKind::CommitFailedKnownCommitted.should_rollback_after_commit());
        assert!(!RefreshErrorKind::CommitUnknown.should_rollback_after_commit());
        assert!(!RefreshErrorKind::MetadataFinalizeFailed.should_rollback_after_commit());
    }

    #[test]
    fn refresh_error_formats_message_with_kind() {
        let error = RefreshError::new(RefreshErrorKind::CommitUnknown, "commit state unknown");

        assert_eq!(error.kind, RefreshErrorKind::CommitUnknown);
        assert_eq!(error.to_string(), "commit state unknown");
    }

    #[test]
    fn storage_engine_maps_to_backend_name() {
        assert_eq!(MvStorageEngine::StarRocks.as_sql_str(), "starrocks");
        assert_eq!(MvStorageEngine::StarRocks.backend_name(), "starrocks");
        assert_eq!(MvStorageEngine::Iceberg.as_sql_str(), "iceberg");
        assert_eq!(MvStorageEngine::Iceberg.backend_name(), "iceberg");
        let err = MvStorageEngine::from_sql_str("starrocks").unwrap_err();
        assert!(err.contains("storage_engine='starrocks'"), "err={err}");
        assert_eq!(
            MvStorageEngine::from_sql_str("iceberg").unwrap(),
            MvStorageEngine::Iceberg
        );
        // Regression assertion: the legacy `"managed"` alias is no longer
        // accepted after the catalog rename. Do not change to "starrocks"
        // here — that's a valid value and would make the unwrap_err() panic.
        assert!(
            MvStorageEngine::from_sql_str("managed")
                .unwrap_err()
                .contains("unknown materialized view storage_engine"),
        );
        assert_eq!(
            MvStorageEngine::from_sql_str("duckdb").unwrap_err(),
            "unknown materialized view storage_engine `duckdb`"
        );
    }
}
