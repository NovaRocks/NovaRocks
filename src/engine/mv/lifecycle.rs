//! Typed materialized-view refresh lifecycle contracts.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, RefreshMaterializedViewStmt,
    ShowMaterializedViewsStmt,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MvStorageEngine {
    ManagedLake,
    Iceberg,
}

impl MvStorageEngine {
    pub(crate) fn as_sql_str(self) -> &'static str {
        match self {
            Self::ManagedLake => "managed_lake",
            Self::Iceberg => "iceberg",
        }
    }

    pub(crate) fn backend_name(self) -> &'static str {
        match self {
            Self::ManagedLake => "managed",
            Self::Iceberg => "iceberg",
        }
    }

    pub(crate) fn from_sql_str(value: &str) -> Result<Self, String> {
        match value.to_ascii_lowercase().as_str() {
            "managed_lake" | "managed" => Ok(Self::ManagedLake),
            "iceberg" => Ok(Self::Iceberg),
            _ => Err(format!(
                "unknown materialized view storage_engine `{value}`"
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvTarget {
    pub catalog: Option<String>,
    pub database: String,
    pub name: String,
}

impl MvTarget {
    pub(crate) fn display_name(&self) -> String {
        match self.catalog.as_deref() {
            Some(catalog) => format!("{catalog}.{}.{}", self.database, self.name),
            None => format!("{}.{}", self.database, self.name),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvBaseRef {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl MvBaseRef {
    pub(crate) fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.table)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefreshMode {
    Noop,
    Full,
    Incremental,
    Rebuild,
}

#[derive(Clone, Debug)]
pub(crate) struct CreateMvRequest {
    pub stmt: CreateMaterializedViewStmt,
    pub current_catalog: Option<String>,
    pub current_database: String,
}

#[derive(Clone, Debug)]
pub(crate) struct DropMvRequest {
    pub stmt: DropMaterializedViewStmt,
    pub current_catalog: Option<String>,
    pub current_database: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ListMvsRequest {
    pub stmt: ShowMaterializedViewsStmt,
    pub current_catalog: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct RefreshRequest {
    pub target: MvTarget,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub statement: RefreshMaterializedViewStmt,
}

#[derive(Clone, Debug)]
pub(crate) struct RefreshPlan {
    pub mv_id: Option<i64>,
    pub target: MvTarget,
    pub storage_engine: MvStorageEngine,
    pub mode: RefreshMode,
    pub base_refs: Vec<MvBaseRef>,
    pub snapshot_pins: BTreeMap<String, Option<i64>>,
    pub backend_plan: BackendRefreshPlan,
}

#[derive(Clone, Debug)]
pub(crate) enum BackendRefreshPlan {
    ManagedLake(ManagedLakeRefreshPlan),
    Iceberg(IcebergRefreshPlan),
}

#[derive(Clone, Debug)]
pub(crate) struct ManagedLakeRefreshPlan {
    pub stmt: RefreshMaterializedViewStmt,
    pub current_catalog: Option<String>,
    pub current_database: String,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergRefreshPlan {
    pub stmt: RefreshMaterializedViewStmt,
    pub current_catalog: Option<String>,
    pub current_database: String,
}

#[derive(Clone, Debug)]
pub(crate) struct RefreshOutcome {
    pub mv_id: Option<i64>,
    pub target: MvTarget,
    pub rows: Option<i64>,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_uuids: BTreeMap<String, String>,
    pub target_snapshot_id: Option<i64>,
    pub backend_outcome: BackendRefreshOutcome,
}

#[derive(Clone, Debug)]
pub(crate) enum BackendRefreshOutcome {
    ManagedLake(ManagedLakeRefreshOutcome),
    Iceberg(IcebergRefreshOutcome),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ManagedLakeRefreshOutcome {
    pub completed_inside_execute: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct IcebergRefreshOutcome {
    pub completed_inside_execute: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RefreshCtx {
    pub refresh_id: Option<i64>,
    pub expected_target_snapshot_id: Option<i64>,
    pub recovery_required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvListRow {
    pub name: String,
    pub database: String,
    pub storage_engine: String,
    pub refresh_mode: String,
    pub last_refresh_time: Option<String>,
    pub last_refresh_rows: Option<String>,
    pub base_tables: String,
    pub select_text: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefreshErrorKind {
    UserError,
    PreCommitFailed,
    CommitFailedKnownUncommitted,
    CommitFailedKnownCommitted,
    CommitUnknown,
    MetadataFinalizeFailed,
}

impl RefreshErrorKind {
    pub(crate) fn should_rollback_after_commit(self) -> bool {
        matches!(
            self,
            Self::UserError | Self::PreCommitFailed | Self::CommitFailedKnownUncommitted
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshError {
    pub kind: RefreshErrorKind,
    pub message: String,
}

impl RefreshError {
    pub(crate) fn new(kind: RefreshErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn user(message: impl Into<String>) -> Self {
        Self::new(RefreshErrorKind::UserError, message)
    }

    pub(crate) fn pre_commit(message: impl Into<String>) -> Self {
        Self::new(RefreshErrorKind::PreCommitFailed, message)
    }

    pub(crate) fn commit_unknown(message: impl Into<String>) -> Self {
        Self::new(RefreshErrorKind::CommitUnknown, message)
    }

    pub(crate) fn metadata_finalize(message: impl Into<String>) -> Self {
        Self::new(RefreshErrorKind::MetadataFinalizeFailed, message)
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
        assert_eq!(MvStorageEngine::ManagedLake.as_sql_str(), "managed_lake");
        assert_eq!(MvStorageEngine::ManagedLake.backend_name(), "managed");
        assert_eq!(MvStorageEngine::Iceberg.as_sql_str(), "iceberg");
        assert_eq!(MvStorageEngine::Iceberg.backend_name(), "iceberg");
        assert_eq!(
            MvStorageEngine::from_sql_str("managed_lake").unwrap(),
            MvStorageEngine::ManagedLake
        );
        assert_eq!(
            MvStorageEngine::from_sql_str("managed").unwrap(),
            MvStorageEngine::ManagedLake
        );
        assert_eq!(
            MvStorageEngine::from_sql_str("iceberg").unwrap(),
            MvStorageEngine::Iceberg
        );
        assert_eq!(
            MvStorageEngine::from_sql_str("duckdb").unwrap_err(),
            "unknown materialized view storage_engine `duckdb`"
        );
    }
}
