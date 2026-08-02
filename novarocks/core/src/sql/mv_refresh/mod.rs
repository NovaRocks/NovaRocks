// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! SQL-owned artifacts for materialized-view refresh.
//!
//! These artifacts describe immutable SQL and refresh facts. They never carry
//! result batches, catalog handles, or a connector implementation.

use std::collections::BTreeMap;

use novarocks_spi::connector::{ConnectorExecutionBindingKey, ConnectorWriteOperationId};

use crate::mv::model::MvTarget;
pub use crate::query_execution::prepared_write::PreparedDistributedWriteRequest;
use crate::sql::parser::ast::RefreshMaterializedViewStmt;

pub mod first_refresh;
pub mod incremental;

pub(crate) const FULL_REFRESH_DISABLED_MESSAGE: &str = "REFRESH MATERIALIZED VIEW ... FULL is currently disabled pending redesign; \
     its previous behavior (drop target + delete definition + recreate empty target) \
     was misleading and non-atomic. To recover from a broken contract or corrupted \
     target, run DROP MATERIALIZED VIEW <name>; CREATE MATERIALIZED VIEW <name> ...; \
     REFRESH MATERIALIZED VIEW <name>; manually.";

/// Typed SQL projection of `REFRESH MATERIALIZED VIEW`.
///
/// It intentionally preserves `FULL`: the preparation service rejects that
/// unsupported request instead of allowing an application route to silently
/// downgrade it to incremental refresh.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshStatement {
    pub name_parts: Vec<String>,
    pub full: bool,
}

impl From<&RefreshMaterializedViewStmt> for MvRefreshStatement {
    fn from(statement: &RefreshMaterializedViewStmt) -> Self {
        Self {
            name_parts: statement.name.parts.clone(),
            full: statement.full,
        }
    }
}

impl MvRefreshStatement {
    pub fn validate_supported(&self) -> Result<(), String> {
        if self.full {
            return Err(FULL_REFRESH_DISABLED_MESSAGE.to_string());
        }
        Ok(())
    }
}

/// SQL facts that the frontend needs to atomically finalize the MV definition
/// after provider publication. The values are observed during preparation;
/// the frontend never resolves catalog metadata itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshFinalizeFacts {
    pub mv_id: i64,
    pub target: MvTarget,
    pub base_snapshots: BTreeMap<String, Option<i64>>,
    pub base_table_uuids: BTreeMap<String, String>,
    pub expected_target_snapshot_id: Option<i64>,
}

/// Frontend-preallocated identities that SQL embeds into an inert refresh
/// preparation.  Generation acquisition and intent persistence remain
/// frontend work; SQL uses these values only to stamp provider planning and
/// the native write artifact consistently.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshAttemptIdentity {
    pub refresh_id: i64,
    pub request_id: [u8; 16],
    pub staging_branch: String,
    pub marker_token: String,
    pub staging_create_operation_id: [u8; 16],
    pub write_operation_id: ConnectorWriteOperationId,
    pub publication_operation_id: [u8; 16],
    pub staging_drop_operation_id: [u8; 16],
}

/// All values SQL needs to prepare one refresh step.  The application owner
/// allocates the attempt identity before entering SQL so every generated
/// native write template is tied to the ledger intent that will be persisted
/// after exact-generation admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshPreparationRequest {
    pub statement: MvRefreshStatement,
    pub target: MvTarget,
    pub attempt: MvRefreshAttemptIdentity,
}

impl MvRefreshPreparationRequest {
    pub fn validate(&self) -> Result<(), String> {
        self.statement.validate_supported()?;
        self.attempt.validate()
    }
}

impl MvRefreshAttemptIdentity {
    pub fn validate(&self) -> Result<(), String> {
        if self.refresh_id <= 0 || self.staging_branch.is_empty() || self.marker_token.is_empty() {
            return Err(
                "MV refresh preparation requires a positive identity and non-empty staging marker"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// The semantic class of a side-effect-free refresh preparation.
pub enum PreparedMvRefreshWork {
    /// No source data exists. There is neither a writer nor a staging ref.
    NoOp,
    /// Source versions are unchanged; only durable MV metadata advances.
    MetadataOnly,
    /// Each physical write request is already encoded for native execution but
    /// remains unbound until the frontend holds the exact connector lease.
    DataProducing {
        distributed_writes: Vec<PreparedDistributedWriteRequest>,
        /// SQL-shaped first-refresh artifacts whose provider activation and
        /// native fragment preparation are deferred until the frontend holds
        /// the exact retained write lease.
        first_refresh_writes: Vec<first_refresh::PreparedMvFirstRefreshWrite>,
        /// SQL-owned incremental change-stream artifacts whose provider
        /// activation and native fragment preparation are deferred until the
        /// frontend holds the exact retained write lease.
        incremental_writes: Vec<incremental::PreparedMvIncrementalWrite>,
    },
}

/// The only value transferred from SQL preparation to the frontend MV
/// application lifecycle. It contains semantic facts and native-wire plans,
/// never a repository, catalog client, provider codec, or local executor.
pub struct PreparedMvRefresh {
    pub statement: MvRefreshStatement,
    pub attempt: MvRefreshAttemptIdentity,
    pub observed_binding: ConnectorExecutionBindingKey,
    pub finalize: MvRefreshFinalizeFacts,
    pub work: PreparedMvRefreshWork,
}

/// SQL preparation port consumed by the frontend lifecycle owner.
pub trait MvRefreshPreparationService: Send + Sync {
    fn prepare_step(
        &self,
        request: MvRefreshPreparationRequest,
    ) -> Result<PreparedMvRefresh, String>;
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::ConnectorWriteOperationId;

    use crate::mv::model::MvTarget;

    use super::{
        FULL_REFRESH_DISABLED_MESSAGE, MvRefreshAttemptIdentity, MvRefreshPreparationRequest,
        MvRefreshStatement,
    };

    #[test]
    fn full_refresh_remains_an_explicitly_unsupported_request() {
        let error = MvRefreshStatement {
            name_parts: vec!["mv".to_string()],
            full: true,
        }
        .validate_supported()
        .expect_err("FULL must not silently downgrade");

        assert_eq!(error, FULL_REFRESH_DISABLED_MESSAGE);
    }

    #[test]
    fn preparation_requires_a_frontend_preallocated_attempt_identity() {
        let valid = MvRefreshAttemptIdentity {
            refresh_id: 7,
            request_id: [1; 16],
            staging_branch: "__nova_mv_7".to_string(),
            marker_token: "marker".to_string(),
            staging_create_operation_id: [2; 16],
            write_operation_id: ConnectorWriteOperationId::from_bytes([3; 16]),
            publication_operation_id: [4; 16],
            staging_drop_operation_id: [5; 16],
        };
        valid.validate().expect("complete attempt identity");

        let missing_marker = MvRefreshAttemptIdentity {
            marker_token: String::new(),
            ..valid
        };
        assert!(missing_marker.validate().is_err());
    }

    #[test]
    fn preparation_request_keeps_full_rejection_and_attempt_identity_together() {
        let request = MvRefreshPreparationRequest {
            statement: MvRefreshStatement {
                name_parts: vec!["mv".to_string()],
                full: false,
            },
            target: MvTarget {
                catalog: Some("iceberg".to_string()),
                database: "db".to_string(),
                name: "mv".to_string(),
            },
            attempt: MvRefreshAttemptIdentity {
                refresh_id: 7,
                request_id: [1; 16],
                staging_branch: "__nova_mv_7".to_string(),
                marker_token: "marker".to_string(),
                staging_create_operation_id: [2; 16],
                write_operation_id: ConnectorWriteOperationId::from_bytes([3; 16]),
                publication_operation_id: [4; 16],
                staging_drop_operation_id: [5; 16],
            },
        };
        request.validate().expect("complete request");

        let full = MvRefreshPreparationRequest {
            statement: MvRefreshStatement {
                full: true,
                ..request.statement
            },
            ..request
        };
        assert!(full.validate().is_err());
    }
}
