// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! SQL-owned artifacts for materialized-view refresh.
//!
//! These artifacts describe immutable SQL and refresh facts. They never carry
//! result batches, catalog handles, or a connector implementation.

use std::collections::BTreeMap;

use novarocks_spi::connector::ConnectorExecutionBindingKey;

use crate::mv::model::MvTarget;
use crate::query_execution::prepared_write::PreparedDistributedWriteRequest;
use crate::sql::parser::ast::RefreshMaterializedViewStmt;

pub(crate) mod first_refresh;

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
            return Err("REFRESH MATERIALIZED VIEW FULL is not supported".to_string());
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
    },
}

/// The only value transferred from SQL preparation to the frontend MV
/// application lifecycle. It contains semantic facts and native-wire plans,
/// never a repository, catalog client, provider codec, or local executor.
pub struct PreparedMvRefresh {
    pub statement: MvRefreshStatement,
    pub observed_binding: ConnectorExecutionBindingKey,
    pub finalize: MvRefreshFinalizeFacts,
    pub work: PreparedMvRefreshWork,
}

/// SQL preparation port consumed by the frontend lifecycle owner.
pub trait MvRefreshPreparationService: Send + Sync {
    fn prepare_step(
        &self,
        statement: &MvRefreshStatement,
        target: &MvTarget,
    ) -> Result<PreparedMvRefresh, String>;
}

#[cfg(test)]
mod tests {
    use super::MvRefreshStatement;

    #[test]
    fn full_refresh_remains_an_explicitly_unsupported_request() {
        let error = MvRefreshStatement {
            name_parts: vec!["mv".to_string()],
            full: true,
        }
        .validate_supported()
        .expect_err("FULL must not silently downgrade");

        assert_eq!(error, "REFRESH MATERIALIZED VIEW FULL is not supported");
    }
}
