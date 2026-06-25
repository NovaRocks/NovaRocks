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

//! Standalone-mode iceberg `TRUNCATE TABLE` entry point.
//!
//! Routes from `statement::execute_truncate_table_statement` for any iceberg
//! target (with optional branch suffix `t.branch_<name>` resolved at parse
//! time and threaded through as `target_ref`).
//!
//! Mirrors `iceberg_writer::execute_iceberg_insert_or_overwrite` structurally,
//! minus the chunk → data-file phase: TRUNCATE writes no new files, only a
//! `operation=delete` snapshot whose manifests mark every live entry as
//! DELETED. The `IcebergCommitCollector` is built with
//! `CommitOpKind::Truncate` and no `inject_written_file` calls; the
//! `TruncateCommit` action then drives the manifest writes through
//! `run_iceberg_commit_typed` exactly the same way `OverwriteCommit` does.

use std::collections::BTreeMap;
use std::sync::Arc;

use iceberg::Catalog;
use iceberg::{NamespaceIdent, TableIdent};

use crate::connector::backend::ResolvedTable;
use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::connector::iceberg::commit::{
    CommitOpKind, IcebergCommitCollector, RunInput, run_iceberg_commit_typed,
};
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::iceberg_writer::{
    build_abort_cleanup_for_catalog_entry, invalidate_iceberg_caches,
};
use crate::engine::{StandaloneState, StatementResult};

pub(crate) fn execute_iceberg_truncate_table(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    _resolved: &ResolvedTable,
    target_ref: &str,
) -> Result<StatementResult, String> {
    debug_assert_eq!(target.backend_name, "iceberg");

    // 1. Resolve catalog entry + build iceberg-rust Catalog handle.
    //    Mirrors iceberg_writer.rs:73-93.
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog: Arc<dyn Catalog> = build_iceberg_catalog(&entry)?;
    let table_ident = TableIdent::new(
        NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|e| format!("load iceberg table {}: {e}", target_string(target)))?;

    // 2. Branch writes require Iceberg v3 (row-lineage semantics) — same rule
    //    as branch INSERT/UPDATE/DELETE/MERGE per iceberg_writer.rs:101-110.
    if target_ref != "main" {
        let fmt = table.metadata().format_version();
        if fmt != iceberg::spec::FormatVersion::V3 {
            return Err(format!(
                "iceberg ref: branch TRUNCATE requires Iceberg v3 tables (table {} is v{})",
                target_string(target),
                fmt as u8,
            ));
        }
    }

    // 3. Build the collector. TRUNCATE never adds files, so no
    //    `inject_written_file` calls — the collector exists only to carry the
    //    op-kind, table identifier, base snapshot id, sequence number, schema,
    //    and partition spec into `run_iceberg_commit_typed`.
    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(
        IcebergCommitCollector::new(
            CommitOpKind::Truncate,
            table_ident,
            metadata.current_snapshot().map(|s| s.snapshot_id()),
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            staging_dir,
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(metadata.clone()),
    );

    // 4. Build the OpenDAL Operator + FileIO for abort cleanup.
    let abort_cleanup = build_abort_cleanup_for_catalog_entry(&entry)?;
    let file_io = table.file_io().clone();

    // 5. Drive commit + abort cleanup on failure.
    let _outcome = block_on_iceberg(async {
        run_iceberg_commit_typed(RunInput {
            collector: collector.clone(),
            catalog: catalog.clone(),
            table,
            fs: abort_cleanup.fs,
            file_io,
            cleanup_path_mapper: abort_cleanup.path_mapper,
            cow_update_rewrite: None,
            target_ref: target_ref.to_string(),
            snapshot_properties: BTreeMap::new(),
        })
        .await
    })
    .map_err(|err| format!("iceberg truncate commit runtime failed: {err}"))?
    .map_err(iceberg_commit_error_to_user_message)?;

    // 6. Invalidate the iceberg entry's table cache so subsequent SELECTs
    //    see the new (zero-row) snapshot.
    invalidate_iceberg_caches(state, target)?;
    crate::engine::dictionary::maintenance::mark_target_stale(state, target)?;

    Ok(StatementResult::Ok)
}

fn target_string(t: &TargetBackend) -> String {
    format!("{}.{}.{}", t.catalog, t.namespace, t.table)
}

fn iceberg_commit_error_to_user_message(
    err: crate::connector::iceberg::commit::CommitServiceError,
) -> String {
    crate::common::engine_error::EngineError::from(err).to_bracketed_user_message()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::commit::{CommitOpKind, CommitServiceError, RecoveryEvidence};

    #[test]
    fn truncate_formats_commit_unknown_at_boundary() {
        let staging_dir = "s3://warehouse/db/t/data/_staging/truncate-attempt".to_string();
        let err = CommitServiceError::unknown(
            "catalog transport disconnected".to_string(),
            RecoveryEvidence {
                table_ident: "db.t".to_string(),
                op_kind: CommitOpKind::Truncate,
                base_snapshot_id: Some(42),
                base_sequence_number: 7,
                staging_dir: staging_dir.clone(),
            },
        );

        let message = iceberg_commit_error_to_user_message(err);
        assert!(
            message.starts_with("[CommitUnknown] iceberg commit unknown"),
            "got: {message}"
        );
        assert!(
            message.contains(&staging_dir),
            "message should contain staging dir {staging_dir}, got: {message}"
        );
    }
}
