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

//! Standalone-mode `DELETE FROM iceberg ... WHERE ...` entry point.
//!
//! Phase 1.0 status: validation + commit-action plumbing is in place.
//! The actual DELETE execution requires the analyzer/planner to project
//! the iceberg virtual columns `_file` and `_pos` through to the scan
//! lowering layer (which already supports them — see
//! [`crate::lower::node::hdfs_scan`] and
//! [`crate::exec::row_position::is_iceberg_file_path`]). The analyzer side
//! is a separate refactor tracked under Plan Task 14B and not part of
//! Phase 1.0.
//!
//! What runs today:
//! 1. Resolve the iceberg target.
//! 2. Run pre-lowering validators (v3-writable, single partition spec,
//!    no equality deletes).
//! 3. Surface a clear, action-oriented error so callers know exactly which
//!    follow-up unblocks DELETE.
//!
//! When the analyzer side lands, the stub below grows into:
//! ```text
//! synthesize_query: SELECT _file, _pos FROM <table> WHERE <predicate>
//! → execute_query → query_result_to_chunks
//! → group rows by _file, write position-delete Parquet via DataFileWriter
//! → IcebergCommitCollector::inject_written_file (content = PositionDeletes,
//!   referenced_data_file = <_file group>)
//! → run_iceberg_commit (op_kind = RowDelta) — already implemented in
//!   `connector::iceberg::commit::row_delta`.
//! ```

use std::sync::Arc;

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_hadoop_catalog};
use crate::connector::iceberg::commit::{
    ensure_no_equality_deletes, ensure_single_partition_spec, ensure_v3_writable,
};
use crate::engine::backend_resolver::resolve_existing_table_target;
use crate::engine::{StandaloneState, StatementResult};
use crate::sql::parser::ast::DeleteStmt;

pub(crate) fn execute_delete_statement(
    state: &Arc<StandaloneState>,
    stmt: &DeleteStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    // 1. Resolve the table; only iceberg backend is supported.
    let target =
        resolve_existing_table_target(state, &stmt.table, current_catalog, current_database)?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "phase 1 DELETE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }

    // 2. Build a Catalog handle and load the table so validators have the
    //    real metadata to inspect.
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let hadoop_catalog = build_hadoop_catalog(&entry)?;
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    use iceberg::Catalog;
    let table = block_on_iceberg(async { hadoop_catalog.load_table(&table_ident).await })?
        .map_err(|e| {
            format!(
                "load iceberg table {}.{}.{}: {e}",
                target.catalog, target.namespace, target.table
            )
        })?;

    // 3. Pre-lowering validators (these run regardless of execution
    //    readiness so users learn about table-level limitations early).
    ensure_v3_writable(&table)?;
    ensure_single_partition_spec(&table)?;
    ensure_no_equality_deletes(&table)?;

    // 4. The remaining SCAN + position-delete-write + commit pipeline is
    //    tracked under Plan Task 14B. Surface the gap explicitly so
    //    integration tests (Task 17 NEG-* etc.) get a deterministic error
    //    they can match against, and operators see the scope clearly.
    Err(format!(
        "DELETE FROM iceberg `{ident}` WHERE ... is not yet wired up in this build. \
         Phase 1.0 ships the AST surface, validators, and the commit-action \
         (`RowDeltaCommit`) needed for the operation, but the analyzer/planner \
         must project the iceberg `_file` / `_pos` virtual columns before \
         row positions can flow into the position-delete writer. See \
         `src/exec/row_position.rs` and `src/lower/node/hdfs_scan.rs:420-440` \
         for the existing scan-side support and Plan Task 14B for the analyzer \
         work that finishes wiring the path. WHERE clause was: {where_clause:?}",
        ident = table_ident,
        where_clause = stmt.where_clause,
    ))
}
