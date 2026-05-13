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

//! Foundational types for Iceberg commit operations.
//!
//! Exports [`CommitOpKind`] (which commit action to run), [`IcebergWriteMode`]
//! (which table write semantics apply), [`WrittenFile`] (metadata for a single
//! Parquet file produced by the pipeline), and [`CommitOutcome`] (result of a
//! successful commit).

mod abort;
mod action;
mod collector;
mod data_file;
mod equality_delete_writer;
pub mod expire_snapshots;
mod fast_append;
mod helpers;
mod mv_refresh_ref;
mod overwrite;
mod overwrite_partitions;
mod position_delete_writer;
mod puffin_dv;
mod ref_action;
pub mod remove_orphan_files;
pub mod retry;
mod rewrite_data_files;
pub mod rewrite_manifests;
mod row_delta;
mod row_delta_dv;
mod run;
pub mod snapshot_lifecycle_helpers;
#[cfg(test)]
mod test_helpers;
mod truncate;
mod types;
mod update_cow;
mod validation;

pub use abort::{AbortLog, CleanupError};
pub use action::{CommitCtx, IcebergCommitAction};
pub use collector::IcebergCommitCollector;
pub use equality_delete_writer::{EqualityDeleteColumn, write_equality_delete_file};
pub use fast_append::FastAppendCommit;
pub use mv_refresh_ref::{
    MV_ID_PROP, MV_REFRESH_ID_PROP, MV_REFRESH_TOKEN_PROP, MvRefreshPublishOutcome,
    MvRefreshPublishPlan, MvRefreshSnapshotMarker, publish_staging_branch_to_main,
    snapshot_matches_refresh_marker,
};
pub use overwrite::OverwriteCommit;
pub use overwrite_partitions::OverwritePartitionsCommit;
pub use position_delete_writer::{PositionDeleteGroup, write_position_delete_files};
pub use puffin_dv::{
    DeletionVector, WrittenPuffinDv, read_deletion_vector_puffin,
    write_single_deletion_vector_puffin,
};
pub use ref_action::{RefAction, RefActionOutcome, RefActionPlan, execute_ref_action};
pub use retry::{commit_with_retry, is_retryable_commit_conflict};
pub use rewrite_data_files::RewriteDataFilesCommit;
pub(crate) use rewrite_data_files::count_current_live_files;
pub use row_delta::RowDeltaCommit;
pub use row_delta_dv::RowDeltaDvCommit;
pub use run::{CleanupPathMapper, RunInput, run_iceberg_commit};
pub use truncate::TruncateCommit;
pub use types::{
    CommitOpKind, CommitOutcome, IcebergSqlDeleteStrategy, IcebergUpdateMode, IcebergWriteMode,
    NOVAROCKS_UPDATE_MODE, NOVAROCKS_UPDATE_MODE_COW, NOVAROCKS_UPDATE_MODE_MOR, WrittenFile,
};
pub use update_cow::{CowUpdateCommit, CowUpdateRewriteSet, CowUpdateTouchedFile};
pub use validation::{
    classify_iceberg_write_mode, classify_sql_delete_strategy,
    ensure_equality_delete_single_partition_spec, ensure_iceberg_write_supported,
    ensure_no_equality_deletes, ensure_no_variant_columns_for_row_level_mutation,
    ensure_overwrite_single_partition_spec, ensure_single_partition_spec,
    match_select_schema_to_table, select_iceberg_update_mode,
};
