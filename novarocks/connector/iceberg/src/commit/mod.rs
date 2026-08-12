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

//! Provider-owned metadata commit primitives.
//!
//! These modules contain only Iceberg catalog/file-format facts and do not
//! depend on Core SQL, execution, or application state.

pub mod abort;
mod action;
mod collector;
mod data_file;
pub mod data_writer;
pub mod equality_delete_writer;
mod fast_append;
pub mod frozen_write;
mod helpers;
pub mod mv_provenance;
pub mod mv_refresh_ref;
mod overwrite;
mod overwrite_partitions;
pub mod position_delete_writer;
pub mod puffin_dv;
pub mod ref_action;
pub mod report;
pub mod retry;
mod rewrite_data_files;
mod row_delta;
mod row_delta_dv;
mod row_delta_dv_from_files;
pub mod row_delta_dv_metadata;
mod row_mutation_activation;
mod row_mutation_preparation;
mod run;
mod selected_rewrite;
pub mod service;
pub mod snapshot_lifecycle_helpers;
pub mod statistics;
mod truncate;
pub mod types;
mod update_cow;
pub mod validation;
pub mod variant_write;
pub mod write_control;
pub mod write_execution;
pub mod write_io;
mod write_preparation;
mod write_shared;

pub use abort::{AbortLog, CleanupError};
pub use equality_delete_writer::{EqualityDeleteColumn, write_equality_delete_file};
pub use mv_provenance::{
    MV_PROVENANCE_V1_PROP, MV_PROVENANCE_VERSION, MV_REFRESH_ROW_COUNT_PROP, MvProvenanceV1,
    ProvenanceBase, RefreshTechnique,
};
pub use mv_refresh_ref::{
    MV_ID_PROP, MV_REFRESH_ID_PROP, MV_REFRESH_TOKEN_PROP, MvRefreshPublishOutcome,
    MvRefreshPublishPlan, MvRefreshSnapshotMarker, publish_staging_branch_to_main,
    snapshot_matches_refresh_marker,
};
pub use position_delete_writer::{PositionDeleteGroup, write_position_delete_files};
pub use puffin_dv::{
    DeletionVector, DeletionVectorBlobInput, WrittenPuffinDv, read_deletion_vector_puffin,
    read_deletion_vector_puffin_with_range_reader, write_multi_deletion_vector_puffin,
    write_single_deletion_vector_puffin,
};
pub use ref_action::{
    RefAction, RefActionOutcome, RefActionPlan, execute_ref_action, lower_ref_action,
};
pub use retry::{
    COMMIT_RETRY_BACKOFF_MS, COMMIT_RETRY_MAX_ATTEMPTS, commit_with_retry,
    is_retryable_commit_conflict,
};
pub use rewrite_data_files::{
    LiveDataFileCompactionStats, current_live_data_file_compaction_stats,
};
pub(crate) use service::RecoveryEvidence;
pub use service::{
    CleanupAttempt, CommitFailureKind, CommitServiceError, CommitServiceOutcome,
    classify_commit_error,
};
pub use snapshot_lifecycle_helpers::{
    FileSet, build_dv_index, compute_live_snapshot_set, enumerate_files_for_snapshots,
    is_puffin_path, puffin_half_reference_protection,
};
pub use types::{
    CommitOpKind, CommitOutcome, IcebergUpdateMode, IcebergWriteMode, NOVAROCKS_UPDATE_MODE,
    NOVAROCKS_UPDATE_MODE_COW, NOVAROCKS_UPDATE_MODE_MOR, WrittenFile,
};
pub use validation::{
    classify_iceberg_write_mode, ensure_column_id_not_regressed,
    ensure_default_sort_order_resolvable, ensure_equality_delete_single_partition_spec,
    ensure_iceberg_write_supported, ensure_iceberg_write_supported_from_metadata,
    ensure_no_equality_deletes, ensure_overwrite_single_partition_spec,
    ensure_partition_id_not_regressed, ensure_single_partition_spec, match_select_schema_to_table,
    row_mutation_strategy_from_metadata,
};
pub use write_control::{IcebergWriteControl, IcebergWriteReconcileEvidenceV1};

pub(crate) use action::CommitCtx;
pub(crate) use collector::IcebergCommitCollector;
pub(crate) use fast_append::build_staged_fast_append_action;
pub(crate) use run::{CleanupPathMapper, RunInput, run_iceberg_commit};
pub(crate) use update_cow::{CowUpdateRewriteSet, CowUpdateTouchedFile};
