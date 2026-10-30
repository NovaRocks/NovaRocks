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

mod action;
pub(crate) mod attempt;
mod data_file;
pub mod data_writer;
pub(crate) mod dependency;
pub mod equality_delete_writer;
pub(crate) mod fast_append;
pub mod frozen_write;
pub(crate) mod helpers;
pub(crate) mod model;
pub mod mv_provenance;
pub mod mv_publication_fence;
pub mod mv_refresh_ref;
pub(crate) mod operation;
pub(crate) mod overwrite;
mod overwrite_partitions;
pub mod position_delete_writer;
pub(crate) mod publication_descriptor;
pub mod puffin_dv;
pub(crate) mod recovery;
pub mod ref_action;
pub mod report;
mod rewrite_data_files;
mod row_delta;
mod row_delta_dv_from_files;
pub mod row_delta_dv_metadata;
mod row_mutation_preparation;
#[cfg(test)]
pub(crate) mod run;
mod selected_rewrite;
pub(crate) mod slot_change;
pub mod snapshot_lifecycle_helpers;
pub(crate) mod staging;
pub mod statistics;
pub(crate) mod truncate;
pub mod types;
mod update_cow;
pub mod validation;
mod write_adaptation;
pub mod write_control;
pub mod write_fence;
pub mod write_io;
mod write_preparation;
mod write_shared;
pub mod write_stack;

pub use mv_provenance::{
    MV_PUBLICATION_PROVENANCE_PROP, MV_PUBLICATION_PROVENANCE_VERSION, MvPublicationProvenanceV2,
    ProvenanceBase, RefreshTechnique,
};
pub use mv_refresh_ref::{
    MV_PUBLICATION_ID_PROP, MV_PUBLICATION_STAGING_REF_PREFIX, MvPublicationSnapshotMarker,
    MvRefreshPublishPlan, snapshot_matches_publication_marker,
};
pub use position_delete_writer::{PositionDeleteGroup, write_position_delete_files};
pub use puffin_dv::{
    DeletionVector, DeletionVectorBlobInput, WrittenPuffinDv, read_deletion_vector_puffin,
    read_deletion_vector_puffin_with_range_reader, write_multi_deletion_vector_puffin,
    write_single_deletion_vector_puffin,
};
pub use ref_action::{RefAction, RefActionPlan, lower_ref_action};
pub use rewrite_data_files::{
    LiveDataFileCompactionStats, current_live_data_file_compaction_stats,
};
pub(crate) use rewrite_data_files::{LiveFiles, append_live_manifest, compactable_file_groups};
pub use snapshot_lifecycle_helpers::{
    FileSet, build_dv_index, compute_live_snapshot_set, enumerate_files_for_snapshots,
    is_puffin_path, puffin_half_reference_protection,
};
pub use types::{
    CommitOpKind, IcebergUpdateMode, IcebergWriteMode, NOVAROCKS_UPDATE_MODE,
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
pub use write_control::IcebergWriteControl;

pub(crate) use data_file::from_written_file as frozen_data_file_from_written;
pub(crate) use fast_append::FastAppendPreparer;
pub(crate) use update_cow::{CowUpdateRewriteSet, CowUpdateTouchedFile};
