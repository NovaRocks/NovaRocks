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

//! IVM `IcebergDeltaScan` ExecNode: snapshot-range delta source.
//!
//! Single source leaf that internally consumes Iceberg snapshot diff
//! products (data files / position-delete / equality-delete / deleted-data-file)
//! and emits a unified chunk stream tagged with the A4 transparent
//! `__change_op` column (+1 for INSERT, -1 for DELETE). Populated by
//! `lower_iceberg_delta_scan` (in `src/lower/thrift/iceberg_delta_scan.rs`)
//! when the Thrift plan carries `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE`.

use std::sync::Arc;

use crate::exec::chunk::ChunkSchemaRef;
use crate::fs::object_store::ObjectStoreConfig;
use crate::fs::opendal::OpendalRangeReaderFactory;

#[derive(Clone, Debug)]
pub struct IcebergDeltaScanNode {
    pub base_table_ident: BaseTableIdent,
    pub from_snapshot_id: i64,
    pub to_snapshot_id: i64,
    pub output_chunk_schema: ChunkSchemaRef,
    pub apply_key_source: ApplyKeySource,
    pub change_files: Vec<DeltaSourceFile>,
    pub object_store_config: Option<ObjectStoreConfig>,
    pub iceberg_runtime: Arc<IcebergRuntimeHandles>,
    pub node_id: i32,
}

/// Three-part identifier of the base Iceberg table that an `IcebergDeltaScan`
/// reads from. Distinct from `iceberg::TableIdent` (which carries a richer
/// `NamespaceIdent`); this struct holds raw normalized strings for matching
/// against NovaRocks-internal MV refresh state.
#[derive(Clone, Debug)]
pub struct BaseTableIdent {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

#[derive(Clone, Debug)]
pub enum ApplyKeySource {
    /// A9 hidden apply key: base table's `_row_id` v3 row lineage column.
    BaseRowId,
}

#[derive(Clone, Debug)]
pub struct DeltaSourceFile {
    pub path: String,
    pub size: i64,
    pub role: DeltaSourceRole,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}

#[derive(Clone, Debug)]
pub enum DeltaSourceRole {
    DataFile,
    PositionDelete {
        targets: Vec<PositionDeleteTargetData>,
        /// Position-delete encoding: Parquet for the v2 (positional rows)
        /// format, Puffin for the v3 (deletion vector blob) format. The
        /// operator dispatches `scan_position_delete_rows_for_targets`
        /// accordingly. Other variants are rejected at plan time.
        file_format: PositionDeleteFileFormat,
        /// Required when `file_format == Puffin`: byte offset of the
        /// `deletion-vector-v1` blob inside the Puffin file. `None` for
        /// the Parquet position-delete format.
        content_offset: Option<i64>,
        /// Required when `file_format == Puffin`: byte length of the
        /// `deletion-vector-v1` blob inside the Puffin file. `None` for
        /// the Parquet position-delete format.
        content_size_in_bytes: Option<i64>,
    },
    EqualityDelete {
        equality_field_ids: Vec<i32>,
        targets: Vec<EqualityDeleteTargetData>,
    },
    DeletedDataFile {
        previous_data_file_visibility: Option<DeletedFileVisibility>,
    },
}

/// Encoding of a position-delete file. Mirrors the subset of
/// `iceberg::spec::DataFileFormat` that IVM-A1 supports for the position
/// delete role; the lowering pass rejects any other format. We carry it
/// here (rather than re-using `iceberg::spec::DataFileFormat`) so the
/// `IcebergDeltaScanNode` does not need to leak the wider iceberg enum
/// through its public type surface — and so adding new formats in the
/// future is a single localized change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PositionDeleteFileFormat {
    /// v2 position-delete file (one row per (file, pos) pair, parquet-encoded).
    Parquet,
    /// v3 deletion-vector file (Puffin blob with `deletion-vector-v1` body).
    Puffin,
}

#[derive(Clone, Debug)]
pub struct PositionDeleteTargetData {
    pub data_file_path: String,
    pub data_file_first_row_id: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct EqualityDeleteTargetData {
    pub data_file_path: String,
    pub data_file_first_row_id: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct DeletedFileVisibility {
    pub already_deleted_positions: Vec<i64>,
}

/// Iceberg per-table runtime handles required by `IcebergDeltaScanOperator`
/// to open delete files and re-read target data files. Constructed by
/// `lower_iceberg_delta_scan` when lowering `ICEBERG_DELTA_SCAN_NODE`:
/// - `base_table` comes from `iceberg::Catalog::load_table`
/// - `object_store_factory` is built once via `build_factory_for_table` and
///   shared across role scanners
/// - `delete_side` is populated via `base_data_file_lineage_index` +
///   `load_existing_delete_visibility_by_data_file_at` only when the change
///   batch contains DELETE-side roles (position / equality / deleted-data-file).
#[derive(Debug)]
pub struct IcebergRuntimeHandles {
    pub base_table: iceberg::table::Table,
    pub object_store_factory: Arc<OpendalRangeReaderFactory>,
    pub delete_side: Option<DeltaScanDeleteSide>,
}

/// Per-target-data-file v3 row-lineage metadata required by the delete-side
/// scanners in `IcebergDeltaScanOperator` to synthesize the
/// `_file` / `_pos` / `_row_id` / `_last_updated_sequence_number` virtual
/// columns when reverse-projecting deleted rows. Filled in from the relevant
/// snapshot read views.
#[derive(Clone, Copy, Debug)]
pub struct BaseDataFileLineage {
    pub first_row_id: i64,
    pub data_sequence_number: i64,
}

#[derive(Debug)]
pub struct DeltaScanDeleteSide {
    pub base_data_file_lineage: std::collections::HashMap<String, BaseDataFileLineage>,
    pub(crate) previous_delete_visibility:
        crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    /// `first_row_id` / `data_sequence_number` index keyed by data-file
    /// path, built from the **previous** MV-refresh snapshot (i.e. the
    /// `from_snapshot_id` of the delta range). Used as a fallback by the
    /// `IcebergDeltaScanOperator`'s `DeletedDataFile` scanner when an
    /// OVERWRITE manifest's deleted entry does not carry an explicit
    /// per-file `first_row_id` (the iceberg writer may have only stamped
    /// the manifest-level `first_row_id` on the original APPEND, leaving
    /// the per-DataFile field `None`).
    pub previous_data_file_lineage: std::collections::HashMap<String, BaseDataFileLineage>,
    /// Data files removed by overwrite snapshots inside this delta range.
    /// Position deletes that target one of these files are subsumed by the
    /// deleted-data-file role and must not be emitted a second time.
    pub(crate) deleted_data_file_paths: std::collections::HashSet<String>,
}
