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

use std::collections::HashMap;

/// Provider-owned static predicate carried only inside an opaque Iceberg split.
/// `field_id` is authoritative for physical reads; `column` is retained for
/// manifest statistics and identity partition metadata, which are keyed by
/// their current logical names.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IcebergPhysicalPredicate {
    pub field_id: i32,
    pub column: String,
    pub domain: IcebergPhysicalPredicateDomain,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum IcebergPhysicalPredicateDomain {
    Range {
        op: IcebergPhysicalPredicateOp,
        value: IcebergPhysicalPredicateValue,
    },
    DiscreteSet {
        values: Vec<IcebergPhysicalPredicateValue>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum IcebergPhysicalPredicateOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum IcebergPhysicalPredicateValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Date32(i32),
}

/// Raw per-column statistics from Iceberg manifest DataFile entries.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct IcebergColumnStats {
    /// Iceberg field id this entry was published under.
    ///
    /// Manifest statistics are keyed by field id at the source. The surrounding
    /// map is keyed by the column's current name for lookup convenience, which
    /// alone cannot survive a rename -- so consumers bind through this id first
    /// and fall back to the map key, matching how the Parquet reader binds
    /// row-group statistics. `None` only for synthetic entries that never came
    /// from a manifest.
    ///
    /// `#[serde(default)]` rather than a split version bump: the encoding and
    /// decoding ends share this very struct, so the shape moves in lockstep,
    /// and a payload produced before this field existed simply decodes to
    /// `None` and falls back to name binding -- which is the intended fallback
    /// anyway.
    #[serde(default)]
    pub field_id: Option<i32>,
    pub null_count: Option<i64>,
    /// Total value count (including nulls) from manifest `value_counts`. The
    /// optimizer treats this as an upper bound on NDV when no precise Puffin
    /// sketch is available.
    pub value_count: Option<i64>,
    pub column_size: Option<i64>,
    pub lower_bound: Option<Vec<u8>>,
    pub upper_bound: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum IcebergPartitionValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    String(String),
    Binary(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IcebergPartitionFieldValue {
    pub source_column: String,
    pub field_name: String,
    pub transform: String,
    pub value: Option<IcebergPartitionValue>,
}

impl IcebergPartitionFieldValue {
    #[doc(hidden)]
    pub fn identity_int64_for_test(source_column: &str, value: i64) -> Self {
        Self {
            source_column: source_column.to_string(),
            field_name: source_column.to_string(),
            transform: "identity".to_string(),
            value: Some(IcebergPartitionValue::Int64(value)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum IcebergDeleteFileFormat {
    Parquet,
    Puffin,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum IcebergDeleteFileContent {
    Position,
    Equality,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct IcebergDeleteFileInfo {
    pub path: String,
    pub file_format: IcebergDeleteFileFormat,
    pub file_content: IcebergDeleteFileContent,
    /// Exact manifest facts; synthetic descriptors may explicitly lack them.
    pub record_count: Option<i64>,
    pub partition_data_json: Option<String>,
    pub length: Option<i64>,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
    pub sequence_number: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub referenced_data_file: Option<String>,
    pub equality_column_names: Vec<String>,
    pub equality_field_ids: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IcebergSchemaFieldDef {
    pub field_id: i32,
    pub name: String,
    #[serde(skip)]
    pub initial_default: Option<crate::iceberg::spec::Literal>,
    #[serde(skip)]
    pub write_default: Option<crate::iceberg::spec::Literal>,
    /// Spec-compliant JSON encoding of `initial_default` precomputed at the
    /// point of construction where the iceberg `Type` is still available.
    /// Necessary because `novarocks_connector_iceberg::iceberg::spec::Literal::Int128` carries no scale,
    /// so decimal defaults cannot be serialized correctly from the literal
    /// alone after the logical Iceberg type is no longer available.
    /// `None` falls back to the type-blind serializer.
    pub initial_default_json: Option<String>,
    /// Spec-compliant JSON encoding of `write_default`, for the same reason as
    /// `initial_default_json`.
    pub write_default_json: Option<String>,
    pub children: Vec<IcebergSchemaFieldDef>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IcebergSchemaDef {
    pub fields: Vec<IcebergSchemaFieldDef>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IcebergTableInfo {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub table_uuid: Option<String>,
    pub current_snapshot_id: Option<i64>,
    pub schema_id: i32,
    pub location: String,
    pub schema: IcebergSchemaDef,
    /// JSON-serialized iceberg `TableMetadata`. Required when the table
    /// is referenced as an Iceberg metadata table (`t$snapshots`,
    /// `t$history`, `t$refs`, `t$partitions`) — the native-Rust
    /// The Iceberg metadata SPI reader parses this string back via
    /// `serde_json::from_str::<TableMetadata>` to materialise the
    /// metadata rows. The native scan plan carries this payload directly;
    /// there is no JNI bridge on the NovaRocks side. `None` for tables
    /// resolved via paths that do not have access to the Iceberg
    /// `TableMetadata` (for example, synthetic test fixtures).
    pub serialized_metadata: Option<String>,
    /// JSON-serialized per-row payload for the `$files` / `$manifests` /
    /// `$entries` metadata tables, produced by the resolution-time manifest
    /// walk. `None` for all other tables.
    pub serialized_metadata_rows: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct IcebergDataFileInfo {
    pub path: String,
    pub size: i64,
    /// Row count from Iceberg file metadata. None for non-Iceberg sources.
    pub row_count: Option<i64>,
    pub column_stats: Option<HashMap<String, IcebergColumnStats>>,
    /// Iceberg partition spec id for this data file. None for non-Iceberg
    /// sources or synthetic scans where partition metadata is unavailable.
    pub partition_spec_id: Option<i32>,
    /// Stable string form of the Iceberg partition struct. Used only as
    /// metadata for read-planning paths that need delete applicability.
    pub partition_key: Option<String>,
    /// Iceberg v3 row-lineage: first row id assigned to this data file.
    /// Used as the fallback base for `_row_id` reads. None for non-Iceberg
    /// sources and tables without row-lineage metadata.
    pub first_row_id: Option<i64>,
    /// Iceberg v3 row-lineage: data sequence number of the manifest entry this
    /// file belongs to.  Populated from the Iceberg manifest at catalog scan
    /// time.  None for non-Iceberg sources.
    pub data_sequence_number: Option<i64>,
    /// IVM delta source tag for this file/range. None for ordinary scans.
    pub ivm_change_op: Option<i8>,
    /// Optional absolute data-file row positions to include when scanning this
    /// file. None means scan the whole selected file range.
    pub included_positions: Option<Vec<i64>>,
    /// Iceberg position-delete / Puffin deletion-vector files that apply to
    /// this data file. Empty for append-only snapshots and non-Iceberg scans.
    pub delete_files: Vec<IcebergDeleteFileInfo>,
    /// Data manifest path that contributed this file. None for non-Iceberg
    /// sources and synthetic test files.
    pub manifest_path: Option<String>,
    /// Partition values decoded from the Iceberg DataFile partition struct.
    /// Currently used for conservative identity-partition pruning.
    pub partition_values: Vec<IcebergPartitionFieldValue>,
}

impl IcebergDataFileInfo {
    #[doc(hidden)]
    pub fn for_test(path: &str, size: i64, row_count: i64) -> Self {
        Self {
            path: path.to_string(),
            size,
            row_count: Some(row_count),
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            delete_files: Vec::new(),
            manifest_path: None,
            partition_values: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergDataFileBinding {
    /// Ordinary catalog table registration. The `files` vector may be empty
    /// for schema-only registration or populated for metadata-table planning,
    /// but execution must bind splits from the table's current snapshot.
    CurrentSnapshot,
    /// Snapshot, refresh, or synthetic delta input whose `files` vector is the
    /// complete execution input, including the empty-snapshot case.
    ExplicitFiles,
}
