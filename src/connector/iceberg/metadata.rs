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

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int32Array, Int64Array, MapBuilder, MapFieldNames, RecordBatch, RecordBatchOptions,
    StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use iceberg::spec::{SnapshotRetention, TableMetadata};

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanOp};
use crate::runtime::profile::RuntimeProfile;

/// Decode the JSON payload that the planner stamps onto
/// `IcebergMetadataScanConfig::serialized_table` back into an iceberg-rust
/// `TableMetadata`. Producer side is `serde_json::to_string` over the same
/// crate's `TableMetadata`, so this is a round-trip.
fn parse_table_metadata(serialized: &str) -> Result<TableMetadata, String> {
    serde_json::from_str::<TableMetadata>(serialized)
        .map_err(|e| format!("parse iceberg table metadata for metadata-scan failed: {e}"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergMetadataTableType {
    Files,
    Manifests,
    LogicalIcebergMetadata,
    Snapshots,
    History,
    Refs,
    Partitions,
}

impl IcebergMetadataTableType {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_uppercase().as_str() {
            "FILES" => Ok(Self::Files),
            "MANIFESTS" => Ok(Self::Manifests),
            "LOGICAL_ICEBERG_METADATA" => Ok(Self::LogicalIcebergMetadata),
            "SNAPSHOTS" => Ok(Self::Snapshots),
            "HISTORY" => Ok(Self::History),
            "REFS" => Ok(Self::Refs),
            "PARTITIONS" => Ok(Self::Partitions),
            "ENTRIES" => Ok(Self::LogicalIcebergMetadata),
            other => Err(format!("unsupported iceberg metadata table type: {other}")),
        }
    }

    // Retained for diagnostics and unit-test assertions; the production reject
    // path that previously consumed it was removed once all metadata flavors
    // gained native builders.
    #[allow(dead_code)]
    fn as_uppercase_str(&self) -> &'static str {
        match self {
            Self::Files => "FILES",
            Self::Manifests => "MANIFESTS",
            Self::LogicalIcebergMetadata => "LOGICAL_ICEBERG_METADATA",
            Self::Snapshots => "SNAPSHOTS",
            Self::History => "HISTORY",
            Self::Refs => "REFS",
            Self::Partitions => "PARTITIONS",
        }
    }
}

#[derive(Clone, Debug)]
pub struct IcebergMetadataOutputColumn {
    pub name: String,
    pub slot_id: SlotId,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug)]
pub struct IcebergMetadataScanRange {
    pub path: String,
    pub serialized_split: String,
}

#[derive(Clone, Debug)]
pub struct IcebergMetadataScanConfig {
    pub metadata_table_type: IcebergMetadataTableType,
    pub serialized_table: String,
    pub serialized_predicate: String,
    pub load_column_stats: bool,
    pub ranges: Vec<IcebergMetadataScanRange>,
    pub batch_size: usize,
    pub output_columns: Vec<IcebergMetadataOutputColumn>,
    pub profile_label: Option<String>,
}

#[derive(Clone, Debug)]
pub struct IcebergMetadataScanOp {
    cfg: IcebergMetadataScanConfig,
    output_schema: SchemaRef,
    output_chunk_schema: Arc<ChunkSchema>,
}

impl IcebergMetadataScanOp {
    pub fn new(cfg: IcebergMetadataScanConfig) -> Result<Self, String> {
        // All metadata-table flavors are now produced natively. Snapshots /
        // History / Refs read directly off `TableMetadata`; Partitions /
        // Files / Manifests / Entries (LogicalIcebergMetadata) are fed by the
        // resolution-time manifest walk (`metadata_read.rs`) via the
        // `{version,rows}` payload on `serialized_predicate`.
        let fields = cfg
            .output_columns
            .iter()
            .map(|col| {
                Arc::new(Field::new(
                    &col.name,
                    normalize_metadata_output_type(&col.data_type),
                    col.nullable,
                ))
            })
            .collect::<Vec<_>>();
        let chunk_schema = Arc::new(ChunkSchema::try_new(
            cfg.output_columns
                .iter()
                .zip(fields.iter())
                .map(|(col, field)| {
                    ChunkSlotSchema::new_with_field(col.slot_id, field.as_ref().clone(), None, None)
                })
                .collect(),
        )?);
        Ok(Self {
            output_schema: Arc::new(Schema::new(fields)),
            output_chunk_schema: chunk_schema,
            cfg,
        })
    }
}

fn normalize_metadata_output_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(item) => DataType::List(Arc::new(normalize_metadata_output_field(item))),
        DataType::LargeList(item) => {
            DataType::LargeList(Arc::new(normalize_metadata_output_field(item)))
        }
        DataType::FixedSizeList(item, len) => {
            DataType::FixedSizeList(Arc::new(normalize_metadata_output_field(item)), *len)
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| normalize_metadata_output_field(field.as_ref()))
                .collect(),
        ),
        DataType::Map(entries, ordered) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return data_type.clone();
            };
            if fields.len() != 2 {
                return data_type.clone();
            }
            let mut normalized_fields = fields.iter().cloned().collect::<Vec<_>>();
            normalized_fields[0] = Arc::new(
                normalized_fields[0]
                    .as_ref()
                    .clone()
                    .with_data_type(normalize_metadata_output_type(
                        normalized_fields[0].data_type(),
                    ))
                    .with_nullable(false),
            );
            normalized_fields[1] = Arc::new(normalized_fields[1].as_ref().clone().with_data_type(
                normalize_metadata_output_type(normalized_fields[1].data_type()),
            ));
            DataType::Map(
                Arc::new(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(normalized_fields.into()))
                        .with_nullable(false),
                ),
                *ordered,
            )
        }
        _ => data_type.clone(),
    }
}

fn normalize_metadata_output_field(field: &Field) -> Field {
    field
        .clone()
        .with_data_type(normalize_metadata_output_type(field.data_type()))
}

impl ScanOp for IcebergMetadataScanOp {
    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        profile: Option<RuntimeProfile>,
        _runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        let ScanMorsel::IcebergMetadata { index } = morsel else {
            return Err("iceberg metadata scan received unexpected morsel".to_string());
        };
        // Indices come from build_morsels (0..ranges.len()), so .get(index) is
        // always Some. Table-level scans (snapshots/history/refs/partitions)
        // borrow `range` only for the optional profile annotation below.
        let range = self
            .cfg
            .ranges
            .get(index)
            .ok_or_else(|| format!("iceberg metadata range index out of bounds: {index}"))?;
        let chunks = match self.cfg.metadata_table_type {
            IcebergMetadataTableType::Files => {
                let rows = load_files_rows(&self.cfg)?;
                build_files_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
            IcebergMetadataTableType::Manifests => {
                let rows = load_manifests_rows(&self.cfg)?;
                build_manifests_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
            IcebergMetadataTableType::LogicalIcebergMetadata => {
                let rows = load_entries_rows(&self.cfg)?;
                build_entries_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
            IcebergMetadataTableType::Snapshots => {
                let rows = load_snapshot_rows(&self.cfg)?;
                build_snapshot_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
            IcebergMetadataTableType::History => {
                let rows = load_history_rows(&self.cfg)?;
                build_history_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
            IcebergMetadataTableType::Refs => {
                let rows = load_ref_rows(&self.cfg)?;
                build_ref_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
            IcebergMetadataTableType::Partitions => {
                let rows = load_partition_rows(&self.cfg)?;
                build_partition_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    &self.output_chunk_schema,
                    self.cfg.batch_size,
                )?
            }
        };

        if let Some(profile) = profile.as_ref() {
            profile.add_info_string(
                "IcebergMetadataTableType",
                format!("{:?}", self.cfg.metadata_table_type),
            );
            profile.add_info_string("RangeIndex", index.to_string());
            if !range.path.is_empty() {
                profile.add_info_string("RangePath", range.path.clone());
            }
        }

        Ok(Box::new(chunks.into_iter().map(Ok)))
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        let morsels = (0..self.cfg.ranges.len())
            .map(|index| ScanMorsel::IcebergMetadata { index })
            .collect();
        Ok(ScanMorsels::new(morsels, false))
    }

    fn profile_name(&self) -> Option<String> {
        let prefix = "ICEBERG_METADATA_SCAN";
        if let Some(label) = self.cfg.profile_label.as_deref() {
            return Some(format!("{prefix} ({label})"));
        }
        Some(prefix.to_string())
    }
}

fn build_chunks(
    schema: &SchemaRef,
    chunk_schema: &Arc<ChunkSchema>,
    arrays: Vec<ArrayRef>,
    row_count: usize,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if row_count == 0 {
        return Ok(Vec::new());
    }

    let batch = if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        RecordBatch::try_new_with_options(Arc::clone(schema), vec![], &options)
            .map_err(|e| format!("failed to build iceberg metadata empty batch: {}", e))?
    } else {
        RecordBatch::try_new(Arc::clone(schema), arrays)
            .map_err(|e| format!("failed to build iceberg metadata batch: {}", e))?
    };

    let batch_size = batch_size.max(1);
    if row_count <= batch_size {
        return Ok(vec![Chunk::new_with_chunk_schema(
            batch,
            Arc::clone(chunk_schema),
        )]);
    }

    let mut chunks = Vec::new();
    let mut offset = 0usize;
    while offset < row_count {
        let len = (row_count - offset).min(batch_size);
        chunks.push(Chunk::new_with_chunk_schema(
            batch.slice(offset, len),
            Arc::clone(chunk_schema),
        ));
        offset += len;
    }
    Ok(chunks)
}

fn iceberg_map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    }
}

#[derive(Clone, Debug)]
struct SnapshotMetadataRow {
    committed_at_micros: i64,
    snapshot_id: i64,
    parent_id: Option<i64>,
    operation: Option<String>,
    manifest_list: String,
    summary: Option<Vec<(String, String)>>,
}

fn load_snapshot_rows(cfg: &IcebergMetadataScanConfig) -> Result<Vec<SnapshotMetadataRow>, String> {
    let metadata = parse_table_metadata(&cfg.serialized_table)?;
    let mut rows = Vec::with_capacity(metadata.snapshots().len());
    for snapshot in metadata.snapshots() {
        let summary = snapshot.summary();
        let summary_pairs = if summary.additional_properties.is_empty() {
            None
        } else {
            let mut pairs: Vec<(String, String)> = summary
                .additional_properties
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            // Stable key order so chunked output is deterministic across runs.
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            Some(pairs)
        };
        rows.push(SnapshotMetadataRow {
            // Iceberg snapshot timestamps are millisecond-resolution; the
            // analyzer surfaces this column as Int64 microseconds.
            committed_at_micros: snapshot.timestamp_ms().saturating_mul(1_000),
            snapshot_id: snapshot.snapshot_id(),
            parent_id: snapshot.parent_snapshot_id(),
            operation: Some(summary.operation.as_str().to_string()),
            manifest_list: snapshot.manifest_list().to_string(),
            summary: summary_pairs,
        });
    }
    Ok(rows)
}

fn build_snapshot_chunks(
    rows: &[SnapshotMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_snapshot_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(
        output_schema,
        output_chunk_schema,
        arrays,
        rows.len(),
        batch_size,
    )
}

fn build_snapshot_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[SnapshotMetadataRow],
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "committed_at" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| r.committed_at_micros)
                .collect::<Vec<_>>(),
        ))),
        "snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        ))),
        "parent_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.parent_id).collect::<Vec<_>>(),
        ))),
        "operation" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.operation.as_deref())
                .collect::<Vec<_>>(),
        ))),
        "manifest_list" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|r| Some(r.manifest_list.as_str()))
                .collect::<Vec<_>>(),
        ))),
        // Serialize as a JSON object string so that the column matches the
        // Utf8 type the analyzer advertises, enabling LIKE / string operations.
        // Example: {"added-data-files":"1","engine-name":"novarocks",...}
        "summary" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|r| {
                    r.summary.as_ref().map(|pairs| {
                        let mut s = String::from("{");
                        for (i, (k, v)) in pairs.iter().enumerate() {
                            if i > 0 {
                                s.push(',');
                            }
                            s.push('"');
                            s.push_str(k);
                            s.push_str("\":\"");
                            s.push_str(v);
                            s.push('"');
                        }
                        s.push('}');
                        s
                    })
                })
                .collect::<Vec<_>>(),
        ))),
        other => Err(format!(
            "unsupported iceberg snapshots metadata column: {}",
            other
        )),
    }
}

// Reference implementation for the `Map`-typed metadata columns; the
// `$files`/`$entries` int-keyed map builders mirror its `MapFieldNames` usage.
// Currently unused in production (the `summary` column is surfaced as Utf8),
// but kept as the canonical pattern.
#[allow(dead_code)]
fn build_string_string_map_array<'a, I>(rows: I) -> Result<ArrayRef, String>
where
    I: IntoIterator<Item = Option<&'a Vec<(String, String)>>>,
{
    let mut builder = MapBuilder::new(
        Some(iceberg_map_field_names()),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    for row in rows {
        match row {
            Some(entries) => {
                for (key, value) in entries {
                    builder.keys().append_value(key);
                    builder.values().append_value(value);
                }
                builder
                    .append(true)
                    .map_err(|e| format!("append map row failed: {}", e))?;
            }
            None => {
                builder
                    .append(false)
                    .map_err(|e| format!("append null map row failed: {}", e))?;
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[derive(Clone, Debug)]
struct HistoryMetadataRow {
    made_current_at_micros: i64,
    snapshot_id: i64,
    parent_id: Option<i64>,
    is_current_ancestor: bool,
}

fn load_history_rows(cfg: &IcebergMetadataScanConfig) -> Result<Vec<HistoryMetadataRow>, String> {
    let metadata = parse_table_metadata(&cfg.serialized_table)?;
    // `is_current_ancestor` is true for any snapshot reachable from the
    // current head by walking parent_snapshot_id pointers. Build the set
    // up front so each history row can be tagged in O(1).
    let mut current_ancestors: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut walker = metadata.current_snapshot_id();
    while let Some(id) = walker {
        if !current_ancestors.insert(id) {
            // Defensive: stop on any cycle in parent pointers.
            break;
        }
        walker = metadata
            .snapshot_by_id(id)
            .and_then(|snap| snap.parent_snapshot_id());
    }

    let history = metadata.history();
    let mut rows = Vec::with_capacity(history.len());
    for entry in history {
        // Resolve parent_snapshot_id by looking the snapshot up; the
        // history log itself only carries (snapshot_id, timestamp_ms).
        let parent_id = metadata
            .snapshot_by_id(entry.snapshot_id)
            .and_then(|snap| snap.parent_snapshot_id());
        rows.push(HistoryMetadataRow {
            made_current_at_micros: entry.timestamp_ms.saturating_mul(1_000),
            snapshot_id: entry.snapshot_id,
            parent_id,
            is_current_ancestor: current_ancestors.contains(&entry.snapshot_id),
        });
    }
    Ok(rows)
}

fn build_history_chunks(
    rows: &[HistoryMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_history_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(
        output_schema,
        output_chunk_schema,
        arrays,
        rows.len(),
        batch_size,
    )
}

fn build_history_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[HistoryMetadataRow],
) -> Result<ArrayRef, String> {
    use arrow::array::BooleanArray;
    match column.name.as_str() {
        "made_current_at" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| r.made_current_at_micros)
                .collect::<Vec<_>>(),
        ))),
        "snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        ))),
        "parent_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.parent_id).collect::<Vec<_>>(),
        ))),
        "is_current_ancestor" => Ok(Arc::new(BooleanArray::from(
            rows.iter()
                .map(|r| r.is_current_ancestor)
                .collect::<Vec<_>>(),
        ))),
        other => Err(format!(
            "unsupported iceberg history metadata column: {}",
            other
        )),
    }
}

#[derive(Clone, Debug)]
struct RefMetadataRow {
    name: String,
    type_: String,
    snapshot_id: i64,
    max_reference_age_in_ms: Option<i64>,
    min_snapshots_to_keep: Option<i32>,
    max_snapshot_age_in_ms: Option<i64>,
}

fn load_ref_rows(cfg: &IcebergMetadataScanConfig) -> Result<Vec<RefMetadataRow>, String> {
    let metadata = parse_table_metadata(&cfg.serialized_table)?;
    let refs = metadata.refs();
    let mut rows: Vec<RefMetadataRow> = refs
        .iter()
        .map(|(name, reference)| {
            let (type_, max_reference_age_in_ms, min_snapshots_to_keep, max_snapshot_age_in_ms) =
                match &reference.retention {
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep,
                        max_snapshot_age_ms,
                        max_ref_age_ms,
                    } => (
                        "BRANCH",
                        *max_ref_age_ms,
                        *min_snapshots_to_keep,
                        *max_snapshot_age_ms,
                    ),
                    SnapshotRetention::Tag { max_ref_age_ms } => {
                        ("TAG", *max_ref_age_ms, None, None)
                    }
                };
            RefMetadataRow {
                name: name.clone(),
                type_: type_.to_string(),
                snapshot_id: reference.snapshot_id,
                max_reference_age_in_ms,
                min_snapshots_to_keep,
                max_snapshot_age_in_ms,
            }
        })
        .collect();
    // Stable name order so output is deterministic across runs.
    rows.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(rows)
}

fn build_ref_chunks(
    rows: &[RefMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_ref_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(
        output_schema,
        output_chunk_schema,
        arrays,
        rows.len(),
        batch_size,
    )
}

fn build_ref_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[RefMetadataRow],
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "name" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|r| Some(r.name.as_str()))
                .collect::<Vec<_>>(),
        ))),
        "type" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|r| Some(r.type_.as_str()))
                .collect::<Vec<_>>(),
        ))),
        "snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
        ))),
        "max_reference_age_in_ms" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| r.max_reference_age_in_ms)
                .collect::<Vec<_>>(),
        ))),
        "min_snapshots_to_keep" => Ok(Arc::new(Int32Array::from(
            rows.iter()
                .map(|r| r.min_snapshots_to_keep)
                .collect::<Vec<_>>(),
        ))),
        "max_snapshot_age_in_ms" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| r.max_snapshot_age_in_ms)
                .collect::<Vec<_>>(),
        ))),
        other => Err(format!(
            "unsupported iceberg refs metadata column: {}",
            other
        )),
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
struct PartitionMetadataPayload {
    version: i32,
    rows: Vec<PartitionMetadataRow>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct PartitionMetadataRow {
    record_count: i64,
    file_count: i64,
    position_delete_file_count: Option<i64>,
    equality_delete_file_count: Option<i64>,
}

fn load_partition_rows(
    cfg: &IcebergMetadataScanConfig,
) -> Result<Vec<PartitionMetadataRow>, String> {
    if cfg.serialized_predicate.trim().is_empty() {
        return Err(
            "iceberg partitions metadata scan missing partition aggregate payload".to_string(),
        );
    }
    let payload: PartitionMetadataPayload = serde_json::from_str(&cfg.serialized_predicate)
        .map_err(|e| format!("parse iceberg partitions metadata payload failed: {e}"))?;
    if payload.version != 1 {
        return Err(format!(
            "unsupported iceberg partitions metadata payload version {}",
            payload.version
        ));
    }
    Ok(payload.rows)
}

fn build_partition_chunks(
    rows: &[PartitionMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_partition_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(
        output_schema,
        output_chunk_schema,
        arrays,
        rows.len(),
        batch_size,
    )
}

fn build_partition_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[PartitionMetadataRow],
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "record_count" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.record_count).collect::<Vec<_>>(),
        ))),
        "file_count" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.file_count).collect::<Vec<_>>(),
        ))),
        "position_delete_file_count" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| r.position_delete_file_count)
                .collect::<Vec<_>>(),
        ))),
        "equality_delete_file_count" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| r.equality_delete_file_count)
                .collect::<Vec<_>>(),
        ))),
        other => Err(format!(
            "unsupported iceberg partitions metadata column: {}",
            other
        )),
    }
}

/// `{version,rows}` envelope shared by the `$files` / `$manifests` /
/// `$entries` metadata tables. The resolution-time manifest walk
/// (`metadata_read.rs`) produces this exact shape; here we decode the row
/// objects back out so the per-table builders can materialise Arrow columns.
#[derive(Clone, Debug, serde::Deserialize)]
struct JsonRowsPayload {
    version: i32,
    rows: Vec<serde_json::Value>,
}

/// Decode the `{version,rows}` payload carried on
/// `IcebergMetadataScanConfig::serialized_predicate` into its row objects.
/// `label` names the metadata table for error messages.
fn load_json_rows(
    cfg: &IcebergMetadataScanConfig,
    label: &str,
) -> Result<Vec<serde_json::Value>, String> {
    if cfg.serialized_predicate.trim().is_empty() {
        return Err(format!("iceberg {label} metadata scan missing payload"));
    }
    let payload: JsonRowsPayload = serde_json::from_str(&cfg.serialized_predicate)
        .map_err(|e| format!("parse iceberg {label} metadata payload failed: {e}"))?;
    if payload.version != 1 {
        return Err(format!(
            "unsupported iceberg {label} metadata payload version {}",
            payload.version
        ));
    }
    Ok(payload.rows)
}

fn load_files_rows(cfg: &IcebergMetadataScanConfig) -> Result<Vec<serde_json::Value>, String> {
    load_json_rows(cfg, "files")
}

fn load_manifests_rows(cfg: &IcebergMetadataScanConfig) -> Result<Vec<serde_json::Value>, String> {
    load_json_rows(cfg, "manifests")
}

fn load_entries_rows(cfg: &IcebergMetadataScanConfig) -> Result<Vec<serde_json::Value>, String> {
    load_json_rows(cfg, "entries")
}

/// Convert a JSON array of small non-negative integers into a `Vec<u8>`,
/// rejecting any element that is not an in-range byte. Used for `key_metadata`
/// and the `lower_bounds`/`upper_bounds` map values (the walk serialises bytes
/// as a JSON array of `u8`).
fn json_u8_array(items: &[serde_json::Value]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(items.len());
    for it in items {
        let v = it
            .as_u64()
            .ok_or_else(|| "expected byte value in JSON array".to_string())?;
        if v > u8::MAX as u64 {
            return Err(format!("byte value out of range: {v}"));
        }
        out.push(v as u8);
    }
    Ok(out)
}

/// Build a `Map<Int32, Int64>` column from rows whose `name` field is a JSON
/// array of `[key, value]` pairs. Map field names mirror
/// `build_string_string_map_array` (via `iceberg_map_field_names`) so the
/// produced type matches the analyzer's `map_int_to(Int64)` declaration.
fn build_int_int_map_array(rows: &[serde_json::Value], name: &str) -> Result<ArrayRef, String> {
    use arrow::array::{Int32Builder, Int64Builder};
    let mut builder = MapBuilder::new(
        Some(iceberg_map_field_names()),
        Int32Builder::new(),
        Int64Builder::new(),
    );
    for row in rows {
        match row.get(name).and_then(|v| v.as_array()) {
            Some(pairs) => {
                for pair in pairs {
                    let entry = pair
                        .as_array()
                        .ok_or_else(|| format!("{name} entry must be a [key,value] array"))?;
                    let key = entry
                        .first()
                        .and_then(|v| v.as_i64())
                        .ok_or_else(|| format!("{name} key must be an integer"))?;
                    let value = entry
                        .get(1)
                        .and_then(|v| v.as_i64())
                        .ok_or_else(|| format!("{name} value must be an integer"))?;
                    builder.keys().append_value(key as i32);
                    builder.values().append_value(value);
                }
                builder
                    .append(true)
                    .map_err(|e| format!("append {name} map row failed: {e}"))?;
            }
            None => {
                builder
                    .append(false)
                    .map_err(|e| format!("append null {name} map row failed: {e}"))?;
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build a `Map<Int32, Binary>` column from rows whose `name` field is a JSON
/// array of `[key, [bytes...]]` pairs. Map field names mirror
/// `build_string_string_map_array` so the produced type matches the analyzer's
/// `map_int_to(Binary)` declaration.
fn build_int_binary_map_array(rows: &[serde_json::Value], name: &str) -> Result<ArrayRef, String> {
    use arrow::array::{BinaryBuilder, Int32Builder};
    let mut builder = MapBuilder::new(
        Some(iceberg_map_field_names()),
        Int32Builder::new(),
        BinaryBuilder::new(),
    );
    for row in rows {
        match row.get(name).and_then(|v| v.as_array()) {
            Some(pairs) => {
                for pair in pairs {
                    let entry = pair
                        .as_array()
                        .ok_or_else(|| format!("{name} entry must be a [key,value] array"))?;
                    let key = entry
                        .first()
                        .and_then(|v| v.as_i64())
                        .ok_or_else(|| format!("{name} key must be an integer"))?;
                    let bytes = entry
                        .get(1)
                        .and_then(|v| v.as_array())
                        .ok_or_else(|| format!("{name} value must be a byte array"))?;
                    builder.keys().append_value(key as i32);
                    builder.values().append_value(json_u8_array(bytes)?);
                }
                builder
                    .append(true)
                    .map_err(|e| format!("append {name} map row failed: {e}"))?;
            }
            None => {
                builder
                    .append(false)
                    .map_err(|e| format!("append null {name} map row failed: {e}"))?;
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build the Arrow array for a single `$files` column from the JSON rows. The
/// produced array type EXACTLY matches the analyzer's `files_columns()`
/// declaration for that column (scalars, field-id maps, lists). Non-nullable
/// columns (`content`, `file_path`, `file_format`, `spec_id`, `record_count`,
/// `file_size_in_bytes`) always receive a value; the rest use `append_option`.
fn build_files_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[serde_json::Value],
) -> Result<ArrayRef, String> {
    use arrow::array::{BinaryBuilder, Int32Builder, Int64Builder, ListBuilder, StringBuilder};
    match column.name.as_str() {
        // Non-nullable Int32 scalar.
        "content" | "spec_id" => {
            let mut b = Int32Builder::new();
            for r in rows {
                b.append_value(r.get(&column.name).and_then(|v| v.as_i64()).unwrap_or(0) as i32);
            }
            Ok(Arc::new(b.finish()))
        }
        // Nullable Int32 scalar.
        "sort_order_id" => {
            let mut b = Int32Builder::new();
            for r in rows {
                b.append_option(
                    r.get(&column.name)
                        .and_then(|v| v.as_i64())
                        .map(|v| v as i32),
                );
            }
            Ok(Arc::new(b.finish()))
        }
        // Non-nullable Int64 scalar.
        "record_count" | "file_size_in_bytes" => {
            let mut b = Int64Builder::new();
            for r in rows {
                b.append_value(r.get(&column.name).and_then(|v| v.as_i64()).unwrap_or(0));
            }
            Ok(Arc::new(b.finish()))
        }
        // Nullable Int64 scalar.
        "first_row_id" => {
            let mut b = Int64Builder::new();
            for r in rows {
                b.append_option(r.get(&column.name).and_then(|v| v.as_i64()));
            }
            Ok(Arc::new(b.finish()))
        }
        // Non-nullable Utf8 scalar.
        "file_path" | "file_format" => {
            let mut b = StringBuilder::new();
            for r in rows {
                b.append_value(r.get(&column.name).and_then(|v| v.as_str()).unwrap_or(""));
            }
            Ok(Arc::new(b.finish()))
        }
        // Nullable Utf8 scalar.
        "partition" => {
            let mut b = StringBuilder::new();
            for r in rows {
                b.append_option(r.get(&column.name).and_then(|v| v.as_str()));
            }
            Ok(Arc::new(b.finish()))
        }
        // Nullable Binary scalar.
        "key_metadata" => {
            let mut b = BinaryBuilder::new();
            for r in rows {
                match r.get("key_metadata").and_then(|v| v.as_array()) {
                    Some(bytes) => b.append_value(json_u8_array(bytes)?),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        "column_sizes" | "value_counts" | "null_value_counts" | "nan_value_counts" => {
            build_int_int_map_array(rows, &column.name)
        }
        "lower_bounds" | "upper_bounds" => build_int_binary_map_array(rows, &column.name),
        "split_offsets" => {
            let mut b = ListBuilder::new(Int64Builder::new());
            for r in rows {
                match r.get("split_offsets").and_then(|v| v.as_array()) {
                    Some(items) => {
                        for it in items {
                            b.values().append_option(it.as_i64());
                        }
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        "equality_ids" => {
            let mut b = ListBuilder::new(Int32Builder::new());
            for r in rows {
                match r.get("equality_ids").and_then(|v| v.as_array()) {
                    Some(items) => {
                        for it in items {
                            b.values().append_option(it.as_i64().map(|x| x as i32));
                        }
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(format!(
            "unsupported iceberg files metadata column: {other}"
        )),
    }
}

fn build_files_chunks(
    rows: &[serde_json::Value],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_files_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(
        output_schema,
        output_chunk_schema,
        arrays,
        rows.len(),
        batch_size,
    )
}

/// Build the Arrow array for a single `$manifests` column. Scalars follow the
/// `$files` pattern; the non-nullable count columns coerce a missing/null
/// source value to `0` (they are declared NON-nullable but the walk emits
/// `null` when the underlying `Option` is absent); `partition_summaries` is a
/// `List<Struct<...>>` whose struct field order/names are derived from the
/// analyzer-declared `column.data_type` so the produced type matches exactly.
fn build_manifests_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[serde_json::Value],
) -> Result<ArrayRef, String> {
    use arrow::array::{
        BooleanBuilder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
    };
    match column.name.as_str() {
        // Non-nullable Int32 scalar.
        "content" | "partition_spec_id" => {
            let mut b = Int32Builder::new();
            for r in rows {
                b.append_value(r.get(&column.name).and_then(|v| v.as_i64()).unwrap_or(0) as i32);
            }
            Ok(Arc::new(b.finish()))
        }
        // Non-nullable Int32 counts: coerce missing/null to 0.
        "added_data_files_count" | "existing_data_files_count" | "deleted_data_files_count" => {
            let mut b = Int32Builder::new();
            for r in rows {
                b.append_value(r.get(&column.name).and_then(|v| v.as_i64()).unwrap_or(0) as i32);
            }
            Ok(Arc::new(b.finish()))
        }
        // Non-nullable Int64 scalar.
        "length" => {
            let mut b = Int64Builder::new();
            for r in rows {
                b.append_value(r.get("length").and_then(|v| v.as_i64()).unwrap_or(0));
            }
            Ok(Arc::new(b.finish()))
        }
        // Non-nullable Int64 counts: coerce missing/null to 0.
        "added_rows_count" | "existing_rows_count" | "deleted_rows_count" => {
            let mut b = Int64Builder::new();
            for r in rows {
                b.append_value(r.get(&column.name).and_then(|v| v.as_i64()).unwrap_or(0));
            }
            Ok(Arc::new(b.finish()))
        }
        // Nullable Int64 scalar.
        "added_snapshot_id" => {
            let mut b = Int64Builder::new();
            for r in rows {
                b.append_option(r.get("added_snapshot_id").and_then(|v| v.as_i64()));
            }
            Ok(Arc::new(b.finish()))
        }
        // Non-nullable Utf8 scalar.
        "path" => {
            let mut b = StringBuilder::new();
            for r in rows {
                b.append_value(r.get("path").and_then(|v| v.as_str()).unwrap_or(""));
            }
            Ok(Arc::new(b.finish()))
        }
        "partition_summaries" => {
            // Derive the struct fields from the analyzer-declared List<Struct>
            // type so names/nullability match exactly at RecordBatch::try_new.
            let fields = match &column.data_type {
                DataType::List(f) => match f.data_type() {
                    DataType::Struct(fs) => fs.clone(),
                    _ => return Err("partition_summaries inner type is not a struct".into()),
                },
                _ => return Err("partition_summaries type is not a list".into()),
            };
            let mut b = ListBuilder::new(StructBuilder::from_fields(fields.clone(), 0));
            for r in rows {
                match r.get("partition_summaries").and_then(|v| v.as_array()) {
                    Some(items) => {
                        for it in items {
                            let sb = b.values();
                            sb.field_builder::<BooleanBuilder>(0)
                                .ok_or("partition_summaries field 0 builder")?
                                .append_option(it.get("contains_null").and_then(|v| v.as_bool()));
                            sb.field_builder::<BooleanBuilder>(1)
                                .ok_or("partition_summaries field 1 builder")?
                                .append_option(it.get("contains_nan").and_then(|v| v.as_bool()));
                            sb.field_builder::<StringBuilder>(2)
                                .ok_or("partition_summaries field 2 builder")?
                                .append_option(it.get("lower_bound").and_then(|v| v.as_str()));
                            sb.field_builder::<StringBuilder>(3)
                                .ok_or("partition_summaries field 3 builder")?
                                .append_option(it.get("upper_bound").and_then(|v| v.as_str()));
                            sb.append(true);
                        }
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(format!(
            "unsupported iceberg manifests metadata column: {other}"
        )),
    }
}

fn build_manifests_chunks(
    rows: &[serde_json::Value],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_manifests_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(
        output_schema,
        output_chunk_schema,
        arrays,
        rows.len(),
        batch_size,
    )
}

/// Build the Arrow array for a single `$entries` column. The entry-level
/// columns (`status` non-nullable Int32; `snapshot_id` / `sequence_number` /
/// `file_sequence_number` nullable Int64) are built here; every other column
/// (including `first_row_id`) is a file property and delegates to
/// `build_files_array`, since the JSON row carries them under identical names.
fn build_entries_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[serde_json::Value],
) -> Result<ArrayRef, String> {
    use arrow::array::{Int32Builder, Int64Builder};
    match column.name.as_str() {
        // Non-nullable Int32 scalar.
        "status" => {
            let mut b = Int32Builder::new();
            for r in rows {
                b.append_value(r.get("status").and_then(|v| v.as_i64()).unwrap_or(0) as i32);
            }
            Ok(Arc::new(b.finish()))
        }
        // Nullable Int64 entry scalars.
        "snapshot_id" | "sequence_number" | "file_sequence_number" => {
            let mut b = Int64Builder::new();
            for r in rows {
                b.append_option(r.get(&column.name).and_then(|v| v.as_i64()));
            }
            Ok(Arc::new(b.finish()))
        }
        // `first_row_id` + every $files column reuse the files builder.
        _ => build_files_array(column, rows),
    }
}

fn build_entries_chunks(
    rows: &[serde_json::Value],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    output_chunk_schema: &Arc<ChunkSchema>,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let arrays = output_columns
        .iter()
        .map(|column| build_entries_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    build_chunks(
        output_schema,
        output_chunk_schema,
        arrays,
        rows.len(),
        batch_size,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        IcebergMetadataScanConfig, IcebergMetadataScanOp, IcebergMetadataTableType,
        normalize_metadata_output_type,
    };
    use crate::common::ids::SlotId;
    use arrow::array::{Array, MapArray};
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn test_normalize_metadata_output_type_makes_map_keys_non_nullable() {
        let ty = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int32, true)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let normalized = normalize_metadata_output_type(&ty);
        let DataType::Map(entries, _) = normalized else {
            panic!("expected map type");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected map entries struct");
        };
        assert!(!fields[0].is_nullable());
        assert!(fields[1].is_nullable());
    }

    #[test]
    fn test_parse_snapshots_history_refs_partitions() {
        assert_eq!(
            IcebergMetadataTableType::parse("SNAPSHOTS").unwrap(),
            IcebergMetadataTableType::Snapshots
        );
        assert_eq!(
            IcebergMetadataTableType::parse("history").unwrap(),
            IcebergMetadataTableType::History
        );
        assert_eq!(
            IcebergMetadataTableType::parse("Refs").unwrap(),
            IcebergMetadataTableType::Refs
        );
        assert_eq!(
            IcebergMetadataTableType::parse("partitions").unwrap(),
            IcebergMetadataTableType::Partitions
        );
    }

    #[test]
    fn test_build_snapshot_arrays_basic_shapes() {
        use super::SnapshotMetadataRow;
        let rows = vec![SnapshotMetadataRow {
            committed_at_micros: 1_700_000_000_000_000,
            snapshot_id: 42,
            parent_id: Some(41),
            operation: Some("append".into()),
            manifest_list: "s3://bucket/manifest-list.avro".into(),
            summary: Some(vec![("added-records".into(), "10".into())]),
        }];
        let columns = [
            ("snapshot_id", DataType::Int64),
            ("operation", DataType::Utf8),
        ];
        for (name, ty) in &columns {
            let col = super::IcebergMetadataOutputColumn {
                name: (*name).into(),
                slot_id: SlotId::new(1),
                data_type: ty.clone(),
                nullable: true,
            };
            let arr = super::build_snapshot_array(&col, &rows).unwrap();
            assert_eq!(arr.len(), 1);
        }
    }

    #[test]
    fn test_build_history_arrays_basic_shapes() {
        use super::HistoryMetadataRow;
        use arrow::array::BooleanArray;
        let rows = vec![
            HistoryMetadataRow {
                made_current_at_micros: 1_700_000_000_000_000,
                snapshot_id: 1,
                parent_id: None,
                is_current_ancestor: true,
            },
            HistoryMetadataRow {
                made_current_at_micros: 1_700_000_000_000_001,
                snapshot_id: 2,
                parent_id: Some(1),
                is_current_ancestor: false,
            },
        ];
        let bool_col = super::IcebergMetadataOutputColumn {
            name: "is_current_ancestor".into(),
            slot_id: SlotId::new(1),
            data_type: DataType::Boolean,
            nullable: false,
        };
        let arr = super::build_history_array(&bool_col, &rows).unwrap();
        let bools = arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("BooleanArray");
        assert_eq!(bools.len(), 2);
        assert!(bools.value(0));
        assert!(!bools.value(1));

        let parent_col = super::IcebergMetadataOutputColumn {
            name: "parent_id".into(),
            slot_id: SlotId::new(2),
            data_type: DataType::Int64,
            nullable: true,
        };
        let arr = super::build_history_array(&parent_col, &rows).unwrap();
        assert_eq!(arr.len(), 2);
        assert!(arr.is_null(0));
        assert!(!arr.is_null(1));
    }

    #[test]
    fn test_build_partition_arrays_basic_shapes() {
        use super::PartitionMetadataRow;
        let rows = vec![
            PartitionMetadataRow {
                record_count: 2,
                file_count: 1,
                position_delete_file_count: Some(0),
                equality_delete_file_count: Some(0),
            },
            PartitionMetadataRow {
                record_count: 1,
                file_count: 1,
                position_delete_file_count: Some(1),
                equality_delete_file_count: Some(0),
            },
        ];
        let count_col = super::IcebergMetadataOutputColumn {
            name: "record_count".into(),
            slot_id: SlotId::new(1),
            data_type: DataType::Int64,
            nullable: false,
        };
        let arr = super::build_partition_array(&count_col, &rows).unwrap();
        assert_eq!(arr.len(), 2);

        let position_delete_col = super::IcebergMetadataOutputColumn {
            name: "position_delete_file_count".into(),
            slot_id: SlotId::new(2),
            data_type: DataType::Int64,
            nullable: true,
        };
        let arr = super::build_partition_array(&position_delete_col, &rows).unwrap();
        assert_eq!(arr.len(), 2);
        assert!(!arr.is_null(1));
    }

    #[test]
    fn test_build_ref_arrays_basic_shapes() {
        use super::RefMetadataRow;
        let rows = vec![
            RefMetadataRow {
                name: "main".into(),
                type_: "BRANCH".into(),
                snapshot_id: 1,
                max_reference_age_in_ms: None,
                min_snapshots_to_keep: None,
                max_snapshot_age_in_ms: None,
            },
            RefMetadataRow {
                name: "release-2026-q1".into(),
                type_: "TAG".into(),
                snapshot_id: 2,
                max_reference_age_in_ms: Some(86_400_000),
                min_snapshots_to_keep: Some(3),
                max_snapshot_age_in_ms: Some(31_536_000_000),
            },
        ];

        let type_col = super::IcebergMetadataOutputColumn {
            name: "type".into(),
            slot_id: SlotId::new(1),
            data_type: DataType::Utf8,
            nullable: false,
        };
        let arr = super::build_ref_array(&type_col, &rows).unwrap();
        assert_eq!(arr.len(), 2);
        let strs = arr
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("StringArray");
        assert_eq!(strs.value(0), "BRANCH");
        assert_eq!(strs.value(1), "TAG");

        let min_col = super::IcebergMetadataOutputColumn {
            name: "min_snapshots_to_keep".into(),
            slot_id: SlotId::new(2),
            data_type: DataType::Int32,
            nullable: true,
        };
        let arr = super::build_ref_array(&min_col, &rows).unwrap();
        assert_eq!(arr.len(), 2);
        assert!(arr.is_null(0));
        assert!(!arr.is_null(1));
    }

    #[test]
    fn test_metadata_table_type_uppercase_strings() {
        assert_eq!(
            IcebergMetadataTableType::Snapshots.as_uppercase_str(),
            "SNAPSHOTS"
        );
        assert_eq!(
            IcebergMetadataTableType::History.as_uppercase_str(),
            "HISTORY"
        );
        assert_eq!(IcebergMetadataTableType::Refs.as_uppercase_str(), "REFS");
        assert_eq!(
            IcebergMetadataTableType::Partitions.as_uppercase_str(),
            "PARTITIONS"
        );
    }

    #[test]
    fn parse_accepts_entries_files_manifests() {
        assert_eq!(
            IcebergMetadataTableType::parse("entries").unwrap(),
            IcebergMetadataTableType::LogicalIcebergMetadata
        );
        assert_eq!(
            IcebergMetadataTableType::parse("files").unwrap(),
            IcebergMetadataTableType::Files
        );
        assert_eq!(
            IcebergMetadataTableType::parse("manifests").unwrap(),
            IcebergMetadataTableType::Manifests
        );
    }

    // Partition metadata payload parsing is exercised by the SQL suite; unit
    // tests above keep the Arrow array contract pinned.

    // ---------------------------------------------------------------------
    // $files / $manifests / $entries builders (IV3-8).
    // ---------------------------------------------------------------------

    use super::{
        IcebergMetadataOutputColumn, build_entries_array, build_entries_chunks, build_files_array,
        build_files_chunks, build_manifests_array, build_manifests_chunks,
    };
    use crate::sql::analyzer::iceberg_metadata::metadata_table_schema;
    use arrow::array::{BinaryArray, BooleanArray, Int32Array, Int64Array, ListArray, StructArray};

    /// Build the real analyzer-declared output columns for a metadata table,
    /// assigning sequential SlotIds. This exercises the REAL declared Arrow
    /// types so the builders' output must match exactly.
    fn output_columns_for(ty: IcebergMetadataTableType) -> Vec<IcebergMetadataOutputColumn> {
        metadata_table_schema(ty)
            .into_iter()
            .enumerate()
            .map(|(i, c)| IcebergMetadataOutputColumn {
                name: c.name,
                slot_id: SlotId::new((i + 1) as u32),
                data_type: c.data_type,
                nullable: c.nullable,
            })
            .collect()
    }

    fn column_named<'a>(
        cols: &'a [IcebergMetadataOutputColumn],
        name: &str,
    ) -> &'a IcebergMetadataOutputColumn {
        cols.iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("missing column {name}"))
    }

    /// Build the normalized output schema + chunk schema exactly the way
    /// `IcebergMetadataScanOp::new` does, so `build_*_chunks` validates the
    /// produced arrays against the real declared (normalized) types.
    fn schemas_for(
        cols: &[IcebergMetadataOutputColumn],
    ) -> (super::SchemaRef, Arc<super::ChunkSchema>) {
        use super::{ChunkSchema, ChunkSlotSchema};
        let fields = cols
            .iter()
            .map(|col| {
                Arc::new(Field::new(
                    &col.name,
                    normalize_metadata_output_type(&col.data_type),
                    col.nullable,
                ))
            })
            .collect::<Vec<_>>();
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(
                cols.iter()
                    .zip(fields.iter())
                    .map(|(col, field)| {
                        ChunkSlotSchema::new_with_field(
                            col.slot_id,
                            field.as_ref().clone(),
                            None,
                            None,
                        )
                    })
                    .collect(),
            )
            .expect("chunk schema"),
        );
        (Arc::new(super::Schema::new(fields)), chunk_schema)
    }

    /// One representative data-file row (content=0) carrying every column the
    /// `$files` table exposes, using the same JSON shapes the resolution walk
    /// emits (`[[k,v],...]` for maps, `[k,[bytes...]]` for binary maps).
    fn sample_data_file_row() -> serde_json::Value {
        serde_json::json!({
            "content": 0,
            "file_path": "s3://bucket/data/f0.parquet",
            "file_format": "PARQUET",
            "spec_id": 0,
            "record_count": 3,
            "file_size_in_bytes": 1024,
            "column_sizes": [[1, 100]],
            "value_counts": [[1, 3]],
            "null_value_counts": [[1, 0]],
            "nan_value_counts": [[1, 0]],
            "lower_bounds": [[1, [1, 2, 3]]],
            "upper_bounds": [[1, [4, 5, 6]]],
            "split_offsets": [0, 128],
            "equality_ids": [],
            "sort_order_id": 0,
            "key_metadata": serde_json::Value::Null,
            "first_row_id": serde_json::Value::Null,
            "partition": "Struct([])"
        })
    }

    #[test]
    fn build_files_array_scalar_columns() {
        let rows = vec![sample_data_file_row()];
        let cols = output_columns_for(IcebergMetadataTableType::Files);

        let content = build_files_array(column_named(&cols, "content"), &rows).unwrap();
        let content = content.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(content.value(0), 0);

        let rec = build_files_array(column_named(&cols, "record_count"), &rows).unwrap();
        let rec = rec.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(rec.value(0), 3);

        let path = build_files_array(column_named(&cols, "file_path"), &rows).unwrap();
        let path = path
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(path.value(0), "s3://bucket/data/f0.parquet");

        let part = build_files_array(column_named(&cols, "partition"), &rows).unwrap();
        let part = part
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(part.value(0), "Struct([])");

        // Nullable scalars surfaced as null.
        let frid = build_files_array(column_named(&cols, "first_row_id"), &rows).unwrap();
        assert!(frid.is_null(0));
        let km = build_files_array(column_named(&cols, "key_metadata"), &rows).unwrap();
        assert!(
            km.as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .is_null(0)
        );
    }

    #[test]
    fn build_files_array_int_int_map_column() {
        let rows = vec![sample_data_file_row()];
        let cols = output_columns_for(IcebergMetadataTableType::Files);
        let arr = build_files_array(column_named(&cols, "column_sizes"), &rows).unwrap();
        let map = arr.as_any().downcast_ref::<MapArray>().expect("MapArray");
        assert_eq!(map.len(), 1);
        let entries = map.value(0);
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let vals = entries
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(keys.value(0), 1);
        assert_eq!(vals.value(0), 100);
    }

    #[test]
    fn build_files_array_int_binary_map_column() {
        let rows = vec![sample_data_file_row()];
        let cols = output_columns_for(IcebergMetadataTableType::Files);
        let arr = build_files_array(column_named(&cols, "lower_bounds"), &rows).unwrap();
        let map = arr.as_any().downcast_ref::<MapArray>().expect("MapArray");
        let entries = map.value(0);
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let vals = entries
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(keys.value(0), 1);
        assert_eq!(vals.value(0), &[1u8, 2, 3]);
    }

    #[test]
    fn build_files_array_list_columns() {
        let rows = vec![sample_data_file_row()];
        let cols = output_columns_for(IcebergMetadataTableType::Files);

        let so = build_files_array(column_named(&cols, "split_offsets"), &rows).unwrap();
        let so = so.as_any().downcast_ref::<ListArray>().expect("ListArray");
        let inner = so.value(0);
        let inner = inner.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(inner.values(), &[0i64, 128]);

        // equality_ids: present-but-empty array -> non-null empty list.
        let eq = build_files_array(column_named(&cols, "equality_ids"), &rows).unwrap();
        let eq = eq.as_any().downcast_ref::<ListArray>().expect("ListArray");
        assert!(!eq.is_null(0));
        assert_eq!(eq.value(0).len(), 0);
    }

    #[test]
    fn build_files_chunks_matches_declared_schema() {
        let rows = vec![sample_data_file_row()];
        let cols = output_columns_for(IcebergMetadataTableType::Files);
        let (schema, chunk_schema) = schemas_for(&cols);
        // A successful build proves every produced array's type equals the
        // analyzer-declared (normalized) type — RecordBatch::try_new errors
        // otherwise.
        let chunks = build_files_chunks(&rows, &cols, &schema, &chunk_schema, 1024)
            .expect("build_files_chunks must match declared schema");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 1);
    }

    #[test]
    fn build_manifests_array_counts_and_summaries() {
        // A manifest row with one partition summary and a NULL count for
        // existing_data_files_count: the non-nullable count must come out 0.
        let rows = vec![serde_json::json!({
            "content": 0,
            "path": "s3://bucket/m0.avro",
            "length": 4096,
            "partition_spec_id": 0,
            "added_snapshot_id": serde_json::Value::Null,
            "added_data_files_count": 1,
            "existing_data_files_count": serde_json::Value::Null,
            "deleted_data_files_count": 0,
            "added_rows_count": 3,
            "existing_rows_count": serde_json::Value::Null,
            "deleted_rows_count": 0,
            "partition_summaries": [{
                "contains_null": false,
                "contains_nan": false,
                "lower_bound": "a",
                "upper_bound": "z"
            }]
        })];
        let cols = output_columns_for(IcebergMetadataTableType::Manifests);

        // Non-nullable count with a null source coerces to 0 (not null).
        let existing =
            build_manifests_array(column_named(&cols, "existing_data_files_count"), &rows).unwrap();
        let existing = existing.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(!existing.is_null(0));
        assert_eq!(existing.value(0), 0);

        let existing_rows =
            build_manifests_array(column_named(&cols, "existing_rows_count"), &rows).unwrap();
        let existing_rows = existing_rows.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(!existing_rows.is_null(0));
        assert_eq!(existing_rows.value(0), 0);

        // added_snapshot_id IS nullable -> stays null.
        let added_snap =
            build_manifests_array(column_named(&cols, "added_snapshot_id"), &rows).unwrap();
        assert!(added_snap.is_null(0));

        // partition_summaries: List<Struct> with exactly one element.
        let ps = build_manifests_array(column_named(&cols, "partition_summaries"), &rows).unwrap();
        let ps = ps.as_any().downcast_ref::<ListArray>().expect("ListArray");
        assert_eq!(ps.len(), 1);
        let elem = ps.value(0);
        assert_eq!(elem.len(), 1);
        let st = elem.as_any().downcast_ref::<StructArray>().expect("Struct");
        let cn = st
            .column_by_name("contains_null")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!cn.value(0));
        let lb = st
            .column_by_name("lower_bound")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(lb.value(0), "a");
    }

    #[test]
    fn build_manifests_chunks_matches_declared_schema() {
        let rows = vec![serde_json::json!({
            "content": 0,
            "path": "s3://bucket/m0.avro",
            "length": 4096,
            "partition_spec_id": 0,
            "added_snapshot_id": 123,
            "added_data_files_count": 1,
            "existing_data_files_count": 0,
            "deleted_data_files_count": 0,
            "added_rows_count": 3,
            "existing_rows_count": 0,
            "deleted_rows_count": 0,
            "partition_summaries": [{
                "contains_null": false,
                "contains_nan": false,
                "lower_bound": "a",
                "upper_bound": "z"
            }]
        })];
        let cols = output_columns_for(IcebergMetadataTableType::Manifests);
        let (schema, chunk_schema) = schemas_for(&cols);
        let chunks = build_manifests_chunks(&rows, &cols, &schema, &chunk_schema, 1024)
            .expect("build_manifests_chunks must match declared schema");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 1);
    }

    #[test]
    fn build_entries_array_status_and_file_columns() {
        // One added (status=1) data-file entry + one deleted (status=2)
        // position-delete entry. File columns ride alongside the entry columns.
        let mut added = sample_data_file_row();
        added.as_object_mut().unwrap().extend(
            serde_json::json!({
                "status": 1,
                "snapshot_id": 100,
                "sequence_number": 5,
                "file_sequence_number": 5
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        let deleted = serde_json::json!({
            "status": 2,
            "snapshot_id": 100,
            "sequence_number": 6,
            "file_sequence_number": serde_json::Value::Null,
            "content": 1,
            "file_path": "s3://bucket/delete/d0.parquet",
            "file_format": "PARQUET",
            "spec_id": 0,
            "record_count": 1,
            "file_size_in_bytes": 64,
            "column_sizes": [],
            "value_counts": [],
            "null_value_counts": [],
            "nan_value_counts": [],
            "lower_bounds": [],
            "upper_bounds": [],
            "split_offsets": [],
            "equality_ids": [],
            "sort_order_id": serde_json::Value::Null,
            "key_metadata": serde_json::Value::Null,
            "first_row_id": serde_json::Value::Null,
            "partition": "Struct([])"
        });
        let rows = vec![added, deleted];
        let cols = output_columns_for(IcebergMetadataTableType::LogicalIcebergMetadata);

        let status = build_entries_array(column_named(&cols, "status"), &rows).unwrap();
        let status = status.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(status.value(0), 1);
        assert_eq!(status.value(1), 2);

        // file_sequence_number nullable: present on row 0, null on row 1.
        let fsn = build_entries_array(column_named(&cols, "file_sequence_number"), &rows).unwrap();
        let fsn = fsn.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(fsn.value(0), 5);
        assert!(fsn.is_null(1));

        // File columns are populated (delegated to build_files_array).
        let fp = build_entries_array(column_named(&cols, "file_path"), &rows).unwrap();
        let fp = fp
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(fp.value(0), "s3://bucket/data/f0.parquet");
        assert_eq!(fp.value(1), "s3://bucket/delete/d0.parquet");

        let rec = build_entries_array(column_named(&cols, "record_count"), &rows).unwrap();
        let rec = rec.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(rec.value(0), 3);
        assert_eq!(rec.value(1), 1);
    }

    #[test]
    fn build_entries_chunks_matches_declared_schema() {
        let mut added = sample_data_file_row();
        added.as_object_mut().unwrap().extend(
            serde_json::json!({
                "status": 1,
                "snapshot_id": 100,
                "sequence_number": 5,
                "file_sequence_number": 5
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        let rows = vec![added];
        let cols = output_columns_for(IcebergMetadataTableType::LogicalIcebergMetadata);
        let (schema, chunk_schema) = schemas_for(&cols);
        let chunks = build_entries_chunks(&rows, &cols, &schema, &chunk_schema, 1024)
            .expect("build_entries_chunks must match declared schema");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 1);
    }
}
