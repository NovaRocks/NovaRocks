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
        // Reject metadata-table flavors that the native-Rust path cannot
        // produce. Snapshots / History / Refs read directly off
        // `TableMetadata`; Files / Manifests / Partitions / Logical require
        // walking the manifest list (data-file scan). The earlier embedded
        // JVM bridge handled all of these by delegating to the Iceberg Java
        // SDK, but that path has been removed in favor of iceberg-rust.
        match cfg.metadata_table_type {
            IcebergMetadataTableType::Snapshots
            | IcebergMetadataTableType::History
            | IcebergMetadataTableType::Refs
            | IcebergMetadataTableType::Partitions => {}
            IcebergMetadataTableType::Files
            | IcebergMetadataTableType::Manifests
            | IcebergMetadataTableType::LogicalIcebergMetadata => {
                return Err(format!(
                    "Iceberg metadata table `{}` is not yet implemented in the \
                     native-Rust scan path; only Snapshots / History / Refs / Partitions are \
                     currently supported",
                    cfg.metadata_table_type.as_uppercase_str()
                ));
            }
        }
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
            IcebergMetadataTableType::Files
            | IcebergMetadataTableType::Manifests
            | IcebergMetadataTableType::LogicalIcebergMetadata => {
                // Constructor (`IcebergMetadataScanOp::new`) already rejects
                // these flavors; reaching here means construction was bypassed.
                return Err(format!(
                    "iceberg metadata scan reached execution for unsupported \
                     flavor `{}`",
                    self.cfg.metadata_table_type.as_uppercase_str()
                ));
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
    fn test_metadata_scan_rejects_unimplemented_flavors() {
        // Files / Manifests / LogicalIcebergMetadata require a
        // manifest-walk that the native-Rust path does not yet implement.
        // The constructor must fail-fast with a clear error so callers
        // surface a usable message instead of hanging in the pipeline.
        for ty in [
            IcebergMetadataTableType::Files,
            IcebergMetadataTableType::Manifests,
            IcebergMetadataTableType::LogicalIcebergMetadata,
        ] {
            let err = IcebergMetadataScanOp::new(IcebergMetadataScanConfig {
                metadata_table_type: ty.clone(),
                serialized_table: String::new(),
                serialized_predicate: String::new(),
                load_column_stats: false,
                ranges: Vec::new(),
                batch_size: 1,
                output_columns: vec![super::IcebergMetadataOutputColumn {
                    name: "x".to_string(),
                    slot_id: SlotId::new(1),
                    data_type: DataType::Int32,
                    nullable: false,
                }],
                profile_label: None,
            })
            .expect_err("native-Rust path should reject unimplemented metadata flavor");
            assert!(
                err.contains("not yet implemented in the native-Rust scan path"),
                "{ty:?}: unexpected error: {err}"
            );
        }
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
    fn test_build_snapshot_summary_map_uses_iceberg_field_names() {
        use super::SnapshotMetadataRow;
        let rows = vec![SnapshotMetadataRow {
            committed_at_micros: 0,
            snapshot_id: 1,
            parent_id: None,
            operation: None,
            manifest_list: "x".into(),
            summary: Some(vec![("added-records".into(), "10".into())]),
        }];
        // The Map type passed in matches what FE will declare for the summary column.
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Utf8, false)),
                        Arc::new(Field::new("value", DataType::Utf8, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let col = super::IcebergMetadataOutputColumn {
            name: "summary".into(),
            slot_id: SlotId::new(1),
            data_type: map_type,
            nullable: true,
        };
        let arr = super::build_snapshot_array(&col, &rows).unwrap();
        let map = arr.as_any().downcast_ref::<MapArray>().expect("MapArray");
        assert_eq!(map.len(), 1);
        let (key_field, value_field) = map.entries_fields();
        assert_eq!(key_field.name(), "key");
        assert_eq!(value_field.name(), "value");
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
}
