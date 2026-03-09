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
use std::sync::Arc;

use apache_avro::Reader;
use apache_avro::types::Value as AvroValue;
use arrow::array::{
    ArrayRef, BinaryBuilder, Int32Array, Int32Builder, Int64Array, Int64Builder, ListArray,
    ListBuilder, MapBuilder, RecordBatch, RecordBatchOptions, StringArray, StringBuilder,
    StructArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use base64::Engine;
use regex::Regex;
use serde_json::{Map as JsonMap, Value as JsonValue, json};

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_with_slot_id};
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanOp};
use crate::fs::path::resolve_opendal_paths;
use crate::runtime::profile::RuntimeProfile;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergMetadataTableType {
    Files,
    Manifests,
    LogicalIcebergMetadata,
}

impl IcebergMetadataTableType {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_uppercase().as_str() {
            "FILES" => Ok(Self::Files),
            "MANIFESTS" => Ok(Self::Manifests),
            "LOGICAL_ICEBERG_METADATA" => Ok(Self::LogicalIcebergMetadata),
            other => Err(format!("unsupported iceberg metadata table type: {other}")),
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
}

#[derive(Clone, Debug)]
pub struct IcebergMetadataScanConfig {
    pub metadata_table_type: IcebergMetadataTableType,
    pub serialized_table: String,
    pub load_column_stats: bool,
    pub ranges: Vec<IcebergMetadataScanRange>,
    pub batch_size: usize,
    pub output_columns: Vec<IcebergMetadataOutputColumn>,
    pub object_store_config: Option<crate::fs::object_store::ObjectStoreConfig>,
    pub profile_label: Option<String>,
}

#[derive(Clone, Debug)]
pub struct IcebergMetadataScanOp {
    cfg: IcebergMetadataScanConfig,
    output_schema: SchemaRef,
}

impl IcebergMetadataScanOp {
    pub fn new(cfg: IcebergMetadataScanConfig) -> Self {
        let fields: Vec<_> = cfg
            .output_columns
            .iter()
            .map(|col| {
                Arc::new(field_with_slot_id(
                    Field::new(&col.name, col.data_type.clone(), col.nullable),
                    col.slot_id,
                ))
            })
            .collect();
        Self {
            output_schema: Arc::new(Schema::new(fields)),
            cfg,
        }
    }
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
        let chunks = match self.cfg.metadata_table_type {
            IcebergMetadataTableType::Files => {
                let range = self.cfg.ranges.get(index).ok_or_else(|| {
                    format!("iceberg metadata range index out of bounds: {index}")
                })?;
                let rows = load_file_rows(range, self.cfg.object_store_config.as_ref())?;
                build_file_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    self.cfg.batch_size,
                    false,
                    self.cfg.load_column_stats,
                )?
            }
            IcebergMetadataTableType::LogicalIcebergMetadata => {
                let range = self.cfg.ranges.get(index).ok_or_else(|| {
                    format!("iceberg metadata range index out of bounds: {index}")
                })?;
                let rows = load_file_rows(range, self.cfg.object_store_config.as_ref())?;
                build_file_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
                    self.cfg.batch_size,
                    true,
                    self.cfg.load_column_stats,
                )?
            }
            IcebergMetadataTableType::Manifests => {
                let rows = load_manifest_rows(
                    &self.cfg.serialized_table,
                    self.cfg.object_store_config.as_ref(),
                )?;
                build_manifest_chunks(
                    &rows,
                    &self.cfg.output_columns,
                    &self.output_schema,
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

#[derive(Clone, Debug)]
struct FileMetadataRow {
    content: i32,
    file_path: String,
    file_format: String,
    spec_id: i32,
    partition_data: Option<Vec<u8>>,
    record_count: i64,
    file_size_in_bytes: i64,
    column_sizes: Option<Vec<(i32, i64)>>,
    value_counts: Option<Vec<(i32, i64)>>,
    null_value_counts: Option<Vec<(i32, i64)>>,
    nan_value_counts: Option<Vec<(i32, i64)>>,
    lower_bounds: Option<Vec<(i32, String)>>,
    upper_bounds: Option<Vec<(i32, String)>>,
    split_offsets: Option<Vec<i64>>,
    sort_id: Option<i32>,
    equality_ids: Option<Vec<i32>>,
    file_sequence_number: Option<i64>,
    data_sequence_number: Option<i64>,
    column_stats: Option<Vec<u8>>,
    key_metadata: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct ManifestPartitionSummary {
    contains_null: Option<String>,
    contains_nan: Option<String>,
    lower_bound: Option<String>,
    upper_bound: Option<String>,
}

#[derive(Clone, Debug)]
struct ManifestMetadataRow {
    path: String,
    length: i64,
    partition_spec_id: i32,
    added_snapshot_id: i64,
    added_data_files_count: i32,
    added_rows_count: i64,
    existing_data_files_count: i32,
    existing_rows_count: i64,
    deleted_data_files_count: i32,
    deleted_rows_count: i64,
    partitions: Option<Vec<ManifestPartitionSummary>>,
}

fn load_file_rows(
    range: &IcebergMetadataScanRange,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Vec<FileMetadataRow>, String> {
    let bytes = read_path_bytes(&range.path, object_store_config)?;
    let reader = Reader::new(bytes.as_slice())
        .map_err(|e| format!("failed to open iceberg manifest avro {}: {}", range.path, e))?;
    let metadata = reader.user_metadata().clone();
    let spec_id = avro_metadata_i32(&metadata, "partition-spec-id")?;

    let mut rows = Vec::new();
    for value in reader {
        let value = value.map_err(|e| format!("failed to read avro record: {}", e))?;
        let entry = avro_record_fields(&value)
            .ok_or_else(|| "iceberg manifest entry is not an avro record".to_string())?;
        let data_file = avro_field(entry, "data_file")
            .and_then(avro_record_fields)
            .ok_or_else(|| "iceberg manifest entry missing data_file record".to_string())?;

        let column_sizes = avro_int_long_map(avro_field(data_file, "column_sizes"))?;
        let value_counts = avro_int_long_map(avro_field(data_file, "value_counts"))?;
        let null_value_counts = avro_int_long_map(avro_field(data_file, "null_value_counts"))?;
        let nan_value_counts = avro_int_long_map(avro_field(data_file, "nan_value_counts"))?;
        let lower_bounds = avro_int_string_map(avro_field(data_file, "lower_bounds"))?;
        let upper_bounds = avro_int_string_map(avro_field(data_file, "upper_bounds"))?;
        let split_offsets = avro_i64_list(avro_field(data_file, "split_offsets"))?;
        let equality_ids = avro_i32_list(avro_field(data_file, "equality_ids"))?;
        let partition_data = encode_partition_data(avro_field(data_file, "partition"))?;
        let key_metadata = avro_bytes(avro_field(data_file, "key_metadata"))?;
        let column_stats = encode_column_stats(
            column_sizes.as_ref(),
            value_counts.as_ref(),
            null_value_counts.as_ref(),
            nan_value_counts.as_ref(),
            lower_bounds.as_ref(),
            upper_bounds.as_ref(),
        )?;

        rows.push(FileMetadataRow {
            content: avro_i32_required(avro_field(data_file, "content"), "content")?,
            file_path: avro_string_required(avro_field(data_file, "file_path"), "file_path")?,
            file_format: avro_string_required(avro_field(data_file, "file_format"), "file_format")?,
            spec_id,
            partition_data,
            record_count: avro_i64_required(avro_field(data_file, "record_count"), "record_count")?,
            file_size_in_bytes: avro_i64_required(
                avro_field(data_file, "file_size_in_bytes"),
                "file_size_in_bytes",
            )?,
            column_sizes,
            value_counts,
            null_value_counts,
            nan_value_counts,
            lower_bounds,
            upper_bounds,
            split_offsets,
            sort_id: avro_i32(avro_field(data_file, "sort_order_id"))?,
            equality_ids,
            file_sequence_number: avro_i64(avro_field(entry, "file_sequence_number"))?,
            data_sequence_number: avro_i64(avro_field(entry, "sequence_number"))?,
            column_stats,
            key_metadata,
        });
    }
    Ok(rows)
}

fn load_manifest_rows(
    serialized_table: &str,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Vec<ManifestMetadataRow>, String> {
    let metadata_json_path = extract_metadata_json_path(serialized_table)?;
    let metadata_cfg = adjusted_object_store_config(object_store_config, &metadata_json_path);
    let metadata_json = read_path_bytes(&metadata_json_path, metadata_cfg.as_ref())?;
    let manifest_list_path = extract_manifest_list_path(&metadata_json)?;
    let manifest_cfg = adjusted_object_store_config(object_store_config, &manifest_list_path);
    let bytes = read_path_bytes(&manifest_list_path, manifest_cfg.as_ref())?;
    let reader = Reader::new(bytes.as_slice()).map_err(|e| {
        format!(
            "failed to open iceberg manifest list avro {}: {}",
            manifest_list_path, e
        )
    })?;

    let mut rows = Vec::new();
    for value in reader {
        let value = value.map_err(|e| format!("failed to read avro record: {}", e))?;
        let fields = avro_record_fields(&value)
            .ok_or_else(|| "iceberg manifest list entry is not an avro record".to_string())?;
        rows.push(ManifestMetadataRow {
            path: avro_string_required(avro_field(fields, "manifest_path"), "manifest_path")?,
            length: avro_i64_required(avro_field(fields, "manifest_length"), "manifest_length")?,
            partition_spec_id: avro_i32_required(
                avro_field(fields, "partition_spec_id"),
                "partition_spec_id",
            )?,
            added_snapshot_id: avro_i64_required(
                avro_field(fields, "added_snapshot_id"),
                "added_snapshot_id",
            )?,
            added_data_files_count: avro_i32_required(
                avro_field(fields, "added_files_count"),
                "added_files_count",
            )?,
            added_rows_count: avro_i64_required(
                avro_field(fields, "added_rows_count"),
                "added_rows_count",
            )?,
            existing_data_files_count: avro_i32_required(
                avro_field(fields, "existing_files_count"),
                "existing_files_count",
            )?,
            existing_rows_count: avro_i64_required(
                avro_field(fields, "existing_rows_count"),
                "existing_rows_count",
            )?,
            deleted_data_files_count: avro_i32_required(
                avro_field(fields, "deleted_files_count"),
                "deleted_files_count",
            )?,
            deleted_rows_count: avro_i64_required(
                avro_field(fields, "deleted_rows_count"),
                "deleted_rows_count",
            )?,
            partitions: avro_manifest_partitions(avro_field(fields, "partitions"))?,
        });
    }
    Ok(rows)
}

fn build_file_chunks(
    rows: &[FileMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    batch_size: usize,
    logical_metadata: bool,
    load_column_stats: bool,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let arrays = output_columns
        .iter()
        .map(|column| build_file_array(column, rows, logical_metadata, load_column_stats))
        .collect::<Result<Vec<_>, _>>()?;

    build_chunks(output_schema, arrays, rows.len(), batch_size)
}

fn build_manifest_chunks(
    rows: &[ManifestMetadataRow],
    output_columns: &[IcebergMetadataOutputColumn],
    output_schema: &SchemaRef,
    batch_size: usize,
) -> Result<Vec<Chunk>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let arrays = output_columns
        .iter()
        .map(|column| build_manifest_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;

    build_chunks(output_schema, arrays, rows.len(), batch_size)
}

fn build_chunks(
    schema: &SchemaRef,
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
        return Ok(vec![Chunk::new(batch)]);
    }

    let mut chunks = Vec::new();
    let mut offset = 0usize;
    while offset < row_count {
        let len = (row_count - offset).min(batch_size);
        chunks.push(Chunk::new(batch.slice(offset, len)));
        offset += len;
    }
    Ok(chunks)
}

fn build_file_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[FileMetadataRow],
    logical_metadata: bool,
    load_column_stats: bool,
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "content" => Ok(Arc::new(Int32Array::from(
            rows.iter().map(|row| row.content).collect::<Vec<_>>(),
        ))),
        "file_path" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.file_path.as_str()))
                .collect::<Vec<_>>(),
        ))),
        "file_format" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.file_format.as_str()))
                .collect::<Vec<_>>(),
        ))),
        "spec_id" => Ok(Arc::new(Int32Array::from(
            rows.iter().map(|row| row.spec_id).collect::<Vec<_>>(),
        ))),
        "partition_data" if logical_metadata => build_binary_array(
            rows.iter()
                .map(|row| row.partition_data.as_deref())
                .collect::<Vec<_>>(),
        ),
        "record_count" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|row| row.record_count).collect::<Vec<_>>(),
        ))),
        "file_size_in_bytes" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.file_size_in_bytes)
                .collect::<Vec<_>>(),
        ))),
        "column_sizes" if !logical_metadata => {
            build_i32_i64_map_array(rows.iter().map(|row| row.column_sizes.as_ref()))
        }
        "value_counts" if !logical_metadata => {
            build_i32_i64_map_array(rows.iter().map(|row| row.value_counts.as_ref()))
        }
        "null_value_counts" if !logical_metadata => {
            build_i32_i64_map_array(rows.iter().map(|row| row.null_value_counts.as_ref()))
        }
        "nan_value_counts" if !logical_metadata => {
            build_i32_i64_map_array(rows.iter().map(|row| row.nan_value_counts.as_ref()))
        }
        "lower_bounds" if !logical_metadata => {
            build_i32_utf8_map_array(rows.iter().map(|row| row.lower_bounds.as_ref()))
        }
        "upper_bounds" if !logical_metadata => {
            build_i32_utf8_map_array(rows.iter().map(|row| row.upper_bounds.as_ref()))
        }
        "split_offsets" => build_i64_list_array(rows.iter().map(|row| row.split_offsets.as_ref())),
        "sort_id" => Ok(Arc::new(Int32Array::from(
            rows.iter().map(|row| row.sort_id).collect::<Vec<_>>(),
        ))),
        "equality_ids" => build_i32_list_array(rows.iter().map(|row| row.equality_ids.as_ref())),
        "file_sequence_number" if logical_metadata => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.file_sequence_number)
                .collect::<Vec<_>>(),
        ))),
        "data_sequence_number" if logical_metadata => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.data_sequence_number)
                .collect::<Vec<_>>(),
        ))),
        "column_stats" if logical_metadata => {
            let values = rows
                .iter()
                .map(|row| {
                    if load_column_stats {
                        row.column_stats.as_deref()
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            build_binary_array(values)
        }
        "key_metadata" => build_binary_array(
            rows.iter()
                .map(|row| row.key_metadata.as_deref())
                .collect::<Vec<_>>(),
        ),
        other => Err(format!(
            "unsupported iceberg {} metadata column: {}",
            if logical_metadata { "logical" } else { "files" },
            other
        )),
    }
}

fn build_manifest_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[ManifestMetadataRow],
) -> Result<ArrayRef, String> {
    match column.name.as_str() {
        "path" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.path.as_str()))
                .collect::<Vec<_>>(),
        ))),
        "length" => Ok(Arc::new(Int64Array::from(
            rows.iter().map(|row| row.length).collect::<Vec<_>>(),
        ))),
        "partition_spec_id" => Ok(Arc::new(Int32Array::from(
            rows.iter()
                .map(|row| row.partition_spec_id)
                .collect::<Vec<_>>(),
        ))),
        "added_snapshot_id" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.added_snapshot_id)
                .collect::<Vec<_>>(),
        ))),
        "added_data_files_count" => Ok(Arc::new(Int32Array::from(
            rows.iter()
                .map(|row| row.added_data_files_count)
                .collect::<Vec<_>>(),
        ))),
        "added_rows_count" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.added_rows_count)
                .collect::<Vec<_>>(),
        ))),
        "existing_data_files_count" => Ok(Arc::new(Int32Array::from(
            rows.iter()
                .map(|row| row.existing_data_files_count)
                .collect::<Vec<_>>(),
        ))),
        "existing_rows_count" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.existing_rows_count)
                .collect::<Vec<_>>(),
        ))),
        "deleted_data_files_count" => Ok(Arc::new(Int32Array::from(
            rows.iter()
                .map(|row| row.deleted_data_files_count)
                .collect::<Vec<_>>(),
        ))),
        "deleted_rows_count" => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.deleted_rows_count)
                .collect::<Vec<_>>(),
        ))),
        "partitions" => build_manifest_partitions_array(column, rows),
        other => Err(format!(
            "unsupported iceberg manifests metadata column: {}",
            other
        )),
    }
}

fn build_binary_array(values: Vec<Option<&[u8]>>) -> Result<ArrayRef, String> {
    let mut builder = BinaryBuilder::new();
    for value in values {
        match value {
            Some(bytes) => builder.append_value(bytes),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_i32_i64_map_array<'a, I>(rows: I) -> Result<ArrayRef, String>
where
    I: IntoIterator<Item = Option<&'a Vec<(i32, i64)>>>,
{
    let mut builder = MapBuilder::new(None, Int32Builder::new(), Int64Builder::new());
    for row in rows {
        match row {
            Some(entries) => {
                for (key, value) in entries {
                    builder.keys().append_value(*key);
                    builder.values().append_value(*value);
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

fn build_i32_utf8_map_array<'a, I>(rows: I) -> Result<ArrayRef, String>
where
    I: IntoIterator<Item = Option<&'a Vec<(i32, String)>>>,
{
    let mut builder = MapBuilder::new(None, Int32Builder::new(), StringBuilder::new());
    for row in rows {
        match row {
            Some(entries) => {
                for (key, value) in entries {
                    builder.keys().append_value(*key);
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

fn build_i64_list_array<'a, I>(rows: I) -> Result<ArrayRef, String>
where
    I: IntoIterator<Item = Option<&'a Vec<i64>>>,
{
    let mut builder = ListBuilder::new(Int64Builder::new());
    for row in rows {
        match row {
            Some(values) => {
                for value in values {
                    builder.values().append_value(*value);
                }
                builder.append(true);
            }
            None => {
                builder.append(false);
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_i32_list_array<'a, I>(rows: I) -> Result<ArrayRef, String>
where
    I: IntoIterator<Item = Option<&'a Vec<i32>>>,
{
    let mut builder = ListBuilder::new(Int32Builder::new());
    for row in rows {
        match row {
            Some(values) => {
                for value in values {
                    builder.values().append_value(*value);
                }
                builder.append(true);
            }
            None => {
                builder.append(false);
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_manifest_partitions_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[ManifestMetadataRow],
) -> Result<ArrayRef, String> {
    let DataType::List(item_field) = &column.data_type else {
        return Err(format!(
            "iceberg manifests partitions expects List type, got {:?}",
            column.data_type
        ));
    };
    let DataType::Struct(struct_fields) = item_field.data_type() else {
        return Err(format!(
            "iceberg manifests partitions list item expects Struct type, got {:?}",
            item_field.data_type()
        ));
    };

    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(0_i32);
    let mut list_nulls = NullBufferBuilder::new(rows.len());

    let mut contains_null = StringBuilder::new();
    let mut contains_nan = StringBuilder::new();
    let mut lower_bound = StringBuilder::new();
    let mut upper_bound = StringBuilder::new();

    let mut total = 0_i32;
    for row in rows {
        match row.partitions.as_ref() {
            Some(partitions) => {
                list_nulls.append(true);
                for part in partitions {
                    match part.contains_null.as_deref() {
                        Some(value) => contains_null.append_value(value),
                        None => contains_null.append_null(),
                    }
                    match part.contains_nan.as_deref() {
                        Some(value) => contains_nan.append_value(value),
                        None => contains_nan.append_null(),
                    }
                    match part.lower_bound.as_deref() {
                        Some(value) => lower_bound.append_value(value),
                        None => lower_bound.append_null(),
                    }
                    match part.upper_bound.as_deref() {
                        Some(value) => upper_bound.append_value(value),
                        None => upper_bound.append_null(),
                    }
                }
                total += i32::try_from(partitions.len())
                    .map_err(|_| "manifest partitions row count overflow".to_string())?;
            }
            None => list_nulls.append(false),
        }
        offsets.push(total);
    }

    let struct_array = StructArray::new(
        struct_fields.clone(),
        vec![
            Arc::new(contains_null.finish()),
            Arc::new(contains_nan.finish()),
            Arc::new(lower_bound.finish()),
            Arc::new(upper_bound.finish()),
        ],
        None,
    );
    let list = ListArray::new(
        Arc::clone(item_field),
        OffsetBuffer::new(offsets.into()),
        Arc::new(struct_array),
        list_nulls.finish(),
    );
    Ok(Arc::new(list))
}

fn read_path_bytes(
    path: &str,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Vec<u8>, String> {
    let (op, resolved) = resolve_opendal_paths(&[path.to_string()], object_store_config)?;
    let rel = resolved
        .paths
        .first()
        .ok_or_else(|| format!("failed to resolve path: {}", path))?;
    let bytes = crate::fs::oss::oss_block_on(op.read(rel))?
        .map_err(|e| format!("failed to read {}: {}", path, e))?;
    Ok(bytes.to_vec())
}

fn adjusted_object_store_config(
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
    path: &str,
) -> Option<crate::fs::object_store::ObjectStoreConfig> {
    let Some(cfg) = object_store_config else {
        return None;
    };
    if !cfg.bucket.is_empty() {
        return Some(cfg.clone());
    }
    let bucket = match parse_bucket_from_path(path) {
        Some(bucket) => bucket,
        None => return Some(cfg.clone()),
    };
    let mut adjusted = cfg.clone();
    adjusted.bucket = bucket;
    Some(adjusted)
}

fn parse_bucket_from_path(path: &str) -> Option<String> {
    for prefix in ["oss://", "s3://"] {
        let rest = path.strip_prefix(prefix)?;
        let bucket = rest.split('/').next()?.trim();
        if !bucket.is_empty() {
            return Some(bucket.to_string());
        }
    }
    None
}

fn extract_metadata_json_path(serialized_table: &str) -> Result<String, String> {
    let encoded = serialized_table.replace(['\r', '\n'], "");
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|e| format!("decode iceberg serialized_table failed: {}", e))?;
    let text = String::from_utf8_lossy(&bytes);
    let re = Regex::new(
        r#"(?:(?:oss|s3|hdfs)://[^\s"']+?\.metadata\.json|file:/[^\s"']+?\.metadata\.json|/[^\s"']+?\.metadata\.json)"#,
    )
    .map_err(|e| format!("build metadata path regex failed: {}", e))?;

    let mut selected: Option<(i64, String)> = None;
    for matched in re.find_iter(&text) {
        let path = normalize_extracted_metadata_path(matched.as_str());
        let version = metadata_json_version(&path);
        match selected.as_ref() {
            Some((current_version, _)) if *current_version > version => {}
            _ => selected = Some((version, path)),
        }
    }
    selected.map(|(_, path)| path).ok_or_else(|| {
        "failed to extract iceberg metadata json path from serialized_table".to_string()
    })
}

fn metadata_json_version(path: &str) -> i64 {
    let Some(file_name) = path.rsplit('/').next() else {
        return -1;
    };
    let Some(version) = file_name
        .strip_prefix('v')
        .and_then(|rest| rest.strip_suffix(".metadata.json"))
    else {
        return -1;
    };
    version.parse::<i64>().unwrap_or(-1)
}

fn normalize_extracted_metadata_path(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("file://") {
        if rest.starts_with('/') || rest.starts_with("localhost/") {
            return path.to_string();
        }
        return format!("file:///{}", rest.trim_start_matches('/'));
    }
    path.to_string()
}

fn extract_manifest_list_path(metadata_json: &[u8]) -> Result<String, String> {
    let json: JsonValue = serde_json::from_slice(metadata_json)
        .map_err(|e| format!("parse iceberg metadata json failed: {}", e))?;
    let current_snapshot_id = json
        .get("current-snapshot-id")
        .and_then(JsonValue::as_i64)
        .ok_or_else(|| "iceberg metadata json missing current-snapshot-id".to_string())?;
    let snapshots = json
        .get("snapshots")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| "iceberg metadata json missing snapshots".to_string())?;
    snapshots
        .iter()
        .find(|snapshot| {
            snapshot.get("snapshot-id").and_then(JsonValue::as_i64) == Some(current_snapshot_id)
        })
        .and_then(|snapshot| snapshot.get("manifest-list").and_then(JsonValue::as_str))
        .map(|path| path.to_string())
        .ok_or_else(|| {
            format!(
                "manifest-list not found for snapshot {}",
                current_snapshot_id
            )
        })
}

fn encode_partition_data(value: Option<&AvroValue>) -> Result<Option<Vec<u8>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = avro_unwrap_union(value);
    let Some(fields) = avro_record_fields(value) else {
        return Ok(None);
    };
    if fields.is_empty() {
        return Ok(None);
    }
    let mut obj = JsonMap::new();
    for (name, field_value) in fields {
        obj.insert(name.clone(), avro_value_to_json(field_value)?);
    }
    serde_json::to_vec(&JsonValue::Object(obj))
        .map(Some)
        .map_err(|e| format!("serialize partition_data failed: {}", e))
}

fn encode_column_stats(
    column_sizes: Option<&Vec<(i32, i64)>>,
    value_counts: Option<&Vec<(i32, i64)>>,
    null_value_counts: Option<&Vec<(i32, i64)>>,
    nan_value_counts: Option<&Vec<(i32, i64)>>,
    lower_bounds: Option<&Vec<(i32, String)>>,
    upper_bounds: Option<&Vec<(i32, String)>>,
) -> Result<Option<Vec<u8>>, String> {
    if column_sizes.is_none()
        && value_counts.is_none()
        && null_value_counts.is_none()
        && nan_value_counts.is_none()
        && lower_bounds.is_none()
        && upper_bounds.is_none()
    {
        return Ok(None);
    }
    let payload = json!({
        "column_sizes": map_entries_to_json(column_sizes),
        "value_counts": map_entries_to_json(value_counts),
        "null_value_counts": map_entries_to_json(null_value_counts),
        "nan_value_counts": map_entries_to_json(nan_value_counts),
        "lower_bounds": string_map_entries_to_json(lower_bounds),
        "upper_bounds": string_map_entries_to_json(upper_bounds),
    });
    serde_json::to_vec(&payload)
        .map(Some)
        .map_err(|e| format!("serialize column_stats failed: {}", e))
}

fn map_entries_to_json(entries: Option<&Vec<(i32, i64)>>) -> JsonValue {
    let Some(entries) = entries else {
        return JsonValue::Null;
    };
    let mut obj = JsonMap::new();
    for (key, value) in entries {
        obj.insert(key.to_string(), JsonValue::from(*value));
    }
    JsonValue::Object(obj)
}

fn string_map_entries_to_json(entries: Option<&Vec<(i32, String)>>) -> JsonValue {
    let Some(entries) = entries else {
        return JsonValue::Null;
    };
    let mut obj = JsonMap::new();
    for (key, value) in entries {
        obj.insert(key.to_string(), JsonValue::from(value.clone()));
    }
    JsonValue::Object(obj)
}

fn avro_record_fields(value: &AvroValue) -> Option<&Vec<(String, AvroValue)>> {
    match avro_unwrap_union(value) {
        AvroValue::Record(fields) => Some(fields),
        _ => None,
    }
}

fn avro_field<'a>(fields: &'a [(String, AvroValue)], name: &str) -> Option<&'a AvroValue> {
    fields
        .iter()
        .find(|(field_name, _)| field_name == name)
        .map(|(_, value)| value)
}

fn avro_unwrap_union(value: &AvroValue) -> &AvroValue {
    match value {
        AvroValue::Union(_, inner) => avro_unwrap_union(inner.as_ref()),
        other => other,
    }
}

fn avro_string(value: Option<&AvroValue>) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match avro_unwrap_union(value) {
        AvroValue::String(value) => Ok(Some(value.clone())),
        AvroValue::Bytes(value) => Ok(Some(hex::encode(value))),
        AvroValue::Fixed(_, value) => Ok(Some(hex::encode(value))),
        AvroValue::Null => Ok(None),
        other => Err(format!(
            "expected string-compatible avro value, got {:?}",
            other
        )),
    }
}

fn avro_string_required(value: Option<&AvroValue>, name: &str) -> Result<String, String> {
    avro_string(value)?.ok_or_else(|| format!("missing required avro string field {}", name))
}

fn avro_i32(value: Option<&AvroValue>) -> Result<Option<i32>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match avro_unwrap_union(value) {
        AvroValue::Int(value) => Ok(Some(*value)),
        AvroValue::Long(value) => i32::try_from(*value)
            .map(Some)
            .map_err(|_| format!("avro long out of i32 range: {}", value)),
        AvroValue::Null => Ok(None),
        other => Err(format!(
            "expected int-compatible avro value, got {:?}",
            other
        )),
    }
}

fn avro_i32_required(value: Option<&AvroValue>, name: &str) -> Result<i32, String> {
    avro_i32(value)?.ok_or_else(|| format!("missing required avro int field {}", name))
}

fn avro_i64(value: Option<&AvroValue>) -> Result<Option<i64>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match avro_unwrap_union(value) {
        AvroValue::Int(value) => Ok(Some(i64::from(*value))),
        AvroValue::Long(value) => Ok(Some(*value)),
        AvroValue::Null => Ok(None),
        other => Err(format!(
            "expected long-compatible avro value, got {:?}",
            other
        )),
    }
}

fn avro_i64_required(value: Option<&AvroValue>, name: &str) -> Result<i64, String> {
    avro_i64(value)?.ok_or_else(|| format!("missing required avro long field {}", name))
}

fn avro_bytes(value: Option<&AvroValue>) -> Result<Option<Vec<u8>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match avro_unwrap_union(value) {
        AvroValue::Bytes(value) => Ok(Some(value.clone())),
        AvroValue::Fixed(_, value) => Ok(Some(value.clone())),
        AvroValue::Null => Ok(None),
        other => Err(format!("expected bytes avro value, got {:?}", other)),
    }
}

fn avro_i64_list(value: Option<&AvroValue>) -> Result<Option<Vec<i64>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match avro_unwrap_union(value) {
        AvroValue::Array(values) => values
            .iter()
            .map(|value| avro_i64_required(Some(value), "list_item"))
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        AvroValue::Null => Ok(None),
        other => Err(format!("expected array avro value, got {:?}", other)),
    }
}

fn avro_i32_list(value: Option<&AvroValue>) -> Result<Option<Vec<i32>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match avro_unwrap_union(value) {
        AvroValue::Array(values) => values
            .iter()
            .map(|value| avro_i32_required(Some(value), "list_item"))
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        AvroValue::Null => Ok(None),
        other => Err(format!("expected array avro value, got {:?}", other)),
    }
}

fn avro_int_long_map(value: Option<&AvroValue>) -> Result<Option<Vec<(i32, i64)>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match avro_unwrap_union(value) {
        AvroValue::Array(entries) => entries
            .iter()
            .map(|entry| {
                let fields = avro_record_fields(entry)
                    .ok_or_else(|| "expected avro key/value record".to_string())?;
                Ok((
                    avro_i32_required(avro_field(fields, "key"), "key")?,
                    avro_i64_required(avro_field(fields, "value"), "value")?,
                ))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        AvroValue::Map(entries) => {
            let mut out = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                out.push((
                    key.parse::<i32>()
                        .map_err(|e| format!("invalid avro map key {}: {}", key, e))?,
                    avro_i64_required(Some(value), "value")?,
                ));
            }
            Ok(Some(out))
        }
        AvroValue::Null => Ok(None),
        other => Err(format!(
            "expected map-compatible avro value, got {:?}",
            other
        )),
    }
}

fn avro_int_string_map(value: Option<&AvroValue>) -> Result<Option<Vec<(i32, String)>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match avro_unwrap_union(value) {
        AvroValue::Array(entries) => entries
            .iter()
            .map(|entry| {
                let fields = avro_record_fields(entry)
                    .ok_or_else(|| "expected avro key/value record".to_string())?;
                Ok((
                    avro_i32_required(avro_field(fields, "key"), "key")?,
                    avro_string_required(avro_field(fields, "value"), "value")?,
                ))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        AvroValue::Map(entries) => {
            let mut out = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                out.push((
                    key.parse::<i32>()
                        .map_err(|e| format!("invalid avro map key {}: {}", key, e))?,
                    avro_string_required(Some(value), "value")?,
                ));
            }
            Ok(Some(out))
        }
        AvroValue::Null => Ok(None),
        other => Err(format!(
            "expected map-compatible avro value, got {:?}",
            other
        )),
    }
}

fn avro_manifest_partitions(
    value: Option<&AvroValue>,
) -> Result<Option<Vec<ManifestPartitionSummary>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match avro_unwrap_union(value) {
        AvroValue::Array(values) => values
            .iter()
            .map(|value| {
                let fields = avro_record_fields(value)
                    .ok_or_else(|| "expected avro manifest partition summary record".to_string())?;
                Ok(ManifestPartitionSummary {
                    contains_null: avro_bool_to_string(avro_field(fields, "contains_null"))?,
                    contains_nan: avro_bool_to_string(avro_field(fields, "contains_nan"))?,
                    lower_bound: avro_string(avro_field(fields, "lower_bound"))?,
                    upper_bound: avro_string(avro_field(fields, "upper_bound"))?,
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        AvroValue::Null => Ok(None),
        other => Err(format!(
            "expected array avro value for manifest partitions, got {:?}",
            other
        )),
    }
}

fn avro_bool_to_string(value: Option<&AvroValue>) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match avro_unwrap_union(value) {
        AvroValue::Boolean(value) => Ok(Some(value.to_string())),
        AvroValue::Null => Ok(None),
        other => Err(format!("expected bool avro value, got {:?}", other)),
    }
}

fn avro_metadata_i32(metadata: &HashMap<String, Vec<u8>>, key: &str) -> Result<i32, String> {
    let value = metadata
        .get(key)
        .ok_or_else(|| format!("iceberg avro metadata missing key {}", key))?;
    let text = std::str::from_utf8(value)
        .map_err(|e| format!("iceberg avro metadata {} is not utf8: {}", key, e))?;
    text.parse::<i32>()
        .map_err(|e| format!("parse iceberg avro metadata {} failed: {}", key, e))
}

fn avro_value_to_json(value: &AvroValue) -> Result<JsonValue, String> {
    match avro_unwrap_union(value) {
        AvroValue::Null => Ok(JsonValue::Null),
        AvroValue::Boolean(value) => Ok(JsonValue::Bool(*value)),
        AvroValue::Int(value) => Ok(JsonValue::from(*value)),
        AvroValue::Long(value) => Ok(JsonValue::from(*value)),
        AvroValue::Float(value) => Ok(JsonValue::from(*value)),
        AvroValue::Double(value) => Ok(JsonValue::from(*value)),
        AvroValue::String(value) => Ok(JsonValue::from(value.clone())),
        AvroValue::Bytes(value) => Ok(JsonValue::from(hex::encode(value))),
        AvroValue::Fixed(_, value) => Ok(JsonValue::from(hex::encode(value))),
        AvroValue::Array(values) => values
            .iter()
            .map(avro_value_to_json)
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        AvroValue::Map(values) => {
            let mut out = JsonMap::new();
            for (key, value) in values {
                out.insert(key.clone(), avro_value_to_json(value)?);
            }
            Ok(JsonValue::Object(out))
        }
        AvroValue::Record(fields) => {
            let mut out = JsonMap::new();
            for (key, value) in fields {
                out.insert(key.clone(), avro_value_to_json(value)?);
            }
            Ok(JsonValue::Object(out))
        }
        other => Err(format!(
            "unsupported avro value to json conversion: {:?}",
            other
        )),
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;

    use super::{
        IcebergMetadataScanRange, adjusted_object_store_config, extract_manifest_list_path,
        extract_metadata_json_path, load_file_rows, load_manifest_rows,
    };

    #[test]
    fn extract_metadata_path_prefers_latest_version() {
        let serialized = "rO0ABXQAJGZpbGU6L3RtcC90L21ldGFkYXRhL3YxLm1ldGFkYXRhLmpzb250AClkZmlsZTovL3RtcC90L21ldGFkYXRhL3YyLm1ldGFkYXRhLmpzb24=";
        let path = extract_metadata_json_path(serialized).expect("extract metadata path");
        assert_eq!(path, "file:///tmp/t/metadata/v2.metadata.json");
    }

    #[test]
    fn adjusted_object_store_config_fills_bucket_from_path() {
        let cfg = crate::fs::object_store::ObjectStoreConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: String::new(),
            root: String::new(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            session_token: None,
            enable_path_style_access: Some(true),
            region: None,
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        };
        let adjusted = adjusted_object_store_config(Some(&cfg), "oss://demo-bucket/path/to/file")
            .expect("adjusted config");
        assert_eq!(adjusted.bucket, "demo-bucket");
    }

    #[test]
    fn extract_manifest_list_path_from_local_metadata_json() {
        let bytes = std::fs::read(
            "/Users/harbor/project/NovaRocks/sql-tests/.sql_test_catalog/tpch/customer/metadata/v2.metadata.json",
        )
        .expect("read metadata json");
        let path = extract_manifest_list_path(&bytes).expect("extract manifest list path");
        assert!(path.ends_with(".avro"));
        assert!(path.contains("snap-"));
    }

    #[test]
    fn load_manifest_rows_from_local_catalog() {
        let serialized = base64::engine::general_purpose::STANDARD.encode(
            b"file:///Users/harbor/project/NovaRocks/sql-tests/.sql_test_catalog/tpch/customer/metadata/v2.metadata.json",
        );
        let rows = load_manifest_rows(&serialized, None).expect("load manifest rows");
        assert_eq!(rows.len(), 1);
        assert!(rows[0].path.ends_with(".avro"));
    }

    #[test]
    fn load_file_rows_from_local_catalog() {
        let range = IcebergMetadataScanRange {
            path: "/Users/harbor/project/NovaRocks/sql-tests/.sql_test_catalog/tpch/customer/metadata/2a1f36d0-237d-41b0-9dec-6b3228da8fba-m0.avro".to_string(),
        };
        let rows = load_file_rows(&range, None).expect("load file rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].spec_id, 0);
        assert!(rows[0].value_counts.is_some());
    }
}
