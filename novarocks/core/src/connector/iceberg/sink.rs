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
//! Iceberg provider-side staged-file helpers.
//!
//! Responsibilities:
//! - Build bounded Iceberg staged-writer metadata and object-store access.
//! - Encode Parquet files and provider-private statistics used by the common
//!   connector writer execution adapter.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use base64::Engine;
use novarocks_connector_iceberg::iceberg::spec::TableMetadata;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::data_type::AsBytes;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::{Statistics, ValueStatistics};

use super::data_writer::StagedWriteContext;
use crate::connector::iceberg::report::IcebergColumnStats;
use crate::connector::iceberg::sink_plan::{
    IcebergSinkMode, IcebergSinkObjectStoreConfig, IcebergSinkPlan, PositionDeleteDataFilePartition,
};
use crate::runtime::global_async_runtime::data_block_on;
use novarocks_execution::exec::chunk::Chunk;
use novarocks_execution::exec::expr::{ExprArena, ExprId, cast_with_special_rules};
use novarocks_execution::exec::row_position::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};

pub(crate) fn build_position_delete_data_file_partition_index(
    metadata: &TableMetadata,
    target_snapshot_id: Option<i64>,
    table_location: &str,
    s3_config: Option<&IcebergSinkObjectStoreConfig>,
) -> Result<HashMap<String, PositionDeleteDataFilePartition>, String> {
    use novarocks_connector_iceberg::iceberg::spec::{
        DataContentType, ManifestContentType, ManifestStatus,
    };

    let Some(snapshot_id) = target_snapshot_id.or_else(|| metadata.current_snapshot_id()) else {
        return Ok(HashMap::new());
    };
    let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
        format!("Iceberg delete sink target snapshot id {snapshot_id} not found in table metadata")
    })?;
    let file_io = build_staged_file_io(table_location, s3_config)?;
    data_block_on(async {
        let manifest_list = snapshot
            .load_manifest_list(&file_io, metadata)
            .await
            .map_err(|e| format!("load Iceberg position-delete target manifest list: {e}"))?;
        let mut index = HashMap::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file.load_manifest(&file_io).await.map_err(|e| {
                format!(
                    "load Iceberg position-delete data manifest {} failed: {e}",
                    manifest_file.manifest_path
                )
            })?;
            for entry in manifest.entries() {
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }
                let data_file = entry.data_file();
                if data_file.content_type() != DataContentType::Data {
                    continue;
                }
                let partition = PositionDeleteDataFilePartition {
                    partition_spec_id: manifest_file.partition_spec_id,
                    partition_values: data_file.partition().clone(),
                };
                insert_position_delete_data_file_partition(
                    &mut index,
                    data_file.file_path().to_string(),
                    partition,
                )?;
            }
        }
        Ok(index)
    })?
}

fn insert_position_delete_data_file_partition(
    index: &mut HashMap<String, PositionDeleteDataFilePartition>,
    path: String,
    partition: PositionDeleteDataFilePartition,
) -> Result<(), String> {
    match index.entry(path) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(partition);
            Ok(())
        }
        std::collections::hash_map::Entry::Occupied(entry) => {
            let existing = entry.get();
            if existing.partition_spec_id == partition.partition_spec_id
                && existing.partition_values == partition.partition_values
            {
                return Ok(());
            }
            Err(format!(
                "Iceberg data file `{}` has conflicting partition metadata: old partition spec id {}, new partition spec id {}",
                entry.key(),
                existing.partition_spec_id,
                partition.partition_spec_id
            ))
        }
    }
}

impl IcebergSinkPlan {
    pub(crate) fn build_staged_write_context(&self) -> Result<StagedWriteContext, String> {
        if self.mode != IcebergSinkMode::Data {
            return Err(format!(
                "staged data-file writer context is only valid for DATA sinks, got {:?}",
                self.mode
            ));
        }

        let writer_schema = Arc::new(iceberg_schema_from_arrow_schema(&self.target_schema)?);
        let metadata = self.build_target_table_metadata(writer_schema.as_ref())?;
        let annotated_schema = Arc::new(
            novarocks_connector_iceberg::iceberg::arrow::schema_to_arrow_schema(&writer_schema)
                .map_err(|e| format!("convert staged iceberg schema to arrow failed: {e}"))?,
        );
        let file_io = build_staged_file_io(&self.data_location, self.object_store_s3.as_ref())?;

        StagedWriteContext::from_parts_with_partition_spec_id(
            metadata,
            file_io,
            writer_schema,
            annotated_schema,
            self.target_partition_spec_id,
        )
    }

    fn build_target_table_metadata(
        &self,
        writer_schema: &novarocks_connector_iceberg::iceberg::spec::Schema,
    ) -> Result<novarocks_connector_iceberg::iceberg::spec::TableMetadata, String> {
        let partition_spec = build_staged_partition_spec(
            writer_schema,
            self.target_partition_spec_id,
            &self.partition_source_column_names,
            &self.partition_column_names,
            &self.transform_exprs,
        )?;
        let mut properties = self
            .target_table_metadata
            .as_ref()
            .map(|metadata| metadata.properties().clone())
            .unwrap_or_default();
        properties.insert("write.data.path".to_string(), self.data_location.clone());
        novarocks_connector_iceberg::iceberg::spec::TableMetadataBuilder::new(
            writer_schema.clone(),
            novarocks_connector_iceberg::iceberg::spec::PartitionSpec::unpartition_spec(),
            novarocks_connector_iceberg::iceberg::spec::SortOrder::unsorted_order(),
            self.table_location.clone(),
            novarocks_connector_iceberg::iceberg::spec::FormatVersion::V2,
            properties,
        )
        .map_err(|e| format!("build staged iceberg table metadata failed: {e}"))?
        .add_current_schema(writer_schema.clone())
        .map_err(|e| format!("add staged iceberg writer schema failed: {e}"))?
        .add_default_partition_spec(partition_spec)
        .map_err(|e| format!("add staged iceberg partition spec failed: {e}"))?
        .build()
        .map_err(|e| format!("finalize staged iceberg table metadata failed: {e}"))
        .and_then(|built| {
            retag_default_partition_spec_id(
                built.metadata,
                self.target_partition_spec_id,
                &self.partition_column_names,
            )
        })
    }
}

fn delete_target_snapshot_id(
    metadata: &novarocks_connector_iceberg::iceberg::spec::TableMetadata,
    target_snapshot_id: Option<i64>,
) -> Option<i64> {
    target_snapshot_id.or_else(|| metadata.current_snapshot_id())
}

fn retag_default_partition_spec_id(
    metadata: novarocks_connector_iceberg::iceberg::spec::TableMetadata,
    target_spec_id: i32,
    partition_column_names: &[String],
) -> Result<novarocks_connector_iceberg::iceberg::spec::TableMetadata, String> {
    // iceberg-rust may assign a fresh spec id when adding a partition spec to
    // synthetic metadata. Writer reports must carry the target table's real
    // spec id so the commit collector can decode descriptors against target
    // metadata.
    let mut value = serde_json::to_value(metadata)
        .map_err(|e| format!("serialize staged iceberg table metadata failed: {e}"))?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| "staged iceberg table metadata must serialize to an object".to_string())?;
    object.insert(
        "default-spec-id".to_string(),
        serde_json::Value::from(target_spec_id),
    );

    let specs = object
        .get_mut("partition-specs")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| "staged iceberg table metadata missing partition-specs array".to_string())?;
    let desired_idx = specs
        .iter()
        .position(|spec| partition_spec_names_match(spec, partition_column_names))
        .ok_or_else(|| {
            format!(
                "staged iceberg table metadata missing partition spec for fields {:?}",
                partition_column_names
            )
        })?;
    let mut desired_spec = specs[desired_idx].clone();
    let spec_object = desired_spec
        .as_object_mut()
        .ok_or_else(|| "staged iceberg partition spec must serialize to an object".to_string())?;
    spec_object.insert(
        "spec-id".to_string(),
        serde_json::Value::from(target_spec_id),
    );
    *specs = vec![desired_spec];

    let metadata =
        serde_json::from_value::<novarocks_connector_iceberg::iceberg::spec::TableMetadata>(value)
            .map_err(|e| format!("deserialize staged iceberg table metadata failed: {e}"))?;
    let spec = metadata.partition_spec_by_id(target_spec_id).ok_or_else(|| {
        format!(
            "staged iceberg table metadata failed to retag default partition spec id to {target_spec_id}"
        )
    })?;
    if metadata.default_partition_spec_id() != target_spec_id
        || !partition_spec_ref_names_match(spec, partition_column_names)
    {
        return Err(format!(
            "staged iceberg table metadata default partition spec does not match fields {:?}",
            partition_column_names
        ));
    }
    Ok(metadata)
}

fn partition_spec_names_match(spec: &serde_json::Value, partition_column_names: &[String]) -> bool {
    let Some(spec_object) = spec.as_object() else {
        return false;
    };
    let Some(fields) = spec_object
        .get("fields")
        .and_then(serde_json::Value::as_array)
    else {
        return partition_column_names.is_empty();
    };
    if fields.len() != partition_column_names.len() {
        return false;
    }
    fields
        .iter()
        .zip(partition_column_names.iter())
        .all(|(field, expected)| {
            field
                .as_object()
                .and_then(|object| object.get("name"))
                .and_then(serde_json::Value::as_str)
                == Some(expected.as_str())
        })
}

fn partition_spec_ref_names_match(
    spec: &novarocks_connector_iceberg::iceberg::spec::PartitionSpecRef,
    partition_column_names: &[String],
) -> bool {
    spec.fields().len() == partition_column_names.len()
        && spec
            .fields()
            .iter()
            .zip(partition_column_names.iter())
            .all(|(field, expected)| field.name == *expected)
}

pub(crate) fn unique_file_path(batch: &RecordBatch) -> Result<Option<String>, String> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let file_path_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "position-delete batch missing file_path Utf8 column".to_string())?;
    let first = file_path_col.value(0);
    for row in 1..batch.num_rows() {
        if file_path_col.value(row) != first {
            return Ok(None);
        }
    }
    Ok(Some(first.to_string()))
}

fn group_positions_by_file(
    batch: &RecordBatch,
    sink_label: &str,
) -> Result<BTreeMap<String, Vec<u64>>, String> {
    let file_path_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("iceberg {sink_label} sink: file_path array expected as Utf8"))?;
    let pos_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("iceberg {sink_label} sink: pos array expected as Int64"))?;
    if file_path_col.null_count() > 0 || pos_col.null_count() > 0 {
        return Err(format!(
            "iceberg {sink_label} sink rejects NULL file_path or pos"
        ));
    }

    let mut out: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        let pos = pos_col.value(row);
        if pos < 0 {
            return Err(format!(
                "iceberg {sink_label} sink pos must be non-negative: {pos}"
            ));
        }
        out.entry(file_path_col.value(row).to_string())
            .or_default()
            .push(pos as u64);
    }
    Ok(out)
}

fn merge_deletion_vectors_by_file(
    mut existing: HashMap<String, crate::connector::iceberg::commit::DeletionVector>,
    positions_by_file: &BTreeMap<String, Vec<u64>>,
) -> Result<BTreeMap<String, crate::connector::iceberg::commit::DeletionVector>, String> {
    let mut out = BTreeMap::new();
    for (file, positions) in positions_by_file {
        let mut dv = existing.remove(file).unwrap_or_default();
        for pos in positions {
            dv.insert(*pos).map_err(|e| {
                format!(
                    "iceberg deletion-vector sink insert position {pos} for `{file}` failed: {e}"
                )
            })?;
        }
        out.insert(file.clone(), dv);
    }
    Ok(out)
}

fn merge_existing_with_pending_deletion_vectors(
    mut existing: HashMap<String, crate::connector::iceberg::commit::DeletionVector>,
    pending: &BTreeMap<String, crate::connector::iceberg::commit::DeletionVector>,
) -> BTreeMap<String, crate::connector::iceberg::commit::DeletionVector> {
    let mut out = BTreeMap::new();
    for (file, pending_dv) in pending {
        let mut dv = existing.remove(file).unwrap_or_default();
        dv.merge(pending_dv);
        out.insert(file.clone(), dv);
    }
    out
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    partition_spec_id: i32,
    path: String,
    null_fingerprint: String,
    partition_key: String,
}

#[derive(Debug)]
struct PartitionGroup {
    indices: Vec<u32>,
    partition_spec_id: i32,
    partition_values: novarocks_connector_iceberg::iceberg::spec::Struct,
}

pub(crate) struct ParquetWriteResult {
    pub(crate) file_size: u64,
    pub(crate) split_offsets: Option<Vec<i64>>,
    pub(crate) column_stats: Option<IcebergColumnStats>,
    /// Per-primitive-column Theta sketches keyed by Iceberg field id. None
    /// when the sink could not compute sketches (e.g. schema lacks parquet
    /// field-id metadata). Wrapped in `Option` so the existing
    /// position-delete path that does not produce sketches stays cheap.
    ///
    /// NOTE: this field is populated but not yet consumed by the commit
    /// action plumbing (Phase 2.3). The `dead_code` lint is suppressed
    /// because the field is intentionally written ahead of its reader.
    #[allow(dead_code)]
    theta_sketches:
        Option<HashMap<i32, novarocks_connector_iceberg::theta_sketch::ThetaSketchHandle>>,
}

#[derive(Default)]
struct ColumnStatsAccumulator {
    column_size: i64,
    value_count: i64,
    null_value_count: i64,
    has_statistics: bool,
    merged_statistics: Option<Statistics>,
}

fn eval_exprs(arena: &ExprArena, exprs: &[ExprId], chunk: &Chunk) -> Result<Vec<ArrayRef>, String> {
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        out.push(arena.eval(*expr, chunk)?);
    }
    Ok(out)
}

fn align_arrays_to_schema(
    arrays: Vec<ArrayRef>,
    schema: &SchemaRef,
) -> Result<Vec<ArrayRef>, String> {
    if arrays.len() != schema.fields().len() {
        return Err(format!(
            "iceberg sink column count mismatch while aligning arrays: arrays={} schema={}",
            arrays.len(),
            schema.fields().len()
        ));
    }

    arrays
        .into_iter()
        .zip(schema.fields().iter())
        .enumerate()
        .map(|(idx, (array, field))| {
            let target_type = field.data_type();
            if array.data_type() == target_type {
                return Ok(array);
            }

            let casted = if data_type_contains_largeint(target_type) {
                cast_with_special_rules(&array, target_type)
            } else {
                cast(array.as_ref(), target_type).map_err(|e| e.to_string())
            }
            .map_err(|e| {
                format!(
                    "iceberg sink cast failed at column index {} name={} from {:?} to {:?}: {}",
                    idx,
                    field.name(),
                    array.data_type(),
                    target_type,
                    e
                )
            })?;

            if !matches!(array.data_type(), DataType::Null)
                && casted.null_count() > array.null_count()
            {
                return Err(format!(
                    "iceberg sink cast introduced nulls at column index {} name={} from {:?} to {:?}",
                    idx,
                    field.name(),
                    array.data_type(),
                    target_type
                ));
            }
            Ok(casted)
        })
        .collect()
}

fn data_type_contains_largeint(data_type: &DataType) -> bool {
    match data_type {
        DataType::FixedSizeBinary(width) => {
            *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            data_type_contains_largeint(field.data_type())
        }
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| data_type_contains_largeint(field.data_type())),
        DataType::Map(entries, _) => data_type_contains_largeint(entries.data_type()),
        _ => false,
    }
}

fn iceberg_schema_from_arrow_schema(
    schema: &Schema,
) -> Result<novarocks_connector_iceberg::iceberg::spec::Schema, String> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| iceberg_nested_field_from_arrow_field(field.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    novarocks_connector_iceberg::iceberg::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields(fields)
        .build()
        .map_err(|e| format!("build staged iceberg writer schema failed: {e}"))
}

fn iceberg_nested_field_from_arrow_field(
    field: &Field,
) -> Result<novarocks_connector_iceberg::iceberg::spec::NestedFieldRef, String> {
    let field_id = arrow_field_id(field)?;
    let field_type = iceberg_type_from_arrow_type(field.data_type())?;
    Ok(Arc::new(
        novarocks_connector_iceberg::iceberg::spec::NestedField::new(
            field_id,
            field.name(),
            field_type,
            !field.is_nullable(),
        ),
    ))
}

fn arrow_field_id(field: &Field) -> Result<i32, String> {
    let raw = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .ok_or_else(|| {
            format!(
                "iceberg sink field {} is missing parquet field id metadata",
                field.name()
            )
        })?;
    raw.parse::<i32>().map_err(|e| {
        format!(
            "iceberg sink field {} has invalid parquet field id {raw}: {e}",
            field.name()
        )
    })
}

fn schema_has_reserved_row_lineage_columns(schema: &Schema) -> Result<bool, String> {
    let mut has_row_id = false;
    let mut has_last_updated = false;
    for field in schema.fields() {
        if field.name().eq_ignore_ascii_case(ICEBERG_ROW_ID_COL) {
            has_row_id = matches!(arrow_field_id(field), Ok(ICEBERG_RESERVED_FIELD_ID_ROW_ID));
        } else if field
            .name()
            .eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
        {
            has_last_updated = matches!(
                arrow_field_id(field),
                Ok(ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)
            );
        }
    }
    Ok(has_row_id && has_last_updated)
}

fn row_lineage_row_id_index(schema: &Schema) -> Result<usize, String> {
    for (idx, field) in schema.fields().iter().enumerate() {
        if field.name().eq_ignore_ascii_case(ICEBERG_ROW_ID_COL)
            && arrow_field_id(field)? == ICEBERG_RESERVED_FIELD_ID_ROW_ID
        {
            return Ok(idx);
        }
    }
    Err("iceberg row-lineage sink missing reserved _row_id field".to_string())
}

fn iceberg_type_from_arrow_type(
    data_type: &DataType,
) -> Result<novarocks_connector_iceberg::iceberg::spec::Type, String> {
    use arrow::datatypes::TimeUnit;
    use novarocks_connector_iceberg::iceberg::spec::{
        ListType, MapType, PrimitiveType, StructType, Type,
    };

    let primitive = match data_type {
        DataType::Boolean => Some(PrimitiveType::Boolean),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Some(PrimitiveType::Int),
        DataType::Int64 => Some(PrimitiveType::Long),
        DataType::Float32 => Some(PrimitiveType::Float),
        DataType::Float64 => Some(PrimitiveType::Double),
        DataType::Decimal128(precision, scale) => Some(PrimitiveType::Decimal {
            precision: (*precision).into(),
            scale: u32::try_from(*scale)
                .map_err(|_| format!("iceberg sink decimal scale {scale} cannot convert to u32"))?,
        }),
        DataType::Date32 => Some(PrimitiveType::Date),
        DataType::Time64(TimeUnit::Microsecond) => Some(PrimitiveType::Time),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Some(PrimitiveType::Timestamp),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Some(PrimitiveType::Timestamptz),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Some(PrimitiveType::TimestampNs),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => Some(PrimitiveType::TimestamptzNs),
        DataType::Utf8 | DataType::LargeUtf8 => Some(PrimitiveType::String),
        DataType::Binary => Some(PrimitiveType::Binary),
        DataType::LargeBinary => Some(PrimitiveType::Variant),
        DataType::FixedSizeBinary(size) => {
            Some(PrimitiveType::Fixed(u64::try_from(*size).map_err(
                |_| format!("iceberg sink fixed binary width {size} cannot convert to u64"),
            )?))
        }
        _ => None,
    };
    if let Some(primitive) = primitive {
        return Ok(Type::Primitive(primitive));
    }

    match data_type {
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|field| iceberg_nested_field_from_arrow_field(field.as_ref()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Type::Struct(StructType::new(fields)))
        }
        DataType::List(element) => Ok(Type::List(ListType::new(
            iceberg_nested_field_from_arrow_field(element.as_ref())?,
        ))),
        DataType::Map(entries, _sorted) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(format!(
                    "iceberg sink MAP entries field must be Struct, got {:?}",
                    entries.data_type()
                ));
            };
            if fields.len() != 2 {
                return Err(format!(
                    "iceberg sink MAP entries Struct must have 2 fields, got {}",
                    fields.len()
                ));
            }
            Ok(Type::Map(MapType::new(
                iceberg_nested_field_from_arrow_field(fields[0].as_ref())?,
                iceberg_nested_field_from_arrow_field(fields[1].as_ref())?,
            )))
        }
        other => Err(format!(
            "unsupported arrow type for staged iceberg sink writer schema: {other:?}"
        )),
    }
}

fn build_staged_partition_spec(
    schema: &novarocks_connector_iceberg::iceberg::spec::Schema,
    partition_spec_id: i32,
    source_column_names: &[String],
    partition_column_names: &[String],
    transform_exprs: &[String],
) -> Result<novarocks_connector_iceberg::iceberg::spec::UnboundPartitionSpec, String> {
    if source_column_names.len() != partition_column_names.len()
        || source_column_names.len() != transform_exprs.len()
    {
        return Err(format!(
            "iceberg sink partition spec metadata mismatch: sources={} names={} transforms={}",
            source_column_names.len(),
            partition_column_names.len(),
            transform_exprs.len()
        ));
    }

    let mut builder = novarocks_connector_iceberg::iceberg::spec::UnboundPartitionSpec::builder()
        .with_spec_id(partition_spec_id);
    for ((source_name, partition_name), transform_expr) in source_column_names
        .iter()
        .zip(partition_column_names.iter())
        .zip(transform_exprs.iter())
    {
        let field = schema
            .field_by_name_case_insensitive(source_name)
            .ok_or_else(|| {
                format!("iceberg sink partition source column {source_name} missing from schema")
            })?;
        builder = builder
            .add_partition_field(
                field.id,
                partition_name,
                parse_staged_partition_transform(transform_expr)?,
            )
            .map_err(|e| format!("build staged iceberg partition field failed: {e}"))?;
    }
    Ok(builder.build())
}

fn parse_staged_partition_transform(
    raw: &str,
) -> Result<novarocks_connector_iceberg::iceberg::spec::Transform, String> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "identity" => Ok(novarocks_connector_iceberg::iceberg::spec::Transform::Identity),
        "year" => Ok(novarocks_connector_iceberg::iceberg::spec::Transform::Year),
        "month" => Ok(novarocks_connector_iceberg::iceberg::spec::Transform::Month),
        "day" => Ok(novarocks_connector_iceberg::iceberg::spec::Transform::Day),
        "hour" => Ok(novarocks_connector_iceberg::iceberg::spec::Transform::Hour),
        "void" => Ok(novarocks_connector_iceberg::iceberg::spec::Transform::Void),
        _ => {
            if let Some(width) = parse_transform_arg(&normalized, "bucket")? {
                return Ok(novarocks_connector_iceberg::iceberg::spec::Transform::Bucket(width));
            }
            if let Some(width) = parse_transform_arg(&normalized, "truncate")? {
                return Ok(novarocks_connector_iceberg::iceberg::spec::Transform::Truncate(width));
            }
            Err(format!(
                "unsupported iceberg partition transform for staged sink writer: {raw}"
            ))
        }
    }
}

fn parse_transform_arg(raw: &str, name: &str) -> Result<Option<u32>, String> {
    let Some(rest) = raw.strip_prefix(name) else {
        return Ok(None);
    };
    let Some(rest) = rest.strip_prefix('[').and_then(|s| s.strip_suffix(']')) else {
        return Err(format!(
            "iceberg partition transform {raw} must use {name}[N] syntax"
        ));
    };
    let value = rest.parse::<u32>().map_err(|e| {
        format!("iceberg partition transform {raw} has invalid numeric argument: {e}")
    })?;
    if value == 0 {
        return Err(format!(
            "iceberg partition transform {raw} requires a positive numeric argument"
        ));
    }
    Ok(Some(value))
}

pub(crate) fn build_staged_file_io(
    data_location: &str,
    s3_config: Option<&IcebergSinkObjectStoreConfig>,
) -> Result<novarocks_connector_iceberg::iceberg::io::FileIO, String> {
    if novarocks_fs::is_object_store_location_parse_only(data_location)
        .map_err(|e| format!("parse staged iceberg data_location {data_location}: {e}"))?
    {
        let s3 = s3_config.ok_or_else(|| {
            format!(
                "iceberg sink missing S3 config for staged writer data_location={data_location}"
            )
        })?;
        let object_store_config = s3.to_object_store_config();
        return Ok(
            crate::connector::iceberg::fs_io::build_file_io_for_location(
                data_location,
                Some(&object_store_config),
            ),
        );
    }
    Ok(crate::connector::iceberg::fs_io::build_file_io_for_location(data_location, None))
}

fn normalize_path(path: &str) -> Result<String, String> {
    if path.starts_with("file:") {
        let url = url::Url::parse(path).map_err(|e| format!("invalid file url: {e}"))?;
        let p = url
            .to_file_path()
            .map_err(|_| "file url is not a valid local path".to_string())?;
        return Ok(p.to_string_lossy().to_string());
    }
    Ok(path.to_string())
}

pub(crate) fn write_parquet_file(
    path: &str,
    s3_config: Option<&IcebergSinkObjectStoreConfig>,
    schema: SchemaRef,
    batch: &RecordBatch,
    compression: Compression,
) -> Result<ParquetWriteResult, String> {
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    if novarocks_fs::is_object_store_location_parse_only(path)
        .map_err(|e| format!("parse iceberg parquet output path {path}: {e}"))?
    {
        let (data, write_result) = write_parquet_to_bytes(schema, batch, props)?;
        let s3 = s3_config.ok_or_else(|| {
            format!(
                "iceberg sink missing S3 config for object-store path={path}; \
                expected sink cloud_configuration to provide credentials"
            )
        })?;
        let object_store_cfg = s3.to_object_store_config();
        let access = crate::connector::iceberg::fs_io::resolve_access_for_location(
            path,
            Some(&object_store_cfg),
        )
        .map_err(|e| format!("resolve Iceberg parquet output {path}: {e}"))?;
        let rel = access
            .single_relative_path()
            .map_err(|e| format!("resolve Iceberg parquet output path {path}: {e}"))?;
        data_block_on(access.operator().write(rel, data))
            .map_err(|e| format!("run object-store write on data runtime failed: {e}"))?
            .map_err(|e| format!("opendal write failed: {e}"))?;
        return Ok(write_result);
    }

    // Local filesystem write: the path reported back to FE keeps its URI scheme
    // (e.g. "file:///tmp/..."), but ::fs APIs need a bare posix path.
    let local_path = normalize_path(path)?;
    let path_buf = PathBuf::from(&local_path);
    if let Some(parent) = path_buf.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create parquet dir failed: {e}"))?;
    }
    let file =
        fs::File::create(&path_buf).map_err(|e| format!("create parquet file failed: {e}"))?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| format!("create parquet writer failed: {e}"))?;
    writer
        .write(batch)
        .map_err(|e| format!("write parquet batch failed: {e}"))?;
    let parquet_metadata = writer
        .close()
        .map_err(|e| format!("close parquet writer failed: {e}"))?;
    let meta = fs::metadata(&path_buf).map_err(|e| format!("stat parquet file failed: {e}"))?;
    Ok(build_parquet_write_result(
        meta.len(),
        &parquet_metadata,
        Some(batch),
    ))
}

fn write_parquet_to_bytes(
    schema: SchemaRef,
    batch: &RecordBatch,
    props: WriterProperties,
) -> Result<(Vec<u8>, ParquetWriteResult), String> {
    let mut buffer = Vec::new();
    let parquet_metadata;
    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = ArrowWriter::try_new(cursor, schema, Some(props))
            .map_err(|e| format!("create parquet writer failed: {e}"))?;
        writer
            .write(batch)
            .map_err(|e| format!("write parquet batch failed: {e}"))?;
        parquet_metadata = writer
            .close()
            .map_err(|e| format!("close parquet writer failed: {e}"))?;
    }
    let write_result =
        build_parquet_write_result(buffer.len() as u64, &parquet_metadata, Some(batch));
    Ok((buffer, write_result))
}

fn build_parquet_write_result(
    file_size: u64,
    metadata: &ParquetMetaData,
    batch: Option<&RecordBatch>,
) -> ParquetWriteResult {
    ParquetWriteResult {
        file_size,
        split_offsets: collect_split_offsets(metadata),
        column_stats: collect_iceberg_column_stats(metadata),
        theta_sketches: batch.and_then(collect_theta_sketches),
    }
}

fn collect_split_offsets(metadata: &ParquetMetaData) -> Option<Vec<i64>> {
    let mut offsets = Vec::with_capacity(metadata.row_groups().len());
    for row_group in metadata.row_groups() {
        if row_group.num_columns() == 0 {
            continue;
        }
        let first_column = row_group.column(0);
        let data_page_offset = first_column.data_page_offset();
        let split_offset = match first_column.dictionary_page_offset() {
            Some(dictionary_page_offset)
                if dictionary_page_offset > 0 && dictionary_page_offset < data_page_offset =>
            {
                dictionary_page_offset
            }
            _ => data_page_offset,
        };
        offsets.push(split_offset);
    }
    (!offsets.is_empty()).then_some(offsets)
}

fn collect_iceberg_column_stats(metadata: &ParquetMetaData) -> Option<IcebergColumnStats> {
    let mut accumulators: BTreeMap<i32, ColumnStatsAccumulator> = BTreeMap::new();

    for row_group in metadata.row_groups() {
        for column in row_group.columns() {
            let basic_info = column.column_descr().self_type().get_basic_info();
            if !basic_info.has_id() {
                continue;
            }
            let field_id = basic_info.id();
            let acc = accumulators.entry(field_id).or_default();
            acc.column_size += column.compressed_size();

            let Some(stats) = column.statistics() else {
                continue;
            };
            acc.has_statistics = true;
            acc.value_count += column.num_values();
            if let Some(null_count) = stats.null_count_opt() {
                acc.null_value_count += null_count as i64;
            }
            if let Some(merged) = acc.merged_statistics.as_mut() {
                merge_statistics(merged, stats);
            } else {
                acc.merged_statistics = Some(stats.clone());
            }
        }
    }

    if accumulators.is_empty() {
        return None;
    }

    let mut column_sizes = BTreeMap::new();
    let mut value_counts = BTreeMap::new();
    let mut null_value_counts = BTreeMap::new();
    let mut lower_bounds = BTreeMap::new();
    let mut upper_bounds = BTreeMap::new();

    for (field_id, acc) in accumulators {
        column_sizes.insert(field_id, acc.column_size);
        if !acc.has_statistics {
            continue;
        }

        value_counts.insert(field_id, acc.value_count);
        null_value_counts.insert(field_id, acc.null_value_count);

        if let Some(stats) = acc.merged_statistics.as_ref() {
            if let Some(min) = stats.min_bytes_opt() {
                lower_bounds.insert(field_id, min.to_vec());
            }
            if let Some(max) = stats.max_bytes_opt() {
                upper_bounds.insert(field_id, max.to_vec());
            }
        }
    }

    Some(IcebergColumnStats {
        column_sizes,
        value_counts,
        null_value_counts,
        nan_value_counts: BTreeMap::new(),
        lower_bounds,
        upper_bounds,
    })
}

/// Compute a Theta sketch per primitive column from the input RecordBatch.
///
/// Each Arrow `Field` whose metadata carries `PARQUET_FIELD_ID_META_KEY`
/// becomes a `field_id → ThetaSketchHandle` entry; columns without a field
/// id, or whose type is non-primitive (struct/list/map/binary), are skipped.
///
/// The hash is computed over the canonical byte representation of the value
/// (little-endian for integers/floats, raw UTF-8 bytes for strings,
/// IEEE 754 bytes for floats with NaN normalization). Nulls are not pushed
/// into the sketch.
///
/// Returns `None` when no primitive column with a parquet field id is
/// found — there is nothing to write into a Puffin blob in that case.
///
/// Public wrapper used by the standalone iceberg_writer path; the
/// pipeline-driven sink calls this through the internal Parquet write
/// helper.
pub(crate) fn compute_theta_sketches_for_batch(
    batch: &RecordBatch,
) -> Option<HashMap<i32, novarocks_connector_iceberg::theta_sketch::ThetaSketchHandle>> {
    collect_theta_sketches(batch)
}

/// Feed every non-null value of `array` into `sketch`, dispatching by Arrow
/// type. Returns true if at least one value was fed. NaN floats are collapsed
/// to a single canonical bit pattern so independent NaN encodings count once.
/// Shared by `collect_theta_sketches` (write path, field-id from Arrow
/// metadata) and `collect_theta_sketches_by_name` (ANALYZE path, field-id from
/// an explicit name map). Unsupported/complex types feed nothing -> false.
fn feed_array_into_sketch(
    sketch: &mut novarocks_connector_iceberg::theta_sketch::ThetaSketchHandle,
    data_type: &DataType,
    array: &arrow::array::ArrayRef,
) -> bool {
    use arrow::array::{
        BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow::datatypes::TimeUnit;
    let mut updated = false;
    macro_rules! feed_int {
        ($ty:ty) => {{
            if let Some(arr) = array.as_any().downcast_ref::<$ty>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        sketch.update(arr.value(i));
                        updated = true;
                    }
                }
            }
        }};
    }
    match data_type {
        DataType::Boolean => {
            if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let v: u8 = if arr.value(i) { 1 } else { 0 };
                        sketch.update(v);
                        updated = true;
                    }
                }
            }
        }
        DataType::Int8 => feed_int!(Int8Array),
        DataType::Int16 => feed_int!(Int16Array),
        DataType::Int32 => feed_int!(Int32Array),
        DataType::Int64 => feed_int!(Int64Array),
        DataType::Date32 => feed_int!(Date32Array),
        DataType::Date64 => feed_int!(Date64Array),
        DataType::Float32 => {
            if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let v = arr.value(i);
                        let bits = if v.is_nan() {
                            f32::NAN.to_bits()
                        } else {
                            v.to_bits()
                        };
                        sketch.update(bits);
                        updated = true;
                    }
                }
            }
        }
        DataType::Float64 => {
            if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let v = arr.value(i);
                        let bits = if v.is_nan() {
                            f64::NAN.to_bits()
                        } else {
                            v.to_bits()
                        };
                        sketch.update(bits);
                        updated = true;
                    }
                }
            }
        }
        DataType::Decimal128(_, _) => {
            if let Some(arr) = array.as_any().downcast_ref::<Decimal128Array>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        sketch.update(arr.value(i));
                        updated = true;
                    }
                }
            }
        }
        DataType::Utf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        sketch.update(arr.value(i));
                        updated = true;
                    }
                }
            }
        }
        DataType::LargeUtf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        sketch.update(arr.value(i));
                        updated = true;
                    }
                }
            }
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => feed_int!(TimestampSecondArray),
            TimeUnit::Millisecond => feed_int!(TimestampMillisecondArray),
            TimeUnit::Microsecond => feed_int!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => feed_int!(TimestampNanosecondArray),
        },
        _ => {}
    }
    updated
}

fn collect_theta_sketches(
    batch: &RecordBatch,
) -> Option<HashMap<i32, novarocks_connector_iceberg::theta_sketch::ThetaSketchHandle>> {
    use novarocks_connector_iceberg::theta_sketch::ThetaSketchHandle;

    // Apache DataSketches Java/Spark default lg_k = 12 (k = 4096, ~1.5% error)
    // matches the spec. Kept hard-coded here; the table property override is
    // a follow-up wired through the sink plan.
    const LG_K: u8 = 12;

    let schema = batch.schema();
    let mut sketches: HashMap<i32, ThetaSketchHandle> = HashMap::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let Some(field_id_str) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
            continue;
        };
        let Ok(field_id) = field_id_str.parse::<i32>() else {
            continue;
        };
        let array = batch.column(col_idx);
        let mut sketch = ThetaSketchHandle::new(LG_K);
        let updated = feed_array_into_sketch(&mut sketch, field.data_type(), array);
        if updated {
            sketches.insert(field_id, sketch);
        }
    }

    if sketches.is_empty() {
        None
    } else {
        Some(sketches)
    }
}

/// Build per-field Theta sketches from a `RecordBatch` whose columns carry no
/// iceberg field-id metadata (e.g. an `execute_query` scan result), using an
/// explicit lowercased-column-name -> field_id map. Columns absent from the map,
/// or of unsupported type, are skipped. Sketches accumulate per call; union
/// across batches via `ThetaSketchHandle::union` at the call site.
pub(crate) fn collect_theta_sketches_by_name(
    batch: &RecordBatch,
    name_to_field_id: &HashMap<String, i32>,
) -> HashMap<i32, novarocks_connector_iceberg::theta_sketch::ThetaSketchHandle> {
    const LG_K: u8 = 12;
    let schema = batch.schema();
    let mut sketches = HashMap::new();
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let Some(&field_id) = name_to_field_id.get(&field.name().to_lowercase()) else {
            continue;
        };
        let mut sketch = novarocks_connector_iceberg::theta_sketch::ThetaSketchHandle::new(LG_K);
        if feed_array_into_sketch(&mut sketch, field.data_type(), batch.column(col_idx)) {
            sketches.insert(field_id, sketch);
        }
    }
    sketches
}

fn merge_statistics(current: &mut Statistics, next: &Statistics) {
    match (current, next) {
        (Statistics::Boolean(cur), Statistics::Boolean(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::Int32(cur), Statistics::Int32(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::Int64(cur), Statistics::Int64(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::Int96(cur), Statistics::Int96(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::Float(cur), Statistics::Float(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::Double(cur), Statistics::Double(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::ByteArray(cur), Statistics::ByteArray(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        (Statistics::FixedLenByteArray(cur), Statistics::FixedLenByteArray(nxt)) => {
            *cur = merge_typed_statistics(cur, nxt, PartialOrd::partial_cmp);
        }
        _ => {}
    }
}

fn merge_typed_statistics<T, F>(
    current: &ValueStatistics<T>,
    next: &ValueStatistics<T>,
    compare: F,
) -> ValueStatistics<T>
where
    T: Clone + AsBytes,
    F: Fn(&T, &T) -> Option<Ordering>,
{
    let min = choose_min(current.min_opt(), next.min_opt(), &compare);
    let max = choose_max(current.max_opt(), next.max_opt(), &compare);
    let null_count =
        Some(current.null_count_opt().unwrap_or(0) + next.null_count_opt().unwrap_or(0));
    let min_is_exact = match (current.min_opt(), next.min_opt()) {
        (Some(_), Some(_)) => current.min_is_exact() && next.min_is_exact(),
        (Some(_), None) => current.min_is_exact(),
        (None, Some(_)) => next.min_is_exact(),
        (None, None) => false,
    };
    let max_is_exact = match (current.max_opt(), next.max_opt()) {
        (Some(_), Some(_)) => current.max_is_exact() && next.max_is_exact(),
        (Some(_), None) => current.max_is_exact(),
        (None, Some(_)) => next.max_is_exact(),
        (None, None) => false,
    };

    ValueStatistics::new(min, max, None, null_count, false)
        .with_backwards_compatible_min_max(
            current.is_min_max_backwards_compatible() && next.is_min_max_backwards_compatible(),
        )
        .with_min_is_exact(min_is_exact)
        .with_max_is_exact(max_is_exact)
}

fn choose_min<T, F>(left: Option<&T>, right: Option<&T>, compare: &F) -> Option<T>
where
    T: Clone,
    F: Fn(&T, &T) -> Option<Ordering>,
{
    match (left, right) {
        (Some(left), Some(right)) => match compare(left, right) {
            Some(Ordering::Greater) => Some(right.clone()),
            _ => Some(left.clone()),
        },
        (Some(left), None) => Some(left.clone()),
        (None, Some(right)) => Some(right.clone()),
        (None, None) => None,
    }
}

fn choose_max<T, F>(left: Option<&T>, right: Option<&T>, compare: &F) -> Option<T>
where
    T: Clone,
    F: Fn(&T, &T) -> Option<Ordering>,
{
    match (left, right) {
        (Some(left), Some(right)) => match compare(left, right) {
            Some(Ordering::Less) => Some(right.clone()),
            _ => Some(left.clone()),
        },
        (Some(left), None) => Some(left.clone()),
        (None, Some(right)) => Some(right.clone()),
        (None, None) => None,
    }
}

fn iceberg_partition_key_for_row(
    partition_column_names: &[String],
    transform_exprs: &[String],
    partition_arrays: &[ArrayRef],
    row: usize,
) -> Result<
    (
        String,
        String,
        novarocks_connector_iceberg::iceberg::spec::Struct,
    ),
    String,
> {
    if partition_column_names.len() != transform_exprs.len()
        || partition_arrays.len() != partition_column_names.len()
    {
        return Err("partition arrays mismatch for iceberg sink".to_string());
    }
    let mut path = String::new();
    let mut nulls = String::with_capacity(partition_column_names.len());
    let mut partition_values = Vec::with_capacity(partition_column_names.len());
    for i in 0..partition_column_names.len() {
        let transform = transform_exprs[i].to_lowercase();
        let base = transform.split('[').next().unwrap_or(transform.as_str());
        let is_null = base == "void" || partition_arrays[i].is_null(row);
        let value = iceberg_partition_value(base, &partition_arrays[i], row)?;
        let literal = if is_null {
            None
        } else {
            Some(iceberg_partition_literal(base, &partition_arrays[i], row)?)
        };
        nulls.push(if is_null { '1' } else { '0' });
        partition_values.push(literal);
        path.push_str(&partition_column_names[i]);
        path.push('=');
        path.push_str(&value);
        path.push('/');
    }
    Ok((
        path,
        nulls,
        novarocks_connector_iceberg::iceberg::spec::Struct::from_iter(partition_values),
    ))
}

fn iceberg_partition_literal(
    transform: &str,
    array: &ArrayRef,
    row: usize,
) -> Result<novarocks_connector_iceberg::iceberg::spec::Literal, String> {
    match transform {
        "year" | "month" | "hour" | "bucket" => {
            let value = array_value_as_i64(array, row)?;
            let value = i32::try_from(value)
                .map_err(|_| format!("{transform} transform value out of INT range"))?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::int(
                value,
            ))
        }
        "day" => {
            let value = array_value_as_i64(array, row)?;
            let days =
                i32::try_from(value).map_err(|_| "day transform value out of range".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::date(
                days,
            ))
        }
        "truncate" | "identity" => column_literal(array, row),
        other => Err(format!("unsupported iceberg partition transform: {other}")),
    }
}

fn iceberg_partition_value(
    transform: &str,
    array: &ArrayRef,
    row: usize,
) -> Result<String, String> {
    if array.is_null(row) || transform == "void" {
        return Ok("null".to_string());
    }
    match transform {
        "year" => {
            let value = array_value_as_i64(array, row)?;
            Ok((value + 1970).to_string())
        }
        "month" => {
            let value = array_value_as_i64(array, row)?;
            let year = 1970 + (value / 12);
            let month = value % 12 + 1;
            Ok(format!("{:04}-{:02}", year, month))
        }
        "day" => {
            let value = array_value_as_i64(array, row)?;
            let days =
                i32::try_from(value).map_err(|_| "day transform value out of range".to_string())?;
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(719_163 + days)
                .ok_or_else(|| "invalid day transform value".to_string())?;
            Ok(date.format("%Y-%m-%d").to_string())
        }
        "hour" => {
            let value = array_value_as_i64(array, row)?;
            let seconds = value * 3600;
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0)
                .ok_or_else(|| "invalid hour transform value".to_string())?
                .naive_utc();
            Ok(dt.format("%Y-%m-%d-%H").to_string())
        }
        "truncate" | "bucket" | "identity" => column_value(array, row),
        other => Err(format!("unsupported iceberg partition transform: {other}")),
    }
}

fn array_value_as_i64(array: &ArrayRef, row: usize) -> Result<i64, String> {
    match array.data_type() {
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "expected INT array".to_string())?;
            Ok(i64::from(arr.value(row)))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected BIGINT array".to_string())?;
            Ok(arr.value(row))
        }
        other => Err(format!(
            "iceberg partition transform expects INT/BIGINT, got {other:?}"
        )),
    }
}

fn column_value(array: &ArrayRef, row: usize) -> Result<String, String> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| "expected BOOLEAN array".to_string())?;
            Ok(if arr.value(row) { "true" } else { "false" }.to_string())
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "expected TINYINT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "expected SMALLINT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "expected INT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected BIGINT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| "expected DATE array".to_string())?;
            let days = arr.value(row);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(719_163 + days)
                .ok_or_else(|| "invalid Date32 value".to_string())?;
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "expected DATETIME array".to_string())?;
            let micros = arr.value(row);
            let secs = micros.div_euclid(1_000_000);
            let rem = micros.rem_euclid(1_000_000);
            let nanos = (rem as u32) * 1000;
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
                .ok_or_else(|| "invalid DATETIME value".to_string())?
                .naive_utc();
            Ok(url_encode(&format_datetime(dt)))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "expected VARCHAR array".to_string())?;
            Ok(url_encode(arr.value(row)))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "expected BINARY array".to_string())?;
            let bytes = arr.value(row);
            let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
            Ok(url_encode(&encoded))
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "expected DECIMAL array".to_string())?;
            Ok(arr.value_as_string(row))
        }
        other => Err(format!(
            "unsupported iceberg partition column type: {other:?}"
        )),
    }
}

fn column_literal(
    array: &ArrayRef,
    row: usize,
) -> Result<novarocks_connector_iceberg::iceberg::spec::Literal, String> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| "expected BOOLEAN array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::bool(
                arr.value(row),
            ))
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "expected TINYINT array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::int(
                i32::from(arr.value(row)),
            ))
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "expected SMALLINT array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::int(
                i32::from(arr.value(row)),
            ))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "expected INT array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::int(
                arr.value(row),
            ))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected BIGINT array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::long(
                arr.value(row),
            ))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| "expected FLOAT array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::float(
                arr.value(row),
            ))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| "expected DOUBLE array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::double(
                arr.value(row),
            ))
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| "expected DATE array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::date(
                arr.value(row),
            ))
        }
        DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Time64MicrosecondArray>()
                .ok_or_else(|| "expected TIME array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::time(
                arr.value(row),
            ))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "expected DATETIME array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::timestamp(arr.value(row)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(_)) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "expected TIMESTAMP array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::timestamptz(arr.value(row)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .ok_or_else(|| "expected TIMESTAMP_NS array".to_string())?;
            Ok(
                novarocks_connector_iceberg::iceberg::spec::Literal::Primitive(
                    novarocks_connector_iceberg::iceberg::spec::PrimitiveLiteral::Long(
                        arr.value(row),
                    ),
                ),
            )
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "expected VARCHAR array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::string(
                arr.value(row),
            ))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "expected BINARY array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::binary(
                arr.value(row).iter().copied(),
            ))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                .ok_or_else(|| "expected FIXED array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::fixed(
                arr.value(row).iter().copied(),
            ))
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "expected DECIMAL array".to_string())?;
            Ok(novarocks_connector_iceberg::iceberg::spec::Literal::decimal(arr.value(row)))
        }
        other => Err(format!(
            "unsupported iceberg partition column type: {other:?}"
        )),
    }
}

fn url_encode(input: &str) -> String {
    url::form_urlencoded::byte_serialize(input.as_bytes()).collect()
}

fn format_datetime(dt: chrono::NaiveDateTime) -> String {
    let micros = dt.and_utc().timestamp_subsec_micros();
    if micros == 0 {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
    }
}
