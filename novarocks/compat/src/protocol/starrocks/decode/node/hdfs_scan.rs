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
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{DataType, Field};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::protocol::starrocks::decode::descriptor::descriptor_snapshot_from_thrift;
use crate::protocol::starrocks::decode::layout::{Layout, layout_from_slot_ids};
use crate::protocol::starrocks::decode::node::decode::QueryGlobalDictMap;
use crate::protocol::starrocks::decode::node::{Lowered, ScanRangeCarrier};
use crate::thrift::{descriptors, exprs, plan_nodes, types};
use novarocks::cache::{CacheOptions, ExternalDataCacheRangeOptions};
use novarocks::common::ids::SlotId;
use novarocks::common::min_max_predicate::MinMaxPredicate;
use novarocks::connector::iceberg::delete_file::{
    IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat,
};
use novarocks::connector::iceberg::scan_model::{
    IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
};
use novarocks::connector::iceberg::{COMPAT_ICEBERG_INSTANCE_ID, build_compat_read_splits};
use novarocks::connector::iceberg::{
    IcebergArrowColumn, IcebergMetadataOutputColumn, IcebergMetadataScanConfig,
    IcebergMetadataScanRange, IcebergMetadataTableType, plan_compat_iceberg_metadata_read_source,
};
use novarocks::connector::runtime::{ConnectorReadScanSource, ConnectorScheduledSplit};
use novarocks::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks::exec::fragment::program::ScanAssignmentKind;
use novarocks::exec::node::scan::{
    BoundScanRanges, ConnectorRowPosition, ConnectorRowPositionLookup, ScanNode,
};
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::exec::row_position::RowPositionSpec;
use novarocks::formats::parquet::{ParquetSlotKind, VariantPathPruningPredicate, VariantPathSpec};
use novarocks::novarocks_connectors::ConnectorRegistry;
use novarocks::novarocks_logging::{debug, warn};
use novarocks::runtime::descriptor_snapshot::{
    DescriptorLogicalType, DescriptorSlot, DescriptorSnapshot, IcebergTableLocationMap,
};
use novarocks::runtime::query_options::{QueryOptions, query_expire_durations};
use novarocks::runtime::scan_range::{FileFormat as RuntimeFileFormat, ScanRange};
use novarocks::runtime::starrocks_fragment_query::StarRocksFragmentQueryRuntime;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorCancellation, ConnectorInstanceId, ConnectorOpenReaderRequest,
    ConnectorRequestContext, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use novarocks_types::QueryId;

fn compat_iceberg_file_from_wire(
    node_id: i32,
    path: String,
    file_len: u64,
    offset: u64,
    length: u64,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    ivm_change_op: Option<i8>,
    included_positions: Option<Vec<i64>>,
    delete_files: Vec<IcebergDeleteFileSpec>,
) -> Result<IcebergDataFileInfo, String> {
    if offset != 0 || (length != 0 && length != file_len) {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={node_id} Iceberg compatibility scan range must cover one complete data file"
        ));
    }
    let delete_files = delete_files
        .into_iter()
        .map(|delete| {
            Ok(IcebergDeleteFileInfo {
                path: delete.path,
                file_format: match delete.file_format {
                    IcebergFileFormat::Parquet => IcebergDeleteFileFormat::Parquet,
                    IcebergFileFormat::Puffin => IcebergDeleteFileFormat::Puffin,
                    IcebergFileFormat::Unknown => {
                        return Err("Iceberg delete file has unknown format".to_string());
                    }
                },
                file_content: match delete.file_content {
                    IcebergFileContent::PositionDeletes => IcebergDeleteFileContent::Position,
                    IcebergFileContent::EqualityDeletes => IcebergDeleteFileContent::Equality,
                    IcebergFileContent::Data => {
                        return Err("Iceberg delete attachment has data-file content".to_string());
                    }
                },
                length: delete.length.and_then(|value| i64::try_from(value).ok()),
                content_offset: delete.content_offset,
                content_size_in_bytes: delete.content_size_in_bytes,
                sequence_number: None,
                partition_spec_id: None,
                partition_key: None,
                equality_column_names: Vec::new(),
                equality_field_ids: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(IcebergDataFileInfo {
        path,
        size: i64::try_from(file_len).map_err(|_| "Iceberg data file length exceeds Int64")?,
        row_count: None,
        column_stats: None,
        partition_spec_id: None,
        partition_key: None,
        first_row_id,
        data_sequence_number,
        ivm_change_op,
        included_positions,
        delete_files,
        manifest_path: None,
        partition_values: Vec::new(),
    })
}

struct CompatIcebergDataReadPlan {
    source: Arc<dyn novarocks::exec::node::scan::ScanSource>,
    row_position_lookup: Option<ConnectorRowPositionLookup>,
}

fn provider_schema_for_compat_iceberg(
    output_schema: &ChunkSchemaRef,
    row_position: Option<&RowPositionSpec>,
) -> Result<ChunkSchemaRef, String> {
    let Some(row_position) = row_position else {
        return Ok(Arc::clone(output_schema));
    };
    let slots = output_schema
        .slots()
        .iter()
        .filter(|slot| {
            slot.slot_id() != row_position.row_source_slot
                && slot.slot_id() != row_position.scan_range_slot
        })
        .cloned()
        .collect();
    Ok(Arc::new(ChunkSchema::try_new(slots)?))
}

fn plan_compat_iceberg_data_read_source(
    iceberg_execution: Arc<novarocks_spi::connector::ConnectorExecutionBinding>,
    query_id: QueryId,
    output_schema: ChunkSchemaRef,
    files: Vec<IcebergDataFileInfo>,
    row_position_ids: Option<Vec<i32>>,
    row_position: Option<&RowPositionSpec>,
    query_options: &QueryOptions,
) -> Result<CompatIcebergDataReadPlan, String> {
    let rows = query_options
        .batch_size()
        .and_then(|value| usize::try_from(value).ok())
        .and_then(NonZeroUsize::new)
        .unwrap_or_else(|| NonZeroUsize::new(4096).expect("default batch size is nonzero"));
    let (_, query_expire) = query_expire_durations(Some(query_options));
    let context = ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        StarRocksFragmentQueryRuntime::new().connector_cancellation(query_id),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())?;
    let splits = build_compat_read_splits(files).map_err(|error| error.to_string())?;
    let provider_schema = provider_schema_for_compat_iceberg(&output_schema, row_position)?;
    let row_position_lookup = match row_position_ids {
        Some(ids) => {
            if ids.len() != splits.len() {
                return Err("compat Iceberg row-position split count mismatch".to_string());
            }
            Some(ConnectorRowPositionLookup::new_execution(
                Arc::clone(&iceberg_execution),
                ids.into_iter().zip(splits.iter().cloned()).collect(),
            ))
        }
        None => None,
    };
    let scheduled = splits
        .into_iter()
        .enumerate()
        .map(|(index, split)| {
            match row_position_lookup.as_ref().and_then(|lookup| {
                lookup.splits().find_map(|(range_id, candidate)| {
                    (candidate.split_id() == split.split_id()).then_some(*range_id)
                })
            }) {
                Some(scan_range_id) => ConnectorScheduledSplit::with_row_position(
                    split,
                    ConnectorRowPosition { scan_range_id },
                ),
                None => ConnectorScheduledSplit::plain(split),
            }
        })
        .collect();
    Ok(CompatIcebergDataReadPlan {
        source: Arc::new(ConnectorReadScanSource::new_scheduled_execution(
            iceberg_execution,
            scheduled,
            ConnectorOpenReaderRequest {
                expected_schema: provider_schema.arrow_schema_ref(),
                batch: ConnectorBatchBudget {
                    max_rows: rows,
                    max_bytes: NonZeroUsize::new(MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES)
                        .expect("SPI handle maximum is nonzero"),
                },
                context,
            },
            provider_schema,
        )),
        row_position_lookup,
    })
}

fn next_hidden_slot_id(visible_slot_ids: &[SlotId]) -> Result<SlotId, String> {
    let max_slot = visible_slot_ids
        .iter()
        .map(|slot_id| slot_id.as_u32())
        .max()
        .unwrap_or(0);
    let next = max_slot
        .checked_add(1)
        .ok_or_else(|| "cannot allocate hidden HDFS scan slot id".to_string())?;
    Ok(SlotId::new(next))
}

fn advance_hidden_slot_id(slot_id: SlotId) -> Result<SlotId, String> {
    slot_id
        .as_u32()
        .checked_add(1)
        .map(SlotId::new)
        .ok_or_else(|| "cannot allocate hidden HDFS scan slot id".to_string())
}

fn hdfs_scan_file_format_from_thrift(
    format: descriptors::THdfsFileFormat,
) -> novarocks::exec::node::scan::HdfsScanFileFormat {
    match format {
        descriptors::THdfsFileFormat::PARQUET => {
            novarocks::exec::node::scan::HdfsScanFileFormat::Parquet
        }
        descriptors::THdfsFileFormat::ORC => novarocks::exec::node::scan::HdfsScanFileFormat::Orc,
        _ => novarocks::exec::node::scan::HdfsScanFileFormat::Other,
    }
}

fn iceberg_reserved_field(name: &str, nullable: bool, field_id: i32) -> Field {
    Field::new(name, DataType::Int64, nullable).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        field_id.to_string(),
    )]))
}

fn file_cache_flags_from_query_options(query_opts: &QueryOptions) -> (bool, bool) {
    // Align with StarRocks BE semantics: cache flags are only effective when
    // FE explicitly carries the corresponding query option.
    let enable_file_metacache = query_opts.enable_file_metacache();
    let enable_file_pagecache = query_opts.enable_file_pagecache();
    (enable_file_metacache, enable_file_pagecache)
}

/// Extract an `ObjectStoreConfig` from the cloud properties map attached to
/// `THdfsScanNode.cloud_configuration`. Returns `Ok(None)` when any required
/// field is absent so the caller falls back to the shard registry (used by
/// native lake tablets).
fn resolve_cloud_object_store_config<S>(
    cloud_props: Option<&std::collections::BTreeMap<S, S>>,
    decode_facts: &crate::protocol::starrocks::decode::instance::StarRocksDecodeFacts,
) -> Result<Option<novarocks_fs::ObjectStoreConfig>, String>
where
    S: std::borrow::Borrow<str> + Ord,
{
    let Some(props) = cloud_props else {
        return Ok(None);
    };
    let credentials =
        novarocks::fs::object_store_credentials::ObjectStoreCredentials::optional_from_aws_s3_properties(
            novarocks::fs::object_store_credentials::ObjectStoreCredentialsSource::AwsS3Properties,
            props,
        )?;
    let Some(credentials) = credentials else {
        return Ok(None);
    };

    let mut cfg = credentials.to_object_store_config();
    decode_facts.object_store_defaults().apply_to(&mut cfg);
    Ok(Some(cfg))
}

#[derive(Clone, Debug)]
struct HdfsSlotInfo {
    name: String,
    logical: DescriptorLogicalType,
    field: Field,
    arrow_type: arrow::datatypes::DataType,
    nullable: bool,
    field_id: Option<i32>,
}

impl HdfsSlotInfo {
    fn from_descriptor_slot(slot: &DescriptorSlot) -> Self {
        Self {
            name: slot.name.clone(),
            logical: slot.logical.clone(),
            field: slot.field.clone(),
            arrow_type: slot.field.data_type().clone(),
            nullable: slot.field.is_nullable(),
            field_id: slot.unique_id,
        }
    }
}

fn parquet_slot_kind_from_logical(logical: &DescriptorLogicalType) -> ParquetSlotKind {
    if logical.is_variant() {
        ParquetSlotKind::Variant
    } else {
        ParquetSlotKind::Regular
    }
}

fn build_hdfs_slot_info_map(
    snapshot: &DescriptorSnapshot,
    tuple_id: types::TTupleId,
) -> Result<HashMap<SlotId, HdfsSlotInfo>, String> {
    let mut slot_info_map: HashMap<SlotId, HdfsSlotInfo> = HashMap::new();
    for slot_id in snapshot.tuple_slots(tuple_id) {
        let slot = snapshot.slot(tuple_id, *slot_id).ok_or_else(|| {
            format!(
                "missing descriptor slot tuple_id={} slot_id={}",
                tuple_id, slot_id
            )
        })?;
        slot_info_map.insert(*slot_id, HdfsSlotInfo::from_descriptor_slot(slot));
    }
    Ok(slot_info_map)
}

fn col_names_from_snapshot_layout(
    layout: &Layout,
    slot_info_map: &HashMap<SlotId, HdfsSlotInfo>,
) -> Result<Vec<String>, String> {
    layout
        .order
        .iter()
        .map(|(_, raw_slot_id)| {
            let slot_id = SlotId::try_from(*raw_slot_id)?;
            let info = slot_info_map
                .get(&slot_id)
                .ok_or_else(|| format!("missing slot info for slot_id={slot_id}"))?;
            Ok(info.name.clone())
        })
        .collect()
}

fn chunk_schema_for_snapshot_layout(
    layout: &Layout,
    slot_info_map: &HashMap<SlotId, HdfsSlotInfo>,
) -> Result<ChunkSchemaRef, String> {
    let slots = layout
        .order
        .iter()
        .map(|(_, raw_slot_id)| {
            let slot_id = SlotId::try_from(*raw_slot_id)?;
            let info = slot_info_map
                .get(&slot_id)
                .ok_or_else(|| format!("missing slot info for slot_id={slot_id}"))?;
            ChunkSlotSchema::from_field(slot_id, &info.field, info.field_id)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(ChunkSchema::try_new(slots)?))
}

#[derive(Clone, Debug, Default)]
struct HdfsScanReadColumns {
    columns: Vec<String>,
    slot_ids: Vec<SlotId>,
    slot_kinds: Vec<ParquetSlotKind>,
    fields: Vec<Field>,
    iceberg_projected_columns: Vec<IcebergArrowColumn>,
}

impl HdfsScanReadColumns {
    fn push_physical(&mut self, read_slot_id: SlotId, info: &HdfsSlotInfo) {
        self.columns.push(info.name.clone());
        self.slot_ids.push(read_slot_id);
        self.slot_kinds
            .push(parquet_slot_kind_from_logical(&info.logical));
        self.fields.push(Field::new(
            info.name.clone(),
            info.arrow_type.clone(),
            info.nullable,
        ));
        self.iceberg_projected_columns.push(IcebergArrowColumn {
            name: info.name.clone(),
            data_type: info.arrow_type.clone(),
            nullable: info.nullable,
        });
    }
}

#[derive(Clone, Debug, Default)]
struct HdfsVariantPathPlan {
    specs: Vec<VariantPathSpec>,
    output_slot_ids: HashSet<SlotId>,
}

fn required_variant_path_string(
    node_id: i32,
    idx: usize,
    field_name: &str,
    value: Option<&String>,
) -> Result<String, String> {
    value
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| {
            format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] missing {field_name}"
            )
        })
}

fn validate_variant_path_column_path(
    node_id: i32,
    idx: usize,
    canonical_path: &str,
) -> Result<(), String> {
    let parsed = novarocks::exec::variant::parse_variant_path(canonical_path).map_err(|e| {
        format!(
            "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] invalid canonical_path={canonical_path:?}: {e}"
        )
    })?;
    if parsed.segments.is_empty() {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] canonical_path={canonical_path:?} must reference at least one object key"
        ));
    }
    if parsed.segments.iter().any(|segment| {
        !matches!(
            segment,
            novarocks::exec::variant::VariantPathSegment::ObjectKey(_)
        )
    }) {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] canonical_path={canonical_path:?} only supports object-key path segments"
        ));
    }
    Ok(())
}

fn is_supported_variant_path_requested_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean | DataType::Int64 | DataType::Float64 | DataType::Utf8 | DataType::Date32
    )
}

fn parse_hdfs_scan_variant_path_columns(
    node_id: i32,
    variant_path_columns: Option<&[plan_nodes::TVariantPathColumn]>,
    slot_info_map: &HashMap<SlotId, HdfsSlotInfo>,
) -> Result<HdfsVariantPathPlan, String> {
    let Some(variant_path_columns) = variant_path_columns else {
        return Ok(HdfsVariantPathPlan::default());
    };
    let mut plan = HdfsVariantPathPlan::default();
    for (idx, column) in variant_path_columns.iter().enumerate() {
        let source_slot_id = column.source_slot_id.ok_or_else(|| {
            format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] missing source_slot_id"
            )
        })?;
        let output_slot_id = column.output_slot_id.ok_or_else(|| {
            format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] missing output_slot_id"
            )
        })?;
        let source_slot_id = SlotId::try_from(source_slot_id).map_err(|e| {
            format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] invalid source_slot_id: {e}"
            )
        })?;
        let output_slot_id = SlotId::try_from(output_slot_id).map_err(|e| {
            format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] invalid output_slot_id: {e}"
            )
        })?;
        if source_slot_id == output_slot_id {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] source_slot_id must differ from output_slot_id"
            ));
        }
        let source_info = slot_info_map.get(&source_slot_id).ok_or_else(|| {
            format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] source_slot_id={source_slot_id} has no slot descriptor"
            )
        })?;
        let output_info = slot_info_map.get(&output_slot_id).ok_or_else(|| {
            format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] output_slot_id={output_slot_id} has no slot descriptor"
            )
        })?;
        if !source_info.logical.is_variant() {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] source_slot_id={source_slot_id} expects VARIANT, got {:?}",
                source_info.logical
            ));
        }
        let source_name = required_variant_path_string(
            node_id,
            idx,
            "source_column",
            column.source_column.as_ref(),
        )?;
        let output_name = required_variant_path_string(
            node_id,
            idx,
            "output_column",
            column.output_column.as_ref(),
        )?;
        let canonical_path = required_variant_path_string(
            node_id,
            idx,
            "canonical_path",
            column.canonical_path.as_ref(),
        )?;
        validate_variant_path_column_path(node_id, idx, &canonical_path)?;
        if source_name != source_info.name {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] source_column={source_name:?} does not match source_slot_id={source_slot_id} name {:?}",
                source_info.name
            ));
        }
        if output_name != output_info.name {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] output_column={output_name:?} does not match output_slot_id={output_slot_id} name {:?}",
                output_info.name
            ));
        }
        let requested_type_desc = column.requested_type.as_ref().ok_or_else(|| {
            format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] missing requested_type"
            )
        })?;
        let requested_type = crate::protocol::starrocks::decode::type_lowering::arrow_type_from_desc(requested_type_desc)
            .ok_or_else(|| {
                format!(
                    "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] unsupported requested_type for output_slot_id={output_slot_id}"
                )
            })?;
        if !is_supported_variant_path_requested_type(&requested_type) {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] unsupported requested_type {:?} for variant path output_slot_id={output_slot_id}",
                requested_type
            ));
        }
        if requested_type != output_info.arrow_type {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] requested_type {:?} does not match output_slot_id={output_slot_id} type {:?}",
                requested_type, output_info.arrow_type
            ));
        }
        let strict = column.strict.ok_or_else(|| {
            format!("HDFS_SCAN_NODE node_id={node_id} variant_path_columns[{idx}] missing strict")
        })?;
        if !plan.output_slot_ids.insert(output_slot_id) {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={node_id} duplicate variant_path_columns output_slot_id={output_slot_id}"
            ));
        }
        plan.specs.push(VariantPathSpec {
            source_slot_id,
            source_read_slot_id: source_slot_id,
            output_slot_id,
            source_field_id: source_info.field_id,
            source_name,
            output_name,
            source_field: Field::new(
                source_info.name.clone(),
                source_info.arrow_type.clone(),
                source_info.nullable,
            ),
            output_field: Field::new(
                output_info.name.clone(),
                output_info.arrow_type.clone(),
                output_info.nullable,
            ),
            canonical_path,
            requested_type,
            strict,
        });
    }
    Ok(plan)
}

fn variant_path_ensure_source_read_columns(
    node_id: i32,
    plan: &mut HdfsVariantPathPlan,
    read_columns: &mut HdfsScanReadColumns,
    visible_slot_ids: &[SlotId],
    slot_info_map: &HashMap<SlotId, HdfsSlotInfo>,
    physical_hdfs_columns: &HashSet<String>,
    restrict_to_hive_columns: bool,
) -> Result<(), String> {
    if plan.specs.is_empty() {
        return Ok(());
    }

    let mut source_read_slots = HashMap::new();
    for spec in &plan.specs {
        if read_columns.slot_ids.contains(&spec.source_slot_id) {
            source_read_slots.insert(spec.source_slot_id, spec.source_slot_id);
        }
    }

    let mut reserved_slot_ids = slot_info_map.keys().copied().collect::<Vec<_>>();
    reserved_slot_ids.extend_from_slice(visible_slot_ids);
    reserved_slot_ids.extend(read_columns.slot_ids.iter().copied());
    let mut next_hidden_slot_id = next_hidden_slot_id(&reserved_slot_ids)?;

    for spec in &mut plan.specs {
        let source_info = slot_info_map.get(&spec.source_slot_id).ok_or_else(|| {
            format!(
                "HDFS_SCAN_NODE node_id={node_id} variant source slot_id={} has no slot descriptor",
                spec.source_slot_id
            )
        })?;
        if restrict_to_hive_columns
            && !physical_hdfs_columns.contains(&source_info.name.to_ascii_lowercase())
        {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={node_id} variant source column {:?} slot_id={} is not a physical HDFS column",
                source_info.name, spec.source_slot_id
            ));
        }
        if let Some(read_slot_id) = source_read_slots.get(&spec.source_slot_id).copied() {
            spec.source_read_slot_id = read_slot_id;
            continue;
        }

        while reserved_slot_ids.contains(&next_hidden_slot_id) {
            next_hidden_slot_id = advance_hidden_slot_id(next_hidden_slot_id)?;
        }
        let read_slot_id = next_hidden_slot_id;
        read_columns.push_physical(read_slot_id, source_info);
        source_read_slots.insert(spec.source_slot_id, read_slot_id);
        spec.source_read_slot_id = read_slot_id;
        reserved_slot_ids.push(read_slot_id);
        next_hidden_slot_id = advance_hidden_slot_id(next_hidden_slot_id)?;
    }

    Ok(())
}

#[derive(Clone, Debug, Default)]
struct HdfsScanPruningPredicates {
    physical: Vec<MinMaxPredicate>,
    variant: Vec<VariantPathPruningPredicate>,
}

fn parse_hdfs_scan_pruning_predicates(
    node_id: i32,
    min_max_conjuncts: Option<&[exprs::TExpr]>,
    out_layout: &Layout,
    variant_path_specs: &[VariantPathSpec],
) -> Result<HdfsScanPruningPredicates, String> {
    let Some(min_max_conjuncts) = min_max_conjuncts else {
        return Ok(HdfsScanPruningPredicates::default());
    };

    let variant_by_output: HashMap<SlotId, &VariantPathSpec> = variant_path_specs
        .iter()
        .map(|spec| (spec.output_slot_id, spec))
        .collect();
    let mut predicates = HdfsScanPruningPredicates::default();

    for conjunct in min_max_conjuncts {
        let parsed =
            crate::protocol::starrocks::decode::expr::parse_min_max_conjuncts_with_column_resolver(
                conjunct,
                |slot_ref| {
                    let slot_id = SlotId::try_from(slot_ref.slot_id).map_err(|e| {
                        format!(
                            "HDFS_SCAN_NODE node_id={node_id} min_max_conjunct slot_ref has invalid slot_id={}: {e}",
                            slot_ref.slot_id
                        )
                    })?;
                    if variant_by_output.contains_key(&slot_id) {
                        return Ok(format!("variant:{}", slot_id.as_u32()));
                    }

                    let key = (slot_ref.tuple_id, slot_ref.slot_id);
                    let idx = out_layout
                        .index
                        .get(&key)
                        .ok_or_else(|| format!("slot not found in layout: {:?}", key))?;
                    Ok(idx.to_string())
                },
            )?;

        for predicate in parsed {
            let Some(slot_text) = predicate.column().strip_prefix("variant:") else {
                predicates.physical.push(predicate);
                continue;
            };
            let slot_num = slot_text.parse::<u32>().map_err(|e| {
                format!(
                    "HDFS_SCAN_NODE node_id={node_id} invalid variant predicate slot {slot_text:?}: {e}"
                )
            })?;
            let output_slot_id = SlotId::new(slot_num);
            let spec = variant_by_output.get(&output_slot_id).ok_or_else(|| {
                format!(
                    "HDFS_SCAN_NODE node_id={node_id} min/max variant output slot_id={output_slot_id} has no variant path spec"
                )
            })?;
            predicates.variant.push(VariantPathPruningPredicate {
                output_slot_id,
                source_slot_id: spec.source_slot_id,
                source_field_id: spec.source_field_id,
                canonical_path: spec.canonical_path.clone(),
                requested_type: spec.requested_type.clone(),
                predicate: predicate.with_column("0".to_string()),
            });
        }
    }

    Ok(predicates)
}

fn apply_row_position_pruning_gate(
    row_position_required: bool,
    enable_page_index: &mut bool,
    min_max_predicates: &mut Vec<MinMaxPredicate>,
    variant_path_predicates: &mut Vec<VariantPathPruningPredicate>,
    runtime_min_max_filter_columns: &mut HashMap<i32, String>,
) {
    if row_position_required {
        // When row position is required, we must keep a stable row_id sequence.
        // Page index and row group pruning can skip rows and would corrupt row_id values.
        *enable_page_index = false;
        min_max_predicates.clear();
        variant_path_predicates.clear();
        runtime_min_max_filter_columns.clear();
    }
}

/// Lower a HDFS_SCAN_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_hdfs_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    _tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    scan_ranges: Option<ScanRangeCarrier>,
    query_opts: &QueryOptions,
    iceberg_execution: Option<Arc<novarocks_spi::connector::ConnectorExecutionBinding>>,
    query_global_dict_map: &QueryGlobalDictMap,
    mut out_layout: Layout,
    decode_facts: &crate::protocol::starrocks::decode::instance::StarRocksDecodeFacts,
    query_id: Option<novarocks_types::QueryId>,
) -> Result<Lowered, String> {
    if node.num_children != 0 {
        return Err(format!(
            "HDFS_SCAN_NODE expected 0 children, got {}",
            node.num_children
        ));
    }

    let Some(hdfs) = node.hdfs_scan_node.as_ref() else {
        return Err("HDFS_SCAN_NODE missing hdfs_scan_node payload".to_string());
    };
    let tuple_id = hdfs
        .tuple_id
        .or_else(|| node.row_tuples.first().copied())
        .ok_or_else(|| "HDFS_SCAN_NODE missing tuple_id".to_string())?;

    debug!(
        "HDFS_SCAN_NODE tuple_id={}, row_tuples={:?}, hive_column_names={:?}",
        tuple_id, node.row_tuples, hdfs.hive_column_names
    );

    if out_layout.order.is_empty() {
        let hint = layout_hints
            .get(&tuple_id)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                format!(
                    "HDFS_SCAN_NODE node_id={} missing output layout for tuple_id={}",
                    node.node_id, tuple_id
                )
            })?;
        out_layout = layout_from_slot_ids(tuple_id, hint.iter().copied());
    }
    if out_layout.order.iter().any(|(tid, _)| *tid != tuple_id) {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={} has multi-tuple layout: tuple_id={} layout={:?}",
            node.node_id, tuple_id, out_layout.order
        ));
    }

    let desc_tbl = desc_tbl.ok_or_else(|| {
        format!(
            "HDFS_SCAN_NODE node_id={} requires descriptor table for column resolution",
            node.node_id
        )
    })?;
    let desc_snapshot = descriptor_snapshot_from_thrift(desc_tbl)?;
    let iceberg_table_locations = IcebergTableLocationMap::from_snapshot(&desc_snapshot);
    let is_paimon = desc_snapshot.is_paimon_table_for_tuple(tuple_id);
    let is_iceberg_table = desc_snapshot.is_iceberg_table_for_tuple(tuple_id);
    if !is_iceberg_table {
        let table_kind = if is_paimon {
            "Paimon"
        } else {
            "unknown external"
        };
        return Err(format!(
            "HDFS_SCAN_NODE node_id={} {} scans are unsupported; compat supports only internal tables and explicit Iceberg descriptors",
            node.node_id, table_kind
        ));
    }
    let hive_column_names = hdfs.hive_column_names.clone();
    let orc_use_column_names = query_opts.orc_use_column_names();

    let slot_info_map = build_hdfs_slot_info_map(&desc_snapshot, tuple_id)?;
    let columns = col_names_from_snapshot_layout(&out_layout, &slot_info_map)?;
    let has_row_position_marker_slots = slot_info_map.values().any(|info| {
        novarocks::exec::row_position::is_row_source_id(&info.name)
            || novarocks::exec::row_position::is_scan_range_id(&info.name)
    });
    let physical_hdfs_columns = hive_column_names
        .as_ref()
        .map(|names| {
            names
                .iter()
                .map(|name| name.to_ascii_lowercase())
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();

    let Some(scan_ranges) = scan_ranges else {
        return Err("HDFS_SCAN_NODE requires exec_params.per_node_scan_ranges".to_string());
    };
    let (assignment_kind, assignment_ranges) = scan_ranges
        .get(node.node_id)
        .ok_or_else(|| format!("missing typed scan assignment for node_id={}", node.node_id))?;
    if assignment_kind != ScanAssignmentKind::File {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={} expected File assignment, got {assignment_kind:?}",
            node.node_id,
        ));
    }

    let mut slot_ids = Vec::with_capacity(out_layout.order.len());
    let mut read_columns = HdfsScanReadColumns::default();
    let mut variant_path_plan = parse_hdfs_scan_variant_path_columns(
        node.node_id,
        hdfs.variant_path_columns.as_deref(),
        &slot_info_map,
    )?;

    let mut row_source_slot: Option<SlotId> = None;
    let mut scan_range_slot: Option<SlotId> = None;
    let mut row_id_slot: Option<SlotId> = None;
    let mut row_source_field: Option<arrow::datatypes::Field> = None;
    let mut scan_range_field: Option<arrow::datatypes::Field> = None;
    let mut row_id_field: Option<arrow::datatypes::Field> = None;
    let mut has_iceberg_virtual_row_id = false;
    let mut has_iceberg_virtual_change_op = false;

    for (tuple_id, slot_id) in &out_layout.order {
        let slot_id = SlotId::try_from(*slot_id)?;
        let info = slot_info_map
            .get(&slot_id)
            .ok_or_else(|| format!("missing slot info for tuple_id={tuple_id} slot_id={slot_id}"))?
            .clone();
        let name = info.name.clone();
        let logical = info.logical.clone();
        let arrow_type = info.arrow_type.clone();
        let nullable = info.nullable;
        slot_ids.push(slot_id);

        if variant_path_plan.output_slot_ids.contains(&slot_id) {
            continue;
        }

        if novarocks::exec::row_position::is_row_source_id(&name) {
            if !logical.is_int32() {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} row_source_id slot_id={} expects INT, got {:?}",
                    node.node_id, slot_id, logical
                ));
            }
            row_source_slot = Some(slot_id);
            row_source_field = Some(arrow::datatypes::Field::new(name, arrow_type, nullable));
            continue;
        }
        if novarocks::exec::row_position::is_scan_range_id(&name) {
            if !logical.is_int32() {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} scan_range_id slot_id={} expects INT, got {:?}",
                    node.node_id, slot_id, logical
                ));
            }
            scan_range_slot = Some(slot_id);
            scan_range_field = Some(arrow::datatypes::Field::new(name, arrow_type, nullable));
            continue;
        }
        if has_row_position_marker_slots && novarocks::exec::row_position::is_row_id(&name) {
            if !logical.is_int64() {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} row_id slot_id={} expects BIGINT, got {:?}",
                    node.node_id, slot_id, logical
                ));
            }
            row_id_slot = Some(slot_id);
            row_id_field = Some(arrow::datatypes::Field::new(name, arrow_type, nullable));
            continue;
        }
        let is_physical_hdfs_column = physical_hdfs_columns.contains(&name.to_ascii_lowercase());
        if !is_physical_hdfs_column && novarocks::exec::row_position::is_iceberg_file_path(&name) {
            if !matches!(logical, DescriptorLogicalType::Utf8) {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} _file slot_id={} expects VARCHAR, got {:?}",
                    node.node_id, slot_id, logical
                ));
            }
            continue;
        }
        if !is_physical_hdfs_column && novarocks::exec::row_position::is_iceberg_row_pos(&name) {
            if !logical.is_int64() {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} _pos slot_id={} expects BIGINT, got {:?}",
                    node.node_id, slot_id, logical
                ));
            }
            continue;
        }

        // Lowering of `_row_id` / `_last_updated_sequence_number` slots into
        // tests (e.g. `select_row_id_and_last_updated_seq_on_v3_row_lineage_table`).
        // The synthetic-fixture style used elsewhere in this file is not added
        // here because constructing a valid `TPlanNode` for an iceberg scan
        // requires substantial scaffolding that the integration path already
        // covers more economically.
        if !is_physical_hdfs_column && novarocks::exec::row_position::is_iceberg_row_id(&name) {
            if !logical.is_int64() {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} _row_id slot_id={} expects BIGINT, got {:?}",
                    node.node_id, slot_id, logical
                ));
            }
            has_iceberg_virtual_row_id = true;
            continue;
        }

        if !is_physical_hdfs_column
            && novarocks::exec::row_position::is_iceberg_last_updated_sequence_number(&name)
        {
            if !logical.is_int64() {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} _last_updated_sequence_number slot_id={} expects BIGINT, got {:?}",
                    node.node_id, slot_id, logical
                ));
            }
            continue;
        }
        if novarocks::exec::row_position::is_change_op(&name)
            && hdfs.extended_slot_ids.as_deref().is_some_and(|slots| {
                slots.iter().any(|raw| {
                    u32::try_from(*raw)
                        .ok()
                        .is_some_and(|raw| raw == slot_id.as_u32())
                })
            })
        {
            if !logical.is_int8() {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} __change_op slot_id={} expects TINYINT, got {:?}",
                    node.node_id, slot_id, logical
                ));
            }
            has_iceberg_virtual_change_op = true;
            continue;
        }

        read_columns.push_physical(slot_id, &info);
    }

    variant_path_ensure_source_read_columns(
        node.node_id,
        &mut variant_path_plan,
        &mut read_columns,
        &slot_ids,
        &slot_info_map,
        &physical_hdfs_columns,
        hive_column_names.is_some(),
    )?;

    if !slot_ids.is_empty() && slot_ids.len() != columns.len() {
        return Err(format!(
            "HDFS_SCAN_NODE output layout/columns mismatch: layout_len={}, columns_len={}, layout={:?}, columns={:?}",
            slot_ids.len(),
            columns.len(),
            out_layout.order,
            columns
        ));
    }

    // Row position slots must be present as a full set; partial definitions corrupt row_id mapping.
    let row_position_spec = match (row_source_slot, scan_range_slot, row_id_slot) {
        (None, None, None) => None,
        (Some(row_source_slot), Some(scan_range_slot), Some(row_id_slot)) => {
            let row_source_field = row_source_field.expect("row_source_field");
            let scan_range_field = scan_range_field.expect("scan_range_field");
            let row_id_field = row_id_field.expect("row_id_field");
            Some(novarocks::exec::row_position::RowPositionSpec {
                row_source_slot,
                scan_range_slot,
                row_id_slot,
                row_source_field,
                scan_range_field,
                row_id_field,
            })
        }
        _ => {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} row position slots must be present together (_row_source_id/_scan_range_id/_row_id)",
                node.node_id
            ));
        }
    };
    let needs_first_row_id = row_position_spec.is_some() || has_iceberg_virtual_row_id;

    let mut cache_options = CacheOptions::from_query_options(Some(query_opts))?;
    if let Some(node_datacache_options) = hdfs.datacache_options.as_ref() {
        let node_range_options = ExternalDataCacheRangeOptions {
            modification_time: None,
            enable_populate_datacache: node_datacache_options.enable_populate_datacache,
            datacache_priority: node_datacache_options.priority,
            candidate_node: None,
        };
        cache_options = cache_options.with_external_range_options(Some(&node_range_options))?;
    }
    if cache_options.enable_cache_select {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={} does not support enable_cache_select yet",
            node.node_id
        ));
    }
    let datacache_requested =
        cache_options.enable_scan_datacache || cache_options.enable_populate_datacache;
    if datacache_requested {
        if !decode_facts.datacache_available() {
            warn!(
                "HDFS_SCAN_NODE node_id={} requested datacache (scan={}, populate={}) but explicit decoder facts report it unavailable; fallback to remote read without datacache",
                node.node_id,
                cache_options.enable_scan_datacache,
                cache_options.enable_populate_datacache
            );
            cache_options.disable_external_datacache();
        }
    }

    let limit = node.limit;
    let limit = (limit >= 0).then_some(limit as usize);
    let connector_io_tasks_per_scan_operator = query_opts.connector_io_tasks_per_scan_operator();
    let iceberg_metadata_table_type = hdfs
        .metadata_table_type
        .as_deref()
        .map(IcebergMetadataTableType::parse)
        .transpose()?;
    if row_position_spec.is_some() && iceberg_metadata_table_type.is_some() {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={} does not support row position with Iceberg metadata tables",
            node.node_id
        ));
    }
    // The metadata-table scan path used to require an embedded JVM bridge
    // for the Iceberg Java SDK; it now runs natively against
    // iceberg-rust's `TableMetadata`. The SPI reader rejects flavors the native path does
    // not yet implement (Files / Manifests / LogicalIcebergMetadata).
    let is_iceberg_metadata_scan = iceberg_metadata_table_type.is_some();
    let mut iceberg_files: Vec<IcebergDataFileInfo> = Vec::new();
    let mut iceberg_metadata_ranges: Vec<IcebergMetadataScanRange> = Vec::new();
    let mut scan_format: Option<descriptors::THdfsFileFormat> = None;
    for p in assignment_ranges {
        if p.empty.unwrap_or(false) {
            if p.has_more.unwrap_or(false) {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} incremental Iceberg scan ranges are unsupported",
                    node.node_id
                ));
            }
            continue;
        }
        let ScanRange::File(hdfs_range) = &p.range else {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} assignment contains non-file range",
                node.node_id
            ));
        };
        if is_iceberg_metadata_scan {
            if !hdfs_range.use_iceberg_jni_metadata_reader {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} expected Iceberg metadata scan range with use_iceberg_jni_metadata_reader=true",
                    node.node_id
                ));
            }
            let path = if let Some(path) = hdfs_range
                .full_path
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                path.to_string()
            } else if let Some(rel) = hdfs_range
                .relative_path
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                let table_id = hdfs_range.table_id.ok_or_else(|| {
                    format!(
                        "HDFS_SCAN_NODE node_id={} has relative_path={rel:?} but missing table_id for Iceberg metadata scan",
                        node.node_id
                    )
                })?;
                let loc = iceberg_table_locations.get(table_id).ok_or_else(|| {
                    format!(
                        "HDFS_SCAN_NODE node_id={} has relative_path={rel:?} but missing cached iceberg location for table_id={table_id}",
                        node.node_id
                    )
                })?;
                let base = loc.trim_end_matches('/');
                let rel = rel.trim_start_matches('/');
                if rel.is_empty() {
                    base.to_string()
                } else {
                    format!("{base}/{rel}")
                }
            } else {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} Iceberg metadata scan requires full_path or relative_path",
                    node.node_id
                ));
            };
            iceberg_metadata_ranges.push(IcebergMetadataScanRange {
                path,
                serialized_split: hdfs_range.serialized_split.clone().unwrap_or_default(),
            });
            continue;
        }
        let mut iceberg_delete_files = hdfs_range
            .delete_files
            .iter()
            .map(|file| IcebergDeleteFileSpec {
                path: file.full_path.clone().unwrap_or_default(),
                file_format: IcebergFileFormat::Parquet,
                file_content: match file.file_content {
                    novarocks::runtime::scan_range::IcebergFileContent::PositionDeletes => {
                        IcebergFileContent::PositionDeletes
                    }
                    novarocks::runtime::scan_range::IcebergFileContent::EqualityDeletes => {
                        IcebergFileContent::EqualityDeletes
                    }
                },
                length: file.length.and_then(|value| u64::try_from(value).ok()),
                content_offset: None,
                content_size_in_bytes: None,
            })
            .collect::<Vec<_>>();
        if let Some(dv) = hdfs_range.deletion_vector_descriptor.as_ref() {
            let path = dv
                .path_or_inline_dv
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    format!(
                        "HDFS_SCAN_NODE node_id={} deletion vector is missing path_or_inline_dv",
                        node.node_id
                    )
                })?
                .to_string();
            let offset = dv.offset.ok_or_else(|| {
                format!(
                    "HDFS_SCAN_NODE node_id={} deletion vector {} is missing offset",
                    node.node_id, path
                )
            })?;
            let size = dv.size_in_bytes.ok_or_else(|| {
                format!(
                    "HDFS_SCAN_NODE node_id={} deletion vector {} is missing size_in_bytes",
                    node.node_id, path
                )
            })?;
            iceberg_delete_files.push(IcebergDeleteFileSpec::puffin_position_delete(
                path, None, offset, size,
            ));
        }
        let file_format = match hdfs_range.file_format {
            RuntimeFileFormat::Parquet => descriptors::THdfsFileFormat::PARQUET,
            RuntimeFileFormat::Orc => descriptors::THdfsFileFormat::ORC,
        };
        if row_position_spec.is_some() && file_format != descriptors::THdfsFileFormat::PARQUET {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} row position requires PARQUET scan ranges, got {:?}",
                node.node_id, file_format
            ));
        }
        if let Some(prev) = scan_format.as_ref() {
            if prev != &file_format {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} mixed file formats: {:?} vs {:?}",
                    node.node_id, prev, file_format
                ));
            }
        } else {
            scan_format = Some(file_format);
        }
        if is_paimon {
            if file_format != descriptors::THdfsFileFormat::PARQUET
                && file_format != descriptors::THdfsFileFormat::ORC
            {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} only supports parquet/orc for Paimon tables",
                    node.node_id
                ));
            }
            if hdfs_range.full_path.as_ref().is_none_or(|s| s.is_empty()) {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} requires full_path for Paimon tables",
                    node.node_id
                ));
            }
        }
        let file_len = hdfs_range.file_length;
        let file_len = if file_len > 0 { file_len as u64 } else { 0 };
        let offset = hdfs_range.offset;
        let offset = if offset >= 0 { offset as u64 } else { 0 };
        let length = hdfs_range.length;
        let mut length = if length > 0 { length as u64 } else { 0 };
        if length == 0 && file_len > offset {
            length = file_len - offset;
        }
        if !hdfs_range.included_positions.is_empty() && !(offset == 0 && length == file_len) {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} included_positions requires a full-file range",
                node.node_id
            ));
        }
        let first_row_id = if needs_first_row_id {
            Some(hdfs_range.first_row_id.ok_or_else(|| {
                format!(
                    "HDFS_SCAN_NODE node_id={} missing first_row_id for iceberg row position or row-lineage scan",
                    node.node_id
                )
            })?)
        } else {
            None
        };
        // data_sequence_number is populated from THdfsScanRange field 38
        // when the NovaRocks iceberg codegen path (standalone SQL) fills it in.
        // For FE-sent scan ranges that do not carry field 38, this will be
        // None, which is acceptable: the incremental morsel builder also
        // produces None for FE-driven ranges (see build_incremental_morsels).
        let data_sequence_number = hdfs_range.data_sequence_number;
        let ivm_change_op = hdfs_range.ivm_change_op;
        if has_iceberg_virtual_change_op && ivm_change_op.is_none() {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} __change_op virtual slot requires every scan range to carry extended_columns",
                node.node_id
            ));
        }

        let path = if let Some(fp) = hdfs_range.full_path.as_ref().filter(|s| !s.is_empty()) {
            fp.clone()
        } else if let Some(rp) = hdfs_range.relative_path.as_ref().filter(|s| !s.is_empty()) {
            let table_id = hdfs_range.table_id.ok_or_else(|| {
                format!(
                    "HDFS_SCAN_NODE node_id={} has relative_path={rp:?} but missing table_id; cannot resolve to full OSS path",
                    node.node_id
                )
            })?;
            let loc = iceberg_table_locations.get(table_id).ok_or_else(|| {
                format!(
                    "HDFS_SCAN_NODE node_id={} has relative_path={rp:?} but missing cached iceberg location for table_id={table_id}",
                    node.node_id
                )
            })?;
            let base = loc.trim_end_matches('/');
            let rel = rp.trim_start_matches('/');
            format!("{base}/{rel}")
        } else {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} Iceberg scan range requires full_path or relative_path",
                node.node_id
            ));
        };
        iceberg_files.push(compat_iceberg_file_from_wire(
            node.node_id,
            path,
            file_len,
            offset,
            length,
            first_row_id,
            data_sequence_number,
            ivm_change_op,
            (!hdfs_range.included_positions.is_empty())
                .then(|| hdfs_range.included_positions.clone()),
            iceberg_delete_files,
        )?);
    }
    if let Some(metadata_table_type) = iceberg_metadata_table_type {
        let batch_size: usize = query_opts
            .batch_size()
            .and_then(|bs| usize::try_from(bs).ok())
            .unwrap_or(4096)
            .max(1);
        let output_columns = output_slots_from_layout(&out_layout, &slot_info_map)?;
        let cfg = IcebergMetadataScanConfig {
            metadata_table_type,
            serialized_table: hdfs.serialized_table.clone().unwrap_or_default(),
            serialized_predicate: hdfs.serialized_predicate.clone().unwrap_or_default(),
            load_column_stats: hdfs.load_column_stats.unwrap_or(false),
            ranges: iceberg_metadata_ranges,
            batch_size,
            output_columns,
            profile_label: Some(format!("hdfs_scan_node_id={}", node.node_id)),
        };
        let source =
            plan_compat_iceberg_metadata_read_source(query_id, node.node_id, cfg, query_opts)
                .map_err(|error| error.to_string())?;
        scan_ranges.capture(node.node_id, BoundScanRanges::None);
        let scan = ScanNode::new(source)
            .with_node_id(node.node_id)
            .with_output_chunk_schema(chunk_schema_for_snapshot_layout(
                &out_layout,
                &slot_info_map,
            )?)
            .with_limit(limit)
            .with_connector_io_tasks_per_scan_operator(connector_io_tasks_per_scan_operator)
            .with_accept_empty_scan_ranges(true);
        return Ok(Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Scan(scan),
            },
            layout: out_layout,
        });
    }
    if is_iceberg_table {
        let query_id = query_id.ok_or_else(|| {
            "HDFS_SCAN_NODE requires a query identity for connector cancellation".to_string()
        })?;
        let output_chunk_schema = chunk_schema_for_snapshot_layout(&out_layout, &slot_info_map)?;
        let row_position_ids = row_position_spec.as_ref().map(|_| {
            iceberg_files
                .iter()
                .enumerate()
                .map(|(index, _)| i32::try_from(index).unwrap_or(i32::MAX))
                .collect::<Vec<_>>()
        });
        let planned = plan_compat_iceberg_data_read_source(
            iceberg_execution
                .clone()
                .ok_or_else(|| "compat Iceberg execution binding is unavailable".to_string())?,
            query_id,
            output_chunk_schema.clone(),
            iceberg_files,
            row_position_ids,
            row_position_spec.as_ref(),
            query_opts,
        )?;
        scan_ranges.capture(node.node_id, BoundScanRanges::None);
        let scan = ScanNode::new(planned.source)
            .with_node_id(node.node_id)
            .with_output_chunk_schema(output_chunk_schema)
            .with_limit(limit)
            .with_connector_io_tasks_per_scan_operator(connector_io_tasks_per_scan_operator)
            .with_accept_empty_scan_ranges(true)
            .with_row_position(row_position_spec)
            .with_connector_row_position_lookup(planned.row_position_lookup);
        return Ok(Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Scan(scan),
            },
            layout: out_layout,
        });
    }
    unreachable!("non-Iceberg HDFS_SCAN_NODEs are rejected before scan materialization")
}

fn output_slots_from_layout(
    layout: &Layout,
    slot_info_map: &HashMap<SlotId, HdfsSlotInfo>,
) -> Result<Vec<IcebergMetadataOutputColumn>, String> {
    layout
        .order
        .iter()
        .map(|(_, slot_id)| {
            let slot_id = SlotId::try_from(*slot_id)?;
            let info = slot_info_map
                .get(&slot_id)
                .ok_or_else(|| format!("missing slot info for slot_id={slot_id}"))?
                .clone();
            Ok(IcebergMetadataOutputColumn {
                name: info.name,
                slot_id,
                data_type: info.arrow_type,
                nullable: info.nullable,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compat_iceberg_normalization_preserves_delete_and_lineage_facts() {
        let file = compat_iceberg_file_from_wire(
            7,
            "s3://warehouse/data.parquet".to_string(),
            128,
            0,
            128,
            Some(100),
            Some(9),
            None,
            Some(vec![4, 8]),
            vec![IcebergDeleteFileSpec::puffin_position_delete(
                "s3://warehouse/delete.puffin".to_string(),
                Some(64),
                12,
                34,
            )],
        )
        .expect("normalize Iceberg compatibility facts");
        assert_eq!(file.first_row_id, Some(100));
        assert_eq!(file.data_sequence_number, Some(9));
        assert_eq!(file.included_positions, Some(vec![4, 8]));
        assert_eq!(file.delete_files.len(), 1);
        assert_eq!(
            file.delete_files[0].file_format,
            IcebergDeleteFileFormat::Puffin
        );
    }

    #[test]
    fn compat_iceberg_normalization_rejects_partial_data_file() {
        let error = compat_iceberg_file_from_wire(
            8,
            "file:///tmp/data.parquet".to_string(),
            128,
            64,
            64,
            None,
            None,
            None,
            None,
            Vec::new(),
        )
        .expect_err("partial compatibility range must not bypass Iceberg coordinates");
        assert!(error.contains("complete data file"));
    }
}
