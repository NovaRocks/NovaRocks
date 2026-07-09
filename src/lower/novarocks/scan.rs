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

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use super::expr::lower_proto_expr;
use super::layout::{chunk_schema_from_output_columns, layout_from_output_columns};
use super::node::{LoweredNode, NodeLoweringContext};
use crate::cache::{CacheOptions, DataCacheManager, ExternalDataCacheRangeOptions};
use crate::common::ids::SlotId;
use crate::connector::iceberg::delete_file::{
    IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat,
};
use crate::connector::iceberg::metadata::{
    IcebergMetadataOutputColumn, IcebergMetadataScanConfig, IcebergMetadataScanRange,
    IcebergMetadataTableType,
};
use crate::connector::iceberg::{
    IcebergArrowColumn, IcebergSchemaDescriptor, IcebergSchemaFieldDescriptor,
    IcebergTableDescriptor, build_projected_output_schema,
    file_pruning::IcebergFilePruningMetadata,
};
use crate::connector::{HdfsIcebergRuntimePruningConfig, HdfsScanConfig, ScanConfig};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::filter::FilterNode;
use crate::exec::node::iceberg_delta_scan::{
    ApplyKeySource, BaseDataFileLineage, BaseTableIdent, DeletedFileVisibility,
    DeltaScanDeleteSide, DeltaScanDeleteSidePayload, DeltaSourceFile, DeltaSourceRole,
    EqualityDeleteTargetData, IcebergDeltaDataColumnPayload, IcebergDeltaScanNode,
    IcebergDeltaTablePayload, IcebergRuntimeHandles, PositionDeleteFileFormat,
    PositionDeleteSourceData,
};
use crate::exec::node::project::ProjectNode;
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::exec::row_position::IcebergVirtualSpec;
use crate::formats::FileFormatConfig;
use crate::formats::parquet::{
    ParquetReadCachePolicy, ParquetScanConfig, ParquetSlotKind, VariantPathSpec,
};
use crate::fs::object_store::{ObjectStoreConfig, apply_object_store_runtime_defaults};
use crate::fs::object_store_credentials::{ObjectStoreCredentials, ObjectStoreCredentialsSource};
use crate::fs::scan_context::FileScanRange;
use crate::proto::{common, novarocks, plan};
use crate::sql::catalog::IcebergColumnStats;

pub(crate) fn lower_scan_node(
    node: &plan::DistributedNode,
    _physical: &plan::PlanNode,
    scan: &plan::ScanNode,
    ctx: &NodeLoweringContext,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    if !node.children.is_empty() {
        return Err(format!(
            "ScanNode node_id={} expected no children, got {}",
            node.node_id,
            node.children.len()
        ));
    }
    if !scan.dict_columns.is_empty() {
        return Err("ScanNode dict_columns are not supported by native lowering yet".to_string());
    }
    let table = scan
        .table
        .as_ref()
        .ok_or_else(|| "ScanNode table missing".to_string())?;
    let source = table
        .source
        .as_ref()
        .and_then(|source| source.kind.as_ref())
        .ok_or_else(|| "ScanNode table source missing".to_string())?;
    match source {
        plan::scan_source::Kind::IcebergDataFiles(source) => {
            lower_iceberg_data_files_scan(node, scan, source, ctx, arena)
        }
        plan::scan_source::Kind::IcebergMetadataTable(source) => {
            reject_variant_columns_for_source(scan, "IcebergMetadataTable")?;
            lower_iceberg_metadata_scan(node, scan, source, ctx, arena)
        }
        plan::scan_source::Kind::IcebergDeltaTable(source) => {
            reject_variant_columns_for_source(scan, "IcebergDeltaTable")?;
            lower_iceberg_delta_table_scan(node, scan, source, ctx, arena)
        }
        plan::scan_source::Kind::IcebergVersionTable(_) => {
            reject_variant_columns_for_source(scan, "IcebergVersionTable")?;
            unsupported_scan_source("IcebergVersionTable")
        }
        plan::scan_source::Kind::IcebergMvTargetState(_) => {
            reject_variant_columns_for_source(scan, "IcebergMvTargetState")?;
            unsupported_scan_source("IcebergMvTargetState")
        }
        plan::scan_source::Kind::IcebergMvTargetLocator(_) => {
            reject_variant_columns_for_source(scan, "IcebergMvTargetLocator")?;
            unsupported_scan_source("IcebergMvTargetLocator")
        }
    }
}

fn reject_variant_columns_for_source(
    scan: &plan::ScanNode,
    source_name: &str,
) -> Result<(), String> {
    if scan.variant_columns.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{source_name} native scan does not support variant_columns"
    ))
}

fn lower_iceberg_data_files_scan(
    node: &plan::DistributedNode,
    scan: &plan::ScanNode,
    source: &plan::IcebergDataFiles,
    ctx: &NodeLoweringContext,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    let output_columns = scan_output_columns(scan)?;
    let table = source
        .table
        .as_ref()
        .ok_or_else(|| "IcebergDataFiles table missing".to_string())?;
    let read_plan = scan_read_plan(scan, table, &output_columns)?;
    let ranges = decode_file_scan_ranges(node.node_id, table, ctx.scan_ranges(node.node_id)?)?;
    let cache_options = CacheOptions::from_query_options(ctx.query_options())?;
    let batch_size = scan_batch_size(ctx.query_options())?;
    let parquet_cfg = ParquetScanConfig {
        columns: read_plan.read_columns.clone(),
        chunk_schema: read_plan.parquet_schema.clone(),
        slot_kinds: read_plan.slot_kinds.clone(),
        case_sensitive: true,
        enable_page_index: false,
        min_max_predicates: Vec::new(),
        runtime_min_max_filter_columns: HashMap::new(),
        variant_path_predicates: Vec::new(),
        batch_size: Some(batch_size),
        datacache: DataCacheManager::instance().external_context(cache_options),
        cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
        profile_label: Some(format!("native_scan_node_id={}", node.node_id)),
        iceberg_output_schema: Some(read_plan.parquet_schema.arrow_schema_ref()),
        variant_path_columns: read_plan.variant_path_columns.clone(),
        query_global_dicts: Default::default(),
    };
    let object_store_config = resolve_cloud_object_store_config(&source.cloud_properties)?;
    let iceberg_runtime_pruning = Some(HdfsIcebergRuntimePruningConfig {
        slot_to_column: read_plan
            .read_slot_ids
            .iter()
            .copied()
            .zip(read_plan.read_columns.iter().cloned())
            .collect(),
        min_max_filter_columns: HashMap::new(),
        discrete_set_max_values: 256,
    });
    let cfg = HdfsScanConfig {
        original_range_count: ranges.len(),
        ranges,
        has_more: false,
        limit: parse_scan_limit(node.limit)?,
        profile_label: Some(format!("native_scan_node_id={}", node.node_id)),
        format: Some(FileFormatConfig::Parquet(parquet_cfg)),
        object_store_config,
        iceberg_table_locations: table_location_map(table),
        query_global_dicts: Default::default(),
        iceberg_runtime_pruning,
    };
    let predicate = lower_scan_predicate(scan, arena, &read_plan.read_layout)?;
    let scan_node = ctx
        .connectors()?
        .create_scan_node("hdfs", ScanConfig::Hdfs(Box::new(cfg)))?
        .with_node_id(node.node_id)
        .with_output_chunk_schema(read_plan.read_schema.clone())
        .with_limit(parse_scan_limit(node.limit)?)
        .with_conjunct_predicate(predicate)
        .with_iceberg_virtual(Some(read_plan.iceberg_virtual.clone()))
        .with_accept_empty_scan_ranges(true);
    let scan_lowered = LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan_node),
        },
        layout: read_plan.read_layout.clone(),
        output_schema: read_plan.read_schema.clone(),
    };
    maybe_project_data_scan_output(node.node_id, scan_lowered, read_plan, arena)
}

fn lower_iceberg_metadata_scan(
    node: &plan::DistributedNode,
    scan: &plan::ScanNode,
    source: &plan::IcebergMetadataTable,
    ctx: &NodeLoweringContext,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    let output_columns = scan_output_columns(scan)?;
    let layout = layout_from_output_columns(&output_columns)?;
    let output_schema = chunk_schema_from_output_columns(&output_columns)?;
    let metadata_table_type = metadata_table_type(source.metadata_table_type)?;
    let ranges = decode_metadata_scan_ranges(ctx.scan_ranges(node.node_id)?)?;
    let cfg = IcebergMetadataScanConfig {
        metadata_table_type,
        serialized_table: source.serialized_table.clone(),
        serialized_predicate: source.metadata_payload.clone().unwrap_or_default(),
        load_column_stats: false,
        ranges,
        batch_size: 4096,
        output_columns: metadata_output_columns(&output_columns)?,
        profile_label: Some(format!("native_scan_node_id={}", node.node_id)),
    };
    let predicate = lower_scan_predicate(scan, arena, &layout)?;
    let scan_node = ctx
        .connectors()?
        .create_scan_node("iceberg", ScanConfig::IcebergMetadata(cfg))?
        .with_node_id(node.node_id)
        .with_output_chunk_schema(output_schema.clone())
        .with_limit(parse_scan_limit(node.limit)?)
        .with_conjunct_predicate(predicate)
        .with_accept_empty_scan_ranges(true);
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan_node),
        },
        layout,
        output_schema,
    })
}

fn lower_iceberg_delta_table_scan(
    node: &plan::DistributedNode,
    scan: &plan::ScanNode,
    source: &plan::IcebergDeltaTable,
    ctx: &NodeLoweringContext,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    let output_columns = scan_output_columns(scan)?;
    let layout = layout_from_output_columns(&output_columns)?;
    let output_schema = chunk_schema_from_output_columns(&output_columns)?;
    let ranges = decode_metadata_scan_ranges(ctx.scan_ranges(node.node_id)?)?;
    if ranges.is_empty() {
        return empty_values_node(node.node_id, layout, output_schema);
    }
    let table = source
        .table
        .as_ref()
        .ok_or_else(|| "IcebergDeltaTable table missing".to_string())?;
    if source.from_snapshot_id < 0 {
        return Err(format!(
            "IcebergDeltaTable node_id={} from_snapshot_id must be non-negative, got {}",
            node.node_id, source.from_snapshot_id
        ));
    }
    if source.to_snapshot_id < 0 {
        return Err(format!(
            "IcebergDeltaTable node_id={} to_snapshot_id must be non-negative, got {}",
            node.node_id, source.to_snapshot_id
        ));
    }
    let delta_plan = source
        .delta_plan
        .as_ref()
        .ok_or_else(|| "IcebergDeltaTable delta_plan missing".to_string())?;
    let table_payload = IcebergDeltaTablePayload {
        table_location: delta_plan.table_location.clone(),
        data_columns: delta_plan
            .data_columns
            .iter()
            .map(|column| IcebergDeltaDataColumnPayload {
                name: column.name.clone(),
                field_id: column.field_id,
            })
            .collect(),
    };
    let change_files = lower_delta_source_files_from_native(&delta_plan.change_files)?;
    let object_store_config = resolve_cloud_object_store_config(&delta_plan.cloud_properties)?;
    let object_store_factory = Arc::new(
        crate::connector::iceberg::changes::build_factory_for_table_location(
            &table_payload.table_location,
            object_store_config.as_ref(),
        )?,
    );
    let delete_side_payload =
        lower_delta_delete_side_payload_from_native(delta_plan.delete_side.as_ref())?;
    let delete_side =
        build_delta_delete_side_from_payload(delete_side_payload, object_store_config.as_ref())?;

    let mut exec_node = ExecNode {
        kind: ExecNodeKind::IcebergDeltaScan(IcebergDeltaScanNode {
            base_table_ident: BaseTableIdent {
                catalog: table.catalog.clone(),
                namespace: table.namespace.clone(),
                table: table.table.clone(),
            },
            table_location: table_payload.table_location.clone(),
            from_snapshot_id: source.from_snapshot_id,
            to_snapshot_id: source.to_snapshot_id,
            output_chunk_schema: output_schema.clone(),
            apply_key_source: ApplyKeySource::BaseRowId,
            change_files,
            object_store_config,
            iceberg_runtime: Arc::new(IcebergRuntimeHandles {
                table: table_payload,
                object_store_factory,
                delete_side,
            }),
            node_id: node.node_id,
        }),
    };
    if let Some(predicate) = lower_scan_predicate(scan, arena, &layout)? {
        exec_node = ExecNode {
            kind: ExecNodeKind::Filter(FilterNode {
                input: Box::new(exec_node),
                node_id: node.node_id,
                predicate,
            }),
        };
    }
    Ok(LoweredNode {
        node: exec_node,
        layout,
        output_schema,
    })
}

fn empty_values_node(
    node_id: i32,
    layout: super::layout::Layout,
    output_schema: ChunkSchemaRef,
) -> Result<LoweredNode, String> {
    let batch = RecordBatch::new_empty(output_schema.arrow_schema_ref());
    let chunk = Chunk::try_new_with_chunk_schema(batch, output_schema.clone())?;
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Values(ValuesNode { chunk, node_id }),
        },
        layout,
        output_schema,
    })
}

fn lower_delta_source_files_from_native(
    files: &[plan::IcebergDeltaSourceFile],
) -> Result<Vec<DeltaSourceFile>, String> {
    files
        .iter()
        .map(lower_delta_source_file_from_native)
        .collect()
}

fn lower_delta_source_file_from_native(
    file: &plan::IcebergDeltaSourceFile,
) -> Result<DeltaSourceFile, String> {
    let role = match plan::IcebergDeltaSourceRole::try_from(file.role).map_err(|_| {
        format!(
            "IcebergDeltaTable source file {} has unknown delta role {}",
            file.path, file.role
        )
    })? {
        plan::IcebergDeltaSourceRole::Unspecified => {
            return Err(format!(
                "IcebergDeltaTable source file {} has unspecified delta role",
                file.path
            ));
        }
        plan::IcebergDeltaSourceRole::DataFile => {
            reject_native_delta_role_payload(
                file,
                "DATA_FILE",
                &[
                    "position_deletes",
                    "equality_field_ids",
                    "equality_targets",
                    "deleted_file_visibility",
                ],
            )?;
            DeltaSourceRole::DataFile
        }
        plan::IcebergDeltaSourceRole::PositionDelete => {
            reject_native_delta_role_payload(
                file,
                "POSITION_DELETE",
                &[
                    "equality_field_ids",
                    "equality_targets",
                    "deleted_file_visibility",
                ],
            )?;
            if file.position_deletes.is_empty() {
                return Err(format!(
                    "IcebergDeltaTable source file {} role POSITION_DELETE requires position_deletes",
                    file.path
                ));
            }
            DeltaSourceRole::PositionDelete {
                deletes: file
                    .position_deletes
                    .iter()
                    .map(lower_position_delete_source_from_native)
                    .collect::<Result<Vec<_>, _>>()?,
            }
        }
        plan::IcebergDeltaSourceRole::EqualityDelete => {
            reject_native_delta_role_payload(
                file,
                "EQUALITY_DELETE",
                &["position_deletes", "deleted_file_visibility"],
            )?;
            if file.equality_field_ids.is_empty() {
                return Err(format!(
                    "IcebergDeltaTable source file {} role EQUALITY_DELETE requires equality_field_ids",
                    file.path
                ));
            }
            if file.equality_targets.is_empty() {
                return Err(format!(
                    "IcebergDeltaTable source file {} role EQUALITY_DELETE requires equality_targets",
                    file.path
                ));
            }
            DeltaSourceRole::EqualityDelete {
                equality_field_ids: file.equality_field_ids.clone(),
                targets: file
                    .equality_targets
                    .iter()
                    .map(lower_equality_delete_target_from_native)
                    .collect(),
            }
        }
        plan::IcebergDeltaSourceRole::DeletedDataFile => {
            reject_native_delta_role_payload(
                file,
                "DELETED_DATA_FILE",
                &["position_deletes", "equality_field_ids", "equality_targets"],
            )?;
            DeltaSourceRole::DeletedDataFile {
                previous_data_file_visibility: file.deleted_file_visibility.as_ref().map(
                    |visibility| DeletedFileVisibility {
                        already_deleted_positions: visibility.already_deleted_positions.clone(),
                    },
                ),
            }
        }
    };

    Ok(DeltaSourceFile {
        path: file.path.clone(),
        size: file.size,
        role,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key.clone(),
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        row_id_allow_list: if file.row_id_allow_list.is_empty() {
            None
        } else {
            Some(file.row_id_allow_list.iter().copied().collect())
        },
    })
}

fn reject_native_delta_role_payload(
    file: &plan::IcebergDeltaSourceFile,
    role_name: &str,
    fields: &[&str],
) -> Result<(), String> {
    for field in fields {
        let present = match *field {
            "position_deletes" => !file.position_deletes.is_empty(),
            "equality_field_ids" => !file.equality_field_ids.is_empty(),
            "equality_targets" => !file.equality_targets.is_empty(),
            "deleted_file_visibility" => file.deleted_file_visibility.is_some(),
            _ => false,
        };
        if present {
            return Err(format!(
                "IcebergDeltaTable source file {} role {} must not carry {}",
                file.path, role_name, field
            ));
        }
    }
    Ok(())
}

fn lower_position_delete_source_from_native(
    delete: &plan::IcebergDeltaPositionDeleteSource,
) -> Result<PositionDeleteSourceData, String> {
    Ok(PositionDeleteSourceData {
        delete_file_path: delete.delete_file_path.clone(),
        delete_file_size: delete.delete_file_size,
        referenced_data_file: delete.referenced_data_file.clone(),
        file_format: lower_position_delete_format_from_native(delete.file_format)?,
        content_offset: delete.content_offset,
        content_size_in_bytes: delete.content_size_in_bytes,
    })
}

fn lower_position_delete_format_from_native(
    format: i32,
) -> Result<PositionDeleteFileFormat, String> {
    match plan::IcebergDeltaPositionDeleteFileFormat::try_from(format).map_err(|_| {
        format!("IcebergDeltaTable unsupported position-delete file format {format}")
    })? {
        plan::IcebergDeltaPositionDeleteFileFormat::Unspecified => {
            Err("IcebergDeltaTable position-delete file format is unspecified".to_string())
        }
        plan::IcebergDeltaPositionDeleteFileFormat::Parquet => {
            Ok(PositionDeleteFileFormat::Parquet)
        }
        plan::IcebergDeltaPositionDeleteFileFormat::Puffin => Ok(PositionDeleteFileFormat::Puffin),
    }
}

fn lower_equality_delete_target_from_native(
    target: &plan::IcebergDeltaEqualityDeleteTarget,
) -> EqualityDeleteTargetData {
    EqualityDeleteTargetData {
        data_file_path: target.data_file_path.clone(),
        data_file_size: target.data_file_size,
        data_file_first_row_id: target.data_file_first_row_id,
        data_file_sequence_number: target.data_file_sequence_number,
    }
}

fn lower_delta_delete_side_payload_from_native(
    payload: Option<&plan::IcebergDeltaDeleteSidePlan>,
) -> Result<Option<DeltaScanDeleteSidePayload>, String> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    Ok(Some(DeltaScanDeleteSidePayload {
        base_data_file_lineage: lower_novarocks_base_lineage_map(&payload.base_data_file_lineage),
        previous_data_file_lineage: lower_novarocks_base_lineage_map(
            &payload.previous_data_file_lineage,
        ),
        previous_delete_visibility_data_files: payload
            .previous_delete_visibility_data_files
            .iter()
            .map(lower_novarocks_delete_visibility_data_file)
            .collect::<Result<Vec<_>, _>>()?,
        previously_deleted_positions_per_file: payload
            .previously_deleted_positions_per_file
            .iter()
            .map(|(path, positions)| (path.clone(), positions.positions.clone()))
            .collect(),
        deleted_data_file_paths: payload.deleted_data_file_paths.iter().cloned().collect(),
    }))
}

fn lower_novarocks_base_lineage_map(
    input: &HashMap<String, plan::IcebergDeltaBaseDataFileLineage>,
) -> HashMap<String, BaseDataFileLineage> {
    input
        .iter()
        .map(|(path, lineage)| {
            (
                path.clone(),
                BaseDataFileLineage {
                    first_row_id: lineage.first_row_id,
                    data_sequence_number: lineage.data_sequence_number,
                },
            )
        })
        .collect()
}

fn lower_novarocks_delete_visibility_data_file(
    file: &plan::IcebergDeltaDeleteVisibilityDataFile,
) -> Result<crate::connector::iceberg::changes::DeleteVisibilityDataFileDescriptor, String> {
    Ok(
        crate::connector::iceberg::changes::DeleteVisibilityDataFileDescriptor {
            path: file.path.clone(),
            size: file.size,
            first_row_id: file.first_row_id,
            data_sequence_number: file.data_sequence_number,
            delete_files: file
                .delete_files
                .iter()
                .map(lower_novarocks_delete_visibility_delete_file)
                .collect::<Result<Vec<_>, _>>()?,
        },
    )
}

fn lower_novarocks_delete_visibility_delete_file(
    file: &plan::IcebergDeltaDeleteVisibilityDeleteFile,
) -> Result<crate::connector::iceberg::changes::DeleteVisibilityDeleteFileDescriptor, String> {
    Ok(
        crate::connector::iceberg::changes::DeleteVisibilityDeleteFileDescriptor {
            path: file.path.clone(),
            file_format: lower_novarocks_delete_file_format(file.file_format)?,
            file_content: lower_novarocks_delete_file_content(file.file_content)?,
            length: file.length,
            content_offset: file.content_offset,
            content_size_in_bytes: file.content_size_in_bytes,
        },
    )
}

fn lower_novarocks_delete_file_format(
    format: i32,
) -> Result<crate::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat, String> {
    match plan::IcebergDeltaDeleteFileFormat::try_from(format)
        .map_err(|_| format!("IcebergDeltaTable unsupported delete file format {format}"))?
    {
        plan::IcebergDeltaDeleteFileFormat::Unspecified => {
            Err("IcebergDeltaTable delete file format is unspecified".to_string())
        }
        plan::IcebergDeltaDeleteFileFormat::Parquet => {
            Ok(crate::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat::Parquet)
        }
        plan::IcebergDeltaDeleteFileFormat::Puffin => {
            Ok(crate::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat::Puffin)
        }
    }
}

fn lower_novarocks_delete_file_content(
    content: i32,
) -> Result<crate::connector::iceberg::changes::DeleteVisibilityDeleteFileContent, String> {
    match plan::IcebergDeltaDeleteFileContent::try_from(content)
        .map_err(|_| format!("IcebergDeltaTable unsupported delete file content {content}"))?
    {
        plan::IcebergDeltaDeleteFileContent::Unspecified => {
            Err("IcebergDeltaTable delete file content is unspecified".to_string())
        }
        plan::IcebergDeltaDeleteFileContent::Position => {
            Ok(crate::connector::iceberg::changes::DeleteVisibilityDeleteFileContent::Position)
        }
        plan::IcebergDeltaDeleteFileContent::Equality => {
            Ok(crate::connector::iceberg::changes::DeleteVisibilityDeleteFileContent::Equality)
        }
    }
}

fn build_delta_delete_side_from_payload(
    payload: Option<DeltaScanDeleteSidePayload>,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<Option<DeltaScanDeleteSide>, String> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    let mut previously_deleted_positions_per_file = HashMap::new();
    for (path, positions) in payload.previously_deleted_positions_per_file {
        let mut bitmap = roaring::RoaringTreemap::new();
        for pos in positions {
            bitmap.insert(pos);
        }
        previously_deleted_positions_per_file.insert(path, bitmap);
    }
    let previous_delete_visibility =
        crate::engine::delete_flow::load_existing_delete_visibility_from_descriptors(
            &payload.previous_delete_visibility_data_files,
            object_store_config,
        )?;
    Ok(Some(DeltaScanDeleteSide {
        base_data_file_lineage: payload.base_data_file_lineage,
        previous_delete_visibility,
        previously_deleted_positions_per_file,
        previous_data_file_lineage: payload.previous_data_file_lineage,
        deleted_data_file_paths: payload.deleted_data_file_paths,
    }))
}

fn scan_output_columns(scan: &plan::ScanNode) -> Result<Vec<common::OutputColumn>, String> {
    if scan.columns.is_empty() {
        return Err("ScanNode columns are empty".to_string());
    }
    if scan.required_columns.is_empty() {
        return Ok(scan.columns.clone());
    }

    let required = scan
        .required_columns
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let output_columns = scan
        .columns
        .iter()
        .filter(|column| required.contains(&column.name.to_ascii_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    if output_columns.is_empty() {
        return Err(format!(
            "ScanNode required_columns {:?} do not match any scan columns",
            scan.required_columns
        ));
    }
    Ok(output_columns)
}

#[derive(Clone, Debug)]
struct ScanReadPlan {
    output_layout: super::layout::Layout,
    output_schema: ChunkSchemaRef,
    read_layout: super::layout::Layout,
    read_schema: ChunkSchemaRef,
    parquet_schema: ChunkSchemaRef,
    read_columns: Vec<String>,
    read_slot_ids: Vec<SlotId>,
    slot_kinds: Vec<ParquetSlotKind>,
    variant_path_columns: Vec<VariantPathSpec>,
    iceberg_virtual: IcebergVirtualSpec,
}

#[derive(Clone, Debug, Default)]
struct NativeVariantPathPlan {
    specs: Vec<VariantPathSpec>,
    output_slot_ids: HashSet<SlotId>,
}

#[derive(Clone, Debug)]
struct PredicateColumnRef {
    column_id: u32,
    name: Option<String>,
    r#type: Option<common::TypeDesc>,
    nullable: bool,
}

fn scan_read_plan(
    scan: &plan::ScanNode,
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
) -> Result<ScanReadPlan, String> {
    let output_layout = layout_from_output_columns(output_columns)?;
    let mut variant_path_plan =
        parse_native_scan_variant_path_columns(scan, table, output_columns)?;
    let output_schema = iceberg_chunk_schema_from_output_columns_with_variants(
        table,
        output_columns,
        &variant_path_plan,
    )?;

    let mut scan_columns = output_columns.to_vec();
    let mut scan_names = output_columns
        .iter()
        .map(|col| col.name.clone())
        .collect::<HashSet<_>>();
    let mut scan_slots = output_columns
        .iter()
        .map(|col| col.column_id)
        .collect::<HashSet<_>>();
    let mut physical_read_columns = Vec::new();
    let mut read_names = HashSet::new();
    let mut read_slots = HashSet::new();
    let mut iceberg_virtual = IcebergVirtualSpec::default();
    for col in output_columns {
        if variant_path_plan
            .output_slot_ids
            .contains(&SlotId::new(col.column_id))
        {
            continue;
        }
        if record_iceberg_virtual_column(table, col, &mut iceberg_virtual)? {
            continue;
        }
        push_physical_read_column(
            &mut physical_read_columns,
            &mut read_names,
            &mut read_slots,
            col.clone(),
        )?;
    }
    let mut next_hidden_column_id = output_columns
        .iter()
        .map(|col| col.column_id)
        .max()
        .unwrap_or(0)
        .saturating_add(1);

    let predicate_refs = scan_predicate_column_refs(&scan.predicates)?;
    let predicate_refs_by_name = predicate_refs
        .values()
        .filter_map(|col| col.name.as_ref().map(|name| (name.clone(), col)))
        .collect::<HashMap<_, _>>();
    let required_names = scan
        .required_columns
        .iter()
        .cloned()
        .collect::<HashSet<_>>();

    for required in &scan.required_columns {
        if scan_names.contains(required) || read_names.contains(required) {
            continue;
        }
        let col = if let Some(pred_col) = predicate_refs_by_name.get(required) {
            output_column_from_predicate_ref(pred_col)?
        } else {
            let hidden_id = allocate_hidden_column_id(&mut next_hidden_column_id, &scan_slots)?;
            output_column_from_table_def(scan, required, hidden_id)?
        };
        push_scan_column(
            table,
            &mut scan_columns,
            &mut scan_names,
            &mut scan_slots,
            &mut physical_read_columns,
            &mut read_names,
            &mut read_slots,
            &mut iceberg_virtual,
            col,
        )?;
    }

    for pred_col in predicate_refs.values() {
        if scan_slots.contains(&pred_col.column_id) {
            continue;
        }
        let name = pred_col.name.as_ref().ok_or_else(|| {
            format!(
                "ScanNode predicate column_id={} is not an output column and does not carry a column name",
                pred_col.column_id
            )
        })?;
        if !required_names.is_empty() && !required_names.contains(name) {
            return Err(format!(
                "ScanNode predicate column {} is not listed in required_columns",
                name
            ));
        }
        push_scan_column(
            table,
            &mut scan_columns,
            &mut scan_names,
            &mut scan_slots,
            &mut physical_read_columns,
            &mut read_names,
            &mut read_slots,
            &mut iceberg_virtual,
            output_column_from_predicate_ref(pred_col)?,
        )?;
    }

    ensure_native_variant_source_read_columns(
        scan,
        &mut variant_path_plan,
        &mut physical_read_columns,
        &mut read_names,
        &mut read_slots,
        &mut scan_slots,
        &mut next_hidden_column_id,
    )?;
    ensure_virtual_only_scan_has_row_count_carrier(
        &mut scan_columns,
        &mut scan_names,
        &mut scan_slots,
        &mut physical_read_columns,
        &mut read_names,
        &mut read_slots,
        &mut next_hidden_column_id,
        &iceberg_virtual,
    )?;

    let read_layout = layout_from_output_columns(&scan_columns)?;
    let read_schema = iceberg_chunk_schema_from_output_columns_with_variants(
        table,
        &scan_columns,
        &variant_path_plan,
    )?;
    let parquet_schema = iceberg_chunk_schema_from_output_columns(table, &physical_read_columns)?;
    let read_slot_ids = physical_read_columns
        .iter()
        .map(|col| SlotId::new(col.column_id))
        .collect::<Vec<_>>();
    let slot_kinds = physical_read_columns
        .iter()
        .map(parquet_slot_kind_from_native_column)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ScanReadPlan {
        output_layout,
        output_schema,
        read_layout,
        read_schema,
        parquet_schema,
        read_columns: physical_read_columns
            .into_iter()
            .map(|col| col.name)
            .collect(),
        read_slot_ids,
        slot_kinds,
        variant_path_columns: variant_path_plan.specs,
        iceberg_virtual,
    })
}

fn push_physical_read_column(
    read_columns: &mut Vec<common::OutputColumn>,
    read_names: &mut HashSet<String>,
    read_slots: &mut HashSet<u32>,
    col: common::OutputColumn,
) -> Result<(), String> {
    if !read_slots.insert(col.column_id) {
        return Err(format!(
            "ScanNode read columns contain duplicate column_id={}",
            col.column_id
        ));
    }
    if !read_names.insert(col.name.clone()) {
        return Err(format!(
            "ScanNode read columns contain duplicate column name {}",
            col.name
        ));
    }
    read_columns.push(col);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn push_scan_column(
    table: &plan::IcebergTableInfo,
    scan_columns: &mut Vec<common::OutputColumn>,
    scan_names: &mut HashSet<String>,
    scan_slots: &mut HashSet<u32>,
    physical_read_columns: &mut Vec<common::OutputColumn>,
    read_names: &mut HashSet<String>,
    read_slots: &mut HashSet<u32>,
    iceberg_virtual: &mut IcebergVirtualSpec,
    col: common::OutputColumn,
) -> Result<(), String> {
    if !scan_names.insert(col.name.clone()) {
        return Err(format!("ScanNode duplicate read column name {}", col.name));
    }
    if !scan_slots.insert(col.column_id) {
        return Err(format!(
            "ScanNode duplicate read column id {} for {}",
            col.column_id, col.name
        ));
    }
    if !record_iceberg_virtual_column(table, &col, iceberg_virtual)? {
        push_physical_read_column(physical_read_columns, read_names, read_slots, col.clone())?;
    }
    scan_columns.push(col);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn ensure_virtual_only_scan_has_row_count_carrier(
    scan_columns: &mut Vec<common::OutputColumn>,
    scan_names: &mut HashSet<String>,
    scan_slots: &mut HashSet<u32>,
    physical_read_columns: &mut Vec<common::OutputColumn>,
    read_names: &mut HashSet<String>,
    read_slots: &mut HashSet<u32>,
    next_hidden_column_id: &mut u32,
    iceberg_virtual: &IcebergVirtualSpec,
) -> Result<(), String> {
    if !physical_read_columns.is_empty() || iceberg_virtual.is_empty() {
        return Ok(());
    }
    let column_id = allocate_hidden_column_id(next_hidden_column_id, scan_slots)?;
    let column = iceberg_virtual_count_column(column_id);
    if !scan_names.insert(column.name.clone()) {
        return Err(format!(
            "ScanNode duplicate read column name {}",
            column.name
        ));
    }
    if !scan_slots.insert(column.column_id) {
        return Err(format!(
            "ScanNode duplicate read column id {} for {}",
            column.column_id, column.name
        ));
    }
    scan_columns.push(column.clone());
    push_physical_read_column(physical_read_columns, read_names, read_slots, column)
}

fn iceberg_virtual_count_column(column_id: u32) -> common::OutputColumn {
    common::OutputColumn {
        column_id,
        name: "___count___".to_string(),
        r#type: Some(common::TypeDesc {
            kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                r#type: common::PrimitiveType::Boolean as i32,
                len: None,
                precision: None,
                scale: None,
                time_unit: None,
            })),
        }),
        nullable: false,
        is_internal: true,
    }
}

fn parse_native_scan_variant_path_columns(
    scan: &plan::ScanNode,
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
) -> Result<NativeVariantPathPlan, String> {
    if scan.variant_columns.is_empty() {
        return Ok(NativeVariantPathPlan::default());
    }
    let table_def = scan
        .table
        .as_ref()
        .ok_or_else(|| "ScanNode table missing".to_string())?;
    let output_by_slot = output_columns
        .iter()
        .map(|col| (SlotId::new(col.column_id), col))
        .collect::<HashMap<_, _>>();
    let scan_by_slot = scan
        .columns
        .iter()
        .map(|col| (SlotId::new(col.column_id), col))
        .collect::<HashMap<_, _>>();
    let mut plan = NativeVariantPathPlan::default();

    for (idx, column) in scan.variant_columns.iter().enumerate() {
        let source_slot_id = SlotId::new(column.source_column_id);
        let output_slot_id = SlotId::new(column.synthetic_column_id);
        if source_slot_id == output_slot_id {
            return Err(format!(
                "ScanNode variant_columns[{idx}] source_column_id must differ from synthetic_column_id"
            ));
        }

        let source_name =
            required_native_variant_path_string(idx, "source_column", &column.source_column)?;
        let output_name =
            required_native_variant_path_string(idx, "synthetic_column", &column.synthetic_column)?;
        let canonical_path =
            required_native_variant_path_string(idx, "canonical_path", &column.canonical_path)?;
        validate_native_variant_path_column_path(idx, &canonical_path)?;

        let source_scan_column = scan_by_slot.get(&source_slot_id).ok_or_else(|| {
            format!(
                "ScanNode variant_columns[{idx}] source_column_id={source_slot_id} is not a scan column"
            )
        })?;
        if source_scan_column.name != source_name {
            return Err(format!(
                "ScanNode variant_columns[{idx}] source_column={source_name:?} does not match source_column_id={source_slot_id} name {:?}",
                source_scan_column.name
            ));
        }
        let source_table_column = table_def
            .columns
            .iter()
            .find(|col| col.name == source_name)
            .ok_or_else(|| {
                format!(
                    "ScanNode variant_columns[{idx}] source_column={source_name:?} is not in table column definitions"
                )
            })?;
        let source_type = column_def_data_type(source_table_column).map_err(|err| {
            format!(
                "ScanNode variant_columns[{idx}] source_column={source_name:?} type error: {err}"
            )
        })?;
        if !matches!(source_type, DataType::LargeBinary) {
            return Err(format!(
                "ScanNode variant_columns[{idx}] source_column={source_name:?} expects VARIANT/LargeBinary, got {:?}",
                source_type
            ));
        }
        let source_field_id = iceberg_schema_field_id(table, &source_name).ok_or_else(|| {
            format!(
                "ScanNode variant_columns[{idx}] source_column={source_name:?} is missing from Iceberg schema"
            )
        })?;

        let output_column = output_by_slot.get(&output_slot_id).ok_or_else(|| {
            format!(
                "ScanNode variant_columns[{idx}] synthetic_column_id={output_slot_id} is not an output column"
            )
        })?;
        if output_column.name != output_name {
            return Err(format!(
                "ScanNode variant_columns[{idx}] synthetic_column={output_name:?} does not match synthetic_column_id={output_slot_id} name {:?}",
                output_column.name
            ));
        }
        let output_type = output_column_data_type(output_column).map_err(|err| {
            format!(
                "ScanNode variant_columns[{idx}] synthetic_column={output_name:?} type error: {err}"
            )
        })?;
        let requested_type_desc = column
            .requested_type
            .as_ref()
            .ok_or_else(|| format!("ScanNode variant_columns[{idx}] missing requested_type"))?;
        let requested_type = super::decode_type(requested_type_desc).map_err(|err| {
            format!("ScanNode variant_columns[{idx}] requested_type decode failed: {err}")
        })?;
        if !is_supported_native_variant_path_requested_type(&requested_type) {
            return Err(format!(
                "ScanNode variant_columns[{idx}] unsupported requested_type {:?} for synthetic_column_id={output_slot_id}",
                requested_type
            ));
        }
        if requested_type != output_type {
            return Err(format!(
                "ScanNode variant_columns[{idx}] requested_type {:?} does not match synthetic_column_id={output_slot_id} type {:?}",
                requested_type, output_type
            ));
        }
        if !plan.output_slot_ids.insert(output_slot_id) {
            return Err(format!(
                "ScanNode duplicate variant_columns synthetic_column_id={output_slot_id}"
            ));
        }

        plan.specs.push(VariantPathSpec {
            source_slot_id,
            source_read_slot_id: source_slot_id,
            output_slot_id,
            source_field_id: Some(source_field_id),
            source_name: source_name.clone(),
            output_name: output_name.clone(),
            source_field: Field::new(source_name, source_type, source_table_column.nullable),
            output_field: Field::new(output_name, output_type, output_column.nullable),
            canonical_path,
            requested_type,
            strict: column.strict,
        });
    }

    Ok(plan)
}

#[allow(clippy::too_many_arguments)]
fn ensure_native_variant_source_read_columns(
    scan: &plan::ScanNode,
    plan: &mut NativeVariantPathPlan,
    physical_read_columns: &mut Vec<common::OutputColumn>,
    read_names: &mut HashSet<String>,
    read_slots: &mut HashSet<u32>,
    scan_slots: &mut HashSet<u32>,
    next_hidden_column_id: &mut u32,
) -> Result<(), String> {
    if plan.specs.is_empty() {
        return Ok(());
    }

    let mut reserved_slots = scan_slots.clone();
    reserved_slots.extend(plan.specs.iter().map(|spec| spec.source_slot_id.as_u32()));
    reserved_slots.extend(plan.specs.iter().map(|spec| spec.output_slot_id.as_u32()));

    for spec in &mut plan.specs {
        if let Some(read_col) = physical_read_columns.iter().find(|col| {
            SlotId::new(col.column_id) == spec.source_slot_id || col.name == spec.source_name
        }) {
            spec.source_read_slot_id = SlotId::new(read_col.column_id);
            continue;
        }

        let hidden_id = allocate_hidden_column_id(next_hidden_column_id, &reserved_slots)?;
        reserved_slots.insert(hidden_id);
        scan_slots.insert(hidden_id);
        let source_col = output_column_from_table_def(scan, &spec.source_name, hidden_id)?;
        push_physical_read_column(physical_read_columns, read_names, read_slots, source_col)?;
        spec.source_read_slot_id = SlotId::new(hidden_id);
    }

    Ok(())
}

fn required_native_variant_path_string(
    idx: usize,
    field_name: &str,
    value: &str,
) -> Result<String, String> {
    value
        .trim()
        .is_empty()
        .then(|| format!("ScanNode variant_columns[{idx}] missing {field_name}"))
        .map_or_else(|| Ok(value.trim().to_string()), Err)
}

fn validate_native_variant_path_column_path(
    idx: usize,
    canonical_path: &str,
) -> Result<(), String> {
    let parsed = crate::exec::variant::parse_variant_path(canonical_path).map_err(|err| {
        format!("ScanNode variant_columns[{idx}] invalid canonical_path={canonical_path:?}: {err}")
    })?;
    if parsed.segments.is_empty() {
        return Err(format!(
            "ScanNode variant_columns[{idx}] canonical_path={canonical_path:?} must reference at least one object key"
        ));
    }
    if parsed.segments.iter().any(|segment| {
        !matches!(
            segment,
            crate::exec::variant::VariantPathSegment::ObjectKey(_)
        )
    }) {
        return Err(format!(
            "ScanNode variant_columns[{idx}] canonical_path={canonical_path:?} only supports object-key path segments"
        ));
    }
    Ok(())
}

fn is_supported_native_variant_path_requested_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean | DataType::Int64 | DataType::Float64 | DataType::Utf8 | DataType::Date32
    )
}

fn column_def_data_type(column: &plan::ColumnDef) -> Result<DataType, String> {
    let desc = column
        .logical_type
        .as_ref()
        .or(column.data_type.as_ref())
        .ok_or_else(|| format!("column {} type missing", column.name))?;
    super::decode_type(desc)
}

fn output_column_data_type(column: &common::OutputColumn) -> Result<DataType, String> {
    let desc = column
        .r#type
        .as_ref()
        .ok_or_else(|| format!("output column {} type missing", column.name))?;
    super::decode_type(desc)
}

fn parquet_slot_kind_from_native_column(
    column: &common::OutputColumn,
) -> Result<ParquetSlotKind, String> {
    let data_type = output_column_data_type(column)?;
    if matches!(data_type, DataType::LargeBinary) {
        Ok(ParquetSlotKind::Variant)
    } else {
        Ok(ParquetSlotKind::Regular)
    }
}

fn iceberg_schema_field_id(table: &plan::IcebergTableInfo, name: &str) -> Option<i32> {
    table
        .schema
        .as_ref()
        .and_then(|schema| schema.fields.iter().find(|field| field.name == name))
        .map(|field| field.field_id)
}

fn record_iceberg_virtual_column(
    table: &plan::IcebergTableInfo,
    col: &common::OutputColumn,
    spec: &mut IcebergVirtualSpec,
) -> Result<bool, String> {
    let Some(field) = iceberg_virtual_projected_field(table, col)? else {
        return Ok(false);
    };
    let slot_id = SlotId::new(col.column_id);
    if crate::exec::row_position::is_iceberg_file_path(&col.name) {
        if spec.file_path_slot.replace(slot_id).is_some() {
            return Err("ScanNode duplicate Iceberg _file virtual column".to_string());
        }
        spec.file_path_field = Some(field);
        return Ok(true);
    }
    if crate::exec::row_position::is_iceberg_row_pos(&col.name) {
        if spec.row_pos_slot.replace(slot_id).is_some() {
            return Err("ScanNode duplicate Iceberg _pos virtual column".to_string());
        }
        spec.row_pos_field = Some(field);
        return Ok(true);
    }
    if crate::exec::row_position::is_iceberg_row_id(&col.name) {
        if spec.row_id_slot.replace(slot_id).is_some() {
            return Err("ScanNode duplicate Iceberg _row_id virtual column".to_string());
        }
        spec.row_id_field = Some(field);
        return Ok(true);
    }
    if crate::exec::row_position::is_iceberg_last_updated_sequence_number(&col.name) {
        if spec.last_updated_seq_slot.replace(slot_id).is_some() {
            return Err(
                "ScanNode duplicate Iceberg _last_updated_sequence_number virtual column"
                    .to_string(),
            );
        }
        spec.last_updated_seq_field = Some(field);
        return Ok(true);
    }
    if crate::exec::row_position::is_change_op(&col.name) {
        if spec.change_op_slot.replace(slot_id).is_some() {
            return Err("ScanNode duplicate Iceberg __change_op virtual column".to_string());
        }
        spec.change_op_field = Some(field);
        return Ok(true);
    }
    Ok(false)
}

fn iceberg_virtual_projected_field(
    table: &plan::IcebergTableInfo,
    col: &common::OutputColumn,
) -> Result<Option<Field>, String> {
    if iceberg_schema_has_field(table, &col.name) {
        return Ok(None);
    }
    let desc = col
        .r#type
        .as_ref()
        .ok_or_else(|| format!("ScanNode output column {} type missing", col.name))?;
    let data_type = super::decode_type(desc)?;
    if crate::exec::row_position::is_iceberg_file_path(&col.name) {
        if !matches!(data_type, DataType::Utf8) {
            return Err(format!(
                "ScanNode Iceberg _file virtual column expects Utf8, got {:?}",
                data_type
            ));
        }
        return Ok(Some(Field::new(col.name.clone(), data_type, col.nullable)));
    }
    if crate::exec::row_position::is_iceberg_row_pos(&col.name) {
        if !matches!(data_type, DataType::Int64) {
            return Err(format!(
                "ScanNode Iceberg _pos virtual column expects Int64, got {:?}",
                data_type
            ));
        }
        return Ok(Some(Field::new(col.name.clone(), data_type, col.nullable)));
    }
    if crate::exec::row_position::is_iceberg_row_id(&col.name) {
        if !matches!(data_type, DataType::Int64) {
            return Err(format!(
                "ScanNode Iceberg _row_id virtual column expects Int64, got {:?}",
                data_type
            ));
        }
        return Ok(Some(iceberg_virtual_field_with_field_id(
            col,
            data_type,
            crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID,
        )));
    }
    if crate::exec::row_position::is_iceberg_last_updated_sequence_number(&col.name) {
        if !matches!(data_type, DataType::Int64) {
            return Err(format!(
                "ScanNode Iceberg _last_updated_sequence_number virtual column expects Int64, got {:?}",
                data_type
            ));
        }
        return Ok(Some(iceberg_virtual_field_with_field_id(
            col,
            data_type,
            crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        )));
    }
    if crate::exec::row_position::is_change_op(&col.name) {
        if !matches!(data_type, DataType::Int8) {
            return Err(format!(
                "ScanNode Iceberg __change_op virtual column expects Int8, got {:?}",
                data_type
            ));
        }
        return Ok(Some(Field::new(col.name.clone(), data_type, col.nullable)));
    }
    Ok(None)
}

fn iceberg_virtual_field_with_field_id(
    col: &common::OutputColumn,
    data_type: DataType,
    field_id: i32,
) -> Field {
    Field::new(col.name.clone(), data_type, col.nullable).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        field_id.to_string(),
    )]))
}

fn iceberg_schema_has_field(table: &plan::IcebergTableInfo, name: &str) -> bool {
    table
        .schema
        .as_ref()
        .is_some_and(|schema| schema.fields.iter().any(|field| field.name == name))
}

fn allocate_hidden_column_id(next: &mut u32, used: &HashSet<u32>) -> Result<u32, String> {
    loop {
        let id = *next;
        *next = next
            .checked_add(1)
            .ok_or_else(|| "ScanNode hidden read column id overflow".to_string())?;
        if !used.contains(&id) {
            return Ok(id);
        }
    }
}

fn output_column_from_predicate_ref(
    col: &PredicateColumnRef,
) -> Result<common::OutputColumn, String> {
    let name = col.name.clone().ok_or_else(|| {
        format!(
            "ScanNode predicate column_id={} requires a column name for hidden read binding",
            col.column_id
        )
    })?;
    Ok(common::OutputColumn {
        column_id: col.column_id,
        name,
        r#type: col.r#type.clone(),
        nullable: col.nullable,
        is_internal: false,
    })
}

fn output_column_from_table_def(
    scan: &plan::ScanNode,
    name: &str,
    column_id: u32,
) -> Result<common::OutputColumn, String> {
    let table = scan
        .table
        .as_ref()
        .ok_or_else(|| "ScanNode table missing".to_string())?;
    let column = table
        .columns
        .iter()
        .chain(table.iceberg_row_lineage_metadata_columns.iter())
        .find(|col| col.name == name)
        .ok_or_else(|| {
            format!("ScanNode required column {name} is not in table column definitions")
        })?;
    let ty = column
        .logical_type
        .as_ref()
        .or(column.data_type.as_ref())
        .ok_or_else(|| format!("ScanNode required column {name} type missing"))?
        .clone();
    Ok(common::OutputColumn {
        column_id,
        name: column.name.clone(),
        r#type: Some(ty),
        nullable: column.nullable,
        is_internal: true,
    })
}

fn scan_predicate_column_refs(
    predicates: &[crate::proto::expr::Expr],
) -> Result<BTreeMap<u32, PredicateColumnRef>, String> {
    let mut refs = BTreeMap::new();
    for predicate in predicates {
        collect_predicate_column_refs(predicate, &mut refs)?;
    }
    Ok(refs)
}

fn collect_predicate_column_refs(
    expr: &crate::proto::expr::Expr,
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), String> {
    use crate::proto::expr::expr::Kind;

    let Some(kind) = expr.kind.as_ref() else {
        return Ok(());
    };
    match kind {
        Kind::ColumnRef(col) => {
            let next = PredicateColumnRef {
                column_id: col.column_id,
                name: col.column.clone(),
                r#type: expr.r#type.clone(),
                nullable: expr.nullable,
            };
            if let Some(prev) = refs.insert(col.column_id, next.clone()) {
                if prev.name != next.name {
                    return Err(format!(
                        "ScanNode predicate column_id={} has inconsistent names {:?} and {:?}",
                        col.column_id, prev.name, next.name
                    ));
                }
            }
        }
        Kind::Literal(_) | Kind::LambdaParamRef(_) => {}
        Kind::BinaryOp(binary) => {
            collect_optional_box_expr(&binary.left, refs)?;
            collect_optional_box_expr(&binary.right, refs)?;
        }
        Kind::UnaryOp(unary) => collect_optional_box_expr(&unary.operand, refs)?,
        Kind::FunctionCall(call) => collect_expr_list(&call.args, refs)?,
        Kind::AggregateCall(call) => {
            collect_expr_list(&call.args, refs)?;
            collect_sort_items(&call.order_by, refs)?;
        }
        Kind::WindowCall(call) => {
            collect_expr_list(&call.args, refs)?;
            collect_expr_list(&call.partition_by, refs)?;
            collect_sort_items(&call.order_by, refs)?;
        }
        Kind::Cast(cast) => collect_optional_box_expr(&cast.operand, refs)?,
        Kind::IsNull(is_null) => collect_optional_box_expr(&is_null.operand, refs)?,
        Kind::InList(in_list) => {
            collect_optional_box_expr(&in_list.operand, refs)?;
            collect_expr_list(&in_list.list, refs)?;
        }
        Kind::Between(between) => {
            collect_optional_box_expr(&between.operand, refs)?;
            collect_optional_box_expr(&between.low, refs)?;
            collect_optional_box_expr(&between.high, refs)?;
        }
        Kind::Like(like) => {
            collect_optional_box_expr(&like.operand, refs)?;
            collect_optional_box_expr(&like.pattern, refs)?;
        }
        Kind::CaseExpr(case_expr) => {
            collect_optional_box_expr(&case_expr.operand, refs)?;
            for branch in &case_expr.when_then {
                collect_optional_expr(&branch.when, refs)?;
                collect_optional_expr(&branch.then, refs)?;
            }
            collect_optional_box_expr(&case_expr.else_expr, refs)?;
        }
        Kind::IsTruth(is_truth) => collect_optional_box_expr(&is_truth.operand, refs)?,
        Kind::Lambda(lambda) => collect_optional_box_expr(&lambda.body, refs)?,
        Kind::Nested(nested) => collect_optional_box_expr(&nested.inner, refs)?,
    }
    Ok(())
}

fn collect_optional_box_expr(
    expr: &Option<Box<crate::proto::expr::Expr>>,
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), String> {
    if let Some(expr) = expr.as_ref() {
        collect_predicate_column_refs(expr, refs)?;
    }
    Ok(())
}

fn collect_optional_expr(
    expr: &Option<crate::proto::expr::Expr>,
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), String> {
    if let Some(expr) = expr.as_ref() {
        collect_predicate_column_refs(expr, refs)?;
    }
    Ok(())
}

fn collect_expr_list(
    exprs: &[crate::proto::expr::Expr],
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), String> {
    for expr in exprs {
        collect_predicate_column_refs(expr, refs)?;
    }
    Ok(())
}

fn collect_sort_items(
    items: &[crate::proto::expr::SortItem],
    refs: &mut BTreeMap<u32, PredicateColumnRef>,
) -> Result<(), String> {
    for item in items {
        collect_optional_expr(&item.expr, refs)?;
    }
    Ok(())
}

fn iceberg_chunk_schema_from_output_columns(
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
) -> Result<ChunkSchemaRef, String> {
    iceberg_chunk_schema_from_output_columns_with_variants(
        table,
        output_columns,
        &NativeVariantPathPlan::default(),
    )
}

fn iceberg_chunk_schema_from_output_columns_with_variants(
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
    variant_path_plan: &NativeVariantPathPlan,
) -> Result<ChunkSchemaRef, String> {
    let slot_ids = output_columns
        .iter()
        .map(|col| SlotId::new(col.column_id))
        .collect::<Vec<_>>();
    let arrow_schema = iceberg_arrow_schema_from_output_columns_with_variants(
        table,
        output_columns,
        variant_path_plan,
    )?;
    ChunkSchema::try_ref_from_schema_and_slot_ids(arrow_schema.as_ref(), &slot_ids)
}

fn iceberg_arrow_schema_from_output_columns(
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
) -> Result<std::sync::Arc<Schema>, String> {
    iceberg_arrow_schema_from_output_columns_with_variants(
        table,
        output_columns,
        &NativeVariantPathPlan::default(),
    )
}

fn iceberg_arrow_schema_from_output_columns_with_variants(
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
    variant_path_plan: &NativeVariantPathPlan,
) -> Result<std::sync::Arc<Schema>, String> {
    let descriptor = iceberg_table_descriptor(table)?;
    let variant_output_fields = variant_path_plan
        .specs
        .iter()
        .map(|spec| (spec.output_slot_id, spec.output_field.clone()))
        .collect::<HashMap<_, _>>();
    let mut fields = Vec::with_capacity(output_columns.len());
    for col in output_columns {
        if let Some(field) = variant_output_fields.get(&SlotId::new(col.column_id)) {
            fields.push(field.clone());
            continue;
        }
        if let Some(field) = iceberg_virtual_projected_field(table, col)? {
            fields.push(field);
            continue;
        }
        let desc = col
            .r#type
            .as_ref()
            .ok_or_else(|| format!("ScanNode output column {} type missing", col.name))?;
        let projected = build_projected_output_schema(
            &descriptor,
            &[IcebergArrowColumn {
                name: col.name.clone(),
                data_type: super::decode_type(desc)?,
                nullable: col.nullable,
            }],
        )?
        .ok_or_else(|| "IcebergDataFiles table schema missing".to_string())?;
        fields.push(projected.field(0).clone());
    }
    Ok(std::sync::Arc::new(Schema::new(fields)))
}

fn iceberg_table_descriptor(
    table: &plan::IcebergTableInfo,
) -> Result<IcebergTableDescriptor, String> {
    let schema = table
        .schema
        .as_ref()
        .ok_or_else(|| "IcebergDataFiles table schema missing".to_string())?;
    Ok(IcebergTableDescriptor {
        columns: Vec::new(),
        iceberg_schema: Some(IcebergSchemaDescriptor {
            fields: schema
                .fields
                .iter()
                .map(iceberg_schema_field_descriptor)
                .collect(),
        }),
        equality_delete_schema: None,
        partition_info: Vec::new(),
        current_snapshot_id: table.current_snapshot_id,
        serialized_metadata: table.serialized_metadata.clone(),
    })
}

fn iceberg_schema_field_descriptor(
    field: &plan::IcebergSchemaFieldDef,
) -> IcebergSchemaFieldDescriptor {
    IcebergSchemaFieldDescriptor {
        name: field.name.clone(),
        field_id: Some(field.field_id),
        children: field
            .children
            .iter()
            .map(iceberg_schema_field_descriptor)
            .collect(),
        initial_default_json: field.initial_default_json.clone(),
    }
}

fn maybe_project_data_scan_output(
    node_id: i32,
    scan_lowered: LoweredNode,
    read_plan: ScanReadPlan,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    if read_plan.read_layout.order() == read_plan.output_layout.order() {
        return Ok(LoweredNode {
            node: scan_lowered.node,
            layout: read_plan.output_layout,
            output_schema: read_plan.output_schema,
        });
    }
    let exprs = read_plan
        .output_layout
        .order()
        .iter()
        .map(|slot_id| {
            let slot = read_plan.read_schema.slot(*slot_id).ok_or_else(|| {
                format!("ScanNode projection references missing read slot {slot_id}")
            })?;
            Ok(arena.push_typed(ExprNode::SlotId(*slot_id), slot.data_type().clone()))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(scan_lowered.node),
                node_id,
                is_subordinate: true,
                exprs,
                expr_slot_ids: read_plan.output_layout.order().to_vec(),
                expr_slot_schemas: Some(read_plan.output_schema.slots().to_vec()),
                output_indices: None,
                output_chunk_schema: read_plan.output_schema.clone(),
            }),
        },
        layout: read_plan.output_layout,
        output_schema: read_plan.output_schema,
    })
}

fn scan_batch_size(
    query_options: Option<&crate::runtime::query_options::QueryOptions>,
) -> Result<usize, String> {
    let Some(value) = query_options.and_then(|opts| opts.batch_size) else {
        return Ok(4096);
    };
    let batch_size = usize::try_from(value).map_err(|_| {
        format!("native ScanNode query_options.batch_size must be positive, got {value}")
    })?;
    if batch_size == 0 {
        return Err("native ScanNode query_options.batch_size must be positive".to_string());
    }
    Ok(batch_size)
}

fn decode_file_scan_ranges(
    node_id: i32,
    table: &plan::IcebergTableInfo,
    ranges: &[novarocks::ScanRangeParams],
) -> Result<Vec<FileScanRange>, String> {
    ranges
        .iter()
        .enumerate()
        .map(|(idx, range)| {
            if range.has_more.unwrap_or(false) {
                return Err(format!(
                    "ScanNode node_id={node_id} range {idx} has_more is not supported by native lowering"
                ));
            }
            if range.empty.unwrap_or(false) {
                Ok(None)
            } else {
                decode_file_scan_range(node_id, table, idx, range).map(Some)
            }
        })
        .collect::<Result<Vec<_>, String>>()
        .map(|ranges| ranges.into_iter().flatten().collect())
}

fn decode_file_scan_range(
    node_id: i32,
    table: &plan::IcebergTableInfo,
    idx: usize,
    range: &novarocks::ScanRangeParams,
) -> Result<FileScanRange, String> {
    if range.has_more.unwrap_or(false) {
        return Err(format!(
            "ScanNode node_id={node_id} range {idx} has_more is not supported by native lowering"
        ));
    }
    let Some(novarocks::scan_range::Kind::File(file)) =
        range.range.as_ref().and_then(|range| range.kind.as_ref())
    else {
        return Err(format!(
            "ScanNode node_id={node_id} range {idx} expected file range"
        ));
    };
    if !file.file_format.eq_ignore_ascii_case("PARQUET") {
        return Err(format!(
            "ScanNode node_id={node_id} range {idx} unsupported file_format {}; only PARQUET is supported",
            file.file_format
        ));
    }
    let path = file_range_path(table, file)?;
    let file_len = nonnegative_u64(file.file_length, "file_length")?;
    let offset = nonnegative_u64(file.offset, "offset")?;
    if offset > file_len {
        return Err(format!(
            "ScanNode node_id={node_id} range {idx} offset {} exceeds file_length {}",
            file.offset, file.file_length
        ));
    }
    let length = if file.length > 0 {
        nonnegative_u64(file.length, "length")?
    } else {
        file_len - offset
    };
    let mut delete_files = decode_delete_files(node_id, idx, &file.delete_files)?;
    if let Some(dv) = file.deletion_vector_descriptor.as_ref() {
        delete_files.push(decode_deletion_vector_descriptor(node_id, idx, dv)?);
    }
    Ok(FileScanRange {
        path,
        file_len,
        offset,
        length,
        scan_range_id: i32::try_from(idx)
            .map_err(|_| format!("ScanNode node_id={node_id} range index overflow"))?,
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        ivm_change_op: decode_change_op(node_id, idx, file.change_op)?,
        included_positions: if file.included_positions.is_empty() {
            None
        } else {
            Some(file.included_positions.clone())
        },
        external_datacache: file_external_datacache(file),
        delete_files,
        iceberg_file_pruning: file_pruning_metadata_from_native(
            node_id,
            idx,
            table,
            &file.file_pruning_min_max_values,
        )?,
    })
}

fn decode_change_op(node_id: i32, idx: usize, value: Option<i32>) -> Result<Option<i8>, String> {
    value
        .map(|value| {
            let change_op = i8::try_from(value).map_err(|_| {
                format!("ScanNode node_id={node_id} range {idx} change_op {value} exceeds i8 range")
            })?;
            crate::exec::change_op::validate_change_op_value(change_op)?;
            Ok(change_op)
        })
        .transpose()
}

fn file_pruning_metadata_from_native(
    node_id: i32,
    range_idx: usize,
    table: &plan::IcebergTableInfo,
    values: &HashMap<i32, novarocks::FilePruningMinMaxValue>,
) -> Result<Option<IcebergFilePruningMetadata>, String> {
    if values.is_empty() {
        return Ok(None);
    }
    let Some(schema) = table.schema.as_ref() else {
        return Ok(None);
    };
    let mut columns = HashMap::new();
    for (ordinal, value) in values {
        let ordinal_usize = usize::try_from(*ordinal).map_err(|_| {
            format!(
                "ScanNode node_id={node_id} range {range_idx} file pruning ordinal {ordinal} must be non-negative"
            )
        })?;
        let Some(field) = schema.fields.get(ordinal_usize) else {
            return Err(format!(
                "ScanNode node_id={node_id} range {range_idx} file pruning ordinal {ordinal} exceeds Iceberg schema field count {}",
                schema.fields.len()
            ));
        };
        let Some(stats) = column_stats_from_native_min_max_value(node_id, range_idx, value)? else {
            continue;
        };
        columns.insert(field.name.clone(), stats);
    }
    if columns.is_empty() {
        Ok(None)
    } else {
        Ok(Some(IcebergFilePruningMetadata { columns }))
    }
}

fn column_stats_from_native_min_max_value(
    node_id: i32,
    range_idx: usize,
    value: &novarocks::FilePruningMinMaxValue,
) -> Result<Option<IcebergColumnStats>, String> {
    let (lower_bound, upper_bound) = match value.value_kind {
        1 => {
            let lower = bool_bound_to_byte(value.min_int_value.ok_or_else(|| {
                format!(
                    "ScanNode node_id={node_id} range {range_idx} bool file pruning min_int_value missing"
                )
            })?)?;
            let upper = bool_bound_to_byte(value.max_int_value.ok_or_else(|| {
                format!(
                    "ScanNode node_id={node_id} range {range_idx} bool file pruning max_int_value missing"
                )
            })?)?;
            (vec![lower], vec![upper])
        }
        2 => (
            value
                .min_int_value
                .ok_or_else(|| {
                    format!(
                        "ScanNode node_id={node_id} range {range_idx} int file pruning min_int_value missing"
                    )
                })?
                .to_le_bytes()
                .to_vec(),
            value
                .max_int_value
                .ok_or_else(|| {
                    format!(
                        "ScanNode node_id={node_id} range {range_idx} int file pruning max_int_value missing"
                    )
                })?
                .to_le_bytes()
                .to_vec(),
        ),
        3 => {
            let lower = value.min_float_value.ok_or_else(|| {
                format!(
                    "ScanNode node_id={node_id} range {range_idx} float file pruning min_float_value missing"
                )
            })?;
            let upper = value.max_float_value.ok_or_else(|| {
                format!(
                    "ScanNode node_id={node_id} range {range_idx} float file pruning max_float_value missing"
                )
            })?;
            if lower.is_nan() || upper.is_nan() {
                return Ok(None);
            }
            (lower.to_le_bytes().to_vec(), upper.to_le_bytes().to_vec())
        }
        0 => {
            return Err(format!(
                "ScanNode node_id={node_id} range {range_idx} file pruning value_kind is unspecified"
            ));
        }
        other => {
            return Err(format!(
                "ScanNode node_id={node_id} range {range_idx} unsupported file pruning value_kind {other}"
            ));
        }
    };

    Ok(Some(IcebergColumnStats {
        null_count: Some(if value.has_null { 1 } else { 0 }),
        value_count: None,
        column_size: None,
        lower_bound: Some(lower_bound),
        upper_bound: Some(upper_bound),
    }))
}

fn bool_bound_to_byte(value: i64) -> Result<u8, String> {
    match value {
        0 => Ok(0),
        1 => Ok(1),
        _ => Err(format!(
            "bool file pruning bound must be 0 or 1, got {value}"
        )),
    }
}

fn file_range_path(
    table: &plan::IcebergTableInfo,
    file: &novarocks::FileScanRange,
) -> Result<String, String> {
    if let Some(path) = file.full_path.as_deref()
        && !path.is_empty()
    {
        return Ok(path.to_string());
    }
    let Some(relative_path) = file
        .relative_path
        .as_deref()
        .filter(|path| !path.is_empty())
    else {
        return Err("file range missing full_path/relative_path".to_string());
    };
    if table.location.is_empty() {
        return Err("HDFS relative_path requires Iceberg table location".to_string());
    }
    Ok(format!(
        "{}/{}",
        table.location.trim_end_matches('/'),
        relative_path.trim_start_matches('/')
    ))
}

fn nonnegative_u64(value: i64, field: &str) -> Result<u64, String> {
    u64::try_from(value)
        .map_err(|_| format!("file range {field} must be non-negative, got {value}"))
}

fn file_external_datacache(
    file: &novarocks::FileScanRange,
) -> Option<ExternalDataCacheRangeOptions> {
    file.datacache_options
        .as_ref()
        .map(|opts| ExternalDataCacheRangeOptions {
            modification_time: file.modification_time,
            enable_populate_datacache: opts.enable_populate_datacache,
            datacache_priority: opts.priority,
            candidate_node: None,
        })
}

fn decode_delete_files(
    node_id: i32,
    range_idx: usize,
    delete_files: &[novarocks::IcebergDeleteFile],
) -> Result<Vec<IcebergDeleteFileSpec>, String> {
    delete_files
        .iter()
        .enumerate()
        .map(|(idx, file)| {
            let path = file.full_path.clone().ok_or_else(|| {
                format!("ScanNode node_id={node_id} range {range_idx} delete file {idx} full_path missing")
            })?;
            let file_format = match file.file_format.to_ascii_uppercase().as_str() {
                "PARQUET" => IcebergFileFormat::Parquet,
                other => {
                    return Err(format!(
                        "ScanNode node_id={node_id} range {range_idx} delete file {idx} unsupported file_format {other}"
                    ));
                }
            };
            let file_content = match file.file_content.to_ascii_uppercase().as_str() {
                "POSITION_DELETES" => IcebergFileContent::PositionDeletes,
                "EQUALITY_DELETES" => IcebergFileContent::EqualityDeletes,
                other => {
                    return Err(format!(
                        "ScanNode node_id={node_id} range {range_idx} delete file {idx} unsupported file_content {other}"
                    ));
                }
            };
            let length = file
                .length
                .map(|value| nonnegative_u64(value, "delete_file.length"))
                .transpose()?;
            Ok(IcebergDeleteFileSpec {
                path,
                file_format,
                file_content,
                length,
                content_offset: None,
                content_size_in_bytes: None,
            })
        })
        .collect()
}

fn decode_deletion_vector_descriptor(
    node_id: i32,
    range_idx: usize,
    dv: &novarocks::DeletionVectorDescriptor,
) -> Result<IcebergDeleteFileSpec, String> {
    let path = dv
        .path_or_inline_dv
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| {
            format!(
                "ScanNode node_id={node_id} range {range_idx} deletion vector is missing path_or_inline_dv"
            )
        })?
        .to_string();
    let offset = dv.offset.ok_or_else(|| {
        format!(
            "ScanNode node_id={node_id} range {range_idx} deletion vector {path} is missing offset"
        )
    })?;
    let size = dv.size_in_bytes.ok_or_else(|| {
        format!(
            "ScanNode node_id={node_id} range {range_idx} deletion vector {path} is missing size_in_bytes"
        )
    })?;
    Ok(IcebergDeleteFileSpec::puffin_position_delete(
        path, None, offset, size,
    ))
}

fn decode_metadata_scan_ranges(
    ranges: &[novarocks::ScanRangeParams],
) -> Result<Vec<IcebergMetadataScanRange>, String> {
    ranges
        .iter()
        .enumerate()
        .map(|(idx, range)| {
            if range.has_more.unwrap_or(false) {
                return Err(format!(
                    "IcebergMetadataTable range {idx} has_more is not supported by native lowering"
                ));
            }
            if range.empty.unwrap_or(false) {
                return Ok(None);
            }
            let Some(novarocks::scan_range::Kind::File(file)) =
                range.range.as_ref().and_then(|range| range.kind.as_ref())
            else {
                return Err(format!(
                    "IcebergMetadataTable range {idx} expected file range"
                ));
            };
            Ok(Some(IcebergMetadataScanRange {
                path: file.full_path.clone().unwrap_or_default(),
                serialized_split: file.serialized_split.clone().unwrap_or_default(),
            }))
        })
        .collect::<Result<Vec<_>, String>>()
        .map(|ranges| ranges.into_iter().flatten().collect())
}

fn metadata_output_columns(
    output_columns: &[common::OutputColumn],
) -> Result<Vec<IcebergMetadataOutputColumn>, String> {
    output_columns
        .iter()
        .map(|col| {
            let data_type = col
                .r#type
                .as_ref()
                .ok_or_else(|| format!("metadata output column {} type missing", col.name))
                .and_then(super::decode_type)?;
            Ok(IcebergMetadataOutputColumn {
                name: col.name.clone(),
                slot_id: SlotId::new(col.column_id),
                data_type,
                nullable: col.nullable,
            })
        })
        .collect()
}

fn metadata_table_type(value: i32) -> Result<IcebergMetadataTableType, String> {
    match plan::IcebergMetadataTableType::try_from(value)
        .map_err(|_| format!("unknown Iceberg metadata table type {value}"))?
    {
        plan::IcebergMetadataTableType::Files => Ok(IcebergMetadataTableType::Files),
        plan::IcebergMetadataTableType::Manifests => Ok(IcebergMetadataTableType::Manifests),
        plan::IcebergMetadataTableType::LogicalIcebergMetadata => {
            Ok(IcebergMetadataTableType::LogicalIcebergMetadata)
        }
        plan::IcebergMetadataTableType::Snapshots => Ok(IcebergMetadataTableType::Snapshots),
        plan::IcebergMetadataTableType::History => Ok(IcebergMetadataTableType::History),
        plan::IcebergMetadataTableType::Refs => Ok(IcebergMetadataTableType::Refs),
        plan::IcebergMetadataTableType::Partitions => Ok(IcebergMetadataTableType::Partitions),
        plan::IcebergMetadataTableType::Unspecified => {
            Err("Iceberg metadata table type is unspecified".to_string())
        }
    }
}

fn lower_scan_predicate(
    scan: &plan::ScanNode,
    arena: &mut ExprArena,
    layout: &super::layout::Layout,
) -> Result<Option<crate::exec::expr::ExprId>, String> {
    let mut predicate = None;
    for (idx, expr) in scan.predicates.iter().enumerate() {
        let expr_id = lower_proto_expr(expr, arena, layout)
            .map_err(|err| format!("ScanNode predicate {idx}: {err}"))?;
        predicate = Some(match predicate {
            Some(prev) => arena.push_typed(ExprNode::And(prev, expr_id), DataType::Boolean),
            None => expr_id,
        });
    }
    Ok(predicate)
}

fn parse_scan_limit(limit: i64) -> Result<Option<usize>, String> {
    if limit == -1 {
        Ok(None)
    } else if limit < 0 {
        Err(format!("ScanNode limit must be -1 or >= 0, got {limit}"))
    } else {
        Ok(Some(limit as usize))
    }
}

fn resolve_cloud_object_store_config(
    cloud_properties: &HashMap<String, String>,
) -> Result<Option<ObjectStoreConfig>, String> {
    let props = cloud_properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let Some(credentials) = ObjectStoreCredentials::optional_from_aws_s3_properties(
        ObjectStoreCredentialsSource::AwsS3Properties,
        &props,
    )?
    else {
        return Ok(None);
    };
    let mut cfg = credentials.to_object_store_config();
    apply_object_store_runtime_defaults(&mut cfg);
    Ok(Some(cfg))
}

fn table_location_map(table: &plan::IcebergTableInfo) -> HashMap<i64, String> {
    let mut locations = HashMap::new();
    if !table.location.is_empty() {
        locations.insert(i64::from(table.schema_id), table.location.clone());
    }
    locations
}

fn unsupported_scan_source(source: &str) -> Result<LoweredNode, String> {
    Err(format!("{source} native scan source is not implemented"))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::datatypes::DataType;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::*;
    use crate::connector::{ConnectorRegistry, ScanConnector};
    use crate::exec::node::ExecNodeKind;
    use crate::exec::node::scan::ScanMorsel;
    use crate::proto::{common, expr};
    use crate::sql::codegen::proto_encode::types::encode_type;

    fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    fn output_column(column_id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            is_internal: false,
        }
    }

    fn column_def(name: &str, data_type: DataType) -> plan::ColumnDef {
        plan::ColumnDef {
            name: name.to_string(),
            data_type: Some(type_desc(&data_type)),
            nullable: true,
            write_default_json: None,
            logical_type: None,
        }
    }

    fn schema_field(field_id: i32, name: &str) -> plan::IcebergSchemaFieldDef {
        plan::IcebergSchemaFieldDef {
            field_id,
            name: name.to_string(),
            initial_default_json: None,
            write_default_json: None,
            children: Vec::new(),
        }
    }

    fn table_info() -> plan::IcebergTableInfo {
        plan::IcebergTableInfo {
            catalog: "rest".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: None,
            current_snapshot_id: Some(1),
            schema_id: 7,
            location: "s3://bucket/warehouse/db/t".to_string(),
            schema: Some(plan::IcebergSchemaDef {
                fields: vec![schema_field(10, "id"), schema_field(11, "flag")],
            }),
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn variant_table_info() -> plan::IcebergTableInfo {
        plan::IcebergTableInfo {
            schema: Some(plan::IcebergSchemaDef {
                fields: vec![schema_field(101, "v")],
            }),
            ..table_info()
        }
    }

    fn scan_node(source: plan::scan_source::Kind) -> plan::DistributedNode {
        let columns = vec![output_column(1, "id", DataType::Int64)];
        scan_node_with(columns, Vec::new(), Vec::new(), source)
    }

    fn scan_node_with(
        columns: Vec<common::OutputColumn>,
        predicates: Vec<expr::Expr>,
        required_columns: Vec<String>,
        source: plan::scan_source::Kind,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: columns.clone(),
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    database: "db".to_string(),
                    table: Some(plan::TableDef {
                        name: "t".to_string(),
                        columns: vec![
                            column_def("id", DataType::Int64),
                            column_def("flag", DataType::Boolean),
                        ],
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        source: Some(plan::ScanSource { kind: Some(source) }),
                    }),
                    alias: None,
                    columns,
                    predicates,
                    required_columns,
                    dict_columns: Vec::new(),
                    variant_columns: Vec::new(),
                    mv_rewritten_from: None,
                })),
            })),
        }
    }

    fn variant_scan_node() -> plan::DistributedNode {
        variant_scan_node_with_source_ids(1, 1)
    }

    fn variant_scan_node_with_source_ids(
        variant_source_column_id: u32,
        scan_source_column_id: u32,
    ) -> plan::DistributedNode {
        let output_columns = vec![output_column(2, "__nr_var_v_0", DataType::Int64)];
        let scan_columns = vec![
            output_column(scan_source_column_id, "v", DataType::LargeBinary),
            output_column(2, "__nr_var_v_0", DataType::Int64),
        ];
        plan::DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    database: "db".to_string(),
                    table: Some(plan::TableDef {
                        name: "t".to_string(),
                        columns: vec![
                            column_def("v", DataType::LargeBinary),
                            column_def("__nr_var_v_0", DataType::Int64),
                        ],
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        source: Some(plan::ScanSource {
                            kind: Some(plan::scan_source::Kind::IcebergDataFiles(
                                plan::IcebergDataFiles {
                                    table: Some(variant_table_info()),
                                    files: Vec::new(),
                                    cloud_properties: HashMap::new(),
                                    binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
                                },
                            )),
                        }),
                    }),
                    alias: None,
                    columns: scan_columns,
                    predicates: Vec::new(),
                    required_columns: vec!["__nr_var_v_0".to_string()],
                    dict_columns: Vec::new(),
                    variant_columns: vec![plan::ScanVariantColumn {
                        source_column_id: variant_source_column_id,
                        source_column: "v".to_string(),
                        synthetic_column_id: 2,
                        synthetic_column: "__nr_var_v_0".to_string(),
                        canonical_path: "$.a.b".to_string(),
                        requested_type: Some(type_desc(&DataType::Int64)),
                        strict: true,
                    }],
                    mv_rewritten_from: None,
                })),
            })),
        }
    }

    #[derive(Clone)]
    struct CapturingHdfsConnector {
        captured: Arc<Mutex<Option<HdfsScanConfig>>>,
    }

    impl ScanConnector for CapturingHdfsConnector {
        fn name(&self) -> &'static str {
            "hdfs"
        }

        fn create_scan_node(
            &self,
            cfg: ScanConfig,
        ) -> Result<crate::exec::node::scan::ScanNode, String> {
            let ScanConfig::Hdfs(cfg) = cfg else {
                return Err("capturing hdfs connector received non-HDFS config".to_string());
            };
            let cfg = *cfg;
            *self.captured.lock().expect("captured hdfs config lock") = Some(cfg.clone());
            Ok(crate::exec::node::scan::ScanNode::new(Arc::new(
                crate::connector::hdfs::HdfsScanOp::new(cfg),
            )))
        }
    }

    fn capturing_hdfs_registry() -> (Arc<ConnectorRegistry>, Arc<Mutex<Option<HdfsScanConfig>>>) {
        let captured = Arc::new(Mutex::new(None));
        let mut registry = ConnectorRegistry::default();
        registry.register_scan_connector(Arc::new(CapturingHdfsConnector {
            captured: Arc::clone(&captured),
        }));
        (Arc::new(registry), captured)
    }

    fn column_ref(column_id: u32, name: &str, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: Some(name.to_string()),
            })),
        }
    }

    fn file_range() -> novarocks::ScanRangeParams {
        novarocks::ScanRangeParams {
            range: Some(novarocks::ScanRange {
                kind: Some(novarocks::scan_range::Kind::File(
                    novarocks::FileScanRange {
                        file_format: "PARQUET".to_string(),
                        full_path: Some("s3://bucket/warehouse/db/t/data-1.parquet".to_string()),
                        relative_path: None,
                        table_id: None,
                        offset: 0,
                        length: 10,
                        file_length: 10,
                        delete_files: Vec::new(),
                        deletion_vector_descriptor: None,
                        first_row_id: None,
                        data_sequence_number: None,
                        modification_time: None,
                        datacache_options: None,
                        included_positions: Vec::new(),
                        serialized_split: None,
                        use_iceberg_jni_metadata_reader: false,
                        change_op: None,
                        file_pruning_min_max_values: HashMap::new(),
                    },
                )),
            }),
            volume_id: None,
            empty: None,
            has_more: None,
        }
    }

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Int64)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    fn greater_than(left: expr::Expr, right: expr::Expr) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Boolean)),
            nullable: true,
            kind: Some(expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: expr::BinaryOp::Gt as i32,
                left: Some(Box::new(left)),
                right: Some(Box::new(right)),
            }))),
        }
    }

    fn iceberg_delta_table_source() -> plan::scan_source::Kind {
        plan::scan_source::Kind::IcebergDeltaTable(plan::IcebergDeltaTable {
            table: Some(table_info()),
            from_snapshot_id: 1,
            to_snapshot_id: 2,
            delta_plan: Some(plan::IcebergDeltaScanPlan {
                table_location: "file:///tmp/novarocks-delta-table".to_string(),
                data_columns: vec![plan::IcebergDeltaDataColumn {
                    name: "id".to_string(),
                    field_id: 10,
                }],
                cloud_properties: HashMap::new(),
                change_files: vec![plan::IcebergDeltaSourceFile {
                    path: "file:///tmp/novarocks-delta-table/data-1.parquet".to_string(),
                    size: 10,
                    role: plan::IcebergDeltaSourceRole::DataFile as i32,
                    partition_spec_id: Some(0),
                    partition_key: None,
                    first_row_id: Some(100),
                    data_sequence_number: Some(7),
                    row_id_allow_list: Vec::new(),
                    position_deletes: Vec::new(),
                    equality_field_ids: Vec::new(),
                    equality_targets: Vec::new(),
                    deleted_file_visibility: None,
                }],
                delete_side: None,
            }),
        })
    }

    fn iceberg_metadata_table_source() -> plan::scan_source::Kind {
        plan::scan_source::Kind::IcebergMetadataTable(plan::IcebergMetadataTable {
            table: Some(table_info()),
            metadata_table_type: plan::IcebergMetadataTableType::Snapshots as i32,
            serialized_table: "{}".to_string(),
            cloud_properties: HashMap::new(),
            metadata_payload: None,
        })
    }

    fn file_range_with_deletion_vector() -> novarocks::ScanRangeParams {
        let mut range = file_range();
        let Some(novarocks::scan_range::Kind::File(file)) =
            range.range.as_mut().and_then(|range| range.kind.as_mut())
        else {
            panic!("expected file range");
        };
        file.deletion_vector_descriptor = Some(novarocks::DeletionVectorDescriptor {
            storage_type: Some("PUFFIN".to_string()),
            path_or_inline_dv: Some("s3://bucket/warehouse/db/t/delete-1.puffin".to_string()),
            offset: Some(12),
            size_in_bytes: Some(34),
            cardinality: Some(2),
        });
        range
    }

    fn file_range_with_change_op_and_pruning() -> novarocks::ScanRangeParams {
        let mut range = file_range();
        let Some(novarocks::scan_range::Kind::File(file)) =
            range.range.as_mut().and_then(|range| range.kind.as_mut())
        else {
            panic!("expected file range");
        };
        file.change_op = Some(crate::exec::change_op::CHANGE_OP_DELETE.into());
        file.file_pruning_min_max_values = HashMap::from([(
            0,
            novarocks::FilePruningMinMaxValue {
                value_kind: 2,
                has_null: true,
                all_null: false,
                min_int_value: Some(10),
                max_int_value: Some(20),
                min_float_value: None,
                max_float_value: None,
            },
        )]);
        range
    }

    #[test]
    fn lowers_iceberg_data_file_scan_to_scan_node() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();
        let lowered = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower native scan");
        let ExecNodeKind::Scan(scan) = lowered.node.kind else {
            panic!("expected Scan");
        };
        assert_eq!(scan.node_id(), Some(10));
        assert_eq!(scan.output_chunk_schema().slot_ids(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_iceberg_data_file_scan_deletion_vector_to_puffin_delete_file() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range_with_deletion_vector()]);
        let mut arena = ExprArena::default();
        let lowered = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower native scan");
        let ExecNodeKind::Scan(scan) = lowered.node.kind else {
            panic!("expected Scan");
        };
        let morsels = scan.build_morsels().expect("build morsels");
        let [ScanMorsel::FileRange { delete_files, .. }] = morsels.morsels.as_slice() else {
            panic!("expected one file morsel, got {:?}", morsels.morsels);
        };
        assert_eq!(delete_files.len(), 1);
        let dv = &delete_files[0];
        assert_eq!(dv.file_format, IcebergFileFormat::Puffin);
        assert_eq!(dv.file_content, IcebergFileContent::PositionDeletes);
        assert_eq!(
            dv.path,
            "s3://bucket/warehouse/db/t/delete-1.puffin".to_string()
        );
        assert_eq!(dv.content_offset, Some(12));
        assert_eq!(dv.content_size_in_bytes, Some(34));
    }

    #[test]
    fn lowers_iceberg_data_file_scan_change_op_and_pruning_metadata() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range_with_change_op_and_pruning()]);
        let mut arena = ExprArena::default();
        let lowered = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower native scan");
        let ExecNodeKind::Scan(scan) = lowered.node.kind else {
            panic!("expected Scan");
        };
        let morsels = scan.build_morsels().expect("build morsels");
        let [
            ScanMorsel::FileRange {
                ivm_change_op,
                iceberg_file_pruning,
                ..
            },
        ] = morsels.morsels.as_slice()
        else {
            panic!("expected one file morsel, got {:?}", morsels.morsels);
        };
        assert_eq!(
            *ivm_change_op,
            Some(crate::exec::change_op::CHANGE_OP_DELETE)
        );
        let pruning = iceberg_file_pruning
            .as_ref()
            .expect("file pruning metadata");
        let stats = pruning.columns.get("id").expect("id stats");
        assert_eq!(stats.null_count, Some(1));
        assert_eq!(stats.lower_bound, Some(10_i64.to_le_bytes().to_vec()));
        assert_eq!(stats.upper_bound, Some(20_i64.to_le_bytes().to_vec()));
    }

    #[test]
    fn rejects_native_file_pruning_ordinal_outside_iceberg_schema() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let mut range = file_range_with_change_op_and_pruning();
        let Some(novarocks::scan_range::Kind::File(file)) =
            range.range.as_mut().and_then(|range| range.kind.as_mut())
        else {
            panic!("expected file range");
        };
        let value = file
            .file_pruning_min_max_values
            .remove(&0)
            .expect("test pruning value");
        file.file_pruning_min_max_values.insert(2, value);

        let ctx = NodeLoweringContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![range]);
        let mut arena = ExprArena::default();
        let err = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect_err("reject out-of-range file pruning ordinal");
        assert!(
            err.contains("file pruning ordinal 2 exceeds Iceberg schema field count 2"),
            "{err}"
        );
    }

    #[test]
    fn rejects_native_file_pruning_unspecified_value_kind() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let mut range = file_range_with_change_op_and_pruning();
        let Some(novarocks::scan_range::Kind::File(file)) =
            range.range.as_mut().and_then(|range| range.kind.as_mut())
        else {
            panic!("expected file range");
        };
        file.file_pruning_min_max_values
            .get_mut(&0)
            .expect("test pruning value")
            .value_kind = 0;

        let ctx = NodeLoweringContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![range]);
        let mut arena = ExprArena::default();
        let err = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect_err("reject unspecified file pruning value kind");
        assert!(
            err.contains("file pruning value_kind is unspecified"),
            "{err}"
        );
    }

    #[test]
    fn lowers_native_iceberg_scan_variant_path_columns() {
        let node = variant_scan_node();
        let (registry, captured_hdfs) = capturing_hdfs_registry();
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(registry)
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();

        let lowered = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower native scan with variant path column");

        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(2)]);
        let scan = match lowered.node.kind {
            ExecNodeKind::Scan(scan) => scan,
            ExecNodeKind::Project(project) => {
                assert!(project.is_subordinate);
                assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(2)]);
                let ExecNodeKind::Scan(scan) = project.input.kind else {
                    panic!("expected project input scan");
                };
                scan
            }
            other => panic!("expected Scan or Project over Scan, got {other:?}"),
        };
        assert_eq!(scan.output_chunk_schema().slot_ids(), &[SlotId::new(2)]);

        let hdfs_cfg = captured_hdfs
            .lock()
            .expect("captured hdfs config lock")
            .clone()
            .expect("captured hdfs config");
        let Some(FileFormatConfig::Parquet(parquet_cfg)) = hdfs_cfg.format else {
            panic!("expected parquet scan config");
        };
        assert_eq!(parquet_cfg.columns, vec!["v".to_string()]);
        assert_eq!(parquet_cfg.chunk_schema.slot_ids(), &[SlotId::new(3)]);
        assert_eq!(parquet_cfg.variant_path_columns.len(), 1);
        let spec = &parquet_cfg.variant_path_columns[0];
        assert_eq!(spec.source_slot_id, SlotId::new(1));
        assert_eq!(spec.source_read_slot_id, SlotId::new(3));
        assert_eq!(spec.output_slot_id, SlotId::new(2));
        assert_eq!(spec.source_name, "v");
        assert_eq!(spec.output_name, "__nr_var_v_0");
        assert_eq!(spec.canonical_path, "$.a.b");
        assert_eq!(spec.requested_type, DataType::Int64);
        assert!(spec.strict);
        assert_eq!(spec.source_field_id, Some(101));
    }

    #[test]
    fn native_variant_source_hidden_slot_reserves_source_slot_id() {
        let node = variant_scan_node_with_source_ids(3, 3);
        let (registry, captured_hdfs) = capturing_hdfs_registry();
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(registry)
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();

        let lowered = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower native scan with colliding source slot");

        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(2)]);
        let hdfs_cfg = captured_hdfs
            .lock()
            .expect("captured hdfs config lock")
            .clone()
            .expect("captured hdfs config");
        let Some(FileFormatConfig::Parquet(parquet_cfg)) = hdfs_cfg.format else {
            panic!("expected parquet scan config");
        };
        assert_eq!(parquet_cfg.chunk_schema.slot_ids(), &[SlotId::new(4)]);
        let spec = &parquet_cfg.variant_path_columns[0];
        assert_eq!(spec.source_slot_id, SlotId::new(3));
        assert_eq!(spec.source_read_slot_id, SlotId::new(4));
        assert_ne!(spec.source_read_slot_id, spec.source_slot_id);
    }

    #[test]
    fn rejects_native_variant_source_id_name_mismatch() {
        let node = variant_scan_node_with_source_ids(4, 3);
        let (registry, _) = capturing_hdfs_registry();
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(registry)
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();

        let err = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect_err("reject source id/name drift");

        assert!(
            err.contains("source_column_id=4 is not a scan column"),
            "{err}"
        );
    }

    #[test]
    fn lowers_iceberg_delta_table_scan_from_native_payload() {
        let node = scan_node(iceberg_delta_table_source());
        let ctx = NodeLoweringContext::default().with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();
        let lowered = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower native delta scan");
        let ExecNodeKind::IcebergDeltaScan(scan) = lowered.node.kind else {
            panic!("expected IcebergDeltaScan");
        };
        assert_eq!(scan.node_id, 10);
        assert_eq!(scan.base_table_ident.catalog, "rest");
        assert_eq!(scan.from_snapshot_id, 1);
        assert_eq!(scan.to_snapshot_id, 2);
        assert_eq!(scan.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        assert_eq!(scan.change_files.len(), 1);
        assert_eq!(
            scan.change_files[0].path,
            "file:///tmp/novarocks-delta-table/data-1.parquet"
        );
        assert!(matches!(
            scan.change_files[0].role,
            DeltaSourceRole::DataFile
        ));
    }

    #[test]
    fn iceberg_delta_table_empty_instance_ranges_lower_to_empty_values() {
        let node = scan_node(iceberg_delta_table_source());
        let ctx = NodeLoweringContext::default().with_scan_ranges(10, Vec::new());
        let mut arena = ExprArena::default();
        let lowered = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower native delta empty instance");

        let ExecNodeKind::Values(values) = lowered.node.kind else {
            panic!("expected empty Values for empty delta instance");
        };
        assert_eq!(values.node_id, 10);
        assert_eq!(values.chunk.batch.num_rows(), 0);
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_iceberg_delta_table_scan_predicates_to_filter() {
        let node = scan_node_with(
            vec![output_column(1, "id", DataType::Int64)],
            vec![greater_than(
                column_ref(1, "id", DataType::Int64),
                int_literal(10),
            )],
            Vec::new(),
            iceberg_delta_table_source(),
        );
        let ctx = NodeLoweringContext::default().with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();
        let lowered = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower native delta scan with predicate");
        let ExecNodeKind::Filter(filter) = lowered.node.kind else {
            panic!("expected Filter wrapper");
        };
        assert_eq!(filter.node_id, 10);
        assert!(matches!(
            arena.node(filter.predicate),
            Some(ExprNode::Gt(_, _))
        ));
        let ExecNodeKind::IcebergDeltaScan(scan) = filter.input.kind else {
            panic!("expected Filter input IcebergDeltaScan");
        };
        assert_eq!(scan.node_id, 10);
        assert_eq!(scan.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_iceberg_metadata_scan_predicate_to_scan_conjunct() {
        let node = scan_node_with(
            vec![output_column(1, "snapshot_id", DataType::Int64)],
            vec![greater_than(
                column_ref(1, "snapshot_id", DataType::Int64),
                int_literal(0),
            )],
            Vec::new(),
            iceberg_metadata_table_source(),
        );
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();

        let lowered = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower metadata scan with predicate");
        let ExecNodeKind::Scan(scan) = lowered.node.kind else {
            panic!("expected Scan");
        };
        assert!(scan.conjunct_predicate().is_some());
    }

    #[test]
    fn metadata_scan_empty_instance_ranges_produce_no_morsels() {
        let ranges = decode_metadata_scan_ranges(&[]).expect("decode empty metadata ranges");

        assert!(
            ranges.is_empty(),
            "empty per-instance metadata ranges must not synthesize work"
        );
    }

    #[test]
    fn metadata_scan_placeholder_range_produces_one_morsel() {
        let ranges = decode_metadata_scan_ranges(&[file_range()])
            .expect("decode placeholder metadata range");

        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].path, "s3://bucket/warehouse/db/t/data-1.parquet");
    }

    #[test]
    fn iceberg_data_file_scan_output_schema_carries_field_ids() {
        let schema = iceberg_arrow_schema_from_output_columns(
            &table_info(),
            &[output_column(1, "id", DataType::Int64)],
        )
        .expect("iceberg schema");
        assert_eq!(
            schema.field(0).metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"10".to_string())
        );
    }

    #[test]
    fn iceberg_data_file_scan_accepts_file_and_pos_virtual_columns() {
        let columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "_file", DataType::Utf8),
            output_column(3, "_pos", DataType::Int64),
            output_column(4, "_row_id", DataType::Int64),
            output_column(5, "_last_updated_sequence_number", DataType::Int64),
        ];
        let schema = iceberg_arrow_schema_from_output_columns(&table_info(), &columns)
            .expect("iceberg output schema");
        assert_eq!(schema.field(1).name(), "_file");
        assert_eq!(schema.field(2).name(), "_pos");
        assert_eq!(schema.field(3).name(), "_row_id");
        assert_eq!(schema.field(4).name(), "_last_updated_sequence_number");
        assert!(
            !schema
                .field(1)
                .metadata()
                .contains_key(PARQUET_FIELD_ID_META_KEY)
        );
        assert!(
            !schema
                .field(2)
                .metadata()
                .contains_key(PARQUET_FIELD_ID_META_KEY)
        );
        assert_eq!(
            schema.field(3).metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID.to_string())
        );

        let node = scan_node_with(
            columns,
            Vec::new(),
            Vec::new(),
            plan::scan_source::Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            }),
        );
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();
        let lowered = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower native scan");
        let ExecNodeKind::Scan(scan) = lowered.node.kind else {
            panic!("expected Scan");
        };
        assert_eq!(
            scan.output_chunk_schema().slot_ids(),
            &[
                SlotId::new(1),
                SlotId::new(2),
                SlotId::new(3),
                SlotId::new(4),
                SlotId::new(5)
            ]
        );
        let virtual_spec = scan.iceberg_virtual().expect("iceberg virtual spec");
        assert_eq!(virtual_spec.file_path_slot, Some(SlotId::new(2)));
        assert_eq!(virtual_spec.row_pos_slot, Some(SlotId::new(3)));
        assert_eq!(virtual_spec.row_id_slot, Some(SlotId::new(4)));
        assert_eq!(virtual_spec.last_updated_seq_slot, Some(SlotId::new(5)));
    }

    #[test]
    fn iceberg_virtual_only_scan_reads_count_carrier_and_projects_outputs() {
        let node = scan_node_with(
            vec![output_column(4, "_row_id", DataType::Int64)],
            Vec::new(),
            Vec::new(),
            plan::scan_source::Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            }),
        );
        let (registry, captured) = capturing_hdfs_registry();
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(registry)
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();
        let lowered = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower virtual-only native scan");

        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(4)]);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected scan wrapper project");
        };
        assert!(project.is_subordinate);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(4)]);
        let ExecNodeKind::Scan(scan) = project.input.kind else {
            panic!("expected project input scan");
        };
        assert_eq!(
            scan.output_chunk_schema().slot_ids(),
            &[SlotId::new(4), SlotId::new(5)]
        );
        let virtual_spec = scan.iceberg_virtual().expect("iceberg virtual spec");
        assert_eq!(virtual_spec.row_id_slot, Some(SlotId::new(4)));

        let cfg = captured
            .lock()
            .expect("captured hdfs config lock")
            .clone()
            .expect("captured hdfs config");
        let Some(FileFormatConfig::Parquet(parquet_cfg)) = cfg.format else {
            panic!("expected parquet scan config");
        };
        assert_eq!(parquet_cfg.columns, ["___count___".to_string()]);
        assert_eq!(parquet_cfg.chunk_schema.slot_ids(), &[SlotId::new(5)]);
    }

    #[test]
    fn rejects_missing_scan_ranges() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()));
        let mut arena = ExprArena::default();
        let err = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx).unwrap_err();
        assert!(err.contains("missing scan ranges"), "err={err}");
    }

    #[test]
    fn predicate_only_required_column_uses_read_layout_and_projects_outputs() {
        let node = scan_node_with(
            vec![output_column(1, "id", DataType::Int64)],
            vec![column_ref(2, "flag", DataType::Boolean)],
            vec!["id".to_string(), "flag".to_string()],
            plan::scan_source::Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                cloud_properties: HashMap::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            }),
        );
        let ctx = NodeLoweringContext::default()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();
        let lowered = crate::lower::novarocks::lower_proto_node(&node, &mut arena, &ctx)
            .expect("lower native scan");
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(1)]);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected scan wrapper project");
        };
        assert!(project.is_subordinate);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        let ExecNodeKind::Scan(scan) = project.input.kind else {
            panic!("expected project input scan");
        };
        assert_eq!(
            scan.output_chunk_schema().slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
    }
}
