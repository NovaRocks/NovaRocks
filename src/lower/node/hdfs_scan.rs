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

use crate::cache::{CacheOptions, DataCacheManager, ExternalDataCacheRangeOptions};
use crate::common::ids::SlotId;
use crate::connector::iceberg::position_delete::{
    IcebergDeleteFileSpec, convert_scan_range_delete_files,
};
use crate::connector::iceberg::{
    IcebergArrowColumn, IcebergMetadataOutputColumn, IcebergMetadataScanConfig,
    IcebergMetadataScanRange, IcebergMetadataTableType, build_projected_output_schema,
    lookup_iceberg_table_location, snapshot_iceberg_table_locations,
};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::formats::parquet::ParquetReadCachePolicy;
use crate::lower::expr::parse_min_max_conjunct;
use crate::lower::layout::{
    Layout, chunk_schema_for_layout, col_names_from_layout, find_tuple_descriptor,
    layout_from_slot_ids,
};
use crate::lower::node::{Lowered, local_rf_waiting_set};
use crate::lower::type_lowering::primitive_type_from_desc;
use crate::novarocks_config::config as novarocks_app_config;
use crate::novarocks_connectors::{
    ConnectorRegistry, FileFormatConfig, FileScanRange, HdfsScanConfig, OrcScanConfig,
    ParquetScanConfig, ScanConfig,
};
use crate::novarocks_logging::{debug, warn};
use crate::{descriptors, internal_service, plan_nodes, types};

/// Cache Iceberg table locations from descriptor table for later use in HDFS scan lowering.
pub(crate) fn cache_iceberg_table_locations(desc_tbl: Option<&descriptors::TDescriptorTable>) {
    crate::connector::iceberg::cache_iceberg_table_locations(desc_tbl);
}

/// Build a `ChunkSchema` covering only the given slot ids, preserving their
/// order and copying per-slot metadata from `parent`. Used to expose a
/// physical-columns-only schema to the parquet reader when the scan node's
/// output layout also includes synthesized virtual columns.
fn sub_chunk_schema(
    _desc_tbl: &descriptors::TDescriptorTable,
    slot_ids: &[crate::common::ids::SlotId],
    parent: &crate::exec::chunk::ChunkSchemaRef,
) -> Result<crate::exec::chunk::ChunkSchemaRef, String> {
    let mut slots = Vec::with_capacity(slot_ids.len());
    for slot_id in slot_ids {
        let slot = parent
            .slot(*slot_id)
            .ok_or_else(|| format!("parquet chunk schema missing slot_id {}", slot_id))?;
        slots.push(slot.clone());
    }
    Ok(std::sync::Arc::new(
        crate::exec::chunk::ChunkSchema::try_new(slots)?,
    ))
}

fn apply_path_rewrite(ranges: &mut [FileScanRange]) -> Result<(), String> {
    let cfg = match novarocks_app_config() {
        Ok(cfg) => cfg,
        Err(_) => return Ok(()),
    };
    let rewrite = &cfg.runtime.path_rewrite;
    if !rewrite.enable {
        return Ok(());
    }

    let from = rewrite.from_prefix.trim();
    let to = rewrite.to_prefix.trim();
    if from.is_empty() || to.is_empty() {
        return Err(
            "path rewrite enabled but runtime.path_rewrite.from_prefix/to_prefix is empty"
                .to_string(),
        );
    }
    if !to.starts_with('/') {
        return Err(format!(
            "path rewrite to_prefix must be absolute path, got: {}",
            to
        ));
    }

    let from = from.trim_end_matches('/');
    let to = to.trim_end_matches('/');

    let mut matched = 0usize;
    let mut rewritten = Vec::with_capacity(ranges.len());
    for range in ranges.iter() {
        let original = range.path.trim();
        if let Some(rest) = original.strip_prefix(from) {
            let rest = rest.trim_start_matches('/');
            let new_path = if rest.is_empty() {
                to.to_string()
            } else {
                format!("{}/{}", to, rest)
            };
            rewritten.push(Some((original.to_string(), new_path)));
            matched += 1;
        } else {
            rewritten.push(None);
        }
    }

    if matched != ranges.len() {
        let first_unmatched = ranges
            .iter()
            .map(|r| r.path.trim())
            .find(|p| !p.starts_with(from))
            .unwrap_or("<unknown>");
        return Err(format!(
            "path rewrite enabled but not all paths match prefix: prefix={} first_unmatched={}",
            from, first_unmatched
        ));
    }

    for (range, item) in ranges.iter_mut().zip(rewritten.into_iter()) {
        let Some((original, new_path)) = item else {
            continue;
        };
        debug!("HDFS_SCAN path rewrite: {} -> {}", original, new_path);
        range.path = new_path;
    }

    Ok(())
}

fn is_paimon_table(desc_tbl: &descriptors::TDescriptorTable, tuple_id: types::TTupleId) -> bool {
    let Ok(tuple_desc) = find_tuple_descriptor(desc_tbl, tuple_id) else {
        return false;
    };
    let Some(table_id) = tuple_desc.table_id else {
        return false;
    };
    let Some(table_descs) = desc_tbl.table_descriptors.as_ref() else {
        return false;
    };
    let Some(table_desc) = table_descs.iter().find(|t| t.id == table_id) else {
        return false;
    };
    table_desc.paimon_table.is_some()
}

fn find_iceberg_table(
    desc_tbl: &descriptors::TDescriptorTable,
    tuple_id: types::TTupleId,
) -> Option<descriptors::TIcebergTable> {
    let Ok(tuple_desc) = find_tuple_descriptor(desc_tbl, tuple_id) else {
        return None;
    };
    let table_id = tuple_desc.table_id?;
    let table_descs = desc_tbl.table_descriptors.as_ref()?;
    table_descs
        .iter()
        .find(|t| t.id == table_id)
        .and_then(|t| t.iceberg_table.clone())
}

fn parse_true_false(value: &str) -> Option<bool> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("true") || trimmed == "1" {
        return Some(true);
    }
    if trimmed.eq_ignore_ascii_case("false") || trimmed == "0" {
        return Some(false);
    }
    None
}

fn file_cache_flags_from_query_options(
    query_opts: Option<&internal_service::TQueryOptions>,
) -> (bool, bool) {
    // Align with StarRocks BE semantics: cache flags are only effective when
    // FE explicitly carries the corresponding query option.
    let enable_file_metacache = query_opts
        .and_then(|opts| opts.enable_file_metacache)
        .unwrap_or(false);
    let enable_file_pagecache = query_opts
        .and_then(|opts| opts.enable_file_pagecache)
        .unwrap_or(false);
    (enable_file_metacache, enable_file_pagecache)
}

/// Extract an `ObjectStoreConfig` from the cloud properties map attached to
/// `THdfsScanNode.cloud_configuration`.  Returns `None` when any required field is absent
/// so the caller falls back to the shard registry (used by native lake tablets).
fn resolve_cloud_object_store_config<S>(
    cloud_props: Option<&std::collections::BTreeMap<S, S>>,
    ranges: &[FileScanRange],
) -> Option<crate::fs::object_store::ObjectStoreConfig>
where
    S: std::borrow::Borrow<str> + Ord,
{
    let props = cloud_props?;
    let retry_settings =
        crate::fs::object_store::ObjectStoreRetrySettings::from_aws_s3_props(cloud_props);
    let get = |key: &str| {
        props
            .get(key)
            .map(|v| v.borrow().trim())
            .filter(|v| !v.is_empty())
    };

    let endpoint = get("aws.s3.endpoint")
        .or_else(|| get("aws.s3.endpoint_url"))?
        .to_string();
    let access_key_id = get("aws.s3.accessKeyId")
        .or_else(|| get("aws.s3.access_key"))?
        .to_string();
    let access_key_secret = get("aws.s3.accessKeySecret")
        .or_else(|| get("aws.s3.secret_key"))?
        .to_string();
    let region = get("aws.s3.region").map(|v| v.to_string());
    let enable_path_style_access =
        get("aws.s3.enable_path_style_access").and_then(parse_true_false);

    // Derive bucket from the first OSS range path so that normalize_oss_path can
    // validate bucket consistency and strip the correct prefix.
    let bucket = ranges
        .iter()
        .find_map(|r| {
            let p = r.path.trim();
            for scheme in ["oss://", "s3://"] {
                if let Some(rest) = p.strip_prefix(scheme) {
                    let b = rest.split('/').next()?.trim();
                    if !b.is_empty() {
                        return Some(b.to_string());
                    }
                }
            }
            None
        })
        .unwrap_or_default();

    let mut cfg = crate::fs::object_store::ObjectStoreConfig {
        endpoint,
        bucket,
        root: String::new(),
        access_key_id,
        access_key_secret,
        session_token: None,
        enable_path_style_access,
        region,
        retry_max_times: retry_settings.retry_max_times,
        retry_min_delay_ms: retry_settings.retry_min_delay_ms,
        retry_max_delay_ms: retry_settings.retry_max_delay_ms,
        timeout_ms: retry_settings.timeout_ms,
        io_timeout_ms: retry_settings.io_timeout_ms,
    };
    crate::fs::object_store::apply_object_store_runtime_defaults(&mut cfg);
    Some(cfg)
}

pub(crate) fn extract_change_op_from_extended_columns(
    node_id: i32,
    hdfs_range: &plan_nodes::THdfsScanRange,
    change_op_slot: Option<SlotId>,
) -> Result<Option<i8>, String> {
    let Some(slot) = change_op_slot else {
        return Ok(None);
    };
    let slot_id = i32::try_from(slot.as_u32()).map_err(|_| {
        format!("HDFS_SCAN_NODE node_id={node_id} __change_op slot_id={slot} exceeds i32")
    })?;
    let context = || format!("HDFS_SCAN_NODE node_id={node_id} __change_op slot_id={slot_id}");
    let Some(expr) = hdfs_range
        .extended_columns
        .as_ref()
        .and_then(|extended_columns| extended_columns.get(&slot_id))
    else {
        return Ok(None);
    };
    if expr.nodes.len() != 1 {
        return Err(format!(
            "{} expects exactly one INT_LITERAL extended column node, got {}",
            context(),
            expr.nodes.len()
        ));
    }
    let node = &expr.nodes[0];
    if node.node_type != crate::exprs::TExprNodeType::INT_LITERAL {
        return Err(format!(
            "{} expects INT_LITERAL extended column, got {:?}",
            context(),
            node.node_type
        ));
    }
    if node.num_children != 0 {
        return Err(format!(
            "{} INT_LITERAL extended column expects 0 children, got {}",
            context(),
            node.num_children
        ));
    }
    let value = node
        .int_literal
        .as_ref()
        .ok_or_else(|| format!("{} INT_LITERAL missing int payload", context()))?
        .value;
    let value = i8::try_from(value)
        .map_err(|_| format!("{} value {} does not fit in int8", context(), value))?;
    crate::exec::change_op::validate_change_op_value(value)
        .map_err(|e| format!("{} invalid value: {e}", context()))?;
    Ok(Some(value))
}

fn scan_ranges_have_extended_column(
    scan_ranges: &[internal_service::TScanRangeParams],
    slot_id: SlotId,
) -> Result<bool, String> {
    let slot_id = i32::try_from(slot_id.as_u32())
        .map_err(|_| format!("extended column slot_id={slot_id} exceeds i32"))?;
    Ok(scan_ranges.iter().any(|params| {
        params
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .and_then(|range| range.extended_columns.as_ref())
            .is_some_and(|extended_columns| extended_columns.contains_key(&slot_id))
    }))
}

/// Lower a HDFS_SCAN_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_hdfs_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    _tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    query_opts: Option<&internal_service::TQueryOptions>,
    connectors: &ConnectorRegistry,
    mut out_layout: Layout,
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
    let is_paimon = is_paimon_table(desc_tbl, tuple_id);
    let hive_column_names = hdfs.hive_column_names.clone();
    let orc_use_column_names = query_opts
        .and_then(|opts| opts.orc_use_column_names)
        .unwrap_or(false);

    let columns = col_names_from_layout(desc_tbl, &out_layout)?;

    let slot_descs = desc_tbl
        .slot_descriptors
        .as_ref()
        .ok_or_else(|| "missing slot_descriptors in desc_tbl".to_string())?;
    let mut slot_info_map: HashMap<
        SlotId,
        (
            String,
            types::TPrimitiveType,
            arrow::datatypes::DataType,
            bool,
        ),
    > = HashMap::new();
    for s in slot_descs {
        let (Some(parent), Some(id), Some(slot_type)) = (s.parent, s.id, s.slot_type.as_ref())
        else {
            continue;
        };
        if parent != tuple_id {
            continue;
        }
        let name = crate::lower::layout::slot_display_name_from_desc(s);
        let primitive =
            primitive_type_from_desc(slot_type).unwrap_or(types::TPrimitiveType::INVALID_TYPE);
        let arrow_type = crate::lower::type_lowering::arrow_type_from_desc(slot_type)
            .ok_or_else(|| format!("unsupported slot_type for slot_id={id}"))?;
        let nullable = s.is_nullable.unwrap_or(true);
        slot_info_map.insert(
            SlotId::try_from(id)?,
            (name, primitive, arrow_type, nullable),
        );
    }
    let has_row_position_marker_slots = slot_info_map.values().any(|(name, _, _, _)| {
        crate::exec::row_position::is_row_source_id(name)
            || crate::exec::row_position::is_scan_range_id(name)
    });

    let Some(exec_params) = exec_params else {
        return Err("HDFS_SCAN_NODE requires exec_params.per_node_scan_ranges".to_string());
    };
    let scan_ranges = exec_params
        .per_node_scan_ranges
        .get(&node.node_id)
        .ok_or_else(|| format!("missing per_node_scan_ranges for node_id={}", node.node_id))?;

    let mut slot_ids = Vec::with_capacity(out_layout.order.len());
    let mut data_columns = Vec::new();
    let mut data_slot_ids = Vec::new();
    let mut data_slot_types = Vec::new();
    let mut iceberg_projected_columns = Vec::new();

    let mut row_source_slot: Option<SlotId> = None;
    let mut scan_range_slot: Option<SlotId> = None;
    let mut row_id_slot: Option<SlotId> = None;
    let mut row_source_field: Option<arrow::datatypes::Field> = None;
    let mut scan_range_field: Option<arrow::datatypes::Field> = None;
    let mut row_id_field: Option<arrow::datatypes::Field> = None;
    let mut iceberg_virtual_file_slot: Option<SlotId> = None;
    let mut iceberg_virtual_pos_slot: Option<SlotId> = None;
    let mut iceberg_virtual_row_id_slot: Option<SlotId> = None;
    let mut iceberg_virtual_last_updated_seq_slot: Option<SlotId> = None;
    let mut iceberg_virtual_change_op_slot: Option<SlotId> = None;
    let mut iceberg_virtual_file_field: Option<arrow::datatypes::Field> = None;
    let mut iceberg_virtual_pos_field: Option<arrow::datatypes::Field> = None;
    let mut iceberg_virtual_row_id_field: Option<arrow::datatypes::Field> = None;
    let mut iceberg_virtual_last_updated_seq_field: Option<arrow::datatypes::Field> = None;
    let mut iceberg_virtual_change_op_field: Option<arrow::datatypes::Field> = None;

    for (tuple_id, slot_id) in &out_layout.order {
        let slot_id = SlotId::try_from(*slot_id)?;
        let (name, primitive, arrow_type, nullable) = slot_info_map
            .get(&slot_id)
            .ok_or_else(|| format!("missing slot info for tuple_id={tuple_id} slot_id={slot_id}"))?
            .clone();
        slot_ids.push(slot_id);

        if crate::exec::row_position::is_row_source_id(&name) {
            if primitive != types::TPrimitiveType::INT {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} row_source_id slot_id={} expects INT, got {:?}",
                    node.node_id, slot_id, primitive
                ));
            }
            row_source_slot = Some(slot_id);
            row_source_field = Some(arrow::datatypes::Field::new(name, arrow_type, nullable));
            continue;
        }
        if crate::exec::row_position::is_scan_range_id(&name) {
            if primitive != types::TPrimitiveType::INT {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} scan_range_id slot_id={} expects INT, got {:?}",
                    node.node_id, slot_id, primitive
                ));
            }
            scan_range_slot = Some(slot_id);
            scan_range_field = Some(arrow::datatypes::Field::new(name, arrow_type, nullable));
            continue;
        }
        if has_row_position_marker_slots && crate::exec::row_position::is_row_id(&name) {
            if primitive != types::TPrimitiveType::BIGINT {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} row_id slot_id={} expects BIGINT, got {:?}",
                    node.node_id, slot_id, primitive
                ));
            }
            row_id_slot = Some(slot_id);
            row_id_field = Some(arrow::datatypes::Field::new(name, arrow_type, nullable));
            continue;
        }
        if crate::exec::row_position::is_iceberg_file_path(&name) {
            if primitive != types::TPrimitiveType::VARCHAR {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} _file slot_id={} expects VARCHAR, got {:?}",
                    node.node_id, slot_id, primitive
                ));
            }
            iceberg_virtual_file_slot = Some(slot_id);
            iceberg_virtual_file_field =
                Some(arrow::datatypes::Field::new(name, arrow_type, nullable));
            continue;
        }
        if crate::exec::row_position::is_iceberg_row_pos(&name) {
            if primitive != types::TPrimitiveType::BIGINT {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} _pos slot_id={} expects BIGINT, got {:?}",
                    node.node_id, slot_id, primitive
                ));
            }
            iceberg_virtual_pos_slot = Some(slot_id);
            iceberg_virtual_pos_field =
                Some(arrow::datatypes::Field::new(name, arrow_type, nullable));
            continue;
        }

        // Lowering of `_row_id` / `_last_updated_sequence_number` slots into
        // IcebergVirtualSpec is exercised end-to-end by the Task 5 integration
        // tests (e.g. `select_row_id_and_last_updated_seq_on_v3_row_lineage_table`).
        // The synthetic-fixture style used elsewhere in this file is not added
        // here because constructing a valid `TPlanNode` for an iceberg scan
        // requires substantial scaffolding that the integration path already
        // covers more economically.
        if crate::exec::row_position::is_iceberg_row_id(&name) {
            if !matches!(arrow_type, arrow::datatypes::DataType::Int64) {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} _row_id slot_id={} expects BIGINT, got {:?}",
                    node.node_id, slot_id, arrow_type
                ));
            }
            iceberg_virtual_row_id_slot = Some(slot_id);
            iceberg_virtual_row_id_field =
                Some(arrow::datatypes::Field::new(name, arrow_type, nullable));
            continue;
        }

        if crate::exec::row_position::is_iceberg_last_updated_sequence_number(&name) {
            if !matches!(arrow_type, arrow::datatypes::DataType::Int64) {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} _last_updated_sequence_number slot_id={} expects BIGINT, got {:?}",
                    node.node_id, slot_id, arrow_type
                ));
            }
            iceberg_virtual_last_updated_seq_slot = Some(slot_id);
            iceberg_virtual_last_updated_seq_field =
                Some(arrow::datatypes::Field::new(name, arrow_type, nullable));
            continue;
        }
        if crate::exec::row_position::is_change_op(&name)
            && scan_ranges_have_extended_column(scan_ranges, slot_id)?
        {
            if primitive != types::TPrimitiveType::TINYINT {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} __change_op slot_id={} expects TINYINT, got {:?}",
                    node.node_id, slot_id, primitive
                ));
            }
            iceberg_virtual_change_op_slot = Some(slot_id);
            iceberg_virtual_change_op_field =
                Some(arrow::datatypes::Field::new(name, arrow_type, nullable));
            continue;
        }

        data_columns.push(name.clone());
        data_slot_ids.push(slot_id);
        data_slot_types.push(primitive);
        iceberg_projected_columns.push(IcebergArrowColumn {
            name,
            data_type: arrow_type,
            nullable,
        });
    }

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
            Some(crate::exec::row_position::RowPositionSpec {
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
    let needs_first_row_id = row_position_spec.is_some() || iceberg_virtual_row_id_slot.is_some();

    let case_sensitive = hdfs.case_sensitive.unwrap_or(true);
    let mut cache_options = CacheOptions::from_query_options(query_opts)?;
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
        let cfg = novarocks_app_config().map_err(|e| e.to_string())?;
        let cache_cfg = &cfg.runtime.cache;
        if !cache_cfg.datacache_enable {
            warn!(
                "HDFS_SCAN_NODE node_id={} requested datacache (scan={}, populate={}) but runtime.cache.datacache_enable=false; fallback to remote read without datacache",
                node.node_id,
                cache_options.enable_scan_datacache,
                cache_options.enable_populate_datacache
            );
            cache_options.disable_external_datacache();
        } else if DataCacheManager::instance().block_cache().is_none() {
            warn!(
                "HDFS_SCAN_NODE node_id={} requested datacache (scan={}, populate={}) but block cache is unavailable; fallback to remote read without datacache",
                node.node_id,
                cache_options.enable_scan_datacache,
                cache_options.enable_populate_datacache
            );
            cache_options.disable_external_datacache();
        }
    }

    let limit = node.limit;
    let limit = (limit >= 0).then_some(limit as usize);
    let connector_io_tasks_per_scan_operator =
        query_opts.and_then(|opts| opts.connector_io_tasks_per_scan_operator);
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
    // iceberg-rust's `TableMetadata`. The operator constructor itself
    // (`IcebergMetadataScanOp::new`) rejects flavors the native path does
    // not yet implement (Files / Manifests / Partitions /
    // LogicalIcebergMetadata).
    let is_iceberg_metadata_scan = iceberg_metadata_table_type.is_some();
    let mut ranges: Vec<FileScanRange> = Vec::new();
    let mut iceberg_metadata_ranges: Vec<IcebergMetadataScanRange> = Vec::new();
    let mut has_more = false;
    let mut scan_format: Option<descriptors::THdfsFileFormat> = None;
    let mut next_scan_range_id: i32 = 0;
    for p in scan_ranges {
        if p.empty.unwrap_or(false) {
            if p.has_more.unwrap_or(false) {
                has_more = true;
            }
            continue;
        }
        let Some(hdfs_range) = p.scan_range.hdfs_scan_range.as_ref() else {
            continue;
        };
        if is_iceberg_metadata_scan {
            if !hdfs_range.use_iceberg_jni_metadata_reader.unwrap_or(false) {
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
                let loc = lookup_iceberg_table_location(table_id).ok_or_else(|| {
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
        if hdfs_range.use_paimon_jni_reader.unwrap_or(false) {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} does not support Paimon JNI reader; require raw parquet/orc scan ranges",
                node.node_id
            ));
        }
        if hdfs_range
            .paimon_split_info
            .as_ref()
            .is_some_and(|s| !s.is_empty())
            || hdfs_range
                .paimon_predicate_info
                .as_ref()
                .is_some_and(|s| !s.is_empty())
        {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} does not support Paimon split/predicate info; require raw parquet/orc scan ranges",
                node.node_id
            ));
        }
        if hdfs_range.paimon_deletion_file.is_some() {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} does not support deletion files (append-only only)",
                node.node_id
            ));
        }
        let mut iceberg_delete_files = convert_scan_range_delete_files(
            &format!("HDFS_SCAN_NODE node_id={}", node.node_id),
            hdfs_range,
        )?;
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
            iceberg_delete_files.push(IcebergDeleteFileSpec {
                path,
                file_format: descriptors::THdfsFileFormat::UNKNOWN,
                file_content: crate::types::TIcebergFileContent::POSITION_DELETES,
                length: None,
                content_offset: Some(offset),
                content_size_in_bytes: Some(size),
            });
        }
        if hdfs_range
            .delete_column_slot_ids
            .as_ref()
            .is_some_and(|v| !v.is_empty())
        {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} does not support delete columns (append-only only)",
                node.node_id
            ));
        }
        let file_format = hdfs_range.file_format.as_ref().ok_or_else(|| {
            format!(
                "HDFS_SCAN_NODE node_id={} missing file_format in scan range",
                node.node_id
            )
        })?;
        if *file_format != descriptors::THdfsFileFormat::PARQUET
            && *file_format != descriptors::THdfsFileFormat::ORC
        {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} unsupported file_format {:?}",
                node.node_id, file_format
            ));
        }
        if row_position_spec.is_some() && *file_format != descriptors::THdfsFileFormat::PARQUET {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} row position requires PARQUET scan ranges, got {:?}",
                node.node_id, file_format
            ));
        }
        if let Some(prev) = scan_format.as_ref() {
            if prev != file_format {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} mixed file formats: {:?} vs {:?}",
                    node.node_id, prev, file_format
                ));
            }
        } else {
            scan_format = Some(*file_format);
        }
        if is_paimon {
            if file_format != &descriptors::THdfsFileFormat::PARQUET
                && file_format != &descriptors::THdfsFileFormat::ORC
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
        let file_len = hdfs_range.file_length.unwrap_or(0);
        let file_len = if file_len > 0 { file_len as u64 } else { 0 };
        let offset = hdfs_range.offset.unwrap_or(0);
        let offset = if offset >= 0 { offset as u64 } else { 0 };
        let length = hdfs_range.length.unwrap_or(0);
        let mut length = if length > 0 { length as u64 } else { 0 };
        if length == 0 && file_len > offset {
            length = file_len - offset;
        }
        let scan_range_id = if row_position_spec.is_some() {
            let id = next_scan_range_id;
            next_scan_range_id = next_scan_range_id.saturating_add(1);
            id
        } else {
            -1
        };
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
        let external_datacache = {
            let range_datacache_options = hdfs_range.datacache_options.as_ref();
            let candidate_node = hdfs_range
                .candidate_node
                .as_ref()
                .map(|node| node.trim())
                .filter(|node| !node.is_empty())
                .map(|node| node.to_string());
            let options = ExternalDataCacheRangeOptions {
                modification_time: hdfs_range.modification_time,
                enable_populate_datacache: range_datacache_options
                    .and_then(|opts| opts.enable_populate_datacache),
                datacache_priority: range_datacache_options.and_then(|opts| opts.priority),
                candidate_node,
            };
            if options.modification_time.is_some()
                || options.enable_populate_datacache.is_some()
                || options.datacache_priority.is_some()
                || options.candidate_node.is_some()
            {
                // Validate range-level cache options early in lowering.
                let _ = cache_options.with_external_range_options(Some(&options))?;
                Some(options)
            } else {
                None
            }
        };

        // data_sequence_number is populated from THdfsScanRange field 38
        // when the NovaRocks iceberg codegen path (standalone SQL) fills it in.
        // For FE-sent scan ranges that do not carry field 38, this will be
        // None, which is acceptable: the incremental morsel builder also
        // produces None for FE-driven ranges (see build_incremental_morsels).
        let data_sequence_number = hdfs_range.data_sequence_number;
        let ivm_change_op = extract_change_op_from_extended_columns(
            node.node_id,
            hdfs_range,
            iceberg_virtual_change_op_slot,
        )?;
        if iceberg_virtual_change_op_slot.is_some() && ivm_change_op.is_none() {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} __change_op virtual slot requires every scan range to carry extended_columns",
                node.node_id
            ));
        }

        if let Some(fp) = hdfs_range.full_path.as_ref().filter(|s| !s.is_empty()) {
            ranges.push(FileScanRange {
                path: fp.clone(),
                file_len,
                offset,
                length,
                scan_range_id,
                first_row_id,
                data_sequence_number,
                ivm_change_op,
                external_datacache: external_datacache.clone(),
                delete_files: iceberg_delete_files.clone(),
            });
        } else if let Some(rp) = hdfs_range.relative_path.as_ref().filter(|s| !s.is_empty()) {
            let table_id = hdfs_range.table_id.ok_or_else(|| {
                format!(
                    "HDFS_SCAN_NODE node_id={} has relative_path={rp:?} but missing table_id; cannot resolve to full OSS path",
                    node.node_id
                )
            })?;
            let loc = lookup_iceberg_table_location(table_id).ok_or_else(|| {
                format!(
                    "HDFS_SCAN_NODE node_id={} has relative_path={rp:?} but missing cached iceberg location for table_id={table_id}",
                    node.node_id
                )
            })?;
            let base = loc.trim_end_matches('/');
            let rel = rp.trim_start_matches('/');
            ranges.push(FileScanRange {
                path: format!("{base}/{rel}"),
                file_len,
                offset,
                length,
                scan_range_id,
                first_row_id,
                data_sequence_number,
                ivm_change_op,
                external_datacache,
                delete_files: iceberg_delete_files,
            });
        }
    }
    if let Some(metadata_table_type) = iceberg_metadata_table_type {
        let batch_size: usize = query_opts
            .and_then(|opts| opts.batch_size)
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
        let scan = connectors
            .create_scan_node(
                "iceberg",
                crate::connector::ScanConfig::IcebergMetadata(cfg),
            )?
            .with_node_id(node.node_id)
            .with_output_chunk_schema(chunk_schema_for_layout(desc_tbl, &out_layout)?)
            .with_limit(limit)
            .with_connector_io_tasks_per_scan_operator(connector_io_tasks_per_scan_operator)
            .with_accept_empty_scan_ranges(true)
            .with_local_rf_waiting_set(local_rf_waiting_set(node));
        return Ok(Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Scan(scan),
            },
            layout: out_layout,
        });
    }
    let original_range_count = ranges.len();
    apply_path_rewrite(&mut ranges)?;
    let mut enable_page_index = query_opts
        .and_then(|opts| opts.enable_parquet_reader_page_index)
        .unwrap_or(false);

    let mut min_max_predicates = Vec::new();
    if let Some(min_max_conjs) = hdfs.min_max_conjuncts.as_ref() {
        debug!(
            "[Row Group Pruning] parsing {} min_max_conjuncts",
            min_max_conjs.len()
        );
        for conj in min_max_conjs {
            if let Some(pred) = parse_min_max_conjunct(conj, &out_layout)? {
                debug!("[Row Group Pruning] parsed predicate: {:?}", pred);
                min_max_predicates.push(pred);
            }
        }
        if !min_max_predicates.is_empty() {
            debug!(
                "[Row Group Pruning] total {} min_max_predicates ready for row group filtering",
                min_max_predicates.len()
            );
        }
    }
    if row_position_spec.is_some() {
        // When row position is required, we must keep a stable row_id sequence.
        // Page index and row group pruning can skip rows and would corrupt row_id values.
        enable_page_index = false;
        min_max_predicates.clear();
    }

    debug!(
        "HDFS_SCAN creating scan with {} ranges, {} columns",
        ranges.len(),
        data_columns.len()
    );
    debug!("HDFS_SCAN final out_layout.order: {:?}", out_layout.order);
    debug!("HDFS_SCAN final out_layout.index: {:?}", out_layout.index);
    let batch_size: Option<usize> = query_opts
        .and_then(|opts| opts.batch_size)
        .map(|bs| bs as usize)
        .or(Some(4096));

    debug!("HDFS_SCAN using batch_size: {:?}", batch_size);

    let external_datacache = DataCacheManager::instance().external_context(cache_options.clone());
    let (enable_file_metacache, enable_file_pagecache) =
        file_cache_flags_from_query_options(query_opts);
    let iceberg_table = find_iceberg_table(desc_tbl, tuple_id);
    if iceberg_table.is_some() && scan_format == Some(descriptors::THdfsFileFormat::ORC) {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={} does not support Iceberg ORC files; NovaRocks currently only supports Parquet for Iceberg schema/partition evolution",
            node.node_id
        ));
    }
    let iceberg_output_schema = iceberg_table
        .as_ref()
        .map(|iceberg| build_projected_output_schema(iceberg, &iceberg_projected_columns))
        .transpose()?
        .flatten();
    let output_chunk_schema = chunk_schema_for_layout(desc_tbl, &out_layout)?;
    // Parquet reader only materializes physical data columns (iceberg `_file` /
    // `_pos` are synthesized by the scan runner afterwards), so its chunk
    // schema must omit virtual-column slots to keep the column-count check on
    // the parquet side happy.
    let parquet_chunk_schema = sub_chunk_schema(desc_tbl, &data_slot_ids, &output_chunk_schema)?;
    let parquet_cfg = ParquetScanConfig {
        columns: data_columns,
        chunk_schema: parquet_chunk_schema,
        slot_types: data_slot_types,
        case_sensitive,
        enable_page_index,
        min_max_predicates,
        batch_size,
        datacache: external_datacache,
        cache_policy: ParquetReadCachePolicy::with_flags(
            enable_file_metacache,
            enable_file_pagecache,
            u32::try_from(cache_options.datacache_evict_probability).ok(),
        ),
        profile_label: Some(format!("hdfs_scan_node_id={}", node.node_id)),
        iceberg_output_schema,
    };
    let orc_cfg = OrcScanConfig {
        columns: parquet_cfg.columns.clone(),
        chunk_schema: parquet_cfg.chunk_schema.clone(),
        case_sensitive: parquet_cfg.case_sensitive,
        orc_use_column_names,
        hive_column_names,
        batch_size: parquet_cfg.batch_size,
        datacache: parquet_cfg.datacache.clone(),
    };
    let format = match scan_format {
        Some(descriptors::THdfsFileFormat::PARQUET) => Some(FileFormatConfig::Parquet(parquet_cfg)),
        Some(descriptors::THdfsFileFormat::ORC) => Some(FileFormatConfig::Orc(orc_cfg)),
        Some(other) => {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} unsupported file_format {:?}",
                node.node_id, other
            ));
        }
        None => None,
    };
    let cloud_props = hdfs
        .cloud_configuration
        .as_ref()
        .and_then(|c| c.cloud_properties.as_ref());
    let object_store_config = resolve_cloud_object_store_config(cloud_props, &ranges);
    let iceberg_table_locations = snapshot_iceberg_table_locations();
    let row_position_ranges = row_position_spec.as_ref().map(|_| ranges.clone());
    let cfg = HdfsScanConfig {
        ranges,
        original_range_count,
        has_more,
        limit,
        profile_label: Some(format!("hdfs_scan_node_id={}", node.node_id)),
        format,
        object_store_config: object_store_config.clone(),
        iceberg_table_locations,
    };
    let row_position_scan = row_position_spec.as_ref().and_then(|_| {
        scan_format.map(
            |file_format| crate::exec::node::scan::RowPositionScanConfig {
                file_format,
                case_sensitive,
                batch_size,
                enable_file_metacache,
                enable_file_pagecache,
                oss_config: object_store_config.clone(),
            },
        )
    });

    let scan = connectors
        .create_scan_node("hdfs", ScanConfig::Hdfs(Box::new(cfg)))?
        .with_node_id(node.node_id)
        .with_output_chunk_schema(output_chunk_schema)
        .with_limit(limit)
        .with_connector_io_tasks_per_scan_operator(connector_io_tasks_per_scan_operator)
        .with_accept_empty_scan_ranges(true)
        .with_row_position(row_position_spec)
        .with_row_position_scan(row_position_scan)
        .with_row_position_ranges(row_position_ranges)
        .with_iceberg_virtual(Some(crate::exec::row_position::IcebergVirtualSpec {
            file_path_slot: iceberg_virtual_file_slot,
            row_pos_slot: iceberg_virtual_pos_slot,
            row_id_slot: iceberg_virtual_row_id_slot,
            last_updated_seq_slot: iceberg_virtual_last_updated_seq_slot,
            change_op_slot: iceberg_virtual_change_op_slot,
            file_path_field: iceberg_virtual_file_field,
            row_pos_field: iceberg_virtual_pos_field,
            row_id_field: iceberg_virtual_row_id_field,
            last_updated_seq_field: iceberg_virtual_last_updated_seq_field,
            change_op_field: iceberg_virtual_change_op_field,
        }))
        .with_local_rf_waiting_set(local_rf_waiting_set(node));
    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan),
        },
        layout: out_layout,
    })
}

fn output_slots_from_layout(
    layout: &Layout,
    slot_info_map: &HashMap<
        SlotId,
        (
            String,
            types::TPrimitiveType,
            arrow::datatypes::DataType,
            bool,
        ),
    >,
) -> Result<Vec<IcebergMetadataOutputColumn>, String> {
    layout
        .order
        .iter()
        .map(|(_, slot_id)| {
            let slot_id = SlotId::try_from(*slot_id)?;
            let (name, _, data_type, nullable) = slot_info_map
                .get(&slot_id)
                .ok_or_else(|| format!("missing slot info for slot_id={slot_id}"))?
                .clone();
            Ok(IcebergMetadataOutputColumn {
                name,
                slot_id,
                data_type,
                nullable,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::common::ids::SlotId;
    use crate::internal_service::TQueryOptions;
    use crate::{exprs, plan_nodes, types};

    use super::{extract_change_op_from_extended_columns, file_cache_flags_from_query_options};

    #[test]
    fn file_cache_flags_default_to_disabled_when_query_options_missing() {
        let (meta, page) = file_cache_flags_from_query_options(None);
        assert!(!meta);
        assert!(!page);
    }

    #[test]
    fn file_cache_flags_follow_explicit_query_options() {
        let query_opts = TQueryOptions {
            enable_file_metacache: Some(true),
            enable_file_pagecache: Some(true),
            ..Default::default()
        };
        let (meta, page) = file_cache_flags_from_query_options(Some(&query_opts));
        assert!(meta);
        assert!(page);
    }

    fn int_expr(value: i64) -> exprs::TExpr {
        exprs::TExpr::new(vec![exprs::TExprNode {
            node_type: exprs::TExprNodeType::INT_LITERAL,
            type_: crate::lower::type_lowering::scalar_type_desc(types::TPrimitiveType::BIGINT),
            opcode: None,
            num_children: 0,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: Some(exprs::TIntLiteral { value }),
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: 0,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: None,
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }])
    }

    #[test]
    fn extract_change_op_reads_int_literal_from_extended_columns() {
        let mut range = plan_nodes::THdfsScanRange::default();
        range.extended_columns = Some(BTreeMap::from([(9, int_expr(-1))]));

        let value =
            extract_change_op_from_extended_columns(7, &range, Some(SlotId::new(9))).unwrap();

        assert_eq!(value, Some(-1));
    }

    #[test]
    fn extract_change_op_ignores_missing_extended_columns_entry() {
        let range = plan_nodes::THdfsScanRange::default();

        let value =
            extract_change_op_from_extended_columns(7, &range, Some(SlotId::new(9))).unwrap();

        assert_eq!(value, None);
    }

    #[test]
    fn extract_change_op_rejects_malformed_expr_shape() {
        let mut expr = int_expr(-1);
        expr.nodes.push(expr.nodes[0].clone());
        let mut range = plan_nodes::THdfsScanRange::default();
        range.extended_columns = Some(BTreeMap::from([(9, expr)]));

        let error =
            extract_change_op_from_extended_columns(7, &range, Some(SlotId::new(9))).unwrap_err();

        assert!(error.contains("exactly one INT_LITERAL"));
        assert!(error.contains("got 2"));
    }

    #[test]
    fn extract_change_op_rejects_int_literal_children() {
        let mut expr = int_expr(-1);
        expr.nodes[0].num_children = 1;
        let mut range = plan_nodes::THdfsScanRange::default();
        range.extended_columns = Some(BTreeMap::from([(9, expr)]));

        let error =
            extract_change_op_from_extended_columns(7, &range, Some(SlotId::new(9))).unwrap_err();

        assert!(error.contains("expects 0 children"));
        assert!(error.contains("got 1"));
    }
}
