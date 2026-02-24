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
use std::sync::{Mutex, OnceLock};

use crate::cache::{CacheOptions, DataCacheManager, ExternalDataCacheRangeOptions};
use crate::common::ids::SlotId;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::formats::parquet::ParquetReadCachePolicy;
use crate::lower::expr::parse_min_max_conjunct;
use crate::lower::layout::{
    Layout, col_names_from_layout, find_tuple_descriptor, layout_from_slot_ids,
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

static ICEBERG_TABLE_LOCATIONS: OnceLock<Mutex<HashMap<types::TTableId, String>>> = OnceLock::new();

fn iceberg_table_locations() -> &'static Mutex<HashMap<types::TTableId, String>> {
    ICEBERG_TABLE_LOCATIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Cache Iceberg table locations from descriptor table for later use in HDFS scan lowering.
pub(crate) fn cache_iceberg_table_locations(desc_tbl: Option<&descriptors::TDescriptorTable>) {
    let Some(desc_tbl) = desc_tbl else { return };
    let Some(tables) = desc_tbl.table_descriptors.as_ref() else {
        return;
    };
    let mut guard = iceberg_table_locations()
        .lock()
        .expect("iceberg_table_locations lock");
    for t in tables {
        let Some(iceberg) = t.iceberg_table.as_ref() else {
            continue;
        };
        let Some(location) = iceberg.location.as_ref().filter(|s| !s.is_empty()) else {
            continue;
        };
        guard.insert(t.id, location.clone());
    }
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
        .or_else(|| node.row_tuples.get(0).copied())
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
        let name = crate::lower::layout::slot_name_from_desc(s).ok_or_else(|| {
            format!(
                "missing column name for slot_id={id} tuple_id={:?}",
                s.parent
            )
        })?;
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

    let mut slot_ids = Vec::with_capacity(out_layout.order.len());
    let mut data_columns = Vec::new();
    let mut data_slot_ids = Vec::new();
    let mut data_slot_types = Vec::new();

    let mut row_source_slot: Option<SlotId> = None;
    let mut scan_range_slot: Option<SlotId> = None;
    let mut row_id_slot: Option<SlotId> = None;
    let mut row_source_field: Option<arrow::datatypes::Field> = None;
    let mut scan_range_field: Option<arrow::datatypes::Field> = None;
    let mut row_id_field: Option<arrow::datatypes::Field> = None;

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
            row_source_field = Some(crate::exec::chunk::field_with_slot_id(
                arrow::datatypes::Field::new(name, arrow_type, nullable),
                slot_id,
            ));
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
            scan_range_field = Some(crate::exec::chunk::field_with_slot_id(
                arrow::datatypes::Field::new(name, arrow_type, nullable),
                slot_id,
            ));
            continue;
        }
        if crate::exec::row_position::is_row_id(&name) {
            if primitive != types::TPrimitiveType::BIGINT {
                return Err(format!(
                    "HDFS_SCAN_NODE node_id={} row_id slot_id={} expects BIGINT, got {:?}",
                    node.node_id, slot_id, primitive
                ));
            }
            row_id_slot = Some(slot_id);
            row_id_field = Some(crate::exec::chunk::field_with_slot_id(
                arrow::datatypes::Field::new(name, arrow_type, nullable),
                slot_id,
            ));
            continue;
        }

        data_columns.push(name);
        data_slot_ids.push(slot_id);
        data_slot_types.push(primitive);
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

    let Some(exec_params) = exec_params else {
        return Err("HDFS_SCAN_NODE requires exec_params.per_node_scan_ranges".to_string());
    };
    let scan_ranges = exec_params
        .per_node_scan_ranges
        .get(&node.node_id)
        .ok_or_else(|| format!("missing per_node_scan_ranges for node_id={}", node.node_id))?;
    let mut ranges: Vec<FileScanRange> = Vec::new();
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
        if hdfs_range
            .delete_files
            .as_ref()
            .is_some_and(|v| !v.is_empty())
        {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} does not support delete files (append-only only)",
                node.node_id
            ));
        }
        if hdfs_range.deletion_vector_descriptor.is_some() {
            return Err(format!(
                "HDFS_SCAN_NODE node_id={} does not support deletion vectors (append-only only)",
                node.node_id
            ));
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
            scan_format = Some(file_format.clone());
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
        let first_row_id = if row_position_spec.is_some() {
            hdfs_range.first_row_id.ok_or_else(|| {
                format!(
                    "HDFS_SCAN_NODE node_id={} missing first_row_id for iceberg row position",
                    node.node_id
                )
            })?
        } else {
            0
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

        if let Some(fp) = hdfs_range.full_path.as_ref().filter(|s| !s.is_empty()) {
            ranges.push(FileScanRange {
                path: fp.clone(),
                file_len,
                offset,
                length,
                scan_range_id,
                first_row_id: row_position_spec.as_ref().map(|_| first_row_id),
                external_datacache: external_datacache.clone(),
            });
        } else if let Some(rp) = hdfs_range.relative_path.as_ref().filter(|s| !s.is_empty()) {
            let table_id = hdfs_range.table_id.ok_or_else(|| {
                format!(
                    "HDFS_SCAN_NODE node_id={} has relative_path={rp:?} but missing table_id; cannot resolve to full OSS path",
                    node.node_id
                )
            })?;
            let loc = iceberg_table_locations()
                .lock()
                .expect("iceberg_table_locations lock")
                .get(&table_id)
                .cloned()
                .ok_or_else(|| {
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
                first_row_id: row_position_spec.as_ref().map(|_| first_row_id),
                external_datacache,
            });
        }
    }
    if has_more {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={} has incremental scan ranges which are not supported",
            node.node_id
        ));
    }
    let original_range_count = ranges.len();
    apply_path_rewrite(&mut ranges)?;

    let limit = node.limit;
    let limit = (limit >= 0).then_some(limit as usize);
    let mut enable_page_index = query_opts
        .and_then(|opts| opts.enable_parquet_reader_page_index)
        .unwrap_or(false);
    let connector_io_tasks_per_scan_operator =
        query_opts.and_then(|opts| opts.connector_io_tasks_per_scan_operator);

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

    let output_slots = slot_ids.clone();
    let external_datacache = DataCacheManager::instance().external_context(cache_options.clone());
    // Align with FE session defaults when these flags are absent in request query options.
    let enable_file_metacache = query_opts
        .and_then(|opts| opts.enable_file_metacache)
        .unwrap_or(true);
    let enable_file_pagecache = query_opts
        .and_then(|opts| opts.enable_file_pagecache)
        .unwrap_or(true);
    let parquet_cfg = ParquetScanConfig {
        columns: data_columns,
        slot_ids: data_slot_ids,
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
    };
    let orc_cfg = OrcScanConfig {
        columns: parquet_cfg.columns.clone(),
        slot_ids: parquet_cfg.slot_ids.clone(),
        case_sensitive: parquet_cfg.case_sensitive,
        orc_use_column_names,
        hive_column_names,
        batch_size: parquet_cfg.batch_size,
        datacache: parquet_cfg.datacache.clone(),
    };
    let format = match scan_format.clone() {
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
    let row_position_ranges = row_position_spec.as_ref().map(|_| ranges.clone());
    let cfg = HdfsScanConfig {
        ranges,
        original_range_count,
        has_more,
        limit,
        profile_label: Some(format!("hdfs_scan_node_id={}", node.node_id)),
        format,
    };
    let row_position_scan = row_position_spec.as_ref().and_then(|_| {
        scan_format.map(
            |file_format| crate::exec::node::scan::RowPositionScanConfig {
                file_format,
                case_sensitive,
                batch_size,
                enable_file_metacache,
                enable_file_pagecache,
            },
        )
    });

    let scan = connectors
        .create_scan_node("hdfs", ScanConfig::Hdfs(cfg))?
        .with_node_id(node.node_id)
        .with_output_slots(output_slots)
        .with_limit(limit)
        .with_connector_io_tasks_per_scan_operator(connector_io_tasks_per_scan_operator)
        .with_accept_empty_scan_ranges(true)
        .with_row_position(row_position_spec)
        .with_row_position_scan(row_position_scan)
        .with_row_position_ranges(row_position_ranges)
        .with_local_rf_waiting_set(local_rf_waiting_set(node));
    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan),
        },
        layout: out_layout,
    })
}
