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
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::common::ids::SlotId;
use crate::connector::starrocks::fe_v2_meta::{
    LakeScanTabletRef, LakeTableIdentity, find_cached_table_identity_names,
    resolve_tablet_paths_for_lake_scan,
};
use crate::connector::starrocks::lake::context::get_tablet_runtime;
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::project::ProjectNode;
use crate::exec::node::scan::LakeGlmScanInfo;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::exec::row_position::{
    LakeRowPositionSpec, is_lake_row_id, is_lake_rss_id, is_lake_source_id, is_lake_tablet_id,
};
use crate::fs::path::{ScanPathScheme, classify_scan_paths};
use crate::lower::expr::parse_min_max_conjunct;
use crate::lower::layout::{
    Layout, chunk_schema_for_layout, chunk_schema_for_tuple, find_tuple_descriptor,
    layout_for_row_tuples, layout_from_slot_ids, slot_arrow_type_lookup,
    slot_display_name_from_desc,
};
use crate::lower::node::{Lowered, QueryGlobalDictMap, local_rf_waiting_set};
use crate::novarocks_config::config as novarocks_app_config;
use crate::novarocks_connectors::{
    ConnectorRegistry, LakeScanSchemaMeta, ScanConfig, StarRocksScanConfig, StarRocksScanRange,
};
use crate::novarocks_logging::{debug, info};
use crate::runtime::query_context::QueryId;
use crate::runtime::starlet_shard_registry::{self, S3StoreConfig};
use crate::{descriptors, internal_service, plan_nodes, types};

/// Lower a LAKE_SCAN_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_lake_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    query_opts: Option<&internal_service::TQueryOptions>,
    arena: &mut ExprArena,
    connectors: &ConnectorRegistry,
    query_global_dict_map: &QueryGlobalDictMap,
    db_name_hint: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    if node.num_children != 0 {
        return Err(format!(
            "LAKE_SCAN_NODE expected 0 children, got {}",
            node.num_children
        ));
    }
    let Some(lake) = node.lake_scan_node.as_ref() else {
        return Err("LAKE_SCAN_NODE missing lake_scan_node payload".to_string());
    };
    let tuple_id = lake.tuple_id;

    let mut out_layout = Layout {
        order: Vec::new(),
        index: HashMap::new(),
    };
    if out_layout.order.is_empty() {
        if let Some(hint) = layout_hints.get(&tuple_id).filter(|v| !v.is_empty()) {
            out_layout = layout_from_slot_ids(tuple_id, hint.iter().copied());
        } else {
            out_layout = layout_for_row_tuples(&[tuple_id], tuple_slots);
        }
    }
    if out_layout.order.is_empty() {
        return Err(format!(
            "LAKE_SCAN_NODE tuple_id={tuple_id} has empty output layout"
        ));
    }
    let original_out_layout = out_layout.clone();

    let desc_tbl = desc_tbl.ok_or_else(|| {
        format!(
            "LAKE_SCAN_NODE node_id={} requires descriptor table for schema",
            node.node_id
        )
    })?;

    // FE may request low-card dictionary slots (INT ids) via dict_string_id_to_int_ids.
    // novarocks scan currently reads storage STRING slots, then we project them back to
    // FE-requested slot ids on top of scan.
    let mut dict_int_to_string: HashMap<types::TSlotId, types::TSlotId> = HashMap::new();
    if let Some(dict_map) = lake.dict_string_id_to_int_ids.as_ref() {
        for (string_slot_id, int_slot_id) in dict_map {
            dict_int_to_string.insert(*int_slot_id, *string_slot_id);
        }
    }
    let mut scan_layout = out_layout.clone();
    if !dict_int_to_string.is_empty() {
        for (_tuple_id, slot_id) in &mut scan_layout.order {
            if let Some(string_slot_id) = dict_int_to_string.get(slot_id) {
                *slot_id = *string_slot_id;
            }
        }
        scan_layout.index = scan_layout
            .order
            .iter()
            .enumerate()
            .map(|(i, key)| (*key, i))
            .collect();
    }

    // Detect lake GLM virtual column slots (synthesized by scan runner, not in storage).
    // They must be excluded from the storage schemas so the native reader ignores them.
    let slot_desc_lookup: HashMap<types::TSlotId, &descriptors::TSlotDescriptor> = desc_tbl
        .slot_descriptors
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .filter_map(|s| s.id.map(|id| (id, s)))
        .collect();

    let mut source_id_info: Option<(SlotId, arrow::datatypes::Field)> = None;
    let mut tablet_id_info: Option<(SlotId, arrow::datatypes::Field)> = None;
    let mut rss_id_info: Option<(SlotId, arrow::datatypes::Field)> = None;
    let mut row_id_info: Option<(SlotId, arrow::datatypes::Field)> = None;

    // Separate virtual cols from storage cols; produce a storage-only order
    let mut storage_order: Vec<(types::TTupleId, types::TSlotId)> =
        Vec::with_capacity(scan_layout.order.len());
    for entry @ (_tup_id, slot_id_raw) in &scan_layout.order {
        let found_virtual = 'check: {
            let Some(s) = slot_desc_lookup.get(slot_id_raw) else {
                break 'check false;
            };
            let Ok(slot_id) = SlotId::try_from(*slot_id_raw) else {
                break 'check false;
            };
            let Some(slot_type) = s.slot_type.as_ref() else {
                break 'check false;
            };
            let Some(arrow_type) = crate::lower::type_lowering::arrow_type_from_desc(slot_type)
            else {
                break 'check false;
            };
            let name = slot_display_name_from_desc(s);
            let nullable = s.is_nullable.unwrap_or(true);
            if is_lake_source_id(&name) {
                source_id_info = Some((
                    slot_id,
                    arrow::datatypes::Field::new(&name, arrow_type, nullable),
                ));
                break 'check true;
            }
            if is_lake_tablet_id(&name) {
                tablet_id_info = Some((
                    slot_id,
                    arrow::datatypes::Field::new(&name, arrow_type, nullable),
                ));
                break 'check true;
            }
            if is_lake_rss_id(&name) {
                rss_id_info = Some((
                    slot_id,
                    arrow::datatypes::Field::new(&name, arrow_type, nullable),
                ));
                break 'check true;
            }
            if is_lake_row_id(&name) {
                row_id_info = Some((
                    slot_id,
                    arrow::datatypes::Field::new(&name, arrow_type, nullable),
                ));
                break 'check true;
            }
            false
        };
        if !found_virtual {
            storage_order.push(*entry);
        }
    }

    let lake_row_position_spec = match (source_id_info, tablet_id_info, rss_id_info, row_id_info) {
        (None, None, None, None) => None,
        (
            Some((source_id_slot, source_id_field)),
            Some((tablet_id_slot, tablet_id_field)),
            Some((rss_id_slot, rss_id_field)),
            Some((row_id_slot, row_id_field)),
        ) => Some(LakeRowPositionSpec {
            source_id_slot,
            tablet_id_slot,
            rss_id_slot,
            row_id_slot,
            source_id_field,
            tablet_id_field,
            rss_id_field,
            row_id_field,
        }),
        _ => {
            return Err(format!(
                "LAKE_SCAN_NODE node_id={} lake row position slots must all be present together \
                (_source_id_/_tablet_id_/_rss_id_/_row_id_)",
                node.node_id
            ));
        }
    };

    if lake_row_position_spec.is_some() {
        // Rebuild scan_layout with virtual cols removed
        scan_layout.order = storage_order;
        scan_layout.index = scan_layout
            .order
            .iter()
            .enumerate()
            .map(|(i, key)| (*key, i))
            .collect();
    }

    let scan_output_chunk_schema = chunk_schema_for_layout(desc_tbl, &scan_layout)?;
    // When lake GLM virtual cols are present, build required_chunk_schema from the storage-only
    // layout; otherwise fall back to the full tuple layout.
    let required_chunk_schema = if lake_row_position_spec.is_some() {
        scan_output_chunk_schema.clone()
    } else {
        chunk_schema_for_tuple(desc_tbl, tuple_id)?
    };
    let scan_output_schema = scan_output_chunk_schema.arrow_schema_ref();
    if !scan_output_chunk_schema.slot_ids().is_empty()
        && scan_output_chunk_schema.slot_ids().len() != scan_output_schema.fields().len()
    {
        return Err(format!(
            "LAKE_SCAN_NODE output layout/schema mismatch: layout_len={}, schema_len={}",
            scan_output_chunk_schema.slot_ids().len(),
            scan_output_schema.fields().len()
        ));
    }

    let Some(exec_params) = exec_params else {
        return Err("LAKE_SCAN_NODE requires exec_params.per_node_scan_ranges".to_string());
    };
    let scan_ranges = exec_params
        .per_node_scan_ranges
        .get(&node.node_id)
        .ok_or_else(|| format!("missing per_node_scan_ranges for node_id={}", node.node_id))?;

    let mut ranges = Vec::new();
    let mut refs = Vec::new();
    let mut internal_db_name: Option<String> = None;
    let mut internal_table_name: Option<String> = None;
    let mut has_more = false;
    for p in scan_ranges {
        if p.empty.unwrap_or(false) {
            if p.has_more.unwrap_or(false) {
                has_more = true;
            }
            continue;
        }
        let Some(internal) = p.scan_range.internal_scan_range.as_ref() else {
            return Err(format!(
                "LAKE_SCAN_NODE node_id={} has scan range without internal_scan_range",
                node.node_id
            ));
        };
        let version = internal
            .version
            .parse::<i64>()
            .map_err(|e| format!("invalid tablet version '{}': {}", internal.version, e))?;
        if version <= 0 {
            return Err(format!(
                "invalid non-positive tablet version for tablet_id={}: {}",
                internal.tablet_id, version
            ));
        }
        let partition_id = internal.partition_id.ok_or_else(|| {
            format!(
                "LAKE_SCAN_NODE missing partition_id in scan range for tablet_id={}",
                internal.tablet_id
            )
        })?;

        refs.push(LakeScanTabletRef {
            tablet_id: internal.tablet_id,
            partition_id,
            version,
        });
        if internal_db_name.is_none() {
            let candidate = internal.db_name.trim();
            if !candidate.is_empty() {
                internal_db_name = Some(candidate.to_string());
            }
        }
        if internal_table_name.is_none() {
            if let Some(name) = internal
                .table_name
                .as_deref()
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
            {
                internal_table_name = Some(name.to_string());
            }
        }
        let fill_data_cache = internal.fill_data_cache.unwrap_or(true);
        let skip_page_cache = internal.skip_page_cache.unwrap_or(false);
        let skip_disk_cache = internal.skip_disk_cache.unwrap_or(false);
        if !fill_data_cache || skip_page_cache || skip_disk_cache {
            return Err(format!(
                "LAKE_SCAN_NODE node_id={} does not support internal-table cache controls yet (fill_data_cache={}, skip_page_cache={}, skip_disk_cache={})",
                node.node_id, fill_data_cache, skip_page_cache, skip_disk_cache
            ));
        }
        ranges.push(StarRocksScanRange {
            tablet_id: internal.tablet_id,
            partition_id: internal.partition_id,
            version: Some(version),
        });
    }
    if has_more {
        return Err(format!(
            "LAKE_SCAN_NODE node_id={} has incremental scan ranges which are not supported",
            node.node_id
        ));
    }
    if ranges.is_empty() {
        return Err(format!(
            "LAKE_SCAN_NODE node_id={} has no effective scan ranges",
            node.node_id
        ));
    }

    let tuple_desc = find_tuple_descriptor(desc_tbl, tuple_id)?;
    let table_id_from_tuple = tuple_desc
        .table_id
        .ok_or_else(|| format!("LAKE_SCAN_NODE tuple_id={} missing table_id", tuple_id))?;
    let table_descs = desc_tbl
        .table_descriptors
        .as_ref()
        .ok_or_else(|| "LAKE_SCAN_NODE missing table_descriptors in desc_tbl".to_string())?;
    let table_desc = table_descs
        .iter()
        .find(|t| t.id == table_id_from_tuple)
        .ok_or_else(|| {
            format!(
                "LAKE_SCAN_NODE missing table descriptor for table_id={}",
                table_id_from_tuple
            )
        })?;

    let mut db_name = if table_desc.db_name.trim().is_empty() {
        if let Some(name) = db_name_hint
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string())
        {
            name
        } else if let Some(name) = internal_db_name.clone() {
            name
        } else {
            "__unknown_db__".to_string()
        }
    } else {
        table_desc.db_name.clone()
    };
    let mut table_name = if table_desc.table_name.trim().is_empty() {
        internal_table_name.unwrap_or_else(|| "__unknown_table__".to_string())
    } else {
        table_desc.table_name.trim().to_string()
    };

    let schema_meta = lake
        .schema_meta
        .as_ref()
        .ok_or_else(|| "LAKE_SCAN_NODE missing schema_meta".to_string())?;
    let db_id = schema_meta
        .db_id
        .ok_or_else(|| "LAKE_SCAN_NODE schema_meta missing db_id".to_string())?;
    let table_id = schema_meta
        .table_id
        .ok_or_else(|| "LAKE_SCAN_NODE schema_meta missing table_id".to_string())?;
    let schema_id = schema_meta
        .schema_id
        .ok_or_else(|| "LAKE_SCAN_NODE schema_meta missing schema_id".to_string())?;

    if table_id != table_desc.id {
        return Err(format!(
            "LAKE_SCAN_NODE schema_meta.table_id={} mismatches descriptor table_id={}",
            table_id, table_desc.id
        ));
    }
    let catalog = load_lake_catalog()?;
    if (db_name == "__unknown_db__" || table_name == "__unknown_table__")
        && let Some((cached_db_name, cached_table_name)) =
            find_cached_table_identity_names(&catalog, db_id, table_id)
    {
        if db_name == "__unknown_db__" {
            db_name = cached_db_name;
        }
        if table_name == "__unknown_table__" {
            table_name = cached_table_name;
        }
    }
    let table_identity = LakeTableIdentity {
        catalog,
        db_name: db_name.clone(),
        table_name: table_name.clone(),
        db_id,
        table_id,
        schema_id,
    };

    let query_id = Some(QueryId {
        hi: exec_params.query_id.hi,
        lo: exec_params.query_id.lo,
    });
    let tablet_path_map = resolve_tablet_paths_for_lake_scan(query_id, fe_addr, &table_identity, &refs)
        .map_err(|e| {
            format!(
                "LAKE_SCAN_NODE resolve tablet paths failed for catalog={} db_name={} table_name={} db_id={} table_id={} schema_id={}: {}",
                table_identity.catalog,
                table_identity.db_name,
                table_identity.table_name,
                table_identity.db_id,
                table_identity.table_id,
                table_identity.schema_id,
                e
            )
        })?;
    let mut properties = build_lake_properties(&tablet_path_map)?;
    let partition_storage_paths = build_partition_storage_paths(&ranges, &tablet_path_map)?;
    properties.remove("tablet_root_paths");
    properties.insert(
        "partition_storage_paths".to_string(),
        serde_json::to_string(&partition_storage_paths)
            .map_err(|e| format!("serialize partition_storage_paths json failed: {}", e))?,
    );
    let limit = (node.limit >= 0).then_some(node.limit as usize);
    let batch_size = query_opts.and_then(|opts| opts.batch_size).or(Some(4096));
    let query_timeout = query_opts.and_then(|opts| opts.query_timeout);
    let mem_limit = query_opts
        .and_then(|opts| opts.query_mem_limit.or(opts.mem_limit))
        .filter(|v| *v > 0);
    let connector_io_tasks_per_scan_operator =
        query_opts.and_then(|opts| opts.connector_io_tasks_per_scan_operator);
    let mut min_max_predicates = Vec::new();
    if let Some(conjuncts) = node.conjuncts.as_ref() {
        for conj in conjuncts {
            if let Some(pred) = parse_min_max_conjunct(conj, &out_layout)? {
                min_max_predicates.push(pred);
            }
        }
    }
    debug!(
        "LAKE_SCAN_NODE node_id={} resolved {} tablets via Starlet AddShard cache",
        node.node_id,
        tablet_path_map.len()
    );

    let cfg = StarRocksScanConfig {
        db_name: normalize_optional_table_name(&db_name, "__unknown_db__"),
        table_name: normalize_optional_table_name(&table_name, "__unknown_table__"),
        properties,
        ranges,
        has_more,
        required_chunk_schema,
        output_chunk_schema: scan_output_chunk_schema.clone(),
        query_global_dicts: build_scan_query_global_dicts(
            scan_output_chunk_schema.slot_ids(),
            query_global_dict_map,
        )?,
        limit,
        batch_size,
        query_timeout,
        mem_limit,
        profile_label: Some(format!("lake_scan_node_id={}", node.node_id)),
        min_max_predicates,
        lake_schema_meta: Some(LakeScanSchemaMeta {
            db_id,
            table_id,
            schema_id,
            fe_addr: fe_addr.cloned(),
            query_id: Some(types::TUniqueId {
                hi: exec_params.query_id.hi,
                lo: exec_params.query_id.lo,
            }),
        }),
        topn_filter_column_map: std::collections::HashMap::new(),
    };

    let lake_row_position_spec_active = lake_row_position_spec.is_some();

    let lake_glm_info = lake_row_position_spec.as_ref().map(|_| LakeGlmScanInfo {
        ranges: cfg.ranges.clone(),
        properties: cfg.properties.clone(),
        lake_schema_meta: cfg.lake_schema_meta.clone(),
    });

    let scan = connectors
        .create_scan_node("starrocks", ScanConfig::StarRocks(cfg))?
        .with_node_id(node.node_id)
        .with_output_chunk_schema(scan_output_chunk_schema.clone())
        .with_limit(limit)
        .with_connector_io_tasks_per_scan_operator(connector_io_tasks_per_scan_operator)
        .with_local_rf_waiting_set(local_rf_waiting_set(node))
        .with_lake_row_position(lake_row_position_spec)
        .with_lake_glm_info(lake_glm_info);
    let scan_lowered = Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan),
        },
        layout: scan_layout,
    };

    // Skip the dict-expansion projection if there's nothing to remap,
    // or if lake GLM virtual cols were removed (that difference is not dict-related).
    if dict_int_to_string.is_empty() || lake_row_position_spec_active {
        return Ok(scan_lowered);
    }

    let slot_types = slot_arrow_type_lookup(desc_tbl)?;
    let projected_output_chunk_schema = chunk_schema_for_layout(desc_tbl, &original_out_layout)?;

    let mut exprs = Vec::with_capacity(original_out_layout.order.len());
    let mut expr_slot_ids = Vec::with_capacity(original_out_layout.order.len());
    for (_tuple_id, output_slot_id) in &original_out_layout.order {
        let source_slot_id = dict_int_to_string
            .get(output_slot_id)
            .copied()
            .unwrap_or(*output_slot_id);
        let source_slot = SlotId::try_from(source_slot_id)?;
        let output_slot = SlotId::try_from(*output_slot_id)?;
        let source_tuple_id =
            resolve_layout_slot_tuple_id(&scan_lowered.layout, source_slot_id, "LAKE_SCAN_NODE")?;
        let data_type = slot_types
            .get(&(source_tuple_id, source_slot_id))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "LAKE_SCAN_NODE missing source slot type for tuple_id={} slot_id={}",
                    source_tuple_id, source_slot_id
                )
            })?;
        let expr = arena.push_typed(ExprNode::SlotId(source_slot), data_type);
        exprs.push(expr);
        expr_slot_ids.push(output_slot);
    }

    let output_indices: Vec<usize> = (0..exprs.len()).collect();
    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(scan_lowered.node),
                node_id: node.node_id,
                is_subordinate: true,
                exprs,
                expr_slot_ids,
                expr_slot_schemas: None,
                output_indices: Some(output_indices),
                output_chunk_schema: projected_output_chunk_schema,
            }),
        },
        layout: original_out_layout,
    })
}

fn resolve_layout_slot_tuple_id(
    layout: &Layout,
    slot_id: types::TSlotId,
    context: &str,
) -> Result<types::TTupleId, String> {
    let mut matches = layout
        .order
        .iter()
        .filter_map(|(tuple_id, layout_slot_id)| (*layout_slot_id == slot_id).then_some(*tuple_id));
    let tuple_id = matches
        .next()
        .ok_or_else(|| format!("{context} missing slot_id={slot_id} in layout"))?;
    if matches.next().is_some() {
        return Err(format!(
            "{context} ambiguous slot_id={} across multiple tuples",
            slot_id
        ));
    }
    Ok(tuple_id)
}

pub(crate) fn load_lake_catalog() -> Result<String, String> {
    let cfg = novarocks_app_config().map_err(|e| e.to_string())?;
    let catalog = cfg.starrocks.fe_catalog.trim();
    if catalog.is_empty() {
        return Err("starrocks.fe_catalog cannot be empty".to_string());
    }
    Ok(catalog.to_string())
}

fn normalize_optional_table_name(name: &str, unknown_sentinel: &str) -> Option<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case(unknown_sentinel) {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn build_scan_query_global_dicts(
    output_slots: &[SlotId],
    query_global_dict_map: &QueryGlobalDictMap,
) -> Result<HashMap<SlotId, Arc<HashMap<Vec<u8>, i32>>>, String> {
    let mut out = HashMap::new();
    for slot_id in output_slots {
        let raw_slot_id = i32::try_from(slot_id.as_u32()).map_err(|_| {
            format!(
                "slot id out of i32 range for query global dict: {}",
                slot_id
            )
        })?;
        let Some(dict_values) = query_global_dict_map.get(&raw_slot_id) else {
            continue;
        };
        let mut value_to_id = HashMap::with_capacity(dict_values.len());
        for (id, value) in dict_values.iter() {
            if let Some(existing) = value_to_id.insert(value.clone(), *id) {
                if existing != *id {
                    return Err(format!(
                        "query global dict has duplicated string with different ids: slot_id={}, existing_id={}, new_id={}",
                        slot_id, existing, id
                    ));
                }
            }
        }
        out.insert(*slot_id, Arc::new(value_to_id));
    }
    if !out.is_empty() {
        info!(
            "LAKE_SCAN query global dict enabled for slots={:?}",
            out.keys().collect::<Vec<_>>()
        );
    }
    Ok(out)
}

pub(crate) fn build_lake_properties(
    tablet_path_map: &HashMap<i64, String>,
) -> Result<BTreeMap<String, String>, String> {
    if tablet_path_map.is_empty() {
        return Err("lake scan tablet_path_map is empty".to_string());
    }

    let mut tablet_paths: BTreeMap<String, String> = BTreeMap::new();
    let mut tablet_ids = Vec::with_capacity(tablet_path_map.len());
    for (tablet_id, path) in tablet_path_map {
        if *tablet_id <= 0 {
            return Err(format!(
                "invalid tablet_id in resolved tablet path map: {}",
                tablet_id
            ));
        }
        let trimmed = path.trim();
        if trimmed.is_empty() {
            return Err(format!(
                "resolved tablet path is empty for tablet_id={}",
                tablet_id
            ));
        }
        tablet_paths.insert(tablet_id.to_string(), trimmed.to_string());
        tablet_ids.push(*tablet_id);
    }
    tablet_ids.sort_unstable();
    tablet_ids.dedup();

    let mut props = BTreeMap::new();
    props.insert(
        "tablet_root_paths".to_string(),
        serde_json::to_string(&tablet_paths)
            .map_err(|e| format!("serialize tablet_root_paths json failed: {}", e))?,
    );

    let scheme = classify_scan_paths(tablet_path_map.values().map(|v| v.as_str()))?;
    match scheme {
        ScanPathScheme::Local => {}
        ScanPathScheme::Oss => {
            let shard_infos = starlet_shard_registry::select_infos(&tablet_ids);
            let mut selected_s3: Option<S3StoreConfig> = None;
            for tablet_id in tablet_ids {
                let tablet_path = tablet_path_map
                    .get(&tablet_id)
                    .map(String::as_str)
                    .unwrap_or("<unknown>");
                let s3_cfg = shard_infos
                    .get(&tablet_id)
                    .and_then(|info| info.s3.clone())
                    .or_else(|| {
                        get_tablet_runtime(tablet_id)
                            .ok()
                            .and_then(|runtime| runtime.s3_config.clone())
                    })
                    .or_else(|| starlet_shard_registry::infer_s3_config_for_path(tablet_path))
                    .ok_or_else(|| {
                        format!(
                            "missing S3 config for lake scan tablet_id={} (path={})",
                            tablet_id, tablet_path
                        )
                    })?;

                match selected_s3.as_ref() {
                    None => selected_s3 = Some(s3_cfg),
                    Some(prev) if prev == &s3_cfg => {}
                    Some(prev) => {
                        return Err(format!(
                            "inconsistent S3 config across tablets in one lake scan; \
                            tablet_id={} endpoint={} bucket={} root={} conflicts with endpoint={} bucket={} root={}",
                            tablet_id,
                            s3_cfg.endpoint,
                            s3_cfg.bucket,
                            s3_cfg.root,
                            prev.endpoint,
                            prev.bucket,
                            prev.root
                        ));
                    }
                }
            }

            let s3 = selected_s3.ok_or_else(|| {
                "lake scan object-store path has no resolved S3 config".to_string()
            })?;
            for (k, v) in s3.to_aws_s3_properties() {
                props.insert(k, v);
            }
        }
        ScanPathScheme::Hdfs => {
            return Err("lake scan does not support hdfs tablet paths yet".to_string());
        }
    }
    Ok(props)
}

fn build_partition_storage_paths(
    ranges: &[StarRocksScanRange],
    tablet_path_map: &HashMap<i64, String>,
) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    for range in ranges {
        let partition_id = range.partition_id.ok_or_else(|| {
            format!(
                "missing partition_id while building partition_storage_paths for tablet_id={}",
                range.tablet_id
            )
        })?;
        if partition_id <= 0 {
            return Err(format!(
                "invalid partition_id while building partition_storage_paths: {}",
                partition_id
            ));
        }
        let raw_path = tablet_path_map.get(&range.tablet_id).ok_or_else(|| {
            format!(
                "missing resolved tablet path while building partition_storage_paths for tablet_id={}",
                range.tablet_id
            )
        })?;
        let partition_path = normalize_partition_storage_path(raw_path, range.tablet_id)?;
        let key = partition_id.to_string();
        match out.get(&key) {
            None => {
                out.insert(key, partition_path);
            }
            Some(existing) if existing == &partition_path => {}
            Some(existing) => {
                return Err(format!(
                    "inconsistent partition storage paths for partition_id={}: existing={} new={} (tablet_id={})",
                    partition_id, existing, partition_path, range.tablet_id
                ));
            }
        }
    }
    Ok(out)
}

fn normalize_partition_storage_path(path: &str, tablet_id: i64) -> Result<String, String> {
    let trimmed = path.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(format!(
            "empty storage path while building partition_storage_paths for tablet_id={}",
            tablet_id
        ));
    }
    let tablet_id_suffix = format!("/{tablet_id}");
    if let Some(prefix) = trimmed.strip_suffix(&tablet_id_suffix)
        && !prefix.is_empty()
    {
        return Ok(prefix.trim_end_matches('/').to_string());
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::build_partition_storage_paths;
    use crate::connector::starrocks::StarRocksScanRange;
    use std::collections::HashMap;

    #[test]
    fn build_partition_storage_paths_collapses_tablet_suffix() {
        let ranges = vec![
            StarRocksScanRange {
                tablet_id: 101,
                partition_id: Some(7),
                version: Some(1),
            },
            StarRocksScanRange {
                tablet_id: 102,
                partition_id: Some(7),
                version: Some(1),
            },
            StarRocksScanRange {
                tablet_id: 201,
                partition_id: Some(8),
                version: Some(1),
            },
        ];
        let tablet_path_map = HashMap::from([
            (101, "s3://bucket/root/7/101".to_string()),
            (102, "s3://bucket/root/7/102/".to_string()),
            (201, "s3://bucket/root/8".to_string()),
        ]);

        let paths = build_partition_storage_paths(&ranges, &tablet_path_map)
            .expect("partition_storage_paths should build");

        assert_eq!(
            paths.get("7").map(String::as_str),
            Some("s3://bucket/root/7")
        );
        assert_eq!(
            paths.get("8").map(String::as_str),
            Some("s3://bucket/root/8")
        );
    }
}
