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

use arrow::datatypes::DataType;

use crate::common::ids::SlotId;
use crate::connector::starrocks::fe_v2_meta::{
    LakeScanTabletRef, LakeTableIdentity, find_cached_table_identity_names,
    resolve_tablet_paths_for_lake_scan,
};
use crate::connector::starrocks::lake::context::get_tablet_runtime;
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::project::ProjectNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::fs::path::{ScanPathScheme, classify_scan_paths};
use crate::lower::expr::parse_min_max_conjunct;
use crate::lower::layout::{
    Layout, find_tuple_descriptor, layout_for_row_tuples, layout_from_slot_ids, schema_for_layout,
    schema_for_tuple,
};
use crate::lower::node::{Lowered, QueryGlobalDictMap, local_rf_waiting_set};
use crate::lower::type_lowering::arrow_type_from_desc;
use crate::runtime::query_context::QueryId;
use crate::runtime::starlet_shard_registry::{self, S3StoreConfig};
use crate::novarocks_config::config as novarocks_app_config;
use crate::novarocks_connectors::{
    ConnectorRegistry, ScanConfig, StarRocksScanConfig, StarRocksScanRange,
};
use crate::novarocks_logging::{debug, info};
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

    let schema = schema_for_layout(desc_tbl, &scan_layout)?;
    let required_schema = schema_for_tuple(desc_tbl, tuple_id)?;
    let slot_ids = scan_layout
        .order
        .iter()
        .map(|(_, s)| SlotId::try_from(*s))
        .collect::<Result<Vec<_>, _>>()?;
    if !slot_ids.is_empty() && slot_ids.len() != schema.fields().len() {
        return Err(format!(
            "LAKE_SCAN_NODE output layout/schema mismatch: layout_len={}, schema_len={}",
            slot_ids.len(),
            schema.fields().len()
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
            version: Some(version),
            schema_hash: None,
            db_name: None,
            table_name: None,
            hosts: internal.hosts.clone(),
            fill_data_cache,
            skip_page_cache,
            skip_disk_cache,
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
    properties.insert("fetch_mode".to_string(), "object_store".to_string());

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
            match parse_min_max_conjunct(conj, &out_layout) {
                Ok(Some(pred)) => min_max_predicates.push(pred),
                Ok(None) => {}
                Err(err) => {
                    debug!(
                        "LAKE_SCAN_NODE node_id={} skip unsupported min/max conjunct for native pruning: {}",
                        node.node_id, err
                    );
                }
            }
        }
    }
    debug!(
        "LAKE_SCAN_NODE node_id={} resolved {} tablets via Starlet AddShard cache",
        node.node_id,
        tablet_path_map.len()
    );

    let output_slots = slot_ids.clone();
    let cfg = StarRocksScanConfig {
        db_name: normalize_optional_table_name(&db_name, "__unknown_db__"),
        table_name: normalize_optional_table_name(&table_name, "__unknown_table__"),
        opaqued_query_plan: String::new(),
        properties,
        ranges,
        has_more,
        required_schema,
        schema,
        slot_ids,
        query_global_dicts: build_scan_query_global_dicts(&output_slots, query_global_dict_map)?,
        limit,
        batch_size,
        query_timeout,
        mem_limit,
        profile_label: Some(format!("lake_scan_node_id={}", node.node_id)),
        min_max_predicates,
    };

    let scan = connectors
        .create_scan_node("starrocks", ScanConfig::StarRocks(cfg))?
        .with_node_id(node.node_id)
        .with_output_slots(output_slots)
        .with_limit(limit)
        .with_connector_io_tasks_per_scan_operator(connector_io_tasks_per_scan_operator)
        .with_local_rf_waiting_set(local_rf_waiting_set(node));
    let scan_lowered = Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan),
        },
        layout: scan_layout,
    };

    if dict_int_to_string.is_empty() || scan_lowered.layout.order == original_out_layout.order {
        return Ok(scan_lowered);
    }

    let mut slot_types = HashMap::<types::TSlotId, DataType>::new();
    if let Some(slot_descs) = desc_tbl.slot_descriptors.as_ref() {
        for s in slot_descs {
            let (Some(slot_id), Some(slot_type)) = (s.id, s.slot_type.as_ref()) else {
                continue;
            };
            if let Some(data_type) = arrow_type_from_desc(slot_type) {
                slot_types.insert(slot_id, data_type);
            }
        }
    }

    let mut exprs = Vec::with_capacity(original_out_layout.order.len());
    let mut expr_slot_ids = Vec::with_capacity(original_out_layout.order.len());
    for (_tuple_id, output_slot_id) in &original_out_layout.order {
        let source_slot_id = dict_int_to_string
            .get(output_slot_id)
            .copied()
            .unwrap_or(*output_slot_id);
        let source_slot = SlotId::try_from(source_slot_id)?;
        let output_slot = SlotId::try_from(*output_slot_id)?;
        let data_type = slot_types
            .get(&source_slot_id)
            .cloned()
            .unwrap_or(DataType::Utf8);
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
                output_indices: Some(output_indices),
                output_slots: original_out_layout
                    .order
                    .iter()
                    .map(|(_, slot_id)| SlotId::try_from(*slot_id))
                    .collect::<Result<Vec<_>, _>>()?,
            }),
        },
        layout: original_out_layout,
    })
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
    if matches!(scheme, ScanPathScheme::Oss) {
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

        let s3 = selected_s3
            .ok_or_else(|| "lake scan object-store path has no resolved S3 config".to_string())?;
        for (k, v) in s3.to_aws_s3_properties() {
            props.insert(k, v);
        }
    }
    Ok(props)
}
