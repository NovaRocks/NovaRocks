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

use arrow::array::ArrayRef;
use base64::Engine;
use thrift::OrderedFloat;

use crate::exec::chunk::Chunk;
use crate::novarocks_logging::{error, info, warn};

use crate::common::app_config;
use crate::common::config::debug_exec_batch_plan_json;
use crate::common::ids::SlotId;
use crate::common::thrift::{
    thrift_binary_deserialize, thrift_compact_serialize, thrift_named_json,
};

use crate::cache::CacheOptions;
use crate::common::types::{FetchResult, UniqueId};
use crate::common::util::{
    http_json_row_from_arrays_with_primitives, mysql_text_row_from_arrays,
    mysql_text_row_from_arrays_with_primitives,
};
use crate::lower::cache_iceberg_table_locations;
use crate::lower::fragment::execute_fragment;
use crate::lower::type_lowering::{
    FIELD_META_PRIMITIVE_JSON, FIELD_META_PRIMITIVE_TYPE, primitive_type_from_desc,
};
use crate::runtime::exchange;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::{
    QueryContextManager, QueryId, desc_tbl_is_cached, is_desc_tbl_effectively_empty,
    observe_total_fragments, query_context_manager, query_expire_durations,
    resolve_desc_tbl_for_instance,
};
use crate::runtime::result_buffer;
use crate::service::fe_report;
use crate::{data, data_sinks, descriptors, exprs, internal_service, planner, types};

const STATISTIC_DATA_VERSION_V1: i32 = 1;
const STATISTIC_HISTOGRAM_VERSION: i32 = 2;
const STATISTIC_TABLE_VERSION: i32 = 3;
const STATISTIC_BATCH_VERSION: i32 = 4;
const STATISTIC_EXTERNAL_VERSION: i32 = 5;
const STATISTIC_EXTERNAL_QUERY_VERSION: i32 = 6;
const STATISTIC_EXTERNAL_HISTOGRAM_VERSION: i32 = 7;
const STATISTIC_EXTERNAL_QUERY_VERSION_V2: i32 = 8;
const STATISTIC_BATCH_VERSION_V5: i32 = 9;
const STATISTIC_DATA_VERSION_V2: i32 = 10;
const STATISTIC_PARTITION_VERSION: i32 = 11;
const STATISTIC_MULTI_COLUMN_VERSION: i32 = 12;
const STATISTIC_QUERY_MULTI_COLUMN_VERSION: i32 = 13;
const STATISTIC_PARTITION_VERSION_V2: i32 = 20;
const STATISTIC_DICT_VERSION: i32 = 101;

fn profile_name_for_fragment(fragment: &planner::TPlanFragment) -> String {
    let plan_node_id = fragment
        .plan
        .as_ref()
        .and_then(|plan| plan.nodes.first().map(|n| n.node_id))
        .unwrap_or(-1);
    if plan_node_id >= 0 {
        format!("execute_fragment (plan_node_id={plan_node_id})")
    } else {
        "execute_fragment".to_string()
    }
}

fn choose_nonempty_str<'a>(primary: Option<&'a str>, fallback: Option<&'a str>) -> Option<&'a str> {
    match primary {
        Some(s) if !s.is_empty() => Some(s),
        _ => match fallback {
            Some(s) if !s.is_empty() => Some(s),
            _ => None,
        },
    }
}

fn validate_network_address(
    addr: Option<&types::TNetworkAddress>,
    missing_msg: &str,
    field_name: &str,
) -> Result<(), String> {
    let addr = addr.ok_or_else(|| missing_msg.to_string())?;
    if addr.hostname.is_empty() {
        return Err(format!("{field_name} hostname is empty"));
    }
    if addr.port <= 0 {
        return Err(format!("{field_name} port must be positive"));
    }
    Ok(())
}

fn validate_nodes_info(
    nodes_info: &descriptors::TNodesInfo,
    field_name: &str,
) -> Result<(), String> {
    for (idx, node) in nodes_info.nodes.iter().enumerate() {
        if node.host.is_empty() {
            return Err(format!("{field_name}[{idx}] host is empty"));
        }
        if node.async_internal_port <= 0 {
            return Err(format!(
                "{field_name}[{idx}] async_internal_port must be positive"
            ));
        }
    }
    Ok(())
}

fn validate_destinations(
    dests: &[data_sinks::TPlanFragmentDestination],
    field_name: &str,
) -> Result<(), String> {
    for (idx, dest) in dests.iter().enumerate() {
        validate_network_address(
            dest.brpc_server
                .as_ref()
                .or_else(|| dest.deprecated_server.as_ref()),
            "missing destination address",
            &format!("{field_name}[{idx}]"),
        )?;
    }
    Ok(())
}

fn validate_internal_addresses(
    exec_params: &internal_service::TPlanFragmentExecParams,
    fragment: Option<&planner::TPlanFragment>,
) -> Result<(), String> {
    if let Some(dests) = exec_params.destinations.as_ref() {
        validate_destinations(dests, "destinations")?;
    }
    if let Some(params) = exec_params.runtime_filter_params.as_ref() {
        if let Some(id_to_probers) = params.id_to_prober_params.as_ref() {
            for (filter_id, probers) in id_to_probers {
                for (idx, prober) in probers.iter().enumerate() {
                    validate_network_address(
                        prober.fragment_instance_address.as_ref(),
                        "missing runtime filter prober address",
                        &format!(
                            "runtime_filter_params.id_to_prober_params[{filter_id}][{idx}].fragment_instance_address"
                        ),
                    )?;
                }
            }
        }
    }
    if let Some(fragment) = fragment {
        if let Some(plan) = fragment.plan.as_ref() {
            for node in &plan.nodes {
                if let Some(fetch) = node.fetch_node.as_ref() {
                    if let Some(nodes_info) = fetch.nodes_info.as_ref() {
                        validate_nodes_info(nodes_info, "fetch.nodes_info")?;
                    }
                }
                if let Some(join) = node.hash_join_node.as_ref() {
                    if let Some(filters) = join.build_runtime_filters.as_ref() {
                        for (filter_idx, desc) in filters.iter().enumerate() {
                            if let Some(merge_nodes) = desc.runtime_filter_merge_nodes.as_ref() {
                                for (node_idx, addr) in merge_nodes.iter().enumerate() {
                                    validate_network_address(
                                        Some(addr),
                                        "missing runtime filter merge address",
                                        &format!(
                                            "hash_join.build_runtime_filters[{filter_idx}].runtime_filter_merge_nodes[{node_idx}]"
                                        ),
                                    )?;
                                }
                            }
                        }
                    }
                }
            }
        }
        if let Some(sink) = fragment.output_sink.as_ref() {
            match sink.type_ {
                data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
                    let Some(multi) = sink.multi_cast_stream_sink.as_ref() else {
                        return Err(
                            "MULTI_CAST_DATA_STREAM_SINK missing multi_cast_stream_sink payload"
                                .to_string(),
                        );
                    };
                    for (idx, dests) in multi.destinations.iter().enumerate() {
                        validate_destinations(
                            dests,
                            &format!("multi_cast_stream_sink.destinations[{idx}]"),
                        )?;
                    }
                }
                data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK => {
                    let Some(split) = sink.split_stream_sink.as_ref() else {
                        return Err(
                            "SPLIT_DATA_STREAM_SINK missing split_stream_sink payload".to_string()
                        );
                    };
                    if let Some(destinations) = split.destinations.as_ref() {
                        for (idx, dests) in destinations.iter().enumerate() {
                            validate_destinations(
                                dests,
                                &format!("split_stream_sink.destinations[{idx}]"),
                            )?;
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

// TODO(novarocks): Align with StarRocks BE by plumbing
// `node_to_per_driver_seq_scan_ranges` through scan lowering and morsel scheduling directly.
// Current implementation is a compatibility shim:
// FE may send scan ranges only via `node_to_per_driver_seq_scan_ranges` in pipeline mode.
// We fill missing/no-concrete `per_node_scan_ranges[node_id]` by flattening per-driver ranges so
// existing lowering paths can consume scan ranges deterministically.
// "no-concrete" means all entries are `empty=true` placeholders.
fn backfill_per_node_scan_ranges(exec_params: &mut internal_service::TPlanFragmentExecParams) {
    fn has_concrete_scan_range(ranges: &[internal_service::TScanRangeParams]) -> bool {
        ranges.iter().any(|range| !range.empty.unwrap_or(false))
    }

    let Some(node_to_per_driver) = exec_params.node_to_per_driver_seq_scan_ranges.as_ref() else {
        return;
    };
    let mut to_insert = Vec::new();
    for (node_id, per_driver) in node_to_per_driver {
        let existing = exec_params.per_node_scan_ranges.get(node_id);
        let need_backfill = existing
            .map(|ranges| !has_concrete_scan_range(ranges))
            .unwrap_or(true);
        if !need_backfill {
            continue;
        }
        let flattened = per_driver
            .values()
            .flat_map(|ranges| ranges.iter().cloned())
            .collect::<Vec<_>>();
        if flattened.is_empty() {
            if existing.is_none() {
                to_insert.push((*node_id, Vec::new()));
            }
            continue;
        }
        to_insert.push((*node_id, flattened));
    }
    if to_insert.is_empty() {
        return;
    }
    for (node_id, ranges) in to_insert {
        exec_params.per_node_scan_ranges.insert(node_id, ranges);
    }
}

fn append_incremental_scan_ranges(
    exec_params: &mut internal_service::TPlanFragmentExecParams,
) -> Result<(), String> {
    backfill_per_node_scan_ranges(exec_params);
    let finst_id = UniqueId {
        hi: exec_params.fragment_instance_id.hi,
        lo: exec_params.fragment_instance_id.lo,
    };
    let mgr = query_context_manager();
    for (node_id, scan_ranges) in &exec_params.per_node_scan_ranges {
        if scan_ranges.is_empty() {
            continue;
        }
        mgr.append_incremental_scan_ranges(finst_id, *node_id, scan_ranges.clone())?;
    }
    Ok(())
}

fn add_exchange_sender_counts(counts: &mut HashMap<i32, usize>, fragment: &planner::TPlanFragment) {
    let Some(sink) = fragment.output_sink.as_ref() else {
        return;
    };
    match sink.type_ {
        data_sinks::TDataSinkType::DATA_STREAM_SINK => {
            if let Some(stream_sink) = sink.stream_sink.as_ref() {
                *counts.entry(stream_sink.dest_node_id).or_insert(0) += 1;
            } else {
                warn!(
                    target: "novarocks::exec",
                    "DATA_STREAM_SINK missing stream_sink payload while collecting senders"
                );
            }
        }
        data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
            if let Some(multi) = sink.multi_cast_stream_sink.as_ref() {
                for stream_sink in &multi.sinks {
                    *counts.entry(stream_sink.dest_node_id).or_insert(0) += 1;
                }
            } else {
                warn!(
                    target: "novarocks::exec",
                    "MULTI_CAST_DATA_STREAM_SINK missing multi_cast_stream_sink payload while collecting senders"
                );
            }
        }
        data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK => {
            if let Some(split) = sink.split_stream_sink.as_ref() {
                if let Some(sinks) = split.sinks.as_ref() {
                    for stream_sink in sinks {
                        *counts.entry(stream_sink.dest_node_id).or_insert(0) += 1;
                    }
                } else {
                    warn!(
                        target: "novarocks::exec",
                        "SPLIT_DATA_STREAM_SINK missing sinks while collecting senders"
                    );
                }
            } else {
                warn!(
                    target: "novarocks::exec",
                    "SPLIT_DATA_STREAM_SINK missing split_stream_sink payload while collecting senders"
                );
            }
        }
        _ => {}
    }
}

fn collect_exchange_sender_counts(
    common: Option<&internal_service::TExecPlanFragmentParams>,
    unique: &[internal_service::TExecPlanFragmentParams],
) -> HashMap<i32, usize> {
    let mut counts = HashMap::new();
    if unique.is_empty() {
        if let Some(fragment) = common.and_then(|c| c.fragment.as_ref()) {
            add_exchange_sender_counts(&mut counts, fragment);
        }
        return counts;
    }

    for one in unique {
        let fragment = one
            .fragment
            .as_ref()
            .or_else(|| common.and_then(|c| c.fragment.as_ref()));
        if let Some(fragment) = fragment {
            add_exchange_sender_counts(&mut counts, fragment);
        }
    }
    counts
}

fn columns_for_output_exprs(
    chunk: &Chunk,
    output_exprs: &[exprs::TExpr],
) -> Result<Vec<ArrayRef>, String> {
    let mut out = Vec::with_capacity(output_exprs.len());
    for (col_idx, e) in output_exprs.iter().enumerate() {
        let root = e
            .nodes
            .get(0)
            .ok_or_else(|| format!("output_exprs[{}] is empty", col_idx))?;
        if root.node_type != exprs::TExprNodeType::SLOT_REF {
            return Err(format!(
                "unsupported output expr node_type at index {}: {:?} (expected SLOT_REF)",
                col_idx, root.node_type
            ));
        }
        let slot = root.slot_ref.as_ref().ok_or_else(|| {
            format!(
                "output_exprs[{}] SLOT_REF missing slot_ref payload",
                col_idx
            )
        })?;
        let slot_id = SlotId::try_from(slot.slot_id)?;
        out.push(chunk.column_by_slot_id(slot_id)?);
    }
    Ok(out)
}

fn primitives_for_output_exprs(
    output_exprs: &[exprs::TExpr],
) -> Result<Vec<types::TPrimitiveType>, String> {
    let mut out = Vec::with_capacity(output_exprs.len());
    for (col_idx, e) in output_exprs.iter().enumerate() {
        let root = e
            .nodes
            .get(0)
            .ok_or_else(|| format!("output_exprs[{}] is empty", col_idx))?;
        let primitive =
            primitive_type_from_desc(&root.type_).unwrap_or(types::TPrimitiveType::INVALID_TYPE);
        out.push(primitive);
    }
    Ok(out)
}

fn parse_lenenc_fields(
    row: &[u8],
    expected_columns: usize,
) -> Result<Vec<Option<Vec<u8>>>, String> {
    let mut fields = Vec::with_capacity(expected_columns);
    let mut cursor = 0usize;
    while fields.len() < expected_columns {
        let marker = *row
            .get(cursor)
            .ok_or_else(|| "mysql text row ended unexpectedly".to_string())?;
        cursor += 1;

        if marker == 0xFB {
            fields.push(None);
            continue;
        }

        let len = if marker < 0xFB {
            marker as usize
        } else if marker == 0xFC {
            let bytes = row
                .get(cursor..cursor + 2)
                .ok_or_else(|| "mysql text row invalid 0xFC length".to_string())?;
            cursor += 2;
            u16::from_le_bytes([bytes[0], bytes[1]]) as usize
        } else if marker == 0xFD {
            let bytes = row
                .get(cursor..cursor + 3)
                .ok_or_else(|| "mysql text row invalid 0xFD length".to_string())?;
            cursor += 3;
            (bytes[0] as usize) | ((bytes[1] as usize) << 8) | ((bytes[2] as usize) << 16)
        } else if marker == 0xFE {
            let bytes = row
                .get(cursor..cursor + 8)
                .ok_or_else(|| "mysql text row invalid 0xFE length".to_string())?;
            cursor += 8;
            u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]) as usize
        } else {
            return Err(format!(
                "mysql text row invalid length marker 0x{marker:02x}"
            ));
        };

        let value = row
            .get(cursor..cursor + len)
            .ok_or_else(|| "mysql text row value length exceeds payload".to_string())?;
        cursor += len;
        fields.push(Some(value.to_vec()));
    }
    if cursor != row.len() {
        return Err("mysql text row has trailing bytes".to_string());
    }
    Ok(fields)
}

fn field_bytes<'a>(
    fields: &'a [Option<Vec<u8>>],
    idx: usize,
    field_name: &str,
) -> Result<Option<&'a [u8]>, String> {
    let value = fields
        .get(idx)
        .ok_or_else(|| format!("missing field {field_name} at column {idx}"))?;
    Ok(value.as_deref())
}

fn field_required_i32(
    fields: &[Option<Vec<u8>>],
    idx: usize,
    field_name: &str,
) -> Result<i32, String> {
    let raw = field_bytes(fields, idx, field_name)?
        .ok_or_else(|| format!("field {field_name} at column {idx} is NULL"))?;
    let text = std::str::from_utf8(raw)
        .map_err(|e| format!("field {field_name} is not valid UTF-8: {e}"))?;
    text.parse::<i32>()
        .map_err(|e| format!("field {field_name} parse i32 failed: {e}"))
}

fn field_optional_i64(
    fields: &[Option<Vec<u8>>],
    idx: usize,
    field_name: &str,
) -> Result<Option<i64>, String> {
    let Some(raw) = field_bytes(fields, idx, field_name)? else {
        return Ok(None);
    };
    let text = std::str::from_utf8(raw)
        .map_err(|e| format!("field {field_name} is not valid UTF-8: {e}"))?;
    text.parse::<i64>()
        .map(Some)
        .map_err(|e| format!("field {field_name} parse i64 failed: {e}"))
}

fn field_optional_f64(
    fields: &[Option<Vec<u8>>],
    idx: usize,
    field_name: &str,
) -> Result<Option<f64>, String> {
    let Some(raw) = field_bytes(fields, idx, field_name)? else {
        return Ok(None);
    };
    let text = std::str::from_utf8(raw)
        .map_err(|e| format!("field {field_name} is not valid UTF-8: {e}"))?;
    text.parse::<f64>()
        .map(Some)
        .map_err(|e| format!("field {field_name} parse f64 failed: {e}"))
}

fn field_optional_string(
    fields: &[Option<Vec<u8>>],
    idx: usize,
    field_name: &str,
) -> Result<Option<String>, String> {
    let Some(raw) = field_bytes(fields, idx, field_name)? else {
        return Ok(None);
    };
    let text = std::str::from_utf8(raw)
        .map_err(|e| format!("field {field_name} is not valid UTF-8: {e}"))?;
    Ok(Some(text.to_string()))
}

fn normalize_hll_hex_payload(raw: &[u8]) -> Vec<u8> {
    if raw.len() % 2 == 0 && raw.iter().all(|b| b.is_ascii_hexdigit()) {
        return raw.to_vec();
    }
    hex::encode_upper(raw).into_bytes()
}

fn field_optional_hll_hex_bytes(
    fields: &[Option<Vec<u8>>],
    idx: usize,
    field_name: &str,
) -> Result<Option<Vec<u8>>, String> {
    let Some(raw) = field_bytes(fields, idx, field_name)? else {
        return Ok(None);
    };
    Ok(Some(normalize_hll_hex_payload(raw)))
}

fn decode_dict_base64(input: &str) -> Result<Vec<u8>, String> {
    base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(input)
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(input))
        .map_err(|e| format!("decode dict base64 failed: {e}"))
}

fn parse_global_dict_json(raw: &str) -> Result<data::TGlobalDict, String> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| format!("parse dict json failed: {e}"))?;
    let strings_list = value
        .get("2")
        .and_then(|v| v.get("lst"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| "dict json missing 2.lst".to_string())?;
    let ids_list = value
        .get("3")
        .and_then(|v| v.get("lst"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| "dict json missing 3.lst".to_string())?;

    if strings_list.len() < 2 || ids_list.len() < 2 {
        return Err("dict json list is too short".to_string());
    }
    let string_type = strings_list[0]
        .as_str()
        .ok_or_else(|| "dict strings type is not string".to_string())?;
    if !string_type.eq_ignore_ascii_case("str") {
        return Err(format!("dict strings type mismatch: {string_type}"));
    }
    let ids_type = ids_list[0]
        .as_str()
        .ok_or_else(|| "dict ids type is not string".to_string())?;
    if !ids_type.eq_ignore_ascii_case("i32") {
        return Err(format!("dict ids type mismatch: {ids_type}"));
    }

    let mut strings = Vec::with_capacity(strings_list.len().saturating_sub(2));
    for item in strings_list.iter().skip(2) {
        let encoded = item
            .as_str()
            .ok_or_else(|| "dict encoded string item is not string".to_string())?;
        strings.push(decode_dict_base64(encoded)?);
    }

    let mut ids = Vec::with_capacity(ids_list.len().saturating_sub(2));
    for item in ids_list.iter().skip(2) {
        let id = item
            .as_i64()
            .ok_or_else(|| "dict id item is not integer".to_string())?;
        let id = i32::try_from(id).map_err(|_| "dict id overflows i32".to_string())?;
        ids.push(id);
    }

    Ok(data::TGlobalDict::new(None, Some(strings), Some(ids), None))
}

fn rows_to_statistic_data(
    version: i32,
    fields: &[Option<Vec<u8>>],
) -> Result<data::TStatisticData, String> {
    let cols = fields.len();
    let mut out = data::TStatisticData::default();
    match version {
        STATISTIC_DICT_VERSION => {
            if cols != 3 {
                return Err(format!(
                    "statistic version {version} expects 3 columns, got {cols}"
                ));
            }
            out.meta_version = field_optional_i64(fields, 1, "meta_version")?;
            if let Some(dict_json) = field_optional_string(fields, 2, "dict_json")? {
                out.dict = Some(parse_global_dict_json(&dict_json)?);
            }
        }
        STATISTIC_DATA_VERSION_V1 => {
            if cols != 11 {
                return Err(format!(
                    "statistic version {version} expects 11 columns, got {cols}"
                ));
            }
            out.update_time = field_optional_string(fields, 1, "update_time")?;
            out.db_id = field_optional_i64(fields, 2, "db_id")?;
            out.table_id = field_optional_i64(fields, 3, "table_id")?;
            out.column_name = field_optional_string(fields, 4, "column_name")?;
            out.row_count = field_optional_i64(fields, 5, "row_count")?;
            out.data_size = field_optional_f64(fields, 6, "data_size")?.map(OrderedFloat);
            out.count_distinct = field_optional_i64(fields, 7, "count_distinct")?;
            out.null_count = field_optional_i64(fields, 8, "null_count")?;
            out.max = field_optional_string(fields, 9, "max")?;
            out.min = field_optional_string(fields, 10, "min")?;
        }
        STATISTIC_DATA_VERSION_V2 => {
            if cols != 12 {
                return Err(format!(
                    "statistic version {version} expects 12 columns, got {cols}"
                ));
            }
            out.update_time = field_optional_string(fields, 1, "update_time")?;
            out.db_id = field_optional_i64(fields, 2, "db_id")?;
            out.table_id = field_optional_i64(fields, 3, "table_id")?;
            out.column_name = field_optional_string(fields, 4, "column_name")?;
            out.row_count = field_optional_i64(fields, 5, "row_count")?;
            out.data_size = field_optional_f64(fields, 6, "data_size")?.map(OrderedFloat);
            out.count_distinct = field_optional_i64(fields, 7, "count_distinct")?;
            out.null_count = field_optional_i64(fields, 8, "null_count")?;
            out.max = field_optional_string(fields, 9, "max")?;
            out.min = field_optional_string(fields, 10, "min")?;
            out.collection_size = field_optional_i64(fields, 11, "collection_size")?;
        }
        STATISTIC_HISTOGRAM_VERSION => {
            if cols != 5 {
                return Err(format!(
                    "statistic version {version} expects 5 columns, got {cols}"
                ));
            }
            out.db_id = field_optional_i64(fields, 1, "db_id")?;
            out.table_id = field_optional_i64(fields, 2, "table_id")?;
            out.column_name = field_optional_string(fields, 3, "column_name")?;
            out.histogram = field_optional_string(fields, 4, "histogram")?;
        }
        STATISTIC_EXTERNAL_HISTOGRAM_VERSION => {
            if cols != 3 {
                return Err(format!(
                    "statistic version {version} expects 3 columns, got {cols}"
                ));
            }
            out.column_name = field_optional_string(fields, 1, "column_name")?;
            out.histogram = field_optional_string(fields, 2, "histogram")?;
        }
        STATISTIC_TABLE_VERSION => {
            if cols != 3 {
                return Err(format!(
                    "statistic version {version} expects 3 columns, got {cols}"
                ));
            }
            out.partition_id = field_optional_i64(fields, 1, "partition_id")?;
            out.row_count = field_optional_i64(fields, 2, "row_count")?;
        }
        STATISTIC_BATCH_VERSION => {
            if cols != 9 {
                return Err(format!(
                    "statistic version {version} expects 9 columns, got {cols}"
                ));
            }
            out.partition_id = field_optional_i64(fields, 1, "partition_id")?;
            out.column_name = field_optional_string(fields, 2, "column_name")?;
            out.row_count = field_optional_i64(fields, 3, "row_count")?;
            out.data_size = field_optional_f64(fields, 4, "data_size")?.map(OrderedFloat);
            out.hll = field_optional_hll_hex_bytes(fields, 5, "hll")?;
            out.null_count = field_optional_i64(fields, 6, "null_count")?;
            out.max = field_optional_string(fields, 7, "max")?;
            out.min = field_optional_string(fields, 8, "min")?;
        }
        STATISTIC_BATCH_VERSION_V5 => {
            if cols != 10 {
                return Err(format!(
                    "statistic version {version} expects 10 columns, got {cols}"
                ));
            }
            out.partition_id = field_optional_i64(fields, 1, "partition_id")?;
            out.column_name = field_optional_string(fields, 2, "column_name")?;
            out.row_count = field_optional_i64(fields, 3, "row_count")?;
            out.data_size = field_optional_f64(fields, 4, "data_size")?.map(OrderedFloat);
            out.hll = field_optional_hll_hex_bytes(fields, 5, "hll")?;
            out.null_count = field_optional_i64(fields, 6, "null_count")?;
            out.max = field_optional_string(fields, 7, "max")?;
            out.min = field_optional_string(fields, 8, "min")?;
            out.collection_size = field_optional_i64(fields, 9, "collection_size")?;
        }
        STATISTIC_PARTITION_VERSION => {
            if cols != 4 {
                return Err(format!(
                    "statistic version {version} expects 4 columns, got {cols}"
                ));
            }
            out.partition_id = field_optional_i64(fields, 1, "partition_id")?;
            out.column_name = field_optional_string(fields, 2, "column_name")?;
            out.count_distinct = field_optional_i64(fields, 3, "count_distinct")?;
        }
        STATISTIC_PARTITION_VERSION_V2 => {
            if cols != 6 {
                return Err(format!(
                    "statistic version {version} expects 6 columns, got {cols}"
                ));
            }
            out.partition_id = field_optional_i64(fields, 1, "partition_id")?;
            out.column_name = field_optional_string(fields, 2, "column_name")?;
            out.count_distinct = field_optional_i64(fields, 3, "count_distinct")?;
            out.null_count = field_optional_i64(fields, 4, "null_count")?;
            out.row_count = field_optional_i64(fields, 5, "row_count")?;
        }
        STATISTIC_EXTERNAL_VERSION => {
            if cols != 9 {
                return Err(format!(
                    "statistic version {version} expects 9 columns, got {cols}"
                ));
            }
            out.partition_name = field_optional_string(fields, 1, "partition_name")?;
            out.column_name = field_optional_string(fields, 2, "column_name")?;
            out.row_count = field_optional_i64(fields, 3, "row_count")?;
            out.data_size = field_optional_f64(fields, 4, "data_size")?.map(OrderedFloat);
            out.hll = field_optional_hll_hex_bytes(fields, 5, "hll")?;
            out.null_count = field_optional_i64(fields, 6, "null_count")?;
            out.max = field_optional_string(fields, 7, "max")?;
            out.min = field_optional_string(fields, 8, "min")?;
        }
        STATISTIC_EXTERNAL_QUERY_VERSION => {
            if cols != 8 {
                return Err(format!(
                    "statistic version {version} expects 8 columns, got {cols}"
                ));
            }
            out.column_name = field_optional_string(fields, 1, "column_name")?;
            out.row_count = field_optional_i64(fields, 2, "row_count")?;
            out.data_size = field_optional_f64(fields, 3, "data_size")?.map(OrderedFloat);
            out.count_distinct = field_optional_i64(fields, 4, "count_distinct")?;
            out.null_count = field_optional_i64(fields, 5, "null_count")?;
            out.max = field_optional_string(fields, 6, "max")?;
            out.min = field_optional_string(fields, 7, "min")?;
        }
        STATISTIC_EXTERNAL_QUERY_VERSION_V2 => {
            if cols != 9 {
                return Err(format!(
                    "statistic version {version} expects 9 columns, got {cols}"
                ));
            }
            out.column_name = field_optional_string(fields, 1, "column_name")?;
            out.row_count = field_optional_i64(fields, 2, "row_count")?;
            out.data_size = field_optional_f64(fields, 3, "data_size")?.map(OrderedFloat);
            out.count_distinct = field_optional_i64(fields, 4, "count_distinct")?;
            out.null_count = field_optional_i64(fields, 5, "null_count")?;
            out.max = field_optional_string(fields, 6, "max")?;
            out.min = field_optional_string(fields, 7, "min")?;
            out.update_time = field_optional_string(fields, 8, "update_time")?;
        }
        STATISTIC_MULTI_COLUMN_VERSION => {
            if cols != 3 {
                return Err(format!(
                    "statistic version {version} expects 3 columns, got {cols}"
                ));
            }
            out.column_name = field_optional_string(fields, 1, "column_name")?;
            out.count_distinct = field_optional_i64(fields, 2, "count_distinct")?;
        }
        STATISTIC_QUERY_MULTI_COLUMN_VERSION => {
            if cols != 5 {
                return Err(format!(
                    "statistic version {version} expects 5 columns, got {cols}"
                ));
            }
            out.db_id = field_optional_i64(fields, 1, "db_id")?;
            out.table_id = field_optional_i64(fields, 2, "table_id")?;
            out.column_name = field_optional_string(fields, 3, "column_name")?;
            out.count_distinct = field_optional_i64(fields, 4, "count_distinct")?;
        }
        _ => {
            return Err(format!("unsupported statistic version: {version}"));
        }
    }
    Ok(out)
}

fn build_statistic_fetch_result(
    chunks: &[Chunk],
    output_exprs: &[exprs::TExpr],
) -> Result<FetchResult, String> {
    let mut batch = data::TResultBatch::new(vec![], false, 0, None);
    for chunk in chunks {
        let columns = columns_for_output_exprs(chunk, output_exprs)?;
        let primitives = primitives_for_output_exprs(output_exprs)?;
        for row in 0..chunk.len() {
            let mysql_row =
                mysql_text_row_from_arrays_with_primitives(&columns, row, Some(&primitives))?;
            let fields = parse_lenenc_fields(&mysql_row, columns.len())?;
            let version = field_required_i32(&fields, 0, "version")?;
            let row_sd = rows_to_statistic_data(version, &fields)?;
            if let Some(existing) = batch.statistic_version {
                if existing != version {
                    return Err(format!(
                        "mixed statistic versions in one batch: {} vs {}",
                        existing, version
                    ));
                }
            } else {
                batch.statistic_version = Some(version);
            }
            let encoded = thrift_compact_serialize(&row_sd)?;
            batch.rows.push(encoded);
        }
    }
    if batch.statistic_version.is_none() {
        batch.statistic_version = Some(STATISTIC_DATA_VERSION_V1);
    }
    Ok(FetchResult {
        packet_seq: 0,
        eos: true,
        result_batch: batch,
    })
}

fn build_http_json_fetch_result(
    chunks: &[Chunk],
    output_exprs: Option<&[exprs::TExpr]>,
) -> Result<FetchResult, String> {
    let mut batch = data::TResultBatch::new(vec![], false, 0, None);
    for chunk in chunks {
        if let Some(output_exprs) = output_exprs.filter(|v| !v.is_empty()) {
            let columns = columns_for_output_exprs(chunk, output_exprs)?;
            let primitives = primitives_for_output_exprs(output_exprs)?;
            for row in 0..chunk.len() {
                batch.rows.push(http_json_row_from_arrays_with_primitives(
                    &columns,
                    row,
                    Some(&primitives),
                    None,
                )?);
            }
        } else {
            let columns = chunk.columns();
            let json_semantics = chunk
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    field
                        .metadata()
                        .get(FIELD_META_PRIMITIVE_TYPE)
                        .is_some_and(|value| value == FIELD_META_PRIMITIVE_JSON)
                })
                .collect::<Vec<_>>();
            for row in 0..chunk.len() {
                batch.rows.push(http_json_row_from_arrays_with_primitives(
                    columns,
                    row,
                    None,
                    Some(&json_semantics),
                )?);
            }
        }
    }
    Ok(FetchResult {
        packet_seq: 0,
        eos: true,
        result_batch: batch,
    })
}

fn build_fetch_result(
    chunks: &[Chunk],
    output_exprs: Option<&[exprs::TExpr]>,
    result_sink_type: Option<data_sinks::TResultSinkType>,
    result_sink_format: Option<data_sinks::TResultSinkFormatType>,
) -> Result<FetchResult, String> {
    let is_statistic_sink = matches!(
        result_sink_type,
        Some(t) if t == data_sinks::TResultSinkType::STATISTIC
    );
    if is_statistic_sink {
        let exprs = output_exprs
            .filter(|v| !v.is_empty())
            .ok_or_else(|| "STATISTIC result sink requires non-empty output_exprs".to_string())?;
        return build_statistic_fetch_result(chunks, exprs);
    }
    let is_http_sink = matches!(
        result_sink_type,
        Some(t) if t == data_sinks::TResultSinkType::HTTP_PROTOCAL
    );
    if is_http_sink {
        let format = result_sink_format.unwrap_or(data_sinks::TResultSinkFormatType::JSON);
        if format != data_sinks::TResultSinkFormatType::JSON {
            return Err(format!(
                "HTTP_PROTOCAL result sink only supports JSON format, got {:?}",
                format
            ));
        }
        return build_http_json_fetch_result(chunks, output_exprs);
    }

    let mut batch = data::TResultBatch::new(vec![], false, 0, None);
    for chunk in chunks {
        if let Some(output_exprs) = output_exprs.filter(|v| !v.is_empty()) {
            let columns = columns_for_output_exprs(chunk, output_exprs)?;
            let primitives = primitives_for_output_exprs(output_exprs)?;
            for row in 0..chunk.len() {
                let bytes =
                    mysql_text_row_from_arrays_with_primitives(&columns, row, Some(&primitives))?;
                batch.rows.push(bytes);
            }
        } else {
            let columns = chunk.columns();
            for row in 0..chunk.len() {
                let bytes = mysql_text_row_from_arrays(columns, row)?;
                batch.rows.push(bytes);
            }
        }
    }
    Ok(FetchResult {
        packet_seq: 0,
        eos: true,
        result_batch: batch,
    })
}

fn spawn_exec_fragment(
    fragment: planner::TPlanFragment,
    desc_tbl: Option<descriptors::TDescriptorTable>,
    exec_params: internal_service::TPlanFragmentExecParams,
    query_opts: Option<internal_service::TQueryOptions>,
    session_time_zone: Option<String>,
    pipeline_dop: i32,
    group_execution_scan_dop: Option<i32>,
    db_name: Option<String>,
    finst_id: UniqueId,
    query_id: QueryId,
    backend_num: Option<i32>,
    profiler: Option<Profiler>,
    last_query_id: Option<String>,
    fe_addr: Option<types::TNetworkAddress>,
    mem_tracker: Option<Arc<crate::runtime::mem_tracker::MemTracker>>,
    mgr: Arc<QueryContextManager>,
) {
    let has_result_sink = fragment.output_exprs.is_some();
    let result_sink = fragment
        .output_sink
        .as_ref()
        .and_then(|sink| sink.result_sink.as_ref())
        .cloned();
    let result_sink_type = result_sink.as_ref().and_then(|sink| sink.type_);
    let result_sink_format = result_sink.as_ref().and_then(|sink| sink.format);
    if has_result_sink {
        result_buffer::create_sender(finst_id);
        if let Some(root) = mem_tracker.as_ref() {
            let label = format!("ResultBuffer: finst={}", finst_id);
            let tracker = crate::runtime::mem_tracker::MemTracker::new_child(label, root);
            result_buffer::set_mem_tracker(finst_id, tracker);
        }
    }
    mgr.register_finst(finst_id, query_id);
    std::thread::spawn(move || {
        let query_opts = query_opts.as_ref();
        let wall_start = std::time::Instant::now();
        let profiler_for_wall = profiler.clone();
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            execute_fragment(
                &fragment,
                desc_tbl.as_ref(),
                Some(&exec_params),
                query_opts,
                session_time_zone.as_deref(),
                pipeline_dop,
                group_execution_scan_dop,
                db_name.as_deref(),
                profiler,
                last_query_id.as_deref(),
                fe_addr.as_ref(),
                backend_num,
                mem_tracker,
            )
        }))
        .unwrap_or_else(|payload| {
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic payload".to_string()
            };
            Err(format!("panic in fragment execution: {msg}"))
        });
        if let Some(p) = profiler_for_wall.as_ref() {
            let elapsed_ns =
                crate::runtime::profile::clamp_u128_to_i64(wall_start.elapsed().as_nanos());
            p.counter_set(
                "QueryExecutionWallTime",
                crate::metrics::TUnit::TIME_NS,
                elapsed_ns,
            );
        }
        let mut report_error: Option<String> = None;
        if has_result_sink {
            let output_exprs = fragment.output_exprs.as_deref();
            match out.and_then(|out| {
                if let Some(json) = out.profile_json.as_deref() {
                    info!(
                        target: "novarocks::profile",
                        finst_id = %finst_id,
                        profile_bytes = json.len(),
                        "fragment_profile"
                    );
                }
                build_fetch_result(
                    &out.chunks,
                    output_exprs,
                    result_sink_type,
                    result_sink_format,
                )
            }) {
                Ok(result) => {
                    result_buffer::insert(finst_id, result);
                    result_buffer::close_ok(finst_id);
                }
                Err(e) => {
                    report_error = Some(e.clone());
                    error!(
                        target: "novarocks::exec",
                        finst_id = %finst_id,
                        error = %e,
                        "exec_plan_fragment failed"
                    );
                    result_buffer::close_error(finst_id, e);
                }
            }
        } else if let Err(e) = out {
            report_error = Some(e.clone());
            error!(
                target: "novarocks::exec",
                finst_id = %finst_id,
                error = %e,
                "exec_plan_fragment failed"
            );
        }
        if let Some(ref err_msg) = report_error {
            let finsts = mgr.cancel_query(query_id, err_msg.clone());
            for id in finsts {
                result_buffer::close_error(id, err_msg.clone());
                exchange::cancel_fragment(id.hi, id.lo);
            }
        }
        fe_report::report_fragment_done(finst_id, report_error);
        mgr.unregister_finst(finst_id);
        mgr.finish_fragment(query_id);
    });
}

pub fn submit_exec_batch_plan_fragments(thrift_bytes: &[u8]) -> Result<usize, String> {
    let batch: internal_service::TExecBatchPlanFragmentsParams =
        thrift_binary_deserialize(thrift_bytes)?;
    if debug_exec_batch_plan_json() {
        match thrift_named_json(&batch) {
            Ok(json) => info!(
                target: "novarocks::rpc",
                rpc = "exec_batch_plan_fragments",
                named_json = %json,
                "named_json"
            ),
            Err(e) => warn!(
                target: "novarocks::rpc",
                rpc = "exec_batch_plan_fragments",
                error = %e,
                "named_json_failed"
            ),
        }
    }
    let common = batch.common_param.as_ref();
    let unique = batch.unique_param_per_instance.unwrap_or_default();
    let mgr = query_context_manager();
    let common_desc_tbl = common.and_then(|c| c.desc_tbl.as_ref());
    let common_query_opts = common.and_then(|c| c.query_options.as_ref());
    let common_query_id = common.and_then(|c| c.params.as_ref()).map(|p| QueryId {
        hi: p.query_id.hi,
        lo: p.query_id.lo,
    });
    let sender_counts = collect_exchange_sender_counts(common, &unique);
    let mut sender_counts_applied = false;
    if let Some(query_id) = common_query_id {
        let (delivery_expire, query_expire) = query_expire_durations(common_query_opts);
        let require_existing = common_desc_tbl.map(desc_tbl_is_cached).unwrap_or(false);
        mgr.ensure_context(query_id, require_existing, delivery_expire, query_expire)?;
        if let Some(desc_tbl) = common_desc_tbl {
            if !desc_tbl_is_cached(desc_tbl) && !is_desc_tbl_effectively_empty(desc_tbl) {
                mgr.with_context_mut(query_id, |ctx| {
                    ctx.desc_tbl = Some(desc_tbl.clone());
                    Ok(())
                })?;
            }
        }
    }

    let mut created = 0usize;

    let mut query_id_for_batch = common_query_id;
    for one in unique.iter() {
        let params = one
            .params
            .as_ref()
            .or_else(|| common.and_then(|c| c.params.as_ref()));
        let fragment = one
            .fragment
            .as_ref()
            .or_else(|| common.and_then(|c| c.fragment.as_ref()));
        let coord = one
            .coord
            .as_ref()
            .or_else(|| common.and_then(|c| c.coord.as_ref()));
        let backend_num = one
            .backend_num
            .or_else(|| common.and_then(|c| c.backend_num));
        // NOTE: backend_num must match FE's instance index (ExecutionDAG index).
        // If this value is wrong, FE will treat reportExecStatus as "unknown backend number"
        // and drop sink_commit_infos, causing Iceberg commit to be skipped.
        let db_name = choose_nonempty_str(
            one.db_name.as_deref(),
            common.and_then(|c| c.db_name.as_deref()),
        );
        let query_opts = one
            .query_options
            .as_ref()
            .or(common.and_then(|c| c.query_options.as_ref()));
        let query_globals = one
            .query_globals
            .as_ref()
            .or_else(|| common.and_then(|c| c.query_globals.as_ref()));
        let last_query_id = query_globals
            .and_then(|g| g.last_query_id.as_deref())
            .map(|s| s.to_string());
        let session_time_zone = query_globals.and_then(|g| g.time_zone.clone());

        let Some(exec_params) = params else {
            continue;
        };
        let Some(fragment) = fragment else {
            continue;
        };

        let query_id = QueryId {
            hi: exec_params.query_id.hi,
            lo: exec_params.query_id.lo,
        };
        if let Some(existing) = query_id_for_batch {
            if existing != query_id {
                return Err("mixed query_id in exec_batch_plan_fragments".to_string());
            }
        } else {
            query_id_for_batch = Some(query_id);
        }

        let (delivery_expire, query_expire) = query_expire_durations(query_opts);
        let require_existing = one
            .desc_tbl
            .as_ref()
            .map(desc_tbl_is_cached)
            .unwrap_or(false);
        mgr.get_or_register(query_id, require_existing, delivery_expire, query_expire)?;
        let cache_options = CacheOptions::from_query_options(query_opts)?;
        mgr.set_cache_options(query_id, cache_options)?;
        if !sender_counts_applied && !sender_counts.is_empty() {
            mgr.update_exchange_sender_counts(query_id, sender_counts.clone())?;
            sender_counts_applied = true;
        }
        let desc_tbl = resolve_desc_tbl_for_instance(
            mgr.as_ref(),
            query_id,
            one.desc_tbl.as_ref(),
            common_desc_tbl,
        )?;

        let finst_id = UniqueId {
            hi: exec_params.fragment_instance_id.hi,
            lo: exec_params.fragment_instance_id.lo,
        };
        let query_mem_tracker = mgr
            .query_mem_tracker(query_id)
            .ok_or_else(|| "QueryContext missing mem_tracker".to_string())?;
        let fragment_label = format!("fragment_{:x}_{:x}", finst_id.hi, finst_id.lo);
        let fragment_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);
        // Result buffer timeout is derived from QueryContext by finst_id.
        let enable_profile = query_opts
            .and_then(|opts| opts.enable_profile)
            .unwrap_or(false);
        let profiler = if enable_profile {
            Some(Profiler::new(profile_name_for_fragment(fragment)))
        } else {
            None
        };
        let report_interval_ns = if enable_profile {
            let from_query = query_opts
                .and_then(|opts| opts.runtime_profile_report_interval)
                .filter(|v| *v > 0)
                .and_then(|v| v.checked_mul(1_000_000_000));
            from_query.or_else(|| {
                app_config::config()
                    .ok()
                    .map(|cfg| cfg.runtime.profile_report_interval.max(1) * 1_000_000_000)
            })
        } else {
            None
        };
        if let (Some(coord), Some(backend_num)) = (coord.cloned(), backend_num) {
            fe_report::register_instance(
                finst_id,
                query_id,
                coord,
                backend_num,
                enable_profile,
                profiler.clone(),
                Some(Arc::clone(&fragment_mem_tracker)),
                Some(Arc::clone(&query_mem_tracker)),
                report_interval_ns,
            );
        } else {
            warn!(
                target: "novarocks::report",
                finst_id = %finst_id,
                "missing coord/backend_num for reportExecStatus"
            );
        }
        mgr.with_context_mut(query_id, |ctx| {
            observe_total_fragments(ctx, exec_params);
            Ok(())
        })?;
        cache_iceberg_table_locations(desc_tbl.as_ref());
        let pipeline_dop = resolve_pipeline_dop(one);
        let group_execution_scan_dop = one.group_execution_scan_dop;
        let query_opts = query_opts.cloned();
        let mut exec_params = exec_params.clone();
        let fragment = fragment.clone();
        backfill_per_node_scan_ranges(&mut exec_params);
        validate_internal_addresses(&exec_params, Some(&fragment))?;
        if let Some(params) = exec_params.runtime_filter_params.clone() {
            let _ = mgr.set_runtime_filter_params(query_id, params);
        }
        spawn_exec_fragment(
            fragment,
            desc_tbl.clone(),
            exec_params,
            query_opts,
            session_time_zone,
            pipeline_dop,
            group_execution_scan_dop,
            db_name.map(|s| s.to_string()),
            finst_id,
            query_id,
            backend_num,
            profiler,
            last_query_id,
            coord.cloned(),
            Some(fragment_mem_tracker),
            Arc::clone(&mgr),
        );
        created += 1;
    }

    if !sender_counts_applied && !sender_counts.is_empty() {
        if let Some(query_id) = query_id_for_batch {
            mgr.update_exchange_sender_counts(query_id, sender_counts)?;
        }
    }

    if query_id_for_batch.is_none() {
        return Ok(0);
    }
    Ok(created)
}

pub fn submit_exec_plan_fragment(thrift_bytes: &[u8]) -> Result<(), String> {
    let one: internal_service::TExecPlanFragmentParams = thrift_binary_deserialize(thrift_bytes)?;
    if debug_exec_batch_plan_json() {
        match thrift_named_json(&one) {
            Ok(json) => info!(
                target: "novarocks::rpc",
                rpc = "exec_plan_fragment",
                named_json = %json,
                "named_json"
            ),
            Err(e) => warn!(
                target: "novarocks::rpc",
                rpc = "exec_plan_fragment",
                error = %e,
                "named_json_failed"
            ),
        }
    }
    let Some(params) = one.params.as_ref() else {
        return Err("missing params in TExecPlanFragmentParams".to_string());
    };
    if one.fragment.is_none() {
        let mut params = params.clone();
        append_incremental_scan_ranges(&mut params)?;
        return Ok(());
    }
    let fragment = one.fragment.as_ref().expect("checked above");
    let coord = one.coord.as_ref();
    let backend_num = one.backend_num;
    let finst_id = UniqueId {
        hi: params.fragment_instance_id.hi,
        lo: params.fragment_instance_id.lo,
    };
    let query_id = QueryId {
        hi: params.query_id.hi,
        lo: params.query_id.lo,
    };
    let query_opts = one.query_options.as_ref();
    let query_globals = one.query_globals.as_ref();
    let last_query_id = query_globals
        .and_then(|g| g.last_query_id.as_deref())
        .map(|s| s.to_string());
    let session_time_zone = query_globals.and_then(|g| g.time_zone.clone());
    let (delivery_expire, query_expire) = query_expire_durations(query_opts);
    let mgr = query_context_manager();
    let require_existing = one
        .desc_tbl
        .as_ref()
        .map(desc_tbl_is_cached)
        .unwrap_or(false);
    mgr.get_or_register(query_id, require_existing, delivery_expire, query_expire)?;
    let cache_options = CacheOptions::from_query_options(query_opts)?;
    mgr.set_cache_options(query_id, cache_options)?;
    mgr.with_context_mut(query_id, |ctx| {
        observe_total_fragments(ctx, params);
        Ok(())
    })?;
    let query_mem_tracker = mgr
        .query_mem_tracker(query_id)
        .ok_or_else(|| "QueryContext missing mem_tracker".to_string())?;
    let fragment_label = format!("fragment_{:x}_{:x}", finst_id.hi, finst_id.lo);
    let fragment_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);
    let desc_tbl =
        resolve_desc_tbl_for_instance(mgr.as_ref(), query_id, one.desc_tbl.as_ref(), None)?;
    cache_iceberg_table_locations(desc_tbl.as_ref());
    // Result buffer timeout is derived from QueryContext by finst_id.
    let enable_profile = query_opts
        .and_then(|opts| opts.enable_profile)
        .unwrap_or(false);
    let profiler = if enable_profile {
        Some(Profiler::new(profile_name_for_fragment(fragment)))
    } else {
        None
    };
    let report_interval_ns = if enable_profile {
        let from_query = query_opts
            .and_then(|opts| opts.runtime_profile_report_interval)
            .filter(|v| *v > 0)
            .and_then(|v| v.checked_mul(1_000_000_000));
        from_query.or_else(|| {
            app_config::config()
                .ok()
                .map(|cfg| cfg.runtime.profile_report_interval.max(1) * 1_000_000_000)
        })
    } else {
        None
    };
    if let (Some(coord), Some(backend_num)) = (coord.cloned(), backend_num) {
        fe_report::register_instance(
            finst_id,
            query_id,
            coord,
            backend_num,
            enable_profile,
            profiler.clone(),
            Some(Arc::clone(&fragment_mem_tracker)),
            Some(Arc::clone(&query_mem_tracker)),
            report_interval_ns,
        );
    } else {
        warn!(
            target: "novarocks::report",
            finst_id = %finst_id,
            "missing coord/backend_num for reportExecStatus"
        );
    }

    let pipeline_dop = resolve_pipeline_dop(&one);
    let group_execution_scan_dop = one.group_execution_scan_dop;

    let mut params = params.clone();
    let fragment = fragment.clone();
    backfill_per_node_scan_ranges(&mut params);
    validate_internal_addresses(&params, Some(&fragment))?;
    if let Some(rf_params) = params.runtime_filter_params.clone() {
        let _ = mgr.set_runtime_filter_params(query_id, rf_params);
    }
    spawn_exec_fragment(
        fragment,
        desc_tbl.clone(),
        params,
        one.query_options.clone(),
        session_time_zone,
        pipeline_dop,
        group_execution_scan_dop,
        one.db_name.clone(),
        finst_id,
        query_id,
        backend_num,
        profiler,
        last_query_id,
        coord.cloned(),
        Some(fragment_mem_tracker),
        Arc::clone(&mgr),
    );
    Ok(())
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct SyncExecPlanResult {
    pub(crate) finst_id: UniqueId,
}

pub(crate) fn execute_plan_fragment_sync(
    one: internal_service::TExecPlanFragmentParams,
) -> Result<SyncExecPlanResult, String> {
    let Some(params) = one.params.as_ref() else {
        return Err("missing params in TExecPlanFragmentParams".to_string());
    };
    let Some(fragment) = one.fragment.as_ref() else {
        return Err("missing fragment in TExecPlanFragmentParams".to_string());
    };

    let finst_id = UniqueId {
        hi: params.fragment_instance_id.hi,
        lo: params.fragment_instance_id.lo,
    };
    let query_id = QueryId {
        hi: params.query_id.hi,
        lo: params.query_id.lo,
    };

    let query_opts = one.query_options.as_ref();
    let query_globals = one.query_globals.as_ref();
    let last_query_id = query_globals.and_then(|g| g.last_query_id.as_deref());
    let session_time_zone = query_globals.and_then(|g| g.time_zone.as_deref());
    let (delivery_expire, query_expire) = query_expire_durations(query_opts);
    let mgr = query_context_manager();
    let require_existing = one
        .desc_tbl
        .as_ref()
        .map(desc_tbl_is_cached)
        .unwrap_or(false);
    mgr.get_or_register(query_id, require_existing, delivery_expire, query_expire)?;
    let cache_options = CacheOptions::from_query_options(query_opts)?;
    mgr.set_cache_options(query_id, cache_options)?;
    mgr.with_context_mut(query_id, |ctx| {
        observe_total_fragments(ctx, params);
        Ok(())
    })?;

    let query_mem_tracker = mgr
        .query_mem_tracker(query_id)
        .ok_or_else(|| "QueryContext missing mem_tracker".to_string())?;
    let fragment_label = format!("fragment_{:x}_{:x}", finst_id.hi, finst_id.lo);
    let fragment_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);
    let desc_tbl =
        resolve_desc_tbl_for_instance(mgr.as_ref(), query_id, one.desc_tbl.as_ref(), None)?;
    cache_iceberg_table_locations(desc_tbl.as_ref());

    let pipeline_dop = resolve_pipeline_dop(&one);
    let group_execution_scan_dop = one.group_execution_scan_dop;
    let mut params = params.clone();
    let fragment = fragment.clone();
    backfill_per_node_scan_ranges(&mut params);
    validate_internal_addresses(&params, Some(&fragment))?;
    if let Some(rf_params) = params.runtime_filter_params.clone() {
        let _ = mgr.set_runtime_filter_params(query_id, rf_params);
    }

    let exec_result = execute_fragment(
        &fragment,
        desc_tbl.as_ref(),
        Some(&params),
        query_opts,
        session_time_zone,
        pipeline_dop,
        group_execution_scan_dop,
        one.db_name.as_deref(),
        None,
        last_query_id,
        one.coord.as_ref(),
        one.backend_num,
        Some(fragment_mem_tracker),
    );
    mgr.finish_fragment(query_id);

    match exec_result {
        Ok(_) => Ok(SyncExecPlanResult { finst_id }),
        Err(err) => {
            crate::runtime::sink_commit::unregister(finst_id);
            Err(err)
        }
    }
}

fn resolve_pipeline_dop(request: &internal_service::TExecPlanFragmentParams) -> i32 {
    // Align with StarRocks: pipeline_dop is a per-fragment-instance (unique request) parameter.
    crate::runtime::exec_env::calc_pipeline_dop(request.pipeline_dop.unwrap_or(0))
}

pub fn cancel(finst_id: UniqueId) {
    let mgr = query_context_manager();
    let query_id = mgr.query_id_by_finst(finst_id);
    let cancel_reason = format!("query canceled by FE: finst={}", finst_id);
    let mut target_finsts = query_id
        .map(|qid| mgr.cancel_query(qid, cancel_reason.clone()))
        .unwrap_or_default();
    if target_finsts.is_empty() {
        target_finsts.push(finst_id);
    }

    info!(
        target: "novarocks::exec",
        finst_id = %finst_id,
        query_id = ?query_id,
        canceled_fragments = target_finsts.len(),
        "cancel request received"
    );

    for id in &target_finsts {
        result_buffer::cancel(*id);
        exchange::cancel_fragment(id.hi, id.lo);
    }

    // Fallback cleanup for detached/unknown finst that cannot be mapped to a query context.
    if query_id.is_none() {
        mgr.unregister_finst(finst_id);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::validate_internal_addresses;
    use crate::{
        data_sinks, descriptors, exprs, internal_service, partitions, plan_nodes, planner,
        runtime_filter, types,
    };

    fn unique_id(hi: i64, lo: i64) -> types::TUniqueId {
        types::TUniqueId::new(hi, lo)
    }

    fn address(host: &str, port: i32) -> types::TNetworkAddress {
        types::TNetworkAddress::new(host.to_string(), port)
    }

    fn exec_params(
        destination: Option<types::TNetworkAddress>,
        prober: Option<types::TNetworkAddress>,
    ) -> internal_service::TPlanFragmentExecParams {
        internal_service::TPlanFragmentExecParams {
            query_id: unique_id(1, 2),
            fragment_instance_id: unique_id(3, 4),
            per_node_scan_ranges: BTreeMap::new(),
            per_exch_num_senders: BTreeMap::new(),
            destinations: Some(vec![data_sinks::TPlanFragmentDestination::new(
                unique_id(5, 6),
                None::<types::TNetworkAddress>,
                destination,
                None::<i32>,
            )]),
            sender_id: None,
            num_senders: None,
            send_query_statistics_with_every_batch: None,
            use_vectorized: None,
            runtime_filter_params: Some(runtime_filter::TRuntimeFilterParams {
                id_to_prober_params: Some(BTreeMap::from([(
                    7,
                    vec![runtime_filter::TRuntimeFilterProberParams {
                        fragment_instance_id: None,
                        fragment_instance_address: prober,
                    }],
                )])),
                runtime_filter_builder_number: None,
                runtime_filter_max_size: None,
                skew_join_runtime_filters: None,
            }),
            instances_number: None,
            enable_exchange_pass_through: None,
            node_to_per_driver_seq_scan_ranges: None,
            enable_exchange_perf: None,
            pipeline_sink_dop: None,
            report_when_finish: None,
            exec_debug_options: None,
        }
    }

    fn plan_node(
        node_id: i32,
        node_type: plan_nodes::TPlanNodeType,
        hash_join_node: Option<plan_nodes::THashJoinNode>,
        fetch_node: Option<plan_nodes::TFetchNode>,
    ) -> plan_nodes::TPlanNode {
        plan_nodes::TPlanNode::new(
            node_id,
            node_type,
            0,
            -1,
            Vec::new(),
            Vec::new(),
            None::<Vec<exprs::TExpr>>,
            false,
            None::<plan_nodes::TPlanNodeCommon>,
            hash_join_node,
            None::<plan_nodes::TAggregationNode>,
            None::<plan_nodes::TSortNode>,
            None::<plan_nodes::TMergeNode>,
            None::<plan_nodes::TExchangeNode>,
            None::<plan_nodes::TMySQLScanNode>,
            None::<plan_nodes::TOlapScanNode>,
            None::<plan_nodes::TFileScanNode>,
            None::<plan_nodes::TSchemaScanNode>,
            None::<plan_nodes::TMetaScanNode>,
            None::<plan_nodes::TAnalyticNode>,
            None::<plan_nodes::TUnionNode>,
            None::<plan_nodes::TBackendResourceProfile>,
            None::<plan_nodes::TEsScanNode>,
            None::<plan_nodes::TRepeatNode>,
            None::<plan_nodes::TAssertNumRowsNode>,
            None::<plan_nodes::TIntersectNode>,
            None::<plan_nodes::TExceptNode>,
            None::<plan_nodes::TMergeJoinNode>,
            None::<plan_nodes::TRawValuesNode>,
            None::<bool>,
            None::<plan_nodes::THdfsScanNode>,
            None::<plan_nodes::TProjectNode>,
            None::<plan_nodes::TTableFunctionNode>,
            None::<Vec<runtime_filter::TRuntimeFilterDescription>>,
            None::<plan_nodes::TDecodeNode>,
            None::<std::collections::BTreeSet<types::TPlanNodeId>>,
            None::<Vec<types::TSlotId>>,
            None::<bool>,
            None::<plan_nodes::TJDBCScanNode>,
            None::<plan_nodes::TConnectorScanNode>,
            None::<plan_nodes::TCrossJoinNode>,
            None::<plan_nodes::TLakeScanNode>,
            None::<plan_nodes::TNestLoopJoinNode>,
            None::<plan_nodes::TStarRocksScanNode>,
            None::<plan_nodes::TStreamScanNode>,
            None::<plan_nodes::TStreamJoinNode>,
            None::<plan_nodes::TStreamAggregationNode>,
            None::<plan_nodes::TSelectNode>,
            fetch_node,
            None::<plan_nodes::TLookUpNode>,
            None::<plan_nodes::TBenchmarkScanNode>,
        )
    }

    fn fragment(fetch_port: i32, merge_port: i32) -> planner::TPlanFragment {
        let fetch_node = plan_nodes::TFetchNode::new(
            Some(8),
            None::<BTreeMap<types::TTupleId, descriptors::TRowPositionDescriptor>>,
            Some(descriptors::TNodesInfo::new(
                1,
                vec![descriptors::TNodeInfo::new(
                    1,
                    0,
                    "fetch-host".to_string(),
                    fetch_port,
                )],
            )),
        );
        let hash_join_node = plan_nodes::THashJoinNode {
            join_op: plan_nodes::TJoinOp::INNER_JOIN,
            eq_join_conjuncts: Vec::new(),
            other_join_conjuncts: None,
            is_push_down: None,
            add_probe_filters: None,
            is_rewritten_from_not_in: None,
            sql_join_predicates: None,
            sql_predicates: None,
            build_runtime_filters: Some(vec![runtime_filter::TRuntimeFilterDescription {
                filter_id: Some(7),
                build_expr: None,
                expr_order: None,
                plan_node_id_to_target_expr: None,
                has_remote_targets: None,
                bloom_filter_size: None,
                runtime_filter_merge_nodes: Some(vec![address("merge-host", merge_port)]),
                build_join_mode: None,
                sender_finst_id: None,
                build_plan_node_id: None,
                broadcast_grf_senders: None,
                broadcast_grf_destinations: None,
                bucketseq_to_instance: None,
                plan_node_id_to_partition_by_exprs: None,
                filter_type: None,
                layout: None,
                build_from_group_execution: None,
                is_broad_cast_join_in_skew: None,
                skew_shuffle_filter_id: None,
            }]),
            build_runtime_filters_from_planner: None,
            distribution_mode: None,
            partition_exprs: None,
            output_columns: None,
            interpolate_passthrough: None,
            late_materialization: None,
            enable_partition_hash_join: None,
            is_skew_join: None,
            common_slot_map: None,
            asof_join_condition: None,
        };
        planner::TPlanFragment {
            plan: Some(plan_nodes::TPlan::new(vec![
                plan_node(
                    10,
                    plan_nodes::TPlanNodeType::FETCH_NODE,
                    None,
                    Some(fetch_node),
                ),
                plan_node(
                    11,
                    plan_nodes::TPlanNodeType::HASH_JOIN_NODE,
                    Some(hash_join_node),
                    None,
                ),
            ])),
            output_exprs: None,
            output_sink: None,
            partition: partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None::<Vec<exprs::TExpr>>,
                None::<Vec<partitions::TRangePartition>>,
                None::<Vec<partitions::TBucketProperty>>,
            ),
            min_reservation_bytes: None,
            initial_reservation_total_claims: None,
            query_global_dicts: None,
            load_global_dicts: None,
            cache_param: None,
            query_global_dict_exprs: None,
            group_execution_param: None,
        }
    }

    #[test]
    fn test_validate_internal_addresses_preserves_plan_ports() {
        let exec_params = exec_params(
            Some(address("dest-host", 9030)),
            Some(address("prober-host", 9040)),
        );
        let fragment = fragment(9050, 9060);

        validate_internal_addresses(&exec_params, Some(&fragment))
            .expect("validate internal addresses");

        assert_eq!(
            exec_params.destinations.as_ref().expect("destinations")[0]
                .brpc_server
                .as_ref()
                .expect("destination addr")
                .port,
            9030
        );
        assert_eq!(
            exec_params
                .runtime_filter_params
                .as_ref()
                .expect("runtime filter params")
                .id_to_prober_params
                .as_ref()
                .expect("probers")[&7][0]
                .fragment_instance_address
                .as_ref()
                .expect("prober addr")
                .port,
            9040
        );
        assert_eq!(
            fragment.plan.as_ref().expect("plan").nodes[0]
                .fetch_node
                .as_ref()
                .expect("fetch node")
                .nodes_info
                .as_ref()
                .expect("nodes_info")
                .nodes[0]
                .async_internal_port,
            9050
        );
        assert_eq!(
            fragment.plan.as_ref().expect("plan").nodes[1]
                .hash_join_node
                .as_ref()
                .expect("hash join")
                .build_runtime_filters
                .as_ref()
                .expect("runtime filters")[0]
                .runtime_filter_merge_nodes
                .as_ref()
                .expect("merge nodes")[0]
                .port,
            9060
        );
    }

    #[test]
    fn test_validate_internal_addresses_rejects_missing_or_invalid_fields() {
        let missing_dest = exec_params(None, Some(address("prober-host", 9040)));
        let err =
            validate_internal_addresses(&missing_dest, None).expect_err("missing destination");
        assert!(err.contains("missing destination address"));

        let empty_host = exec_params(Some(address("", 9030)), Some(address("prober-host", 9040)));
        let err = validate_internal_addresses(&empty_host, None).expect_err("empty host");
        assert!(err.contains("hostname is empty"));

        let bad_fetch_port = fragment(0, 9060);
        let err = validate_internal_addresses(
            &exec_params(
                Some(address("dest-host", 9030)),
                Some(address("prober-host", 9040)),
            ),
            Some(&bad_fetch_port),
        )
        .expect_err("invalid fetch port");
        assert!(err.contains("async_internal_port must be positive"));

        let bad_prober_port = exec_params(
            Some(address("dest-host", 9030)),
            Some(address("prober-host", 0)),
        );
        let err =
            validate_internal_addresses(&bad_prober_port, None).expect_err("invalid prober port");
        assert!(err.contains("port must be positive"));
    }
}
