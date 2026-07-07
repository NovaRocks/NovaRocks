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
use std::time::Duration;

use base64::Engine;
use thrift::OrderedFloat;

use crate::common::thrift::thrift_compact_serialize;
use crate::exec::expr::ExprArena;
use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan, push_down_local_runtime_filters};
use crate::exec::row_position::RowPositionDescriptor;
use crate::novarocks_connectors::ConnectorRegistry;

use crate::common::config::debug_exec_node_output;
use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::exec::operators::{
    DataStreamSinkFactory, IcebergChangeStreamRouterSinkFactory, IcebergTableSinkFactory,
    MultiCastDataStreamSinkFactory, NoopSinkFactory, OlapTableSinkFactory, ResultBufferSinkFactory,
    SplitDataStreamSinkFactory,
};
use crate::exec::pipeline::executor::{
    execute_plan_with_pipeline, execute_plan_with_pipeline_with_root_sink_dop,
};
use crate::lower::common::fragment_runtime::{
    RuntimeStateInputs, apply_query_option_overrides, build_runtime_state,
};
use crate::lower::compat::layout::{
    build_tuple_slot_order, infer_tuple_slot_order, reorder_tuple_slots,
};
use crate::lower::compat::node::{Lowered, PlanOrigin, lower_plan};
use crate::lower::compat::type_lowering::{
    native_primitive_type_from_desc, render_schema_from_type_desc,
};
use crate::runtime::fragment_output::FragmentOutput;
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::query_options::QueryOptions;
use crate::runtime::runtime_filter_params::RuntimeFilterParams;
use crate::service::result_batch_wire::{ResultProjection, ResultSinkConfig};
use crate::thrift::{data, data_sinks, descriptors, internal_service, planner, types};
use crate::types::PrimitiveType;

fn merge_row_pos_descs(
    target: &mut HashMap<i32, RowPositionDescriptor>,
    incoming: &HashMap<i32, RowPositionDescriptor>,
) -> Result<(), String> {
    for (tuple_id, desc) in incoming {
        match target.get(tuple_id) {
            None => {
                target.insert(*tuple_id, desc.clone());
            }
            Some(existing) => {
                if existing.row_position_type != desc.row_position_type
                    || existing.row_source_slot != desc.row_source_slot
                    || existing.fetch_ref_slots != desc.fetch_ref_slots
                    || existing.lookup_ref_slots != desc.lookup_ref_slots
                {
                    return Err(format!(
                        "conflicting row position descriptor for tuple_id={}",
                        tuple_id
                    ));
                }
            }
        }
    }
    Ok(())
}

fn collect_glm_metadata(
    node: &ExecNode,
    row_pos_descs: &mut HashMap<i32, RowPositionDescriptor>,
) -> Result<(), String> {
    match &node.kind {
        ExecNodeKind::LookUp(lookup) => {
            merge_row_pos_descs(row_pos_descs, &lookup.row_pos_descs)?;
        }
        ExecNodeKind::Fetch(fetch) => {
            merge_row_pos_descs(row_pos_descs, &fetch.row_pos_descs)?;
            collect_glm_metadata(&fetch.input, row_pos_descs)?;
        }
        ExecNodeKind::AssertNumRows(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Project(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Filter(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Repeat(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::ChangeEventExpand(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::UnionAll(node) => {
            for input in &node.inputs {
                collect_glm_metadata(input, row_pos_descs)?;
            }
        }
        ExecNodeKind::Limit(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::ExchangeSource(_) => {}
        ExecNodeKind::Scan(_) => {}
        ExecNodeKind::Aggregate(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Join(node) => {
            collect_glm_metadata(&node.left, row_pos_descs)?;
            collect_glm_metadata(&node.right, row_pos_descs)?;
        }
        ExecNodeKind::NestedLoopJoin(node) => {
            collect_glm_metadata(&node.left, row_pos_descs)?;
            collect_glm_metadata(&node.right, row_pos_descs)?;
        }
        ExecNodeKind::Sort(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::TableFunction(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Analytic(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::SetOp(node) => {
            for input in &node.inputs {
                collect_glm_metadata(input, row_pos_descs)?;
            }
        }
        ExecNodeKind::Values(_) => {}
        ExecNodeKind::IcebergDeltaScan(_) => {}
    }
    Ok(())
}

fn iceberg_sink_type_name(t: data_sinks::TDataSinkType) -> &'static str {
    match t {
        data_sinks::TDataSinkType::ICEBERG_DELETE_SINK => "ICEBERG_DELETE_SINK",
        data_sinks::TDataSinkType::ICEBERG_DV_SINK => "ICEBERG_DV_SINK",
        data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK => "ICEBERG_EQUALITY_DELETE_SINK",
        _ => "ICEBERG_TABLE_SINK",
    }
}

fn result_sink_config_from_thrift(
    result_sink: &data_sinks::TResultSink,
) -> Result<ResultSinkConfig, String> {
    let sink_type = result_sink
        .type_
        .unwrap_or(data_sinks::TResultSinkType::MYSQL_PROTOCAL);
    match sink_type {
        t if t == data_sinks::TResultSinkType::MYSQL_PROTOCAL => Ok(ResultSinkConfig::mysql()),
        t if t == data_sinks::TResultSinkType::HTTP_PROTOCAL => {
            let format = result_sink
                .format
                .unwrap_or(data_sinks::TResultSinkFormatType::JSON);
            if format != data_sinks::TResultSinkFormatType::JSON {
                return Err(format!(
                    "HTTP_PROTOCAL result sink only supports JSON format, got {:?}",
                    format
                ));
            }
            Ok(ResultSinkConfig::http_json())
        }
        t if t == data_sinks::TResultSinkType::STATISTIC => {
            Ok(ResultSinkConfig::statistic(thrift_statistic_row_encoder))
        }
        other => Err(format!("unsupported RESULT_SINK type {:?}", other)),
    }
}

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
    if raw.len().is_multiple_of(2) && raw.iter().all(|b| b.is_ascii_hexdigit()) {
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

fn thrift_statistic_row_encoder(
    version: i32,
    fields: &[Option<Vec<u8>>],
) -> Result<Vec<u8>, String> {
    let row_sd = rows_to_statistic_data(version, fields)?;
    thrift_compact_serialize(&row_sd)
}

fn result_projection_from_thrift_expr(
    expr: &crate::thrift::exprs::TExpr,
    idx: usize,
) -> Result<ResultProjection, String> {
    let root = expr
        .nodes
        .first()
        .ok_or_else(|| format!("RESULT_SINK output_exprs[{idx}] is empty"))?;
    if root.node_type != crate::thrift::exprs::TExprNodeType::SLOT_REF {
        return Err(format!(
            "RESULT_SINK output_exprs[{idx}] unsupported node_type {:?} (expected SLOT_REF)",
            root.node_type
        ));
    }
    let slot = root
        .slot_ref
        .as_ref()
        .ok_or_else(|| format!("RESULT_SINK output_exprs[{idx}] missing slot_ref payload"))?;
    Ok(ResultProjection {
        slot_id: SlotId::try_from(slot.slot_id)?,
        primitive: native_primitive_type_from_desc(&root.type_).unwrap_or(PrimitiveType::Invalid),
        field_schema: render_schema_from_type_desc(&root.type_)?,
    })
}

fn result_projections_from_thrift_exprs(
    output_exprs: Option<&Vec<crate::thrift::exprs::TExpr>>,
) -> Result<Option<Vec<ResultProjection>>, String> {
    let Some(output_exprs) = output_exprs.filter(|exprs| !exprs.is_empty()) else {
        return Ok(None);
    };
    output_exprs
        .iter()
        .enumerate()
        .map(|(idx, expr)| result_projection_from_thrift_expr(expr, idx))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

pub(crate) fn execute_fragment(
    fragment: &planner::TPlanFragment,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    query_opts: Option<&internal_service::TQueryOptions>,
    session_time_zone: Option<&str>,
    pipeline_dop: i32,
    _group_execution_scan_dop: Option<i32>,
    db_name: Option<&str>,
    profiler: Option<Profiler>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
    backend_num: Option<i32>,
    mem_tracker: Option<std::sync::Arc<crate::runtime::mem_tracker::MemTracker>>,
    typed_result_sink: bool,
    plan_origin: PlanOrigin,
) -> Result<FragmentOutput, String> {
    let runtime_query_opts = query_opts
        .map(|opts| QueryOptions::from_thrift(Some(opts)))
        .transpose()?;
    let runtime_query_opts = apply_query_option_overrides(runtime_query_opts);

    let profile_name = fragment
        .plan
        .as_ref()
        .and_then(|plan| plan.nodes.first().map(|n| n.node_id))
        .filter(|id| *id >= 0)
        .map(|id| format!("execute_fragment (plan_node_id={id})"));
    let profiler = if profiler.is_some() {
        profiler
    } else if runtime_query_opts
        .as_ref()
        .map(|opts| opts.enable_profile)
        .unwrap_or(false)
    {
        Some(Profiler::new(
            profile_name.as_deref().unwrap_or("execute_fragment"),
        ))
    } else {
        None
    };

    let query_id = exec_params.map(|params| QueryId {
        hi: params.query_id.hi,
        lo: params.query_id.lo,
    });
    let runtime_filter_params = exec_params
        .and_then(|params| params.runtime_filter_params.as_ref())
        .map(RuntimeFilterParams::from_thrift)
        .transpose()?;
    let fragment_instance_id = exec_params.map(|params| UniqueId {
        hi: params.fragment_instance_id.hi,
        lo: params.fragment_instance_id.lo,
    });
    let runtime_state = build_runtime_state(
        RuntimeStateInputs {
            query_options: runtime_query_opts.clone(),
            query_id,
            runtime_filter_params,
            fragment_instance_id,
            backend_num,
            mem_tracker,
        },
        profiler.as_ref(),
    )?;

    if let Some(plan) = fragment.plan.as_ref() {
        let mut tuple_slots = build_tuple_slot_order(desc_tbl);
        let inferred = infer_tuple_slot_order(fragment);
        if tuple_slots.is_empty() {
            tuple_slots = inferred.clone();
        } else {
            for (tuple_id, slots) in &inferred {
                if tuple_slots.contains_key(tuple_id) {
                    continue;
                }
                tuple_slots.insert(*tuple_id, slots.clone());
            }
        }
        reorder_tuple_slots(&mut tuple_slots, desc_tbl);
        let mut arena = ExprArena::default();
        let allow_throw_exception = runtime_query_opts
            .as_ref()
            .map(|opts| opts.allow_throw_exception)
            .unwrap_or(false);
        let allow_throw_exception = allow_throw_exception
            || query_opts.is_some_and(|opts| {
                matches!(
                    opts.overflow_mode,
                    Some(mode) if mode == internal_service::TOverflowMode::REPORT_ERROR
                )
            });
        arena.set_allow_throw_exception(allow_throw_exception);
        arena.set_session_time_zone(session_time_zone.map(|s| s.to_string()));
        // Layout hints are used by scan nodes to decide which columns to materialize.
        //
        // For exchange fragments, pruning only by "local usage" is not correct because downstream
        // fragments may require additional columns that do not appear in this fragment's exprs.
        // The descriptor table already encodes the materialized slots for each tuple, so we use it
        // as the source of truth to avoid producing mismatched layouts at runtime.
        let layout_hints = tuple_slots.clone();
        let connectors = ConnectorRegistry::default();
        let lowered: Lowered = {
            let _lower_timer = profiler.as_ref().map(|p| p.scoped_timer("LowerPlanTime"));
            lower_plan(
                plan,
                &mut arena,
                &tuple_slots,
                desc_tbl,
                fragment.query_global_dicts.as_deref(),
                fragment.query_global_dict_exprs.as_ref(),
                exec_params,
                query_opts,
                db_name,
                &connectors,
                &layout_hints,
                last_query_id,
                fe_addr,
                plan_origin,
            )?
        };

        // PlanFragment must have a sink
        let sink = fragment
            .output_sink
            .as_ref()
            .ok_or_else(|| "PlanFragment must have output_sink field".to_string())?;

        let mut exec_plan = ExecPlan {
            arena,
            root: lowered.node,
        };
        if let Some(query_id) = query_id {
            let mut row_pos_descs = HashMap::new();
            collect_glm_metadata(&exec_plan.root, &mut row_pos_descs)?;
            if !row_pos_descs.is_empty() {
                query_context_manager().register_row_pos_descs(query_id, row_pos_descs)?;
            }
        }
        push_down_local_runtime_filters(&mut exec_plan.root, &exec_plan.arena);
        let root_plan_node_id = plan.nodes.first().map(|n| n.node_id).unwrap_or(-1);

        match sink.type_ {
            data_sinks::TDataSinkType::DATA_STREAM_SINK => {
                let stream_sink = sink
                    .stream_sink
                    .as_ref()
                    .ok_or_else(|| "DATA_STREAM_SINK missing stream_sink payload".to_string())?;
                let exec_params = exec_params
                    .ok_or_else(|| "DATA_STREAM_SINK requires exec_params".to_string())?;

                let sink_factory = DataStreamSinkFactory::new(
                    stream_sink.clone(),
                    exec_params.clone(),
                    lowered.layout.clone(),
                    root_plan_node_id,
                    last_query_id.map(|id| id.to_string()),
                    fe_addr.cloned(),
                );
                let exchange_finst_id = Some((
                    exec_params.fragment_instance_id.hi,
                    exec_params.fragment_instance_id.lo,
                ));
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
                let multi_cast_stream_sink =
                    sink.multi_cast_stream_sink.as_ref().ok_or_else(|| {
                        "MULTI_CAST_DATA_STREAM_SINK missing multi_cast_stream_sink payload"
                            .to_string()
                    })?;
                let exec_params = exec_params.ok_or_else(|| {
                    "MULTI_CAST_DATA_STREAM_SINK requires exec_params".to_string()
                })?;

                let sink_factory = MultiCastDataStreamSinkFactory::new(
                    multi_cast_stream_sink.clone(),
                    exec_params.clone(),
                    lowered.layout.clone(),
                    root_plan_node_id,
                    last_query_id.map(|id| id.to_string()),
                    fe_addr.cloned(),
                );
                let exchange_finst_id = Some((
                    exec_params.fragment_instance_id.hi,
                    exec_params.fragment_instance_id.lo,
                ));
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK => {
                let split_stream_sink = sink.split_stream_sink.as_ref().ok_or_else(|| {
                    "SPLIT_DATA_STREAM_SINK missing split_stream_sink payload".to_string()
                })?;
                let exec_params = exec_params
                    .ok_or_else(|| "SPLIT_DATA_STREAM_SINK requires exec_params".to_string())?;
                let split_exprs = split_stream_sink.split_exprs.as_ref().ok_or_else(|| {
                    "SPLIT_DATA_STREAM_SINK missing split_exprs payload".to_string()
                })?;

                let mut split_expr_ids = Vec::with_capacity(split_exprs.len());
                for expr in split_exprs {
                    split_expr_ids.push(crate::lower::compat::expr::lower_t_expr(
                        expr,
                        &mut exec_plan.arena,
                        &lowered.layout,
                        last_query_id,
                        fe_addr,
                    )?);
                }

                let sink_factory = SplitDataStreamSinkFactory::new(
                    split_stream_sink.clone(),
                    exec_params.clone(),
                    lowered.layout.clone(),
                    root_plan_node_id,
                    last_query_id.map(|id| id.to_string()),
                    fe_addr.cloned(),
                    Arc::new(exec_plan.arena.clone()),
                    split_expr_ids,
                );
                let exchange_finst_id = Some((
                    exec_params.fragment_instance_id.hi,
                    exec_params.fragment_instance_id.lo,
                ));
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK => {
                let router = sink.iceberg_change_stream_router_sink.as_ref().ok_or_else(|| {
                    "ICEBERG_CHANGE_STREAM_ROUTER_SINK missing iceberg_change_stream_router_sink"
                        .to_string()
                })?;
                let exec_params = exec_params.ok_or_else(|| {
                    "ICEBERG_CHANGE_STREAM_ROUTER_SINK requires exec_params".to_string()
                })?;

                let sink_factory = IcebergChangeStreamRouterSinkFactory::try_new(
                    router.clone(),
                    exec_params.clone(),
                    lowered.layout.clone(),
                    root_plan_node_id,
                    last_query_id.map(|id| id.to_string()),
                    fe_addr.cloned(),
                )?;
                let exchange_finst_id = Some((
                    exec_params.fragment_instance_id.hi,
                    exec_params.fragment_instance_id.lo,
                ));
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            data_sinks::TDataSinkType::RESULT_SINK => {
                let result_sink = sink
                    .result_sink
                    .as_ref()
                    .ok_or_else(|| "RESULT_SINK missing result_sink payload".to_string())?;
                let result_sink_config = result_sink_config_from_thrift(result_sink)?;
                let output_projections =
                    result_projections_from_thrift_exprs(fragment.output_exprs.as_ref())?;
                let sink_factory = ResultBufferSinkFactory::new(
                    output_projections,
                    result_sink_config,
                    None,
                    typed_result_sink,
                );
                let exchange_finst_id = exec_params.map(|params| {
                    (
                        params.fragment_instance_id.hi,
                        params.fragment_instance_id.lo,
                    )
                });
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            data_sinks::TDataSinkType::NOOP_SINK => {
                let sink_factory = NoopSinkFactory::new();
                let exchange_finst_id = exec_params.map(|params| {
                    (
                        params.fragment_instance_id.hi,
                        params.fragment_instance_id.lo,
                    )
                });
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            data_sinks::TDataSinkType::SCHEMA_TABLE_SINK => {
                // SCHEMA_TABLE_SINK statements (for example information_schema config updates)
                // only need side effects in FE metadata path; compute fragment output is discarded.
                let sink_factory = NoopSinkFactory::new();
                let exchange_finst_id = exec_params.map(|params| {
                    (
                        params.fragment_instance_id.hi,
                        params.fragment_instance_id.lo,
                    )
                });
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    exchange_finst_id,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            data_sinks::TDataSinkType::ICEBERG_TABLE_SINK
            | data_sinks::TDataSinkType::ICEBERG_DELETE_SINK
            | data_sinks::TDataSinkType::ICEBERG_DV_SINK
            | data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK => {
                let sink_type_name = iceberg_sink_type_name(sink.type_);
                let iceberg_sink = sink.iceberg_table_sink.as_ref().ok_or_else(|| {
                    format!("{sink_type_name} missing iceberg_table_sink payload")
                })?;
                let output_exprs = fragment
                    .output_exprs
                    .as_ref()
                    .ok_or_else(|| format!("{sink_type_name} missing output_exprs"))?;
                let desc_tbl = desc_tbl
                    .ok_or_else(|| format!("{sink_type_name} requires descriptor table"))?;

                let sink_mode =
                    crate::lower::compat::sink::iceberg::iceberg_sink_mode_for_type(sink.type_);
                let sink_input =
                    crate::lower::compat::sink::iceberg::lower_iceberg_sink_factory_input(
                        iceberg_sink,
                        sink_mode,
                        output_exprs,
                        &lowered.layout,
                        desc_tbl,
                        last_query_id,
                        fe_addr,
                    )?;
                let sink_factory = IcebergTableSinkFactory::try_new(sink_input)?;
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                let root_sink_dop = (sink_mode
                    == crate::connector::iceberg::IcebergSinkMode::DeletionVectors)
                    .then_some(1);
                execute_plan_with_pipeline_with_root_sink_dop(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    None,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                    root_sink_dop,
                )?;
            }
            data_sinks::TDataSinkType::OLAP_TABLE_SINK => {
                let olap_sink = sink
                    .olap_table_sink
                    .as_ref()
                    .ok_or_else(|| "OLAP_TABLE_SINK missing olap_table_sink payload".to_string())?;
                let sink_input =
                    crate::lower::compat::sink::starrocks::lower_starrocks_sink_factory_input(
                        olap_sink,
                        fragment.output_exprs.as_deref(),
                        Some(&exec_plan),
                        Some(&lowered.layout),
                        last_query_id,
                        session_time_zone,
                        fe_addr,
                    )?;
                let sink_factory = OlapTableSinkFactory::try_new(sink_input)?;
                let _exec_timer = profiler
                    .as_ref()
                    .map(|p| p.scoped_timer("PipelineExecuteTime"));
                execute_plan_with_pipeline(
                    exec_plan,
                    debug_exec_node_output(),
                    Duration::from_millis(50),
                    Box::new(sink_factory),
                    None,
                    profiler.clone(),
                    pipeline_dop,
                    Arc::clone(&runtime_state),
                    query_id,
                    fe_addr.cloned(),
                    backend_num,
                )?;
            }
            other => {
                return Err(format!(
                    "unsupported sink type: {:?}. Only DATA_STREAM_SINK, MULTI_CAST_DATA_STREAM_SINK, SPLIT_DATA_STREAM_SINK, ICEBERG_CHANGE_STREAM_ROUTER_SINK, RESULT_SINK, NOOP_SINK, SCHEMA_TABLE_SINK, ICEBERG_TABLE_SINK, ICEBERG_DELETE_SINK, ICEBERG_DV_SINK, ICEBERG_EQUALITY_DELETE_SINK, and OLAP_TABLE_SINK are supported",
                    other
                ));
            }
        }
        return Ok(FragmentOutput { profile_json: None });
    }

    Err("unsupported fragment: missing plan".to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::lower::compat::type_lowering::scalar_type_desc;
    use crate::service::result_batch_wire::{ResultSinkFormat, ResultSinkType};
    use crate::thrift::{
        data_sinks, exprs, internal_service, partitions, plan_nodes, planner, types,
    };

    #[test]
    fn iceberg_dv_sink_lowers_to_deletion_vectors_mode() {
        let mode = crate::lower::compat::sink::iceberg::iceberg_sink_mode_for_type(
            data_sinks::TDataSinkType::ICEBERG_DV_SINK,
        );

        assert_eq!(
            mode,
            crate::connector::iceberg::IcebergSinkMode::DeletionVectors
        );
        assert_eq!(
            crate::lower::compat::sink::iceberg::iceberg_sink_mode_for_type(
                data_sinks::TDataSinkType::ICEBERG_DELETE_SINK
            ),
            crate::connector::iceberg::IcebergSinkMode::PositionDeletes
        );
    }

    #[test]
    fn iceberg_equality_delete_sink_lowers_to_equality_deletes_mode() {
        let sink_type = data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK;

        assert_eq!(i32::from(sink_type), 19);
        assert_eq!(data_sinks::TDataSinkType::from(19), sink_type);
        assert_eq!(
            crate::lower::compat::sink::iceberg::iceberg_sink_mode_for_type(sink_type),
            crate::connector::iceberg::IcebergSinkMode::EqualityDeletes
        );
    }

    fn empty_partition() -> partitions::TDataPartition {
        partitions::TDataPartition::new(
            partitions::TPartitionType::UNPARTITIONED,
            None::<Vec<exprs::TExpr>>,
            None::<Vec<partitions::TRangePartition>>,
            None::<Vec<partitions::TBucketProperty>>,
        )
    }

    fn empty_data_sink_for_test(type_: data_sinks::TDataSinkType) -> data_sinks::TDataSink {
        data_sinks::TDataSink::new(
            type_,
            None::<data_sinks::TDataStreamSink>,
            None::<data_sinks::TResultSink>,
            None::<data_sinks::TMysqlTableSink>,
            None::<data_sinks::TExportSink>,
            None::<data_sinks::TOlapTableSink>,
            None::<data_sinks::TMemoryScratchSink>,
            None::<data_sinks::TMultiCastDataStreamSink>,
            None::<data_sinks::TSchemaTableSink>,
            None::<data_sinks::TIcebergTableSink>,
            None::<data_sinks::THiveTableSink>,
            None::<data_sinks::TTableFunctionTableSink>,
            None::<data_sinks::TDictionaryCacheSink>,
            None::<Vec<Box<data_sinks::TDataSink>>>,
            None::<i64>,
            None::<data_sinks::TSplitDataStreamSink>,
            None::<data_sinks::TIcebergChangeStreamRouterSink>,
        )
    }

    fn stream_sink_for_test(dest_node_id: i32) -> data_sinks::TDataStreamSink {
        data_sinks::TDataStreamSink::new(
            dest_node_id,
            empty_partition(),
            None::<bool>,
            None::<bool>,
            None::<i32>,
            None::<Vec<i32>>,
            None::<i64>,
        )
    }

    fn router_branch_for_test(
        branch_id: i32,
        branch_kind: data_sinks::TIcebergChangeStreamRouterBranchKind,
    ) -> data_sinks::TIcebergChangeStreamRouterBranch {
        data_sinks::TIcebergChangeStreamRouterBranch::new(
            branch_id,
            branch_kind,
            stream_sink_for_test(100 + branch_id),
            Vec::new(),
        )
    }

    fn raw_values_plan_for_test() -> plan_nodes::TPlan {
        let mut node = crate::lower::compat::node::test_plan_node(
            1,
            plan_nodes::TPlanNodeType::RAW_VALUES_NODE,
            0,
        );
        node.raw_values_node = Some(plan_nodes::TRawValuesNode::new(
            0,
            scalar_type_desc(types::TPrimitiveType::INT),
            None::<Vec<i64>>,
            None::<Vec<String>>,
        ));
        plan_nodes::TPlan::new(vec![node])
    }

    fn fragment_with_sink_for_test(sink: data_sinks::TDataSink) -> planner::TPlanFragment {
        planner::TPlanFragment::new(
            Some(raw_values_plan_for_test()),
            None::<Vec<exprs::TExpr>>,
            Some(sink),
            empty_partition(),
            None::<i64>,
            None::<i64>,
            None::<Vec<crate::thrift::data::TGlobalDict>>,
            None::<Vec<crate::thrift::data::TGlobalDict>>,
            None::<planner::TCacheParam>,
            None::<BTreeMap<i32, exprs::TExpr>>,
            None::<planner::TGroupExecutionParam>,
        )
    }

    fn exec_params_for_test() -> internal_service::TPlanFragmentExecParams {
        internal_service::TPlanFragmentExecParams::new(
            types::TUniqueId::new(0, 1),
            types::TUniqueId::new(0, 2),
            BTreeMap::new(),
            BTreeMap::new(),
            None::<Vec<data_sinks::TPlanFragmentDestination>>,
            None::<i32>,
            None::<i32>,
            None::<bool>,
            None::<bool>,
            None::<crate::thrift::runtime_filter::TRuntimeFilterParams>,
            None::<i32>,
            None::<bool>,
            None::<
                BTreeMap<
                    types::TPlanNodeId,
                    BTreeMap<i32, Vec<internal_service::TScanRangeParams>>,
                >,
            >,
            None::<bool>,
            None::<i32>,
            None::<bool>,
            None::<Vec<internal_service::TExecDebugOption>>,
        )
    }

    #[test]
    fn result_sink_config_defaults_to_mysql_protocol() {
        let result_sink = data_sinks::TResultSink::default();

        let config = super::result_sink_config_from_thrift(&result_sink).expect("config");

        assert_eq!(config.sink_type, ResultSinkType::MySqlProtocol);
        assert_eq!(config.format, None);
        assert!(config.statistic_encoder.is_none());
    }

    #[test]
    fn result_sink_config_defaults_http_format_to_json() {
        let mut result_sink = data_sinks::TResultSink::default();
        result_sink.type_ = Some(data_sinks::TResultSinkType::HTTP_PROTOCAL);

        let config = super::result_sink_config_from_thrift(&result_sink).expect("config");

        assert_eq!(config.sink_type, ResultSinkType::HttpProtocol);
        assert_eq!(config.format, Some(ResultSinkFormat::Json));
        assert!(config.statistic_encoder.is_none());
    }

    #[test]
    fn result_sink_config_rejects_non_json_http_format() {
        let mut result_sink = data_sinks::TResultSink::default();
        result_sink.type_ = Some(data_sinks::TResultSinkType::HTTP_PROTOCAL);
        result_sink.format = Some(data_sinks::TResultSinkFormatType::OTHERS);

        let err = super::result_sink_config_from_thrift(&result_sink)
            .expect_err("non-json HTTP format must fail");

        assert!(
            err.contains("HTTP_PROTOCAL result sink only supports JSON format"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn result_sink_config_binds_statistic_encoder() {
        let mut result_sink = data_sinks::TResultSink::default();
        result_sink.type_ = Some(data_sinks::TResultSinkType::STATISTIC);

        let config = super::result_sink_config_from_thrift(&result_sink).expect("config");

        assert_eq!(config.sink_type, ResultSinkType::Statistic);
        assert!(config.statistic_encoder.is_some());
    }

    #[test]
    fn lower_router_sink_requires_router_payload() {
        let sink =
            empty_data_sink_for_test(data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK);
        let fragment = fragment_with_sink_for_test(sink);

        let err = super::execute_fragment(
            &fragment,
            None,
            Some(&exec_params_for_test()),
            None,
            None,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            crate::lower::compat::node::PlanOrigin::StarRocksFeCompatible,
        )
        .expect_err("missing router payload must fail");

        assert_eq!(
            err,
            "ICEBERG_CHANGE_STREAM_ROUTER_SINK missing iceberg_change_stream_router_sink"
        );
    }

    #[test]
    fn lower_router_sink_rejects_duplicate_branch_kind() {
        let mut sink =
            empty_data_sink_for_test(data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK);
        sink.iceberg_change_stream_router_sink =
            Some(data_sinks::TIcebergChangeStreamRouterSink::new(
                7,
                None::<i32>,
                vec![
                    router_branch_for_test(
                        0,
                        data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV,
                    ),
                    router_branch_for_test(
                        1,
                        data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV,
                    ),
                ],
            ));
        let fragment = fragment_with_sink_for_test(sink);

        let err = super::execute_fragment(
            &fragment,
            None,
            Some(&exec_params_for_test()),
            None,
            None,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            crate::lower::compat::node::PlanOrigin::StarRocksFeCompatible,
        )
        .expect_err("duplicate branch kind must fail");

        assert!(
            err.contains("duplicate change-stream branch kind"),
            "unexpected error: {err}"
        );
    }
}
