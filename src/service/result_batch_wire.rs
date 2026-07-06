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

use arrow::array::ArrayRef;
use base64::Engine;
use thrift::OrderedFloat;

use crate::common::ids::SlotId;
use crate::common::thrift::thrift_compact_serialize;
use crate::common::util::{
    FieldRenderSchema, http_json_row_from_arrays_with_primitives,
    mysql_text_row_from_arrays_with_primitives,
};
use crate::exec::chunk::Chunk;
use crate::thrift::{data, data_sinks, exprs, types};
use crate::types::arrow_thrift::{arrow_field_to_primitive, thrift_desc_to_primitive};

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

fn columns_for_output_exprs(
    chunk: &Chunk,
    output_exprs: &[exprs::TExpr],
) -> Result<Vec<ArrayRef>, String> {
    let mut out = Vec::with_capacity(output_exprs.len());
    for (col_idx, e) in output_exprs.iter().enumerate() {
        let root = e
            .nodes
            .first()
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
            .first()
            .ok_or_else(|| format!("output_exprs[{}] is empty", col_idx))?;
        let primitive =
            thrift_desc_to_primitive(&root.type_).unwrap_or(types::TPrimitiveType::INVALID_TYPE);
        out.push(primitive);
    }
    Ok(out)
}

fn primitives_for_chunk_fields(chunk: &Chunk) -> Vec<types::TPrimitiveType> {
    chunk
        .chunk_schema()
        .slots()
        .iter()
        .map(|slot| {
            arrow_field_to_primitive(slot.field()).unwrap_or(types::TPrimitiveType::INVALID_TYPE)
        })
        .collect()
}

fn field_schemas_for_output_exprs(
    output_exprs: &[exprs::TExpr],
) -> Result<Vec<FieldRenderSchema>, String> {
    let mut out = Vec::with_capacity(output_exprs.len());
    for (col_idx, e) in output_exprs.iter().enumerate() {
        let root = e
            .nodes
            .first()
            .ok_or_else(|| format!("output_exprs[{}] is empty", col_idx))?;
        out.push(FieldRenderSchema::try_from_type_desc(&root.type_)?);
    }
    Ok(out)
}

fn field_schemas_for_chunk_fields(chunk: &Chunk) -> Vec<FieldRenderSchema> {
    chunk
        .chunk_schema()
        .slots()
        .iter()
        .map(|slot| FieldRenderSchema::from_field(slot.field()))
        .collect()
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

pub(crate) fn build_empty_fetch_result_batch_template(
    result_sink_type: Option<data_sinks::TResultSinkType>,
    result_sink_format: Option<data_sinks::TResultSinkFormatType>,
) -> Result<data::TResultBatch, String> {
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
    }

    let mut batch = data::TResultBatch::new(vec![], false, 0, None);
    if matches!(
        result_sink_type,
        Some(t) if t == data_sinks::TResultSinkType::STATISTIC
    ) {
        batch.statistic_version = Some(STATISTIC_DATA_VERSION_V1);
    }
    Ok(batch)
}

pub(crate) fn build_fetch_result_batch_for_chunk(
    chunk: &Chunk,
    output_exprs: Option<&[exprs::TExpr]>,
    result_sink_type: Option<data_sinks::TResultSinkType>,
    result_sink_format: Option<data_sinks::TResultSinkFormatType>,
) -> Result<data::TResultBatch, String> {
    let is_statistic_sink = matches!(
        result_sink_type,
        Some(t) if t == data_sinks::TResultSinkType::STATISTIC
    );
    if is_statistic_sink {
        let exprs = output_exprs
            .filter(|v| !v.is_empty())
            .ok_or_else(|| "STATISTIC result sink requires non-empty output_exprs".to_string())?;
        let mut batch = data::TResultBatch::new(vec![], false, 0, None);
        let columns = columns_for_output_exprs(chunk, exprs)?;
        let primitives = primitives_for_output_exprs(exprs)?;
        let field_schemas = field_schemas_for_output_exprs(exprs)?;
        for row in 0..chunk.len() {
            let mysql_row = mysql_text_row_from_arrays_with_primitives(
                &columns,
                row,
                Some(&primitives),
                Some(&field_schemas),
            )?;
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
        if batch.statistic_version.is_none() {
            batch.statistic_version = Some(STATISTIC_DATA_VERSION_V1);
        }
        return Ok(batch);
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

        let mut batch = data::TResultBatch::new(vec![], false, 0, None);
        if let Some(output_exprs) = output_exprs.filter(|v| !v.is_empty()) {
            let columns = columns_for_output_exprs(chunk, output_exprs)?;
            let primitives = primitives_for_output_exprs(output_exprs)?;
            let field_schemas = field_schemas_for_output_exprs(output_exprs)?;
            for row in 0..chunk.len() {
                batch.rows.push(http_json_row_from_arrays_with_primitives(
                    &columns,
                    row,
                    Some(&primitives),
                    Some(&field_schemas),
                )?);
            }
        } else {
            let columns = chunk.columns();
            let primitives = primitives_for_chunk_fields(chunk);
            let field_schemas = field_schemas_for_chunk_fields(chunk);
            for row in 0..chunk.len() {
                batch.rows.push(http_json_row_from_arrays_with_primitives(
                    columns,
                    row,
                    Some(&primitives),
                    Some(&field_schemas),
                )?);
            }
        }
        return Ok(batch);
    }

    let mut batch = data::TResultBatch::new(vec![], false, 0, None);
    if let Some(output_exprs) = output_exprs.filter(|v| !v.is_empty()) {
        let columns = columns_for_output_exprs(chunk, output_exprs)?;
        let primitives = primitives_for_output_exprs(output_exprs)?;
        let field_schemas = field_schemas_for_output_exprs(output_exprs)?;
        for row in 0..chunk.len() {
            let bytes = mysql_text_row_from_arrays_with_primitives(
                &columns,
                row,
                Some(&primitives),
                Some(&field_schemas),
            )?;
            batch.rows.push(bytes);
        }
    } else {
        let columns = chunk.columns();
        let primitives = primitives_for_chunk_fields(chunk);
        let field_schemas = field_schemas_for_chunk_fields(chunk);
        for row in 0..chunk.len() {
            let bytes = mysql_text_row_from_arrays_with_primitives(
                columns,
                row,
                Some(&primitives),
                Some(&field_schemas),
            )?;
            batch.rows.push(bytes);
        }
    }
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, BinaryArray, StringArray};
    use arrow::datatypes::{DataType, Field};

    use super::build_fetch_result_batch_for_chunk;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkFieldSchema, ChunkSchema, ChunkSlotSchema};
    use crate::thrift::data_sinks;
    use crate::types::logical::{LogicalType, field_with_logical_type};

    fn chunk_with_stale_field_schema(field: Field, column: ArrayRef) -> Result<Chunk, String> {
        let chunk_schema = Arc::new(ChunkSchema::try_new(vec![
            ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                field,
                Some(ChunkFieldSchema::empty()),
                None,
            ),
        ])?);
        Chunk::try_new_with_columns(chunk_schema, vec![column])
    }

    #[test]
    fn fetch_fallback_http_json_uses_arrow_field_metadata_for_json() {
        let field = field_with_logical_type(
            Field::new("payload", DataType::Utf8, true),
            LogicalType::Json,
        );
        let chunk = chunk_with_stale_field_schema(
            field,
            Arc::new(StringArray::from(vec![r#"{"a":1}"#])) as ArrayRef,
        )
        .expect("chunk");

        let batch = build_fetch_result_batch_for_chunk(
            &chunk,
            None,
            Some(data_sinks::TResultSinkType::HTTP_PROTOCAL),
            Some(data_sinks::TResultSinkFormatType::JSON),
        )
        .expect("fetch batch");

        assert_eq!(batch.rows, vec![b"{\"data\":[{\"a\":1}]}\n".to_vec()]);
    }

    #[test]
    fn fetch_fallback_mysql_uses_arrow_field_metadata_for_opaque_binary() {
        let field =
            field_with_logical_type(Field::new("hll", DataType::Binary, true), LogicalType::Hll);
        let chunk = chunk_with_stale_field_schema(
            field,
            Arc::new(BinaryArray::from(vec![Some(b"opaque".as_slice())])) as ArrayRef,
        )
        .expect("chunk");

        let batch =
            build_fetch_result_batch_for_chunk(&chunk, None, None, None).expect("fetch batch");

        assert_eq!(batch.rows, vec![vec![0xFB]]);
    }
}
