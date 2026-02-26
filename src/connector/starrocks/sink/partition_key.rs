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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{Datelike, NaiveDate, NaiveDateTime};

use crate::common::ids::SlotId;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::lower::expr::lower_t_expr;
use crate::lower::layout::Layout;
use crate::lower::type_lowering::{arrow_type_from_desc, primitive_type_from_node};
use crate::{data_sinks, descriptors, exprs, types};

#[derive(Clone)]
pub(crate) enum PartitionKeySource {
    None,
    SlotRefs(Vec<PartitionSlotRef>),
    Expr(Arc<PartitionExprPlan>),
}

#[derive(Clone)]
pub(crate) struct PartitionSlotRef {
    pub(crate) slot_id: SlotId,
    pub(crate) column_name: String,
}

#[derive(Clone)]
pub(crate) struct PartitionExprPlan {
    pub(crate) arena: ExprArena,
    pub(crate) expr_ids: Vec<ExprId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PartitionMode {
    Unpartitioned,
    Range,
    List,
}

#[derive(Clone, Debug)]
pub(crate) struct PartitionRoutingEntry {
    pub(crate) partition_id: i64,
    pub(crate) tablet_ids: Vec<i64>,
    pub(crate) start_key: Option<Vec<PartitionKeyValue>>,
    pub(crate) end_key: Option<Vec<PartitionKeyValue>>,
    pub(crate) in_keys: Vec<Vec<PartitionKeyValue>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PartitionKeyValue {
    Null,
    Bool(bool),
    Int(i128),
    Date32(i32),
    TimestampMicros(i64),
    Decimal { value: i128, scale: i8 },
    Utf8(String),
    Binary(Vec<u8>),
}

const UNIX_EPOCH_DAY_OFFSET: i32 = 719_163;

pub(crate) fn partition_key_source_len(source: &PartitionKeySource) -> usize {
    match source {
        PartitionKeySource::None => 0,
        PartitionKeySource::SlotRefs(slot_refs) => slot_refs.len(),
        PartitionKeySource::Expr(expr_plan) => expr_plan.expr_ids.len(),
    }
}

pub(crate) fn build_slot_name_map(
    slot_descs: &[descriptors::TSlotDescriptor],
) -> Result<HashMap<String, SlotId>, String> {
    let mut slot_by_name = HashMap::new();
    for (idx, slot) in slot_descs.iter().enumerate() {
        let Some(id) = slot.id else {
            return Err(format!(
                "OLAP_TABLE_SINK schema.slot_descs[{}] missing id while resolving slot names",
                idx
            ));
        };
        let slot_id = SlotId::try_from(id)?;
        if let Some(col_name) = slot
            .col_name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            slot_by_name.insert(col_name.to_ascii_lowercase(), slot_id);
        }
        if let Some(physical_name) = slot
            .col_physical_name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            slot_by_name.insert(physical_name.to_ascii_lowercase(), slot_id);
        }
    }
    Ok(slot_by_name)
}

pub(crate) fn resolve_slot_ids_by_names(
    slot_descs: &[descriptors::TSlotDescriptor],
    names: &[String],
    label: &str,
    slot_name_overrides: Option<&HashMap<String, SlotId>>,
) -> Result<Vec<SlotId>, String> {
    let slot_by_name = build_slot_name_map(slot_descs)?;
    let mut out = Vec::with_capacity(names.len());
    for col in names {
        let slot_id = slot_name_overrides
            .and_then(|map| map.get(col).copied())
            .or_else(|| slot_by_name.get(col).copied())
            .ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK cannot find {} '{}' in schema.slot_descs",
                    label, col
                )
            })?;
        out.push(slot_id);
    }
    Ok(out)
}

pub(crate) fn build_partition_key_source(
    sink: &data_sinks::TOlapTableSink,
    slot_name_overrides: Option<&HashMap<String, SlotId>>,
    slot_id_overrides: Option<&HashMap<SlotId, SlotId>>,
) -> Result<PartitionKeySource, String> {
    if let Some(exprs) = sink.partition.partition_exprs.as_ref()
        && !exprs.is_empty()
    {
        let plan = lower_partition_expr_plan(exprs, slot_id_overrides)?;
        return Ok(PartitionKeySource::Expr(Arc::new(plan)));
    }

    let partition_columns = sink
        .partition
        .partition_columns
        .as_ref()
        .map(|v| {
            v.iter()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if partition_columns.is_empty() {
        return Ok(PartitionKeySource::None);
    }

    let slot_ids = resolve_slot_ids_by_names(
        &sink.schema.slot_descs,
        &partition_columns,
        "partition column",
        slot_name_overrides,
    )?;
    let slot_refs = partition_columns
        .iter()
        .zip(slot_ids.iter())
        .map(|(name, slot_id)| PartitionSlotRef {
            slot_id: *slot_id,
            column_name: name.clone(),
        })
        .collect::<Vec<_>>();
    Ok(PartitionKeySource::SlotRefs(slot_refs))
}

fn lower_partition_expr_plan(
    exprs: &[exprs::TExpr],
    slot_id_overrides: Option<&HashMap<SlotId, SlotId>>,
) -> Result<PartitionExprPlan, String> {
    let mut arena = ExprArena::default();
    let mut expr_ids = Vec::with_capacity(exprs.len());
    let empty_layout = Layout {
        order: Vec::new(),
        index: HashMap::new(),
    };
    for expr in exprs {
        let mut rewritten_expr = expr.clone();
        if let Some(overrides) = slot_id_overrides {
            remap_partition_expr_slot_ids(&mut rewritten_expr, overrides)?;
        }
        let expr_id = lower_t_expr(&rewritten_expr, &mut arena, &empty_layout, None, None)?;
        expr_ids.push(expr_id);
    }
    Ok(PartitionExprPlan { arena, expr_ids })
}

fn remap_partition_expr_slot_ids(
    expr: &mut exprs::TExpr,
    slot_id_overrides: &HashMap<SlotId, SlotId>,
) -> Result<(), String> {
    if slot_id_overrides.is_empty() {
        return Ok(());
    }
    for (idx, node) in expr.nodes.iter_mut().enumerate() {
        if node.node_type != exprs::TExprNodeType::SLOT_REF {
            continue;
        }
        let Some(slot_ref) = node.slot_ref.as_mut() else {
            continue;
        };
        let source_slot_id = SlotId::try_from(slot_ref.slot_id).map_err(|e| {
            format!(
                "invalid partition expr slot id {} at node {}: {}",
                slot_ref.slot_id, idx, e
            )
        })?;
        let Some(target_slot_id) = slot_id_overrides.get(&source_slot_id).copied() else {
            continue;
        };
        let target_slot_i32 = i32::try_from(target_slot_id.as_u32()).map_err(|_| {
            format!(
                "partition expr remapped slot id {} exceeds i32 range",
                target_slot_id
            )
        })?;
        slot_ref.slot_id = target_slot_i32;
    }
    Ok(())
}

pub(crate) fn parse_partition_boundary_key(
    key_nodes: Option<&[exprs::TExprNode]>,
    legacy_node: Option<&exprs::TExprNode>,
) -> Result<Option<Vec<PartitionKeyValue>>, String> {
    if let Some(nodes) = key_nodes {
        if nodes.is_empty() {
            return Ok(None);
        }
        return parse_partition_key_nodes(nodes).map(Some);
    }
    if let Some(node) = legacy_node {
        return parse_partition_key_nodes(std::slice::from_ref(node)).map(Some);
    }
    Ok(None)
}

pub(crate) fn parse_partition_in_keys(
    in_keys: Option<&[Vec<exprs::TExprNode>]>,
) -> Result<Vec<Vec<PartitionKeyValue>>, String> {
    let Some(in_keys) = in_keys else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(in_keys.len());
    for key in in_keys {
        out.push(parse_partition_key_nodes(key)?);
    }
    Ok(out)
}

pub(crate) fn validate_partition_key_length(
    partition_id: i64,
    expected_len: usize,
    start_key: Option<&[PartitionKeyValue]>,
    end_key: Option<&[PartitionKeyValue]>,
    in_keys: &[Vec<PartitionKeyValue>],
) -> Result<(), String> {
    if expected_len == 0 {
        if start_key.is_some() || end_key.is_some() || !in_keys.is_empty() {
            return Err(format!(
                "OLAP_TABLE_SINK partition {} has partition key metadata but key source is empty",
                partition_id
            ));
        }
        return Ok(());
    }

    if let Some(start_key) = start_key
        && start_key.len() != expected_len
    {
        return Err(format!(
            "OLAP_TABLE_SINK partition {} start key length mismatch: expected={} actual={}",
            partition_id,
            expected_len,
            start_key.len()
        ));
    }
    if let Some(end_key) = end_key
        && end_key.len() != expected_len
    {
        return Err(format!(
            "OLAP_TABLE_SINK partition {} end key length mismatch: expected={} actual={}",
            partition_id,
            expected_len,
            end_key.len()
        ));
    }
    for (idx, key) in in_keys.iter().enumerate() {
        if key.len() != expected_len {
            return Err(format!(
                "OLAP_TABLE_SINK partition {} in_keys[{}] length mismatch: expected={} actual={}",
                partition_id,
                idx,
                expected_len,
                key.len()
            ));
        }
    }
    Ok(())
}

fn parse_partition_key_nodes(nodes: &[exprs::TExprNode]) -> Result<Vec<PartitionKeyValue>, String> {
    let mut out = Vec::with_capacity(nodes.len());
    for node in nodes {
        out.push(parse_partition_key_node(node)?);
    }
    Ok(out)
}

fn parse_partition_key_node(node: &exprs::TExprNode) -> Result<PartitionKeyValue, String> {
    match node.node_type {
        t if t == exprs::TExprNodeType::NULL_LITERAL => Ok(PartitionKeyValue::Null),
        t if t == exprs::TExprNodeType::BOOL_LITERAL => {
            let value = node
                .bool_literal
                .as_ref()
                .ok_or_else(|| "BOOL_LITERAL missing bool_literal payload".to_string())?
                .value;
            Ok(PartitionKeyValue::Bool(value))
        }
        t if t == exprs::TExprNodeType::INT_LITERAL => {
            let value = node
                .int_literal
                .as_ref()
                .ok_or_else(|| "INT_LITERAL missing int_literal payload".to_string())?
                .value as i128;
            Ok(PartitionKeyValue::Int(value))
        }
        t if t == exprs::TExprNodeType::LARGE_INT_LITERAL => {
            let value = node
                .large_int_literal
                .as_ref()
                .ok_or_else(|| "LARGE_INT_LITERAL missing payload".to_string())?
                .value
                .trim()
                .parse::<i128>()
                .map_err(|_| "LARGE_INT_LITERAL parse failed".to_string())?;
            Ok(PartitionKeyValue::Int(value))
        }
        t if t == exprs::TExprNodeType::DECIMAL_LITERAL => {
            let text = node
                .decimal_literal
                .as_ref()
                .ok_or_else(|| "DECIMAL_LITERAL missing decimal_literal payload".to_string())?
                .value
                .clone();
            let DataType::Decimal128(precision, scale) = arrow_type_from_desc(&node.type_)
                .ok_or_else(|| {
                    "DECIMAL_LITERAL missing or unsupported type descriptor".to_string()
                })?
            else {
                return Err("DECIMAL_LITERAL type descriptor is not decimal".to_string());
            };
            let value = parse_decimal_literal_value(&text, precision, scale)?;
            Ok(PartitionKeyValue::Decimal { value, scale })
        }
        t if t == exprs::TExprNodeType::STRING_LITERAL
            || t == exprs::TExprNodeType::DATE_LITERAL =>
        {
            let value = if t == exprs::TExprNodeType::STRING_LITERAL {
                node.string_literal
                    .as_ref()
                    .ok_or_else(|| "STRING_LITERAL missing string_literal payload".to_string())?
                    .value
                    .clone()
            } else {
                node.date_literal
                    .as_ref()
                    .ok_or_else(|| "DATE_LITERAL missing date_literal payload".to_string())?
                    .value
                    .clone()
            };
            match primitive_type_from_node(node) {
                Some(p) if p == types::TPrimitiveType::DATE => {
                    Ok(PartitionKeyValue::Date32(parse_date_literal_days(&value)?))
                }
                Some(p)
                    if p == types::TPrimitiveType::DATETIME || p == types::TPrimitiveType::TIME =>
                {
                    Ok(PartitionKeyValue::TimestampMicros(
                        parse_datetime_literal_micros(&value)?,
                    ))
                }
                Some(p)
                    if p == types::TPrimitiveType::BINARY
                        || p == types::TPrimitiveType::VARBINARY =>
                {
                    Ok(PartitionKeyValue::Binary(value.into_bytes()))
                }
                _ => Ok(PartitionKeyValue::Utf8(value)),
            }
        }
        t if t == exprs::TExprNodeType::BINARY_LITERAL => {
            let value = node
                .binary_literal
                .as_ref()
                .ok_or_else(|| "BINARY_LITERAL missing payload".to_string())?
                .value
                .clone();
            Ok(PartitionKeyValue::Binary(value))
        }
        t if t == exprs::TExprNodeType::FLOAT_LITERAL => {
            let _ = node
                .float_literal
                .as_ref()
                .ok_or_else(|| "FLOAT_LITERAL missing float_literal payload".to_string())?;
            Err("unsupported partition key literal node type: FLOAT_LITERAL".to_string())
        }
        other => Err(format!(
            "unsupported partition key literal node type: {:?}",
            other
        )),
    }
}

fn parse_date_literal_days(value: &str) -> Result<i32, String> {
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok(date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.date().num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    Err(format!("invalid DATE literal '{value}'"))
}

fn parse_datetime_literal_micros(value: &str) -> Result<i64, String> {
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| format!("invalid DATETIME literal '{value}'"))?;
        return Ok(dt.and_utc().timestamp_micros());
    }
    Err(format!("invalid DATETIME literal '{value}'"))
}

fn parse_decimal_literal_value(value: &str, precision: u8, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err(format!("invalid decimal scale: {scale}"));
    }
    let mut s = value.trim();
    if s.is_empty() {
        return Err("empty DECIMAL literal".to_string());
    }

    let mut sign: i128 = 1;
    if let Some(rest) = s.strip_prefix('-') {
        sign = -1;
        s = rest;
    } else if let Some(rest) = s.strip_prefix('+') {
        s = rest;
    }
    if s.is_empty() {
        return Err("empty DECIMAL literal".to_string());
    }

    let mut iter = s.split('.');
    let int_part_raw = iter.next().unwrap_or("");
    let frac_part = iter.next().unwrap_or("");
    if iter.next().is_some() {
        return Err(format!("invalid DECIMAL literal '{value}'"));
    }
    if int_part_raw.is_empty() && frac_part.is_empty() {
        return Err(format!("invalid DECIMAL literal '{value}'"));
    }

    let int_part = if int_part_raw.is_empty() {
        "0"
    } else {
        int_part_raw
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("invalid DECIMAL literal '{value}'"));
    }

    let scale_usize = scale as usize;
    if frac_part.len() > scale_usize {
        return Err(format!(
            "DECIMAL literal '{}' exceeds scale {}",
            value, scale_usize
        ));
    }

    let mut digits = String::with_capacity(int_part.len() + scale_usize);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in 0..(scale_usize - frac_part.len()) {
        digits.push('0');
    }

    let digits_trim = digits.trim_start_matches('0');
    let digits_final = if digits_trim.is_empty() {
        "0"
    } else {
        digits_trim
    };
    if digits_final.len() > precision as usize {
        return Err(format!(
            "DECIMAL literal '{}' exceeds precision {}",
            value, precision
        ));
    }

    let unsigned = digits_final
        .parse::<i128>()
        .map_err(|_| format!("failed to parse DECIMAL literal '{value}'"))?;
    Ok(unsigned.saturating_mul(sign))
}

pub(crate) fn build_partition_key_arrays(
    partition_key_source: &PartitionKeySource,
    chunk: &Chunk,
) -> Result<Vec<ArrayRef>, String> {
    match partition_key_source {
        PartitionKeySource::None => Ok(Vec::new()),
        PartitionKeySource::SlotRefs(slot_refs) => {
            let mut arrays = Vec::with_capacity(slot_refs.len());
            for slot_ref in slot_refs {
                let arr = match chunk.column_by_slot_id(slot_ref.slot_id) {
                    Ok(arr) => arr,
                    Err(slot_err) => {
                        find_chunk_column_by_name(chunk, &slot_ref.column_name).ok_or_else(|| {
                            format!(
                                "OLAP_TABLE_SINK partition slot {} ('{}') is not available in chunk: {}",
                                slot_ref.slot_id,
                                slot_ref.column_name,
                                slot_err
                            )
                        })?
                    }
                };
                arrays.push(arr);
            }
            Ok(arrays)
        }
        PartitionKeySource::Expr(plan) => {
            let mut arrays = Vec::with_capacity(plan.expr_ids.len());
            for expr_id in &plan.expr_ids {
                arrays.push(plan.arena.eval(*expr_id, chunk)?);
            }
            Ok(arrays)
        }
    }
}

fn find_chunk_column_by_name(chunk: &Chunk, column_name: &str) -> Option<ArrayRef> {
    let target = column_name.trim();
    if target.is_empty() {
        return None;
    }
    let idx = chunk
        .batch
        .schema()
        .fields()
        .iter()
        .position(|field| field.name().eq_ignore_ascii_case(target))?;
    chunk.batch.columns().get(idx).cloned()
}

pub(crate) fn build_row_partition_key(
    partition_key_arrays: &[ArrayRef],
    row: usize,
) -> Result<Vec<PartitionKeyValue>, String> {
    let mut out = Vec::with_capacity(partition_key_arrays.len());
    for array in partition_key_arrays {
        out.push(read_partition_key_value(array.as_ref(), row)?);
    }
    Ok(out)
}

fn read_partition_key_value(array: &dyn Array, row: usize) -> Result<PartitionKeyValue, String> {
    if array.is_null(row) {
        return Ok(PartitionKeyValue::Null);
    }
    match array.data_type() {
        DataType::Boolean => {
            let typed = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "downcast BooleanArray failed".to_string())?;
            Ok(PartitionKeyValue::Bool(typed.value(row)))
        }
        DataType::Int8 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "downcast Int8Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::Int16 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "downcast Int16Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::Int32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "downcast Int32Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::Int64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "downcast Int64Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::UInt8 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| "downcast UInt8Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::UInt16 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| "downcast UInt16Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::UInt32 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "downcast UInt32Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::UInt64 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "downcast UInt64Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::Date32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "downcast Date32Array failed".to_string())?;
            Ok(PartitionKeyValue::Date32(typed.value(row)))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| "downcast TimestampSecondArray failed".to_string())?;
            Ok(PartitionKeyValue::TimestampMicros(
                typed.value(row).saturating_mul(1_000_000),
            ))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| "downcast TimestampMillisecondArray failed".to_string())?;
            Ok(PartitionKeyValue::TimestampMicros(
                typed.value(row).saturating_mul(1_000),
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "downcast TimestampMicrosecondArray failed".to_string())?;
            Ok(PartitionKeyValue::TimestampMicros(typed.value(row)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| "downcast TimestampNanosecondArray failed".to_string())?;
            Ok(PartitionKeyValue::TimestampMicros(typed.value(row) / 1_000))
        }
        DataType::Decimal128(_, scale) => {
            let typed = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "downcast Decimal128Array failed".to_string())?;
            Ok(PartitionKeyValue::Decimal {
                value: typed.value(row),
                scale: *scale,
            })
        }
        DataType::Utf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "downcast StringArray failed".to_string())?;
            Ok(PartitionKeyValue::Utf8(typed.value(row).to_string()))
        }
        DataType::LargeUtf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "downcast LargeStringArray failed".to_string())?;
            Ok(PartitionKeyValue::Utf8(typed.value(row).to_string()))
        }
        DataType::Binary => {
            let typed = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "downcast BinaryArray failed".to_string())?;
            Ok(PartitionKeyValue::Binary(typed.value(row).to_vec()))
        }
        DataType::LargeBinary => {
            let typed = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "downcast LargeBinaryArray failed".to_string())?;
            Ok(PartitionKeyValue::Binary(typed.value(row).to_vec()))
        }
        other => Err(format!(
            "unsupported partition key data type in routed chunk: {:?}",
            other
        )),
    }
}

pub(crate) fn compare_partition_key_vectors(
    left: &[PartitionKeyValue],
    right: &[PartitionKeyValue],
) -> Result<Ordering, String> {
    if left.len() != right.len() {
        return Err(format!(
            "partition key length mismatch in comparison: left={} right={}",
            left.len(),
            right.len()
        ));
    }
    for (idx, (lhs, rhs)) in left.iter().zip(right.iter()).enumerate() {
        let ord = compare_partition_key_value(lhs, rhs)
            .map_err(|e| format!("partition key compare failed at column {}: {}", idx, e))?;
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(Ordering::Equal)
}

fn compare_partition_key_value(
    left: &PartitionKeyValue,
    right: &PartitionKeyValue,
) -> Result<Ordering, String> {
    match (left, right) {
        (PartitionKeyValue::Null, PartitionKeyValue::Null) => Ok(Ordering::Equal),
        (PartitionKeyValue::Null, _) => Ok(Ordering::Less),
        (_, PartitionKeyValue::Null) => Ok(Ordering::Greater),
        (PartitionKeyValue::Bool(lhs), PartitionKeyValue::Bool(rhs)) => Ok(lhs.cmp(rhs)),
        (PartitionKeyValue::Int(lhs), PartitionKeyValue::Int(rhs)) => Ok(lhs.cmp(rhs)),
        (PartitionKeyValue::Date32(lhs), PartitionKeyValue::Date32(rhs)) => Ok(lhs.cmp(rhs)),
        (PartitionKeyValue::TimestampMicros(lhs), PartitionKeyValue::TimestampMicros(rhs)) => {
            Ok(lhs.cmp(rhs))
        }
        (
            PartitionKeyValue::Decimal {
                value: lhs_value,
                scale: lhs_scale,
            },
            PartitionKeyValue::Decimal {
                value: rhs_value,
                scale: rhs_scale,
            },
        ) => {
            if lhs_scale == rhs_scale {
                return Ok(lhs_value.cmp(rhs_value));
            }
            let target_scale = (*lhs_scale).max(*rhs_scale);
            let lhs = scale_decimal_to(*lhs_value, *lhs_scale, target_scale)
                .ok_or_else(|| "decimal scale conversion overflow".to_string())?;
            let rhs = scale_decimal_to(*rhs_value, *rhs_scale, target_scale)
                .ok_or_else(|| "decimal scale conversion overflow".to_string())?;
            Ok(lhs.cmp(&rhs))
        }
        (PartitionKeyValue::Utf8(lhs), PartitionKeyValue::Utf8(rhs)) => Ok(lhs.cmp(rhs)),
        (PartitionKeyValue::Binary(lhs), PartitionKeyValue::Binary(rhs)) => Ok(lhs.cmp(rhs)),
        (lhs, rhs) => Err(format!(
            "incompatible partition key types: left={:?} right={:?}",
            lhs, rhs
        )),
    }
}

fn scale_decimal_to(value: i128, from_scale: i8, to_scale: i8) -> Option<i128> {
    if to_scale == from_scale {
        return Some(value);
    }
    if to_scale < from_scale {
        let divisor = pow10_i128((from_scale - to_scale) as u32)?;
        return Some(value / divisor);
    }
    let multiplier = pow10_i128((to_scale - from_scale) as u32)?;
    value.checked_mul(multiplier)
}

fn pow10_i128(exp: u32) -> Option<i128> {
    let mut out = 1_i128;
    for _ in 0..exp {
        out = out.checked_mul(10)?;
    }
    Some(out)
}
