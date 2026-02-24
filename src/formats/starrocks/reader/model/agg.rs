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
//! AGG_KEYS model reader.
//!
//! This reader reuses native segment decoding and applies StarRocks-like
//! key-based aggregation on top of decoded rows.
//!
//! Current aggregation scope:
//! - `SUM`, `MIN`, `MAX`, `REPLACE`, `REPLACE_IF_NOT_NULL`.
//! - `BITMAP_UNION`, `HLL_UNION`.
//! - `UNIQUE_KEYS` is treated as `REPLACE`-only aggregation.
//! - Scalar output columns only (no ARRAY/MAP/STRUCT aggregation).

use std::collections::{BTreeSet, HashMap};
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Date32Array,
    Date32Builder, Decimal128Array, Decimal128Builder, Float32Array, Float32Builder, Float64Array,
    Float64Builder, Int8Array, Int8Builder, Int16Array, Int16Builder, Int32Array, Int32Builder,
    Int64Array, Int64Builder, StringArray, StringBuilder, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder, new_empty_array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use roaring::RoaringBitmap;

use crate::connector::MinMaxPredicate;
use crate::connector::starrocks::ObjectStoreProfile;
use crate::formats::starrocks::plan::{
    StarRocksNativeColumnPlan, StarRocksNativeGroupKeyColumnPlan, StarRocksNativeReadPlan,
    StarRocksTableModelPlan,
};
use crate::formats::starrocks::segment::StarRocksSegmentFooter;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AggOp {
    Sum,
    Min,
    Max,
    Replace,
    ReplaceIfNotNull,
    BitmapUnion,
    HllUnion,
}

#[derive(Clone, Debug)]
struct AggColumnSpec {
    output_index: usize,
    output_name: String,
    data_type: DataType,
    is_key: bool,
    agg_op: Option<AggOp>,
}

#[derive(Clone, Debug)]
struct GroupKeySpec {
    scan_index: usize,
    output_name: String,
    data_type: DataType,
}

#[derive(Clone, Debug)]
enum AggCell {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    Date32(i32),
    TimestampMicros(i64),
    Utf8(String),
    Binary(Vec<u8>),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
}

pub(super) fn build_agg_record_batch(
    plan: &StarRocksNativeReadPlan,
    segment_footers: &[StarRocksSegmentFooter],
    tablet_root_path: &str,
    object_store_profile: Option<&ObjectStoreProfile>,
    output_schema: &SchemaRef,
    min_max_predicates: &[MinMaxPredicate],
) -> Result<RecordBatch, String> {
    let (scan_plan, scan_schema, group_key_specs) = build_agg_scan_plan(plan, output_schema)?;
    let raw = super::super::record_batch::build_dup_record_batch(
        &scan_plan,
        segment_footers,
        tablet_root_path,
        object_store_profile,
        &scan_schema,
        min_max_predicates,
    )?;
    aggregate_record_batch(plan, &raw, output_schema, &group_key_specs)
}

fn build_agg_scan_plan(
    plan: &StarRocksNativeReadPlan,
    output_schema: &SchemaRef,
) -> Result<(StarRocksNativeReadPlan, SchemaRef, Vec<GroupKeySpec>), String> {
    if plan.group_key_columns.is_empty() {
        return Err(format!(
            "{} native reader requires key columns in tablet schema: tablet_id={}, version={}",
            model_name(plan.table_model),
            plan.tablet_id,
            plan.version
        ));
    }

    let mut scan_plan = plan.clone();
    let mut scan_columns = plan.projected_columns.clone();
    let mut scan_fields = output_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<Field>>();

    let mut index_by_unique = HashMap::<u32, usize>::new();
    for projected in &scan_columns {
        index_by_unique.insert(projected.schema_unique_id, projected.output_index);
    }

    for key in &plan.group_key_columns {
        if index_by_unique.contains_key(&key.schema_unique_id) {
            continue;
        }
        let key_type = group_key_arrow_type(key)?;
        let output_index = scan_fields.len();
        let synthetic_name = format!("__sr_group_key_{}", key.output_name);
        scan_fields.push(Field::new(
            synthetic_name.clone(),
            key_type,
            key.schema.is_nullable,
        ));
        scan_columns.push(StarRocksNativeColumnPlan {
            output_index,
            output_name: synthetic_name,
            schema_unique_id: key.schema_unique_id,
            schema_type: key.schema_type.clone(),
            schema: key.schema.clone(),
        });
        index_by_unique.insert(key.schema_unique_id, output_index);
    }

    let mut group_key_specs = Vec::with_capacity(plan.group_key_columns.len());
    for key in &plan.group_key_columns {
        let scan_index = *index_by_unique.get(&key.schema_unique_id).ok_or_else(|| {
            format!(
                "{} internal key column is missing after scan-plan expansion: tablet_id={}, version={}, key_column={}, unique_id={}",
                model_name(plan.table_model),
                plan.tablet_id,
                plan.version,
                key.output_name,
                key.schema_unique_id
            )
        })?;
        let data_type = scan_fields
            .get(scan_index)
            .ok_or_else(|| {
                format!(
                    "{} internal key output index out of bounds: tablet_id={}, version={}, key_column={}, output_index={}, field_count={}",
                    model_name(plan.table_model),
                    plan.tablet_id,
                    plan.version,
                    key.output_name,
                    scan_index,
                    scan_fields.len()
                )
            })?
            .data_type()
            .clone();
        group_key_specs.push(GroupKeySpec {
            scan_index,
            output_name: key.output_name.clone(),
            data_type,
        });
    }

    scan_plan.projected_columns = scan_columns;
    let scan_schema = Arc::new(Schema::new(scan_fields));
    Ok((scan_plan, scan_schema, group_key_specs))
}

fn aggregate_record_batch(
    plan: &StarRocksNativeReadPlan,
    batch: &RecordBatch,
    output_schema: &SchemaRef,
    group_key_specs: &[GroupKeySpec],
) -> Result<RecordBatch, String> {
    if batch.num_rows() == 0 {
        return RecordBatch::try_new(
            output_schema.clone(),
            output_schema
                .fields()
                .iter()
                .map(|f| new_empty_array(f.data_type()))
                .collect::<Vec<_>>(),
        )
        .map_err(|e| {
            format!(
                "build empty {} record batch failed: {}",
                model_name(plan.table_model),
                e
            )
        });
    }

    let specs = build_agg_specs(plan, output_schema)?;
    if group_key_specs.is_empty() {
        return Err(format!(
            "{} native reader requires key columns in scan plan: tablet_id={}, version={}",
            model_name(plan.table_model),
            plan.tablet_id,
            plan.version
        ));
    }

    let arrays = batch.columns().to_vec();
    let mut grouped_rows: Vec<Vec<AggCell>> = Vec::new();
    let mut key_to_group = HashMap::<Vec<u8>, usize>::new();
    for row in 0..batch.num_rows() {
        let key = encode_key_row(&arrays, group_key_specs, row)?;
        if let Some(group_idx) = key_to_group.get(&key).copied() {
            merge_row_into_group(&arrays, row, &specs, &mut grouped_rows[group_idx])?;
        } else {
            let mut values = Vec::with_capacity(specs.len());
            for spec in &specs {
                values.push(cell_from_array(
                    arrays[spec.output_index].as_ref(),
                    &spec.data_type,
                    row,
                    &spec.output_name,
                )?);
            }
            key_to_group.insert(key, grouped_rows.len());
            grouped_rows.push(values);
        }
    }

    let mut out_arrays = Vec::<ArrayRef>::with_capacity(specs.len());
    for spec in &specs {
        out_arrays.push(build_array_from_cells(spec, &grouped_rows)?);
    }
    RecordBatch::try_new(output_schema.clone(), out_arrays).map_err(|e| {
        format!(
            "build aggregated record batch failed for {} reader: {}",
            model_name(plan.table_model),
            e
        )
    })
}

fn build_agg_specs(
    plan: &StarRocksNativeReadPlan,
    output_schema: &SchemaRef,
) -> Result<Vec<AggColumnSpec>, String> {
    let field_count = output_schema.fields().len();
    let mut slots = vec![None::<AggColumnSpec>; field_count];
    for projected in &plan.projected_columns {
        if projected.output_index >= field_count {
            return Err(format!(
                "AGG_KEYS projected output index out of bounds: output_index={}, field_count={}, output_column={}",
                projected.output_index, field_count, projected.output_name
            ));
        }
        let agg_op = if projected.schema.is_key {
            None
        } else if plan.table_model == StarRocksTableModelPlan::UniqueKeys {
            if let Some(raw) = projected.schema.aggregation.as_deref() {
                let parsed = parse_agg_op(raw, &projected.output_name)?;
                if parsed != AggOp::Replace {
                    return Err(format!(
                        "UNIQUE_KEYS value column must use REPLACE aggregation in native reader: output_column={}, aggregation={}",
                        projected.output_name, raw
                    ));
                }
            }
            Some(AggOp::Replace)
        } else {
            let raw = projected
                .schema
                .aggregation
                .as_deref()
                .ok_or_else(|| {
                    format!(
                        "missing aggregation type for {} value column: output_column={}, schema_type={}",
                        model_name(plan.table_model),
                        projected.output_name,
                        projected.schema.schema_type
                    )
                })?;
            Some(parse_agg_op(raw, &projected.output_name)?)
        };

        let spec = AggColumnSpec {
            output_index: projected.output_index,
            output_name: projected.output_name.clone(),
            data_type: output_schema
                .field(projected.output_index)
                .data_type()
                .clone(),
            is_key: projected.schema.is_key,
            agg_op,
        };
        if slots[projected.output_index].replace(spec).is_some() {
            return Err(format!(
                "duplicated AGG_KEYS projected output index: output_index={}, output_column={}",
                projected.output_index, projected.output_name
            ));
        }
    }

    let mut out = Vec::with_capacity(field_count);
    for (idx, slot) in slots.into_iter().enumerate() {
        let spec = slot.ok_or_else(|| {
            format!(
                "missing AGG_KEYS projected column plan for output index {}",
                idx
            )
        })?;
        out.push(spec);
    }
    Ok(out)
}

fn parse_agg_op(raw: &str, output_name: &str) -> Result<AggOp, String> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "SUM" => Ok(AggOp::Sum),
        "MIN" => Ok(AggOp::Min),
        "MAX" => Ok(AggOp::Max),
        "REPLACE" | "NONE" => Ok(AggOp::Replace),
        "REPLACE_IF_NOT_NULL" => Ok(AggOp::ReplaceIfNotNull),
        "BITMAP_UNION" => Ok(AggOp::BitmapUnion),
        "HLL_UNION" => Ok(AggOp::HllUnion),
        other => Err(format!(
            "unsupported AGG_KEYS aggregation in native reader: output_column={}, aggregation={}, supported=[SUM,MIN,MAX,REPLACE,REPLACE_IF_NOT_NULL,BITMAP_UNION,HLL_UNION]",
            output_name, other
        )),
    }
}

fn merge_row_into_group(
    arrays: &[ArrayRef],
    row: usize,
    specs: &[AggColumnSpec],
    group: &mut [AggCell],
) -> Result<(), String> {
    for spec in specs {
        if spec.is_key {
            continue;
        }
        let incoming = cell_from_array(
            arrays[spec.output_index].as_ref(),
            &spec.data_type,
            row,
            &spec.output_name,
        )?;
        let state = group.get_mut(spec.output_index).ok_or_else(|| {
            format!(
                "AGG_KEYS internal state out of bounds: {}",
                spec.output_name
            )
        })?;
        merge_cell(state, incoming, spec)?;
    }
    Ok(())
}

fn merge_cell(state: &mut AggCell, incoming: AggCell, spec: &AggColumnSpec) -> Result<(), String> {
    let op = spec.agg_op.ok_or_else(|| {
        format!(
            "missing aggregation op for AGG_KEYS value column: {}",
            spec.output_name
        )
    })?;

    match op {
        AggOp::Replace => {
            *state = incoming;
            Ok(())
        }
        AggOp::ReplaceIfNotNull => {
            if !matches!(incoming, AggCell::Null) {
                *state = incoming;
            }
            Ok(())
        }
        AggOp::Sum => merge_sum_cell(state, incoming, spec),
        AggOp::Min => merge_ordered_cell(state, incoming, spec, true),
        AggOp::Max => merge_ordered_cell(state, incoming, spec, false),
        AggOp::BitmapUnion => merge_bitmap_union_cell(state, incoming, spec),
        AggOp::HllUnion => merge_hll_union_cell(state, incoming, spec),
    }
}

const BITMAP_TYPE_EMPTY: u8 = 0;
const BITMAP_TYPE_SINGLE32: u8 = 1;
const BITMAP_TYPE_BITMAP32: u8 = 2;
const BITMAP_TYPE_SINGLE64: u8 = 3;
const BITMAP_TYPE_BITMAP64: u8 = 4;
const BITMAP_TYPE_SET: u8 = 10;
const BITMAP_TYPE_BITMAP32_SERIV2: u8 = 12;
const BITMAP_TYPE_BITMAP64_SERIV2: u8 = 13;

const HLL_DATA_EMPTY: u8 = 0;
const HLL_DATA_EXPLICIT: u8 = 1;
const HLL_DATA_SPARSE: u8 = 2;
const HLL_DATA_FULL: u8 = 3;

const HLL_COLUMN_PRECISION: usize = 14;
const HLL_EXPLICIT_INT64_NUM: usize = 160;
const HLL_SPARSE_THRESHOLD: usize = 4096;
const HLL_REGISTERS_COUNT: usize = 16 * 1024;

#[derive(Clone, Debug)]
enum StarRocksHllState {
    Empty,
    Explicit(BTreeSet<u64>),
    Registers(Vec<u8>),
}

fn merge_bitmap_union_cell(
    state: &mut AggCell,
    incoming: AggCell,
    spec: &AggColumnSpec,
) -> Result<(), String> {
    let incoming_bytes = cell_bytes_for_union(&incoming, spec, "BITMAP_UNION")?;
    let Some(incoming_bytes) = incoming_bytes else {
        return Ok(());
    };

    let mut merged = if let Some(state_bytes) = cell_bytes_for_union(state, spec, "BITMAP_UNION")? {
        decode_starrocks_bitmap(&state_bytes)?
    } else {
        BTreeSet::new()
    };
    merged.extend(decode_starrocks_bitmap(&incoming_bytes)?);
    let encoded = encode_starrocks_bitmap(&merged)?;
    *state = bytes_to_union_cell(encoded, spec, "BITMAP_UNION")?;
    Ok(())
}

fn merge_hll_union_cell(
    state: &mut AggCell,
    incoming: AggCell,
    spec: &AggColumnSpec,
) -> Result<(), String> {
    let incoming_bytes = cell_bytes_for_union(&incoming, spec, "HLL_UNION")?;
    let Some(incoming_bytes) = incoming_bytes else {
        return Ok(());
    };
    let incoming_hll = decode_starrocks_hll(&incoming_bytes)?;

    let mut merged = if let Some(state_bytes) = cell_bytes_for_union(state, spec, "HLL_UNION")? {
        decode_starrocks_hll(&state_bytes)?
    } else {
        StarRocksHllState::Empty
    };
    merge_starrocks_hll(&mut merged, &incoming_hll);
    let encoded = encode_starrocks_hll(&merged)?;
    *state = bytes_to_union_cell(encoded, spec, "HLL_UNION")?;
    Ok(())
}

fn cell_bytes_for_union(
    cell: &AggCell,
    spec: &AggColumnSpec,
    agg_name: &str,
) -> Result<Option<Vec<u8>>, String> {
    match cell {
        AggCell::Null => Ok(None),
        AggCell::Binary(v) => Ok(Some(v.clone())),
        AggCell::Utf8(v) => Ok(Some(v.as_bytes().to_vec())),
        _ => Err(format!(
            "{} expects UTF8/BINARY output column, but got data type {:?} on column {}",
            agg_name, spec.data_type, spec.output_name
        )),
    }
}

fn bytes_to_union_cell(
    bytes: Vec<u8>,
    spec: &AggColumnSpec,
    agg_name: &str,
) -> Result<AggCell, String> {
    match &spec.data_type {
        DataType::Binary => Ok(AggCell::Binary(bytes)),
        DataType::Utf8 => String::from_utf8(bytes).map(AggCell::Utf8).map_err(|e| {
            format!(
                "{} produced non-UTF8 bytes for UTF8 output column {}: {}",
                agg_name, spec.output_name, e
            )
        }),
        other => Err(format!(
            "{} expects UTF8/BINARY output column, but got data type {:?} on column {}",
            agg_name, other, spec.output_name
        )),
    }
}

fn decode_starrocks_bitmap(bytes: &[u8]) -> Result<BTreeSet<u64>, String> {
    if bytes.is_empty() {
        return Err("BITMAP_UNION input is empty".to_string());
    }
    let ty = bytes[0];
    match ty {
        BITMAP_TYPE_EMPTY => {
            if bytes.len() != 1 {
                return Err(format!(
                    "BITMAP_UNION EMPTY payload length mismatch: expected=1 actual={}",
                    bytes.len()
                ));
            }
            Ok(BTreeSet::new())
        }
        BITMAP_TYPE_SINGLE32 => {
            if bytes.len() != 1 + 4 {
                return Err(format!(
                    "BITMAP_UNION SINGLE32 payload length mismatch: expected=5 actual={}",
                    bytes.len()
                ));
            }
            let v = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|_| "BITMAP_UNION decode SINGLE32 failed".to_string())?,
            );
            Ok([u64::from(v)].into_iter().collect())
        }
        BITMAP_TYPE_SINGLE64 => {
            if bytes.len() != 1 + 8 {
                return Err(format!(
                    "BITMAP_UNION SINGLE64 payload length mismatch: expected=9 actual={}",
                    bytes.len()
                ));
            }
            let v = u64::from_le_bytes(
                bytes[1..9]
                    .try_into()
                    .map_err(|_| "BITMAP_UNION decode SINGLE64 failed".to_string())?,
            );
            Ok([v].into_iter().collect())
        }
        BITMAP_TYPE_SET => {
            if bytes.len() < 1 + 4 {
                return Err(format!(
                    "BITMAP_UNION SET payload too short: actual={}",
                    bytes.len()
                ));
            }
            let count = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|_| "BITMAP_UNION decode SET count failed".to_string())?,
            ) as usize;
            let expected = 1 + 4 + count.saturating_mul(8);
            if bytes.len() != expected {
                return Err(format!(
                    "BITMAP_UNION SET payload length mismatch: expected={} actual={} count={}",
                    expected,
                    bytes.len(),
                    count
                ));
            }
            let mut out = BTreeSet::new();
            let mut offset = 5usize;
            for _ in 0..count {
                let v = u64::from_le_bytes(
                    bytes[offset..offset + 8]
                        .try_into()
                        .map_err(|_| "BITMAP_UNION decode SET value failed".to_string())?,
                );
                out.insert(v);
                offset += 8;
            }
            Ok(out)
        }
        BITMAP_TYPE_BITMAP32_SERIV2 => {
            let mut cursor = Cursor::new(&bytes[1..]);
            let bitmap = RoaringBitmap::deserialize_from(&mut cursor).map_err(|e| {
                format!(
                    "BITMAP_UNION decode BITMAP32_SERIV2 roaring payload failed: {}",
                    e
                )
            })?;
            if cursor.position() as usize != bytes.len() - 1 {
                return Err(format!(
                    "BITMAP_UNION BITMAP32_SERIV2 payload has trailing bytes: consumed={} total={}",
                    cursor.position(),
                    bytes.len() - 1
                ));
            }
            Ok(bitmap.into_iter().map(u64::from).collect())
        }
        BITMAP_TYPE_BITMAP64_SERIV2 => {
            let mut offset = 1usize;
            let map_size = decode_varint_u64(bytes, &mut offset)? as usize;
            let mut out = BTreeSet::new();
            for entry_idx in 0..map_size {
                if offset + 4 > bytes.len() {
                    return Err(format!(
                        "BITMAP_UNION BITMAP64_SERIV2 key overflow: entry={} offset={} len={}",
                        entry_idx,
                        offset,
                        bytes.len()
                    ));
                }
                let high =
                    u32::from_le_bytes(bytes[offset..offset + 4].try_into().map_err(|_| {
                        "BITMAP_UNION decode BITMAP64_SERIV2 key failed".to_string()
                    })?);
                offset += 4;
                let mut cursor = Cursor::new(&bytes[offset..]);
                let bitmap = RoaringBitmap::deserialize_from(&mut cursor).map_err(|e| {
                    format!(
                        "BITMAP_UNION decode BITMAP64_SERIV2 roaring payload failed at entry {}: {}",
                        entry_idx, e
                    )
                })?;
                let consumed = cursor.position() as usize;
                if consumed == 0 {
                    return Err(format!(
                        "BITMAP_UNION decode BITMAP64_SERIV2 consumed zero bytes at entry {}",
                        entry_idx
                    ));
                }
                offset = offset.saturating_add(consumed);
                for low in bitmap {
                    let value = (u64::from(high) << 32) | u64::from(low);
                    out.insert(value);
                }
            }
            if offset != bytes.len() {
                return Err(format!(
                    "BITMAP_UNION BITMAP64_SERIV2 payload has trailing bytes: offset={} len={}",
                    offset,
                    bytes.len()
                ));
            }
            Ok(out)
        }
        BITMAP_TYPE_BITMAP32 | BITMAP_TYPE_BITMAP64 => Err(
            "BITMAP_UNION does not support legacy bitmap serialization version v1 (type_code=2/4)"
                .to_string(),
        ),
        other => Err(format!(
            "BITMAP_UNION unsupported bitmap payload type code: {}",
            other
        )),
    }
}

fn encode_starrocks_bitmap(values: &BTreeSet<u64>) -> Result<Vec<u8>, String> {
    if values.is_empty() {
        return Ok(vec![BITMAP_TYPE_EMPTY]);
    }
    if values.len() == 1 {
        let v = *values
            .first()
            .ok_or_else(|| "BITMAP_UNION internal empty set".to_string())?;
        if u32::try_from(v).is_ok() {
            let mut out = Vec::with_capacity(1 + 4);
            out.push(BITMAP_TYPE_SINGLE32);
            out.extend_from_slice(&(v as u32).to_le_bytes());
            return Ok(out);
        }
        let mut out = Vec::with_capacity(1 + 8);
        out.push(BITMAP_TYPE_SINGLE64);
        out.extend_from_slice(&v.to_le_bytes());
        return Ok(out);
    }
    let count = u32::try_from(values.len())
        .map_err(|_| format!("BITMAP_UNION value count overflow: {}", values.len()))?;
    let mut out = Vec::with_capacity(1 + 4 + values.len() * 8);
    out.push(BITMAP_TYPE_SET);
    out.extend_from_slice(&count.to_le_bytes());
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    Ok(out)
}

fn decode_varint_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, String> {
    let mut shift = 0u32;
    let mut out = 0u64;
    loop {
        if *offset >= bytes.len() {
            return Err("BITMAP_UNION decode varint reached end of payload".to_string());
        }
        let b = bytes[*offset];
        *offset += 1;
        out |= u64::from(b & 0x7f) << shift;
        if (b & 0x80) == 0 {
            return Ok(out);
        }
        shift += 7;
        if shift >= 64 {
            return Err("BITMAP_UNION decode varint overflow".to_string());
        }
    }
}

fn decode_starrocks_hll(bytes: &[u8]) -> Result<StarRocksHllState, String> {
    if bytes.is_empty() {
        return Err("HLL_UNION input is empty".to_string());
    }
    match bytes[0] {
        HLL_DATA_EMPTY => {
            if bytes.len() != 1 {
                return Err(format!(
                    "HLL_UNION EMPTY payload length mismatch: expected=1 actual={}",
                    bytes.len()
                ));
            }
            Ok(StarRocksHllState::Empty)
        }
        HLL_DATA_EXPLICIT => {
            if bytes.len() < 2 {
                return Err(format!(
                    "HLL_UNION EXPLICIT payload too short: actual={}",
                    bytes.len()
                ));
            }
            let count = bytes[1] as usize;
            let expected = 2 + count.saturating_mul(8);
            if bytes.len() != expected {
                return Err(format!(
                    "HLL_UNION EXPLICIT payload length mismatch: expected={} actual={} count={}",
                    expected,
                    bytes.len(),
                    count
                ));
            }
            let mut values = BTreeSet::new();
            let mut offset = 2usize;
            for _ in 0..count {
                let value = u64::from_le_bytes(
                    bytes[offset..offset + 8]
                        .try_into()
                        .map_err(|_| "HLL_UNION decode EXPLICIT value failed".to_string())?,
                );
                values.insert(value);
                offset += 8;
            }
            Ok(StarRocksHllState::Explicit(values))
        }
        HLL_DATA_SPARSE => {
            if bytes.len() < 5 {
                return Err(format!(
                    "HLL_UNION SPARSE payload too short: actual={}",
                    bytes.len()
                ));
            }
            let count = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|_| "HLL_UNION decode SPARSE count failed".to_string())?,
            ) as usize;
            let expected = 5 + count.saturating_mul(3);
            if bytes.len() != expected {
                return Err(format!(
                    "HLL_UNION SPARSE payload length mismatch: expected={} actual={} count={}",
                    expected,
                    bytes.len(),
                    count
                ));
            }
            let mut registers = vec![0u8; HLL_REGISTERS_COUNT];
            let mut offset = 5usize;
            for _ in 0..count {
                let idx = u16::from_le_bytes(
                    bytes[offset..offset + 2]
                        .try_into()
                        .map_err(|_| "HLL_UNION decode SPARSE index failed".to_string())?,
                ) as usize;
                offset += 2;
                if idx >= HLL_REGISTERS_COUNT {
                    return Err(format!(
                        "HLL_UNION SPARSE register index out of range: idx={} max={}",
                        idx,
                        HLL_REGISTERS_COUNT - 1
                    ));
                }
                let value = bytes[offset];
                offset += 1;
                registers[idx] = value;
            }
            Ok(StarRocksHllState::Registers(registers))
        }
        HLL_DATA_FULL => {
            let expected = 1 + HLL_REGISTERS_COUNT;
            if bytes.len() != expected {
                return Err(format!(
                    "HLL_UNION FULL payload length mismatch: expected={} actual={}",
                    expected,
                    bytes.len()
                ));
            }
            Ok(StarRocksHllState::Registers(bytes[1..].to_vec()))
        }
        other => Err(format!(
            "HLL_UNION unsupported hll payload type code: {}",
            other
        )),
    }
}

fn merge_starrocks_hll(left: &mut StarRocksHllState, right: &StarRocksHllState) {
    if matches!(right, StarRocksHllState::Empty) {
        return;
    }
    match left {
        StarRocksHllState::Empty => {
            *left = right.clone();
        }
        StarRocksHllState::Explicit(left_values) => match right {
            StarRocksHllState::Empty => {}
            StarRocksHllState::Explicit(right_values) => {
                left_values.extend(right_values.iter().copied());
                if left_values.len() > HLL_EXPLICIT_INT64_NUM {
                    let registers = hll_explicit_to_registers(left_values);
                    *left = StarRocksHllState::Registers(registers);
                }
            }
            StarRocksHllState::Registers(right_regs) => {
                let mut registers = hll_explicit_to_registers(left_values);
                merge_hll_registers(&mut registers, right_regs);
                *left = StarRocksHllState::Registers(registers);
            }
        },
        StarRocksHllState::Registers(left_regs) => match right {
            StarRocksHllState::Empty => {}
            StarRocksHllState::Explicit(right_values) => {
                for hash_value in right_values {
                    hll_update_register(left_regs, *hash_value);
                }
            }
            StarRocksHllState::Registers(right_regs) => merge_hll_registers(left_regs, right_regs),
        },
    }
}

fn hll_explicit_to_registers(values: &BTreeSet<u64>) -> Vec<u8> {
    let mut registers = vec![0u8; HLL_REGISTERS_COUNT];
    for hash_value in values {
        hll_update_register(&mut registers, *hash_value);
    }
    registers
}

fn hll_update_register(registers: &mut [u8], hash_value: u64) {
    let idx = (hash_value % HLL_REGISTERS_COUNT as u64) as usize;
    let mut shifted = hash_value >> HLL_COLUMN_PRECISION;
    shifted |= 1_u64 << (64 - HLL_COLUMN_PRECISION);
    let first_one_bit = shifted.trailing_zeros() as u8 + 1;
    if registers[idx] < first_one_bit {
        registers[idx] = first_one_bit;
    }
}

fn merge_hll_registers(left: &mut [u8], right: &[u8]) {
    for (l, r) in left.iter_mut().zip(right.iter()) {
        if *l < *r {
            *l = *r;
        }
    }
}

fn encode_starrocks_hll(hll: &StarRocksHllState) -> Result<Vec<u8>, String> {
    match hll {
        StarRocksHllState::Empty => Ok(vec![HLL_DATA_EMPTY]),
        StarRocksHllState::Explicit(values) => {
            if values.is_empty() {
                return Ok(vec![HLL_DATA_EMPTY]);
            }
            if values.len() > HLL_EXPLICIT_INT64_NUM {
                let registers = hll_explicit_to_registers(values);
                return encode_starrocks_hll(&StarRocksHllState::Registers(registers));
            }
            let count = u8::try_from(values.len())
                .map_err(|_| format!("HLL_UNION EXPLICIT count overflow: {}", values.len()))?;
            let mut out = Vec::with_capacity(2 + values.len() * 8);
            out.push(HLL_DATA_EXPLICIT);
            out.push(count);
            for hash_value in values {
                out.extend_from_slice(&hash_value.to_le_bytes());
            }
            Ok(out)
        }
        StarRocksHllState::Registers(registers) => {
            if registers.len() != HLL_REGISTERS_COUNT {
                return Err(format!(
                    "HLL_UNION register payload length mismatch: expected={} actual={}",
                    HLL_REGISTERS_COUNT,
                    registers.len()
                ));
            }
            let non_zero = registers.iter().filter(|v| **v != 0).count();
            if non_zero > HLL_SPARSE_THRESHOLD {
                let mut out = Vec::with_capacity(1 + HLL_REGISTERS_COUNT);
                out.push(HLL_DATA_FULL);
                out.extend_from_slice(registers);
                return Ok(out);
            }
            let count = u32::try_from(non_zero)
                .map_err(|_| format!("HLL_UNION sparse register count overflow: {}", non_zero))?;
            let mut out = Vec::with_capacity(1 + 4 + non_zero * 3);
            out.push(HLL_DATA_SPARSE);
            out.extend_from_slice(&count.to_le_bytes());
            for (idx, value) in registers.iter().enumerate() {
                if *value == 0 {
                    continue;
                }
                let idx_u16 = u16::try_from(idx)
                    .map_err(|_| format!("HLL_UNION sparse register index overflow: {}", idx))?;
                out.extend_from_slice(&idx_u16.to_le_bytes());
                out.push(*value);
            }
            Ok(out)
        }
    }
}

fn merge_sum_cell(
    state: &mut AggCell,
    incoming: AggCell,
    spec: &AggColumnSpec,
) -> Result<(), String> {
    if matches!(incoming, AggCell::Null) {
        return Ok(());
    }
    if matches!(state, AggCell::Null) {
        *state = incoming;
        return Ok(());
    }

    *state = match (state.clone(), incoming) {
        (AggCell::Int8(a), AggCell::Int8(b)) => {
            AggCell::Int8(a.checked_add(b).ok_or_else(|| {
                format!("AGG_KEYS SUM overflow on INT8 column {}", spec.output_name)
            })?)
        }
        (AggCell::Int16(a), AggCell::Int16(b)) => {
            AggCell::Int16(a.checked_add(b).ok_or_else(|| {
                format!("AGG_KEYS SUM overflow on INT16 column {}", spec.output_name)
            })?)
        }
        (AggCell::Int32(a), AggCell::Int32(b)) => {
            AggCell::Int32(a.checked_add(b).ok_or_else(|| {
                format!("AGG_KEYS SUM overflow on INT32 column {}", spec.output_name)
            })?)
        }
        (AggCell::Int64(a), AggCell::Int64(b)) => {
            AggCell::Int64(a.checked_add(b).ok_or_else(|| {
                format!("AGG_KEYS SUM overflow on INT64 column {}", spec.output_name)
            })?)
        }
        (AggCell::Float32(a), AggCell::Float32(b)) => AggCell::Float32(a + b),
        (AggCell::Float64(a), AggCell::Float64(b)) => AggCell::Float64(a + b),
        (
            AggCell::Decimal128 {
                value: a,
                precision,
                scale,
            },
            AggCell::Decimal128 {
                value: b,
                precision: p2,
                scale: s2,
            },
        ) => {
            if precision != p2 || scale != s2 {
                return Err(format!(
                    "AGG_KEYS decimal SUM precision/scale mismatch on column {}",
                    spec.output_name
                ));
            }
            AggCell::Decimal128 {
                value: a.checked_add(b).ok_or_else(|| {
                    format!(
                        "AGG_KEYS SUM overflow on DECIMAL column {}",
                        spec.output_name
                    )
                })?,
                precision,
                scale,
            }
        }
        _ => {
            return Err(format!(
                "AGG_KEYS SUM type mismatch on column {} with data type {:?}",
                spec.output_name, spec.data_type
            ));
        }
    };
    Ok(())
}

fn merge_ordered_cell(
    state: &mut AggCell,
    incoming: AggCell,
    spec: &AggColumnSpec,
    is_min: bool,
) -> Result<(), String> {
    if matches!(incoming, AggCell::Null) {
        return Ok(());
    }
    if matches!(state, AggCell::Null) {
        *state = incoming;
        return Ok(());
    }

    let cmp = compare_cells(state, &incoming, spec)?;
    let should_replace = if is_min { cmp.is_gt() } else { cmp.is_lt() };
    if should_replace {
        *state = incoming;
    }
    Ok(())
}

fn compare_cells(
    left: &AggCell,
    right: &AggCell,
    spec: &AggColumnSpec,
) -> Result<std::cmp::Ordering, String> {
    use std::cmp::Ordering;
    let ord = match (left, right) {
        (AggCell::Int8(a), AggCell::Int8(b)) => a.cmp(b),
        (AggCell::Int16(a), AggCell::Int16(b)) => a.cmp(b),
        (AggCell::Int32(a), AggCell::Int32(b)) => a.cmp(b),
        (AggCell::Int64(a), AggCell::Int64(b)) => a.cmp(b),
        (AggCell::Float32(a), AggCell::Float32(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (AggCell::Float64(a), AggCell::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (AggCell::Boolean(a), AggCell::Boolean(b)) => a.cmp(b),
        (AggCell::Date32(a), AggCell::Date32(b)) => a.cmp(b),
        (AggCell::TimestampMicros(a), AggCell::TimestampMicros(b)) => a.cmp(b),
        (AggCell::Utf8(a), AggCell::Utf8(b)) => a.as_bytes().cmp(b.as_bytes()),
        (AggCell::Binary(a), AggCell::Binary(b)) => a.cmp(b),
        (
            AggCell::Decimal128 {
                value: a,
                precision: p1,
                scale: s1,
            },
            AggCell::Decimal128 {
                value: b,
                precision: p2,
                scale: s2,
            },
        ) => {
            if p1 != p2 || s1 != s2 {
                return Err(format!(
                    "AGG_KEYS decimal precision/scale mismatch on column {}",
                    spec.output_name
                ));
            }
            a.cmp(b)
        }
        _ => {
            return Err(format!(
                "AGG_KEYS compare type mismatch on column {} with data type {:?}",
                spec.output_name, spec.data_type
            ));
        }
    };
    Ok(ord)
}

fn encode_key_row(
    arrays: &[ArrayRef],
    key_specs: &[GroupKeySpec],
    row: usize,
) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    for spec in key_specs {
        let value = cell_from_array(
            arrays[spec.scan_index].as_ref(),
            &spec.data_type,
            row,
            &spec.output_name,
        )?;
        encode_cell_for_key(&value, &mut out)?;
    }
    Ok(out)
}

fn model_name(model: StarRocksTableModelPlan) -> &'static str {
    match model {
        StarRocksTableModelPlan::AggKeys => "AGG_KEYS",
        StarRocksTableModelPlan::UniqueKeys => "UNIQUE_KEYS",
        StarRocksTableModelPlan::DupKeys => "DUP_KEYS",
        StarRocksTableModelPlan::PrimaryKeys => "PRIMARY_KEYS",
    }
}

fn group_key_arrow_type(key: &StarRocksNativeGroupKeyColumnPlan) -> Result<DataType, String> {
    match key.schema.schema_type.as_str() {
        "TINYINT" => Ok(DataType::Int8),
        "SMALLINT" => Ok(DataType::Int16),
        "INT" => Ok(DataType::Int32),
        "BIGINT" => Ok(DataType::Int64),
        "FLOAT" => Ok(DataType::Float32),
        "DOUBLE" => Ok(DataType::Float64),
        "BOOLEAN" => Ok(DataType::Boolean),
        "DATE" => Ok(DataType::Date32),
        "DATETIME" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "CHAR" | "VARCHAR" => Ok(DataType::Utf8),
        "BINARY" | "VARBINARY" => Ok(DataType::Binary),
        "DECIMAL32" | "DECIMAL64" | "DECIMAL128" => {
            let precision = key.schema.precision.ok_or_else(|| {
                format!(
                    "missing decimal precision for group key column in native reader: output_column={}, schema_type={}",
                    key.output_name, key.schema.schema_type
                )
            })?;
            let scale = key.schema.scale.ok_or_else(|| {
                format!(
                    "missing decimal scale for group key column in native reader: output_column={}, schema_type={}",
                    key.output_name, key.schema.schema_type
                )
            })?;
            Ok(DataType::Decimal128(precision, scale))
        }
        other => Err(format!(
            "unsupported group key schema type in native reader: output_column={}, schema_type={}",
            key.output_name, other
        )),
    }
}

fn encode_cell_for_key(value: &AggCell, out: &mut Vec<u8>) -> Result<(), String> {
    match value {
        AggCell::Null => out.push(0),
        AggCell::Int8(v) => {
            out.push(1);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggCell::Int16(v) => {
            out.push(2);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggCell::Int32(v) => {
            out.push(3);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggCell::Int64(v) => {
            out.push(4);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggCell::Float32(v) => {
            out.push(5);
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        AggCell::Float64(v) => {
            out.push(6);
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        AggCell::Boolean(v) => {
            out.push(7);
            out.push(u8::from(*v));
        }
        AggCell::Date32(v) => {
            out.push(8);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggCell::TimestampMicros(v) => {
            out.push(9);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggCell::Utf8(v) => {
            out.push(10);
            append_len_prefixed(out, v.as_bytes())?;
        }
        AggCell::Binary(v) => {
            out.push(11);
            append_len_prefixed(out, v)?;
        }
        AggCell::Decimal128 {
            value,
            precision,
            scale,
        } => {
            out.push(12);
            out.push(*precision);
            out.push(*scale as u8);
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
    Ok(())
}

fn append_len_prefixed(out: &mut Vec<u8>, data: &[u8]) -> Result<(), String> {
    let len = u32::try_from(data.len())
        .map_err(|_| format!("AGG_KEYS key cell too large: length={}", data.len()))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(data);
    Ok(())
}

fn cell_from_array(
    array: &dyn Array,
    data_type: &DataType,
    row: usize,
    output_name: &str,
) -> Result<AggCell, String> {
    if array.is_null(row) {
        return Ok(AggCell::Null);
    }
    match data_type {
        DataType::Int8 => Ok(AggCell::Int8(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| format!("AGG_KEYS column type mismatch for {}", output_name))?
                .value(row),
        )),
        DataType::Int16 => Ok(AggCell::Int16(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| format!("AGG_KEYS column type mismatch for {}", output_name))?
                .value(row),
        )),
        DataType::Int32 => Ok(AggCell::Int32(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| format!("AGG_KEYS column type mismatch for {}", output_name))?
                .value(row),
        )),
        DataType::Int64 => Ok(AggCell::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("AGG_KEYS column type mismatch for {}", output_name))?
                .value(row),
        )),
        DataType::Float32 => Ok(AggCell::Float32(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| format!("AGG_KEYS column type mismatch for {}", output_name))?
                .value(row),
        )),
        DataType::Float64 => Ok(AggCell::Float64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| format!("AGG_KEYS column type mismatch for {}", output_name))?
                .value(row),
        )),
        DataType::Boolean => Ok(AggCell::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| format!("AGG_KEYS column type mismatch for {}", output_name))?
                .value(row),
        )),
        DataType::Date32 => Ok(AggCell::Date32(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| format!("AGG_KEYS column type mismatch for {}", output_name))?
                .value(row),
        )),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            Ok(AggCell::TimestampMicros(
                array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| format!("AGG_KEYS column type mismatch for {}", output_name))?
                    .value(row),
            ))
        }
        DataType::Utf8 => Ok(AggCell::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("AGG_KEYS column type mismatch for {}", output_name))?
                .value(row)
                .to_string(),
        )),
        DataType::Binary => Ok(AggCell::Binary(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| format!("AGG_KEYS column type mismatch for {}", output_name))?
                .value(row)
                .to_vec(),
        )),
        DataType::Decimal128(precision, scale) => Ok(AggCell::Decimal128 {
            value: array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| format!("AGG_KEYS column type mismatch for {}", output_name))?
                .value(row),
            precision: *precision,
            scale: *scale,
        }),
        _ => Err(format!(
            "AGG_KEYS native reader does not support output data type {:?} on column {}",
            data_type, output_name
        )),
    }
}

fn build_array_from_cells(spec: &AggColumnSpec, rows: &[Vec<AggCell>]) -> Result<ArrayRef, String> {
    let values = rows
        .iter()
        .map(|row| {
            row.get(spec.output_index).ok_or_else(|| {
                format!(
                    "AGG_KEYS internal row state out of bounds for column {}",
                    spec.output_name
                )
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    match &spec.data_type {
        DataType::Int8 => {
            let mut builder = Int8Builder::with_capacity(values.len());
            for value in values {
                match value {
                    AggCell::Null => builder.append_null(),
                    AggCell::Int8(v) => builder.append_value(*v),
                    _ => {
                        return Err(format!(
                            "AGG_KEYS output type mismatch when building INT8 column {}",
                            spec.output_name
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::with_capacity(values.len());
            for value in values {
                match value {
                    AggCell::Null => builder.append_null(),
                    AggCell::Int16(v) => builder.append_value(*v),
                    _ => {
                        return Err(format!(
                            "AGG_KEYS output type mismatch when building INT16 column {}",
                            spec.output_name
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(values.len());
            for value in values {
                match value {
                    AggCell::Null => builder.append_null(),
                    AggCell::Int32(v) => builder.append_value(*v),
                    _ => {
                        return Err(format!(
                            "AGG_KEYS output type mismatch when building INT32 column {}",
                            spec.output_name
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(values.len());
            for value in values {
                match value {
                    AggCell::Null => builder.append_null(),
                    AggCell::Int64(v) => builder.append_value(*v),
                    _ => {
                        return Err(format!(
                            "AGG_KEYS output type mismatch when building INT64 column {}",
                            spec.output_name
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(values.len());
            for value in values {
                match value {
                    AggCell::Null => builder.append_null(),
                    AggCell::Float32(v) => builder.append_value(*v),
                    _ => {
                        return Err(format!(
                            "AGG_KEYS output type mismatch when building FLOAT column {}",
                            spec.output_name
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(values.len());
            for value in values {
                match value {
                    AggCell::Null => builder.append_null(),
                    AggCell::Float64(v) => builder.append_value(*v),
                    _ => {
                        return Err(format!(
                            "AGG_KEYS output type mismatch when building DOUBLE column {}",
                            spec.output_name
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for value in values {
                match value {
                    AggCell::Null => builder.append_null(),
                    AggCell::Boolean(v) => builder.append_value(*v),
                    _ => {
                        return Err(format!(
                            "AGG_KEYS output type mismatch when building BOOLEAN column {}",
                            spec.output_name
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::with_capacity(values.len());
            for value in values {
                match value {
                    AggCell::Null => builder.append_null(),
                    AggCell::Date32(v) => builder.append_value(*v),
                    _ => {
                        return Err(format!(
                            "AGG_KEYS output type mismatch when building DATE column {}",
                            spec.output_name
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len());
            for value in values {
                match value {
                    AggCell::Null => builder.append_null(),
                    AggCell::TimestampMicros(v) => builder.append_value(*v),
                    _ => {
                        return Err(format!(
                            "AGG_KEYS output type mismatch when building DATETIME column {}",
                            spec.output_name
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(values.len(), 0);
            for value in values {
                match value {
                    AggCell::Null => builder.append_null(),
                    AggCell::Utf8(v) => builder.append_value(v),
                    _ => {
                        return Err(format!(
                            "AGG_KEYS output type mismatch when building VARCHAR column {}",
                            spec.output_name
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(values.len(), 0);
            for value in values {
                match value {
                    AggCell::Null => builder.append_null(),
                    AggCell::Binary(v) => builder.append_value(v),
                    _ => {
                        return Err(format!(
                            "AGG_KEYS output type mismatch when building BINARY column {}",
                            spec.output_name
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Decimal128(precision, scale) => {
            let mut builder = Decimal128Builder::with_capacity(values.len())
                .with_data_type(DataType::Decimal128(*precision, *scale));
            for value in values {
                match value {
                    AggCell::Null => builder.append_null(),
                    AggCell::Decimal128 {
                        value,
                        precision: p,
                        scale: s,
                    } => {
                        if p != precision || s != scale {
                            return Err(format!(
                                "AGG_KEYS decimal precision/scale mismatch when building column {}",
                                spec.output_name
                            ));
                        }
                        builder.append_value(*value);
                    }
                    _ => {
                        return Err(format!(
                            "AGG_KEYS output type mismatch when building DECIMAL column {}",
                            spec.output_name
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(format!(
            "AGG_KEYS native reader does not support building output type {:?} for column {}",
            spec.data_type, spec.output_name
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::starrocks::plan::{
        StarRocksNativeColumnPlan, StarRocksNativeGroupKeyColumnPlan, StarRocksNativeReadPlan,
        StarRocksNativeSchemaColumnPlan, StarRocksTableModelPlan,
    };
    use arrow::array::{BinaryArray, Int64Array};
    use arrow::datatypes::{Field, Schema};

    #[test]
    fn aggregate_record_batch_sums_values_by_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int64, true),
            Field::new("v1", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(5)])) as ArrayRef,
            ],
        )
        .expect("build input batch");

        let plan = StarRocksNativeReadPlan {
            tablet_id: 10,
            version: 20,
            table_model: StarRocksTableModelPlan::AggKeys,
            projected_columns: vec![
                StarRocksNativeColumnPlan {
                    output_index: 0,
                    output_name: "k1".to_string(),
                    schema_unique_id: 1,
                    schema_type: "BIGINT".to_string(),
                    schema: StarRocksNativeSchemaColumnPlan {
                        unique_id: Some(1),
                        schema_type: "BIGINT".to_string(),
                        is_nullable: true,
                        is_key: true,
                        aggregation: None,
                        precision: None,
                        scale: None,
                        children: Vec::new(),
                    },
                },
                StarRocksNativeColumnPlan {
                    output_index: 1,
                    output_name: "v1".to_string(),
                    schema_unique_id: 2,
                    schema_type: "BIGINT".to_string(),
                    schema: StarRocksNativeSchemaColumnPlan {
                        unique_id: Some(2),
                        schema_type: "BIGINT".to_string(),
                        is_nullable: true,
                        is_key: false,
                        aggregation: Some("SUM".to_string()),
                        precision: None,
                        scale: None,
                        children: Vec::new(),
                    },
                },
            ],
            group_key_columns: vec![StarRocksNativeGroupKeyColumnPlan {
                output_name: "k1".to_string(),
                schema_unique_id: 1,
                schema_type: "BIGINT".to_string(),
                schema: StarRocksNativeSchemaColumnPlan {
                    unique_id: Some(1),
                    schema_type: "BIGINT".to_string(),
                    is_nullable: true,
                    is_key: true,
                    aggregation: None,
                    precision: None,
                    scale: None,
                    children: Vec::new(),
                },
            }],
            segments: Vec::new(),
            delete_predicates: Vec::new(),
            primary_delvec: None,
            estimated_rows: 0,
        };

        let out = aggregate_record_batch(
            &plan,
            &batch,
            &schema,
            &[GroupKeySpec {
                scan_index: 0,
                output_name: "k1".to_string(),
                data_type: DataType::Int64,
            }],
        )
        .expect("aggregate batch");
        let out_k = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("k1 int64");
        let out_v = out
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v1 int64");

        assert_eq!(out.num_rows(), 2);
        assert_eq!(out_k.value(0), 1);
        assert_eq!(out_v.value(0), 30);
        assert_eq!(out_k.value(1), 2);
        assert_eq!(out_v.value(1), 5);
    }

    #[test]
    fn aggregate_record_batch_allows_output_without_key_columns() {
        let scan_schema = Arc::new(Schema::new(vec![
            Field::new("v1", DataType::Int64, true),
            Field::new("__sr_group_key_k1", DataType::Int64, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![Field::new("v1", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            scan_schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(5)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2)])) as ArrayRef,
            ],
        )
        .expect("build input batch");
        let plan = StarRocksNativeReadPlan {
            tablet_id: 10,
            version: 20,
            table_model: StarRocksTableModelPlan::AggKeys,
            projected_columns: vec![StarRocksNativeColumnPlan {
                output_index: 0,
                output_name: "v1".to_string(),
                schema_unique_id: 2,
                schema_type: "BIGINT".to_string(),
                schema: StarRocksNativeSchemaColumnPlan {
                    unique_id: Some(2),
                    schema_type: "BIGINT".to_string(),
                    is_nullable: true,
                    is_key: false,
                    aggregation: Some("SUM".to_string()),
                    precision: None,
                    scale: None,
                    children: Vec::new(),
                },
            }],
            group_key_columns: vec![StarRocksNativeGroupKeyColumnPlan {
                output_name: "k1".to_string(),
                schema_unique_id: 1,
                schema_type: "BIGINT".to_string(),
                schema: StarRocksNativeSchemaColumnPlan {
                    unique_id: Some(1),
                    schema_type: "BIGINT".to_string(),
                    is_nullable: true,
                    is_key: true,
                    aggregation: None,
                    precision: None,
                    scale: None,
                    children: Vec::new(),
                },
            }],
            segments: Vec::new(),
            delete_predicates: Vec::new(),
            primary_delvec: None,
            estimated_rows: 0,
        };

        let out = aggregate_record_batch(
            &plan,
            &batch,
            &output_schema,
            &[GroupKeySpec {
                scan_index: 1,
                output_name: "k1".to_string(),
                data_type: DataType::Int64,
            }],
        )
        .expect("aggregate batch");
        let out_v = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v1 int64");
        assert_eq!(out.num_rows(), 2);
        assert_eq!(out_v.value(0), 30);
        assert_eq!(out_v.value(1), 5);
    }

    #[test]
    fn build_agg_specs_treats_unique_values_as_replace() {
        let output_schema = Arc::new(Schema::new(vec![Field::new("c2", DataType::Int64, true)]));
        let plan = StarRocksNativeReadPlan {
            tablet_id: 11,
            version: 22,
            table_model: StarRocksTableModelPlan::UniqueKeys,
            projected_columns: vec![StarRocksNativeColumnPlan {
                output_index: 0,
                output_name: "c2".to_string(),
                schema_unique_id: 2,
                schema_type: "BIGINT".to_string(),
                schema: StarRocksNativeSchemaColumnPlan {
                    unique_id: Some(2),
                    schema_type: "BIGINT".to_string(),
                    is_nullable: true,
                    is_key: false,
                    aggregation: None,
                    precision: None,
                    scale: None,
                    children: Vec::new(),
                },
            }],
            group_key_columns: vec![StarRocksNativeGroupKeyColumnPlan {
                output_name: "c1".to_string(),
                schema_unique_id: 1,
                schema_type: "BIGINT".to_string(),
                schema: StarRocksNativeSchemaColumnPlan {
                    unique_id: Some(1),
                    schema_type: "BIGINT".to_string(),
                    is_nullable: true,
                    is_key: true,
                    aggregation: None,
                    precision: None,
                    scale: None,
                    children: Vec::new(),
                },
            }],
            segments: Vec::new(),
            delete_predicates: Vec::new(),
            primary_delvec: None,
            estimated_rows: 0,
        };

        let specs = build_agg_specs(&plan, &output_schema).expect("build agg specs");
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].agg_op, Some(AggOp::Replace));
    }

    fn bitmap_single32(value: u32) -> Vec<u8> {
        let mut out = Vec::with_capacity(5);
        out.push(BITMAP_TYPE_SINGLE32);
        out.extend_from_slice(&value.to_le_bytes());
        out
    }

    fn hll_explicit(values: &[u64]) -> Vec<u8> {
        let mut out = Vec::with_capacity(2 + values.len() * 8);
        out.push(HLL_DATA_EXPLICIT);
        out.push(values.len() as u8);
        for value in values {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }

    #[test]
    fn aggregate_record_batch_bitmap_union_merges_values_by_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int64, true),
            Field::new("b1", DataType::Binary, true),
        ]));
        let mut bitmap_builder = BinaryBuilder::new();
        bitmap_builder.append_value(bitmap_single32(1));
        bitmap_builder.append_value(bitmap_single32(3));
        bitmap_builder.append_value(bitmap_single32(7));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2)])) as ArrayRef,
                Arc::new(bitmap_builder.finish()) as ArrayRef,
            ],
        )
        .expect("build input batch");

        let plan = StarRocksNativeReadPlan {
            tablet_id: 21,
            version: 42,
            table_model: StarRocksTableModelPlan::AggKeys,
            projected_columns: vec![
                StarRocksNativeColumnPlan {
                    output_index: 0,
                    output_name: "k1".to_string(),
                    schema_unique_id: 1,
                    schema_type: "BIGINT".to_string(),
                    schema: StarRocksNativeSchemaColumnPlan {
                        unique_id: Some(1),
                        schema_type: "BIGINT".to_string(),
                        is_nullable: true,
                        is_key: true,
                        aggregation: None,
                        precision: None,
                        scale: None,
                        children: Vec::new(),
                    },
                },
                StarRocksNativeColumnPlan {
                    output_index: 1,
                    output_name: "b1".to_string(),
                    schema_unique_id: 2,
                    schema_type: "OBJECT".to_string(),
                    schema: StarRocksNativeSchemaColumnPlan {
                        unique_id: Some(2),
                        schema_type: "OBJECT".to_string(),
                        is_nullable: true,
                        is_key: false,
                        aggregation: Some("BITMAP_UNION".to_string()),
                        precision: None,
                        scale: None,
                        children: Vec::new(),
                    },
                },
            ],
            group_key_columns: vec![StarRocksNativeGroupKeyColumnPlan {
                output_name: "k1".to_string(),
                schema_unique_id: 1,
                schema_type: "BIGINT".to_string(),
                schema: StarRocksNativeSchemaColumnPlan {
                    unique_id: Some(1),
                    schema_type: "BIGINT".to_string(),
                    is_nullable: true,
                    is_key: true,
                    aggregation: None,
                    precision: None,
                    scale: None,
                    children: Vec::new(),
                },
            }],
            segments: Vec::new(),
            delete_predicates: Vec::new(),
            primary_delvec: None,
            estimated_rows: 0,
        };

        let out = aggregate_record_batch(
            &plan,
            &batch,
            &schema,
            &[GroupKeySpec {
                scan_index: 0,
                output_name: "k1".to_string(),
                data_type: DataType::Int64,
            }],
        )
        .expect("aggregate batch");
        assert_eq!(out.num_rows(), 2);

        let bitmap_col = out
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("bitmap binary");
        let first = decode_starrocks_bitmap(bitmap_col.value(0)).expect("decode first bitmap");
        let second = decode_starrocks_bitmap(bitmap_col.value(1)).expect("decode second bitmap");
        assert_eq!(first, [1_u64, 3_u64].into_iter().collect());
        assert_eq!(second, [7_u64].into_iter().collect());
    }

    #[test]
    fn aggregate_record_batch_hll_union_merges_values_by_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int64, true),
            Field::new("h1", DataType::Binary, true),
        ]));
        let mut hll_builder = BinaryBuilder::new();
        hll_builder.append_value(hll_explicit(&[11]));
        hll_builder.append_value(hll_explicit(&[22]));
        hll_builder.append_value(hll_explicit(&[33]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2)])) as ArrayRef,
                Arc::new(hll_builder.finish()) as ArrayRef,
            ],
        )
        .expect("build input batch");

        let plan = StarRocksNativeReadPlan {
            tablet_id: 22,
            version: 43,
            table_model: StarRocksTableModelPlan::AggKeys,
            projected_columns: vec![
                StarRocksNativeColumnPlan {
                    output_index: 0,
                    output_name: "k1".to_string(),
                    schema_unique_id: 1,
                    schema_type: "BIGINT".to_string(),
                    schema: StarRocksNativeSchemaColumnPlan {
                        unique_id: Some(1),
                        schema_type: "BIGINT".to_string(),
                        is_nullable: true,
                        is_key: true,
                        aggregation: None,
                        precision: None,
                        scale: None,
                        children: Vec::new(),
                    },
                },
                StarRocksNativeColumnPlan {
                    output_index: 1,
                    output_name: "h1".to_string(),
                    schema_unique_id: 2,
                    schema_type: "HLL".to_string(),
                    schema: StarRocksNativeSchemaColumnPlan {
                        unique_id: Some(2),
                        schema_type: "HLL".to_string(),
                        is_nullable: true,
                        is_key: false,
                        aggregation: Some("HLL_UNION".to_string()),
                        precision: None,
                        scale: None,
                        children: Vec::new(),
                    },
                },
            ],
            group_key_columns: vec![StarRocksNativeGroupKeyColumnPlan {
                output_name: "k1".to_string(),
                schema_unique_id: 1,
                schema_type: "BIGINT".to_string(),
                schema: StarRocksNativeSchemaColumnPlan {
                    unique_id: Some(1),
                    schema_type: "BIGINT".to_string(),
                    is_nullable: true,
                    is_key: true,
                    aggregation: None,
                    precision: None,
                    scale: None,
                    children: Vec::new(),
                },
            }],
            segments: Vec::new(),
            delete_predicates: Vec::new(),
            primary_delvec: None,
            estimated_rows: 0,
        };

        let out = aggregate_record_batch(
            &plan,
            &batch,
            &schema,
            &[GroupKeySpec {
                scan_index: 0,
                output_name: "k1".to_string(),
                data_type: DataType::Int64,
            }],
        )
        .expect("aggregate batch");
        assert_eq!(out.num_rows(), 2);

        let hll_col = out
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("hll binary");
        let first = decode_starrocks_hll(hll_col.value(0)).expect("decode first hll");
        let second = decode_starrocks_hll(hll_col.value(1)).expect("decode second hll");

        match first {
            StarRocksHllState::Explicit(values) => {
                assert_eq!(values, [11_u64, 22_u64].into_iter().collect());
            }
            other => panic!("unexpected first HLL state: {:?}", other),
        }
        match second {
            StarRocksHllState::Explicit(values) => {
                assert_eq!(values, [33_u64].into_iter().collect());
            }
            other => panic!("unexpected second HLL state: {:?}", other),
        }
    }
}
