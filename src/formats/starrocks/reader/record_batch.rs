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
//! StarRocks native data reader orchestration layer.
//!
//! Responsibilities:
//! - Initialize projected output columns from read plan + output schema.
//! - Read segment byte ranges from object storage.
//! - Apply segment/page pruning from zone map and bloom/page index metadata.
//! - Decode selected rows for projected columns and assemble a `RecordBatch`.
//!
//! Current limitations:
//! - Segment/page pruning currently supports conjunctive `MinMaxPredicate` only.
//! - Bloom pruning is limited to EQ predicates on BLOCK_BLOOM_FILTER + MURMUR3.
//! - Complex columns still decode full segment pages when page pruning selects partial rows.

use std::collections::HashMap;
use std::io::Cursor;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, BooleanBuilder, Date32Array, Decimal128Array,
    Float32Array, Float32Builder, Float64Array, Float64Builder, Int8Array, Int8Builder, Int16Array,
    Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, StringArray, StringBuilder,
    TimestampMicrosecondArray, UInt32Array, new_empty_array, new_null_array,
};
use arrow::compute::{concat, take};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use roaring::RoaringBitmap;
use serde_json::Value as JsonValue;

use crate::connector::MinMaxPredicate;
use crate::connector::starrocks::ObjectStoreProfile;
use crate::exec::expr::LiteralValue;
use crate::formats::starrocks::plan::{
    StarRocksDeletePredicateOpPlan, StarRocksFlatJsonProjectionPlan, StarRocksNativeColumnPlan,
    StarRocksNativeReadPlan, StarRocksNativeSchemaColumnPlan, StarRocksNativeSegmentPlan,
    StarRocksTableModelPlan,
};
use crate::formats::starrocks::segment::{
    StarRocksSegmentColumnMeta, StarRocksSegmentFooter, StarRocksZoneMapMeta,
};
use crate::novarocks_logging::debug;

use super::column_decode::{
    build_column_decode_spec, decode_all_data_page_refs, decode_one_data_page,
    find_column_meta_by_unique_id,
};
use super::column_state::{OutputColumnData, OutputColumnKind};
use super::complex::decode_column_array_for_segment;
use super::constants::{
    DATE_UNIX_EPOCH_JULIAN, LOGICAL_TYPE_BIGINT, LOGICAL_TYPE_BINARY, LOGICAL_TYPE_BOOLEAN,
    LOGICAL_TYPE_CHAR, LOGICAL_TYPE_DATE, LOGICAL_TYPE_DATETIME, LOGICAL_TYPE_DECIMAL32,
    LOGICAL_TYPE_DECIMAL64, LOGICAL_TYPE_DECIMAL128, LOGICAL_TYPE_DOUBLE, LOGICAL_TYPE_FLOAT,
    LOGICAL_TYPE_INT, LOGICAL_TYPE_SMALLINT, LOGICAL_TYPE_TINYINT, LOGICAL_TYPE_VARBINARY,
    LOGICAL_TYPE_VARCHAR, USECS_PER_DAY_I64,
};
use super::indexed_column::decode_indexed_binary_values;
use super::io::{TabletRoot, build_operator, read_range_bytes};
use super::page::{DecodedDataPageValues, DecodedPageValuePayload};
use super::schema_map::{
    decimal_output_meta_from_arrow_type, expected_logical_type_from_schema_type,
    is_char_schema_type,
};

const DATE_EPOCH_DAY_OFFSET: i32 = 719_163;
const BLOOM_SEED: u64 = 1_575_457_558;
const BLOOM_ALGORITHM_BLOCK: i32 = 0;
const BLOOM_HASH_MURMUR3: i32 = 0;
const BLOOM_BLOCK_BYTES: usize = 32;
const BLOOM_SALTS: [u32; 8] = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
];

enum FlatJsonSourceArray<'a> {
    Binary(&'a BinaryArray),
    Utf8(&'a StringArray),
}

impl<'a> FlatJsonSourceArray<'a> {
    fn from_array(
        array: &'a ArrayRef,
        segment_path: &str,
        output_name: &str,
    ) -> Result<Self, String> {
        if let Some(binary) = array.as_any().downcast_ref::<BinaryArray>() {
            return Ok(Self::Binary(binary));
        }
        if let Some(utf8) = array.as_any().downcast_ref::<StringArray>() {
            return Ok(Self::Utf8(utf8));
        }
        Err(format!(
            "flat json source type mismatch: segment={}, output_column={}, source_type={:?}, expected=Binary/Utf8",
            segment_path,
            output_name,
            array.data_type()
        ))
    }

    fn len(&self) -> usize {
        match self {
            Self::Binary(array) => array.len(),
            Self::Utf8(array) => array.len(),
        }
    }

    fn json_text(&self, row: usize) -> Option<&str> {
        match self {
            Self::Binary(array) => {
                if array.is_null(row) {
                    return None;
                }
                std::str::from_utf8(array.value(row)).ok()
            }
            Self::Utf8(array) => {
                if array.is_null(row) {
                    return None;
                }
                Some(array.value(row))
            }
        }
    }
}

struct PredicateBinding<'a> {
    predicate: &'a MinMaxPredicate,
    projected: &'a StarRocksNativeColumnPlan,
    column_meta: &'a StarRocksSegmentColumnMeta,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PredicateOp {
    Le,
    Ge,
    Lt,
    Gt,
    Eq,
}

/// Build an Arrow `RecordBatch` by dispatching to model-specific readers.
pub fn build_native_record_batch(
    plan: &StarRocksNativeReadPlan,
    segment_footers: &[StarRocksSegmentFooter],
    tablet_root_path: &str,
    object_store_profile: Option<&ObjectStoreProfile>,
    output_schema: &SchemaRef,
    min_max_predicates: &[MinMaxPredicate],
) -> Result<RecordBatch, String> {
    super::model::build_record_batch_by_model(
        plan,
        segment_footers,
        tablet_root_path,
        object_store_profile,
        output_schema,
        min_max_predicates,
    )
}

/// Build an Arrow `RecordBatch` for DUP_KEYS by scanning native segment pages.
pub(super) fn build_dup_record_batch(
    plan: &StarRocksNativeReadPlan,
    segment_footers: &[StarRocksSegmentFooter],
    tablet_root_path: &str,
    object_store_profile: Option<&ObjectStoreProfile>,
    output_schema: &SchemaRef,
    min_max_predicates: &[MinMaxPredicate],
) -> Result<RecordBatch, String> {
    if plan.segments.len() != segment_footers.len() {
        return Err(format!(
            "native read plan segment mismatch: plan_segments={}, segment_footers={}",
            plan.segments.len(),
            segment_footers.len()
        ));
    }
    if plan.projected_columns.len() != output_schema.fields().len() {
        return Err(format!(
            "native read plan projected column mismatch: plan_columns={}, output_schema_fields={}",
            plan.projected_columns.len(),
            output_schema.fields().len()
        ));
    }

    let projected_by_output_index = plan
        .projected_columns
        .iter()
        .map(|c| (c.output_index, c))
        .collect::<HashMap<_, _>>();

    let mut per_output_segment_arrays = vec![Vec::<ArrayRef>::new(); output_schema.fields().len()];
    let mut total_output_rows = 0usize;
    let root = TabletRoot::parse(tablet_root_path)?;
    let op = build_operator(&root, object_store_profile)?;
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| format!("create tokio runtime for native starrocks reader failed: {e}"))?;

    for (segment_index, segment) in plan.segments.iter().enumerate() {
        let footer = segment_footers.get(segment_index).ok_or_else(|| {
            format!(
                "segment footer index out of bounds: index={}, segment_count={}",
                segment_index,
                segment_footers.len()
            )
        })?;
        let segment_rows = segment.footer_num_rows as usize;
        let predicate_bindings = bind_predicates_for_segment(
            min_max_predicates,
            &projected_by_output_index,
            footer,
            &segment.path,
        );

        if !segment_may_match(&predicate_bindings)? {
            if !predicate_bindings.is_empty() {
                debug!(
                    "native pruning: skip segment by segment zone map: segment={}, predicates={}",
                    segment.path,
                    predicate_bindings.len()
                );
            }
            continue;
        }

        let start = u64::try_from(segment.bundle_file_offset).map_err(|_| {
            format!(
                "invalid bundle offset in native read plan: segment={}, offset={}",
                segment.path, segment.bundle_file_offset
            )
        })?;
        let end = start.checked_add(segment.segment_size).ok_or_else(|| {
            format!(
                "segment range overflow in native read plan: segment={}, offset={}, segment_size={}",
                segment.path, segment.bundle_file_offset, segment.segment_size
            )
        })?;
        let segment_bytes = read_range_bytes(&rt, &op, &segment.relative_path, start, end)?;

        let selected_ranges = selected_ranges_for_segment(
            &predicate_bindings,
            &segment.path,
            &segment_bytes,
            segment_rows,
        )?;
        if selected_ranges.is_empty() {
            if !predicate_bindings.is_empty() {
                debug!(
                    "native pruning: skip segment by page indexes: segment={}, predicates={}",
                    segment.path,
                    predicate_bindings.len()
                );
            }
            continue;
        }
        if !ranges_cover_segment(&selected_ranges, segment_rows) {
            debug!(
                "native pruning: selected partial rows by page indexes: segment={}, segment_rows={}, selected_rows={}",
                segment.path,
                segment_rows,
                ranges_total_rows(&selected_ranges)
            );
        }
        let selected_rows_before_delete = ranges_total_rows(&selected_ranges);
        let full_segment_selected = ranges_cover_segment(&selected_ranges, segment_rows);
        let primary_keep_mask = build_primary_delvec_keep_mask_for_segment(
            plan,
            segment,
            &rt,
            &op,
            &segment.path,
            &selected_ranges,
            selected_rows_before_delete,
        )?;
        let delete_keep_mask = build_delete_keep_mask_for_segment(
            plan,
            segment,
            footer,
            &segment.path,
            &segment_bytes,
            segment_rows,
            &selected_ranges,
            full_segment_selected,
        )?;
        let keep_mask = merge_keep_masks(
            primary_keep_mask,
            delete_keep_mask,
            &segment.path,
            selected_rows_before_delete,
        )?;
        let selected_rows = if let Some(mask) = keep_mask.as_ref() {
            let keep_rows = mask.iter().filter(|v| **v).count();
            if keep_rows == 0 {
                continue;
            }
            keep_rows
        } else {
            selected_rows_before_delete
        };
        total_output_rows = total_output_rows
            .checked_add(selected_rows)
            .ok_or_else(|| "native output row count overflow".to_string())?;

        for projected in &plan.projected_columns {
            let output_field = output_schema.field(projected.output_index);
            let mut array = if projected.source_column_missing {
                new_null_array(output_field.data_type(), selected_rows_before_delete)
            } else {
                let column_meta = find_column_meta_by_unique_id(&footer.columns, projected.schema_unique_id)
                    .ok_or_else(|| {
                        format!(
                            "segment footer missing projected column unique_id: segment={}, unique_id={}, output_column={}",
                            segment.path, projected.schema_unique_id, projected.output_name
                        )
                    })?;
                if let Some(flat_projection) = projected.flat_json_projection.as_ref() {
                    decode_flat_json_projection_array_for_selected_rows(
                        &segment.path,
                        &segment_bytes,
                        column_meta,
                        &projected.schema,
                        flat_projection,
                        output_field.data_type(),
                        &projected.output_name,
                        &selected_ranges,
                        full_segment_selected,
                        segment_rows,
                    )?
                } else {
                    decode_column_array_for_selected_rows(
                        &segment.path,
                        &segment_bytes,
                        column_meta,
                        &projected.schema,
                        output_field.data_type(),
                        &projected.output_name,
                        &selected_ranges,
                        full_segment_selected,
                        segment_rows,
                    )?
                }
            };
            if array.len() != selected_rows_before_delete {
                return Err(format!(
                    "segment row count mismatch in output array before delete filtering: segment={}, expected_rows={}, actual_rows={}, output_column={}",
                    segment.path,
                    selected_rows_before_delete,
                    array.len(),
                    projected.output_name
                ));
            }
            if let Some(mask) = keep_mask.as_ref() {
                array =
                    take_array_by_keep_mask(&array, mask, &segment.path, &projected.output_name)?;
            }
            if array.len() != selected_rows {
                return Err(format!(
                    "segment row count mismatch in output array after delete filtering: segment={}, expected_rows={}, actual_rows={}, output_column={}",
                    segment.path,
                    selected_rows,
                    array.len(),
                    projected.output_name
                ));
            };
            per_output_segment_arrays[projected.output_index].push(array);
        }
    }

    let expected_rows = total_output_rows;
    let arrays = per_output_segment_arrays
        .into_iter()
        .enumerate()
        .map(|(idx, segment_arrays)| {
            let field = output_schema.field(idx);
            if segment_arrays.is_empty() {
                if expected_rows == 0 {
                    return Ok(new_empty_array(field.data_type()));
                }
                return Err(format!(
                    "native column array missing after scan: output_index={}, field_name={}",
                    idx,
                    field.name()
                ));
            }
            let merged = if segment_arrays.len() == 1 {
                segment_arrays[0].clone()
            } else {
                let refs: Vec<&dyn Array> = segment_arrays.iter().map(|a| a.as_ref()).collect();
                concat(&refs).map_err(|e| {
                    format!(
                        "concat segment arrays failed in native reader: output_index={}, field_name={}, error={}",
                        idx,
                        field.name(),
                        e
                    )
                })?
            };
            if merged.len() != expected_rows {
                return Err(format!(
                    "native column row count mismatch after scan: output_index={}, field_name={}, expected_rows={}, actual_rows={}",
                    idx,
                    field.name(),
                    expected_rows,
                    merged.len()
                ));
            }
            Ok(merged)
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(output_schema.clone(), arrays)
        .map_err(|e| format!("build native starrocks record batch failed: {e}"))
}

fn bind_predicates_for_segment<'a>(
    predicates: &'a [MinMaxPredicate],
    projected_by_output_index: &HashMap<usize, &'a StarRocksNativeColumnPlan>,
    footer: &'a StarRocksSegmentFooter,
    segment_path: &str,
) -> Vec<PredicateBinding<'a>> {
    let mut out = Vec::new();
    for predicate in predicates {
        let Some(output_index) = predicate_column_index(predicate) else {
            continue;
        };
        let Some(projected) = projected_by_output_index.get(&output_index).copied() else {
            continue;
        };
        if projected.flat_json_projection.is_some() {
            // Flat JSON rewritten slots are evaluated after JSON extraction, not from
            // on-disk scalar zone map/indexes.
            continue;
        }
        if projected.source_column_missing {
            continue;
        }
        let Some(column_meta) =
            find_column_meta_by_unique_id(&footer.columns, projected.schema_unique_id)
        else {
            continue;
        };
        if column_meta.logical_type.is_none() {
            continue;
        }
        out.push(PredicateBinding {
            predicate,
            projected,
            column_meta,
        });
    }

    if out.is_empty() {
        let _ = segment_path;
    }
    out
}

fn segment_may_match(bindings: &[PredicateBinding<'_>]) -> Result<bool, String> {
    for binding in bindings {
        let Some(zone_map) = binding.column_meta.segment_zone_map.as_ref() else {
            continue;
        };
        if !zone_map_may_satisfy(binding, zone_map)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn selected_ranges_for_segment(
    bindings: &[PredicateBinding<'_>],
    segment_path: &str,
    segment_bytes: &[u8],
    segment_rows: usize,
) -> Result<Vec<Range<usize>>, String> {
    if segment_rows == 0 {
        return Ok(Vec::new());
    }

    let mut ranges = vec![0..segment_rows];
    let mut used_page_pruning = false;

    for binding in bindings {
        let Some(page_ranges) =
            page_ranges_for_predicate(binding, segment_path, segment_bytes, segment_rows)?
        else {
            continue;
        };
        used_page_pruning = true;
        ranges = intersect_ranges(&ranges, &page_ranges);
        if ranges.is_empty() {
            break;
        }
    }

    if !used_page_pruning {
        return Ok(vec![0..segment_rows]);
    }

    Ok(merge_ranges(ranges))
}

fn decode_column_array_for_selected_rows(
    segment_path: &str,
    segment_bytes: &[u8],
    column_meta: &StarRocksSegmentColumnMeta,
    schema: &StarRocksNativeSchemaColumnPlan,
    output_data_type: &DataType,
    output_name: &str,
    selected_ranges: &[Range<usize>],
    full_segment_selected: bool,
    segment_rows: usize,
) -> Result<ArrayRef, String> {
    if full_segment_selected {
        return decode_column_array_for_segment(
            segment_path,
            segment_bytes,
            column_meta,
            schema,
            output_data_type,
            output_name,
            segment_rows,
        );
    }
    if OutputColumnKind::from_arrow_type(output_data_type).is_some() {
        return decode_scalar_array_for_ranges(
            segment_path,
            segment_bytes,
            column_meta,
            schema,
            output_data_type,
            output_name,
            selected_ranges,
            segment_rows,
        );
    }
    let full_array = decode_column_array_for_segment(
        segment_path,
        segment_bytes,
        column_meta,
        schema,
        output_data_type,
        output_name,
        segment_rows,
    )?;
    take_array_by_ranges(&full_array, selected_ranges, segment_path, output_name)
}

fn decode_flat_json_projection_array_for_selected_rows(
    segment_path: &str,
    segment_bytes: &[u8],
    column_meta: &StarRocksSegmentColumnMeta,
    schema: &StarRocksNativeSchemaColumnPlan,
    projection: &StarRocksFlatJsonProjectionPlan,
    output_data_type: &DataType,
    output_name: &str,
    selected_ranges: &[Range<usize>],
    full_segment_selected: bool,
    segment_rows: usize,
) -> Result<ArrayRef, String> {
    let source_array = decode_column_array_for_selected_rows(
        segment_path,
        segment_bytes,
        column_meta,
        schema,
        &DataType::Binary,
        output_name,
        selected_ranges,
        full_segment_selected,
        segment_rows,
    )?;
    let source = FlatJsonSourceArray::from_array(&source_array, segment_path, output_name)?;
    project_flat_json_array(
        &source,
        projection,
        output_data_type,
        segment_path,
        output_name,
    )
}

fn project_flat_json_array(
    source: &FlatJsonSourceArray<'_>,
    projection: &StarRocksFlatJsonProjectionPlan,
    output_data_type: &DataType,
    segment_path: &str,
    output_name: &str,
) -> Result<ArrayRef, String> {
    match output_data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for row in 0..source.len() {
                match extract_flat_json_path_value(source, row, projection)
                    .and_then(|v| json_value_to_flat_bool(&v))
                {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int8 => {
            let mut builder = Int8Builder::new();
            for row in 0..source.len() {
                match extract_flat_json_path_value(source, row, projection)
                    .and_then(|v| json_value_to_flat_i64(&v))
                    .and_then(|v| i8::try_from(v).ok())
                {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::new();
            for row in 0..source.len() {
                match extract_flat_json_path_value(source, row, projection)
                    .and_then(|v| json_value_to_flat_i64(&v))
                    .and_then(|v| i16::try_from(v).ok())
                {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::new();
            for row in 0..source.len() {
                match extract_flat_json_path_value(source, row, projection)
                    .and_then(|v| json_value_to_flat_i64(&v))
                    .and_then(|v| i32::try_from(v).ok())
                {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::new();
            for row in 0..source.len() {
                match extract_flat_json_path_value(source, row, projection)
                    .and_then(|v| json_value_to_flat_i64(&v))
                {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::new();
            for row in 0..source.len() {
                match extract_flat_json_path_value(source, row, projection)
                    .and_then(|v| json_value_to_flat_f64(&v))
                    .map(|v| v as f32)
                {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for row in 0..source.len() {
                match extract_flat_json_path_value(source, row, projection)
                    .and_then(|v| json_value_to_flat_f64(&v))
                {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for row in 0..source.len() {
                match extract_flat_json_path_value(source, row, projection)
                    .and_then(|v| json_value_to_flat_string(&v))
                {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        other => Err(format!(
            "unsupported output type for flat json projection: segment={}, output_column={}, output_type={:?}",
            segment_path, output_name, other
        )),
    }
}

fn extract_flat_json_path_value(
    source: &FlatJsonSourceArray<'_>,
    row: usize,
    projection: &StarRocksFlatJsonProjectionPlan,
) -> Option<JsonValue> {
    let raw = source.json_text(row)?;
    let root = serde_json::from_str::<JsonValue>(raw).ok()?;
    let mut current = &root;
    for key in &projection.path {
        current = match current {
            JsonValue::Object(map) => map.get(key)?,
            _ => return None,
        };
    }
    Some(current.clone())
}

fn json_value_to_flat_bool(value: &JsonValue) -> Option<bool> {
    if value.is_null() {
        return None;
    }
    if let Some(v) = value.as_bool() {
        return Some(v);
    }
    if let Some(v) = value.as_i64() {
        return Some(v != 0);
    }
    if let Some(v) = value.as_u64() {
        return Some(v != 0);
    }
    if let Some(v) = value.as_f64() {
        return Some(v != 0.0);
    }
    if let Some(v) = value.as_str() {
        return parse_bool_like_string(v);
    }
    None
}

fn json_value_to_flat_i64(value: &JsonValue) -> Option<i64> {
    if value.is_null() {
        return None;
    }
    if let Some(v) = value.as_bool() {
        return Some(if v { 1 } else { 0 });
    }
    if let Some(v) = value.as_i64() {
        return Some(v);
    }
    if let Some(v) = value.as_u64() {
        return i64::try_from(v).ok();
    }
    if let Some(v) = value.as_f64() {
        if v.is_finite() {
            return Some(v as i64);
        }
        return None;
    }
    if let Some(v) = value.as_str() {
        return v.trim().parse::<i64>().ok();
    }
    None
}

fn json_value_to_flat_f64(value: &JsonValue) -> Option<f64> {
    if value.is_null() {
        return None;
    }
    if let Some(v) = value.as_bool() {
        return Some(if v { 1.0 } else { 0.0 });
    }
    if let Some(v) = value.as_f64() {
        return Some(v);
    }
    if let Some(v) = value.as_i64() {
        return Some(v as f64);
    }
    if let Some(v) = value.as_u64() {
        return Some(v as f64);
    }
    if let Some(v) = value.as_str() {
        return v.trim().parse::<f64>().ok();
    }
    None
}

fn json_value_to_flat_string(value: &JsonValue) -> Option<String> {
    if value.is_null() {
        return None;
    }
    if let Some(v) = value.as_str() {
        return Some(v.to_string());
    }
    json_value_to_query_string(value)
}

fn parse_bool_like_string(value: &str) -> Option<bool> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(parsed) = trimmed.parse::<i32>() {
        return Some(parsed != 0);
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Some(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Some(false);
    }
    None
}

fn format_json_number(value: &serde_json::Number) -> String {
    if let Some(v) = value.as_i64() {
        return v.to_string();
    }
    if let Some(v) = value.as_u64() {
        return v.to_string();
    }
    if let Some(v) = value.as_f64() {
        if v.is_finite() && v.fract() == 0.0 {
            let as_i128 = v as i128;
            if as_i128 >= i64::MIN as i128 && as_i128 <= i64::MAX as i128 {
                return (as_i128 as i64).to_string();
            }
            if as_i128 >= 0 && as_i128 <= u64::MAX as i128 {
                return (as_i128 as u64).to_string();
            }
        }
        return v.to_string();
    }
    value.to_string()
}

fn format_json_query_value(value: &JsonValue, out: &mut String) -> Result<(), String> {
    match value {
        JsonValue::Null => out.push_str("null"),
        JsonValue::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
        JsonValue::Number(v) => out.push_str(&format_json_number(v)),
        JsonValue::String(v) => {
            let escaped = serde_json::to_string(v).map_err(|e| e.to_string())?;
            out.push_str(&escaped);
        }
        JsonValue::Array(items) => {
            out.push('[');
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                format_json_query_value(item, out)?;
            }
            out.push(']');
        }
        JsonValue::Object(map) => {
            out.push('{');
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (idx, key) in keys.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                let escaped_key = serde_json::to_string(key).map_err(|e| e.to_string())?;
                out.push_str(&escaped_key);
                out.push_str(": ");
                let child = map
                    .get(*key)
                    .ok_or_else(|| "flat json formatter missing object key".to_string())?;
                format_json_query_value(child, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn json_value_to_query_string(value: &JsonValue) -> Option<String> {
    if value.is_null() {
        return None;
    }
    let mut out = String::new();
    format_json_query_value(value, &mut out).ok()?;
    Some(out)
}

fn build_primary_delvec_keep_mask_for_segment(
    plan: &StarRocksNativeReadPlan,
    segment: &StarRocksNativeSegmentPlan,
    rt: &tokio::runtime::Runtime,
    op: &opendal::Operator,
    segment_path: &str,
    selected_ranges: &[Range<usize>],
    selected_rows: usize,
) -> Result<Option<Vec<bool>>, String> {
    if plan.table_model != StarRocksTableModelPlan::PrimaryKeys {
        return Ok(None);
    }
    if selected_rows == 0 {
        return Ok(None);
    }
    let bitmap = load_primary_delvec_bitmap_for_segment(plan, segment, rt, op, segment_path)?;
    let Some(bitmap) = bitmap else {
        return Ok(None);
    };

    let mut keep = Vec::with_capacity(selected_rows);
    let mut any_deleted = false;
    for range in selected_ranges {
        for row in range.start..range.end {
            let row_u32 = u32::try_from(row).map_err(|_| {
                format!(
                    "row id overflow while applying primary delvec: segment={}, row_id={}",
                    segment_path, row
                )
            })?;
            let is_deleted = bitmap.contains(row_u32);
            if is_deleted {
                any_deleted = true;
            }
            keep.push(!is_deleted);
        }
    }
    if !any_deleted {
        return Ok(None);
    }
    Ok(Some(keep))
}

fn load_primary_delvec_bitmap_for_segment(
    plan: &StarRocksNativeReadPlan,
    segment: &StarRocksNativeSegmentPlan,
    rt: &tokio::runtime::Runtime,
    op: &opendal::Operator,
    segment_path: &str,
) -> Result<Option<RoaringBitmap>, String> {
    let primary_delvec = plan.primary_delvec.as_ref().ok_or_else(|| {
        format!(
            "missing primary delvec metadata in native read plan: tablet_id={}, version={}",
            plan.tablet_id, plan.version
        )
    })?;
    let segment_id = segment.segment_id.ok_or_else(|| {
        format!(
            "missing segment_id in native read plan for primary key table: segment={}",
            segment_path
        )
    })?;
    let Some(page) = primary_delvec.segment_delvec_pages.get(&segment_id) else {
        return Ok(None);
    };
    if page.size == 0 {
        return Ok(None);
    }
    let rel_path = primary_delvec
        .version_to_file_rel_path
        .get(&page.version)
        .ok_or_else(|| {
            format!(
                "missing primary delvec file mapping for version {}: segment={}",
                page.version, segment_path
            )
        })?;
    let end = page.offset.checked_add(page.size).ok_or_else(|| {
        format!(
            "primary delvec read range overflow: segment={}, offset={}, size={}",
            segment_path, page.offset, page.size
        )
    })?;
    let bytes = read_range_bytes(rt, op, rel_path, page.offset, end)?;
    if let Some(expected_masked) = page.crc32c {
        if page.crc32c_gen_version == Some(page.version) {
            let expected = crc32c_unmask(expected_masked);
            let actual = crc32c::crc32c(&bytes);
            if expected != actual {
                return Err(format!(
                    "primary delvec crc32c mismatch: segment={}, delvec_version={}, expected={}, actual={}",
                    segment_path, page.version, expected, actual
                ));
            }
        }
    }
    decode_delvec_bitmap(&bytes, segment_path, page.version).map(Some)
}

fn decode_delvec_bitmap(
    bytes: &[u8],
    segment_path: &str,
    delvec_version: i64,
) -> Result<RoaringBitmap, String> {
    if bytes.is_empty() {
        return Err(format!(
            "invalid primary delvec payload (empty): segment={}, delvec_version={}",
            segment_path, delvec_version
        ));
    }
    if bytes[0] != 0x01 {
        return Err(format!(
            "invalid primary delvec format flag: segment={}, delvec_version={}, flag={}",
            segment_path, delvec_version, bytes[0]
        ));
    }
    if bytes.len() == 1 {
        return Ok(RoaringBitmap::new());
    }
    let mut cursor = Cursor::new(&bytes[1..]);
    RoaringBitmap::deserialize_from(&mut cursor).map_err(|e| {
        format!(
            "decode primary delvec roaring bitmap failed: segment={}, delvec_version={}, error={}",
            segment_path, delvec_version, e
        )
    })
}

fn crc32c_unmask(masked_crc: u32) -> u32 {
    const MASK_DELTA: u32 = 0xa282_ead8;
    let rot = masked_crc.wrapping_sub(MASK_DELTA);
    rot.rotate_right(17)
}

fn merge_keep_masks(
    left: Option<Vec<bool>>,
    right: Option<Vec<bool>>,
    segment_path: &str,
    expected_len: usize,
) -> Result<Option<Vec<bool>>, String> {
    match (left, right) {
        (None, None) => Ok(None),
        (Some(mask), None) | (None, Some(mask)) => {
            if mask.len() != expected_len {
                return Err(format!(
                    "keep-mask length mismatch: segment={}, expected_rows={}, actual_rows={}",
                    segment_path,
                    expected_len,
                    mask.len()
                ));
            }
            Ok(Some(mask))
        }
        (Some(left_mask), Some(right_mask)) => {
            if left_mask.len() != expected_len || right_mask.len() != expected_len {
                return Err(format!(
                    "keep-mask length mismatch while merging: segment={}, expected_rows={}, left_rows={}, right_rows={}",
                    segment_path,
                    expected_len,
                    left_mask.len(),
                    right_mask.len()
                ));
            }
            let mut merged = Vec::with_capacity(expected_len);
            for (l, r) in left_mask.into_iter().zip(right_mask.into_iter()) {
                merged.push(l && r);
            }
            Ok(Some(merged))
        }
    }
}

fn build_delete_keep_mask_for_segment(
    plan: &StarRocksNativeReadPlan,
    segment: &StarRocksNativeSegmentPlan,
    footer: &StarRocksSegmentFooter,
    segment_path: &str,
    segment_bytes: &[u8],
    segment_rows: usize,
    selected_ranges: &[Range<usize>],
    full_segment_selected: bool,
) -> Result<Option<Vec<bool>>, String> {
    if plan.delete_predicates.is_empty() {
        return Ok(None);
    }
    if segment.rowset_version < 0 {
        return Err(format!(
            "invalid rowset version in native read plan segment: segment={}, rowset_version={}",
            segment_path, segment.rowset_version
        ));
    }

    let applicable = plan
        .delete_predicates
        .iter()
        .filter(|pred| pred.version >= segment.rowset_version)
        .collect::<Vec<_>>();
    if applicable.is_empty() {
        return Ok(None);
    }

    let selected_rows = ranges_total_rows(selected_ranges);
    if selected_rows == 0 {
        return Ok(None);
    }

    let mut decoded_by_unique_id = HashMap::<u32, ArrayRef>::new();
    for group in &applicable {
        for term in &group.terms {
            if decoded_by_unique_id.contains_key(&term.schema_unique_id) {
                continue;
            }
            let column_meta =
                find_column_meta_by_unique_id(&footer.columns, term.schema_unique_id).ok_or_else(|| {
                    format!(
                        "segment footer missing delete predicate column unique_id: segment={}, unique_id={}, column_name={}",
                        segment_path, term.schema_unique_id, term.column_name
                    )
                })?;
            let output_data_type = delete_predicate_output_data_type(term)?;
            let schema = StarRocksNativeSchemaColumnPlan {
                unique_id: Some(term.schema_unique_id),
                schema_type: term.schema_type.clone(),
                is_nullable: true,
                is_key: true,
                aggregation: None,
                precision: term.precision,
                scale: term.scale,
                children: Vec::new(),
            };
            let array = decode_column_array_for_selected_rows(
                segment_path,
                segment_bytes,
                column_meta,
                &schema,
                &output_data_type,
                &format!("__delete_predicate_col_{}", term.column_name),
                selected_ranges,
                full_segment_selected,
                segment_rows,
            )?;
            if array.len() != selected_rows {
                return Err(format!(
                    "delete predicate column row count mismatch after decode: segment={}, column_name={}, expected_rows={}, actual_rows={}",
                    segment_path,
                    term.column_name,
                    selected_rows,
                    array.len()
                ));
            }
            decoded_by_unique_id.insert(term.schema_unique_id, array);
        }
    }

    let mut deleted = vec![false; selected_rows];
    let mut any_deleted = false;
    for group in applicable {
        let mut group_mask = vec![true; selected_rows];
        for term in &group.terms {
            let array = decoded_by_unique_id.get(&term.schema_unique_id).ok_or_else(|| {
                format!(
                    "decoded delete predicate column not found: segment={}, unique_id={}, column_name={}",
                    segment_path, term.schema_unique_id, term.column_name
                )
            })?;
            apply_delete_term_to_mask(array.as_ref(), term, &mut group_mask, segment_path)?;
            if !group_mask.iter().any(|v| *v) {
                break;
            }
        }
        for (idx, matched) in group_mask.iter().enumerate() {
            if *matched && !deleted[idx] {
                deleted[idx] = true;
                any_deleted = true;
            }
        }
        if deleted.iter().all(|v| *v) {
            break;
        }
    }
    if !any_deleted {
        return Ok(None);
    }

    Ok(Some(deleted.into_iter().map(|v| !v).collect()))
}

fn delete_predicate_output_data_type(
    term: &crate::formats::starrocks::plan::StarRocksDeletePredicateTermPlan,
) -> Result<DataType, String> {
    match term.schema_type.trim().to_ascii_uppercase().as_str() {
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
            let precision = term.precision.ok_or_else(|| {
                format!(
                    "missing decimal precision in delete predicate term: column_name={}, schema_type={}",
                    term.column_name, term.schema_type
                )
            })?;
            let scale = term.scale.ok_or_else(|| {
                format!(
                    "missing decimal scale in delete predicate term: column_name={}, schema_type={}",
                    term.column_name, term.schema_type
                )
            })?;
            Ok(DataType::Decimal128(precision, scale))
        }
        other => Err(format!(
            "unsupported delete predicate schema type in reader: column_name={}, schema_type={}",
            term.column_name, other
        )),
    }
}

fn apply_delete_term_to_mask(
    array: &dyn Array,
    term: &crate::formats::starrocks::plan::StarRocksDeletePredicateTermPlan,
    group_mask: &mut [bool],
    segment_path: &str,
) -> Result<(), String> {
    let op = term.op;
    let schema_type = term.schema_type.trim().to_ascii_uppercase();

    match schema_type.as_str() {
        "TINYINT" => {
            let values = parse_delete_term_int_values::<i8>(term)?;
            let typed = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                format!(
                    "delete predicate array type mismatch: segment={}, column_name={}, expected=Int8",
                    segment_path, term.column_name
                )
            })?;
            apply_delete_term_mask_scalar(&typed, op, &values, group_mask);
        }
        "SMALLINT" => {
            let values = parse_delete_term_int_values::<i16>(term)?;
            let typed = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                format!(
                    "delete predicate array type mismatch: segment={}, column_name={}, expected=Int16",
                    segment_path, term.column_name
                )
            })?;
            apply_delete_term_mask_scalar(&typed, op, &values, group_mask);
        }
        "INT" => {
            let values = parse_delete_term_int_values::<i32>(term)?;
            let typed = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                format!(
                    "delete predicate array type mismatch: segment={}, column_name={}, expected=Int32",
                    segment_path, term.column_name
                )
            })?;
            apply_delete_term_mask_scalar(&typed, op, &values, group_mask);
        }
        "BIGINT" => {
            let values = parse_delete_term_int_values::<i64>(term)?;
            let typed = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                format!(
                    "delete predicate array type mismatch: segment={}, column_name={}, expected=Int64",
                    segment_path, term.column_name
                )
            })?;
            apply_delete_term_mask_scalar(&typed, op, &values, group_mask);
        }
        "FLOAT" => {
            let values = parse_delete_term_float_values::<f32>(term)?;
            let typed = array.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                format!(
                    "delete predicate array type mismatch: segment={}, column_name={}, expected=Float32",
                    segment_path, term.column_name
                )
            })?;
            apply_delete_term_mask_scalar(&typed, op, &values, group_mask);
        }
        "DOUBLE" => {
            let values = parse_delete_term_float_values::<f64>(term)?;
            let typed = array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                format!(
                    "delete predicate array type mismatch: segment={}, column_name={}, expected=Float64",
                    segment_path, term.column_name
                )
            })?;
            apply_delete_term_mask_scalar(&typed, op, &values, group_mask);
        }
        "BOOLEAN" => {
            let values = parse_delete_term_bool_values(term)?;
            let typed = array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                format!(
                    "delete predicate array type mismatch: segment={}, column_name={}, expected=Boolean",
                    segment_path, term.column_name
                )
            })?;
            apply_delete_term_mask_boolean(typed, op, &values, group_mask);
        }
        "DATE" => {
            let values = parse_delete_term_date_values(term)?;
            let typed = array.as_any().downcast_ref::<Date32Array>().ok_or_else(|| {
                format!(
                    "delete predicate array type mismatch: segment={}, column_name={}, expected=Date32",
                    segment_path, term.column_name
                )
            })?;
            apply_delete_term_mask_scalar(&typed, op, &values, group_mask);
        }
        "DATETIME" => {
            let values = parse_delete_term_datetime_values(term)?;
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    format!(
                        "delete predicate array type mismatch: segment={}, column_name={}, expected=TimestampMicrosecond",
                        segment_path, term.column_name
                    )
                })?;
            apply_delete_term_mask_scalar(&typed, op, &values, group_mask);
        }
        "CHAR" | "VARCHAR" => {
            let values = parse_delete_term_utf8_values(term);
            let typed = array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                format!(
                    "delete predicate array type mismatch: segment={}, column_name={}, expected=Utf8",
                    segment_path, term.column_name
                )
            })?;
            apply_delete_term_mask_utf8(typed, op, &values, group_mask);
        }
        "BINARY" | "VARBINARY" => {
            let values = parse_delete_term_binary_values(term);
            let typed = array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                format!(
                    "delete predicate array type mismatch: segment={}, column_name={}, expected=Binary",
                    segment_path, term.column_name
                )
            })?;
            apply_delete_term_mask_binary(typed, op, &values, group_mask);
        }
        "DECIMAL32" | "DECIMAL64" | "DECIMAL128" => {
            let values = parse_delete_term_decimal_values(term)?;
            let typed = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    format!(
                        "delete predicate array type mismatch: segment={}, column_name={}, expected=Decimal128",
                        segment_path, term.column_name
                    )
                })?;
            apply_delete_term_mask_decimal(typed, op, &values, group_mask);
        }
        _ => {
            return Err(format!(
                "unsupported delete predicate schema type in reader evaluation: segment={}, column_name={}, schema_type={}",
                segment_path, term.column_name, term.schema_type
            ));
        }
    }
    Ok(())
}

trait IntCast: Copy + PartialEq + PartialOrd + std::str::FromStr + Sized {
    fn try_from_i64(v: i64) -> Option<Self>;
}

impl IntCast for i8 {
    fn try_from_i64(v: i64) -> Option<Self> {
        i8::try_from(v).ok()
    }
}

impl IntCast for i16 {
    fn try_from_i64(v: i64) -> Option<Self> {
        i16::try_from(v).ok()
    }
}

impl IntCast for i32 {
    fn try_from_i64(v: i64) -> Option<Self> {
        i32::try_from(v).ok()
    }
}

impl IntCast for i64 {
    fn try_from_i64(v: i64) -> Option<Self> {
        Some(v)
    }
}

trait FloatCast: Copy + PartialEq + PartialOrd + std::str::FromStr + Sized {
    fn from_f64(v: f64) -> Option<Self>;
}

impl FloatCast for f32 {
    fn from_f64(v: f64) -> Option<Self> {
        if v.is_finite() && v >= f32::MIN as f64 && v <= f32::MAX as f64 {
            Some(v as f32)
        } else if v.is_nan() {
            Some(f32::NAN)
        } else {
            None
        }
    }
}

impl FloatCast for f64 {
    fn from_f64(v: f64) -> Option<Self> {
        Some(v)
    }
}

fn parse_delete_term_int_values<T: IntCast>(
    term: &crate::formats::starrocks::plan::StarRocksDeletePredicateTermPlan,
) -> Result<Vec<T>, String> {
    term.values
        .iter()
        .map(|raw| {
            let text = normalize_delete_value(raw);
            let v = text.parse::<i64>().map_err(|_| {
                format!(
                    "parse delete predicate integer literal failed: column_name={}, schema_type={}, value={}",
                    term.column_name, term.schema_type, raw
                )
            })?;
            T::try_from_i64(v).ok_or_else(|| {
                format!(
                    "delete predicate integer literal overflow: column_name={}, schema_type={}, value={}",
                    term.column_name, term.schema_type, raw
                )
            })
        })
        .collect()
}

fn parse_delete_term_float_values<T: FloatCast>(
    term: &crate::formats::starrocks::plan::StarRocksDeletePredicateTermPlan,
) -> Result<Vec<T>, String> {
    term.values
        .iter()
        .map(|raw| {
            let text = normalize_delete_value(raw);
            let v = text.parse::<f64>().map_err(|_| {
                format!(
                    "parse delete predicate float literal failed: column_name={}, schema_type={}, value={}",
                    term.column_name, term.schema_type, raw
                )
            })?;
            T::from_f64(v).ok_or_else(|| {
                format!(
                    "delete predicate float literal out of range: column_name={}, schema_type={}, value={}",
                    term.column_name, term.schema_type, raw
                )
            })
        })
        .collect()
}

fn parse_delete_term_bool_values(
    term: &crate::formats::starrocks::plan::StarRocksDeletePredicateTermPlan,
) -> Result<Vec<bool>, String> {
    term.values
        .iter()
        .map(|raw| {
            let text = normalize_delete_value(raw);
            match text.to_ascii_lowercase().as_str() {
                "1" | "true" => Ok(true),
                "0" | "false" => Ok(false),
                _ => Err(format!(
                    "parse delete predicate bool literal failed: column_name={}, schema_type={}, value={}",
                    term.column_name, term.schema_type, raw
                )),
            }
        })
        .collect()
}

fn parse_delete_term_date_values(
    term: &crate::formats::starrocks::plan::StarRocksDeletePredicateTermPlan,
) -> Result<Vec<i32>, String> {
    term.values
        .iter()
        .map(|raw| {
            let text = normalize_delete_value(raw);
            parse_date_days(text.as_bytes()).ok_or_else(|| {
                format!(
                    "parse delete predicate date literal failed: column_name={}, schema_type={}, value={}",
                    term.column_name, term.schema_type, raw
                )
            })
        })
        .collect()
}

fn parse_delete_term_datetime_values(
    term: &crate::formats::starrocks::plan::StarRocksDeletePredicateTermPlan,
) -> Result<Vec<i64>, String> {
    term.values
        .iter()
        .map(|raw| {
            let text = normalize_delete_value(raw);
            parse_datetime_micros(text.as_bytes()).ok_or_else(|| {
                format!(
                    "parse delete predicate datetime literal failed: column_name={}, schema_type={}, value={}",
                    term.column_name, term.schema_type, raw
                )
            })
        })
        .collect()
}

fn parse_delete_term_utf8_values(
    term: &crate::formats::starrocks::plan::StarRocksDeletePredicateTermPlan,
) -> Vec<String> {
    term.values
        .iter()
        .map(|v| normalize_delete_value(v))
        .collect()
}

fn parse_delete_term_binary_values(
    term: &crate::formats::starrocks::plan::StarRocksDeletePredicateTermPlan,
) -> Vec<Vec<u8>> {
    term.values
        .iter()
        .map(|v| normalize_delete_value(v).into_bytes())
        .collect()
}

fn parse_delete_term_decimal_values(
    term: &crate::formats::starrocks::plan::StarRocksDeletePredicateTermPlan,
) -> Result<Vec<i128>, String> {
    let precision = term.precision.ok_or_else(|| {
        format!(
            "missing decimal precision in delete predicate: column_name={}, schema_type={}",
            term.column_name, term.schema_type
        )
    })?;
    let scale = term.scale.ok_or_else(|| {
        format!(
            "missing decimal scale in delete predicate: column_name={}, schema_type={}",
            term.column_name, term.schema_type
        )
    })?;
    term.values
        .iter()
        .map(|raw| {
            let text = normalize_delete_value(raw);
            parse_decimal_scaled(&text, precision, scale).map_err(|e| {
                format!(
                    "parse delete predicate decimal literal failed: column_name={}, schema_type={}, value={}, error={}",
                    term.column_name, term.schema_type, raw, e
                )
            })
        })
        .collect()
}

fn normalize_delete_value(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}

fn apply_delete_term_mask_scalar<T, A>(
    array: &A,
    op: StarRocksDeletePredicateOpPlan,
    values: &[T],
    group_mask: &mut [bool],
) where
    T: Copy + PartialEq + PartialOrd,
    A: Array + arrow::array::ArrayAccessor<Item = T>,
{
    for (idx, keep) in group_mask.iter_mut().enumerate() {
        if !*keep {
            continue;
        }
        let matched = if array.is_null(idx) {
            match op {
                StarRocksDeletePredicateOpPlan::IsNull => true,
                StarRocksDeletePredicateOpPlan::IsNotNull => false,
                _ => false,
            }
        } else {
            let value = array.value(idx);
            eval_delete_term_scalar_value(op, value, values)
        };
        if !matched {
            *keep = false;
        }
    }
}

fn apply_delete_term_mask_boolean(
    array: &BooleanArray,
    op: StarRocksDeletePredicateOpPlan,
    values: &[bool],
    group_mask: &mut [bool],
) {
    for (idx, keep) in group_mask.iter_mut().enumerate() {
        if !*keep {
            continue;
        }
        let matched = if array.is_null(idx) {
            match op {
                StarRocksDeletePredicateOpPlan::IsNull => true,
                StarRocksDeletePredicateOpPlan::IsNotNull => false,
                _ => false,
            }
        } else {
            let value = array.value(idx);
            eval_delete_term_scalar_value(op, value, values)
        };
        if !matched {
            *keep = false;
        }
    }
}

fn apply_delete_term_mask_utf8(
    array: &StringArray,
    op: StarRocksDeletePredicateOpPlan,
    values: &[String],
    group_mask: &mut [bool],
) {
    let value_bytes = values
        .iter()
        .map(|v| v.as_bytes().to_vec())
        .collect::<Vec<_>>();
    for (idx, keep) in group_mask.iter_mut().enumerate() {
        if !*keep {
            continue;
        }
        let matched = if array.is_null(idx) {
            match op {
                StarRocksDeletePredicateOpPlan::IsNull => true,
                StarRocksDeletePredicateOpPlan::IsNotNull => false,
                _ => false,
            }
        } else {
            let value = array.value(idx);
            eval_delete_term_bytes_value(op, value.as_bytes(), &value_bytes)
        };
        if !matched {
            *keep = false;
        }
    }
}

fn apply_delete_term_mask_binary(
    array: &BinaryArray,
    op: StarRocksDeletePredicateOpPlan,
    values: &[Vec<u8>],
    group_mask: &mut [bool],
) {
    for (idx, keep) in group_mask.iter_mut().enumerate() {
        if !*keep {
            continue;
        }
        let matched = if array.is_null(idx) {
            match op {
                StarRocksDeletePredicateOpPlan::IsNull => true,
                StarRocksDeletePredicateOpPlan::IsNotNull => false,
                _ => false,
            }
        } else {
            eval_delete_term_bytes_value(op, array.value(idx), values)
        };
        if !matched {
            *keep = false;
        }
    }
}

fn apply_delete_term_mask_decimal(
    array: &Decimal128Array,
    op: StarRocksDeletePredicateOpPlan,
    values: &[i128],
    group_mask: &mut [bool],
) {
    for (idx, keep) in group_mask.iter_mut().enumerate() {
        if !*keep {
            continue;
        }
        let matched = if array.is_null(idx) {
            match op {
                StarRocksDeletePredicateOpPlan::IsNull => true,
                StarRocksDeletePredicateOpPlan::IsNotNull => false,
                _ => false,
            }
        } else {
            eval_delete_term_scalar_value(op, array.value(idx), values)
        };
        if !matched {
            *keep = false;
        }
    }
}

fn eval_delete_term_scalar_value<T: Copy + PartialEq + PartialOrd>(
    op: StarRocksDeletePredicateOpPlan,
    value: T,
    values: &[T],
) -> bool {
    match op {
        StarRocksDeletePredicateOpPlan::Eq => values.first().copied() == Some(value),
        StarRocksDeletePredicateOpPlan::Ne => values.first().copied().is_some_and(|v| value != v),
        StarRocksDeletePredicateOpPlan::Lt => values.first().copied().is_some_and(|v| value < v),
        StarRocksDeletePredicateOpPlan::Le => values.first().copied().is_some_and(|v| value <= v),
        StarRocksDeletePredicateOpPlan::Gt => values.first().copied().is_some_and(|v| value > v),
        StarRocksDeletePredicateOpPlan::Ge => values.first().copied().is_some_and(|v| value >= v),
        StarRocksDeletePredicateOpPlan::In => values.contains(&value),
        StarRocksDeletePredicateOpPlan::NotIn => !values.contains(&value),
        StarRocksDeletePredicateOpPlan::IsNull => false,
        StarRocksDeletePredicateOpPlan::IsNotNull => true,
    }
}

fn eval_delete_term_bytes_value(
    op: StarRocksDeletePredicateOpPlan,
    value: &[u8],
    values: &[Vec<u8>],
) -> bool {
    let first = values.first().map(Vec::as_slice);
    match op {
        StarRocksDeletePredicateOpPlan::Eq => first == Some(value),
        StarRocksDeletePredicateOpPlan::Ne => first.is_some_and(|v| value != v),
        StarRocksDeletePredicateOpPlan::Lt => first.is_some_and(|v| value < v),
        StarRocksDeletePredicateOpPlan::Le => first.is_some_and(|v| value <= v),
        StarRocksDeletePredicateOpPlan::Gt => first.is_some_and(|v| value > v),
        StarRocksDeletePredicateOpPlan::Ge => first.is_some_and(|v| value >= v),
        StarRocksDeletePredicateOpPlan::In => values.iter().any(|v| v.as_slice() == value),
        StarRocksDeletePredicateOpPlan::NotIn => values.iter().all(|v| v.as_slice() != value),
        StarRocksDeletePredicateOpPlan::IsNull => false,
        StarRocksDeletePredicateOpPlan::IsNotNull => true,
    }
}

fn page_ranges_for_predicate(
    binding: &PredicateBinding<'_>,
    segment_path: &str,
    segment_bytes: &[u8],
    segment_rows: usize,
) -> Result<Option<Vec<Range<usize>>>, String> {
    let has_zone_map_index = binding.column_meta.page_zone_map_index.is_some();
    let has_bloom_index = matches!(binding.predicate, MinMaxPredicate::Eq { .. })
        && binding
            .column_meta
            .bloom_filter_index
            .as_ref()
            .and_then(|meta| meta.bloom_filter.as_ref())
            .is_some();
    if !has_zone_map_index && !has_bloom_index {
        return Ok(None);
    }

    let page_refs = decode_all_data_page_refs(
        segment_path,
        segment_bytes,
        binding.column_meta,
        segment_rows,
    )?;
    if page_refs.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let zone_maps = if let Some(page_zone_map_index) =
        binding.column_meta.page_zone_map_index.as_ref()
    {
        let zone_map_payloads =
            decode_indexed_binary_values(segment_path, segment_bytes, page_zone_map_index)?;
        if zone_map_payloads.len() != page_refs.len() {
            return Err(format!(
                "page zone map count mismatch: segment={}, unique_id={:?}, page_refs={}, zone_maps={}",
                segment_path,
                binding.column_meta.unique_id,
                page_refs.len(),
                zone_map_payloads.len()
            ));
        }
        let maps = zone_map_payloads
            .iter()
            .map(|v| StarRocksZoneMapMeta::decode_from_bytes(v))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                format!(
                    "decode page zone map payload failed: segment={}, unique_id={:?}, error={}",
                    segment_path, binding.column_meta.unique_id, e
                )
            })?;
        Some(maps)
    } else {
        None
    };

    let bloom_values = if let Some(indexed) = binding
        .column_meta
        .bloom_filter_index
        .as_ref()
        .and_then(|meta| meta.bloom_filter.as_ref())
    {
        if matches!(binding.predicate, MinMaxPredicate::Eq { .. }) {
            let values = decode_indexed_binary_values(segment_path, segment_bytes, indexed)?;
            if values.len() != page_refs.len() {
                return Err(format!(
                    "bloom page count mismatch: segment={}, unique_id={:?}, page_refs={}, bloom_pages={}",
                    segment_path,
                    binding.column_meta.unique_id,
                    page_refs.len(),
                    values.len()
                ));
            }
            Some(values)
        } else {
            None
        }
    } else {
        None
    };

    let mut ranges = Vec::new();
    for (idx, page_ref) in page_refs.iter().enumerate() {
        if let Some(zone_maps) = zone_maps.as_ref() {
            let zone_map = zone_maps
                .get(idx)
                .ok_or_else(|| "page zone map index out of bounds".to_string())?;
            if !zone_map_may_satisfy(binding, zone_map)? {
                continue;
            }
        }

        if let Some(blooms) = bloom_values.as_ref() {
            let bloom = blooms
                .get(idx)
                .ok_or_else(|| "bloom page index out of bounds".to_string())?;
            if let Some(false) = bloom_may_satisfy_eq(binding, bloom)? {
                continue;
            }
        }

        let start = usize::try_from(page_ref.first_ordinal).map_err(|_| {
            format!(
                "page first_ordinal overflow in pruning: segment={}, first_ordinal={}",
                segment_path, page_ref.first_ordinal
            )
        })?;
        let len = usize::try_from(page_ref.num_values).map_err(|_| {
            format!(
                "page num_values overflow in pruning: segment={}, num_values={}",
                segment_path, page_ref.num_values
            )
        })?;
        let end = start.checked_add(len).ok_or_else(|| {
            format!(
                "page range overflow in pruning: segment={}, start={}, num_values={}",
                segment_path, start, len
            )
        })?;
        if end > segment_rows {
            return Err(format!(
                "page range exceeds segment rows in pruning: segment={}, page_index={}, start={}, end={}, segment_rows={}",
                segment_path, idx, start, end, segment_rows
            ));
        }
        if start < end {
            ranges.push(start..end);
        }
    }

    Ok(Some(merge_ranges(ranges)))
}

fn decode_scalar_array_for_ranges(
    segment_path: &str,
    segment_bytes: &[u8],
    column_meta: &StarRocksSegmentColumnMeta,
    schema: &StarRocksNativeSchemaColumnPlan,
    output_data_type: &DataType,
    output_name: &str,
    selected_ranges: &[Range<usize>],
    segment_rows: usize,
) -> Result<ArrayRef, String> {
    let output_kind = OutputColumnKind::from_arrow_type(output_data_type).ok_or_else(|| {
        format!(
            "unsupported scalar output type in native starrocks reader: segment={}, output_column={}, output_type={:?}",
            segment_path, output_name, output_data_type
        )
    })?;
    let expected_logical_type =
        expected_logical_type_from_schema_type(&schema.schema_type).ok_or_else(|| {
            format!(
                "unsupported scalar schema type in native starrocks reader: segment={}, output_column={}, schema_type={}",
                segment_path, output_name, schema.schema_type
            )
        })?;

    let spec = build_column_decode_spec(
        segment_path,
        segment_bytes,
        column_meta,
        expected_logical_type,
        output_kind.arrow_type_name(),
        output_kind,
        output_name,
    )?;
    let page_refs =
        decode_all_data_page_refs(segment_path, segment_bytes, column_meta, segment_rows)?;

    let expected_rows = ranges_total_rows(selected_ranges);
    let mut data = OutputColumnData::with_capacity(output_kind, expected_rows);
    let mut null_flags = Vec::with_capacity(expected_rows);
    let mut has_null = false;

    for page_ref in &page_refs {
        let page_start = usize::try_from(page_ref.first_ordinal).map_err(|_| {
            format!(
                "page first_ordinal overflow while decoding selected rows: segment={}, output_column={}, first_ordinal={}",
                segment_path, output_name, page_ref.first_ordinal
            )
        })?;
        let page_rows = usize::try_from(page_ref.num_values).map_err(|_| {
            format!(
                "page num_values overflow while decoding selected rows: segment={}, output_column={}, num_values={}",
                segment_path, output_name, page_ref.num_values
            )
        })?;
        let page_end = page_start.checked_add(page_rows).ok_or_else(|| {
            format!(
                "page range overflow while decoding selected rows: segment={}, output_column={}, page_start={}, page_rows={}",
                segment_path, output_name, page_start, page_rows
            )
        })?;

        let local_ranges = overlap_local_ranges(selected_ranges, page_start, page_end);
        if local_ranges.is_empty() {
            continue;
        }

        let decoded = decode_one_data_page(
            segment_path,
            segment_bytes,
            &page_ref.page_pointer,
            column_meta.unique_id,
            &spec,
        )?;
        if decoded.num_values != page_rows {
            return Err(format!(
                "decoded data page row count mismatch: segment={}, output_column={}, page_start={}, expected_rows={}, actual_rows={}",
                segment_path, output_name, page_start, page_rows, decoded.num_values
            ));
        }

        append_selected_payload(
            &mut data,
            decoded,
            &local_ranges,
            segment_path,
            output_name,
            is_char_schema_type(&schema.schema_type),
            &mut null_flags,
            &mut has_null,
        )?;
    }

    if null_flags.len() != expected_rows {
        return Err(format!(
            "selected scalar row count mismatch after page decode: segment={}, output_column={}, expected_rows={}, actual_rows={}",
            segment_path,
            output_name,
            expected_rows,
            null_flags.len()
        ));
    }

    data.into_array(
        &null_flags,
        has_null,
        output_name,
        decimal_output_meta_from_arrow_type(output_data_type),
    )
}

fn append_selected_payload(
    data: &mut OutputColumnData,
    decoded: DecodedDataPageValues,
    local_ranges: &[Range<usize>],
    segment_path: &str,
    output_name: &str,
    trim_char_padding: bool,
    output_null_flags: &mut Vec<u8>,
    has_null: &mut bool,
) -> Result<(), String> {
    let selected_rows = ranges_total_rows(local_ranges);

    match decoded.payload {
        DecodedPageValuePayload::Fixed {
            value_bytes,
            elem_size,
        } => {
            let mut selected_bytes = Vec::with_capacity(selected_rows.saturating_mul(elem_size));
            for range in local_ranges {
                let start = range
                    .start
                    .checked_mul(elem_size)
                    .ok_or_else(|| "fixed payload byte offset overflow".to_string())?;
                let end = range
                    .end
                    .checked_mul(elem_size)
                    .ok_or_else(|| "fixed payload byte offset overflow".to_string())?;
                if end > value_bytes.len() {
                    return Err(format!(
                        "fixed payload range out of bounds: segment={}, output_column={}, range={:?}, elem_size={}, payload_bytes={}",
                        segment_path,
                        output_name,
                        range,
                        elem_size,
                        value_bytes.len()
                    ));
                }
                selected_bytes.extend_from_slice(&value_bytes[start..end]);
            }
            data.append_from_bytes(&selected_bytes, elem_size, segment_path, output_name)?;
        }
        DecodedPageValuePayload::Variable { values } => {
            let mut selected_values = Vec::with_capacity(selected_rows);
            for range in local_ranges {
                if range.end > values.len() {
                    return Err(format!(
                        "variable payload range out of bounds: segment={}, output_column={}, range={:?}, payload_values={}",
                        segment_path,
                        output_name,
                        range,
                        values.len()
                    ));
                }
                selected_values.extend(values[range.start..range.end].iter().cloned());
            }
            data.append_variable_values(
                selected_values,
                segment_path,
                output_name,
                trim_char_padding,
            )?;
        }
    }

    match decoded.null_flags {
        Some(page_null_flags) => {
            for range in local_ranges {
                if range.end > page_null_flags.len() {
                    return Err(format!(
                        "null flag range out of bounds: segment={}, output_column={}, range={:?}, null_flags={}",
                        segment_path,
                        output_name,
                        range,
                        page_null_flags.len()
                    ));
                }
                for flag in &page_null_flags[range.start..range.end] {
                    if *flag != 0 && *flag != 1 {
                        return Err(format!(
                            "invalid null flag value in selected payload: segment={}, output_column={}, flag={}",
                            segment_path, output_name, flag
                        ));
                    }
                    if *flag == 1 {
                        *has_null = true;
                    }
                    output_null_flags.push(*flag);
                }
            }
        }
        None => {
            output_null_flags.extend(std::iter::repeat_n(0_u8, selected_rows));
        }
    }

    Ok(())
}

fn take_array_by_ranges(
    full_array: &ArrayRef,
    selected_ranges: &[Range<usize>],
    segment_path: &str,
    output_name: &str,
) -> Result<ArrayRef, String> {
    let selected_rows = ranges_total_rows(selected_ranges);
    let mut indices = Vec::with_capacity(selected_rows);
    for range in selected_ranges {
        if range.end > full_array.len() {
            return Err(format!(
                "complex take range out of bounds: segment={}, output_column={}, range={:?}, array_rows={}",
                segment_path,
                output_name,
                range,
                full_array.len()
            ));
        }
        for idx in range.clone() {
            indices.push(u32::try_from(idx).map_err(|_| {
                format!(
                    "row index overflow while taking complex array: segment={}, output_column={}, row_index={}",
                    segment_path, output_name, idx
                )
            })?);
        }
    }
    let index_array = UInt32Array::from(indices);
    take(full_array.as_ref(), &index_array, None).map_err(|e| {
        format!(
            "take selected rows from complex array failed: segment={}, output_column={}, error={}",
            segment_path, output_name, e
        )
    })
}

fn take_array_by_keep_mask(
    array: &ArrayRef,
    keep_mask: &[bool],
    segment_path: &str,
    output_name: &str,
) -> Result<ArrayRef, String> {
    if array.len() != keep_mask.len() {
        return Err(format!(
            "delete keep-mask length mismatch: segment={}, output_column={}, array_rows={}, keep_mask_rows={}",
            segment_path,
            output_name,
            array.len(),
            keep_mask.len()
        ));
    }
    if keep_mask.iter().all(|v| *v) {
        return Ok(array.clone());
    }
    let mut indices = Vec::with_capacity(array.len());
    for (idx, keep) in keep_mask.iter().enumerate() {
        if *keep {
            indices.push(u32::try_from(idx).map_err(|_| {
                format!(
                    "row index overflow while applying delete keep-mask: segment={}, output_column={}, row_index={}",
                    segment_path, output_name, idx
                )
            })?);
        }
    }
    let index_array = UInt32Array::from(indices);
    take(array.as_ref(), &index_array, None).map_err(|e| {
        format!(
            "take rows by delete keep-mask failed: segment={}, output_column={}, error={}",
            segment_path, output_name, e
        )
    })
}

fn predicate_column_index(predicate: &MinMaxPredicate) -> Option<usize> {
    let column = match predicate {
        MinMaxPredicate::Le { column, .. }
        | MinMaxPredicate::Ge { column, .. }
        | MinMaxPredicate::Lt { column, .. }
        | MinMaxPredicate::Gt { column, .. }
        | MinMaxPredicate::Eq { column, .. } => column,
    };
    column.parse::<usize>().ok()
}

fn predicate_literal(predicate: &MinMaxPredicate) -> &LiteralValue {
    match predicate {
        MinMaxPredicate::Le { value, .. }
        | MinMaxPredicate::Ge { value, .. }
        | MinMaxPredicate::Lt { value, .. }
        | MinMaxPredicate::Gt { value, .. }
        | MinMaxPredicate::Eq { value, .. } => value,
    }
}

fn predicate_op(predicate: &MinMaxPredicate) -> PredicateOp {
    match predicate {
        MinMaxPredicate::Le { .. } => PredicateOp::Le,
        MinMaxPredicate::Ge { .. } => PredicateOp::Ge,
        MinMaxPredicate::Lt { .. } => PredicateOp::Lt,
        MinMaxPredicate::Gt { .. } => PredicateOp::Gt,
        MinMaxPredicate::Eq { .. } => PredicateOp::Eq,
    }
}

fn zone_map_may_satisfy(
    binding: &PredicateBinding<'_>,
    zone_map: &StarRocksZoneMapMeta,
) -> Result<bool, String> {
    let has_not_null = zone_map.has_not_null.unwrap_or(false);
    if !has_not_null {
        return Ok(false);
    }

    let Some(logical_type) = binding.column_meta.logical_type else {
        return Ok(true);
    };
    let Some(min_raw) = zone_map.min.as_deref() else {
        return Ok(true);
    };
    let Some(max_raw) = zone_map.max.as_deref() else {
        return Ok(true);
    };
    let op = predicate_op(binding.predicate);
    let literal = predicate_literal(binding.predicate);

    let maybe = match logical_type {
        LOGICAL_TYPE_TINYINT | LOGICAL_TYPE_SMALLINT | LOGICAL_TYPE_INT | LOGICAL_TYPE_BIGINT => {
            let min = parse_zone_i64(min_raw);
            let max = parse_zone_i64(max_raw);
            let value = literal_to_i64(literal);
            match (min, max, value) {
                (Some(min), Some(max), Some(value)) => {
                    Some(range_may_match_i64(op, min, max, value))
                }
                _ => None,
            }
        }
        LOGICAL_TYPE_FLOAT | LOGICAL_TYPE_DOUBLE => {
            let min = parse_zone_f64(min_raw);
            let max = parse_zone_f64(max_raw);
            let value = literal_to_f64(literal);
            match (min, max, value) {
                (Some(min), Some(max), Some(value)) => {
                    Some(range_may_match_f64(op, min, max, value))
                }
                _ => None,
            }
        }
        LOGICAL_TYPE_BOOLEAN => {
            let min = parse_zone_bool(min_raw).map(i64::from);
            let max = parse_zone_bool(max_raw).map(i64::from);
            let value = literal_to_bool(literal).map(i64::from);
            match (min, max, value) {
                (Some(min), Some(max), Some(value)) => {
                    Some(range_may_match_i64(op, min, max, value))
                }
                _ => None,
            }
        }
        LOGICAL_TYPE_DATE => {
            let min = parse_zone_date_days(min_raw);
            let max = parse_zone_date_days(max_raw);
            let value = literal_to_date_days(literal);
            match (min, max, value) {
                (Some(min), Some(max), Some(value)) => Some(range_may_match_i64(
                    op,
                    i64::from(min),
                    i64::from(max),
                    i64::from(value),
                )),
                _ => None,
            }
        }
        LOGICAL_TYPE_DATETIME => {
            let min = parse_zone_datetime_micros(min_raw);
            let max = parse_zone_datetime_micros(max_raw);
            let value = literal_to_datetime_micros(literal);
            match (min, max, value) {
                (Some(min), Some(max), Some(value)) => {
                    Some(range_may_match_i64(op, min, max, value))
                }
                _ => None,
            }
        }
        LOGICAL_TYPE_DECIMAL32 | LOGICAL_TYPE_DECIMAL64 | LOGICAL_TYPE_DECIMAL128 => {
            match decimal_precision_scale(&binding.projected.schema) {
                Some((precision, scale)) => {
                    let min = parse_zone_decimal(min_raw, precision, scale);
                    let max = parse_zone_decimal(max_raw, precision, scale);
                    let value = literal_to_decimal_scaled(literal, scale);
                    match (min, max, value) {
                        (Some(min), Some(max), Some(value)) => {
                            Some(range_may_match_i128(op, min, max, value))
                        }
                        _ => None,
                    }
                }
                None => None,
            }
        }
        LOGICAL_TYPE_VARCHAR | LOGICAL_TYPE_BINARY | LOGICAL_TYPE_VARBINARY => {
            let value = literal_to_bytes(literal);
            value.map(|v| range_may_match_bytes(op, min_raw, max_raw, v.as_slice()))
        }
        LOGICAL_TYPE_CHAR => {
            let value = literal_to_bytes(literal);
            value.map(|v| {
                let min_trim = trim_char_zero_padding(min_raw);
                let max_trim = trim_char_zero_padding(max_raw);
                range_may_match_bytes(op, &min_trim, &max_trim, v.as_slice())
            })
        }
        _ => None,
    };

    Ok(maybe.unwrap_or(true))
}

fn bloom_may_satisfy_eq(
    binding: &PredicateBinding<'_>,
    bloom_bytes: &[u8],
) -> Result<Option<bool>, String> {
    if !matches!(binding.predicate, MinMaxPredicate::Eq { .. }) {
        return Ok(None);
    }

    let Some(bloom_meta) = binding.column_meta.bloom_filter_index.as_ref() else {
        return Ok(None);
    };
    if bloom_meta.algorithm != Some(BLOOM_ALGORITHM_BLOCK)
        || bloom_meta.hash_strategy != Some(BLOOM_HASH_MURMUR3)
    {
        return Ok(None);
    }

    let Some(value_bytes) = encode_literal_for_bloom(binding)? else {
        return Ok(None);
    };

    let may_match = block_split_bloom_test(bloom_bytes, &value_bytes).ok_or_else(|| {
        format!(
            "invalid bloom filter payload: unique_id={:?}, bytes={}",
            binding.column_meta.unique_id,
            bloom_bytes.len()
        )
    })?;
    Ok(Some(may_match))
}

fn encode_literal_for_bloom(binding: &PredicateBinding<'_>) -> Result<Option<Vec<u8>>, String> {
    let Some(logical_type) = binding.column_meta.logical_type else {
        return Ok(None);
    };
    let literal = predicate_literal(binding.predicate);

    let encoded = match logical_type {
        LOGICAL_TYPE_TINYINT => literal_to_i64(literal)
            .and_then(|v| i8::try_from(v).ok())
            .map(|v| vec![v as u8]),
        LOGICAL_TYPE_SMALLINT => literal_to_i64(literal)
            .and_then(|v| i16::try_from(v).ok())
            .map(|v| v.to_le_bytes().to_vec()),
        LOGICAL_TYPE_INT => literal_to_i64(literal)
            .and_then(|v| i32::try_from(v).ok())
            .map(|v| v.to_le_bytes().to_vec()),
        LOGICAL_TYPE_BIGINT => literal_to_i64(literal).map(|v| v.to_le_bytes().to_vec()),
        LOGICAL_TYPE_BOOLEAN => literal_to_bool(literal).map(|v| vec![u8::from(v)]),
        LOGICAL_TYPE_FLOAT => literal_to_f64(literal).map(|v| (v as f32).to_le_bytes().to_vec()),
        LOGICAL_TYPE_DOUBLE => literal_to_f64(literal).map(|v| v.to_le_bytes().to_vec()),
        LOGICAL_TYPE_DATE => literal_to_date_days(literal).map(|days| {
            let julian = days + DATE_UNIX_EPOCH_JULIAN;
            julian.to_le_bytes().to_vec()
        }),
        LOGICAL_TYPE_DECIMAL32 | LOGICAL_TYPE_DECIMAL64 | LOGICAL_TYPE_DECIMAL128 => {
            let Some((_, scale)) = decimal_precision_scale(&binding.projected.schema) else {
                return Ok(None);
            };
            let Some(decimal) = literal_to_decimal_scaled(literal, scale) else {
                return Ok(None);
            };
            match logical_type {
                LOGICAL_TYPE_DECIMAL32 => i32::try_from(decimal)
                    .ok()
                    .map(|v| v.to_le_bytes().to_vec()),
                LOGICAL_TYPE_DECIMAL64 => i64::try_from(decimal)
                    .ok()
                    .map(|v| v.to_le_bytes().to_vec()),
                LOGICAL_TYPE_DECIMAL128 => Some(decimal.to_le_bytes().to_vec()),
                _ => None,
            }
        }
        LOGICAL_TYPE_VARCHAR | LOGICAL_TYPE_BINARY | LOGICAL_TYPE_VARBINARY => {
            literal_to_bytes(literal)
        }
        _ => None,
    };

    Ok(encoded)
}

fn range_may_match_i64(op: PredicateOp, min: i64, max: i64, value: i64) -> bool {
    match op {
        PredicateOp::Le => min <= value,
        PredicateOp::Lt => min < value,
        PredicateOp::Ge => max >= value,
        PredicateOp::Gt => max > value,
        PredicateOp::Eq => min <= value && max >= value,
    }
}

fn range_may_match_i128(op: PredicateOp, min: i128, max: i128, value: i128) -> bool {
    match op {
        PredicateOp::Le => min <= value,
        PredicateOp::Lt => min < value,
        PredicateOp::Ge => max >= value,
        PredicateOp::Gt => max > value,
        PredicateOp::Eq => min <= value && max >= value,
    }
}

fn range_may_match_f64(op: PredicateOp, min: f64, max: f64, value: f64) -> bool {
    if min.is_nan() || max.is_nan() || value.is_nan() {
        return true;
    }
    match op {
        PredicateOp::Le => min <= value,
        PredicateOp::Lt => min < value,
        PredicateOp::Ge => max >= value,
        PredicateOp::Gt => max > value,
        PredicateOp::Eq => min <= value && max >= value,
    }
}

fn range_may_match_bytes(op: PredicateOp, min: &[u8], max: &[u8], value: &[u8]) -> bool {
    match op {
        PredicateOp::Le => min <= value,
        PredicateOp::Lt => min < value,
        PredicateOp::Ge => max >= value,
        PredicateOp::Gt => max > value,
        PredicateOp::Eq => min <= value && max >= value,
    }
}

fn trim_char_zero_padding(value: &[u8]) -> Vec<u8> {
    let mut end = value.len();
    while end > 0 && value[end - 1] == 0 {
        end -= 1;
    }
    value[..end].to_vec()
}

fn literal_to_i64(literal: &LiteralValue) -> Option<i64> {
    match literal {
        LiteralValue::Int8(v) => Some(i64::from(*v)),
        LiteralValue::Int16(v) => Some(i64::from(*v)),
        LiteralValue::Int32(v) => Some(i64::from(*v)),
        LiteralValue::Int64(v) => Some(*v),
        _ => None,
    }
}

fn literal_to_f64(literal: &LiteralValue) -> Option<f64> {
    match literal {
        LiteralValue::Float32(v) => Some(f64::from(*v)),
        LiteralValue::Float64(v) => Some(*v),
        _ => None,
    }
}

fn literal_to_bool(literal: &LiteralValue) -> Option<bool> {
    match literal {
        LiteralValue::Bool(v) => Some(*v),
        _ => None,
    }
}

fn literal_to_bytes(literal: &LiteralValue) -> Option<Vec<u8>> {
    match literal {
        LiteralValue::Utf8(v) => Some(v.as_bytes().to_vec()),
        _ => None,
    }
}

fn literal_to_date_days(literal: &LiteralValue) -> Option<i32> {
    match literal {
        LiteralValue::Date32(v) => Some(*v),
        LiteralValue::Utf8(v) => parse_date_days(v.as_bytes()),
        _ => None,
    }
}

fn literal_to_datetime_micros(literal: &LiteralValue) -> Option<i64> {
    match literal {
        LiteralValue::Utf8(v) => parse_datetime_micros(v.as_bytes()),
        LiteralValue::Date32(v) => {
            let days = i64::from(*v);
            days.checked_mul(USECS_PER_DAY_I64)
        }
        _ => None,
    }
}

fn literal_to_decimal_scaled(literal: &LiteralValue, target_scale: i8) -> Option<i128> {
    match literal {
        LiteralValue::Decimal128 { value, scale, .. } => {
            align_decimal_scale(*value, *scale, target_scale)
        }
        LiteralValue::Int8(v) => scale_integer(i128::from(*v), target_scale),
        LiteralValue::Int16(v) => scale_integer(i128::from(*v), target_scale),
        LiteralValue::Int32(v) => scale_integer(i128::from(*v), target_scale),
        LiteralValue::Int64(v) => scale_integer(i128::from(*v), target_scale),
        _ => None,
    }
}

fn decimal_precision_scale(schema: &StarRocksNativeSchemaColumnPlan) -> Option<(u8, i8)> {
    Some((schema.precision?, schema.scale?))
}

fn parse_zone_i64(raw: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(raw).ok()?.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<i64>().ok()
}

fn parse_zone_f64(raw: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(raw).ok()?.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok()
}

fn parse_zone_bool(raw: &[u8]) -> Option<bool> {
    let s = std::str::from_utf8(raw).ok()?.trim();
    match s {
        "0" | "false" | "FALSE" => Some(false),
        "1" | "true" | "TRUE" => Some(true),
        _ => None,
    }
}

fn parse_zone_date_days(raw: &[u8]) -> Option<i32> {
    parse_date_days(raw)
}

fn parse_zone_datetime_micros(raw: &[u8]) -> Option<i64> {
    parse_datetime_micros(raw)
}

fn parse_zone_decimal(raw: &[u8], precision: u8, scale: i8) -> Option<i128> {
    let s = std::str::from_utf8(raw).ok()?.trim();
    if s.is_empty() {
        return None;
    }
    parse_decimal_scaled(s, precision, scale).ok()
}

fn parse_date_days(raw: &[u8]) -> Option<i32> {
    let text = std::str::from_utf8(raw).ok()?.trim();
    if text.is_empty() {
        return None;
    }
    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return Some(date.num_days_from_ce() - DATE_EPOCH_DAY_OFFSET);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.date().num_days_from_ce() - DATE_EPOCH_DAY_OFFSET);
    }
    None
}

fn parse_datetime_micros(raw: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(raw).ok()?.trim();
    if text.is_empty() {
        return None;
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(dt.and_utc().timestamp_micros());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.and_utc().timestamp_micros());
    }
    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        let dt = date.and_hms_opt(0, 0, 0)?;
        return Some(dt.and_utc().timestamp_micros());
    }
    None
}

fn parse_decimal_scaled(value: &str, precision: u8, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err(format!("invalid decimal scale: {}", scale));
    }
    let mut s = value.trim();
    if s.is_empty() {
        return Err("empty decimal value".to_string());
    }
    let mut sign: i128 = 1;
    if let Some(rest) = s.strip_prefix('-') {
        sign = -1;
        s = rest;
    } else if let Some(rest) = s.strip_prefix('+') {
        s = rest;
    }
    if s.is_empty() {
        return Err("empty decimal value".to_string());
    }
    let mut iter = s.split('.');
    let int_part_raw = iter.next().unwrap_or("");
    let frac_part = iter.next().unwrap_or("");
    if iter.next().is_some() {
        return Err(format!("invalid decimal value '{}'", value));
    }
    if int_part_raw.is_empty() && frac_part.is_empty() {
        return Err(format!("invalid decimal value '{}'", value));
    }
    let int_part = if int_part_raw.is_empty() {
        "0"
    } else {
        int_part_raw
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("invalid decimal value '{}'", value));
    }

    let target_scale = usize::try_from(scale).map_err(|_| "decimal scale overflow")?;
    if frac_part.len() > target_scale {
        return Err(format!(
            "decimal value '{}' exceeds scale {}",
            value, target_scale
        ));
    }

    let mut digits = String::with_capacity(int_part.len() + target_scale);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in 0..(target_scale - frac_part.len()) {
        digits.push('0');
    }

    let trimmed = digits.trim_start_matches('0');
    let final_digits = if trimmed.is_empty() { "0" } else { trimmed };
    if final_digits.len() > precision as usize {
        return Err(format!(
            "decimal value '{}' exceeds precision {}",
            value, precision
        ));
    }

    let unsigned = final_digits
        .parse::<i128>()
        .map_err(|_| format!("parse decimal '{}' failed", value))?;
    Ok(unsigned.saturating_mul(sign))
}

fn align_decimal_scale(value: i128, from_scale: i8, to_scale: i8) -> Option<i128> {
    if from_scale == to_scale {
        return Some(value);
    }
    if from_scale < 0 || to_scale < 0 {
        return None;
    }

    let from_scale_u = u32::try_from(from_scale).ok()?;
    let to_scale_u = u32::try_from(to_scale).ok()?;
    if to_scale_u > from_scale_u {
        let factor = pow10_i128(to_scale_u - from_scale_u)?;
        value.checked_mul(factor)
    } else {
        let factor = pow10_i128(from_scale_u - to_scale_u)?;
        if factor == 0 || value % factor != 0 {
            return None;
        }
        Some(value / factor)
    }
}

fn scale_integer(value: i128, scale: i8) -> Option<i128> {
    if scale < 0 {
        return None;
    }
    let factor = pow10_i128(u32::try_from(scale).ok()?)?;
    value.checked_mul(factor)
}

fn pow10_i128(exp: u32) -> Option<i128> {
    let mut out = 1_i128;
    for _ in 0..exp {
        out = out.checked_mul(10)?;
    }
    Some(out)
}

fn block_split_bloom_test(bloom: &[u8], key: &[u8]) -> Option<bool> {
    if bloom.len() <= 1 {
        return None;
    }
    let num_bytes = bloom.len() - 1;
    if num_bytes < BLOOM_BLOCK_BYTES || (num_bytes & (num_bytes - 1)) != 0 {
        return None;
    }

    let hash = murmur_hash3_x64_64(key, BLOOM_SEED);
    let num_blocks = num_bytes / BLOOM_BLOCK_BYTES;
    let block_index = ((hash >> 32) as usize) & (num_blocks - 1);
    let key32 = hash as u32;
    let block_offset = block_index * BLOOM_BLOCK_BYTES;

    for (i, salt) in BLOOM_SALTS.iter().enumerate() {
        let bit = key32.wrapping_mul(*salt) >> 27;
        let mask = 1_u32 << bit;
        let word_off = block_offset + i * 4;
        let word_bytes = bloom.get(word_off..word_off + 4)?;
        let word = u32::from_le_bytes(word_bytes.try_into().ok()?);
        if (word & mask) == 0 {
            return Some(false);
        }
    }
    Some(true)
}

fn murmur_hash3_x64_64(data: &[u8], seed: u64) -> u64 {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;

    let mut h1 = seed;
    let nblocks = data.len() / 8;

    for i in 0..nblocks {
        let start = i * 8;
        let mut k1 = u64::from_le_bytes(data[start..start + 8].try_into().expect("slice len"));
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;

        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);
    }

    let tail = &data[nblocks * 8..];
    let mut k1 = 0_u64;
    match tail.len() {
        7 => {
            k1 ^= u64::from(tail[6]) << 48;
            k1 ^= u64::from(tail[5]) << 40;
            k1 ^= u64::from(tail[4]) << 32;
            k1 ^= u64::from(tail[3]) << 24;
            k1 ^= u64::from(tail[2]) << 16;
            k1 ^= u64::from(tail[1]) << 8;
            k1 ^= u64::from(tail[0]);
        }
        6 => {
            k1 ^= u64::from(tail[5]) << 40;
            k1 ^= u64::from(tail[4]) << 32;
            k1 ^= u64::from(tail[3]) << 24;
            k1 ^= u64::from(tail[2]) << 16;
            k1 ^= u64::from(tail[1]) << 8;
            k1 ^= u64::from(tail[0]);
        }
        5 => {
            k1 ^= u64::from(tail[4]) << 32;
            k1 ^= u64::from(tail[3]) << 24;
            k1 ^= u64::from(tail[2]) << 16;
            k1 ^= u64::from(tail[1]) << 8;
            k1 ^= u64::from(tail[0]);
        }
        4 => {
            k1 ^= u64::from(tail[3]) << 24;
            k1 ^= u64::from(tail[2]) << 16;
            k1 ^= u64::from(tail[1]) << 8;
            k1 ^= u64::from(tail[0]);
        }
        3 => {
            k1 ^= u64::from(tail[2]) << 16;
            k1 ^= u64::from(tail[1]) << 8;
            k1 ^= u64::from(tail[0]);
        }
        2 => {
            k1 ^= u64::from(tail[1]) << 8;
            k1 ^= u64::from(tail[0]);
        }
        1 => {
            k1 ^= u64::from(tail[0]);
        }
        _ => {}
    }
    if !tail.is_empty() {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= data.len() as u64;
    fmix64(h1)
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

fn ranges_cover_segment(ranges: &[Range<usize>], segment_rows: usize) -> bool {
    ranges.len() == 1 && ranges[0].start == 0 && ranges[0].end == segment_rows
}

fn ranges_total_rows(ranges: &[Range<usize>]) -> usize {
    ranges
        .iter()
        .map(|r| r.end.saturating_sub(r.start))
        .sum::<usize>()
}

fn merge_ranges(mut ranges: Vec<Range<usize>>) -> Vec<Range<usize>> {
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort_by_key(|r| r.start);
    let mut merged = Vec::with_capacity(ranges.len());
    let mut current = ranges[0].clone();
    for r in ranges.into_iter().skip(1) {
        if r.start <= current.end {
            if r.end > current.end {
                current.end = r.end;
            }
        } else {
            merged.push(current);
            current = r;
        }
    }
    merged.push(current);
    merged
}

fn intersect_ranges(a: &[Range<usize>], b: &[Range<usize>]) -> Vec<Range<usize>> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        let start = a[i].start.max(b[j].start);
        let end = a[i].end.min(b[j].end);
        if start < end {
            out.push(start..end);
        }
        if a[i].end < b[j].end {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

fn overlap_local_ranges(
    selected_ranges: &[Range<usize>],
    page_start: usize,
    page_end: usize,
) -> Vec<Range<usize>> {
    let mut out = Vec::new();
    for range in selected_ranges {
        if range.end <= page_start {
            continue;
        }
        if range.start >= page_end {
            break;
        }
        let start = range.start.max(page_start) - page_start;
        let end = range.end.min(page_end) - page_start;
        if start < end {
            out.push(start..end);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use std::sync::Arc;

    #[test]
    fn normalize_delete_value_trims_quotes() {
        assert_eq!(normalize_delete_value("'abc'"), "abc");
        assert_eq!(normalize_delete_value("\"abc\""), "abc");
        assert_eq!(normalize_delete_value("  abc  "), "abc");
        assert_eq!(normalize_delete_value("'a b'"), "a b");
    }

    #[test]
    fn eval_delete_term_scalar_handles_in_and_not_in() {
        let in_values = vec![1_i64, 3, 5];
        assert!(eval_delete_term_scalar_value(
            StarRocksDeletePredicateOpPlan::In,
            3_i64,
            &in_values
        ));
        assert!(!eval_delete_term_scalar_value(
            StarRocksDeletePredicateOpPlan::In,
            4_i64,
            &in_values
        ));
        assert!(eval_delete_term_scalar_value(
            StarRocksDeletePredicateOpPlan::NotIn,
            4_i64,
            &in_values
        ));
        assert!(!eval_delete_term_scalar_value(
            StarRocksDeletePredicateOpPlan::NotIn,
            5_i64,
            &in_values
        ));
    }

    #[test]
    fn take_array_by_keep_mask_filters_rows() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
        let keep = vec![true, false, true, false];
        let taken =
            take_array_by_keep_mask(&array, &keep, "segment.dat", "c1").expect("apply keep-mask");
        let int64 = taken
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("downcast to Int64Array");
        assert_eq!(int64.len(), 2);
        assert_eq!(int64.value(0), 10);
        assert_eq!(int64.value(1), 30);
    }

    #[test]
    fn merge_keep_masks_ands_two_masks() {
        let merged = merge_keep_masks(
            Some(vec![true, false, true]),
            Some(vec![false, true, true]),
            "segment.dat",
            3,
        )
        .expect("merge keep masks")
        .expect("merged mask");
        assert_eq!(merged, vec![false, false, true]);
    }

    #[test]
    fn decode_delvec_bitmap_round_trip() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(3);
        let mut bytes = vec![0x01];
        bitmap
            .serialize_into(&mut bytes)
            .expect("serialize roaring bitmap");
        let decoded = decode_delvec_bitmap(&bytes, "segment.dat", 8).expect("decode delvec");
        assert!(decoded.contains(1));
        assert!(decoded.contains(3));
        assert!(!decoded.contains(2));
    }

    #[test]
    fn crc32c_unmask_is_inverse_of_mask() {
        fn mask(crc: u32) -> u32 {
            crc.rotate_right(15).wrapping_add(0xa282_ead8)
        }
        let raw = 0x1234_5678_u32;
        assert_eq!(crc32c_unmask(mask(raw)), raw);
    }
}
