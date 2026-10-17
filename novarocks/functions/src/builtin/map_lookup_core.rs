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
//! Original map key equality, reverse search, take and NULL repair author.
use super::array_literal_core::CollectionObservation;
use crate::largeint;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, MapArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt32Array,
};
use arrow_schema::DataType;
use arrow_select::take::take;
/// This existing frozen Map has an uninhabited key. Its value type remains
/// independent and complete, including nominal domains and nullability. It
/// cannot contain an entry without violating the ordinary non-null key rule.
/// This is a complete-type fact, independent of literal origin or row bytes.
pub fn has_uninhabited_map_key(value: &crate::FunctionValueType) -> bool {
    use novarocks_type_contract::{ValueKind, ValueLogicalType};
    if value.logical_type() != ValueLogicalType::Physical
        || !has_uninhabited_map_key_carrier(value.data_type())
    {
        return false;
    }
    let ValueKind::Map { key, .. } = value.value_type().kind() else {
        return false;
    };
    !key.nullable() && matches!(key.kind(), ValueKind::Scalar(DataType::Null))
}

/// Signature-carrier constraint only; executable selection also checks the
/// complete semantic declaration through `has_uninhabited_map_key`.
pub(super) fn has_uninhabited_map_key_carrier(value: &DataType) -> bool {
    let DataType::Map(entries, _) = value else {
        return false;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return false;
    };
    fields.len() == 2
        && !entries.is_nullable()
        && fields[0].data_type() == &DataType::Null
        && !fields[0].is_nullable()
}

pub fn row_index(row: usize, len: usize) -> usize {
    if len == 1 { 0 } else { row }
}
#[derive(Debug)]
pub enum MapLookupFailure<E> {
    Data(String),
    Control(E),
}
pub fn cast_output(out: ArrayRef, output_type: Option<&DataType>) -> Result<ArrayRef, String> {
    let Some(target) = output_type else {
        return Ok(out);
    };
    if out.data_type() == target {
        return Ok(out);
    }
    arrow_cast::cast(&out, target).map_err(|e| e.to_string())
}
pub fn compare_keys_at(
    keys: &ArrayRef,
    key_idx: usize,
    targets: &ArrayRef,
    target_idx: usize,
) -> Result<bool, String> {
    if keys.is_null(key_idx) || targets.is_null(target_idx) {
        return Ok(false);
    }
    if keys.data_type() != targets.data_type() {
        return Err(format!(
            "map key type mismatch: {:?} vs {:?}",
            keys.data_type(),
            targets.data_type()
        ));
    }

    match keys.data_type() {
        DataType::Int8 => {
            let l = keys.as_any().downcast_ref::<Int8Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Int16 => {
            let l = keys.as_any().downcast_ref::<Int16Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Int32 => {
            let l = keys.as_any().downcast_ref::<Int32Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Int64 => {
            let l = keys.as_any().downcast_ref::<Int64Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Float32 => {
            let l = keys.as_any().downcast_ref::<Float32Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Float64 => {
            let l = keys.as_any().downcast_ref::<Float64Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Boolean => {
            let l = keys.as_any().downcast_ref::<BooleanArray>().unwrap();
            let r = targets.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Utf8 => {
            let l = keys.as_any().downcast_ref::<StringArray>().unwrap();
            let r = targets.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Date32 => {
            let l = keys.as_any().downcast_ref::<Date32Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Decimal128(_, _) => {
            let l = keys.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Decimal256(_, _) => {
            let l = keys.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Decimal256Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Second, None) => {
            let l = keys
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let r = targets
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None) => {
            let l = keys
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let r = targets
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => {
            let l = keys
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let r = targets
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None) => {
            let l = keys
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let r = targets
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let l = keys
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast key to FixedSizeBinaryArray".to_string())?;
            let r = targets
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast target to FixedSizeBinaryArray".to_string())?;
            let lv = largeint::i128_from_be_bytes(l.value(key_idx))
                .map_err(|e| format!("map key LARGEINT decode failed: {}", e))?;
            let rv = largeint::i128_from_be_bytes(r.value(target_idx))
                .map_err(|e| format!("map key LARGEINT decode failed: {}", e))?;
            Ok(lv == rv)
        }
        other => Err(format!("map key compare unsupported type: {:?}", other)),
    }
}

pub struct MapLookupInputs<'a> {
    pub map: &'a MapArray,
    key_arr: &'a ArrayRef,
    check_arr: Option<&'a BooleanArray>,
}
impl<'a> MapLookupInputs<'a> {
    /// Original carrier checks, in their original order after all child evals.
    pub fn new(
        map_arr: &'a ArrayRef,
        key_arr: &'a ArrayRef,
        check_arr: Option<&'a ArrayRef>,
    ) -> Result<Self, String> {
        let map = map_arr
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| format!("element_at expects MapArray, got {:?}", map_arr.data_type()))?;
        let check_arr = check_arr
            .map(|a| {
                a.as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| "element_at check flag must be BOOLEAN".to_string())
            })
            .transpose()?;
        Ok(Self {
            map,
            key_arr,
            check_arr,
        })
    }
    /// This is the v1 array-layout contract, not a guess about selected mappings.
    pub fn validate_legacy_lengths(&self) -> Result<(), String> {
        if self.key_arr.len() != 1 && self.key_arr.len() != self.map.len() {
            return Err(format!(
                "element_at key length mismatch: map rows={}, key rows={}",
                self.map.len(),
                self.key_arr.len()
            ));
        }
        if let Some(flags) = self.check_arr
            && flags.len() != 1
            && flags.len() != self.map.len()
        {
            return Err(format!(
                "element_at check flag length mismatch: map rows={}, check rows={}",
                self.map.len(),
                flags.len()
            ));
        }
        Ok(())
    }
    pub fn legacy_rows(&self, row: usize) -> LookupRows {
        LookupRows {
            map: row,
            key: row_index(row, self.key_arr.len()),
            check: self.check_arr.map(|flags| row_index(row, flags.len())),
        }
    }
}
#[derive(Clone, Copy)]
pub struct LookupRows {
    pub map: usize,
    pub key: usize,
    pub check: Option<usize>,
}
fn observe<E>(
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
    event: CollectionObservation,
) -> Result<(), MapLookupFailure<E>> {
    observer(event).map_err(MapLookupFailure::Control)
}
/// Each caller supplies its real argument mapping, never a type-derived default.
pub fn lookup_observed<E>(
    inputs: &MapLookupInputs<'_>,
    selection: crate::Selection<'_>,
    mut rows: impl FnMut(usize, usize) -> Result<LookupRows, E>,
    output_type: Option<&DataType>,
    mut before_take: impl FnMut(&dyn Array, &[Option<u32>]) -> Result<(), E>,
    mut row_error: impl FnMut(usize, String) -> Result<(), String>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, MapLookupFailure<E>> {
    let map = inputs.map;
    let key_arr = inputs.key_arr;
    let check_arr = inputs.check_arr;
    let keys = map.keys();
    let values = map.values();
    let offsets = map.value_offsets();
    if values.is_empty() {
        if let Some(flags) = check_arr {
            for (ordinal, row) in selection.iter().enumerate() {
                observe(observer, CollectionObservation::Step)?;
                let row = rows(ordinal, row).map_err(MapLookupFailure::Control)?;
                let check_idx = row.check.expect("check_arr exists when check_idx exists");
                let strict = !flags.is_null(check_idx) && flags.value(check_idx);
                if strict && !map.is_null(row.map) {
                    row_error(ordinal, "Key not present in map".to_string())
                        .map_err(MapLookupFailure::Data)?;
                }
            }
        }
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let out = arrow_array::new_null_array(values.data_type(), selection.len());
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        let out = cast_output(out, output_type).map_err(|cause| {
            MapLookupFailure::Data(format!("element_at: failed to cast output: {cause}"))
        })?;
        observe(observer, CollectionObservation::OpaqueBoundary)?;
        return Ok(out);
    }
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let mut indices = Vec::with_capacity(selection.len());
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    'rows: for (ordinal, row) in selection.iter().enumerate() {
        observe(observer, CollectionObservation::Step)?;
        let row = rows(ordinal, row).map_err(MapLookupFailure::Control)?;
        let key_idx = row.key;
        let check_idx = row.check;
        let check_out_of_bounds = check_idx
            .map(|idx| {
                let flags = check_arr.expect("check_arr exists when check_idx exists");
                !flags.is_null(idx) && flags.value(idx)
            })
            .unwrap_or(false);
        if map.is_null(row.map) {
            indices.push(None);
            continue;
        }
        let start = offsets[row.map] as usize;
        let end = offsets[row.map + 1] as usize;
        let mut found = None;
        if key_arr.is_null(key_idx) {
            for i in (start..end).rev() {
                observe(observer, CollectionObservation::Step)?;
                if keys.is_null(i) {
                    found = Some(i as u32);
                    break;
                }
            }
        } else {
            for i in (start..end).rev() {
                observe(observer, CollectionObservation::Step)?;
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                let comparison = compare_keys_at(keys, i, key_arr, key_idx);
                observe(observer, CollectionObservation::OpaqueBoundary)?;
                match comparison {
                    Ok(true) => {
                        found = Some(i as u32);
                        break;
                    }
                    Ok(false) => {}
                    Err(message) => {
                        row_error(ordinal, message).map_err(MapLookupFailure::Data)?;
                        indices.push(None);
                        continue 'rows;
                    }
                }
            }
        }
        if found.is_none() && check_out_of_bounds {
            row_error(ordinal, "Key not present in map".to_string())
                .map_err(MapLookupFailure::Data)?;
        }
        if let Some(v) = found {
            indices.push(Some(v));
        } else {
            indices.push(None);
        }
    }
    before_take(values.as_ref(), &indices).map_err(MapLookupFailure::Control)?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let indices = UInt32Array::from(indices);
    let out =
        take(values.as_ref(), &indices, None).map_err(|e| MapLookupFailure::Data(e.to_string()))?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let out = apply_indices_nulls(&out, &indices);
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    let out = cast_output(out, output_type).map_err(|cause| {
        MapLookupFailure::Data(format!("element_at: failed to cast output: {cause}"))
    })?;
    observe(observer, CollectionObservation::OpaqueBoundary)?;
    Ok(out)
}
fn apply_indices_nulls(out: &ArrayRef, indices: &UInt32Array) -> ArrayRef {
    // NullArray already represents NULL at every position and cannot carry a
    // physical validity buffer. The lookup still ran before this storage repair.
    if out.data_type() == &DataType::Null {
        return out.clone();
    }
    let Some(idx_nulls) = indices.nulls() else {
        return out.clone();
    };
    if idx_nulls.null_count() == 0 {
        return out.clone();
    }
    let combined_nulls = match out.nulls() {
        Some(existing) => arrow_buffer::NullBuffer::union(Some(existing), Some(idx_nulls))
            .expect("combined null buffer is always Some"),
        None => idx_nulls.clone(),
    };
    let data = out.to_data().into_builder().nulls(Some(combined_nulls));
    arrow_array::make_array(unsafe { data.build_unchecked() })
}
