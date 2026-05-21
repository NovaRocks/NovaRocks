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
//
// map_value_count(col) -> Map<K, Int64>
//   result[v] = COUNT(rows with col = v); NULL col is skipped; empty -> empty map.
// map_value_count_signed(col, change_op: Int8) -> Map<K, Int64>
//   result[v] = SUM(change_op for rows with col = v); NULL col is skipped;
//   NULL change_op contributes 0 (defensive). Counts can go negative.
//
// These aggregates back the IVM-P5 detail-state path for MIN/MAX. They are
// designed so partial states can be merged across pipeline stages: the
// finalized state is a Map<K, Int64>, and merge sums per-key counts.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int8Array, MapArray, StructArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use arrow_buffer::OffsetBuffer;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use super::common::{
    AggScalarValue, build_scalar_array, compare_scalar_values, key_fingerprint, scalar_from_array,
};

pub(super) struct MapValueCountAgg;

/// Per-group accumulator for map_value_count / map_value_count_signed.
///
/// Keyed by a stable byte fingerprint of the AggScalarValue so we can
/// uniformly handle all supported scalar K. We retain a representative
/// AggScalarValue per fingerprint so finalize() can rebuild a typed Arrow
/// MapArray without re-decoding the fingerprint.
#[derive(Clone, Debug, Default)]
struct MapValueCountState {
    /// fingerprint -> (representative key, signed count)
    entries: HashMap<Vec<u8>, (AggScalarValue, i64)>,
}

impl AggregateFunction for MapValueCountAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type = input_type.ok_or_else(|| format!("{} input type missing", func.name))?;
        let kind = kind_from_name(func.name.as_str())
            .ok_or_else(|| format!("unsupported map_value_count function: {}", func.name))?;

        if input_is_intermediate {
            // Intermediate is the Map<K, Int64> finalized state.
            let output_type = func
                .types
                .as_ref()
                .and_then(|t| t.output_type.clone())
                .unwrap_or_else(|| input_type.clone());
            return Ok(AggSpec {
                kind,
                output_type,
                intermediate_type: input_type.clone(),
                input_arg_type: func.types.as_ref().and_then(|t| t.input_arg_type.clone()),
                count_all: false,
            });
        }

        // Update-stage input layout:
        //   map_value_count(col)            -> input_type is K (the column type)
        //   map_value_count_signed(col, op) -> input_type is Struct<col: K, op: Int8>
        //                                       (packed by lower::pack_struct_inputs)
        let key_field = match (&kind, input_type) {
            (AggKind::MapValueCount, ty) => Arc::new(Field::new("key", ty.clone(), false)),
            (AggKind::MapValueCountSigned, DataType::Struct(fields)) => {
                if fields.len() != 2 {
                    return Err(format!(
                        "map_value_count_signed expects 2 arguments, got struct with {} fields",
                        fields.len()
                    ));
                }
                Arc::new(Field::new("key", fields[0].data_type().clone(), false))
            }
            (AggKind::MapValueCountSigned, other) => {
                return Err(format!(
                    "map_value_count_signed expects struct input (col, change_op), got {:?}",
                    other
                ));
            }
            (other, _) => unreachable!("unexpected kind for map_value_count: {:?}", other),
        };
        let value_field = Arc::new(Field::new("value", DataType::Int64, true));

        let output_type = func
            .types
            .as_ref()
            .and_then(|t| t.output_type.clone())
            .unwrap_or_else(|| build_default_map_type(key_field.clone(), value_field.clone()));
        let intermediate_type = func
            .types
            .as_ref()
            .and_then(|t| t.intermediate_type.clone())
            .unwrap_or_else(|| output_type.clone());

        Ok(AggSpec {
            kind,
            output_type,
            intermediate_type,
            input_arg_type: func.types.as_ref().and_then(|t| t.input_arg_type.clone()),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::MapValueCount | AggKind::MapValueCountSigned => (
                std::mem::size_of::<MapValueCountState>(),
                std::mem::align_of::<MapValueCountState>(),
            ),
            other => unreachable!("unexpected kind for map_value_count: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "map_value_count input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "map_value_count merge input missing".to_string())?;
        let _ = arr
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "map_value_count merge input must be MapArray".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(
                ptr as *mut MapValueCountState,
                MapValueCountState::default(),
            );
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut MapValueCountState);
        }
    }

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("map_value_count batch input type mismatch".to_string());
        };

        match spec.kind {
            AggKind::MapValueCount => {
                // Single-column input: counts +1 per non-null row.
                update_unsigned(offset, state_ptrs, array)
            }
            AggKind::MapValueCountSigned => {
                // Struct<col, change_op:Int8> input.
                let struct_arr = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| "map_value_count_signed expects struct input".to_string())?;
                if struct_arr.num_columns() != 2 {
                    return Err(format!(
                        "map_value_count_signed expects 2 arguments, got {}",
                        struct_arr.num_columns()
                    ));
                }
                update_signed(offset, state_ptrs, struct_arr)
            }
            ref other => unreachable!("unexpected kind for map_value_count: {:?}", other),
        }
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("map_value_count merge input type mismatch".to_string());
        };
        let map_arr = array
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "map_value_count merge input must be MapArray".to_string())?;
        let key_arr = map_arr.keys().clone();
        let value_arr = map_arr.values().clone();
        let offsets = map_arr.value_offsets();

        for (row, &base) in state_ptrs.iter().enumerate() {
            if map_arr.is_null(row) {
                continue;
            }
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MapValueCountState) };
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            for idx in start..end {
                let Some(key) = scalar_from_array(&key_arr, idx)? else {
                    // NULL key is illegal in our finalized format; skip defensively.
                    continue;
                };
                let delta = match scalar_from_array(&value_arr, idx)? {
                    Some(AggScalarValue::Int64(v)) => v,
                    Some(_) => return Err("map_value_count merge value must be Int64".to_string()),
                    None => 0,
                };
                add_to_state(state, key, delta);
            }
        }
        Ok(())
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        let target_type = if output_intermediate {
            &spec.intermediate_type
        } else {
            &spec.output_type
        };
        let (map_field, field_defs, ordered) = parse_map_type(target_type)?;
        let key_type = field_defs[0].data_type();
        let value_type = field_defs[1].data_type();

        let mut key_values = Vec::<Option<AggScalarValue>>::new();
        let mut value_values = Vec::<Option<AggScalarValue>>::new();
        let mut offsets = Vec::with_capacity(group_states.len() + 1);
        offsets.push(0_i32);
        let mut current: i64 = 0;

        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const MapValueCountState) };
            // Sort entries by key for deterministic finalized output. We sort
            // by the value-domain ordering (compare_scalar_values), which is
            // the natural ordering for ints/floats/strings/dates/etc., not by
            // fingerprint byte order.
            let mut sorted: Vec<(&AggScalarValue, i64)> =
                state.entries.values().map(|(k, v)| (k, *v)).collect();
            sorted.sort_by(|a, b| {
                compare_scalar_values(a.0, b.0).unwrap_or(std::cmp::Ordering::Equal)
            });

            for (key, count) in sorted {
                key_values.push(Some(key.clone()));
                value_values.push(Some(AggScalarValue::Int64(count)));
                current += 1;
                if current > i32::MAX as i64 {
                    return Err("map_value_count offset overflow".to_string());
                }
            }
            offsets.push(current as i32);
        }

        let mut out_keys = build_scalar_array(key_type, key_values)?;
        let mut out_values = build_scalar_array(value_type, value_values)?;
        if out_keys.data_type() != field_defs[0].data_type() {
            out_keys = cast(&out_keys, field_defs[0].data_type())
                .map_err(|e| format!("map_value_count failed to cast output key: {}", e))?;
        }
        if out_values.data_type() != field_defs[1].data_type() {
            out_values = cast(&out_values, field_defs[1].data_type())
                .map_err(|e| format!("map_value_count failed to cast output value: {}", e))?;
        }

        let entries = StructArray::new(field_defs, vec![out_keys, out_values], None);
        let out = MapArray::try_new(
            map_field,
            OffsetBuffer::new(offsets.into()),
            entries,
            None,
            ordered,
        )
        .map_err(|e| format!("map_value_count: {}", e))?;
        Ok(Arc::new(out))
    }
}

fn kind_from_name(name: &str) -> Option<AggKind> {
    match name {
        "map_value_count" => Some(AggKind::MapValueCount),
        "map_value_count_signed" => Some(AggKind::MapValueCountSigned),
        _ => None,
    }
}

fn update_unsigned(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    array: &ArrayRef,
) -> Result<(), String> {
    for (row, &base) in state_ptrs.iter().enumerate() {
        let Some(key) = scalar_from_array(array, row)? else {
            continue;
        };
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MapValueCountState) };
        add_to_state(state, key, 1);
    }
    Ok(())
}

fn update_signed(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    struct_arr: &StructArray,
) -> Result<(), String> {
    let key_arr = struct_arr.column(0).clone();
    let op_arr_ref = struct_arr.column(1).clone();
    // change_op must be Int8 per the function contract.
    let op_arr = op_arr_ref
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| {
            format!(
                "map_value_count_signed change_op must be Int8, got {:?}",
                op_arr_ref.data_type()
            )
        })?;

    for (row, &base) in state_ptrs.iter().enumerate() {
        // A struct-level null row means "no contribution from this row" —
        // even if the inner key cell is non-null, treat the row as absent.
        // Without this, scalar_from_array would happily return the inner key
        // and we'd insert a phantom (key, 0) entry.
        if struct_arr.is_null(row) {
            continue;
        }
        let Some(key) = scalar_from_array(&key_arr, row)? else {
            continue;
        };
        // NULL change_op is treated as 0 (defensive).
        let delta = if op_arr.is_null(row) {
            0_i64
        } else {
            op_arr.value(row) as i64
        };
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MapValueCountState) };
        add_to_state(state, key, delta);
    }
    Ok(())
}

fn add_to_state(state: &mut MapValueCountState, key: AggScalarValue, delta: i64) {
    let fp = key_fingerprint(&key);
    state
        .entries
        .entry(fp)
        .and_modify(|(_, count)| {
            *count = count.saturating_add(delta);
        })
        .or_insert((key, delta));
}

fn parse_map_type(ty: &DataType) -> Result<(Arc<Field>, arrow::datatypes::Fields, bool), String> {
    let DataType::Map(field, ordered) = ty else {
        return Err(format!(
            "map_value_count output type must be MAP, got {:?}",
            ty
        ));
    };
    let DataType::Struct(fields) = field.data_type().clone() else {
        return Err("map_value_count map entries type must be STRUCT".to_string());
    };
    if fields.len() != 2 {
        return Err("map_value_count map entries type must have 2 fields".to_string());
    }
    Ok((field.clone(), fields, *ordered))
}

fn build_default_map_type(key_field: Arc<Field>, value_field: Arc<Field>) -> DataType {
    // Iceberg-rust convention: entries-struct field is named "key_value"
    // (iceberg-0.9 `DEFAULT_MAP_FIELD_NAME`) and the value field is nullable.
    // `map_value_count` backs the IVM-P5 MIN/MAX detail-state, whose output
    // lands directly in the Iceberg target sink — so the runtime MapArray
    // must already follow the Iceberg shape to avoid a field-name / null
    // mismatch when the sink re-annotates field IDs.
    DataType::Map(
        Arc::new(Field::new(
            "key_value",
            DataType::Struct(arrow::datatypes::Fields::from(vec![
                Arc::new(Field::new(
                    "key",
                    key_field.data_type().clone(),
                    key_field.is_nullable(),
                )),
                Arc::new(Field::new(
                    "value",
                    value_field.data_type().clone(),
                    value_field.is_nullable(),
                )),
            ])),
            false,
        )),
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Decimal128Array, Float64Array, Int8Array, Int64Array, Int64Builder, MapBuilder,
        StringArray, StructArray,
    };
    use std::mem::MaybeUninit;

    fn map_type_with_key(key_type: DataType) -> DataType {
        // Mirrors the iceberg-rust convention applied in
        // `build_default_map_type` (entries-field name `"key_value"`,
        // value field nullable).
        DataType::Map(
            Arc::new(Field::new(
                "key_value",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    Arc::new(Field::new("key", key_type, false)),
                    Arc::new(Field::new("value", DataType::Int64, true)),
                ])),
                false,
            )),
            false,
        )
    }

    fn build_spec(kind_name: &str, input_type: DataType) -> AggSpec {
        let map_ty = map_type_with_key(match input_type.clone() {
            DataType::Struct(fields) => fields[0].data_type().clone(),
            other => other,
        });
        let func = AggFunction {
            name: kind_name.to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(map_ty.clone()),
                output_type: Some(map_ty),
                input_arg_type: None,
            }),
        };
        MapValueCountAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap()
    }

    fn build_intermediate_spec(kind_name: &str, map_ty: DataType) -> AggSpec {
        let func = AggFunction {
            name: kind_name.to_string(),
            inputs: vec![],
            input_is_intermediate: true,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(map_ty.clone()),
                output_type: Some(map_ty.clone()),
                input_arg_type: None,
            }),
        };
        MapValueCountAgg
            .build_spec_from_type(&func, Some(&map_ty), true)
            .unwrap()
    }

    /// Wrapper that owns one MaybeUninit cell and the spec so callers can
    /// run multiple update / merge / finalize calls against a single state.
    struct StateCell {
        spec: AggSpec,
        cell: Box<MaybeUninit<MapValueCountState>>,
        initialized: bool,
    }

    impl StateCell {
        fn new(spec: AggSpec) -> Self {
            let mut cell = Box::new(MaybeUninit::<MapValueCountState>::uninit());
            MapValueCountAgg.init_state(&spec, cell.as_mut_ptr() as *mut u8);
            Self {
                spec,
                cell,
                initialized: true,
            }
        }

        fn ptr(&mut self) -> AggStatePtr {
            self.cell.as_mut_ptr() as AggStatePtr
        }

        fn update(&mut self, input: ArrayRef) {
            let n = input.len();
            let view = AggInputView::Any(&input);
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; n];
            MapValueCountAgg
                .update_batch(&self.spec, 0, &state_ptrs, &view)
                .unwrap();
        }

        fn merge(&mut self, input: ArrayRef) {
            let n = input.len();
            let view = AggInputView::Any(&input);
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; n];
            MapValueCountAgg
                .merge_batch(&self.spec, 0, &state_ptrs, &view)
                .unwrap();
        }

        fn finalize(&mut self) -> ArrayRef {
            let ptr = self.ptr();
            MapValueCountAgg
                .build_array(&self.spec, 0, &[ptr], false)
                .unwrap()
        }
    }

    impl Drop for StateCell {
        fn drop(&mut self) {
            if self.initialized {
                MapValueCountAgg.drop_state(&self.spec, self.cell.as_mut_ptr() as *mut u8);
                self.initialized = false;
            }
        }
    }

    fn collect_int_map(out: &ArrayRef) -> Vec<(i64, i64)> {
        let map = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.len(), 1);
        let start = map.value_offsets()[0] as usize;
        let end = map.value_offsets()[1] as usize;
        let keys = map.keys().as_any().downcast_ref::<Int64Array>().unwrap();
        let vals = map.values().as_any().downcast_ref::<Int64Array>().unwrap();
        (start..end)
            .map(|i| (keys.value(i), vals.value(i)))
            .collect()
    }

    #[test]
    fn test_map_value_count_three_distinct_int64_x5_each() {
        // Insert 3 distinct Int64 values 5 times each.
        let spec = build_spec("map_value_count", DataType::Int64);
        let mut cell = StateCell::new(spec);
        let values: Vec<i64> = (0..15).map(|i| (i % 3) as i64 + 10).collect();
        let arr = Arc::new(Int64Array::from(values)) as ArrayRef;
        cell.update(arr);
        let out = cell.finalize();
        let entries = collect_int_map(&out);
        assert_eq!(entries.len(), 3);
        // sorted ascending by key
        assert_eq!(entries, vec![(10, 5), (11, 5), (12, 5)]);
    }

    #[test]
    fn test_map_value_count_skips_null() {
        // mix including NULLs
        let spec = build_spec("map_value_count", DataType::Int64);
        let mut cell = StateCell::new(spec);
        let arr = Arc::new(Int64Array::from(vec![
            Some(1),
            None,
            Some(2),
            Some(1),
            None,
            Some(2),
            Some(2),
        ])) as ArrayRef;
        cell.update(arr);
        let out = cell.finalize();
        let entries = collect_int_map(&out);
        // NULLs are skipped; counts are over non-null rows.
        assert_eq!(entries, vec![(1, 2), (2, 3)]);
    }

    fn signed_input(keys: Vec<Option<i64>>, ops: Vec<Option<i8>>) -> ArrayRef {
        assert_eq!(keys.len(), ops.len());
        let key_arr = Arc::new(Int64Array::from(keys)) as ArrayRef;
        let op_arr = Arc::new(Int8Array::from(ops)) as ArrayRef;
        Arc::new(StructArray::new(
            arrow::datatypes::Fields::from(vec![
                Arc::new(Field::new("k", DataType::Int64, true)),
                Arc::new(Field::new("op", DataType::Int8, true)),
            ]),
            vec![key_arr, op_arr],
            None,
        )) as ArrayRef
    }

    #[test]
    fn test_map_value_count_signed_negative_for_unseen() {
        // update_signed(v, -1) on a value not previously seen -> count -1.
        let struct_ty = DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]));
        let spec = build_spec("map_value_count_signed", struct_ty);
        let mut cell = StateCell::new(spec);
        cell.update(signed_input(vec![Some(42)], vec![Some(-1)]));
        let out = cell.finalize();
        let entries = collect_int_map(&out);
        assert_eq!(entries, vec![(42, -1)]);
    }

    #[test]
    fn test_map_value_count_signed_keeps_zero_no_prune() {
        // +1 then -1 -> count 0 retained (no prune at this layer).
        let struct_ty = DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]));
        let spec = build_spec("map_value_count_signed", struct_ty);
        let mut cell = StateCell::new(spec);
        cell.update(signed_input(vec![Some(7)], vec![Some(1)]));
        cell.update(signed_input(vec![Some(7)], vec![Some(-1)]));
        let out = cell.finalize();
        let entries = collect_int_map(&out);
        // Phase 4 will prune; Phase 1 keeps the zero entry.
        assert_eq!(entries, vec![(7, 0)]);
    }

    #[test]
    fn test_map_value_count_signed_null_change_op_is_zero() {
        // NULL change_op contributes 0; key is otherwise tracked.
        let struct_ty = DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]));
        let spec = build_spec("map_value_count_signed", struct_ty);
        let mut cell = StateCell::new(spec);
        cell.update(signed_input(
            vec![Some(3), Some(3), Some(4)],
            vec![Some(1), None, Some(1)],
        ));
        let out = cell.finalize();
        let entries = collect_int_map(&out);
        assert_eq!(entries, vec![(3, 1), (4, 1)]);
    }

    fn intermediate_map(entries: Vec<Vec<(i64, i64)>>) -> ArrayRef {
        let mut builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        for row in entries {
            for (k, v) in row {
                builder.keys().append_value(k);
                builder.values().append_value(v);
            }
            builder.append(true).unwrap();
        }
        Arc::new(builder.finish()) as ArrayRef
    }

    #[test]
    fn test_merge_disjoint_keys_union() {
        let map_ty = map_type_with_key(DataType::Int64);
        let spec = build_intermediate_spec("map_value_count", map_ty);
        let mut cell = StateCell::new(spec);
        cell.merge(intermediate_map(vec![vec![(1, 3), (2, 5)]]));
        cell.merge(intermediate_map(vec![vec![(3, 2), (4, 7)]]));
        let out = cell.finalize();
        let entries = collect_int_map(&out);
        assert_eq!(entries, vec![(1, 3), (2, 5), (3, 2), (4, 7)]);
    }

    #[test]
    fn test_merge_overlapping_keys_sum() {
        let map_ty = map_type_with_key(DataType::Int64);
        let spec = build_intermediate_spec("map_value_count", map_ty);
        let mut cell = StateCell::new(spec);
        cell.merge(intermediate_map(vec![vec![(1, 3), (2, 5)]]));
        cell.merge(intermediate_map(vec![vec![(2, 4), (3, 1)]]));
        let out = cell.finalize();
        let entries = collect_int_map(&out);
        assert_eq!(entries, vec![(1, 3), (2, 9), (3, 1)]);
    }

    #[test]
    fn test_map_value_count_utf8_keys() {
        let spec = build_spec("map_value_count", DataType::Utf8);
        let mut cell = StateCell::new(spec);
        let arr = Arc::new(StringArray::from(vec![
            Some("b"),
            Some("a"),
            Some("b"),
            None,
            Some("c"),
            Some("a"),
        ])) as ArrayRef;
        cell.update(arr);
        let out = cell.finalize();
        let map = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(*map.keys().data_type(), DataType::Utf8);
        assert_eq!(*map.values().data_type(), DataType::Int64);
        let start = map.value_offsets()[0] as usize;
        let end = map.value_offsets()[1] as usize;
        let keys = map.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let vals = map.values().as_any().downcast_ref::<Int64Array>().unwrap();
        let got: Vec<(String, i64)> = (start..end)
            .map(|i| (keys.value(i).to_string(), vals.value(i)))
            .collect();
        assert_eq!(
            got,
            vec![
                ("a".to_string(), 2),
                ("b".to_string(), 2),
                ("c".to_string(), 1),
            ]
        );
    }

    #[test]
    fn test_map_value_count_float64_keys() {
        let spec = build_spec("map_value_count", DataType::Float64);
        let mut cell = StateCell::new(spec);
        let arr = Arc::new(Float64Array::from(vec![
            Some(1.5),
            Some(2.5),
            Some(1.5),
            Some(1.5),
        ])) as ArrayRef;
        cell.update(arr);
        let out = cell.finalize();
        let map = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(*map.keys().data_type(), DataType::Float64);
        assert_eq!(*map.values().data_type(), DataType::Int64);
        let start = map.value_offsets()[0] as usize;
        let end = map.value_offsets()[1] as usize;
        let keys = map.keys().as_any().downcast_ref::<Float64Array>().unwrap();
        let vals = map.values().as_any().downcast_ref::<Int64Array>().unwrap();
        let got: Vec<(f64, i64)> = (start..end)
            .map(|i| (keys.value(i), vals.value(i)))
            .collect();
        assert_eq!(got, vec![(1.5, 3), (2.5, 1)]);
    }

    #[test]
    fn test_map_value_count_decimal128_keys() {
        let decimal_ty = DataType::Decimal128(10, 2);
        let spec = build_spec("map_value_count", decimal_ty.clone());
        let mut cell = StateCell::new(spec);
        let arr = Arc::new(
            Decimal128Array::from(vec![Some(100_i128), Some(200), Some(100)])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ) as ArrayRef;
        cell.update(arr);
        let out = cell.finalize();
        let map = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert!(matches!(map.keys().data_type(), DataType::Decimal128(_, _)));
        assert_eq!(*map.values().data_type(), DataType::Int64);
        let start = map.value_offsets()[0] as usize;
        let end = map.value_offsets()[1] as usize;
        let keys = map
            .keys()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let vals = map.values().as_any().downcast_ref::<Int64Array>().unwrap();
        let got: Vec<(i128, i64)> = (start..end)
            .map(|i| (keys.value(i), vals.value(i)))
            .collect();
        assert_eq!(got, vec![(100, 2), (200, 1)]);
    }

    #[test]
    fn test_empty_group_finalizes_to_empty_map_not_null() {
        let spec = build_spec("map_value_count", DataType::Int64);
        let mut cell = StateCell::new(spec);
        let out = cell.finalize();
        let map = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.len(), 1);
        assert!(!map.is_null(0));
        assert_eq!(map.value_length(0), 0);
    }

    #[test]
    fn test_map_value_count_int8_round_trip() {
        // Drive update_unsigned with Int8 input. The accumulator widens to
        // AggScalarValue::Int64 internally; build_scalar_array must narrow
        // back to Int8 when finalizing against an Int8-keyed map type.
        let spec = build_spec("map_value_count", DataType::Int8);
        let mut cell = StateCell::new(spec);
        let arr = Arc::new(Int8Array::from(vec![1_i8, 2, 1, 3, 2, 1])) as ArrayRef;
        cell.update(arr);
        let out = cell.finalize();
        let map = out.as_any().downcast_ref::<MapArray>().unwrap();
        // Key type must round-trip back to Int8, not stay widened to Int64.
        assert_eq!(*map.keys().data_type(), DataType::Int8);
        assert_eq!(*map.values().data_type(), DataType::Int64);
        let start = map.value_offsets()[0] as usize;
        let end = map.value_offsets()[1] as usize;
        let keys = map.keys().as_any().downcast_ref::<Int8Array>().unwrap();
        let vals = map.values().as_any().downcast_ref::<Int64Array>().unwrap();
        let mut got: Vec<(i8, i64)> = (start..end)
            .map(|i| (keys.value(i), vals.value(i)))
            .collect();
        got.sort_by_key(|&(k, _)| k);
        assert_eq!(got, vec![(1_i8, 3), (2, 2), (3, 1)]);
    }

    #[test]
    fn test_map_value_count_signed_skips_struct_null_row() {
        // Build a row-nullable StructArray: row 0 is null at the struct level
        // but its inner key cell is non-null. Without the struct-level null
        // guard, update_signed would read the inner key and insert a phantom
        // (key, 0) entry. With the guard, the row is skipped entirely.
        let key_arr = Arc::new(Int64Array::from(vec![Some(7_i64), Some(8)])) as ArrayRef;
        let op_arr = Arc::new(Int8Array::from(vec![Some(1_i8), Some(1)])) as ArrayRef;
        let fields = arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]);
        let mut nulls = arrow_buffer::NullBufferBuilder::new(2);
        nulls.append_null(); // row 0: struct-level null (but inner key=7 is non-null)
        nulls.append_non_null(); // row 1: live struct, key=8 op=1
        let struct_arr = Arc::new(StructArray::new(
            fields,
            vec![key_arr, op_arr],
            nulls.finish(),
        )) as ArrayRef;
        // Sanity: row 0 is null at the struct level but its inner key cell is non-null.
        let raw_struct = struct_arr.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(raw_struct.is_null(0));
        assert!(!raw_struct.column(0).is_null(0));

        let struct_ty = DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]));
        let spec = build_spec("map_value_count_signed", struct_ty);
        let mut cell = StateCell::new(spec);
        cell.update(struct_arr);
        let out = cell.finalize();
        let entries = collect_int_map(&out);
        // Only the live row 1 contributes; the null struct row does NOT add (7, 0).
        assert_eq!(entries, vec![(8, 1)]);
    }
}
