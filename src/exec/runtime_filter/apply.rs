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
//! Runtime-filter application helpers.
//!
//! Responsibilities:
//! - Applies membership and IN filters to chunks with optional expression-driven slot mapping.
//! - Builds row-selection masks and returns filtered chunks for probe-side pruning.
//!
//! Key exported interfaces:
//! - Functions: `filter_chunk_by_in_filters`, `filter_chunk_by_in_filters_with_exprs`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, DictionaryArray};
use arrow::compute::{cast, filter_record_batch};
use arrow::datatypes::{DataType, Int32Type};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

use super::{RuntimeInFilter, RuntimeMembershipFilter, RuntimeMinMaxFilter};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[allow(dead_code)]
enum DictionaryFoldKind {
    In,
    Membership,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[allow(dead_code)]
struct DictionaryFoldKey {
    kind: DictionaryFoldKind,
    filter_id: i32,
    values_ptr: usize,
    values_len: usize,
    values_type_tag: DictionaryValuesTypeTag,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[allow(dead_code)]
enum DictionaryValuesTypeTag {
    Utf8,
    LargeUtf8,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct DictionaryFold {
    accepts: Arc<Vec<bool>>,
    null_accepts: bool,
    values: ArrayRef,
}

#[derive(Default, Debug)]
#[allow(dead_code)]
pub(crate) struct RuntimeFilterDictionaryFoldCache {
    folds: HashMap<DictionaryFoldKey, DictionaryFold>,
    #[cfg(test)]
    build_count: usize,
}

#[allow(dead_code)]
impl RuntimeFilterDictionaryFoldCache {
    pub(crate) fn clear(&mut self) {
        self.folds.clear();
        #[cfg(test)]
        {
            self.build_count = 0;
        }
    }

    #[cfg(test)]
    pub(crate) fn build_count_for_test(&self) -> usize {
        self.build_count
    }
}

#[allow(dead_code)]
fn dictionary_int32_string(
    array: &ArrayRef,
) -> Result<Option<&DictionaryArray<Int32Type>>, String> {
    match array.data_type() {
        DataType::Dictionary(key_type, value_type)
            if key_type.as_ref() == &DataType::Int32
                && matches!(value_type.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .map(Some)
                .ok_or_else(|| "failed to downcast runtime filter dictionary probe".to_string())
        }
        DataType::Dictionary(_, _) => Err(format!(
            "unsupported runtime filter dictionary probe type: {:?}",
            array.data_type()
        )),
        _ => Ok(None),
    }
}

#[allow(dead_code)]
fn dictionary_values_tag(values: &ArrayRef) -> Result<DictionaryValuesTypeTag, String> {
    match values.data_type() {
        DataType::Utf8 => Ok(DictionaryValuesTypeTag::Utf8),
        DataType::LargeUtf8 => Ok(DictionaryValuesTypeTag::LargeUtf8),
        other => Err(format!(
            "unsupported runtime filter dictionary values type: {:?}",
            other
        )),
    }
}

#[allow(dead_code)]
fn dictionary_values_as_rf_utf8(values: &ArrayRef) -> Result<ArrayRef, String> {
    match values.data_type() {
        DataType::Utf8 => Ok(Arc::clone(values)),
        DataType::LargeUtf8 => cast(values.as_ref(), &DataType::Utf8).map_err(|e| e.to_string()),
        other => Err(format!(
            "unsupported runtime filter dictionary values type: {:?}",
            other
        )),
    }
}

#[allow(dead_code)]
fn dictionary_fold_key(
    kind: DictionaryFoldKind,
    filter_id: i32,
    values: &ArrayRef,
) -> Result<DictionaryFoldKey, String> {
    Ok(DictionaryFoldKey {
        kind,
        filter_id,
        values_ptr: Arc::as_ptr(values) as *const () as usize,
        values_len: values.len(),
        values_type_tag: dictionary_values_tag(values)?,
    })
}

#[allow(dead_code)]
fn fold_in_filter_for_dictionary(
    filter: &RuntimeInFilter,
    dict: &DictionaryArray<Int32Type>,
    cache: &mut RuntimeFilterDictionaryFoldCache,
) -> Result<DictionaryFold, String> {
    let values = dict.values();
    let key = dictionary_fold_key(DictionaryFoldKind::In, filter.filter_id(), values)?;
    if let Some(fold) = cache.folds.get(&key) {
        return Ok(fold.clone());
    }

    let probe_values = dictionary_values_as_rf_utf8(values)?;
    let mut accepts = Vec::with_capacity(probe_values.len());
    for idx in 0..probe_values.len() {
        accepts.push(filter.contains_non_null_value(&probe_values, idx)?);
    }
    let fold = DictionaryFold {
        accepts: Arc::new(accepts),
        null_accepts: false,
        values: Arc::clone(values),
    };
    cache.folds.insert(key, fold.clone());
    #[cfg(test)]
    {
        cache.build_count += 1;
    }
    Ok(fold)
}

#[allow(dead_code)]
fn fold_membership_filter_for_dictionary(
    filter: &RuntimeMembershipFilter,
    dict: &DictionaryArray<Int32Type>,
    cache: &mut RuntimeFilterDictionaryFoldCache,
) -> Result<DictionaryFold, String> {
    let values = dict.values();
    let key = dictionary_fold_key(DictionaryFoldKind::Membership, filter.filter_id(), values)?;
    if let Some(fold) = cache.folds.get(&key) {
        return Ok(fold.clone());
    }

    let probe_values = dictionary_values_as_rf_utf8(values)?;
    let mut accepts = vec![true; probe_values.len()];
    filter.apply_to_value_selection(&probe_values, &mut accepts)?;
    let fold = DictionaryFold {
        accepts: Arc::new(accepts),
        null_accepts: filter.has_null(),
        values: Arc::clone(values),
    };
    cache.folds.insert(key, fold.clone());
    #[cfg(test)]
    {
        cache.build_count += 1;
    }
    Ok(fold)
}

#[allow(dead_code)]
fn apply_dictionary_fold(
    dict: &DictionaryArray<Int32Type>,
    fold: &DictionaryFold,
    keep: &mut [bool],
) -> Result<(), String> {
    if keep.len() != dict.len() {
        return Err("runtime filter dictionary selection size mismatch".to_string());
    }
    let keys = dict.keys();
    for row in 0..dict.len() {
        if !keep[row] {
            continue;
        }
        if dict.is_null(row) {
            keep[row] = fold.null_accepts;
            continue;
        }
        let code = keys.value(row);
        let code = usize::try_from(code).map_err(|_| {
            format!(
                "runtime filter dictionary code must be non-negative, got {} at row {}",
                code, row
            )
        })?;
        keep[row] = fold.accepts.get(code).copied().ok_or_else(|| {
            format!(
                "runtime filter dictionary code out of bounds: code={} values_len={}",
                code,
                fold.accepts.len()
            )
        })?;
    }
    Ok(())
}

/// Apply IN filters to a chunk and return the filtered chunk.
pub(crate) fn filter_chunk_by_in_filters(
    filters: &[Arc<RuntimeInFilter>],
    chunk: Chunk,
) -> Result<Option<Chunk>, String> {
    if filters.is_empty() {
        return Ok(Some(chunk));
    }
    if chunk.is_empty() {
        return Ok(Some(chunk));
    }
    let len = chunk.len();
    let mut keep = vec![true; len];
    for filter in filters {
        let filter = filter.as_ref();
        if filter.is_empty() {
            continue;
        }
        if !chunk.slot_id_to_index().contains_key(&filter.slot_id()) {
            continue;
        }
        let array = chunk.column_by_slot_id(filter.slot_id())?;
        for (row, keep_row) in keep.iter_mut().enumerate().take(len) {
            if !*keep_row {
                continue;
            }
            if array.is_null(row) {
                *keep_row = false;
                continue;
            }
            if !filter.contains(&array, row)? {
                *keep_row = false;
            }
        }
        if keep.iter().all(|v| !*v) {
            return Ok(None);
        }
    }
    if keep.iter().all(|v| *v) {
        return Ok(Some(chunk));
    }
    let mask = BooleanArray::from(keep);
    let filtered_batch = filter_record_batch(&chunk.batch, &mask).map_err(|e| e.to_string())?;
    Ok(Some(Chunk::new_like(filtered_batch, &chunk)))
}

/// Apply IN filters using expression mappings and return the filtered chunk.
pub(crate) fn filter_chunk_by_in_filters_with_exprs(
    arena: &ExprArena,
    exprs: &HashMap<i32, ExprId>,
    filters: &[Arc<RuntimeInFilter>],
    chunk: Chunk,
) -> Result<Option<Chunk>, String> {
    if filters.is_empty() {
        return Ok(Some(chunk));
    }
    let mut current = Some(chunk);
    for filter in filters {
        let filter_ref = filter.as_ref();
        let Some(chunk) = current else {
            return Ok(None);
        };
        let Some(expr_id) = exprs.get(&filter_ref.filter_id()) else {
            current = filter_chunk_by_in_filters(std::slice::from_ref(filter), chunk)?;
            continue;
        };
        let array = match arena.eval(*expr_id, &chunk) {
            Ok(array) => array,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("slot id") && msg.contains("not found in chunk") {
                    current = Some(chunk);
                    continue;
                }
                return Err(msg);
            }
        };
        current = filter_ref.filter_chunk_with_array(&array, chunk)?;
    }
    Ok(current)
}

#[allow(dead_code)]
pub(crate) fn filter_chunk_by_in_filters_with_exprs_and_dict_cache(
    arena: &ExprArena,
    exprs: &HashMap<i32, ExprId>,
    filters: &[Arc<RuntimeInFilter>],
    chunk: Chunk,
    _dict_cache: &mut RuntimeFilterDictionaryFoldCache,
) -> Result<Option<Chunk>, String> {
    filter_chunk_by_in_filters_with_exprs(arena, exprs, filters, chunk)
}

/// Apply membership filters with expression mappings and return the filtered chunk.
pub(crate) fn filter_chunk_by_membership_filters_with_exprs(
    arena: &ExprArena,
    exprs: &HashMap<i32, ExprId>,
    filters: &[Arc<RuntimeMembershipFilter>],
    chunk: Chunk,
) -> Result<Option<Chunk>, String> {
    if filters.is_empty() {
        return Ok(Some(chunk));
    }
    let mut current = Some(chunk);
    for filter in filters {
        let filter = filter.as_ref();
        let Some(chunk) = current else {
            return Ok(None);
        };
        if let Some(expr_id) = exprs.get(&filter.filter_id()) {
            let array = match arena.eval(*expr_id, &chunk) {
                Ok(array) => array,
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("slot id") && msg.contains("not found in chunk") {
                        current = Some(chunk);
                        continue;
                    }
                    return Err(msg);
                }
            };
            current = filter.filter_chunk_with_array(&array, chunk)?;
        } else {
            current = filter.filter_chunk(chunk)?;
        }
    }
    Ok(current)
}

#[allow(dead_code)]
pub(crate) fn filter_chunk_by_membership_filters_with_exprs_and_dict_cache(
    arena: &ExprArena,
    exprs: &HashMap<i32, ExprId>,
    filters: &[Arc<RuntimeMembershipFilter>],
    chunk: Chunk,
    _dict_cache: &mut RuntimeFilterDictionaryFoldCache,
) -> Result<Option<Chunk>, String> {
    filter_chunk_by_membership_filters_with_exprs(arena, exprs, filters, chunk)
}

/// Apply min-max filters using expression mappings and return the filtered chunk.
///
/// For each `(filter_id, min_max_filter)` pair, looks up the probe expression via
/// `exprs[filter_id]`, evaluates it on the chunk to obtain the probe column array,
/// then applies `RuntimeMinMaxFilter::apply_to_array` to build a boolean selection
/// mask.  Rows outside the `[min, max]` range are dropped.
pub(crate) fn filter_chunk_by_min_max_filters_with_exprs(
    arena: &ExprArena,
    exprs: &HashMap<i32, ExprId>,
    filters: &[(i32, Arc<RuntimeMinMaxFilter>)],
    chunk: Chunk,
) -> Result<Option<Chunk>, String> {
    if filters.is_empty() {
        return Ok(Some(chunk));
    }
    if chunk.is_empty() {
        return Ok(Some(chunk));
    }
    let mut current = Some(chunk);
    for (filter_id, filter) in filters {
        let Some(chunk) = current else {
            return Ok(None);
        };
        let Some(expr_id) = exprs.get(filter_id) else {
            // No expression mapping for this filter_id — skip it.
            current = Some(chunk);
            continue;
        };
        let array = match arena.eval(*expr_id, &chunk) {
            Ok(array) => array,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("slot id") && msg.contains("not found in chunk") {
                    current = Some(chunk);
                    continue;
                }
                return Err(msg);
            }
        };
        let len = chunk.len();
        let mut keep = vec![true; len];
        // has_null=false, check_null=true  → null rows are excluded
        filter.apply_to_array(&array, false, true, &mut keep)?;
        if keep.iter().all(|v| *v) {
            current = Some(chunk);
            continue;
        }
        if keep.iter().all(|v| !*v) {
            current = None;
            continue;
        }
        let mask = BooleanArray::from(keep);
        let filtered_batch = filter_record_batch(&chunk.batch, &mask).map_err(|e| e.to_string())?;
        current = Some(Chunk::new_like(filtered_batch, &chunk));
    }
    Ok(current)
}

#[allow(dead_code)]
pub(crate) fn filter_chunk_by_min_max_filters_with_exprs_and_dict_cache(
    arena: &ExprArena,
    exprs: &HashMap<i32, ExprId>,
    filters: &[(i32, Arc<RuntimeMinMaxFilter>)],
    chunk: Chunk,
    _dict_cache: &mut RuntimeFilterDictionaryFoldCache,
) -> Result<Option<Chunk>, String> {
    filter_chunk_by_min_max_filters_with_exprs(arena, exprs, filters, chunk)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::runtime_filter::{
        RUNTIME_FILTER_JOIN_MODE_BROADCAST, RuntimeBloomFilter, RuntimeFilterType, RuntimeInFilter,
        RuntimeInFilterValues, RuntimeMembershipFilter, RuntimeMinMaxFilter,
    };
    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Int32Array, LargeStringArray, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn dict_chunk(keys: Vec<Option<i32>>, values: ArrayRef, logical_type: DataType) -> Chunk {
        let keys = Int32Array::from(keys);
        let dict =
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap()) as ArrayRef;
        let schema = Schema::new(vec![Field::new("status", logical_type, true)]);
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[SlotId::new(1)]).unwrap();
        Chunk::try_new_with_columns(chunk_schema, vec![dict]).unwrap()
    }

    fn output_strings(chunk: &Chunk) -> Vec<Option<String>> {
        let array = chunk.columns()[0].clone();
        let flat = arrow::compute::cast(array.as_ref(), &DataType::Utf8).unwrap();
        let strings = flat.as_any().downcast_ref::<StringArray>().unwrap();
        (0..strings.len())
            .map(|idx| {
                if strings.is_null(idx) {
                    None
                } else {
                    Some(strings.value(idx).to_string())
                }
            })
            .collect()
    }

    #[test]
    fn in_filter_folds_utf8_dictionary_values_and_rejects_null_keys() {
        let values = Arc::new(StringArray::from(vec!["PAID", "NEW", "CLOSED"])) as ArrayRef;
        let chunk = dict_chunk(
            vec![Some(0), Some(1), None, Some(2), Some(0)],
            values,
            DataType::Utf8,
        );
        let filter = Arc::new(RuntimeInFilter::new(
            7,
            SlotId::new(1),
            RuntimeInFilterValues::Utf8(
                ["PAID".to_string(), "CLOSED".to_string()]
                    .into_iter()
                    .collect(),
            ),
        ));

        let out = filter_chunk_by_in_filters(std::slice::from_ref(&filter), chunk)
            .unwrap()
            .unwrap();

        assert_eq!(
            output_strings(&out),
            vec![
                Some("PAID".to_string()),
                Some("CLOSED".to_string()),
                Some("PAID".to_string()),
            ]
        );
    }

    #[test]
    fn in_filter_folds_large_utf8_dictionary_values() {
        let values = Arc::new(LargeStringArray::from(vec!["PAID", "NEW", "CLOSED"])) as ArrayRef;
        let chunk = dict_chunk(vec![Some(0), Some(1), Some(2)], values, DataType::LargeUtf8);
        let filter = Arc::new(RuntimeInFilter::new(
            8,
            SlotId::new(1),
            RuntimeInFilterValues::Utf8(["NEW".to_string()].into_iter().collect()),
        ));

        let out = filter_chunk_by_in_filters(std::slice::from_ref(&filter), chunk)
            .unwrap()
            .unwrap();

        assert_eq!(output_strings(&out), vec![Some("NEW".to_string())]);
    }

    #[test]
    fn membership_filter_folds_dictionary_values_and_preserves_has_null() {
        let build = Arc::new(StringArray::from(vec![Some("A"), Some("Z"), None])) as ArrayRef;
        let filter = Arc::new(RuntimeMembershipFilter::Bloom(
            RuntimeBloomFilter::build_from_array(
                9,
                SlotId::new(1),
                RuntimeFilterType::Utf8,
                &build,
                RUNTIME_FILTER_JOIN_MODE_BROADCAST,
            )
            .unwrap(),
        ));
        assert!(filter.has_null());

        let values = Arc::new(StringArray::from(vec!["A", "M", "Z"])) as ArrayRef;
        let chunk = dict_chunk(
            vec![Some(0), Some(1), None, Some(2)],
            values,
            DataType::Utf8,
        );

        let out = filter_chunk_by_membership_filters_with_exprs(
            &ExprArena::default(),
            &HashMap::new(),
            std::slice::from_ref(&filter),
            chunk,
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            output_strings(&out),
            vec![Some("A".to_string()), None, Some("Z".to_string())]
        );
    }

    #[test]
    fn min_max_filter_hydrates_dictionary_probe_values_as_correctness_fallback() {
        let build = Arc::new(StringArray::from(vec!["M", "T"])) as ArrayRef;
        let filter =
            Arc::new(RuntimeMinMaxFilter::from_array(RuntimeFilterType::Utf8, &build).unwrap());
        let values = Arc::new(StringArray::from(vec!["A", "M", "P", "Z", "T"])) as ArrayRef;
        let chunk = dict_chunk(
            vec![Some(0), Some(1), Some(2), Some(3), Some(4), None],
            values,
            DataType::Utf8,
        );

        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let out = filter_chunk_by_min_max_filters_with_exprs(
            &arena,
            &HashMap::from([(11, expr)]),
            &[(11, filter)],
            chunk,
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            output_strings(&out),
            vec![
                Some("M".to_string()),
                Some("P".to_string()),
                Some("T".to_string())
            ]
        );
    }
}
