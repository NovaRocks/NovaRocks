// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The original Chunk borrowed key-safety/root logical-NULL traversal.
use super::{ArrayNormalizationStep, ArrayProjectionFailure, ArrayProjectionViolation};
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, Int32Array, LargeListArray, ListArray,
    MapArray, StructArray, UnionArray,
};
use arrow_schema::DataType;
pub fn validate_encoded_keys_and_root_nulls_observed<E, F>(
    source: &ArrayRef,
    observe: &mut F,
    depth: usize,
    nodes: &mut usize,
) -> Result<bool, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    observe(ArrayNormalizationStep::InspectType(source.data_type()))?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(E::from(ArrayProjectionFailure::ValueType(
            novarocks_type_contract::ValueTypeError::TooDeep,
        )));
    }
    *nodes = nodes.checked_add(1).ok_or_else(|| {
        E::from(ArrayProjectionFailure::ValueType(
            novarocks_type_contract::ValueTypeError::TooManyNodes,
        ))
    })?;
    let mut child = |array: &ArrayRef| {
        validate_encoded_keys_and_root_nulls_observed(array, observe, depth + 1, nodes).map(|_| ())
    };
    match source.data_type() {
        DataType::Dictionary(_, _) => {
            let dictionary = source
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .ok_or_else(|| {
                    E::from(ArrayProjectionFailure::Contract(
                        ArrayProjectionViolation::NonInt32Dictionary,
                    ))
                })?;
            if !matches!(
                dictionary.values().data_type(),
                DataType::Utf8 | DataType::LargeUtf8
            ) {
                return Err(E::from(ArrayProjectionFailure::Contract(
                    ArrayProjectionViolation::NonStringDictionary,
                )));
            }
            return validate_dictionary_rows_observed(
                source,
                dictionary.keys(),
                dictionary.values(),
                observe,
            );
        }
        DataType::Struct(_) => {
            let array = source
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    E::from(ArrayProjectionFailure::Contract(
                        ArrayProjectionViolation::PhysicalWrapper,
                    ))
                })?;
            for array in array.columns() {
                child(array)?;
            }
        }
        DataType::List(_) => child(
            source
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    E::from(ArrayProjectionFailure::Contract(
                        ArrayProjectionViolation::PhysicalWrapper,
                    ))
                })?
                .values(),
        )?,
        DataType::LargeList(_) => child(
            source
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    E::from(ArrayProjectionFailure::Contract(
                        ArrayProjectionViolation::PhysicalWrapper,
                    ))
                })?
                .values(),
        )?,
        DataType::FixedSizeList(_, _) => child(
            source
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    E::from(ArrayProjectionFailure::Contract(
                        ArrayProjectionViolation::PhysicalWrapper,
                    ))
                })?
                .values(),
        )?,
        DataType::Map(_, _) => {
            let array = source.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                E::from(ArrayProjectionFailure::Contract(
                    ArrayProjectionViolation::PhysicalWrapper,
                ))
            })?;
            for array in array.entries().columns() {
                child(array)?;
            }
        }
        DataType::Union(fields, _) => {
            let array = source
                .as_any()
                .downcast_ref::<UnionArray>()
                .ok_or_else(|| {
                    E::from(ArrayProjectionFailure::Contract(
                        ArrayProjectionViolation::PhysicalWrapper,
                    ))
                })?;
            for (id, _) in fields.iter() {
                child(array.child(id))?;
            }
        }
        _ => {}
    }
    Ok(source.null_count() != 0)
}

/// Borrowed row validation is shared by nullable and required ingress. It
/// checks all visible keys, including keys after an earlier logical NULL.
/// No logical_nulls bitmap is created and unused dictionary values are ignored.
pub fn validate_dictionary_rows_observed<E, F>(
    source: &ArrayRef,
    keys: &Int32Array,
    values: &ArrayRef,
    observe: &mut F,
) -> Result<bool, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    let mut root_has_nulls = false;
    for start in (0..keys.len()).step_by(1024) {
        let end = keys.len().min(start.saturating_add(1024));
        observe(ArrayNormalizationStep::InspectDictionaryRows {
            source,
            row_start: start,
            row_count: end - start,
        })?;
        for row in start..end {
            if keys.is_null(row) {
                root_has_nulls = true;
                continue;
            }
            let index = usize::try_from(keys.value(row))
                .ok()
                .filter(|index| *index < values.len())
                .ok_or_else(|| {
                    E::from(ArrayProjectionFailure::Contract(
                        ArrayProjectionViolation::DictionaryKeyBounds,
                    ))
                })?;
            root_has_nulls |= values.is_null(index);
        }
    }
    Ok(root_has_nulls)
}
