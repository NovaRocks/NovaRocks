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

//! Explicit fixture ingress for metadata-only Arrow wrapper normalization.
//! The caller supplies the already authored complete semantic binding.

use crate::FunctionValueType;
use arrow_array::{ArrayRef, make_array};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};

pub(crate) fn array_for_binding(
    source: &ArrayRef,
    target: &FunctionValueType,
) -> Result<ArrayRef, ArrowError> {
    if source.data_type() == target.data_type() {
        return Ok(source.clone());
    }
    Ok(make_array(rebind(source.to_data(), target.data_type())?))
}
fn rebind(source: ArrayData, target: &DataType) -> Result<ArrayData, ArrowError> {
    if source.data_type() == target {
        return Ok(source);
    }
    let children: Vec<&DataType> = match (source.data_type(), target) {
        (DataType::List(_), DataType::List(item))
        | (DataType::LargeList(_), DataType::LargeList(item)) => vec![item.data_type()],
        (DataType::FixedSizeList(_, source_len), DataType::FixedSizeList(item, target_len)) if source_len == target_len => vec![item.data_type()],
        (DataType::Struct(source), DataType::Struct(target)) if source.len() == target.len() => target.iter().map(|field| field.data_type()).collect(),
        (DataType::Map(_, source_sorted), DataType::Map(entries, target_sorted)) if source_sorted == target_sorted => vec![entries.data_type()],
        (DataType::Union(source, sm), DataType::Union(target, tm)) if sm == tm && source.len() == target.len() && source.iter().zip(target.iter()).all(|((a,_),(b,_))| a == b) => target.iter().map(|(_, field)| field.data_type()).collect(),
        _ => return Err(ArrowError::SchemaError("fixture ingress can only rebind metadata wrappers; encoding conversion requires an explicit owner".into())),
    };
    if children.len() != source.child_data().len() {
        return Err(ArrowError::SchemaError(
            "fixture child layout differs from authored binding".into(),
        ));
    }
    let child_data = source
        .child_data()
        .iter()
        .cloned()
        .zip(children)
        .map(|(data, target)| rebind(data, target))
        .collect::<Result<Vec<_>, _>>()?;
    source
        .into_builder()
        .data_type(target.clone())
        .child_data(child_data)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Int8Type;
    use arrow_array::{Array, DictionaryArray, Int8Array, Int64Array, ListArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::Field;
    use std::sync::Arc;
    #[test]
    fn metadata_ingress_preserves_slice_offsets_nulls_and_original_payload_buffers() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(11), None, Some(33), Some(44)]));
        let source: ArrayRef = Arc::new(ListArray::new(
            Arc::new(
                Field::new("external_item", DataType::Int64, true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider-id".into(),
                        "external-owner".into(),
                    )]),
                ),
            ),
            OffsetBuffer::new(vec![0, 1, 3, 4].into()),
            values.clone(),
            Some(NullBuffer::from(vec![true, false, true])),
        ));
        let source = source.slice(1, 2);
        let target = crate::builtin::signature::declared_list(
            &FunctionValueType::new(DataType::Int64, true),
            true,
        )
        .unwrap();
        let result = array_for_binding(&source, &target).unwrap();
        let source = source.as_any().downcast_ref::<ListArray>().unwrap();
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(source.value_offsets(), result.value_offsets());
        assert_eq!(source.nulls(), result.nulls());
        assert_eq!(
            source.values().to_data().buffers()[0].as_ptr(),
            result.values().to_data().buffers()[0].as_ptr()
        );
        assert_eq!(source.len(), result.len());
        assert_eq!(source.value(1).to_data(), result.value(1).to_data());
        assert_eq!(result.data_type(), target.data_type());
    }
    #[test]
    fn metadata_ingress_refuses_implicit_dictionary_decode() {
        let source: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0)]),
                Arc::new(Int64Array::from(vec![Some(7)])),
            )
            .unwrap(),
        );
        assert!(
            array_for_binding(&source, &FunctionValueType::new(DataType::Int64, true)).is_err()
        );
    }
}
