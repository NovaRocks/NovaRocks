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

//! Required encoded children use logical NULL on active ancestor rows.
//! Arrow's safe ArrayData validation checks physical child masks, whereas its
//! high-level container constructors also check dictionary value NULLs.

#[cfg(test)]
use arrow::array::{Array, ArrayRef, DictionaryArray, ListArray, MapArray, StructArray};
#[cfg(test)]
use arrow::datatypes::DataType;
#[cfg(test)]
use arrow::datatypes::Int32Type;
#[cfg(test)]
use novarocks_type_contract::ValueKind;
#[cfg(test)]
use novarocks_type_contract::ValueTypeRef;

#[cfg(test)]
use super::typed_array_normalization::ArrayNormalizationStep;

pub(super) use novarocks_functions::array_projection::validate_required_dictionary_children;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
    use crate::exec::pipeline::operator::input_construction::{
        ConstructionFailure, try_runtime_input_construction_factory,
    };
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeErrorState;
    use arrow::array::{ArrayData, Int32Array, StringArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Field;
    use novarocks_type_contract::{
        TypedValueBinding, ValueField, ValueLayout, ValueTypeStorageLifetime,
    };
    use novarocks_types::SlotId;
    use std::sync::Arc;

    fn dictionary(keys: Vec<i32>) -> ArrayRef {
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(keys),
                Arc::new(StringArray::from(vec![Some("kept"), None, None])),
            )
            .unwrap(),
        )
    }

    fn required_struct(keys: Vec<i32>, mask: Option<Vec<bool>>) -> (ArrayRef, ValueTypeRef) {
        let child = dictionary(keys);
        let fields = vec![Arc::new(Field::new(
            "payload",
            child.data_type().clone(),
            false,
        ))];
        let nulls = mask.map(NullBuffer::from);
        if nulls.is_none() && child.logical_null_count() != 0 {
            assert!(
                StructArray::try_new(fields.clone().into(), vec![child.clone()], None).is_err()
            );
        }
        // Default Arrow validation is deliberately retained. Its physical
        // child-mask check admits value NULLs behind valid dictionary keys.
        let data = ArrayData::builder(DataType::Struct(fields.into()))
            .len(child.len())
            .nulls(nulls)
            .child_data(vec![child.to_data()])
            .build()
            .unwrap();
        data.validate_full().unwrap();
        let array: ArrayRef = Arc::new(StructArray::from(data));
        let storage = ValueTypeStorageLifetime::pure();
        let value = ValueTypeRef::try_new(
            ValueKind::Struct(
                vec![ValueField::new(
                    "payload",
                    ValueTypeRef::try_new(ValueKind::String, false, storage.clone()).unwrap(),
                    storage.clone(),
                )]
                .into_boxed_slice(),
            ),
            true,
            storage,
        )
        .unwrap();
        (array, value)
    }

    fn schema(array: &ArrayRef, value: ValueTypeRef) -> ChunkSchemaRef {
        let storage = ValueTypeStorageLifetime::pure();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(array.data_type(), storage.clone()).unwrap(),
        )
        .unwrap();
        Arc::new(
            ChunkSchema::try_new_typed(
                vec![
                    ChunkSlotSchema::try_new_typed(
                        SlotId::new(1),
                        Arc::new(Field::new("value", array.data_type().clone(), true)),
                        binding,
                        None,
                        storage.clone(),
                    )
                    .unwrap(),
                ],
                storage,
            )
            .unwrap(),
        )
    }

    fn ingress(array: ArrayRef, value: ValueTypeRef) -> Result<Chunk, ConstructionFailure> {
        let schema = schema(&array, value);
        let factory = try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            MemTracker::new_root("required encoded child ingress"),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let owner = factory.begin_typed_construction(&schema).unwrap();
        owner
            .observe(
                0,
                &[],
                ArrayNormalizationStep::CreateChunkColumns { channels: 1 },
            )
            .unwrap();
        let rows = array.len();
        Chunk::try_new_typed_with_columns_owned_observed(
            schema,
            vec![array],
            rows,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )
    }

    fn assert_required_null(error: &ConstructionFailure) {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_value_arrays::{ValueError, ValueViolation};
        let expected = ValueError::Invariant(ValueViolation::RequiredNull);
        assert_eq!(error.failure_fact(), expected.failure_fact());
        let mut source: &dyn std::error::Error = error;
        loop {
            if let Some(original) = source.downcast_ref::<ValueError>() {
                assert!(matches!(
                    original,
                    ValueError::Invariant(ValueViolation::RequiredNull)
                ));
                assert_eq!(original.failure_fact(), expected.failure_fact());
                break;
            }
            source = source
                .source()
                .expect("the original value required-child cause");
        }
    }

    #[test]
    fn safe_array_data_required_dictionary_child_is_rejected_at_actual_typed_ingress() {
        let (array, value) = required_struct(vec![1, 0], None);
        let error = ingress(array, value).unwrap_err();
        assert_required_null(&error);
    }

    #[test]
    fn parent_null_mask_slices_and_unused_null_values_keep_required_child_declaration() {
        let (masked, value) = required_struct(vec![1, 0], Some(vec![false, true]));
        for array in [masked.clone(), masked.slice(0, 1), masked.slice(1, 1)] {
            let output = ingress(array.clone(), value.clone()).unwrap();
            assert!(Arc::ptr_eq(&array, &output.columns()[0]));
            assert!(
                !output.chunk_schema().slots()[0]
                    .typed_binding()
                    .value_type()
                    .child_type_at(0)
                    .unwrap()
                    .nullable()
            );
        }
        let (array, value) = required_struct(vec![0, 0], None);
        assert!(ingress(array, value).is_ok());
    }

    #[test]
    fn list_offsets_select_only_active_required_encoded_rows() {
        let storage = ValueTypeStorageLifetime::pure();
        let child = dictionary(vec![1, 0, 1]);
        let field = Arc::new(Field::new("item", child.data_type().clone(), false));
        let offsets = OffsetBuffer::new(vec![0i32, 1, 2, 3].into());
        let data = ArrayData::builder(DataType::List(field))
            .len(3)
            .buffers(vec![offsets.inner().inner().clone()])
            .child_data(vec![child.to_data()])
            .nulls(Some(NullBuffer::from(vec![false, true, true])))
            .build()
            .unwrap();
        data.validate_full().unwrap();
        let array: ArrayRef = Arc::new(ListArray::from(data));
        let value = ValueTypeRef::try_new(
            ValueKind::List(
                ValueTypeRef::try_new(ValueKind::String, false, storage.clone()).unwrap(),
            ),
            true,
            storage,
        )
        .unwrap();
        assert!(ingress(array.slice(0, 2), value.clone()).is_ok());
        let error = ingress(array.slice(1, 2), value).unwrap_err();
        assert_required_null(&error);
    }

    #[test]
    fn dirty_roles_and_metadata_do_not_change_active_null_admission_or_payload_buffers() {
        let storage = ValueTypeStorageLifetime::pure();
        let child = dictionary(vec![1, 0, 1]);
        let make_list = |name: &str, dirty: bool| {
            let field = Field::new(name, child.data_type().clone(), false);
            let field = if dirty {
                field.with_metadata(std::collections::HashMap::<String, String>::from([(
                    "provider.fact".to_owned(),
                    "retained".to_owned(),
                )]))
            } else {
                field
            };
            let offsets = OffsetBuffer::new(vec![0i32, 1, 2, 3].into());
            let data = ArrayData::builder(DataType::List(Arc::new(field)))
                .len(3)
                .buffers(vec![offsets.inner().inner().clone()])
                .child_data(vec![child.to_data()])
                .nulls(Some(NullBuffer::from(vec![false, true, true])))
                .build()
                .unwrap();
            data.validate_full().unwrap();
            Arc::new(ListArray::from(data)) as ArrayRef
        };
        let value = ValueTypeRef::try_new(
            ValueKind::List(
                ValueTypeRef::try_new(ValueKind::String, false, storage.clone()).unwrap(),
            ),
            true,
            storage,
        )
        .unwrap();
        for original in [make_list("item", false), make_list("provider_item", true)] {
            for input in [original.slice(0, 2), original.slice(1, 1)] {
                let source = input.as_any().downcast_ref::<ListArray>().unwrap();
                let key_pointer = source.values().to_data().buffers()[0].as_ptr();
                let output = ingress(input.clone(), value.clone()).unwrap();
                let actual = output.columns()[0]
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                assert_eq!(actual.value_offsets(), source.value_offsets());
                for row in 0..source.len() {
                    assert_eq!(actual.is_null(row), source.is_null(row));
                }
                if source.null_count() != 0 {
                    assert_eq!(actual.nulls(), source.nulls());
                }
                // Default ArrayData validation removes an all-valid sliced
                // bitmap; it preserves the row NULL law without copying it.
                assert_eq!(actual.values().to_data().buffers()[0].as_ptr(), key_pointer);
                let DataType::List(field) = actual.data_type() else {
                    unreachable!()
                };
                assert_eq!(field.name(), "item");
                assert!(field.metadata().as_arc().is_none());
                let DataType::List(source_field) = input.data_type() else {
                    unreachable!()
                };
                if source_field.metadata().as_arc().is_some() {
                    assert_eq!(source_field.metadata()["provider.fact"], "retained");
                    assert!(
                        !Arc::ptr_eq(source.values(), actual.values()),
                        "validated ArrayData reconstruction shares buffers, not child wrapper identity"
                    );
                } else {
                    assert!(Arc::ptr_eq(&input, &output.columns()[0]));
                }
            }
            let error = ingress(original.slice(1, 2), value.clone()).unwrap_err();
            assert_required_null(&error);
        }
    }

    #[test]
    fn map_offsets_parent_nulls_and_provider_roles_keep_required_value_law() {
        let values = dictionary(vec![1, 0, 1]);
        let fields = vec![
            Arc::new(Field::new("provider_key", DataType::Int32, false)),
            Arc::new(Field::new(
                "provider_value",
                values.data_type().clone(),
                false,
            )),
        ];
        let entries_type = DataType::Struct(fields.into());
        let entries = ArrayData::builder(entries_type.clone())
            .len(3)
            .child_data(vec![
                Int32Array::from(vec![1, 2, 3]).to_data(),
                values.to_data(),
            ])
            .build()
            .unwrap();
        let offsets = OffsetBuffer::new(vec![0i32, 1, 2, 3].into());
        let data = ArrayData::builder(DataType::Map(
            Arc::new(Field::new("provider_entries", entries_type, false)),
            false,
        ))
        .len(3)
        .buffers(vec![offsets.inner().inner().clone()])
        .child_data(vec![entries])
        .nulls(Some(NullBuffer::from(vec![false, true, true])))
        .build()
        .unwrap();
        data.validate_full().unwrap();
        let source: ArrayRef = Arc::new(MapArray::from(data));
        let storage = ValueTypeStorageLifetime::pure();
        let value = ValueTypeRef::try_new(
            ValueKind::Map {
                key: ValueTypeRef::try_new(
                    ValueKind::Scalar(DataType::Int32),
                    false,
                    storage.clone(),
                )
                .unwrap(),
                value: ValueTypeRef::try_new(ValueKind::String, false, storage.clone()).unwrap(),
            },
            true,
            storage,
        )
        .unwrap();
        for input in [source.slice(0, 2), source.slice(1, 1)] {
            let output = ingress(input.clone(), value.clone()).unwrap();
            let actual = output.columns()[0]
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap();
            let input = input.as_any().downcast_ref::<MapArray>().unwrap();
            assert_eq!(actual.value_offsets(), input.value_offsets());
            assert_eq!(
                actual.values().to_data().buffers()[0].as_ptr(),
                input.values().to_data().buffers()[0].as_ptr()
            );
            assert_eq!(actual.entries().fields()[0].name(), "key");
            assert_eq!(actual.entries().fields()[1].name(), "value");
            assert!(!actual.entries().fields()[1].is_nullable());
        }
        let failure = ingress(source.slice(1, 2), value).unwrap_err();
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(&failure),
            novarocks_failure_contract::FailureProjection::failure_fact(
                &novarocks_value_arrays::ValueError::Invariant(
                    novarocks_value_arrays::ValueViolation::RequiredNull
                )
            )
        );
    }

    #[test]
    fn every_borrowed_active_check_preserves_the_first_caller_refusal() {
        let (array, value) = required_struct(vec![1, 0], Some(vec![false, true]));
        let original = crate::runtime::fragment::ExecutionFailure::from(
            novarocks_functions::KernelFailure::Cancelled,
        );
        let mut calls = 0;
        validate_required_dictionary_children::<crate::runtime::fragment::ExecutionFailure, _>(
            &array,
            &TypedValueBinding::try_new(
                value.clone(),
                ValueLayout::try_new(array.data_type(), ValueTypeStorageLifetime::pure()).unwrap(),
            )
            .unwrap(),
            &mut |_| {
                calls += 1;
                Ok(())
            },
        )
        .unwrap();
        assert!(calls > 2);
        for stop in 1..=calls {
            let mut observed = 0;
            let error = validate_required_dictionary_children::<
                crate::runtime::fragment::ExecutionFailure,
                _,
            >(
                &array,
                &TypedValueBinding::try_new(
                    value.clone(),
                    ValueLayout::try_new(array.data_type(), ValueTypeStorageLifetime::pure())
                        .unwrap(),
                )
                .unwrap(),
                &mut |_| {
                    observed += 1;
                    if observed == stop {
                        Err(original.clone())
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
            assert_eq!(error, original);
            assert_eq!(observed, stop);
        }
    }
}
