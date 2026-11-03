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

//! Plain representation of a required decoded leaf under a retained typed
//! carrier. This is not an independent semantic projection of a raw child.

#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use arrow::array::Array;
#[cfg(test)]
use arrow::array::ArrayRef;
#[cfg(test)]
use arrow::array::LargeStringArray;
#[cfg(test)]
use arrow::array::StringArray;
#[cfg(test)]
use arrow::datatypes::DataType;
#[cfg(test)]
use novarocks_type_contract::ValueKind;
#[cfg(test)]
use novarocks_type_contract::ValueTypeRef;

#[cfg(test)]
use super::ArrayNormalizationStep;
#[cfg(test)]
use super::ArrayPathStep;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::hydrate::hydrate_dictionary_columns_owned_observed;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::exec::pipeline::operator::input_construction::{
        ConstructionFailure, InputConstructionFactory, try_runtime_input_construction_factory,
    };
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeErrorState;
    use crate::runtime::task::{ExecutionFailure, PipelineOperation};
    use arrow::array::{ArrayData, DictionaryArray, Int32Array, ListArray};
    use arrow::buffer::{Buffer, NullBuffer};
    use arrow::datatypes::{Field, Int32Type};
    use novarocks_type_contract::{TypedValueBinding, ValueLayout, ValueTypeStorageLifetime};
    use novarocks_types::SlotId;

    fn typed_source(array: ArrayRef, item_nullable: bool) -> Chunk {
        let storage = ValueTypeStorageLifetime::pure();
        let value = ValueTypeRef::try_new(
            ValueKind::List(
                ValueTypeRef::try_new(ValueKind::String, item_nullable, storage.clone()).unwrap(),
            ),
            true,
            storage.clone(),
        )
        .unwrap();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(array.data_type(), storage.clone()).unwrap(),
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(Field::new("input", array.data_type().clone(), true)),
            binding,
            None,
            storage.clone(),
        )
        .unwrap();
        Chunk::try_new_typed_with_columns_owned_observed::<
            crate::runtime::task::ExecutionFailure,
            _,
        >(
            Arc::new(ChunkSchema::try_new_typed(vec![slot], storage.clone()).unwrap()),
            vec![array],
            2,
            storage,
            |_| Ok(()),
        )
        .unwrap()
    }

    fn encoded_source(large: bool, item_nullable: bool) -> Chunk {
        let values = ArrayData::builder(DataType::Utf8)
            .len(2)
            .buffers(vec![
                Buffer::from_slice_ref([0i32, 4, 10]),
                Buffer::from(b"keptshadow".as_slice()),
            ])
            .nulls(Some(NullBuffer::from(vec![true, false])))
            .build()
            .unwrap();
        let mut values: ArrayRef = Arc::new(StringArray::from(values));
        if large {
            values = arrow::compute::cast(values.as_ref(), &DataType::LargeUtf8).unwrap();
        }
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![1, 0, 1]), values).unwrap(),
        );
        let data = ArrayData::builder(DataType::List(Arc::new(Field::new(
            "item",
            dictionary.data_type().clone(),
            item_nullable,
        ))))
        .len(3)
        .buffers(vec![Buffer::from_slice_ref([0i32, 1, 2, 3])])
        .child_data(vec![dictionary.to_data()])
        .nulls(Some(NullBuffer::from(vec![false, true, true])))
        .build()
        .unwrap();
        data.validate_full().unwrap();
        let array: ArrayRef = Arc::new(ListArray::from(data));
        typed_source(array.slice(0, 2), item_nullable)
    }

    fn factory(tracker: &Arc<MemTracker>) -> Arc<dyn InputConstructionFactory> {
        try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap()
    }

    #[test]
    fn required_hydrated_strings_publish_once_share_clone_slice_and_retain_real_creator() {
        for large in [false, true] {
            let source = encoded_source(large, false);
            let tracker = MemTracker::new_root("inactive required strings");
            let factory = factory(&tracker);
            let scope = factory.begin_input_construction(&source).unwrap();
            let mut publications = 0;
            let output = hydrate_dictionary_columns_owned_observed(
                &source,
                scope.storage_lifetime(),
                &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
                |_, _, _, _| false,
                |occurrence, path, step| {
                    if let ArrayNormalizationStep::ValueStructure {
                        event:
                            novarocks_value_arrays::control::ValueStructure::ByteArray {
                                new_buffer_owners,
                                ..
                            },
                        ..
                    } = step
                    {
                        assert_eq!(path, &[ArrayPathStep::ListItem]);
                        assert_eq!(
                            new_buffer_owners, 2,
                            "only actual offsets and payload Buffer owners"
                        );
                        publications += 1;
                    }
                    scope.observe(occurrence, path, step)
                },
                |layout| scope.request(layout),
                |_| Ok(()),
            )
            .unwrap();
            let list = output.columns()[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(list.nulls(), source.columns()[0].nulls());
            assert_eq!(list.value_offsets(), &[0, 1, 2]);
            assert_eq!(list.values().null_count(), 0);
            let actual = if large {
                let a = list
                    .values()
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap();
                assert_eq!(a.value(0), "");
                assert_eq!(a.value(1), "kept");
                assert_eq!(a.value(2), "");
                (a.values().as_ptr(), a.offsets().as_ptr() as *const u8)
            } else {
                let a = list
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(a.value(0), "");
                assert_eq!(a.value(1), "kept");
                assert_eq!(a.value(2), "");
                (a.values().as_ptr(), a.offsets().as_ptr() as *const u8)
            };
            assert_eq!(publications, 1);
            let cloned = output.clone();
            let sliced = output
                .slice_typed_observed::<ConstructionFailure, _>(
                    1,
                    1,
                    scope.storage_lifetime(),
                    |step| scope.observe(0, &[], step),
                )
                .unwrap();
            for alias in [&cloned, &sliced] {
                let values = alias.columns()[0]
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .values();
                let alias = if large {
                    let a = values.as_any().downcast_ref::<LargeStringArray>().unwrap();
                    (a.values().as_ptr(), a.offsets().as_ptr() as *const u8)
                } else {
                    let a = values.as_any().downcast_ref::<StringArray>().unwrap();
                    (a.values().as_ptr(), a.offsets().as_ptr() as *const u8)
                };
                assert_eq!(alias, actual);
            }
            assert_eq!(
                output.chunk_schema().slots()[0]
                    .typed_binding()
                    .value_type(),
                source.chunk_schema().slots()[0]
                    .typed_binding()
                    .value_type()
            );
            drop(source);
            drop(scope);
            drop(factory);
            assert!(tracker.current() > 0);
            drop(output);
            drop(cloned);
            assert!(
                tracker.current() > 0,
                "the final slice still holds its real creator"
            );
            drop(sliced);
            assert_eq!(tracker.current(), 0);
        }
    }

    #[test]
    fn nullable_decoded_leaf_publishes_actual_validity_owner_and_keeps_null_mask() {
        let source = encoded_source(false, true);
        let tracker = MemTracker::new_root("nullable hydrated strings");
        let factory = factory(&tracker);
        let scope = factory.begin_input_construction(&source).unwrap();
        let output = hydrate_dictionary_columns_owned_observed(
            &source,
            scope.storage_lifetime(),
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            |_, _, _, _| false,
            |occurrence, path, step| {
                if let ArrayNormalizationStep::ValueStructure {
                    event:
                        novarocks_value_arrays::control::ValueStructure::ByteArray {
                            new_buffer_owners,
                            ..
                        },
                    ..
                } = step
                {
                    assert_eq!(
                        new_buffer_owners, 3,
                        "nullable publication also creates its actual validity owner"
                    );
                }
                scope.observe(occurrence, path, step)
            },
            |layout| scope.request(layout),
            |_| Ok(()),
        )
        .unwrap();
        let list = output.columns()[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.values().null_count(), 2);
        let DataType::List(item) = list.data_type() else {
            unreachable!()
        };
        assert!(item.is_nullable());
        drop(output);
        drop(scope);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn required_wrapper_refusal_preserves_exact_original_cause_and_source() {
        let source = encoded_source(false, false);
        let tracker = MemTracker::new_root("required wrapper refusal");
        let factory = factory(&tracker);
        let initial = tracker.current();
        let scope = factory.begin_input_construction(&source).unwrap();
        let original = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded)
            .at_operator(19, PipelineOperation::Admission);
        let failure = hydrate_dictionary_columns_owned_observed(
            &source,
            scope.storage_lifetime(),
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            |_, _, _, _| false,
            |occurrence, path, step| {
                if matches!(
                    step,
                    ArrayNormalizationStep::ValueStructure {
                        event: novarocks_value_arrays::control::ValueStructure::ByteArray { .. },
                        ..
                    }
                ) {
                    return Err(ConstructionFailure::Caller(Box::new(original.clone())));
                }
                scope.observe(occurrence, path, step)
            },
            |layout| scope.request(layout),
            |_| Ok(()),
        )
        .unwrap_err();
        assert_eq!(ExecutionFailure::from(failure), original);
        assert!(matches!(
            source.columns()[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .values()
                .data_type(),
            DataType::Dictionary(_, _)
        ));
        drop(scope);
        assert_eq!(tracker.current(), initial);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn hydration_preserves_ancestor_mask_required_strings_and_rejects_visible_dictionary_null() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        use std::error::Error;

        // This fixture protects hydration and typed ingress only. The retired
        // arena ArrayMap/assert_true side effect has no installed pure owner;
        // these explicit Arrow inputs do not claim higher-order evaluation.
        let source = encoded_source(false, false);
        let original = source.columns()[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(original.len(), 2);
        assert_eq!(original.value_offsets(), &[0, 1, 2]);
        assert_eq!(original.nulls(), Some(&NullBuffer::from(vec![false, true])));
        let dictionary = original
            .values()
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dictionary.keys().values().as_ref(), &[1, 0, 1]);
        assert_eq!(dictionary.keys().null_count(), 0);
        let dictionary_values = dictionary
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(dictionary_values.value(0), "kept");
        assert!(dictionary_values.is_null(1));
        // Physical bytes under the logical dictionary NULL remain the actual
        // source bytes. Neither our input author nor hydration invents them.
        assert_eq!(dictionary_values.value(1), "shadow");

        let tracker = MemTracker::new_root("hydration ancestor-mask contract");
        let factory = factory(&tracker);
        let scope = factory.begin_input_construction(&source).unwrap();
        let hydrated = hydrate_dictionary_columns_owned_observed(
            &source,
            scope.storage_lifetime(),
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            |_, _, _, _| false,
            |occurrence, path, step| scope.observe(occurrence, path, step),
            |layout| scope.request(layout),
            |_| Ok(()),
        )
        .unwrap();
        let plain_array: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("item", DataType::Utf8, false)),
                arrow::buffer::OffsetBuffer::new(vec![0i32, 1, 2].into()),
                Arc::new(StringArray::from(vec!["would fail", "kept"])),
                Some(NullBuffer::from(vec![false, true])),
            )
            .unwrap(),
        );
        let plain = typed_source(plain_array, false);
        for input in [&hydrated, &plain] {
            let list = input.columns()[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(list.len(), 2);
            assert_eq!(list.nulls(), original.nulls());
            assert!(list.is_null(0));
            assert!(list.is_valid(1));
            assert_eq!(list.value_offsets(), original.value_offsets());
            let DataType::List(item) = list.data_type() else {
                unreachable!()
            };
            assert_eq!(item.name(), "item");
            assert_eq!(item.data_type(), &DataType::Utf8);
            assert!(!item.is_nullable());
            assert_eq!(list.values().null_count(), 0);
            let visible = list.value(1);
            let visible = visible.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(visible.iter().collect::<Vec<_>>(), vec![Some("kept")]);
            let binding = input.chunk_schema().slots()[0].typed_binding();
            assert_eq!(
                binding.value_type(),
                source.chunk_schema().slots()[0]
                    .typed_binding()
                    .value_type()
            );
            assert!(binding.value_type().nullable());
            let ValueKind::List(item) = binding.value_type().kind() else {
                unreachable!()
            };
            assert!(!item.nullable());
        }
        let plain_values = plain.columns()[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values();
        assert_eq!(
            plain_values
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "would fail"
        );
        let hydrated_values = hydrated.columns()[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values();
        let hydrated_values = hydrated_values
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            hydrated_values.iter().collect::<Vec<_>>(),
            vec![Some(""), Some("kept"), Some("")]
        );
        // Dictionary values/keys are borrowed unchanged by the input fixture.
        assert!(matches!(
            source.columns()[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .values()
                .data_type(),
            DataType::Dictionary(_, _)
        ));
        assert_eq!(dictionary_values.value(1), "shadow");
        assert!(dictionary_values.is_null(1));

        // A visible ordinary non-NULL string remains legal even when its text
        // was a failing value for the retired assertion lambda. This is a
        // required-NULL contract, not a replacement string predicate.
        let unmasked_plain: ArrayRef = Arc::new(ListArray::from(
            plain.columns()[0]
                .to_data()
                .into_builder()
                .nulls(None)
                .build()
                .unwrap(),
        ));
        let unmasked_plain = typed_source(unmasked_plain, false);
        let list = unmasked_plain.columns()[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.null_count(), 0);
        assert_eq!(list.values().null_count(), 0);
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("would fail"), Some("kept")]
        );
        drop(unmasked_plain);

        // Removing only the parent validity exposes key 1's logical NULL at
        // the required child. Use the actual typed-ingress owner to reject it,
        // rather than a test-authored lambda or a manufactured assertion error.
        let unmasked: ArrayRef = Arc::new(ListArray::from(
            original
                .to_data()
                .into_builder()
                .nulls(None)
                .build()
                .unwrap(),
        ));
        assert_eq!(unmasked.data_type(), source.columns()[0].data_type());
        assert_eq!(unmasked.null_count(), 0);
        let failure = Chunk::try_new_typed_with_columns_owned_observed::<ExecutionFailure, _>(
            source.chunk_schema_ref(),
            vec![unmasked],
            source.len(),
            source.batch_projection().array_type_storage_lifetime(),
            |_| Ok(()),
        )
        .unwrap_err();
        let expected = novarocks_value_arrays::ValueError::Invariant(
            novarocks_value_arrays::ValueViolation::RequiredNull,
        );
        assert_eq!(failure.failure_fact(), expected.failure_fact());
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(
            failure.detail(),
            "value array contract violated: RequiredNull"
        );
        let mut cause: &(dyn Error + 'static) = &failure;
        loop {
            if let Some(original) = cause.downcast_ref::<novarocks_value_arrays::ValueError>() {
                assert!(matches!(
                    original,
                    novarocks_value_arrays::ValueError::Invariant(
                        novarocks_value_arrays::ValueViolation::RequiredNull
                    )
                ));
                break;
            }
            cause = cause
                .source()
                .expect("typed ingress retains its actual projection source");
        }
        drop(failure);
        drop(source);
        drop(plain);
        drop(scope);
        drop(factory);
        assert!(
            tracker.current() > 0,
            "hydrated output retains its real creator"
        );
        drop(hydrated);
        assert_eq!(tracker.current(), 0);
    }
}
