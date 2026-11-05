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
use std::sync::Arc;

use arrow::array::BinaryArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::{Chunk, ChunkSchema, ChunkSlotSchema};
use novarocks_types::SlotId;
use novarocks_types::logical::{LogicalType, field_with_logical_type, logical_type_of_field};

#[test]
fn strict_rejects_duplicate_slot_id() {
    let err = ChunkSchema::try_new(vec![
        ChunkSlotSchema::new_with_field(
            SlotId::new(1),
            Field::new("a", DataType::Int32, true),
            None,
            None,
        ),
        ChunkSlotSchema::new_with_field(
            SlotId::new(1),
            Field::new("b", DataType::Int32, true),
            None,
            None,
        ),
    ])
    .expect_err("duplicate slot ids should fail");
    assert!(err.to_string().contains("duplicate slot id"), "err={}", err);
}

#[test]
fn chunk_schema_recovers_logical_metadata_and_unique_id() {
    let hll_field =
        field_with_logical_type(Field::new("a", DataType::Binary, true), LogicalType::Hll);
    let schema = Arc::new(Schema::new(vec![hll_field.clone()]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(BinaryArray::from(vec![Some(b"x".as_slice())]))],
    )
    .expect("record batch");
    let chunk = Chunk::try_new_with_chunk_schema(
        batch,
        Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(7),
                hll_field,
                None,
                Some(77),
            )])
            .expect("chunk schema"),
        ),
    )
    .expect("chunk");
    let slot = chunk
        .chunk_schema()
        .slot(SlotId::new(7))
        .expect("slot schema");
    assert_eq!(slot.data_type(), &DataType::Binary);
    assert_eq!(
        slot.field_schema().logical_type(),
        Some(novarocks_type_contract::ValueLogicalType::Hll)
    );
    assert_eq!(logical_type_of_field(slot.field()), Some(LogicalType::Hll));
    assert_eq!(slot.name(), "a");
    assert_eq!(slot.unique_id(), Some(77));
}

fn typed_slot(
    field: Field,
    value: novarocks_type_contract::ValueTypeRef,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
) -> ChunkSlotSchema {
    use novarocks_type_contract::{TypedValueBinding, ValueLayout};
    let layout = ValueLayout::try_new(field.data_type(), storage.clone()).unwrap();
    let binding = TypedValueBinding::try_new(value, layout).unwrap();
    ChunkSlotSchema::try_new_typed(SlotId::new(91), Arc::new(field), binding, None, storage)
        .unwrap()
}

#[test]
fn typed_chunk_zero_channels_retains_positive_row_count() {
    use novarocks_type_contract::ValueTypeStorageLifetime;
    let schema =
        Arc::new(ChunkSchema::try_new_typed(Vec::new(), ValueTypeStorageLifetime::pure()).unwrap());
    let chunk = Chunk::try_new_typed_with_columns_observed::<
        crate::runtime::task::ExecutionFailure,
        _,
    >(schema, Vec::new(), 37, |_| Ok(()))
    .unwrap();
    assert_eq!(chunk.len(), 37);
    assert_eq!(chunk.columns().len(), 0);
    assert_eq!(
        chunk
            .slice_typed_observed::<crate::runtime::task::ExecutionFailure, _>(
                3,
                9,
                ValueTypeStorageLifetime::pure(),
                |_| Ok(())
            )
            .unwrap()
            .len(),
        9
    );
}

#[test]
fn typed_chunk_normalizes_actual_arrays_and_shares_semantics_and_payload() {
    use arrow::array::{Array, ArrayRef, ListArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use novarocks_type_contract::{
        ValueKind, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
    };
    let storage = ValueTypeStorageLifetime::pure();
    let item = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        true,
        storage.clone(),
    )
    .unwrap();
    let list = ValueTypeRef::try_new(ValueKind::List(item.clone()), true, storage.clone()).unwrap();
    let item_field = Arc::new(field_with_logical_type(
        Field::new("foreign_element", DataType::Utf8, true),
        LogicalType::Json,
    ));
    let values: ArrayRef = Arc::new(StringArray::from(vec![Some("{}"), None]));
    let array: ArrayRef = Arc::new(
        ListArray::try_new(
            item_field.clone(),
            OffsetBuffer::from_lengths([2]),
            values.clone(),
            None,
        )
        .unwrap(),
    );
    let slot = typed_slot(
        Field::new("items", DataType::List(item_field), true),
        list.clone(),
        storage.clone(),
    );
    let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], storage).unwrap());
    let chunk = Chunk::try_new_typed_with_columns_observed::<
        crate::runtime::task::ExecutionFailure,
        _,
    >(schema.clone(), vec![array.clone()], 1, |_| Ok(()))
    .unwrap();
    assert!(Arc::ptr_eq(&schema, &chunk.chunk_schema_ref()));
    assert!(
        chunk.chunk_schema().slots()[0]
            .field_schema()
            .value_type()
            .shares_storage(&list)
    );
    let normalized = chunk.columns()[0]
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(Arc::ptr_eq(normalized.values(), &values));
    let DataType::List(field) = normalized.data_type() else {
        panic!("list")
    };
    assert_eq!(field.name(), "item");
    assert!(field.metadata().is_empty());
    assert!(
        item.shares_storage(
            chunk.chunk_schema().slots()[0]
                .field_schema()
                .list_item()
                .unwrap()
        )
    );
    let unchanged = Chunk::try_new_typed_with_columns_observed::<
        crate::runtime::task::ExecutionFailure,
        _,
    >(schema, chunk.columns().to_vec(), 1, |_| Ok(()))
    .unwrap();
    assert!(Arc::ptr_eq(&chunk.columns()[0], &unchanged.columns()[0]));
    let DataType::List(original) = array.data_type() else {
        panic!("list")
    };
    assert_eq!(original.name(), "foreign_element");
    assert!(!original.metadata().is_empty());
}

#[test]
fn typed_chunk_refusal_and_required_null_do_not_widen_or_mutate_source() {
    use super::ArrayNormalizationStep;
    use arrow::array::{ArrayRef, Int32Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use novarocks_type_contract::{ValueKind, ValueTypeRef, ValueTypeStorageLifetime};
    let storage = ValueTypeStorageLifetime::pure();
    let item =
        ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), true, storage.clone()).unwrap();
    let list = ValueTypeRef::try_new(ValueKind::List(item), true, storage.clone()).unwrap();
    let field = Arc::new(Field::new("external", DataType::Int32, true));
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(7)]));
    let input: ArrayRef = Arc::new(
        ListArray::try_new(field.clone(), OffsetBuffer::from_lengths([1]), values, None).unwrap(),
    );
    let slot = typed_slot(
        Field::new("items", DataType::List(field), true),
        list,
        storage.clone(),
    );
    let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], storage.clone()).unwrap());
    let refused = Chunk::try_new_typed_with_columns_observed::<
        crate::runtime::task::ExecutionFailure,
        _,
    >(schema, vec![input.clone()], 1, |step| match step {
        ArrayNormalizationStep::RebuildField { .. } => {
            Err(crate::runtime::task::ExecutionFailure::from(
                novarocks_functions::KernelFailure::Cancelled,
            ))
        }
        _ => Ok(()),
    });
    assert_eq!(
        refused.unwrap_err(),
        crate::runtime::task::ExecutionFailure::from(novarocks_functions::KernelFailure::Cancelled)
    );
    let DataType::List(field) = input.data_type() else {
        panic!("list")
    };
    assert_eq!(field.name(), "external");
    let value =
        ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), false, storage.clone()).unwrap();
    let slot = typed_slot(
        Field::new("required", DataType::Int32, false),
        value,
        storage.clone(),
    );
    let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], storage).unwrap());
    let nulls: ArrayRef = Arc::new(Int32Array::from(vec![None]));
    let error = Chunk::try_new_typed_with_columns_observed::<
        crate::runtime::task::ExecutionFailure,
        _,
    >(schema.clone(), vec![nulls], 1, |_| Ok(()))
    .unwrap_err();
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&error),
        novarocks_failure_contract::FailureProjection::failure_fact(
            &novarocks_value_arrays::ValueError::Invariant(
                novarocks_value_arrays::ValueViolation::RequiredNull
            )
        )
    );
    assert!(!schema.slots()[0].nullable());
}

#[test]
fn typed_chunk_schema_drop_leaves_binding_child_and_projection_lifetimes_live() {
    use novarocks_type_contract::{
        ValueKind, ValueTypeRef, ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let released = Arc::new(AtomicUsize::new(0));
    let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(released.clone())));
    let item = ValueTypeRef::try_new(ValueKind::String, true, storage.clone()).unwrap();
    let list = ValueTypeRef::try_new(ValueKind::List(item), true, storage.clone()).unwrap();
    let slot = typed_slot(
        Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        list,
        storage.clone(),
    );
    let binding = slot.typed_binding().clone();
    let child = slot.field_schema().list_item().unwrap().clone();
    let schema = ChunkSchema::try_new_typed(vec![slot], storage.clone()).unwrap();
    let projected = schema
        .project_by_slots_typed(&[SlotId::new(91)], storage.clone())
        .unwrap();
    drop(storage);
    drop(schema);
    drop(binding);
    drop(child);
    assert_eq!(released.load(Ordering::SeqCst), 0);
    drop(projected);
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[test]
fn typed_slice_releases_intermediate_wrapper_hold_and_keeps_shared_creation_hold() {
    use arrow::array::{ArrayRef, ListArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use novarocks_type_contract::{
        ValueKind, ValueTypeRef, ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let hold = |released: &Arc<AtomicUsize>| {
        ValueTypeStorageLifetime::held(Arc::new(Hold(released.clone())))
    };
    let original_released = Arc::new(AtomicUsize::new(0));
    let first_released = Arc::new(AtomicUsize::new(0));
    let second_released = Arc::new(AtomicUsize::new(0));
    let pure = ValueTypeStorageLifetime::pure();
    let item = ValueTypeRef::try_new(ValueKind::String, true, pure.clone()).unwrap();
    let value = ValueTypeRef::try_new(ValueKind::List(item), true, pure.clone()).unwrap();
    let field = Arc::new(Field::new("external_item", DataType::Utf8, true));
    let array: ArrayRef = Arc::new(
        ListArray::try_new(
            field.clone(),
            OffsetBuffer::from_lengths([1, 1]),
            Arc::new(StringArray::from(vec!["a", "b"])),
            None,
        )
        .unwrap(),
    );
    let slot = typed_slot(
        Field::new("items", DataType::List(field), true),
        value,
        pure.clone(),
    );
    let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], pure).unwrap());
    let original = Chunk::try_new_typed_with_columns_owned_observed::<
        crate::runtime::task::ExecutionFailure,
        _,
    >(schema, vec![array], 2, hold(&original_released), |_| Ok(()))
    .unwrap();
    let first = original
        .slice_typed_observed::<crate::runtime::task::ExecutionFailure, _>(
            0,
            2,
            hold(&first_released),
            |_| Ok(()),
        )
        .unwrap();
    let second = first
        .slice_typed_observed::<crate::runtime::task::ExecutionFailure, _>(
            1,
            1,
            hold(&second_released),
            |_| Ok(()),
        )
        .unwrap();
    drop(original);
    drop(first);
    assert_eq!(first_released.load(Ordering::SeqCst), 1);
    assert_eq!(original_released.load(Ordering::SeqCst), 0);
    assert_eq!(second_released.load(Ordering::SeqCst), 0);
    drop(second);
    assert_eq!(original_released.load(Ordering::SeqCst), 1);
    assert_eq!(second_released.load(Ordering::SeqCst), 1);
}

#[test]
fn typed_required_dictionary_uses_referenced_values_nulls_and_slice_coordinates() {
    use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::Int32Type;
    use novarocks_type_contract::{ValueKind, ValueTypeRef, ValueTypeStorageLifetime};
    let values: ArrayRef = Arc::new(StringArray::from(vec![None, Some("good")]));
    let encoded = |keys: Vec<i32>| -> ArrayRef {
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), values.clone()).unwrap(),
        )
    };
    let bind = |array: ArrayRef| -> crate::runtime::task::ExecutionResult<Chunk> {
        let storage = ValueTypeStorageLifetime::pure();
        let value = ValueTypeRef::try_new(ValueKind::String, false, storage.clone()).unwrap();
        let slot = typed_slot(
            Field::new("required", array.data_type().clone(), false),
            value,
            storage.clone(),
        );
        Chunk::try_new_typed_with_columns_owned_observed(
            Arc::new(ChunkSchema::try_new_typed(vec![slot], storage.clone()).unwrap()),
            vec![array.clone()],
            array.len(),
            storage,
            |_| Ok(()),
        )
    };
    let referenced = encoded(vec![0]);
    assert_eq!(referenced.null_count(), 0);
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&bind(referenced).unwrap_err()),
        novarocks_failure_contract::FailureProjection::failure_fact(
            &novarocks_value_arrays::ValueError::Invariant(
                novarocks_value_arrays::ValueViolation::RequiredNull
            )
        )
    );
    let unused_null = encoded(vec![1, 1]);
    assert_eq!(bind(unused_null).unwrap().len(), 2);
    let source = encoded(vec![1, 0, 1]);
    assert_eq!(bind(source.slice(0, 1)).unwrap().len(), 1);
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(
            &bind(source.slice(1, 1)).unwrap_err()
        ),
        novarocks_failure_contract::FailureProjection::failure_fact(
            &novarocks_value_arrays::ValueError::Invariant(
                novarocks_value_arrays::ValueViolation::RequiredNull
            )
        )
    );
    assert_eq!(bind(source.slice(2, 1)).unwrap().len(), 1);
}

#[test]
fn typed_nested_required_dictionary_respects_struct_parent_null_mask() {
    use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, StringArray, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::Int32Type;
    use novarocks_type_contract::{ValueField, ValueKind, ValueTypeRef, ValueTypeStorageLifetime};
    let storage = ValueTypeStorageLifetime::pure();
    let values: ArrayRef = Arc::new(StringArray::from(vec![None, Some("good")]));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 1]), values).unwrap(),
    );
    let fields = vec![Arc::new(Field::new(
        "payload",
        dictionary.data_type().clone(),
        false,
    ))];
    let parent: ArrayRef = Arc::new(
        StructArray::try_new(
            fields.clone().into(),
            vec![dictionary],
            Some(NullBuffer::from(vec![false, true])),
        )
        .unwrap(),
    );
    let child = ValueTypeRef::try_new(ValueKind::String, false, storage.clone()).unwrap();
    let value = ValueTypeRef::try_new(
        ValueKind::Struct(
            vec![ValueField::new("payload", child, storage.clone())].into_boxed_slice(),
        ),
        true,
        storage.clone(),
    )
    .unwrap();
    let slot = typed_slot(
        Field::new("parent", DataType::Struct(fields.into()), true),
        value,
        storage.clone(),
    );
    let source = Chunk::try_new_typed_with_columns_owned_observed::<
        crate::runtime::task::ExecutionFailure,
        _,
    >(
        Arc::new(ChunkSchema::try_new_typed(vec![slot], storage.clone()).unwrap()),
        vec![parent],
        2,
        storage.clone(),
        |_| Ok(()),
    )
    .unwrap();
    let null_parent = source
        .slice_typed_observed::<crate::runtime::task::ExecutionFailure, _>(
            0,
            1,
            storage.clone(),
            |_| Ok(()),
        )
        .unwrap();
    assert!(null_parent.columns()[0].is_null(0));
    let valid_parent = source
        .slice_typed_observed::<crate::runtime::task::ExecutionFailure, _>(
            1,
            1,
            storage,
            |_| Ok(()),
        )
        .unwrap();
    assert!(!valid_parent.columns()[0].is_null(0));
    assert!(
        !valid_parent.chunk_schema().slots()[0]
            .field_schema()
            .struct_child(0)
            .unwrap()
            .nullable()
    );
}

#[test]
fn typed_required_dictionary_observes_bounded_rows_and_stops_on_refusal() {
    use super::ArrayNormalizationStep;
    use arrow::array::{ArrayRef, DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::Int32Type;
    use novarocks_type_contract::{ValueKind, ValueTypeRef, ValueTypeStorageLifetime};
    let storage = ValueTypeStorageLifetime::pure();
    let values: ArrayRef = Arc::new(StringArray::from(vec![None, Some("good")]));
    let array: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![1; 2050]), values).unwrap(),
    );
    let value = ValueTypeRef::try_new(ValueKind::String, false, storage.clone()).unwrap();
    let slot = typed_slot(
        Field::new("required", array.data_type().clone(), false),
        value,
        storage.clone(),
    );
    let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], storage.clone()).unwrap());
    let mut rows = Vec::new();
    let refused = Chunk::try_new_typed_with_columns_owned_observed::<
        crate::runtime::task::ExecutionFailure,
        _,
    >(schema, vec![array], 2050, storage, |step| {
        if let ArrayNormalizationStep::InspectDictionaryRows {
            row_start,
            row_count,
            ..
        } = step
        {
            rows.push((row_start, row_count));
            if row_start == 1024 {
                return Err(novarocks_functions::KernelFailure::Cancelled.into());
            }
        }
        Ok(())
    })
    .unwrap_err();
    assert_eq!(
        refused,
        crate::runtime::task::ExecutionFailure::from(novarocks_functions::KernelFailure::Cancelled)
    );
    assert_eq!(rows, vec![(0, 1024), (1024, 1024)]);
}
