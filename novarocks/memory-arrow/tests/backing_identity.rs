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

// Compile the independent collector before the main integration adds it to
// memory-arrow's public module tree.
#[path = "../src/backing.rs"]
mod backing;

use std::ptr::NonNull;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int32Array, Int64Array, RecordBatch, StringViewArray,
    StructArray, UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow_buffer::{Buffer, NullBuffer};
use backing::{BackingCollector, BackingError, BackingProvenance};

fn standard_arrow() -> BackingProvenance {
    // Each positive test constructs its buffers using Arrow's standard
    // constructors. The imported body slices and take output retain those
    // standard owners.
    unsafe { BackingProvenance::trusted_standard_arrow() }
}

#[test]
fn slice_uses_allocation_base_and_full_capacity() {
    let buffer = Buffer::from_vec(vec![7u8; 256]);
    let visible = buffer.slice_with_length(20, 3);
    let mut collector = BackingCollector::new();
    let origin = standard_arrow();
    assert_eq!(
        collector.collect_buffer(&visible, &origin).unwrap(),
        Some(0)
    );
    assert_eq!(collector.collect_buffer(&buffer, &origin).unwrap(), Some(0));
    let collection = collector.finish();
    assert_eq!(collection.backings().len(), 1);
    assert_eq!(
        collection.backings()[0].base(),
        buffer.data_ptr().as_ptr() as usize
    );
    assert_eq!(
        collection.backings()[0].capacity(),
        buffer.capacity() as u64
    );
    assert_eq!(collection.total_capacity(), buffer.capacity() as u64);
    assert_eq!(collection.backings()[0].buffer().len(), 3);
}

#[test]
fn grouped_batches_deduplicate_shared_body_and_dictionary() {
    // Slices of one body model the sharing produced by an uncompressed IPC
    // message. Collect both outputs before assigning either one's lineage.
    let body = Buffer::from_vec(vec![0u8; 128]);
    let left = Int32Array::from(
        arrow::array::ArrayData::builder(DataType::Int32)
            .len(4)
            .add_buffer(body.slice_with_length(0, 16))
            .build()
            .unwrap(),
    );
    let right = Int32Array::from(
        arrow::array::ArrayData::builder(DataType::Int32)
            .len(4)
            .add_buffer(body.slice_with_length(16, 16))
            .build()
            .unwrap(),
    );
    let dictionary: DictionaryArray<Int32Type> = vec![
        "long dictionary value",
        "second dictionary value",
        "long dictionary value",
        "second dictionary value",
    ]
    .into_iter()
    .collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int32, false),
        Field::new(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
    ]));
    let batch_a = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(left) as ArrayRef,
            Arc::new(dictionary.clone()) as ArrayRef,
        ],
    )
    .unwrap();
    let batch_b = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(right) as ArrayRef,
            Arc::new(dictionary) as ArrayRef,
        ],
    )
    .unwrap();

    let mut collector = BackingCollector::new();
    let origin = standard_arrow();
    let a = collector.collect_batch(&batch_a, &origin).unwrap();
    let b = collector.collect_batch(&batch_b, &origin).unwrap();
    assert!(a.contains(&0) && b.contains(&0));
    assert_eq!(a, b, "each output refers to the same complete backing set");
    let collection = collector.finish();
    assert_eq!(collection.backings()[0].capacity(), body.capacity() as u64);
    assert_eq!(
        collection.total_capacity(),
        collection
            .backings()
            .iter()
            .map(|backing| backing.capacity())
            .sum::<u64>()
    );
}

#[test]
fn nested_null_dictionary_take_and_view_payload_are_reached() {
    let nulls = NullBuffer::from(vec![true, false, true, true]);
    let numbers = Arc::new(Int64Array::new(
        vec![1, 2, 3, 4].into(),
        Some(nulls.clone()),
    )) as ArrayRef;
    let views = Arc::new(StringViewArray::from(vec![
        "long view payload one",
        "long view payload two",
        "long view payload three",
        "long view payload four",
    ])) as ArrayRef;
    let nested = StructArray::from(vec![
        (
            Arc::new(Field::new("number", DataType::Int64, true)),
            numbers,
        ),
        (
            Arc::new(Field::new("view", DataType::Utf8View, false)),
            views,
        ),
    ]);
    let dictionary: DictionaryArray<Int32Type> = vec!["shared", "values", "shared", "values"]
        .into_iter()
        .collect();
    let taken = take(&dictionary, &UInt32Array::from(vec![3, 2, 1, 0]), None).unwrap();

    let mut collector = BackingCollector::new();
    let origin = standard_arrow();
    let nested_indices = collector
        .collect_array_data(&nested.to_data(), &origin)
        .unwrap();
    let dict_indices = collector
        .collect_array_data(&dictionary.to_data(), &origin)
        .unwrap();
    let taken_indices = collector
        .collect_array_data(&taken.to_data(), &origin)
        .unwrap();
    let collection = collector.finish();
    assert!(
        nested_indices.len() >= 4,
        "nested children, null bitmap and view payload"
    );
    assert!(
        dict_indices.len() >= 3,
        "keys, offsets and dictionary values"
    );
    assert!(
        dict_indices
            .iter()
            .any(|index| taken_indices.contains(index)),
        "take must preserve at least the dictionary values backing"
    );
    assert!(
        collection.backings().iter().any(|backing| {
            backing.base() == nulls.inner().inner().data_ptr().as_ptr() as usize
        })
    );
}

#[test]
fn zero_capacity_does_not_create_data_charge() {
    let mut collector = BackingCollector::new();
    assert_eq!(
        collector
            .collect_buffer(&Buffer::from_vec(Vec::<u8>::new()), &standard_arrow())
            .unwrap(),
        None
    );
    let (backings, total_capacity) = collector.finish().into_parts();
    assert_eq!(backings.len(), 0);
    assert_eq!(total_capacity, 0);
}

#[test]
fn large_local_group_preserves_first_seen_indices() {
    let buffers: Vec<_> = (0..32)
        .map(|value| Buffer::from_vec(vec![value; 64]))
        .collect();
    let origin = standard_arrow();
    let mut collector = BackingCollector::new();
    for (index, buffer) in buffers.iter().enumerate() {
        assert_eq!(
            collector.collect_buffer(buffer, &origin).unwrap(),
            Some(index)
        );
    }
    for (index, buffer) in buffers.iter().enumerate().rev() {
        assert_eq!(
            collector.collect_buffer(buffer, &origin).unwrap(),
            Some(index)
        );
    }
    assert_eq!(collector.finish().backings().len(), 32);
}

#[test]
fn nonempty_custom_allocation_is_rejected_without_guessing_from_visible_len() {
    let mut owner = vec![42u8; 4096];
    let pointer = NonNull::new(owner.as_mut_ptr()).unwrap();
    let custom = unsafe { Buffer::from_custom_allocation(pointer, 32, Arc::new(owner)) };
    assert_eq!(
        custom.capacity(),
        32,
        "Arrow reports supplied len, not owner capacity"
    );
    let mut collector = BackingCollector::new();
    assert!(matches!(
        collector.collect_buffer(&custom, &BackingProvenance::unknown()),
        Err(BackingError::UnknownProvenance {
            visible_len: 32,
            ..
        })
    ));

    let ordinary = Buffer::from_vec(vec![0u8; 32]);
    assert!(matches!(
        collector.collect_buffer(&ordinary, &BackingProvenance::unknown()),
        Err(BackingError::UnknownProvenance {
            visible_len: 32,
            ..
        })
    ));

    let mut empty_owner = vec![0u8; 4096];
    let empty_ptr = NonNull::new(empty_owner.as_mut_ptr()).unwrap();
    let empty_custom =
        unsafe { Buffer::from_custom_allocation(empty_ptr, 0, Arc::new(empty_owner)) };
    assert!(matches!(
        collector.collect_buffer(&empty_custom, &BackingProvenance::unknown()),
        Err(BackingError::UnknownProvenance { visible_len: 0, .. })
    ));
}
