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

use super::*;
use crate::{ConstantPolicy, ConstantPool};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int32Array, Int64Array, LargeListArray, ListArray,
    MapArray, RunArray, StringArray, StructArray,
    types::{Int8Type, Int32Type, Int64Type},
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::Field;
use novarocks_type_contract::{CompileControlError, FunctionValueType};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const PHASE: CompilePhase = CompilePhase::Validate;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, PHASE);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after first refusal");
        }
        trace.push(units);
        match self.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 65536,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 8 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 128 << 20,
        max_library_validation_bytes: 128 << 20,
    }
}
fn pool(array: ArrayRef) -> ConstantPool {
    let field = Arc::new(
        Field::new("source_collection", array.data_type().clone(), true)
            .with_metadata(HashMap::from([("source-owned".into(), "original".into())])),
    );
    let ty = FunctionValueType::try_from_field(&field).unwrap();
    // Actual admission uses its own explicit finite policy and recording
    // control, separate from the selected-reader trace under test.
    ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        PHASE,
        &Control::default(),
    )
    .unwrap()
}
fn list() -> ListArray {
    ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(99)]),
        Some(vec![Some(i32::MIN), None, Some(i32::MAX)]),
        None,
        Some(vec![]),
    ])
}
fn map() -> MapArray {
    let keys: ArrayRef = Arc::new(StringArray::from(vec![
        Some("prefix"),
        Some("dup"),
        Some(""),
        Some(""),
        Some("dup"),
        Some("dup"),
        Some("suffix"),
    ]));
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        Some("unused"),
        Some("one"),
        Some("雪☃"),
        None,
        Some("left"),
        Some("right"),
        Some("unused"),
    ]));
    let fields = vec![
        Arc::new(
            Field::new("source_key", DataType::Utf8, false)
                .with_metadata(HashMap::from([("key-source".into(), "retained".into())])),
        ),
        Arc::new(Field::new("source_value", DataType::Utf8, true)),
    ]
    .into();
    // A genuine sliced Struct entry backing exercises child offsets. The
    // selected Map row additionally starts after an unused entry.
    let entries = StructArray::new(fields, vec![keys, values], None).slice(1, 5);
    let field = Arc::new(Field::new(
        "source_entries",
        entries.data_type().clone(),
        false,
    ));
    MapArray::try_new(
        field,
        OffsetBuffer::new(vec![0i32, 1, 5].into()),
        entries,
        None,
        false,
    )
    .unwrap()
}
fn assert_prefix<T>(invoke: impl Fn(&Control) -> Result<T, ConstantError>, ordinary: bool) {
    let good = Control::default();
    let result = invoke(&good);
    assert_eq!(result.is_err(), ordinary);
    if ordinary {
        assert!(matches!(result, Err(ConstantError::Invalid(_))));
    }
    let trace = good.trace.lock().unwrap().clone();
    assert_eq!(trace[0], 0);
    assert!(trace.last().is_some_and(|units| *units > 0));
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::new(vec![]),
                stop: Some((at, cause.clone())),
            };
            assert!(
                matches!(invoke(&control), Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn int32_list_borrows_original_ordinal_slice_field_and_nullable_items() {
    let p = pool(Arc::new(list()));
    let value = p.value(1).unwrap();
    let control = Control::default();
    let view = value.int32_list_observed(PHASE, &control).unwrap().unwrap();
    assert!(std::ptr::eq(view.source(), &value));
    assert!(std::ptr::eq(view.source().field(), p.field()));
    assert!(std::ptr::eq(view.source().value_type(), p.value_type()));
    assert_eq!(view.source().ordinal(), 1);
    assert_eq!(view.len(), 3);
    assert!(!view.is_empty());
    assert_eq!(view.item(0, PHASE, &control).unwrap(), Some(i32::MIN));
    assert_eq!(view.item(1, PHASE, &control).unwrap(), None);
    assert_eq!(view.item(2, PHASE, &control).unwrap(), Some(i32::MAX));
    assert!(std::ptr::eq(view.child, &p.0.data.child_data()[0]));
    let sliced = pool(Arc::new(list().slice(1, 2)));
    let sliced_value = sliced.value(0).unwrap();
    let slice = sliced_value
        .int32_list_observed(PHASE, &control)
        .unwrap()
        .unwrap();
    assert_eq!(slice.item(2, PHASE, &control).unwrap(), Some(i32::MAX));
    assert!(
        sliced
            .value(1)
            .unwrap()
            .int32_list_observed(PHASE, &control)
            .unwrap()
            .is_none()
    );
    assert!(
        p.value(2)
            .unwrap()
            .int32_list_observed(PHASE, &control)
            .unwrap()
            .is_none()
    );
    let empty_value = p.value(3).unwrap();
    let empty = empty_value
        .int32_list_observed(PHASE, &control)
        .unwrap()
        .unwrap();
    assert!(empty.is_empty());
    assert!(matches!(
        empty.item(0, PHASE, &control),
        Err(ConstantError::Invalid(_))
    ));
}

#[test]
fn utf8_map_preserves_nullable_values_duplicates_order_and_borrowed_payloads() {
    let original = map();
    let p = pool(Arc::new(original.clone()));
    let value = p.value(1).unwrap();
    let control = Control::default();
    let view = value.utf8_map_observed(PHASE, &control).unwrap().unwrap();
    assert!(std::ptr::eq(view.source(), &value));
    assert!(std::ptr::eq(view.source().field(), p.field()));
    assert_eq!(view.len(), 4);
    assert_eq!(
        view.item(0, PHASE, &control).unwrap(),
        (Some(""), Some("雪☃"))
    );
    assert_eq!(view.item(1, PHASE, &control).unwrap(), (Some(""), None));
    assert_eq!(
        view.item(2, PHASE, &control).unwrap(),
        (Some("dup"), Some("left"))
    );
    assert_eq!(
        view.item(3, PHASE, &control).unwrap(),
        (Some("dup"), Some("right"))
    );
    let (_, text) = view.item(0, PHASE, &control).unwrap();
    let array = original
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(std::ptr::eq(
        text.unwrap().as_ptr(),
        array.value(1).as_ptr()
    ));
    assert_eq!(view.source().field().metadata()["source-owned"], "original");
    let sliced = pool(Arc::new(original.slice(1, 1)));
    let selected = sliced.value(0).unwrap();
    let view = selected
        .utf8_map_observed(PHASE, &control)
        .unwrap()
        .unwrap();
    assert_eq!(
        view.item(3, PHASE, &control).unwrap(),
        (Some("dup"), Some("right"))
    );
}

#[test]
fn selected_collection_roots_resolve_dictionary_runs_and_encoded_nulls() {
    let values: ArrayRef = Arc::new(list());
    let dict = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1), None, Some(2)]),
        values,
    )
    .unwrap();
    let p = pool(Arc::new(dict));
    let control = Control::default();
    let selected = p.value(1).unwrap();
    let view = selected
        .int32_list_observed(PHASE, &control)
        .unwrap()
        .unwrap();
    assert_eq!(view.item(2, PHASE, &control).unwrap(), Some(i32::MAX));
    for ordinal in [2, 3] {
        assert!(
            p.value(ordinal)
                .unwrap()
                .int32_list_observed(PHASE, &control)
                .unwrap()
                .is_none()
        );
    }
    let values = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(-7)]), None]);
    let run = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 5]), &values).unwrap();
    let p = pool(Arc::new(run));
    let selected = p.value(1).unwrap();
    assert_eq!(
        selected
            .int32_list_observed(PHASE, &control)
            .unwrap()
            .unwrap()
            .item(0, PHASE, &control)
            .unwrap(),
        Some(-7)
    );
    assert!(
        p.value(4)
            .unwrap()
            .int32_list_observed(PHASE, &control)
            .unwrap()
            .is_none()
    );
    let values: ArrayRef = Arc::new(map());
    let dict =
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![Some(0), Some(1), None]), values)
            .unwrap();
    let p = pool(Arc::new(dict));
    let selected = p.value(1).unwrap();
    assert_eq!(
        selected
            .utf8_map_observed(PHASE, &control)
            .unwrap()
            .unwrap()
            .item(0, PHASE, &control)
            .unwrap(),
        (Some(""), Some("雪☃"))
    );
    assert!(
        p.value(2)
            .unwrap()
            .utf8_map_observed(PHASE, &control)
            .unwrap()
            .is_none()
    );
}

#[test]
fn wrong_nonnull_collection_carriers_and_out_of_range_items_have_ordinary_tails() {
    for array in [
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
            vec![Some(1)],
        )])) as ArrayRef,
        Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1)]),
        ])) as ArrayRef,
    ] {
        let value = pool(array).value(0).unwrap();
        assert_prefix(
            |control| value.int32_list_observed(PHASE, control).map(|_| ()),
            true,
        );
        assert_prefix(
            |control| value.utf8_map_observed(PHASE, control).map(|_| ()),
            true,
        );
    }
    let value = pool(Arc::new(list())).value(1).unwrap();
    for index in [3, usize::MAX] {
        assert_prefix(
            |control| {
                value
                    .int32_list_observed(PHASE, control)?
                    .unwrap()
                    .item(index, PHASE, control)
            },
            true,
        );
    }
    let value = pool(Arc::new(map())).value(1).unwrap();
    assert_prefix(
        |control| {
            value
                .utf8_map_observed(PHASE, control)?
                .unwrap()
                .item(4, PHASE, control)
                .map(|_| ())
        },
        true,
    );
}

#[test]
fn successful_item_reads_and_null_roots_keep_every_original_control_prefix() {
    let list = pool(Arc::new(list()));
    for ordinal in [1, 2, 3] {
        let value = list.value(ordinal).unwrap();
        assert_prefix(
            |control| {
                if let Some(view) = value.int32_list_observed(PHASE, control)? {
                    for index in 0..view.len() {
                        view.item(index, PHASE, control)?;
                    }
                }
                Ok(())
            },
            false,
        );
    }
    let value = pool(Arc::new(map())).value(1).unwrap();
    assert_prefix(
        |control| {
            let view = value.utf8_map_observed(PHASE, control)?.unwrap();
            for index in 0..view.len() {
                view.item(index, PHASE, control)?;
            }
            Ok(())
        },
        false,
    );
}

#[test]
fn selected_null_text_payload_is_not_validated_and_nonnull_text_is_borrowed() {
    let hidden = "x".repeat(320 * 1024);
    let mut bytes = Vec::from(b"one".as_slice());
    bytes.extend_from_slice(hidden.as_bytes());
    let values: ArrayRef = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0i32, 3, i32::try_from(bytes.len()).unwrap()].into()),
        Buffer::from(bytes),
        Some(NullBuffer::from(vec![true, false])),
    ));
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Utf8, true)),
        ]
        .into(),
        vec![keys, values],
        None,
    );
    let field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let array = MapArray::try_new(
        field,
        OffsetBuffer::new(vec![0i32, 2].into()),
        entries,
        None,
        false,
    )
    .unwrap();
    let value = pool(Arc::new(array)).value(0).unwrap();
    let view = value
        .utf8_map_observed(PHASE, &Control::default())
        .unwrap()
        .unwrap();
    let good = Control::default();
    assert_eq!(view.item(1, PHASE, &good).unwrap(), (Some("b"), None));
    assert!(!good.trace.lock().unwrap().contains(&256));
    assert_eq!(
        view.item(0, PHASE, &good).unwrap(),
        (Some("a"), Some("one"))
    );
}

#[test]
fn caller_owned_collection_scan_observes_real_quantum_without_item_tails() {
    let array = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(
        (0..320).map(Some).collect::<Vec<_>>(),
    )]);
    let value = pool(Arc::new(array)).value(0).unwrap();
    let view = value
        .int32_list_observed(PHASE, &Control::default())
        .unwrap()
        .unwrap();
    let invoke = |control: &Control| {
        let mut work = CompileCheckpoints::try_new(control, PHASE)?;
        let result = (|| {
            for index in 0..view.len() {
                assert_eq!(
                    view.item_observed(index, &mut work)?,
                    Some(i32::try_from(index).unwrap())
                );
            }
            Ok(())
        })();
        finish(work, result)
    };
    let control = Control::default();
    invoke(&control).unwrap();
    let trace = control.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::new(vec![]),
                stop: Some((at, cause.clone())),
            };
            assert!(
                matches!(invoke(&control), Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn numerical_ordinal_limit_is_primary_without_a_later_callback() {
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    let result = ordinal(usize::MAX, 2, 1, &mut work);
    assert!(matches!(finish(work, result), Err(ConstantError::Limit(_))));
    assert_eq!(*control.trace.lock().unwrap(), [0]);
    // This private arithmetic seam is unreachable for an admitted view; it
    // proves first-limit behavior without manufacturing an invalid pool.
}

#[test]
fn raw_map_and_entry_parent_offsets_are_applied_once_by_original_pool() {
    let keys = StringArray::from(vec![Some("hidden"), Some("prefix"), Some(""), Some("tail")]);
    let values = StringArray::from(vec!["hidden", "unused", "雪", "last"]);
    let fields = vec![
        Arc::new(Field::new("key", DataType::Utf8, false)),
        Arc::new(Field::new("value", DataType::Utf8, false)),
    ]
    .into();
    // Supply raw ArrayData directly to the original checked owner, before
    // any Array implementation has normalized this genuine Struct offset.
    let entries = ArrayData::builder(DataType::Struct(fields))
        .len(3)
        .offset(1)
        .child_data(vec![keys.to_data(), values.to_data()])
        .build()
        .unwrap();
    assert_eq!(entries.offset(), 1);
    let ty = DataType::Map(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        false,
    );
    let raw = ArrayData::builder(ty.clone())
        .len(2)
        .buffers(vec![Buffer::from_slice_ref([0_i32, 1, 3])])
        .child_data(vec![entries])
        .build()
        .unwrap();
    for (data, ordinal) in [(raw.clone(), 1), (raw.slice(1, 1), 0)] {
        let field = Arc::new(Field::new("raw_source", ty.clone(), false));
        let pool = ConstantPool::try_new(
            field.clone(),
            FunctionValueType::try_from_field(&field).unwrap(),
            data,
            policy(),
            PHASE,
            &Control::default(),
        )
        .unwrap();
        assert_eq!(pool.0.data.child_data()[0].offset(), 0);
        let selected = pool.value(ordinal).unwrap();
        let view = selected
            .utf8_map_observed(PHASE, &Control::default())
            .unwrap()
            .unwrap();
        assert!(std::ptr::eq(view.source(), &selected));
        assert!(Arc::ptr_eq(view.source().pool().field_ref(), &field));
        assert_eq!(view.len(), 2);
        assert_eq!(
            view.item(0, PHASE, &Control::default()).unwrap(),
            (Some(""), Some("雪"))
        );
        assert_eq!(
            view.item(1, PHASE, &Control::default()).unwrap(),
            (Some("tail"), Some("last"))
        );
        let (_, text) = view.item(0, PHASE, &Control::default()).unwrap();
        assert_eq!(text.unwrap().as_ptr(), values.value(2).as_ptr());
        assert_prefix(
            |control| {
                let view = selected.utf8_map_observed(PHASE, control)?.unwrap();
                view.item(0, PHASE, control)
            },
            false,
        );
    }
}

#[path = "borrowed_reader_tests.rs"]
mod borrowed_reader_tests;

fn utf8_list() -> (ListArray, StringArray) {
    let original = StringArray::from(vec![
        Some("hidden"),
        Some("prefix"),
        Some("雪☃"),
        Some(""),
        None,
        Some("tail"),
        Some("unused"),
    ]);
    let values: ArrayRef = Arc::new(original.slice(1, 5));
    let item = Arc::new(
        Field::new("source_item", DataType::Utf8, true)
            .with_metadata(HashMap::from([("child-source".into(), "retained".into())])),
    );
    let array = ListArray::try_new(
        item,
        OffsetBuffer::new(vec![0_i32, 1, 4, 4, 4, 5].into()),
        values,
        Some(NullBuffer::from(vec![true, true, false, true, true])),
    )
    .unwrap();
    (array, original)
}

#[test]
fn utf8_list_borrows_nonzero_ordinal_complete_source_and_child_payload() {
    let (array, original) = utf8_list();
    let field = Arc::new(
        Field::new("original_list", array.data_type().clone(), true)
            .with_metadata(HashMap::from([("source-owned".into(), "original".into())])),
    );
    let value_type = FunctionValueType::try_from_field(&field).unwrap();
    let p = ConstantPool::try_new(
        field.clone(),
        value_type.clone(),
        array.to_data(),
        policy(),
        PHASE,
        &Control::default(),
    )
    .unwrap();
    let value = p.value(1).unwrap();
    let backing = p.backing_identity();
    let control = Control::default();
    let view = value.utf8_list_observed(PHASE, &control).unwrap().unwrap();
    assert!(std::ptr::eq(view.source(), &value));
    assert!(Arc::ptr_eq(view.source().pool().field_ref(), &field));
    assert!(std::ptr::eq(view.source().value_type(), p.value_type()));
    assert_eq!(view.source().value_type(), &value_type);
    assert_eq!(view.source().pool().backing_identity(), backing);
    assert_eq!(view.source().ordinal(), 1);
    assert_eq!(view.source().field().metadata()["source-owned"], "original");
    let DataType::List(item) = view.source().field().data_type() else {
        panic!()
    };
    assert_eq!(item.metadata()["child-source"], "retained");
    assert!(view.source().value_type().nullable());
    assert!(view.source().value_type().child_at(0).unwrap().nullable());
    assert!(std::ptr::eq(view.child, &p.0.data.child_data()[0]));
    assert_eq!(view.len(), 3);
    assert!(!view.is_empty());
    assert_eq!(view.item(0, PHASE, &control).unwrap(), Some("雪☃"));
    assert_eq!(view.item(1, PHASE, &control).unwrap(), Some(""));
    assert_eq!(view.item(2, PHASE, &control).unwrap(), None);
    let text = view.item(0, PHASE, &control).unwrap().unwrap();
    assert_eq!(text.as_ptr(), original.value(2).as_ptr());
    assert_eq!(value.ordinal(), 1);
    assert_eq!(p.backing_identity(), backing);
    assert!(Arc::ptr_eq(p.field_ref(), &field));
    assert_eq!(value.value_type(), &value_type);
    assert_eq!(
        p.value(0)
            .unwrap()
            .utf8_list_observed(PHASE, &control)
            .unwrap()
            .unwrap()
            .item(0, PHASE, &control)
            .unwrap(),
        Some("prefix"),
    );
}

#[test]
fn utf8_list_root_null_child_null_empty_and_real_parent_slice_are_distinct() {
    let (array, _) = utf8_list();
    let p = pool(Arc::new(array.clone()));
    let control = Control::default();
    assert!(
        p.value(2)
            .unwrap()
            .utf8_list_observed(PHASE, &control)
            .unwrap()
            .is_none()
    );
    let empty_value = p.value(3).unwrap();
    let empty = empty_value
        .utf8_list_observed(PHASE, &control)
        .unwrap()
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert!(matches!(
        empty.item(0, PHASE, &control),
        Err(ConstantError::Invalid(_))
    ));
    let selected = p.value(1).unwrap();
    let view = selected
        .utf8_list_observed(PHASE, &control)
        .unwrap()
        .unwrap();
    assert_eq!(view.item(2, PHASE, &control).unwrap(), None);
    let sliced = pool(Arc::new(array.slice(1, 4)));
    let selected = sliced.value(3).unwrap();
    let view = selected
        .utf8_list_observed(PHASE, &control)
        .unwrap()
        .unwrap();
    assert_eq!(view.source().ordinal(), 3);
    assert_eq!(view.len(), 1);
    assert_eq!(view.item(0, PHASE, &control).unwrap(), Some("tail"));
    let selected = sliced.value(0).unwrap();
    let view = selected
        .utf8_list_observed(PHASE, &control)
        .unwrap()
        .unwrap();
    assert_eq!(view.item(0, PHASE, &control).unwrap(), Some("雪☃"));
    assert_eq!(view.item(2, PHASE, &control).unwrap(), None);
    assert!(
        sliced
            .value(1)
            .unwrap()
            .utf8_list_observed(PHASE, &control)
            .unwrap()
            .is_none()
    );
    assert!(
        sliced
            .value(2)
            .unwrap()
            .utf8_list_observed(PHASE, &control)
            .unwrap()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn utf8_list_required_children_keep_their_original_nullability() {
    let item = Arc::new(Field::new("source_required_item", DataType::Utf8, false));
    let array = ListArray::try_new(
        item,
        OffsetBuffer::new(vec![0_i32, 1, 2].into()),
        Arc::new(StringArray::from(vec!["first", "selected"])),
        None,
    )
    .unwrap();
    let field = Arc::new(Field::new(
        "required_list",
        array.data_type().clone(),
        false,
    ));
    let ty = FunctionValueType::try_from_field(&field).unwrap();
    let p = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        array.to_data(),
        policy(),
        PHASE,
        &Control::default(),
    )
    .unwrap();
    let value = p.value(1).unwrap();
    let view = value
        .utf8_list_observed(PHASE, &Control::default())
        .unwrap()
        .unwrap();
    assert_eq!(
        view.item(0, PHASE, &Control::default()).unwrap(),
        Some("selected")
    );
    assert!(!view.source().value_type().nullable());
    assert!(!view.source().value_type().child_at(0).unwrap().nullable());
    assert_eq!(view.source().value_type(), &ty);
    assert!(Arc::ptr_eq(view.source().pool().field_ref(), &field));
}

#[test]
fn utf8_list_resolves_only_original_root_dictionary_and_run_encodings() {
    let (array, _) = utf8_list();
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(1), Some(4), None, Some(2)]),
        Arc::new(array.clone()),
    )
    .unwrap();
    let p = pool(Arc::new(dictionary));
    let value = p.value(1).unwrap();
    let view = value
        .utf8_list_observed(PHASE, &Control::default())
        .unwrap()
        .unwrap();
    assert_eq!(view.source().ordinal(), 1);
    assert_eq!(
        view.item(0, PHASE, &Control::default()).unwrap(),
        Some("tail")
    );
    for ordinal in [2, 3] {
        assert!(
            p.value(ordinal)
                .unwrap()
                .utf8_list_observed(PHASE, &Control::default())
                .unwrap()
                .is_none()
        );
    }
    let run =
        RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 4, 6, 8, 10]), &array).unwrap();
    let p = pool(Arc::new(run));
    let value = p.value(3).unwrap();
    let view = value
        .utf8_list_observed(PHASE, &Control::default())
        .unwrap()
        .unwrap();
    assert_eq!(view.source().ordinal(), 3);
    assert_eq!(
        view.item(0, PHASE, &Control::default()).unwrap(),
        Some("雪☃")
    );
    assert!(
        p.value(5)
            .unwrap()
            .utf8_list_observed(PHASE, &Control::default())
            .unwrap()
            .is_none()
    );
    assert!(
        p.value(6)
            .unwrap()
            .utf8_list_observed(PHASE, &Control::default())
            .unwrap()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn utf8_list_wrong_nonnull_carriers_and_bad_indices_keep_original_control_tails() {
    let large = LargeListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(vec![0_i64, 1].into()),
        Arc::new(StringArray::from(vec!["value"])),
        None,
    )
    .unwrap();
    let large_text = ListArray::try_new(
        Arc::new(Field::new("item", DataType::LargeUtf8, true)),
        OffsetBuffer::new(vec![0_i32, 1].into()),
        Arc::new(arrow_array::LargeStringArray::from(vec!["value"])),
        None,
    )
    .unwrap();
    for array in [
        Arc::new(list()) as ArrayRef,
        Arc::new(large),
        Arc::new(large_text),
    ] {
        let value = pool(array).value(0).unwrap();
        assert_prefix(
            |control| value.utf8_list_observed(PHASE, control).map(|_| ()),
            true,
        );
    }
    let value = pool(Arc::new(utf8_list().0)).value(1).unwrap();
    for index in [3, usize::MAX] {
        assert_prefix(
            |control| {
                value
                    .utf8_list_observed(PHASE, control)?
                    .unwrap()
                    .item(index, PHASE, control)
            },
            true,
        );
    }
}

#[test]
fn utf8_list_reads_reuse_caller_meter_and_never_read_null_payloads() {
    let hidden = "x".repeat(320 * 1024);
    let mut bytes = Vec::from(b"one".as_slice());
    bytes.extend_from_slice(hidden.as_bytes());
    let values: ArrayRef = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0_i32, 3, i32::try_from(bytes.len()).unwrap()].into()),
        Buffer::from(bytes),
        Some(NullBuffer::from(vec![true, false])),
    ));
    let array = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(vec![0_i32, 2].into()),
        values,
        None,
    )
    .unwrap();
    let value = pool(Arc::new(array)).value(0).unwrap();
    let view = value
        .utf8_list_observed(PHASE, &Control::default())
        .unwrap()
        .unwrap();
    let control = Control::default();
    assert_eq!(view.item(1, PHASE, &control).unwrap(), None);
    assert!(!control.trace.lock().unwrap().contains(&256));
    assert_eq!(view.item(0, PHASE, &control).unwrap(), Some("one"));
    assert_prefix(
        |control| {
            let mut work = CompileCheckpoints::try_new(control, PHASE)?;
            let result = (|| {
                let view = value.utf8_list_in(&mut work)?.unwrap();
                for index in 0..view.len() {
                    view.item_observed(index, &mut work)?;
                }
                Ok(())
            })();
            finish(work, result)
        },
        false,
    );
    let source = pool(Arc::new(utf8_list().0));
    for ordinal in [1, 2, 3] {
        let value = source.value(ordinal).unwrap();
        assert_prefix(
            |control| {
                if let Some(view) = value.utf8_list_observed(PHASE, control)? {
                    for index in 0..view.len() {
                        view.item(index, PHASE, control)?;
                    }
                }
                Ok(())
            },
            false,
        );
    }
}
