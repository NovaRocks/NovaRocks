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
use crate::{
    exec::{
        hash_table::partition_keys::prepare_canonical_tuple,
        pipeline::operator::InputConstructionOwner,
    },
    runtime::mem_tracker::MemTracker,
};
use arrow::array::DictionaryArray;
use arrow::datatypes::{Field, Int32Type};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use novarocks_type_contract::FunctionValueType;
use novarocks_value_arrays::{decode::ValueConversionContext, reachability::ReachableRows};
use std::cell::Cell;

const BINARY_VALUE: &[u8] = &[0, 255];

struct Control;
impl novarocks_functions::KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), novarocks_functions::KernelFailure> {
        assert!(units <= novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK);
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
        panic!("canonical byte decoding never waits")
    }
}
fn owner() -> Box<dyn InputConstructionOwner> {
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        MemTracker::new_root("canonical nested byte construction"),
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    ).unwrap();
    let declaration = crate::exec::chunk::typed_fixture::layout(
        Arc::new(arrow::datatypes::Schema::empty()),
        &[],
        &[],
    );
    let schema = factory.create_compiled_chunk_schema(&declaration).unwrap();
    factory.begin_typed_construction(&schema).unwrap()
}
fn nullable(payload: &[u8]) -> Vec<u8> {
    let mut result = vec![1];
    result.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
    result.extend_from_slice(payload);
    result
}
fn fnv(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
    })
}
fn list(values: ArrayRef, offsets: Vec<i32>) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        None,
    ))
}

#[test]
fn nested_string_carriers_preserve_original_frames_for_nonnull_null_and_empty_values() {
    let values = [Some("skip"), Some("a"), None, Some("")];
    let children: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(values.to_vec())),
        Arc::new(StringViewArray::from(values.to_vec())),
        Arc::new(LargeStringArray::from(values.to_vec())),
    ];
    // Hand-authored original frame: List["a", NULL, ""]. This expectation is
    // independent of all encoder, sizing and target-layout selection helpers.
    let expected = vec![
        16, 3, 0, 0, 0, 1, 6, 0, 0, 0, 8, 1, 0, 0, 0, b'a', 0, 1, 5, 0, 0, 0, 8, 0, 0, 0, 0,
    ];
    let empty = [16, 0, 0, 0, 0];
    let constructor = owner();
    for children in children {
        let source = list(children, vec![0, 1, 4, 4]).slice(1, 2);
        assert_eq!(
            encoded_group_key_non_null_len(&source, 0).unwrap(),
            expected.len()
        );
        assert_eq!(encode_group_key_row(&source, 0).unwrap().unwrap(), expected);
        assert_eq!(encode_group_key_row(&source, 1).unwrap().unwrap(), empty);
        assert_eq!(
            canonical_group_key_fnv_hash(&source, 0).unwrap(),
            Some(fnv(&expected))
        );
        assert!(
            canonical_nullable_equals_observed(&source, 0, &nullable(&expected), &|| Ok(()))
                .unwrap()
        );
        assert!(
            !canonical_nullable_equals_observed(&source, 1, &nullable(&expected), &|| Ok(()))
                .unwrap()
        );
        let decoded = decode_group_key_rows_construction(
            source.data_type(),
            &[Some(expected.as_slice()), None, Some(empty.as_slice())],
            constructor.as_ref(),
            &Control,
        )
        .unwrap();
        assert_eq!(decoded.data_type(), source.data_type());
        let lists = decoded.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.value_offsets(), &[0, 3, 3, 3]);
        assert!(lists.is_null(1));
        let source_children = source
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .slice(1, 3);
        assert_eq!(lists.values().to_data(), source_children.to_data());
    }
}

#[test]
fn nested_binary_view_shares_binary_frame_while_largebinary_keeps_its_existing_marker() {
    let values = vec![Some(b"skip".as_slice()), Some(BINARY_VALUE)];
    let children: Vec<(ArrayRef, u8)> = vec![
        (Arc::new(BinaryArray::from(values.clone())), 20),
        (Arc::new(BinaryViewArray::from(values.clone())), 20),
        (Arc::new(LargeBinaryArray::from(values)), 21),
    ];
    for (children, marker) in children {
        let source = list(children, vec![0, 1, 2]).slice(1, 1);
        let expected = [16, 1, 0, 0, 0, 1, 7, 0, 0, 0, marker, 2, 0, 0, 0, 0, 255];
        assert_eq!(
            encoded_group_key_non_null_len(&source, 0).unwrap(),
            expected.len()
        );
        assert_eq!(encode_group_key_row(&source, 0).unwrap().unwrap(), expected);
        assert_eq!(
            canonical_group_key_fnv_hash(&source, 0).unwrap(),
            Some(fnv(&expected))
        );
        let constructor = owner();
        let decoded = decode_group_key_rows_construction(
            source.data_type(),
            &[Some(expected.as_slice())],
            constructor.as_ref(),
            &Control,
        )
        .unwrap();
        assert_eq!(decoded.data_type(), source.data_type());
        let decoded_values = decoded
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values();
        let source_values = source
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .slice(1, 1);
        assert_eq!(decoded_values.to_data(), source_values.to_data());
    }
}

#[test]
fn governed_decoder_materializes_aliases_in_the_exact_requested_plain_layout() {
    let owner = owner();
    let text = [8, 1, 0, 0, 0, b'a'];
    let binary = [20, 2, 0, 0, 0, 0, 255];
    let large_binary = [21, 2, 0, 0, 0, 0, 255];
    let strings = vec![Some("a"), None, Some("a")];
    let binary_values = vec![Some(BINARY_VALUE), None, Some(BINARY_VALUE)];
    let cases: Vec<(ArrayRef, &[u8])> = vec![
        (Arc::new(StringArray::from(strings.clone())), &text),
        (Arc::new(StringViewArray::from(strings.clone())), &text),
        (Arc::new(LargeStringArray::from(strings)), &text),
        (Arc::new(BinaryArray::from(binary_values.clone())), &binary),
        (
            Arc::new(BinaryViewArray::from(binary_values.clone())),
            &binary,
        ),
        (
            Arc::new(LargeBinaryArray::from(binary_values)),
            &large_binary,
        ),
    ];
    for (expected, payload) in cases {
        let output = decode_group_key_rows_construction(
            expected.data_type(),
            &[Some(payload), None, Some(payload)],
            owner.as_ref(),
            &Control,
        )
        .unwrap();
        assert_eq!(output.data_type(), expected.data_type());
        assert_eq!(output.to_data(), expected.to_data());
        let empty =
            decode_group_key_rows_construction(expected.data_type(), &[], owner.as_ref(), &Control)
                .unwrap();
        assert_eq!(empty.data_type(), expected.data_type());
        assert_eq!(empty.len(), 0);
    }
}

#[test]
fn prepared_nested_dictionary_preserves_selected_roots_logical_nulls_and_exact_frames() {
    let owner = owner();
    let unused = "unselected external dictionary bytes".repeat(16_384);
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        None,
        Some(unused.as_str()),
    ]));
    let keys = Int32Array::from(vec![Some(0), Some(1), None, Some(2)]);
    let dictionary: ArrayRef =
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());
    let arrays = [list(dictionary, vec![0, 3, 4])];
    let binding = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true))),
        true,
    );
    let mut conversions = ValueConversionContext::default();
    let prepared = prepare_canonical_tuple(
        &arrays,
        &[binding],
        &ReachableRows::indices(2, &[0]),
        &mut conversions,
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap()
    .into_storage();
    let view = prepared.view(&arrays);
    let (normalized, _) = view.physical_component(0).unwrap();
    assert_eq!(normalized.len(), 1);
    let values = normalized
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values();
    let strings = values.as_any().downcast_ref::<StringViewArray>().unwrap();
    assert_eq!(strings.len(), 3);
    assert_eq!(strings.value(0), "a");
    assert!(strings.is_null(1));
    assert!(strings.is_null(2));
    // Only dictionary bottom rows 0 and 1 are reachable in the selected
    // root; reuse may avoid conversion, but the unselected third bottom value
    // must not be converted. Prepared views may retain actual source custody
    // or an owned inline region; neither implies an empty buffer table.
    assert!(conversions.stats().converted_values <= 2);
    // One selected list, containing a value, bottom NULL, then dictionary-key NULL.
    let expected = [
        1, 18, 0, 0, 0, 16, 3, 0, 0, 0, 1, 6, 0, 0, 0, 8, 1, 0, 0, 0, b'a', 0, 0,
    ];
    let allocator =
        AggregateAllocator::new(MemTracker::new_root("nested dictionary canonical bytes"));
    assert_eq!(
        view.encode_component_row(0, 0, allocator.clone(), &|| Ok(()))
            .unwrap()
            .as_slice(),
        expected
    );
    assert!(
        view.encode_component_row(0, 1, allocator, &|| Ok(()))
            .is_err()
    );
}

#[test]
fn long_nested_view_encoding_observes_cancellation_inside_the_original_byte_writer() {
    let text = "文x".repeat(40_000);
    let children: ArrayRef = Arc::new(StringViewArray::from(vec![text.as_str()]));
    let source = list(children, vec![0, 1]);
    let mut payload = vec![16, 1, 0, 0, 0, 1];
    payload.extend_from_slice(&u32::try_from(5 + text.len()).unwrap().to_le_bytes());
    payload.push(8);
    payload.extend_from_slice(&u32::try_from(text.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(text.as_bytes());
    let expected = nullable(&payload);
    let allocator =
        AggregateAllocator::new(MemTracker::new_root("long nested view canonical bytes"));
    let baseline = Cell::new(0usize);
    let mut result = AggregateVec::new_in(allocator.clone());
    append_canonical_nullable_tracked_observed(&source, 0, &mut result, &|| {
        baseline.set(baseline.get() + 1);
        Ok(())
    })
    .unwrap();
    assert_eq!(result.as_slice(), expected);
    let original = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded);
    let mut stopped_mid_value = false;
    for stop in 1..=baseline.get() {
        let observed = Cell::new(0usize);
        let mut partial = AggregateVec::new_in(allocator.clone());
        let error = append_canonical_nullable_tracked_observed(&source, 0, &mut partial, &|| {
            let next = observed.get() + 1;
            observed.set(next);
            if next == stop {
                Err(original.clone())
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert_eq!(
            error.failure_fact().class(),
            original.failure_fact().class()
        );
        assert_eq!(
            error.failure_fact().reason(),
            original.failure_fact().reason()
        );
        assert_eq!(observed.get(), stop);
        assert_eq!(partial.as_slice(), &expected[..partial.len()]);
        stopped_mid_value |= partial.len() > 32 && partial.len() < expected.len();
    }
    assert!(
        stopped_mid_value,
        "a single long nested leaf must expose a real internal byte-copy checkpoint"
    );
}
