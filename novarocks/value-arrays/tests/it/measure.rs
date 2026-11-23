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

use arrow_array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, Int32Array, Int64Array, ListArray, StringArray,
    StringViewArray, StructArray, types::Int32Type,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields};
use novarocks_value_arrays::{
    ValueError, ValueFailure,
    control::{global_growth, uncontrolled},
    measure::{OutputColumn, OutputRows, PrefixPlanner},
};
use std::{num::NonZeroU32, sync::Arc};

fn cap(rows: u32) -> NonZeroU32 {
    NonZeroU32::new(rows).unwrap()
}

#[test]
fn additional_column_storage_preserves_the_original_prefix_and_first_row_rule() {
    let source: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2, 3]));
    let mut planner = PrefixPlanner::new(
        &[OutputColumn::new(&source, &DataType::Int32)],
        OutputRows::Contiguous(1..4),
        &mut global_growth,
        &mut uncontrolled,
    )
    .unwrap();
    let mut measured = Vec::new();
    let mut storage = |source_row| {
        measured.push(source_row);
        Ok::<_, std::convert::Infallible>(if source_row == 3 { 40 } else { 14 })
    };
    let first = planner
        .plan_prefix_with_additional_storage(cap(8), 28, Some(&mut storage), &mut uncontrolled)
        .unwrap();
    assert_eq!((first.rows(), first.new_column_bytes()), (2, 28));
    assert!(first.shares_source());
    let rows = first.rows();
    planner.advance(rows).unwrap();
    let last = planner
        .plan_prefix_with_additional_storage(cap(8), 28, Some(&mut storage), &mut uncontrolled)
        .unwrap();
    assert_eq!((last.rows(), last.new_column_bytes()), (1, 40));
    assert_eq!(measured, [1, 2, 3, 3]);
}

#[test]
fn logical_bytes_ignore_carrier_and_keep_dictionary_occurrences() {
    let long = "a long string value";
    let offsets: ArrayRef = Arc::new(StringArray::from(vec![Some(long), None, Some(long)]));
    let views: ArrayRef = Arc::new(StringViewArray::from(vec![Some(long), None, Some(long)]));
    let values: ArrayRef = Arc::new(StringArray::from(vec![Some(long), None]));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1), Some(0)]),
            Arc::clone(&values),
        )
        .unwrap(),
    );
    for (source, target, expected_bytes, shares) in [
        (&offsets, &DataType::Utf8, 0, true),
        (&views, &DataType::Utf8View, 0, true),
        (&views, &DataType::Utf8, 17 + 2 * long.len() as u128, false),
        (&dictionary, dictionary.data_type(), 0, true),
        (
            &dictionary,
            &DataType::Utf8,
            17 + 2 * long.len() as u128,
            false,
        ),
        (&dictionary, &DataType::Utf8View, 49, true),
    ] {
        let mut planner = PrefixPlanner::new(
            &[OutputColumn::new(source, target)],
            OutputRows::Contiguous(0..3),
            &mut global_growth,
            &mut uncontrolled,
        )
        .unwrap();
        let plan = planner
            .plan_prefix(cap(8), 1024, &mut uncontrolled)
            .unwrap();
        assert_eq!(plan.rows(), 3);
        assert_eq!(plan.logical_bytes(), 2 * long.len() as u128);
        assert_eq!(plan.new_column_bytes(), expected_bytes);
        assert_eq!(plan.shares_source(), shares);
        let arrays = plan
            .build_observed(&mut global_growth, &mut uncontrolled)
            .unwrap();
        assert_eq!(arrays[0].data_type(), target);
        if let Some(array) = arrays[0].as_any().downcast_ref::<StringArray>() {
            assert_eq!(
                array.iter().collect::<Vec<_>>(),
                [Some(long), None, Some(long)]
            );
        }
        if let Some(array) = arrays[0].as_any().downcast_ref::<StringViewArray>() {
            assert_eq!(
                array.iter().collect::<Vec<_>>(),
                [Some(long), None, Some(long)]
            );
        }
        if let Some(array) = arrays[0]
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
        {
            assert!(Arc::ptr_eq(array.values(), &values));
        }
    }
}

#[test]
fn selecting_dictionary_rows_reuses_the_values_and_copies_only_keys_and_validity() {
    let values: ArrayRef = Arc::new(StringArray::from(vec![Some("first"), None, Some("last")]));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1), None, Some(2)]),
            Arc::clone(&values),
        )
        .unwrap(),
    );
    let mut planner = PrefixPlanner::new(
        &[OutputColumn::new(&dictionary, dictionary.data_type())],
        OutputRows::SharedSelected(Arc::from([3, 0, 3, 2, 1])),
        &mut global_growth,
        &mut uncontrolled,
    )
    .unwrap();
    let plan = planner
        .plan_prefix(cap(5), 1024, &mut uncontrolled)
        .unwrap();
    assert_eq!(plan.logical_bytes(), 13);
    assert_eq!(plan.new_column_bytes(), 21);
    assert!(plan.shares_source());
    let arrays = plan
        .build_observed(&mut global_growth, &mut uncontrolled)
        .unwrap();
    let array = arrays[0]
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .unwrap();
    assert!(Arc::ptr_eq(array.values(), &values));
    assert_eq!(
        array.keys().iter().collect::<Vec<_>>(),
        [Some(2), Some(0), Some(2), None, Some(1)]
    );
}

#[test]
fn all_columns_share_the_selected_prefix_and_keep_the_rejected_boundary_row() {
    let strings: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("a"),
        Some("bbbb"),
        Some("cccc"),
    ]));
    let flags: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
    let mut planner = PrefixPlanner::new(
        &[
            OutputColumn::new(&strings, &DataType::Utf8),
            OutputColumn::new(&flags, &DataType::Boolean),
        ],
        OutputRows::Selected(&[2, 0, 1]),
        &mut global_growth,
        &mut uncontrolled,
    )
    .unwrap();
    let first = planner.plan_prefix(cap(3), 18, &mut uncontrolled).unwrap();
    // Offsets (three i32), selected payload (4+1), one packed Boolean byte.
    assert_eq!(
        (
            first.rows(),
            first.logical_bytes(),
            first.new_column_bytes()
        ),
        (2, 7, 18)
    );
    let arrays = first
        .build_observed(&mut global_growth, &mut uncontrolled)
        .unwrap();
    let strings = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        strings.iter().collect::<Vec<_>>(),
        [Some("cccc"), Some("a")]
    );
    planner.advance(2).unwrap();

    // The row already inspected at the boundary can start the next page
    // without repeating the Work for either column.
    let mut checkpoints = Vec::new();
    let mut work = |units| {
        checkpoints.push(units);
        Ok::<_, ()>(())
    };
    let second = planner.plan_prefix(cap(3), 0, &mut work).unwrap();
    assert_eq!(
        (
            second.rows(),
            second.logical_bytes(),
            second.new_column_bytes()
        ),
        (1, 5, 13)
    );
    assert_eq!(checkpoints, [0]);
    let arrays = second
        .build_observed(&mut global_growth, &mut uncontrolled)
        .unwrap();
    assert_eq!(
        arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "bbbb"
    );
    planner.advance(1).unwrap();
    assert_eq!(planner.remaining_rows(), 0);
}

#[test]
fn nested_logical_count_and_build_skip_unreachable_child_values() {
    let item = Arc::new(Field::new("item", DataType::Utf8View, true));
    let strings: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("ignored"),
        Some("aa"),
        None,
        Some("bb"),
    ]));
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::clone(&item),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 4])),
        strings,
        None,
    ));
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![99, 42]));
    let fields = Fields::from(vec![
        Field::new("bytes", list.data_type().clone(), true),
        Field::new("count", DataType::Int64, true),
    ]);
    let source: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![list, counts],
        Some(NullBuffer::from(vec![false, true])),
    ));
    let target = DataType::Struct(Fields::from(vec![
        Field::new(
            "bytes",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("count", DataType::Int64, true),
    ]));
    let mut planner = PrefixPlanner::new(
        &[OutputColumn::new(&source, &target)],
        OutputRows::Selected(&[0, 1, 1]),
        &mut global_growth,
        &mut uncontrolled,
    )
    .unwrap();
    let plan = planner
        .plan_prefix(cap(10), 1024, &mut uncontrolled)
        .unwrap();
    assert_eq!(plan.logical_bytes(), 24);
    // Struct validity 1, list offsets and validity 17, strings offsets,
    // validity and payload 37, int64 slots and validity 25.
    assert_eq!(plan.new_column_bytes(), 80);
    assert!(!plan.shares_source());
    let arrays = plan
        .build_observed(&mut global_growth, &mut uncontrolled)
        .unwrap();
    let output = arrays[0].as_any().downcast_ref::<StructArray>().unwrap();
    assert!(output.is_null(0));
    assert_eq!(output.data_type(), &target);
    let lists = output
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(lists.value_offsets(), [0, 0, 3, 6]);
    let values = lists
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        [Some("aa"), None, Some("bb"), Some("aa"), None, Some("bb")]
    );
}

#[test]
fn repeated_rows_allow_a_wide_first_row_and_stop_at_real_offsets() {
    let value = "x".repeat(512 * 1024);
    let source: ArrayRef = Arc::new(StringArray::from(vec![value.as_str()]));
    let mut planner = PrefixPlanner::new(
        &[OutputColumn::constant(&source, &DataType::Utf8)],
        OutputRows::Contiguous(0..8192),
        &mut global_growth,
        &mut uncontrolled,
    )
    .unwrap();
    let singleton = planner
        .plan_prefix(cap(8192), 0, &mut uncontrolled)
        .unwrap();
    assert_eq!(singleton.rows(), 1);
    assert_eq!(singleton.logical_bytes(), value.len() as u128);
    assert_eq!(singleton.new_column_bytes(), value.len() as u128 + 8);
    assert!(!singleton.shares_source());
    planner.advance(1).unwrap();

    let prefix = planner
        .plan_prefix(cap(8192), u64::MAX, &mut uncontrolled)
        .unwrap();
    assert_eq!(prefix.rows(), 4095);
    assert_eq!(prefix.logical_bytes(), 4095 * value.len() as u128);
}

#[test]
fn zero_columns_keep_positions_and_control_failures_remain_original() {
    let mut planner = PrefixPlanner::new(
        &[],
        OutputRows::Contiguous(0..2500),
        &mut global_growth,
        &mut uncontrolled,
    )
    .unwrap();
    assert!(matches!(
        planner.plan_prefix(cap(1024), 0, &mut |_| Err("stopped")),
        Err(ValueFailure::Caller("stopped"))
    ));
    let plan = planner
        .plan_prefix(cap(1024), 0, &mut uncontrolled)
        .unwrap();
    assert_eq!(
        (plan.rows(), plan.logical_bytes(), plan.new_column_bytes()),
        (1024, 0, 0)
    );
    assert!(!plan.shares_source());
    assert!(
        plan.build_observed(&mut global_growth, &mut uncontrolled)
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        planner.advance(1023),
        Err(ValueError::Invariant(_))
    ));
    planner.advance(1024).unwrap();
    assert_eq!(planner.remaining_rows(), 1476);
}

#[test]
fn fixed_width_slice_counts_the_selected_validity_bitmap() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![
        None,
        Some(1),
        None,
        Some(2),
        Some(3),
    ]));
    let source = source.slice(1, 4);
    let mut planner = PrefixPlanner::new(
        &[OutputColumn::new(&source, &DataType::Int64)],
        OutputRows::Contiguous(1..4),
        &mut global_growth,
        &mut uncontrolled,
    )
    .unwrap();
    let plan = planner.plan_prefix(cap(3), 0, &mut uncontrolled).unwrap();
    assert_eq!(
        (plan.rows(), plan.logical_bytes(), plan.new_column_bytes()),
        (3, 16, 0)
    );
    assert!(plan.shares_source());

    let mut gathered = PrefixPlanner::new(
        &[OutputColumn::new(&source, &DataType::Int64)],
        OutputRows::Selected(&[2, 0, 1, 2]),
        &mut global_growth,
        &mut uncontrolled,
    )
    .unwrap();
    let plan = gathered
        .plan_prefix(cap(8), 1024, &mut uncontrolled)
        .unwrap();
    assert_eq!(
        (plan.rows(), plan.logical_bytes(), plan.new_column_bytes()),
        (4, 24, 33)
    );
    let arrays = plan
        .build_observed(&mut global_growth, &mut uncontrolled)
        .unwrap();
    let values = arrays[0].as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        [Some(2), Some(1), None, Some(2)]
    );

    let strings: ArrayRef = Arc::new(StringViewArray::from(vec!["p", "a", "b", "c"]));
    let constant: ArrayRef = Arc::new(Int32Array::from(vec![7]));
    let mut mixed = PrefixPlanner::new(
        &[
            OutputColumn::new(&strings, &DataType::Utf8),
            OutputColumn::new(&source, &DataType::Int64),
            OutputColumn::constant(&constant, &DataType::Int32),
        ],
        OutputRows::Contiguous(1..4),
        &mut global_growth,
        &mut uncontrolled,
    )
    .unwrap();
    let plan = mixed.plan_prefix(cap(8), 31, &mut uncontrolled).unwrap();
    assert_eq!(
        (plan.rows(), plan.logical_bytes(), plan.new_column_bytes()),
        (3, 31, 31)
    );
    assert!(plan.shares_source());
}

#[test]
fn minimum_nested_row_fails_by_real_representation_without_large_allocation() {
    use novarocks_failure_contract::RepresentationBoundary;

    let value = "x".repeat(128 * 1024);
    let values: ArrayRef = Arc::new(StringArray::from(vec![value.as_str()]));
    let repetitions = 16_385;
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0; repetitions]), values)
            .unwrap(),
    );
    let source: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", dictionary.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, repetitions as i32])),
        dictionary,
        None,
    ));
    let target = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    let mut planner = PrefixPlanner::new(
        &[OutputColumn::new(&source, &target)],
        OutputRows::Contiguous(0..1),
        &mut global_growth,
        &mut uncontrolled,
    )
    .unwrap();
    let Err(ValueFailure::Value(ValueError::Representation(facts))) =
        planner.plan_prefix(cap(8), u64::MAX, &mut uncontrolled)
    else {
        panic!("minimum complete row should not fit 32-bit offsets");
    };
    assert_eq!(facts.boundary, RepresentationBoundary::MinimumUnit);
    assert_eq!(facts.representable_prefix, Some(0));
    assert_eq!(facts.extent_limit, Some(i32::MAX as u64));
    assert_eq!(facts.required, Some((repetitions * value.len()) as u64));
    assert_eq!(planner.remaining_rows(), 1);
}
