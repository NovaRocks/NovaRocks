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

use arrow_array::{ArrayRef, Int32Array, LargeListArray, ListArray, MapArray, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields};
use novarocks_value_arrays::{ValueFailure, visit_reachable_child_rows_observed};
use std::{convert::Infallible, ops::Range, sync::Arc};

fn visits(source: &ArrayRef, rows: impl IntoIterator<Item = usize>) -> Vec<(usize, Range<usize>)> {
    let mut out = Vec::new();
    visit_reachable_child_rows_observed(
        source,
        rows,
        &mut |_| Ok::<_, Infallible>(()),
        &mut |ordinal, rows| {
            out.push((ordinal, rows));
            Ok(())
        },
    )
    .unwrap();
    out
}

#[test]
fn null_struct_parents_hide_children_and_selected_rows_keep_occurrences() {
    let fields: Fields = vec![
        Arc::new(Field::new("a", DataType::Int32, true)),
        Arc::new(Field::new("b", DataType::Int32, true)),
    ]
    .into();
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            fields,
            vec![
                Arc::new(Int32Array::from(vec![None, Some(5), Some(8)])),
                Arc::new(Int32Array::from(vec![None, Some(6), Some(9)])),
            ],
            Some(NullBuffer::from(vec![false, true, true])),
        )
        .unwrap(),
    );
    // The first parent is NULL; each field of parent 2 is visited twice.
    assert_eq!(
        visits(&source, [0, 2, 2]),
        vec![(0, 2..3), (1, 2..3), (0, 2..3), (1, 2..3)]
    );
}

#[test]
fn sliced_lists_use_child_offsets_and_skip_null_parent_ranges() {
    for large in [false, true] {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]));
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let validity = Some(NullBuffer::from(vec![true, true, false, true]));
        let source: ArrayRef = if large {
            Arc::new(
                LargeListArray::try_new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(vec![1i64, 3, 4, 6, 7])),
                    values,
                    validity,
                )
                .unwrap(),
            )
        } else {
            Arc::new(
                ListArray::try_new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(vec![1i32, 3, 4, 6, 7])),
                    values,
                    validity,
                )
                .unwrap(),
            )
        };
        let source = source.slice(1, 3);
        // Local parent 0 references value 3, parent 1 is NULL, parent 2 value 6.
        assert_eq!(visits(&source, 0..3), vec![(0, 3..4), (0, 6..7)]);
    }
}

#[test]
fn sliced_maps_report_key_and_value_coordinates_without_an_entries_wrapper() {
    let fields: Fields = vec![
        Arc::new(Field::new("key", DataType::Int32, false)),
        Arc::new(Field::new("value", DataType::Int32, true)),
    ]
    .into();
    let entries = StructArray::try_new(
        fields.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5])),
            Arc::new(Int32Array::from(vec![10, 11, 12, 13, 14, 15])),
        ],
        None,
    )
    .unwrap();
    let source: ArrayRef = Arc::new(
        MapArray::try_new(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![1i32, 2, 4, 5])),
            entries,
            Some(NullBuffer::from(vec![true, false, true])),
            false,
        )
        .unwrap(),
    );
    let source = source.slice(1, 2);
    // Local parent 0 is NULL, local parent 1 references entry 4 in both arrays.
    assert_eq!(visits(&source, 0..2), vec![(0, 4..5), (1, 4..5)]);
}

#[test]
fn nested_parent_validity_composes_without_reconstructing_child_geometry() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
    let list: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 4, 5])),
            values,
            Some(NullBuffer::from(vec![false, true, true])),
        )
        .unwrap(),
    );
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::new(Field::new(
                "items",
                list.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::clone(&list)],
            Some(NullBuffer::from(vec![true, false, true])),
        )
        .unwrap(),
    );
    let list_rows = visits(&source, 0..3).into_iter().flat_map(|(_, rows)| rows);
    // Struct parent 1 is NULL, list parent 0 is NULL, so only value 4 survives.
    assert_eq!(visits(&list, list_rows), vec![(0, 4..5)]);
}

#[test]
fn an_empty_parent_selection_does_no_work_and_reports_no_children() {
    let source: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    visit_reachable_child_rows_observed(
        &source,
        [],
        &mut |_| panic!("empty selection must not observe work"),
        &mut |_, _| -> Result<(), Infallible> { panic!("empty selection must not visit a child") },
    )
    .unwrap();
}

#[test]
fn caller_work_and_visitor_failures_keep_the_original_failure() {
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::new(Field::new("a", DataType::Int32, false))].into(),
            vec![Arc::new(Int32Array::from(vec![1]))],
            None,
        )
        .unwrap(),
    );
    let work_failure = visit_reachable_child_rows_observed(
        &source,
        [0],
        &mut |_| Err("work stopped"),
        &mut |_, _| panic!("work refused before the visitor"),
    );
    assert!(matches!(
        work_failure,
        Err(ValueFailure::Caller("work stopped"))
    ));
    let visitor_failure =
        visit_reachable_child_rows_observed(&source, [0], &mut |_| Ok(()), &mut |_, _| {
            Err("visitor stopped")
        });
    assert!(matches!(
        visitor_failure,
        Err(ValueFailure::Caller("visitor stopped"))
    ));
}
