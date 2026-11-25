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

use arrow_array::{Float64Array, Int64Array, MapArray, StructArray, types::Int64Type};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{Field, Fields};

use super::*;
use crate::FunctionValueType;
use crate::builtin::array_invocation_test_support::{Control, Host};

fn prepared(operation: Operation, left: &ArrayRef, right: Option<&ArrayRef>) -> PreparedRelation {
    let prepare = |array: &ArrayRef| {
        PreparedKeyOperations::prepare_observed(
            &FunctionValueType::new(array.data_type().clone(), true),
            operation.relation(),
            &mut |_| Ok::<_, KernelFailure>(()),
        )
        .unwrap()
    };
    PreparedRelation {
        operation,
        keys: prepare(left),
        right_keys: right.map(prepare),
        flat: !matches!(left.data_type(), DataType::List(_) | DataType::Map(_, _)),
    }
}

fn side(values: &ArrayRef) -> Side<'_> {
    Side {
        values,
        range: 0..values.len(),
    }
}

fn selected<'a>(left: &'a ArrayRef, right: Option<&'a ArrayRef>) -> SelectedRow<'a> {
    SelectedRow {
        left: side(left),
        right: right.map(side),
    }
}

fn array(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn list(values: Vec<Vec<Option<i64>>>) -> ArrayRef {
    Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
        values.into_iter().map(Some),
    ))
}

fn with_evaluation<T>(
    prepared: &PreparedRelation,
    control: &Control,
    run: impl FnOnce(&mut Evaluation<'_, '_>) -> T,
) -> T {
    let host = Host::with_limit(usize::MAX);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let mut work = EvaluationCheckpoints::new(control);
    let value = run(&mut Evaluation {
        prepared,
        allocator: &allocator,
        work: &mut work,
    });
    drop(allocator);
    host.released();
    value
}

fn output_positions(
    operation: Operation,
    left: &ArrayRef,
    right: Option<&ArrayRef>,
) -> Vec<(usize, usize)> {
    let prepared = prepared(operation, left, right);
    with_evaluation(&prepared, &Control::default(), |cx| {
        let mut segments = scratch(0, cx.allocator).unwrap();
        let mut output = RowOutput {
            segments: &mut segments,
            initial: 0,
            count: 0,
        };
        assert!(cx.list_row(&selected(left, right), &mut output).unwrap());
        segments
            .iter()
            .flat_map(|segment| {
                (segment.start..segment.start + segment.len)
                    .map(move |position| (segment.source, position))
            })
            .collect()
    })
}

#[test]
fn array_relation_flat_lookup_stops_before_the_denied_tail() {
    let mut values = vec![Some(1); 4096];
    values[0] = None;
    values[1] = Some(7);
    let left = array(values);
    let probe = array(vec![Some(7)]);
    let prepared = prepared(Operation::Contains, &left, Some(&probe));
    let control = Control {
        refusal: Some((0, KernelFailure::Cancelled)),
        ..Control::default()
    };
    with_evaluation(&prepared, &control, |cx| {
        assert!(matches!(
            cx.find(&selected(&left, Some(&probe))),
            Ok(Find::Present(1))
        ));
    });
    assert!(control.trace.lock().unwrap().is_empty());

    let probe = array(vec![Some(8)]);
    with_evaluation(&prepared, &control, |cx| {
        assert!(matches!(
            cx.find(&selected(&left, Some(&probe))),
            Err(KernelFailure::Cancelled)
        ));
    });
}

#[test]
fn array_relation_flat_unknown_means_only_a_top_level_null() {
    let left = array(vec![Some(7), None]);
    let probe = array(vec![Some(8)]);
    let prepared = prepared(Operation::Position, &left, Some(&probe));
    with_evaluation(&prepared, &Control::default(), |cx| {
        assert!(matches!(
            cx.find(&selected(&left, Some(&probe))).unwrap(),
            Find::Missing { saw_null: true },
        ));
    });
    assert_eq!(
        output_positions(Operation::Remove, &left, Some(&array(vec![Some(7)]))),
        [(0, 1)]
    );
}

#[test]
fn array_relation_composite_lookup_preserves_the_first_reached_comparison() {
    let unknown_first = list(vec![vec![None], vec![Some(7)]]);
    let match_first = list(vec![vec![Some(7)], vec![None]]);
    let probe = list(vec![vec![Some(7)]]);
    let prepared = prepared(Operation::Contains, &unknown_first, Some(&probe));
    with_evaluation(&prepared, &Control::default(), |cx| {
        assert!(matches!(
            cx.find(&selected(&unknown_first, Some(&probe))).unwrap(),
            Find::Uncertain
        ));
        assert!(matches!(
            cx.find(&selected(&match_first, Some(&probe))).unwrap(),
            Find::Present(0)
        ));
    });
}

#[test]
fn array_relation_identity_retains_null_float_and_intersection_representatives() {
    let first_nan = f64::from_bits(0x7ff8_0000_0000_0001);
    let other_nan = f64::from_bits(0x7ff8_0000_0000_0002);
    let values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-0.0),
        None,
        Some(first_nan),
        Some(0.0),
        Some(other_nan),
        None,
    ]));
    assert_eq!(
        output_positions(Operation::Distinct, &values, None),
        [(0, 0), (0, 1), (0, 2)]
    );

    let left = array(vec![Some(2), Some(1), None, Some(3)]);
    let right = array(vec![None, Some(3), Some(2), Some(2)]);
    assert_eq!(
        output_positions(Operation::Intersect, &left, Some(&right)),
        [(0, 0), (0, 2), (0, 3)]
    );
    let right = array(vec![None, Some(3), Some(3), Some(2), None]);
    assert_eq!(
        output_positions(Operation::Intersect, &left, Some(&right)),
        [(1, 0), (1, 1), (1, 3)]
    );
}

#[test]
fn array_relation_equal_hashes_must_still_compare_the_original_values() {
    let left = array(vec![Some(7), Some(8)]);
    let prepared = prepared(Operation::Distinct, &left, None);
    with_evaluation(&prepared, &Control::default(), |cx| {
        let bound = bind(&prepared.keys, &side(&left), cx.work).unwrap();
        let mut set = SetIndex::new(2, cx.allocator, cx.work).unwrap();
        set.slots[0] = Some(Entry {
            hash: 0,
            position: 0,
            matched: false,
        });
        assert!(matches!(
            set.find(0, &bound, 1, &bound, cx).unwrap(),
            Probe::Vacant(1)
        ));
        assert!(matches!(
            set.find(0, &bound, 0, &bound, cx).unwrap(),
            Probe::Found(0)
        ));
    });
}

#[test]
fn array_relation_overlap_and_contains_sequence_have_distinct_null_rules() {
    let left = array(vec![Some(1), None, Some(2)]);
    let right = array(vec![None, Some(2)]);
    let empty = array(Vec::new());
    let null = array(vec![None]);
    let unrelated = array(vec![Some(8)]);
    let overlap = prepared(Operation::Overlap, &left, Some(&right));
    let sequence = prepared(Operation::ContainsSequence, &left, Some(&right));
    with_evaluation(&overlap, &Control::default(), |cx| {
        assert_eq!(
            cx.overlap(&selected(&left, Some(&right))).unwrap(),
            Some(true)
        );
        assert_eq!(
            cx.overlap(&selected(&left, Some(&empty))).unwrap(),
            Some(false)
        );
        assert_eq!(cx.overlap(&selected(&left, Some(&null))).unwrap(), None);
        assert_eq!(
            cx.overlap(&selected(&unrelated, Some(&right))).unwrap(),
            None
        );
    });
    with_evaluation(&sequence, &Control::default(), |cx| {
        assert!(
            cx.contains_sequence(&selected(&left, Some(&right)))
                .unwrap()
        );
        assert!(
            cx.contains_sequence(&selected(&left, Some(&empty)))
                .unwrap()
        );
        assert!(
            !cx.contains_sequence(&selected(&right, Some(&left)))
                .unwrap()
        );
    });
}

#[test]
fn array_relation_map_identity_ignores_entry_order_and_releases_comparison_scratch() {
    let fields = Fields::from(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]);
    let entries = StructArray::new(
        fields,
        vec![
            array(vec![Some(1), Some(2), Some(2), Some(1)]),
            array(vec![None, Some(7), Some(7), None]),
        ],
        None,
    );
    let left: ArrayRef = Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::from_lengths([2, 2]),
        entries,
        None,
        false,
    ));
    assert_eq!(output_positions(Operation::Distinct, &left, None), [(0, 0)]);

    let host = Host::with_limit(usize::MAX);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let prepared = prepared(Operation::Distinct, &left, None);
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let bound = bind(&prepared.keys, &side(&left), &mut work).unwrap();
    let mut cx = Evaluation {
        prepared: &prepared,
        allocator: &allocator,
        work: &mut work,
    };
    let baseline = host.live_blocks();
    assert!(cx.identical(&bound, 0, &bound, 1).unwrap());
    assert_eq!(host.live_blocks(), baseline);
    host.arm_allocation(0, KernelFailure::DeadlineExceeded);
    assert!(matches!(
        cx.identical(&bound, 0, &bound, 1),
        Err(KernelFailure::DeadlineExceeded)
    ));
    assert_eq!(host.live_blocks(), baseline);
    drop(allocator);
    host.released();
}
