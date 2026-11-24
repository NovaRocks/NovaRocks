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
use arrow_array::{DictionaryArray, StringArray, StringViewArray, types::Int32Type};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{Field, Fields};
use novarocks_type_contract::{
    CharacterType, ValueLayout, ValueLogicalType, ValueTypeStorageLifetime,
};
use std::{convert::Infallible, sync::Arc};

fn work(_: usize) -> Result<(), Infallible> {
    Ok(())
}

fn growth(_: std::alloc::Layout) -> Result<(), Infallible> {
    Ok(())
}

fn prepared(array: &ArrayRef, relation: KeyRelation) -> PreparedKeyOperations {
    PreparedKeyOperations::prepare_observed(
        &FunctionValueType::new(array.data_type().clone(), true),
        relation,
        &mut work,
    )
    .unwrap()
}

fn lists<const N: usize>(rows: Vec<[Option<i32>; N]>) -> ArrayRef {
    Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        rows.into_iter().map(Some),
    ))
}

fn map_with_offsets(
    keys: ArrayRef,
    values: ArrayRef,
    lengths: impl IntoIterator<Item = usize>,
    nulls: Option<NullBuffer>,
) -> ArrayRef {
    let fields = Fields::from(vec![
        Field::new("key", keys.data_type().clone(), false),
        Field::new("value", values.data_type().clone(), true),
    ]);
    let entries = StructArray::new(fields, vec![keys, values], None);
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::from_lengths(lengths),
        entries,
        nulls,
        false,
    ))
}

fn map(keys: ArrayRef, values: ArrayRef) -> ArrayRef {
    let len = keys.len();
    map_with_offsets(keys, values, [len], None)
}

fn compare(left: &ArrayRef, right: &ArrayRef, relation: KeyRelation) -> KeyComparison {
    let left_ops = prepared(left, relation);
    let right_ops = prepared(right, relation);
    let left = left_ops
        .bind_observed(left, KeyRows::All, &mut work)
        .unwrap();
    let right = right_ops
        .bind_observed(right, KeyRows::All, &mut work)
        .unwrap();
    left_ops
        .compare_observed(&left, 0, &right, 0, &mut growth, &mut work)
        .unwrap()
}

#[test]
fn lookup_prioritizes_a_sure_match_and_false_is_not_unknown() {
    let candidates = lists(vec![[None, Some(1)], [Some(2), Some(1)]]);
    let probes = lists(vec![[Some(2), Some(1)], [Some(2), Some(9)]]);
    let operations = prepared(&candidates, KeyRelation::Equal);
    let probes = operations
        .bind_observed(&probes, KeyRows::All, &mut work)
        .unwrap();
    let all = operations
        .bind_observed(&candidates, KeyRows::All, &mut work)
        .unwrap();
    let mut coverage = SelectionSet::try_new(2).unwrap();
    coverage.insert(0).unwrap();
    let only_uncertain = operations
        .bind_observed(&candidates, KeyRows::Selected(&coverage), &mut work)
        .unwrap();
    assert_eq!(
        operations
            .lookup_observed(&all, &probes, 0, &mut growth, &mut work)
            .unwrap(),
        KeyLookup::Found(1)
    );
    assert_eq!(
        operations
            .lookup_observed(&only_uncertain, &probes, 0, &mut growth, &mut work)
            .unwrap(),
        KeyLookup::Indeterminate
    );
    assert_eq!(
        operations
            .lookup_observed(&only_uncertain, &probes, 1, &mut growth, &mut work)
            .unwrap(),
        KeyLookup::Missing
    );
    assert_eq!(
        operations
            .compare_pairs_observed(
                &all,
                &probes,
                &[(0, 0), (0, 1), (1, 0), (0, 0)],
                &mut growth,
                &mut work
            )
            .unwrap(),
        vec![
            KeyComparison::Unknown,
            KeyComparison::False,
            KeyComparison::True,
            KeyComparison::Unknown
        ]
    );
    assert!(!operations.determinate_observed(&all, 0, &mut work).unwrap());
    assert!(matches!(
        operations.hash_observed(&only_uncertain, 1, &mut work),
        Err(ValueFailure::Value(ValueError::Invariant(
            ValueViolation::UndefinedRow
        )))
    ));
}

#[test]
fn map_equal_reduces_false_before_key_uncertainty_and_value_unknown() {
    let compare_with = |n| {
        let left_keys = [[None, Some(1)], [Some(2), Some(0)], [Some(3), Some(0)]];
        let right_keys = [[Some(8), Some(1)], [Some(2), Some(0)], [Some(3), Some(0)]];
        let left_values = [Some(5), None, Some(7)];
        let right_values = [Some(5), Some(1), Some(8)];
        let left = map(
            lists(left_keys[..n].to_vec()),
            Arc::new(Int32Array::from(left_values[..n].to_vec())),
        );
        let right = map(
            lists(right_keys[..n].to_vec()),
            Arc::new(Int32Array::from(right_values[..n].to_vec())),
        );
        let forward = compare(&left, &right, KeyRelation::Equal);
        assert_eq!(forward, compare(&right, &left, KeyRelation::Equal));
        (left, right, forward)
    };
    assert_eq!(compare_with(2).2, KeyComparison::Indeterminate);
    assert_eq!(compare_with(3).2, KeyComparison::False);

    let same_keys = lists(vec![[Some(2), Some(0)]]);
    let unknown_left = map(same_keys.clone(), Arc::new(Int32Array::from(vec![None])));
    let unknown_right = map(same_keys, Arc::new(Int32Array::from(vec![Some(1)])));
    assert_eq!(
        compare(&unknown_left, &unknown_right, KeyRelation::Equal),
        KeyComparison::Unknown
    );

    let (left, right, _) = compare_with(1);
    let enclosing_row = |value: ArrayRef, other: i32| -> ArrayRef {
        let fields = Fields::from(vec![
            Field::new("m", value.data_type().clone(), true),
            Field::new("other", DataType::Int32, true),
        ]);
        Arc::new(StructArray::new(
            fields,
            vec![value, Arc::new(Int32Array::from(vec![other]))],
            None,
        ))
    };
    // The first field raises inner key uncertainty. A known mismatch in
    // another field still proves the outer row unequal.
    assert_eq!(
        compare(
            &enclosing_row(left.clone(), 1),
            &enclosing_row(right.clone(), 2),
            KeyRelation::Equal
        ),
        KeyComparison::False
    );
    assert_eq!(
        compare(
            &enclosing_row(left, 1),
            &enclosing_row(right, 1),
            KeyRelation::Equal
        ),
        KeyComparison::Indeterminate
    );
}

#[test]
fn map_equal_checks_that_a_key_is_determinately_missing_in_either_direction() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let left = map(
        lists(vec![[None, Some(1)], [Some(3), None]]),
        values.clone(),
    );
    let right = map(lists(vec![[Some(3), Some(1)], [Some(4), Some(9)]]), values);
    // Both left keys have an UNKNOWN comparison with [3,1], while [4,9]
    // is false against BOTH left keys.
    assert_eq!(
        compare(&left, &right, KeyRelation::Equal),
        KeyComparison::False
    );
    assert_eq!(
        compare(&right, &left, KeyRelation::Equal),
        KeyComparison::False
    );
}

#[test]
fn recursive_map_identity_is_an_equivalence_and_hashes_paired_entries() {
    let row = |order: [usize; 3], nan_bits, zero, swap_values| {
        let keys = [f64::from_bits(nan_bits), 1.0, 2.0];
        let values = if swap_values {
            [Some(zero), None, Some(9.0)]
        } else {
            [None, Some(zero), Some(9.0)]
        };
        let inner = map(
            Arc::new(Float64Array::from(order.map(|index| keys[index]).to_vec())),
            Arc::new(Float64Array::from(
                order.map(|index| values[index]).to_vec(),
            )),
        );
        map(Arc::new(Int32Array::from(vec![1])), inner)
    };
    let maps = [
        row([0, 1, 2], 0x7ff8_0000_0000_0001, -0.0, false),
        row([2, 0, 1], 0x7ff8_0000_0000_0011, 0.0, false),
        row([1, 2, 0], 0xfff8_0000_0000_0007, -0.0, false),
    ];
    let distinct = row([0, 1, 2], 0x7ff8_0000_0000_0001, -0.0, true);
    let op = prepared(&maps[0], KeyRelation::Identical);
    let values = maps
        .each_ref()
        .map(|a| op.bind_observed(a, KeyRows::All, &mut work).unwrap());
    let expected = op.hash_observed(&values[0], 0, &mut work).unwrap();
    for left in &values {
        assert_eq!(op.hash_observed(left, 0, &mut work).unwrap(), expected);
        for right in &values {
            assert_eq!(
                op.compare_observed(left, 0, right, 0, &mut growth, &mut work)
                    .unwrap(),
                KeyComparison::True
            );
        }
    }
    assert_eq!(
        compare(&maps[0], &distinct, KeyRelation::Identical),
        KeyComparison::False
    );
    assert_eq!(
        compare(&maps[0], &maps[0], KeyRelation::Equal),
        KeyComparison::False
    );
}

#[test]
fn forced_candidate_collision_verifies_the_actual_typed_key() {
    let maps = map(
        Arc::new(Int32Array::from(vec![7, 8])),
        Arc::new(Int32Array::from(vec![41, 42])),
    );
    let probes: ArrayRef = Arc::new(Int32Array::from(vec![8]));
    let map_op = prepared(&maps, KeyRelation::Identical);
    let probe_op = prepared(&probes, KeyRelation::Identical);
    let maps = map_op
        .bind_observed(&maps, KeyRows::All, &mut work)
        .unwrap();
    let probes = probe_op
        .bind_observed(&probes, KeyRows::All, &mut work)
        .unwrap();
    let origin = maps.position(0).unwrap();
    let Value::Map(map) = origin.read(&mut work).unwrap() else {
        panic!("expected map")
    };
    let probe = probes.position(0).unwrap();
    let same_hash = hash(probe, &mut work).unwrap();
    let mut index = MatchIndex {
        slots: [None; 4].into_iter().collect::<AllocatedVec<_>>(),
        determinate: true,
    };
    let first = index.slot(same_hash);
    let second = index.next(first);
    index.slots[first] = Some(Candidate {
        hash: same_hash,
        row: 0,
    });
    index.slots[second] = Some(Candidate {
        hash: same_hash,
        row: 1,
    });
    let mut compare = ComparisonWork {
        relation: KeyRelation::Identical,
        growth: &mut growth,
        work: &mut work,
        allocator: &Global,
    };
    assert_eq!(
        compare.lookup(probe, &map, &index).unwrap(),
        KeyLookup::Found(1)
    );
    index.slots[second] = None;
    assert_eq!(
        compare.lookup(probe, &map, &index).unwrap(),
        KeyLookup::Missing
    );
}

#[test]
fn carrier_offsets_dictionary_nulls_and_hidden_children_use_only_reachable_values() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(2), Some(1), Some(0)]),
            Arc::new(StringArray::from(vec![Some("unselected"), None, Some("a")])),
        )
        .unwrap(),
    );
    let view: ArrayRef = Arc::new(StringViewArray::from(vec![Some("a"), None]));
    let dictionary_ops = PreparedKeyOperations::prepare_observed(
        &FunctionValueType::new(DataType::Utf8, true),
        KeyRelation::Identical,
        &mut work,
    )
    .unwrap();
    let view_ops = prepared(&view, KeyRelation::Identical);
    let dictionary = dictionary_ops
        .bind_observed(&dictionary, KeyRows::Range(0..2), &mut work)
        .unwrap();
    let view = view_ops
        .bind_observed(&view, KeyRows::All, &mut work)
        .unwrap();
    for row in 0..2 {
        assert_eq!(
            dictionary_ops
                .compare_observed(&dictionary, row, &view, row, &mut growth, &mut work)
                .unwrap(),
            KeyComparison::True
        );
        assert_eq!(
            dictionary_ops
                .hash_observed(&dictionary, row, &mut work)
                .unwrap(),
            view_ops.hash_observed(&view, row, &mut work).unwrap(),
        );
    }

    let left = map_with_offsets(
        Arc::new(Int32Array::from(vec![11, 4])),
        Arc::new(Int32Array::from(vec![None, Some(9)])),
        [1, 1],
        Some(NullBuffer::from(vec![false, true])),
    );
    let right = map_with_offsets(
        Arc::new(Int32Array::from(vec![99, 4])),
        Arc::new(Int32Array::from(vec![Some(99), Some(9)])),
        [1, 1],
        Some(NullBuffer::from(vec![false, true])),
    );
    assert_eq!(
        compare(&left, &right, KeyRelation::Identical),
        KeyComparison::True
    );
    assert_eq!(
        compare(
            &left.slice(1, 1),
            &right.slice(1, 1),
            KeyRelation::Identical
        ),
        KeyComparison::True
    );
}

#[test]
fn char_and_nominal_domains_are_prepared_from_the_declaration() {
    let chars: ArrayRef = Arc::new(StringArray::from(vec!["a ", "a"]));
    let declaration = |kind| {
        let storage = ValueTypeStorageLifetime::pure();
        FunctionValueType::try_from_value(
            ValueTypeRef::try_new(
                ValueKind::BoundedCharacter(CharacterType::new(kind, 3).unwrap()),
                false,
                storage.clone(),
            )
            .unwrap(),
            ValueLayout::try_new(chars.data_type(), storage).unwrap(),
        )
        .unwrap()
    };
    for (kind, expected) in [
        (
            novarocks_type_contract::CharacterKind::Char,
            KeyComparison::True,
        ),
        (
            novarocks_type_contract::CharacterKind::Varchar,
            KeyComparison::False,
        ),
    ] {
        let op = PreparedKeyOperations::prepare_observed(
            &declaration(kind),
            KeyRelation::Identical,
            &mut work,
        )
        .unwrap();
        let chars = op.bind_observed(&chars, KeyRows::All, &mut work).unwrap();
        assert_eq!(
            op.compare_observed(&chars, 0, &chars, 1, &mut growth, &mut work)
                .unwrap(),
            expected
        );
        if expected == KeyComparison::True {
            assert_eq!(
                op.hash_observed(&chars, 0, &mut work).unwrap(),
                op.hash_observed(&chars, 1, &mut work).unwrap()
            );
        }
    }
    let bytes: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter([[0u8; 16]].into_iter()).unwrap());
    let plain = prepared(&bytes, KeyRelation::Identical);
    let uuid = PreparedKeyOperations::prepare_observed(
        &FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
        KeyRelation::Identical,
        &mut work,
    )
    .unwrap();
    let physical = plain
        .bind_observed(&bytes, KeyRows::All, &mut work)
        .unwrap();
    let nominal = uuid.bind_observed(&bytes, KeyRows::All, &mut work).unwrap();
    assert!(matches!(
        plain.compare_observed(&physical, 0, &nominal, 0, &mut growth, &mut work),
        Err(ValueFailure::Value(ValueError::Invariant(
            ValueViolation::Layout
        )))
    ));
}

#[test]
fn precise_temporal_identity_uses_the_instant_and_the_full_fraction() {
    use novarocks_type_contract::{
        TemporalCarrier, TemporalMicros, TemporalPrecision, TemporalType, TemporalValue,
        TimeZoneKey,
    };
    let temporal = TemporalType::TimestampWithTimeZone(TemporalPrecision::PICOSECONDS);
    let encoded = |fraction, zone| {
        let value = TemporalValue::TimestampWithTimeZone {
            instant: TemporalMicros::new(-1, fraction).unwrap(),
            zone,
        };
        let TemporalCarrier::ZonedTimestamp(bytes) = value.to_carrier(temporal).unwrap() else {
            panic!("expected zoned timestamp");
        };
        bytes
    };
    let values: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_iter(
            [
                encoded(7, TimeZoneKey::UTC),
                encoded(7, TimeZoneKey::from_offset_minutes(600).unwrap()),
                encoded(8, TimeZoneKey::UTC),
            ]
            .into_iter(),
        )
        .unwrap(),
    );
    let storage = ValueTypeStorageLifetime::pure();
    let declaration = FunctionValueType::try_from_value(
        ValueTypeRef::try_new(ValueKind::Temporal(temporal), false, storage.clone()).unwrap(),
        ValueLayout::try_new(&temporal.carrier(), storage).unwrap(),
    )
    .unwrap();
    for relation in [KeyRelation::Equal, KeyRelation::Identical] {
        let op =
            PreparedKeyOperations::prepare_observed(&declaration, relation, &mut work).unwrap();
        let values = op.bind_observed(&values, KeyRows::All, &mut work).unwrap();
        assert_eq!(
            op.compare_observed(&values, 0, &values, 1, &mut growth, &mut work)
                .unwrap(),
            KeyComparison::True,
        );
        assert_eq!(
            op.hash_observed(&values, 0, &mut work).unwrap(),
            op.hash_observed(&values, 1, &mut work).unwrap()
        );
        assert_eq!(
            op.compare_observed(&values, 0, &values, 2, &mut growth, &mut work)
                .unwrap(),
            KeyComparison::False,
        );
    }
}

#[test]
fn map_match_scratch_and_wide_leaf_checks_preserve_original_failures() {
    let map = map(
        Arc::new(Int32Array::from(vec![4])),
        Arc::new(Int32Array::from(vec![1])),
    );
    let operations = prepared(&map, KeyRelation::Identical);
    let map = operations
        .bind_observed(&map, KeyRows::All, &mut work)
        .unwrap();
    assert!(matches!(
        operations.compare_observed(&map, 0, &map, 0, &mut |_| Err("growth"), &mut |_| Ok(())),
        Err(ValueFailure::Caller("growth"))
    ));
    let wide: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(BYTE_BLOCK * 3)]));
    let operations = prepared(&wide, KeyRelation::Equal);
    let wide = operations
        .bind_observed(&wide, KeyRows::All, &mut work)
        .unwrap();
    let mut blocks = 0;
    assert!(matches!(
        operations.compare_observed(&wide, 0, &wide, 0, &mut |_| Ok(()), &mut |len| {
            if len == BYTE_BLOCK {
                blocks += 1;
                if blocks == 2 {
                    return Err("cancel wide comparison");
                }
            }
            Ok(())
        }),
        Err(ValueFailure::Caller("cancel wide comparison"))
    ));
    assert_eq!(blocks, 2);
}
