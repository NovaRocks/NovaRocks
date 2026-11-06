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

//! Functional owner tests for content identity. IEEE bit patterns and explicit
//! class tables are the independent oracle; no cluster or external input is used.

use super::*;
use arrow::datatypes::Field;
use arrow_buffer::{NullBuffer, OffsetBuffer};

const FIRST_NAN: u64 = 0x7ff8_0000_0000_0042;
const SECOND_NAN: u64 = 0xfff8_0000_0000_0917;

fn floats() -> Vec<Option<f64>> {
    vec![
        Some(-0.0),
        Some(0.0),
        Some(f64::from_bits(FIRST_NAN)),
        Some(f64::from_bits(SECOND_NAN)),
        None,
        None,
    ]
}
fn nested(kind: usize) -> ArrayRef {
    let values = floats();
    match kind {
        0 => Arc::new(Float64Array::from(values)),
        1 => {
            let mut builder = ListBuilder::new(Float64Builder::new());
            for (row, value) in values.into_iter().enumerate() {
                builder
                    .values()
                    .append_value(value.unwrap_or(123.0 + row as f64));
                builder.append(value.is_some());
            }
            Arc::new(builder.finish())
        }
        2 => Arc::new(
            StructArray::try_new(
                vec![Field::new("v", DataType::Float64, false)].into(),
                vec![Arc::new(Float64Array::from(vec![
                    -0.0,
                    0.0,
                    f64::from_bits(FIRST_NAN),
                    f64::from_bits(SECOND_NAN),
                    123.0,
                    -456.0,
                ]))],
                Some(NullBuffer::from(vec![true, true, true, true, false, false])),
            )
            .unwrap(),
        ),
        3 => {
            let entries = StructArray::try_new(
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new("value", DataType::Float64, false),
                ]
                .into(),
                vec![
                    Arc::new(Int32Array::from(vec![1; 6])),
                    Arc::new(Float64Array::from(vec![
                        -0.0,
                        0.0,
                        f64::from_bits(FIRST_NAN),
                        f64::from_bits(SECOND_NAN),
                        123.0,
                        -456.0,
                    ])),
                ],
                None,
            )
            .unwrap();
            Arc::new(
                MapArray::try_new(
                    Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                    OffsetBuffer::new(vec![0, 1, 2, 3, 4, 5, 6].into()),
                    entries,
                    Some(NullBuffer::from(vec![true, true, true, true, false, false])),
                    false,
                )
                .unwrap(),
            )
        }
        _ => unreachable!(),
    }
}
fn float_bits(array: &ArrayRef, row: usize) -> Option<u64> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Float64 => Some(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row)
                .to_bits(),
        ),
        DataType::List(_) => float_bits(
            &array
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(row),
            0,
        ),
        DataType::Struct(_) => float_bits(
            array
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(0),
            row,
        ),
        DataType::Map(_, _) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            float_bits(map.entries().column(1), map.value_offsets()[row] as usize)
        }
        _ => unreachable!(),
    }
}
fn prepared<'a>(
    spec: &KeySpec,
    keys: &'a KeyInput,
    tracker: &Arc<MemTracker>,
    owner: &dyn InputConstructionOwner,
) -> PreparedKeyBatch<'a> {
    PreparedKeyBatch::new(
        spec,
        keys,
        AggregateAllocator::new(tracker.clone()),
        owner,
        &|| Ok(()),
    )
    .unwrap()
}

#[test]
fn content_and_grouping_classify_zero_nan_and_parent_null_through_storage_and_compaction() {
    for kind in 0..4 {
        for equality in [
            KeyEquality::SqlGrouping(NullPolicy::Equal),
            KeyEquality::ResultContent,
        ] {
            let (owner, tracker) = owner_with_tracker();
            let array = nested(kind);
            let ty = FunctionValueType::new(array.data_type().clone(), true);
            let spec = KeySpec::with_equalities(
                std::slice::from_ref(&ty),
                &[equality],
                owner.as_ref(),
                &|| Ok(()),
            )
            .unwrap();
            let keys = input(
                &[array],
                std::slice::from_ref(&ty),
                &[0, 1, 2, 3, 4, 5],
                owner.as_ref(),
            );
            let mut index = KeyIndex::new(
                spec.clone(),
                IndexPurpose::KeyIds,
                tracker.clone(),
                owner.as_ref(),
            )
            .unwrap();
            let normal = hash(&keys, &spec, index.policy(), owner.as_ref());
            let expected = if equality == KeyEquality::ResultContent {
                [0, 1, 2, 2, 3, 3]
            } else {
                [0, 0, 1, 1, 2, 2]
            };
            for a in 0..6 {
                for b in 0..6 {
                    if expected[a] == expected[b] {
                        assert_eq!(normal.hash(a).unwrap(), normal.hash(b).unwrap());
                    }
                }
            }
            // A deliberate local collision proves the exact comparison and stored
            // representative paths rather than merely trusting different hashes.
            let hashes = normal.with_hashes_for_test(&[0; 6]);
            let values = prepared(&spec, &keys, &tracker, owner.as_ref());
            let found = index
                .intern_batch(&values, &hashes, owner.as_ref(), &CopyControl)
                .unwrap();
            assert_eq!(
                found.matches(),
                expected.map(|id| KeyMatch::Found(KeyId(id))),
                "kind={kind}, policy={equality:?}"
            );
            let mut compared = [0; 6];
            super::super::pairs::compare_prepared_pairs(
                &values,
                &[0, 0, 2, 2, 4, 4],
                &values,
                &[0, 1, 2, 3, 4, 5],
                &mut compared,
                owner.as_ref(),
                &|| Ok(()),
            )
            .unwrap();
            assert_eq!(
                compared,
                [
                    1,
                    u8::from(equality != KeyEquality::ResultContent),
                    1,
                    1,
                    1,
                    1
                ]
            );
            index.compact(owner.as_ref(), &CopyControl).unwrap();
            assert_eq!(
                index
                    .probe_batch(&values, &hashes, owner.as_ref(), &CopyControl)
                    .unwrap()
                    .matches(),
                found.matches()
            );
            if equality == KeyEquality::ResultContent {
                let exported = index
                    .export_keys(
                        &[KeyId(0), KeyId(1), KeyId(2), KeyId(3)],
                        std::slice::from_ref(&ty),
                        owner.as_ref(),
                        &CopyControl,
                    )
                    .unwrap();
                assert_eq!(
                    (0..4)
                        .map(|row| float_bits(&exported.arrays()[0], row))
                        .collect::<Vec<_>>(),
                    vec![Some((-0.0f64).to_bits()), Some(0), Some(FIRST_NAN), None]
                );
            }
        }
    }
}

#[test]
fn float32_content_policy_keeps_zero_sign_and_merges_all_nan_payloads() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Float32, false);
    let array: ArrayRef = Arc::new(Float32Array::from(vec![
        -0.0,
        0.0,
        f32::from_bits(0x7fc0_0011),
        f32::from_bits(0xffc0_0022),
    ]));
    let spec = KeySpec::with_equalities(
        std::slice::from_ref(&ty),
        &[KeyEquality::ResultContent],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let keys = input(&[array], &[ty], &[0, 1, 2, 3], owner.as_ref());
    let mut index = KeyIndex::new(
        spec.clone(),
        IndexPurpose::KeyIds,
        tracker.clone(),
        owner.as_ref(),
    )
    .unwrap();
    let hashes = hash(&keys, &spec, index.policy(), owner.as_ref());
    let values = prepared(&spec, &keys, &tracker, owner.as_ref());
    assert_eq!(
        index
            .intern_batch(&values, &hashes, owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        [
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(1)),
            KeyMatch::Found(KeyId(2)),
            KeyMatch::Found(KeyId(2))
        ]
    );
}

#[test]
fn per_column_policy_is_frozen_and_content_root_null_cannot_be_unmatchable() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Float64, true);
    let spec = KeySpec::with_equalities(
        &[ty.clone(), ty.clone()],
        &[
            KeyEquality::SqlGrouping(NullPolicy::Unmatchable),
            KeyEquality::ResultContent,
        ],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(spec.nulls(), &[NullPolicy::Unmatchable, NullPolicy::Equal]);
    let keys = input(
        &[
            Arc::new(Float64Array::from(vec![Some(-0.0), Some(0.0), None])),
            Arc::new(Float64Array::from(vec![Some(-0.0), Some(0.0), None])),
        ],
        &[ty.clone(), ty],
        &[0, 1, 2],
        owner.as_ref(),
    );
    let mut index = KeyIndex::new(
        spec.clone(),
        IndexPurpose::KeyIds,
        tracker.clone(),
        owner.as_ref(),
    )
    .unwrap();
    let hashes = hash(&keys, &spec, index.policy(), owner.as_ref());
    let values = prepared(&spec, &keys, &tracker, owner.as_ref());
    let found = index
        .intern_batch(&values, &hashes, owner.as_ref(), &CopyControl)
        .unwrap();
    assert_ne!(found.matches()[0], found.matches()[1]);
    assert!(!hashes.participates(2).unwrap());
}

fn fingerprints(arrays: &[ArrayRef], types: &[FunctionValueType], rows: &[usize]) -> Vec<u128> {
    let (owner, tracker) = owner_with_tracker();
    let spec = KeySpec::with_equalities(
        types,
        &vec![KeyEquality::ResultContent; types.len()],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let keys = input(arrays, types, rows, owner.as_ref());
    let values = prepared(&spec, &keys, &tracker, owner.as_ref());
    (0..rows.len())
        .map(|row| values.content_fingerprint(row, &|| Ok(())).unwrap())
        .collect()
}

#[test]
fn fingerprint_fixed_vectors_distinguish_zero_sign_and_ignore_nan_payload() {
    // Independent libxxhash 0.8.3 C API over the explicit nullable frame,
    // fixed f64 marker and little-endian bits; these are frozen constants.
    let array: ArrayRef = Arc::new(Float64Array::from(floats()));
    assert_eq!(
        fingerprints(
            &[array],
            &[FunctionValueType::new(DataType::Float64, true)],
            &[0, 1, 2, 3, 4, 5]
        ),
        vec![
            0xdecd4bf6e6a2ee2aa6f1798ead3fc373,
            0xb35511a088413e4c13d9f94ee696c382,
            0x3c36dafb333cfffa806a3c0a0a3bcb18,
            0x3c36dafb333cfffa806a3c0a0a3bcb18,
            0xffee988d572cc7360d9827fe527dc164,
            0xffee988d572cc7360d9827fe527dc164,
        ]
    );
}

#[test]
fn content_bytes_are_carrier_independent_and_json_stays_original_text() {
    let expected = vec![
        0x72d1c5e413ad10642e9b4bd70271c2c9,
        0xffee988d572cc7360d9827fe527dc164,
    ];
    let carriers: Vec<(ArrayRef, DataType)> = vec![
        (
            Arc::new(StringArray::from(vec![Some("abc"), None])),
            DataType::Utf8,
        ),
        (
            Arc::new(LargeStringArray::from(vec![Some("abc"), None])),
            DataType::LargeUtf8,
        ),
        (
            Arc::new(StringViewArray::from(vec![Some("abc"), None])),
            DataType::Utf8View,
        ),
        (
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![Some(1), Some(0), Some(1)]),
                    Arc::new(StringArray::from(vec![None, Some("abc")])),
                )
                .unwrap(),
            )
            .slice(0, 2),
            DataType::Utf8,
        ),
    ];
    for (array, data_type) in carriers {
        let ty = FunctionValueType::new(data_type, true);
        assert_eq!(
            fingerprints(
                std::slice::from_ref(&array),
                std::slice::from_ref(&ty),
                &[0, 1]
            ),
            expected
        );
        let (owner, tracker) = owner_with_tracker();
        let spec = KeySpec::with_equalities(
            &[ty.clone()],
            &[KeyEquality::ResultContent],
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let keys = input(&[array], &[ty], &[0, 1, 0], owner.as_ref());
        let mut index = KeyIndex::new(
            spec.clone(),
            IndexPurpose::KeyIds,
            tracker.clone(),
            owner.as_ref(),
        )
        .unwrap();
        let hashes = hash(&keys, &spec, index.policy(), owner.as_ref());
        let values = prepared(&spec, &keys, &tracker, owner.as_ref());
        assert_eq!(
            index
                .intern_batch(&values, &hashes, owner.as_ref(), &CopyControl)
                .unwrap()
                .matches(),
            [
                KeyMatch::Found(KeyId(0)),
                KeyMatch::Found(KeyId(1)),
                KeyMatch::Found(KeyId(0))
            ]
        );
    }
    let json = FunctionValueType::try_with_logical_type(
        DataType::Utf8,
        false,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    let array: ArrayRef = Arc::new(StringArray::from(vec!["{\"a\":1}", "{\"a\": 1}"]));
    let ids = fingerprints(&[array], &[json], &[0, 1]);
    assert_ne!(ids[0], ids[1]);
}

#[test]
fn content_identity_survives_bulk_capture_partition_build_read_and_export() {
    use std::num::NonZeroUsize;
    for kind in [0, 1, 2, 3] {
        let (owner, tracker) = owner_with_tracker();
        let array = nested(kind);
        let ty = FunctionValueType::new(array.data_type().clone(), true);
        let spec = KeySpec::with_equalities(
            std::slice::from_ref(&ty),
            &[KeyEquality::ResultContent],
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let domain =
            super::super::capture::BulkDomain::new(&spec, owner.as_ref(), &CopyControl).unwrap();
        let host =
            crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone());
        let work = super::super::capture::CaptureWork {
            owner: owner.as_ref(),
            allocator: AggregateAllocator::new(tracker.clone()),
            host: Arc::clone(&host),
            control: &CopyControl,
        };
        let mut runs = Vec::new();
        for _ in 0..2 {
            let mut capture = domain.begin_capture(&work).unwrap();
            capture
                .push_evaluated(
                    super::super::capture::EvaluatedPage {
                        arrays: std::slice::from_ref(&array),
                        bindings: std::slice::from_ref(&ty),
                        readable: &readable(6),
                        occurrences: &[0, 1, 2, 3, 4, 5],
                    },
                    &mut ValueConversionContext::default(),
                    &work,
                )
                .unwrap();
            runs.push(capture.seal(&work).unwrap());
        }
        let complete = CompleteInput::from_captures(
            super::super::capture::BulkInput::new(&domain, runs, &work).unwrap(),
            IndexPurpose::KeyIds,
            &work,
        )
        .unwrap();
        let ports = BulkWork {
            owner: owner.as_ref(),
            allocator: AggregateAllocator::new(tracker.clone()),
            host: Arc::clone(&host),
            control: &CopyControl,
        };
        let graph = BulkBuildGraph::begin(
            complete,
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            &ports,
        )
        .unwrap();
        let mut pending = Vec::new();
        loop {
            while let BulkClaim::Step(task) = graph.claim(&ports).unwrap() {
                pending.push(task);
            }
            if pending.is_empty() {
                break;
            }
            let mut next = Vec::new();
            for mut task in pending {
                if task.advance(1, &ports).unwrap().is_done() {
                    let mut finished = task.finish().unwrap();
                    graph.complete(&mut finished, &ports).unwrap();
                } else {
                    next.push(task);
                }
            }
            pending = next;
        }
        let ready = graph.take_ready(&ports).unwrap().unwrap();
        let keys = input(
            &[array],
            std::slice::from_ref(&ty),
            &[0, 1, 2, 3, 4, 5],
            owner.as_ref(),
        );
        let values = prepared(&spec, &keys, &tracker, owner.as_ref());
        let found = ready
            .probe(&values, None, owner.as_ref(), &CopyControl)
            .unwrap();
        let expected = [0, 1, 2, 2, 3, 3];
        for a in 0..6 {
            for b in 0..6 {
                assert_eq!(
                    found.matches()[a] == found.matches()[b],
                    expected[a] == expected[b],
                    "kind={kind}"
                );
            }
        }
        let ids = [0, 1, 2, 4].map(|row| match found.matches()[row] {
            BulkMatch::Found(id) => id,
            other => panic!("expected content class: {other:?}"),
        });
        let exported = ready
            .export_keys(
                &ids,
                std::slice::from_ref(&ty),
                owner.as_ref(),
                host,
                &CopyControl,
            )
            .unwrap();
        assert_eq!(
            (0..4)
                .map(|row| float_bits(&exported.arrays()[0], row))
                .collect::<Vec<_>>(),
            vec![Some((-0.0f64).to_bits()), Some(0), Some(FIRST_NAN), None]
        );
    }
}

#[test]
fn content_equal_values_keep_the_same_native_routing_hash() {
    for kind in 0..4 {
        let owner = owner();
        let array = nested(kind);
        let ty = FunctionValueType::new(array.data_type().clone(), true);
        let hashes = crate::exec::hash_table::partition_keys::native_tuple_hashes_observed(
            &[array],
            &[ty],
            &ReachableRows::all(6),
            &mut ValueConversionContext::default(),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        // Routing deliberately remains coarser: even opposite zero signs route
        // together; all content-equal NaNs and root NULLs must also do so.
        assert_eq!(hashes.as_slice()[0], hashes.as_slice()[1]);
        assert_eq!(hashes.as_slice()[2], hashes.as_slice()[3]);
        assert_eq!(hashes.as_slice()[4], hashes.as_slice()[5]);
    }
}

#[test]
fn sliced_dictionary_float_fingerprints_retain_occurrence_and_null_coordinates() {
    let source: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(2), Some(1), None, Some(0), Some(3), Some(1)]),
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(f64::from_bits(FIRST_NAN)),
                None,
            ])),
        )
        .unwrap(),
    );
    let sliced = source.slice(1, 4);
    assert_eq!(
        fingerprints(
            &[sliced],
            &[FunctionValueType::new(DataType::Float64, true)],
            &[2, 0, 1, 3, 0]
        ),
        vec![
            0xdecd4bf6e6a2ee2aa6f1798ead3fc373,
            0xb35511a088413e4c13d9f94ee696c382,
            0xffee988d572cc7360d9827fe527dc164,
            0xffee988d572cc7360d9827fe527dc164,
            0xb35511a088413e4c13d9f94ee696c382,
        ]
    );
}

#[test]
fn content_fingerprint_is_neutral_to_nested_byte_layout_and_parent_null_children() {
    let mut expected = None;
    for ty in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let child: ArrayRef = match ty {
            DataType::Utf8 => Arc::new(StringArray::from(vec!["abc", "hidden original"])),
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec!["abc", "hidden large"])),
            DataType::Utf8View => Arc::new(StringViewArray::from(vec!["abc", "hidden view"])),
            _ => unreachable!(),
        };
        let array: ArrayRef = Arc::new(
            StructArray::try_new(
                vec![Field::new("v", ty, false)].into(),
                vec![child],
                Some(NullBuffer::from(vec![true, false])),
            )
            .unwrap(),
        );
        let binding = FunctionValueType::new(array.data_type().clone(), true);
        let actual = fingerprints(&[array], &[binding], &[0, 1]);
        if let Some(expected) = &expected {
            assert_eq!(&actual, expected);
        } else {
            expected = Some(actual);
        }
    }
    let mut expected = None;
    for ty in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        let child: ArrayRef = match ty {
            DataType::Binary => Arc::new(BinaryArray::from(vec![b"abc".as_slice()])),
            DataType::LargeBinary => Arc::new(LargeBinaryArray::from(vec![b"abc".as_slice()])),
            DataType::BinaryView => Arc::new(BinaryViewArray::from(vec![b"abc".as_slice()])),
            _ => unreachable!(),
        };
        let array: ArrayRef = Arc::new(
            StructArray::try_new(vec![Field::new("v", ty, false)].into(), vec![child], None)
                .unwrap(),
        );
        let binding = FunctionValueType::new(array.data_type().clone(), false);
        let actual = fingerprints(&[array], &[binding], &[0]);
        if let Some(expected) = &expected {
            assert_eq!(&actual, expected);
        } else {
            expected = Some(actual);
        }
    }
}

#[test]
fn one_content_index_matches_original_and_dictionary_string_carriers() {
    let (owner, tracker) = owner_with_tracker();
    let expected = FunctionValueType::new(DataType::Utf8, true);
    let spec = KeySpec::with_equalities(
        std::slice::from_ref(&expected),
        &[KeyEquality::ResultContent],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut index = KeyIndex::new(
        spec.clone(),
        IndexPurpose::KeyIds,
        tracker.clone(),
        owner.as_ref(),
    )
    .unwrap();
    let carriers: Vec<(ArrayRef, DataType)> = vec![
        (
            Arc::new(StringArray::from(vec![Some("abc"), None, Some("abc")])),
            DataType::Utf8,
        ),
        (
            Arc::new(LargeStringArray::from(vec![Some("abc"), None, Some("abc")])),
            DataType::LargeUtf8,
        ),
        (
            Arc::new(StringViewArray::from(vec![Some("abc"), None, Some("abc")])),
            DataType::Utf8View,
        ),
        (
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![1, 0, 1]),
                    Arc::new(StringArray::from(vec![None, Some("abc")])),
                )
                .unwrap(),
            ),
            DataType::Utf8,
        ),
    ];
    let mut same_hashes = None;
    for (array, data_type) in carriers {
        let actual = FunctionValueType::new(data_type, true);
        let keys = input(&[array], &[actual], &[0, 1, 2], owner.as_ref());
        let hashes = hash(&keys, &spec, index.policy(), owner.as_ref());
        let actual_hashes = (0..3).map(|i| hashes.hash(i).unwrap()).collect::<Vec<_>>();
        if let Some(expected) = &same_hashes {
            assert_eq!(&actual_hashes, expected);
        } else {
            same_hashes = Some(actual_hashes);
        }
        let values = prepared(&spec, &keys, &tracker, owner.as_ref());
        assert_eq!(
            index
                .intern_batch(&values, &hashes, owner.as_ref(), &CopyControl)
                .unwrap()
                .matches(),
            [
                KeyMatch::Found(KeyId(0)),
                KeyMatch::Found(KeyId(1)),
                KeyMatch::Found(KeyId(0))
            ]
        );
        index.compact(owner.as_ref(), &CopyControl).unwrap();
    }
}

#[test]
fn content_json_classes_keep_original_text_in_storage_and_export() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::try_with_logical_type(
        DataType::Utf8,
        false,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    let array: ArrayRef = Arc::new(StringArray::from(vec!["{\"a\":1}", "{\"a\": 1}"]));
    let spec = KeySpec::with_equalities(
        std::slice::from_ref(&ty),
        &[KeyEquality::ResultContent],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let keys = input(&[array], std::slice::from_ref(&ty), &[0, 1], owner.as_ref());
    let mut index = KeyIndex::new(
        spec.clone(),
        IndexPurpose::KeyIds,
        tracker.clone(),
        owner.as_ref(),
    )
    .unwrap();
    let hashes = hash(&keys, &spec, index.policy(), owner.as_ref());
    let values = prepared(&spec, &keys, &tracker, owner.as_ref());
    assert_eq!(
        index
            .intern_batch(&values, &hashes, owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        [KeyMatch::Found(KeyId(0)), KeyMatch::Found(KeyId(1))]
    );
    let exported = index
        .export_keys(&[KeyId(1), KeyId(0)], &[ty], owner.as_ref(), &CopyControl)
        .unwrap();
    let actual = exported.arrays()[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(actual.value(0), "{\"a\": 1}");
    assert_eq!(actual.value(1), "{\"a\":1}");
}

#[test]
fn content_fingerprint_supports_the_existing_fixed_key_carriers() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(UInt8Array::from(vec![u8::MAX])),
        Arc::new(UInt16Array::from(vec![u16::MAX])),
        Arc::new(UInt32Array::from(vec![u32::MAX])),
        Arc::new(UInt64Array::from(vec![u64::MAX])),
        Arc::new(Date64Array::from(vec![123_i64])),
        Arc::new(Time32SecondArray::from(vec![123_i32])),
        Arc::new(Time32MillisecondArray::from(vec![123_i32])),
        Arc::new(Time64MicrosecondArray::from(vec![123_i64])),
        Arc::new(Time64NanosecondArray::from(vec![123_i64])),
        Arc::new(DurationSecondArray::from(vec![123_i64])),
        Arc::new(DurationMillisecondArray::from(vec![123_i64])),
        Arc::new(DurationMicrosecondArray::from(vec![123_i64])),
        Arc::new(DurationNanosecondArray::from(vec![123_i64])),
        Arc::new(FixedSizeBinaryArray::try_from_iter([b"abc".as_slice()].into_iter()).unwrap()),
    ];
    // Independent C libxxhash vectors over hand-framed fixed values.
    let expected = [
        0xf9a195999023af32b6b2e7349218a514,
        0xb68d9d7204757758df4eebc35661d1a4,
        0xb2eae47a8eabfff7a889648fc1f5268c,
        0x0400c9ca98bb6fd36834e8ef2fda8953,
        0x4873ae7e5b69c873228f00288d1ad478,
        0x2a8e7acedef0648e4e1fbeb00caa74f7,
        0xcea24a5a701b0512cb25d4ce4f14c5e2,
        0x5f47f5949f3ed65ab4778d661ac955b8,
        0x0adba7f835fd863c71b0bad7a289d347,
        0x5e2c38ef0b3164e221bf462cf1496cac,
        0x260b27c039725c2086014c11d9365e04,
        0x4f31fa36b96e2764c3c0427162838fe4,
        0xee4855c5f0058c8de2976a212e550f03,
        0xff6cf34cb90d2df5be57a09f2681641d,
    ];
    for (array, expected) in arrays.into_iter().zip(expected) {
        let ty = FunctionValueType::new(array.data_type().clone(), false);
        assert_eq!(fingerprints(&[array], &[ty], &[0]), vec![expected]);
    }
}
