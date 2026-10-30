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

//! Real Arrow multi-source constructor/copy evidence, including complete backing.
use super::*;
use arrow_array::types::Int8Type;
use arrow_array::{DictionaryArray, Int8Array, Int16Array, StringArray, StringViewArray};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use arrow_schema::Field;
use std::sync::Arc;
fn segment(source: usize, start: usize, len: usize) -> ExtendSegment {
    ExtendSegment {
        source,
        start,
        len,
        repeats: 1,
        nulls: 0,
    }
}
fn observed_copy(sources: &[ArrayRef], segments: &[ExtendSegment], capacity: usize) -> ArrayRef {
    let refs = sources.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
    preflight_extend_multi(&refs, segments, capacity, |_| Ok(())).unwrap();
    let data = sources.iter().map(|a| a.to_data()).collect::<Vec<_>>();
    let mut mutable = MutableArrayData::new(data.iter().collect(), true, capacity);
    for segment in segments {
        for _ in 0..segment.repeats {
            if segment.len != 0 {
                mutable
                    .try_extend(segment.source, segment.start, segment.start + segment.len)
                    .unwrap();
            }
            if segment.nulls != 0 {
                mutable.try_extend_nulls(segment.nulls).unwrap();
            }
        }
    }
    make_array(mutable.freeze())
}
fn dictionary(values: ArrayRef, keys: Vec<i8>) -> ArrayRef {
    Arc::new(DictionaryArray::<Int8Type>::try_new(Int8Array::from(keys), values).unwrap())
}
fn list(values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> ArrayRef {
    Arc::new(GenericListArray::<i32>::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(offsets.into()),
        values,
        valid.map(NullBuffer::from),
    ))
}
#[test]
fn multi_extend_actual_byte_offsets_include_null_payload_and_ordered_padding() {
    let a: ArrayRef = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0, 1, 4, 6].into()),
        Buffer::from("aBADé".as_bytes()),
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let b: ArrayRef = Arc::new(StringArray::from(vec![Some("long"), None]));
    let plan = [
        ExtendSegment {
            nulls: 1,
            ..segment(0, 1, 2)
        },
        segment(1, 0, 2),
    ];
    let out = observed_copy(&[a, b], &plan, 0);
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out.value_offsets(), &[0, 3, 5, 5, 9, 9]);
    assert_eq!(out.value_data(), "BADélong".as_bytes());
    assert_eq!(
        out.iter().collect::<Vec<_>>(),
        vec![None, Some("é"), None, Some("long"), None]
    );
}
#[test]
fn multi_extend_actual_nested_offsets_and_struct_field_metadata_match_arrow_concat() {
    let field = Arc::new(
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )
        .with_metadata(std::collections::HashMap::<String, String>::from([(
            "origin".to_string(),
            "frozen-field".to_string(),
        )])),
    );
    let fields: arrow_schema::Fields = vec![field.clone()].into();
    let a = list(
        Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("hidden"),
            Some("b"),
        ])),
        vec![0, 2, 3, 4],
        Some(vec![true, false, true]),
    );
    let b = list(
        Arc::new(StringArray::from(vec![Some("long"), Some("é")])),
        vec![0, 0, 2],
        None,
    );
    let a: ArrayRef = Arc::new(StructArray::new(fields.clone(), vec![a], None));
    let b: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![b],
        Some(NullBuffer::from(vec![true, false])),
    ));
    let out = observed_copy(
        &[a.clone(), b.clone()],
        &[segment(0, 0, 3), segment(1, 0, 2)],
        0,
    );
    let expected = arrow_select::concat::concat(&[a.as_ref(), b.as_ref()]).unwrap();
    assert_eq!(out.to_data(), expected.to_data());
    let out = out.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(&out.fields()[0], &field);
    let child = out
        .column(0)
        .as_any()
        .downcast_ref::<GenericListArray<i32>>()
        .unwrap();
    assert_eq!(child.value_offsets(), &[0, 2, 3, 4, 4, 6]);
    assert!(out.is_null(4));
    assert!(child.is_null(1));
}
#[test]
fn multi_extend_dictionary_constructor_tracks_actual_ptr_eq_and_full_unused_domains() {
    let shared: ArrayRef = Arc::new(StringArray::from(vec!["a", "unused-a", "unused-b"]));
    let a = dictionary(shared.clone(), vec![0]);
    let same = dictionary(shared, vec![1]);
    let other = dictionary(Arc::new(StringArray::from(vec!["z", "unused-z"])), vec![0]);
    let same_out = observed_copy(&[a.clone(), same], &[segment(0, 0, 1), segment(1, 0, 1)], 0);
    let same_out = same_out
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(same_out.values().len(), 3);
    assert_eq!(same_out.keys().values().as_ref(), &[0, 1]);
    for plan in [vec![], vec![segment(0, 0, 1), segment(1, 0, 1)]] {
        let out = observed_copy(&[a.clone(), other.clone()], &plan, 0);
        let out = out
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        assert_eq!(out.values().len(), 5); // constructor copies complete domains even for no rows
        assert_eq!(out.keys().len(), plan.len());
        if !plan.is_empty() {
            assert_eq!(out.keys().values().as_ref(), &[0, 3]);
        }
        assert_eq!(
            out.values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![
                Some("a"),
                Some("unused-a"),
                Some("unused-b"),
                Some("z"),
                Some("unused-z")
            ]
        );
    }
    let first = dictionary(Arc::new(StringArray::from(vec!["first"; 64])), vec![0]);
    let second = dictionary(Arc::new(StringArray::from(vec!["second"; 64])), vec![0]);
    assert!(matches!(
        preflight_extend_multi(&[first.as_ref(), second.as_ref()], &[], 0, |_| Ok(())),
        Err(CopyError::Extent(_))
    ));
    let data = [first.to_data(), second.to_data()];
    // The existing first-party extent admission above stays unchanged. The
    // SDK now accepts all 128 i8 dictionary values (last index 127), so this
    // historical boundary is no longer an actual SDK constructor error.
    let constructed = MutableArrayData::try_new(data.iter().collect(), false, 0)
        .unwrap()
        .freeze();
    assert_eq!(constructed.len(), 0);
    assert_eq!(constructed.child_data()[0].len(), 128);

    // Keep an actual SDK failure oracle at the first unrepresentable index.
    let overflow = dictionary(Arc::new(StringArray::from(vec!["second"; 65])), vec![0]);
    assert!(matches!(
        preflight_extend_multi(&[first.as_ref(), overflow.as_ref()], &[], 0, |_| Ok(())),
        Err(CopyError::Extent(_))
    ));
    let data = [first.to_data(), overflow.to_data()];
    assert!(matches!(
        MutableArrayData::try_new(data.iter().collect(), false, 0),
        Err(arrow_schema::ArrowError::DictionaryKeyOverflowError)
    ));
}
#[test]
fn multi_extend_view_constructor_remaps_actual_source_variadic_buffers() {
    let a: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("source-one-long-view"),
        None,
    ]));
    let b: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("source-two-long-view"),
        Some("short"),
    ]));
    let out = observed_copy(&[a, b], &[segment(1, 0, 2), segment(0, 0, 2)], 0);
    assert_eq!(
        out.as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some("source-two-long-view"),
            Some("short"),
            Some("source-one-long-view"),
            None
        ]
    );
    assert_eq!(out.to_data().buffers().len(), 3); // views plus both original data buffers
}
#[test]
fn multi_extend_run_end_keeps_original_segment_runs_and_one_null_value_per_padding_call() {
    let a: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![2, 4]),
            &StringArray::from(vec!["a", "b"]),
        )
        .unwrap(),
    );
    let b: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![1, 3]),
            &StringArray::from(vec!["b", "d"]),
        )
        .unwrap(),
    );
    let out = observed_copy(
        &[a, b],
        &[
            segment(0, 1, 2),
            ExtendSegment {
                nulls: 1,
                ..segment(1, 0, 2)
            },
        ],
        0,
    );
    let out = out.as_any().downcast_ref::<RunArray<Int16Type>>().unwrap();
    assert_eq!(out.run_ends().values(), &[1, 2, 3, 4, 5]);
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("a"), Some("b"), Some("b"), Some("d"), None]
    ); // no cross-segment coalescing
}
#[test]
fn multi_extend_combined_recursive_payload_refuses_before_large_output_allocation() {
    let a: ArrayRef = Arc::new(StringArray::from(vec!["aaaa"]));
    let b: ArrayRef = Arc::new(StringArray::from(vec!["bbbb"]));
    let repeats = i32::MAX as usize / 8 + 1;
    let one = ExtendSegment {
        repeats,
        ..segment(0, 0, 1)
    };
    preflight_extend_multi(&[a.as_ref()], &[one], 0, |_| Ok(())).unwrap();
    let both = [one, ExtendSegment { source: 1, ..one }];
    assert!(matches!(
        preflight_extend_multi(&[a.as_ref(), b.as_ref()], &both, 0, |_| Ok(())),
        Err(CopyError::Extent(_))
    ));
    let a = list(a, vec![0, 1], None);
    let b = list(b, vec![0, 1], None);
    assert!(matches!(
        preflight_extend_multi(&[a.as_ref(), b.as_ref()], &both, 0, |_| Ok(())),
        Err(CopyError::Extent(_))
    ));
    assert!(matches!(
        preflight_extend_multi(
            &[a.as_ref()],
            &[ExtendSegment {
                nulls: usize::MAX,
                ..segment(0, 0, 1)
            }],
            0,
            |_| Ok(())
        ),
        Err(CopyError::Extent(_))
    ));
}
#[test]
fn multi_extend_invalid_sources_ranges_fields_and_all_actual_callback_causes_keep_prefix() {
    let a = list(
        Arc::new(StringArray::from(vec!["a", "b"])),
        vec![0, 1, 2],
        None,
    );
    let b = list(
        Arc::new(StringArray::from(vec![Some("long"), None])),
        vec![0, 2],
        None,
    );
    let wrong: ArrayRef = Arc::new(Int16Array::from(vec![1]));
    let shared: ArrayRef = Arc::new(StringArray::from(vec!["a", "unused"]));
    let dictionary_a = dictionary(shared, vec![0]);
    let dictionary_b = dictionary(Arc::new(StringArray::from(vec!["z"])), vec![0]);
    for (sources, plan, success) in [
        (
            vec![a.clone(), b.clone()],
            vec![segment(0, 0, 2), segment(1, 0, 1)],
            true,
        ),
        (vec![dictionary_a, dictionary_b], vec![], true),
        (vec![a.clone()], vec![segment(1, 0, 1)], false),
        (vec![a.clone()], vec![segment(0, 2, 1)], false),
        (vec![a, wrong], vec![], false),
    ] {
        let refs = sources.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
        let mut trace = vec![];
        assert_eq!(
            preflight_extend_multi(&refs, &plan, 0, |boundary| {
                trace.push(boundary);
                Ok(())
            })
            .is_ok(),
            success
        );
        assert_eq!(trace.first(), Some(&true));
        assert_eq!(trace.last(), Some(&true));
        for at in 0..trace.len() {
            for cause in super::broadcast_tests::failures() {
                let mut actual = vec![];
                let result = preflight_extend_multi(&refs, &plan, 0, |boundary| {
                    actual.push(boundary);
                    if actual.len() - 1 == at {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                });
                assert!(matches!(result,Err(CopyError::Control(ref value)) if value==&cause));
                assert_eq!(actual, trace[..=at]);
            }
        }
    }
}
#[test]
fn multi_extend_real_map_fixed_list_and_dense_sparse_union_match_original_concat() {
    fn map(keys: Vec<i16>, text: Vec<&str>, offsets: Vec<i32>) -> ArrayRef {
        let fields: arrow_schema::Fields = vec![
            Arc::new(Field::new("keys", DataType::Int16, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        ]
        .into();
        let entries = StructArray::new(
            fields,
            vec![
                Arc::new(Int16Array::from(keys)),
                Arc::new(StringArray::from(text)),
            ],
            None,
        );
        Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(offsets.into()),
            entries,
            None,
            false,
        ))
    }
    let a = map(vec![1, 2], vec!["a", "b"], vec![0, 1, 2]);
    let b = map(vec![3, 4], vec!["c", "d"], vec![0, 0, 2]);
    let field = Arc::new(Field::new("item", DataType::Int16, true));
    let fixed_a: ArrayRef = Arc::new(FixedSizeListArray::new(
        field.clone(),
        2,
        Arc::new(Int16Array::from(vec![1, 2, 3, 4])),
        Some(NullBuffer::from(vec![true, false])),
    ));
    let fixed_b: ArrayRef = Arc::new(FixedSizeListArray::new(
        field,
        2,
        Arc::new(Int16Array::from(vec![5, 6])),
        None,
    ));
    for (a, b) in [(a, b), (fixed_a, fixed_b)] {
        let out = observed_copy(
            &[a.clone(), b.clone()],
            &[segment(0, 0, a.len()), segment(1, 0, b.len())],
            0,
        );
        assert_eq!(
            out.to_data(),
            arrow_select::concat::concat(&[a.as_ref(), b.as_ref()])
                .unwrap()
                .to_data()
        );
    }
    for sparse in [false, true] {
        let fields = arrow_schema::UnionFields::try_new(
            [1, 7],
            [
                Arc::new(Field::new("number", DataType::Int16, true)),
                Arc::new(Field::new("text", DataType::Utf8, true)),
            ],
        )
        .unwrap();
        let mut sources = vec![];
        for reverse in [false, true] {
            let ids = if reverse {
                vec![7_i8, 1]
            } else {
                vec![1_i8, 7]
            };
            let (numbers, text): (ArrayRef, ArrayRef) = if sparse {
                (
                    Arc::new(Int16Array::from(vec![Some(1), None])),
                    Arc::new(StringArray::from(vec![None, Some("x")])),
                )
            } else {
                (
                    Arc::new(Int16Array::from(vec![Some(1)])),
                    Arc::new(StringArray::from(vec![Some("x")])),
                )
            };
            let array: ArrayRef = Arc::new(
                UnionArray::try_new(
                    fields.clone(),
                    ids.into(),
                    (!sparse).then(|| vec![0_i32, 0].into()),
                    vec![numbers, text],
                )
                .unwrap(),
            );
            sources.push(array);
        }
        let out = observed_copy(&sources, &[segment(1, 0, 2), segment(0, 0, 2)], 0);
        assert_eq!(
            out.to_data(),
            arrow_select::concat::concat(&[sources[1].as_ref(), sources[0].as_ref()])
                .unwrap()
                .to_data()
        );
        preflight_extend_multi(
            &sources.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
            &[ExtendSegment {
                nulls: 1,
                ..segment(0, 0, 1)
            }],
            0,
            |_| Ok(()),
        )
        .unwrap();
    }
}
#[test]
fn multi_extend_frozen_field_metadata_refusal_and_actual_owned_shape_work_over_256() {
    let a: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(
            Field::new("item", DataType::Int16, false).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([(
                "identity".to_string(),
                "first".to_string(),
            )])),
        )]
        .into(),
        vec![Arc::new(Int16Array::from(vec![1]))],
        None,
    ));
    let b: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(
            Field::new("item", DataType::Int16, false).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([(
                "identity".to_string(),
                "second".to_string(),
            )])),
        )]
        .into(),
        vec![Arc::new(Int16Array::from(vec![2]))],
        None,
    ));
    assert!(matches!(
        preflight_extend_multi(&[a.as_ref(), b.as_ref()], &[], 0, |_| Ok(())),
        Err(CopyError::Invalid(
            "mutable copy sources differ from their complete carrier"
        ))
    ));
    let fields = (0..320)
        .map(|i| Arc::new(Field::new(format!("field-{i}"), DataType::Int16, false)))
        .collect::<Vec<_>>();
    let values = (0..320)
        .map(|i| Arc::new(Int16Array::from(vec![i as i16])) as ArrayRef)
        .collect();
    let a: ArrayRef = Arc::new(StructArray::new(fields.into(), values, None));
    let mut trace = vec![];
    preflight_extend_multi(
        &[a.as_ref(), a.as_ref()],
        &[segment(0, 0, 1), segment(1, 0, 1)],
        0,
        |boundary| {
            trace.push(boundary);
            Ok(())
        },
    )
    .unwrap();
    assert!(trace.iter().filter(|boundary| !**boundary).count() > 256);
    let out = observed_copy(&[a.clone(), a], &[segment(0, 0, 1), segment(1, 0, 1)], 0);
    assert_eq!(out.len(), 2);
    assert_eq!(
        out.as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .num_columns(),
        320
    );
}

#[test]
fn multi_extend_union_padding_selects_first_nullable_field_and_sliced_sparse_rows() {
    for dense in [false, true] {
        let fields = arrow_schema::UnionFields::try_new(
            [5, 99],
            [
                Field::new("required", DataType::Int64, false),
                Field::new("nullable", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let source: ArrayRef = Arc::new(
            UnionArray::try_new(
                fields,
                vec![5_i8, 99, 5].into(),
                dense.then(|| vec![0_i32, 0, 1].into()),
                if dense {
                    vec![
                        Arc::new(arrow_array::Int64Array::from(vec![11, 22])),
                        Arc::new(StringArray::from(vec!["selected"])),
                    ]
                } else {
                    vec![
                        Arc::new(arrow_array::Int64Array::from(vec![11, 0, 22])),
                        Arc::new(StringArray::from(vec!["hidden", "selected", "unused"])),
                    ]
                },
            )
            .unwrap(),
        );
        let source = source.slice(1, 2);
        let output = observed_copy(
            &[source],
            &[ExtendSegment {
                nulls: 2,
                ..segment(0, 0, 2)
            }],
            0,
        );
        let union = output.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(union.type_ids().as_ref(), &[99_i8, 5, 99, 99]);
        let strings = union
            .child(99)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(strings.value(union.value_offset(0)), "selected");
        assert!(strings.is_null(union.value_offset(2)));
        assert!(strings.is_null(union.value_offset(3)));
        let ints = union
            .child(5)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(ints.value(union.value_offset(1)), 22);
    }
}
