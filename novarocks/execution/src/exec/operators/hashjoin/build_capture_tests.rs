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

//! Original evaluated arrays and K metadata ports, without a fake Ready.

use super::super::{
    build_pages::{
        BuildRows, CapturesForGraphs, ProjectionKind,
        tests::{collect, error, naa, ordinary, source, work},
    },
    join_completion::tests::{Control, fixture},
};
use super::*;
use crate::exec::hash_table::key_index::IndexPurpose;
use arrow::{
    array::{Array, DictionaryArray, Int32Array, StringArray},
    datatypes::{DataType, Int32Type},
};
use novarocks_functions::{KernelEvaluationControl, KernelFailure};

#[test]
fn naa_value_null_facts_exclude_scope_null_and_separate_ordinary_from_nullsafe_only() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let types = [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Utf8, true),
        FunctionValueType::new(DataType::Int32, true),
    ];
    let domains = naa(
        &types,
        &[
            NullPolicy::Equal,
            NullPolicy::Unmatchable,
            NullPolicy::Equal,
        ],
        1..3,
        &[0],
        IndexPurpose::PresenceOnly,
        &work,
    );
    let scope: ArrayRef = Arc::new(Int32Array::from(vec![
        None,
        Some(1),
        Some(1),
        Some(1),
        Some(1),
    ]));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![
                Some(2),
                Some(0),
                Some(1),
                Some(2),
                Some(0),
                None,
                Some(0),
            ]),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
        )
        .unwrap(),
    );
    let ordinary = dictionary.slice(1, 5);
    let nullsafe: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(2),
        None,
        Some(4),
        None,
    ]));
    let mut writer = InputRunWriter::new(
        &domains,
        B3RunId::from_input_slot(0, 1).unwrap(),
        false,
        &work,
    )
    .unwrap();
    let page = writer
        .push_evaluated(
            &source(5),
            EvaluatedJoinKeyPage::Naa {
                scope: EvaluatedKeyProjection {
                    arrays: std::slice::from_ref(&scope),
                    bindings: &types[..1],
                },
                values: EvaluatedKeyProjection {
                    arrays: &[Arc::clone(&ordinary), nullsafe],
                    bindings: &types[1..],
                },
            },
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap();
    assert!(page.payload().is_none());
    assert_eq!(page.rows(), 5);
    let facts = page.value_nulls().unwrap();
    assert_eq!(
        facts
            .rows()
            .iter()
            .map(|r| (r.row_in_page(), r.has_ordinary_null()))
            .collect::<Vec<_>>(),
        [(1, true), (2, false), (4, true)]
    );
    assert_eq!(facts.ordinary_rows().collect::<Vec<_>>(), [1, 4]);
    assert_eq!(facts.nullsafe_only_rows().collect::<Vec<_>>(), [2]);
    let values = page.values().unwrap();
    assert_eq!(values.components(), 1..3);
    assert!(Arc::ptr_eq(values.column(0).unwrap().0, &ordinary));
    assert_eq!(values.source_row(4).unwrap(), 4);
    assert!(values.column(2).is_err());
    assert_eq!(values.prepared(&work).unwrap().len(), 5);
    let receipt = writer.seal(&work).unwrap();
    assert_eq!(receipt.pages.ordinary_nulls, Some(2));
    assert_eq!(receipt.pages.ordinary_null_pages().unwrap(), [0]);
    let (inputs, _begin) = collect(vec![receipt], &work);
    let (rows, captures) = BuildRows::from_inputs(inputs, &domains, &work).unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows.ordinary_null_count().unwrap(), 2);
    let CapturesForGraphs::Naa {
        group: Some(group),
        full,
    } = captures
    else {
        panic!("real NAA captures")
    };
    let ProjectionDomains::Naa {
        group: Some(group_domain),
        full: full_domain,
        full_purpose,
        ..
    } = &domains
    else {
        unreachable!()
    };
    let _group = group
        .into_complete(group_domain, IndexPurpose::KeyIds, &work)
        .unwrap();
    let _full = full
        .into_complete(full_domain, *full_purpose, &work)
        .unwrap();
}

#[test]
fn no_scope_null_directory_skips_nonnull_and_nullsafe_only_pages_without_value_copies() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let types = [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Int32, true),
    ];
    let domains = naa(
        &types,
        &[NullPolicy::Unmatchable, NullPolicy::Equal],
        0..2,
        &[],
        IndexPurpose::PresenceOnly,
        &work,
    );
    let mut receipts = Vec::new();
    for (slot, pages) in [
        vec![
            (vec![Some(1); 4], vec![None; 4]),
            (vec![], vec![]),
            (
                vec![None, Some(1), Some(2), None],
                vec![Some(0), None, None, None],
            ),
        ],
        vec![(vec![Some(4)], vec![None]), (vec![None], vec![Some(5)])],
    ]
    .into_iter()
    .enumerate()
    {
        let mut writer = InputRunWriter::new(
            &domains,
            B3RunId::from_input_slot(slot, 2).unwrap(),
            false,
            &work,
        )
        .unwrap();
        for (ordinary, safe) in pages {
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(Int32Array::from(ordinary)),
                Arc::new(Int32Array::from(safe)),
            ];
            let page = writer
                .push_evaluated(
                    &source(arrays[0].len()),
                    EvaluatedJoinKeyPage::Naa {
                        scope: EvaluatedKeyProjection {
                            arrays: &[],
                            bindings: &[],
                        },
                        values: EvaluatedKeyProjection {
                            arrays: &arrays,
                            bindings: &types,
                        },
                    },
                    &mut ValueConversionContext::default(),
                    &work,
                )
                .unwrap();
            assert!(Arc::ptr_eq(
                page.values().unwrap().column(0).unwrap().0,
                &arrays[0]
            ));
        }
        let receipt = writer.seal(&work).unwrap();
        assert_eq!(
            receipt.pages.ordinary_null_pages().unwrap(),
            if slot == 0 { &[2][..] } else { &[1][..] }
        );
        receipts.push(receipt);
    }
    let (inputs, _lease) = collect(receipts, &work);
    let (rows, _) = BuildRows::from_inputs(inputs, &domains, &work).unwrap();
    assert_eq!(rows.len(), 10);
    assert_eq!(rows.ordinary_null_count().unwrap(), 3);
    let mut actual = Vec::new();
    for slot in 0..2 {
        let run = B3RunId::from_input_slot(slot, 2).unwrap();
        for &index in rows.run(run).unwrap().ordinary_null_pages().unwrap() {
            let page = rows.page(run, index).unwrap();
            for &row in page.value_nulls().unwrap().ordinary_row_indices() {
                actual.push(rows.row_in_page(run, index, row).unwrap().index());
            }
        }
    }
    assert_eq!(actual, [4, 7, 9]);
}

#[test]
fn no_sql_scope_can_still_have_an_explicit_nonnullable_value_group_without_changing_roles() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let types = [
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::new(DataType::Utf8, true),
    ];
    let domains = naa(
        &types,
        &[NullPolicy::Unmatchable; 2],
        0..2,
        &[0],
        IndexPurpose::KeyIds,
        &work,
    );
    let arrays: [ArrayRef; 2] = [
        Arc::new(Int32Array::from(vec![7, 9])),
        Arc::new(StringArray::from(vec![None, Some("x")])),
    ];
    let mut writer = InputRunWriter::new(
        &domains,
        B3RunId::from_input_slot(0, 1).unwrap(),
        false,
        &work,
    )
    .unwrap();
    let page = writer
        .push_evaluated(
            &source(2),
            EvaluatedJoinKeyPage::Naa {
                scope: EvaluatedKeyProjection {
                    arrays: &[],
                    bindings: &[],
                },
                values: EvaluatedKeyProjection {
                    arrays: &arrays,
                    bindings: &types,
                },
            },
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap();
    assert!(page.projection_id(ProjectionKind::Group).is_some());
    assert_eq!(page.values().unwrap().components(), 0..2);
    assert_eq!(
        page.value_nulls()
            .unwrap()
            .ordinary_rows()
            .collect::<Vec<_>>(),
        [0]
    );
    assert_eq!(writer.seal(&work).unwrap().rows(), 2);
}

#[test]
fn group_includes_original_scope_prefix_and_selected_nonnullable_value_columns_exactly() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let types = [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::new(DataType::Utf8, true),
        FunctionValueType::new(DataType::Int32, false),
    ];
    let domains = naa(
        &types,
        &[
            NullPolicy::Equal,
            NullPolicy::Unmatchable,
            NullPolicy::Unmatchable,
            NullPolicy::Unmatchable,
        ],
        1..4,
        &[0, 3, 1],
        IndexPurpose::PresenceOnly,
        &work,
    );
    let scope: ArrayRef = Arc::new(Int32Array::from(vec![5, 5]));
    let values: [ArrayRef; 3] = [
        Arc::new(Int32Array::from(vec![7, 7])),
        Arc::new(StringArray::from(vec![None, Some("x")])),
        Arc::new(Int32Array::from(vec![9, 10])),
    ];
    let mut writer = InputRunWriter::new(
        &domains,
        B3RunId::from_input_slot(0, 1).unwrap(),
        false,
        &work,
    )
    .unwrap();
    let page = writer
        .push_evaluated(
            &source(2),
            EvaluatedJoinKeyPage::Naa {
                scope: EvaluatedKeyProjection {
                    arrays: &[scope],
                    bindings: &types[..1],
                },
                values: EvaluatedKeyProjection {
                    arrays: &values,
                    bindings: &types[1..],
                },
            },
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap();
    assert_eq!(page.values().unwrap().components(), 1..4);
    for (column, expected) in values.iter().enumerate() {
        assert!(Arc::ptr_eq(
            page.values().unwrap().column(column).unwrap().0,
            expected
        ));
    }
    assert_eq!(
        page.value_nulls()
            .unwrap()
            .ordinary_rows()
            .collect::<Vec<_>>(),
        [0]
    );
    let receipt = writer.seal(&work).unwrap();
    let super::super::build_pages::InputCaptures::Naa {
        group: Some(group),
        full,
    } = receipt.captures
    else {
        panic!("actual group/full")
    };
    assert_eq!((group.input_rows(), full.input_rows()), (2, 2));
}

#[test]
fn absent_group_is_not_a_zero_key_tuple_and_empty_e_still_seals_a_real_full_run() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let types = [FunctionValueType::new(DataType::Utf8, true)];
    let domains = naa(
        &types,
        &[NullPolicy::Unmatchable],
        0..1,
        &[],
        IndexPurpose::PresenceOnly,
        &work,
    );
    let mut writer = InputRunWriter::new(
        &domains,
        B3RunId::from_input_slot(0, 1).unwrap(),
        false,
        &work,
    )
    .unwrap();
    let receipt = writer.seal(&work).unwrap();
    assert_eq!((receipt.rows(), receipt.page_count()), (0, 0));
    let (inputs, _begin) = collect(vec![receipt], &work);
    let (rows, captures) = BuildRows::from_inputs(inputs, &domains, &work).unwrap();
    assert_eq!((rows.len(), rows.run_count()), (0, 1));
    assert!(rows.row(0).is_err());
    let CapturesForGraphs::Naa { group: None, full } = captures else {
        panic!("no invented group")
    };
    let ProjectionDomains::Naa { full: domain, .. } = &domains else {
        unreachable!()
    };
    let _full = full
        .into_complete(domain, IndexPurpose::PresenceOnly, &work)
        .unwrap();
}

#[test]
fn invalid_group_metadata_cannot_omit_scope_repeat_or_add_nullable_values() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let types = [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::new(DataType::Utf8, true),
    ];
    for indices in [&[1][..], &[0, 1, 1][..], &[0, 2][..], &[][..]] {
        let domains = naa(
            &types,
            &[NullPolicy::Equal; 3],
            1..3,
            indices,
            IndexPurpose::PresenceOnly,
            &work,
        );
        assert!(
            InputRunWriter::new(
                &domains,
                B3RunId::from_input_slot(0, 1).unwrap(),
                false,
                &work
            )
            .is_err()
        );
    }
    let mut domains = naa(
        &types,
        &[NullPolicy::Equal; 3],
        1..3,
        &[0, 1],
        IndexPurpose::PresenceOnly,
        &work,
    );
    let ProjectionDomains::Naa { group, .. } = &mut domains else {
        unreachable!()
    };
    let wrong = crate::exec::hash_table::key_index::KeySpec::new(
        &types[..2],
        &[NullPolicy::Equal, NullPolicy::Unmatchable],
        work.owner,
        &|| check(&work),
    )
    .unwrap();
    *group = Some(BulkDomain::new(&wrong, work.owner, work.control).unwrap());
    assert!(
        InputRunWriter::new(
            &domains,
            B3RunId::from_input_slot(0, 1).unwrap(),
            false,
            &work
        )
        .is_err()
    );
}

#[test]
fn append_refusal_latches_first_cause_drops_partial_state_and_prevents_seal() {
    struct Stop;
    impl KernelEvaluationControl for Stop {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Err(KernelFailure::Cancelled)
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("capture never waits")
        }
    }
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let ty = FunctionValueType::new(DataType::Int32, false);
    let domains = ordinary(&ty, NullPolicy::Equal, IndexPurpose::KeyIds, &work);
    let mut writer = InputRunWriter::new(
        &domains,
        B3RunId::from_input_slot(0, 1).unwrap(),
        true,
        &work,
    )
    .unwrap();
    let keys: [ArrayRef; 1] = [Arc::new(Int32Array::from(vec![1]))];
    let good = writer
        .push_evaluated(
            &source(1),
            EvaluatedJoinKeyPage::Ordinary(EvaluatedKeyProjection {
                arrays: &keys,
                bindings: std::slice::from_ref(&ty),
            }),
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap();
    let stopped = BulkWork {
        owner: work.owner,
        allocator: work.allocator.clone(),
        host: Arc::clone(&work.host),
        control: &Stop,
    };
    let original = error(writer.push_evaluated(
        &source(1),
        EvaluatedJoinKeyPage::Ordinary(EvaluatedKeyProjection {
            arrays: &keys,
            bindings: std::slice::from_ref(&ty),
        }),
        &mut ValueConversionContext::default(),
        &stopped,
    ));
    assert!(error(writer.seal(&work)).same_occurrence(&original));
    assert!(writer.pages.is_empty());
    assert!(writer.captures.is_none());
    assert_eq!(
        good.payload()
            .unwrap()
            .column(0)
            .unwrap()
            .array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        0
    );
}

#[test]
fn failure_after_group_capture_cannot_turn_into_normal_full_receipt() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let types = [
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::new(DataType::Int32, false),
    ];
    let domains = naa(
        &types,
        &[NullPolicy::Equal; 2],
        1..2,
        &[0],
        IndexPurpose::PresenceOnly,
        &work,
    );
    let mut writer = InputRunWriter::new(
        &domains,
        B3RunId::from_input_slot(0, 1).unwrap(),
        false,
        &work,
    )
    .unwrap();
    // The original group succeeds. The full source then violates its required
    // signed value declaration; its original RequiredNull author must refuse.
    let scope: [ArrayRef; 1] = [Arc::new(Int32Array::from(vec![1]))];
    let values: [ArrayRef; 1] = [Arc::new(Int32Array::from(vec![None::<i32>]))];
    let cause = error(writer.push_evaluated(
        &source(1),
        EvaluatedJoinKeyPage::Naa {
            scope: EvaluatedKeyProjection {
                arrays: &scope,
                bindings: &types[..1],
            },
            values: EvaluatedKeyProjection {
                arrays: &values,
                bindings: &types[1..],
            },
        },
        &mut ValueConversionContext::default(),
        &work,
    ));
    assert!(error(writer.seal(&work)).same_occurrence(&cause));
    assert_eq!(writer.rows, 0);
}
