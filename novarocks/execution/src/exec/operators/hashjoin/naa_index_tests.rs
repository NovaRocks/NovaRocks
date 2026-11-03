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

use super::super::{
    build_capture::{EvaluatedJoinKeyPage, EvaluatedKeyProjection, InputRunWriter},
    build_finalize::{FinalizeOptions, FinalizeStep, FinalizeWorker, JoinBuildFrame},
    build_pages::tests::{naa, source, work},
    build_state::ProjectionDomains,
    join_completion::{
        JoinParticipants,
        tests::{Control, fixture},
    },
};
use super::*;
use arrow::{
    array::{ArrayRef, Int32Array},
    datatypes::DataType,
};
use novarocks_type_contract::FunctionValueType;
use novarocks_value_arrays::{decode::ValueConversionContext, reachability::ReachableRows};
use std::{
    collections::BTreeSet,
    num::{NonZeroU64, NonZeroUsize},
};

type Row = Vec<Option<i32>>;
fn turn(units: u32) -> TurnWork {
    TurnWork::new(NonZeroU64::MIN, units)
}
fn arrays(rows: &[Row], width: usize) -> Vec<ArrayRef> {
    (0..width)
        .map(|column| {
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row[column]).collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect()
}
fn build(
    rows: &[Row],
    scope: usize,
    types: &[FunctionValueType],
    policies: &[NullPolicy],
    eligibility: bool,
    linear: bool,
    work: &BulkWork<'_>,
) -> (Arc<FinalizedBuild>, Vec<usize>) {
    let value_modes = policies[scope..]
        .iter()
        .map(|p| *p == NullPolicy::Equal)
        .collect::<Vec<_>>();
    let mode = if linear {
        NaaValueMode::from_signed_values(
            &types[scope..],
            &types[scope..],
            &value_modes,
            eligibility,
            work.control,
        )
        .unwrap()
    } else {
        NaaValueMode::General
    };
    let mut group_columns = (0..scope).collect::<Vec<_>>();
    if let NaaValueMode::SingleNullable { value } = mode {
        group_columns.extend((scope..types.len()).filter(|&column| column != scope + value));
    }
    let mut domains = naa(
        types,
        policies,
        scope..types.len(),
        &group_columns,
        if eligibility {
            IndexPurpose::KeyIds
        } else {
            IndexPurpose::PresenceOnly
        },
        work,
    );
    let ProjectionDomains::Naa { value_mode, .. } = &mut domains else {
        unreachable!()
    };
    *value_mode = mode;
    let frame = JoinBuildFrame::new(
        domains,
        FinalizeOptions {
            participants: JoinParticipants {
                inputs: 2,
                finalizers: NonZeroUsize::new(3).unwrap(),
                probes: 2,
                tails: 0,
            },
            handoff: None,
            live_slots: NonZeroUsize::MIN,
            match_bits: false,
        },
        work,
    )
    .unwrap();
    let midpoint = rows.len() / 2;
    for (slot, rows) in [&rows[..midpoint], &rows[midpoint..]]
        .into_iter()
        .enumerate()
    {
        let mut writer = InputRunWriter::new(
            &frame.domains,
            B3RunId::from_input_slot(slot, 2).unwrap(),
            false,
            work,
        )
        .unwrap();
        // Empty physical pages are real E input, rather than missing receipts.
        let empty = arrays(&[], types.len());
        writer
            .push_evaluated(
                &source(0),
                EvaluatedJoinKeyPage::Naa {
                    scope: EvaluatedKeyProjection {
                        arrays: &empty[..scope],
                        bindings: &types[..scope],
                    },
                    values: EvaluatedKeyProjection {
                        arrays: &empty[scope..],
                        bindings: &types[scope..],
                    },
                },
                &mut ValueConversionContext::default(),
                work,
            )
            .unwrap();
        for page in rows.chunks(37) {
            let columns = arrays(page, types.len());
            writer
                .push_evaluated(
                    &source(page.len()),
                    EvaluatedJoinKeyPage::Naa {
                        scope: EvaluatedKeyProjection {
                            arrays: &columns[..scope],
                            bindings: &types[..scope],
                        },
                        values: EvaluatedKeyProjection {
                            arrays: &columns[scope..],
                            bindings: &types[scope..],
                        },
                    },
                    &mut ValueConversionContext::default(),
                    work,
                )
                .unwrap();
        }
        frame
            .inputs
            .normal_input(slot, writer.seal(work).unwrap())
            .unwrap();
    }
    let mut workers = [
        FinalizeWorker::new(),
        FinalizeWorker::new(),
        FinalizeWorker::new(),
    ];
    let mut finished = false;
    for _ in 0..1_000_000 {
        for worker in &mut workers {
            if worker.step(&frame, &mut turn(17), work).unwrap() == FinalizeStep::Finished {
                finished = true;
                break;
            }
        }
        if finished {
            break;
        }
    }
    assert!(finished, "actual original K/CSR publication");
    (
        frame
            .completion
            .published()
            .unwrap()
            .unwrap()
            .artifact
            .clone(),
        group_columns,
    )
}
fn capture(
    spec: &crate::exec::hash_table::key_index::KeySpec,
    columns: &[ArrayRef],
    types: &[FunctionValueType],
    work: &BulkWork<'_>,
) -> KeyInput {
    let count = columns[0].len();
    let readable = ReachableRows::all(count)
        .materialize_observed(
            &mut |layout| {
                work.owner
                    .request(layout)
                    .map_err(crate::runtime::fragment::ExecutionFailure::from)
            },
            &mut |_| Ok::<_, crate::runtime::fragment::ExecutionFailure>(()),
        )
        .unwrap();
    KeyInput::new_for_spec(
        spec,
        columns,
        types,
        &readable,
        &(0..count).collect::<Vec<_>>(),
        &mut ValueConversionContext::default(),
        work.owner,
        &|| work.control.checkpoint(0).map_err(Into::into),
    )
    .unwrap()
}
fn probe(
    store: &NaaCandidateStore,
    rows: &[Row],
    types: &[FunctionValueType],
    group: &[usize],
    work: &BulkWork<'_>,
) -> NaaProbePage {
    let columns = arrays(rows, types.len());
    let full = capture(store.full_spec(), &columns, types, work);
    let group = store.group_spec().map(|spec| {
        capture(
            spec,
            &group
                .iter()
                .map(|&i| columns[i].clone())
                .collect::<Vec<_>>(),
            &group.iter().map(|&i| types[i].clone()).collect::<Vec<_>>(),
            work,
        )
    });
    store.prepare_probe(full, group, work).unwrap()
}
fn eq(a: Option<i32>, b: Option<i32>, policy: NullPolicy) -> Option<bool> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a == b),
        _ if policy == NullPolicy::Equal => Some(a == b),
        _ => None,
    }
}
fn truth(left: &[Option<i32>], right: &[Option<i32>], policies: &[NullPolicy]) -> Option<bool> {
    let mut unknown = false;
    for ((&a, &b), &policy) in left.iter().zip(right).zip(policies) {
        match eq(a, b, policy) {
            Some(false) => return Some(false),
            None => unknown = true,
            Some(true) => {}
        }
    }
    if unknown { None } else { Some(true) }
}
fn candidates(
    store: &NaaCandidateStore,
    mut cursor: NaaCandidates,
    work: &BulkWork<'_>,
) -> Vec<BuildRowId> {
    match cursor.next(store, &mut turn(0), work).unwrap() {
        NaaCandidateStep::Finished => return Vec::new(),
        NaaCandidateStep::Yielded => {}
        NaaCandidateStep::Row(_) => panic!("zero work cannot advance a physical candidate"),
    }
    let mut rows = Vec::new();
    loop {
        let mut allowance = turn(7);
        loop {
            match cursor.next(store, &mut allowance, work).unwrap() {
                NaaCandidateStep::Row(row) => rows.push(row),
                NaaCandidateStep::Yielded => break,
                NaaCandidateStep::Finished => return rows,
            }
        }
    }
}
#[test]
fn general_candidate_matrix_matches_independent_three_value_oracle_without_cross_scope_or_duplicate_pairs()
 {
    for scope in [0, 1] {
        for scope_mode in [NullPolicy::Unmatchable, NullPolicy::Equal] {
            for cmodes in [
                [NullPolicy::Unmatchable; 2],
                [NullPolicy::Unmatchable, NullPolicy::Equal],
                [NullPolicy::Equal, NullPolicy::Unmatchable],
                [NullPolicy::Equal; 2],
            ] {
                for eligibility in [false, true] {
                    let (owner, tracker) = fixture();
                    let work = work(owner.as_ref(), &tracker, &Control);
                    let mut policies = vec![scope_mode; scope];
                    policies.extend(cmodes);
                    let types = vec![FunctionValueType::new(DataType::Int32, true); scope + 2];
                    let mut build_rows = Vec::new();
                    for g in [None, Some(1), Some(2)] {
                        for pair in [
                            [None, Some(1)],
                            [Some(2), Some(1)],
                            [Some(2), Some(2)],
                            [Some(2), Some(1)],
                            [Some(2), None],
                            [None, None],
                        ] {
                            let mut row = vec![g; scope];
                            row.extend(pair);
                            build_rows.push(row);
                        }
                    }
                    let (data, group_columns) = build(
                        &build_rows,
                        scope,
                        &types,
                        &policies,
                        eligibility,
                        false,
                        &work,
                    );
                    let store =
                        NaaCandidateStore::new(&data, scope..scope + 2, eligibility).unwrap();
                    let mut probe_rows = Vec::new();
                    for g in [None, Some(1), Some(2), Some(9)] {
                        for pair in [
                            [None, Some(1)],
                            [None, Some(9)],
                            [Some(2), Some(1)],
                            [Some(2), Some(9)],
                            [Some(2), None],
                            [None, None],
                        ] {
                            let mut row = vec![g; scope];
                            row.extend(pair);
                            probe_rows.push(row);
                        }
                    }
                    let page = probe(&store, &probe_rows, &types, &group_columns, &work);
                    for (i, left) in probe_rows.iter().enumerate() {
                        let in_scope = |right: &Row| {
                            truth(&left[..scope], &right[..scope], &policies[..scope]) == Some(true)
                        };
                        let eligible = |b: usize| !eligibility || b % 3 == 1;
                        let expected = build_rows.iter().enumerate().any(|(b, right)| {
                            in_scope(right)
                                && truth(&left[scope..], &right[scope..], &policies[scope..])
                                    != Some(false)
                                && eligible(b)
                        });
                        let actual = match store.start(&page, i).unwrap() {
                            NaaRowStart::Decided { suppressed } => suppressed,
                            NaaRowStart::Candidates(cursor) => {
                                let rows = candidates(&store, cursor, &work);
                                assert_eq!(
                                    rows.iter()
                                        .map(|r| r.index())
                                        .collect::<BTreeSet<_>>()
                                        .len(),
                                    rows.len()
                                );
                                assert!(rows.iter().all(|r| in_scope(&build_rows[r.index()])));
                                let mut compared = vec![None; rows.len()];
                                store
                                    .compare_pairs(
                                        &page,
                                        &vec![i; rows.len()],
                                        &rows,
                                        &mut compared,
                                        &work,
                                    )
                                    .unwrap();
                                for (&row, &actual) in rows.iter().zip(&compared) {
                                    assert_eq!(
                                        actual,
                                        truth(
                                            &left[scope..],
                                            &build_rows[row.index()][scope..],
                                            &policies[scope..]
                                        )
                                    );
                                }
                                // Visit every duplicate even after an earlier suppression. P07
                                // consumes this cursor to preserve later required E evaluation.
                                if eligibility {
                                    for (b, right) in build_rows.iter().enumerate() {
                                        if in_scope(right)
                                            && truth(
                                                &left[scope..],
                                                &right[scope..],
                                                &policies[scope..],
                                            ) != Some(false)
                                        {
                                            assert!(rows.iter().any(|r| r.index() == b));
                                        }
                                    }
                                }
                                rows.iter()
                                    .zip(compared)
                                    .any(|(r, c)| c != Some(false) && eligible(r.index()))
                            }
                        };
                        assert_eq!(
                            actual, expected,
                            "scope={scope}, policies={policies:?}, eligibility={eligibility}, probe={left:?}"
                        );
                    }
                }
            }
        }
    }
}
#[test]
fn nullable_probe_scans_complete_unscoped_domain_across_actual_k_partitions() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let types = vec![FunctionValueType::new(DataType::Int32, true); 2];
    let build_rows = (0..2048)
        .map(|i| vec![Some(2), Some(i)])
        .collect::<Vec<_>>();
    let (data, group) = build(
        &build_rows,
        0,
        &types,
        &[NullPolicy::Unmatchable; 2],
        false,
        false,
        &work,
    );
    let store = NaaCandidateStore::new(&data, 0..2, false).unwrap();
    assert!(
        store.full.view.partition_count() > 1,
        "actual K partitioning, never a B3 router"
    );
    let page = probe(
        &store,
        &[vec![None, Some(1)], vec![None, Some(-1)]],
        &types,
        &group,
        &work,
    );
    for i in 0..2 {
        let NaaRowStart::Candidates(cursor) = store.start(&page, i).unwrap() else {
            panic!("general NULL probe");
        };
        let rows = candidates(&store, cursor, &work);
        assert_eq!(rows.len(), build_rows.len());
        let mut compared = vec![None; rows.len()];
        store
            .compare_pairs(&page, &vec![i; rows.len()], &rows, &mut compared, &work)
            .unwrap();
        assert_eq!(compared.iter().any(|c| *c != Some(false)), i == 0);
        assert_eq!(
            compared.iter().filter(|c| c.is_none()).count(),
            usize::from(i == 0)
        );
    }
}
#[test]
fn signed_linear_modes_use_membership_and_group_summaries_without_duplicate_scans() {
    for nullable in [false, true] {
        for scope in [0, 1] {
            let (owner, tracker) = fixture();
            let work = work(owner.as_ref(), &tracker, &Control);
            let mut types = vec![FunctionValueType::new(DataType::Int32, false); scope + 2];
            types[scope] = FunctionValueType::new(DataType::Int32, nullable);
            let policies = vec![NullPolicy::Unmatchable; scope + 2];
            let mut rows = Vec::new();
            for i in 0..2000 {
                let mut row = vec![Some(1); scope];
                row.extend([
                    if nullable && i % 2 == 0 {
                        None
                    } else {
                        Some(2)
                    },
                    Some(3),
                ]);
                rows.push(row);
            }
            let (data, group) = build(&rows, scope, &types, &policies, false, true, &work);
            let store = NaaCandidateStore::new(&data, scope..scope + 2, false).unwrap();
            let mut probes = Vec::new();
            for value in [Some(2), Some(9)]
                .into_iter()
                .chain(nullable.then_some(None))
            {
                for other in [3, 8] {
                    let mut row = vec![Some(1); scope];
                    row.extend([value, Some(other)]);
                    probes.push(row);
                }
            }
            let page = probe(&store, &probes, &types, &group, &work);
            for (i, left) in probes.iter().enumerate() {
                let expected = rows
                    .iter()
                    .any(|right| truth(left, right, &policies) != Some(false));
                let NaaRowStart::Decided { suppressed } = store.start(&page, i).unwrap() else {
                    panic!("linear mode must not create a duplicate-group cursor");
                };
                assert_eq!(suppressed, expected);
            }
        }
    }
}
#[test]
fn empty_physical_domain_keeps_null_probes_and_no_eligibility_pairs() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let types = vec![FunctionValueType::new(DataType::Int32, true); 2];
    for eligibility in [false, true] {
        let (data, group) = build(
            &[],
            0,
            &types,
            &[NullPolicy::Unmatchable; 2],
            eligibility,
            false,
            &work,
        );
        let store = NaaCandidateStore::new(&data, 0..2, eligibility).unwrap();
        let page = probe(&store, &[vec![None, Some(1)]], &types, &group, &work);
        let NaaRowStart::Candidates(mut cursor) = store.start(&page, 0).unwrap() else {
            panic!("general empty domain");
        };
        assert_eq!(
            cursor.next(&store, &mut turn(7), &work).unwrap(),
            NaaCandidateStep::Finished
        );
    }
}

#[test]
fn single_key_mark_truth_preserves_true_unknown_false_and_empty_build() {
    let probe_rows = vec![vec![Some(1)], vec![Some(2)], vec![None], vec![Some(1)]];
    for build_rows in [
        vec![],
        vec![vec![Some(1)], vec![Some(1)]],
        vec![vec![None]],
        vec![vec![Some(1)], vec![None]],
    ] {
        let (owner, tracker) = fixture();
        let work = work(owner.as_ref(), &tracker, &Control);
        let types = [FunctionValueType::new(DataType::Int32, true)];
        let policies = [NullPolicy::Unmatchable];
        let (data, group) = build(&build_rows, 0, &types, &policies, false, true, &work);
        let store = NaaCandidateStore::new(&data, 0..1, false).unwrap();
        let page = probe(&store, &probe_rows, &types, &group, &work);
        for (row, values) in probe_rows.iter().enumerate() {
            // An independent SQL ANY oracle over original row occurrences.
            let comparisons = build_rows
                .iter()
                .map(|build| eq(values[0], build[0], policies[0]))
                .collect::<Vec<_>>();
            let expected = if comparisons.contains(&Some(true)) {
                Some(true)
            } else if comparisons.contains(&None) {
                None
            } else {
                Some(false)
            };
            assert_eq!(store.mark_truth(&page, row).unwrap(), expected);
        }
    }
}

#[cfg(novarocks_dev_controls)]
#[test]
fn retained_key_observation_disappears_when_original_writer_latches_failure() {
    let (owner, tracker) = fixture();
    let work = work(owner.as_ref(), &tracker, &Control);
    let types = [FunctionValueType::new(DataType::Int32, true)];
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
    assert_eq!(writer.retained_key_rows(), None);
    let columns = arrays(&[vec![Some(1)], vec![None]], 1);
    writer
        .push_evaluated(
            &source(2),
            EvaluatedJoinKeyPage::Naa {
                scope: EvaluatedKeyProjection {
                    arrays: &[],
                    bindings: &[],
                },
                values: EvaluatedKeyProjection {
                    arrays: &columns,
                    bindings: &types,
                },
            },
            &mut ValueConversionContext::default(),
            &work,
        )
        .unwrap();
    assert_eq!(writer.retained_key_rows(), Some(2));
    assert!(writer.push_payload(&source(1), &work).is_err());
    assert_eq!(
        writer.retained_key_rows(),
        None,
        "historical rows cannot witness destroyed pages"
    );
}
