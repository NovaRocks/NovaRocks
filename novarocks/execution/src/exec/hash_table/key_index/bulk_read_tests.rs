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
    KeyInput, NullPolicy,
    direct_int::{
        CountCursor, DirectGeometry, DirectWork, FillCursor, InitCursor, MarkCursor, PrefixCursor,
    },
    local_hash::BYTE_HASH_INITS,
    statistics::IntegerDomain,
};
use super::*;
use crate::{
    exec::{hash_table::allocation::AggregateAllocator, pipeline::operator::ConstructionFailure},
    runtime::{mem_tracker::MemTracker, tracked_state_allocator::expression_allocation_host},
};
use arrow::{
    array::{
        ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array, StringArray, StringViewArray,
    },
    datatypes::DataType,
};
use novarocks_failure_contract::FailureProjection;
use novarocks_functions::KernelFailure;
use novarocks_value_arrays::{
    ValueError, decode::ValueConversionContext, reachability::ReachableRows,
};
use std::{alloc::Layout, cell::Cell, num::NonZeroUsize};
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("direct read never waits")
    }
}
fn fixture() -> (Box<dyn InputConstructionOwner>, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("direct read original owner");
    let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default())).unwrap();
    let layout = crate::exec::chunk::typed_fixture::layout(
        Arc::new(arrow::datatypes::Schema::empty()),
        &[],
        &[],
    );
    let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
    (factory.begin_typed_construction(&schema).unwrap(), tracker)
}
fn ports<'a>(owner: &'a dyn InputConstructionOwner, tracker: &Arc<MemTracker>) -> DirectWork<'a> {
    DirectWork {
        owner,
        allocator: AggregateAllocator::new(tracker.clone()),
        host: expression_allocation_host(tracker.clone()),
        control: &Control,
    }
}
fn error<T>(result: ExecutionResult<T>) -> ExecutionFailure {
    match result {
        Err(e) => e,
        Ok(_) => panic!("the operation must reject"),
    }
}
fn assert_invariant(cause: &ExecutionFailure, v: ValueViolation) {
    assert_eq!(
        cause.failure_fact(),
        ValueError::Invariant(v).failure_fact()
    );
}
fn build_data(
    array: ArrayRef,
    spec_ty: &FunctionValueType,
    actual: &FunctionValueType,
    min_max: Option<(i64, i64)>,
    purpose: IndexPurpose,
    workers: usize,
    work: &DirectWork<'_>,
) -> Arc<Storage> {
    let spec = KeySpec::new(
        std::slice::from_ref(spec_ty),
        &[NullPolicy::Equal],
        work.owner,
        &|| Ok(()),
    )
    .unwrap();
    let rows = (0..array.len()).collect::<Vec<_>>();
    let readable = ReachableRows::all(array.len())
        .materialize_observed(
            &mut |l| work.owner.request(l).map_err(ExecutionFailure::from),
            &mut |_| Ok(()),
        )
        .unwrap();
    let keys = KeyInput::new_for_spec(
        &spec,
        &[array],
        std::slice::from_ref(actual),
        &readable,
        &rows,
        &mut ValueConversionContext::default(),
        work.owner,
        &|| Ok(()),
    )
    .unwrap();
    let geometry = DirectGeometry::for_selected(
        &spec,
        IntegerDomain { min_max },
        purpose,
        NonZeroUsize::new(workers).unwrap(),
    )
    .unwrap();
    let store = Storage::new(geometry, &spec, Some(actual.nullable()), work).unwrap();
    for shard in 0..geometry.shard_count() {
        let mut cursor = InitCursor::new(Arc::clone(&store), shard, work).unwrap();
        while !cursor.advance(97, work).unwrap() {}
    }
    let mut mark = MarkCursor::new(Arc::clone(&store), &keys, 0..keys.len()).unwrap();
    while !mark.advance(17, work).unwrap() {}
    if purpose == IndexPurpose::KeyIds {
        let mut base = 0;
        for shard in 0..geometry.shard_count() {
            let mut count = CountCursor::new(Arc::clone(&store), shard, work).unwrap();
            while !count.advance(7, work).unwrap() {}
            let counted = count.finish().unwrap();
            let next = base + counted.total();
            let mut prefix = PrefixCursor::new(counted, base).unwrap();
            while !prefix.advance(5, work).unwrap() {}
            let mut fill = FillCursor::new(prefix.finish().unwrap(), work).unwrap();
            while !fill.advance(11, work).unwrap() {}
            base = next;
        }
        store.finish_null(base, work).unwrap();
    }
    store
}

fn keys(
    spec: &KeySpec,
    arrays: &[ArrayRef],
    bindings: &[FunctionValueType],
    rows: &[usize],
    owner: &dyn InputConstructionOwner,
) -> KeyInput {
    let readable = ReachableRows::all(arrays[0].len())
        .materialize_observed(
            &mut |l| owner.request(l).map_err(ExecutionFailure::from),
            &mut |_| Ok(()),
        )
        .unwrap();
    KeyInput::new_for_spec(
        spec,
        arrays,
        bindings,
        &readable,
        rows,
        &mut ValueConversionContext::default(),
        owner,
        &|| Ok(()),
    )
    .unwrap()
}
fn prepared<'a>(
    view: &IndexView,
    input: &'a KeyInput,
    tracker: &Arc<MemTracker>,
) -> PreparedKeyBatch<'a> {
    PreparedKeyBatch::from_captured(
        view.spec(),
        input,
        AggregateAllocator::new(tracker.clone()),
        &|| Ok(()),
    )
    .unwrap()
}
fn hash_view(
    arrays: &[ArrayRef],
    types: &[FunctionValueType],
    rows: &[usize],
    nulls: &[NullPolicy],
    purpose: IndexPurpose,
    owner: &dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
) -> (IndexView, KeyInput, LocalHashBatch) {
    let spec = KeySpec::new(types, nulls, owner, &|| Ok(())).unwrap();
    let policy = LocalHashPolicy::with_seed(&spec, 0x4372617465, owner).unwrap();
    let input = keys(&spec, arrays, types, rows, owner);
    let prepared = PreparedKeyBatch::from_captured(
        &spec,
        &input,
        AggregateAllocator::new(tracker.clone()),
        &|| Ok(()),
    )
    .unwrap();
    let hashes = hash_batch(
        &prepared,
        &policy,
        &mut HashScratch::default(),
        owner,
        &|| Ok(()),
    )
    .unwrap();
    let eligible = (0..hashes.len())
        .filter(|&i| hashes.participates(i).unwrap())
        .count();
    let layout = HashLayout::for_bulk(2, eligible);
    let mut partitions = vector(owner, layout.partitions()).unwrap();
    for partition in 0..layout.partitions() {
        let mut index = KeyIndex::new_for_bulk_hash(
            spec.clone(),
            purpose,
            policy.clone(),
            layout,
            tracker.clone(),
            owner,
        )
        .unwrap();
        let occurrences = (0..input.len())
            .filter(|&i| {
                hashes.participates(i).unwrap()
                    && layout.route(hashes.hash(i).unwrap()) == partition
            })
            .collect::<Vec<_>>();
        match purpose {
            IndexPurpose::KeyIds => {
                drop(
                    index
                        .intern_selected(&prepared, &hashes, &occurrences, owner, &Control)
                        .unwrap(),
                );
            }
            IndexPurpose::PresenceOnly => {
                index
                    .include_selected_presence(&prepared, &hashes, &occurrences, owner, &Control)
                    .unwrap();
            }
        }
        partitions.push(index);
    }
    drop(prepared);
    (
        IndexView::hash(partitions, spec, policy, layout, purpose, owner, &Control).unwrap(),
        input,
        hashes,
    )
}
fn found(value: BulkMatch) -> BulkKey {
    match value {
        BulkMatch::Found(key) => key,
        _ => panic!("known selected class"),
    }
}

#[test]
fn direct_all_signed_widths_keep_null_last_ranges_and_duplicate_typed_reads_without_saved_hashes() {
    let (owner, tracker) = fixture();
    macro_rules! case {
        ($dtype:expr, $array:ty) => {{
            let ty = FunctionValueType::new($dtype, true);
            let store = build_data(
                Arc::new(<$array>::from(vec![
                    Some(-3),
                    Some(2),
                    None,
                    Some(1),
                    Some(-3),
                ])),
                &ty,
                &ty,
                Some((-3, 2)),
                IndexPurpose::KeyIds,
                3,
                &ports(owner.as_ref(), &tracker),
            );
            let view = IndexView::direct(store, owner.as_ref(), &Control).unwrap();
            let input = keys(
                view.spec(),
                &[Arc::new(<$array>::from(vec![
                    Some(-4),
                    None,
                    Some(2),
                    Some(-3),
                    Some(1),
                    None,
                ]))],
                std::slice::from_ref(&ty),
                &[0, 1, 2, 3, 4, 5],
                owner.as_ref(),
            );
            let prepared = prepared(&view, &input, &tracker);
            BYTE_HASH_INITS.with(|n| n.set(0));
            let matches = view
                .probe_range(&prepared, None, 1..5, owner.as_ref(), &Control)
                .unwrap();
            assert_eq!(
                matches.matches(),
                &[3, 2, 0, 1].map(|id| BulkMatch::Found(BulkKey {
                    partition: 0,
                    id: KeyId(id)
                }))
            );
            let present = view
                .contains(&prepared, None, owner.as_ref(), &Control)
                .unwrap();
            assert_eq!(present.present(), &[0, 1, 1, 1, 1, 1]);
            assert_eq!(present.participates(), &[1; 6]);
            assert_eq!(BYTE_HASH_INITS.with(Cell::get), 0);
            let requested = [2, 3, 0, 2].map(|id| BulkKey {
                partition: 0,
                id: KeyId(id),
            });
            let output = view
                .export_keys(
                    &requested,
                    std::slice::from_ref(&ty),
                    owner.as_ref(),
                    expression_allocation_host(tracker.clone()),
                    &Control,
                )
                .unwrap();
            assert_eq!(
                output.arrays()[0]
                    .as_any()
                    .downcast_ref::<$array>()
                    .unwrap(),
                &<$array>::from(vec![Some(2), None, Some(-3), Some(2)])
            );
            let required = FunctionValueType::new($dtype, false);
            assert_invariant(
                &error(view.export_keys(
                    &requested,
                    &[required],
                    owner.as_ref(),
                    expression_allocation_host(tracker.clone()),
                    &Control,
                )),
                ValueViolation::RequiredNull,
            );
            assert!(
                view.probe(&prepared, None, owner.as_ref(), &Control)
                    .is_ok()
            );
        }};
    }
    case!(DataType::Int8, Int8Array);
    case!(DataType::Int16, Int16Array);
    case!(DataType::Int32, Int32Array);
    case!(DataType::Int64, Int64Array);
}

#[test]
fn real_multipart_hash_matches_reference_class_ids_ranges_duplicates_and_exactly_one_uncached_hash_batch()
 {
    let (owner, tracker) = fixture();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let values = (0..32)
        .map(|i| Some(format!("a sufficiently long key {i}")))
        .chain([None])
        .collect::<Vec<_>>();
    let array: ArrayRef = Arc::new(StringArray::from(values.clone()));
    let mut rows = (0..33).rev().collect::<Vec<_>>();
    rows.extend([4, 4, 17, 32]);
    let (view, input, hashes) = hash_view(
        &[array],
        std::slice::from_ref(&ty),
        &rows,
        &[NullPolicy::Equal],
        IndexPurpose::KeyIds,
        owner.as_ref(),
        &tracker,
    );
    let Backend::Hash { layout, policy, .. } = &view.backend else {
        panic!("hash fixture")
    };
    // Independent first-occurrence class model; it compares actual logical
    // strings/NULL, never the index's equality or returned IDs.
    let mut classes = vec![Vec::<Option<String>>::new(); layout.partitions()];
    let expected = (0..input.len())
        .map(|occurrence| {
            let partition = layout.route(hashes.hash(occurrence).unwrap());
            let value = values[input.row(occurrence).unwrap()].clone();
            let id = classes[partition]
                .iter()
                .position(|old| old == &value)
                .unwrap_or_else(|| {
                    classes[partition].push(value);
                    classes[partition].len() - 1
                });
            BulkMatch::Found(BulkKey {
                partition,
                id: KeyId(id),
            })
        })
        .collect::<Vec<_>>();
    assert!(classes.iter().filter(|c| !c.is_empty()).count() > 1);
    let prepared = prepared(&view, &input, &tracker);
    BYTE_HASH_INITS.with(|n| n.set(0));
    let saved = view
        .probe(&prepared, Some(&hashes), owner.as_ref(), &Control)
        .unwrap();
    assert_eq!(saved.matches(), expected);
    assert_eq!(BYTE_HASH_INITS.with(Cell::get), 0);
    let sliced = view
        .probe_range(&prepared, Some(&hashes), 3..19, owner.as_ref(), &Control)
        .unwrap();
    assert_eq!(sliced.matches(), &expected[3..19]);
    BYTE_HASH_INITS.with(|n| n.set(0));
    drop(
        hash_batch(
            &prepared,
            policy,
            &mut HashScratch::default(),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap(),
    );
    let once = BYTE_HASH_INITS.with(Cell::get);
    assert!(once > 0);
    BYTE_HASH_INITS.with(|n| n.set(0));
    assert_eq!(
        view.probe(&prepared, None, owner.as_ref(), &Control)
            .unwrap()
            .matches(),
        expected
    );
    assert_eq!(BYTE_HASH_INITS.with(Cell::get), once);
    let query = keys(
        view.spec(),
        &[Arc::new(StringArray::from(vec![
            values[4].as_deref(),
            Some("a new key absent from build"),
            None,
        ]))],
        std::slice::from_ref(&ty),
        &[0, 1, 2, 0],
        owner.as_ref(),
    );
    let query_prepared = PreparedKeyBatch::from_captured(
        view.spec(),
        &query,
        AggregateAllocator::new(tracker.clone()),
        &|| Ok(()),
    )
    .unwrap();
    let known = expected[rows.iter().position(|&row| row == 4).unwrap()];
    let null = expected[rows.iter().position(|&row| row == 32).unwrap()];
    assert_eq!(
        view.probe(&query_prepared, None, owner.as_ref(), &Control)
            .unwrap()
            .matches(),
        &[known, BulkMatch::Miss, null, known]
    );
    let order = [19, 0, 4, 19, 2, 30];
    let requested = order.map(|i| found(expected[i]));
    let read = view
        .read_keys(&requested, owner.as_ref(), &Control)
        .unwrap();
    let mut seen = vec![false; requested.len()];
    for page in read.columns()[0].pages() {
        for row in page.rows() {
            assert!(!seen[row.ordinal]);
            seen[row.ordinal] = true;
        }
    }
    assert!(seen.into_iter().all(|v| v));
    drop(read);
    let target = FunctionValueType::new(DataType::Utf8View, true);
    let output = view
        .export_keys(
            &requested,
            &[target],
            owner.as_ref(),
            expression_allocation_host(tracker.clone()),
            &Control,
        )
        .unwrap();
    let expected_values = order
        .iter()
        .map(|&i| values[input.row(i).unwrap()].as_deref())
        .collect::<Vec<_>>();
    assert_eq!(
        output.arrays()[0]
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        expected_values
    );
}

#[test]
fn composite_reindexed_loans_preserve_both_columns_nulls_and_signed_output_layout() {
    let (owner, tracker) = fixture();
    let types = [
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::new(DataType::Utf8, true),
    ];
    let arrays: [ArrayRef; 2] = [
        Arc::new(Int32Array::from(vec![1, 2, 1, 99, 1])),
        Arc::new(StringArray::from(vec![
            Some("long alpha value"),
            Some("long beta value"),
            Some("long alpha value"),
            Some("unselected"),
            None,
        ])),
    ];
    let rows = [2, 0, 1, 4, 2, 1];
    let (view, input, hashes) = hash_view(
        &arrays,
        &types,
        &rows,
        &[NullPolicy::Equal; 2],
        IndexPurpose::KeyIds,
        owner.as_ref(),
        &tracker,
    );
    let prepared = prepared(&view, &input, &tracker);
    let probe = view
        .probe(&prepared, Some(&hashes), owner.as_ref(), &Control)
        .unwrap();
    let order = [4, 2, 3, 0, 2];
    let requested = order.map(|i| found(probe.matches()[i]));
    let targets = [
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::new(DataType::Utf8View, true),
    ];
    let output = view
        .export_keys(
            &requested,
            &targets,
            owner.as_ref(),
            expression_allocation_host(tracker.clone()),
            &Control,
        )
        .unwrap();
    assert_eq!(
        output.arrays()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &Int32Array::from(vec![1, 2, 1, 1, 2])
    );
    assert_eq!(
        output.arrays()[1]
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some("long alpha value"),
            Some("long beta value"),
            None,
            Some("long alpha value"),
            Some("long beta value")
        ]
    );
}

#[test]
fn empty_and_presence_views_keep_participation_and_refuse_id_access_even_for_empty_requests() {
    let (owner, tracker) = fixture();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    for nulls in [NullPolicy::Equal, NullPolicy::Unmatchable] {
        let spec = KeySpec::new(std::slice::from_ref(&ty), &[nulls], owner.as_ref(), &|| {
            Ok(())
        })
        .unwrap();
        let policy = LocalHashPolicy::new(&spec, owner.as_ref()).unwrap();
        let input = keys(
            &spec,
            &[Arc::new(StringArray::from(vec![Some("new"), None]))],
            std::slice::from_ref(&ty),
            &[1, 0, 1],
            owner.as_ref(),
        );
        let view =
            IndexView::empty(spec, policy, IndexPurpose::KeyIds, owner.as_ref(), &Control).unwrap();
        let prepared = prepared(&view, &input, &tracker);
        let probe = view
            .probe(&prepared, None, owner.as_ref(), &Control)
            .unwrap();
        let null_match = if nulls == NullPolicy::Equal {
            BulkMatch::Miss
        } else {
            BulkMatch::Unmatchable
        };
        assert_eq!(probe.matches(), &[null_match, BulkMatch::Miss, null_match]);
        let present = view
            .contains(&prepared, None, owner.as_ref(), &Control)
            .unwrap();
        assert_eq!(present.present(), &[0; 3]);
        assert_eq!(
            present.participates(),
            if nulls == NullPolicy::Equal {
                &[1, 1, 1]
            } else {
                &[0, 1, 0]
            }
        );
        let output = view
            .export_keys(
                &[],
                std::slice::from_ref(&ty),
                owner.as_ref(),
                expression_allocation_host(tracker.clone()),
                &Control,
            )
            .unwrap();
        assert_eq!(output.arrays()[0].len(), 0);
    }
    for backend in [0, 1, 2] {
        let direct = backend == 1;
        let view = if backend == 2 {
            let spec = KeySpec::new(
                std::slice::from_ref(&ty),
                &[NullPolicy::Unmatchable],
                owner.as_ref(),
                &|| Ok(()),
            )
            .unwrap();
            let policy = LocalHashPolicy::new(&spec, owner.as_ref()).unwrap();
            IndexView::empty(
                spec,
                policy,
                IndexPurpose::PresenceOnly,
                owner.as_ref(),
                &Control,
            )
            .unwrap()
        } else if direct {
            let i = FunctionValueType::new(DataType::Int32, false);
            IndexView::direct(
                build_data(
                    Arc::new(Int32Array::from(vec![1, 2])),
                    &i,
                    &i,
                    Some((1, 2)),
                    IndexPurpose::PresenceOnly,
                    2,
                    &ports(owner.as_ref(), &tracker),
                ),
                owner.as_ref(),
                &Control,
            )
            .unwrap()
        } else {
            hash_view(
                &[Arc::new(StringArray::from(vec![Some("a"), None]))],
                std::slice::from_ref(&ty),
                &[0, 1],
                &[NullPolicy::Unmatchable],
                IndexPurpose::PresenceOnly,
                owner.as_ref(),
                &tracker,
            )
            .0
        };
        assert_invariant(
            &error(view.read_keys(&[], owner.as_ref(), &Control)),
            ValueViolation::Layout,
        );
        assert_invariant(
            &error(view.export_keys(
                &[],
                &[],
                owner.as_ref(),
                expression_allocation_host(tracker.clone()),
                &Control,
            )),
            ValueViolation::Layout,
        );
        let arrays: [ArrayRef; 1] = if direct {
            [Arc::new(Int32Array::from(Vec::<i32>::new()))]
        } else {
            [Arc::new(StringArray::from(Vec::<&str>::new()))]
        };
        let bindings = if direct {
            FunctionValueType::new(DataType::Int32, false)
        } else {
            ty.clone()
        };
        let input = keys(view.spec(), &arrays, &[bindings], &[], owner.as_ref());
        let query_array: ArrayRef = if direct {
            Arc::new(Int32Array::from(vec![Some(1), Some(3), None]))
        } else {
            Arc::new(StringArray::from(vec![Some("a"), Some("new"), None]))
        };
        let query_type = FunctionValueType::new(
            if direct {
                DataType::Int32
            } else {
                DataType::Utf8
            },
            true,
        );
        let query = keys(
            view.spec(),
            &[query_array],
            &[query_type],
            &[0, 1, 2],
            owner.as_ref(),
        );
        let present = view
            .contains(
                &prepared(&view, &query, &tracker),
                None,
                owner.as_ref(),
                &Control,
            )
            .unwrap();
        assert_eq!(
            present.present(),
            if backend == 2 { &[0, 0, 0] } else { &[1, 0, 0] }
        );
        assert_eq!(
            present.participates(),
            if direct { &[1, 1, 1] } else { &[1, 1, 0] }
        );
        assert_invariant(
            &error(view.probe(
                &prepared(&view, &input, &tracker),
                None,
                owner.as_ref(),
                &Control,
            )),
            ValueViolation::Layout,
        );
    }
}

struct RefusingOwner<'a>(&'a dyn InputConstructionOwner, ExecutionFailure);
impl InputConstructionOwner for RefusingOwner<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.0.storage_lifetime()
    }
    fn request(&self, _: Layout) -> Result<(), ConstructionFailure> {
        Err(self.1.clone().into())
    }
    fn observe_opaque_upper_bound(&self, n: usize) -> Result<(), ConstructionFailure> {
        self.0.observe_opaque_upper_bound(n)
    }
    fn observe(
        &self,
        occurrence: usize,
        path: &[crate::exec::chunk::ArrayPathStep],
        step: crate::exec::chunk::ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure> {
        self.0.observe(occurrence, path, step)
    }
}
struct Stop;
impl KernelEvaluationControl for Stop {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Err(KernelFailure::DeadlineExceeded)
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("read never waits")
    }
}
#[test]
fn readonly_refusal_foreign_hash_invalid_ranges_and_output_failure_do_not_poison_the_same_view() {
    let (owner, tracker) = fixture();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let arrays: [ArrayRef; 1] = [Arc::new(StringArray::from(vec![
        Some("long original value"),
        None,
        Some("other value"),
    ]))];
    let (view, input, hashes) = hash_view(
        &arrays,
        std::slice::from_ref(&ty),
        &[2, 0, 1, 2],
        &[NullPolicy::Equal],
        IndexPurpose::KeyIds,
        owner.as_ref(),
        &tracker,
    );
    let prepared = prepared(&view, &input, &tracker);
    let expected = view
        .probe(&prepared, Some(&hashes), owner.as_ref(), &Control)
        .unwrap();
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let refusing = RefusingOwner(owner.as_ref(), cause.clone());
    assert_eq!(
        error(view.probe(&prepared, Some(&hashes), &refusing, &Control)),
        cause
    );
    assert!(
        view.contains(&prepared, Some(&hashes), owner.as_ref(), &Stop)
            .is_err()
    );
    assert_invariant(
        &error(view.probe_range(
            &prepared,
            Some(&hashes),
            std::ops::Range { start: 3, end: 2 },
            owner.as_ref(),
            &Control,
        )),
        ValueViolation::Bounds,
    );
    let Backend::Hash { policy, .. } = &view.backend else {
        panic!("hash fixture")
    };
    let other = keys(
        view.spec(),
        &arrays,
        std::slice::from_ref(&ty),
        &[0, 1, 2],
        owner.as_ref(),
    );
    let other_prepared = PreparedKeyBatch::from_captured(
        view.spec(),
        &other,
        AggregateAllocator::new(tracker.clone()),
        &|| Ok(()),
    )
    .unwrap();
    let foreign = hash_batch(
        &other_prepared,
        policy,
        &mut HashScratch::default(),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    assert_invariant(
        &error(view.probe(&prepared, Some(&foreign), owner.as_ref(), &Control)),
        ValueViolation::Layout,
    );
    assert_invariant(
        &error(view.read_keys(
            &[BulkKey {
                partition: usize::MAX,
                id: KeyId(0),
            }],
            owner.as_ref(),
            &Control,
        )),
        ValueViolation::Bounds,
    );
    let key = found(expected.matches()[2]);
    assert_invariant(
        &error(view.export_keys(
            &[key],
            &[FunctionValueType::new(DataType::Utf8, false)],
            owner.as_ref(),
            expression_allocation_host(tracker.clone()),
            &Control,
        )),
        ValueViolation::RequiredNull,
    );
    assert_eq!(
        view.probe(&prepared, Some(&hashes), owner.as_ref(), &Control)
            .unwrap()
            .matches(),
        expected.matches()
    );
}

#[test]
fn escaped_source_read_and_new_export_buffers_keep_distinct_real_creators_after_view_exit() {
    let (source_owner, source_tracker) = fixture();
    let ty = FunctionValueType::new(DataType::Int32, false);
    let store = {
        let work = ports(source_owner.as_ref(), &source_tracker);
        build_data(
            Arc::new(Int32Array::from(vec![9, 4, 9])),
            &ty,
            &ty,
            Some((4, 9)),
            IndexPurpose::KeyIds,
            2,
            &work,
        )
    };
    let (read_owner, _read_tracker) = fixture();
    let view = IndexView::direct(store, read_owner.as_ref(), &Control).unwrap();
    let requested = [
        BulkKey {
            partition: 0,
            id: KeyId(1),
        },
        BulkKey {
            partition: 0,
            id: KeyId(0),
        },
    ];
    let loan = view
        .read_keys(&requested, read_owner.as_ref(), &Control)
        .unwrap();
    let super::super::read::KeyReadValues::Fixed { array, .. } =
        loan.columns()[0].pages()[0].values()
    else {
        panic!("original fixed page")
    };
    let original_buffer = array
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .inner()
        .clone();
    drop(loan);
    let (export_owner, export_tracker) = fixture();
    let output = view
        .export_keys(
            &[requested[0], requested[1], requested[0]],
            std::slice::from_ref(&ty),
            export_owner.as_ref(),
            expression_allocation_host(export_tracker.clone()),
            &Control,
        )
        .unwrap();
    let values = output.arrays()[0]
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values, &Int32Array::from(vec![9, 4, 9]));
    let exported_buffer = values.values().inner().clone();
    drop(output);
    drop(export_owner);
    drop(view);
    drop(read_owner);
    drop(source_owner);
    assert!(source_tracker.current() > 0);
    assert!(export_tracker.current() > 0);
    drop(original_buffer);
    assert_eq!(source_tracker.current(), 0);
    assert!(export_tracker.current() > 0);
    drop(exported_buffer);
    assert_eq!(export_tracker.current(), 0);
}
