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

//! Direct representative data tests. These drive real low-level phases;
//! they do not construct or claim the parent Graph's immutable Ready state.
use super::*;
use crate::runtime::{
    mem_tracker::MemTracker, tracked_state_allocator::expression_allocation_host,
};
use arrow::array::{Int8Array, Int16Array, Int32Array, Int64Array};
use novarocks_failure_contract::FailureProjection;
use novarocks_functions::KernelFailure;
use novarocks_value_arrays::{
    ValueError, decode::ValueConversionContext, reachability::ReachableRows,
};
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
#[test]
fn all_signed_widths_preserve_requested_order_duplicates_exact_type_and_separate_last_null_class() {
    let (owner, tracker) = fixture();
    let work = ports(owner.as_ref(), &tracker);
    macro_rules! case {
        ($dtype:expr,$array:ty,$min:expr,$max:expr) => {{
            let spec_ty = FunctionValueType::new($dtype, false);
            let actual = FunctionValueType::new($dtype, true);
            let array: ArrayRef = Arc::new(<$array>::from(vec![
                Some($min + 3),
                None,
                Some($max),
                Some($min),
                Some($min + 1),
                Some($min + 3),
            ]));
            let store = build_data(
                array,
                &spec_ty,
                &actual,
                Some((i64::from($min), i64::from($max))),
                IndexPurpose::KeyIds,
                9,
                &work,
            );
            assert_eq!(store.id_count_after_fill().unwrap(), 5);
            let ids = [
                KeyId(3),
                KeyId(0),
                KeyId(4),
                KeyId(1),
                KeyId(3),
                KeyId(2),
                KeyId(0),
            ];
            let read = store.read_keys(&ids, owner.as_ref(), &Control).unwrap();
            assert_eq!(read.len(), ids.len());
            let mut seen = vec![false; ids.len()];
            for page in read.columns()[0].pages() {
                let super::super::read::KeyReadValues::Fixed { array, binding } = page.values()
                else {
                    panic!("direct publishes original typed fixed pages")
                };
                assert_eq!(array.data_type(), spec_ty.data_type());
                assert!(binding.nullable());
                for row in page.rows() {
                    assert!(!seen[row.ordinal]);
                    seen[row.ordinal] = true;
                    assert!(row.row < array.len());
                }
            }
            assert!(seen.into_iter().all(|seen| seen));
            drop(read);
            let output = store
                .export_keys(
                    &ids,
                    std::slice::from_ref(&actual),
                    owner.as_ref(),
                    expression_allocation_host(tracker.clone()),
                    &Control,
                )
                .unwrap();
            let expected = <$array>::from(vec![
                Some($max),
                Some($min),
                None,
                Some($min + 1),
                Some($max),
                Some($min + 3),
                Some($min),
            ]);
            assert_eq!(
                output.arrays()[0]
                    .as_any()
                    .downcast_ref::<$array>()
                    .unwrap(),
                &expected
            );
            assert_eq!(output.bindings()[0].data_type(), actual.data_type());
            assert!(output.bindings()[0].nullable());
            // Refusal is readonly. Required output can still copy a selected
            // non-NULL subset of the same nullable representative declaration.
            assert_invariant(
                &error(store.export_keys(
                    &[KeyId(4)],
                    std::slice::from_ref(&spec_ty),
                    owner.as_ref(),
                    expression_allocation_host(tracker.clone()),
                    &Control,
                )),
                ValueViolation::RequiredNull,
            );
            store.check_failure().unwrap();
            let output = store
                .export_keys(
                    &[KeyId(3), KeyId(0), KeyId(3)],
                    std::slice::from_ref(&spec_ty),
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
                &<$array>::from(vec![$max, $min, $max])
            );
            let empty = store
                .export_keys(
                    &[],
                    std::slice::from_ref(&spec_ty),
                    owner.as_ref(),
                    expression_allocation_host(tracker.clone()),
                    &Control,
                )
                .unwrap();
            assert_eq!(empty.arrays()[0].data_type(), spec_ty.data_type());
            assert_eq!(empty.arrays()[0].len(), 0);
        }};
    }
    case!(DataType::Int8, Int8Array, -100, 100);
    case!(DataType::Int16, Int16Array, -2000, 2000);
    case!(DataType::Int32, Int32Array, -20_000, 20_000);
    case!(DataType::Int64, Int64Array, 1i64 << 40, (1i64 << 40) + 63);
}
#[test]
fn sparse_and_empty_shards_and_multiple_representative_pages_locate_without_exporting_private_page_size()
 {
    let (owner, tracker) = fixture();
    let work = ports(owner.as_ref(), &tracker);
    let ty = FunctionValueType::new(DataType::Int32, false);
    let array: ArrayRef = Arc::new(Int32Array::from((-2048..=2048).collect::<Vec<_>>()));
    let store = build_data(
        array,
        &ty,
        &ty,
        Some((-2048, 2048)),
        IndexPurpose::KeyIds,
        3,
        &work,
    );
    let count = store.id_count_after_fill().unwrap();
    assert_eq!(count, 4097);
    let ids = [
        KeyId(count - 1),
        KeyId(0),
        KeyId(count / 3),
        KeyId(count / 2),
        KeyId(count - 1),
    ];
    let output = store
        .export_keys(
            &ids,
            std::slice::from_ref(&ty),
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
        &Int32Array::from(vec![2048, -2048, -683, 0, 2048])
    );
    assert_invariant(
        &error(store.read_keys(&[KeyId(count)], owner.as_ref(), &Control)),
        ValueViolation::Bounds,
    );
    store.check_failure().unwrap();
    let incompatible = FunctionValueType::new(DataType::Int64, false);
    assert_invariant(
        &error(store.export_keys(
            &[],
            &[incompatible],
            owner.as_ref(),
            expression_allocation_host(tracker.clone()),
            &Control,
        )),
        ValueViolation::Layout,
    );
    store.check_failure().unwrap();
    // A separate sparse domain creates empty internal shards. Values at the
    // ends and duplicate/out-of-order ids remain exact without a page-size oracle.
    let array: ArrayRef = Arc::new(Int32Array::from(vec![-2000, 2000]));
    let sparse = build_data(
        array,
        &ty,
        &ty,
        Some((-2000, 2000)),
        IndexPurpose::KeyIds,
        16,
        &work,
    );
    let output = sparse
        .export_keys(
            &[KeyId(1), KeyId(0), KeyId(1)],
            std::slice::from_ref(&ty),
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
        &Int32Array::from(vec![2000, -2000, 2000])
    );
}
#[test]
fn presence_only_and_unfinished_direct_data_cannot_expose_ids_even_for_empty_requests() {
    let (owner, tracker) = fixture();
    let work = ports(owner.as_ref(), &tracker);
    let ty = FunctionValueType::new(DataType::Int32, false);
    let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let presence = build_data(
        array,
        &ty,
        &ty,
        Some((1, 2)),
        IndexPurpose::PresenceOnly,
        2,
        &work,
    );
    assert_invariant(
        &error(presence.read_keys(&[], owner.as_ref(), &Control)),
        ValueViolation::Layout,
    );
    assert_invariant(
        &error(presence.export_keys(
            &[],
            std::slice::from_ref(&ty),
            owner.as_ref(),
            expression_allocation_host(tracker.clone()),
            &Control,
        )),
        ValueViolation::Layout,
    );
    presence.check_failure().unwrap();
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let geometry = DirectGeometry::for_selected(
        &spec,
        IntegerDomain {
            min_max: Some((1, 2)),
        },
        IndexPurpose::KeyIds,
        NonZeroUsize::new(2).unwrap(),
    )
    .unwrap();
    let unfinished = Storage::new(geometry, &spec, Some(false), &work).unwrap();
    assert_invariant(
        &error(unfinished.read_keys(&[], owner.as_ref(), &Control)),
        ValueViolation::RowTransaction,
    );
    unfinished.check_failure().unwrap();
}
#[test]
fn escaped_original_read_buffer_and_new_export_buffer_retain_their_actual_distinct_creators() {
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
    let read = store
        .read_keys(&[KeyId(1), KeyId(0)], read_owner.as_ref(), &Control)
        .unwrap();
    let super::super::read::KeyReadValues::Fixed { array, .. } =
        read.columns()[0].pages()[0].values()
    else {
        panic!("fixed direct representative")
    };
    let original_buffer = array
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .inner()
        .clone();
    drop(read);
    drop(read_owner);
    let (export_owner, export_tracker) = fixture();
    let output = store
        .export_keys(
            &[KeyId(1), KeyId(0), KeyId(1)],
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
    drop(store);
    drop(source_owner);
    assert!(source_tracker.current() > 0);
    assert!(export_tracker.current() > 0);
    drop(original_buffer);
    assert_eq!(source_tracker.current(), 0);
    assert!(export_tracker.current() > 0);
    drop(exported_buffer);
    assert_eq!(export_tracker.current(), 0);
}
