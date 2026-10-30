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

//! Capture receipts preserve original values and instance identity across
//! repeated loans; no test hook or private representation is required.

use super::*;
use arrow::datatypes::Field;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_functions::KernelFailure;
use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
use std::sync::atomic::{AtomicUsize, Ordering};

fn failure<T>(result: ExecutionResult<T>) -> ExecutionFailure {
    match result {
        Err(cause) => cause,
        Ok(_) => panic!("capture refusal must remain visible"),
    }
}
fn capture(
    spec: &KeySpec,
    arrays: &[ArrayRef],
    types: &[FunctionValueType],
    rows: &[usize],
    owner: &dyn InputConstructionOwner,
) -> KeyInput {
    KeyInput::new_for_spec(
        spec,
        arrays,
        types,
        &readable(arrays[0].len()),
        rows,
        &mut ValueConversionContext::default(),
        owner,
        &|| Ok(()),
    )
    .unwrap()
}
fn captured_hash(
    spec: &KeySpec,
    input: &KeyInput,
    policy: &LocalHashPolicy,
    tracker: &Arc<MemTracker>,
    owner: &dyn InputConstructionOwner,
) -> LocalHashBatch {
    let loan = PreparedKeyBatch::from_captured(
        spec,
        input,
        AggregateAllocator::new(tracker.clone()),
        &|| Ok(()),
    )
    .unwrap();
    hash_batch(&loan, policy, &mut HashScratch::new(), owner, &|| Ok(())).unwrap()
}
fn mixed(large: bool) -> (Vec<ArrayRef>, Vec<FunctionValueType>) {
    let bytes = [
        Some(b"\xff".as_slice()),
        None,
        Some(b"".as_slice()),
        Some(b"\xff".as_slice()),
        None,
    ];
    let children: ArrayRef = if large {
        Arc::new(LargeBinaryArray::from(bytes.to_vec()))
    } else {
        Arc::new(BinaryArray::from(bytes.to_vec()))
    };
    let field = Arc::new(Field::new("item", children.data_type().clone(), true));
    let list: ArrayRef = Arc::new(
        ListArray::try_new(
            field,
            OffsetBuffer::new(vec![0, 2, 2, 3, 5].into()),
            children,
            Some(NullBuffer::from(vec![true, false, true, true])),
        )
        .unwrap(),
    );
    let plain: ArrayRef = if large {
        Arc::new(StringArray::from(vec![
            Some("alpha external value"),
            None,
            Some("beta external value"),
            Some("alpha external value"),
        ]))
    } else {
        Arc::new(StringViewArray::from(vec![
            Some("alpha external value"),
            None,
            Some("beta external value"),
            Some("alpha external value"),
        ]))
    };
    let types = vec![
        FunctionValueType::new(plain.data_type().clone(), true),
        FunctionValueType::new(list.data_type().clone(), true),
    ];
    (vec![plain, list], types)
}
fn projected_types(nullable: bool) -> [FunctionValueType; 2] {
    [
        FunctionValueType::new(DataType::Utf8, nullable),
        FunctionValueType::new(
            DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, true))),
            nullable,
        ),
    ]
}

#[test]
fn captured_occurrences_cached_hashes_and_selected_index_keep_values_order_and_nulls() {
    let (owner, tracker) = owner_with_tracker();
    let types = [
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::new(DataType::Utf8View, true),
    ];
    let spec = KeySpec::new(
        &types,
        &[NullPolicy::Equal, NullPolicy::Unmatchable],
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
    let input = capture(
        &spec,
        &[
            Arc::new(Int32Array::from(vec![7, 8, 7, 9])),
            Arc::new(StringViewArray::from(vec![
                Some("a"),
                None,
                Some("a"),
                Some("b"),
            ])),
        ],
        &types,
        &[2, 0, 3, 2, 1],
        owner.as_ref(),
    );
    assert_eq!(
        (0..input.len())
            .map(|i| input.row(i).unwrap())
            .collect::<Vec<_>>(),
        vec![2, 0, 3, 2, 1]
    );
    let hashes = captured_hash(&spec, &input, index.policy(), &tracker, owner.as_ref());
    assert_eq!(hashes.hash(0).unwrap(), hashes.hash(1).unwrap());
    assert_eq!(hashes.hash(0).unwrap(), hashes.hash(3).unwrap());
    assert!(!hashes.participates(4).unwrap());
    let hashes = hashes.force_hash(0);
    let loan = PreparedKeyBatch::from_captured(
        &spec,
        &input,
        AggregateAllocator::new(tracker.clone()),
        &|| Ok(()),
    )
    .unwrap();
    let before = super::super::local_hash::BYTE_HASH_INITS.with(Cell::get);
    let output = index
        .intern_selected(
            &loan,
            &hashes,
            &[3, 1, 3, 4, 2],
            owner.as_ref(),
            &CopyControl,
        )
        .unwrap();
    assert_eq!(
        output.matches(),
        &[
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Unmatchable,
            KeyMatch::Found(KeyId(1))
        ]
    );
    assert_eq!(
        output.new_keys(),
        &[
            NewKey {
                occurrence: 3,
                row: 2,
                id: KeyId(0)
            },
            NewKey {
                occurrence: 2,
                row: 3,
                id: KeyId(1)
            },
        ]
    );
    assert_eq!(
        index
            .probe_selected(&loan, &hashes, &[2, 0, 4, 1], owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        &[
            KeyMatch::Found(KeyId(1)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Unmatchable,
            KeyMatch::Found(KeyId(0))
        ]
    );
    assert_eq!(
        super::super::local_hash::BYTE_HASH_INITS.with(Cell::get),
        before
    );
    let exported = index
        .export_keys(
            &[KeyId(1), KeyId(0), KeyId(1)],
            &types,
            owner.as_ref(),
            &CopyControl,
        )
        .unwrap();
    assert_eq!(
        exported.arrays()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &Int32Array::from(vec![9, 7, 9])
    );
    assert_eq!(
        exported.arrays()[1]
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("b"), Some("a"), Some("b")]
    );
}

#[test]
fn captured_mixed_layout_projection_preserves_source_nulls_and_is_shared_by_4096_loans() {
    let (owner, tracker) = owner_with_tracker();
    let types = projected_types(false);
    let spec = KeySpec::new(&types, &[NullPolicy::Equal; 2], owner.as_ref(), &|| Ok(())).unwrap();
    let (arrays, actual) = mixed(false);
    let count = CountOwner {
        original: owner.as_ref(),
        calls: AtomicUsize::new(0),
        stop: usize::MAX,
        cause: ExecutionFailure::from(KernelFailure::ResourceExhausted),
    };
    let input = capture(&spec, &arrays, &actual, &[3, 1, 0, 2, 3], &count);
    let loan = PreparedKeyBatch::from_captured(
        &spec,
        &input,
        AggregateAllocator::new(tracker.clone()),
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(loan.component_binding(0).unwrap(), &actual[0]);
    assert_eq!(
        loan.component_binding(1).unwrap().data_type(),
        types[1].data_type()
    );
    assert!(loan.component_binding(1).unwrap().nullable());
    let normalized = loan.tuples().physical_component(1).unwrap().0.clone();
    assert_eq!(normalized.data_type(), types[1].data_type());
    let requested = count.calls.load(Ordering::SeqCst);
    let checkpoints = Cell::new(0);
    for _ in 0..4_096 {
        let loan = PreparedKeyBatch::from_captured(
            &spec,
            &input,
            AggregateAllocator::new(tracker.clone()),
            &|| {
                checkpoints.set(checkpoints.get() + 1);
                Ok(())
            },
        )
        .unwrap();
        assert!(Arc::ptr_eq(
            &normalized,
            loan.tuples().physical_component(1).unwrap().0
        ));
        assert_eq!(loan.bytes(0).unwrap().unwrap().len(), 4);
    }
    assert_eq!(checkpoints.get(), 4_096);
    assert_eq!(count.calls.load(Ordering::SeqCst), requested);

    let (other_arrays, other_types) = mixed(true);
    let other = capture(
        &spec,
        &other_arrays,
        &other_types,
        &[0, 1, 2, 3],
        owner.as_ref(),
    );
    let other_loan = PreparedKeyBatch::from_captured(
        &spec,
        &other,
        AggregateAllocator::new(tracker.clone()),
        &|| Ok(()),
    )
    .unwrap();
    let mut equal = [0; 6];
    super::super::pairs::compare_prepared_pairs(
        &loan,
        &[0, 1, 2, 3, 0, 2],
        &other_loan,
        &[3, 1, 2, 0, 2, 0],
        &mut equal,
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(equal, [1, 1, 1, 1, 0, 0]);
    let mut index = KeyIndex::new(
        spec.clone(),
        IndexPurpose::KeyIds,
        tracker.clone(),
        owner.as_ref(),
    )
    .unwrap();
    let hashes = captured_hash(&spec, &input, index.policy(), &tracker, owner.as_ref());
    let other_hashes = captured_hash(&spec, &other, index.policy(), &tracker, owner.as_ref());
    for (occurrence, other_occurrence) in [(0, 3), (1, 1), (2, 0), (3, 2)] {
        assert_eq!(
            hashes.hash(occurrence).unwrap(),
            other_hashes.hash(other_occurrence).unwrap()
        );
    }
    assert_eq!(
        index
            .intern_batch(&loan, &hashes, owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        &[
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(1)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(2)),
            KeyMatch::Found(KeyId(0))
        ]
    );
    let targets = projected_types(true);
    let exported = index
        .export_keys(
            &[KeyId(2), KeyId(1), KeyId(0), KeyId(2)],
            &targets,
            owner.as_ref(),
            &CopyControl,
        )
        .unwrap();
    assert_eq!(exported.bindings(), &targets);
    assert_eq!(
        exported.arrays()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some("beta external value"),
            None,
            Some("alpha external value"),
            Some("beta external value")
        ]
    );
    let lists = exported.arrays()[1]
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(lists.data_type(), targets[1].data_type());
    assert_eq!(lists.value_offsets(), &[0, 1, 1, 3, 4]);
    assert_eq!(
        (0..4).map(|row| lists.is_null(row)).collect::<Vec<_>>(),
        vec![false, true, false, false]
    );
    assert_eq!(
        lists
            .values()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some(b"".as_slice()),
            Some(b"\xff".as_slice()),
            None,
            Some(b"".as_slice())
        ]
    );
    for array in exported.arrays() {
        array.to_data().validate_full().unwrap();
    }
}

#[test]
fn captured_loan_rejects_missing_or_foreign_receipts_and_new_subset_cannot_expand_domain() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
        Some("selected"),
        Some("not captured"),
    ]))];
    let plain = input(&arrays, std::slice::from_ref(&ty), &[0], owner.as_ref());
    let input = capture(
        &spec,
        &arrays,
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
    );
    let foreign = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    for (requested, page) in [(&spec, &plain), (&foreign, &input)] {
        let calls = Cell::new(0);
        assert_value_failure(
            &failure(PreparedKeyBatch::from_captured(
                requested,
                page,
                AggregateAllocator::new(tracker.clone()),
                &|| {
                    calls.set(calls.get() + 1);
                    Ok(())
                },
            )),
            ValueViolation::Layout,
        );
        assert_eq!(calls.get(), 1);
    }
    let loan = PreparedKeyBatch::from_captured(
        &spec,
        &input,
        AggregateAllocator::new(tracker),
        &|| Ok(()),
    )
    .unwrap();
    let mut expanded = SelectionSet::try_new(2).unwrap();
    expanded.insert(1).unwrap();
    assert_value_failure(
        &failure(loan.byte_subset(0, &expanded, &|| Ok(()))),
        ValueViolation::UndefinedRow,
    );
    expanded = SelectionSet::try_new(2).unwrap();
    expanded.insert(0).unwrap();
    assert_eq!(loan.byte_subset(0, &expanded, &|| Ok(())).unwrap().len(), 1);
    assert_eq!(loan.bytes(0).unwrap().unwrap().len(), 1);
}

struct Tag(Arc<AtomicUsize>);
impl ValueTypeStorageHold for Tag {}
impl Drop for Tag {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn tagged<'a>(
    owner: &'a dyn InputConstructionOwner,
    drops: &Arc<AtomicUsize>,
) -> TaggedCreator<'a> {
    TaggedCreator {
        original: owner,
        storage: owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &ValueTypeStorageLifetime::held(Arc::new(Tag(drops.clone()))),
                |layout| owner.request(layout),
            )
            .unwrap(),
    }
}

#[test]
fn captured_input_and_saved_hash_alias_keep_actual_source_and_preparation_creators_after_move() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8View, false);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut source_index = KeyIndex::new(
        spec.clone(),
        IndexPurpose::KeyIds,
        tracker.clone(),
        owner.as_ref(),
    )
    .unwrap();
    let source = input(
        &[Arc::new(StringViewArray::from(vec![
            "source backing kept after caller exit",
        ]))],
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
    );
    let source_hashes = index_hash(&source, &source_index, &tracker, owner.as_ref());
    let source_loan = PreparedKeyBatch::new(
        &spec,
        &source,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    source_index
        .intern_batch(&source_loan, &source_hashes, owner.as_ref(), &CopyControl)
        .unwrap();
    let source_drops = Arc::new(AtomicUsize::new(0));
    let preparation_drops = Arc::new(AtomicUsize::new(0));
    let source_owner = tagged(owner.as_ref(), &source_drops);
    let exported = source_index
        .export_keys(
            &[KeyId(0)],
            std::slice::from_ref(&ty),
            &source_owner,
            &CopyControl,
        )
        .unwrap();
    let preparation_owner = tagged(owner.as_ref(), &preparation_drops);
    let mut context = ValueConversionContext::default();
    let captured = KeyInput::new_for_spec(
        &spec,
        exported.arrays(),
        exported.bindings(),
        &readable(1),
        &[0],
        &mut context,
        &preparation_owner,
        &|| Ok(()),
    )
    .unwrap();
    context.clear();
    let captured_hashes = captured_hash(
        &spec,
        &captured,
        source_index.policy(),
        &tracker,
        owner.as_ref(),
    );
    drop(context);
    drop(exported);
    drop(source_owner);
    drop(preparation_owner);
    drop(source_loan);
    drop(source_hashes);
    drop(source);
    drop(source_index);
    drop(owner);
    assert_eq!(source_drops.load(Ordering::SeqCst), 0);
    assert_eq!(preparation_drops.load(Ordering::SeqCst), 0);
    let (captured, hashes) = std::thread::spawn(move || {
        let loan = PreparedKeyBatch::from_captured(
            &spec,
            &captured,
            AggregateAllocator::new(tracker),
            &|| Ok(()),
        )
        .unwrap();
        assert_eq!(loan.len(), 1);
        assert_eq!(loan.bytes(0).unwrap().unwrap().len(), 1);
        drop(loan);
        (captured, captured_hashes)
    })
    .join()
    .unwrap();
    drop(captured);
    assert_eq!(source_drops.load(Ordering::SeqCst), 0);
    assert_eq!(preparation_drops.load(Ordering::SeqCst), 0);
    assert!(hashes.participates(0).unwrap());
    drop(hashes);
    assert_eq!(source_drops.load(Ordering::SeqCst), 1);
    assert_eq!(preparation_drops.load(Ordering::SeqCst), 1);
}

struct CountOwner<'a> {
    original: &'a dyn InputConstructionOwner,
    calls: AtomicUsize,
    stop: usize,
    cause: ExecutionFailure,
}
impl InputConstructionOwner for CountOwner<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.original.storage_lifetime()
    }
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == self.stop {
            Err(ConstructionFailure::from(self.cause.clone()))
        } else {
            self.original.request(layout)
        }
    }
    fn observe_opaque_upper_bound(&self, n: usize) -> Result<(), ConstructionFailure> {
        self.original.observe_opaque_upper_bound(n)
    }
    fn observe(
        &self,
        n: usize,
        path: &[crate::exec::chunk::ArrayPathStep],
        step: crate::exec::chunk::ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure> {
        self.original.observe(n, path, step)
    }
}

#[test]
fn captured_input_actual_growth_refusals_preserve_the_original_cause_without_publishing() {
    let (owner, _) = owner_with_tracker();
    let types = projected_types(false);
    let spec = KeySpec::new(&types, &[NullPolicy::Equal; 2], owner.as_ref(), &|| Ok(())).unwrap();
    let (arrays, actual) = mixed(false);
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let baseline = CountOwner {
        original: owner.as_ref(),
        calls: AtomicUsize::new(0),
        stop: usize::MAX,
        cause: cause.clone(),
    };
    drop(capture(&spec, &arrays, &actual, &[3, 0, 1, 2], &baseline));
    let requests = baseline.calls.load(Ordering::SeqCst);
    assert!(requests > 3);
    for stop in [1, requests / 2, requests] {
        let refusing = CountOwner {
            original: owner.as_ref(),
            calls: AtomicUsize::new(0),
            stop,
            cause: cause.clone(),
        };
        let result = KeyInput::new_for_spec(
            &spec,
            &arrays,
            &actual,
            &readable(4),
            &[3, 0, 1, 2],
            &mut ValueConversionContext::default(),
            &refusing,
            &|| Ok(()),
        );
        assert_eq!(failure(result), cause);
        assert_eq!(refusing.calls.load(Ordering::SeqCst), stop);
    }
}

#[test]
fn captured_input_mid_capture_and_loan_entry_cancellation_preserve_original_occurrence() {
    let (owner, tracker) = owner_with_tracker();
    let types = projected_types(false);
    let spec = KeySpec::new(&types, &[NullPolicy::Equal; 2], owner.as_ref(), &|| Ok(())).unwrap();
    let (arrays, actual) = mixed(false);
    let calls = Cell::new(0);
    let captured = KeyInput::new_for_spec(
        &spec,
        &arrays,
        &actual,
        &readable(4),
        &[3, 0, 1, 2],
        &mut ValueConversionContext::default(),
        owner.as_ref(),
        &|| {
            calls.set(calls.get() + 1);
            Ok(())
        },
    )
    .unwrap();
    assert!(calls.get() > 3);
    let cause = ExecutionFailure::from(KernelFailure::Cancelled);
    let stopped = Cell::new(0);
    let stop = calls.get() / 2;
    let result = KeyInput::new_for_spec(
        &spec,
        &arrays,
        &actual,
        &readable(4),
        &[3, 0, 1, 2],
        &mut ValueConversionContext::default(),
        owner.as_ref(),
        &|| {
            stopped.set(stopped.get() + 1);
            if stopped.get() == stop {
                Err(cause.clone())
            } else {
                Ok(())
            }
        },
    );
    assert!(failure(result).same_occurrence(&cause));
    assert_eq!(stopped.get(), stop);
    assert!(
        failure(PreparedKeyBatch::from_captured(
            &spec,
            &captured,
            AggregateAllocator::new(tracker.clone()),
            &|| Err(cause.clone())
        ))
        .same_occurrence(&cause)
    );
    assert_eq!(
        PreparedKeyBatch::from_captured(
            &spec,
            &captured,
            AggregateAllocator::new(tracker),
            &|| Ok(())
        )
        .unwrap()
        .len(),
        4
    );
}
