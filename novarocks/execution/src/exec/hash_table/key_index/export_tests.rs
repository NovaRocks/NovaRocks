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

//! Independent typed-output expectations. Requested IDs are deliberately
//! interleaved across representative batches and include repeated occurrences.

use super::*;
use arrow::datatypes::{Field, Fields};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use novarocks_functions::{KernelEvaluationControl, KernelFailure};
use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
use std::sync::atomic::{AtomicUsize, Ordering};

fn intern(
    index: &mut KeyIndex,
    arrays: &[ArrayRef],
    types: &[FunctionValueType],
    rows: &[usize],
    owner: &dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
) {
    let keys = input(arrays, types, rows, owner);
    let hashes = index_hash(&keys, index, tracker, owner);
    let prepared = PreparedKeyBatch::new(
        index.spec(),
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner,
        &|| Ok(()),
    )
    .unwrap();
    index
        .intern_batch(&prepared, &hashes, owner, &CopyControl)
        .unwrap();
}
fn index(
    types: &[FunctionValueType],
    owner: &dyn InputConstructionOwner,
    tracker: &Arc<MemTracker>,
) -> KeyIndex {
    let spec = KeySpec::new(types, &vec![NullPolicy::Equal; types.len()], owner, &|| {
        Ok(())
    })
    .unwrap();
    KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner).unwrap()
}
fn strings(array: &ArrayRef) -> Vec<Option<&str>> {
    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .iter()
            .collect(),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .iter()
            .collect(),
        _ => panic!("expected exact STRING output"),
    }
}
fn binary(array: &ArrayRef) -> Vec<Option<&[u8]>> {
    match array.data_type() {
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .iter()
            .collect(),
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .iter()
            .collect(),
        DataType::BinaryView => array
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap()
            .iter()
            .collect(),
        _ => panic!("expected exact BINARY output"),
    }
}
fn failure<T>(result: ExecutionResult<T>) -> ExecutionFailure {
    match result {
        Err(cause) => cause,
        Ok(_) => panic!("refused export must not publish output"),
    }
}

#[test]
fn export_interleaved_batches_preserves_duplicate_ids_fixed_nulls_and_each_signed_string_layout() {
    let (owner, tracker) = owner_with_tracker();
    let types = [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Utf8, true),
    ];
    let mut index = index(&types, owner.as_ref(), &tracker);
    let long = "alpha-".repeat(20_000);
    intern(
        &mut index,
        &[
            Arc::new(Int32Array::from(vec![Some(7), None, Some(7), Some(9)])),
            Arc::new(StringArray::from(vec![
                Some(long.as_str()),
                Some("n"),
                Some(long.as_str()),
                None,
            ])),
        ],
        &types,
        &[2, 1, 3, 0],
        owner.as_ref(),
        &tracker,
    );
    let second_types = [
        types[0].clone(),
        FunctionValueType::new(DataType::LargeUtf8, true),
    ];
    intern(
        &mut index,
        &[
            Arc::new(Int32Array::from(vec![Some(7), Some(11), None, Some(9)])),
            Arc::new(LargeStringArray::from(vec![
                Some(long.as_str()),
                Some("beta"),
                Some("n"),
                None,
            ])),
        ],
        &second_types,
        &[1, 0, 2, 1, 3],
        owner.as_ref(),
        &tracker,
    );
    assert_eq!(index.len(), 4);
    let ids = [KeyId(3), KeyId(0), KeyId(2), KeyId(1), KeyId(3), KeyId(0)];
    for layout in [DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8] {
        let targets = [
            types[0].clone(),
            FunctionValueType::new(layout.clone(), true),
        ];
        let output = index
            .export_keys(&ids, &targets, owner.as_ref(), &CopyControl)
            .unwrap();
        assert_eq!(output.bindings(), &targets);
        assert_eq!(
            output.arrays()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(11), Some(7), Some(9), None, Some(11), Some(7)]
        );
        assert_eq!(output.arrays()[1].data_type(), &layout);
        assert_eq!(
            strings(&output.arrays()[1]),
            vec![
                Some("beta"),
                Some(long.as_str()),
                None,
                Some("n"),
                Some("beta"),
                Some(long.as_str())
            ]
        );
        for array in output.arrays() {
            array.to_data().validate_full().unwrap();
        }
    }
}

#[test]
fn export_binary_layouts_preserve_non_utf8_null_empty_and_repeated_long_values() {
    let (owner, tracker) = owner_with_tracker();
    let types = [FunctionValueType::new(DataType::Binary, true)];
    let mut index = index(&types, owner.as_ref(), &tracker);
    let long = vec![0xff; 150_019];
    intern(
        &mut index,
        &[Arc::new(BinaryArray::from(vec![
            Some(b"\xff\0a".as_slice()),
            None,
            Some(b"".as_slice()),
            Some(long.as_slice()),
        ]))],
        &types,
        &[0, 1, 2, 3],
        owner.as_ref(),
        &tracker,
    );
    assert_eq!(index.len(), 4);
    for layout in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        let output = index
            .export_keys(
                &[KeyId(3), KeyId(1), KeyId(0), KeyId(2), KeyId(3)],
                &[FunctionValueType::new(layout.clone(), true)],
                owner.as_ref(),
                &CopyControl,
            )
            .unwrap();
        assert_eq!(output.arrays()[0].data_type(), &layout);
        assert_eq!(
            binary(&output.arrays()[0]),
            vec![
                Some(long.as_slice()),
                None,
                Some(b"\xff\0a".as_slice()),
                Some(b"".as_slice()),
                Some(long.as_slice())
            ]
        );
        output.arrays()[0].to_data().validate_full().unwrap();
    }
}

#[test]
fn export_nested_struct_and_list_preserves_fields_nulls_and_requested_byte_layouts() {
    let (owner, tracker) = owner_with_tracker();
    let item = Arc::new(Field::new("item", DataType::Utf8, true));
    let lists: ArrayRef = Arc::new(
        ListArray::try_new(
            item.clone(),
            OffsetBuffer::new(vec![0, 2, 2, 3].into()),
            Arc::new(StringArray::from(vec![Some("left"), None, Some("right")])),
            Some(NullBuffer::from(vec![true, false, true])),
        )
        .unwrap(),
    );
    let fields: Fields = vec![
        Field::new("texts", DataType::List(item), true),
        Field::new("payload", DataType::Binary, true),
    ]
    .into();
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            fields,
            vec![
                lists,
                Arc::new(BinaryArray::from(vec![
                    Some(b"\xff\0".as_slice()),
                    None,
                    Some(b"".as_slice()),
                ])),
            ],
            Some(NullBuffer::from(vec![true, false, true])),
        )
        .unwrap(),
    );
    let source_type = FunctionValueType::new(source.data_type().clone(), true);
    let mut index = index(std::slice::from_ref(&source_type), owner.as_ref(), &tracker);
    intern(
        &mut index,
        &[source],
        &[source_type],
        &[2, 0, 1, 0],
        owner.as_ref(),
        &tracker,
    );
    assert_eq!(index.len(), 3);
    for (text, bytes) in [
        (DataType::LargeUtf8, DataType::LargeBinary),
        (DataType::Utf8View, DataType::BinaryView),
    ] {
        let target_type = DataType::Struct(
            vec![
                Field::new(
                    "texts",
                    DataType::List(Arc::new(Field::new("item", text.clone(), true))),
                    true,
                ),
                Field::new("payload", bytes.clone(), true),
            ]
            .into(),
        );
        let target = FunctionValueType::new(target_type.clone(), true);
        let output = index
            .export_keys(
                &[KeyId(1), KeyId(2), KeyId(0), KeyId(1)],
                std::slice::from_ref(&target),
                owner.as_ref(),
                &CopyControl,
            )
            .unwrap();
        assert_eq!(output.bindings(), &[target]);
        assert_eq!(output.arrays()[0].data_type(), &target_type);
        let root = output.arrays()[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            (0..4).map(|row| root.is_null(row)).collect::<Vec<_>>(),
            vec![false, true, false, false]
        );
        let lists = root.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.value_offsets(), &[0, 2, 2, 3, 5]);
        assert!(lists.is_null(1));
        assert_eq!(lists.values().data_type(), &text);
        assert_eq!(
            strings(lists.values()),
            vec![Some("left"), None, Some("right"), Some("left"), None]
        );
        assert_eq!(root.column(1).data_type(), &bytes);
        assert_eq!(
            binary(root.column(1)),
            vec![
                Some(b"\xff\0".as_slice()),
                None,
                Some(b"".as_slice()),
                Some(b"\xff\0".as_slice())
            ]
        );
        output.arrays()[0].to_data().validate_full().unwrap();
    }
}

#[test]
fn export_empty_and_zero_payload_types_keep_exact_rows_and_reject_empty_domain_mismatches() {
    let (owner, tracker) = owner_with_tracker();
    for ty in [
        DataType::FixedSizeBinary(0),
        DataType::Int64,
        DataType::Struct(Fields::empty()),
        DataType::Utf8,
    ] {
        let source_type = FunctionValueType::new(ty.clone(), true);
        let index = index(std::slice::from_ref(&source_type), owner.as_ref(), &tracker);
        let target = FunctionValueType::new(ty.clone(), false);
        let output = index
            .export_keys(
                &[],
                std::slice::from_ref(&target),
                owner.as_ref(),
                &CopyControl,
            )
            .unwrap();
        assert_eq!(output.bindings(), &[target]);
        assert_eq!(output.arrays()[0].len(), 0);
        assert_eq!(output.arrays()[0].data_type(), &ty);
        output.arrays()[0].to_data().validate_full().unwrap();
    }
    let zero: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_new(
            0,
            Buffer::from_vec(Vec::<u8>::new()),
            Some(NullBuffer::new_valid(3)),
        )
        .unwrap(),
    );
    let empty_struct: ArrayRef =
        Arc::new(StructArray::try_new_with_length(Fields::empty(), vec![], None, 3).unwrap());
    for source in [zero, empty_struct] {
        let source_type = FunctionValueType::new(source.data_type().clone(), false);
        let mut index = index(std::slice::from_ref(&source_type), owner.as_ref(), &tracker);
        intern(
            &mut index,
            &[source],
            std::slice::from_ref(&source_type),
            &[0, 1, 2],
            owner.as_ref(),
            &tracker,
        );
        assert_eq!(index.len(), 1);
        let output = index
            .export_keys(
                &[KeyId(0), KeyId(0)],
                std::slice::from_ref(&source_type),
                owner.as_ref(),
                &CopyControl,
            )
            .unwrap();
        assert_eq!(output.arrays()[0].len(), 2);
        assert_eq!(output.arrays()[0].data_type(), source_type.data_type());
        assert_eq!(output.arrays()[0].null_count(), 0);
        output.arrays()[0].to_data().validate_full().unwrap();
    }
    let named = |name: &str, item_nullable| {
        DataType::Struct(
            vec![Field::new(
                name,
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, item_nullable))),
                true,
            )]
            .into(),
        )
    };
    for (source, target) in [
        (DataType::Utf8, DataType::Binary),
        (named("payload", true), named("renamed", true)),
        (named("payload", true), named("payload", false)),
    ] {
        let index = index(
            &[FunctionValueType::new(source, true)],
            owner.as_ref(),
            &tracker,
        );
        let error = failure(index.export_keys(
            &[],
            &[FunctionValueType::new(target, true)],
            owner.as_ref(),
            &CopyControl,
        ));
        assert_value_failure(&error, novarocks_value_arrays::ValueViolation::Layout);
        assert_eq!(index.len(), 0);
    }
}

#[test]
fn export_selected_null_to_required_is_typed_refusal_without_poisoning_index() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let mut index = index(std::slice::from_ref(&ty), owner.as_ref(), &tracker);
    intern(
        &mut index,
        &[Arc::new(StringArray::from(vec![Some("kept"), None]))],
        std::slice::from_ref(&ty),
        &[0, 1],
        owner.as_ref(),
        &tracker,
    );
    let required = FunctionValueType::new(DataType::Utf8, false);
    let first = index
        .export_keys(
            &[KeyId(0), KeyId(0)],
            std::slice::from_ref(&required),
            owner.as_ref(),
            &CopyControl,
        )
        .unwrap();
    assert_eq!(
        strings(&first.arrays()[0]),
        vec![Some("kept"), Some("kept")]
    );
    let error = failure(index.export_keys(&[KeyId(1)], &[required], owner.as_ref(), &CopyControl));
    assert_value_failure(&error, novarocks_value_arrays::ValueViolation::RequiredNull);
    assert_eq!(index.len(), 2);
    let after = index
        .export_keys(
            &[KeyId(1), KeyId(0)],
            std::slice::from_ref(&ty),
            owner.as_ref(),
            &CopyControl,
        )
        .unwrap();
    assert_eq!(strings(&after.arrays()[0]), vec![None, Some("kept")]);
    assert_eq!(
        index
            .read_keys(&[KeyId(0), KeyId(1)], owner.as_ref(), &CopyControl)
            .unwrap()
            .len(),
        2
    );
}

struct CountedExportOwner<'a> {
    original: &'a dyn InputConstructionOwner,
    requests: AtomicUsize,
    stop: usize,
    cause: ExecutionFailure,
}
impl InputConstructionOwner for CountedExportOwner<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.original.storage_lifetime()
    }
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
        let next = self.requests.fetch_add(1, Ordering::SeqCst) + 1;
        if next == self.stop {
            Err(ConstructionFailure::from(self.cause.clone()))
        } else {
            self.original.request(layout)
        }
    }
    fn observe_opaque_upper_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
        self.original.observe_opaque_upper_bound(bytes)
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
struct ExportControl {
    calls: AtomicUsize,
    stop: usize,
}
impl KernelEvaluationControl for ExportControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        let next = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if next == self.stop {
            Err(KernelFailure::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("export never waits")
    }
}
#[test]
fn export_original_growth_and_control_refusals_leave_published_keys_readable() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let mut index = index(std::slice::from_ref(&ty), owner.as_ref(), &tracker);
    let text = "retained-long-export-".repeat(5000);
    intern(
        &mut index,
        &[Arc::new(StringArray::from(vec![text.as_str()]))],
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
        &tracker,
    );
    let ids = [KeyId(0), KeyId(0)];
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let baseline = CountedExportOwner {
        original: owner.as_ref(),
        requests: AtomicUsize::new(0),
        stop: usize::MAX,
        cause: cause.clone(),
    };
    index
        .export_keys(&ids, std::slice::from_ref(&ty), &baseline, &CopyControl)
        .unwrap();
    let requests = baseline.requests.load(Ordering::SeqCst);
    assert!(requests > 0);
    for stop in 1..=requests {
        let refusing = CountedExportOwner {
            original: owner.as_ref(),
            requests: AtomicUsize::new(0),
            stop,
            cause: cause.clone(),
        };
        assert_eq!(
            failure(index.export_keys(&ids, std::slice::from_ref(&ty), &refusing, &CopyControl)),
            cause
        );
        assert_eq!(refusing.requests.load(Ordering::SeqCst), stop);
    }
    let baseline_control = ExportControl {
        calls: AtomicUsize::new(0),
        stop: usize::MAX,
    };
    index
        .export_keys(
            &ids,
            std::slice::from_ref(&ty),
            owner.as_ref(),
            &baseline_control,
        )
        .unwrap();
    let calls = baseline_control.calls.load(Ordering::SeqCst);
    assert!(calls > 3);
    for stop in [1, calls / 2, calls] {
        let control = ExportControl {
            calls: AtomicUsize::new(0),
            stop,
        };
        assert_eq!(
            failure(index.export_keys(&ids, std::slice::from_ref(&ty), owner.as_ref(), &control)),
            ExecutionFailure::from(KernelFailure::DeadlineExceeded)
        );
        assert_eq!(control.calls.load(Ordering::SeqCst), stop);
    }
    assert_eq!(index.len(), 1);
    let output = index
        .export_keys(
            &ids,
            std::slice::from_ref(&ty),
            owner.as_ref(),
            &CopyControl,
        )
        .unwrap();
    assert_eq!(
        strings(&output.arrays()[0]),
        vec![Some(text.as_str()), Some(text.as_str())]
    );
}

#[test]
fn export_creator_survives_export_index_owner_and_arrow_alias_until_last_payload_buffer() {
    struct Tag(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Tag {}
    impl Drop for Tag {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let mut index = index(std::slice::from_ref(&ty), owner.as_ref(), &tracker);
    let text = "long output must keep its real export creator".repeat(100);
    intern(
        &mut index,
        &[Arc::new(StringArray::from(vec![text.as_str()]))],
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
        &tracker,
    );
    let drops = Arc::new(AtomicUsize::new(0));
    let tagged = TaggedCreator {
        original: owner.as_ref(),
        storage: owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &ValueTypeStorageLifetime::held(Arc::new(Tag(drops.clone()))),
                |layout| owner.request(layout),
            )
            .unwrap(),
    };
    let exported = index
        .export_keys(&[KeyId(0)], &[ty], &tagged, &CopyControl)
        .unwrap();
    let arrow_alias = Arc::clone(&exported.arrays()[0]);
    let slice_alias = arrow_alias.slice(0, 1);
    let data = slice_alias.to_data();
    let last_buffer = data.buffers()[1].clone();
    drop(data);
    drop(exported);
    drop(index);
    drop(tagged);
    drop(owner);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(strings(&slice_alias), vec![Some(text.as_str())]);
    drop(arrow_alias);
    drop(slice_alias);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(last_buffer.as_slice(), text.as_bytes());
    drop(last_buffer);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn export_rejects_nominal_fixed_type_changes_without_casting_or_poisoning() {
    let (owner, tracker) = owner_with_tracker();
    let decimal: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(123), None])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    );
    let fixed: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_iter(
            [b"0123456789abcdef0123456789abcdef".as_slice()].into_iter(),
        )
        .unwrap(),
    );
    for (source, wrong) in [
        (decimal, DataType::Decimal128(11, 2)),
        (fixed, DataType::FixedSizeBinary(16)),
    ] {
        let actual = FunctionValueType::new(source.data_type().clone(), true);
        let mut index = index(std::slice::from_ref(&actual), owner.as_ref(), &tracker);
        intern(
            &mut index,
            &[source],
            std::slice::from_ref(&actual),
            &[0],
            owner.as_ref(),
            &tracker,
        );
        let error = failure(index.export_keys(
            &[KeyId(0)],
            &[FunctionValueType::new(wrong, true)],
            owner.as_ref(),
            &CopyControl,
        ));
        assert_value_failure(&error, novarocks_value_arrays::ValueViolation::Layout);
        assert_eq!(
            index
                .export_keys(
                    &[KeyId(0)],
                    std::slice::from_ref(&actual),
                    owner.as_ref(),
                    &CopyControl
                )
                .unwrap()
                .arrays()[0]
                .data_type(),
            actual.data_type()
        );
    }
}

#[test]
fn export_required_complex_root_checks_selected_logical_null_without_changing_children() {
    let (owner, tracker) = owner_with_tracker();
    let fields: Fields = vec![Field::new("value", DataType::Int32, true)].into();
    let source: ArrayRef = Arc::new(
        StructArray::try_new(
            fields,
            vec![Arc::new(Int32Array::from(vec![Some(7), None]))],
            Some(NullBuffer::from(vec![true, false])),
        )
        .unwrap(),
    );
    let ty = FunctionValueType::new(source.data_type().clone(), true);
    let mut index = index(std::slice::from_ref(&ty), owner.as_ref(), &tracker);
    intern(
        &mut index,
        &[source],
        std::slice::from_ref(&ty),
        &[0, 1],
        owner.as_ref(),
        &tracker,
    );
    let target = FunctionValueType::new(ty.data_type().clone(), false);
    let exported = index
        .export_keys(
            &[KeyId(0)],
            std::slice::from_ref(&target),
            owner.as_ref(),
            &CopyControl,
        )
        .unwrap();
    let root = exported.arrays()[0]
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        root.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_value_failure(
        &failure(index.export_keys(&[KeyId(1)], &[target], owner.as_ref(), &CopyControl)),
        novarocks_value_arrays::ValueViolation::RequiredNull,
    );
    assert_eq!(
        index
            .export_keys(&[KeyId(1)], &[ty], owner.as_ref(), &CopyControl)
            .unwrap()
            .arrays()[0]
            .null_count(),
        1
    );
}

#[test]
fn exported_null_carrier_cannot_be_signed_as_required_and_survives_compaction() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Null, true);
    let mut index = index(std::slice::from_ref(&ty), owner.as_ref(), &tracker);
    intern(
        &mut index,
        &[Arc::new(NullArray::new(3))],
        std::slice::from_ref(&ty),
        &[2, 0, 2, 1],
        owner.as_ref(),
        &tracker,
    );
    assert_eq!(index.len(), 1);
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    let error = failure(index.export_keys(
        &[KeyId(0)],
        &[FunctionValueType::new(DataType::Null, false)],
        owner.as_ref(),
        &CopyControl,
    ));
    assert_value_failure(&error, novarocks_value_arrays::ValueViolation::RequiredNull);
    let output = index
        .export_keys(&[KeyId(0), KeyId(0)], &[ty], owner.as_ref(), &CopyControl)
        .unwrap();
    assert_eq!(output.arrays()[0].len(), 2);
    assert_eq!(output.arrays()[0].logical_null_count(), 2);
    assert!(output.arrays()[0].nulls().is_none());
}
