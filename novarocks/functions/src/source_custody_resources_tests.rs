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

//! Independent requested-Layout measurements, with input and host stock excluded.
//! This test-only observer is not a production allocator or a funding authority.
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::source_custody_direct_tests::{Control, Host};
use crate::*;
use arrow_array::{types::*, *};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit, UnionFields, UnionMode};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::{Cell, RefCell},
    sync::Arc,
};

struct Requests;
#[global_allocator]
static REQUESTS: Requests = Requests;
const SLOTS: usize = 8192;
struct Measured {
    cumulative: usize,
    requests: usize,
    live: usize,
    peak: usize,
    slots: [(usize, usize); SLOTS],
    overflow: bool,
}
impl Measured {
    const fn new() -> Self {
        Self {
            cumulative: 0,
            requests: 0,
            live: 0,
            peak: 0,
            slots: [(0, 0); SLOTS],
            overflow: false,
        }
    }
    fn allocate(&mut self, pointer: *mut u8, size: usize) {
        if pointer.is_null() || size == 0 {
            return;
        }
        self.cumulative += size;
        self.requests += 1;
        self.live += size;
        self.peak = self.peak.max(self.live);
        if let Some(slot) = self.slots.iter_mut().find(|slot| slot.0 == 0) {
            *slot = (pointer.addr(), size);
        } else {
            self.overflow = true;
        }
    }
    fn release(&mut self, pointer: *mut u8) {
        if let Some(slot) = self
            .slots
            .iter_mut()
            .find(|slot| slot.0 == pointer.addr() && slot.0 != 0)
        {
            self.live -= slot.1;
            *slot = (0, 0);
        }
    }
    fn reallocate(&mut self, old_pointer: *mut u8, new_pointer: *mut u8, size: usize) {
        if new_pointer.is_null() {
            return;
        }
        // Fix the old slot before adding the new request: an in-place realloc
        // may put its new record into an earlier hole with the same address.
        let old = self
            .slots
            .iter()
            .position(|slot| slot.0 == old_pointer.addr() && slot.0 != 0);
        self.allocate(new_pointer, size);
        if let Some(index) = old {
            self.live -= self.slots[index].1;
            self.slots[index] = (0, 0);
        }
    }
}
thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static MEASURED: RefCell<Measured> = const { RefCell::new(Measured::new()) };
}
fn observe(operation: impl FnOnce(&mut Measured)) {
    let _ = ENABLED.try_with(|enabled| {
        if enabled.get() {
            let _ = MEASURED.try_with(|measured| operation(&mut measured.borrow_mut()));
        }
    });
}
unsafe impl GlobalAlloc for Requests {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        observe(|m| m.allocate(pointer, layout.size()));
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        observe(|m| m.allocate(pointer, layout.size()));
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        observe(|m| m.release(pointer));
        unsafe {
            System.dealloc(pointer, layout);
        }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let new_pointer = unsafe { System.realloc(pointer, layout, size) };
        if !new_pointer.is_null() {
            // The full new request counts even for in-place growth. The old/new
            // overlap is a conservative peak until the old request retires.
            observe(|m| m.reallocate(pointer, new_pointer, size));
        }
        new_pointer
    }
}
pub(crate) fn without_recording<T>(operation: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            let _ = ENABLED.try_with(|enabled| enabled.set(self.0));
        }
    }
    let old = ENABLED
        .try_with(|enabled| enabled.replace(false))
        .unwrap_or(false);
    let restore = Restore(old);
    let result = operation();
    drop(restore);
    result
}
fn measured<T>(operation: impl FnOnce() -> T) -> (T, (usize, usize, usize, usize)) {
    MEASURED.with(|measured| *measured.borrow_mut() = Measured::new());
    ENABLED.with(|enabled| enabled.set(true));
    struct Stop;
    impl Drop for Stop {
        fn drop(&mut self) {
            ENABLED.with(|enabled| enabled.set(false));
        }
    }
    let stop = Stop;
    let result = operation();
    drop(stop);
    let facts = MEASURED.with(|measured| {
        let m = measured.borrow();
        assert!(
            !m.overflow,
            "fixed observer capacity must cover all actual requests"
        );
        (m.cumulative, m.requests, m.peak, m.live)
    });
    (result, facts)
}

fn check(array: ArrayRef) {
    let host = Arc::new(Host::default());
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let before = Arc::strong_count(&array);
    let (prepared, prepare_requests) =
        measured(|| PreparedSourceCustody::<()>::prepare(&array, &mut work));
    let prepared = prepared.unwrap();
    assert_eq!(prepare_requests, (0, 0, 0, 0));
    assert_eq!(Arc::strong_count(&array), before);
    let facts = prepared.request_facts();
    let (source, holder_requests) =
        measured(|| SourceBackingOwner::try_from_owned_direct((), host.clone(), &mut work));
    let source = source.unwrap();
    assert_eq!(holder_requests, (0, 0, 0, 0));
    let holder = SourceBackingOwner::<()>::owned_request_facts().unwrap();
    let ledger = host.ledger.lock().unwrap();
    assert_eq!(
        ledger
            .events
            .iter()
            .filter(|e| !e.0)
            .map(|e| e.1)
            .sum::<usize>(),
        holder.typed().bytes()
    );
    assert_eq!(
        ledger.events.iter().filter(|e| !e.0).count(),
        holder.typed().requests()
    );
    drop(ledger);
    let charge = OpaqueRetainedCharge::try_new(host.clone()).unwrap();
    let mut reservation = charge.reserve_operation(facts.global_peak_upper()).unwrap();
    let at = host.ledger.lock().unwrap().events.len();
    let (result, actual) = measured(|| prepared.retain(&source, &mut reservation, &mut work));
    let result = result.unwrap();
    assert!(
        actual.0 <= facts.global().bytes(),
        "{:?}: cumulative {:?} exceeds {:?}",
        array.data_type(),
        actual,
        facts
    );
    assert!(
        actual.1 <= facts.global().requests(),
        "{:?}: request count {:?} exceeds {:?}",
        array.data_type(),
        actual,
        facts
    );
    assert!(actual.2 <= facts.global_peak_upper());
    assert!(actual.3 <= facts.global_retained_upper());
    assert_eq!(reservation.remaining_bytes(), 0);
    let ledger = host.ledger.lock().unwrap();
    let typed = &ledger.events[at..];
    assert_eq!(typed, &[(false, facts.typed().bytes())]);
    assert_eq!(facts.typed().requests(), 1);
    drop(ledger);
    assert_eq!(
        result.to_data(),
        array.to_data(),
        "original value/type/NULL and offset must be preserved"
    );
    let old_source = SourceBackingOwner::try_from_owned((), host.clone(), &control).unwrap();
    let legacy = retain_source_backing(array.clone(), &old_source, &control).unwrap();
    assert_eq!(legacy.to_data(), result.to_data());
    drop(legacy);
    drop(old_source);
    drop(result);
    drop(source);
    drop(reservation);
    drop(charge);
    host.zero();
}
fn field(data_type: DataType) -> Arc<Field> {
    Arc::new(Field::new("original", data_type, true))
}
#[test]
fn source_custody_resources_all_builtin_empty_carriers_and_nominal_metadata() {
    let mut types = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Date32,
        DataType::Date64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Utf8View,
        DataType::BinaryView,
        DataType::FixedSizeBinary(3),
        DataType::Decimal32(9, 2),
        DataType::Decimal64(18, 2),
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 2),
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Timestamp(unit, Some("America/New_York".into())));
        types.push(DataType::Duration(unit));
    }
    types.extend([
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::List(field(DataType::Int64)),
        DataType::LargeList(field(DataType::Int64)),
        DataType::ListView(field(DataType::Int64)),
        DataType::LargeListView(field(DataType::Int64)),
        DataType::FixedSizeList(field(DataType::Int64), 2),
        DataType::Struct(vec![field(DataType::Null)].into()),
        DataType::Struct(Vec::<Field>::new().into()),
    ]);
    let entries = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Int64, false)),
                field(DataType::Utf8),
            ]
            .into(),
        ),
        false,
    );
    types.push(DataType::Map(Arc::new(entries), false));
    for key in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ] {
        types.push(DataType::Dictionary(
            Box::new(key),
            Box::new(DataType::Utf8),
        ));
    }
    for key in [DataType::Int16, DataType::Int32, DataType::Int64] {
        types.push(DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", key, false)),
            field(DataType::Utf8),
        ));
    }
    for mode in [UnionMode::Dense, UnionMode::Sparse] {
        types.push(DataType::Union(
            UnionFields::try_new([127], [Field::new("nominal", DataType::Utf8, true)]).unwrap(),
            mode,
        ));
    }
    for data_type in types {
        check(new_empty_array(&data_type));
    }
}
#[test]
fn source_custody_resources_nonempty_offset_nested_view_union_and_validation_carriers() {
    check((Arc::new(Int64Array::from(vec![Some(9), None, Some(3)])) as ArrayRef).slice(1, 2));
    check(
        (Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])) as ArrayRef).slice(1, 2),
    );
    check(
        (Arc::new(StringArray::from(vec![
            Some("prefix"),
            None,
            Some("original"),
        ])) as ArrayRef)
            .slice(1, 2),
    );
    check(
        (Arc::new(LargeStringArray::from(vec![
            Some("prefix"),
            None,
            Some("original"),
        ])) as ArrayRef)
            .slice(1, 2),
    );
    let view = StringViewArray::from(vec![
        Some("inline"),
        None,
        Some("long original out of line payload"),
    ]);
    let mut buffers = view.data_buffers().to_vec();
    buffers.extend((0..512).map(|_| Buffer::from_vec(Vec::<u8>::new())));
    check(
        (Arc::new(
            StringViewArray::try_new(view.views().clone(), buffers, view.nulls().cloned()).unwrap(),
        ) as ArrayRef)
            .slice(1, 2),
    );
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6]));
    check(
        (Arc::new(ListArray::new(
            field(DataType::Int64),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4, 6])),
            values.clone(),
            Some(NullBuffer::from(vec![true, false, true])),
        )) as ArrayRef)
            .slice(1, 2),
    );
    check(
        (Arc::new(ListViewArray::new(
            field(DataType::Int64),
            ScalarBuffer::from(vec![4, 0, 2]),
            ScalarBuffer::from(vec![2, 2, 2]),
            values.clone(),
            None,
        )) as ArrayRef)
            .slice(1, 2),
    );
    let fsl = FixedSizeListArray::new(
        Arc::new(Field::new("required", DataType::Int64, false)),
        2,
        values,
        Some(NullBuffer::from(vec![true, false, true])),
    );
    check((Arc::new(fsl) as ArrayRef).slice(1, 2));
    let inner = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), None, Some(1)]),
        Arc::new(StringArray::from(vec!["a", "b"])),
    )
    .unwrap();
    let outer =
        DictionaryArray::<UInt64Type>::try_new(UInt64Array::from(vec![0, 1, 2]), Arc::new(inner))
            .unwrap();
    check((Arc::new(outer) as ArrayRef).slice(1, 2));
    let runs = Int32Array::from(vec![2, 5]);
    let values = StringArray::from(vec![Some("a"), None]);
    check(
        (Arc::new(RunArray::<Int32Type>::try_new(&runs, &values).unwrap()) as ArrayRef).slice(1, 3),
    );
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            field(DataType::Utf8),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
        None,
    );
    let entry_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    check(
        (Arc::new(MapArray::new(
            entry_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 3])),
            entries,
            None,
            true,
        )) as ArrayRef)
            .slice(1, 1),
    );
    for mode in [UnionMode::Dense, UnionMode::Sparse] {
        let fields =
            UnionFields::try_new([127], [Field::new("original", DataType::Utf8, true)]).unwrap();
        let offsets = (mode == UnionMode::Dense).then(|| ScalarBuffer::from(vec![0, 1, 2]));
        let array = UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![127, 127, 127]),
            offsets,
            vec![Arc::new(StringArray::from(vec![
                Some("a"),
                None,
                Some("c"),
            ]))],
        )
        .unwrap();
        check((Arc::new(array) as ArrayRef).slice(1, 2));
    }
    let original_field = Arc::new(
        Field::new("nominal", DataType::Utf8, true).with_metadata(
            [(
                String::from("key"),
                String::from("independent original metadata"),
            )]
            .into(),
        ),
    );
    let original_count = Arc::strong_count(&original_field);
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![original_field.clone()].into(),
        vec![Arc::new(StringArray::from(vec!["a", "b", "c"]))],
        None,
    ));
    check(array.slice(1, 1));
    drop(array);
    assert_eq!(Arc::strong_count(&original_field), original_count);
}
#[test]
fn source_custody_resources_sparse_union_and_view_tables_have_independent_request_deltas() {
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let union = |id| -> ArrayRef {
        Arc::new(
            UnionArray::try_new(
                UnionFields::try_new([id], [Field::new("value", DataType::Int32, false)]).unwrap(),
                ScalarBuffer::from(vec![id]),
                None,
                vec![Arc::new(Int32Array::from(vec![42]))],
            )
            .unwrap(),
        )
    };
    let low = union(0);
    let high = union(127);
    let low = PreparedSourceCustody::<()>::prepare(&low, &mut work)
        .unwrap()
        .request_facts();
    let high = PreparedSourceCustody::<()>::prepare(&high, &mut work)
        .unwrap()
        .request_facts();
    // Arrow's T indexed header and M indexed result each request max_id+1 slots.
    assert_eq!(
        high.global().bytes() - low.global().bytes(),
        2 * 127 * size_of::<Option<ArrayRef>>()
    );
    assert_eq!(high.global().requests(), low.global().requests());
    let base = StringViewArray::from(vec!["x"]);
    let many: ArrayRef = Arc::new(
        StringViewArray::try_new(
            base.views().clone(),
            vec![Buffer::from_vec(Vec::<u8>::new()); 512],
            None,
        )
        .unwrap(),
    );
    let base: ArrayRef = Arc::new(base);
    let base = PreparedSourceCustody::<()>::prepare(&base, &mut work)
        .unwrap()
        .request_facts();
    let many = PreparedSourceCustody::<()>::prepare(&many, &mut work)
        .unwrap()
        .request_facts();
    assert!(many.global().bytes() > base.global().bytes());
    assert!(many.global().requests() > base.global().requests());
}
#[test]
fn source_custody_resources_prefix_and_checked_arithmetic_preserve_typed_failures() {
    let prefix = source_custody_prefix_facts();
    assert_eq!(prefix.requests().bytes(), MAX_ROW_ERROR_MESSAGE_BYTES);
    assert_eq!(prefix.requests().requests(), 1);
    assert_eq!(prefix.failure_retained_upper(), MAX_ROW_ERROR_MESSAGE_BYTES);
    use crate::source_custody_resources::*;
    assert_eq!(
        SourceCustodyRequestFacts::owned(Layout::new::<u64>(), usize::MAX),
        Err(ScalarResourceError::Arithmetic)
    );
    assert_eq!(
        model_failure(ScalarResourceError::Arithmetic),
        KernelFailure::ResourceExhausted
    );
    assert_eq!(
        model_failure(ScalarResourceError::SourceModel("original model refusal")),
        crate::kernel_control::invalid("original model refusal")
    );
    assert_eq!(
        SourceCustodyRequestFacts::from_parts(
            SourceCustodyAllocationFacts::layout(
                Layout::from_size_align(isize::MAX as usize, 1).unwrap()
            ),
            SourceCustodyAllocationFacts::layout(
                Layout::from_size_align(isize::MAX as usize, 1).unwrap()
            )
        )
        .unwrap()
        .global()
        .bytes(),
        isize::MAX as usize
    );
}

#[test]
fn source_custody_resources_primitive_requests_have_a_source_independent_closed_expectation() {
    use arrow_data::BufferSpec;
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let facts = PreparedSourceCustody::<()>::prepare(&array, &mut work)
        .unwrap()
        .request_facts();
    // Three Vec<Buffer> tables (T, W, D), two validation tables, one
    // pointer-sized custodian Arc, Bytes upper Arc, and primitive rebox Arc.
    let expected = 3 * size_of::<Buffer>()
        + 2 * size_of::<BufferSpec>()
        + arc_layout(Layout::new::<usize>()).unwrap().size()
        + 64
        + arc_layout(Layout::new::<Int64Array>()).unwrap().size();
    assert_eq!(facts.global().bytes(), expected);
    assert_eq!(facts.global().requests(), 8);
    assert_eq!(facts.global_peak_upper(), expected);
    assert_eq!(facts.global_retained_upper(), expected);
}
#[test]
fn source_custody_resources_metadata_recursion_observes_the_borrowed_work_quantum() {
    use crate::source_custody_resources::{
        RequestSourceNode, RequestSummary, SourceNodeRole, SourceRequestPricing,
        summarize_source_node,
    };
    struct Cancel(std::sync::Mutex<Vec<u32>>);
    impl KernelEvaluationControl for Cancel {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            self.0.lock().unwrap().push(units);
            Err(KernelFailure::Cancelled)
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("no wait")
        }
    }
    let control = Cancel(std::sync::Mutex::new(Vec::new()));
    let mut work = EvaluationCheckpoints::new(&control);
    let mut data_type = DataType::Int64;
    for _ in 0..300 {
        data_type = DataType::Dictionary(Box::new(DataType::Int8), Box::new(data_type));
    }
    let pricing = SourceRequestPricing {
        group: Layout::new::<u64>(),
        custodian: Layout::new::<u64>(),
        bufferless: Layout::new::<u64>(),
    };
    let result = summarize_source_node(
        RequestSourceNode {
            data_type: &data_type,
            data_buffers: 1,
            child_count: 1,
            nulls: None,
            header_fields: 0,
            body: Some(Layout::new::<u64>()),
            role: SourceNodeRole::Rebox,
        },
        RequestSummary::empty(),
        &pricing,
        &mut work,
    );
    assert!(matches!(result, Err(KernelFailure::Cancelled)));
    assert_eq!(*control.0.lock().unwrap(), vec![MAX_UNOBSERVED_KERNEL_WORK]);
    // The latched first refusal also precedes a deliberately oversized table.
    let result = summarize_source_node(
        RequestSourceNode {
            data_type: &DataType::Int64,
            data_buffers: 1,
            child_count: 0,
            nulls: None,
            header_fields: 0,
            body: Some(Layout::new::<u64>()),
            role: SourceNodeRole::Rebox,
        },
        RequestSummary::empty(),
        &pricing,
        &mut work,
    );
    assert!(matches!(result, Err(KernelFailure::Cancelled)));
    let fresh_control = Control::default();
    let mut fresh = EvaluationCheckpoints::new(&fresh_control);
    let result = summarize_source_node(
        RequestSourceNode {
            data_type: &DataType::Int64,
            data_buffers: usize::MAX,
            child_count: 0,
            nulls: None,
            header_fields: 0,
            body: Some(Layout::new::<u64>()),
            role: SourceNodeRole::Rebox,
        },
        RequestSummary::empty(),
        &pricing,
        &mut fresh,
    );
    assert!(matches!(result, Err(KernelFailure::ResourceExhausted)));
}

#[test]
fn source_custody_resources_observer_in_place_realloc_preserves_exact_old_slot() {
    let first = 0x10usize as *mut u8;
    let old = 0x20usize as *mut u8;
    let moved = 0x30usize as *mut u8;
    let mut requests = Measured::new();
    requests.allocate(first, 8);
    requests.allocate(old, 16);
    requests.release(first);
    requests.reallocate(old, old, 64);
    assert_eq!(
        (
            requests.cumulative,
            requests.requests,
            requests.live,
            requests.peak
        ),
        (88, 3, 64, 80)
    );
    assert_eq!(
        requests
            .slots
            .iter()
            .filter(|slot| slot.0 == old.addr())
            .count(),
        1
    );
    requests.reallocate(old, std::ptr::null_mut(), 256);
    assert_eq!(
        (
            requests.cumulative,
            requests.requests,
            requests.live,
            requests.peak
        ),
        (88, 3, 64, 80)
    );
    requests.reallocate(old, moved, 32);
    assert_eq!(
        (
            requests.cumulative,
            requests.requests,
            requests.live,
            requests.peak
        ),
        (120, 4, 32, 96)
    );
    requests.release(moved);
    assert_eq!(requests.live, 0);
}
