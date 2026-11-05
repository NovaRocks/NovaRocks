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

//! Scoped allocation and physical exit of the staged Native scalar cursor.
//! Source/schema/budget fixtures are prepared outside measurement. This proves
//! the cursor Box and one-column Vec, not source growth, a RootInputPermit,
//! a whole Native Session, or complete process/query graph ownership.
//! Existing credit-release notification runs outside cursor allocation measurement.

use arrow::array::{ArrayRef, RecordBatch, TimestampNanosecondArray};
use arrow::datatypes::Field;
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use novarocks_execution::runtime::task::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_native_adapter::root_scalar_leaf_codec::{
    NativeScalarLeafEncoder, NativeScalarLeafError,
};
use novarocks_result_contract::{ScalarField, ScalarSchema, ScalarTimestampUnit, ScalarValueType};
use novarocks_result_render::RenderTurnStatus;
use novarocks_types::SlotId;
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::sync::Arc;

#[derive(Clone, Copy)]
struct Allocation {
    pointer: usize,
    family: u8,
}
const EMPTY: Allocation = Allocation {
    pointer: 0,
    family: 0,
};
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static FAMILY: Cell<u8> = const { Cell::new(0) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
    static REQUESTED: Cell<usize> = const { Cell::new(0) };
    static RECORDS: RefCell<[Allocation; 128]> = const { RefCell::new([EMPTY; 128]) };
}
fn allocated(pointer: *mut u8, size: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        CALLS.with(|v| v.set(v.get() + 1));
        REQUESTED.with(|v| v.set(v.get() + size));
        let family = FAMILY.with(Cell::get);
        // Failed attempts count requested bytes but own no new allocation.
        if family != 0 && !pointer.is_null() {
            RECORDS.with(|records| {
                let mut records = records.borrow_mut();
                let slot = records.iter_mut().find(|r| r.pointer == 0).unwrap();
                *slot = Allocation {
                    pointer: pointer as usize,
                    family,
                };
            });
        }
    }
}
fn freed(pointer: *mut u8) {
    let _ = RECORDS.try_with(|records| {
        if let Some(record) = records
            .borrow_mut()
            .iter_mut()
            .find(|r| r.pointer == pointer as usize)
        {
            *record = EMPTY;
        }
    });
}
struct Probe;
// SAFETY: Allocation operations delegate unchanged to System. Fixed TLS records
// neither allocate nor dereference the recorded pointers.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        allocated(pointer, layout.size());
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        allocated(pointer, layout.size());
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(pointer, layout, size) };
        // Failed realloc leaves the original backing live and owned.
        if !next.is_null() {
            freed(pointer);
        }
        allocated(next, size);
        next
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        freed(pointer);
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
struct Tracking;
impl Drop for Tracking {
    fn drop(&mut self) {
        TRACK.with(|v| v.set(false));
        FAMILY.with(|v| v.set(0));
    }
}
fn measure<T>(family: u8, operation: impl FnOnce() -> T) -> (T, usize, usize) {
    assert!(!TRACK.with(|v| v.replace(true)));
    FAMILY.with(|v| v.set(family));
    CALLS.with(|v| v.set(0));
    REQUESTED.with(|v| v.set(0));
    let tracking = Tracking;
    let result = operation();
    drop(tracking);
    (result, CALLS.with(Cell::get), REQUESTED.with(Cell::get))
}
fn no_allocation<T>(operation: impl FnOnce() -> T) -> T {
    let (result, calls, bytes) = measure(0, operation);
    assert_eq!((calls, bytes), (0, 0));
    result
}

fn fixture(wrong_last_byte: bool) -> (Chunk, Arc<ScalarSchema>) {
    let zone = "z".repeat(novarocks_type_contract::MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES);
    let array =
        Arc::new(TimestampNanosecondArray::from(vec![-7]).with_timezone(zone.clone())) as ArrayRef;
    let field = Field::new("value", array.data_type().clone(), true);
    let slot = ChunkSlotSchema::try_new_with_field(SlotId::new(7), field, None, None).unwrap();
    let schema = Arc::new(ChunkSchema::try_new(vec![slot]).unwrap());
    let batch = RecordBatch::try_new(schema.arrow_schema_ref(), vec![array]).unwrap();
    let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
    let mut expected_zone = zone.into_bytes();
    if wrong_last_byte {
        *expected_zone.last_mut().unwrap() = b'q';
    }
    let schema = Arc::new(
        ScalarSchema::try_new(ScalarField {
            nullable: true,
            value_type: ScalarValueType::Timestamp {
                unit: ScalarTimestampUnit::Nanosecond,
                timezone: Some(String::from_utf8(expected_zone).unwrap()),
            },
        })
        .unwrap()
        .bind_native_slots(&[7])
        .unwrap(),
    );
    (chunk, schema)
}
fn budget_and_credit() -> (Arc<ResultRetainedBudget>, ResultWriteCredit) {
    let bound = NativeScalarLeafEncoder::scratch_capacity_bytes();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(bound).unwrap());
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bound).unwrap() else {
        panic!("original cursor scratch pregrant");
    };
    (budget, credit)
}
struct PhysicalExit {
    family: u8,
    _credit: ResultWriteCredit,
}
impl Drop for PhysicalExit {
    fn drop(&mut self) {
        assert!(
            RECORDS.with(|records| records
                .borrow()
                .iter()
                .all(|r| r.pointer == 0 || r.family != self.family)),
            "cursor Box and columns Vec must physically exit before original credit"
        );
    }
}
struct FundedCursor {
    cursor: Box<NativeScalarLeafEncoder>,
    _exit: PhysicalExit,
}
fn begin(input: &Chunk, schema: Arc<ScalarSchema>, credit: ResultWriteCredit) -> FundedCursor {
    let (cursor, calls, requested) = measure(1, || {
        Box::new(
            NativeScalarLeafEncoder::try_begin(
                input,
                schema,
                NativeScalarLeafEncoder::scratch_capacity_bytes(),
            )
            .unwrap(),
        )
    });
    assert_eq!(calls, 2, "one cursor Box and one one-column columns Vec");
    assert_eq!(requested, NativeScalarLeafEncoder::scratch_capacity_bytes());
    FundedCursor {
        cursor,
        _exit: PhysicalExit {
            family: 1,
            _credit: credit,
        },
    }
}
fn close_cursor(owned: FundedCursor) {
    let FundedCursor { cursor, _exit } = owned;
    // Keep the original credit alive while measuring actual cursor destruction.
    // The existing Worker notification callback is separate control work.
    no_allocation(|| drop(cursor));
    drop(_exit);
}
fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(matches!(
        budget
            .try_reserve_process(NativeScalarLeafEncoder::scratch_capacity_bytes())
            .unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn returned(budget: &Arc<ResultRetainedBudget>) {
    let ResultWriteAdmission::Granted(credit) = budget
        .try_reserve_process(NativeScalarLeafEncoder::scratch_capacity_bytes())
        .unwrap()
    else {
        panic!("actual original scratch credit returned");
    };
    drop(credit);
}
#[test]
fn constructor_and_all_turns_fit_original_scratch_without_extra_allocation() {
    let oversized = Field::new(
        "value",
        arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            Some(
                "z".repeat(novarocks_type_contract::MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES + 1)
                    .into(),
            ),
        ),
        true,
    );
    let refusal = ChunkSlotSchema::try_new_with_field(SlotId::new(7), oversized, None, None)
        .expect_err("execution timezone keeps its original type bound");
    use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
    let expected = novarocks_type_contract::ExecutionValueTypeError::Invalid(
        "value layout timezone exceeds the byte limit",
    );
    assert_eq!(refusal.failure_fact(), expected.failure_fact());
    assert_eq!(refusal.failure_fact().class(), FailureClass::Invariant);
    let mut source = std::error::Error::source(&refusal);
    let mut original = None;
    while let Some(cause) = source {
        if let Some(cause) =
            cause.downcast_ref::<novarocks_type_contract::ExecutionValueTypeError>()
        {
            original = Some(cause);
            break;
        }
        source = cause.source();
    }
    assert_eq!(original, Some(&expected));
    let (input, schema) = fixture(false);
    let (budget, credit) = budget_and_credit();
    let mut owned = begin(&input, schema, credit);
    assert_eq!(owned.cursor.encoded_len(), None);
    held(&budget);
    for index in 0..1 {
        let mut untouched = [0xa5; 32];
        let turn = no_allocation(|| owned.cursor.step(&mut untouched).unwrap());
        assert_eq!(untouched, [0xa5; 32]);
        assert_eq!((turn.emitted_bytes, turn.completed_rows), (0, 0));
        assert_eq!(
            turn.examined_bytes,
            6 * novarocks_type_contract::MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES
        );
        assert!(turn.visited_cells <= 1024);
        assert_eq!(turn.status, RenderTurnStatus::Yielded);
        assert_eq!(owned.cursor.encoded_len().is_some(), index == 0);
    }
    let mut output = [0; 32];
    let turn = no_allocation(|| owned.cursor.step(&mut output).unwrap());
    assert_eq!((turn.emitted_bytes, turn.completed_rows), (32, 1));
    assert_eq!(&output[..4], b"SCV1");
    assert_eq!(&output[24..], &(-7_i64).to_le_bytes());
    held(&budget);
    close_cursor(owned);
    returned(&budget);
}
#[test]
fn scratch_and_length_preflight_refuse_before_any_cursor_allocation() {
    let (input, schema) = fixture(false);
    let (result, calls, bytes) = measure(0, || {
        NativeScalarLeafEncoder::try_begin(
            &input,
            schema,
            NativeScalarLeafEncoder::scratch_capacity_bytes() - 1,
        )
    });
    assert!(matches!(result, Err(NativeScalarLeafError::ScratchLimit)));
    assert_eq!((calls, bytes), (0, 0));
    let (input, schema) = fixture(false);
    let wrong = Arc::new(
        ScalarSchema::try_new(ScalarField {
            nullable: true,
            value_type: ScalarValueType::Timestamp {
                unit: ScalarTimestampUnit::Nanosecond,
                timezone: Some("UTC".to_owned()),
            },
        })
        .unwrap()
        .bind_native_slots(&[7])
        .unwrap(),
    );
    drop(schema);
    let (result, calls, bytes) = measure(0, || {
        NativeScalarLeafEncoder::try_begin(
            &input,
            wrong,
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
    });
    assert!(matches!(result, Err(NativeScalarLeafError::Type)));
    assert_eq!((calls, bytes), (0, 0));
}
#[test]
fn failed_validation_keeps_original_scratch_until_physical_cursor_exit() {
    let (input, schema) = fixture(true);
    let (budget, credit) = budget_and_credit();
    let mut owned = begin(&input, schema, credit);
    let mut output = [0xa5; 32];
    assert!(matches!(
        no_allocation(|| owned.cursor.step(&mut output)),
        Err(NativeScalarLeafError::Type)
    ));
    assert_eq!(output, [0xa5; 32]);
    assert_eq!(owned.cursor.encoded_len(), None);
    assert!(matches!(
        no_allocation(|| owned.cursor.step(&mut output)),
        Err(NativeScalarLeafError::Failed)
    ));
    held(&budget);
    close_cursor(owned);
    returned(&budget);
}
#[test]
fn cancellation_in_each_phase_frees_cursor_backings_before_original_credit() {
    for validation_complete in [false, true] {
        let (input, schema) = fixture(false);
        let weak_array = Arc::downgrade(input.record_batch().column(0));
        let weak_field = Arc::downgrade(input.chunk_schema().slots()[0].field_ref());
        let (budget, credit) = budget_and_credit();
        let mut owned = begin(&input, schema, credit);
        drop(input);
        if validation_complete {
            for _ in 0..1 {
                no_allocation(|| owned.cursor.step(&mut []).unwrap());
            }
            assert_eq!(owned.cursor.encoded_len(), Some(32));
        } else {
            assert_eq!(owned.cursor.encoded_len(), None);
        }
        assert!(weak_array.upgrade().is_some());
        assert!(weak_field.upgrade().is_some());
        held(&budget);
        close_cursor(owned);
        assert!(weak_array.upgrade().is_none());
        assert!(weak_field.upgrade().is_none());
        returned(&budget);
    }
}

fn empty_fixture(wrong_last_byte: bool) -> (Chunk, Arc<ScalarSchema>) {
    let (input, schema) = fixture(wrong_last_byte);
    // Slice preserves the actual timestamp carrier and timezone owners, while
    // making every selected-cell read invalid. Source fixtures are unmeasured.
    let batch = RecordBatch::try_new(
        Arc::clone(input.record_batch().schema_ref()),
        vec![input.record_batch().column(0).slice(0, 0)],
    )
    .unwrap();
    let empty = Chunk::try_new_with_chunk_schema(batch, input.chunk_schema_ref()).unwrap();
    (empty, schema)
}
fn begin_empty(
    input: &Chunk,
    schema: Arc<ScalarSchema>,
    credit: ResultWriteCredit,
) -> FundedCursor {
    let (cursor, calls, requested) = measure(2, || {
        Box::new(
            NativeScalarLeafEncoder::try_validate_empty(
                input,
                schema,
                NativeScalarLeafEncoder::scratch_capacity_bytes(),
            )
            .unwrap(),
        )
    });
    assert_eq!(
        calls, 2,
        "empty witness owns one cursor Box and one columns Vec"
    );
    assert_eq!(requested, NativeScalarLeafEncoder::scratch_capacity_bytes());
    FundedCursor {
        cursor,
        _exit: PhysicalExit {
            family: 2,
            _credit: credit,
        },
    }
}

#[test]
fn empty_schema_witness_emits_neither_value_nor_absent_record() {
    let (input, schema) = empty_fixture(false);
    let (budget, credit) = budget_and_credit();
    let mut owned = begin_empty(&input, schema, credit);
    assert_eq!(owned.cursor.encoded_len(), None);
    let mut untouched = [0xa5; 32];
    for index in 0..1 {
        let output = if index % 2 == 0 {
            &mut untouched[..]
        } else {
            &mut []
        };
        let turn = no_allocation(|| owned.cursor.step(output).unwrap());
        assert_eq!(untouched, [0xa5; 32]);
        assert_eq!((turn.emitted_bytes, turn.completed_rows), (0, 0));
        assert_eq!(
            turn.examined_bytes,
            6 * novarocks_type_contract::MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES
        );
        assert!(turn.visited_cells <= 1024);
        assert_eq!(
            turn.status,
            if index == 0 {
                RenderTurnStatus::InputComplete
            } else {
                RenderTurnStatus::Yielded
            }
        );
        assert_eq!(
            owned.cursor.encoded_len(),
            if index == 0 { Some(0) } else { None }
        );
        held(&budget);
    }
    for _ in 0..2 {
        let turn = no_allocation(|| owned.cursor.step(&mut untouched).unwrap());
        assert_eq!(turn.status, RenderTurnStatus::InputComplete);
        assert_eq!(
            (
                turn.emitted_bytes,
                turn.examined_bytes,
                turn.visited_cells,
                turn.completed_rows
            ),
            (0, 0, 0, 0)
        );
        assert_eq!(untouched, [0xa5; 32]);
    }
    close_cursor(owned);
    returned(&budget);
}

#[test]
fn empty_preflight_preserves_value_gate_and_refuses_shape_slot_carrier_and_scratch() {
    let (empty, schema) = empty_fixture(false);
    let error = no_allocation(|| {
        NativeScalarLeafEncoder::try_begin(
            &empty,
            Arc::clone(&schema),
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
    });
    assert!(matches!(error, Err(NativeScalarLeafError::EmptyInput)));
    let (value, _) = fixture(false);
    let error = no_allocation(|| {
        NativeScalarLeafEncoder::try_validate_empty(
            &value,
            Arc::clone(&schema),
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
    });
    assert!(matches!(error, Err(NativeScalarLeafError::Shape)));
    let error = no_allocation(|| {
        NativeScalarLeafEncoder::try_validate_empty(
            &empty,
            Arc::clone(&schema),
            NativeScalarLeafEncoder::scratch_capacity_bytes() - 1,
        )
    });
    assert!(matches!(error, Err(NativeScalarLeafError::ScratchLimit)));
    let wrong_slot = Arc::new(
        ScalarSchema::try_new(schema.field().clone())
            .unwrap()
            .bind_native_slots(&[8])
            .unwrap(),
    );
    let error = no_allocation(|| {
        NativeScalarLeafEncoder::try_validate_empty(
            &empty,
            wrong_slot,
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
    });
    assert!(matches!(error, Err(NativeScalarLeafError::Slot)));
    let wrong_carrier = Arc::new(
        ScalarSchema::try_new(ScalarField {
            nullable: true,
            value_type: ScalarValueType::SignedInteger(64),
        })
        .unwrap()
        .bind_native_slots(&[7])
        .unwrap(),
    );
    let error = no_allocation(|| {
        NativeScalarLeafEncoder::try_validate_empty(
            &empty,
            wrong_carrier,
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
    });
    assert!(matches!(error, Err(NativeScalarLeafError::Type)));
    let mut wrong_nullable_field = schema.field().clone();
    wrong_nullable_field.nullable = false;
    let wrong_nullable = Arc::new(
        ScalarSchema::try_new(wrong_nullable_field)
            .unwrap()
            .bind_native_slots(&[7])
            .unwrap(),
    );
    let error = no_allocation(|| {
        NativeScalarLeafEncoder::try_validate_empty(
            &empty,
            wrong_nullable,
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
    });
    assert!(matches!(error, Err(NativeScalarLeafError::Type)));
    let wrong_length = Arc::new(
        ScalarSchema::try_new(ScalarField {
            nullable: true,
            value_type: ScalarValueType::Timestamp {
                unit: ScalarTimestampUnit::Nanosecond,
                timezone: Some("UTC".to_owned()),
            },
        })
        .unwrap()
        .bind_native_slots(&[7])
        .unwrap(),
    );
    let error = no_allocation(|| {
        NativeScalarLeafEncoder::try_validate_empty(
            &empty,
            wrong_length,
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
    });
    assert!(matches!(error, Err(NativeScalarLeafError::Type)));
}

#[test]
fn empty_zone_mismatch_latches_failure_without_emission_or_credit_return() {
    let (input, schema) = empty_fixture(true);
    let (budget, credit) = budget_and_credit();
    let mut owned = begin_empty(&input, schema, credit);
    let mut untouched = [0xa5; 32];
    assert!(matches!(
        no_allocation(|| owned.cursor.step(&mut untouched)),
        Err(NativeScalarLeafError::Type)
    ));
    assert_eq!(owned.cursor.encoded_len(), None);
    assert!(matches!(
        no_allocation(|| owned.cursor.step(&mut [])),
        Err(NativeScalarLeafError::Failed)
    ));
    assert_eq!(untouched, [0xa5; 32]);
    held(&budget);
    close_cursor(owned);
    returned(&budget);
}

#[test]
fn empty_cancellation_before_and_after_witness_frees_backing_before_credit() {
    for validated in [false, true] {
        let (input, schema) = empty_fixture(false);
        let weak_array = Arc::downgrade(input.record_batch().column(0));
        let weak_field = Arc::downgrade(input.chunk_schema().slots()[0].field_ref());
        let (budget, credit) = budget_and_credit();
        let mut owned = begin_empty(&input, schema, credit);
        drop(input);
        for _ in 0..if validated { 1 } else { 0 } {
            no_allocation(|| owned.cursor.step(&mut []).unwrap());
        }
        assert_eq!(
            owned.cursor.encoded_len(),
            if validated { Some(0) } else { None }
        );
        assert!(weak_array.upgrade().is_some());
        assert!(weak_field.upgrade().is_some());
        held(&budget);
        close_cursor(owned);
        assert!(weak_array.upgrade().is_none());
        assert!(weak_field.upgrade().is_none());
        returned(&budget);
    }
}
