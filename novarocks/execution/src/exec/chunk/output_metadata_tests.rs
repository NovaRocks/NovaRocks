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

//! Original container funding, publication refusal and actual last-owner exit.
//! Key buffer custody is a separate lane. These local declared Arrow/tracker
//! inputs need no service; Execution owns this short affected-change lane.
//! The oracle is the real host limit, typed cause, known requested layouts,
//! weak payload lifetime and tracker convergence, rather than RSS or timing.

use super::*;
use crate::exec::chunk::root_schema_backing::RootSchemaInspection;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use crate::exec::operators::compiled_aggregate::expression_allocation_host;
use crate::runtime::mem_tracker::MemTracker;
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field};
use novarocks_functions::opaque_memory::OpaqueAllocationHost;
use novarocks_type_contract::{
    TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
};
use novarocks_types::SlotId;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, Weak};
use std::time::Duration;

struct Control {
    calls: AtomicUsize,
    refuse_at: Option<usize>,
}
impl Control {
    fn running() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            refuse_at: None,
        }
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        let ordinal = self.calls.fetch_add(1, Ordering::SeqCst);
        if Some(ordinal) == self.refuse_at {
            Err(KernelFailure::Cancelled)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("column metadata never waits")
    }
}

fn schema() -> crate::exec::chunk::ChunkSchemaRef {
    let storage = ValueTypeStorageLifetime::pure();
    let binding = TypedValueBinding::try_new(
        ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int64), true, storage.clone()).unwrap(),
        ValueLayout::try_new(&DataType::Int64, storage.clone()).unwrap(),
    )
    .unwrap();
    let slot = ChunkSlotSchema::try_new_typed(
        SlotId::new(1),
        Arc::new(Field::new("key", DataType::Int64, true)),
        binding,
        None,
        storage.clone(),
    )
    .unwrap();
    Arc::new(ChunkSchema::try_new_typed(vec![slot], storage).unwrap())
}
fn construction_owner(
    schema: &ChunkSchema,
) -> Box<dyn crate::exec::pipeline::operator::InputConstructionOwner> {
    let tracker = MemTracker::new_root("column-metadata-typed-construction");
    tracker.install_limit_once(64 * 1024 * 1024).unwrap();
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker,
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    ).unwrap();
    factory.begin_typed_construction(schema).unwrap()
}

fn paid_chunk(host: Arc<dyn AggregateStateAllocator>) -> Chunk {
    let output = schema();
    let metadata = OutputColumnMetadata::try_new(1, host, &Control::running()).unwrap();
    let mut columns = Vec::new();
    columns.try_reserve_exact(1).unwrap();
    columns.push(Arc::new(Int64Array::from(vec![Some(7), None])) as ArrayRef);
    let batch = RecordBatch::try_new(output.arrow_schema_ref(), columns).unwrap();
    let owner = construction_owner(&output);
    Chunk::try_new_with_output_column_metadata(batch, output, metadata, owner.as_ref()).unwrap()
}

#[test]
fn original_column_charge_is_shared_until_the_last_chunk_alias_exits() {
    let tracker = MemTracker::new_root("column-metadata-last-owner");
    let chunk = paid_chunk(expression_allocation_host(tracker.clone()));
    assert!(!chunk.source_authority().is_attached());
    let charged = tracker.current();
    let holder = SourceBackingOwner::<ColumnMetadataCharge>::owned_request_facts()
        .unwrap()
        .typed()
        .bytes();
    assert_eq!(
        charged,
        i64::try_from(Layout::array::<ArrayRef>(1).unwrap().size() + holder).unwrap(),
        "original Vec request and real cold holders are separately paid"
    );
    let clone = chunk.clone();
    let slice_owner = construction_owner(chunk.chunk_schema());
    let slice = chunk
        .slice_typed_observed::<crate::runtime::fragment::ExecutionFailure, _>(
            1,
            1,
            slice_owner.storage_lifetime(),
            |step| Ok(slice_owner.observe(0, &[], step)?),
        )
        .unwrap();
    drop(slice_owner);
    let like = Chunk::try_new_like(chunk.record_batch().slice(0, 1), &chunk).unwrap();
    assert_eq!(tracker.current(), charged, "sharing creates no new grant");
    drop(chunk);
    drop(clone);
    drop(slice);
    assert_eq!(tracker.current(), charged);
    std::thread::spawn(move || drop(like)).join().unwrap();
    assert_eq!(tracker.current(), 0);
}

#[test]
fn column_admission_refuses_overflow_and_the_real_host_limit_before_allocation() {
    let tracker = MemTracker::new_root("column-metadata-refusal");
    tracker.install_limit_once(1).unwrap();
    let host = expression_allocation_host(tracker.clone());
    assert!(matches!(
        OutputColumnMetadata::try_new(1, host.clone(), &Control::running()),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert_eq!(tracker.current(), 0);
    assert!(matches!(
        OutputColumnMetadata::try_new(usize::MAX, host, &Control::running()),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert_eq!(tracker.current(), 0);
}

#[test]
fn raw_column_capacity_is_paid_without_changing_the_logical_arity() {
    // The declared arrays are test inputs. This case qualifies the raw column
    // container only, not its Arrow payload or a new source authority.
    struct RawBatch {
        batch: RecordBatch,
        _columns: OpaqueRetainedCharge,
    }
    let tracker = MemTracker::new_root("raw-column-capacity");
    let host = Arc::new(DropOrderHost {
        original: expression_allocation_host(tracker.clone()),
        weak: Mutex::new(None),
        armed: AtomicBool::new(false),
        observed: AtomicBool::new(false),
        column_bytes: AtomicUsize::new(0),
    });
    let mut columns = Vec::new();
    columns.try_reserve_exact(7).unwrap();
    let array = Arc::new(Int64Array::from(vec![Some(7), None]));
    *host.weak.lock().unwrap() = Some(Arc::downgrade(&array));
    columns.push(array as ArrayRef);
    let capacity = columns.capacity();
    let batch = RecordBatch::try_new(schema().arrow_schema_ref(), columns).unwrap();
    let (arrow_schema, columns, rows) = batch.into_parts();
    assert_eq!(
        columns.capacity(),
        capacity,
        "the actual moved Vec is retained"
    );
    let charge = retain_column_capacity(columns.capacity(), host.clone()).unwrap();
    assert_eq!(
        charge.bytes(),
        Layout::array::<ArrayRef>(capacity).unwrap().size()
    );
    host.column_bytes.store(charge.bytes(), Ordering::SeqCst);
    host.armed.store(true, Ordering::SeqCst);
    let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(rows));
    let batch = RecordBatch::try_new_with_options(arrow_schema, columns, &options).unwrap();
    // Sharing here belongs only to the test; production uses the one paid
    // FilteredBatchBacking, whose raw batch likewise precedes this charge.
    let raw = Arc::new(RawBatch {
        batch,
        _columns: charge,
    });
    assert_eq!(raw.batch.num_columns(), 1);
    let charged = tracker.current();
    let last = raw.clone();
    drop(raw);
    assert_eq!(tracker.current(), charged);
    std::thread::spawn(move || drop(last)).join().unwrap();
    assert!(host.observed.load(Ordering::SeqCst));
    assert_eq!(tracker.current(), 0);
}

#[test]
fn raw_column_capacity_rejects_overflow_and_preserves_actual_empty_capacity() {
    let tracker = MemTracker::new_root("raw-column-invalid-capacity");
    let host = expression_allocation_host(tracker.clone());
    assert!(matches!(
        retain_column_capacity(usize::MAX, host.clone()),
        Err(KernelFailure::ResourceExhausted)
    ));
    let empty = Vec::<ArrayRef>::new();
    let zero = retain_column_capacity(empty.capacity(), host).unwrap();
    assert_eq!(zero.bytes(), 0);
    assert_eq!(tracker.current(), 0);
    drop(zero);
}

#[test]
fn late_column_owner_cancellation_destroys_the_unpublished_charge() {
    let tracker = MemTracker::new_root("column-metadata-late-cancel");
    let host = expression_allocation_host(tracker.clone());
    let running = Control::running();
    drop(OutputColumnMetadata::try_new(1, host.clone(), &running).unwrap());
    let total = running.calls.load(Ordering::SeqCst);
    assert!(total > 0);
    // Observe the original continuation, then refuse its final completion;
    // there is no fixed callback count or product test hook.
    let cancel = Control {
        calls: AtomicUsize::new(0),
        refuse_at: Some(total - 1),
    };
    assert!(matches!(
        OutputColumnMetadata::try_new(1, host, &cancel),
        Err(KernelFailure::Cancelled)
    ));
    assert_eq!(tracker.current(), 0);
}

/// Only this test's release observation is added to the real production host.
/// It asserts payload teardown before the retained container charge exits.
struct DropOrderHost {
    original: Arc<dyn AggregateStateAllocator>,
    weak: Mutex<Option<Weak<Int64Array>>>,
    armed: AtomicBool,
    observed: AtomicBool,
    column_bytes: AtomicUsize,
}
impl OpaqueAllocationHost for DropOrderHost {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        self.original
            .opaque_allocation_host()
            .unwrap()
            .reserve_opaque(bytes)
    }
    fn release_opaque(&self, bytes: usize) {
        if self.armed.load(Ordering::SeqCst) && bytes == self.column_bytes.load(Ordering::SeqCst) {
            assert!(
                self.weak
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .is_none(),
                "column payload outlived its container charge"
            );
            self.observed.store(true, Ordering::SeqCst);
        }
        self.original
            .opaque_allocation_host()
            .unwrap()
            .release_opaque(bytes);
    }
}
impl AggregateStateAllocator for DropOrderHost {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        self.original.allocate(layout)
    }
    unsafe fn release(&self, block: NonNull<u8>, layout: Layout) {
        // SAFETY: this host forwards the exact original block and layout.
        unsafe { self.original.release(block, layout) };
    }
}

fn publication(
    host: Arc<DropOrderHost>,
    metadata_columns: usize,
) -> crate::runtime::fragment::ExecutionResult<Chunk> {
    let output = schema();
    let metadata =
        OutputColumnMetadata::try_new(metadata_columns, host.clone(), &Control::running())?;
    let mut columns = Vec::new();
    columns.try_reserve_exact(metadata_columns).unwrap();
    let array = Arc::new(Int64Array::from(vec![7]));
    *host.weak.lock().unwrap() = Some(Arc::downgrade(&array));
    columns.push(array as ArrayRef);
    let batch = RecordBatch::try_new(output.arrow_schema_ref(), columns).unwrap();
    host.column_bytes.store(
        Layout::array::<ArrayRef>(metadata_columns).unwrap().size(),
        Ordering::SeqCst,
    );
    host.armed.store(true, Ordering::SeqCst);
    let owner = construction_owner(&output);
    Chunk::try_new_with_output_column_metadata(batch, output, metadata, owner.as_ref())
}

#[test]
fn published_container_destroys_its_payload_before_releasing_the_charge() {
    let tracker = MemTracker::new_root("column-metadata-drop-order");
    let host = Arc::new(DropOrderHost {
        original: expression_allocation_host(tracker.clone()),
        weak: Mutex::new(None),
        armed: AtomicBool::new(false),
        observed: AtomicBool::new(false),
        column_bytes: AtomicUsize::new(0),
    });
    drop(publication(host.clone(), 1).unwrap());
    assert!(host.observed.load(Ordering::SeqCst));
    assert_eq!(tracker.current(), 0);
}

#[test]
fn unpublished_container_refusal_drops_the_batch_and_all_original_charges() {
    let tracker = MemTracker::new_root("column-metadata-publication-refusal");
    let host = Arc::new(DropOrderHost {
        original: expression_allocation_host(tracker.clone()),
        weak: Mutex::new(None),
        armed: AtomicBool::new(false),
        observed: AtomicBool::new(false),
        column_bytes: AtomicUsize::new(0),
    });
    assert!(matches!(
        publication(host.clone(), 2),
        Err(failure) if matches!(failure.cause(), crate::runtime::fragment::ExecutionFailureCause::Kernel(KernelFailure::InvalidProgram(_)))
    ));
    assert!(
        host.weak
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .is_none()
    );
    assert!(host.observed.load(Ordering::SeqCst));
    assert_eq!(tracker.current(), 0);
}

#[test]
fn pure_values_refuses_a_runtime_keeper_and_preserves_plain_values() {
    let tracker = MemTracker::new_root("column-metadata-values");
    let chunk = paid_chunk(expression_allocation_host(tracker.clone()));
    assert_eq!(
        chunk.try_into_static_batch().unwrap_err(),
        "StaticValues cannot retain runtime output column metadata"
    );
    assert_eq!(tracker.current(), 0);
    let plain_schema = schema();
    let plain = Chunk::try_new_with_chunk_schema(
        RecordBatch::new_empty(plain_schema.arrow_schema_ref()),
        plain_schema,
    )
    .unwrap();
    assert_eq!(plain.try_into_static_batch().unwrap().num_columns(), 1);
}

#[test]
fn fixed_root_inspection_counts_known_holders_without_granting_payload_authority() {
    let tracker = MemTracker::new_root("column-metadata-root-holder");
    let chunk = paid_chunk(expression_allocation_host(tracker.clone()));
    let metadata = OutputColumnMetadata::try_new(
        1,
        expression_allocation_host(tracker.clone()),
        &Control::running(),
    )
    .unwrap();
    let holder = metadata.holder_bytes().unwrap();
    drop(metadata);
    let mut inspection = RootSchemaInspection::new(1 << 20, 32, 8);
    chunk
        .inspect_root_holder_scaffolds(&mut inspection)
        .unwrap();
    // This fixture owns an ordinary Arc<RecordBatch>: its two counters and
    // wrapper are known scaffolds, independent of any Arrow payload buffers.
    let batch_holder = Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::new::<RecordBatch>())
        .unwrap()
        .0
        .pad_to_align()
        .size();
    assert_eq!(
        inspection.bytes(),
        std::mem::size_of::<Chunk>() + batch_holder + holder
    );
    assert!(!chunk.source_authority().is_attached());
    drop(chunk);
    assert_eq!(tracker.current(), 0);
}
