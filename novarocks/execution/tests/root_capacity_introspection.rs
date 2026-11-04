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

use arrow::array::{ArrayRef, Int32Array, StructArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::Arc;

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
    static BYTES: Cell<usize> = const { Cell::new(0) };
}
struct AllocationProbe;
// SAFETY: The wrapper delegates every allocator operation unchanged and only
// observes a thread-local counter, without allocating inside the observation.
unsafe impl GlobalAlloc for AllocationProbe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACK.try_with(Cell::get).unwrap_or(false) {
            let _ = CALLS.try_with(|c| c.set(c.get() + 1));
            let _ = BYTES.try_with(|b| b.set(b.get() + layout.size()));
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if TRACK.try_with(Cell::get).unwrap_or(false) {
            let _ = CALLS.try_with(|c| c.set(c.get() + 1));
        }
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if TRACK.try_with(Cell::get).unwrap_or(false) {
            let _ = CALLS.try_with(|c| c.set(c.get() + 1));
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: AllocationProbe = AllocationProbe;
fn allocations<T>(f: impl FnOnce() -> T) -> (T, usize, usize) {
    CALLS.with(|c| c.set(0));
    BYTES.with(|b| b.set(0));
    TRACK.with(|c| c.set(true));
    let value = f();
    TRACK.with(|c| c.set(false));
    (value, CALLS.with(Cell::get), BYTES.with(Cell::get))
}
fn no_allocation<T>(f: impl FnOnce() -> T) -> T {
    CALLS.with(|c| c.set(0));
    TRACK.with(|c| c.set(true));
    let value = f();
    TRACK.with(|c| c.set(false));
    assert_eq!(
        CALLS.with(Cell::get),
        0,
        "a borrowed capacity proof must not allocate"
    );
    value
}
#[test]
fn standard_buffer_slice_reports_complete_allocator_layout_without_copy() {
    let mut values = Vec::<u8>::with_capacity(8192);
    values.extend_from_slice(b"payload");
    let capacity = values.capacity();
    let buffer = Buffer::from_vec(values);
    let alias = buffer.slice_with_length(2, 1);
    assert_eq!(alias.len(), 1);
    assert_eq!(no_allocation(|| alias.capacity()), capacity);
    assert_eq!(no_allocation(|| buffer.capacity()), capacity);
    assert_eq!(alias.data_ptr(), buffer.data_ptr());
}
/// A custom allocation reports only the region it declares. The complete
/// backing stays with the owner that created it, which accounts for it.
#[test]
fn custom_buffer_reports_only_its_declared_region() {
    let mut backing = Vec::<u8>::with_capacity(8192);
    backing.push(7);
    let pointer = NonNull::new(backing.as_mut_ptr()).unwrap();
    let owner = Arc::new(backing);
    let weak = Arc::downgrade(&owner);
    // SAFETY: owner retains the initialized one-byte region at pointer and
    // its complete backing until the last custom buffer alias is dropped.
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, 1, owner) };
    let alias = buffer.clone();
    assert_eq!(no_allocation(|| buffer.capacity()), 1);
    assert_eq!(no_allocation(|| alias.capacity()), 1);
    drop(buffer);
    assert!(weak.upgrade().is_some());
    drop(alias);
    assert!(weak.upgrade().is_none());
}

fn storage_limits(bytes: usize) -> novarocks_execution::exec::chunk::RootArrayStorageLimits {
    novarocks_execution::exec::chunk::RootArrayStorageLimits {
        bytes,
        nodes: 65536,
        depth: 64,
    }
}

#[test]
fn borrowed_ipc_batch_counts_shared_full_backing_without_allocating() {
    use arrow::array::{Array, StringArray};
    use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
    use novarocks_execution::exec::chunk::{
        ARROW_BUFFER_OWNER_METADATA_BOUND, RootArrayStorageError, borrowed_root_batch_storage,
    };
    let value = "q".repeat(1024 * 1024);
    let fields: Vec<_> = (0..17)
        .map(|i| Field::new(format!("c{i}"), DataType::Utf8, false))
        .collect();
    let columns: Vec<_> = (0..17)
        .map(|_| Arc::new(StringArray::from(vec![value.as_str()])) as ArrayRef)
        .collect();
    let original = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let mut wire = Vec::new();
    let mut writer = StreamWriter::try_new(&mut wire, &original.schema()).unwrap();
    writer.write(&original).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let batch = StreamReader::try_new(std::io::Cursor::new(wire), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let first = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let backing = first.values().capacity();
    assert!(backing >= 17 * 1024 * 1024);
    for column in batch.columns() {
        let column = column.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(column.values().data_ptr(), first.values().data_ptr());
        assert_eq!(
            column.offsets().inner().inner().data_ptr(),
            first.values().data_ptr()
        );
    }
    let measured =
        no_allocation(|| borrowed_root_batch_storage(&batch, storage_limits(96 * 1024 * 1024)))
            .expect("one legal shared IPC body must fit the original backing allowance");
    assert!(measured >= backing + 34 * ARROW_BUFFER_OWNER_METADATA_BOUND);
    assert!(measured < backing + 64 * 1024);
    assert_eq!(
        no_allocation(|| borrowed_root_batch_storage(&batch, storage_limits(backing - 1))),
        Err(RootArrayStorageError::CapacityExceeded)
    );

    // Production receiver custody pins every original Buffer but declares
    // only its initialized base prefix. Shared IPC slices consequently keep
    // the same base with different capacities, unlike the raw reader above.
    use novarocks_execution::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use novarocks_execution::exec::pipeline::{
        operator::input_construction::try_runtime_input_construction_factory,
        pipeline::representation_construction_profile,
    };
    use novarocks_execution::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
    let storage = ValueTypeStorageLifetime::pure();
    let value_type = FunctionValueType::new(DataType::Utf8, false);
    let slots = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            ChunkSlotSchema::try_new_typed(
                novarocks_types::SlotId::new(u32::try_from(ordinal).unwrap()),
                Arc::clone(field),
                value_type.typed_binding().clone(),
                None,
                storage.clone(),
            )
            .unwrap()
        })
        .collect();
    let declared = ChunkSchema::try_new_typed(slots, storage).unwrap();
    let factory = try_runtime_input_construction_factory(
        representation_construction_profile().unwrap(),
        MemTracker::new_root("IPC root custody regression"),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let owner = factory.begin_typed_construction(&declared).unwrap();
    let columns = batch
        .columns()
        .iter()
        .map(|array| {
            factory
                .retain_decoded_source(Arc::clone(array), owner.storage_lifetime(), owner.as_ref())
                .unwrap()
        })
        .collect();
    let retained = RecordBatch::try_new(batch.schema(), columns).unwrap();
    let base = first.values().data_ptr();
    let mut previous = 0;
    for array in retained.columns() {
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        for buffer in [array.offsets().inner().inner(), array.values()] {
            assert_eq!(buffer.data_ptr(), base);
            assert!(buffer.capacity() > previous);
            previous = buffer.capacity();
        }
    }
    assert_eq!(previous, backing);
    let forward =
        no_allocation(|| borrowed_root_batch_storage(&retained, storage_limits(96 * 1024 * 1024)))
            .expect("custody's same-base prefixes must fit the original backing allowance");
    assert!(forward >= backing + 34 * ARROW_BUFFER_OWNER_METADATA_BOUND);
    assert!(forward < backing + 64 * 1024);
    assert_eq!(
        no_allocation(|| borrowed_root_batch_storage(&retained, storage_limits(forward - 1))),
        Err(RootArrayStorageError::CapacityExceeded)
    );
    let reversed = RecordBatch::try_new(
        retained.schema(),
        retained.columns().iter().rev().cloned().collect(),
    )
    .unwrap();
    let backward =
        no_allocation(|| borrowed_root_batch_storage(&reversed, storage_limits(96 * 1024 * 1024)))
            .unwrap();
    assert_eq!(backward, forward);
    assert_eq!(
        no_allocation(|| borrowed_root_batch_storage(&reversed, storage_limits(backing - 1))),
        Err(RootArrayStorageError::CapacityExceeded)
    );
}

#[test]
fn borrowed_batch_rejects_independent_sliced_away_backings() {
    use arrow::array::StringArray;
    use arrow::buffer::OffsetBuffer;
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_batch_storage};
    let fields: Vec<_> = (0..17)
        .map(|i| Field::new(format!("c{i}"), DataType::Utf8, false))
        .collect();
    let columns: Vec<_> = (0..17)
        .map(|_| {
            let mut bytes = Vec::with_capacity(6 * 1024 * 1024);
            bytes.push(b'q');
            Arc::new(StringArray::new(
                OffsetBuffer::new(vec![0_i32, 1].into()),
                Buffer::from_vec(bytes),
                None,
            )) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    assert_eq!(
        no_allocation(|| borrowed_root_batch_storage(&batch, storage_limits(96 * 1024 * 1024))),
        Err(RootArrayStorageError::CapacityExceeded)
    );
}

#[test]
fn fixed_backing_cache_overflow_remains_conservative_without_allocating() {
    use arrow::buffer::ScalarBuffer;
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_batch_storage};
    let mut columns: Vec<_> = (0..65)
        .map(|_| {
            let mut values = Vec::with_capacity(1024);
            values.push(7_i32);
            Arc::new(Int32Array::new(ScalarBuffer::from(values), None)) as ArrayRef
        })
        .collect();
    columns.push(columns[64].clone());
    columns.push(columns[64].clone());
    let fields: Vec<_> = (0..columns.len())
        .map(|i| Field::new(format!("c{i}"), DataType::Int32, false))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let measured =
        no_allocation(|| borrowed_root_batch_storage(&batch, storage_limits(1024 * 1024))).unwrap();
    assert!(
        measured >= 67 * 4096,
        "uncached aliases must not escape accounting"
    );
    assert_eq!(
        no_allocation(|| borrowed_root_batch_storage(&batch, storage_limits(67 * 4096 - 1))),
        Err(RootArrayStorageError::CapacityExceeded)
    );
}

#[test]
fn borrowed_storage_rejects_sliced_away_standard_capacity_without_cell_scan() {
    use arrow::buffer::ScalarBuffer;
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_array_storage};
    let mut backing = Vec::<i32>::with_capacity(32768);
    backing.push(42);
    let capacity = backing.capacity() * std::mem::size_of::<i32>();
    let array = Int32Array::new(ScalarBuffer::from(backing), None).slice(0, 1);
    assert_eq!(array.len(), 1);
    let bytes =
        no_allocation(|| borrowed_root_array_storage(&array, storage_limits(capacity + 4096)))
            .unwrap();
    assert!(bytes >= capacity);
    assert_eq!(
        no_allocation(|| { borrowed_root_array_storage(&array, storage_limits(capacity - 1)) }),
        Err(RootArrayStorageError::CapacityExceeded)
    );
}

#[test]
fn borrowed_storage_charges_custom_buffer_declared_region_without_copy() {
    use arrow::buffer::ScalarBuffer;
    use novarocks_execution::exec::chunk::{
        ARROW_BUFFER_OWNER_METADATA_BOUND, borrowed_root_array_storage,
    };
    let mut backing = Vec::<i32>::with_capacity(32768);
    backing.push(42);
    let backing_bytes = backing.capacity() * std::mem::size_of::<i32>();
    let pointer = NonNull::new(backing.as_mut_ptr().cast::<u8>()).unwrap();
    let owner = Arc::new(backing);
    let weak = Arc::downgrade(&owner);
    // SAFETY: The owner retains the initialized aligned i32 value and all
    // backing until the final array/buffer alias exits.
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, 4, owner) };
    let array = Int32Array::new(ScalarBuffer::new(buffer, 0, 1), None);
    let bytes =
        no_allocation(|| borrowed_root_array_storage(&array, storage_limits(96 * 1024 * 1024)))
            .unwrap();
    // The declared region and the owner record are charged; the complete
    // backing is the source owner's responsibility.
    assert!(bytes >= 4 + ARROW_BUFFER_OWNER_METADATA_BOUND);
    assert!(bytes < backing_bytes);
    assert!(weak.upgrade().is_some());
    drop(array);
    assert!(weak.upgrade().is_none());
}

#[test]
fn dictionary_storage_includes_unreferenced_complete_values_backing() {
    use arrow::array::{DictionaryArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Int32Type;
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_array_storage};
    let mut backing = Vec::<u8>::with_capacity(1024 * 1024);
    backing.extend_from_slice(b"aunused");
    let capacity = backing.capacity();
    let values = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0_i32, 1, 7].into()),
        Buffer::from_vec(backing),
        None,
    )) as ArrayRef;
    let dictionary =
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), values).unwrap();
    let bytes =
        no_allocation(|| borrowed_root_array_storage(&dictionary, storage_limits(capacity + 4096)))
            .unwrap();
    assert!(bytes >= capacity);
    assert_eq!(
        no_allocation(|| {
            borrowed_root_array_storage(&dictionary, storage_limits(capacity - 1))
        }),
        Err(RootArrayStorageError::CapacityExceeded)
    );
}

#[test]
fn borrowed_storage_counts_struct_columns_and_stops_at_finite_work() {
    use novarocks_execution::exec::chunk::{
        RootArrayStorageError, RootArrayStorageLimits, borrowed_root_array_storage,
    };
    let columns = vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef];
    let column_bytes = columns.len() * std::mem::size_of::<ArrayRef>();
    let structure = StructArray::try_new(
        vec![Arc::new(Field::new("v", DataType::Int32, false))].into(),
        columns,
        None,
    )
    .unwrap();
    let bytes =
        no_allocation(|| borrowed_root_array_storage(&structure, storage_limits(4096))).unwrap();
    assert!(bytes >= column_bytes);
    assert_eq!(
        no_allocation(|| borrowed_root_array_storage(&structure, storage_limits(bytes - 1))),
        Err(RootArrayStorageError::CapacityExceeded)
    );
    for (nodes, depth) in [(1, 64), (65536, 0)] {
        assert_eq!(
            no_allocation(|| {
                borrowed_root_array_storage(
                    &structure,
                    RootArrayStorageLimits {
                        bytes: 4096,
                        nodes,
                        depth,
                    },
                )
            }),
            Err(RootArrayStorageError::WorkExceeded)
        );
    }
}

#[test]
fn borrowed_storage_includes_list_child_and_sliced_null_backing() {
    use arrow::array::ListArray;
    use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
    use novarocks_execution::exec::chunk::borrowed_root_array_storage;
    let mut values = Vec::<i32>::with_capacity(8192);
    values.push(5);
    let values_capacity = values.capacity() * std::mem::size_of::<i32>();
    let mut validity = Vec::<u8>::with_capacity(8192);
    validity.push(0xff);
    let null_capacity = validity.capacity();
    let nulls = NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(validity), 0, 1));
    let values = Arc::new(Int32Array::new(ScalarBuffer::from(values), Some(nulls))) as ArrayRef;
    let list = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(vec![0_i32, 1].into()),
        values,
        None,
    )
    .unwrap();
    let bytes = no_allocation(|| {
        borrowed_root_array_storage(
            &list,
            storage_limits(values_capacity + null_capacity + 4096),
        )
    })
    .unwrap();
    assert!(bytes >= values_capacity + null_capacity);
}

#[test]
fn storage_inspection_does_not_accept_unclosed_view_carriers() {
    use arrow::array::StringViewArray;
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_array_storage};
    let view = StringViewArray::from(vec!["value"]);
    assert_eq!(
        no_allocation(|| { borrowed_root_array_storage(&view, storage_limits(96 * 1024 * 1024)) }),
        Err(RootArrayStorageError::UnsupportedCarrier)
    );
}

#[test]
fn physical_map_entries_do_not_spend_an_extra_semantic_depth_level() {
    use arrow::array::{Array, MapArray};
    use arrow::buffer::OffsetBuffer;
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_array_storage};
    let mut child = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    for _ in 0..64 {
        let fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", child.data_type().clone(), false)),
        ]
        .into();
        let entries = StructArray::try_new(
            fields,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef, child],
            None,
        )
        .unwrap();
        child = Arc::new(
            MapArray::try_new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new(vec![0_i32, 1].into()),
                entries,
                None,
                false,
            )
            .unwrap(),
        );
    }
    let bytes =
        no_allocation(|| borrowed_root_array_storage(child.as_ref(), storage_limits(1024 * 1024)))
            .expect("64 legal semantic Map levels");
    assert!(bytes > 0);
    let mut limits = storage_limits(1024 * 1024);
    limits.depth = 63;
    assert_eq!(
        no_allocation(|| borrowed_root_array_storage(child.as_ref(), limits)),
        Err(RootArrayStorageError::WorkExceeded)
    );
}

#[test]
fn dictionary_physical_keys_do_not_spend_the_last_list_semantic_level() {
    use arrow::array::{DictionaryArray, ListArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Int32Type;
    use novarocks_execution::exec::chunk::borrowed_root_array_storage;
    let mut child = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0]),
            Arc::new(StringArray::from(vec!["value"])),
        )
        .unwrap(),
    ) as ArrayRef;
    for _ in 0..64 {
        child = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("item", child.data_type().clone(), false)),
                OffsetBuffer::new(vec![0_i32, 1].into()),
                child,
                None,
            )
            .unwrap(),
        );
    }
    no_allocation(|| borrowed_root_array_storage(child.as_ref(), storage_limits(1024 * 1024)))
        .expect("dictionary leaf at semantic depth 64");
}

#[test]
fn forwarded_as_any_cannot_hide_the_actual_outer_owner() {
    use arrow::array::Array;
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_array_storage};
    let inner = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    // Arrow's ArrayRef trait implementation forwards as_any to the concrete
    // array, but this borrowed wrapper is a distinct outer object. Callers
    // must inspect inner.as_ref(), or supply an explicit source-owner proof.
    let wrapper = &inner as &dyn Array;
    assert_eq!(
        no_allocation(|| borrowed_root_array_storage(wrapper, storage_limits(4096))),
        Err(RootArrayStorageError::CarrierOuterOwnerMismatch)
    );
    no_allocation(|| borrowed_root_array_storage(inner.as_ref(), storage_limits(4096))).unwrap();
}

#[test]
fn caller_cannot_relax_the_frozen_structural_work_ceiling() {
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_array_storage};
    let array = Int32Array::from(vec![1]);
    let mut limits = storage_limits(4096);
    limits.nodes = 0;
    assert_eq!(
        no_allocation(|| borrowed_root_array_storage(&array, limits)),
        Err(RootArrayStorageError::WorkExceeded)
    );
    // Array aliases stay alive; the fixed maximum prevents repeated references
    // from turning a single root inspection into unbounded structural work.
    let count = 2 * novarocks_result_contract::RootProfileV1::SCHEMA_TYPE_NODES;
    let children = vec![Arc::new(array) as ArrayRef; count];
    let fields = (0..count)
        .map(|_| Arc::new(Field::new("", DataType::Int32, false)))
        .collect::<Vec<_>>()
        .into();
    let structure = StructArray::try_new(fields, children, None).unwrap();
    limits.bytes = usize::MAX;
    limits.nodes = usize::MAX;
    limits.depth = usize::MAX;
    assert_eq!(
        no_allocation(|| borrowed_root_array_storage(&structure, limits)),
        Err(RootArrayStorageError::WorkExceeded)
    );
}

/// Pins `ARROW_BUFFER_OWNER_METADATA_BOUND` against the owner record upstream
/// arrow-buffer actually allocates when a Vec becomes an immutable Buffer.
/// A pool-enabled arrow-buffer build grows this record and fails here.
#[test]
fn buffer_owner_metadata_bound_covers_the_actual_owner_allocation() {
    use novarocks_execution::exec::chunk::ARROW_BUFFER_OWNER_METADATA_BOUND;
    let values = vec![1_u8; 64];
    let (buffer, calls, bytes) = allocations(move || Buffer::from_vec(values));
    assert_eq!(
        calls, 1,
        "the payload Vec is moved; only the owner is allocated"
    );
    assert!(
        bytes <= ARROW_BUFFER_OWNER_METADATA_BOUND,
        "owner record {bytes} exceeds its fixed bound"
    );
    assert_eq!(buffer.len(), 64);
}

#[test]
fn whole_batch_storage_counts_columns_and_shared_work_limit() {
    use novarocks_execution::exec::chunk::{
        RootArrayStorageError, RootArrayStorageLimits, borrowed_root_batch_storage,
    };
    let array = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
        vec![Arc::clone(&array)],
    )
    .unwrap();
    let bound =
        no_allocation(|| borrowed_root_batch_storage(&batch, storage_limits(4096))).unwrap();
    assert!(bound >= std::mem::size_of::<ArrayRef>());
    assert_eq!(
        no_allocation(|| borrowed_root_batch_storage(&batch, storage_limits(bound - 1))),
        Err(RootArrayStorageError::CapacityExceeded)
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ])),
        vec![Arc::clone(&array), array],
    )
    .unwrap();
    assert_eq!(
        no_allocation(|| borrowed_root_batch_storage(
            &batch,
            RootArrayStorageLimits {
                bytes: 4096,
                nodes: 1,
                depth: 64,
            }
        )),
        Err(RootArrayStorageError::WorkExceeded)
    );
    assert!(
        no_allocation(|| borrowed_root_batch_storage(
            &batch,
            RootArrayStorageLimits {
                bytes: 4096,
                nodes: 2,
                depth: 64,
            }
        ))
        .is_ok()
    );
}

#[test]
fn whole_batch_storage_rejects_wide_inputs_without_scanning_columns() {
    use novarocks_execution::exec::chunk::{RootArrayStorageError, borrowed_root_batch_storage};
    let schema = Schema::new(
        (0..=novarocks_result_contract::RootProfileV1::MAX_COLUMNS)
            .map(|i| Field::new(i.to_string(), DataType::Int32, false))
            .collect::<Vec<_>>(),
    );
    let batch = RecordBatch::new_empty(Arc::new(schema));
    assert_eq!(
        no_allocation(|| borrowed_root_batch_storage(&batch, storage_limits(96 * 1024 * 1024))),
        Err(RootArrayStorageError::WorkExceeded)
    );
}

fn owned_root_field(
    name: String,
    data_type: DataType,
) -> novarocks_types::arrow_metadata_owner::MetadataOwnedField {
    use novarocks_types::arrow_metadata_owner::{ArrowMetadataOwner, MetadataOwnerLimits};
    ArrowMetadataOwner::try_new(
        Vec::new(),
        MetadataOwnerLimits {
            entries: 65536,
            construction_bytes: 96 * 1024 * 1024,
        },
    )
    .unwrap()
    .into_field(name, data_type, false)
}

fn owned_root_schema(
    field: novarocks_types::arrow_metadata_owner::MetadataOwnedField,
    extras: Vec<novarocks_types::arrow_metadata_owner::MetadataOwnedField>,
) -> Arc<novarocks_execution::exec::chunk::ChunkSchema> {
    use novarocks_execution::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use novarocks_types::arrow_metadata_owner::FieldMetadataOrigins;
    let mut owners = vec![field.clone()];
    owners.extend(extras);
    let origins = FieldMetadataOrigins::try_new(owners, 65536).unwrap();
    let slot = ChunkSlotSchema::try_new_with_metadata_origins(
        novarocks_types::SlotId::new(1),
        Arc::clone(field.field()),
        origins,
        None,
        Some(7),
    )
    .unwrap();
    Arc::new(ChunkSchema::try_new(vec![slot]).unwrap())
}

#[test]
fn whole_chunk_proof_counts_name_and_columns_without_allocation() {
    use novarocks_execution::exec::chunk::{
        Chunk, RootArrayStorageError, borrowed_root_chunk_storage,
    };
    let mut name = String::with_capacity(512 * 1024);
    name.push_str("value");
    let name_capacity = name.capacity();
    let schema = owned_root_schema(owned_root_field(name, DataType::Int32), Vec::new());
    let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
    let columns = vec![array];
    let spare = columns.len() * std::mem::size_of::<ArrayRef>();
    let batch = RecordBatch::try_new(schema.arrow_schema_ref(), columns).unwrap();
    let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
    let bytes =
        no_allocation(|| borrowed_root_chunk_storage(&chunk, storage_limits(96 * 1024 * 1024)))
            .unwrap();
    assert!(bytes >= name_capacity + spare);
    assert_eq!(
        no_allocation(|| borrowed_root_chunk_storage(&chunk, storage_limits(name_capacity - 1))),
        Err(RootArrayStorageError::CapacityExceeded)
    );
}

#[test]
fn whole_chunk_proof_refuses_foreign_equal_schema_with_unknown_empty_table_history() {
    use novarocks_execution::exec::chunk::{
        Chunk, RootArrayStorageError, borrowed_root_chunk_storage,
    };
    let schema = owned_root_schema(
        owned_root_field("value".into(), DataType::Int32),
        Vec::new(),
    );
    let mut unknown = std::collections::HashMap::with_capacity(8192);
    unknown.insert("deleted".into(), "value".into());
    unknown.clear();
    let actual = Arc::new(Schema::new_with_metadata(
        schema.arrow_schema_ref().fields().clone(),
        unknown,
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&actual),
        vec![Arc::new(Int32Array::from(vec![7]))],
    )
    .unwrap();
    let chunk = Chunk::try_new_with_chunk_schema(batch, Arc::clone(&schema)).unwrap();
    assert!(Arc::ptr_eq(&chunk.record_batch().schema(), &actual));
    assert!(!Arc::ptr_eq(&actual, &schema.arrow_schema_ref()));
    assert_eq!(
        no_allocation(|| borrowed_root_chunk_storage(&chunk, storage_limits(96 * 1024 * 1024))),
        Err(RootArrayStorageError::UnknownMetadataOwner)
    );
}

#[test]
fn whole_chunk_proof_refuses_foreign_nested_array_field_even_with_known_batch_schema() {
    use novarocks_execution::exec::chunk::{
        Chunk, RootArrayStorageError, borrowed_root_chunk_storage,
    };
    let child = owned_root_field("child".into(), DataType::Int32);
    let parent = owned_root_field(
        "parent".into(),
        DataType::Struct(vec![Arc::clone(child.field())].into()),
    );
    let schema = owned_root_schema(parent, vec![child.clone()]);
    let mut unknown = std::collections::HashMap::with_capacity(8192);
    unknown.insert("deleted".into(), "value".into());
    unknown.clear();
    let foreign = Arc::new(Field::new("child", DataType::Int32, false).with_metadata(unknown));
    let array = StructArray::try_new(
        vec![foreign].into(),
        vec![Arc::new(Int32Array::from(vec![7]))],
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(schema.arrow_schema_ref(), vec![Arc::new(array)]).unwrap();
    let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
    assert_eq!(
        no_allocation(|| borrowed_root_chunk_storage(&chunk, storage_limits(96 * 1024 * 1024))),
        Err(RootArrayStorageError::UnknownMetadataOwner)
    );
}

#[test]
fn whole_chunk_proof_counts_unreachable_field_owners_kept_by_origin_indices() {
    use novarocks_execution::exec::chunk::{
        Chunk, RootArrayStorageError, borrowed_root_chunk_storage,
    };
    let mut spare = String::with_capacity(1024 * 1024);
    spare.push_str("unused");
    let root = owned_root_field("value".into(), DataType::Int32);
    let extra = owned_root_field(spare, DataType::Int64);
    let schema = owned_root_schema(root, vec![extra]);
    let batch = RecordBatch::try_new(
        schema.arrow_schema_ref(),
        vec![Arc::new(Int32Array::from(vec![7]))],
    )
    .unwrap();
    let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
    assert_eq!(
        no_allocation(|| borrowed_root_chunk_storage(&chunk, storage_limits(512 * 1024))),
        Err(RootArrayStorageError::CapacityExceeded)
    );
    let bytes =
        no_allocation(|| borrowed_root_chunk_storage(&chunk, storage_limits(96 * 1024 * 1024)))
            .unwrap();
    assert!(bytes >= 1024 * 1024);
}

#[test]
fn whole_chunk_proof_accepts_maximum_flat_dictionary_columns_with_shared_physical_owners() {
    use arrow::array::DictionaryArray;
    use arrow::datatypes::Int32Type;
    use novarocks_execution::exec::chunk::{
        Chunk, ChunkSchema, ChunkSlotSchema, borrowed_root_chunk_storage,
    };
    use novarocks_types::arrow_metadata_owner::FieldMetadataOrigins;
    let dictionary = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0]),
            Arc::new(arrow::array::StringArray::from(vec!["x"])),
        )
        .unwrap(),
    ) as ArrayRef;
    let mut slots = Vec::with_capacity(4096);
    for index in 0..4096 {
        let field = owned_root_field(format!("c{index}"), dictionary.data_type().clone());
        let origins = FieldMetadataOrigins::try_new(vec![field.clone()], 1).unwrap();
        slots.push(
            ChunkSlotSchema::try_new_with_metadata_origins(
                novarocks_types::SlotId::new(index),
                Arc::clone(field.field()),
                origins,
                None,
                None,
            )
            .unwrap(),
        );
    }
    let schema = Arc::new(ChunkSchema::try_new(slots).unwrap());
    let batch = RecordBatch::try_new(schema.arrow_schema_ref(), vec![dictionary; 4096]).unwrap();
    let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
    no_allocation(|| borrowed_root_chunk_storage(&chunk, storage_limits(96 * 1024 * 1024)))
        .unwrap();
}
