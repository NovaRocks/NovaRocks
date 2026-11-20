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

use std::sync::atomic::{AtomicU64, Ordering};

use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::Field;
use novarocks_spi::connector::{
    ConnectorError, ConnectorExecutionResources, ConnectorReservationBackend,
    ConnectorResourceCheckpoint, ConnectorResourceClass, ConnectorResourceLedger,
};
use novarocks_types::SlotId;

use super::*;
use crate::runtime::mem_tracker::MemTracker;

fn declared_schema(with_column: bool) -> ChunkSchemaRef {
    declared_schema_at(with_column, SlotId::new(17))
}

fn declared_schema_at(with_column: bool, slot: SlotId) -> ChunkSchemaRef {
    use arrow::datatypes::DataType;
    use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
    let storage = ValueTypeStorageLifetime::pure();
    let slots = if with_column {
        let declaration = FunctionValueType::new(DataType::Int64, false);
        vec![
            ChunkSlotSchema::try_new_typed(
                slot,
                Arc::new(Field::new("v", DataType::Int64, false)),
                declaration.typed_binding().clone(),
                None,
                storage.clone(),
            )
            .unwrap(),
        ]
    } else {
        vec![]
    };
    Arc::new(ChunkSchema::try_new_typed(slots, storage).unwrap())
}

fn actual_factory(
    tracker: &Arc<MemTracker>,
    error: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
) -> Arc<dyn InputConstructionFactory> {
    crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        error,
    )
    .unwrap()
}

#[test]
fn typed_page_initial_chunk_keeps_only_its_finite_creator_and_frozen_slots() {
    let tracker = MemTracker::new_root("initial typed page");
    let error = Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default());
    let controller = Arc::downgrade(&error);
    let factory = actual_factory(&tracker, error);
    let schema = declared_schema(true);
    let mut converter = SourcePageConverter::with_output_schema(schema.clone());
    let chunk = converter
        .convert_with_factory(int_page(vec![1, 2]), factory.as_ref())
        .unwrap();
    assert_eq!(chunk.chunk_schema().slot_ids(), &[SlotId::new(17)]);
    assert_eq!(chunk.len(), 2);
    assert!(Arc::ptr_eq(&chunk.chunk_schema_ref(), &schema));
    drop(converter);
    drop(factory);
    assert!(
        controller.upgrade().is_none(),
        "late chunk does not retain the control/factory"
    );
    assert!(tracker.current() > 0);
    let late = chunk.clone();
    drop(chunk);
    assert!(tracker.current() > 0);
    drop(late);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn typed_count_only_page_and_final_wrapper_refusal_use_the_actual_owner() {
    use crate::runtime::task::ExecutionFailureCause;
    let tracker = MemTracker::new_root("count-only page constructor");
    let factory = actual_factory(
        &tracker,
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    );
    let initial = tracker.current();
    let mut converter = SourcePageConverter::with_output_schema(declared_schema(false));
    let chunk = converter
        .convert_with_factory(SourcePage::zero_channel(7), factory.as_ref())
        .unwrap();
    assert_eq!(chunk.len(), 7);
    assert!(chunk.columns().is_empty());
    assert!(chunk.chunk_schema().is_typed());
    let request_bytes = tracker.current() - initial;
    assert!(request_bytes > 1);
    drop(chunk);
    assert_eq!(tracker.current(), initial);
    // This same constructor has only scope metadata and its final batch
    // Arc requests: one byte below its real successful total refuses that
    // final wrapper without changing the provider's row-count semantics.
    tracker
        .install_limit_once(initial + request_bytes - 1)
        .unwrap();
    let error = converter
        .convert_with_factory(SourcePage::zero_channel(7), factory.as_ref())
        .unwrap_err();
    assert!(matches!(
        error.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
    ));
    assert_eq!(
        tracker.current(),
        initial,
        "all successful prefix requests roll back"
    );
    drop(factory);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn typed_page_original_stop_precedes_source_errors_without_constructing_a_chunk() {
    let tracker = MemTracker::new_root("page original stop");
    let error = Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default());
    let factory = actual_factory(&tracker, error.clone());
    let initial = tracker.current();
    let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Internal(
        novarocks_functions::KernelDiagnostic::new("original scan stop"),
    ));
    error.set_error(original.clone());
    let mut converter = SourcePageConverter::with_output_schema(declared_schema(true));
    let refused = converter
        .convert_with_factory(SourcePage::zero_channel(1), factory.as_ref())
        .unwrap_err();
    assert_eq!(
        refused, original,
        "the missing source channel must not replace the original stop"
    );
    assert_eq!(tracker.current(), initial);
    drop(factory);
    assert_eq!(tracker.current(), 0);
}

struct OutputLedger {
    retained: Arc<AtomicU64>,
}

impl ConnectorResourceLedger for OutputLedger {
    fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
        Ok(ConnectorResourceCheckpoint::new(0))
    }

    fn try_reserve(
        &self,
        class: ConnectorResourceClass,
        bytes: u64,
    ) -> Result<Box<dyn ConnectorReservationBackend>, ConnectorError> {
        assert_eq!(class, ConnectorResourceClass::ReaderOutput);
        self.retained.fetch_add(bytes, Ordering::AcqRel);
        Ok(Box::new(OutputReservation {
            retained: Arc::clone(&self.retained),
            bytes,
        }))
    }
}

struct OutputReservation {
    retained: Arc<AtomicU64>,
    bytes: u64,
}

impl ConnectorReservationBackend for OutputReservation {
    fn bytes(&self) -> u64 {
        self.bytes
    }

    fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
        self.retained.fetch_add(additional, Ordering::AcqRel);
        self.bytes += additional;
        Ok(())
    }

    fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
        assert!(bytes <= self.bytes);
        self.retained
            .fetch_sub(self.bytes - bytes, Ordering::AcqRel);
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for OutputReservation {
    fn drop(&mut self) {
        self.retained.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

fn int_page(values: Vec<i64>) -> SourcePage {
    let positions = values.len();
    let column: ArrayRef = Arc::new(Int64Array::from(values));
    SourcePage::try_new(positions, vec![column]).expect("valid page")
}

fn convert_declared(page: SourcePage, with_column: bool) -> Result<Chunk, PageAdapterError> {
    SourcePageConverter::with_output_schema(declared_schema_at(with_column, SlotId::new(1)))
        .convert(page)
}

#[test]
fn a_zero_channel_page_converts_to_a_row_counted_chunk() {
    let chunk = convert_declared(SourcePage::zero_channel(1024), false).expect("chunk");
    assert_eq!(chunk.len(), 1024);
    assert_eq!(chunk.columns().len(), 0);
    // It is a real result, never end of stream.
    assert!(!chunk.is_empty());
}

#[test]
fn accounted_page_moves_one_existing_charge_to_the_chunk() {
    let retained = Arc::new(AtomicU64::new(0));
    let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(OutputLedger {
        retained: Arc::clone(&retained),
    }));
    let column: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let bytes = column.get_array_memory_size() as u64;
    let output_memory = resources
        .try_reserve(ConnectorResourceClass::ReaderOutput, bytes)
        .expect("reserve provider output")
        .into_output(bytes)
        .expect("freeze provider output charge");
    let page = SourcePage::try_new_accounted(3, vec![column], output_memory)
        .expect("accounted source page");
    let tracker = MemTracker::new_root("converted connector chunk");
    let mut chunk =
        SourcePageConverter::with_output_schema(declared_schema_at(true, SlotId::new(1)))
            .convert(page)
            .expect("convert accounted page");
    chunk.transfer_to(&tracker);
    assert_eq!(retained.load(Ordering::Acquire), bytes);
    assert_eq!(tracker.current(), 0, "the page must not be charged twice");

    let shared = chunk.clone();
    drop(chunk);
    assert_eq!(retained.load(Ordering::Acquire), bytes);
    drop(shared);
    assert_eq!(retained.load(Ordering::Acquire), 0);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn accounted_page_releases_hidden_materialized_channels_before_chunk_handoff() {
    let retained = Arc::new(AtomicU64::new(0));
    let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(OutputLedger {
        retained: Arc::clone(&retained),
    }));
    let visible: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let hidden: ArrayRef = Arc::new(Int64Array::from((0_i64..1024).collect::<Vec<_>>()));
    let hidden = hidden.slice(0, 3);
    let visible_bytes = visible.get_array_memory_size() as u64;
    let total_bytes = visible_bytes + hidden.get_array_memory_size() as u64;
    let output_memory = resources
        .try_reserve(ConnectorResourceClass::ReaderOutput, total_bytes)
        .expect("reserve visible and hidden provider output")
        .into_output(total_bytes)
        .expect("freeze full provider output charge");
    let page = SourcePage::try_new_accounted(3, vec![visible, hidden], output_memory)
        .expect("accounted source page");
    let chunk = convert_declared(page, true).expect("convert projected page");
    assert_eq!(retained.load(Ordering::Acquire), visible_bytes);
    drop(chunk);
    assert_eq!(retained.load(Ordering::Acquire), 0);
}

#[test]
fn accounted_count_only_projection_releases_the_complete_page_charge() {
    let retained = Arc::new(AtomicU64::new(0));
    let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(OutputLedger {
        retained: Arc::clone(&retained),
    }));
    let hidden: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let bytes = hidden.get_array_memory_size() as u64;
    let output_memory = resources
        .try_reserve(ConnectorResourceClass::ReaderOutput, bytes)
        .expect("reserve hidden provider output")
        .into_output(bytes)
        .expect("freeze hidden provider output charge");
    let page = SourcePage::try_new_accounted(3, vec![hidden], output_memory)
        .expect("accounted source page");
    let chunk = convert_declared(page, false).expect("convert count-only page");
    assert_eq!(chunk.len(), 3);
    assert_eq!(retained.load(Ordering::Acquire), 0);
}

#[test]
fn an_unbound_trailing_channel_is_dropped() {
    let visible: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
    let hidden: ArrayRef = Arc::new(Int64Array::from(vec![5_i64, 6]));
    let page = SourcePage::try_new(2, vec![visible, hidden]).expect("valid page");

    let chunk = convert_declared(page, true).expect("chunk");
    assert_eq!(chunk.columns().len(), 1);
    assert_eq!(chunk.len(), 2);
}

#[test]
fn binding_more_channels_than_the_page_carries_is_rejected() {
    let error =
        convert_declared(SourcePage::zero_channel(4), true).expect_err("channel mismatch");
    assert_eq!(error.kind(), PageAdapterErrorKind::ChannelMismatch);
}

#[test]
fn the_derived_schema_is_reused_while_the_arrow_types_hold() {
    let mut converter =
        SourcePageConverter::with_output_schema(declared_schema_at(true, SlotId::new(1)));
    let first = converter
        .convert(int_page(vec![1]))
        .expect("first chunk")
        .chunk_schema_ref();
    let second = converter
        .convert(int_page(vec![2, 3]))
        .expect("second chunk")
        .chunk_schema_ref();
    assert!(Arc::ptr_eq(&first, &second));
}

fn frozen_map_schema(value_type: arrow::datatypes::DataType) -> ChunkSchemaRef {
    use arrow::datatypes::DataType;
    let map_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    Arc::new(Field::new("value", value_type, true)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
    let storage = ValueTypeStorageLifetime::pure();
    let declaration = FunctionValueType::new(map_type, true);
    let slot = ChunkSlotSchema::try_new_typed(
        SlotId::new(17),
        Arc::new(Field::new("mi", declaration.data_type().clone(), true)),
        declaration.typed_binding().clone(),
        None,
        storage.clone(),
    )
    .expect("frozen map slot");
    Arc::new(ChunkSchema::try_new_typed(vec![slot], storage).expect("frozen map schema"))
}

fn physical_map() -> ArrayRef {
    use arrow::array::{Int32Array, MapArray, StructArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", arrow::datatypes::DataType::Int32, false)),
            Arc::new(Field::new("value", arrow::datatypes::DataType::Int32, true)),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Int32Array::from(vec![None, Some(7)])),
        ],
        None,
    );
    Arc::new(MapArray::new(
        Arc::new(Field::new("key_value", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0_i32, 0, 0, 1, 2].into()),
        entries,
        Some(NullBuffer::from(vec![false, true, true, true])),
        false,
    ))
}

#[test]
fn frozen_scan_map_retags_buffers_and_keeps_parent_value_nulls_and_one_lease() {
    use arrow::array::MapArray;
    let retained = Arc::new(AtomicU64::new(0));
    let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(OutputLedger {
        retained: Arc::clone(&retained),
    }));
    let source = physical_map();
    let physical = source
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("source map");
    let key_pointer = physical.keys().to_data().buffers()[0].as_ptr();
    let value_pointer = physical.values().to_data().buffers()[0].as_ptr();
    let offset_pointer = physical.offsets().inner().inner().as_ptr();
    let bytes = source.get_array_memory_size() as u64;
    let lease = resources
        .try_reserve(ConnectorResourceClass::ReaderOutput, bytes)
        .expect("reserve")
        .into_output(bytes)
        .expect("output lease");
    let page = SourcePage::try_new_accounted(4, vec![source], lease).expect("page");
    let schema = frozen_map_schema(arrow::datatypes::DataType::Int32);
    let mut converter = SourcePageConverter::with_output_schema(Arc::clone(&schema));
    let mut chunk = converter.convert(page).expect("frozen map output");
    assert_eq!(
        chunk.columns()[0].data_type(),
        schema.slots()[0].data_type()
    );
    assert_eq!(chunk.chunk_schema().slot_ids(), &[SlotId::new(17)]);
    let map = chunk.columns()[0]
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("map");
    assert_eq!(map.keys().to_data().buffers()[0].as_ptr(), key_pointer);
    assert_eq!(map.values().to_data().buffers()[0].as_ptr(), value_pointer);
    assert_eq!(map.offsets().inner().inner().as_ptr(), offset_pointer);
    assert!(map.is_null(0));
    assert!(!map.is_null(1));
    assert_eq!(map.value_length(1), 0);
    assert_eq!(map.values().null_count(), 1);
    assert_eq!(map.value_length(2), 1);
    let tracker = MemTracker::new_root("frozen map output");
    chunk.transfer_to(&tracker);
    assert_eq!(
        tracker.current(),
        0,
        "retag must keep the existing provider lease"
    );
    assert_eq!(retained.load(Ordering::Acquire), bytes);
    let cloned = chunk.clone();
    drop(chunk);
    assert_eq!(retained.load(Ordering::Acquire), bytes);
    drop(cloned);
    assert_eq!(retained.load(Ordering::Acquire), 0);
    let second = converter
        .convert(SourcePage::try_new(4, vec![physical_map()]).expect("page"))
        .expect("second page uses the same frozen contract");
    assert_eq!(
        second.columns()[0].data_type(),
        schema.slots()[0].data_type()
    );
}

#[test]
fn frozen_scan_rejects_nested_value_drift_instead_of_rebinding_from_the_page() {
    let mut converter = SourcePageConverter::with_output_schema(frozen_map_schema(
        arrow::datatypes::DataType::Int64,
    ));
    let error = converter
        .convert(SourcePage::try_new(4, vec![physical_map()]).expect("page"))
        .expect_err("Int32 must not become Int64 by metadata retag");
    assert_eq!(error.kind(), PageAdapterErrorKind::Chunk);
}

#[test]
fn frozen_zero_channel_scan_preserves_count_and_output_contract() {
    let schema = declared_schema(false);
    let chunk = SourcePageConverter::with_output_schema(Arc::clone(&schema))
        .convert(SourcePage::zero_channel(19))
        .expect("count-only page");
    assert_eq!(chunk.len(), 19);
    assert!(chunk.columns().is_empty());
    assert_eq!(chunk.chunk_schema().slot_ids(), schema.slot_ids());
}
