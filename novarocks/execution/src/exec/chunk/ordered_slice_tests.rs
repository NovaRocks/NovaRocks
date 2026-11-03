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

//! LIMIT views retain the original native/provider source through the last
//! Buffer, including when the input has never passed through a mixed filter.

use super::*;
use crate::exec::chunk::typed_fixture;
use crate::exec::pipeline::operator::{InputConstructionFactory, InputConstructionOwner};
use crate::runtime::tracked_state_allocator::expression_allocation_host;
use crate::runtime::{
    mem_tracker::MemTracker, runtime_state::RuntimeErrorState, task::ExecutionFailureCause,
};
use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
};
use novarocks_functions::FunctionValueType;
use novarocks_types::SlotId;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

struct Control {
    calls: AtomicUsize,
    refuse_at: Option<usize>,
    cause: KernelFailure,
}
impl Control {
    fn running() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            refuse_at: None,
            cause: KernelFailure::Cancelled,
        }
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        let ordinal = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.refuse_at == Some(ordinal) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("synchronous LIMIT slicing never waits")
    }
}

struct Fixture {
    input: Option<Chunk>,
    tracker: Arc<MemTracker>,
    host: Arc<dyn AggregateStateAllocator>,
    factory: Arc<dyn InputConstructionFactory>,
    construction: Box<dyn InputConstructionOwner>,
    source_stock: i64,
}
impl Fixture {
    fn new(attach: bool) -> Self {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("text", DataType::Utf8, true),
                Field::new("number", DataType::Int64, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("head"),
                    Some("港"),
                    None,
                    Some(""),
                    Some("tail"),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(11),
                    Some(22),
                    None,
                    Some(44),
                    Some(55),
                ])),
            ],
        )
        .unwrap();
        let mut input = typed_fixture::chunk(
            batch,
            &[SlotId::new(3), SlotId::new(8)],
            &[
                FunctionValueType::new(DataType::Utf8, true),
                FunctionValueType::new(DataType::Int64, true),
            ],
        );
        let tracker = MemTracker::new_root("ordered-slice-actual-creator");
        if attach {
            input.try_transfer_to(&tracker).unwrap();
        }
        let source_stock = tracker.current();
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(), Arc::new(RuntimeErrorState::default()),
        ).unwrap();
        let construction = factory
            .begin_typed_construction(input.chunk_schema())
            .unwrap();
        let host = expression_allocation_host(tracker.clone());
        Self {
            input: Some(input),
            tracker,
            host,
            factory,
            construction,
            source_stock,
        }
    }
    fn apply(&mut self, offset: usize, length: usize, control: &Control) -> ExecutionResult<Chunk> {
        self.input.take().unwrap().slice_ordered_scan_controlled(
            offset,
            length,
            self.host.clone(),
            control,
            self.construction.as_ref(),
        )
    }
}

fn assert_middle(output: &Chunk) {
    assert_eq!(output.len(), 3);
    assert_eq!(
        output.chunk_schema().slot_ids(),
        &[SlotId::new(3), SlotId::new(8)]
    );
    assert_eq!(
        output.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("港"), None, Some("")]
    );
    assert_eq!(
        output.columns()[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(22), None, Some(44)]
    );
}

#[test]
fn ordered_slice_native_zero_delta_and_original_input_survive_last_value_or_null_buffer() {
    for (offset, rows) in [(0, 5), (1, 3)] {
        for null_buffer in [false, true] {
            let mut fixture = Fixture::new(true);
            fixture.tracker.install_limit_once(64 << 20).unwrap();
            let tracker = fixture.tracker.clone();
            let source_stock = fixture.source_stock;
            let original_input = fixture.input.as_ref().unwrap().clone();
            let original_array = Arc::downgrade(&original_input.columns()[0]);
            let authority = original_input.source_authority().owner_identity();
            let schema = original_input.chunk_schema_ref();
            let mut output = fixture.apply(offset, rows, &Control::running()).unwrap();
            if offset == 1 {
                assert_middle(&output);
            }
            assert_eq!(output.len(), rows);
            assert!(Arc::ptr_eq(&schema, &output.chunk_schema_ref()));
            assert_eq!(output.source_authority().owner_identity(), authority);
            assert_eq!(
                record_batch_additional_bytes_observed(
                    output.record_batch(),
                    original_input.record_batch(),
                    || Ok(())
                )
                .unwrap(),
                0
            );
            assert!(output.memory_lease().is_none());
            assert!(output.take_memory_lease().is_none());
            assert_eq!(output.source_authority().owner_identity(), authority);
            let data = output.columns()[0].to_data();
            let last = if null_buffer {
                data.nulls().unwrap().buffer().clone()
            } else {
                data.buffers()[1].clone()
            };
            drop(data);
            drop(output);
            drop(original_input);
            drop(schema);
            drop(fixture);
            assert!(
                original_array.upgrade().is_some(),
                "a naked source Array survives through the slice's Buffer custody"
            );
            assert!(
                tracker.current() > source_stock,
                "the zero-delta extent and slice/holder creators remain paid"
            );
            std::thread::spawn(move || {
                assert!(original_array.upgrade().is_some());
                drop(last);
                assert!(original_array.upgrade().is_none());
            })
            .join()
            .unwrap();
            assert_eq!(tracker.current(), 0);
        }
    }
}

#[test]
fn ordered_slice_provider_keeps_the_original_token_without_recharging_its_stock() {
    use crate::exec::pipeline::async_writer::tests::provider_chunk_with_original_token;
    let mut fixture = Fixture::new(false);
    fixture.tracker.install_limit_once(64 << 20).unwrap();
    let tracker = fixture.tracker.clone();
    let provider_tracker = MemTracker::new_root("ordered-slice-original-provider");
    provider_tracker.install_limit_once(64 << 20).unwrap();
    let raw = fixture.input.take().unwrap();
    let (provider, stock) =
        provider_chunk_with_original_token(raw.record_batch().clone(), &provider_tracker);
    let authority = provider.source_authority().owner_identity();
    let mut typed = typed_fixture::chunk(
        provider.record_batch().clone(),
        &[SlotId::new(3), SlotId::new(8)],
        &[
            FunctionValueType::new(DataType::Utf8, true),
            FunctionValueType::new(DataType::Int64, true),
        ],
    );
    typed.retain_source_buffer_funding(&provider);
    fixture.construction = fixture
        .factory
        .begin_typed_construction(typed.chunk_schema())
        .unwrap();
    let original = Arc::downgrade(&typed.columns()[0]);
    fixture.input = Some(typed);
    drop(provider);
    drop(raw);
    let mut output = fixture.apply(1, 3, &Control::running()).unwrap();
    assert_middle(&output);
    assert_eq!(output.source_authority().owner_identity(), authority);
    assert!(output.take_memory_lease().is_none());
    let data = output.columns()[0].to_data();
    let last = data.buffers()[1].clone();
    drop(data);
    drop(output);
    drop(fixture);
    assert!(original.upgrade().is_some());
    assert_eq!(provider_tracker.current(), i64::try_from(stock).unwrap());
    assert_eq!(provider_tracker.allocated(), i64::try_from(stock).unwrap());
    assert!(tracker.current() > 0);
    std::thread::spawn(move || {
        drop(last);
        assert!(original.upgrade().is_none());
    })
    .join()
    .unwrap();
    assert_eq!(provider_tracker.current(), 0);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ordered_slice_empty_end_view_keeps_a_real_zero_extent_and_exact_schema() {
    let mut fixture = Fixture::new(true);
    fixture.tracker.install_limit_once(64 << 20).unwrap();
    let tracker = fixture.tracker.clone();
    let original = fixture.input.as_ref().unwrap().clone();
    let schema = original.chunk_schema_ref();
    let authority = original.source_authority().owner_identity();
    let mut output = fixture.apply(5, 0, &Control::running()).unwrap();
    assert_eq!(output.len(), 0);
    assert_eq!(output.columns().len(), 2);
    assert!(Arc::ptr_eq(&schema, &output.chunk_schema_ref()));
    assert_eq!(output.source_authority().owner_identity(), authority);
    assert_eq!(
        record_batch_additional_bytes_observed(
            output.record_batch(),
            original.record_batch(),
            || Ok(())
        )
        .unwrap(),
        0
    );
    assert!(output.take_memory_lease().is_none());
    drop(original);
    drop(schema);
    drop(fixture);
    assert!(tracker.current() > 0);
    drop(output);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ordered_slice_of_an_additional_extent_preserves_the_original_buffer_owner() {
    let mut fixture = Fixture::new(true);
    fixture.tracker.install_limit_once(64 << 20).unwrap();
    let tracker = fixture.tracker.clone();
    let original = Arc::downgrade(&fixture.input.as_ref().unwrap().columns()[0]);
    let authority = fixture
        .input
        .as_ref()
        .unwrap()
        .source_authority()
        .owner_identity();
    let first = fixture.apply(1, 3, &Control::running()).unwrap();
    let mut second = first
        .slice_ordered_scan_controlled(
            1,
            1,
            fixture.host.clone(),
            &Control::running(),
            fixture.construction.as_ref(),
        )
        .unwrap();
    assert_eq!(second.len(), 1);
    assert!(second.columns()[0].is_null(0));
    assert!(second.columns()[1].is_null(0));
    assert_eq!(second.source_authority().owner_identity(), authority);
    assert!(second.take_memory_lease().is_none());
    let data = second.columns()[0].to_data();
    let last = data.nulls().unwrap().buffer().clone();
    drop(data);
    drop(second);
    drop(fixture);
    assert!(original.upgrade().is_some());
    assert!(tracker.current() > 0);
    drop(last);
    assert!(original.upgrade().is_none());
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ordered_slice_rejects_missing_authority_and_out_of_range_before_control() {
    for (attach, offset, rows, message) in [
        (
            false,
            0,
            1,
            "ordered scan slicing requires its actual typed source authority",
        ),
        (
            true,
            4,
            2,
            "ordered scan slice is outside its explicit source contract",
        ),
        (
            true,
            usize::MAX,
            1,
            "ordered scan slice is outside its explicit source contract",
        ),
        (
            true,
            6,
            0,
            "ordered scan slice is outside its explicit source contract",
        ),
    ] {
        let mut fixture = Fixture::new(attach);
        let tracker = fixture.tracker.clone();
        let control = Control::running();
        let error = fixture.apply(offset, rows, &control).unwrap_err();
        assert!(
            matches!(error.cause(), ExecutionFailureCause::Kernel(KernelFailure::InvalidProgram(detail)) if detail.message() == message)
        );
        assert_eq!(control.calls.load(Ordering::SeqCst), 0);
        drop(fixture);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn ordered_slice_control_refusal_keeps_first_cause_and_releases_unpublished_owners() {
    let mut fixture = Fixture::new(true);
    fixture.tracker.install_limit_once(64 << 20).unwrap();
    let tracker = fixture.tracker.clone();
    let control = Control::running();
    drop(fixture.apply(1, 3, &control).unwrap());
    let calls = control.calls.load(Ordering::SeqCst);
    assert!(calls > 1);
    drop(fixture);
    assert_eq!(tracker.current(), 0);
    for cause in [KernelFailure::Cancelled, KernelFailure::DeadlineExceeded] {
        for ordinal in [0, calls - 1] {
            let mut fixture = Fixture::new(true);
            fixture.tracker.install_limit_once(64 << 20).unwrap();
            let tracker = fixture.tracker.clone();
            let control = Control {
                refuse_at: Some(ordinal),
                cause: cause.clone(),
                ..Control::running()
            };
            let error = fixture.apply(1, 3, &control).unwrap_err();
            assert_eq!(error.cause(), &ExecutionFailureCause::Kernel(cause.clone()));
            assert_eq!(control.calls.load(Ordering::SeqCst), ordinal + 1);
            drop(fixture);
            assert_eq!(tracker.current(), 0);
        }
    }
}

#[test]
fn ordered_slice_positive_host_limit_refusal_keeps_no_raw_or_source_owner() {
    let mut fixture = Fixture::new(true);
    let tracker = fixture.tracker.clone();
    let limit = tracker.current().checked_add(1).unwrap();
    tracker.install_limit_once(limit).unwrap();
    let error = fixture.apply(1, 3, &Control::running()).unwrap_err();
    let fact = novarocks_failure_contract::FailureProjection::failure_fact(&error);
    assert_eq!(
        fact.class(),
        novarocks_failure_contract::FailureClass::Resource
    );
    let resource = fact.resource().expect("the original tracker refusal");
    assert_eq!(resource.limit, Some(limit as u64));
    assert!(resource.requested.unwrap() > 0);
    assert!(resource.committed.unwrap() > limit as u64);
    drop(fixture);
    assert_eq!(tracker.current(), 0);
}
