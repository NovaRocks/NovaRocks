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

//! Actual ordered-filter creator contracts. Inputs and expected rows are
//! declared locally; the original typed ingress, native charge, host and
//! construction factory are real. These tests attest no Native/GLOBAL peak.

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
        panic!("synchronous filter creation never waits");
    }
}

struct Fixture {
    input: Option<Chunk>,
    tracker: Arc<MemTracker>,
    host: Arc<dyn AggregateStateAllocator>,
    _factory: Arc<dyn InputConstructionFactory>,
    construction: Box<dyn InputConstructionOwner>,
    source_stock: i64,
}
impl Fixture {
    fn new(attach: bool, limit: Option<i64>) -> Self {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("keep\0港"),
                Some("drop"),
                None,
                Some("海"),
                Some(""),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(11),
                Some(22),
                None,
                Some(44),
                Some(55),
            ])),
        ];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("text", DataType::Utf8, true),
                Field::new("number", DataType::Int64, true),
            ])),
            columns,
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
        let tracker = MemTracker::new_root("ordered-filter-actual-creator");
        if let Some(limit) = limit {
            tracker.install_limit_once(limit).unwrap();
        }
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
            _factory: factory,
            construction,
            source_stock,
        }
    }
    fn apply(&mut self, mask: &BooleanArray, control: &Control) -> ExecutionResult<Option<Chunk>> {
        self.input.take().unwrap().filter_ordered_scan_controlled(
            mask,
            self.host.clone(),
            control,
            self.construction.as_ref(),
        )
    }
}
fn mixed_mask() -> BooleanArray {
    BooleanArray::from(vec![Some(true), None, Some(true), Some(false), Some(true)])
}
fn assert_mixed_rows(output: &Chunk) {
    assert_eq!(output.len(), 3);
    let text = output.columns()[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let number = output.columns()[1]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        text.iter().collect::<Vec<_>>(),
        vec![Some("keep\0港"), None, Some("")]
    );
    assert_eq!(
        number.iter().collect::<Vec<_>>(),
        vec![Some(11), None, Some(55)]
    );
    assert_eq!(
        output.chunk_schema().slot_ids(),
        &[SlotId::new(3), SlotId::new(8)]
    );
}

#[test]
fn ordered_filter_creator_all_true_keeps_original_and_all_false_publishes_nothing() {
    let mut fixture = Fixture::new(true, None);
    let tracker = fixture.tracker.clone();
    let source_stock = fixture.source_stock;
    let input = fixture.input.as_ref().unwrap();
    let batch = input.record_batch() as *const RecordBatch;
    let authority = input.source_authority().owner_identity();
    let original = Arc::downgrade(&input.columns()[0]);
    let before = tracker.current();
    // The actual source/creator are already admitted. This same host permits
    // the original prefix, but cannot admit a mixed-filter public-work lease.
    let prefix = i64::try_from(source_custody_prefix_facts().requests().bytes()).unwrap();
    tracker
        .install_limit_once(before.checked_add(prefix).unwrap())
        .unwrap();
    let output = fixture
        .apply(&BooleanArray::from(vec![true; 5]), &Control::running())
        .unwrap()
        .unwrap();
    assert!(std::ptr::eq(batch, output.record_batch()));
    assert_eq!(output.source_authority().owner_identity(), authority);
    assert_eq!(
        tracker.current(),
        before,
        "no filter stock escapes an all-true return"
    );
    drop(fixture);
    assert_eq!(tracker.current(), source_stock);
    assert!(original.upgrade().is_some());
    drop(output);
    assert!(original.upgrade().is_none());
    assert_eq!(tracker.current(), 0);

    let mut fixture = Fixture::new(true, None);
    let tracker = fixture.tracker.clone();
    let original = Arc::downgrade(&fixture.input.as_ref().unwrap().columns()[0]);
    tracker
        .install_limit_once(tracker.current().checked_add(prefix).unwrap())
        .unwrap();
    let mask = BooleanArray::from(vec![Some(false), None, Some(false), None, Some(false)]);
    assert!(fixture.apply(&mask, &Control::running()).unwrap().is_none());
    assert!(original.upgrade().is_none());
    drop(fixture);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ordered_filter_creator_mixed_nulls_and_last_value_or_null_buffer_keep_actual_stock() {
    for null_buffer in [false, true] {
        let mut fixture = Fixture::new(true, Some(64 << 20));
        let tracker = fixture.tracker.clone();
        let source_stock = fixture.source_stock;
        let input = fixture.input.as_ref().unwrap();
        let authority = input.source_authority().owner_identity();
        let original = Arc::downgrade(&input.columns()[0]);
        let schema = input.chunk_schema_ref();
        let mut output = fixture
            .apply(&mixed_mask(), &Control::running())
            .unwrap()
            .unwrap();
        assert_mixed_rows(&output);
        assert!(Arc::ptr_eq(&schema, &output.chunk_schema_ref()));
        assert_eq!(output.source_authority().owner_identity(), authority);
        assert!(output.memory_lease().is_none());
        assert!(output.take_memory_lease().is_none());
        assert_eq!(output.source_authority().owner_identity(), authority);
        let data = output.columns()[0].to_data();
        let last = if null_buffer {
            data.nulls().unwrap().buffer().clone()
        } else {
            data.buffers()[1].clone()
        };
        assert!(!last.is_empty());
        drop(data);
        drop(output);
        drop(schema);
        drop(fixture);
        assert!(
            original.upgrade().is_some(),
            "the last Buffer retains the real input Chunk"
        );
        assert!(
            tracker.current() > source_stock,
            "the last Buffer retains the original input and actual new ledger/holder stock"
        );
        std::thread::spawn(move || {
            assert!(original.upgrade().is_some());
            drop(last);
            assert!(original.upgrade().is_none());
        })
        .join()
        .unwrap();
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn ordered_filter_creator_requires_source_and_preserves_early_and_late_control_causes() {
    let mut fixture = Fixture::new(false, Some(64 << 20));
    let tracker = fixture.tracker.clone();
    let control = Control::running();
    let error = fixture.apply(&mixed_mask(), &control).unwrap_err();
    assert!(
        matches!(error.cause(), ExecutionFailureCause::Kernel(KernelFailure::InvalidProgram(detail))
        if detail.message() == "ordered scan filtering requires its actual typed source authority")
    );
    assert_eq!(control.calls.load(Ordering::SeqCst), 0);
    drop(fixture);
    assert_eq!(tracker.current(), 0);

    let mut fixture = Fixture::new(true, Some(64 << 20));
    let tracker = fixture.tracker.clone();
    let successful = Control::running();
    drop(fixture.apply(&mixed_mask(), &successful).unwrap());
    let calls = successful.calls.load(Ordering::SeqCst);
    assert!(calls > 1);
    drop(fixture);
    assert_eq!(tracker.current(), 0);
    for cause in [KernelFailure::Cancelled, KernelFailure::DeadlineExceeded] {
        for ordinal in [0, calls - 1] {
            let mut fixture = Fixture::new(true, Some(64 << 20));
            let tracker = fixture.tracker.clone();
            let control = Control {
                refuse_at: Some(ordinal),
                cause: cause.clone(),
                ..Control::running()
            };
            let error = fixture.apply(&mixed_mask(), &control).unwrap_err();
            assert_eq!(error.cause(), &ExecutionFailureCause::Kernel(cause.clone()));
            assert_eq!(control.calls.load(Ordering::SeqCst), ordinal + 1);
            drop(fixture);
            assert_eq!(tracker.current(), 0);
        }
    }
}

#[test]
fn ordered_filter_creator_refuses_a_real_positive_tracker_limit_without_leaking_input() {
    let mut fixture = Fixture::new(true, None);
    let tracker = fixture.tracker.clone();
    let before = tracker.current();
    assert!(before > 0);
    let limit = before.checked_add(1).unwrap();
    tracker.install_limit_once(limit).unwrap();
    let error = fixture
        .apply(&mixed_mask(), &Control::running())
        .unwrap_err();
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

#[test]
fn ordered_filter_creator_provider_token_survives_the_last_cross_thread_buffer() {
    use crate::exec::pipeline::async_writer::tests::provider_chunk_with_original_token;

    let mut fixture = Fixture::new(false, Some(64 << 20));
    let tracker = fixture.tracker.clone();
    let provider_tracker = MemTracker::new_root("ordered-filter-original-provider");
    provider_tracker.install_limit_once(64 << 20).unwrap();
    let original_typed = fixture.input.take().unwrap();
    let (provider, stock) = provider_chunk_with_original_token(
        original_typed.record_batch().clone(),
        &provider_tracker,
    );
    let authority = provider.source_authority().owner_identity();
    assert!(authority.0.is_none() && authority.1.is_some());
    // The exact provider arrays enter through the real typed constructor under
    // these independent explicit declarations. Sharing the original token is
    // separate from schema/type construction; no lease is taken or reminted.
    let mut typed = typed_fixture::chunk(
        provider.record_batch().clone(),
        &[SlotId::new(3), SlotId::new(8)],
        &[
            FunctionValueType::new(DataType::Utf8, true),
            FunctionValueType::new(DataType::Int64, true),
        ],
    );
    typed.retain_source_buffer_funding(&provider);
    assert_eq!(typed.source_authority().owner_identity(), authority);
    drop(provider);
    drop(original_typed);
    fixture.construction = fixture
        ._factory
        .begin_typed_construction(typed.chunk_schema())
        .unwrap();
    let original = Arc::downgrade(&typed.columns()[0]);
    fixture.input = Some(typed);
    let mut output = fixture
        .apply(&mixed_mask(), &Control::running())
        .unwrap()
        .unwrap();
    assert_mixed_rows(&output);
    assert_eq!(output.source_authority().owner_identity(), authority);
    assert!(output.take_memory_lease().is_none());
    assert_eq!(output.source_authority().owner_identity(), authority);
    let data = output.columns()[0].to_data();
    let last = data.buffers()[1].clone();
    drop(data);
    drop(output);
    drop(fixture);
    assert!(original.upgrade().is_some());
    assert_eq!(provider_tracker.current(), i64::try_from(stock).unwrap());
    assert_eq!(
        provider_tracker.allocated(),
        i64::try_from(stock).unwrap(),
        "the original provider stock was never charged a second time"
    );
    assert!(
        tracker.current() > 0,
        "actual filter stock follows the same last Buffer"
    );
    std::thread::spawn(move || {
        assert!(original.upgrade().is_some());
        drop(last);
        assert!(original.upgrade().is_none());
    })
    .join()
    .unwrap();
    assert_eq!(provider_tracker.current(), 0);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ordered_filter_creator_zero_delta_keeps_a_real_cold_extent_for_a_declared_projection() {
    let mut fixture = Fixture::new(true, Some(64 << 20));
    let tracker = fixture.tracker.clone();
    let source_stock = fixture.source_stock;
    assert!(source_stock > 0);
    let native_input = fixture.input.take().unwrap();
    let authority = native_input.source_authority().owner_identity();
    // This is an explicitly declared zero-column projection of an actual
    // nonzero native input, not a claim that the installed Scan emits zero
    // channels. The empty subset retains that SAME original native account;
    // neither an empty schema nor a dummy allocation authors source authority.
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(native_input.len())),
    )
    .unwrap();
    let mut projection = typed_fixture::chunk(batch, &[], &[]);
    projection.retain_source_buffer_funding(&native_input);
    assert_eq!(projection.source_authority().owner_identity(), authority);
    fixture.construction = fixture
        ._factory
        .begin_typed_construction(projection.chunk_schema())
        .unwrap();
    fixture.input = Some(projection);
    // Keep the actual original input alive until the subset enters the real
    // creator. There is no fabricated zero-byte native grant at this boundary.
    let mut output = fixture
        .apply(&mixed_mask(), &Control::running())
        .unwrap()
        .unwrap();
    assert_eq!(output.len(), 3);
    assert!(output.columns().is_empty());
    assert!(output.chunk_schema().is_typed());
    assert_eq!(output.source_authority().owner_identity(), authority);
    assert!(output.memory_lease().is_none());
    assert!(output.take_memory_lease().is_none());
    assert_eq!(output.source_authority().owner_identity(), authority);
    drop(native_input);
    drop(fixture);
    assert!(
        tracker.current() > source_stock,
        "the zero-payload extent still has actual paid cold/output metadata"
    );
    // A zero-column output has no Buffer alias to test. Only the real whole
    // Chunk's lifetime is asserted here; other tests cover last Buffer custody.
    drop(output);
    assert_eq!(tracker.current(), 0);
}

#[path = "ordered_filter_refusal_tests.rs"]
mod resource_refusal_tests;
