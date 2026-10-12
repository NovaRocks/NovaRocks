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

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_types::SlotId;

use super::{Chunk, ChunkSchema};
use crate::exec::pipeline::async_writer::tests::provider_chunk_with_original_token;
use crate::runtime::mem_tracker::MemTracker;

pub(crate) fn input() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap()
}

fn native_chunk(tracker: &Arc<MemTracker>) -> Chunk {
    let batch = input();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId(1)])
            .unwrap();
    let mut chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
    chunk.try_transfer_to(tracker).unwrap();
    chunk
}

#[test]
fn native_capture_keeps_original_charge_until_last_cross_thread_drop() {
    let tracker = MemTracker::new_root("native-source-capture");
    let chunk = native_chunk(&tracker);
    let bytes = tracker.current();
    assert!(bytes > 0);
    let captured = chunk.source_authority().capture().unwrap();
    let last = captured.clone();
    assert_eq!(chunk.source_authority().strong_counts(), (Some(3), None));
    assert_eq!(tracker.allocated(), bytes);
    drop(chunk);
    assert_eq!(tracker.current(), bytes);
    std::thread::spawn(move || drop(captured)).join().unwrap();
    assert_eq!(tracker.current(), bytes);
    assert_eq!(tracker.deallocated(), 0);
    std::thread::spawn(move || drop(last)).join().unwrap();
    assert_eq!(tracker.current(), 0);
    assert_eq!(tracker.deallocated(), bytes);
}

#[test]
fn provider_capture_keeps_original_token_until_last_cross_thread_drop() {
    let tracker = MemTracker::new_root("provider-source-capture");
    let (chunk, bytes) = provider_chunk_with_original_token(input(), &tracker);
    let captured = chunk.source_authority().capture().unwrap();
    let last = captured.clone();
    assert_eq!(chunk.source_authority().strong_counts(), (None, Some(3)));
    assert_eq!(tracker.allocated(), bytes as i64);
    drop(chunk);
    assert_eq!(tracker.current(), bytes as i64);
    std::thread::spawn(move || drop(captured)).join().unwrap();
    assert_eq!(tracker.current(), bytes as i64);
    assert_eq!(tracker.deallocated(), 0);
    std::thread::spawn(move || drop(last)).join().unwrap();
    assert_eq!(tracker.current(), 0);
    assert_eq!(tracker.deallocated(), bytes as i64);
}

#[test]
fn borrow_does_not_change_native_or_provider_exclusivity() {
    let tracker = MemTracker::new_root("borrow-exclusivity");
    let native = native_chunk(&tracker);
    let (provider, _) = provider_chunk_with_original_token(input(), &tracker);
    for mut chunk in [native, provider] {
        let before = chunk.source_authority().strong_counts();
        let borrowed = chunk.source_authority();
        assert!(borrowed.is_attached());
        assert_eq!(borrowed.strong_counts(), before);
        assert_eq!(chunk.source_authority().strong_counts(), before);
        let lease = chunk.take_memory_lease().expect("original exclusive owner");
        drop(chunk);
        drop(lease);
    }
    assert_eq!(tracker.current(), 0);
    assert_eq!(tracker.allocated(), tracker.deallocated());
}

#[test]
fn chunk_without_attached_authority_has_no_capturable_owner() {
    let batch = input();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId(1)])
            .unwrap();
    let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
    let source = chunk.source_authority();
    assert!(!source.is_attached());
    assert_eq!(source.strong_counts(), (None, None));
    assert!(source.capture().is_none());
}

// The allocation host here is the existing Execution compatibility tracker.
// These tests prove original capture teardown, not formal Runtime funding.
fn direct_capture_last_buffer(chunk: Chunk, source_tracker: Arc<MemTracker>, stock: i64) {
    use crate::exec::operators::compiled_expression::RuntimeKernelControl;
    use crate::runtime::runtime_state::RuntimeErrorState;
    use novarocks_functions::opaque_memory::OpaqueRetainedCharge;
    use novarocks_functions::{EvaluationCheckpoints, PreparedSourceCustody, SourceBackingOwner};
    let wrap_tracker = MemTracker::new_root("direct source wrapping");
    let mut control = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
    control.bind_mem_tracker(wrap_tracker.clone());
    let host = control.allocator().unwrap();
    let array = chunk.batch.column(0).clone();
    let captured = chunk.source_authority().capture().unwrap();
    let mut work = EvaluationCheckpoints::new(&control);
    let source =
        SourceBackingOwner::try_from_owned_direct(captured, host.clone(), &mut work).unwrap();
    let prepared = PreparedSourceCustody::prepare(&array, &mut work).unwrap();
    let charge = OpaqueRetainedCharge::try_new(host).unwrap();
    let mut reservation = charge
        .reserve_operation(prepared.request_facts().global_peak_upper())
        .unwrap();
    let result = prepared
        .retain(&source, &mut reservation, &mut work)
        .unwrap();
    assert_eq!(result.to_data(), array.to_data());
    assert_eq!(
        source_tracker.allocated(),
        stock,
        "original input never charged again"
    );
    let slice = result.slice(1, 2);
    let data = slice.clone().into_data();
    let last = data.buffers()[0].clone();
    drop(reservation);
    drop(charge);
    drop(source);
    drop(array);
    drop(chunk);
    drop(slice);
    drop(result);
    drop(data);
    assert_eq!(source_tracker.current(), stock);
    assert_eq!(source_tracker.deallocated(), 0);
    assert!(
        wrap_tracker.current() > 0,
        "actual wrapping remains admitted"
    );
    std::thread::spawn(move || drop(last)).join().unwrap();
    assert_eq!(source_tracker.current(), 0);
    assert_eq!(source_tracker.deallocated(), stock);
    assert_eq!(wrap_tracker.current(), 0);
    assert_eq!(wrap_tracker.allocated(), wrap_tracker.deallocated());
}
#[test]
fn source_authority_direct_native_capture_is_held_by_last_cross_thread_buffer() {
    let tracker = MemTracker::new_root("direct native input");
    let chunk = native_chunk(&tracker);
    let stock = tracker.current();
    direct_capture_last_buffer(chunk, tracker, stock);
}
#[test]
fn source_authority_direct_provider_capture_is_held_by_last_cross_thread_buffer() {
    let tracker = MemTracker::new_root("direct provider input");
    let (chunk, stock) = provider_chunk_with_original_token(input(), &tracker);
    direct_capture_last_buffer(chunk, tracker, stock as i64);
}

#[test]
fn source_authority_direct_constructor_refusal_preserves_the_paid_raw_source_alias() {
    use crate::exec::operators::compiled_expression::RuntimeKernelControl;
    use crate::runtime::runtime_state::RuntimeErrorState;
    use novarocks_functions::{EvaluationCheckpoints, KernelFailure, SourceBackingOwner};
    for provider in [false, true] {
        let tracker = MemTracker::new_root("failed capture original input");
        let chunk = if provider {
            provider_chunk_with_original_token(input(), &tracker).0
        } else {
            native_chunk(&tracker)
        };
        let stock = tracker.current();
        let raw = chunk.batch.column(0).clone();
        let captured = chunk.source_authority().capture().unwrap();
        let wrap_tracker = MemTracker::new_root("refused direct holder");
        wrap_tracker.install_limit_once(1).unwrap();
        let mut control = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
        control.bind_mem_tracker(wrap_tracker.clone());
        let mut work = EvaluationCheckpoints::new(&control);
        let result = SourceBackingOwner::try_from_owned_direct(
            captured,
            control.allocator().unwrap(),
            &mut work,
        );
        assert!(matches!(result, Err(KernelFailure::ResourceExhausted)));
        assert_eq!(
            tracker.current(),
            stock,
            "raw aliases remain paid by the existing Chunk"
        );
        assert_eq!(wrap_tracker.current(), 0);
        assert_eq!(raw.len(), 3);
        drop(raw);
        drop(chunk);
        assert_eq!(tracker.current(), 0);
        assert_eq!(tracker.allocated(), tracker.deallocated());
    }
}
