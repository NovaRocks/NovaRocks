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

use arrow::array::{Int64Array, RecordBatch};
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
