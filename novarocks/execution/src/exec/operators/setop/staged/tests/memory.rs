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

use super::super::frame::Phase;
use super::fixtures::{Fixture, turn};
use arrow::{array::Int32Array, datatypes::DataType};
use novarocks_local_program::SetOpKind;
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;

#[test]
fn original_tracker_owns_published_index_and_membership_storage() {
    let fixture = Fixture::new(
        SetOpKind::Intersect,
        &FunctionValueType::new(DataType::Int32, true),
        3,
    );
    let tracker = fixture.tracker.clone();
    let initial = tracker.current();
    fixture.input(0, Arc::new(Int32Array::from_iter_values(0..5000)));
    fixture.input(1, Arc::new(Int32Array::from_iter_values(5000..10000)));
    fixture.finalize(&mut fixture.workers());
    let artifact = fixture
        .frame
        .completion
        .published()
        .unwrap()
        .unwrap()
        .artifact;
    let bitmap_bytes = (10000usize * 3).div_ceil(64) * std::mem::size_of::<u64>();
    assert_eq!(
        artifact.bits.word_count(),
        bitmap_bytes / std::mem::size_of::<u64>()
    );
    assert!(tracker.current() >= initial + bitmap_bytes as i64);
    drop(fixture);
    assert!(tracker.current() >= bitmap_bytes as i64);
    drop(artifact);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn captured_inputs_are_released_before_bitmap_initialization() {
    let fixture = Fixture::new(
        SetOpKind::Intersect,
        &FunctionValueType::new(DataType::Int32, true),
        2,
    );
    let captured = Arc::new(Int32Array::from_iter_values(0..96));
    let captured_weak = Arc::downgrade(&captured);
    fixture.input(0, captured);
    fixture.input(1, Arc::new(Int32Array::from_iter_values(12..128)));
    let mut workers = fixture.workers();
    let work = fixture.work();
    for _ in 0..2000 {
        if workers[0].settled_key_build() {
            break;
        }
        workers[0].step(&fixture.frame, &mut turn(), &work).unwrap();
    }
    assert!(workers[0].settled_key_build());
    assert!(captured_weak.upgrade().is_some());
    assert!(matches!(
        fixture.frame.observe(true).unwrap(),
        Phase::WaitingForKeyWorkers
    ));

    for worker in &mut workers[1..] {
        worker.step(&fixture.frame, &mut turn(), &work).unwrap();
        assert!(worker.settled_key_build());
    }
    assert!(matches!(
        fixture.frame.observe(true).unwrap(),
        Phase::Bitmap(_)
    ));
    assert!(captured_weak.upgrade().is_none());
}
