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

use super::fixtures::{Fixture, turn};
use crate::exec::{pipeline::step::NextStep, shared_stage::tail_ranges::TailClaim};
use arrow::{array::Int32Array, datatypes::DataType};
use novarocks_local_program::SetOpKind;
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;

#[test]
fn missing_probe_seal_is_distinct_from_released_output_demand() {
    let fixture = Fixture::new(
        SetOpKind::Except,
        &FunctionValueType::new(DataType::Int32, true),
        1,
    );
    fixture.input(0, Arc::new(Int32Array::from(vec![None, Some(1)])));
    fixture.input(1, Arc::new(Int32Array::from(Vec::<i32>::new())));
    fixture.finalize(&mut fixture.workers());
    fixture.probe(0, 0, Arc::new(Int32Array::from(Vec::<i32>::new())));
    assert!(fixture.frame.completion.withdraw_if_pending(1).unwrap());
    assert!(matches!(
        fixture.frame.claim_tail(&fixture.work()).unwrap(),
        TailClaim::Unavailable
    ));

    fixture.handoff.close_consumer(0);
    assert!(matches!(
        fixture.frame.claim_tail(&fixture.work()).unwrap(),
        TailClaim::Unavailable
    ));
    fixture.handoff.close_consumer(1);
    assert!(matches!(
        fixture.frame.claim_tail(&fixture.work()).unwrap(),
        TailClaim::Withdrawn
    ));
}

#[test]
fn released_handoff_retires_pending_k_workers_and_probe_slots() {
    let fixture = Fixture::new(
        SetOpKind::Intersect,
        &FunctionValueType::new(DataType::Int32, false),
        1,
    );
    fixture.input(0, Arc::new(Int32Array::from_iter_values(0..4500)));
    fixture.input(1, Arc::new(Int32Array::from_iter_values(2000..6500)));
    let mut workers = fixture.workers();
    let work = fixture.work();
    for worker in &mut workers {
        worker.step(&fixture.frame, &mut turn(), &work).unwrap();
    }
    assert!(fixture.frame.completion.published().unwrap().is_none());
    fixture.handoff.close_consumer(0);
    fixture.handoff.close_consumer(1);
    for worker in &mut workers {
        assert!(matches!(
            worker.step(&fixture.frame, &mut turn(), &work).unwrap().0,
            NextStep::Finished
        ));
    }
    assert!(fixture.frame.completion.withdraw_if_pending(0).unwrap());
    assert!(fixture.frame.completion.withdraw_if_pending(1).unwrap());
    assert!(fixture.frame.completion.published().unwrap().is_none());
    assert!(matches!(
        fixture.frame.claim_tail(&fixture.work()).unwrap(),
        TailClaim::Withdrawn
    ));
}
