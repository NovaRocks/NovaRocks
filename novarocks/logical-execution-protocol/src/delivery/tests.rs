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

use super::*;
use novarocks_types::{AttemptId, QueryExecutionId, QueryId};

fn activation(attempt: u64, generation: u64) -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(
        17,
        QueryExecutionId::new(QueryId::new(21, 22), AttemptId::new(attempt).unwrap()).unwrap(),
        generation,
    )
}

#[test]
fn delivery_winning_the_race_irreversibly_closes_replacement() {
    let first = activation(1, 9);
    let mut gate = DeliveryGate::new(first);
    gate.mark_visible(first).unwrap();
    assert_eq!(gate.visible_to(), Some(first));
    assert_eq!(
        gate.revoke_for_replacement(first),
        Err(DeliveryGateError::AlreadyVisible)
    );
    assert_eq!(
        gate.mark_visible(first),
        Err(DeliveryGateError::AlreadyVisible)
    );
}

#[test]
fn replacement_winning_the_race_rejects_the_old_activation() {
    let first = activation(1, 9);
    let mut gate = DeliveryGate::new(first);
    assert_eq!(gate.revoke_for_replacement(first).unwrap(), 10);
    let second = activation(2, 10);
    gate.activate(second).unwrap();
    assert_eq!(
        gate.mark_visible(first),
        Err(DeliveryGateError::ForeignAttempt)
    );
    assert!(!gate.output_visible());
    gate.mark_visible(second).unwrap();
    assert_eq!(gate.visible_to(), Some(second));
}

#[test]
fn initial_generation_is_consumed_exactly_and_foreign_generation_is_refused() {
    let first = activation(1, 42);
    let mut gate = DeliveryGate::new(first);
    assert_eq!(gate.generation(), 42);
    assert_eq!(
        gate.mark_visible(activation(1, 1)),
        Err(DeliveryGateError::ForeignAttempt)
    );
    assert_eq!(gate.revoke_for_replacement(first).unwrap(), 43);
    assert_eq!(
        gate.activate(activation(2, 42)),
        Err(DeliveryGateError::StaleEligibility)
    );
    gate.activate(activation(2, 43)).unwrap();
}

#[test]
fn closing_preserves_visibility_and_never_reopens_eligibility() {
    let first = activation(1, 1);
    let mut visible = DeliveryGate::new(first);
    visible.mark_visible(first).unwrap();
    visible.close();
    assert!(visible.output_visible());
    assert_eq!(visible.current(), None);
    assert_eq!(
        visible.activate(activation(2, 2)),
        Err(DeliveryGateError::AlreadyEnded)
    );
    let mut empty = DeliveryGate::new(first);
    empty.close();
    assert!(!empty.output_visible());
    assert_eq!(empty.phase(), DeliveryPhase::Ended);
}

#[test]
fn exhausted_eligibility_does_not_partially_revoke_current() {
    let first = activation(1, u64::MAX);
    let mut gate = DeliveryGate::new(first);
    assert_eq!(
        gate.revoke_for_replacement(first),
        Err(DeliveryGateError::EligibilityExhausted)
    );
    assert_eq!(gate.current(), Some(first));
}
