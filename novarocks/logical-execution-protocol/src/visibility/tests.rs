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
use crate::test_support::*;

fn stream() -> VisibilityState {
    VisibilityState::new(LogicalOutputMode::ResultStream, Some(carrier()))
}
fn opened() -> VisibilityState {
    let mut state = stream();
    assert_eq!(
        state.open_delivery(activation(1), root(1), carrier(), true, false),
        Ok(OpenDecision::WriteSchema)
    );
    state.schema_written().unwrap();
    state
}
fn summary(packets: u64) -> EndSummary {
    EndSummary {
        end: end(packets, 17),
        accepted_packets: packets,
        delivered_through: packets.checked_sub(1),
    }
}
#[test]
fn open_rejects_conclusion_frozen_foreign_and_carrier_before_any_schema() {
    let mut state = stream();
    assert_eq!(
        state.open_delivery(activation(1), root(1), carrier(), true, true),
        Err(ResultPathRefusal::Concluded)
    );
    assert_eq!(
        state.open_delivery(activation(1), root(1), carrier(), false, false),
        Err(ResultPathRefusal::ForeignActivation)
    );
    let wrong = RootCarrier {
        kind: novarocks_result_contract::RootOutputKind::CountOnly,
        client_rows: None,
    };
    assert_eq!(
        state.open_delivery(activation(1), root(1), wrong, true, false),
        Err(ResultPathRefusal::CarrierMismatch)
    );
    assert_eq!(state.schema(), SchemaState::NotStarted);
    state
        .open_delivery(activation(1), root(1), carrier(), true, false)
        .unwrap();
    assert_eq!(
        state.failure(activation(1), true, false),
        Ok(FailureDisposition::Freeze)
    );
    assert_eq!(
        state.open_delivery(activation(1), root(1), carrier(), true, false),
        Err(ResultPathRefusal::Frozen)
    );
}
#[test]
fn original_schema_receipt_survives_replacement_and_is_never_reissued() {
    let mut state = stream();
    state
        .open_delivery(activation(1), root(1), carrier(), true, false)
        .unwrap();
    state.failure(activation(1), true, false).unwrap();
    assert_eq!(
        state.open_delivery(activation(2), root(2), carrier(), true, false),
        Ok(OpenDecision::Opened)
    );
    assert_eq!(
        state.grant_visibility(activation(2), true, false),
        Err(ResultPathRefusal::SchemaPending)
    );
    state.schema_written().unwrap();
    state.grant_visibility(activation(2), true, false).unwrap();
    assert_eq!(state.schema(), SchemaState::Done);
    assert_eq!(state.visible_to(), Some(activation(2)));
}
#[test]
fn internal_delivery_is_granted_without_client_visibility() {
    let mut state = VisibilityState::new(LogicalOutputMode::InternalResultStream, Some(carrier()));
    assert_eq!(
        state.open_delivery(activation(1), root(1), carrier(), true, false),
        Ok(OpenDecision::DeliveryGranted)
    );
    assert_eq!(state.visible_to(), None);
    assert_eq!(
        state.failure(activation(1), true, false),
        Ok(FailureDisposition::Freeze)
    );
}
#[test]
fn reserved_position_visibility_closes_recovery_before_enqueue() {
    let mut state = opened();
    assert_eq!(
        state.failure(activation(1), true, false),
        Ok(FailureDisposition::Freeze)
    );
    assert_eq!(
        state.grant_visibility(activation(1), true, false),
        Err(ResultPathRefusal::Frozen)
    );
    let mut state = opened();
    state.grant_visibility(activation(1), true, false).unwrap();
    assert_eq!(
        state.failure(activation(1), true, false),
        Ok(FailureDisposition::Conclude)
    );
}
#[test]
fn exact_empty_and_nonempty_end_frontiers_are_independently_checked() {
    for packets in [0, 1, 4] {
        assert_eq!(summary(packets).validate(), Ok(()));
        let mut wrong = summary(packets);
        wrong.accepted_packets += 1;
        assert_eq!(wrong.validate(), Err(ResultPathRefusal::InvalidEnd));
        let mut wrong = summary(packets);
        wrong.delivered_through = Some(packets);
        assert_eq!(wrong.validate(), Err(ResultPathRefusal::InvalidEnd));
    }
}
#[test]
fn end_requires_original_root_and_cannot_change_after_consumption() {
    let mut state = opened();
    assert_eq!(
        state.end_consumed(activation(1), root(2), summary(0), true, false),
        Err(ResultPathRefusal::ConflictingRoot)
    );
    state
        .end_consumed(activation(1), root(1), summary(0), true, false)
        .unwrap();
    state
        .end_consumed(activation(1), root(1), summary(0), true, false)
        .unwrap();
    assert_eq!(
        state.end_consumed(activation(1), root(1), summary(1), true, false),
        Err(ResultPathRefusal::EndAlreadyConsumed)
    );
}
#[test]
fn each_success_gate_prevents_end_reservation_until_its_original_fact_arrives() {
    for (finished, sealed, establish) in [
        (false, true, true),
        (true, false, true),
        (true, true, false),
    ] {
        let mut state = opened();
        state
            .end_consumed(activation(1), root(1), summary(2), true, false)
            .unwrap();
        assert_eq!(
            state.request_end_slot(activation(1), finished, sealed, establish),
            Ok(false)
        );
        assert_eq!(
            state.request_end_slot(activation(1), true, true, true),
            Ok(true)
        );
        assert_eq!(
            state.request_end_slot(activation(1), true, true, true),
            Ok(false)
        );
    }
}
#[test]
fn empty_end_still_waits_for_original_schema_acknowledgement() {
    let mut state = stream();
    state
        .open_delivery(activation(1), root(1), carrier(), true, false)
        .unwrap();
    state
        .end_consumed(activation(1), root(1), summary(0), true, false)
        .unwrap();
    assert_eq!(
        state.request_end_slot(activation(1), true, true, true),
        Ok(false)
    );
    state.schema_written().unwrap();
    assert_eq!(
        state.request_end_slot(activation(1), true, true, true),
        Ok(true)
    );
}
#[test]
fn success_is_committed_before_end_enqueue_and_late_transport_cannot_revoke_it() {
    let mut state = opened();
    state
        .end_consumed(activation(1), root(1), summary(0), true, false)
        .unwrap();
    assert_eq!(
        state.commit_end_slot(activation(1)),
        Err(ResultPathRefusal::EndSlotNotRequested)
    );
    state
        .request_end_slot(activation(1), true, true, true)
        .unwrap();
    state.commit_end_slot(activation(1)).unwrap();
    assert!(state.committed());
    assert_eq!(
        state.failure(activation(1), true, false),
        Ok(FailureDisposition::Residual)
    );
}
#[test]
fn duplicate_open_and_foreign_root_do_not_replace_the_registered_root() {
    let mut state = opened();
    assert_eq!(
        state.open_delivery(activation(1), root(1), carrier(), true, false),
        Err(ResultPathRefusal::DuplicateOpen)
    );
    assert_eq!(
        state.open_delivery(activation(1), root(2), carrier(), true, false),
        Err(ResultPathRefusal::ForeignActivation)
    );
    assert_eq!(state.root(activation(1)), Some(root(1)));
}

#[test]
fn failure_before_delivery_opens_freezes_the_same_activation_only() {
    let mut state = stream();
    assert_eq!(
        state.failure(activation(1), true, false),
        Ok(FailureDisposition::Freeze)
    );
    assert_eq!(
        state.open_delivery(activation(1), root(1), carrier(), true, false),
        Err(ResultPathRefusal::Frozen)
    );
    assert_eq!(
        state.open_delivery(activation(2), root(2), carrier(), true, false),
        Ok(OpenDecision::WriteSchema)
    );
}
