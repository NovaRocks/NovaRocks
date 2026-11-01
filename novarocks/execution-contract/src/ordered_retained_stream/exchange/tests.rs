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

use super::*;

fn sequence(n: u64) -> ExchangeUnitSequence {
    ExchangeUnitSequence::try_new(n).unwrap()
}

fn demand(n: u64) -> ExchangeUnitDemand {
    ExchangeUnitDemand {
        sequence: sequence(n),
        wire_bytes: 7,
    }
}

fn unit(n: u64, end_after_unit: bool) -> ExchangeUnitItem {
    ExchangeUnitItem::Unit {
        demand: demand(n),
        end_after_unit,
    }
}

fn grant(receipt: &mut ExchangeStreamReceipt, n: u64) {
    assert_eq!(
        receipt.register_demand(demand(n), 64).unwrap(),
        ExchangeDemandOutcome::Registered
    );
    assert_eq!(receipt.grant_next().unwrap(), Some(demand(n)));
}

fn claim(receipt: &mut ExchangeStreamReceipt, n: u64, end: bool) -> ExchangeDecodeToken {
    match receipt.begin_admission(unit(n, end)).unwrap() {
        ExchangeAdmissionStart::Decode(token) => token,
        other => panic!("expected a unique decode claim, got {other:?}"),
    }
}

#[test]
fn early_last_unit_and_end_wait_for_the_missing_data_prefix() {
    let mut receipt = ExchangeStreamReceipt::try_new(2).unwrap();
    grant(&mut receipt, 1);
    grant(&mut receipt, 2);
    let second = claim(&mut receipt, 2, true);
    assert_eq!(receipt.observed_end(), Some(sequence(3)));
    receipt.finish_decode(second).unwrap();
    assert_eq!(receipt.facts().custody_through, 0);
    assert_eq!(
        receipt.hand_off(sequence(2)),
        Err(ExchangeReceiptError::HandoffGap)
    );

    let first = claim(&mut receipt, 1, false);
    receipt.finish_decode(first).unwrap();
    assert_eq!(receipt.facts().custody_through, 3);
    assert_eq!(receipt.occupied_positions(), 2);
    assert_eq!(receipt.handed_off_through(), 0);
    assert_eq!(receipt.hand_off(sequence(1)).unwrap(), demand(1));
    assert_eq!(receipt.hand_off(sequence(2)).unwrap(), demand(2));
    assert_eq!(receipt.handed_off_through(), 2);
    receipt.complete_input_consumption().unwrap();
    receipt.complete_input_consumption().unwrap();
    receipt
        .close_receiver(ExchangeUnitCloseDisposition::Consumed {
            final_sequence: sequence(3),
        })
        .unwrap();
    assert_eq!(receipt.occupied_positions(), 0);
}

#[test]
fn standalone_early_end_and_combined_end_are_the_same_boundary() {
    let mut receipt = ExchangeStreamReceipt::try_new(1).unwrap();
    grant(&mut receipt, 1);
    assert_eq!(
        receipt
            .begin_admission(ExchangeUnitItem::End(sequence(2)))
            .unwrap(),
        ExchangeAdmissionStart::Observed(ExchangeAdmissionVerdict::Applied)
    );
    assert_eq!(receipt.facts().custody_through, 0);
    let first = claim(&mut receipt, 1, true);
    receipt.finish_decode(first).unwrap();
    assert_eq!(receipt.facts().custody_through, 2);
    assert_eq!(
        receipt
            .begin_admission(ExchangeUnitItem::End(sequence(2)))
            .unwrap(),
        ExchangeAdmissionStart::Observed(ExchangeAdmissionVerdict::Idempotent)
    );
    assert_eq!(
        receipt.begin_admission(ExchangeUnitItem::End(sequence(3))),
        Err(ExchangeReceiptError::EndConflict)
    );
    assert_eq!(
        receipt.begin_admission(unit(2, false)),
        Err(ExchangeReceiptError::EndConflict)
    );
}

#[test]
fn in_progress_replay_observes_facts_without_a_second_decode_token() {
    let mut receipt = ExchangeStreamReceipt::try_new(1).unwrap();
    grant(&mut receipt, 1);
    let original = claim(&mut receipt, 1, true);
    for _ in 0..3 {
        assert_eq!(
            receipt.begin_admission(unit(1, true)).unwrap(),
            ExchangeAdmissionStart::Observed(ExchangeAdmissionVerdict::Idempotent)
        );
        assert_eq!(receipt.decoding_positions(), 1);
        assert_eq!(receipt.facts().custody_through, 0);
    }
    receipt.finish_decode(original).unwrap();
    assert_eq!(receipt.facts().custody_through, 2);
    assert_eq!(receipt.decoding_positions(), 0);
    assert_eq!(receipt.occupied_positions(), 1);
}

#[test]
fn sender_stop_fences_late_demand_and_data_while_the_receiver_is_live() {
    let mut receipt = ExchangeStreamReceipt::try_new(2).unwrap();
    grant(&mut receipt, 1);
    assert_eq!(receipt.stop_sender(), vec![demand(1)]);
    assert!(receipt.facts().sender_stopped);
    assert_eq!(receipt.facts().receiver_closed, None);
    assert_eq!(receipt.facts().granted_through, 1);
    assert_eq!(receipt.occupied_positions(), 0);
    assert_eq!(
        receipt.register_demand(demand(1), 64).unwrap(),
        ExchangeDemandOutcome::Closed
    );
    assert_eq!(
        receipt.register_demand(demand(2), 64).unwrap(),
        ExchangeDemandOutcome::Closed
    );
    assert_eq!(
        receipt.begin_admission(unit(1, true)).unwrap(),
        ExchangeAdmissionStart::Closed
    );
    assert_eq!(receipt.grant_next().unwrap(), None);
    assert!(receipt.stop_sender().is_empty());
    assert_eq!(receipt.facts().custody_through, 0);
    assert_eq!(receipt.handed_off_through(), 0);
    assert_eq!(
        receipt.complete_input_consumption(),
        Err(ExchangeReceiptError::InputNotConsumed)
    );
}

#[test]
fn sender_stop_does_not_revoke_an_already_owned_decode() {
    let mut receipt = ExchangeStreamReceipt::try_new(2).unwrap();
    grant(&mut receipt, 1);
    grant(&mut receipt, 2);
    let original = claim(&mut receipt, 1, false);
    assert_eq!(receipt.stop_sender(), vec![demand(2)]);
    assert_eq!(receipt.occupied_positions(), 1);
    assert_eq!(
        receipt.finish_decode(original).unwrap(),
        ExchangeDecodeCommit::Committed
    );
    assert_eq!(receipt.facts().custody_through, 1);
    receipt.hand_off(sequence(1)).unwrap();
    assert_eq!(receipt.occupied_positions(), 0);
    assert_eq!(
        receipt.complete_input_consumption(),
        Err(ExchangeReceiptError::InputNotConsumed)
    );
}

#[test]
fn receiver_close_pins_decode_work_and_never_manufactures_consumption() {
    let mut receipt = ExchangeStreamReceipt::try_new(2).unwrap();
    grant(&mut receipt, 1);
    grant(&mut receipt, 2);
    let original = claim(&mut receipt, 1, false);
    assert_eq!(
        receipt
            .close_receiver(ExchangeUnitCloseDisposition::Withdrawn)
            .unwrap(),
        vec![demand(2)]
    );
    assert_eq!(receipt.occupied_positions(), 1);
    assert_eq!(receipt.decoding_positions(), 1);
    assert_eq!(receipt.facts().custody_through, 0);
    assert_eq!(receipt.handed_off_through(), 0);
    assert_eq!(
        receipt.finish_decode(original).unwrap(),
        ExchangeDecodeCommit::Discarded
    );
    assert_eq!(receipt.occupied_positions(), 0);
    assert_eq!(receipt.facts().custody_through, 0);
    assert!(
        receipt
            .close_receiver(ExchangeUnitCloseDisposition::Withdrawn)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn oversized_or_conflicting_demand_cannot_consume_a_position() {
    let mut receipt = ExchangeStreamReceipt::try_new(1).unwrap();
    assert_eq!(
        receipt.register_demand(demand(1), 6),
        Err(ExchangeReceiptError::UnitTooLarge {
            requested: 7,
            limit: 6
        })
    );
    assert_eq!(receipt.pending_demand(), None);
    assert_eq!(receipt.occupied_positions(), 0);
    assert_eq!(
        receipt.register_demand(demand(2), 64),
        Err(ExchangeReceiptError::DemandGap)
    );
    grant(&mut receipt, 1);
    assert_eq!(
        receipt.register_demand(demand(1), 64).unwrap(),
        ExchangeDemandOutcome::Idempotent
    );
    receipt.register_demand(demand(2), 64).unwrap();
    assert_eq!(
        receipt.grant_next(),
        Err(ExchangeReceiptError::PositionLimit)
    );
    assert_eq!(receipt.pending_demand(), Some(demand(2)));
    assert_eq!(
        receipt.begin_admission(unit(2, false)),
        Err(ExchangeReceiptError::MissingGrant)
    );
    let first = claim(&mut receipt, 1, false);
    receipt.finish_decode(first).unwrap();
    receipt.hand_off(sequence(1)).unwrap();
    assert_eq!(receipt.grant_next().unwrap(), Some(demand(2)));
}

#[test]
fn empty_end_needs_no_grant_but_still_needs_real_input_completion() {
    let mut receipt = ExchangeStreamReceipt::try_new(1).unwrap();
    assert_eq!(
        receipt.begin_admission(ExchangeUnitItem::End(sequence(2))),
        Err(ExchangeReceiptError::EndConflict)
    );
    receipt
        .begin_admission(ExchangeUnitItem::End(sequence(1)))
        .unwrap();
    assert_eq!(receipt.facts().custody_through, 1);
    assert_eq!(receipt.occupied_positions(), 0);
    assert_eq!(receipt.handed_off_through(), 0);
    assert_eq!(
        receipt.close_receiver(ExchangeUnitCloseDisposition::Consumed {
            final_sequence: sequence(1)
        }),
        Err(ExchangeReceiptError::InputNotConsumed)
    );
    receipt.complete_input_consumption().unwrap();
    receipt
        .close_receiver(ExchangeUnitCloseDisposition::Consumed {
            final_sequence: sequence(1),
        })
        .unwrap();
}

#[test]
fn unit_evidence_and_end_type_are_checked_before_duplicate_classification() {
    let mut receipt = ExchangeStreamReceipt::try_new(1).unwrap();
    grant(&mut receipt, 1);
    let token = claim(&mut receipt, 1, true);
    assert_eq!(
        receipt.begin_admission(unit(1, false)),
        Err(ExchangeReceiptError::UnitConflict)
    );
    receipt.finish_decode(token).unwrap();
    receipt.hand_off(sequence(1)).unwrap();
    assert_eq!(
        receipt.begin_admission(unit(2, false)),
        Err(ExchangeReceiptError::EndConflict)
    );
    assert_eq!(
        receipt.register_demand(demand(2), 64),
        Err(ExchangeReceiptError::EndConflict)
    );
}
