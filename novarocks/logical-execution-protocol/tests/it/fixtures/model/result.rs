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

use super::Seed;
use crate::support::*;
use novarocks_logical_execution_protocol::failure::result_delivery_failed;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Disposition {
    Completed,
    Failed,
    Dropped,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReceiptKind {
    Schema,
    Body { sequence: u64, rows: u64 },
    End,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientWinner {
    Success,
    Cancelled,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Event {
    RelayStarted(AttemptActivationIdentity),
    Opened(AttemptActivationIdentity),
    PacketAccepted {
        sequence: u64,
        rows: u64,
    },
    SlotReserved {
        slot: u64,
        sequence: u64,
    },
    SlotDropped(u64),
    VisibilityRequested {
        token: LogicalRequestToken,
        slot: u64,
    },
    VisibilityGranted {
        activation: AttemptActivationIdentity,
        token: LogicalRequestToken,
    },
    Reserved {
        receipt: u64,
        kind: ReceiptKind,
    },
    Settled {
        receipt: u64,
        disposition: Disposition,
    },
    Visible {
        activation: AttemptActivationIdentity,
        slot: u64,
        receipt: u64,
        permission: LogicalRequestToken,
    },
    EndConsumed {
        sequence: u64,
        rows: u64,
        accepted: u64,
        delivered: Option<u64>,
    },
    ProtocolSealRequested(AttemptRequestToken),
    ProtocolSealGranted(AttemptRequestToken),
    EndSlotReserved(LogicalActionId),
    EndSlotReady(LogicalActionId),
    LogicalSuccessPromised,
    EndQueued {
        receipt: u64,
    },
    ClientEndReceived {
        receipt: u64,
    },
    StatementSealAttempt,
    StatementSuccessSealed,
    StatementSealRejected,
    ClientEndWritten,
    Cancelled,
    ConsumerClosed,
    AckHandedOff,
    Stopped,
}

/// Independent external history. LP's queue promise and the external statement
/// success seal are different events; a late scope cancellation may win the
/// latter without retracting the LP conclusion.
#[derive(Default, Debug)]
pub(crate) struct History(pub(crate) Vec<Event>);
impl History {
    pub(crate) fn check(&self) {
        let mut started = None;
        let mut opened = None;
        let mut schema_receipt = None;
        let mut schema = false;
        let mut accepted = BTreeMap::new();
        let mut slots = BTreeMap::new();
        let mut used_slots = Vec::new();
        let mut requests = Vec::new();
        let mut granted = None;
        let mut receipts = BTreeMap::new();
        let mut settled = BTreeMap::new();
        let mut end_consumed = false;
        let mut seal_request = None;
        let mut protocol_seal = false;
        let mut end_slot = None;
        let mut end_slot_ready = false;
        let mut logical_success = false;
        let mut queued_end = None;
        let mut received_end = false;
        let mut client_winner = None;
        let mut statement_sealed = false;
        let mut client_end_written = false;
        let mut closed = false;
        for event in &self.0 {
            match *event {
                Event::RelayStarted(a) => assert!(started.replace(a).is_none()),
                Event::Opened(a) => {
                    assert_eq!(started, Some(a));
                    assert!(opened.replace(a).is_none());
                }
                Event::PacketAccepted { sequence, rows } => {
                    assert!(opened.is_some() && sequence == accepted.len() as u64 + 1);
                    assert!(accepted.insert(sequence, rows).is_none());
                }
                Event::SlotReserved { slot, sequence } => {
                    assert!(
                        accepted.contains_key(&sequence) && slots.insert(slot, sequence).is_none()
                    );
                }
                Event::SlotDropped(slot) => {
                    assert!(slots.contains_key(&slot) && !used_slots.contains(&slot));
                    used_slots.push(slot);
                }
                Event::VisibilityRequested { token, slot } => {
                    assert!(slots.contains_key(&slot) && !used_slots.contains(&slot));
                    assert!(
                        requests.is_empty(),
                        "first visibility is one activation-level grant"
                    );
                    requests.push(token);
                }
                Event::VisibilityGranted { activation, token } => {
                    assert_eq!(opened, Some(activation));
                    assert!(
                        schema
                            && requests.contains(&token)
                            && granted.replace((activation, token)).is_none()
                    );
                }
                Event::Reserved { receipt, kind } => {
                    if kind == ReceiptKind::Schema {
                        assert!(
                            schema_receipt.replace(receipt).is_none(),
                            "schema is issued once"
                        );
                    }
                    if let ReceiptKind::Body { sequence, rows } = kind {
                        assert_eq!(accepted.get(&sequence), Some(&rows));
                    }
                    assert!(receipts.insert(receipt, kind).is_none());
                }
                Event::Settled {
                    receipt,
                    disposition,
                } => {
                    let kind = receipts
                        .get(&receipt)
                        .expect("settled original receipt exists");
                    assert!(
                        settled.insert(receipt, disposition).is_none(),
                        "receipt settles once"
                    );
                    if *kind == ReceiptKind::Schema && disposition == Disposition::Completed {
                        assert!(!schema);
                        schema = true;
                    }
                }
                Event::Visible {
                    activation,
                    slot,
                    receipt,
                    permission,
                } => {
                    assert!(schema && !closed && !end_consumed);
                    assert_eq!(granted, Some((activation, permission)));
                    let Some(ReceiptKind::Body { sequence, .. }) = receipts.get(&receipt) else {
                        panic!("body receipt required")
                    };
                    assert_eq!(slots.get(&slot), Some(sequence));
                    assert!(!used_slots.contains(&slot));
                    used_slots.push(slot);
                }
                Event::EndConsumed {
                    sequence,
                    rows,
                    accepted: reported,
                    delivered,
                } => {
                    assert!(schema && !end_consumed);
                    end_consumed = true;
                    let count = accepted.len() as u64;
                    assert_eq!(sequence, count + 1);
                    assert_eq!(rows, accepted.values().sum::<u64>());
                    assert_eq!(reported, count);
                    assert_eq!(delivered, count.checked_sub(1));
                    assert_eq!(slots.len(), used_slots.len());
                    for (id, kind) in &receipts {
                        if matches!(kind, ReceiptKind::Body { .. }) {
                            assert_eq!(settled.get(id), Some(&Disposition::Completed));
                        }
                    }
                }
                Event::ProtocolSealRequested(token) => {
                    assert!(end_consumed && seal_request.replace(token).is_none())
                }
                Event::ProtocolSealGranted(token) => {
                    assert_eq!(seal_request, Some(token));
                    assert!(!protocol_seal);
                    protocol_seal = true;
                }
                Event::EndSlotReserved(action) => {
                    assert!(protocol_seal && end_slot.replace(action).is_none())
                }
                Event::EndSlotReady(action) => {
                    assert_eq!(end_slot, Some(action));
                    assert!(!end_slot_ready);
                    end_slot_ready = true;
                }
                Event::LogicalSuccessPromised => {
                    assert!(protocol_seal && end_slot_ready && !logical_success);
                    logical_success = true;
                }
                Event::EndQueued { receipt } => {
                    assert!(logical_success && queued_end.replace(receipt).is_none());
                    assert_eq!(receipts.get(&receipt), Some(&ReceiptKind::End));
                }
                Event::ClientEndReceived { receipt } => {
                    assert!(!closed && !received_end);
                    received_end = true;
                    assert_eq!(queued_end, Some(receipt));
                    assert_eq!(settled.get(&receipt), Some(&Disposition::Completed));
                }
                Event::StatementSealAttempt => {
                    assert!(received_end);
                    client_winner.get_or_insert(ClientWinner::Success);
                }
                Event::StatementSuccessSealed => {
                    assert_eq!(client_winner, Some(ClientWinner::Success));
                    assert!(received_end && !statement_sealed);
                    statement_sealed = true;
                }
                Event::StatementSealRejected => {
                    assert_eq!(client_winner, Some(ClientWinner::Cancelled))
                }
                Event::ClientEndWritten => {
                    assert!(received_end && statement_sealed && !closed && !client_end_written);
                    assert_eq!(client_winner, Some(ClientWinner::Success));
                    client_end_written = true;
                }
                Event::Cancelled => {
                    client_winner.get_or_insert(ClientWinner::Cancelled);
                }
                Event::ConsumerClosed => closed = true,
                Event::AckHandedOff => assert!(end_consumed && logical_success),
                Event::Stopped => {
                    assert_eq!(receipts.len(), settled.len());
                    assert_eq!(slots.len(), used_slots.len());
                }
            }
        }
        assert_eq!(
            receipts.len(),
            settled.len(),
            "every accepted external receipt settles"
        );
        assert_eq!(
            slots.len(),
            used_slots.len(),
            "every original slot is consumed or dropped"
        );
    }
}

struct Receipt {
    id: u64,
    kind: ReceiptKind,
    due: u64,
    outcome: Disposition,
    action: Option<LogicalActionId>,
    settled: bool,
}
struct PendingPacket {
    slot: u64,
    sequence: u64,
    rows: u64,
    outcome: Disposition,
}

/// Consumes actual Core permission effects. Queue-slot ownership precedes the
/// first visibility request, while later packets reuse that activation grant.
/// No receipt or external statement latch decides LP recovery or logical success.
pub(crate) struct ResultPort {
    seed: Seed,
    tick: u64,
    receipts: Vec<Receipt>,
    start: LogicalActionId,
    activation: AttemptActivationIdentity,
    accepted: u64,
    next_slot: u64,
    pending: Option<PendingPacket>,
    permission: Option<LogicalRequestToken>,
    relay_failure: Option<novarocks_coordination_contract::QueryExecutionError>,
    client_winner: Option<ClientWinner>,
    closed: bool,
    history: History,
}
impl ResultPort {
    pub(crate) fn attach(f: &mut Fixture, seed: u64) -> Self {
        Self::with_schema(f, seed, Disposition::Completed)
    }
    pub(crate) fn with_schema(f: &mut Fixture, seed: u64, schema: Disposition) -> Self {
        let start = f.take(|b| matches!(b, LogicalEffectBody::Relay(RelayEffect::Start { .. })));
        let LogicalEffectBody::Relay(RelayEffect::Start { activation: actual }) = start.body else {
            unreachable!()
        };
        let mut port = Self {
            seed: Seed::new(seed),
            tick: 0,
            receipts: Vec::new(),
            start: start.action,
            activation: actual,
            accepted: 0,
            next_slot: 1,
            pending: None,
            permission: None,
            relay_failure: None,
            client_winner: None,
            closed: false,
            history: History::default(),
        };
        port.history.0.push(Event::RelayStarted(actual));
        let token = f.relay(RelayRequest::OpenDelivery {
            root: root(1),
            carrier: carrier(false),
        });
        port.reply(f, token, |r| matches!(r, RelayResponse::Opened));
        port.history.0.push(Event::Opened(actual));
        let effect = f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
            )
        });
        port.reserve(ReceiptKind::Schema, schema, Some(effect.action));
        port
    }
    fn reply(
        &mut self,
        f: &mut Fixture,
        token: LogicalRequestToken,
        expected: impl Fn(&RelayResponse) -> bool,
    ) {
        let effect = f.take(|b| matches!(b, LogicalEffectBody::Relay(RelayEffect::Reply { token: actual, .. }) if *actual == token));
        let LogicalEffectBody::Relay(RelayEffect::Reply {
            activation: actual,
            response,
            ..
        }) = effect.body
        else {
            unreachable!()
        };
        assert_eq!(actual, self.activation);
        assert!(expected(&response), "original response: {response:?}");
        f.ack(effect.action, ActionResult::Relay(RelayFact::Applied));
    }
    fn reserve(
        &mut self,
        kind: ReceiptKind,
        outcome: Disposition,
        action: Option<LogicalActionId>,
    ) -> u64 {
        let id = self.receipts.len() as u64 + 1;
        self.history.0.push(Event::Reserved { receipt: id, kind });
        self.receipts.push(Receipt {
            id,
            kind,
            due: self.tick + self.seed.delay(),
            outcome,
            action,
            settled: false,
        });
        id
    }
    pub(crate) fn advance(&mut self, f: &mut Fixture) {
        self.tick += 1;
        let mut failed_body = false;
        for receipt in &mut self.receipts {
            if receipt.settled || receipt.due > self.tick {
                continue;
            }
            receipt.settled = true;
            self.history.0.push(Event::Settled {
                receipt: receipt.id,
                disposition: receipt.outcome,
            });
            failed_body |= matches!(receipt.kind, ReceiptKind::Body { .. })
                && receipt.outcome != Disposition::Completed;
            if receipt.kind == ReceiptKind::End && receipt.outcome == Disposition::Completed {
                self.history.0.push(Event::ClientEndReceived {
                    receipt: receipt.id,
                });
                self.history.0.push(Event::StatementSealAttempt);
                let winner = *self.client_winner.get_or_insert(ClientWinner::Success);
                if winner == ClientWinner::Success {
                    self.history.0.push(Event::StatementSuccessSealed);
                    self.history.0.push(Event::ClientEndWritten);
                } else {
                    self.history.0.push(Event::StatementSealRejected);
                }
            }
            if let Some(action) = receipt.action {
                let result = match (receipt.kind, receipt.outcome) {
                    (ReceiptKind::Schema, Disposition::Completed) => ConsumerFact::SchemaWritten,
                    (ReceiptKind::End, Disposition::Completed) => ConsumerFact::EndSettled,
                    (_, Disposition::Failed | Disposition::Dropped) => {
                        ConsumerFact::Failed(result_delivery_failed())
                    }
                    _ => unreachable!(),
                };
                f.ack(action, ActionResult::Consumer(result));
            }
        }
        if failed_body && self.relay_failure.is_none() {
            let error = result_delivery_failed();
            self.relay_failure = Some(error.clone());
            let token = f.relay(RelayRequest::DeliveryFailed(FailureReport {
                error,
                original_attempt_failure: None,
                pre_ready_replacement: None,
            }));
            self.reply(f, token, |r| matches!(r, RelayResponse::Applied));
        }
    }
    pub(crate) fn settle_all(&mut self, f: &mut Fixture) {
        for _ in 0..4 {
            self.advance(f);
        }
        assert!(self.receipts.iter().all(|r| r.settled));
    }
    pub(crate) fn reserve_packet(&mut self, sequence: u64, rows: u64, outcome: Disposition) {
        assert!(!self.closed && self.pending.is_none() && sequence == self.accepted + 1);
        self.accepted += 1;
        let slot = self.next_slot;
        self.next_slot += 1;
        self.history
            .0
            .push(Event::PacketAccepted { sequence, rows });
        self.history.0.push(Event::SlotReserved { slot, sequence });
        self.pending = Some(PendingPacket {
            slot,
            sequence,
            rows,
            outcome,
        });
    }
    pub(crate) fn visibility(&mut self, f: &mut Fixture) -> LogicalRequestToken {
        let slot = self
            .pending
            .as_ref()
            .expect("first packet and original slot precede permission")
            .slot;
        assert!(self.permission.is_none());
        let token = f.relay(RelayRequest::RequestVisibility);
        self.history
            .0
            .push(Event::VisibilityRequested { token, slot });
        token
    }
    pub(crate) fn reject_visibility(&mut self, f: &mut Fixture, token: LogicalRequestToken) {
        self.reply(f, token, |r| matches!(r, RelayResponse::Rejected(_)));
    }
    pub(crate) fn accept_visibility(&mut self, f: &mut Fixture, token: LogicalRequestToken) {
        self.reply(f, token, |r| matches!(r, RelayResponse::VisibilityGranted));
        self.history.0.push(Event::VisibilityGranted {
            activation: self.activation,
            token,
        });
        assert!(self.permission.replace(token).is_none());
    }
    pub(crate) fn deliver_packet(&mut self) {
        assert!(!self.closed);
        let permission = self
            .permission
            .expect("actual first-visibility grant required");
        let packet = self
            .pending
            .take()
            .expect("original packet and queue slot retained");
        let receipt = self.reserve(
            ReceiptKind::Body {
                sequence: packet.sequence,
                rows: packet.rows,
            },
            packet.outcome,
            None,
        );
        self.history.0.push(Event::Visible {
            activation: self.activation,
            slot: packet.slot,
            receipt,
            permission,
        });
    }
    pub(crate) fn consume_end(
        &mut self,
        f: &mut Fixture,
        original: RootResultEnd,
    ) -> LogicalRequestToken {
        assert!(
            self.pending.is_none()
                && self
                    .receipts
                    .iter()
                    .all(|r| r.settled && r.outcome == Disposition::Completed)
        );
        let delivered = self.accepted.checked_sub(1);
        self.history.0.push(Event::EndConsumed {
            sequence: original.sequence.get(),
            rows: original.output_rows,
            accepted: self.accepted,
            delivered,
        });
        f.relay(RelayRequest::EndConsumed {
            root: root(1),
            end: original,
            accepted_packets: self.accepted,
            delivered_through: delivered,
        })
    }
    pub(crate) fn observe_success_seal(&mut self, token: AttemptRequestToken) {
        // Observation supplied by the world/AP endpoint; the result port does
        // not issue or choose a protocol seal verdict.
        self.history.0.push(Event::ProtocolSealRequested(token));
        self.history.0.push(Event::ProtocolSealGranted(token));
    }
    pub(crate) fn reserve_end_slot(&mut self, f: &mut Fixture) -> LogicalActionId {
        let effect = f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Consumer(ConsumerEffect::ReserveEndSlot { .. })
            )
        });
        self.history.0.push(Event::EndSlotReserved(effect.action));
        effect.action
    }
    pub(crate) fn end_slot_ready(&mut self, f: &mut Fixture, action: LogicalActionId) {
        self.history.0.push(Event::EndSlotReady(action));
        f.ack(action, ActionResult::Consumer(ConsumerFact::EndSlotReady));
    }
    pub(crate) fn enqueue_end(
        &mut self,
        f: &mut Fixture,
        token: LogicalRequestToken,
        outcome: Disposition,
    ) {
        let effect = f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
            )
        });
        self.history.0.push(Event::LogicalSuccessPromised);
        let receipt = self.reserve(ReceiptKind::End, outcome, Some(effect.action));
        self.history.0.push(Event::EndQueued { receipt });
        self.reply(f, token, |r| matches!(r, RelayResponse::Applied));
    }
    pub(crate) fn cancel(&mut self, f: &mut Fixture) {
        self.history.0.push(Event::Cancelled);
        self.client_winner.get_or_insert(ClientWinner::Cancelled);
        f.send(LogicalInput::Cancelled(novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::ClientDisconnected));
    }
    pub(crate) fn close_consumer(&mut self, f: &mut Fixture) {
        self.closed = true;
        self.history.0.push(Event::ConsumerClosed);
        for receipt in &mut self.receipts {
            if !receipt.settled {
                receipt.outcome = Disposition::Dropped;
            }
        }
        f.send(LogicalInput::ConsumerClosed {
            source: LogicalSourceId::new(std::num::NonZeroU64::new(3).unwrap()),
        });
    }
    pub(crate) fn relay_end_handoff(&mut self, f: &mut Fixture, original: RootResultEnd) {
        self.history.0.push(Event::AckHandedOff);
        f.ack(
            self.start,
            ActionResult::Relay(RelayFact::EndRelayed {
                activation: self.activation,
                root: root(1),
                end: original,
            }),
        );
    }
    pub(crate) fn stopped(&mut self, f: &mut Fixture) {
        if let Some(packet) = self.pending.take() {
            self.history.0.push(Event::SlotDropped(packet.slot));
        }
        assert!(self.receipts.iter().all(|r| r.settled));
        let stop = f.take(|b| matches!(b, LogicalEffectBody::Relay(RelayEffect::Stop { .. })));
        self.history.0.push(Event::Stopped);
        f.ack(
            stop.action,
            ActionResult::Relay(RelayFact::Stopped {
                activation: self.activation,
                all_deliveries_settled: true,
            }),
        );
        let result = match self.relay_failure.clone() {
            Some(error) => RelayFact::Failed {
                activation: self.activation,
                failure: FailureReport {
                    error,
                    original_attempt_failure: None,
                    pre_ready_replacement: None,
                },
                all_deliveries_settled: true,
            },
            None => RelayFact::Stopped {
                activation: self.activation,
                all_deliveries_settled: true,
            },
        };
        f.ack(self.start, ActionResult::Relay(result));
    }
    pub(crate) fn history(&self) -> &History {
        &self.history
    }
}
