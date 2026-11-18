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

//! ScalarValueV1 is an existing named pure domain/codec. These complete value
//! proofs are not a production WriteReady, statistics artifact, lease or Join.
use super::Seed;
use crate::support::*;
use novarocks_coordination_contract::{
    AttemptComputationCut, QueryExecutionError, QueryFailureReason,
};
use novarocks_result_contract::{
    BorrowedScalarLeaf, BorrowedScalarRecord, InternalResultDomain, RootOutputKind, ScalarField,
    ScalarLeafCursor, ScalarSchema, ScalarValueType,
};
use std::sync::{Arc, Weak};

pub(crate) struct ScalarPayload {
    schema: ScalarSchema,
    bytes: Vec<u8>,
    expected: i64,
}
impl ScalarPayload {
    pub(crate) fn new(value: i64) -> Self {
        let schema = ScalarSchema::try_new(ScalarField {
            nullable: false,
            value_type: ScalarValueType::SignedInteger(64),
        })
        .unwrap()
        .bind_native_slots(&[0])
        .unwrap();
        let cursor = ScalarLeafCursor::try_new(
            &schema,
            BorrowedScalarLeaf::SignedInteger { bits: 64, value },
        )
        .unwrap();
        let mut bytes = vec![0; cursor.encoded_len()];
        let copied = cursor.copy_range(0, &mut bytes).unwrap();
        assert_eq!(copied.emitted_bytes, bytes.len());
        Self {
            schema,
            bytes,
            expected: value,
        }
    }
    pub(crate) fn corrupt(&mut self) {
        self.bytes.pop();
    }
    fn decoded(&self) -> Option<(i64, u64)> {
        let record = BorrowedScalarRecord::try_decode(&self.schema, &self.bytes).ok()?;
        let BorrowedScalarLeaf::SignedInteger { bits: 64, value } =
            BorrowedScalarLeaf::decode(&self.schema, &self.bytes).ok()?
        else {
            return None;
        };
        (value == self.expected).then_some((value, record.rows()))
    }
    pub(crate) fn value(&self) -> i64 {
        self.expected
    }
}
pub(crate) fn scalar_start() -> LogicalStart {
    let mut original = start(LogicalOutputMode::InternalResultStream);
    original.policy.root_carrier = Some(scalar_carrier());
    original.policy.effect = ExecutionEffect::External;
    original
}
fn scalar_carrier() -> RootCarrier {
    RootCarrier {
        kind: RootOutputKind::InternalFacts(InternalResultDomain::ScalarValueV1),
        client_rows: None,
    }
}

#[derive(Clone, Debug)]
pub(crate) enum InternalEvent {
    Frozen {
        root: TaskIdentity,
        plan: LogicalPlanIdentity,
        value: i64,
    },
    Registered,
    WaitingForDomain,
    Decoded {
        value: i64,
        rows: u64,
    },
    ProvisionalEnd {
        receipt: InternalEndReceiptId,
        end: RootResultEnd,
    },
    Proposed {
        token: LogicalRequestToken,
        root: TaskIdentity,
        plan: LogicalPlanIdentity,
        receipt: InternalEndReceiptId,
        end: RootResultEnd,
    },
    Rejected(LogicalRequestToken),
    SealRequested(AttemptRequestToken),
    SealGranted(AttemptRequestToken),
    SourceClosed,
    ExitVerdict {
        failed: bool,
    },
    DomainRejected,
    Ready {
        token: LogicalRequestToken,
        cut: AttemptComputationCut,
    },
}
#[derive(Default)]
pub(crate) struct InternalHistory(pub(crate) Vec<InternalEvent>);
impl InternalHistory {
    pub(crate) fn check(&self) {
        let mut frozen = None;
        let mut registered = false;
        let mut decoded = None;
        let mut original_end = None;
        let mut proposals = Vec::new();
        let mut rejected = Vec::new();
        let mut seal_requested = None;
        let mut seal = false;
        let mut closed = false;
        let mut exit_failed = None;
        let mut domain_rejected = false;
        let mut ready = false;
        for event in &self.0 {
            match *event {
                InternalEvent::Frozen { root, plan, value } => {
                    assert!(frozen.replace((root, plan, value)).is_none());
                }
                InternalEvent::Registered => {
                    assert!(!registered);
                    registered = true;
                }
                InternalEvent::WaitingForDomain => assert!(registered && decoded.is_none()),
                InternalEvent::Decoded { value, rows } => {
                    assert!(registered && rows == 1 && value == frozen.unwrap().2);
                    assert!(decoded.replace((value, rows)).is_none());
                }
                InternalEvent::ProvisionalEnd { receipt, end } => {
                    assert_eq!(decoded.unwrap().1, end.output_rows);
                    assert_eq!(end.sequence.get(), 2);
                    assert!(original_end.replace((receipt, end)).is_none());
                }
                InternalEvent::Proposed {
                    token,
                    root,
                    plan,
                    receipt,
                    end,
                } => {
                    proposals.push((token, root, plan, receipt, end));
                }
                InternalEvent::Rejected(token) => {
                    assert!(!rejected.contains(&token));
                    rejected.push(token);
                }
                InternalEvent::SealRequested(token) => {
                    assert!(seal_requested.replace(token).is_none());
                }
                InternalEvent::SealGranted(token) => {
                    assert_eq!(seal_requested, Some(token));
                    assert!(!seal);
                    seal = true;
                }
                InternalEvent::SourceClosed => closed = true,
                InternalEvent::ExitVerdict { failed } => {
                    assert!(exit_failed.replace(failed).is_none());
                }
                InternalEvent::DomainRejected => domain_rejected = true,
                InternalEvent::Ready { token, cut } => {
                    assert!(!ready && seal && !domain_rejected && !closed && exit_failed.is_none());
                    let (_, root, plan, receipt, end) =
                        *proposals.iter().find(|p| p.0 == token).unwrap();
                    let original = frozen.unwrap();
                    assert_eq!((root, plan), (original.0, original.1));
                    assert_eq!(Some((receipt, end)), original_end);
                    assert!(!rejected.contains(&token));
                    assert_eq!((cut.root, cut.end), (root, end));
                    assert_eq!(cut.activation.execution(), root.query_execution_id());
                    ready = true;
                }
            }
        }
    }
}

/// Only this actual PublishReady receiving path constructs a modeled accepted
/// value. It retains the SAME codec-backed scalar payload, not a forged token.
pub(crate) struct AcceptedScalar {
    cut: AttemptComputationCut,
    payload: Arc<ScalarPayload>,
}
impl AcceptedScalar {
    pub(crate) fn cut(&self) -> AttemptComputationCut {
        self.cut
    }
    pub(crate) fn value(&self) -> i64 {
        self.payload.value()
    }
    pub(crate) fn weak_payload(&self) -> Weak<ScalarPayload> {
        Arc::downgrade(&self.payload)
    }
}
pub(crate) struct InternalPort {
    decode_wait: u64,
    decode_finished: bool,
    domain_complete: bool,
    payload: Option<Arc<ScalarPayload>>,
    receipt: InternalEndReceiptId,
    end: RootResultEnd,
    proposal: Option<LogicalRequestToken>,
    relay: LogicalActionId,
    history: InternalHistory,
}
impl InternalPort {
    pub(crate) fn attach(f: &mut Fixture, seed: u64, payload: ScalarPayload) -> Self {
        let relay = f.take(|b| matches!(b, LogicalEffectBody::Relay(RelayEffect::Start { .. })));
        let mut script = Seed::new(seed);
        let mut port = Self {
            decode_wait: script.delay(),
            decode_finished: false,
            domain_complete: false,
            payload: Some(Arc::new(payload)),
            receipt: InternalEndReceiptId(std::num::NonZeroU64::MIN),
            end: end(1, 1),
            proposal: None,
            relay: relay.action,
            history: InternalHistory::default(),
        };
        port.history.0.push(InternalEvent::Frozen {
            root: root(1),
            plan: PLAN,
            value: port.payload.as_ref().unwrap().expected,
        });
        let registered = f.internal(InternalRequest::RegisterSource {
            root: root(1),
            plan: PLAN,
            supervised_exit: true,
        });
        port.reply(f, registered, |r| {
            matches!(r, InternalResponse::SourceRegistered)
        });
        port.history.0.push(InternalEvent::Registered);
        let opened = f.relay(RelayRequest::OpenDelivery {
            root: root(1),
            carrier: scalar_carrier(),
        });
        port.relay_reply(f, opened);
        port
    }
    fn reply(
        &mut self,
        f: &mut Fixture,
        token: LogicalRequestToken,
        predicate: impl Fn(&InternalResponse) -> bool,
    ) {
        let effect = f.take(|b| matches!(b, LogicalEffectBody::Consumer(ConsumerEffect::InternalReply { token: actual, .. }) if *actual == token));
        let LogicalEffectBody::Consumer(ConsumerEffect::InternalReply {
            activation: actual,
            response,
            ..
        }) = effect.body
        else {
            unreachable!()
        };
        assert_eq!(actual, activation(1));
        assert!(predicate(&response), "internal response: {response:?}");
        if matches!(response, InternalResponse::Rejected(_)) {
            self.history.0.push(InternalEvent::Rejected(token));
        }
        f.ack(effect.action, ActionResult::Consumer(ConsumerFact::Applied));
    }
    fn relay_reply(&mut self, f: &mut Fixture, token: LogicalRequestToken) {
        let effect = f.take(|b| matches!(b, LogicalEffectBody::Relay(RelayEffect::Reply { token: actual, .. }) if *actual == token));
        let LogicalEffectBody::Relay(RelayEffect::Reply { response, .. }) = effect.body else {
            unreachable!()
        };
        assert!(matches!(
            response,
            RelayResponse::DeliveryGranted | RelayResponse::Applied
        ));
        f.ack(effect.action, ActionResult::Relay(RelayFact::Applied));
    }
    pub(crate) fn advance_domain(&mut self, f: &mut Fixture) -> Option<bool> {
        assert!(
            !self.decode_finished,
            "original domain response settles once"
        );
        if self.decode_wait != 0 {
            self.decode_wait -= 1;
            self.history.0.push(InternalEvent::WaitingForDomain);
            return None;
        }
        self.decode_finished = true;
        let Some((value, rows)) = self.payload.as_ref().unwrap().decoded() else {
            self.history.0.push(InternalEvent::DomainRejected);
            let token = f.internal(InternalRequest::Failed(FailureReport {
                error: novarocks_logical_execution_protocol::failure::query_failure(
                    QueryFailureReason::ResultProtocol,
                    "invalid ScalarValueV1 model record",
                ),
                original_attempt_failure: None,
                pre_ready_replacement: None,
            }));
            self.reply(f, token, |r| matches!(r, InternalResponse::Applied));
            return Some(false);
        };
        self.domain_complete = true;
        self.history.0.push(InternalEvent::Decoded { value, rows });
        let end_reply = f.relay(RelayRequest::EndConsumed {
            root: root(1),
            end: self.end,
            accepted_packets: 1,
            delivered_through: Some(0),
        });
        self.relay_reply(f, end_reply);
        let decoded = f.internal(InternalRequest::EndDecoded {
            receipt: self.receipt,
            end: self.end,
        });
        self.reply(f, decoded, |r| matches!(r, InternalResponse::Applied));
        self.history.0.push(InternalEvent::ProvisionalEnd {
            receipt: self.receipt,
            end: self.end,
        });
        Some(true)
    }
    pub(crate) fn decode_complete_domain(&mut self, f: &mut Fixture) -> bool {
        for _ in 0..4 {
            if let Some(result) = self.advance_domain(f) {
                return result;
            }
        }
        panic!("finite seeded external domain response did not settle")
    }
    pub(crate) fn original_end(&self) -> (InternalEndReceiptId, RootResultEnd) {
        (self.receipt, self.end)
    }
    pub(crate) fn propose(&mut self, f: &mut Fixture) -> LogicalRequestToken {
        self.propose_exact(f, PLAN, self.receipt, self.end)
    }
    pub(crate) fn propose_exact(
        &mut self,
        f: &mut Fixture,
        plan: LogicalPlanIdentity,
        receipt: InternalEndReceiptId,
        end: RootResultEnd,
    ) -> LogicalRequestToken {
        assert!(
            self.domain_complete,
            "named-domain completeness precedes a Ready proposal"
        );
        let token = f.internal(InternalRequest::AcceptReady {
            root: root(1),
            plan,
            receipt,
            end,
        });
        self.history.0.push(InternalEvent::Proposed {
            token,
            root: root(1),
            plan,
            receipt,
            end,
        });
        self.proposal = Some(token);
        token
    }
    pub(crate) fn reject(&mut self, f: &mut Fixture, token: LogicalRequestToken) {
        self.reply(f, token, |r| matches!(r, InternalResponse::Rejected(_)));
    }
    pub(crate) fn observe_success_seal(&mut self, token: AttemptRequestToken) {
        self.history.0.push(InternalEvent::SealRequested(token));
        self.history.0.push(InternalEvent::SealGranted(token));
    }
    pub(crate) fn accept_ready(&mut self, f: &mut Fixture) -> AcceptedScalar {
        let effect = f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Consumer(ConsumerEffect::PublishReady { .. })
            )
        });
        let LogicalEffectBody::Consumer(ConsumerEffect::PublishReady { token, cut, .. }) =
            effect.body
        else {
            unreachable!()
        };
        assert_eq!(Some(token), self.proposal);
        self.history.0.push(InternalEvent::Ready { token, cut });
        f.ack(
            effect.action,
            ActionResult::Consumer(ConsumerFact::ReadyHandedOff),
        );
        // ACK-handoff remains separate from protocol terminal or actual exit.
        f.ack(
            self.relay,
            ActionResult::Relay(RelayFact::EndRelayed {
                activation: activation(1),
                root: root(1),
                end: self.end,
            }),
        );
        AcceptedScalar {
            cut,
            payload: self.payload.take().unwrap(),
        }
    }
    pub(crate) fn close_source(&mut self, f: &mut Fixture) {
        self.history.0.push(InternalEvent::SourceClosed);
        let token = f.internal(InternalRequest::SourceClosed);
        self.reply(f, token, |r| matches!(r, InternalResponse::Applied));
    }
    pub(crate) fn report_exit(
        &mut self,
        f: &mut Fixture,
        outcome: Option<Result<(), QueryExecutionError>>,
    ) {
        self.history.0.push(InternalEvent::ExitVerdict {
            failed: !matches!(outcome, Some(Ok(()))),
        });
        let token = f.internal(InternalRequest::ExitObserved { outcome });
        self.reply(f, token, |r| matches!(r, InternalResponse::Applied));
    }
    pub(crate) fn history(&self) -> &InternalHistory {
        &self.history
    }
}
