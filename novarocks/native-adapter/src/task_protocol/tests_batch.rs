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

//! Transport oracles moved from the retired Registry bridge. The scripted
//! author validates every call, identity, absolute deadline and explicit reply;
//! it models no Worker admission, Task state or physical resource retirement.

use std::sync::Arc;

use super::{
    TaskIngressTiming, TaskOperationBatchApplier, TaskOperationReceiptAck as Ack, TransportBudget,
    apply_task_control_operations_at, apply_task_operations_at,
};
use novarocks_execution_contract::operation::OperationResult;
use novarocks_execution_contract::{
    AdmissionEpochCapability, AdmissionTicketId, LeaseReceipt, LeaseSequence, LeaseValidFor,
    OperationOutcome, ParticipationReceipt, ParticipationRef, ParticipationState,
    QuiesceParticipationReceipt, ReleaseOutcome, SafeDetail, TaskIdentity, TaskOperationId,
};
use novarocks_proto_models::{catalog, novarocks as proto};
use novarocks_task_codec::identity::{
    encode_participation_ref, encode_task_identity, encode_task_operation_id,
};
use novarocks_task_codec::operation::{
    DecodedOperation, DecodedUpdateParticipation, encode_participation_ack, encode_quiesce_ack,
    encode_receipt, encode_release_ack,
};
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, NativeCompatibilityId, QueryExecutionId,
    QueryId, StageId, TaskId,
};
use novarocks_worker::NodeAdmissionTicketObservation;
use std::collections::VecDeque;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Input {
    Acquire(ParticipationRef, NativeCompatibilityId),
    Establish(ParticipationRef, AdmissionTicketId, NativeCompatibilityId),
    Renew(ParticipationRef),
    Quiesce(ParticipationRef),
    Release(ParticipationRef),
    Cancel(TaskIdentity),
}
impl Input {
    fn of(operation: &DecodedOperation) -> Self {
        match operation {
            DecodedOperation::AcquireParticipationAdmissionTicket(r) => {
                Self::Acquire(r.participation(), r.native_compatibility_id())
            }
            DecodedOperation::UpdateParticipation(DecodedUpdateParticipation::Establish(r)) => {
                Self::Establish(
                    r.participation(),
                    r.admission_ticket_id(),
                    r.native_compatibility_id(),
                )
            }
            DecodedOperation::UpdateParticipation(DecodedUpdateParticipation::RenewLease {
                participation,
                ..
            }) => Self::Renew(*participation),
            DecodedOperation::QuiesceParticipation(r) => Self::Quiesce(r.participation()),
            DecodedOperation::ReleaseParticipation(r) => Self::Release(r.participation()),
            DecodedOperation::CancelTask(r) => Self::Cancel(r.identity()),
            other => panic!("unexpected author input: {other:?}"),
        }
    }
}
#[derive(Debug)]
enum Call {
    Apply {
        operation: TaskOperationId,
        input: Input,
        deadline: Instant,
        reply: Box<proto::TaskOperationReceipt>,
    },
    Observe {
        operation: TaskOperationId,
        ticket: AdmissionTicketId,
        participation: ParticipationRef,
        deadline: Instant,
        answer: NodeAdmissionTicketObservation,
    },
    Preflight {
        operation: TaskOperationId,
        ticket: AdmissionTicketId,
        participation: ParticipationRef,
        compatibility: NativeCompatibilityId,
        deadline: Instant,
        reply: Option<Box<proto::TaskOperationReceipt>>,
    },
}
struct RecordingAuthor {
    calls: Mutex<VecDeque<Call>>,
    active_apply: AtomicBool,
}
impl RecordingAuthor {
    fn new(calls: Vec<Call>) -> Self {
        Self {
            calls: Mutex::new(calls.into()),
            active_apply: AtomicBool::new(false),
        }
    }
    fn next(&self) -> Call {
        self.calls
            .lock()
            .expect("script lock")
            .pop_front()
            .expect("an unplanned owner call is forbidden")
    }
    fn assert_drained(&self) {
        assert!(
            self.calls.lock().expect("script lock").is_empty(),
            "every required owner call must occur"
        );
        assert!(!self.active_apply.load(Ordering::SeqCst));
    }
}
#[tonic::async_trait]
impl TaskOperationBatchApplier for RecordingAuthor {
    fn validate_ingress_ownership(
        &self,
        _ownership: Option<&Arc<crate::native_ingress::NativeIngressOwnership>>,
    ) -> Result<(), tonic::Status> {
        Ok(())
    }

    async fn apply_task_operation(
        &self,
        operation: DecodedOperation,
        deadline: Instant,
        _ownership: Option<Arc<crate::native_ingress::NativeIngressOwnership>>,
    ) -> Result<proto::TaskOperationReceipt, tonic::Status> {
        assert!(
            !self.active_apply.swap(true, Ordering::SeqCst),
            "the previous owner future must exit before the next apply starts"
        );
        let Call::Apply {
            operation: expected,
            input,
            deadline: expected_deadline,
            reply,
        } = self.next()
        else {
            panic!("unexpected apply")
        };
        assert_eq!(operation.envelope().operation_id(), expected);
        assert_eq!(Input::of(&operation), input);
        assert_eq!(
            deadline, expected_deadline,
            "the adapter must preserve the original absolute wait"
        );
        // A real Pending boundary detects a batch that starts later effects
        // before its preceding original future has returned.
        tokio::task::yield_now().await;
        self.active_apply.store(false, Ordering::SeqCst);
        Ok(*reply)
    }
    async fn observe_admission_ticket(
        &self,
        operation: TaskOperationId,
        ticket: AdmissionTicketId,
        participation: ParticipationRef,
        deadline: Instant,
    ) -> Result<NodeAdmissionTicketObservation, tonic::Status> {
        let Call::Observe {
            operation: op,
            ticket: t,
            participation: c,
            deadline: d,
            answer,
        } = self.next()
        else {
            panic!("unexpected observation")
        };
        assert_eq!((operation, ticket, participation, deadline), (op, t, c, d));
        Ok(answer)
    }
    async fn preflight_expired_establish_ticket(
        &self,
        operation: TaskOperationId,
        ticket: AdmissionTicketId,
        participation: ParticipationRef,
        compatibility: Option<&proto::NativeCompatibilityId>,
        deadline: Instant,
    ) -> Result<Option<proto::TaskOperationReceipt>, tonic::Status> {
        let Call::Preflight {
            operation: op,
            ticket: t,
            participation: c,
            compatibility: expected,
            deadline: d,
            reply,
        } = self.next()
        else {
            panic!("unexpected preflight")
        };
        assert_eq!((operation, ticket, participation, deadline), (op, t, c, d));
        assert_eq!(
            compatibility
                .expect("original compatibility")
                .value
                .as_slice(),
            expected.as_bytes()
        );
        Ok(reply.map(|reply| *reply))
    }
}
fn participation() -> ParticipationRef {
    ParticipationRef::new(
        QueryExecutionId::new(QueryId::new(31, 32), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    )
}
fn compatibility() -> NativeCompatibilityId {
    NativeCompatibilityId::new([0x44; 32])
}
fn epoch() -> AdmissionEpochCapability {
    AdmissionEpochCapability::try_from_bytes([0x11; 16]).unwrap()
}
fn ticket(byte: u8) -> AdmissionTicketId {
    AdmissionTicketId::try_from_bytes([byte; 16]).unwrap()
}
fn timing() -> TaskIngressTiming {
    let now = Instant::now();
    TaskIngressTiming::new(now, now + Duration::from_secs(60))
}
fn limit(t: TaskIngressTiming) -> Instant {
    t.operation_deadline(Duration::from_secs(5))
}
fn reply(
    operation: TaskOperationId,
    outcome: OperationOutcome,
    acknowledgement: Option<Ack>,
) -> proto::TaskOperationReceipt {
    encode_receipt(
        operation,
        OperationResult::Verdict {
            outcome,
            detail: (acknowledgement.is_none())
                .then(|| SafeDetail::truncating("the scripted author refused this exact item")),
            acknowledgement,
        },
    )
}
fn active_ack(c: ParticipationRef) -> Ack {
    let lease = LeaseReceipt::new(
        LeaseSequence::INITIAL,
        LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
        Duration::from_secs(30),
    );
    Ack::Participation(
        encode_participation_ack(
            &ParticipationReceipt::new(c, ParticipationState::Active).with_lease(lease),
            None,
        )
        .unwrap(),
    )
}
fn observe(
    op: TaskOperationId,
    t: AdmissionTicketId,
    c: ParticipationRef,
    d: Instant,
    answer: NodeAdmissionTicketObservation,
) -> Call {
    Call::Observe {
        operation: op,
        ticket: t,
        participation: c,
        deadline: d,
        answer,
    }
}
fn apply(op: TaskOperationId, input: Input, d: Instant, r: proto::TaskOperationReceipt) -> Call {
    Call::Apply {
        operation: op,
        input,
        deadline: d,
        reply: Box::new(r),
    }
}
fn preflight(
    op: TaskOperationId,
    t: AdmissionTicketId,
    c: ParticipationRef,
    compat: NativeCompatibilityId,
    d: Instant,
    r: Option<proto::TaskOperationReceipt>,
) -> Call {
    Call::Preflight {
        operation: op,
        ticket: t,
        participation: c,
        compatibility: compat,
        deadline: d,
        reply: r.map(Box::new),
    }
}
fn ordinary(operations: Vec<proto::TaskOperation>) -> proto::ApplyTaskOperationsRequest {
    proto::ApplyTaskOperationsRequest { operations }
}
fn control(operations: Vec<proto::TaskOperation>) -> proto::ApplyTaskControlOperationsRequest {
    proto::ApplyTaskControlOperationsRequest {
        operations: operations.into_iter().map(to_control_operation).collect(),
    }
}
fn outcome(r: &proto::TaskOperationReceipt) -> proto::TaskOperationOutcome {
    let proto::task_operation_receipt::Result::Verdict(v) =
        r.result.as_ref().expect("typed result")
    else {
        panic!("expected verdict")
    };
    proto::TaskOperationOutcome::try_from(v.outcome).unwrap()
}
fn malformed_lease(operation: &mut proto::TaskOperation) {
    let Some(proto::task_operation::Operation::UpdateParticipation(update)) =
        operation.operation.as_mut()
    else {
        panic!("Establish")
    };
    let Some(proto::update_participation_request::Command::Establish(establish)) =
        update.command.as_mut()
    else {
        panic!("Establish")
    };
    establish.initial_lease.as_mut().unwrap().valid_for_millis = 0;
}
fn to_control_operation(operation: proto::TaskOperation) -> proto::TaskControlOperation {
    let control = match operation.operation.expect("a control command") {
        proto::task_operation::Operation::UpdateParticipation(update) => {
            let Some(proto::update_participation_request::Command::RenewLease(renew)) =
                update.command
            else {
                panic!("only renewal uses control UpdateParticipation");
            };
            proto::task_control_operation::Control::RenewLease(renew)
        }
        proto::task_operation::Operation::CancelTask(cancel) => {
            proto::task_control_operation::Control::CancelTask(cancel)
        }
        proto::task_operation::Operation::AbortParticipation(abort) => {
            proto::task_control_operation::Control::AbortParticipation(abort)
        }
        proto::task_operation::Operation::QuiesceParticipation(quiesce) => {
            proto::task_control_operation::Control::QuiesceParticipation(quiesce)
        }
        proto::task_operation::Operation::ReleaseParticipation(release) => {
            proto::task_control_operation::Control::ReleaseParticipation(release)
        }
        _ => panic!("ordinary operation cannot be sent to control method"),
    };
    proto::TaskControlOperation {
        envelope: operation.envelope,
        control: Some(control),
    }
}

fn envelope(operation: TaskOperationId) -> proto::TaskOperationEnvelope {
    proto::TaskOperationEnvelope {
        operation_id: Some(encode_task_operation_id(operation)),
        max_wait_millis: 5_000,
    }
}

fn acquire_ticket(
    participation: ParticipationRef,
    operation: TaskOperationId,
    native_compatibility_id: NativeCompatibilityId,
    admission_epoch_capability: novarocks_execution_contract::AdmissionEpochCapability,
) -> proto::TaskOperation {
    proto::TaskOperation {
        envelope: Some(envelope(operation)),
        operation: Some(
            proto::task_operation::Operation::AcquireParticipationAdmissionTicket(
                proto::AcquireParticipationAdmissionTicketRequest {
                    participation: Some(encode_participation_ref(participation)),
                    valid_for_millis: 10_000,
                    native_compatibility_id: Some(proto::NativeCompatibilityId {
                        value: native_compatibility_id.as_bytes().to_vec(),
                    }),
                    admission_epoch_capability: Some(proto::AdmissionEpochCapability {
                        value: admission_epoch_capability.to_bytes().to_vec(),
                    }),
                },
            ),
        ),
    }
}

fn establish_with_compatibility(
    participation: ParticipationRef,
    operation: TaskOperationId,
    admission_ticket_id: AdmissionTicketId,
    native_compatibility_id: NativeCompatibilityId,
) -> proto::TaskOperation {
    proto::TaskOperation {
        envelope: Some(envelope(operation)),
        operation: Some(proto::task_operation::Operation::UpdateParticipation(
            proto::UpdateParticipationRequest {
                command: Some(proto::update_participation_request::Command::Establish(
                    proto::EstablishParticipationRequest {
                        participation: Some(encode_participation_ref(participation)),
                        catalog_set: Some(catalog::CatalogSet::default()),
                        initial_runtime_filter: Some(proto::RuntimeFilterContribution::default()),
                        initial_credential: Some(proto::ParticipationCredentialDomain {
                            credential_domain_id: 1,
                            epoch: 1,
                            descriptors: Vec::new(),
                        }),
                        initial_lease: Some(proto::ParticipationLeaseGrant {
                            sequence: 0,
                            valid_for_millis: 30_000,
                        }),
                        query_options: Some(query_options()),
                        native_compatibility_id: Some(proto::NativeCompatibilityId {
                            value: native_compatibility_id.as_bytes().to_vec(),
                        }),
                        admission_ticket_id: Some(proto::AdmissionTicketId {
                            value: admission_ticket_id.to_bytes().to_vec(),
                        }),
                    },
                )),
            },
        )),
    }
}

fn renew_lease(
    participation: ParticipationRef,
    operation: TaskOperationId,
    sequence: u64,
) -> proto::TaskOperation {
    proto::TaskOperation {
        envelope: Some(envelope(operation)),
        operation: Some(proto::task_operation::Operation::UpdateParticipation(
            proto::UpdateParticipationRequest {
                command: Some(proto::update_participation_request::Command::RenewLease(
                    proto::RenewParticipationLeaseRequest {
                        participation: Some(encode_participation_ref(participation)),
                        lease: Some(proto::ParticipationLeaseGrant {
                            sequence,
                            valid_for_millis: 5_000,
                        }),
                    },
                )),
            },
        )),
    }
}

fn release(participation: ParticipationRef, operation: TaskOperationId) -> proto::TaskOperation {
    proto::TaskOperation {
        envelope: Some(envelope(operation)),
        operation: Some(proto::task_operation::Operation::ReleaseParticipation(
            proto::ReleaseParticipationRequest {
                participation: Some(encode_participation_ref(participation)),
            },
        )),
    }
}

fn quiesce(participation: ParticipationRef, operation: TaskOperationId) -> proto::TaskOperation {
    proto::TaskOperation {
        envelope: Some(envelope(operation)),
        operation: Some(proto::task_operation::Operation::QuiesceParticipation(
            proto::QuiesceParticipationRequest {
                participation: Some(encode_participation_ref(participation)),
            },
        )),
    }
}

fn cancel(identity: TaskIdentity, operation: TaskOperationId) -> proto::TaskOperation {
    proto::TaskOperation {
        envelope: Some(envelope(operation)),
        operation: Some(proto::task_operation::Operation::CancelTask(
            proto::CancelTaskRequest {
                identity: Some(encode_task_identity(identity)),
                reason: proto::TaskCancelReason::UpstreamNoLongerNeeded as i32,
            },
        )),
    }
}

fn query_options() -> proto::QueryOptions {
    proto::QueryOptions {
        pipeline_dop: 2,
        ..Default::default()
    }
}

#[tokio::test]
async fn ordinary_then_control_methods_preserve_receipt_order() {
    let c = participation();
    let t = ticket(1);
    let ids = [
        TaskOperationId::new_v7(),
        TaskOperationId::new_v7(),
        TaskOperationId::new_v7(),
        TaskOperationId::new_v7(),
    ];
    let clock = timing();
    let d = limit(clock);
    let expected = vec![
        reply(ids[0], OperationOutcome::Accepted, Some(active_ack(c))),
        reply(ids[1], OperationOutcome::Accepted, Some(active_ack(c))),
        reply(
            ids[2],
            OperationOutcome::Accepted,
            Some(Ack::QuiesceParticipation(
                encode_quiesce_ack(&QuiesceParticipationReceipt::new(
                    c,
                    1,
                    Vec::new(),
                    ParticipationState::Quiescing,
                ))
                .unwrap(),
            )),
        ),
        reply(
            ids[3],
            OperationOutcome::Accepted,
            Some(Ack::ReleaseParticipation(
                encode_release_ack(
                    c,
                    ReleaseOutcome::Released,
                    ParticipationState::TerminalRetained,
                    None,
                )
                .unwrap(),
            )),
        ),
    ];
    let author = RecordingAuthor::new(vec![
        observe(
            ids[0],
            t,
            c,
            d,
            NodeAdmissionTicketObservation::Issued {
                remaining: Duration::from_secs(60),
            },
        ),
        observe(
            ids[0],
            t,
            c,
            d,
            NodeAdmissionTicketObservation::Issued {
                remaining: Duration::from_secs(60),
            },
        ),
        apply(
            ids[0],
            Input::Establish(c, t, compatibility()),
            d,
            expected[0].clone(),
        ),
        apply(ids[1], Input::Renew(c), d, expected[1].clone()),
        apply(ids[2], Input::Quiesce(c), d, expected[2].clone()),
        apply(ids[3], Input::Release(c), d, expected[3].clone()),
    ]);
    let ordinary = apply_task_operations_at(
        &author,
        ordinary(vec![establish_with_compatibility(
            c,
            ids[0],
            t,
            compatibility(),
        )]),
        clock,
    )
    .await
    .unwrap();
    let control = apply_task_control_operations_at(
        &author,
        control(vec![
            renew_lease(c, ids[1], 1),
            quiesce(c, ids[2]),
            release(c, ids[3]),
        ]),
        clock,
    )
    .await
    .unwrap();
    assert_eq!([ordinary.receipts, control.receipts].concat(), expected);
    author.assert_drained();
}

#[tokio::test]
async fn one_refused_control_item_leaves_renewal_receipt_untouched() {
    let c = participation();
    let a = TaskOperationId::new_v7();
    let b = TaskOperationId::new_v7();
    let clock = timing();
    let d = limit(clock);
    let task = TaskIdentity::new(
        c.query_execution_id(),
        StageId::new(9).unwrap(),
        TaskId::new(9).unwrap(),
        c.backend_process_id(),
    );
    let refusal = reply(a, OperationOutcome::InvalidStateOrRequest, None);
    let renewal = reply(b, OperationOutcome::Accepted, Some(active_ack(c)));
    let author = RecordingAuthor::new(vec![
        apply(a, Input::Cancel(task), d, refusal.clone()),
        apply(b, Input::Renew(c), d, renewal.clone()),
    ]);
    let result = apply_task_control_operations_at(
        &author,
        control(vec![cancel(task, a), renew_lease(c, b, 1)]),
        clock,
    )
    .await
    .unwrap();
    assert_eq!(result.receipts, vec![refusal, renewal]);
    author.assert_drained();
}

#[tokio::test]
async fn operation_waits_are_measured_from_tower_arrival_per_item() {
    let c = participation();
    let a = TaskOperationId::new_v7();
    let b = TaskOperationId::new_v7();
    let now = Instant::now();
    let clock = TaskIngressTiming::new(now - Duration::from_secs(2), now + Duration::from_secs(20));
    let mut first = acquire_ticket(c, a, compatibility(), epoch());
    first.envelope.as_mut().unwrap().max_wait_millis = 1000;
    let mut second = acquire_ticket(c, b, compatibility(), epoch());
    second.envelope.as_mut().unwrap().max_wait_millis = 30000;
    let grant = Ack::ParticipationAdmissionTicket(
        novarocks_task_codec::operation::encode_participation_admission_ticket_ack(
            novarocks_execution_contract::ParticipationAdmissionTicketReceipt::new(
                ticket(8),
                c,
                LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
            ),
        ),
    );
    let accepted = reply(b, OperationOutcome::Accepted, Some(grant));
    // An attempted first apply or a fresh second-item duration is a failure.
    let author = RecordingAuthor::new(vec![apply(
        b,
        Input::Acquire(c, compatibility()),
        now + Duration::from_secs(20),
        accepted.clone(),
    )]);
    let result = apply_task_operations_at(&author, ordinary(vec![first, second]), clock)
        .await
        .unwrap();
    assert_eq!(result.receipts.len(), 2);
    assert_eq!(
        outcome(&result.receipts[0]),
        proto::TaskOperationOutcome::OperationTimedOut
    );
    assert_eq!(
        result.receipts[0].operation_id,
        Some(encode_task_operation_id(a))
    );
    assert_eq!(result.receipts[1], accepted);
    author.assert_drained();
}

#[tokio::test]
async fn single_expired_establish_is_refused_before_deep_content_decode() {
    let c = participation();
    let t = ticket(1);
    let op = TaskOperationId::new_v7();
    let clock = timing();
    let d = limit(clock);
    let refused = reply(op, OperationOutcome::InvalidStateOrRequest, None);
    let author = RecordingAuthor::new(vec![
        observe(op, t, c, d, NodeAdmissionTicketObservation::Expired),
        preflight(op, t, c, compatibility(), d, Some(refused.clone())),
    ]);
    let mut request = establish_with_compatibility(c, op, t, compatibility());
    malformed_lease(&mut request);
    let result = apply_task_operations_at(&author, ordinary(vec![request]), clock)
        .await
        .expect("only the original author can skip deep decode");
    assert_eq!(result.receipts, vec![refused]);
    author.assert_drained();
}

#[tokio::test]
async fn expired_establish_in_mixed_batch_keeps_whole_batch_validation() {
    let c = participation();
    let t = ticket(1);
    let op = TaskOperationId::new_v7();
    let clock = timing();
    let d = limit(clock);
    let author = RecordingAuthor::new(vec![
        observe(op, t, c, d, NodeAdmissionTicketObservation::Expired),
        preflight(
            op,
            t,
            c,
            compatibility(),
            d,
            Some(reply(op, OperationOutcome::InvalidStateOrRequest, None)),
        ),
    ]);
    let invalid = proto::TaskOperation {
        envelope: Some(envelope(TaskOperationId::new_v7())),
        operation: None,
    };
    let error = apply_task_operations_at(
        &author,
        ordinary(vec![
            establish_with_compatibility(c, op, t, compatibility()),
            invalid,
        ]),
        clock,
    )
    .await
    .expect_err("a later malformed item prevents every apply");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("requires a command"));
    author.assert_drained();
}

#[tokio::test]
async fn expired_establish_in_mixed_batch_keeps_other_item_and_receipt_order() {
    let c = participation();
    let other = participation();
    let t = ticket(1);
    let a = TaskOperationId::new_v7();
    let b = TaskOperationId::new_v7();
    let clock = timing();
    let d = limit(clock);
    let refusal = reply(a, OperationOutcome::InvalidStateOrRequest, None);
    let accepted = reply(
        b,
        OperationOutcome::Accepted,
        Some(Ack::ParticipationAdmissionTicket(
            novarocks_task_codec::operation::encode_participation_admission_ticket_ack(
                novarocks_execution_contract::ParticipationAdmissionTicketReceipt::new(
                    ticket(9),
                    other,
                    LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
                ),
            ),
        )),
    );
    let author = RecordingAuthor::new(vec![
        observe(a, t, c, d, NodeAdmissionTicketObservation::Expired),
        preflight(a, t, c, compatibility(), d, Some(refusal.clone())),
        apply(
            b,
            Input::Acquire(other, compatibility()),
            d,
            accepted.clone(),
        ),
    ]);
    let result = apply_task_operations_at(
        &author,
        ordinary(vec![
            establish_with_compatibility(c, a, t, compatibility()),
            acquire_ticket(other, b, compatibility(), epoch()),
        ]),
        clock,
    )
    .await
    .unwrap();
    assert_eq!(result.receipts, vec![refusal, accepted]);
    author.assert_drained();
}

#[tokio::test]
async fn later_expired_establish_waits_for_earlier_participation_effect() {
    let c = participation();
    let live = ticket(1);
    let expired = ticket(2);
    let a = TaskOperationId::new_v7();
    let b = TaskOperationId::new_v7();
    let clock = timing();
    let d = limit(clock);
    let accepted = reply(a, OperationOutcome::Accepted, Some(active_ack(c)));
    let conflict = reply(b, OperationOutcome::ParticipationConflict, None);
    let author = RecordingAuthor::new(vec![
        observe(
            a,
            live,
            c,
            d,
            NodeAdmissionTicketObservation::Issued {
                remaining: Duration::from_secs(60),
            },
        ),
        observe(b, expired, c, d, NodeAdmissionTicketObservation::Expired),
        observe(
            a,
            live,
            c,
            d,
            NodeAdmissionTicketObservation::Issued {
                remaining: Duration::from_secs(60),
            },
        ),
        apply(
            a,
            Input::Establish(c, live, compatibility()),
            d,
            accepted.clone(),
        ),
        apply(
            b,
            Input::Establish(c, expired, compatibility()),
            d,
            conflict.clone(),
        ),
    ]);
    let result = apply_task_operations_at(
        &author,
        ordinary(vec![
            establish_with_compatibility(c, a, live, compatibility()),
            establish_with_compatibility(c, b, expired, compatibility()),
        ]),
        clock,
    )
    .await
    .unwrap();
    assert_eq!(result.receipts, vec![accepted, conflict]);
    author.assert_drained();
}

#[tokio::test]
async fn expired_ticket_cannot_override_compatibility_or_operation_deadline() {
    let c = participation();
    let t = ticket(1);
    let op = TaskOperationId::new_v7();
    let foreign = NativeCompatibilityId::new([0x99; 32]);
    let clock = timing();
    let d = limit(clock);
    let mismatch = reply(op, OperationOutcome::CompatibilityMismatch, None);
    // The author explicitly declines an early verdict for this foreign
    // compatibility. Transport must preserve the later original refusal.
    let author = RecordingAuthor::new(vec![
        observe(op, t, c, d, NodeAdmissionTicketObservation::Expired),
        preflight(op, t, c, foreign, d, None),
        apply(op, Input::Establish(c, t, foreign), d, mismatch.clone()),
    ]);
    let result = apply_task_operations_at(
        &author,
        ordinary(vec![establish_with_compatibility(c, op, t, foreign)]),
        clock,
    )
    .await
    .unwrap();
    assert_eq!(result.receipts, vec![mismatch]);
    author.assert_drained();
    let now = Instant::now();
    let expired_clock =
        TaskIngressTiming::new(now - Duration::from_secs(6), now + Duration::from_secs(20));
    let author = RecordingAuthor::new(vec![observe(
        op,
        t,
        c,
        limit(expired_clock),
        NodeAdmissionTicketObservation::Expired,
    )]);
    let result = apply_task_operations_at(
        &author,
        ordinary(vec![establish_with_compatibility(
            c,
            op,
            t,
            compatibility(),
        )]),
        expired_clock,
    )
    .await
    .unwrap();
    assert_eq!(
        outcome(&result.receipts[0]),
        proto::TaskOperationOutcome::OperationTimedOut
    );
    assert_eq!(
        result.receipts[0].operation_id,
        Some(encode_task_operation_id(op))
    );
    author.assert_drained();
}

#[tokio::test]
async fn malformed_establish_wait_is_rejected_before_ticket_observation() {
    for invalid_wait in [0, 300001] {
        let c = participation();
        let mut request =
            establish_with_compatibility(c, TaskOperationId::new_v7(), ticket(1), compatibility());
        request.envelope.as_mut().unwrap().max_wait_millis = invalid_wait;
        // Empty script is the original NoTicketAccess oracle: every call to
        // observe, preflight or apply panics instead of fabricating a reply.
        let author = RecordingAuthor::new(Vec::new());
        let error = apply_task_operations_at(&author, ordinary(vec![request]), timing())
            .await
            .expect_err("invalid wait must not reach the author");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        author.assert_drained();
    }
}

#[tokio::test]
async fn an_oversized_batch_is_refused_before_any_item_is_walked() {
    let author = RecordingAuthor::new(Vec::new());
    let operations = (0..TransportBudget::DEFAULT.max_batch_items() + 1)
        .map(|_| proto::TaskOperation {
            envelope: Some(envelope(TaskOperationId::new_v7())),
            operation: None,
        })
        .collect();
    let error = apply_task_operations_at(&author, ordinary(operations), timing())
        .await
        .expect_err("item count precedes decoding or author access");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("item bound"));
    assert!(!error.message().contains("requires a command"));
    author.assert_drained();
}
