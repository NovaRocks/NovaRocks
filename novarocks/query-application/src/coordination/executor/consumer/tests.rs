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
use novarocks_physical_plan::PlanVersionId;
use novarocks_result_contract::RootOutputKind;
use novarocks_types::{AttemptId, BackendProcessId, StageId, TaskId};
use std::{future::Future, pin::Pin, task::Poll, time::Duration};
use tokio::sync::mpsc;

fn activation(n: u64) -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(
        17,
        QueryExecutionId::new(QueryId::new(19, 23), AttemptId::new(n).unwrap()).unwrap(),
        n + 1,
    )
}
fn root(a: AttemptActivationIdentity) -> TaskIdentity {
    TaskIdentity::new(
        a.execution(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    )
}
fn end() -> RootResultEnd {
    RootResultEnd {
        sequence: NonZeroU64::MIN,
        output_rows: 0,
    }
}
fn plan() -> PlanSeal {
    PlanSeal::Version(PlanVersionId::try_new([37; 16]).unwrap())
}
fn carrier() -> RootCarrier {
    RootCarrier {
        kind: RootOutputKind::CountOnly,
        client_rows: None,
    }
}
fn client() -> ConsumerExecutor {
    ConsumerExecutor::new_client(
        QueryId::new(19, 23),
        ResultSchema::new(Vec::<ResultField>::new()),
        carrier(),
        plan(),
        NonZeroUsize::MIN,
        None,
    )
    .unwrap()
}
async fn poll_once<F: Future + ?Sized>(mut f: Pin<&mut F>) -> Poll<F::Output> {
    std::future::poll_fn(|cx| Poll::Ready(f.as_mut().poll(cx))).await
}
async fn exposed(owner: &ConsumerExecutor) -> QueryResultStream {
    assert!(matches!(
        owner
            .execute(ConsumerEffect::ExposeOutput {
                activation: activation(1)
            })
            .await,
        ConsumerFact::OutputExposed
    ));
    owner.take_exposed_output().unwrap()
}
struct Source(mpsc::UnboundedSender<LogicalHostFact>);
impl LogicalFactSource for Source {
    fn id(&self) -> LogicalSourceId {
        LogicalSourceId::new(NonZeroU64::MIN)
    }
    fn send(
        &mut self,
        fact: LogicalHostFact,
    ) -> LogicalFuture<'_, Result<(), LogicalSourceClosed>> {
        Box::pin(async move { self.0.send(fact).map_err(|_| LogicalSourceClosed) })
    }
}
#[tokio::test]
async fn schema_receipt_is_original_and_survives_attempt_cleanup() {
    let owner = client();
    let mut output = exposed(&owner).await;
    let mut write = owner.execute(ConsumerEffect::WriteSchema {
        activation: activation(1),
    });
    assert!(poll_once(write.as_mut()).await.is_pending());
    owner.retire_pending_end(activation(1));
    output.begin_schema().unwrap().complete();
    assert!(matches!(write.await, ConsumerFact::SchemaWritten));
    assert!(owner.take_exposed_output().is_none());
}
#[tokio::test]
async fn successful_end_waits_for_the_original_transport_receipt() {
    let owner = client();
    let a = activation(1);
    let mut output = exposed(&owner).await;
    output.begin_schema().unwrap().complete();
    owner.record_end(a, end()).unwrap();
    assert!(matches!(
        owner
            .execute(ConsumerEffect::ReserveEndSlot { activation: a })
            .await,
        ConsumerFact::EndSlotReady
    ));
    let mut write = owner.execute(ConsumerEffect::EnqueueEnd { activation: a });
    assert!(poll_once(write.as_mut()).await.is_pending());
    let ResultDelivery::End(original) = output.next().await.unwrap().unwrap() else {
        panic!("original EOF expected");
    };
    assert_eq!(original.execution_id(), a.execution());
    assert_eq!(original.root_output_rows(), Some(0));
    assert!(poll_once(write.as_mut()).await.is_pending());
    original.complete();
    assert!(matches!(write.await, ConsumerFact::EndSettled));
}
#[tokio::test]
async fn committed_end_retains_original_transport_failure() {
    let owner = client();
    let a = activation(1);
    let mut output = exposed(&owner).await;
    output.begin_schema().unwrap().complete();
    owner.record_end(a, end()).unwrap();
    owner
        .execute(ConsumerEffect::ReserveEndSlot { activation: a })
        .await;
    let mut write = owner.execute(ConsumerEffect::EnqueueEnd { activation: a });
    assert!(poll_once(write.as_mut()).await.is_pending());
    let ResultDelivery::End(original) = output.next().await.unwrap().unwrap() else {
        panic!("original EOF expected");
    };
    let failure = protocol("actual client writer failed");
    original.fail(failure.clone());
    assert!(matches!(write.await, ConsumerFact::Failed(e) if e == failure));
}
#[tokio::test]
async fn end_reservation_finishing_after_stop_releases_its_position() {
    let owner = client();
    let a = activation(1);
    let held = owner.reserve_segment(a).await.unwrap();
    let mut reserve = owner.execute(ConsumerEffect::ReserveEndSlot { activation: a });
    assert!(poll_once(reserve.as_mut()).await.is_pending());
    owner.retire_pending_end(a);
    drop(held);
    assert!(matches!(reserve.await, ConsumerFact::Failed(_)));
    assert!(owner.lock().end_slots.is_empty());
    assert!(owner.reserve_segment(activation(2)).await.is_ok());
}
#[tokio::test]
async fn stopping_one_attempt_does_not_revoke_successor_end_position() {
    let owner = client();
    let first = activation(1);
    let second = activation(2);
    owner
        .execute(ConsumerEffect::ReserveEndSlot { activation: first })
        .await;
    owner.retire_pending_end(first);
    assert!(matches!(
        owner
            .execute(ConsumerEffect::ReserveEndSlot { activation: second })
            .await,
        ConsumerFact::EndSlotReady
    ));
    assert!(owner.lock().end_slots.contains_key(&key(second)));
}
#[tokio::test]
async fn missing_original_observation_owner_is_a_typed_failure() {
    let owner = client();
    let now = MonotonicInstant::from_origin(Duration::from_secs(1));
    assert!(matches!(
        owner
            .execute(ConsumerEffect::PublishDiagnostics {
                activation: activation(1),
                deadline: now
            })
            .await,
        ConsumerFact::Failed(_)
    ));
    assert!(matches!(
        owner
            .execute(ConsumerEffect::PublishConvergence {
                activation: activation(1),
                all_participations_closed: true,
                remote_tracking_ended: true,
                local_resources_exited: true
            })
            .await,
        ConsumerFact::Failed(_)
    ));
}
#[tokio::test]
async fn internal_end_crosses_real_domain_channel_before_ready_proposal() {
    exercise_original_ready_handoff(false).await;
}
#[tokio::test]
async fn supervised_failure_preserves_the_exact_original_ready_and_payload() {
    exercise_original_ready_handoff(true).await;
}
async fn exercise_original_ready_handoff(supervise: bool) {
    let owner = Arc::new(
        ConsumerExecutor::new_internal(carrier(), plan(), NonZeroUsize::MIN, None).unwrap(),
    );
    let a = activation(1);
    let r = root(a);
    let (facts, mut observed) = mpsc::unbounded_channel();
    owner.attach_source(Box::new(Source(facts))).await.unwrap();
    let original = owner.bind_internal(a, r).unwrap();
    let exit = original.supervise_exit().await.unwrap();
    let (mut stream, source) = original.into_parts();
    let mut transfer = Box::pin(owner.deliver_internal_end(a, end()));
    assert!(poll_once(transfer.as_mut()).await.is_pending());
    let Some(InternalResultDelivery::End(delivery)) = stream.next().await.unwrap() else {
        panic!("provisional End expected");
    };
    let mut decoded = Box::pin(delivery.complete());
    assert!(poll_once(decoded.as_mut()).await.is_pending());
    assert!(poll_once(transfer.as_mut()).await.is_pending());
    let LogicalHostFact::Internal {
        token,
        activation,
        request:
            InternalRequest::EndDecoded {
                receipt,
                end: observed_end,
            },
    } = observed.recv().await.unwrap()
    else {
        panic!("decoded receipt fact expected");
    };
    assert_eq!(activation, a);
    assert_eq!(observed_end, end());
    assert!(
        owner
            .lock()
            .internal
            .get(&key(a))
            .unwrap()
            .ready
            .borrow()
            .is_none()
    );
    assert!(matches!(
        owner
            .execute(ConsumerEffect::InternalReply {
                source: LogicalSourceId::new(NonZeroU64::MIN),
                token,
                activation: a,
                response: InternalResponse::Applied
            })
            .await,
        ConsumerFact::Applied
    ));
    transfer.await.unwrap();
    let decoded = decoded.await.unwrap();
    let mut accept = Box::pin(source.accept(decoded));
    assert!(poll_once(accept.as_mut()).await.is_pending());
    let LogicalHostFact::Internal {
        token,
        request:
            InternalRequest::AcceptReady {
                root,
                receipt: proposal_receipt,
                ..
            },
        ..
    } = observed.recv().await.unwrap()
    else {
        panic!("domain Ready proposal expected");
    };
    assert_eq!(root, r);
    assert_eq!(proposal_receipt, receipt);
    let now = MonotonicInstant::from_origin(Duration::from_secs(1));
    let cut = AttemptComputationCut {
        activation: a,
        root: r,
        cut: now,
        diagnostic_deadline: now,
        convergence_deadline: now,
        end: end(),
    };
    assert!(matches!(
        owner
            .execute(ConsumerEffect::PublishReady {
                source: LogicalSourceId::new(NonZeroU64::MIN),
                token,
                cut
            })
            .await,
        ConsumerFact::ReadyHandedOff
    ));
    let accepted = accept.await.unwrap();
    assert_eq!(accepted.end(), end());
    if supervise {
        assert!(matches!(
            owner
                .execute(ConsumerEffect::PublishAttemptFailure {
                    activation: a,
                    error: protocol("late business failure after original Ready"),
                })
                .await,
            ConsumerFact::Applied
        ));
        assert!(
            owner
                .lock()
                .internal
                .get(&key(a))
                .unwrap()
                .failure_issued
                .is_none()
        );
        owner.begin_supervised_failure(protocol("original owner failed after actual Ready"));
        let state = owner.lock();
        let original = state.internal.get(&key(a)).unwrap();
        assert!(original.failure.borrow().is_none());
        assert!(matches!(*original.ready.borrow(), Some(Ok(token)) if token == accepted));
    }
    assert_eq!(owner.wait_internal_ready(a).await.unwrap(), accepted);
    assert_eq!(accepted.root(), r);
    exit.complete(Ok(()));
}
#[tokio::test]
async fn decoded_end_from_a_foreign_original_seal_cannot_be_accepted() {
    let owner = Arc::new(
        ConsumerExecutor::new_internal(carrier(), plan(), NonZeroUsize::MIN, None).unwrap(),
    );
    let a = activation(1);
    let r = root(a);
    let original = owner.bind_internal(a, r).unwrap();
    let id = InternalEndReceiptId(NonZeroU64::MIN);
    owner.lock().internal.get_mut(&key(a)).unwrap().decoded = Some((id, end()));
    let foreign = Arc::new(InternalConsumerSeal {
        activation: a,
        root: r,
        plan: plan(),
    });
    let decoded = DecodedInternalEndReceipt::issued(foreign, end());
    let (_, source) = original.into_parts();
    assert!(source.accept(decoded).await.is_err());
}
#[tokio::test]
async fn logical_failure_wakes_idle_stream_without_queue_capacity() {
    let owner = client();
    let mut output = exposed(&owner).await;
    output.begin_schema().unwrap().complete();
    let held = owner.reserve_segment(activation(1)).await.unwrap();
    let error = protocol("accepted original cancellation");
    assert!(matches!(
        owner
            .execute(ConsumerEffect::PublishFailure(error.clone()))
            .await,
        ConsumerFact::Applied
    ));
    assert_eq!(output.next().await.err(), Some(error));
    drop(held);
}
#[tokio::test]
async fn schema_writer_failure_keeps_the_original_domain_input() {
    let owner = client();
    let mut output = exposed(&owner).await;
    let error = protocol("original schema domain error");
    output.begin_schema().unwrap().fail(error.clone());
    assert!(
        matches!(owner.execute(ConsumerEffect::WriteSchema { activation: activation(1) }).await, ConsumerFact::Failed(original) if original == error)
    );
}
#[tokio::test]
async fn dropped_client_before_or_after_schema_reports_real_channel_close() {
    for completed in [false, true] {
        let owner = client();
        let (facts, mut observed) = mpsc::unbounded_channel();
        owner.attach_source(Box::new(Source(facts))).await.unwrap();
        let mut output = exposed(&owner).await;
        if completed {
            output.begin_schema().unwrap().complete();
        }
        let mut observer = owner.observe_client_closed();
        assert!(poll_once(observer.as_mut()).await.is_pending());
        drop(output);
        observer.await.unwrap();
        assert!(matches!(
            observed.recv().await,
            Some(LogicalHostFact::ConsumerClosed)
        ));
    }
}
#[tokio::test]
async fn old_attempt_failure_stays_with_its_original_stream() {
    let owner = Arc::new(
        ConsumerExecutor::new_internal(carrier(), plan(), NonZeroUsize::MIN, None).unwrap(),
    );
    let first = activation(1);
    let second = activation(2);
    let original = owner.bind_internal(first, root(first)).unwrap();
    let successor = owner.bind_internal(second, root(second)).unwrap();
    let (mut old, _old_source) = original.into_parts();
    let (mut new, _new_source) = successor.into_parts();
    let old_failure = protocol("original attempt failure");
    let new_failure = protocol("replacement attempt failure");
    assert!(matches!(
        owner
            .execute(ConsumerEffect::PublishAttemptFailure {
                activation: first,
                error: old_failure.clone()
            })
            .await,
        ConsumerFact::Applied
    ));
    assert_eq!(old.failure().await, old_failure);
    assert!(new.current_failure().is_none());
    owner
        .execute(ConsumerEffect::PublishAttemptFailure {
            activation: second,
            error: new_failure.clone(),
        })
        .await;
    assert_eq!(new.failure().await, new_failure);
    assert_eq!(old.current_failure(), Some(old_failure));
}
#[tokio::test]
async fn supervised_closed_receiver_waits_for_original_exit_error() {
    let owner = Arc::new(
        ConsumerExecutor::new_internal(carrier(), plan(), NonZeroUsize::MIN, None).unwrap(),
    );
    let a = activation(1);
    let original = owner.bind_internal(a, root(a)).unwrap();
    let exit = original.supervise_exit().await.unwrap();
    let (stream, source) = original.into_parts();
    drop(stream);
    let (facts, mut observed) = mpsc::unbounded_channel();
    owner.attach_source(Box::new(Source(facts))).await.unwrap();
    let mut reserve = Box::pin(owner.reserve_segment(a));
    assert!(poll_once(reserve.as_mut()).await.is_pending());
    assert!(
        observed.try_recv().is_err(),
        "channel close must not synthesize an exit verdict"
    );
    let error = protocol("original supervised decoder error");
    exit.complete(Err(error.clone()));
    assert!(poll_once(reserve.as_mut()).await.is_pending());
    let LogicalHostFact::Internal {
        token,
        request: InternalRequest::ExitObserved {
            outcome: Some(Err(original)),
        },
        ..
    } = observed.recv().await.unwrap()
    else {
        panic!("original supervised verdict expected");
    };
    assert_eq!(original, error);
    owner
        .execute(ConsumerEffect::InternalReply {
            source: LogicalSourceId::new(NonZeroU64::MIN),
            token,
            activation: a,
            response: InternalResponse::Applied,
        })
        .await;
    assert_eq!(reserve.await.err(), Some(error));
    drop(source);
}
#[tokio::test]
async fn supervised_internal_end_drop_waits_for_original_exit_error() {
    let owner = Arc::new(
        ConsumerExecutor::new_internal(carrier(), plan(), NonZeroUsize::MIN, None).unwrap(),
    );
    let a = activation(1);
    let original = owner.bind_internal(a, root(a)).unwrap();
    let exit = original.supervise_exit().await.unwrap();
    let (mut stream, source) = original.into_parts();
    let (facts, mut observed) = mpsc::unbounded_channel();
    owner.attach_source(Box::new(Source(facts))).await.unwrap();
    let mut transfer = Box::pin(owner.deliver_internal_end(a, end()));
    assert!(poll_once(transfer.as_mut()).await.is_pending());
    let Some(InternalResultDelivery::End(delivery)) = stream.next().await.unwrap() else {
        panic!("provisional End expected");
    };
    drop(delivery);
    assert!(poll_once(transfer.as_mut()).await.is_pending());
    assert!(observed.try_recv().is_err());
    let error = protocol("original supervised End decoder error");
    exit.complete(Err(error.clone()));
    assert!(poll_once(transfer.as_mut()).await.is_pending());
    let LogicalHostFact::Internal {
        token,
        request: InternalRequest::ExitObserved {
            outcome: Some(Err(original)),
        },
        ..
    } = observed.recv().await.unwrap()
    else {
        panic!("original supervised verdict expected");
    };
    assert_eq!(original, error);
    owner
        .execute(ConsumerEffect::InternalReply {
            source: LogicalSourceId::new(NonZeroU64::MIN),
            token,
            activation: a,
            response: InternalResponse::Applied,
        })
        .await;
    assert_eq!(transfer.await.err(), Some(error));
    drop(source);
}
#[tokio::test]
async fn cancelled_known_reply_waiter_settles_without_replacing_original_failure() {
    let owner = client();
    let (reply, receiver) = oneshot::channel();
    let token = LogicalRequestToken::new(NonZeroU64::MIN);
    let source = LogicalSourceId::new(NonZeroU64::MIN);
    let a = activation(1);
    owner.lock().pending.insert(
        token,
        Pending {
            source,
            activation: a,
            reply: Reply::Internal(reply),
        },
    );
    drop(receiver);
    let original = protocol("accepted original governed cancellation");
    assert!(matches!(
        owner
            .execute(ConsumerEffect::InternalReply {
                source,
                token,
                activation: a,
                response: InternalResponse::Rejected(original.clone())
            })
            .await,
        ConsumerFact::Applied
    ));
    assert!(owner.lock().pending.is_empty());
    assert!(matches!(
        owner
            .execute(ConsumerEffect::InternalReply {
                source,
                token,
                activation: a,
                response: InternalResponse::Rejected(original)
            })
            .await,
        ConsumerFact::Failed(_)
    ));
}
#[tokio::test]
async fn internal_end_acknowledgement_loss_waits_for_original_exit_error() {
    let owner = Arc::new(
        ConsumerExecutor::new_internal(carrier(), plan(), NonZeroUsize::MIN, None).unwrap(),
    );
    let a = activation(1);
    let original = owner.bind_internal(a, root(a)).unwrap();
    let exit = original.supervise_exit().await.unwrap();
    let (mut stream, source) = original.into_parts();
    let (facts, mut observed) = mpsc::unbounded_channel();
    owner.attach_source(Box::new(Source(facts))).await.unwrap();
    let mut transfer = Box::pin(owner.deliver_internal_end(a, end()));
    assert!(poll_once(transfer.as_mut()).await.is_pending());
    let Some(InternalResultDelivery::End(delivery)) = stream.next().await.unwrap() else {
        panic!("provisional End expected");
    };
    let mut acknowledgement = Box::pin(delivery.complete());
    assert!(poll_once(acknowledgement.as_mut()).await.is_pending());
    drop(acknowledgement);
    assert!(poll_once(transfer.as_mut()).await.is_pending());
    let LogicalHostFact::Internal {
        token,
        request: InternalRequest::EndDecoded { .. },
        ..
    } = observed.recv().await.unwrap()
    else {
        panic!("decoded End fact expected");
    };
    owner
        .execute(ConsumerEffect::InternalReply {
            source: LogicalSourceId::new(NonZeroU64::MIN),
            token,
            activation: a,
            response: InternalResponse::Applied,
        })
        .await;
    assert!(
        poll_once(transfer.as_mut()).await.is_pending(),
        "lost ACK is not the supervised domain verdict"
    );
    let error = protocol("original error after acknowledgement loss");
    exit.complete(Err(error.clone()));
    assert!(poll_once(transfer.as_mut()).await.is_pending());
    let LogicalHostFact::Internal {
        token,
        request: InternalRequest::ExitObserved {
            outcome: Some(Err(original)),
        },
        ..
    } = observed.recv().await.unwrap()
    else {
        panic!("original exit verdict expected");
    };
    assert_eq!(original, error);
    owner
        .execute(ConsumerEffect::InternalReply {
            source: LogicalSourceId::new(NonZeroU64::MIN),
            token,
            activation: a,
            response: InternalResponse::Applied,
        })
        .await;
    assert_eq!(transfer.await.err(), Some(error));
    drop(source);
}
#[tokio::test]
async fn pending_internal_end_receives_original_frozen_failure_before_actual_exit() {
    for detail in [
        "original frozen task failure",
        "original accepted governed cancellation",
    ] {
        let owner = Arc::new(
            ConsumerExecutor::new_internal(carrier(), plan(), NonZeroUsize::MIN, None).unwrap(),
        );
        let a = activation(1);
        let original = owner.bind_internal(a, root(a)).unwrap();
        let exit = original.supervise_exit().await.unwrap();
        let (mut stream, source) = original.into_parts();
        let (facts, mut observed) = mpsc::unbounded_channel();
        owner.attach_source(Box::new(Source(facts))).await.unwrap();
        let mut transfer = Box::pin(owner.deliver_internal_end(a, end()));
        assert!(poll_once(transfer.as_mut()).await.is_pending());
        let Some(InternalResultDelivery::End(delivery)) = stream.next().await.unwrap() else {
            panic!("provisional End expected");
        };
        let mut domain = Box::pin(delivery.complete());
        assert!(poll_once(domain.as_mut()).await.is_pending());
        assert!(poll_once(transfer.as_mut()).await.is_pending());
        let LogicalHostFact::Internal {
            token,
            request: InternalRequest::EndDecoded { .. },
            ..
        } = observed.recv().await.unwrap()
        else {
            panic!("decoded End fact expected");
        };
        let error = protocol(detail);
        owner
            .execute(ConsumerEffect::PublishAttemptFailure {
                activation: a,
                error: error.clone(),
            })
            .await;
        owner
            .execute(ConsumerEffect::InternalReply {
                source: LogicalSourceId::new(NonZeroU64::MIN),
                token,
                activation: a,
                response: InternalResponse::Rejected(error.clone()),
            })
            .await;
        assert_eq!(transfer.await.err(), Some(error.clone()));
        assert_eq!(domain.await.err(), Some(error.clone()));
        assert_eq!(stream.current_failure(), Some(error));
        assert!(
            owner
                .lock()
                .internal
                .get(&key(a))
                .unwrap()
                .exit
                .as_ref()
                .unwrap()
                .peek()
                .is_none(),
            "settled End is not actual consumer exit"
        );
        exit.complete(Ok(()));
        drop(source);
    }
}

fn completion() -> (
    novarocks_workload_control::WorkloadControl,
    novarocks_workload_control::RootWork,
    ConsumerExecutor,
    oneshot::Receiver<Result<ExecutionOutputObligation, QueryExecutionError>>,
) {
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1 << 12,
            per_scope_bytes: 1 << 18,
        },
    )
    .unwrap();
    control.mark_ready().unwrap();
    let original = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let (reply, receiver) = oneshot::channel();
    let owner = ConsumerExecutor::new_completion(original.owner.cancellation_requester(), reply);
    (control, original, owner, receiver)
}
#[tokio::test]
async fn completion_only_waits_for_owner_success_and_returns_original_completion_obligation() {
    let (_control, original, owner, mut receiver) = completion();
    assert!(
        poll_once(Pin::new(&mut receiver)).await.is_pending(),
        "neither installation nor row exposure implies success"
    );
    assert!(owner.carrier().is_err());
    assert!(owner.lock().schema.is_none());
    assert!(owner.lock().output.is_none());
    assert!(matches!(
        owner.execute(ConsumerEffect::PublishCompletion).await,
        ConsumerFact::Applied
    ));
    let mut output = receiver.await.unwrap().unwrap();
    assert!(matches!(
        output.take_output(),
        Some(ExecutionOutput::Completion)
    ));
    assert!(output.ready_token().is_none());
    assert!(
        output.take_exit_observer().is_none(),
        "completion publication does not manufacture actual exit"
    );
    output.request_cancel().unwrap();
    assert_eq!(
        original.owner.scope().cancellation().unwrap().reason(),
        Some(novarocks_workload_control::CancellationReason::Requested)
    );
}
#[tokio::test]
async fn completion_failure_uses_the_same_original_reply_without_a_client_channel() {
    let (_control, _original, owner, receiver) = completion();
    let error = protocol("original completion task failure");
    assert!(owner.lock().failure.is_none());
    assert!(matches!(
        owner
            .execute(ConsumerEffect::PublishFailure(error.clone()))
            .await,
        ConsumerFact::Applied
    ));
    assert!(matches!(receiver.await.unwrap(), Err(original) if original == error));
    assert!(
        matches!(owner.execute(ConsumerEffect::PublishCompletion).await, ConsumerFact::Failed(original) if original == error)
    );
}
#[tokio::test]
async fn published_completion_cannot_be_replaced_by_late_cleanup_failure() {
    let (_control, _original, owner, receiver) = completion();
    owner.execute(ConsumerEffect::PublishCompletion).await;
    let late = protocol("late original cleanup error");
    assert!(matches!(
        owner.execute(ConsumerEffect::PublishFailure(late)).await,
        ConsumerFact::Applied
    ));
    assert!(matches!(
        owner.execute(ConsumerEffect::PublishCompletion).await,
        ConsumerFact::Applied
    ));
    let mut output = receiver.await.unwrap().unwrap();
    assert!(matches!(
        output.take_output(),
        Some(ExecutionOutput::Completion)
    ));
    assert!(matches!(
        owner.lock().completion.as_ref().unwrap().published,
        Some(Ok(()))
    ));
}
#[tokio::test]
async fn completion_receiver_cancellation_does_not_fabricate_a_new_result_failure() {
    let (_control, _original, owner, receiver) = completion();
    drop(receiver);
    assert!(matches!(
        owner.execute(ConsumerEffect::PublishCompletion).await,
        ConsumerFact::Applied
    ));
    assert!(matches!(
        owner.lock().completion.as_ref().unwrap().published,
        Some(Ok(()))
    ));
}
#[tokio::test]
async fn result_stream_cannot_be_repurposed_as_a_completion_reply() {
    let owner = client();
    assert!(matches!(
        owner.execute(ConsumerEffect::PublishCompletion).await,
        ConsumerFact::Failed(_)
    ));
    assert!(owner.lock().output.is_some());
    assert!(owner.lock().schema.is_some());
}
#[tokio::test]
async fn independent_stop_settles_original_internal_end_after_start_waiter_vanished() {
    let owner = Arc::new(
        ConsumerExecutor::new_internal(carrier(), plan(), NonZeroUsize::MIN, None).unwrap(),
    );
    let a = activation(1);
    let original = owner.bind_internal(a, root(a)).unwrap();
    let exit = original.supervise_exit().await.unwrap();
    let (mut stream, source) = original.into_parts();
    let (facts, mut observed) = mpsc::unbounded_channel();
    owner.attach_source(Box::new(Source(facts))).await.unwrap();
    let slot = owner.reserve_segment(a).await.unwrap();
    let start_waiter = owner.deliver_internal_end_in(a, end(), slot);
    drop(start_waiter);
    assert!(!owner.deliveries_settled(a));
    let Some(InternalResultDelivery::End(delivery)) = stream.next().await.unwrap() else {
        panic!("same original provisional End expected");
    };
    let mut cleanup = Box::pin(owner.settle_deliveries(a));
    assert!(poll_once(cleanup.as_mut()).await.is_pending());
    let mut decoded = Box::pin(delivery.complete());
    assert!(poll_once(decoded.as_mut()).await.is_pending());
    assert!(poll_once(cleanup.as_mut()).await.is_pending());
    let LogicalHostFact::Internal {
        token,
        request: InternalRequest::EndDecoded { .. },
        ..
    } = observed.recv().await.unwrap()
    else {
        panic!("original EndDecoded expected");
    };
    owner
        .execute(ConsumerEffect::InternalReply {
            source: LogicalSourceId::new(NonZeroU64::MIN),
            token,
            activation: a,
            response: InternalResponse::Applied,
        })
        .await;
    cleanup.await.unwrap();
    assert_eq!(decoded.await.unwrap().end(), end());
    assert!(owner.deliveries_settled(a));
    assert!(
        owner
            .lock()
            .internal
            .get(&key(a))
            .unwrap()
            .exit
            .as_ref()
            .unwrap()
            .peek()
            .is_none(),
        "physical End settlement cannot certify actual consumer exit"
    );
    exit.complete(Ok(()));
    drop(source);
}

#[tokio::test]
async fn original_client_dropped_receipt_keeps_query_60_unexpected() {
    use novarocks_execution_contract::failure::{FailureClass, FailureReason, domain};
    let (signal, receipt) = crate::api::result::DeliverySignal::channel();
    drop(signal);
    let error = consumed(receipt).await.unwrap_err();
    let input = error.execution_failure().unwrap();
    assert_eq!(input.fact().class(), FailureClass::Unexpected);
    assert_eq!(
        input.fact().reason(),
        FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(60).unwrap())
    );
    assert_eq!(
        error,
        novarocks_logical_execution_protocol::failure::result_delivery_failed()
    );
}

#[tokio::test]
async fn supervised_failure_preserves_end_already_handed_to_original_channel() {
    let owner = client();
    let a = activation(1);
    let mut output = exposed(&owner).await;
    output.begin_schema().unwrap().complete();
    owner.record_end(a, end()).unwrap();
    assert!(matches!(
        owner
            .execute(ConsumerEffect::ReserveEndSlot { activation: a })
            .await,
        ConsumerFact::EndSlotReady
    ));
    let mut write = owner.execute(ConsumerEffect::EnqueueEnd { activation: a });
    assert!(poll_once(write.as_mut()).await.is_pending());
    owner.begin_supervised_failure(protocol("original logical owner failed after End handoff"));
    assert_eq!(
        owner.lock().client_end_publication,
        ClientEndPublication::HandedOff
    );
    let ResultDelivery::End(original) = output.next().await.unwrap().unwrap() else {
        panic!("the exact original success End must survive supervision failure");
    };
    assert_eq!(original.execution_id(), a.execution());
    assert_eq!(original.root_output_rows(), Some(end().output_rows));
    assert!(poll_once(write.as_mut()).await.is_pending());
    original.complete();
    assert!(matches!(write.await, ConsumerFact::EndSettled));
}

#[tokio::test]
async fn supervised_failure_does_not_publish_an_unpolled_original_end() {
    let owner = client();
    let a = activation(1);
    let mut output = exposed(&owner).await;
    output.begin_schema().unwrap().complete();
    owner.record_end(a, end()).unwrap();
    owner
        .execute(ConsumerEffect::ReserveEndSlot { activation: a })
        .await;
    let write = owner.execute(ConsumerEffect::EnqueueEnd { activation: a });
    let error = protocol("original logical owner failed before End handoff");
    owner.begin_supervised_failure(error.clone());
    assert!(matches!(write.await, ConsumerFact::Failed(actual) if actual == error));
    assert!(matches!(output.next().await, Err(actual) if actual == error));
    assert_eq!(
        owner.lock().client_end_publication,
        ClientEndPublication::Owed
    );
    assert!(
        owner.lock().end_slots.contains_key(&key(a)),
        "failure does not fake physical retirement of the original unused End position"
    );
    owner.retire_pending_end(a);
}

#[tokio::test]
async fn supervised_failure_preserves_original_completion_promise() {
    let (_control, _original, owner, receiver) = completion();
    assert!(matches!(
        owner.execute(ConsumerEffect::PublishCompletion).await,
        ConsumerFact::Applied
    ));
    owner.begin_supervised_failure(protocol("original owner failed after completion reply"));
    let mut output = receiver.await.unwrap().unwrap();
    assert!(matches!(
        output.take_output(),
        Some(ExecutionOutput::Completion)
    ));
    assert!(matches!(
        owner.lock().completion.as_ref().unwrap().published,
        Some(Ok(()))
    ));
    assert!(!owner.fail_one_supervised_request());
}

#[tokio::test]
async fn supervised_failure_during_original_end_enqueue_does_not_steal_the_handoff() {
    struct FailOnWake {
        owner: std::sync::Weak<ConsumerExecutor>,
        called: std::sync::atomic::AtomicBool,
    }
    impl std::task::Wake for FailOnWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let owner = self.owner.upgrade().unwrap();
            assert_eq!(
                owner.lock().client_end_publication,
                ClientEndPublication::Publishing
            );
            self.called
                .store(true, std::sync::atomic::Ordering::Release);
            owner.begin_supervised_failure(protocol("owner failure in original End wake"));
        }
    }
    let owner = Arc::new(client());
    let a = activation(1);
    let mut output = exposed(&owner).await;
    output.begin_schema().unwrap().complete();
    owner.record_end(a, end()).unwrap();
    owner
        .execute(ConsumerEffect::ReserveEndSlot { activation: a })
        .await;
    let notification = Arc::new(FailOnWake {
        owner: Arc::downgrade(&owner),
        called: std::sync::atomic::AtomicBool::new(false),
    });
    let wake = std::task::Waker::from(notification.clone());
    let mut next = Box::pin(output.next());
    assert!(
        next.as_mut()
            .poll(&mut std::task::Context::from_waker(&wake))
            .is_pending()
    );
    drop(next);
    let mut write = owner.execute(ConsumerEffect::EnqueueEnd { activation: a });
    assert!(poll_once(write.as_mut()).await.is_pending());
    assert!(
        notification
            .called
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(
        owner.lock().client_end_publication,
        ClientEndPublication::HandedOff
    );
    let ResultDelivery::End(original) = output.next().await.unwrap().unwrap() else {
        panic!("supervision cannot replace the original in-flight End");
    };
    original.complete();
    assert!(matches!(write.await, ConsumerFact::EndSettled));
}

#[tokio::test]
async fn supervised_failure_drains_original_request_once_and_closes_late_admission() {
    let owner = client();
    let (facts, mut observed) = mpsc::unbounded_channel();
    owner.attach_source(Box::new(Source(facts))).await.unwrap();
    let request = InternalRequest::Failed(report(protocol("original internal fact")));
    let mut pending = Box::pin(owner.request(activation(1), request.clone()));
    assert!(poll_once(pending.as_mut()).await.is_pending());
    assert!(matches!(
        observed.recv().await,
        Some(LogicalHostFact::Internal { .. })
    ));
    let error = protocol("original owner supervision cause");
    owner.begin_supervised_failure(error.clone());
    owner.begin_supervised_failure(protocol("secondary owner failure"));
    assert!(owner.fail_one_supervised_request());
    assert!(!owner.fail_one_supervised_request());
    assert!(matches!(pending.await, Err(actual) if actual == error));
    assert!(matches!(owner.request(activation(1), request).await, Err(actual) if actual == error));
    assert!(
        observed.try_recv().is_err(),
        "late failure does not create a fresh source request"
    );
}

#[tokio::test]
async fn supervised_failure_preserves_canonical_error_when_original_source_closes() {
    struct ClosingSource(Option<oneshot::Receiver<()>>);
    impl LogicalFactSource for ClosingSource {
        fn id(&self) -> LogicalSourceId {
            LogicalSourceId::new(NonZeroU64::MIN)
        }
        fn send(
            &mut self,
            _: LogicalHostFact,
        ) -> LogicalFuture<'_, Result<(), LogicalSourceClosed>> {
            Box::pin(async move {
                self.0.take().unwrap().await.unwrap();
                Err(LogicalSourceClosed)
            })
        }
    }
    let owner = client();
    let (close, closed) = oneshot::channel();
    owner
        .attach_source(Box::new(ClosingSource(Some(closed))))
        .await
        .unwrap();
    let mut pending = Box::pin(owner.request(
        activation(1),
        InternalRequest::Failed(report(protocol("original internal fact"))),
    ));
    assert!(poll_once(pending.as_mut()).await.is_pending());
    let error = protocol("canonical failure before actual source close");
    owner.begin_supervised_failure(error.clone());
    assert!(owner.fail_one_supervised_request());
    close.send(()).unwrap();
    assert!(matches!(pending.await, Err(actual) if actual == error));
    assert!(!owner.fail_one_supervised_request());
}

#[tokio::test]
async fn supervised_failure_before_ready_wakes_original_waiter_without_accepting_payload() {
    let owner = Arc::new(
        ConsumerExecutor::new_internal(carrier(), plan(), NonZeroUsize::MIN, None).unwrap(),
    );
    let a = activation(1);
    let original = owner.bind_internal(a, root(a)).unwrap();
    let exit = original.supervise_exit().await.unwrap();
    let (mut stream, source) = original.into_parts();
    let mut ready = Box::pin(owner.wait_internal_ready(a));
    assert!(poll_once(ready.as_mut()).await.is_pending());
    let error = protocol("original owner failed before Ready handoff");
    owner.begin_supervised_failure(error.clone());
    assert!(matches!(ready.await, Err(actual) if actual == error));
    assert!(matches!(stream.next().await, Err(actual) if actual == error));
    assert!(
        owner
            .lock()
            .internal
            .get(&key(a))
            .unwrap()
            .decoded
            .is_none()
    );
    let mut actual_exit = Box::pin(
        owner
            .lock()
            .internal
            .get(&key(a))
            .unwrap()
            .exit
            .clone()
            .unwrap(),
    );
    assert!(
        poll_once(actual_exit.as_mut()).await.is_pending(),
        "owner failure is not original source exit"
    );
    exit.complete(Ok(()));
    assert!(matches!(actual_exit.await, Some(Ok(()))));
    drop(source);
}

#[derive(Clone, Copy)]
enum FailureWakeDestination {
    Client,
    Internal(AttemptActivationIdentity),
    Completion,
}
struct ReenterOriginalFailureWake {
    owner: Weak<ConsumerExecutor>,
    destination: FailureWakeDestination,
    original: QueryExecutionError,
    supervision: QueryExecutionError,
    calls: std::sync::atomic::AtomicUsize,
}
impl std::task::Wake for ReenterOriginalFailureWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let owner = self.owner.upgrade().unwrap();
        // Fail synchronously on the old implementation instead of hanging a
        // test worker behind its non-reentrant custody mutex.
        let state = owner.state.try_lock().unwrap_or_else(|_| {
            panic!("original failure notification must release Consumer State before waking");
        });
        match self.destination {
            FailureWakeDestination::Client => {
                assert_eq!(state.client_failure_issued.as_ref(), Some(&self.original));
            }
            FailureWakeDestination::Internal(activation) => {
                let original = state.internal.get(&key(activation)).unwrap();
                assert_eq!(original.failure_issued.as_ref(), Some(&self.original));
                assert!(original.ready_issued.is_none());
            }
            FailureWakeDestination::Completion => {
                assert!(matches!(&state.completion.as_ref().unwrap().published,
                    Some(Err(error)) if error == &self.original));
            }
        }
        drop(state);
        assert!(
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 3,
            "idempotent original failure notifications must not recurse indefinitely"
        );
        owner.begin_supervised_failure(self.supervision.clone());
    }
}
fn reenter_failure_wake(
    owner: &Arc<ConsumerExecutor>,
    destination: FailureWakeDestination,
    original: QueryExecutionError,
    supervision: QueryExecutionError,
) -> Arc<ReenterOriginalFailureWake> {
    Arc::new(ReenterOriginalFailureWake {
        owner: Arc::downgrade(owner),
        destination,
        original,
        supervision,
        calls: std::sync::atomic::AtomicUsize::new(0),
    })
}
#[tokio::test]
async fn ordinary_client_failure_notification_reenters_supervision_without_lock_or_cause_replacement()
 {
    let owner = Arc::new(client());
    let mut output = exposed(&owner).await;
    output.begin_schema().unwrap().complete();
    let original = protocol("original client business failure");
    let supervision = protocol("independent supervision failure during client wake");
    let observer = reenter_failure_wake(
        &owner,
        FailureWakeDestination::Client,
        original.clone(),
        supervision.clone(),
    );
    let waker = std::task::Waker::from(observer.clone());
    let mut view = output.failure_view().unwrap();
    let mut pending = Box::pin(view.wait());
    assert!(
        pending
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending()
    );
    assert!(matches!(
        owner
            .execute(ConsumerEffect::PublishFailure(original.clone()))
            .await,
        ConsumerFact::Applied
    ));
    assert_eq!(pending.await, original);
    assert_eq!(output.next().await.err(), Some(original.clone()));
    assert_eq!(owner.lock().supervision, Some(supervision));
    assert_eq!(owner.lock().client_failure_issued, Some(original));
    assert_eq!(observer.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}
#[tokio::test]
async fn ordinary_internal_failure_reentry_completes_same_failure_ready_tail_without_recursion() {
    let owner = Arc::new(
        ConsumerExecutor::new_internal(carrier(), plan(), NonZeroUsize::MIN, None).unwrap(),
    );
    let a = activation(1);
    let original_activation = owner.bind_internal(a, root(a)).unwrap();
    let (mut stream, _source) = original_activation.into_parts();
    let original = protocol("original internal attempt business failure");
    let supervision = protocol("independent supervision failure during internal wake");
    let observer = reenter_failure_wake(
        &owner,
        FailureWakeDestination::Internal(a),
        original.clone(),
        supervision.clone(),
    );
    let waker = std::task::Waker::from(observer.clone());
    let mut failure = Box::pin(stream.failure());
    let mut ready = owner
        .lock()
        .internal
        .get(&key(a))
        .unwrap()
        .ready
        .subscribe();
    let mut ready_changed = Box::pin(ready.changed());
    assert!(
        failure
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending()
    );
    assert!(
        ready_changed
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending()
    );
    assert!(matches!(
        owner
            .execute(ConsumerEffect::PublishAttemptFailure {
                activation: a,
                error: original.clone(),
            })
            .await,
        ConsumerFact::Applied
    ));
    assert_eq!(failure.await, original);
    ready_changed.await.unwrap();
    assert!(matches!(ready.borrow().as_ref(), Some(Err(error)) if error == &original));
    assert_eq!(owner.lock().supervision, Some(supervision));
    assert_eq!(
        owner.lock().internal.get(&key(a)).unwrap().failure_issued,
        Some(original)
    );
    assert_eq!(observer.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
}
#[tokio::test]
async fn ordinary_completion_failure_sends_original_reply_outside_lock_before_reentrant_supervision()
 {
    let (_control, _original, owner, mut receiver) = completion();
    let owner = Arc::new(owner);
    let original = protocol("original completion business failure");
    let supervision = protocol("independent supervision failure during completion wake");
    let observer = reenter_failure_wake(
        &owner,
        FailureWakeDestination::Completion,
        original.clone(),
        supervision.clone(),
    );
    let waker = std::task::Waker::from(observer.clone());
    assert!(
        Pin::new(&mut receiver)
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending()
    );
    assert!(matches!(
        owner
            .execute(ConsumerEffect::PublishFailure(original.clone()))
            .await,
        ConsumerFact::Applied
    ));
    assert!(matches!(receiver.await.unwrap(), Err(error) if error == original));
    assert_eq!(owner.lock().supervision, Some(supervision));
    assert!(
        matches!(&owner.lock().completion.as_ref().unwrap().published,
        Some(Err(error)) if error == &original)
    );
    assert_eq!(observer.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}
#[tokio::test]
async fn promised_internal_business_failure_survives_supervision_before_its_notification_tail() {
    let owner = Arc::new(
        ConsumerExecutor::new_internal(carrier(), plan(), NonZeroUsize::MIN, None).unwrap(),
    );
    let a = activation(1);
    let activation = owner.bind_internal(a, root(a)).unwrap();
    let (mut stream, _source) = activation.into_parts();
    let original = protocol("original failure reserved before notification");
    let supervision = protocol("supervision races before ordinary no-await notification");
    // Script the real production promise/notification cut while retaining the
    // same original channel pair. No watch value has been published yet.
    let tail = {
        let mut state = owner.lock();
        InternalFailureNotice::reserve(state.internal.get_mut(&key(a)).unwrap(), original.clone())
    };
    assert!(stream.current_failure().is_none());
    owner.begin_supervised_failure(supervision.clone());
    assert_eq!(stream.current_failure(), Some(original.clone()));
    tail.notify();
    assert_eq!(stream.failure().await, original.clone());
    assert!(
        matches!(owner.lock().internal.get(&key(a)).unwrap().ready.borrow().as_ref(),
        Some(Err(error)) if error == &original)
    );
    assert_eq!(owner.lock().supervision, Some(supervision));
}
#[tokio::test]
async fn later_global_business_failure_keeps_each_original_attempts_first_promise() {
    let owner = Arc::new(
        ConsumerExecutor::new_internal(carrier(), plan(), NonZeroUsize::MIN, None).unwrap(),
    );
    let a = activation(1);
    let b = activation(2);
    let old = owner.bind_internal(a, root(a)).unwrap();
    let new = owner.bind_internal(b, root(b)).unwrap();
    let (mut old, _old_source) = old.into_parts();
    let (mut new, _new_source) = new.into_parts();
    let original = protocol("original failed attempt A");
    let later = protocol("later accepted logical failure from B");
    owner
        .execute(ConsumerEffect::PublishAttemptFailure {
            activation: a,
            error: original.clone(),
        })
        .await;
    owner
        .execute(ConsumerEffect::PublishFailure(later.clone()))
        .await;
    assert_eq!(old.failure().await, original.clone());
    assert_eq!(new.failure().await, later.clone());
    assert!(
        matches!(owner.lock().internal.get(&key(a)).unwrap().ready.borrow().as_ref(),
        Some(Err(error)) if error == &original)
    );
    assert!(
        matches!(owner.lock().internal.get(&key(b)).unwrap().ready.borrow().as_ref(),
        Some(Err(error)) if error == &later)
    );
}
#[tokio::test]
async fn earlier_client_failure_prevents_late_end_without_disposing_the_original_unused_slot() {
    let owner = client();
    let a = activation(1);
    let mut output = exposed(&owner).await;
    output.begin_schema().unwrap().complete();
    owner.record_end(a, end()).unwrap();
    assert!(matches!(
        owner
            .execute(ConsumerEffect::ReserveEndSlot { activation: a })
            .await,
        ConsumerFact::EndSlotReady
    ));
    let original = protocol("original client failure before End tail");
    owner
        .execute(ConsumerEffect::PublishFailure(original.clone()))
        .await;
    assert!(
        matches!(owner.execute(ConsumerEffect::EnqueueEnd { activation: a }).await,
        ConsumerFact::Failed(error) if error == original)
    );
    assert_eq!(
        owner.lock().client_end_publication,
        ClientEndPublication::Owed
    );
    assert!(owner.lock().end_slots.contains_key(&key(a)));
    assert_eq!(output.next().await.err(), Some(original));
    owner.retire_pending_end(a);
}
#[tokio::test]
async fn ordinary_late_failure_preserves_actual_success_end_handoff() {
    let owner = client();
    let a = activation(1);
    let mut output = exposed(&owner).await;
    output.begin_schema().unwrap().complete();
    owner.record_end(a, end()).unwrap();
    owner
        .execute(ConsumerEffect::ReserveEndSlot { activation: a })
        .await;
    let mut write = owner.execute(ConsumerEffect::EnqueueEnd { activation: a });
    assert!(poll_once(write.as_mut()).await.is_pending());
    assert_eq!(
        owner.lock().client_end_publication,
        ClientEndPublication::HandedOff
    );
    assert!(matches!(
        owner
            .execute(ConsumerEffect::PublishFailure(protocol(
                "late business failure"
            )))
            .await,
        ConsumerFact::Applied
    ));
    assert!(owner.lock().client_failure_issued.is_none());
    assert!(owner.lock().failure.as_ref().unwrap().borrow().is_none());
    let ResultDelivery::End(delivery) = output.next().await.unwrap().unwrap() else {
        panic!("the original successful End promise");
    };
    delivery.complete();
    assert!(matches!(write.await, ConsumerFact::EndSettled));
}

#[tokio::test]
async fn promised_client_business_failure_survives_supervision_before_actual_notification() {
    let owner = Arc::new(client());
    let mut output = exposed(&owner).await;
    output.begin_schema().unwrap().complete();
    let original = protocol("original client failure reserved before notification");
    let supervision = protocol("supervision before client notification tail");
    let (sender, promised) = {
        let mut state = owner.lock();
        reserve_client_failure(&mut state, original.clone()).unwrap()
    };
    assert!(sender.borrow().is_none());
    owner.begin_supervised_failure(supervision.clone());
    assert_eq!(*sender.borrow(), Some(original.clone()));
    notify_original_failure(&sender, promised);
    assert_eq!(output.next().await.err(), Some(original));
    assert_eq!(owner.lock().supervision, Some(supervision));
}
