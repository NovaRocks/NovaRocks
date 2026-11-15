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
    assert_eq!(accept.await.unwrap().end(), end());
    assert_eq!(owner.wait_internal_ready(a).await.unwrap().root(), r);
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
