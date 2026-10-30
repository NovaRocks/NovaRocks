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

// Included as a child of actor::tests to share the existing governed fixtures.
use super::*;
use crate::api::{
    DecodedInternalEndReceipt, InternalEndDelivery, InternalResultDelivery, PlanSeal,
};

fn internal_config(
    execution: QueryExecutionId,
    port: Arc<dyn ReplacementQualificationEffectPort>,
) -> LogicalExecutionActorConfig {
    // This component fixture starts after description validation. It preserves
    // the existing row carrier used by internal profiling consumption.
    let mut config = recovery_config(execution, port, 2);
    config.output_mode = LogicalOutputMode::InternalResultStream;
    config.internal_plan = Some(PlanSeal::Version(
        novarocks_physical_plan::PlanVersionId::try_new([97; 16]).unwrap(),
    ));
    config.result_schema = None;
    config
}

struct InternalFixture {
    owner: LogicalExecutionActorOwner,
    running: Arc<RunningAttemptPermit>,
    observer: RootResultObserver,
    activation: InternalConsumerActivation,
    _output: LogicalExecutionOutputTransfer,
}

impl InternalFixture {
    async fn new(tag: i64) -> Self {
        Self::with_clock(tag, Arc::new(ProcessMonotonicClock::new())).await
    }

    async fn with_clock(tag: i64, clock: Arc<dyn MonotonicClock>) -> Self {
        Self::with_clock_and_qualification(
            tag,
            clock,
            Arc::new(DelayedQualificationPort::default()),
        )
        .await
    }

    async fn with_clock_and_qualification(
        tag: i64,
        clock: Arc<dyn MonotonicClock>,
        qualification: Arc<DelayedQualificationPort>,
    ) -> Self {
        let first = execution(tag);
        let config = internal_config(first, qualification).with_clock(clock);
        let (owner, initial, output) = spawn_logical_execution_actor(&Handle::current(), config)
            .unwrap()
            .into_parts();
        let running = Arc::new(owner.actor().activate(initial.ready()).await.unwrap());
        let root = root_task(first, 1);
        let activation = running.bind_internal_consumer(root).await.unwrap();
        let observer = running.bind_root_result(root).await.unwrap();
        Self {
            owner,
            running,
            observer,
            activation,
            _output: output,
        }
    }

    async fn end(
        &mut self,
        end: novarocks_execution_contract::root_result::RootResultEnd,
    ) -> (
        InternalEndDelivery,
        JoinHandle<Result<(), LogicalExecutionActorError>>,
    ) {
        let root = self.observer.root;
        self.observer
            .observe_local_root_end(root, end)
            .await
            .unwrap();
        let running = Arc::clone(&self.running);
        let delivery = tokio::spawn(async move { running.deliver_internal_end(root, end).await });
        let InternalResultDelivery::End(end) =
            self.activation.stream.next().await.unwrap().unwrap()
        else {
            panic!("the actor must deliver its original internal End");
        };
        (end, delivery)
    }

    async fn decoded_end(&mut self) -> DecodedInternalEndReceipt {
        let (end, delivery) = self.end(result_end(0)).await;
        let receipt = end.complete().await.unwrap();
        delivery.await.unwrap().unwrap();
        receipt
    }

    async fn finished(&self) {
        self.observer
            .observe_status(root_status(self.observer.root, 1, TaskState::Finished))
            .await
            .unwrap();
    }
}

async fn poll_pending<F: Future>(future: std::pin::Pin<&mut F>) {
    let mut future = future;
    assert!(matches!(
        std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await,
        Poll::Pending
    ));
}

#[tokio::test]
async fn internal_end_failure_preserves_domain_error_and_never_emits_ready() {
    let mut fixture = InternalFixture::new(12_001).await;
    fixture.finished().await;
    let (end, delivery) = fixture.end(result_end(0)).await;
    let error = actual_consumer_body_failure();
    end.fail(error.clone());
    assert!(delivery.await.unwrap().is_err());
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Failed).await;
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err(),
        &error
    );
    assert!(
        !fixture
            .owner
            .actor()
            .snapshot()
            .await
            .unwrap()
            .output_visible
    );
}

#[tokio::test]
async fn internal_end_decodes_while_running_and_waits_for_root_before_ready() {
    let mut fixture = InternalFixture::new(12_002).await;
    let decoded = fixture.decoded_end().await;
    let snapshot = fixture.owner.actor().snapshot().await.unwrap();
    assert!(matches!(snapshot.phase, ExecutionPhase::Running { .. }));
    assert_eq!(snapshot.conclusion, None);
    assert!(!snapshot.output_visible);
    let InternalConsumerActivation {
        source,
        ready,
        stream: _stream,
    } = fixture.activation;
    let mut acceptance = Box::pin(source.accept(decoded));
    poll_pending(acceptance.as_mut()).await;
    fixture.owner.actor().snapshot().await.unwrap();
    assert!(ready.borrow().is_none());
    fixture
        .observer
        .observe_status(root_status(fixture.observer.root, 1, TaskState::Finished))
        .await
        .unwrap();
    let token = acceptance.await.unwrap();
    assert_eq!(token.root(), fixture.observer.root);
    assert_eq!(token.end(), result_end(0));
    assert!(token.diagnostic_deadline() > token.cut());
    assert!(token.convergence_deadline() > token.cut());
    assert_eq!(ready.borrow().as_ref().unwrap().as_ref().unwrap(), &token);
    assert!(
        fixture
            .owner
            .actor()
            .snapshot()
            .await
            .unwrap()
            .output_visible
    );
}

#[tokio::test]
async fn cancellation_before_internal_ready_refuses_the_proof() {
    let mut fixture = InternalFixture::new(12_003).await;
    fixture.finished().await;
    let decoded = fixture.decoded_end().await;
    fixture
        .owner
        .actor()
        .request_registry_close_for_join(RegistryClose::Shutdown);
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Cancelled).await;
    assert!(
        !fixture
            .owner
            .actor()
            .snapshot()
            .await
            .unwrap()
            .output_visible
    );
    assert!(fixture.activation.source.accept(decoded).await.is_err());
    assert!(fixture.activation.ready.borrow().as_ref().unwrap().is_err());
}

#[tokio::test]
async fn internal_ready_before_cancellation_remains_the_first_conclusion() {
    let mut fixture = InternalFixture::new(12_004).await;
    fixture.finished().await;
    let decoded = fixture.decoded_end().await;
    let token = fixture.activation.source.accept(decoded).await.unwrap();
    let snapshot =
        wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::ComputationReady).await;
    assert!(snapshot.output_visible);
    let mut closed = fixture
        .owner
        .actor()
        .request_registry_close_for_join(RegistryClose::Shutdown);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !*closed.borrow() {
            closed.changed().await.unwrap();
        }
    })
    .await
    .expect("late shutdown must close the settled actor ingress");
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap(),
        &token
    );
}

#[tokio::test]
async fn dropped_source_while_proof_is_pending_fails_closed() {
    let mut fixture = InternalFixture::new(12_005).await;
    let decoded = fixture.decoded_end().await;
    let mut acceptance = Box::pin(fixture.activation.source.accept(decoded));
    poll_pending(acceptance.as_mut()).await;
    fixture.owner.actor().snapshot().await.unwrap();
    assert!(fixture.activation.ready.borrow().is_none());
    drop(acceptance);
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Failed).await;
    assert!(fixture.activation.ready.borrow().as_ref().unwrap().is_err());
}

#[tokio::test]
async fn internal_end_requires_the_bound_root_and_original_end_facts() {
    let mut fixture = InternalFixture::new(12_006).await;
    let root = fixture.observer.root;
    fixture
        .observer
        .observe_local_root_end(root, result_end(0))
        .await
        .unwrap();
    assert_eq!(
        fixture
            .running
            .deliver_internal_end(root_task(root.query_execution_id(), 2), result_end(0))
            .await
            .unwrap_err(),
        LogicalExecutionActorError::StaleAuthority
    );
    let mut wrong_rows = result_end(0);
    wrong_rows.output_rows = 1;
    assert_eq!(
        fixture
            .running
            .deliver_internal_end(root, wrong_rows)
            .await
            .unwrap_err(),
        LogicalExecutionActorError::ResultProtocolViolation
    );
    assert_eq!(
        fixture
            .running
            .deliver_internal_end(root, result_end(1))
            .await
            .unwrap_err(),
        LogicalExecutionActorError::ResultProtocolViolation
    );
    assert_eq!(
        fixture.owner.actor().snapshot().await.unwrap().conclusion,
        None
    );
    // Rejected proposals do not replace the original facts or poison the source.
    let running = Arc::clone(&fixture.running);
    let delivery =
        tokio::spawn(async move { running.deliver_internal_end(root, result_end(0)).await });
    let InternalResultDelivery::End(end) = fixture.activation.stream.next().await.unwrap().unwrap()
    else {
        panic!("the original End must still be accepted");
    };
    let decoded = end.complete().await.unwrap();
    delivery.await.unwrap().unwrap();
    fixture.finished().await;
    fixture.activation.source.accept(decoded).await.unwrap();
}

// The clock hook injects an already-authorized ingress event at the top of the
// actor turn immediately following proof receipt. It adds no production hook.
struct CutIngressClock {
    next: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl fmt::Debug for CutIngressClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CutIngressClock").finish_non_exhaustive()
    }
}
impl CutIngressClock {
    fn new() -> Self {
        Self {
            next: Mutex::new(None),
        }
    }
    fn arm(&self, hook: impl FnOnce() + Send + 'static) {
        assert!(self.next.lock().unwrap().replace(Box::new(hook)).is_none());
    }
}
impl MonotonicClock for CutIngressClock {
    fn now(&self) -> MonotonicInstant {
        if let Some(hook) = self.next.lock().unwrap().take() {
            hook();
        }
        MonotonicInstant::from_origin(Duration::from_nanos(1))
    }
}

#[tokio::test]
async fn registry_close_in_ready_turn_is_reduced_before_the_cut() {
    let clock = Arc::new(CutIngressClock::new());
    let mut fixture = InternalFixture::with_clock(12_007, clock.clone()).await;
    fixture.finished().await;
    let decoded = fixture.decoded_end().await;
    let mut acceptance = Box::pin(fixture.activation.source.accept(decoded));
    poll_pending(acceptance.as_mut()).await;
    let close = fixture.owner.actor().registry_close_requested.clone();
    clock.arm(move || {
        close.send_replace(Some(RegistryClose::Shutdown));
    });
    assert!(acceptance.await.is_err());
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .kind(),
        QueryExecutionErrorKind::Cancelled
    );
}

#[tokio::test]
async fn queued_root_failure_in_ready_turn_prevents_the_cut() {
    let clock = Arc::new(CutIngressClock::new());
    let mut fixture = InternalFixture::with_clock(12_008, clock.clone()).await;
    fixture.finished().await;
    let decoded = fixture.decoded_end().await;
    let mut acceptance = Box::pin(fixture.activation.source.accept(decoded));
    poll_pending(acceptance.as_mut()).await;
    let terminal = fixture.observer.terminal_mailbox.clone();
    let root = fixture.observer.root;
    let activation = fixture.observer.activation;
    let (reply, response) = oneshot::channel();
    clock.arm(move || {
        terminal
            .try_send(RootTerminalObservation {
                activation,
                root,
                // The reserved ingress refines the root seal with the exact
                // attempt's authoritative failure before readiness is accepted.
                status: root_status(root, 1, TaskState::Finished),
                terminal_failure: RootTerminalFailure::Authoritative(Box::new(
                    actual_consumer_body_failure(),
                )),
                reply,
            })
            .unwrap();
    });
    assert!(response.await.unwrap().is_err());
    fixture.owner.actor().snapshot().await.unwrap();
    assert!(fixture.activation.ready.borrow().is_none());
    assert_eq!(
        fixture.owner.actor().snapshot().await.unwrap().conclusion,
        None
    );
    // The exact attempt owner still chooses failure/replacement after the fact.
    drop(acceptance);
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Failed).await;
}

async fn replace_internal_attempt(
    owner: &LogicalExecutionActorOwner,
    running: RunningAttemptPermit,
    port: &DelayedQualificationPort,
) -> RunningAttemptPermit {
    let successor = replacement(running.identity().execution(), 2);
    let qualification = owner
        .actor()
        .begin_replacement(
            running,
            AttemptFailureClass::RecoverableInfrastructure,
            successor,
            Vec::new(),
        )
        .await
        .unwrap();
    let mut activation = Box::pin(owner.actor().activate_replacement(qualification));
    poll_pending(activation.as_mut()).await;
    qualify(port.submissions.lock().unwrap().pop().unwrap());
    owner
        .actor()
        .activate(activation.await.unwrap().ready())
        .await
        .unwrap()
}

#[tokio::test]
async fn old_source_and_retained_input_cannot_mutate_or_release_the_successor() {
    let first = execution(12_009);
    let port = Arc::new(DelayedQualificationPort::default());
    let (owner, initial, _output) =
        spawn_logical_execution_actor(&Handle::current(), internal_config(first, port.clone()))
            .unwrap()
            .into_parts();
    let outcome = owner.actor().internal_outcome().unwrap();
    let running = Arc::new(owner.actor().activate(initial.ready()).await.unwrap());
    let (control, window_owner, capacity, segment) = result_segment(first, 1);
    let root = segment.root_task();
    let mut old = running.bind_internal_consumer(root).await.unwrap();
    let old_observer = running.bind_root_result(root).await.unwrap();
    let sender = Arc::clone(&running);
    let delivery = tokio::spawn(async move {
        sender
            .deliver_root_segment(
                ResultPacketSequence::new(0),
                segment,
                Some((result_profile(), ClientRowStreamCursor::default())),
                2,
                crate::api::RootRelayResidentWindow::default(),
            )
            .await
    });
    let InternalResultDelivery::Segment(segment) = old.stream.next().await.unwrap().unwrap() else {
        panic!("the original segment must reach its consumer");
    };
    let (input, consumption) = segment.into_parts();
    assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
    // Abandon the pump waiter, not the consumer's retained original input.
    delivery.abort();
    assert!(delivery.await.unwrap_err().is_cancelled());
    let running = Arc::try_unwrap(running).expect("the pump waiter relinquished its permit");
    let successor = replace_internal_attempt(&owner, running, &port).await;
    drop(old_observer);
    let next_root = root_task(successor.identity().execution(), 1);
    let mut next = successor.bind_internal_consumer(next_root).await.unwrap();
    let next_observer = successor.bind_root_result(next_root).await.unwrap();
    assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
    assert!(old.ready.has_changed().is_err());
    assert!(old.ready.borrow().is_none());
    assert!(outcome.borrow().is_none());
    assert!(
        !outcome.has_changed().unwrap(),
        "retirement leaves the execution observer alive and undecided"
    );
    assert!(
        old.source
            .fail(actual_consumer_body_failure())
            .await
            .is_err()
    );
    assert_eq!(owner.actor().snapshot().await.unwrap().conclusion, None);
    // Completing a retired receipt must not advance the successor's watermark.
    consumption.complete();
    assert_eq!(
        owner
            .actor()
            .snapshot()
            .await
            .unwrap()
            .accepted_result_packets,
        0
    );
    assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
    drop(input);
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    next_observer
        .observe_local_root_end(next_root, result_end(0))
        .await
        .unwrap();
    let sender = Arc::new(successor);
    let end_sender = Arc::clone(&sender);
    let end_task = tokio::spawn(async move {
        end_sender
            .deliver_internal_end(next_root, result_end(0))
            .await
    });
    let InternalResultDelivery::End(end) = next.stream.next().await.unwrap().unwrap() else {
        panic!("the successor must own a distinct End");
    };
    let decoded = end.complete().await.unwrap();
    end_task.await.unwrap().unwrap();
    next_observer
        .observe_status(root_status(next_root, 1, TaskState::Finished))
        .await
        .unwrap();
    let token = next.source.accept(decoded).await.unwrap();
    wait_for_conclusion(owner.actor(), LogicalConclusion::ComputationReady).await;
    assert_eq!(outcome.borrow().as_ref().unwrap().as_ref().unwrap(), &token);
    window_owner.complete();
    drop(control);
}

#[test]
fn internal_profile_data_and_end_remain_replaceable_until_ready() {
    let first = execution(12_010);
    let mut state = LogicalExecutionState::new(
        first,
        RecoveryMode::RestartAttemptBeforeVisibility,
        ExecutionEffect::None,
        LogicalOutputMode::InternalResultStream,
        2,
    )
    .unwrap();
    let capability = state.attempt_capability(first).unwrap();
    state.mark_running(&capability).unwrap();
    let data = state
        .begin_result_delivery(&capability, 0, ResultPacket::Data(()))
        .unwrap();
    assert!(!state.output_visible());
    state.complete_result_delivery(data).unwrap();
    let end = state.begin_internal_end(&capability, 1).unwrap();
    state.complete_result_delivery(end).unwrap();
    assert!(!state.output_visible());
    assert!(matches!(state.phase(), ExecutionPhase::Running { .. }));
    let _pending_replacement = state
        .begin_replacement(
            &capability,
            replacement(first, 2),
            AttemptFailureClass::RecoverableInfrastructure,
            None,
            false,
        )
        .unwrap();

    let mut accepted = LogicalExecutionState::new(
        first,
        RecoveryMode::RestartAttemptBeforeVisibility,
        ExecutionEffect::None,
        LogicalOutputMode::InternalResultStream,
        2,
    )
    .unwrap();
    let capability = accepted.attempt_capability(first).unwrap();
    accepted.mark_running(&capability).unwrap();
    let end = accepted.begin_internal_end(&capability, 0).unwrap();
    accepted.complete_result_delivery(end).unwrap();
    accepted.accept_computation_ready(&capability).unwrap();
    assert!(accepted.output_visible());
    assert_eq!(
        accepted
            .begin_replacement(
                &capability,
                replacement(first, 2),
                AttemptFailureClass::RecoverableInfrastructure,
                None,
                false
            )
            .unwrap_err(),
        ActorStateError::AlreadyConcluded
    );
}

#[tokio::test]
async fn supervised_source_drop_retains_the_actual_consumer_exit_error() {
    let fixture = InternalFixture::new(12_010).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    let error = result_protocol_failure("the internal domain rejected its actual decoded result");
    drop(fixture.activation.source);
    assert_eq!(
        fixture.owner.actor().snapshot().await.unwrap().conclusion,
        None,
        "the reserved actual-exit owner still holds its verdict"
    );
    exit.complete(Err(error.clone()));
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Failed).await;
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err(),
        &error
    );
    assert_eq!(
        fixture
            .owner
            .actor()
            .internal_outcome()
            .unwrap()
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err(),
        &error
    );
}

#[tokio::test]
async fn pending_proof_source_loss_waits_for_its_reserved_typed_exit() {
    let mut fixture = InternalFixture::new(12_011).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    let decoded = fixture.decoded_end().await;
    let mut acceptance = Box::pin(fixture.activation.source.accept(decoded));
    poll_pending(acceptance.as_mut()).await;
    fixture.owner.actor().snapshot().await.unwrap();
    drop(acceptance);
    fixture
        .observer
        .observe_status(root_status(fixture.observer.root, 1, TaskState::Finished))
        .await
        .unwrap();
    assert_eq!(
        fixture.owner.actor().snapshot().await.unwrap().conclusion,
        None
    );
    let error = result_protocol_failure("the abandoned proof retained its actual consumer failure");
    exit.complete(Err(error.clone()));
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Failed).await;
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err(),
        &error
    );
}

#[tokio::test]
async fn consumer_exit_after_ready_cannot_revoke_the_accepted_cut() {
    let mut fixture = InternalFixture::new(12_012).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    fixture.finished().await;
    let decoded = fixture.decoded_end().await;
    let accepted = fixture.activation.source.accept(decoded).await.unwrap();
    exit.complete(Err(result_protocol_failure(
        "a late consumer failure is only diagnostic",
    )));
    let snapshot = fixture.owner.actor().snapshot().await.unwrap();
    assert_eq!(
        snapshot.conclusion,
        Some(LogicalConclusion::ComputationReady)
    );
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap(),
        &accepted
    );
    assert_eq!(
        fixture
            .owner
            .actor()
            .internal_outcome()
            .unwrap()
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap(),
        &accepted,
    );
}

async fn assert_internal_exit_is_pending(
    actor: &LogicalExecutionActor,
    ready: &watch::Receiver<Option<Result<crate::api::ComputationReadyToken, QueryExecutionError>>>,
) {
    assert_eq!(
        actor.snapshot().await.unwrap().conclusion,
        None,
        "a delivery destructor cannot preempt the reserved actual-exit verdict"
    );
    assert!(
        ready.borrow().is_none(),
        "no generic failure or Ready may escape"
    );
    assert!(actor.internal_outcome().unwrap().borrow().is_none());
}

async fn assert_internal_exit_preserves_error(
    actor: &LogicalExecutionActor,
    ready: &watch::Receiver<Option<Result<crate::api::ComputationReadyToken, QueryExecutionError>>>,
    error: QueryExecutionError,
) {
    wait_for_conclusion(actor, LogicalConclusion::Failed).await;
    assert_eq!(
        actor
            .internal_outcome()
            .unwrap()
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err(),
        &error
    );
    assert_eq!(
        ready.borrow().as_ref().unwrap().as_ref().unwrap_err(),
        &error
    );
}

#[tokio::test]
async fn supervised_segment_drop_waits_for_its_original_exit_error() {
    let mut fixture = InternalFixture::new(12_013).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    let root = fixture.observer.root;
    let (control, window_owner, capacity, original) = result_segment(root.query_execution_id(), 1);
    // The shared result fixture mints a fresh backend process identity. Bind
    // this fixture reply to the exact root already registered by InternalFixture.
    let mut response = original.response().clone();
    let novarocks_execution_contract::root_result::RootReadResponse::Reply(reply) = &mut response
    else {
        panic!("the shared result fixture must produce one root reply");
    };
    reply.root_task = root;
    let segment = Arc::new(
        RetainedRootReply::try_new(response, original.retain_physical_guard(), 4096).unwrap(),
    );
    drop(original);
    let running = Arc::clone(&fixture.running);
    let delivery = tokio::spawn(async move {
        running
            .deliver_root_segment(
                ResultPacketSequence::new(0),
                segment,
                Some((result_profile(), ClientRowStreamCursor::default())),
                2,
                crate::api::RootRelayResidentWindow::default(),
            )
            .await
    });
    let InternalResultDelivery::Segment(segment) =
        fixture.activation.stream.next().await.unwrap().unwrap()
    else {
        panic!("the supervised consumer must receive the original segment");
    };
    let (input, consumption) = segment.into_parts();
    drop(consumption);
    drop(fixture.activation.source);
    assert_internal_exit_is_pending(fixture.owner.actor(), &fixture.activation.ready).await;
    assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
    let error = actual_consumer_body_failure();
    exit.complete(Err(error.clone()));
    assert_internal_exit_preserves_error(fixture.owner.actor(), &fixture.activation.ready, error)
        .await;
    assert!(delivery.await.unwrap().is_err());
    drop(input);
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    window_owner.complete();
    drop(control);
}

#[tokio::test]
async fn supervised_end_drop_waits_for_its_original_exit_error() {
    let mut fixture = InternalFixture::new(12_014).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    fixture.finished().await;
    let (end, delivery) = fixture.end(result_end(0)).await;
    drop(end);
    drop(fixture.activation.source);
    assert_internal_exit_is_pending(fixture.owner.actor(), &fixture.activation.ready).await;
    let error = actual_consumer_body_failure();
    exit.complete(Err(error.clone()));
    assert_internal_exit_preserves_error(fixture.owner.actor(), &fixture.activation.ready, error)
        .await;
    assert!(delivery.await.unwrap().is_err());
}

#[tokio::test]
async fn supervised_end_acknowledgement_loss_waits_for_its_original_exit_error() {
    let mut fixture = InternalFixture::new(12_015).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    fixture.finished().await;
    let (end, delivery) = fixture.end(result_end(0)).await;
    let mut completion = Box::pin(end.complete());
    // Poll once without yielding to the actor: Completed has been sent, while
    // the actor-issued decoded receipt is still waiting for its receiver.
    poll_pending(completion.as_mut()).await;
    drop(completion);
    drop(fixture.activation.source);
    assert_internal_exit_is_pending(fixture.owner.actor(), &fixture.activation.ready).await;
    let error = actual_consumer_body_failure();
    exit.complete(Err(error.clone()));
    assert_internal_exit_preserves_error(fixture.owner.actor(), &fixture.activation.ready, error)
        .await;
    assert!(delivery.await.unwrap().is_err());
}

#[tokio::test]
async fn supervised_end_receiver_closed_before_first_item_preserves_exit_error() {
    let fixture = InternalFixture::new(12_016).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    fixture.finished().await;
    let root = fixture.observer.root;
    fixture
        .observer
        .observe_local_root_end(root, result_end(0))
        .await
        .unwrap();
    let mut delivery = Box::pin(fixture.running.deliver_internal_end(root, result_end(0)));
    // Queue the pump command, then let the consumer exit before its first next().
    // The actor must reserve against the now-closed registered internal channel.
    poll_pending(delivery.as_mut()).await;
    drop(fixture.activation.stream);
    drop(fixture.activation.source);
    tokio::time::timeout(
        Duration::from_secs(1),
        assert_internal_exit_is_pending(fixture.owner.actor(), &fixture.activation.ready),
    )
    .await
    .expect("a closed reservation must not busy-loop or preempt actual exit");
    let error = actual_consumer_body_failure();
    exit.complete(Err(error.clone()));
    assert_internal_exit_preserves_error(fixture.owner.actor(), &fixture.activation.ready, error)
        .await;
    assert!(delivery.await.is_err());
}

#[tokio::test]
async fn supervised_segment_receiver_closed_before_first_item_preserves_exit_error() {
    let fixture = InternalFixture::new(12_017).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    let root = fixture.observer.root;
    let (control, window_owner, capacity, original) = result_segment(root.query_execution_id(), 1);
    let mut response = original.response().clone();
    let novarocks_execution_contract::root_result::RootReadResponse::Reply(reply) = &mut response
    else {
        panic!("the shared result fixture must produce one root reply");
    };
    reply.root_task = root;
    let segment = Arc::new(
        RetainedRootReply::try_new(response, original.retain_physical_guard(), 4096).unwrap(),
    );
    drop(original);
    let mut delivery = Box::pin(fixture.running.deliver_root_segment(
        ResultPacketSequence::new(0),
        segment,
        Some((result_profile(), ClientRowStreamCursor::default())),
        2,
        crate::api::RootRelayResidentWindow::default(),
    ));
    poll_pending(delivery.as_mut()).await;
    drop(fixture.activation.stream);
    drop(fixture.activation.source);
    tokio::time::timeout(
        Duration::from_secs(1),
        assert_internal_exit_is_pending(fixture.owner.actor(), &fixture.activation.ready),
    )
    .await
    .expect("a closed reservation must not busy-loop or preempt actual exit");
    assert_eq!(
        capacity.snapshot().held_positions,
        [1, 0, 0, 0],
        "the deferred pending delivery still owns its original payload"
    );
    let error = actual_consumer_body_failure();
    exit.complete(Err(error.clone()));
    assert_internal_exit_preserves_error(fixture.owner.actor(), &fixture.activation.ready, error)
        .await;
    assert!(delivery.await.is_err());
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    window_owner.complete();
    drop(control);
}

#[tokio::test]
async fn deferred_end_cancellation_settles_delivery_before_actual_exit() {
    let mut fixture = InternalFixture::new(12_018).await;
    let exit = fixture.activation.supervise_exit().await.unwrap();
    let outcome = fixture.owner.actor().internal_outcome().unwrap();
    fixture.finished().await;
    let (end, delivery) = fixture.end(result_end(0)).await;
    drop(end);
    drop(fixture.activation.source);
    assert_internal_exit_is_pending(fixture.owner.actor(), &fixture.activation.ready).await;
    let mut closed = fixture
        .owner
        .actor()
        .request_registry_close_for_join(RegistryClose::Shutdown);
    wait_for_conclusion(fixture.owner.actor(), LogicalConclusion::Cancelled).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), delivery)
            .await
            .expect(
                "accepted cancellation settles deferred delivery without waiting for actual exit"
            )
            .unwrap()
            .unwrap_err(),
        LogicalExecutionActorError::ExecutionConcluded(LogicalConclusion::Cancelled),
    );
    let cancellation = outcome
        .borrow()
        .as_ref()
        .unwrap()
        .as_ref()
        .unwrap_err()
        .clone();
    assert_eq!(cancellation.kind(), QueryExecutionErrorKind::Cancelled);
    assert!(
        !fixture
            .owner
            .actor()
            .registry_join_readiness(false)
            .await
            .unwrap(),
        "actual consumer exit remains independently outstanding after logical cancellation"
    );
    exit.complete(Err(actual_consumer_body_failure()));
    tokio::time::timeout(Duration::from_secs(1), async {
        while !*closed.borrow() {
            closed.changed().await.unwrap();
        }
    })
    .await
    .expect("actual exit allows the cancelled actor to close ingress");
    assert_eq!(
        outcome.borrow().as_ref().unwrap().as_ref().unwrap_err(),
        &cancellation
    );
}

#[tokio::test]
async fn deferred_end_retirement_keeps_execution_outcome_open_until_successor_failure() {
    let first = execution(12_019);
    let port = Arc::new(DelayedQualificationPort::default());
    let (owner, initial, _output) =
        spawn_logical_execution_actor(&Handle::current(), internal_config(first, port.clone()))
            .unwrap()
            .into_parts();
    let outcome = owner.actor().internal_outcome().unwrap();
    let running = Arc::new(owner.actor().activate(initial.ready()).await.unwrap());
    let root = root_task(first, 1);
    let mut old = running.bind_internal_consumer(root).await.unwrap();
    let exit = old.supervise_exit().await.unwrap();
    let observer = running.bind_root_result(root).await.unwrap();
    observer
        .observe_local_root_end(root, result_end(0))
        .await
        .unwrap();
    let sender = Arc::clone(&running);
    let delivery =
        tokio::spawn(async move { sender.deliver_internal_end(root, result_end(0)).await });
    let InternalResultDelivery::End(end) = old.stream.next().await.unwrap().unwrap() else {
        panic!("the old consumer must receive its original End");
    };
    drop(end);
    assert_internal_exit_is_pending(owner.actor(), &old.ready).await;
    // Relinquish only the waiting API future so the attempt permit can move
    // into replacement. The actor still owns the exact deferred End metadata.
    delivery.abort();
    assert!(delivery.await.unwrap_err().is_cancelled());
    let running = Arc::try_unwrap(running).expect("the old pump released its handle");
    let successor = replace_internal_attempt(&owner, running, &port).await;
    assert!(old.ready.has_changed().is_err());
    assert!(outcome.borrow().is_none());
    assert!(!outcome.has_changed().unwrap());
    exit.complete(Err(actual_consumer_body_failure()));
    let next_root = root_task(successor.identity().execution(), 1);
    let next = successor.bind_internal_consumer(next_root).await.unwrap();
    let _next_observer = successor.bind_root_result(next_root).await.unwrap();
    let error = result_protocol_failure("the replacement consumer rejected its original result");
    next.source.fail(error.clone()).await.unwrap();
    wait_for_conclusion(owner.actor(), LogicalConclusion::Failed).await;
    assert_eq!(
        outcome.borrow().as_ref().unwrap().as_ref().unwrap_err(),
        &error
    );
}

fn internal_end_task_failure(
    identity: TaskIdentity,
    phase: novarocks_execution_contract::TaskFailurePhase,
    reason: crate::execution_error::QueryFailureReason,
    detail: &'static str,
) -> QueryExecutionError {
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure, TerminationDetail, failure::FailureProjection,
    };
    let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
        identity,
        TerminationDetail::Failed(TaskFailure::new_in_phase(
            reason.failure_fact(),
            SafeDetail::new(detail).unwrap(),
            phase,
        )),
    );
    QueryExecutionError::from_execution(
        crate::execution_error::ExecutionFailureInput::from_selected_task(&selected).unwrap(),
    )
}

async fn join_original_internal_actor(owner: LogicalExecutionActorOwner) {
    let (actor, join) = owner.into_test_parts();
    drop(actor);
    tokio::time::timeout(Duration::from_secs(5), join)
        .await
        .expect("the original internal actor must actually exit")
        .unwrap();
}

#[tokio::test]
async fn provisional_end_failure_waiter_receives_the_original_frozen_task_input() {
    use crate::execution_error::{ExecutionFailureSite, QueryFailureReason};
    use novarocks_execution_contract::{TaskFailurePhase, failure::FailureProjection};

    let mut fixture = InternalFixture::new(12_401).await;
    // Complete the real actor-delivered End, but never propose Ready.
    let decoded = fixture.decoded_end().await;
    let origin = root_task(fixture.running.identity().execution(), 2);
    let original = internal_end_task_failure(
        origin,
        TaskFailurePhase::Preparation,
        QueryFailureReason::TransportCapacity,
        "the original peer Task exhausted its admitted preparation position",
    );
    let original_input = original.execution_failure().unwrap().clone();
    assert_eq!(
        original_input.site(),
        ExecutionFailureSite::Task {
            identity: origin,
            phase: TaskFailurePhase::Preparation,
        }
    );
    assert_eq!(
        original_input.fact(),
        QueryFailureReason::TransportCapacity.failure_fact()
    );
    assert!(fixture.activation.stream.current_failure().is_none());
    let mut waiting = Box::pin(fixture.activation.stream.failure());
    poll_pending(waiting.as_mut()).await;
    fixture
        .running
        .freeze_result_attempt_failure(original.clone())
        .await
        .unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the retired original stream must wake its pending failure waiter");
    assert_eq!(observed.execution_failure(), Some(&original_input));
    assert_eq!(observed, original);
    assert_eq!(
        fixture.activation.stream.current_failure(),
        Some(original.clone())
    );
    assert!(fixture.activation.ready.borrow().is_none());
    assert!(
        fixture
            .owner
            .actor()
            .internal_outcome()
            .unwrap()
            .borrow()
            .is_none()
    );
    let snapshot = fixture.owner.actor().snapshot().await.unwrap();
    assert_eq!(snapshot.conclusion, None);
    assert!(!snapshot.output_visible);
    // Attempt freezing still does not choose the logical terminal verdict.
    let InternalFixture {
        owner,
        running,
        observer,
        activation,
        _output,
    } = fixture;
    let running = Arc::try_unwrap(running).expect("the original End sender actually exited");
    assert_eq!(
        owner
            .actor()
            .fail_attempt_with_error(running, original)
            .await
            .unwrap(),
        LogicalConclusion::Failed
    );
    drop((decoded, observer, activation, _output));
    join_original_internal_actor(owner).await;
}

#[tokio::test]
async fn replaced_provisional_end_failure_stays_with_its_original_stream() {
    use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason};
    use novarocks_execution_contract::{SafeDetail, TaskFailurePhase, failure::FailureProjection};

    let qualification = Arc::new(DelayedQualificationPort::default());
    let mut fixture = InternalFixture::with_clock_and_qualification(
        12_402,
        Arc::new(ProcessMonotonicClock::new()),
        Arc::clone(&qualification),
    )
    .await;
    let old_decoded = fixture.decoded_end().await;
    let original = QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::BackendLost.failure_fact(),
        SafeDetail::new("the original attempt lost its exact backend before Ready").unwrap(),
        ExecutionFailureSite::Backend {
            process: BackendProcessId::new_v7(),
        },
    ));
    assert_eq!(
        original.execution_failure().unwrap().attempt_class(),
        AttemptFailureClass::RecoverableInfrastructure
    );
    fixture
        .running
        .freeze_result_attempt_failure(original.clone())
        .await
        .unwrap();
    assert_eq!(fixture.activation.stream.failure().await, original);
    let InternalFixture {
        owner,
        running,
        observer: old_observer,
        activation: mut old,
        _output,
    } = fixture;
    let running = Arc::try_unwrap(running).expect("the old original End sender actually exited");
    let successor = replace_internal_attempt(&owner, running, &qualification).await;
    let next_root = root_task(successor.identity().execution(), 1);
    let next_activation = successor.bind_internal_consumer(next_root).await.unwrap();
    let next_observer = successor.bind_root_result(next_root).await.unwrap();
    let mut next = InternalFixture {
        owner,
        running: Arc::new(successor),
        observer: next_observer,
        activation: next_activation,
        _output,
    };
    assert!(old.source.accept(old_decoded).await.is_err());
    let next_decoded = next.decoded_end().await;
    assert!(next.activation.stream.current_failure().is_none());
    assert_eq!(old.stream.current_failure(), Some(original.clone()));
    assert!(
        next.owner
            .actor()
            .internal_outcome()
            .unwrap()
            .borrow()
            .is_none()
    );

    let current = internal_end_task_failure(
        root_task(next.running.identity().execution(), 3),
        TaskFailurePhase::Preparation,
        QueryFailureReason::TransportCapacity,
        "the successor peer Task exhausted its own preparation position",
    );
    assert_ne!(current.execution_failure(), original.execution_failure());
    let mut waiting = Box::pin(next.activation.stream.failure());
    poll_pending(waiting.as_mut()).await;
    next.running
        .freeze_result_attempt_failure(current.clone())
        .await
        .unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the successor stream must receive only its original failure");
    assert_eq!(observed.execution_failure(), current.execution_failure());
    assert_eq!(observed, current);
    assert_eq!(old.stream.failure().await, original);
    assert!(old.ready.borrow().is_none());
    assert!(next.activation.ready.borrow().is_none());
    assert_eq!(
        next.owner.actor().snapshot().await.unwrap().conclusion,
        None
    );

    let InternalFixture {
        owner,
        running,
        observer,
        activation,
        _output,
    } = next;
    let running =
        Arc::try_unwrap(running).expect("the successor original End sender actually exited");
    assert_eq!(
        owner
            .actor()
            .fail_attempt_with_error(running, current)
            .await
            .unwrap(),
        LogicalConclusion::Failed
    );
    drop((
        old_observer,
        old.stream,
        old.ready,
        next_decoded,
        observer,
        activation,
        _output,
    ));
    join_original_internal_actor(owner).await;
}

#[tokio::test]
async fn provisional_end_failure_waiter_receives_the_accepted_governed_cancellation() {
    use crate::execution_error::{ExecutionFailureSite, QueryFailureReason};
    use novarocks_execution_contract::failure::FailureProjection;

    let mut fixture = InternalFixture::new(12_403).await;
    let decoded = fixture.decoded_end().await;
    let mut outcome = fixture.owner.actor().internal_outcome().unwrap();
    let mut waiting = Box::pin(fixture.activation.stream.failure());
    poll_pending(waiting.as_mut()).await;
    fixture
        .owner
        .actor()
        .request_registry_close_for_join(RegistryClose::Shutdown);
    let observed = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("governed cancellation must wake the provisional End waiter");
    let input = observed.execution_failure().unwrap();
    assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
    assert_eq!(input.fact(), QueryFailureReason::Cancelled.failure_fact());
    assert_eq!(
        observed.message(),
        "logical execution was cancelled before success EOF"
    );
    assert_eq!(
        fixture.activation.stream.current_failure(),
        Some(observed.clone())
    );
    assert_eq!(
        fixture
            .activation
            .ready
            .borrow()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err(),
        &observed
    );
    let accepted = tokio::time::timeout(
        Duration::from_secs(5),
        outcome.wait_for(|value| value.is_some()),
    )
    .await
    .expect("the original actor must publish its selected outcome")
    .unwrap();
    assert_eq!(accepted.as_ref().unwrap().as_ref().unwrap_err(), &observed);
    drop(accepted);
    let InternalFixture {
        owner,
        running,
        observer,
        activation,
        _output,
    } = fixture;
    drop((decoded, running, observer, activation, _output, outcome));
    join_original_internal_actor(owner).await;
}

// This waker is installed on the original fail request's oneshot receiver.
// It probes dispatch synchronously inside ActorReply::send, before the actor
// can return to its central pre-watch fallback. It creates no issue authority.
struct InternalFailureReplyProbe {
    permit: Mutex<Option<EstablishIssuePermit>>,
    transport: HoldingEstablishTransport,
    observed: Mutex<Option<oneshot::Sender<Result<EstablishIssueSubmit, EstablishIssueError>>>>,
}

impl std::task::Wake for InternalFailureReplyProbe {
    fn wake(self: Arc<Self>) {
        std::task::Wake::wake_by_ref(&self);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let permit = self.permit.lock().unwrap().take();
        if let Some(permit) = permit {
            let submitted = permit.try_submit(&self.transport);
            if let Some(observed) = self.observed.lock().unwrap().take() {
                let _ = observed.send(submitted);
            }
        }
    }
}

async fn internal_failure_revokes_original_issue_authority(probe_reply: bool) {
    use crate::coordination::{EstablishIssueState, EstablishWorkerSettlement};

    let first = execution(if probe_reply { 12_090 } else { 12_091 });
    let root = root_task(first, 1);
    let frontend = FrontendProcessId::new_v7();
    let root_context = QueryContextRef::new(first, frontend, root.backend_process_id());
    let held_context = QueryContextRef::new(first, frontend, BackendProcessId::new_v7());
    let mut config = internal_config(first, Arc::new(DelayedQualificationPort::default()));
    config.required_establish_contexts = vec![root_context, held_context];
    let (capacity, _) = watch::channel(0);
    config = config.with_abort_query_context_effect_port(
        Arc::new(SelectiveAbortPort {
            blocked: held_context,
            accepted: Arc::new(Mutex::new(Vec::new())),
            capacity,
        }),
        NonZeroUsize::new(2).unwrap(),
    );
    let (owner, initial, output) = spawn_logical_execution_actor(&Handle::current(), config)
        .unwrap()
        .into_parts();
    let actor = owner.actor().clone();
    let mut outcome = actor.internal_outcome().unwrap();
    let running = actor.activate(initial.ready()).await.unwrap();
    let activation = running.bind_internal_consumer(root).await.unwrap();

    // Original actor-issued admission/Establish capabilities: the root settles,
    // while the other context keeps its exact pre-transport permit.
    let mut held = None;
    for (context, tag) in [(root_context, 81), (held_context, 82)] {
        let ticket = admission_ticket(context, tag);
        let admission = running
            .begin_admission_issue(admission_request(context, tag))
            .await
            .unwrap();
        running
            .settle_admission_issue(
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Accepted,
                    ticket,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let permit = running
            .authorize_establish(
                establish_request(context, ticket, tag),
                NativeCompatibilityId::new([tag; 32]),
            )
            .await
            .unwrap();
        if context == held_context {
            held = Some(permit);
        } else {
            let transport = HoldingEstablishTransport::default();
            assert_eq!(
                permit.try_submit(&transport).unwrap(),
                EstablishIssueSubmit::Accepted
            );
            transport
                .take()
                .worker_settled(OperationOutcome::Accepted)
                .unwrap();
        }
    }
    assert_eq!(
        actor
            .establish_snapshot(root_context)
            .await
            .unwrap()
            .unwrap()
            .state(),
        EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Applied)
    );
    assert_eq!(
        actor
            .establish_snapshot(held_context)
            .await
            .unwrap()
            .unwrap()
            .state(),
        EstablishIssueState::IssueAuthorized
    );
    assert!(outcome.borrow().is_none());

    let InternalConsumerActivation {
        source,
        ready,
        stream,
    } = activation;
    let original = actual_consumer_body_failure();
    let mut reply_probe = None;
    let mut submitted_at_reply = None;
    if probe_reply {
        let (observed, response) = oneshot::channel();
        let probe = Arc::new(InternalFailureReplyProbe {
            permit: Mutex::new(held.take()),
            transport: HoldingEstablishTransport::default(),
            observed: Mutex::new(Some(observed)),
        });
        let waker = std::task::Waker::from(Arc::clone(&probe));
        let mut cx = std::task::Context::from_waker(&waker);
        // The current-thread fixture cannot run the actor during this poll.
        // Its mailbox is empty after the preceding snapshot reply. Disabling
        // cooperative yield leaves only the original oneshot reply pending;
        // no scheduler-budget wake can masquerade as ActorReply::send.
        let mut failure = Box::pin(tokio::task::unconstrained(source.fail(original.clone())));
        assert!(failure.as_mut().poll(&mut cx).is_pending());
        submitted_at_reply = Some(
            tokio::time::timeout(Duration::from_secs(5), response)
                .await
                .expect("the actual fail reply must wake its original receiver")
                .unwrap(),
        );
        failure.await.unwrap();
        reply_probe = Some(probe);
    } else {
        source.fail(original.clone()).await.unwrap();
    }
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = outcome.borrow().clone();
            if let Some(verdict) = current {
                break verdict;
            }
            outcome.changed().await.unwrap();
        }
    })
    .await
    .expect("the original actor must publish its statement verdict");
    let watch_transport = HoldingEstablishTransport::default();
    let (submitted, transport) = if let Some(probe) = reply_probe.as_ref() {
        (submitted_at_reply.take().unwrap(), &probe.transport)
    } else {
        (
            held.take().unwrap().try_submit(&watch_transport),
            &watch_transport,
        )
    };
    let reached_transport = transport.submission.lock().unwrap().is_some();
    // On the old source this can be Accepted. Settle that exact controlled
    // submission before asserting, keeping the original actor join observable.
    let late_settlement = if reached_transport {
        Some(transport.take().worker_settled(OperationOutcome::Accepted))
    } else {
        None
    };
    drop((running, ready, stream, output, outcome));
    let supervisor = owner.into_residual_stand_down_supervisor();
    // These separate Worker/context stop facts come from the existing fixture
    // author. Neither the ACK nor the statement watch is an actual-exit proof.
    supervisor
        .observe_worker_stopped_and_context_fenced(root_context)
        .await
        .unwrap();
    supervisor
        .observe_worker_stopped_and_context_fenced(held_context)
        .await
        .unwrap();
    drop(actor);
    let ResidualStandDownSupervisor { actor, mut join } = supervisor;
    drop(actor);
    let joined = tokio::time::timeout(Duration::from_secs(5), &mut join).await;
    if !matches!(joined, Ok(Ok(()))) {
        std::panic::panic_any((join, joined));
    }
    assert_eq!(observed.unwrap_err(), original);
    assert!(late_settlement.is_none_or(|settled| settled.is_ok()));
    assert!(matches!(
        submitted,
        Err(EstablishIssueError::IssueAuthorityRevoked)
    ));
    assert!(
        !reached_transport,
        "a terminal internal cut left Establish authority open"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn internal_failure_ack_revokes_the_other_contexts_pre_transport_permit() {
    internal_failure_revokes_original_issue_authority(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn internal_failure_watch_revokes_the_other_contexts_pre_transport_permit() {
    internal_failure_revokes_original_issue_authority(false).await;
}

struct AttemptConsumerCause(Arc<AtomicUsize>);
impl Drop for AttemptConsumerCause {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct HeldAttemptConsumer {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    gate: Mutex<Option<oneshot::Receiver<()>>>,
    drops: Arc<AtomicUsize>,
}
impl crate::api::InternalComputationConsumer for HeldAttemptConsumer {
    fn begin_local_close(&self) {}
    fn poll_local_closed(
        &self,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
        // This actor fixture never issues a CPU job.
        Poll::Ready(Ok(()))
    }
    fn consume(
        &self,
        activation: InternalConsumerActivation,
    ) -> crate::api::InternalConsumerFuture {
        let entered = self.entered.lock().unwrap().take().unwrap();
        let gate = self.gate.lock().unwrap().take().unwrap();
        let drops = Arc::clone(&self.drops);
        Box::pin(async move {
            let _original = activation;
            let _ = entered.send(());
            let _ = gate.await;
            std::panic::panic_any(AttemptConsumerCause(drops));
        })
    }
}

#[tokio::test]
async fn original_old_and_successor_consumers_keep_distinct_causes_after_their_real_joins() {
    use crate::coordination::native_active_custody::{
        OriginalConsumerStart, root_ack_successor_fixture, root_ack_test_fixture,
    };
    let first = execution(12_599);
    let port = Arc::new(DelayedQualificationPort::default());
    let (owner, initial, _output) =
        spawn_logical_execution_actor(&Handle::current(), internal_config(first, port.clone()))
            .unwrap()
            .into_parts();
    let running = Arc::new(owner.actor().activate(initial.ready()).await.unwrap());
    let root = root_task(first, 1);
    let mut old = running.bind_internal_consumer(root).await.unwrap();
    let old_exit = old.supervise_exit().await.unwrap();
    let observer = running.bind_root_result(root).await.unwrap();
    observer
        .observe_local_root_end(root, result_end(0))
        .await
        .unwrap();
    let sender = Arc::clone(&running);
    let delivery =
        tokio::spawn(async move { sender.deliver_internal_end(root, result_end(0)).await });
    let InternalResultDelivery::End(end) = old.stream.next().await.unwrap().unwrap() else {
        panic!("original End")
    };
    drop(end);
    assert_internal_exit_is_pending(owner.actor(), &old.ready).await;
    delivery.abort();
    assert!(delivery.await.unwrap_err().is_cancelled());
    let (old_record, old_ack) = root_ack_test_fixture(running.identity(), root);
    let (old_release, old_gate) = oneshot::channel();
    let (old_entered, old_entry) = oneshot::channel();
    let old_drops = Arc::new(AtomicUsize::new(0));
    let mut old_input = Some(OriginalConsumerStart {
        consumer: Arc::new(HeldAttemptConsumer {
            entered: Mutex::new(Some(old_entered)),
            gate: Mutex::new(Some(old_gate)),
            drops: Arc::clone(&old_drops),
        }),
        activation: old,
        exit: old_exit,
        activity: None,
    });
    old_record.start_consumer_in(&mut old_input);
    old_entry.await.unwrap();
    let running = Arc::try_unwrap(running).expect("the real old delivery joined");
    let successor = replace_internal_attempt(&owner, running, &port).await;
    let next_root = root_task(successor.identity().execution(), 1);
    let next = successor.bind_internal_consumer(next_root).await.unwrap();
    let next_exit = next.supervise_exit().await.unwrap();
    let _next_observer = successor.bind_root_result(next_root).await.unwrap();
    let (next_record, next_ack) =
        root_ack_successor_fixture(&old_record, successor.identity(), next_root);
    let (next_release, next_gate) = oneshot::channel();
    let (next_entered, next_entry) = oneshot::channel();
    let next_drops = Arc::new(AtomicUsize::new(0));
    let mut next_input = Some(OriginalConsumerStart {
        consumer: Arc::new(HeldAttemptConsumer {
            entered: Mutex::new(Some(next_entered)),
            gate: Mutex::new(Some(next_gate)),
            drops: Arc::clone(&next_drops),
        }),
        activation: next,
        exit: next_exit,
        activity: None,
    });
    next_record.start_consumer_in(&mut next_input);
    next_entry.await.unwrap();
    // These metadata-only fixture slots never issued a Native source or ACK.
    drop((old_ack, next_ack));
    old_record.close_native();
    next_record.close_native();
    let both_pending = old_record.consumer_pending() && next_record.consumer_pending();
    let both_held = old_record.retained() && next_record.retained();
    let _ = old_release.send(());
    old_record.join_consumer().await;
    let old_joined_before_successor =
        !old_record.consumer_pending() && next_record.consumer_pending();
    let _ = next_release.send(());
    next_record.join_consumer().await;
    wait_for_conclusion(owner.actor(), LogicalConclusion::Failed).await;
    drop((successor, observer, _next_observer, _output));
    join_original_internal_actor(owner).await;
    assert!(both_pending && both_held && old_joined_before_successor);
    assert!(old_record.consumer_failed() && next_record.consumer_failed());
    assert!(!old_record.live_registration() && !next_record.live_registration());
    assert!(
        old_record.retained() && next_record.retained(),
        "only the original finite health headers remain"
    );
    assert_eq!(old_drops.load(Ordering::SeqCst), 0);
    assert_eq!(next_drops.load(Ordering::SeqCst), 0);
    old_record.dispose_for_process_exit();
    drop(next_record);
    assert_eq!(old_drops.load(Ordering::SeqCst), 1);
    assert_eq!(next_drops.load(Ordering::SeqCst), 1);
}

#[derive(Debug)]
struct ExitTailPanicConsumer(Arc<AtomicUsize>);
impl Drop for ExitTailPanicConsumer {
    fn drop(&mut self) {
        std::panic::panic_any(AttemptConsumerCause(Arc::clone(&self.0)));
    }
}
impl crate::api::InternalComputationConsumer for ExitTailPanicConsumer {
    fn begin_local_close(&self) {}
    fn poll_local_closed(
        &self,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
        Poll::Ready(Ok(()))
    }
    fn consume(
        &self,
        activation: InternalConsumerActivation,
    ) -> crate::api::InternalConsumerFuture {
        Box::pin(async move {
            drop(activation);
            Ok(())
        })
    }
}

#[tokio::test]
async fn actual_consumer_join_keeps_the_opaque_receipt_tail_cause_in_its_original_attempt() {
    use crate::coordination::native_active_custody::{
        OriginalConsumerStart, root_ack_test_fixture,
    };
    let InternalFixture {
        owner,
        running,
        observer,
        activation,
        _output,
    } = InternalFixture::new(12_600).await;
    let exit = activation.supervise_exit().await.unwrap();
    let (record, ack) = root_ack_test_fixture(running.identity(), observer.root);
    let drops = Arc::new(AtomicUsize::new(0));
    let mut input = Some(OriginalConsumerStart {
        consumer: Arc::new(ExitTailPanicConsumer(Arc::clone(&drops))),
        activation,
        exit,
        activity: None,
    });
    record.start_consumer_in(&mut input);
    drop(ack);
    record.close_native();
    record.join_consumer().await;
    wait_for_conclusion(owner.actor(), LogicalConclusion::Failed).await;
    drop((running, observer, _output));
    join_original_internal_actor(owner).await;
    assert!(record.consumer_join_panicked());
    assert!(record.consumer_failed());
    assert!(!record.live_registration());
    assert!(record.retained());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    record.dispose_for_process_exit();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn owned_consumer_cleanup_releases_p1_but_keeps_opaque_join_cause_until_process_exit() {
    use crate::admitted_query_context::QueryResultCapacityBinding;
    use crate::coordination::native_active_custody::{
        OriginalConsumerStart, root_ack_test_fixture,
    };
    use novarocks_workload_control::{
        ControlStorageConfig, ResultCapacityConfig, WorkClass, WorkOwner, WorkRequest,
        WorkloadConfig, WorkloadControl,
        joint_admission::{
            InternalComputationGrant, JointLane, JointProgress, JointProgressDriver, JointResource,
            JointResources, ResponsibilityConfig, RetainedPayloadGrant,
        },
    };
    // Drive the real admission author, not a mirror selector or synthetic grant.
    fn resolve<F: Future + Unpin>(driver: &mut JointProgressDriver, future: &mut F) -> F::Output {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        loop {
            if let Poll::Ready(value) = std::pin::Pin::new(&mut *future).poll(&mut cx) {
                return value;
            }
            if matches!(driver.step(), JointProgress::Idle(_)) {
                if let Poll::Ready(value) = std::pin::Pin::new(&mut *future).poll(&mut cx) {
                    return value;
                }
                panic!("original finite admission unexpectedly remained pending at idle");
            }
        }
    }
    fn child(
        driver: &mut JointProgressDriver,
        root: &novarocks_workload_control::WorkScope,
    ) -> WorkOwner {
        let mut request = root.request_joint_child().unwrap();
        let mut ready = resolve(driver, &mut request).unwrap();
        let mut child = None;
        ready
            .adjustment
            .as_mut()
            .unwrap()
            .install_child_in(root, &mut child)
            .unwrap()
            .notify();
        child.unwrap()
    }
    let count = 64;
    let workload = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let mut config = ResultCapacityConfig::V1;
    config.positions[2] = 1;
    let result_capacity = workload.configure_result_capacity(config).unwrap();
    let (admission, mut driver) = workload
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    workload
        .configure_control_storage(ControlStorageConfig::new(count, &[]))
        .unwrap();
    workload.mark_ready().unwrap();
    let root = workload
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    let mut first_bind = admission.begin_paid_proposal(&root.owner).unwrap();
    first_bind
        .begin_first_bind(root.owner.control_claim_view().unwrap())
        .unwrap();
    first_bind
        .push_dependency(&novarocks_resource_contract::SharedDependency::LocalHost)
        .unwrap();
    first_bind.end_change().unwrap();
    let mut first_request = first_bind.submit().unwrap();
    let mut first_ready = resolve(&mut driver, &mut first_request).unwrap();
    first_ready
        .adjustment
        .take()
        .unwrap()
        .install_control(&root.owner, &[])
        .unwrap();
    drop((first_ready, first_request));
    let mut builder = admission.begin_paid_proposal(&root.owner).unwrap();
    builder
        .begin_new(
            JointLane::Custody,
            JointResources::ZERO
                .with(JointResource::ClaimRecords, 1)
                .with(JointResource::PayloadBytes, 8 * 1024 * 1024),
        )
        .unwrap();
    builder
        .push_dependency(&novarocks_resource_contract::SharedDependency::LocalHost)
        .unwrap();
    builder.end_change().unwrap();
    let mut request = builder.submit().unwrap();
    let mut ready = resolve(&mut driver, &mut request).unwrap();
    let mut raw = ready.new_claims.pop();
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&scope, &mut raw, &mut payload)
        .unwrap()
        .notify();
    drop((ready, request));
    let payload = payload.unwrap();
    let stage = child(&mut driver, &scope);
    let next_stage = child(&mut driver, &scope);
    let source = payload.root_stage_source(&scope, &stage).unwrap();
    let mut builder = source
        .begin(8 * 1024 * 1024, JointLane::Foreground)
        .unwrap();
    builder.end_payload().unwrap();
    builder.end_stage().unwrap();
    let mut stage_request = builder.submit().unwrap();
    let mut ready = resolve(&mut driver, &mut stage_request).unwrap();
    let mut compute: Option<InternalComputationGrant> = None;
    ready
        .install_in(&scope, &payload, &stage, &mut compute)
        .unwrap()
        .notify();
    drop((ready, stage_request));
    let mut capacity = None;
    QueryResultCapacityBinding::from_owned_internal_computation_in(
        &scope,
        &stage,
        &payload,
        &mut compute,
        &mut capacity,
    )
    .unwrap();
    assert!(compute.is_none());
    let capacity = capacity.unwrap();
    let cut = capacity.streamed_computation_cut_handle().unwrap();
    let activity = cut.begin_consumer_activity().unwrap();
    let InternalFixture {
        owner,
        running,
        observer,
        activation,
        _output,
    } = InternalFixture::new(12_601).await;
    let exit = activation.supervise_exit().await.unwrap();
    let (record, ack) = root_ack_test_fixture(running.identity(), observer.root);
    let drops = Arc::new(AtomicUsize::new(0));
    let mut input = Some(OriginalConsumerStart {
        consumer: Arc::new(ExitTailPanicConsumer(Arc::clone(&drops))),
        activation,
        exit,
        activity: Some(activity),
    });
    // Before the consumer task can receive its first poll, a real second
    // request must wait for the original P=1 computation.
    let source = payload.root_stage_source(&scope, &next_stage).unwrap();
    let mut builder = source
        .begin(8 * 1024 * 1024, JointLane::Foreground)
        .unwrap();
    builder.end_payload().unwrap();
    builder.end_stage().unwrap();
    let mut next = builder.submit().unwrap();
    while matches!(driver.step(), JointProgress::More) {}
    assert!(
        std::pin::Pin::new(&mut next)
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    record.start_consumer_in(&mut input);
    assert!(input.is_none());
    // No Native producer was launched in this component fixture. Remove the
    // unused creator views: the actual consumer slot now owns the sole activity.
    // This does not fabricate a pump token or call native_body_exited.
    drop((capacity, cut));
    drop(ack);
    record.close_native();
    record.join_consumer().await;
    wait_for_conclusion(owner.actor(), LogicalConclusion::Failed).await;
    drop((running, observer, _output));
    join_original_internal_actor(owner).await;
    assert!(record.consumer_join_panicked());
    assert!(record.consumer_failed());
    assert!(record.retained());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    // The original F/Join finished and its cleanup cut is stable. No Native
    // producer was issued here; this proves only this component's local cut.
    driver.recheck_retired_backing();
    let mut ready = resolve(&mut driver, &mut next).unwrap();
    let mut next_compute = None;
    ready
        .install_in(&scope, &payload, &next_stage, &mut next_compute)
        .unwrap()
        .notify();
    assert_eq!(
        result_capacity.snapshot().held_positions[2],
        1,
        "the same waiting P=1 request reuses the position before raw cause disposal"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(record.consumer_failed() && record.retained());
    record.dispose_for_process_exit();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop((ready, next, next_compute, payload));
    stage.complete();
    next_stage.complete();
    root.business.release();
    root.owner.complete();
    drop(scope);
    workload.close_admission();
    driver.recheck_retired_backing();
    while matches!(driver.step(), JointProgress::More) {}
    assert_eq!(result_capacity.snapshot().held_positions, [0; 4]);
    drop((result_capacity, driver, admission));
    workload
        .shutdown()
        .unwrap_or_else(|failure| panic!("actual fixture shutdown failed: {:?}", failure.error()));
}
