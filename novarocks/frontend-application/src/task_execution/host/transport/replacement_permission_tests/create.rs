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
use novarocks_execution_contract::{TaskIdentity, TaskOperationId};
use novarocks_query_application::execution_error::ExecutionFailureInput;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreateCase {
    Replacement,
    NoRecovery,
    Expiry,
}

#[tokio::test]
async fn pipelined_create_refusal_keeps_its_original_qa_permission_and_ticket_horizon() {
    for case in [
        CreateCase::Replacement,
        CreateCase::NoRecovery,
        CreateCase::Expiry,
    ] {
        tokio::time::timeout(Duration::from_secs(3), rejected_create(case))
            .await
            .unwrap_or_else(|_| panic!("original Create refusal did not finish: {case:?}"));
    }
}

async fn rejected_create(case: CreateCase) {
    let mut start = start();
    let task = start.plan.graph.root;
    let participation = *start.plan.establish_material.keys().next().unwrap();
    let execution = participation.query_execution_id();
    let compatibility = start.plan.compatibility;
    let facts = AttemptEstablishFacts::freeze(
        Default::default(),
        [(participation.backend_process_id(), Default::default())],
        Default::default(),
        &QueryCredentialLeases::empty(),
    )
    .unwrap();
    let mut payload = id(500);
    let (quotes, material) =
        AttemptEstablishMaterial::project(&facts, [participation], &mut payload).unwrap();
    start.plan.establish_material = quotes;

    let workload = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1 << 12,
            per_scope_bytes: 1 << 18,
        },
    )
    .unwrap();
    workload.mark_ready().unwrap();
    let work = workload
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = work.owner.scope();
    let stage = scope.try_acquire(Stage::Execution).unwrap();
    let roster = vec![participation];
    let two = NonZeroUsize::new(2).unwrap();
    let config = if case == CreateCase::NoRecovery {
        LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            ExecutionEffect::None,
            two,
            roster.clone(),
            two,
            two,
            work.owner,
            stage,
        )
    } else {
        let profile = ClientRowProfile::try_new(
            RootProfileV1::SEGMENT_BYTES,
            RootProfileV1::ROW_PAYLOAD_BYTES,
        )
        .unwrap();
        LogicalExecutionActorConfig::read_only_pre_visibility_recovery_rows(
            execution,
            two,
            roster.clone(),
            two,
            two,
            NonZeroU32::new(2).unwrap(),
            Arc::new(NoSuccessor(tokio::sync::watch::channel(0).0)),
            work.owner,
            stage,
            Duration::from_secs(30),
            ResultSchema::new(Vec::new()),
            NonZeroUsize::MIN,
            ResultRowCarrier::relayed(RootOutputKind::ClientRows, Some(profile)).unwrap(),
        )
    }
    .unwrap()
    .with_abort_participation_effect_port(LogicalAbortRouter::new(), two);
    let mut logical = LogicalExecutionTestHarness::install(
        tokio::runtime::Handle::current(),
        config,
        execution,
        roster,
    )
    .unwrap();
    let activation = logical.activate_initial().await.unwrap();
    let drive = Arc::new(logical.native_attempt_drive());
    start.activation.identity = activation;
    start.plan.authorization_limits = drive.authorization_limits();
    let bridge = AttemptQaBridge::new(drive, compatibility, Arc::new(material));
    let clock = Arc::new(SampleClock::default());
    let mut cut = Cut::new(start, clock.clone(), bridge);

    let endpoint = RuntimeEndpoint::new("127.0.0.1", 19091).unwrap();
    let probe = Arc::new(ProcessProbe {
        original: participation,
        endpoint: endpoint.clone(),
        replacement: BackendProcessId::new_v7(),
        replaced: AtomicBool::new(false),
        reads: AtomicU64::new(0),
    });
    let observer = AttemptProcessObserver::new(
        probe.clone(),
        Arc::from([FrozenMember {
            participation,
            endpoint,
        }]),
        clock.clone(),
    );
    let outbox = TerminalOutbox::new(activation);
    let Refusal {
        ticket,
        establish,
        create,
        port,
        original,
        expected,
    } = create_refusal(&mut cut, task, participation).await;
    assert_ne!(establish, create);
    let ticket_duration = ticket.valid_for().get();
    assert_eq!(ticket_duration, Duration::from_secs(10));

    loop {
        if let Some(snapshot) = logical.establish_snapshot(participation).await.unwrap()
            && snapshot.state() == EstablishIssueState::TransportUnknown
        {
            let exact = snapshot.identity().unwrap();
            assert_eq!(exact.activation(), activation);
            assert_eq!(exact.participation(), participation);
            assert_eq!(exact.operation_id(), establish);
            assert_eq!(snapshot.ticket(), ticket);
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(logical.actor_snapshot().await.unwrap().conclusion, None);

    let may_observe = case != CreateCase::NoRecovery;
    if !may_observe {
        assert!(
            !logical
                .native_attempt_drive()
                .may_observe_pre_ready_replacement(&expected)
                .await
                .unwrap()
        );
    }
    let permission = cut.permission(port, may_observe).await;
    let mut waiting_reads = 0;
    let effect = if may_observe {
        let (mut last, mut waiting) = sample(&mut cut, permission, &observer, participation);
        assert!(waiting.effect.is_none());
        assert_eq!(probe.reads.load(Ordering::SeqCst), 1);
        // The failed ACK prefix leaves the exact Create payload for the next
        // round's cleanup. That round can recheck the same refusal without
        // moving the clock or changing its original ticket horizon.
        for _ in 0..8 {
            if cut.core.next_deadline() != Some(clock.instant()) {
                break;
            }
            let Some(effect) = cut.advance(waiting).await else {
                break;
            };
            let query = exact_permission_query(effect, activation, &original);
            let permission = cut.permission(query, true).await;
            let (next, sampled) = sample(&mut cut, permission, &observer, participation);
            assert_ne!(last, next);
            last = next;
            waiting = sampled;
            assert!(waiting.effect.is_none());
        }
        waiting_reads = probe.reads.load(Ordering::SeqCst);
        let horizon = MonotonicInstant::ORIGIN.saturating_add(ticket_duration);
        assert_eq!(cut.core.next_deadline(), Some(horizon));
        {
            let mut terminal = Box::pin(outbox.terminal());
            assert!(futures::poll!(terminal.as_mut()).is_pending());
        }

        if case == CreateCase::Replacement {
            clock.set_seconds(4);
            probe.replaced.store(true, Ordering::SeqCst);
            let wake = cut.step(
                cut.sources.membership,
                AttemptInput::Membership(MembershipView {
                    revision: 1,
                    processes: [(
                        participation,
                        MembershipObservation::ExactReplacement(probe.replacement),
                    )]
                    .into(),
                }),
            );
            let effect = cut.advance(wake).await.unwrap();
            let query = exact_permission_query(effect, activation, &original);
            let permission = cut.permission(query, true).await;
            let (second, sampled) = sample(&mut cut, permission, &observer, participation);
            assert_ne!(last, second);
            cut.advance(sampled).await.unwrap()
        } else {
            clock.set_seconds(9);
            assert!(clock.instant() < horizon);
            assert_eq!(cut.core.next_deadline(), Some(horizon));
            assert_eq!(probe.reads.load(Ordering::SeqCst), waiting_reads);

            // Exact replacement becomes available when the original ticket
            // closes. A fresh policy answer cannot authorize a late sample.
            clock.set_seconds(10);
            assert_eq!(clock.instant(), horizon);
            probe.replaced.store(true, Ordering::SeqCst);
            let timer = cut.actor(AttemptInput::Timer);
            let effect = cut.advance(timer).await.unwrap();
            if matches!(&effect.body, EffectBody::Port(port)
                if matches!(&port.body, AttemptToLogicalBody::PreReadyReplacementQuery { .. }))
            {
                let query = exact_permission_query(effect, activation, &original);
                let permission = cut.permission(query, true).await;
                cut.advance(permission).await.unwrap()
            } else {
                effect
            }
        }
    } else {
        cut.advance(permission).await.unwrap()
    };
    let terminal = terminal_port(effect);
    assert_eq!(terminal.activation, activation);
    let publication = TerminalPublication::new(terminal);
    assert!(publication.host_reference().is_none());
    assert!(outbox.publish(Box::new(publication)).is_ok());
    let NativeAttemptTerminal::Failed(failure) = outbox.terminal().await.unwrap() else {
        panic!("original Create refusal");
    };
    assert_eq!(failure.input(), &expected);
    assert_eq!(
        failure.input().site(),
        ExecutionFailureSite::Operation {
            participation,
            operation_id: create,
            kind: OperationKind::CreateTask,
        }
    );
    assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
    assert_eq!(
        failure.input().detail().as_str(),
        "original Create process mismatch"
    );
    match case {
        CreateCase::Replacement => {
            let witness = failure.pre_ready_replacement().unwrap();
            assert_eq!(witness.participation(), participation);
            assert_eq!(witness.operation_id(), create);
            assert_eq!(witness.replacement_process(), probe.replacement);
            assert_eq!(probe.reads.load(Ordering::SeqCst), waiting_reads + 1);
        }
        CreateCase::NoRecovery => {
            assert!(failure.pre_ready_replacement().is_none());
            assert_eq!(probe.reads.load(Ordering::SeqCst), 0);
        }
        CreateCase::Expiry => {
            assert!(failure.pre_ready_replacement().is_none());
            assert_eq!(probe.reads.load(Ordering::SeqCst), waiting_reads);
        }
    }
    assert_eq!(
        outbox.terminal().await.unwrap(),
        NativeAttemptTerminal::Failed(failure)
    );

    drop(cut);
    logical.abandon_running_attempt();
    while logical
        .stand_down_snapshot(participation)
        .await
        .unwrap()
        .is_none()
    {
        tokio::task::yield_now().await;
    }
    logical
        .observe_worker_process_replaced(participation)
        .await
        .unwrap();
    work.business.release();
    logical
        .finish_until(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    scope.wait_released().await;
    workload.close_admission();
    assert!(workload.shutdown().is_ok());
}

fn exact_permission_query(
    effect: AttemptEffect,
    activation: AttemptActivationIdentity,
    original: &AttemptFailure,
) -> Port {
    let EffectBody::Port(port) = effect.body else {
        panic!("the original Core must ask QA about this Create");
    };
    assert_eq!(port.activation, activation);
    assert!(
        matches!(&port.body, AttemptToLogicalBody::PreReadyReplacementQuery { refusal, .. } if refusal == original)
    );
    port
}

struct Refusal {
    ticket: ParticipationAdmissionTicketReceipt,
    establish: TaskOperationId,
    create: TaskOperationId,
    port: Port,
    original: AttemptFailure,
    expected: ExecutionFailureInput,
}

async fn create_refusal(
    cut: &mut Cut,
    task: TaskIdentity,
    participation: ParticipationRef,
) -> Refusal {
    let begun = cut.actor(AttemptInput::StartProtocol);
    let admitted = cut.advance(begun).await.unwrap();
    let (operation, generation, sent) = cut.send(admitted).await;
    let OperationIntent::AcquireParticipationAdmissionTicket(admission) = operation else {
        panic!("the original admission must precede Establish");
    };
    let started = cut.native(ExecutorFact::SendStarted {
        effect: sent,
        started_at: cut.clock.instant(),
    });
    assert!(cut.advance(started).await.is_none());
    let ticket = ParticipationAdmissionTicketReceipt::new(
        AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
        participation,
        admission.valid_for(),
    );
    let admission_id = admission.envelope().operation_id();
    let admitted = OperationAcknowledgement::worker_receipt(
        admission_id,
        OperationKind::AcquireParticipationAdmissionTicket,
        OperationOutcome::Accepted,
        AckPayload::AdmissionTicket(ticket),
    )
    .attempt_observation(admission_id, generation, None, None)
    .unwrap();
    let got_ticket = cut.native(ExecutorFact::Operation(admitted));
    let establish = cut.advance(got_ticket).await.unwrap();
    let (operation, establish_generation, establish_send) = cut.send(establish).await;
    let establish_id = operation.operation_id();
    assert!(
        matches!(operation, OperationIntent::EstablishParticipation { participation: exact, ticket: original, .. }
        if exact == participation && original == ticket.ticket_id())
    );

    let started = cut.native(ExecutorFact::EstablishRpcStarted {
        effect: establish_send,
        operation: establish_id,
        participation,
        started_at: cut.clock.instant(),
    });
    let create = cut
        .advance(started)
        .await
        .expect("the exact RPC start permits the original pipelined Create");
    let (create, create_generation, create_send) = cut.send(create).await;
    assert!(matches!(create, OperationIntent::CreateTask { identity, .. } if identity == task));
    let create_id = create.operation_id();
    let started = cut.native(ExecutorFact::SendStarted {
        effect: create_send,
        started_at: cut.clock.instant(),
    });
    assert!(cut.advance(started).await.is_none());

    cut.clock.set_seconds(3);
    let unknown = OperationAcknowledgement::transport_unknown(
        establish_id,
        OperationKind::UpdateParticipation,
    )
    .attempt_observation(establish_id, establish_generation, None, None)
    .unwrap();
    let raw = OperationAcknowledgement::worker_receipt(
        create_id,
        OperationKind::CreateTask,
        OperationOutcome::IdentityMismatch,
        AckPayload::None,
    )
    .with_detail(Some(SafeDetail::truncating(
        "original Create process mismatch",
    )));
    let rejection = raw.worker_header().unwrap();
    let original = AttemptFailure::TaskProtocol(Box::new(TaskExecutionError::OperationFailed {
        task: Some(task),
        participation,
        operation_id: create_id,
        kind: OperationKind::CreateTask,
        rejection: rejection.clone(),
    }));
    let expected = crate::task_execution::error::TaskExecutionError::OperationFailed {
        participation,
        operation_id: create_id,
        kind: OperationKind::CreateTask,
        rejection,
    }
    .preparation_input();
    let refused = raw
        .attempt_observation(create_id, create_generation, None, None)
        .unwrap();

    // Both replies belong to the selected original Native FIFO prefix. The
    // observer debt must settle the first Unknown in QA before permission.
    let held = cut.native_before_queued_reply(ExecutorFact::Operation(unknown));
    assert!(held.effect.is_none() && held.round_result.is_none());
    assert_eq!(held.selection, InputSelection::Only(cut.sources.native));
    let decided = cut.native(ExecutorFact::Operation(refused));
    let query = cut.advance(decided).await.unwrap();
    assert_eq!(cut.round_failure, Some(original.clone()));
    let port = exact_permission_query(query, cut.qa.activation(), &original);
    Refusal {
        ticket,
        establish: establish_id,
        create: create_id,
        port,
        original,
        expected,
    }
}
