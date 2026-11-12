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
use crate::native::task_result_transport::{DynamicFilterRead, DynamicFilterReadError};
use novarocks_control_runtime::{
    actor::{Actor, ActorConfig, ActorStep},
    actor_turn::{ActorRegistration, ActorRuntime},
    causal_input::CausalInputObligation,
    completion::ReportSlot,
    exit_custody::ExitCustodyPhase,
    exit_work::{PanicSink, run_future},
    obligation::{ReservationObligation, WorkObligation},
    reported_work::ReportedWork,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::{ActorId, ConsumerId, Generation, ScopeId, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{
    DomainVersion, OperatorCounter, OperatorStatistics, SafeDetail, TaskOutputFacts, TaskState,
    TaskStatus, TaskStatusVersion,
};
use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId};
use std::{
    collections::VecDeque,
    num::NonZeroU64,
    sync::atomic::{AtomicUsize, Ordering},
};

struct OriginalTransport {
    reads: Mutex<VecDeque<(TaskIdentity, Box<FinalTaskInfo>)>>,
}
impl TaskResultTransport for OriginalTransport {
    fn final_task_info(
        &self,
        requested: TaskIdentity,
    ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
        Box::pin(async move {
            let (expected, original) = self.reads.lock().unwrap().pop_front().unwrap();
            assert_eq!(requested, expected);
            Ok(FinalTaskInfoRead::Available(original))
        })
    }
    fn dynamic_filters(
        &self,
        _: TaskIdentity,
        _: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
        unreachable!("final-info tests never read filters")
    }
}
struct ReadRetention(Arc<AtomicUsize>);
impl Drop for ReadRetention {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct Recorder(Arc<Mutex<Vec<FinalTaskInfoProjection>>>);
impl Actor for Recorder {
    type Input = ExecutorFact;
    type Effect = ();
    fn timer_input(_: Tick) -> ExecutorFact {
        unreachable!("no timer")
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {
        unreachable!("no owner failure")
    }
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        value: ExecutorFact,
        _: &mut ActorStep<'_, ()>,
    ) {
        let ExecutorFact::DiagnosticsFinished {
            result: DiagnosticRead::Available(projection),
            ..
        } = value
        else {
            unreachable!("only actual available diagnostic results")
        };
        self.0.lock().unwrap().push(projection);
    }
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn task(id: u32) -> TaskIdentity {
    TaskIdentity::new(
        QueryExecutionId::new(QueryId::new(211, 9), AttemptId::new(1).unwrap()).unwrap(),
        StageId::new(1).unwrap(),
        TaskId::new(id).unwrap(),
        BackendProcessId::try_from_bytes([1, 1, 1, 1, 1, 1, 0x71, 1, 0x81, 1, 1, 1, 1, 1, 1, 1])
            .unwrap(),
    )
}
fn info(task: TaskIdentity) -> Box<FinalTaskInfo> {
    Box::new(
        FinalTaskInfo::try_new(
            task,
            TaskStatus::try_new(
                task,
                TaskStatusVersion::new(1).unwrap(),
                TaskState::Finished,
                None,
                TaskOutputFacts::new(true),
            )
            .unwrap(),
            Vec::new(),
            false,
        )
        .unwrap(),
    )
}
fn large_information(task: TaskIdentity) -> Box<FinalTaskInfo> {
    let operators = (0..4096)
        .map(|operator| {
            OperatorStatistics::new(operator, SafeDetail::new("original-scan").unwrap())
                .with_counters(
                    (0..64)
                        .map(|counter| {
                            OperatorCounter::new(
                                SafeDetail::new("original-counter").unwrap(),
                                counter,
                            )
                        })
                        .collect(),
                )
        })
        .collect();
    Box::new(
        FinalTaskInfo::try_new(task, info(task).final_status().clone(), operators, false).unwrap(),
    )
}
fn request(id: u64) -> AttemptRequestToken {
    AttemptRequestToken::new(NonZeroU64::new(id).unwrap())
}

struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    runtime: ActorRuntime<Recorder>,
    registration: ActorRegistration,
    source_work: WorkObligation,
    source_capacity: ReservationObligation,
    records: Arc<DiagnosticRecords>,
    seen: Arc<Mutex<Vec<FinalTaskInfoProjection>>>,
    retired: Arc<AtomicUsize>,
}
impl Fixture {
    fn new(reads: Vec<(TaskIdentity, Box<FinalTaskInfo>)>) -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (mut source_work, source_capacity) = scope
            .reserve_next(
                generation(),
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        source_work.start().unwrap();
        let runtime = ActorRuntime::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let registration = runtime
            .register_bound(
                actor(),
                scope.clone(),
                Recorder(seen.clone()),
                ActorConfig {
                    mailbox: [8; 4],
                    max_batches: 8,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap();
        let records = DiagnosticRecords::new(Arc::new(OriginalTransport {
            reads: Mutex::new(reads.into()),
        }));
        Self {
            ledger,
            scope,
            runtime,
            registration,
            source_work,
            source_capacity,
            records,
            seen,
            retired: Arc::new(AtomicUsize::new(0)),
        }
    }
    async fn read_and_retire(
        &mut self,
        request: AttemptRequestToken,
        task: TaskIdentity,
    ) -> (FinalTaskInfoProjection, usize, bool) {
        let mut input = CausalInputObligation::reserve_admitted(
            &self.runtime,
            &self.registration,
            ActorId::new(2).unwrap(),
            InputClass::Ordinary,
            &self.source_work,
        )
        .unwrap();
        let (work, mut capacity) = self
            .scope
            .reserve_next(
                generation(),
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Cleanup,
                WorkAdmission::Derived(self.source_work.id()),
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            generation(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (custody, exit) = ExitCustody::from_admitted(
            ReportedWork::try_new(work, capacity, report.clone()).unwrap(),
        );
        self.records
            .register(DiagnosticRegistration {
                request,
                task,
                failure: HostFailureRef(NonZeroU64::new(request.get()).unwrap()),
                custody: custody.clone(),
                original_retention: Some(Box::new(ReadRetention(self.retired.clone()))),
            })
            .map_err(|(error, _)| error)
            .unwrap();
        let fact = run_future(
            exit,
            self.records.run_admitted_read(request, task),
            PanicSink::new(),
        )
        .await
        .unwrap()
        .unwrap();
        let actual_exit =
            custody.phase() == ExitCustodyPhase::Exited && custody.actual_exit_ready().unwrap();
        let (_, reply) = input
            .fill_with_receipt(None, fact)
            .map_err(|failure| failure.error)
            .unwrap();
        self.records.bind_reply_input(request, reply).unwrap();
        self.runtime.poll_one(Tick(1)).unwrap();
        self.runtime.retire_input(actor(), reply).unwrap();
        self.records.core_consumed(&[reply]);
        input.close_after_exit().unwrap();
        report.consume(consumer).unwrap();
        assert!(custody.try_retire().unwrap());
        let retired = self.records.take_retired();
        let count = retired.len();
        drop(retired);
        self.ledger.reap_scope_ready(self.scope.id(), 32).unwrap();
        let projection = self.seen.lock().unwrap().last().unwrap().clone();
        (projection, count, actual_exit)
    }
    fn finish(mut self) {
        // Every original read/capture/role header reaps before its Source exits.
        self.records.close_external();
        self.ledger.reap_scope_ready(self.scope.id(), 32).unwrap();
        self.source_work.body_exited().unwrap();
        self.source_work.captures_exited().unwrap();
        self.source_capacity.finish().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 32).unwrap();
        self.runtime.close_actor_admission(actor()).unwrap();
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        self.runtime.seal_actor(actor()).unwrap();
        self.scope.close_external().unwrap();
        self.runtime.remove_actor(actor()).unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}

#[tokio::test]
async fn original_information_survives_physical_retirement_and_does_not_occupy_read_slot() {
    let first = large_information(task(1));
    let first_pointer = first.as_ref() as *const FinalTaskInfo as usize;
    let operators_pointer = first.operator_statistics().as_ptr() as usize;
    let counters_pointer = first.operator_statistics()[0].counters().as_ptr() as usize;
    let second = info(task(1));
    let second_pointer = second.as_ref() as *const FinalTaskInfo as usize;
    let mut f = Fixture::new(vec![(task(1), first), (task(1), second)]);
    let (one, retired_one, exit_one) = f.read_and_retire(request(1), task(1)).await;
    // The prior rich allocation remains retained while the actual physical read
    // slot is reused by a second original admitted job.
    let (two, retired_two, exit_two) = f.read_and_retire(request(2), task(1)).await;
    f.records.close_external();
    let taken_one = f.records.take_information(one.reference).unwrap();
    let taken_two = f.records.take_information(two.reference).unwrap();
    let same_one = taken_one.as_ref() as *const FinalTaskInfo as usize == first_pointer;
    let same_two = taken_two.as_ref() as *const FinalTaskInfo as usize == second_pointer;
    let same_operators = taken_one.operator_statistics().as_ptr() as usize == operators_pointer;
    let same_counters =
        taken_one.operator_statistics()[0].counters().as_ptr() as usize == counters_pointer;
    let operator_count = taken_one.operator_statistics().len();
    let counters_per_operator = taken_one
        .operator_statistics()
        .iter()
        .map(|v| v.counters().len())
        .collect::<Vec<_>>();
    let repeated_take = matches!(
        f.records.take_information(one.reference),
        Err(PumpRecordError::MissingRecord)
    );
    let repeated_discard = f
        .records
        .discard_information(one.reference)
        .unwrap()
        .is_none();
    let retained_drops = f.retired.load(Ordering::SeqCst);
    drop((taken_one, taken_two));
    f.finish();
    assert!(exit_one && exit_two);
    assert_eq!((retired_one, retired_two, retained_drops), (1, 1, 2));
    assert!(
        same_one
            && same_two
            && same_operators
            && same_counters
            && repeated_take
            && repeated_discard
    );
    assert_eq!(operator_count, 4096);
    assert_eq!(counters_per_operator, vec![64; 4096]);
}

#[tokio::test]
async fn foreign_status_keeps_requested_reference_and_exact_once_discard() {
    let original = info(task(2));
    let pointer = original.as_ref() as *const FinalTaskInfo as usize;
    let mut f = Fixture::new(vec![(task(1), original)]);
    let (projection, retired, exited) = f.read_and_retire(request(1), task(1)).await;
    let wrong = FinalTaskInfoRef::new(request(1), task(2));
    let unknown = FinalTaskInfoRef::new(request(99), task(1));
    let wrong_take = matches!(
        f.records.take_information(wrong),
        Err(PumpRecordError::WrongIdentity)
    );
    let wrong_discard = matches!(
        f.records.discard_information(wrong),
        Err(PumpRecordError::WrongIdentity)
    );
    let unknown_take = matches!(
        f.records.take_information(unknown),
        Err(PumpRecordError::MissingRecord)
    );
    let unknown_discard = matches!(
        f.records.discard_information(unknown),
        Err(PumpRecordError::MissingRecord)
    );
    f.records.close_external();
    let discarded = f
        .records
        .discard_information(projection.reference)
        .unwrap()
        .unwrap();
    let same = discarded.as_ref() as *const FinalTaskInfo as usize == pointer;
    let actual_status = discarded.final_status().identity();
    let twice = f
        .records
        .discard_information(projection.reference)
        .unwrap()
        .is_none();
    let wrong_after_consumed = matches!(
        f.records.discard_information(wrong),
        Err(PumpRecordError::WrongIdentity)
    );
    drop(discarded);
    f.finish();
    assert!(exited && same && twice);
    assert_eq!(retired, 1);
    assert_eq!(
        projection.reference,
        FinalTaskInfoRef::new(request(1), task(1))
    );
    assert_eq!(projection.final_status.identity(), task(2));
    assert_eq!(actual_status, task(2));
    assert!(wrong_take && wrong_discard && unknown_take && unknown_discard && wrong_after_consumed);
}

#[tokio::test]
async fn paid_diagnostic_source_reports_actual_exit_before_its_original_result() {
    use crate::{
        control_runtime::transport_withdrawal::AttemptEffectEnvelope,
        task_execution::host::{
            custody::{TransportRetirements, tests::WithdrawalInbox},
            pumps::AttemptPumpSource,
            transport::handoff_actor_transport,
        },
    };
    use novarocks_attempt_protocol::effect::{AttemptEffect, EffectBody, EffectClass};
    use novarocks_coordination_contract::{AttemptActionId, MonotonicInstant};
    use std::time::Duration;
    use tokio::sync::oneshot;

    struct GatedRead {
        original: Mutex<Option<oneshot::Receiver<Box<FinalTaskInfo>>>>,
        polled: Arc<tokio::sync::Notify>,
    }
    impl TaskResultTransport for GatedRead {
        fn final_task_info(
            &self,
            _: TaskIdentity,
        ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
            let original = self.original.lock().unwrap().take().unwrap();
            Box::pin(async move {
                self.polled.notify_one();
                Ok(FinalTaskInfoRead::Available(original.await.unwrap()))
            })
        }
        fn dynamic_filters(
            &self,
            _: TaskIdentity,
            _: Option<DomainVersion>,
        ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
            unreachable!("only the original diagnostic read")
        }
    }

    let ledger = RuntimeLedger::new(ResourceVector::default());
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut parent, mut payer) = scope
        .reserve_next(
            generation(),
            ReservationResources::work_only(),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    parent.start().unwrap();
    let task = task(1);
    let request = request(1);
    let effect_id = AttemptActionId::new(NonZeroU64::MIN);
    let effect = AttemptEffectEnvelope::new(AttemptEffect {
        id: effect_id,
        class: EffectClass::Settlement,
        body: EffectBody::CollectDiagnostics {
            request,
            task,
            deadline: MonotonicInstant::from_origin(Duration::from_secs(1)),
        },
    });
    let inbox = WithdrawalInbox::with_commands(&scope, [(effect.clone(), None)], [16; 4]);
    let (send, receive) = oneshot::channel();
    let polled = Arc::new(tokio::sync::Notify::new());
    let records = DiagnosticRecords::new(Arc::new(GatedRead {
        original: Mutex::new(Some(receive)),
        polled: polled.clone(),
    }));
    let mut source = AttemptPumpSource::bind(
        inbox.runtime(),
        inbox.registration(),
        inbox.source(),
        &parent,
        &payer,
        ConsumerId::new(1).unwrap(),
        None,
        Some(records.clone()),
        None,
    )
    .unwrap();
    let position = source.prepay(&effect).unwrap().unwrap();
    let ticket = inbox.command(&parent, effect_id);
    let command = ticket.input;
    let handle = tokio::runtime::Handle::current();
    let mut original = source.bind_job(&ticket, position);
    let mut retirement = TransportRetirements::new(
        inbox.runtime().clone(),
        inbox.registration().clone(),
        handle.clone(),
        source.capacity(),
    );
    retirement
        .track(effect, original.transport.take().unwrap())
        .unwrap_or_else(|_| panic!("exact original diagnostic Join"));
    handoff_actor_transport(&handle, original)
        .unwrap_or_else(|_| panic!("original diagnostic handoff"));
    inbox
        .runtime()
        .effect_completed(ticket)
        .unwrap_or_else(|_| panic!("original diagnostic handoff completion"));
    inbox
        .runtime()
        .retire_input(inbox.registration().actor(), command)
        .unwrap();

    fn drive(
        inbox: &WithdrawalInbox,
        source: &mut AttemptPumpSource,
        retirement: &mut TransportRetirements,
        closing: bool,
    ) {
        while let Some(turn) = inbox.runtime().claim_host_actor() {
            let transport = retirement.progress(&turn, 1, |effect, event| {
                source
                    .transport_event(effect, event)
                    .unwrap_or_else(|original| std::panic::resume_unwind(original))
            });
            let source = source.progress(false, closing, closing, 1, true, drop);
            turn.release(transport.more || source.more).unwrap();
        }
    }

    polled.notified().await;
    drive(&inbox, &mut source, &mut retirement, false);
    inbox.assert_no_result();
    let original = info(task);
    let pointer = std::ptr::from_ref(original.as_ref()) as usize;
    send.send(original).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            drive(&inbox, &mut source, &mut retirement, false);
            if let Some((exit, fact)) = inbox.try_commit_next() {
                assert!(matches!(fact, ExecutorFact::ActualExit { effect } if effect == effect_id));
                let (input, fact) = inbox.try_commit_next().unwrap();
                assert!(matches!(fact, ExecutorFact::DiagnosticsFinished {
                    request: actual, task: actual_task,
                    result: DiagnosticRead::Available(FinalTaskInfoProjection { reference, .. }),
                } if actual == request && actual_task == task && reference == FinalTaskInfoRef::new(request, task)));
                inbox.runtime().retire_input(inbox.registration().actor(), exit).unwrap();
                inbox.runtime().retire_input(inbox.registration().actor(), input).unwrap();
                break;
            }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    let recovered = records
        .take_information(FinalTaskInfoRef::new(request, task))
        .unwrap();
    assert_eq!(pointer, std::ptr::from_ref(recovered.as_ref()) as usize);
    drop(recovered);

    inbox
        .runtime()
        .notify_host_actor(inbox.registration())
        .unwrap();
    drive(&inbox, &mut source, &mut retirement, true);
    assert!(retirement.is_empty() && source.is_drained());
    parent.body_exited().unwrap();
    parent.captures_exited().unwrap();
    payer.finish().unwrap();
    scope.close_external().unwrap();
    ledger.reap_scope_ready(scope.id(), 32).unwrap();
    assert!(scope.join_ready().unwrap());
    inbox
        .runtime()
        .close_actor_admission(inbox.registration().actor())
        .unwrap();
    while let Some(turn) = inbox.runtime().claim_host_actor() {
        turn.release(false).unwrap();
    }
    inbox
        .runtime()
        .seal_actor(inbox.registration().actor())
        .unwrap();
    inbox.finish();
}

#[cfg(novarocks_dev_controls)]
#[tokio::test(flavor = "current_thread")]
async fn original_ready_publishes_through_the_core_while_paid_diagnostic_read_is_held() {
    use crate::{
        control_runtime::transport_withdrawal::AttemptEffectEnvelope,
        query_execution::internal_consumer::test_fixture::internal_count_description,
        task_execution::{
            bridge::root_control::PendingRootControls,
            host::{
                adapter::tests::start,
                custody::{TransportRetirements, tests::WithdrawalInbox},
                publication::{
                    AttemptDiagnosticSources, AttemptPublicationOwner, PublicationRecord,
                },
                pumps::AttemptPumpSource,
                transport::handoff_actor_transport,
            },
            intent::{AckPayload, OperationIntent},
            profile_diagnostics::{
                ProfileDiagnosticClosure, ProfileDiagnosticMissing, ProfileTaskDiagnostic,
                TaskFinalInfoCollector,
            },
            protocol_facts::AttemptProtocolFactsPublisher,
            status_intake::CountingWake,
            tests::{
                manifest_normal_release_ack_with_actor_facts_fixture,
                settle_original_actor_establish_facts,
            },
        },
    };
    use novarocks_attempt_protocol::{
        AttemptCore, Core, InputSelection,
        capacity::{QueueStage, StepContext},
        config::DispatchLane,
        effect::{AttemptEffect, EffectBody, Publication},
        input::{
            AttemptInput, AttemptInputClaim, AttemptInputId, AttemptSourceCut,
            MembershipObservation, ObservationBatch, ObservationEvent, ObservationFact,
            ObservationOpening, PublicationApplied, ReceivedObservation,
        },
        start::{
            FrozenCreation, FrozenEstablish, FrozenExchangeEdge, FrozenTask, FrozenTaskGraph,
            PayloadRef,
        },
    };
    use novarocks_coordination_contract::{
        AttemptActionId, AttemptComputationCut, AttemptConvergenceMode, AttemptInheritedAdmission,
        AttemptToLogicalBody, ExecutionEffect, LogicalToAttempt, LogicalToAttemptBody,
        MonotonicClock,
    };
    use novarocks_execution_contract::{AdmissionEpochCapability, root_result::RootResultEnd};
    use novarocks_query_application::{
        api::ComputationObservationBudgets, coordination::LogicalExecutionActorConfig,
        test_support::LogicalExecutionTestHarness,
    };
    use novarocks_workload_control::{
        ResourceConfig, Stage, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    use std::{
        num::NonZeroUsize,
        time::{Duration, Instant},
    };
    use tokio::sync::{Notify, oneshot};

    struct Units(usize);
    impl StepContext for Units {
        fn charge(&mut self, units: usize) -> bool {
            let Some(remaining) = self.0.checked_sub(units) else {
                return false;
            };
            self.0 = remaining;
            true
        }

        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            _: bool,
            _: BackendProcessId,
            _: usize,
            _: usize,
        ) -> QueueStage {
            panic!("this fixture holds the original remote observation instead of sending")
        }
    }

    struct HeldRead {
        receive: Mutex<Option<oneshot::Receiver<Box<FinalTaskInfo>>>>,
        polled: Arc<Notify>,
        exited: Arc<AtomicUsize>,
    }
    impl TaskResultTransport for HeldRead {
        fn final_task_info(
            &self,
            _: TaskIdentity,
        ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
            let receive = self
                .receive
                .lock()
                .unwrap()
                .take()
                .expect("one original read");
            let polled = self.polled.clone();
            let retention = ReadRetention(self.exited.clone());
            Box::pin(async move {
                let _retention = retention;
                polled.notify_one();
                Ok(FinalTaskInfoRead::Available(receive.await.unwrap()))
            })
        }

        fn dynamic_filters(
            &self,
            _: TaskIdentity,
            _: Option<DomainVersion>,
        ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
            unreachable!("this fixture reads only the original final-info RPC")
        }
    }

    let (round, _, _, clock, establish) = manifest_normal_release_ack_with_actor_facts_fixture();
    let graph = round.execution().graph();
    let root = graph.root_identity();
    let participants = graph.participations().copied().collect::<Vec<_>>();
    assert_eq!(participants.len(), 1);
    let participation = participants[0];
    let expected_fragments = graph
        .tasks()
        .map(|task| (task.identity(), task.fragment_id()))
        .collect::<Vec<_>>();
    let statuses = graph
        .tasks()
        .map(|task| {
            round
                .execution()
                .task(task.task_id())
                .unwrap()
                .status()
                .unwrap()
                .clone()
        })
        .collect::<Vec<_>>();
    assert!(statuses.iter().all(TaskStatus::is_terminal));
    let root_status = statuses
        .iter()
        .find(|status| status.identity() == root)
        .unwrap();
    let inherited = establish
        .iter()
        .filter_map(|(intent, ack)| {
            let (
                OperationIntent::AcquireParticipationAdmissionTicket(request),
                AckPayload::AdmissionTicket(receipt),
            ) = (intent, ack.payload())
            else {
                return None;
            };
            Some(AttemptInheritedAdmission {
                participation: request.participation(),
                receipt: *receipt,
            })
        })
        .collect::<Box<[_]>>();
    assert_eq!(inherited.len(), 1);

    let workload = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1 << 10,
            per_scope_bytes: 1 << 18,
        },
    )
    .unwrap();
    workload.mark_ready().unwrap();
    let governed = workload
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let stage = governed
        .owner
        .scope()
        .try_acquire(Stage::Execution)
        .unwrap();
    let (abort, _intake) = crate::task_execution::abort_effect::NativeAbortEffectAdapter::bounded(
        NonZeroUsize::new(2).unwrap(),
        Arc::new(CountingWake::default()),
    );
    let config = LogicalExecutionActorConfig::single_attempt_completion(
        root.query_execution_id(),
        ExecutionEffect::None,
        NonZeroUsize::new(8).unwrap(),
        participants.clone(),
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(2).unwrap(),
        governed.owner,
        stage,
    )
    .unwrap()
    .with_abort_participation_effect_port(abort, NonZeroUsize::new(2).unwrap());
    let (description, plan) = internal_count_description();
    let diagnostic_budget = Duration::from_millis(10);
    let remote_budget = Duration::from_secs(60);
    let mut logical = LogicalExecutionTestHarness::install_internal(
        tokio::runtime::Handle::current(),
        config,
        &description,
        NonZeroUsize::MIN,
        clock.clone() as Arc<dyn MonotonicClock>,
        ComputationObservationBudgets::new(diagnostic_budget, remote_budget),
        root.query_execution_id(),
        participants,
    )
    .unwrap();
    let activation = logical.activate_initial().await.unwrap();
    settle_original_actor_establish_facts(&logical, establish).await;
    let original_cut = clock.now();
    let ready = logical
        .accept_internal_end(
            root,
            root_status.clone(),
            RootResultEnd {
                sequence: NonZeroU64::MIN,
                output_rows: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(ready.activation(), activation);
    assert_eq!(ready.root(), root);
    assert_eq!(ready.cut(), original_cut);
    assert_eq!(
        ready.diagnostic_deadline(),
        original_cut.saturating_add(diagnostic_budget)
    );
    assert_eq!(
        ready.convergence_deadline(),
        original_cut.saturating_add(remote_budget)
    );

    let mut start = start();
    start.initial_now = original_cut;
    start.activation.identity = activation;
    start.activation.inherited_admissions = inherited;
    start.activation.policy.retain_diagnostics = true;
    start.plan.authorization_limits = logical.native_attempt_drive().authorization_limits();
    start.plan.graph = FrozenTaskGraph {
        root,
        tasks: graph
            .tasks()
            .enumerate()
            .map(|(index, task)| {
                (
                    task.task_id(),
                    FrozenTask {
                        identity: task.identity(),
                        participation: task.participation(),
                        task_key: task.task_key(),
                        fragment_id: task.fragment_id(),
                        split_plan_nodes: task.split_plan_nodes().into(),
                        // Creation is scripted by the original accepted statuses.
                        creation: FrozenCreation {
                            resource: PayloadRef(NonZeroU64::new(index as u64 + 1).unwrap()),
                            queued_bytes: 256,
                            plan_carrier_bytes: 64,
                            descriptor_encoded_bytes: 64,
                        },
                    },
                )
            })
            .collect(),
        stages: graph
            .stages()
            .map(|stage| {
                (
                    stage.stage_id(),
                    stage
                        .tasks()
                        .iter()
                        .map(|&id| graph.task(id).unwrap().identity())
                        .collect(),
                )
            })
            .collect(),
        edges: graph
            .edges()
            .map(|edge| {
                (
                    edge.edge_id(),
                    FrozenExchangeEdge {
                        edge: edge.edge_id(),
                        producer_stage: edge.producer_stage(),
                        consumer_stage: edge.consumer_stage(),
                        destination_node: edge.destination_node_id(),
                        delivery: edge.delivery(),
                        producers: edge.producers().into(),
                        destinations: edge.destinations().into(),
                    },
                )
            })
            .collect(),
    };
    let backend = participation.backend_process_id();
    start.plan.admission_epochs = [(
        backend,
        AdmissionEpochCapability::try_from_bytes([0x61; 16]).unwrap(),
    )]
    .into();
    start.plan.preparation_positions = [(backend, start.plan.deployment_window)].into();
    start.plan.establish_material = [(
        participation,
        FrozenEstablish {
            material: PayloadRef(NonZeroU64::new(100).unwrap()),
            queued_bytes: 256,
        },
    )]
    .into();

    let (send, receive) = oneshot::channel();
    let polled = Arc::new(Notify::new());
    let exited = Arc::new(AtomicUsize::new(0));
    let transport = Arc::new(HeldRead {
        receive: Mutex::new(Some(receive)),
        polled: polled.clone(),
        exited: exited.clone(),
    });
    let records = DiagnosticRecords::new(transport.clone());
    let mut profile = TaskFinalInfoCollector::from_graph(
        transport,
        true,
        graph,
        Arc::new(CountingWake::default()),
    );
    let observer = profile.enable_profile_observation().unwrap();
    let (protocol, _protocol_facts) = AttemptProtocolFactsPublisher::for_graph(
        plan,
        graph,
        &start.plan.writers,
        &start.plan.summary_sources,
    )
    .unwrap();
    let (root_sender, _root_statuses, _root_requests) = PendingRootControls::project(root);
    let publication = AttemptPublicationOwner::new(
        PublicationRecord::new(
            activation,
            root_sender,
            protocol,
            Some(profile),
            Some(records.clone()),
        ),
        Some(AttemptDiagnosticSources::new(&start, None)),
        None,
    );
    publication.register_computation_cut(ready).await.unwrap();

    let sources = start.sources;
    let mut core = <Core as AttemptCore>::new(start).unwrap();
    let generation_id = NonZeroU64::new(400).unwrap();
    let covered_generation = core
        .open_subscription_for_test(
            participation,
            AttemptActionId::new(generation_id),
            AttemptRequestToken::new(generation_id),
            &mut Units(128),
        )
        .unwrap();
    let mut sequence = 0;
    let mut step = |core: &mut Core, now, source, input| {
        sequence += 1;
        let input_id = AttemptInputId(NonZeroU64::new(sequence).unwrap());
        core.handle(
            now,
            AttemptInputClaim {
                source,
                input: input_id,
                native_cut: Some(AttemptSourceCut {
                    source: sources.native,
                    tail: None,
                }),
                selected_cut: Some(AttemptSourceCut {
                    source: sources.observation,
                    tail: (source == sources.observation).then_some(input_id),
                }),
            },
            input,
            &mut Units(128),
        )
        .unwrap()
    };
    let started = step(
        &mut core,
        clock.now(),
        sources.actor,
        AttemptInput::Bootstrap,
    );
    assert_eq!(started.selection, InputSelection::Prefer(sources.native));
    let mut observed = step(
        &mut core,
        clock.now(),
        sources.observation,
        AttemptInput::Executor(ExecutorFact::Observations(
            ObservationBatch::new(
                statuses
                    .iter()
                    .cloned()
                    .map(|status| ReceivedObservation {
                        participation,
                        generation: covered_generation,
                        received_at: original_cut,
                        opening: ObservationOpening {
                            started_at: original_cut,
                            opened_at: Some(original_cut),
                        },
                        event: ObservationEvent::Fact {
                            source_revision: None,
                            fact: ObservationFact::Status(status),
                        },
                    })
                    .collect(),
            )
            .unwrap(),
        )),
    );
    for _ in 0..32 {
        if let Some(effect) = observed.effect.take() {
            match effect.body {
                EffectBody::Publish(value) => {
                    let (input, error) = publication.publish(effect.id, &value).await;
                    assert!(error.is_none());
                    observed = step(&mut core, clock.now(), sources.publication, input);
                    continue;
                }
                EffectBody::ReleaseCreationPayload { .. } | EffectBody::AdjustReadWindow(_) => {}
                other => panic!("unexpected startup effect {other:?}"),
            }
        }
        if observed.selection == InputSelection::Prefer(sources.native) {
            break;
        }
        assert!(matches!(observed.selection, InputSelection::Only(source)
            if source == sources.actor || source == sources.observation));
        observed = step(&mut core, clock.now(), sources.actor, AttemptInput::Timer);
    }
    assert_eq!(observed.selection, InputSelection::Prefer(sources.native));

    let ready_projection = AttemptComputationCut {
        activation: ready.activation(),
        root: ready.root(),
        cut: ready.cut(),
        diagnostic_deadline: ready.diagnostic_deadline(),
        convergence_deadline: ready.convergence_deadline(),
        end: ready.end(),
    };
    let converge = |remote_deadline| {
        AttemptInput::Port(LogicalToAttempt {
            activation,
            body: LogicalToAttemptBody::BeginConvergence {
                mode: AttemptConvergenceMode::Normal,
                remote_deadline,
                computation_ready: Some(ready_projection),
                retain_diagnostics: true,
            },
        })
    };
    step(
        &mut core,
        clock.now(),
        sources.actor,
        converge(ready.convergence_deadline()),
    );
    let read = step(&mut core, clock.now(), sources.actor, AttemptInput::Timer)
        .effect
        .expect("the original Core selected the original diagnostic read");
    let EffectBody::CollectDiagnostics {
        request,
        task,
        deadline,
    } = read.body
    else {
        panic!("expected the original diagnostic effect");
    };
    assert_eq!(deadline, ready.diagnostic_deadline());
    let effect_id = read.id;
    let read = AttemptEffectEnvelope::new(AttemptEffect {
        body: EffectBody::CollectDiagnostics {
            request,
            task,
            deadline,
        },
        ..read
    });
    let process = step(&mut core, clock.now(), sources.actor, AttemptInput::Timer);
    let Some(AttemptEffect {
        body:
            EffectBody::ObserveProcess {
                request: process_request,
                participation: process_participation,
            },
        ..
    }) = process.effect
    else {
        panic!("the original admission ticket prevents unproven remote closure");
    };
    assert_eq!(process_participation, participation);
    assert_eq!(process.selection, InputSelection::Only(sources.process));
    assert_eq!(core.next_deadline(), Some(ready.diagnostic_deadline()));

    let ledger = RuntimeLedger::new(ResourceVector::default());
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut parent, mut payer) = scope
        .reserve_next(
            generation(),
            ReservationResources::work_only(),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    parent.start().unwrap();
    let inbox = WithdrawalInbox::with_commands(&scope, [(read.clone(), None)], [16; 4]);
    let mut pump = AttemptPumpSource::bind(
        inbox.runtime(),
        inbox.registration(),
        inbox.source(),
        &parent,
        &payer,
        ConsumerId::new(1).unwrap(),
        None,
        Some(records.clone()),
        None,
    )
    .unwrap();
    let position = pump.prepay(&read).unwrap().unwrap();
    let ticket = inbox.command(&parent, effect_id);
    let input = ticket.input;
    let handle = tokio::runtime::Handle::current();
    let mut job = pump.bind_job(&ticket, position);
    let mut retirement = TransportRetirements::new(
        inbox.runtime().clone(),
        inbox.registration().clone(),
        handle.clone(),
        pump.capacity(),
    );
    retirement
        .track(read, job.transport.take().unwrap())
        .unwrap_or_else(|_| panic!("same original diagnostic Join"));
    handoff_actor_transport(&handle, job).unwrap_or_else(|_| panic!("original diagnostic handoff"));
    inbox
        .runtime()
        .effect_completed(ticket)
        .unwrap_or_else(|_| panic!("original handoff receipt"));
    inbox
        .runtime()
        .retire_input(inbox.registration().actor(), input)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), polled.notified())
        .await
        .unwrap();

    let drive_host =
        |pump: &mut AttemptPumpSource, retirement: &mut TransportRetirements, closing| {
            while let Some(turn) = inbox.runtime().claim_host_actor() {
                let transport = retirement.progress(&turn, 1, |effect, event| {
                    pump.transport_event(effect, event)
                        .unwrap_or_else(|value| std::panic::resume_unwind(value))
                });
                let pump = pump.progress(false, closing, closing, 1, true, drop);
                turn.release(transport.more || pump.more).unwrap();
            }
        };
    drive_host(&mut pump, &mut retirement, false);
    inbox.assert_no_result();

    clock.advance(diagnostic_budget - Duration::from_nanos(1));
    let later_remote = ready
        .convergence_deadline()
        .saturating_add(Duration::from_secs(300));
    let reentry = step(
        &mut core,
        clock.now(),
        sources.actor,
        converge(later_remote),
    );
    assert_eq!(reentry.selection, InputSelection::Only(sources.process));
    let before = step(&mut core, clock.now(), sources.actor, AttemptInput::Timer);
    assert!(before.effect.is_none());
    assert!(observer.current(ready).unwrap().is_none());
    assert_eq!(core.next_deadline(), Some(ready.diagnostic_deadline()));
    clock.advance(Duration::from_nanos(1));

    let selected = step(&mut core, clock.now(), sources.actor, AttemptInput::Timer);
    let Some(AttemptEffect {
        id: publication_effect,
        body: EffectBody::Publish(value @ Publication::Diagnostics(_)),
        ..
    }) = selected.effect
    else {
        panic!("Core did not select the original Ready deadline");
    };
    let (published, error) = publication.publish(publication_effect, &value).await;
    assert!(error.is_none());
    assert!(matches!(&published,
        AttemptInput::Executor(ExecutorFact::PublicationApplied { effect, result: Ok(PublicationApplied::Diagnostics) })
            if *effect == publication_effect));
    let after_publication = step(&mut core, clock.now(), sources.publication, published);
    assert_eq!(
        after_publication.selection,
        InputSelection::Only(sources.process)
    );
    let snapshot = observer
        .current(ready)
        .unwrap()
        .expect("the original snapshot is visible before either reply");
    assert_eq!(snapshot.ready, ready);
    assert_eq!(snapshot.selected_at, ready.diagnostic_deadline());
    assert_eq!(
        snapshot.closure,
        ProfileDiagnosticClosure::ObservationDeadline
    );
    assert_eq!(snapshot.fragments.as_ref(), expected_fragments.as_slice());
    assert!(!snapshot.runtime_filters.is_complete());
    assert!(snapshot.runtime_filters.contributions().is_empty());
    assert!(
        snapshot
            .tasks
            .iter()
            .any(|(identity, fact)| *identity == task
                && matches!(
                    fact,
                    ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::ReadPending)
                ))
    );
    assert!(snapshot.tasks.iter().any(|(_, fact)| matches!(
        fact,
        ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::NotRead)
    )));
    assert_eq!(exited.load(Ordering::SeqCst), 0);
    assert!(!retirement.is_empty());
    drive_host(&mut pump, &mut retirement, false);
    inbox.assert_no_result();

    let mut remote = step(
        &mut core,
        clock.now(),
        sources.process,
        AttemptInput::Executor(ExecutorFact::ProcessSnapshot {
            request: process_request,
            participation,
            sampled_at: clock.now(),
            observation: MembershipObservation::ExactReplacement(BackendProcessId::new_v7()),
        }),
    );
    let mut remote_reported = false;
    for _ in 0..16 {
        if let Some(effect) = remote.effect.take() {
            let EffectBody::Port(port) = effect.body else {
                panic!("local convergence cannot close before the original diagnostic read");
            };
            assert!(matches!(
                port.body,
                AttemptToLogicalBody::ConvergenceProgress {
                    local_exited: false,
                    closure: Some(_),
                    ..
                }
            ));
            remote_reported = true;
        }
        if remote.selection == InputSelection::Prefer(sources.native) {
            break;
        }
        remote = step(&mut core, clock.now(), sources.actor, AttemptInput::Timer);
    }
    assert!(remote_reported);
    assert_eq!(remote.selection, InputSelection::Prefer(sources.native));
    assert_eq!(exited.load(Ordering::SeqCst), 0);

    let status = statuses
        .into_iter()
        .find(|status| status.identity() == task)
        .unwrap();
    let original = Box::new(FinalTaskInfo::try_new(task, status, Vec::new(), false).unwrap());
    let address = std::ptr::from_ref(original.as_ref()) as usize;
    send.send(original).unwrap();
    let (actual_exit, late_result) = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            drive_host(&mut pump, &mut retirement, false);
            if let Some((exit_input, actual_exit)) = inbox.try_commit_next() {
                let (result_input, late_result) =
                    inbox.try_commit_next().expect("original causal successor");
                for input in [exit_input, result_input] {
                    inbox
                        .runtime()
                        .retire_input(inbox.registration().actor(), input)
                        .unwrap();
                }
                break (actual_exit, late_result);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(exited.load(Ordering::SeqCst), 1);
    assert!(matches!(&actual_exit, ExecutorFact::ActualExit { effect } if *effect == effect_id));
    let retired_read = step(
        &mut core,
        clock.now(),
        sources.transport,
        AttemptInput::Executor(actual_exit),
    );
    assert!(retired_read.effect.is_none() && retired_read.diagnostic_failure.is_none());
    let late = step(
        &mut core,
        clock.now(),
        sources.transport,
        AttemptInput::Executor(late_result),
    );
    assert!(late.diagnostic_failure.is_none());
    let Some(AttemptEffect {
        body:
            EffectBody::DisposeDiagnosticInfo {
                request: original_request,
                task: original_task,
                information,
            },
        ..
    }) = late.effect
    else {
        panic!("late original final-info must return to its disposal owner");
    };
    assert_eq!((original_request, original_task), (request, task));
    assert_eq!(information, FinalTaskInfoRef::new(request, task));
    let disposed = records.discard_information(information).unwrap().unwrap();
    assert_eq!(std::ptr::from_ref(disposed.as_ref()) as usize, address);
    drop(disposed);
    assert!(Arc::ptr_eq(
        &snapshot,
        &observer.current(ready).unwrap().unwrap()
    ));

    inbox
        .runtime()
        .notify_host_actor(inbox.registration())
        .unwrap();
    drive_host(&mut pump, &mut retirement, true);
    assert!(retirement.is_empty() && pump.is_drained());
    parent.body_exited().unwrap();
    parent.captures_exited().unwrap();
    payer.finish().unwrap();
    scope.close_external().unwrap();
    ledger.reap_scope_ready(scope.id(), 32).unwrap();
    assert!(scope.join_ready().unwrap());
    inbox
        .runtime()
        .close_actor_admission(inbox.registration().actor())
        .unwrap();
    while let Some(turn) = inbox.runtime().claim_host_actor() {
        turn.release(false).unwrap();
    }
    inbox
        .runtime()
        .seal_actor(inbox.registration().actor())
        .unwrap();
    inbox.finish();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());

    logical.abandon_running_attempt();
    logical
        .observe_worker_stopped_and_participation_fenced(participation)
        .await
        .unwrap();
    logical
        .finish_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}
