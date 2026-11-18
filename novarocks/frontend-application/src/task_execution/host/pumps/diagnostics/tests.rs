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
    OperatorCounter, OperatorStatistics, SafeDetail, TaskOutputFacts, TaskState, TaskStatus,
    TaskStatusVersion,
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
async fn drive_original_diagnostic_fixture<F: std::future::Future>(
    fixture: &mut crate::fixtures::FrontendFixture,
    joint: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
    clock: &crate::task_execution::clock::ManualClock,
    advance: bool,
    original: F,
) -> F::Output {
    use novarocks_coordination_contract::MonotonicClock;
    let mut original = Box::pin(original);
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        crate::query_execution::native_execution_adapter::source_cohort::tests::drive(joint);
        if let std::task::Poll::Ready(value) = futures::poll!(original.as_mut()) {
            return value;
        }
        let progress = fixture
            .progress(
                crate::fixtures::ProgressSelection::All,
                crate::fixtures::ProgressBudget::default(),
            )
            .await;
        if advance
            && progress.actors == 0
            && progress.hosts == 0
            && let Some(next) = fixture.runtime().next_deadline()
            && std::time::Duration::from_nanos(next.0) > clock.now().since_origin()
        {
            clock.advance(std::time::Duration::from_nanos(
                next.0 - u64::try_from(clock.now().since_origin().as_nanos()).unwrap(),
            ));
        }
        assert!(
            std::time::Instant::now() < until,
            "the original diagnostic fixture did not finish"
        );
        tokio::task::yield_now().await;
    }
}

#[cfg(novarocks_dev_controls)]
#[tokio::test(flavor = "current_thread")]
async fn original_ready_publishes_through_the_core_while_paid_diagnostic_read_is_held() {
    use crate::fixtures::native_recipe::{
        OriginalRecipeContext, OriginalRecipeOperands, PreparedOriginalRecipeProcess, RecipeRoot,
        count_description, install_original_recipe, prepare_original_recipe_process_with_budgets,
    };
    use crate::native::{data_runtime::FrontendDataRuntime, task_transport::tests::Loopback};
    use crate::query_execution::internal_consumer::profile::profile_computation_consumer;
    use crate::{
        control_runtime::transport_withdrawal::AttemptEffectEnvelope,
        task_execution::{
            host::{
                adapter::tests::start,
                custody::{TransportRetirements, tests::WithdrawalInbox},
                publication::{
                    AttemptDiagnosticSources, AttemptPublicationOwner, PublicationRecord,
                },
                pumps::AttemptPumpSource,
                transport::handoff_actor_transport,
            },
            profile_diagnostics::{
                ProfileDiagnosticClosure, ProfileDiagnosticMissing, ProfileTaskDiagnostic,
            },
            protocol_facts::AttemptProtocolFactsPublisher,
        },
    };
    use novarocks_attempt_protocol::port_output::AttemptToLogicalBody;
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
        LogicalToAttempt, LogicalToAttemptBody, MonotonicClock,
    };
    use novarocks_execution_contract::AdmissionEpochCapability;
    use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
    use novarocks_workload_control::{
        ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
    };
    use std::time::{Duration, Instant};
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
    }

    let (send, receive) = oneshot::channel();
    let polled = Arc::new(Notify::new());
    let exited = Arc::new(AtomicUsize::new(0));
    let transport = Arc::new(HeldRead {
        receive: Mutex::new(Some(receive)),
        polled: polled.clone(),
        exited: exited.clone(),
    });
    let diagnostic_budget = Duration::from_millis(10);
    let remote_budget = Duration::from_secs(60);
    let mut data = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let mut peer = Loopback::start().await;
    let descriptor = peer.bounded_root_descriptor(BackendProcessId::new_v7());
    let compatibility = descriptor.native_compatibility_id();
    let history = novarocks_control_runtime::history::HistoryCapture::new(32_768);
    let PreparedOriginalRecipeProcess {
        mut fixture,
        process,
        clock,
        mut joint,
        workload,
        mut supervisor,
        client,
        capacity,
        handle,
        descriptor,
    } = prepare_original_recipe_process_with_budgets(
        &data,
        descriptor,
        history,
        ResultCapacityConfig::V1,
        None,
        diagnostic_budget,
        remote_budget,
    );
    let original_root = workload
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let original_scope = original_root.owner.scope();
    let original_business = original_root.business;
    let window = capacity
        .try_acquire(&original_scope, ResultWindowClass::Internal)
        .unwrap();
    let (description, plan) = count_description();
    let (consumer, offers, output) = profile_computation_consumer(&description, plan).unwrap();
    let (mut logical, cohort, mut output) = install_original_recipe(
        OriginalRecipeContext {
            fixture: &mut fixture,
            joint: &mut joint,
            process: &process,
            data: &data,
            clock: &clock,
            handle,
            descriptor,
            client,
        },
        OriginalRecipeOperands {
            owner: original_root.owner,
            binding: QueryResultCapacityBinding::try_new(&original_scope, window.retain_alias())
                .unwrap(),
            description,
            plan,
            root: RecipeRoot::EmptyCountOnly.with_three_task_protocol_chain(plan, [2, 3]),
            consumer,
            offers,
            output,
        },
    );
    logical
        .probe
        .graph_capture_requested
        .store(true, Ordering::Release);
    logical
        .probe
        .profile_capture_requested
        .store(true, Ordering::Release);
    *logical.probe.profile_transport.lock().unwrap() = Some(transport.clone());
    let installed = fixture.activate_attempt(&logical, &mut joint, false).await;
    let graph = logical
        .probe
        .graph_capture
        .lock()
        .unwrap()
        .take()
        .expect("same actual frozen graph");
    assert_eq!(graph.plan, plan);
    assert_eq!(
        (graph.tasks.len(), graph.stages.len(), graph.edges.len()),
        (3, 3, 2)
    );
    assert_eq!(graph.fragments, vec![1, 2, 3]);
    let root = graph.root;
    let participants = graph.participations.clone();
    assert_eq!(participants.len(), 1);
    let participation = participants[0];
    let expected_fragments = graph
        .tasks
        .iter()
        .map(|task| (task.identity(), task.fragment_id()))
        .collect::<Vec<_>>();
    let mut profile = logical
        .probe
        .profile_capture
        .lock()
        .unwrap()
        .take()
        .expect("same graph-bound component collector");
    peer.enable_single_root_lifecycle(root, novarocks_result_contract::RootOutputKind::CountOnly);
    peer.enable_three_task_roster(
        graph
            .tasks
            .iter()
            .map(|task| task.identity())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap(),
    )
    .unwrap();
    logical.probe.release_activation();
    let mut receiving = drive_original_diagnostic_fixture(
        &mut fixture,
        &mut joint,
        clock.as_ref(),
        false,
        logical.ready.take().unwrap(),
    )
    .await
    .unwrap();
    let ready = *receiving
        .ready_token()
        .expect("the actual consumer/LP Ready");
    let received = output.take_ready(ready).unwrap();
    let activation = ready.activation();
    let original_cut = clock.now();
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
    // These are passive operands of the independent AP decision component,
    // not additional Worker observations or another live Native attempt.
    let statuses = graph
        .tasks
        .iter()
        .map(|task| {
            TaskStatus::try_new(
                task.identity(),
                TaskStatusVersion::new(3).unwrap(),
                TaskState::Finished,
                None,
                TaskOutputFacts::new(true),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let inherited = Box::new([AttemptInheritedAdmission {
        participation,
        receipt: novarocks_execution_contract::ParticipationAdmissionTicketReceipt::new(
            novarocks_execution_contract::AdmissionTicketId::try_from_bytes([0x71; 16]).unwrap(),
            participation,
            novarocks_execution_contract::LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
        ),
    }]);
    let mut start = start();
    start.initial_now = original_cut;
    start.activation.identity = activation;
    start.activation.inherited_admissions = inherited;
    start.activation.policy.retain_diagnostics = true;
    let binding = logical.binding.bound().unwrap();
    start.plan.authorization_limits = novarocks_coordination_contract::AttemptAuthorizationLimits {
        activation,
        establish_per_participation: binding.start.budgets.establish_per_participation,
        abort_per_participation: binding.start.budgets.abort_per_participation,
    };
    start.plan.compatibility = compatibility;
    drop(binding);
    start.plan.graph = FrozenTaskGraph {
        root,
        tasks: graph
            .tasks
            .iter()
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
                        },
                    },
                )
            })
            .collect(),
        stages: graph
            .stages
            .iter()
            .map(|stage| {
                (
                    stage.stage_id(),
                    stage
                        .tasks()
                        .iter()
                        .map(|&id| {
                            graph
                                .tasks
                                .iter()
                                .find(|task| task.task_id() == id)
                                .unwrap()
                                .identity()
                        })
                        .collect(),
                )
            })
            .collect(),
        edges: graph
            .edges
            .iter()
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

    let records = DiagnosticRecords::new(transport);
    let observer = profile.enable_profile_observation().unwrap();
    let (protocol, _protocol_facts) = AttemptProtocolFactsPublisher::for_diagnostic_metadata(
        plan,
        root,
        &graph
            .tasks
            .iter()
            .map(|task| task.identity())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let publication = AttemptPublicationOwner::new(
        PublicationRecord::new(activation, protocol, Some(profile), Some(records.clone())),
        Some(AttemptDiagnosticSources::new(&start, None)),
    );
    // Bind only passive metadata here; the real Source/HostExit composition is
    // tested separately. Deliberately never poll Native RecordReady before the
    // Core's actual profile publication below.
    use crate::query_execution::logical_binding::{
        OriginalAttemptObservation, OriginalComputationCutPublisher,
    };
    let observation = OriginalAttemptObservation::new(activation, true, false);
    publication.bind_observation(&observation);
    let unregistered = OriginalAttemptObservation::new(activation, true, false);
    unregistered.bind_publisher(&publication);
    assert!(
        publication
            .register_ready_with(ready, &mut || Err(
                crate::task_execution::host::port::control::PortControlError::Closed
            ))
            .is_err()
    );
    assert_eq!(observation.original_registered_cut(), None);
    publication
        .register_ready_with(ready, &mut || Ok(()))
        .unwrap();
    observation.notify_cut_registered();
    assert_eq!(observation.original_registered_cut(), Some(ready));

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
                EffectBody::Port(original) => {
                    assert_eq!(original.activation, activation);
                    assert_eq!(original.action, effect.id);
                    let AttemptToLogicalBody::RootStatus(projected) = original.body else {
                        panic!(
                            "the independent AP startup only publishes its original root status"
                        );
                    };
                    assert_eq!(
                        &projected.status,
                        statuses
                            .iter()
                            .find(|status| status.identity() == root)
                            .unwrap()
                    );
                    assert_eq!(
                        projected.failure,
                        novarocks_coordination_contract::AttemptRootFailure::None
                    );
                    // This independent AP component consumes the passive receipt
                    // for its exact output. The live Source above already supplied
                    // genuine Ready; this is no second LP handoff or Ready proof.
                    observed = step(
                        &mut core,
                        clock.now(),
                        sources.publication,
                        AttemptInput::Executor(ExecutorFact::PublicationApplied {
                            effect: effect.id,
                            result: Ok(PublicationApplied::RootStatus),
                        }),
                    );
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
    let process_step = step(&mut core, clock.now(), sources.actor, AttemptInput::Timer);
    let Some(AttemptEffect {
        body:
            EffectBody::ObserveProcess {
                request: process_request,
                participation: process_participation,
            },
        ..
    }) = process_step.effect
    else {
        panic!("the original admission ticket prevents unproven remote closure");
    };
    assert_eq!(process_participation, participation);
    assert_eq!(
        process_step.selection,
        InputSelection::Only(sources.process)
    );
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
    let original_publisher = Arc::downgrade(&publication);
    drop(publication);
    assert!(original_publisher.upgrade().is_none());
    // First-poll the late Native confirmation only after actual profile
    // publication and the original rich publisher's destruction.
    observation.register_cut(ready).unwrap();
    assert!(unregistered.register_cut(ready).is_err());
    let foreign = novarocks_query_application::api::ComputationReadyToken::from_runtime_cut(
        AttemptComputationCut {
            activation: ready.activation(),
            root: ready.root(),
            cut: ready.cut().saturating_add(Duration::from_nanos(1)),
            diagnostic_deadline: ready.diagnostic_deadline(),
            convergence_deadline: ready.convergence_deadline(),
            end: ready.end(),
        },
    );
    assert!(observation.register_cut(foreign).is_err());
    assert_eq!(observation.original_registered_cut(), Some(ready));
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

    drop((
        snapshot,
        observer,
        unregistered,
        observation,
        records,
        pump,
        retirement,
    ));
    let mut exit = receiving
        .take_exit_observer()
        .or_else(|| logical.exit.take())
        .expect("same original logical exit observer");
    drop((received, receiving));
    let receipt = drive_original_diagnostic_fixture(
        &mut fixture,
        &mut joint,
        clock.as_ref(),
        true,
        std::future::poll_fn(|cx| exit.poll_exit(cx)),
    )
    .await
    .unwrap();
    assert!(*installed.fixture_exit.observe_actual_exit().borrow());
    assert_eq!(peer.single_root_result_progress(), (true, 1));
    drop((
        receipt, exit, installed, logical, output, cohort, window, capacity,
    ));
    drive_original_diagnostic_fixture(
        &mut fixture,
        &mut joint,
        clock.as_ref(),
        true,
        supervisor.shutdown_until(Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
    drop(supervisor);
    process.close_original_host_registration_for_fixture();
    fixture.finish().await;
    drop(fixture);
    original_business.release();
    crate::query_execution::native_execution_adapter::source_cohort::tests::drive(&mut joint);
    tokio::time::timeout(Duration::from_secs(5), original_scope.wait_released())
        .await
        .unwrap();
    drop(original_scope);
    workload.close_admission();
    crate::query_execution::native_execution_adapter::source_cohort::tests::drive(&mut joint);
    assert!(workload.shutdown().is_ok());
    let until = Instant::now() + Duration::from_secs(5);
    data.drain_original_apply_until(until).await.unwrap();
    data.drain_native_outgoing_until(until).await.unwrap();
    assert!(peer.settle().await);
    assert!(process.original_registries_empty_for_fixture());
}
