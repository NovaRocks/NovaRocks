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
