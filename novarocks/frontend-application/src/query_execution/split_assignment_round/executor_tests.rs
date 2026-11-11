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
use crate::query_execution::connector_domain::TaskUpdateRequest;
use crate::query_execution::split_assignment::{
    TaskUpdateOutcome, TaskUpdateTicket, TaskUpdateTransportError,
};
use crate::task_execution::blocking_io::tests::{OriginalExecutorsFixture, admitted, until};
use novarocks_attempt_protocol::{
    failure::{AttemptFailure, AttemptFailureSite, ProtocolFailure},
    input::{AttemptInput, ExecutorFact},
    publication::SplitDiagnosticObservation,
    start::PayloadRef,
};
use novarocks_control_runtime::{
    actor::{Actor, ActorConfig, ActorStep},
    actor_turn::ActorRuntime,
    causal_input::CausalInputObligation,
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyPhase},
    exit_work::{PanicSink, run_future},
    reported_work::ReportedWork,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::{ActorId, ConsumerId, Generation, InputId, ScopeId, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_spi::connector::read_stack::{
    ConnectorReadDynamicFilterSnapshot, ConnectorReadSplit, ConnectorReadSplitSource,
    ConnectorSplitBatch,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorReadWireEncoder};
use novarocks_types::{AttemptId, QueryId, UniqueId};
use std::{
    num::NonZeroU64,
    sync::{atomic::AtomicUsize, mpsc},
    time::{Duration, Instant},
};

struct NeverSend;
impl TaskUpdateTransport for NeverSend {
    fn begin(
        &self,
        _: QueryExecutionId,
        _: &AssignmentTarget,
        _: &TaskUpdateRequest,
    ) -> Result<TaskUpdateTicket, TaskUpdateTransportError> {
        unreachable!("the original enumeration fails before transport delivery")
    }
    fn poll(
        &self,
        _: TaskUpdateTicket,
        _: &RoundSplitAssignmentStop,
    ) -> Option<Result<TaskUpdateOutcome, TaskUpdateTransportError>> {
        unreachable!("the original enumeration never submitted an update")
    }
}
struct NeverEncode;
impl ConnectorReadWireEncoder for NeverEncode {
    fn owner(&self) -> &str {
        "executor-failure-fixture"
    }
    fn encode_relation_payload(
        &self,
        _: &novarocks_spi::connector::read_stack::ConnectorReadRelation,
    ) -> Result<
        novarocks_spi::connector::ConnectorReadRelationPayload,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        unreachable!()
    }
    fn encode_column_payload(
        &self,
        _: &novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
    ) -> Result<
        novarocks_spi::connector::ConnectorEncodedPayload,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        unreachable!()
    }
    fn encode_transaction_payload(
        &self,
        _: &novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
    ) -> Result<
        novarocks_spi::connector::ConnectorEncodedPayload,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        unreachable!()
    }
    fn encode_split_payload(
        &self,
        _: &ConnectorReadSplit,
    ) -> Result<
        novarocks_spi::connector::ConnectorReadSplitPayload,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        unreachable!()
    }
}
struct FailingEnumeration {
    enumerations: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    release: mpsc::Receiver<()>,
}
impl ConnectorReadSplitSource for FailingEnumeration {
    fn next_batch(
        &mut self,
        _: usize,
        _: &ConnectorReadDynamicFilterSnapshot,
    ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError> {
        self.enumerations.fetch_add(1, Ordering::SeqCst);
        Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "original enumeration cause",
        ))
    }
    fn is_finished(&self) -> bool {
        false
    }
    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        self.release
            .recv_timeout(Duration::from_secs(5))
            .expect("release original close even on assertion unwind");
        Ok(())
    }
}
impl Drop for FailingEnumeration {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Release(Option<mpsc::Sender<()>>);
impl Release {
    fn now(&mut self) {
        self.0.take().unwrap().send(()).unwrap();
    }
}
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Seen {
    Failure(AttemptFailure),
    Profile(SplitDiagnosticObservation),
    Finished,
}
#[derive(Debug)]
struct Recorder(Arc<Mutex<Vec<Seen>>>);
impl Actor for Recorder {
    type Input = AttemptInput;
    type Effect = ();
    fn timer_input(_: Tick) -> AttemptInput {
        unreachable!()
    }
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        value: AttemptInput,
        _: &mut ActorStep<'_, ()>,
    ) {
        let value = match value {
            AttemptInput::Executor(ExecutorFact::PumpFailed { failure, .. }) => {
                Seen::Failure(failure)
            }
            AttemptInput::Executor(ExecutorFact::SplitProfile { observation, .. }) => {
                Seen::Profile(observation)
            }
            AttemptInput::Executor(ExecutorFact::PumpFinished { .. }) => Seen::Finished,
            _ => unreachable!("fixture received unrelated protocol input"),
        };
        self.0.lock().unwrap().push(value);
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {
        unreachable!()
    }
}
fn project(error: &SplitAssignmentDriverError) -> AttemptFailure {
    let fact = TaskExecutionError::SplitAssignment(error.clone()).preparation_input();
    AttemptFailure::Protocol(ProtocolFailure {
        fact: fact.fact(),
        detail: fact.detail().clone(),
        site: AttemptFailureSite::Attempt,
    })
}
/// Actual original test Source: distinct admitted causal inputs, canonical
/// typed failure custody and actor receipts. It is not a production author.
struct PaidPublisher {
    failure: Mutex<CausalInputObligation<Recorder>>,
    profile: Mutex<CausalInputObligation<Recorder>>,
    finished: Mutex<CausalInputObligation<Recorder>>,
    cause: Mutex<Option<SplitAssignmentDriverError>>,
    result: Mutex<Option<TaskExecutionError>>,
    filled: Mutex<Vec<InputId>>,
    failure_calls: AtomicUsize,
    profile_calls: AtomicUsize,
    refuse_failure: bool,
    changed: Notify,
}
impl PaidPublisher {
    fn fill(
        &self,
        source: &Mutex<CausalInputObligation<Recorder>>,
        input: AttemptInput,
    ) -> Result<(), TaskExecutionError> {
        let (_, input) = source
            .lock()
            .unwrap()
            .fill_with_receipt(None, input)
            .map_err(|_| {
                TaskExecutionError::Schedule("original prepaid fixture input refused".into())
            })?;
        self.filled.lock().unwrap().push(input);
        Ok(())
    }
    fn finish(&self, original: TaskExecutionError) {
        let projection = original.preparation_input();
        let failure = AttemptFailure::Protocol(ProtocolFailure {
            fact: projection.fact(),
            detail: projection.detail().clone(),
            site: AttemptFailureSite::Attempt,
        });
        assert!(self.result.lock().unwrap().replace(original).is_none());
        self.fill(
            &self.finished,
            AttemptInput::Executor(ExecutorFact::PumpFinished {
                pump: PayloadRef(NonZeroU64::new(1).unwrap()),
                result: Err(failure),
            }),
        )
        .unwrap();
    }
}
impl OriginalSplitPublisher for PaidPublisher {
    fn publish_original_failure(
        &self,
        failure: SplitAssignmentDriverError,
    ) -> Result<(), TaskExecutionError> {
        assert_eq!(
            self.failure_calls.fetch_add(1, Ordering::SeqCst),
            0,
            "one original failure handoff attempt"
        );
        let projected = project(&failure);
        assert!(self.cause.lock().unwrap().replace(failure).is_none());
        self.changed.notify_one();
        if self.refuse_failure {
            return Err(TaskExecutionError::Schedule(
                "original failure publication refused".into(),
            ));
        }
        self.fill(
            &self.failure,
            AttemptInput::Executor(ExecutorFact::PumpFailed {
                pump: PayloadRef(NonZeroU64::new(1).unwrap()),
                failure: projected,
            }),
        )
    }
    fn publish_original_profile(
        &self,
        observation: SplitAssignmentProfileRead,
    ) -> Result<(), TaskExecutionError> {
        self.profile_calls.fetch_add(1, Ordering::SeqCst);
        assert!(matches!(observation, SplitAssignmentProfileRead::Failed));
        self.fill(
            &self.profile,
            AttemptInput::Executor(ExecutorFact::SplitProfile {
                pump: PayloadRef(NonZeroU64::new(1).unwrap()),
                observation: SplitDiagnosticObservation::Failed,
            }),
        )
    }
}

async fn run_case(refuse_failure: bool, check_idle_timer: bool) {
    let data_runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let supervisor = data_runtime.connector_blocking_io().clone();
    let mut workers = OriginalExecutorsFixture::install(&supervisor);
    let (control, root, window) = admitted();
    let responsibility =
        ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias())
            .unwrap();
    let backing = responsibility.retain_backing();
    let enumerations = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let (release, released) = mpsc::channel();
    let mut release = Release(Some(release));
    let execution =
        QueryExecutionId::new(QueryId::new(19, 23), AttemptId::new(1).unwrap()).unwrap();
    let scan = ScanNodeKey::new(FragmentId::from(1u32), 7);
    let source = RoundSplitSource {
        scan,
        source: SupervisedSplitSource::new(
            Box::new(FailingEnumeration {
                enumerations: enumerations.clone(),
                closes: closes.clone(),
                drops: drops.clone(),
                release: released,
            }),
            7,
            responsibility,
        ),
        encoder: Arc::new(NeverEncode),
        feedback: Arc::new(
            crate::runtime_filter::feedback::RuntimeFilterFeedbackState::new(
                execution,
                Default::default(),
            )
            .unwrap(),
        ),
        feedback_bindings: Vec::new(),
        initial_wait_initialized: false,
        initial_wait_deadline: None,
        _backing: Some(backing),
    };
    let plan = RoundSplitAssignmentPlan::new(
        BTreeMap::from([(
            scan,
            vec![AssignmentTarget {
                backend_idx: 0,
                task_key: UniqueId::new(1, 1),
            }],
        )]),
        vec![source],
        TaskUpdateRetryPolicy::default(),
        Duration::ZERO,
        supervisor,
    );
    let executor = SplitAssignmentExecutor::prepare(
        execution,
        plan,
        Arc::new(NeverSend),
        data_runtime.clone(),
    )
    .unwrap();
    if check_idle_timer {
        let now = Instant::now();
        {
            let mut original = executor.pump.lock().unwrap();
            let pump = original.as_mut().unwrap();
            pump.arm_wake(now + Duration::from_secs(2));
            pump.arm_wake(now + Duration::from_millis(20));
        }
        tokio::time::timeout(Duration::from_secs(1), executor.changed.notified())
            .await
            .expect("the original timer wakes the executor without another pump turn");
    }
    let ledger = RuntimeLedger::new(ResourceVector::default());
    let scope: ScopeHandle = ledger
        .open_scope_next(None, Generation::new(1).unwrap())
        .unwrap();
    let (mut source_work, mut source_capacity) = scope
        .reserve_next(
            Generation::new(1).unwrap(),
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
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runtime = ActorRuntime::new();
    let actor = ActorId::new(1).unwrap();
    let registration = runtime
        .register_bound(
            actor,
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
    let input = |id, class| {
        Mutex::new(
            CausalInputObligation::reserve_admitted(
                &runtime,
                &registration,
                ActorId::new(id).unwrap(),
                class,
                &source_work,
            )
            .unwrap(),
        )
    };
    let publisher = Arc::new(PaidPublisher {
        failure: input(2, InputClass::Lifecycle),
        profile: input(3, InputClass::Ordinary),
        finished: input(4, InputClass::Lifecycle),
        cause: Mutex::new(None),
        result: Mutex::new(None),
        filled: Mutex::new(Vec::new()),
        failure_calls: AtomicUsize::new(0),
        profile_calls: AtomicUsize::new(0),
        refuse_failure,
        changed: Notify::new(),
    });
    let (work, mut parcel) = scope
        .reserve_next(
            Generation::new(1).unwrap(),
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
            WorkAdmission::Derived(source_work.id()),
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let consumer = ConsumerId::new(1).unwrap();
    let report = ReportSlot::reserve_next(
        &mut parcel,
        Generation::new(1).unwrap(),
        1,
        [(consumer, ConsumerKind::Observer)],
    )
    .unwrap();
    let (custody, exit) =
        ExitCustody::from_admitted(ReportedWork::try_new(work, parcel, report.clone()).unwrap());
    let runner = executor.clone();
    let publishing = publisher.clone();
    let job = tokio::spawn(run_future(
        exit,
        async move { runner.run_admitted(publishing.as_ref()).await },
        PanicSink::new(),
    ));
    tokio::time::timeout(Duration::from_secs(2), publisher.changed.notified())
        .await
        .expect("original failure must be published before close joins");
    until(|| closes.load(Ordering::SeqCst) == 1);
    assert_eq!(enumerations.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        custody.phase(),
        ExitCustodyPhase::Started,
        "failure handoff is not physical runner exit"
    );
    assert!(!job.is_finished());
    assert!(matches!(
        executor.profile_source().current(),
        SplitAssignmentProfileRead::Pending
    ));
    assert_eq!(
        publisher.profile_calls.load(Ordering::SeqCst),
        0,
        "profile/result visibility is independent"
    );
    let original = executor.failure_source().failure().unwrap();
    assert_eq!(*publisher.cause.lock().unwrap(), Some(original.clone()));
    assert!(matches!(
        original,
        SplitAssignmentDriverError::SourceEnumeration { .. }
    ));
    assert!(executor.outcome.lock().unwrap().is_none());
    runtime.poll_one(Tick(1)).unwrap();
    if refuse_failure {
        assert!(seen.lock().unwrap().is_empty());
    } else {
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[Seen::Failure(project(&original))]
        );
    }
    // Extra real progress notifications while the physical close is parked
    // must not republish the same primary, including a refused first handoff.
    for _ in 0..3 {
        executor.changed.notify_one();
        tokio::task::yield_now().await;
    }
    assert_eq!(publisher.failure_calls.load(Ordering::SeqCst), 1);
    assert_eq!(publisher.profile_calls.load(Ordering::SeqCst), 0);
    assert!(!job.is_finished());
    release.now();
    let outcome = tokio::time::timeout(Duration::from_secs(2), job)
        .await
        .expect("same close really exits")
        .unwrap()
        .expect("original credentialed future exits normally");
    let primary = match outcome {
        Err(error) => error,
        Ok(_) => panic!("failing original source became successful"),
    };
    match &primary {
        TaskExecutionError::Schedule(message) if refuse_failure => {
            assert_eq!(message, "original failure publication refused")
        }
        TaskExecutionError::SplitAssignment(failure) if !refuse_failure => {
            assert_eq!(failure, &original)
        }
        _ => panic!("original primary result was replaced"),
    }
    assert_eq!(custody.phase(), ExitCustodyPhase::Exited);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(publisher.failure_calls.load(Ordering::SeqCst), 1);
    assert_eq!(publisher.profile_calls.load(Ordering::SeqCst), 1);
    assert_eq!(*publisher.cause.lock().unwrap(), Some(original));
    publisher.finish(primary);
    while runtime.poll_one(Tick(1)).unwrap() {}
    for input in std::mem::take(&mut *publisher.filled.lock().unwrap()) {
        runtime.retire_input(actor, input).unwrap();
    }
    for input in [&publisher.failure, &publisher.profile, &publisher.finished] {
        input.lock().unwrap().close_after_exit().unwrap();
    }
    assert!(
        seen.lock()
            .unwrap()
            .contains(&Seen::Profile(SplitDiagnosticObservation::Failed))
    );
    assert!(seen.lock().unwrap().contains(&Seen::Finished));
    report.consume(consumer).unwrap();
    assert!(custody.try_retire().unwrap());
    drop(publisher);
    drop(executor);
    drop(data_runtime);
    // The same genuinely exited derived runner/causal headers must reap before
    // their original parent Source can retire. A report is not header reaping.
    ledger.reap_scope_ready(scope.id(), 32).unwrap();
    source_work.body_exited().unwrap();
    source_work.captures_exited().unwrap();
    source_capacity.finish().unwrap();
    ledger.reap_scope_ready(scope.id(), 32).unwrap();
    runtime.close_actor_admission(actor).unwrap();
    while let Some(turn) = runtime.claim_host_actor() {
        turn.release(false).unwrap();
    }
    runtime.seal_actor(actor).unwrap();
    scope.close_external().unwrap();
    runtime.remove_actor(actor).unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
    root.owner.complete();
    root.business.release();
    drop(window);
    until(|| control.snapshot().scopes.is_empty());
    control.close_admission();
    assert!(control.shutdown().is_ok());
    workers.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_failure_reaches_paid_source_while_close_profile_finish_and_exit_are_pending() {
    run_case(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_failure_publication_is_primary_and_never_retried_while_original_close_is_pending()
{
    run_case(true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_timer_wakes_the_original_executor_without_an_external_event() {
    run_case(false, true).await;
}
