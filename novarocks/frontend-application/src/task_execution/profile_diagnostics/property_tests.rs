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

//! Actual final-info Source/job custody; eligibility stays in the AP Core.

use crate::{
    control_runtime::transport_withdrawal::AttemptEffectEnvelope,
    native::task_result_transport::{FinalTaskInfoRead, TaskResultTransport},
    task_execution::host::{
        custody::{TransportRetirements, tests::WithdrawalInbox},
        pumps::{AttemptPumpSource, DiagnosticRecords},
        transport::handoff_actor_transport,
    },
};
use futures::future::BoxFuture;
use novarocks_attempt_protocol::{
    effect::{AttemptEffect, EffectBody, EffectClass},
    input::{DiagnosticRead, ExecutorFact, FinalTaskInfoRef},
};
use novarocks_control_runtime::{
    obligation::{ReservationObligation, WorkObligation},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::{ConsumerId, Generation},
    scope::{WorkAdmission, WorkKind},
};
use novarocks_coordination_contract::{AttemptActionId, AttemptRequestToken, MonotonicInstant};
use novarocks_execution_contract::{OperationOutcome, TaskIdentity};
use std::{
    collections::{BTreeSet, VecDeque},
    future::poll_fn,
    mem::ManuallyDrop,
    num::NonZeroU64,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

#[derive(Default)]
struct FinalReadProbe {
    started: Mutex<Vec<TaskIdentity>>,
    completed: Mutex<Vec<TaskIdentity>>,
    dropped: Mutex<Vec<TaskIdentity>>,
}
struct FinalReadDrop {
    identity: TaskIdentity,
    probe: Arc<FinalReadProbe>,
}
impl Drop for FinalReadDrop {
    fn drop(&mut self) {
        self.probe.dropped.lock().unwrap().push(self.identity);
    }
}

struct GatedFinalInfo {
    release: Mutex<VecDeque<tokio::sync::oneshot::Receiver<()>>>,
    probe: Arc<FinalReadProbe>,
}
impl TaskResultTransport for GatedFinalInfo {
    fn final_task_info(
        &self,
        identity: TaskIdentity,
    ) -> BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
        // Taking the one actual receiver detects a duplicate read even if
        // a previous borrowed wait was cancelled and rebuilt.
        let release = self
            .release
            .lock()
            .unwrap()
            .pop_front()
            .expect("one receiver for each original read");
        self.probe.started.lock().unwrap().push(identity);
        let actual_drop = FinalReadDrop {
            identity,
            probe: Arc::clone(&self.probe),
        };
        Box::pin(async move {
            let _actual_drop = actual_drop;
            release
                .await
                .map_err(|_| "the fixture release gate closed".to_owned())?;
            self.probe.completed.lock().unwrap().push(identity);
            // Missing optional diagnostics must not be fabricated as a
            // complete final-info value for this or any unread Task.
            Ok(FinalTaskInfoRead::Unavailable(OperationOutcome::Gone))
        })
    }
}

fn tasks() -> Vec<TaskIdentity> {
    // Same real frozen graph identities; no TaskRound or observer owns them.
    let graph = crate::task_execution::graph_manifest_tests::unstarted_chain_graph();
    let tasks = graph
        .tasks()
        .map(|task| task.identity())
        .collect::<Vec<_>>();
    assert!(
        tasks.len() > 1,
        "the real graph leaves an unread original sibling"
    );
    tasks
}
fn request(index: usize) -> AttemptRequestToken {
    AttemptRequestToken::new(NonZeroU64::new(index as u64 + 1).unwrap())
}
fn effect(index: usize, task: TaskIdentity) -> Arc<AttemptEffectEnvelope> {
    AttemptEffectEnvelope::new(AttemptEffect {
        id: AttemptActionId::new(NonZeroU64::new(index as u64 + 1).unwrap()),
        class: EffectClass::Settlement,
        body: EffectBody::CollectDiagnostics {
            request: request(index),
            task,
            deadline: MonotonicInstant::from_origin(Duration::from_secs(1)),
        },
    })
}
struct SourceFixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    parent: WorkObligation,
    payer: ReservationObligation,
    inbox: WithdrawalInbox,
    source: AttemptPumpSource,
    retirement: TransportRetirements,
    records: Arc<DiagnosticRecords>,
    closing: bool,
    replies: Vec<ExecutorFact>,
    actual_notified_visits: usize,
}
impl SourceFixture {
    fn new(transport: Arc<dyn TaskResultTransport>) -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let (mut parent, payer) = scope
            .reserve_next(
                generation,
                ReservationResources::work_only(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        parent.start().unwrap();
        let inbox = WithdrawalInbox::with_commands(&scope, [], [16; 4]);
        let records = DiagnosticRecords::new(transport);
        let source = AttemptPumpSource::bind(
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
        let retirement = TransportRetirements::new(
            inbox.runtime().clone(),
            inbox.registration().clone(),
            tokio::runtime::Handle::current(),
            source.capacity(),
        );
        Self {
            ledger,
            scope,
            parent,
            payer,
            inbox,
            source,
            retirement,
            records,
            closing: false,
            replies: Vec::new(),
            actual_notified_visits: 0,
        }
    }
    fn issue(&mut self, original: Arc<AttemptEffectEnvelope>) {
        let position = self
            .source
            .prepay(&original)
            .unwrap()
            .expect("same paid original read role");
        self.inbox.push_transport_command(original.clone(), None);
        let ticket = self.inbox.command(&self.parent, original.protocol.id);
        let command = ticket.input;
        let mut job = self.source.bind_job(&ticket, position);
        self.retirement
            .track(original, job.transport.take().unwrap())
            .unwrap_or_else(|_| panic!("same original diagnostic report/Join"));
        handoff_actor_transport(&tokio::runtime::Handle::current(), job)
            .unwrap_or_else(|_| panic!("same original diagnostic handoff"));
        self.inbox.runtime().effect_completed(ticket).unwrap();
        self.inbox
            .runtime()
            .retire_input(self.inbox.registration().actor(), command)
            .unwrap();
    }
    fn close_starting(&mut self) {
        if !self.closing {
            self.closing = true;
            // An actual external close owes a finite original Source pass.
            self.inbox
                .runtime()
                .notify_host_actor(self.inbox.registration())
                .unwrap();
        }
    }
    fn progress(&mut self) {
        let mut visits = 0;
        while let Some(turn) = self.inbox.runtime().claim_host_actor() {
            visits += 1;
            assert!(visits <= 1024, "an idle Source must not self-wake forever");
            self.actual_notified_visits += usize::from(turn.was_notified());
            let source = &mut self.source;
            let transport = self.retirement.progress(&turn, 1, |effect, event| {
                source
                    .transport_event(effect, event)
                    .unwrap_or_else(|original| std::panic::resume_unwind(original))
            });
            let source = self
                .source
                .progress(false, self.closing, self.closing, 1, true, drop);
            turn.release(transport.more || source.more).unwrap();
        }
    }
    fn consume(&mut self) {
        while let Some((input, fact)) = self.inbox.try_commit_next() {
            assert!(
                matches!(
                    fact,
                    ExecutorFact::ActualExit { .. }
                        | ExecutorFact::DiagnosticsFinished {
                            result: DiagnosticRead::Unavailable(OperationOutcome::Gone),
                            ..
                        }
                ),
                "only actual exit and the actual unavailable read result are delivered"
            );
            self.source.core_consumed(self.inbox.runtime(), &[input]);
            self.inbox
                .runtime()
                .retire_input(self.inbox.registration().actor(), input)
                .unwrap();
            self.replies.push(fact);
        }
    }
    fn result_count(&self) -> usize {
        self.replies
            .iter()
            .filter(|fact| matches!(fact, ExecutorFact::DiagnosticsFinished { .. }))
            .count()
    }
    async fn until(&mut self, mut predicate: impl FnMut(&Self) -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                self.progress();
                self.consume();
                self.progress();
                if predicate(self) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual original diagnostic cleanup must finish");
    }
    async fn finish(mut self) {
        self.close_starting();
        self.until(|fixture| fixture.retirement.is_empty() && fixture.source.is_drained())
            .await;
        let original_parent_header = self.parent.retirement_observer();
        assert!(
            !original_parent_header.is_retired(),
            "read completion does not retire its original Source parent"
        );
        self.parent.body_exited().unwrap();
        self.parent.captures_exited().unwrap();
        self.payer.finish().unwrap();
        self.scope.close_external().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 32).unwrap();
        assert!(original_parent_header.is_retired());
        assert!(self.scope.join_ready().unwrap());
        self.inbox
            .runtime()
            .close_actor_admission(self.inbox.registration().actor())
            .unwrap();
        while let Some(turn) = self.inbox.runtime().claim_host_actor() {
            turn.release(false).unwrap();
        }
        self.inbox
            .runtime()
            .seal_actor(self.inbox.registration().actor())
            .unwrap();
        self.inbox.finish();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}

#[tokio::test]
async fn pending_final_info_survives_borrowed_wait_cancel_and_stop_starting_until_same_future_drops()
 {
    let identities = tasks();
    let (release_read, read_gate) = tokio::sync::oneshot::channel();
    let probe = Arc::new(FinalReadProbe::default());
    let transport: Arc<dyn TaskResultTransport> = Arc::new(GatedFinalInfo {
        release: Mutex::new(VecDeque::from([read_gate])),
        probe: probe.clone(),
    });
    // On a failed assertion retain the original unproven-exit custody instead
    // of unwinding through a second obligation Drop. Normal cleanup consumes it.
    let mut fixture = ManuallyDrop::new(SourceFixture::new(transport));
    fixture.issue(effect(0, identities[0]));
    fixture
        .until(|_| probe.started.lock().unwrap().len() == 1)
        .await;
    let mut borrowed = Box::pin(poll_fn(|_| {
        fixture.progress();
        if fixture.source.is_drained() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }));
    assert!(futures::poll!(borrowed.as_mut()).is_pending());
    drop(borrowed);
    assert_eq!(*probe.started.lock().unwrap(), [identities[0]]);
    assert!(probe.completed.lock().unwrap().is_empty());
    assert!(
        probe.dropped.lock().unwrap().is_empty(),
        "borrowing wait does not own the read"
    );
    assert!(!fixture.retirement.is_empty() && !fixture.source.is_drained());

    fixture.close_starting();
    let mut stopped_wait = Box::pin(poll_fn(|_| {
        fixture.progress();
        if fixture.source.is_drained() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }));
    assert!(futures::poll!(stopped_wait.as_mut()).is_pending());
    drop(stopped_wait);
    assert!(
        fixture
            .source
            .prepay(&effect(1, identities[1]))
            .unwrap()
            .is_none(),
        "closing may not issue the unread original sibling"
    );
    assert_eq!(*probe.started.lock().unwrap(), [identities[0]]);
    assert!(
        probe.dropped.lock().unwrap().is_empty(),
        "stop-starting cannot destroy an issued F"
    );
    assert!(!fixture.source.is_drained());
    release_read.send(()).unwrap();
    fixture
        .until(|fixture| fixture.retirement.is_empty() && fixture.source.is_drained())
        .await;
    assert_eq!(*probe.completed.lock().unwrap(), [identities[0]]);
    assert_eq!(
        *probe.dropped.lock().unwrap(),
        [identities[0]],
        "same original F really drops once"
    );
    assert_eq!(
        *probe.started.lock().unwrap(),
        [identities[0]],
        "unread sibling never starts"
    );
    assert_eq!(fixture.result_count(), 1);
    assert_eq!(fixture.replies.len(), 2);
    assert!(matches!(fixture.replies[0], ExecutorFact::ActualExit { effect } if effect.get() == 1));
    assert!(
        matches!(fixture.replies[1], ExecutorFact::DiagnosticsFinished { request: actual, task, .. }
        if actual == request(0) && task == identities[0])
    );
    assert!(
        fixture
            .replies
            .iter()
            .any(|fact| matches!(fact, ExecutorFact::ActualExit { effect } if effect.get() == 1))
    );
    for _ in 0..3 {
        fixture.progress();
        fixture.consume();
    }
    assert_eq!(*probe.started.lock().unwrap(), [identities[0]]);
    assert!(
        fixture
            .records
            .take_information(FinalTaskInfoRef::new(request(0), identities[0]))
            .is_err()
    );
    ManuallyDrop::into_inner(fixture).finish().await;
}

#[tokio::test]
async fn issued_final_info_reads_keep_each_original_future_and_resume_on_actual_host_progress() {
    // AP Core tests own first-terminal eligibility/order and no duplicate
    // candidates. Here each eligible effect uses the SAME actual Source.
    let identities = tasks();
    assert_eq!(
        identities.iter().copied().collect::<BTreeSet<_>>().len(),
        identities.len()
    );
    let mut releases = VecDeque::new();
    let mut gates = VecDeque::new();
    for _ in &identities {
        let (release, gate) = tokio::sync::oneshot::channel();
        releases.push_back(release);
        gates.push_back(gate);
    }
    let probe = Arc::new(FinalReadProbe::default());
    let transport: Arc<dyn TaskResultTransport> = Arc::new(GatedFinalInfo {
        release: Mutex::new(gates),
        probe: probe.clone(),
    });
    let mut fixture = ManuallyDrop::new(SourceFixture::new(transport));
    for (index, identity) in identities.iter().enumerate() {
        fixture.issue(effect(index, *identity));
        fixture
            .until(|_| probe.started.lock().unwrap().len() == index + 1)
            .await;
        assert_eq!(*probe.started.lock().unwrap(), identities[..=index]);
        assert_eq!(*probe.completed.lock().unwrap(), identities[..index]);
        for _ in 0..3 {
            fixture.progress();
            fixture.consume();
        }
        assert_eq!(
            *probe.started.lock().unwrap(),
            identities[..=index],
            "no repeated read factory"
        );
        assert_eq!(probe.dropped.lock().unwrap().len(), index);
        let original_notifications = fixture.actual_notified_visits;
        releases.pop_front().unwrap().send(()).unwrap();
        fixture
            .until(|fixture| fixture.result_count() == index + 1 && fixture.retirement.is_empty())
            .await;
        assert!(
            fixture.actual_notified_visits > original_notifications,
            "actual original completion/Join wakes the original Host; no test notification supplies a successor"
        );
        assert_eq!(*probe.completed.lock().unwrap(), identities[..=index]);
        assert_eq!(*probe.dropped.lock().unwrap(), identities[..=index]);
        assert_eq!(fixture.replies.len(), 2 * (index + 1));
        assert!(
            matches!(fixture.replies[2 * index], ExecutorFact::ActualExit { effect }
            if effect.get() == index as u64 + 1)
        );
        assert!(
            matches!(fixture.replies[2 * index + 1], ExecutorFact::DiagnosticsFinished { request: actual, task, .. }
            if actual == request(index) && task == *identity)
        );
        assert!(
            fixture
                .records
                .take_information(FinalTaskInfoRef::new(request(index), *identity))
                .is_err()
        );
    }
    let actual = fixture
        .replies
        .iter()
        .filter_map(|fact| match fact {
            ExecutorFact::DiagnosticsFinished {
                request,
                task,
                result: DiagnosticRead::Unavailable(OperationOutcome::Gone),
            } => Some((*request, *task)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        identities
            .iter()
            .enumerate()
            .map(|(i, task)| (request(i), *task))
            .collect::<Vec<_>>()
    );
    for _ in 0..3 {
        fixture.progress();
        fixture.consume();
    }
    assert_eq!(
        *probe.started.lock().unwrap(),
        identities,
        "finished idle turns cannot create more reads"
    );
    ManuallyDrop::into_inner(fixture).finish().await;
}
