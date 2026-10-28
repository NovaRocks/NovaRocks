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

mod source_storage;

use super::*;
use bytes::Bytes;
use novarocks_control_runtime::transaction::RuntimeLedger;
use novarocks_control_runtime_core::{
    completion::ConsumerKind,
    identity::{ConsumerId, Generation},
};
use novarocks_execution_contract::{CreationContent, FrozenBytes};
use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId};
use std::sync::atomic::{AtomicUsize, Ordering};

fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn core() -> ConsumerId {
    ConsumerId::new(1).unwrap()
}
fn observer() -> ConsumerId {
    ConsumerId::new(2).unwrap()
}
fn identity(n: u32) -> TaskIdentity {
    TaskIdentity::new(
        QueryExecutionId::new(QueryId::new(7401, 7402), AttemptId::new(1).unwrap()).unwrap(),
        StageId::new(1).unwrap(),
        TaskId::new(n).unwrap(),
        BackendProcessId::new_v7(),
    )
}
struct Pending;
impl TaskPendingNotice for Pending {
    fn pending(&self) {}
}
#[derive(Debug)]
struct Assignment(Arc<AtomicUsize>);
impl Drop for Assignment {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl CreationContent for Assignment {
    fn encoded_len(&self) -> usize {
        1
    }
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}
fn input(drops: Arc<AtomicUsize>) -> TaskCreationInput {
    TaskCreationInput::new(
        FrozenBytes::freeze(Bytes::from_static(b"actual-plan")),
        Box::new(Assignment(drops)),
    )
}
struct Fixture {
    ledger: RuntimeLedger,
    scopes: Vec<ScopeHandle>,
}
impl Fixture {
    fn new() -> Self {
        Self {
            ledger: RuntimeLedger::new(
                ResourceVector::new([
                    (Dimension::PreparingTasks.id(), 4),
                    (Dimension::PreparingBytes.id(), 40),
                    (Dimension::InboundRecords.id(), 4),
                    (Dimension::InboundBytes.id(), 36),
                ])
                .unwrap(),
                32,
                4,
            ),
            scopes: Vec::new(),
        }
    }
    fn scope(&mut self) -> ScopeHandle {
        let scope = self
            .ledger
            .open_scope_next(None, generation(), 16, 16)
            .unwrap();
        self.scopes.push(scope.clone());
        scope
    }
    fn admitted(
        &self,
        scope: &ScopeHandle,
        n: u32,
        input: TaskCreationInput,
    ) -> (PreparationCustody, PreparationJob) {
        let (work, capacity) = scope
            .reserve_next(
                generation(),
                ResourceVector::new([
                    (Dimension::PreparingTasks.id(), 1),
                    (Dimension::PreparingBytes.id(), 10),
                    (Dimension::InboundRecords.id(), 1),
                    (Dimension::InboundBytes.id(), 9),
                ])
                .unwrap(),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let report = || {
            ReportSlot::reserve_next(
                scope,
                generation(),
                work.id(),
                2,
                [
                    (
                        core(),
                        ConsumerKind::Owner {
                            scope: scope.id(),
                            generation: generation(),
                        },
                    ),
                    (observer(), ConsumerKind::Observer),
                ],
            )
            .unwrap()
        };
        let reports = [report(), report()];
        PreparationCustody::from_admitted(
            PreparationBinding {
                stop: crate::task_port::TaskStopSignal::initial(identity(n)),
                pending: Arc::new(Pending),
                health: Arc::new(ExecutionHealth::default()),
            },
            work,
            capacity,
            reports,
            input,
        )
    }
    fn release(&self, owner: &PreparationCustody) {
        assert!(owner.host_borrowers_exited());
        assert!(owner.publish_whole_exit().unwrap());
        for report in [owner.body_report(), owner.exit_report()] {
            report.consume(core()).unwrap();
            report.consume(observer()).unwrap();
        }
        let mut bundle = owner
            .take_after_exit()
            .unwrap()
            .expect("whole exited original bundle");
        assert_eq!(
            bundle
                .resources()
                .unwrap()
                .get(Dimension::PreparingTasks.id()),
            0
        );
        let resources = bundle.resources().unwrap();
        bundle.release(&resources).unwrap();
    }
    fn finish(self) {
        for scope in self.scopes {
            self.ledger.reap_scope_ready(scope.id(), 16).unwrap();
            scope.close_external().unwrap();
            self.ledger.remove_scope(scope.id()).unwrap();
        }
        assert!(self.ledger.is_empty());
    }
}
#[test]
fn body_slot_reuse_does_not_return_preparation_or_retained_close_capacity() {
    let mut fixture = Fixture::new();
    let a = fixture.scope();
    let b = fixture.scope();
    let drops = Arc::new(AtomicUsize::new(0));
    let (owner_a, job_a) = fixture.admitted(&a, 1, input(drops.clone()));
    let (owner_b, job_b) = fixture.admitted(&b, 2, input(drops.clone()));
    // Work IDs are scope-local; both original queue entries deliberately use 1.
    assert_eq!(job_a.key().1, job_b.key().1);
    let original = owner_a
        .0
        .state
        .lock()
        .unwrap()
        .capacity
        .as_ref()
        .unwrap()
        .id();
    let queue = PreparationQueue::new(2, 2, 1, 1);
    queue
        .enqueue(job_a)
        .unwrap_or_else(|_| panic!("first Accepted enqueue"));
    queue
        .enqueue(job_b)
        .unwrap_or_else(|_| panic!("second Accepted enqueue"));
    let running = queue.pop().unwrap();
    assert!(queue.pop().is_none());
    let mut child = match running.run(|input, parent| {
        drop(input);
        let (mut child, capacity) = parent
            .derive(WorkKind::Cleanup)
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        child.start().unwrap();
        (child, capacity)
    }) {
        PreparationOutcome::Returned(child) => child,
        _ => panic!("actual first body"),
    };
    assert!(!owner_a.publish_whole_exit().unwrap());
    assert_eq!(fixture.ledger.used(Dimension::PreparingTasks.id()), 2);
    assert!(matches!(
        queue.pop().unwrap().run(|input, _| drop(input)),
        PreparationOutcome::Returned(())
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert!(owner_b.publish_whole_exit().unwrap());
    assert_eq!(fixture.ledger.used(Dimension::PreparingBytes.id()), 20);
    child.0.body_exited().unwrap();
    child.0.captures_exited().unwrap();
    let resources = child.1.resources().unwrap();
    child.1.release(&resources).unwrap();
    fixture.ledger.reap_scope_ready(a.id(), 16).unwrap();
    assert!(owner_a.publish_whole_exit().unwrap());
    owner_a.body_report().consume(core()).unwrap();
    owner_a.body_report().consume(observer()).unwrap();
    owner_a.exit_report().consume(core()).unwrap();
    assert!(owner_a.take_after_exit().unwrap().is_none());
    assert_eq!(fixture.ledger.used(Dimension::PreparingTasks.id()), 2);
    owner_a.exit_report().consume(observer()).unwrap();
    let mut retained = owner_a.take_after_exit().unwrap().unwrap();
    assert_eq!(retained.id(), original);
    assert_eq!(fixture.ledger.used(Dimension::PreparingTasks.id()), 1);
    assert_eq!(fixture.ledger.used(Dimension::InboundBytes.id()), 18);
    assert!(owner_a.take_after_exit().unwrap().is_none());
    fixture.release(&owner_b);
    assert_eq!(fixture.ledger.used(Dimension::InboundBytes.id()), 9);
    let resources = retained.resources().unwrap();
    retained.release(&resources).unwrap();
    fixture.finish();
}

#[derive(Debug)]
struct ReentrantDrop {
    queue: std::sync::Weak<Queue>,
    drops: Arc<AtomicUsize>,
}
impl CreationContent for ReentrantDrop {
    fn encoded_len(&self) -> usize {
        1
    }
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}
impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        let queue = self.queue.upgrade().unwrap();
        assert!(
            queue.state.try_lock().is_ok(),
            "opaque input disposal must not hold the queue lock"
        );
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn queued_cancel_and_refused_start_dispose_the_original_input_outside_queue_locks() {
    let mut fixture = Fixture::new();
    let scope = fixture.scope();
    let queue = PreparationQueue::new(1, 2, 2, 1);
    let drops = Arc::new(AtomicUsize::new(0));
    let create_input = || {
        TaskCreationInput::new(
            FrozenBytes::freeze(Bytes::new()),
            Box::new(ReentrantDrop {
                queue: Arc::downgrade(&queue.0),
                drops: drops.clone(),
            }),
        )
    };
    let (a, job_a) = fixture.admitted(&scope, 1, create_input());
    let (b, job_b) = fixture.admitted(&scope, 2, create_input());
    let key = job_a.key();
    queue
        .enqueue(job_a)
        .unwrap_or_else(|_| panic!("first queue"));
    queue
        .enqueue(job_b)
        .unwrap_or_else(|_| panic!("second queue"));
    let canceled = queue.cancel(key.0, key.1).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(!a.publish_whole_exit().unwrap());
    assert!(canceled.dispose());
    scope.request_stop().unwrap();
    match queue
        .pop()
        .unwrap()
        .run(|_, _| panic!("stopped queued work cannot start"))
    {
        PreparationOutcome::StartRefused(RuntimeError::Scope(
            novarocks_control_runtime_core::scope::ScopeError::Stopped,
        )) => {}
        _ => panic!("actual current start fence"),
    }
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    fixture.release(&a);
    fixture.release(&b);
    fixture.finish();
}

#[test]
fn lost_job_authority_keeps_its_actual_input_work_and_charges_unresolved() {
    let mut fixture = Fixture::new();
    let scope = fixture.scope();
    let drops = Arc::new(AtomicUsize::new(0));
    let (owner, job) = fixture.admitted(&scope, 1, input(drops.clone()));
    drop(job);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.ledger.unresolved_work(), vec![owner.key()]);
    assert_eq!(fixture.ledger.used(Dimension::PreparingBytes.id()), 10);
    assert!(!owner.publish_whole_exit().unwrap());
    assert!(owner.take_after_exit().unwrap().is_none());
    assert!(scope.close_external().is_ok());
    assert!(!scope.join_ready().unwrap());
    // Deliberately unresolved physical custody retains itself; no fictitious
    // cleanup/exit is introduced to make teardown report empty.
}

#[derive(Debug)]
struct PanicGraph(Arc<AtomicUsize>);
impl Drop for PanicGraph {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct PanickingInput(Arc<AtomicUsize>);
impl CreationContent for PanickingInput {
    fn encoded_len(&self) -> usize {
        1
    }
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}
impl Drop for PanickingInput {
    fn drop(&mut self) {
        std::panic::panic_any(PanicGraph(self.0.clone()));
    }
}
#[test]
fn input_destructor_panic_retains_unknown_work_and_payload_while_other_context_progresses() {
    let mut fixture = Fixture::new();
    let a = fixture.scope();
    let b = fixture.scope();
    let payload_drops = Arc::new(AtomicUsize::new(0));
    let bad_input = TaskCreationInput::new(
        FrozenBytes::freeze(Bytes::new()),
        Box::new(PanickingInput(payload_drops.clone())),
    );
    let (bad, bad_job) = fixture.admitted(&a, 1, bad_input);
    let (good, good_job) = fixture.admitted(&b, 2, input(Arc::new(AtomicUsize::new(0))));
    let queue = PreparationQueue::new(2, 2, 1, 1);
    queue
        .enqueue(bad_job)
        .unwrap_or_else(|_| panic!("bad original job"));
    queue
        .enqueue(good_job)
        .unwrap_or_else(|_| panic!("good original job"));
    assert!(matches!(
        queue.pop().unwrap().run(|input, _| drop(input)),
        PreparationOutcome::Unresolved
    ));
    assert_eq!(
        payload_drops.load(Ordering::SeqCst),
        0,
        "the caught opaque panic is still in original custody"
    );
    assert!(!bad.host_borrowers_exited());
    assert!(!bad.publish_whole_exit().unwrap());
    assert!(bad.take_after_exit().unwrap().is_none());
    assert_eq!(fixture.ledger.unresolved_work(), vec![bad.key()]);
    assert!(matches!(
        queue.pop().unwrap().run(|input, _| drop(input)),
        PreparationOutcome::Returned(())
    ));
    fixture.release(&good);
    assert_eq!(fixture.ledger.used(Dimension::PreparingTasks.id()), 1);
    assert_eq!(fixture.ledger.used(Dimension::PreparingBytes.id()), 10);
    assert!(bad.0.state.lock().unwrap().body_exited);
    assert!(!bad.0.state.lock().unwrap().captures_exited);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
}
