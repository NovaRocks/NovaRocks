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

//! Actual Task-bootstrap calls keep their original owned roles and Context
//! failure publication stage. Process quotas are distinct from object storage.

use super::*;
use crate::control_runtime::{
    admitted_role::{adopt_reported_work, reserve_reported_role},
    task_host_call::{
        HOST_CALL_STORAGE, TaskHostCall, TaskHostCallError, TaskHostCallKind, TaskHostCallOutcome,
        TaskHostCallOwner,
    },
};
use novarocks_control_runtime::{
    driver::FailureSourceState, exit_role::ExitRolePhase, transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::scope::WorkKind;
use std::{
    panic::panic_any,
    sync::{
        Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    thread::{self, ThreadId},
};

fn original_port(f: &mut Harness, task: TaskIdentity) -> Arc<crate::task_port::TaskRuntimePort> {
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .unwrap()
        .port()
}
fn supervised_foreign_port(
    f: &mut Harness,
    task: TaskIdentity,
) -> Arc<crate::task_port::TaskRuntimePort> {
    original_port(f, task)
}
fn same_identity_loading_fixture(identity: QueryContextRef) -> (Harness, TaskIdentity) {
    let (mut f, task, work, create, load) = pending_loading_task_fixture(Some(identity));
    let context_address = f.service.context(f.identity).unwrap();
    let mut context = context_address.lock().unwrap();
    let registration = context.registration().clone();
    context
        .adopt_task(
            &f.runtime,
            &load,
            &crate::control_runtime::task::TaskHostBinding {
                registration,
                context: f.identity,
                task_source: ActorId::new(86).unwrap(),
                protocol_generation: generation(),
                max_cuts: 4,
                pending: Arc::new(RpcTaskPending),
                health: Arc::new(
                    novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
                ),
            },
        )
        .unwrap();
    drop(context);
    finish_create_waiter(&mut f, work, create, load);
    (f, task)
}
fn funder(f: &mut Harness, task: TaskIdentity, calls: usize) -> ContextMetadataRole {
    let host_address = f.service.context(f.identity).unwrap();
    let mut host = host_address.lock().unwrap();
    let parent = host
        .task(task)
        .unwrap()
        .resources_for_retirement()
        .active
        .id();
    let scope = host.scope().clone();
    // This actual parent owns the explicit inventory for the requested host
    // roles. Its own bound header is independent of each future child header.
    let resources = HOST_CALL_STORAGE
        .checked_mul(calls)
        .unwrap()
        .checked_add(StorageNeed {
            work: 1,
            ..StorageNeed::default()
        })
        .unwrap()
        .resources()
        .unwrap();
    let (mut work, capacity) = scope
        .reserve_child_next(generation(), parent, resources, WorkKind::Cleanup)
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    work.start().unwrap();
    ContextMetadataRole { work, capacity }
}
fn original_call(f: &mut Harness, task: TaskIdentity, kind: TaskHostCallKind) -> TaskHostCall {
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .unwrap()
        .host_call(kind)
        .clone()
}
fn publish_failure(f: &mut Harness) -> InputId {
    let unknown = crate::control_runtime::actors::tests::task(f.identity, 999);
    let input = model_fact(f, ContextInput::Installed { task: unknown }, 9);
    let failure = super::failure_publications::take_actual_failure(f, input);
    let failure_input = failure.input;
    assert!(matches!(failure.effect, WorkerAction::OwnerFailed { .. }));
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(failure)
        .unwrap();
    let mut drained = false;
    for _ in 0..128 {
        if f.service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_failure_publications(1)
            .unwrap()
        {
            drained = true;
            break;
        }
    }
    assert!(drained);
    failure_input
}
fn finish_failed_accepted_task_fixture(
    f: Harness,
    task: TaskIdentity,
    preparation: ContextMetadataRole,
    failure_input: InputId,
) {
    let headers = super::failure_publications::actual_context_headers(&f);
    finish_accepted_task_fixture_with(f, task, preparation, |f| {
        super::failure_publications::complete_actual_failure_source(f, failure_input);
        assert!(
            headers.iter().all(|header| !header.is_retired()),
            "Task teardown and supervisor input completion do not exit Context holders"
        );
    });
    assert!(
        headers.iter().all(|header| header.is_retired()),
        "the original Context holders must retire through their actual fixture exits"
    );
}
fn usage(ledger: &RuntimeLedger) -> [u64; 4] {
    [
        Dimension::ActiveTasks,
        Dimension::PreparingTasks,
        Dimension::PreparingBytes,
        Dimension::Requests,
    ]
    .map(|dimension| ledger.used(dimension.id()))
}
fn retire_call(call: &TaskHostCall, ledger: &RuntimeLedger) {
    let mut retired = false;
    for _ in 0..64 {
        if call.advance_retirement(ledger).unwrap() {
            retired = true;
            break;
        }
    }
    assert!(retired && call.is_retired());
}
fn release_funder(f: &Harness, mut payer: ContextMetadataRole) {
    payer.work.body_exited().unwrap();
    payer.work.captures_exited().unwrap();
    assert!(payer.work.actual_exit_ready().unwrap());
    f.ledger
        .reap_scope_ready(payer.work.scope().id(), 64)
        .unwrap();
    payer
        .capacity
        .release_quota(&payer.capacity.resources().unwrap().quota)
        .unwrap();
    payer.capacity.finish().unwrap();
}

struct ObserverGate {
    entered: SyncSender<ThreadId>,
    released: Mutex<bool>,
    changed: Condvar,
    calls: Arc<AtomicUsize>,
}
impl ObserverGate {
    fn new() -> (Arc<Self>, Receiver<ThreadId>) {
        let (entered, receiver) = sync_channel(1);
        (
            Arc::new(Self {
                entered,
                released: Mutex::new(false),
                changed: Condvar::new(),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            receiver,
        )
    }
    fn observe(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.send(thread::current().id()).unwrap();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

fn take_original_retire_job(
    f: &mut Harness,
    task: TaskIdentity,
) -> Result<
    Option<crate::control_runtime::task_host_call::TaskHostCallJob>,
    crate::control_runtime::context_host::failure::FailurePublicationError,
> {
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .take_failed_retire_borrowing_job(task)
}

#[test]
fn stop_and_borrowing_close_run_on_hosts_and_return_consumption_precedes_header_retirement() {
    let (mut f, task, preparation) = accepted_task_fixture();
    // Both original calls and their bound storage were admitted at Task bootstrap.
    // These are observer aliases of that inventory, not replacement admissions.
    let paid = usage(&f.ledger);
    let stop = original_call(&mut f, task, TaskHostCallKind::Stop);
    let close = original_call(&mut f, task, TaskHostCallKind::RetireBorrowing);
    assert_eq!(usage(&f.ledger), paid);
    let port = original_port(&mut f, task);
    let failure_input = publish_failure(&mut f);
    {
        let context_address = f.service.context(f.identity).unwrap();
        let context = context_address.lock().unwrap();
        let stage = context.failure_publication_stage().unwrap();
        assert!(matches!(
            close.take_failure_job(&stage).err().unwrap(),
            TaskHostCallError::BorrowerExitRequired
        ));
    }
    assert!(take_original_retire_job(&mut f, task).unwrap().is_none());
    let metadata_thread = thread::current().id();
    let mut prepared_close = None;
    for call in [&stop, &close] {
        assert!(!call.is_retired());
        if call.kind() == TaskHostCallKind::RetireBorrowing {
            assert!(port.prepare_source_retirement().is_err());
        }
        let (gate, entered) = ObserverGate::new();
        let observer = gate.clone();
        let subscription = port
            .stop_observable()
            .subscribe(Arc::new(move || observer.observe()));
        let job = if call.kind() == TaskHostCallKind::Stop {
            let context_address = f.service.context(f.identity).unwrap();
            let context = context_address.lock().unwrap();
            let stage = context.failure_publication_stage().unwrap();
            let job = call.take_failure_job(&stage).unwrap();
            assert!(matches!(
                call.take_failure_job(&stage).err().unwrap(),
                TaskHostCallError::AlreadyIssued
            ));
            job
        } else {
            assert!(matches!(
                take_original_retire_job(&mut f, task).err().unwrap(),
                crate::control_runtime::context_host::failure::FailurePublicationError::HostCall(
                    TaskHostCallError::AlreadyIssued
                )
            ));
            prepared_close
                .take()
                .expect("the original Retire job was admitted before Stop report consumption")
        };
        let runner = thread::spawn(move || job.run());
        let arrived = entered.recv_timeout(Duration::from_secs(10));
        // Gather observations while the real callback is blocked, then always
        // release/join before assertions so a failed oracle cannot park a host.
        let pending = call.poll_return();
        let held = usage(&f.ledger);
        let phase = call.phase();
        let progressed = f
            .service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_failure_publications(1);
        let borrowing_closed = port.prepare_source_retirement().is_ok();
        let stopped = port.is_stopped();
        let blocked_retire = if call.kind() == TaskHostCallKind::Stop {
            Some(take_original_retire_job(&mut f, task))
        } else {
            None
        };
        gate.release();
        let joined = runner.join();
        drop(subscription);
        assert!(joined.is_ok());
        let callback_thread = arrived.expect("the actual host entered its observer");
        assert_ne!(callback_thread, metadata_thread);
        assert!(pending.unwrap().is_none());
        assert_eq!(phase, ExitRolePhase::Started);
        assert_eq!(
            held, paid,
            "host-call storage grants no additional process quota"
        );
        assert!(progressed.unwrap());
        assert_eq!(
            borrowing_closed,
            call.kind() == TaskHostCallKind::RetireBorrowing
        );
        assert!(stopped);
        if let Some(blocked) = blocked_retire {
            assert!(blocked.unwrap().is_none());
        }
        assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
        // Actual return and capture exit have happened, but neither report
        // observer nor the slot fee may retire merely because the thread joined.
        assert_eq!(call.phase(), ExitRolePhase::Exited);
        assert!(!call.is_retired());
        assert_eq!(usage(&f.ledger), held);
        if call.kind() == TaskHostCallKind::Stop {
            // Borrowers have truly exited: obtain the same original Retire job
            // before consuming Stop's return or report, and before its fee refund.
            prepared_close = Some(
                take_original_retire_job(&mut f, task)
                    .unwrap()
                    .expect("actual Stop exit opens borrowing retirement"),
            );
            assert_eq!(usage(&f.ledger), held);
            assert_eq!(stop.phase(), ExitRolePhase::Exited);
            assert!(!stop.is_retired());
            assert!(matches!(
                take_original_retire_job(&mut f, task).err().unwrap(),
                crate::control_runtime::context_host::failure::FailurePublicationError::HostCall(
                    TaskHostCallError::AlreadyIssued
                )
            ));
        }
        assert!(matches!(
            call.poll_return().unwrap(),
            Some(TaskHostCallOutcome::Completed)
        ));
        assert_eq!(usage(&f.ledger), held);
        retire_call(call, &f.ledger);
        assert_eq!(usage(&f.ledger), held);
        assert!(
            call.is_retired(),
            "the same actual call header must be reaped"
        );
        if call.kind() == TaskHostCallKind::Stop {
            let context_address = f.service.context(f.identity).unwrap();
            let context = context_address.lock().unwrap();
            let stage = context.failure_publication_stage().unwrap();
            assert!(matches!(
                call.take_failure_job(&stage).err().unwrap(),
                TaskHostCallError::AlreadyIssued
            ));
        } else {
            assert!(matches!(
                take_original_retire_job(&mut f, task).err().unwrap(),
                crate::control_runtime::context_host::failure::FailurePublicationError::HostCall(
                    TaskHostCallError::AlreadyIssued
                )
            ));
        }
    }
    assert!(prepared_close.is_none());
    assert!(port.is_stopped());
    assert!(port.prepare_source_retirement().is_ok());
    assert_eq!(usage(&f.ledger), paid);
    assert!(stop.is_retired() && close.is_retired());
    drop((port, stop, close));
    finish_failed_accepted_task_fixture(f, task, preparation, failure_input);
}

struct PanicOriginals {
    _harness: Harness,
    _preparation: ContextMetadataRole,
    drops: Arc<AtomicUsize>,
}
impl Drop for PanicOriginals {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn observer_panic_keeps_the_original_payload_port_and_storage_in_unknown_custody() {
    let (mut f, task, preparation) = accepted_task_fixture();
    let call = original_call(&mut f, task, TaskHostCallKind::Stop);
    let ledger = f.ledger.clone();
    let held = usage(&ledger);
    let port = original_port(&mut f, task);
    let health = port.health();
    let failure_input = publish_failure(&mut f);
    let failure_runtime = f.runtime.clone();
    let keeper = Arc::new(Mutex::new(None::<PanicOriginals>));
    let payload_drops = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let original = keeper.clone();
    let observed = calls.clone();
    let subscription = port.stop_observable().subscribe(Arc::new(move || {
        observed.fetch_add(1, Ordering::SeqCst);
        let payload = original
            .lock()
            .unwrap()
            .take()
            .expect("one original host panic payload");
        panic_any(payload);
    }));
    let job = {
        let host_address = f.service.context(f.identity).unwrap();
        let host = host_address.lock().unwrap();
        let stage = host.failure_publication_stage().unwrap();
        let job = call.take_failure_job(&stage).unwrap();
        assert!(matches!(
            call.take_failure_job(&stage).err().unwrap(),
            TaskHostCallError::AlreadyIssued
        ));
        job
    };
    *keeper.lock().unwrap() = Some(PanicOriginals {
        _harness: f,
        _preparation: preparation,
        drops: payload_drops.clone(),
    });
    drop(port);
    let joined = thread::spawn(move || job.run()).join();
    assert!(
        joined.is_ok(),
        "the actual runner captures its observer panic"
    );
    assert!(keeper.lock().unwrap().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(call.has_panic());
    assert_eq!(call.phase(), ExitRolePhase::Unresolved);
    assert!(matches!(
        call.poll_return().unwrap(),
        Some(TaskHostCallOutcome::Panicked)
    ));
    assert!(!call.advance_retirement(&ledger).unwrap());
    assert!(!call.advance_retirement(&ledger).unwrap());
    assert!(matches!(
        call.retire_unused(&ledger).err().unwrap(),
        TaskHostCallError::AlreadyIssued
    ));
    assert!(!call.is_retired());
    assert_eq!(usage(&ledger), held);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!health.is_healthy());
    assert_eq!(
        failure_runtime
            .failure_source_state(context_actor())
            .unwrap(),
        FailureSourceState::ClosedPending(failure_input),
        "Unknown host custody cannot complete the original supervisor source"
    );
    drop(subscription);
    // The production primitive's one actual panic slot owns this entire
    // unresolved original graph. No test leak registry or fake exit is used.
}

#[test]
fn foreign_runtime_with_equal_scope_numbers_returns_the_whole_paid_original_for_retry() {
    let (mut f, task, preparation) = accepted_task_fixture();
    let (mut other, other_task) = same_identity_loading_fixture(f.identity);
    assert_eq!(task, other_task);
    let mut payer = funder(&mut f, task, 1);
    let scope = payer.work.scope().clone();
    let foreign_scope = other
        .service
        .context(other.identity)
        .unwrap()
        .lock()
        .unwrap()
        .scope()
        .clone();
    assert_eq!(scope.id(), foreign_scope.id());
    let admitted = reserve_reported_role(
        &scope,
        &mut payer.capacity,
        &payer.work,
        HOST_CALL_STORAGE.resources().unwrap(),
        WorkKind::Cleanup,
        None,
    )
    .unwrap();
    let work = admitted.work.id();
    let report = admitted.report.clone();
    let original_reservation = admitted.capacity.id();
    let port = original_port(&mut f, task);
    let before = scope.snapshot().unwrap();
    let other_before = foreign_scope.snapshot().unwrap();
    let held = usage(&f.ledger);
    let mut refusal = TaskHostCall::from_unbound_admitted(
        TaskHostCallOwner {
            kind: TaskHostCallKind::Stop,
            runtime: other.runtime.clone(),
            actor: context_actor(),
            task,
            health: port.health(),
            pending: Arc::new(RpcTaskPending),
        },
        adopt_reported_work(admitted.work, admitted.capacity, admitted.report),
    )
    .err()
    .expect("equal numeric scope/actor IDs cannot replace the original runtime");
    assert!(matches!(refusal.error, TaskHostCallError::WrongScope));
    assert_eq!(refusal.original.work().id(), work);
    assert!(refusal.original.report().same_slot(&report));
    let (returned_work, returned_capacity, returned_report) = refusal.original.into_parts();
    assert_eq!(returned_capacity.id(), original_reservation);
    refusal.original = adopt_reported_work(returned_work, returned_capacity, returned_report);
    assert_eq!(
        refusal.original.bound_storage_profile().unwrap(),
        HOST_CALL_STORAGE.resources().unwrap().owned_slots
    );
    assert_eq!(scope.snapshot().unwrap(), before);
    assert_eq!(foreign_scope.snapshot().unwrap(), other_before);
    assert_eq!(usage(&f.ledger), held);

    // Retry the exact returned Work/report/bundle through production's
    // two-phase owner. A refused port attachment leaves this same paid call
    // unbound, so it needs neither a replacement Work nor another payment.
    refusal._original_owner.runtime = f.runtime.clone();
    let call =
        TaskHostCall::from_unbound_admitted(refusal._original_owner, refusal.original).unwrap();
    assert_eq!(call.key(), (scope.id(), work));
    let paid_before_attachment = usage(&f.ledger);
    let owner_before_attachment = scope.snapshot().unwrap();
    let foreign_port = supervised_foreign_port(&mut other, other_task);
    assert_eq!(foreign_port.identity(), port.identity());
    assert!(
        matches!(
            call.attach_port(&foreign_port),
            Err(TaskHostCallError::WrongScope)
        ),
        "an equal Task identity in a foreign runtime cannot replace the original publication owner"
    );
    assert_eq!(call.key(), (scope.id(), work));
    assert_eq!(scope.snapshot().unwrap(), owner_before_attachment);
    assert_eq!(foreign_scope.snapshot().unwrap(), other_before);
    assert_eq!(usage(&f.ledger), paid_before_attachment);
    call.attach_port(&port).unwrap();
    assert_eq!(call.key(), (scope.id(), work));
    assert_eq!(scope.snapshot().unwrap(), owner_before_attachment);
    assert_eq!(usage(&f.ledger), paid_before_attachment);
    let mut retired = false;
    for _ in 0..64 {
        if call.retire_unused(&f.ledger).unwrap() {
            retired = true;
            break;
        }
    }
    assert!(retired && call.is_retired());
    assert_eq!(
        usage(&f.ledger),
        paid_before_attachment,
        "object retirement never refunds unrelated process quotas"
    );
    // An actual unused withdrawal is not an unissued executable job. Keep
    // the original port alive and use a real stage for the late handoff check.
    let failure_input = publish_failure(&mut f);
    let before_late_handoff = scope.snapshot().unwrap();
    let paid_before_late_handoff = usage(&f.ledger);
    {
        let host_address = f.service.context(f.identity).unwrap();
        let host = host_address.lock().unwrap();
        let stage = host.failure_publication_stage().unwrap();
        assert!(matches!(
            call.take_failure_job(&stage)
                .err()
                .expect("withdrawn originals cannot restart"),
            TaskHostCallError::Withdrawn
        ));
    }
    assert!(call.retire_unused(&f.ledger).unwrap());
    assert_eq!(scope.snapshot().unwrap(), before_late_handoff);
    assert_eq!(usage(&f.ledger), paid_before_late_handoff);
    release_funder(&f, payer);
    drop(call);
    finish_failed_accepted_task_fixture(f, task, preparation, failure_input);
    finish_task_fixture(other, other_task);
}

fn normal_stop_ticket(
    f: &mut Harness,
    task: TaskIdentity,
) -> (InputId, EffectTicket<WorkerAction>) {
    use crate::control_runtime::task_publication::commit_task_projection;
    use novarocks_execution_contract::{AbortCause, TaskConclusionProposal};
    let port = original_port(f, task);
    let handoff = port
        .ingress()
        .submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
    assert!(handoff.transferred && handoff.discarded.is_none() && handoff.error.is_none());
    let input = f
        .service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .unwrap()
        .open_summary_mut()
        .unwrap()
        .deliver_next(generation())
        .unwrap()
        .unwrap();
    f.runtime.poll_one(Tick(7)).unwrap();
    let runtime = f.runtime.clone();
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .dispatch
        .index_committed(
            &runtime,
            crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
        )
        .unwrap();
    for _ in 0..crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS {
        let ticket = f
            .service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .dispatch
            .take_ready(
                &runtime,
                crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
            )
            .unwrap()
            .expect("the real summary has a ready publication or StopTask action");
        if matches!(
            ticket.effect,
            WorkerAction::Context(ContextEffect::StopTask { .. })
        ) {
            assert!(port.publication().borrow().neutral_accepted().is_some());
            assert_eq!(
                f.service
                    .context(f.identity)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .task(task)
                    .unwrap()
                    .open_summary_mut()
                    .unwrap()
                    .observe_core()
                    .unwrap(),
                Consumption::Consumed
            );
            return (input, ticket);
        }
        drop(commit_task_projection(&runtime, &ticket, &port).unwrap());
        f.service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .dispatch
            .publication_committed(&runtime, &ticket)
            .unwrap();
        runtime.effect_completed(ticket).unwrap();
    }
    panic!("the actual proposal must produce its original StopTask");
}

#[test]
fn a_queued_normal_stop_losing_its_generation_waits_for_the_real_failure_stage() {
    let (mut f, task, preparation) = accepted_task_fixture();
    let call = original_call(&mut f, task, TaskHostCallKind::Stop);
    let key = call.key();
    let paid = usage(&f.ledger);
    let port = original_port(&mut f, task);
    let (summary_input, stop) = normal_stop_ticket(&mut f, task);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let subscription = port.stop_observable().subscribe(Arc::new(move || {
        observed.fetch_add(1, Ordering::SeqCst);
    }));
    let queued = call.take_stop_effect_job(&stop).unwrap();
    let unknown = crate::control_runtime::actors::tests::task(f.identity, 999);
    let failed_input = model_fact(&mut f, ContextInput::Installed { task: unknown }, 9);
    assert!(f.runtime.snapshot(context_actor()).unwrap().failed);
    // The normal job was genuinely handed off, but no foreign observer may
    // start from that stale permission before global failure publication.
    thread::spawn(move || queued.run()).join().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(call.poll_return().unwrap().is_none());
    assert_eq!(call.phase(), ExitRolePhase::Admitted);
    assert!(!call.has_panic());
    assert_eq!(usage(&f.ledger), paid);
    assert_eq!(call.key(), key);
    assert!(call.take_stop_effect_job(&stop).is_err());
    assert!(matches!(
        call.retire_unused(&f.ledger).err().unwrap(),
        TaskHostCallError::AlreadyCaptured
    ));
    assert!(
        f.service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .failure_publication_stage()
            .is_err()
    );
    let failure = super::failure_publications::take_actual_failure(&f, failed_input);
    let failure_input = failure.input;
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .begin_failure_publications(failure)
        .unwrap();
    let mut drained = false;
    for _ in 0..128 {
        if f.service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .advance_failure_publications(1)
            .unwrap()
        {
            drained = true;
            break;
        }
    }
    assert!(drained);
    let actual = {
        let host_address = f.service.context(f.identity).unwrap();
        let host = host_address.lock().unwrap();
        call.take_failure_job(&host.failure_publication_stage().unwrap())
            .unwrap()
    };
    assert_eq!(call.key(), key);
    assert_eq!(usage(&f.ledger), paid);
    thread::spawn(move || actual.run()).join().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        call.poll_return().unwrap(),
        Some(TaskHostCallOutcome::Completed)
    ));
    assert_eq!(usage(&f.ledger), paid);
    retire_call(&call, &f.ledger);
    assert_eq!(usage(&f.ledger), paid);
    assert!(
        call.is_retired(),
        "same actual header retires after its original report"
    );
    // This original Started StopTask now has its real host completion. No
    // failure flag, deferred return, or queue removal supplied that fact.
    f.runtime.effect_completed(stop).unwrap();
    f.runtime
        .retire_input(context_actor(), summary_input)
        .unwrap();
    f.service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .task(task)
        .unwrap()
        .open_summary_mut()
        .unwrap()
        .retire_unclaimed()
        .unwrap();
    drop(subscription);
    drop((call, port));
    finish_failed_accepted_task_fixture(f, task, preparation, failure_input);
}
