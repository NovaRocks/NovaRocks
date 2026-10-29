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

//! Actual original M/R calls on a symbolic host instance. These tests do not
//! claim Native materialization, and only Core decides the establishment reply.
mod node_flow;
mod node_task_flow;

use super::source_pools::SourcePoolGuard;
use super::*;
use crate::{
    HostRejection, QueryContextHostFactory, QueryContextHostInstance, ReleasedContextEvidence,
    SharedFactsRequest,
    control_runtime::{
        actors::{ContextActor, ContextMessage},
        context_executors::{
            ContextCancelError, ContextExecutorHandles, ContextExecutorStartError,
            ContextExecutors, ContextSubmitError,
        },
        context_host::{
            CONTEXT_BOOTSTRAP_STORAGE, ContextHostBinding, ContextHostProgressError,
            ContextHostRecord, ContextInternalAdvance,
        },
        context_host_jobs::{ContextHostJob, ContextHostJobs},
        context_ready::ContextReadyNotice,
        effect_dispatch::EffectRef,
        preparation::QueuedPreparation,
    },
    control_runtime_policy::{ControlRuntimePolicy, IngressRoleLimits},
};
use novarocks_control_runtime::{actor::ActorConfig, publication::PublishedSnapshot};
use novarocks_control_runtime_core::{
    effect::EffectState, fair_queue::FairQueueError, mailbox::InputClass,
};
use novarocks_execution_contract::QueryContextState;
use novarocks_worker_protocol::{
    context::{
        ContextConfig, ContextCore, ContextEffect, ContextEstablishment,
        ContextEstablishmentDecision, ContextInput, EstablishReplayRequest,
    },
    lease::LeaseBounds,
};
use std::{
    sync::{Condvar, Mutex},
    time::Instant,
};

fn context_actor() -> ActorId {
    ActorId::new(4).unwrap()
}

// Root lifecycle commands have their own actual producer funding. Context
// continuations instead use the original host's prepaid internal source.
const BRIDGE_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: 1,
    batches: 1,
    effects: crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
    work: 1,
    captures: 1,
    cuts: 0,
    replies: 0,
    completions: 0,
};

struct SymbolicFactory {
    instance: Arc<SymbolicInstance>,
}
struct SymbolicInstance {
    context: QueryContextRef,
    materializing: Arc<Gate>,
    calls: Arc<AtomicUsize>,
    releases: Arc<AtomicUsize>,
}
impl QueryContextHostFactory for SymbolicFactory {
    fn create_instance(
        self: Arc<Self>,
        context: QueryContextRef,
    ) -> Result<Arc<dyn QueryContextHostInstance>, HostRejection> {
        assert_eq!(context, self.instance.context);
        Ok(self.instance.clone())
    }
}
impl QueryContextHostInstance for SymbolicInstance {
    fn context(&self) -> QueryContextRef {
        self.context
    }
    fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        assert_eq!(request.context(), self.context);
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.materializing.stop();
        Ok(())
    }
    fn release(&self) -> ReleasedContextEvidence {
        self.releases.fetch_add(1, Ordering::SeqCst);
        ReleasedContextEvidence::none()
    }
    fn advance_shared_domain(
        &self,
        _: &novarocks_execution_contract::task_execution::operation::QueryContextDomainUpdate,
    ) -> Result<(), HostRejection> {
        panic!("this symbolic M/R host never advances a shared domain")
    }
}

struct MaterializationFixture {
    f: Fixture,
    input: InputId,
    external: ControlRequestHandle,
    jobs: ContextHostJobs,
    job: ContextHostJob,
    getters: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    establishment: ContextEstablishment,
    source_pools: SourcePoolGuard,
}
impl MaterializationFixture {
    fn new(materializing: Arc<Gate>, calls: Arc<AtomicUsize>, releases: Arc<AtomicUsize>) -> Self {
        Self::with_factory(materializing, calls, releases, None)
    }

    fn with_factory(
        materializing: Arc<Gate>,
        calls: Arc<AtomicUsize>,
        releases: Arc<AtomicUsize>,
        factory: Option<Arc<dyn QueryContextHostFactory>>,
    ) -> Self {
        let policy = ControlRuntimePolicy::try_new(IngressRoleLimits {
            ordinary_running: 1,
            ordinary_waiting: 0,
            control_running: 1,
            control_waiting: 0,
            max_batch_items: 1,
            root_read_holders: 1,
        })
        .unwrap();
        let ticket_storage = StorageNeed {
            work: 1,
            captures: 1,
            ..StorageNeed::default()
        };
        let additional = ESTABLISH_REQUEST_STORAGE
            .checked_sub(CONTROL_REQUEST_STORAGE)
            .unwrap()
            .checked_add(CONTEXT_BOOTSTRAP_STORAGE)
            .unwrap()
            .checked_add(ticket_storage)
            .unwrap()
            .checked_add(BRIDGE_STORAGE.checked_mul(2).unwrap())
            .unwrap()
            .resources()
            .unwrap()
            .checked_add(
                &ResourceVector::new([(Dimension::Tickets.id(), 1), (Dimension::Contexts.id(), 1)])
                    .unwrap(),
            )
            .unwrap();
        let storage = AdmissionStorage {
            loading: policy.loading.resources().unwrap(),
            accepted: policy.accepted.resources().unwrap(),
            context: CONTEXT_BOOTSTRAP_STORAGE.resources().unwrap(),
            ticket: ticket_storage.resources().unwrap(),
        };
        let mut f = Fixture::with_geometry_records(2, 2, 16, storage.clone(), additional, 3, 2, 32);
        let mut source_pools = SourcePoolGuard::start(&mut f);
        let factory: Arc<dyn QueryContextHostFactory> = factory.unwrap_or_else(|| {
            Arc::new(SymbolicFactory {
                instance: Arc::new(SymbolicInstance {
                    context: f.context,
                    materializing,
                    calls,
                    releases,
                }),
            })
        });
        let mut acquisition_bytes = [12; 16];
        acquisition_bytes[6] = 0x70;
        acquisition_bytes[8] = 0x80;
        let acquisition = TaskOperationId::try_from_bytes(acquisition_bytes).unwrap();
        let (acquire, mut acquire_external) = f
            .root
            .admit_request(
                ActorId::new(3).unwrap(),
                acquisition,
                MonotonicInstant::from_origin(Duration::from_secs(100)),
            )
            .unwrap();
        f.root
            .submit_request(
                acquire,
                ControlInput::Acquire {
                    request: AcquireQueryContextAdmissionTicket::new(
                        acquisition,
                        f.context,
                        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
                        NativeCompatibilityId::new([3; 32]),
                        f.capability,
                    ),
                    ticket: AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
                },
            )
            .unwrap();
        f.runtime.poll_one(Tick(1)).unwrap();
        f.root.index_control_effects(64).unwrap();
        let grant = f.root.take_control_effect(64).unwrap().unwrap();
        f.root.adopt_ticket(&grant).unwrap();
        complete_root(&mut f, grant);
        let reply = f.root.take_control_effect(64).unwrap().unwrap();
        f.root.publish_request_reply(reply).unwrap();
        drop(acquire_external.try_take().unwrap());
        drop(acquire_external);
        assert_eq!(f.root.reap_ready_requests(8).unwrap(), 1);

        let (raw, getters, drops) = request(&f, None, None, false);
        let (input, external) = f
            .root
            .admit_establish_request(
                ActorId::new(2).unwrap(),
                operation(),
                MonotonicInstant::from_origin(Duration::from_secs(100)),
            )
            .unwrap();
        source_pools.getter(
            f.root
                .begin_establish_ingress(input, raw, PayloadId(1))
                .unwrap(),
        );
        let EstablishIngressOutcome::Projected(projection) =
            source_pools::wait_ingress(&mut f, input)
        else {
            panic!("the original getter returns the actual projection")
        };
        f.root
            .submit_request(
                input,
                ControlInput::Establish {
                    request: projection.operation,
                    context: projection.context,
                    ticket: projection.ticket,
                    payload: projection.payload,
                    semantics: projection.semantics,
                    generation: generation(),
                },
            )
            .unwrap();
        f.runtime.poll_one(Tick(2)).unwrap();
        f.root.index_control_effects(64).unwrap();
        let materialize = f.root.take_control_effect(64).unwrap().unwrap();
        let establishment = match &materialize.effect {
            WorkerAction::Control {
                effect: ControlEffect::Materialize { establishment, .. },
                ..
            } => *establishment,
            _ => panic!("the redeemed original ticket elects M"),
        };
        f.root
            .adopt_context(
                &materialize,
                ContextHostBinding {
                    actor: context_actor(),
                    core: ContextCore::new(
                        f.context,
                        generation(),
                        establishment,
                        ContextConfig {
                            max_tasks: 1,
                            max_creation_waiters: 1,
                            max_pending_requests: 4,
                            max_preparing: 1,
                            max_task_bytes: 1024,
                            step_budget: ContextCore::MAX_STEP_WORK,
                            lease_bounds: LeaseBounds::DEFAULT,
                            metric_publish_interval: Duration::from_millis(100),
                        },
                    )
                    .unwrap(),
                    policy,
                    storage,
                    max_live_tasks: 1,
                    max_live_roots: 1,
                },
            )
            .unwrap();
        f.root
            .service()
            .context(f.context)
            .unwrap()
            .lock()
            .unwrap()
            .prepare_host_jobs()
            .unwrap();
        let original_jobs = f
            .root
            .service()
            .context(f.context)
            .unwrap()
            .lock()
            .unwrap()
            .host_jobs()
            .unwrap()
            .clone();
        f.root
            .bind_materialization_return(&materialize, original_jobs)
            .unwrap();
        let source = f.root.take_materialization_source(&materialize).unwrap();
        let owner_address = f.root.service().context(f.context).unwrap();
        let owner = owner_address.lock().unwrap();
        let jobs = owner.host_jobs().unwrap().clone();
        let job = owner.capture_materialization(source, factory).unwrap();
        complete_root(&mut f, materialize);
        Self {
            f,
            input,
            external,
            jobs,
            job,
            getters,
            drops,
            establishment,
            source_pools,
        }
    }
}

fn complete_root(f: &mut Fixture, ticket: EffectTicket<WorkerAction>) {
    let completed_lane = matches!(
        &ticket.effect,
        WorkerAction::Control {
            effect: ControlEffect::AdmissionReply { .. }
                | ControlEffect::DomainFailureReply { .. }
                | ControlEffect::Reply { .. }
                | ControlEffect::WakeInternal
                | ControlEffect::RouteEstablish { .. }
                | ControlEffect::CancelEstablishRoute { .. }
                | ControlEffect::RetireReservation(_),
            ..
        } | WorkerAction::ControlReply(_)
    );
    let reference = EffectRef {
        input: ticket.input,
        action: ticket.descriptor.id,
    };
    f.complete_body(ticket);
    if completed_lane {
        f.root.control_completion_observed(reference).unwrap();
    }
}

fn complete_context(f: &mut Fixture, ticket: EffectTicket<WorkerAction>) {
    let context = f.context;
    if matches!(
        &ticket.effect,
        WorkerAction::Context(ContextEffect::WakeInternal)
    ) {
        f.root
            .service()
            .context(context)
            .unwrap()
            .lock()
            .unwrap()
            .handoff_internal_wake(ticket)
            .unwrap();
        return;
    }
    if matches!(
        &ticket.effect,
        WorkerAction::Context(ContextEffect::EstablishmentDecided { .. })
    ) {
        f.root
            .service()
            .context(context)
            .unwrap()
            .lock()
            .unwrap()
            .handoff_establishment_decision(ticket)
            .unwrap();
        return;
    }
    if matches!(
        &ticket.effect,
        WorkerAction::Context(ContextEffect::RetainContext { .. })
    ) {
        f.root
            .service()
            .context(context)
            .unwrap()
            .lock()
            .unwrap()
            .handoff_retention(ticket)
            .unwrap();
        return;
    }
    // Direct effects have no Completed metadata lane. Their real action must
    // finish before this call; the wrappers above own both metadata handoffs.

    let input = ticket.input;
    let completed = ticket.descriptor.id;
    f.root
        .service()
        .context(context)
        .unwrap()
        .lock()
        .unwrap()
        .complete_host_effect(ticket)
        .unwrap();
    for (descriptor, _) in f
        .runtime
        .effect_inventory(
            context_actor(),
            input,
            None,
            crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
        )
        .unwrap()
    {
        if descriptor.dependencies.contains(&completed) {
            f.runtime
                .acknowledge_dependency(context_actor(), input, descriptor.id, completed)
                .unwrap();
        }
    }
}

fn assert_context_internal_idle(f: &mut Fixture) {
    let context = f.context;
    let advanced = f
        .root
        .service()
        .context(context)
        .unwrap()
        .lock()
        .unwrap()
        .advance_internal()
        .unwrap();
    assert!(
        matches!(advanced, ContextInternalAdvance::Idle),
        "the zero-task Tick completed its real close scan without residual work: {advanced:?}"
    );
}

fn context_effects(f: &mut Fixture) -> Vec<EffectTicket<WorkerAction>> {
    const BUDGET: usize = 64;
    let context = f.context;
    let runtime = f.runtime.clone();
    let host_address = f.root.service().context(context).unwrap();
    let mut host = host_address.lock().unwrap();
    host.dispatch.index_committed(&runtime, BUDGET).unwrap();
    let mut result = Vec::new();
    for _ in 0..BUDGET {
        let Some(ticket) = host.dispatch.take_ready(&runtime, BUDGET).unwrap() else {
            break;
        };
        result.push(ticket);
    }
    result
}

// Consume through the real bounded address index. An empty index means no
// completion work this turn; polling the original jobs directly would hide a
// lost wake or an incorrectly detached original Context owner.
fn poll_host_returns(f: &mut Fixture) -> usize {
    let service = f.root.service();
    let Some(notice) = service.next_host_ready().unwrap() else {
        return 0;
    };
    service
        .ready_context(&notice)
        .expect("the ready notice names this still-registered original owner")
        .lock()
        .unwrap()
        .host_jobs()
        .unwrap()
        .poll_returns()
        .unwrap()
}

// The Service keeps the same actual owner while its actor attachment and
// metadata child scope exit. Late H completion still resolves via its notice.
fn dispose_context_metadata(f: &mut Fixture) {
    let runtime = f.runtime.clone();
    let ledger = f.ledger.clone();
    let context = f.context;
    while let Some(original_host_turn) = runtime.claim_host_actor() {
        original_host_turn.release(false).unwrap();
    }
    runtime.close_actor_admission(context_actor()).unwrap();
    let host_address = f.root.service().context(context).unwrap();
    let mut host = host_address.lock().unwrap();
    let resources = host.resources_for_test();
    assert!(resources.owner_control.close_for_teardown().unwrap());
    assert!(resources.internal.close_for_teardown().unwrap());
    runtime.seal_actor(context_actor()).unwrap();
    let scope = resources.scope.as_ref().unwrap().clone();
    for role in resources.roles.iter_mut().rev() {
        role.work.body_exited().unwrap();
        role.work.captures_exited().unwrap();
    }
    ledger.reap_scope_ready(scope.id(), 16).unwrap();
    for role in resources.roles.iter_mut().rev() {
        role.capacity
            .release(&role.capacity.resources().unwrap())
            .unwrap();
    }
    drop(runtime.remove_actor(context_actor()).unwrap());
    resources.registered = false;
    ledger.remove_scope(scope.id()).unwrap();
    resources.scope = None;
}

fn root_handoff(f: &mut Fixture, command: ControlInput, now: Tick) -> HostInput {
    let input = f.host_input(command, BRIDGE_STORAGE);
    assert!(f.runtime.poll_one(now).unwrap());
    f.root.index_control_effects(64).unwrap();
    input
}

fn handoff_release(f: &mut Fixture, ticket: EffectTicket<WorkerAction>) {
    let context = f.context;
    f.root
        .service()
        .context(context)
        .unwrap()
        .lock()
        .unwrap()
        .handoff_host_release(ticket)
        .unwrap();
}

const HOST_WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Default)]
struct FixedPoolLatchState {
    entered: bool,
    released: bool,
}
#[derive(Default)]
struct FixedPoolLatch {
    state: Mutex<FixedPoolLatchState>,
    changed: Condvar,
}
impl FixedPoolLatch {
    fn enter_and_wait(&self) {
        let mut state = self.state.lock().unwrap();
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).unwrap();
        }
    }
    fn wait_entered(&self) -> bool {
        let deadline = Instant::now() + HOST_WATCHDOG;
        let mut state = self.state.lock().unwrap();
        while !state.entered {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            state = self.changed.wait_timeout(state, remaining).unwrap().0;
        }
        true
    }
    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }
    fn is_released(&self) -> bool {
        self.state.lock().unwrap().released
    }
}

#[derive(Default)]
struct FixedPoolThreads {
    factory: Mutex<Option<thread::ThreadId>>,
    materialization: Mutex<Option<thread::ThreadId>>,
    release: Mutex<Option<thread::ThreadId>>,
    factory_drop: Mutex<Option<thread::ThreadId>>,
}
struct FixedPoolFactory {
    latch: Arc<FixedPoolLatch>,
    calls: Arc<AtomicUsize>,
    releases: Arc<AtomicUsize>,
    threads: Arc<FixedPoolThreads>,
}
impl QueryContextHostFactory for FixedPoolFactory {
    fn create_instance(
        self: Arc<Self>,
        context: QueryContextRef,
    ) -> Result<Arc<dyn QueryContextHostInstance>, HostRejection> {
        *self.threads.factory.lock().unwrap() = Some(thread::current().id());
        Ok(Arc::new(FixedPoolInstance {
            context,
            latch: self.latch.clone(),
            calls: self.calls.clone(),
            releases: self.releases.clone(),
            threads: self.threads.clone(),
        }))
    }
}
impl Drop for FixedPoolFactory {
    fn drop(&mut self) {
        *self.threads.factory_drop.lock().unwrap() = Some(thread::current().id());
    }
}
struct FixedPoolInstance {
    context: QueryContextRef,
    latch: Arc<FixedPoolLatch>,
    calls: Arc<AtomicUsize>,
    releases: Arc<AtomicUsize>,
    threads: Arc<FixedPoolThreads>,
}
impl QueryContextHostInstance for FixedPoolInstance {
    fn context(&self) -> QueryContextRef {
        self.context
    }
    fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        assert_eq!(request.context(), self.context);
        *self.threads.materialization.lock().unwrap() = Some(thread::current().id());
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.latch.enter_and_wait();
        Ok(())
    }
    fn release(&self) -> ReleasedContextEvidence {
        *self.threads.release.lock().unwrap() = Some(thread::current().id());
        self.releases.fetch_add(1, Ordering::SeqCst);
        ReleasedContextEvidence::none()
    }
    fn advance_shared_domain(
        &self,
        _: &novarocks_execution_contract::task_execution::operation::QueryContextDomainUpdate,
    ) -> Result<(), HostRejection> {
        panic!("the fixed-pool M/R host never advances a shared domain")
    }
}

struct FixedPoolGuard {
    latch: Arc<FixedPoolLatch>,
    owner: Option<ContextExecutors>,
    handles: Option<ContextExecutorHandles>,
    // Unexpected refusal preserves the exact original until accepted physical
    // work has drained. It is never silently dropped as unused or called green.
    refused: Vec<ContextHostJob>,
}
fn join_context_pool_owner(owner: ContextExecutors) {
    if let Err((reason, _original_owner)) = owner.shutdown() {
        // The original physical owner remains on this stack until the failed
        // run ends. It is never dropped or forgotten as a successful drain.
        eprintln!("Context pool shutdown retains an original owner: {reason:?}");
        std::process::abort();
    }
}
impl FixedPoolGuard {
    fn start(f: &mut Fixture, latch: Arc<FixedPoolLatch>) -> Self {
        let owner = match f
            .root
            .service()
            .start_context_executors(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1).unwrap())
        {
            Ok(owner) => owner,
            Err(refusal) => {
                latch.release();
                if let Some(partial) = refusal.owner {
                    join_context_pool_owner(*partial);
                }
                panic!("actual Context pool startup refused: {:?}", refusal.error);
            }
        };
        let mut guard = Self {
            latch,
            owner: Some(owner),
            handles: None,
            refused: Vec::with_capacity(3),
        };
        guard.handles = guard.owner.as_ref().unwrap().handles();
        assert!(guard.handles.is_some(), "both actual fixed pools started");
        guard
    }
    fn submit(&mut self, job: ContextHostJob) {
        if let Err((reason, original)) = self.handles.as_ref().unwrap().try_submit(job) {
            self.refused.push(original);
            self.latch.release();
            panic!("actual Context pool submission refused: {reason:?}");
        }
    }
    fn submit_stopped(&mut self, job: ContextHostJob) {
        if let Err((reason, original)) = self.handles.as_ref().unwrap().try_submit_stopped(job) {
            self.refused.push(original);
            self.latch.release();
            panic!("actual stopped Context job submission refused: {reason:?}");
        }
    }
    fn close_and_join(&mut self) {
        // Always unblock the fake M before any potentially waiting join. This
        // is idempotent on normal finish and on every unwinding path.
        self.latch.release();
        drop(self.handles.take());
        if let Some(owner) = self.owner.take() {
            join_context_pool_owner(owner);
        }
        if !self.refused.is_empty() {
            eprintln!(
                "Context pool test retains {} refused original jobs",
                self.refused.len()
            );
            std::process::abort();
        }
    }
    fn finish(mut self) {
        self.close_and_join();
    }
}
impl Drop for FixedPoolGuard {
    fn drop(&mut self) {
        self.close_and_join();
    }
}

fn wait_host_returns(f: &mut Fixture) -> Result<usize, &'static str> {
    let deadline = Instant::now() + HOST_WATCHDOG;
    loop {
        // Sample before the actual ready predicate. Re-sampling after a zero
        // result would miss a completion between that predicate and sleep.
        let version = f.runtime.change_version();
        let returned = poll_host_returns(f);
        if returned != 0 {
            return Ok(returned);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("actual Context host ready watchdog expired");
        }
        f.runtime.wait_after_timeout(version, remaining);
    }
}

fn wait_host_notice(f: &mut Fixture) -> Result<ContextReadyNotice, &'static str> {
    let deadline = Instant::now() + HOST_WATCHDOG;
    loop {
        let version = f.runtime.change_version();
        if let Some(notice) = f.root.service().next_host_ready().unwrap() {
            return Ok(notice);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("actual stopped M completion notice watchdog expired");
        }
        f.runtime.wait_after_timeout(version, remaining);
    }
}

fn poll_noticed_host_returns(f: &mut Fixture, notice: &ContextReadyNotice) -> usize {
    f.root
        .service()
        .ready_context(notice)
        .expect("the retained notice still names its original Service owner")
        .lock()
        .unwrap()
        .host_jobs()
        .unwrap()
        .poll_returns()
        .unwrap()
}

fn all_worker_fees(f: &Fixture) -> [u64; 16] {
    [
        Dimension::ActiveTasks,
        Dimension::PreparingTasks,
        Dimension::PreparingBytes,
        Dimension::InboundRecords,
        Dimension::InboundBytes,
        Dimension::Contexts,
        Dimension::Tickets,
        Dimension::Inputs,
        Dimension::Batches,
        Dimension::Effects,
        Dimension::Cuts,
        Dimension::Replies,
        Dimension::Completions,
        Dimension::Captures,
        Dimension::Work,
        Dimension::Requests,
    ]
    .map(|dimension| f.ledger.used(dimension.id()))
}

// The removed owner's Root inputs and payer are deliberately still live.
// Register a real, different Context at its former ActorId and produce only a
// neutral replay probe: no materialization or other foreign I/O is simulated.
fn removed_owner_rejects_reused_actor(
    f: &mut Fixture,
    removed: &ContextHostRecord,
    mut establishment: ContextEstablishment,
    now: Tick,
) -> bool {
    let before = all_worker_fees(f);
    let context = QueryContextRef::new(
        novarocks_types::QueryExecutionId::new(
            novarocks_types::QueryId::new(9701, 9702),
            novarocks_types::AttemptId::new(1).unwrap(),
        )
        .unwrap(),
        f.context.frontend_process_id(),
        f.context.backend_process_id(),
    );
    establishment.operation = TaskOperationId::new_v7();
    establishment.elected_at = MonotonicInstant::from_origin(Duration::from_nanos(now.0));
    let scope = f.ledger.open_scope_next(None, generation(), 4, 4).unwrap();
    let (mut work, mut capacity) = scope
        .reserve_next(
            generation(),
            BRIDGE_STORAGE.resources().unwrap(),
            WorkKind::Step,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Step)
        .unwrap();
    work.start().unwrap();
    let core = ContextCore::new(
        context,
        generation(),
        establishment,
        ContextConfig {
            max_tasks: 1,
            max_creation_waiters: 1,
            max_pending_requests: 4,
            max_preparing: 1,
            max_task_bytes: 1024,
            step_budget: ContextCore::MAX_STEP_WORK,
            lease_bounds: LeaseBounds::DEFAULT,
            metric_publish_interval: Duration::from_millis(100),
        },
    )
    .unwrap();
    let context_driver = ContextActor::new(
        core,
        scope.clone(),
        AdmissionStorage {
            loading: ResourceVector::default(),
            accepted: ResourceVector::default(),
            context: ResourceVector::default(),
            ticket: ResourceVector::default(),
        },
        work.id(),
        work.id(),
    );
    f.runtime
        .register_next(
            context_actor(),
            scope.clone(),
            WorkerActor::Context(Box::new(context_driver)),
            ActorConfig {
                mailbox: [1; 4],
                max_batches: 1,
                max_effects_per_step:
                    crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
                max_dependencies_per_effect:
                    crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
                turn_budget: crate::control_runtime::actors::ContextActor::TURN_BUDGET,
                max_step_work: crate::control_runtime::actors::ContextActor::MAX_STEP_WORK,
            },
        )
        .unwrap_or_else(|(error, _pure_actor)| {
            panic!("replacement Context actor registration refused: {error:?}")
        });
    let permit = f
        .runtime
        .reserve_input(context_actor(), actor(), InputClass::Control)
        .unwrap();
    let input = permit.id();
    permit
        .fill(WorkerMessage::Context(ContextMessage::Command {
            work: work.id(),
            request: None,
            command: ContextInput::ProbeEstablish {
                context,
                request: EstablishReplayRequest {
                    request: RequestContext {
                        token: input,
                        operation: establishment.operation,
                        deadline: MonotonicInstant::from_origin(
                            Duration::from_nanos(now.0) + Duration::from_secs(1),
                        ),
                    },
                    ticket: establishment.ticket,
                    payload: establishment.payload,
                    semantics: establishment.semantics,
                },
                comparison: None,
            },
        }))
        .unwrap();
    assert!(f.runtime.poll_one(now).unwrap());
    let action = f
        .runtime
        .ready_effects(
            context_actor(),
            input,
            crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
        )
        .unwrap()[0];
    let ticket = f
        .runtime
        .take_effect(context_actor(), input, action)
        .unwrap();
    let funded = all_worker_fees(f);
    let rejected = match removed.complete_host_effect(ticket) {
        Err(refusal) => {
            let untouched = refusal.error == DriverError::EffectTicketOriginMismatch
                && refusal.ticket.actor == context_actor()
                && refusal.ticket.input == input
                && refusal.ticket.descriptor.id == action
                && matches!(&refusal.ticket.effect,
                    WorkerAction::Context(ContextEffect::EstablishProbed { context: actual, .. })
                        if *actual == context)
                && f.runtime
                    .effect_inventory(context_actor(), input, None, 1)
                    .unwrap()[0]
                    .1
                    == EffectState::Started
                && all_worker_fees(f) == funded;
            // Its actual new owner can still complete the same exclusive
            // ticket after the unrelated removed record refused it.
            f.runtime.effect_completed(refusal.ticket).unwrap();
            untouched
        }
        Ok(()) => false,
    };
    f.runtime.retire_input(context_actor(), input).unwrap();
    scope.close_external().unwrap();
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    f.ledger.reap_scope_ready(scope.id(), 4).unwrap();
    capacity.release(&capacity.resources().unwrap()).unwrap();
    while let Some(original_host_turn) = f.runtime.claim_host_actor() {
        original_host_turn.release(false).unwrap();
    }
    drop(f.runtime.remove_actor(context_actor()).unwrap());
    f.ledger.remove_scope(scope.id()).unwrap();
    rejected && before == all_worker_fees(f)
}

#[test]
fn the_same_published_instance_releases_while_materialization_is_blocked_and_core_timer_progresses()
{
    let latch = Arc::new(FixedPoolLatch::default());
    let threads = Arc::new(FixedPoolThreads::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let releases = Arc::new(AtomicUsize::new(0));
    let poller_thread = thread::current().id();
    let factory: Arc<dyn QueryContextHostFactory> = Arc::new(FixedPoolFactory {
        latch: latch.clone(),
        calls: calls.clone(),
        releases: releases.clone(),
        threads: threads.clone(),
    });
    let MaterializationFixture {
        mut f,
        input,
        mut external,
        jobs,
        job,
        getters,
        drops,
        establishment,
        mut source_pools,
    } = MaterializationFixture::with_factory(
        Gate::new(),
        calls.clone(),
        releases.clone(),
        Some(factory),
    );
    // Declared after the fixture: on unwind the guard first releases the fake
    // host and joins both actual pools before strict fixture destruction.
    let mut pools = FixedPoolGuard::start(&mut f, latch.clone());
    let (repeat_start_refused, repeat_start_owner_absent) = match f
        .root
        .service()
        .start_context_executors(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1).unwrap())
    {
        Err(refusal) => {
            let refused = matches!(refusal.error, ContextExecutorStartError::AlreadyStarted);
            let absent = refusal.owner.is_none();
            if let Some(partial) = refusal.owner {
                join_context_pool_owner(*partial);
            }
            (refused, absent)
        }
        Ok(unexpected_owner) => {
            join_context_pool_owner(unexpected_owner);
            (false, false)
        }
    };
    let scope = f
        .root
        .service()
        .context(f.context)
        .unwrap()
        .lock()
        .unwrap()
        .scope()
        .clone();
    let route = PublishedSnapshot::new(
        &f.root.resources().scope,
        Arc::new(QueryContextState::Absent),
    );

    let host_address = f.root.service().context(f.context).unwrap();
    let mut host = host_address.lock().unwrap();
    if let Err((reason, original)) = host.stage_materialization(job) {
        pools.refused.push(original);
        panic!("original M staging refused: {reason:?}");
    }
    drop(host);
    // Matching numeric actors in another runtime grant no submission or
    // report-consumption authority over this already staged original M.
    let before_foreign_pool = all_worker_fees(&f);
    let foreign_owner = match ContextExecutors::start(
        ActorRuntime::new(1),
        actor(),
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ) {
        Ok(owner) => owner,
        Err(refusal) => {
            if let Some(partial) = refusal.owner {
                join_context_pool_owner(*partial);
            }
            panic!("foreign fixture pool startup refused: {:?}", refusal.error);
        }
    };
    let foreign_handles = foreign_owner.handles().unwrap();
    let notice = f.root.service().next_host_ready().unwrap().unwrap();
    let foreign_progress = f
        .root
        .service()
        .advance_context_host(&notice, &foreign_handles);
    drop(foreign_handles);
    join_context_pool_owner(foreign_owner);
    let foreign_pool_refused = matches!(
        foreign_progress,
        Err(ContextHostProgressError::Host(
            crate::control_runtime::context_host_jobs::ContextHostJobError::WrongOwner
        ))
    );
    let after_foreign_pool = all_worker_fees(&f);
    let calls_after_foreign_pool = calls.load(Ordering::SeqCst);
    let initial_progress = f
        .root
        .service()
        .advance_context_host(&notice, pools.handles.as_ref().unwrap())
        .unwrap();
    assert!(
        latch.wait_entered(),
        "the fixed M worker entered the actual host call"
    );
    let returned_before_release = poll_host_returns(&mut f);
    let callbacks_before_release = jobs.retire_callbacks().unwrap();
    let fee_while_running = f.ledger.used(Dimension::Replies.id());

    // This is the actual Context lease timer, independent of the blocked host
    // thread. Zero tasks let this same Tick finish its real close scan.
    let now = Tick(30_000_000_002);
    let advanced = f.runtime.advance_time(now, 1).unwrap();
    let timer_polled = f.runtime.poll_one(now).unwrap();
    let mut timer_input = None;
    let mut saw_internal_wake = false;
    let mut released_by_core = false;
    let mut direct_completion_woke_original = false;
    for effect in context_effects(&mut f) {
        timer_input = Some(effect.input);
        match &effect.effect {
            WorkerAction::Context(ContextEffect::CloseExternalAdmission { .. }) => {
                scope.close_external().unwrap();
            }
            WorkerAction::Context(ContextEffect::SealRoot { .. }) => {
                // No Task or result root has ever been admitted in this scope.
            }
            WorkerAction::Context(ContextEffect::WakeInternal) => {
                saw_internal_wake = true;
            }
            WorkerAction::Context(ContextEffect::ReleaseHost { .. }) => {
                released_by_core = true;
                handoff_release(&mut f, effect);
                continue;
            }
            other => panic!("unexpected actual zero-task lease-close effect: {other:?}"),
        }
        let direct_close = matches!(
            &effect.effect,
            WorkerAction::Context(ContextEffect::CloseExternalAdmission { .. })
        );
        if direct_close {
            // M is held inside the real latch and R has not been submitted.
            // Remove old coalesced marks before testing the completion wake.
            while f.root.service().next_host_ready().unwrap().is_some() {}
        }
        complete_context(&mut f, effect);
        if direct_close {
            let ready = f.root.service().next_host_ready().unwrap();
            direct_completion_woke_original = ready.as_ref().is_some_and(|ready| {
                f.root.service().ready_context(ready).is_some() && ready.context() == f.context
            });
        }
    }
    f.runtime
        .retire_input(context_actor(), timer_input.unwrap())
        .unwrap();
    // Tick already performed the zero-task scan and requested real R.
    // A filtered stale ContinueClose must not create another input or Wake.
    assert!(!saw_internal_wake && released_by_core);
    assert_context_internal_idle(&mut f);

    let release_notice = f.root.service().next_host_ready().unwrap().unwrap();
    let release_progress = f
        .root
        .service()
        .advance_context_host(&release_notice, pools.handles.as_ref().unwrap())
        .unwrap();
    let r_callbacks = wait_host_returns(&mut f).expect("R returns on the independent cleanup pool");
    let release_count_while_m_blocked = releases.load(Ordering::SeqCst);
    let m_still_blocked_during_r = !latch.is_released();
    assert!(f.runtime.poll_one(now).unwrap());
    let mut retained = false;
    let retention_effects = context_effects(&mut f);
    for effect in &retention_effects {
        match &effect.effect {
            WorkerAction::Context(ContextEffect::RetainContext { context }) => {
                assert_eq!(*context, f.context);
                retained = true;
            }
            _ => panic!("only actual HostReleased may emit the retention fact"),
        }
    }
    let callbacks_with_m_blocked = jobs.retire_callbacks().unwrap();
    let raw_drops_with_m_blocked = drops.load(Ordering::SeqCst);

    // The earlier R metadata must complete its real Context FIFO handoff
    // before a later M decision is dispatched. Root can retain this original
    // Establishing row while the M call and its RPC reply still remain live.
    for effect in retention_effects {
        complete_context(&mut f, effect);
    }
    let callbacks_after_early_retention = jobs.retire_callbacks().unwrap();
    assert!(f.runtime.poll_one(now).unwrap());
    f.root.index_control_effects(64).unwrap();
    while let Some(effect) = f.root.take_control_effect(64).unwrap() {
        match &effect.effect {
            WorkerAction::Control {
                effect: ControlEffect::RetireReservation(_),
                ..
            } => {
                assert!(f.root.retire_ticket(&effect).unwrap());
            }
            WorkerAction::Control {
                effect: ControlEffect::Route { state, .. },
                ..
            } => {
                drop(
                    f.runtime
                        .commit_snapshot(&effect, &route, Arc::new(*state))
                        .unwrap(),
                );
                f.root.control_publication_committed(&effect).unwrap();
            }
            _ => panic!("early actual retention does not decide an unfinished M"),
        }
        complete_root(&mut f, effect);
    }
    let early_delivery = external.try_take();
    let no_reply_before_m_exit = early_delivery.is_none();
    drop(early_delivery);
    let fee_after_early_retention = f.ledger.used(Dimension::Replies.id());

    latch.release();
    let m_callbacks = wait_host_returns(&mut f).expect("the original fixed-pool M return is ready");
    assert!(f.runtime.poll_one(now).unwrap());
    let mut decision: Option<ContextEstablishmentDecision> = None;
    let materialization_effects = context_effects(&mut f);
    let callbacks_with_final_effect_pending = jobs.retire_callbacks().unwrap();
    for effect in &materialization_effects {
        match &effect.effect {
            WorkerAction::Context(ContextEffect::EstablishmentDecided {
                context,
                decision: actual,
                ..
            }) => {
                assert_eq!(*context, f.context);
                decision = Some(actual.clone());
            }
            _ => panic!("the real M callback decides the original establishment"),
        }
    }

    // The original Context has physically released. Send its actual decision;
    // this is neither a HostOk shortcut nor a fabricated bootstrap failure.
    let decision = decision.unwrap();
    let actual_outcome = decision.outcome.clone();
    for effect in materialization_effects {
        complete_context(&mut f, effect);
    }
    let source_after_final_handoff = jobs.source_consumed().unwrap();
    assert!(f.runtime.poll_one(now).unwrap());
    f.root.index_control_effects(64).unwrap();
    let mut disposal = None;
    while let Some(effect) = f.root.take_control_effect(64).unwrap() {
        match &effect.effect {
            WorkerAction::Control {
                effect: ControlEffect::RetireReservation(_),
                ..
            } => {
                assert!(f.root.retire_ticket(&effect).unwrap());
            }
            WorkerAction::Control {
                effect: ControlEffect::Route { state, .. },
                ..
            } => {
                drop(
                    f.runtime
                        .commit_snapshot(&effect, &route, Arc::new(*state))
                        .unwrap(),
                );
                f.root.control_publication_committed(&effect).unwrap();
            }
            WorkerAction::Control {
                effect: ControlEffect::Reply { request, outcome },
                ..
            } => {
                assert_eq!(*request, operation());
                assert_eq!(Ok(*outcome), actual_outcome);
            }
            WorkerAction::ControlReply(reply) => {
                assert_eq!(reply.request.token, input);
                disposal = f.root.begin_rejected_ingress_disposal(&effect).unwrap();
                f.root.publish_request_reply(effect).unwrap();
                continue;
            }
            _ => panic!("only the actual retained establishment settlement is expected"),
        }
        complete_root(&mut f, effect);
    }
    let raw_disposal =
        disposal.expect("the actual non-success reply permits original raw disposal");
    source_pools.disposal(raw_disposal);
    let raw_disposed = source_pools::wait_disposal(&f, input);
    let delivered = external.try_take().unwrap();
    drop(delivered);
    drop(external);
    for _ in 0..8 {
        f.root.reap_ready_requests(8).unwrap();
    }

    let context = f.context;
    let removal_time = Tick(41_000_000_002);
    let timer = root_handoff(&mut f, ControlInput::Tick, removal_time);
    let removal = f.root.take_control_effect(64).unwrap().unwrap();
    let removal_source_input = timer.input;
    let idle_owner_source_is_still_live = !f
        .root
        .service()
        .context(context)
        .unwrap()
        .lock()
        .unwrap()
        .retire_owner_control()
        .unwrap();
    f.root.handoff_context_removal(removal).unwrap();
    // This is the independently paid HostInput used for this test's Tick,
    // not evidence about the production builtin Root timer slot.
    f.finish_host_input(timer);
    let removal_source_retired = f
        .runtime
        .input_custody(actor(), removal_source_input)
        .unwrap()
        .is_none();
    let before_holder_move = f
        .root
        .service()
        .take_physically_removed_context(context)
        .unwrap();
    let before_holder_publication = before_holder_move.is_some();
    let original_owner_before_holder = f.root.service().context(context).is_some();
    // M/R physical returns and the real Root decision already happened. The
    // final M report consumer/header still belongs to the original Context.
    let holder_before_final_callbacks = jobs.take_holder_disposal().unwrap();
    let holder_waited_for_final_callbacks = holder_before_final_callbacks.is_none();
    assert!(holder_before_final_callbacks.is_none());
    let holder_notice = f.root.service().next_host_ready().unwrap().unwrap();
    let holder_progress = f
        .root
        .service()
        .advance_context_host(&holder_notice, pools.handles.as_ref().unwrap())
        .unwrap();
    let callbacks_retired = holder_progress.callbacks_retired;
    let owner_control_retired_by_original_removal = holder_progress.owner_control_retired;
    let h_callbacks = wait_host_returns(&mut f).expect("H returns on the original cleanup worker");
    let holder_retired = jobs.retire_holder().unwrap();
    let before_child_move = f
        .root
        .service()
        .take_physically_removed_context(context)
        .unwrap();
    let before_child_removal_publication = before_child_move.is_some();
    let original_owner_before_child_removal = f.root.service().context(context).is_some();

    dispose_context_metadata(&mut f);
    let physically_removed = f
        .root
        .service()
        .take_physically_removed_context(context)
        .unwrap();
    let removed_published = physically_removed.is_some();
    let mut host = physically_removed.expect("the original H and child scope have actually exited");
    let original_owner_removed_after_exit = f.root.service().context(context).is_none();
    let context_dispatch_empty = host.dispatch.is_empty();
    assert!(f.runtime.poll_one(removal_time).unwrap());
    f.root.index_control_effects(64).unwrap();
    let retirement = f.root.take_control_effect(64).unwrap().unwrap();
    let removed_owner_rejects_reused_actor =
        removed_owner_rejects_reused_actor(&mut f, &host, establishment, removal_time);
    let lifecycle_with_own_batch_pending = host.retire_lifecycle().unwrap();
    let admission_paid_while_batch_pending = {
        let capacity = &host.resources_for_test().admission.capacity;
        !capacity.is_released()
            && capacity.resources().unwrap().get(Dimension::Work.id()) > 0
            && capacity.resources().unwrap().get(Dimension::Inputs.id()) >= 2
    };
    host.authorize_parent_retirement(&retirement).unwrap();
    complete_root(&mut f, retirement);
    let lifecycle_retired = host.retire_lifecycle().unwrap();
    let (mut resources, tasks) = host.into_resources();
    assert!(tasks.is_empty());
    resources.admission.work.body_exited().unwrap();
    resources.admission.work.captures_exited().unwrap();
    f.ledger
        .reap_scope_ready(f.root.resources().scope.id(), 32)
        .unwrap();
    resources
        .admission
        .capacity
        .release(&resources.admission.capacity.resources().unwrap())
        .unwrap();
    drop(resources);
    drop(jobs);
    let dropped = drops.load(Ordering::SeqCst);
    // All original Context producers, Scope and Root legs have really exited.
    // Cleanup stayed open through H; shutdown now joins M and cleanup workers.
    pools.finish();
    let factory_thread = *threads.factory.lock().unwrap();
    let materialization_thread = *threads.materialization.lock().unwrap();
    let release_thread = *threads.release.lock().unwrap();
    let holder_thread = *threads.factory_drop.lock().unwrap();
    source_pools.finish();
    f.finish();

    assert!(repeat_start_refused && repeat_start_owner_absent);
    assert!(matches!(
        raw_disposed,
        ExitRolePhase::Exited | ExitRolePhase::Retired
    ));
    assert!(removed_owner_rejects_reused_actor);
    assert!(foreign_pool_refused);
    assert!(direct_completion_woke_original);
    assert_eq!(before_foreign_pool, after_foreign_pool);
    assert_eq!(calls_after_foreign_pool, 0);
    assert_eq!(initial_progress.submitted, 1);
    assert_eq!(initial_progress.returned, 0);
    assert!(!initial_progress.callbacks_retired && !initial_progress.holder_retired);
    assert_eq!(release_progress.submitted, 1);
    assert_eq!(holder_progress.submitted, 1);
    assert!(m_still_blocked_during_r);
    assert!(factory_thread.is_some() && materialization_thread.is_some());
    assert!(release_thread.is_some() && holder_thread.is_some());
    assert_eq!(factory_thread, materialization_thread);
    assert_eq!(release_thread, holder_thread);
    assert_ne!(materialization_thread, release_thread);
    assert_ne!(materialization_thread, Some(poller_thread));
    assert_ne!(release_thread, Some(poller_thread));
    assert_eq!(returned_before_release, 0);
    assert!(!callbacks_before_release);
    assert_eq!(fee_while_running, 1);
    assert_eq!(advanced, 1);
    assert!(timer_polled && !saw_internal_wake && released_by_core && retained);
    assert_eq!(release_count_while_m_blocked, 1);
    assert_eq!(r_callbacks, 1);
    assert!(!callbacks_with_m_blocked);
    assert_eq!(raw_drops_with_m_blocked, 0);
    assert_eq!(m_callbacks, 1);
    assert!(!callbacks_with_final_effect_pending);
    assert!(!callbacks_after_early_retention);
    assert!(no_reply_before_m_exit);
    assert_eq!(fee_after_early_retention, 1);
    assert!(source_after_final_handoff);
    assert!(callbacks_retired && holder_retired);
    assert!(holder_waited_for_final_callbacks);
    assert!(idle_owner_source_is_still_live && owner_control_retired_by_original_removal);
    assert!(!before_holder_publication && !before_child_removal_publication);
    assert!(original_owner_before_holder && original_owner_before_child_removal);
    assert!(removal_source_retired && original_owner_removed_after_exit);
    assert!(removed_published);
    assert!(!lifecycle_with_own_batch_pending);
    assert!(admission_paid_while_batch_pending && lifecycle_retired);
    assert!(context_dispatch_empty);
    assert_eq!(h_callbacks, 1);
    assert_eq!(actual_outcome, Ok(OperationOutcome::LeaseExpired));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(dropped, 4);
}

#[test]
fn accepted_creator_retires_its_original_rpc_while_active_context_keeps_the_credential_holder() {
    let gate = Gate::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let releases = Arc::new(AtomicUsize::new(0));
    let MaterializationFixture {
        mut f,
        input,
        mut external,
        jobs,
        job,
        getters,
        drops,
        mut source_pools,
        ..
    } = MaterializationFixture::new(gate.clone(), calls.clone(), releases.clone());
    let scope = f
        .root
        .service()
        .context(f.context)
        .unwrap()
        .lock()
        .unwrap()
        .scope()
        .clone();
    let route = PublishedSnapshot::new(
        &f.root.resources().scope,
        Arc::new(QueryContextState::Absent),
    );
    let materializer = thread::spawn(move || job.run_on_host());
    gate.entered.wait();
    gate.resume.wait();
    materializer.join().unwrap();
    let m_callbacks = poll_host_returns(&mut f);
    let source_consumed = jobs.source_consumed().unwrap();
    let before_m_effect = jobs.retire_callbacks().unwrap();
    let materialized_at = Tick(3);
    assert!(f.runtime.poll_one(materialized_at).unwrap());
    let mut actual_decision = None;
    for effect in context_effects(&mut f) {
        match &effect.effect {
            WorkerAction::Context(ContextEffect::EstablishmentDecided { decision, .. }) => {
                actual_decision = Some(decision.clone());
            }
            _ => panic!("only the original successful M callback decides establishment"),
        }
        // Fill the original Root return before completing its Context handoff.
        complete_context(&mut f, effect);
    }
    let after_m_effect_before_r = jobs.retire_callbacks().unwrap();
    assert!(f.runtime.poll_one(materialized_at).unwrap());
    f.root.index_control_effects(64).unwrap();
    let mut accepted_reply = false;
    while let Some(effect) = f.root.take_control_effect(64).unwrap() {
        match &effect.effect {
            WorkerAction::Control {
                effect: ControlEffect::Route { state, .. },
                ..
            } => {
                drop(
                    f.runtime
                        .commit_snapshot(&effect, &route, Arc::new(*state))
                        .unwrap(),
                );
                f.root.control_publication_committed(&effect).unwrap();
            }
            WorkerAction::Control {
                effect: ControlEffect::Reply { request, outcome },
                ..
            } => {
                assert_eq!(*request, operation());
                assert_eq!(*outcome, OperationOutcome::Accepted);
            }
            WorkerAction::ControlReply(reply) => {
                accepted_reply = reply.request.token == input
                    && reply.result == RequestResult::Outcome(OperationOutcome::Accepted);
                f.root.publish_request_reply(effect).unwrap();
                continue;
            }
            _ => panic!("the actual Active decision publishes its route and Accepted reply"),
        }
        complete_root(&mut f, effect);
    }

    // Accepted alone grants no disposal. This receipt observes the same M's
    // actual return, restored source loan and original physical call exit.
    let raw = f
        .root
        .take_consumed_creator_disposal(input)
        .unwrap()
        .expect("the actual original M has returned its borrowed source");
    source_pools.disposal(raw);
    let disposed_source = matches!(
        source_pools::wait_disposal(&f, input),
        ExitRolePhase::Exited | ExitRolePhase::Retired
    );
    let delivery = external.try_take().unwrap();
    let delivered_accepted = delivery.body()
        == &ControlReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::Accepted));
    drop(delivery);
    drop(external);
    for _ in 0..8 {
        f.root.reap_ready_requests(8).unwrap();
    }
    let active_route = *route.read();
    let active_context_charge = f.ledger.used(Dimension::Contexts.id());
    let active_rpc_charge = f.ledger.used(Dimension::Replies.id());
    let original_rpc_retired =
        f.root.resources().requests.is_empty() && f.root.request_ready().registered() == 0;
    let drops_while_active = drops.load(Ordering::SeqCst);
    let release_calls_while_active = releases.load(Ordering::SeqCst);

    // Retire the actual Active Context through its own lease and actual R.
    // Its original prepaid source remains Idle when this Tick finishes the scan.
    let closed_at = Tick(30_000_000_002);
    let advanced = f.runtime.advance_time(closed_at, 1).unwrap();
    let timer_polled = f.runtime.poll_one(closed_at).unwrap();
    let mut timer_input = None;
    let mut saw_internal_wake = false;
    let mut core_requested_release = false;
    for effect in context_effects(&mut f) {
        timer_input = Some(effect.input);
        match &effect.effect {
            WorkerAction::Context(ContextEffect::CloseExternalAdmission { .. }) => {
                scope.close_external().unwrap();
            }
            WorkerAction::Context(ContextEffect::SealRoot { .. }) => {
                // No Task or result root was admitted by this fixture.
            }
            WorkerAction::Context(ContextEffect::WakeInternal) => {
                saw_internal_wake = true;
            }
            WorkerAction::Context(ContextEffect::ReleaseHost { .. }) => {
                core_requested_release = true;
                handoff_release(&mut f, effect);
                continue;
            }
            other => panic!("unexpected zero-task Active Context close effect: {other:?}"),
        }
        complete_context(&mut f, effect);
    }
    f.runtime
        .retire_input(context_actor(), timer_input.unwrap())
        .unwrap();
    assert!(!saw_internal_wake && core_requested_release);
    assert_context_internal_idle(&mut f);

    let release = jobs.take_release_job().unwrap().unwrap();
    thread::spawn(move || release.run_on_host()).join().unwrap();
    let r_callbacks = poll_host_returns(&mut f);
    assert!(f.runtime.poll_one(closed_at).unwrap());
    let retention_effects = context_effects(&mut f);
    let before_r_handoff = jobs.retire_callbacks().unwrap();
    let context = f.context;
    let mut actual_retention = false;
    for effect in retention_effects {
        match &effect.effect {
            WorkerAction::Context(ContextEffect::RetainContext { context: actual }) => {
                actual_retention = *actual == context;
            }
            _ => panic!("the original R callback reports its actual retained Context"),
        }
        complete_context(&mut f, effect);
    }
    let callbacks_retired = jobs.retire_callbacks().unwrap();
    assert!(f.runtime.poll_one(closed_at).unwrap());
    f.root.index_control_effects(64).unwrap();
    while let Some(effect) = f.root.take_control_effect(64).unwrap() {
        match &effect.effect {
            WorkerAction::Control {
                effect: ControlEffect::RetireReservation(_),
                ..
            } => {
                assert!(f.root.retire_ticket(&effect).unwrap());
            }
            WorkerAction::Control {
                effect: ControlEffect::Route { state, .. },
                ..
            } => {
                drop(
                    f.runtime
                        .commit_snapshot(&effect, &route, Arc::new(*state))
                        .unwrap(),
                );
                f.root.control_publication_committed(&effect).unwrap();
            }
            _ => panic!("actual retention retires its ticket and publishes retained routing"),
        }
        complete_root(&mut f, effect);
    }

    let removal_time = Tick(41_000_000_002);
    let timer = root_handoff(&mut f, ControlInput::Tick, removal_time);
    let removal = f.root.take_control_effect(64).unwrap().unwrap();
    let removal_source_input = timer.input;
    f.root.handoff_context_removal(removal).unwrap();
    // This is the independently paid HostInput used for this test's Tick,
    // not evidence about the production builtin Root timer slot.
    f.finish_host_input(timer);
    let removal_source_retired = f
        .runtime
        .input_custody(actor(), removal_source_input)
        .unwrap()
        .is_none();
    let before_holder_move = f
        .root
        .service()
        .take_physically_removed_context(context)
        .unwrap();
    let before_holder_publication = before_holder_move.is_some();
    let original_owner_before_holder = f.root.service().context(context).is_some();
    let holder = jobs.take_holder_disposal().unwrap().unwrap();
    thread::spawn(move || holder.run_on_host()).join().unwrap();
    let h_callbacks = poll_host_returns(&mut f);
    let holder_retired = jobs.retire_holder().unwrap();
    let before_child_move = f
        .root
        .service()
        .take_physically_removed_context(context)
        .unwrap();
    let before_child_removal_publication = before_child_move.is_some();
    let original_owner_before_child_removal = f.root.service().context(context).is_some();
    let drops_after_holder = drops.load(Ordering::SeqCst);
    dispose_context_metadata(&mut f);
    let physically_removed = f
        .root
        .service()
        .take_physically_removed_context(context)
        .unwrap();
    let removed_published = physically_removed.is_some();
    let mut host = physically_removed.expect("the original H and child scope have actually exited");
    let original_owner_removed_after_exit = f.root.service().context(context).is_none();
    let context_dispatch_empty = host.dispatch.is_empty();
    assert!(f.runtime.poll_one(removal_time).unwrap());
    f.root.index_control_effects(64).unwrap();
    let retirement = f.root.take_control_effect(64).unwrap().unwrap();
    let lifecycle_with_own_batch_pending = host.retire_lifecycle().unwrap();
    let admission_paid_while_batch_pending = {
        let capacity = &host.resources_for_test().admission.capacity;
        !capacity.is_released()
            && capacity.resources().unwrap().get(Dimension::Work.id()) > 0
            && capacity.resources().unwrap().get(Dimension::Inputs.id()) >= 2
    };
    host.authorize_parent_retirement(&retirement).unwrap();
    complete_root(&mut f, retirement);
    let lifecycle_retired = host.retire_lifecycle().unwrap();
    let (mut resources, tasks) = host.into_resources();
    assert!(tasks.is_empty());
    resources.admission.work.body_exited().unwrap();
    resources.admission.work.captures_exited().unwrap();
    f.ledger
        .reap_scope_ready(f.root.resources().scope.id(), 32)
        .unwrap();
    resources
        .admission
        .capacity
        .release(&resources.admission.capacity.resources().unwrap())
        .unwrap();
    drop(resources);
    drop(jobs);
    source_pools.finish();
    f.finish();

    assert_eq!(m_callbacks, 1);
    assert!(source_consumed && disposed_source);
    assert!(!before_m_effect && !after_m_effect_before_r);
    assert_eq!(
        actual_decision
            .as_ref()
            .map(|decision| (decision.state, decision.outcome.clone())),
        Some((QueryContextState::Active, Ok(OperationOutcome::Accepted)))
    );
    assert!(accepted_reply && delivered_accepted);
    assert_eq!(active_route, QueryContextState::Active);
    assert_eq!(active_context_charge, 1);
    assert_eq!(active_rpc_charge, 0);
    assert!(original_rpc_retired);
    assert_eq!(drops_while_active, 3);
    assert_eq!(release_calls_while_active, 0);
    assert_eq!(advanced, 1);
    assert!(timer_polled && !saw_internal_wake && core_requested_release && actual_retention);
    assert_eq!(r_callbacks, 1);
    assert!(!before_r_handoff);
    assert!(callbacks_retired && holder_retired);
    assert!(!before_holder_publication && !before_child_removal_publication);
    assert!(original_owner_before_holder && original_owner_before_child_removal);
    assert!(removal_source_retired && original_owner_removed_after_exit);
    assert!(removed_published);
    assert!(!lifecycle_with_own_batch_pending);
    assert!(admission_paid_while_batch_pending && lifecycle_retired);
    assert!(context_dispatch_empty);
    assert_eq!(h_callbacks, 1);
    assert_eq!(drops_after_holder, 4);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
}

struct NeverCalledFactory {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl QueryContextHostFactory for NeverCalledFactory {
    fn create_instance(
        self: Arc<Self>,
        _: QueryContextRef,
    ) -> Result<Arc<dyn QueryContextHostInstance>, HostRejection> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        panic!("a queued M withdrawn by the real release fence must not call its factory")
    }
}
impl Drop for NeverCalledFactory {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn a_core_release_before_queued_materialization_never_calls_factory_and_retires_the_original_roles()
{
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let factory_drops = Arc::new(AtomicUsize::new(0));
    let factory: Arc<dyn QueryContextHostFactory> = Arc::new(NeverCalledFactory {
        calls: factory_calls.clone(),
        drops: factory_drops.clone(),
    });
    let MaterializationFixture {
        mut f,
        input,
        mut external,
        jobs,
        job,
        getters,
        drops,
        source_pools,
        ..
    } = MaterializationFixture::with_factory(
        Gate::new(),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Some(factory),
    );
    let mut pools = FixedPoolGuard::start(&mut f, Arc::new(FixedPoolLatch::default()));
    let scope = f
        .root
        .service()
        .context(f.context)
        .unwrap()
        .lock()
        .unwrap()
        .scope()
        .clone();
    let route = PublishedSnapshot::new(
        &f.root.resources().scope,
        Arc::new(QueryContextState::Absent),
    );

    let original_key = job.queue_key();
    let prestop_scope = scope.snapshot().unwrap();
    let prestop_fees = all_worker_fees(&f);
    let (prestop_refusal, job) = match pools.handles.as_ref().unwrap().try_submit_stopped(job) {
        Err((reason, original)) => (reason, original),
        Ok(()) => panic!("an unstopped original M must not enter the cleanup pool"),
    };
    let prestop_same_key = job.queue_key() == original_key;
    let prestop_scope_unchanged = scope.snapshot().unwrap() == prestop_scope;
    let prestop_fees_unchanged = all_worker_fees(&f) == prestop_fees;

    // The original M job is held in this owner, never submitted to a worker.
    // Its original source loan and fee stay resident while Core closes.
    let before_m_return = poll_host_returns(&mut f);
    let source_before_m = jobs.source_consumed().unwrap();
    let raw_drops_before_m = drops.load(Ordering::SeqCst);
    let rpc_fee_before_m = f.ledger.used(Dimension::Replies.id());
    let closed_at = Tick(30_000_000_002);
    let advanced = f.runtime.advance_time(closed_at, 1).unwrap();
    let timer_polled = f.runtime.poll_one(closed_at).unwrap();
    let mut timer_input = None;
    let mut saw_internal_wake = false;
    let mut core_requested_release = false;
    let mut pending_release_effects = Vec::new();
    for effect in context_effects(&mut f) {
        timer_input = Some(effect.input);
        match &effect.effect {
            WorkerAction::Context(ContextEffect::CloseExternalAdmission { .. }) => {
                scope.close_external().unwrap();
            }
            WorkerAction::Context(ContextEffect::SealRoot { .. }) => {
                // No Task or result root has ever been admitted.
            }
            WorkerAction::Context(ContextEffect::WakeInternal) => {
                saw_internal_wake = true;
            }
            WorkerAction::Context(ContextEffect::ReleaseHost { .. }) => {
                pending_release_effects.push(effect);
                continue;
            }
            other => panic!("unexpected actual queued-M lease-close effect: {other:?}"),
        }
        complete_context(&mut f, effect);
    }
    assert!(!saw_internal_wake && !pending_release_effects.is_empty());
    assert_context_internal_idle(&mut f);

    let committed_stop_before_release_effect = scope.gates().unwrap();
    let release_effect_still_unexecuted =
        !core_requested_release && !pending_release_effects.is_empty();
    let factory_before_m = factory_calls.load(Ordering::SeqCst);

    // This original J never entered the ordinary M queue. The negative
    // extraction cannot invent queued cancellation or another returned J.
    let cancel_scope = scope.snapshot().unwrap();
    let cancel_fees = all_worker_fees(&f);
    let cancel_unqueued = match pools
        .handles
        .as_ref()
        .unwrap()
        .cancel_materialization(&jobs)
    {
        Err(error) => error,
        Ok(unexpected_original) => {
            pools.refused.push(unexpected_original);
            panic!("an original held outside the M queue cannot be extracted from it");
        }
    };
    let cancel_scope_unchanged = scope.snapshot().unwrap() == cancel_scope;
    let cancel_fees_unchanged = all_worker_fees(&f) == cancel_fees;
    let held_key_after_cancel = job.queue_key();

    // The same J now enters the actual cleanup worker behind Core's committed
    // stop fence, before this host has executed any ReleaseHost effect.
    pools.submit_stopped(job);
    let m_notice =
        wait_host_notice(&mut f).expect("the original host has a coalesced ready notice");
    let original_notice_before_consumption = f.root.service().ready_context(&m_notice).is_some();
    let source_before_notice_consumption = jobs.source_consumed().unwrap();
    for effect in pending_release_effects {
        core_requested_release = true;
        handoff_release(&mut f, effect);
    }
    f.runtime
        .retire_input(context_actor(), timer_input.unwrap())
        .unwrap();
    // The coalesced host notice is held, but it may also have been marked by
    // an earlier Direct effect completion. It proves no cell return or exit.
    let release_before_m_taken = jobs.take_release_job().unwrap();
    let no_cleanup_worker_occupied = release_before_m_taken.is_none();
    let m_callbacks = match poll_noticed_host_returns(&mut f, &m_notice) {
        0 => wait_host_returns(&mut f).expect("the original stopped-M return becomes consumable"),
        actual => actual,
    };
    let source_after_m = jobs.source_consumed().unwrap();
    assert!(f.runtime.poll_one(closed_at).unwrap());
    let mut actual_decision = None;
    let before_m_effect = jobs.retire_callbacks().unwrap();
    for effect in context_effects(&mut f) {
        match &effect.effect {
            WorkerAction::Context(ContextEffect::EstablishmentDecided { decision, .. }) => {
                actual_decision = Some(decision.clone());
            }
            _ => panic!("the actual withdrawn M callback asks Core for its saved close verdict"),
        }
        complete_context(&mut f, effect);
    }
    let m_retired_but_r_not_issued = jobs.retire_callbacks().unwrap();
    assert!(f.runtime.poll_one(closed_at).unwrap());
    f.root.index_control_effects(64).unwrap();
    let mut lease_reply = false;
    while let Some(effect) = f.root.take_control_effect(64).unwrap() {
        match &effect.effect {
            WorkerAction::Control {
                effect: ControlEffect::Route { state, .. },
                ..
            } => {
                drop(
                    f.runtime
                        .commit_snapshot(&effect, &route, Arc::new(*state))
                        .unwrap(),
                );
                f.root.control_publication_committed(&effect).unwrap();
            }
            WorkerAction::Control {
                effect: ControlEffect::Reply { request, outcome },
                ..
            } => {
                assert_eq!(*request, operation());
                assert_eq!(*outcome, OperationOutcome::LeaseExpired);
            }
            WorkerAction::ControlReply(reply) => {
                lease_reply = reply.request.token == input
                    && reply.result == RequestResult::Outcome(OperationOutcome::LeaseExpired);
                f.root.publish_request_reply(effect).unwrap();
                continue;
            }
            _ => panic!("only Core's original close reason may settle this withdrawn M"),
        }
        complete_root(&mut f, effect);
    }
    // The actual rejection publisher has retained the original raw job in R.
    // Only the unified owner turn dispatches it to the same cleanup pool.
    let raw_progress = f
        .root
        .advance_ready_requests(source_pools.handles(), 8)
        .unwrap();
    let raw_dispatched = raw_progress.dispatched == 1;
    let raw_disposed = matches!(
        source_pools::wait_disposal(&f, input),
        ExitRolePhase::Exited | ExitRolePhase::Retired
    );
    let delivery = external.try_take().unwrap();
    let delivered_lease = delivery.body()
        == &ControlReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::LeaseExpired));
    drop(delivery);
    drop(external);
    for _ in 0..8 {
        f.root
            .advance_ready_requests(source_pools.handles(), 8)
            .unwrap();
    }
    let raw_drops_before_r = drops.load(Ordering::SeqCst);
    let rpc_fee_after_m = f.ledger.used(Dimension::Replies.id());
    let context_fee_before_r = f.ledger.used(Dimension::Contexts.id());
    let factory_still_held = factory_drops.load(Ordering::SeqCst);

    // No instance was created: only Taken plus the actual M exit permits this
    // original R role's physical no-instance cleanup. It still owes its report.
    let release =
        release_before_m_taken.unwrap_or_else(|| jobs.take_release_job().unwrap().unwrap());
    pools.submit(release);
    let r_callbacks =
        wait_host_returns(&mut f).expect("the original no-instance R returned on cleanup");
    assert!(f.runtime.poll_one(closed_at).unwrap());
    let retention_effects = context_effects(&mut f);
    let before_r_effect = jobs.retire_callbacks().unwrap();
    let context = f.context;
    let mut actual_retention = false;
    for effect in retention_effects {
        match &effect.effect {
            WorkerAction::Context(ContextEffect::RetainContext { context: actual }) => {
                actual_retention = *actual == context;
            }
            _ => panic!("only the true R callback retains the closed Context"),
        }
        complete_context(&mut f, effect);
    }
    let callbacks_retired = jobs.retire_callbacks().unwrap();
    assert!(f.runtime.poll_one(closed_at).unwrap());
    f.root.index_control_effects(64).unwrap();
    while let Some(effect) = f.root.take_control_effect(64).unwrap() {
        match &effect.effect {
            WorkerAction::Control {
                effect: ControlEffect::RetireReservation(_),
                ..
            } => {
                assert!(f.root.retire_ticket(&effect).unwrap());
            }
            WorkerAction::Control {
                effect: ControlEffect::Route { state, .. },
                ..
            } => {
                drop(
                    f.runtime
                        .commit_snapshot(&effect, &route, Arc::new(*state))
                        .unwrap(),
                );
                f.root.control_publication_committed(&effect).unwrap();
            }
            _ => panic!("actual retention retires its ticket and publishes retained routing"),
        }
        complete_root(&mut f, effect);
    }

    let removal_time = Tick(41_000_000_002);
    let timer = root_handoff(&mut f, ControlInput::Tick, removal_time);
    let removal = f.root.take_control_effect(64).unwrap().unwrap();
    let removal_source_input = timer.input;
    f.root.handoff_context_removal(removal).unwrap();
    // This is the independently paid HostInput used for this test's Tick,
    // not evidence about the production builtin Root timer slot.
    f.finish_host_input(timer);
    let removal_source_retired = f
        .runtime
        .input_custody(actor(), removal_source_input)
        .unwrap()
        .is_none();
    let before_holder_move = f
        .root
        .service()
        .take_physically_removed_context(context)
        .unwrap();
    let before_holder_publication = before_holder_move.is_some();
    let original_owner_before_holder = f.root.service().context(context).is_some();
    let holder = jobs.take_holder_disposal().unwrap().unwrap();
    pools.submit(holder);
    let h_callbacks =
        wait_host_returns(&mut f).expect("the original factory holder exited on cleanup");
    let holder_retired = jobs.retire_holder().unwrap();
    let before_child_move = f
        .root
        .service()
        .take_physically_removed_context(context)
        .unwrap();
    let before_child_removal_publication = before_child_move.is_some();
    let original_owner_before_child_removal = f.root.service().context(context).is_some();
    let factory_after_holder = factory_drops.load(Ordering::SeqCst);
    let raw_drops_after_holder = drops.load(Ordering::SeqCst);
    dispose_context_metadata(&mut f);
    let physically_removed = f
        .root
        .service()
        .take_physically_removed_context(context)
        .unwrap();
    let removed_published = physically_removed.is_some();
    let mut host = physically_removed.expect("the original H and child scope have actually exited");
    let original_owner_removed_after_exit = f.root.service().context(context).is_none();
    let context_dispatch_empty = host.dispatch.is_empty();
    assert!(f.runtime.poll_one(removal_time).unwrap());
    f.root.index_control_effects(64).unwrap();
    let retirement = f.root.take_control_effect(64).unwrap().unwrap();
    let lifecycle_with_own_batch_pending = host.retire_lifecycle().unwrap();
    let admission_paid_while_batch_pending = {
        let capacity = &host.resources_for_test().admission.capacity;
        !capacity.is_released()
            && capacity.resources().unwrap().get(Dimension::Work.id()) > 0
            && capacity.resources().unwrap().get(Dimension::Inputs.id()) >= 2
    };
    host.authorize_parent_retirement(&retirement).unwrap();
    complete_root(&mut f, retirement);
    let lifecycle_retired = host.retire_lifecycle().unwrap();
    let (mut resources, tasks) = host.into_resources();
    assert!(tasks.is_empty());
    resources.admission.work.body_exited().unwrap();
    resources.admission.work.captures_exited().unwrap();
    f.ledger
        .reap_scope_ready(f.root.resources().scope.id(), 32)
        .unwrap();
    resources
        .admission
        .capacity
        .release(&resources.admission.capacity.resources().unwrap())
        .unwrap();
    drop(resources);
    drop(jobs);
    drop(m_notice);
    pools.finish();
    source_pools.finish();
    f.finish();

    assert_eq!(prestop_refusal, ContextSubmitError::StopRequired);
    assert!(prestop_same_key && prestop_scope_unchanged && prestop_fees_unchanged);
    assert_eq!(
        cancel_unqueued,
        ContextCancelError::Queue(FairQueueError::UnknownWork)
    );
    assert!(cancel_scope_unchanged && cancel_fees_unchanged);
    assert_eq!(held_key_after_cancel, original_key);
    assert!(original_notice_before_consumption);
    assert!(!source_before_notice_consumption);
    assert_eq!(before_m_return, 0);
    assert!(!source_before_m);
    assert_eq!(raw_drops_before_m, 0);
    assert_eq!(rpc_fee_before_m, 1);
    assert_eq!(advanced, 1);
    assert!(timer_polled && !saw_internal_wake && core_requested_release);
    assert!(committed_stop_before_release_effect.stopped);
    assert!(!committed_stop_before_release_effect.failed);
    assert_eq!(
        committed_stop_before_release_effect.generation,
        generation()
    );
    assert!(release_effect_still_unexecuted);
    assert_eq!(factory_before_m, 0);
    assert!(no_cleanup_worker_occupied);
    assert_eq!(m_callbacks, 1);
    assert!(source_after_m && raw_dispatched && raw_disposed);
    assert!(!before_m_effect && !m_retired_but_r_not_issued);
    assert_eq!(
        actual_decision
            .as_ref()
            .map(|decision| (decision.state, decision.outcome.clone())),
        Some((
            QueryContextState::Aborting,
            Ok(OperationOutcome::LeaseExpired)
        ))
    );
    assert!(lease_reply && delivered_lease);
    assert_eq!(raw_drops_before_r, 4);
    assert_eq!(rpc_fee_after_m, 0);
    assert_eq!(context_fee_before_r, 1);
    assert_eq!(factory_still_held, 0);
    assert_eq!(r_callbacks, 1);
    assert!(!before_r_effect);
    assert!(actual_retention && callbacks_retired && holder_retired);
    assert!(!before_holder_publication && !before_child_removal_publication);
    assert!(original_owner_before_holder && original_owner_before_child_removal);
    assert!(removal_source_retired && original_owner_removed_after_exit);
    assert!(removed_published);
    assert!(!lifecycle_with_own_batch_pending);
    assert!(admission_paid_while_batch_pending && lifecycle_retired);
    assert!(context_dispatch_empty);
    assert_eq!(h_callbacks, 1);
    assert_eq!(factory_after_holder, 1);
    assert_eq!(raw_drops_after_holder, 4);
    assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
}
