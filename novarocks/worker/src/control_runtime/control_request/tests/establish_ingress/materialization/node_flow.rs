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

//! One real Node lifecycle, without manual effect dispatch, acknowledgement,
//! Scope exit or report consumption. Only the foreign Context I/O is symbolic.

use super::*;
use crate::control_runtime::{
    context_host::TaskDriverPorts,
    control_internal::CONTROL_INTERNAL_STORAGE,
    node::{NodeActorIds, NodeContextConfig, WorkerNodeHost},
    request_endpoint::{RequestDelivery, RequestHandle},
};
use novarocks_control_runtime::driver::{Clock, PollerPool};
use novarocks_worker_protocol::context::{ContextCommand, ContextReplyBody, RequestResult};
use std::{mem::ManuallyDrop, num::NonZeroU64, sync::atomic::AtomicU64, time::Duration};

#[derive(Default)]
struct ScenarioClock(AtomicU64);
impl Clock for ScenarioClock {
    fn now(&self) -> Tick {
        Tick(self.0.load(Ordering::SeqCst))
    }
}
#[derive(Default)]
struct Calls {
    factories: AtomicUsize,
    materializations: AtomicUsize,
    releases: AtomicUsize,
    instance_drops: AtomicUsize,
    on_host: AtomicUsize,
}
impl Calls {
    fn observe_host(&self) {
        self.on_host.fetch_add(
            usize::from(
                std::thread::current()
                    .name()
                    .is_some_and(|name| name.starts_with("worker-host-")),
            ),
            Ordering::SeqCst,
        );
    }
}
struct Factory(Arc<Calls>);
struct Instance {
    context: QueryContextRef,
    calls: Arc<Calls>,
}
impl QueryContextHostFactory for Factory {
    fn create_instance(
        self: Arc<Self>,
        context: QueryContextRef,
    ) -> Result<Arc<dyn QueryContextHostInstance>, HostRejection> {
        self.0.observe_host();
        self.0.factories.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Instance {
            context,
            calls: self.0.clone(),
        }))
    }
}
impl QueryContextHostInstance for Instance {
    fn context(&self) -> QueryContextRef {
        self.context
    }
    fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        if request.context() != self.context {
            panic!("the real Node must transfer its original Context source");
        }
        self.calls.observe_host();
        self.calls.materializations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn release(&self) -> ReleasedContextEvidence {
        self.calls.observe_host();
        self.calls.releases.fetch_add(1, Ordering::SeqCst);
        ReleasedContextEvidence::none()
    }
    fn advance_shared_domain(
        &self,
        _: &novarocks_execution_contract::task_execution::operation::QueryContextDomainUpdate,
    ) -> Result<(), HostRejection> {
        panic!("the cold Node scenario has no shared-domain update");
    }
}
impl Drop for Instance {
    fn drop(&mut self) {
        self.calls.observe_host();
        self.calls.instance_drops.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) struct Scenario<T = ()> {
    pub(super) node: Arc<WorkerNodeHost>,
    pollers: PollerPool<WorkerActor>,
    sources: crate::control_runtime::establish_executors::EstablishExecutors,
    contexts: ContextExecutors,
    pub(super) runtime: ActorRuntime<WorkerActor>,
    pub(super) ledger: RuntimeLedger,
    clock: Arc<ScenarioClock>,
    calls: Arc<Calls>,
    task_resources: Option<T>,
    pub(super) diagnostic_task:
        Mutex<Option<(QueryContextRef, novarocks_execution_contract::TaskIdentity)>>,
}
impl<T: Send + 'static> Scenario<T> {
    fn fail<O: Send + 'static>(self, stage: &str, original: O) -> ! {
        let task = *self.diagnostic_task.lock().unwrap();
        if let Some((context, task)) = task {
            self.node.print_neutral_task_inventory(context, task);
        }
        eprintln!(
            "Node scenario failure at {stage}; clock={:?}; Root={:?}; Context={:?}; Requests={}; Contexts={}; Tickets={}; Work={}",
            self.clock.now(),
            self.runtime.snapshot(actor()),
            self.runtime.snapshot(context_actor()),
            self.ledger.used(Dimension::Requests.id()),
            self.ledger.used(Dimension::Contexts.id()),
            self.ledger.used(Dimension::Tickets.id()),
            self.ledger.used(Dimension::Work.id())
        );
        retain_failure(stage, (self, original));
    }
}
pub(super) fn retain_failure<T: Send + 'static>(stage: &str, originals: T) -> ! {
    eprintln!(
        "retaining the actual Node/pool/refusal inventory at {stage}; no exit or cleanup inferred"
    );
    // Failure-only evidence custody. Do not unwind/drop unjoined pools or
    // opaque refusal originals and obscure the first failure with fake cleanup.
    std::panic::panic_any(ManuallyDrop::new(originals));
}
pub(super) fn operation_number(number: u8) -> TaskOperationId {
    let mut bytes = [number; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    TaskOperationId::try_from_bytes(bytes).unwrap()
}
pub(super) fn wait_until(predicate: impl FnMut() -> bool) -> bool {
    wait_until_deadline(Instant::now() + Duration::from_secs(5), predicate)
}
fn wait_until_deadline(deadline: Instant, mut predicate: impl FnMut() -> bool) -> bool {
    loop {
        if predicate() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::yield_now();
    }
}
pub(super) fn wait_delivery<B, H: Copy>(
    handle: &mut RequestHandle<B, H>,
) -> Option<RequestDelivery<B, H>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(5), handle)
                .await
                .ok()?
                .ok()
        })
}

pub(super) enum NodeClose {
    Release,
    Abort,
}
pub(super) fn run_node_lifecycle_with_task<T: Send + 'static>(
    workers: usize,
    close: NodeClose,
    task_geometry: ResourceVector,
    start_task_ports: impl FnOnce(
        &mut ProcessControlOwner,
    ) -> Result<(TaskDriverPorts, T), Box<dyn std::any::Any + Send>>,
    exercise_task: impl FnOnce(
        &Scenario<T>,
        QueryContextRef,
        &mut T,
    ) -> Result<(), Box<dyn std::any::Any + Send>>,
    shutdown_task_ports: impl FnOnce(T) -> Result<(), Box<dyn std::any::Any + Send>>,
) {
    let close_kind = match &close {
        NodeClose::Release => "QuiesceRelease",
        NodeClose::Abort => "AbortQueryFailed",
    };
    eprintln!(
        "actual Node branch begin: workers={workers}, close={close_kind}, task_enabled={}",
        task_geometry.get(Dimension::ActiveTasks.id()) > 0,
    );
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
        .checked_add(CONTROL_INTERNAL_STORAGE)
        .unwrap()
        .resources()
        .unwrap()
        .checked_add(
            &ResourceVector::new([(Dimension::Tickets.id(), 1), (Dimension::Contexts.id(), 1)])
                .unwrap(),
        )
        .unwrap()
        .checked_add(&task_geometry)
        .unwrap();
    let storage = AdmissionStorage {
        loading: policy.loading.resources().unwrap(),
        accepted: policy.accepted.resources().unwrap(),
        context: CONTEXT_BOOTSTRAP_STORAGE.resources().unwrap(),
        ticket: ticket_storage.resources().unwrap(),
    };
    // Fixture supplies only the original Root/bootstrap geometry. No Fixture
    // method polls, dispatches, publishes or tears down after this point.
    let extra_headers = [
        Dimension::Work,
        Dimension::Inputs,
        Dimension::Completions,
        Dimension::Captures,
    ]
    .into_iter()
    .try_fold(0usize, |sum, dimension| {
        sum.checked_add(usize::try_from(task_geometry.get(dimension.id())).unwrap())
    })
    .unwrap();
    let reservations = 128usize.checked_add(extra_headers).unwrap();
    let mut f =
        Fixture::with_geometry_records(2, 2, 64, storage.clone(), additional, 8, 3, reservations);
    let (raw, getters, source_drops) = request(&f, None, None, false);
    let one = NonZeroUsize::new(1).unwrap();
    let sources = match f.root.start_establish_executors(one, one) {
        Ok(owner) => owner,
        Err(original) => retain_failure("source pool startup", (f, raw, original)),
    };
    let contexts = match f.root.service().start_context_executors(one, one) {
        Ok(owner) => owner,
        Err(original) => retain_failure("Context pool startup", (f, raw, sources, original)),
    };
    let source_handles = match sources.handles() {
        Some(handles) => handles,
        None => retain_failure("source pool handles", (f, raw, sources, contexts)),
    };
    let context_handles = match contexts.handles() {
        Some(handles) => handles,
        None => retain_failure("Context pool handles", (f, raw, sources, contexts)),
    };
    let (task_ports, task_resources) = match start_task_ports(&mut f.root) {
        Ok(originals) => originals,
        Err(original) => retain_failure(
            "Task physical startup",
            (f, raw, sources, contexts, original),
        ),
    };
    let Fixture {
        root,
        runtime,
        ledger,
        capability,
        context,
        requests,
    } = f;
    if !requests.is_empty() {
        retain_failure(
            "bootstrap unexpectedly contains RPCs",
            (root, raw, sources, contexts, requests),
        );
    }
    let calls = Arc::new(Calls::default());
    let node = match WorkerNodeHost::from_started(
        root,
        context_handles,
        task_ports,
        source_handles,
        Arc::new(Factory(calls.clone())),
        NodeContextConfig {
            core: ContextConfig {
                max_tasks: 1,
                max_creation_waiters: 1,
                max_pending_requests: 4,
                max_preparing: 1,
                max_task_bytes: 1024,
                step_budget: ContextCore::MAX_STEP_WORK,
                lease_bounds: LeaseBounds::DEFAULT,
                metric_publish_interval: Duration::from_millis(100),
            },
            policy,
            storage,
            max_live_tasks: 1,
            max_live_roots: 1,
            max_contexts: 2,
        },
        NodeActorIds::new(NonZeroU64::new(4).unwrap()),
    ) {
        Ok(node) => Arc::new(node),
        Err(original) => retain_failure(
            "Node composition",
            (raw, sources, contexts, task_resources, original),
        ),
    };
    let clock = Arc::new(ScenarioClock::default());
    let pollers = match node.start_pollers(clock.clone(), workers) {
        Ok(pool) => pool,
        Err(original) => retain_failure(
            "Common poller startup",
            (node, raw, sources, contexts, task_resources, original),
        ),
    };
    let mut scene = Scenario {
        node,
        pollers,
        sources,
        contexts,
        runtime,
        ledger,
        clock,
        calls,
        task_resources: Some(task_resources),
        diagnostic_task: Mutex::new(None),
    };
    let deadline = MonotonicInstant::from_origin(Duration::from_secs(100));
    let acquisition = operation_number(12);
    let mut acquire = match scene.node.submit_control(
        ActorId::new(3).unwrap(),
        acquisition,
        deadline,
        ControlInput::Acquire {
            request: AcquireQueryContextAdmissionTicket::new(
                acquisition,
                context,
                LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
                NativeCompatibilityId::new([3; 32]),
                capability,
            ),
            ticket: AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
        },
    ) {
        Ok(handle) => handle,
        Err(original) => scene.fail("Acquire submission", (raw, original)),
    };
    let Some(delivery) = wait_delivery(&mut acquire) else {
        scene.fail("Acquire delivery timeout", (raw, acquire));
    };
    if !matches!(delivery.body(), ControlReplyBody::Admission(Ok(_)))
        || delivery.request() != acquire.request()
    {
        scene.fail("Acquire original reply", (raw, delivery, acquire));
    }
    drop(delivery);
    drop(acquire);

    let mut establish = match scene.node.submit_establish(
        ActorId::new(2).unwrap(),
        operation(),
        deadline,
        raw,
        PayloadId(1),
    ) {
        Ok(handle) => handle,
        Err(original) => scene.fail("Establish submission", original),
    };
    let Some(delivery) = wait_delivery(&mut establish) else {
        scene.fail("Establish delivery timeout", establish);
    };
    if !matches!(
        delivery.body(),
        ControlReplyBody::Context(ack)
            if ack.outcome == OperationOutcome::Accepted
                && ack.receipt.context() == context
                && ack.receipt.lease().is_some()
    ) || delivery.request() != establish.request()
    {
        scene.fail("Establish original accepted reply", (delivery, establish));
    }
    drop(delivery);
    drop(establish);
    if !wait_until(|| {
        scene
            .node
            .route(context)
            .is_some_and(|route| route.read().state == QueryContextState::Active)
    }) {
        scene.fail("actual Active route publication timeout", ());
    }
    if !wait_until(|| scene.ledger.used(Dimension::Requests.id()) == 0) {
        scene.fail("original Root RPC retirement timeout", ());
    }
    let mut task_resources = scene
        .task_resources
        .take()
        .expect("same original Task pools");
    if let Err(original) = exercise_task(&scene, context, &mut task_resources) {
        scene.fail("actual Task lifecycle", (task_resources, original));
    }
    scene.task_resources = Some(task_resources);
    match close {
        NodeClose::Release => {
            let quiesce_operation = operation_number(13);
            let mut quiesce = match scene.node.submit_context(
                context,
                ActorId::new(3).unwrap(),
                quiesce_operation,
                deadline,
                ContextCommand::Quiesce,
            ) {
                Ok(handle) => handle,
                Err(original) => scene.fail("Quiesce submission", original),
            };
            let Some(delivery) = wait_delivery(&mut quiesce) else {
                scene.fail("Quiesce delivery timeout", quiesce);
            };
            if !matches!(
                delivery.body(),
                ContextReplyBody::Quiesce {
                    outcome: OperationOutcome::Accepted,
                    ..
                }
            ) || delivery.request() != quiesce.request()
            {
                scene.fail("actual empty-Task Quiesce receipt", (delivery, quiesce));
            }
            drop(delivery);
            drop(quiesce);
            if !wait_until(|| scene.ledger.used(Dimension::Requests.id()) == 0) {
                scene.fail("Quiesce external/physical retirement timeout", ());
            }
            // FE retries only the ordinary ReleaseNotReady verdict. Every attempt
            // uses the same wire operation and a new independently admitted R; the
            // preceding external delivery and original R must really retire first.
            let release_operation = operation_number(14);
            let release_expires = Instant::now() + Duration::from_secs(5);
            let mut attempts = 0usize;
            let mut last_not_ready = false;
            let mut retry_backoff = Duration::from_millis(50);
            loop {
                if Instant::now() >= release_expires {
                    eprintln!(
                        "actual Release timeout: attempts={attempts}, last={:?}",
                        last_not_ready.then_some(OperationOutcome::ReleaseNotReady)
                    );
                    scene.fail("Release retry deadline", ());
                }
                let mut release = match scene.node.submit_context(
                    context,
                    ActorId::new(3).unwrap(),
                    release_operation,
                    deadline,
                    ContextCommand::Release,
                ) {
                    Ok(handle) => handle,
                    Err(original) => scene.fail("Release submission", original),
                };
                attempts += 1;
                let mut original = None;
                if !wait_until_deadline(release_expires, || {
                    original = release.try_take();
                    original.is_some()
                }) {
                    eprintln!(
                        "actual Release delivery timeout: attempts={attempts}, last={:?}",
                        last_not_ready.then_some(OperationOutcome::ReleaseNotReady)
                    );
                    scene.fail("Release delivery timeout", release);
                }
                let delivery = original.expect("the observed original Release delivery");
                if delivery.request() != release.request() {
                    scene.fail("Release original request identity", (delivery, release));
                }
                let retry = match delivery.body() {
                    ContextReplyBody::Release {
                        release: novarocks_execution_contract::ReleaseOutcome::Released,
                        ..
                    } => false,
                    ContextReplyBody::Release {
                        release: novarocks_execution_contract::ReleaseOutcome::NotReady,
                        ..
                    } => true,
                    _ => {
                        eprintln!("actual Release refusal: {:?}", delivery.body());
                        scene.fail("actual released reply", (delivery, release));
                    }
                };
                drop(delivery);
                drop(release);
                if !retry {
                    break;
                }
                last_not_ready = true;
                if !wait_until_deadline(release_expires, || {
                    scene.ledger.used(Dimension::Requests.id()) == 0
                }) {
                    eprintln!(
                        "actual Release retry retirement timeout: attempts={attempts}, last={:?}",
                        OperationOutcome::ReleaseNotReady
                    );
                    scene.fail("Release original R retirement timeout", ());
                }
                // Match the actual FE caller's bounded retry cadence. This
                // external test thread may wait; no poller sleeps, no elapsed
                // time proves cleanup, and the original deadline never moves.
                let wait =
                    retry_backoff.min(release_expires.saturating_duration_since(Instant::now()));
                if !wait.is_zero() {
                    std::thread::sleep(wait);
                }
                retry_backoff = retry_backoff.saturating_mul(2).min(Duration::from_secs(1));
            }
        }
        NodeClose::Abort => {
            // An unread Root cannot be released as if it were acknowledged.
            // The client explicitly cancels this Context through its real RPC.
            let mut abort = match scene.node.submit_context(
                context,
                ActorId::new(3).unwrap(),
                operation_number(15),
                deadline,
                ContextCommand::Abort {
                    cause: novarocks_execution_contract::AbortCause::QueryFailed,
                },
            ) {
                Ok(handle) => handle,
                Err(original) => scene.fail("Abort submission", original),
            };
            let Some(delivery) = wait_delivery(&mut abort) else {
                scene.fail("Abort delivery timeout", abort);
            };
            if !matches!(
                delivery.body(),
                ContextReplyBody::Context(ack) if ack.outcome == OperationOutcome::Accepted
            ) || delivery.request() != abort.request()
            {
                eprintln!("actual Abort refusal: {:?}", delivery.body());
                scene.fail("actual Abort reply", (delivery, abort));
            }
            drop(delivery);
            drop(abort);
            if !wait_until(|| scene.ledger.used(Dimension::Requests.id()) == 0) {
                scene.fail("Abort original R retirement timeout", ());
            }
        }
    }
    // Root must really consume Retained at Tick0 before the clock advances;
    // otherwise a later receipt would correctly start a later retention lease.
    if !wait_until(|| {
        scene
            .node
            .route(context)
            .is_some_and(|route| route.read().state == QueryContextState::TerminalRetained)
    }) {
        scene.fail("actual retained route publication timeout", ());
    }
    scene.clock.0.store(11_000_000_000, Ordering::SeqCst);
    scene.runtime.notify_pollers();
    if !wait_until(|| {
        scene.node.route(context).is_none()
            && scene.ledger.used(Dimension::Contexts.id()) == 0
            && scene.ledger.used(Dimension::Requests.id()) == 0
            && scene.ledger.used(Dimension::Tickets.id()) == 0
    }) {
        scene.fail("H/child/parent actual retirement timeout", ());
    }
    if scene.calls.factories.load(Ordering::SeqCst) != 1
        || scene.calls.materializations.load(Ordering::SeqCst) != 1
        || scene.calls.releases.load(Ordering::SeqCst) != 1
        || scene.calls.instance_drops.load(Ordering::SeqCst) != 1
        || scene.calls.on_host.load(Ordering::SeqCst) != 4
        || getters.load(Ordering::SeqCst) != 3
        || source_drops.load(Ordering::SeqCst) != 4
    {
        scene.fail(
            "actual I/O and original source destructor counts",
            (getters, source_drops),
        );
    }
    if let Err(original) = scene.node.begin_drain() {
        scene.fail("Node begin_drain", original);
    }
    if !wait_until(|| scene.runtime.is_empty() && scene.ledger.is_empty()) {
        scene.fail("Root actual metadata and Scope drain timeout", ());
    }
    let completed_task = *scene.diagnostic_task.lock().unwrap();
    let Scenario {
        node,
        pollers,
        sources,
        contexts,
        runtime,
        ledger,
        task_resources,
        ..
    } = scene;
    let task_resources = task_resources.expect("same original Task pools");
    if let Err(original) = pollers.shutdown() {
        retain_failure(
            "Common physical joins",
            (
                node,
                original,
                sources,
                contexts,
                runtime,
                ledger,
                task_resources,
            ),
        );
    }
    if let Err(original) = contexts.shutdown() {
        retain_failure(
            "Context physical joins",
            (node, original, sources, runtime, ledger, task_resources),
        );
    }
    if let Err(original) = sources.shutdown() {
        retain_failure(
            "source physical joins",
            (node, original, runtime, ledger, task_resources),
        );
    }
    if let Err(original) = shutdown_task_ports(task_resources) {
        retain_failure("Task physical shutdown", (node, original, runtime, ledger));
    }
    drop(node);
    assert!(runtime.is_empty());
    assert!(ledger.is_empty());
    eprintln!(
        "actual Node branch joined: workers={workers}, close={close_kind}, had_task={}, task={:?}, runtime_empty=true, ledger_empty=true",
        completed_task.is_some(),
        completed_task.map(|(_, task)| task),
    );
}

// A Context still owns real RPC notification callbacks when Task intake is
// closed. This pool has no preparation/execution/loading capability.
type ColdNotificationPool = crate::control_runtime::host_executor::HostExecutor<
    crate::control_runtime::context_host::task_driver::TaskDriverHostJob,
    fn(crate::control_runtime::context_host::task_driver::TaskDriverHostJob),
>;
fn run_cold_notification(
    job: crate::control_runtime::context_host::task_driver::TaskDriverHostJob,
) {
    job.run_on_host();
}
fn start_cold_notifications(
    root: &mut ProcessControlOwner,
) -> Result<(TaskDriverPorts, ColdNotificationPool), Box<dyn std::any::Any + Send>> {
    use crate::control_runtime::host_executor::{HostExecutor, HostExecutorLimits};
    let capacity = root
        .task_capacity_signal()
        .map_err(|error| Box::new(error) as Box<dyn std::any::Any + Send>)?;
    let workers = NonZeroUsize::new(1).unwrap();
    let bound = NonZeroUsize::new(
        root.request_ready()
            .limit()
            .get()
            .checked_add(workers.get())
            .unwrap(),
    )
    .unwrap();
    let limits = HostExecutorLimits {
        contexts: bound,
        jobs: bound,
        per_context: bound,
        workers,
    };
    let lifecycle =
        HostExecutor::start_with_waker(limits, run_cold_notification as fn(_), capacity.waker())
            .map_err(|original| Box::new(original) as Box<dyn std::any::Any + Send>)?;
    Ok((
        TaskDriverPorts {
            lifecycle: Some(lifecycle.handle()),
            capacity: Some(capacity),
            ..TaskDriverPorts::default()
        },
        lifecycle,
    ))
}
fn shutdown_cold_notifications(
    original: ColdNotificationPool,
) -> Result<(), Box<dyn std::any::Any + Send>> {
    original
        .shutdown()
        .map(drop)
        .map_err(|original| Box::new(original) as Box<dyn std::any::Any + Send>)
}

#[test]
fn real_node_context_lifecycle_runs_on_one_and_two_common_pollers() {
    for workers in [1, 2] {
        run_node_lifecycle_with_task(
            workers,
            NodeClose::Release,
            ResourceVector::default(),
            start_cold_notifications,
            |_, _, _| Ok(()),
            shutdown_cold_notifications,
        );
    }
}
