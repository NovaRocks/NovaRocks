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

mod materialization_boundaries;
pub(super) fn accepted_creator_retains_only_context_custody() {
    materialization_boundaries::accepted_creator_retains_only_context_custody();
}
pub(super) fn blocked_materialization_keeps_control_live() {
    materialization_boundaries::blocked_materialization_keeps_control_live();
}
pub(super) fn queued_materialization_withdraws_without_factory() {
    materialization_boundaries::queued_materialization_withdraws_without_factory();
}

use super::*;
use crate::control_runtime::{
    context_host::TaskDriverPorts,
    control_internal::CONTROL_INTERNAL_STORAGE,
    node::{NodeActorIds, NodeContextConfig, WorkerNodeHost},
    request_endpoint::{RequestDelivery, RequestHandle},
};
use novarocks_control_runtime::driver::{Clock, PollerPool};
use novarocks_worker_protocol::{
    context::{ContextCommand, ContextReplyBody},
    creation::RequestContext,
};

pub(super) type HeldTaskDelivery = (
    RequestDelivery<ContextReplyBody, crate::task_proposal_ingress::RequestMetadata>,
    RequestContext,
);
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
            "Node scenario failure at {stage}; clock={:?}; Root={:?}; Context={:?}; Requests={}; ContextRoles={:?}; RootRoles={:?}",
            self.clock.now(),
            self.runtime.snapshot(actor()),
            self.runtime.snapshot(context_actor()),
            self.ledger.used(Dimension::Requests.id()),
            self.runtime
                .snapshot(context_actor())
                .map(|snapshot| snapshot.roles),
            self.runtime
                .snapshot(actor())
                .map(|snapshot| snapshot.roles)
        );
        eprintln!(
            "original execution observation={:?}; original charge observation={:?}",
            self.node.try_execution_quiescence_snapshot(),
            self.ledger.try_capacity_usage(
                [
                    Dimension::ActiveTasks.id(),
                    Dimension::PreparingTasks.id(),
                    Dimension::PreparingBytes.id(),
                    Dimension::Requests.id(),
                ],
                Dimension::PreparingTasks.id()
            )
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
fn start_scenario<T: Send + 'static>(
    workers: usize,
    positions: usize,
    task_geometry: ReservationResources,
    start_task_ports: impl FnOnce(
        &mut ProcessControlOwner,
    ) -> Result<(TaskDriverPorts, T), Box<dyn std::any::Any + Send>>,
    make_factory: impl FnOnce(Arc<Calls>) -> Arc<dyn QueryContextHostFactory>,
) -> StartedScenario<T> {
    let policy = ControlRuntimePolicy::try_new(IngressRoleLimits {
        ordinary_running: 1,
        ordinary_waiting: 0,
        control_running: 1,
        control_waiting: 0,
        max_batch_items: 1,
        root_read_holders: 1,
    })
    .unwrap();
    // Empty-Context scenarios still compose the same production execution
    // observer. Declare its actual four-dimension limits from the existing
    // compiled-host fixture; missing dimensions mean unavailable, never zero.
    // This grants no Task/Work, and already configured Task geometry stays exact.
    let mut task_geometry = task_geometry;
    for (dimension, limit) in super::node_task_flow::compiled_task_geometry().quota.iter() {
        if task_geometry.quota.get(dimension) == 0 {
            task_geometry.quota = task_geometry
                .quota
                .checked_add(
                    &novarocks_control_runtime_core::capacity::ResourceVector::new([(
                        dimension, limit,
                    )])
                    .unwrap(),
                )
                .unwrap();
        }
    }
    let additional = ESTABLISH_REQUEST_STORAGE
        .checked_sub(CONTROL_REQUEST_STORAGE)
        .unwrap()
        .checked_add(CONTEXT_BOOTSTRAP_STORAGE)
        .unwrap()
        .checked_add(BRIDGE_STORAGE.checked_mul(2).unwrap())
        .unwrap()
        .checked_add(CONTROL_INTERNAL_STORAGE)
        .unwrap()
        .resources()
        .unwrap()
        .checked_add(&task_geometry)
        .unwrap();
    let storage = AdmissionStorage {
        loading: policy.loading.resources().unwrap(),
        accepted: policy.accepted.resources().unwrap(),
        context: CONTEXT_BOOTSTRAP_STORAGE.resources().unwrap(),
    };
    // Fixture supplies only the original Root/bootstrap geometry. No Fixture
    // method polls, dispatches, publishes or tears down after this point.
    // The production Node configures this actual usage index at bootstrap.
    // A missing index is an unavailable observation, never a zero sample.
    let mut f = Fixture::with_control_storage_and_usage(
        positions,
        positions,
        storage.clone(),
        additional,
        [Dimension::PreparingTasks.id()],
    );
    let configured = f
        .ledger
        .try_capacity_usage(
            [
                Dimension::ActiveTasks.id(),
                Dimension::PreparingTasks.id(),
                Dimension::PreparingBytes.id(),
                Dimension::Requests.id(),
            ],
            Dimension::PreparingTasks.id(),
        )
        .unwrap()
        .expect("no physical producer or poller exists during this original ledger check");
    assert!(configured.used.iter().all(|used| *used == 0));
    let (raw, getters, source_drops) = request(&f, None, None, false);
    let one = NonZeroUsize::new(1).unwrap();
    let sources = match f.root.start_establish_executors(one, one) {
        Ok(owner) => owner,
        Err(original) => retain_failure("source pool startup", (f, raw, original)),
    };
    let contexts = match start_original_context_executors(&mut f) {
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
        positions,
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
    let clock = Arc::new(ScenarioClock::default());
    let request_horizon = crate::RequestHorizon::DEFAULT;
    let node = match WorkerNodeHost::from_started(
        root,
        context_handles,
        task_ports,
        source_handles,
        make_factory(calls.clone()),
        NodeContextConfig {
            core: ContextConfig {
                max_tasks: 1,
                max_creation_waiters: positions,
                max_pending_requests: positions,
                max_preparing: 1,
                max_task_bytes: 1024,
                step_budget: ContextCore::MAX_STEP_WORK,
                lease_bounds: LeaseBounds::DEFAULT,
                metric_publish_interval: Duration::from_millis(100),
                request_horizon,
            },
            policy,
            storage,
            max_live_tasks: 1,
            max_live_roots: 1,
        },
        NodeActorIds::new(NonZeroU64::new(4).unwrap()),
        clock.clone(),
    ) {
        Ok(node) => Arc::new(node),
        Err(original) => retain_failure(
            "Node composition",
            (raw, sources, contexts, task_resources, original),
        ),
    };
    let pollers = match node.start_pollers(workers) {
        Ok(pool) => pool,
        Err(original) => retain_failure(
            "Common poller startup",
            (node, raw, sources, contexts, task_resources, original),
        ),
    };
    let scene = Scenario {
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
    StartedScenario {
        scene,
        raw,
        getters,
        source_drops,
        context,
        capability,
    }
}

struct StartedScenario<T> {
    scene: Scenario<T>,
    raw: EstablishQueryContext,
    getters: Arc<AtomicUsize>,
    source_drops: Arc<AtomicUsize>,
    context: QueryContextRef,
    capability: AdmissionEpochCapability,
}

pub(super) fn run_node_lifecycle_with_task<T: Send + 'static>(
    workers: usize,
    close: NodeClose,
    task_geometry: ReservationResources,
    start_task_ports: impl FnOnce(
        &mut ProcessControlOwner,
    ) -> Result<(TaskDriverPorts, T), Box<dyn std::any::Any + Send>>,
    exercise_task: impl FnOnce(
        &Scenario<T>,
        QueryContextRef,
        &mut T,
    ) -> Result<Option<HeldTaskDelivery>, Box<dyn std::any::Any + Send>>,
    shutdown_task_ports: impl FnOnce(T) -> Result<(), Box<dyn std::any::Any + Send>>,
) {
    let close_kind = match &close {
        NodeClose::Release => "QuiesceRelease",
        NodeClose::Abort => "AbortQueryFailed",
    };
    eprintln!(
        "actual Node branch begin: workers={workers}, close={close_kind}, task_enabled={}",
        task_geometry.quota.get(Dimension::ActiveTasks.id()) > 0,
    );
    let StartedScenario {
        mut scene,
        raw,
        getters,
        source_drops,
        context,
        capability,
    } = start_scenario(workers, 2, task_geometry, start_task_ports, |calls| {
        Arc::new(Factory(calls))
    });
    let clock_origin = scene.clock.now();
    let request_horizon = crate::RequestHorizon::DEFAULT;
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
    let held_task_delivery = match exercise_task(&scene, context, &mut task_resources) {
        Ok(original) => original,
        Err(original) => scene.fail("actual Task lifecycle", (task_resources, original)),
    };
    // This count follows the real retained delivery, never a synthetic switch.
    let held_requests = u64::from(held_task_delivery.is_some());
    if let Some((delivery, request)) = &held_task_delivery {
        assert_eq!(delivery.request().operation, request.operation);
        assert_eq!(delivery.request().deadline, request.deadline);
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
            if !wait_until(|| scene.ledger.used(Dimension::Requests.id()) == held_requests) {
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
                    scene.ledger.used(Dimension::Requests.id()) == held_requests
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
            if !wait_until(|| scene.ledger.used(Dimension::Requests.id()) == held_requests) {
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
    if !wait_until(|| {
        scene
            .node
            .try_execution_quiescence_snapshot()
            .is_some_and(|snapshot| {
                snapshot.active_tasks == 0
                    && snapshot.preparing_tasks == 0
                    && snapshot.preparing_bytes == 0
                    && snapshot.requests == held_requests
            })
    }) {
        scene.fail(
            "actual execution custody did not retire before advancing the original clock",
            held_task_delivery,
        );
    }
    if let Some((delivery, request)) = held_task_delivery {
        let mut held_snapshot = None;
        if !wait_until(|| {
            scene
                .node
                .original_task_request_for_test(context, request.author)
                .is_some_and(|(actual, task, attached, scope_present)| {
                    actual == request
                        && Some((context, task)) == *scene.diagnostic_task.lock().unwrap()
                        && !attached
                        && !scope_present
                })
                && scene
                    .node
                    .try_execution_quiescence_snapshot()
                    .is_some_and(|snapshot| {
                        if snapshot.active_tasks == 0
                            && snapshot.preparing_tasks == 0
                            && snapshot.preparing_bytes == 0
                            && snapshot.requests == 1
                        {
                            held_snapshot = Some(snapshot);
                            true
                        } else {
                            false
                        }
                    })
        }) {
            scene.fail(
                "same held Task R outlives actual Task/Context execution custody",
                (delivery, request),
            );
        }
        assert_eq!(delivery.request().operation, request.operation);
        assert_eq!(delivery.request().deadline, request.deadline);
        assert_eq!(scene.ledger.used(Dimension::Requests.id()), 1);
        // Keep the actual successful nonblocking observation from this wait.
        // A later try-lock miss supplies no different lifecycle fact.
        let held_snapshot = held_snapshot.expect("the same retained R inventory is observable");
        assert!(
            held_snapshot.contexts_with_execution_custody >= 1,
            "this diagnostic inventory still counts the actual live R"
        );
        eprintln!(
            "same original GetFinalInfo delivery held after actual Scope removal: request={request:?}, inventory={held_snapshot:?}"
        );
        drop(delivery);
        if !wait_until(|| {
            scene.ledger.used(Dimension::Requests.id()) == 0
                && scene
                    .node
                    .try_execution_quiescence_snapshot()
                    .is_some_and(|snapshot| {
                        snapshot.contexts_with_execution_custody == 0 && snapshot.requests == 0
                    })
                && scene
                    .node
                    .original_task_request_for_test(context, request.author)
                    .is_none()
        }) {
            scene.fail(
                "the same held GetFinalInfo R did not actually retire",
                request,
            );
        }
    }
    // This deterministic clock never advanced during execution retirement.
    // Production captured retired_at from this same clock after actual Scope
    // and parent-link removal, so its original horizon starts at clock_origin.
    assert_eq!(scene.clock.now(), clock_origin);
    let horizon_nanos = u64::try_from(request_horizon.total().as_nanos())
        .expect("the frozen original request horizon fits the clock");
    let expiry = clock_origin
        .0
        .checked_add(horizon_nanos)
        .and_then(|deadline| deadline.checked_add(1))
        .expect("the original request horizon has a representable due tick");
    scene.clock.0.store(expiry, Ordering::SeqCst);
    scene.runtime.notify_pollers();
    if !wait_until(|| {
        scene.node.route(context).is_none() && scene.ledger.used(Dimension::Requests.id()) == 0
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
    // This fixture exposes no Native listener. Match the production role's
    // explicit ingress-close confirmation before waiting for Root retirement.
    if let Err(original) = scene.node.confirm_ingress_closed() {
        scene.fail("Node actual ingress close", original);
    }
    if !wait_until(|| scene.runtime.is_empty() && scene.ledger.is_empty()) {
        scene.fail("Root actual metadata and Scope drain timeout", ());
    }
    finish_scene(scene, workers, close_kind, shutdown_task_ports);
}

fn finish_scene<T: Send + 'static>(
    scene: Scenario<T>,
    workers: usize,
    close_kind: &str,
    shutdown_task_ports: impl FnOnce(T) -> Result<(), Box<dyn std::any::Any + Send>>,
) {
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

// An empty Context still requires the actual execution-host close boundary.
// Reuse the existing compiled host and joined pools; no Task is submitted.
type ColdNotificationPool = super::node_task_flow::TaskResources;
fn start_cold_notifications(
    root: &mut ProcessControlOwner,
) -> Result<(TaskDriverPorts, ColdNotificationPool), Box<dyn std::any::Any + Send>> {
    super::node_task_flow::start_tasks(root)
}
fn shutdown_cold_notifications(
    original: ColdNotificationPool,
) -> Result<(), Box<dyn std::any::Any + Send>> {
    super::node_task_flow::shutdown(original)
}

#[test]
fn real_node_context_lifecycle_runs_on_one_and_two_common_pollers() {
    for workers in [1, 2] {
        run_node_lifecycle_with_task(
            workers,
            NodeClose::Release,
            ReservationResources::default(),
            start_cold_notifications,
            |_, _, _| Ok(None),
            shutdown_cold_notifications,
        );
    }
}
