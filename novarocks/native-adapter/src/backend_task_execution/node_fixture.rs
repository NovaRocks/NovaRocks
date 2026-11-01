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

//! Shared test/dev-control fixtures backed by the production Worker Node and original roles.
//!
//! The metadata fixture pauses the real preparation callback. Its weak view
//! neither accepts a conclusion nor creates replacement preparation authority.

use novarocks_execution::runtime::{
    execution_health::ExecutionHealth, fragment::ExecutionFailure,
    task_control::TaskExecutionControl,
};
use novarocks_execution_contract::{
    AbortCause, AbortQueryContext, AcquireQueryContextAdmissionTicket, AdmissionEpochCapability,
    AdmissionTicketId, CancelReason, CancelTask, CodecOwnedContent, ConfidentialContent,
    ContentFingerprint, CreateTask, CreationContent, CredentialEpoch, CredentialLeaseId,
    CredentialUpdate, EstablishQueryContext, ExchangeTopology, FrozenBytes, LeaseValidFor,
    OperationOutcome, QueryContextRef, TaskCreationInput, TaskDescriptor, TaskDomainUpdate,
    TaskFailureCategory, TaskIdentity, TaskOperationId,
};
use novarocks_types::{FrontendProcessId, NativeCompatibilityId, UniqueId};
use novarocks_worker::{
    HostRejection, LeaseBounds, ManualClock, NODE_CONTEXT_STEP_BUDGET, NODE_CONTROL_STEP_BUDGET,
    NodeAdmissionTicketConfig, NodeContextCoreConfig, NodeControlConfig, NodeGeometry,
    NodeGeometryInputs, NodeWorkers, PreparationControlLoan, PreparedTaskInstallation,
    QueryContextHostFactory, QueryContextHostInstance, ReleasedContextEvidence, RequestHorizon,
    RunnableTask, SharedFactsRequest, TaskExecutionHost, TaskIngressRetirement,
    TaskPreparationContext, TaskPreparationView, TaskStatusReporter, WorkerAdmissionEpochAuthority,
    WorkerContextReplyBody, WorkerControlReplyBody, WorkerCreateReply, WorkerNodeConfig,
    WorkerNodeRuntime, WorkerTaskControl,
    control_runtime_policy::{ControlRuntimePolicy, IngressRoleLimits},
};
use std::{
    collections::BTreeMap,
    future::Future,
    num::NonZeroUsize,
    pin::pin,
    rc::Rc,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

/// Poll the original delivery; this helper neither creates an ACK nor retries a
/// business request. Production physical pools supply its original wake.
pub fn await_original<F: Future>(future: F) -> F::Output {
    await_original_until(future, Instant::now() + Duration::from_secs(10))
}
fn await_original_until<F: Future>(future: F, deadline: Instant) -> F::Output {
    struct Waiter(std::thread::Thread);
    impl Wake for Waiter {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Waiter(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return result;
        }
        assert!(
            Instant::now() < deadline,
            "original Node delivery did not complete"
        );
        std::thread::park_timeout(Duration::from_millis(10));
    }
}
fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

struct ComponentIngressBacking {
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl novarocks_worker::WorkerIngressBacking for ComponentIngressBacking {}

pub struct NativeNodeFixture {
    owner: Option<WorkerNodeRuntime>,
    pub control: WorkerTaskControl,
    pub clock: Arc<ManualClock>,
    ingress: Arc<tokio::sync::Semaphore>,
}
impl NativeNodeFixture {
    pub fn start(
        backend: novarocks_types::BackendProcessId,
        factory: Arc<dyn QueryContextHostFactory>,
        execution: Arc<dyn TaskExecutionHost>,
        health: Arc<ExecutionHealth>,
        tasks: usize,
        preparing: usize,
    ) -> Self {
        // The fixture declares its actual owners: one Context, one loader and
        // one preparation body; cleanup and protocol polling remain independent.
        let policy = ControlRuntimePolicy::try_new_with_request_sources(
            IngressRoleLimits {
                ordinary_running: 1,
                ordinary_waiting: 1,
                control_running: 1,
                control_waiting: 1,
                max_batch_items: 1,
                root_read_holders: 2,
            },
            2,
            1,
        )
        .unwrap();
        let tickets = NodeAdmissionTicketConfig::new(
            Duration::from_secs(10),
            RequestHorizon::DEFAULT.total(),
        )
        .unwrap();
        let geometry = NodeGeometry::try_new(NodeGeometryInputs {
            tasks_per_context: nz(tasks),
            active_tasks: nz(tasks),
            preparing_tasks: nz(preparing),
            preparing_bytes: nz(64 << 20),

            policy,
            native_root_data_holders: nz(2),
        })
        .unwrap();
        let requests = geometry.request_positions().get();
        let clock = Arc::new(ManualClock::new());
        let owner = WorkerNodeRuntime::start(WorkerNodeConfig {
            observation: crate::worker_node_projection::observation_projection(),
            backend,
            compatibility: NativeCompatibilityId::new([0x71; 32]),
            initial_admission_epoch: AdmissionEpochCapability::try_from_bytes([1; 16]).unwrap(),
            clock: clock.clone(),
            context_factory: factory,
            execution,
            execution_ports: crate::task_execution_observation::backend_task_execution_ports(
                health,
                #[cfg(novarocks_dev_controls)]
                None,
            ),
            control: NodeControlConfig {
                max_pending_requests: requests,
                step_budget: NODE_CONTROL_STEP_BUDGET,
            },
            context: NodeContextCoreConfig {
                request_horizon: RequestHorizon::DEFAULT,
                max_tasks: tasks,
                max_creation_waiters: requests,
                max_pending_requests: requests,
                max_preparing: preparing,
                max_task_bytes: 64 << 20,
                step_budget: NODE_CONTEXT_STEP_BUDGET,
                lease_bounds: LeaseBounds::DEFAULT,
                metric_publish_interval: novarocks_worker::METRIC_PUBLISH_MIN_INTERVAL,
            },
            tickets,
            geometry,
            workers: NodeWorkers {
                pollers: nz(1),
                context_materialization: nz(1),
                context_cleanup: nz(1),
                establish_getters: nz(1),
                establish_cleanup: nz(1),
                loading: nz(1),
                preparation: nz(preparing),
                lifecycle: nz(1),
            },
            cadence: Duration::from_millis(1),
        })
        .expect("production Node fixture startup");
        Self {
            control: owner.task_control(),
            owner: Some(owner),
            clock,
            ingress: Arc::new(tokio::sync::Semaphore::new(requests)),
        }
    }
    /// A real component ingress position follows the original rich input.
    /// Listener admission is exercised separately by Native system scenarios.
    pub fn ingress_responsibility(&self) -> novarocks_worker::WorkerIngressResponsibility {
        let permit = self
            .ingress
            .clone()
            .try_acquire_owned()
            .expect("the fixture's declared original ingress positions");
        novarocks_worker::WorkerIngressResponsibility::from_admitted(Arc::new(
            ComponentIngressBacking { _permit: permit },
        ))
    }
    pub fn deadline(&self) -> novarocks_worker::WorkerMonotonicInstant {
        self.control.now().saturating_add(Duration::from_secs(10))
    }
    pub fn create(
        &self,
        request: CreateTask,
        input: TaskCreationInput,
    ) -> novarocks_worker::WorkerCreateDelivery {
        await_original(self.control.create(
            Arc::new(request),
            input,
            0,
            self.deadline(),
            self.ingress_responsibility(),
        ))
        .expect("the original Create delivery")
    }
    pub fn status(
        &self,
        context: QueryContextRef,
        task: TaskIdentity,
    ) -> novarocks_execution_contract::TaskStatus {
        self.wait_status(context, task, Duration::from_secs(10), |_| true)
    }
    pub fn wait_status(
        &self,
        context: QueryContextRef,
        task: TaskIdentity,
        budget: Duration,
        accepted: impl Fn(&novarocks_execution_contract::TaskStatus) -> bool,
    ) -> novarocks_execution_contract::TaskStatus {
        let until = Instant::now() + budget;
        let mut source = self
            .control
            .begin_status_subscription(
                context,
                TaskOperationId::new_v7(),
                self.deadline(),
                1,
                novarocks_worker::NodeCoveredSubscriptionRequest {
                    status: vec![novarocks_execution_contract::TaskStatusCursor::unobserved(
                        task,
                    )],
                    required: vec![task],
                    ..Default::default()
                },
            )
            .expect("the original covered subscription");
        loop {
            let frame = await_original_until(source.next(), until)
                .expect("the original selected status frame");
            use novarocks_worker::{
                NodeCoveredObservationFact as Fact, NodeCoveredObservationFrame as Frame,
            };
            let status = match &frame.selected().frame {
                Frame::CatchUp(Fact::Status(status))
                | Frame::Live {
                    fact: Fact::Status(status),
                    ..
                } => Some(status.clone()),
                _ => None,
            };
            frame.delivered().unwrap_or_else(|(_error, original)| {
                drop(original);
                panic!("the actual selected frame was not accepted")
            });
            if let Some(status) = status
                && accepted(&status)
            {
                return status;
            }
        }
    }
    pub fn establish(&self, context: QueryContextRef) {
        self.establish_with(
            context,
            ContextContents {
                catalog: Arc::new(Content),
                filters: Arc::new(Content),
                options: Arc::new(Content),
                credential: CredentialUpdate::new(
                    CredentialLeaseId::new(1),
                    CredentialEpoch::FIRST,
                    Arc::new(Secret),
                ),
            },
        );
    }
    pub fn establish_with(&self, context: QueryContextRef, contents: ContextContents) {
        let ticket =
            AdmissionTicketId::try_from_bytes(TaskOperationId::new_v7().to_bytes()).unwrap();
        let request = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context,
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
            NativeCompatibilityId::new([0x71; 32]),
            self.control.admission_epoch_capability(),
        );
        let admitted =
            await_original(self.control.acquire(request, ticket, self.deadline())).unwrap();
        assert!(
            matches!(admitted.body(), WorkerControlReplyBody::Admission(Ok(_))),
            "{:?}",
            admitted.body()
        );
        drop(admitted);
        let result = await_original(self.control.establish(
            EstablishQueryContext::new(
                TaskOperationId::new_v7(),
                context,
                ticket,
                contents.catalog,
                contents.filters,
                contents.options,
                contents.credential,
                LeaseValidFor::new(LeaseBounds::INITIAL_REQUEST).unwrap(),
            ),
            self.deadline(),
        ))
        .unwrap();
        assert!(
            matches!(result.body(), WorkerControlReplyBody::Context(ack)
            if ack.outcome == OperationOutcome::Accepted),
            "{:?}",
            result.body()
        );
    }
}
impl Drop for NativeNodeFixture {
    fn drop(&mut self) {
        let Some(owner) = self.owner.take() else {
            return;
        };
        owner.begin_drain().expect("close actual Node admission");
        // This in-process fixture never opened a listener or transport handle.
        owner
            .confirm_ingress_closed()
            .expect("no Native ingress remains");
        let deadline = Instant::now() + Duration::from_secs(10);
        while !owner.is_drained() {
            if Instant::now() >= deadline {
                // A timed-out physical owner is a failed process-level test.
                // It cannot be leaked to let subsequent cases report success.
                eprintln!(
                    "original Node fixture drain inventory: charges={:?} preparation={:?} root={:?} execution={:?}",
                    self.control.metrics_snapshot(),
                    self.control.try_preparation_snapshot(),
                    self.control.try_root_ownership_snapshot(),
                    self.control.try_execution_quiescence_snapshot(),
                );
                eprintln!("original Node fixture did not retire before its deadline");
                std::process::abort();
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        owner.try_shutdown().unwrap_or_else(|(error, original)| {
            // Keep the failed original alive until the process exits; no
            // destructor or synthetic completion can manufacture a true join.
            let _original = original;
            eprintln!("actual Node pools did not join: {error}");
            std::process::abort();
        });
    }
}

pub struct ContextContents {
    pub catalog: Arc<dyn CodecOwnedContent>,
    pub filters: Arc<dyn CodecOwnedContent>,
    pub options: Arc<dyn CodecOwnedContent>,
    pub credential: CredentialUpdate,
}

#[derive(Debug)]
struct Content;
impl CodecOwnedContent for Content {
    fn fingerprint(&self) -> ContentFingerprint {
        ContentFingerprint::from_bytes([1; 16])
    }
    fn encoded_len(&self) -> usize {
        1
    }
}
struct Secret;
impl ConfidentialContent for Secret {
    fn encoded_len(&self) -> usize {
        1
    }
    fn matches(&self, other: &dyn ConfidentialContent) -> bool {
        other.encoded_len() == 1
    }
}
#[derive(Debug)]
struct Assignment;
impl CreationContent for Assignment {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn encoded_len(&self) -> usize {
        0
    }
    fn into_stored(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}
pub fn factory_from_host(
    host: Arc<dyn novarocks_worker::QueryContextHost>,
) -> Arc<dyn QueryContextHostFactory> {
    struct Factory(Arc<dyn novarocks_worker::QueryContextHost>);
    struct Instance {
        context: QueryContextRef,
        original: Arc<dyn novarocks_worker::QueryContextHost>,
    }
    impl QueryContextHostFactory for Factory {
        fn create_instance(
            self: Arc<Self>,
            context: QueryContextRef,
        ) -> Result<Arc<dyn QueryContextHostInstance>, HostRejection> {
            Ok(Arc::new(Instance {
                context,
                original: self.0.clone(),
            }))
        }
    }
    impl QueryContextHostInstance for Instance {
        fn context(&self) -> QueryContextRef {
            self.context
        }
        fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
            assert_eq!(request.context(), self.context);
            self.original.materialize(request)
        }
        fn release(&self) -> ReleasedContextEvidence {
            self.original.release(self.context)
        }
        fn advance_shared_domain(
            &self,
            domain: &novarocks_execution_contract::QueryContextDomainUpdate,
        ) -> Result<(), HostRejection> {
            self.original.advance_shared_domain(self.context, domain)
        }
    }
    Arc::new(Factory(host))
}

struct MetadataFactory;
struct MetadataContext(QueryContextRef);
impl QueryContextHostFactory for MetadataFactory {
    fn create_instance(
        self: Arc<Self>,
        context: QueryContextRef,
    ) -> Result<Arc<dyn QueryContextHostInstance>, HostRejection> {
        Ok(Arc::new(MetadataContext(context)))
    }
}
impl QueryContextHostInstance for MetadataContext {
    fn context(&self) -> QueryContextRef {
        self.0
    }
    fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        assert_eq!(request.context(), self.0);
        Ok(())
    }
    fn release(&self) -> ReleasedContextEvidence {
        ReleasedContextEvidence::none()
    }
    fn advance_shared_domain(
        &self,
        _: &novarocks_execution_contract::QueryContextDomainUpdate,
    ) -> Result<(), HostRejection> {
        unreachable!("metadata fixture has no domain update")
    }
}
#[derive(Debug)]
struct MetadataTurnFinished;
impl std::fmt::Display for MetadataTurnFinished {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the original metadata preparation turn returned")
    }
}
impl std::error::Error for MetadataTurnFinished {}
impl novarocks_execution_contract::failure::FailureProjection for MetadataTurnFinished {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        FailureFact::new(
            FailureClass::Control,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(60045).unwrap()),
        )
    }
}
type PreparationGateState = (
    Option<TaskPreparationView>,
    Option<novarocks_worker::TaskDynamicFilterWriter>,
    bool,
    Option<std::sync::Weak<TaskExecutionControl>>,
);

#[derive(Default)]
struct PreparationGate {
    state: Mutex<PreparationGateState>,
    changed: Condvar,
}
struct MetadataExecution(BTreeMap<TaskIdentity, Arc<PreparationGate>>);
impl TaskExecutionHost for MetadataExecution {
    fn close_context_admission(&self, _: QueryContextRef) {}
    fn retire_context_execution(&self, _: QueryContextRef) {}
    fn forget_context_admission(&self, _: QueryContextRef) {}
    fn install_receiver(
        &self,
        descriptor: &TaskDescriptor,
        _: TaskCreationInput,
        mut preparation: TaskPreparationContext<'_>,
    ) -> Result<PreparedTaskInstallation, HostRejection> {
        let gate = self
            .0
            .get(&descriptor.identity())
            .expect("the admitted original Task gate");
        let control = preparation.runtime_control();
        let roles = preparation
            .take_roles()
            .expect("one original physical preparation");
        let mut state = gate.state.lock().unwrap();
        state.0 = Some(preparation.view());
        state.1 = Some(roles.dynamic_filters.writer());
        state.3 = Some(Arc::downgrade(&control));
        gate.changed.notify_all();
        while !state.2 {
            state = gate.changed.wait(state).unwrap();
        }
        drop(state);
        // All roles are still unbound. Drop the original permits/producers on
        // this physical body; their actual unused/exit paths remain observable.
        drop(roles);
        // No receiver or child was exposed; the real preparation callback now
        // returns its own refusal. Worker supervises capture and unused roles.
        Err(HostRejection::from_domain(
            TaskFailureCategory::Execution,
            MetadataTurnFinished,
        ))
    }
    fn remove_receiver(&self, _: &TaskDescriptor) {}
    fn retire_receiver_checked(
        &self,
        _: &TaskDescriptor,
        mode: TaskIngressRetirement,
    ) -> Result<(), HostRejection> {
        assert_eq!(mode, TaskIngressRetirement::Remove);
        Ok(())
    }
    fn install_inbound_capability(&self, _: &TaskDescriptor) -> Result<(), HostRejection> {
        unreachable!("metadata preparation never installs")
    }
    fn remove_inbound_capability(&self, _: &TaskDescriptor) {}
    fn submit_runnable(
        &self,
        _: &TaskDescriptor,
        _: TaskStatusReporter,
    ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
        unreachable!("metadata preparation returns a refusal")
    }
    fn apply_task_domain(
        &self,
        _: &TaskDescriptor,
        _: &TaskDomainUpdate,
        _: TaskOperationId,
    ) -> Result<Option<u64>, HostRejection> {
        unreachable!("no initial task domains")
    }
}

#[derive(Clone, Copy)]
pub enum PreparationStop {
    Cancel(CancelReason),
    Abort(AbortCause),
}
pub struct NativeNodePreparation {
    view: TaskPreparationView,
    gate: Arc<PreparationGate>,
    node: Rc<NativeNodeFixture>,
    task: TaskIdentity,
    context: QueryContextRef,
    writer: novarocks_worker::TaskDynamicFilterWriter,
}
impl NativeNodePreparation {
    pub fn new(task: TaskIdentity) -> Self {
        let context = QueryContextRef::new(
            task.query_execution_id(),
            FrontendProcessId::new_v7(),
            task.backend_process_id(),
        );
        Self::with_context(
            context,
            Arc::new(MetadataFactory),
            ContextContents {
                catalog: Arc::new(Content),
                filters: Arc::new(Content),
                options: Arc::new(Content),
                credential: CredentialUpdate::new(
                    CredentialLeaseId::new(1),
                    CredentialEpoch::FIRST,
                    Arc::new(Secret),
                ),
            },
            &[task],
        )
        .pop()
        .unwrap()
    }
    pub fn with_context(
        context: QueryContextRef,
        factory: Arc<dyn QueryContextHostFactory>,
        contents: ContextContents,
        tasks: &[TaskIdentity],
    ) -> Vec<Self> {
        assert!(!tasks.is_empty());
        let gates: BTreeMap<_, _> = tasks
            .iter()
            .copied()
            .map(|task| {
                task.verify_query_context(context).unwrap();
                (task, Arc::new(PreparationGate::default()))
            })
            .collect();
        assert_eq!(gates.len(), tasks.len());
        let node = Rc::new(NativeNodeFixture::start(
            context.backend_process_id(),
            factory,
            Arc::new(MetadataExecution(gates.clone())),
            Arc::new(ExecutionHealth::default()),
            tasks.len(),
            tasks.len(),
        ));
        node.establish_with(context, contents);
        let mut originals = Vec::new();
        for &task in tasks {
            let descriptor = TaskDescriptor::try_new(
                task,
                UniqueId::new(
                    i64::from(task.stage_id().get()),
                    i64::from(task.task_id().get()),
                ),
                nz(1),
                vec![],
                ExchangeTopology::default(),
            )
            .unwrap();
            let request = Arc::new(
                CreateTask::try_new(TaskOperationId::new_v7(), context, descriptor, vec![])
                    .unwrap(),
            );
            let delivery = await_original(node.control.create(
                request,
                TaskCreationInput::new(
                    FrozenBytes::freeze(bytes::Bytes::from_static(&[1])),
                    Box::new(Assignment),
                ),
                0,
                node.deadline(),
                node.ingress_responsibility(),
            ))
            .unwrap();
            assert!(
                matches!(
                    delivery.reply(),
                    WorkerCreateReply::Context(WorkerContextReplyBody::Create {
                        outcome: OperationOutcome::Accepted,
                        ..
                    })
                ),
                "original Create must be Accepted"
            );
            drop(delivery);
            let gate = gates[&task].clone();
            let until = Instant::now() + Duration::from_secs(10);
            let mut state = gate.state.lock().unwrap();
            while state.0.is_none() {
                let (next, timeout) = gate
                    .changed
                    .wait_timeout(state, until.saturating_duration_since(Instant::now()))
                    .unwrap();
                state = next;
                assert!(
                    !timeout.timed_out(),
                    "original preparation callback did not start"
                );
            }
            let view = state.0.take().unwrap();
            let writer = state.1.take().unwrap();
            drop(state);
            originals.push(Self {
                view,
                gate,
                node: node.clone(),
                task,
                context,
                writer,
            });
        }
        originals
    }
    pub fn status(&self) -> novarocks_execution_contract::TaskStatus {
        self.node.status(self.context, self.task)
    }
    pub fn wait_status(
        &self,
        accepted: impl Fn(&novarocks_execution_contract::TaskStatus) -> bool,
    ) -> novarocks_execution_contract::TaskStatus {
        self.node
            .wait_status(self.context, self.task, Duration::from_secs(10), accepted)
    }
    pub fn dynamic_filters(&self) -> novarocks_worker::WorkerTaskReadDelivery {
        await_original(self.node.control.get_dynamic_filters(
            self.context,
            novarocks_execution_contract::FetchTaskDynamicFilters::new(
                TaskOperationId::new_v7(),
                self.task,
                None,
            ),
            self.node.deadline(),
        ))
        .expect("the original Node dynamic-filter read")
    }
    pub fn dynamic_filter_writer(&self) -> novarocks_worker::TaskDynamicFilterWriter {
        self.writer.clone()
    }
    pub fn release_context(originals: &[Self]) {
        let first = originals.first().expect("the original Context members");
        let quiesce = await_original(first.node.control.quiesce(
            novarocks_execution_contract::QuiesceQueryContext::new(
                TaskOperationId::new_v7(),
                first.context,
            ),
            first.node.deadline(),
        ))
        .unwrap();
        assert!(matches!(
            quiesce.body(),
            WorkerContextReplyBody::Quiesce {
                outcome: OperationOutcome::Accepted | OperationOutcome::Idempotent,
                ..
            }
        ));
        drop(quiesce);
        for original in originals {
            assert_eq!(original.context, first.context);
            assert!(Rc::ptr_eq(&original.node, &first.node));
            let mut state = original.gate.state.lock().unwrap();
            state.2 = true;
            original.gate.changed.notify_all();
        }
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let release = await_original(first.node.control.release(
                novarocks_execution_contract::ReleaseQueryContext::new(
                    TaskOperationId::new_v7(),
                    first.context,
                ),
                first.node.deadline(),
            ))
            .unwrap();
            match release.body() {
                WorkerContextReplyBody::Release {
                    release:
                        novarocks_execution_contract::ReleaseOutcome::Released
                        | novarocks_execution_contract::ReleaseOutcome::AlreadyTerminal,
                    ..
                } => return,
                WorkerContextReplyBody::Release {
                    release: novarocks_execution_contract::ReleaseOutcome::NotReady,
                    ..
                } => {}
                other => panic!("unexpected original Release: {other:?}"),
            }
            drop(release);
            assert!(
                Instant::now() < until,
                "original Task/Context did not release"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    pub fn loan(&self) -> PreparationControlLoan<'_> {
        self.view.borrow_control()
    }
    pub fn stop_view(&self) -> TaskPreparationView {
        self.view.clone()
    }
    pub fn execution_control(&self) -> Arc<TaskExecutionControl> {
        self.gate
            .state
            .lock()
            .unwrap()
            .3
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .expect("the original physical preparation still lends its control")
    }

    pub fn report_failure(&self, source: ExecutionFailure) {
        self.execution_control()
            .in_phase(novarocks_execution_contract::TaskFailurePhase::Preparation)
            .report_failure(source);
        self.wait_stopped();
    }
    fn wait_stopped(&self) {
        let until = Instant::now() + Duration::from_secs(10);
        while self.loan().checkpoint().is_ok() {
            assert!(
                Instant::now() < until,
                "original Core did not accept the stop"
            );
            std::thread::yield_now();
        }
    }
    pub fn stop(&self, stop: PreparationStop) -> bool {
        match stop {
            PreparationStop::Cancel(reason) => {
                drop(
                    await_original(self.node.control.cancel(
                        CancelTask::new(TaskOperationId::new_v7(), self.task, reason),
                        self.node.deadline(),
                    ))
                    .unwrap(),
                );
            }
            PreparationStop::Abort(cause) => {
                drop(
                    await_original(self.node.control.abort(
                        AbortQueryContext::new(TaskOperationId::new_v7(), self.context, cause),
                        self.node.deadline(),
                    ))
                    .unwrap(),
                );
            }
        }
        self.wait_stopped();
        true
    }
}
impl Drop for NativeNodePreparation {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock().unwrap();
        state.2 = true;
        self.gate.changed.notify_all();
    }
}

/// An assertion borrows the original physical result before its one return to
/// Worker. No HostRejection, RootArc, permit or role escapes this observation.
type InstallAssertion = Box<dyn FnOnce(&Result<PreparedTaskInstallation, HostRejection>) + Send>;
struct InspectInstall {
    original: Arc<dyn TaskExecutionHost>,
    assertion: Mutex<Option<InstallAssertion>>,
    completed: AtomicBool,
    foreign_descriptor: Option<TaskDescriptor>,
}
impl TaskExecutionHost for InspectInstall {
    fn close_context_admission(&self, context: QueryContextRef) {
        self.original.close_context_admission(context)
    }
    fn abort_context_admission(&self, context: QueryContextRef) {
        self.original.abort_context_admission(context)
    }
    fn retire_context_execution(&self, context: QueryContextRef) {
        self.original.retire_context_execution(context)
    }
    fn forget_context_admission(&self, context: QueryContextRef) {
        self.original.forget_context_admission(context)
    }
    fn retains_normal_close(&self, context: QueryContextRef) -> bool {
        self.original.retains_normal_close(context)
    }
    fn install_receiver(
        &self,
        descriptor: &TaskDescriptor,
        input: TaskCreationInput,
        preparation: TaskPreparationContext<'_>,
    ) -> Result<PreparedTaskInstallation, HostRejection> {
        let descriptor = self.foreign_descriptor.as_ref().unwrap_or(descriptor);
        let result = self
            .original
            .install_receiver(descriptor, input, preparation);
        let assertion = self
            .assertion
            .lock()
            .unwrap()
            .take()
            .expect("one original install assertion");
        assertion(&result);
        self.completed.store(true, Ordering::Release);
        result
    }
    fn remove_receiver(&self, descriptor: &TaskDescriptor) {
        self.original.remove_receiver(descriptor)
    }
    fn retire_receiver_checked(
        &self,
        descriptor: &TaskDescriptor,
        mode: TaskIngressRetirement,
    ) -> Result<(), HostRejection> {
        self.original.retire_receiver_checked(descriptor, mode)
    }
    fn install_inbound_capability(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        self.original.install_inbound_capability(descriptor)
    }
    fn remove_inbound_capability(&self, descriptor: &TaskDescriptor) {
        self.original.remove_inbound_capability(descriptor)
    }
    fn submit_runnable(
        &self,
        _: &TaskDescriptor,
        _: TaskStatusReporter,
    ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
        // This fixture inspects installation only. Its explicit refusal lets
        // the original preparation owner drain the real dormant kernel and
        // receiver without racing provider activation after the assertion.
        Err(HostRejection::from_domain(
            TaskFailureCategory::Execution,
            MetadataTurnFinished,
        ))
    }

    fn apply_task_domain(
        &self,
        descriptor: &TaskDescriptor,
        domain: &TaskDomainUpdate,
        operation: TaskOperationId,
    ) -> Result<Option<u64>, HostRejection> {
        self.original
            .apply_task_domain(descriptor, domain, operation)
    }
}

pub fn inspect_install(
    original: Arc<dyn TaskExecutionHost>,
    factory: Arc<dyn QueryContextHostFactory>,
    health: Arc<ExecutionHealth>,
    descriptor: TaskDescriptor,
    input: TaskCreationInput,
    foreign_descriptor: Option<TaskDescriptor>,
    assertion: impl FnOnce(&Result<PreparedTaskInstallation, HostRejection>) + Send + 'static,
) {
    let task = descriptor.identity();
    let execution = Arc::new(InspectInstall {
        original,
        assertion: Mutex::new(Some(Box::new(assertion))),
        completed: AtomicBool::new(false),
        foreign_descriptor,
    });
    let node = NativeNodeFixture::start(
        task.backend_process_id(),
        factory,
        execution.clone(),
        health,
        1,
        1,
    );
    let context = QueryContextRef::new(
        task.query_execution_id(),
        FrontendProcessId::new_v7(),
        task.backend_process_id(),
    );
    node.establish(context);
    let result = node.create(
        CreateTask::try_new(TaskOperationId::new_v7(), context, descriptor, vec![]).unwrap(),
        input,
    );
    assert!(
        matches!(
            result.reply(),
            WorkerCreateReply::Context(WorkerContextReplyBody::Create {
                outcome: OperationOutcome::Accepted,
                ..
            })
        ),
        "the actual original creation must reach preparation"
    );
    drop(result);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !execution.completed.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "the original Native install assertion did not return"
        );
        std::thread::yield_now();
    }
    // Actual Node drain then waits on the same preparation/kernel/Root/source
    // inventories. This is not a synthetic rollback or a terminal publication.
    drop(node);
}
