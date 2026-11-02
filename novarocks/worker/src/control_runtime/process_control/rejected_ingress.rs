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

//! Original Native-backed disposal before any Worker RPC is admitted. Rich
//! Drop runs only in the existing cleanup pool; this lane creates no RPC,
//! input, Context, Source, or request charge. Native backing stays until the
//! same role, independent report consumer and actual header have retired.

use super::{LIFETIME, ProcessControlOwner};
use crate::control_runtime::{
    actors::WorkerActor,
    control_request::ControlRequestReady,
    establish_executors::{EstablishExecutorHandles, EstablishSubmitError},
    host_executor::HostSubmitError,
    loading::TaskCreatePayload,
    node_api::WorkerIngressResponsibility,
    preparation::{HostFairnessGroup, QueuedPreparation},
    task::TaskHostBinding,
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{ActorRegistration, ActorRuntime, DriverError},
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    lease::{WorkLease, WorkRetirementObserver},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::{ConsumerKind, Consumption},
    fair_queue::FairQueueError,
    identity::{ConsumerId, ScopeId, WorkId},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{
    AdvanceQueryContextDomain, QueryContextRef, TaskOperationId, UpdateTask,
};
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    mem::ManuallyDrop,
    ops::Bound::{Excluded, Included, Unbounded},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

pub(crate) type PanicPayload = ManuallyDrop<Box<dyn Any + Send>>;
fn observer() -> ConsumerId {
    ConsumerId::new(1).expect("original rejected ingress observer")
}

pub(crate) enum RejectedIngressPayload {
    Create {
        payload: TaskCreatePayload,
        binding: Option<TaskHostBinding>,
    },
    TaskDomain(UpdateTask),
    ContextDomain(AdvanceQueryContextDomain),
}
impl RejectedIngressPayload {
    fn metadata(&self) -> (TaskOperationId, &'static str) {
        match self {
            Self::Create { payload, .. } => (payload.request.envelope().operation_id(), "create"),
            Self::TaskDomain(request) => (request.envelope().operation_id(), "task_domain"),
            Self::ContextDomain(request) => (request.envelope().operation_id(), "context_domain"),
        }
    }
    /// All destructor boundaries execute on the physical host, outside every
    /// Root/runtime/queue lock. Another original field survives a caught panic.
    pub(crate) fn dispose(self) -> Vec<PanicPayload> {
        fn dispose_one<T>(original: T, panics: &mut Vec<PanicPayload>) {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(original))) {
                panics.push(ManuallyDrop::new(payload));
            }
        }
        let mut panics = Vec::new();
        match self {
            Self::Create { payload, binding } => {
                let TaskCreatePayload { request, input, .. } = payload;
                // Neither remaining field unwinds through another destructor.
                let request = ManuallyDrop::new(request);
                let input = ManuallyDrop::new(input);
                let binding = ManuallyDrop::new(binding);
                dispose_one(ManuallyDrop::into_inner(request), &mut panics);
                dispose_one(ManuallyDrop::into_inner(input), &mut panics);
                dispose_one(ManuallyDrop::into_inner(binding), &mut panics);
            }
            Self::TaskDomain(request) => {
                let (_, _, domains) = request.into_parts();
                for domain in domains {
                    dispose_one(domain, &mut panics);
                }
            }
            Self::ContextDomain(request) => {
                let (_, _, domain) = request.into_parts();
                dispose_one(domain, &mut panics);
            }
        }
        panics
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RejectedIngressAdmissionError {
    WrongOwner,
    Driver(DriverError),
    Runtime(RuntimeError),
}
impl std::fmt::Display for RejectedIngressAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongOwner => {
                f.write_str("Rejected ingress does not belong to the original Root owner")
            }
            Self::Driver(error) => write!(f, "Rejected ingress runtime binding failed: {error}"),
            Self::Runtime(error) => write!(f, "Rejected ingress cleanup admission failed: {error}"),
        }
    }
}
impl std::error::Error for RejectedIngressAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::WrongOwner => None,
            Self::Driver(error) => Some(error),
            Self::Runtime(error) => Some(error),
        }
    }
}

#[must_use = "Admission refusal returns both complete original owners"]
pub(crate) struct RejectedIngressRefusal {
    pub error: RejectedIngressAdmissionError,
    pub _original_payload: RejectedIngressPayload,
    pub _original_ingress: WorkerIngressResponsibility,
}
impl std::fmt::Debug for RejectedIngressRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RejectedIngressRefusal")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum RejectedIngressFailure {
    Start(ExitRoleError),
    Exit(ExitRoleError),
    DropPanicked,
    JobLost,
}
impl std::fmt::Display for RejectedIngressFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Start(error) => write!(f, "Rejected ingress cleanup could not start: {error}"),
            Self::Exit(error) => write!(f, "Rejected ingress cleanup exit could not be recorded: {error}"),
            Self::DropPanicked => f.write_str("Rejected ingress payload destruction panicked; original custody remains unresolved"),
            Self::JobLost => f.write_str("Rejected ingress cleanup job was lost before a physical exit was observed"),
        }
    }
}
impl std::error::Error for RejectedIngressFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Start(error) | Self::Exit(error) => Some(error),
            Self::DropPanicked | Self::JobLost => None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum RejectedIngressAdvanceError {
    WrongOwner,
    Submit {
        work: WorkId,
        error: EstablishSubmitError,
    },
    Unresolved {
        work: WorkId,
        error: RejectedIngressFailure,
    },
    Runtime(RuntimeError),
    Exit(ExitRoleError),
}
impl std::fmt::Display for RejectedIngressAdvanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongOwner => {
                f.write_str("Rejected ingress progress does not belong to the original Root owner")
            }
            Self::Submit { work, error } => write!(
                f,
                "Rejected ingress work {work:?} submission failed: {error:?}"
            ),
            Self::Unresolved { work, error } => write!(
                f,
                "Rejected ingress work {work:?} remains unresolved: {error}"
            ),
            Self::Runtime(error) => write!(f, "Rejected ingress retirement failed: {error}"),
            Self::Exit(error) => write!(
                f,
                "Rejected ingress physical exit observation failed: {error}"
            ),
        }
    }
}
impl std::error::Error for RejectedIngressAdvanceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Unresolved { error, .. } => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Exit(error) => Some(error),
            Self::WrongOwner | Self::Submit { .. } => None,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct RejectedIngressProgress {
    pub visited: usize,
    pub submitted: usize,
    pub retired: usize,
    pub runnable: bool,
}

struct Ready {
    runtime: ActorRuntime<WorkerActor>,
    root: ActorRegistration,
    work: Mutex<BTreeSet<WorkId>>,
}
impl Ready {
    fn mark(&self, work: WorkId) {
        self.work
            .lock()
            .expect("original rejected ingress readiness")
            .insert(work);
        // No foreign waker runs under the readiness mutex.
        if self.runtime.notify_host_actor(&self.root).is_err() {
            std::process::abort();
        }
    }
    fn pop(&self) -> Option<WorkId> {
        self.work
            .lock()
            .expect("original rejected ingress readiness")
            .pop_first()
    }
    fn is_empty(&self) -> bool {
        self.work
            .lock()
            .expect("original rejected ingress readiness")
            .is_empty()
    }
}
struct State {
    payload: Option<ManuallyDrop<RejectedIngressPayload>>,
    authority: Option<ExitAuthority>,
    panics: Vec<PanicPayload>,
    failure: Option<RejectedIngressFailure>,
}
struct Custody {
    key: (ScopeId, WorkId),
    context: QueryContextRef,
    operation: TaskOperationId,
    kind: &'static str,
    role: ExitRole,
    report: ReportSlot,
    header: WorkRetirementObserver,
    namespace: ControlRequestReady,
    ready: Arc<Ready>,
    state: Mutex<State>,
    // This is transport admission bookkeeping, not the rich Work capture.
    // It cannot return before the same report/header genuinely retire.
    _ingress: WorkerIngressResponsibility,
    retained: Mutex<Option<Arc<Custody>>>,
}
#[must_use = "Run the original rich disposal once on its actual cleanup host"]
pub(crate) struct RejectedIngressJob {
    owner: Arc<Custody>,
    active: bool,
}
impl RejectedIngressJob {
    pub(crate) fn belongs_to(&self, ready: &ControlRequestReady) -> bool {
        self.owner.namespace.same_owner(ready)
    }
    pub(crate) fn run(mut self) {
        let (mut authority, payload) = {
            let mut state = self
                .owner
                .state
                .lock()
                .expect("original rejected ingress capture");
            (
                state
                    .authority
                    .take()
                    .expect("one original cleanup authority"),
                state
                    .payload
                    .take()
                    .expect("one original rejected rich payload"),
            )
        };
        if let Err(error) = authority.start() {
            let mut state = self
                .owner
                .state
                .lock()
                .expect("original rejected ingress start");
            state.payload = Some(payload);
            state.failure = Some(RejectedIngressFailure::Start(error));
            drop(state);
            drop(authority); // Records Unresolved; never retries the rich object.
        } else {
            let panics = ManuallyDrop::into_inner(payload).dispose();
            if panics.is_empty() {
                if let Err(error) = authority.exited() {
                    self.owner
                        .state
                        .lock()
                        .expect("original rejected ingress exit")
                        .failure = Some(RejectedIngressFailure::Exit(error));
                } else {
                    tracing::debug!(context = ?self.owner.context,
                        work = ?novarocks_control_runtime_core::identity::ScopedWorkRef {
                            scope: self.owner.key.0, work: self.owner.key.1 },
                        operation = %self.owner.operation, kind = self.owner.kind,
                        "Worker original rejected ingress disposed");
                }
            } else {
                let mut state = self
                    .owner
                    .state
                    .lock()
                    .expect("original rejected ingress panic");
                state.panics = panics;
                state.failure = Some(RejectedIngressFailure::DropPanicked);
                drop(state);
                drop(authority); // Unknown physical exit, not a filled report.
            }
        }
        self.active = false;
        self.owner.ready.mark(self.owner.key.1);
    }
}
impl QueuedPreparation for RejectedIngressJob {
    fn queue_key(&self) -> (ScopeId, WorkId) {
        self.owner.key
    }
    fn fairness_group(&self, _: ScopeId) -> HostFairnessGroup {
        HostFairnessGroup::Context(self.owner.context)
    }
}
impl Drop for RejectedIngressJob {
    fn drop(&mut self) {
        if self.active {
            let authority = {
                let mut state = self
                    .owner
                    .state
                    .lock()
                    .expect("original rejected ingress lost job");
                state.failure = Some(RejectedIngressFailure::JobLost);
                state.authority.take()
            };
            drop(authority);
            self.owner.ready.mark(self.owner.key.1);
        }
    }
}
struct Original {
    owner: Arc<Custody>,
    pending: Option<RejectedIngressJob>,
    waiting_revision: Option<u64>,
}
struct RetryScan {
    revision: u64,
    after: Option<WorkId>,
    through: WorkId,
}
/// Each row is the actual original owner, not a second report/claim table.
/// Every row retains the real Native backing until its genuine final exit.
pub(super) struct RejectedIngressLane {
    runtime: ActorRuntime<WorkerActor>,
    registration: ActorRegistration,
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    namespace: ControlRequestReady,
    ready: Arc<Ready>,
    originals: BTreeMap<WorkId, Original>,
    scanned_revision: u64,
    retry_scan: Option<RetryScan>,
}
impl RejectedIngressLane {
    pub(super) fn new(
        runtime: ActorRuntime<WorkerActor>,
        registration: ActorRegistration,
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        namespace: ControlRequestReady,
    ) -> Self {
        let scanned_revision = namespace.source_capacity_revision();
        let ready = Arc::new(Ready {
            runtime: runtime.clone(),
            root: registration.clone(),
            work: Mutex::new(BTreeSet::new()),
        });
        Self {
            runtime,
            registration,
            ledger,
            scope,
            namespace,
            ready,
            originals: BTreeMap::new(),
            scanned_revision,
            retry_scan: None,
        }
    }
    fn adopt(
        &mut self,
        parent: &WorkLease,
        context: QueryContextRef,
        payload: RejectedIngressPayload,
        ingress: WorkerIngressResponsibility,
    ) -> Result<(), RejectedIngressRefusal> {
        let refusal = |error, payload, ingress| RejectedIngressRefusal {
            error,
            _original_payload: payload,
            _original_ingress: ingress,
        };
        if !self.scope.same_scope(parent.scope()) {
            return Err(refusal(
                RejectedIngressAdmissionError::WrongOwner,
                payload,
                ingress,
            ));
        }
        if let Err(error) = self.runtime.validate_registration(&self.registration) {
            return Err(refusal(
                RejectedIngressAdmissionError::Driver(error),
                payload,
                ingress,
            ));
        }
        let generation = match self.scope.gates() {
            Ok(gates) => gates.generation,
            Err(error) => {
                return Err(refusal(
                    RejectedIngressAdmissionError::Runtime(error),
                    payload,
                    ingress,
                ));
            }
        };
        let resources = ReservationResources::new(
            ResourceVector::default(),
            OwnedSlotNeed {
                work_headers: 1,
                completions: 1,
                captures: 1,
                ..OwnedSlotNeed::default()
            },
        );
        let mut guard = match self.scope.reserve_next(
            generation,
            resources,
            WorkKind::Cleanup,
            WorkAdmission::Derived(parent.id()),
        ) {
            Ok(guard) => guard,
            Err(error) => {
                return Err(refusal(
                    RejectedIngressAdmissionError::Runtime(error),
                    payload,
                    ingress,
                ));
            }
        };
        let report = match ReportSlot::reserve_next(
            &mut guard,
            generation,
            1,
            [(observer(), ConsumerKind::Observer)],
        ) {
            Ok(report) => report,
            Err(error) => {
                return Err(refusal(
                    RejectedIngressAdmissionError::Runtime(error),
                    payload,
                    ingress,
                ));
            }
        };
        let (work, capacity) = guard.bind(WorkKind::Cleanup).unwrap_or_else(|_original| {
            // A fresh same-kind binding failure is a shared invariant. Abort
            // without unwinding either original rich owner or its metadata.
            std::process::abort();
        });
        let key = (work.scope().id(), work.id());
        let header = work.retirement_observer();
        let admitted = crate::control_runtime::admitted_role::adopt_reported_work(
            work,
            capacity,
            report.clone(),
        );
        let (role, authority) = ExitRole::from_admitted(admitted);
        let (operation, kind) = payload.metadata();
        let owner = Arc::new(Custody {
            key,
            context,
            operation,
            kind,
            role,
            report,
            header,
            namespace: self.namespace.clone(),
            ready: self.ready.clone(),
            state: Mutex::new(State {
                payload: Some(ManuallyDrop::new(payload)),
                authority: Some(authority),
                panics: Vec::new(),
                failure: None,
            }),
            _ingress: ingress,
            retained: Mutex::new(None),
        });
        *owner
            .retained
            .lock()
            .expect("original rejected ingress retention") = Some(owner.clone());
        let job = RejectedIngressJob {
            owner: owner.clone(),
            active: true,
        };
        assert!(
            self.originals
                .insert(
                    key.1,
                    Original {
                        owner,
                        pending: Some(job),
                        waiting_revision: None
                    }
                )
                .is_none()
        );
        self.ready.mark(key.1);
        Ok(())
    }
    pub(super) fn is_empty(&self) -> bool {
        self.originals.is_empty()
    }
    /// Scan at most budget original metadata rows per source revision. A new
    /// revision is coalesced behind the current finite round, never restarts
    /// its cursor and starves the later pending originals.
    fn retry_capacity(&mut self, budget: usize) -> usize {
        let revision = self.namespace.source_capacity_revision();
        if self.retry_scan.is_none() && revision != self.scanned_revision {
            if let Some((&through, _)) = self.originals.last_key_value() {
                self.retry_scan = Some(RetryScan {
                    revision,
                    after: None,
                    through,
                });
            } else {
                self.scanned_revision = revision;
            }
        }
        let mut visited = 0;
        for _ in 0..budget {
            let Some(scan) = self.retry_scan.as_ref() else {
                break;
            };
            let lower = scan.after.map_or(Unbounded, Excluded);
            let next = self
                .originals
                .range((lower, Included(scan.through)))
                .next()
                .map(|(&key, _)| key);
            let Some(key) = next else {
                self.scanned_revision = scan.revision;
                self.retry_scan = None;
                break;
            };
            self.retry_scan
                .as_mut()
                .expect("same finite revision round")
                .after = Some(key);
            visited += 1;
            let original = &self.originals[&key];
            if original.pending.is_some()
                && original
                    .waiting_revision
                    .is_some_and(|before| before != revision)
            {
                self.ready.mark(key);
            }
        }
        visited
    }
    pub(super) fn advance(
        &mut self,
        handles: &EstablishExecutorHandles,
        budget: usize,
    ) -> Result<RejectedIngressProgress, RejectedIngressAdvanceError> {
        if !handles.belongs_to(&self.namespace) {
            return Err(RejectedIngressAdvanceError::WrongOwner);
        }
        let mut progress = RejectedIngressProgress {
            visited: self.retry_capacity(budget),
            ..Default::default()
        };
        for _ in 0..budget {
            let Some(key) = self.ready.pop() else {
                break;
            };
            let original = self
                .originals
                .get_mut(&key)
                .expect("ready original rejected ingress");
            progress.visited += 1;
            if let Some(error) = original
                .owner
                .state
                .lock()
                .expect("original rejected ingress result")
                .failure
            {
                return Err(RejectedIngressAdvanceError::Unresolved { work: key, error });
            }
            if let Some(job) = original.pending.take() {
                let before = self.namespace.source_capacity_revision();
                if original.waiting_revision == Some(before) {
                    original.pending = Some(job);
                    continue;
                }
                match handles.try_submit_rejected_ingress(job) {
                    Ok(()) => {
                        original.waiting_revision = None;
                        progress.submitted += 1;
                        // Only the physical job's final notice may schedule
                        // observation. Do not race its not-yet-posted notice.
                        continue;
                    }
                    Err((error, job)) => {
                        original.pending = Some(job);
                        if matches!(
                            error,
                            EstablishSubmitError::Host(HostSubmitError::Queue(
                                FairQueueError::Full | FairQueueError::ContextFull
                            ))
                        ) {
                            original.waiting_revision = Some(before);
                            if self.namespace.source_capacity_revision() != before {
                                self.ready.mark(key);
                            }
                            continue;
                        }
                        return Err(RejectedIngressAdvanceError::Submit { work: key, error });
                    }
                }
            }
            match original.owner.role.phase() {
                ExitRolePhase::Admitted | ExitRolePhase::Started => continue,
                ExitRolePhase::Unresolved => {
                    return Err(RejectedIngressAdvanceError::Unresolved {
                        work: key,
                        error: RejectedIngressFailure::JobLost,
                    });
                }
                ExitRolePhase::Exited => {
                    if original
                        .owner
                        .report
                        .consumption(observer())
                        .map_err(RejectedIngressAdvanceError::Runtime)?
                        == Consumption::Pending
                    {
                        original
                            .owner
                            .report
                            .consume(observer())
                            .map_err(RejectedIngressAdvanceError::Runtime)?;
                    }
                    if !original
                        .owner
                        .role
                        .try_retire()
                        .map_err(RejectedIngressAdvanceError::Exit)?
                    {
                        continue;
                    }
                }
                ExitRolePhase::Retired => {}
            }
            self.ledger
                .reap_scope_ready(self.scope.id(), 1)
                .map_err(RejectedIngressAdvanceError::Runtime)?;
            if !original.owner.header.is_retired() {
                self.ready.mark(key); // Finite ready-header work, not live-I/O polling.
                continue;
            }
            let original = self
                .originals
                .remove(&key)
                .expect("same fully retired original");
            tracing::debug!(context = ?original.owner.context,
                work = ?novarocks_control_runtime_core::identity::ScopedWorkRef {
                    scope: original.owner.key.0, work: original.owner.key.1 },
                operation = %original.owner.operation, kind = original.owner.kind,
                "Worker original rejected ingress retired");
            let retained = original
                .owner
                .retained
                .lock()
                .expect("original rejected ingress retention")
                .take();
            drop(retained);
            drop(original); // Only the documented pure Native admission marker remains.
            progress.retired += 1;
        }
        progress.runnable = !self.ready.is_empty()
            || self.retry_scan.is_some()
            || self.namespace.source_capacity_revision() != self.scanned_revision;
        Ok(progress)
    }
}
impl ProcessControlOwner {
    /// Node releases its Context guard before this synchronous whole handoff.
    pub(crate) fn adopt_rejected_ingress(
        &mut self,
        context: QueryContextRef,
        payload: RejectedIngressPayload,
        ingress: WorkerIngressResponsibility,
    ) -> Result<(), RejectedIngressRefusal> {
        let Some(lane) = self.resources.rejected_ingress.as_mut() else {
            return Err(RejectedIngressRefusal {
                error: RejectedIngressAdmissionError::WrongOwner,
                _original_payload: payload,
                _original_ingress: ingress,
            });
        };
        lane.adopt(
            &self.resources.roles[LIFETIME].work,
            context,
            payload,
            ingress,
        )
    }
}
