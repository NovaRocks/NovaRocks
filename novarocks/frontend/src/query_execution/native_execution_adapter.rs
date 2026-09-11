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

//! Frontend implementation of the process-wide Query Application Native-open
//! boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, LeaseValidFor, OperationOutcome, QueryContextRef,
    TaskOperationId,
};
use novarocks_proto_codec::lifecycle::QueryOptions as ProtocolQueryOptions;
use novarocks_query_application::api::{
    ActivatedNativeAttempt, ActiveNativeAttemptOwner, CancellationView, DormantNativeAttemptOwner,
    LogicalExecutionNativePort, LogicalNativeOpenFuture, LogicalNativeOpenRequest,
    NativeActiveAttemptConvergenceFuture, NativeAttemptActivationFailure,
    NativeAttemptActivationFuture, NativeAttemptConvergenceFuture, NativeAttemptPreparationError,
    NativeAttemptPreparationFailure, NativeAttemptPreparationFuture, NativeAttemptPreparationPort,
    NativeAttemptPreparationRequest, NativeAttemptRunFuture, QueryExecutionClient,
    QueryExecutionError, QueryExecutionErrorKind, QueryExecutionFuture, QueryExecutionRequest,
};
use novarocks_query_application::coordination::{
    AbortQueryContextEffectAdmission, AbortQueryContextEffectPort, AbortQueryContextIssueIdentity,
    AcceptedRootStatusSource, ActiveAttemptIsolationOwner, AttemptFailureClass,
    AttemptIsolationActivationFailure, AttemptIsolationReservation, AttemptSchedule,
    CoordinationBudgets, NativeAttemptDrive, QualifiedReplacementReservation,
    QualifiedWorkerAdmission, RecoveryMode, ReplacementQualificationEffectAdmission,
    ReplacementQualificationEffectPort, ReplacementQualificationEffectReservation,
    ReplacementQualificationEffectSubmission, ReplacementQualificationFailure,
    ReplacementQualificationIdentity, ReplacementQualificationRequest, RootResultPumpBinding,
};
use novarocks_task_codec::TransportBudget;
use novarocks_types::NativeCompatibilityId;
use novarocks_workload_control::WorkOwner;
use tokio::sync::{Mutex as AsyncMutex, watch};

use crate::common::backend_topology::{BackendProcessObservationService, BackendTopologyService};
use crate::coordinator::task_round::{AttemptPumps, install_attempt_pumps};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::fragment_encoder::instance::encode_query_options;
use crate::native::fragment_encoder::submission::encode_native_submission;
use crate::native::fragment_transport::{
    NativeTaskResultTransport, TaskReadGrace, native_root_result_pump_binding,
};
use crate::native::task_transport::{AttemptWireFacts, NativeTaskOperationSink, TaskAckIntake};
use crate::query_execution::artifact::{
    ManifestBoundNativeAttemptInputs, PreparedDistributedAttemptTemplate, PreparedDistributedQuery,
    SnapshotBoundDormantAttemptInputs, TaskExecutionPreparedQuery, TaskManifestBinding,
};
use crate::query_execution::attempt_initialization::{
    AttemptInitializing, RoundCredentialLeaseSource,
};
use crate::query_execution::completion::PreparedLogicalRead;
use crate::query_execution::contract::{DistributedQueryError, ResolvedQueryOptions};
use crate::query_execution::lifecycle_plan::QueryInitOptions;
use crate::query_execution::logical_read::LogicalReadLauncher;
use crate::query_execution::split_assignment::TaskUpdateRetryPolicy;
use crate::query_execution::split_assignment_round::SplitAssignmentRoundGuard;
use crate::runtime_filter::compiler::{
    FrontendRuntimeFilterDeploymentCompilerConfig, compile_scheduled_runtime_filter_deployment,
};
use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
use crate::runtime_filter::plan_encoder::encode_binding_attachment;
use crate::task_execution::abort_effect::{NativeAbortEffectAdapter, NativeAbortEffectIntake};
use crate::task_execution::credential_pump::CredentialRotationPump;
use crate::task_execution::feedback_pump::TaskDynamicFilterReads;
use crate::task_execution::intent::{
    AckPayload, DispatchBatch, OperationIntent, TaskOperationQueueAdmission, TaskOperationSink,
    TaskOperationSubmit,
};
use crate::task_execution::manifest_round::{ManifestAssembledRound, ManifestAttemptCompletion};
use crate::task_execution::sources::AttemptEstablishFacts;
use crate::task_execution::status_intake::{NotifyWake, StatusIntakeWake};

/// One logical execution's Abort port. Every physical attempt installs one
/// exact route and retains its registration through residual convergence.
#[derive(Debug)]
struct LogicalAbortRouter {
    routes: Mutex<BTreeMap<novarocks_types::QueryExecutionId, Arc<NativeAbortEffectAdapter>>>,
    capacity_epoch: watch::Sender<u64>,
    worker_closed_contexts: Arc<Mutex<BTreeSet<QueryContextRef>>>,
    closure_epoch: watch::Sender<u64>,
}

impl LogicalAbortRouter {
    fn new() -> Arc<Self> {
        let (capacity_epoch, _) = watch::channel(0);
        let (closure_epoch, _) = watch::channel(0);
        Arc::new(Self {
            routes: Mutex::new(BTreeMap::new()),
            capacity_epoch,
            worker_closed_contexts: Arc::new(Mutex::new(BTreeSet::new())),
            closure_epoch,
        })
    }

    fn install(
        self: &Arc<Self>,
        execution: novarocks_types::QueryExecutionId,
        capacity: NonZeroUsize,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Result<(LogicalAbortRoute, NativeAbortEffectIntake), NativeAttemptActivationFailure> {
        let closed = Arc::clone(&self.worker_closed_contexts);
        let closure_epoch = self.closure_epoch.clone();
        let observer = Arc::new(move |context| {
            closed
                .lock()
                .expect("logical Abort closed-context facts")
                .insert(context);
            closure_epoch.send_modify(|epoch| *epoch = epoch.saturating_add(1));
        });
        let (adapter, intake) = NativeAbortEffectAdapter::bounded_with_observer(
            capacity,
            wake,
            self.capacity_epoch.clone(),
            Some(observer),
        );
        let mut routes = self.routes.lock().expect("logical Abort route table");
        if routes.insert(execution, adapter).is_some() {
            return Err(NativeAttemptActivationFailure::new(
                attempt_runtime_failure(
                    AttemptFailureClass::ContractViolation,
                    QueryExecutionErrorKind::InvalidRequest,
                    "logical execution installed the same Native Abort route twice",
                ),
            ));
        }
        drop(routes);
        self.capacity_epoch
            .send_modify(|epoch| *epoch = epoch.saturating_add(1));
        Ok((
            LogicalAbortRoute {
                execution,
                router: Arc::clone(self),
            },
            intake,
        ))
    }

    fn worker_closed_contexts(&self) -> Arc<Mutex<BTreeSet<QueryContextRef>>> {
        Arc::clone(&self.worker_closed_contexts)
    }

    fn closure_epoch(&self) -> watch::Sender<u64> {
        self.closure_epoch.clone()
    }
}

impl AbortQueryContextEffectPort for LogicalAbortRouter {
    fn subscribe_capacity(&self) -> watch::Receiver<u64> {
        self.capacity_epoch.subscribe()
    }

    fn try_reserve(
        &self,
        identity: AbortQueryContextIssueIdentity,
    ) -> AbortQueryContextEffectAdmission {
        let execution = identity.context().query_execution_id();
        self.routes
            .lock()
            .expect("logical Abort route table")
            .get(&execution)
            .map_or(AbortQueryContextEffectAdmission::Backpressured, |route| {
                route.try_reserve(identity)
            })
    }
}

#[derive(Debug)]
struct LogicalAbortRoute {
    execution: novarocks_types::QueryExecutionId,
    router: Arc<LogicalAbortRouter>,
}

impl Drop for LogicalAbortRoute {
    fn drop(&mut self) {
        self.router
            .routes
            .lock()
            .expect("logical Abort route table")
            .remove(&self.execution);
        self.router
            .capacity_epoch
            .send_modify(|epoch| *epoch = epoch.saturating_add(1));
    }
}

#[derive(Clone, Debug)]
struct ReplacementCandidate {
    topology_revision: u64,
    targets: BTreeMap<
        novarocks_types::BackendProcessId,
        crate::common::backend_topology::LiveBackendTarget,
    >,
}

#[derive(Clone)]
struct LogicalReplacementQualificationPort {
    inner: Arc<LogicalReplacementQualificationState>,
}

struct LogicalReplacementQualificationState {
    runtime: FrontendNativeLogicalExecutionRuntime,
    candidates: Mutex<BTreeMap<novarocks_types::QueryExecutionId, ReplacementCandidate>>,
    busy: AtomicBool,
    capacity_epoch: watch::Sender<u64>,
    worker_closed_contexts: Arc<Mutex<BTreeSet<QueryContextRef>>>,
    closure_epoch: watch::Sender<u64>,
}

impl std::fmt::Debug for LogicalReplacementQualificationPort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LogicalReplacementQualificationPort")
            .field("busy", &self.inner.busy.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl LogicalReplacementQualificationPort {
    fn new(
        runtime: FrontendNativeLogicalExecutionRuntime,
        worker_closed_contexts: Arc<Mutex<BTreeSet<QueryContextRef>>>,
        closure_epoch: watch::Sender<u64>,
    ) -> Self {
        let (capacity_epoch, _) = watch::channel(0);
        Self {
            inner: Arc::new(LogicalReplacementQualificationState {
                runtime,
                candidates: Mutex::new(BTreeMap::new()),
                busy: AtomicBool::new(false),
                capacity_epoch,
                worker_closed_contexts,
                closure_epoch,
            }),
        }
    }

    fn register_candidate(
        &self,
        execution: novarocks_types::QueryExecutionId,
        snapshot: &crate::common::backend_topology::BackendTopologySnapshot,
    ) -> Result<(), NativeAttemptPreparationError> {
        let mut targets = BTreeMap::new();
        for target in snapshot.targets() {
            let process = target.process_id().map_err(|error| {
                attempt_failure(
                    AttemptFailureClass::ContractViolation,
                    QueryExecutionErrorKind::InvalidRequest,
                    error.to_string(),
                )
            })?;
            if targets.insert(process, target.clone()).is_some() {
                return Err(attempt_failure(
                    AttemptFailureClass::ContractViolation,
                    QueryExecutionErrorKind::InvalidRequest,
                    "replacement candidate topology repeats a Worker process",
                ));
            }
        }
        self.inner
            .candidates
            .lock()
            .expect("replacement candidate registry")
            .insert(
                execution,
                ReplacementCandidate {
                    topology_revision: snapshot.revision(),
                    targets,
                },
            );
        Ok(())
    }

    async fn qualify(self, submission: ReplacementQualificationEffectSubmission) {
        let cancellation = submission.subscribe_cancellation();
        let result = self
            .qualify_request(submission.request(), cancellation)
            .await;
        match result {
            Ok(reservation) => match QualifiedReplacementReservation::try_new(
                submission.request(),
                Box::new(reservation),
            ) {
                Ok(reservation) => {
                    let _ = submission.qualified(reservation);
                }
                Err(_) => submission.rejected(),
            },
            Err(_) => {
                submission.rejected();
            }
        }
        self.inner.busy.store(false, Ordering::Release);
        self.inner
            .capacity_epoch
            .send_modify(|epoch| *epoch = epoch.saturating_add(1));
    }

    async fn qualify_request(
        &self,
        request: &ReplacementQualificationRequest,
        mut cancellation: watch::Receiver<bool>,
    ) -> Result<FrontendAttemptIsolationReservation, ReplacementQualificationFailure> {
        if *cancellation.borrow() {
            return Err(ReplacementQualificationFailure::Rejected);
        }
        let (failed, replacement) = {
            let candidates = self
                .inner
                .candidates
                .lock()
                .expect("replacement candidate registry");
            (
                candidates
                    .get(&request.identity().failed())
                    .cloned()
                    .ok_or(ReplacementQualificationFailure::Rejected)?,
                candidates
                    .get(&request.identity().replacement())
                    .cloned()
                    .ok_or(ReplacementQualificationFailure::Rejected)?,
            )
        };
        let mut closure_epoch = self.inner.closure_epoch.subscribe();
        let mut process_epoch = self
            .inner
            .runtime
            .process_observation
            .subscribe_process_changes();
        for context in request.failed_contexts() {
            let target = failed
                .targets
                .get(&context.backend_process_id())
                .ok_or(ReplacementQualificationFailure::InvalidReservation)?;
            let endpoint = target
                .endpoint()
                .map_err(|_| ReplacementQualificationFailure::InvalidReservation)?;
            wait_for_failed_context_isolation(
                &self.inner.runtime.process_observation,
                *context,
                &endpoint,
                &self.inner.worker_closed_contexts,
                &mut closure_epoch,
                &mut process_epoch,
                &mut cancellation,
                request.absolute_expiry(),
            )
            .await?;
        }
        let admissions = acquire_replacement_admissions(
            &self.inner.runtime,
            request,
            &replacement,
            cancellation,
        )
        .await?;
        Ok(FrontendAttemptIsolationReservation {
            identity: request.identity(),
            topology_revision: replacement.topology_revision,
            failed_contexts: request.failed_contexts().iter().copied().collect(),
            admissions,
            runtime: self.inner.runtime.data_runtime.clone(),
        })
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the wait consumes every independent identity, evidence, and bound"
)]
async fn wait_for_failed_context_isolation(
    process_observation: &BackendProcessObservationService,
    context: QueryContextRef,
    endpoint: &novarocks_execution::runtime::endpoint::RuntimeEndpoint,
    worker_closed_contexts: &Arc<Mutex<BTreeSet<QueryContextRef>>>,
    closure_epoch: &mut watch::Receiver<u64>,
    process_epoch: &mut watch::Receiver<u64>,
    cancellation: &mut watch::Receiver<bool>,
    absolute_expiry: Instant,
) -> Result<(), ReplacementQualificationFailure> {
    loop {
        if *cancellation.borrow() {
            return Err(ReplacementQualificationFailure::Rejected);
        }
        let observation =
            process_observation.observe_process_at_endpoint(context.backend_process_id(), endpoint);
        let closed = worker_closed_contexts
            .lock()
            .expect("logical Abort closed-context facts")
            .contains(&context);
        if failed_context_allows_successor(&observation, closed) {
            return Ok(());
        }
        tokio::select! {
            _ = closure_epoch.changed() => {},
            _ = process_epoch.changed() => {},
            _ = cancellation.changed() => {},
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(absolute_expiry)) => {
                return Err(ReplacementQualificationFailure::Expired);
            }
        }
    }
}

fn failed_context_allows_successor(
    observation: &Result<
        crate::common::backend_topology::BackendProcessObservation,
        crate::common::backend_topology::BackendTopologyError,
    >,
    worker_closed: bool,
) -> bool {
    match observation {
        Ok(crate::common::backend_topology::BackendProcessObservation::Current) => worker_closed,
        Ok(
            crate::common::backend_topology::BackendProcessObservation::Unobservable
            | crate::common::backend_topology::BackendProcessObservation::Replaced { .. },
        )
        | Err(_) => true,
    }
}

struct LogicalReplacementReservation {
    port: LogicalReplacementQualificationPort,
    submitted: bool,
}

impl std::fmt::Debug for LogicalReplacementReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LogicalReplacementReservation")
    }
}

impl ReplacementQualificationEffectReservation for LogicalReplacementReservation {
    fn submit(mut self: Box<Self>, submission: ReplacementQualificationEffectSubmission) {
        self.submitted = true;
        let port = self.port.clone();
        let runtime = port.inner.runtime.data_runtime.clone();
        runtime.spawn(async move { port.qualify(submission).await });
    }
}

impl Drop for LogicalReplacementReservation {
    fn drop(&mut self) {
        if !self.submitted {
            self.port.inner.busy.store(false, Ordering::Release);
            self.port
                .inner
                .capacity_epoch
                .send_modify(|epoch| *epoch = epoch.saturating_add(1));
        }
    }
}

impl ReplacementQualificationEffectPort for LogicalReplacementQualificationPort {
    fn subscribe_capacity(&self) -> watch::Receiver<u64> {
        self.inner.capacity_epoch.subscribe()
    }

    fn try_reserve(
        &self,
        _request: &ReplacementQualificationRequest,
    ) -> ReplacementQualificationEffectAdmission {
        if self
            .inner
            .busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            ReplacementQualificationEffectAdmission::Backpressured
        } else {
            ReplacementQualificationEffectAdmission::Admitted(Box::new(
                LogicalReplacementReservation {
                    port: self.clone(),
                    submitted: false,
                },
            ))
        }
    }
}

struct FrontendAttemptIsolationReservation {
    identity: ReplacementQualificationIdentity,
    topology_revision: u64,
    failed_contexts: BTreeSet<QueryContextRef>,
    admissions: Box<[QualifiedWorkerAdmission]>,
    runtime: FrontendDataRuntime,
}

impl std::fmt::Debug for FrontendAttemptIsolationReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrontendAttemptIsolationReservation")
            .field("identity", &self.identity)
            .field("topology_revision", &self.topology_revision)
            .field("admissions", &self.admissions.len())
            .finish_non_exhaustive()
    }
}

impl AttemptIsolationReservation for FrontendAttemptIsolationReservation {
    fn identity(&self) -> ReplacementQualificationIdentity {
        self.identity
    }

    fn topology_revision(&self) -> u64 {
        self.topology_revision
    }

    fn admissions(&self) -> &[QualifiedWorkerAdmission] {
        &self.admissions
    }

    fn validate_binding(
        &self,
        failed_contexts: &[QueryContextRef],
    ) -> Result<(), ReplacementQualificationFailure> {
        let expected = failed_contexts.iter().copied().collect::<BTreeSet<_>>();
        (expected == self.failed_contexts)
            .then_some(())
            .ok_or(ReplacementQualificationFailure::InvalidReservation)
    }

    fn activate(
        self: Box<Self>,
    ) -> Result<Box<dyn ActiveAttemptIsolationOwner>, AttemptIsolationActivationFailure> {
        Ok(Box::new(FrontendActiveIsolationOwner {
            execution: self.identity.replacement(),
            runtime: self.runtime.clone(),
            admissions: Some(self.admissions),
        }))
    }

    fn abandon(self: Box<Self>) {
        retain_admissions_until_expiry(self.runtime.clone(), self.admissions);
    }
}

struct FrontendActiveIsolationOwner {
    execution: novarocks_types::QueryExecutionId,
    runtime: FrontendDataRuntime,
    admissions: Option<Box<[QualifiedWorkerAdmission]>>,
}

impl std::fmt::Debug for FrontendActiveIsolationOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrontendActiveIsolationOwner")
            .field("execution", &self.execution)
            .field(
                "admissions",
                &self.admissions.as_ref().map(|value| value.len()),
            )
            .finish_non_exhaustive()
    }
}

impl ActiveAttemptIsolationOwner for FrontendActiveIsolationOwner {
    fn execution(&self) -> novarocks_types::QueryExecutionId {
        self.execution
    }

    fn finish(mut self: Box<Self>) {
        self.admissions.take();
    }

    fn abandon(mut self: Box<Self>) {
        if let Some(admissions) = self.admissions.take() {
            retain_admissions_until_expiry(self.runtime.clone(), admissions);
        }
    }
}

fn retain_admissions_until_expiry(
    runtime: FrontendDataRuntime,
    admissions: Box<[QualifiedWorkerAdmission]>,
) {
    let duration = admissions
        .iter()
        .map(|admission| admission.receipt().valid_for().get())
        .max()
        .unwrap_or_default();
    runtime.spawn(async move {
        tokio::time::sleep(duration).await;
        drop(admissions);
    });
}

async fn acquire_replacement_admissions(
    runtime: &FrontendNativeLogicalExecutionRuntime,
    qualification: &ReplacementQualificationRequest,
    candidate: &ReplacementCandidate,
    mut cancellation: watch::Receiver<bool>,
) -> Result<Box<[QualifiedWorkerAdmission]>, ReplacementQualificationFailure> {
    let mut backends = Vec::with_capacity(qualification.replacement_contexts().len());
    for context in qualification.replacement_contexts() {
        let target = candidate
            .targets
            .get(&context.backend_process_id())
            .ok_or(ReplacementQualificationFailure::InvalidReservation)?;
        backends.push((
            context.backend_process_id(),
            target
                .endpoint()
                .map_err(|_| ReplacementQualificationFailure::InvalidReservation)?,
        ));
    }
    let notify = Arc::new(tokio::sync::Notify::new());
    let wake = Arc::new(NotifyWake::new(Arc::clone(&notify))) as Arc<dyn StatusIntakeWake>;
    let acks = TaskAckIntake::new(wake);
    let sink = NativeTaskOperationSink::new(
        &backends,
        runtime.transport_budget,
        AttemptWireFacts {
            native_compatibility_id: runtime.native_compatibility_id,
        },
        acks.handle(),
        runtime.data_runtime.clone(),
    )
    .map_err(|_| ReplacementQualificationFailure::EffectOwnerClosed)?;
    let valid_for =
        LeaseValidFor::new(crate::task_execution::context_owner::ADMISSION_TICKET_VALID_FOR)
            .map_err(|_| ReplacementQualificationFailure::InvalidReservation)?;
    let acquire = async {
        let mut admissions = PendingReplacementAdmissions::new(runtime.data_runtime.clone());
        for context in qualification.replacement_contexts() {
            if *cancellation.borrow() {
                return Err(ReplacementQualificationFailure::Rejected);
            }
            let request = AcquireQueryContextAdmissionTicket::new(
                TaskOperationId::new_v7(),
                *context,
                valid_for,
                runtime.native_compatibility_id,
                candidate
                    .targets
                    .get(&context.backend_process_id())
                    .expect("replacement context target checked above")
                    .admission_epoch_capability(),
            );
            let intent = OperationIntent::AcquireQueryContextAdmissionTicket(request);
            loop {
                let permit = match sink.try_reserve_queue(intent.queue_request()) {
                    TaskOperationQueueAdmission::Admitted(permit) => permit,
                    TaskOperationQueueAdmission::Backpressured => {
                        tokio::select! {
                            _ = tokio::task::yield_now() => {},
                            _ = cancellation.changed() => {
                                return Err(ReplacementQualificationFailure::Rejected);
                            }
                        }
                        continue;
                    }
                };
                let batch = DispatchBatch::with_queued_at(
                    context.backend_process_id(),
                    novarocks_query_application::coordination::DispatchLane::Lifecycle,
                    vec![intent.clone()],
                    vec![permit],
                    intent.queued_bytes(),
                    qualification.issued_at(),
                );
                match sink.try_submit(batch) {
                    TaskOperationSubmit::Accepted => {}
                    TaskOperationSubmit::Backpressured(_) => {
                        tokio::select! {
                            _ = tokio::task::yield_now() => {},
                            _ = cancellation.changed() => {
                                return Err(ReplacementQualificationFailure::Rejected);
                            }
                        }
                        continue;
                    }
                    TaskOperationSubmit::Rejected { .. } => {
                        return Err(ReplacementQualificationFailure::Rejected);
                    }
                }
                let acknowledgement = loop {
                    if let Some(ack) = acks
                        .drain()
                        .into_iter()
                        .find(|ack| ack.operation_id() == request.envelope().operation_id())
                    {
                        break ack;
                    }
                    tokio::select! {
                        _ = notify.notified() => {},
                        _ = cancellation.changed() => {
                            return Err(ReplacementQualificationFailure::Rejected);
                        }
                    }
                };
                match (acknowledgement.worker_outcome(), acknowledgement.payload()) {
                    (
                        Some(OperationOutcome::Accepted | OperationOutcome::Idempotent),
                        AckPayload::AdmissionTicket(receipt),
                    ) => {
                        admissions.push(QualifiedWorkerAdmission::try_new(
                            qualification.identity().replacement(),
                            request,
                            *receipt,
                        )?);
                        break;
                    }
                    (None, _) => continue,
                    _ => {
                        return Err(ReplacementQualificationFailure::Rejected);
                    }
                }
            }
        }
        Ok(admissions.finish())
    };
    tokio::time::timeout_at(
        tokio::time::Instant::from_std(qualification.absolute_expiry()),
        acquire,
    )
    .await
    .map_err(|_| ReplacementQualificationFailure::Expired)?
}

struct PendingReplacementAdmissions {
    runtime: FrontendDataRuntime,
    admissions: Option<Vec<QualifiedWorkerAdmission>>,
}

impl PendingReplacementAdmissions {
    fn new(runtime: FrontendDataRuntime) -> Self {
        Self {
            runtime,
            admissions: Some(Vec::new()),
        }
    }

    fn push(&mut self, admission: QualifiedWorkerAdmission) {
        self.admissions
            .as_mut()
            .expect("pending replacement admissions remain armed")
            .push(admission);
    }

    fn finish(mut self) -> Box<[QualifiedWorkerAdmission]> {
        self.admissions
            .take()
            .expect("pending replacement admissions finish once")
            .into_boxed_slice()
    }
}

impl Drop for PendingReplacementAdmissions {
    fn drop(&mut self) {
        let Some(admissions) = self.admissions.take() else {
            return;
        };
        if !admissions.is_empty() {
            retain_admissions_until_expiry(self.runtime.clone(), admissions.into_boxed_slice());
        }
    }
}

/// Stateless process adapter which consumes the exact Native seed already
/// sealed into an executable Query Application request.
///
/// Attempt-specific state belongs to the move-only preparation port carried by
/// that seed. Keeping this adapter stateless lets the process host share one
/// narrow handle without becoming an alternate execution owner.
#[derive(Debug, Default)]
pub(crate) struct FrontendLogicalExecutionNativePort;

impl LogicalExecutionNativePort for FrontendLogicalExecutionNativePort {
    fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
        Box::pin(async move { request.bind().map_err(Into::into) })
    }
}

/// Explicit process capabilities used to instantiate one logical Native read.
/// Every field is fixed by server composition; an attempt may only project a
/// manifest through these handles and cannot reacquire policy from globals.
#[derive(Clone)]
pub(crate) struct FrontendNativeLogicalExecutionRuntime {
    topology: BackendTopologyService,
    process_observation: BackendProcessObservationService,
    data_runtime: FrontendDataRuntime,
    decode_runtime: novarocks_query_application::coordination::RootResultDecodeRuntime,
    native_compatibility_id: NativeCompatibilityId,
    runtime_filter_worker_count: NonZeroUsize,
    task_update_retry_policy: TaskUpdateRetryPolicy,
    split_initial_wait_cap: Duration,
    coordination_budgets: CoordinationBudgets,
    transport_budget: TransportBudget,
    abort_capacity: NonZeroUsize,
}

impl std::fmt::Debug for FrontendNativeLogicalExecutionRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrontendNativeLogicalExecutionRuntime")
            .field("native_compatibility_id", &self.native_compatibility_id)
            .field(
                "runtime_filter_worker_count",
                &self.runtime_filter_worker_count,
            )
            .field("coordination_budgets", &self.coordination_budgets)
            .field("transport_budget", &self.transport_budget)
            .field("abort_capacity", &self.abort_capacity)
            .finish_non_exhaustive()
    }
}

impl FrontendNativeLogicalExecutionRuntime {
    #[expect(
        clippy::too_many_arguments,
        reason = "server composition supplies each independent Native runtime policy"
    )]
    pub(crate) fn new(
        topology: BackendTopologyService,
        process_observation: BackendProcessObservationService,
        data_runtime: FrontendDataRuntime,
        decode_runtime: novarocks_query_application::coordination::RootResultDecodeRuntime,
        native_compatibility_id: NativeCompatibilityId,
        runtime_filter_worker_count: NonZeroUsize,
        task_update_retry_policy: TaskUpdateRetryPolicy,
        split_initial_wait_cap: Duration,
        coordination_budgets: CoordinationBudgets,
        transport_budget: TransportBudget,
        abort_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            topology,
            process_observation,
            data_runtime,
            decode_runtime,
            native_compatibility_id,
            runtime_filter_worker_count,
            task_update_retry_policy,
            split_initial_wait_cap,
            coordination_budgets,
            transport_budget,
            abort_capacity,
        }
    }
}

#[derive(Clone)]
pub(crate) struct FrontendNativeLogicalReadLauncher {
    client: QueryExecutionClient,
    runtime: FrontendNativeLogicalExecutionRuntime,
}

impl FrontendNativeLogicalReadLauncher {
    pub(crate) const fn new(
        client: QueryExecutionClient,
        runtime: FrontendNativeLogicalExecutionRuntime,
    ) -> Self {
        Self { client, runtime }
    }
}

impl LogicalReadLauncher for FrontendNativeLogicalReadLauncher {
    fn start(&self, read: PreparedLogicalRead, owner: WorkOwner) -> QueryExecutionFuture {
        let (description, template, options) = read.into_parts();
        if !description.matches_plan_seal(template.native_manifest_template().plan_seal()) {
            return Box::pin(async {
                Err(QueryExecutionError::new(
                    QueryExecutionErrorKind::InvalidRequest,
                    "prepared logical read Native template has a foreign plan seal",
                ))
            });
        }
        let aborts = LogicalAbortRouter::new();
        let replacements = LogicalReplacementQualificationPort::new(
            self.runtime.clone(),
            aborts.worker_closed_contexts(),
            aborts.closure_epoch(),
        );
        let factory = ProductionDormantAttemptFactory {
            projection: ProductionManifestAttemptProjection {
                runtime: self.runtime.clone(),
                options,
                aborts: Arc::clone(&aborts),
            },
            replacements: replacements.clone(),
        };
        let attempts = FrontendNativeAttemptPreparationPort::new(
            template,
            Arc::clone(&self.runtime.topology),
            factory,
        );
        let replacement_port = match description.recovery() {
            RecoveryMode::NoRecovery => None,
            RecoveryMode::RestartAttemptBeforeVisibility => {
                Some(Arc::new(replacements) as Arc<dyn ReplacementQualificationEffectPort>)
            }
        };
        let request = QueryExecutionRequest::bind_native(
            description,
            aborts as Arc<dyn AbortQueryContextEffectPort>,
            replacement_port,
            attempts,
        );
        self.client.start(request, owner)
    }
}

struct GovernedConnectorCancellation(CancellationView);

impl novarocks_spi::connector::ConnectorCancellation for GovernedConnectorCancellation {
    fn is_cancelled(&self) -> bool {
        self.0.reason().is_some()
    }
}

#[derive(Clone)]
struct ProductionManifestAttemptProjection {
    runtime: FrontendNativeLogicalExecutionRuntime,
    options: Arc<ResolvedQueryOptions>,
    aborts: Arc<LogicalAbortRouter>,
}

impl std::fmt::Debug for ProductionManifestAttemptProjection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductionManifestAttemptProjection")
            .field("runtime", &self.runtime)
            .finish_non_exhaustive()
    }
}

/// Frontend-local Task protocol and transport behavior used by the fixed
/// snapshot-owning dormant adapter.
///
/// The behavior only borrows the manifest already bound by the adapter. It
/// cannot provide eligible backend identities, substitute a topology snapshot,
/// or move attempt leases into an activation future. The adapter transfers the
/// manifest only after activation returns an active behavior successfully.
pub(crate) trait FrontendActiveAttemptBehavior<M = ManifestBoundNativeAttemptInputs>:
    std::fmt::Debug + Send + 'static
{
    fn take_rows_runtime(&mut self) -> Option<(RootResultPumpBinding, AcceptedRootStatusSource)> {
        None
    }

    fn run<'a>(
        &'a mut self,
        inputs: &'a mut M,
        drive: &'a NativeAttemptDrive,
        cancellation: CancellationView,
    ) -> NativeAttemptRunFuture<'a>;

    fn converge<'a>(
        &'a mut self,
        inputs: &'a mut M,
        cancellation: CancellationView,
    ) -> NativeActiveAttemptConvergenceFuture<'a>;
}

pub(crate) trait FrontendDormantAttemptBehavior<M = ManifestBoundNativeAttemptInputs>:
    std::fmt::Debug + Send + 'static
{
    type ActiveBehavior: FrontendActiveAttemptBehavior<M>;

    fn activate<'a>(
        &'a mut self,
        inputs: &'a mut M,
        replacement_admissions: Option<
            Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>,
        >,
        cancellation: CancellationView,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Self::ActiveBehavior, NativeAttemptActivationFailure>,
                > + Send
                + 'a,
        >,
    >;

    fn converge<'a>(
        &'a mut self,
        inputs: &'a mut M,
        cancellation: CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a>;
}

/// Frontend-local constructor for Task protocol and transport behavior. The
/// adapter itself remains the only constructor of the complete dormant owner.
pub(crate) trait FrontendDormantAttemptFactory: std::fmt::Debug + Send + 'static {
    type Behavior: FrontendDormantAttemptBehavior<ManifestBoundNativeAttemptInputs>;

    fn create(&mut self) -> Result<Self::Behavior, NativeAttemptPreparationError>;

    fn register_candidate(
        &mut self,
        _execution: novarocks_types::QueryExecutionId,
        _snapshot: &crate::common::backend_topology::BackendTopologySnapshot,
    ) -> Result<(), NativeAttemptPreparationError> {
        Ok(())
    }
}

/// C3's exact-manifest runtime projection consumed by the concrete Task
/// protocol behavior.
///
/// Implementations may encode the already frozen fragment plans and create
/// attempt-local split/RF/credential owners. They cannot receive a compiler,
/// live topology service, legacy `SchedulingPlan`, or backend ordinal map.
/// Every resource made live while this borrowed future is polled must remain
/// owned by the projection until it is transferred into the returned round;
/// `converge` resumes cleanup after cancellation, error, or panic.
pub(crate) trait FrontendManifestAttemptProjection:
    std::fmt::Debug + Send + 'static
{
    fn project<'a>(
        &'a mut self,
        prepared: PreparedDistributedQuery,
        manifest: &'a TaskManifestBinding,
        replacement_admissions: Option<
            Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>,
        >,
        cancellation: CancellationView,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ProjectedManifestAttempt, NativeAttemptActivationFailure>,
                > + Send
                + 'a,
        >,
    >;

    fn converge<'a>(
        &'a mut self,
        cancellation: CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a>;
}

/// One exact Task round and its move-only root result owners.
pub(crate) struct ProjectedManifestAttempt {
    round: ManifestAssembledRound,
    rows: Option<(RootResultPumpBinding, AcceptedRootStatusSource)>,
    _prepared: Option<TaskExecutionPreparedQuery>,
    _split_assignment: Option<SplitAssignmentRoundGuard>,
    _credential_rotation: Option<Arc<CredentialRotationPump>>,
    _abort_route: Option<LogicalAbortRoute>,
}

impl ProjectedManifestAttempt {
    pub(crate) fn rows(
        round: ManifestAssembledRound,
        binding: RootResultPumpBinding,
        statuses: AcceptedRootStatusSource,
    ) -> Self {
        Self {
            round,
            rows: Some((binding, statuses)),
            _prepared: None,
            _split_assignment: None,
            _credential_rotation: None,
            _abort_route: None,
        }
    }

    fn production_rows(
        round: ManifestAssembledRound,
        binding: RootResultPumpBinding,
        statuses: AcceptedRootStatusSource,
        prepared: TaskExecutionPreparedQuery,
        split_assignment: Option<SplitAssignmentRoundGuard>,
        credential_rotation: Option<Arc<CredentialRotationPump>>,
        abort_route: LogicalAbortRoute,
    ) -> Self {
        Self {
            round,
            rows: Some((binding, statuses)),
            _prepared: Some(prepared),
            _split_assignment: split_assignment,
            _credential_rotation: credential_rotation,
            _abort_route: Some(abort_route),
        }
    }
}

#[derive(Clone)]
struct ProductionDormantAttemptFactory {
    projection: ProductionManifestAttemptProjection,
    replacements: LogicalReplacementQualificationPort,
}

impl std::fmt::Debug for ProductionDormantAttemptFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductionDormantAttemptFactory")
            .finish_non_exhaustive()
    }
}

impl FrontendDormantAttemptFactory for ProductionDormantAttemptFactory {
    type Behavior = FrontendTaskProtocolDormantBehavior<ProductionManifestAttemptProjection>;

    fn create(&mut self) -> Result<Self::Behavior, NativeAttemptPreparationError> {
        Ok(FrontendTaskProtocolDormantBehavior::new(
            self.projection.clone(),
            ManifestAttemptCompletion::AcceptedRootSuccessSeal,
        ))
    }

    fn register_candidate(
        &mut self,
        execution: novarocks_types::QueryExecutionId,
        snapshot: &crate::common::backend_topology::BackendTopologySnapshot,
    ) -> Result<(), NativeAttemptPreparationError> {
        self.replacements.register_candidate(execution, snapshot)
    }
}

impl FrontendManifestAttemptProjection for ProductionManifestAttemptProjection {
    fn project<'a>(
        &'a mut self,
        prepared: PreparedDistributedQuery,
        manifest: &'a TaskManifestBinding,
        replacement_admissions: Option<
            Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>,
        >,
        cancellation: CancellationView,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ProjectedManifestAttempt, NativeAttemptActivationFailure>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.project_attempt(prepared, manifest, replacement_admissions, cancellation)
                .await
        })
    }

    fn converge<'a>(
        &'a mut self,
        _cancellation: CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a> {
        Box::pin(async {})
    }
}

impl ProductionManifestAttemptProjection {
    async fn project_attempt(
        &self,
        prepared: PreparedDistributedQuery,
        manifest: &TaskManifestBinding,
        replacement_admissions: Option<
            Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>,
        >,
        cancellation: CancellationView,
    ) -> Result<ProjectedManifestAttempt, NativeAttemptActivationFailure> {
        let execution = manifest.execution();
        let schedule = prepared
            .native_schedule_from_manifest(manifest)
            .map_err(projection_failure)?;
        let feedback = Arc::new(
            RuntimeFilterFeedbackState::new(execution, Default::default())
                .map_err(|error| projection_message(error.to_string()))?,
        );
        let deadline = cancellation.deadline().map(Into::into).unwrap_or_else(|| {
            Instant::now()
                .checked_add(Duration::from_millis(
                    self.options.timeout_ms().max(1) as u64
                ))
                .unwrap_or_else(Instant::now)
        });
        let credential_source = RoundCredentialLeaseSource::fresh(execution);
        let connector_context = novarocks_spi::connector::ConnectorRequestContext::try_new(
            deadline,
            Arc::new(GovernedConnectorCancellation(cancellation.clone())),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .map(crate::connector::install_frontend_connector_resources)
        .map_err(|error| projection_message(error.to_string()))?;
        let connector_context = credential_source.connector_request_context(connector_context);
        let ready = AttemptInitializing::new_governed(
            execution,
            prepared,
            schedule,
            self.runtime.task_update_retry_policy,
            Arc::clone(&feedback),
            self.runtime.split_initial_wait_cap,
            connector_context,
            cancellation,
            self.runtime.data_runtime.clone(),
            credential_source,
        )
        .map_err(projection_failure)?
        .initialize()
        .await
        .map_err(projection_failure)?;
        let (ready_execution, artifacts, schedule, ready_feedback, split_plan, credential_leases) =
            ready.into_parts();
        if ready_execution != execution || !Arc::ptr_eq(&feedback, &ready_feedback) {
            return Err(projection_message(
                "attempt initialization returned facts for another Native attempt",
            ));
        }

        let bindings = encode_binding_attachment(artifacts.runtime_filter_binding_view())
            .map_err(projection_failure)?;
        let scheduled = artifacts
            .attach_runtime_filter_bindings(bindings)
            .and_then(|artifacts| artifacts.bind_schedule(schedule))
            .map_err(projection_failure)?;
        let deployment = compile_scheduled_runtime_filter_deployment(
            scheduled
                .runtime_filter_scheduled_view()
                .map_err(projection_failure)?,
            FrontendRuntimeFilterDeploymentCompilerConfig::from_query_lifecycle(
                self.options.runtime_filter_lifecycle(),
                self.runtime.runtime_filter_worker_count.get(),
            )
            .map_err(projection_failure)?,
        )
        .map_err(projection_failure)?;
        let feedback_declaration = deployment.feedback_declaration().clone();
        feedback
            .configure(feedback_declaration.clone())
            .map_err(projection_message)?;
        let deployment = scheduled
            .seal_runtime_filter_deployment(deployment.contributions())
            .map_err(projection_failure)?;
        let ready = scheduled
            .attach_runtime_filter_deployment(deployment)
            .map_err(projection_failure)?;

        let live_backends = manifest
            .contexts()
            .iter()
            .map(|context| context.backend().target().clone())
            .collect::<Vec<_>>();
        let options = QueryInitOptions::new(
            execution,
            self.runtime.native_compatibility_id,
            live_backends,
            &self.options,
            ProtocolQueryOptions::parse(encode_query_options(self.options.runtime_options()))
                .map_err(|error| projection_message(error.to_string()))?,
        )
        .map_err(projection_failure)?
        .with_credential_leases(credential_leases);
        let mut task_prepared = ready
            .prepare_task_execution(options)
            .map_err(projection_failure)?;
        let submission = encode_native_submission(
            &task_prepared
                .native_submission_view()
                .map_err(projection_failure)?,
        )
        .map_err(projection_message)?;
        let (submissions, root_fetch, expected_output) = task_prepared
            .seal_task_submission(submission)
            .map_err(projection_failure)?
            .into_parts();
        if !root_fetch.uses_result_buffer() {
            return Err(projection_message(
                "logical read root does not own a Native result buffer",
            ));
        }

        let mut runtime_filters = Vec::with_capacity(manifest.contexts().len());
        let compiled_filters = task_prepared.runtime_filter_contributions();
        for context in manifest.contexts() {
            let backend = context.backend();
            let contribution = match compiled_filters.get(&backend.backend_idx()) {
                Some(contribution) => contribution.clone(),
                None if compiled_filters.is_empty() => Default::default(),
                None => {
                    return Err(projection_message(format!(
                        "runtime filter deployment omits manifest backend {}",
                        backend.process_id()
                    )));
                }
            };
            runtime_filters.push((backend.process_id(), contribution));
        }
        let establish = AttemptEstablishFacts::freeze(
            task_prepared.catalog_set().as_proto().clone(),
            runtime_filters,
            *task_prepared.init_options().query_options().as_proto(),
            task_prepared.init_options().credential_leases(),
        )
        .map_err(|error| projection_message(error.to_string()))?;
        let initial_credential = establish.credential().clone();
        let credential_storage = task_prepared.take_terminal_storage_resolver();
        let backends = manifest
            .contexts()
            .iter()
            .map(|context| {
                (
                    context.backend().process_id(),
                    context.backend().endpoint().clone(),
                )
            })
            .collect::<Vec<_>>();
        let mut round = crate::task_execution::manifest_round::assemble_manifest_round(
            manifest,
            submissions,
            establish,
            replacement_admissions,
            crate::task_execution::manifest_round::ManifestAttemptTransport {
                dispatch_budget: self.runtime.coordination_budgets.dispatch,
                transport_budget: self.runtime.transport_budget,
                status_subscription_error_budget: self
                    .runtime
                    .coordination_budgets
                    .status_subscription_error_budget,
                wire: AttemptWireFacts {
                    native_compatibility_id: self.runtime.native_compatibility_id,
                },
                data_runtime: self.runtime.data_runtime.clone(),
                convergence_source: Arc::clone(&self.runtime.process_observation),
            },
        )
        .map_err(|error| projection_message(error.to_string()))?;
        let result_transport = Arc::new(
            NativeTaskResultTransport::new(
                &backends,
                self.runtime.data_runtime.clone(),
                TaskReadGrace::new(self.runtime.transport_budget.frontend_queue_residence()),
            )
            .map_err(projection_message)?,
        );
        let split_assignment = split_plan.and_then(|plan| {
            round.install_split_assignment(execution, plan, self.runtime.data_runtime.clone())
        });
        let credential_rotation = install_attempt_pumps(
            &mut round.round,
            AttemptPumps {
                execution_id: execution,
                feedback_state: feedback,
                declared_feedback_channels: feedback_declaration.channels().len(),
                reads: Arc::clone(&result_transport) as Arc<dyn TaskDynamicFilterReads>,
                initial_credential: &initial_credential,
                credential_storage,
            },
        );
        let (abort_route, abort_intake) =
            self.aborts
                .install(execution, self.runtime.abort_capacity, round.abort_wake())?;
        round.install_abort_effect_intake(abort_intake);
        let root_status = round.take_root_status_source().ok_or_else(|| {
            projection_message("logical read Task round has no accepted root status source")
        })?;
        let root_binding = native_root_result_pump_binding(
            self.runtime.decode_runtime.clone(),
            result_transport,
            Arc::clone(expected_output.fetch_view().chunk_schema()),
        );
        Ok(ProjectedManifestAttempt::production_rows(
            round,
            root_binding,
            root_status,
            task_prepared,
            split_assignment,
            credential_rotation,
            abort_route,
        ))
    }
}

fn projection_failure(error: DistributedQueryError) -> NativeAttemptActivationFailure {
    projection_message(error.to_string())
}

fn projection_message(message: impl Into<String>) -> NativeAttemptActivationFailure {
    NativeAttemptActivationFailure::new(attempt_runtime_failure(
        AttemptFailureClass::ContractViolation,
        QueryExecutionErrorKind::InvalidRequest,
        message.into(),
    ))
}

/// Dormant half of the concrete manifest-only Task protocol behavior.
pub(crate) struct FrontendTaskProtocolDormantBehavior<P> {
    projection: P,
    completion: ManifestAttemptCompletion,
}

impl<P> std::fmt::Debug for FrontendTaskProtocolDormantBehavior<P>
where
    P: std::fmt::Debug,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrontendTaskProtocolDormantBehavior")
            .field("projection", &self.projection)
            .field("completion", &self.completion)
            .finish()
    }
}

impl<P> FrontendTaskProtocolDormantBehavior<P> {
    pub(crate) const fn new(projection: P, completion: ManifestAttemptCompletion) -> Self {
        Self {
            projection,
            completion,
        }
    }
}

impl<P> FrontendDormantAttemptBehavior for FrontendTaskProtocolDormantBehavior<P>
where
    P: FrontendManifestAttemptProjection,
{
    type ActiveBehavior = FrontendTaskProtocolActiveBehavior;

    fn activate<'a>(
        &'a mut self,
        inputs: &'a mut ManifestBoundNativeAttemptInputs,
        replacement_admissions: Option<
            Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>,
        >,
        cancellation: CancellationView,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Self::ActiveBehavior, NativeAttemptActivationFailure>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let activation = inputs.begin_activation().map_err(|error| {
                NativeAttemptActivationFailure::new(attempt_runtime_failure(
                    AttemptFailureClass::ContractViolation,
                    QueryExecutionErrorKind::InvalidRequest,
                    error.to_string(),
                ))
            })?;
            let attempt = {
                let (prepared, manifest) = activation.into_prepared_and_manifest();
                self.projection
                    .project(prepared, manifest, replacement_admissions, cancellation)
                    .await?
            };
            Ok(FrontendTaskProtocolActiveBehavior::new(
                attempt,
                self.completion,
            ))
        })
    }

    fn converge<'a>(
        &'a mut self,
        _inputs: &'a mut ManifestBoundNativeAttemptInputs,
        cancellation: CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a> {
        self.projection.converge(cancellation)
    }
}

/// Concrete active behavior for the manifest-only Frontend Task protocol.
///
/// C3 constructs this only after it has projected and validated the exact
/// manifest's fragment plans, establish facts and attempt-local source owners.
/// This type receives no compiler, topology reader, scheduling plan or backend
/// ordinal map; its only execution authority is the already assembled round.
pub(crate) struct FrontendTaskProtocolActiveBehavior {
    attempt: ManifestAssembledRound,
    completion: ManifestAttemptCompletion,
    rows: Option<(RootResultPumpBinding, AcceptedRootStatusSource)>,
    prepared: Option<TaskExecutionPreparedQuery>,
    split_assignment: Option<SplitAssignmentRoundGuard>,
    credential_rotation: Option<Arc<CredentialRotationPump>>,
    abort_route: Option<LogicalAbortRoute>,
}

impl std::fmt::Debug for FrontendTaskProtocolActiveBehavior {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrontendTaskProtocolActiveBehavior")
            .field("completion", &self.completion)
            .finish_non_exhaustive()
    }
}

impl FrontendTaskProtocolActiveBehavior {
    pub(crate) fn new(
        attempt: ProjectedManifestAttempt,
        completion: ManifestAttemptCompletion,
    ) -> Self {
        let ProjectedManifestAttempt {
            round,
            rows,
            _prepared: prepared,
            _split_assignment: split_assignment,
            _credential_rotation: credential_rotation,
            _abort_route: abort_route,
        } = attempt;
        Self {
            attempt: round,
            completion,
            rows,
            prepared,
            split_assignment,
            credential_rotation,
            abort_route,
        }
    }
}

impl FrontendActiveAttemptBehavior for FrontendTaskProtocolActiveBehavior {
    fn take_rows_runtime(&mut self) -> Option<(RootResultPumpBinding, AcceptedRootStatusSource)> {
        self.rows.take()
    }

    fn run<'a>(
        &'a mut self,
        _inputs: &'a mut ManifestBoundNativeAttemptInputs,
        drive: &'a NativeAttemptDrive,
        cancellation: CancellationView,
    ) -> NativeAttemptRunFuture<'a> {
        Box::pin(self.attempt.run(drive, cancellation, self.completion))
    }

    fn converge<'a>(
        &'a mut self,
        _inputs: &'a mut ManifestBoundNativeAttemptInputs,
        cancellation: CancellationView,
    ) -> NativeActiveAttemptConvergenceFuture<'a> {
        Box::pin(self.attempt.converge(cancellation))
    }
}

trait RetainedAttemptInputs: std::fmt::Debug + Send + 'static {
    type Manifest: std::fmt::Debug + Send + 'static;

    fn eligible_backends(&self) -> &[novarocks_types::identity::BackendProcessId];

    fn bind_manifest(
        self,
        schedule: &AttemptSchedule,
    ) -> Result<Self::Manifest, NativeAttemptActivationFailure>;
}

impl RetainedAttemptInputs for SnapshotBoundDormantAttemptInputs {
    type Manifest = ManifestBoundNativeAttemptInputs;

    fn eligible_backends(&self) -> &[novarocks_types::identity::BackendProcessId] {
        self.eligible_backends()
    }

    fn bind_manifest(
        self,
        schedule: &AttemptSchedule,
    ) -> Result<Self::Manifest, NativeAttemptActivationFailure> {
        self.bind_manifest(schedule).map_err(|error| {
            NativeAttemptActivationFailure::new(attempt_runtime_failure(
                AttemptFailureClass::ContractViolation,
                QueryExecutionErrorKind::InvalidRequest,
                error.to_string(),
            ))
        })
    }
}

/// Fixed dormant owner which keeps topology authority in the adapter instead
/// of delegating it to a replaceable factory implementation.
struct SnapshotBoundDormantAttemptOwner<B, I = SnapshotBoundDormantAttemptInputs>
where
    I: RetainedAttemptInputs,
{
    dormant_inputs: Option<I>,
    manifest_inputs: Option<I::Manifest>,
    behavior: B,
}

impl<B, I> std::fmt::Debug for SnapshotBoundDormantAttemptOwner<B, I>
where
    B: std::fmt::Debug,
    I: RetainedAttemptInputs,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotBoundDormantAttemptOwner")
            .field("dormant_inputs", &self.dormant_inputs)
            .field("manifest_inputs", &self.manifest_inputs)
            .field("behavior", &self.behavior)
            .finish()
    }
}

impl<B, I> SnapshotBoundDormantAttemptOwner<B, I>
where
    I: RetainedAttemptInputs,
{
    fn new(inputs: I, behavior: B) -> Self {
        Self {
            dormant_inputs: Some(inputs),
            manifest_inputs: None,
            behavior,
        }
    }

    #[cfg(test)]
    fn from_manifest(inputs: I::Manifest, behavior: B) -> Self {
        Self {
            dormant_inputs: None,
            manifest_inputs: Some(inputs),
            behavior,
        }
    }

    fn retained_eligible_backends(&self) -> &[novarocks_types::identity::BackendProcessId] {
        self.dormant_inputs
            .as_ref()
            .expect("a dormant Native attempt retains inputs until its sole activation")
            .eligible_backends()
    }

    fn bind_manifest(
        &mut self,
        schedule: &AttemptSchedule,
    ) -> Result<(), NativeAttemptActivationFailure> {
        if self.manifest_inputs.is_some() {
            return Ok(());
        }
        let inputs = self
            .dormant_inputs
            .take()
            .expect("Query Application activates a dormant Native attempt only once");
        self.manifest_inputs = Some(inputs.bind_manifest(schedule)?);
        Ok(())
    }

    fn activate_fixed_retained<'a>(
        &'a mut self,
        replacement_admissions: Option<
            Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>,
        >,
        cancellation: CancellationView,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        (
                            SnapshotBoundActiveAttemptOwner<I::Manifest, B::ActiveBehavior>,
                            Option<(RootResultPumpBinding, AcceptedRootStatusSource)>,
                        ),
                        NativeAttemptActivationFailure,
                    >,
                > + Send
                + 'a,
        >,
    >
    where
        B: FrontendDormantAttemptBehavior<I::Manifest>,
    {
        let behavior = &mut self.behavior;
        let manifest_inputs = &mut self.manifest_inputs;
        Box::pin(async move {
            let mut active_behavior = behavior
                .activate(
                    manifest_inputs
                        .as_mut()
                        .expect("activation retains its manifest inputs"),
                    replacement_admissions,
                    cancellation,
                )
                .await?;
            let inputs = manifest_inputs
                .take()
                .expect("successful activation transfers the retained manifest once");
            let rows = active_behavior.take_rows_runtime();
            Ok((
                SnapshotBoundActiveAttemptOwner {
                    inputs,
                    behavior: active_behavior,
                },
                rows,
            ))
        })
    }

    fn activate_retained<'a>(
        &'a mut self,
        replacement_admissions: Option<
            Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>,
        >,
        cancellation: CancellationView,
    ) -> NativeAttemptActivationFuture<'a>
    where
        B: FrontendDormantAttemptBehavior<I::Manifest>,
    {
        let activation = self.activate_fixed_retained(replacement_admissions, cancellation);
        Box::pin(async move {
            let (owner, rows) = activation.await?;
            Ok(match rows {
                Some((binding, statuses)) => ActivatedNativeAttempt::rows(owner, binding, statuses),
                None => ActivatedNativeAttempt::completion(owner),
            })
        })
    }

    fn converge_retained<'a>(
        &'a mut self,
        cancellation: CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a>
    where
        B: FrontendDormantAttemptBehavior<I::Manifest>,
    {
        match self.manifest_inputs.as_mut() {
            Some(inputs) => self.behavior.converge(inputs, cancellation),
            None => Box::pin(async {}),
        }
    }
}

struct SnapshotBoundActiveAttemptOwner<M, B> {
    inputs: M,
    behavior: B,
}

impl<M, B> std::fmt::Debug for SnapshotBoundActiveAttemptOwner<M, B>
where
    M: std::fmt::Debug,
    B: std::fmt::Debug,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotBoundActiveAttemptOwner")
            .field("inputs", &self.inputs)
            .field("behavior", &self.behavior)
            .finish()
    }
}

impl<M, B> ActiveNativeAttemptOwner for SnapshotBoundActiveAttemptOwner<M, B>
where
    M: std::fmt::Debug + Send + 'static,
    B: FrontendActiveAttemptBehavior<M>,
{
    fn run<'a>(
        &'a mut self,
        drive: &'a NativeAttemptDrive,
        cancellation: CancellationView,
    ) -> NativeAttemptRunFuture<'a> {
        self.behavior.run(&mut self.inputs, drive, cancellation)
    }

    fn converge<'a>(
        &'a mut self,
        cancellation: CancellationView,
    ) -> NativeActiveAttemptConvergenceFuture<'a> {
        self.behavior.converge(&mut self.inputs, cancellation)
    }
}

impl<B> DormantNativeAttemptOwner for SnapshotBoundDormantAttemptOwner<B>
where
    B: FrontendDormantAttemptBehavior,
{
    fn eligible_backends(&self) -> &[novarocks_types::identity::BackendProcessId] {
        self.retained_eligible_backends()
    }

    fn activate<'a>(
        &'a mut self,
        schedule: &'a AttemptSchedule,
        replacement_admissions: Option<
            Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>,
        >,
        cancellation: CancellationView,
    ) -> NativeAttemptActivationFuture<'a> {
        if let Err(error) = self.bind_manifest(schedule) {
            return Box::pin(async move { Err(error) });
        }
        self.activate_retained(replacement_admissions, cancellation)
    }

    fn converge<'a>(
        &'a mut self,
        cancellation: CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a> {
        self.converge_retained(cancellation)
    }
}

/// Per-logical-execution Frontend adapter for Native attempt preparation.
///
/// Each call captures membership once, derives the Query Application placement
/// inputs from the owner that retains that capture, and returns a dormant owner
/// which must consume the same capture at activation. Recovery calls reuse the
/// immutable template while capturing a new snapshot for the new attempt.
pub(crate) struct FrontendNativeAttemptPreparationPort<F> {
    state: Arc<AsyncMutex<FrontendNativeAttemptPreparationState<F>>>,
}

struct FrontendNativeAttemptPreparationState<F> {
    template: PreparedDistributedAttemptTemplate,
    topology: BackendTopologyService,
    dormant_factory: F,
    last_topology_revision: Option<u64>,
}

impl<F> std::fmt::Debug for FrontendNativeAttemptPreparationPort<F>
where
    F: FrontendDormantAttemptFactory,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrontendNativeAttemptPreparationPort")
            .finish_non_exhaustive()
    }
}

impl<F> FrontendNativeAttemptPreparationPort<F>
where
    F: FrontendDormantAttemptFactory,
{
    pub(crate) fn new(
        template: PreparedDistributedAttemptTemplate,
        topology: BackendTopologyService,
        dormant_factory: F,
    ) -> Self {
        Self {
            state: Arc::new(AsyncMutex::new(FrontendNativeAttemptPreparationState {
                template,
                topology,
                dormant_factory,
                last_topology_revision: None,
            })),
        }
    }
}

impl<F> NativeAttemptPreparationPort for FrontendNativeAttemptPreparationPort<F>
where
    F: FrontendDormantAttemptFactory,
{
    fn prepare(
        &mut self,
        request: NativeAttemptPreparationRequest,
    ) -> NativeAttemptPreparationFuture {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let cancellation = request.cancellation();
            let mut state = state.lock().await;
            let snapshot = match state.last_topology_revision {
                None => state.topology.snapshot().map_err(|error| {
                    attempt_failure(
                        AttemptFailureClass::RecoverableInfrastructure,
                        QueryExecutionErrorKind::Failed,
                        format!("failed to capture Native attempt topology: {error}"),
                    )
                })?,
                Some(previous_revision) => {
                    let mut changes = state.topology.subscribe_changes();
                    loop {
                        if let Ok(snapshot) = state.topology.snapshot()
                            && snapshot.revision() > previous_revision
                        {
                            break snapshot;
                        }
                        tokio::select! {
                            reason = cancellation.cancelled() => {
                                return Err(attempt_failure(
                                    AttemptFailureClass::ExecutionFailure,
                                    QueryExecutionErrorKind::Cancelled,
                                    format!(
                                        "Native replacement topology wait was cancelled: {reason:?}"
                                    ),
                                ));
                            }
                            changed = changes.changed() => {
                                if changed.is_err() {
                                    return Err(attempt_failure(
                                        AttemptFailureClass::RecoverableInfrastructure,
                                        QueryExecutionErrorKind::Failed,
                                        "Native replacement topology observation closed",
                                    ));
                                }
                            }
                        }
                    }
                }
            };
            state.last_topology_revision = Some(snapshot.revision());
            state
                .dormant_factory
                .register_candidate(request.execution(), &snapshot)?;
            let scan_work = state.template.native_scan_work_facts().map_err(|error| {
                attempt_failure(
                    AttemptFailureClass::ContractViolation,
                    QueryExecutionErrorKind::InvalidRequest,
                    error.to_string(),
                )
            })?;
            let inputs =
                SnapshotBoundDormantAttemptInputs::capture(&state.template, &request, snapshot)
                    .map_err(|error| {
                        attempt_failure(
                            AttemptFailureClass::ContractViolation,
                            QueryExecutionErrorKind::InvalidRequest,
                            error.to_string(),
                        )
                    })?;
            let behavior = state.dormant_factory.create()?;
            let owner = SnapshotBoundDormantAttemptOwner::new(inputs, behavior);
            request
                .bind(scan_work, owner)
                .map_err(NativeAttemptPreparationError::from)
        })
    }
}

fn attempt_failure(
    class: AttemptFailureClass,
    kind: QueryExecutionErrorKind,
    message: impl Into<std::sync::Arc<str>>,
) -> NativeAttemptPreparationError {
    attempt_runtime_failure(class, kind, message).into()
}

fn attempt_runtime_failure(
    class: AttemptFailureClass,
    kind: QueryExecutionErrorKind,
    message: impl Into<std::sync::Arc<str>>,
) -> NativeAttemptPreparationFailure {
    NativeAttemptPreparationFailure::new(class, QueryExecutionError::new(kind, message))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use futures::FutureExt;
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_query_application::api::{
        ActiveNativeAttemptOwner, CancellationView, NativeActiveAttemptConvergenceFuture,
        NativeAttemptActivationFailure, NativeAttemptConvergence, NativeAttemptConvergenceFuture,
        NativeAttemptPreparationError, NativeAttemptRunFuture, NativeAttemptTerminal,
    };
    use novarocks_query_application::coordination::{AttemptSchedule, NativeAttemptDrive};
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };
    use novarocks_workload_control::{
        CancellationReason, ResourceConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };

    use super::{
        FrontendActiveAttemptBehavior, FrontendDormantAttemptBehavior,
        FrontendDormantAttemptFactory, RetainedAttemptInputs, SnapshotBoundDormantAttemptOwner,
        attempt_runtime_failure, failed_context_allows_successor,
        wait_for_failed_context_isolation,
    };
    use crate::common::backend_topology::{
        BackendProcessObservation, BackendProcessObservationPort, BackendProcessObservationService,
        BackendTopologyError,
    };
    use crate::query_execution::artifact::ManifestBoundNativeAttemptInputs;

    #[derive(Debug)]
    struct CapturedInputs {
        eligible_backends: Vec<BackendProcessId>,
    }

    #[test]
    fn readonly_replacement_waits_only_for_a_reachable_current_worker_to_close() {
        let replacement = BackendProcessId::new_v7();
        assert!(!failed_context_allows_successor(
            &Ok(BackendProcessObservation::Current),
            false,
        ));
        assert!(failed_context_allows_successor(
            &Ok(BackendProcessObservation::Current),
            true,
        ));
        assert!(failed_context_allows_successor(
            &Ok(BackendProcessObservation::Unobservable),
            false,
        ));
        assert!(failed_context_allows_successor(
            &Ok(BackendProcessObservation::Replaced {
                current_process: replacement,
            }),
            false,
        ));
        assert!(failed_context_allows_successor(
            &Err(BackendTopologyError::Unavailable {
                message: "transient observation failure".to_string(),
            }),
            false,
        ));
    }

    #[derive(Debug)]
    struct TestProcessObservation {
        observation: std::sync::Mutex<BackendProcessObservation>,
        epoch: tokio::sync::watch::Sender<u64>,
    }

    impl BackendProcessObservationPort for TestProcessObservation {
        fn subscribe_process_changes(&self) -> tokio::sync::watch::Receiver<u64> {
            self.epoch.subscribe()
        }

        fn observe_process_at_endpoint(
            &self,
            _expected_process: BackendProcessId,
            _expected_endpoint: &RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, BackendTopologyError> {
            Ok(*self.observation.lock().expect("test observation"))
        }
    }

    #[tokio::test]
    async fn current_worker_waits_for_abort_closure_under_the_same_deadline() {
        let process = BackendProcessId::new_v7();
        let context = novarocks_execution_contract::QueryContextRef::new(
            QueryExecutionId::new(
                QueryId::new(0x1234, 0x5678),
                AttemptId::new(1).expect("attempt one"),
            )
            .expect("test execution identity"),
            FrontendProcessId::new_v7(),
            process,
        );
        let endpoint = RuntimeEndpoint::new("127.0.0.1", 19040).unwrap();
        let (process_epoch, _) = tokio::sync::watch::channel(0);
        let observation: BackendProcessObservationService = Arc::new(TestProcessObservation {
            observation: std::sync::Mutex::new(BackendProcessObservation::Current),
            epoch: process_epoch,
        });
        let closed = Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::new()));
        let (closure_epoch, _) = tokio::sync::watch::channel(0);
        let mut closure_changes = closure_epoch.subscribe();
        let mut process_changes = observation.subscribe_process_changes();
        let (_cancel, mut cancellation) = tokio::sync::watch::channel(false);
        let wait = wait_for_failed_context_isolation(
            &observation,
            context,
            &endpoint,
            &closed,
            &mut closure_changes,
            &mut process_changes,
            &mut cancellation,
            Instant::now() + Duration::from_secs(1),
        );
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), &mut wait)
                .await
                .is_err()
        );

        closed.lock().expect("closed contexts").insert(context);
        closure_epoch.send_modify(|epoch| *epoch += 1);
        tokio::time::timeout(Duration::from_millis(100), &mut wait)
            .await
            .expect("Abort closure wakes replacement qualification")
            .expect("closed current Worker permits the successor");
    }

    #[tokio::test]
    async fn current_worker_wait_honors_cancellation_and_absolute_expiry() {
        let process = BackendProcessId::new_v7();
        let context = novarocks_execution_contract::QueryContextRef::new(
            QueryExecutionId::new(
                QueryId::new(0x2345, 0x6789),
                AttemptId::new(1).expect("attempt one"),
            )
            .expect("test execution identity"),
            FrontendProcessId::new_v7(),
            process,
        );
        let endpoint = RuntimeEndpoint::new("127.0.0.1", 19041).unwrap();
        let (process_epoch, _) = tokio::sync::watch::channel(0);
        let observation: BackendProcessObservationService = Arc::new(TestProcessObservation {
            observation: std::sync::Mutex::new(BackendProcessObservation::Current),
            epoch: process_epoch,
        });
        let closed = Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::new()));
        let (closure_epoch, _) = tokio::sync::watch::channel(0);

        let mut closure_changes = closure_epoch.subscribe();
        let mut process_changes = observation.subscribe_process_changes();
        let (_cancel_owner, mut cancelled) = tokio::sync::watch::channel(true);
        assert!(matches!(
            wait_for_failed_context_isolation(
                &observation,
                context,
                &endpoint,
                &closed,
                &mut closure_changes,
                &mut process_changes,
                &mut cancelled,
                Instant::now() + Duration::from_secs(1),
            )
            .await,
            Err(novarocks_query_application::coordination::ReplacementQualificationFailure::Rejected)
        ));

        let mut closure_changes = closure_epoch.subscribe();
        let mut process_changes = observation.subscribe_process_changes();
        let (_cancel_owner, mut active) = tokio::sync::watch::channel(false);
        assert!(matches!(
            wait_for_failed_context_isolation(
                &observation,
                context,
                &endpoint,
                &closed,
                &mut closure_changes,
                &mut process_changes,
                &mut active,
                Instant::now() + Duration::from_millis(1),
            )
            .await,
            Err(
                novarocks_query_application::coordination::ReplacementQualificationFailure::Expired
            )
        ));
    }

    impl RetainedAttemptInputs for CapturedInputs {
        type Manifest = TestManifest;

        fn eligible_backends(&self) -> &[BackendProcessId] {
            &self.eligible_backends
        }

        fn bind_manifest(
            self,
            _schedule: &AttemptSchedule,
        ) -> Result<Self::Manifest, NativeAttemptActivationFailure> {
            Ok(TestManifest {
                marker: 73,
                identity: Box::new(91),
            })
        }
    }

    #[derive(Debug)]
    struct AdversarialBehavior {
        replacement_backends: Vec<BackendProcessId>,
    }

    #[derive(Debug)]
    struct NeverActiveBehavior;

    impl FrontendActiveAttemptBehavior for NeverActiveBehavior {
        fn run<'a>(
            &'a mut self,
            _inputs: &'a mut ManifestBoundNativeAttemptInputs,
            _drive: &'a NativeAttemptDrive,
            _cancellation: CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { panic!("adversarial eligibility test must not run") })
        }

        fn converge<'a>(
            &'a mut self,
            _inputs: &'a mut ManifestBoundNativeAttemptInputs,
            _cancellation: CancellationView,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async { NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced() })
        }
    }

    impl FrontendDormantAttemptBehavior for AdversarialBehavior {
        type ActiveBehavior = NeverActiveBehavior;

        fn activate<'a>(
            &'a mut self,
            _inputs: &'a mut ManifestBoundNativeAttemptInputs,
            _replacement_admissions: Option<Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: CancellationView,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<Self::ActiveBehavior, NativeAttemptActivationFailure>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async { panic!("adversarial eligibility test must not activate") })
        }

        fn converge<'a>(
            &'a mut self,
            _inputs: &'a mut ManifestBoundNativeAttemptInputs,
            _cancellation: CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async {})
        }
    }

    #[derive(Debug)]
    struct AdversarialFactory {
        replacement_backend: BackendProcessId,
    }

    impl FrontendDormantAttemptFactory for AdversarialFactory {
        type Behavior = AdversarialBehavior;

        fn create(&mut self) -> Result<Self::Behavior, NativeAttemptPreparationError> {
            Ok(AdversarialBehavior {
                replacement_backends: vec![self.replacement_backend],
            })
        }
    }

    #[test]
    fn adversarial_factory_cannot_replace_snapshot_derived_eligible_backends() {
        let captured = BackendProcessId::new_v7();
        let replacement = BackendProcessId::new_v7();
        let mut factory = AdversarialFactory {
            replacement_backend: replacement,
        };
        let owner = SnapshotBoundDormantAttemptOwner::new(
            CapturedInputs {
                eligible_backends: vec![captured],
            },
            factory.create().expect("adversarial behavior"),
        );

        assert_eq!(owner.retained_eligible_backends(), &[captured]);
        assert_eq!(owner.behavior.replacement_backends, vec![replacement]);
    }

    #[derive(Debug, Eq, PartialEq)]
    struct TestManifest {
        marker: u8,
        identity: Box<u8>,
    }

    #[derive(Clone, Copy, Debug)]
    enum ActivationMode {
        Pending,
        Error,
        Panic,
        Success,
    }

    #[derive(Debug)]
    struct LifecycleBehavior {
        mode: ActivationMode,
        convergence_observed: Arc<AtomicBool>,
    }

    #[derive(Debug)]
    struct TestActiveBehavior {
        identity_address: usize,
        run_observed: Arc<AtomicBool>,
        convergence_observed: Arc<AtomicBool>,
    }

    impl TestActiveBehavior {
        fn observe_run_inputs(&self, inputs: &mut TestManifest) {
            assert_eq!(inputs.marker, 73);
            assert_eq!(
                inputs.identity.as_ref() as *const u8 as usize,
                self.identity_address
            );
            self.run_observed.store(true, Ordering::SeqCst);
        }
    }

    impl FrontendActiveAttemptBehavior<TestManifest> for TestActiveBehavior {
        fn run<'a>(
            &'a mut self,
            inputs: &'a mut TestManifest,
            _drive: &'a NativeAttemptDrive,
            _cancellation: CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            self.observe_run_inputs(inputs);
            Box::pin(async { NativeAttemptTerminal::Completed })
        }

        fn converge<'a>(
            &'a mut self,
            inputs: &'a mut TestManifest,
            _cancellation: CancellationView,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            assert_eq!(inputs.marker, 73);
            assert_eq!(
                inputs.identity.as_ref() as *const u8 as usize,
                self.identity_address
            );
            let observed = Arc::clone(&self.convergence_observed);
            Box::pin(async move {
                observed.store(true, Ordering::SeqCst);
                NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced()
            })
        }
    }

    impl FrontendDormantAttemptBehavior<TestManifest> for LifecycleBehavior {
        type ActiveBehavior = TestActiveBehavior;

        fn activate<'a>(
            &'a mut self,
            inputs: &'a mut TestManifest,
            _replacement_admissions: Option<Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: CancellationView,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<Self::ActiveBehavior, NativeAttemptActivationFailure>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                assert_eq!(inputs.marker, 73);
                match self.mode {
                    ActivationMode::Pending => std::future::pending().await,
                    ActivationMode::Error => Err(NativeAttemptActivationFailure::new(
                        attempt_runtime_failure(
                            super::AttemptFailureClass::RecoverableInfrastructure,
                            super::QueryExecutionErrorKind::Failed,
                            "activation failed",
                        ),
                    )),
                    ActivationMode::Panic => panic!("activation poll panic"),
                    ActivationMode::Success => Ok(TestActiveBehavior {
                        identity_address: inputs.identity.as_ref() as *const u8 as usize,
                        run_observed: Arc::new(AtomicBool::new(false)),
                        convergence_observed: Arc::clone(&self.convergence_observed),
                    }),
                }
            })
        }

        fn converge<'a>(
            &'a mut self,
            inputs: &'a mut TestManifest,
            _cancellation: CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            assert_eq!(inputs.marker, 73);
            let observed = Arc::clone(&self.convergence_observed);
            Box::pin(async move {
                observed.store(true, Ordering::SeqCst);
            })
        }
    }

    fn governed_cancellation() -> (WorkloadControl, RootWork, CancellationView) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 10,
                per_scope_bytes: 1 << 18,
            },
        )
        .expect("valid workload control");
        control.mark_ready().expect("workload control ready");
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("query work admitted");
        let cancellation = root
            .owner
            .scope()
            .cancellation()
            .expect("active work cancellation view");
        (control, root, cancellation)
    }

    fn lifecycle_owner(
        mode: ActivationMode,
    ) -> (
        SnapshotBoundDormantAttemptOwner<LifecycleBehavior, CapturedInputs>,
        Arc<AtomicBool>,
    ) {
        let convergence_observed = Arc::new(AtomicBool::new(false));
        (
            SnapshotBoundDormantAttemptOwner::from_manifest(
                TestManifest {
                    marker: 73,
                    identity: Box::new(91),
                },
                LifecycleBehavior {
                    mode,
                    convergence_observed: Arc::clone(&convergence_observed),
                },
            ),
            convergence_observed,
        )
    }

    async fn assert_retained_and_converges(
        owner: &mut SnapshotBoundDormantAttemptOwner<LifecycleBehavior, CapturedInputs>,
        convergence_observed: &AtomicBool,
        cancellation: CancellationView,
    ) {
        assert_eq!(owner.manifest_inputs.as_ref().unwrap().marker, 73);
        owner.converge_retained(cancellation).await;
        assert!(convergence_observed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancelled_activation_future_retains_manifest_for_convergence() {
        let (_control, root, cancellation) = governed_cancellation();
        let (mut owner, convergence_observed) = lifecycle_owner(ActivationMode::Pending);
        let mut activation = owner.activate_retained(None, cancellation.clone());
        assert!(
            tokio::time::timeout(Duration::from_millis(1), &mut activation)
                .await
                .is_err()
        );
        root.owner.cancel(CancellationReason::ClientDisconnected);
        drop(activation);

        assert_retained_and_converges(&mut owner, &convergence_observed, cancellation).await;
    }

    #[tokio::test]
    async fn activation_error_retains_manifest_for_convergence() {
        let (_control, _root, cancellation) = governed_cancellation();
        let (mut owner, convergence_observed) = lifecycle_owner(ActivationMode::Error);
        assert!(
            owner
                .activate_retained(None, cancellation.clone())
                .await
                .is_err()
        );

        assert_retained_and_converges(&mut owner, &convergence_observed, cancellation).await;
    }

    #[tokio::test]
    async fn activation_poll_panic_retains_manifest_for_convergence() {
        let (_control, _root, cancellation) = governed_cancellation();
        let (mut owner, convergence_observed) = lifecycle_owner(ActivationMode::Panic);
        let outcome =
            std::panic::AssertUnwindSafe(owner.activate_retained(None, cancellation.clone()))
                .catch_unwind()
                .await;
        assert!(outcome.is_err());

        assert_retained_and_converges(&mut owner, &convergence_observed, cancellation).await;
    }

    #[tokio::test]
    async fn successful_activation_transfers_one_manifest_to_the_fixed_active_owner() {
        let (_control, _root, cancellation) = governed_cancellation();
        let (mut dormant, convergence_observed) = lifecycle_owner(ActivationMode::Success);
        let identity_address =
            dormant.manifest_inputs.as_ref().unwrap().identity.as_ref() as *const u8 as usize;
        let (mut active, rows) = dormant
            .activate_fixed_retained(None, cancellation.clone())
            .await
            .expect("activation succeeds");

        assert!(dormant.manifest_inputs.is_none());
        assert!(rows.is_none());
        assert_eq!(active.inputs.marker, 73);
        assert_eq!(
            active.inputs.identity.as_ref() as *const u8 as usize,
            identity_address
        );
        active.behavior.observe_run_inputs(&mut active.inputs);
        assert!(active.behavior.run_observed.load(Ordering::SeqCst));
        ActiveNativeAttemptOwner::converge(&mut active, cancellation).await;
        assert!(convergence_observed.load(Ordering::SeqCst));
    }
}
