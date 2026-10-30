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
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

mod control_binding;
pub(crate) mod source_cohort;
use control_binding::NativeControlBinding;
use source_cohort::NativeSourceCohortAdmission;
pub(crate) use source_cohort::source_count as native_source_count;

use crate::task_protocol_config::CoordinationBudgets;
#[cfg(test)]
use novarocks_coordination_contract::AttemptFailureClass;
use novarocks_coordination_contract::RecoveryMode;
use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, LeaseValidFor, OperationOutcome, QueryContextRef,
    TaskOperationId,
};
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
use novarocks_proto_codec::lifecycle::QueryOptions as ProtocolQueryOptions;
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
#[cfg(test)]
use novarocks_query_application::api::QueryExecutionErrorKind;
use novarocks_query_application::api::{
    ActivatedNativeAttempt, ActiveNativeAttemptOwner, CancellationView, DormantNativeAttemptOwner,
    LogicalExecutionNativePort, LogicalNativeOpenRequest, NativeActiveAttemptConvergenceFuture,
    NativeActiveConvergenceClosure, NativeActiveConvergenceRequest, NativeAttemptActivationFailure,
    NativeAttemptActivationFuture, NativeAttemptConvergenceFuture, NativeAttemptPreparationError,
    NativeAttemptPreparationFailure, NativeAttemptPreparationFuture, NativeAttemptPreparationPort,
    NativeAttemptPreparationRequest, NativeAttemptPreparationWork, NativeAttemptRunFuture,
    NativeAttemptTerminal, QueryExecutionClient, QueryExecutionError, QueryExecutionRequest,
};
use novarocks_query_application::coordination::{
    AbortQueryContextEffectAdmission, AbortQueryContextEffectPort, AbortQueryContextIssueIdentity,
    AcceptedRootStatusSource, ActiveAttemptIsolationOwner, AttemptIsolationActivationFailure,
    AttemptIsolationReservation, AttemptSchedule, NativeAttemptDrive,
    QualifiedReplacementReservation, QualifiedWorkerAdmission,
    ReplacementQualificationEffectAdmission, ReplacementQualificationEffectPort,
    ReplacementQualificationEffectReservation, ReplacementQualificationEffectSubmission,
    ReplacementQualificationFailure, ReplacementQualificationIdentity,
    ReplacementQualificationRequest, ReplacementWorkerAdmissionEvidence, RootRelayBinding,
    RootRelayFrontier,
};
use novarocks_query_application::execution_error::{
    ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
};
use novarocks_task_codec::TransportBudget;
use novarocks_types::NativeCompatibilityId;
use novarocks_workload_control::WorkOwner;
use tokio::sync::watch;

use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::fragment_encoder::instance::encode_query_options;
use crate::native::fragment_encoder::submission::encode_native_submission;
use crate::native::fragment_transport::{
    NativeBoundedRootReadPort, NativeTaskResultTransport, TaskReadGrace,
};
use crate::native::task_transport::{
    AttemptWireFacts, NativeTaskOperationSink, TaskAckIntake, TaskOperationIntakeEvent,
};
use crate::query_execution::artifact::{
    ManifestBoundNativeAttemptInputs, PreparedDistributedAttemptTemplate, PreparedDistributedQuery,
    SnapshotBoundDormantAttemptInputs, TaskExecutionPreparedQuery, TaskManifestBinding,
};
use crate::query_execution::attempt_initialization::{
    AttemptInitializing, AttemptReady, RoundCredentialLeaseSource,
};
use crate::query_execution::completion::PreparedLogicalRead;
use crate::query_execution::contract::{DistributedQueryError, ResolvedQueryOptions};
use crate::query_execution::internal_consumer::attempt_handoff::{
    AttemptHandoffProducer, AttemptHandoffReserveView, AttemptHandoffResources,
};
use crate::query_execution::lifecycle_plan::QueryInitOptions;
use crate::query_execution::logical_execution::{
    ComputationDiagnostics, InitialComputationCredentials, LogicalExecutionLauncher,
    PreparedLogicalComputation, validate_initial_computation_start,
};
use crate::query_execution::split_assignment_round::SplitAssignmentRoundGuard;
use crate::runtime_filter::compiler::{
    FrontendRuntimeFilterDeploymentCompilerConfig, compile_scheduled_runtime_filter_deployment,
};
use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
use crate::task_execution::abort_effect::{NativeAbortEffectAdapter, NativeAbortEffectIntake};
use crate::task_execution::attempt_pumps::{AttemptPumps, install_attempt_pumps};
use crate::task_execution::feedback_pump::TaskDynamicFilterReads;
use crate::task_execution::intent::{
    AckPayload, DispatchBatch, OperationIntent, TaskOperationQueueAdmission, TaskOperationSink,
    TaskOperationSubmit,
};
use crate::task_execution::manifest_round::{ManifestAssembledRound, ManifestAttemptCompletion};
use crate::task_execution::sources::AttemptEstablishFacts;
use crate::task_execution::status_intake::{NotifyWake, StatusIntakeWake};
use crate::task_protocol_config::TaskUpdateRetryPolicy;
use novarocks_query_application::api::{BackendProcessObservationService, BackendTopologyService};

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
                    QueryFailureReason::PreparationProtocol,
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
        novarocks_query_application::api::LiveBackendTarget,
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
        snapshot: &novarocks_query_application::api::BackendTopologySnapshot,
    ) -> Result<(), NativeAttemptPreparationError> {
        let mut targets = BTreeMap::new();
        for target in snapshot.targets() {
            let process = target.process_id().map_err(|error| {
                attempt_failure(QueryFailureReason::PreparationProtocol, error.to_string())
            })?;
            if targets.insert(process, target.clone()).is_some() {
                return Err(attempt_failure(
                    QueryFailureReason::PreparationProtocol,
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
                Err(error) => submission.refused(error),
            },
            Err(error) => {
                submission.refused(error);
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
                tracing::warn!(?context, "replacement failed-context isolation expired");
                return Err(ReplacementQualificationFailure::Expired);
            }
        }
    }
}

fn failed_context_allows_successor(
    observation: &Result<
        novarocks_query_application::api::BackendProcessObservation,
        novarocks_query_application::api::BackendTopologyError,
    >,
    worker_closed: bool,
) -> bool {
    match observation {
        Ok(novarocks_query_application::api::BackendProcessObservation::Current) => worker_closed,
        Ok(
            novarocks_query_application::api::BackendProcessObservation::Unobservable
            | novarocks_query_application::api::BackendProcessObservation::Replaced { .. },
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
        if target.descriptor().process_id() != context.backend_process_id() {
            return Err(ReplacementQualificationFailure::InvalidReservation);
        }
        backends.push(target.descriptor().clone());
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
        qualification
            .native_action_authority()
            .map_err(|_| ReplacementQualificationFailure::InvalidReservation)?
            .clone(),
    )
    .map_err(|error| ReplacementQualificationFailure::DomainRefused(error.failure_fact()))?;
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
            let mut retry_delay = runtime.task_update_retry_policy.initial_backoff();
            // These are fields of the same original qualification future,
            // already held by its stable QA owner. Local backpressure never
            // recreates this request, paid ticket, or queue source.
            let mut queue_preparation =
                crate::task_execution::intent::TaskOperationQueuePreparation::new();
            let mut pending_batch = None;
            loop {
                if *cancellation.borrow() {
                    return Err(ReplacementQualificationFailure::Rejected);
                }
                let notified = notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if pending_batch.is_none() {
                    let permit = match sink
                        .try_reserve_queue(intent.queue_request(), &mut queue_preparation)
                    {
                        TaskOperationQueueAdmission::Admitted(permit) => Some(permit),
                        TaskOperationQueueAdmission::TargetFull
                        | TaskOperationQueueAdmission::ProcessFull => None,
                        TaskOperationQueueAdmission::Rejected(error) => {
                            return Err(ReplacementQualificationFailure::DomainRefused(
                                error.failure_fact(),
                            ));
                        }
                    };
                    if let Some(permit) = permit {
                        pending_batch = Some(DispatchBatch::with_queued_at(
                            context.backend_process_id(),
                            crate::task_protocol_config::DispatchLane::Lifecycle,
                            vec![intent.clone()],
                            vec![permit],
                            intent.queued_bytes(),
                            qualification.issued_at(),
                        ));
                    } else {
                        tokio::select! {_=&mut notified=>{},_ = cancellation.changed()=>return Err(ReplacementQualificationFailure::Rejected)}
                        continue;
                    }
                }
                let batch = pending_batch
                    .take()
                    .expect("same original replacement batch");
                match sink.try_submit(batch) {
                    TaskOperationSubmit::Accepted => {}
                    TaskOperationSubmit::Backpressured(batch) => {
                        pending_batch = Some(batch);
                        tokio::select! {_=&mut notified=>{},_ = cancellation.changed()=>return Err(ReplacementQualificationFailure::Rejected)}
                        continue;
                    }
                    TaskOperationSubmit::AdmissionFailed { error, .. } => {
                        return Err(ReplacementQualificationFailure::DomainRefused(
                            error.failure_fact(),
                        ));
                    }
                    TaskOperationSubmit::Rejected { .. } => {
                        return Err(ReplacementQualificationFailure::Rejected);
                    }
                }
                let acknowledgement = wait_for_replacement_acknowledgement(
                    &acks,
                    &notify,
                    notified.as_mut(),
                    &mut cancellation,
                    request.envelope().operation_id(),
                )
                .await?;
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
                    (None, _) => {
                        // Replay the exact admission without turning immediate
                        // transport refusal into an unbounded RPC spin. The
                        // qualification's outer expiry still bounds this wait.
                        tokio::select! {
                            _ = tokio::time::sleep(retry_delay) => {},
                            _ = cancellation.changed() => {
                                return Err(ReplacementQualificationFailure::Rejected);
                            }
                        }
                        retry_delay = retry_delay
                            .saturating_mul(2)
                            .min(runtime.task_update_retry_policy.max_backoff());
                        continue;
                    }
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
    .map_err(|_| {
        tracing::warn!(identity = ?qualification.identity(), "replacement admission acquisition expired");
        ReplacementQualificationFailure::Expired
    })?
}

/// Reuse the qualification's original registered waiter. A second waiter
/// would allow notify_one to wake the older dormant registration while this
/// consumer sleeps with its real acknowledgement already in the intake.
async fn wait_for_replacement_acknowledgement<'a>(
    acks: &TaskAckIntake,
    notify: &'a tokio::sync::Notify,
    mut notified: std::pin::Pin<&mut tokio::sync::futures::Notified<'a>>,
    cancellation: &mut watch::Receiver<bool>,
    operation: TaskOperationId,
) -> Result<crate::task_execution::intent::OperationAcknowledgement, ReplacementQualificationFailure>
{
    loop {
        // Retire the same previous registration, then register before reading
        // the level-triggered intake. Both early and racing ACKs are retained.
        notified.as_mut().set(notify.notified());
        notified.as_mut().enable();
        if *cancellation.borrow() {
            return Err(ReplacementQualificationFailure::Rejected);
        }
        if let Some(ack) = acks
            .drain_events()
            .into_iter()
            .filter_map(|event| match event {
                TaskOperationIntakeEvent::Acknowledgement(ack) => Some(*ack),
                TaskOperationIntakeEvent::EstablishSendStarted { .. } => None,
            })
            .find(|ack| ack.operation_id() == operation)
        {
            return Ok(ack);
        }
        tokio::select! {
            _ = &mut notified => {},
            _ = cancellation.changed() => {
                return Err(ReplacementQualificationFailure::Rejected);
            }
        }
    }
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
    fn bind_in(
        &self,
        source: &mut Option<LogicalNativeOpenRequest>,
        target: &mut Option<novarocks_query_application::api::LogicalNativeSession>,
    ) -> Result<(), novarocks_query_application::api::LogicalNativeOpenError> {
        LogicalNativeOpenRequest::bind_in(source, target).map_err(Into::into)
    }
}

/// Explicit process capabilities used to instantiate one logical Native read.
/// Every field is fixed by server composition; an attempt may only project a
/// manifest through these handles and cannot reacquire policy from globals.
#[derive(Clone)]
pub(crate) struct FrontendNativeLogicalExecutionRuntime {
    #[cfg(novarocks_dev_controls)]
    statement_observation:
        Option<std::sync::Weak<crate::preparation_diagnostics::StatementObservationOwner>>,
    clock: Arc<dyn novarocks_coordination_contract::MonotonicClock>,
    #[cfg(novarocks_dev_controls)]
    test_controls: Option<Arc<novarocks_native_adapter::test_controls::NativeTestControls>>,
    #[cfg(novarocks_dev_controls)]
    observations:
        Option<Arc<novarocks_native_adapter::test_observation_controls::NativeObservationControls>>,
    topology: BackendTopologyService,
    process_observation: BackendProcessObservationService,
    data_runtime: FrontendDataRuntime,
    native_compatibility_id: NativeCompatibilityId,
    runtime_filter_worker_count: NonZeroUsize,
    task_update_retry_policy: TaskUpdateRetryPolicy,
    split_initial_wait_cap: Duration,
    coordination_budgets: CoordinationBudgets,
    transport_budget: TransportBudget,
    abort_capacity: NonZeroUsize,
    lifecycle_diagnostics:
        Arc<crate::query_execution::lifecycle_diagnostics::FrontendLifecycleDiagnostics>,
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
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn with_test_controls(
        mut self,
        controls: Arc<novarocks_native_adapter::test_controls::NativeTestControls>,
        observations: Option<
            Arc<novarocks_native_adapter::test_observation_controls::NativeObservationControls>,
        >,
        statement_observation: Option<
            std::sync::Weak<crate::preparation_diagnostics::StatementObservationOwner>,
        >,
    ) -> Self {
        self.statement_observation = statement_observation;
        self.test_controls = Some(controls);
        self.observations = observations;
        self
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "server composition supplies each independent Native runtime policy"
    )]
    pub(crate) fn new(
        topology: BackendTopologyService,
        process_observation: BackendProcessObservationService,
        data_runtime: FrontendDataRuntime,
        native_compatibility_id: NativeCompatibilityId,
        runtime_filter_worker_count: NonZeroUsize,
        task_update_retry_policy: TaskUpdateRetryPolicy,
        split_initial_wait_cap: Duration,
        coordination_budgets: CoordinationBudgets,
        transport_budget: TransportBudget,
        abort_capacity: NonZeroUsize,
        lifecycle_diagnostics: Arc<
            crate::query_execution::lifecycle_diagnostics::FrontendLifecycleDiagnostics,
        >,
        clock: Arc<dyn novarocks_coordination_contract::MonotonicClock>,
    ) -> Self {
        Self {
            clock,
            #[cfg(novarocks_dev_controls)]
            statement_observation: None,
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
            #[cfg(novarocks_dev_controls)]
            observations: None,
            topology,
            process_observation,
            data_runtime,
            native_compatibility_id,
            runtime_filter_worker_count,
            task_update_retry_policy,
            split_initial_wait_cap,
            coordination_budgets,
            transport_budget,
            abort_capacity,
            lifecycle_diagnostics,
        }
    }
}

#[derive(Clone)]
pub(crate) struct FrontendNativeLogicalExecutionLauncher {
    client: QueryExecutionClient,
    runtime: FrontendNativeLogicalExecutionRuntime,
}

impl FrontendNativeLogicalExecutionLauncher {
    pub(crate) const fn new(
        client: QueryExecutionClient,
        runtime: FrontendNativeLogicalExecutionRuntime,
    ) -> Self {
        Self { client, runtime }
    }
}

impl LogicalExecutionLauncher for FrontendNativeLogicalExecutionLauncher {
    #[cfg(novarocks_dev_controls)]
    fn resolve_statement_observation(
        &self,
        nomination: &novarocks_failpoint::target_binding::StatementWorkNomination,
    ) -> Option<crate::preparation_diagnostics::StatementObservationHandle> {
        let owner = self
            .runtime
            .statement_observation
            .as_ref()
            .and_then(std::sync::Weak::upgrade)?;
        owner.observation_for_nomination(nomination)
    }

    fn start_read_in(
        &self,
        read: &mut Option<PreparedLogicalRead>,
        owner: &mut Option<WorkOwner>,
        capacity: &QueryResultCapacityBinding,
        io: &Arc<crate::query::read_action::io_source::ReadMetadataIoSource>,
        original: &mut crate::query_execution::logical_execution::OriginalComputationStart,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Result<novarocks_query_application::api::LogicalExecutionStartAttempt, QueryExecutionError>
    {
        let refused = |detail: &'static str| {
            QueryExecutionError::from_execution(ExecutionFailureInput::new(
                QueryFailureReason::PreparationProtocol.failure_fact(),
                SafeDetail::truncating(detail),
                ExecutionFailureSite::Coordinator,
            ))
        };
        if original.ready.is_some() || original.exit.is_some() || original.reservation.is_some() {
            return Err(refused("ordinary Read start target is occupied"));
        }
        let work = owner
            .as_ref()
            .ok_or_else(|| refused("ordinary Read stage owner is absent"))?;
        if !capacity.window_alias().is_for_scope(&work.scope()) {
            return Err(refused("ordinary Read window has a foreign stage"));
        }
        if original.input.is_none() && original.request.is_none() {
            let prepared = read
                .as_ref()
                .ok_or_else(|| refused("prepared Read is absent"))?;
            prepared.validate_read_start().map_err(refused)?;
            let carrier = prepared.row_carrier().map_err(|e| {
                attempt_runtime_failure(QueryFailureReason::PreparationProtocol, e)
                    .error()
                    .clone()
            })?;
            drop(ProductionRootDelivery::bind(
                carrier,
                Some(capacity.window_alias()),
            )?);
            let (description, template, options) = read.take().unwrap().into_parts();
            let aborts = LogicalAbortRouter::new();
            let replacements = LogicalReplacementQualificationPort::new(
                self.runtime.clone(),
                aborts.worker_closed_contexts(),
                aborts.closure_epoch(),
            );
            let factory = ProductionDormantAttemptFactory {
                metadata_io: OriginalAttemptMetadataSource::Read(Arc::downgrade(io)),
                runtime: self.runtime.clone(),
                options,
                aborts: Arc::clone(&aborts),
                diagnostics: ComputationDiagnostics::None,
                initial_credentials: None,
                replacements: replacements.clone(),
                handoffs: None,
                pending_projection: None,
                pending_handoff: None,
            };
            let attempts = FrontendNativeAttemptPreparationPort::new(
                template,
                Arc::clone(&self.runtime.topology),
                factory,
                #[cfg(novarocks_dev_controls)]
                observation,
                #[cfg(novarocks_dev_controls)]
                self.runtime.test_controls.clone(),
                #[cfg(novarocks_dev_controls)]
                self.runtime.observations.clone(),
            );
            original.request = Some(
                QueryExecutionRequest::bind_native(
                    description,
                    aborts as Arc<dyn AbortQueryContextEffectPort>,
                    Some(Arc::new(replacements) as Arc<dyn ReplacementQualificationEffectPort>),
                    attempts,
                )
                .with_result_capacity(capacity.clone())?,
            );
        }
        if original.input.is_none() {
            let request = original
                .request
                .take()
                .ok_or_else(|| refused("ordinary Read request is absent"))?;
            match self.client.prepare_observed_start(request, None) {
                Ok(input) => original.input = Some(input),
                Err((error, request, reserved)) => {
                    original.request = Some(request);
                    original.reservation = reserved;
                    return Err(error);
                }
            }
        }
        self.client.start_observed_in(
            &mut original.input,
            owner,
            &mut original.ready,
            &mut original.exit,
        )
    }

    fn start_computation_in(
        &self,
        computation: &mut Option<PreparedLogicalComputation>,
        owner: &mut Option<WorkOwner>,
        capacity: &QueryResultCapacityBinding,
        io: &Arc<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>,
        original: &mut crate::query_execution::logical_execution::OriginalComputationStart,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Result<novarocks_query_application::api::LogicalExecutionStartAttempt, QueryExecutionError>
    {
        let refused = |detail: &'static str| {
            QueryExecutionError::from_execution(ExecutionFailureInput::new(
                QueryFailureReason::PreparationProtocol.failure_fact(),
                SafeDetail::truncating(detail),
                ExecutionFailureSite::Coordinator,
            ))
        };
        if original.ready.is_some() || original.exit.is_some() {
            return Err(refused("computation start target is occupied"));
        }
        let work = owner
            .as_ref()
            .ok_or_else(|| refused("computation start owner is absent"))?;
        if !capacity.window_alias().is_for_scope(&work.scope()) {
            return Err(refused(
                "logical execution result window belongs to a foreign scope",
            ));
        }
        // Borrow the SAME positive metadata Root before moving prepared input,
        // credentials, reservation or owner. The existing stage author checks
        // the real authority, active direct child and retained original grant.
        io.root_stage_source(work).map(drop).map_err(|error| {
            QueryExecutionError::from_execution(ExecutionFailureInput::new(
                error.failure_fact(),
                SafeDetail::truncating_display(format_args!(
                    "computation metadata source refused: {error}"
                )),
                ExecutionFailureSite::Coordinator,
            ))
        })?;
        if original.input.is_none() && original.request.is_none() {
            let prepared = computation
                .as_ref()
                .ok_or_else(|| refused("prepared computation is absent"))?;
            if prepared.description.finish_boundary()
                != novarocks_query_application::preparation::ExecutionFinishBoundary::ComputationReady
            {
                return Err(refused("observed computation has a foreign finish boundary"));
            }
            // Validate both original inputs before moving caller-owned custody.
            validate_initial_computation_start(
                &prepared.description,
                prepared.diagnostics,
                prepared.initial_credentials.as_ref(),
                original.reservation.as_ref(),
            )
            .map_err(refused)?;
            if !prepared
                .description
                .matches_plan_seal(prepared.template.native_manifest_template().plan())
            {
                return Err(refused(
                    "prepared logical execution Native template has a foreign plan seal",
                ));
            }
            let carrier = prepared.description.row_carrier().map_err(|error| {
                attempt_runtime_failure(QueryFailureReason::PreparationProtocol, error)
                    .error()
                    .clone()
            })?;
            let delivery = ProductionRootDelivery::bind(carrier, Some(capacity.window_alias()))?;
            drop(delivery);
            let PreparedLogicalComputation {
                description,
                template,
                options,
                consumer,
                handoffs,
                diagnostics,
                initial_credentials,
            } = computation.take().unwrap();
            let aborts = LogicalAbortRouter::new();
            let replacements = LogicalReplacementQualificationPort::new(
                self.runtime.clone(),
                aborts.worker_closed_contexts(),
                aborts.closure_epoch(),
            );
            let factory = ProductionDormantAttemptFactory {
                metadata_io: OriginalAttemptMetadataSource::Computation(Arc::downgrade(io)),
                runtime: self.runtime.clone(),
                options,
                aborts: Arc::clone(&aborts),
                diagnostics,
                initial_credentials: initial_credentials.map(|c| Arc::downgrade(&c)),
                replacements: replacements.clone(),
                handoffs: Some(handoffs.reserve_view()),
                pending_projection: None,
                pending_handoff: None,
            };
            let attempts = FrontendNativeAttemptPreparationPort::new(
                template,
                Arc::clone(&self.runtime.topology),
                factory,
                #[cfg(novarocks_dev_controls)]
                observation,
                #[cfg(novarocks_dev_controls)]
                self.runtime.test_controls.clone(),
                #[cfg(novarocks_dev_controls)]
                self.runtime.observations.clone(),
            );
            let replacement_port = match description.recovery() {
                RecoveryMode::NoRecovery => None,
                RecoveryMode::RestartAttemptBeforeVisibility => {
                    Some(Arc::new(replacements) as Arc<dyn ReplacementQualificationEffectPort>)
                }
            };
            // All fallible carrier checks ran while the original computation
            // and unique owner still occupied the caller's stable fields.
            let request = QueryExecutionRequest::bind_native(
                description,
                aborts as Arc<dyn AbortQueryContextEffectPort>,
                replacement_port,
                attempts,
            )
            .with_result_capacity(capacity.clone())?
            .with_internal_consumer(consumer)?;
            original.request = Some(request);
        }
        if original.input.is_none() {
            let request = original
                .request
                .take()
                .ok_or_else(|| refused("original computation request is absent"))?;
            match self
                .client
                .prepare_observed_start(request, original.reservation.take())
            {
                Ok(input) => original.input = Some(input),
                Err((error, request, reserved)) => {
                    original.request = Some(request);
                    original.reservation = reserved;
                    return Err(error);
                }
            }
        }
        // The same fixed input, owner, response and exact-exit targets exist
        // before the first foreign notification. The caller finishes outside
        // every metadata lock; a callback panic cannot roll these moves back.
        self.client.start_observed_in(
            &mut original.input,
            owner,
            &mut original.ready,
            &mut original.exit,
        )
    }

    fn reserve_identity(
        &self,
    ) -> Result<novarocks_query_application::api::ReservedExecutionIdentity, QueryExecutionError>
    {
        self.client.reserve_identity()
    }
}

/// Both arms borrow an already-owned original action Source. The Internal
/// arm can project its existing positive Root for metadata CPU; the ordinary
/// Read arm retains its original lazy preparation and progress author.
#[derive(Clone)]
enum OriginalAttemptMetadataSource {
    Read(Weak<crate::query::read_action::io_source::ReadMetadataIoSource>),
    Computation(Weak<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>),
}

struct ProductionManifestAttemptProjection {
    metadata_io: OriginalAttemptMetadataSource,
    control_binding: NativeControlBinding,
    source_cohort: NativeSourceCohortAdmission,
    native_actions: novarocks_workload_control::joint_admission::NativeAsyncActionAuthority,
    work_scope: novarocks_workload_control::WorkScope,
    runtime: FrontendNativeLogicalExecutionRuntime,
    options: Arc<ResolvedQueryOptions>,
    aborts: Arc<LogicalAbortRouter>,
    delivery: ProductionRootDelivery,
    diagnostics: ComputationDiagnostics,
    initial_credentials: Option<Weak<InitialComputationCredentials>>,
    pending_credential_source: Option<RoundCredentialLeaseSource>,
    pending_initializer: Option<AttemptInitializing>,
    pending_ready: Option<AttemptReady>,
    // A complete round is stable before source-plan installation may publish
    // callbacks. Its existing round convergence remains the actual exit author.
    pending_projected: Option<ProjectedManifestAttempt>,
    credential_anchor: Option<Arc<InitialComputationCredentials>>,
}

impl std::fmt::Debug for ProductionManifestAttemptProjection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductionManifestAttemptProjection")
            .field("runtime", &self.runtime)
            .finish_non_exhaustive()
    }
}

/// The frozen carrier and its already-admitted window travel together. A
/// relayed root cannot fall back to Arrow when its class has no capacity.
#[derive(Clone)]
pub(crate) enum ProductionRootDelivery {
    Relayed {
        kind: novarocks_result_contract::RootOutputKind,
        client_rows: Option<novarocks_result_contract::ClientRowProfile>,
        window: novarocks_workload_control::ResultWindowAlias,
    },
}
impl ProductionRootDelivery {
    pub(crate) fn bind(
        carrier: novarocks_query_application::api::ResultRowCarrier,
        window: Option<novarocks_workload_control::ResultWindowAlias>,
    ) -> Result<Self, QueryExecutionError> {
        use novarocks_query_application::api::ResultRowCarrier as C;
        use novarocks_result_contract::RootOutputKind as K;
        use novarocks_workload_control::ResultWindowClass as W;
        match carrier {
            C::Relayed { kind, client_rows } => {
                C::relayed(kind, client_rows)?;
                let window = window.ok_or_else(|| {
                    QueryExecutionError::from_execution(ExecutionFailureInput::new(
                        QueryFailureReason::PreparationProtocol.failure_fact(),
                        SafeDetail::truncating(
                            "relayed logical execution requires its admitted result window",
                        ),
                        ExecutionFailureSite::Coordinator,
                    ))
                })?;
                let expected = match kind {
                    K::ClientRows => W::Client,
                    _ => W::Internal,
                };
                if window.class() != expected {
                    return Err(QueryExecutionError::from_execution(
                        ExecutionFailureInput::new(
                            QueryFailureReason::PreparationProtocol.failure_fact(),
                            SafeDetail::truncating(
                                "relayed logical execution window differs from its frozen output class",
                            ),
                            ExecutionFailureSite::Coordinator,
                        ),
                    ));
                }
                Ok(Self::Relayed {
                    kind,
                    client_rows,
                    window,
                })
            }
        }
    }
}

pub(crate) type FrontendRootRowsRuntime = RootRelayBinding;

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
    fn take_rows_runtime(&mut self) -> Option<(FrontendRootRowsRuntime, AcceptedRootStatusSource)> {
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
        request: NativeActiveConvergenceRequest,
    ) -> NativeActiveAttemptConvergenceFuture<'a>;

    /// Closes a completed activation result which never entered run(). The
    /// same active/manifest fields stay installed throughout this borrowed F.
    fn converge_unstarted<'a>(
        &'a mut self,
        inputs: &'a mut M,
    ) -> NativeAttemptConvergenceFuture<'a>;
}

pub(crate) trait FrontendDormantAttemptBehavior<M = ManifestBoundNativeAttemptInputs>:
    std::fmt::Debug + Send + 'static
{
    type ActiveBehavior: FrontendActiveAttemptBehavior<M>;

    fn poll_bind_qualification(
        &mut self,
        inputs: &mut M,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), NativeAttemptActivationFailure>>;

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

    fn converge<'a>(&'a mut self, inputs: &'a mut M) -> NativeAttemptConvergenceFuture<'a>;
}

/// Frontend-local constructor for Task protocol and transport behavior. The
/// adapter itself remains the only constructor of the complete dormant owner.
pub(crate) trait FrontendDormantAttemptFactory: std::fmt::Debug + Send + 'static {
    type Behavior: FrontendDormantAttemptBehavior<ManifestBoundNativeAttemptInputs>;

    fn create(
        &mut self,
        request: &NativeAttemptPreparationRequest,
    ) -> Result<Self::Behavior, NativeAttemptPreparationError>;

    fn register_candidate(
        &mut self,
        _execution: novarocks_types::QueryExecutionId,
        _snapshot: &novarocks_query_application::api::BackendTopologySnapshot,
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
    fn poll_bind_qualification(
        &mut self,
        manifest: &TaskManifestBinding,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), NativeAttemptActivationFailure>>;

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

    fn converge<'a>(&'a mut self) -> NativeAttemptConvergenceFuture<'a>;
}

/// One exact Task round and its move-only root result owners.
pub(crate) struct ProjectedManifestAttempt {
    round: ManifestAssembledRound,
    rows: Option<(FrontendRootRowsRuntime, AcceptedRootStatusSource)>,
    _prepared: Option<TaskExecutionPreparedQuery>,
    _credential_storage:
        Option<Arc<crate::query_execution::lifecycle_plan::AttemptCredentialStorage>>,
    _split_assignment: Option<SplitAssignmentRoundGuard>,
    _abort_route: Option<LogicalAbortRoute>,
    handoff: Option<AttemptHandoffProducer>,
    profile_observer: Option<crate::task_execution::profile_diagnostics::ProfileDiagnosticObserver>,
}

impl ProjectedManifestAttempt {
    fn production_rows(
        round: ManifestAssembledRound,
        binding: FrontendRootRowsRuntime,
        statuses: AcceptedRootStatusSource,
        prepared: TaskExecutionPreparedQuery,
        credential_storage: Option<
            Arc<crate::query_execution::lifecycle_plan::AttemptCredentialStorage>,
        >,
        split_assignment: Option<SplitAssignmentRoundGuard>,
        abort_route: LogicalAbortRoute,
    ) -> Self {
        Self {
            round,
            rows: Some((binding, statuses)),
            _prepared: Some(prepared),
            _credential_storage: credential_storage,
            _split_assignment: split_assignment,
            _abort_route: Some(abort_route),
            handoff: None,
            profile_observer: None,
        }
    }
}

struct ProductionDormantAttemptFactory {
    metadata_io: OriginalAttemptMetadataSource,
    runtime: FrontendNativeLogicalExecutionRuntime,
    options: Arc<ResolvedQueryOptions>,
    aborts: Arc<LogicalAbortRouter>,
    diagnostics: ComputationDiagnostics,
    initial_credentials: Option<Weak<InitialComputationCredentials>>,
    replacements: LogicalReplacementQualificationPort,
    handoffs: Option<AttemptHandoffReserveView>,
    pending_projection: Option<ProductionManifestAttemptProjection>,
    pending_handoff: Option<AttemptHandoffProducer>,
}

impl std::fmt::Debug for ProductionDormantAttemptFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductionDormantAttemptFactory")
            .finish_non_exhaustive()
    }
}

impl FrontendDormantAttemptFactory for ProductionDormantAttemptFactory {
    type Behavior = FrontendTaskProtocolDormantBehavior<RegisteredManifestAttemptProjection>;

    fn create(
        &mut self,
        request: &NativeAttemptPreparationRequest,
    ) -> Result<Self::Behavior, NativeAttemptPreparationError> {
        if self.pending_projection.is_some() || self.pending_handoff.is_some() {
            return Err(attempt_failure(
                QueryFailureReason::PreparationProtocol,
                "the original Native factory still retains a preparation source",
            ));
        }
        let capacity = request.result_capacity().ok_or_else(|| {
            attempt_failure(
                QueryFailureReason::PreparationProtocol,
                "the admitted attempt has no result capacity",
            )
        })?;
        let carrier = request
            .description()
            .row_carrier()
            .map_err(|error| attempt_failure(QueryFailureReason::PreparationProtocol, error))?;
        let delivery = ProductionRootDelivery::bind(carrier, Some(capacity.window_alias()))
            .map_err(|error| attempt_failure(QueryFailureReason::PreparationProtocol, error))?;
        let native_actions = capacity
            .native_action_authority()
            .map_err(|error| attempt_failure(QueryFailureReason::PreparationProtocol, error))?;
        self.pending_projection = Some(ProductionManifestAttemptProjection {
            metadata_io: self.metadata_io.clone(),
            control_binding: NativeControlBinding::default(),
            source_cohort: NativeSourceCohortAdmission::default(),
            native_actions,
            runtime: self.runtime.clone(),
            work_scope: capacity.scope().clone(),
            options: Arc::clone(&self.options),
            aborts: Arc::clone(&self.aborts),
            delivery,
            diagnostics: self.diagnostics,
            initial_credentials: self.initial_credentials.clone(),
            pending_credential_source: None,
            pending_initializer: None,
            pending_ready: None,
            pending_projected: None,
            credential_anchor: None,
        });
        if let Some(offers) = self.handoffs.as_ref() {
            // The actual producer enters this stable factory field before an
            // older offer's close can execute any callback outside its lock.
            offers
                .reserve_in(request.execution(), &mut self.pending_handoff)
                .map_err(|error| NativeAttemptPreparationError::from(handoff_failure(error)))?;
        }
        Ok(FrontendTaskProtocolDormantBehavior::new(
            RegisteredManifestAttemptProjection {
                base: self.pending_projection.take().unwrap(),
                handoff: self.pending_handoff.take(),
                pending_projected: None,
            },
            ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            Arc::clone(&self.runtime.lifecycle_diagnostics),
        ))
    }

    fn register_candidate(
        &mut self,
        execution: novarocks_types::QueryExecutionId,
        snapshot: &novarocks_query_application::api::BackendTopologySnapshot,
    ) -> Result<(), NativeAttemptPreparationError> {
        self.replacements.register_candidate(execution, snapshot)
    }
}

/// The fixed factory reserves this attempt's independent handoff cell before
/// projection. Borrowed initialization never owns a replacement's resource cell.
struct RegisteredManifestAttemptProjection {
    base: ProductionManifestAttemptProjection,
    handoff: Option<AttemptHandoffProducer>,
    // This complete original result survives a handoff notification unwind.
    pending_projected: Option<ProjectedManifestAttempt>,
}
impl std::fmt::Debug for RegisteredManifestAttemptProjection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredManifestAttemptProjection")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}
impl FrontendManifestAttemptProjection for RegisteredManifestAttemptProjection {
    fn poll_bind_qualification(
        &mut self,
        manifest: &TaskManifestBinding,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), NativeAttemptActivationFailure>> {
        self.base.poll_bind_qualification(manifest, cx)
    }

    fn project<'a>(
        &'a mut self,
        prepared: PreparedDistributedQuery,
        manifest: &'a TaskManifestBinding,
        replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
        cancellation: CancellationView,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ProjectedManifestAttempt, NativeAttemptActivationFailure>,
                > + Send
                + 'a,
        >,
    > {
        if self.pending_projected.is_some() {
            return Box::pin(async {
                Err(projection_message(
                    "the original projected attempt still awaits source disposition",
                ))
            });
        }
        Box::pin(async move {
            self.pending_projected = Some(
                self.base
                    .project(prepared, manifest, replacement_admissions, cancellation)
                    .await?,
            );
            let projected = self.pending_projected.as_mut().unwrap();
            if let Some(handoff) = self.handoff.as_mut() {
                let protocol = projected.round.take_protocol_facts().ok_or_else(|| {
                    projection_message(
                        "internal Native attempt lost its original protocol receiver",
                    )
                })?;
                handoff
                    .stage(
                        manifest.execution(),
                        projected.round.round.root_task(),
                        manifest.plan(),
                        AttemptHandoffResources::new(
                            protocol,
                            projected._credential_storage.clone(),
                            projected.profile_observer.take(),
                        ),
                    )
                    .map_err(|error| NativeAttemptActivationFailure::new(handoff_failure(error)))?;
                projected.handoff = self.handoff.take();
            }
            Ok(self.pending_projected.take().unwrap())
        })
    }
    fn converge<'a>(&'a mut self) -> NativeAttemptConvergenceFuture<'a> {
        Box::pin(async move {
            if let Some(projected) = self.pending_projected.as_mut() {
                if let Some(handoff) = projected.handoff.as_mut() {
                    handoff.retire_unclaimed();
                }
                projected.round.converge_unstarted().await?;
                // Only after actual original close may the synchronously
                // drop-safe return leave its stable receiving field.
                drop(self.pending_projected.take());
            }
            if let Some(handoff) = self.handoff.as_mut() {
                handoff.retire_unclaimed();
            }
            self.base.converge().await
        })
    }
}

impl FrontendManifestAttemptProjection for ProductionManifestAttemptProjection {
    fn poll_bind_qualification(
        &mut self,
        manifest: &TaskManifestBinding,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), NativeAttemptActivationFailure>> {
        let ProductionRootDelivery::Relayed { window, .. } = &self.delivery;
        // This is the SAME original projection field used by activation.
        // Internal binding derives from original positive payload/computation
        // grants and grows their full union without moving or refunding them.
        self.control_binding
            .poll(
                window,
                &self.work_scope,
                manifest.selected_network_origins(self.runtime.data_runtime.native_transport()),
                cx,
            )
            .map(|result| {
                result.map_err(|error| {
                    NativeAttemptActivationFailure::new(
                        NativeAttemptPreparationFailure::from_input(ExecutionFailureInput::new(
                            error.failure_fact(),
                            SafeDetail::truncating_display(format_args!(
                                "Native control binding refused: {error}"
                            )),
                            ExecutionFailureSite::Coordinator,
                        )),
                    )
                })
            })
    }

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

    fn converge<'a>(&'a mut self) -> NativeAttemptConvergenceFuture<'a> {
        Box::pin(async move {
            // Withdraw the same unpublished binding/reply before ending its
            // original preparation source; no Task RPC can precede this phase.
            self.control_binding.close();
            if let Some(projected) = self.pending_projected.as_mut() {
                projected.round.converge_unstarted().await?;
                drop(self.pending_projected.take());
            }
            if let Some(ready) = self.pending_ready.as_mut() {
                ready.close().await?;
            }
            if let Some(initializing) = self.pending_initializer.as_mut() {
                initializing.close().await?;
            }
            // All original source-open/close and panic-payload jobs actually
            // joined. Credential/source fields remain installed through that cut.
            self.pending_ready = None;
            self.pending_initializer = None;
            self.pending_credential_source = None;
            self.credential_anchor = None;
            self.source_cohort.close_after_sources();
            Ok(())
        })
    }
}

impl ProductionManifestAttemptProjection {
    async fn project_attempt(
        &mut self,
        prepared: PreparedDistributedQuery,
        manifest: &TaskManifestBinding,
        replacement_admissions: Option<
            Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>,
        >,
        cancellation: CancellationView,
    ) -> Result<ProjectedManifestAttempt, NativeAttemptActivationFailure> {
        if self.pending_credential_source.is_some()
            || self.pending_initializer.is_some()
            || self.pending_ready.is_some()
            || self.pending_projected.is_some()
            || self.credential_anchor.is_some()
        {
            return Err(projection_message(
                "the original Native initializer still owns its source disposition",
            ));
        }
        // Recovery has already driven this SAME original field before its
        // qualification RPC. Initial activation drives it for the first time.
        std::future::poll_fn(|cx| self.poll_bind_qualification(manifest, cx)).await?;
        // Full parent D was bound by the original exact manifest above. One
        // positive cohort must precede writers, initializer, watch and every round RPC.
        // Qualification has its own paid Native action source after the same full-D gate.
        let source_count =
            native_source_count(prepared.typed_scans().count(), manifest.contexts().len())
                .map_err(|error| projection_message(error.to_string()))?;
        std::future::poll_fn(|cx| self.source_cohort.poll(&self.work_scope, source_count, cx))
            .await
            .map_err(|error| {
                NativeAttemptActivationFailure::new(NativeAttemptPreparationFailure::from_input(
                    ExecutionFailureInput::new(
                        error.failure_fact(),
                        SafeDetail::truncating_display(format_args!(
                            "Native Source cohort refused: {error}"
                        )),
                        ExecutionFailureSite::Coordinator,
                    ),
                ))
            })?;
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
        // This projection runs only after QA binds the original open ticket.
        // Upgrade once under the admitted attempt, then keep that exact owner
        // until its original table has crossed Native initialization.
        self.credential_anchor = self
            .initial_credentials
            .as_ref()
            .map(|credentials| {
                credentials.upgrade().ok_or_else(|| projection_message(
                "initial computation credential custody was released before Native projection",
            ))
            })
            .transpose()?;
        let write_session = if manifest.writer_cohorts().next().is_some() {
            let session = self
                .credential_anchor
                .as_ref()
                .and_then(|initial| initial.write_production_session())
                .and_then(|original| original.upgrade())
                .ok_or_else(|| {
                    projection_message("Native writers have no original write production receiver")
                })?;
            // Original complete control D must already be bound. Registration
            // precedes initializer effects, round stage and every Create RPC.
            session
                .register_manifest_productions(manifest, &self.work_scope)
                .await
                .map_err(write_production_failure)?;
            Some(session)
        } else {
            None
        };
        self.pending_credential_source = Some(match &self.credential_anchor {
            Some(credentials) => credentials
                .take_source(execution)
                .map_err(projection_failure)?,
            None => RoundCredentialLeaseSource::fresh(execution),
        });
        let connector_context = crate::connector::query_connector_request_context_on_runtime(
            self.runtime.data_runtime.handle(),
            deadline,
            novarocks_query_application::cancellation::QueryCancellationView::governed(
                cancellation.clone(),
                Some(self.options.timeout_ms().max(1) as u64),
            ),
        )
        .map_err(projection_message)?;
        // The original action strongly owns and drives this source until all
        // active/retired initializers actually exit. A scope cannot replace it.
        let unavailable = || {
            projection_message("original metadata I/O source exited before Native initialization")
        };
        let (connector_context, metadata_preparation) = match &self.metadata_io {
            OriginalAttemptMetadataSource::Read(original) => (
                connector_context.with_io_admission(original.upgrade().ok_or_else(unavailable)?),
                None,
            ),
            OriginalAttemptMetadataSource::Computation(original) => (
                connector_context.with_io_admission(original.upgrade().ok_or_else(unavailable)?),
                Some(original.clone()),
            ),
        };
        let connector_context = self
            .pending_credential_source
            .as_ref()
            .unwrap()
            .connector_request_context(connector_context)
            .map_err(projection_message)?;
        // Reborrow after the mutable binding/cohort awaits; initializer keeps
        // the same original window alias and its payload/computation owners.
        let ProductionRootDelivery::Relayed { window, .. } = &self.delivery;
        AttemptInitializing::new_governed_in(
            execution,
            self.work_scope.clone(),
            window.clone(),
            self.source_cohort
                .borrowed()
                .map_err(|error| projection_message(error.to_string()))?,
            prepared,
            schedule,
            self.runtime.task_update_retry_policy,
            Arc::clone(&feedback),
            self.runtime.split_initial_wait_cap,
            connector_context,
            cancellation,
            self.runtime.data_runtime.clone(),
            metadata_preparation,
            &mut self.pending_credential_source,
            &mut self.pending_initializer,
        )
        .map_err(projection_failure)?;
        let initializing = self.pending_initializer.as_mut().unwrap();
        initializing
            .initialize()
            .await
            .map_err(projection_failure)?;
        initializing.take_ready_in(&mut self.pending_ready);
        let (ready_execution, artifacts, schedule, ready_feedback, credential_leases) = self
            .pending_ready
            .as_mut()
            .unwrap()
            .take_preparation_parts();
        if ready_execution != execution || !Arc::ptr_eq(&feedback, &ready_feedback) {
            return Err(projection_message(
                "attempt initialization returned facts for another Native attempt",
            ));
        }

        let scheduled = crate::runtime_filter::plan_encoder::bind_runtime_filters(artifacts)
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
            ProtocolQueryOptions::from_proto(encode_query_options(self.options.runtime_options())),
        )
        .map_err(projection_failure)?
        .with_credential_leases(credential_leases);
        // The frozen writer handle must resolve against the same generation
        // as its original session, including writes with no provider reads.
        let options = match write_session.as_ref() {
            Some(session) => {
                let catalogs = crate::query_execution::artifact::merge_write_session_catalog(
                    options.catalog_set(),
                    session,
                )
                .map_err(projection_failure)?;
                options.with_catalog_set(catalogs)
            }
            None => options,
        };
        let mut task_prepared = ready
            .prepare_task_execution(options)
            .map_err(projection_failure)?;
        let submission = encode_native_submission(
            &task_prepared
                .native_submission_view()
                .map_err(projection_failure)?,
        )
        .map_err(projection_message)?;
        let (submissions, root_fetch) = task_prepared
            .seal_task_submission(submission)
            .map_err(projection_failure)?
            .into_parts();
        if !root_fetch.uses_result_buffer() {
            return Err(projection_message(
                "logical execution root does not own a Native result buffer",
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
        // Preserve the original capability beyond projection. Its planning
        // route and material remain attached to this exact attempt until the
        // domain result transfers them to its publication owner.
        let storage = task_prepared.take_terminal_storage();
        if let Some(credentials) = &self.credential_anchor {
            credentials
                .verify_native_storage(&storage)
                .map_err(projection_failure)?;
        }
        // Even an empty table carries the original collector's transfer fact.
        let credential_storage = Some(storage);
        let backends = manifest
            .contexts()
            .iter()
            .map(|context| context.backend().target().descriptor().clone())
            .collect::<Vec<_>>();
        let mut round = crate::task_execution::manifest_round::assemble_manifest_round(
            manifest,
            submissions,
            establish,
            replacement_admissions,
            crate::task_execution::manifest_round::ManifestAttemptTransport {
                clock: Arc::clone(&self.runtime.clock),
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
                subscription_scope: self.work_scope.clone(),
                source_cohort: self
                    .source_cohort
                    .borrowed()
                    .map_err(|error| projection_message(error.to_string()))?,
                native_actions: self.native_actions.clone(),
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
        install_attempt_pumps(
            &mut round.round,
            AttemptPumps {
                feedback_state: feedback,
                declared_feedback_channels: feedback_declaration.channels().len(),
                reads: Arc::clone(&result_transport) as Arc<dyn TaskDynamicFilterReads>,
            },
        );
        let (abort_route, abort_intake) =
            self.aborts
                .install(execution, self.runtime.abort_capacity, round.abort_wake())?;
        round.install_abort_effect_intake(abort_intake);
        let root_status = round.take_root_status_source().ok_or_else(|| {
            projection_message("logical execution Task round has no accepted root status source")
        })?;
        let result_transport_for_diagnostics = Arc::clone(&result_transport)
            as Arc<dyn crate::native::fragment_transport::TaskResultTransport>;
        let root_binding = match &self.delivery {
            ProductionRootDelivery::Relayed {
                kind,
                client_rows,
                window,
            } => {
                let frontier = RootRelayFrontier::new(
                    round.round.root_task(),
                    novarocks_result_contract::RootProfileId::V1,
                    *kind,
                    *client_rows,
                )
                .map_err(|error| projection_message(error.to_string()))?;
                RootRelayBinding {
                    port: Arc::new(NativeBoundedRootReadPort::new(result_transport)),
                    frontier,
                    window: window.clone(),
                    max_wait: Duration::from_millis(250),
                }
            }
        };
        let profile_observer = match self.diagnostics {
            ComputationDiagnostics::None => None,
            ComputationDiagnostics::Profile => Some(
                round
                    .install_profile_diagnostics(result_transport_for_diagnostics)
                    .map_err(|error| projection_message(error.to_string()))?,
            ),
        };
        self.pending_projected = Some(ProjectedManifestAttempt::production_rows(
            round,
            root_binding,
            root_status,
            task_prepared,
            credential_storage,
            None,
            abort_route,
        ));
        let attempt = self.pending_projected.as_mut().unwrap();
        attempt.profile_observer = profile_observer;
        // Every fallible preparation step completed while the initializer's
        // original Ready field still owned all source handles. Publish the
        // complete original round before the sole plan-to-pump transfer.
        if let Some(plan) = self.pending_ready.as_mut().unwrap().take_split_plan() {
            attempt._split_assignment = attempt.round.install_split_assignment(
                execution,
                plan,
                self.runtime.data_runtime.clone(),
            );
        }
        Ok(self.pending_projected.take().unwrap())
    }
}

/// Preserve the handoff author's typed local invariant failure through Native.
fn handoff_failure(error: QueryExecutionError) -> NativeAttemptPreparationFailure {
    match error.execution_failure() {
        Some(input) => NativeAttemptPreparationFailure::from_input(input.clone()),
        None => attempt_runtime_failure(QueryFailureReason::PreparationProtocol, error),
    }
}

/// Preserve the production owner's real refusal; capacity is not a Native protocol invariant.
fn write_production_failure(
    error: novarocks_spi::connector::ConnectorError,
) -> NativeAttemptActivationFailure {
    tracing::debug!(error = %error, "Native write production registration failed");
    NativeAttemptActivationFailure::new(NativeAttemptPreparationFailure::from_input(
        ExecutionFailureInput::new(
            error.failure_fact(),
            SafeDetail::truncating("native write production responsibility was refused"),
            ExecutionFailureSite::Coordinator,
        ),
    ))
}

fn projection_failure(error: DistributedQueryError) -> NativeAttemptActivationFailure {
    NativeAttemptActivationFailure::new(NativeAttemptPreparationFailure::from_input(
        error.into_execution_failure_input(),
    ))
}

fn projection_message(message: impl Into<String>) -> NativeAttemptActivationFailure {
    NativeAttemptActivationFailure::new(attempt_runtime_failure(
        QueryFailureReason::PreparationProtocol,
        message.into(),
    ))
}

/// Dormant half of the concrete manifest-only Task protocol behavior.
pub(crate) struct FrontendTaskProtocolDormantBehavior<P> {
    projection: P,
    completion: ManifestAttemptCompletion,
    lifecycle_diagnostics:
        Arc<crate::query_execution::lifecycle_diagnostics::FrontendLifecycleDiagnostics>,
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
    pub(crate) fn new(
        projection: P,
        completion: ManifestAttemptCompletion,
        lifecycle_diagnostics: Arc<
            crate::query_execution::lifecycle_diagnostics::FrontendLifecycleDiagnostics,
        >,
    ) -> Self {
        Self {
            projection,
            completion,
            lifecycle_diagnostics,
        }
    }
}

impl<P> FrontendDormantAttemptBehavior for FrontendTaskProtocolDormantBehavior<P>
where
    P: FrontendManifestAttemptProjection,
{
    type ActiveBehavior = FrontendTaskProtocolActiveBehavior;

    fn poll_bind_qualification(
        &mut self,
        inputs: &mut ManifestBoundNativeAttemptInputs,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), NativeAttemptActivationFailure>> {
        // Binding needs only the exact retained manifest; it must not invoke
        // access.instantiate, a writer, initializer, watch or network request.
        self.projection
            .poll_bind_qualification(inputs.manifest(), cx)
    }

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
                    QueryFailureReason::PreparationProtocol,
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
                Arc::clone(&self.lifecycle_diagnostics),
            ))
        })
    }

    fn converge<'a>(
        &'a mut self,
        _inputs: &'a mut ManifestBoundNativeAttemptInputs,
    ) -> NativeAttemptConvergenceFuture<'a> {
        self.projection.converge()
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
    rows: Option<(FrontendRootRowsRuntime, AcceptedRootStatusSource)>,
    _prepared: Option<TaskExecutionPreparedQuery>,
    _credential_storage:
        Option<Arc<crate::query_execution::lifecycle_plan::AttemptCredentialStorage>>,
    _split_assignment: Option<SplitAssignmentRoundGuard>,
    _abort_route: Option<LogicalAbortRoute>,
    handoff: Option<AttemptHandoffProducer>,
    _profile_observer:
        Option<crate::task_execution::profile_diagnostics::ProfileDiagnosticObserver>,
    lifecycle_diagnostics:
        Arc<crate::query_execution::lifecycle_diagnostics::FrontendLifecycleDiagnostics>,
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
        lifecycle_diagnostics: Arc<
            crate::query_execution::lifecycle_diagnostics::FrontendLifecycleDiagnostics,
        >,
    ) -> Self {
        let ProjectedManifestAttempt {
            round,
            rows,
            _prepared: prepared,
            _credential_storage: credential_storage,
            _split_assignment: split_assignment,
            _abort_route: abort_route,
            handoff,
            profile_observer,
        } = attempt;
        Self {
            attempt: round,
            completion,
            rows,
            _prepared: prepared,
            _credential_storage: credential_storage,
            _split_assignment: split_assignment,
            _abort_route: abort_route,
            handoff,
            _profile_observer: profile_observer,
            lifecycle_diagnostics,
        }
    }
}

impl FrontendActiveAttemptBehavior for FrontendTaskProtocolActiveBehavior {
    fn converge_unstarted<'a>(
        &'a mut self,
        _inputs: &'a mut ManifestBoundNativeAttemptInputs,
    ) -> NativeAttemptConvergenceFuture<'a> {
        Box::pin(async move {
            if let Some(handoff) = self.handoff.as_mut() {
                handoff.retire_unclaimed();
            }
            self.attempt.converge_unstarted().await
        })
    }

    fn take_rows_runtime(&mut self) -> Option<(FrontendRootRowsRuntime, AcceptedRootStatusSource)> {
        self.rows.take()
    }

    fn run<'a>(
        &'a mut self,
        _inputs: &'a mut ManifestBoundNativeAttemptInputs,
        drive: &'a NativeAttemptDrive,
        cancellation: CancellationView,
    ) -> NativeAttemptRunFuture<'a> {
        Box::pin(async move {
            if let Err(error) = self.attempt.round.bind_protocol_drive(drive) {
                return crate::task_execution::manifest_round::task_protocol_failure(error);
            }
            if let Some(handoff) = self.handoff.as_mut()
                && let Err(error) = handoff.bind(drive)
            {
                return NativeAttemptTerminal::Failed(handoff_failure(error));
            }
            self.attempt.run(drive, cancellation, self.completion).await
        })
    }

    fn converge<'a>(
        &'a mut self,
        inputs: &'a mut ManifestBoundNativeAttemptInputs,
        request: NativeActiveConvergenceRequest,
    ) -> NativeActiveAttemptConvergenceFuture<'a> {
        let execution_id = inputs.execution_id();
        Box::pin(async move {
            if let Some(handoff) = self.handoff.as_mut() {
                handoff.retire_unclaimed();
            }
            let convergence = self.attempt.converge(request).await?;
            if self.lifecycle_diagnostics.is_enabled() {
                let observed_failure = self.attempt.observed_failure();
                let contributions = self
                    .attempt
                    .round
                    .execution()
                    .released_runtime_filter_contributions();
                let runtime_filter = if contributions.is_complete() {
                    crate::query_execution::lifecycle_diagnostics::RuntimeFilterTerminalRollupSnapshot::Available(
                    crate::query_execution::runtime_filter_terminal_rollup::rollup_from_release_contributions(
                        contributions.contributions(),
                    ),
                )
                } else {
                    crate::query_execution::lifecycle_diagnostics::RuntimeFilterTerminalRollupSnapshot::Unavailable(
                    crate::query_execution::lifecycle_diagnostics::RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
                )
                };
                self.lifecycle_diagnostics.publish(
                crate::query_execution::lifecycle_diagnostics::QueryLifecycleConvergenceSnapshot {
                    execution_id,
                    primary_error: None,
                failure: observed_failure,
                worker_contexts_converged: matches!(&convergence, NativeActiveConvergenceClosure::Evidence(_)),
                    runtime_filter,
                },
            );
            }
            Ok(convergence)
        })
    }
}

trait RetainedAttemptInputs: std::fmt::Debug + Send + 'static {
    type Manifest: std::fmt::Debug + Send + 'static;

    fn eligible_backends(&self) -> &[novarocks_types::identity::BackendProcessId];

    fn manifest_eligible_backends(
        manifest: &Self::Manifest,
    ) -> &[novarocks_types::identity::BackendProcessId];

    fn validate_bound_schedule(
        manifest: &Self::Manifest,
        schedule: &AttemptSchedule,
    ) -> Result<(), NativeAttemptActivationFailure>;

    #[cfg(novarocks_dev_controls)]
    fn publish_fault_task_selection(
        manifest: &Self::Manifest,
        controls: &novarocks_native_adapter::test_controls::NativeTestControls,
        nomination: &novarocks_failpoint::target_binding::StatementWorkNomination,
    ) -> Result<(), NativeAttemptActivationFailure>;

    fn bind_manifest(
        self,
        schedule: &AttemptSchedule,
    ) -> Result<Self::Manifest, NativeAttemptActivationFailure>;
}

impl RetainedAttemptInputs for SnapshotBoundDormantAttemptInputs {
    type Manifest = ManifestBoundNativeAttemptInputs;

    #[cfg(novarocks_dev_controls)]
    fn publish_fault_task_selection(
        inputs: &Self::Manifest,
        controls: &novarocks_native_adapter::test_controls::NativeTestControls,
        nomination: &novarocks_failpoint::target_binding::StatementWorkNomination,
    ) -> Result<(), NativeAttemptActivationFailure> {
        let publish = || -> Result<(), String> {
            if controls.needs_remote_writer_selection(nomination)? {
                let manifest = inputs.manifest();
                let task = manifest.remote_writer_sender().ok_or(
                    "selected original manifest has no remote writer sender into its Root",
                )?;
                controls.publish_nominated_writer_task(
                    nomination,
                    task,
                    manifest.root().backend_process_id(),
                )?;
            }
            Ok(())
        };
        publish().map_err(|detail| {
            NativeAttemptActivationFailure::new(attempt_runtime_failure(
                QueryFailureReason::PreparationProtocol,
                detail,
            ))
        })
    }

    fn validate_bound_schedule(
        manifest: &Self::Manifest,
        schedule: &AttemptSchedule,
    ) -> Result<(), NativeAttemptActivationFailure> {
        manifest
            .manifest()
            .validate_schedule(schedule)
            .map_err(projection_failure)
    }

    fn eligible_backends(&self) -> &[novarocks_types::identity::BackendProcessId] {
        self.eligible_backends()
    }

    fn manifest_eligible_backends(
        manifest: &Self::Manifest,
    ) -> &[novarocks_types::identity::BackendProcessId] {
        manifest.eligible_backends()
    }

    fn bind_manifest(
        self,
        schedule: &AttemptSchedule,
    ) -> Result<Self::Manifest, NativeAttemptActivationFailure> {
        self.bind_manifest(schedule).map_err(|error| {
            NativeAttemptActivationFailure::new(attempt_runtime_failure(
                QueryFailureReason::PreparationProtocol,
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
    B: FrontendDormantAttemptBehavior<I::Manifest>,
{
    #[cfg(novarocks_dev_controls)]
    test_placement: Option<(
        Arc<novarocks_native_adapter::test_controls::NativeTestControls>,
        novarocks_failpoint::target_binding::StatementWorkNomination,
    )>,
    dormant_inputs: Option<I>,
    manifest_inputs: Option<I::Manifest>,
    pending_active_behavior:
        Option<<B as FrontendDormantAttemptBehavior<I::Manifest>>::ActiveBehavior>,
    behavior: B,
}

impl<B, I> std::fmt::Debug for SnapshotBoundDormantAttemptOwner<B, I>
where
    B: FrontendDormantAttemptBehavior<I::Manifest>,
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

type NativeActiveAttemptActivationFuture<'a, Manifest, Behavior> = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    (
                        SnapshotBoundActiveAttemptOwner<Manifest, Behavior>,
                        Option<(FrontendRootRowsRuntime, AcceptedRootStatusSource)>,
                    ),
                    NativeAttemptActivationFailure,
                >,
            > + Send
            + 'a,
    >,
>;

impl<B, I> SnapshotBoundDormantAttemptOwner<B, I>
where
    I: RetainedAttemptInputs,
    B: FrontendDormantAttemptBehavior<I::Manifest>,
{
    fn new(inputs: I, behavior: B) -> Self {
        Self {
            #[cfg(novarocks_dev_controls)]
            test_placement: None,
            dormant_inputs: Some(inputs),
            manifest_inputs: None,
            pending_active_behavior: None,
            behavior,
        }
    }

    #[cfg(test)]
    fn from_manifest(inputs: I::Manifest, behavior: B) -> Self {
        Self {
            #[cfg(novarocks_dev_controls)]
            test_placement: None,
            dormant_inputs: None,
            manifest_inputs: Some(inputs),
            pending_active_behavior: None,
            behavior,
        }
    }

    fn retained_eligible_backends(&self) -> &[novarocks_types::identity::BackendProcessId] {
        match &self.dormant_inputs {
            Some(inputs) => inputs.eligible_backends(),
            None => I::manifest_eligible_backends(
                self.manifest_inputs
                    .as_ref()
                    .expect("dormant owner retains its bound manifest until activation"),
            ),
        }
    }

    fn bind_manifest(
        &mut self,
        schedule: &AttemptSchedule,
    ) -> Result<(), NativeAttemptActivationFailure> {
        if let Some(manifest) = self.manifest_inputs.as_ref() {
            return I::validate_bound_schedule(manifest, schedule);
        }
        let inputs = self
            .dormant_inputs
            .take()
            .expect("Query Application activates a dormant Native attempt only once");
        self.manifest_inputs = Some(inputs.bind_manifest(schedule)?);
        #[cfg(novarocks_dev_controls)]
        if let Some((controls, nomination)) = &self.test_placement {
            I::publish_fault_task_selection(
                self.manifest_inputs.as_ref().unwrap(),
                controls,
                nomination,
            )?;
        }
        Ok(())
    }

    fn activate_fixed_retained<'a>(
        &'a mut self,
        replacement_admissions: Option<
            Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>,
        >,
        cancellation: CancellationView,
    ) -> NativeActiveAttemptActivationFuture<'a, I::Manifest, B::ActiveBehavior>
    where
        B: FrontendDormantAttemptBehavior<I::Manifest>,
    {
        if self.pending_active_behavior.is_some() {
            return Box::pin(async {
                Err(projection_message(
                    "the original active behavior still awaits source disposition",
                ))
            });
        }
        let behavior = &mut self.behavior;
        let manifest_inputs = &mut self.manifest_inputs;
        let pending_active = &mut self.pending_active_behavior;
        Box::pin(async move {
            *pending_active = Some(
                behavior
                    .activate(
                        manifest_inputs
                            .as_mut()
                            .expect("activation retains its manifest inputs"),
                        replacement_admissions,
                        cancellation,
                    )
                    .await?,
            );
            // This trait extraction may unwind. Both the original manifest
            // and the returned active behavior remain in their owner fields.
            let rows = pending_active.as_mut().unwrap().take_rows_runtime();
            let inputs = manifest_inputs
                .take()
                .expect("successful activation transfers the retained manifest once");
            let active_behavior = pending_active.take().unwrap();
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
                Some((binding, statuses)) => {
                    ActivatedNativeAttempt::relayed_rows(owner, binding, statuses)
                }
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
        // Cleanup is irrevocable. Its private borrowed chain needs no owned
        // cancellation view whose notification-bearing Drop could follow a
        // retained health clone through several async return frames.
        drop(cancellation);
        Box::pin(async move {
            if let Some(active) = self.pending_active_behavior.as_mut() {
                let inputs = self
                    .manifest_inputs
                    .as_mut()
                    .expect("a stable returned active retains its original manifest");
                active.converge_unstarted(inputs).await?;
                drop(self.pending_active_behavior.take());
            }
            match self.manifest_inputs.as_mut() {
                Some(inputs) => self.behavior.converge(inputs).await,
                None => Ok(()),
            }
        })
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
        request: NativeActiveConvergenceRequest,
    ) -> NativeActiveAttemptConvergenceFuture<'a> {
        self.behavior.converge(&mut self.inputs, request)
    }
}

impl<B> DormantNativeAttemptOwner for SnapshotBoundDormantAttemptOwner<B>
where
    B: FrontendDormantAttemptBehavior,
{
    #[cfg(novarocks_dev_controls)]
    fn test_preferred_backend_process(
        &self,
        execution: novarocks_types::QueryExecutionId,
    ) -> Result<Option<novarocks_types::BackendProcessId>, String> {
        match &self.test_placement {
            Some((controls, nomination)) => {
                controls.preferred_backend_process(nomination, execution)
            }
            None => Ok(None),
        }
    }
    fn eligible_backends(&self) -> &[novarocks_types::identity::BackendProcessId] {
        self.retained_eligible_backends()
    }

    fn poll_bind_qualification(
        &mut self,
        schedule: &AttemptSchedule,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), NativeAttemptActivationFailure>> {
        if let Err(error) = self.bind_manifest(schedule) {
            return std::task::Poll::Ready(Err(error));
        }
        self.behavior.poll_bind_qualification(
            self.manifest_inputs
                .as_mut()
                .expect("same original bound successor manifest"),
            cx,
        )
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
pub(crate) struct FrontendNativeAttemptPreparationPort<F: FrontendDormantAttemptFactory> {
    state: FrontendNativeAttemptPreparationState<F>,
}

struct FrontendNativeAttemptPreparationState<F: FrontendDormantAttemptFactory> {
    pending_changes: Option<watch::Receiver<u64>>,
    pending_snapshot: Option<novarocks_query_application::api::BackendTopologySnapshot>,
    pending_inputs: Option<SnapshotBoundDormantAttemptInputs>,
    pending_behavior: Option<F::Behavior>,
    pending_owner: Option<Box<dyn DormantNativeAttemptOwner>>,
    pending_scheduling: Option<novarocks_query_application::api::ExecutionSchedulingFacts>,
    template: PreparedDistributedAttemptTemplate,
    topology: BackendTopologyService,
    dormant_factory: F,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
    #[cfg(novarocks_dev_controls)]
    controls: Option<Arc<novarocks_native_adapter::test_controls::NativeTestControls>>,
    #[cfg(novarocks_dev_controls)]
    observations:
        Option<Arc<novarocks_native_adapter::test_observation_controls::NativeObservationControls>>,
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
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
        #[cfg(novarocks_dev_controls)] controls: Option<
            Arc<novarocks_native_adapter::test_controls::NativeTestControls>,
        >,
        #[cfg(novarocks_dev_controls)] observations: Option<
            Arc<novarocks_native_adapter::test_observation_controls::NativeObservationControls>,
        >,
    ) -> Self {
        Self {
            state: FrontendNativeAttemptPreparationState {
                pending_changes: None,
                pending_snapshot: None,
                pending_inputs: None,
                pending_behavior: None,
                pending_owner: None,
                pending_scheduling: None,
                template,
                topology,
                dormant_factory,
                #[cfg(novarocks_dev_controls)]
                observation,
                #[cfg(novarocks_dev_controls)]
                controls,
                #[cfg(novarocks_dev_controls)]
                observations,
            },
        }
    }
}

impl<F> NativeAttemptPreparationPort for FrontendNativeAttemptPreparationPort<F>
where
    F: FrontendDormantAttemptFactory,
{
    fn prepare_in<'a>(
        &'a mut self,
        request: &'a mut NativeAttemptPreparationWork,
    ) -> NativeAttemptPreparationFuture<'a> {
        let state = &mut self.state;
        Box::pin(async move {
            let cancellation = request.cancellation();
            #[cfg(novarocks_dev_controls)]
            if let Some(observation) = &state.observation {
                observation.bind_attempt(request.execution());
            }
            // A failed attempt can be replaced on the same live processes once
            // its exact contexts are fenced. Qualification, not a topology
            // revision change, enforces that isolation before activation. A typed
            // failed process additionally constrains successor placement.
            if state.pending_changes.is_none() {
                state.pending_changes = Some(state.topology.subscribe_changes());
            }
            wait_for_attempt_topology_in(
                &state.topology,
                state.pending_changes.as_mut().unwrap(),
                &mut state.pending_snapshot,
                request.topology_requirement(),
                &cancellation,
            )
            .await?;
            let snapshot = state.pending_snapshot.as_ref().unwrap();
            #[cfg(novarocks_dev_controls)]
            let nominated_execution = state
                .observation
                .as_ref()
                .and_then(|handle| handle.nomination_for_exact_execution(request.execution()));
            #[cfg(novarocks_dev_controls)]
            if let Some(nomination) = &nominated_execution {
                for target in snapshot.targets() {
                    let process = target.process_id().map_err(|error| {
                        attempt_failure(QueryFailureReason::PreparationProtocol, error.to_string())
                    })?;
                    let binding = (|| -> Result<(), String> {
                        if let Some(controls) = &state.controls {
                            controls.publish_nominated_binding(
                                nomination,
                                process,
                                request.execution(),
                            )?;
                        }
                        if let Some(observations) = &state.observations {
                            for action in [
                                "grpc-fragment",
                                "reader-marker",
                                "writer-marker",
                                "catalog-materialization",
                                "catalog-lifecycle",
                                "exchange-snapshot",
                                "catalog-hold",
                                "catalog-failure",
                            ] {
                                observations.publish_nominated_binding(
                                    action,
                                    nomination,
                                    process,
                                    request.execution(),
                                )?;
                            }
                        }
                        Ok(())
                    })();
                    binding.map_err(|error| {
                        attempt_failure(QueryFailureReason::PreparationProtocol, error)
                    })?;
                }
            }
            state
                .dormant_factory
                .register_candidate(request.execution(), snapshot)?;
            state.pending_scheduling =
                Some(state.template.attempt_scheduling_facts().map_err(|error| {
                    attempt_failure(QueryFailureReason::PreparationProtocol, error)
                })?);
            state.pending_inputs = Some(
                SnapshotBoundDormantAttemptInputs::capture(
                    &state.template,
                    request,
                    state.pending_snapshot.take().unwrap(),
                )
                .map_err(|error| {
                    attempt_failure(QueryFailureReason::PreparationProtocol, error.to_string())
                })?,
            );
            state.pending_behavior = Some(state.dormant_factory.create(request)?);
            let owner = SnapshotBoundDormantAttemptOwner::new(
                state.pending_inputs.take().unwrap(),
                state.pending_behavior.take().unwrap(),
            );
            #[cfg(novarocks_dev_controls)]
            let owner = {
                let mut owner = owner;
                owner.test_placement = state.controls.clone().zip(nominated_execution);
                owner
            };
            state.pending_owner = Some(Box::new(owner));
            request
                .bind_in(&mut state.pending_scheduling, &mut state.pending_owner)
                .map_err(NativeAttemptPreparationError::from)
        })
    }
}

#[cfg(test)]
async fn wait_for_attempt_topology(
    topology: &BackendTopologyService,
    requirement: novarocks_query_application::api::NativeAttemptTopologyRequirement,
    cancellation: &CancellationView,
) -> Result<novarocks_query_application::api::BackendTopologySnapshot, NativeAttemptPreparationError>
{
    let mut changes = topology.subscribe_changes();
    let mut snapshot = None;
    wait_for_attempt_topology_in(
        topology,
        &mut changes,
        &mut snapshot,
        requirement,
        cancellation,
    )
    .await?;
    Ok(snapshot.unwrap())
}

async fn wait_for_attempt_topology_in(
    topology: &BackendTopologyService,
    changes: &mut watch::Receiver<u64>,
    retained_snapshot: &mut Option<novarocks_query_application::api::BackendTopologySnapshot>,
    requirement: novarocks_query_application::api::NativeAttemptTopologyRequirement,
    cancellation: &CancellationView,
) -> Result<(), NativeAttemptPreparationError> {
    loop {
        *retained_snapshot = Some(capture_attempt_topology(topology)?);
        if topology_satisfies_requirement(retained_snapshot.as_ref().unwrap(), requirement) {
            return Ok(());
        }
        tokio::select! {
            reason = cancellation.cancelled() => {
                let reason_fact = query_cancellation_reason(&reason);
                return Err(attempt_failure(
                    reason_fact,
                    format!("Native replacement topology wait was cancelled: {reason:?}"),
                ));
            }
            changed = changes.changed() => {
                if changed.is_err() {
                    return Err(attempt_failure(
                        QueryFailureReason::ObservationLost,
                        "Native replacement topology owner closed",
                    ));
                }
            }
        }
    }
}

fn topology_satisfies_requirement(
    snapshot: &novarocks_query_application::api::BackendTopologySnapshot,
    requirement: novarocks_query_application::api::NativeAttemptTopologyRequirement,
) -> bool {
    match requirement {
        novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot => true,
        novarocks_query_application::api::NativeAttemptTopologyRequirement::ExcludeProcess(
            process,
        ) => snapshot.targets().iter().any(|target| {
            target
                .process_id()
                .is_ok_and(|candidate| candidate != process)
        }),
    }
}

fn capture_attempt_topology(
    topology: &BackendTopologyService,
) -> Result<novarocks_query_application::api::BackendTopologySnapshot, NativeAttemptPreparationError>
{
    topology.snapshot().map_err(|error| {
        attempt_failure(
            QueryFailureReason::ObservationLost,
            format!("failed to capture Native attempt topology: {error}"),
        )
    })
}

fn query_cancellation_reason(
    reason: &novarocks_workload_control::CancellationReason,
) -> QueryFailureReason {
    match reason {
        novarocks_workload_control::CancellationReason::DeadlineExceeded
        | novarocks_workload_control::CancellationReason::FrontendDrainDeadlineExceeded => {
            QueryFailureReason::DeadlineExceeded
        }
        _ => QueryFailureReason::Cancelled,
    }
}
fn attempt_failure(
    reason: QueryFailureReason,
    diagnostic: impl std::fmt::Display,
) -> NativeAttemptPreparationError {
    attempt_runtime_failure(reason, diagnostic).into()
}
fn attempt_runtime_failure(
    reason: QueryFailureReason,
    diagnostic: impl std::fmt::Display,
) -> NativeAttemptPreparationFailure {
    tracing::debug!(reason = ?reason, error = %diagnostic, "Native attempt preparation failed");
    // These are this owner's reasons. Foreign display strings remain private
    // diagnostics and never supply machine facts or a client-safe promise.
    let safe = match reason {
        QueryFailureReason::Cancelled => "logical execution was cancelled during preparation",
        QueryFailureReason::DeadlineExceeded => {
            "logical execution deadline expired during preparation"
        }
        QueryFailureReason::PreparationProtocol => "native attempt preparation contract failed",
        QueryFailureReason::ObservationLost => {
            "native attempt topology observation became unavailable"
        }
        _ => "native attempt preparation failed",
    };
    NativeAttemptPreparationFailure::from_input(ExecutionFailureInput::new(
        reason.failure_fact(),
        SafeDetail::truncating(safe),
        ExecutionFailureSite::Coordinator,
    ))
}

#[cfg(test)]
mod replacement_ack_tests {
    use super::*;
    use std::future::Future;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Context, Poll, Wake, Waker};

    struct AckWake(AtomicUsize);
    impl Wake for AckWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn replacement_ack_reuses_its_registered_waiter_for_early_and_late_real_intake_facts() {
        for early in [false, true] {
            let notify = Arc::new(tokio::sync::Notify::new());
            let wake = Arc::new(NotifyWake::new(Arc::clone(&notify))) as Arc<dyn StatusIntakeWake>;
            let acks = TaskAckIntake::new(wake);
            let operation = TaskOperationId::new_v7();
            // This is the same still-live registration made before the real
            // queue/submit stage. It must not steal the sole ACK notification.
            let original_waiter = notify.notified();
            tokio::pin!(original_waiter);
            original_waiter.as_mut().enable();
            let publish = || {
                acks.handle().publish(crate::task_execution::intent::OperationAcknowledgement::new(
                    operation,
                    novarocks_execution_contract::OperationKind::AcquireQueryContextAdmissionTicket,
                    OperationOutcome::ResourceExhausted,
                    AckPayload::None,
                ));
            };
            if early {
                publish();
            }
            let (_cancellation, mut cancellation) = watch::channel(false);
            let wait = wait_for_replacement_acknowledgement(
                &acks,
                &notify,
                original_waiter.as_mut(),
                &mut cancellation,
                operation,
            );
            tokio::pin!(wait);
            let wake = Arc::new(AckWake(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wake));
            let mut cx = Context::from_waker(&waker);
            if !early {
                assert!(matches!(wait.as_mut().poll(&mut cx), Poll::Pending));
                publish();
                assert!(
                    wake.0.load(Ordering::SeqCst) > 0,
                    "the real ACK must wake the actively awaited original registration"
                );
            }
            let Poll::Ready(Ok(ack)) = wait.as_mut().poll(&mut cx) else {
                panic!("a published ACK must not remain stranded behind a dormant waiter");
            };
            assert_eq!(ack.operation_id(), operation);
            assert_eq!(
                ack.worker_outcome(),
                Some(OperationOutcome::ResourceExhausted)
            );
            assert_eq!(acks.queued(), 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use futures::FutureExt;
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_query_application::api::{
        ActiveNativeAttemptOwner, CancellationView, NativeActiveAttemptConvergenceFuture,
        NativeActiveConvergenceClosure, NativeActiveConvergenceRequest,
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
        AttemptFailureClass, FrontendActiveAttemptBehavior, FrontendDormantAttemptBehavior,
        QueryExecutionErrorKind, RetainedAttemptInputs, SnapshotBoundDormantAttemptOwner,
        attempt_runtime_failure, failed_context_allows_successor,
        wait_for_failed_context_isolation,
    };
    use novarocks_execution::task_execution::AdmissionEpochCapability;
    use novarocks_execution_contract::BackendProcessDescriptor;
    use novarocks_query_application::api::{
        BackendProcessObservation, BackendProcessObservationPort, BackendProcessObservationService,
        BackendTopologyError, BackendTopologyPort, BackendTopologyService, BackendTopologySnapshot,
        BackendTopologyValidationError, LiveBackendTarget,
    };
    use novarocks_types::NativeCompatibilityId;

    #[derive(Debug)]
    struct FixedLiveTopology {
        snapshot: std::sync::Mutex<BackendTopologySnapshot>,
        changes: tokio::sync::watch::Sender<u64>,
    }

    impl BackendTopologyPort for FixedLiveTopology {
        fn snapshot(&self) -> Result<BackendTopologySnapshot, BackendTopologyError> {
            Ok(self.snapshot.lock().unwrap().clone())
        }

        fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
            self.changes.subscribe()
        }

        fn validate_snapshot(
            &self,
            expected: &BackendTopologySnapshot,
        ) -> Result<(), BackendTopologyValidationError> {
            assert_eq!(expected, &*self.snapshot.lock().unwrap());
            Ok(())
        }

        fn wait_for_eligible_after(
            &self,
            _revision: u64,
            _deadline: Instant,
        ) -> Result<BackendTopologySnapshot, BackendTopologyError> {
            unreachable!("attempt preparation captures a live snapshot directly")
        }

        fn record_successful_stage(&self, _backend_idx: usize, _fragment_count: usize) {}
    }

    #[tokio::test]
    async fn replacement_waits_for_other_capacity_without_rewriting_membership() {
        use novarocks_query_application::api::NativeAttemptTopologyRequirement;
        let failed = BackendProcessId::new_v7();
        let remaining = BackendProcessId::new_v7();
        let target = |index, process| {
            LiveBackendTarget::new(
                index,
                BackendProcessDescriptor::try_new(
                    process,
                    RuntimeEndpoint::new("127.0.0.1", 19100 + index as i32).unwrap(),
                    RuntimeEndpoint::new(format!("control-{index}.test.invalid"), 19061).unwrap(),
                    "test-deployment",
                    "test-build",
                    NativeCompatibilityId::new([0x71; 32]),
                    8,
                )
                .unwrap(),
                AdmissionEpochCapability::try_from_bytes([index as u8 + 1; 16]).unwrap(),
            )
        };
        let (changes, _) = tokio::sync::watch::channel(7);
        let mutable = Arc::new(FixedLiveTopology {
            snapshot: std::sync::Mutex::new(
                BackendTopologySnapshot::try_new(7, vec![target(0, failed)]).unwrap(),
            ),
            changes,
        });
        let topology: BackendTopologyService = mutable.clone();
        let (_control, root, cancellation) = governed_cancellation();
        {
            let wait = super::wait_for_attempt_topology(
                &topology,
                NativeAttemptTopologyRequirement::ExcludeProcess(failed),
                &cancellation,
            );
            tokio::pin!(wait);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut wait)
                    .await
                    .is_err()
            );
            // Losing all eligible targets cannot produce a schedulable replacement.
            *mutable.snapshot.lock().unwrap() = BackendTopologySnapshot::empty(8);
            mutable.changes.send_replace(8);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut wait)
                    .await
                    .is_err()
            );
            // No restart is required: remaining live capacity can admit the successor.
            *mutable.snapshot.lock().unwrap() =
                BackendTopologySnapshot::try_new(9, vec![target(0, failed), target(1, remaining)])
                    .unwrap();
            mutable.changes.send_replace(9);
            let snapshot = tokio::time::timeout(Duration::from_secs(1), &mut wait)
                .await
                .unwrap()
                .unwrap();
            assert!(
                snapshot
                    .targets()
                    .iter()
                    .any(|target| target.process_id() == Ok(failed)),
                "membership remains the owner's complete snapshot"
            );
            assert!(
                snapshot
                    .targets()
                    .iter()
                    .any(|target| target.process_id() == Ok(remaining))
            );
        }
        *mutable.snapshot.lock().unwrap() =
            BackendTopologySnapshot::try_new(10, vec![target(1, remaining)]).unwrap();
        mutable.changes.send_replace(10);
        root.owner.cancel(CancellationReason::ClientDisconnected);
        let cancelled = tokio::time::timeout(
            Duration::from_secs(1),
            super::wait_for_attempt_topology(
                &topology,
                NativeAttemptTopologyRequirement::ExcludeProcess(remaining),
                &cancellation,
            ),
        )
        .await
        .unwrap();
        assert!(cancelled.is_err());
    }

    #[tokio::test]
    async fn replacement_topology_wait_preserves_deadline_failure() {
        let (changes, _) = tokio::sync::watch::channel(1);
        let topology: BackendTopologyService = Arc::new(FixedLiveTopology {
            snapshot: std::sync::Mutex::new(BackendTopologySnapshot::empty(1)),
            changes,
        });
        let (_control, root, cancellation) = governed_cancellation();
        root.owner.cancel(CancellationReason::DeadlineExceeded);
        let error = super::wait_for_attempt_topology(
            &topology,
            novarocks_query_application::api::NativeAttemptTopologyRequirement::ExcludeProcess(
                BackendProcessId::new_v7(),
            ),
            &cancellation,
        )
        .await
        .unwrap_err();
        let NativeAttemptPreparationError::Runtime(failure) = error else {
            panic!("runtime deadline failure expected")
        };
        assert_eq!(failure.class(), AttemptFailureClass::DeadlineExceeded);
        assert_eq!(
            failure.error().kind(),
            QueryExecutionErrorKind::DeadlineExceeded
        );
    }

    #[tokio::test]
    async fn catalog_establish_failure_can_prepare_on_the_same_three_live_processes() {
        let processes: Vec<_> = (0..3).map(|_| BackendProcessId::new_v7()).collect();
        let targets = processes
            .iter()
            .enumerate()
            .map(|(index, process)| {
                let descriptor = BackendProcessDescriptor::try_new(
                    *process,
                    RuntimeEndpoint::new("127.0.0.1", 19050 + index as i32).expect("test endpoint"),
                    RuntimeEndpoint::new(format!("control-{index}.test.invalid"), 19061)
                        .expect("test control endpoint"),
                    "test-deployment",
                    "test-build",
                    NativeCompatibilityId::new([0x71; 32]),
                    8,
                )
                .expect("test descriptor");
                LiveBackendTarget::new(
                    index,
                    descriptor,
                    AdmissionEpochCapability::try_from_bytes([index as u8 + 1; 16])
                        .expect("test admission epoch"),
                )
            })
            .collect();
        let snapshot = BackendTopologySnapshot::try_new(7, targets).expect("live topology");
        let (changes, _) = tokio::sync::watch::channel(snapshot.revision());
        let topology: BackendTopologyService = Arc::new(FixedLiveTopology {
            snapshot: std::sync::Mutex::new(snapshot),
            changes,
        });

        let first = super::capture_attempt_topology(&topology).expect("first attempt topology");
        // A catalog Establish rejection changes no process identity or revision.
        let (_control, _root, cancellation) = governed_cancellation();
        let replacement = tokio::time::timeout(
            Duration::from_millis(100),
            super::wait_for_attempt_topology(
                &topology,
                novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot,
                &cancellation,
            ),
        )
        .await
        .expect("replacement preparation must not wait for membership")
        .expect("replacement attempt topology");
        assert_eq!(replacement.revision(), first.revision());
        assert_eq!(replacement.targets().len(), 3);
        assert_eq!(
            replacement
                .targets()
                .iter()
                .map(|target| target.process_id().expect("test process identity"))
                .collect::<Vec<_>>(),
            processes,
        );

        let old_context = novarocks_execution_contract::QueryContextRef::new(
            QueryExecutionId::new(
                QueryId::new(0x4567, 0x89ab),
                AttemptId::new(1).expect("attempt one"),
            )
            .expect("test execution identity"),
            FrontendProcessId::new_v7(),
            processes[0],
        );
        let endpoint = first.targets()[0].endpoint().expect("frozen endpoint");
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
        let qualification = wait_for_failed_context_isolation(
            &observation,
            old_context,
            &endpoint,
            &closed,
            &mut closure_changes,
            &mut process_changes,
            &mut cancellation,
            Instant::now() + Duration::from_secs(1),
        );
        tokio::pin!(qualification);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), &mut qualification)
                .await
                .is_err(),
            "the same live process must fence the old context before replacement"
        );
        closed.lock().expect("closed contexts").insert(old_context);
        closure_epoch.send_modify(|epoch| *epoch += 1);
        tokio::time::timeout(Duration::from_millis(100), &mut qualification)
            .await
            .expect("exact closure wakes qualification")
            .expect("closed old context permits replacement on unchanged topology");
    }

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

        #[cfg(novarocks_dev_controls)]
        fn publish_fault_task_selection(
            _: &Self::Manifest,
            _: &novarocks_native_adapter::test_controls::NativeTestControls,
            _: &novarocks_failpoint::target_binding::StatementWorkNomination,
        ) -> Result<(), NativeAttemptActivationFailure> {
            panic!("eligibility-only fixture cannot publish writer Task selection")
        }

        fn validate_bound_schedule(
            _manifest: &Self::Manifest,
            _schedule: &AttemptSchedule,
        ) -> Result<(), NativeAttemptActivationFailure> {
            // This eligibility-only fixture does not model manifest placement.
            Ok(())
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            &self.eligible_backends
        }

        fn manifest_eligible_backends(manifest: &Self::Manifest) -> &[BackendProcessId] {
            &manifest.eligible_backends
        }

        fn bind_manifest(
            self,
            _schedule: &AttemptSchedule,
        ) -> Result<Self::Manifest, NativeAttemptActivationFailure> {
            Ok(TestManifest {
                eligible_backends: self.eligible_backends,
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

    impl<M: std::fmt::Debug + Send + 'static> FrontendActiveAttemptBehavior<M> for NeverActiveBehavior {
        fn converge_unstarted<'a>(
            &'a mut self,
            _: &'a mut M,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async { panic!("the adversarial eligibility fixture never activates") })
        }

        fn run<'a>(
            &'a mut self,
            _inputs: &'a mut M,
            _drive: &'a NativeAttemptDrive,
            _cancellation: CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { panic!("adversarial eligibility test must not run") })
        }

        fn converge<'a>(
            &'a mut self,
            _inputs: &'a mut M,
            _request: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async {
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }

    impl<M: std::fmt::Debug + Send + 'static> FrontendDormantAttemptBehavior<M>
        for AdversarialBehavior
    {
        fn poll_bind_qualification(
            &mut self,
            _inputs: &mut M,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), NativeAttemptActivationFailure>> {
            panic!("the adversarial eligibility fixture never qualifies")
        }

        type ActiveBehavior = NeverActiveBehavior;

        fn activate<'a>(
            &'a mut self,
            _inputs: &'a mut M,
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

        fn converge<'a>(&'a mut self, _inputs: &'a mut M) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn adversarial_behavior_cannot_replace_snapshot_derived_eligible_backends() {
        let captured = BackendProcessId::new_v7();
        let replacement = BackendProcessId::new_v7();
        let owner = SnapshotBoundDormantAttemptOwner::new(
            CapturedInputs {
                eligible_backends: vec![captured],
            },
            AdversarialBehavior {
                replacement_backends: vec![replacement],
            },
        );

        assert_eq!(owner.retained_eligible_backends(), &[captured]);
        assert_eq!(owner.behavior.replacement_backends, vec![replacement]);
    }

    #[test]
    fn bound_dormant_owner_borrows_original_manifest_eligibility() {
        let captured = BackendProcessId::new_v7();
        let replacement = BackendProcessId::new_v7();
        let eligible_backends = vec![captured];
        let original = eligible_backends.as_ptr();
        let owner = SnapshotBoundDormantAttemptOwner::<_, CapturedInputs>::from_manifest(
            TestManifest {
                eligible_backends,
                marker: 73,
                identity: Box::new(91),
            },
            AdversarialBehavior {
                replacement_backends: vec![replacement],
            },
        );
        assert!(owner.dormant_inputs.is_none());
        assert_eq!(owner.retained_eligible_backends(), &[captured]);
        assert_eq!(owner.retained_eligible_backends().as_ptr(), original);
        assert_eq!(owner.behavior.replacement_backends, vec![replacement]);
    }

    #[derive(Debug, Eq, PartialEq)]
    struct TestManifest {
        eligible_backends: Vec<BackendProcessId>,
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
        fn converge_unstarted<'a>(
            &'a mut self,
            inputs: &'a mut TestManifest,
        ) -> NativeAttemptConvergenceFuture<'a> {
            assert_eq!(inputs.marker, 73);
            assert_eq!(
                inputs.identity.as_ref() as *const u8 as usize,
                self.identity_address
            );
            let observed = Arc::clone(&self.convergence_observed);
            Box::pin(async move {
                observed.store(true, Ordering::SeqCst);
                Ok(())
            })
        }

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
            _request: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            assert_eq!(inputs.marker, 73);
            assert_eq!(
                inputs.identity.as_ref() as *const u8 as usize,
                self.identity_address
            );
            let observed = Arc::clone(&self.convergence_observed);
            Box::pin(async move {
                observed.store(true, Ordering::SeqCst);
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }

    impl FrontendDormantAttemptBehavior<TestManifest> for LifecycleBehavior {
        fn poll_bind_qualification(
            &mut self,
            _inputs: &mut TestManifest,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), NativeAttemptActivationFailure>> {
            // This lifecycle fixture owns no external dependency.
            std::task::Poll::Ready(Ok(()))
        }
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
                            super::QueryFailureReason::PreparationFailed,
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
        ) -> NativeAttemptConvergenceFuture<'a> {
            assert_eq!(inputs.marker, 73);
            let observed = Arc::clone(&self.convergence_observed);
            Box::pin(async move {
                observed.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    #[test]
    fn frozen_root_carrier_requires_its_exact_admitted_window_class() {
        use super::ProductionRootDelivery;
        use novarocks_query_application::api::ResultRowCarrier as C;
        use novarocks_result_contract::{
            ClientRowProfile, InternalResultDomain, RootOutputKind as K, RootProfileV1 as P,
        };
        use novarocks_workload_control::{ResultCapacityConfig, ResultWindowClass as W};
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 10,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        let profile = ClientRowProfile::try_new(P::SEGMENT_BYTES, P::ROW_PAYLOAD_BYTES).unwrap();
        for kind in [
            K::ClientRows,
            K::InternalFacts(InternalResultDomain::ScalarValueV1),
            K::CountOnly,
        ] {
            let carrier = C::relayed(kind, (kind == K::ClientRows).then_some(profile)).unwrap();
            assert!(ProductionRootDelivery::bind(carrier, None).is_err());
            for class in [W::Client, W::Local, W::Internal, W::Closing] {
                let grant = if class == W::Closing {
                    capacity
                        .try_acquire_closing(
                            &scope,
                            novarocks_workload_control::ResultClosingCut::OriginatingFailure,
                        )
                        .unwrap()
                } else {
                    capacity.try_acquire(&scope, class).unwrap()
                };
                let expected = if kind == K::ClientRows {
                    W::Client
                } else {
                    W::Internal
                };
                let bound = ProductionRootDelivery::bind(carrier, Some(grant.retain_alias()));
                assert_eq!(bound.is_ok(), class == expected);
                if let Ok(bound) = bound {
                    drop(grant);
                    assert_eq!(capacity.snapshot().held_positions.iter().sum::<usize>(), 1);
                    drop(bound);
                    assert_eq!(capacity.snapshot().held_positions.iter().sum::<usize>(), 0);
                }
            }
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
                    eligible_backends: Vec::new(),
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
        owner
            .converge_retained(cancellation)
            .await
            .expect("original dormant fixture cleanup succeeds");
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
        ActiveNativeAttemptOwner::converge(
            &mut active,
            NativeActiveConvergenceRequest::new(
                cancellation,
                Instant::now() + Duration::from_secs(5),
            ),
        )
        .await
        .expect("the original active fixture joins successfully");
        assert!(convergence_observed.load(Ordering::SeqCst));
    }

    struct OriginalRowsExtractionPanic {
        drops: Arc<AtomicU64>,
    }
    impl Drop for OriginalRowsExtractionPanic {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[derive(Debug)]
    struct ExtractionPanicDormant {
        close_gate: Arc<tokio::sync::Notify>,
        active_closes: Arc<AtomicU64>,
        active_drops: Arc<AtomicU64>,
        cause_drops: Arc<AtomicU64>,
        extractions: Arc<AtomicU64>,
        dormant_closes: Arc<AtomicU64>,
    }
    #[derive(Debug)]
    struct ExtractionPanicActive {
        close_gate: Arc<tokio::sync::Notify>,
        active_closes: Arc<AtomicU64>,
        active_drops: Arc<AtomicU64>,
        cause_drops: Arc<AtomicU64>,
        extractions: Arc<AtomicU64>,
    }
    impl Drop for ExtractionPanicActive {
        fn drop(&mut self) {
            self.active_drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl FrontendActiveAttemptBehavior<TestManifest> for ExtractionPanicActive {
        fn converge_unstarted<'a>(
            &'a mut self,
            inputs: &'a mut TestManifest,
        ) -> NativeAttemptConvergenceFuture<'a> {
            assert_eq!(inputs.marker, 73);
            let gate = Arc::clone(&self.close_gate);
            let closes = Arc::clone(&self.active_closes);
            Box::pin(async move {
                gate.notified().await;
                closes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }

        fn take_rows_runtime(
            &mut self,
        ) -> Option<(
            super::FrontendRootRowsRuntime,
            super::AcceptedRootStatusSource,
        )> {
            self.extractions.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(OriginalRowsExtractionPanic {
                drops: Arc::clone(&self.cause_drops),
            });
        }
        fn run<'a>(
            &'a mut self,
            _inputs: &'a mut TestManifest,
            _drive: &'a NativeAttemptDrive,
            _cancellation: CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { panic!("the extraction-failure fixture must not dispatch") })
        }
        fn converge<'a>(
            &'a mut self,
            _inputs: &'a mut TestManifest,
            _request: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async { panic!("no active close receipt is invented by this fixture") })
        }
    }
    impl FrontendDormantAttemptBehavior<TestManifest> for ExtractionPanicDormant {
        fn poll_bind_qualification(
            &mut self,
            _inputs: &mut TestManifest,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), NativeAttemptActivationFailure>> {
            // This lifecycle fixture owns no external dependency.
            std::task::Poll::Ready(Ok(()))
        }
        type ActiveBehavior = ExtractionPanicActive;
        fn activate<'a>(
            &'a mut self,
            inputs: &'a mut TestManifest,
            _admissions: Option<Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>>,
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
                Ok(ExtractionPanicActive {
                    close_gate: Arc::clone(&self.close_gate),
                    active_closes: Arc::clone(&self.active_closes),
                    active_drops: Arc::clone(&self.active_drops),
                    cause_drops: Arc::clone(&self.cause_drops),
                    extractions: Arc::clone(&self.extractions),
                })
            })
        }
        fn converge<'a>(
            &'a mut self,
            _inputs: &'a mut TestManifest,
        ) -> NativeAttemptConvergenceFuture<'a> {
            self.dormant_closes.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }
    #[tokio::test]
    async fn real_rows_extraction_panic_keeps_returned_active_and_manifest_in_original_owner() {
        let (_control, root, cancellation) = governed_cancellation();
        let active_drops = Arc::new(AtomicU64::new(0));
        let cause_drops = Arc::new(AtomicU64::new(0));
        let extractions = Arc::new(AtomicU64::new(0));
        let dormant_closes = Arc::new(AtomicU64::new(0));
        let close_gate = Arc::new(tokio::sync::Notify::new());
        let active_closes = Arc::new(AtomicU64::new(0));
        let mut owner = SnapshotBoundDormantAttemptOwner::<_, CapturedInputs>::from_manifest(
            TestManifest {
                eligible_backends: vec![],
                marker: 73,
                identity: Box::new(91),
            },
            ExtractionPanicDormant {
                close_gate: Arc::clone(&close_gate),
                active_closes: Arc::clone(&active_closes),
                active_drops: Arc::clone(&active_drops),
                cause_drops: Arc::clone(&cause_drops),
                extractions: Arc::clone(&extractions),
                dormant_closes: Arc::clone(&dormant_closes),
            },
        );
        let original_identity =
            owner.manifest_inputs.as_ref().unwrap().identity.as_ref() as *const u8 as usize;
        let outcome =
            std::panic::AssertUnwindSafe(owner.activate_fixed_retained(None, cancellation.clone()))
                .catch_unwind()
                .await;
        let original_cause = match outcome {
            Err(original) => Some(original),
            Ok(result) => {
                drop(result);
                None
            }
        };
        let exact_cause = original_cause
            .as_ref()
            .is_some_and(|cause| cause.is::<OriginalRowsExtractionPanic>());
        let manifest_retained = owner.manifest_inputs.as_ref().is_some_and(|manifest| {
            manifest.identity.as_ref() as *const u8 as usize == original_identity
        });
        let active_retained = owner.pending_active_behavior.is_some();
        let mut close = owner.converge_retained(cancellation.clone());
        let close_pending = futures::poll!(&mut close).is_pending();
        drop(close);
        assert!(owner.pending_active_behavior.is_some());
        assert_eq!(active_closes.load(Ordering::SeqCst), 0);
        assert_eq!(active_drops.load(Ordering::SeqCst), 0);
        close_gate.notify_one();
        owner.converge_retained(cancellation).await.unwrap();
        assert!(owner.pending_active_behavior.is_none());
        assert!(owner.manifest_inputs.is_some());
        assert_eq!(active_closes.load(Ordering::SeqCst), 1);
        let before = (
            active_drops.load(Ordering::SeqCst),
            cause_drops.load(Ordering::SeqCst),
        );
        // This explicit fixture-owner disposition is not a normal Native close
        // receipt, WorkerStopped evidence or a profile qualification.
        drop(owner);
        drop(original_cause);
        root.owner.complete();
        root.business.release();
        assert!(exact_cause && manifest_retained && active_retained && close_pending);
        assert_eq!(before, (1, 0));
        assert_eq!(extractions.load(Ordering::SeqCst), 1);
        assert_eq!(dormant_closes.load(Ordering::SeqCst), 1);
        assert_eq!(active_drops.load(Ordering::SeqCst), 1);
        assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod production_failure_projection_tests {
    use super::*;

    #[test]
    fn original_production_capacity_refusal_preserves_resource_fact_and_safe_client_detail() {
        let original = crate::query_execution::write_session::production_work_error_for_test(
            novarocks_workload_control::WorkError::Capacity(
                "private original production capacity diagnostic",
            ),
        );
        let fact = original.failure_fact();
        let projected = write_production_failure(original);
        let input = projected.failure().input();
        assert_eq!(input.fact(), fact);
        assert_eq!(
            projected.failure().class(),
            AttemptFailureClass::ResourceGovernance
        );
        assert_ne!(
            input.fact(),
            QueryFailureReason::PreparationProtocol.failure_fact()
        );
        assert_eq!(
            input.detail(),
            &SafeDetail::truncating("native write production responsibility was refused")
        );
        assert_eq!(
            input.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
    }
}
