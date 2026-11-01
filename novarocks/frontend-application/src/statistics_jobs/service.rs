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
// software distributed under the Apache License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Frontend adapters for the statistics application product owner.
//!
//! This module translates typed frontend commands and connector target
//! captures. It deliberately owns neither a job ledger nor business attempt
//! orchestration. Role composition starts the product-owned job runtime with
//! the concrete attempt adapter; without the required bindings the entrypoint
//! fails closed.

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use super::application;
use novarocks_statistics_application::{
    StatisticsAttemptExecutor, StatisticsColumns, StatisticsJob, StatisticsJobAdmission,
    StatisticsJobCreate, StatisticsJobId, StatisticsJobRuntime, StatisticsJobService,
    StatisticsTarget,
};
use novarocks_workload_control::{PendingQueryRoot, RootAdmissionHandle, WorkClass, WorkRequest};

/// T12's role composition supplies a root for each independently-owned
/// background statistics business request. There is intentionally no default
/// implementation: borrowing the statement observer would let observer
/// cancellation destroy the job root.
pub trait StatisticsJobRootScopeSource: Send + Sync {
    fn begin_statistics_job(&self) -> Result<PendingQueryRoot, String>;
}

/// Role-local source for independently-owned ANALYZE jobs.
///
/// The Server injects only the narrow root-admission capability.  A submitted
/// job retains the returned owner in the statistics application repository;
/// the admission's transient business permit is deliberately released after
/// it has become that durable process-local responsibility.
#[derive(Clone)]
pub struct RootAdmissionStatisticsJobSource {
    admission: RootAdmissionHandle,
}

impl RootAdmissionStatisticsJobSource {
    pub fn new(admission: RootAdmissionHandle) -> Self {
        Self { admission }
    }
}

impl StatisticsJobRootScopeSource for RootAdmissionStatisticsJobSource {
    fn begin_statistics_job(&self) -> Result<PendingQueryRoot, String> {
        self.admission
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .map_err(|error| error.to_string())
    }
}

pub struct FrontendStatisticsApplicationPort {
    job_runtime: Arc<StatisticsJobRuntime>,
    target_resolver: Arc<dyn application::StatisticsTargetResolver>,
    root_scope: Arc<dyn StatisticsJobRootScopeSource>,
    runtime: tokio::runtime::Handle,
    blocking: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
}
impl FrontendStatisticsApplicationPort {
    pub(crate) fn new(
        job_service: StatisticsJobService,
        target_resolver: Arc<dyn application::StatisticsTargetResolver>,
        root_scope: Arc<dyn StatisticsJobRootScopeSource>,
        core_executor: Arc<dyn StatisticsAttemptExecutor>,
        runtime: tokio::runtime::Handle,
        blocking: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
    ) -> Self {
        Self {
            job_runtime: Arc::new(StatisticsJobRuntime::start(
                job_service,
                core_executor,
                runtime.clone(),
            )),
            target_resolver,
            root_scope,
            runtime,
            blocking,
        }
    }
    pub async fn shutdown_worker_until(&self, _deadline: Instant) -> Result<(), String> {
        self.job_runtime.shutdown_until(_deadline).await
    }
    pub fn request_worker_stop_for_process_exit(&self) {
        self.job_runtime.request_stop_for_process_exit();
    }
}

impl application::StatisticsApplicationPort for FrontendStatisticsApplicationPort {
    fn prepare_statement_action(
        &self,
        command: application::StatisticsApplicationCommand,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        context: &novarocks_spi::connector::ConnectorRequestContext,
        family: novarocks_result_contract::ClientPresentationFamily,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        application::StatisticsApplicationError,
    > {
        use crate::query_execution::maintenance::io_admission::MetadataIoAdmission;
        let command = match command {
            application::StatisticsApplicationCommand::ShowTableStats { target } => {
                return self.prepare_read_action(target, execution, context, family);
            }
            command => command,
        };
        let application::StatisticsApplicationCommand::AnalyzeTable { target, columns } = command
        else {
            return Err(application::StatisticsApplicationError::new(
                "only ANALYZE has a statistics submission source",
            ));
        };
        let fail = |error: novarocks_workload_control::WorkError| {
            application::StatisticsApplicationError::new(error.to_string())
        };
        let capacity = execution.result_capacity().ok_or_else(|| {
            application::StatisticsApplicationError::new(
                "statistics submission has no original capacity",
            )
        })?;
        let origin = capacity.try_window_alias().map_err(fail)?;
        let scope = capacity.scope().clone();
        let planning = self.target_resolver.planning_lease(&target)?;
        let lease = planning
            .derive_statistics_lease()
            .map_err(application::StatisticsApplicationError::from_connector_error)?;
        let profile = lease
            .collection_io_profile()
            .map_err(application::StatisticsApplicationError::from_connector_error)?;
        // Original inline Source declaration only; it is neither an SDK
        // envelope nor a reservation of the statement's result payload bytes.
        let metadata = std::mem::size_of::<StatisticsSubmitAction>() as u64;
        let io = MetadataIoAdmission::new(scope, profile, metadata).map_err(fail)?;
        let context = context.clone().with_io_admission(io.clone());
        Ok(Box::new(StatisticsSubmitAction {
            io,
            origin,
            input: Some(StatisticsSubmitInput {
                runtime: self.job_runtime.clone(),
                resolver: self.target_resolver.clone(),
                roots: self.root_scope.clone(),
                target,
                columns,
                context,
                planning,
                lease,
                #[cfg(novarocks_dev_controls)]
                nomination: execution.test_statement_nomination().cloned(),
            }),
            cpu: None,
            blocking: self.blocking.clone(),
            runtime: self.runtime.clone(),
            cancellation: execution.cancellation().clone(),
            wake: Arc::new(StatisticsSubmitWake::default()),
            preparation: None,
            prepared: None,
            admission: None,
            future: None,
            result: None,
            capture_closed: false,
            started: false,
            closed: false,
            io_close_failure: None,
        }))
    }

    fn execute<'a>(
        &'a self,
        command: application::StatisticsApplicationCommand,
        _execution: Option<
            &'a novarocks_query_application::admitted_query_context::QueryExecutionContext,
        >,
    ) -> std::pin::Pin<
        Box<
            dyn Future<
                    Output = Result<
                        application::StatisticsApplicationResult,
                        application::StatisticsApplicationError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(self.execute_local_command(command))
    }
}

impl FrontendStatisticsApplicationPort {
    async fn execute_local_command(
        &self,
        command: application::StatisticsApplicationCommand,
    ) -> Result<application::StatisticsApplicationResult, application::StatisticsApplicationError>
    {
        match command {
            application::StatisticsApplicationCommand::ShowAnalyzeJobs => self
                .job_runtime
                .list()
                .await
                .map(|jobs| {
                    application::StatisticsApplicationResult::AnalyzeJobs(
                        jobs.into_iter().map(job_view).collect(),
                    )
                })
                .map_err(|e| application::StatisticsApplicationError::new(e.to_string())),
            application::StatisticsApplicationCommand::CancelAnalyze { job_id } => {
                let at = now_ms().map_err(application::StatisticsApplicationError::new)?;
                self.job_runtime
                    .request_cancel(StatisticsJobId::from_uuid(job_id), at)
                    .await
                    .map(|job| {
                        application::StatisticsApplicationResult::JobCancellationRequested(
                            job_view(job),
                        )
                    })
                    .map_err(|e| application::StatisticsApplicationError::new(e.to_string()))
            }
            application::StatisticsApplicationCommand::AnalyzeTable { .. }
            | application::StatisticsApplicationCommand::ShowTableStats { .. } => {
                Err(application::StatisticsApplicationError::new(
                    "table statistics commands require their original governed statement source",
                ))
            }
        }
    }
}

fn job_view(job: StatisticsJob) -> application::StatisticsJobView {
    let (error_message, error_kind) = if let Some(failure) = &job.publication_failure {
        (Some(failure.public_message()), Some("PUBLICATION"))
    } else if let Some(failure) = &job.publication_finalization_failure {
        (Some(failure.public_message()), Some("FINALIZATION"))
    } else if let Some(failure) = &job.failure {
        (Some(failure.message.to_string()), Some("STATISTICS"))
    } else {
        (
            job.publication_cleanup_failure
                .as_ref()
                .map(|failure| failure.public_message()),
            job.publication_cleanup_failure.as_ref().map(|_| "CLEANUP"),
        )
    };
    application::StatisticsJobView {
        job_id: job.id.as_uuid(),
        operation_id: novarocks_spi::connector::LakePublicationId::try_from_uuid(
            job.publication_id.as_uuid(),
        )
        .expect("statistics publication IDs are UUIDv7"),
        state: match job.state {
            novarocks_statistics_application::StatisticsJobState::Active(phase) => {
                format!("{phase:?}")
            }
            novarocks_statistics_application::StatisticsJobState::Terminal(conclusion) => {
                format!("{conclusion:?}")
            }
        }
        .to_ascii_uppercase(),
        attempt: u32::from(job.query_attempt_id.is_some()),
        target: application::StatisticsTableTarget {
            catalog: job.target.catalog.to_string(),
            namespace: job.target.namespace.to_string(),
            table: job.target.table.to_string(),
        },
        error_kind: error_kind.map(str::to_owned),
        error_message,
    }
}
fn now_ms() -> Result<i64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_millis()
        .try_into()
        .map_err(|_| "statistics time exceeds i64 milliseconds".into())
}

// Design: ADR-0169 (docs/adr/ADR-0169-read-only-hms-and-single-writer-admission.md)
async fn await_statistics_conclusion(
    runtime: &StatisticsJobRuntime,
    id: StatisticsJobId,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatisticsJob, application::StatisticsApplicationError> {
    await_statistics_conclusion_with_clock(runtime, id, context, now_ms).await
}

async fn await_statistics_conclusion_with_clock(
    runtime: &StatisticsJobRuntime,
    id: StatisticsJobId,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    clock: impl Fn() -> Result<i64, String>,
) -> Result<StatisticsJob, application::StatisticsApplicationError> {
    use novarocks_statistics_application::{StatisticsJobConclusion, StatisticsJobState};
    let wait = runtime.wait_for_conclusion(id);
    tokio::pin!(wait);
    let stopped = context.stop().stopped();
    tokio::pin!(stopped);
    let deadline = tokio::time::sleep_until(context.deadline().into());
    tokio::pin!(deadline);
    let mut cancelled = false;
    let mut cancellation_error = None;
    let terminal = tokio::select! {
        biased;
        terminal = &mut wait => terminal,
        _ = &mut stopped => {
            cancelled = true;
            cancellation_error = request_statistics_cancellation(runtime, id, &clock).await;
            wait.await
        },
        _ = &mut deadline => {
            cancelled = true;
            cancellation_error = request_statistics_cancellation(runtime, id, &clock).await;
            wait.await
        },
    }
    .map_err(|error| application::StatisticsApplicationError::new(error.to_string()))?;
    // The product already selected this business result. A late observer stop
    // or cancellation-request failure cannot revoke a committed success.
    if terminal.state == StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded) {
        return Ok(terminal);
    }
    if let Some(error) = cancellation_error {
        return Err(
            application::StatisticsApplicationError::from_statistics_job(&terminal)
                .with_observer_message(error),
        );
    }
    if cancelled {
        return Err(
            application::StatisticsApplicationError::from_statistics_job(&terminal)
                .with_observer_message(
                    "ANALYZE statement cancelled; statistics job business result is available",
                ),
        );
    }
    Err(application::StatisticsApplicationError::from_statistics_job(&terminal))
}

/// Legacy admission fixture: production transfers the original SQL permit.
#[cfg(test)]
/// Stop-aware query admission for a job root that has not been submitted yet.
/// Dropping the admission releases its queue/grant before the root completes.
async fn admit_statistics_job_root(
    root: PendingQueryRoot,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    novarocks_statistics_application::StatisticsJobOriginals,
    application::StatisticsApplicationError,
> {
    use novarocks_workload_control::CancellationReason;
    let admission = match root.owner.scope().admit_query() {
        Ok(admission) => admission,
        Err(error) => {
            root.owner.complete();
            return Err(application::StatisticsApplicationError::new(
                error.to_string(),
            ));
        }
    };
    let permit = {
        tokio::pin!(admission);
        tokio::select! {
            biased;
            _ = context.stop().stopped() => {
                root.owner.cancel(CancellationReason::Requested);
                Err(application::StatisticsApplicationError::new("ANALYZE cancelled before job submission"))
            },
            _ = tokio::time::sleep_until(context.deadline().into()) => {
                root.owner.cancel(CancellationReason::DeadlineExceeded);
                Err(application::StatisticsApplicationError::new("ANALYZE deadline elapsed before job submission"))
            },
            permit = &mut admission => permit.map_err(|error| application::StatisticsApplicationError::new(error.to_string())),
        }
    };
    match permit {
        Ok(permit) => {
            if let Err(error) =
                novarocks_spi::connector::ConnectorOperationControl::check_active(context)
            {
                root.owner.cancel(if context.is_cancelled() {
                    CancellationReason::Requested
                } else {
                    CancellationReason::DeadlineExceeded
                });
                drop(permit);
                root.owner.complete_after_terminal_cancel_settled();
                return Err(application::StatisticsApplicationError::new(
                    error.to_string(),
                ));
            }
            Ok(novarocks_statistics_application::StatisticsJobOriginals {
                owner: Some(root.owner),
                query_concurrency: Some(permit),
                result_capacity: None,
                root_payload: None,
                #[cfg(novarocks_dev_controls)]
                test_statement_nomination: None,
            })
        }
        Err(error) => {
            root.owner.complete_after_terminal_cancel_settled();
            Err(error)
        }
    }
}

/// No Provider work starts here. The existing statement Source owns this
/// bounded Root proof until the same admitted job slot accepts all originals.
async fn pay_queued_statistics_original(
    mut originals: novarocks_statistics_application::StatisticsJobOriginals,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    metadata_bytes: u64,
    payload_bytes: std::num::NonZeroU64,
) -> Result<
    novarocks_statistics_application::StatisticsJobOriginals,
    application::StatisticsApplicationError,
> {
    use novarocks_workload_control::joint_admission::{JointLane, JointTryBegin};
    use novarocks_workload_control::{CancellationReason, WorkError};
    use std::pin::Pin;
    use std::task::Poll;
    let scope = originals
        .owner
        .as_ref()
        .expect("original queued Root")
        .scope();
    let result = async {
        let domains = scope.prepare_root_domains()?;
        let source = domains.initial_payload_source(&scope, JointLane::Custody)?;
        let mut waiting: Option<
            novarocks_workload_control::joint_admission::JointProposalSlotWait,
        > = None;
        let mut proof: Option<
            novarocks_workload_control::joint_admission::RootDomainInitialAdmission,
        > = None;
        std::future::poll_fn(|cx| {
            scope.check()?;
            if let Some(wait) = waiting.as_mut() {
                match Pin::new(wait).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => drop(waiting.take()),
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                }
            }
            if proof.is_none() {
                match source
                    .try_begin(
                        std::num::NonZeroU64::new(metadata_bytes)
                            .ok_or(WorkError::ArithmeticOverflow)?,
                        payload_bytes,
                    )
                    .map_err(|e| e.error)?
                {
                    JointTryBegin::Started(mut proposal) => {
                        proposal.push_dependency(
                            &novarocks_spi::connector::SharedDependency::LocalHost,
                        )?;
                        proof = Some(proposal.submit().map_err(|e| e.error)?);
                    }
                    JointTryBegin::Full(mut wait) => {
                        match Pin::new(&mut wait).poll(cx) {
                            Poll::Pending => waiting = Some(wait),
                            Poll::Ready(Ok(())) => cx.waker().wake_by_ref(),
                            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        }
                        return Poll::Pending;
                    }
                }
            }
            let notice = std::task::ready!(proof.as_mut().unwrap().poll_install_in(
                cx,
                &scope,
                &mut originals.root_payload
            ))
            .map_err(|e| e.error)?;
            drop(proof.take());
            notice.notify();
            Poll::Ready(Ok(()))
        })
        .await
    };
    let result = tokio::select! {
        biased;
        _=context.stop().stopped()=>Err(WorkError::Cancelled(CancellationReason::Requested)),
        _=tokio::time::sleep_until(context.deadline().into())=>Err(WorkError::Cancelled(CancellationReason::DeadlineExceeded)),
        result=result=>result,
    };
    if let Err(error) = result {
        drop(originals.root_payload.take());
        drop(originals.result_capacity.take());
        drop(originals.query_concurrency.take());
        originals
            .owner
            .take()
            .unwrap()
            .complete_after_terminal_cancel_settled();
        return Err(application::StatisticsApplicationError::new(
            error.to_string(),
        ));
    }
    Ok(originals)
}

/// Clock failure cannot prevent cancellation or bypass actual convergence.
/// Without a new observation time, preserve the repository's existing times.
async fn request_statistics_cancellation(
    runtime: &StatisticsJobRuntime,
    id: StatisticsJobId,
    clock: &impl Fn() -> Result<i64, String>,
) -> Option<String> {
    use novarocks_statistics_application::StatisticsRepositoryErrorKind;
    let (result, clock_error) = match clock() {
        Ok(at_ms) => (runtime.request_cancel(id, at_ms).await, None),
        Err(error) => (runtime.request_cancel_without_time(id).await, Some(error)),
    };
    match result {
        Ok(_) => clock_error,
        Err(error) if error.kind() == StatisticsRepositoryErrorKind::NotFound => clock_error,
        Err(error) => Some(error.to_string()),
    }
}

/// Submission borrows the statement's real Root and keeps the original CPU
/// packet alive until its physical exit, including after observer cancellation.
struct StatisticsSubmitAction {
    io: Arc<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>,
    origin: novarocks_workload_control::ResultWindowAlias,
    input: Option<StatisticsSubmitInput>,
    cpu: Option<Arc<dyn novarocks_spi::connector::StatisticsCpuPort>>,
    blocking: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
    runtime: tokio::runtime::Handle,
    cancellation: novarocks_query_application::cancellation::QueryCancellationView,
    wake: Arc<StatisticsSubmitWake>,
    preparation: Option<StatisticsPreparationFuture>,
    prepared: Option<PreparedStatisticsSubmission>,
    admission: Option<StatisticsJobAdmission>,
    future: Option<StatisticsSubmitFuture>,
    result: Option<Result<StatisticsSubmissionOutcome, application::StatisticsApplicationError>>,
    capture_closed: bool,
    started: bool,
    closed: bool,
    io_close_failure: Option<novarocks_workload_control::WorkError>,
}
type StatisticsPreparationFuture = std::pin::Pin<
    Box<
        dyn Future<
                Output = Result<
                    PreparedStatisticsSubmission,
                    application::StatisticsApplicationError,
                >,
            > + Send,
    >,
>;

/// The existing statement Source owns these originals before publication.
/// No new work may start on this Root until the original permit is installed.
struct PreparedStatisticsSubmission {
    runtime: Arc<StatisticsJobRuntime>,
    context: novarocks_spi::connector::ConnectorRequestContext,
    request: Option<StatisticsJobCreate>,
    originals: novarocks_statistics_application::StatisticsJobOriginals,
    mode: novarocks_spi::connector::ConnectorTableJobAdmission,
    submitted: Option<StatisticsJob>,
}

impl PreparedStatisticsSubmission {
    fn install_in(
        &mut self,
        admission: &mut Option<StatisticsJobAdmission>,
    ) -> Result<(), application::StatisticsApplicationError> {
        StatisticsJobAdmission::install_internal_in(
            &mut self.originals.owner,
            &mut self.originals.query_concurrency,
            &mut self.originals.root_payload,
            admission,
        )
        .map_err(|e| application::StatisticsApplicationError::new(e.to_string()))?;
        #[cfg(novarocks_dev_controls)]
        if let Some(nomination) = self.originals.test_statement_nomination.take() {
            // The exact same nomination was captured from the admitted SQL.
            admission
                .as_mut()
                .expect("stable job admission")
                .install_test_statement_nomination_in(nomination)
                .map_err(|e| application::StatisticsApplicationError::new(e.to_string()))?;
        }
        let (job, notice) = self
            .runtime
            .install_internal_job_in(
                self.request
                    .take()
                    .expect("one captured statistics request"),
                admission,
            )
            .map_err(|e| application::StatisticsApplicationError::new(e.to_string()))?;
        self.submitted = Some(job);
        self.runtime
            .notify_installed_job(self.submitted.as_ref().unwrap().id, notice);
        Ok(())
    }

    fn start_observation_in(
        &self,
        future: &mut Option<StatisticsSubmitFuture>,
        result: &mut Option<
            Result<StatisticsSubmissionOutcome, application::StatisticsApplicationError>,
        >,
    ) {
        assert!(
            future.is_none() && result.is_none(),
            "one original submission observer"
        );
        let job = self.submitted.as_ref().expect("installed original job");
        if self.mode == novarocks_spi::connector::ConnectorTableJobAdmission::Detached {
            *result = Some(Ok(StatisticsSubmissionOutcome {
                value: application::StatisticsApplicationResult::JobSubmitted(job_view(
                    job.clone(),
                )),
                publication: None,
            }));
        } else {
            *future = Some(Box::pin(wait_statistics_submission(
                self.runtime.clone(),
                (job.id, self.context.clone()),
            )));
        }
    }

    fn retire_unsubmitted(&mut self, admission: &mut Option<StatisticsJobAdmission>) {
        if self.submitted.is_some() {
            return;
        }
        if let Some(admitted) = admission.take() {
            self.originals = admitted.into_originals();
        }
        drop(self.request.take());
        drop(self.originals.root_payload.take());
        drop(self.originals.result_capacity.take());
        drop(self.originals.query_concurrency.take());
        if let Some(owner) = self.originals.owner.take() {
            owner.complete_after_terminal_cancel_settled();
        }
    }
}

/// The exact product terminal survives the public JobView projection. Detached
/// submission carries no inferred publication; Attached carries its actual fact.
struct StatisticsSubmissionOutcome {
    value: application::StatisticsApplicationResult,
    publication: Option<novarocks_spi::connector::LakePublicationDisposition>,
}
fn submission_publication(
    result: Option<&Result<StatisticsSubmissionOutcome, application::StatisticsApplicationError>>,
) -> Option<novarocks_spi::connector::LakePublicationDisposition> {
    match result? {
        Ok(outcome) => outcome.publication,
        Err(error) => error.publication_disposition(),
    }
}

type StatisticsSubmitFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    StatisticsSubmissionOutcome,
                    application::StatisticsApplicationError,
                >,
            > + Send,
    >,
>;
struct StatisticsSubmitInput {
    runtime: Arc<StatisticsJobRuntime>,
    resolver: Arc<dyn application::StatisticsTargetResolver>,
    roots: Arc<dyn StatisticsJobRootScopeSource>,
    target: application::StatisticsTableTarget,
    columns: application::StatisticsColumnIntent,
    context: novarocks_spi::connector::ConnectorRequestContext,
    planning: novarocks_spi::connector::ConnectorControlPlanningLease,
    lease: novarocks_spi::connector::ConnectorStatisticsLease,
    #[cfg(novarocks_dev_controls)]
    nomination: Option<Arc<novarocks_failpoint::target_binding::StatementWorkNomination>>,
}
#[derive(Debug, Default)]
struct StatisticsSubmitWake(futures::task::AtomicWaker);
impl crate::task_execution::status_intake::StatusIntakeWake for StatisticsSubmitWake {
    fn wake(&self) {
        self.0.wake();
    }
}
struct StatisticsSubmitResidual {
    original: StatisticsSubmitAction,
}

impl novarocks_query_application::session_control::GovernedStatementAction
    for StatisticsSubmitAction
{
    fn external_effect_disposition(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        submission_publication(self.result.as_ref()).map(crate::query::external_effect_from_lake)
    }
    fn poll_action(
        &mut self,
        owner: &mut novarocks_query_application::session_control::GovernedActionContext<'_>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Result<
            novarocks_query_application::session_control::StatementActionValue,
            novarocks_query_application::session_error::QueryServiceError,
        >,
    > {
        use novarocks_query_application::session_control::StatementActionValue;
        use std::task::Poll;
        let error = |e: String| {
            novarocks_query_application::session_error::QueryServiceError::new(
                novarocks_query_application::session_error::QueryServiceErrorKind::Internal,
                e,
            )
        };
        self.wake.0.register(cx.waker());
        if !self.origin.is_for_scope(owner.scope()) {
            return Poll::Ready(Err(error(
                "statistics capture has a foreign original scope".into(),
            )));
        }
        let work = owner
            .execution_owner_slot()
            .as_ref()
            .ok_or_else(|| error("statistics capture lost its original owner".into()))?;
        if !self.capture_closed {
            self.started |= self
                .io
                .poll_progress(work, cx)
                .map_err(|e| error(e.to_string()))?;
        }
        if !self.started {
            return Poll::Pending;
        }
        if let Some(input) = self.input.as_ref() {
            let cpu = self
                .blocking
                .provider_creator_cpu()
                .map_err(|e| error(e.to_string()))?
                .statistics_binding_cpu_port(
                    &work.scope(),
                    self.origin.clone(),
                    &input.lease,
                    self.cancellation.clone(),
                    self.runtime.clone(),
                    self.wake.clone(),
                )
                .map_err(|e| error(e.to_string()))?;
            self.cpu = Some(cpu.clone());
            self.preparation = Some(Box::pin(prepare_statistics_submission(
                self.input.take().expect("original capture input"),
                cpu,
            )));
        }
        if let Some(future) = self.preparation.as_mut() {
            match std::task::ready!(future.as_mut().poll(cx)) {
                Ok(prepared) => self.prepared = Some(prepared),
                Err(failure) => self.result = Some(Err(failure)),
            }
            drop(self.preparation.take());
        }
        // Transfer only after the original capture has actually stopped using
        // the SQL computation. Cleanup can no longer depend on its permit.
        if !self.capture_closed {
            if let Some(cpu) = self.cpu.as_ref() {
                std::task::ready!(cpu.poll_closed(cx)).map_err(|e| error(e.to_string()))?;
            }
            std::task::ready!(self.io.poll_quiescent(work, cx))
                .map_err(|e| error(e.to_string()))?;
            self.capture_closed = true;
        }
        if self.result.is_none() && self.future.is_none() {
            let prepared = self
                .prepared
                .as_mut()
                .expect("prepared original submission");
            novarocks_spi::connector::ConnectorOperationControl::check_active(&prepared.context)
                .map_err(|e| error(e.to_string()))?;
            let notice = owner
                .handoff_query_permit_in(
                    prepared
                        .originals
                        .owner
                        .as_ref()
                        .expect("original job Root"),
                    &mut prepared.originals.query_concurrency,
                )
                .map_err(|e| error(e.to_string()))?;
            // The same non-clone grant is already in the receiving stable
            // Source. Installation refusal leaves it there for retirement.
            let installed = prepared.install_in(&mut self.admission);
            notice.notify();
            match installed {
                Ok(()) => prepared.start_observation_in(&mut self.future, &mut self.result),
                Err(failure) => self.result = Some(Err(failure)),
            }
        }
        if let Some(future) = self.future.as_mut() {
            self.result = Some(std::task::ready!(future.as_mut().poll(cx)));
            drop(self.future.take());
        }
        if self.result.as_ref().is_some_and(Result::is_err)
            && let Some(prepared) = self.prepared.as_mut()
        {
            prepared.retire_unsubmitted(&mut self.admission);
        }
        let result = self
            .result
            .as_ref()
            .expect("one original submission result");
        self.closed = true;
        match &result
            .as_ref()
            .map_err(application::StatisticsApplicationError::to_query_service_error)?
            .value
        {
            application::StatisticsApplicationResult::JobSubmitted(_) => {
                Poll::Ready(Ok(StatementActionValue::Ok(
                    novarocks_query_application::session_control::StatementCommandCompletion::new(
                        0,
                    ),
                )))
            }
            _ => Poll::Ready(Err(error(
                "statistics submission produced a foreign result".into(),
            ))),
        }
    }
    fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.io.health_failure().or_else(||self.cpu.as_ref().and_then(|cpu|cpu.local_failure()).map(|cause|novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven("statistics capture CPU exited unsuccessfully",cause)))
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<
            Box<dyn novarocks_query_application::session_control::GovernedStatementResidual>,
        >,
    ) -> Result<(), novarocks_query_application::api::StatementNativeHealthFailure> {
        if target.is_some() {
            return Err(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven("statistics submission residual is occupied",Arc::new(novarocks_workload_control::WorkError::Conflict)));
        }
        if !self.closed {
            *target = Some(Box::new(StatisticsSubmitResidual {
                original: StatisticsSubmitAction {
                    io: self.io.clone(),
                    origin: self.origin.clone(),
                    input: self.input.take(),
                    cpu: self.cpu.take(),
                    blocking: self.blocking.clone(),
                    runtime: self.runtime.clone(),
                    cancellation: self.cancellation.clone(),
                    wake: self.wake.clone(),
                    preparation: self.preparation.take(),
                    prepared: self.prepared.take(),
                    admission: self.admission.take(),
                    future: self.future.take(),
                    result: self.result.take(),
                    capture_closed: self.capture_closed,
                    started: self.started,
                    closed: self.closed,
                    io_close_failure: self.io_close_failure.take(),
                },
            }));
        }
        Ok(())
    }
}
impl novarocks_query_application::session_control::GovernedStatementResidual
    for StatisticsSubmitResidual
{
    fn poll_retirement(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), novarocks_query_application::api::StatementNativeHealthFailure>>
    {
        use std::task::Poll;
        self.original.wake.0.register(cx.waker());
        let failure = |e| {
            novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                "statistics submission actual exit is unproven",
                Arc::new(e),
            )
        };
        if !self.original.started {
            drop(self.original.input.take());
            return Poll::Ready(Ok(()));
        }
        if !self.original.capture_closed {
            self.original.io.poll_continuing(cx).map_err(failure)?;
            if let Some(future) = self.original.preparation.as_mut() {
                match std::task::ready!(future.as_mut().poll(cx)) {
                    Ok(prepared) => self.original.prepared = Some(prepared),
                    Err(error) => self.original.result = Some(Err(error)),
                }
                drop(self.original.preparation.take());
            }
            if let Some(cpu) = self.original.cpu.as_ref() {
                std::task::ready!(cpu.poll_closed(cx)).map_err(|cause|novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven("statistics capture CPU actual exit failed",Arc::new(cause)))?;
            }
            let observed =
                std::task::ready!(self.original.io.poll_actual_quiescent_continuing(cx))?;
            if let Some(failure) = observed.into_ordinary_failure() {
                self.original.io_close_failure.get_or_insert(failure);
            }
            self.original.capture_closed = true;
        }
        if let Some(future) = self.original.future.as_mut() {
            self.original.result = Some(std::task::ready!(future.as_mut().poll(cx)));
            drop(self.original.future.take());
        }
        if let Some(prepared) = self.original.prepared.as_mut() {
            prepared.retire_unsubmitted(&mut self.original.admission);
        }
        Poll::Ready(Ok(()))
    }
}
async fn prepare_statistics_submission(
    input: StatisticsSubmitInput,
    cpu: Arc<dyn novarocks_spi::connector::StatisticsCpuPort>,
) -> Result<PreparedStatisticsSubmission, application::StatisticsApplicationError> {
    let StatisticsSubmitInput {
        runtime,
        resolver,
        roots,
        target,
        columns,
        context,
        planning,
        lease,
        #[cfg(novarocks_dev_controls)]
        nomination,
    } = input;
    let capture = resolver
        .capture_table_object_async(&target, context.clone(), &planning, cpu)
        .await?;
    novarocks_spi::connector::ConnectorOperationControl::check_active(&context)
        .map_err(|e| application::StatisticsApplicationError::new(e.to_string()))?;
    let profile = lease
        .collection_io_profile()
        .map_err(application::StatisticsApplicationError::from_connector_error)?;
    let submitted_at_ms = now_ms().map_err(application::StatisticsApplicationError::new)?;
    let root = roots
        .begin_statistics_job()
        .map_err(application::StatisticsApplicationError::new)?;
    // The same queued originals cross the stable receiving cut. This is a
    // structural responsibility fact, not a prediction of all future SDK work.
    let metadata =
        std::mem::size_of::<novarocks_statistics_application::StatisticsJobOriginals>() as u64;
    let originals = novarocks_statistics_application::StatisticsJobOriginals {
        owner: Some(root.owner),
        query_concurrency: None,
        result_capacity: None,
        root_payload: None,
        #[cfg(novarocks_dev_controls)]
        test_statement_nomination: nomination,
    };
    let originals = pay_queued_statistics_original(
        originals,
        &context,
        metadata,
        profile.initial_owned_bytes(),
    )
    .await?;
    let columns = match columns {
        application::StatisticsColumnIntent::AllColumns => StatisticsColumns::All,
        application::StatisticsColumnIntent::Explicit(columns) => {
            let mut original_columns = Vec::with_capacity(columns.len());
            for column in columns {
                original_columns.push(Arc::<str>::from(column));
            }
            StatisticsColumns::Explicit(original_columns.into())
        }
    };
    Ok(PreparedStatisticsSubmission {
        runtime,
        context,
        originals,
        mode: capture.job_admission,
        submitted: None,
        request: Some(StatisticsJobCreate {
            target: StatisticsTarget {
                catalog: Arc::from(target.catalog),
                namespace: Arc::from(target.namespace),
                table: Arc::from(target.table),
                object_id: Arc::from(capture.object_id),
            },
            columns,
            submitted_at_ms,
        }),
    })
}

async fn wait_statistics_submission(
    runtime: Arc<StatisticsJobRuntime>,
    original: (
        StatisticsJobId,
        novarocks_spi::connector::ConnectorRequestContext,
    ),
) -> Result<StatisticsSubmissionOutcome, application::StatisticsApplicationError> {
    use novarocks_statistics_application::StatisticsPublicationFact;
    let terminal = await_statistics_conclusion(&runtime, original.0, &original.1).await?;
    let publication = match terminal.publication {
        StatisticsPublicationFact::NotStarted | StatisticsPublicationFact::NoOp => None,
        StatisticsPublicationFact::KnownCommitted => {
            Some(novarocks_spi::connector::LakePublicationDisposition::KnownCommitted)
        }
        StatisticsPublicationFact::KnownUncommitted => {
            Some(novarocks_spi::connector::LakePublicationDisposition::KnownUncommitted)
        }
        StatisticsPublicationFact::CommitUnknown => {
            Some(novarocks_spi::connector::LakePublicationDisposition::CommitUnknown)
        }
    };
    Ok(StatisticsSubmissionOutcome {
        value: application::StatisticsApplicationResult::JobSubmitted(job_view(terminal)),
        publication,
    })
}

#[cfg(test)]
mod admission_and_conclusion_tests {
    use std::future::Future;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use novarocks_statistics_application::{
        StatisticsAttemptError, StatisticsFailure, StatisticsJobConclusion, StatisticsJobState,
        StatisticsPublicationFact, StatisticsPublicationOutcome,
    };
    use novarocks_workload_control::{ResourceConfig, WorkloadConfig, WorkloadControl};

    use super::*;

    fn control() -> WorkloadControl {
        let control = WorkloadControl::try_new(
            WorkloadConfig {
                query_concurrency_limit: 1,
                ..WorkloadConfig::default()
            },
            ResourceConfig {
                total_bytes: 16 * 1024 * 1024,
                control_bytes: 1024 * 1024,
                per_scope_bytes: 8 * 1024 * 1024,
            },
        )
        .expect("control");
        control
            .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
            .expect("result capacity");
        control.mark_ready().expect("ready");
        control
    }

    fn context(
        stop: &novarocks_spi::connector::ConnectorStopOwner,
        deadline: Instant,
    ) -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            deadline,
            stop.view(),
            64 * 1024,
            1024 * 1024,
        )
        .expect("context")
    }

    async fn is_pending<T>(future: std::pin::Pin<&mut (impl Future<Output = T> + ?Sized)>) -> bool {
        let mut future = future;
        std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx).is_pending()))
            .await
    }

    #[tokio::test]
    async fn statement_stop_releases_the_unsubmitted_root_and_admission_record() {
        assert_unsubmitted_root_control(false).await;
    }

    #[tokio::test]
    async fn statement_deadline_releases_the_unsubmitted_root_and_admission_record() {
        assert_unsubmitted_root_control(true).await;
    }

    async fn assert_unsubmitted_root_control(deadline_expired: bool) {
        let control = control();
        let held = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .expect("held root");
        let held_permit = held.owner.scope().admit_query().unwrap().await.unwrap();
        let pending = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .expect("pending root");
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let request = context(&stop, Instant::now() + Duration::from_secs(30));
        let result = if deadline_expired {
            let expired = context(&stop, Instant::now());
            tokio::time::timeout(
                Duration::from_secs(1),
                admit_statistics_job_root(pending, &expired),
            )
            .await
            .expect("deadline interrupts admission")
        } else {
            let wait = admit_statistics_job_root(pending, &request);
            tokio::pin!(wait);
            assert!(is_pending(wait.as_mut()).await);
            assert_eq!(control.observation().snapshot().waiting_records, 1);
            stop.request_stop();
            tokio::time::timeout(Duration::from_secs(1), &mut wait)
                .await
                .expect("statement stop interrupts admission")
        };
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("an interrupted statement must not submit a root"),
        };
        assert!(error.to_string().contains(if deadline_expired {
            "deadline elapsed"
        } else {
            "cancelled"
        }));
        let snapshot = control.observation().snapshot();
        assert_eq!(snapshot.admitted_queries, 1, "only the held query remains");
        assert_eq!(snapshot.root_responsibilities, 1, "pending root completed");
        assert_eq!(snapshot.waiting_records, 0);
        assert_eq!(snapshot.admission_records, 0);
        drop(held_permit);
        held.owner.complete();
    }

    struct ControlledExecutor {
        started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        exited: Arc<AtomicBool>,
        fail: bool,
    }

    impl StatisticsAttemptExecutor for ControlledExecutor {
        fn execute<'a>(
            &'a self,
            job: StatisticsJob,
            admission: StatisticsJobAdmission,
            progress: novarocks_statistics_application::StatisticsJobProgress,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), StatisticsAttemptError>> + Send + 'a>,
        > {
            Box::pin(async move {
                let original = admission.into_originals();
                let root = original.owner.as_ref().unwrap().scope();
                self.started
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(())
                    .unwrap();
                let release = self.release.lock().unwrap().take().unwrap();
                release.await.expect("test releases the actual executor");
                self.exited.store(true, Ordering::SeqCst);
                let result = match root.check() {
                    Err(error) => Err(StatisticsAttemptError::Cancelled(StatisticsFailure {
                        message: Arc::from(error.to_string()),
                        compile_control: None,
                    })),
                    Ok(()) if self.fail => Err(StatisticsAttemptError::Failed(StatisticsFailure {
                        message: Arc::from("controlled statistics failure"),
                        compile_control: None,
                    })),
                    Ok(()) => {
                        progress.prepared(job.updated_at_ms).unwrap();
                        progress.collected(job.updated_at_ms).unwrap();
                        Ok(StatisticsPublicationOutcome::KnownCommitted {
                            finalization_failure: None,
                            cleanup_failure: None,
                        })
                    }
                };
                // This fixture owns no background source: its actual executor
                // has exited at this point. Production uses the original bank observer.
                progress
                    .retire_unmounted(original, job.updated_at_ms)
                    .unwrap();
                progress.finish(result, job.updated_at_ms).unwrap();
                Ok(())
            })
        }
    }

    fn handoff_control() -> (
        WorkloadControl,
        novarocks_workload_control::joint_admission::JointProgressDriver,
        u64,
    ) {
        use novarocks_workload_control::{
            ControlStorageConfig, ResponsibilityConfig, ResultCapacityConfig,
        };
        use std::num::NonZeroUsize;
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: 8,
            business_limit: 8,
            query_concurrency_limit: 1,
            scope_records_limit: 8,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        let (_, driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(7).unwrap(),
            })
            .unwrap();
        control
            .configure_control_storage(ControlStorageConfig::new(8, &[]))
            .unwrap();
        control.mark_ready().unwrap();
        (
            control,
            driver,
            std::mem::size_of::<novarocks_statistics_application::StatisticsJobOriginals>() as u64,
        )
    }

    fn drive_handoff_control(
        driver: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
    ) {
        for _ in 0..100000 {
            if matches!(
                driver.step(),
                novarocks_workload_control::joint_admission::JointProgress::Idle(_)
            ) {
                return;
            }
        }
        panic!("original component proof did not become idle");
    }

    #[tokio::test]
    async fn attached_analyze_hands_off_the_one_query_permit_before_waiting() {
        assert_one_permit_submission(false, false).await;
    }
    #[tokio::test]
    async fn detached_analyze_hands_off_the_one_query_permit_before_replying() {
        assert_one_permit_submission(true, false).await;
    }
    #[tokio::test]
    async fn refused_statistics_install_preserves_the_transferred_originals() {
        assert_one_permit_submission(false, true).await;
    }

    async fn assert_one_permit_submission(detached: bool, reject_install: bool) {
        use novarocks_query_application::{
            client_connection::ClientConnectionToken,
            query_control::QueryApplicationControl,
            session_control::{QueryControlService, SessionIdentity},
        };
        let (control, mut driver, m) = handoff_control();
        let query = QueryControlService::new(Arc::new(QueryApplicationControl::default()));
        let session = query
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(73, 1).unwrap(),
                "root",
            ))
            .unwrap();
        let mut sql = query
            .begin_queued_governed_query_statement(
                session.token(),
                &control.root_admission(),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let sql_scope = sql.scope().clone();
        let job_root = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .unwrap();
        let job_scope = job_root.owner.scope();
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let context = context(&stop, Instant::now() + Duration::from_secs(30));
        let originals = novarocks_statistics_application::StatisticsJobOriginals {
            owner: Some(job_root.owner),
            query_concurrency: None,
            result_capacity: None,
            root_payload: None,
            #[cfg(novarocks_dev_controls)]
            test_statement_nomination: None,
        };
        let pay = pay_queued_statistics_original(
            originals,
            &context,
            m,
            std::num::NonZeroU64::new(128).unwrap(),
        );
        tokio::pin!(pay);
        assert!(is_pending(pay.as_mut()).await);
        drive_handoff_control(&mut driver);
        let originals = tokio::time::timeout(Duration::from_secs(1), pay)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(control.observation().snapshot().admitted_queries, 1);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let exited = Arc::new(AtomicBool::new(false));
        let service = StatisticsJobService::new();
        let runtime = Arc::new(StatisticsJobRuntime::start(
            service.clone(),
            Arc::new(ControlledExecutor {
                started: Mutex::new(Some(started)),
                release: Mutex::new(Some(release_rx)),
                exited: exited.clone(),
                fail: false,
            }),
            tokio::runtime::Handle::current(),
        ));
        if reject_install {
            runtime.request_stop_for_process_exit();
        }
        let mut prepared = PreparedStatisticsSubmission {
            runtime: runtime.clone(),
            context: context.clone(),
            originals,
            mode: if detached {
                novarocks_spi::connector::ConnectorTableJobAdmission::Detached
            } else {
                novarocks_spi::connector::ConnectorTableJobAdmission::AwaitTerminal
            },
            request: Some(StatisticsJobCreate {
                target: StatisticsTarget {
                    catalog: Arc::from("ice"),
                    namespace: Arc::from("db"),
                    table: Arc::from("t"),
                    object_id: Arc::from(&b"object"[..]),
                },
                columns: StatisticsColumns::All,
                submitted_at_ms: 1,
            }),
            submitted: None,
        };
        let mut admission = None;
        let notice = sql
            .action_context(None)
            .handoff_query_permit_in(
                prepared.originals.owner.as_ref().unwrap(),
                &mut prepared.originals.query_concurrency,
            )
            .unwrap();
        assert!(
            prepared
                .originals
                .query_concurrency
                .as_ref()
                .unwrap()
                .is_for_scope(&job_scope)
        );
        assert!(
            !prepared
                .originals
                .query_concurrency
                .as_ref()
                .unwrap()
                .is_for_scope(&sql_scope)
        );
        assert_eq!(control.observation().snapshot().admitted_queries, 1);
        let installed = prepared.install_in(&mut admission);
        notice.notify();
        if reject_install {
            assert!(installed.is_err());
            assert!(prepared.submitted.is_none());
            let original = admission.take().unwrap().into_originals();
            assert_eq!(
                original.owner.as_ref().unwrap().scope().id(),
                job_scope.id()
            );
            assert!(
                original
                    .query_concurrency
                    .as_ref()
                    .unwrap()
                    .is_for_scope(&job_scope)
            );
            assert!(
                original
                    .root_payload
                    .as_ref()
                    .unwrap()
                    .is_for_scope(&job_scope)
            );
            assert_eq!(
                original
                    .root_payload
                    .as_ref()
                    .unwrap()
                    .capacity_bytes()
                    .unwrap(),
                128
            );
            assert!(
                original
                    .result_capacity
                    .as_ref()
                    .unwrap()
                    .is_root_backed_internal()
            );
            prepared.originals = original;
            assert_eq!(control.observation().snapshot().admitted_queries, 1);
            assert!(service.list().await.unwrap().is_empty());
            prepared.retire_unsubmitted(&mut admission);
            assert!(admission.is_none());
            assert_eq!(control.observation().snapshot().admitted_queries, 0);
        } else {
            installed.unwrap();
            assert!(admission.is_none());
            let job = prepared.submitted.as_ref().unwrap().clone();
            tokio::time::timeout(Duration::from_secs(1), started_rx)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(control.observation().snapshot().admitted_queries, 1);
            let mut wait = None;
            let mut response = None;
            prepared.start_observation_in(&mut wait, &mut response);
            if detached {
                assert!(wait.is_none());
                let application::StatisticsApplicationResult::JobSubmitted(response) =
                    response.take().unwrap().unwrap().value
                else {
                    panic!("actual Detached response");
                };
                assert_eq!(response.job_id, job.id.as_uuid());
                assert!(!exited.load(Ordering::SeqCst));
                sql.complete_execution();
                let _ = sql.finish();
                assert_eq!(control.observation().snapshot().admitted_queries, 1);
                release.send(()).unwrap();
                runtime.wait_for_conclusion(job.id).await.unwrap();
            } else {
                assert!(response.is_none());
                let mut wait = wait.take().expect("actual Attached observer");
                assert!(is_pending(wait.as_mut()).await);
                assert!(!exited.load(Ordering::SeqCst));
                release.send(()).unwrap();
                wait.await.unwrap();
                sql.complete_execution();
                let _ = sql.finish();
            }
            assert!(exited.load(Ordering::SeqCst));
            assert_eq!(control.observation().snapshot().admitted_queries, 0);
            runtime
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            return;
        }
        sql.complete_execution();
        let _ = sql.finish();
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        drive_handoff_control(&mut driver);
    }

    #[derive(Clone, Copy)]
    enum Outcome {
        Success,
        Failure,
        StatementStop,
        StatementDeadline,
        CancellationClockFailure,
    }

    #[tokio::test]
    async fn successful_analyze_wait_returns_after_actual_executor_exit() {
        assert_statement_wait(Outcome::Success).await;
    }

    #[tokio::test]
    async fn failed_analyze_wait_reports_failure_after_actual_executor_exit() {
        assert_statement_wait(Outcome::Failure).await;
    }

    #[tokio::test]
    async fn cancelled_analyze_wait_holds_the_statement_until_actual_executor_exit() {
        assert_statement_wait(Outcome::StatementStop).await;
    }

    #[tokio::test]
    async fn expired_analyze_wait_holds_the_statement_until_actual_executor_exit() {
        assert_statement_wait(Outcome::StatementDeadline).await;
    }

    #[tokio::test]
    async fn cancellation_clock_error_cannot_bypass_actual_executor_exit() {
        assert_statement_wait(Outcome::CancellationClockFailure).await;
    }

    async fn assert_statement_wait(outcome: Outcome) {
        let control = control();
        let service = StatisticsJobService::new();
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let exited = Arc::new(AtomicBool::new(false));
        let runtime = StatisticsJobRuntime::start(
            service.clone(),
            Arc::new(ControlledExecutor {
                started: Mutex::new(Some(started)),
                release: Mutex::new(Some(release_rx)),
                exited: Arc::clone(&exited),
                fail: matches!(outcome, Outcome::Failure),
            }),
            tokio::runtime::Handle::current(),
        );
        let root = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .expect("job root");
        let (permit, window) = root
            .owner
            .scope()
            .admit_query_with_result(novarocks_workload_control::ResultWindowClass::Internal)
            .expect("atomic job admission")
            .await
            .expect("admitted job root");
        let admission = StatisticsJobAdmission::try_new(root.owner, permit, window)
            .expect("same-root Internal admission");
        let submitted = runtime
            .submit_admitted(
                StatisticsJobCreate {
                    target: StatisticsTarget {
                        catalog: Arc::from("ice"),
                        namespace: Arc::from("db"),
                        table: Arc::from("t"),
                        object_id: Arc::from(&b"object"[..]),
                    },
                    columns: StatisticsColumns::All,
                    submitted_at_ms: 1,
                },
                admission,
            )
            .await
            .expect("submit");
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .expect("executor starts")
            .expect("started message");
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let deadline = if matches!(outcome, Outcome::StatementDeadline) {
            Instant::now()
        } else {
            Instant::now() + Duration::from_secs(30)
        };
        let context = context(&stop, deadline);
        if matches!(
            outcome,
            Outcome::StatementStop | Outcome::CancellationClockFailure
        ) {
            stop.request_stop();
        }
        let clock = || {
            if matches!(outcome, Outcome::CancellationClockFailure) {
                Err("controlled wall-clock failure".to_string())
            } else {
                now_ms()
            }
        };
        let wait = await_statistics_conclusion_with_clock(&runtime, submitted.id, &context, clock);
        tokio::pin!(wait);
        assert!(is_pending(wait.as_mut()).await);
        if matches!(outcome, Outcome::StatementDeadline) {
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    assert!(is_pending(wait.as_mut()).await);
                    if service.list().await.unwrap().remove(0).cancel_requested {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("elapsed statement deadline requests job cancellation");
        }
        assert!(!exited.load(Ordering::SeqCst));
        let before = service.list().await.unwrap().remove(0);
        assert!(!before.convergence.is_complete());
        if matches!(
            outcome,
            Outcome::StatementStop | Outcome::CancellationClockFailure
        ) {
            assert!(before.cancel_requested);
        }
        release.send(()).expect("release actual executor");
        let result = tokio::time::timeout(Duration::from_secs(1), &mut wait)
            .await
            .expect("statement converges");
        assert!(exited.load(Ordering::SeqCst));
        let terminal = service.list().await.unwrap().remove(0);
        assert!(terminal.convergence.is_complete());
        if matches!(
            outcome,
            Outcome::StatementStop | Outcome::StatementDeadline | Outcome::CancellationClockFailure
        ) {
            assert_eq!(
                terminal.state,
                StatisticsJobState::Terminal(StatisticsJobConclusion::Cancelled)
            );
        }
        assert_eq!(control.observation().snapshot().admitted_queries, 0);
        match outcome {
            Outcome::Success => {
                assert_eq!(result.unwrap(), terminal);
                assert_eq!(
                    terminal.state,
                    StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded)
                );
            }
            Outcome::Failure => assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("controlled statistics failure")
            ),
            Outcome::CancellationClockFailure => assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("controlled wall-clock failure")
            ),
            Outcome::StatementStop | Outcome::StatementDeadline => assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("statistics job business result is available")
            ),
        }
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }
    struct PublicationExecutor(StatisticsPublicationOutcome);
    impl StatisticsAttemptExecutor for PublicationExecutor {
        fn execute<'a>(
            &'a self,
            job: StatisticsJob,
            admission: StatisticsJobAdmission,
            progress: novarocks_statistics_application::StatisticsJobProgress,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), StatisticsAttemptError>> + Send + 'a>>
        {
            Box::pin(async move {
                let originals = admission.into_originals();
                progress.prepared(job.updated_at_ms).unwrap();
                progress.collected(job.updated_at_ms).unwrap();
                // This fixture has no physical child; retire its actual owner,
                // not a terminal-derived convergence assertion.
                progress
                    .retire_unmounted(originals, job.updated_at_ms)
                    .unwrap();
                progress
                    .finish(Ok(self.0.clone()), job.updated_at_ms)
                    .unwrap();
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn publication_waiter_preserves_original_failure_effect_and_safe_projection() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        use novarocks_spi::connector::{
            ConnectorError, ConnectorErrorKind, ConnectorMutationFailure,
            LakePublicationDisposition, LakePublicationFamily,
        };
        let provider = ConnectorMutationFailure::from_connector(ConnectorError::from_domain(
            ConnectorErrorKind::ResourceExhausted,
            novarocks_type_contract::CompileControlError::Source(SharedFailureCause::new(
                ConnectorError::new(
                    ConnectorErrorKind::PermissionDenied,
                    "private provider token=secret",
                ),
            )),
        ));
        // Capture provenance from the actual typed compiler error in this
        // Provider source chain, not from a matching message/class.
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&provider);
        let mut compile = None;
        while let Some(current) = cause {
            if let Some(control) =
                current.downcast_ref::<novarocks_type_contract::CompileControlError>()
            {
                compile = Some(control.provenance());
                break;
            }
            cause = current.source();
        }
        let failure = novarocks_statistics_application::StatisticsPublicationFailure::capture(
            provider.failure_fact(),
            compile,
            &provider,
        );
        let cases = [
            (
                StatisticsPublicationOutcome::KnownUncommitted {
                    failure,
                    cleanup_failure: None,
                },
                Some(LakePublicationDisposition::KnownUncommitted),
            ),
            (
                StatisticsPublicationOutcome::CommitUnknown { failure },
                Some(LakePublicationDisposition::CommitUnknown),
            ),
            (
                StatisticsPublicationOutcome::KnownCommitted {
                    finalization_failure: Some(failure),
                    cleanup_failure: None,
                },
                Some(LakePublicationDisposition::KnownCommitted),
            ),
            (
                StatisticsPublicationOutcome::KnownCommitted {
                    finalization_failure: None,
                    cleanup_failure: Some(failure),
                },
                None,
            ),
        ];
        for (outcome, failed_disposition) in cases {
            let control = control();
            let service = StatisticsJobService::new();
            let runtime = StatisticsJobRuntime::start(
                service.clone(),
                Arc::new(PublicationExecutor(outcome)),
                tokio::runtime::Handle::current(),
            );
            let root = control
                .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
                .unwrap();
            let (permit, window) = root
                .owner
                .scope()
                .admit_query_with_result(novarocks_workload_control::ResultWindowClass::Internal)
                .unwrap()
                .await
                .unwrap();
            let admission = StatisticsJobAdmission::try_new(root.owner, permit, window).unwrap();
            let job = runtime
                .submit_admitted(
                    StatisticsJobCreate {
                        target: StatisticsTarget {
                            catalog: Arc::from("ice"),
                            namespace: Arc::from("db"),
                            table: Arc::from("t"),
                            object_id: Arc::from(&b"object"[..]),
                        },
                        columns: StatisticsColumns::All,
                        submitted_at_ms: 1,
                    },
                    admission,
                )
                .await
                .unwrap();
            let stop = novarocks_spi::connector::ConnectorStopOwner::new();
            let context = context(&stop, Instant::now() + Duration::from_secs(30));
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                await_statistics_conclusion(&runtime, job.id, &context),
            )
            .await
            .unwrap();
            let terminal = service.list().await.unwrap().remove(0);
            if let Some(disposition) = failed_disposition {
                let application = result.unwrap_err();
                assert!(
                    std::error::Error::source(&application).is_none(),
                    "historical Job projection cannot retain the Provider tree"
                );
                let sql = application.to_query_service_error();
                assert_eq!(
                    sql.execution_failure().unwrap().fact(),
                    provider.failure_fact()
                );
                assert_eq!(sql.compile_control_provenance(), failure.compile_control());
                assert!(sql.compile_control_error().is_none());
                let effect = sql.publication_terminal().unwrap();
                assert_eq!(effect.disposition(), disposition);
                assert_eq!(effect.header().family(), LakePublicationFamily::Statistics);
                assert_eq!(
                    *effect.header().publication_id().as_uuid(),
                    job.publication_id.as_uuid()
                );
                assert_eq!(
                    effect.do_not_retry(),
                    disposition != LakePublicationDisposition::KnownUncommitted
                );
                assert!(!sql.to_string().contains("token=secret"));
                assert_ne!(terminal.publication, StatisticsPublicationFact::NotStarted);
            } else {
                assert_eq!(
                    result.unwrap().state,
                    StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded)
                );
                assert_eq!(
                    terminal.publication,
                    StatisticsPublicationFact::KnownCommitted
                );
                assert!(terminal.failure.is_none());
                assert!(terminal.publication_cleanup_failure.is_some());
            }
            if terminal.publication == StatisticsPublicationFact::KnownCommitted {
                let cancelled =
                    application::StatisticsApplicationError::from_statistics_job(&terminal)
                        .with_observer_message("observer cancelled after publication")
                        .to_query_service_error();
                assert_eq!(
                    cancelled.publication_terminal().unwrap().disposition(),
                    LakePublicationDisposition::KnownCommitted
                );
                assert!(cancelled.to_string().contains("do_not_retry=true"));
            }
            let view = job_view(terminal);
            assert!(!view.error_message.unwrap().contains("token=secret"));
            runtime
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
        }
    }
    struct CommittingExecutor {
        outcome: StatisticsPublicationOutcome,
        started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    }
    impl StatisticsAttemptExecutor for CommittingExecutor {
        fn execute<'a>(
            &'a self,
            job: StatisticsJob,
            admission: StatisticsJobAdmission,
            progress: novarocks_statistics_application::StatisticsJobProgress,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), StatisticsAttemptError>> + Send + 'a>>
        {
            Box::pin(async move {
                let originals = admission.into_originals();
                progress.prepared(job.updated_at_ms).unwrap();
                progress.collected(job.updated_at_ms).unwrap();
                self.started
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(())
                    .unwrap();
                let release = self.release.lock().unwrap().take().unwrap();
                release.await.unwrap();
                // The original provider selects its actual business result despite
                // an observer stop while its publication was in flight.
                progress
                    .retire_unmounted(originals, job.updated_at_ms)
                    .unwrap();
                progress
                    .finish(Ok(self.outcome.clone()), job.updated_at_ms)
                    .unwrap();
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn committed_success_wins_late_stop_and_simultaneously_ready_observer() {
        for poll_before_commit in [false, true] {
            let (runtime, service, job, release) = committing_job().await;
            let stop = novarocks_spi::connector::ConnectorStopOwner::new();
            let context = context(&stop, Instant::now() + Duration::from_secs(30));
            let wait = wait_statistics_submission(runtime.clone(), (job.id, context));
            tokio::pin!(wait);
            if poll_before_commit {
                assert!(is_pending(wait.as_mut()).await);
            }
            release.send(()).unwrap();
            let selected = runtime.wait_for_conclusion(job.id).await.unwrap();
            assert_eq!(
                selected.state,
                StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded)
            );
            // Both the product result and observer stop are ready on this
            // actual waiter poll, including when the waiter was already Pending.
            stop.request_stop();
            let result = tokio::time::timeout(Duration::from_secs(1), wait)
                .await
                .unwrap();
            assert_eq!(
                submission_publication(Some(&result)),
                Some(novarocks_spi::connector::LakePublicationDisposition::KnownCommitted)
            );
            let application::StatisticsApplicationResult::JobSubmitted(view) =
                result.unwrap().value
            else {
                panic!("same Attached result")
            };
            assert_eq!(view.job_id, job.id.as_uuid());
            let after = service.list().await.unwrap().remove(0);
            assert_eq!(after.state, selected.state);
            assert!(
                !after.cancel_requested,
                "already selected result does not request new cancellation"
            );
            runtime
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn publication_success_survives_an_earlier_observer_cancellation_error() {
        let (runtime, service, job, release) = committing_job().await;
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let context = context(&stop, Instant::now() + Duration::from_secs(30));
        stop.request_stop();
        let wait = await_statistics_conclusion_with_clock(&runtime, job.id, &context, || {
            Err("original cancellation clock failure".into())
        });
        tokio::pin!(wait);
        assert!(is_pending(wait.as_mut()).await);
        assert!(service.list().await.unwrap().remove(0).cancel_requested);
        release.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            result.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded)
        );
        assert_eq!(
            result.publication,
            StatisticsPublicationFact::KnownCommitted
        );
        assert!(
            result.cancel_requested,
            "retained stop fact does not undo commit"
        );
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    async fn committing_job() -> (
        Arc<StatisticsJobRuntime>,
        StatisticsJobService,
        StatisticsJob,
        tokio::sync::oneshot::Sender<()>,
    ) {
        publication_job(StatisticsPublicationOutcome::KnownCommitted {
            finalization_failure: None,
            cleanup_failure: None,
        })
        .await
    }
    async fn publication_job(
        outcome: StatisticsPublicationOutcome,
    ) -> (
        Arc<StatisticsJobRuntime>,
        StatisticsJobService,
        StatisticsJob,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let control = control();
        let service = StatisticsJobService::new();
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let runtime = Arc::new(StatisticsJobRuntime::start(
            service.clone(),
            Arc::new(CommittingExecutor {
                outcome,
                started: Mutex::new(Some(started)),
                release: Mutex::new(Some(release_rx)),
            }),
            tokio::runtime::Handle::current(),
        ));
        let root = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .unwrap();
        let (permit, window) = root
            .owner
            .scope()
            .admit_query_with_result(novarocks_workload_control::ResultWindowClass::Internal)
            .unwrap()
            .await
            .unwrap();
        let admission = StatisticsJobAdmission::try_new(root.owner, permit, window).unwrap();
        let job = runtime
            .submit_admitted(
                StatisticsJobCreate {
                    target: StatisticsTarget {
                        catalog: Arc::from("ice"),
                        namespace: Arc::from("db"),
                        table: Arc::from("t"),
                        object_id: Arc::from(&b"object"[..]),
                    },
                    columns: StatisticsColumns::All,
                    submitted_at_ms: 1,
                },
                admission,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .unwrap()
            .unwrap();
        (runtime, service, job, release)
    }
    #[tokio::test]
    async fn provider_noop_survives_the_actual_attached_waiter_without_a_commit_marker() {
        for settled in [false, true] {
            for poll_before_ready in [false, true] {
                let outcome =
                    crate::statistics_jobs::attempt_executor::tests::no_op_publication_for_test(
                        settled,
                    )
                    .await;
                let (runtime, service, job, release) = publication_job(outcome).await;
                let stop = novarocks_spi::connector::ConnectorStopOwner::new();
                let context = context(&stop, Instant::now() + Duration::from_secs(30));
                let wait = wait_statistics_submission(runtime.clone(), (job.id, context));
                tokio::pin!(wait);
                if poll_before_ready {
                    assert!(is_pending(wait.as_mut()).await);
                }
                release.send(()).unwrap();
                let selected = runtime.wait_for_conclusion(job.id).await.unwrap();
                assert_eq!(
                    selected.state,
                    StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded)
                );
                assert_eq!(selected.publication, StatisticsPublicationFact::NoOp);
                stop.request_stop();
                let result = tokio::time::timeout(Duration::from_secs(1), wait)
                    .await
                    .unwrap();
                assert!(
                    result.is_ok(),
                    "NoOp is successful even after a late observer stop"
                );
                assert_eq!(
                    submission_publication(Some(&result)),
                    None,
                    "the actual SQL action has no applied-publication effect"
                );
                let application::StatisticsApplicationResult::JobSubmitted(view) =
                    result.unwrap().value
                else {
                    panic!("same Attached SQL result")
                };
                assert_eq!(view.job_id, job.id.as_uuid());
                let retained = service.list().await.unwrap().remove(0);
                assert_eq!(retained.publication, StatisticsPublicationFact::NoOp);
                assert!(!retained.cancel_requested);
                let sql = application::StatisticsApplicationError::from_statistics_job(&retained)
                    .with_observer_message("late observer diagnostic")
                    .to_query_service_error();
                assert!(sql.publication_terminal().is_none());
                assert!(!sql.message().contains("do_not_retry=true"));
                runtime
                    .shutdown_until(Instant::now() + Duration::from_secs(1))
                    .await
                    .unwrap();
            }
        }
    }

    #[tokio::test]
    async fn noop_finalization_failure_and_cleanup_keep_distinct_business_results_without_effects()
    {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let cause = ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "private finalize or cleanup detail",
        );
        let failure = novarocks_statistics_application::StatisticsPublicationFailure::capture(
            cause.failure_fact(),
            None,
            &cause,
        );
        for necessary in [false, true] {
            let (runtime, service, job, release) =
                publication_job(StatisticsPublicationOutcome::NoOp {
                    finalization_failure: necessary.then_some(failure),
                    cleanup_failure: (!necessary).then_some(failure),
                })
                .await;
            let stop = novarocks_spi::connector::ConnectorStopOwner::new();
            let context = context(&stop, Instant::now() + Duration::from_secs(30));
            release.send(()).unwrap();
            let result = wait_statistics_submission(runtime.clone(), (job.id, context)).await;
            assert_eq!(submission_publication(Some(&result)), None);
            let retained = service.list().await.unwrap().remove(0);
            assert_eq!(retained.publication, StatisticsPublicationFact::NoOp);
            assert_eq!(
                retained.publication_finalization_failure,
                necessary.then_some(failure)
            );
            assert_eq!(
                retained.publication_cleanup_failure,
                (!necessary).then_some(failure)
            );
            if necessary {
                assert_eq!(
                    retained.state,
                    StatisticsJobState::Terminal(StatisticsJobConclusion::Failed)
                );
                let Err(error) = result else {
                    panic!("necessary finalization remains a business failure")
                };
                let sql = error.to_query_service_error();
                assert_eq!(
                    sql.execution_failure().unwrap().fact(),
                    cause.failure_fact()
                );
                assert!(sql.publication_terminal().is_none());
                assert!(!sql.message().contains("do_not_retry=true"));
            } else {
                assert_eq!(
                    retained.state,
                    StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded)
                );
                assert!(result.is_ok());
            }
            runtime
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
        }
    }
}

#[path = "statistics_read.rs"]
mod statistics_read;
