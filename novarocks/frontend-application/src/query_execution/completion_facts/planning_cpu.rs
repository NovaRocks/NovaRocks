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

//! Concrete pure planning turns retained by the original statement Source.
use crate::query::compiler::{FrontendQueryCompilerError, FrontendQueryPurpose};
use crate::query_execution::{
    package_freeze::CompiledPackageCarrier, physical_encoding::PreparedStaticPlan,
    provider_read_facts::FrozenProviderRead,
};
use crate::task_execution::blocking_io::{
    ConnectorBlockingIoError, ConnectorBlockingIoSupervisor, ConnectorOperationJob,
    ConnectorSourceCloseResult,
};
use async_trait::async_trait;
use novarocks_query_application::{
    api::StatementNativeHealthFailure,
    preparation::{
        CompletedPhysicalPlanCandidate, FinalPlanCompletionError, FinalPlanRuntimeAccess,
        OutputContract, SqlCompletionCpuPort, SqlCompletionCpuResult, SqlCompletionPureSteps,
    },
};
use novarocks_sql::compiler::{
    CatalogRelationFact, ExplainLevel, ExplainRenderBudget, SqlCompilation, SqlCompileControl,
    SqlCompileError, SqlCompileProgress, SqlCompletedPlan, SqlConstantEvaluator, SqlFactBatch,
    SqlFinalPlanCompileRequest, SqlFunctionCatalog, SqlLogicalExplainCatalog,
    SqlPlanningEnvironment, SqlSessionContext,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};

fn client_render_schema_error(
    error: novarocks_sql::compiler::ClientRenderSchemaError,
) -> FrontendQueryCompilerError {
    match error {
        error @ novarocks_sql::compiler::ClientRenderSchemaError::UnsupportedClientType {
            ..
        } => FrontendQueryCompilerError::Application(
            novarocks_query_application::session_error::QueryServiceError::from_user_error(
                novarocks_query_application::sql::session_admit::client_type_unsupported_error(
                    error.to_string(),
                ),
            ),
        ),
        novarocks_sql::compiler::ClientRenderSchemaError::Invalid(message) => {
            FrontendQueryCompilerError::Engine(message)
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ReadEncodingPurpose {
    Read(FrontendQueryPurpose),
    Profile(std::time::Instant),
}
pub(crate) struct ReadEncodingInput {
    pub(crate) purpose: ReadEncodingPurpose,
    pub(crate) version: novarocks_physical_plan::PlanVersionId,
    pub(crate) options: Option<novarocks_proto_codec::lifecycle::QueryOptions>,
    pub(crate) family: novarocks_result_contract::ClientPresentationFamily,
    pub(crate) offset: i32,
    pub(crate) carrier: CompiledPackageCarrier,
    pub(crate) policy: novarocks_physical_plan::ConstantPolicy,
    pub(crate) control: SqlCompileControl,
    #[cfg(novarocks_dev_controls)]
    pub(crate) rewrite: Option<ReadRewriteInput>,
}
#[cfg(novarocks_dev_controls)]
pub(crate) struct ReadRewriteInput {
    pub(crate) controls: Arc<crate::mv::test_controls::MvTestControls>,
    pub(crate) catalog: Option<String>,
    pub(crate) database: String,
    pub(crate) query: novarocks_parser::ast::Query,
    pub(crate) target: Option<crate::mv::test_controls::MvRewriteStatementTarget>,
    pub(crate) cancellation: novarocks_query_application::cancellation::QueryCancellationView,
}
pub(crate) enum CompletedReadCpuInput {
    Encoding(Box<ReadEncodingInput>),
    ExplainRender {
        budget: ExplainRenderBudget,
        control: SqlCompileControl,
    },
}
enum ReadPreparedKind {
    Read {
        description: novarocks_query_application::preparation::FrozenExecutionDescription,
        options: Arc<crate::query_execution::contract::ResolvedQueryOptions>,
    },
    Profile {
        parts: crate::query_execution::internal_consumer::profile::PreparedProfileComputationParts,
        plan: Arc<novarocks_physical_plan::PhysicalPlan>,
        annotations: Arc<[novarocks_sql::compiler::SqlDisplayAnnotation]>,
        planning_started_at: std::time::Instant,
        control: SqlCompileControl,
    },
}
struct ReadEncodedParts {
    prepared: PreparedStaticPlan,
    version: novarocks_physical_plan::PlanVersionId,
    kind: ReadPreparedKind,
}
pub(crate) struct LogicalFinishInput {
    pub(crate) logical: SqlLogicalExplainCatalog,
    pub(crate) facts: Vec<CatalogRelationFact>,
    pub(crate) functions: Arc<dyn SqlFunctionCatalog>,
    pub(crate) evaluator: &'static dyn SqlConstantEvaluator,
    pub(crate) policy: novarocks_functions::ConstantPolicy,
    pub(crate) environment: SqlPlanningEnvironment,
    pub(crate) level: ExplainLevel,
}
struct LogicalCatalogInput {
    query: novarocks_parser::ast::Query,
    session: SqlSessionContext,
    control: SqlCompileControl,
}
enum PlanningCpuInput {
    Start(Box<SqlFinalPlanCompileRequest>),
    Finish {
        compilation: SqlCompilation,
        facts: SqlFactBatch,
        control: SqlCompileControl,
    },
    Candidate {
        completed: SqlCompletedPlan,
        control: SqlCompileControl,
    },
    Completed(CompletedReadCpuInput),
    LogicalCatalog(Box<LogicalCatalogInput>),
    LogicalFinish(Box<LogicalFinishInput>),
}
enum PlanningCpuOutput {
    Progress(Result<SqlCompileProgress, FinalPlanCompletionError>),
    Candidate(Result<CompletedPhysicalPlanCandidate, FinalPlanCompletionError>),
    Encoded(Box<Result<ReadEncodedParts, FrontendQueryCompilerError>>),
    Rendered(Result<Vec<String>, FinalPlanCompletionError>),
    LogicalCatalog(Box<Result<SqlLogicalExplainCatalog, SqlCompileError>>),
    LogicalLines(Result<Vec<String>, SqlCompileError>),
}
struct PlanningCpuBody {
    input: Option<PlanningCpuInput>,
    output: Option<PlanningCpuOutput>,
    candidate: Option<CompletedPhysicalPlanCandidate>,
    access: Option<FinalPlanRuntimeAccess<FrozenProviderRead>>,
    capabilities: Option<
        std::collections::BTreeMap<
            novarocks_physical_plan::ProviderReadOccurrenceId,
            crate::query_execution::preparation::attempt_access::FrozenReadCapability,
        >,
    >,
    started: bool,
    ready_read: Option<crate::query_execution::completion::PreparedQueryOperation>,
    template: Option<crate::query_execution::artifact::PreparedDistributedAttemptTemplate>,
    profile_parts:
        Option<crate::query_execution::internal_consumer::profile::PreparedProfileComputationParts>,
    profile_received: Option<(
        crate::query_execution::logical_execution::PreparedLogicalComputation,
        crate::query_execution::internal_consumer::profile::ProfileComputationOutput,
    )>,
    ready_profile: Option<Box<crate::query::compiler::PreparedAsyncProfile>>,
}
struct PlanningCpuOriginal {
    body: Mutex<Option<PlanningCpuBody>>,
    in_foreign: AtomicBool,
}
struct PlanningBodyLoan<'a> {
    original: &'a PlanningCpuOriginal,
    body: Option<PlanningCpuBody>,
}
impl Drop for PlanningBodyLoan<'_> {
    fn drop(&mut self) {
        let mut target = self.original.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(target.is_none(), "one original planning body loan");
        *target = self.body.take();
    }
}
impl PlanningCpuOriginal {
    fn loan(&self) -> PlanningBodyLoan<'_> {
        let body = self.body.lock().unwrap_or_else(|p| p.into_inner()).take();
        PlanningBodyLoan {
            original: self,
            body,
        }
    }
    fn run_once(&self) {
        assert!(
            !self.in_foreign.swap(true, Ordering::AcqRel),
            "unwound planning input is never retried"
        );
        let mut loan = self.loan();
        let body = loan.body.as_mut().expect("original planning CPU body");
        assert!(!body.started, "one original planning CPU turn");
        body.started = true;
        let input = body.input.take().expect("original planning input");
        let output = match input {
            PlanningCpuInput::Start(request) => {
                PlanningCpuOutput::Progress(SqlCompletionPureSteps::start(*request))
            }
            PlanningCpuInput::Finish {
                compilation,
                facts,
                control,
            } => PlanningCpuOutput::Progress(SqlCompletionPureSteps::finish(
                compilation,
                facts,
                &control,
            )),
            PlanningCpuInput::Candidate { completed, control } => PlanningCpuOutput::Candidate(
                SqlCompletionPureSteps::completed_candidate(completed, &control),
            ),
            PlanningCpuInput::Completed(CompletedReadCpuInput::Encoding(input)) => {
                PlanningCpuOutput::Encoded(Box::new(Self::encode(body, *input)))
            }
            PlanningCpuInput::Completed(CompletedReadCpuInput::ExplainRender {
                budget,
                control,
            }) => PlanningCpuOutput::Rendered(
                body.candidate
                    .as_ref()
                    .expect("original explain candidate")
                    .render_explain_lines(budget, &control),
            ),
            PlanningCpuInput::LogicalCatalog(input) => {
                let LogicalCatalogInput {
                    query,
                    session,
                    control,
                } = *input;
                PlanningCpuOutput::LogicalCatalog(Box::new(SqlLogicalExplainCatalog::try_new(
                    query, session, control,
                )))
            }
            PlanningCpuInput::LogicalFinish(input) => {
                let input = *input;
                PlanningCpuOutput::LogicalLines(input.logical.finish(
                    &input.facts,
                    input.functions.as_ref(),
                    input.evaluator,
                    input.policy,
                    input.environment,
                    input.level,
                ))
            }
        };
        body.output = Some(output);
        if matches!(&body.output, Some(PlanningCpuOutput::Encoded(result)) if result.is_ok()) {
            // Decomposition uses only the already-unique occurrence keys. The
            // real capability map and good result are stable before the first
            // opaque column/encoding destructor, still on this CPU turn.
            let (encodings, capabilities) =
                crate::query_execution::physical_encoding::split_frozen_reads(
                    body.access
                        .take()
                        .expect("same original encoded read access"),
                );
            body.capabilities = Some(capabilities);
            drop(encodings);
        }
        drop(loan);
        self.in_foreign.store(false, Ordering::Release);
    }
    fn encode(
        body: &mut PlanningCpuBody,
        input: ReadEncodingInput,
    ) -> Result<ReadEncodedParts, FrontendQueryCompilerError> {
        let candidate = body.candidate.take().expect("original read candidate");
        let root_output = match input.purpose {
            ReadEncodingPurpose::Read(FrontendQueryPurpose::ClientRows) => {
                novarocks_result_contract::FrozenRootOutput::ClientRows(
                    novarocks_sql::compiler::client_render_schema_from_port(
                        candidate.original_public_result_port(),
                        match &input.family {
                            novarocks_result_contract::ClientPresentationFamily::MysqlText => {
                                novarocks_sql::compiler::RenderFamilyRequest::MysqlText {
                                    timezone_offset_seconds: input.offset,
                                }
                            }
                            novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                                facts,
                            ) => novarocks_sql::compiler::RenderFamilyRequest::TrinoJson(facts),
                        },
                    )
                    .map_err(client_render_schema_error)?,
                )
            }
            ReadEncodingPurpose::Read(FrontendQueryPurpose::ScalarValue) => {
                crate::query::compiler::scalar_root_output_from_public_port(
                    candidate.original_public_result_port(),
                )?
            }
            ReadEncodingPurpose::Read(FrontendQueryPurpose::LocalRows) => {
                return Err(FrontendQueryCompilerError::Engine(
                    "distributed read received a different application output purpose".into(),
                ));
            }
            ReadEncodingPurpose::Profile(_) => {
                novarocks_result_contract::FrozenRootOutput::CountOnly
            }
        };
        let candidate = candidate
            .freeze_root_output(root_output)
            .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?;
        let profile = matches!(input.purpose, ReadEncodingPurpose::Profile(_)).then(|| {
            (
                Arc::clone(candidate.plan()),
                candidate.display_annotations().to_vec().into(),
            )
        });
        let output = OutputContract::from_completed_candidate(
            novarocks_query_application::api::QueryExecutionKind::Read,
            &candidate,
        )
        .map_err(FrontendQueryCompilerError::Engine)?;
        #[cfg(novarocks_dev_controls)]
        let selected_mv_rewrite = candidate
            .plan()
            .annotations()
            .iter()
            .any(|a| a.key.as_ref() == "sql.mv_rewrite_provenance");
        let access = body
            .access
            .as_ref()
            .expect("original read access remains outside CPU call input");
        let encoded = crate::query_execution::physical_encoding::encode_completed_plan_borrowed(
            candidate,
            access,
            &input.carrier,
            input.policy,
            &input.control,
        )
        .map_err(FrontendQueryCompilerError::from_encode)?;
        let prepared = encoded
            .prepare_access(access)
            .map_err(FrontendQueryCompilerError::from_encode)?;
        let description = novarocks_query_application::preparation::FrozenExecutionDescription::for_completed_plan(
            novarocks_query_application::api::QueryExecutionKind::Read,
            prepared.candidate().clone(),
            prepared.scheduling_facts(input.version).map_err(FrontendQueryCompilerError::Engine)?.fragments.iter().flat_map(|fragment| fragment.scans.iter().map(|scan| scan.scan)).collect(),
            output,
            novarocks_coordination_contract::ExecutionEffect::None,
            novarocks_coordination_contract::RecoveryMode::RestartAttemptBeforeVisibility,
            Vec::new(),
            novarocks_query_application::preparation::FrozenCostEstimate::unknown(novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected),
            novarocks_query_application::preparation::ExecutionResourceRequirements::unknown(novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected),
        ).map_err(FrontendQueryCompilerError::Engine)?;
        #[cfg(novarocks_dev_controls)]
        if let Some(rewrite) = &input.rewrite {
            rewrite
                .controls
                .rewrite(
                    rewrite.catalog.as_deref(),
                    &rewrite.database,
                    &rewrite.query,
                    selected_mv_rewrite,
                    rewrite.target.as_ref(),
                    &rewrite.cancellation,
                )
                .map_err(FrontendQueryCompilerError::Engine)?;
        }
        let options = Arc::new(
            crate::query_execution::contract::ResolvedQueryOptions::from_upstream(input.options),
        );
        let kind = match input.purpose {
            ReadEncodingPurpose::Read(_) => ReadPreparedKind::Read {
                description,
                options,
            },
            ReadEncodingPurpose::Profile(planning_started_at) => {
                let parts = crate::query_execution::internal_consumer::profile::prepare_profile_computation_parts(
                    description, novarocks_query_application::api::PlanSeal::Version(input.version), options,
                ).map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?;
                let (plan, annotations) = profile.expect("original Profile snapshot");
                ReadPreparedKind::Profile {
                    parts,
                    plan,
                    annotations,
                    planning_started_at,
                    control: input.control,
                }
            }
        };
        Ok(ReadEncodedParts {
            prepared,
            version: input.version,
            kind,
        })
    }
}
struct PlanningCpuDriver {
    job: Option<ConnectorOperationJob<()>>,
    response: Option<Result<(), ConnectorBlockingIoError>>,
    health: Option<StatementNativeHealthFailure>,
    closing: bool,
}
pub(crate) struct FrontendPlanningCpuSource {
    executor: ConnectorBlockingIoSupervisor,
    original: Arc<PlanningCpuOriginal>,
    driver: Mutex<Option<PlanningCpuDriver>>,
}
struct PlanningDriverLoan<'a> {
    source: &'a FrontendPlanningCpuSource,
    body: Option<PlanningCpuDriver>,
}
impl Drop for PlanningDriverLoan<'_> {
    fn drop(&mut self) {
        let mut target = self.source.driver.lock().unwrap_or_else(|p| p.into_inner());
        assert!(target.is_none(), "one original planning driver loan");
        *target = self.body.take();
    }
}
fn run_planning_original(
    original: &mut Arc<PlanningCpuOriginal>,
    _: &ConnectorSourceCloseResult<'_>,
) -> Option<()> {
    original.run_once();
    Some(())
}
impl FrontendPlanningCpuSource {
    pub(crate) fn new(executor: ConnectorBlockingIoSupervisor) -> Arc<Self> {
        Arc::new(Self {
            executor,
            original: Arc::new(PlanningCpuOriginal {
                body: Mutex::new(Some(PlanningCpuBody {
                    input: None,
                    output: None,
                    candidate: None,
                    access: None,
                    capabilities: None,
                    started: false,
                    ready_read: None,
                    template: None,
                    profile_parts: None,
                    profile_received: None,
                    ready_profile: None,
                })),
                in_foreign: AtomicBool::new(false),
            }),
            driver: Mutex::new(Some(PlanningCpuDriver {
                job: None,
                response: None,
                health: None,
                closing: false,
            })),
        })
    }
    fn driver_loan(&self) -> PlanningDriverLoan<'_> {
        let body = self.driver.lock().unwrap_or_else(|p| p.into_inner()).take();
        PlanningDriverLoan { source: self, body }
    }
    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.driver
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|s| s.health.clone())
    }
    fn install(&self, input: PlanningCpuInput) {
        let mut driver_loan = self.driver_loan();
        let driver = driver_loan.body.as_mut().expect("original planning driver");
        assert!(
            !driver.closing && driver.health.is_none() && driver.job.is_none(),
            "preceding original turn must be received before rearm"
        );
        let mut loan = self.original.loan();
        let body = loan.body.as_mut().expect("original planning body");
        assert!(
            body.input.is_none()
                && body.output.is_none()
                && body.access.is_none()
                && body.capabilities.is_none()
                && body.candidate.is_none()
                && body.ready_read.is_none()
                && body.template.is_none()
                && body.profile_parts.is_none()
                && body.profile_received.is_none()
                && body.ready_profile.is_none(),
            "original planning receiving slots are empty"
        );
        body.input = Some(input);
        body.started = false;
        drop(loan);
        driver.job = Some(self.executor.prepare_cpu_owned(
            Vec::new(),
            Arc::clone(&self.original),
            run_planning_original,
        ));
    }
    pub(super) fn install_completed_in(
        &self,
        source: &mut Option<super::provider_source::CompletionResult>,
        input: &mut Option<CompletedReadCpuInput>,
    ) -> Result<(), &'static str> {
        let mut driver_loan = self.driver_loan();
        let driver = driver_loan
            .body
            .as_mut()
            .ok_or("planning driver is borrowed")?;
        if driver.closing || driver.health.is_some() || driver.job.is_some() {
            return Err("planning receiving turn is occupied or closing");
        }
        let mut loan = self.original.loan();
        let body = loan.body.as_mut().ok_or("planning original is borrowed")?;
        if body.input.is_some()
            || body.output.is_some()
            || body.access.is_some()
            || body.capabilities.is_some()
            || body.candidate.is_some()
            || body.ready_read.is_some()
            || body.template.is_some()
            || body.profile_parts.is_some()
            || body.profile_received.is_some()
            || body.ready_profile.is_some()
            || input.is_none()
            || !matches!(source, Some(Ok(_)))
        {
            return Err("completed read source or destination is unavailable");
        }
        let Some(Ok(completed)) = source.take() else {
            unreachable!()
        };
        let (candidate, access) = completed.into_parts();
        body.candidate = Some(candidate);
        body.access = Some(access);
        body.input = Some(PlanningCpuInput::Completed(input.take().unwrap()));
        body.started = false;
        drop(loan);
        driver.job = Some(self.executor.prepare_cpu_owned(
            Vec::new(),
            Arc::clone(&self.original),
            run_planning_original,
        ));
        Ok(())
    }
    pub(crate) fn poll_quiescent(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        let mut loan = self.driver_loan();
        let driver = loan.body.as_mut().expect("original planning driver");
        if let Some(health) = &driver.health {
            return Poll::Ready(Err(health.clone()));
        }
        if driver.response.is_some() {
            return Poll::Ready(Ok(()));
        }
        let Some(job) = driver.job.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        let result = match job.poll_progress(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(r) => r,
        };
        if let Err(error) = &result {
            let health = StatementNativeHealthFailure::actual_exit_unproven(
                "original planning CPU turn is unhealthy",
                error.health_cause(),
            );
            driver.health = Some(health.clone());
            driver.response = Some(result);
            return Poll::Ready(Err(health));
        }
        assert!(
            job.actual_exit_observed(),
            "planning result does not replace actual CPU exit"
        );
        driver.response = Some(result);
        Poll::Ready(Ok(()))
    }
    async fn drive(&self) -> Result<(), StatementNativeHealthFailure> {
        std::future::poll_fn(|cx| self.poll_quiescent(cx)).await
    }
    fn retire_received(&self) {
        let mut loan = self.driver_loan();
        let driver = loan.body.as_mut().expect("original planning driver");
        assert!(
            driver.health.is_none() && matches!(driver.response, Some(Ok(()))),
            "only healthy actual exit can retire a turn"
        );
        let job = driver.job.as_mut().expect("same original CPU job");
        assert!(
            matches!(job.try_take(), Some(Ok(()))),
            "same original completion acknowledgement"
        );
        let retired = driver.job.take();
        driver.response = None;
        drop(loan);
        // No foreign input/result destructor runs under the driver lock.
        drop(retired);
    }
    async fn receive(&self) -> Result<PlanningCpuOutput, StatementNativeHealthFailure> {
        self.drive().await?;
        assert!(
            !self.original.in_foreign.load(Ordering::Acquire),
            "foreign planning exit is unproven"
        );
        let output = self
            .original
            .loan()
            .body
            .as_mut()
            .expect("same original body")
            .output
            .take()
            .expect("same original planning output");
        self.retire_received();
        Ok(output)
    }
    pub(crate) async fn prepare_read_in(
        &self,
    ) -> Result<Result<(), FrontendQueryCompilerError>, StatementNativeHealthFailure> {
        self.drive().await?;
        assert!(
            !self.original.in_foreign.load(Ordering::Acquire),
            "original encoding exit is unproven"
        );
        let mut loan = self.original.loan();
        let body = loan.body.as_mut().expect("original encoding body");
        let PlanningCpuOutput::Encoded(result) =
            body.output.take().expect("original encoding output")
        else {
            panic!("original encoding returned another phase")
        };
        let result = match *result {
            Err(error) => Err(error),
            Ok(parts) => {
                let ReadPreparedKind::Read {
                    description,
                    options,
                } = parts.kind
                else {
                    panic!("Read receiver has a foreign Profile phase")
                };
                let encoded = parts.prepared.bind_capabilities(
                    body.capabilities
                        .take()
                        .expect("same original access after actual CPU exit"),
                );
                let template = encoded.into_attempt_template(parts.version);
                body.ready_read = Some(
                    crate::query_execution::completion::PreparedQueryOperation::LogicalRead(
                        crate::query_execution::completion::PreparedLogicalRead::new(
                            description,
                            template,
                            options,
                        ),
                    ),
                );
                Ok(())
            }
        };
        drop(loan);
        self.retire_received();
        Ok(result)
    }
    pub(crate) fn take_read_in(
        &self,
        destination: &mut Option<crate::query_execution::completion::PreparedQueryOperation>,
    ) -> Result<(), &'static str> {
        if destination.is_some() {
            return Err("Read receiver is occupied");
        }
        let mut loan = self.original.loan();
        let body = loan.body.as_mut().ok_or("original Read body is borrowed")?;
        if body.ready_read.is_none() {
            return Err("original Read has no received output");
        }
        *destination = body.ready_read.take();
        Ok(())
    }
    pub(crate) async fn prepare_profile_in(
        &self,
    ) -> Result<Result<(), FrontendQueryCompilerError>, StatementNativeHealthFailure> {
        self.drive().await?;
        assert!(
            !self.original.in_foreign.load(Ordering::Acquire),
            "original Profile encoding exit is unproven"
        );
        let mut loan = self.original.loan();
        let body = loan.body.as_mut().expect("original Profile body");
        let PlanningCpuOutput::Encoded(result) =
            body.output.take().expect("original Profile static result")
        else {
            panic!("Profile returned another phase")
        };
        let result = match *result {
            Err(error) => Err(error),
            Ok(parts) => {
                let ReadPreparedKind::Profile {
                    parts: profile_parts,
                    plan,
                    annotations,
                    planning_started_at,
                    control,
                } = parts.kind
                else {
                    panic!("Profile receiver has a foreign Read phase")
                };
                assert!(
                    body.template.is_none()
                        && body.profile_parts.is_none()
                        && body.profile_received.is_none()
                        && body.ready_profile.is_none(),
                    "original Profile receiving cells are empty"
                );
                body.profile_parts = Some(profile_parts);
                let encoded = parts.prepared.bind_capabilities(
                    body.capabilities
                        .take()
                        .expect("same original Profile access after actual CPU exit"),
                );
                body.template = Some(encoded.into_attempt_template(parts.version));
                match crate::query_execution::internal_consumer::profile::PreparedProfileComputationParts::move_in(
                    &mut body.profile_parts, &mut body.template, &mut body.profile_received,
                ) {
                    Err(error) => Err(FrontendQueryCompilerError::Engine(error.to_string())),
                    Ok(()) => {
                        let (computation, output) = body.profile_received.take().unwrap();
                        let completion = crate::query_execution::completion::PreparedQueryCompletion::completed_profile(plan, annotations, planning_started_at.elapsed(), std::time::Instant::now(), control);
                        body.ready_profile = Some(Box::new(crate::query::compiler::PreparedAsyncProfile { computation: Some(computation), output, completion }));
                        Ok(())
                    }
                }
            }
        };
        drop(loan);
        self.retire_received();
        Ok(result)
    }
    pub(crate) fn take_profile_in(
        &self,
        destination: &mut Option<Box<crate::query::compiler::PreparedAsyncProfile>>,
    ) -> Result<(), &'static str> {
        if destination.is_some() {
            return Err("Profile receiver is occupied");
        }
        let mut loan = self.original.loan();
        let body = loan
            .body
            .as_mut()
            .ok_or("original Profile body is borrowed")?;
        if body.ready_profile.is_none() {
            return Err("original Profile has no received output");
        }
        *destination = body.ready_profile.take();
        Ok(())
    }
    pub(crate) async fn explain_render(&self) -> SqlCompletionCpuResult<Vec<String>> {
        let output = self.receive().await?;
        let PlanningCpuOutput::Rendered(result) = output else {
            panic!("original explain returned another phase")
        };
        // The original candidate/access remain in this Source through its close.
        Ok(result)
    }
    pub(crate) async fn logical_catalog(
        &self,
        query: novarocks_parser::ast::Query,
        session: SqlSessionContext,
        control: SqlCompileControl,
    ) -> Result<Result<SqlLogicalExplainCatalog, SqlCompileError>, StatementNativeHealthFailure>
    {
        self.install(PlanningCpuInput::LogicalCatalog(Box::new(
            LogicalCatalogInput {
                query,
                session,
                control,
            },
        )));
        let PlanningCpuOutput::LogicalCatalog(result) = self.receive().await? else {
            panic!("logical catalog returned another phase")
        };
        Ok(*result)
    }
    pub(crate) async fn logical_finish(
        &self,
        input: LogicalFinishInput,
    ) -> Result<Result<Vec<String>, SqlCompileError>, StatementNativeHealthFailure> {
        self.install(PlanningCpuInput::LogicalFinish(Box::new(input)));
        let PlanningCpuOutput::LogicalLines(result) = self.receive().await? else {
            panic!("logical finish returned another phase")
        };
        Ok(result)
    }
    pub(crate) fn poll_closed(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        {
            let mut loan = self.driver_loan();
            loan.body
                .as_mut()
                .expect("original planning driver")
                .closing = true;
        }
        self.poll_quiescent(cx)
    }
    pub(crate) async fn close(&self) -> Result<(), StatementNativeHealthFailure> {
        std::future::poll_fn(|cx| self.poll_closed(cx)).await
    }
}
#[async_trait]
impl SqlCompletionCpuPort for FrontendPlanningCpuSource {
    async fn start(
        &self,
        request: SqlFinalPlanCompileRequest,
    ) -> SqlCompletionCpuResult<SqlCompileProgress> {
        self.install(PlanningCpuInput::Start(Box::new(request)));
        let PlanningCpuOutput::Progress(result) = self.receive().await? else {
            panic!("planning start returned another phase")
        };
        Ok(result)
    }
    async fn finish(
        &self,
        compilation: SqlCompilation,
        facts: SqlFactBatch,
        control: SqlCompileControl,
    ) -> SqlCompletionCpuResult<SqlCompileProgress> {
        self.install(PlanningCpuInput::Finish {
            compilation,
            facts,
            control,
        });
        let PlanningCpuOutput::Progress(result) = self.receive().await? else {
            panic!("planning finish returned another phase")
        };
        Ok(result)
    }
    async fn completed_candidate(
        &self,
        completed: SqlCompletedPlan,
        control: SqlCompileControl,
    ) -> SqlCompletionCpuResult<CompletedPhysicalPlanCandidate> {
        self.install(PlanningCpuInput::Candidate { completed, control });
        let PlanningCpuOutput::Candidate(result) = self.receive().await? else {
            panic!("planning candidate returned another phase")
        };
        Ok(result)
    }
}

#[cfg(test)]
mod client_render_error_tests {
    use super::*;

    #[test]
    fn protocol_type_rejection_retains_typed_application_error() {
        let error = client_render_schema_error(
            novarocks_sql::compiler::ClientRenderSchemaError::UnsupportedClientType {
                column: "wide_result".into(),
                native_type: novarocks_result_contract::NativeRenderType::LargeInt,
            },
        );
        let FrontendQueryCompilerError::Application(error) = error else {
            panic!("client type rejection must remain a typed application failure");
        };
        let user = error.user_error().expect("admit descriptor");
        assert_eq!(user.code().as_str(), "sql.admit.client_type_unsupported");
        assert_eq!(user.phase(), novarocks_user_error::ErrorPhase::Admit);
        assert!(user.location().is_none());
        assert!(user.to_string().contains("wide_result"));
        assert!(user.to_string().contains("LargeInt"));
    }

    #[test]
    fn preexisting_render_errors_keep_original_engine_message() {
        let message = "public output logical type UUID is unsupported";
        let error = client_render_schema_error(
            novarocks_sql::compiler::ClientRenderSchemaError::Invalid(message.into()),
        );
        assert!(matches!(error, FrontendQueryCompilerError::Engine(actual) if actual == message));
    }
}
