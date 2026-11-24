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

//! One frozen SET computation runs on the original supervised planning CPU.
use super::property_cpu_context::PropertyCpuContext;
use novarocks_expression::RequiredExpressionRowError;
use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
use novarocks_parser::{Span, ast};
use novarocks_query_application::sql::session::registry::{
    SessionCatalogPropertyPort, SessionPropertyRegistry, SessionPropertyType,
    sql_session_property_error_at,
};
use novarocks_query_application::sql::{
    session::SessionSqlState, session_admit::SessionAdmitError,
};
use novarocks_query_application::{
    session_effects::SessionEffects, session_error::QueryServiceError,
};
use novarocks_sql::compiler::{
    RequiredConstantInput, RequiredConstantRequest, RequiredConstantTarget, SqlCompileControl,
    SqlCompileError, SqlConstantEvaluationError, SqlFunctionCatalog, SqlPlannerTableSnapshot,
    prepare_required_constant,
};
use novarocks_type_contract::{
    CompilePhase, PureCompileControl, TemporalValue, TimeZoneKey, ValueKind,
};
use novarocks_workload_control::{ResultWindowAlias, WorkScope};
use std::sync::{Arc, RwLock};

/// Semantic input from a typed statement owner. P09 moves the original
/// qualified name, expression and statement range into these fields.
pub(crate) enum ScopedSessionMutation {
    Set {
        name: ast::ObjectName,
        value: ast::Expr,
    },
    Reset {
        name: ast::ObjectName,
        span: Span,
    },
    SetTimeZone(ast::TrinoSetTimeZone),
}

pub(crate) struct SessionPropertyMutation {
    pub(crate) mutation: Box<ScopedSessionMutation>,
    pub(crate) parameters: Option<Arc<novarocks_sql::compiler::SqlPreparedParameters>>,
    pub(crate) catalogs: Arc<dyn SessionCatalogPropertyPort>,
}

pub(crate) struct SessionPropertyCpuInput {
    pub(crate) source: Arc<str>,
    pub(crate) mutation: SessionPropertyMutation,
    pub(crate) staged: SessionSqlState,
    pub(crate) statement_time: novarocks_type_contract::StatementTimeEnvironment,
    pub(crate) registry: Arc<SessionPropertyRegistry>,
    pub(crate) interpreter: Arc<crate::RequiredConstantInterpreter>,
    pub(crate) control: SqlCompileControl,
    pub(crate) scope: WorkScope,
    pub(crate) window: ResultWindowAlias,
    pub(crate) catalog: Arc<RwLock<novarocks_sql::planning::catalog::PlannerMemoryCatalog>>,
    pub(crate) functions: Arc<dyn SqlFunctionCatalog>,
    pub(crate) policy: novarocks_functions::ConstantPolicy,
}
pub(crate) struct PreparedSessionProperties {
    pub(crate) staged: SessionSqlState,
    pub(crate) effects: SessionEffects,
}
impl SessionPropertyCpuInput {
    pub(crate) fn run(mut self) -> Result<PreparedSessionProperties, QueryServiceError> {
        let evaluator = SessionPropertyEvaluation {
            interpreter: &self.interpreter,
            sql_semantics: self.staged.statement_sql_semantics(self.statement_time),
            control: &self.control,
            scope: &self.scope,
            window: &self.window,
            catalog: &self.catalog,
            functions: &self.functions,
            policy: self.policy,
            parameters: self.mutation.parameters.as_deref(),
        };
        let effects = self.prepare_scoped(
            &evaluator,
            &self.mutation.mutation,
            self.mutation.catalogs.as_ref(),
        )?;
        // Projection is still CPU work. The live session is untouched until
        // this whole computation has returned and its original worker exited.
        self.staged.apply_effects(&effects)?;
        Ok(PreparedSessionProperties {
            staged: self.staged,
            effects,
        })
    }
    fn prepare_scoped(
        &self,
        evaluator: &SessionPropertyEvaluation<'_>,
        mutation: &ScopedSessionMutation,
        catalogs: &dyn SessionCatalogPropertyPort,
    ) -> Result<SessionEffects, QueryServiceError> {
        self.control
            .checkpoint(CompilePhase::Validate, 0)
            .map_err(QueryServiceError::from_compile_control)?;
        match mutation {
            ScopedSessionMutation::Set { name, value } => {
                let span = value.span();
                let property = self
                    .registry
                    .resolve_sql_property(name, catalogs)
                    .map_err(|error| sql_session_property_error_at(error, &self.source, span))?;
                let evaluated = evaluator.evaluate(
                    &self.staged,
                    value,
                    &property.qualified_name(),
                    property.value_type(),
                )?;
                let validated = property
                    .validate_sql_value(&evaluated, &self.control)
                    .map_err(|error| sql_session_property_error_at(error, &self.source, span))?;
                Ok(SessionEffects::property(validated))
            }
            ScopedSessionMutation::SetTimeZone(node) => {
                let zone = node
                    .time_zone
                    .as_ref()
                    .map(|expression| {
                        evaluator.evaluate_time_zone(&self.staged, expression, &self.source)
                    })
                    .transpose()?;
                self.control
                    .checkpoint(CompilePhase::Validate, 0)
                    .map_err(QueryServiceError::from_compile_control)?;
                self.registry
                    .prepare_trino_time_zone(zone)
                    .map_err(|error| super::internal_error(error.to_string()))
            }
            ScopedSessionMutation::Reset { name, span } => {
                let property = self
                    .registry
                    .resolve_sql_property(name, catalogs)
                    .map_err(|error| sql_session_property_error_at(error, &self.source, *span))?;
                self.control
                    .checkpoint(CompilePhase::Validate, 0)
                    .map_err(QueryServiceError::from_compile_control)?;
                Ok(SessionEffects::reset_property(property))
            }
        }
    }
}
pub(crate) struct QuerySessionPropertyCpuInput {
    pub(crate) source: Arc<str>,
    pub(super) parameters: Option<Arc<novarocks_sql::compiler::SqlPreparedParameters>>,
    pub(super) statement: super::query_session_action::QueryFamily,
    pub(crate) original: SessionSqlState,
    pub(crate) registry: Arc<SessionPropertyRegistry>,
    pub(crate) catalogs: novarocks_query_application::sql::catalog::SessionCatalogService,
    pub(crate) context: PropertyCpuContext,
}

pub(crate) struct PreparedQuerySession {
    pub(super) statement: super::query_session_action::QueryFamily,
    pub(super) parameters: Option<Arc<novarocks_sql::compiler::SqlPreparedParameters>>,
    pub(crate) state: SessionSqlState,
}

impl QuerySessionPropertyCpuInput {
    pub(crate) fn run(self) -> Result<PreparedQuerySession, QueryServiceError> {
        let query = self.statement.query();
        let evaluator = SessionPropertyEvaluation {
            interpreter: &self.context.interpreter,
            sql_semantics: self.context.sql_semantics.clone(),
            control: &self.context.control,
            scope: &self.context.scope,
            window: &self.context.window,
            catalog: &self.context.catalog,
            functions: &self.context.functions,
            policy: self.context.policy,
            parameters: self.parameters.as_deref(),
        };
        let state = self.registry.prepare_query_session(
            &self.source,
            &query.session_properties,
            &self.original,
            self.catalogs.as_ref(),
            &self.context.control,
            |original, property, value| {
                evaluator.evaluate(
                    original,
                    value,
                    &property.qualified_name(),
                    property.value_type(),
                )
            },
        )?;
        Ok(PreparedQuerySession {
            statement: self.statement,
            parameters: self.parameters,
            state,
        })
    }
}

pub(crate) struct QueryContextsCpuInput {
    source: Arc<str>,
    prepared: PreparedQuerySession,
    context: PropertyCpuContext,
}

impl QueryContextsCpuInput {
    pub(crate) fn run(mut self) -> Result<PreparedQuerySession, QueryServiceError> {
        if let Some(parameters) = self.prepared.parameters.take() {
            self.prepared.parameters = Some(complete_prepared_context(
                parameters,
                self.prepared.statement.query(),
                &self.context,
                &self.source,
            )?);
        }
        Ok(self.prepared)
    }
}

struct QueryContextsEnvironment {
    source: Arc<str>,
    compiler: super::compiler::FrontendQueryCompiler,
    interpreter: Arc<crate::RequiredConstantInterpreter>,
    statement_time: novarocks_type_contract::StatementTimeEnvironment,
}

pub(super) fn complete_prepared_context(
    parameters: Arc<novarocks_sql::compiler::SqlPreparedParameters>,
    query: &ast::Query,
    context: &PropertyCpuContext,
    source: &str,
) -> Result<Arc<novarocks_sql::compiler::SqlPreparedParameters>, QueryServiceError> {
    let needs = parameters.contextual_needs(query);
    if needs.is_empty() || parameters.is_description() {
        return Ok(parameters);
    }
    let compiler_error = |error| {
        super::routed_execution_service_error(super::routed_compiler_failure(
            super::compiler::FrontendQueryCompilerError::from_compile(error),
            source,
        ))
    };
    let mut evaluated = Vec::with_capacity(needs.len());
    for need in needs {
        let prepared = {
            let catalog = context.catalog.read().map_err(super::poisoned_state)?;
            prepare_required_constant(RequiredConstantRequest {
                input: need.input(),
                target: need.target(),
                catalog: &SqlPlannerTableSnapshot::new(&*catalog),
                current_database: &context.current_database,
                functions: context.functions.clone(),
                sealed_functions: context.interpreter.sealed_functions(),
                sql_semantics: &context.sql_semantics,
                constant_policy: context.policy,
                control: &context.control,
            })
            .map_err(compiler_error)?
        };
        let value = context
            .interpreter
            .evaluate(prepared, &context.scope, &context.window)
            .map_err(|cause| match cause {
                SqlConstantEvaluationError::Source(cause)
                | SqlConstantEvaluationError::Outer(cause) => {
                    QueryServiceError::from_failure_cause(cause)
                }
                SqlConstantEvaluationError::Control(control) => {
                    QueryServiceError::from_compile_control(control)
                }
                other => super::internal_error(other.to_string()),
            })?;
        evaluated.push(value);
    }
    parameters
        .with_contextual_values(query, evaluated, &context.control)
        .map(Arc::new)
        .map_err(compiler_error)
}

/// First phase of the registered Read/Explain Action. It moves the actual
/// statement through the required CPU and retains the original queue wait.
pub(super) struct QuerySessionPreparation {
    source: Arc<crate::query_execution::completion_facts::FrontendPlanningCpuSource>,
    input: Option<QuerySessionPreparationInput>,
    computation: Option<QuerySessionComputation>,
    contexts: Option<QueryContextsEnvironment>,
    context_computation: Option<QuerySessionComputation>,
    effective: Option<PreparedQuerySession>,
    admission: Option<novarocks_query_application::session_control::SessionQueryAdmission>,
    grant: Option<novarocks_query_application::session_control::QueuedQueryGrant>,
    health: Option<novarocks_query_application::api::StatementNativeHealthFailure>,
}

pub(super) struct QuerySessionPreparationInput {
    pub(super) source: Arc<str>,
    pub(super) parameters: Option<Arc<novarocks_sql::compiler::SqlPreparedParameters>>,
    pub(super) statement: super::query_session_action::QueryFamily,
    pub(super) original: SessionSqlState,
    pub(super) statement_time: novarocks_type_contract::StatementTimeEnvironment,
    pub(super) compiler: super::compiler::FrontendQueryCompiler,
    pub(super) registry: Arc<SessionPropertyRegistry>,
    pub(super) catalogs: novarocks_query_application::sql::catalog::SessionCatalogService,
    pub(super) interpreter: Arc<crate::RequiredConstantInterpreter>,
}

type QuerySessionComputation = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    Result<PreparedQuerySession, QueryServiceError>,
                    novarocks_query_application::api::StatementNativeHealthFailure,
                >,
            > + Send,
    >,
>;

impl QuerySessionPreparation {
    pub(super) fn new(
        input: QuerySessionPreparationInput,
        source: Arc<crate::query_execution::completion_facts::FrontendPlanningCpuSource>,
    ) -> Self {
        Self {
            source,
            input: Some(input),
            computation: None,
            contexts: None,
            context_computation: None,
            effective: None,
            admission: None,
            grant: None,
            health: None,
        }
    }

    pub(super) fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.source.local_health_failure())
    }

    pub(super) fn poll(
        &mut self,
        owner: &mut novarocks_query_application::session_control::GovernedActionContext<'_>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<PreparedQuerySession, QueryServiceError>> {
        use novarocks_query_application::cancellation::QueryCancellationView;
        use novarocks_query_application::session_outcome::cancellation_error;
        use std::task::Poll;

        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if let Some(input) = self.input.take() {
            if input.parameters.as_ref().is_some_and(|parameters| {
                !parameters.is_description()
                    && !parameters
                        .contextual_needs(input.statement.query())
                        .is_empty()
            }) {
                self.contexts = Some(QueryContextsEnvironment {
                    source: input.source.clone(),
                    compiler: input.compiler.clone(),
                    interpreter: input.interpreter.clone(),
                    statement_time: input.statement_time,
                });
            }
            let cancellation =
                QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
            if let Some(reason) = cancellation.reason() {
                return Poll::Ready(Err(cancellation_error(reason)));
            }
            let window = owner
                .result_window_alias()
                .filter(|alias| {
                    alias.is_root_backed_internal() && alias.is_for_scope(owner.scope())
                })
                .ok_or_else(|| {
                    super::internal_error("query session CPU lacks its original root retention")
                })?;
            let control = SqlCompileControl::new(
                owner
                    .cancellation()
                    .deadline()
                    .map(tokio::time::Instant::into_std),
                crate::query_execution::planning::sql_cancellation_observation(cancellation),
            );
            let context = input.compiler.property_cpu_context(
                input.original.current_database().to_owned(),
                input.original.statement_sql_semantics(input.statement_time),
                input.interpreter,
                control,
                owner.scope().clone(),
                window,
            );
            let cpu_input = QuerySessionPropertyCpuInput {
                source: input.source,
                parameters: input.parameters,
                statement: input.statement,
                original: input.original,
                registry: input.registry,
                catalogs: input.catalogs,
                context,
            };
            let source = Arc::clone(&self.source);
            self.computation = Some(Box::pin(async move {
                source.query_session_properties(cpu_input).await
            }));
        }
        if let Some(computation) = &mut self.computation {
            let effective = match computation.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(health)) => {
                    self.health = Some(health);
                    return Poll::Pending;
                }
                Poll::Ready(Ok(value)) => value,
            };
            drop(self.computation.take());
            self.effective = Some(effective?);
        }
        if self.admission.is_none() {
            let execution = self
                .effective
                .as_ref()
                .expect("received the original session CPU")
                .state
                .execution_settings();
            let timeout = execution
                .query_timeout_secs()
                .map(std::time::Duration::from_secs);
            use novarocks_query_application::session_control::SessionQueryAdmissionError;
            self.admission = Some(owner.queue_after_session_preparation(timeout).map_err(
                |error| match error {
                    SessionQueryAdmissionError::BindDeadline(
                        novarocks_workload_control::WorkError::ArithmeticOverflow,
                    ) => super::internal_error("query deadline exceeds monotonic clock range"),
                    SessionQueryAdmissionError::BindDeadline(error)
                    | SessionQueryAdmissionError::Queue(error) => {
                        session_query_work_error(owner, error)
                    }
                },
            )?);
        }
        if let Some(environment) = self.contexts.take() {
            // queue_after_session_preparation has frozen the actual timeout.
            // The original preparation retention is live until the grant is accepted.
            let prepared = self
                .effective
                .take()
                .expect("prepared session for bound contexts");
            let window = owner.result_window_alias().ok_or_else(|| {
                super::internal_error("Prepared context CPU lacks its original retention")
            })?;
            let cancellation =
                QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
            let control = SqlCompileControl::new(
                owner
                    .cancellation()
                    .deadline()
                    .map(tokio::time::Instant::into_std),
                crate::query_execution::planning::sql_cancellation_observation(cancellation),
            );
            let context = environment.compiler.property_cpu_context(
                prepared.state.current_database().to_owned(),
                prepared
                    .state
                    .statement_sql_semantics(environment.statement_time),
                environment.interpreter,
                control,
                owner.scope().clone(),
                window,
            );
            let input = QueryContextsCpuInput {
                source: environment.source,
                prepared,
                context,
            };
            let source = self.source.clone();
            self.context_computation =
                Some(Box::pin(async move { source.query_contexts(input).await }));
        }
        if let Some(computation) = self.context_computation.as_mut() {
            match computation.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(health)) => {
                    self.health = Some(health);
                    return Poll::Pending;
                }
                Poll::Ready(Ok(result)) => {
                    drop(self.context_computation.take());
                    self.effective = Some(result?);
                }
            }
        }
        let grant = match self.admission.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        drop(self.admission.take());
        self.grant = Some(grant.map_err(|error| session_query_work_error(owner, error))?);
        let accepted = owner.accept_session_query_grant_in(&mut self.grant);
        if let Err(error) = accepted {
            drop(self.grant.take());
            return Poll::Ready(Err(session_query_work_error(owner, error)));
        }
        Poll::Ready(Ok(self
            .effective
            .take()
            .expect("the original queued query input")))
    }
}

fn session_query_work_error(
    owner: &novarocks_query_application::session_control::GovernedActionContext<'_>,
    error: novarocks_workload_control::WorkError,
) -> QueryServiceError {
    if matches!(error, novarocks_workload_control::WorkError::Cancelled(_)) {
        let view = novarocks_query_application::cancellation::QueryCancellationView::governed(
            owner.cancellation().clone(),
            owner.timeout_ms(),
        );
        if let Some(reason) = view.reason() {
            return novarocks_query_application::session_outcome::cancellation_error(reason);
        }
    }
    super::statement_computation::statement_work_error(error)
}

struct SessionPropertyEvaluation<'a> {
    interpreter: &'a crate::RequiredConstantInterpreter,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    control: &'a SqlCompileControl,
    scope: &'a WorkScope,
    window: &'a ResultWindowAlias,
    catalog: &'a RwLock<novarocks_sql::planning::catalog::PlannerMemoryCatalog>,
    functions: &'a Arc<dyn SqlFunctionCatalog>,
    policy: novarocks_functions::ConstantPolicy,
    parameters: Option<&'a novarocks_sql::compiler::SqlPreparedParameters>,
}

impl SessionPropertyEvaluation<'_> {
    fn evaluate(
        &self,
        original: &SessionSqlState,
        expression: &ast::Expr,
        name: &str,
        value_type: SessionPropertyType,
    ) -> Result<novarocks_functions::ConstantValue, QueryServiceError> {
        let target = match value_type {
            SessionPropertyType::Boolean => RequiredConstantTarget::Boolean,
            SessionPropertyType::Integer => RequiredConstantTarget::Integer,
            SessionPropertyType::Bigint => RequiredConstantTarget::Bigint,
            SessionPropertyType::Double => RequiredConstantTarget::Double,
            SessionPropertyType::Varchar => RequiredConstantTarget::Varchar,
        };
        self.evaluate_target(original, expression, target, name, None)
    }

    fn evaluate_time_zone(
        &self,
        original: &SessionSqlState,
        expression: &ast::Expr,
        source: &str,
    ) -> Result<TimeZoneKey, QueryServiceError> {
        let value = self.evaluate_target(
            original,
            expression,
            RequiredConstantTarget::TimeZone,
            "time_zone_id",
            Some(source),
        )?;
        let constant_failure =
            |error| QueryServiceError::from_failure_cause(SharedFailureCause::new(error));
        let missing = || super::internal_error("SET TIME ZONE expression returned NULL");
        if matches!(
            value.value_type().value_type().kind(),
            ValueKind::String | ValueKind::BoundedCharacter(_)
        ) {
            let name = value
                .utf8_observed(CompilePhase::Validate, self.control)
                .map_err(constant_failure)?
                .ok_or_else(missing)?;
            if name.is_empty() {
                return Err(super::internal_error("Zone id is an empty string"));
            }
            return TimeZoneKey::parse(name).ok_or_else(|| {
                QueryServiceError::new(
                    novarocks_query_application::session_error::QueryServiceErrorKind::Unsupported,
                    format!("Time zone not supported: {name}"),
                )
            });
        }
        let Some(TemporalValue::IntervalDayTime(offset)) = value
            .temporal_observed(CompilePhase::Validate, self.control)
            .map_err(constant_failure)?
        else {
            return Err(missing());
        };
        const MICROS_PER_MINUTE: i64 = 60_000_000;
        if offset.picoseconds_of_microsecond() != 0 || offset.micros() % MICROS_PER_MINUTE != 0 {
            return Err(QueryServiceError::from_user_error(
                novarocks_sql::analyze_error::AnalyzeError::invalid_literal(
                    "Invalid TIME ZONE offset interval: interval contains seconds",
                    expression.span(),
                )
                .to_user_error(None),
            ));
        }
        let minutes = offset.micros() / MICROS_PER_MINUTE;
        i16::try_from(minutes)
            .ok()
            .and_then(TimeZoneKey::from_offset_minutes)
            .ok_or_else(|| {
                use novarocks_query_application::execution_error::{
                    SqlExecutionErrorDescriptor, SqlExecutionErrorKind,
                };
                use novarocks_user_error::{RetryClass, UserError};
                QueryServiceError::from_user_error(UserError::from_descriptor(
                    SqlExecutionErrorDescriptor::for_kind(
                        SqlExecutionErrorKind::InvalidFunctionArgument,
                    )
                    .descriptor,
                    format!("Invalid offset minutes {minutes}"),
                    None,
                    RetryClass::Never,
                ))
            })
    }

    fn evaluate_target(
        &self,
        original: &SessionSqlState,
        expression: &ast::Expr,
        target: RequiredConstantTarget,
        name: &str,
        time_zone_source: Option<&str>,
    ) -> Result<novarocks_functions::ConstantValue, QueryServiceError> {
        let prepared = {
            let catalog = self.catalog.read().map_err(super::poisoned_state)?;
            prepare_required_constant(RequiredConstantRequest {
                input: match self.parameters {
                    Some(parameters) => RequiredConstantInput::PreparedExpression {
                        expression,
                        parameters,
                    },
                    None => RequiredConstantInput::Expression(expression),
                },
                target,
                catalog: &SqlPlannerTableSnapshot::new(&*catalog),
                current_database: original.current_database(),
                functions: Arc::clone(self.functions),
                sealed_functions: self.interpreter.sealed_functions(),
                sql_semantics: &self.sql_semantics,
                constant_policy: self.policy,
                control: self.control,
            })
        }
        .map_err(|error| compile_error_for_target(error, time_zone_source))?;
        self.interpreter
            .evaluate(prepared, self.scope, self.window)
            .map_err(|error| {
                if let (Some(_), SqlConstantEvaluationError::Source(cause)) =
                    (time_zone_source, &error)
                {
                    return QueryServiceError::from_failure_cause(cause.clone());
                }
                evaluation_error(error, name)
            })
    }
}

fn compile_error_for_target(
    error: SqlCompileError,
    time_zone_source: Option<&str>,
) -> QueryServiceError {
    use novarocks_type_contract::CompileControlError as C;
    match error {
        SqlCompileError::Cancelled => QueryServiceError::from_compile_control(C::Cancelled),
        SqlCompileError::DeadlineExceeded => {
            QueryServiceError::from_compile_control(C::DeadlineExceeded)
        }
        SqlCompileError::ResourceExhausted => {
            QueryServiceError::from_compile_control(C::ResourceExhausted)
        }
        SqlCompileError::CompilationSource(cause) => QueryServiceError::from_failure_cause(cause),
        SqlCompileError::Analyze(error) => {
            if let Some(control) = error.control_error() {
                return QueryServiceError::from_compile_control(control);
            }
            if let Some(cause) = error.source_failure() {
                return QueryServiceError::from_failure_cause(cause.clone());
            }
            if error.kind() == novarocks_sql::analyze_error::AnalyzeErrorKind::Internal {
                return super::internal_error(error.to_string());
            }
            if error.kind()
                == novarocks_sql::analyze_error::AnalyzeErrorKind::UnavailableImplementation
            {
                return QueryServiceError::from_user_error(error.to_user_error(None));
            }
            if let Some(source) = time_zone_source {
                let source = (error.kind()
                    != novarocks_sql::analyze_error::AnalyzeErrorKind::TypeMismatch)
                    .then_some(source);
                return QueryServiceError::from_user_error(error.to_user_error(source));
            }
            QueryServiceError::from_user_error(
                SessionAdmitError::InvalidSessionProperty.without_location(error.to_string()),
            )
        }
        other => super::internal_error(other.to_string()),
    }
}
fn evaluation_error(error: SqlConstantEvaluationError, name: &str) -> QueryServiceError {
    match error {
        SqlConstantEvaluationError::Control(error) => {
            QueryServiceError::from_compile_control(error)
        }
        SqlConstantEvaluationError::Constant(error) => {
            QueryServiceError::from_failure_cause(SharedFailureCause::new(error))
        }
        SqlConstantEvaluationError::Outer(cause) => QueryServiceError::from_failure_cause(cause),
        SqlConstantEvaluationError::Source(cause) => {
            let selected_data = cause.failure_fact().class() == FailureClass::Data
                && cause.downcast_ref::<RequiredExpressionRowError>().is_some();
            let required_capability = cause.downcast_ref::<crate::query_execution::required_constant_eval::RequiredConstantEligibilityFailure>().is_some();
            if selected_data || required_capability {
                QueryServiceError::from_user_error_with_cause(
                    SessionAdmitError::InvalidSessionProperty.without_location(format!(
                        "Unable to set session property '{name}': {cause}"
                    )),
                    cause,
                )
            } else {
                QueryServiceError::from_failure_cause(cause)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };

    struct SystemOnly;
    impl SessionCatalogPropertyPort for SystemOnly {
        fn session_property_catalog(
            &self,
            _: &str,
        ) -> Result<
            novarocks_query_application::sql::session::registry::SessionCatalogPropertyAdmission,
            QueryServiceError,
        > {
            panic!("a system property has no catalog scope")
        }
    }

    struct Fixture {
        root: RootWork,
        window: ResultWindowAlias,
        _control: WorkloadControl,
    }
    impl Fixture {
        fn new() -> Self {
            let control = WorkloadControl::try_new(
                WorkloadConfig::default(),
                ResourceConfig {
                    total_bytes: 1 << 20,
                    control_bytes: 1024,
                    per_scope_bytes: (1 << 20) - 1024,
                },
            )
            .unwrap();
            control
                .configure_result_capacity(ResultCapacityConfig::V1)
                .unwrap();
            control.mark_ready().unwrap();
            let (root, grant) = control
                .root_admission()
                .try_begin_root_with_result(
                    WorkRequest::new(WorkClass::Management),
                    novarocks_workload_control::ResultWindowClass::Local,
                )
                .unwrap();
            Self {
                root,
                window: grant.retain_alias(),
                _control: control,
            }
        }
        fn input(&self, sql: &str, staged: SessionSqlState) -> SessionPropertyCpuInput {
            let ast::TrinoStatement::SetSession(set) =
                novarocks_parser::parser::parse_statement(sql).unwrap()
            else {
                panic!("SET SESSION fixture");
            };
            let mutation = ScopedSessionMutation::Set {
                name: set.name,
                value: set.value,
            };
            SessionPropertyCpuInput {
                source: Arc::from(sql),
                mutation: SessionPropertyMutation {
                    mutation: Box::new(mutation),
                    parameters: None,
                    catalogs: Arc::new(SystemOnly),
                },
                staged,
                statement_time: crate::application::test_statement_time(),
                registry: Arc::new(SessionPropertyRegistry::new().unwrap()),
                interpreter: Arc::new(crate::query_execution::required_constant_eval::test_required_constant_interpreter()),
                control: SqlCompileControl::unbounded(), scope: self.root.owner.scope(), window: self.window.clone(),
                catalog: Arc::new(RwLock::new(Default::default())),
                functions: Arc::new(novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap()),
                policy: crate::application::test_constant_policy(),
            }
        }
        fn close(self) {
            assert_eq!(self.window.retained_payload_bytes(), 0);
            drop(self.window);
            self.root.owner.complete();
            self.root.business.release();
        }
    }
    #[test]
    fn scoped_query_cpu_uses_real_parser_and_checked_root_retention() {
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1024,
                per_scope_bytes: (1 << 20) - 1024,
            },
        )
        .unwrap();
        workload
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        workload.mark_ready().unwrap();
        let root = workload
            .root_admission()
            .begin_query_root_with_deferred_deadline()
            .unwrap();
        let window = ResultWindowAlias::from_root_owner(&root.owner).unwrap();
        let registry = Arc::new(SessionPropertyRegistry::new().unwrap());
        let live = SessionSqlState::default();
        let source = "WITH SESSION query_timeout = 1 + 2, enable_scan_datacache = true AND true, explain_detail = 'LOGICAL_COSTS' SELECT 1 AS answer";
        let parsed = novarocks_parser::parser::parse_query(source).unwrap();
        let catalogs = Arc::new(
            crate::query_execution::kernels::SessionCatalogResolver::new(
                Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
                None,
                Arc::new(crate::query_execution::compiler::TestConnectorControlRegistry::default()),
                Arc::clone(&registry),
                Arc::new(
                    novarocks_query_application::system_tables::SystemTableRegistry::try_new([])
                        .unwrap(),
                ),
            ),
        );
        let prepared = QuerySessionPropertyCpuInput {
            source: Arc::from(source),
            parameters: None,
            statement: super::super::query_session_action::QueryFamily::Read(Box::new(parsed.clone())),
            original: live.clone(),
            registry,
            catalogs,
            context: PropertyCpuContext {
                current_database: live.current_database().to_owned(),
                sql_semantics: live.sql_semantics().clone(),
                interpreter: Arc::new(crate::query_execution::required_constant_eval::test_required_constant_interpreter()),
                control: SqlCompileControl::unbounded(),
                scope: root.owner.scope(),
                window: window.clone(),
                catalog: Arc::new(RwLock::new(Default::default())),
                functions: Arc::new(novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap()),
                policy: crate::application::test_constant_policy(),
            },
        }.run().unwrap();
        assert_eq!(prepared.statement.query(), &parsed);
        assert_eq!(
            prepared.state.execution_settings().query_timeout_secs(),
            Some(3)
        );
        assert!(prepared.state.execution_settings().enable_scan_datacache());
        assert_eq!(
            prepared.state.execution_settings().explain_detail(),
            novarocks_query_application::sql::session::ExplainDetail::LogicalCosts
        );
        assert_eq!(live.execution_settings().query_timeout_secs(), None);
        assert!(!live.execution_settings().enable_scan_datacache());
        assert_eq!(window.retained_payload_bytes(), 0);
        drop(window);
        root.owner.complete();
    }

    #[test]
    fn set_prepares_typed_values_and_effects_without_optional_folding() {
        let fixture = Fixture::new();
        let mut state = SessionSqlState::default();
        state
            .apply_session_property_text(
                &SessionPropertyRegistry::new().unwrap(),
                "disable_optimizer_rules",
                "FoldConstant",
            )
            .unwrap();
        let prepared = fixture
            .input(
                "SET SESSION pipeline_dop = 1 + CASE WHEN false AND (1 / 0 = 1) THEN 0 ELSE 2 END",
                state,
            )
            .run()
            .unwrap();
        assert_eq!(prepared.staged.execution_settings().pipeline_dop(), Some(3));
        assert_eq!(
            prepared.effects.set_properties().collect::<Vec<_>>(),
            vec![("pipeline_dop".into(), "3".into())]
        );
        assert!(fixture.window.retained_payload_bytes() > 0);
        drop(prepared);
        fixture.close();
    }
    #[test]
    fn refused_assignment_does_not_publish_session_state() {
        let fixture = Fixture::new();
        let state = SessionSqlState::default();
        let error = fixture
            .input("SET SESSION trc_unknown = true", state.clone())
            .run()
            .err()
            .unwrap();
        assert_eq!(
            error.user_error().unwrap().code().as_str(),
            "sql.admit.invalid_session_property"
        );
        assert_eq!(state.execution_settings().pipeline_dop(), None);
        assert!(state.render_session_property("pipeline_dop").is_none());
        assert!(error.user_error().unwrap().location().is_some());
        drop(error);
        fixture.close();
    }
    #[test]
    fn property_locations_distinguish_lookup_validation_and_required_evaluation() {
        let fixture = Fixture::new();
        for (sql, location) in [
            ("SET SESSION trc_unknown = 1", true),
            ("SET SESSION decimal_overflow_to_double = 1 / 0", true),
            ("SET SESSION query_timeout = -1", true),
            ("SET SESSION pipeline_dop = 1.5", false),
            ("SET SESSION pipeline_dop = NULL", true),
            ("SET SESSION pipeline_dop = missing_column", false),
            ("SET SESSION pipeline_dop = (SELECT 1)", false),
            ("SET SESSION pipeline_dop = 1 / 0", false),
        ] {
            let error = fixture
                .input(sql, SessionSqlState::default())
                .run()
                .err()
                .unwrap_or_else(|| panic!("{sql} must fail"));
            let user = error.user_error().unwrap();
            assert_eq!(
                user.code().as_str(),
                "sql.admit.invalid_session_property",
                "{sql}"
            );
            assert_eq!(user.location().is_some(), location, "{sql}");
        }
        fixture.close();
    }

    #[test]
    fn declared_required_ineligibility_is_distinct_from_missing_execution_capability() {
        let original = SharedFailureCause::new(
            novarocks_sql::compiler::PackageSemanticsError::UnsupportedCapability(
                "an uninstalled execution capability".into(),
            ),
        );
        let error = evaluation_error(
            SqlConstantEvaluationError::Source(original.clone()),
            "pipeline_dop",
        );
        assert_eq!(error.failure_cause(), Some(&original));
        assert!(
            error
                .user_error()
                .is_none_or(|error| error.code().as_str() != "sql.admit.invalid_session_property")
        );
        let eligibility = SharedFailureCause::new(
            crate::query_execution::required_constant_eval::RequiredConstantEligibilityFailure::new(
            ),
        );
        let error = evaluation_error(
            SqlConstantEvaluationError::Source(eligibility.clone()),
            "pipeline_dop",
        );
        assert_eq!(
            error.user_error().unwrap().code().as_str(),
            "sql.admit.invalid_session_property"
        );
        assert_eq!(error.failure_cause(), Some(&eligibility));
    }

    #[derive(Debug)]
    struct OriginalData;
    impl std::fmt::Display for OriginalData {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("selected division failure")
        }
    }
    impl std::error::Error for OriginalData {}
    impl FailureProjection for OriginalData {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            novarocks_functions::DataFailureReason::DivisionByZero.failure_fact()
        }
    }
    #[test]
    fn property_business_boundary_preserves_outer_and_control_source_identity() {
        let source = SharedFailureCause::new(OriginalData);
        let source_error = evaluation_error(
            SqlConstantEvaluationError::Source(source.clone()),
            "pipeline_dop",
        );
        assert_eq!(source_error.failure_cause(), Some(&source));
        assert!(
            source_error
                .user_error()
                .is_none_or(|error| error.code().as_str() != "sql.admit.invalid_session_property")
        );
        let compile =
            compile_error_for_target(SqlCompileError::CompilationSource(source.clone()), None);
        assert_eq!(compile.failure_cause(), Some(&source));
        assert!(
            compile
                .user_error()
                .is_none_or(|error| error.code().as_str() != "sql.admit.invalid_session_property")
        );
        let outer = evaluation_error(
            SqlConstantEvaluationError::Outer(source.clone()),
            "pipeline_dop",
        );
        assert_eq!(outer.failure_cause(), Some(&source));
        assert!(
            outer
                .user_error()
                .is_none_or(|error| error.code().as_str() != "sql.admit.invalid_session_property")
        );
        let control = evaluation_error(
            SqlConstantEvaluationError::Control(
                novarocks_type_contract::CompileControlError::Source(source.clone()),
            ),
            "pipeline_dop",
        );
        assert!(
            control
                .user_error()
                .is_none_or(|error| error.code().as_str() != "sql.admit.invalid_session_property")
        );
        let Some(novarocks_type_contract::CompileControlError::Source(original)) =
            control.compile_control_error()
        else {
            panic!("original control source");
        };
        assert_eq!(original, source);
    }
}
