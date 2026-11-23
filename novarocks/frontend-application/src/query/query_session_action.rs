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

//! Typed root session preparation within the original Read or EXPLAIN Action.

use super::session_property_computation::{
    PreparedQuerySession, QuerySessionPreparation, QuerySessionPreparationInput,
};
use super::*;
use crate::query_execution::completion_facts::FrontendPlanningCpuSource;
use novarocks_query_application::session_control::{
    GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
    QuerySessionDeadline, StatementActionValue,
};
use novarocks_query_application::{
    api::StatementNativeHealthFailure, sql::trino_admission::TrinoStatementSource,
};
use novarocks_sql::compiler::SqlPreparedParameters;
use std::task::{Context, Poll};

pub(super) enum QueryFamily {
    Read(Box<ast::Query>),
    Explain(compiler::ExplainInput),
}

impl QueryFamily {
    pub(super) fn query(&self) -> &ast::Query {
        match self {
            Self::Read(query) => query,
            Self::Explain(input) => &input.query,
        }
    }

    fn from_trino(
        action: novarocks_query_application::sql::trino_admission::TrinoQueryAction,
    ) -> Self {
        use novarocks_query_application::sql::trino_admission::TrinoQueryAction;
        match action {
            TrinoQueryAction::Read(query) => Self::Read(Box::new(query)),
            TrinoQueryAction::Explain(query) => {
                Self::Explain(compiler::ExplainInput::plan(Box::new(query)))
            }
            TrinoQueryAction::ExplainAnalyze(query) => {
                Self::Explain(compiler::ExplainInput::analyze(Box::new(query)))
            }
        }
    }

    fn window_class(&self) -> ResultWindowClass {
        match self {
            Self::Read(_) => ResultWindowClass::Client,
            Self::Explain(input) if input.is_analyze() => ResultWindowClass::Internal,
            Self::Explain(_) => ResultWindowClass::Local,
        }
    }
}

#[derive(Clone)]
pub(crate) struct QueryActionEnvironment {
    pub(super) compiler: compiler::FrontendQueryCompiler,
    pub(super) topology: BackendTopologyService,
    pub(super) role: ClusterRole,
    pub(super) optimizer_limit: u64,
    pub(super) state_store: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    pub(super) launcher: Arc<dyn LogicalExecutionLauncher>,
}
impl QueryActionEnvironment {
    pub(super) fn new(service: &FrontendQueryService) -> Self {
        Self {
            compiler: service.query_compiler.clone(),
            topology: service.topology.clone(),
            role: service.role,
            optimizer_limit: service.optimizer_query_mem_limit_bytes,
            state_store: service.state_store.clone(),
            launcher: Arc::clone(&service.logical_execution_launcher),
        }
    }
}

pub(super) struct QueryActionInputs {
    pub(super) scope: novarocks_workload_control::WorkScope,
    pub(super) deadline: Option<Instant>,
    pub(super) cancellation: QueryCancellationView,
    pub(super) window: ResultWindowAlias,
    pub(super) parameters: Option<Arc<SqlPreparedParameters>>,
    pub(super) planning_cpu: Option<Arc<FrontendPlanningCpuSource>>,
    pub(super) statement_time: novarocks_type_contract::StatementTimeEnvironment,
    #[cfg(novarocks_dev_controls)]
    pub(super) observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
    #[cfg(novarocks_dev_controls)]
    pub(super) statement_token: StatementToken,
}

#[expect(
    clippy::large_enum_variant,
    reason = "Both preparation paths are already carried inside the boxed governed query action."
)]
enum QueryFamilyPreparation {
    Ready(Option<PreparedQuerySession>),
    Declared(QuerySessionPreparation),
}

/// The original registered Action owns both the prefix and the normal body.
struct ScopedQueryAction {
    preparation: QueryFamilyPreparation,
    environment: QueryActionEnvironment,
    source: TrinoStatementSource,
    planning_cpu: Arc<FrontendPlanningCpuSource>,
    action: Option<Box<dyn GovernedStatementAction>>,
    statement_time: novarocks_type_contract::StatementTimeEnvironment,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
    #[cfg(novarocks_dev_controls)]
    statement_token: StatementToken,
}

impl GovernedStatementAction for ScopedQueryAction {
    fn external_effect_disposition(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        self.action
            .as_ref()
            .and_then(|action| action.external_effect_disposition())
    }

    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        let preparation = match &self.preparation {
            QueryFamilyPreparation::Ready { .. } => None,
            QueryFamilyPreparation::Declared(preparation) => preparation.local_health_failure(),
        };
        preparation.or_else(|| {
            self.action
                .as_ref()
                .and_then(|action| action.local_health_failure())
        })
    }

    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if let Some(action) = self.action.as_mut() {
            return action.handoff_residual_in(target);
        }
        // A prefix business result can be returned only after CPU receive
        // proves actual exit and the original queue future has been retired.
        Ok(())
    }

    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if self.action.is_none() {
            let prepared = match &mut self.preparation {
                QueryFamilyPreparation::Ready(input) => input
                    .take()
                    .ok_or_else(|| internal_error("query lost its original input"))?,
                QueryFamilyPreparation::Declared(preparation) => {
                    match preparation.poll(owner, cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => result?,
                    }
                }
            };
            let window = owner
                .result_window_alias()
                .ok_or_else(|| internal_error("scoped query lacks its admitted window"))?;
            let inputs = QueryActionInputs {
                scope: owner.scope().clone(),
                deadline: owner
                    .cancellation()
                    .deadline()
                    .map(tokio::time::Instant::into_std),
                cancellation: QueryCancellationView::governed(
                    owner.cancellation().clone(),
                    owner.timeout_ms(),
                ),
                window,
                parameters: prepared.parameters,
                planning_cpu: Some(Arc::clone(&self.planning_cpu)),
                statement_time: self.statement_time,
                #[cfg(novarocks_dev_controls)]
                observation: self.observation.clone(),
                #[cfg(novarocks_dev_controls)]
                statement_token: self.statement_token,
            };
            let family =
                prepared.state.presentation().copied().ok_or_else(|| {
                    internal_error("scoped query requires an explicit presentation")
                })?;
            self.action = Some(match prepared.statement {
                QueryFamily::Read(query) => read_action::build_read_action(
                    &self.environment,
                    inputs,
                    Arc::clone(self.source.sql()),
                    query,
                    prepared.state,
                    FrontendQueryPurpose::ClientRows,
                )?,
                QueryFamily::Explain(explain) => explain_action::build_explain_action(
                    &self.environment,
                    inputs,
                    self.source.sql().to_string(),
                    explain,
                    prepared.state,
                    family,
                )?,
            });
        }
        self.action
            .as_mut()
            .expect("ready original query action")
            .poll_action(owner, cx)
    }
}

impl FrontendQuerySession {
    pub(super) async fn execute_trino_query_family(
        &self,
        statement_source: TrinoStatementSource,
        parameters: Option<Arc<SqlPreparedParameters>>,
        action: novarocks_query_application::sql::trino_admission::TrinoQueryAction,
        statement_time: novarocks_type_contract::StatementTimeEnvironment,
    ) -> Result<StatementResult, QueryServiceError> {
        let family = QueryFamily::from_trino(action);
        use novarocks_query_application::{
            session::{StatementAdmission, StatementOutputShape},
            statement_kind::StatementKind,
        };
        let kind = match &family {
            QueryFamily::Read(_) => StatementKind::Query,
            QueryFamily::Explain(_) => StatementKind::Explain,
        };
        let admitted = StatementAdmission {
            kind,
            shape: StatementOutputShape::ResultSet,
        };
        self.execute_query_family(
            statement_source,
            parameters,
            family,
            admitted,
            statement_time,
        )
        .await
    }

    async fn execute_query_family(
        &self,
        statement_source: TrinoStatementSource,
        parameters: Option<Arc<SqlPreparedParameters>>,
        family: QueryFamily,
        admitted: novarocks_query_application::session::StatementAdmission,
        statement_time: novarocks_type_contract::StatementTimeEnvironment,
    ) -> Result<StatementResult, QueryServiceError> {
        let source = statement_source.sql().clone();
        self.admission.publish(admitted);
        *self.output_shape.lock().map_err(poisoned_state)? = Some(admitted.shape);
        let class = family.window_class();
        let plain_explain = class == ResultWindowClass::Local;
        let needs_properties = !family.query().session_properties.is_empty()
            || parameters
                .as_ref()
                .is_some_and(|parameters| !parameters.contextual_needs(family.query()).is_empty());
        #[cfg(novarocks_dev_controls)]
        let is_read = class == ResultWindowClass::Client;
        let token = self.token()?;
        let (statement, declared_original) = if !needs_properties {
            // A known session uses the original eager queue admission.
            // Preserve Read and EXPLAIN's different deadline diagnostics.
            let (deadline, mut timeout_ms) = {
                let state = self.state.lock().map_err(poisoned_state)?;
                governed_query_deadline(&state)?
            };
            if class != ResultWindowClass::Client {
                timeout_ms = deadline.map(|end| {
                    timeout_message_millis(end.saturating_duration_since(Instant::now()))
                });
            }
            let statement = self
                .service
                .query_control
                .begin_queued_governed_query_statement_with_result(
                    token,
                    &self.service.workload_root_admission,
                    deadline.map(tokio::time::Instant::from_std),
                    timeout_ms,
                    Some(Arc::clone(&source)),
                    class,
                )
                .await
                .map_err(|error| self.governed_statement_begin_error(error))?;
            (statement, None)
        } else {
            let original = self.state.lock().map_err(poisoned_state)?.clone();
            let clock = if plain_explain {
                let (deadline, timeout_ms) = governed_query_deadline(&original)?;
                QuerySessionDeadline::Inherit {
                    deadline: deadline.map(tokio::time::Instant::from_std),
                    timeout_ms,
                }
            } else {
                QuerySessionDeadline::ResolveFromProperties
            };
            let statement = self
                .service
                .query_control
                .begin_governed_query_session_preparation(
                    token,
                    &self.service.workload_root_admission,
                    clock,
                    Arc::clone(&source),
                    class,
                )
                .map_err(|error| self.governed_statement_begin_error(error))?;
            (statement, Some(original))
        };
        let planning_cpu = self.service.query_compiler.new_planning_cpu_source();
        #[cfg(novarocks_dev_controls)]
        let observation = match self
            .service
            .statement_observation
            .as_ref()
            .map(|owner| owner.bind_statement(statement.token(), &source))
            .transpose()
        {
            Ok(observation) => observation.flatten(),
            Err(error) => return Ok(self.governed_typed_error(internal_error(error), statement)),
        };
        #[cfg(novarocks_dev_controls)]
        let handoff_statement = statement.token();
        #[cfg(novarocks_dev_controls)]
        let handoff_target = observation
            .as_ref()
            .and_then(|handle| handle.statement_target());
        let preparation = if let Some(original) = declared_original {
            QueryFamilyPreparation::Declared(QuerySessionPreparation::new(
                QuerySessionPreparationInput {
                    source: Arc::clone(&source),
                    parameters,
                    statement: family,
                    original,
                    statement_time,
                    compiler: self.service.query_compiler.clone(),
                    registry: Arc::clone(&self.service.session_properties),
                    catalogs: Arc::clone(&self.service.session_catalog_resolver),
                    interpreter: Arc::clone(&self.service.required_constant_interpreter),
                },
                Arc::clone(&planning_cpu),
            ))
        } else {
            QueryFamilyPreparation::Ready(Some(PreparedQuerySession {
                statement: family,
                parameters,
                state: self.state.lock().map_err(poisoned_state)?.clone(),
            }))
        };
        let action = ScopedQueryAction {
            preparation,
            environment: QueryActionEnvironment::new(&self.service),
            source: statement_source,
            planning_cpu,
            action: None,
            statement_time,
            #[cfg(novarocks_dev_controls)]
            observation,
            #[cfg(novarocks_dev_controls)]
            statement_token: statement.token(),
        };
        let result = self
            .execute_observed_statement_action(statement, Box::new(action))
            .await?;
        #[cfg(novarocks_dev_controls)]
        if is_read {
            self.finish_observed_read_handoff(handoff_statement, handoff_target, &result)
                .await?;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_query_admission_moves_the_same_query_through_the_existing_window_classes() {
        use novarocks_parser::ast::SyntaxEq;
        use novarocks_query_application::sql::trino_admission::{
            TrinoStatementAction, admit_trino_statement,
        };
        for (sql, query_sql, expected) in [
            ("SELECT 1", "SELECT 1", ResultWindowClass::Client),
            (
                "EXPLAIN (TYPE LOGICAL) SELECT 1",
                "SELECT 1",
                ResultWindowClass::Local,
            ),
            (
                "EXPLAIN ANALYZE VERBOSE WITH SESSION pipeline_dop = 2 SELECT 1",
                "WITH SESSION pipeline_dop = 2 SELECT 1",
                ResultWindowClass::Internal,
            ),
        ] {
            let statement = novarocks_parser::parser::parse_statement(sql).unwrap();
            let TrinoStatementAction::Query(action) =
                admit_trino_statement(sql, statement).unwrap()
            else {
                panic!("original query entry: {sql}");
            };
            let family = QueryFamily::from_trino(action);
            assert_eq!(family.window_class(), expected);
            let query = novarocks_parser::parser::parse_query(query_sql).unwrap();
            assert!(family.query().syntax_eq(&query));
        }
    }
}
