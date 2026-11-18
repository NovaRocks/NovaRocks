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
use novarocks_query_application::api::StatementNativeHealthFailure;
use novarocks_query_application::session_control::{
    GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
    QuerySessionDeadline, StatementActionValue,
};
use std::task::{Context, Poll};

#[derive(Clone, Copy)]
pub(super) enum ScopedQueryAssignments {
    LegacyRootHints,
    #[allow(dead_code, reason = "P09 supplies the declared WITH SESSION entrance")]
    DeclaredProperties,
}

/// The retired parser folds several hint forms into SessionProperty nodes.
/// Only the four properties consumed by the original query owners select
/// the legacy preparation path.
pub(super) fn uses_legacy_root_hints(parsed: &ParsedStatement) -> bool {
    let query = match parsed {
        ParsedStatement::Query(query) => query,
        ParsedStatement::ExplainQuery(explain) => explain.query.as_ref(),
        _ => return false,
    };
    query.session_properties.iter().any(|property| {
        let [name] = property.name.parts.as_slice() else {
            return false;
        };
        [
            "sql_mode",
            "decimal_overflow_to_double",
            "query_mem_limit",
            "recursive_cte_max_depth",
        ]
        .iter()
        .any(|supported| name.value.eq_ignore_ascii_case(supported))
    })
}

pub(super) struct QueryActionEnvironment {
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
    pub(super) planning_cpu: Option<Arc<FrontendPlanningCpuSource>>,
    pub(super) root_session_admitted: bool,
    #[cfg(novarocks_dev_controls)]
    pub(super) observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
    #[cfg(novarocks_dev_controls)]
    pub(super) statement_token: StatementToken,
}

enum QueryFamilyPreparation {
    Legacy(Option<PreparedQuerySession>),
    Declared(QuerySessionPreparation),
}

/// The original registered Action owns both the prefix and the normal body.
struct ScopedQueryAction {
    preparation: QueryFamilyPreparation,
    environment: QueryActionEnvironment,
    source: Arc<str>,
    planning_cpu: Arc<FrontendPlanningCpuSource>,
    action: Option<Box<dyn GovernedStatementAction>>,
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
            QueryFamilyPreparation::Legacy(_) => None,
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
            let (prepared, root_session_admitted) = match &mut self.preparation {
                QueryFamilyPreparation::Legacy(prepared) => (
                    prepared
                        .take()
                        .ok_or_else(|| internal_error("legacy query lost its original input"))?,
                    false,
                ),
                QueryFamilyPreparation::Declared(preparation) => {
                    match preparation.poll(owner, cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => (result?, true),
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
                planning_cpu: Some(Arc::clone(&self.planning_cpu)),
                root_session_admitted,
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
                statement @ ParsedStatement::Query(_) => read_action::build_read_action(
                    &self.environment,
                    inputs,
                    Arc::clone(&self.source),
                    statement,
                    prepared.state,
                    FrontendQueryPurpose::ClientRows,
                )?,
                ParsedStatement::ExplainQuery(explain) => explain_action::build_explain_action(
                    &self.environment,
                    inputs,
                    self.source.to_string(),
                    explain,
                    prepared.state,
                    family,
                )?,
                _ => {
                    return Poll::Ready(Err(internal_error(
                        "scoped Action lost its original query",
                    )));
                }
            });
        }
        self.action
            .as_mut()
            .expect("ready original query action")
            .poll_action(owner, cx)
    }
}

impl FrontendQuerySession {
    /// The parser entrance chooses which assignment semantics the original
    /// read or EXPLAIN Action receives.
    pub(super) async fn execute_scoped_query_family(
        &self,
        source: Arc<str>,
        parsed: ParsedStatement,
        assignments: ScopedQueryAssignments,
    ) -> Result<StatementResult, QueryServiceError> {
        let (query, class, plain_explain) = match &parsed {
            ParsedStatement::Query(query) => (query, ResultWindowClass::Client, false),
            ParsedStatement::ExplainQuery(explain)
                if explain.format == ast::ExplainFormat::Analyze =>
            {
                (explain.query.as_ref(), ResultWindowClass::Internal, false)
            }
            ParsedStatement::ExplainQuery(explain) => {
                (explain.query.as_ref(), ResultWindowClass::Local, true)
            }
            _ => {
                return Err(internal_error(
                    "scoped query entry requires a read or EXPLAIN",
                ));
            }
        };
        let admitted =
            novarocks_query_application::session::StatementAdmission::from_statement(&parsed);
        self.admission.publish(admitted);
        *self.output_shape.lock().map_err(poisoned_state)? = Some(admitted.shape);
        if query.session_properties.is_empty() {
            return match parsed {
                ParsedStatement::Query(_) => {
                    self.execute_governed_read(source.to_string(), parsed).await
                }
                _ => {
                    self.execute_typed_statement(source.to_string(), parsed)
                        .await
                }
            };
        }

        #[cfg(novarocks_dev_controls)]
        let is_read = matches!(&parsed, ParsedStatement::Query(_));
        let token = self.token()?;
        let legacy = matches!(assignments, ScopedQueryAssignments::LegacyRootHints);
        let (statement, declared_original) = if legacy {
            // Use the original eager queue admission for legacy hints. This
            // retains the serving-admission/cancellation error boundary and
            // the different Read and EXPLAIN deadline diagnostics.
            let (deadline, mut timeout_ms) = {
                let state = self.state.lock().map_err(poisoned_state)?;
                governed_query_deadline(&state)?
            };
            if matches!(&parsed, ParsedStatement::ExplainQuery(_)) {
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
                    statement: parsed,
                    original,
                    compiler: self.service.query_compiler.clone(),
                    registry: Arc::clone(&self.service.session_properties),
                    catalogs: Arc::clone(&self.service.session_catalog_resolver),
                    interpreter: Arc::clone(&self.service.required_constant_interpreter),
                },
                Arc::clone(&planning_cpu),
            ))
        } else {
            QueryFamilyPreparation::Legacy(Some(PreparedQuerySession {
                statement: parsed,
                state: self.state.lock().map_err(poisoned_state)?.clone(),
            }))
        };
        let action = ScopedQueryAction {
            preparation,
            environment: QueryActionEnvironment::new(&self.service),
            source,
            planning_cpu,
            action: None,
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
    fn parsed_legacy_root_hints_select_only_existing_query_consumers() {
        for (source, scoped) in [
            (
                "SELECT /*+ SET_VAR(sql_mode = 'ALLOW_THROW_EXCEPTION') */ 1",
                true,
            ),
            (
                "SELECT /*+ SET_VAR(decimal_overflow_to_double = 'ON') */ 1",
                true,
            ),
            (
                "EXPLAIN VERBOSE SELECT /*+ SET_VAR(query_mem_limit = 1024) */ 1",
                true,
            ),
            (
                "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n) SELECT /*+ SET_VAR(recursive_cte_max_depth = 4) */ x FROM n",
                true,
            ),
            (
                "SELECT /*+ SET_VAR(query_timeout = 2, trc_unknown_property = 3) */ 1",
                false,
            ),
            ("SELECT 1", false),
        ] {
            let parsed = parse_single_statement(source).unwrap();
            assert_eq!(uses_legacy_root_hints(&parsed), scoped, "{source}");
        }
    }
}
