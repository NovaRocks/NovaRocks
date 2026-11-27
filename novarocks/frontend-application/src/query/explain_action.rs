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

//! EXPLAIN runs in the original registered statement, not a second Work role.

use super::read_action::io_source::ReadMetadataIoSource;
use super::read_action::{
    ProviderClose, ReadNativeServices, ReadNativeStageTail, ReadNativeTail, close_provider,
    poll_provider_close,
};
use super::statement_computation::StatementStageAdmission;
use super::*;
use crate::query::compiler::{ExplainPreparationPorts, PreparedAsyncExplain, PreparedAsyncProfile};
use crate::query_execution::completion_facts::{
    FrontendCompletionCustody, FrontendPlanningCpuSource,
};
use crate::query_execution::dml::delete::{
    self, DeleteExplainStatementInput, DeletePath, OwnedDeleteStatement,
};
use crate::query_execution::kernels::DmlExecutionKernel;
use crate::query_execution::logical_execution::OriginalComputationStart;
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_query_application::api::StatementNativeHealthFailure;
use novarocks_query_application::api::{
    ExecutionOutputObligation, LocalResultProducer, OwnedLocalResult,
};
use novarocks_query_application::session_control::{
    GovernedActionContext, GovernedStatementAction, GovernedStatementResidual, StatementActionValue,
};
use novarocks_spi::connector::{
    ConnectorIoAdmissionPort, ConnectorMetadataMaintenanceIoProfile, SharedDependency,
};
use novarocks_state_store_api::StateStoreExecutionRequests;
use novarocks_workload_control::WorkError;
use std::task::{Context, Poll};

type PreparationFuture = Pin<
    Box<
        dyn Future<
                Output = Result<
                    Result<PreparedExplain, QueryServiceError>,
                    StatementNativeHealthFailure,
                >,
            > + Send,
    >,
>;
type DiagnosticFuture =
    Pin<Box<dyn Future<Output = Result<QueryResult, QueryServiceError>> + Send>>;

use super::{
    compiler::ExplainInput,
    query_session_action::{QueryActionEnvironment, QueryActionInputs},
};

/// A read-only projection of the DELETE path already proved in the original
/// statement. Row-level input is the ordinary rendered matching read.
pub(super) enum DeleteExplainPlan<'a> {
    Empty,
    Metadata {
        target: &'a crate::catalog_application::resolver::TargetBackend,
        snapshot_id: Option<i64>,
        constrained_columns: &'a [String],
    },
    RowLevel {
        target: &'a crate::catalog_application::resolver::TargetBackend,
        snapshot_id: Option<i64>,
        strategy: novarocks_spi::connector::ConnectorRowMutationStrategy,
        matching_read: Vec<String>,
    },
}

pub(super) fn render_delete_plan(
    plan: DeleteExplainPlan<'_>,
) -> Result<Vec<String>, QueryServiceError> {
    use novarocks_spi::connector::ConnectorRowMutationStrategy;

    let layout = "    Layout: [rows:bigint]".to_owned();
    match plan {
        DeleteExplainPlan::Empty => Ok(vec![
            "Values (0 rows)".to_owned(),
            layout,
            "    (bigint '0')".to_owned(),
        ]),
        DeleteExplainPlan::Metadata {
            target,
            snapshot_id,
            constrained_columns,
        } => {
            if snapshot_id.is_none() && !constrained_columns.is_empty() {
                return Err(internal_error(
                    "Filtered TableDelete has no original snapshot",
                ));
            }
            let target = delete_target_label(target, snapshot_id);
            let condition = if constrained_columns.is_empty() {
                String::new()
            } else {
                let columns = constrained_columns
                    .iter()
                    .map(|column| delete_explain_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(" constraint on [{columns}]")
            };
            Ok(vec![
                format!("TableDelete[target = {target}{condition}]"),
                layout,
            ])
        }
        DeleteExplainPlan::RowLevel {
            target,
            snapshot_id,
            strategy,
            matching_read,
        } => {
            let format = match strategy {
                ConnectorRowMutationStrategy::DeletionVector => "DeletionVector",
                ConnectorRowMutationStrategy::PositionDelete => "PositionDelete",
                other => {
                    return Err(internal_error(format!(
                        "Cannot explain DELETE strategy {other:?}"
                    )));
                }
            };
            if matching_read.is_empty() {
                return Err(internal_error("Row-level DELETE has no matching read plan"));
            }
            let target = delete_target_label(target, snapshot_id);
            let mut lines = Vec::with_capacity(matching_read.len() + 2);
            lines.push(format!(
                "RowLevelDelete[target = {target}, format = {format}]"
            ));
            lines.push(layout);
            lines.extend(matching_read);
            Ok(lines)
        }
    }
}

fn delete_target_label(
    target: &crate::catalog_application::resolver::TargetBackend,
    snapshot_id: Option<i64>,
) -> String {
    use std::fmt::Write;
    let mut label = format!(
        "{}:{}.{}",
        delete_explain_identifier(&target.catalog),
        delete_explain_identifier(&target.namespace),
        delete_explain_identifier(&target.table),
    );
    if let Some(snapshot_id) = snapshot_id {
        write!(&mut label, "@{snapshot_id}").expect("write to a String");
    }
    label
}

fn delete_explain_identifier(value: &str) -> String {
    let name =
        novarocks_parser::build::object_name(vec![novarocks_parser::build::ident_from_catalog(
            value,
        )])
        .expect("one identifier forms a name");
    novarocks_parser::printer::print_object_name(&name)
}

pub(super) fn explain_lines_result(lines: Vec<String>) -> Result<QueryResult, QueryServiceError> {
    build_string_query_result("Explain String", lines).map_err(internal_error)
}

#[cfg(test)]
mod delete_explain_tests {
    use super::*;
    use crate::catalog_application::resolver::TargetBackend;
    use novarocks_spi::connector::{ConnectorProviderId, ConnectorRowMutationStrategy};

    fn target() -> TargetBackend {
        TargetBackend {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            catalog: "lake".into(),
            namespace: "schema.with.dot".into(),
            table: "source_table".into(),
        }
    }

    #[test]
    fn explain_delete_displays_the_original_metadata_target_and_layout() {
        let target = target();
        let lines = render_delete_plan(DeleteExplainPlan::Metadata {
            target: &target,
            snapshot_id: Some(57),
            constrained_columns: &["p".into(), "Part Id".into()],
        })
        .unwrap();
        assert_eq!(
            lines,
            [
                r#"TableDelete[target = lake:"schema.with.dot".source_table@57 constraint on [p, "Part Id"]]"#,
                "    Layout: [rows:bigint]",
            ]
        );
        let all_rows = render_delete_plan(DeleteExplainPlan::Metadata {
            target: &target,
            snapshot_id: None,
            constrained_columns: &[],
        })
        .unwrap();
        assert_eq!(
            all_rows[0],
            r#"TableDelete[target = lake:"schema.with.dot".source_table]"#
        );
        let result = explain_lines_result(lines).unwrap();
        assert_eq!(result.row_count(), 2);
        assert_eq!(result.columns[0].name(), "Explain String");
    }

    #[test]
    fn explain_delete_zero_path_displays_the_completed_zero_value() {
        assert_eq!(
            render_delete_plan(DeleteExplainPlan::Empty).unwrap(),
            [
                "Values (0 rows)",
                "    Layout: [rows:bigint]",
                "    (bigint '0')",
            ]
        );
    }

    #[test]
    fn explain_delete_preserves_the_read_plan_and_provider_format() {
        let target = target();
        let read = vec!["SCAN t".to_owned(), "  Filter: data > 5".to_owned()];
        for (strategy, label) in [
            (
                ConnectorRowMutationStrategy::DeletionVector,
                "DeletionVector",
            ),
            (
                ConnectorRowMutationStrategy::PositionDelete,
                "PositionDelete",
            ),
        ] {
            let lines = render_delete_plan(DeleteExplainPlan::RowLevel {
                target: &target,
                snapshot_id: Some(57),
                strategy,
                matching_read: read.clone(),
            })
            .unwrap();
            assert_eq!(
                lines[0],
                format!(
                    r#"RowLevelDelete[target = lake:"schema.with.dot".source_table@57, format = {label}]"#
                )
            );
            assert_eq!(lines[2..], read);
        }
    }
}

struct ExplainPreparationInput {
    compiler: FrontendQueryCompiler,
    topology: BackendTopologyService,
    role: ClusterRole,
    optimizer_limit: u64,
    sql: String,
    explain: ExplainInput,
    parameters: Option<Arc<novarocks_sql::compiler::SqlPreparedParameters>>,
    state: SessionSqlState,
    timing: Option<novarocks_query_application::session_control::ClientQueryTiming>,
    statement_time: novarocks_type_contract::StatementTimeEnvironment,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    scope: novarocks_workload_control::WorkScope,
    window: ResultWindowAlias,
    provider_source: Arc<crate::query_execution::completion_facts::FrontendCompletionCustody>,
    planning_cpu: Arc<FrontendPlanningCpuSource>,
    io: Arc<ReadMetadataIoSource>,
    state_store: Option<Arc<dyn StateStoreExecutionRequests>>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}
struct PreparedExplain {
    value: PreparedAsyncExplain,
}
async fn make_explain_preparation(
    input: ExplainPreparationInput,
) -> Result<Result<PreparedExplain, QueryServiceError>, StatementNativeHealthFailure> {
    let prepared: Result<_, QueryServiceError> = async {
        if input.cancellation.is_cancelled() {
            return Err(internal_error(
                "typed statement was cancelled before query compilation began",
            ));
        }
        let topology = input
            .topology
            .snapshot()
            .map_err(|e| internal_error(e.to_string()))?;
        let topology = if topology.targets().is_empty() {
            wait_for_eligible_query_topology_after(
                &input.topology,
                &input.cancellation,
                topology.revision(),
                input
                    .deadline
                    .unwrap_or_else(|| Instant::now() + Duration::from_secs(30)),
            )
            .await?
        } else {
            topology
        };
        let presentation = input.state.presentation().copied();
        let statement_time = input.state.statement_time(input.statement_time);
        let (catalog, database, execution, mut optimizer, semantics, property_inputs) =
            input.state.into_query_attempt_inputs();
        if optimizer.optimizer_query_mem_limit_bytes.is_none() {
            optimizer.optimizer_query_mem_limit_bytes = Some(input.optimizer_limit as f64);
        }
        let options = query_options_from_session_settings(&execution);
        let options = if !input.explain.is_analyze() {
            // The outer EXPLAIN deadline already belongs to RequestContext.
            // A missing outer deadline uses the original connector bound.
            let mut raw = *options.as_proto();
            raw.query_timeout = 0;
            QueryOptions::from_proto(raw)
        } else {
            options
        };
        let capacity = QueryResultCapacityBinding::try_new(&input.scope, input.window)
            .map_err(|e| internal_error(e.to_string()))?;
        let context = RequestContext::admit(
            RequestAdmission::new(
                catalog,
                database,
                input.role,
                topology,
                input.deadline,
                input.cancellation,
                optimizer,
                semantics.with_statement_time(statement_time),
                presentation,
            )
            .with_session_property_inputs(property_inputs),
        )
        .with_prepared_parameters(input.parameters)
        .with_result_capacity(capacity.clone())
        .map_err(|e| internal_error(e.to_string()))?;
        #[cfg(novarocks_dev_controls)]
        let context = match input
            .observation
            .as_ref()
            .and_then(|handle| handle.nomination())
        {
            Some(nomination) => context
                .with_test_statement_nomination(Arc::new(nomination))
                .map_err(internal_error)?,
            None => context,
        };
        Ok((context, options, execution.explain_detail()))
    }
    .await;
    let (context, options, detail) = match prepared {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let value = input
        .compiler
        .prepare_explain_async(
            input.explain,
            detail,
            &context,
            Some(options),
            &input.scope,
            ExplainPreparationPorts {
                provider: &input.provider_source,
                cpu: &input.planning_cpu,
                io: input.io,
                state_store: input.state_store,
                timing: input.timing,
                query_local_overlays: Vec::new(),
                original_context: None,
            },
        )
        .await?;
    Ok(value
        .map(|value| PreparedExplain { value })
        .map_err(|e| routed_execution_service_error(routed_compiler_failure(e, &input.sql))))
}
struct ProfileDiagnosticInput {
    ready: crate::query_execution::internal_consumer::profile::ReadyProfileComputation,
    completion: crate::query_execution::completion::PreparedQueryCompletion,
}
async fn make_profile_diagnostics(
    mut input: ProfileDiagnosticInput,
) -> Result<QueryResult, QueryServiceError> {
    let snapshot = input.ready.wait_diagnostics().await;
    input
        .completion
        .complete_profile_observation(&input.ready, snapshot.as_deref().map_err(|e| *e))
        .map_err(internal_error)
}

struct ExplainStatementAction {
    family: novarocks_result_contract::ClientPresentationFamily,
    // Outside the polled preparation F: async unwind may destroy its fields.
    provider_source: Arc<crate::query_execution::completion_facts::FrontendCompletionCustody>,
    planning_cpu: Arc<FrontendPlanningCpuSource>,
    prefix: Option<Arc<FrontendCompletionCustody>>,
    preparation_health: Option<StatementNativeHealthFailure>,
    provider_close: Option<ProviderClose>,
    provider_close_result: Option<Result<(), StatementNativeHealthFailure>>,
    tail: Option<Box<ReadNativeTail>>,
    bootstrap_ready: bool,
    result_transferred: bool,
    error: Option<QueryServiceError>,
    preparation: Option<PreparationFuture>,
    preparation_started: bool,
    prepared: Option<Result<PreparedExplain, QueryServiceError>>,
    profile: Option<Box<PreparedAsyncProfile>>,
    capacity: Option<QueryResultCapacityBinding>,
    original: OriginalComputationStart,
    execution: Option<Result<ExecutionOutputObligation, QueryExecutionError>>,
    diagnostics: Option<DiagnosticFuture>,
    raw_result: Option<Result<QueryResult, QueryServiceError>>,
    local: Option<OwnedLocalResult>,
    launcher: Arc<dyn LogicalExecutionLauncher>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}
impl GovernedStatementAction for ExplainStatementAction {
    fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.preparation_health
            .clone()
            .or_else(|| self.planning_cpu.local_health_failure())
            .or_else(|| self.provider_source.local_health_failure())
            .or_else(|| {
                self.prefix
                    .as_ref()
                    .and_then(|prefix| prefix.local_health_failure())
            })
            .or_else(|| {
                self.provider_close_result
                    .as_ref()
                    .and_then(|result| result.as_ref().err().cloned())
            })
            .or_else(|| {
                self.tail.as_ref().and_then(|tail| {
                    tail.health
                        .clone()
                        .or_else(|| tail.services.io().health_failure())
                        .or_else(|| tail.native.stage.local_health_failure())
                })
            })
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if self.result_transferred && self.tail.is_some() {
            if target.is_some() {
                return Err(StatementNativeHealthFailure::actual_exit_unproven(
                    "EXPLAIN residual receiver is occupied",
                    Arc::new(WorkError::Conflict),
                ));
            }
            *target = self
                .tail
                .take()
                .map(|tail| tail as Box<dyn GovernedStatementResidual>);
        }
        Ok(())
    }
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if let Some(tail) = self.tail.as_mut() {
            tail.services.poll_state_store(cx, &mut tail.health);
        }
        let polled = self.poll_explain(owner, cx);
        if let Some(tail) = self.tail.as_mut() {
            tail.services.poll_state_store(cx, &mut tail.health);
        }
        let result = match polled {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        let cancellation =
            QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
        let Some(reason) = cancellation.reason() else {
            return Poll::Ready(result);
        };
        // Preserve an already selected execution fact; otherwise retain the
        // original public cancellation policy and its compile-control sidecar.
        Poll::Ready(match result {
            Err(error) if error.execution_failure().is_some() => Err(error),
            Err(error) => {
                Err(cancellation_error(reason).with_compile_control(error.compile_control_error()))
            }
            Ok(value) => {
                drop(value);
                Err(cancellation_error(reason))
            }
        })
    }
}
impl ExplainStatementAction {
    fn poll_explain(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if !self.preparation_started && owner.cancellation().reason().is_some() {
            self.error.get_or_insert_with(|| {
                internal_error(
                    "typed statement was cancelled before prepared query execution began",
                )
            });
        }
        if let Some(tail) = self.tail.as_mut() {
            let work = owner
                .execution_owner_slot()
                .as_ref()
                .expect("EXPLAIN retains its original Root");
            match tail.services.io().poll_progress(work, cx) {
                Ok(ready) => self.bootstrap_ready |= ready,
                Err(error) => {
                    self.error.get_or_insert_with(|| {
                        QueryServiceError::from_failure_cause(
                            novarocks_failure_contract::SharedFailureCause::new(error),
                        )
                    });
                }
            }
        }
        if !self.bootstrap_ready && self.error.is_none() {
            return Poll::Pending;
        }
        // Once polled, preparation remains in this Source through its actual
        // return and completed-F Drop, including cancellation and business Err.
        if self.error.is_some() && !self.preparation_started {
            drop(self.preparation.take());
        }
        if let Some(future) = self.preparation.as_mut() {
            self.preparation_started = true;
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(value)) => self.prepared = Some(value),
                Poll::Ready(Err(health)) => {
                    self.preparation_health = Some(health);
                    return Poll::Pending;
                }
            }
            if self.provider_source.local_health_failure().is_some() {
                return Poll::Pending;
            }
            drop(self.preparation.take());
        }
        match poll_provider_close(
            &mut self.provider_close,
            &mut self.provider_close_result,
            cx,
        ) {
            Poll::Pending | Poll::Ready(Err(_)) => return Poll::Pending,
            Poll::Ready(Ok(())) => {}
        }
        if let Some(prepared) = self.prepared.take() {
            match prepared {
                Err(error) => {
                    self.error.get_or_insert(error);
                }
                Ok(PreparedExplain {
                    value: PreparedAsyncExplain::Lines(lines),
                }) => {
                    self.raw_result = Some(explain_lines_result(lines));
                }
                Ok(PreparedExplain {
                    value: PreparedAsyncExplain::Profile(profile),
                }) => {
                    self.profile = Some(profile);
                }
            }
        }
        if self.error.is_none()
            && let Some(error) = self
                .tail
                .as_ref()
                .and_then(|tail| tail.services.io().original())
                .and_then(|io| io.failure())
        {
            self.error = Some(QueryServiceError::from_failure_cause(
                novarocks_failure_contract::SharedFailureCause::new(error),
            ));
        }
        if self.error.is_none()
            && self
                .profile
                .as_ref()
                .is_some_and(|profile| profile.computation.is_some())
        {
            if owner.cancellation().reason().is_some() {
                self.error = Some(internal_error(
                    "typed statement was cancelled before prepared query execution began",
                ));
            } else {
                let tail = self
                    .tail
                    .as_mut()
                    .expect("Profile has its original metadata and stage Source");
                if !tail.services.state_store_quiescent(cx, &mut tail.health) {
                    return Poll::Pending;
                }
                let io = tail
                    .services
                    .io()
                    .original()
                    .expect("EXPLAIN preparation installed original metadata source");
                let work = owner
                    .execution_owner_slot()
                    .as_ref()
                    .expect("same EXPLAIN Root");
                match tail.native.stage.poll_admit(&io, work, cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => self.error = Some(error),
                    Poll::Ready(Ok(())) => {}
                }
                if self.error.is_none()
                    && self.capacity.is_none()
                    && let Err(error) = tail
                        .native
                        .stage
                        .install_internal_read_capacity_in(&io, &mut self.capacity)
                {
                    self.error = Some(error);
                }
                if self.error.is_none() {
                    match self.launcher.start_computation_in(
                        &mut self.profile.as_mut().expect("original Profile").computation,
                        tail.native.stage.owner_slot(),
                        self.capacity
                            .as_ref()
                            .expect("original positive Internal stage capacity"),
                        &io,
                        &mut self.original,
                        #[cfg(novarocks_dev_controls)]
                        self.observation.clone(),
                    ) {
                        Err(error) => self.error = Some(governed_query_execution_error(error)),
                        Ok(notification) => {
                            tail.native.observe_start_in(
                                &notification,
                                &mut self.original.exit,
                                &mut tail.health,
                            );
                            if let Err(error) = notification.finish() {
                                self.error = Some(governed_query_execution_error(error));
                            }
                        }
                    }
                }
            }
        }
        if let Some(future) = self.original.ready.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(value) => self.execution = Some(value),
            }
            drop(self.original.ready.take());
        }
        if let Some(execution) = self.execution.take() {
            match execution {
                Err(error) => {
                    self.error
                        .get_or_insert_with(|| governed_query_execution_error(error));
                }
                Ok(execution) => {
                    let ready = match execution
                        .ready_token()
                        .copied()
                        .ok_or_else(|| internal_error("Profile has no computation Ready token"))
                        .and_then(|token| {
                            self.profile
                                .as_mut()
                                .expect("started Profile")
                                .output
                                .take_ready(token)
                                .map_err(governed_query_execution_error)
                        }) {
                        Ok(ready) => Some(ready),
                        Err(error) => {
                            self.error.get_or_insert(error);
                            None
                        }
                    };
                    if let Some(ready) = ready {
                        let profile = *self.profile.take().expect("same ready Profile");
                        self.diagnostics =
                            Some(Box::pin(make_profile_diagnostics(ProfileDiagnosticInput {
                                ready,
                                completion: profile.completion,
                            })));
                    }
                    drop(execution);
                }
            }
        }
        if let Some(future) = self.diagnostics.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(value) => self.raw_result = Some(value),
            }
            drop(self.diagnostics.take());
        }
        if self.error.is_none()
            && self.local.is_none()
            && let Some(result) = self.raw_result.take()
        {
            match result {
                Err(error) => self.error = Some(error),
                Ok(result) => {
                    let local = owner
                        .result_window_alias()
                        .ok_or_else(|| internal_error("EXPLAIN has no original result window"))
                        .and_then(|window| {
                            LocalResultProducer::try_new(owner.scope(), window, self.family)
                                .map_err(internal_error)
                        })
                        .and_then(|producer| {
                            producer.produce(|| Ok(result)).map_err(internal_error)
                        });
                    match local {
                        Ok(local) => self.local = Some(local),
                        Err(error) => self.error = Some(error),
                    }
                }
            }
        }
        if self.error.is_some() || self.local.is_some() {
            if let Some(tail) = self.tail.as_mut() {
                if !tail.native.is_launched() {
                    drop(self.original.input.take());
                    drop(self.original.request.take());
                    drop(self.profile.take());
                }
                drop(self.capacity.take());
                match tail.poll_business_exit(cx) {
                    Poll::Pending | Poll::Ready(Err(_)) => return Poll::Pending,
                    Poll::Ready(Ok(())) => {}
                }
                if let Some(error) = tail.services.io().original().and_then(|io| io.failure()) {
                    self.error.get_or_insert_with(|| {
                        QueryServiceError::from_failure_cause(
                            novarocks_failure_contract::SharedFailureCause::new(error),
                        )
                    });
                }
            }
            self.result_transferred = true;
            if let Some(error) = self.error.take() {
                return Poll::Ready(Err(error));
            }
            return Poll::Ready(Ok(StatementActionValue::Local(
                self.local.take().expect("sealed original EXPLAIN graph"),
            )));
        }
        Poll::Pending
    }
}

pub(super) fn build_explain_action(
    environment: &QueryActionEnvironment,
    inputs: QueryActionInputs,
    sql: String,
    explain: ExplainInput,
    state: SessionSqlState,
    family: novarocks_result_contract::ClientPresentationFamily,
) -> Result<Box<dyn GovernedStatementAction>, QueryServiceError> {
    let QueryActionInputs {
        scope,
        deadline,
        cancellation,
        window,
        parameters,
        planning_cpu,
        timing,
        statement_time,
        #[cfg(novarocks_dev_controls)]
        observation,
        #[cfg(novarocks_dev_controls)]
            statement_token: _,
        principal: _,
    } = inputs;
    let prepared_scope = scope.clone();
    let prepared_cancellation = cancellation.clone();
    #[cfg(novarocks_dev_controls)]
    let observation_for_preparation = observation.clone();
    build_scoped_explain(
        environment,
        ExplainActionAdmission {
            scope,
            deadline,
            cancellation,
            family,
            planning_cpu,
            prefix: None,
            preparation_bytes: std::mem::size_of::<ExplainPreparationInput>(),
            #[cfg(novarocks_dev_controls)]
            observation,
        },
        move |services| {
            let scope = prepared_scope;
            let cancellation = prepared_cancellation;
            let input = ExplainPreparationInput {
                compiler: environment.compiler.clone(),
                topology: environment.topology.clone(),
                role: environment.role,
                optimizer_limit: environment.optimizer_limit,
                sql,
                explain,
                parameters,
                state,
                timing,
                statement_time,
                deadline,
                cancellation,
                scope,
                window,
                provider_source: services.provider.clone(),
                planning_cpu: services.cpu.clone(),
                io: services.io.clone(),
                state_store: services.state_store,
                #[cfg(novarocks_dev_controls)]
                observation: observation_for_preparation,
            };
            Box::pin(make_explain_preparation(input))
        },
    )
}

struct DeleteExplainPreparation {
    kernel: DmlExecutionKernel,
    target: crate::catalog_application::resolver::TargetBackend,
    planning: novarocks_spi::connector::ConnectorControlPlanningPin,
    input: DeleteExplainStatementInput,
    prefix: Arc<FrontendCompletionCustody>,
    sources: ExplainActionSources,
}

pub(crate) fn prepare_delete_explain(
    kernel: DmlExecutionKernel,
    input: DeleteExplainStatementInput,
) -> Result<Box<dyn GovernedStatementAction>, QueryServiceError> {
    let (target, _) = delete::resolve_target(
        &kernel,
        &input.statement,
        input.context.session().current_catalog(),
        input.context.session().current_database(),
    )
    .map_err(|error| error.into_service())?;
    let planning = crate::connector::acquire_metadata_planning_pin(
        kernel.connector_control().as_ref(),
        &target.catalog,
    )
    .map_err(|error| {
        let error = crate::catalog_application::resolver::TargetResolutionError::from(error);
        error.into_service()
    })?;
    let prefix = FrontendCompletionCustody::new();
    let admission = ExplainActionAdmission {
        scope: input.command.scope().clone(),
        deadline: input.context.execution().deadline(),
        cancellation: input.context.execution().cancellation().clone(),
        family: input.family,
        planning_cpu: None,
        prefix: Some(prefix.clone()),
        preparation_bytes: std::mem::size_of::<DeleteExplainPreparation>(),
        #[cfg(novarocks_dev_controls)]
        observation: input.observation.clone(),
    };
    let environment = input.environment.clone();
    build_scoped_explain(&environment, admission, move |sources| {
        Box::pin(make_delete_explain_preparation(DeleteExplainPreparation {
            kernel,
            target,
            planning,
            input,
            prefix,
            sources,
        }))
    })
}

async fn make_delete_explain_preparation(
    preparation: DeleteExplainPreparation,
) -> Result<Result<PreparedExplain, QueryServiceError>, StatementNativeHealthFailure> {
    let DeleteExplainPreparation {
        kernel,
        target,
        planning,
        input,
        prefix,
        sources,
    } = preparation;
    let statement = match input.statement {
        OwnedDeleteStatement::Predicate(statement) => statement,
        #[cfg(novarocks_dev_controls)]
        OwnedDeleteStatement::TrinoEquality { .. } => {
            return Ok(Err(internal_error("EXPLAIN expected a SQL DELETE")));
        }
    };
    let reference = if statement.target.branch_is_explicit() {
        if statement.selection.is_none() {
            return Ok(Err(QueryServiceError::new(
                QueryServiceErrorKind::Unsupported,
                "EXPLAIN does not support a whole-branch DELETE",
            )));
        }
        match statement.target.table_and_reference() {
            Ok((_, reference)) => Some(reference.to_owned()),
            Err(error) => return Ok(Err(internal_error(error))),
        }
    } else {
        None
    };
    let connector = input
        .command
        .connector_context()
        .clone()
        .with_io_admission(sources.io.clone())
        .with_original_table_preparation_reuse();
    let proved = delete::select_delete_path(
        &kernel,
        &target,
        planning,
        &statement,
        &input.context,
        &connector,
        &prefix,
    )
    .await;
    prefix.close().await?;
    let (original, decision) = match proved {
        Ok(proved) => proved,
        Err(error) => {
            return Ok(Err(routed_execution_service_error(
                dml_result::<()>(Err(error.into_dml_error(Some(&input.sql)))).unwrap_err(),
            )));
        }
    };
    let rendered = match decision.path {
        DeletePath::Empty => render_delete_plan(DeleteExplainPlan::Empty),
        DeletePath::Metadata { .. } => render_delete_plan(DeleteExplainPlan::Metadata {
            target: &target,
            snapshot_id: decision.snapshot_id,
            constrained_columns: &decision.constrained_columns,
        }),
        DeletePath::RowLevel => {
            let (snapshot_id, strategy) = if let Some(reference) = &reference {
                match crate::query_execution::provider_read_facts::observe_delete_target_on_ref(
                    kernel.typed_connector_control(),
                    &original,
                    &connector,
                    Some(reference),
                ) {
                    Ok(Some(facts)) => (facts.snapshot_id, facts.row_delete_strategy),
                    Ok(None) => {
                        return Ok(Err(internal_error(
                            "DELETE branch has no original read facts",
                        )));
                    }
                    Err(error) => {
                        let projected = match error.into_original_cause() {
                            Ok(cause) => QueryServiceError::from_failure_cause(cause),
                            Err(error) => internal_error(error.to_string()),
                        };
                        return Ok(Err(projected));
                    }
                }
            } else {
                (decision.snapshot_id, decision.row_delete_strategy)
            };
            let Some(strategy) = strategy else {
                return Ok(Err(QueryServiceError::new(
                    QueryServiceErrorKind::Unsupported,
                    "Iceberg table format does not support row-level DELETE",
                )));
            };
            let prepared = (|| {
                let query = delete::absorption::predicate_query(&target, statement.selection)
                    .map_err(internal_error)?;
                let overlay = delete::absorption::original_target_explain_overlay(
                    &target, &original, reference,
                )
                .map_err(internal_error)?;
                let planning_context =
                    novarocks_spi::connector::ConnectorPlanningContext::try_from_request(connector)
                        .map_err(|error| internal_error(error.to_string()))?;
                Ok::<_, QueryServiceError>((query, overlay, planning_context))
            })();
            let (query, overlay, planning_context) = match prepared {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
            let mut context = input.context;
            if context.execution().topology().targets().is_empty() {
                let execution = context.execution();
                let topology = wait_for_initial_query_topology(
                    &input.environment.topology,
                    execution.cancellation(),
                    execution.deadline(),
                )
                .await;
                let execution = match topology.and_then(|topology| {
                    execution
                        .clone()
                        .with_first_compute_topology(topology)
                        .map_err(|error| internal_error(error.to_string()))
                }) {
                    Ok(execution) => execution,
                    Err(error) => return Ok(Err(error)),
                };
                context = RequestContext::new(context.session().clone(), execution);
            }
            let mut options = *input.query_options.as_proto();
            options.query_timeout = 0;
            let matching = input
                .environment
                .compiler
                .prepare_explain_async(
                    ExplainInput::plan(Box::new(query)),
                    input.detail,
                    &context,
                    Some(QueryOptions::from_proto(options)),
                    input.command.scope(),
                    ExplainPreparationPorts {
                        provider: &sources.provider,
                        cpu: &sources.cpu,
                        io: sources.io,
                        state_store: sources.state_store,
                        timing: input.timing,
                        query_local_overlays: vec![overlay],
                        original_context: Some(planning_context),
                    },
                )
                .await?;
            let matching_read = match matching {
                Ok(PreparedAsyncExplain::Lines(lines)) => lines,
                Ok(PreparedAsyncExplain::Profile(_)) => {
                    return Ok(Err(internal_error(
                        "EXPLAIN DELETE acquired a Profile computation",
                    )));
                }
                Err(error) => {
                    return Ok(Err(routed_execution_service_error(
                        routed_compiler_failure(error, &input.sql),
                    )));
                }
            };
            render_delete_plan(DeleteExplainPlan::RowLevel {
                target: &target,
                snapshot_id,
                strategy,
                matching_read,
            })
        }
    };
    Ok(rendered.map(|lines| PreparedExplain {
        value: PreparedAsyncExplain::Lines(lines),
    }))
}

struct ExplainActionAdmission {
    scope: novarocks_workload_control::WorkScope,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    family: novarocks_result_contract::ClientPresentationFamily,
    planning_cpu: Option<Arc<FrontendPlanningCpuSource>>,
    prefix: Option<Arc<FrontendCompletionCustody>>,
    preparation_bytes: usize,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}

struct ExplainActionSources {
    provider: Arc<FrontendCompletionCustody>,
    cpu: Arc<FrontendPlanningCpuSource>,
    io: Arc<ReadMetadataIoSource>,
    state_store: Option<Arc<dyn StateStoreExecutionRequests>>,
}

fn build_scoped_explain(
    environment: &QueryActionEnvironment,
    admission: ExplainActionAdmission,
    make_preparation: impl FnOnce(ExplainActionSources) -> PreparationFuture,
) -> Result<Box<dyn GovernedStatementAction>, QueryServiceError> {
    let ExplainActionAdmission {
        scope,
        deadline,
        cancellation,
        family,
        planning_cpu,
        prefix,
        preparation_bytes,
        #[cfg(novarocks_dev_controls)]
        observation,
    } = admission;
    environment
        .compiler
        .validate_original_read_runtime()
        .map_err(|error| internal_error(error.to_string()))?;
    let state_store = environment
        .state_store
        .as_ref()
        .map(|binding| {
            let owner = binding.for_root(scope.clone())?;
            let requests: Arc<dyn StateStoreExecutionRequests> =
                Arc::new(owner.execution_requests(binding.transaction_deadline, deadline)?);
            Ok::<_, novarocks_state_store_api::StateStoreExecutionFailure>((owner, requests))
        })
        .transpose()
        .map_err(|cause| {
            QueryServiceError::from_failure_cause(cause)
                .reconcile_local_stop_observation(&cancellation)
        })?;
    let dependencies = environment
        .state_store
        .as_ref()
        .map_or(&[][..], |binding| binding.blocking_dependency_facts());
    let profile = explain_preparation_profile(dependencies, preparation_bytes)
        .map_err(|error| internal_error(error.to_string()))?;
    let io = ReadMetadataIoSource::new(scope.clone(), profile.clone());
    io.configure_read_source(&profile)
        .map_err(|error| internal_error(error.to_string()))?;
    let (state_store_owner, state_store_requests) = match state_store {
        Some((owner, requests)) => (Some(owner), Some(requests)),
        None => (None, None),
    };
    let stage = StatementStageAdmission::new(scope.clone(), 0);
    let provider_source =
        crate::query_execution::completion_facts::FrontendCompletionCustody::new();
    let planning_cpu =
        planning_cpu.unwrap_or_else(|| environment.compiler.new_planning_cpu_source());
    let close_prefix = prefix.clone();
    let close_provider_source = provider_source.clone();
    let close_cpu = planning_cpu.clone();
    let provider_close = Some(Box::pin(async move {
        let prefix_closed = match close_prefix {
            Some(prefix) => prefix.close().await,
            None => Ok(()),
        };
        let provider_closed = close_provider(close_provider_source, close_cpu).await;
        prefix_closed.and(provider_closed)
    }) as ProviderClose);
    let preparation = make_preparation(ExplainActionSources {
        provider: provider_source.clone(),
        cpu: planning_cpu.clone(),
        io: io.clone(),
        state_store: state_store_requests,
    });
    Ok(Box::new(ExplainStatementAction {
        family,
        provider_source,
        prefix,
        planning_cpu,
        preparation_health: None,
        provider_close,
        provider_close_result: None,
        tail: Some(Box::new(ReadNativeTail {
            services: ReadNativeServices::new(state_store_owner, io),
            native: ReadNativeStageTail::new(stage),
            health: None,
        })),
        bootstrap_ready: false,
        result_transferred: false,
        error: None,
        preparation: Some(preparation),
        preparation_started: false,
        prepared: None,
        profile: None,
        capacity: None,
        original: OriginalComputationStart::default(),
        execution: None,
        diagnostics: None,
        raw_result: None,
        local: None,
        launcher: Arc::clone(&environment.launcher),
        #[cfg(novarocks_dev_controls)]
        observation,
    }))
}

fn explain_preparation_profile(
    dependencies: &[SharedDependency],
    preparation_bytes: usize,
) -> Result<ConnectorMetadataMaintenanceIoProfile, WorkError> {
    let mut dependencies = dependencies.to_vec();
    if !dependencies.contains(&SharedDependency::LocalHost) {
        dependencies.push(SharedDependency::LocalHost);
    }
    ConnectorMetadataMaintenanceIoProfile::try_new(
        std::num::NonZeroUsize::new(1).unwrap(),
        std::num::NonZeroU64::new(crate::query_execution::maintenance::io_admission::MetadataIoAdmission::request_metadata_bytes())
            .ok_or(WorkError::ArithmeticOverflow)?,
        std::num::NonZeroU64::new(preparation_bytes as u64)
            .ok_or(WorkError::ArithmeticOverflow)?,
        dependencies,
    ).map_err(|_| WorkError::InvalidConfig("local EXPLAIN preparation source declaration"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::session_control::GovernedQueryStatementOwner;
    use novarocks_query_application::{
        api::ReservedExecutionIdentity, query_control::QueryApplicationControl,
        session_control::StatementActionReady,
    };
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, ResultCapacityHandle, WorkloadConfig, WorkloadControl,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct NoNativeLaunch;
    impl LogicalExecutionLauncher for NoNativeLaunch {
        #[cfg(novarocks_dev_controls)]
        fn resolve_statement_observation(
            &self,
            _: &novarocks_failpoint::target_binding::StatementWorkNomination,
        ) -> Option<crate::preparation_diagnostics::StatementObservationHandle> {
            panic!("this fixture cannot resolve a Native statement observation")
        }

        fn reserve_identity(&self) -> Result<ReservedExecutionIdentity, QueryExecutionError> {
            panic!("Local EXPLAIN cannot reserve Native work")
        }
        fn start_read_in(
            &self,
            _: &mut Option<crate::query_execution::completion::PreparedLogicalRead>,
            _: &mut Option<novarocks_workload_control::WorkOwner>,
            _: &QueryResultCapacityBinding,
            _: &Arc<crate::query::read_action::io_source::ReadMetadataIoSource>,
            _: &mut OriginalComputationStart,
            #[cfg(novarocks_dev_controls)] _: Option<
                crate::preparation_diagnostics::StatementObservationHandle,
            >,
        ) -> Result<
            novarocks_query_application::api::LogicalExecutionStartAttempt,
            QueryExecutionError,
        > {
            panic!("EXPLAIN fixture cannot start ordinary Read")
        }
        fn start_computation_in(
            &self,
            _: &mut Option<crate::query_execution::logical_execution::PreparedLogicalComputation>,
            _: &mut Option<WorkOwner>,
            _: &QueryResultCapacityBinding,
            _: &Arc<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>,
            _: &mut OriginalComputationStart,
            #[cfg(novarocks_dev_controls)] _: Option<
                crate::preparation_diagnostics::StatementObservationHandle,
            >,
        ) -> Result<
            novarocks_query_application::api::LogicalExecutionStartAttempt,
            QueryExecutionError,
        > {
            panic!("Local EXPLAIN cannot start Native work")
        }
    }
    async fn statement() -> (
        WorkloadControl,
        ResultCapacityHandle,
        GovernedQueryStatementOwner,
        novarocks_query_application::session_control::QuerySessionLease,
    ) {
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control);
        let session = service
            .register_session(SessionIdentity::client_statement(
                service.mint_client_query_id(),
                "root",
            ))
            .unwrap();
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        workload.mark_ready().unwrap();
        let owner = service
            .begin_queued_governed_query_statement_with_result(
                session.token(),
                &workload.root_admission(),
                None,
                None,
                None,
                ResultWindowClass::Local,
            )
            .await
            .unwrap();
        (workload, capacity, owner, session)
    }
    fn action(preparation: PreparationFuture) -> ExplainStatementAction {
        ExplainStatementAction {
            family: novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    parametric_interval: false,
                    variant: true,
                    variant_binary: true,
                },
            ),
            provider_source:
                crate::query_execution::completion_facts::FrontendCompletionCustody::new(),
            planning_cpu: FrontendPlanningCpuSource::new(
                crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor::new(
                    tokio::runtime::Handle::current(),
                ),
            ),
            preparation_health: None,
            prefix: None,
            provider_close: None,
            provider_close_result: Some(Ok(())),
            tail: None,
            bootstrap_ready: true,
            result_transferred: false,
            error: None,
            preparation: Some(preparation),
            preparation_started: false,
            prepared: None,
            profile: None,
            capacity: None,
            original: OriginalComputationStart::default(),
            execution: None,
            diagnostics: None,
            raw_result: None,
            local: None,
            launcher: Arc::new(NoNativeLaunch),
            #[cfg(novarocks_dev_controls)]
            observation: None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn prepared_explain_wait_leaves_native_runtime_free_to_poll_first_send() {
        let (_workload, capacity, mut owner, _session) = statement().await;
        let (send, receive) = futures::channel::oneshot::channel();
        let mut action = action(Box::pin(async move {
            receive.await.unwrap();
            Ok(Ok(PreparedExplain {
                value: PreparedAsyncExplain::Lines(vec!["observed".into()]),
            }))
        }));
        let produce = futures::future::poll_fn(|cx| {
            let window = owner.result_window_alias();
            action.poll_action(&mut owner.action_context(window.as_ref()), cx)
        });
        let signal = async move {
            tokio::task::yield_now().await;
            send.send(()).unwrap();
        };
        let (result, ()) = tokio::join!(produce, signal);
        let StatementActionValue::Local(local) = result.unwrap() else {
            panic!("real closed Local output")
        };
        assert_eq!(local.row_count(), 1);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        drop(action);
        drop(local);
        owner.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn prepared_explain_checks_cancellation_before_dispatch() {
        let (_workload, capacity, mut owner, _session) = statement().await;
        let polls = Arc::new(AtomicUsize::new(0));
        let inside = Arc::clone(&polls);
        let mut action = action(Box::pin(futures::future::poll_fn(move |_| {
            inside.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        })));
        owner
            .execution_owner_slot()
            .as_ref()
            .unwrap()
            .cancel(novarocks_workload_control::CancellationReason::Requested);
        let result = futures::future::poll_fn(|cx| {
            let window = owner.result_window_alias();
            action.poll_action(&mut owner.action_context(window.as_ref()), cx)
        })
        .await;
        let Err(error) = result else {
            panic!("cancelled preparation cannot deliver a result");
        };
        assert_eq!(error.kind(), QueryServiceErrorKind::Interrupted);
        assert_eq!(
            error.user_error().expect("typed cancellation").message(),
            "Query execution cancellation was requested"
        );
        assert_eq!(
            error
                .execution_failure()
                .expect("original cancellation fact")
                .site(),
            novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        drop(action);
        owner.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn occupied_protocol_destination_preserves_original_ready_and_owner() {
        let (_workload, _capacity, owner, _session) = statement().await;
        let token = owner.token();
        let mut source = Some(StatementActionReady::new(
            Ok(StatementActionValue::Ok(
                novarocks_query_application::session_control::StatementCommandCompletion::new(0),
            )),
            owner,
        ));
        let mut target = Some(StatementResult::Ok);
        assert!(StatementActionReady::transfer_protocol_in(&mut source, &mut target).is_err());
        assert_eq!(source.as_ref().unwrap().owner.token(), token);
        assert!(matches!(target, Some(StatementResult::Ok)));
        target = None;
        StatementActionReady::transfer_protocol_in(&mut source, &mut target).unwrap();
        assert!(source.is_none());
        let StatementResult::GovernedCompletion(result) = target.take().unwrap() else {
            panic!("same governed completion")
        };
        let (completion, mut protocol) = result.into_parts();
        assert_eq!(completion.count(), Some(0));
        assert_eq!(
            protocol.complete(),
            novarocks_query_application::session_control::GovernedStatementFinishOutcome::Completed
        );
    }
}
