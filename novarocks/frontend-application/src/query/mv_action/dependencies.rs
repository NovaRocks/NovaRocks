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

//! The original statement owns dependency observation and its pure graph turn.
//! A borrowed waiter never owns the StateStore request or CPU exit receipt.

use super::{CommandInput, CreateRequest, MvCommandRequest, MvPhaseFailure, MvWake, StepRequest};
use crate::mv::domain::{
    application::{MvApplicationError, MvApplicationErrorKind, MvRefreshRequest},
    dependency::refresh::MvRefreshDependencyStep,
};
use crate::query_execution::{
    maintenance::io_admission::MetadataIoAdmission,
    mv_assembly::refresh_preparation::{
        async_original::{
            MvRefreshDependencyGraphInput, MvRefreshPreparationOriginal, MvRefreshPreparationOutput,
        },
        driver::{MvRefreshTurnCustody, MvTurnFailure},
        io_gate::{MvRefreshPreparationIo, MvRefreshPreparationServices},
    },
};
use crate::task_execution::status_intake::StatusIntakeWake;
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::StatementNativeHealthFailure,
    cancellation::QueryCancellationView,
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_state_store_api::StateStoreExecutionRequests;
use std::{
    sync::Arc,
    task::{Context, Poll},
};

pub(super) struct DependencyPreparation {
    input: Arc<CommandInput>,
    driver: Option<MvRefreshTurnCustody>,
    raw: Option<Result<MvRefreshPreparationOutput, MvApplicationError>>,
    steps: Option<Vec<StepRequest>>,
    failure: Option<MvPhaseFailure>,
    health: Option<StatementNativeHealthFailure>,
    exit_unproven: StatementNativeHealthFailure,
    busy: bool,
    handed_off: bool,
}
impl DependencyPreparation {
    pub(super) fn new(
        input: Arc<CommandInput>,
        io: Arc<MetadataIoAdmission>,
        capacity: QueryResultCapacityBinding,
        requests: Arc<dyn StateStoreExecutionRequests>,
        cancel: QueryCancellationView,
        wake: Arc<MvWake>,
    ) -> Result<Self, QueryServiceError> {
        let mut result = Self {
            input,
            driver: None,
            raw: None,
            steps: None,
            failure: None,
            health: None,
            exit_unproven: StatementNativeHealthFailure::actual_exit_unproven(
                "MV dependency preparation poll or retirement did not return normally",
                Arc::new(DependencyExitUnproven),
            ),
            busy: false,
            handed_off: false,
        };
        match &result.input.request {
            MvCommandRequest::Create(request) => {
                result.steps = Some(vec![StepRequest::Create(Box::new(CreateRequest {
                    target: request.target.clone(),
                    statement: request.statement.clone(),
                }))]);
            }
            MvCommandRequest::Control(request) => {
                // One metadata command, not a refresh dependency traversal.
                result.steps = Some(vec![StepRequest::Control(request.clone())]);
            }
            MvCommandRequest::Repartition {
                statement,
                target,
                fields,
            } => {
                // Repartition has no upstream refresh traversal. Preserve its
                // original single request without acquiring any CPU position.
                result.steps = Some(vec![StepRequest::Iceberg {
                    target: target.clone(),
                    statement: statement.clone(),
                    repartition: Some(fields.clone()),
                }]);
            }
            MvCommandRequest::Refresh { requested, .. }
            | MvCommandRequest::ScheduledRefresh { requested, .. } => {
                let catalog = requested.catalog.as_deref().ok_or_else(|| {
                    invalid("REFRESH MATERIALIZED VIEW for an Iceberg MV requires current Iceberg catalog context")
                })?;
                let requested = novarocks_mv_application::dependency::iceberg_mv_dependency_ref(
                    catalog,
                    &requested.database,
                    &requested.name,
                );
                // Authenticate the supplied alias against this MIO's original
                // positive Root before constructing any source or borrowed F.
                let positive = io.root_capacity().map_err(super::original_error)?;
                let window = capacity.try_window_alias().map_err(super::original_error)?;
                if !window.is_for_scope(positive.scope())
                    || !window.shares_capacity_with(&positive.window_alias())
                {
                    return Err(super::original_error(
                        novarocks_workload_control::WorkError::Conflict,
                    ));
                }
                let original = io
                    .original_result_scope(&result.input.context)
                    .map_err(super::original_error)?;
                let cpu = result
                    .input
                    .services
                    .blocking
                    .provider_creator_cpu()
                    .map_err(super::original_error)?;
                let runtime = result.input.services.data.handle().clone();
                let wake: Arc<dyn StatusIntakeWake> = wake;
                let gate = Arc::new(MvRefreshPreparationIo::new(
                    MvRefreshPreparationServices {
                        cpu: cpu.clone(),
                        scope: positive.scope().clone(),
                        window: window.clone(),
                        cancellation: cancel.clone(),
                        runtime: runtime.clone(),
                        wake: wake.clone(),
                        requests,
                    },
                    original,
                ));
                let source = MvRefreshPreparationOriginal::for_dependency_graph(
                    MvRefreshDependencyGraphInput {
                        readiness: result.input.ports.readiness().clone(),
                        requested,
                    },
                    gate.clone(),
                );
                let mut driver = MvRefreshTurnCustody::new(
                    gate,
                    cpu,
                    positive.scope().clone(),
                    window,
                    cancel,
                    runtime,
                    wake,
                );
                // The same gate was used to construct both original objects;
                // no worker or Provider future has been polled yet.
                assert!(
                    driver.install_preparation(source).is_ok(),
                    "same original MV dependency gate"
                );
                result.driver = Some(driver);
            }
        }
        Ok(result)
    }

    pub(super) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.busy.then(|| self.exit_unproven.clone()))
            .or_else(|| {
                self.driver
                    .as_ref()
                    .and_then(MvRefreshTurnCustody::local_health_failure)
            })
    }

    pub(super) fn poll_steps(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Vec<StepRequest>, MvPhaseFailure>> {
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        self.busy = true;
        let polled = self.poll_steps_inner(cx);
        self.busy = false;
        polled
    }

    fn poll_steps_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Vec<StepRequest>, MvPhaseFailure>> {
        if let Some(error) = &self.failure {
            return Poll::Ready(Err(error.clone()));
        }
        if self.handed_off {
            return Poll::Ready(Err(super::service(
                "MV dependency steps were already transferred",
            )
            .into()));
        }
        if self.steps.is_none() {
            let driver = self
                .driver
                .as_mut()
                .expect("Refresh owns its original graph driver");
            match driver.poll_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(MvTurnFailure::Health(health))) => {
                    self.health = Some(health);
                    return Poll::Pending;
                }
                Poll::Ready(Err(MvTurnFailure::Admission(error))) => {
                    let error = super::original_error(error);
                    self.failure = Some(error.clone().into());
                    return Poll::Ready(Err(error.into()));
                }
                Poll::Ready(Err(MvTurnFailure::Cpu(error))) => {
                    let error = QueryServiceError::from_execution_failure((*error).clone());
                    self.failure = Some(error.clone().into());
                    return Poll::Ready(Err(error.into()));
                }
                Poll::Ready(Ok(())) => {}
            }
            // The driver has received the actual CPU marker AND footer. Move
            // the original result into this stable field before retiring it.
            if !driver.preparation_original().take_result_in(&mut self.raw) {
                self.health = Some(self.exit_unproven.clone());
                return Poll::Pending;
            }
            if matches!(
                self.raw,
                Some(Ok(MvRefreshPreparationOutput::DependencySteps(_))) | Some(Err(_))
            ) {
                match self.raw.take().unwrap() {
                    Ok(MvRefreshPreparationOutput::DependencySteps(steps)) => {
                        let (MvCommandRequest::Refresh {
                            statement,
                            requested,
                        }
                        | MvCommandRequest::ScheduledRefresh {
                            statement,
                            requested,
                        }) = &self.input.request
                        else {
                            unreachable!("only a refresh command owns the dependency graph");
                        };
                        match refresh_steps(steps, statement, requested) {
                            Ok(steps) => self.steps = Some(steps),
                            Err(error) => self.failure = Some(error.into()),
                        }
                    }
                    Err(error) => self.failure = Some(application_error(error)),
                    _ => unreachable!(),
                }
            } else {
                // Do not Drop another domain's unique result on a protocol
                // mismatch. The same Source retains it with unproven health.
                self.health = Some(self.exit_unproven.clone());
                return Poll::Pending;
            }
            if driver.retire_transferred().is_err() {
                self.health = Some(self.exit_unproven.clone());
                return Poll::Pending;
            }
            if let Some(error) = &self.failure {
                return Poll::Ready(Err(error.clone()));
            }
        }
        self.handed_off = true;
        Poll::Ready(Ok(self.steps.take().expect("stable dependency result")))
    }

    pub(super) fn poll_closed(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(health) = self.local_health_failure() {
            return Poll::Ready(Err(health));
        }
        self.busy = true;
        let polled = match self.driver.as_mut() {
            Some(driver) => driver.poll_closed(cx),
            None => Poll::Ready(Ok(())),
        };
        if let Poll::Ready(Err(health)) = &polled {
            self.health = Some(health.clone());
        }
        self.busy = false;
        polled
    }
}

fn refresh_steps(
    steps: Vec<MvRefreshDependencyStep>,
    statement: &MvRefreshRequest,
    requested: &novarocks_sql::planning::mv::SqlMvTarget,
) -> Result<Vec<StepRequest>, QueryServiceError> {
    if steps.is_empty() {
        return Err(invalid("MV refresh dependency planner returned no steps"));
    }
    Ok(steps
        .into_iter()
        .map(|step| {
            if !step.is_iceberg() {
                // The serial executor encounters this refusal in the ORIGINAL
                // order, after preserving any preceding publication effects.
                return StepRequest::Unsupported {
                    display_name: step.display_name(),
                };
            }
            let target = step.into_target();
            let full = statement.full && &target == requested;
            let statement = MvRefreshRequest {
                name_parts: vec![target.database.clone(), target.name.clone()],
                full,
            };
            StepRequest::Iceberg {
                target,
                statement,
                repartition: None,
            }
        })
        .collect())
}

fn application_error(error: MvApplicationError) -> MvPhaseFailure {
    let service = application_service_error(error.clone());
    MvPhaseFailure {
        service,
        domain: Some(error),
    }
}
fn application_service_error(error: MvApplicationError) -> QueryServiceError {
    if let Some(cause) = error.original_failure() {
        return QueryServiceError::from_failure_cause(cause.clone())
            .with_compile_control(error.compile_control_error());
    }
    if let Some(control) = error.compile_control_error() {
        return QueryServiceError::from_compile_control(control);
    }
    if let Some(analyze) = error.unavailable_implementation() {
        return QueryServiceError::from_user_error(analyze.to_user_error(None));
    }
    let kind = match error.kind() {
        MvApplicationErrorKind::InvalidRequest => QueryServiceErrorKind::InvalidValue,
        _ => QueryServiceErrorKind::Internal,
    };
    QueryServiceError::new(kind, error.message())
}
fn invalid(message: &'static str) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::InvalidValue, message)
}
#[derive(Debug)]
struct DependencyExitUnproven;
impl std::fmt::Display for DependencyExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MV dependency source poll or Drop did not return normally")
    }
}
impl std::error::Error for DependencyExitUnproven {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::dependency::refresh::refresh_step_for_dependency_object;
    use novarocks_mv_application::dependency::{
        iceberg_mv_dependency_ref, starrocks_mv_dependency_ref,
    };

    #[test]
    fn actual_dependency_steps_preserve_serial_refusal_and_only_requested_full() {
        let objects = [
            iceberg_mv_dependency_ref("ice", "db", "upstream"),
            starrocks_mv_dependency_ref("db", "unsupported"),
            iceberg_mv_dependency_ref("ice", "db", "requested"),
        ];
        let original = objects
            .iter()
            .map(refresh_step_for_dependency_object)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let requested = original[2].target().clone();
        let statement = MvRefreshRequest {
            name_parts: vec!["requested".into()],
            full: true,
        };
        let actual = refresh_steps(original, &statement, &requested).unwrap();
        assert_eq!(actual.len(), 3);
        let StepRequest::Iceberg {
            target,
            statement,
            repartition,
        } = &actual[0]
        else {
            panic!("first real step")
        };
        assert_eq!(target.name, "upstream");
        assert_eq!(statement.name_parts, ["db", "upstream"]);
        assert!(!statement.full);
        assert!(repartition.is_none());
        let StepRequest::Unsupported { display_name } = &actual[1] else {
            panic!("original serial refusal")
        };
        assert_eq!(display_name, &objects[1].display_name());
        let StepRequest::Iceberg {
            target,
            statement,
            repartition,
        } = &actual[2]
        else {
            panic!("requested real step")
        };
        assert_eq!(target, &requested);
        assert!(statement.full);
        assert!(repartition.is_none());
    }

    #[test]
    fn empty_original_graph_is_not_a_successful_noop() {
        let requested = novarocks_sql::planning::mv::SqlMvTarget {
            catalog: Some("ice".into()),
            database: "db".into(),
            name: "requested".into(),
        };
        let statement = MvRefreshRequest {
            name_parts: vec!["requested".into()],
            full: false,
        };
        assert!(
            matches!(refresh_steps(Vec::new(), &statement, &requested), Err(error)
            if error.kind() == QueryServiceErrorKind::InvalidValue
                && error.message() == "MV refresh dependency planner returned no steps")
        );
    }

    #[test]
    fn original_admission_cause_survives_dependency_projection() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let application = MvApplicationError::from_connector(
            MvApplicationErrorKind::Engine,
            ConnectorError::from_domain(
                ConnectorErrorKind::ResourceExhausted,
                novarocks_workload_control::WorkError::Capacity("original dependency admission"),
            ),
        );
        let source = application.original_failure().unwrap().clone();
        let fact = source.failure_fact();
        let selected = application_error(application);
        assert_eq!(selected.service.failure_cause(), Some(&source));
        assert_eq!(selected.service.execution_failure().unwrap().fact(), fact);
        assert!(selected.service.compile_control_error().is_none());
        let domain = selected.domain.expect("original dependency domain policy");
        assert_eq!(domain.kind(), MvApplicationErrorKind::Engine);
        assert_eq!(domain.original_failure(), Some(&source));
    }
}
