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

//! Metadata maintenance progresses in its original registered statement.

use super::*;
use crate::query_execution::maintenance::{
    MaintenanceStatementResult, command::MaintenanceCommandCall,
};
use novarocks_query_application::{
    api::LocalResultProducer,
    session_control::{GovernedActionContext, GovernedStatementAction, StatementActionValue},
};
use std::task::{Context, Poll};

pub(super) fn prepare(
    executor: &MaintenanceCommandExecutor,
    command: &novarocks_sql::semantic::MaintenanceSqlCommand,
    context: &RequestContext,
    command_context: &CommandContext,
    #[cfg(novarocks_dev_controls)] observation: Option<
        crate::preparation_diagnostics::StatementObservationHandle,
    >,
) -> Result<Box<dyn GovernedStatementAction>, CommandError> {
    if let Some(action) = executor
        .prepare_optimize_action(
            command,
            context.session().current_catalog(),
            context.session().current_database(),
            context.execution(),
            command_context.connector_context(),
        )
        .map_err(maintenance_command_error)?
    {
        return Ok(action);
    }
    if let Some(action) = executor
        .prepare_direct_rewrite_action(
            command,
            context.session().current_catalog(),
            context.session().current_database(),
            context.execution(),
            command_context.connector_context(),
            *context.session().presentation(),
            #[cfg(novarocks_dev_controls)]
            observation,
        )
        .map_err(maintenance_command_error)?
    {
        return Ok(action);
    }
    if let Some(action) = executor
        .prepare_cleanup_action(
            command,
            context.session().current_catalog(),
            context.session().current_database(),
            context.execution(),
            command_context.connector_context(),
            *context.session().presentation(),
        )
        .map_err(maintenance_command_error)?
    {
        return Ok(action);
    }
    let capacity = context.execution().result_capacity().ok_or_else(|| {
        CommandError::new(
            CommandErrorKind::Rejected,
            "maintenance has no admitted result capacity",
        )
    })?;
    let origin = capacity.try_window_alias().map_err(|error| {
        CommandError::new(
            CommandErrorKind::Rejected,
            format!("maintenance result custody refused: {error:?}"),
        )
    })?;
    if !origin.is_for_scope(command_context.scope()) {
        return Err(CommandError::new(
            CommandErrorKind::Conflict,
            "maintenance contexts belong to different original scopes",
        ));
    }
    let call = executor
        .prepare_command(
            command,
            context.session().current_catalog(),
            context.session().current_database(),
            context.execution(),
            command_context.connector_context(),
        )
        .map_err(maintenance_command_error)?;
    Ok(Box::new(MetadataMaintenanceAction {
        family: *context.session().presentation(),
        call,
        origin,
        started: false,
        result: None,
    }))
}

struct MetadataMaintenanceAction {
    family: novarocks_result_contract::ClientPresentationFamily,
    call: MaintenanceCommandCall,
    origin: novarocks_workload_control::ResultWindowAlias,
    started: bool,
    result: Option<
        Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError>,
    >,
}
impl MetadataMaintenanceAction {
    fn poll_result(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if !self.origin.is_for_scope(owner.scope()) {
            return Poll::Ready(Err(internal_error(
                "maintenance action has a foreign original owner",
            )));
        }
        let cancellation =
            QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
        if !self.started {
            if let Some(reason) = cancellation.reason() {
                return Poll::Ready(Err(cancellation_error(reason)));
            }
            self.started = true;
        }
        if self.result.is_none() {
            let execution = match owner.execution_owner_slot().as_ref() {
                Some(execution) => execution,
                None => {
                    return Poll::Ready(Err(internal_error(
                        "metadata source lost its original execution owner",
                    )));
                }
            };
            match self.call.poll_result(execution, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => self.result = Some(result),
            }
        }
        // The source owns the Call and its publication facts independently of
        // its completed Product F and of the UI observer's continued presence.
        let disposition = self.call.publication_disposition();
        let applied = match self.call.has_applied_publication() {
            Ok(applied) => applied,
            Err(message) => {
                return Poll::Ready(Err(match disposition {
                    Some(disposition) => internal_error(message)
                        .attach_external_effect_disposition(
                            crate::query::external_effect_from_lake(disposition),
                        ),
                    None => internal_error(message),
                }));
            }
        };
        let result = self
            .result
            .take()
            .expect("stable original maintenance result");
        let result = match select_maintenance_completion(
            result,
            disposition,
            applied,
            cancellation.reason(),
        ) {
            Ok(result) => result,
            Err(error) => return Poll::Ready(Err(error)),
        };
        Poll::Ready(match result {
            MaintenanceStatementResult::Ok => Ok(StatementActionValue::Ok(
                novarocks_query_application::session_control::StatementCommandCompletion::new(0),
            )),
            MaintenanceStatementResult::Query(result) => {
                let window = match owner.result_window_alias() {
                    Some(window) => window,
                    None => {
                        return Poll::Ready(Err(internal_error(
                            "maintenance has no original result window",
                        )));
                    }
                };
                let producer =
                    match LocalResultProducer::try_new(owner.scope(), window, self.family) {
                        Ok(producer) => producer,
                        Err(error) => return Poll::Ready(Err(internal_error(error))),
                    };
                producer
                    .produce(|| Ok(result))
                    .map(StatementActionValue::Local)
                    .map_err(internal_error)
            }
        })
    }
}

impl GovernedStatementAction for MetadataMaintenanceAction {
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<
            Box<dyn novarocks_query_application::session_control::GovernedStatementResidual>,
        >,
    ) -> Result<(), novarocks_query_application::api::StatementNativeHealthFailure> {
        self.call.handoff_residual_in(target)
    }

    fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.call.local_health_failure()
    }

    fn external_effect_disposition(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        self.call
            .publication_disposition()
            .map(crate::query::external_effect_from_lake)
    }

    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        match self.poll_result(owner, cx) {
            Poll::Ready(Err(error)) => {
                Poll::Ready(Err(match self.call.publication_disposition() {
                    Some(disposition) => error.attach_external_effect_disposition(
                        crate::query::external_effect_from_lake(disposition),
                    ),
                    None => error,
                }))
            }
            result => result,
        }
    }
}

/// A real publication is irreversible. Cancellation still vetoes new work,
/// while the original completed Product selects its necessary business result.
fn select_maintenance_completion(
    result: Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError>,
    disposition: Option<novarocks_spi::connector::LakePublicationDisposition>,
    applied: bool,
    cancellation: Option<QueryCancellationReason>,
) -> Result<MaintenanceStatementResult, QueryServiceError> {
    use novarocks_spi::connector::LakePublicationDisposition;
    let committed = disposition == Some(LakePublicationDisposition::KnownCommitted);
    match result {
        Err(error) => {
            let routed = product_command_error(maintenance_command_error(error));
            Err(match cancellation {
                Some(reason) if !committed => {
                    selected_execution_or_worker_fallback(Err(routed), cancellation_error(reason))
                }
                _ => routed_execution_service_error(routed),
            })
        }
        Ok(result) => match cancellation {
            Some(reason) if !(committed && applied) => {
                let mut error = cancellation_error(reason);
                if let Some(disposition) = disposition {
                    error = error.attach_external_effect_disposition(
                        crate::query::external_effect_from_lake(disposition),
                    );
                }
                drop(result);
                Err(error)
            }
            _ => Ok(result),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::LakePublicationDisposition;
    use novarocks_table_maintenance::runtime::TerminalError;

    #[test]
    fn irreversible_maintenance_keeps_success_and_required_failure_after_cancel() {
        for reason in [
            QueryCancellationReason::ExplicitKill {
                requester_session:
                    novarocks_query_application::session_control::ClientSessionKey::new(7)
                        .expect("nonzero fixture requester"),
            },
            QueryCancellationReason::DeadlineExceeded { timeout_ms: 1 },
            QueryCancellationReason::ClientDisconnected,
            QueryCancellationReason::ServerShutdown,
        ] {
            assert!(matches!(
                select_maintenance_completion(
                    Ok(MaintenanceStatementResult::Ok),
                    Some(LakePublicationDisposition::KnownCommitted),
                    true,
                    Some(reason.clone()),
                ),
                Ok(MaintenanceStatementResult::Ok)
            ));
            let error = select_maintenance_completion(
                Err(TerminalError::known_committed_finalization_failed(
                    "required cache invalidation failed",
                )),
                Some(LakePublicationDisposition::KnownCommitted),
                true,
                Some(reason),
            )
            .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("required cache invalidation failed")
            );
            assert_eq!(
                error.external_effect_disposition(),
                Some(novarocks_query_application::api::StatementExternalEffectDisposition::KnownCommitted)
            );
        }
    }

    #[test]
    fn no_op_completion_is_not_an_irreversible_publication() {
        assert!(
            select_maintenance_completion(
                Ok(MaintenanceStatementResult::Ok),
                Some(LakePublicationDisposition::KnownCommitted),
                false,
                Some(QueryCancellationReason::ExplicitKill {
                    requester_session:
                        novarocks_query_application::session_control::ClientSessionKey::new(7)
                            .expect("nonzero fixture requester")
                }),
            )
            .is_err()
        );
    }

    #[test]
    fn cancellation_still_vetoes_unpublished_maintenance() {
        let error = select_maintenance_completion(
            Ok(MaintenanceStatementResult::Ok),
            None,
            false,
            Some(QueryCancellationReason::DeadlineExceeded { timeout_ms: 1 }),
        )
        .unwrap_err();
        assert_eq!(
            error.kind(),
            cancellation_error(QueryCancellationReason::DeadlineExceeded { timeout_ms: 1 }).kind()
        );
        assert_eq!(error.external_effect_disposition(), None);
    }
}
