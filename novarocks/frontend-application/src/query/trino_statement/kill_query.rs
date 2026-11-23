// SPDX-License-Identifier: Apache-2.0

use super::{CommandEntry, FrontendQuerySession};
use crate::query::{
    RoutedExecutionError, StatementResult, TypedCommandWorker, execute_prepared_dml_statement,
    internal_error,
};
use novarocks_query_application::{
    query_control::ClientQueryTime,
    session_error::QueryServiceError,
    sql::kill_query::{KillQueryCall, control_error},
};
use novarocks_sql::compiler::SqlCompileControl;
use std::sync::Arc;

impl FrontendQuerySession {
    pub(super) async fn execute_kill_query(
        &self,
        source: Arc<str>,
        call: Box<KillQueryCall>,
        entry: CommandEntry,
    ) -> Result<StatementResult, QueryServiceError> {
        let CommandEntry {
            mut statement,
            input,
        } = entry;
        let Some(window) = input.command.result_window_alias() else {
            return Ok(self.governed_typed_error(
                internal_error("kill_query has no original command result window"),
                statement,
            ));
        };
        let context = self.service.query_compiler.property_cpu_context(
            input.request.session().current_database().to_owned(),
            input.request.execution().sql_semantics().clone(),
            self.service.required_constant_interpreter.clone(),
            SqlCompileControl::new(
                input.deadline,
                crate::query_execution::planning::sql_cancellation_observation(
                    input.cancellation.clone(),
                ),
            ),
            input.command.scope().clone(),
            window.clone(),
        );
        let token = statement.token();
        let root = statement
            .take_execution_owner()
            .expect("original kill_query command owns its Work");
        let control = self.service.query_control.clone();
        let parameters = input.request.execution().prepared_parameters().cloned();
        let mut worker = Box::pin(execute_prepared_dml_statement(
            self.service.query_blocking_executor.clone(),
            input.cancellation.clone(),
            root,
            Some(window),
            statement.take_control_timing(),
            move || {
                call.evaluate(
                    &source,
                    &mut context.constant_port_with_parameters(parameters.as_deref()),
                )
                .map_err(RoutedExecutionError::Application)
            },
            move |arguments| {
                let (query_id, message) = arguments
                    .borrow()
                    .map_err(RoutedExecutionError::Application)?;
                control
                    .kill_client_query(token.session(), query_id, message, ClientQueryTime::now())
                    .map_err(|error| {
                        RoutedExecutionError::Application(control_error(error, query_id))
                    })?;
                Ok(StatementResult::Ok)
            },
        )) as TypedCommandWorker;
        self.finish_command_worker(
            statement,
            input.cancellation,
            input.timeout_duration,
            &mut worker,
        )
        .await
    }
}
