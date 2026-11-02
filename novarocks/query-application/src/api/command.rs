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

use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc};

use novarocks_parser::ast::MaterializedViewStatement;
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_sql::semantic::{CatalogSqlCommand, MaintenanceSqlCommand, StatisticsSqlCommand};
use novarocks_workload_control::{ResultWindowAlias, ResultWindowClass, WorkError, WorkScope};

use crate::admitted_query_context::RequestContext;
use crate::protocol_delivery::QuerySessionOutput;
use crate::session_control::StatementToken;

pub type CommandFuture =
    Pin<Box<dyn Future<Output = Result<QuerySessionOutput, CommandError>> + Send + 'static>>;

/// An explicit specialized route may decline its exact parser-admitted
/// shape, allowing SQL to continue with the closed product-command router.
/// It is intentionally separate from [`CommandFuture`]: `None` is routing
/// control flow, never a protocol result.
pub type OptionalCommandFuture = Pin<
    Box<dyn Future<Output = Result<Option<QuerySessionOutput>, CommandError>> + Send + 'static>,
>;

/// Governed command context transferred from the SQL application to a product.
///
/// A command consumer can attribute work to the statement and observe its
/// cancellation state, but it never receives the statement's root owner. The
/// SQL protocol retains that owner until it has a terminal protocol outcome.
/// The connector context is frozen at the same admission boundary, so a
/// consumer cannot rebuild provider requests with a default deadline or a
/// detached cancellation source.
#[derive(Clone)]
pub struct CommandContext {
    scope: WorkScope,
    connector_context: ConnectorRequestContext,
    statement_token: StatementToken,
    principal: Arc<str>,
    result_window: Option<ResultWindowAlias>,
}

impl CommandContext {
    pub fn new(
        scope: WorkScope,
        connector_context: ConnectorRequestContext,
        statement_token: StatementToken,
        principal: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            scope,
            connector_context,
            statement_token,
            principal: principal.into(),
            result_window: None,
        }
    }

    /// Retain the already admitted position through this command's actual
    /// asynchronous and blocking owners. This never requests another window.
    pub fn with_result_window(mut self, window: ResultWindowAlias) -> Result<Self, CommandError> {
        if self.result_window.is_some()
            || !window.is_for_scope(&self.scope)
            || window.class() == ResultWindowClass::Closing
        {
            return Err(CommandError::new(
                CommandErrorKind::Conflict,
                "command result window is duplicated, foreign or closing",
            ));
        }
        self.scope.check().map_err(|error| {
            CommandError::new(
                match error {
                    WorkError::Cancelled(_) => CommandErrorKind::Cancelled,
                    _ => CommandErrorKind::Rejected,
                },
                format!("bind command result window: {error}"),
            )
        })?;
        self.result_window = Some(window);
        Ok(self)
    }

    /// A child producer must delegate this alias to its exact child scope;
    /// cloning it preserves the original position and all-objects envelope.
    pub fn result_window_alias(&self) -> Option<ResultWindowAlias> {
        self.result_window.clone()
    }

    pub fn scope(&self) -> &WorkScope {
        &self.scope
    }

    pub fn connector_context(&self) -> &ConnectorRequestContext {
        &self.connector_context
    }

    /// Who the server authenticated for this statement.
    ///
    /// A command that records what someone did needs the identity the server
    /// established, not one an argument claimed. Carrying it on the admitted
    /// context is what keeps the two from being confusable.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Immutable identity used only to bind role-local diagnostic observation
    /// to the admitted statement. It is not a completion or release handle.
    pub const fn statement_token(&self) -> StatementToken {
        self.statement_token
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CommandErrorKind {
    Invalid,
    Unsupported,
    Conflict,
    Rejected,
    Cancelled,
    Failed,
    EffectUnknown,
}

/// Knowledge of an original statement's external effect. This is independent
/// of provider identity, domain receipts, failure cause, and actual exit.
/// None at the observation boundary means no effect fact has been supplied;
/// it must not be interpreted as evidence of non-commit or safe retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatementExternalEffectDisposition {
    KnownUncommitted,
    CommitUnknown,
    KnownCommitted,
}
impl StatementExternalEffectDisposition {
    pub const fn do_not_retry(self) -> bool {
        matches!(self, Self::CommitUnknown | Self::KnownCommitted)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandError {
    failure_cause: Option<novarocks_failure_contract::SharedFailureCause>,
    kind: CommandErrorKind,
    message: Arc<str>,
    compile_control: Option<novarocks_type_contract::CompileControlError>,
    execution_failure: Option<crate::execution_error::ExecutionFailureInput>,
    user_error: Option<novarocks_user_error::UserError>,
    external_effect_disposition: Option<StatementExternalEffectDisposition>,
}

impl CommandError {
    pub fn new(kind: CommandErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            user_error: None,
            external_effect_disposition: None,
            failure_cause: None,
        }
    }

    /// The original action's external effect, independent of the selected
    /// failure cause. This creates no domain identity or receipt.
    pub fn with_external_effect_disposition(
        mut self,
        disposition: Option<StatementExternalEffectDisposition>,
    ) -> Self {
        self.external_effect_disposition = disposition;
        self
    }

    pub fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        self.external_effect_disposition
    }

    pub fn from_failure_cause(cause: novarocks_failure_contract::SharedFailureCause) -> Self {
        let service = crate::session_error::QueryServiceError::from_failure_cause(cause.clone());
        let mut error = Self::from_execution_failure(
            service
                .execution_failure()
                .expect("same selected domain fact")
                .clone(),
        );
        error.failure_cause = Some(cause);
        error
    }
    pub fn failure_cause(&self) -> Option<&novarocks_failure_contract::SharedFailureCause> {
        self.failure_cause.as_ref()
    }

    pub fn from_execution_failure(input: crate::execution_error::ExecutionFailureInput) -> Self {
        let mut error = Self::new(CommandErrorKind::Failed, input.user_error().to_string());
        error.execution_failure = Some(input);
        error
    }

    /// Preserve a domain-authored descriptor without reducing it to Display.
    pub fn from_user_error(error: novarocks_user_error::UserError) -> Self {
        let mut failure = Self::new(CommandErrorKind::Failed, error.to_string());
        failure.user_error = Some(error);
        failure
    }

    pub fn user_error(&self) -> Option<&novarocks_user_error::UserError> {
        self.user_error.as_ref()
    }

    pub fn execution_failure(&self) -> Option<&crate::execution_error::ExecutionFailureInput> {
        self.execution_failure.as_ref()
    }

    pub fn from_compile_control(error: novarocks_type_contract::CompileControlError) -> Self {
        let kind = match error {
            novarocks_type_contract::CompileControlError::Cancelled => CommandErrorKind::Cancelled,
            _ => CommandErrorKind::Failed,
        };
        let mut failure = Self::new(kind, error.to_string());
        failure.compile_control = Some(error);
        failure
    }
    pub fn with_compile_control(
        mut self,
        control: Option<novarocks_type_contract::CompileControlError>,
    ) -> Self {
        self.compile_control = control;
        self
    }

    pub fn compile_control_error(&self) -> Option<novarocks_type_contract::CompileControlError> {
        self.compile_control.clone()
    }

    pub const fn kind(&self) -> CommandErrorKind {
        self.kind
    }
}

impl From<String> for CommandError {
    fn from(error: String) -> Self {
        Self::new(CommandErrorKind::Failed, error)
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for CommandError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.failure_cause
            .as_ref()
            .map(|cause| cause as &dyn Error)
            .or_else(|| {
                self.compile_control
                    .as_ref()
                    .map(|cause| cause as &dyn Error)
            })
    }
}

pub trait CatalogCommandConsumer: Send + Sync + 'static {
    fn prepare_statement_action(
        &self,
        _command: CatalogSqlCommand,
        _request_context: RequestContext,
        _command_context: CommandContext,
    ) -> Result<Box<dyn crate::session_control::GovernedStatementAction>, CommandError> {
        Err(CommandError::new(
            CommandErrorKind::Unsupported,
            "catalog consumer does not provide a governed statement action",
        ))
    }

    fn execute(
        &self,
        command: CatalogSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> CommandFuture;
}

pub trait StatisticsCommandConsumer: Send + Sync + 'static {
    fn prepare_statement_action(
        &self,
        _command: StatisticsSqlCommand,
        _request_context: RequestContext,
        _command_context: CommandContext,
    ) -> Result<Box<dyn crate::session_control::GovernedStatementAction>, CommandError> {
        Err(CommandError::new(
            CommandErrorKind::Unsupported,
            "statistics consumer does not provide a governed statement action",
        ))
    }

    fn execute(
        &self,
        command: StatisticsSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> CommandFuture;
}

pub trait MaintenanceCommandConsumer: Send + Sync + 'static {
    /// Prepare a move-only action for the original registered statement source.
    /// Construction performs no physical dispatch and creates no Work owner.
    fn prepare_statement_action(
        &self,
        _command: MaintenanceSqlCommand,
        _request_context: RequestContext,
        _command_context: CommandContext,
    ) -> Result<Box<dyn crate::session_control::GovernedStatementAction>, CommandError> {
        Err(CommandError::new(
            CommandErrorKind::Unsupported,
            "maintenance consumer does not provide a governed statement action",
        ))
    }

    fn execute(
        &self,
        command: MaintenanceSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> CommandFuture;
}

pub struct MaterializedViewCommand {
    statement: MaterializedViewStatement,
}

impl MaterializedViewCommand {
    /// The SQL application retains the parser-admitted statement until the
    /// injected role adapter performs product-specific lowering.
    pub fn new(statement: MaterializedViewStatement) -> Self {
        Self { statement }
    }

    pub const fn statement(&self) -> &MaterializedViewStatement {
        &self.statement
    }
}

/// Protocol-neutral SQL-to-MV product consumer. Query Application owns the
/// statement and immutable admission contexts; role composition supplies the
/// adapter that owns Connector and query-execution capabilities.
pub trait MaterializedViewCommandConsumer: Send + Sync + 'static {
    /// Prepare the original asynchronous action for a parser-admitted MV statement.
    /// The caller retains the existing statement admission and supervision.
    fn prepare_statement_action(
        &self,
        command: MaterializedViewCommand,
        context: RequestContext,
        command_context: CommandContext,
    ) -> Result<Box<dyn crate::session_control::GovernedStatementAction>, CommandError>;
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::*;
    use crate::session_control::{SessionToken, StatementToken};

    #[test]
    fn command_context_exposes_the_statement_scope() {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();

        let expected_scope = root.owner.scope();
        let connector_context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            ConnectorStopOwner::new().view(),
            4_096,
            4_096,
        )
        .unwrap();
        let statement_token = StatementToken::new(
            SessionToken::new(
                crate::session_control::ClientSessionKey::new((7_u64 << 32) | 11_u64)
                    .expect("nonzero fixture session"),
            ),
            13,
        );
        let context = CommandContext::new(
            expected_scope.clone(),
            connector_context,
            statement_token,
            "test-principal",
        );

        assert_eq!(context.scope().id(), expected_scope.id());
        assert!(context.scope().check().is_ok());
        assert!(!context.connector_context().is_cancelled());
        assert_eq!(context.statement_token(), statement_token);
    }

    #[test]
    fn command_window_is_exact_and_survives_until_the_last_consumer_exit() {
        use novarocks_workload_control::{ResultCapacityConfig, ResultClosingCut};
        let control = WorkloadControl::try_new(
            WorkloadConfig {
                query_concurrency_limit: 1,
                ..WorkloadConfig::default()
            },
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig {
                positions: [1; 4],
                client_compute_positions: 1,
                client_short_tail_positions: 0,
                supported_cancel_burst: 0,
                sustained_cancels_per_second: 0,
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        control.mark_ready().unwrap();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let other = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let context_for = |scope| {
            CommandContext::new(
                scope,
                ConnectorRequestContext::try_new(
                    Instant::now() + Duration::from_secs(1),
                    ConnectorStopOwner::new().view(),
                    4096,
                    4096,
                )
                .unwrap(),
                StatementToken::new(
                    SessionToken::new(
                        crate::session_control::ClientSessionKey::new((7_u64 << 32) | 11_u64)
                            .expect("nonzero fixture session"),
                    ),
                    13,
                ),
                "test-principal",
            )
        };
        assert_eq!(
            context_for(other.owner.scope())
                .with_result_window(window.retain_alias())
                .err()
                .unwrap()
                .kind(),
            CommandErrorKind::Conflict
        );
        let closing = capacity
            .try_acquire_closing(&root.owner.scope(), ResultClosingCut::OriginatingFailure)
            .unwrap();
        assert_eq!(
            context_for(root.owner.scope())
                .with_result_window(closing.retain_alias())
                .err()
                .unwrap()
                .kind(),
            CommandErrorKind::Conflict
        );
        drop(closing);
        let context = context_for(root.owner.scope())
            .with_result_window(window.retain_alias())
            .unwrap();
        assert_eq!(
            context.result_window_alias().unwrap().class(),
            ResultWindowClass::Local
        );
        assert_eq!(
            context
                .clone()
                .with_result_window(window.retain_alias())
                .err()
                .unwrap()
                .kind(),
            CommandErrorKind::Conflict
        );
        let consumer = context.clone();
        let late_context = context_for(root.owner.scope());
        let late_alias = window.retain_alias();
        root.owner.complete();
        assert_eq!(
            late_context
                .with_result_window(late_alias)
                .err()
                .unwrap()
                .kind(),
            CommandErrorKind::Rejected
        );
        root.business.release();
        other.owner.complete();
        other.business.release();
        drop(window);
        drop(context);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        assert_eq!(control.snapshot().root_responsibilities, 1);
        drop(consumer);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn materialized_view_command_retains_the_parser_admitted_shape() {
        let statements = novarocks_parser::parse("SHOW MATERIALIZED VIEWS FROM analytics")
            .expect("MV statement should parse");
        let [novarocks_parser::ast::Statement::MaterializedView(statement)] = statements.as_slice()
        else {
            panic!("expected one parser-admitted MV statement");
        };

        let command = MaterializedViewCommand::new(statement.clone());
        assert!(matches!(
            command.statement(),
            novarocks_parser::ast::MaterializedViewStatement::Show(_)
        ));
    }
}

#[cfg(test)]
mod compile_control_tests {
    use super::*;
    use novarocks_type_contract::CompileControlError;

    #[test]
    fn command_terminal_retains_control_without_inferring_from_display_text() {
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let error = CommandError::from_compile_control(control.clone());
            assert_eq!(error.compile_control_error(), Some(control.clone()));
            assert_eq!(error.clone().compile_control_error(), Some(control.clone()));
            assert_eq!(
                CommandError::from(control.to_string()).compile_control_error(),
                None
            );
        }
    }
}
