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
use novarocks_workload_control::WorkScope;

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
}

impl CommandContext {
    pub fn new(
        scope: WorkScope,
        connector_context: ConnectorRequestContext,
        statement_token: StatementToken,
    ) -> Self {
        Self {
            scope,
            connector_context,
            statement_token,
        }
    }

    pub fn scope(&self) -> &WorkScope {
        &self.scope
    }

    pub fn connector_context(&self) -> &ConnectorRequestContext {
        &self.connector_context
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandError {
    kind: CommandErrorKind,
    message: Arc<str>,
}

impl CommandError {
    pub fn new(kind: CommandErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> CommandErrorKind {
        self.kind
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for CommandError {}

pub trait CatalogCommandConsumer: Send + Sync + 'static {
    fn execute(
        &self,
        command: CatalogSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> CommandFuture;
}

pub trait StatisticsCommandConsumer: Send + Sync + 'static {
    fn execute(
        &self,
        command: StatisticsSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> CommandFuture;
}

pub trait MaintenanceCommandConsumer: Send + Sync + 'static {
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
    fn execute(
        &self,
        command: &MaterializedViewCommand,
        context: &RequestContext,
        command_context: &CommandContext,
    ) -> Result<QuerySessionOutput, String>;
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{ConnectorCancellation, ConnectorRequestContext};
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::*;
    use crate::session_control::{SessionToken, StatementToken};

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

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
            Arc::new(NeverCancelled),
            4_096,
            4_096,
        )
        .unwrap();
        let statement_token = StatementToken::new(SessionToken::new(7, 11), 13);
        let context =
            CommandContext::new(expected_scope.clone(), connector_context, statement_token);

        assert_eq!(context.scope().id(), expected_scope.id());
        assert!(context.scope().check().is_ok());
        assert!(!context.connector_context().cancellation().is_cancelled());
        assert_eq!(context.statement_token(), statement_token);
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
