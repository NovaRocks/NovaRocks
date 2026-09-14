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
use novarocks_workload_control::WorkScope;

use super::ObjectPath;
use crate::admitted_query_context::RequestContext;
use crate::protocol_delivery::QuerySessionOutput;

pub type CommandFuture =
    Pin<Box<dyn Future<Output = Result<CommandOutput, CommandError>> + Send + 'static>>;

/// Governed command context transferred from the SQL application to a product.
///
/// A command consumer can attribute work to the statement and observe its
/// cancellation state, but it never receives the statement's root owner. The
/// SQL protocol retains that owner until it has a terminal protocol outcome.
/// The connector context is frozen at the same admission boundary, so a
/// consumer cannot rebuild provider requests with a default deadline or a
/// detached cancellation source.
pub struct CommandContext {
    scope: WorkScope,
    connector_context: ConnectorRequestContext,
}

impl CommandContext {
    pub fn new(scope: WorkScope, connector_context: ConnectorRequestContext) -> Self {
        Self {
            scope,
            connector_context,
        }
    }

    pub fn scope(&self) -> &WorkScope {
        &self.scope
    }

    pub fn connector_context(&self) -> &ConnectorRequestContext {
        &self.connector_context
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandProperty {
    name: Arc<str>,
    value: Arc<str>,
}

impl CommandProperty {
    pub fn try_new(name: impl Into<Arc<str>>, value: impl Into<Arc<str>>) -> Option<Self> {
        let name = name.into();
        let value = value.into();
        if name.is_empty() || value.len() > 64 * 1024 {
            return None;
        }
        Some(Self { name, value })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CommandOutput {
    Acknowledged,
    Rows(CommandRows),
}

/// Bounded administrative rows. Distributed query rows use ExecutionHandle.
pub type CommandCell = Option<Arc<str>>;
pub type CommandRow = Arc<[CommandCell]>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandRows {
    columns: Arc<[Arc<str>]>,
    rows: Arc<[CommandRow]>,
}

impl CommandRows {
    pub fn try_new(columns: Vec<Arc<str>>, rows: Vec<Vec<Option<Arc<str>>>>) -> Option<Self> {
        if columns.is_empty()
            || columns.iter().any(|column| column.is_empty())
            || rows.len() > 100_000
            || rows.iter().any(|row| row.len() != columns.len())
        {
            return None;
        }
        Some(Self {
            columns: columns.into(),
            rows: rows
                .into_iter()
                .map(Arc::<[Option<Arc<str>>]>::from)
                .collect::<Vec<_>>()
                .into(),
        })
    }

    pub fn columns(&self) -> &[Arc<str>] {
        &self.columns
    }

    pub fn rows(&self) -> &[CommandRow] {
        &self.rows
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CatalogCommandKind {
    CreateCatalog,
    DropCatalog,
    CreateNamespace,
    DropNamespace,
    CreateTable,
    DropTable,
    AlterTable,
    AlterReference,
}

pub struct CatalogCommand {
    kind: CatalogCommandKind,
    target: ObjectPath,
    if_exists: bool,
    properties: Arc<[CommandProperty]>,
}

impl CatalogCommand {
    #[allow(dead_code)]
    pub(crate) fn new(
        kind: CatalogCommandKind,
        target: ObjectPath,
        if_exists: bool,
        properties: Vec<CommandProperty>,
    ) -> Self {
        Self {
            kind,
            target,
            if_exists,
            properties: properties.into(),
        }
    }

    pub const fn kind(&self) -> CatalogCommandKind {
        self.kind
    }
    pub const fn target(&self) -> &ObjectPath {
        &self.target
    }
    pub const fn if_exists(&self) -> bool {
        self.if_exists
    }
    pub fn properties(&self) -> &[CommandProperty] {
        &self.properties
    }
}

pub trait CatalogCommandConsumer: Send + Sync + 'static {
    fn execute(&self, command: CatalogCommand, context: CommandContext) -> CommandFuture;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StatisticsCommandKind {
    Analyze,
    ShowAnalyzeJobs,
    CancelAnalyze,
    ShowTableStatistics,
}

pub struct StatisticsCommand {
    kind: StatisticsCommandKind,
    target: Option<ObjectPath>,
    columns: Arc<[Arc<str>]>,
    job_id: Option<[u8; 16]>,
}

impl StatisticsCommand {
    #[allow(dead_code)]
    pub(crate) fn new(
        kind: StatisticsCommandKind,
        target: Option<ObjectPath>,
        columns: Vec<Arc<str>>,
        job_id: Option<[u8; 16]>,
    ) -> Self {
        Self {
            kind,
            target,
            columns: columns.into(),
            job_id,
        }
    }

    pub const fn kind(&self) -> StatisticsCommandKind {
        self.kind
    }
    pub const fn target(&self) -> Option<&ObjectPath> {
        self.target.as_ref()
    }
    pub fn columns(&self) -> &[Arc<str>] {
        &self.columns
    }
    pub const fn job_id(&self) -> Option<[u8; 16]> {
        self.job_id
    }
}

pub trait StatisticsCommandConsumer: Send + Sync + 'static {
    fn execute(&self, command: StatisticsCommand, context: CommandContext) -> CommandFuture;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MaintenanceCommandKind {
    Optimize,
    RewriteDataFiles,
    RewriteManifests,
    RemoveOrphanFiles,
    ExpireSnapshots,
    ShowOptimize,
}

pub struct MaintenanceCommand {
    kind: MaintenanceCommandKind,
    target: ObjectPath,
    properties: Arc<[CommandProperty]>,
}

impl MaintenanceCommand {
    #[allow(dead_code)]
    pub(crate) fn new(
        kind: MaintenanceCommandKind,
        target: ObjectPath,
        properties: Vec<CommandProperty>,
    ) -> Self {
        Self {
            kind,
            target,
            properties: properties.into(),
        }
    }

    pub const fn kind(&self) -> MaintenanceCommandKind {
        self.kind
    }
    pub const fn target(&self) -> &ObjectPath {
        &self.target
    }
    pub fn properties(&self) -> &[CommandProperty] {
        &self.properties
    }
}

pub trait MaintenanceCommandConsumer: Send + Sync + 'static {
    fn execute(&self, command: MaintenanceCommand, context: CommandContext) -> CommandFuture;
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
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use novarocks_spi::connector::{ConnectorCancellation, ConnectorRequestContext};
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    #[test]
    fn administrative_rows_require_one_value_per_column() {
        let columns = vec![Arc::<str>::from("job"), Arc::<str>::from("state")];
        let rows = vec![vec![Some(Arc::<str>::from("one"))]];
        assert!(CommandRows::try_new(columns, rows).is_none());
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
        let context = CommandContext::new(expected_scope.clone(), connector_context);

        assert_eq!(context.scope().id(), expected_scope.id());
        assert!(context.scope().check().is_ok());
        assert!(!context.connector_context().cancellation().is_cancelled());
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
