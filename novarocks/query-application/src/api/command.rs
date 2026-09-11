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

use novarocks_workload_control::WorkOwner;

use super::ObjectPath;

pub type CommandFuture =
    Pin<Box<dyn Future<Output = Result<CommandOutput, CommandError>> + Send + 'static>>;

/// Governed command context transferred from the SQL application to a product.
pub struct CommandContext {
    owner: WorkOwner,
}

impl CommandContext {
    #[allow(dead_code)]
    pub(crate) const fn new(owner: WorkOwner) -> Self {
        Self { owner }
    }

    pub fn into_work_owner(self) -> WorkOwner {
        self.owner
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MaterializedViewCommandKind {
    Create,
    Drop,
    Alter,
    Refresh,
    Show,
    ExplainRefresh,
}

pub struct MaterializedViewCommand {
    kind: MaterializedViewCommandKind,
    target: Option<ObjectPath>,
    properties: Arc<[CommandProperty]>,
}

impl MaterializedViewCommand {
    #[allow(dead_code)]
    pub(crate) fn new(
        kind: MaterializedViewCommandKind,
        target: Option<ObjectPath>,
        properties: Vec<CommandProperty>,
    ) -> Self {
        Self {
            kind,
            target,
            properties: properties.into(),
        }
    }

    pub const fn kind(&self) -> MaterializedViewCommandKind {
        self.kind
    }
    pub const fn target(&self) -> Option<&ObjectPath> {
        self.target.as_ref()
    }
    pub fn properties(&self) -> &[CommandProperty] {
        &self.properties
    }
}

pub trait MaterializedViewCommandConsumer: Send + Sync + 'static {
    fn execute(&self, command: MaterializedViewCommand, context: CommandContext) -> CommandFuture;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn administrative_rows_require_one_value_per_column() {
        let columns = vec![Arc::<str>::from("job"), Arc::<str>::from("state")];
        let rows = vec![vec![Some(Arc::<str>::from("one"))]];
        assert!(CommandRows::try_new(columns, rows).is_none());
    }
}
