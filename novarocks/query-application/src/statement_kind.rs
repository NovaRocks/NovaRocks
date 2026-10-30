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

//! Application-owned statement classification from admitted AST.
//!
//! Parser-admitted statement facts shared with client protocol projections.
//! No protocol task names, parser dispatch, or execution authority live here.

use novarocks_parser::ast;

/// The logical statement family, independent of its client protocol.
///
/// Classification does not authorize a statement or claim that execution
/// succeeded. Unsupported transaction statements still retain their real kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatementKind {
    /// The existing empty-input fast path has no AST statement.
    Empty,
    Query,
    Explain,
    ShowBackends,
    ShowProcessList,
    Analyze,
    ShowAnalyzeJobs,
    CancelAnalyze,
    ShowTableStats,
    ShowBasicStatsMeta,
    ShowHistogramStatsMeta,
    DropStats,
    DropHistogram,
    DropMultipleColumnsStats,
    CreateCatalog,
    DropCatalog,
    CreateDatabase,
    DropDatabase,
    CreateTable,
    DropTable,
    TruncateTable,
    ShowCreateTable,
    AddColumn,
    DropColumn,
    RenameColumn,
    AlterColumnType,
    ReorderColumn,
    SetColumnNotNull,
    DropColumnNotNull,
    CommentColumn,
    SetTableProperties,
    UnsetTableProperties,
    CommentTable,
    AddPartitionField,
    DropPartitionField,
    CreateBranch,
    CreateTag,
    DropBranch,
    DropTag,
    AddFiles,
    Call,
    Optimize,
    RewriteManifests,
    ExpireSnapshots,
    RemoveOrphanFiles,
    ShowOptimize,
    CreateMaterializedView,
    DropMaterializedView,
    AlterMaterializedViewRefresh,
    AlterMaterializedViewProperties,
    PauseMaterializedViewRefresh,
    ResumeMaterializedViewRefresh,
    RepartitionMaterializedView,
    RefreshMaterializedView,
    ShowMaterializedViews,
    ExplainMaterializedViewRefresh,
    CreateView,
    DropView,
    ShowViews,
    ShowCreateView,
    Insert,
    Delete,
    Update,
    Merge,
    AddEqualityDelete,
    SetSession,
    Use,
    KillQuery,
    KillConnection,
    BeginTransaction,
    Commit,
    Rollback,
    Savepoint,
}

impl StatementKind {
    /// Classify the syntax already accepted by the application's parser owner.
    /// There is deliberately no SQL-string dispatch and no catch-all kind.
    pub fn from_statement(statement: &ast::Statement) -> Self {
        match statement {
            ast::Statement::ShowBackends(_) => Self::ShowBackends,
            ast::Statement::ShowProcessList(_) => Self::ShowProcessList,
            ast::Statement::Statistics(statement) => match statement {
                ast::StatisticsStatement::AnalyzeTable(_) => Self::Analyze,
                ast::StatisticsStatement::ShowAnalyzeJobs(_) => Self::ShowAnalyzeJobs,
                ast::StatisticsStatement::CancelAnalyze(_) => Self::CancelAnalyze,
                ast::StatisticsStatement::ShowTableStats(_) => Self::ShowTableStats,
                ast::StatisticsStatement::ShowBasicStatsMeta(_) => Self::ShowBasicStatsMeta,
                ast::StatisticsStatement::ShowHistogramStatsMeta(_) => Self::ShowHistogramStatsMeta,
                ast::StatisticsStatement::DropStats(_) => Self::DropStats,
                ast::StatisticsStatement::DropHistogram(_) => Self::DropHistogram,
                ast::StatisticsStatement::DropMultipleColumnsStats(_) => {
                    Self::DropMultipleColumnsStats
                }
            },
            ast::Statement::Catalog(statement) => match statement {
                ast::CatalogStatement::TruncateTable(_) => Self::TruncateTable,
                ast::CatalogStatement::CreateCatalog(_) => Self::CreateCatalog,
                ast::CatalogStatement::DropCatalog(_) => Self::DropCatalog,
                ast::CatalogStatement::CreateDatabase(_) => Self::CreateDatabase,
                ast::CatalogStatement::DropDatabase(_) => Self::DropDatabase,
                ast::CatalogStatement::DropTable(_) => Self::DropTable,
                ast::CatalogStatement::ShowCreateTable(_) => Self::ShowCreateTable,
            },
            ast::Statement::Iceberg(statement) => match statement {
                ast::IcebergStatement::AlterTable(statement) => match &statement.action {
                    ast::IcebergTableAction::Schema(change) => match change {
                        ast::IcebergSchemaChange::AddColumn { .. } => Self::AddColumn,
                        ast::IcebergSchemaChange::DropColumn { .. } => Self::DropColumn,
                        ast::IcebergSchemaChange::RenameColumn { .. } => Self::RenameColumn,
                        ast::IcebergSchemaChange::ModifyColumn { .. } => Self::AlterColumnType,
                        ast::IcebergSchemaChange::AlterColumn { action, .. } => match action {
                            ast::IcebergColumnAction::Reorder(position) => match position {
                                ast::ColumnPosition::Default
                                | ast::ColumnPosition::First
                                | ast::ColumnPosition::After(_)
                                | ast::ColumnPosition::Before(_) => Self::ReorderColumn,
                            },
                            ast::IcebergColumnAction::SetNullable(nullable) => {
                                if *nullable {
                                    Self::DropColumnNotNull
                                } else {
                                    Self::SetColumnNotNull
                                }
                            }
                            ast::IcebergColumnAction::Comment(_) => Self::CommentColumn,
                        },
                    },
                    ast::IcebergTableAction::Properties(action) => match action {
                        ast::IcebergPropertiesAction::Set { .. } => Self::SetTableProperties,
                        ast::IcebergPropertiesAction::Unset { .. } => Self::UnsetTableProperties,
                        ast::IcebergPropertiesAction::Comment { .. } => Self::CommentTable,
                    },
                    ast::IcebergTableAction::Partition(change) => match change {
                        ast::IcebergPartitionChange::Add { .. } => Self::AddPartitionField,
                        ast::IcebergPartitionChange::Drop { .. } => Self::DropPartitionField,
                    },
                    ast::IcebergTableAction::Reference(action) => match action {
                        ast::IcebergReferenceAction::Create { kind, .. } => match kind {
                            ast::IcebergReferenceKind::Branch => Self::CreateBranch,
                            ast::IcebergReferenceKind::Tag => Self::CreateTag,
                        },
                        ast::IcebergReferenceAction::Drop { kind, .. } => match kind {
                            ast::IcebergReferenceKind::Branch => Self::DropBranch,
                            ast::IcebergReferenceKind::Tag => Self::DropTag,
                        },
                    },
                    ast::IcebergTableAction::AddFiles(_) => Self::AddFiles,
                },
            },
            ast::Statement::Maintenance(statement) => match statement {
                ast::MaintenanceStatement::Call(_) => Self::Call,
                ast::MaintenanceStatement::Optimize(_) => Self::Optimize,
                ast::MaintenanceStatement::RewriteManifests(_) => Self::RewriteManifests,
                ast::MaintenanceStatement::ExpireSnapshots(_) => Self::ExpireSnapshots,
                ast::MaintenanceStatement::RemoveOrphanFiles(_) => Self::RemoveOrphanFiles,
                ast::MaintenanceStatement::ShowOptimize(_) => Self::ShowOptimize,
            },
            ast::Statement::MaterializedView(statement) => match statement {
                ast::MaterializedViewStatement::Create(_) => Self::CreateMaterializedView,
                ast::MaterializedViewStatement::Drop(_) => Self::DropMaterializedView,
                ast::MaterializedViewStatement::Alter(statement) => match &statement.action {
                    ast::MaterializedViewAlterAction::SetRefresh(policy) => match policy {
                        ast::MaterializedViewRefreshPolicy::Immediate
                        | ast::MaterializedViewRefreshPolicy::Manual { .. }
                        | ast::MaterializedViewRefreshPolicy::AsyncOnChange { .. }
                        | ast::MaterializedViewRefreshPolicy::AsyncEvery { .. } => {
                            Self::AlterMaterializedViewRefresh
                        }
                    },
                    ast::MaterializedViewAlterAction::SetProperties(_) => {
                        Self::AlterMaterializedViewProperties
                    }
                    ast::MaterializedViewAlterAction::PauseRefresh => {
                        Self::PauseMaterializedViewRefresh
                    }
                    ast::MaterializedViewAlterAction::ResumeRefresh => {
                        Self::ResumeMaterializedViewRefresh
                    }
                    ast::MaterializedViewAlterAction::Repartition(_) => {
                        Self::RepartitionMaterializedView
                    }
                },
                ast::MaterializedViewStatement::Refresh(_) => Self::RefreshMaterializedView,
                ast::MaterializedViewStatement::Show(_) => Self::ShowMaterializedViews,
                ast::MaterializedViewStatement::ExplainRefresh(_) => {
                    Self::ExplainMaterializedViewRefresh
                }
            },
            ast::Statement::View(statement) => match statement {
                ast::ViewStatement::Create(_) => Self::CreateView,
                ast::ViewStatement::Drop(_) => Self::DropView,
                ast::ViewStatement::Show(_) => Self::ShowViews,
                ast::ViewStatement::ShowCreate(_) => Self::ShowCreateView,
            },
            ast::Statement::Table(statement) => match statement {
                ast::TableStatement::Create(_) => Self::CreateTable,
            },
            ast::Statement::Dml(statement) => match statement {
                // CTAS is the same client-visible table-creation operation.
                ast::DmlStatement::CreateTableAsSelect(_) => Self::CreateTable,
                ast::DmlStatement::Insert(_) => Self::Insert,
                ast::DmlStatement::Delete(_) => Self::Delete,
                ast::DmlStatement::Update(_) => Self::Update,
                ast::DmlStatement::Merge(_) => Self::Merge,
                ast::DmlStatement::AddEqualityDelete(_) => Self::AddEqualityDelete,
            },
            ast::Statement::Session(statement) => match statement {
                ast::SessionStatement::Set(_) => Self::SetSession,
                ast::SessionStatement::Use(_) => Self::Use,
                ast::SessionStatement::Kill(statement) => match statement.kind {
                    ast::KillKind::Default | ast::KillKind::Connection => Self::KillConnection,
                    ast::KillKind::Query => Self::KillQuery,
                },
                ast::SessionStatement::TransactionControl(statement) => match statement.kind {
                    ast::TransactionControlKind::Begin
                    | ast::TransactionControlKind::StartTransaction => Self::BeginTransaction,
                    ast::TransactionControlKind::Commit => Self::Commit,
                    ast::TransactionControlKind::Rollback => Self::Rollback,
                    ast::TransactionControlKind::Savepoint => Self::Savepoint,
                },
            },
            ast::Statement::Query(_) => Self::Query,
            ast::Statement::ExplainQuery(_) => Self::Explain,
        }
    }

    /// True only for syntax families whose result is statically a row result.
    ///
    /// This is not proof of a completion carrier when false: CALL may produce
    /// rows. An adapter must inspect the actual carrier and suppress updateType
    /// for row output even if this method is false. Empty has no updateType.
    pub const fn is_row_kind(self) -> bool {
        match self {
            Self::Query
            | Self::Explain
            | Self::ShowBackends
            | Self::ShowProcessList
            | Self::ShowAnalyzeJobs
            | Self::ShowTableStats
            | Self::ShowBasicStatsMeta
            | Self::ShowHistogramStatsMeta
            | Self::ShowCreateTable
            | Self::ShowOptimize
            | Self::ShowMaterializedViews
            | Self::ExplainMaterializedViewRefresh
            | Self::ShowViews
            | Self::ShowCreateView => true,
            Self::Empty
            | Self::Analyze
            | Self::CancelAnalyze
            | Self::DropStats
            | Self::DropHistogram
            | Self::DropMultipleColumnsStats
            | Self::CreateCatalog
            | Self::DropCatalog
            | Self::CreateDatabase
            | Self::DropDatabase
            | Self::CreateTable
            | Self::DropTable
            | Self::TruncateTable
            | Self::AddColumn
            | Self::DropColumn
            | Self::RenameColumn
            | Self::AlterColumnType
            | Self::ReorderColumn
            | Self::SetColumnNotNull
            | Self::DropColumnNotNull
            | Self::CommentColumn
            | Self::SetTableProperties
            | Self::UnsetTableProperties
            | Self::CommentTable
            | Self::AddPartitionField
            | Self::DropPartitionField
            | Self::CreateBranch
            | Self::CreateTag
            | Self::DropBranch
            | Self::DropTag
            | Self::AddFiles
            | Self::Call
            | Self::Optimize
            | Self::RewriteManifests
            | Self::ExpireSnapshots
            | Self::RemoveOrphanFiles
            | Self::CreateMaterializedView
            | Self::DropMaterializedView
            | Self::AlterMaterializedViewRefresh
            | Self::AlterMaterializedViewProperties
            | Self::PauseMaterializedViewRefresh
            | Self::ResumeMaterializedViewRefresh
            | Self::RepartitionMaterializedView
            | Self::RefreshMaterializedView
            | Self::CreateView
            | Self::DropView
            | Self::Insert
            | Self::Delete
            | Self::Update
            | Self::Merge
            | Self::AddEqualityDelete
            | Self::SetSession
            | Self::Use
            | Self::KillQuery
            | Self::KillConnection
            | Self::BeginTransaction
            | Self::Commit
            | Self::Rollback
            | Self::Savepoint => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed_kind(source: &str) -> StatementKind {
        let statements = novarocks_parser::parse(source).expect("representative statement parses");
        let [statement] = statements.as_slice() else {
            panic!("expected exactly one actual AST statement");
        };
        StatementKind::from_statement(statement)
    }

    #[test]
    fn query_and_show_are_row_kinds_without_string_dispatch() {
        assert_eq!(parsed_kind("SELECT 1"), StatementKind::Query);
        assert!(parsed_kind("SELECT 1").is_row_kind());
        assert_eq!(
            parsed_kind("SHOW PROCESSLIST"),
            StatementKind::ShowProcessList
        );
        assert!(parsed_kind("SHOW PROCESSLIST").is_row_kind());
        assert!(!StatementKind::Call.is_row_kind());
    }

    #[test]
    fn session_control_retains_the_actual_default_kill_semantics() {
        assert_eq!(
            parsed_kind("SET pipeline_dop = 2"),
            StatementKind::SetSession
        );
        assert_eq!(parsed_kind("USE `catalog`.`analytics`"), StatementKind::Use);
        assert_eq!(parsed_kind("KILL QUERY 2"), StatementKind::KillQuery);
        assert_eq!(parsed_kind("KILL 1"), StatementKind::KillConnection);
        assert_eq!(
            parsed_kind("KILL CONNECTION 3"),
            StatementKind::KillConnection
        );
    }

    #[test]
    fn real_ddl_corpus_keeps_distinct_operation_kinds() {
        // These inputs are existing parser corpus, not synthetic AST objects.
        assert_eq!(
            parsed_kind(
                "ALTER TABLE ice.db.orders ADD COLUMN address.zip INT DEFAULT 94107 AFTER address.city"
            ),
            StatementKind::AddColumn
        );
        assert_eq!(
            parsed_kind(
                "ALTER TABLE ice.db.orders SET TBLPROPERTIES ('write.format.default' = 'parquet')"
            ),
            StatementKind::SetTableProperties
        );
        assert_eq!(
            parsed_kind("ALTER MATERIALIZED VIEW analytics.mv PAUSE REFRESH"),
            StatementKind::PauseMaterializedViewRefresh
        );
    }
}
