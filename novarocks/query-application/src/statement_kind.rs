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
    ShowProcessList,
    Analyze,
    ShowStats,
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
    CreateMaterializedView,
    DropMaterializedView,
    AlterMaterializedViewRefresh,
    AlterMaterializedViewProperties,
    PauseMaterializedViewRefresh,
    ResumeMaterializedViewRefresh,
    RepartitionMaterializedView,
    RefreshMaterializedView,
    ExplainMaterializedViewRefresh,
    CreateView,
    DropView,
    ShowCreateView,
    Insert,
    Delete,
    Update,
    Merge,
    AddEqualityDelete,
    SetSession,
    ResetSession,
    Use,
    KillQuery,
    KillConnection,
    BeginTransaction,
    Commit,
    Rollback,
    Savepoint,
}

impl StatementKind {
    /// True only for syntax families whose result is statically a row result.
    ///
    /// This is not proof of a completion carrier when false: CALL may produce
    /// rows. An adapter inspects the actual carrier; row output suppresses
    /// updateType except for Trino table procedures such as OPTIMIZE that
    /// explicitly report both. Empty has no updateType.
    pub const fn is_row_kind(self) -> bool {
        match self {
            Self::Query
            | Self::Explain
            | Self::ShowProcessList
            | Self::ShowStats
            | Self::ShowCreateTable
            | Self::ExplainMaterializedViewRefresh
            | Self::ShowCreateView => true,
            Self::Empty
            | Self::Analyze
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
            | Self::ResetSession
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
    use super::StatementKind;

    #[test]
    fn row_kinds_remain_distinct_from_command_result_shapes() {
        assert!(StatementKind::Query.is_row_kind());
        assert!(StatementKind::ShowStats.is_row_kind());
        assert!(!StatementKind::Call.is_row_kind());
        assert!(!StatementKind::AddFiles.is_row_kind());
    }
}
