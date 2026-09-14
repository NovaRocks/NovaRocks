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

//! Closed, spanless semantic values for SQL product commands.
//!
//! Parser AST nodes are intentionally absent from this module.  Query
//! Application lowers one admitted syntax node to one of these values before
//! selecting a role-local product adapter.  The values retain syntax facts
//! which are still meaningful to a product (for example `FORCE`, creation
//! policy, and maintenance options), but never retain a source span or a SQL
//! fragment that would invite reparsing at the application boundary.

use super::{ColumnAggregation, IcebergPartitionFieldExpr, ObjectName, TableKeyDesc};
use novarocks_types::schema::SqlType;

#[derive(Clone, Debug, PartialEq)]
pub enum CatalogSqlCommand {
    TruncateTable {
        table: ObjectName,
        target_ref: String,
    },
    CreateCatalog(CatalogCreateCommand),
    DropCatalog {
        name: String,
        if_exists: bool,
    },
    CreateDatabase {
        name: ObjectName,
        if_not_exists: bool,
    },
    DropDatabase {
        name: ObjectName,
        if_exists: bool,
        force: bool,
    },
    DropTable {
        name: ObjectName,
        if_exists: bool,
        force: bool,
    },
    ShowCreateTable {
        name: ObjectName,
    },
    CreateTable(CreateTableSqlCommand),
    AlterIcebergTable(AlterIcebergTableSqlCommand),
}

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogCreateCommand {
    pub external: bool,
    pub if_not_exists: bool,
    pub name: String,
    pub comment: Option<CommandLiteral>,
    pub properties: Vec<CommandProperty>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommandProperty {
    pub key: CommandLiteral,
    pub value: CommandLiteral,
}

/// A literal value retained by administrative command syntax.
///
/// `Number` is a lexical numeric value, not a SQL fragment: it preserves
/// decimal precision and scale until the owning product validates the target
/// type.  It cannot contain punctuation accepted by a general SQL expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandLiteral {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Binary(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateTableSqlCommand {
    pub temporary: bool,
    pub external: bool,
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub engine: Option<String>,
    pub like: Option<ObjectName>,
    pub columns: Vec<TableColumnSqlCommand>,
    pub key: Option<TableKeyDesc>,
    pub distribution: Option<TableDistributionSqlCommand>,
    pub partition: Option<TablePartitionSqlCommand>,
    pub order_by: Vec<String>,
    pub properties: Vec<CommandProperty>,
    pub comment: Option<CommandLiteral>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableColumnSqlCommand {
    pub name: String,
    pub data_type: SqlType,
    pub nullable: Option<bool>,
    pub aggregation: Option<ColumnAggregation>,
    pub default: Option<CommandLiteral>,
    pub comment: Option<CommandLiteral>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableDistributionSqlCommand {
    pub columns: Vec<String>,
    pub random: bool,
    pub buckets: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TablePartitionSqlCommand {
    Transform(Vec<IcebergPartitionFieldExpr>),
    /// The parser accepts a legacy range declaration, but no current product
    /// adapter supports it.  Its individual SQL ranges are deliberately not
    /// carried across this semantic boundary.
    UnsupportedLegacyRange {
        columns: Vec<String>,
        definition_count: usize,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct AlterIcebergTableSqlCommand {
    pub table: ObjectName,
    pub action: IcebergTableSqlAction,
}

#[derive(Clone, Debug, PartialEq)]
pub enum IcebergTableSqlAction {
    Schema(IcebergSchemaSqlChange),
    Properties(IcebergPropertiesSqlAction),
    Partition(IcebergPartitionSqlChange),
    Reference(IcebergReferenceSqlAction),
    AddFiles { location: CommandLiteral },
}

#[derive(Clone, Debug, PartialEq)]
pub enum IcebergSchemaSqlChange {
    AddColumn {
        path: Vec<String>,
        data_type: SqlType,
        nullable: Option<bool>,
        default: Option<CommandLiteral>,
        position: ColumnPositionSql,
    },
    DropColumn {
        path: Vec<String>,
    },
    RenameColumn {
        from: Vec<String>,
        to: Vec<String>,
    },
    ModifyColumn {
        path: Vec<String>,
        data_type: SqlType,
    },
    AlterColumn {
        path: Vec<String>,
        action: IcebergColumnSqlAction,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnPositionSql {
    Default,
    First,
    After(Vec<String>),
    Before(Vec<String>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum IcebergColumnSqlAction {
    Reorder(ColumnPositionSql),
    SetNullable(bool),
    Comment(CommandLiteral),
}

#[derive(Clone, Debug, PartialEq)]
pub enum IcebergPropertiesSqlAction {
    Set {
        entries: Vec<(String, CommandLiteral)>,
    },
    Unset {
        keys: Vec<String>,
        if_exists: bool,
    },
    Comment {
        value: CommandLiteral,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergPartitionSqlChange {
    Add(IcebergPartitionFieldExpr),
    Drop(IcebergPartitionFieldExpr),
}

#[derive(Clone, Debug, PartialEq)]
pub enum IcebergReferenceSqlAction {
    Create {
        kind: IcebergReferenceKindSql,
        name: String,
        if_not_exists: bool,
        or_replace: bool,
        anchor: ReferenceAnchorSql,
        /// Current product code ignores parser-retained provider extension
        /// tokens. Keep that fact without passing raw SQL into an adapter.
        has_uninterpreted_provider_options: bool,
    },
    Drop {
        kind: IcebergReferenceKindSql,
        name: String,
        if_exists: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergReferenceKindSql {
    Branch,
    Tag,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReferenceAnchorSql {
    CurrentMain,
    Version(CommandLiteral),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatisticsSqlCommand {
    AnalyzeTable {
        mode: AnalyzeModeSql,
        table: ObjectName,
        columns: Vec<String>,
        with_sync_mode: bool,
    },
    ShowAnalyzeJobs,
    CancelAnalyze {
        job_id: String,
    },
    ShowTableStats {
        table: ObjectName,
    },
    ShowBasicStatsMeta,
    ShowHistogramStatsMeta,
    DropStats {
        table: ObjectName,
    },
    DropHistogram {
        table: ObjectName,
        columns: Vec<String>,
    },
    DropMultipleColumnsStats {
        table: ObjectName,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnalyzeModeSql {
    Default,
    Full,
    Sample,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MaintenanceSqlCommand {
    Call {
        procedure: ObjectName,
        arguments: Vec<ProcedureArgumentSql>,
        argument_mode: ProcedureArgumentModeSql,
    },
    Optimize {
        table: ObjectName,
    },
    RewriteManifests {
        table: ObjectName,
    },
    ExpireSnapshots {
        table: ObjectName,
        options: Vec<ExpireSnapshotsOptionSql>,
    },
    RemoveOrphanFiles {
        table: ObjectName,
        older_than: MaintenanceValueSql,
    },
    ShowOptimize(ShowOptimizeSqlCommand),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcedureArgumentSql {
    pub name: Option<String>,
    pub value: MaintenanceValueSql,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcedureArgumentModeSql {
    Empty,
    Positional,
    Named,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MaintenanceValueSql {
    Literal(CommandLiteral),
    Timestamp(CommandLiteral),
    Map(Vec<(CommandLiteral, CommandLiteral)>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExpireSnapshotsOptionSql {
    OlderThan(MaintenanceValueSql),
    RetainLast(MaintenanceValueSql),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowOptimizeSqlCommand {
    pub from: Option<ObjectName>,
    pub filter: Option<ShowOptimizeFilterSql>,
    pub order_by: Option<ShowOptimizeOrderSql>,
    pub limit: Option<CommandLiteral>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowOptimizeFilterSql {
    pub column: String,
    pub value: CommandLiteral,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowOptimizeOrderSql {
    pub column: String,
    pub direction: Option<SortDirectionSql>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirectionSql {
    Asc,
    Desc,
}
