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

//! Temporary old-parser facts. Removed with their original lowering at cutover.
use super::{Expr, Ident};
use crate::Span;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinOperator {
    Inner,
    InnerExplicit,
    LeftOuter,
    LeftOuterExplicit,
    RightOuter,
    RightOuterExplicit,
    FullOuter,
    FullOuterExplicit,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableFunctionSyntax {
    TableWrapper,
    BareCall,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableFunction {
    pub expr: Expr,
    pub lateral: bool,
    pub syntax: TableFunctionSyntax,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableAlias {
    pub name: Ident,
    pub columns: Vec<Ident>,
    pub explicit_as: bool,
    pub span: Span,
}

pub(crate) mod catalog;
pub(crate) mod command;
pub(crate) mod dml;
mod explain;
pub(crate) mod iceberg;
pub(crate) mod maintenance;
pub(crate) mod materialized_view;
pub(crate) mod session;
mod statement;
pub(crate) mod statistics;
pub(crate) mod table;
pub(crate) mod view;

pub use catalog::{
    CatalogProperty, CatalogStatement, CreateCatalog, CreateDatabase, DropCatalog, DropDatabase,
    DropTable, ShowCreateTable, TruncateTable,
};
pub use command::{Property, PropertyKeyValue};
pub use dml::{
    AddEqualityDelete, Assignment, CreateTableAsSelect, Delete, DmlStatement, Insert,
    InsertPartitionEntry, InsertPartitions, Merge, MergeClause, MergeMatchedAction,
    MergeNotMatchedAction, MutationSource, Update,
};
pub use explain::{ExplainFormat, ExplainQuery};
pub use iceberg::{
    AddFiles, AlterIcebergTable, ColumnPath, ColumnPosition, IcebergColumnAction,
    IcebergPartitionChange, IcebergPartitionField, IcebergPropertiesAction, IcebergReferenceAction,
    IcebergReferenceKind, IcebergSchemaChange, IcebergStatement, IcebergTableAction,
    RawReferenceOptions, ReferenceAnchor,
};
pub use maintenance::{
    CallStatement, ExpireSnapshots, ExpireSnapshotsOption, MaintenanceStatement, MaintenanceValue,
    OptimizeTable, ProcedureArgument, ProcedureArgumentMode, ProcedureMap, ProcedureMapEntry,
    RemoveOrphanFiles, RewriteManifests,
};
pub use materialized_view::{
    AlterMaterializedView, CreateMaterializedView, DropMaterializedView,
    ExplainRefreshMaterializedView, MaterializedViewAlterAction, MaterializedViewDistribution,
    MaterializedViewExplainLevel, MaterializedViewPartitionArgument,
    MaterializedViewPartitionField, MaterializedViewProperty, MaterializedViewRefreshMode,
    MaterializedViewRefreshPolicy, MaterializedViewStatement, RefreshMaterializedView,
};
pub use session::ShowProcessList;
pub use session::{
    KillKind, KillStatement, SessionStatement, SetAssignment, SetScope, SetStatement,
    TransactionControlKind, TransactionControlStatement, UseStatement,
};
pub use statement::Statement;
pub use statistics::{AnalyzeTable, ShowStats, StatisticsStatement};
pub use table::{
    ColumnDefinition, CreateTable, LegacyRangePartition, LegacyRangePartitionDefinition,
    PartitionTransform, TableDistribution, TableKey, TableKeyKind, TablePartition,
    TablePartitionTransform, TableProperty, TableStatement,
};
pub use view::{CreateView, DropView, ShowCreateView, ViewStatement};
