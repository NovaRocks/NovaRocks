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

//! Old-parser top-level statement vocabulary.

use super::{
    CatalogStatement, DmlStatement, ExplainQuery, IcebergStatement, MaintenanceStatement,
    MaterializedViewStatement, SessionStatement, ShowProcessList, StatisticsStatement,
    TableStatement, ViewStatement,
};
use crate::{Span, ast::Query};

/// A top-level SQL statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Statement {
    ShowProcessList(ShowProcessList),
    Statistics(StatisticsStatement),
    Catalog(CatalogStatement),
    Iceberg(IcebergStatement),
    Maintenance(MaintenanceStatement),
    MaterializedView(MaterializedViewStatement),
    View(ViewStatement),
    Table(TableStatement),
    Dml(DmlStatement),
    Session(SessionStatement),
    Query(Query),
    ExplainQuery(ExplainQuery),
}

impl Statement {
    pub const fn span(&self) -> Span {
        match self {
            Self::ShowProcessList(statement) => statement.span,
            Self::Statistics(statement) => statement.span(),
            Self::Catalog(statement) => statement.span(),
            Self::Iceberg(statement) => statement.span(),
            Self::Maintenance(statement) => statement.span(),
            Self::MaterializedView(statement) => statement.span(),
            Self::View(statement) => statement.span(),
            Self::Table(statement) => statement.span(),
            Self::Dml(statement) => statement.span(),
            Self::Session(statement) => statement.span(),
            Self::Query(query) => query.span,
            Self::ExplainQuery(query) => query.span,
        }
    }
}
