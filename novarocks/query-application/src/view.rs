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

//! Query-application view contracts.
//!
//! Query application owns the SQL-facing view service and its immutable
//! vocabulary. Role adapters implement only the connector-backed metadata
//! operations required by external views.

use std::collections::HashMap;

use novarocks_parser::ast::{Query, ViewStatement};
use novarocks_spi::connector::{ConnectorRequestContext, DropPolicy};

use crate::api::QueryResult;
use crate::persisted_query_definition::PersistedQueryDefinition;

pub use crate::view_service::{LocalViewSnapshot, QueryViewService};

#[derive(Clone, Copy)]
pub struct ViewRequestContext<'a> {
    pub current_catalog: Option<&'a str>,
    pub current_database: &'a str,
    /// Query-owned context used by connector reads and external view mutations.
    pub connector_context: Option<&'a ConnectorRequestContext>,
}

#[derive(Clone, Debug)]
pub enum ViewStatementResult {
    Ok,
    Query(QueryResult),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ViewTarget {
    pub catalog: String,
    pub database: String,
    pub view: String,
}

pub use novarocks_sql::planning::catalog::SqlViewColumn as ViewColumnDefinition;

pub struct ViewDefinitionAnalysisRequest<'a> {
    pub catalog: &'a str,
    pub database: &'a str,
    pub target: &'a ViewTarget,
    pub query: &'a Query,
    pub columns: &'a [novarocks_parser::ast::Ident],
    pub local_views: &'a LocalViewSnapshot,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateExternalViewRequest {
    pub target: ViewTarget,
    pub columns: Vec<ViewColumnDefinition>,
    pub definition: PersistedQueryDefinition,
    pub comment: Option<String>,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub properties: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedExternalView {
    pub definition: PersistedQueryDefinition,
    pub column_names: Vec<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
}

/// Pure SHOW preparation, before any selected Provider is called.
/// Local names and external names share the same original result author.
pub enum ShowViewsSource {
    Local(Vec<String>),
    External { catalog: String, namespace: String },
}
pub struct PreparedShowViews {
    pub column_name: String,
    pub source: ShowViewsSource,
}
impl PreparedShowViews {
    pub fn finish(self, mut names: Vec<String>) -> Result<QueryResult, String> {
        if matches!(self.source, ShowViewsSource::External { .. }) {
            names.sort();
            names.dedup();
        }
        names.sort();
        crate::view_service::build_query_result(vec![(self.column_name, names)])
    }
}

/// Query-application-owned view command and declaration service.
pub trait ViewService: Send + Sync {
    fn catalog_snapshot(&self) -> Result<LocalViewSnapshot, String>;

    fn prepare_show_views(
        &self,
        statement: &novarocks_parser::ast::ShowViews,
        context: ViewRequestContext<'_>,
    ) -> Result<PreparedShowViews, String>;

    fn execute_statement(
        &self,
        engine: &dyn ViewEngine,
        statement: &ViewStatement,
        context: ViewRequestContext<'_>,
    ) -> Result<ViewStatementResult, crate::api::CommandError>;

    fn drop_database(&self, catalog: &str, database: &str) -> Result<(), String>;
}

/// Role-local external-view metadata and mutation adapter.
pub trait ViewEngine: Send + Sync {
    fn create_external_view(
        &self,
        request: CreateExternalViewRequest,
        context: &ConnectorRequestContext,
    ) -> Result<(), crate::api::CommandError>;
    fn drop_external_view(
        &self,
        target: &ViewTarget,
        context: &ConnectorRequestContext,
        policy: DropPolicy,
    ) -> Result<(), crate::api::CommandError>;
    fn load_external_view(
        &self,
        target: &ViewTarget,
        context: &ConnectorRequestContext,
    ) -> Result<Option<ResolvedExternalView>, crate::api::CommandError>;

    fn analyze_view_definition(
        &self,
        request: ViewDefinitionAnalysisRequest<'_>,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<ViewColumnDefinition>, novarocks_sql::compiler::SqlCompileError>;
}
