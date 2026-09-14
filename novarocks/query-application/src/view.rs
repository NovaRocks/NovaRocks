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

use novarocks_parser::ast::{Query, TypeName, ViewStatement};
use novarocks_spi::connector::{ConnectorRequestContext, DropPolicy};

use crate::api::QueryResult;
use crate::persisted_query_definition::PersistedQueryDefinition;

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

#[derive(Clone, Debug, PartialEq)]
pub struct ViewColumnDefinition {
    pub name: String,
    pub data_type: TypeName,
    pub nullable: bool,
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

#[derive(Clone, Debug, PartialEq)]
pub enum ExternalViewResolution {
    Table,
    View(ResolvedExternalView),
    Missing,
}

/// Query-application-owned view command and rewrite service.
pub trait ViewService: Send + Sync {
    fn execute_statement(
        &self,
        engine: &dyn ViewEngine,
        statement: &ViewStatement,
        context: ViewRequestContext<'_>,
    ) -> Result<ViewStatementResult, String>;

    fn rewrite_query(
        &self,
        engine: &dyn ViewEngine,
        query: &mut Query,
        context: ViewRequestContext<'_>,
    ) -> Result<(), String>;

    fn drop_database(&self, catalog: &str, database: &str) -> Result<(), String>;
}

/// Role-local external-view metadata and mutation adapter.
pub trait ViewEngine: Send + Sync {
    /// Resolve a table-or-view name through exactly one connector control
    /// generation. Missing view metadata is not equivalent to an undeclared
    /// view capability; the latter remains a typed Unsupported error.
    fn resolve_external_view(
        &self,
        target: &ViewTarget,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalViewResolution, String>;
    fn create_external_view(
        &self,
        request: CreateExternalViewRequest,
        context: &ConnectorRequestContext,
    ) -> Result<(), String>;
    fn drop_external_view(
        &self,
        target: &ViewTarget,
        context: &ConnectorRequestContext,
        policy: DropPolicy,
    ) -> Result<(), String>;
    fn load_external_view(
        &self,
        target: &ViewTarget,
        context: &ConnectorRequestContext,
    ) -> Result<Option<ResolvedExternalView>, String>;
    fn list_external_views(
        &self,
        catalog: &str,
        database: &str,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, String>;
    fn analyze_external_view(
        &self,
        catalog: &str,
        database: &str,
        query: &Query,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<ViewColumnDefinition>, String>;
}
