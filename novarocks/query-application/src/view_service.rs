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

//! Query-application-owned view DDL, metadata, and query rewrite service.

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};

use crate::api::{QueryResult, build_utf8_query_result};
use crate::persisted_query_definition::{PersistedQueryDefinition, PersistedQueryDialect};
use crate::view::{ViewEngine, ViewRequestContext, ViewService, ViewStatementResult};
use novarocks_parser::{
    ast::{CreateView, Query, Statement, ViewStatement},
    printer,
};
use novarocks_types::naming::normalize_identifier;

pub(crate) const DEFAULT_CATALOG: &str = "default_catalog";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct SessionViewKey {
    pub(crate) catalog: String,
    pub(crate) database: String,
    pub(crate) view: String,
}

#[derive(Clone, Debug)]
pub(crate) struct StoredView {
    pub(crate) definition: PersistedQueryDefinition,
    pub(crate) query: Box<Query>,
}

/// Local (non-external catalog) views, for as long as this query application
/// runs.
///
/// The in-process registry is the whole authority. Local views are the
/// process-runtime family: no external system defines them, so no external
/// system could rebuild them. Their lifetime ends with the hosting application
/// process rather than making a role adapter a durable metadata authority.
/// The family therefore owns no persistent key prefix, and a restart starts
/// from an empty registry.
///
/// That is a capability ceiling, not a rejection: `CREATE VIEW`, `DROP VIEW`
/// and `SHOW VIEWS` behave as before within one process. A deployment that
/// needs durable views defines them in an external catalog, whose provider owns
/// them as its own truth.
pub struct QueryViewService {
    registry: RwLock<HashMap<SessionViewKey, StoredView>>,
    /// Serialises view DDL against other view DDL, so that a create and a drop
    /// of the same name keep statement order instead of interleaving their
    /// check-then-write halves.
    mutation: Mutex<()>,
}

impl QueryViewService {
    pub fn new() -> Self {
        Self {
            registry: RwLock::new(HashMap::new()),
            mutation: Mutex::new(()),
        }
    }
}

impl Default for QueryViewService {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryViewService {
    fn create_session_view(
        &self,
        key: SessionViewKey,
        definition: PersistedQueryDefinition,
        query: Box<Query>,
        or_replace: bool,
    ) -> Result<(), String> {
        let _mutation = self
            .mutation
            .lock()
            .map_err(|error| format!("view mutation lock: {error}"))?;
        let mut registry = self
            .registry
            .write()
            .map_err(|error| format!("view registry write lock: {error}"))?;
        if registry.contains_key(&key) && !or_replace {
            return Err(format!(
                "view already exists: {}.{}",
                key.database, key.view
            ));
        }
        registry.insert(key, StoredView { definition, query });
        Ok(())
    }

    fn drop_session_view(&self, key: &SessionViewKey) -> Result<(), String> {
        let _mutation = self
            .mutation
            .lock()
            .map_err(|error| format!("view mutation lock: {error}"))?;
        self.registry
            .write()
            .map_err(|error| format!("view registry write lock: {error}"))?
            .remove(key);
        Ok(())
    }

    fn handle_create(
        &self,
        engine: &dyn ViewEngine,
        create_view: &CreateView,
        context: ViewRequestContext<'_>,
    ) -> Result<ViewStatementResult, String> {
        let parts = view_object_name_parts(&create_view.name);
        if let Some(target) =
            crate::view_iceberg::resolve_external_target_parts(engine, &parts, context)?
        {
            return crate::view_iceberg::create_external_view(engine, target, create_view, context);
        }
        let key = session_view_key(&create_view.name, context.current_database)?;
        let definition = query_definition(&printer::print_query(&create_view.query), context)?;
        let query = parse_query(&definition.raw_query_source)?;
        self.create_session_view(key, definition, query, create_view.or_replace)?;
        Ok(ViewStatementResult::Ok)
    }

    fn handle_drop(
        &self,
        engine: &dyn ViewEngine,
        drop_view: &novarocks_parser::ast::DropView,
        context: ViewRequestContext<'_>,
    ) -> Result<ViewStatementResult, String> {
        let parts = view_object_name_parts(&drop_view.name);
        if let Some(target) =
            crate::view_iceberg::resolve_external_target_parts(engine, &parts, context)?
        {
            crate::view_iceberg::drop_external_view(engine, &target, drop_view.if_exists, context)?;
        } else {
            self.drop_session_view(&session_view_key(
                &drop_view.name,
                context.current_database,
            )?)?;
        }
        Ok(ViewStatementResult::Ok)
    }

    fn handle_show_views(
        &self,
        engine: &dyn ViewEngine,
        show_views: &novarocks_parser::ast::ShowViews,
        context: ViewRequestContext<'_>,
    ) -> Result<ViewStatementResult, String> {
        let database = show_views
            .database
            .as_ref()
            .and_then(|database| view_object_name_parts(database).last().cloned())
            .unwrap_or_else(|| context.current_database.to_string());
        let normalized_database = normalize_identifier(&database)?;
        let active_external_catalog = context
            .current_catalog
            .filter(|catalog| !catalog.eq_ignore_ascii_case(DEFAULT_CATALOG));
        let mut names = match active_external_catalog {
            Some(catalog) => {
                let catalog = normalize_identifier(catalog)?;
                let connector_context = context.connector_context.ok_or_else(|| {
                    "SHOW VIEWS for an external catalog requires connector request context"
                        .to_string()
                })?;
                engine.list_external_views(&catalog, &normalized_database, connector_context)?
            }
            None => self
                .registry
                .read()
                .map_err(|error| format!("view registry read lock: {error}"))?
                .keys()
                .filter(|key| key.catalog == DEFAULT_CATALOG && key.database == normalized_database)
                .map(|key| key.view.clone())
                .collect(),
        };
        names.sort();
        Ok(ViewStatementResult::Query(build_string_result(
            &format!("Views_in_{database}"),
            names,
        )?))
    }

    fn registry_snapshot(&self) -> Result<HashMap<SessionViewKey, StoredView>, String> {
        self.registry
            .read()
            .map_err(|error| format!("view registry read lock: {error}"))
            .map(|registry| registry.clone())
    }
}

impl ViewService for QueryViewService {
    fn execute_statement(
        &self,
        engine: &dyn ViewEngine,
        statement: &ViewStatement,
        context: ViewRequestContext<'_>,
    ) -> Result<ViewStatementResult, String> {
        match statement {
            ViewStatement::Create(create_view) => self.handle_create(engine, create_view, context),
            ViewStatement::Drop(drop_view) => self.handle_drop(engine, drop_view, context),
            ViewStatement::Show(show_views) => self.handle_show_views(engine, show_views, context),
            ViewStatement::ShowCreate(show_create) => {
                crate::view_iceberg::show_create_view(engine, &show_create.name, context)
            }
        }
    }

    fn rewrite_query(
        &self,
        engine: &dyn ViewEngine,
        query: &mut Query,
        context: ViewRequestContext<'_>,
    ) -> Result<(), String> {
        let registry = self.registry_snapshot()?;
        crate::view_rewrite::expand_session_views(query, &registry, context.current_database);
        crate::view_rewrite::expand_external_views(engine, query, context)
    }

    fn drop_database(&self, catalog: &str, database: &str) -> Result<(), String> {
        if !catalog.eq_ignore_ascii_case(DEFAULT_CATALOG) {
            return Ok(());
        }
        let catalog = normalize_identifier(catalog)?;
        let database = normalize_identifier(database)?;
        let _mutation = self
            .mutation
            .lock()
            .map_err(|error| format!("view mutation lock: {error}"))?;
        self.registry
            .write()
            .map_err(|error| format!("view registry write lock: {error}"))?
            .retain(|key, _| key.catalog != catalog || key.database != database);
        Ok(())
    }
}

pub(crate) fn parse_query(sql: &str) -> Result<Box<Query>, String> {
    let statements =
        novarocks_parser::parse(sql).map_err(|error| format!("query parse failed: {error}"))?;
    match statements.as_slice() {
        [Statement::Query(query)] => Ok(Box::new(query.clone())),
        _ => Err("view SQL must contain exactly one query statement".to_string()),
    }
}

fn view_object_name_parts(name: &novarocks_parser::ast::ObjectName) -> Vec<String> {
    name.parts.iter().map(|part| part.value.clone()).collect()
}

fn session_view_key(
    name: &novarocks_parser::ast::ObjectName,
    current_database: &str,
) -> Result<SessionViewKey, String> {
    let parts = view_object_name_parts(name);
    let (catalog, database, view) = match parts.as_slice() {
        [view] => (
            DEFAULT_CATALOG.to_string(),
            current_database.to_string(),
            view.clone(),
        ),
        [database, view] => (DEFAULT_CATALOG.to_string(), database.clone(), view.clone()),
        [catalog, database, view] => (catalog.clone(), database.clone(), view.clone()),
        _ => return Err(format!("invalid view name: {}", parts.join("."))),
    };
    let catalog = normalize_identifier(&catalog)?;
    if catalog != DEFAULT_CATALOG {
        return Err(format!("unknown iceberg catalog: {catalog}"));
    }
    Ok(SessionViewKey {
        catalog,
        database: normalize_identifier(&database)?,
        view: normalize_identifier(&view)?,
    })
}

fn query_definition(
    raw_query_source: &str,
    context: ViewRequestContext<'_>,
) -> Result<PersistedQueryDefinition, String> {
    PersistedQueryDefinition::new(
        raw_query_source,
        PersistedQueryDialect::StarRocks,
        context.current_catalog.unwrap_or(DEFAULT_CATALOG),
        context.current_database,
    )
}

fn build_string_result(column_name: &str, rows: Vec<String>) -> Result<QueryResult, String> {
    build_query_result(vec![(column_name.to_string(), rows)])
}

pub(crate) fn build_query_result(
    columns: Vec<(String, Vec<String>)>,
) -> Result<QueryResult, String> {
    let row_count = columns.first().map(|(_, rows)| rows.len()).unwrap_or(0);
    if columns.iter().any(|(_, rows)| rows.len() != row_count) {
        return Err("view query result columns have different row counts".to_string());
    }
    let names = columns
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>();
    let rows = (0..row_count)
        .map(|row| {
            columns
                .iter()
                .map(|(_, values)| values[row].clone())
                .collect()
        })
        .collect();
    build_utf8_query_result(&names, rows)
        .map_err(|error| format!("build view query result failed: {error}"))
}
