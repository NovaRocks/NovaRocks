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

//! Frontend-owned view DDL, metadata, and query rewrite service.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks::runtime::query_result::{QueryResult, QueryResultColumn};
use novarocks::view::{
    ViewEngine, ViewRequestContext, ViewService, ViewSqlDialect, ViewStatementResult,
};
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema};
use novarocks_spi::state_store::StateStore;
use novarocks_types::SlotId;
use sqlparser::ast::{ObjectName, ObjectNamePart, Query, Statement};
use sqlparser::parser::Parser;
use tokio::runtime::Handle;

pub(crate) mod command;
mod iceberg;
pub mod repository;
mod rewrite;

use repository::{DatabaseMutation, StoredDatabaseViewsV1, ViewRepository};

const DEFAULT_CATALOG: &str = "default_catalog";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SessionViewKey {
    catalog: String,
    database: String,
    view: String,
}

#[derive(Clone, Debug)]
struct StoredView {
    query: Box<Query>,
}

pub struct FrontendViewService {
    registry: RwLock<HashMap<SessionViewKey, StoredView>>,
    mutation: Mutex<()>,
    repository: Option<ViewRepository>,
    runtime: Handle,
}

impl FrontendViewService {
    pub async fn open(store: Option<Arc<dyn StateStore>>, runtime: Handle) -> Result<Self, String> {
        let repository = match store {
            Some(store) => Some(ViewRepository::open(store, runtime.clone()).await?),
            None => None,
        };
        let records = match &repository {
            Some(repository) => repository.load_all().await?,
            None => Vec::new(),
        };
        let service = Self {
            registry: RwLock::new(HashMap::new()),
            mutation: Mutex::new(()),
            repository,
            runtime,
        };
        service.replace_all_records(records)?;
        Ok(service)
    }

    fn replace_all_records(&self, records: Vec<StoredDatabaseViewsV1>) -> Result<(), String> {
        let mut replacement = HashMap::new();
        for record in records {
            append_record_views(&mut replacement, &record)?;
        }
        *self
            .registry
            .write()
            .map_err(|error| format!("frontend view registry write lock: {error}"))? = replacement;
        Ok(())
    }

    fn replace_database_record(&self, record: &StoredDatabaseViewsV1) -> Result<(), String> {
        let mut parsed = HashMap::new();
        append_record_views(&mut parsed, record)?;
        let mut registry = self
            .registry
            .write()
            .map_err(|error| format!("frontend view registry write lock: {error}"))?;
        registry.retain(|key, _| key.catalog != record.catalog || key.database != record.database);
        registry.extend(parsed);
        Ok(())
    }

    fn recover_cache_after_mutation_error(&self) -> Result<(), String> {
        let Some(repository) = &self.repository else {
            return Ok(());
        };
        let records = self.block_on(repository.load_all())?;
        self.replace_all_records(records)
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        if Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }

    fn create_session_view(
        &self,
        key: SessionViewKey,
        query: Box<Query>,
        or_replace: bool,
    ) -> Result<(), String> {
        let _mutation = self
            .mutation
            .lock()
            .map_err(|error| format!("frontend view mutation lock: {error}"))?;
        if let Some(repository) = &self.repository {
            let mutation = DatabaseMutation::Create {
                view: key.view.clone(),
                sql: query.to_string(),
                or_replace,
            };
            match self.block_on(repository.mutate_database(&key.catalog, &key.database, mutation)) {
                Ok(record) => self.replace_database_record(&record),
                Err(error) => {
                    let recovery = self.recover_cache_after_mutation_error();
                    match recovery {
                        Ok(()) => Err(error),
                        Err(recovery_error) => Err(format!(
                            "{error}; reload frontend view cache failed: {recovery_error}"
                        )),
                    }
                }
            }
        } else {
            let mut registry = self
                .registry
                .write()
                .map_err(|error| format!("frontend view registry write lock: {error}"))?;
            if registry.contains_key(&key) && !or_replace {
                return Err(format!(
                    "view already exists: {}.{}",
                    key.database, key.view
                ));
            }
            registry.insert(key, StoredView { query });
            Ok(())
        }
    }

    fn drop_session_view(&self, key: &SessionViewKey) -> Result<(), String> {
        let _mutation = self
            .mutation
            .lock()
            .map_err(|error| format!("frontend view mutation lock: {error}"))?;
        if let Some(repository) = &self.repository {
            match self.block_on(repository.mutate_database(
                &key.catalog,
                &key.database,
                DatabaseMutation::DropView {
                    view: key.view.clone(),
                },
            )) {
                Ok(record) => self.replace_database_record(&record),
                Err(error) => {
                    let recovery = self.recover_cache_after_mutation_error();
                    match recovery {
                        Ok(()) => Err(error),
                        Err(recovery_error) => Err(format!(
                            "{error}; reload frontend view cache failed: {recovery_error}"
                        )),
                    }
                }
            }
        } else {
            self.registry
                .write()
                .map_err(|error| format!("frontend view registry write lock: {error}"))?
                .remove(key);
            Ok(())
        }
    }

    fn handle_create(
        &self,
        engine: &dyn ViewEngine,
        sql: &str,
        context: ViewRequestContext<'_>,
    ) -> Result<ViewStatementResult, String> {
        let mut parser = Parser::new(&ViewSqlDialect)
            .try_with_sql(sql)
            .map_err(|error| format!("CREATE VIEW parse error: {error}"))?;
        let statement = parser
            .parse_statement()
            .map_err(|error| format!("CREATE VIEW parse error: {error}"))?;
        let Statement::CreateView(create_view) = statement else {
            return Err("CREATE VIEW: failed to parse statement".to_string());
        };
        if let Some(target) = iceberg::resolve_external_target(engine, &create_view.name, context)?
        {
            return iceberg::create_external_view(engine, target, create_view, context);
        }
        let key = session_view_key(&create_view.name, context.current_database)?;
        self.create_session_view(key, create_view.query, create_view.or_replace)?;
        Ok(ViewStatementResult::Ok)
    }

    fn handle_drop(
        &self,
        engine: &dyn ViewEngine,
        sql: &str,
        context: ViewRequestContext<'_>,
    ) -> Result<ViewStatementResult, String> {
        let mut parser = Parser::new(&ViewSqlDialect)
            .try_with_sql(sql)
            .map_err(|error| format!("DROP VIEW parse error: {error}"))?;
        let statement = parser
            .parse_statement()
            .map_err(|error| format!("DROP VIEW parse error: {error}"))?;
        let Statement::Drop {
            object_type: sqlparser::ast::ObjectType::View,
            names,
            if_exists,
            ..
        } = statement
        else {
            return Err("DROP VIEW: failed to parse statement".to_string());
        };
        for name in names {
            if let Some(target) = iceberg::resolve_external_target(engine, &name, context)? {
                iceberg::drop_external_view(engine, &target, if_exists, context)?;
            } else {
                self.drop_session_view(&session_view_key(&name, context.current_database)?)?;
            }
        }
        Ok(ViewStatementResult::Ok)
    }

    fn handle_show_views(
        &self,
        engine: &dyn ViewEngine,
        sql: &str,
        context: ViewRequestContext<'_>,
    ) -> Result<ViewStatementResult, String> {
        let database =
            iceberg::parse_show_views(sql)?.unwrap_or_else(|| context.current_database.to_string());
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
                .map_err(|error| format!("frontend view registry read lock: {error}"))?
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
            .map_err(|error| format!("frontend view registry read lock: {error}"))
            .map(|registry| registry.clone())
    }
}

impl ViewService for FrontendViewService {
    fn try_handle_statement(
        &self,
        engine: &dyn ViewEngine,
        sql: &str,
        context: ViewRequestContext<'_>,
    ) -> Result<Option<ViewStatementResult>, String> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let normalized = trimmed.to_ascii_lowercase();
        if normalized.starts_with("create view ")
            || normalized.starts_with("create or replace view ")
        {
            return self.handle_create(engine, trimmed, context).map(Some);
        }
        if normalized.starts_with("drop view ") {
            return self.handle_drop(engine, trimmed, context).map(Some);
        }
        if has_keyword_prefix(&normalized, &["show", "create", "view"]) {
            return iceberg::show_create_view(engine, trimmed, context).map(Some);
        }
        if has_keyword_prefix(&normalized, &["show", "views"]) {
            return self.handle_show_views(engine, trimmed, context).map(Some);
        }
        Ok(None)
    }

    fn rewrite_query(
        &self,
        engine: &dyn ViewEngine,
        query: &mut Query,
        context: ViewRequestContext<'_>,
    ) -> Result<(), String> {
        let registry = self.registry_snapshot()?;
        rewrite::expand_session_views(query, &registry, context.current_database);
        rewrite::expand_external_views(engine, query, context)
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
            .map_err(|error| format!("frontend view mutation lock: {error}"))?;
        if let Some(repository) = &self.repository {
            match self.block_on(repository.mutate_database(
                &catalog,
                &database,
                DatabaseMutation::DropDatabase,
            )) {
                Ok(record) => self.replace_database_record(&record),
                Err(error) => {
                    let recovery = self.recover_cache_after_mutation_error();
                    match recovery {
                        Ok(()) => Err(error),
                        Err(recovery_error) => Err(format!(
                            "{error}; reload frontend view cache failed: {recovery_error}"
                        )),
                    }
                }
            }
        } else {
            self.registry
                .write()
                .map_err(|error| format!("frontend view registry write lock: {error}"))?
                .retain(|key, _| key.catalog != catalog || key.database != database);
            Ok(())
        }
    }
}

fn append_record_views(
    registry: &mut HashMap<SessionViewKey, StoredView>,
    record: &StoredDatabaseViewsV1,
) -> Result<(), String> {
    for (view, sql) in &record.views {
        registry.insert(
            SessionViewKey {
                catalog: record.catalog.clone(),
                database: record.database.clone(),
                view: view.clone(),
            },
            StoredView {
                query: parse_query(sql)?,
            },
        );
    }
    Ok(())
}

fn parse_query(sql: &str) -> Result<Box<Query>, String> {
    let statements = Parser::parse_sql(&ViewSqlDialect, sql)
        .map_err(|error| format!("query parse failed: {error}"))?;
    match statements.as_slice() {
        [Statement::Query(query)] => Ok(query.clone()),
        _ => Err("view SQL must contain exactly one query statement".to_string()),
    }
}

fn object_name_parts(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .filter_map(|part| match part {
            ObjectNamePart::Identifier(identifier) => Some(identifier.value.clone()),
            _ => None,
        })
        .collect()
}

fn session_view_key(name: &ObjectName, current_database: &str) -> Result<SessionViewKey, String> {
    let parts = object_name_parts(name);
    let (catalog, database, view) = match parts.as_slice() {
        [view] => (
            DEFAULT_CATALOG.to_string(),
            current_database.to_string(),
            view.clone(),
        ),
        [database, view] => (DEFAULT_CATALOG.to_string(), database.clone(), view.clone()),
        [catalog, database, view] => (catalog.clone(), database.clone(), view.clone()),
        _ => return Err(format!("invalid view name: {name}")),
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

fn build_string_result(column_name: &str, rows: Vec<String>) -> Result<QueryResult, String> {
    build_query_result(vec![(column_name.to_string(), rows)])
}

fn has_keyword_prefix(sql: &str, expected: &[&str]) -> bool {
    sql.split_ascii_whitespace()
        .zip(expected)
        .all(|(actual, expected)| actual == *expected)
        && sql.split_ascii_whitespace().count() >= expected.len()
}

fn build_query_result(columns: Vec<(String, Vec<String>)>) -> Result<QueryResult, String> {
    let row_count = columns.first().map(|(_, rows)| rows.len()).unwrap_or(0);
    if columns.iter().any(|(_, rows)| rows.len() != row_count) {
        return Err("view query result columns have different row counts".to_string());
    }
    let fields = columns
        .iter()
        .map(|(name, _)| Field::new(name, DataType::Utf8, false))
        .collect::<Vec<_>>();
    let arrays = columns
        .iter()
        .map(|(_, rows)| Arc::new(StringArray::from(rows.clone())) as ArrayRef)
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|error| format!("build view query result failed: {error}"))?;
    let slot_ids = (1..=columns.len())
        .map(|index| {
            u32::try_from(index)
                .map(SlotId::new)
                .map_err(|_| "too many view query result columns".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &slot_ids)?;
    let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema)?;
    Ok(QueryResult {
        columns: columns
            .into_iter()
            .map(|(name, _)| QueryResultColumn {
                name,
                data_type: DataType::Utf8,
                nullable: false,
                logical_type: None,
            })
            .collect(),
        chunks: vec![chunk],
    })
}
