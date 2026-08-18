// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the
// License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

//! Persisted Iceberg MV definition lookup and SQL identity helpers.

use novarocks_catalog::identifier::TableIdentity;
use sha2::{Digest, Sha256};

use crate::mv::domain::model::MvTarget;
use crate::mv::domain::persistence::definition::StoredMvDefinition;
use crate::mv::domain::refresh::target::IcebergMvTarget;
use crate::mv::domain::repository::MvRepository;

/// Loads the persisted definition for one normalized Iceberg MV target.
pub fn load_iceberg_mv_definition_by_target(
    repository: &dyn MvRepository,
    target: &IcebergMvTarget,
) -> Result<StoredMvDefinition, String> {
    #[cfg(test)]
    record_definition_load();
    repository
        .find_by_target(&MvTarget {
            catalog: Some(target.catalog.clone()),
            database: target.namespace.clone(),
            name: target.table.clone(),
        })
        .map_err(|e| format!("load iceberg mv definition failed: {e}"))?
        .ok_or_else(|| {
            format!(
                "iceberg materialized view {}.{}.{} has no MV definition",
                target.catalog, target.namespace, target.table
            )
        })
}

/// Computes the stable persisted-SQL fingerprint used by refresh artifacts.
pub fn mv_definition_fingerprint(select_sql: &str) -> String {
    hex::encode(Sha256::digest(select_sql.as_bytes()))
}

/// Parses the raw stored MV SELECT SQL without invoking query compilation.
pub fn parse_mv_select_query(sql: &str) -> Result<sqlparser::ast::Query, String> {
    let normalized = novarocks_sql::syntax::normalize_for_raw_parse(sql)
        .map_err(|e| format!("stored MV SELECT normalize error: {e}"))?;
    let statement = novarocks_sql::syntax::parse_normalized_sql_raw(&normalized)
        .map_err(|err| format!("sql parser error: {err}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("stored MV SQL must be a SELECT query".to_string());
    };
    Ok(*query)
}

/// Parses persisted Iceberg base-table references into canonical identities.
pub fn parse_iceberg_table_refs(refs: &[String]) -> Result<Vec<TableIdentity>, String> {
    refs.iter()
        .map(|fqn| {
            let parts = fqn.split('.').collect::<Vec<_>>();
            let [catalog, namespace, table] = parts.as_slice() else {
                return Err(format!(
                    "materialized view base table reference must be catalog.namespace.table, got `{fqn}`"
                ));
            };
            Ok(TableIdentity {
                catalog: novarocks_catalog::identifier::normalize_identifier(catalog)?,
                namespace: novarocks_catalog::identifier::normalize_identifier(namespace)?,
                table: novarocks_catalog::identifier::normalize_identifier(table)?,
            })
        })
        .collect()
}

#[cfg(test)]
thread_local! {
    static DEFINITION_LOAD_COUNTER: std::cell::RefCell<Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) struct DefinitionLoadCounterGuard;

#[cfg(test)]
impl DefinitionLoadCounterGuard {
    pub(crate) fn install(counter: std::sync::Arc<std::sync::atomic::AtomicUsize>) -> Self {
        DEFINITION_LOAD_COUNTER.with(|slot| {
            assert!(
                slot.borrow().is_none(),
                "definition load counter already installed"
            );
            *slot.borrow_mut() = Some(counter);
        });
        Self
    }
}

#[cfg(test)]
impl Drop for DefinitionLoadCounterGuard {
    fn drop(&mut self) {
        DEFINITION_LOAD_COUNTER.with(|slot| *slot.borrow_mut() = None);
    }
}

#[cfg(test)]
fn record_definition_load() {
    DEFINITION_LOAD_COUNTER.with(|slot| {
        if let Some(counter) = slot.borrow().as_ref() {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definition_fingerprint_is_stable_for_the_same_sql() {
        assert_eq!(
            mv_definition_fingerprint("SELECT * FROM ice.db.t"),
            mv_definition_fingerprint("SELECT * FROM ice.db.t")
        );
        assert_ne!(
            mv_definition_fingerprint("SELECT * FROM ice.db.t"),
            mv_definition_fingerprint("SELECT * FROM ice.db.other")
        );
    }

    #[test]
    fn parse_stored_select_rejects_non_query_statement() {
        assert_eq!(
            parse_mv_select_query("DELETE FROM ice.db.t").unwrap_err(),
            "stored MV SQL must be a SELECT query"
        );
    }
}
