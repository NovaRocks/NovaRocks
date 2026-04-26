//! Query preparation that materializes external connector tables into the
//! standalone in-memory catalog before planning.

use std::sync::Arc;

use crate::sql::catalog::TableDef;
use crate::sql::parser::ast::ObjectName;
use crate::standalone::engine::StandaloneState;
use crate::standalone::engine::backend_resolver::resolve_table_target;
use crate::standalone::engine::sqlparse::statement::{
    extract_table_names_from_query, extract_three_part_table_refs,
};

pub(crate) fn register_external_tables_for_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    register_external_tables_for_query_impl(state, current_catalog, current_database, query, false)
}

pub(crate) fn refresh_external_tables_for_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    register_external_tables_for_query_impl(state, current_catalog, current_database, query, true)
}

fn register_external_tables_for_query_impl(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    force_refresh: bool,
) -> Result<(), String> {
    let mut names = query_table_names(current_catalog, query);
    if names.is_empty() {
        return Ok(());
    }
    names.sort_by(|left, right| left.parts.cmp(&right.parts));
    names.dedup_by(|left, right| left.parts == right.parts);

    let (catalog, source) = {
        let registry = state
            .connectors
            .read()
            .expect("standalone connector registry read lock");
        (
            registry.catalog_backend("iceberg")?,
            registry.table_source("iceberg")?,
        )
    };

    for name in names {
        let Ok(target) = resolve_table_target(state, &name, current_catalog, current_database)
        else {
            continue;
        };
        if target.backend_name != "iceberg" {
            continue;
        }
        if !force_refresh {
            let local = state.catalog.read().expect("catalog read lock");
            if local.get(&target.namespace, &target.table).is_ok() {
                continue;
            }
        }

        let resolved = match catalog.load_table(&target.catalog, &target.namespace, &target.table) {
            Ok(resolved) => resolved,
            Err(_) => continue,
        };
        let table_def = source.build_table_def(&resolved)?;
        register_external_table(state, &target.namespace, table_def)?;
    }

    Ok(())
}

fn query_table_names(
    current_catalog: Option<&str>,
    query: &sqlparser::ast::Query,
) -> Vec<ObjectName> {
    if current_catalog.is_some() {
        extract_table_names_from_query(query)
            .into_iter()
            .map(|table| ObjectName { parts: vec![table] })
            .collect()
    } else {
        extract_three_part_table_refs(query)
            .into_iter()
            .map(|(catalog, namespace, table)| ObjectName {
                parts: vec![catalog, namespace, table],
            })
            .collect()
    }
}

fn register_external_table(
    state: &Arc<StandaloneState>,
    namespace: &str,
    table_def: TableDef,
) -> Result<(), String> {
    let mut guard = state.catalog.write().expect("catalog write lock");
    guard.create_database(namespace).ok();
    guard
        .register(namespace, table_def)
        .map_err(|e| format!("register external table: {e}"))
}

pub(crate) fn build_iceberg_table_def_with_files(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    data_files: Vec<(String, i64, Option<i64>)>,
) -> Result<TableDef, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg registry read lock");
        registry.get(catalog_name)?
    };
    let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table_name)?;
    crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
        &entry, namespace, table_name, loaded, data_files,
    )
}
