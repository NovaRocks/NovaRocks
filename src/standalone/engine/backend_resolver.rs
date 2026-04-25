//! Backend target resolution for standalone connector dispatch.
//!
//! This is the one place that maps a parsed SQL object name plus session
//! context into the backend name and normalized catalog/namespace/table
//! identifiers used by connector traits.

use std::sync::Arc;

use crate::sql::parser::ast::ObjectName;
use crate::standalone::engine::StandaloneState;
use crate::standalone::engine::name_resolve::{
    resolve_iceberg_namespace_name, resolve_iceberg_table_name, resolve_local_table_name,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TargetBackend {
    pub(crate) backend_name: &'static str,
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
}

pub(crate) fn resolve_table_target(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<TargetBackend, String> {
    if current_catalog.is_none() && name.parts.len() <= 2 {
        let resolved = resolve_local_table_name(name, current_database)?;
        let managed_exists = state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock")
            .contains_table(&resolved.database, &resolved.table)?;
        if managed_exists || state.managed_lake_config.is_some() {
            return Ok(TargetBackend {
                backend_name: "managed",
                catalog: String::new(),
                namespace: resolved.database,
                table: resolved.table,
            });
        }
    }

    let resolved = resolve_iceberg_table_name(name.clone(), current_catalog, current_database)?;
    Ok(TargetBackend {
        backend_name: "iceberg",
        catalog: resolved.catalog,
        namespace: resolved.namespace,
        table: resolved.table,
    })
}

pub(crate) fn resolve_existing_table_target(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<TargetBackend, String> {
    if current_catalog.is_none() && name.parts.len() <= 2 {
        let resolved = resolve_local_table_name(name, current_database)?;
        let managed_exists = state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock")
            .contains_table(&resolved.database, &resolved.table)?;
        if managed_exists {
            return Ok(TargetBackend {
                backend_name: "managed",
                catalog: String::new(),
                namespace: resolved.database,
                table: resolved.table,
            });
        }
    }

    let resolved = resolve_iceberg_table_name(name.clone(), current_catalog, current_database)?;
    Ok(TargetBackend {
        backend_name: "iceberg",
        catalog: resolved.catalog,
        namespace: resolved.namespace,
        table: resolved.table,
    })
}

pub(crate) fn resolve_namespace_target(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
) -> Result<TargetBackend, String> {
    if current_catalog.is_none() && name.parts.len() == 1 {
        return Ok(TargetBackend {
            backend_name: "managed",
            catalog: String::new(),
            namespace: crate::standalone::engine::catalog::normalize_identifier(name.leaf())?,
            table: String::new(),
        });
    }

    let resolved = resolve_iceberg_namespace_name(name.clone(), current_catalog)?;
    let _ = state;
    Ok(TargetBackend {
        backend_name: "iceberg",
        catalog: resolved.catalog,
        namespace: resolved.namespace,
        table: String::new(),
    })
}
