//! Backend target resolution for standalone connector dispatch.
//!
//! This is the one place that maps a parsed SQL object name plus session
//! context into the backend name and normalized catalog/namespace/table
//! identifiers used by connector traits.

use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::engine::name_resolve::{
    resolve_iceberg_namespace_name, resolve_iceberg_table_name, resolve_local_table_name,
};
use crate::sql::parser::ast::ObjectName;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TargetBackend {
    pub(crate) backend_name: &'static str,
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
}

const DEFAULT_CATALOG_SENTINEL: &str = "default_catalog";

/// StarRocks-compat shorthand: `default_catalog.<db>.<tbl>` is a fully-qualified
/// reference to the local (standalone) catalog. Strip the prefix and surface a
/// 2-part name so the downstream resolver routes through the local catalog
/// path instead of being treated as an iceberg table reference.
fn strip_default_catalog(name: &ObjectName) -> Option<ObjectName> {
    if name.parts.len() == 3 && name.parts[0].eq_ignore_ascii_case(DEFAULT_CATALOG_SENTINEL) {
        Some(ObjectName {
            parts: name.parts[1..].to_vec(),
        })
    } else {
        None
    }
}

pub(crate) fn resolve_table_target(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<TargetBackend, String> {
    let stripped = strip_default_catalog(name);
    let effective_name = stripped.as_ref().unwrap_or(name);
    let effective_catalog = if stripped.is_some() {
        None
    } else {
        current_catalog
    };

    if effective_catalog.is_none() && effective_name.parts.len() <= 2 {
        let resolved = resolve_local_table_name(effective_name, current_database)?;
        let managed_exists = state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock")
            .contains_table(&resolved.database, &resolved.table)?;
        if managed_exists || state.managed_lake_config.is_some() || stripped.is_some() {
            return Ok(TargetBackend {
                backend_name: "managed",
                catalog: String::new(),
                namespace: resolved.database,
                table: resolved.table,
            });
        }
    }

    let resolved =
        resolve_iceberg_table_name(effective_name.clone(), effective_catalog, current_database)?;
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
    let stripped = strip_default_catalog(name);
    let effective_name = stripped.as_ref().unwrap_or(name);
    let effective_catalog = if stripped.is_some() {
        None
    } else {
        current_catalog
    };

    if effective_catalog.is_none() && effective_name.parts.len() <= 2 {
        let resolved = resolve_local_table_name(effective_name, current_database)?;
        let managed_exists = state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock")
            .contains_table(&resolved.database, &resolved.table)?;
        if managed_exists || stripped.is_some() {
            return Ok(TargetBackend {
                backend_name: "managed",
                catalog: String::new(),
                namespace: resolved.database,
                table: resolved.table,
            });
        }
    }

    let resolved =
        resolve_iceberg_table_name(effective_name.clone(), effective_catalog, current_database)?;
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
            namespace: crate::engine::catalog::normalize_identifier(name.leaf())?,
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
