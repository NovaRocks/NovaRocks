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

//! Backend target resolution for standalone connector dispatch.
//!
//! This is the one place that maps a parsed SQL object name plus session
//! context into the backend name and normalized catalog/namespace/table
//! identifiers used by connector traits.

use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::sql::parser::ast::ObjectName;
use novarocks_catalog::identifier::{resolve_catalog_namespace_name, resolve_catalog_table_name};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TargetBackend {
    pub(crate) backend_name: &'static str,
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
}

const DEFAULT_CATALOG_NAME: &str = "default_catalog";

fn is_default_catalog(value: &str) -> bool {
    value.eq_ignore_ascii_case(DEFAULT_CATALOG_NAME)
}

fn default_catalog_error() -> String {
    "default_catalog is not a user table catalog; create an external Iceberg catalog and SET catalog before using persistent tables".to_string()
}

fn missing_current_catalog_error(kind: &str) -> String {
    format!(
        "{kind} requires an Iceberg catalog; create an external Iceberg catalog and SET catalog before using persistent tables"
    )
}

fn reject_default_catalog_reference(
    name: &ObjectName,
    current_catalog: Option<&str>,
) -> Result<(), String> {
    if current_catalog.is_some_and(is_default_catalog)
        || name
            .parts
            .first()
            .is_some_and(|part| is_default_catalog(part))
    {
        return Err(default_catalog_error());
    }
    Ok(())
}

pub(crate) fn resolve_table_target(
    _state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<TargetBackend, String> {
    reject_default_catalog_reference(name, current_catalog)?;
    if current_catalog.is_none() && name.parts.len() <= 2 {
        return Err(missing_current_catalog_error("CREATE TABLE"));
    }

    let resolved =
        resolve_catalog_table_name(name.parts.as_slice(), current_catalog, current_database)?;
    Ok(TargetBackend {
        backend_name: "iceberg",
        catalog: resolved.catalog,
        namespace: resolved.namespace,
        table: resolved.table,
    })
}

pub(crate) fn resolve_existing_table_target(
    _state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<TargetBackend, String> {
    reject_default_catalog_reference(name, current_catalog)?;
    if current_catalog.is_none() && name.parts.len() <= 2 {
        return Err(missing_current_catalog_error("Table operation"));
    }

    let resolved =
        resolve_catalog_table_name(name.parts.as_slice(), current_catalog, current_database)?;
    Ok(TargetBackend {
        backend_name: "iceberg",
        catalog: resolved.catalog,
        namespace: resolved.namespace,
        table: resolved.table,
    })
}

pub(crate) fn resolve_namespace_target(
    _state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
) -> Result<TargetBackend, String> {
    reject_default_catalog_reference(name, current_catalog)?;
    if current_catalog.is_none() && name.parts.len() == 1 {
        return Err(missing_current_catalog_error("CREATE DATABASE"));
    }

    let resolved = resolve_catalog_namespace_name(name.parts.as_slice(), current_catalog)?;
    Ok(TargetBackend {
        backend_name: "iceberg",
        catalog: resolved.catalog,
        namespace: resolved.namespace,
        table: String::new(),
    })
}
