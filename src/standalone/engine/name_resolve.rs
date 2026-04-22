//! Table/namespace name resolution helpers.
//!
//! Turn a parsed `ObjectName` (1, 2 or 3 parts) into a fully-qualified
//! `(catalog, database, table)` triple using the session's current-catalog /
//! current-database context and consistent identifier normalization rules.
//!
//! Resolution errors are explicit strings — callers treat any ambiguity as a
//! hard error rather than guessing.

use crate::sql::parser::ast::ObjectName;
use crate::standalone::engine::local::normalize_identifier;

#[derive(Clone, Debug)]
pub(crate) struct ResolvedLocalTableName {
    pub(crate) database: String,
    pub(crate) table: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedIcebergNamespaceName {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedIcebergTableName {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
}

pub(crate) fn resolve_local_table_name(
    name: &ObjectName,
    current_database: &str,
) -> Result<ResolvedLocalTableName, String> {
    match name.parts.as_slice() {
        [table] => Ok(ResolvedLocalTableName {
            database: normalize_identifier(current_database)?,
            table: normalize_identifier(table)?,
        }),
        [database, table] => Ok(ResolvedLocalTableName {
            database: normalize_identifier(database)?,
            table: normalize_identifier(table)?,
        }),
        _ => Err(format!(
            "local table name must be `<table>` or `<database>.<table>`, got `{}`",
            name.parts.join(".")
        )),
    }
}

pub(crate) fn resolve_iceberg_namespace_name(
    name: ObjectName,
    current_catalog: Option<&str>,
) -> Result<ResolvedIcebergNamespaceName, String> {
    match (
        normalize_optional_identifier(current_catalog)?,
        name.parts.as_slice(),
    ) {
        (Some(catalog), [namespace]) => Ok(ResolvedIcebergNamespaceName {
            catalog,
            namespace: normalize_identifier(namespace)?,
        }),
        (_, [catalog, namespace]) => Ok(ResolvedIcebergNamespaceName {
            catalog: normalize_identifier(catalog)?,
            namespace: normalize_identifier(namespace)?,
        }),
        _ => Err(format!(
            "iceberg database name must be `<database>` with current catalog or `<catalog>.<database>`, got `{}`",
            name.parts.join(".")
        )),
    }
}

pub(crate) fn resolve_iceberg_table_name(
    name: ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<ResolvedIcebergTableName, String> {
    match (
        normalize_optional_identifier(current_catalog)?,
        name.parts.as_slice(),
    ) {
        (Some(catalog), [table]) => Ok(ResolvedIcebergTableName {
            catalog,
            namespace: normalize_identifier(current_database)?,
            table: normalize_identifier(table)?,
        }),
        (Some(catalog), [namespace, table]) => Ok(ResolvedIcebergTableName {
            catalog,
            namespace: normalize_identifier(namespace)?,
            table: normalize_identifier(table)?,
        }),
        (_, [catalog, namespace, table]) => Ok(ResolvedIcebergTableName {
            catalog: normalize_identifier(catalog)?,
            namespace: normalize_identifier(namespace)?,
            table: normalize_identifier(table)?,
        }),
        _ => Err(format!(
            "iceberg table name must be `<table>`/`<database>.<table>` with current catalog or `<catalog>.<database>.<table>`, got `{}`",
            name.parts.join(".")
        )),
    }
}

pub(crate) fn resolve_iceberg_table_name_explicit(
    name: &ObjectName,
) -> Result<ResolvedIcebergTableName, String> {
    let [catalog, namespace, table] = name.parts.as_slice() else {
        return Err(format!(
            "iceberg table name must be `<catalog>.<database>.<table>`, got `{}`",
            name.parts.join(".")
        ));
    };
    Ok(ResolvedIcebergTableName {
        catalog: normalize_identifier(catalog)?,
        namespace: normalize_identifier(namespace)?,
        table: normalize_identifier(table)?,
    })
}

pub(crate) fn normalize_optional_identifier(raw: Option<&str>) -> Result<Option<String>, String> {
    raw.map(normalize_identifier).transpose()
}
