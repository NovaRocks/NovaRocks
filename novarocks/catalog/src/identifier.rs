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

//! Catalog identifier normalization, identity, and name resolution.
//!
//! This module owns string-only catalog naming rules. Parser and session
//! adapters pass already-separated name parts and current context explicitly.

#[derive(Clone, Debug)]
pub struct LocalTableIdentity {
    pub database: String,
    pub table: String,
}

#[derive(Clone, Debug)]
pub struct CatalogNamespaceIdentity {
    pub catalog: String,
    pub namespace: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableIdentity {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl TableIdentity {
    pub fn new(catalog: &str, namespace: &str, table: &str) -> Self {
        Self {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
        }
    }

    pub fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.table)
    }
}

pub fn normalize_identifier(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    // Strip backtick quotes if present
    let trimmed = trimmed
        .strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .unwrap_or(trimmed);
    if trimmed.is_empty() {
        return Err("identifier is empty".to_string());
    }
    let mut chars = trimmed.chars();
    let Some(first) = chars.next() else {
        return Err("identifier is empty".to_string());
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(format!("unsupported identifier `{trimmed}`"));
    }
    if !chars.all(|c| c == '_' || c.is_ascii_alphanumeric()) {
        return Err(format!("unsupported identifier `{trimmed}`"));
    }
    Ok(trimmed.to_ascii_lowercase())
}

pub fn normalize_optional_identifier(raw: Option<&str>) -> Result<Option<String>, String> {
    raw.map(normalize_identifier).transpose()
}

pub fn resolve_local_table_name(
    parts: &[String],
    current_database: &str,
) -> Result<LocalTableIdentity, String> {
    match parts {
        [table] => Ok(LocalTableIdentity {
            database: normalize_identifier(current_database)?,
            table: normalize_identifier(table)?,
        }),
        [database, table] => Ok(LocalTableIdentity {
            database: normalize_identifier(database)?,
            table: normalize_identifier(table)?,
        }),
        _ => Err(format!(
            "local table name must be `<table>` or `<database>.<table>`, got `{}`",
            parts.join(".")
        )),
    }
}

pub fn resolve_catalog_namespace_name(
    parts: &[String],
    current_catalog: Option<&str>,
) -> Result<CatalogNamespaceIdentity, String> {
    match (normalize_optional_identifier(current_catalog)?, parts) {
        (Some(catalog), [namespace]) => Ok(CatalogNamespaceIdentity {
            catalog,
            namespace: normalize_identifier(namespace)?,
        }),
        (_, [catalog, namespace]) => Ok(CatalogNamespaceIdentity {
            catalog: normalize_identifier(catalog)?,
            namespace: normalize_identifier(namespace)?,
        }),
        _ => Err(format!(
            "iceberg database name must be `<database>` with current catalog or `<catalog>.<database>`, got `{}`",
            parts.join(".")
        )),
    }
}

pub fn resolve_catalog_table_name(
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<TableIdentity, String> {
    match (normalize_optional_identifier(current_catalog)?, parts) {
        (Some(catalog), [table]) => Ok(TableIdentity {
            catalog,
            namespace: normalize_identifier(current_database)?,
            table: normalize_identifier(table)?,
        }),
        (Some(catalog), [namespace, table]) => Ok(TableIdentity {
            catalog,
            namespace: normalize_identifier(namespace)?,
            table: normalize_identifier(table)?,
        }),
        (_, [catalog, namespace, table]) => Ok(TableIdentity {
            catalog: normalize_identifier(catalog)?,
            namespace: normalize_identifier(namespace)?,
            table: normalize_identifier(table)?,
        }),
        _ => Err(format!(
            "iceberg table name must be `<table>`/`<database>.<table>` with current catalog or `<catalog>.<database>.<table>`, got `{}`",
            parts.join(".")
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TableIdentity, normalize_identifier, normalize_optional_identifier,
        resolve_catalog_namespace_name, resolve_catalog_table_name, resolve_local_table_name,
    };

    fn parts(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn normalization_preserves_existing_identifier_rules() {
        assert_eq!(normalize_identifier("  Orders  "), Ok("orders".to_string()));
        assert_eq!(
            normalize_identifier("`MiXeD_42`"),
            Ok("mixed_42".to_string())
        );
        assert_eq!(
            normalize_identifier("_Leading9"),
            Ok("_leading9".to_string())
        );
        assert_eq!(normalize_optional_identifier(None), Ok(None));
        assert_eq!(
            normalize_optional_identifier(Some("  `Catalog_1`  ")),
            Ok(Some("catalog_1".to_string()))
        );
    }

    #[test]
    fn normalization_preserves_exact_errors() {
        assert_eq!(
            normalize_identifier("   "),
            Err("identifier is empty".to_string())
        );
        assert_eq!(
            normalize_identifier("9table"),
            Err("unsupported identifier `9table`".to_string())
        );
        assert_eq!(
            normalize_identifier("some-name"),
            Err("unsupported identifier `some-name`".to_string())
        );
        assert_eq!(
            normalize_identifier("`leading"),
            Err("unsupported identifier ``leading`".to_string())
        );
        assert_eq!(
            normalize_identifier("trailing`"),
            Err("unsupported identifier `trailing``".to_string())
        );
    }

    #[test]
    fn local_resolution_preserves_success_and_arity_contract() {
        let current = resolve_local_table_name(&parts(&["Orders"]), "Sales").unwrap();
        assert_eq!(current.database, "sales");
        assert_eq!(current.table, "orders");

        let explicit =
            resolve_local_table_name(&parts(&["Analytics", "Events"]), "ignored").unwrap();
        assert_eq!(explicit.database, "analytics");
        assert_eq!(explicit.table, "events");

        assert_eq!(
            resolve_local_table_name(&parts(&[]), "db").unwrap_err(),
            "local table name must be `<table>` or `<database>.<table>`, got ``"
        );
        assert_eq!(
            resolve_local_table_name(&parts(&["a", "b", "c"]), "db").unwrap_err(),
            "local table name must be `<table>` or `<database>.<table>`, got `a.b.c`"
        );
    }

    #[test]
    fn local_resolution_preserves_database_before_table_error_precedence() {
        assert_eq!(
            resolve_local_table_name(&parts(&["bad-table"]), "bad-db").unwrap_err(),
            "unsupported identifier `bad-db`"
        );
        assert_eq!(
            resolve_local_table_name(&parts(&["bad-db", "bad-table"]), "ignored").unwrap_err(),
            "unsupported identifier `bad-db`"
        );
    }

    #[test]
    fn namespace_resolution_preserves_success_and_arity_contract() {
        let current = resolve_catalog_namespace_name(&parts(&["Sales"]), Some("Ice")).unwrap();
        assert_eq!(current.catalog, "ice");
        assert_eq!(current.namespace, "sales");

        let explicit =
            resolve_catalog_namespace_name(&parts(&["Other", "Analytics"]), Some("Ice")).unwrap();
        assert_eq!(explicit.catalog, "other");
        assert_eq!(explicit.namespace, "analytics");

        let explicit_without_current =
            resolve_catalog_namespace_name(&parts(&["Other", "Analytics"]), None).unwrap();
        assert_eq!(explicit_without_current.catalog, "other");
        assert_eq!(explicit_without_current.namespace, "analytics");

        let arity = "iceberg database name must be `<database>` with current catalog or `<catalog>.<database>`, got `sales`";
        assert_eq!(
            resolve_catalog_namespace_name(&parts(&["sales"]), None).unwrap_err(),
            arity
        );
        assert_eq!(
            resolve_catalog_namespace_name(&parts(&[]), None).unwrap_err(),
            "iceberg database name must be `<database>` with current catalog or `<catalog>.<database>`, got ``"
        );
        assert_eq!(
            resolve_catalog_namespace_name(&parts(&["a", "b", "c"]), None).unwrap_err(),
            "iceberg database name must be `<database>` with current catalog or `<catalog>.<database>`, got `a.b.c`"
        );
    }

    #[test]
    fn namespace_resolution_preserves_eager_error_precedence() {
        assert_eq!(
            resolve_catalog_namespace_name(&parts(&["bad-namespace"]), Some("bad-current"))
                .unwrap_err(),
            "unsupported identifier `bad-current`"
        );
        assert_eq!(
            resolve_catalog_namespace_name(&parts(&["bad-namespace"]), Some("valid_current"))
                .unwrap_err(),
            "unsupported identifier `bad-namespace`"
        );
        assert_eq!(
            resolve_catalog_namespace_name(&parts(&["bad-namespace"]), None).unwrap_err(),
            "iceberg database name must be `<database>` with current catalog or `<catalog>.<database>`, got `bad-namespace`"
        );
        assert_eq!(
            resolve_catalog_namespace_name(&parts(&[]), Some("bad-current")).unwrap_err(),
            "unsupported identifier `bad-current`"
        );
        assert_eq!(
            resolve_catalog_namespace_name(&parts(&["a", "b", "c"]), Some("bad-current"))
                .unwrap_err(),
            "unsupported identifier `bad-current`"
        );
        assert_eq!(
            resolve_catalog_namespace_name(
                &parts(&["bad-explicit", "bad-namespace"]),
                Some("bad-current")
            )
            .unwrap_err(),
            "unsupported identifier `bad-current`"
        );
        assert_eq!(
            resolve_catalog_namespace_name(
                &parts(&["bad-explicit", "bad-namespace"]),
                Some("valid_current")
            )
            .unwrap_err(),
            "unsupported identifier `bad-explicit`"
        );
        assert_eq!(
            resolve_catalog_namespace_name(
                &parts(&["valid_explicit", "bad-namespace"]),
                Some("valid_current")
            )
            .unwrap_err(),
            "unsupported identifier `bad-namespace`"
        );
    }

    #[test]
    fn table_resolution_preserves_success_and_arity_contract() {
        let current =
            resolve_catalog_table_name(&parts(&["Orders"]), Some("Ice"), "Sales").unwrap();
        assert_eq!(current.catalog, "ice");
        assert_eq!(current.namespace, "sales");
        assert_eq!(current.table, "orders");

        let namespace =
            resolve_catalog_table_name(&parts(&["Analytics", "Events"]), Some("Ice"), "ignored")
                .unwrap();
        assert_eq!(namespace.catalog, "ice");
        assert_eq!(namespace.namespace, "analytics");
        assert_eq!(namespace.table, "events");

        let explicit = resolve_catalog_table_name(
            &parts(&["Other", "Analytics", "Events"]),
            Some("Ice"),
            "ignored",
        )
        .unwrap();
        assert_eq!(explicit.catalog, "other");
        assert_eq!(explicit.namespace, "analytics");
        assert_eq!(explicit.table, "events");

        let explicit_without_current =
            resolve_catalog_table_name(&parts(&["Other", "Analytics", "Events"]), None, "ignored")
                .unwrap();
        assert_eq!(explicit_without_current.catalog, "other");
        assert_eq!(explicit_without_current.namespace, "analytics");
        assert_eq!(explicit_without_current.table, "events");

        let arity = "iceberg table name must be `<table>`/`<database>.<table>` with current catalog or `<catalog>.<database>.<table>`, got `bad-table`";
        assert_eq!(
            resolve_catalog_table_name(&parts(&["bad-table"]), None, "bad-db"),
            Err(arity.to_string())
        );
        assert_eq!(
            resolve_catalog_table_name(&parts(&["bad-ns", "bad-table"]), None, "ignored"),
            Err("iceberg table name must be `<table>`/`<database>.<table>` with current catalog or `<catalog>.<database>.<table>`, got `bad-ns.bad-table`".to_string())
        );
    }

    #[test]
    fn table_resolution_preserves_eager_error_precedence() {
        assert_eq!(
            resolve_catalog_table_name(&parts(&[]), None, "ignored").unwrap_err(),
            "iceberg table name must be `<table>`/`<database>.<table>` with current catalog or `<catalog>.<database>.<table>`, got ``"
        );
        assert_eq!(
            resolve_catalog_table_name(
                &parts(&["a", "b", "c", "d"]),
                Some("valid_current"),
                "ignored"
            )
            .unwrap_err(),
            "iceberg table name must be `<table>`/`<database>.<table>` with current catalog or `<catalog>.<database>.<table>`, got `a.b.c.d`"
        );
        assert_eq!(
            resolve_catalog_table_name(&parts(&[]), Some("bad-current"), "bad-db"),
            Err("unsupported identifier `bad-current`".to_string())
        );
        assert_eq!(
            resolve_catalog_table_name(
                &parts(&["a", "b", "c", "d"]),
                Some("bad-current"),
                "bad-db"
            ),
            Err("unsupported identifier `bad-current`".to_string())
        );

        assert_eq!(
            resolve_catalog_table_name(&parts(&["bad-table"]), Some("bad-current"), "bad-db"),
            Err("unsupported identifier `bad-current`".to_string())
        );
        assert_eq!(
            resolve_catalog_table_name(&parts(&["bad-table"]), Some("valid_current"), "bad-db"),
            Err("unsupported identifier `bad-db`".to_string())
        );
        assert_eq!(
            resolve_catalog_table_name(&parts(&["bad-table"]), Some("valid_current"), "valid_db"),
            Err("unsupported identifier `bad-table`".to_string())
        );

        assert_eq!(
            resolve_catalog_table_name(
                &parts(&["bad-ns", "bad-table"]),
                Some("bad-current"),
                "ignored"
            ),
            Err("unsupported identifier `bad-current`".to_string())
        );
        assert_eq!(
            resolve_catalog_table_name(
                &parts(&["bad-ns", "bad-table"]),
                Some("valid_current"),
                "ignored"
            ),
            Err("unsupported identifier `bad-ns`".to_string())
        );
        assert_eq!(
            resolve_catalog_table_name(
                &parts(&["valid_ns", "bad-table"]),
                Some("valid_current"),
                "ignored"
            ),
            Err("unsupported identifier `bad-table`".to_string())
        );

        assert_eq!(
            resolve_catalog_table_name(
                &parts(&["bad-explicit", "bad-ns", "bad-table"]),
                Some("bad-current"),
                "ignored"
            ),
            Err("unsupported identifier `bad-current`".to_string())
        );
        assert_eq!(
            resolve_catalog_table_name(
                &parts(&["bad-explicit", "bad-ns", "bad-table"]),
                Some("valid_current"),
                "ignored"
            ),
            Err("unsupported identifier `bad-explicit`".to_string())
        );
        assert_eq!(
            resolve_catalog_table_name(
                &parts(&["valid_explicit", "bad-ns", "bad-table"]),
                Some("valid_current"),
                "ignored"
            ),
            Err("unsupported identifier `bad-ns`".to_string())
        );
        assert_eq!(
            resolve_catalog_table_name(
                &parts(&["valid_explicit", "valid_ns", "bad-table"]),
                Some("valid_current"),
                "ignored"
            ),
            Err("unsupported identifier `bad-table`".to_string())
        );
    }

    #[test]
    fn table_identity_constructor_preserves_raw_case() {
        let identity = TableIdentity::new("MiXeD", "Ns", "T");
        assert_eq!(identity.catalog, "MiXeD");
        assert_eq!(identity.namespace, "Ns");
        assert_eq!(identity.table, "T");
    }

    #[test]
    fn table_identity_formats_fully_qualified_name() {
        let identity = TableIdentity::new("ice", "sales", "orders");
        assert_eq!(identity.fqn(), "ice.sales.orders");
    }
}
