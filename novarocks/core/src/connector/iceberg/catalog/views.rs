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

//! Iceberg view metadata operations. Views are only supported on REST
//! catalogs: Hadoop catalogs reject every operation here. Views
//! are deliberately not cached — each query re-loads the view metadata
//! so external changes are visible immediately.

use std::collections::HashMap;

use iceberg::spec::{
    NestedField, Schema, SqlViewRepresentation, ViewMetadata, ViewRepresentation,
    ViewRepresentations, ViewVersion,
};
use iceberg::{Catalog, NamespaceIdent, TableIdent, ViewCommit, ViewCreation, ViewRequirement};

use super::registry::{
    IcebergCatalogEntry, IcebergCatalogKind, block_on_iceberg, build_iceberg_catalog,
    iceberg_type_for_sql_type,
};
use crate::sql::parser::ast::TableColumnDef;
use novarocks_catalog::identifier::normalize_identifier;

/// Dialect tag NovaRocks writes into view representations. NovaRocks parses
/// StarRocks-flavoured SQL, so it shares StarRocks' dialect tag for
/// cross-engine interop.
pub(crate) const VIEW_DIALECT_STARROCKS: &str = "starrocks";

/// A view loaded from an iceberg catalog, reduced to what the engine needs.
#[derive(Clone, Debug)]
pub(crate) struct LoadedIcebergView {
    pub sql: String,
    pub dialect: String,
    /// Dotted default namespace from the current view version; bare table
    /// names in `sql` resolve against this (and the catalog the view was
    /// loaded from — the stored default-catalog is intentionally ignored,
    /// matching StarRocks, because other engines write their own local
    /// catalog aliases there).
    pub default_namespace: String,
    pub column_names: Vec<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
}

fn catalog_for_views(entry: &IcebergCatalogEntry) -> Result<std::sync::Arc<dyn Catalog>, String> {
    if !matches!(entry.kind, IcebergCatalogKind::Rest) {
        return Err(format!(
            "view operations require a REST iceberg catalog; this catalog is {:?}",
            entry.kind
        ));
    }
    build_iceberg_catalog(entry)
}

fn view_ident(namespace: &str, view: &str) -> Result<(NamespaceIdent, TableIdent), String> {
    let ns_name = normalize_identifier(namespace)?;
    let view_name = normalize_identifier(view)?;
    let ident = TableIdent::from_strs([ns_name.as_str(), view_name.as_str()])
        .map_err(|e| format!("build view ident: {e}"))?;
    Ok((NamespaceIdent::new(ns_name), ident))
}

fn current_millis() -> Result<i64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .map_err(|e| format!("system clock before epoch: {e}"))
}

fn build_view_schema(columns: &[TableColumnDef]) -> Result<Schema, String> {
    let mut next_nested_field_id =
        i32::try_from(columns.len() + 1).map_err(|_| "too many view columns".to_string())?;
    let fields = columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let field_id =
                i32::try_from(idx + 1).map_err(|_| "too many view columns".to_string())?;
            let iceberg_type =
                iceberg_type_for_sql_type(&column.data_type, &mut next_nested_field_id)?;
            let field = if column.nullable {
                NestedField::optional(field_id, &column.name, iceberg_type)
            } else {
                NestedField::required(field_id, &column.name, iceberg_type)
            };
            Ok(field.into())
        })
        .collect::<Result<Vec<_>, String>>()?;
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| format!("build iceberg view schema failed: {e}"))
}

pub(crate) fn create_view(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    view_name: &str,
    columns: &[TableColumnDef],
    view_sql: &str,
    comment: Option<&str>,
    or_replace: bool,
    extra_properties: &[(String, String)],
) -> Result<(), String> {
    let catalog = catalog_for_views(entry)?;
    let (ns, ident) = view_ident(namespace, view_name)?;
    let schema = build_view_schema(columns)?;
    let representations =
        ViewRepresentations::new(vec![ViewRepresentation::Sql(SqlViewRepresentation {
            sql: view_sql.to_string(),
            dialect: VIEW_DIALECT_STARROCKS.to_string(),
        })]);
    let mut properties = HashMap::new();
    if let Some(comment) = comment {
        properties.insert("comment".to_string(), comment.to_string());
    }
    for (key, value) in extra_properties {
        properties.insert(key.clone(), value.clone());
    }
    let mut summary = HashMap::new();
    summary.insert("engine-name".to_string(), "novarocks".to_string());

    if or_replace {
        let existing = block_on_iceberg(async { catalog.load_view(&ident).await })
            .map_err(|e| format!("load iceberg view runtime failed: {e}"))?;
        match existing {
            Ok(current) => {
                return replace_view(
                    catalog.as_ref(),
                    &ident,
                    current,
                    schema,
                    representations,
                    properties,
                    summary,
                );
            }
            Err(err)
                if err
                    .to_string()
                    .contains("Tried to load a view that does not exist") => {}
            Err(err) => return Err(format!("load iceberg view {ident}: {err}")),
        }
    }

    let creation = ViewCreation::builder()
        .name(ident.name.clone())
        .location(None)
        .representations(representations)
        .schema(schema)
        .properties(properties)
        .default_namespace(ns.clone())
        .default_catalog(None)
        .summary(summary)
        .build();
    block_on_iceberg(async { catalog.create_view(&ns, creation).await })
        .map_err(|e| format!("create iceberg view runtime failed: {e}"))?
        .map_err(|e| {
            let message = e.to_string();
            if message.contains("The view already exists") {
                format!("view already exists: {ident}")
            } else {
                format!("create iceberg view {ident}: {message}")
            }
        })?;
    Ok(())
}

fn replace_view(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    current: ViewMetadata,
    schema: Schema,
    representations: ViewRepresentations,
    properties: HashMap<String, String>,
    summary: HashMap<String, String>,
) -> Result<(), String> {
    let uuid = current.uuid();
    let new_version = ViewVersion::builder()
        .with_version_id(1) // reassigned by the builder when added
        .with_schema_id(schema.schema_id())
        .with_timestamp_ms(current_millis()?)
        .with_summary(summary)
        .with_representations(representations)
        .with_default_catalog(None)
        .with_default_namespace(ident.namespace.clone())
        .build();

    let mut builder = current.into_builder();
    if !properties.is_empty() {
        builder = builder
            .set_properties(properties)
            .map_err(|e| format!("set replaced view properties: {e}"))?;
    }
    let result = builder
        .set_current_version(new_version, schema)
        .map_err(|e| format!("stage replaced view version: {e}"))?
        .build()
        .map_err(|e| format!("build replaced view metadata: {e}"))?;

    let commit = ViewCommit::builder()
        .ident(ident.clone())
        .requirements(vec![ViewRequirement::UuidMatch { uuid }])
        .updates(result.changes)
        .build();
    block_on_iceberg(async { catalog.update_view(commit).await })
        .map_err(|e| format!("replace iceberg view runtime failed: {e}"))?
        .map_err(|e| format!("replace iceberg view {ident}: {e}"))?;
    Ok(())
}

pub(crate) fn load_view(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    view_name: &str,
) -> Result<LoadedIcebergView, String> {
    let catalog = catalog_for_views(entry)?;
    let (_ns, ident) = view_ident(namespace, view_name)?;
    let metadata = block_on_iceberg(async { catalog.load_view(&ident).await })
        .map_err(|e| format!("load iceberg view runtime failed: {e}"))?
        .map_err(|e| format_view_not_found(&ident, "load", e))?;
    loaded_view_from_metadata(&ident, &metadata)
}

pub(crate) fn drop_view(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    view_name: &str,
) -> Result<(), String> {
    let catalog = catalog_for_views(entry)?;
    let (_ns, ident) = view_ident(namespace, view_name)?;
    block_on_iceberg(async { catalog.drop_view(&ident).await })
        .map_err(|e| format!("drop iceberg view runtime failed: {e}"))?
        .map_err(|e| format_view_not_found(&ident, "drop", e))
}

pub(crate) fn view_exists(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    view_name: &str,
) -> Result<bool, String> {
    let catalog = catalog_for_views(entry)?;
    let (_ns, ident) = view_ident(namespace, view_name)?;
    block_on_iceberg(async { catalog.view_exists(&ident).await })
        .map_err(|e| format!("view exists runtime failed: {e}"))?
        .map_err(|e| format!("check iceberg view {ident}: {e}"))
}

pub(crate) fn list_views(
    entry: &IcebergCatalogEntry,
    namespace: &str,
) -> Result<Vec<String>, String> {
    let catalog = catalog_for_views(entry)?;
    let ns = NamespaceIdent::new(normalize_identifier(namespace)?);
    let idents = block_on_iceberg(async { catalog.list_views(&ns).await })
        .map_err(|e| format!("list iceberg views runtime failed: {e}"))?
        .map_err(|e| format!("list iceberg views in {ns}: {e}"))?;
    let mut names: Vec<String> = idents.into_iter().map(|ident| ident.name).collect();
    names.sort();
    Ok(names)
}

fn format_view_not_found<E: std::fmt::Display>(ident: &TableIdent, op: &str, err: E) -> String {
    let message = err.to_string();
    if message.contains("view that does not exist") {
        format!("unknown view: {ident}")
    } else {
        format!("{op} REST iceberg view {ident}: {message}")
    }
}

fn loaded_view_from_metadata(
    ident: &TableIdent,
    metadata: &ViewMetadata,
) -> Result<LoadedIcebergView, String> {
    let version = metadata.current_version();
    // Prefer the starrocks representation; otherwise fall back to the first
    // SQL representation (mirrors iceberg-java View::sqlFor).
    let mut chosen: Option<&SqlViewRepresentation> = None;
    for representation in version.representations().iter() {
        let ViewRepresentation::Sql(sql_repr) = representation;
        if sql_repr
            .dialect
            .eq_ignore_ascii_case(VIEW_DIALECT_STARROCKS)
        {
            chosen = Some(sql_repr);
            break;
        }
        if chosen.is_none() {
            chosen = Some(sql_repr);
        }
    }
    let chosen = chosen.ok_or_else(|| format!("iceberg view {ident} has no SQL representation"))?;
    let default_namespace = version
        .default_namespace()
        .iter()
        .map(|part| part.to_string())
        .collect::<Vec<_>>()
        .join(".");
    let column_names = metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect();
    let properties = metadata.properties().clone();
    Ok(LoadedIcebergView {
        sql: chosen.sql.clone(),
        dialect: chosen.dialect.clone(),
        default_namespace,
        column_names,
        comment: properties.get("comment").cloned(),
        properties,
    })
}

#[cfg(test)]
mod rest_view_tests {
    //! Mocked unit tests for the REST view wiring, following the
    //! `rest_catalog_tests` pattern in registry.rs: mock `GET /v1/config`
    //! first, then the view route, and wrap sync entry points in
    //! `spawn_blocking`.
    use mockito::Server;

    use super::super::registry::{IcebergCatalogEntry, build_catalog_entry};
    use super::{create_view, drop_view, list_views, load_view, view_exists};
    use crate::sql::parser::ast::TableColumnDef;
    use novarocks_catalog::schema::SqlType;

    fn rest_props(uri: &str) -> Vec<(String, String)> {
        vec![
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "rest".to_string()),
            ("uri".to_string(), uri.to_string()),
        ]
    }

    const EMPTY_CONFIG_BODY: &str = r#"{"overrides":{},"defaults":{}}"#;

    fn rest_entry(uri: &str) -> IcebergCatalogEntry {
        build_catalog_entry("ice_rest", &rest_props(uri)).expect("rest entry")
    }

    /// Minimal spec-valid LoadViewResult body with the given representations
    /// JSON array (e.g. `[{"type":"sql","sql":"SELECT 1","dialect":"spark"}]`).
    fn load_view_body(representations: &str) -> String {
        load_view_body_with_properties(representations, r#"{"comment": "a test view"}"#)
    }

    fn load_view_body_with_properties(representations: &str, properties: &str) -> String {
        format!(
            r#"{{
              "metadata-location": "s3://warehouse/db/v/metadata/00001-x.metadata.json",
              "metadata": {{
                "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
                "format-version": 1,
                "location": "s3://warehouse/db/v",
                "current-version-id": 1,
                "versions": [{{
                  "version-id": 1,
                  "schema-id": 0,
                  "timestamp-ms": 1700000000000,
                  "summary": {{"engine-name": "novarocks"}},
                  "default-namespace": ["analytics"],
                  "representations": {representations}
                }}],
                "version-log": [{{"version-id": 1, "timestamp-ms": 1700000000000}}],
                "schemas": [{{
                  "schema-id": 0,
                  "type": "struct",
                  "fields": [{{"id": 1, "name": "id", "required": false, "type": "long"}}]
                }}],
                "properties": {properties}
              }},
              "config": {{}}
            }}"#
        )
    }

    #[tokio::test]
    async fn create_view_posts_starrocks_dialect() {
        let mut server = Server::new_async().await;
        let _config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let create = server
            .mock("POST", "/v1/namespaces/analytics/views")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""dialect":"starrocks""#.to_string()),
                mockito::Matcher::Regex(r#""sql":"SELECT id FROM t""#.to_string()),
            ]))
            .with_status(200)
            .with_body(load_view_body(
                r#"[{"type":"sql","sql":"SELECT id FROM t","dialect":"starrocks"}]"#,
            ))
            .expect(1)
            .create_async()
            .await;

        let entry = rest_entry(&server.url());
        let columns = vec![TableColumnDef {
            name: "id".to_string(),
            data_type: SqlType::BigInt,
            nullable: true,
            aggregation: None,
            default: None,
        }];
        tokio::task::spawn_blocking(move || {
            create_view(
                &entry,
                "analytics",
                "v_demo",
                &columns,
                "SELECT id FROM t",
                Some("a test view"),
                false,
                &[],
            )
            .expect("create view via mock");
        })
        .await
        .expect("join");
        create.assert_async().await;
    }

    #[tokio::test]
    async fn load_view_prefers_starrocks_representation() {
        let mut server = Server::new_async().await;
        let _config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let _load = server
            .mock("GET", "/v1/namespaces/analytics/views/v_demo")
            .with_status(200)
            .with_body(load_view_body(
                r#"[{"type":"sql","sql":"SELECT 1","dialect":"spark"},
                   {"type":"sql","sql":"SELECT 2","dialect":"StarRocks"}]"#,
            ))
            .create_async()
            .await;

        let entry = rest_entry(&server.url());
        let view = tokio::task::spawn_blocking(move || {
            load_view(&entry, "analytics", "v_demo").expect("load view")
        })
        .await
        .expect("join");
        assert_eq!(view.sql, "SELECT 2");
        assert!(view.dialect.eq_ignore_ascii_case("starrocks"));
        assert_eq!(view.default_namespace, "analytics");
        assert_eq!(view.column_names, vec!["id".to_string()]);
        assert_eq!(view.comment.as_deref(), Some("a test view"));
    }

    #[tokio::test]
    async fn load_view_exposes_all_metadata_properties() {
        let mut server = Server::new_async().await;
        let _config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let _load = server
            .mock("GET", "/v1/namespaces/analytics/views/v_demo")
            .with_status(200)
            .with_body(load_view_body_with_properties(
                r#"[{"type":"sql","sql":"SELECT id FROM analytics.orders","dialect":"starrocks"}]"#,
                r#"{"comment":"a test view","owner":"analytics","quality":"gold","purpose":"dashboard"}"#,
            ))
            .create_async()
            .await;

        let entry = rest_entry(&server.url());
        let view = tokio::task::spawn_blocking(move || {
            load_view(&entry, "analytics", "v_demo").expect("load view")
        })
        .await
        .expect("join");
        assert_eq!(view.comment.as_deref(), Some("a test view"));
        assert_eq!(
            view.properties.get("owner").map(String::as_str),
            Some("analytics")
        );
        assert_eq!(
            view.properties.get("quality").map(String::as_str),
            Some("gold")
        );
        assert_eq!(
            view.properties.get("purpose").map(String::as_str),
            Some("dashboard")
        );
    }

    #[tokio::test]
    async fn load_view_falls_back_to_first_sql_representation() {
        let mut server = Server::new_async().await;
        let _config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let _load = server
            .mock("GET", "/v1/namespaces/analytics/views/v_spark")
            .with_status(200)
            .with_body(load_view_body(
                r#"[{"type":"sql","sql":"SELECT 1","dialect":"spark"}]"#,
            ))
            .create_async()
            .await;

        let entry = rest_entry(&server.url());
        let view = tokio::task::spawn_blocking(move || {
            load_view(&entry, "analytics", "v_spark").expect("load view")
        })
        .await
        .expect("join");
        assert_eq!(view.dialect, "spark");
        assert_eq!(view.sql, "SELECT 1");
    }

    #[tokio::test]
    async fn load_view_not_found_maps_to_unknown_view() {
        let mut server = Server::new_async().await;
        let _config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let _load = server
            .mock("GET", "/v1/namespaces/analytics/views/missing")
            .with_status(404)
            .with_body(
                r#"{"error":{"message":"not found","type":"NoSuchViewException","code":404}}"#,
            )
            .create_async()
            .await;

        let entry = rest_entry(&server.url());
        let err = tokio::task::spawn_blocking(move || {
            load_view(&entry, "analytics", "missing").expect_err("must fail")
        })
        .await
        .expect("join");
        assert!(
            err.contains("unknown view: analytics.missing"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn drop_view_not_found_maps_to_unknown_view() {
        let mut server = Server::new_async().await;
        let _config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let _drop = server
            .mock("DELETE", "/v1/namespaces/analytics/views/missing")
            .with_status(404)
            .with_body(
                r#"{"error":{"message":"not found","type":"NoSuchViewException","code":404}}"#,
            )
            .create_async()
            .await;

        let entry = rest_entry(&server.url());
        let err = tokio::task::spawn_blocking(move || {
            drop_view(&entry, "analytics", "missing").expect_err("must fail")
        })
        .await
        .expect("join");
        assert!(
            err.contains("unknown view: analytics.missing"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn view_exists_and_list_views_roundtrip() {
        let mut server = Server::new_async().await;
        let _config = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .with_body(EMPTY_CONFIG_BODY)
            .create_async()
            .await;
        let _head = server
            .mock("HEAD", "/v1/namespaces/analytics/views/v_demo")
            .with_status(204)
            .create_async()
            .await;
        let _list = server
            .mock("GET", "/v1/namespaces/analytics/views")
            .with_status(200)
            .with_body(r#"{"identifiers":[{"namespace":["analytics"],"name":"v_demo"}]}"#)
            .create_async()
            .await;

        let entry = rest_entry(&server.url());
        let entry2 = entry.clone();
        let exists = tokio::task::spawn_blocking(move || {
            view_exists(&entry, "analytics", "v_demo").expect("exists")
        })
        .await
        .expect("join");
        assert!(exists);
        let names =
            tokio::task::spawn_blocking(move || list_views(&entry2, "analytics").expect("list"))
                .await
                .expect("join");
        assert_eq!(names, vec!["v_demo".to_string()]);
    }

    #[test]
    fn view_ops_require_rest_catalog() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let props = vec![
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
            (
                "warehouse".to_string(),
                format!("file://{}", dir.path().display()),
            ),
        ];
        let entry = build_catalog_entry("ice_hadoop", &props).expect("hadoop entry");
        let err = list_views(&entry, "analytics").expect_err("must fail");
        assert!(err.contains("require a REST iceberg catalog"), "got: {err}");
    }
}
