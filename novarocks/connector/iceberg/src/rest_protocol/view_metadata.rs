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

//! Iceberg REST view metadata, including every version's storage table.

use std::collections::{HashMap, HashSet};

use novarocks_failure_contract::{FailureFact, FailureProjection};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorObjectKind, ConnectorStorageTableIdentifier,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::sync::Arc;
use uuid::Uuid;

use crate::iceberg::spec::Schema;
use crate::iceberg::{NamespaceIdent, TableIdent};
use crate::metadata_json::{self, Document};

/// The opaque table pointer shared by REST request and response versions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct StorageTableIdentifier {
    pub(crate) namespace: Vec<String>,
    pub(crate) name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) catalog: Option<String>,
}

impl StorageTableIdentifier {
    pub(crate) fn materialized_view_kind(&self) -> ConnectorObjectKind {
        ConnectorObjectKind::MaterializedView {
            storage_table: ConnectorStorageTableIdentifier {
                namespace: self
                    .namespace
                    .iter()
                    .map(|name| Arc::from(name.as_str()))
                    .collect(),
                name: Arc::from(self.name.as_str()),
                catalog: self.catalog.as_deref().map(Arc::from),
            },
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ViewObjectKind {
    View,
    MaterializedView {
        storage_table: StorageTableIdentifier,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct ViewMetadata {
    view_uuid: Uuid,
    current_version_id: i32,
    versions: Vec<ViewVersion>,
    schemas: Vec<Schema>,
    properties: HashMap<String, String>,
    // These fields are retained for callers that need the complete document.
    #[allow(dead_code)]
    format_version: i64,
    #[allow(dead_code)]
    location: String,
    #[allow(dead_code)]
    version_log: Vec<ViewVersionLog>,
}

impl ViewMetadata {
    pub(crate) fn uuid(&self) -> Uuid {
        self.view_uuid
    }

    pub(crate) fn current_version_id(&self) -> i32 {
        self.current_version_id
    }

    pub(crate) fn versions(&self) -> impl ExactSizeIterator<Item = &ViewVersion> {
        self.versions.iter()
    }

    pub(crate) fn version_by_id(&self, id: i32) -> Option<&ViewVersion> {
        self.versions
            .iter()
            .find(|version| version.version_id == id)
    }

    pub(crate) fn current_version(&self) -> &ViewVersion {
        self.version_by_id(self.current_version_id)
            .expect("the decoder validates the current view version")
    }

    pub(crate) fn schemas_iter(&self) -> impl ExactSizeIterator<Item = &Schema> {
        self.schemas.iter()
    }

    pub(crate) fn schema_by_id(&self, id: i32) -> Option<&Schema> {
        self.schemas.iter().find(|schema| schema.schema_id() == id)
    }

    pub(crate) fn current_schema(&self) -> &Schema {
        self.schema_by_id(self.current_version().schema_id())
            .expect("the decoder validates view version schemas")
    }

    pub(crate) fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ViewVersion {
    version_id: i32,
    schema_id: i32,
    timestamp_ms: i64,
    summary: HashMap<String, String>,
    representations: Vec<ViewRepresentation>,
    default_catalog: Option<String>,
    default_namespace: NamespaceIdent,
    storage_table: Option<StorageTableIdentifier>,
}

impl ViewVersion {
    pub(crate) fn version_id(&self) -> i32 {
        self.version_id
    }

    pub(crate) fn schema_id(&self) -> i32 {
        self.schema_id
    }

    pub(crate) fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    pub(crate) fn summary(&self) -> &HashMap<String, String> {
        &self.summary
    }

    pub(crate) fn representations(&self) -> &[ViewRepresentation] {
        &self.representations
    }

    pub(crate) fn default_catalog(&self) -> Option<&String> {
        self.default_catalog.as_ref()
    }

    pub(crate) fn default_namespace(&self) -> &NamespaceIdent {
        &self.default_namespace
    }

    pub(crate) fn storage_table(&self) -> Option<&StorageTableIdentifier> {
        self.storage_table.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ViewRepresentation {
    Sql(SqlViewRepresentation),
    Unknown { representation_type: String },
}

impl ViewRepresentation {
    pub(crate) fn as_sql(&self) -> Option<&SqlViewRepresentation> {
        match self {
            Self::Sql(sql) => Some(sql),
            Self::Unknown { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlViewRepresentation {
    pub(crate) sql: String,
    pub(crate) dialect: String,
}

#[allow(dead_code)] // Retain the complete log even when callers use only the current version.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ViewVersionLog {
    version_id: i32,
    timestamp_ms: i64,
}

/// An observed version of the Iceberg view format that this provider cannot decode.
#[derive(Debug)]
pub(crate) struct ViewFormatVersionUnsupported {
    pub(crate) version: i64,
    view: TableIdent,
}

impl std::fmt::Display for ViewFormatVersionUnsupported {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Iceberg view {} uses unsupported format version {}; only version 1 is supported",
            self.view, self.version
        )
    }
}

impl std::error::Error for ViewFormatVersionUnsupported {}

impl FailureProjection for ViewFormatVersionUnsupported {
    fn failure_fact(&self) -> FailureFact {
        ConnectorErrorKind::Unsupported.failure_fact()
    }
}

// Borrowed JSON fields permit field-specific diagnostics without exposing
// response values, and allow identification to ignore fields it cannot use.
#[derive(Deserialize)]
struct ResponseMetadata<'a> {
    #[serde(borrow)]
    metadata: Option<&'a RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ResponseLocation<'a> {
    #[serde(borrow)]
    metadata_location: Option<&'a RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct FormatVersionField<'a> {
    #[serde(borrow)]
    format_version: Option<&'a RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct VersionSelectionFields<'a> {
    #[serde(borrow)]
    current_version_id: Option<&'a RawValue>,
    #[serde(borrow)]
    versions: Option<&'a RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct VersionIdField<'a> {
    #[serde(borrow)]
    version_id: Option<&'a RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct StorageTableField<'a> {
    #[serde(borrow)]
    storage_table: Option<&'a RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct MetadataFields<'a> {
    #[serde(borrow)]
    view_uuid: Option<&'a RawValue>,
    #[serde(borrow)]
    location: Option<&'a RawValue>,
    #[serde(borrow)]
    schemas: Option<&'a RawValue>,
    #[serde(borrow)]
    current_version_id: Option<&'a RawValue>,
    #[serde(borrow)]
    versions: Option<&'a RawValue>,
    #[serde(borrow)]
    version_log: Option<&'a RawValue>,
    #[serde(borrow)]
    properties: Option<&'a RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct VersionFields<'a> {
    #[serde(borrow)]
    version_id: Option<&'a RawValue>,
    #[serde(borrow)]
    schema_id: Option<&'a RawValue>,
    #[serde(borrow)]
    timestamp_ms: Option<&'a RawValue>,
    #[serde(borrow)]
    summary: Option<&'a RawValue>,
    #[serde(borrow)]
    representations: Option<&'a RawValue>,
    #[serde(borrow)]
    default_catalog: Option<&'a RawValue>,
    #[serde(borrow)]
    default_namespace: Option<&'a RawValue>,
    #[serde(borrow)]
    storage_table: Option<&'a RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SchemaIdField<'a> {
    #[serde(borrow)]
    schema_id: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct RepresentationTypeField<'a> {
    #[serde(borrow, rename = "type")]
    representation_type: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct SqlRepresentationFields<'a> {
    #[serde(borrow)]
    sql: Option<&'a RawValue>,
    #[serde(borrow)]
    dialect: Option<&'a RawValue>,
}

/// Decode the full original load, create, or commit view response.
pub(crate) fn decode_view_full(
    bytes: &[u8],
    view: &TableIdent,
) -> Result<ViewMetadata, ConnectorError> {
    let metadata = response_metadata(bytes, view)?;
    let format_version = supported_format_version(metadata, view)?;
    // response_metadata has already checked the depth and syntax of the full
    // envelope, including the location skipped by the identification path.
    let response: ResponseLocation<'_> = decode_json(
        std::str::from_utf8(bytes).map_err(|_| corrupt(view, "invalid REST response"))?,
    )
    .map_err(|_| corrupt(view, "invalid field `metadata-location`"))?;
    let _: String = required(response.metadata_location, view, "metadata-location")?;

    let fields: MetadataFields<'_> = object(metadata, view, "metadata")?;
    let current_version_id = required(fields.current_version_id, view, "current-version-id")?;
    let versions: Vec<&RawValue> = required(fields.versions, view, "versions")?;
    let mut version_ids = HashSet::new();
    let versions = versions
        .into_iter()
        .map(|raw| {
            let version = decode_version(raw, view)?;
            if !version_ids.insert(version.version_id) {
                return Err(corrupt(view, "duplicate `version-id` in `versions`"));
            }
            Ok(version)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !version_ids.contains(&current_version_id) {
        return Err(corrupt(
            view,
            "`current-version-id` is absent from `versions`",
        ));
    }

    let schemas: Vec<&RawValue> = required(fields.schemas, view, "schemas")?;
    let mut schema_ids = HashSet::new();
    let schemas = schemas
        .into_iter()
        .map(|raw| {
            // Schema also accepts v1 table schemas with no ID. A view schema
            // must have its own ID because every version refers to it.
            let field: SchemaIdField<'_> = object(raw, view, "schemas")?;
            let id: i32 = required(field.schema_id, view, "schemas[].schema-id")?;
            if !schema_ids.insert(id) {
                return Err(corrupt(view, "duplicate `schema-id` in `schemas`"));
            }
            object(raw, view, "schemas")
        })
        .collect::<Result<Vec<Schema>, _>>()?;
    if versions
        .iter()
        .any(|version| !schema_ids.contains(&version.schema_id))
    {
        return Err(corrupt(
            view,
            "a version's `schema-id` is absent from `schemas`",
        ));
    }

    let version_log: Vec<&RawValue> = required(fields.version_log, view, "version-log")?;
    let version_log = version_log
        .into_iter()
        .map(|entry| object(entry, view, "version-log"))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ViewMetadata {
        view_uuid: required(fields.view_uuid, view, "view-uuid")?,
        current_version_id,
        versions,
        schemas,
        properties: optional(fields.properties, view, "properties")?.unwrap_or_default(),
        format_version,
        location: required(fields.location, view, "location")?,
        version_log,
    })
}

/// Read only the current version's storage-table declaration. Other parts of
/// the document may be unusable without preventing identification or DROP.
pub(crate) fn classify_view_metadata(
    bytes: &[u8],
    view: &TableIdent,
) -> Result<ViewObjectKind, ConnectorError> {
    let metadata = response_metadata(bytes, view)?;
    supported_format_version(metadata, view)?;

    let fields: VersionSelectionFields<'_> = object(metadata, view, "metadata")?;
    let current_id: i32 = required(fields.current_version_id, view, "current-version-id")?;
    let versions: Vec<&RawValue> = required(fields.versions, view, "versions")?;
    let mut current = None;
    for raw in versions {
        // In particular, the storage table on a historical version is not
        // relevant even if the JSON gives it a bad shape.
        let field: VersionIdField<'_> = object(raw, view, "versions")?;
        let id: i32 = required(field.version_id, view, "versions[].version-id")?;
        if id == current_id && current.replace(raw).is_some() {
            return Err(corrupt(
                view,
                "`current-version-id` is ambiguous in `versions`",
            ));
        }
    }
    let current =
        current.ok_or_else(|| corrupt(view, "`current-version-id` is absent from `versions`"))?;
    let field: StorageTableField<'_> = object(current, view, "versions[].storage-table")?;
    match storage_table(field.storage_table, view)? {
        Some(storage_table) => Ok(ViewObjectKind::MaterializedView { storage_table }),
        None => Ok(ViewObjectKind::View),
    }
}

fn response_metadata<'a>(
    bytes: &'a [u8],
    view: &TableIdent,
) -> Result<&'a RawValue, ConnectorError> {
    let response: ResponseMetadata<'_> =
        metadata_json::from_slice_document(bytes, Document::ViewResponse).map_err(|error| {
            if error.is_unsupported() {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    format!("Iceberg view {view}: {error}"),
                )
            } else {
                corrupt(view, "invalid REST response or `metadata` field")
            }
        })?;
    if !bytes.trim_ascii_start().starts_with(b"{") {
        return Err(corrupt(view, "REST response must be an object"));
    }
    response
        .metadata
        .ok_or_else(|| corrupt(view, "missing `metadata` field"))
}

fn supported_format_version(metadata: &RawValue, view: &TableIdent) -> Result<i64, ConnectorError> {
    let field: FormatVersionField<'_> = object(metadata, view, "metadata.format-version")?;
    let version = required(field.format_version, view, "format-version")?;
    if version != 1 {
        return Err(ConnectorError::from_domain(
            ConnectorErrorKind::Unsupported,
            ViewFormatVersionUnsupported {
                version,
                view: view.clone(),
            },
        ));
    }
    Ok(version)
}

fn decode_version(raw: &RawValue, view: &TableIdent) -> Result<ViewVersion, ConnectorError> {
    let fields: VersionFields<'_> = object(raw, view, "versions")?;
    let representations: Vec<&RawValue> =
        required(fields.representations, view, "versions[].representations")?;
    let representations = representations
        .into_iter()
        .map(|raw| decode_representation(raw, view))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ViewVersion {
        version_id: required(fields.version_id, view, "versions[].version-id")?,
        schema_id: required(fields.schema_id, view, "versions[].schema-id")?,
        timestamp_ms: required(fields.timestamp_ms, view, "versions[].timestamp-ms")?,
        summary: required(fields.summary, view, "versions[].summary")?,
        representations,
        default_catalog: optional(fields.default_catalog, view, "versions[].default-catalog")?,
        default_namespace: required(
            fields.default_namespace,
            view,
            "versions[].default-namespace",
        )?,
        storage_table: storage_table(fields.storage_table, view)?,
    })
}

fn decode_representation(
    raw: &RawValue,
    view: &TableIdent,
) -> Result<ViewRepresentation, ConnectorError> {
    let kind: RepresentationTypeField<'_> = object(raw, view, "representations")?;
    let representation_type: String =
        required(kind.representation_type, view, "representations[].type")?;
    if representation_type != "sql" {
        return Ok(ViewRepresentation::Unknown {
            representation_type,
        });
    }
    let fields: SqlRepresentationFields<'_> = object(raw, view, "representations")?;
    Ok(ViewRepresentation::Sql(SqlViewRepresentation {
        sql: required(fields.sql, view, "representations[].sql")?,
        dialect: required(fields.dialect, view, "representations[].dialect")?,
    }))
}

fn storage_table(
    raw: Option<&RawValue>,
    view: &TableIdent,
) -> Result<Option<StorageTableIdentifier>, ConnectorError> {
    raw.map(|raw| object(raw, view, "versions[].storage-table"))
        .transpose()
}

fn required<'a, T: Deserialize<'a>>(
    raw: Option<&'a RawValue>,
    view: &TableIdent,
    field: &str,
) -> Result<T, ConnectorError> {
    let raw = raw.ok_or_else(|| corrupt(view, &format!("missing field `{field}`")))?;
    decode_json(raw.get()).map_err(|_| corrupt(view, &format!("invalid field `{field}`")))
}

fn optional<'a, T: Deserialize<'a>>(
    raw: Option<&'a RawValue>,
    view: &TableIdent,
    field: &str,
) -> Result<Option<T>, ConnectorError> {
    raw.map(|raw| required(Some(raw), view, field)).transpose()
}

fn object<'a, T: Deserialize<'a>>(
    raw: &'a RawValue,
    view: &TableIdent,
    field: &str,
) -> Result<T, ConnectorError> {
    if !raw.get().starts_with('{') {
        return Err(corrupt(view, &format!("field `{field}` must be an object")));
    }
    required(Some(raw), view, field)
}

fn decode_json<'a, T: Deserialize<'a>>(text: &'a str) -> Result<T, serde_json::Error> {
    let mut decoder = serde_json::Deserializer::from_str(text);
    // The outer document scanner authorizes this slice's finite geometry.
    decoder.disable_recursion_limit();
    let value = T::deserialize(&mut decoder)?;
    decoder.end()?;
    Ok(value)
}

fn corrupt(view: &TableIdent, detail: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        format!("Iceberg view {view}: {detail}"),
    )
}

#[cfg(test)]
pub(crate) fn decode_test_metadata(metadata: serde_json::Value) -> ViewMetadata {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "metadata-location": "file:///test-view/metadata/1.json",
        "metadata": metadata
    }))
    .unwrap();
    let view = TableIdent::from_strs(["db", "v"]).unwrap();
    decode_view_full(&bytes, &view).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::error::Error;

    const SPEC_EXAMPLE: &[u8] = include_bytes!("tests/fixtures/views/mv-spec.load-view.json");
    const CATALOG_EXAMPLE: &[u8] =
        include_bytes!("tests/fixtures/views/mv-with-catalog.load-view.json");

    fn ident() -> TableIdent {
        TableIdent::from_strs(["default", "event_agg_mv"]).unwrap()
    }

    fn document() -> Value {
        serde_json::from_slice(SPEC_EXAMPLE).unwrap()
    }

    fn decode(document: &Value) -> Result<ViewMetadata, ConnectorError> {
        decode_view_full(&serde_json::to_vec(document).unwrap(), &ident())
    }

    fn classify(document: &Value) -> Result<ViewObjectKind, ConnectorError> {
        classify_view_metadata(&serde_json::to_vec(document).unwrap(), &ident())
    }

    fn pointer(catalog: Option<&str>) -> StorageTableIdentifier {
        StorageTableIdentifier {
            namespace: vec!["default".to_owned()],
            name: "event_agg_mv__storage".to_owned(),
            catalog: catalog.map(str::to_owned),
        }
    }

    #[test]
    fn appendix_example_preserves_all_storage_identifier_fields() {
        for (bytes, catalog) in [(SPEC_EXAMPLE, None), (CATALOG_EXAMPLE, Some("prod"))] {
            let metadata = decode_view_full(bytes, &ident()).unwrap();
            assert_eq!(
                metadata.current_version().storage_table(),
                Some(&pointer(catalog))
            );
            assert_eq!(
                classify_view_metadata(bytes, &ident()).unwrap(),
                ViewObjectKind::MaterializedView {
                    storage_table: pointer(catalog)
                }
            );
            assert_eq!(
                metadata.location,
                "s3://bucket/warehouse/default.db/event_agg_mv"
            );
            assert_eq!(metadata.format_version, 1);
            assert_eq!(metadata.version_log[0].version_id, 1);
            assert_eq!(metadata.version_log[0].timestamp_ms, 1_573_518_431_292);
        }
    }

    #[test]
    fn absent_and_null_storage_tables_both_identify_an_ordinary_view() {
        let mut data = document();
        data["metadata"]["versions"][0]["storage-table"] = Value::Null;
        assert_eq!(classify(&data).unwrap(), ViewObjectKind::View);
        assert_eq!(
            decode(&data).unwrap().current_version().storage_table(),
            None
        );

        data["metadata"]["versions"][0]
            .as_object_mut()
            .unwrap()
            .remove("storage-table");
        assert_eq!(classify(&data).unwrap(), ViewObjectKind::View);
        assert_eq!(
            decode(&data).unwrap().current_version().storage_table(),
            None
        );

        data["metadata"]["versions"][0]["storage-table"] = json!({
            "namespace": ["default"],
            "name":"event_agg_mv__storage",
            "catalog": null
        });
        assert_eq!(
            decode(&data).unwrap().current_version().storage_table(),
            Some(&pointer(None))
        );
    }

    #[test]
    fn full_decode_ignores_unknown_fields_and_preserves_unknown_representation_types() {
        let mut data = document();
        data["metadata"]["future-field"] = Value::Bool(true);
        data["metadata"]["versions"][0]["future-field"] = json!([1, 2, 3]);
        data["metadata"]["versions"][0]["representations"] = json!([
            {"type": "future", "sql": false, "dialect": 5},
            {"type": "sql", "sql": "SELECT 1", "dialect": "trino", "future-field": "other"}
        ]);
        let metadata = decode(&data).unwrap();
        let representations = metadata.current_version().representations();
        assert_eq!(
            representations[0],
            ViewRepresentation::Unknown {
                representation_type: "future".to_owned()
            }
        );
        assert_eq!(
            representations[1].as_sql(),
            Some(&SqlViewRepresentation {
                sql: "SELECT 1".to_owned(),
                dialect: "trino".to_owned()
            })
        );
    }

    #[test]
    fn both_decoders_report_the_original_unsupported_format_before_other_fields() {
        let data =
            json!({"metadata-location": "file:///future", "metadata": {"format-version": 2}});
        for error in [decode(&data).unwrap_err(), classify(&data).unwrap_err()] {
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
            let cause = error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<ViewFormatVersionUnsupported>()
                .unwrap();
            assert_eq!(cause.version, 2);
        }
    }

    #[test]
    fn missing_current_version_or_invalid_pointer_refuses_both_paths() {
        let mut future = document();
        future["metadata"]["current-version-id"] = json!(2);
        for error in [decode(&future).unwrap_err(), classify(&future).unwrap_err()] {
            assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
            assert!(error.message().contains("current-version-id"));
        }

        for pointer in [
            json!(17),
            json!({"namespace": "default", "name": "t"}),
            json!({"namespace": [9], "name": "t"}),
            json!({"namespace": ["default"]}),
            json!({"namespace": ["default"], "name": "t", "catalog": false}),
            json!([["default"], "t", null]),
        ] {
            let mut malformed = document();
            malformed["metadata"]["versions"][0]["storage-table"] = pointer;
            for error in [
                decode(&malformed).unwrap_err(),
                classify(&malformed).unwrap_err(),
            ] {
                assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
                assert!(error.message().contains("storage-table"));
                assert!(error.message().contains("event_agg_mv"));
            }
        }
    }

    #[test]
    fn identification_reads_the_current_version_and_ignores_other_defects() {
        let mut data = document();
        let mut ordinary = data["metadata"]["versions"][0].clone();
        ordinary["version-id"] = json!(2);
        ordinary["storage-table"] = Value::Null;
        data["metadata"]["versions"]
            .as_array_mut()
            .unwrap()
            .push(ordinary);
        data["metadata"]["current-version-id"] = json!(2);

        let metadata = decode(&data).unwrap();
        assert_eq!(
            metadata.version_by_id(1).unwrap().storage_table(),
            Some(&pointer(None))
        );
        assert_eq!(metadata.current_version().storage_table(), None);
        assert_eq!(classify(&data).unwrap(), ViewObjectKind::View);

        data["metadata"]["versions"][0]["storage-table"] = json!("bad historical pointer");
        assert_eq!(classify(&data).unwrap(), ViewObjectKind::View);
        assert_eq!(
            decode(&data).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
        data["metadata"]["versions"][0]["storage-table"] = json!(pointer(None));
        data["metadata"]["schemas"] = Value::Bool(false);
        assert_eq!(classify(&data).unwrap(), ViewObjectKind::View);
        assert_eq!(
            decode(&data).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn full_decode_checks_every_version_schema_without_changing_identification() {
        let mut data = document();
        let mut previous = data["metadata"]["versions"][0].clone();
        previous["version-id"] = json!(2);
        previous["schema-id"] = json!(5);
        data["metadata"]["versions"]
            .as_array_mut()
            .unwrap()
            .push(previous);
        assert_eq!(
            classify(&data).unwrap(),
            ViewObjectKind::MaterializedView {
                storage_table: pointer(None)
            }
        );
        let error = decode(&data).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(error.message().contains("schema-id"));
    }
}
