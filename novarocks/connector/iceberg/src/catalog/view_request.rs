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

//! Validated view creations and replacements.
//!
//! Design: ADR-0176 (docs/adr/ADR-0176-iceberg-rest-request-vocabulary-and-supported-server-floor.md)
//!
//! The semantic checks a server repeats at commit time — dialect retention
//! and the version-history property — run here, before anything is
//! dispatched: a request refused by the server after dispatch is an unknown
//! outcome, while a local refusal is certain. Both types can only be built by
//! validation, so an owner can receive nothing else. The REST protocol layer
//! only encodes them.
//!
//! A replacement always adds a version carrying the complete target
//! definition and a fresh operation identity, and selects it as the version
//! the commit added. It never names an existing version by number: a server
//! reuses version IDs after history expiry, so a number loaded earlier may
//! denote another definition by dispatch time. The operation identity keeps
//! the server from folding the new version into an equivalent existing one,
//! which would leave nothing for `-1` to select.

use std::collections::{BTreeSet, HashMap};

use novarocks_spi::connector::{ConnectorMutationFailure, ConnectorMutationFailureKind};
use uuid::Uuid;

use crate::iceberg::spec::Schema;
use crate::iceberg::{NamespaceIdent, TableIdent};
use crate::rest_protocol::mutation::{SqlRepresentationWire, ViewVersionWire};
use crate::rest_protocol::view_metadata::{
    StorageTableIdentifier, ViewMetadata, ViewRepresentation,
};

/// The only dialect NovaRocks writes.
pub(crate) const VIEW_DIALECT_TRINO: &str = "trino";
/// The summary key of a replacement's operation identity.
pub(crate) const OPERATION_ID_SUMMARY_KEY: &str = "novarocks.operation-id";
const REPLACE_DROP_DIALECT_ALLOWED: &str = "replace.drop-dialect.allowed";
const VERSION_HISTORY_NUM_ENTRIES: &str = "version.history.num-entries";
const INITIAL_VIEW_VERSION_ID: i32 = 1;

/// The definition a NovaRocks view version states.
#[derive(Clone, Debug)]
pub(crate) struct ViewDefinition {
    pub(crate) schema: Schema,
    pub(crate) sql: String,
    pub(crate) summary: HashMap<String, String>,
    pub(crate) default_catalog: Option<String>,
    pub(crate) default_namespace: NamespaceIdent,
    pub(crate) timestamp_ms: i64,
}

impl ViewDefinition {
    fn version(
        self,
        version_id: i32,
        schema_id: i32,
        storage_table: Option<StorageTableIdentifier>,
    ) -> ViewVersionWire {
        ViewVersionWire {
            version_id,
            schema_id,
            timestamp_ms: self.timestamp_ms,
            summary: self.summary,
            representations: vec![SqlRepresentationWire {
                sql: self.sql,
                dialect: VIEW_DIALECT_TRINO.to_string(),
            }],
            default_catalog: self.default_catalog,
            default_namespace: self.default_namespace,
            storage_table,
        }
    }
}

/// A view creation whose properties satisfy the server's constraints.
#[derive(Clone, Debug)]
pub(crate) struct ViewCreate {
    name: String,
    schema: Schema,
    version: ViewVersionWire,
    properties: HashMap<String, String>,
}

impl ViewCreate {
    pub(crate) fn validate(
        name: String,
        definition: ViewDefinition,
        properties: HashMap<String, String>,
        storage_table: Option<StorageTableIdentifier>,
    ) -> Result<Self, String> {
        validate_history_entries(&properties)?;
        let schema = definition.schema.clone();
        let version =
            definition.version(INITIAL_VIEW_VERSION_ID, schema.schema_id(), storage_table);
        Ok(Self {
            name,
            schema,
            version,
            properties,
        })
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn into_parts(self) -> (String, Schema, ViewVersionWire, HashMap<String, String>) {
        (self.name, self.schema, self.version, self.properties)
    }
}

/// The schema a replacement's new version uses.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ViewSchema {
    /// A schema the view already holds; it is referenced by ID because the
    /// reference server adds nothing for it, so `-1` would select nothing.
    Existing(i32),
    /// A schema added by the same commit and selected by `-1`.
    New(Box<Schema>),
}

/// A replacement checked against the loaded view metadata.
#[derive(Clone, Debug)]
pub(crate) struct ValidatedViewReplace {
    ident: TableIdent,
    uuid: Uuid,
    schema: ViewSchema,
    version: ViewVersionWire,
    properties: HashMap<String, String>,
}

impl ValidatedViewReplace {
    /// Validate replacing `current` with `definition`, setting `properties`.
    pub(crate) fn validate(
        ident: TableIdent,
        current: &ViewMetadata,
        definition: ViewDefinition,
        properties: HashMap<String, String>,
    ) -> Result<Self, ConnectorMutationFailure> {
        if let Some(storage_table) = current.current_version().storage_table() {
            return Err(materialized_view_already_exists(storage_table));
        }
        let mut merged = current.properties().clone();
        merged.extend(properties.clone());
        validate_history_entries(&merged).map_err(invalid_replacement)?;
        if !merged
            .get(REPLACE_DROP_DIALECT_ALLOWED)
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            let current_dialects = current
                .current_version()
                .representations()
                .iter()
                .filter_map(ViewRepresentation::as_sql)
                .map(|sql| sql.dialect.to_ascii_lowercase())
                .collect::<BTreeSet<_>>();
            let new_dialects = BTreeSet::from([VIEW_DIALECT_TRINO.to_string()]);
            let dropped = current_dialects
                .difference(&new_dialects)
                .cloned()
                .collect::<Vec<_>>();
            if !dropped.is_empty() {
                let current_dialects = current_dialects.into_iter().collect::<Vec<_>>();
                let new_dialects = new_dialects.into_iter().collect::<Vec<_>>();
                return Err(invalid_replacement(format!(
                    "Iceberg view {ident} replacement would drop SQL dialects {dropped:?}: \
                     the current version has {current_dialects:?} and the new version has \
                     {new_dialects:?}; set {REPLACE_DROP_DIALECT_ALLOWED}=true to allow it"
                )));
            }
        }

        let (schema, schema_id) = match existing_schema_id(current, &definition.schema) {
            Some(id) => (ViewSchema::Existing(id), id),
            None => {
                let next_id = current
                    .schemas_iter()
                    .map(|schema| schema.schema_id())
                    .max()
                    .map_or(0, |id| id + 1);
                let schema = definition
                    .schema
                    .clone()
                    .into_builder()
                    .with_schema_id(next_id)
                    .build()
                    .map_err(|error| {
                        invalid_replacement(format!("build replaced Iceberg view schema: {error}"))
                    })?;
                (ViewSchema::New(Box::new(schema)), -1)
            }
        };
        let version_id = current
            .versions()
            .map(|version| version.version_id())
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                invalid_replacement(format!("Iceberg view {ident} version ID space exhausted"))
            })?;
        let mut definition = definition;
        definition.summary.insert(
            OPERATION_ID_SUMMARY_KEY.to_string(),
            Uuid::now_v7().to_string(),
        );
        Ok(Self {
            ident,
            uuid: current.uuid(),
            schema,
            version: definition.version(version_id, schema_id, None),
            properties,
        })
    }

    pub(crate) fn ident(&self) -> &TableIdent {
        &self.ident
    }

    pub(crate) fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub(crate) fn schema(&self) -> &ViewSchema {
        &self.schema
    }

    pub(crate) fn version(&self) -> &ViewVersionWire {
        &self.version
    }

    pub(crate) fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

pub(crate) fn materialized_view_already_exists(
    storage_table: &StorageTableIdentifier,
) -> ConnectorMutationFailure {
    ConnectorMutationFailure::new(
        ConnectorMutationFailureKind::AlreadyExists,
        "Iceberg materialized view already exists",
    )
    .with_existing_object(storage_table.materialized_view_kind())
}

fn invalid_replacement(message: String) -> ConnectorMutationFailure {
    ConnectorMutationFailure::new(ConnectorMutationFailureKind::InvalidRequest, message)
}

/// The view's schema the reference server would reuse for `schema`: equal
/// fields and the same identifier field set.
fn existing_schema_id(current: &ViewMetadata, schema: &Schema) -> Option<i32> {
    let identifiers = schema.identifier_field_ids().collect::<BTreeSet<_>>();
    current
        .schemas_iter()
        .filter(|existing| {
            existing.as_struct() == schema.as_struct()
                && existing.identifier_field_ids().collect::<BTreeSet<_>>() == identifiers
        })
        .map(|existing| existing.schema_id())
        .min()
}

/// `version.history.num-entries`, when present, must be a positive 32-bit
/// decimal integer, as the reference server parses it. Only ASCII digits are
/// accepted; the server also accepts other Unicode digits, so the local rule
/// errs on the strict side.
fn validate_history_entries(properties: &HashMap<String, String>) -> Result<(), String> {
    let Some(value) = properties.get(VERSION_HISTORY_NUM_ENTRIES) else {
        return Ok(());
    };
    let positive = !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.parse::<i32>().is_ok_and(|entries| entries > 0);
    if positive {
        Ok(())
    } else {
        Err(format!(
            "Iceberg view property {VERSION_HISTORY_NUM_ENTRIES} must be a positive 32-bit \
             integer, got `{value}`"
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::iceberg::spec::{NestedField, PrimitiveType, Type};
    use crate::rest_protocol::view_metadata::decode_test_metadata;

    fn columns(names: &[&str]) -> Schema {
        Schema::builder()
            .with_fields(
                names
                    .iter()
                    .enumerate()
                    .map(|(index, name)| {
                        Arc::new(NestedField::optional(
                            index as i32 + 1,
                            *name,
                            Type::Primitive(PrimitiveType::Long),
                        ))
                    })
                    .collect::<Vec<_>>(),
            )
            .build()
            .unwrap()
    }

    fn definition(schema: Schema, sql: &str) -> ViewDefinition {
        ViewDefinition {
            schema,
            sql: sql.to_string(),
            summary: HashMap::from([("engine-name".to_string(), "novarocks".to_string())]),
            default_catalog: Some("source_catalog".to_string()),
            default_namespace: NamespaceIdent::new("db".to_string()),
            timestamp_ms: 1_700_000_000_000,
        }
    }

    /// Metadata of view `db.v` whose versions 1..=n all use schema 0 (`a`,
    /// `b`); the current version is n and carries `dialects`.
    fn metadata(versions: i32, dialects: &[&str], properties: serde_json::Value) -> ViewMetadata {
        decode_test_metadata(metadata_json(versions, dialects, properties))
    }

    fn metadata_json(
        versions: i32,
        dialects: &[&str],
        properties: serde_json::Value,
    ) -> serde_json::Value {
        let version = |id: i32, dialects: &[&str]| {
            serde_json::json!({
                "version-id": id,
                "schema-id": 0,
                "timestamp-ms": 1_600_000_000_000i64 + id as i64,
                "summary": {"engine-name": "novarocks"},
                "representations": dialects.iter().map(|dialect| serde_json::json!({
                    "type": "sql", "sql": format!("select {id}"), "dialect": dialect
                })).collect::<Vec<_>>(),
                "default-namespace": ["db"],
            })
        };
        serde_json::json!({
            "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
            "format-version": 1,
            "location": "s3://warehouse/db/v",
            "current-version-id": versions,
            "versions": (1..=versions)
                .map(|id| version(id, if id == versions { dialects } else { &["trino"] }))
                .collect::<Vec<_>>(),
            "version-log": (1..=versions).map(|id| serde_json::json!({
                "version-id": id, "timestamp-ms": 1_600_000_000_000i64 + id as i64
            })).collect::<Vec<_>>(),
            "schemas": [serde_json::to_value(columns(&["a", "b"]).into_builder().with_schema_id(0).build().unwrap()).unwrap()],
            "properties": properties,
        })
    }

    fn ident() -> TableIdent {
        TableIdent::from_strs(["db", "v"]).unwrap()
    }

    fn replace(
        current: &ViewMetadata,
        schema: Schema,
        properties: &[(&str, &str)],
    ) -> Result<ValidatedViewReplace, ConnectorMutationFailure> {
        ValidatedViewReplace::validate(
            ident(),
            current,
            definition(schema, "select a, b from t2"),
            properties
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )
    }

    #[test]
    fn an_unchanged_schema_is_referenced_by_its_id() {
        let current = metadata(3, &["trino"], serde_json::json!({}));
        let replace = replace(&current, columns(&["a", "b"]), &[]).unwrap();
        assert_eq!(replace.schema(), &ViewSchema::Existing(0));
        assert_eq!(replace.version().schema_id, 0);
        assert_eq!(replace.uuid(), current.uuid());
    }

    #[test]
    fn a_changed_schema_is_added_and_selected_by_the_commit() {
        let current = metadata(1, &["trino"], serde_json::json!({}));
        let replace = replace(&current, columns(&["a", "c"]), &[]).unwrap();
        let ViewSchema::New(schema) = replace.schema() else {
            panic!("a changed schema must be added");
        };
        assert_eq!(schema.schema_id(), 1);
        assert_eq!(replace.version().schema_id, -1);
    }

    #[test]
    fn ordinary_replacement_validation_refuses_a_current_storage_table() {
        let mut json = metadata_json(1, &["trino"], serde_json::json!({}));
        json["versions"][0]["storage-table"] =
            serde_json::json!({"namespace":["db"], "name":"storage"});
        let error = replace(&decode_test_metadata(json), columns(&["a", "b"]), &[]).unwrap_err();
        assert_eq!(error.kind(), ConnectorMutationFailureKind::AlreadyExists);
        assert!(
            matches!(error.existing_object(), Some(novarocks_spi::connector::ConnectorObjectKind::MaterializedView { storage_table }) if storage_table.name.as_ref() == "storage")
        );
    }

    #[test]
    fn identifier_fields_take_part_in_schema_equality() {
        let mut json = metadata_json(1, &["trino"], serde_json::json!({}));
        json["schemas"][0]["fields"][0]["required"] = true.into();
        let current = decode_test_metadata(json);
        let fields = || {
            vec![
                Arc::new(NestedField::required(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "b",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ]
        };
        let plain = Schema::builder().with_fields(fields()).build().unwrap();
        assert_eq!(
            replace(&current, plain, &[]).unwrap().schema(),
            &ViewSchema::Existing(0)
        );
        let identified = Schema::builder()
            .with_fields(fields())
            .with_identifier_field_ids([1])
            .build()
            .unwrap();
        assert!(matches!(
            replace(&current, identified, &[]).unwrap().schema(),
            ViewSchema::New(_)
        ));
    }

    #[test]
    fn every_replacement_adds_a_version_with_a_fresh_operation_identity() {
        let current = metadata(4, &["trino"], serde_json::json!({}));
        let first = replace(&current, columns(&["a", "b"]), &[]).unwrap();
        let second = replace(&current, columns(&["a", "b"]), &[]).unwrap();
        assert_eq!(first.version().version_id, 5);
        let operation = |replace: &ValidatedViewReplace| {
            replace
                .version()
                .summary
                .get(OPERATION_ID_SUMMARY_KEY)
                .cloned()
                .expect("operation identity")
        };
        assert_ne!(operation(&first), operation(&second));
        assert_eq!(
            first
                .version()
                .summary
                .get("engine-name")
                .map(String::as_str),
            Some("novarocks")
        );
        assert_eq!(
            first.version().representations,
            vec![SqlRepresentationWire {
                sql: "select a, b from t2".to_string(),
                dialect: VIEW_DIALECT_TRINO.to_string(),
            }]
        );
    }

    #[test]
    fn dropping_a_dialect_is_refused_unless_allowed() {
        let current = metadata(2, &["trino", "Spark"], serde_json::json!({}));
        let error = replace(&current, columns(&["a", "b"]), &[]).unwrap_err();
        assert!(error.message().contains("[\"spark\"]"), "{error}");
        assert!(
            error.message().contains("[\"spark\", \"trino\"]"),
            "{error}"
        );
        assert!(
            error.message().contains(REPLACE_DROP_DIALECT_ALLOWED),
            "{error}"
        );

        for allowed in ["true", "TRUE", "True"] {
            assert!(
                replace(
                    &current,
                    columns(&["a", "b"]),
                    &[(REPLACE_DROP_DIALECT_ALLOWED, allowed)]
                )
                .is_ok(),
                "{allowed}"
            );
        }
        // The reference server reads only a case-insensitive `true`.
        for not_allowed in ["1", "on", "t", "yes"] {
            assert!(
                replace(
                    &current,
                    columns(&["a", "b"]),
                    &[(REPLACE_DROP_DIALECT_ALLOWED, not_allowed)]
                )
                .is_err(),
                "{not_allowed}"
            );
        }
        // An existing property counts as well as one set by this statement.
        let allowing = metadata(
            2,
            &["trino", "spark"],
            serde_json::json!({REPLACE_DROP_DIALECT_ALLOWED: "true"}),
        );
        assert!(replace(&allowing, columns(&["a", "b"]), &[]).is_ok());
        // Retaining every dialect needs no permission.
        let trino_only = metadata(2, &["TrInO"], serde_json::json!({}));
        assert!(replace(&trino_only, columns(&["a", "b"]), &[]).is_ok());
        let retired_only = metadata(2, &["starrocks"], serde_json::json!({}));
        let error = replace(&retired_only, columns(&["a", "b"]), &[]).unwrap_err();
        assert!(
            error
                .message()
                .contains("would drop SQL dialects [\"starrocks\"]"),
            "{error}"
        );
    }

    #[test]
    fn version_history_entries_must_be_a_positive_32_bit_integer() {
        let current = metadata(1, &["trino"], serde_json::json!({}));
        for invalid in ["0", "-1", "abc", "", "+3", "2147483648", "1.5", "٣"] {
            assert!(
                replace(
                    &current,
                    columns(&["a", "b"]),
                    &[(VERSION_HISTORY_NUM_ENTRIES, invalid)]
                )
                .is_err(),
                "replace {invalid:?}"
            );
            assert!(
                ViewCreate::validate(
                    "v".to_string(),
                    definition(columns(&["a"]), "select 1"),
                    HashMap::from([(VERSION_HISTORY_NUM_ENTRIES.to_string(), invalid.to_string())]),
                    None,
                )
                .is_err(),
                "create {invalid:?}"
            );
        }
        for valid in ["1", "10", "2147483647"] {
            assert!(
                replace(
                    &current,
                    columns(&["a", "b"]),
                    &[(VERSION_HISTORY_NUM_ENTRIES, valid)]
                )
                .is_ok()
            );
        }
        // An invalid value already on the view is refused as well.
        let invalid_on_view = metadata(
            1,
            &["trino"],
            serde_json::json!({VERSION_HISTORY_NUM_ENTRIES: "0"}),
        );
        assert!(replace(&invalid_on_view, columns(&["a", "b"]), &[]).is_err());
    }

    #[test]
    fn a_creation_states_its_first_version() {
        let creation = ViewCreate::validate(
            "v".to_string(),
            definition(columns(&["a"]), "select 1"),
            HashMap::from([("comment".to_string(), "c".to_string())]),
            None,
        )
        .unwrap();
        assert_eq!(creation.name(), "v");
        let (_, schema, version, properties) = creation.into_parts();
        assert_eq!(version.version_id, 1);
        assert_eq!(version.schema_id, schema.schema_id());
        assert!(!version.summary.contains_key(OPERATION_ID_SUMMARY_KEY));
        assert_eq!(properties.get("comment").map(String::as_str), Some("c"));
    }
}
