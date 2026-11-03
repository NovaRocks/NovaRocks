// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::iceberg::spec::{
    FormatVersion, NestedField, PartitionSpec, SortOrder, TableMetadataBuilder,
};
use crate::scalar_integer_domain::ScalarIntegerDomain;
use novarocks_spi::connector::{ConnectorColumnPath, ConnectorSchemaChange};
use std::sync::Arc;

fn metadata() -> TableMetadata {
    let schema = Schema::builder()
        .with_fields(
            [
                (1, "h", PrimitiveType::Binary),
                (2, "b", PrimitiveType::Binary),
                (3, "n", PrimitiveType::Fixed(16)),
                (4, "t", PrimitiveType::Int),
                (5, "s", PrimitiveType::Int),
                (6, "plain", PrimitiveType::Binary),
            ]
            .into_iter()
            .map(|(id, name, ty)| Arc::new(NestedField::optional(id, name, ty.into()))),
        )
        .build()
        .unwrap();
    let document = TableNominalDocument {
        nominals: BTreeMap::from([
            (1, NominalSourceDomain::Hll),
            (2, NominalSourceDomain::Bitmap),
            (3, NominalSourceDomain::LargeInt),
        ]),
        integers: BTreeMap::from([
            (4, ScalarIntegerDomain::Int8),
            (5, ScalarIntegerDomain::Int16),
        ]),
    };
    TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "memory://unified-domains".into(),
        FormatVersion::V3,
        HashMap::from([(PROPERTY.into(), document.encode().unwrap())]),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata
}
fn loaded(raw: serde_json::Value) -> TableMetadata {
    serde_json::from_value(raw).unwrap()
}

#[test]
fn unified_five_domains_have_one_document_and_two_exact_views() {
    let metadata = metadata();
    let document = TableNominalDocument::from_metadata(&metadata).unwrap();
    assert_eq!(document.nominals.len(), 3);
    assert_eq!(document.integers.len(), 2);
    assert_eq!(document.encode().unwrap(), metadata.properties()[PROPERTY]);
    let arrow = crate::scalar_integer_domain::sql_schema_from_document(
        metadata.current_schema(),
        &document,
    )
    .unwrap();
    assert_eq!(
        arrow.field(3).data_type(),
        &arrow::datatypes::DataType::Int8
    );
    assert_eq!(
        arrow.field(4).data_type(),
        &arrow::datatypes::DataType::Int16
    );
    assert_eq!(
        novarocks_type_contract::field_logical_type(arrow.field(2)).unwrap(),
        ValueLogicalType::LargeInt
    );
}

#[test]
fn external_rename_and_drop_name_reuse_follow_only_stable_ids() {
    let metadata = metadata();
    let mut raw = serde_json::to_value(&metadata).unwrap();
    let fields = raw["schemas"][0]["fields"].as_array_mut().unwrap();
    fields[0]["name"] = serde_json::json!("renamed");
    fields.retain(|field| field["id"] != serde_json::json!(2));
    fields.push(serde_json::json!({"id":7,"name":"b","required":false,"type":"binary"}));
    raw["last-column-id"] = serde_json::json!(7);
    let external = loaded(raw);
    let document = TableNominalDocument::from_metadata(&external).unwrap();
    assert_eq!(document.nominals.get(&1), Some(&NominalSourceDomain::Hll));
    assert!(!document.nominals.contains_key(&2));
    assert!(!document.nominals.contains_key(&7));
    let arrow = crate::scalar_integer_domain::sql_schema_from_document(
        external.current_schema(),
        &document,
    )
    .unwrap();
    assert_eq!(arrow.field(0).name(), "renamed");
    assert_eq!(
        novarocks_type_contract::field_logical_type(arrow.field(0)).unwrap(),
        ValueLogicalType::Hll
    );
    assert_eq!(
        novarocks_type_contract::field_logical_type(arrow.fields().last().unwrap()).unwrap(),
        ValueLogicalType::Physical
    );
}

#[test]
fn removed_history_orphan_is_ignored_and_pruned_by_next_schema_document_write() {
    let mut raw = serde_json::to_value(metadata()).unwrap();
    raw["schemas"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .retain(|field| field["id"] != serde_json::json!(3));
    let external = loaded(raw);
    let document = TableNominalDocument::from_metadata(&external).unwrap();
    assert!(!document.nominals.contains_key(&3));
    assert!(external.properties()[PROPERTY].contains("\"id\":3"));
    let prepared = crate::catalog_control::catalog_mutation::prepare_schema_updates(
        &external,
        &ConnectorSchemaChange::RenameColumn {
            path: ConnectorColumnPath {
                segments: vec!["plain".into()],
            },
            to: "ordinary".into(),
        },
    )
    .unwrap();
    let mut builder = external.into_builder(None);
    for update in prepared.updates {
        builder = update.apply(builder).unwrap();
    }
    let written = builder.build().unwrap().metadata;
    assert!(!written.properties()[PROPERTY].contains("\"id\":3"));
    assert_eq!(
        TableNominalDocument::from_metadata(&written)
            .unwrap()
            .nominals
            .len(),
        2
    );
}

#[test]
fn beyond_high_water_and_each_historical_storage_conflict_report_the_exact_id() {
    let metadata = metadata();
    let mut raw = serde_json::to_value(&metadata).unwrap();
    raw["properties"][PROPERTY] =
        serde_json::json!(r#"{"version":1,"fields":[{"id":7,"domain":"bitmap"}]}"#);
    let error = TableNominalDocument::from_metadata(&loaded(raw)).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(error.message().contains("7") && error.message().contains("last-column-id"));
    for (id, ty) in [(3, "binary"), (4, "long"), (5, "long")] {
        let mut raw = serde_json::to_value(&metadata).unwrap();
        let mut history = raw["schemas"][0].clone();
        history["schema-id"] = serde_json::json!(10);
        history["fields"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|field| field["id"] == serde_json::json!(id))
            .unwrap()["type"] = serde_json::json!(ty);
        raw["schemas"].as_array_mut().unwrap().push(history);
        let error = TableNominalDocument::from_metadata(&loaded(raw)).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(
            error.message().contains(&format!("field ID {id}")),
            "{}",
            error.message()
        );
    }
}

#[test]
fn external_replace_same_id_storage_retains_domain_and_changed_storage_refuses() {
    let metadata = metadata();
    let mut raw = serde_json::to_value(&metadata).unwrap();
    raw["schemas"][0]["fields"][0]["name"] = serde_json::json!("replacement");
    let replacement = loaded(raw.clone());
    assert_eq!(
        TableNominalDocument::from_metadata(&replacement)
            .unwrap()
            .nominals
            .get(&1),
        Some(&NominalSourceDomain::Hll)
    );
    raw["schemas"][0]["fields"][0]["type"] = serde_json::json!("string");
    let error = TableNominalDocument::from_metadata(&loaded(raw)).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(error.message().contains("field ID 1"));
}

#[test]
fn tiny_small_remain_top_level_and_document_limits_are_resource_errors() {
    let nested = Schema::builder()
        .with_fields([Arc::new(NestedField::optional(
            1,
            "root",
            Type::Struct(crate::iceberg::spec::StructType::new(vec![Arc::new(
                NestedField::optional(2, "child", PrimitiveType::Int.into()),
            )])),
        ))])
        .build()
        .unwrap();
    let properties = HashMap::from([(
        PROPERTY.into(),
        r#"{"version":1,"fields":[{"id":2,"domain":"tinyint"}]}"#.into(),
    )]);
    assert!(
        TableNominalDocument::from_properties_checked::<ConnectorError>(
            &nested,
            &properties,
            &|_| Ok(())
        )
        .unwrap_err()
        .message()
        .contains("field ID 2")
    );
    let raw = format!(
        r#"{{"version":1,"fields":[{}]}}"#,
        (1..=MAX_FIELDS + 1)
            .map(|id| format!(r#"{{"id":{id},"domain":"hll"}}"#))
            .collect::<Vec<_>>()
            .join(",")
    );
    assert_eq!(
        decode(&raw).unwrap_err().kind(),
        ConnectorErrorKind::ResourceExhausted
    );
    assert_eq!(
        decode(&"x".repeat(MAX_DECLARATION_BYTES + 1))
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::ResourceExhausted
    );
}

#[test]
fn planning_semantic_kind_follows_external_rename_by_source_id() {
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorRequestContext, ConnectorStopOwner,
        ConnectorTableColumnSemanticKind,
    };
    let mut raw = serde_json::to_value(metadata()).unwrap();
    raw["schemas"][0]["fields"][0]["name"] = serde_json::json!("renamed_hll");
    raw["schemas"][0]["fields"][1]["name"] = serde_json::json!("renamed_bitmap");
    let metadata = loaded(raw);
    let document = TableNominalDocument::from_metadata(&metadata).unwrap();
    let schema = crate::scalar_integer_domain::sql_schema_from_document(
        metadata.current_schema(),
        &document,
    )
    .unwrap();
    let context = ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        ConnectorStopOwner::new().view(),
        1024 * 1024,
        4 * 1024 * 1024,
    )
    .unwrap();
    let namespace = Arc::from("db");
    let instance = ConnectorInstanceId::parse("ice").unwrap();
    let facts = crate::planning_facts::table_planning_facts(
        crate::planning_facts::IcebergTablePlanningFactsInput {
            schema: &schema,
            iceberg_schema: Some(metadata.current_schema()),
            metadata_columns: &[],
            hidden_columns: &[],
            nominal_declarations: &document.nominals,
            table_metadata: Some(&metadata),
            namespace: &namespace,
            instance_id: &instance,
            context: &context,
        },
    )
    .unwrap();
    assert_eq!(schema.field(0).name(), "renamed_hll");
    assert_eq!(
        facts.column_facts()[0].semantic_kind(),
        ConnectorTableColumnSemanticKind::Hll
    );
    assert_eq!(
        facts.column_facts()[1].semantic_kind(),
        ConnectorTableColumnSemanticKind::Bitmap
    );
    assert_eq!(
        facts.column_facts()[5].semantic_kind(),
        ConnectorTableColumnSemanticKind::None
    );
}

#[test]
fn declared_smallint_modify_refuses_other_domains_before_publication() {
    use novarocks_spi::connector::ConnectorDataType as D;
    let metadata = metadata();
    let change = |data_type| ConnectorSchemaChange::ModifyColumn {
        path: ConnectorColumnPath {
            segments: vec!["s".into()],
        },
        data_type,
    };
    assert!(
        crate::catalog_control::catalog_mutation::prepare_schema_updates(
            &metadata,
            &change(D::SmallInt)
        )
        .unwrap()
        .updates
        .is_empty()
    );
    for target in [D::TinyInt, D::Int, D::BigInt, D::Hll, D::LargeInt] {
        let error = crate::catalog_control::catalog_mutation::prepare_schema_updates(
            &metadata,
            &change(target),
        )
        .err()
        .unwrap();
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(
            error.message(),
            "Iceberg MODIFY cannot change a nominal scalar integer domain without rewriting stored values"
        );
    }
}

#[test]
fn ordinary_properties_cannot_author_any_nominal_domain() {
    let mut raw = serde_json::to_value(metadata()).unwrap();
    let properties = raw["properties"].as_object_mut().unwrap();
    properties.remove(PROPERTY);
    properties.insert("novarocks.logical_type.h".into(), serde_json::json!("hll"));
    properties.insert(
        "novarocks.logical_type.t".into(),
        serde_json::json!("tinyint"),
    );
    properties.insert(
        "novarocks.scalar_integer_domains.v1".into(),
        serde_json::json!("ordinary invalid JSON text"),
    );
    let metadata = loaded(raw);
    let document = TableNominalDocument::from_metadata(&metadata).unwrap();
    assert!(document.is_empty());
    let arrow = crate::scalar_integer_domain::sql_schema_from_document(
        metadata.current_schema(),
        &document,
    )
    .unwrap();
    assert_eq!(
        novarocks_type_contract::field_logical_type(arrow.field(0)).unwrap(),
        ValueLogicalType::Physical
    );
    assert_eq!(
        arrow.field(3).data_type(),
        &arrow::datatypes::DataType::Int32
    );
}
