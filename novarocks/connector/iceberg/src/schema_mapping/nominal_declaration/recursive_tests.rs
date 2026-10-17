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
use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorDataType as D, ConnectorStructField,
};
use novarocks_type_contract::ValueLogicalType;
use std::sync::Arc;

pub(crate) fn columns() -> Vec<ConnectorColumnDefinition> {
    let child = |name: &str, data_type| ConnectorStructField {
        name: name.into(),
        data_type,
        nullable: true,
    };
    vec![ConnectorColumnDefinition {
        name: "payload".into(),
        data_type: D::Struct(vec![
            child("items", D::Array(Box::new(D::LargeInt))),
            child("state", D::Hll),
            child(
                "mapping",
                D::Map(Box::new(D::LargeInt), Box::new(D::Bitmap)),
            ),
            child("ordinary", D::Binary),
        ]),
        nullable: true,
        aggregation: None,
        default: None,
    }]
}
pub(crate) fn metadata() -> TableMetadata {
    let columns = columns();
    let schema = Schema::builder()
        .with_fields(crate::catalog_control::type_mapping::schema_fields(&columns).unwrap())
        .build()
        .unwrap();
    let declarations = from_columns(&schema, &columns).unwrap();
    let raw = encode(&declarations).unwrap();
    let metadata = TableMetadataBuilder::new(
        schema.clone(),
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "memory://recursive-nominals".into(),
        FormatVersion::V3,
        HashMap::from([(PROPERTY.into(), raw)]),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    // This is the actual SDK create normalization, not a copied ID algorithm.
    assert_eq!(metadata.current_schema().as_struct(), schema.as_struct());
    assert_eq!(from_metadata(&metadata).unwrap(), declarations);
    metadata
}

#[test]
fn recursive_declarations_survive_sdk_create_and_author_complete_values() {
    let metadata = metadata();
    let schema = metadata.current_schema();
    let declarations = from_metadata(&metadata).unwrap();
    assert_eq!(declarations.len(), 4);
    assert_eq!(
        restrict_to_schema(schema, &declarations).unwrap(),
        declarations
    );
    validate_frozen_schema(schema, &declarations).unwrap();
    let mut request = |_: std::alloc::Layout| Ok(());
    let mut work = |_: usize| Ok(());
    let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut work,
    );
    let public =
        crate::schema_mapping::catalog_value::table(&metadata, schema, &[], &mut creation).unwrap();
    let root = public.binding(0).unwrap();
    assert_eq!(
        root.child_at(0)
            .unwrap()
            .child_at(0)
            .unwrap()
            .logical_type(),
        ValueLogicalType::LargeInt
    );
    assert_eq!(
        root.child_at(1).unwrap().logical_type(),
        ValueLogicalType::Hll
    );
    let map = root.child_at(2).unwrap();
    assert_eq!(
        map.child_at(0).unwrap().logical_type(),
        ValueLogicalType::LargeInt
    );
    assert_eq!(
        map.child_at(1).unwrap().logical_type(),
        ValueLogicalType::Bitmap
    );
    assert!(!map.child_at(0).unwrap().nullable());
    assert_eq!(
        root.child_at(3).unwrap().logical_type(),
        ValueLogicalType::Physical
    );
    let arrow = crate::scalar_integer_domain::metadata_sql_schema(&metadata, schema).unwrap();
    let arrow::datatypes::DataType::Struct(fields) = arrow.field(0).data_type() else {
        panic!("struct");
    };
    let arrow::datatypes::DataType::List(item) = fields[0].data_type() else {
        panic!("list");
    };
    assert_eq!(
        novarocks_type_contract::field_logical_type(item).unwrap(),
        ValueLogicalType::LargeInt
    );
    assert_eq!(
        novarocks_type_contract::field_logical_type(&fields[3]).unwrap(),
        ValueLogicalType::Physical
    );
}

#[test]
fn nominal_documents_reject_corruption_and_conflicting_legacy_claims() {
    for raw in [
        r#"{"version":2,"fields":[]}"#,
        r#"{"version":1,"fields":[{"id":1,"domain":"largeint"},{"id":1,"domain":"largeint"}]}"#,
        r#"{"version":1,"fields":[{"id":0,"domain":"largeint"}]}"#,
        r#"{"version":1,"fields":[{"id":1,"domain":"uuid"}]}"#,
        r#"{"version":1,"fields":[],"extra":true}"#,
        r#"{"version":1,"fields":[{"id":1,"domain":"largeint","extra":true}]}"#,
    ] {
        assert!(decode(raw).is_err(), "{raw}");
    }
    let schema = Schema::builder()
        .with_fields([Arc::new(NestedField::optional(
            1,
            "state",
            Type::Primitive(PrimitiveType::Binary),
        ))])
        .build()
        .unwrap();
    let mut properties = HashMap::from([
        (
            PROPERTY.into(),
            encode(&BTreeMap::from([(1, NominalSourceDomain::Hll)])).unwrap(),
        ),
        ("novarocks.logical_type.state".into(), "bitmap".into()),
    ]);
    assert!(from_properties(&schema, &properties).is_err());
    properties.insert("novarocks.logical_type.state".into(), "hll".into());
    assert_eq!(
        from_properties(&schema, &properties).unwrap().get(&1),
        Some(&NominalSourceDomain::Hll)
    );
    properties.insert("novarocks.nominal-types.v2".into(), "{}".into());
    assert!(from_properties(&schema, &properties).is_err());
}

#[test]
fn nominal_metadata_refuses_unknown_ids_and_nested_storage_conflicts() {
    let metadata = metadata();
    let mut raw = serde_json::to_value(&metadata).unwrap();
    raw["properties"][PROPERTY] = serde_json::Value::String(
        encode(&BTreeMap::from([(12345, NominalSourceDomain::Hll)])).unwrap(),
    );
    let invalid: TableMetadata = serde_json::from_value(raw).unwrap();
    assert!(from_metadata(&invalid).is_err());
    let declarations = from_metadata(&metadata).unwrap();
    let id = *declarations
        .iter()
        .find(|(_, domain)| **domain == NominalSourceDomain::LargeInt)
        .unwrap()
        .0;
    let invalid = Schema::builder()
        .with_fields([Arc::new(NestedField::optional(
            id,
            "wrong",
            Type::Primitive(PrimitiveType::Binary),
        ))])
        .build()
        .unwrap();
    assert!(validate_schema(&invalid, &declarations).is_err());
    let mut history = serde_json::to_value(&metadata).unwrap();
    let mut conflicting = serde_json::to_value(&invalid).unwrap();
    conflicting["schema-id"] = serde_json::json!(123);
    history["schemas"].as_array_mut().unwrap().push(conflicting);
    let history: TableMetadata = serde_json::from_value(history).unwrap();
    assert!(
        from_metadata(&history).is_err(),
        "a historical occurrence with the same ID must validate"
    );
    let missing = Schema::builder()
        .with_fields([Arc::new(NestedField::optional(
            12345,
            "ordinary",
            Type::Primitive(PrimitiveType::Fixed(16)),
        ))])
        .build()
        .unwrap();
    assert!(
        restrict_to_schema(&missing, &declarations)
            .unwrap()
            .is_empty()
    );
    assert!(validate_frozen_schema(&missing, &declarations).is_err());
    // Matching physical width never establishes a domain without a declaration.
    assert!(
        from_properties(&missing, &HashMap::new())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn recursive_arrow_projection_rejects_undeclared_and_conflicting_nominal_annotations() {
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
    let foreign = |field: Field, logical: &str| {
        field.with_metadata(HashMap::from([(
            NR_LOGICAL_TYPE_KEY.into(),
            logical.into(),
        )]))
    };
    let plain = Arc::new(NestedField::optional(
        1,
        "plain",
        Type::Primitive(PrimitiveType::Fixed(16)),
    ));
    let schema = Schema::builder()
        .with_fields([plain.clone()])
        .build()
        .unwrap();
    for logical in ["largeint", "invalid"] {
        let arrow = Arc::new(ArrowSchema::new(vec![foreign(
            Field::new("plain", DataType::FixedSizeBinary(16), true),
            logical,
        )]));
        assert!(apply_to_arrow(arrow, &schema, &BTreeMap::new()).is_err());
    }
    let schema = Schema::builder()
        .with_fields([Arc::new(NestedField::optional(
            2,
            "outer",
            Type::Struct(crate::iceberg::spec::StructType::new(vec![plain])),
        ))])
        .build()
        .unwrap();
    let child = Arc::new(foreign(
        Field::new("plain", DataType::FixedSizeBinary(16), true),
        "largeint",
    ));
    let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
        "outer",
        DataType::Struct(vec![child].into()),
        true,
    )]));
    assert!(apply_to_arrow(arrow, &schema, &BTreeMap::new()).is_err());
    let schema = Schema::builder()
        .with_fields([Arc::new(NestedField::optional(
            1,
            "state",
            Type::Primitive(PrimitiveType::Binary),
        ))])
        .build()
        .unwrap();
    let arrow = Arc::new(ArrowSchema::new(vec![foreign(
        Field::new("state", DataType::Binary, true),
        "hll",
    )]));
    assert!(apply_to_arrow(arrow, &schema, &BTreeMap::new()).is_err());
    let arrow = Arc::new(ArrowSchema::new(vec![foreign(
        Field::new("state", DataType::Binary, true),
        "bitmap",
    )]));
    assert!(
        apply_to_arrow(
            arrow,
            &schema,
            &BTreeMap::from([(1, NominalSourceDomain::Hll)])
        )
        .is_err()
    );
}
