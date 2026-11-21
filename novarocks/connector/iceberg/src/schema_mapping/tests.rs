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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::{
    ICEBERG_INITIAL_DEFAULT_META_KEY, annotate_read_schema_from_scan_model,
    apply_scan_field_id_recursive, canonical_name_mapping, sql_read_field,
    sql_read_schema_from_iceberg,
};
use crate::iceberg::spec::{
    ListType, MapType, NestedField, PrimitiveType, Schema as IcebergSchema, StructType, Type,
};
use crate::scan_model::IcebergSchemaFieldDef;

#[test]
fn canonical_name_mapping_is_strict_and_provider_owned() {
    assert_eq!(
        canonical_name_mapping(r#"[{"names":["legacy_id"],"field-id":1}]"#)
            .expect("canonical mapping"),
        r#"[{"field-id":1,"names":["legacy_id"]}]"#,
    );
    assert!(
        canonical_name_mapping(r#"[{"field-id":1,"names":["id"],"credential":"secret"}]"#).is_err()
    );
    assert!(
        canonical_name_mapping(
            r#"[
            {"field-id":1,"names":["left"],"fields":[{"field-id":2,"names":["id"]}]},
            {"field-id":3,"names":["right"],"fields":[{"field-id":4,"names":["id"]}]}
        ]"#
        )
        .is_ok()
    );
}

#[test]
fn metadata_value_struct_annotation_preserves_real_child_ids() {
    let source = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::optional(
                1,
                "payload",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(2, "metadata", Type::Primitive(PrimitiveType::Binary))
                        .into(),
                    NestedField::optional(3, "value", Type::Primitive(PrimitiveType::Binary))
                        .into(),
                ])),
            )
            .into(),
        ])
        .build()
        .unwrap();
    let arrow = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
        "payload",
        DataType::Struct(
            vec![
                Field::new("metadata", DataType::Binary, true),
                Field::new("value", DataType::Binary, true),
            ]
            .into(),
        ),
        true,
    )]));
    let frozen = crate::schema_facts::iceberg_schema_def(&source);
    let output = annotate_read_schema_from_scan_model(&arrow, &frozen).unwrap();
    let DataType::Struct(children) = output.field(0).data_type() else {
        panic!("struct");
    };
    assert_eq!(children[0].metadata()[PARQUET_FIELD_ID_META_KEY], "2");
    assert_eq!(children[1].metadata()[PARQUET_FIELD_ID_META_KEY], "3");
}

#[test]
fn frozen_variant_annotation_keeps_physical_children_opaque() {
    let field = Field::new(
        "v",
        DataType::Struct(
            vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("value", DataType::LargeBinary, false),
            ]
            .into(),
        ),
        true,
    );
    let frozen = IcebergSchemaFieldDef {
        is_variant: true,
        field_id: 7,
        name: "v".to_string(),
        initial_default: None,
        write_default: None,
        initial_default_json: None,
        write_default_json: None,
        children: Vec::new(),
    };

    let annotated = apply_scan_field_id_recursive(&field, &frozen).expect("annotate variant field");
    assert_eq!(
        annotated.metadata().get(PARQUET_FIELD_ID_META_KEY),
        Some(&"7".to_string())
    );
    assert_eq!(annotated.data_type(), field.data_type());
    let DataType::Struct(children) = annotated.data_type() else {
        panic!("variant remains a struct carrier");
    };
    assert!(children.iter().all(|child| child.metadata().is_empty()));
    assert_eq!(children.len(), 2);
}

#[test]
fn real_storage_schema_authors_uuid_variant_and_distinct_fixed_domains() {
    use novarocks_type_contract::{ValueLogicalType, field_logical_type};
    let iceberg = IcebergSchema::builder()
        .with_schema_id(8)
        .with_fields(vec![
            NestedField::required(11, "uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
            NestedField::optional(12, "fixed", Type::Primitive(PrimitiveType::Fixed(16))).into(),
            NestedField::optional(
                13,
                "variant",
                Type::Variant(crate::iceberg::spec::VariantType),
            )
            .into(),
            NestedField::required(14, "binary", Type::Primitive(PrimitiveType::Binary)).into(),
        ])
        .build()
        .unwrap();
    let schema = sql_read_schema_from_iceberg(&iceberg).unwrap();
    for (ordinal, carrier, logical, nullable, id) in [
        (
            0,
            DataType::FixedSizeBinary(16),
            ValueLogicalType::Uuid,
            false,
            11,
        ),
        (
            1,
            DataType::FixedSizeBinary(16),
            ValueLogicalType::Physical,
            true,
            12,
        ),
        (
            2,
            DataType::LargeBinary,
            ValueLogicalType::Variant,
            true,
            13,
        ),
        (3, DataType::Binary, ValueLogicalType::Physical, false, 14),
    ] {
        let field = schema.field(ordinal);
        assert_eq!(field.data_type(), &carrier);
        assert_eq!(field.is_nullable(), nullable);
        assert_eq!(field_logical_type(field).unwrap(), logical);
        assert_eq!(
            field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&id.to_string())
        );
    }
}

#[test]
fn recursive_storage_fields_preserve_uuid_and_variant_in_read_schema() {
    use novarocks_type_contract::{ValueLogicalType, field_logical_type};
    let iceberg = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::required(
                1,
                "record",
                Type::Struct(StructType::new(vec![
                    Arc::new(NestedField::required(
                        2,
                        "identity",
                        Type::Primitive(PrimitiveType::Uuid),
                    )),
                    Arc::new(NestedField::optional(
                        3,
                        "payload",
                        Type::Variant(crate::iceberg::spec::VariantType),
                    )),
                ])),
            )
            .into(),
            NestedField::optional(
                4,
                "identities",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    5,
                    Type::Primitive(PrimitiveType::Uuid),
                    false,
                )))),
            )
            .into(),
            NestedField::optional(
                6,
                "objects",
                Type::Map(MapType::new(
                    Arc::new(NestedField::map_key_element(
                        7,
                        Type::Primitive(PrimitiveType::Uuid),
                    )),
                    Arc::new(NestedField::map_value_element(
                        8,
                        Type::Variant(crate::iceberg::spec::VariantType),
                        true,
                    )),
                )),
            )
            .into(),
        ])
        .build()
        .unwrap();
    let schema = sql_read_schema_from_iceberg(&iceberg).unwrap();
    let DataType::Struct(record) = schema.field(0).data_type() else {
        panic!("record")
    };
    let DataType::List(items) = schema.field(1).data_type() else {
        panic!("items")
    };
    let DataType::Map(entries, _) = schema.field(2).data_type() else {
        panic!("map")
    };
    let DataType::Struct(map) = entries.data_type() else {
        panic!("entries")
    };
    for (field, logical, id) in [
        (record[0].as_ref(), ValueLogicalType::Uuid, 2),
        (record[1].as_ref(), ValueLogicalType::Variant, 3),
        (items.as_ref(), ValueLogicalType::Uuid, 5),
        (map[0].as_ref(), ValueLogicalType::Uuid, 7),
        (map[1].as_ref(), ValueLogicalType::Variant, 8),
    ] {
        assert_eq!(field_logical_type(field).unwrap(), logical);
        assert_eq!(
            field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&id.to_string())
        );
    }
    assert!(!record[0].is_nullable());
    assert!(record[1].is_nullable());
    assert!(items.is_nullable());
    // The frozen Iceberg Map declares a required key independently of its
    // generic Arrow projection; execution must retain that declaration.
    assert!(!map[0].is_nullable());
    assert!(!map[1].is_nullable());
    let frozen = crate::schema_facts::iceberg_schema_def(&iceberg);
    let annotated = annotate_read_schema_from_scan_model(&schema, &frozen).unwrap();
    assert!(novarocks_type_contract::arrow_schemas_exact(
        &schema, &annotated
    ));
}

#[test]
fn authored_logical_metadata_conflicts_and_wrong_uuid_carriers_fail() {
    use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, ValueLogicalType, field_logical_type};
    let uuid = NestedField::optional(11, "u", Type::Primitive(PrimitiveType::Uuid));
    for field in [
        Field::new("u", DataType::Utf8, true),
        Field::new("u", DataType::FixedSizeBinary(15), true),
        Field::new("u", DataType::FixedSizeBinary(16), true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                NR_LOGICAL_TYPE_KEY.into(),
                "largeint".into(),
            )]),
        ),
        Field::new("u", DataType::FixedSizeBinary(16), true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                NR_LOGICAL_TYPE_KEY.into(),
                "unknown".into(),
            )]),
        ),
    ] {
        assert!(sql_read_field(&field, &uuid).is_err());
    }
    let valid = Field::new("u", DataType::FixedSizeBinary(16), true).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            NR_LOGICAL_TYPE_KEY.into(),
            "uuid".into(),
        )]),
    );
    assert_eq!(
        field_logical_type(&sql_read_field(&valid, &uuid).unwrap()).unwrap(),
        ValueLogicalType::Uuid
    );
    let fixed = NestedField::optional(12, "u", Type::Primitive(PrimitiveType::Fixed(16)));
    assert!(sql_read_field(&valid, &fixed).is_err());
    let binary = NestedField::optional(13, "u", Type::Primitive(PrimitiveType::Binary));
    let forged_variant =
        Field::new("u", DataType::LargeBinary, true).with_metadata(std::collections::HashMap::<
            String,
            String,
        >::from([(
            NR_LOGICAL_TYPE_KEY.into(),
            "variant".into(),
        )]));
    assert!(sql_read_field(&forged_variant, &binary).is_err());
    let variant = NestedField::optional(14, "u", Type::Variant(crate::iceberg::spec::VariantType));
    assert!(sql_read_field(&valid, &variant).is_err());
    // An unlabelled LargeBinary field itself does not author VARIANT.
    let plain = sql_read_field(
        &Field::new("u", DataType::LargeBinary, true),
        &NestedField::optional(15, "u", Type::Primitive(PrimitiveType::Binary)),
    )
    .unwrap();
    assert_eq!(
        field_logical_type(&plain).unwrap(),
        ValueLogicalType::Physical
    );
}

#[test]
#[allow(deprecated)]
fn read_field_projection_preserves_provider_metadata_and_dictionary_facts() {
    use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, arrow_fields_exact};
    let source = Field::new("u", DataType::FixedSizeBinary(16), false).with_metadata(
        std::collections::HashMap::<String, String>::from([
            (PARQUET_FIELD_ID_META_KEY.into(), "91".into()),
            ("provider.annotation".into(), "opaque-source-fact".into()),
            (
                ICEBERG_INITIAL_DEFAULT_META_KEY.into(),
                "\"00112233-4455-6677-8899-aabbccddeeff\"".into(),
            ),
        ]),
    );
    let uuid = NestedField::required(91, "u", Type::Primitive(PrimitiveType::Uuid));
    let mut metadata = source.metadata().clone();
    metadata.insert(NR_LOGICAL_TYPE_KEY.to_string(), "uuid".to_string());
    let projected = sql_read_field(&source, &uuid).unwrap();
    assert!(arrow_fields_exact(
        &projected,
        &source.with_metadata(metadata)
    ));
    let dictionary = Field::new_dict(
        "codes",
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
        true,
        72,
        true,
    )
    .with_metadata(std::collections::HashMap::<String, String>::from([(
        "provider.annotation".into(),
        "kept".into(),
    )]));
    let storage = NestedField::optional(72, "codes", Type::Primitive(PrimitiveType::String));
    assert!(arrow_fields_exact(
        &sql_read_field(&dictionary, &storage).unwrap(),
        &dictionary
    ));
}

#[test]
fn frozen_iceberg_primitives_use_exact_sql_read_carriers() {
    let iceberg = IcebergSchema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(1, "binary", Type::Primitive(PrimitiveType::Binary)).into(),
            NestedField::optional(
                2,
                "variant",
                Type::Variant(crate::iceberg::spec::VariantType),
            )
            .into(),
            NestedField::required(
                3,
                "timestamptz",
                Type::Primitive(PrimitiveType::Timestamptz),
            )
            .into(),
        ])
        .build()
        .expect("Iceberg schema");

    let schema = sql_read_schema_from_iceberg(&iceberg).expect("SQL read schema");
    assert_eq!(schema.field(0).data_type(), &DataType::Binary);
    assert_eq!(schema.field(1).data_type(), &DataType::LargeBinary);
    assert_eq!(
        schema.field(2).data_type(),
        &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
    );
}

#[test]
fn frozen_nested_iceberg_primitives_use_exact_sql_read_carriers() {
    let iceberg = IcebergSchema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::required(
                1,
                "record",
                Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                    2,
                    "payload",
                    Type::Primitive(PrimitiveType::Binary),
                ))])),
            )
            .into(),
            NestedField::optional(
                3,
                "items",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    4,
                    Type::Primitive(PrimitiveType::Binary),
                    false,
                )))),
            )
            .into(),
            NestedField::optional(
                5,
                "attributes",
                Type::Map(MapType::new(
                    Arc::new(NestedField::map_key_element(
                        6,
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::map_value_element(
                        7,
                        Type::Variant(crate::iceberg::spec::VariantType),
                        false,
                    )),
                )),
            )
            .into(),
        ])
        .build()
        .expect("Iceberg schema");

    let schema = sql_read_schema_from_iceberg(&iceberg).expect("SQL read schema");
    let DataType::Struct(record) = schema.field(0).data_type() else {
        panic!("record is a struct")
    };
    assert_eq!(record[0].data_type(), &DataType::Binary);
    let DataType::List(items) = schema.field(1).data_type() else {
        panic!("items is a list")
    };
    assert_eq!(items.data_type(), &DataType::Binary);
    let DataType::Map(entries, _) = schema.field(2).data_type() else {
        panic!("attributes is a map")
    };
    let DataType::Struct(entries) = entries.data_type() else {
        panic!("map entries are a struct")
    };
    assert_eq!(entries[1].data_type(), &DataType::LargeBinary);
}

#[test]
fn maps_list_roles_and_keeps_unused_and_idless_entries() {
    let physical_element = Arc::new(Field::new(
        "array",
        DataType::Struct(vec![Field::new("value.with.dot", DataType::Int64, true)].into()),
        true,
    ));
    let input = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("legacy_items", DataType::List(physical_element), true),
        Field::new("ignored", DataType::Int64, true),
    ]));
    let mapping = super::parse_name_mapping(
        r#"[{"field-id":1,"names":["legacy_items"],"fields":[{"field-id":2,"names":["element"],"fields":[{"field-id":3,"names":["value.with.dot"]}]}]},{"field-id":4,"names":[]},{"names":["ignored"]}]"#,
    ).unwrap();
    let mut observed = Vec::new();
    let mapped = super::apply_name_mapping(&input, &mapping, |path, id| {
        observed.push((
            path.iter()
                .map(|segment| segment.to_string())
                .collect::<Vec<_>>(),
            id,
        ));
    })
    .unwrap();
    assert_eq!(
        super::field_id_for_arrow_field(mapped.field(1)).unwrap(),
        None
    );
    let DataType::List(element) = mapped.field(0).data_type() else {
        panic!("expected a list")
    };
    assert_eq!(element.name(), "array");
    assert_eq!(super::field_id_for_arrow_field(element).unwrap(), Some(2));
    let DataType::Struct(fields) = element.data_type() else {
        panic!("expected element struct")
    };
    assert_eq!(
        super::field_id_for_arrow_field(&fields[0]).unwrap(),
        Some(3)
    );
    assert_eq!(
        observed[2],
        (
            vec![
                "legacy_items".into(),
                "element".into(),
                "value.with.dot".into()
            ],
            Some(3)
        )
    );
    assert_eq!(observed[3], (vec!["ignored".into()], None));
    for (path, id) in observed {
        assert_eq!(super::mapped_path_id(&mapping, &path).unwrap(), id);
    }
}

#[test]
fn two_physical_aliases_cannot_claim_one_iceberg_field() {
    let input = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("old", DataType::Int64, true),
        Field::new("new", DataType::Int64, true),
    ]));
    let mapping = super::parse_name_mapping(r#"[{"field-id":1,"names":["old","new"]}]"#).unwrap();
    let error = super::apply_name_mapping_to_schema(&input, &mapping).unwrap_err();
    assert!(error.contains("multiple physical fields to ID 1"));
}
