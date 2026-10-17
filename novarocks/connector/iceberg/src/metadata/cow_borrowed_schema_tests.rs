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

// Verify the loaded COW schema projection against the original wire path.
use super::*;
use crate::iceberg::spec::{
    FormatVersion, ListType, Literal, NestedField, Operation, PartitionSpec, PrimitiveType,
    Schema as IcebergSchema, Snapshot, SortOrder, Summary, TableMetadata, TableMetadataBuilder,
    Type,
};

// Non-null initial defaults are lawful stock metadata only from v3 onward.
// Write defaults remain valid across all tested versions under the SDK check.
fn with_initial_default_for_version(
    field: NestedField,
    default: Literal,
    version: FormatVersion,
) -> NestedField {
    if version == FormatVersion::V3 {
        field.with_initial_default(default)
    } else {
        field
    }
}

fn schema(version: FormatVersion, extra: bool) -> IcebergSchema {
    let mut fields = vec![
        Arc::new(with_initial_default_for_version(
            NestedField::optional(1, "tiny", PrimitiveType::Int.into())
                .with_doc("real stored column documentation"),
            Literal::int(7),
            version,
        )),
        Arc::new(NestedField::optional(
            2,
            "hidden",
            PrimitiveType::Long.into(),
        )),
        Arc::new(with_initial_default_for_version(
            NestedField::optional(3, "label", PrimitiveType::String.into())
                .with_write_default(Literal::string("write default")),
            Literal::string("quote \" and newline\n"),
            version,
        )),
    ];
    if extra {
        fields.push(Arc::new(NestedField::optional(
            4,
            "new_column",
            PrimitiveType::Boolean.into(),
        )));
    }
    IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .expect("small supported schema")
}
fn snapshot(base: &TableMetadata, id: i64, schema_id: i32, sequence: i64) -> Snapshot {
    Snapshot::builder()
        .with_snapshot_id(id)
        .with_schema_id(schema_id)
        .with_timestamp_ms(base.last_updated_ms())
        .with_sequence_number(if base.format_version() == FormatVersion::V1 {
            0
        } else {
            sequence
        })
        .with_manifest_list(format!("memory://cow-schema/snapshot-{id}.avro"))
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .with_row_range(base.next_row_id(), 1)
        .build()
}
fn loaded_stock_metadata(version: FormatVersion) -> TableMetadata {
    let properties = HashMap::from([
        (
            crate::scalar_integer_domain::PROPERTY.to_string(),
            r#"{"1":"tinyint"}"#.to_string(),
        ),
        (HIDDEN_COLUMNS_PROPERTY.to_string(), "hidden".to_string()),
    ]);
    let base = TableMetadataBuilder::new(
        schema(version, false),
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        "memory://cow-schema".to_string(),
        version,
        properties,
    )
    .expect("stock builder")
    .build()
    .expect("base metadata")
    .metadata;
    let old_id = base.current_schema_id();
    let old_snapshot = snapshot(&base, 40, old_id, 1);
    let historical = base
        .into_builder(None)
        .add_snapshot(old_snapshot)
        .expect("historic snapshot")
        .add_schema(schema(version, true))
        .expect("current schema")
        .set_current_schema(-1)
        .expect("select actual new schema")
        .build()
        .expect("schema history")
        .metadata;
    let current_snapshot = snapshot(&historical, 41, historical.current_schema_id(), 2);
    let metadata = historical
        .into_builder(None)
        .add_snapshot(current_snapshot)
        .expect("current snapshot")
        .build()
        .expect("stock metadata")
        .metadata;
    // This is the EXISTING production load provenance: stock REST/HMS/Hadoop
    // decode TableMetadata JSON before the COW session obtains metadata_ref.
    serde_json::from_str(
        &serde_json::to_string(&metadata).expect("small original stock serialization"),
    )
    .expect("same actual stock metadata decoder")
}
fn payload(metadata: &TableMetadata, snapshot_id: i64) -> IcebergTablePayload {
    frozen_copy_on_write_source_payload(
        &ConnectorInstanceId::parse("iceberg").unwrap(),
        "db",
        "cow_schema",
        metadata,
        snapshot_id,
        IcebergDataFileInfo::for_test("memory://cow-schema/data.parquet", 100, 1),
    )
    .expect("actual original source payload constructor")
}
#[test]
fn stock_v1_v2_v3_current_and_historical_cow_schemas_keep_defaults_hidden_and_integer_domains() {
    for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
        let metadata = loaded_stock_metadata(version);
        for id in [40, 41] {
            let source = payload(&metadata, id);
            let decoded = projected_schema(&source, &[]).expect("original decode path");
            let borrowed = projected_schema_with_metadata(&source, &[], &metadata)
                .expect("same loaded metadata");
            assert_eq!(borrowed, decoded);
            assert_eq!(
                borrowed.field(0).data_type(),
                &arrow::datatypes::DataType::Int8
            );
            assert!(
                borrowed
                    .field(1)
                    .metadata()
                    .contains_key(novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL)
            );
            assert_eq!(
                borrowed
                    .field(0)
                    .metadata()
                    .get(crate::default_value::ICEBERG_INITIAL_DEFAULT_META_KEY)
                    .map(String::as_str),
                (version == FormatVersion::V3).then_some("7")
            );
            assert_eq!(
                borrowed
                    .fields()
                    .iter()
                    .any(|field| field.name() == "new_column"),
                id == 41
            );
            let selected = projected_schema_with_metadata(&source, &[2, 0], &metadata).unwrap();
            assert_eq!(selected, projected_schema(&source, &[2, 0]).unwrap());
            assert_eq!(selected.field(0).name(), "label");
        }
    }
}
#[test]
fn actual_projection_error_and_ordinary_decode_failure_are_preserved() {
    let metadata = loaded_stock_metadata(FormatVersion::V2);
    let mut source = payload(&metadata, 40);
    let original = projected_schema(&source, &[usize::MAX]).err().unwrap();
    assert_eq!(
        projected_schema_with_metadata(&source, &[usize::MAX], &metadata)
            .err()
            .unwrap(),
        original
    );
    source.table_info.as_mut().unwrap().serialized_metadata = None;
    assert_eq!(
        projected_schema(&source, &[]).err().unwrap(),
        corrupt("Iceberg table handle has no serialized metadata")
    );
}

#[test]
fn historical_cow_projection_keeps_nominal_domains_and_raw_carriers_distinct() {
    use arrow::datatypes::DataType;
    use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, ValueLogicalType, field_logical_type};

    let mut fields = schema(FormatVersion::V3, false)
        .as_struct()
        .fields()
        .to_vec();
    for (id, name, ty) in [
        (5, "h", PrimitiveType::Binary),
        (6, "b", PrimitiveType::Binary),
        (7, "n", PrimitiveType::Fixed(16)),
        (8, "raw", PrimitiveType::Fixed(16)),
        (9, "u", PrimitiveType::Uuid),
    ] {
        fields.push(Arc::new(NestedField::optional(id, name, ty.into())));
    }
    let historical_schema = IcebergSchema::builder()
        .with_fields(fields.clone())
        .build()
        .unwrap();
    let properties = HashMap::from([
        (
            crate::scalar_integer_domain::PROPERTY.into(),
            r#"{"1":"tinyint"}"#.into(),
        ),
        (HIDDEN_COLUMNS_PROPERTY.into(), "hidden".into()),
        (format!("{LOGICAL_TYPE_PROPERTY_PREFIX}h"), "hll".into()),
        (format!("{LOGICAL_TYPE_PROPERTY_PREFIX}b"), "bitmap".into()),
        (
            format!("{LOGICAL_TYPE_PROPERTY_PREFIX}n"),
            "largeint".into(),
        ),
    ]);
    let base = TableMetadataBuilder::new(
        historical_schema,
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        "memory://cow-schema/nominal".into(),
        FormatVersion::V3,
        properties,
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    // CREATE normalizes field IDs. Evolve the exact persisted declaration;
    // reusing the pre-CREATE IDs would assign domains to different fields.
    let mut fields = base.current_schema().as_struct().fields().to_vec();
    fields.push(Arc::new(NestedField::optional(
        base.last_column_id() + 1,
        "current_only",
        PrimitiveType::Boolean.into(),
    )));
    let current_schema = IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .unwrap();
    for name in ["h", "b", "n", "raw", "u"] {
        assert_eq!(
            current_schema.field_by_name(name).unwrap().id,
            base.current_schema().field_by_name(name).unwrap().id
        );
    }
    let old = snapshot(&base, 40, base.current_schema_id(), 1);
    let history = base
        .into_builder(None)
        .add_snapshot(old)
        .unwrap()
        .add_schema(current_schema)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    // Match the original stock wire-normalized load provenance.
    let metadata: TableMetadata =
        serde_json::from_str(&serde_json::to_string(&history).unwrap()).unwrap();
    let source = payload(&metadata, 40);
    let borrowed = projected_schema_with_metadata(&source, &[], &metadata).unwrap();
    assert_eq!(borrowed, projected_schema(&source, &[]).unwrap());
    assert!(
        !borrowed
            .fields()
            .iter()
            .any(|field| field.name() == "current_only")
    );
    for (ordinal, logical) in [
        (3, ValueLogicalType::Hll),
        (4, ValueLogicalType::Bitmap),
        (5, ValueLogicalType::LargeInt),
    ] {
        assert_eq!(
            field_logical_type(borrowed.field(ordinal)).unwrap(),
            logical
        );
        assert_eq!(
            borrowed.field(ordinal).metadata()[NR_LOGICAL_TYPE_KEY],
            logical.metadata_value().unwrap()
        );
    }
    assert_eq!(
        borrowed.field(6).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert!(
        !borrowed
            .field(6)
            .metadata()
            .contains_key(NR_LOGICAL_TYPE_KEY)
    );
    assert_eq!(
        field_logical_type(borrowed.field(7)).unwrap(),
        ValueLogicalType::Uuid
    );
    assert_eq!(borrowed.field(0).data_type(), &DataType::Int8);
    assert_eq!(
        borrowed.field(0).metadata()[crate::default_value::ICEBERG_INITIAL_DEFAULT_META_KEY],
        "7"
    );
    assert!(
        borrowed
            .field(1)
            .metadata()
            .contains_key(novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL)
    );
    assert!(
        borrowed
            .fields()
            .iter()
            .skip(8)
            .all(|field| !field.is_nullable())
    );
    let projected = projected_schema_with_metadata(&source, &[5, 6, 3, 5], &metadata).unwrap();
    assert_eq!(projected.fields()[0], projected.fields()[3]);
    assert_eq!(
        field_logical_type(projected.field(0)).unwrap(),
        ValueLogicalType::LargeInt
    );
    assert!(
        !projected
            .field(1)
            .metadata()
            .contains_key(NR_LOGICAL_TYPE_KEY)
    );
}
#[test]
fn self_built_synthetic_doc_is_not_the_production_wire_normalized_provenance() {
    // A lawful in-memory library schema can contain extra synthetic child doc.
    // This normal counterexample constrains the helper's production caller;
    // it is NOT a new Unsupported condition or a product failure claim.
    let schema = IcebergSchema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1,
            "items",
            Type::List(ListType {
                element_field: Arc::new(
                    NestedField::list_element(2, PrimitiveType::Long.into(), false)
                        .with_doc("only exists before original Type wire normalization"),
                ),
            }),
        ))])
        .build()
        .unwrap();
    let base = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        "memory://cow-schema/synthetic".to_string(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let actual_snapshot = snapshot(&base, 40, base.current_schema_id(), 1);
    let memory = base
        .into_builder(None)
        .add_snapshot(actual_snapshot)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let source = payload(&memory, 40);
    assert_ne!(
        projected_schema(&source, &[]).unwrap(),
        projected_schema_with_metadata(&source, &[], &memory).unwrap()
    );
    let loaded: TableMetadata =
        serde_json::from_str(&serde_json::to_string(&memory).unwrap()).unwrap();
    let source = payload(&loaded, 40);
    assert_eq!(
        projected_schema(&source, &[]).unwrap(),
        projected_schema_with_metadata(&source, &[], &loaded).unwrap()
    );
}
