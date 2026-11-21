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

use std::collections::HashMap as StdHashMap;

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::Fields;

use super::*;
use crate::iceberg::spec::{
    Literal as IcebergLiteral, NestedField, PartitionSpec, PrimitiveType, Schema as IcebergSchema,
    Struct as IcebergStruct, StructType, Transform, Type,
};
use crate::typed_read::column_handle::{IcebergColumnHandle, IcebergColumnHandleParams};

fn field_id_metadata(field_id: i32) -> StdHashMap<String, String> {
    [(PARQUET_FIELD_ID_META_KEY.to_owned(), field_id.to_string())]
        .into_iter()
        .collect()
}

fn table_schema() -> IcebergSchema {
    IcebergSchema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "region",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                3,
                "amount",
                Type::Primitive(PrimitiveType::Double),
            )),
        ])
        .build()
        .expect("frozen table schema")
}

fn handle(schema: &IcebergSchema, field_id: i32) -> IcebergColumnHandle {
    IcebergColumnHandle::base_column_of(schema, field_id).expect("base column handle")
}

fn empty_binding_request<'a>(
    schema: &'a IcebergSchema,
    file_schema: &'a SchemaRef,
    columns: &'a [IcebergColumnHandle],
) -> IcebergSchemaBindingRequest<'a> {
    IcebergSchemaBindingRequest {
        table_schema: schema,
        file_schema,
        name_mapping: None,
        partition_spec: None,
        partition_values: None,
        columns,
    }
}

#[test]
fn metadata_value_struct_keeps_nested_identity_and_values() {
    use arrow::array::BinaryArray;
    let schema = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::optional(
                1,
                "payload",
                Type::Struct(StructType::new(vec![
                    NestedField::required(2, "metadata", Type::Primitive(PrimitiveType::Binary))
                        .into(),
                    NestedField::required(3, "value", Type::Primitive(PrimitiveType::Binary))
                        .into(),
                ])),
            )
            .into(),
        ])
        .build()
        .unwrap();
    let target = annotated_read_schema(&schema).unwrap();
    let DataType::Struct(children) = target.field(0).data_type() else {
        panic!("ordinary struct");
    };
    assert_eq!(field_id_for_arrow_field(&children[0]).unwrap(), Some(2));
    assert_eq!(field_id_for_arrow_field(&children[1]).unwrap(), Some(3));
    let values: ArrayRef = Arc::new(StructArray::new(
        children.clone(),
        vec![
            Arc::new(BinaryArray::from_vec(vec![b"opaque metadata"])),
            Arc::new(BinaryArray::from_vec(vec![b"opaque value"])),
        ],
        None,
    ));
    let batch = RecordBatch::try_new(target.clone(), vec![values.clone()]).unwrap();
    let columns = [handle(&schema, 1)];
    let binding = bind_scan_columns(empty_binding_request(&schema, &target, &columns)).unwrap();
    let output = binding
        .materialize(
            &batch,
            None,
            &IcebergSplitFacts {
                path: "struct.parquet",
                file_first_row_id: None,
                data_sequence_number: None,
            },
        )
        .unwrap();
    assert_eq!(output[0].to_data(), values.to_data());
    assert!(physical_adaptation(values.data_type(), &DataType::LargeBinary, false).is_err());
    assert_eq!(
        physical_adaptation(values.data_type(), &DataType::LargeBinary, true).unwrap(),
        IcebergPhysicalAdaptation::CollapseVariant
    );
}

#[test]
fn a_matching_field_id_binds_to_the_physical_field() {
    let schema = table_schema();
    let file_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
        Field::new("whatever_it_was_renamed_to", DataType::Int64, false)
            .with_metadata(field_id_metadata(1)),
    ]));
    let columns = vec![handle(&schema, 1)];
    let binding =
        bind_scan_columns(empty_binding_request(&schema, &file_schema, &columns)).expect("binds");
    assert!(matches!(
        binding.columns()[0].source(),
        IcebergColumnSource::Physical {
            base_field_id: 1,
            ..
        }
    ));
    assert_eq!(binding.physical_base_field_ids(), &[1]);
}

#[test]
fn an_identity_partition_column_becomes_a_frozen_constant() {
    let schema = table_schema();
    let file_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(field_id_metadata(1)),
    ]));
    let spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(7)
        .add_partition_field("region", "region", Transform::Identity)
        .expect("identity partition field")
        .build()
        .expect("partition spec");
    let values = IcebergStruct::from_iter([Some(IcebergLiteral::string("emea"))]);
    let columns = vec![handle(&schema, 2)];
    let binding = bind_scan_columns(IcebergSchemaBindingRequest {
        table_schema: &schema,
        file_schema: &file_schema,
        name_mapping: None,
        partition_spec: Some(&spec),
        partition_values: Some(&values),
        columns: &columns,
    })
    .expect("binds");

    let IcebergColumnSource::IdentityPartitionConstant(value) = binding.columns()[0].source()
    else {
        panic!("expected an identity partition constant");
    };
    assert_eq!(value.as_ref(), Some(&IcebergLiteral::string("emea")));

    let batch = rows_without_stored_lineage(2);
    let facts = IcebergSplitFacts {
        path: "s3://bucket/data/file.parquet",
        file_first_row_id: None,
        data_sequence_number: None,
    };
    let produced = binding.materialize(&batch, None, &facts).expect("columns");
    let region = produced[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8 partition constant");
    assert_eq!(region.value(0), "emea");
    assert_eq!(region.value(1), "emea");
}

#[test]
fn a_legacy_file_without_field_ids_binds_through_the_name_mapping() {
    let schema = table_schema();
    let file_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![Field::new(
        "legacy_id",
        DataType::Int64,
        false,
    )]));
    let mapping: NameMapping =
        serde_json::from_str(r#"[{"names":["legacy_id"],"field-id":1}]"#).expect("name mapping");
    let columns = vec![handle(&schema, 1)];
    let binding = bind_scan_columns(IcebergSchemaBindingRequest {
        table_schema: &schema,
        file_schema: &file_schema,
        name_mapping: Some(Arc::new(mapping)),
        partition_spec: None,
        partition_values: None,
        columns: &columns,
    })
    .expect("binds");
    assert!(matches!(
        binding.columns()[0].source(),
        IcebergColumnSource::Physical {
            base_field_id: 1,
            ..
        }
    ));
    assert_eq!(binding.coverage(), FileFieldIdCoverage::None);
}

#[test]
fn a_partly_identified_file_schema_is_corrupt_rather_than_name_mapped() {
    let file_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(field_id_metadata(1)),
        Field::new("region", DataType::Utf8, true),
    ]));
    let error = file_field_id_coverage(&file_schema).expect_err("partial coverage is corrupt");
    assert_eq!(
        error.kind(),
        novarocks_spi::connector::ConnectorErrorKind::CorruptData
    );
}

#[test]
fn a_nested_initial_default_reaches_struct_fields() {
    let inner = StructType::new(vec![Arc::new(
        NestedField::optional(11, "count", Type::Primitive(PrimitiveType::Long))
            .with_initial_default(IcebergLiteral::long(7)),
    )]);
    let schema = IcebergSchema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            10,
            "detail",
            Type::Struct(inner),
        ))])
        .build()
        .expect("nested schema");
    let read_schema = annotated_read_schema(&schema).expect("annotated read schema");
    let DataType::Struct(children) = read_schema.field(0).data_type() else {
        panic!("expected a struct carrier");
    };
    assert_eq!(
        children[0]
            .metadata()
            .get(ICEBERG_INITIAL_DEFAULT_META_KEY)
            .map(String::as_str),
        Some("7")
    );
}

#[test]
fn a_missing_column_with_an_initial_default_materializes_it() {
    let schema = IcebergSchema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(
                NestedField::optional(4, "grade", Type::Primitive(PrimitiveType::Long))
                    .with_initial_default(IcebergLiteral::long(42)),
            ),
        ])
        .build()
        .expect("schema with a default");
    let file_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(field_id_metadata(1)),
    ]));
    let columns = vec![handle(&schema, 4)];
    let binding =
        bind_scan_columns(empty_binding_request(&schema, &file_schema, &columns)).expect("binds");
    assert!(matches!(
        binding.columns()[0].source(),
        IcebergColumnSource::InitialDefault
    ));

    assert!(binding.root_projection().is_empty());
    let batch = rows_without_stored_lineage(1);
    let facts = IcebergSplitFacts {
        path: "file.parquet",
        file_first_row_id: None,
        data_sequence_number: None,
    };
    let produced = binding.materialize(&batch, None, &facts).expect("columns");
    let grade = produced[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 default");
    assert_eq!(grade.value(0), 42);
}

#[test]
fn a_missing_nullable_column_materializes_a_typed_null() {
    let schema = table_schema();
    let file_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(field_id_metadata(1)),
    ]));
    let columns = vec![handle(&schema, 3)];
    let binding =
        bind_scan_columns(empty_binding_request(&schema, &file_schema, &columns)).expect("binds");
    assert!(matches!(
        binding.columns()[0].source(),
        IcebergColumnSource::TypedNull
    ));
}

#[test]
fn a_missing_required_column_without_a_default_fails_closed() {
    let schema = table_schema();
    let file_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
        Field::new("region", DataType::Utf8, true).with_metadata(field_id_metadata(2)),
    ]));
    let columns = vec![handle(&schema, 1)];
    let error = bind_scan_columns(empty_binding_request(&schema, &file_schema, &columns))
        .expect_err("a required field cannot be invented");
    assert_eq!(
        error.kind(),
        novarocks_spi::connector::ConnectorErrorKind::CorruptData
    );
}

#[test]
fn every_legal_promotion_is_accepted_and_nothing_else_is() {
    for (source, target, expected) in [
        (
            DataType::Int32,
            DataType::Int64,
            IcebergTypePromotion::IntToLong,
        ),
        (
            DataType::Float32,
            DataType::Float64,
            IcebergTypePromotion::FloatToDouble,
        ),
        (
            DataType::Decimal128(9, 2),
            DataType::Decimal128(18, 2),
            IcebergTypePromotion::DecimalPrecisionWidening,
        ),
        (
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            IcebergTypePromotion::DateToTimestamp,
        ),
        (
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            IcebergTypePromotion::DateToTimestampNs,
        ),
        (
            DataType::Null,
            DataType::Utf8,
            IcebergTypePromotion::UnknownToAny,
        ),
    ] {
        assert_eq!(
            physical_adaptation(&source, &target, false).expect("legal promotion"),
            IcebergPhysicalAdaptation::Promotion(expected),
            "{source:?} -> {target:?}"
        );
    }

    // Narrowing, re-scaling, and unrelated types are all mismatches.
    for (source, target) in [
        (DataType::Int64, DataType::Int32),
        (DataType::Float64, DataType::Float32),
        (DataType::Decimal128(18, 2), DataType::Decimal128(9, 2)),
        (DataType::Decimal128(9, 2), DataType::Decimal128(18, 4)),
        (DataType::Utf8, DataType::Int64),
    ] {
        let error = physical_adaptation(&source, &target, false)
            .expect_err("an illegal same-field-id mismatch must fail closed");
        assert_eq!(
            error.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );
    }
}

#[test]
fn an_illegal_same_field_id_mismatch_fails_before_the_reader_opens() {
    let schema = table_schema();
    let file_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Utf8, false).with_metadata(field_id_metadata(1)),
    ]));
    let columns = vec![handle(&schema, 1)];
    let error = bind_scan_columns(empty_binding_request(&schema, &file_schema, &columns))
        .expect_err("string is not a legal source for a long");

    assert_eq!(
        error.kind(),
        novarocks_spi::connector::ConnectorErrorKind::CorruptData
    );
}

#[test]
fn an_int_column_promotes_to_long_when_it_is_materialized() {
    let schema = table_schema();
    let file_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(field_id_metadata(1)),
    ]));
    let columns = vec![handle(&schema, 1)];
    let binding =
        bind_scan_columns(empty_binding_request(&schema, &file_schema, &columns)).expect("binds");
    let batch = RecordBatch::try_new(
        Arc::clone(&file_schema),
        vec![Arc::new(Int32Array::from(vec![5_i32, 6]))],
    )
    .expect("physical batch");
    let facts = IcebergSplitFacts {
        path: "file.parquet",
        file_first_row_id: None,
        data_sequence_number: None,
    };
    let produced = binding.materialize(&batch, None, &facts).expect("columns");
    let ids = produced[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("promoted to int64");
    assert_eq!(ids.values(), &[5, 6]);
}

#[test]
fn a_nested_projection_dereferences_by_field_id_and_keeps_parent_nulls() {
    let inner = StructType::new(vec![Arc::new(NestedField::optional(
        21,
        "count",
        Type::Primitive(PrimitiveType::Long),
    ))]);
    let schema = IcebergSchema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            20,
            "detail",
            Type::Struct(inner),
        ))])
        .build()
        .expect("nested schema");
    let child = Field::new("count", DataType::Int64, true).with_metadata(field_id_metadata(21));
    let file_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
        Field::new(
            "detail",
            DataType::Struct(Fields::from(vec![Arc::new(child.clone())])),
            true,
        )
        .with_metadata(field_id_metadata(20)),
    ]));
    let projected = IcebergColumnHandle::base_column_of(&schema, 20)
        .expect("base handle")
        .dereference(&[21])
        .expect("nested handle");
    let columns = vec![projected];
    let binding =
        bind_scan_columns(empty_binding_request(&schema, &file_schema, &columns)).expect("binds");

    let counts: ArrayRef = Arc::new(Int64Array::from(vec![Some(3_i64), Some(4)]));
    let detail = StructArray::new(
        Fields::from(vec![Arc::new(child)]),
        vec![counts],
        Some(NullBuffer::from(vec![true, false])),
    );
    let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![Arc::new(detail)])
        .expect("physical batch");
    let facts = IcebergSplitFacts {
        path: "file.parquet",
        file_first_row_id: None,
        data_sequence_number: None,
    };
    let produced = binding.materialize(&batch, None, &facts).expect("columns");
    let values = produced[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 leaf");
    assert_eq!(values.value(0), 3);
    assert!(
        values.is_null(1),
        "an absent parent nulls its whole subtree"
    );
}

/// A physical batch of `rows` rows that materializes no column at all, so
/// every row-lineage column it is asked for must be inherited.
fn rows_without_stored_lineage(rows: usize) -> RecordBatch {
    RecordBatch::try_new_with_options(
        Arc::new(ArrowSchema::empty()),
        Vec::new(),
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .expect("empty physical batch")
}

#[test]
fn row_id_overflow_fails_closed_instead_of_wrapping() {
    let target = Field::new("_row_id", DataType::Int64, false);
    let positions = UInt64Array::from(vec![1_u64]);
    let facts = IcebergSplitFacts {
        path: "file.parquet",
        file_first_row_id: Some(i64::MAX),
        data_sequence_number: Some(3),
    };
    let error = metadata_column(
        IcebergMetadataColumn::RowId,
        &target,
        &rows_without_stored_lineage(1),
        Some(&positions),
        &facts,
    )
    .expect_err("row id overflow is never wrapped");
    assert_eq!(
        error.kind(),
        novarocks_spi::connector::ConnectorErrorKind::CorruptData
    );
}

#[test]
fn pre_v3_files_publish_nullable_lineage_without_row_positions() {
    let facts = IcebergSplitFacts {
        path: "pre-v3.parquet",
        file_first_row_id: None,
        data_sequence_number: Some(3),
    };
    for metadata in ROW_LINEAGE_METADATA_COLUMNS {
        assert!(metadata.nullable());
        let target = Field::new(metadata.column_name(), DataType::Int64, metadata.nullable());
        let array = metadata_column(
            metadata,
            &target,
            &rows_without_stored_lineage(2),
            None,
            &facts,
        )
        .expect("pre-v3 file lineage is null");
        assert_eq!(array.len(), 2);
        assert_eq!(array.null_count(), 2);
    }
}

#[test]
fn allocated_file_without_required_sequence_fails_closed() {
    let target = Field::new("_last_updated_sequence_number", DataType::Int64, true);
    let facts = IcebergSplitFacts {
        path: "file.parquet",
        file_first_row_id: Some(100),
        data_sequence_number: None,
    };
    assert!(
        metadata_column(
            IcebergMetadataColumn::LastUpdatedSequenceNumber,
            &target,
            &rows_without_stored_lineage(1),
            None,
            &facts,
        )
        .is_err()
    );
}

fn metadata_handle(metadata: IcebergMetadataColumn) -> IcebergColumnHandle {
    IcebergColumnHandle::base_column(&NestedField::optional(
        metadata.field_id(),
        metadata.column_name(),
        Type::Primitive(metadata.declared_type()),
    ))
    .expect("metadata column handle")
}

/// Whether a row-lineage column is stored is a fact of one file, and each
/// column answers for itself. This file materializes `_row_id` and leaves
/// `_last_updated_sequence_number` to inheritance, so only the first joins
/// the projection the reader opens.
#[test]
fn only_a_row_lineage_column_the_file_stores_joins_the_projection() {
    let schema = table_schema();
    let file_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(field_id_metadata(1)),
        Field::new(ICEBERG_ROW_ID_COL, DataType::Int64, true)
            .with_metadata(field_id_metadata(ICEBERG_RESERVED_FIELD_ID_ROW_ID)),
    ]));
    let columns = vec![
        handle(&schema, 1),
        metadata_handle(IcebergMetadataColumn::RowId),
        metadata_handle(IcebergMetadataColumn::LastUpdatedSequenceNumber),
    ];
    let binding =
        bind_scan_columns(empty_binding_request(&schema, &file_schema, &columns)).expect("binds");

    assert!(matches!(
        binding.columns()[1].source(),
        IcebergColumnSource::StoredRowLineage(IcebergRowLineageColumn::RowId)
    ));
    assert!(matches!(
        binding.columns()[2].source(),
        IcebergColumnSource::Metadata(IcebergMetadataColumn::LastUpdatedSequenceNumber)
    ));
    assert_eq!(
        binding.physical_base_field_ids(),
        &[1, ICEBERG_RESERVED_FIELD_ID_ROW_ID]
    );
    // A stored `_row_id` may still be null per row, so the reader must
    // keep producing absolute positions for the rows that inherit.
    assert!(binding.requires_row_positions());
}

/// The same scan against a file that stores nothing keeps both columns
/// synthesized and asks the reader for no extra Parquet column.
#[test]
fn a_file_without_stored_row_lineage_synthesizes_both_columns() {
    let schema = table_schema();
    let file_schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(field_id_metadata(1)),
    ]));
    let columns = vec![
        handle(&schema, 1),
        metadata_handle(IcebergMetadataColumn::RowId),
        metadata_handle(IcebergMetadataColumn::LastUpdatedSequenceNumber),
    ];
    let binding =
        bind_scan_columns(empty_binding_request(&schema, &file_schema, &columns)).expect("binds");

    assert!(matches!(
        binding.columns()[1].source(),
        IcebergColumnSource::Metadata(IcebergMetadataColumn::RowId)
    ));
    assert!(matches!(
        binding.columns()[2].source(),
        IcebergColumnSource::Metadata(IcebergMetadataColumn::LastUpdatedSequenceNumber)
    ));
    assert_eq!(binding.physical_base_field_ids(), &[1]);
}

#[test]
fn every_metadata_column_a_scan_may_name_is_spelled_the_way_the_engine_asks_for_it() {
    let published = ALWAYS_BOUND_METADATA_COLUMNS
        .into_iter()
        .chain(ROW_LINEAGE_METADATA_COLUMNS)
        .collect::<Vec<_>>();
    assert_eq!(
        published
            .iter()
            .map(|column| column.column_name())
            .collect::<Vec<_>>(),
        vec!["_file", "_pos", "_row_id", "_last_updated_sequence_number"]
    );
    for column in published {
        assert!(column.is_externally_visible(), "{column:?}");
        assert_eq!(
            IcebergMetadataColumn::from_field_id(column.field_id()),
            Some(column)
        );
    }
    // `_deleted` is the one metadata column this page source cannot answer,
    // so it is the one a scan may never name.
    assert!(!IcebergMetadataColumn::IsDeleted.is_externally_visible());
    assert_eq!(
        IcebergMetadataColumn::from_field_id(IcebergMetadataColumn::IsDeleted.field_id()),
        Some(IcebergMetadataColumn::IsDeleted)
    );
}

#[test]
fn a_metadata_column_a_scan_may_not_name_refuses_to_bind() {
    let handle = IcebergColumnHandle::try_new(IcebergColumnHandleParams {
        base_column_identity: crate::typed_read::column_handle::ColumnIdentity::try_new(
            ICEBERG_METADATA_FIELD_ID_IS_DELETED,
            "_deleted",
            crate::typed_read::column_handle::ColumnIdentityCategory::Primitive,
            Vec::new(),
        )
        .expect("identity"),
        base_type_json: "\"boolean\"".to_owned(),
        field_id_path: Vec::new(),
        type_json: "\"boolean\"".to_owned(),
        nullable: true,
        comment: None,
    })
    .expect("handle");
    let error = bind_metadata_column(
        &handle,
        IcebergMetadataColumn::IsDeleted,
        StoredRowLineageIndices::default(),
    )
    .expect_err("_deleted never binds");
    assert_eq!(
        error.kind(),
        novarocks_spi::connector::ConnectorErrorKind::Unsupported
    );
}

#[test]
fn a_metadata_column_named_against_its_reserved_id_is_rejected() {
    let handle = IcebergColumnHandle::try_new(IcebergColumnHandleParams {
        base_column_identity: crate::typed_read::column_handle::ColumnIdentity::try_new(
            ICEBERG_METADATA_FIELD_ID_PATH,
            "not_the_path_column",
            crate::typed_read::column_handle::ColumnIdentityCategory::Primitive,
            Vec::new(),
        )
        .expect("identity"),
        base_type_json: "\"string\"".to_owned(),
        field_id_path: Vec::new(),
        type_json: "\"string\"".to_owned(),
        nullable: false,
        comment: None,
    })
    .expect("handle");
    let error = bind_metadata_column(
        &handle,
        IcebergMetadataColumn::Path,
        StoredRowLineageIndices::default(),
    )
    .expect_err("a reserved id must carry its reserved name");
    assert_eq!(
        error.kind(),
        novarocks_spi::connector::ConnectorErrorKind::CorruptData
    );
}
#[test]
fn scalar_integer_binding_reads_one_physical_id_into_exact_logical_and_delete_occurrences() {
    use crate::scalar_integer_domain::ScalarIntegerDomain;
    use arrow::array::{Int8Array, Int32Array};
    let schema = IcebergSchema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            31,
            "tiny",
            Type::Primitive(PrimitiveType::Int),
        ))])
        .build()
        .unwrap();
    let physical_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("old_name", DataType::Int32, true).with_metadata(field_id_metadata(31)),
    ]));
    let plain = handle(&schema, 31);
    let narrow = plain
        .clone()
        .with_scalar_integer_domain(Some(ScalarIntegerDomain::Int8))
        .unwrap();
    let columns = vec![narrow, plain];
    let binding =
        bind_scan_columns(empty_binding_request(&schema, &physical_schema, &columns)).unwrap();
    assert_eq!(binding.physical_base_field_ids(), &[31]);
    assert_eq!(binding.columns()[0].target().data_type(), &DataType::Int8);
    assert_eq!(binding.columns()[1].target().data_type(), &DataType::Int32);
    let array: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(999),
        Some(-128),
        None,
        Some(127),
        Some(999),
    ]));
    let batch = RecordBatch::try_new(physical_schema.clone(), vec![array.slice(1, 3)]).unwrap();
    let facts = IcebergSplitFacts {
        path: "memory://tiny",
        file_first_row_id: None,
        data_sequence_number: None,
    };
    let values = binding.materialize(&batch, None, &facts).unwrap();
    assert_eq!(
        values[0]
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-128), None, Some(127)]
    );
    assert_eq!(
        values[1]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-128), None, Some(127)]
    );
    assert!(
        physical_adaptation(&DataType::Int32, &DataType::Int8, false).is_err(),
        "unclaimed physical narrowing stays closed"
    );
    let corrupt_batch = RecordBatch::try_new(
        physical_schema,
        vec![Arc::new(Int32Array::from(vec![Some(128)]))],
    )
    .unwrap();
    assert_eq!(
        binding
            .materialize(&corrupt_batch, None, &facts)
            .unwrap_err()
            .kind(),
        novarocks_spi::connector::ConnectorErrorKind::CorruptData
    );
}

#[test]
fn scalar_integer_initial_defaults_and_missing_nulls_have_exact_domains() {
    use crate::iceberg::spec::PrimitiveLiteral;
    use crate::scalar_integer_domain::ScalarIntegerDomain;
    use arrow::array::{Int8Array, Int16Array};
    for (default, expected) in [(Some(32767), Some(32767_i16)), (None, None)] {
        let mut field = NestedField::optional(21, "small", Type::Primitive(PrimitiveType::Int));
        if let Some(value) = default {
            field = field.with_initial_default(Literal::Primitive(PrimitiveLiteral::Int(value)));
        }
        let schema = IcebergSchema::builder()
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap();
        let columns = [handle(&schema, 21)
            .with_scalar_integer_domain(Some(ScalarIntegerDomain::Int16))
            .unwrap()];
        let empty = Arc::new(ArrowSchema::empty());
        let binding = bind_scan_columns(empty_binding_request(&schema, &empty, &columns)).unwrap();
        let batch = RecordBatch::try_new_with_options(
            empty,
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(2)),
        )
        .unwrap();
        let values = binding
            .materialize(
                &batch,
                None,
                &IcebergSplitFacts {
                    path: "memory://default",
                    file_first_row_id: None,
                    data_sequence_number: None,
                },
            )
            .unwrap();
        assert_eq!(
            values[0]
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![expected, expected]
        );
    }
    let field = Field::new("tiny", DataType::Int8, true);
    let valid = partition_constant(
        Some(&Literal::Primitive(PrimitiveLiteral::Int(-128))),
        &field,
        2,
    )
    .unwrap();
    assert_eq!(
        valid
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-128), Some(-128)]
    );
    assert!(
        partition_constant(
            Some(&Literal::Primitive(PrimitiveLiteral::Int(128))),
            &field,
            2
        )
        .is_err()
    );
}

#[test]
fn nominal_targets_preserve_physical_buffers_nulls_and_repeated_occurrences() {
    use crate::schema_mapping::nominal_declaration::NominalSourceDomain;
    use arrow::array::{BinaryArray, FixedSizeBinaryArray};

    for domain in [
        NominalSourceDomain::LargeInt,
        NominalSourceDomain::Hll,
        NominalSourceDomain::Bitmap,
    ] {
        let (primitive, carrier, input): (_, _, ArrayRef) = match domain {
            NominalSourceDomain::LargeInt => {
                let first = i128::MIN.to_be_bytes();
                let last = i128::MAX.to_be_bytes();
                (
                    PrimitiveType::Fixed(16),
                    DataType::FixedSizeBinary(16),
                    Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                            [Some(first.as_slice()), None, Some(last.as_slice())].into_iter(),
                            16,
                        )
                        .unwrap(),
                    ),
                )
            }
            _ => (
                PrimitiveType::Binary,
                DataType::Binary,
                Arc::new(BinaryArray::from(vec![
                    Some(b"first".as_slice()),
                    None,
                    Some(b"last".as_slice()),
                ])),
            ),
        };
        let schema = IcebergSchema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                17,
                "declared",
                Type::Primitive(primitive),
            ))])
            .build()
            .unwrap();
        let physical = Arc::new(ArrowSchema::new(vec![
            Field::new("stored", carrier, true).with_metadata(field_id_metadata(17)),
        ]));
        let columns = [
            handle(&schema, 17),
            handle(&schema, 17),
            handle(&schema, 17),
        ];
        // Provider declarations select the complete value domain, while
        // physical column handles remain domain-free.
        let mut request = |_: std::alloc::Layout| Ok(());
        let mut work = |_: usize| Ok(());
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        let value = crate::schema_mapping::typed_value::from_declaration(
            schema.as_struct().fields()[0].field_type.as_ref(),
            true,
            Some(domain.logical_type()),
            None,
            &mut creation,
        )
        .unwrap();
        assert_eq!(value.binding.logical_type(), domain.logical_type());
        let binding =
            bind_scan_columns(empty_binding_request(&schema, &physical, &columns)).unwrap();
        assert_eq!(binding.physical_base_field_ids(), &[17]);
        for ordinal in [0, 2] {
            assert_eq!(
                binding.columns()[ordinal].target().metadata()[PARQUET_FIELD_ID_META_KEY],
                "17"
            );
        }
        assert!(
            !binding.columns()[1]
                .target()
                .metadata()
                .contains_key(novarocks_type_contract::NR_LOGICAL_TYPE_KEY)
        );
        let batch = RecordBatch::try_new(physical, vec![input.slice(1, 2)]).unwrap();
        let values = binding
            .materialize(
                &batch,
                None,
                &IcebergSplitFacts {
                    path: "memory://nominal",
                    file_first_row_id: None,
                    data_sequence_number: None,
                },
            )
            .unwrap();
        for output in &values {
            assert_eq!(output.to_data(), batch.column(0).to_data());
            assert!(
                Arc::ptr_eq(output, batch.column(0)),
                "identity metadata does not copy values"
            );
            assert!(output.is_null(0));
        }
    }
}

#[test]
fn nominal_largeint_defaults_keep_original_refusal_and_missing_nulls() {
    use crate::iceberg::spec::PrimitiveLiteral;
    use crate::schema_mapping::nominal_declaration::NominalSourceDomain;
    use arrow::array::FixedSizeBinaryArray;
    for default in [Some(i128::MIN.to_be_bytes()), None] {
        let mut field = NestedField::optional(23, "n", Type::Primitive(PrimitiveType::Fixed(16)));
        if let Some(bytes) = default {
            field = field
                .with_initial_default(Literal::Primitive(PrimitiveLiteral::Binary(bytes.to_vec())));
        }
        let schema = IcebergSchema::builder()
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap();
        let columns = [handle(&schema, 23)];
        let mut request = |_: std::alloc::Layout| Ok(());
        let mut work = |_: usize| Ok(());
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        let value = crate::schema_mapping::typed_value::from_declaration(
            schema.as_struct().fields()[0].field_type.as_ref(),
            true,
            Some(NominalSourceDomain::LargeInt.logical_type()),
            None,
            &mut creation,
        )
        .unwrap();
        assert_eq!(
            value.binding.logical_type(),
            novarocks_type_contract::ValueLogicalType::LargeInt
        );
        let empty = Arc::new(ArrowSchema::empty());
        let binding = bind_scan_columns(empty_binding_request(&schema, &empty, &columns)).unwrap();
        assert!(
            !binding.columns()[0]
                .target()
                .metadata()
                .contains_key(novarocks_type_contract::NR_LOGICAL_TYPE_KEY)
        );
        let batch = RecordBatch::try_new_with_options(
            empty,
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(2)),
        )
        .unwrap();
        let facts = IcebergSplitFacts {
            path: "memory://nominal-default",
            file_first_row_id: None,
            data_sequence_number: None,
        };
        let result = binding.materialize(&batch, None, &facts);
        if default.is_some() {
            let plain_columns = [handle(&schema, 23)];
            let plain_binding = bind_scan_columns(empty_binding_request(
                &schema,
                &batch.schema(),
                &plain_columns,
            ))
            .unwrap();
            let original = plain_binding.materialize(&batch, None, &facts).unwrap_err();
            let declared = result.unwrap_err();
            assert_eq!(declared.kind(), original.kind());
            assert_eq!(declared.message(), original.message());
            assert_eq!(
                declared.message(),
                "iceberg initial default: unsupported initial-default for column n: arrow type FixedSizeBinary(16) cannot carry an iceberg default"
            );
        } else {
            let values = result.unwrap();
            let values = values[0]
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            assert_eq!(values.null_count(), 2);
        }
    }
}

#[test]
fn legacy_projection_uses_the_slots_returned_by_the_reader() {
    let schema = table_schema();
    let file_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("old_region", DataType::Utf8, true),
        Field::new("unused", DataType::Int32, true),
        Field::new("old.id", DataType::Int64, false),
    ]));
    let mapping = crate::schema_mapping::parse_name_mapping(
        r#"[{"field-id":1,"names":["old.id"]},{"field-id":2,"names":["old_region"]},{"field-id":3,"names":[]},{"names":["unused"]}]"#,
    )
    .expect("name mapping");
    let columns = vec![handle(&schema, 1), handle(&schema, 2)];
    let binding = bind_scan_columns(IcebergSchemaBindingRequest {
        name_mapping: Some(Arc::new(mapping)),
        ..empty_binding_request(&schema, &file_schema, &columns)
    })
    .expect("binds");
    assert_eq!(binding.root_projection(), &[0, 2]);
    let batch = RecordBatch::try_new(
        Arc::new(file_schema.project(&[0, 2]).expect("reader schema")),
        vec![
            Arc::new(StringArray::from(vec!["old"])),
            Arc::new(Int64Array::from(vec![91])),
        ],
    )
    .expect("physical batch");
    let facts = IcebergSplitFacts {
        path: "legacy.parquet",
        file_first_row_id: None,
        data_sequence_number: None,
    };
    let values = binding.materialize(&batch, None, &facts).expect("columns");
    assert_eq!(
        values[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        91
    );
    assert_eq!(
        values[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "old"
    );

    let wrong = batch.project(&[1, 0]).expect("reordered physical fields");
    assert!(binding.materialize(&wrong, None, &facts).is_err());
}

#[test]
fn nested_reordering_and_defaults_preserve_the_original_parent_mask() {
    let schema = IcebergSchema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            10,
            "details",
            Type::Struct(StructType::new(vec![
                Arc::new(NestedField::required(
                    11,
                    "left",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    12,
                    "right",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(
                    NestedField::required(13, "added", Type::Primitive(PrimitiveType::Long))
                        .with_initial_default(IcebergLiteral::long(17)),
                ),
                Arc::new(NestedField::optional(
                    14,
                    "optional",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])),
        ))])
        .build()
        .expect("table schema");
    let fields = Fields::from(vec![
        Arc::new(
            Field::new("earlier_right", DataType::Int64, false)
                .with_metadata(field_id_metadata(12)),
        ),
        Arc::new(
            Field::new("earlier_left", DataType::Int64, false).with_metadata(field_id_metadata(11)),
        ),
    ]);
    let file_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("earlier_details", DataType::Struct(fields.clone()), true)
            .with_metadata(field_id_metadata(10)),
    ]));
    let stored = StructArray::new(
        fields,
        vec![
            Arc::new(Int64Array::from(vec![91, 92])),
            Arc::new(Int64Array::from(vec![15, 16])),
        ],
        Some(NullBuffer::from(vec![true, false])),
    );
    let batch = RecordBatch::try_new(file_schema.clone(), vec![Arc::new(stored)]).unwrap();
    let root = handle(&schema, 10);
    let columns = vec![
        root.clone(),
        root.dereference(&[13]).expect("default field"),
    ];
    let binding =
        bind_scan_columns(empty_binding_request(&schema, &file_schema, &columns)).expect("binds");
    let facts = IcebergSplitFacts {
        path: "evolved.parquet",
        file_first_row_id: None,
        data_sequence_number: None,
    };
    let values = binding.materialize(&batch, None, &facts).expect("columns");
    let details = values[0].as_any().downcast_ref::<StructArray>().unwrap();
    assert!(details.is_null(1));
    assert_eq!(
        details
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        15
    );
    assert_eq!(
        details
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        91
    );
    assert!(details.column(3).is_null(0));
    let added = values[1].as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(added.value(0), 17);
    assert!(added.is_null(1));
}

#[test]
fn missing_required_list_member_only_fails_for_reachable_elements() {
    use crate::iceberg::spec::ListType;
    use arrow::array::ListArray;
    use arrow::buffer::OffsetBuffer;

    let schema = IcebergSchema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1,
            "items",
            Type::List(ListType {
                element_field: Arc::new(NestedField::list_element(
                    2,
                    Type::Struct(StructType::new(vec![Arc::new(NestedField::required(
                        3,
                        "new_child",
                        Type::Primitive(PrimitiveType::Long),
                    ))])),
                    false,
                )),
            }),
        ))])
        .build()
        .expect("table schema");
    let fields = Fields::from(vec![Arc::new(
        Field::new("old_child", DataType::Int64, true).with_metadata(field_id_metadata(4)),
    )]);
    let items = StructArray::new(
        fields.clone(),
        vec![Arc::new(Int64Array::from(vec![19, 25]))],
        None,
    );
    let element = Arc::new(
        Field::new("legacy_array", DataType::Struct(fields), true)
            .with_metadata(field_id_metadata(2)),
    );
    let physical = ListArray::try_new(
        element,
        OffsetBuffer::new(vec![0, 1, 2].into()),
        Arc::new(items),
        Some(NullBuffer::from(vec![false, true])),
    )
    .expect("physical array");
    let file_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("legacy_items", physical.data_type().clone(), true)
            .with_metadata(field_id_metadata(1)),
    ]));
    let batch = RecordBatch::try_new(file_schema.clone(), vec![Arc::new(physical)]).unwrap();
    let columns = [handle(&schema, 1)];
    let binding =
        bind_scan_columns(empty_binding_request(&schema, &file_schema, &columns)).unwrap();
    let facts = IcebergSplitFacts {
        path: "older-list.parquet",
        file_first_row_id: None,
        data_sequence_number: None,
    };
    let concealed = binding
        .materialize(&batch.slice(0, 1), None, &facts)
        .unwrap();
    let concealed = concealed[0].as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(concealed.value_offsets(), &[0, 1]);
    assert!(concealed.is_null(0));
    let error = binding
        .materialize(&batch.slice(1, 1), None, &facts)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("missing required field new_child")
    );
}
