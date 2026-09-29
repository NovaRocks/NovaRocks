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

use super::*;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, Int8Array, Int16Array, Int32Array,
    LargeBinaryArray, ListArray, MapArray, StructArray, TimestampMicrosecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;
use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{LogicalType, Type as ParquetPhysicalType};
use std::collections::HashMap;
use std::fs::File;

fn local_binding() -> IcebergReadBinding {
    let runtime = tokio::runtime::Handle::current();
    IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(runtime.clone())),
        Arc::new(TokioFileTaskSpawner::new(runtime)),
    )
}

fn frozen_field(
    id: i32,
    name: &str,
    children: Vec<crate::scan_model::IcebergSchemaFieldDef>,
) -> crate::scan_model::IcebergSchemaFieldDef {
    crate::scan_model::IcebergSchemaFieldDef {
        field_id: id,
        name: name.to_string(),
        initial_default: None,
        write_default: None,
        initial_default_json: None,
        write_default_json: None,
        children,
    }
}

fn frozen_facts(
    location: &str,
    fields: Vec<crate::scan_model::IcebergSchemaFieldDef>,
) -> FrozenDataWriteFacts {
    FrozenDataWriteFacts {
        table_location: location.to_string(),
        data_location: format!("{location}/data"),
        target_partition_spec_id: 7,
        partition_source_column_names: Vec::new(),
        partition_column_names: Vec::new(),
        transform_exprs: Vec::new(),
        data_input_schema: IcebergSchemaDef { fields },
        parquet_row_group_size_bytes: Some(1024),
    }
}

fn bytes_at(array: &ArrayRef, row: usize) -> &[u8] {
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return array.value(row);
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return array.value(row);
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryViewArray>() {
        return array.value(row);
    }
    panic!("unexpected binary carrier {:?}", array.data_type());
}

fn field_with_marker(name: &str, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
        "writer-regression".to_string(),
        name.to_string(),
    )]))
}

#[tokio::test]
async fn frozen_narrow_context_writes_canonical_int32_parquet_and_preserves_other_domains() {
    let dir = tempfile::tempdir().unwrap();
    let location = format!("file://{}", dir.path().display());
    let list_child = Arc::new(field_with_marker("element", DataType::Int8, true));
    let struct_child = Arc::new(field_with_marker("child", DataType::Int16, true));
    let map_key = Arc::new(field_with_marker("key", DataType::Int8, false));
    let map_value = Arc::new(field_with_marker("value", DataType::Int16, true));
    let entries_field = Arc::new(field_with_marker(
        "entries",
        DataType::Struct(vec![map_key.clone(), map_value.clone()].into()),
        false,
    ));
    // The standard Iceberg writer marks VARIANT with this Arrow extension.
    // Preserve that existing field metadata through integer canonicalization.
    let variant_metadata = HashMap::from([
        ("writer-regression".to_string(), "variant".to_string()),
        (
            "ARROW:extension:name".to_string(),
            "arrow.parquet.variant".to_string(),
        ),
        ("ARROW:extension:metadata".to_string(), String::new()),
    ]);
    let fields = vec![
        field_with_marker("tiny", DataType::Int8, true),
        field_with_marker("small", DataType::Int16, true),
        field_with_marker("list", DataType::List(list_child.clone()), true),
        field_with_marker(
            "record",
            DataType::Struct(vec![struct_child.clone()].into()),
            true,
        ),
        field_with_marker("mapping", DataType::Map(entries_field.clone(), false), true),
        field_with_marker("raw", DataType::Binary, true),
        field_with_marker(
            "zoned",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("variant", DataType::LargeBinary, true).with_metadata(variant_metadata),
    ];
    let input_schema = Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([(
            "writer-schema-regression".to_string(),
            "preserved".to_string(),
        )]),
    ));
    let facts = frozen_facts(
        &location,
        vec![
            frozen_field(1, "tiny", vec![]),
            frozen_field(2, "small", vec![]),
            frozen_field(3, "list", vec![frozen_field(31, "element", vec![])]),
            frozen_field(4, "record", vec![frozen_field(41, "child", vec![])]),
            frozen_field(
                5,
                "mapping",
                vec![
                    frozen_field(51, "key", vec![]),
                    frozen_field(52, "value", vec![]),
                ],
            ),
            frozen_field(6, "raw", vec![]),
            frozen_field(7, "zoned", vec![]),
            frozen_field(8, "variant", vec![]),
        ],
    );
    let ctx =
        staged_write_context_from_frozen_facts(&local_binding(), &input_schema, facts).unwrap();
    assert_eq!(ctx.partition_spec_id(), 7);
    for id in [1, 2, 31, 41, 51, 52] {
        assert_eq!(
            ctx.metadata()
                .current_schema()
                .field_by_id(id)
                .unwrap()
                .field_type
                .as_ref(),
            &Type::Primitive(PrimitiveType::Int)
        );
        assert_eq!(
            ctx.schema().field_by_id(id).unwrap().field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Int)
        );
    }
    assert_eq!(
        ctx.schema().field_by_id(6).unwrap().field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Binary)
    );
    assert_eq!(
        ctx.schema().field_by_id(7).unwrap().field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Timestamptz)
    );
    assert_eq!(
        ctx.schema().field_by_id(8).unwrap().field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Variant)
    );
    let parent_nulls = Some(NullBuffer::from(vec![true, false, true]));
    let list = ListArray::try_new(
        list_child,
        OffsetBuffer::new(vec![0, 2, 2, 3].into()),
        Arc::new(Int8Array::from(vec![Some(-128), None, Some(127)])),
        parent_nulls.clone(),
    )
    .unwrap();
    let record = StructArray::try_new(
        vec![struct_child].into(),
        vec![Arc::new(Int16Array::from(vec![
            Some(-32768),
            Some(0),
            None,
        ]))],
        Some(NullBuffer::from(vec![true, true, false])),
    )
    .unwrap();
    let entries = StructArray::try_new(
        vec![map_key, map_value].into(),
        vec![
            Arc::new(Int8Array::from(vec![-128, 127])),
            Arc::new(Int16Array::from(vec![Some(-32768), None])),
        ],
        None,
    )
    .unwrap();
    let mapping = MapArray::try_new(
        entries_field,
        OffsetBuffer::new(vec![0, 1, 1, 2].into()),
        entries,
        parent_nulls,
        false,
    )
    .unwrap();
    // Independent valid Iceberg VARIANT representation of the string "hello".
    // Prefix=metadata length3 + value length6; metadata=empty dictionary v1.
    let variant_payload = [9, 0, 0, 0, 1, 0, 0, 21, b'h', b'e', b'l', b'l', b'o'];
    let batch = RecordBatch::try_new(
        input_schema.clone(),
        vec![
            Arc::new(Int8Array::from(vec![Some(-128), None, Some(127)])),
            Arc::new(Int16Array::from(vec![Some(-32768), Some(0), None])),
            Arc::new(list),
            Arc::new(record),
            Arc::new(mapping),
            Arc::new(BinaryArray::from(vec![
                Some(&[0xff, 0][..]),
                None,
                Some(&[][..]),
            ])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(0), None, Some(123456789)])
                    .with_timezone("UTC"),
            ),
            Arc::new(LargeBinaryArray::from(vec![
                Some(variant_payload.as_slice()),
                None,
                Some(variant_payload.as_slice()),
            ])),
        ],
    )
    .unwrap();
    assert_eq!(batch.column(0).data_type(), &DataType::Int8);
    assert_eq!(batch.column(1).data_type(), &DataType::Int16);
    let files = crate::commit::data_writer::write_record_batches(&ctx, vec![batch])
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    let data_file = &files[0].data_file;
    assert_eq!(data_file.record_count(), 3);
    assert_eq!(files[0].partition_spec_id, 7);
    // These are standard Iceberg INT bounds, never an I8/I16 private encoding.
    for (id, lower, upper) in [(1, -128_i32, 127_i32), (2, -32768, 0)] {
        let lb = data_file.lower_bounds().get(&id).unwrap();
        let ub = data_file.upper_bounds().get(&id).unwrap();
        assert_eq!(lb.data_type(), &PrimitiveType::Int);
        assert_eq!(ub.data_type(), &PrimitiveType::Int);
        assert_eq!(
            lb.to_bytes().unwrap().as_ref(),
            lower.to_le_bytes().as_slice()
        );
        assert_eq!(
            ub.to_bytes().unwrap().as_ref(),
            upper.to_le_bytes().as_slice()
        );
        assert_eq!(data_file.null_value_counts().get(&id), Some(&1));
    }
    let path = data_file.file_path();
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        File::open(path.strip_prefix("file://").unwrap_or(path)).unwrap(),
    )
    .unwrap();
    for id in [1, 2, 31, 41, 51, 52] {
        let column = builder
            .parquet_schema()
            .columns()
            .iter()
            .find(|column| {
                column.self_type().get_basic_info().has_id()
                    && column.self_type().get_basic_info().id() == id
            })
            .unwrap();
        assert_eq!(column.physical_type(), ParquetPhysicalType::INT32);
        assert!(
            !matches!(
                column.logical_type_ref(),
                Some(LogicalType::Integer {
                    bit_width: 8 | 16,
                    ..
                })
            ),
            "physical storage must not carry a narrow logical label"
        );
    }
    let variant_field = builder
        .parquet_schema()
        .root_schema()
        .get_fields()
        .iter()
        .find(|field| field.name() == "variant")
        .unwrap();
    assert!(matches!(
        variant_field.get_basic_info().logical_type_ref(),
        Some(LogicalType::Variant { .. })
    ));
    let output_schema = builder.schema().clone();
    assert_eq!(output_schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(output_schema.field(1).data_type(), &DataType::Int32);
    assert_eq!(
        output_schema.field(6).data_type(),
        input_schema.field(6).data_type()
    );
    assert!(output_schema.field(0).is_nullable());
    assert_eq!(
        output_schema
            .field(7)
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("arrow.parquet.variant")
    );
    assert_eq!(
        output_schema
            .metadata()
            .get("writer-schema-regression")
            .map(String::as_str),
        Some("preserved")
    );
    for (index, id) in [1, 2, 3, 4, 5, 6, 7, 8].into_iter().enumerate() {
        assert_eq!(
            output_schema
                .field(index)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY),
            Some(&id.to_string())
        );
        assert_eq!(
            output_schema
                .field(index)
                .metadata()
                .get("writer-regression")
                .map(String::as_str),
            Some(input_schema.field(index).name().as_str())
        );
    }
    let mut reader = builder.with_batch_size(1024).build().unwrap();
    let out = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    let tiny = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(
        tiny.iter().collect::<Vec<_>>(),
        vec![Some(-128), None, Some(127)]
    );
    let small = out.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(
        small.iter().collect::<Vec<_>>(),
        vec![Some(-32768), Some(0), None]
    );
    let list = out.column(2).as_any().downcast_ref::<ListArray>().unwrap();
    assert!(list.is_null(1));
    let values = list.values().as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some(-128), None, Some(127)]
    );
    assert_eq!(list.value_offsets(), &[0, 2, 2, 3]);
    let record = out
        .column(3)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(record.is_null(2));
    let values = record
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.value(0), -32768);
    assert_eq!(values.value(1), 0);
    let mapping = out.column(4).as_any().downcast_ref::<MapArray>().unwrap();
    assert!(mapping.is_null(1));
    assert_eq!(mapping.value_offsets(), &[0, 1, 1, 2]);
    let keys = mapping
        .keys()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(keys.iter().collect::<Vec<_>>(), vec![Some(-128), Some(127)]);
    let values = mapping
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(-32768), None]);
    let raw = out
        .column(5)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(raw.value(0), &[0xff, 0]);
    assert!(raw.is_null(1));
    assert_eq!(raw.value(2), &[] as &[u8]);
    let zoned = out
        .column(6)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(
        zoned.iter().collect::<Vec<_>>(),
        vec![Some(0), None, Some(123456789)]
    );
    let variant = out
        .column(7)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(variant.is_null(1));
    for row in [0, 2] {
        assert_eq!(
            bytes_at(variant.column_by_name("metadata").unwrap(), row),
            &[1, 0, 0]
        );
        assert_eq!(
            bytes_at(variant.column_by_name("value").unwrap(), row),
            &[21, b'h', b'e', b'l', b'l', b'o']
        );
    }
}

#[tokio::test]
async fn frozen_narrow_identity_partition_writes_standard_int_files_under_exact_spec() {
    let dir = tempfile::tempdir().unwrap();
    let location = format!("file://{}", dir.path().display());
    let schema = Arc::new(Schema::new(vec![
        Field::new("tiny", DataType::Int8, false),
        Field::new("small", DataType::Int16, true),
    ]));
    let mut facts = frozen_facts(
        &location,
        vec![
            frozen_field(11, "tiny", vec![]),
            frozen_field(12, "small", vec![]),
        ],
    );
    facts.partition_source_column_names = vec!["tiny".to_string()];
    facts.partition_column_names = vec!["tiny".to_string()];
    facts.transform_exprs = vec!["identity".to_string()];
    let ctx = staged_write_context_from_frozen_facts(&local_binding(), &schema, facts).unwrap();
    assert_eq!(ctx.partition_spec_id(), 7);
    assert_eq!(ctx.partition_spec().fields()[0].source_id, 11);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int8Array::from(vec![-128, -128, 127])),
            Arc::new(Int16Array::from(vec![Some(-32768), None, Some(32767)])),
        ],
    )
    .unwrap();
    let files = crate::commit::data_writer::write_record_batches(&ctx, vec![batch])
        .await
        .unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(
        files
            .iter()
            .map(|file| file.data_file.record_count())
            .sum::<u64>(),
        3
    );
    let mut actual = Vec::new();
    for file in files {
        assert_eq!(file.partition_spec_id, 7);
        let path = file.data_file.file_path();
        let builder = ParquetRecordBatchReaderBuilder::try_new(
            File::open(path.strip_prefix("file://").unwrap_or(path)).unwrap(),
        )
        .unwrap();
        assert_eq!(builder.schema().field(0).data_type(), &DataType::Int32);
        assert!(!builder.schema().field(0).is_nullable());
        assert_eq!(builder.schema().field(1).data_type(), &DataType::Int32);
        let partition = if path.contains("/tiny=-128/") {
            -128
        } else if path.contains("/tiny=127/") {
            127
        } else {
            panic!("unexpected partition path {path}")
        };
        for batch in builder.build().unwrap() {
            let batch = batch.unwrap();
            let tiny = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let small = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(tiny.value(row), partition);
                actual.push((
                    tiny.value(row),
                    (!small.is_null(row)).then(|| small.value(row)),
                ));
            }
        }
    }
    actual.sort();
    assert_eq!(
        actual,
        vec![(-128, None), (-128, Some(-32768)), (127, Some(32767))]
    );
}
