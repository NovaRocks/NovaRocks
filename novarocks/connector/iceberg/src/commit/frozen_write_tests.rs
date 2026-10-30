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
use crate::iceberg::spec::{ListType, MapType, StructType};
use arrow::array::{
    Array, BinaryArray, Int8Array, Int16Array, Int32Array, ListArray, MapArray, StructArray,
    TimestampMicrosecondArray,
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

fn frozen_field(id: i32, name: &str, children: Vec<Arc<NestedField>>) -> Arc<NestedField> {
    let ty = match name {
        "list" => Type::List(ListType::new(children[0].clone())),
        "record" => Type::Struct(StructType::new(children)),
        "mapping" => Type::Map(MapType::new(children[0].clone(), children[1].clone())),
        "raw" => Type::Primitive(PrimitiveType::Binary),
        "zoned" => Type::Primitive(PrimitiveType::Timestamptz),
        _ => Type::Primitive(PrimitiveType::Int),
    };
    Arc::new(NestedField::new(id, name, ty, name == "key"))
}

fn frozen_facts(location: &str, fields: Vec<Arc<NestedField>>) -> FrozenDataWriteFacts {
    FrozenDataWriteFacts {
        format_version: 2,
        table_location: location.to_string(),
        data_location: format!("{location}/data"),
        target_partition_spec_id: 7,
        partition_source_column_names: Vec::new(),
        partition_column_names: Vec::new(),
        transform_exprs: Vec::new(),
        target_schema_json: serde_json::to_string(
            &crate::iceberg::spec::Schema::builder()
                .with_schema_id(19)
                .with_fields(fields)
                .build()
                .unwrap(),
        )
        .unwrap(),
        parquet_row_group_size_bytes: Some(1024),
    }
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
    let list_child = Arc::new(field_with_marker("element", DataType::Int32, true));
    let struct_child = Arc::new(field_with_marker("child", DataType::Int32, true));
    let map_key = Arc::new(field_with_marker("key", DataType::Int32, false));
    let map_value = Arc::new(field_with_marker("value", DataType::Int32, true));
    let entries_field = Arc::new(field_with_marker(
        "entries",
        DataType::Struct(vec![map_key.clone(), map_value.clone()].into()),
        false,
    ));
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
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
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
    let parent_nulls = Some(NullBuffer::from(vec![true, false, true]));
    let list = ListArray::try_new(
        list_child,
        OffsetBuffer::new(vec![0, 2, 2, 3].into()),
        Arc::new(Int32Array::from(vec![Some(-128), None, Some(127)])),
        parent_nulls.clone(),
    )
    .unwrap();
    let record = StructArray::try_new(
        vec![struct_child].into(),
        vec![Arc::new(Int32Array::from(vec![
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
            Arc::new(Int32Array::from(vec![-128, 127])),
            Arc::new(Int32Array::from(vec![Some(-32768), None])),
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
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(0),
                None,
                Some(123456789),
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
                Some(LogicalType::Integer(value)) if matches!(value.bit_width, 8 | 16)
            ),
            "physical storage must not carry a narrow logical label"
        );
    }
    let output_schema = builder.schema().clone();
    assert_eq!(output_schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(output_schema.field(1).data_type(), &DataType::Int32);
    assert_eq!(
        output_schema.field(6).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
    );
    assert!(output_schema.field(0).is_nullable());
    assert_eq!(
        output_schema
            .metadata()
            .get("writer-schema-regression")
            .map(String::as_str),
        None
    );
    for (index, id) in [1, 2, 3, 4, 5, 6, 7].into_iter().enumerate() {
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
            None
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
        .downcast_ref::<arrow::array::LargeBinaryArray>()
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
            Arc::new(NestedField::required(
                11,
                "tiny",
                Type::Primitive(PrimitiveType::Int),
            )),
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

#[tokio::test]
async fn frozen_replacement_after_images_partition_new_values_and_preserve_lineage_and_null() {
    use crate::iceberg::spec::{Literal, Struct};
    use arrow::array::Int64Array;
    for (transform, after, partition) in [("identity", 11_i32, 11_i32), ("truncate[10]", 11, 10)] {
        let dir = tempfile::tempdir().unwrap();
        let location = format!("file://{}", dir.path().display());
        let schema = Arc::new(Schema::new(vec![
            Field::new("p", DataType::Int32, true),
            Field::new(
                crate::row_lineage_synth::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                true,
            ),
            Field::new(
                crate::row_lineage_synth::ICEBERG_LAST_UPDATED_SEQ_COL,
                DataType::Int64,
                true,
            ),
        ]));
        let mut facts = frozen_facts(
            &location,
            vec![Arc::new(NestedField::optional(
                11,
                "p",
                Type::Primitive(PrimitiveType::Int),
            ))],
        );
        facts.format_version = 3;
        facts.partition_source_column_names = vec!["p".into()];
        facts.partition_column_names = vec!["new_partition".into()];
        facts.transform_exprs = vec![transform.into()];
        let context =
            staged_write_context_from_frozen_facts(&local_binding(), &schema, facts).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(after), None])),
                Arc::new(Int64Array::from(vec![Some(41), Some(42)])),
                Arc::new(Int64Array::from(vec![None, None])),
            ],
        )
        .unwrap();
        let files = crate::commit::data_writer::write_record_batches(&context, vec![batch])
            .await
            .unwrap();
        assert_eq!(
            files.len(),
            2,
            "NULL and the new transformed value are distinct partitions"
        );
        let mut actual = Vec::new();
        for file in files {
            assert_eq!(file.partition_spec_id, 7);
            let is_null = file.data_file.partition() == &Struct::from_iter([None]);
            if !is_null {
                assert_eq!(
                    file.data_file.partition(),
                    &Struct::from_iter([Some(Literal::int(partition))])
                );
            }
            let path = file.data_file.file_path();
            let reader = ParquetRecordBatchReaderBuilder::try_new(
                File::open(path.strip_prefix("file://").unwrap_or(path)).unwrap(),
            )
            .unwrap();
            for batch in reader.build().unwrap() {
                let batch = batch.unwrap();
                let p = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let id = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let sequence = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    assert!(
                        sequence.is_null(row),
                        "Replace inherits actual commit sequence"
                    );
                    assert_eq!(p.is_null(row), is_null);
                    actual.push((id.value(row), (!p.is_null(row)).then(|| p.value(row))));
                }
            }
        }
        actual.sort();
        assert_eq!(
            actual,
            [(41, Some(after)), (42, None)],
            "new partitioning must not mint a new logical row identity"
        );
    }
}
