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
use crate::access_binding::IcebergReadBinding;
use crate::commit::frozen_write::{FrozenDataWriteFacts, staged_write_context_from_frozen_facts};
use arrow::array::{BinaryArray, ListArray, MapArray, StringArray, TimestampMicrosecondArray};
use arrow::buffer::OffsetBuffer;
use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn schema() -> Schema {
    serde_json::from_value(serde_json::json!({"type":"struct","schema-id":42,"fields":[
        {"id":1,"name":"record","required":false,"type":{"type":"struct","fields":[
            {"id":2,"name":"blob","required":false,"type":"binary"},
            {"id":3,"name":"zone","required":false,"type":"timestamptz"},
            {"id":4,"name":"items","required":false,"type":{"type":"list","element-id":5,"element-required":false,"element":"binary"}}
        ]}},
        {"id":6,"name":"mapping","required":false,"type":{"type":"map","key-id":7,"key":"string","value-id":8,"value-required":false,"value":{"type":"struct","fields":[
            {"id":9,"name":"blob","required":false,"type":"binary"},
            {"id":10,"name":"zone","required":false,"type":"timestamptz"}
        ]}}}
    ]})).unwrap()
}
fn input(schema: &Schema) -> RecordBatch {
    let arrow = annotated_read_schema(schema).unwrap();
    let DataType::Struct(record_fields) = arrow.field(0).data_type() else {
        panic!("record");
    };
    let DataType::List(element) = record_fields[2].data_type() else {
        panic!("list");
    };
    let items = ListArray::try_new(
        element.clone(),
        OffsetBuffer::new(vec![0, 2, 2, 3].into()),
        Arc::new(BinaryArray::from(vec![
            Some(&[1_u8][..]),
            None,
            Some(&[2_u8][..]),
        ])),
        Some(NullBuffer::from(vec![true, false, true])),
    )
    .unwrap();
    let record = StructArray::try_new(
        record_fields.clone(),
        vec![
            Arc::new(BinaryArray::from(vec![
                Some(&[0xff_u8, 0][..]),
                None,
                Some(&[][..]),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(123_i64),
                Some(456),
                None,
            ])),
            Arc::new(items),
        ],
        Some(NullBuffer::from(vec![true, false, true])),
    )
    .unwrap();
    let DataType::Map(entry_field, sorted) = arrow.field(1).data_type() else {
        panic!("map");
    };
    let DataType::Struct(entry_fields) = entry_field.data_type() else {
        panic!("entries");
    };
    let DataType::Struct(value_fields) = entry_fields[1].data_type() else {
        panic!("value");
    };
    let values = StructArray::try_new(
        value_fields.clone(),
        vec![
            Arc::new(BinaryArray::from(vec![Some(&[4_u8, 5][..]), None])),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(-123_i64), None])),
        ],
        Some(NullBuffer::from(vec![true, false])),
    )
    .unwrap();
    let entries = StructArray::try_new(
        entry_fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(values),
        ],
        None,
    )
    .unwrap();
    let mapping = MapArray::try_new(
        entry_field.clone(),
        OffsetBuffer::new(vec![0, 1, 1, 2].into()),
        entries,
        Some(NullBuffer::from(vec![true, false, true])),
        *sorted,
    )
    .unwrap();
    RecordBatch::try_new(arrow, vec![Arc::new(record), Arc::new(mapping)]).unwrap()
}
fn assert_values(arrays: &[ArrayRef]) {
    let record = arrays[0].as_any().downcast_ref::<StructArray>().unwrap();
    assert!(record.is_null(1));
    let blob = record
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(blob.value(0), &[0xff, 0]);
    assert_eq!(blob.value(2), &[] as &[u8]);
    let zone = record
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(zone.value(0), 123);
    assert!(zone.is_null(2));
    assert_eq!(zone.timezone(), None);
    let list = record
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_offsets(), &[0, 2, 2, 3]);
    assert!(list.is_null(1));
    let values = list
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(values.value(0), &[1]);
    assert!(values.is_null(1));
    assert_eq!(values.value(2), &[2]);
    let map = arrays[1].as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_offsets(), &[0, 1, 1, 2]);
    assert!(map.is_null(1));
    let values = map.values().as_any().downcast_ref::<StructArray>().unwrap();
    assert!(values.is_null(1));
    let blob = values
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(blob.value(0), &[4, 5]);
    let zone = values
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(zone.value(0), -123);
    assert_eq!(zone.timezone(), None);
}
#[tokio::test]
async fn sdk_file_nested_binary_and_timestamptz_bind_to_sql_carriers() {
    let dir = tempfile::tempdir().unwrap();
    let location = format!("file://{}", dir.path().display());
    let table_schema = schema();
    let input = input(&table_schema);
    let runtime = tokio::runtime::Handle::current();
    let binding = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(runtime.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
        Arc::new(TokioFileIoSpawner::new(runtime)),
    );
    let facts = FrozenDataWriteFacts {
        format_version: 3,
        table_location: location.clone(),
        data_location: format!("{location}/data"),
        target_partition_spec_id: 0,
        partition_source_column_names: vec![],
        partition_column_names: vec![],
        transform_exprs: vec![],
        target_schema_json: serde_json::to_string(&table_schema).unwrap(),
        parquet_row_group_size_bytes:
            crate::commit::data_writer::DEFAULT_PARQUET_ROW_GROUP_SIZE_BYTES,
        target_file_size_bytes: crate::catalog_config::DEFAULT_TARGET_FILE_SIZE_BYTES,
        max_partitions_per_writer: crate::catalog_config::DEFAULT_MAX_PARTITIONS_PER_WRITER,
    };
    let context = staged_write_context_from_frozen_facts(&binding, &input.schema(), facts).unwrap();
    let files = crate::commit::data_writer::write_record_batches(&context, vec![input])
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    let path = files[0].data_file.file_path();
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(path.strip_prefix("file://").unwrap_or(path)).unwrap(),
    )
    .unwrap();
    let file_schema = builder.schema().clone();
    let DataType::Struct(fields) = file_schema.field(0).data_type() else {
        panic!("record file carrier");
    };
    assert_eq!(fields[0].data_type(), &DataType::LargeBinary);
    assert_eq!(
        fields[1].data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
    );
    let columns = [
        IcebergColumnHandle::base_column_of(&table_schema, 1).unwrap(),
        IcebergColumnHandle::base_column_of(&table_schema, 6).unwrap(),
    ];
    let scan = bind_scan_columns(IcebergSchemaBindingRequest {
        table_schema: &table_schema,
        file_schema: &file_schema,
        name_mapping: None,
        partition_spec: None,
        partition_values: None,
        columns: &columns,
    })
    .unwrap();
    let batch = builder.build().unwrap().next().unwrap().unwrap();
    let facts = IcebergSplitFacts {
        path,
        file_first_row_id: None,
        data_sequence_number: None,
    };
    let arrays = scan.materialize(&batch, None, &facts).unwrap();
    assert_values(&arrays);
    let sliced = scan.materialize(&batch.slice(1, 2), None, &facts).unwrap();
    let record = sliced[0].as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(record.len(), 2);
    assert!(record.is_null(0));
    let list = record
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_offsets(), &[2, 2, 3]);
    let map = sliced[1].as_any().downcast_ref::<MapArray>().unwrap();
    assert!(map.is_null(0));
    assert_eq!(map.value_offsets(), &[1, 1, 2]);
}

#[tokio::test]
async fn recursive_nominal_largeint_sdk_file_preserves_nested_values_and_storage() {
    use crate::schema_mapping::nominal_declaration;
    use arrow::array::{FixedSizeBinaryArray, Int32Array};
    use novarocks_spi::connector::{ConnectorColumnDefinition, ConnectorDataType as D};
    let column = |name: &str, data_type| ConnectorColumnDefinition {
        name: name.into(),
        data_type,
        nullable: true,
        default: None,
    };
    let columns = vec![
        column("numbers", D::Array(Box::new(D::LargeInt))),
        column("mapping", D::Map(Box::new(D::Int), Box::new(D::LargeInt))),
    ];
    let schema = Schema::builder()
        .with_fields(crate::catalog_control::type_mapping::schema_fields(&columns).unwrap())
        .build()
        .unwrap();
    let declarations = nominal_declaration::from_columns(&schema, &columns).unwrap();
    let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
        schema,
        crate::iceberg::spec::PartitionSpec::unpartition_spec(),
        crate::iceberg::spec::SortOrder::unsorted_order(),
        "memory://nested-largeint".into(),
        crate::iceberg::spec::FormatVersion::V3,
        std::collections::HashMap::from([(
            nominal_declaration::PROPERTY.into(),
            nominal_declaration::encode(&declarations).unwrap(),
        )]),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let table_schema = metadata.current_schema();
    let arrow = crate::scalar_integer_domain::metadata_sql_schema(&metadata, table_schema).unwrap();
    let bytes = [
        i128::MAX.to_be_bytes(),
        i128::MIN.to_be_bytes(),
        (-1_i128).to_be_bytes(),
    ];
    let values = |input: Vec<Option<&[u8]>>| -> ArrayRef {
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(input.into_iter(), 16).unwrap(),
        )
    };
    let DataType::List(item) = arrow.field(0).data_type() else {
        panic!("list");
    };
    let numbers = ListArray::try_new(
        item.clone(),
        OffsetBuffer::new(vec![0, 2, 2, 4].into()),
        values(vec![
            Some(&bytes[0]),
            None,
            Some(&bytes[1]),
            Some(&bytes[2]),
        ]),
        Some(NullBuffer::from(vec![true, false, true])),
    )
    .unwrap();
    let DataType::Map(entries, sorted) = arrow.field(1).data_type() else {
        panic!("map");
    };
    let DataType::Struct(fields) = entries.data_type() else {
        panic!("entries");
    };
    let entries_array = StructArray::try_new(
        fields.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            values(vec![Some(&bytes[0]), Some(&bytes[1]), None]),
        ],
        None,
    )
    .unwrap();
    let mapping = MapArray::try_new(
        entries.clone(),
        OffsetBuffer::new(vec![0, 1, 1, 3].into()),
        entries_array,
        Some(NullBuffer::from(vec![true, false, true])),
        *sorted,
    )
    .unwrap();
    let input = RecordBatch::try_new(arrow, vec![Arc::new(numbers), Arc::new(mapping)]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let location = format!("file://{}", dir.path().display());
    let runtime = tokio::runtime::Handle::current();
    let binding = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(runtime.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
        Arc::new(TokioFileIoSpawner::new(runtime)),
    );
    let context = staged_write_context_from_frozen_facts(
        &binding,
        &input.schema(),
        FrozenDataWriteFacts {
            format_version: 3,
            table_location: location.clone(),
            data_location: format!("{location}/data"),
            target_partition_spec_id: 0,
            partition_source_column_names: vec![],
            partition_column_names: vec![],
            transform_exprs: vec![],
            target_schema_json: serde_json::to_string(table_schema).unwrap(),
            parquet_row_group_size_bytes:
                crate::commit::data_writer::DEFAULT_PARQUET_ROW_GROUP_SIZE_BYTES,
            target_file_size_bytes: crate::catalog_config::DEFAULT_TARGET_FILE_SIZE_BYTES,
            max_partitions_per_writer: crate::catalog_config::DEFAULT_MAX_PARTITIONS_PER_WRITER,
        },
    )
    .unwrap();
    let files = crate::commit::data_writer::write_record_batches(&context, vec![input])
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    let path = files[0].data_file.file_path();
    let reader = ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(path.strip_prefix("file://").unwrap()).unwrap(),
    )
    .unwrap();
    let columns = [
        IcebergColumnHandle::base_column_of(table_schema, 1).unwrap(),
        IcebergColumnHandle::base_column_of(table_schema, 2).unwrap(),
    ];
    let scan = bind_scan_columns(IcebergSchemaBindingRequest {
        table_schema,
        file_schema: reader.schema(),
        name_mapping: None,
        partition_spec: None,
        partition_values: None,
        columns: &columns,
    })
    .unwrap();
    let batch = reader.build().unwrap().next().unwrap().unwrap();
    let facts = IcebergSplitFacts {
        path,
        file_first_row_id: None,
        data_sequence_number: None,
    };
    let output = scan.materialize(&batch, None, &facts).unwrap();
    let list = output[0].as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 2, 2, 4]);
    assert!(list.is_null(1));
    let actual = list
        .values()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(actual.value(0), bytes[0]);
    assert!(actual.is_null(1));
    assert_eq!(actual.value(2), bytes[1]);
    assert_eq!(actual.value(3), bytes[2]);
    let map = output[1].as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_offsets(), &[0, 1, 1, 3]);
    assert!(map.is_null(1));
    let actual = map
        .values()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(actual.value(0), bytes[0]);
    assert_eq!(actual.value(1), bytes[1]);
    assert!(actual.is_null(2));
    let mut request = |_: std::alloc::Layout| Ok(());
    let mut work = |_: usize| Ok(());
    let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut work,
    );
    let exact =
        crate::schema_mapping::catalog_value::table(&metadata, table_schema, &[], &mut creation)
            .unwrap();
    assert_eq!(
        exact
            .binding(0)
            .unwrap()
            .child_at(0)
            .unwrap()
            .logical_type(),
        novarocks_type_contract::ValueLogicalType::LargeInt
    );
    assert_eq!(
        exact
            .binding(1)
            .unwrap()
            .child_at(1)
            .unwrap()
            .logical_type(),
        novarocks_type_contract::ValueLogicalType::LargeInt
    );
    let sliced = scan.materialize(&batch.slice(1, 2), None, &facts).unwrap();
    let list = sliced[0].as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[2, 2, 4]);
    assert!(list.is_null(0));
}
#[test]
fn recursive_representation_keeps_closed_leaf_and_container_rules() {
    let list = |ty| DataType::List(Arc::new(Field::new("element", ty, true)));
    assert_eq!(
        physical_adaptation(&list(DataType::LargeBinary), &list(DataType::Binary), false).unwrap(),
        IcebergPhysicalAdaptation::RepresentationCast
    );
    assert!(physical_adaptation(&list(DataType::Int32), &list(DataType::Int64), false).is_err());
    assert!(
        physical_adaptation(
            &list(DataType::LargeBinary),
            &DataType::LargeList(Arc::new(Field::new("element", DataType::Binary, true))),
            false
        )
        .is_err()
    );
}
