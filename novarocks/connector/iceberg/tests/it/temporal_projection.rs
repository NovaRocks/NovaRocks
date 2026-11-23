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

use arrow::datatypes::{DataType, TimeUnit};
use novarocks_connector_iceberg::iceberg::spec::{
    FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
    TableMetadataBuilder, Transform, Type,
};
use novarocks_connector_iceberg::metadata_batch_reader::{
    MetadataTableType, metadata_output_execution_schema, metadata_output_schema,
    metadata_table_output_columns,
};
use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
use novarocks_type_contract::{
    TemporalPrecision, TemporalType, ValueKind, ValueTypeStorageLifetime,
};
use std::{collections::HashMap, sync::Arc};

#[test]
fn temporal_projection_preserves_identity_partition_type_and_metadata_time() {
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                3,
                "local_time",
                Type::Primitive(PrimitiveType::Time),
            )),
            Arc::new(NestedField::required(
                7,
                "instant",
                Type::Primitive(PrimitiveType::TimestamptzNs),
            )),
        ])
        .build()
        .unwrap();
    let spec = PartitionSpec::builder(Arc::new(schema.clone()))
        .add_partition_field("local_time", "local_time", Transform::Identity)
        .unwrap()
        .add_partition_field("instant", "instant", Transform::Identity)
        .unwrap()
        .build()
        .unwrap();
    let metadata = TableMetadataBuilder::new(
        schema,
        spec.into_unbound(),
        SortOrder::unsorted_order(),
        "file:///temporal-projection".into(),
        FormatVersion::V3,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let mut request = |_| Ok(());
    let mut work = |_| Ok(());
    let mut creation =
        ConnectorReadSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut work);

    let files =
        metadata_table_output_columns(MetadataTableType::Files, &metadata, &mut creation).unwrap();
    let format = metadata_output_schema(&files).unwrap();
    let execution = metadata_output_execution_schema(&files, &mut creation).unwrap();
    let ordinal = format.index_of("partition").unwrap();
    let DataType::Struct(physical) = format.field(ordinal).data_type() else {
        panic!("partition format row");
    };
    assert_eq!(
        physical[0].data_type(),
        &DataType::Time64(TimeUnit::Microsecond)
    );
    assert_eq!(
        physical[1].data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );

    let binding = execution.binding(ordinal).unwrap();
    let ValueKind::Struct(fields) = binding.value_type().kind() else {
        panic!("partition execution row");
    };
    for (field, ty) in fields.iter().zip([
        TemporalType::Time(TemporalPrecision::MICROSECONDS),
        TemporalType::TimestampWithTimeZone(TemporalPrecision::NANOSECONDS),
    ]) {
        assert!(field.value_type().nullable());
        assert_eq!(field.value_type().temporal(), Some(ty));
    }

    let snapshots =
        metadata_table_output_columns(MetadataTableType::Snapshots, &metadata, &mut creation)
            .unwrap();
    let execution = metadata_output_execution_schema(&snapshots, &mut creation).unwrap();
    let timestamp = execution.binding(0).unwrap();
    assert_eq!(
        timestamp.value_type().temporal(),
        Some(TemporalType::TimestampWithTimeZone(
            TemporalPrecision::MICROSECONDS,
        ))
    );
    assert!(!timestamp.nullable());
}
