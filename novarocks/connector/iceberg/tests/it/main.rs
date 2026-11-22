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

use arrow::datatypes::DataType;
use novarocks_connector_iceberg::access_binding::IcebergReadBinding;
use novarocks_connector_iceberg::catalog_config::parse_catalog_configuration;
use novarocks_connector_iceberg::catalog_control::IcebergCatalogControlState;
use novarocks_connector_iceberg::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use novarocks_connector_iceberg::metadata_context::IcebergMetadataContext;
use novarocks_connector_iceberg::resources::IcebergMetadataResources;
use novarocks_connector_iceberg::typed_boundary::IcebergTypedBoundary;
use novarocks_connector_iceberg::typed_read::{
    HiveTransactionHandle, IcebergColumnHandle, IcebergRuntimeRelation, IcebergTableHandle,
    IcebergTableHandleParams,
};
use novarocks_fs::{
    FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner, TokioRefreshExecutor,
};
use novarocks_spi::connector::read_stack::{
    ConnectorReadMetadata, ConnectorReadSchemaCreation, ConnectorSession, SchemaTableName,
    TupleDomain,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorProviderId, ProviderBindingEpoch,
};
use novarocks_type_contract::{ValueKind, ValueLogicalType, ValueTypeStorageLifetime};

#[test]
fn fixed_and_uuid_keep_distinct_public_read_types_and_source_identity() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let warehouse = tempfile::tempdir().unwrap();
    let configuration = parse_catalog_configuration(
        "ice",
        &[(
            "iceberg.catalog.warehouse".into(),
            warehouse.path().display().to_string(),
        )],
    )
    .unwrap();
    let io = runtime.handle().clone();
    let binding = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(io.clone())),
        TokioRefreshExecutor::shared(io.clone()),
        Arc::new(TokioFileIoSpawner::new(io.clone())),
    );
    let context = IcebergMetadataContext::try_new(
        IcebergCatalogControlState::new(configuration),
        IcebergMetadataResources::new(binding, io),
    )
    .unwrap();
    let instance_id = ConnectorInstanceId::parse("ice").unwrap();
    let adapter = Arc::new(IcebergTypedBoundary::new(
        ConnectorInstanceDescriptor {
            instance_id: instance_id.clone(),
            provider_id: ConnectorProviderId::parse(novarocks_connector_iceberg::PROVIDER_ID)
                .unwrap(),
        },
        ProviderBindingEpoch::new(),
        CatalogHandle::new(instance_id, CatalogVersion::from_bytes([1; 32])),
        HiveTransactionHandle::new(true, [2; 16]),
        Arc::new(context),
    ))
    .read_runtime_adapter();

    let declaration = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "raw",
                Type::Primitive(PrimitiveType::Fixed(16)),
            )),
            Arc::new(NestedField::optional(
                2,
                "uuid",
                Type::Primitive(PrimitiveType::Uuid),
            )),
        ])
        .build()
        .unwrap();
    let table = IcebergTableHandle::try_new(IcebergTableHandleParams {
        schema_table_name: SchemaTableName::try_new("db", "t").unwrap(),
        snapshot_id: None,
        read_domain: None,
        table_schema_json: serde_json::to_string(&declaration).unwrap(),
        spec_id: None,
        partition_spec_jsons: Default::default(),
        format_version: 2,
        unenforced_predicate: TupleDomain::all(),
        enforced_predicate: TupleDomain::all(),
        limit: None,
        projected_columns: Default::default(),
        name_mapping_json: None,
        table_location: "s3://warehouse/db/t".into(),
        storage_properties: Default::default(),
        pinned_data_files: None,
    })
    .unwrap();
    let table = adapter.wrap_table(IcebergRuntimeRelation::Table(table));
    let columns = [1, 2].map(|id| {
        adapter.wrap_column(IcebergColumnHandle::base_column_of(&declaration, id).unwrap())
    });
    let session =
        ConnectorSession::try_new("q", "u", "UTC", "en_US", std::time::SystemTime::UNIX_EPOCH)
            .unwrap();
    let mut request = |_| Ok(());
    let mut work = |_| Ok(());
    let mut creation =
        ConnectorReadSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut work);
    let published = adapter
        .read_public_schema(&session, &table, &columns, &mut creation)
        .unwrap();
    let execution = published.execution_schema().unwrap();
    let raw = execution.binding(0).unwrap();
    assert_eq!(raw.value_type().kind(), &ValueKind::Binary);
    assert_eq!(raw.data_type(), &DataType::Binary);
    assert!(!raw.nullable());
    let uuid = execution.binding(1).unwrap();
    assert_eq!(uuid.logical_type(), ValueLogicalType::Uuid);
    assert_eq!(uuid.data_type(), &DataType::FixedSizeBinary(16));
    assert!(uuid.nullable());

    let source = published.source_schema_facts().unwrap();
    assert_eq!(source.field_fact(&[0]).unwrap().field_id(), Some(1));
    assert_eq!(source.field_fact(&[1]).unwrap().field_id(), Some(2));
    assert_eq!(
        source.shape().schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
}
