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

use std::collections::BTreeSet;

use super::*;

fn dv(path: &str) -> IcebergDeleteFileInfo {
    IcebergDeleteFileInfo {
        record_count: Some(1),
        partition_data_json: Some(r#"{"version":1,"values":[]}"#.to_string()),
        path: path.to_string(),
        file_format: IcebergDeleteFileFormat::Puffin,
        file_content: IcebergDeleteFileContent::Position,
        length: Some(1024),
        content_offset: Some(4),
        content_size_in_bytes: Some(32),
        sequence_number: Some(7),
        partition_spec_id: Some(0),
        partition_key: None,
        referenced_data_file: Some("s3://bucket/data-1.parquet".to_string()),
        equality_column_names: Vec::new(),
        equality_field_ids: Vec::new(),
    }
}

fn live(paths: &[&str]) -> BTreeSet<EntryIdentity> {
    paths
        .iter()
        .map(|path| delete_entry_identity(&dv(path)).unwrap())
        .collect()
}
#[test]
fn optimize_planning_rejects_a_replaced_captured_object_before_dispatch() {
    let uuid = uuid::Uuid::from_u128(42);
    let expected =
        novarocks_spi::connector::ConnectorTableObjectId::try_new(Bytes::from(uuid.to_string()))
            .unwrap();
    async_plan::validate_expected_rewrite_object(Some(&expected), uuid).unwrap();
    let error =
        async_plan::validate_expected_rewrite_object(Some(&expected), uuid::Uuid::from_u128(43))
            .unwrap_err();
    assert_eq!(
        error.table_object_binding_failure(),
        Some(novarocks_spi::connector::ConnectorTableObjectBindingFailure::Replaced)
    );
    assert!(error.to_string().contains("TARGET_REPLACED"));
    assert_eq!(
        error.public_failure_detail(),
        Some("TARGET_REPLACED: Iceberg optimize target changed before rewrite planning")
    );
}

fn optimize_file(path: &str, size: i64, spec: i32, partition: &str) -> IcebergDataFileInfo {
    let mut file = IcebergDataFileInfo::for_test(path, size, 1);
    file.partition_spec_id = Some(spec);
    file.partition_key = Some(partition.to_owned());
    file
}

#[test]
fn optimize_selects_multiple_small_files_and_skips_singleton_partitions() {
    let groups = plan_optimize_groups(
        vec![
            optimize_file("a1", 10, 0, "a"),
            optimize_file("a2", 10, 0, "a"),
            optimize_file("b1", 10, 0, "b"),
            optimize_file("large", 101, 0, "a"),
        ],
        0,
        100,
    )
    .unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(
        groups[0]
            .data_files
            .iter()
            .map(|f| f.path.as_str())
            .collect::<Vec<_>>(),
        ["a1", "a2"]
    );
    assert!(groups[0].owned_data_delete_files.is_empty());
}

#[test]
fn optimize_includes_delete_bearing_files_regardless_of_size_and_only_attached_deletes() {
    let mut file = optimize_file("s3://bucket/data-1.parquet", 1000, 0, "a");
    file.delete_files.push(dv("s3://bucket/attached.puffin"));
    let groups =
        plan_optimize_groups(vec![file, optimize_file("singleton", 1, 0, "b")], 0, 100).unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].data_files.len(), 1);
    assert_eq!(
        groups[0].owned_data_delete_files,
        live(&["s3://bucket/attached.puffin"])
            .into_iter()
            .collect::<Vec<_>>()
    );
}

#[test]
fn optimize_old_spec_rewrites_all_candidates_but_keeps_size_exclusions() {
    let groups = plan_optimize_groups(
        vec![
            optimize_file("old", 10, 0, "a"),
            optimize_file("current", 10, 1, "b"),
            optimize_file("large", 101, 0, "c"),
        ],
        1,
        100,
    )
    .unwrap();
    let files = groups
        .iter()
        .flat_map(|g| &g.data_files)
        .map(|f| f.path.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(files, BTreeSet::from(["old", "current"]));
}

#[test]
fn optimize_zero_selection_is_a_noop() {
    assert!(plan_optimize_groups(Vec::new(), 0, 100).unwrap().is_empty());
    assert!(
        plan_optimize_groups(vec![optimize_file("only", 100, 0, "a")], 0, 100)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn data_groups_are_partition_owned_and_stably_sorted() {
    let mut a = IcebergDataFileInfo::for_test("s3://bucket/a.parquet", 10, 1);
    a.partition_spec_id = Some(2);
    a.partition_key = Some("{\"day\":1}".to_string());
    let mut b = IcebergDataFileInfo::for_test("s3://bucket/b.parquet", 20, 2);
    b.partition_spec_id = Some(2);
    b.partition_key = Some("{\"day\":1}".to_string());
    let mut c = IcebergDataFileInfo::for_test("s3://bucket/c.parquet", 30, 3);
    c.partition_spec_id = Some(2);
    c.partition_key = Some("{\"day\":2}".to_string());
    let groups = group_data_files(vec![c, b, a]).expect("groups");
    assert_eq!(groups.len(), 2);
    assert_eq!(
        groups
            .iter()
            .map(|group| group.data_files.len())
            .sum::<usize>(),
        3
    );
    assert!(groups.iter().all(|group| {
        group
            .data_files
            .windows(2)
            .all(|pair| pair[0].path < pair[1].path)
    }));
}

#[test]
fn shared_puffin_vectors_keep_distinct_identity_and_digest() {
    let first = dv("s3://bucket/shared.puffin");
    let mut second = first.clone();
    second.content_offset = Some(first.content_offset.unwrap() + 128);
    let mut file = IcebergDataFileInfo::for_test("s3://bucket/a.parquet", 10, 1);
    file.delete_files = vec![second.clone(), first];
    let groups = group_data_files(vec![file.clone()]).unwrap();
    assert_eq!(groups[0].owned_data_delete_files.len(), 2);
    let encoded = serde_json::to_vec(&groups[0]).unwrap();
    let decoded: IcebergFrozenRewriteGroupV1 = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(
        decoded.owned_data_delete_files,
        groups[0].owned_data_delete_files
    );
    let mut old_shape = serde_json::to_value(&groups[0]).unwrap();
    old_shape["selected_position_delete_files"] = serde_json::json!([]);
    assert!(serde_json::from_value::<IcebergFrozenRewriteGroupV1>(old_shape).is_err());
    file.delete_files.reverse();
    let reverse = group_data_files(vec![file.clone()]).unwrap();
    assert_eq!(groups[0].group_digest_hex, reverse[0].group_digest_hex);
    file.delete_files[0].content_size_in_bytes = Some(second.content_size_in_bytes.unwrap() + 1);
    let changed = group_data_files(vec![file]).unwrap();
    assert_ne!(groups[0].group_digest_hex, changed[0].group_digest_hex);
}

#[test]
fn shared_delete_file_has_one_canonical_owner() {
    let delete = IcebergDeleteFileInfo {
        record_count: Some(1),
        partition_data_json: Some(r#"{"version":1,"values":[]}"#.to_string()),
        path: "s3://bucket/shared-delete.parquet".to_string(),
        file_format: IcebergDeleteFileFormat::Parquet,
        file_content: IcebergDeleteFileContent::Equality,
        length: Some(8),
        content_offset: None,
        content_size_in_bytes: None,
        sequence_number: Some(3),
        partition_spec_id: Some(0),
        partition_key: None,
        referenced_data_file: None,
        equality_column_names: vec!["id".to_string()],
        equality_field_ids: vec![1],
    };
    let mut a = IcebergDataFileInfo::for_test("s3://bucket/a.parquet", 10, 1);
    a.partition_key = Some("a".to_string());
    a.delete_files.push(delete.clone());
    let mut b = IcebergDataFileInfo::for_test("s3://bucket/b.parquet", 10, 1);
    b.partition_key = Some("b".to_string());
    b.delete_files.push(delete);
    let groups = group_data_files(vec![b, a]).expect("groups");
    assert_eq!(
        groups
            .iter()
            .flat_map(|group| &group.owned_data_delete_files)
            .filter(|path| path.path() == "s3://bucket/shared-delete.parquet")
            .count(),
        1
    );
}

#[test]
fn canonical_json_and_artifact_digest_are_deterministic() {
    let mut first = std::collections::HashMap::new();
    first.insert("z".to_string(), vec![1_u8]);
    first.insert("a".to_string(), vec![2_u8]);
    let mut second = std::collections::HashMap::new();
    second.insert("a".to_string(), vec![2_u8]);
    second.insert("z".to_string(), vec![1_u8]);
    assert_eq!(
        canonical_json(&first).unwrap(),
        canonical_json(&second).unwrap()
    );
    assert_eq!(artifact_digest(b"same"), artifact_digest(b"same"));
    assert_ne!(artifact_digest(b"same"), artifact_digest(b"different"));
}
fn admission_runtime(
    catalog_type: &str,
) -> (
    tokio::runtime::Runtime,
    tempfile::TempDir,
    Arc<IcebergMetadataContext>,
    std::net::TcpListener,
) {
    let executor = tokio::runtime::Runtime::new().expect("runtime");
    let warehouse = tempfile::tempdir().expect("warehouse");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("HMS probe listener");
    listener.set_nonblocking(true).expect("nonblocking probe");
    let mut properties = vec![
        ("iceberg.catalog.type".to_string(), catalog_type.to_string()),
        (
            "iceberg.catalog.warehouse".to_string(),
            warehouse.path().display().to_string(),
        ),
    ];
    if catalog_type == "rest" {
        properties.push((
            "uri".to_string(),
            format!(
                "http://{}",
                listener.local_addr().expect("listener address")
            ),
        ));
    }
    if catalog_type == "hive" {
        properties.push((
            "hive.metastore.uris".to_string(),
            format!(
                "thrift://{}",
                listener.local_addr().expect("listener address")
            ),
        ));
    }
    let configuration = crate::catalog_config::parse_catalog_configuration("ice", &properties)
        .expect("configuration");
    let binding = crate::access_binding::IcebergReadBinding::new(
        None,
        novarocks_fs::FsAccessResolver::new(),
        Arc::new(novarocks_fs::TokioFileIoRuntime::new(
            executor.handle().clone(),
        )),
        novarocks_fs::TokioRefreshExecutor::shared(executor.handle().clone()),
        Arc::new(novarocks_fs::TokioFileIoSpawner::new(
            executor.handle().clone(),
        )),
    );
    let runtime = Arc::new(
        IcebergMetadataContext::try_new(
            crate::catalog_control::IcebergCatalogControlState::new(configuration),
            crate::resources::IcebergMetadataResources::new(binding, executor.handle().clone()),
        )
        .expect("control runtime"),
    );
    (executor, warehouse, runtime, listener)
}

#[test]
fn same_generation_rewrite_and_write_begin_declare_the_same_original_rest_route() {
    use novarocks_spi::connector::write_stack::session::ConnectorWriteControl;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, ConnectorProviderId,
        NetworkServiceOrigin, SharedDependency,
    };

    let (_executor, warehouse, runtime, listener) = admission_runtime("rest");
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
        instance_id: ConnectorInstanceId::parse("ice").unwrap(),
    };
    let incarnation = ProviderBindingEpoch::from_bytes([9; 16]);
    let provider = Arc::new(IcebergMetadata::new(
        descriptor.clone(),
        incarnation,
        runtime.clone(),
    ));
    let rewrite = IcebergDistributedRewriteControl::new(
        descriptor.clone(),
        incarnation,
        runtime.clone(),
        provider,
    )
    .unwrap();
    let write = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
        descriptor.clone(),
        incarnation,
        CatalogHandle::new(descriptor.instance_id, CatalogVersion::from_bytes([3; 32])),
        runtime.clone(),
    );
    assert!(Arc::ptr_eq(write.runtime(), &runtime));
    assert!(Arc::ptr_eq(&rewrite.runtime, &runtime));
    assert_eq!(write.binding_key(), &rewrite.key);
    let planning = rewrite.planning_initial_dependencies().unwrap();
    let beginning = write.rewrite_begin_initial_dependencies().unwrap();
    let expected = SharedDependency::Network(
        NetworkServiceOrigin::from_selected_url(
            &url::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap(),
        )
        .unwrap(),
    );
    assert_eq!(planning, vec![expected.clone()]);
    assert_eq!(beginning, vec![expected]);
    assert_admission_left_no_io(&warehouse, &listener);
}

fn admission_context(catalog_type: &str) -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        1024,
        4096,
    )
    .expect("context")
    .with_initiation(if catalog_type == "hive" {
        novarocks_spi::connector::ConnectorRequestInitiation::Statement
    } else {
        novarocks_spi::connector::ConnectorRequestInitiation::Background
    })
}

fn admission_table(
    instance: novarocks_spi::connector::ConnectorInstanceId,
) -> ConnectorTableHandle {
    ConnectorTableHandle::try_new(instance, Bytes::from_static(br#"{"namespace":"db","table":"absent","metadata_location":null,"table_info":null,"metadata_columns":[],"metadata_table_type":null,"prepared_files":[],"explicit_files":null}"#)).expect("table handle")
}

fn assert_admission_left_no_io(warehouse: &tempfile::TempDir, listener: &std::net::TcpListener) {
    assert_eq!(
        std::fs::read_dir(warehouse.path())
            .expect("warehouse inventory")
            .count(),
        0
    );
    let error = listener
        .accept()
        .expect_err("admission must not connect to HMS");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
}

#[test]
fn rewrite_preparation_retains_complete_input_declarations() {
    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, SortOrder, TableMetadataBuilder,
        Transform, Type,
    };
    let declaration = crate::iceberg::spec::Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .unwrap();
    let spec = PartitionSpec::builder(declaration.clone())
        .with_spec_id(0)
        .add_partition_field("id", "id", Transform::Identity)
        .unwrap()
        .build()
        .unwrap();
    let metadata = TableMetadataBuilder::new(
        declaration,
        spec.into_unbound(),
        SortOrder::unsorted_order(),
        "file:///warehouse/db/t".to_string(),
        FormatVersion::V3,
        std::collections::HashMap::from([(
            crate::schema_mapping::nominal_declaration::PROPERTY.to_string(),
            r#"{"version":1,"fields":[{"id":1,"domain":"tinyint"}]}"#.to_string(),
        )]),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let owner = ConnectorProviderBindingKey {
        instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("ice").unwrap(),
        incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
    };
    for row_lineage in [false, true] {
        let operation = ConnectorDistributedRewriteOperation::OptimizeDataFiles {
            table: admission_table(owner.instance_id.clone()),
            file_size_threshold_bytes: 1024,
        };
        let request = ConnectorDistributedRewritePlanningRequest::try_new(
            novarocks_spi::connector::ConnectorWriteOperationId::new(),
            owner.clone(),
            operation,
            admission_context("hive"),
        )
        .unwrap();
        let mut layout = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut layout,
            &mut work,
        );
        let physical = Arc::new(
            crate::iceberg::arrow::schema_to_arrow_schema(metadata.current_schema()).unwrap(),
        );
        let source = rewrite_input_schema(physical, row_lineage);
        if row_lineage {
            for name in [ICEBERG_ROW_ID_COL, ICEBERG_LAST_UPDATED_SEQ_COL] {
                assert!(source.field_with_name(name).unwrap().is_nullable());
            }
        }
        let execution = rewrite_execution_schema(&metadata, row_lineage, &mut creation).unwrap();
        let preparation = rewrite_preparation(
            &owner,
            &request,
            ConnectorWriteIntent::Overwrite,
            &source,
            &execution,
            row_lineage,
            &metadata,
        )
        .unwrap();
        for (ordinal, source_field) in source.fields().iter().enumerate() {
            let actual = preparation
                .input()
                .fields()
                .into_iter()
                .find(|field| field.field().name() == source_field.name())
                .unwrap();
            let expected = execution.binding(ordinal).unwrap();
            assert_eq!(actual.execution_type(), Some(expected));
            assert_eq!(actual.field().data_type(), expected.data_type());
            assert_eq!(actual.field().is_nullable(), expected.nullable());
            assert_eq!(source_field.is_nullable(), expected.nullable());
            assert_eq!(actual.field().metadata(), source_field.metadata());
        }
        assert_eq!(
            preparation.input().fields()[0].field().data_type(),
            &DataType::Int8
        );
    }
}

#[test]
fn data_rewrite_denies_variant_and_uuid_without_writing_plan_artifacts() {
    use crate::iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, Type, VariantType,
    };
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorProviderId, ConnectorWriteOperationId,
    };
    fn files(root: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
        fn walk(
            path: &std::path::Path,
            output: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
        ) {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(&path, output);
                } else {
                    output.insert(path.clone(), std::fs::read(&path).unwrap());
                }
            }
        }
        let mut output = std::collections::BTreeMap::new();
        walk(root, &mut output);
        output
    }
    for data_type in [
        Type::Variant(VariantType),
        Type::Primitive(PrimitiveType::Uuid),
    ] {
        let (_executor, warehouse, runtime, _listener) = admission_runtime("hadoop");
        // Model a table authored by another engine. Product CREATE/ALTER
        // admission remains closed to new Variant and uuid columns.
        // Match the configured catalog root, which canonicalizes local
        // warehouse aliases (for example /var to /private/var on macOS).
        let table_location = format!(
            "{}/db/t",
            runtime
                .control_state()
                .configuration()
                .warehouse_uri
                .trim_end_matches('/')
        );
        let table_root = runtime
            .control_state()
            .configuration()
            .warehouse_path
            .join("db/t");
        let metadata_root = table_root.join("metadata");
        std::fs::create_dir_all(&metadata_root).unwrap();
        let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(vec![NestedField::optional(1, "payload", data_type).into()])
                .build()
                .unwrap(),
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            table_location,
            FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        std::fs::write(
            metadata_root.join("v1.metadata.json"),
            serde_json::to_vec(&metadata).unwrap(),
        )
        .unwrap();
        std::fs::write(metadata_root.join("version-hint.text"), b"1").unwrap();
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("ice").unwrap(),
        };
        let incarnation = ProviderBindingEpoch::from_bytes([8; 16]);
        let provider = Arc::new(IcebergMetadata::new(
            descriptor.clone(),
            incarnation,
            runtime.clone(),
        ));
        let adapter =
            IcebergDistributedRewriteControl::new(descriptor, incarnation, runtime, provider)
                .unwrap();
        let table = ConnectorTableHandle::try_new(adapter.key.instance_id.clone(), Bytes::from_static(br#"{"namespace":"db","table":"t","metadata_location":null,"table_info":null,"metadata_columns":[],"metadata_table_type":null,"prepared_files":[],"explicit_files":null}"#)).unwrap();
        let mut context = admission_context("hadoop");
        context = context
            .with_initiation(novarocks_spi::connector::ConnectorRequestInitiation::Statement);
        let observed = adapter
            .runtime
            .observe_table_for_request("db", "t", &context)
            .expect("externally authored fixed-root table must load before rewrite admission");
        assert_eq!(observed.table.metadata().uuid(), metadata.uuid());
        let request = ConnectorDistributedRewritePlanningRequest::try_new(
            ConnectorWriteOperationId::new(),
            adapter.key.clone(),
            ConnectorDistributedRewriteOperation::OptimizeDataFiles {
                table,
                file_size_threshold_bytes: 1024,
            },
            context,
        )
        .unwrap();
        let before = files(warehouse.path());
        let mut layout = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut layout,
            &mut work,
        );
        let error = adapter.plan_rewrite(request, &mut creation).unwrap_err();
        let novarocks_connector_contract::PureProviderCompileError::Provider(error) = error else {
            panic!("provider denial")
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert!(error.message().contains("column payload"), "{error}");
        assert_eq!(
            files(warehouse.path()),
            before,
            "refused data rewrite writes nothing"
        );
    }
}

#[test]
fn planning_refuses_hms_and_background_hadoop_before_rewrite_artifacts() {
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorProviderId, ConnectorWriteOperationId,
    };
    for catalog_type in ["hive", "hadoop"] {
        let (_executor, warehouse, runtime, listener) = admission_runtime(catalog_type);
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let incarnation = ProviderBindingEpoch::from_bytes([8; 16]);
        let provider = Arc::new(IcebergMetadata::new(
            descriptor.clone(),
            incarnation,
            runtime.clone(),
        ));
        let adapter =
            IcebergDistributedRewriteControl::new(descriptor, incarnation, runtime, provider)
                .expect("adapter");
        {
            let operation = ConnectorDistributedRewriteOperation::OptimizeDataFiles {
                table: admission_table(adapter.key.instance_id.clone()),
                file_size_threshold_bytes: 1024,
            };
            let request = ConnectorDistributedRewritePlanningRequest::try_new(
                ConnectorWriteOperationId::new(),
                adapter.key.clone(),
                operation,
                admission_context(catalog_type),
            )
            .expect("request");
            let mut layout = |_| Ok(());
            let mut work = |_| Ok(());
            let mut creation =
                novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
                    novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                    &mut layout,
                    &mut work,
                );
            let error = adapter
                .plan_rewrite(request, &mut creation)
                .expect_err("catalog admission must refuse planning");
            let novarocks_connector_contract::PureProviderCompileError::Provider(error) = error
            else {
                panic!("catalog admission must retain its provider cause");
            };
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
            assert!(error.to_string().contains(if catalog_type == "hive" {
                "read-only compatibility entry"
            } else {
                "background"
            }));
            // Rejected requests retain no attempt content in the adapter.
            assert_admission_left_no_io(&warehouse, &listener);
        }
    }
}
#[test]
fn frozen_rewrite_artifacts_accept_large_groups_and_many_cohorts() {
    let mut artifact = IcebergFrozenRewriteArtifactV1 {
        version: ARTIFACT_VERSION,
        operation_kind: novarocks_spi::connector::REWRITE_DATA_FILES_KIND.to_string(),
        namespace: "db".to_string(),
        table: "t".to_string(),
        table_uuid: "uuid".to_string(),
        target_ref: "main".to_string(),
        base_snapshot_id: Some(1),
        base_sequence_number: 1,
        schema_id: 0,
        default_spec_id: 0,
        groups: vec![IcebergFrozenRewriteGroupV1::default()],
    };
    artifact.groups[0]
        .data_files
        .push(IcebergDataFileInfo::for_test(
            &"x".repeat(1024 * 1024 + 1),
            1,
            1,
        ));
    assert!(artifact.canonical_bytes().unwrap().len() > 1024 * 1024);
    artifact.groups = vec![IcebergFrozenRewriteGroupV1::default(); 4097];
    assert!(artifact.canonical_bytes().is_ok());
}
