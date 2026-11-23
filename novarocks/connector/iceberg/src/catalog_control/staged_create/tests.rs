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

mod location;
use crate::access_binding::IcebergReadBinding;
use crate::catalog_control::IcebergCatalogControlState;
use crate::resources::IcebergMetadataResources;
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorInstanceId, ConnectorMutationOperationId,
    ConnectorProviderId, ConnectorTableIdentity, ConnectorTableObjectId, CreatePolicy,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};

fn context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + std::time::Duration::from_secs(60),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .expect("valid request context")
}

/// A REST generation that never contacts its endpoint.
///
/// `RestCatalogBuilder::load` only assembles config — the HTTP context is a
/// `OnceCell` filled on first use — so a REST control generation is
/// constructible offline without a configured warehouse.
fn rest_runtime() -> IcebergMetadataContext {
    let executor = tokio::runtime::Runtime::new().expect("runtime");
    let handle = executor.handle().clone();
    let configuration = crate::catalog_config::parse_catalog_configuration(
        "ice",
        &[
            ("iceberg.catalog.type".to_string(), "rest".to_string()),
            ("uri".to_string(), "http://127.0.0.1:1".to_string()),
        ],
    )
    .expect("configuration");
    let binding = IcebergReadBinding::new(
        None,
        novarocks_fs::FsAccessResolver::new(),
        Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(handle.clone()),
        Arc::new(novarocks_fs::TokioFileIoSpawner::new(handle.clone())),
    );
    IcebergMetadataContext::try_new(
        IcebergCatalogControlState::new(configuration),
        IcebergMetadataResources::new(binding, handle),
    )
    .expect("control runtime")
}

fn adapter_for(runtime: Arc<IcebergMetadataContext>) -> IcebergStagedCreateAdapter {
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
        instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
    };
    let provider = Arc::new(IcebergMetadata::new(
        descriptor.clone(),
        ProviderBindingEpoch::new(),
        Arc::clone(&runtime),
    ));
    IcebergStagedCreateAdapter::try_new(provider).expect("staged-create adapter")
}

fn unsupported_staged_adapter() -> (
    IcebergStagedCreateAdapter,
    ConnectorStagedCreatePrepareRequest,
) {
    let adapter = adapter_for(Arc::new(hadoop_runtime()));
    let request = stage_http_request(&adapter, context());
    (adapter, request)
}

pub(super) fn stage_publication_for_test(
    operation_id: ConnectorStagedCreateOperationId,
) -> StagePublication {
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
        instance_id: ConnectorInstanceId::parse("ice").unwrap(),
    };
    let incarnation = ProviderBindingEpoch::new();
    let owner = ConnectorProviderBindingKey {
        instance_id: descriptor.instance_id.clone(),
        incarnation,
    };
    StagePublication {
        journal: crate::commit::attempt::observation::PublicationJournal::new(
            crate::commit::model::OperationToken::from_mutation(operation_id),
        ),
        receipt: ConnectorStagedCreateReceipt::try_new(
            owner,
            operation_id,
            ConnectorStagedCreateReceiptPhase::Prepared,
            ExternalMutationEffect::Applied,
            Bytes::copy_from_slice(&operation_id.to_bytes()),
        )
        .unwrap(),
        evidence: ExternalMutationEvidence::try_new(
            1,
            descriptor,
            incarnation,
            operation_id,
            "staged-create-prepare",
            Bytes::new(),
        )
        .unwrap(),
        policy: Default::default(),
    }
}

/// A prepared aggregate with no writer bound, for state-machine tests.
fn prepared_operation(runtime: &IcebergMetadataContext) -> PreparedOperation {
    prepared_operation_at(runtime, "file:///tmp/novarocks-staged-create/table")
}

fn prepared_operation_at(runtime: &IcebergMetadataContext, location: &str) -> PreparedOperation {
    let schema = crate::iceberg::spec::Schema::builder()
        .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::required(
            1,
            "id",
            crate::iceberg::spec::Type::Primitive(crate::iceberg::spec::PrimitiveType::Long),
        ))])
        .build()
        .expect("schema");
    let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
        schema,
        crate::iceberg::spec::PartitionSpec::unpartition_spec(),
        crate::iceberg::spec::SortOrder::unsorted_order(),
        location.to_string(),
        crate::iceberg::spec::FormatVersion::V2,
        HashMap::new(),
    )
    .expect("metadata builder")
    .build()
    .expect("metadata")
    .metadata;
    let table = crate::iceberg::table::Table::builder()
        .runtime(
            runtime
                .resources()
                .catalog_runtime()
                .iceberg_runtime()
                .clone(),
        )
        .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).expect("identifier"))
        .file_io(crate::fs_io::build_file_io_for_location(
            location,
            runtime.resources().planning_binding().clone(),
        ))
        .metadata(metadata)
        .build()
        .expect("table");
    let publication_id = novarocks_spi::connector::LakePublicationId::new_v7();
    PreparedOperation {
        stage_publication: stage_publication_for_test(ConnectorMutationOperationId::from_bytes(
            publication_id.to_bytes(),
        )),
        final_publication: None,
        publication_id,
        handle_digest: [7u8; 32],
        staged: RestStagedTableCreate {
            table,
            initialization_updates: Vec::new(),
            publication_updates: Vec::new(),
            location_refusal: None,
            final_metadata_path: None,
        },
        planning: None,
        write: None,
        document_properties: None,
        publication: None,
        publication_closed: false,
    }
}

#[test]
fn document_managed_create_freezes_actual_iceberg_field_ids_in_request_order() {
    let runtime = hadoop_runtime();
    let prepared = prepared_operation(&runtime);
    let fields = prepared_document_field_bindings(prepared.staged.table.metadata()).unwrap();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].request_ordinal(), 0);
    assert_eq!(fields[0].provider_field_id().as_ref(), 1_i32.to_be_bytes());
}

fn publication_table_with_properties(
    prepared: &PreparedOperation,
    properties: HashMap<String, String>,
    runtime: crate::iceberg::Runtime,
) -> crate::iceberg::table::Table {
    let metadata = prepared
        .staged
        .table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(properties)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    crate::iceberg::table::Table::builder()
        .runtime(runtime)
        .identifier(prepared.staged.table.identifier().clone())
        .file_io(prepared.staged.table.file_io().clone())
        .metadata(metadata)
        .build()
        .unwrap()
}

#[test]
fn applied_publication_requires_every_exact_keyed_property_and_management_slot() {
    let runtime = hadoop_runtime();
    let mut prepared = prepared_operation(&runtime);
    let document_properties = HashMap::from([
        (
            crate::document_storage::keyed::keys::version_key(
                novarocks_spi::connector::DocumentIdentity::from_bytes([1; 32]),
            ),
            "exact-version-envelope".to_string(),
        ),
        (
            "novarocks.doc.v2.slot.novarocks.mv~configuration".to_string(),
            "mv".to_string(),
        ),
        (
            "novarocks.doc.v2.root.novarocks.mv".to_string(),
            "deployment".to_string(),
        ),
        (
            "novarocks.doc.v2.slot.novarocks.mv~management".to_string(),
            "incarnation".to_string(),
        ),
    ]);
    prepared.document_properties = Some(document_properties.clone());
    let mut published_properties = document_properties;
    published_properties.insert(
        CTAS_OPERATION_MARKER.to_string(),
        publication_marker(prepared.publication_id),
    );
    let exact = publication_table_with_properties(
        &prepared,
        published_properties.clone(),
        runtime
            .resources()
            .catalog_runtime()
            .iceberg_runtime()
            .clone(),
    );
    assert!(publication_matches(
        &exact,
        prepared.operation_id(),
        &prepared,
        None
    ));

    published_properties.remove("novarocks.doc.v2.slot.novarocks.mv~management");
    let partial = publication_table_with_properties(
        &prepared,
        published_properties.clone(),
        runtime
            .resources()
            .catalog_runtime()
            .iceberg_runtime()
            .clone(),
    );
    assert!(!publication_matches(
        &partial,
        prepared.operation_id(),
        &prepared,
        None
    ));

    published_properties.insert(
        "novarocks.doc.v2.slot.novarocks.mv~management".to_string(),
        "other".to_string(),
    );
    let mismatched = publication_table_with_properties(
        &prepared,
        published_properties,
        runtime
            .resources()
            .catalog_runtime()
            .iceberg_runtime()
            .clone(),
    );
    assert!(!publication_matches(
        &mismatched,
        prepared.operation_id(),
        &prepared,
        None
    ));
}

#[test]
fn document_managed_target_cannot_publish_without_application_documents() {
    let runtime = hadoop_runtime();
    let prepared = prepared_operation(&runtime);
    let owner = ConnectorProviderBindingKey {
        instance_id: ConnectorInstanceId::parse("ice").unwrap(),
        incarnation: ProviderBindingEpoch::new(),
    };
    let operation_id = ConnectorMutationOperationId::new();
    let target = ConnectorPreparedCreateDocumentTarget::try_new(
        owner.clone(),
        CatalogHandle::new(
            owner.instance_id.clone(),
            CatalogVersion::from_bytes([3; 32]),
        ),
        operation_id,
        ConnectorTableIdentity {
            instance_id: owner.instance_id.clone(),
            namespace: Arc::from("db"),
            table: Arc::from("t"),
        },
        ConnectorTableObjectId::try_new(Bytes::from(
            prepared.staged.table.metadata().uuid().to_string(),
        ))
        .unwrap(),
        crate::storage_inspector::exact_schema_version(
            prepared.staged.table.metadata().current_schema_id(),
        ),
        crate::storage_inspector::exact_partition_spec_version(
            prepared.staged.table.metadata().default_partition_spec_id(),
        ),
        prepared_document_field_bindings(prepared.staged.table.metadata()).unwrap(),
        crate::storage_inspector::prepared_create_partition_fields(
            prepared.staged.table.metadata(),
            &context(),
        )
        .unwrap(),
        Bytes::from_static(b"target"),
    )
    .unwrap();
    let handle = ConnectorStagedTableHandle::try_new_document_managed(
        owner,
        operation_id,
        Bytes::from_static(b"handle"),
        target,
    )
    .unwrap();
    assert_eq!(
        publication_document_properties(
            &handle,
            &ConnectorStagedCreatePublicationPayload::Ordinary
        )
        .unwrap_err()
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

fn hadoop_runtime() -> IcebergMetadataContext {
    let executor = tokio::runtime::Runtime::new().expect("runtime");
    let handle = executor.handle().clone();
    let warehouse = tempfile::tempdir().expect("warehouse");
    let configuration = crate::catalog_config::parse_catalog_configuration(
        "ice",
        &[(
            "iceberg.catalog.warehouse".to_string(),
            warehouse.path().display().to_string(),
        )],
    )
    .expect("configuration");
    let binding = IcebergReadBinding::new(
        None,
        novarocks_fs::FsAccessResolver::new(),
        Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(handle.clone()),
        Arc::new(novarocks_fs::TokioFileIoSpawner::new(handle.clone())),
    );
    IcebergMetadataContext::try_new(
        IcebergCatalogControlState::new(configuration),
        IcebergMetadataResources::new(binding, handle),
    )
    .expect("control runtime")
}

#[test]
fn schema_nesting_refuses_sync_and_async_ctas_before_any_request() {
    use crate::rest_protocol::test_server::{Script, TestServer};
    use novarocks_spi::connector::{ConnectorDataType, ConnectorStructField};
    let server = TestServer::start(Script::new());
    let (executor, adapter) = stage_http_adapter(&server);
    let mut data_type = ConnectorDataType::Int;
    for level in (0..42).rev() {
        data_type = ConnectorDataType::Struct(vec![ConnectorStructField {
            name: format!("n{level}").into(),
            data_type,
            nullable: true,
        }]);
    }
    let columns = vec![ConnectorColumnDefinition {
        name: "deep".into(),
        data_type,
        nullable: true,
        default: None,
    }];
    let mut request = stage_http_request(&adapter, context());
    request.columns = columns;
    let sync = adapter.prepare(request.clone()).unwrap();
    let asynchronous = executor.block_on(adapter.prepare_async(request)).unwrap();
    for result in [sync, asynchronous] {
        match result {
            ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure } => {
                assert_eq!(failure.kind(), ConnectorMutationFailureKind::Unsupported);
                assert!(
                    failure
                        .message()
                        .contains("Iceberg schema JSON exceeds 127 container levels")
                );
            }
            other => panic!("unsupported schema must not stage a target: {other:?}"),
        }
    }
    assert!(server.requests().is_empty());
}

#[test]
fn variant_columns_and_shredding_properties_refuse_ctas_before_any_request() {
    use crate::rest_protocol::test_server::{Script, TestServer};
    use novarocks_spi::connector::{ConnectorDataType, ConnectorStructField};

    let server = TestServer::start(Script::new());
    let executor = tokio::runtime::Runtime::new().expect("runtime");
    let handle = executor.handle().clone();
    let configuration = crate::catalog_config::parse_catalog_configuration(
        "ice",
        &[
            ("iceberg.catalog.type".to_owned(), "rest".to_owned()),
            ("uri".to_owned(), server.url()),
            ("warehouse".to_owned(), "s3://warehouse".to_owned()),
        ],
    )
    .unwrap();
    let binding = IcebergReadBinding::new(
        None,
        novarocks_fs::FsAccessResolver::new(),
        Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(handle.clone()),
        Arc::new(novarocks_fs::TokioFileIoSpawner::new(handle.clone())),
    );
    let runtime = IcebergMetadataContext::try_new(
        IcebergCatalogControlState::new(configuration),
        IcebergMetadataResources::new(binding, handle),
    )
    .unwrap();
    let nested = ConnectorDataType::Struct(vec![ConnectorStructField {
        name: Arc::from("leaf"),
        data_type: ConnectorDataType::Array(Box::new(ConnectorDataType::Variant)),
        nullable: true,
    }]);
    let adapter = adapter_for(Arc::new(runtime));
    for (data_type, properties, diagnostic) in [
        (ConnectorDataType::Variant, Vec::new(), "column payload"),
        (nested, Vec::new(), "column payload.leaf.element"),
        (
            ConnectorDataType::Int,
            vec![(
                Arc::from("write.parquet.variant-shredding.enabled"),
                Arc::from("true"),
            )],
            "write.parquet.variant-shredding.",
        ),
    ] {
        let mut request = stage_http_request(&adapter, context());
        request.columns = vec![ConnectorColumnDefinition {
            name: Arc::from("payload"),
            data_type,
            nullable: true,
            default: None,
        }];
        if properties.is_empty() {
            let outcome = adapter.prepare(request).unwrap();
            assert!(
                matches!(outcome,
                ConnectorStagedCreatePrepareOutcome::KnownUncommitted { ref failure }
                    if failure.kind() == ConnectorMutationFailureKind::Unsupported
                        && failure.message().contains(diagnostic)),
                "{outcome:?}"
            );
        } else {
            let lease = ordinary_ctas_lease(&adapter);
            let mut typed = ctas_defaults(&lease);
            typed.push((
                "extra_properties".into(),
                novarocks_spi::connector::DdlPropertyValue::MapVarcharVarchar(
                    properties
                        .into_iter()
                        .map(|(key, value): (Arc<str>, Arc<str>)| {
                            (key.to_string(), Some(value.to_string()))
                        })
                        .collect(),
                ),
            ));
            let error = match lease.prepare_ctas_properties(typed.into(), None, &request.context) {
                Ok(_) => panic!("unsupported CTAS property admitted"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
            assert!(error.message().contains(diagnostic), "{error}");
        }
        assert!(
            server.requests().is_empty(),
            "admission must not contact namespace or stage-create endpoints"
        );
    }
}

pub(super) fn stage_http_adapter(
    server: &crate::rest_protocol::test_server::TestServer,
) -> (tokio::runtime::Runtime, IcebergStagedCreateAdapter) {
    let executor = tokio::runtime::Runtime::new().unwrap();
    let handle = executor.handle().clone();
    let configuration = crate::catalog_config::parse_catalog_configuration(
        "ice",
        &[
            ("iceberg.catalog.type".into(), "rest".into()),
            ("uri".into(), server.url()),
        ],
    )
    .unwrap();
    let binding = IcebergReadBinding::new(
        None,
        novarocks_fs::FsAccessResolver::new(),
        Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(handle.clone()),
        Arc::new(novarocks_fs::TokioFileIoSpawner::new(handle.clone())),
    );
    let runtime = Arc::new(
        IcebergMetadataContext::try_new(
            IcebergCatalogControlState::new(configuration),
            IcebergMetadataResources::new(binding, handle),
        )
        .unwrap(),
    );
    (executor, adapter_for(runtime))
}
pub(super) fn stage_http_request(
    adapter: &IcebergStagedCreateAdapter,
    request_context: ConnectorRequestContext,
) -> ConnectorStagedCreatePrepareRequest {
    let operation_id = ConnectorMutationOperationId::new();
    let lease = ordinary_ctas_lease(adapter);
    let storage = lease
        .prepare_ctas_properties(ctas_defaults(&lease).into(), None, &context())
        .unwrap();
    lease
        .prepare_typed_request(
            novarocks_spi::connector::LakePublicationId::try_from_bytes(operation_id.to_bytes())
                .unwrap(),
            operation_id,
            ConnectorTableIdentity {
                instance_id: adapter.descriptor.instance_id.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
            vec![ConnectorColumnDefinition {
                name: Arc::from("id"),
                data_type: novarocks_spi::connector::ConnectorDataType::BigInt,
                nullable: false,
                default: None,
            }],
            storage,
            CreatePolicy::FailIfExists,
            request_context,
        )
        .unwrap()
}

#[test]
fn rest_stage_same_name_preserves_the_typed_failure_for_both_policies() {
    use crate::rest_protocol::test_server::{Reply, Script, TestServer};
    use novarocks_failure_contract::{
        FailureClass, FailureFact, FailureProjection,
        connector_reason::ICEBERG_TABLE_ALREADY_EXISTS,
    };

    let server = TestServer::start(
        Script::new()
            .on("GET", "/v1/config", Reply::config(&[], &[], None))
            .on(
                "GET",
                "/v1/namespaces/db",
                Reply::json(
                    200,
                    &serde_json::json!({"namespace": ["db"], "properties": {}}),
                ),
            )
            .on("POST", "/v1/namespaces/db/tables", Reply::status(409)),
    );
    let (executor, adapter) = stage_http_adapter(&server);
    for (policy, scoped) in [
        (CreatePolicy::FailIfExists, false),
        (CreatePolicy::NoOpIfExists, true),
    ] {
        let mut request = stage_http_request(&adapter, context());
        request.policy = policy;
        let operation = request.operation_id;
        let outcome = if scoped {
            executor
                .block_on(adapter.prepare_async_original(request))
                .unwrap()
        } else {
            adapter.prepare(request).unwrap()
        };
        let ConnectorStagedCreatePrepareOutcome::Conflict { failure } = outcome else {
            panic!("REST stage 409 must reject the create");
        };
        assert_eq!(failure.kind(), ConnectorMutationFailureKind::AlreadyExists);
        assert_eq!(
            failure.failure_fact(),
            FailureFact::new(FailureClass::Data, ICEBERG_TABLE_ALREADY_EXISTS)
        );
        assert_eq!(failure.message(), "Table db.t already exists");
        assert!(!adapter.operations.lock().unwrap().contains_key(&operation));
    }
    assert_eq!(
        server.requests_to("POST", "/v1/namespaces/db/tables").len(),
        2
    );
}

#[test]
fn an_attributed_stage_with_unusable_full_metadata_preserves_applied_and_refuses_reuse() {
    use crate::rest_protocol::test_server::{Reply, Script, TestServer};
    let uuid = uuid::Uuid::new_v4();
    let server = TestServer::start(
        Script::new()
            .on("GET", "/v1/config", Reply::config(&[], &[], None))
            .on(
                "GET",
                "/v1/namespaces/db",
                Reply::json(
                    200,
                    &serde_json::json!({"namespace":["db"],"properties":{}}),
                ),
            )
            .on(
                "POST",
                "/v1/namespaces/db/tables",
                Reply::json(
                    200,
                    &serde_json::json!({
                        "metadata-location": null, "metadata": {"table-uuid": uuid.to_string()}
                    }),
                ),
            ),
    );
    let (_executor, adapter) = stage_http_adapter(&server);
    let request = stage_http_request(&adapter, context());
    let operation_id = request.operation_id;
    let outcome = adapter.prepare(request.clone()).unwrap();
    let ConnectorStagedCreatePrepareOutcome::AppliedButUnusable {
        receipt,
        finalization,
    } = outcome
    else {
        panic!("a materialization failure cannot erase the attributed stage")
    };
    assert_eq!(receipt.operation_id(), operation_id);
    assert_eq!(receipt.effect(), ExternalMutationEffect::Applied);
    assert!(matches!(
        finalization,
        ExternalMutationFinalization::Failed(_)
    ));
    {
        let operations = adapter.operations.lock().unwrap();
        let Some(OperationState::StageAppliedUnusable {
            _stage_publication: stage,
        }) = operations.get(&operation_id)
        else {
            panic!("unusable stage remains terminal")
        };
        let entry = stage.journal.current_entry().unwrap();
        let crate::catalog::error::CatalogOutcome::KnownCommitted {
            receipt: proof,
            finalization,
            ..
        } = stage.journal.commit_outcome(entry)
        else {
            panic!("journal retains Applied")
        };
        assert_eq!(proof.table_uuid.as_deref(), Some(uuid.to_string().as_str()));
        assert!(matches!(
            finalization,
            ExternalMutationFinalization::Failed(_)
        ));
        let descriptor = stage.journal.entry_descriptor(entry).unwrap();
        assert!(descriptor.first_dispatch_ms.is_some());
        let body: serde_json::Value = serde_json::from_slice(&descriptor.request).unwrap();
        assert_eq!(body["stage-create"], true);
    }
    assert!(adapter.prepare(request).is_err());
    assert_eq!(server.count("POST", "/v1/namespaces/db/tables"), 1);
}

#[test]
fn stage_protocol_rejection_and_unknown_keep_distinct_frontiers_without_replay() {
    use crate::rest_protocol::test_server::{Reply, Script, TestServer};
    for status in [409, 403, 500] {
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(
                    "GET",
                    "/v1/namespaces/db",
                    Reply::json(
                        200,
                        &serde_json::json!({"namespace":["db"],"properties":{}}),
                    ),
                )
                .on("POST", "/v1/namespaces/db/tables", Reply::status(status)),
        );
        let (_executor, adapter) = stage_http_adapter(&server);
        let request = stage_http_request(&adapter, context());
        let operation_id = request.operation_id;
        let outcome = adapter.prepare(request.clone()).unwrap();
        match (status, outcome) {
            (409, ConnectorStagedCreatePrepareOutcome::Conflict { failure }) => {
                assert_eq!(failure.kind(), ConnectorMutationFailureKind::AlreadyExists)
            }
            (403, ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure }) => {
                assert_eq!(
                    failure.kind(),
                    ConnectorMutationFailureKind::PermissionDenied
                )
            }
            (500, ConnectorStagedCreatePrepareOutcome::CommitUnknown { .. }) => {
                assert!(adapter.prepare(request).is_err());
                let operations = adapter.operations.lock().unwrap();
                let Some(OperationState::StageUnknown {
                    _stage_publication: stage,
                }) = operations.get(&operation_id)
                else {
                    panic!("unknown stage retains its exact journal")
                };
                let entry = stage.journal.current_entry().unwrap();
                assert!(matches!(
                    stage.journal.commit_outcome(entry),
                    crate::catalog::error::CatalogOutcome::CommitUnknown { .. }
                ));
                assert!(stage.journal.next_frontier().is_err());
            }
            (status, outcome) => panic!("unexpected stage verdict for {status}: {outcome:?}"),
        }
        assert_eq!(server.count("POST", "/v1/namespaces/db/tables"), 1);
    }
}

#[test]
fn stage_preparation_waits_obey_the_original_deadline_and_stop_before_any_post() {
    use crate::rest_protocol::test_server::{Reply, Script, TestServer};
    use novarocks_spi::connector::ConnectorStopOwner;
    for blocked in ["config", "namespace"] {
        for cancel in [false, true] {
            let blocked_path = if blocked == "config" {
                "/v1/config"
            } else {
                "/v1/namespaces/db"
            };
            let script = if blocked == "config" {
                Script::new().on("GET", blocked_path, Reply::NeverAnswer)
            } else {
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on("GET", blocked_path, Reply::NeverAnswer)
            };
            let server = Arc::new(TestServer::start(script));
            let (_executor, adapter) = stage_http_adapter(&server);
            let stop = ConnectorStopOwner::new();
            let context = ConnectorRequestContext::try_new(
                Instant::now()
                    + if cancel {
                        std::time::Duration::from_secs(2)
                    } else {
                        std::time::Duration::from_millis(50)
                    },
                stop.view(),
                MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .unwrap();
            let stopper = if cancel {
                let observed = server.clone();
                Some(std::thread::spawn(move || {
                    let limit = Instant::now() + std::time::Duration::from_secs(1);
                    while observed.count("GET", blocked_path) == 0 && Instant::now() < limit {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                    stop.request_stop();
                }))
            } else {
                None
            };
            let outcome = adapter
                .prepare(stage_http_request(&adapter, context))
                .unwrap();
            if let Some(stopper) = stopper {
                stopper.join().unwrap();
            }
            let ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure } = outcome else {
                panic!("stopped preparation cannot stage a target")
            };
            assert_eq!(
                failure.kind(),
                if cancel {
                    ConnectorMutationFailureKind::Cancelled
                } else {
                    ConnectorMutationFailureKind::DeadlineExceeded
                }
            );
            assert_eq!(
                server.count("GET", blocked_path),
                1,
                "the original preparation waiter actually entered its pending request"
            );
            assert_eq!(server.count("POST", "/v1/namespaces/db/tables"), 0);
        }
    }
}

/// A vended CTAS whose route an execution node could not follow is refused
/// before the stage request (L25): the server's `/v1/config` adds a
/// routing header the catalog never declared, so nothing is staged.
#[test]
fn an_undeclared_routing_header_refuses_a_vended_ctas_before_staging() {
    use crate::rest_protocol::test_server::{Reply, Script, TestServer};

    let server = TestServer::start(
        Script::new()
            .on(
                "GET",
                "/v1/config",
                Reply::config(&[("header.x-region", "r1")], &[], None),
            )
            // No advertised endpoints: the existence probe is a plain GET.
            .on(
                "GET",
                "/v1/namespaces/db",
                Reply::json(
                    200,
                    &serde_json::json!({"namespace": ["db"], "properties": {}}),
                ),
            ),
    );
    let executor = tokio::runtime::Runtime::new().expect("runtime");
    let handle = executor.handle().clone();
    let endpoints = executor
        .block_on(async {
            crate::catalog_runtime::build_vended_rest_generation_for_test(
                HashMap::from([("uri".to_string(), server.url())]),
                None,
                crate::iceberg::Runtime::try_current().expect("test runtime"),
            )
            .await
        })
        .expect("REST generation");
    let configuration = crate::catalog_config::parse_catalog_configuration(
        "ice",
        &[
            ("iceberg.catalog.type".to_string(), "rest".to_string()),
            ("uri".to_string(), server.url()),
            ("warehouse".to_string(), "s3://warehouse".to_string()),
        ],
    )
    .expect("configuration");
    let binding = IcebergReadBinding::new(
        None,
        novarocks_fs::FsAccessResolver::new(),
        Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(handle.clone()),
        Arc::new(novarocks_fs::TokioFileIoSpawner::new(handle.clone())),
    );
    let runtime = IcebergMetadataContext::with_catalog_for_test(
        IcebergCatalogControlState::new(configuration),
        IcebergMetadataResources::new(binding, handle),
        crate::catalog::factory::NovaRocksCatalogFactory::adopt_vended_rest_for_test(
            endpoints,
            "s3://warehouse",
        ),
    );

    let adapter = adapter_for(Arc::new(runtime));
    let outcome = adapter
        .prepare(stage_http_request(&adapter, context()))
        .unwrap();
    assert!(
        matches!(&outcome,
        ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure }
            if failure.kind() == ConnectorMutationFailureKind::Unsupported
                && failure.message().contains("x-region")
                && failure.message().contains("iceberg.rest.server-routing-headers")
                && !failure.message().contains("r1")),
        "{outcome:?}"
    );
    assert_eq!(server.count("GET", "/v1/namespaces/db"), 2);
    assert_eq!(
        server.count("POST", "/v1/namespaces/db/tables"),
        0,
        "nothing is staged"
    );
}

#[test]
fn hadoop_generation_fails_closed_without_constructing_a_rest_client() {
    let runtime = Arc::new(hadoop_runtime());
    // The catalog owner answers this now; there is no concrete-client slot
    // left to inspect.
    assert_eq!(runtime.novarocks_catalog().implementation_name(), "hadoop");
    let adapter = adapter_for(Arc::clone(&runtime));
    let request = stage_http_request(&adapter, context());
    let table = prepare_typed_staged_table(&request, "s3://warehouse").unwrap();
    let failure = match prepare_rest_staged_table(
        &runtime,
        Some(&context()),
        None,
        ConnectorMutationOperationId::new(),
        novarocks_spi::connector::LakePublicationId::new_v7(),
        "db",
        "t",
        table,
    ) {
        Ok(_) => panic!("Hadoop must not expose a REST staged-create surface"),
        Err(failure) => failure,
    };
    // `Unsupported`, not `KnownUncommitted`: the two arms differ in what
    // they promise. Both say nothing was published, but only this one says
    // nothing was attempted, and a caller may act on that.
    // The catalog owner explains itself now, so the refusal names the
    // missing protocol rather than only repeating the word "unsupported".
    assert!(matches!(
        failure,
        RestStagedPrepareFailure::Unsupported(message)
            if message.contains("staged-create protocol")
    ));
}

fn ordinary_ctas_lease(
    adapter: &IcebergStagedCreateAdapter,
) -> novarocks_spi::connector::ConnectorStagedCreateSession {
    use novarocks_spi::connector::{ConnectorControlBinding, ConnectorControlPlanningPin};

    let descriptor = adapter.descriptor.clone();
    let incarnation = adapter.owner().incarnation;
    let provider = Arc::clone(&adapter.provider);
    let distribution = Arc::new(crate::provider_binding::IcebergInstanceDistribution::new(
        descriptor.clone(),
        incarnation,
    ));
    let control = ConnectorControlBinding::try_new_with_all_capabilities_and_staged_create(
        descriptor,
        incarnation,
        provider.clone(),
        provider,
        distribution,
        None,
        None,
        None,
        Some(Arc::new(adapter.clone())),
        None,
        None,
    )
    .unwrap();
    ConnectorControlPlanningPin::new(Arc::new(control), || {})
        .derive_staged_create_lease()
        .unwrap()
}

fn ctas_defaults(
    lease: &novarocks_spi::connector::ConnectorStagedCreateSession,
) -> Vec<(Arc<str>, novarocks_spi::connector::DdlPropertyValue)> {
    use novarocks_spi::connector::DdlPropertyValue;

    lease
        .ctas_property_declarations()
        .unwrap()
        .into_iter()
        .filter(|property| property.default_value() != &DdlPropertyValue::Null)
        .map(|property| (property.name().into(), property.default_value().clone()))
        .collect()
}

#[test]
fn ordinary_typed_ctas_retains_evaluated_values_and_real_output_partition_ids() {
    use crate::rest_protocol::test_server::{Script, TestServer};
    use novarocks_spi::connector::DdlPropertyValue;

    let server = TestServer::start(Script::new());
    let (_executor, adapter) = stage_http_adapter(&server);
    let lease = ordinary_ctas_lease(&adapter);
    let mut values = ctas_defaults(&lease);
    assert_eq!(
        values
            .iter()
            .find(|(name, _)| name.as_ref() == "format_version")
            .unwrap()
            .1,
        DdlPropertyValue::Integer(2),
    );
    assert_eq!(
        lease
            .check_ctas_property_name("sorted_by")
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::Unsupported
    );
    values
        .iter_mut()
        .find(|(name, _)| name.as_ref() == "partitioning")
        .unwrap()
        .1 = DdlPropertyValue::ArrayVarchar(vec![Some("bucket(id, 16)".into())]);
    values.push(("gc_enabled".into(), DdlPropertyValue::Boolean(false)));
    values.push((
        "extra_properties".into(),
        DdlPropertyValue::MapVarcharVarchar(vec![(
            "write.metadata.path".into(),
            Some("s3://warehouse/custom-metadata".into()),
        )]),
    ));
    let values: Arc<[(Arc<str>, DdlPropertyValue)]> = values.into();
    let original = stage_http_request(&adapter, context());
    let storage = lease
        .prepare_ctas_properties(
            Arc::clone(&values),
            Some("actual CTAS comment".into()),
            &original.context,
        )
        .unwrap();
    let request = lease
        .prepare_typed_request(
            original.publication_id,
            original.operation_id,
            original.table,
            original.columns,
            Arc::clone(&storage),
            original.policy,
            original.context,
        )
        .unwrap();
    let alias = request.clone();
    let actual = alias.ordinary_typed_properties().unwrap().unwrap();
    assert!(std::ptr::eq(storage.as_ref(), actual));
    assert!(std::ptr::eq(values.as_ref(), actual.properties()));

    let prepared = prepare_typed_staged_table(&request, "s3://warehouse/base").unwrap();
    assert_eq!(prepared.format_version, FormatVersion::V2);
    assert!(prepared.requested_location.is_none());
    assert_eq!(prepared.properties["write.format.default"], "parquet");
    assert_eq!(prepared.properties["gc.enabled"], "false");
    assert_eq!(prepared.properties["comment"], "actual CTAS comment");
    assert_eq!(
        prepared.properties["write.metadata.path"],
        "s3://warehouse/custom-metadata"
    );
    let spec = prepared.partition_spec.unwrap();
    let field = &spec.fields()[0];
    assert_eq!(
        field.source_id,
        prepared.schema.field_by_name("id").unwrap().id
    );
    assert_eq!(field.transform, crate::iceberg::spec::Transform::Bucket(16));

    let mut foreign = request.clone();
    foreign.owner.incarnation = ProviderBindingEpoch::new();
    let error = foreign.ordinary_typed_properties().err().unwrap();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert!(error.message().contains("foreign generation"));

    assert!(server.requests().is_empty());
}

#[test]
fn ordinary_typed_ctas_preserves_pre_source_and_bound_schema_failures() {
    use crate::rest_protocol::test_server::{Script, TestServer};
    use novarocks_spi::connector::{DdlPropertyValue, InvalidTableProperty};

    let server = TestServer::start(Script::new());
    let (executor, adapter) = stage_http_adapter(&server);
    let lease = ordinary_ctas_lease(&adapter);
    let mut values = ctas_defaults(&lease);
    values
        .iter_mut()
        .find(|(name, _)| name.as_ref() == "format_version")
        .unwrap()
        .1 = DdlPropertyValue::Integer(4);
    let error = lease
        .prepare_ctas_properties(values.into(), None, &context())
        .err()
        .unwrap();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert!(InvalidTableProperty::from_connector_error(&error).is_some());

    for asynchronous in [false, true] {
        let mut values = ctas_defaults(&lease);
        values
            .iter_mut()
            .find(|(name, _)| name.as_ref() == "partitioning")
            .unwrap()
            .1 = DdlPropertyValue::ArrayVarchar(vec![Some("bucket(missing, 16)".into())]);
        let original = stage_http_request(&adapter, context());
        let operation = original.operation_id;
        let storage = lease
            .prepare_ctas_properties(values.into(), None, &original.context)
            .unwrap();
        let request = lease
            .prepare_typed_request(
                original.publication_id,
                operation,
                original.table,
                original.columns,
                storage,
                original.policy,
                original.context,
            )
            .unwrap();
        let outcome = if asynchronous {
            let receiving = Mutex::new(None);
            executor.block_on(lease.prepare_async(request, &receiving))
        } else {
            lease.prepare(request)
        }
        .unwrap();
        let ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure } = outcome else {
            panic!("missing real output source cannot dispatch a stage");
        };
        let marker =
            InvalidTableProperty::from_connector_error(failure.connector_source().unwrap())
                .unwrap();
        assert!(
            marker
                .source_error()
                .message()
                .contains("partition source column")
        );
        assert!(!adapter.operations.lock().unwrap().contains_key(&operation));
    }
    assert!(server.requests().is_empty());
}

#[test]
fn typed_mv_staged_preflight_binds_visible_partition_and_refuses_hidden_partition() {
    use crate::rest_protocol::test_server::{Script, TestServer};
    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperties, CatalogVersion, ConnectorControlBinding,
        ConnectorControlPlanningPin, ConnectorDocumentManagementAdmissionRequest,
        ConnectorDocumentManagementOperation, ConnectorDocumentStorageBinding, DdlPropertyValue,
        InvalidTableProperty,
    };

    let server = TestServer::start(Script::new());
    let (executor, adapter) = stage_http_adapter(&server);
    let provider = Arc::clone(&adapter.provider);
    let descriptor = adapter.descriptor.clone();
    let owner = adapter.owner();
    let catalog = CatalogHandle::new(
        owner.instance_id.clone(),
        CatalogVersion::from_bytes([17; 32]),
    );
    let storage = Arc::new(crate::document_storage::IcebergDocumentStorage::new(
        descriptor.clone(),
        owner.incarnation,
        Arc::clone(&adapter.runtime),
    ));
    let documents = ConnectorDocumentStorageBinding::try_new(
        descriptor.clone(),
        owner.incarnation,
        None,
        Some(storage),
    )
    .unwrap();
    let distribution = Arc::new(crate::provider_binding::IcebergInstanceDistribution::new(
        descriptor.clone(),
        owner.incarnation,
    ));
    let control_binding = ConnectorControlBinding::try_new_with_all_capabilities_and_staged_create(
        descriptor.clone(),
        owner.incarnation,
        provider.clone(),
        provider,
        distribution,
        None,
        None,
        None,
        Some(Arc::new(adapter.clone())),
        None,
        None,
    )
    .and_then(|binding| {
        binding.with_catalog_properties(
            CatalogProperties::new(catalog.clone(), descriptor.provider_id, 1, vec![], vec![])
                .unwrap(),
        )
    })
    .and_then(|binding| binding.try_with_document_storage(Some(documents)))
    .unwrap();
    let pin = ConnectorControlPlanningPin::new(Arc::new(control_binding), || {});
    let lease = pin.derive_staged_create_lease().unwrap();
    let document_lease = pin.derive_document_storage_lease().unwrap();
    let request = |partition: &str, include_metadata_path: bool| {
        let original = stage_http_request(&adapter, context());
        let admission = document_lease
            .admit_management(
                ConnectorDocumentManagementAdmissionRequest::try_new(
                    owner.clone(),
                    catalog.clone(),
                    original.operation_id,
                    original.table.clone(),
                    None,
                    ConnectorDocumentManagementOperation::Create,
                    original.context.clone(),
                )
                .unwrap(),
            )
            .unwrap();
        let mut columns = original.columns;
        let mut hidden = columns[0].clone();
        hidden.name = "hidden_key".into();
        columns.push(hidden);
        let mut properties: Vec<(Arc<str>, DdlPropertyValue)> = lease
            .mv_storage_property_declarations()
            .unwrap()
            .into_iter()
            .filter(|declaration| declaration.default_value() != &DdlPropertyValue::Null)
            .map(|declaration| {
                (
                    Arc::from(declaration.name()),
                    declaration.default_value().clone(),
                )
            })
            .collect::<Vec<_>>();
        properties
            .iter_mut()
            .find(|(name, _)| &**name == "partitioning")
            .unwrap()
            .1 = DdlPropertyValue::ArrayVarchar(vec![Some(format!("bucket({partition}, 16)"))]);
        properties.push((
            "data_location".into(),
            DdlPropertyValue::Varchar("s3://user/data".into()),
        ));
        if include_metadata_path {
            properties.push((
                "extra_properties".into(),
                DdlPropertyValue::MapVarcharVarchar(vec![(
                    "WRITE.METADATA.PATH".into(),
                    Some("s3://warehouse/base/metadata/managed".into()),
                )]),
            ));
        }
        lease
            .prepare_document_managed_typed_request(
                original.publication_id,
                original.operation_id,
                original.table,
                columns,
                vec!["id".into()],
                properties.into(),
                BTreeMap::from([(
                    Arc::from(novarocks_spi::connector::CONNECTOR_MV_APPLY_KEY_COLUMN_PROPERTY),
                    Arc::from("hidden_key"),
                )]),
                original.policy,
                admission,
                original.context,
            )
            .unwrap()
    };
    let path = "s3://warehouse/base/metadata/managed";
    let valid = request("id", true);
    let alias = valid.clone();
    let (
        ConnectorStagedCreateMode::ApplicationDocumentManaged {
            storage: original, ..
        },
        ConnectorStagedCreateMode::ApplicationDocumentManaged { storage: alias, .. },
    ) = (&valid.mode, &alias.mode)
    else {
        panic!("Typed request must retain its original evaluated storage");
    };
    assert!(Arc::ptr_eq(original, alias));
    let prepared = prepare_mv_staged_table(&valid, "s3://warehouse/base").unwrap();
    assert_eq!(
        prepared
            .final_metadata_path
            .as_ref()
            .map(StagedMetadataPath::as_str),
        Some(path)
    );
    assert_eq!(prepared.properties[WRITE_METADATA_PATH], path);
    assert_eq!(
        prepare_mv_staged_table(&valid, "s3://foreign/base")
            .err()
            .unwrap()
            .kind(),
        ConnectorErrorKind::Unsupported,
    );
    assert_eq!(prepared.format_version, FormatVersion::V3);
    assert_eq!(prepared.schema.field_by_name("hidden_key").unwrap().id, 2);
    let spec = prepared.partition_spec.unwrap();
    assert_eq!(spec.fields()[0].source_id, 1);
    assert_eq!(
        spec.fields()[0].transform,
        crate::iceberg::spec::Transform::Bucket(16)
    );
    assert_eq!(prepared.properties["write.data.path"], "s3://user/data");
    assert_eq!(prepared.properties["write.format.default"], "parquet");

    for asynchronous in [false, true] {
        let invalid = request("hidden_key", false);
        let operation = invalid.operation_id;
        let outcome = if asynchronous {
            let receiving = Mutex::new(None);
            executor.block_on(lease.prepare_async(invalid, &receiving))
        } else {
            lease.prepare(invalid)
        }
        .unwrap();
        let ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure } = outcome else {
            panic!("An invisible partition field must fail before dispatch");
        };
        let property =
            InvalidTableProperty::from_connector_error(failure.connector_source().unwrap())
                .unwrap();
        assert!(
            property
                .source_error()
                .message()
                .contains("must be an output column")
        );
        assert!(!adapter.operations.lock().unwrap().contains_key(&operation));
    }
    assert!(server.requests().is_empty());
}

/// How an `Unsupported` refusal crosses the SPI boundary.
///
/// The neutral prepare outcome has three arms and deliberately no
/// `Unsupported` one: the arm answers whether anything happened out there,
/// and a refusal answers "no". So the refusal travels in `KnownUncommitted`
/// and carries its reason in the failure kind. This asserts that shape, and
/// that a refused prepare leaves no operation slot behind.
#[test]
fn an_unsupported_ctas_reaches_the_spi_as_uncommitted_with_an_unsupported_kind() {
    let (adapter, request) = unsupported_staged_adapter();
    let operation_id = request.operation_id;
    let outcome = adapter.prepare(request).expect("prepare answers typed");
    let ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure } = outcome else {
        panic!("a refusal must not claim an unknown or conflicting outcome");
    };
    assert_eq!(failure.kind(), ConnectorMutationFailureKind::Unsupported);
    assert!(
        adapter
            .operations
            .lock()
            .expect("operation lock")
            .get(&operation_id)
            .is_none(),
        "a refusal that attempted nothing must not retain an operation slot"
    );
}

/// The adapter attaches everywhere; the refusal moved to admission.
///
/// This used to assert the opposite — that constructing the adapter on a
/// Hadoop generation failed — which made an absent slot stand in for an
/// unsupported request. Those are different facts, and conflating them is
/// what let a CTAS discover it was impossible only after work had started.
#[test]
fn staged_adapter_attaches_on_every_generation() {
    let runtime = Arc::new(hadoop_runtime());
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
        instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
    };
    let provider = Arc::new(IcebergMetadata::new(
        descriptor.clone(),
        ProviderBindingEpoch::new(),
        Arc::clone(&runtime),
    ));
    IcebergStagedCreateAdapter::try_new(provider)
        .expect("the staged-create adapter attaches on a Hadoop generation");
}

/// A Hadoop catalog refuses CTAS before the statement can do anything.
#[test]
fn hadoop_refuses_ctas_at_admission_with_a_typed_unsupported() {
    let runtime = Arc::new(hadoop_runtime());
    let unsupported = runtime
        .novarocks_catalog()
        .admit(
            &crate::catalog::admission::CatalogAdmissionRequest::statement(
                crate::catalog::admission::CatalogOperation::CreateTable(
                    crate::catalog::CatalogCreateIntent::CreateTableAsSelect,
                ),
                crate::catalog::CatalogTableName::new("db", "t"),
            ),
        )
        .expect_err("Hadoop cannot publish a CTAS target atomically");
    assert!(unsupported.message().contains("staged-create"));
    runtime
        .novarocks_catalog()
        .admit(
            &crate::catalog::admission::CatalogAdmissionRequest::statement(
                crate::catalog::admission::CatalogOperation::CreateTable(
                    crate::catalog::CatalogCreateIntent::EmptyTable,
                ),
                crate::catalog::CatalogTableName::new("db", "t"),
            ),
        )
        .expect("an empty-table create is atomic on this catalog");
}

#[test]
fn operation_marker_is_a_canonical_operation_uuid() {
    let operation_id = ConnectorMutationOperationId::new();
    assert_eq!(
        operation_marker(operation_id),
        uuid::Uuid::from_bytes(operation_id.to_bytes()).to_string()
    );
}

#[test]
fn staged_write_prefix_is_operation_bound_and_canonical() {
    let operation_id = ConnectorMutationOperationId::new();
    assert_eq!(
        staged_write_data_prefix("s3://warehouse/db/table/", operation_id),
        format!(
            "s3://warehouse/db/table/data/_staging/{}",
            operation_marker(operation_id)
        )
    );
}

/// A possibly-applied publication can never be aborted or driven again.
///
/// `take_prepared` is the single gate in front of `plan_write`,
/// `bind_write` and `publish`; abort's cleanup-only gate also refuses
/// unknown states. Proving these gates put closed states back untouched
/// proves that a
/// `PublicationUnknown` operation cannot be cleaned up, re-published, or
/// silently re-prepared under the same identity.
#[test]
fn an_unknown_publication_can_never_be_aborted_or_driven_again() {
    let runtime = rest_runtime();
    let operation_id = ConnectorMutationOperationId::new();
    let closed = [
        OperationState::PublicationUnknown(PublicationUnknownOperation {
            evidence: ExternalMutationEvidence::try_new(
                EVIDENCE_VERSION,
                ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                    instance_id: ConnectorInstanceId::parse("ice").unwrap(),
                },
                ProviderBindingEpoch::new(),
                operation_id,
                "staged-create-publish",
                Bytes::new(),
            )
            .unwrap(),
            prepared: prepared_operation(&runtime),
        }),
        OperationState::Published {
            handle_digest: [0; 32],
        },
        OperationState::PublishedRetained(prepared_operation(&runtime)),
        OperationState::Aborted {
            handle_digest: [0; 32],
        },
        OperationState::Preparing,
        OperationState::Publishing {
            handle_digest: [4; 32],
        },
    ];
    for state in closed {
        let label = format!("{:?}", std::mem::discriminant(&state));
        let operations = Mutex::new(HashMap::from([(operation_id, state)]));
        take_prepared(&operations, operation_id)
            .err()
            .unwrap_or_else(|| panic!("{label} must not hand out an abortable aggregate"));
        assert!(
            take_abortable(&operations, operation_id).is_err(),
            "{label} must not grant cleanup authority"
        );
        assert!(
            operations
                .lock()
                .expect("operation lock")
                .contains_key(&operation_id),
            "{label} must be put back, not consumed by a refused caller"
        );
    }

    // The one state that does hand it out, so the assertions above are
    // about the state and not about the gate being closed to everything.
    let operations = Mutex::new(HashMap::from([(
        operation_id,
        OperationState::Prepared(prepared_operation(&runtime)),
    )]));
    take_prepared(&operations, operation_id).expect("a prepared target is still drivable");
    assert!(
        !operations
            .lock()
            .expect("operation lock")
            .contains_key(&operation_id),
        "a driven target leaves its slot to the caller's terminal record"
    );
}

#[test]
fn cleanup_failure_is_not_overwritten_as_complete() {
    let ExternalMutationFinalization::Failed(failure) =
        cleanup_failed("delete staged manifest failed")
    else {
        panic!("cleanup failure must remain visible")
    };
    assert_eq!(failure.kind(), ConnectorMutationFailureKind::Unavailable);
    assert!(failure.message().contains("staged manifest"));
}

/// The receipt is the only thing that crosses from the write session to the
/// publication, so it is also the only thing that can be forged. A payload
/// this generation did not mint fails to decode rather than being
/// interpreted as "no artifacts", which would publish an empty table over a
/// write that actually staged files.
#[test]
fn cleanup_pending_can_retry_cleanup_without_reopening_publication() {
    let runtime = rest_runtime();
    let operation_id = ConnectorMutationOperationId::new();
    let mut prepared = prepared_operation(&runtime);
    prepared.publication_closed = true;
    let operations = Mutex::new(HashMap::from([(
        operation_id,
        OperationState::CleanupPending(prepared),
    )]));
    assert!(take_prepared(&operations, operation_id).is_err());
    let prepared = take_abortable(&operations, operation_id)
        .expect("definite non-publication still permits cleanup");
    assert!(prepared.publication_closed);
    operations
        .lock()
        .unwrap()
        .insert(operation_id, OperationState::Prepared(prepared));
    assert!(
        take_prepared(&operations, operation_id).is_err(),
        "closed aggregate cannot be reopened by an incorrect state projection"
    );
}

#[test]
fn a_foreign_receipt_is_refused_rather_than_read_as_an_empty_write() {
    let runtime = Arc::new(rest_runtime());
    let prepared = prepared_operation(&runtime);
    let owner = adapter_for(runtime).owner();
    let metadata = prepared.staged.table.metadata();

    let foreign = ConnectorStagedWriteProof::try_new(
        novarocks_spi::connector::ConnectorWriteReceipt::try_new(Bytes::from_static(
            b"not an Iceberg staged-create seal",
        ))
        .expect("receipt"),
        7,
    )
    .expect("write proof");
    let Err(error) =
        crate::write_codec::open_compact_staged_files(foreign.receipt(), metadata, 7, &owner)
    else {
        panic!("a foreign receipt must be rejected");
    };
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);

    let receipt =
        crate::write_codec::seal_staged_files(Vec::new(), metadata, [9; 16], &owner).expect("seal");
    let mine = ConnectorStagedWriteProof::try_new(receipt, 0).expect("write proof");
    let seal = crate::write_codec::open_compact_staged_files(mine.receipt(), metadata, 0, &owner)
        .expect("own compact seal");
    seal.with_inventory(|inventory| {
        assert!(inventory.files.is_empty());
        Ok(())
    })
    .unwrap();
}

// Observe one original compact attachment and its actual cleanup ownership across refusals.
#[test]
fn binding_rejects_missing_changed_or_foreign_seal_before_catalog_and_preserves_original_owner() {
    use crate::commit::write_stack::domain::{
        IcebergCommitFragment, IcebergDataFileArtifact, IcebergWriteFlavor, IcebergWriteSessionId,
        IcebergWriteSessionState, IcebergWriteTableFacts,
    };
    use crate::commit::write_stack::planning::{IcebergWriteSessionPlanInput, plan_write_session};
    use crate::commit::write_stack::test_support::{
        data_branch_plan, paid_fragments, sample_partition, write_query_identity,
    };
    use crate::rest_protocol::test_server::{Script, TestServer};
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteControl, ConnectorWriteFinishRequest, WriteTargetOrdinal,
    };
    use novarocks_spi::connector::{
        ConnectorWriteAdmissionPurpose, ConnectorWriteReceipt, ExternalMutationOutcome,
    };
    for case in [
        "missing",
        "wire",
        "digest",
        "foreign_generation",
        "writer_endpoint",
        "promoted_schema",
        "row_count",
    ] {
        let server = TestServer::start(Script::new());
        let (_executor, adapter) = stage_http_adapter(&server);
        let scratch = tempfile::tempdir().unwrap();
        let location = format!("file://{}/table", scratch.path().display());
        let mut target = prepared_operation_at(&adapter.runtime, &location);
        let staged_metadata = target.staged.table.metadata().clone();
        let operation_id = target.operation_id();
        let owner = adapter.owner();
        let handle = ConnectorStagedTableHandle::try_new(
            owner.clone(),
            operation_id,
            Bytes::from_static(b"unpublished-target"),
        )
        .unwrap();
        target.handle_digest = handle.digest();
        let planning = adapter
            .provider
            .staged_write_table_handle(&target.staged.table, operation_id, &context())
            .unwrap();
        let metadata = Arc::new(
            crate::metadata::staged_target_metadata(&owner.instance_id, &planning)
                .unwrap()
                .metadata,
        );
        let data_prefix = metadata.properties()["write.data.path"].clone();
        target.planning = Some(
            ConnectorStagedWritePlanningBinding::try_new(
                &handle,
                planning,
                Bytes::new(),
                context(),
            )
            .unwrap(),
        );
        adapter.record_terminal(operation_id, OperationState::Prepared(target));
        let data = url::Url::parse(&data_prefix)
            .unwrap()
            .to_file_path()
            .unwrap()
            .join("owned.parquet");
        let sibling = data.with_file_name("sibling.parquet");
        std::fs::create_dir_all(data.parent().unwrap()).unwrap();
        std::fs::write(&data, b"owned output").unwrap();
        std::fs::write(&sibling, b"outside the operation").unwrap();
        let path = format!("file://{}", data.display());
        let session_id = IcebergWriteSessionId::new();
        let facts = IcebergWriteTableFacts::try_new(
            metadata.uuid().to_string(),
            "db".into(),
            "t".into(),
            location.clone(),
            data_prefix,
            "main".into(),
            None,
            0,
            metadata.current_schema_id(),
            metadata.default_partition_spec_id(),
            2,
        )
        .unwrap();
        let (session, _) = plan_write_session(
            session_id,
            IcebergWriteSessionPlanInput {
                flavor: IcebergWriteFlavor::StagedCreate,
                purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                table: facts,
                base_version_digest: None,
                staged_metadata: Some(metadata.clone()),
                data: data_branch_plan(),
                deletes: vec![],
            },
        )
        .unwrap();
        let catalog_handle = CatalogHandle::new(
            owner.instance_id.clone(),
            CatalogVersion::from_bytes([1; 32]),
        );
        let source_adapter = crate::commit::write_stack::runtime::build_write_adapter(
            adapter.descriptor.clone(),
            catalog_handle.clone(),
        );
        let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
            adapter.descriptor.clone(),
            owner.incarnation,
            catalog_handle,
            adapter.runtime.clone(),
        );
        let expected = session.expected_targets();
        let commit = source_adapter.wrap_commit_session(session);
        let fragment = IcebergCommitFragment::data_file(
            IcebergDataFileArtifact::try_new(
                path,
                crate::delete_file::IcebergFileFormat::Parquet,
                sample_partition(),
                crate::commit::write_stack::domain::IcebergArtifactMetrics::try_new(
                    3,
                    12,
                    Vec::new(),
                    Some(crate::commit::report::IcebergColumnStats {
                        lower_bounds: BTreeMap::from([(1, 1_i64.to_le_bytes().to_vec())]),
                        upper_bounds: BTreeMap::from([(1, 3_i64.to_le_bytes().to_vec())]),
                        ..Default::default()
                    }),
                )
                .unwrap(),
                None,
            )
            .unwrap(),
        );
        let (mut paid, author) = paid_fragments(
            &source_adapter,
            vec![source_adapter.wrap_commit_fragment(fragment)],
        );
        let query = write_query_identity();
        control
            .begin_write_query(&commit, &query, &expected, None, None)
            .unwrap();
        control
            .arrive_write_fragments(
                &commit,
                &query,
                vec![(WriteTargetOrdinal::try_new(0).unwrap(), paid.pop().unwrap())],
                &author,
            )
            .unwrap();
        control.confirm_write_received(&commit, &query).unwrap();
        let outcome = control
            .finish_write(ConnectorWriteFinishRequest {
                commit: &commit,
                row_count: 3,
                statistics: vec![],
                publication:
                    novarocks_spi::connector::write_stack::ConnectorWriteFinishPublication::None,
                context: context(),
            })
            .unwrap();
        let ExternalMutationOutcome::KnownCommitted {
            receipt,
            effect: ExternalMutationEffect::NoOp,
            ..
        } = outcome
        else {
            panic!("the source only seals its accepted files");
        };
        let seal =
            crate::write_codec::open_compact_staged_files(&receipt, &metadata, 3, &owner).unwrap();
        let files = crate::write_codec::open_staged_files(&receipt, &metadata, &owner).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].lower_bounds[&1],
            crate::iceberg::spec::Datum::long(1)
        );
        assert_eq!(
            files[0].upper_bounds[&1],
            crate::iceberg::spec::Datum::long(3)
        );

        let offered = match case {
            "missing" => ConnectorWriteReceipt::try_new(receipt.payload().clone()).unwrap(),
            "wire" => {
                ConnectorWriteReceipt::try_from_wire_v1(&receipt.try_to_wire_v1().unwrap()).unwrap()
            }
            "digest" => {
                let mut facts: serde_json::Value =
                    serde_json::from_slice(receipt.payload()).unwrap();
                let first = facts["files_digest"][0].as_u64().unwrap();
                facts["files_digest"][0] = serde_json::Value::from(first ^ 1);
                ConnectorWriteReceipt::try_new(Bytes::from(serde_json::to_vec(&facts).unwrap()))
                    .unwrap()
                    .with_process_local_payload(
                        &owner,
                        receipt
                            .process_local_payload(&owner)
                            .unwrap()
                            .unwrap()
                            .clone(),
                    )
                    .unwrap()
            }
            "foreign_generation" => {
                let foreign = ConnectorProviderBindingKey {
                    instance_id: owner.instance_id.clone(),
                    incarnation: ProviderBindingEpoch::new(),
                };
                crate::write_codec::seal_staged_files(
                    files.to_vec(),
                    &metadata,
                    session_id.to_bytes(),
                    &foreign,
                )
                .unwrap()
            }
            "writer_endpoint" => crate::write_codec::seal_staged_files(
                files.to_vec(),
                &staged_metadata,
                session_id.to_bytes(),
                &owner,
            )
            .unwrap(),
            "promoted_schema" => {
                use crate::iceberg::spec::{Datum, NestedField, PrimitiveType, Schema, Type};
                let schema = Schema::builder()
                    .with_fields(vec![Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    ))])
                    .build()
                    .unwrap();
                let narrowed = metadata
                    .as_ref()
                    .clone()
                    .into_builder(None)
                    .add_current_schema(schema)
                    .unwrap()
                    .build()
                    .unwrap()
                    .metadata;
                let mut narrowed_files = files.to_vec();
                narrowed_files[0].lower_bounds.insert(1, Datum::int(1));
                narrowed_files[0].upper_bounds.insert(1, Datum::int(3));
                crate::write_codec::seal_staged_files(
                    narrowed_files,
                    &narrowed,
                    session_id.to_bytes(),
                    &owner,
                )
                .unwrap()
            }
            "row_count" => receipt.clone(),
            _ => unreachable!(),
        };
        let offered_rows = if case == "row_count" { 4 } else { 3 };
        let error = adapter
            .bind_write(
                handle.clone(),
                ConnectorStagedWriteProof::try_new(offered, offered_rows).unwrap(),
            )
            .unwrap_err();
        assert_eq!(
            error.kind(),
            if case == "foreign_generation" {
                ConnectorErrorKind::InvalidRequest
            } else {
                ConnectorErrorKind::CorruptData
            },
            "{case}: {error}"
        );
        assert!(
            server.requests().is_empty(),
            "{case}: binding must not call Catalog"
        );
        assert!(data.exists());
        assert!(matches!(
            source_adapter
                .commit_session(&commit)
                .unwrap()
                .state()
                .unwrap(),
            IcebergWriteSessionState::Sealed
        ));
        assert_eq!(
            control.cleanup_responsibility(&commit).unwrap(),
            novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained
        );
        {
            let operations = adapter.operations.lock().unwrap();
            let Some(OperationState::Prepared(target)) = operations.get(&operation_id) else {
                panic!("failed binding preserves the original target");
            };
            assert!(target.write.is_none());
            assert!(target.publication.is_none());
        }

        let valid = ConnectorStagedWriteProof::try_new(receipt, 3).unwrap();
        adapter.bind_write(handle.clone(), valid.clone()).unwrap();
        {
            let operations = adapter.operations.lock().unwrap();
            let Some(OperationState::Prepared(target)) = operations.get(&operation_id) else {
                panic!("binding preserves the target");
            };
            assert!(Arc::ptr_eq(&seal, &target.write.as_ref().unwrap().seal));
        }
        let aborted = adapter
            .abort(ConnectorStagedCreateAbortRequest {
                operation_id,
                handle,
                write: Some(valid),
                context: context(),
            })
            .unwrap();
        assert!(matches!(
            aborted,
            ConnectorStagedCreateAbortOutcome::Aborted {
                finalization: ExternalMutationFinalization::Complete,
                ..
            }
        ));
        assert!(seal.with_inventory(|_| Ok(())).is_err());
        assert!(!data.exists());
        assert_eq!(std::fs::read(sibling).unwrap(), b"outside the operation");
        assert!(server.requests().is_empty());
    }
}
