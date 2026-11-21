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
use std::time::{Duration, Instant};

use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorMetadataMaintenanceExecuteRequest,
    ConnectorMetadataMaintenanceOperation, ConnectorMetadataMaintenancePlanningRequest,
    ConnectorProviderId, ConnectorTableHandle, ProviderBindingEpoch,
};

use crate::access_binding::IcebergReadBinding;
use crate::catalog_control::IcebergCatalogControlState;
use crate::resources::IcebergMetadataResources;

trait MaintenanceExecuteForTest {
    async fn execute(
        &self,
        request: ConnectorMetadataMaintenanceExecuteRequest,
    ) -> Result<ConnectorMetadataMaintenanceOutcome, ConnectorError>;
}
impl MaintenanceExecuteForTest for novarocks_spi::connector::ConnectorMetadataMaintenanceSession {
    async fn execute(
        &self,
        request: ConnectorMetadataMaintenanceExecuteRequest,
    ) -> Result<ConnectorMetadataMaintenanceOutcome, ConnectorError> {
        let mut execution = self.begin_execute(request)?;
        execution.wait().await
    }
}
fn context() -> novarocks_spi::connector::ConnectorRequestContext {
    novarocks_spi::connector::ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        1024,
        4096,
    )
    .expect("context")
}

fn adapter() -> (
    tokio::runtime::Runtime,
    tempfile::TempDir,
    IcebergMetadataMaintenanceAdapter,
    ConnectorProviderBindingKey,
) {
    let executor = tokio::runtime::Runtime::new().expect("runtime");
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
            IcebergCatalogControlState::new(configuration),
            IcebergMetadataResources::new(binding, executor.handle().clone()),
        )
        .expect("control runtime"),
    );
    let key = ConnectorProviderBindingKey {
        instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
    };
    runtime
        .novarocks_catalog()
        .bind_publication_generation(key.incarnation)
        .expect("test generation");
    let adapter = IcebergMetadataMaintenanceAdapter::new(key.clone(), runtime).expect("adapter");
    (executor, warehouse, adapter, key)
}

fn plan(
    key: ConnectorProviderBindingKey,
    operation_id: ConnectorMutationOperationId,
    table_payload: &'static [u8],
) -> ConnectorMetadataMaintenancePlan {
    let table =
        ConnectorTableHandle::try_new(key.instance_id.clone(), Bytes::from_static(table_payload))
            .expect("table handle");
    let request = ConnectorMetadataMaintenancePlanningRequest::try_new(
        operation_id,
        key,
        ConnectorMetadataMaintenanceOperation::rewrite_metadata_layout(table).expect("operation"),
        context(),
    )
    .expect("planning request");
    ConnectorMetadataMaintenancePlan::try_new(
        &request,
        [3; 32],
        ConnectorMetadataMaintenancePlanSummary::new(1, 2, 3, 4, 5),
        Bytes::from_static(b"provider-plan"),
    )
    .expect("plan")
}

fn receipt(
    adapter: &IcebergMetadataMaintenanceAdapter,
    plan: &ConnectorMetadataMaintenancePlan,
) -> ConnectorMetadataMaintenanceReceipt {
    ConnectorMetadataMaintenanceReceipt::try_new(
        adapter.descriptor.clone(),
        adapter.key.incarnation,
        plan.operation_id(),
        plan.operation_kind(),
        plan.request_digest(),
        plan.plan_digest(),
        plan.intent_digest(),
        ConnectorMetadataMaintenanceReceiptSummary::default(),
        Bytes::from_static(b"receipt"),
    )
    .expect("receipt")
}

#[test]
fn maintenance_hex_codec_is_canonical_and_strict() {
    let digest = Sha256::digest(b"maintenance");
    let encoded = hex_encode(digest);
    assert_eq!(encoded.len(), 64);
    assert_eq!(hex_decode(&encoded).as_deref(), Some(digest.as_slice()));
    assert!(hex_decode("0").is_none());
    assert!(hex_decode("xy").is_none());
}

fn create_local_table(
    executor: &tokio::runtime::Runtime,
    adapter: &IcebergMetadataMaintenanceAdapter,
) -> crate::iceberg::table::Table {
    create_local_table_with_version(executor, adapter, crate::iceberg::spec::FormatVersion::V2)
}

fn create_local_table_with_version(
    executor: &tokio::runtime::Runtime,
    adapter: &IcebergMetadataMaintenanceAdapter,
    version: crate::iceberg::spec::FormatVersion,
) -> crate::iceberg::table::Table {
    use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use crate::iceberg::{NamespaceIdent, TableCreation};

    executor.block_on(async {
        use crate::catalog::CatalogNamespaceName;
        use crate::catalog::catalog_request::CatalogPublicationRequest;
        use crate::catalog::error::CatalogOutcome;
        let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
            &adapter.runtime,
            adapter.key.incarnation,
            CatalogPublicationRequest::CreateNamespace {
                namespace: CatalogNamespaceName::new("maintenance_test"),
                properties: HashMap::new(),
                no_op_if_exists: false,
            },
        )
        .await;
        assert!(
            matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
            "{outcome:?}"
        );
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
            &adapter.runtime,
            adapter.key.incarnation,
            CatalogPublicationRequest::CreateTable {
                namespace: CatalogNamespaceName::new("maintenance_test"),
                creation: TableCreation::builder()
                    .name("t".into())
                    .schema(schema)
                    .format_version(version)
                    .build(),
                no_op_if_exists: false,
            },
        )
        .await;
        assert!(
            matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
            "{outcome:?}"
        );
        crate::catalog::admission_test_support::load_static_fixture_table(
            &adapter.runtime,
            &crate::iceberg::TableIdent::new(
                NamespaceIdent::new("maintenance_test".into()),
                "t".into(),
            ),
        )
        .await
    })
}

fn append_local_file(
    executor: &tokio::runtime::Runtime,
    adapter: &IcebergMetadataMaintenanceAdapter,
    ident: &crate::iceberg::TableIdent,
    name: &str,
) {
    executor.block_on(append_fixture_file(adapter, ident, name));
}

async fn append_fixture_file(
    adapter: &IcebergMetadataMaintenanceAdapter,
    ident: &crate::iceberg::TableIdent,
    name: &str,
) {
    use crate::iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
    let table =
        crate::catalog::admission_test_support::load_static_fixture_table(&adapter.runtime, ident)
            .await;
    let location = format!("{}/data/{name}.parquet", table.metadata().location());
    table
        .file_io()
        .new_output(&location)
        .expect("data output")
        .write(Bytes::from_static(b"test data"))
        .await
        .expect("write data file");
    let file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(location)
        .file_format(DataFileFormat::Parquet)
        .record_count(1)
        .file_size_in_bytes(9)
        .partition(Struct::empty())
        .build()
        .expect("synthetic data file");
    let contents = vec![
        crate::commit::model::AddedContent::new_logical_data(
            file,
            table.metadata().default_partition_spec_id(),
        )
        .unwrap(),
    ];
    let provider = crate::metadata::IcebergMetadata::new(
        adapter.descriptor.clone(),
        adapter.key.incarnation,
        Arc::clone(&adapter.runtime),
    );
    crate::commit::run::append_contents_for_test(
        provider,
        table,
        contents,
        "main".into(),
        BTreeMap::new(),
    )
    .await
    .expect("publish fixture append through its owner");
}

fn local_planning_request(
    key: &ConnectorProviderBindingKey,
    expire: bool,
) -> ConnectorMetadataMaintenancePlanningRequest {
    let table = ConnectorTableHandle::try_new(
        key.instance_id.clone(),
        Bytes::from_static(br#"{"namespace":"maintenance_test","table":"t","metadata_location":null,"table_info":null,"metadata_columns":[],"metadata_table_type":null,"prepared_files":[],"explicit_files":null}"#),
    )
    .expect("table handle");
    let operation = if expire {
        ConnectorMetadataMaintenanceOperation::expire_table_versions(table, Some(i64::MAX), Some(1))
    } else {
        ConnectorMetadataMaintenanceOperation::rewrite_metadata_layout(table)
    }
    .expect("operation");
    ConnectorMetadataMaintenancePlanningRequest::try_new(
        ConnectorMutationOperationId::new(),
        key.clone(),
        operation,
        context(),
    )
    .expect("planning request")
}

fn inventory_without_private_artifacts(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn visit(path: &std::path::Path, inventory: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(path).expect("table inventory") {
            let entry = entry.expect("table entry");
            assert_ne!(
                entry.file_name(),
                "_novarocks",
                "private maintenance artifact"
            );
            inventory.push(entry.path());
            if entry.file_type().expect("entry type").is_dir() {
                visit(&entry.path(), inventory);
            }
        }
    }
    let mut inventory = Vec::new();
    visit(root, &mut inventory);
    inventory.sort();
    inventory
}

// Responsibility: real Hadoop/FileIO adapter behavior, including no-op and
// rejected plans. SQL/system tests own the distributed product surface.
#[test]
fn metadata_maintenance_keeps_plans_and_receipts_out_of_table_locations() {
    let (executor, _warehouse, adapter, key) = adapter();
    let table = create_local_table(&executor, &adapter);
    let location =
        novarocks_fs::FsLocation::parse(table.metadata().location()).expect("table location");
    let root = std::path::PathBuf::from(location.path());
    for expire in [false, true] {
        let before = inventory_without_private_artifacts(&root);
        let plan = executor
            .block_on(adapter.plan_maintenance(local_planning_request(&key, expire)))
            .expect("no-op plan");
        assert_eq!(inventory_without_private_artifacts(&root), before);
        let payload: IcebergMetadataMaintenancePlanPayloadV1 =
            decode_canonical_json(plan.provider_payload(), "test plan").expect("plan payload");
        assert!(payload.evaluation_time_ms > 0);
        assert_eq!(payload.table_uuid, table.metadata().uuid().to_string());
        let outcome = executor
            .block_on(
                adapter.execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context())
                        .expect("execute request"),
                ),
            )
            .expect("no-op execute");
        let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
            panic!("no-op maintenance must complete")
        };
        assert_eq!(
            receipt.summary(),
            ConnectorMetadataMaintenanceReceiptSummary::default()
        );
        assert_eq!(inventory_without_private_artifacts(&root), before);
    }

    append_local_file(&executor, &adapter, table.identifier(), "a");
    append_local_file(&executor, &adapter, table.identifier(), "b");
    for expire in [false, true] {
        let before = inventory_without_private_artifacts(&root);
        let plan = executor
            .block_on(adapter.plan_maintenance(local_planning_request(&key, expire)))
            .expect("maintenance plan");
        assert_eq!(inventory_without_private_artifacts(&root), before);
        let outcome = executor
            .block_on(
                adapter.execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context())
                        .expect("execute request"),
                ),
            )
            .expect("maintenance execute");
        let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
            panic!("maintenance must commit")
        };
        if expire {
            assert!(receipt.summary().affected_versions > 0);
        } else {
            assert!(receipt.summary().rewritten_items > 0);
        }
        let payload: serde_json::Value =
            serde_json::from_slice(receipt.provider_payload()).expect("receipt JSON");
        assert_eq!(payload.as_object().expect("receipt object").len(), 7);
        assert_eq!(
            payload["frozen_request_digest_hex"].as_str().unwrap().len(),
            64
        );
        assert_eq!(payload["attempt_id_hex"].as_str().unwrap().len(), 32);
        assert!(payload.get("snapshot_id").is_some());
        inventory_without_private_artifacts(&root);
    }
}

#[test]
fn metadata_maintenance_rejected_payload_leaves_no_artifacts_and_fresh_attempt_accepts_advance() {
    let (executor, _warehouse, adapter, key) = adapter();
    let table = create_local_table(&executor, &adapter);
    let location =
        novarocks_fs::FsLocation::parse(table.metadata().location()).expect("table location");
    let root = std::path::PathBuf::from(location.path());
    for expire in [false, true] {
        let request = local_planning_request(&key, expire);
        let plan = executor
            .block_on(adapter.plan_maintenance(request.clone()))
            .expect("plan");
        let before = inventory_without_private_artifacts(&root);
        let mut payload: IcebergMetadataMaintenancePlanPayloadV1 =
            decode_canonical_json(plan.provider_payload(), "test plan").expect("payload");
        payload.operation_kind = "mismatched-operation".to_string();
        let invalid_plan = ConnectorMetadataMaintenancePlan::try_new(
            &request,
            plan.intent_digest(),
            plan.summary(),
            canonical_json(&payload, "test invalid plan").expect("encode payload"),
        )
        .expect("invalid provider plan");
        let error = executor
            .block_on(
                adapter.execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(invalid_plan, context())
                        .expect("execute request"),
                ),
            )
            .expect_err("provider payload must be validated without a cache");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(inventory_without_private_artifacts(&root), before);

        append_local_file(
            &executor,
            &adapter,
            table.identifier(),
            if expire {
                "expire-conflict"
            } else {
                "rewrite-conflict"
            },
        );
        let fixed_payload = plan.provider_payload().clone();
        let fixed_intent = plan.intent_digest();
        let execute_plan = plan.clone();
        let outcome = executor
            .block_on(
                adapter.execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(execute_plan, context())
                        .expect("execute request"),
                ),
            )
            .expect("fresh policy attempt outcome");
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        assert_eq!(plan.provider_payload(), &fixed_payload);
        assert_eq!(plan.intent_digest(), fixed_intent);
        inventory_without_private_artifacts(&root);
    }
}

#[test]
fn v3_empty_maintenance_is_noop_without_publication_or_artifacts() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (executor, _warehouse, base, key) = adapter();
    let table =
        create_local_table_with_version(&executor, &base, crate::iceberg::spec::FormatVersion::V3);
    let location = novarocks_fs::FsLocation::parse(table.metadata().location()).unwrap();
    let before = inventory_without_private_artifacts(std::path::Path::new(location.path()));
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let actual = recorded_adapter(
        &base,
        Arc::new(move |_, _| {
            seen.fetch_add(1, Ordering::SeqCst);
        }),
    );
    let lease = real_maintenance_lease(actual);
    for expire in [false, true] {
        let plan = executor
            .block_on(lease.plan_maintenance(local_planning_request(&key, expire)))
            .unwrap();
        let outcome = executor
            .block_on(lease.execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context()).unwrap(),
            ))
            .unwrap();
        let ExternalMutationOutcome::KnownCommitted {
            cleanup: _,
            effect: ExternalMutationEffect::NoOp,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
        } = outcome
        else {
            panic!("empty V3 must be a complete no-op")
        };
        assert_eq!(
            receipt.summary(),
            ConnectorMetadataMaintenanceReceiptSummary::default()
        );
        assert_eq!(
            inventory_without_private_artifacts(std::path::Path::new(location.path())),
            before
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

type TableMaintenanceRecorder =
    Arc<dyn Fn(&[crate::iceberg::TableRequirement], &[crate::iceberg::TableUpdate]) + Send + Sync>;

fn recorded_adapter(
    adapter: &IcebergMetadataMaintenanceAdapter,
    recorder: TableMaintenanceRecorder,
) -> IcebergMetadataMaintenanceAdapter {
    let owner = crate::catalog::admission_test_support::all_admitted_recording(
        Arc::clone(adapter.runtime.novarocks_catalog()),
        recorder,
    );
    let runtime = IcebergMetadataContext::with_catalog_for_test(
        adapter.runtime.control_state().clone(),
        adapter.runtime.resources().clone(),
        owner,
    );
    IcebergMetadataMaintenanceAdapter::new(adapter.key.clone(), Arc::new(runtime)).unwrap()
}
fn real_maintenance_lease(
    adapter: IcebergMetadataMaintenanceAdapter,
) -> novarocks_spi::connector::ConnectorMetadataMaintenanceSession {
    let descriptor = adapter.descriptor.clone();
    let epoch = adapter.key.incarnation;
    let metadata = Arc::new(crate::metadata::IcebergMetadata::new(
        descriptor.clone(),
        epoch,
        Arc::clone(&adapter.runtime),
    ));
    novarocks_spi::connector::ConnectorMetadataMaintenanceSession::new(
        descriptor,
        novarocks_spi::connector::ConnectorControlRuntimeId::new(),
        epoch,
        metadata,
        Arc::new(adapter),
        || {},
    )
    .expect("real metadata and maintenance adapter share the exact generation")
}

struct ObservationCpu {
    descriptor: ConnectorInstanceDescriptor,
    epoch: novarocks_spi::connector::ProviderBindingEpoch,
    calls: std::sync::atomic::AtomicUsize,
    first_receipt: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    footer: Mutex<tokio::sync::oneshot::Receiver<()>>,
    closed: std::sync::atomic::AtomicBool,
}
// Component-only CPU executor: runs the real private phase. The independent
// footer verifies Source custody, not the FA Work/physical CPU admission.
impl novarocks_spi::connector::ProviderMetadataCpuPort for ObservationCpu {
    fn submit_metadata_in(
        &self,
        input: &mut Option<novarocks_spi::connector::ProviderMetadataCpuInput>,
    ) -> Result<(), ConnectorError> {
        use std::sync::atomic::Ordering;
        let source = input.as_ref().expect("original phase");
        assert_eq!(source.descriptor(), &self.descriptor);
        assert_eq!(source.incarnation(), self.epoch);
        assert_eq!(
            source.original_source_type_id(),
            std::any::TypeId::of::<
                crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal,
            >()
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        input.take().unwrap().run_original_phase();
        Ok(())
    }
    fn poll_finished(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        let mut receipt = self.first_receipt.lock().unwrap();
        if let Some(receiving) = receipt.as_mut() {
            std::task::ready!(Pin::new(receiving).poll(cx))
                .expect("original first CPU receipt released");
            *receipt = None;
        }
        Poll::Ready(Ok(()))
    }
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        use std::sync::atomic::Ordering;
        if self.closed.load(Ordering::SeqCst) {
            return Poll::Ready(Ok(()));
        }
        std::task::ready!(Pin::new(&mut *self.footer.lock().unwrap()).poll(cx))
            .expect("original footer released");
        self.closed.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        None
    }
}

fn competing_adapter(
    base: &IcebergMetadataMaintenanceAdapter,
) -> Arc<IcebergMetadataMaintenanceAdapter> {
    Arc::new(
        IcebergMetadataMaintenanceAdapter::new(base.key.clone(), Arc::clone(&base.runtime))
            .unwrap(),
    )
}

fn foreign_properties(
    runtime: crate::resources::IcebergCatalogRuntime,
    catalog: Arc<IcebergMetadataMaintenanceAdapter>,
    ident: crate::iceberg::TableIdent,
    properties: HashMap<String, String>,
) {
    runtime
        .block_on(async move {
            let table = crate::catalog::admission_test_support::load_static_fixture_table(
                &catalog.runtime,
                &ident,
            )
            .await;
            let operation = OperationToken::from_mutation(ConnectorMutationOperationId::new());
            let preparation = context();
            let frozen = crate::commit::staging::freeze_existing_change(
                ident,
                "main",
                table.metadata(),
                table.metadata_location().unwrap(),
                RequestShape::MetadataOnly,
                AttemptToken::new(operation, 0),
                PreparedChange {
                    requirements: vec![],
                    updates: vec![crate::iceberg::TableUpdate::SetProperties {
                        updates: properties,
                    }],
                },
                &preparation,
            )
            .unwrap();
            let outcome = crate::catalog::admission_test_support::publish_frozen(
                Arc::clone(catalog.runtime.novarocks_catalog()),
                catalog.descriptor.clone(),
                catalog.key.incarnation,
                frozen,
                CatalogOperation::AlterProperties,
                preparation,
                None,
            )
            .await;
            assert!(
                matches!(
                    outcome,
                    crate::catalog::error::CatalogOutcome::KnownCommitted { .. }
                ),
                "{outcome:?}"
            );
        })
        .unwrap();
}

#[test]
fn bounded_confirmed_conflicts_stop_without_metadata_effect_or_operation_artifact_leaks() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (executor, _warehouse, base, key) = adapter();
    let table = create_local_table(&executor, &base);
    append_local_file(&executor, &base, table.identifier(), "first");
    append_local_file(&executor, &base, table.identifier(), "second");
    let catalog = competing_adapter(&base);
    let runtime = base.runtime.resources().catalog_runtime().clone();
    foreign_properties(
        runtime.clone(),
        catalog.clone(),
        table.identifier().clone(),
        HashMap::from([
            ("commit.retry.num-retries".into(), "2".into()),
            ("commit.retry.min-wait-ms".into(), "1".into()),
            ("commit.retry.max-wait-ms".into(), "1".into()),
        ]),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let ident = table.identifier().clone();
    let adapter = recorded_adapter(
        &base,
        Arc::new(move |_, _| {
            let n = seen.fetch_add(1, Ordering::SeqCst);
            foreign_properties(
                runtime.clone(),
                catalog.clone(),
                ident.clone(),
                HashMap::from([("external-interference".into(), n.to_string())]),
            );
        }),
    );
    let plan = executor
        .block_on(adapter.plan_maintenance(local_planning_request(&key, false)))
        .unwrap();
    let operation = OperationToken::from_mutation(plan.operation_id()).path_component();
    let outcome =
        executor
            .block_on(adapter.execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context()).unwrap(),
            ))
            .unwrap();
    assert!(
        matches!(outcome,ExternalMutationOutcome::KnownUncommitted{failure,..} if failure.kind()==ConnectorMutationFailureKind::Conflict)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let location = novarocks_fs::FsLocation::parse(table.metadata().location()).unwrap();
    assert!(
        inventory_without_private_artifacts(std::path::Path::new(location.path()))
            .iter()
            .all(|path| !path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains(&operation))
    );
}
#[test]
fn terminal_replay_preserves_failed_finalization() {
    let (executor, _warehouse, adapter, key) = adapter();
    let plan = plan(key, ConnectorMutationOperationId::new(), b"table-a");
    let expected_receipt = receipt(&adapter, &plan);
    let expected_failure = ConnectorMutationFailure::new(
        ConnectorMutationFailureKind::Unavailable,
        "necessary owner cache finalization was lost",
    );
    adapter.terminal.lock().expect("terminal").insert(
        plan.operation_id(),
        TerminalRecord {
            plan_digest: plan.plan_digest(),
            outcome: ExternalMutationOutcome::KnownCommitted {
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
                effect: ExternalMutationEffect::Applied,
                receipt: expected_receipt.clone(),
                finalization: ExternalMutationFinalization::Failed(expected_failure.clone()),
            },
        },
    );

    let outcome = executor
        .block_on(
            adapter.execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context())
                    .expect("execute request"),
            ),
        )
        .expect("terminal replay");
    let ExternalMutationOutcome::KnownCommitted {
        receipt,
        finalization: ExternalMutationFinalization::Failed(failure),
        ..
    } = outcome
    else {
        panic!("terminal replay must preserve failed finalization")
    };
    assert_eq!(receipt, expected_receipt);
    assert_eq!(failure.kind(), expected_failure.kind());
    assert_eq!(failure.message(), expected_failure.message());
}

#[test]
fn response_loss_terminal_replay_is_idempotent_and_plan_bound() {
    let (executor, _warehouse, adapter, key) = adapter();
    let table = create_local_table(&executor, &adapter);
    let location = novarocks_fs::FsLocation::parse(table.metadata().location()).unwrap();
    let before = inventory_without_private_artifacts(std::path::Path::new(location.path()));
    let operation_id = ConnectorMutationOperationId::new();
    let original_request = local_planning_request(&key, false);
    let request = ConnectorMetadataMaintenancePlanningRequest::try_new(
        operation_id,
        key.clone(),
        original_request.operation().clone(),
        context(),
    )
    .unwrap();
    let exact_plan = executor
        .block_on(adapter.plan_maintenance(request.clone()))
        .unwrap();
    let evidence = ExternalMutationEvidence::try_new(
        PAYLOAD_VERSION,
        adapter.descriptor.clone(),
        adapter.key.incarnation,
        operation_id,
        exact_plan.operation_kind(),
        Bytes::from_static(b"response-loss-evidence"),
    )
    .expect("evidence");
    adapter.terminal.lock().expect("terminal").insert(
        operation_id,
        TerminalRecord {
            plan_digest: exact_plan.plan_digest(),
            outcome: ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "commit response was lost",
                ),
                evidence: evidence.clone(),
            },
        },
    );

    for _ in 0..2 {
        let outcome = executor
            .block_on(
                adapter.execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(
                        exact_plan.clone(),
                        context(),
                    )
                    .expect("execute request"),
                ),
            )
            .expect("unknown replay");
        let ExternalMutationOutcome::CommitUnknown {
            evidence: replayed, ..
        } = outcome
        else {
            panic!("response-loss replay must remain unknown")
        };
        assert_eq!(replayed.digest(), evidence.digest());
    }

    // This is a fully valid competing provider plan for the same operation,
    // rather than an undecodable test payload which can fail another gate.
    let mut competing_payload: IcebergMetadataMaintenancePlanPayloadV1 =
        decode_canonical_json(exact_plan.provider_payload(), "test fixed intent").unwrap();
    competing_payload.evaluation_time_ms += 1;
    let conflicting = ConnectorMetadataMaintenancePlan::try_new(
        &request,
        intent_digest(&competing_payload).unwrap(),
        exact_plan.summary(),
        canonical_json(&competing_payload, "test competing intent").unwrap(),
    )
    .unwrap();
    adapter
        .payload_for_plan(&conflicting)
        .expect("competing provider plan is valid");
    assert_eq!(conflicting.request_digest(), exact_plan.request_digest());
    assert_ne!(conflicting.intent_digest(), exact_plan.intent_digest());
    assert_ne!(conflicting.plan_digest(), exact_plan.plan_digest());
    let error = executor
        .block_on(
            adapter.execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(conflicting, context())
                    .expect("execute request"),
            ),
        )
        .expect_err("same operation with a different plan must conflict");
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(error.message(), "Maintenance replay changes the fixed plan");
    // A malformed payload without a terminal entry remains a distinct
    // provider-payload validation failure and never claims this operation.
    let malformed = plan(
        key,
        ConnectorMutationOperationId::new(),
        b"invalid-table-handle",
    );
    let malformed_error = executor
        .block_on(adapter.execute(
            ConnectorMetadataMaintenanceExecuteRequest::try_new(malformed, context()).unwrap(),
        ))
        .expect_err("malformed payload must fail before publication");
    assert_eq!(malformed_error.kind(), ConnectorErrorKind::InvalidRequest);
    assert!(
        malformed_error.message().contains("decode"),
        "{malformed_error}"
    );
    assert_eq!(
        inventory_without_private_artifacts(std::path::Path::new(location.path())),
        before
    );
    assert_eq!(
        adapter
            .terminal
            .lock()
            .unwrap()
            .get(&operation_id)
            .unwrap()
            .plan_digest,
        exact_plan.plan_digest()
    );
}

#[test]
fn foreign_generation_is_rejected_before_catalog_access() {
    let (executor, _warehouse, adapter, key) = adapter();
    let foreign_key = ConnectorProviderBindingKey {
        instance_id: key.instance_id,
        incarnation: ProviderBindingEpoch::from_bytes([8; 16]),
    };
    let foreign = plan(
        foreign_key,
        ConnectorMutationOperationId::new(),
        b"foreign-table",
    );
    let error = executor
        .block_on(
            adapter.execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(foreign, context())
                    .expect("execute request"),
            ),
        )
        .expect_err("foreign generation must fail closed");
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert!(error.to_string().contains("exact connector generation"));
}

#[test]
fn provider_descriptor_remains_exact_for_terminal_receipts() {
    let (_executor, _warehouse, adapter, key) = adapter();
    assert_eq!(
        adapter.descriptor.provider_id,
        ConnectorProviderId::parse("iceberg").expect("provider")
    );
    assert_eq!(adapter.descriptor.instance_id, key.instance_id);
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

fn admission_context(catalog_type: &str) -> novarocks_spi::connector::ConnectorRequestContext {
    novarocks_spi::connector::ConnectorRequestContext::try_new(
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
fn planning_refuses_hms_and_background_hadoop_before_maintenance_artifacts() {
    for catalog_type in ["hive", "hadoop"] {
        let (executor, warehouse, runtime, listener) = admission_runtime(catalog_type);
        let key = ConnectorProviderBindingKey {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            incarnation: ProviderBindingEpoch::from_bytes([8; 16]),
        };
        let adapter =
            IcebergMetadataMaintenanceAdapter::new(key.clone(), runtime).expect("adapter");
        for operation in [
            ConnectorMetadataMaintenanceOperation::rewrite_metadata_layout(admission_table(
                key.instance_id.clone(),
            ))
            .expect("rewrite manifests"),
            ConnectorMetadataMaintenanceOperation::expire_table_versions(
                admission_table(key.instance_id.clone()),
                None,
                Some(1),
            )
            .expect("expire snapshots"),
        ] {
            let request = ConnectorMetadataMaintenancePlanningRequest::try_new(
                ConnectorMutationOperationId::new(),
                key.clone(),
                operation,
                admission_context(catalog_type),
            )
            .expect("request");
            let error = executor
                .block_on(adapter.plan_maintenance(request))
                .expect_err("catalog admission must refuse planning");
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
            assert!(error.to_string().contains(if catalog_type == "hive" {
                "read-only compatibility entry"
            } else {
                "background"
            }));
            assert_admission_left_no_io(&warehouse, &listener);
        }
    }
}
