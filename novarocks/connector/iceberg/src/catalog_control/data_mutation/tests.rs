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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use novarocks_spi::connector::{
    ConnectorDataMutationExecuteRequest, ConnectorDataMutationPlanningRequest,
    ConnectorDataMutationReconcileRequest, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorMetadata, ConnectorProviderId, ConnectorRequestContext, ConnectorTableHandle,
    ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution, ProviderBindingEpoch,
};

use crate::access_binding::IcebergReadBinding;
use crate::catalog_control::IcebergCatalogControlState;
use crate::iceberg::TableCreation;
use crate::iceberg::spec::{
    FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
    TableMetadataBuilder, Type,
};
use crate::resources::IcebergMetadataResources;

struct FakeBackend {
    lookup: Mutex<MarkerLookup>,
    execute_count: AtomicUsize,
    context_calls: AtomicUsize,
    namespace: String,
    observed_marker: Mutex<Option<(uuid::Uuid, String)>>,
}

impl FakeBackend {
    fn new() -> Self {
        Self {
            lookup: Mutex::new(MarkerLookup::Missing),
            execute_count: AtomicUsize::new(0),
            context_calls: AtomicUsize::new(0),
            namespace: "db".to_string(),
            observed_marker: Mutex::new(None),
        }
    }

    fn matching(&self, snapshot_id: i64) -> MarkerLookup {
        let (table_uuid, marker_value) = self.observed_marker.lock().unwrap().clone().unwrap();
        MarkerLookup::Matching { observation: crate::commit::publication_descriptor::PositiveAttributionObservation::SnapshotMarker {
            table_uuid, snapshot_id, marker_key: MARKER_PROPERTY.to_owned(), marker_value,
        } }
    }

    fn with_namespace(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            ..Self::new()
        }
    }
}

impl IcebergDataMutationBackend for FakeBackend {
    // This backend tests mutation protocol replay independently of catalog policy.
    fn admit(&self, _request: &ConnectorDataMutationPlanningRequest) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn plan(
        &self,
        _request: &ConnectorDataMutationPlanningRequest,
    ) -> Result<
        (
            PlannedIcebergMutation,
            [u8; 32],
            ConnectorDataMutationPlanSummary,
        ),
        ConnectorError,
    > {
        self.context_calls.fetch_add(1, Ordering::SeqCst);
        let source = fake_source_metadata();
        Ok((
            PlannedIcebergMutation::Truncate {
                payload: IcebergDataMutationPlanPayloadV2 {
                    version: PLAN_PAYLOAD_VERSION,
                    namespace: self.namespace.clone(),
                    table: "orders".to_string(),
                    table_uuid: source.uuid().to_string(),
                    target_ref: "main".to_string(),
                    base_snapshot_id: None,
                    base_sequence_number: None,
                    schema_id: source.current_schema_id(),
                    default_spec_id: source.default_partition_spec_id(),
                    metadata_version_digest_hex: "aa".repeat(32),
                },
                source_metadata: source,
            },
            [9; 32],
            ConnectorDataMutationPlanSummary::default(),
        ))
    }

    fn execute(
        &self,
        planned: &PlannedIcebergMutation,
        marker: &IcebergDataMutationMarkerV1,
        recovery: &MutationRecoveryTemplate,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        self.execute_count.fetch_add(1, Ordering::SeqCst);
        self.context_calls.fetch_add(1, Ordering::SeqCst);
        use crate::commit::{
            attempt::observation::{DispatchEvidence, ProtocolObservation, PublicationJournal},
            model::OperationToken,
            publication_descriptor::AttributionPlan,
        };
        let marker_value = String::from_utf8(canonical_json(marker, "marker")?.to_vec()).unwrap();
        let (payload, source_metadata) = planned.truncate_parts().expect("fake truncate plan");
        let table_uuid = source_metadata.uuid();
        *self.observed_marker.lock().unwrap() = Some((table_uuid, marker_value.clone()));
        let journal =
            PublicationJournal::new(OperationToken::from_mutation(recovery.plan.operation_id()));
        let entry = journal
            .prepare_catalog(
                recovery.evidence(RecoveryPublication::MarkerOnly {})?,
                recovery.publication_receipt(42)?,
                vec![format!("{}.{}", payload.namespace, payload.table)],
                b"independent fake mutation dispatch".to_vec(),
                AttributionPlan::Snapshot {
                    table_uuid,
                    snapshot_id: 42,
                    marker_key: MARKER_PROPERTY.to_owned(),
                    marker_value,
                },
                context.clone(),
            )
            .map_err(|error| internal(error.to_string()))?;
        // Exercise the real journal executor with one independent backend
        // reply. This fixture has no catalog transport or second verdict.
        struct UnresolvedDispatch;
        #[async_trait::async_trait]
        impl crate::catalog::publication_dispatch::PreparedCatalogDispatch for UnresolvedDispatch {
            async fn dispatch_once(
                self: Box<Self>,
                _: crate::catalog::publication::EvidenceContext,
            ) -> crate::catalog::publication_dispatch::DispatchCompletion {
                crate::catalog::publication_dispatch::DispatchCompletion::evidence(
                    DispatchEvidence::Unknown {
                        failure: failure(
                            ConnectorMutationFailureKind::Unavailable,
                            "marker response unresolved",
                        ),
                        observation: ProtocolObservation::default(),
                    },
                )
            }
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let issued = runtime
            .block_on(journal.publish_once(
                context.clone(),
                crate::catalog::publication::CatalogEvidencePolicy::default(),
                |_, _, _| async {
                    Ok(
                        crate::catalog::publication_dispatch::PreparedPublication::Mutation {
                            binding: None,
                            dispatch: Box::new(UnresolvedDispatch),
                        },
                    )
                },
            ))
            .map_err(|error| internal(error.to_string()))?;
        assert_eq!(issued, entry);
        Ok(publication::project_external(
            journal.catalog_entry_outcome(entry),
        ))
    }

    fn evidence_policy(&self) -> crate::catalog::publication::CatalogEvidencePolicy {
        crate::catalog::publication::CatalogEvidencePolicy::default()
    }
    fn positive_attribution_finalization(&self) -> ExternalMutationFinalization {
        ExternalMutationFinalization::Complete
    }
    fn finalize_reconciled(
        &self,
        _facts: &DataFinalizationFacts,
        _context: &ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn lookup_marker(
        &self,
        _namespace: &str,
        _table: &str,
        _target_ref: &str,
        _operation_id_hex: &str,
        _identity_digest_hex: &str,
        _context: &ConnectorRequestContext,
    ) -> Result<MarkerLookup, ConnectorError> {
        self.context_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.lookup.lock().expect("lookup").clone())
    }
}

fn fake_source_metadata() -> TableMetadata {
    TableMetadataBuilder::new(
        Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap(),
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "file:///mutation-fixture".into(),
        FormatVersion::V3,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata
}

fn test_context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        1024,
        4096,
    )
    .expect("context")
}

pub(super) fn table_context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        64 * 1024,
        256 * 1024,
    )
    .expect("table context")
}

pub(super) fn exact_provider_with_empty_table() -> (
    tokio::runtime::Runtime,
    tempfile::TempDir,
    Arc<IcebergMetadata>,
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
    let resources = IcebergMetadataResources::new(binding, executor.handle().clone());
    let runtime = Arc::new(
        IcebergMetadataContext::try_new(IcebergCatalogControlState::new(configuration), resources)
            .expect("control runtime"),
    );
    executor.block_on(async {
        use crate::catalog::CatalogNamespaceName;
        use crate::catalog::catalog_request::CatalogPublicationRequest;
        use crate::catalog::error::CatalogOutcome;
        let generation = ProviderBindingEpoch::from_bytes([8; 16]);
        let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
            &runtime,
            generation,
            CatalogPublicationRequest::CreateNamespace {
                namespace: CatalogNamespaceName::new("db"),
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
            .with_fields(vec![
                NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
            &runtime,
            generation,
            CatalogPublicationRequest::CreateTable {
                namespace: CatalogNamespaceName::new("db"),
                creation: TableCreation::builder()
                    .name("t".into())
                    .schema(schema)
                    .format_version(FormatVersion::V2)
                    .build(),
                no_op_if_exists: false,
            },
        )
        .await;
        assert!(
            matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
            "{outcome:?}"
        );
    });
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
        instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
    };
    let provider = Arc::new(IcebergMetadata::new(
        descriptor,
        ProviderBindingEpoch::from_bytes([8; 16]),
        runtime,
    ));
    (executor, warehouse, provider)
}

fn test_adapter(
    backend: Arc<FakeBackend>,
) -> (
    IcebergDataMutationAdapter,
    ConnectorProviderBindingKey,
    ConnectorInstanceId,
) {
    let instance_id = ConnectorInstanceId::parse("ice").expect("instance");
    let key = ConnectorProviderBindingKey {
        instance_id: instance_id.clone(),
        incarnation: ProviderBindingEpoch::from_bytes([3; 16]),
    };
    (
        IcebergDataMutationAdapter::new_with_backend(key.clone(), backend).expect("adapter"),
        key,
        instance_id,
    )
}

fn truncate_request(
    key: ConnectorProviderBindingKey,
    instance_id: ConnectorInstanceId,
    operation_id: ConnectorMutationOperationId,
    target_ref: &str,
) -> ConnectorDataMutationPlanningRequest {
    let handle =
        ConnectorTableHandle::try_new(instance_id, Bytes::from_static(b"table")).expect("handle");
    ConnectorDataMutationPlanningRequest::try_new(
        operation_id,
        key,
        ConnectorDataMutationOperation::truncate(handle, target_ref).expect("operation"),
        test_context(),
    )
    .expect("request")
}

#[test]
fn exact_runtime_truncate_commits_and_replays_without_a_catalog_registry() {
    let (_executor, _warehouse, provider) = exact_provider_with_empty_table();
    let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).expect("adapter");
    let metadata = provider
        .load_table(
            ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("t"),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: table_context(),
            },
            &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                &mut |_| Ok(()),
                &mut |_| Ok(()),
            ),
        )
        .expect("load table");
    let operation_id = ConnectorMutationOperationId::new();
    let planning = ConnectorDataMutationPlanningRequest::try_new(
        operation_id,
        adapter.binding_key().clone(),
        ConnectorDataMutationOperation::truncate(metadata.table, "main")
            .expect("truncate operation"),
        table_context(),
    )
    .expect("planning request");
    let plan = adapter.plan_mutation(planning).expect("plan truncate");
    let request = ConnectorDataMutationExecuteRequest::try_new(plan, table_context())
        .expect("execute request");
    let first = adapter.execute(request.clone()).expect("execute truncate");
    let replay = adapter.execute(request).expect("replay truncate");
    assert!(matches!(
        first,
        ExternalMutationOutcome::KnownCommitted { .. }
    ));
    assert_eq!(first, replay);
}

pub(super) fn write_external_parquet(
    directory: &std::path::Path,
    rows: Vec<i64>,
) -> std::path::PathBuf {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    let path = directory.join("external.parquet");
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("value", DataType::Int64, true)
            .with_metadata(HashMap::from([("PARQUET:field_id".into(), "1".into())])),
    ]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(rows))]).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    path
}

#[test]
fn mutation_recovery_rejects_changed_source_sequence_and_nested_unknown_fields() {
    let backend = Arc::new(FakeBackend::new());
    let (adapter, key, instance) = test_adapter(backend);
    let plan = adapter
        .plan_mutation(truncate_request(
            key,
            instance,
            ConnectorMutationOperationId::new(),
            "main",
        ))
        .unwrap();
    let cached = adapter
        .plans
        .lock()
        .unwrap()
        .get(&plan.operation_id())
        .unwrap()
        .clone();
    let evidence = adapter
        .evidence(
            &plan,
            cached.private.truncate_parts().expect("truncate plan").0,
        )
        .unwrap();
    let mut decoded: IcebergDataMutationEvidenceV3 =
        decode_canonical_json(evidence.provider_payload(), "recovery").unwrap();
    decoded.base_sequence_number = Some(5);
    assert!(validate_recovery_facts(&decoded, plan.operation_id()).is_err());
    let mut value = serde_json::to_value(&decoded).unwrap();
    value["publication"]["cleanup_authority"] = serde_json::json!(true);
    assert!(serde_json::from_value::<IcebergDataMutationEvidenceV3>(value).is_err());
}

#[tokio::test]
async fn mutation_source_freeze_uses_snapshot_sequence_when_another_ref_advances() {
    use crate::commit::fast_append::FastAppendPreparer;
    use crate::commit::overwrite::preparer_tests::Fixture;
    let fixture = Fixture::new();
    let base = fixture.metadata(FormatVersion::V3);
    let main_intent = fixture.intent(&base, "main", vec![]);
    let (main, _) = fixture.stage(base, &main_intent, &FastAppendPreparer).await;
    let initial = source_snapshot(&main, "main").unwrap().unwrap();
    let dev_intent = fixture.intent(&main, "dev", vec![]);
    let (after, _) = fixture.stage(main, &dev_intent, &FastAppendPreparer).await;
    assert!(after.last_sequence_number() > initial.sequence_number);
    assert_eq!(source_snapshot(&after, "main").unwrap(), Some(initial));
    assert_eq!(
        source_snapshot(&after, "dev")
            .unwrap()
            .unwrap()
            .sequence_number,
        after.last_sequence_number()
    );
}

#[test]
fn marker_codec_is_canonical_and_rejects_unknown_fields() {
    let marker = IcebergDataMutationMarkerV1 {
        version: 1,
        identity_digest_hex: "11".repeat(32),
        incarnation_hex: "22".repeat(16),
        operation_id_hex: "33".repeat(16),
        operation_kind: "truncate".to_string(),
        request_digest_hex: "44".repeat(32),
        plan_digest_hex: "55".repeat(32),
        state_digest_hex: "66".repeat(32),
        target_ref: "main".to_string(),
        base_snapshot_id: Some(7),
        file_count: 0,
        row_count: 0,
        total_bytes: 0,
    };
    let encoded = canonical_json(&marker, "marker").expect("encode");
    assert_eq!(
        decode_canonical_json::<IcebergDataMutationMarkerV1>(&encoded, "marker").expect("decode"),
        marker
    );
    let mut value: serde_json::Value = serde_json::from_slice(&encoded).expect("json");
    value["credential"] = serde_json::Value::String("secret".to_string());
    assert!(
        decode_canonical_json::<IcebergDataMutationMarkerV1>(
            &serde_json::to_vec(&value).expect("json"),
            "marker"
        )
        .is_err()
    );
}

#[test]
fn truncate_state_digest_binds_ref_and_base() {
    let mut payload = IcebergDataMutationPlanPayloadV2 {
        version: 1,
        namespace: "db".to_string(),
        table: "orders".to_string(),
        table_uuid: "uuid".to_string(),
        target_ref: "main".to_string(),
        base_snapshot_id: Some(7),
        base_sequence_number: Some(3),
        schema_id: 1,
        default_spec_id: 0,
        metadata_version_digest_hex: "aa".repeat(32),
    };
    let first = truncate_state_digest(&payload);
    payload.target_ref = "dev".to_string();
    assert_ne!(first, truncate_state_digest(&payload));
    payload.target_ref = "main".to_string();
    payload.base_snapshot_id = Some(8);
    assert_ne!(first, truncate_state_digest(&payload));
}

#[test]
fn truncate_evidence_wire_fits_exact_durable_hex_boundary_and_rejects_one_over() {
    fn planned_evidence_wire_len(
        adapter: &IcebergDataMutationAdapter,
        plan: &ConnectorDataMutationPlan,
    ) -> usize {
        let plans = adapter.plans.lock().expect("plans");
        let cached = plans.get(&plan.operation_id()).expect("cached plan");
        adapter
            .evidence(
                plan,
                cached.private.truncate_parts().expect("truncate plan").0,
            )
            .expect("evidence")
            .try_to_wire_v1()
            .expect("wire")
            .len()
    }

    assert_eq!(
        MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES
            .checked_mul(2)
            .expect("hex size"),
        MAX_DURABLE_TRUNCATE_EVIDENCE_HEX_BYTES
    );

    let empty_backend = Arc::new(FakeBackend::with_namespace("n"));
    let (empty_adapter, key, instance_id) = test_adapter(empty_backend);
    let base_plan = empty_adapter
        .plan_mutation(truncate_request(
            key,
            instance_id,
            ConnectorMutationOperationId::from_bytes([11; 16]),
            "main",
        ))
        .expect("base plan");
    let base_wire_len = planned_evidence_wire_len(&empty_adapter, &base_plan);
    let boundary_namespace_len = MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES
        .checked_sub(base_wire_len)
        .and_then(|length| length.checked_add(1))
        .expect("evidence base must fit durable cap");

    let boundary_backend = Arc::new(FakeBackend::with_namespace(
        "n".repeat(boundary_namespace_len),
    ));
    let (boundary_adapter, key, instance_id) = test_adapter(Arc::clone(&boundary_backend));
    let boundary_plan = boundary_adapter
        .plan_mutation(truncate_request(
            key,
            instance_id,
            ConnectorMutationOperationId::from_bytes([12; 16]),
            "main",
        ))
        .expect("evidence exactly at durable cap must plan");
    assert_eq!(
        planned_evidence_wire_len(&boundary_adapter, &boundary_plan),
        MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES
    );
    assert_eq!(boundary_backend.execute_count.load(Ordering::SeqCst), 0);

    let over_backend = Arc::new(FakeBackend::with_namespace(
        "n".repeat(boundary_namespace_len + 1),
    ));
    let (over_adapter, key, instance_id) = test_adapter(Arc::clone(&over_backend));
    let error = over_adapter
        .plan_mutation(truncate_request(
            key,
            instance_id,
            ConnectorMutationOperationId::from_bytes([13; 16]),
            "main",
        ))
        .expect_err("over-budget evidence must fail during planning");
    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    assert!(over_adapter.plans.lock().expect("plans").is_empty());
    assert_eq!(over_backend.execute_count.load(Ordering::SeqCst), 0);
}

#[test]
fn truncate_receipt_provider_payload_has_a_fixed_small_durable_bound() {
    for snapshot_id in [i64::MIN, -1, 0, 1, i64::MAX] {
        let payload = durable_receipt_payload(snapshot_id).expect("receipt payload");
        assert!(payload.len() <= MAX_DURABLE_ICEBERG_TRUNCATE_RECEIPT_PROVIDER_PAYLOAD_BYTES);
    }
}

#[test]
fn operation_replay_is_idempotent_and_conflicting_request_is_rejected() {
    let backend = Arc::new(FakeBackend::new());
    let (adapter, key, instance_id) = test_adapter(backend);
    let operation_id = ConnectorMutationOperationId::from_bytes([8; 16]);
    let request = truncate_request(key.clone(), instance_id.clone(), operation_id, "main");
    let first = adapter.plan_mutation(request.clone()).expect("first plan");
    let replay = adapter.plan_mutation(request).expect("replay plan");
    assert_eq!(first.plan_digest(), replay.plan_digest());
    let conflict = truncate_request(key, instance_id, operation_id, "dev");
    assert!(adapter.plan_mutation(conflict).is_err());
}

#[test]
fn adapter_threads_the_current_context_to_plan_execute_and_reconcile() {
    let backend = Arc::new(FakeBackend::new());
    let (adapter, key, instance_id) = test_adapter(Arc::clone(&backend));
    let plan = adapter
        .plan_mutation(truncate_request(
            key,
            instance_id,
            ConnectorMutationOperationId::from_bytes([14; 16]),
            "main",
        ))
        .expect("plan");
    let ExternalMutationOutcome::CommitUnknown { evidence, .. } = adapter
        .execute(
            ConnectorDataMutationExecuteRequest::try_new(plan.clone(), test_context())
                .expect("execute request"),
        )
        .expect("unknown execution")
    else {
        panic!("expected commit unknown");
    };
    adapter
        .reconcile(
            ConnectorDataMutationReconcileRequest::try_new(&plan, evidence, test_context())
                .expect("reconcile request"),
        )
        .expect("reconcile");
    assert_eq!(backend.context_calls.load(Ordering::SeqCst), 4);
}

#[test]
fn unknown_is_not_reexecuted_and_reconcile_survives_adapter_restart() {
    let backend = Arc::new(FakeBackend::new());
    let (adapter, key, instance_id) = test_adapter(Arc::clone(&backend));
    let operation_id = ConnectorMutationOperationId::from_bytes([9; 16]);
    let plan = adapter
        .plan_mutation(truncate_request(
            key.clone(),
            instance_id,
            operation_id,
            "main",
        ))
        .expect("plan");
    let execute = ConnectorDataMutationExecuteRequest::try_new(plan.clone(), test_context())
        .expect("execute");
    let first = adapter.execute(execute.clone()).expect("unknown");
    let evidence = match first {
        ExternalMutationOutcome::CommitUnknown { evidence, .. } => evidence,
        other => panic!("expected unknown, got {other:?}"),
    };
    assert!(matches!(
        adapter.execute(execute).expect("cached unknown"),
        ExternalMutationOutcome::CommitUnknown { .. }
    ));
    assert_eq!(backend.execute_count.load(Ordering::SeqCst), 1);

    *backend.lookup.lock().expect("lookup") = backend.matching(42);
    let restarted =
        IcebergDataMutationAdapter::new_with_backend(key, backend).expect("restart adapter");
    let reconcile = ConnectorDataMutationReconcileRequest::try_new(&plan, evidence, test_context())
        .expect("reconcile request");
    assert!(matches!(
        restarted.reconcile(reconcile).expect("reconciled"),
        ExternalMutationOutcome::KnownCommitted { receipt, .. }
            if receipt.summary() == ConnectorDataMutationPlanSummary::default()
    ));
}

#[test]
fn unknown_outcome_reconciliation_uses_a_fresh_owner_after_client_cancellation() {
    let backend = Arc::new(FakeBackend::new());
    let (adapter, key, instance_id) = test_adapter(Arc::clone(&backend));
    let plan = adapter
        .plan_mutation(truncate_request(
            key.clone(),
            instance_id,
            ConnectorMutationOperationId::from_bytes([19; 16]),
            "main",
        ))
        .expect("plan");
    let original_cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
    let original_context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        original_cancellation.view(),
        1024,
        4096,
    )
    .expect("original context");
    let ExternalMutationOutcome::CommitUnknown { evidence, .. } = adapter
        .execute(
            ConnectorDataMutationExecuteRequest::try_new(plan.clone(), original_context)
                .expect("execute request"),
        )
        .expect("unknown result")
    else {
        panic!("expected unknown result");
    };
    original_cancellation.request_stop();
    *backend.lookup.lock().expect("lookup") = backend.matching(42);
    let restarted = IcebergDataMutationAdapter::new_with_backend(key, backend.clone())
        .expect("restart adapter");
    let recovery = ConnectorDataMutationReconcileRequest::try_new(&plan, evidence, test_context())
        .expect("independent reconciliation context");
    assert!(matches!(
        restarted.reconcile(recovery).expect("reconcile"),
        ExternalMutationOutcome::KnownCommitted { .. }
    ));
    assert_eq!(backend.execute_count.load(Ordering::SeqCst), 1);
}

#[test]
fn reconcile_marker_matrix_is_typed_and_never_reexecutes() {
    let backend = Arc::new(FakeBackend::new());
    let (adapter, key, instance_id) = test_adapter(Arc::clone(&backend));
    let plan = adapter
        .plan_mutation(truncate_request(
            key.clone(),
            instance_id,
            ConnectorMutationOperationId::from_bytes([10; 16]),
            "main",
        ))
        .expect("plan");
    let execute = ConnectorDataMutationExecuteRequest::try_new(plan.clone(), test_context())
        .expect("execute");
    let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
        adapter.execute(execute).expect("unknown")
    else {
        panic!("expected unknown");
    };

    let reconcile = || {
        ConnectorDataMutationReconcileRequest::try_new(&plan, evidence.clone(), test_context())
            .expect("reconcile request")
    };
    let restarted = IcebergDataMutationAdapter::new_with_backend(
        key.clone(),
        Arc::clone(&backend) as Arc<dyn IcebergDataMutationBackend>,
    )
    .expect("restart adapter");
    assert!(matches!(
        restarted.reconcile(reconcile()).expect("missing marker"),
        ExternalMutationOutcome::CommitUnknown { .. }
    ));

    *backend.lookup.lock().expect("lookup") = MarkerLookup::Conflicting;
    assert!(matches!(
        restarted
            .reconcile(reconcile())
            .expect("conflicting marker"),
        ExternalMutationOutcome::CommitUnknown { failure, .. }
            if failure.kind() == ConnectorMutationFailureKind::Conflict
    ));

    *backend.lookup.lock().expect("lookup") = backend.matching(43);
    assert!(matches!(
        restarted
            .reconcile(reconcile())
            .expect("marker on another snapshot"),
        ExternalMutationOutcome::CommitUnknown { .. }
    ));
    let wire_only =
        ExternalMutationEvidence::try_from_wire_v1(&evidence.try_to_wire_v1().unwrap()).unwrap();
    assert!(matches!(
        restarted
            .reconcile(
                ConnectorDataMutationReconcileRequest::try_new(&plan, wire_only, test_context())
                    .unwrap()
            )
            .unwrap(),
        ExternalMutationOutcome::CommitUnknown { .. }
    ));
    let mut foreign = backend.matching(42);
    if let MarkerLookup::Matching {
        observation:
            crate::commit::publication_descriptor::PositiveAttributionObservation::SnapshotMarker {
                table_uuid,
                ..
            },
    } = &mut foreign
    {
        *table_uuid = uuid::Uuid::new_v4();
    }
    *backend.lookup.lock().unwrap() = foreign;
    assert!(matches!(
        restarted.reconcile(reconcile()).unwrap(),
        ExternalMutationOutcome::CommitUnknown { .. }
    ));
    *backend.lookup.lock().expect("lookup") = backend.matching(42);
    assert!(matches!(
        restarted
            .reconcile(reconcile())
            .expect("exact original request marker"),
        ExternalMutationOutcome::KnownCommitted { .. }
    ));
    assert_eq!(backend.execute_count.load(Ordering::SeqCst), 1);
}

fn admission_runtime(
    catalog_type: &str,
    add_files_enabled: bool,
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
    properties.push((
        "iceberg.add-files-procedure.enabled".into(),
        add_files_enabled.to_string(),
    ));
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
fn planning_refuses_hms_and_background_hadoop_before_backend_io() {
    for catalog_type in ["hive", "hadoop"] {
        let (_executor, warehouse, runtime, listener) = admission_runtime(catalog_type, false);
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let provider = Arc::new(IcebergMetadata::new(
            descriptor,
            ProviderBindingEpoch::from_bytes([8; 16]),
            runtime,
        ));
        let adapter = IcebergDataMutationAdapter::try_new(provider).expect("adapter");
        for operation in [
            ConnectorDataMutationOperation::truncate(
                admission_table(adapter.key.instance_id.clone()),
                "main",
            )
            .expect("truncate"),
            ConnectorDataMutationOperation::register_existing_files(
                admission_table(adapter.key.instance_id.clone()),
                Some(warehouse.path().join("source").display().to_string().into()),
                Some(ConnectorAddFilesFormat::Parquet),
                ConnectorAddFilesRecursion::Fail,
            ),
        ] {
            let request = ConnectorDataMutationPlanningRequest::try_new(
                ConnectorMutationOperationId::new(),
                adapter.key.clone(),
                operation,
                admission_context(catalog_type),
            )
            .expect("request");
            let error = adapter
                .plan_mutation(request)
                .expect_err("catalog admission must refuse planning");
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
            assert!(error.to_string().contains(if catalog_type == "hive" {
                "read-only compatibility entry"
            } else {
                "background"
            }));
            assert!(adapter.plans.lock().expect("plans").is_empty());
            assert_admission_left_no_io(&warehouse, &listener);
        }
    }
}

#[test]
fn cached_plan_cannot_bypass_background_hadoop_admission() {
    let (_executor, _warehouse, provider) = exact_provider_with_empty_table();
    let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).expect("adapter");
    let metadata = provider
        .load_table(
            ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("t"),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: table_context(),
            },
            &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                &mut |_| Ok(()),
                &mut |_| Ok(()),
            ),
        )
        .expect("table");
    let mut request = ConnectorDataMutationPlanningRequest::try_new(
        ConnectorMutationOperationId::new(),
        adapter.key.clone(),
        ConnectorDataMutationOperation::truncate(metadata.table, "main").expect("truncate"),
        table_context(),
    )
    .expect("request");
    adapter
        .plan_mutation(request.clone())
        .expect("statement admission");
    request.context = request
        .context
        .with_initiation(novarocks_spi::connector::ConnectorRequestInitiation::Background);
    let error = adapter
        .plan_mutation(request)
        .expect_err("cached plan must not authorize a background request");
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    assert!(error.to_string().contains("background"));
    assert_eq!(adapter.plans.lock().expect("plans").len(), 1);
}

#[test]
fn registration_version_precedes_readable_format_admission() {
    use crate::iceberg::spec::FormatVersion;
    use ConnectorAddFilesFormat::{Avro, Orc, Parquet};

    for version in [FormatVersion::V1, FormatVersion::V2] {
        validate_registration_target("db", "t", version, Parquet).unwrap();
        for format in [Orc, Avro] {
            let error = validate_registration_target("db", "t", version, format).unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
            assert_eq!(
                error.message(),
                "NovaRocks ADD FILES only reads Parquet data files"
            );
        }
    }
    let error = validate_registration_target("db", "t", FormatVersion::V3, Orc).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    assert_eq!(
        error.message(),
        "ADD_FILES is not supported for Iceberg table format version > 2. Table \"db\".\"t\" format version is 3.",
    );
}

#[test]
fn registration_switch_and_required_arguments_are_admitted_in_order_without_io() {
    use novarocks_failure_contract::FailureProjection;

    for (enabled, location, message, reason) in [
        (
            false,
            None,
            "add_files procedure is disabled",
            connector_reason::ADD_FILES_DISABLED,
        ),
        (
            true,
            None,
            "Required procedure argument 'location' is missing",
            connector_reason::PROCEDURE_ARGUMENT_INVALID,
        ),
        (
            true,
            Some("s3://bucket/source"),
            "Required procedure argument 'format' is missing",
            connector_reason::PROCEDURE_ARGUMENT_INVALID,
        ),
    ] {
        let (_executor, warehouse, runtime, listener) = admission_runtime("hadoop", enabled);
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("ice").unwrap(),
        };
        let provider = Arc::new(IcebergMetadata::new(
            descriptor,
            ProviderBindingEpoch::from_bytes([8; 16]),
            runtime,
        ));
        let adapter = IcebergDataMutationAdapter::try_new(provider).unwrap();
        let request = ConnectorDataMutationPlanningRequest::try_new(
            ConnectorMutationOperationId::new(),
            adapter.key.clone(),
            ConnectorDataMutationOperation::register_existing_files(
                admission_table(adapter.key.instance_id.clone()),
                location.map(Arc::from),
                None,
                ConnectorAddFilesRecursion::Fail,
            ),
            admission_context("hadoop")
                .with_initiation(novarocks_spi::connector::ConnectorRequestInitiation::Statement),
        )
        .unwrap();
        let error = adapter.plan_mutation(request).unwrap_err();
        assert_eq!(error.message(), message);
        assert_eq!(error.failure_fact().reason(), reason);
        assert_admission_left_no_io(&warehouse, &listener);
    }
}
