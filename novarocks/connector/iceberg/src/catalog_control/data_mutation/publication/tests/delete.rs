// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;

fn load_delete_target(
    provider: &IcebergMetadata,
    context: ConnectorRequestContext,
) -> novarocks_spi::connector::ConnectorTableMetadata {
    provider
        .load_table(
            novarocks_spi::connector::ConnectorTableRequest {
                table: novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: "db".into(),
                    table: "t".into(),
                },
                resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                context,
            },
            &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                &mut |_| Ok(()),
                &mut |_| Ok(()),
            ),
        )
        .unwrap()
}

fn delete_plan(
    adapter: &IcebergDataMutationAdapter,
    provider: &IcebergMetadata,
) -> ConnectorDataMutationPlan {
    let context = table_context();
    let table = load_delete_target(provider, context.clone());
    adapter
        .plan_mutation(
            ConnectorDataMutationPlanningRequest::try_new(
                ConnectorMutationOperationId::new(),
                adapter.binding_key().clone(),
                ConnectorDataMutationOperation::DeleteByCondition {
                    table: table.table,
                    condition: ConnectorDeleteCondition::AllRows,
                },
                context,
            )
            .unwrap(),
        )
        .unwrap()
}

fn current_delete_test_table(
    executor: &tokio::runtime::Runtime,
    provider: &IcebergMetadata,
) -> crate::iceberg::table::Table {
    executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            provider.runtime(),
            &TableIdent::new(NamespaceIdent::new("db".into()), "t".into()),
        ),
    )
}

fn seed_delete_test_data(
    executor: &tokio::runtime::Runtime,
    provider: &IcebergMetadata,
    ordinal: usize,
    rows: u64,
) {
    use crate::commit::model::AddedContent;
    use crate::iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
    let table = current_delete_test_table(executor, provider);
    let file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!(
            "{}/data/test-{ordinal}.parquet",
            table.metadata().location()
        ))
        .file_format(DataFileFormat::Parquet)
        .record_count(rows)
        .partition(Struct::empty())
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .file_size_in_bytes(8)
        .build()
        .unwrap();
    let content =
        AddedContent::new_logical_data(file, table.metadata().default_partition_spec_id()).unwrap();
    executor
        .block_on(crate::commit::run::append_contents_for_test(
            provider.clone(),
            table,
            vec![content],
            "main".into(),
            BTreeMap::new(),
        ))
        .unwrap();
}

fn with_delete_test_catalog(
    provider: &Arc<IcebergMetadata>,
    catalog: Arc<dyn crate::catalog::NovaRocksCatalog>,
) -> Arc<IcebergMetadata> {
    Arc::new(IcebergMetadata::new(
        provider.descriptor().clone(),
        provider.incarnation(),
        Arc::new(IcebergMetadataContext::with_catalog_for_test(
            provider.runtime().control_state().clone(),
            provider.runtime().resources().clone(),
            catalog,
        )),
    ))
}

fn committed_delete_receipt(
    outcome: ExternalMutationOutcome<ConnectorDataMutationReceipt>,
) -> ConnectorDataMutationReceipt {
    let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
        panic!("Expected committed DELETE: {outcome:?}");
    };
    receipt
}

#[test]
fn metadata_delete_update_count_checks_the_snapshot_summary_formula() {
    let counts = |deleted: Option<u64>, positions: Option<u64>, equalities: Option<u64>| {
        let properties = [
            ("deleted-records", deleted),
            ("removed-position-deletes", positions),
            ("removed-equality-deletes", equalities),
        ]
        .into_iter()
        .filter_map(|(key, value)| Some((key.into(), value?.to_string())))
        .collect();
        deleted_rows_from_summary(&properties).unwrap()
    };
    assert_eq!(counts(Some(19), Some(3), Some(5)), Some(11));
    assert_eq!(counts(Some(5), Some(5), None), Some(0));
    assert_eq!(
        counts(Some(i64::MAX as u64), None, None),
        Some(i64::MAX as u64)
    );
    assert_eq!(counts(None, Some(5), None), None);
    assert_eq!(counts(Some(5), Some(5), Some(3)), None);
    assert_eq!(counts(Some(i64::MAX as u64 + 1), None, None), None);
    assert_eq!(counts(Some(u64::MAX), Some(u64::MAX), Some(u64::MAX)), None);
    assert!(
        deleted_rows_from_summary(&HashMap::from([("deleted-records".into(), "-1".into())]))
            .is_err()
    );
}

#[test]
fn metadata_delete_creates_the_first_snapshot_in_every_supported_format() {
    use crate::catalog_control::data_mutation::tests::exact_provider_with_table;
    use crate::iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Type};

    for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
        let (executor, _warehouse, provider) = exact_provider_with_table(
            version,
            NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)),
            vec![],
            None,
        );
        let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).unwrap();
        let plan = delete_plan(&adapter, &provider);
        let request = ConnectorDataMutationExecuteRequest::try_new(plan, table_context()).unwrap();
        let committed = adapter.execute(request.clone()).unwrap();
        assert_eq!(adapter.execute(request).unwrap(), committed);
        let receipt = committed_delete_receipt(committed);
        let after = current_delete_test_table(&executor, &provider);
        let snapshot = after.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Delete);
        assert_eq!(receipt.deleted_rows(), None);
        assert_eq!(
            receipt.provider_payload(),
            &durable_receipt_payload(snapshot.snapshot_id()).unwrap()
        );
    }
}

#[test]
fn metadata_delete_retries_definite_conflicts_with_a_new_manifest_list() {
    use crate::catalog::admission_test_support::all_admitted_recording_with_snapshot_trace_and_dispatch_fault;
    use crate::catalog_control::data_mutation::tests::exact_provider_with_table;
    use crate::iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Type};

    for retries in [0, 1] {
        let (executor, _warehouse, base) = exact_provider_with_table(
            FormatVersion::V2,
            NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)),
            vec![
                ("commit.retry.num-retries".into(), retries.to_string()),
                ("commit.retry.min-wait-ms".into(), "0".into()),
                ("commit.retry.max-wait-ms".into(), "0".into()),
            ],
            None,
        );
        seed_delete_test_data(&executor, &base, 1, 3);
        let prior = current_delete_test_table(&executor, &base)
            .metadata()
            .current_snapshot_id();
        let dispatched = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&dispatched);
        let catalog = all_admitted_recording_with_snapshot_trace_and_dispatch_fault(
            Arc::clone(base.runtime().novarocks_catalog()),
            Arc::new(move |_, updates| {
                for update in updates {
                    if let TableUpdate::AddSnapshot { snapshot } = update {
                        assert_eq!(snapshot.summary().operation, Operation::Delete);
                        recorded
                            .lock()
                            .unwrap()
                            .push((snapshot.snapshot_id(), snapshot.manifest_list().to_owned()));
                    }
                }
            }),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(Some(
                crate::iceberg::ErrorKind::CatalogCommitConflicts,
            ))),
        );
        let provider = with_delete_test_catalog(&base, catalog);
        let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).unwrap();
        let plan = delete_plan(&adapter, &provider);
        let outcome = adapter
            .execute(ConnectorDataMutationExecuteRequest::try_new(plan, table_context()).unwrap())
            .unwrap();
        let after = current_delete_test_table(&executor, &base);
        let snapshots = dispatched.lock().unwrap();
        if retries == 0 {
            let ExternalMutationOutcome::KnownUncommitted { failure, .. } = outcome else {
                panic!("expected retry exhaustion: {outcome:?}");
            };
            assert_eq!(
                failure.failure_fact().reason(),
                connector_reason::ICEBERG_COMMIT_ERROR
            );
            assert!(
                failure
                    .connector_source()
                    .unwrap()
                    .public_failure_detail()
                    .unwrap()
                    .starts_with("Failed to commit during delete: ")
            );
            assert_eq!(snapshots.len(), 1);
            assert_eq!(after.metadata().current_snapshot_id(), prior);
        } else {
            let receipt = committed_delete_receipt(outcome);
            assert_eq!(snapshots.len(), 2);
            let (first_id, first_list) = &snapshots[0];
            let (winning_id, winning_list) = &snapshots[1];
            assert_eq!(first_id, winning_id);
            assert_ne!(first_list, winning_list);
            let snapshot = after.metadata().current_snapshot().unwrap();
            assert_eq!(snapshot.snapshot_id(), *winning_id);
            assert_eq!(snapshot.manifest_list(), winning_list.as_str());
            assert_eq!(receipt.deleted_rows(), Some(3));
            assert_eq!(
                receipt.provider_payload(),
                &durable_receipt_payload(*winning_id).unwrap()
            );
        }
    }
}

#[test]
fn metadata_delete_adjudicates_the_actual_snapshot_count_after_a_lost_reply() {
    use crate::catalog::admission_test_support::all_admitted_recording_with_lost_reply;

    for (files, expected) in [(vec![3], Some(3)), (vec![i64::MAX as u64, 1], None)] {
        let (executor, _warehouse, base) = exact_provider_with_empty_table();
        let dispatches = Arc::new(AtomicUsize::new(0));
        let recorded = Arc::clone(&dispatches);
        let catalog = all_admitted_recording_with_lost_reply(
            Arc::clone(base.runtime().novarocks_catalog()),
            Arc::new(move |_, _| {
                recorded.fetch_add(1, Ordering::SeqCst);
            }),
        );
        let provider = with_delete_test_catalog(&base, catalog);
        let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).unwrap();
        // E has no files. The real first B includes appends that committed
        // after planning. Each authored file count fits Iceberg's Avro long.
        let plan = delete_plan(&adapter, &provider);
        for (i, rows) in files.into_iter().enumerate() {
            seed_delete_test_data(&executor, &base, i, rows);
        }
        let request =
            ConnectorDataMutationExecuteRequest::try_new(plan.clone(), table_context()).unwrap();
        let ExternalMutationOutcome::CommitUnknown { failure, evidence } =
            adapter.execute(request.clone()).unwrap()
        else {
            panic!("lost reply must preserve the original unknown");
        };
        assert_eq!(
            failure.failure_fact().reason(),
            connector_reason::ICEBERG_COMMIT_STATE_UNKNOWN
        );
        assert!(
            failure
                .connector_source()
                .unwrap()
                .public_failure_detail()
                .unwrap()
                .starts_with("Failed to commit during delete: [CommitUnknown]")
        );
        let after = current_delete_test_table(&executor, &base);
        let snapshot = after.metadata().current_snapshot().unwrap();
        let receipt = committed_delete_receipt(
            adapter
                .reconcile(
                    ConnectorDataMutationReconcileRequest::try_new(
                        &plan,
                        evidence,
                        request.context.clone(),
                    )
                    .unwrap(),
                )
                .unwrap(),
        );
        assert_eq!(receipt.deleted_rows(), expected);
        assert_eq!(
            receipt.provider_payload(),
            &durable_receipt_payload(snapshot.snapshot_id()).unwrap()
        );
        assert_eq!(
            committed_delete_receipt(adapter.execute(request).unwrap()),
            receipt
        );
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn metadata_delete_refuses_a_replaced_target_before_catalog_dispatch() {
    use crate::catalog::admission_test_support::{all_admitted_recording, publish_catalog_fixture};
    use crate::catalog::catalog_request::CatalogPublicationRequest;
    use crate::catalog::{CatalogNamespaceName, CatalogTableName};

    let (executor, _warehouse, base) = exact_provider_with_empty_table();
    let source = current_delete_test_table(&executor, &base);
    let dispatches = Arc::new(AtomicUsize::new(0));
    let recorded = Arc::clone(&dispatches);
    let catalog = all_admitted_recording(
        Arc::clone(base.runtime().novarocks_catalog()),
        Arc::new(move |_, _| {
            recorded.fetch_add(1, Ordering::SeqCst);
        }),
    );
    let provider = with_delete_test_catalog(&base, catalog);
    let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).unwrap();
    let plan = delete_plan(&adapter, &provider);

    let dropped = executor.block_on(publish_catalog_fixture(
        base.runtime(),
        base.incarnation(),
        CatalogPublicationRequest::DropTable {
            table: CatalogTableName::new("db", "t"),
            no_op_if_missing: false,
            purge: false,
            capture: None,
        },
    ));
    assert!(matches!(dropped, CatalogOutcome::KnownCommitted { .. }));
    let recreated = executor.block_on(publish_catalog_fixture(
        base.runtime(),
        base.incarnation(),
        CatalogPublicationRequest::CreateTable {
            namespace: CatalogNamespaceName::new("db"),
            creation: crate::iceberg::TableCreation::builder()
                .name("t".into())
                .schema(source.metadata().current_schema().as_ref().clone())
                .build(),
            no_op_if_exists: false,
        },
    ));
    assert!(matches!(recreated, CatalogOutcome::KnownCommitted { .. }));

    let outcome = adapter
        .execute(ConnectorDataMutationExecuteRequest::try_new(plan, table_context()).unwrap())
        .unwrap();
    let ExternalMutationOutcome::KnownUncommitted { failure, .. } = outcome else {
        panic!("expected UUID validation failure: {outcome:?}");
    };
    assert_eq!(
        failure.failure_fact().reason(),
        connector_reason::ICEBERG_COMMIT_VALIDATION_CONFLICT
    );
    assert!(
        failure
            .connector_source()
            .unwrap()
            .public_failure_detail()
            .unwrap()
            .starts_with("Failed to commit during delete: ")
    );
    assert_eq!(dispatches.load(Ordering::SeqCst), 0);
    let replacement = current_delete_test_table(&executor, &base);
    assert_ne!(source.metadata().uuid(), replacement.metadata().uuid());
    assert!(replacement.metadata().current_snapshot_id().is_none());
}

#[test]
fn metadata_delete_retains_the_predicate_only_for_its_original_request() {
    use crate::catalog::admission_test_support::all_admitted_recording;
    use crate::commit::delete_condition::DeleteCondition;
    use crate::commit::model::AddedContent;
    use crate::iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Datum, Struct};
    use crate::typed_read::IcebergColumnHandle;
    use novarocks_spi::connector::read_stack::{ConnectorValue, Domain, TupleDomain};

    let (executor, _warehouse, base) = exact_provider_with_empty_table();
    let table = current_delete_test_table(&executor, &base);
    let file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!(
            "{}/data/mixed.parquet",
            table.metadata().location()
        ))
        .file_format(DataFileFormat::Parquet)
        .record_count(2)
        .partition(Struct::empty())
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .file_size_in_bytes(8)
        .null_value_counts(HashMap::from([(1, 0)]))
        .value_counts(HashMap::from([(1, 2)]))
        .lower_bounds(HashMap::from([(1, Datum::long(1))]))
        .upper_bounds(HashMap::from([(1, Datum::long(2))]))
        .build()
        .unwrap();
    let content =
        AddedContent::new_logical_data(file, table.metadata().default_partition_spec_id()).unwrap();
    executor
        .block_on(crate::commit::run::append_contents_for_test(
            base.as_ref().clone(),
            table,
            vec![content],
            "main".into(),
            BTreeMap::new(),
        ))
        .unwrap();

    let recorded = Arc::new(AtomicUsize::new(0));
    let dispatches = Arc::clone(&recorded);
    let catalog = all_admitted_recording(
        Arc::clone(base.runtime().novarocks_catalog()),
        Arc::new(move |_, _| {
            dispatches.fetch_add(1, Ordering::SeqCst);
        }),
    );
    let provider = with_delete_test_catalog(&base, catalog);
    let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).unwrap();
    let context = table_context();
    let target = load_delete_target(&provider, context.clone());
    let source = current_delete_test_table(&executor, &base);
    let schema = source.metadata().current_schema();
    let column = IcebergColumnHandle::base_column_of(schema, 1).unwrap();
    let domain = Domain::single_value(ConnectorValue::BigInt(1)).unwrap();
    let condition = DeleteCondition::from_enforced(
        schema,
        &TupleDomain::with_column_domains(BTreeMap::from([(column, domain)])).unwrap(),
    )
    .unwrap();
    let digest = condition.digest().unwrap();
    let fact = Arc::new(IcebergDeleteConditionFact {
        condition,
        table_uuid: source.metadata().uuid(),
        snapshot_id: source.metadata().current_snapshot_id().unwrap(),
        metadata_identity: crate::read_snapshot::metadata_identity(source.metadata()).unwrap(),
        partition_proof_digest: [7; 32],
        digest,
        _retention: None,
    });
    let weak = Arc::downgrade(&fact);
    let payload = ConnectorProcessLocalPayload::new(adapter.key.clone(), fact);
    let plan = adapter
        .plan_mutation(
            ConnectorDataMutationPlanningRequest::try_new_with_process_local(
                ConnectorMutationOperationId::new(),
                adapter.key.clone(),
                ConnectorDataMutationOperation::DeleteByCondition {
                    table: target.table,
                    condition: ConnectorDeleteCondition::SealedPredicate { digest },
                },
                Some(payload.clone()),
                context,
            )
            .unwrap(),
        )
        .unwrap();
    let request =
        ConnectorDataMutationExecuteRequest::try_new(plan.clone(), table_context()).unwrap();
    assert!(matches!(
        adapter.execute(request.clone()).unwrap(),
        ExternalMutationOutcome::KnownUncommitted { failure, .. }
        if failure.kind() == ConnectorMutationFailureKind::InvalidRequest
    ));

    let outcome = adapter
        .execute(request.with_delete_condition(payload).unwrap())
        .unwrap();
    let ExternalMutationOutcome::KnownUncommitted { failure, .. } = outcome else {
        panic!("expected the B file to be a partial match: {outcome:?}");
    };
    assert_eq!(
        failure.failure_fact().reason(),
        connector_reason::ICEBERG_COMMIT_VALIDATION_CONFLICT
    );
    let detail = failure
        .connector_source()
        .unwrap()
        .public_failure_detail()
        .unwrap();
    assert!(detail.starts_with("Failed to commit during delete: "));
    assert!(detail.contains("some, but not all"));
    assert_eq!(recorded.load(Ordering::SeqCst), 0);
    assert!(
        weak.upgrade().is_none(),
        "the generation plan cache must not hold the predicate"
    );
}
