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

#[test]
fn committed_expiry_keeps_cleanup_failure_and_replays_without_republishing() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (executor, _warehouse, base, key) = adapter();
    let table =
        create_local_table_with_version(&executor, &base, crate::iceberg::spec::FormatVersion::V3);
    append_local_file(&executor, &base, table.identifier(), "first");
    append_local_file(&executor, &base, table.identifier(), "second");
    let before = executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            table.identifier(),
        ),
    );
    let head = before.metadata().current_snapshot_id();
    let retired_list = before
        .metadata()
        .snapshots()
        .find(|snapshot| Some(snapshot.snapshot_id()) != head)
        .unwrap()
        .manifest_list()
        .to_owned();
    let location = novarocks_fs::FsLocation::parse(&retired_list).unwrap();
    let blocked = std::path::Path::new(location.path()).to_owned();
    // The retired snapshot's exact manifest list becomes an unreadable
    // directory. REAL reclamation I/O must fail AFTER expiry publication;
    // no cleanup status or catalog verdict is injected.
    std::fs::remove_file(&blocked).unwrap();
    std::fs::create_dir(&blocked).unwrap();
    std::fs::write(blocked.join("retain"), b"unreclaimed").unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let actual = recorded_adapter(
        &base,
        Arc::new(move |_, _| {
            seen.fetch_add(1, Ordering::SeqCst);
        }),
    );
    let lease = real_maintenance_lease(actual);
    let plan = executor
        .block_on(lease.plan_maintenance(local_planning_request(&key, true)))
        .unwrap();
    let execute =
        || ConnectorMetadataMaintenanceExecuteRequest::try_new(plan.clone(), context()).unwrap();
    let mut execution = lease.begin_execute(execute()).unwrap();
    let first = executor.block_on(execution.wait()).unwrap();
    assert_eq!(
        execution.cleanup_responsibility(),
        novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained
    );
    let ExternalMutationOutcome::KnownCommitted {
        effect: ExternalMutationEffect::Applied,
        finalization,
        cleanup,
        ..
    } = &first
    else {
        panic!("metadata expiration publication must be proven")
    };
    assert_eq!(finalization, &ExternalMutationFinalization::Complete);
    assert!(matches!(cleanup, ExternalMutationFinalization::Failed(_)));
    let after = executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            table.identifier(),
        ),
    );
    assert_eq!(after.metadata().current_snapshot_id(), head);
    assert_eq!(after.metadata().snapshots().len(), 1);
    assert_eq!(
        std::fs::read(blocked.join("retain")).unwrap(),
        b"unreclaimed"
    );
    assert_eq!(executor.block_on(lease.execute(execute())).unwrap(), first);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn v3_expiry_publishes_one_source_uuid_and_keeps_exactly_the_original_head() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (executor, _warehouse, base, key) = adapter();
    let table =
        create_local_table_with_version(&executor, &base, crate::iceberg::spec::FormatVersion::V3);
    append_local_file(&executor, &base, table.identifier(), "first");
    append_local_file(&executor, &base, table.identifier(), "second");
    let loaded = executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            table.identifier(),
        ),
    );
    let head = loaded.metadata().current_snapshot_id().unwrap();
    let uuid = loaded.metadata().uuid();
    assert_eq!(loaded.metadata().snapshots().len(), 2);
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let actual = recorded_adapter(
        &base,
        Arc::new(move |requirements, _| {
            assert_eq!(seen.fetch_add(1, Ordering::SeqCst), 0);
            assert_eq!(
                requirements
                    .iter()
                    .filter_map(|r| {
                        if let crate::iceberg::TableRequirement::UuidMatch { uuid } = r {
                            Some(*uuid)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>(),
                vec![uuid]
            );
        }),
    );
    let lease = real_maintenance_lease(actual);
    let plan = executor
        .block_on(lease.plan_maintenance(local_planning_request(&key, true)))
        .unwrap();
    let outcome =
        executor
            .block_on(lease.execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context()).unwrap(),
            ))
            .unwrap();
    let ExternalMutationOutcome::KnownCommitted {
        cleanup: _,
        effect: ExternalMutationEffect::Applied,
        receipt,
        finalization: ExternalMutationFinalization::Complete,
    } = outcome
    else {
        panic!("V3 expiry must commit")
    };
    assert_eq!(receipt.summary().affected_versions, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let after = executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            table.identifier(),
        ),
    );
    assert_eq!(
        after.metadata().format_version(),
        crate::iceberg::spec::FormatVersion::V3
    );
    assert_eq!(after.metadata().current_snapshot_id(), Some(head));
    assert_eq!(
        after
            .metadata()
            .snapshots()
            .map(|snapshot| snapshot.snapshot_id())
            .collect::<Vec<_>>(),
        vec![head]
    );
}
#[test]
fn gc_disabled_real_lease_refuses_before_dispatch_and_preserves_shared_source_rows_and_files() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;
    use parquet::arrow::ArrowWriter;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn rows(table: &crate::iceberg::table::Table, snapshot: Option<i64>) -> Vec<i64> {
        let mut builder = table.scan().select_all();
        if let Some(snapshot) = snapshot {
            builder = builder.snapshot_id(snapshot);
        }
        let scan = builder.build().unwrap();
        let batches = scan
            .to_arrow()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        values
    }

    let (executor, _warehouse, base, key) = adapter();
    let table = create_local_table(&executor, &base);
    // These are readable Parquet objects, not the synthetic bytes used by
    // append_local_file. An independent frozen source reads their rows.
    for value in [11_i64, 22] {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false)
                .with_metadata(HashMap::from([("PARQUET:field_id".into(), "1".into())])),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        executor.block_on(async {
            let current = crate::catalog::admission_test_support::load_static_fixture_table(
                &base.runtime,
                table.identifier(),
            )
            .await;
            let path = format!(
                "{}/data/real-{value}.parquet",
                current.metadata().location()
            );
            let length = bytes.len() as u64;
            current
                .file_io()
                .new_output(&path)
                .unwrap()
                .write(Bytes::from(bytes))
                .await
                .unwrap();
            let file = crate::iceberg::spec::DataFileBuilder::default()
                .content(crate::iceberg::spec::DataContentType::Data)
                .file_path(path)
                .file_format(crate::iceberg::spec::DataFileFormat::Parquet)
                .record_count(1)
                .file_size_in_bytes(length)
                .partition(crate::iceberg::spec::Struct::empty())
                .build()
                .unwrap();
            let content = crate::commit::model::AddedContent::new_logical_data(
                file,
                current.metadata().default_partition_spec_id(),
            )
            .unwrap();
            crate::commit::run::append_contents_for_test(
                crate::metadata::IcebergMetadata::new(
                    base.descriptor.clone(),
                    key.incarnation,
                    Arc::clone(&base.runtime),
                ),
                current,
                vec![content],
                "main".into(),
                BTreeMap::new(),
            )
            .await
            .unwrap();
        });
    }
    let source = executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            table.identifier(),
        ),
    );
    let read_source = || executor.block_on(rows(&source, None));
    assert_eq!(source.metadata().snapshots().len(), 2);
    assert_eq!(read_source(), vec![11, 22]);
    let copy_ident = crate::iceberg::TableIdent::new(
        source.identifier().namespace().clone(),
        "snapshot_copy".into(),
    );
    let copied_snapshot = executor.block_on(async {
        let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
            &base.runtime,
            key.incarnation,
            crate::catalog::catalog_request::CatalogPublicationRequest::CreateTable {
                namespace: crate::catalog::CatalogNamespaceName::new("maintenance_test"),
                creation: crate::iceberg::TableCreation::builder()
                    .name("snapshot_copy".into())
                    .schema(source.metadata().current_schema().as_ref().clone())
                    .format_version(crate::iceberg::spec::FormatVersion::V2)
                    .build(),
                no_op_if_exists: false,
            },
        )
        .await;
        assert!(matches!(
            outcome,
            crate::catalog::error::CatalogOutcome::KnownCommitted { .. }
        ));
        let copy = crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            &copy_ident,
        )
        .await;
        assert_ne!(copy.metadata().uuid(), source.metadata().uuid());
        let list = crate::manifest::load_manifest_list(
            source.metadata().current_snapshot().unwrap(),
            source.file_io(),
            source.metadata(),
        )
        .await
        .unwrap();
        let mut contents = Vec::new();
        for descriptor in list.entries() {
            let manifest = crate::manifest::load_manifest(descriptor, source.file_io())
                .await
                .unwrap();
            for entry in manifest.entries() {
                assert_ne!(entry.status, crate::iceberg::spec::ManifestStatus::Deleted);
                contents.push(
                    crate::commit::model::AddedContent::new_logical_data(
                        entry.data_file().clone(),
                        descriptor.partition_spec_id,
                    )
                    .unwrap(),
                );
            }
        }
        assert_eq!(contents.len(), 2);
        let provider = || {
            crate::metadata::IcebergMetadata::new(
                base.descriptor.clone(),
                key.incarnation,
                Arc::clone(&base.runtime),
            )
        };
        let copied_snapshot = crate::commit::run::append_contents_for_test(
            provider(),
            copy,
            contents,
            "main".into(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
        let copy = crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            &copy_ident,
        )
        .await;
        assert_eq!(rows(&copy, None).await, vec![11, 22]);
        let copied_list = crate::manifest::load_manifest_list(
            copy.metadata().current_snapshot().unwrap(),
            copy.file_io(),
            copy.metadata(),
        )
        .await
        .unwrap();
        let mut shared_paths = Vec::new();
        for descriptor in copied_list.entries() {
            let manifest = crate::manifest::load_manifest(descriptor, copy.file_io())
                .await
                .unwrap();
            shared_paths.extend(
                manifest
                    .entries()
                    .iter()
                    .map(|entry| entry.data_file().file_path().to_owned()),
            );
        }
        shared_paths.sort();
        assert_eq!(
            shared_paths,
            vec![
                format!("{}/data/real-11.parquet", source.metadata().location()),
                format!("{}/data/real-22.parquet", source.metadata().location()),
            ]
        );

        // Overwrite the copy with a new physical object. The old shared
        // objects are now absent from its live head, but serve source.
        let bytes = source
            .file_io()
            .new_input(format!(
                "{}/data/real-11.parquet",
                source.metadata().location()
            ))
            .unwrap()
            .read()
            .await
            .unwrap();
        let replacement_path = format!("{}/data/replacement.parquet", copy.metadata().location());
        let length = bytes.len() as u64;
        copy.file_io()
            .new_output(&replacement_path)
            .unwrap()
            .write(bytes)
            .await
            .unwrap();
        let replacement = crate::iceberg::spec::DataFileBuilder::default()
            .content(crate::iceberg::spec::DataContentType::Data)
            .file_path(replacement_path)
            .file_format(crate::iceberg::spec::DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(length)
            .partition(crate::iceberg::spec::Struct::empty())
            .build()
            .unwrap();
        let preparation = context();
        let operation = IcebergCommitOperation::new(
            OperationToken::from_mutation(ConnectorMutationOperationId::new()),
            copy.metadata().location(),
            base.runtime.resources().planning_binding().clone(),
            preparation.clone(),
            base.runtime.resources().catalog_runtime().clone(),
            OperationLimits::default(),
        )
        .unwrap();
        let attempt = operation.begin_attempt().unwrap();
        let intent = OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: copy_ident.clone(),
                uuid: Some(copy.metadata().uuid()),
            },
            target_ref: "main".into(),
            start: None,
            changes: FileChanges {
                added: vec![
                    crate::commit::model::AddedContent::new_logical_data(
                        replacement,
                        copy.metadata().default_partition_spec_id(),
                    )
                    .unwrap(),
                ],
                removed: Vec::new(),
            },
            dependencies: vec![Dependency::NoRead],
            isolation: IsolationLevel::Serializable,
            shape: RequestShape::SnapshotProducing,
            summary: BTreeMap::new(),
            token: operation.token(),
        })
        .unwrap();
        let mut engine = crate::commit::staging::StagingEngine::begin(
            crate::commit::staging::StagingBase::Existing {
                metadata: copy.metadata().clone(),
                metadata_location: copy.metadata_location().unwrap().into(),
            },
            &intent,
            &attempt,
        )
        .unwrap();
        engine
            .stage(&crate::commit::overwrite::OverwritePreparer)
            .await
            .unwrap();
        let frozen = engine.freeze().unwrap();
        assert!(frozen.updates().iter().any(|update| matches!(update,
            crate::iceberg::TableUpdate::AddSnapshot { snapshot }
            if snapshot.summary().operation == crate::iceberg::spec::Operation::Overwrite
        )));
        let outcome = crate::catalog::admission_test_support::publish_frozen(
            Arc::clone(base.runtime.novarocks_catalog()),
            base.descriptor.clone(),
            key.incarnation,
            frozen,
            CatalogOperation::Overwrite,
            preparation,
            None,
        )
        .await;
        assert!(matches!(
            outcome,
            crate::catalog::error::CatalogOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                finalization: ExternalMutationFinalization::Complete,
                ..
            }
        ));
        copied_snapshot
    });
    foreign_properties(
        base.runtime.resources().catalog_runtime().clone(),
        competing_adapter(&base),
        copy_ident.clone(),
        HashMap::from([("gc.enabled".into(), "false".into())]),
    );
    let target = executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            &copy_ident,
        ),
    );
    assert_ne!(target.metadata().uuid(), source.metadata().uuid());
    assert_eq!(target.metadata().snapshots().len(), 2);
    assert!(target.metadata().snapshot_by_id(copied_snapshot).is_some());
    assert_ne!(
        target.metadata().current_snapshot().unwrap().snapshot_id(),
        copied_snapshot
    );
    assert_eq!(executor.block_on(rows(&target, None)), vec![11]);
    assert_eq!(
        executor.block_on(rows(&target, Some(copied_snapshot))),
        vec![11, 22]
    );
    let metadata_location = target.metadata_location().unwrap().to_owned();
    let metadata_bytes = executor
        .block_on(
            target
                .file_io()
                .new_input(&metadata_location)
                .unwrap()
                .read(),
        )
        .unwrap();
    let location = novarocks_fs::FsLocation::parse(source.metadata().location()).unwrap();
    let copy_location = novarocks_fs::FsLocation::parse(target.metadata().location()).unwrap();
    let inventory = || {
        inventory_without_private_artifacts(std::path::Path::new(location.path()))
            .into_iter()
            .chain(inventory_without_private_artifacts(std::path::Path::new(
                copy_location.path(),
            )))
            .filter(|path| path.is_file())
            .map(|path| {
                let bytes = std::fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect::<BTreeMap<_, _>>()
    };
    let physical_before = inventory();
    let dispatches = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&dispatches);
    let lease = real_maintenance_lease(recorded_adapter(
        &base,
        Arc::new(move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
        }),
    ));
    let handle = ConnectorTableHandle::try_new(key.instance_id.clone(), Bytes::from_static(
        br#"{"namespace":"maintenance_test","table":"snapshot_copy","metadata_location":null,"table_info":null,"metadata_columns":[],"metadata_table_type":null,"prepared_files":[],"explicit_files":null}"#,
    )).unwrap();
    let plan = executor
        .block_on(
            lease.plan_maintenance(
                ConnectorMetadataMaintenancePlanningRequest::try_new(
                    ConnectorMutationOperationId::new(),
                    key.clone(),
                    ConnectorMetadataMaintenanceOperation::expire_table_versions(
                        handle,
                        Some(i64::MAX),
                        Some(1),
                    )
                    .unwrap(),
                    context(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    let fixed_payload = plan.provider_payload().clone();
    let fixed_intent = plan.intent_digest();
    let outcome = executor
        .block_on(lease.execute(
            ConnectorMetadataMaintenanceExecuteRequest::try_new(plan.clone(), context()).unwrap(),
        ))
        .unwrap();
    let ExternalMutationOutcome::KnownUncommitted { failure, cleanup } = outcome else {
        panic!("gc.enabled=false must reject the real maintenance entry before publication")
    };
    assert_eq!(failure.kind(), ConnectorMutationFailureKind::Unsupported);
    assert!(failure.message().contains("gc.enabled is false"));
    assert_eq!(cleanup, ExternalMutationFinalization::Complete);
    assert_eq!(dispatches.load(Ordering::SeqCst), 0);
    assert_eq!(plan.provider_payload(), &fixed_payload);
    assert_eq!(plan.intent_digest(), fixed_intent);
    let current = executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            &copy_ident,
        ),
    );
    assert_eq!(
        current.metadata_location(),
        Some(metadata_location.as_str())
    );
    assert_eq!(current.metadata().uuid(), target.metadata().uuid());
    assert_eq!(current.metadata().refs(), target.metadata().refs());
    assert_eq!(
        executor
            .block_on(
                current
                    .file_io()
                    .new_input(&metadata_location)
                    .unwrap()
                    .read()
            )
            .unwrap(),
        metadata_bytes
    );
    assert_eq!(
        inventory(),
        physical_before,
        "No metadata, manifest, data or planning object may change"
    );
    assert_eq!(
        read_source(),
        vec![11, 22],
        "The independently retained source remains readable"
    );
}

#[test]
fn expire_conflict_reloads_policy_without_accumulating_old_candidates_or_changing_fixed_intent() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (executor, _warehouse, base, key) = adapter();
    let table = create_local_table(&executor, &base);
    append_local_file(&executor, &base, table.identifier(), "first");
    append_local_file(&executor, &base, table.identifier(), "second");
    let source_uuid = table.metadata().uuid();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let catalog = competing_adapter(&base);
    let runtime = base.runtime.resources().catalog_runtime().clone();
    let ident = table.identifier().clone();
    let adapter = recorded_adapter(
        &base,
        Arc::new(move |requirements, updates| {
            assert_eq!(
                requirements
                    .iter()
                    .filter_map(|r| {
                        if let crate::iceberg::TableRequirement::UuidMatch { uuid } = r {
                            Some(*uuid)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>(),
                vec![source_uuid]
            );
            assert!(updates.iter().any(|update|matches!(update,crate::iceberg::TableUpdate::RemoveSnapshots{snapshot_ids} if snapshot_ids.len()==1)));
            assert_eq!(seen.fetch_add(1, Ordering::SeqCst), 0);
            foreign_properties(
                runtime.clone(),
                catalog.clone(),
                ident.clone(),
                HashMap::from([("history.expire.min-snapshots-to-keep".into(), "3".into())]),
            );
        }),
    );
    let lease = real_maintenance_lease(adapter);
    let mut request = local_planning_request(&key, true);
    // Use table defaults so the confirmed-conflict retry observes the new
    // min-keep policy instead of a caller override.
    request = ConnectorMetadataMaintenancePlanningRequest::try_new(
        request.operation_id(),
        key.clone(),
        ConnectorMetadataMaintenanceOperation::expire_table_versions(
            request.operation().table().clone(),
            Some(i64::MAX),
            None,
        )
        .unwrap(),
        context(),
    )
    .unwrap();
    let plan = executor.block_on(lease.plan_maintenance(request)).unwrap();
    let payload = plan.provider_payload().clone();
    let intent = plan.intent_digest();
    let outcome = executor
        .block_on(lease.execute(
            ConnectorMetadataMaintenanceExecuteRequest::try_new(plan.clone(), context()).unwrap(),
        ))
        .unwrap();
    let ExternalMutationOutcome::KnownCommitted {
        effect, receipt, ..
    } = outcome
    else {
        panic!("fresh policy should produce a real no-op")
    };
    assert_eq!(effect, ExternalMutationEffect::NoOp);
    assert_eq!(receipt.summary().affected_versions, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let attempt: serde_json::Value = serde_json::from_slice(receipt.provider_payload()).unwrap();
    assert_eq!(attempt["attempt_ordinal"], 1);
    assert_eq!(plan.provider_payload(), &payload);
    assert_eq!(plan.intent_digest(), intent);
    assert_eq!(receipt.plan_digest(), plan.plan_digest());
    assert_eq!(receipt.intent_digest(), intent);
    let loaded = executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            table.identifier(),
        ),
    );
    assert_eq!(loaded.metadata().snapshots().len(), 2);
}
