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
fn rewrite_plan_and_commit_use_refs_while_expiry_uses_all() {
    for expire in [false, true] {
        let (executor, _warehouse, original, key) = adapter();
        let loads = Arc::new(Mutex::new(Vec::new()));
        let owner = crate::catalog::admission_test_support::all_admitted_with_snapshot_trace(
            Arc::clone(original.runtime.novarocks_catalog()),
            Arc::clone(&loads),
        );
        let runtime = Arc::new(IcebergMetadataContext::with_catalog_for_test(
            original.runtime.control_state().clone(),
            original.runtime.resources().clone(),
            owner,
        ));
        let adapter = IcebergMetadataMaintenanceAdapter::new(key.clone(), runtime)
            .expect("traced maintenance adapter");
        let table = create_local_table(&executor, &adapter);
        append_local_file(&executor, &adapter, table.identifier(), "first");
        append_local_file(&executor, &adapter, table.identifier(), "second");
        // A preceding read in this same request scope may have frozen the
        // old metadata. Generation-cache invalidation does not clear that
        // attempt cache; every maintenance effect observation must bypass it.
        let execution_context = context();
        let cached_before = adapter
            .runtime
            .load_table_for_request("maintenance_test", "t", &execution_context)
            .expect("cache the pre-maintenance observation");
        assert_eq!(cached_before.table.metadata().snapshots().count(), 2);
        let previous_head = cached_before.table.metadata().current_snapshot_id();
        loads.lock().expect("load trace").clear();

        let expected = if expire {
            SnapshotLoading::All
        } else {
            SnapshotLoading::Refs
        };
        let plan = executor
            .block_on(adapter.plan_maintenance(local_planning_request(&key, expire)))
            .expect("maintenance plan");
        assert_eq!(*loads.lock().expect("plan loads"), vec![expected]);
        let outcome = executor
            .block_on(
                adapter.execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, execution_context)
                        .expect("execute request"),
                ),
            )
            .expect("maintenance execution");
        let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
            panic!("real maintenance must commit")
        };
        if expire {
            assert_eq!(receipt.summary().affected_versions, 1);
        } else {
            assert_eq!(receipt.summary().rewritten_items, 2);
            assert_eq!(receipt.summary().added_items, 1);
        }
        let observed = loads.lock().expect("execution loads").clone();
        assert!(
            observed.len() >= 3,
            "plan, execution and actual publication must load metadata: {observed:?}"
        );
        assert!(
            observed.iter().all(|mode| *mode == expected),
            "rewrite must never request full history; expiry must never request only refs: {observed:?}"
        );
        let committed = executor.block_on(
            crate::catalog::admission_test_support::load_static_fixture_table(
                &adapter.runtime,
                table.identifier(),
            ),
        );
        let receipt_payload: IcebergMetadataMaintenanceReceiptV1 =
            decode_canonical_json(receipt.provider_payload(), "maintenance receipt")
                .expect("receipt payload");
        assert_eq!(
            receipt_payload.snapshot_id,
            committed.metadata().current_snapshot_id(),
            "the receipt must describe the postcommit observation",
        );
        if expire {
            assert_eq!(committed.metadata().snapshots().count(), 1);
            assert_eq!(committed.metadata().current_snapshot_id(), previous_head);
        } else {
            assert_eq!(committed.metadata().snapshots().count(), 3);
            assert_ne!(committed.metadata().current_snapshot_id(), previous_head);
            assert_eq!(
                committed
                    .metadata()
                    .current_snapshot()
                    .unwrap()
                    .parent_snapshot_id(),
                previous_head,
            );
        }
    }
}
#[test]
fn original_compaction_scalar_retains_real_manifest_source_until_footer() {
    use std::sync::atomic::Ordering;
    let (executor, _warehouse, adapter, key) = adapter();
    let table = create_local_table(&executor, &adapter);
    for file in ["a", "b", "c"] {
        append_local_file(&executor, &adapter, table.identifier(), file);
    }
    let root = std::path::PathBuf::from(
        novarocks_fs::FsLocation::parse(table.metadata().location())
            .unwrap()
            .path(),
    );
    let before = inventory_without_private_artifacts(&root);
    let request = ConnectorMaxCompactableDataFilesRequest::try_new(
        local_planning_request(&key, false)
            .operation()
            .table()
            .clone(),
        context(),
    )
    .unwrap();
    let (release_first, first_receipt) = tokio::sync::oneshot::channel();
    let (release, footer) = tokio::sync::oneshot::channel();
    let cpu = Arc::new(ObservationCpu {
        descriptor: adapter.descriptor.clone(),
        epoch: key.incarnation,
        calls: std::sync::atomic::AtomicUsize::new(0),
        first_receipt: Mutex::new(Some(first_receipt)),
        footer: Mutex::new(footer),
        closed: std::sync::atomic::AtomicBool::new(false),
    });
    let lease = real_maintenance_lease(adapter);
    executor.block_on(async {
        let mut original = lease
            .begin_read_max_compactable_data_files(request, cpu.clone())
            .unwrap();
        // Cancelling a borrowed wait does not drop its original Source.
        let mut borrowed = Box::pin(std::future::poll_fn(|cx| original.poll_result(cx)));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(borrowed.as_mut().poll(&mut cx).is_pending());
        assert_eq!(cpu.calls.load(Ordering::SeqCst), 1);
        drop(borrowed);
        release_first.send(()).unwrap();
        let scalar = std::future::poll_fn(|cx| original.poll_result(cx))
            .await
            .unwrap();
        assert_eq!(scalar.value(), Some(3));
        assert!(
            cpu.calls.load(Ordering::SeqCst) > 2,
            "real table/list/manifest CPU phases"
        );
        assert!(original.poll_closed(&mut cx).is_pending());
        assert!(original.local_failure().is_none());
        let calls = cpu.calls.load(Ordering::SeqCst);
        assert_eq!(
            std::future::poll_fn(|cx| original.poll_result(cx))
                .await
                .unwrap(),
            scalar
        );
        assert_eq!(
            cpu.calls.load(Ordering::SeqCst),
            calls,
            "observation is not replayed"
        );
        release.send(()).unwrap();
        std::future::poll_fn(|cx| original.poll_closed(cx))
            .await
            .unwrap();
        assert!(cpu.closed.load(Ordering::SeqCst));
        let foreign = ConnectorMaxCompactableDataFilesRequest::try_new(
            ConnectorTableHandle::try_new(
                ConnectorInstanceId::parse("foreign-observation").unwrap(),
                Bytes::from_static(b"not decoded"),
            )
            .unwrap(),
            context(),
        )
        .unwrap();
        assert_eq!(
            lease
                .begin_read_max_compactable_data_files(foreign, cpu.clone())
                .err()
                .unwrap()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(cpu.calls.load(Ordering::SeqCst), calls);
    });
    assert_eq!(
        inventory_without_private_artifacts(&root),
        before,
        "read-only observation"
    );
}
#[test]
fn rewrite_append_conflict_reuses_operation_manifest_and_rebuilds_parent_and_exact_receipt() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (executor, _warehouse, base, key) = adapter();
    let table = create_local_table(&executor, &base);
    append_local_file(&executor, &base, table.identifier(), "first");
    append_local_file(&executor, &base, table.identifier(), "second");
    let source_uuid = table.metadata().uuid();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let captured = Arc::new(Mutex::new(Vec::new()));
    let observed = captured.clone();
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
            observed
                .lock()
                .unwrap()
                .push((requirements.to_vec(), updates.to_vec()));
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                let catalog = catalog.clone();
                let ident = ident.clone();
                runtime
                    .block_on(async move {
                        append_fixture_file(&catalog, &ident, "concurrent").await;
                    })
                    .unwrap();
            }
        }),
    );
    let lease = real_maintenance_lease(adapter);
    let plan = executor
        .block_on(lease.plan_maintenance(local_planning_request(&key, false)))
        .unwrap();
    let operation = OperationToken::from_mutation(plan.operation_id()).path_component();
    let outcome = executor
        .block_on(lease.execute(
            ConnectorMetadataMaintenanceExecuteRequest::try_new(plan.clone(), context()).unwrap(),
        ))
        .unwrap();
    let ExternalMutationOutcome::KnownCommitted {
        cleanup: _,
        effect: ExternalMutationEffect::Applied,
        receipt,
        finalization,
    } = outcome
    else {
        panic!("rewrite should commit after the bounded append conflict")
    };
    assert_eq!(finalization, ExternalMutationFinalization::Complete);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let observations = captured.lock().unwrap();
    let parent = |updates: &[crate::iceberg::TableUpdate]| {
        updates
            .iter()
            .find_map(|update| {
                if let crate::iceberg::TableUpdate::AddSnapshot { snapshot } = update {
                    Some(snapshot.parent_snapshot_id())
                } else {
                    None
                }
            })
            .unwrap()
    };
    assert_ne!(parent(&observations[0].1), parent(&observations[1].1));
    let last = &observations[1];
    let expected =
        serde_json::json!({"identifier":table.identifier(),"requirements":last.0,"updates":last.1});
    let digest = Sha256::digest(serde_json::to_vec(&expected).unwrap());
    let actual: serde_json::Value = serde_json::from_slice(receipt.provider_payload()).unwrap();
    assert_eq!(actual["frozen_request_digest_hex"], hex_encode(digest));
    assert_eq!(actual["attempt_ordinal"], 1);
    assert_eq!(receipt.intent_digest(), plan.intent_digest());
    assert_eq!(receipt.plan_digest(), plan.plan_digest());
    let loaded = executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            table.identifier(),
        ),
    );
    assert_eq!(loaded.metadata().snapshots().len(), 4);
    let list = executor
        .block_on(crate::manifest::load_manifest_list(
            loaded.metadata().current_snapshot().unwrap(),
            loaded.file_io(),
            loaded.metadata(),
        ))
        .unwrap();
    assert_eq!(list.entries().len(), 2);
    let operation_manifests = list
        .entries()
        .iter()
        .filter(|m| m.manifest_path.contains(&operation))
        .count();
    assert_eq!(operation_manifests, 1);
    let location = novarocks_fs::FsLocation::parse(loaded.metadata().location()).unwrap();
    let owned = inventory_without_private_artifacts(std::path::Path::new(location.path()))
        .into_iter()
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .contains(&operation)
        })
        .collect::<Vec<_>>();
    let mut expected = list
        .entries()
        .iter()
        .filter(|manifest| manifest.manifest_path.contains(&operation))
        .map(|manifest| {
            let location = novarocks_fs::FsLocation::parse(&manifest.manifest_path).unwrap();
            std::path::PathBuf::from(location.path())
        })
        .collect::<std::collections::BTreeSet<_>>();
    let list_location = novarocks_fs::FsLocation::parse(
        loaded
            .metadata()
            .current_snapshot()
            .unwrap()
            .manifest_list(),
    )
    .unwrap();
    expected.insert(std::path::PathBuf::from(list_location.path()));
    assert_eq!(expected.len(), 2);
    assert_eq!(
        owned.into_iter().collect::<std::collections::BTreeSet<_>>(),
        expected,
        "only the reused manifest and committed manifest list remain"
    );
}
#[test]
fn rewrite_selected_manifest_replacement_replans_to_noop_and_cleans_unadopted_operation_roots() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (executor, _warehouse, base, key) = adapter();
    let table = create_local_table(&executor, &base);
    append_local_file(&executor, &base, table.identifier(), "first");
    append_local_file(&executor, &base, table.identifier(), "second");
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let installed = Arc::new(Mutex::new(None));
    let observed = installed.clone();
    let catalog = competing_adapter(&base);
    let runtime = base.runtime.resources().catalog_runtime().clone();
    let ident = table.identifier().clone();
    let adapter = recorded_adapter(
        &base,
        Arc::new(move |_, updates| {
            assert!(
                updates.iter().any(|update| matches!(
                    update,
                    crate::iceberg::TableUpdate::AddSnapshot { .. }
                ))
            );
            assert_eq!(seen.fetch_add(1, Ordering::SeqCst), 0);
            let catalog = catalog.clone();
            let ident = ident.clone();
            let installed = runtime
                .block_on(async move {
                    let plan = catalog
                        .plan_maintenance(local_planning_request(&catalog.key, false))
                        .await
                        .unwrap();
                    let outcome = catalog
                        .execute(
                            ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
                        panic!("external rewrite must actually publish")
                    };
                    assert_eq!(receipt.summary().rewritten_items, 2);
                    let after = crate::catalog::admission_test_support::load_static_fixture_table(
                        &catalog.runtime,
                        &ident,
                    )
                    .await;
                    let location = after.metadata_location().unwrap().to_owned();
                    let bytes = after
                        .file_io()
                        .new_input(&location)
                        .unwrap()
                        .read()
                        .await
                        .unwrap();
                    (
                        location,
                        bytes.to_vec(),
                        after.metadata().current_snapshot_id().unwrap(),
                    )
                })
                .unwrap();
            *observed.lock().unwrap() = Some(installed);
        }),
    );
    let lease = real_maintenance_lease(adapter);
    let plan = executor
        .block_on(lease.plan_maintenance(local_planning_request(&key, false)))
        .unwrap();
    let payload = plan.provider_payload().clone();
    let intent = plan.intent_digest();
    let operation = OperationToken::from_mutation(plan.operation_id()).path_component();
    let outcome = executor
        .block_on(lease.execute(
            ConnectorMetadataMaintenanceExecuteRequest::try_new(plan.clone(), context()).unwrap(),
        ))
        .unwrap();
    let ExternalMutationOutcome::KnownCommitted {
        cleanup: _,
        effect: ExternalMutationEffect::NoOp,
        receipt,
        finalization,
    } = outcome
    else {
        panic!("fresh single-manifest selection must be NoOp")
    };
    assert_eq!(finalization, ExternalMutationFinalization::Complete);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        receipt.summary(),
        ConnectorMetadataMaintenanceReceiptSummary::default()
    );
    assert_eq!(plan.provider_payload(), &payload);
    assert_eq!(plan.intent_digest(), intent);
    assert_eq!(receipt.intent_digest(), intent);
    assert_eq!(receipt.plan_digest(), plan.plan_digest());
    let (location, bytes, external_head) = installed.lock().unwrap().clone().unwrap();
    let attempt: IcebergMetadataMaintenanceReceiptV1 =
        decode_canonical_json(receipt.provider_payload(), "actual retry receipt").unwrap();
    assert_eq!(attempt.attempt_ordinal, 1);
    assert_eq!(attempt.base_snapshot_id, Some(external_head));
    // Independently encode the receipt's specified domain and length frame
    // around the actual competitor location; this is not an unframed hash.
    let mut framed_location = b"novarocks.iceberg.metadata-maintenance.intent.v2\0".to_vec();
    framed_location.extend_from_slice(&(location.len() as u64).to_be_bytes());
    framed_location.extend_from_slice(location.as_bytes());
    assert_eq!(
        attempt.metadata_location_digest_hex,
        hex_encode(Sha256::digest(&framed_location))
    );
    let loaded = executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            table.identifier(),
        ),
    );
    assert_eq!(loaded.metadata().snapshots().len(), 3);
    assert_eq!(loaded.metadata().current_snapshot_id(), Some(external_head));
    assert_eq!(loaded.metadata_location(), Some(location.as_str()));
    assert_eq!(
        executor
            .block_on(loaded.file_io().new_input(&location).unwrap().read())
            .unwrap()
            .as_ref(),
        bytes.as_slice()
    );
    let list = executor
        .block_on(crate::manifest::load_manifest_list(
            loaded.metadata().current_snapshot().unwrap(),
            loaded.file_io(),
            loaded.metadata(),
        ))
        .unwrap();
    assert_eq!(list.entries().len(), 1);
    let files = executor
        .block_on(crate::manifest::load_manifest(
            &list.entries()[0],
            loaded.file_io(),
        ))
        .unwrap();
    let live = files
        .entries()
        .iter()
        .filter(|entry| entry.status != crate::iceberg::spec::ManifestStatus::Deleted)
        .map(|entry| entry.data_file().file_path().to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        live,
        std::collections::BTreeSet::from([
            format!("{}/data/first.parquet", loaded.metadata().location()),
            format!("{}/data/second.parquet", loaded.metadata().location()),
        ])
    );
    let root = novarocks_fs::FsLocation::parse(loaded.metadata().location()).unwrap();
    assert!(
        inventory_without_private_artifacts(std::path::Path::new(root.path()))
            .iter()
            .all(|path| !path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains(&operation)),
        "the invalidated Operation manifest and unsuccessful Attempt list must be collected"
    );
}

#[test]
fn rewrite_uuid_replacement_stops_before_a_second_dispatch_and_keeps_replacement_bytes() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (executor, _warehouse, base, key) = adapter();
    let table = create_local_table(&executor, &base);
    append_local_file(&executor, &base, table.identifier(), "first");
    append_local_file(&executor, &base, table.identifier(), "second");
    let original_uuid = table.metadata().uuid();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let replacement = Arc::new(Mutex::new(None));
    let observed = replacement.clone();
    let catalog = competing_adapter(&base);
    let runtime = base.runtime.resources().catalog_runtime().clone();
    let ident = table.identifier().clone();
    let adapter = recorded_adapter(
        &base,
        Arc::new(move |requirements, _| {
            assert!(
                requirements.contains(&crate::iceberg::TableRequirement::UuidMatch {
                    uuid: original_uuid,
                })
            );
            assert_eq!(seen.fetch_add(1, Ordering::SeqCst), 0);
            let catalog = catalog.clone();
            let ident = ident.clone();
            let replaced = runtime
                .block_on(async move {
                    use crate::catalog::catalog_request::CatalogPublicationRequest;
                    let before = crate::catalog::admission_test_support::load_static_fixture_table(
                        &catalog.runtime,
                        &ident,
                    )
                    .await;
                    let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                        &catalog.runtime,
                        catalog.key.incarnation,
                        CatalogPublicationRequest::DropTable {
                            table: crate::catalog::CatalogTableName::from_identifier(&ident),
                            no_op_if_missing: false,
                            purge: false,
                            capture: None,
                        },
                    )
                    .await;
                    assert!(
                        matches!(
                            outcome,
                            crate::catalog::error::CatalogOutcome::KnownCommitted { .. }
                        ),
                        "{outcome:?}"
                    );
                    let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                        &catalog.runtime,
                        catalog.key.incarnation,
                        CatalogPublicationRequest::CreateTable {
                            namespace: crate::catalog::CatalogNamespaceName::new(
                                ident.namespace().join("."),
                            ),
                            creation: crate::iceberg::TableCreation::builder()
                                .name(ident.name().to_owned())
                                .schema(before.metadata().current_schema().as_ref().clone())
                                .format_version(before.metadata().format_version())
                                .build(),
                            no_op_if_exists: false,
                        },
                    )
                    .await;
                    assert!(
                        matches!(
                            outcome,
                            crate::catalog::error::CatalogOutcome::KnownCommitted { .. }
                        ),
                        "{outcome:?}"
                    );
                    let after = crate::catalog::admission_test_support::load_static_fixture_table(
                        &catalog.runtime,
                        &ident,
                    )
                    .await;
                    assert_ne!(after.metadata().uuid(), original_uuid);
                    let location = after.metadata_location().unwrap().to_owned();
                    let bytes = after
                        .file_io()
                        .new_input(&location)
                        .unwrap()
                        .read()
                        .await
                        .unwrap();
                    (after.metadata().uuid(), location, bytes.to_vec())
                })
                .unwrap();
            *observed.lock().unwrap() = Some(replaced);
        }),
    );
    let lease = real_maintenance_lease(adapter);
    let plan = executor
        .block_on(lease.plan_maintenance(local_planning_request(&key, false)))
        .unwrap();
    let payload = plan.provider_payload().clone();
    let intent = plan.intent_digest();
    let fixed: IcebergMetadataMaintenancePlanPayloadV1 =
        decode_canonical_json(&payload, "fixed original target").unwrap();
    assert_eq!(fixed.table_uuid, original_uuid.to_string());
    let operation = OperationToken::from_mutation(plan.operation_id()).path_component();
    let outcome = executor
        .block_on(lease.execute(
            ConnectorMetadataMaintenanceExecuteRequest::try_new(plan.clone(), context()).unwrap(),
        ))
        .unwrap();
    assert!(
        matches!(outcome, ExternalMutationOutcome::KnownUncommitted { failure, cleanup }
        if failure.kind() == ConnectorMutationFailureKind::Conflict
            && cleanup == ExternalMutationFinalization::Complete)
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "foreign UUID must stop before a second mutation"
    );
    assert_eq!(plan.provider_payload(), &payload);
    assert_eq!(plan.intent_digest(), intent);
    let (replacement_uuid, location, bytes) = replacement.lock().unwrap().clone().unwrap();
    let after = executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            table.identifier(),
        ),
    );
    assert_eq!(after.metadata().uuid(), replacement_uuid);
    assert!(after.metadata().snapshots().next().is_none());
    assert_eq!(after.metadata_location(), Some(location.as_str()));
    assert_eq!(
        executor
            .block_on(after.file_io().new_input(&location).unwrap().read())
            .unwrap()
            .as_ref(),
        bytes.as_slice()
    );
    let root = novarocks_fs::FsLocation::parse(after.metadata().location()).unwrap();
    assert!(
        inventory_without_private_artifacts(std::path::Path::new(root.path()))
            .iter()
            .all(|path| !path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains(&operation)),
        "terminal UUID replacement must clean only the original operation's private artifacts"
    );
    for name in ["first", "second"] {
        assert_eq!(
            std::fs::read(
                std::path::Path::new(root.path())
                    .join("data")
                    .join(format!("{name}.parquet"))
            )
            .unwrap(),
            b"test data"
        );
    }
}

#[test]
fn rewrite_real_lost_reply_keeps_one_dispatch_and_reconciles_the_original_journal() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (executor, _warehouse, base, key) = adapter();
    let table = create_local_table(&executor, &base);
    append_local_file(&executor, &base, table.identifier(), "first");
    append_local_file(&executor, &base, table.identifier(), "second");
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let frozen = Arc::new(Mutex::new(Vec::new()));
    let observed = frozen.clone();
    let owner = crate::catalog::admission_test_support::all_admitted_recording_with_lost_reply(
        Arc::clone(base.runtime.novarocks_catalog()),
        Arc::new(move |requirements, updates| {
            seen.fetch_add(1, Ordering::SeqCst);
            observed
                .lock()
                .unwrap()
                .push((requirements.to_vec(), updates.to_vec()));
        }),
    );
    let loads = Arc::new(Mutex::new(Vec::new()));
    let owner = crate::catalog::admission_test_support::all_admitted_with_snapshot_trace(
        owner,
        Arc::clone(&loads),
    );
    let runtime = IcebergMetadataContext::with_catalog_for_test(
        base.runtime.control_state().clone(),
        base.runtime.resources().clone(),
        owner,
    );
    let actual = IcebergMetadataMaintenanceAdapter::new(key.clone(), Arc::new(runtime)).unwrap();
    let lease = real_maintenance_lease(actual);
    let plan = executor
        .block_on(lease.plan_maintenance(local_planning_request(&key, false)))
        .unwrap();
    let fixed_payload = plan.provider_payload().clone();
    let fixed_intent = plan.intent_digest();
    let operation = OperationToken::from_mutation(plan.operation_id()).path_component();
    let execute =
        || ConnectorMetadataMaintenanceExecuteRequest::try_new(plan.clone(), context()).unwrap();
    let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
        executor.block_on(lease.execute(execute())).unwrap()
    else {
        panic!("the real Applied reply was lost before its journal observed the proof")
    };
    assert_eq!(calls.load(Ordering::SeqCst), 1, "Unknown must not retry");
    let (journal, entry) = PublicationJournal::<MaintenancePublication>::from_evidence(&evidence)
        .unwrap()
        .expect("the actual original journal is retained");
    let descriptor = journal.entry_descriptor(entry).unwrap();
    assert!(matches!(
        journal.catalog_entry_outcome(entry),
        ExternalMutationOutcome::CommitUnknown { .. }
    ));
    let loaded = executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &base.runtime,
            table.identifier(),
        ),
    );
    assert_eq!(
        loaded.metadata().snapshots().len(),
        3,
        "actual Hadoop metadata changed once"
    );
    let current = loaded.metadata().current_snapshot().unwrap();
    let marker: IcebergMetadataMaintenanceMarkerV1 = decode_canonical_json(
        current.summary().additional_properties[MARKER_PROPERTY].as_bytes(),
        "actual snapshot marker",
    )
    .unwrap();
    assert_eq!(
        marker.operation_id_hex,
        hex_encode(plan.operation_id().to_bytes())
    );
    assert_eq!(marker.intent_digest_hex, hex_encode(fixed_intent));
    let snapshot = current.snapshot_id();
    let location = loaded.metadata_location().unwrap().to_owned();
    let metadata_bytes = executor
        .block_on(loaded.file_io().new_input(&location).unwrap().read())
        .unwrap();
    let root = novarocks_fs::FsLocation::parse(loaded.metadata().location()).unwrap();
    let inventory = inventory_without_private_artifacts(std::path::Path::new(root.path()));
    let private = inventory
        .iter()
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .contains(&operation)
        })
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        private.len(),
        2,
        "Unknown retains the real Operation manifest and Attempt list"
    );
    let list = executor
        .block_on(crate::manifest::load_manifest_list(
            current,
            loaded.file_io(),
            loaded.metadata(),
        ))
        .unwrap();
    assert_eq!(list.entries().len(), 1);
    assert!(list.entries()[0].manifest_path.contains(&operation));
    assert!(current.manifest_list().contains(&operation));
    let captured = frozen.lock().unwrap();
    assert_eq!(captured.len(), 1);
    let sent = serde_json::json!({"identifier":table.identifier(),"requirements":captured[0].0,"updates":captured[0].1});
    let expected_digest = hex_encode(Sha256::digest(serde_json::to_vec(&sent).unwrap()));
    drop(captured);
    let mut committed_receipt = None;
    for replay in 0..2 {
        loads.lock().unwrap().clear();
        let ExternalMutationOutcome::KnownCommitted {
            cleanup: _,
            effect: ExternalMutationEffect::Applied,
            receipt,
            finalization: ExternalMutationFinalization::Failed(failure),
        } = executor.block_on(lease.execute(execute())).unwrap()
        else {
            panic!("read-only marker attribution must refine that original Unknown")
        };
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "reconcile must not republish"
        );
        assert_eq!(
            *loads.lock().unwrap(),
            if replay == 0 {
                vec![SnapshotLoading::All]
            } else {
                vec![]
            },
            "historical attribution requests All once; settled replay performs no new read",
        );
        assert_eq!(plan.provider_payload(), &fixed_payload);
        assert_eq!(plan.intent_digest(), fixed_intent);
        assert_eq!(receipt.plan_digest(), plan.plan_digest());
        assert_eq!(receipt.intent_digest(), fixed_intent);
        let facts: IcebergMetadataMaintenanceReceiptV1 =
            decode_canonical_json(receipt.provider_payload(), "retained actual receipt").unwrap();
        assert_eq!(
            facts.attempt_ordinal, 0,
            "a positive replay is not a fresh attempt"
        );
        assert_eq!(facts.snapshot_id, Some(snapshot));
        assert_eq!(facts.frozen_request_digest_hex, expected_digest);
        assert_eq!(journal.entry_descriptor(entry).unwrap().key, descriptor.key);
        assert!(matches!(
            journal.catalog_entry_outcome(entry),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        // Attribution proves publication, but the original owner cache
        // finalizer was dropped with the reply and was never observed.
        assert!(failure.message().contains("finalization"));
        if let Some(expected) = &committed_receipt {
            assert_eq!(&receipt, expected);
        } else {
            committed_receipt = Some(receipt);
        }
        assert_eq!(
            inventory_without_private_artifacts(std::path::Path::new(root.path())),
            inventory,
            "positive attribution preserves the exact physical roots without a new publication"
        );
        assert_eq!(
            executor
                .block_on(loaded.file_io().new_input(&location).unwrap().read())
                .unwrap(),
            metadata_bytes
        );
    }
    for name in ["first", "second"] {
        assert_eq!(
            std::fs::read(
                std::path::Path::new(root.path())
                    .join("data")
                    .join(format!("{name}.parquet"))
            )
            .unwrap(),
            b"test data"
        );
    }
}
