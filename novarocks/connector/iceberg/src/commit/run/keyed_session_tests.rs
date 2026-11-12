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

use novarocks_spi::connector::{
    ConnectorDocumentVersion, ConnectorSnapshotRecord, KeyedPublication, KeyedPublicationIntent,
};

struct PreparedKeyed {
    declaration: ConnectorDocumentPublicationDeclaration,
    intent: KeyedPublicationIntent,
    base_snapshot_id: Option<i64>,
    records: BTreeMap<String, String>,
    expected_properties: HashMap<String, String>,
}

async fn keyed_fixture() -> Fixture {
    fixture().await
}

async fn prepare_keyed(fixture: &Fixture, payload: &'static [u8]) -> PreparedKeyed {
    let loaded = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await
        .unwrap();
    let metadata = loaded.metadata();
    let uuid = metadata.uuid().to_string();
    let base_snapshot_id = metadata.current_snapshot_id();
    let object = ConnectorTableObjectId::try_new(Bytes::copy_from_slice(uuid.as_bytes())).unwrap();
    let publication_id = LakePublicationId::new_v7();
    let admission = document_storage_lease(fixture)
        .admit_management(
            ConnectorDocumentManagementAdmissionRequest::try_new(
                ConnectorProviderBindingKey {
                    instance_id: fixture.provider.descriptor().instance_id.clone(),
                    incarnation: fixture.provider.incarnation(),
                },
                fixture.catalog_handle.clone(),
                ConnectorMutationOperationId::from_bytes(publication_id.to_bytes()),
                ConnectorTableIdentity {
                    instance_id: fixture.provider.descriptor().instance_id.clone(),
                    namespace: "db".into(),
                    table: "t".into(),
                },
                Some(object.clone()),
                ConnectorDocumentManagementOperation::Publication,
                context(),
            )
            .unwrap(),
        )
        .unwrap();
    let base = ConnectorWriteBaseVersion::try_new(Bytes::from(format!(
        "iceberg/write-base/v1/{uuid}/main/{}",
        crate::commit::write_shared::snapshot_token(base_snapshot_id)
    )))
    .unwrap();
    let declaration = ConnectorDocumentPublicationDeclaration::try_new(
        publication_id,
        admission,
        object,
        base,
        ConnectorManagedPublicationTechnique::Full,
        ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
        None,
        None,
    )
    .unwrap();
    // Provider-neutral opaque payloads. The provider owns framing/closure, not
    // the MV-specific interpretation of these D/L/B-shaped facts.
    let make = |name, version, refs, content: &'static [u8]| {
        ConnectorDocumentVersion::try_new(
            "novarocks.test",
            name,
            ConnectorDocumentFormat::try_new("novarocks.test", name, version).unwrap(),
            refs,
            Bytes::from_static(content),
        )
        .unwrap()
    };
    let d = make("definition", 1, vec![], b"definition");
    let l = make(
        "interpretation",
        2,
        vec![("definition".into(), d.identity())],
        b"layout",
    );
    let b = make(
        "binding",
        1,
        vec![
            ("definition".into(), d.identity()),
            ("interpretation".into(), l.identity()),
        ],
        b"binding",
    );
    let encoded_payload = match payload {
        b"result" => "cmVzdWx0",
        b"next" => "bmV4dA",
        _ => panic!("add a literal expected payload, not an encoder oracle"),
    };
    let records = BTreeMap::from([(
        "novarocks.doc.v2.record.novarocks.test~publication".into(),
        format!("2.{}.{encoded_payload}", b.identity().to_hex()),
    )]);
    // Independent literal JSON field order and base64url payloads. These
    // expectations do not call the product envelope encoder or property parser.
    let version_json = |name: &str, version: u32, refs: &str, content: &str, id: String| {
        format!(
            r#"{{"v":2,"owner":"novarocks.test","name":"{name}","format":{{"owner":"novarocks.test","name":"{name}","version":{version}}},"refs":{refs},"content":"{content}","id":"{id}"}}"#
        )
    };
    let d_id = d.identity().to_hex();
    let l_id = l.identity().to_hex();
    let b_id = b.identity().to_hex();
    let expected_properties = HashMap::from([
        (
            format!("novarocks.doc.v2.rev.{d_id}"),
            version_json("definition", 1, "[]", "ZGVmaW5pdGlvbg", d_id.clone()),
        ),
        (
            format!("novarocks.doc.v2.rev.{l_id}"),
            version_json(
                "interpretation",
                2,
                &format!(r#"[{{"rel":"definition","id":"{d_id}"}}]"#),
                "bGF5b3V0",
                l_id.clone(),
            ),
        ),
        (
            format!("novarocks.doc.v2.rev.{b_id}"),
            version_json(
                "binding",
                1,
                &format!(
                    r#"[{{"rel":"definition","id":"{d_id}"}},{{"rel":"interpretation","id":"{l_id}"}}]"#
                ),
                "YmluZGluZw",
                b_id.clone(),
            ),
        ),
        ("novarocks.doc.v2.root.novarocks.test".into(), b_id),
    ]);
    let documents = KeyedPublication {
        records: vec![ConnectorSnapshotRecord {
            owner: "novarocks.test".into(),
            name: "publication".into(),
            binding: Some(b.identity()),
            payload: Bytes::from_static(payload),
        }],
        versions: vec![d, l, b.clone()],
        roots: BTreeMap::from([("novarocks.test".into(), b.identity())]),
        markers_set: BTreeMap::new(),
        markers_remove: Default::default(),
    };
    PreparedKeyed {
        intent: KeyedPublicationIntent {
            declaration: declaration.clone(),
            documents,
        },
        declaration,
        base_snapshot_id,
        records,
        expected_properties,
    }
}

fn keyed_begin(prepared: &PreparedKeyed) -> ConnectorWriteBeginRequest {
    ConnectorWriteBeginRequest {
        table: Arc::from("db.t"),
        target_ref: ConnectorWriteTargetRef::main(),
        intent: ConnectorWriteIntent::Overwrite,
        purpose: ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
        input: data_input(),
        base: Some(prepared.declaration.expected_base().clone()),
        flavor: ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
            declaration: prepared.declaration.clone(),
            shape: ConnectorManagedPublicationShape::Data,
        },
        context: context(),
    }
}

fn empty_keyed_finish<'a>(
    plan: &'a novarocks_spi::connector::write_stack::session::ConnectorWriteSessionPlan,
    prepared: &PreparedKeyed,
) -> ConnectorWriteFinishRequest<'a> {
    ConnectorWriteFinishRequest {
        commit: plan.commit_session(),
        prepared: ConnectorPreparedWriteSet::try_new(0, vec![], &plan.expected_targets()).unwrap(),
        statistics: vec![],
        publication: ConnectorWriteFinishPublication::KeyedDocuments(prepared.intent.clone()),
        context: context(),
    }
}

fn expected_record_digest(records: &BTreeMap<String, String>) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for (key, value) in records {
        for bytes in [key.as_bytes(), value.as_bytes()] {
            hash.update((bytes.len() as u32).to_be_bytes());
            hash.update(bytes);
        }
    }
    hash.finalize().into()
}

fn assert_keyed_evidence(
    fixture: &Fixture,
    plan: &novarocks_spi::connector::write_stack::session::ConnectorWriteSessionPlan,
    prepared: &PreparedKeyed,
) -> serde_json::Value {
    let evidence = iceberg_handle(fixture, plan).recovery_evidence().unwrap();
    assert_eq!(evidence.schema_version(), 4);
    let payload =
        crate::commit::write_stack::control::decode_write_recovery_facts_for_test(&evidence)
            .unwrap();
    assert_eq!(payload["version"], serde_json::json!(4));
    assert_eq!(
        payload["document_record_digest"],
        serde_json::json!(expected_record_digest(&prepared.records))
    );
    payload
}

fn actual_snapshot_updates(fixture: &Fixture) -> Vec<crate::iceberg::spec::Snapshot> {
    fixture
        .catalog
        .raw_updates()
        .iter()
        .flatten()
        .filter_map(|update| match update {
            TableUpdate::AddSnapshot { snapshot } => Some(snapshot.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn real_keyed_empty_full_begin_finish_reasserts_closure_with_v4_evidence() {
    let fixture = keyed_fixture().await;
    // Both no-parent and existing-empty-parent Full publications have no file
    // delta and must append. Logical rows are not used to choose the operation.
    for expected_parent in [None, Some(())] {
        let prepared = prepare_keyed(
            &fixture,
            if expected_parent.is_none() {
                b"result"
            } else {
                b"next"
            },
        )
        .await;
        assert_eq!(
            prepared.base_snapshot_id.is_some(),
            expected_parent.is_some()
        );
        fixture.catalog.clear();
        fixture.load_modes.lock().unwrap().clear(); // passive owner trace extension
        let control = write_control(&fixture);
        let plan = control.begin_write(keyed_begin(&prepared)).unwrap();
        let result = control
            .finish_write(empty_keyed_finish(&plan, &prepared))
            .unwrap();
        let ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization: ExternalMutationFinalization::Complete,
            ..
        } = result
        else {
            panic!("keyed finish must commit")
        };
        let id = receipt
            .committed_version()
            .and_then(|v| v.snapshot_id())
            .unwrap();
        let snapshots = actual_snapshot_updates(&fixture);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].snapshot_id(), id);
        assert_eq!(snapshots[0].parent_snapshot_id(), prepared.base_snapshot_id);
        assert_eq!(
            snapshots[0].summary().operation,
            crate::iceberg::spec::Operation::Append
        );
        for (key, value) in &prepared.records {
            assert_eq!(
                snapshots[0].summary().additional_properties.get(key),
                Some(value)
            );
        }
        let commits = fixture.catalog.commits();
        assert_eq!(commits.len(), 1);
        assert_eq!(
            commits[0].uuid_requirements,
            vec![fixture.table.metadata().uuid()]
        );
        assert_eq!(
            commits[0].ref_requirements,
            vec![("main".into(), prepared.base_snapshot_id)]
        );
        assert_eq!(commits[0].ref_updates, vec![("main".into(), id)]);
        let updates = fixture.catalog.raw_updates();
        let properties: HashMap<_, _> = updates
            .iter()
            .flatten()
            .filter_map(|u| match u {
                TableUpdate::SetProperties { updates } => Some(updates),
                _ => None,
            })
            .flat_map(|p| p.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(properties, prepared.expected_properties);
        assert!(
            snapshots[0]
                .summary()
                .additional_properties
                .keys()
                .all(|k| !k.starts_with("novarocks.managed.") && k != "novarocks.documents.v1")
        );
        let payload = assert_keyed_evidence(&fixture, &plan, &prepared);
        assert_eq!(payload["attempt_owned"].as_array().unwrap().len(), 1);
        assert_eq!(payload["published_snapshot_id"], serde_json::json!(id));
        let modes = fixture.load_modes.lock().unwrap();
        assert!(!modes.is_empty());
        assert!(
            modes
                .iter()
                .all(|m| *m == crate::catalog::SnapshotLoading::Refs)
        );
    }
}

#[tokio::test]
async fn keyed_full_zero_output_that_removes_live_files_is_delete() {
    let fixture = keyed_fixture().await;
    append_snapshot_for_test(
        fixture.provider.clone(),
        fixture.table.clone(),
        vec![written_file(&fixture.table, "keyed-old-data")],
        "main".into(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    let prepared = prepare_keyed(&fixture, b"result").await;
    fixture.catalog.clear();
    let control = write_control(&fixture);
    let plan = control.begin_write(keyed_begin(&prepared)).unwrap();
    assert!(matches!(
        control
            .finish_write(empty_keyed_finish(&plan, &prepared))
            .unwrap(),
        ExternalMutationOutcome::KnownCommitted { .. }
    ));
    assert_eq!(
        actual_snapshot_updates(&fixture)[0].summary().operation,
        crate::iceberg::spec::Operation::Delete
    );
    let committed = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await
        .unwrap();
    assert!(live_data_paths(&fixture, &committed).await.is_empty());
    assert_eq!(fixture.catalog.commits().len(), 1);
}

// Stage a real local Parquet object through the writer sealed by this session,
// then hand its original creation receipt to the publication owner.
async fn keyed_data_finish<'a>(
    fixture: &Fixture,
    plan: &'a novarocks_spi::connector::write_stack::session::ConnectorWriteSessionPlan,
    prepared: &PreparedKeyed,
) -> (ConnectorWriteFinishRequest<'a>, String) {
    use crate::commit::write_stack::execution::IcebergWriteStackExecution;
    use crate::commit::write_stack::test_support::received_write_proof;
    use novarocks_spi::connector::write_stack::{
        ConnectorOpenWriterRequest, ConnectorWriteExecution, ConnectorWriterPhysicalContext,
    };

    let [target] = plan.targets() else {
        panic!("keyed data publication must seal one writer target");
    };
    let schema = Arc::new(arrow::datatypes::Schema::new(
        target
            .input()
            .fields_iter()
            .map(|binding| binding.field().clone())
            .collect::<Vec<_>>(),
    ));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(vec![7]))],
    )
    .unwrap();
    let adapter = crate::commit::write_stack::runtime::build_write_adapter(
        fixture.provider.descriptor().clone(),
        fixture.catalog_handle.clone(),
    );
    let execution = IcebergWriteStackExecution::new(
        fixture.catalog_handle.clone(),
        adapter.clone(),
        fixture
            .provider
            .runtime()
            .resources()
            .planning_binding()
            .clone(),
    );
    let physical = ConnectorWriterPhysicalContext::new(
        *Uuid::now_v7().as_bytes(),
        1,
        [2; 16],
        0,
        target.ordinal().get(),
    );
    let mut writer = execution
        .open_writer(ConnectorOpenWriterRequest {
            resources: None,
            handle: target.handle().clone(),
            target: target.ordinal(),
            expected_schema: schema,
            physical,
            context: context(),
        })
        .await
        .unwrap();
    writer.append(batch).await.unwrap();
    let fragments = writer.finish().await.unwrap();
    let [fragment] = fragments.as_slice() else {
        panic!("one keyed data row must produce one commit fragment");
    };
    let path = adapter
        .commit_fragment(fragment)
        .unwrap()
        .new_object_path()
        .unwrap()
        .to_owned();
    let received = ConnectorPreparedWriteSet::try_new(
        1,
        fragments
            .into_iter()
            .map(|fragment| (target.ordinal(), fragment))
            .collect(),
        &plan.expected_targets(),
    )
    .unwrap()
    .with_received_production(received_write_proof(target.ordinal(), physical));
    (
        ConnectorWriteFinishRequest {
            commit: plan.commit_session(),
            prepared: received,
            statistics: vec![],
            publication: ConnectorWriteFinishPublication::KeyedDocuments(prepared.intent.clone()),
            context: context(),
        },
        path,
    )
}

#[tokio::test]
async fn keyed_unknown_retains_session_data_and_manifest_without_redispatch() {
    let fixture = keyed_fixture().await;
    let prepared = prepare_keyed(&fixture, b"result").await;
    let control = write_control(&fixture);
    let plan = control.begin_write(keyed_begin(&prepared)).unwrap();
    let (request, data) = keyed_data_finish(&fixture, &plan, &prepared).await;
    fixture.catalog.clear();
    fixture
        .catalog
        .fail_next(crate::iceberg::ErrorKind::Unexpected);
    let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
        control.finish_write(request).unwrap()
    else {
        panic!("ambiguous dispatch must stay unknown")
    };
    let payload = assert_keyed_evidence(&fixture, &plan, &prepared);
    let paths: Vec<_> = payload["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["path"].as_str().unwrap().to_string())
        .collect();
    assert!(paths.contains(&data));
    assert!(paths.len() > 1);
    let session_indices = payload["session_references"].as_array().unwrap();
    assert_eq!(session_indices.len(), 1);
    let data_index = session_indices[0].as_u64().unwrap() as usize;
    assert_eq!(
        payload["artifacts"][data_index]["path"],
        serde_json::json!(data)
    );
    assert_eq!(
        payload["artifacts"][data_index]["class"],
        serde_json::json!("session_data")
    );
    for path in &paths {
        assert!(std::path::Path::new(path.strip_prefix("file://").unwrap()).exists());
    }
    let lists = actual_snapshot_updates(&fixture);
    assert_eq!(lists.len(), 1);
    assert!(paths.contains(&lists[0].manifest_list().to_string()));
    assert_eq!(fixture.catalog.commits().len(), 1);
    assert!(matches!(
        control
            .reconcile_write(ConnectorWriteSessionReconcileRequest {
                commit: plan.commit_session(),
                evidence,
                context: context()
            })
            .unwrap(),
        ExternalMutationOutcome::CommitUnknown { .. }
    ));
    assert_eq!(fixture.catalog.commits().len(), 1);
    for path in &paths {
        assert!(std::path::Path::new(path.strip_prefix("file://").unwrap()).exists());
    }
}

#[tokio::test]
async fn keyed_conflict_cleans_owned_files_without_another_prepare() {
    let fixture = keyed_fixture().await;
    let prepared = prepare_keyed(&fixture, b"result").await;
    let control = write_control(&fixture);
    let plan = control.begin_write(keyed_begin(&prepared)).unwrap();
    let (request, data) = keyed_data_finish(&fixture, &plan, &prepared).await;
    fixture.catalog.clear();
    fixture
        .catalog
        .fail_next(crate::iceberg::ErrorKind::CatalogCommitConflicts);
    let ExternalMutationOutcome::KnownUncommitted {
        failure,
        cleanup: ExternalMutationFinalization::Complete,
    } = control.finish_write(request).unwrap()
    else {
        panic!("definite conflict must reject")
    };
    assert_eq!(
        failure.kind(),
        novarocks_spi::connector::ConnectorMutationFailureKind::Conflict
    );
    assert_eq!(fixture.catalog.commits().len(), 1);
    let snapshot = actual_snapshot_updates(&fixture);
    assert_eq!(snapshot.len(), 1);
    assert!(!std::path::Path::new(data.strip_prefix("file://").unwrap()).exists());
    assert!(
        !std::path::Path::new(snapshot[0].manifest_list().strip_prefix("file://").unwrap())
            .exists()
    );
    assert!(
        fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .unwrap()
            .metadata()
            .current_snapshot_id()
            .is_none()
    );
}

// Preserve the two legacy session tests' unique branch/repartition facts while
// using the existing independent record and closure expectations.
async fn prepare_keyed_variant(
    fixture: &Fixture,
    technique: ConnectorManagedPublicationTechnique,
    repartition: bool,
) -> PreparedKeyed {
    let mut prepared = prepare_keyed(fixture, b"result").await;
    let (replacement, expected_partitioning) = if repartition {
        let loaded = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .unwrap();
        let metadata = loaded.metadata();
        assert!(metadata.default_partition_spec().is_unpartitioned());
        let prior = ConnectorManagedPartitionSpecObservation::try_from_fields(
            metadata.default_partition_spec_id(),
            &[],
        )
        .unwrap();
        let replacement = ConnectorManagedPartitionSpecReplacement::try_new(
            ConnectorWriteOperationId::from_bytes(prepared.declaration.publication_id().to_bytes()),
            prior,
            vec![
                ConnectorManagedPartitionField::try_new(
                    1,
                    0,
                    ConnectorManagedPartitionTransform::Identity,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let expected = crate::commit::write_stack::repartition::preview_managed_repartition(
            metadata,
            &replacement,
        )
        .unwrap()
        .committed()
        .clone();
        assert_ne!(expected.spec_id(), metadata.default_partition_spec_id());
        assert_eq!(expected.fields().len(), 1);
        assert_eq!(expected.fields()[0].source_field_id(), 1);
        assert_eq!(expected.fields()[0].source_column_name(), "id");
        assert_eq!(
            expected.fields()[0].transform(),
            ConnectorManagedPartitionTransform::Identity
        );
        (Some(replacement), Some(expected))
    } else {
        (None, None)
    };
    let declaration = ConnectorDocumentPublicationDeclaration::try_new(
        prepared.declaration.publication_id(),
        prepared.declaration.admission().clone(),
        prepared.declaration.target_object_id().clone(),
        prepared.declaration.expected_base().clone(),
        technique,
        ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
        replacement,
        expected_partitioning,
    )
    .unwrap();
    prepared.intent.declaration = declaration.clone();
    prepared.declaration = declaration;
    prepared
}

fn keyed_begin_shape(
    prepared: &PreparedKeyed,
    shape: ConnectorManagedPublicationShape,
) -> ConnectorWriteBeginRequest {
    let mut request = keyed_begin(prepared);
    request.intent = match (prepared.declaration.technique(), shape) {
        (ConnectorManagedPublicationTechnique::Full, _) => ConnectorWriteIntent::Overwrite,
        (
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationShape::RowMutation,
        ) => ConnectorWriteIntent::RowDelta,
        (ConnectorManagedPublicationTechnique::Incremental, _)
        | (ConnectorManagedPublicationTechnique::MetadataOnly, _) => ConnectorWriteIntent::Append,
    };
    request.input = match shape {
        ConnectorManagedPublicationShape::RowMutation => row_mutation_input(),
        ConnectorManagedPublicationShape::Data
        | ConnectorManagedPublicationShape::InsertOnlyChangeStream => data_input(),
    };
    request.flavor = ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
        declaration: prepared.declaration.clone(),
        shape,
    };
    request
}

async fn assert_keyed_branch_publication(
    fixture: &Fixture,
    prepared: &PreparedKeyed,
    snapshot_id: i64,
    expected_updates: &[&'static str],
) -> Table {
    assert_exact_target_mutation(
        fixture,
        prepared.base_snapshot_id,
        expected_updates,
        snapshot_id,
    );
    let commits = fixture.catalog.commits();
    for requirement in ["assert-current-schema-id", "assert-default-spec-id"] {
        assert!(commits[0].requirement_kinds.contains(&requirement));
    }
    let snapshots = actual_snapshot_updates(fixture);
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].parent_snapshot_id(), prepared.base_snapshot_id);
    let actual_records: BTreeMap<_, _> = snapshots[0]
        .summary()
        .additional_properties
        .iter()
        .filter(|(key, _)| key.starts_with("novarocks.doc.v2.record."))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(actual_records, prepared.records);
    let updates = fixture.catalog.raw_updates();
    let actual_private: HashMap<_, _> = updates
        .iter()
        .flatten()
        .filter_map(|u| match u {
            TableUpdate::SetProperties { updates } => Some(updates),
            _ => None,
        })
        .flat_map(|p| p.iter())
        .filter(|(key, _)| key.starts_with("novarocks.doc."))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(actual_private, prepared.expected_properties);
    let table = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await
        .unwrap();
    let committed_private: HashMap<_, _> = table
        .metadata()
        .properties()
        .iter()
        .filter(|(key, _)| key.starts_with("novarocks.doc."))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(committed_private, prepared.expected_properties);
    table
}

#[tokio::test]
async fn real_keyed_incremental_row_mutation_prepares_begins_and_finishes_one_exact_publication() {
    use crate::commit::write_stack::domain::IcebergWriteBranch;
    let fixture = row_lineage_fixture().await;
    seed_empty_snapshot(&fixture).await;
    let before = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await
        .unwrap();
    let before_live = live_data_for_test(fixture.provider.runtime().as_ref(), &before)
        .await
        .unwrap();
    assert!(before_live.is_empty());
    let mut prepared = prepare_keyed_variant(
        &fixture,
        ConnectorManagedPublicationTechnique::Incremental,
        false,
    )
    .await;
    let expected_base = crate::commit::write_shared::position_mutation_base(
        before.metadata(),
        before.metadata_location(),
        "main",
        prepared.base_snapshot_id,
    )
    .unwrap();
    let previous = &prepared.declaration;
    let declaration = ConnectorDocumentPublicationDeclaration::try_new(
        previous.publication_id(),
        previous.admission().clone(),
        previous.target_object_id().clone(),
        expected_base,
        previous.technique(),
        previous.empty_input(),
        None,
        None,
    )
    .unwrap();
    prepared.intent.declaration = declaration.clone();
    prepared.declaration = declaration;
    let request = keyed_begin_shape(&prepared, ConnectorManagedPublicationShape::RowMutation);
    assert_eq!(request.intent, ConnectorWriteIntent::RowDelta);
    fixture.catalog.clear();
    let control = write_control(&fixture);
    let plan = control.begin_write(request).unwrap();
    let handle = iceberg_handle(&fixture, &plan);
    assert_eq!(handle.table().format_version(), 3);
    assert_eq!(
        handle.table().schema_id(),
        before.metadata().current_schema_id()
    );
    assert_eq!(handle.table().base_snapshot_id(), prepared.base_snapshot_id);
    assert_eq!(
        handle
            .targets()
            .iter()
            .map(|target| target.branch())
            .collect::<Vec<_>>(),
        vec![IcebergWriteBranch::Data, IcebergWriteBranch::DeletionVector]
    );
    assert_eq!(plan.targets().len(), 2);
    assert!(plan.targets()[0].route().is_some());
    assert!(plan.targets()[1].position_materialization().is_some());
    assert!(handle.mutation_read().is_some());
    // The exact bytes may be bound before finish; finish's same-value binding
    // must be idempotent and retain these bytes through checked dispatch.
    assert!(handle.document_records().unwrap().is_none());
    handle
        .bind_document_records(prepared.records.clone())
        .unwrap();
    assert_eq!(
        handle.document_records().unwrap(),
        Some(prepared.records.clone())
    );
    assert!(fixture.catalog.commits().is_empty());
    let result = control
        .finish_write(empty_keyed_finish(&plan, &prepared))
        .unwrap();
    let ExternalMutationOutcome::KnownCommitted {
        receipt,
        finalization: ExternalMutationFinalization::Complete,
        ..
    } = result
    else {
        panic!("keyed incremental publication must commit")
    };
    assert_eq!(receipt.resulting_row_count(), Some(0));
    let snapshot_id = receipt
        .committed_version()
        .and_then(|v| v.snapshot_id())
        .unwrap();
    let after = assert_keyed_branch_publication(
        &fixture,
        &prepared,
        snapshot_id,
        &["set-properties", "add-snapshot", "set-snapshot-ref"],
    )
    .await;
    assert_eq!(
        live_data_for_test(fixture.provider.runtime().as_ref(), &after)
            .await
            .unwrap(),
        before_live
    );
    assert_eq!(
        after.metadata().current_schema().as_ref(),
        before.metadata().current_schema().as_ref()
    );
    assert_eq!(
        handle.document_records().unwrap(),
        Some(prepared.records.clone())
    );
    assert_keyed_evidence(&fixture, &plan, &prepared);
}

#[tokio::test]
async fn real_keyed_eager_repartition_prepares_begins_and_finishes_one_exact_publication() {
    use crate::commit::write_stack::domain::IcebergWriteBranch;
    let fixture = keyed_fixture().await;
    let before = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await
        .unwrap();
    let prepared =
        prepare_keyed_variant(&fixture, ConnectorManagedPublicationTechnique::Full, true).await;
    let expected = prepared
        .declaration
        .expected_committed_partitioning()
        .unwrap()
        .clone();
    let request = keyed_begin_shape(&prepared, ConnectorManagedPublicationShape::Data);
    fixture.catalog.clear();
    let control = write_control(&fixture);
    let plan = control.begin_write(request).unwrap();
    let handle = iceberg_handle(&fixture, &plan);
    assert_eq!(plan.targets().len(), 1);
    assert_eq!(handle.targets()[0].branch(), IcebergWriteBranch::Data);
    // Commit authority remains on the old generation; writer binding already
    // names the prospective partition spec before any file can be produced.
    assert_eq!(
        handle.table().schema_id(),
        before.metadata().current_schema_id()
    );
    assert_eq!(
        handle.table().default_partition_spec_id(),
        before.metadata().default_partition_spec_id()
    );
    let adapter = crate::commit::write_stack::runtime::build_write_adapter(
        fixture.provider.descriptor().clone(),
        fixture.catalog_handle.clone(),
    );
    let writer = adapter.writer_handle(plan.targets()[0].handle()).unwrap();
    assert_eq!(
        writer.table().schema_id(),
        before.metadata().current_schema_id()
    );
    assert_eq!(
        writer.table().default_partition_spec_id(),
        expected.spec_id()
    );
    let data = writer.data().unwrap();
    // The recipe freezes the provider's authoritative SDK schema, while the
    // execution input schema is admitted separately by write adaptation.
    let target_schema: crate::iceberg::spec::Schema = serde_json::from_str(
        data.target_schema_json()
            .expect("eager repartition must retain its frozen target schema"),
    )
    .unwrap();
    assert_eq!(&target_schema, before.metadata().current_schema().as_ref());
    assert_eq!(data.partition_source_column_names(), &["id".to_string()]);
    assert_eq!(data.transform_exprs(), &["identity".to_string()]);
    assert_eq!(
        data.partition_column_names(),
        &[expected.fields()[0].partition_field_name().to_string()]
    );
    assert!(handle.document_records().unwrap().is_none());
    handle
        .bind_document_records(prepared.records.clone())
        .unwrap();
    assert!(fixture.catalog.commits().is_empty());
    let result = control
        .finish_write(empty_keyed_finish(&plan, &prepared))
        .unwrap();
    let ExternalMutationOutcome::KnownCommitted {
        receipt,
        finalization: ExternalMutationFinalization::Complete,
        ..
    } = result
    else {
        panic!("keyed eager repartition must commit")
    };
    assert_eq!(receipt.resulting_row_count(), Some(0));
    assert_eq!(receipt.committed_partitioning(), Some(&expected));
    let snapshot_id = receipt
        .committed_version()
        .and_then(|v| v.snapshot_id())
        .unwrap();
    let after = assert_keyed_branch_publication(
        &fixture,
        &prepared,
        snapshot_id,
        &[
            "add-spec",
            "set-default-spec",
            "set-properties",
            "add-snapshot",
            "set-snapshot-ref",
        ],
    )
    .await;
    assert_eq!(
        after.metadata().default_partition_spec_id(),
        expected.spec_id()
    );
    assert_eq!(
        after.metadata().current_schema().as_ref(),
        before.metadata().current_schema().as_ref()
    );
    assert!(
        live_data_for_test(fixture.provider.runtime().as_ref(), &after)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        handle.document_records().unwrap(),
        Some(prepared.records.clone())
    );
    assert_keyed_evidence(&fixture, &plan, &prepared);
    let evidence = handle.recovery_evidence().unwrap();
    let reconciled = control
        .reconcile_write(ConnectorWriteSessionReconcileRequest {
            commit: plan.commit_session(),
            evidence,
            context: context(),
        })
        .unwrap();
    let ExternalMutationOutcome::KnownCommitted {
        receipt: reconciled,
        finalization: ExternalMutationFinalization::Complete,
        ..
    } = reconciled
    else {
        panic!("known keyed eager repartition must reconcile idempotently")
    };
    assert_eq!(reconciled.resulting_row_count(), Some(0));
    assert_eq!(reconciled.committed_partitioning(), Some(&expected));
    assert_eq!(
        reconciled.committed_version().and_then(|v| v.snapshot_id()),
        Some(snapshot_id)
    );
    assert_eq!(fixture.catalog.commits().len(), 1);
}
