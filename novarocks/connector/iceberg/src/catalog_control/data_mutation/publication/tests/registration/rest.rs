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
use crate::access_binding::IcebergReadBinding;
use crate::catalog_control::IcebergCatalogControlState;
use crate::iceberg::spec::{
    Datum, ManifestList, PartitionSpec, Schema, SortOrder, TableMetadataBuilder,
};
use crate::iceberg::{TableRequirement, TableUpdate};
use crate::resources::IcebergMetadataResources;
use crate::rest_protocol::test_server::{RecordedRequest, Reply, Script, TestServer};

use novarocks_spi::connector::{ConnectorInstanceId, ConnectorProviderId, ProviderBindingEpoch};
use serde_json::json;
use std::sync::mpsc;

const TABLE_ROUTE: &str = "/v1/namespaces/db/tables/t";

#[derive(Deserialize)]
struct StandardCommit {
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

struct CatalogState {
    metadata: TableMetadata,
    revision: u32,
    requests: Vec<StandardCommit>,
    issued_unconfirmed: Option<StandardCommit>,
}

impl CatalogState {
    fn location(&self) -> String {
        format!(
            "{}/metadata/rest-v{}.json",
            self.metadata.location(),
            self.revision
        )
    }

    fn read(&self) -> Reply {
        Reply::json(
            200,
            &json!({
                "metadata-location": self.location(),
                "metadata": self.metadata,
            }),
        )
    }

    fn commit(&mut self, incoming: StandardCommit) -> Reply {
        let result = incoming
            .requirements
            .iter()
            .try_for_each(|requirement| requirement.check(Some(&self.metadata)));
        if let Err(error) = result {
            self.requests.push(incoming);
            return Reply::json(
                409,
                &json!({"error": {
                    "message": error.message(), "type": "CommitFailedException", "code": 409
                }}),
            );
        }
        let mut next = self.metadata.clone().into_builder(Some(self.location()));
        for update in &incoming.updates {
            next = update.clone().apply(next).unwrap();
        }
        self.metadata = next.build().unwrap().metadata;
        self.revision += 1;
        self.requests.push(incoming);
        self.read()
    }
}

pub(super) struct RestTarget {
    pub(super) provider: Arc<IcebergMetadata>,
    server: TestServer,
    state: Arc<Mutex<CatalogState>>,
    runtime: tokio::runtime::Runtime,
    _warehouse: tempfile::TempDir,
}

impl RestTarget {
    fn new(on_commit: impl Fn(&RecordedRequest, usize) -> bool + Send + Sync + 'static) -> Self {
        Self::with_source(on_commit, None, None)
    }

    pub(super) fn with_s3(source: novarocks_fs::ObjectStoreConfig) -> Self {
        Self::with_source(|_, _| false, Some(source), None)
    }

    pub(super) fn with_s3_spawner(
        source: novarocks_fs::ObjectStoreConfig,
        spawner: Arc<dyn novarocks_fs::FileIoSpawner>,
    ) -> Self {
        Self::with_source(|_, _| false, Some(source), Some(spawner))
    }

    fn with_source(
        on_commit: impl Fn(&RecordedRequest, usize) -> bool + Send + Sync + 'static,
        source: Option<novarocks_fs::ObjectStoreConfig>,
        spawner: Option<Arc<dyn novarocks_fs::FileIoSpawner>>,
    ) -> Self {
        let warehouse = tempfile::tempdir().unwrap();
        let location = if source.is_some() {
            "s3://source-bucket/catalog/db/t".to_owned()
        } else {
            format!("file://{}/db/t", warehouse.path().display())
        };
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            location,
            FormatVersion::V2,
            HashMap::from([
                ("commit.retry.num-retries".into(), "2".into()),
                ("commit.retry.min-wait-ms".into(), "0".into()),
                ("commit.retry.max-wait-ms".into(), "0".into()),
            ]),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let state = Arc::new(Mutex::new(CatalogState {
            metadata,
            revision: 1,
            requests: vec![],
            issued_unconfirmed: None,
        }));
        let loads = Arc::clone(&state);
        let commits = Arc::clone(&state);
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(
                    "GET",
                    "/v1/namespaces/db",
                    Reply::json(200, &json!({"namespace":["db"], "properties":{}})),
                )
                .on_fn("GET", TABLE_ROUTE, move |_, _| loads.lock().unwrap().read())
                .on_fn("POST", TABLE_ROUTE, move |request, index| {
                    let lose_response = on_commit(request, index);
                    let incoming = serde_json::from_slice(&request.body).unwrap();
                    let mut state = commits.lock().unwrap();
                    if lose_response {
                        state.issued_unconfirmed = Some(incoming);
                        Reply::CloseAfterRequest
                    } else {
                        state.commit(incoming)
                    }
                }),
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[
                ("iceberg.catalog.type".into(), "rest".into()),
                ("iceberg.catalog.uri".into(), server.url()),
                ("iceberg.add-files-procedure.enabled".into(), "true".into()),
            ],
        )
        .unwrap();
        let handle = runtime.handle();
        let binding = IcebergReadBinding::new(
            source,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            novarocks_fs::TokioRefreshExecutor::shared(handle.clone()),
            spawner
                .unwrap_or_else(|| Arc::new(novarocks_fs::TokioFileIoSpawner::new(handle.clone()))),
        );
        let context = Arc::new(
            IcebergMetadataContext::try_new(
                IcebergCatalogControlState::new(configuration),
                IcebergMetadataResources::new(binding, handle.clone()),
            )
            .unwrap(),
        );
        let provider = Arc::new(IcebergMetadata::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("ice").unwrap(),
            },
            ProviderBindingEpoch::from_bytes([8; 16]),
            context,
        ));
        Self {
            provider,
            server,
            state,
            runtime,
            _warehouse: warehouse,
        }
    }
}

fn manifest_list(request: &StandardCommit) -> ManifestList {
    let location = request
        .updates
        .iter()
        .find_map(|update| match update {
            TableUpdate::AddSnapshot { snapshot } => Some(snapshot.manifest_list()),
            _ => None,
        })
        .expect("regular REST append snapshot");
    let bytes = std::fs::read(location.strip_prefix("file://").unwrap_or(location)).unwrap();
    ManifestList::parse_with_version(&bytes, FormatVersion::V2).unwrap()
}

fn assert_existing_empty_main(request: &StandardCommit, uuid: uuid::Uuid) {
    assert!(
        request
            .requirements
            .contains(&TableRequirement::UuidMatch { uuid })
    );
    assert!(
        request
            .requirements
            .contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".into(),
                snapshot_id: None,
            }),
        "{:?}",
        request.requirements
    );
}

#[test]
fn rest_concurrent_registration_rechecks_paths_and_reuses_added_manifests() {
    for same_path in [true, false] {
        let (issued, observed) = mpsc::channel::<Vec<u8>>();
        let (release, permit) = mpsc::channel();
        let permit = Mutex::new(permit);
        let target = RestTarget::new(move |request, index| {
            if index == 0 {
                issued.send(request.body.clone()).unwrap();
                permit
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(20))
                    .unwrap();
            }
            false
        });
        let source = tempfile::tempdir().unwrap();
        let a = external_file(
            &source.path().join("a.parquet"),
            vec![Some(5)],
            Some(1),
            None,
        );
        let b = external_file(
            &source.path().join("b.parquet"),
            vec![Some(9)],
            Some(1),
            None,
        );
        let a_location = format!("file://{}", a.display());
        let b_location = format!("file://{}", b.display());
        let budget = Budget::new(8 * 1024 * 1024);
        let first = IcebergDataMutationAdapter::try_new(Arc::clone(&target.provider)).unwrap();
        let first_request = request(
            &first,
            &target.provider,
            a_location.clone(),
            context(&ConnectorStopOwner::new()),
            &budget,
        );
        let (first_commit, a_manifest) = std::thread::scope(|scope| {
            let in_flight = scope.spawn(move || first.execute(first_request).unwrap());
            let body = observed.recv_timeout(Duration::from_secs(15)).unwrap();
            let frozen: StandardCommit = serde_json::from_slice(&body).unwrap();
            let a_manifest = manifest_list(&frozen).entries()[0].manifest_path.clone();
            assert_existing_empty_main(&frozen, target.state.lock().unwrap().metadata.uuid());
            let winner = if same_path { &a_location } else { &b_location };
            let won = execute(&target.provider, winner.as_str(), &budget);
            release.send(()).unwrap();
            assert_committed(
                won,
                1,
                1,
                std::fs::metadata(if same_path { &a } else { &b })
                    .unwrap()
                    .len(),
            );
            (in_flight.join().unwrap(), a_manifest)
        });
        let table = read_table(&target.provider);
        let files = live_files(&target.runtime, &table);
        let state = target.state.lock().unwrap();
        assert_eq!(state.requests.len(), if same_path { 2 } else { 3 });
        assert_eq!(state.revision, if same_path { 2 } else { 3 });
        if same_path {
            assert_failure(
                first_commit,
                connector_reason::ICEBERG_COMMIT_ERROR,
                &format!("Failed to add files: File already exists: {a_location}"),
            );
            assert_eq!(files.len(), 1);
            assert_eq!(files[0].file_path(), a_location);
            assert!(!Path::new(a_manifest.strip_prefix("file://").unwrap_or(&a_manifest)).exists());
        } else {
            assert_committed(first_commit, 1, 1, std::fs::metadata(a).unwrap().len());
            assert_eq!(files.len(), 2);
            let updated = manifest_list(&state.requests[2]);
            assert!(
                updated
                    .entries()
                    .iter()
                    .any(|desc| desc.manifest_path == a_manifest)
            );
            assert!(state.requests[2].requirements.iter().any(|req|
                matches!(req, TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id: Some(_) } if r#ref == "main")
            ));
        }
        assert_eq!(budget.held.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn rest_rejects_missing_ids_without_dispatch_and_publishes_empty_append() {
    let target = RestTarget::new(|_, _| false);
    let dir = tempfile::tempdir().unwrap();
    let path = external_file(&dir.path().join("legacy"), vec![Some(5)], None, None);
    let budget = Budget::new(8 * 1024 * 1024);
    let refused = execute(
        &target.provider,
        format!("file://{}", path.display()),
        &budget,
    );
    assert!(
        matches!(refused, ExternalMutationOutcome::KnownUncommitted { ref failure, .. }
        if failure.kind() == ConnectorMutationFailureKind::Unsupported),
        "{refused:?}"
    );
    assert!(target.server.requests_to("POST", TABLE_ROUTE).is_empty());
    assert!(path.exists());
    assert_eq!(budget.held.load(Ordering::SeqCst), 0);

    let empty = tempfile::tempdir().unwrap();
    assert_committed(
        execute(
            &target.provider,
            format!("file://{}", empty.path().display()),
            &budget,
        ),
        0,
        0,
        0,
    );
    let state = target.state.lock().unwrap();
    let [published] = state.requests.as_slice() else {
        panic!("expected exactly one append")
    };
    assert_existing_empty_main(published, state.metadata.uuid());
    assert!(
        published
            .updates
            .iter()
            .all(|update| !matches!(update, TableUpdate::SetProperties { .. }))
    );
    assert!(manifest_list(published).entries().is_empty());
    assert_eq!(budget.held.load(Ordering::SeqCst), 0);
}

#[test]
fn rest_lost_registration_response_retains_exact_evidence_until_reconciliation() {
    let target = RestTarget::new(|_, index| index == 0);
    let dir = tempfile::tempdir().unwrap();
    let path = external_file(
        &dir.path().join("exact.parquet"),
        vec![Some(5)],
        Some(1),
        None,
    );
    let original = std::fs::read(&path).unwrap();
    let budget = Budget::new(8 * 1024 * 1024);
    let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&target.provider)).unwrap();
    let execution = request(
        &adapter,
        &target.provider,
        format!("file://{}", path.display()),
        context(&ConnectorStopOwner::new()),
        &budget,
    );
    let plan = execution.plan.clone();
    let original_context = execution.context.clone();
    let outcome = adapter.execute(execution).unwrap();
    let ExternalMutationOutcome::CommitUnknown { evidence, failure } = outcome else {
        panic!("expected unknown after accepted request lost its response: {outcome:?}");
    };
    assert_eq!(
        failure.failure_fact().reason(),
        connector_reason::ICEBERG_COMMIT_STATE_UNKNOWN
    );
    assert_eq!(budget.held.load(Ordering::SeqCst), 0);
    let logical: IcebergDataMutationEvidenceV3 =
        decode_canonical_json(evidence.provider_payload(), "actual registration evidence").unwrap();
    assert_eq!(logical.file_count, 1);
    assert_eq!(logical.row_count, 1);
    assert_eq!(logical.total_bytes, original.len() as u64);
    assert!(matches!(
        logical.publication,
        RecoveryPublication::Dispatched { .. }
    ));
    {
        let mut state = target.state.lock().unwrap();
        let frozen = state
            .issued_unconfirmed
            .take()
            .expect("one issued unknown request");
        let manifest = manifest_list(&frozen);
        assert_eq!(manifest.entries().len(), 1);
        let path = &manifest.entries()[0].manifest_path;
        assert!(Path::new(path.strip_prefix("file://").unwrap_or(path)).exists());
        let _ = state.commit(frozen);
    }
    let reconciled = adapter
        .reconcile(ConnectorDataMutationReconcileRequest {
            owner: adapter.binding_key().clone(),
            operation_id: evidence.operation_id(),
            operation_kind: REGISTER_EXISTING_FILES_KIND.into(),
            request_digest: plan.request_digest(),
            plan_digest: plan.plan_digest(),
            state_digest: plan.state_digest(),
            evidence,
            context: original_context,
        })
        .unwrap();
    assert_committed(reconciled, 1, 1, original.len() as u64);
    assert_eq!(target.server.requests_to("POST", TABLE_ROUTE).len(), 1);
    assert_eq!(std::fs::read(path).unwrap(), original);
    let files = live_files(&target.runtime, &read_table(&target.provider));
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].lower_bounds()[&1], Datum::long(5));
}
